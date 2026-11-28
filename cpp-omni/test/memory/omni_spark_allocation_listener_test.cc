/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Unit tests for OmniSparkAllocationListener.
 *
 * Covers:
 *   - DeltaSignDispatch: positive delta → jReserveMethod,
 *     negative delta → jUnreserveMethod with absolute value, zero → no JNI call.
 *   - DetachedReservationReplaysWhenAttached: GetEnv returns
 *     JNI_EDETACHED → allocationChanged defers its delta without a JNI call;
 *     the next attached call delivers it and updates the listener ledger.
 *   - ClearJavaExceptionBeforeThrow: ExceptionCheck() returns true
 *     after CallLongMethod → ExceptionClear() is called, then
 *     ReservationDenied is thrown.
 *
 * Mock surface: minimal JNIInvokeInterface_ + JNINativeInterface_ tables.
 * NO real JVM bootstrap (JNI_CreateJavaVM) — function pointers are test-controlled.
 * Real JVM bootstrap is covered by the JniGlobalRef RAII test.
 */

#include "gtest/gtest.h"
#include "jni/OmniSparkAllocationListener.h"
#include "memory/reservation_denied.h"
#include "memory/OmniMemoryManager.h"
#include "memory/RuntimeMemoryHelper.h"
#include "operator/spillable_operator.h"
#include <jni.h>
#include <atomic>
#include <cstring>

namespace {

// ============================================================================
// Shared test state — captured via raw pointers set per test case.
// The JNI function pointer tables must be C function pointers, not lambdas
// with captures, so we use file-scope state variables for inter-function comms.
// ============================================================================

// Records which method ID was passed to CallLongMethod and with what argument.
static jmethodID g_callLongMethodId = nullptr;
static jlong     g_callLongMethodArg = 0;
static int       g_callLongMethodCalls = 0;

// Controls the bytes the mock reports Spark "granted" (the reserve/unreserve(J)J
// return). When g_callLongMethodEcho is true the mock echoes the requested amount
// (full grant); otherwise it returns g_callLongMethodReturn (partial / zero grant).
// Default echo=false, return=0 preserves the original mock behaviour.
static bool      g_callLongMethodEcho = false;
static jlong     g_callLongMethodReturn = 0L;

// Controls whether GetEnv succeeds (JNI_OK) or fails (JNI_EDETACHED).
static jint      g_getEnvReturnCode = JNI_OK;

// Pointer to the fake JNIEnv the mock GetEnv will return.
static JNIEnv*   g_fakeEnvPtr = nullptr;

// Controls whether ExceptionCheck returns JNI_TRUE (simulating pending exception).
static jboolean  g_exceptionPending = JNI_FALSE;
static bool g_exceptionIsDenial = true;
static const char* g_exceptionMessage = "Spark reservation denied";

// Counts ExceptionClear calls.
static int       g_exceptionClearCalls = 0;

// ============================================================================
// Mock JNIEnv (JNINativeInterface_) function table
// ============================================================================

// Shared recorder: stores which method/arg was passed and returns the configured
// grant (echo = full grant; else g_callLongMethodReturn for partial/zero grant).
static jlong MockCallLongRecord(jmethodID mid, jlong arg) {
    g_callLongMethodId = mid;
    g_callLongMethodArg = arg;
    g_callLongMethodCalls++;
    return g_callLongMethodEcho ? g_callLongMethodArg : g_callLongMethodReturn;
}

static jlong MockCallLongMethod(JNIEnv*, jobject, jmethodID mid, ...) {
    va_list args;
    va_start(args, mid);
    jlong arg = va_arg(args, jlong);
    va_end(args);
    return MockCallLongRecord(mid, arg);
}

// jni.h's C++ JNIEnv wrapper implements env->CallLongMethod(obj, mid, ...) by
// forwarding to functions->CallLongMethodV (the va_list variant — verified in the
// JDK8 jni.h: `result = functions->CallLongMethodV(this,obj,methodID,args)`). The
// listener calls the C++ form (env->CallLongMethod), so the mock MUST populate the
// CallLongMethodV slot too; leaving it null made the reserve/unreserve upcall jump
// through a null function pointer (SEGV at pc=0x0). Pre-existing harness gap since
// the mock was written (80c5aea): memorytest was only ever a static lib, never
// linked into a runnable binary, so it surfaced on first-ever execution — NOT a
// regression from any listener change.
static jlong MockCallLongMethodV(JNIEnv*, jobject, jmethodID mid, va_list args) {
    jlong arg = va_arg(args, jlong);
    return MockCallLongRecord(mid, arg);
}

static jboolean MockExceptionCheck(JNIEnv*) {
    return g_exceptionPending;
}

static void MockExceptionClear(JNIEnv*) {
    g_exceptionClearCalls++;
    g_exceptionPending = JNI_FALSE;
}

static jobject MockNewGlobalRef(JNIEnv*, jobject obj) {
    // Return the same pointer — it's a mock object; tests don't deref it.
    return obj;
}

static void MockDeleteGlobalRef(JNIEnv*, jobject) {
    // no-op in tests
}

static jclass MockFindClass(JNIEnv*, const char*) {
    return reinterpret_cast<jclass>(uintptr_t{0x10});
}
static jmethodID MockGetMethodID(JNIEnv*, jclass, const char*, const char*) {
    return reinterpret_cast<jmethodID>(uintptr_t{0x20});
}
static void MockDeleteLocalRef(JNIEnv*, jobject) {}
static jthrowable MockExceptionOccurred(JNIEnv*) {
    return reinterpret_cast<jthrowable>(uintptr_t{0x30});
}
static jboolean MockIsInstanceOf(JNIEnv*, jobject, jclass) {
    return g_exceptionIsDenial ? JNI_TRUE : JNI_FALSE;
}
static jobject MockCallObjectMethodV(JNIEnv*, jobject, jmethodID, va_list) {
    return reinterpret_cast<jobject>(uintptr_t{0x40});
}
static const char* MockGetStringUTFChars(JNIEnv*, jstring, jboolean*) {
    return g_exceptionMessage;
}
static void MockReleaseStringUTFChars(JNIEnv*, jstring, const char*) {}

// Build a zeroed-out JNINativeInterface_ then populate the subset we need.
// Callers must keep this struct alive for the duration of the JNIEnv it backs.
static JNINativeInterface_* MakeNativeInterface() {
    static JNINativeInterface_ iface;
    std::memset(&iface, 0, sizeof(iface));

    // Set the function pointers our listener calls.
    // Offsets match JNI 1.8 spec (stable across JDK 8+).
    iface.FindClass = MockFindClass;
    iface.GetMethodID = MockGetMethodID;
    iface.DeleteLocalRef = MockDeleteLocalRef;
    iface.ExceptionOccurred = MockExceptionOccurred;
    iface.IsInstanceOf = MockIsInstanceOf;
    iface.CallObjectMethodV = MockCallObjectMethodV;
    iface.GetStringUTFChars = MockGetStringUTFChars;
    iface.ReleaseStringUTFChars = MockReleaseStringUTFChars;
    iface.NewGlobalRef    = MockNewGlobalRef;
    iface.DeleteGlobalRef = MockDeleteGlobalRef;
    iface.ExceptionCheck  = MockExceptionCheck;
    iface.ExceptionClear  = MockExceptionClear;
    iface.CallLongMethod  = MockCallLongMethod;
    iface.CallLongMethodV = MockCallLongMethodV;   // env->CallLongMethod(...) dispatches here (jni.h)

    return &iface;
}

// ============================================================================
// Mock JavaVM (JNIInvokeInterface_) function table
// ============================================================================

static jint MockGetEnv(JavaVM*, void** env, jint) {
    if (g_getEnvReturnCode == JNI_OK) {
        *reinterpret_cast<JNIEnv**>(env) = g_fakeEnvPtr;
    }
    return g_getEnvReturnCode;
}

static jint MockAttachCurrentThreadAsDaemon(JavaVM*, void** env, void*) {
    // Called by the ctor's AttachCurrentThreadAsDaemonOrThrow when GetEnv returns
    // JNI_EDETACHED. In ctor tests we always set GetEnv to JNI_OK so this isn't
    // normally called; provide it for completeness.
    *reinterpret_cast<JNIEnv**>(env) = g_fakeEnvPtr;
    return JNI_OK;
}

static JNIInvokeInterface_* MakeInvokeInterface() {
    static JNIInvokeInterface_ iface;
    std::memset(&iface, 0, sizeof(iface));
    iface.GetEnv                     = MockGetEnv;
    iface.AttachCurrentThreadAsDaemon = MockAttachCurrentThreadAsDaemon;
    return &iface;
}

// ============================================================================
// Helper: reset all per-test state before each test.
// ============================================================================
static void ResetState() {
    g_callLongMethodId    = nullptr;
    g_callLongMethodArg   = 0;
    g_callLongMethodCalls = 0;
    g_callLongMethodEcho   = false;
    g_callLongMethodReturn = 0L;
    g_getEnvReturnCode    = JNI_OK;
    g_exceptionPending    = JNI_FALSE;
    g_exceptionIsDenial = true;
    g_exceptionMessage = "Spark reservation denied";
    g_exceptionClearCalls = 0;
}

// ============================================================================
// Test-scoped fixture: builds stable JNIEnv + JavaVM backed by the mock tables.
// ============================================================================
struct MockJni {
    JNINativeInterface_* nativeIface = MakeNativeInterface();
    JNIEnv jniEnv{nativeIface};

    JNIInvokeInterface_* invokeIface = MakeInvokeInterface();
    JavaVM javaVM{invokeIface};

    MockJni() {
        g_fakeEnvPtr = &jniEnv;
    }

    JavaVM* vm()   { return &javaVM; }
    JNIEnv* env()  { return &jniEnv; }
};

// Sentinel method IDs — non-null distinct pointers so the listener can
// differentiate reserve vs. unreserve.
static int s_reserveSentinel  = 1;
static int s_unreserveSentinel = 2;

static jmethodID kReserveId  = reinterpret_cast<jmethodID>(&s_reserveSentinel);
static jmethodID kUnreserveId = reinterpret_cast<jmethodID>(&s_unreserveSentinel);

// Dummy listener jobject — used as the local ref passed to the ctor.
// The mock NewGlobalRef returns it unchanged; the listener stores it as
// jListenerGlobalRef_ and passes it to CallLongMethod.
static int        s_listenerSentinel = 42;
static jobject    kListenerObj = reinterpret_cast<jobject>(&s_listenerSentinel);

} // anonymous namespace

TEST(OmniSparkAllocationListener, ManagerExternalRepaymentUpdatesBothListenersOnce) {
    constexpr int64_t kMiB = int64_t{1} << 20;
    for (int64_t repaid : {0, 2, 4}) {
        ResetState();
        g_callLongMethodEcho = true;
        MockJni jni;
        auto listener = std::make_unique<gluten::OmniSparkAllocationListener>(
            jni.vm(), kListenerObj, kReserveId, kUnreserveId);
        auto* spark = listener.get();
        gluten::OmniMemoryManager manager("omni", std::move(listener));
        omniruntime::BindOmniSparkListener(&manager, spark);
        auto* pool = manager.getTaskMemoryPool();
        class Donor final : public omniruntime::op::SpillableOperator {
        public:
            explicit Donor(omniruntime::mem::OmniTaskMemoryPool* pool) : pool_(pool) {}
            uint64_t reclaimableBytes() const override { return 4 * kMiB; }
            int64_t reclaim(int64_t) override { ++reclaimCalls; pool_->SubMemory(4 * kMiB); return 4 * kMiB; }
            bool isInNonReclaimableSection() const noexcept override { return false; }
            const char* reclaimableName() const noexcept override { return "test-donor"; }
            omniruntime::op::Operator* operatorBase() noexcept override { return nullptr; }
            int reclaimCalls{0};
        private:
            omniruntime::mem::OmniTaskMemoryPool* pool_;
        } donor(pool);
        pool->AddMemory(8 * kMiB);
        pool->registerSpillable(&donor);
        const int callsBeforeShrink = g_callLongMethodCalls;
        const auto usedBeforeShrink = pool->currentBytes();
        const auto reservedBeforeShrink = pool->reservedBytes();
        const auto sparkBeforeShrink = spark->currentBytes();
        EXPECT_EQ(manager.shrink(4 * kMiB), 0);
        EXPECT_EQ(donor.reclaimCalls, 0);
        EXPECT_EQ(pool->currentBytes(), usedBeforeShrink);
        EXPECT_EQ(pool->reservedBytes(), reservedBeforeShrink);
        EXPECT_EQ(spark->currentBytes(), sparkBeforeShrink);
        EXPECT_EQ(g_callLongMethodCalls, callsBeforeShrink);
        const int64_t freed = omniruntime::SpillOmniMemory(&manager, 4 * kMiB);
        ASSERT_EQ(freed, 4 * kMiB);
        EXPECT_EQ(donor.reclaimCalls, 1);
        const int beforeRepay = g_callLongMethodCalls;
        omniruntime::ReconcileExternalRepay(&manager, freed, repaid * kMiB);
        EXPECT_EQ(g_callLongMethodCalls, beforeRepay);
        EXPECT_EQ(pool->currentBytes(), 4 * kMiB);
        EXPECT_EQ(pool->reservedBytes(), (8 - repaid) * kMiB);
        EXPECT_EQ(spark->currentBytes(), (8 - repaid) * kMiB);
        EXPECT_EQ(spark->peakBytes(), 8 * kMiB);
        pool->unregisterSpillable(&donor);
        pool->SubMemory(4 * kMiB);
        pool->releaseUnusedReservation();
        EXPECT_EQ(spark->currentBytes(), 0);
        EXPECT_EQ(spark->peakBytes(), 8 * kMiB);
        EXPECT_EQ(g_callLongMethodCalls, beforeRepay + 1);
    }
}

TEST(OmniSparkAllocationListener, FailedManagerRepaymentLeavesPoolAndBlockUnchanged) {
    constexpr int64_t kMiB = int64_t{1} << 20;
    ResetState();
    g_callLongMethodEcho = true;
    MockJni jni;
    auto listener = std::make_unique<gluten::OmniSparkAllocationListener>(
        jni.vm(), kListenerObj, kReserveId, kUnreserveId);
    auto* spark = listener.get();
    gluten::OmniMemoryManager manager("omni", std::move(listener));
    omniruntime::BindOmniSparkListener(&manager, spark);
    auto* pool = manager.getTaskMemoryPool();
    pool->AddMemory(8 * kMiB);
    // Inject a downstream ledger mismatch without altering the pool/block.
    // The manager must reject it without making that mismatch worse.
    spark->reconcileExternalRepay(8 * kMiB);
    const auto reservedBefore = pool->reservedBytes();
    const auto callsBefore = g_callLongMethodCalls;
    EXPECT_THROW(manager.reconcileExternalRepay(4 * kMiB, 2 * kMiB), std::underflow_error);
    EXPECT_EQ(pool->reservedBytes(), reservedBefore);
    EXPECT_EQ(pool->currentBytes(), 8 * kMiB);
    EXPECT_EQ(spark->currentBytes(), 0);
    EXPECT_EQ(g_callLongMethodCalls, callsBefore);
    // Repair the injected fault; ordinary teardown then proves the block
    // ledger also retained all eight MiB rather than committing a partial repay.
    spark->allocationChanged(8 * kMiB);
    EXPECT_NO_THROW(pool->SubMemory(8 * kMiB));
    EXPECT_NO_THROW(pool->releaseUnusedReservation());
    EXPECT_EQ(spark->currentBytes(), 0);
}

TEST(OmniSparkAllocationListener, ManagerShrinkLeavesReservedHeadroomUntouched) {
    constexpr int64_t kMiB = int64_t{1} << 20;
    ResetState();
    g_callLongMethodEcho = true;
    MockJni jni;
    auto listener = std::make_unique<gluten::OmniSparkAllocationListener>(
        jni.vm(), kListenerObj, kReserveId, kUnreserveId);
    auto* spark = listener.get();
    gluten::OmniMemoryManager manager("omni", std::move(listener));
    omniruntime::BindOmniSparkListener(&manager, spark);
    auto* pool = manager.getTaskMemoryPool();
    pool->AddMemory(4 * kMiB);
    ASSERT_TRUE(pool->reserve(8 * kMiB));
    const auto used = pool->currentBytes();
    const auto reserved = pool->reservedBytes();
    const auto billed = spark->currentBytes();
    const int calls = g_callLongMethodCalls;
    ASSERT_GT(reserved, used);
    EXPECT_EQ(manager.shrink(8 * kMiB), 0);
    EXPECT_EQ(pool->currentBytes(), used);
    EXPECT_EQ(pool->reservedBytes(), reserved);
    EXPECT_EQ(spark->currentBytes(), billed);
    EXPECT_EQ(g_callLongMethodCalls, calls);
    pool->SubMemory(4 * kMiB);
    pool->releaseUnusedReservation();
    EXPECT_EQ(spark->currentBytes(), 0);
}

// ============================================================================
// TEST 1 — signed-delta dispatch
// ============================================================================
TEST(OmniSparkAllocationListener, DeltaSignDispatch_ReqList01) {
    ResetState();
    // Spark grants exactly what is requested (full grant). The listener honours the
    // GRANTED bytes (usedBytes_ += granted), so without this the mock's default 0
    // return would leave currentBytes() at 0 and the +1024/+512 assertions below
    // would fail — the test would encode the old inflating semantics.
    g_callLongMethodEcho = true;
    MockJni jni;

    gluten::OmniSparkAllocationListener listener(
        jni.vm(), kListenerObj, kReserveId, kUnreserveId);

    // (a) Positive size → CallLongMethod with jReserveMethod, +size
    g_callLongMethodCalls = 0;
    listener.allocationChanged(1024);
    EXPECT_EQ(g_callLongMethodCalls, 1) << "positive delta must call reserve";
    EXPECT_EQ(g_callLongMethodId, kReserveId) << "positive delta must use jReserveMethod";
    EXPECT_EQ(g_callLongMethodArg, static_cast<jlong>(1024)) << "reserve arg must be +size";
    EXPECT_EQ(listener.currentBytes(), 1024) << "usedBytes_ must reflect positive delta";

    // (b) Negative size → CallLongMethod with jUnreserveMethod, +|size|
    g_callLongMethodCalls = 0;
    listener.allocationChanged(-512);
    EXPECT_EQ(g_callLongMethodCalls, 1) << "negative delta must call unreserve";
    EXPECT_EQ(g_callLongMethodId, kUnreserveId) << "negative delta must use jUnreserveMethod";
    EXPECT_EQ(g_callLongMethodArg, static_cast<jlong>(512)) << "unreserve arg must be |size|";
    EXPECT_EQ(listener.currentBytes(), 512) << "usedBytes_ must reflect subtraction";

    // (c) Zero size → no JNI call (early return per allocationChanged body)
    g_callLongMethodCalls = 0;
    listener.allocationChanged(0);
    EXPECT_EQ(g_callLongMethodCalls, 0) << "zero delta must not produce a JNI call";
}

// ============================================================================
// TEST 2 — silent no-op on detached thread
// ============================================================================
TEST(OmniSparkAllocationListener, DetachedReservationReplaysWhenAttached) {
    ResetState();
    MockJni jni;

    // Ctor path: GetEnv returns JNI_OK so construction succeeds.
    gluten::OmniSparkAllocationListener listener(
        jni.vm(), kListenerObj, kReserveId, kUnreserveId);

    // Now simulate allocationChanged being called from a detached (JIT) thread.
    g_getEnvReturnCode    = JNI_EDETACHED;
    g_callLongMethodCalls = 0;

    // A detached call defers delivery without throwing or calling JNI.
    EXPECT_NO_THROW(listener.allocationChanged(1024)) << "detached thread must not throw";

    // No JNI up-call must have been made.
    EXPECT_EQ(g_callLongMethodCalls, 0) << "no JNI call on detached thread";

    // usedBytes_ must NOT have been bumped (the CAS loop is AFTER GetEnv check).
    EXPECT_EQ(listener.currentBytes(), 0) << "usedBytes_ stays unchanged until deferred delivery";

    // peakBytes_ also stays 0.
    EXPECT_EQ(listener.peakBytes(), 0) << "peakBytes_ stays unchanged until deferred delivery";

    g_getEnvReturnCode = JNI_OK;
    g_callLongMethodEcho = true;
    listener.allocationChanged(0);
    EXPECT_EQ(g_callLongMethodCalls, 1);
    EXPECT_EQ(listener.currentBytes(), 1024);
    EXPECT_EQ(listener.peakBytes(), 1024);
}

// ============================================================================
// TEST 3 — ExceptionClear before throw on Spark TMM denial
// ============================================================================
TEST(OmniSparkAllocationListener, ClearJavaExceptionBeforeThrow_ReqJni02) {
    ResetState();
    MockJni jni;

    // Ctor path: GetEnv returns JNI_OK, construction succeeds.
    gluten::OmniSparkAllocationListener listener(
        jni.vm(), kListenerObj, kReserveId, kUnreserveId);

    // Simulate Spark TMM denial: CallLongMethod leaves a pending Java exception.
    g_exceptionPending    = JNI_TRUE;
    g_exceptionClearCalls = 0;

    EXPECT_THROW(listener.allocationChanged(1024), omniruntime::mem::ReservationDenied);
    EXPECT_EQ(g_exceptionClearCalls, 1);
    EXPECT_FALSE(g_exceptionPending);
    EXPECT_EQ(listener.currentBytes(), 0);
}

// ============================================================================
// TEST 4 — partial grant is honoured
//
// Spark's reserve(J)J can grant FEWER bytes than requested. The listener must
// bump usedBytes_/peakBytes_ by the GRANTED amount, never the requested amount —
// otherwise omni's counter inflates above what Spark actually reserved. The original mock hardcoded a 0 return and no test ever exercised
// 0 < granted < requested.
// ============================================================================
TEST(OmniSparkAllocationListener, PartialGrantHonored_ReqS01) {
    ResetState();
    g_callLongMethodReturn = 600;   // Spark grants only 600 of the 1024 requested
    MockJni jni;

    gluten::OmniSparkAllocationListener listener(
        jni.vm(), kListenerObj, kReserveId, kUnreserveId);

    listener.allocationChanged(1024);
    EXPECT_EQ(g_callLongMethodArg, static_cast<jlong>(1024)) << "reserve must request the full 1024";
    EXPECT_EQ(listener.currentBytes(), 600)
        << "usedBytes_ must reflect GRANTED bytes (600), not requested (1024)";
    EXPECT_EQ(listener.peakBytes(), 600)
        << "peakBytes_ tracks the granted high-watermark, not the request";
}

// ============================================================================
// TEST 5 — zero grant must not inflate usedBytes_
//
// When Spark grants nothing (return 0, no exception), usedBytes_ must stay 0.
// This is the exact case the stale DeltaSignDispatch assertion got wrong: with
// the granted-honoring source, a 0 grant leaves currentBytes() at 0, not at the
// requested 1024.
// ============================================================================
TEST(OmniSparkAllocationListener, ZeroGrantNoInflation_ReqS01) {
    ResetState();
    // g_callLongMethodReturn stays 0 (default) → Spark grants nothing.
    MockJni jni;

    gluten::OmniSparkAllocationListener listener(
        jni.vm(), kListenerObj, kReserveId, kUnreserveId);

    listener.allocationChanged(1024);
    EXPECT_EQ(g_callLongMethodCalls, 1) << "reserve up-call still happens";
    EXPECT_EQ(listener.currentBytes(), 0)
        << "zero grant must NOT inflate usedBytes_ by the requested amount";
}

TEST(OmniSparkAllocationListener, FatalJavaFailurePreservesCauseAndIsNotDenial) {
    ResetState();
    MockJni jni;
    gluten::OmniSparkAllocationListener listener(jni.vm(), kListenerObj, kReserveId, kUnreserveId);
    g_exceptionPending = JNI_TRUE;
    g_exceptionIsDenial = false;
    g_exceptionMessage = "java.lang.RuntimeException: JOIN_SPILL_WRITE_FAILED";
    try {
        listener.allocationChanged(1024);
        FAIL() << "fatal callback must propagate";
    } catch (const std::runtime_error& error) {
        EXPECT_EQ(dynamic_cast<const omniruntime::mem::ReservationDenied*>(&error), nullptr)
            << "fatal callback must not be an advisory denial";
        EXPECT_EQ(std::string(error.what()), g_exceptionMessage);
    }
    EXPECT_FALSE(g_exceptionPending);
}

TEST(OmniSparkAllocationListener, DeferredReplayDoesNotSwallowFatalJavaFailure) {
    ResetState();
    MockJni jni;
    gluten::OmniSparkAllocationListener listener(jni.vm(), kListenerObj, kReserveId, kUnreserveId);
    g_getEnvReturnCode = JNI_EDETACHED;
    listener.allocationChanged(1024);
    g_getEnvReturnCode = JNI_OK;
    g_exceptionPending = JNI_TRUE;
    g_exceptionIsDenial = false;
    g_exceptionMessage = "java.lang.RuntimeException: spill disk failure";
    EXPECT_THROW(listener.allocationChanged(0), std::runtime_error);
    EXPECT_FALSE(g_exceptionPending);
}

TEST(OmniSparkAllocationListener, ExternalRepaymentUpdatesUsageWithoutCallingJava) {
    ResetState();
    MockJni jni;
    g_callLongMethodEcho = true;
    gluten::OmniSparkAllocationListener listener(jni.vm(), kListenerObj, kReserveId, kUnreserveId);
    listener.allocationChanged(8);
    for (int i = 0; i < 10; ++i) {
        const int callsBefore = g_callLongMethodCalls;
        listener.reconcileExternalRepay(4);
        EXPECT_EQ(g_callLongMethodCalls, callsBefore);
        EXPECT_EQ(listener.currentBytes(), 4);
        listener.allocationChanged(4);
        EXPECT_EQ(listener.currentBytes(), 8);
        EXPECT_EQ(listener.peakBytes(), 8);
    }
    listener.reconcileExternalRepay(0);
    EXPECT_EQ(listener.currentBytes(), 8);
    EXPECT_THROW(listener.reconcileExternalRepay(9), std::underflow_error);
    EXPECT_EQ(listener.currentBytes(), 8);
    listener.allocationChanged(-8);
    EXPECT_EQ(listener.currentBytes(), 0);
}
