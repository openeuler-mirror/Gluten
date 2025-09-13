package org.apache.gluten.init;

import nova.hetu.omniruntime.memory.MemoryManager;
import nova.hetu.omniruntime.operator.OmniOperatorFactoryContext;
import scala.runtime.BoxedUnit;

import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.util.SparkShutdownManagerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class OmniNativeBackendInitializer {
    private static final Logger LOG = LoggerFactory.getLogger(OmniNativeBackendInitializer.class);
    private static final Map<String, OmniNativeBackendInitializer> instances = new ConcurrentHashMap<>();

    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final String backendName;

    private OmniNativeBackendInitializer(String backendName) {
        this.backendName = backendName;
    }

    public static OmniNativeBackendInitializer forBackend(String backendName) {
        return instances.computeIfAbsent(backendName, k -> new OmniNativeBackendInitializer(backendName));
    }

    // Spark DriverPlugin/ExecutorPlugin will only invoke NativeBackendInitializer#initializeBackend
    // method once in its init method.
    // In cluster mode, NativeBackendInitializer#initializeBackend only will be invoked in different
    // JVM.
    // In local mode, NativeBackendInitializer#initializeBackend will be invoked twice in same
    // thread, driver first then executor, initialized flag ensure only invoke initializeBackend once,
    // so there are no race condition here.
    public void initialize(scala.collection.Map<String, String> conf) {
        if (!initialized.compareAndSet(false, true)) {
            // Already called.
            return;
        }
        initialize0(conf);
        SparkShutdownManagerUtil.addHook(
            () -> BoxedUnit.UNIT);
    }

    private void initialize0(scala.collection.Map<String, String> conf) {
        try {
            long offHeapSize = JavaUtils.byteStringAsBytes(conf.getOrElse("spark.memory.offHeap.size", () -> "1g"));
            MemoryManager.setGlobalMemoryLimit(offHeapSize);
            OmniOperatorFactoryContext.setDefaultNeedCacheValue(conf.getOrElse("spark.gluten.sql.columnar.backend.omni.operator.factory.cache.enabled", () -> true));
        } catch (Exception e) {
            LOG.error("Failed to call native backend's initialize method", e);
            throw e;
        }
    }

}
