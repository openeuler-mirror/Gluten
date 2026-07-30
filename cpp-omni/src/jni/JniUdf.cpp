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

#include "JniUdf.h"
#include "jni_common.h"
#include "udf/UdfLoader.h"

#include <exception>

namespace {
static JavaVM *vm;

const std::string kUdfResolverClassName = "org/apache/spark/sql/expression/UDFResolver$";
const std::string kUdfResolverClassSignature = "Lorg/apache/spark/sql/expression/UDFResolver$;";

static jclass udfResolverClass;
static jmethodID registerUDFMethod;
static jmethodID registerUDAFMethod;
} // namespace

void gluten::initOmniJniUDF(JNIEnv *env)
{
    if (env->GetJavaVM(&vm) != JNI_OK) {
        throw std::runtime_error("Unable to get JavaVM instance");
    }

    // classes
    udfResolverClass = createGlobalClassReferenceOrError(env, kUdfResolverClassName.c_str());

    // methods
    registerUDFMethod = getMethodIdOrError(env, udfResolverClass, "registerUDF", "(Ljava/lang/String;[B[BZZ)V");
    registerUDAFMethod =
        getMethodIdOrError(env, udfResolverClass, "registerUDAF", "(Ljava/lang/String;[B[B[BZZ)V");
}

void gluten::finalizeOmniJniUDF(JNIEnv *env)
{
    env->DeleteGlobalRef(udfResolverClass);
}

void gluten::jniRegisterFunctionSignatures(JNIEnv *env)
{
    try {
        auto udfLoader = gluten::UdfLoader::getInstance();

        jfieldID moduleField =
            env->GetStaticFieldID(udfResolverClass, "MODULE$", kUdfResolverClassSignature.c_str());
        CheckException(env);
        jobject instance = env->GetStaticObjectField(udfResolverClass, moduleField);
        CheckException(env);

        const auto &signatures = udfLoader->getRegisteredUdfSignatures();
        for (const auto &signature : signatures) {
            jstring name = env->NewStringUTF(signature->name.c_str());
            jbyteArray returnType = env->NewByteArray(signature->returnType.length());
            env->SetByteArrayRegion(returnType, 0, signature->returnType.length(),
                reinterpret_cast<const jbyte *>(signature->returnType.c_str()));
            jbyteArray argTypes = env->NewByteArray(signature->argTypes.length());
            env->SetByteArrayRegion(argTypes, 0, signature->argTypes.length(),
                reinterpret_cast<const jbyte *>(signature->argTypes.c_str()));
            if (!signature->intermediateType.empty()) {
                jbyteArray intermediateType = env->NewByteArray(signature->intermediateType.length());
                env->SetByteArrayRegion(intermediateType, 0, signature->intermediateType.length(),
                    reinterpret_cast<const jbyte *>(signature->intermediateType.c_str()));
                env->CallVoidMethod(instance, registerUDAFMethod, name, returnType, argTypes, intermediateType,
                    signature->variableArity, signature->allowTypeConversion);
            } else {
                env->CallVoidMethod(instance, registerUDFMethod, name, returnType, argTypes, signature->variableArity,
                    signature->allowTypeConversion);
            }
        }
    } catch (const std::exception &e) {
        jclass exceptionClass = env->FindClass("java/lang/RuntimeException");
        env->ThrowNew(exceptionClass, e.what());
    } catch (...) {
        jclass exceptionClass = env->FindClass("java/lang/RuntimeException");
        env->ThrowNew(exceptionClass, "Unknown native error while registering UDF/UDAF signatures");
    }
}
