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
package org.apache.gluten.vectorized;

import org.apache.gluten.exception.GlutenException;

import org.apache.spark.storage.BufferReleasingInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FilterInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CheckedInputStream;

/** Create optimal {@link JniByteInputStream} implementation from Java {@link InputStream}. */
public final class JniByteInputStreams {
  private static final Logger LOG = LoggerFactory.getLogger(JniByteInputStreams.class);

    private static final Field FIELD_FILTER_INPUT_STREAM_IN;
    private static final AtomicBoolean NETTY_FALLBACK_WARNING_EMITTED = new AtomicBoolean();

    static {
        try {
            FIELD_FILTER_INPUT_STREAM_IN = FilterInputStream.class.getDeclaredField("in");
            FIELD_FILTER_INPUT_STREAM_IN.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new GlutenException(e);
        }
    }

  private JniByteInputStreams() {}

    /**
     * Creates the most efficient JNI input stream implementation supported by {@code in}.
     *
     * @param in source input stream
     * @return a JNI input stream that owns the source stream
     */
    public static JniByteInputStream create(InputStream in) {
        // Unwrap BufferReleasingInputStream
        final InputStream unwrapped = unwrapSparkInputStream(in);
        if (LowCopyNettyJniByteInputStream.isSupported(unwrapped)) {
            return new LowCopyNettyJniByteInputStream(in);
        }
        if (unwrapped instanceof io.netty.buffer.ByteBufInputStream
                && NETTY_FALLBACK_WARNING_EMITTED.compareAndSet(false, true)) {
            LOG.warn(
                    "Netty shuffle buffer does not use direct storage; "
                            + "falling back from Gluten low-copy shuffle input. "
                            + "This may reduce shuffle performance.");
        }
        if (LowCopyFileSegmentJniByteInputStream.isSupported(unwrapped)) {
            return new LowCopyFileSegmentJniByteInputStream(in);
        }
        return new OnHeapJniByteInputStream(in);
    }

  static InputStream unwrapSparkInputStream(InputStream in) {
    InputStream unwrapped = in;
    if (unwrapped instanceof BufferReleasingInputStream) {
      final BufferReleasingInputStream brin = (BufferReleasingInputStream) unwrapped;
      unwrapped =
          org.apache.spark.storage.SparkInputStreamUtil.unwrapBufferReleasingInputStream(brin);
    }
    if (unwrapped instanceof CheckedInputStream) {
      final CheckedInputStream cin = (CheckedInputStream) unwrapped;
      try {
        final Object wrapped = FIELD_FILTER_INPUT_STREAM_IN.get(cin);
        if (wrapped instanceof InputStream) {
            unwrapped = (InputStream) wrapped;
        } else {
            throw new GlutenException("CheckedInputStream does not wrap an InputStream");
        }
      } catch (IllegalAccessException e) {
        throw new GlutenException(e);
      }
    }
    return unwrapped;
  }

    /**
     * Wraps an existing native address as a direct buffer through the standard JNI API. JNI has
     * supported this API since JDK 1.4, unlike Netty's private DirectByteBuffer constructor path.
     *
     * @param address native memory address that backs the returned buffer
     * @param capacity capacity, in bytes, of the returned buffer
     * @return a direct buffer backed by the supplied native address
     */
    static native ByteBuffer newDirectByteBuffer(long address, int capacity);
}
