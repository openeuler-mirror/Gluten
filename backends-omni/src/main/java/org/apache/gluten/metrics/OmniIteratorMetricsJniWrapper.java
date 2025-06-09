/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.apache.gluten.metrics;

import org.apache.gluten.runtime.RuntimeAware;
import org.apache.gluten.vectorized.OmniColumnarBatchOutIterator;

/**
 * OmniIteratorMetricsJniWrapper
 *
 * This class is mainly used to obtain the measurement information of the iterator.
 *
 * @since 2025-06-03
 */
public class OmniIteratorMetricsJniWrapper implements RuntimeAware {
    private OmniIteratorMetricsJniWrapper() {
    }

    /**
     * create OmniIteratorMetricsJniWrapper object
     *
     * @return OmniIteratorMetricsJniWrapper
     */
    public static OmniIteratorMetricsJniWrapper create() {
        return new OmniIteratorMetricsJniWrapper();
    }

    /**
     * fetch Metrics from native
     *
     * @param out out
     * @return Metrics
     */
    public Metrics fetch(OmniColumnarBatchOutIterator out) {
        return nativeFetchMetrics(out.itrHandle());
    }

    private native Metrics nativeFetchMetrics(long itrHandle);

    /**
     * get rtHandle
     *
     * @return -1
     */
    @Override
    public long rtHandle() {
        return -1;
    }
}
