package com.mogobiz.elasticsearch.rivers.cache

import com.mogobiz.common.rivers.AbstractRiverCache

/**
 *
 * Created by smanciot on 09/03/15.
 */
class FeatureRiverCache extends AbstractRiverCache<Map> {
    private static FeatureRiverCache featureRiverCache

    private FeatureRiverCache() {}

    public static FeatureRiverCache getInstance() {
        if (!featureRiverCache) {
            featureRiverCache = new FeatureRiverCache()
        }
        featureRiverCache
    }
}
