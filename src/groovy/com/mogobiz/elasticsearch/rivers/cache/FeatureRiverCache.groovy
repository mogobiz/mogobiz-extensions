/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.elasticsearch.rivers.cache

import com.mogobiz.common.rivers.AbstractRiverCache

/**
 *
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
