/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.elasticsearch.rivers.cache

import com.mogobiz.common.rivers.AbstractRiverCache

/**
 *
 */
class BrandRiverCache extends AbstractRiverCache<Map> {
    private static BrandRiverCache brandRiverCache

    private BrandRiverCache() {}

    public static BrandRiverCache getInstance() {
        if (!brandRiverCache) {
            brandRiverCache = new BrandRiverCache()
        }
        brandRiverCache
    }
}
