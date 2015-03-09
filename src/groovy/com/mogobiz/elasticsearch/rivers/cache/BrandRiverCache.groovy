package com.mogobiz.elasticsearch.rivers.cache

import com.mogobiz.common.rivers.AbstractRiverCache

/**
 *
 * Created by smanciot on 09/03/15.
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
