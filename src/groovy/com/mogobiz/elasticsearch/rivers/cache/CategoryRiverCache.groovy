package com.mogobiz.elasticsearch.rivers.cache

import com.mogobiz.common.rivers.AbstractRiverCache

/**
 *
 * Created by smanciot on 09/03/15.
 */
class CategoryRiverCache extends AbstractRiverCache<Map> {
    private static CategoryRiverCache categoryRiverCache

    private CategoryRiverCache() {}

    public static CategoryRiverCache getInstance() {
        if (!categoryRiverCache) {
            categoryRiverCache = new CategoryRiverCache()
        }
        categoryRiverCache
    }
}
