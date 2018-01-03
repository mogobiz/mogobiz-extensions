/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.elasticsearch.rivers.cache

import com.mogobiz.common.rivers.AbstractRiverCache

/**
 *
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
