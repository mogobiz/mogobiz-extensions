/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.elasticsearch.rivers.cache

import com.mogobiz.common.rivers.AbstractRiverCache

/**
 *
 */
class BrandCategoriesRiverCache extends AbstractRiverCache<Set<String>> {
    private static BrandCategoriesRiverCache brandCategoriesRiverCache

    private BrandCategoriesRiverCache() {}

    public static BrandCategoriesRiverCache getInstance() {
        if (!brandCategoriesRiverCache) {
            brandCategoriesRiverCache = new BrandCategoriesRiverCache()
        }
        brandCategoriesRiverCache
    }
}
