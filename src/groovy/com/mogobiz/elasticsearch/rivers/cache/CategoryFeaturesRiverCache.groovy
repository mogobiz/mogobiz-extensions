/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.elasticsearch.rivers.cache

import com.mogobiz.common.rivers.AbstractRiverCache
import com.mogobiz.store.domain.Feature

/**
 *
 */
class CategoryFeaturesRiverCache extends AbstractRiverCache<Set<Feature>> {
    private static CategoryFeaturesRiverCache categoryFeaturesRiverCache

    private CategoryFeaturesRiverCache() {}

    public static CategoryFeaturesRiverCache getInstance() {
        if (!categoryFeaturesRiverCache) {
            categoryFeaturesRiverCache = new CategoryFeaturesRiverCache()
        }
        categoryFeaturesRiverCache
    }
}
