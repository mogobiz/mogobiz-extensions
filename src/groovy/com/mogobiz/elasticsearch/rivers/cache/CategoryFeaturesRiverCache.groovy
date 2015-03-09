package com.mogobiz.elasticsearch.rivers.cache

import com.mogobiz.common.rivers.AbstractRiverCache
import com.mogobiz.store.domain.Feature

/**
 *
 * Created by smanciot on 09/03/15.
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
