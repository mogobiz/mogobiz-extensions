/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.elasticsearch.rivers.cache

import com.mogobiz.common.rivers.AbstractRiverCache

/**
 *
 */
class TagCategoriesRiverCache extends AbstractRiverCache<Set<String>> {
    private static TagCategoriesRiverCache tagCategoriesRiverCache

    private TagCategoriesRiverCache() {}

    public static TagCategoriesRiverCache getInstance() {
        if (!tagCategoriesRiverCache) {
            tagCategoriesRiverCache = new TagCategoriesRiverCache()
        }
        tagCategoriesRiverCache
    }
}
