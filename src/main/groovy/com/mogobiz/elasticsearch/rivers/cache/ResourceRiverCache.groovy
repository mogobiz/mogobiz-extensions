/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.elasticsearch.rivers.cache

import com.mogobiz.common.rivers.AbstractRiverCache

/**
 *
 */
class ResourceRiverCache extends AbstractRiverCache<Map> {
    private static ResourceRiverCache resourceRiverCache

    private ResourceRiverCache() {}

    public static ResourceRiverCache getInstance() {
        if (!resourceRiverCache) {
            resourceRiverCache = new ResourceRiverCache()
        }
        resourceRiverCache
    }
}
