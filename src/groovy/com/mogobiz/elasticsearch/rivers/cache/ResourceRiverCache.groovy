package com.mogobiz.elasticsearch.rivers.cache

import com.mogobiz.common.rivers.AbstractRiverCache

/**
 *
 * Created by smanciot on 09/03/15.
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
