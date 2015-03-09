package com.mogobiz.elasticsearch.rivers.cache

import com.mogobiz.common.rivers.AbstractRiverCache

/**
 *
 * Created by smanciot on 09/03/15.
 */
class ShippingRiverCache extends AbstractRiverCache<Map> {
    private static ShippingRiverCache shippingRiverCache

    private ShippingRiverCache() {}

    public static ShippingRiverCache getInstance() {
        if (!shippingRiverCache) {
            shippingRiverCache = new ShippingRiverCache()
        }
        shippingRiverCache
    }
}
