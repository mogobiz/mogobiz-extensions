/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.elasticsearch.rivers.cache

import com.mogobiz.common.rivers.AbstractRiverCache

/**
 *
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
