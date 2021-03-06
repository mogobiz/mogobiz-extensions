/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.elasticsearch.rivers.cache

import com.mogobiz.common.rivers.AbstractRiverCache
import com.mogobiz.store.domain.Coupon

/**
 *
 */
class CouponsRiverCache extends AbstractRiverCache<Set<Coupon>> {
    private static CouponsRiverCache couponsRiverCache

    private CouponsRiverCache() {}

    public static CouponsRiverCache getInstance() {
        if (!couponsRiverCache) {
            couponsRiverCache = new CouponsRiverCache()
        }
        couponsRiverCache
    }
}
