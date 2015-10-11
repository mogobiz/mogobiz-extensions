/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.elasticsearch.rivers.cache

import com.mogobiz.common.rivers.AbstractRiverCache
import com.mogobiz.store.domain.Translation

/**
 *
 * Created by smanciot on 09/03/15.
 */
class TranslationsRiverCache extends AbstractRiverCache<List<Translation>> {
    private static TranslationsRiverCache transalationsRiverCache

    private TranslationsRiverCache() {}

    public static TranslationsRiverCache getInstance() {
        if (!transalationsRiverCache) {
            transalationsRiverCache = new TranslationsRiverCache()
        }
        transalationsRiverCache
    }
}
