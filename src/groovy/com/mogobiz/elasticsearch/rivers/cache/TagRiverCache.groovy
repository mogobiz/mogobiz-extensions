/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.elasticsearch.rivers.cache

import com.mogobiz.common.rivers.AbstractRiverCache

/**
 *
 * Created by smanciot on 09/03/15.
 */
class TagRiverCache extends AbstractRiverCache<Map> {
    private static TagRiverCache tagRiverCache

    private TagRiverCache() {}

    public static TagRiverCache getInstance() {
        if (!tagRiverCache) {
            tagRiverCache = new TagRiverCache()
        }
        tagRiverCache
    }
}
