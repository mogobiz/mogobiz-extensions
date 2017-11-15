/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.rivers.spi.AbstractESBORiver
import com.mogobiz.json.RenderUtil
import com.mogobiz.pay.domain.BOAccount
import groovy.json.JsonSlurper
import org.hibernate.FlushMode
import rx.Observable

/**
 *
 */
class BOAccountRiver extends AbstractESBORiver<BOAccount>{

    @Override
    Observable<BOAccount> retrieveCatalogItems(RiverConfig config) {
        return Observable.from(BOAccount.executeQuery(
                "select a from BOAccount a",
                [:],
                [readOnly: true, flushMode: FlushMode.MANUAL]
        ))
    }

//    @Override
    Item asItem(BOAccount boAccount, RiverConfig config) {
        BOAccount.withTransaction {
            def map = new JsonSlurper().parse(new StringReader(boAccount.extra)) as Map
            map << [dateCreated: RenderUtil.formatToIso8601(boAccount.dateCreated)]
            map << [lastUpdated: RenderUtil.formatToIso8601(boAccount.lastUpdated)]
            new Item(id: boAccount.uuid, map: map)
        }
    }

    @Override
    String getType() {
        return "Account"
    }
}
