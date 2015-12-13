/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.rivers.spi.AbstractESBORiver
import com.mogobiz.json.RenderUtil
import com.mogobiz.pay.domain.BOTransactionLog
import groovy.json.JsonSlurper
import org.hibernate.FlushMode
import rx.Observable

/**
 *
 */
class BOTransactionLogRiver extends AbstractESBORiver<BOTransactionLog>{

    @Override
    Observable<BOTransactionLog> retrieveCatalogItems(RiverConfig config) {
        return Observable.from(BOTransactionLog.executeQuery(
                "select l from BOTransactionLog l",
                [:],
                [readOnly: true, flushMode: FlushMode.MANUAL]
        ))
    }

    @Override
    Item asItem(BOTransactionLog boTransactionLog, RiverConfig config) {
        BOTransactionLog.withTransaction {
            def map = new JsonSlurper().parse(new StringReader(boTransactionLog.extra)) as Map
            map << [dateCreated: RenderUtil.formatToIso8601(boTransactionLog.dateCreated)]
            map << [lastUpdated: RenderUtil.formatToIso8601(boTransactionLog.lastUpdated)]
            new Item(id: boTransactionLog.uuid, map: map)
        }
    }

    @Override
    String getType() {
        return "BOTransactionLog"
    }
}
