package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.rivers.spi.AbstractESBORiver
import com.mogobiz.json.RenderUtil
import com.mogobiz.pay.domain.BOTransactionLog
import groovy.json.JsonSlurper
import rx.Observable

/**
 *
 * Created by smanciot on 11/07/15.
 */
class BOTransactionLogRiver extends AbstractESBORiver<BOTransactionLog>{

    @Override
    Observable<BOTransactionLog> retrieveCatalogItems(RiverConfig config) {
        return Observable.from(BOTransactionLog.findAll())
    }

    @Override
    Item asItem(BOTransactionLog boTransactionLog, RiverConfig config) {
        def map = new JsonSlurper().parse(new StringReader(boTransactionLog.extra)) as Map
        map << [dateCreated: RenderUtil.formatToIso8601(boTransactionLog.dateCreated)]
        map << [lastUpdated: RenderUtil.formatToIso8601(boTransactionLog.lastUpdated)]
        new Item(id: boTransactionLog.uuid, map: map)
    }

    @Override
    String getType() {
        return "BOTransactionLog"
    }
}
