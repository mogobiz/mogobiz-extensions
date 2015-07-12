package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.rivers.spi.AbstractESBORiver
import com.mogobiz.json.RenderUtil
import com.mogobiz.pay.domain.BOTransaction
import groovy.json.JsonSlurper
import rx.Observable

/**
 *
 * Created by smanciot on 11/07/15.
 */
class BOTransactionRiver extends AbstractESBORiver<BOTransaction>{

    @Override
    Observable<BOTransaction> retrieveCatalogItems(RiverConfig config) {
        return Observable.from(BOTransaction.findAll())
    }

    @Override
    Item asItem(BOTransaction boTransaction, RiverConfig config) {
        def map = new JsonSlurper().parse(new StringReader(boTransaction.extra)) as Map
        map << [dateCreated: RenderUtil.formatToIso8601(boTransaction.dateCreated)]
        map << [lastUpdated: RenderUtil.formatToIso8601(boTransaction.lastUpdated)]
        new Item(id: boTransaction.uuid, map: map)
    }

    @Override
    String getType() {
        return "BOTransaction"
    }
}
