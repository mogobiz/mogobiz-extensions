package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.rivers.spi.AbstractESBORiver
import com.mogobiz.json.RenderUtil
import com.mogobiz.pay.domain.BOAccount
import groovy.json.JsonSlurper
import rx.Observable

/**
 *
 * Created by smanciot on 11/07/15.
 */
class BOAccountRiver extends AbstractESBORiver<BOAccount>{

    @Override
    Observable<BOAccount> retrieveCatalogItems(RiverConfig config) {
        return Observable.from(BOAccount.findAll())
    }

    @Override
    Item asItem(BOAccount boAccount, RiverConfig config) {
        def map = new JsonSlurper().parse(new StringReader(boAccount.extra)) as Map
        map << [dateCreated: RenderUtil.formatToIso8601(boAccount.dateCreated)]
        map << [lastUpdated: RenderUtil.formatToIso8601(boAccount.lastUpdated)]
        new Item(id: boAccount.uuid, map: map)
    }

    @Override
    String getType() {
        return "Account"
    }
}
