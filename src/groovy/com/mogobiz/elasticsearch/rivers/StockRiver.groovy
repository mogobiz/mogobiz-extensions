/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESMapping
import com.mogobiz.elasticsearch.client.ESProperty
import com.mogobiz.elasticsearch.rivers.spi.AbstractESRiver
import com.mogobiz.store.domain.ProductState
import com.mogobiz.store.domain.StockCalendar
import com.mogobiz.store.domain.TicketType
import groovy.transform.TupleConstructor
import org.hibernate.FlushMode
import org.springframework.transaction.TransactionDefinition

/**
 *
 */
class StockRiver  extends AbstractESRiver<StockCalendarSku> {
    @Override
    rx.Observable<StockCalendarSku> retrieveCatalogItems(RiverConfig riverConfig) {
        Calendar now = Calendar.getInstance()
        return rx.Observable.from(
                TicketType.executeQuery(
                        'SELECT s FROM TicketType s left join fetch s.product as product left join fetch s.stock as stock left join fetch s.stockCalendars WHERE product.category.catalog.id in (:idCatalogs) and product.state = :productState and (s.stopDate is null or s.stopDate >= :today)',
                        [idCatalogs:riverConfig.idCatalogs, productState:ProductState.ACTIVE, today: now], [readOnly: true, flushMode: FlushMode.MANUAL])
                        .collect {new StockCalendarSku(it, it.stockCalendars)}/*TODO rx way*/
        )
    }

    @Override
    Item asItem(StockCalendarSku tuple, RiverConfig riverConfig) {
        new Item(id:tuple.sku?.uuid, type: getType(), map:
                TicketType.withTransaction([propagationBehavior: TransactionDefinition.PROPAGATION_SUPPORTS]) {
                    RiverTools.asStockCalendarSkuMap(tuple, riverConfig)
                }
        )
    }

    @Override
    ESMapping defineESMapping() {
        def stockCalendarProperties = []
        stockCalendarProperties << new ESProperty(name:'id', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        stockCalendarProperties << new ESProperty(name:'uuid', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        stockCalendarProperties << new ESProperty(name:'stock', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        stockCalendarProperties << new ESProperty(name:'startDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        stockCalendarProperties << new ESProperty(name:'dateCreated', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        stockCalendarProperties << new ESProperty(name:'lastUpdated', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)

        new ESMapping(type:getType(),
                timestamp:true,
                properties: []
                        << new ESProperty(name:'id', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'sku', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'uuid', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'productId', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'productUuid', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'startDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'stopDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'calendarType', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'stockByDateTime', type:ESClient.TYPE.NESTED, properties: stockCalendarProperties)
                        << new ESProperty(name:'initialStock', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'stockUnlimited', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'stockOutSelling', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'stockDisplay', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'stock', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'availabilityDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'dateCreated', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'lastUpdated', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        )
    }

    @Override
    String getType() {
        return "stock"
    }

    @Override
    String getUuid(StockCalendarSku s){
        s.sku?.uuid
    }

}

@TupleConstructor
class StockCalendarSku{
    TicketType sku
    Set<StockCalendar> stockCalendars
}
