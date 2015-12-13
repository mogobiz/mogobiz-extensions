/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.rivers.spi.AbstractESRiver
import com.mogobiz.store.domain.Catalog
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESMapping
import com.mogobiz.elasticsearch.client.ESProperty
import org.springframework.transaction.TransactionDefinition
import rx.Observable

/**
 */
class CatalogRiver extends AbstractESRiver<Catalog> {

    @Override
    ESMapping defineESMapping() {
        new ESMapping(type:getType(),
                timestamp:true,
                properties: [] << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
                        << new ESProperty(name:'description', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
                        << new ESProperty(name:'uuid', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'activationDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'imported', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        )
    }

    @Override
    Observable<Catalog> retrieveCatalogItems(final RiverConfig config) {
        return Observable.from([Catalog.get(config.idCatalog)])
    }

    @Override
    String getType() {
        return 'catalog'
    }

    @Override
    Item asItem(Catalog catalog, RiverConfig config) {
        new Item(id:catalog.id, type: getType(), map:
                Catalog.withTransaction([propagationBehavior: TransactionDefinition.PROPAGATION_SUPPORTS]) {
                    RiverTools.asCatalogMap(catalog, config)
                }
        )
    }

    @Override
    String getUuid(Catalog c){
        c.uuid
    }

}
