/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.rivers.spi.AbstractESRiver
import com.mogobiz.store.domain.ProductState
import com.mogobiz.store.domain.Suggestion
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESMapping
import com.mogobiz.elasticsearch.client.ESProperty
import org.hibernate.FlushMode
import org.springframework.transaction.TransactionDefinition
import rx.Observable

/**
 */
class SuggestionRiver extends AbstractESRiver<Suggestion>{

    @Override
    Item asItem(Suggestion suggestion, RiverConfig config){
        Suggestion.withTransaction([propagationBehavior: TransactionDefinition.PROPAGATION_SUPPORTS]) {
            new Item(
                    id: suggestion.id,
                    type: getType(),
                    map: RiverTools.asSuggestionMap(suggestion, config),
                    parent: suggestion && suggestion.pack ? new Item(id: suggestion.pack.id, type: 'product') : null
            )
        }
    }

    @Override
    ESMapping defineESMapping() {
        ESMapping mapping = new ESMapping(type:getType(),
                timestamp:true)
        mapping.properties = new ProductRiver().defineESMapping().properties
        mapping.properties << new ESProperty(name:'required', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        mapping.properties << new ESProperty(name:'position', type:ESClient.TYPE.INTEGER, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        mapping.properties << new ESProperty(name:'discount', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        mapping.parent = 'product'
        mapping
    }

    @Override
    Observable<Suggestion> retrieveCatalogItems(RiverConfig config) {
        return Observable.from(Suggestion.executeQuery('FROM Suggestion s ' +
                'join fetch s.pack as pack ' +
                'join fetch s.product as p ' +
                'left join fetch p.ticketTypes ' +
                'left join fetch p.features ' +
                'left join fetch p.featureValues ' +
                'left join fetch p.productProperties ' +
                'left join fetch p.product2Resources as pr ' +
                'left join fetch pr.resource ' +
                'left join fetch p.intraDayPeriods ' +
                'left join fetch p.datePeriods ' +
                'left join fetch p.tags ' +
                'left join fetch p.ticketTypes as sku ' +
                'left join fetch sku.variation1 v1 ' +
                'left join fetch v1.variation ' +
                'left join fetch sku.variation2 v2 ' +
                'left join fetch v2.variation ' +
                'left join fetch sku.variation3 v3 ' +
                'left join fetch v3.variation ' +
                'left join fetch p.poi ' +
                'left join fetch p.category as category ' +
                'left join fetch category.parent ' +
                'left join fetch p.brand as brand ' +
                'left join fetch brand.brandProperties ' +
                'left join fetch p.shipping ' +
                'left join fetch p.taxRate as taxRate ' +
                'left join fetch taxRate.localTaxRates ' +
                'left join fetch p.ibeacon ' +
                'left join fetch p.company ' +
                'WHERE s.pack.category.catalog.id in (:idCatalogs) and p.state = :productState',
                [idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE], [readOnly: true, flushMode: FlushMode.MANUAL]))
    }

    @Override
    String getType() {
        return 'suggestion'
    }

    @Override
    String getUuid(Suggestion s){
        s.uuid
    }

}
