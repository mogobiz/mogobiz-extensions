/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESMapping
import com.mogobiz.elasticsearch.client.ESProperty
import com.mogobiz.elasticsearch.rivers.cache.TranslationsRiverCache
import com.mogobiz.elasticsearch.rivers.spi.AbstractESRiver
import com.mogobiz.store.domain.ProductState
import com.mogobiz.store.domain.Resource
import com.mogobiz.store.domain.Translation
import org.hibernate.FlushMode
import org.springframework.transaction.TransactionDefinition
import rx.Observable

/**
 *
 */
class ResourceRiver  extends AbstractESRiver<Resource> {

    @Override
    Observable<Resource> retrieveCatalogItems(RiverConfig config) {
        def languages = config?.languages ?: ['fr', 'en', 'es', 'de']
        def defaultLang = config?.defaultLang ?: 'fr'
        def _defaultLang = defaultLang.trim().toLowerCase()
        def _languages = languages.collect {it.trim().toLowerCase()} - _defaultLang
        final args = [readOnly: true, flushMode: FlushMode.MANUAL]
        if(!_languages.flatten().isEmpty()) {
            def translations = config.partial ? Translation.executeQuery('select t from Product2Resource pr left join pr.product as p left join pr.resource as r, Translation t where t.target=r.id and t.lang in :languages and (p.id in (:idProducts) and p.state=:productState)',
                    [languages: _languages, idProducts: config.idProducts, productState: ProductState.ACTIVE], args) : Translation.executeQuery('select t from Product2Resource pr left join pr.product as p left join pr.resource as r, Translation t where t.target=r.id and t.lang in :languages and (p.category.catalog.id in (:idCatalogs) and p.state=:productState)',
                    [languages: _languages, idCatalogs: config.idCatalogs, productState: ProductState.ACTIVE], args)
            translations.groupBy {
                it.target.toString()
            }.each { String k, List<Translation> v -> TranslationsRiverCache.instance.put(k, v) }
        }
        Observable.from(config.partial ? Resource.executeQuery('SELECT r FROM Product2Resource pr left join pr.product as p left join pr.resource as r WHERE p.id in (:idProducts) and p.state=:productState and p.deleted=false and r.active=true and r.deleted=false',
                [idProducts:config.idProducts, productState:ProductState.ACTIVE], args).toSet() : Resource.executeQuery('SELECT r FROM Product2Resource pr left join pr.product as p left join pr.resource as r WHERE p.category.catalog.id in (:idCatalogs) and p.state=:productState and p.deleted=false and r.active=true and r.deleted=false',
                [idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE], args).toSet())
    }

    @Override
    Item asItem(Resource resource, RiverConfig config) {
        new Item(id:resource.id, type: getType(), map:
                Resource.withTransaction([propagationBehavior: TransactionDefinition.PROPAGATION_SUPPORTS]) {
                    RiverTools.asResourceMap(resource, config)
                }
        )
    }

    @Override
    ESMapping defineESMapping() {
        new ESMapping(type:getType(),
                timestamp:true,
                properties: [] << new ESProperty(name:'uuid', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'active', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NO, multilang:false)
                        << new ESProperty(name:'deleted', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NO, multilang:false)
                        << new ESProperty(name:'uploaded', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NO, multilang:false)
                        << new ESProperty(name:'code', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:false)
                        << new ESProperty(name:'description', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:false)
                        << new ESProperty(name:'xtype', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'contentType', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'content', type:ESClient.TYPE.BINARY, index:ESClient.INDEX.NO, multilang:false)
                        << new ESProperty(name:'sanitizedName', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NO, multilang:false)
                        << new ESProperty(name:'url', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NO, multilang:false)
                        << new ESProperty(name:'smallPicture', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NO, multilang:false)
                        << new ESProperty(name:'md5', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        )
    }

    @Override
    String getType() {
        return 'resource'
    }

    @Override
    String getUuid(Resource r){
        r.uuid
    }

}


