/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.rivers.cache.TranslationsRiverCache
import com.mogobiz.elasticsearch.rivers.spi.AbstractESRiver
import com.mogobiz.store.domain.Category
import com.mogobiz.store.domain.Catalog
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESMapping
import com.mogobiz.elasticsearch.client.ESProperty
import com.mogobiz.store.domain.Translation
import org.hibernate.FlushMode
import org.springframework.transaction.TransactionDefinition
import rx.Observable

/**
 */
class CategoryRiver extends AbstractESRiver<Category>{

    @Override
    ESMapping defineESMapping(){
        new ESMapping(type:getType(),
                timestamp:true,
                properties: [] << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
                        << new ESProperty(name:'description', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
                        << new ESProperty(name:'uuid', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'keywords', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
                        << new ESProperty(name:'path', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'imported', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'hide', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'parentId', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'increments', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'position', type:ESClient.TYPE.INTEGER, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        )
    }

    @Override
    Observable<Category> retrieveCatalogItems(final RiverConfig config){
        def languages = config?.languages ?: ['fr', 'en', 'es', 'de']
        def defaultLang = config?.defaultLang ?: 'fr'
        def _defaultLang = defaultLang.trim().toLowerCase()
        def _languages = languages.collect {it.trim().toLowerCase()} - _defaultLang
        final args = [readOnly: true, flushMode: FlushMode.MANUAL]
        if(!_languages.flatten().isEmpty()) {
            def translations = config.partial ? Translation.executeQuery('select t from Category cat, Translation t where t.target=cat.id and t.lang in :languages and cat.id in (:idCategories)', [languages: _languages, idCategories: config.idCategories], args) : Translation.executeQuery('select t from Category cat, Translation t where t.target=cat.id and t.lang in :languages and cat.catalog.id in (:idCatalogs)', [languages: _languages, idCatalogs: config.idCatalogs], args)
            translations.groupBy {
                it.target.toString()
            }.each { String k, List<Translation> v -> TranslationsRiverCache.instance.put(k, v) }
        }

        return Observable.from(config.partial ? Category.findAll('From Category cat where cat.id in (:idCategories) and deleted=false', [idCategories: config.idCategories], args) : Category.findAll('From Category cat where cat.catalog.id in (:idCatalogs) and deleted=false', [idCatalogs: config.idCatalogs], args))
    }

    @Override
    String getType(){
        'category'
    }

    @Override
    Item asItem(Category category, RiverConfig config) {
        new Item(id:category.id, type: getType(), map:
                Category.withTransaction([propagationBehavior: TransactionDefinition.PROPAGATION_SUPPORTS]) {
                    RiverTools.asCategoryMap(category, config)
                }
        )
    }

    @Override
    String getUuid(Category c){
        c.uuid
    }

}


