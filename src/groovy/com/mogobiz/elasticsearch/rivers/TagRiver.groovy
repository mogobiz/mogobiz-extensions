/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.rivers.cache.TagCategoriesRiverCache
import com.mogobiz.elasticsearch.rivers.cache.TranslationsRiverCache
import com.mogobiz.elasticsearch.rivers.spi.AbstractESRiver
import com.mogobiz.store.domain.Category
import com.mogobiz.store.domain.Product
import com.mogobiz.store.domain.ProductState
import com.mogobiz.store.domain.Tag
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESMapping
import com.mogobiz.elasticsearch.client.ESProperty
import com.mogobiz.store.domain.Translation
import org.hibernate.FlushMode
import org.springframework.transaction.TransactionDefinition
import rx.Observable

/**
 */
class TagRiver extends AbstractESRiver<Tag>{

    @Override
    Observable<Tag> retrieveCatalogItems(final RiverConfig config){
        def languages = config?.languages ?: ['fr', 'en', 'es', 'de']
        def defaultLang = config?.defaultLang ?: 'fr'
        def _defaultLang = defaultLang.trim().toLowerCase()
        def _languages = languages.collect {it.trim().toLowerCase()} - _defaultLang
        final args = [readOnly: true, flushMode: FlushMode.MANUAL]
        if(!_languages.flatten().isEmpty()) {
            def translations = Translation.executeQuery('select t from Tag tag, Translation t where t.target=tag.id and t.lang in :languages and tag.company.id=:idCompany',
                    [languages: _languages, idCompany: config.idCompany], args)
            translations.groupBy {
                it.target.toString()
            }.each { String k, List<Translation> v -> TranslationsRiverCache.instance.put(k, v) }
        }

        def tagCategoriesRiverCache = TagCategoriesRiverCache.instance

        def results = config.partial ? Product.executeQuery('select p, tag.uuid, category from Product p left join fetch p.category as category left join fetch p.tags as tag left join fetch category.parent WHERE category.id in (:idCategories) and p.state = :productState and p.deleted = false',
                [idCategories:config.idCategories, productState:ProductState.ACTIVE], args) : Product.executeQuery('select p, tag.uuid, category from Product p left join fetch p.category as category left join fetch p.tags as tag left join fetch category.parent WHERE category.catalog.id in (:idCatalogs) and p.state = :productState and p.deleted = false',
                [idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE], args)
        results.each { a ->
            String key = a[1] as String
            Category category = a[2] as Category
            String fullpath = category.fullpath ?: RiverTools.retrieveCategoryPath(category)
            Set<String> categories = tagCategoriesRiverCache.get(key) ?: []
            categories << fullpath
            tagCategoriesRiverCache.put(key, categories)
        }

        return Observable.from(config.partial ? Tag.executeQuery('SELECT DISTINCT p.tags FROM Product p WHERE p.id in (:idProducts)',
                [idProducts:config.idProducts], args) : Tag.executeQuery('SELECT DISTINCT p.tags FROM Product p WHERE p.category.catalog.id in (:idCatalogs)',
                [idCatalogs:config.idCatalogs], args))
    }

    @Override
    ESMapping defineESMapping(){
        new ESMapping(type:getType(),
                timestamp:true,
                properties: [] << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
                        << new ESProperty(name:'imported', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'increments', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'categories', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        )
    }

    @Override
    String getType(){
        return 'tag'
    }

    @Override
    Item asItem(Tag tag, RiverConfig config) {
        new Item(id:tag.id, type: getType(), map:
                Tag.withTransaction([propagationBehavior: TransactionDefinition.PROPAGATION_SUPPORTS]) {
                    RiverTools.asTagMap(tag, config, true)
                }
        )
    }

    @Override
    String getUuid(Tag t){
        t.uuid
    }

}

