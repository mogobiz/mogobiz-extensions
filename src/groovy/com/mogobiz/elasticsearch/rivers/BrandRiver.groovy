/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.rivers.cache.BrandCategoriesRiverCache
import com.mogobiz.elasticsearch.rivers.cache.TranslationsRiverCache
import com.mogobiz.elasticsearch.rivers.spi.AbstractESRiver
import com.mogobiz.store.domain.Brand
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESMapping
import com.mogobiz.elasticsearch.client.ESProperty
import com.mogobiz.store.domain.Category
import com.mogobiz.store.domain.Product
import com.mogobiz.store.domain.ProductState
import com.mogobiz.store.domain.Translation
import org.hibernate.FlushMode
import org.springframework.transaction.TransactionDefinition
import rx.Observable

/**
 */
class BrandRiver extends AbstractESRiver<Brand> {

    @Override
    ESMapping defineESMapping(){
        new ESMapping(type:getType(),
                timestamp:true,
                properties: [] << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
                        << new ESProperty(name:'website', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NO, multilang:true)
                        << new ESProperty(name:'imported', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'twitter', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'description', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:false)
                        << new ESProperty(name:'hide', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'increments', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'content', type:ESClient.TYPE.BINARY, index:ESClient.INDEX.NO, multilang:false)
                        << new ESProperty(name:'md5', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'categories', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        )
    }

    @Override
    Observable<Brand> retrieveCatalogItems(final RiverConfig config){
        def languages = config?.languages ?: ['fr', 'en', 'es', 'de']
        def defaultLang = config?.defaultLang ?: 'fr'
        def _defaultLang = defaultLang.trim().toLowerCase()
        def _languages = languages.collect {it.trim().toLowerCase()} - _defaultLang
        if(!_languages.flatten().isEmpty()) {
            Translation.executeQuery('select t from Brand brand, Translation t where t.target=brand.id and t.lang in :languages and brand.company.id=:idCompany',
                    [languages: _languages, idCompany: config.idCompany], [readOnly: true, flushMode: FlushMode.MANUAL]).groupBy {
                it.target.toString()
            }.each { String k, List<Translation> v -> TranslationsRiverCache.instance.put(k, v) }
        }

        def brandCategoriesRiverCache = BrandCategoriesRiverCache.instance

        def results = config.partial ? Product.executeQuery('select brand.uuid, category.fullpath from Product p, Category category, Brand brand WHERE (p.id in (:idProducts) or category.id in (:idCategories)) and p.state = :productState and p.deleted = false and brand.id = p.brand.id and category.id = p.category.id',
                [idProducts: config.idProducts, idCategories:config.idCategories, productState:ProductState.ACTIVE], [readOnly: true, flushMode: FlushMode.MANUAL]) : Product.executeQuery('select brand.uuid, category.fullpath from Product p, Category category, Brand brand WHERE category.catalog.id in (:idCatalogs) and p.state = :productState and p.deleted = false and brand.id = p.brand.id and category.id = p.category.id',
                [idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE], [readOnly: true, flushMode: FlushMode.MANUAL])
        results.each {a ->
            String key = a[0] as String
            String fullpath = a[1] as String
            Set<String> categories = brandCategoriesRiverCache.get(key) ?: []
            categories << fullpath
            brandCategoriesRiverCache.put(key, categories)
        }

        return Observable.from(Brand.executeQuery('select brand from Brand brand left join fetch brand.brandProperties where brand.company.id=:idCompany',
                [idCompany: config.idCompany], [readOnly: true, flushMode: FlushMode.MANUAL]))
    }

    @Override
    String getType(){
        'brand'
    }

    @Override
    Item asItem(Brand b, RiverConfig config) {
        new Item(id:b.id, type: getType(), map:
                Brand.withTransaction([propagationBehavior: TransactionDefinition.PROPAGATION_SUPPORTS]){
                    RiverTools.asBrandMap(b, config, true)
                }
        )
    }

    @Override
    String getUuid(Brand b){
        b.uuid
    }

}

