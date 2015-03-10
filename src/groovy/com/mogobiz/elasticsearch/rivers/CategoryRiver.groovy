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
import rx.Observable

/**
 * Created by stephane.manciot@ebiznext.com on 15/02/2014.
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
        )
    }

    @Override
    Observable<Category> retrieveCatalogItems(final RiverConfig config){
        def languages = config?.languages ?: ['fr', 'en', 'es', 'de']
        def defaultLang = config?.defaultLang ?: 'fr'
        def _defaultLang = defaultLang.trim().toLowerCase()
        def _languages = languages.collect {it.trim().toLowerCase()} - _defaultLang
        if(!_languages.flatten().isEmpty()) {
            Translation.executeQuery('select t from Category cat, Translation t where t.target=cat.id and t.lang in :languages and cat.catalog.id=:idCatalog',
                    [languages: _languages, idCatalog: config.idCatalog]).groupBy {
                it.target.toString()
            }.each { k, v -> TranslationsRiverCache.instance.put(k, v) }
        }

        return Observable.from(Category.findAllByCatalogAndDeleted(Catalog.get(config.idCatalog), false))
    }

    @Override
    String getType(){
        'category'
    }

    @Override
    Item asItem(Category category, RiverConfig config) {
        new Item(id:category.id, type: getType(), map:RiverTools.asCategoryMap(category, config))
    }

    @Override
    List<String> previousProperties(){
        ['id', 'increments']
    }
}


