package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.rivers.cache.TranslationsRiverCache
import com.mogobiz.elasticsearch.rivers.spi.AbstractESRiver
import com.mogobiz.store.domain.Brand
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESMapping
import com.mogobiz.elasticsearch.client.ESProperty
import com.mogobiz.store.domain.Translation
import rx.Observable

/**
 * Created by stephane.manciot@ebiznext.com on 02/02/2014.
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
        )
    }

    @Override
    Observable<Brand> retrieveCatalogItems(final RiverConfig config){
        def languages = config?.languages ?: ['fr', 'en', 'es', 'de']
        def defaultLang = config?.defaultLang ?: 'fr'
        def _defaultLang = defaultLang.trim().toLowerCase()
        def _languages = languages.collect {it.trim().toLowerCase()} - _defaultLang
        if(!_languages.flatten().isEmpty()) {
            Translation.executeQuery('select t from Brand brand, Translation t where t.target=brand.id and t.lang in :languages and brand.company in (select c.company from Catalog c where c.id=:idCatalog)',
                    [languages: _languages, idCatalog: config.idCatalog]).groupBy {
                it.target.toString()
            }.each { k, v -> TranslationsRiverCache.instance.put(k, v) }
        }
        return Observable.from(Brand.executeQuery('select brand from Brand brand left join fetch brand.brandProperties where brand.company in (select c.company from Catalog c where c.id=:idCatalog)',
                [idCatalog:config.idCatalog]))
    }

    @Override
    String getType(){
        'brand'
    }

    @Override
    Item asItem(Brand b, RiverConfig config) {
        new Item(id:b.id, type: getType(), map: RiverTools.asBrandMap(b, config))
    }

    @Override
    List<String> previousProperties(){
        ['id', 'increments']
    }

}

