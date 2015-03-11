package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.rivers.cache.TranslationsRiverCache
import com.mogobiz.elasticsearch.rivers.spi.AbstractESRiver
import com.mogobiz.store.domain.Tag
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESMapping
import com.mogobiz.elasticsearch.client.ESProperty
import com.mogobiz.store.domain.Translation
import org.hibernate.FlushMode
import org.springframework.transaction.TransactionDefinition
import rx.Observable

/**
 * Created by stephane.manciot@ebiznext.com on 02/02/2014.
 */
class TagRiver extends AbstractESRiver<Tag>{

    @Override
    Observable<Tag> retrieveCatalogItems(final RiverConfig config){
        def languages = config?.languages ?: ['fr', 'en', 'es', 'de']
        def defaultLang = config?.defaultLang ?: 'fr'
        def _defaultLang = defaultLang.trim().toLowerCase()
        def _languages = languages.collect {it.trim().toLowerCase()} - _defaultLang
        if(!_languages.flatten().isEmpty()) {
            Translation.executeQuery('select t from Tag tag, Translation t where t.target=tag.id and t.lang in :languages and tag.company in (select c.company from Catalog c where c.id=:idCatalog)',
                    [languages: _languages, idCatalog: config.idCatalog], [flushMode: FlushMode.MANUAL]).groupBy {
                it.target.toString()
            }.each { k, v -> TranslationsRiverCache.instance.put(k, v) }
        }
        return Observable.from(Tag.executeQuery('SELECT DISTINCT p.tags FROM Product p WHERE p.category.catalog.id=:idCatalog',
                [idCatalog:config.idCatalog], [flushMode: FlushMode.MANUAL]))
    }

    @Override
    ESMapping defineESMapping(){
        new ESMapping(type:getType(),
                timestamp:true,
                properties: [] << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
                        << new ESProperty(name:'imported', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'increments', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
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
                    RiverTools.asTagMap(tag, config)
                }
        )
    }

    @Override
    List<String> previousProperties(){
        ['id', 'increments']
    }

    @Override
    String getUuid(Tag t){
        t.uuid
    }

}

