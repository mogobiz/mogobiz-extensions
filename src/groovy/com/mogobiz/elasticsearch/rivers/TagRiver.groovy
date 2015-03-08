package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.AbstractRiverCache
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.rivers.spi.AbstractESRiver
import com.mogobiz.store.domain.Tag
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESMapping
import com.mogobiz.elasticsearch.client.ESProperty
import rx.Observable

/**
 * Created by stephane.manciot@ebiznext.com on 02/02/2014.
 */
class TagRiver extends AbstractESRiver<Tag>{

    @Override
    Observable<Tag> retrieveCatalogItems(final RiverConfig config){
        return Observable.from(Tag.executeQuery('SELECT DISTINCT p.tags FROM Product p WHERE p.category.catalog.id=:idCatalog', [idCatalog:config.idCatalog]))
//        DetachedCriteria<Product> query = Product.where{
//            category.catalog.id==config.idCatalog && state==ProductState.ACTIVE
//        }.distinct("tags")
//        return Observable.from(query.list().flatten() as Collection<Tag>)
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
        new Item(id:tag.id, type: getType(), map:RiverTools.asTagMap(tag, config))
    }

    @Override
    List<String> previousProperties(){
        ['id', 'increments']
    }

}

class TagRiverCache extends AbstractRiverCache<Map> {
    private static TagRiverCache tagRiverCache

    private TagRiverCache(){}

    public static TagRiverCache getInstance(){
        if(!tagRiverCache){
            tagRiverCache = new TagRiverCache()
        }
        tagRiverCache
    }
}
