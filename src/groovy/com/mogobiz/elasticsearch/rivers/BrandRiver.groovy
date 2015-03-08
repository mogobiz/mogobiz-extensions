package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.AbstractRiverCache
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.rivers.spi.AbstractESRiver
import com.mogobiz.store.domain.Brand
import com.mogobiz.store.domain.Catalog
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESMapping
import com.mogobiz.elasticsearch.client.ESProperty
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
        return Observable.from(Brand.findAllByCompany(Catalog.get(config.idCatalog)?.company))
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

class BrandRiverCache extends AbstractRiverCache<Map>{
    private static BrandRiverCache brandRiverCache

    private BrandRiverCache(){}

    public static BrandRiverCache getInstance(){
        if(!brandRiverCache){
            brandRiverCache = new BrandRiverCache()
        }
        brandRiverCache
    }
}