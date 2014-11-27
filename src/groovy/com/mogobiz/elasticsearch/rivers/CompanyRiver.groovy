package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESMapping
import com.mogobiz.elasticsearch.client.ESProperty
import com.mogobiz.elasticsearch.rivers.spi.AbstractESRiver
import com.mogobiz.store.domain.Brand
import com.mogobiz.store.domain.Catalog
import com.mogobiz.store.domain.Company
import rx.Observable

/**
 * Created by stephane.manciot@ebiznext.com on 26/11/2014.
 */
class CompanyRiver extends AbstractESRiver<Company> {

    @Override
    ESMapping defineESMapping(){
        new ESMapping(type:getType(),
                timestamp:true,
                properties: [] << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
                        << new ESProperty(name:'code', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:false)
                        << new ESProperty(name:'uuid', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'aesPassword', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'imported', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        )
    }

    @Override
    Observable<Company> retrieveCatalogItems(final RiverConfig config){
        return Observable.from([Catalog.get(config.idCatalog)?.company])
    }

    @Override
    String getType(){
        'company'
    }

    @Override
    Item asItem(Company b, RiverConfig config) {
        new Item(id:b.code, type: getType(), map: RiverTools.asCompanyMap(b, config))
    }

}
