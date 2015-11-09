/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESMapping
import com.mogobiz.elasticsearch.client.ESProperty
import com.mogobiz.elasticsearch.rivers.spi.AbstractESRiver
import com.mogobiz.store.domain.Catalog
import com.mogobiz.store.domain.Company
import org.hibernate.FlushMode
import org.springframework.transaction.TransactionDefinition
import rx.Observable

/**
 *
 * Created by stephane.manciot@ebiznext.com on 26/11/2014.
 */
class CompanyRiver extends AbstractESRiver<Company> {

    @Override
    ESMapping defineESMapping(){

        def countryProperties = []
        countryProperties << new ESProperty(name:'code', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
//        countryProperties << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)

        def locationProperties = []
        locationProperties << new ESProperty(name:'latitude', type:ESClient.TYPE.DOUBLE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        locationProperties << new ESProperty(name:'longitude', type:ESClient.TYPE.DOUBLE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        locationProperties << new ESProperty(name:'road1', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NO, multilang:false)
        locationProperties << new ESProperty(name:'road2', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NO, multilang:false)
        locationProperties << new ESProperty(name:'road3', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NO, multilang:false)
        locationProperties << new ESProperty(name:'roadNum', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NO, multilang:false)
        locationProperties << new ESProperty(name:'postalCode', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        locationProperties << new ESProperty(name:'state', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:false)
        locationProperties << new ESProperty(name:'city', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:false)
        locationProperties << new ESProperty(name:'country', type:ESClient.TYPE.OBJECT, properties: countryProperties)

        new ESMapping(type:getType(),
                timestamp:true,
                properties: [] << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
                        << new ESProperty(name:'code', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:false)
                        << new ESProperty(name:'uuid', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'aesPassword', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'phone', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'imported', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'shipFrom', type:ESClient.TYPE.OBJECT, properties: locationProperties)
                        << new ESProperty(name:'location', type:ESClient.TYPE.OBJECT, properties: locationProperties)
                        << new ESProperty(name:'shippingInternational', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        )
    }

    @Override
    Observable<Company> retrieveCatalogItems(final RiverConfig config){
        final idCompany = Catalog.load(config.idCatalog)?.company?.id
        Observable.from(
                Company.executeQuery(
                        "SELECT c FROM Company c left join fetch c.shipFrom left join fetch c.location WHERE c.id =:idCompany",
                        [idCompany: idCompany],
                        [
                                readOnly: true,
                                flushMode: FlushMode.MANUAL
                        ]
                )
        )
    }

    @Override
    String getType(){
        'company'
    }

    @Override
    Item asItem(Company b, RiverConfig config) {
        new Item(id:b.code, type: getType(), map:
                Company.withTransaction([propagationBehavior: TransactionDefinition.PROPAGATION_SUPPORTS]) {
                    RiverTools.asCompanyMap(b, config)
                }
        )
    }

    @Override
    String getUuid(Company c){
        c.uuid
    }

}
