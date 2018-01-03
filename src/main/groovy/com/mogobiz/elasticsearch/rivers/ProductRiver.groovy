/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESMapping
import com.mogobiz.elasticsearch.client.ESProperty
import com.mogobiz.elasticsearch.rivers.cache.CategoryFeaturesRiverCache
import com.mogobiz.elasticsearch.rivers.cache.CouponsRiverCache
import com.mogobiz.elasticsearch.rivers.cache.TranslationsRiverCache
import com.mogobiz.elasticsearch.rivers.spi.AbstractESRiver
import com.mogobiz.store.domain.*
import com.mogobiz.tools.InnerSubscription
import org.hibernate.FlushMode
import org.springframework.transaction.TransactionDefinition
import rx.Observable
import rx.Observable.OnSubscribe
import rx.Subscriber
import rx.functions.Func1

/**
 */
class ProductRiver extends AbstractESRiver<Product>{

    @Override
    Item asItem(Product product, RiverConfig config) {
        new Item(id:product.id, type: getType(), map:
                Product.withTransaction([propagationBehavior: TransactionDefinition.PROPAGATION_SUPPORTS]){
                    RiverTools.asProductMap(product, config)
                }
        )
    }

    @Override
    ESMapping defineESMapping() {
        def countryProperties = []
        countryProperties << new ESProperty(name:'code', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        countryProperties << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)

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

        def poiProperties = []
        poiProperties << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
        poiProperties << new ESProperty(name:'description', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
        poiProperties << new ESProperty(name:'picture', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NO, multilang:false)
        poiProperties << new ESProperty(name:'xtype', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        poiProperties << new ESProperty(name:'location', type:ESClient.TYPE.OBJECT, properties: locationProperties)

        def categoryProperties = []
        categoryProperties << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
        categoryProperties << new ESProperty(name:'description', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
        categoryProperties << new ESProperty(name:'uuid', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        categoryProperties << new ESProperty(name:'keywords', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
        categoryProperties << new ESProperty(name:'path', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        categoryProperties << new ESProperty(name:'hide', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)

        def brandProperties = []
        brandProperties << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
        brandProperties << new ESProperty(name:'website', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NO, multilang:true)
        brandProperties << new ESProperty(name:'hide', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)

        def shippingProperties = []
        shippingProperties << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
        shippingProperties << new ESProperty(name:'weight', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NO, multilang:false)
        shippingProperties << new ESProperty(name:'width', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NO, multilang:false)
        shippingProperties << new ESProperty(name:'height', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NO, multilang:false)
        shippingProperties << new ESProperty(name:'depth', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NO, multilang:false)
        shippingProperties << new ESProperty(name:'amount', type:ESClient.TYPE.FLOAT, index:ESClient.INDEX.NO, multilang:false)
        shippingProperties << new ESProperty(name:'free', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        shippingProperties << new ESProperty(name:'weightUnit', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:true)
        shippingProperties << new ESProperty(name:'linearUnit', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:true)

        def featureProperties = []
        featureProperties << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
        featureProperties << new ESProperty(name:'position', type:ESClient.TYPE.INTEGER, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        featureProperties << new ESProperty(name:'domain', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
        featureProperties << new ESProperty(name:'uuid', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        featureProperties << new ESProperty(name:'hide', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        featureProperties << new ESProperty(name:'value', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)

        def variationValueProperties = []
        variationValueProperties << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
        variationValueProperties << new ESProperty(name:'position', type:ESClient.TYPE.INTEGER, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        variationValueProperties << new ESProperty(name:'uuid', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        variationValueProperties << new ESProperty(name:'hide', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        variationValueProperties << new ESProperty(name:'value', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)

        def stockCalendarProperties = []
        stockCalendarProperties << new ESProperty(name:'stock', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        stockCalendarProperties << new ESProperty(name:'startDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)

        def datePeriodProperties = []
        datePeriodProperties << new ESProperty(name:'startDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        datePeriodProperties << new ESProperty(name:'endDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)

        def intraDayPeriodProperties = []
        intraDayPeriodProperties << new ESProperty(name:'weekday1', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        intraDayPeriodProperties << new ESProperty(name:'weekday2', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        intraDayPeriodProperties << new ESProperty(name:'weekday3', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        intraDayPeriodProperties << new ESProperty(name:'weekday4', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        intraDayPeriodProperties << new ESProperty(name:'weekday5', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        intraDayPeriodProperties << new ESProperty(name:'weekday6', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        intraDayPeriodProperties << new ESProperty(name:'weekday7', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        intraDayPeriodProperties << new ESProperty(name:'startDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        intraDayPeriodProperties << new ESProperty(name:'endDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)

        def promotionProperties = []
        promotionProperties << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
        promotionProperties << new ESProperty(name:'description', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
        promotionProperties << new ESProperty(name:'reduction', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        promotionProperties << new ESProperty(name:'pastille', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)

        def couponsProperties = []
        couponsProperties << new ESProperty(name:'id', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
//        couponsProperties << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
//        couponsProperties << new ESProperty(name:'description', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)

        def resourceProperties = []
        resourceProperties << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
        resourceProperties << new ESProperty(name:'description', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
        resourceProperties << new ESProperty(name:'xtype', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        resourceProperties << new ESProperty(name:'url', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NO, multilang:false)
        resourceProperties << new ESProperty(name:'active', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NO, multilang:false)
        resourceProperties << new ESProperty(name:'deleted', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NO, multilang:false)
        resourceProperties << new ESProperty(name:'uploaded', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NO, multilang:false)
        resourceProperties << new ESProperty(name:'contentType', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        resourceProperties << new ESProperty(name:'smallPicture', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NO, multilang:false)
        resourceProperties << new ESProperty(name:'sanitizedName', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NO, multilang:false)
        resourceProperties << new ESProperty(name:'content', type:ESClient.TYPE.BINARY, index:ESClient.INDEX.NO, multilang:false)
        resourceProperties << new ESProperty(name:'uuid', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        resourceProperties << new ESProperty(name:'md5', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)

        def byDateTimeProperties = []
        byDateTimeProperties << new ESProperty(name:'id', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        byDateTimeProperties << new ESProperty(name:'uuid', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        byDateTimeProperties << new ESProperty(name:'stock', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        byDateTimeProperties << new ESProperty(name:'startDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        byDateTimeProperties << new ESProperty(name:'dateCreated', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        byDateTimeProperties << new ESProperty(name:'lastUpdated', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        byDateTimeProperties << new ESProperty(name:'available', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)

        def skuProperties = []
        skuProperties << new ESProperty(name:'id', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        skuProperties << new ESProperty(name:'uuid', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        skuProperties << new ESProperty(name:'sku', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        skuProperties << new ESProperty(name:'price', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        skuProperties << new ESProperty(name:'minOrder', type:ESClient.TYPE.INTEGER, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        skuProperties << new ESProperty(name:'maxOrder', type:ESClient.TYPE.INTEGER, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        skuProperties << new ESProperty(name:'nbSales', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        skuProperties << new ESProperty(name:'startDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        skuProperties << new ESProperty(name:'stopDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        skuProperties << new ESProperty(name:'xprivate', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        skuProperties << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
        skuProperties << new ESProperty(name:'description', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
        skuProperties << new ESProperty(name:'picture', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NO, multilang:false) // maybe we should use attachment type
        skuProperties << new ESProperty(name:'resources', type:ESClient.TYPE.OBJECT, properties: resourceProperties)
        skuProperties << new ESProperty(name:'position', type:ESClient.TYPE.INTEGER, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        skuProperties << new ESProperty(name:'variation1', type:ESClient.TYPE.OBJECT, properties: variationValueProperties)
        skuProperties << new ESProperty(name:'variation2', type:ESClient.TYPE.OBJECT, properties: variationValueProperties)
        skuProperties << new ESProperty(name:'variation3', type:ESClient.TYPE.OBJECT, properties: variationValueProperties)
        skuProperties << new ESProperty(name:'coupons', type:ESClient.TYPE.OBJECT, properties: couponsProperties)
        skuProperties << new ESProperty(name:'salePrice', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        skuProperties << new ESProperty(name:'promotion', type:ESClient.TYPE.OBJECT, properties: promotionProperties)
        skuProperties << new ESProperty(name:'promotions', type:ESClient.TYPE.OBJECT, properties: promotionProperties)
        skuProperties << new ESProperty(name:'available', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        skuProperties << new ESProperty(name:'stockDisplay', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        skuProperties << new ESProperty(name:'calendarType', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        skuProperties << new ESProperty(name:'initialStock', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        skuProperties << new ESProperty(name:'stockUnlimited', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        skuProperties << new ESProperty(name:'stockOutSelling', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        skuProperties << new ESProperty(name:'stock', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        skuProperties << new ESProperty(name:'byDateTimes', type:ESClient.TYPE.NESTED, properties: byDateTimeProperties)
        skuProperties << new ESProperty(name:'miraklOffer', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        skuProperties << new ESProperty(name:'miraklOfferId', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)

        def tagProperties = []
        tagProperties << new ESProperty(name:'id', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        tagProperties << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)

        def localTaxRatesProperties = []
        localTaxRatesProperties << new ESProperty(name:'rate', type:ESClient.TYPE.FLOAT, index:ESClient.INDEX.NO, multilang:false)
        localTaxRatesProperties << new ESProperty(name:'countryCode', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        localTaxRatesProperties << new ESProperty(name:'stateCode', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)

        def taxRateProperties = []
        taxRateProperties << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        taxRateProperties << new ESProperty(name:'localTaxRates', type:ESClient.TYPE.OBJECT, properties: localTaxRatesProperties)

        def iBeaconProperties = []
        iBeaconProperties << new ESProperty(name:'uuid', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        iBeaconProperties << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        iBeaconProperties << new ESProperty(name:'startDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        iBeaconProperties << new ESProperty(name:'endDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        iBeaconProperties << new ESProperty(name:'active', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        iBeaconProperties << new ESProperty(name:'major', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        iBeaconProperties << new ESProperty(name:'minor', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)

        def notationsProperties = []
        notationsProperties << new ESProperty(name:'notation', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:false)
        notationsProperties << new ESProperty(name:'nbcomments', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)

        new ESMapping(type:getType(),
                timestamp:true,
                properties: []
                        << new ESProperty(name:'code', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
                        << new ESProperty(name:'description', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
                        << new ESProperty(name:'descriptionAsText', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
                        << new ESProperty(name:'xtype', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'price', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'startDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'stopDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'startFeatureDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'stopFeatureDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'nbSales', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'picture', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NO, multilang:false) // maybe we should use attachment type
                        << new ESProperty(name:'smallPicture', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NO, multilang:false) // maybe we should use attachment type
                        << new ESProperty(name:'stockDisplay', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'calendarType', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'uuid', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'hide', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'sanitizedName', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NO, multilang:false)
                        << new ESProperty(name:'keywords', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
                        << new ESProperty(name:'poi', type:ESClient.TYPE.OBJECT, properties: poiProperties)
                        << new ESProperty(name:'category', type:ESClient.TYPE.OBJECT, properties: categoryProperties)
                        << new ESProperty(name:'brand', type:ESClient.TYPE.OBJECT, properties: brandProperties)
                        << new ESProperty(name:'shipping', type:ESClient.TYPE.OBJECT, properties: shippingProperties)
                        << new ESProperty(name:'features', type:ESClient.TYPE.NESTED, properties: featureProperties)
                        << new ESProperty(name:'skus', type:ESClient.TYPE.OBJECT, properties: skuProperties)
                        << new ESProperty(name:'datePeriods', type:ESClient.TYPE.OBJECT, properties: datePeriodProperties)
                        << new ESProperty(name:'intraDayPeriods', type:ESClient.TYPE.OBJECT, properties: intraDayPeriodProperties)
                        << new ESProperty(name:'resources', type:ESClient.TYPE.OBJECT, properties: resourceProperties)
                        << new ESProperty(name:'tags', type:ESClient.TYPE.NESTED, properties: tagProperties)
                        << new ESProperty(name:'taxRate', type:ESClient.TYPE.OBJECT, properties: taxRateProperties)
                        << new ESProperty(name:'imported', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'increments', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'ibeacon', type:ESClient.TYPE.OBJECT, properties: iBeaconProperties)
                        << new ESProperty(name:'availabilityDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'dateCreated', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'lastUpdated', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'salePrice', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'promotion', type:ESClient.TYPE.OBJECT, properties: promotionProperties)
                        << new ESProperty(name:'promotions', type:ESClient.TYPE.OBJECT, properties: promotionProperties)
                        << new ESProperty(name:'notations', type:ESClient.TYPE.NESTED, properties: notationsProperties)
                        << new ESProperty(name:'viewed', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'purchased', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'similar', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'popularity', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'coupons', type:ESClient.TYPE.OBJECT, properties: couponsProperties)
                        << new ESProperty(name:'stockAvailable', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        )
    }

    @Override
    Observable<Product> retrieveCatalogItems(RiverConfig config) {
        Calendar now = Calendar.getInstance()
        def languages = config?.languages ?: ['fr', 'en', 'es', 'de']
        def defaultLang = config?.defaultLang ?: 'fr'
        def _defaultLang = defaultLang.trim().toLowerCase()
        def _languages = languages.collect {it.trim().toLowerCase()} - _defaultLang
        final args = [readOnly: true, flushMode: FlushMode.MANUAL]
        if(!_languages.flatten().isEmpty()){
            Set<Translation> translations = []
            if(config.partial){
                translations << Translation.executeQuery('select t from Product p, Translation t where t.target=p.id and t.lang in :languages and (p.id in (:idProducts) and p.state=:productState)',
                        [languages:_languages, idProducts:config.idProducts, productState:ProductState.ACTIVE], args)
                translations << Translation.executeQuery('select t from Product p left join p.features as f, Translation t where t.target=f.id and t.lang in :languages and (p.id in (:idProducts) and p.state=:productState)',
                        [languages:_languages, idProducts:config.idProducts, productState:ProductState.ACTIVE], args)
                translations << Translation.executeQuery('select t from Product p left join p.featureValues as fv, Translation t where t.target=fv.id and t.lang in :languages and (p.id in (:idProducts) and p.state=:productState)',
                        [languages:_languages, idProducts:config.idProducts, productState:ProductState.ACTIVE], args)
                translations << Translation.executeQuery('select t from TicketType sku, Translation t where t.target=sku.id and t.lang in :languages and (sku.product.id in (:idProducts) and sku.product.state=:productState and (sku.stopDate is null or sku.stopDate >= :today))',
                        [languages:_languages, idProducts:config.idProducts, productState:ProductState.ACTIVE, today: now], args)
                translations << Translation.executeQuery('select t from TicketType sku left join sku.variation1 as v1 left outer join sku.variation2 as v2 left outer join sku.variation3 as v3, Translation t where (t.target=v1.id or t.target=v1.variation.id or (v2 != null and (t.target=v2.id or t.target=v2.variation.id)) or (v3 != null and t.target=v3.id or t.target=v3.variation.id)) and t.lang in :languages and (sku.product.id in (:idProducts) and sku.product.state=:productState and (sku.stopDate is null or sku.stopDate >= :today) and (sku.product.stopDate is null or sku.product.stopDate >= :today))',
                        [languages:_languages, idProducts:config.idProducts, productState:ProductState.ACTIVE, today: now], args)
                translations << Translation.executeQuery('select t from Category cat, Translation t where t.target=cat.id and t.lang in :languages and cat.id in (:idCategories)',
                        [languages:_languages, idCategories:config.idCategories], args)
                translations << Translation.executeQuery('select t from Category cat left join cat.features as f, Translation t where t.target=f.id and t.lang in :languages and cat.id in (:idCategories)',
                        [languages:_languages, idCategories:config.idCategories], args)
                translations << Translation.executeQuery('select t from Brand brand, Translation t where t.target=brand.id and t.lang in :languages and brand.company.id=:idCompany',
                        [languages:_languages, idCompany:config.idCompany], args)
                translations << Translation.executeQuery('select t from Tag tag, Translation t where t.target=tag.id and t.lang in :languages and tag.company.id=:idCompany',
                        [languages:_languages, idCompany:config.idCompany], args)
                translations << Translation.executeQuery('select t from Product2Resource pr left join pr.product as p left join pr.resource as r, Translation t where t.target=r.id and t.lang in :languages and (p.id in (:idProducts) and p.state=:productState)',
                        [languages:_languages, idProducts:config.idProducts, productState:ProductState.ACTIVE], args)
                translations << Translation.executeQuery('select t from Product p left join p.shipping as s, Translation t where t.target=s.id and t.lang in :languages and (p.id in (:idProducts) and p.state=:productState)',
                        [languages:_languages, idProducts:config.idProducts, productState:ProductState.ACTIVE], args)
                translations << Translation.executeQuery('select t from Product p left join p.poi as poi, Translation t where t.target=poi.id and t.lang in :languages and (p.id in (:idProducts) and p.state=:productState)',
                        [languages:_languages, idProducts:config.idProducts, productState:ProductState.ACTIVE], args)
                translations << Translation.executeQuery('select t from Coupon coupon join coupon.products as p, Translation t where t.target=coupon.id and t.lang in :languages and (p.id in (:idProducts) and p.state=:productState)',
                        [languages:_languages, idProducts:config.idProducts, productState:ProductState.ACTIVE], args)
                translations << Translation.executeQuery('select t from Coupon coupon join coupon.categories as category, Translation t where t.target=coupon.id and t.lang in :languages and (category.id in (:idCategories) and coupon.active=true)',
                        [languages:_languages, idCategories:config.idCategories], args)
                translations << Translation.executeQuery('select t from Coupon coupon join coupon.ticketTypes as ticketType, Translation t where t.target=coupon.id and t.lang in :languages and (ticketType.product.id in (:idProducts) and ticketType.product.state=:productState and (ticketType.stopDate is null or ticketType.stopDate >= :today) and coupon.active=true)',
                        [languages:_languages, idProducts:config.idProducts, productState:ProductState.ACTIVE, today: now], args)
                translations << Translation.executeQuery('select t from Coupon coupon join coupon.catalogs as catalog, Translation t where t.target=coupon.id and t.lang in :languages and (catalog.id in (:idCatalogs) and coupon.active=true)',
                        [languages:_languages, idCatalogs:config.idCatalogs], [readOnly: true, flushMode: FlushMode.MANUAL])
                translations << Translation.executeQuery('select t from Product p left join p.productProperties as pp, Translation t where t.target=pp.id and t.lang in :languages and (p.id in (:idProducts) and p.state=:productState)',
                        [languages:_languages, idProducts:config.idProducts, productState:ProductState.ACTIVE], args)
            }
            else{
                translations << Translation.executeQuery('select t from Product p, Translation t where t.target=p.id and t.lang in :languages and (p.category.catalog.id in (:idCatalogs) and p.state=:productState)',
                        [languages:_languages, idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE], args)
                translations << Translation.executeQuery('select t from Product p left join p.features as f, Translation t where t.target=f.id and t.lang in :languages and (p.category.catalog.id in (:idCatalogs) and p.state=:productState)',
                        [languages:_languages, idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE], args)
                translations << Translation.executeQuery('select t from Product p left join p.featureValues as fv, Translation t where t.target=fv.id and t.lang in :languages and (p.category.catalog.id in (:idCatalogs) and p.state=:productState)',
                        [languages:_languages, idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE], args)
                translations << Translation.executeQuery('select t from TicketType sku, Translation t where t.target=sku.id and t.lang in :languages and (sku.product.category.catalog.id in (:idCatalogs) and sku.product.state=:productState and (sku.stopDate is null or sku.stopDate >= :today))',
                        [languages:_languages, idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE, today: now], args)
                translations << Translation.executeQuery('select t from TicketType sku left join sku.variation1 as v1 left outer join sku.variation2 as v2 left outer join sku.variation3 as v3, Translation t where (t.target=v1.id or t.target=v1.variation.id or (v2 != null and (t.target=v2.id or t.target=v2.variation.id)) or (v3 != null and t.target=v3.id or t.target=v3.variation.id)) and t.lang in :languages and (sku.product.category.catalog.id in (:idCatalogs) and sku.product.state=:productState and (sku.stopDate is null or sku.stopDate >= :today) and (sku.product.stopDate is null or sku.product.stopDate >= :today))',
                        [languages:_languages, idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE, today: now], args)
                translations << Translation.executeQuery('select t from Category cat, Translation t where t.target=cat.id and t.lang in :languages and cat.catalog.id in (:idCatalogs)',
                        [languages:_languages, idCatalogs:config.idCatalogs], args)
                translations << Translation.executeQuery('select t from Category cat left join cat.features as f, Translation t where t.target=f.id and t.lang in :languages and cat.catalog.id in (:idCatalogs)',
                        [languages:_languages, idCatalogs:config.idCatalogs], args)
                translations << Translation.executeQuery('select t from Brand brand, Translation t where t.target=brand.id and t.lang in :languages and brand.company in (select c.company from Catalog c where c.id in (:idCatalogs))',
                        [languages:_languages, idCatalogs:config.idCatalogs], args)
                translations << Translation.executeQuery('select t from Tag tag, Translation t where t.target=tag.id and t.lang in :languages and tag.company in (select c.company from Catalog c where c.id in (:idCatalogs))',
                        [languages:_languages, idCatalogs:config.idCatalogs], args)
                translations << Translation.executeQuery('select t from Product2Resource pr left join pr.product as p left join pr.resource as r, Translation t where t.target=r.id and t.lang in :languages and (p.category.catalog.id in (:idCatalogs) and p.state=:productState)',
                        [languages:_languages, idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE], args)
                translations << Translation.executeQuery('select t from Product p left join p.shipping as s, Translation t where t.target=s.id and t.lang in :languages and (p.category.catalog.id in (:idCatalogs) and p.state=:productState)',
                        [languages:_languages, idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE], args)
                translations << Translation.executeQuery('select t from Product p left join p.poi as poi, Translation t where t.target=poi.id and t.lang in :languages and (p.category.catalog.id in (:idCatalogs) and p.state=:productState)',
                        [languages:_languages, idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE], args)
                translations << Translation.executeQuery('select t from Coupon coupon join coupon.products as p, Translation t where t.target=coupon.id and t.lang in :languages and (p.category.catalog.id in (:idCatalogs) and p.state=:productState)',
                        [languages:_languages, idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE], args)
                translations << Translation.executeQuery('select t from Coupon coupon join coupon.categories as category, Translation t where t.target=coupon.id and t.lang in :languages and (category.catalog.id in (:idCatalogs) and coupon.active=true)',
                        [languages:_languages, idCatalogs:config.idCatalogs], args)
                translations << Translation.executeQuery('select t from Coupon coupon join coupon.ticketTypes as ticketType, Translation t where t.target=coupon.id and t.lang in :languages and (ticketType.product.category.catalog.id in (:idCatalogs) and ticketType.product.state=:productState and (ticketType.stopDate is null or ticketType.stopDate >= :today) and coupon.active=true)',
                        [languages:_languages, idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE, today: now], args)
                translations << Translation.executeQuery('select t from Coupon coupon join coupon.catalogs as catalog, Translation t where t.target=coupon.id and t.lang in :languages and (catalog.id in (:idCatalogs) and coupon.active=true)',
                        [languages:_languages, idCatalogs:config.idCatalogs], args)
                translations << Translation.executeQuery('select t from Product p left join p.productProperties as pp, Translation t where t.target=pp.id and t.lang in :languages and (p.category.catalog.id in (:idCatalogs) and p.state=:productState)',
                        [languages:_languages, idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE], args)
            }
            translations.flatten().groupBy {"${it.target}"}.each {k, v ->
                TranslationsRiverCache.instance.put(k, v)
            }
        }

        def categories = config.partial ? Category.executeQuery('select cat FROM Category cat left join fetch cat.features where cat.id in (:idCategories)',
                [idCategories:config.idCategories], args) : Category.executeQuery('select cat FROM Category cat left join fetch cat.features where cat.catalog.id in (:idCatalogs)',
                [idCatalogs:config.idCatalogs], args)
        categories.each {CategoryFeaturesRiverCache.instance.put(it.uuid, it.features)}

        def productCoupons = config.partial ? Coupon.executeQuery('select product, coupon FROM Coupon coupon left join fetch coupon.rules left join coupon.products as product where (product.id in (:idProducts) and product.state=:productState and coupon.active=true)',
                [idProducts:config.idProducts, productState:ProductState.ACTIVE], args) : Coupon.executeQuery('select product, coupon FROM Coupon coupon left join fetch coupon.rules left join coupon.products as product where (product.category.catalog.id in (:idCatalogs) and product.state=:productState and coupon.active=true)',
                [idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE], args)
        productCoupons.each { a ->
            def key = (a[0] as Product).uuid
            Set<Coupon> coupons = CouponsRiverCache.instance.get(key) as Set<Coupon> ?: []
            coupons.add(a[1] as Coupon)
            CouponsRiverCache.instance.put(key, coupons)
        }

        def skuCoupons = config.partial ? Coupon.executeQuery('select ticketType, coupon FROM Coupon coupon left join fetch coupon.rules left join coupon.ticketTypes as ticketType left join ticketType.product as product where (product.id in (:idProducts) and product.state=:productState and (ticketType.stopDate is null or ticketType.stopDate >= :today) and coupon.active=true)',
                [idProducts:config.idProducts, productState:ProductState.ACTIVE, today: now], args) : Coupon.executeQuery('select ticketType, coupon FROM Coupon coupon left join fetch coupon.rules left join coupon.ticketTypes as ticketType left join ticketType.product as product where (product.category.catalog.id in (:idCatalogs) and product.state=:productState and (ticketType.stopDate is null or ticketType.stopDate >= :today) and coupon.active=true)',
                [idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE, today: now], args)
        skuCoupons.each { a ->
            def key = (a[0] as TicketType).uuid
            Set<Coupon> coupons = CouponsRiverCache.instance.get(key) as Set<Coupon> ?: []
            coupons.add(a[1] as Coupon)
            CouponsRiverCache.instance.put(key, coupons)
        }

        def categoryCoupons = config.partial ? Coupon.executeQuery('select category, coupon FROM Coupon coupon left join fetch coupon.rules left join coupon.categories as category where category.id in (:idCategories) and coupon.active=true',
                [idCategories:config.idCategories], args) : Coupon.executeQuery('select category, coupon FROM Coupon coupon left join fetch coupon.rules left join coupon.categories as category where category.catalog.id in (:idCatalogs) and coupon.active=true',
                [idCatalogs:config.idCatalogs], args)
        categoryCoupons.each { a ->
            def key = (a[0] as Category).uuid
            Set<Coupon> coupons = CouponsRiverCache.instance.get(key) as Set<Coupon> ?: []
            coupons.add(a[1] as Coupon)
            CouponsRiverCache.instance.put(key, coupons)
        }

        def catalogCoupons = Coupon.executeQuery('select catalog, coupon FROM Coupon coupon left join fetch coupon.rules left join coupon.catalogs as catalog where catalog.id in (:idCatalogs) and coupon.active=true',
                [idCatalogs:config.idCatalogs], args)
        catalogCoupons?.each { a ->
            def key = (a[0] as Catalog).uuid
            Set<Coupon> coupons = CouponsRiverCache.instance.get(key) as Set<Coupon> ?: []
            coupons.add(a[1] as Coupon)
            CouponsRiverCache.instance.put(key, coupons)
        }

        Observable.from(config.partial ?
                config.idProducts as List<Long> :
                Product.executeQuery(
                        'select p.id from Product p WHERE p.category.catalog.id in (:idCatalogs) and p.state = :productState and p.deleted = false and p.ticketTypes.size > 0',
                        [idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE],
                        args
                ) as List<Long>).buffer(config.bulkSize).flatMap(new Func1<List<Long>, Observable<Product>>() {
            @Override
            Observable<Product> call(List<Long> sub) {
                return Observable.from(
                        Product.executeQuery('SELECT p FROM Product p ' +
                                'left join fetch p.features ' +
                                'left join fetch p.featureValues ' +
                                'left join fetch p.productProperties ' +
                                'left join fetch p.product2Resources as pr ' +
                                'left join fetch pr.resource ' +
                                'left join fetch p.intraDayPeriods ' +
                                'left join fetch p.datePeriods ' +
                                'left join fetch p.tags ' +
                                'left join fetch p.ticketTypes as sku ' +
                                'left join fetch sku.variation1 v1 ' +
                                'left join fetch v1.variation ' +
                                'left join fetch sku.variation2 v2 ' +
                                'left join fetch v2.variation ' +
                                'left join fetch sku.variation3 v3 ' +
                                'left join fetch v3.variation ' +
                                'left join fetch sku.stock ' +
                                'left join fetch sku.stockCalendars ' +
                                'left join fetch p.poi ' +
                                'left join fetch p.category as category ' +
                                'left join fetch category.parent ' +
                                'left join fetch p.brand as brand ' +
                                'left join fetch brand.brandProperties ' +
                                'left join fetch p.shipping ' +
                                'left join fetch p.taxRate as taxRate ' +
                                'left join fetch taxRate.localTaxRates ' +
                                'left join fetch p.ibeacon ' +
                                'left join fetch p.company ' +
                                'WHERE p.id in (:idProducts) and p.state = :productState and p.deleted = false and p.ticketTypes.size > 0',
                                [idProducts: sub, productState:ProductState.ACTIVE], args
                        ))
            }
        })

    }

    @Override
    String getType() {
        return 'product'
    }

    @Override
    String getUuid(Product p){
        p.uuid
    }

}
