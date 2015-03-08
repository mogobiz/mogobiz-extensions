package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.rivers.spi.AbstractESRiver
import com.mogobiz.store.domain.*
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESMapping
import com.mogobiz.elasticsearch.client.ESProperty
import rx.Observable

/**
 * Created by stephane.manciot@ebiznext.com on 18/02/2014.
 */
class ProductRiver extends AbstractESRiver<Product>{

    @Override
    Item asItem(Product product, RiverConfig config) {
        new Item(id:product.id, type: getType(), map:RiverTools.asProductMap(product, config))
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
                        << new ESProperty(name:'code', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:false)
                        << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
                        << new ESProperty(name:'description', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
                        << new ESProperty(name:'descriptionAsText', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
                        << new ESProperty(name:'xtype', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'price', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'startDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'stopDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'startFeatureDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'stopFeatureDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'nbSales', type:ESClient.TYPE.LONG, index:ESClient.INDEX.ANALYZED, multilang:false)
                        << new ESProperty(name:'picture', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NO, multilang:false) // maybe we should use attachment type
                        << new ESProperty(name:'smallPicture', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NO, multilang:false) // maybe we should use attachment type
                        << new ESProperty(name:'stockDisplay', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'calendarType', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'uuid', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'hide', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'sanitizedName', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
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
                        << new ESProperty(name:'notations', type:ESClient.TYPE.NESTED, properties: notationsProperties)
                        << new ESProperty(name:'viewed', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'purchased', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'similar', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'popularity', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'coupons', type:ESClient.TYPE.OBJECT, properties: couponsProperties)
        )
    }

    @Override
    Observable<Product> retrieveCatalogItems(RiverConfig config) {
        Category.executeQuery('select cat FROM Category cat left join fetch cat.features where cat.catalog.id=:idCatalog',
                [idCatalog:config.idCatalog]).each {CategoryFeaturesRiverCache.instance.put(it.uuid, it.features)}

        def couponsMap = [:]

        Coupon.executeQuery('select product, coupon FROM Coupon coupon left join fetch coupon.rules left join coupon.products as product where (product.category.catalog.id=:idCatalog and product.state=:productState)',
                [idCatalog:config.idCatalog, productState:ProductState.ACTIVE]).each {a ->
            def key = (a[0] as Product).uuid
            Set<Coupon> coupons = couponsMap.get(key) as Set<Coupon> ?: []
            coupons.add(a[1] as Coupon)
            couponsMap.put(key, coupons)
        }

        Coupon.executeQuery('select ticketType, coupon FROM Coupon coupon left join fetch coupon.rules left join coupon.ticketTypes as ticketType left join ticketType.product as product where (product.category.catalog.id=:idCatalog and product.state=:productState)',
                [idCatalog:config.idCatalog, productState:ProductState.ACTIVE]).each {a ->
            def key = (a[0] as TicketType).uuid
            Set<Coupon> coupons = couponsMap.get(key) as Set<Coupon> ?: []
            coupons.add(a[1] as Coupon)
            couponsMap.put(key, coupons)
        }

        Coupon.executeQuery('select category, coupon FROM Coupon coupon left join fetch coupon.rules left join coupon.categories as category where category.catalog.id=:idCatalog',
                [idCatalog:config.idCatalog]).each {a ->
            def key = (a[0] as Category).uuid
            Set<Coupon> coupons = couponsMap.get(key) as Set<Coupon> ?: []
            coupons.add(a[1] as Coupon)
            couponsMap.put(key, coupons)
        }

        couponsMap.each {k, v ->
            CouponsRiverCache.instance.put(k as String, v as Set<Coupon>)
        }

        return Observable.from(Product.executeQuery(
                'SELECT p FROM Product p ' +
            'left join fetch p.ticketTypes ' +
                        'left join fetch p.features ' +
                        'left join fetch p.featureValues ' +
                        'left join fetch p.productProperties ' +
                        'left join fetch p.product2Resources as pr ' +
                        'left join fetch pr.resource ' +
                        'left join fetch p.intraDayPeriods ' +
                        'left join fetch p.datePeriods ' +
                        'left join fetch p.tags ' +
                        'left join fetch p.ticketTypes ' +
                        'left join fetch p.poi ' +
                        'left join fetch p.category as category ' +
                        'left join fetch category.parent ' +
                        'left join fetch p.brand ' +
                        'left join fetch p.shipping ' +
                        'left join fetch p.taxRate ' +
                        'left join fetch p.ibeacon ' +
                        'left join fetch p.company ' +
                        'WHERE p.category.catalog.id=:idCatalog and p.state = :productState and p.deleted = false',
                [idCatalog:config.idCatalog, productState:ProductState.ACTIVE]
        ))
    }

    @Override
    String getType() {
        return 'product'
    }

    @Override
    List<String> previousProperties(){
        ['id', 'increments', 'notations']
    }
}
