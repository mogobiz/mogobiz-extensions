/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESMapping
import com.mogobiz.elasticsearch.client.ESProperty
import com.mogobiz.elasticsearch.rivers.cache.CouponsRiverCache
import com.mogobiz.elasticsearch.rivers.cache.TranslationsRiverCache
import com.mogobiz.elasticsearch.rivers.spi.AbstractESRiver
import com.mogobiz.store.domain.Catalog
import com.mogobiz.store.domain.Category
import com.mogobiz.store.domain.Coupon
import com.mogobiz.store.domain.Product
import com.mogobiz.store.domain.ProductState
import com.mogobiz.store.domain.TicketType
import com.mogobiz.store.domain.Translation
import org.hibernate.FlushMode
import org.springframework.transaction.TransactionDefinition
import rx.Observable

/**
 *
 */
class SkuRiver  extends AbstractESRiver<TicketType>{

    @Override
    Observable<TicketType> retrieveCatalogItems(RiverConfig config) {
        Calendar now = Calendar.getInstance()
        // preload translations
        def languages = config?.languages ?: ['fr', 'en', 'es', 'de']
        def defaultLang = config?.defaultLang ?: 'fr'
        def _defaultLang = defaultLang.trim().toLowerCase()
        def _languages = languages.collect {it.trim().toLowerCase()} - _defaultLang
        if(!_languages.flatten().isEmpty()){
            Set<Translation> translations = []
            translations << Translation.executeQuery('select t from Product p, Translation t where t.target=p.id and t.lang in :languages and (p.category.catalog.id in (:idCatalogs) and p.state=:productState)',
                    [languages:_languages, idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from Product p left join p.features as f, Translation t where t.target=f.id and t.lang in :languages and (p.category.catalog.id in (:idCatalogs) and p.state=:productState)',
                    [languages:_languages, idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from Product p left join p.featureValues as fv, Translation t where t.target=fv.id and t.lang in :languages and (p.category.catalog.id in (:idCatalogs) and p.state=:productState)',
                    [languages:_languages, idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from TicketType sku, Translation t where t.target=sku.id and t.lang in :languages and (sku.product.category.catalog.id in (:idCatalogs) and sku.product.state=:productState and (sku.stopDate is null or sku.stopDate >= :today))',
                    [languages:_languages, idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE, today: now], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from TicketType sku left join sku.variation1 as v1 left outer join sku.variation2 as v2 left outer join sku.variation3 as v3, Translation t where (t.target=v1.id or t.target=v1.variation.id or (v2 != null and (t.target=v2.id or t.target=v2.variation.id)) or (v3 != null and t.target=v3.id or t.target=v3.variation.id)) and t.lang in :languages and (sku.product.category.catalog.id in (:idCatalogs) and sku.product.state=:productState) and (sku.stopDate is null or sku.stopDate >= :today)',
                    [languages:_languages, idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE, today: now], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from Category cat, Translation t where t.target=cat.id and t.lang in :languages and cat.catalog.id in (:idCatalogs)',
                    [languages:_languages, idCatalogs:config.idCatalogs], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from Category cat left join cat.features as f, Translation t where t.target=f.id and t.lang in :languages and cat.catalog.id in (:idCatalogs)',
                    [languages:_languages, idCatalogs:config.idCatalogs], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from Brand brand, Translation t where t.target=brand.id and t.lang in :languages and brand.company in (select c.company from Catalog c where c.id in (:idCatalogs))',
                    [languages:_languages, idCatalogs:config.idCatalogs], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from Tag tag, Translation t where t.target=tag.id and t.lang in :languages and tag.company in (select c.company from Catalog c where c.id in (:idCatalogs))',
                    [languages:_languages, idCatalogs:config.idCatalogs], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from Product2Resource pr left join pr.product as p left join pr.resource as r, Translation t where t.target=r.id and t.lang in :languages and (p.category.catalog.id in (:idCatalogs) and p.state=:productState)',
                    [languages:_languages, idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from Coupon coupon join coupon.products as p, Translation t where t.target=coupon.id and t.lang in :languages and (p.category.catalog.id in (:idCatalogs) and p.state=:productState)',
                    [languages:_languages, idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from Coupon coupon join coupon.categories as category, Translation t where t.target=coupon.id and t.lang in :languages and (category.catalog.id in (:idCatalogs) and coupon.active=true)',
                    [languages:_languages, idCatalogs:config.idCatalogs], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from Coupon coupon join coupon.ticketTypes as ticketType, Translation t where t.target=coupon.id and t.lang in :languages and (ticketType.product.category.catalog.id in (:idCatalogs) and ticketType.product.state=:productState and (ticketType.stopDate is null or ticketType.stopDate >= :today) and coupon.active=true)',
                    [languages:_languages, idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE, today: now], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from Coupon coupon join coupon.catalogs as catalog, Translation t where t.target=coupon.id and t.lang in :languages and (catalog.id in (:idCatalogs) and coupon.active=true)',
                    [languages:_languages, idCatalogs:config.idCatalogs], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations.flatten().groupBy {"${it.target}"}.each {k, v ->
                TranslationsRiverCache.instance.put(k, v)
            }
        }

        // preload coupons
        def couponsMap = [:]

        Coupon.executeQuery('select product, coupon FROM Coupon coupon left join fetch coupon.rules left join coupon.products as product where (product.category.catalog.id in (:idCatalogs) and product.state=:productState and coupon.active=true)',
                [idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE], [readOnly: true, flushMode: FlushMode.MANUAL]).each {a ->
            def key = (a[0] as Product).uuid
            Set<Coupon> coupons = couponsMap.get(key) as Set<Coupon> ?: []
            coupons.add(a[1] as Coupon)
            couponsMap.put(key, coupons)
        }

        Coupon.executeQuery('select ticketType, coupon FROM Coupon coupon left join fetch coupon.rules left join coupon.ticketTypes as ticketType left join ticketType.product as product where (product.category.catalog.id in (:idCatalogs) and product.state=:productState and (ticketType.stopDate is null or ticketType.stopDate >= :today) and coupon.active=true)',
                [idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE, today: now], [readOnly: true, flushMode: FlushMode.MANUAL]).each {a ->
            def key = (a[0] as TicketType).uuid
            Set<Coupon> coupons = couponsMap.get(key) as Set<Coupon> ?: []
            coupons.add(a[1] as Coupon)
            couponsMap.put(key, coupons)
        }

        Coupon.executeQuery('select category, coupon FROM Coupon coupon left join fetch coupon.rules left join coupon.categories as category where category.catalog.id in (:idCatalogs) and coupon.active=true',
                [idCatalogs:config.idCatalogs], [readOnly: true, flushMode: FlushMode.MANUAL]).each {a ->
            def key = (a[0] as Category).uuid
            Set<Coupon> coupons = couponsMap.get(key) as Set<Coupon> ?: []
            coupons.add(a[1] as Coupon)
            couponsMap.put(key, coupons)
        }

        Coupon.executeQuery('select catalog, coupon FROM Coupon coupon left join fetch coupon.rules left join coupon.catalogs as catalog where catalog.id in (:idCatalogs) and coupon.active=true',
                [idCatalogs:config.idCatalogs], [readOnly: true, flushMode: FlushMode.MANUAL]).each {a ->
            def key = (a[0] as Catalog).uuid
            Set<Coupon> coupons = couponsMap.get(key) as Set<Coupon> ?: []
            coupons.add(a[1] as Coupon)
            couponsMap.put(key, coupons)
        }

        couponsMap.each {k, v ->
            CouponsRiverCache.instance.put(k as String, v as Set<Coupon>)
        }

        Observable.from(TicketType.executeQuery(
                'SELECT sku FROM TicketType sku ' +
                        'left join fetch sku.product as p ' +
                        'left join fetch p.features ' +
                        'left join fetch p.featureValues ' +
                        'left join fetch p.tags ' +
                        'left join fetch p.category as category ' +
                        'left join fetch category.parent ' +
                        'left join fetch p.brand as brand ' +
                        'left join fetch brand.brandProperties ' +
                        'left join fetch p.product2Resources as pr ' +
                        'left join fetch pr.resource ' +
                        'left join fetch sku.variation1 v1 ' +
                        'left join fetch v1.variation ' +
                        'left join fetch sku.variation2 v2 ' +
                        'left join fetch v2.variation ' +
                        'left join fetch sku.variation3 v3 ' +
                        'left join fetch v3.variation ' +
                        'left join fetch sku.stock ' +
                        'left join fetch sku.stockCalendars ' +
                        'left join fetch p.taxRate as taxRate ' +
                        'left join fetch taxRate.localTaxRates ' +
                        'WHERE p.category.catalog.id in (:idCatalogs) and p.state = :productState and p.deleted = false and (sku.stopDate is null or sku.stopDate >= :today)',
                [idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE, today: now], [readOnly: true, flushMode: FlushMode.MANUAL])
        )
    }

    @Override
    Item asItem(TicketType ticketType, RiverConfig config) {
        new Item(id:ticketType.id, type: getType(), map:
                Product.withTransaction([propagationBehavior: TransactionDefinition.PROPAGATION_SUPPORTS]){
                    RiverTools.asSkuMap(ticketType, ticketType.product, config, true)
                }
        )
    }

    @Override
    ESMapping defineESMapping() {
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

        def featureProperties = []
        featureProperties << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
        featureProperties << new ESProperty(name:'position', type:ESClient.TYPE.INTEGER, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        featureProperties << new ESProperty(name:'domain', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
        featureProperties << new ESProperty(name:'uuid', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        featureProperties << new ESProperty(name:'hide', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        featureProperties << new ESProperty(name:'value', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)

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

        def productProperties = []
        productProperties << new ESProperty(name:'code', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:false)
        productProperties << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
        productProperties << new ESProperty(name:'xtype', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        productProperties << new ESProperty(name:'startFeatureDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        productProperties << new ESProperty(name:'stopFeatureDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        productProperties << new ESProperty(name:'category', type:ESClient.TYPE.OBJECT, properties: categoryProperties)
        productProperties << new ESProperty(name:'brand', type:ESClient.TYPE.OBJECT, properties: brandProperties)
        productProperties << new ESProperty(name:'features', type:ESClient.TYPE.NESTED, properties: featureProperties)
        productProperties << new ESProperty(name:'tags', type:ESClient.TYPE.NESTED, properties: tagProperties)
        productProperties << new ESProperty(name:'dateCreated', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        productProperties << new ESProperty(name:'taxRate', type:ESClient.TYPE.OBJECT, properties: taxRateProperties)

        def variationValueProperties = []
        variationValueProperties << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
        variationValueProperties << new ESProperty(name:'position', type:ESClient.TYPE.INTEGER, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        variationValueProperties << new ESProperty(name:'uuid', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        variationValueProperties << new ESProperty(name:'hide', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        variationValueProperties << new ESProperty(name:'value', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)

        def promotionProperties = []
        promotionProperties << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
        promotionProperties << new ESProperty(name:'description', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:true)
        promotionProperties << new ESProperty(name:'reduction', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        promotionProperties << new ESProperty(name:'pastille', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)

        def couponsProperties = []
        couponsProperties << new ESProperty(name:'id', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)

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
        skuProperties << new ESProperty(name:'product', type:ESClient.TYPE.OBJECT, properties: productProperties)
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

        new ESMapping(type:getType(),
                timestamp:true,
                properties: skuProperties
        )
    }

    @Override
    String getType() {
        return "sku"
    }

    @Override
    String getUuid(TicketType ticketType) {
        return ticketType.uuid
    }
}
