package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESMapping
import com.mogobiz.elasticsearch.client.ESProperty
import com.mogobiz.elasticsearch.rivers.cache.CouponsRiverCache
import com.mogobiz.elasticsearch.rivers.cache.TranslationsRiverCache
import com.mogobiz.elasticsearch.rivers.spi.AbstractESRiver
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
 * Created by smanciot on 12/07/15.
 */
class SkuRiver  extends AbstractESRiver<TicketType>{

    @Override
    Observable<TicketType> retrieveCatalogItems(RiverConfig config) {
        // preload translations
        def languages = config?.languages ?: ['fr', 'en', 'es', 'de']
        def defaultLang = config?.defaultLang ?: 'fr'
        def _defaultLang = defaultLang.trim().toLowerCase()
        def _languages = languages.collect {it.trim().toLowerCase()} - _defaultLang
        if(!_languages.flatten().isEmpty()){
            Translation.executeQuery('select t from TicketType sku, Translation t where t.target=sku.id and t.lang in :languages and (sku.product.category.catalog.id=:idCatalog and sku.product.state=:productState)',
                    [languages:_languages, idCatalog:config.idCatalog, productState:ProductState.ACTIVE], [readOnly: true, flushMode: FlushMode.MANUAL]).groupBy {it.target.toString()}.each {k, v -> TranslationsRiverCache.instance.put(k, v)}
            Translation.executeQuery('select t from TicketType sku, Translation t where (t.target=sku.variation1.id or t.target=sku.variation1.variation.id or t.target=sku.variation2.id or t.target=sku.variation2.variation.id or t.target=sku.variation3.id or t.target=sku.variation3.variation.id) and t.lang in :languages and (sku.product.category.catalog.id=:idCatalog and sku.product.state=:productState)',
                    [languages:_languages, idCatalog:config.idCatalog, productState:ProductState.ACTIVE], [readOnly: true, flushMode: FlushMode.MANUAL]).groupBy {it.target.toString()}.each {k, v -> TranslationsRiverCache.instance.put(k, v)}
        }

        // preload coupons
        def couponsMap = [:]

        Coupon.executeQuery('select product, coupon FROM Coupon coupon left join fetch coupon.rules left join coupon.products as product where (product.category.catalog.id=:idCatalog and product.state=:productState)',
                [idCatalog:config.idCatalog, productState:ProductState.ACTIVE], [readOnly: true, flushMode: FlushMode.MANUAL]).each {a ->
            def key = (a[0] as Product).uuid
            Set<Coupon> coupons = couponsMap.get(key) as Set<Coupon> ?: []
            coupons.add(a[1] as Coupon)
            couponsMap.put(key, coupons)
        }

        Coupon.executeQuery('select ticketType, coupon FROM Coupon coupon left join fetch coupon.rules left join coupon.ticketTypes as ticketType left join ticketType.product as product where (product.category.catalog.id=:idCatalog and product.state=:productState)',
                [idCatalog:config.idCatalog, productState:ProductState.ACTIVE], [readOnly: true, flushMode: FlushMode.MANUAL]).each {a ->
            def key = (a[0] as TicketType).uuid
            Set<Coupon> coupons = couponsMap.get(key) as Set<Coupon> ?: []
            coupons.add(a[1] as Coupon)
            couponsMap.put(key, coupons)
        }

        Coupon.executeQuery('select category, coupon FROM Coupon coupon left join fetch coupon.rules left join coupon.categories as category where category.catalog.id=:idCatalog',
                [idCatalog:config.idCatalog], [readOnly: true, flushMode: FlushMode.MANUAL]).each {a ->
            def key = (a[0] as Category).uuid
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
                        'left join fetch p.product2Resources as pr ' +
                        'left join fetch pr.resource ' +
                        'left join fetch sku.variation1 v1 ' +
                        'left join fetch v1.variation ' +
                        'left join fetch sku.variation2 v2 ' +
                        'left join fetch v2.variation ' +
                        'left join fetch sku.variation3 v3 ' +
                        'left join fetch v3.variation ' +
                        'left join fetch p.taxRate as taxRate ' +
                        'left join fetch taxRate.localTaxRates ' +
                        'WHERE p.category.catalog.id=:idCatalog and p.state = :productState and p.deleted = false',
                [idCatalog:config.idCatalog, productState:ProductState.ACTIVE], [readOnly: true, flushMode: FlushMode.MANUAL])
        )
    }

    @Override
    Item asItem(TicketType ticketType, RiverConfig config) {
        new Item(id:ticketType.id, type: getType(), map:
                Product.withTransaction([propagationBehavior: TransactionDefinition.PROPAGATION_SUPPORTS]){
                    RiverTools.asSkuMap(ticketType, config)
                }
        )
    }

    @Override
    ESMapping defineESMapping() {
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
