package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESMapping
import com.mogobiz.elasticsearch.client.ESProperty
import com.mogobiz.elasticsearch.rivers.cache.TranslationsRiverCache
import com.mogobiz.elasticsearch.rivers.spi.AbstractESRiver
import com.mogobiz.common.client.Item
import com.mogobiz.store.domain.Coupon
import com.mogobiz.store.domain.ProductState
import com.mogobiz.store.domain.Translation
import org.hibernate.FlushMode
import org.springframework.transaction.TransactionDefinition

/**
 * Created by stephane.manciot@ebiznext.com on 14/04/2014.
 */
class CouponRiver extends AbstractESRiver<Coupon> {

    @Override
    rx.Observable<Coupon> retrieveCatalogItems(final RiverConfig config) {
        Calendar now = Calendar.getInstance()
        def languages = config?.languages ?: ['fr', 'en', 'es', 'de']
        def defaultLang = config?.defaultLang ?: 'fr'
        def _defaultLang = defaultLang.trim().toLowerCase()
        def _languages = languages.collect {it.trim().toLowerCase()} - _defaultLang
        if(!_languages.flatten().isEmpty()){
            Set<Translation> translations = []
            translations << Translation.executeQuery('select t from Coupon coupon join coupon.products as p, Translation t where t.target=coupon.id and t.lang in :languages and (p.category.catalog.id=:idCatalog and p.state=:productState)',
                    [languages:_languages, idCatalog:config.idCatalog, productState:ProductState.ACTIVE], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from Coupon coupon join coupon.categories as category, Translation t where t.target=coupon.id and t.lang in :languages and (category.catalog.id=:idCatalog and coupon.active=true)',
                    [languages:_languages, idCatalog:config.idCatalog], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from Coupon coupon join coupon.ticketTypes as ticketType, Translation t where t.target=coupon.id and t.lang in :languages and (ticketType.product.category.catalog.id=:idCatalog and ticketType.product.state=:productState and (ticketType.stopDate is null or ticketType.stopDate >= :today) and coupon.active=true)',
                    [languages:_languages, idCatalog:config.idCatalog, productState:ProductState.ACTIVE, today: now], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from Coupon coupon join coupon.catalogs as catalog, Translation t where t.target=coupon.id and t.lang in :languages and (catalog.id=:idCatalog and coupon.active=true)',
                    [languages:_languages, idCatalog:config.idCatalog], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations.flatten().groupBy {(it.target as Long).toString()}.each {k, v -> TranslationsRiverCache.instance.put(k, v)}
        }
        Set<Coupon> results = []
        results << Coupon.executeQuery('select coupon FROM Coupon coupon join fetch coupon.rules left join coupon.products as product where (product.category.catalog.id=:idCatalog and product.state=:productState and coupon.active=true)',
                [idCatalog:config.idCatalog, productState:ProductState.ACTIVE], [readOnly: true, flushMode: FlushMode.MANUAL])
        results << Coupon.executeQuery('select coupon FROM Coupon coupon join fetch coupon.rules left join coupon.categories as category where (category.catalog.id=:idCatalog and coupon.active=true)',
                [idCatalog:config.idCatalog], [readOnly: true, flushMode: FlushMode.MANUAL])
        results << Coupon.executeQuery('select coupon FROM Coupon coupon join fetch coupon.rules left join coupon.ticketTypes as ticketType where (ticketType.product.category.catalog.id=:idCatalog and ticketType.product.state=:productState and (ticketType.stopDate is null or ticketType.stopDate >= :today) and coupon.active=true)',
                [idCatalog:config.idCatalog, productState:ProductState.ACTIVE, today: now], [readOnly: true, flushMode: FlushMode.MANUAL])
        results << Coupon.executeQuery('select coupon FROM Coupon coupon join fetch coupon.rules left join coupon.catalogs as catalog where (catalog.id=:idCatalog and coupon.active=true)',
                [idCatalog:config.idCatalog], [readOnly: true, flushMode: FlushMode.MANUAL])
        return rx.Observable.from(results.flatten())
    }

    @Override
    Item asItem(Coupon coupon, RiverConfig riverConfig) {
        new Item(id:coupon.id, type: getType(), map:
                Coupon.withTransaction([propagationBehavior: TransactionDefinition.PROPAGATION_SUPPORTS]) {
                    RiverTools.asCouponMap(coupon, riverConfig)
                }
        )
    }

    @Override
    ESMapping defineESMapping() {
        def ruleProperties = []
        ruleProperties << new ESProperty(name:'id', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        ruleProperties << new ESProperty(name:'xtype', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        ruleProperties << new ESProperty(name:'quantityMin', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        ruleProperties << new ESProperty(name:'quantityMax', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        ruleProperties << new ESProperty(name:'discount', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        ruleProperties << new ESProperty(name:'xPurchased', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        ruleProperties << new ESProperty(name:'yOffered', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)

//        def skuProperties = []
//        skuProperties << new ESProperty(name:'id', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)

        new ESMapping(type:getType(),
                timestamp:true,
                properties: []
                        << new ESProperty(name:'code', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:false)
                        << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:false)
                        << new ESProperty(name:'description', type:ESClient.TYPE.STRING, index:ESClient.INDEX.ANALYZED, multilang:false)
                        << new ESProperty(name:'active', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'catalogWise', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'numberOfUses', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'startDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'stopDate', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'consumed', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'rules', type:ESClient.TYPE.OBJECT, properties: ruleProperties)
//                        << new ESProperty(name:'skus', type:ESClient.TYPE.OBJECT, properties: skuProperties)
                        << new ESProperty(name:'anonymous', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'pastille', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        )
    }

    @Override
    String getType() {
        return 'coupon'
    }

    @Override
    String getUuid(Coupon c){
        c.uuid
    }

}
