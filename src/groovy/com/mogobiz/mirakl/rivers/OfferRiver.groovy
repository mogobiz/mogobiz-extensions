/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.mirakl.rivers

import akka.dispatch.Futures
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.rivers.RiverTools
import com.mogobiz.elasticsearch.rivers.cache.CouponsRiverCache
import com.mogobiz.elasticsearch.rivers.cache.TranslationsRiverCache
import com.mogobiz.mirakl.client.MiraklClient
import com.mogobiz.mirakl.client.domain.MiraklOffer
import com.mogobiz.mirakl.client.io.ImportOffersResponse
import com.mogobiz.store.domain.*
import org.hibernate.FlushMode
import rx.Observable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import java.util.concurrent.Callable

/**
 *
 */
class OfferRiver extends AbstractMiraklRiver<TicketType, MiraklOffer, ImportOffersResponse> {

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
            translations << Translation.executeQuery('select t from Product p, Translation t where t.target=p.id and t.lang in :languages and (p.category.catalog.id=:idCatalog and p.state=:productState)',
                    [languages:_languages, idCatalog:config.idCatalog, productState:ProductState.ACTIVE], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from Product p left join p.features as f, Translation t where t.target=f.id and t.lang in :languages and (p.category.catalog.id=:idCatalog and p.state=:productState)',
                    [languages:_languages, idCatalog:config.idCatalog, productState:ProductState.ACTIVE], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from Product p left join p.featureValues as fv, Translation t where t.target=fv.id and t.lang in :languages and (p.category.catalog.id=:idCatalog and p.state=:productState)',
                    [languages:_languages, idCatalog:config.idCatalog, productState:ProductState.ACTIVE], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from TicketType sku, Translation t where t.target=sku.id and t.lang in :languages and (sku.product.category.catalog.id=:idCatalog and sku.product.state=:productState and (sku.stopDate is null or sku.stopDate >= :today))',
                    [languages:_languages, idCatalog:config.idCatalog, productState:ProductState.ACTIVE, today: now], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from TicketType sku left join sku.variation1 as v1 left outer join sku.variation2 as v2 left outer join sku.variation3 as v3, Translation t where (t.target=v1.id or t.target=v1.variation.id or (v2 != null and (t.target=v2.id or t.target=v2.variation.id)) or (v3 != null and t.target=v3.id or t.target=v3.variation.id)) and t.lang in :languages and (sku.product.category.catalog.id=:idCatalog and sku.product.state=:productState) and (sku.stopDate is null or sku.stopDate >= :today)',
                    [languages:_languages, idCatalog:config.idCatalog, productState:ProductState.ACTIVE, today: now], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from Category cat, Translation t where t.target=cat.id and t.lang in :languages and cat.catalog.id=:idCatalog',
                    [languages:_languages, idCatalog:config.idCatalog], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from Category cat left join cat.features as f, Translation t where t.target=f.id and t.lang in :languages and cat.catalog.id=:idCatalog',
                    [languages:_languages, idCatalog:config.idCatalog], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from Brand brand, Translation t where t.target=brand.id and t.lang in :languages and brand.company in (select c.company from Catalog c where c.id=:idCatalog)',
                    [languages:_languages, idCatalog:config.idCatalog], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from Tag tag, Translation t where t.target=tag.id and t.lang in :languages and tag.company in (select c.company from Catalog c where c.id=:idCatalog)',
                    [languages:_languages, idCatalog:config.idCatalog], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from Product2Resource pr left join pr.product as p left join pr.resource as r, Translation t where t.target=r.id and t.lang in :languages and (p.category.catalog.id=:idCatalog and p.state=:productState)',
                    [languages:_languages, idCatalog:config.idCatalog, productState:ProductState.ACTIVE], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from Coupon coupon join coupon.products as p, Translation t where t.target=coupon.id and t.lang in :languages and (p.category.catalog.id=:idCatalog and p.state=:productState)',
                    [languages:_languages, idCatalog:config.idCatalog, productState:ProductState.ACTIVE], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from Coupon coupon join coupon.categories as category, Translation t where t.target=coupon.id and t.lang in :languages and (category.catalog.id=:idCatalog and coupon.active=true)',
                    [languages:_languages, idCatalog:config.idCatalog], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from Coupon coupon join coupon.ticketTypes as ticketType, Translation t where t.target=coupon.id and t.lang in :languages and (ticketType.product.category.catalog.id=:idCatalog and ticketType.product.state=:productState and (ticketType.stopDate is null or ticketType.stopDate >= :today) and coupon.active=true)',
                    [languages:_languages, idCatalog:config.idCatalog, productState:ProductState.ACTIVE, today: now], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations << Translation.executeQuery('select t from Coupon coupon join coupon.catalogs as catalog, Translation t where t.target=coupon.id and t.lang in :languages and (catalog.id=:idCatalog and coupon.active=true)',
                    [languages:_languages, idCatalog:config.idCatalog], [readOnly: true, flushMode: FlushMode.MANUAL])
            translations.flatten().groupBy {"${it.target}"}.each {k, v ->
                TranslationsRiverCache.instance.put(k, v)
            }
        }

        // preload coupons
        def couponsMap = [:]

        Coupon.executeQuery('select product, coupon FROM Coupon coupon left join fetch coupon.rules left join coupon.products as product where (product.category.catalog.id=:idCatalog and product.state=:productState and coupon.active=true)',
                [idCatalog:config.idCatalog, productState:ProductState.ACTIVE], [readOnly: true, flushMode: FlushMode.MANUAL]).each {a ->
            def key = (a[0] as Product).uuid
            Set<Coupon> coupons = couponsMap.get(key) as Set<Coupon> ?: []
            coupons.add(a[1] as Coupon)
            couponsMap.put(key, coupons)
        }

        Coupon.executeQuery('select ticketType, coupon FROM Coupon coupon left join fetch coupon.rules left join coupon.ticketTypes as ticketType left join ticketType.product as product where (product.category.catalog.id=:idCatalog and product.state=:productState and (ticketType.stopDate is null or ticketType.stopDate >= :today) and coupon.active=true)',
                [idCatalog:config.idCatalog, productState:ProductState.ACTIVE, today: now], [readOnly: true, flushMode: FlushMode.MANUAL]).each {a ->
            def key = (a[0] as TicketType).uuid
            Set<Coupon> coupons = couponsMap.get(key) as Set<Coupon> ?: []
            coupons.add(a[1] as Coupon)
            couponsMap.put(key, coupons)
        }

        Coupon.executeQuery('select category, coupon FROM Coupon coupon left join fetch coupon.rules left join coupon.categories as category where category.catalog.id=:idCatalog and coupon.active=true',
                [idCatalog:config.idCatalog], [readOnly: true, flushMode: FlushMode.MANUAL]).each {a ->
            def key = (a[0] as Category).uuid
            Set<Coupon> coupons = couponsMap.get(key) as Set<Coupon> ?: []
            coupons.add(a[1] as Coupon)
            couponsMap.put(key, coupons)
        }

        Coupon.executeQuery('select catalog, coupon FROM Coupon coupon left join fetch coupon.rules left join coupon.catalogs as catalog where catalog.id=:idCatalog and coupon.active=true',
                [idCatalog:config.idCatalog], [readOnly: true, flushMode: FlushMode.MANUAL]).each {a ->
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
                        'WHERE p.category.catalog.id=:idCatalog and p.state = :productState and p.publishable = true and p.deleted = false and (sku.stopDate is null or sku.stopDate >= :today)',
                [idCatalog:config.idCatalog, productState:ProductState.ACTIVE, today: now], [readOnly: true, flushMode: FlushMode.MANUAL])
        )
    }

    @Override
    MiraklOffer asRiverItem(TicketType sku, RiverConfig config) {
        return RiverTools.asMiraklOffer(sku, sku.product, config, true)
    }

    @Override
    Future<ImportOffersResponse> bulk(RiverConfig config, List<MiraklOffer> items, ExecutionContext ec) {
        Future<ImportOffersResponse> f = Futures.future(new Callable<ImportOffersResponse>(){
            @Override
            ImportOffersResponse call() throws Exception {
                return MiraklClient.importOffers(config, items)
            }
        }, ec)
        return f
    }

}
