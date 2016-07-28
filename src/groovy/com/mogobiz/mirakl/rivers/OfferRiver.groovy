/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.mirakl.rivers

import akka.dispatch.Futures
import com.mogobiz.common.rivers.spi.AbstractGenericRiver
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.rivers.RiverTools
import com.mogobiz.elasticsearch.rivers.cache.CategoryFeaturesRiverCache
import com.mogobiz.elasticsearch.rivers.cache.CouponsRiverCache
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
class OfferRiver extends AbstractGenericRiver<MiraklOffer, ImportOffersResponse> {

    @Override
    Observable<TicketType> retrieveCatalogItems(RiverConfig config) {
        Calendar now = Calendar.getInstance()
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

        Category.executeQuery('select cat FROM Category cat left join fetch cat.features where cat.catalog.id=:idCatalog',
                [idCatalog:config.idCatalog], [readOnly: true, flushMode: FlushMode.MANUAL]).each {CategoryFeaturesRiverCache.instance.put(it.uuid, it.features)}

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
                        'left join fetch p.productProperties ' +
                        'WHERE p.category.catalog.id=:idCatalog and p.state = :productState and p.publishable = true and p.deleted = false and (sku.stopDate is null or sku.stopDate >= :today)',
                [idCatalog:config.idCatalog, productState:ProductState.ACTIVE, today: now], [readOnly: true, flushMode: FlushMode.MANUAL])
        )
    }

    @Override
    MiraklOffer asRiverItem(Object e, RiverConfig config) {
        def sku = e as TicketType
        return RiverTools.asMiraklOffer(sku, sku.product, config)
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

    @Override
    String getType() {
        return "mirakl_offer"
    }
}
