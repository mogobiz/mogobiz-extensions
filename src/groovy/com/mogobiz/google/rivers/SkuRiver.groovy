/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.google.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.rivers.RiverTools
import com.mogobiz.google.client.GoogleClient
import com.mogobiz.google.rivers.spi.AbstractGoogleRiver
import com.mogobiz.store.domain.GoogleVariationType
import com.mogobiz.store.domain.GoogleVariationValue
import com.mogobiz.store.domain.Product2Resource
import com.mogobiz.store.domain.ProductCalendar
import com.mogobiz.store.domain.ProductState
import com.mogobiz.store.domain.ResourceType
import com.mogobiz.store.domain.StockCalendar
import com.mogobiz.store.domain.TicketType
import com.mogobiz.store.domain.VariationValue

/**
 */
class SkuRiver extends AbstractGoogleRiver<TicketType>{
    @Override
    rx.Observable<TicketType> retrieveCatalogItems(RiverConfig riverConfig) {
        // FIXME filter with xprivate == false ?
        return rx.Observable.from(TicketType.executeQuery('FROM TicketType sku WHERE sku.product.category.catalog.id=:idCatalog and sku.product.state = :productState',
                [idCatalog:riverConfig.idCatalog, productState:ProductState.ACTIVE]))
    }

    @Override
    Item asItem(TicketType sku, RiverConfig riverConfig) {
        def map = [:]
        if(sku){
            /** basic product information **/
            map << [id:sku.sku]
            map << [title:sku.name]
            def description = sku.description
            map << [content:description && description.trim().length() > 0 ? description : sku.product.description]
            map << [google_product_category: sku.product?.category?.googleCategory]
            def path = sku.product?.category?.fullpath ?: RiverTools.retrieveCategoryPath(sku.product?.category, sku.product?.category?.sanitizedName)
            map << [product_type: path?.split('/')?.join(' &gt; ')]
            map << [link:RiverTools.retrieveSkuUrl(sku, riverConfig)]
            def picture = sku.picture
            def image_link = picture && ResourceType.PICTURE.equals(picture.xtype) ? RiverTools.extractResourceUrl(picture, riverConfig) : null
            if(!image_link){
                List<Product2Resource> bindedResources = Product2Resource.executeQuery(
                        'select distinct pr from Product2Resource pr join pr.resource as r where pr.product=:product and r.xtype=:xtype order by pr.position asc',
                        [product: sku.product, xtype: ResourceType.PICTURE])
                picture = bindedResources.size() > 0 ? bindedResources.get(0).resource : null
                if(picture){
                    image_link = RiverTools.extractResourceUrl(picture, riverConfig)
                }
            }
            map << [image_link:image_link]
            map << [condition:'new'] // one from new, used or refurbished (actually only new value is applicable for mogobiz)

            /** availability and price **/
            def stock = sku.stock
            if(!stock){
                map << [availability:'in stock']
            }
            else{
                if(stock.stockUnlimited){
                    map << [availability:'in stock']
                }
                else{
                    def remaining
                    StockCalendar stockCalendar
                    if (sku?.product?.calendarType == ProductCalendar.NO_DATE) {
                        stockCalendar = StockCalendar.findByTicketType(sku)
                    }
                    else{
                        stockCalendar = StockCalendar.findByTicketTypeAndStartDateGreaterThanEquals(sku, Calendar.instance)
                    }
                    if (stockCalendar) {
                        remaining = Math.max(0, stockCalendar.stock - stockCalendar.sold)
                    }
                    else
                    {
                        remaining = stock.stock
                    }
                    if(remaining > 0){
                        map << [availability:'in stock']
                    }
                    else if(stock.stockOutSelling){
                        map << [availability:'available for order']
                    }
                    else{
                        map << [availability:'out of stock']
                    }
                }
            }

            def price = sku.product?.price ?: 0l

            def language = riverConfig.defaultLang?.toLowerCase()
            map << [content_language:language]

            def countryCode = riverConfig.countryCode?.toUpperCase()
            map << [target_country:countryCode]

            Locale locale = new Locale(language, countryCode)

            def currencyCode = riverConfig.currencyCode
            def rate = RiverTools.retrieveRates(riverConfig)?.find {r ->
                r.code == currencyCode
            }

            def localTaxRate = sku.product?.taxRate?.localTaxRates?.find { localTaxRate ->
                localTaxRate.active && localTaxRate.countryCode.equalsIgnoreCase(countryCode)
            }
            def taxRate = localTaxRate ? localTaxRate.rate : 0f

            // for the US do not include tax in the price
            if(Locale.US.country.equalsIgnoreCase(countryCode)){
                map << [price:(price as Long) * (rate ? rate.rate : 0d)]//RiverTools.format(price as Long, currencyCode, locale, rate ? rate.rate : 0d)
                map << [tax:taxRate]
            }
            else{
                map << [price:((price as Long) + ((price * taxRate / 100f) as Long)) * (rate ? rate.rate : 0d)]//RiverTools.format((price as Long) + ((price * taxRate / 100f) as Long), currencyCode, locale, rate ? rate.rate : 0d)
            }

            // TODO add sale price and sale price effective date (from coupon.startDate to coupon.endDate)

            /** Unique Product Identifiers **/
            def brand = sku.product?.brand
            if(brand){
                map << [brand:brand.name]
            }

            if(!sku.gtin && !sku.mpn){
                map << [identifier_exists:false]
            }
            else{
                if(sku.gtin){
                    map << [gtin:sku.gtin]
                }
                if(sku.mpn){
                    map << [mpn:sku.mpn]
                }
            }

            /** Apparel Products **/

            // gender (male, female, unisex), age group (adults, kids), color, size

            /** Product Variants **/

            // 'color', 'material', 'pattern', and/or 'size'

            def variants = false
            def variation1 = sku.variation1
            if(variation1){
                variants = addVariant(map, variation1, language)
            }
            def variation2 = sku.variation2
            if(variation2){
                variants = addVariant(map, variation2, language) || variants
            }
            def variation3 = sku.variation3
            if(variation3){
                variants = addVariant(map, variation3, language) || variants
            }

            if(variants){
                map << [item_group_id:sku.product.id]
            }

            /** Shipping **/
            // not required
            //def shipping = sku.product?.shipping

            map << [channel:'online']
        }

        return new Item(id:sku?.id, type: getType(), map:map)
    }

    private boolean addVariant(LinkedHashMap map, VariationValue variationValue, String language) {
        def check = true
        def variation = variationValue.variation
        def type = variation.googleVariationType ? variation.googleVariationType :
                RiverTools.translateProperty(variation.id, language, 'name', variation.name)
        def value = variationValue.googleVariationValue ? variationValue.googleVariationValue :
                RiverTools.translateProperty(variationValue.id, language, 'value', variationValue.value)
        GoogleVariationType gtype = GoogleVariationType.findByXtype(type)
        if(gtype){
            Collection<GoogleVariationValue> values = GoogleVariationValue.findAllByType(gtype)
            if(!values.isEmpty()){
                // check variation value
                check = values.any {GoogleVariationValue gvalue ->
                    gvalue.value == value
                }
            }
        }
        else{
            check = false
        }
        if(check){
            map.put(type.split(' ').join('_'), value)
        }
        check
    }

    @Override
    GoogleClient getClient() {
        return GoogleClient.getInstance()
    }

    @Override
    String getType() {
        return 'sku'
    }
}
