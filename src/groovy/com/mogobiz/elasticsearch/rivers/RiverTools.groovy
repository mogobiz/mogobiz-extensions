package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.http.client.HTTPClient
import com.mogobiz.store.domain.Brand
import com.mogobiz.store.domain.BrandProperty
import com.mogobiz.store.domain.Catalog
import com.mogobiz.store.domain.Category
import com.mogobiz.store.domain.Coupon
import com.mogobiz.store.domain.DatePeriod
import com.mogobiz.store.domain.Feature
import com.mogobiz.store.domain.Ibeacon
import com.mogobiz.store.domain.IntraDayPeriod
import com.mogobiz.store.domain.LocalTaxRate
import com.mogobiz.store.domain.Product
import com.mogobiz.store.domain.Product2Resource
import com.mogobiz.store.domain.ProductCalendar
import com.mogobiz.store.domain.ProductProperty
import com.mogobiz.store.domain.ReductionRule
import com.mogobiz.store.domain.ReductionRuleType
import com.mogobiz.store.domain.Resource
import com.mogobiz.store.domain.ResourceType
import com.mogobiz.store.domain.Shipping
import com.mogobiz.store.domain.StockCalendar
import com.mogobiz.store.domain.Suggestion
import com.mogobiz.store.domain.Tag
import com.mogobiz.store.domain.TaxRate
import com.mogobiz.store.domain.TicketType
import com.mogobiz.store.domain.Translation
import com.mogobiz.store.domain.VariationValue
import com.mogobiz.geolocation.domain.Poi
import com.mogobiz.json.RenderUtil
import com.mogobiz.store.vo.Country
import com.mogobiz.utils.IperUtil
import com.mogobiz.utils.MogopayRate
import grails.converters.JSON
import grails.util.Holders
import groovy.json.JsonSlurper
import org.apache.commons.lang.StringUtils
import org.codehaus.groovy.grails.web.json.JSONObject

import java.text.NumberFormat

/**
 * Created by stephane.manciot@ebiznext.com on 20/02/2014.
 */
final class RiverTools {

    private RiverTools(){}

    static Map translate(
            Map m,
            final Long id,
            final List<String> included = [],
            final List<String> languages = ['fr', 'en', 'es', 'de'],
            final String defaultLang = 'fr'){
        // translations for default language
        m[defaultLang] = [:]
        included.each {k ->
            m[defaultLang][k] = m[k]
        }
        // translations for other languages
        def _languages = languages - defaultLang
        _languages.each {lang ->
            def final list = Translation.createCriteria().list {
                eq ("target", id)
                eq("lang", lang)
            }
            // translated properties
            def translations = [:]
            list.each {translation ->
                new JsonSlurper().parseText(translation.value).each {k, v ->
                    if(included.contains(k)){
                        translations[k] = v
                    }
                }
                m[lang] = translations
            }
        }
        m
    }

    static String translateProperty(Long id, String lang, String property, String value){
        def final list = Translation.createCriteria().list {
            eq ("target", id)
            eq("lang", lang)
        }
        list.each { translation ->
            new JsonSlurper().parseText(translation.value).each {k, v ->
                if(k == property){
                    value = v
                }
            }
        }
        value
    }

    static Map asBrandMap(final Brand b, final RiverConfig config) {
        def m = [:]
        if(b){
            m = translate(
                    RenderUtil.asIsoMapForJSON(
                            [
                                    "id",
                                    "name",
                                    "website",
                                    "hide",
                                    'description',
                                    "twitter"
                            ],
                            b
                    ),
                    b.id,
                    [
                            'name',
                            'website'
                    ],
                    config?.languages,
                    config?.defaultLang
            ) << [increments:0]
            BrandProperty.findAllByBrand(b).each {BrandProperty property ->
                m << ["${property.name}":property.value]
            }
        }
        m
    }

    static Map asCatalogMap(final Catalog catalog, final RiverConfig config){
        catalog ? translate(
                RenderUtil.asIsoMapForJSON(
                        [
                                "id",
                                "name",
                                "description",
                                "uuid",
                                "activationdate"
                        ],
                        catalog
                ),
                catalog.id,
                [
                        'name',
                        'description'
                ],
                config?.languages,
                config?.defaultLang
        ) : [:]
    }

    static Map asCategoryMap(Category category, RiverConfig config) {
        category ? translate(
                RenderUtil.asIsoMapForJSON(
                        [
                                "id",
                                "name",
                                "description",
                                "uuid",
                                "keywords",
                                "hide"
                        ],
                        category
                ),
                category.id,
                [
                        'name',
                        'description',
                        'keywords'
                ],
                config.languages,
                config.defaultLang
        ) << [path: retrieveCategoryPath(category, category.sanitizedName)] << [parentId:category.parent?.id] << [increments:0] : [:]
    }

    static String retrieveCategoryPath(Category cat, String path = cat?.sanitizedName){
        def parent = cat?.parent
        parent ? retrieveCategoryPath(parent, parent.sanitizedName + '/' + path) : path
    }

    static Map asCountryMap(Country country, RiverConfig config) {
        country ?
                RenderUtil.asIsoMapForJSON([
                        'code',
                        'name'
                ], country) : [:]
    }

    static Map asRateMap(MogopayRate rate, RiverConfig config) {
        rate ? RenderUtil.asIsoMapForJSON([
                'code',
                'name',
                'rate',
                'currencyFractionDigits'
        ], rate) : [:]
    }

    static Map asTagMap(Tag tag, RiverConfig config) {
        tag ? translate(
                RenderUtil.asIsoMapForJSON(
                        [
                                "id",
                                "name"
                        ],
                        tag
                ),
                tag.id,
                [
                        'name'
                ],
                config.languages,
                config.defaultLang
        ) << [increments:0] : [:]
    }

    static Map asSkuMap(TicketType sku, RiverConfig config){
        def msku = RenderUtil.asIsoMapForJSON([
                'id',
                'sku',
                'price',
                'minOrder',
                'maxOrder',
                'nbSales',
                'startDate',
                'stopDate',
                'xprivate',
                'name',
                'description',
                'picture',
                'position',
                'availabilityDate'
        ], sku)

        translate(msku, sku.id, ['name', 'description'], config.languages, config.defaultLang)

        msku << [variation1:asVariationMap(sku.variation1, config)]
        msku << [variation2:asVariationMap(sku.variation2, config)]
        msku << [variation3:asVariationMap(sku.variation3, config)]

        // liste des coupons associés à ce sku
        def product = sku.product
        Set<Coupon> coupons = extractProductCoupons(product)
        coupons << Coupon.executeQuery('select distinct coupon FROM Coupon coupon left join coupon.ticketTypes as ticketType where (ticketType.id=:idSku)',
                [idSku:sku.id])

        asPromotionsAndCouponsMap(coupons, sku.price).each {k, v ->
            msku[k] = v
        }

        msku
    }

    private static Set<Coupon> extractProductCoupons(Product product) {
        Set<Coupon> coupons = []
        coupons << Coupon.executeQuery('select distinct coupon FROM Coupon coupon left join coupon.products as product where (product.id=:idProduct)',
                [idProduct: product.id])
        def idCategories = []
        categoryWithParents(product.category).each { Category c ->
            idCategories << c.id
        }
        coupons << Coupon.executeQuery('select distinct coupon FROM Coupon coupon left join coupon.categories as category where (category.id in (:idCategories))', ['idCategories': idCategories])
        coupons
    }

    private static Map asPromotionsAndCouponsMap(Set<Coupon> coupons, Long price){
        def m = [:]
        def mCoupons = []
        Long reduction = 0L
        String name = null
        String description = null
        coupons.flatten().each {Coupon coupon ->
            mCoupons << [id: coupon.id]
            if(coupon.active && coupon.anonymous && coupon.startDate?.compareTo(Calendar.getInstance()) <= 0 && coupon.endDate?.compareTo(Calendar.getInstance()) >= 0){
                if(!name){
                    name = coupon.name
                }
                if(!description){
                    description = coupon.description
                }
                reduction += calculerReduction(coupon, price)
            }
        }
        if(!mCoupons.isEmpty()){
            m << ['coupons':mCoupons]
            if(reduction > 0){
                m << [promotion:[name:name, description:description, reduction:reduction]]
                m << [salePrice:price - reduction]
            }
        }
        m
    }

    private static Long calculerReduction(Coupon coupon, Long prixDeBase){
        def reduction = 0
        coupon.rules?.each {rule ->
            switch (rule.xtype) {
                case ReductionRuleType.DISCOUNT:
                    String discount = rule.discount
                    if(discount?.endsWith("%")){
                        reduction += (Long)(prixDeBase * Float.parseFloat(discount.substring(0, discount.length()-1)) / 100)
                    }
                    else if (discount.startsWith ("+")){
                        //should never be the case
                        reduction -= Long.parseLong (discount.substring(1))
                    }
                    else if (discount.startsWith ("-")){
                        reduction += Long.parseLong (discount.substring(1))
                    }
                    break
                default:
                    break
            }
        }
        reduction
    }

    static Map asVariationMap(VariationValue variationValue, RiverConfig config){
        def m = variationValue ? RenderUtil.asIsoMapForJSON(['value'], variationValue) << RenderUtil.asIsoMapForJSON([
                'name', 'position', 'uuid', 'hide'], variationValue.variation) : [:]
        if(variationValue){
            translate(m, variationValue.id, ['value'], config.languages, config.defaultLang)
            translate(m, variationValue.variation.id, ['name'], config.languages, config.defaultLang)
        }
        m
    }

    static Map asResourceMap(Resource resource, RiverConfig config) {
        def m = resource ? RenderUtil.asIsoMapForJSON([
                'name',
                'xtype',
                'active',
                'deleted',
                'uploaded',
                'contentType',
                'sanitizedName'
        ], resource) <<
                [url:extractResourceUrl(resource, config)] <<
                [smallPicture:extractSmallPictureUrl(resource, config)] : [:]
        if(resource){
            translate(m, resource.id, ['name'], config.languages, config.defaultLang)
        }
        m
    }

    static Map asFeatureMap(Feature feature, RiverConfig config) {
        def m = feature ? RenderUtil.asIsoMapForJSON([
                'name',
                'position',
                'domain',
                'uuid',
                'hide',
                'value'
        ], feature) : [:]
        if(feature){
            translate(m, feature.id, ['name', 'value'], config.languages, config.defaultLang)
        }
        m
    }

    static Map asShippingMap(Shipping shipping, RiverConfig config) {
        def m = shipping ? RenderUtil.asIsoMapForJSON([
                'name',
                'weight',
                'width',
                'height',
                'depth',
                'amount',
                'free'
        ], shipping) : [:]
        if(shipping){
            translate(m, shipping.id, ['name'], config.languages, config.defaultLang)
        }
        m
    }

    static Map asPoiMap(Poi poi, RiverConfig config) {
        def mpoi = [:]
        mpoi << [name: poi.name]
        mpoi << [description: poi.description]
        mpoi << [picture: poi.picture]
        mpoi << [xtype: poi.poiType?.xtype]
        translate(mpoi, poi.id, ['name', 'description'], config.languages, config.defaultLang)
        def location = [:]
        location << [latitude: poi.latitude]
        location << [longitude: poi.longitude]
        location << [road1: poi.road1]
        location << [road2: poi.road2]
        location << [road3: poi.road3]
        location << [roadNum: poi.roadNum]
        location << [postalCode: poi.postalCode]
        location << [state: poi.state]
        location << [city: poi.city]
        def country = [:]
        country << [code: poi.countryCode]
        // TODO retrieve country name + translations
        location << [country: country]
        mpoi << [location: location]
        mpoi
    }

    static Map asProductMap(Product p, RiverConfig config) {
        if(p){
            Map m = RenderUtil.asIsoMapForJSON([
                    "id",
                    "code",
                    "name",
                    "description",
                    "descriptionAsText",
                    "price",
                    "startDate",
                    "stopDate",
                    "startFeatureDate",
                    "stopFeatureDate",
                    "availabilityDate",
                    "nbSales",
                    "stockDisplay",
                    "uuid",
                    "hide",
                    "sanitizedName",
                    "keywords",
                    "dateCreated",
                    "lastUpdated",
                    "availabilityDate"
            ], p)

            m << [calendarType:p.calendarType?.name()]
            m << [xtype:p.xtype?.name()]

            translate(m, p.id, ['name', 'description', 'descriptionAsText', 'keywords'])

//            DetachedCriteria<Product2Resource> query = Product2Resource.where {
//                product==p && resource.xtype==ResourceType.PICTURE
//            }
//            List<Product2Resource> bindedResources = query.list([sort:'position', order:'asc'] as Map)
            List<Product2Resource> bindedResources = Product2Resource.executeQuery(
                    'select distinct pr from Product2Resource pr join pr.resource as r where pr.product=:product and r.xtype=:xtype order by pr.position asc',
                    [product: p, xtype: ResourceType.PICTURE])
            def picture = bindedResources.size() > 0 ? bindedResources.get(0).resource : null
            if(picture){
                m << [picture:extractResourceUrl(picture, config)]
                if(picture.smallPicture){
                    m << [smallPicture:extractSmallPictureUrl(picture, config)]
                }
            }

            if(p.poi){
                m << [poi:asPoiMap(p.poi, config)]
            }

            if(p.category){
                m << [category:asCategoryMap(p.category, config)]
            }

            if(p.brand){
                m << [brand:asBrandMap(p.brand, config)]
            }

            if(p.shipping){
                m << [shipping:asShippingMap(p.shipping, config)]
            }

            if(p.taxRate){
                m << [taxRate:asTaxRateMap(p.taxRate, config)]
            }

            if(p.ibeacon){
                m << [ibeacon:asIBeaconMap(p.ibeacon, config)]
            }

            def features = []
            Feature.findAllByProduct(p).each { feature ->
                features << asFeatureMap(feature, config)
            }
            if(!features.isEmpty()){
                m << [features:features]
            }

            def tags = []
            p.tags.each{ tag ->
                tags << [name:tag.name]
            }
            if(!tags.isEmpty()){
                m << [tags:tags]
            }

            def resources = []
            Product2Resource.findAllByProduct(p).each {pr ->
                resources << asResourceMap(pr.resource, config)
            }
            if(!resources.isEmpty()){
                m << [resources:resources]
            }

            def skus = []
            TicketType.findAllByProduct(p).each {sku ->
                def msku = asSkuMap(sku, config)
                def stock = sku.stock
                if(stock){
                    msku << [initialStock: stock.stock]
                    msku << [stockUnlimited: stock.stockUnlimited]
                    msku << [stockOutSelling: stock.stockOutSelling]
                    msku << [stockDisplay: p.stockDisplay]
                    if(!stock.stockUnlimited){
                        if (p.calendarType == ProductCalendar.NO_DATE) {
                            StockCalendar stockCalendar = StockCalendar.findByTicketType(sku)
                            if (stockCalendar) {
                                msku << [stock: Math.max(0, stockCalendar.stock - stockCalendar.sold)]
                            }
                            else
                            {
                                msku << [stock: stock.stock]
                            }
                        }
                        else{
                            def stockCalendars = []
                            StockCalendar.findAllByTicketType(sku).each {stockCalendar ->
                                stockCalendars << (RenderUtil.asIsoMapForJSON(['startDate'], stockCalendar)
                                        << [stock: Math.max(0, stockCalendar.stock - stockCalendar.sold)])
                            }
                            if(!stockCalendars.isEmpty()){
                                msku << [stockByDateTime:stockCalendars]
                            }
                        }
                    }
                }
                skus << msku
            }
            if(!skus.isEmpty()){
                m << [skus:skus]
            }

            if (p.calendarType != ProductCalendar.NO_DATE) {
                def datePeriods = []
                DatePeriod.findAllByProduct(p, [sort: "startDate", order: "asc"]).each {datePeriod ->
                    datePeriods << RenderUtil.asIsoMapForJSON(['startDate', 'endDate'], datePeriod)
                }
                if(!datePeriods.isEmpty()){
                    m << [datePeriods:datePeriods]
                }

                def intraDayPeriods = []
                IntraDayPeriod.findAllByProduct(p, [sort: "startDate", order: "asc"]).each {intraDayPeriod ->
                    intraDayPeriods << RenderUtil.asIsoMapForJSON([
                            'id',
                            'weekday1',
                            'weekday2',
                            'weekday3',
                            'weekday4',
                            'weekday5',
                            'weekday6',
                            'weekday7',
                            'startDate',
                            'endDate'
                    ], intraDayPeriod)
                }
                if(!intraDayPeriods.isEmpty()){
                    m << [intraDayPeriods:intraDayPeriods]
                }
            }

            m << [imported: RenderUtil.formatToIso8601(new Date())]

            m  << [increments:0]

            ProductProperty.findAllByProduct(p).each {ProductProperty property ->
                m << ["${property.name}":property.value]
            }

            asPromotionsAndCouponsMap(extractProductCoupons(p), p.price).each {k, v ->
                m[k] = v
            }

            return m
        }
        [:]
    }

    static Map asSuggestionMap(Suggestion suggestion, RiverConfig config){
        suggestion ?
                asProductMap(
                        suggestion.product, config) << RenderUtil.asIsoMapForJSON(
                            [
                                'id',
                                'required',
                                'position',
                                'discount'
                            ],
                            suggestion
                ) << [price: IperUtil.computeDiscount(suggestion.discount, suggestion.product.price)] : [:]
    }


    static Map asTaxRateMap(TaxRate taxRate, RiverConfig config){
        if(taxRate){
            Map m = RenderUtil.asIsoMapForJSON(['id', 'name'], taxRate)
            def localTaxRates = []
            taxRate.localTaxRates?.each {localTaxRate ->
                if(localTaxRate.active){
                    localTaxRates << asLocalTaxRate(localTaxRate, config)
                }
            }
            m << [localTaxRates:localTaxRates]
            return m
        }
        [:]
    }

    static Map asLocalTaxRate(LocalTaxRate localTaxRate, RiverConfig config){
        localTaxRate ? RenderUtil.asIsoMapForJSON(['id', 'rate', 'countryCode', 'stateCode'], localTaxRate) : [:]
    }

    static Map asIBeaconMap(Ibeacon ibeacon, RiverConfig config){
        ibeacon ? RenderUtil.asIsoMapForJSON(['uuid', 'name', 'startDate', 'endDate', 'active', 'major', 'minor'], ibeacon) : [:]
    }

    static String extractResourceUrl(Resource resource, RiverConfig config) {
        String url = resource?.url;
        if (resource?.uploaded) {
            StringBuffer buffer = new StringBuffer('/api/store/')
                    .append(config.clientConfig.store)
                    .append('/resources/')
                    .append(resource.id)
            url = buffer.toString()
        }
        return retrieveResourceUrl(url)
    }

    static String extractSmallPictureUrl(Resource resource, RiverConfig config) {
        def smallPicture = null
        if(ResourceType.PICTURE.equals (resource?.xtype)){
            smallPicture = resource?.smallPicture
            if (resource?.uploaded) {
                StringBuffer buffer = new StringBuffer('/api/store/')
                        .append(config.clientConfig.store)
                        .append('/resources/')
                        .append(resource.id)
                        .append('/SMALL')
                smallPicture = buffer.toString()
            }
            smallPicture = retrieveResourceUrl(smallPicture)
        }
        return smallPicture
    }

    static String retrieveResourceUrl(String url) {
        if (url && !url.startsWith("http://") && !url.startsWith("https://")) {
            url = Holders.config.resourcesServerURL + (url - Holders.config.rootPath);
        }
        url
    }

    static String retrieveSkuUrl(TicketType sku, RiverConfig config) {
        String merchant_url = config.clientConfig?.merchant_url
        new StringBuffer(merchant_url ? merchant_url : Holders.config.grails.serverURL as String)
                .append('/api/store/')
                .append(config.clientConfig.store)
                .append('/products/')
                .append(retrieveCategoryPath(sku.product?.category, sku.product?.category?.sanitizedName))
                .append('/')
                .append(sku.sku).toString()
    }

    static Map asCouponMap(Coupon coupon, RiverConfig config){
        def map = [:]
        if(coupon){
            map <<  RenderUtil.asIsoMapForJSON(['id', 'code', 'name', 'active', 'numberOfUses', 'startDate', 'endDate', 'catalogWise', 'anonymous'], coupon)
            map << ['sold': coupon.reductionSold ? coupon.reductionSold.sold : 0L]
            def rules = []
            coupon.rules.each {rule ->
                rules << asReductionRuleMap(rule, config)
            }
            map << ['rules':rules]
        }
        map
    }

    static Map asReductionRuleMap(ReductionRule rule, RiverConfig config){
        rule ? RenderUtil.asIsoMapForJSON([
                'id',
                'xtype',
                'quantityMin',
                'quantityMax',
                'discount',
                'xPurchased',
                'yOffered'], rule) : [:]
    }

    static List<Category> categoryWithParents(Category category, List<Category> categories = []){
        categories << category
        def parent = category?.parent
        if(parent){
            return categoryWithParents(parent, categories)
        }
        return categories
    }

    static List<Category> categoryWithChildren(Category category, List<Category> categories = []){
        categories << category
        def children = Category.findAllByParent(category)
        children.each {Category child ->
            categoryWithChildren(child, categories)
        }
        return categories
    }

    /**
     * Format the given amount (in the Mogobiz unit) into the given currency by using
     * the number format of the given country
     * @param amount
     * @param currencyCode
     * @param locale
     * @return the given amount formated
     */
    static String format(long amount, String currencyCode, Locale locale = Locale.default, double rate = 0) {
        NumberFormat numberFormat = NumberFormat.getCurrencyInstance(locale);
        numberFormat.setCurrency(Currency.getInstance(currencyCode));
        return numberFormat.format(amount * rate);
    }

    static List<MogopayRate> retrieveRates(RiverConfig config){
        def conn = null
        try
        {
            def rates = []
            def debug = config.debug
            def http = HTTPClient.instance
            conn = http.doGet([debug:debug], new StringBuffer(Holders.config.mogopay.url as String).append('rate/list').toString())
            if(conn.responseCode >=200 && conn.responseCode < 400){
                def data = http.getText([debug:debug], conn)
                if(data && !StringUtils.isEmpty(data.toString())){
                    List<JSONObject> res = JSON.parse(data.toString()) as List<JSONObject>
                    res.each { JSONObject r ->
                        rates << new MogopayRate(
                                id : r.get('id'),
                                code: r.get('currencyCode'),
                                name: r.get('currencyCode'),
                                rate: r.get('currencyRate') as Double,
                                currencyFractionDigits: r.get('currencyFractionDigits') as Integer)
                    }
                }
            }
            rates
        }
        finally{
            conn?.disconnect()
        }
    }
}
