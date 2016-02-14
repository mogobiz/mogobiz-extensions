/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.rivers.cache.BrandRiverCache
import com.mogobiz.elasticsearch.rivers.cache.CategoryFeaturesRiverCache
import com.mogobiz.elasticsearch.rivers.cache.CategoryRiverCache
import com.mogobiz.elasticsearch.rivers.cache.CouponsRiverCache
import com.mogobiz.elasticsearch.rivers.cache.FeatureRiverCache
import com.mogobiz.elasticsearch.rivers.cache.ResourceRiverCache
import com.mogobiz.elasticsearch.rivers.cache.ShippingRiverCache
import com.mogobiz.elasticsearch.rivers.cache.TagRiverCache
import com.mogobiz.elasticsearch.rivers.cache.TranslationsRiverCache
import com.mogobiz.geolocation.domain.Location
import com.mogobiz.http.client.HTTPClient
import com.mogobiz.store.domain.Brand
import com.mogobiz.store.domain.BrandProperty
import com.mogobiz.store.domain.Catalog
import com.mogobiz.store.domain.Category
import com.mogobiz.store.domain.Company
import com.mogobiz.store.domain.Coupon
import com.mogobiz.store.domain.Feature
import com.mogobiz.store.domain.FeatureValue
import com.mogobiz.store.domain.Ibeacon
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
import com.mogobiz.store.domain.ShippingRule
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
import static com.mogobiz.tools.FileTools.encodeFileBase64
import static com.mogobiz.tools.HashTools.generateMD5
import static com.mogobiz.tools.ImageTools.encodeImageBase64
import static com.mogobiz.tools.MimeTypeTools.detectMimeType
import com.mogobiz.utils.IperUtil
import com.mogobiz.utils.MogopayRate
import grails.converters.JSON
import grails.util.Holders
import groovy.json.JsonSlurper
import groovy.util.logging.Log4j
import org.apache.commons.lang.StringUtils
import org.codehaus.groovy.grails.web.json.JSONObject

import java.text.NumberFormat
import java.util.regex.Matcher
import java.util.regex.Pattern

/**
 *
 */
@Log4j
final class RiverTools {

    private RiverTools(){}

    def static final Pattern RESOURCE_VARIATION_VALUES = ~/(.*)__(.*)__(.*)/

    static Map translate(
            Map m,
            final Long id,
            final List<String> included = [],
            final List<String> languages = ['fr', 'en', 'es', 'de'],
            final String defaultLang = 'fr',
            boolean force = true){
        // translations for default language
        def _defaultLang = defaultLang.trim().toLowerCase()
        m[_defaultLang] = m[_defaultLang] as Map ?: [:]
        included.each {k ->
            m[_defaultLang][k] = m[k]
        }
        // translations for other languages
        def _languages = languages.collect {it.trim().toLowerCase()} - _defaultLang
        if(!_languages.flatten().isEmpty()){
            def final translationsPerLang = (TranslationsRiverCache.instance.get(id.toString()) ?: force ? Translation.createCriteria().list {
                eq ("target", id)
                inList("lang", _languages)
            } : []).groupBy {it.lang}
            _languages.each {lang ->
                // translated properties
                def translations = m[lang] as Map ?: [:]
                def items = translationsPerLang.get(lang)
                items?.each {translation ->
                    new JsonSlurper().parseText(translation.value).each {k, v ->
                        if(included.contains(k)){
                            translations[k] = v
                        }
                        else{
                            log.warn("$k not in ${included.join(",")}")
                        }
                    }
                }
                included.each {k ->
                    if(!translations[k]){
                        translations[k] = m[k]
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

    static Map asBrandMap(final Brand b, final RiverConfig config, boolean deep = false) {
        if(b){
            def m = BrandRiverCache.instance.get(b.uuid)
            if(!m){
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
                        config?.defaultLang,
                        false
                ) << [increments:0]

                StringBuffer buffer = new StringBuffer('/api/store/')
                        .append(config.clientConfig.store)
                        .append('/logos/')
                        .append(b.id)
                String url = retrieveResourceUrl(buffer.toString())
                m << [picture: url, smallPicture: "$url/SMALL"]

                if(deep){
                    def logo = null
                    def logos = retrieveBrandLogos(b.id, config)
                    if(logos && logos.size() > 0){
                        final file = logos.iterator().next()
                        logo = encodeImageBase64(file)
                    }
                    if(logo){
                        m << [content: logo]
                    }
                }

                b.brandProperties.each {BrandProperty property ->
                    m << ["${property.name}":property.value]
                }
                BrandRiverCache.instance.put(b.uuid, m)
            }
            return m
        }
        [:]
    }

    static File[] retrieveBrandLogos(Long brandId, RiverConfig config){
        String logoName = brandId.toString()
        StringBuffer buffer = new StringBuffer(Holders.config.resources.path as String)
                .append('/brands/logos/')
                .append(config.clientConfig.store)
        File dir = new File(buffer.toString())
        File[] files = dir.listFiles(
                new FilenameFilter() {
                    @Override
                    boolean accept(File d, String name) {
                        return name.startsWith(logoName + '.')
                    }
                }
        )
        files
    }

    static Map asCatalogMap(final Catalog catalog, final RiverConfig config){
        if(catalog){
            def m = translate(
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
            )
            return m
        }
        [:]
    }

    static Map asCategoryMap(Category category, RiverConfig config) {
        if(category){
            def m = CategoryRiverCache.instance.get(category.uuid)
            if(!m){
                m =  translate(
                        RenderUtil.asIsoMapForJSON(
                                [
                                        "id",
                                        "name",
                                        "description",
                                        "uuid",
                                        "keywords",
                                        "position",
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
                        config.defaultLang,
                        false
                ) << [path: retrieveCategoryPath(category, category.sanitizedName)] << [parentId:category.parent?.id] << [increments:0]
                def coupons = []
                extractCategoryCoupons(category).each {coupon ->
                    coupons << coupon.id
                }
                if(!coupons.isEmpty()){
                    m << [coupons:coupons]
                }
                CategoryRiverCache.instance.put(category.uuid, m)
            }
            return m
        }
        [:]
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
        if(tag){
            def m = TagRiverCache.instance.get(tag.uuid)
            if(!m){
                m = translate(
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
                    config.defaultLang,
                    false
                ) << [increments:0]
                TagRiverCache.instance.put(tag.uuid, m)
            }
            return m
        }
        [:]
    }

    static Map asSkuMap(TicketType sku, Product p = sku.product, RiverConfig config, boolean deep = false){
        if(sku){
            def m = RenderUtil.asIsoMapForJSON([
                    'id',
                    'uuid',
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
                    'position'
            ], sku)

            translate(m, sku.id, ['name', 'description'], config.languages, config.defaultLang, false)

            List<VariationValue> variations = []
            final variation1 = sku.variation1
            if(variation1){
                variations << variation1
            }
            m << [variation1:asVariationMap(variation1, config)]

            final variation2 = sku.variation2
            if(variation2){
                variations << variation2
            }
            m << [variation2:asVariationMap(variation2, config)]

            final variation3 = sku.variation3
            if(variation3){
                variations << variation3
            }
            m << [variation3:asVariationMap(variation3, config)]

            // liste des images associées à ce sku
            List<Map> resources = []
            final nbVariations = variations.size()
            Set<Product2Resource> bindedResources = p.product2Resources.findAll {it.resource.xtype = ResourceType.PICTURE}.toSet()
            bindedResources?.each {Product2Resource product2Resource ->
                Resource resource = product2Resource.resource
                def resourceName = resource.name
                Matcher matcher = RESOURCE_VARIATION_VALUES.matcher(resourceName)
                if (matcher.find() && matcher.groupCount() > 1){
                    final match = matcher.group(2)
                    final resourceVariationValues = match.split("_")
                    if(resourceVariationValues.size() == nbVariations &&
                        (0..(nbVariations-1)).every {index ->
                            final resourceVariationValue = resourceVariationValues[index]
                            final variationValue = variations.get(index)
                            !variationValue.variation.hide && (resourceVariationValue.equalsIgnoreCase('x') ||
                                    resourceVariationValue.equalsIgnoreCase(variationValue.value) ||
                                    resourceVariationValue.equals("${variationValue.position}"))
                        })
                    {
                        def map = asResourceMap(resource, config)
                        map << [variationValues: resourceVariationValues]
                        resources << map
                    }
                }
            }
            if(!resources.isEmpty()){
                m << [resources:resources]
            }

            final price = sku.price
            asPromotionsAndCouponsMap(extractSkuCoupons(sku), price, config).each {k, v ->
                m[k] = v
            }

            final countries = config.countries
            final taxRate = p.taxRate
            final salePrice = m.salePrice
            if(taxRate){
                Map<String, List<LocalTaxRate>> countryTaxRates = taxRate.localTaxRates?.groupBy {it.countryCode}
                countryTaxRates?.each {country, localTaxRates ->
                    def l = [:]
                    localTaxRates?.each {localTaxRate ->
                    final rate = localTaxRate.rate
                        if(localTaxRate.stateCode){
                            l << [endPrice: price]
                            def s = [:]
                            s << [endPrice: computeEndPrice(price, rate)]
                            if(salePrice){
                                l << [saleEndPrice: salePrice]
                                s << [saleEndPrice: computeEndPrice(salePrice as Long, rate)]
                            }
                            l << ["${localTaxRate.stateCode}": s]
                        }
                        else{
                            l << [endPrice: computeEndPrice(price, rate)]
                            if(salePrice){
                                l << [saleEndPrice: computeEndPrice(salePrice as Long, rate)]
                            }
                        }
                    }
                    l << [enabled: true]
                    m << ["${country}": l]
                    countries -= country
                }
            }
            countries.each{country ->
                def l = [:]
                l << [endPrice: price]
                if(salePrice){
                    l << [saleEndPrice: salePrice]
                }
                l << [enabled: false]
                m << ["${country}": l]
            }

            if(deep){
                Map _p = RenderUtil.asIsoMapForJSON([
                        "id",
                        "code",
                        "name",
                        "startFeatureDate",
                        "stopFeatureDate",
                        "dateCreated"
                ], p)

                _p << [xtype:p.xtype?.name()]

                translate(_p, p.id, ['name'], config.languages, config.defaultLang, false)

                if(p.category){
                    _p << [category:asCategoryMap(p.category, config)]
                }

                if(p.brand){
                    _p << [brand:asBrandMap(p.brand, config)]
                }

                def features = extractProductFeatures(p, config)
                if(!features.isEmpty()){
                    _p << [features:features]
                }

                def tags = []
                p.tags.each{ tag ->
                    tags << asTagMap(tag, config)
                }
                if(!tags.isEmpty()){
                    _p << [tags:tags]
                }

                _p << [taxRate:asTaxRateMap(taxRate, config)]

                m << [product: _p]
            }

            m << [available: isAvailable(sku)]

            m << asStockMap(sku)

            return m
        }
        [:]
    }

    private static Set<Coupon> extractSkuCoupons(TicketType sku) {
        Set<Coupon> coupons = CouponsRiverCache.instance.get(sku.uuid) ?: []
        coupons.addAll(extractProductCoupons(sku.product))
        coupons.flatten()
    }

    private static Set<Coupon> extractProductCoupons(Product product) {
        Set<Coupon> coupons = CouponsRiverCache.instance.get(product.uuid) ?: []
        coupons.addAll(extractCategoryCoupons(product.category))
        coupons.addAll(extractCatalogCoupons(product.category.catalog))
        coupons.flatten()
    }

    private static Set<Coupon> extractCategoryCoupons(Category category) {
        Set<Coupon> coupons = []
        categoryWithParents(category).each { Category c ->
            coupons.addAll(CouponsRiverCache.instance.get(c.uuid) ?: [])
        }
        coupons.flatten()
    }

    private static Set<Coupon> extractCatalogCoupons(Catalog catalog) {
        Set<Coupon> coupons = []
        coupons.addAll(CouponsRiverCache.instance.get(catalog.uuid) ?: [])
        coupons.flatten()
    }

    private static Map asPromotionsAndCouponsMap(Set<Coupon> coupons, Long price, RiverConfig config){
        def m = [:]
        def mCoupons = []
        Long reductions = 0L
        def mPromotions = []
        def mPromotion
        coupons.flatten().each {Coupon coupon ->
            mCoupons << [id: coupon.id]
            if(coupon.active && coupon.anonymous && (coupon.startDate == null || coupon.startDate?.compareTo(Calendar.getInstance()) <= 0) && (coupon.endDate == null || coupon.endDate?.compareTo(Calendar.getInstance()) >= 0)){
                def reduction = calculerReduction(coupon, price)
                def map = [description: coupon.description?:"", name: coupon.name, pastille: coupon.pastille, reduction: reduction]
                translate(map, coupon.id, ['name', 'pastille'], config.languages, config.defaultLang, false)
                mPromotions << map
                if(reduction > reductions){
                    reductions = reduction
                    mPromotion = map
                }
                else if(!mPromotion){
                    mPromotion = map
                }
            }
        }
        if(!mCoupons.isEmpty()){
            m << [coupons: mCoupons]
            m << [promotions: mPromotions]
            m << [promotion: mPromotion]
            m << [salePrice: Math.max(0, price - reductions)]
        }
        else{
            m << [salePrice: price]
        }
        m
    }

    private static Long calculerReduction(Coupon coupon, Long prixDeBase){
        def reduction = 0
        coupon.rules?.each {rule ->
            switch (rule.xtype) {
                case ReductionRuleType.DISCOUNT:
                    String discount = rule.discount
                    boolean isPercent = discount?.endsWith("%")
                    boolean isMinus = discount?.startsWith("-")
                    boolean isPlus = discount?.startsWith("+")
                    String discountWithoutSigns = discount?.substring((isMinus || isPlus)? 1 : 0, (isPercent) ? discount.length()-1 : discount.length())
                    Long reduc = (isPercent) ? (Long)(prixDeBase * Float.parseFloat(discountWithoutSigns) / 100) : Long.parseLong(discountWithoutSigns)
                    if (isPlus) {
                        //should never be the case
                        reduction -= reduc
                    }
                    else {
                        reduction += reduc
                    }
                    break
                default:
                    break
            }
        }
        reduction
    }

    static Map asVariationMap(VariationValue variationValue, RiverConfig config){
        if(variationValue && !variationValue.variation?.hide){
            def m = RenderUtil.asIsoMapForJSON(['value'], variationValue) << RenderUtil.asIsoMapForJSON([
                    'name', 'position', 'uuid', 'hide'], variationValue.variation)
            translate(m, variationValue.id, ['value'], config.languages, config.defaultLang, false)
            translate(m, variationValue.variation.id, ['name'], config.languages, config.defaultLang, false)
            return m
        }
        [:]
    }

    static Map asResourceMap(Resource resource, RiverConfig config) {
        if(resource){
            def m = ResourceRiverCache.instance.get(resource.uuid)
            if(!m){
                m = RenderUtil.asIsoMapForJSON([
                        'id',
                        'uuid',
                        'name',
                        'description',
                        'xtype',
                        'active',
                        'deleted',
                        'uploaded',
                        'contentType',
                        'sanitizedName'
                ], resource) << [url:extractResourceUrl(resource, config)]
                def content = resource.content
                if(!content && resource.uploaded){
                    def path = extractResourcePath(resource)
                    def file = new File(path)
                    if(file.exists()){
                        content = encodeImageBase64(file)
                    }
                    else{
                        log.warn("${path} not found")
                    }
                }
                if(content){
                    m << [content: content]
                    m << [md5: generateMD5(content)]
                }
                if(ResourceType.PICTURE.equals(resource.xtype)){
                    m << [smallPicture:extractSmallPictureUrl(resource, config)]
                }
                translate(m, resource.id, ['name', 'description'], config.languages, config.defaultLang, false)
                ResourceRiverCache.instance.put(resource.uuid, m)
            }
            return m
        }
        [:]
    }

    def static String extractResourcePath(Resource resource) {
        def path = resource?.url
        if(!path || !new File(path).exists()){
            StringBuilder sb = new StringBuilder(Holders.config.resources.path as String).append(File.separator).append('resources').append(File.separator)
            sb.append(resource.company.code).append(File.separator)
            def contentType = resource.contentType
            if (contentType != null) {
                if (contentType.toLowerCase().contains('audio')) {
                    sb.append('audio')
                } else if (contentType.toLowerCase().contains('video')) {
                    sb.append('video')
                } else if (contentType.toLowerCase().contains('image')) {
                    sb.append('image')
                } else if (contentType.toLowerCase().contains('text')) {
                    sb.append('text')
                }
                sb.append(File.separator)
            }
            path = sb.append(resource.id).toString()
        }
        path
    }

    static Map asFeatureMap(Feature feature, FeatureValue featureValue, RiverConfig config) {
        if(feature){
            def key = "${feature.uuid}-${featureValue?.uuid}"
            def m = FeatureRiverCache.instance.get(key)
            if(!m){
                m = RenderUtil.asIsoMapForJSON([
                    'name',
                    'position',
                    'domain',
                    'uuid',
                    'hide'
            ], feature) << [value: featureValue?.value ?: feature.value]
            translate(m, feature.id, ['name', 'value'], config.languages, config.defaultLang, false)
            if(featureValue){
                translate(m, featureValue.id, ['value'], config.languages, config.defaultLang, false)
            }
                FeatureRiverCache.instance.put(key, m)
            }
            return m
        }
        [:]
    }

    static Map asShippingMap(Shipping shipping, RiverConfig config) {
        if(shipping){
            def m = ShippingRiverCache.instance.get(shipping.uuid)
            if(!m){
                m = RenderUtil.asIsoMapForJSON([
                    'name',
                    'weight',
                    'width',
                    'height',
                    'depth',
                    'amount',
                    'free'
                ], shipping)
                if(shipping.weightUnit){
                    m << [weightUnit: shipping.weightUnit.name()]
                }
                if(shipping.linearUnit){
                    m << [linearUnit: shipping.linearUnit.name()]
                }
                translate(m, shipping.id, ['name'], config.languages, config.defaultLang, false)
                ShippingRiverCache.instance.put(shipping.uuid, m)
            }
            return m
        }
        [:]
    }

    static Map asPoiMap(Poi poi, RiverConfig config) {
        def mpoi = [:]
        mpoi << [name: poi.name]
        mpoi << [description: poi.description]
        mpoi << [picture: poi.picture]
        mpoi << [xtype: poi.poiType?.xtype]
        translate(mpoi, poi.id, ['name', 'description'], config.languages, config.defaultLang, false)
        mpoi << [location: asLocationMap(poi, config)]
        mpoi
    }

    static Map asLocationMap(Location location, RiverConfig config){
        location ? RenderUtil.asIsoMapForJSON(
                [
                        'latitude',
                        'longitude',
                        'road1',
                        'road2',
                        'road3',
                        'roadNum',
                        'postalCode',
                        'state',
                        'city'
                ], location) << [country: [code: location.countryCode]] : [:]
    }

    static Map asProductMap(Product p, RiverConfig config) {
        if(p){
            Map m = RenderUtil.asIsoMapForJSON([
                    "id",
                    "code",
                    "name",
                    "description",
                    "descriptionAsText",
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

            translate(m, p.id, ['name', 'description', 'descriptionAsText', 'keywords'], config.languages, config.defaultLang, false)

            List<Product2Resource> bindedResources = p.product2Resources.findAll {it.resource.xtype = ResourceType.PICTURE}.toList()
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

            def features = extractProductFeatures(p, config)
            if(!features.isEmpty()){
                m << [features:features]
            }

            def tags = []
            p.tags.each{ tag ->
                tags << asTagMap(tag, config)
            }
            if(!tags.isEmpty()){
                m << [tags:tags]
            }

            List<Map> skus = []
            p.ticketTypes/*.findAll {!it.stopDate || it.stopDate.after(new Date())}*/.each {sku ->
                skus << asSkuMap(sku, p, config)
            }
            if(!skus.isEmpty()){
                m << [skus:skus]
            }


            final maxPrice = skus?.collect { it.price as Long  ?: 0L}?.max() ?: 0L
            m << [maxPrice: maxPrice]

            final maxSalePrice = skus?.collect { it.salePrice as Long ?: 0L }?.max() ?: 0L
            m << [maxSalePrice: maxSalePrice]

            final minPrice = skus?.collect { it.price as Long ?: [] }?.flatten()?.min() ?: 0L
            m << [minPrice: minPrice]

            final minSalePrice = skus?.collect { it.salePrice as Long ?: [] }?.flatten()?.min() ?: 0L
            m << [minSalePrice: minSalePrice]

            Set<Long> skuResources = []
            skus?.each {sku ->
                if(sku.containsKey("resources")){
                    skuResources.addAll((sku.resources as List<Map>).groupBy {it.id as Long}.keySet())
                }
            }
            def resources = []
            p.product2Resources.each {Product2Resource pr ->
                final r = pr.resource
                if(!skuResources.contains(r.id)){
                    resources << asResourceMap(r, config)
                }
            }
            if(!resources.isEmpty()){
                m << [resources:resources]
            }

            if (p.calendarType != ProductCalendar.NO_DATE) {
                def datePeriods = []
                p.datePeriods.sort {a,b -> a.startDate <=> b.startDate}.each {datePeriod ->
                    datePeriods << RenderUtil.asIsoMapForJSON(['startDate', 'endDate'], datePeriod)
                }
                if(!datePeriods.isEmpty()){
                    m << [datePeriods:datePeriods]
                }

                def intraDayPeriods = []
                p.intraDayPeriods.sort {a,b -> a.startDate <=> b.startDate}.each {intraDayPeriod ->
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

            p.productProperties.each {ProductProperty property ->
                m << ["${property.name}":property.value]
            }

            final price = minPrice as Long
            m << [price: price]

            asPromotionsAndCouponsMap(extractProductCoupons(p), price, config).each {k, v ->
                m[k] = v
            }

            final countries = config.countries
            final taxRate = p.taxRate
            final salePrice = m.salePrice
            if(taxRate){
                Map<String, List<LocalTaxRate>> countryTaxRates = taxRate.localTaxRates?.groupBy {it.countryCode}
                countryTaxRates?.each {country, localTaxRates ->
                    def l = [:]
                    localTaxRates?.each {localTaxRate ->
                        final rate = localTaxRate.rate
                        if(localTaxRate.stateCode){
                            l << [endPrice: price]
                            l << [maxEndPrice: maxPrice]
                            l << [maxSaleEndPrice: maxSalePrice]
                            l << [minEndPrice: minPrice]
                            l << [minSaleEndPrice: minSalePrice]
                            def s = [:]
                            s << [endPrice: computeEndPrice(price, rate)]
                            s << [maxEndPrice: computeEndPrice(maxPrice as Long, rate)]
                            s << [maxSaleEndPrice: computeEndPrice(maxSalePrice as Long, rate)]
                            s << [minEndPrice: computeEndPrice(minPrice as Long, rate)]
                            s << [minSaleEndPrice: computeEndPrice(minSalePrice as Long, rate)]
                            if(salePrice){
                                l << [saleEndPrice: salePrice]
                                s << [saleEndPrice: computeEndPrice(salePrice as Long, rate)]
                            }
                            l << ["${localTaxRate.stateCode}": s]
                        }
                        else{
                            l << [endPrice: computeEndPrice(price, rate)]
                            l << [maxEndPrice: computeEndPrice(maxPrice as Long, rate)]
                            l << [maxSaleEndPrice: computeEndPrice(maxSalePrice as Long, rate)]
                            l << [minEndPrice: computeEndPrice(minPrice as Long, rate)]
                            l << [minSaleEndPrice: computeEndPrice(minSalePrice as Long, rate)]
                            if(salePrice){
                                l << [saleEndPrice: computeEndPrice(salePrice as Long, rate)]
                            }
                        }
                    }
                    l << [enabled: true]
                    m << ["${country}": l]
                    countries -= country
                }
            }
            countries.each{country ->
                def l = [:]
                l << [endPrice: price]
                l << [maxEndPrice: maxPrice]
                l << [maxSaleEndPrice: maxSalePrice]
                l << [minEndPrice: minPrice]
                if(salePrice){
                    l << [saleEndPrice: salePrice]
                }
                l << [enabled: false]
                m << ["${country}": l]
            }

            m << [stockAvailable: p.ticketTypes.any { sku ->
                isAvailable(sku)
            }]

            return m
        }
        [:]
    }

    def static List<Map> extractProductFeatures(Product p, RiverConfig config) {
        def features = []
        Set<Feature> productFeatures = p.features
        categoryWithParents(p.category).each { cat ->
            productFeatures.addAll(CategoryFeaturesRiverCache.instance.get(cat.uuid) ?: [])
        }
        productFeatures.flatten()

        final featureValues = p.featureValues
        productFeatures.each { feature ->
            final featureValue = featureValues.find { it.feature.id == feature.id }
            features << asFeatureMap(feature, featureValue, config)
        }
        features
    }

    static Map asStockCalendarSkuMap(StockCalendarSku tuple, RiverConfig config){
        TicketType sku = tuple.sku
        if(sku) {
            def m = RenderUtil.asIsoMapForJSON([
                    'id',
                    'sku',
                    'uuid',
                    'startDate',
                    'stopDate',
                    'availabilityDate',
                    'dateCreated',
                    'lastUpdated'
            ], sku)
            def product = sku.product
            if (product) {
                m << [productId: product.id]
                m << [productUuid: product.uuid]
                m << [stockDisplay: product.stockDisplay]
                m << [calendarType: product.calendarType]
            }
            def stock = sku.stock
            if (stock) {
                m << [initialStock: stock.stock]
                m << [stockUnlimited: stock.stockUnlimited]
                m << [stockOutSelling: stock.stockOutSelling]
                if(!stock.stockUnlimited){
                    def stockCalendars = tuple.stockCalendars
                    if(stockCalendars){
                        // Il y a eu des ventes, on prend le stock restant
                        if (product?.calendarType == ProductCalendar.NO_DATE) {
                            StockCalendar stockCalendar = stockCalendars.size() > 0 ? stockCalendars.first() : null
                            if (stockCalendar) {
                                m << [stock: Math.max(0, stockCalendar.stock - stockCalendar.sold)]
                            }
                            else
                            {
                                m << [stock: stock.stock]
                            }
                        }
                        else{
                            def stockByDateTime = []
                            stockCalendars.each {stockCalendar ->
                                stockByDateTime << (RenderUtil.asIsoMapForJSON(['id', 'uuid', 'startDate', 'dateCreated', 'lastUpdated'], stockCalendar)
                                        << [stock: Math.max(0, stockCalendar.stock - stockCalendar.sold)])
                            }
                            if(!stockByDateTime.isEmpty()){
                                m << [stockByDateTime: stockByDateTime]
                            }
                        }
                    }
                    else {
                        // Il n'y a pas eu de vente, on prend le stock initial
                        m << [stock: stock.stock]
                    }
                }
            }
            return m
        }
        [:]
    }

    static Map asStockMap(TicketType sku){
        def stock = sku?.stock
        def product = sku?.product
        def m = [:]
        if(product && stock){
            m << [stockDisplay: product.stockDisplay]
            m << [calendarType: product.calendarType]
            m << [initialStock: stock.stock]
            m << [stockUnlimited: stock.stockUnlimited]
            m << [stockOutSelling: stock.stockOutSelling]
            if(!stock.stockUnlimited){
                def stockCalendars = sku.stockCalendars
                if(stockCalendars){
                    // Il y a eu des ventes, on prend le stock restant
                    if (product?.calendarType == ProductCalendar.NO_DATE) {
                        StockCalendar stockCalendar = stockCalendars.size() > 0 ? stockCalendars.first() : null
                        if (stockCalendar) {
                            m << [stock: Math.max(0, stockCalendar.stock - stockCalendar.sold)]
                        }
                        else
                        {
                            m << [stock: stock.stock]
                        }
                    }
                    else{
                        def byDateTime = []
                        stockCalendars.each {stockCalendar ->
                            final _stock = Math.max(0, stockCalendar.stock - stockCalendar.sold)
                            byDateTime << (RenderUtil.asIsoMapForJSON(['id', 'uuid', 'startDate', 'dateCreated', 'lastUpdated'], stockCalendar)
                                    << [stock: _stock] << [available: _stock > 0])
                        }
                        if(!byDateTime.isEmpty()){
                            m << [byDateTime: byDateTime]
                        }
                    }
                }
                else {
                    // Il n'y a pas eu de vente, on prend le stock initial
                    m << [stock: stock.stock]
                }
            }
        }
        m
    }

    static boolean isAvailable(TicketType sku){
        boolean ret = false
        def stock = sku?.stock
        if(stock){
            ret = stock.stockUnlimited || stock.stockOutSelling
            if(!ret){
                def stockCalendars = sku.stockCalendars
                if(stockCalendars){
                    if (sku?.product?.calendarType == ProductCalendar.NO_DATE) {
                        StockCalendar stockCalendar = stockCalendars.size() > 0 ? stockCalendars.first() : null
                        if (stockCalendar) {
                            ret = Math.max(0, stockCalendar.stock - stockCalendar.sold) > 0
                        }
                        else
                        {
                            ret = stock.stock > 0
                        }
                    }
                    else{
                        // TODO
                        ret = true
                    }
                }
                else{
                    ret = stock.stock > 0
                }
            }
        }
        ret
    }

    static Map asSuggestionMap(Suggestion suggestion, RiverConfig config){
        suggestion ?
                asProductMap(suggestion.product, config) << RenderUtil.asIsoMapForJSON(
                            [
                                'id',
                                'required',
                                'position',
                                'discount'
                            ],
                            suggestion
                ) << [price: IperUtil.computeDiscount(suggestion.discount, suggestion.product.price)] << [productId: suggestion.product.id] : [:]
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

    static String extractSkuResourceUrl(Resource resource, RiverConfig config) {
        String url = resource?.url;
        if (resource?.uploaded) {
            StringBuffer buffer = new StringBuffer('/api/store/')
                    .append(config.clientConfig.store)
                    .append('/resources/')
                    .append("${resource.id}${resource.name.toLowerCase()}")
            url = buffer.toString()
        }
        return retrieveResourceUrl(url)
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
            url = Holders.config.resources.url + (url - Holders.config.resources.path);
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
            map <<  RenderUtil.asIsoMapForJSON(['id', 'code', 'name', 'description', 'active', 'numberOfUses', 'startDate', 'endDate', 'catalogWise', 'anonymous', 'pastille'], coupon)
            map << ['consumed': coupon.consumed]
            translate(map, coupon.id, ['name', 'pastille'], config.languages, config.defaultLang, false)
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
                                id : -1L,
                                uuid: r.get('uuid'),
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

    static Map asShippingRuleMap(ShippingRule shippingRule, RiverConfig config){
        def m = [:]
        if(shippingRule){
            m = RenderUtil.asIsoMapForJSON(
                    [
                            'id',
                            'uuid',
                            'dateCreated',
                            'lastUpdated',
                            'countryCode',
                            'minAmount',
                            'maxAmount'
                    ],
                    shippingRule
            )
            def absolute = true
            def percentage = false
            def price = shippingRule.price
            if(price.startsWith("-")){
                absolute = false
                price = price.substring(1)
            }
            else if(price.startsWith("+")){
                price = price.substring(1)
            }
            m << [absolute:absolute]
            if(price?.endsWith("%")){
                percentage = true
                price = price.substring(0, price.length()-1)
            }
            m << [percentage:percentage]
            m << [price:Long.parseLong(price)]
        }
        m
    }

    static Map asCompanyMap(Company company, RiverConfig config){
        company ?  RenderUtil.asIsoMapForJSON([
                'id',
                'name',
                'code',
                'uuid',
                'aesPassword',
                'phone',
                'shippingInternational'
        ], company) <<
                [shipFrom: asLocationMap(company.shipFrom, config)] <<
                [location: asLocationMap(company.location, config)] : [:]
    }

    static Map asDownloadableMap(File file, RiverConfig config){
        if(file?.exists()) {
            [
                    id: file.name,
                    file: [
                            content: encodeFileBase64(file),
                            content_type: detectMimeType(file)
                    ]
            ]
        }
        else
            [:]
    }

    static Long computeEndPrice(Long price, Float rate){
        if(price && rate){
            return price + (price * rate / 100f).toLong()
        }
        price
    }
}





