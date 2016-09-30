/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.service

import com.mogobiz.common.client.BulkAction
import com.mogobiz.common.client.ClientConfig
import com.mogobiz.common.client.Credentials
import com.mogobiz.common.rivers.AbstractRiverCache
import com.mogobiz.common.rivers.GenericRiversFlow
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.rivers.RiverTools
import com.mogobiz.mirakl.client.MiraklClient
import com.mogobiz.mirakl.client.MiraklSftpConfig
import com.mogobiz.mirakl.client.domain.Attribute
import com.mogobiz.mirakl.client.domain.AttributeType
import com.mogobiz.mirakl.client.domain.MiraklApi
import com.mogobiz.mirakl.client.domain.MiraklAttribute
import com.mogobiz.mirakl.client.domain.MiraklAttributeValue
import com.mogobiz.mirakl.client.domain.MiraklProduct
import com.mogobiz.mirakl.client.domain.MiraklReportItem
import com.mogobiz.mirakl.client.domain.OutputShop
import com.mogobiz.mirakl.client.domain.ProductReference
import com.mogobiz.mirakl.client.domain.SynchronizationStatus
import com.mogobiz.mirakl.client.io.ImportOffersResponse
import com.mogobiz.mirakl.client.io.SearchShopsRequest
import com.mogobiz.mirakl.rivers.OfferRiver
import com.mogobiz.store.cmd.PagedListCommand
import com.mogobiz.store.domain.Brand
import com.mogobiz.store.domain.Coupon
import com.mogobiz.store.domain.Product
import com.mogobiz.store.domain.ProductCalendar
import com.mogobiz.store.domain.ProductState
import com.mogobiz.store.domain.ProductType
import com.mogobiz.store.domain.ReductionRule
import com.mogobiz.store.domain.ReductionRuleType
import com.mogobiz.store.domain.Resource
import com.mogobiz.store.domain.ResourceType
import com.mogobiz.store.domain.Seller
import com.mogobiz.store.domain.Stock
import com.mogobiz.store.domain.TicketType
import com.mogobiz.store.domain.Variation
import com.mogobiz.store.domain.VariationValue
import com.mogobiz.tools.CsvLine
import com.mogobiz.tools.Reader
import com.mogobiz.utils.PermissionType
import rx.Observable
import rx.Subscriber
import rx.functions.Action1
import rx.internal.reactivestreams.SubscriberAdapter
import scala.Some

import static com.mogobiz.mirakl.client.MiraklClient.*
import com.mogobiz.mirakl.client.domain.MiraklCategory
import com.mogobiz.mirakl.client.domain.MiraklHierarchy
import com.mogobiz.mirakl.client.domain.MiraklValue
import com.mogobiz.store.domain.Catalog
import com.mogobiz.store.domain.Category
import com.mogobiz.store.domain.Company
import com.mogobiz.store.domain.MiraklEnv
import com.mogobiz.store.domain.MiraklSync
import com.mogobiz.store.domain.MiraklSyncStatus
import com.mogobiz.store.domain.MiraklSyncType
import com.mogobiz.store.domain.Translation
import org.hibernate.FlushMode

import static com.mogobiz.tools.ScalaTools.*

import static com.mogobiz.elasticsearch.rivers.RiverTools.*

class MiraklService {

    static transactional = false

    def sanitizeUrlService

    def grailsApplication

    def catalogService

    def profileService

    def publish(Company company, MiraklEnv env, Catalog catalog, boolean manual = false) {
        if (catalog?.name?.trim()?.toLowerCase() == "impex") {
            return
        }
        if (company && env && env.company == company && !env.running && catalog && catalog.company == company && (manual || catalog.activationDate < new Date())) {
            log.info("${manual ? "Manual " : ""}Export to Mirakl has started ...")
            final readOnly = catalog.readOnly
            if(readOnly){
                catalogService.refreshMiraklCatalog(catalog)
                env = catalog.miraklEnv
            }
            MiraklEnv.withTransaction {
                env.refresh()
                env.running = true
                env.save(flush: true)
            }
            def languages = Translation.executeQuery('SELECT DISTINCT t.lang FROM Translation t WHERE t.companyId=:idCompany', [idCompany: company.id]) as List<String>
            if (languages.size() == 0) {
                languages = [company.defaultLanguage] as List<String>
            }
            def debug = true

            RiverConfig config = new RiverConfig(
                    debug: true,
                    clientConfig: new ClientConfig(
                            store: company.code,
                            merchant_id: env.shopId,
                            merchant_url: env.url,
                            debug: debug,
                            credentials: new Credentials(
                                    frontKey: env.frontKey as String,
                                    apiKey: env.apiKey
                            )
                    ),
                    idCatalogs: [catalog.id] as List<Long>,
                    languages: languages,
                    defaultLang: company.defaultLanguage,
                    bulkSize: 100 //TODO add to grails application configuration
            )

            // 0. Load catalog categories
            Set<Category> categories = Category.executeQuery(
                    'select cat FROM Category cat left join fetch cat.parent left join fetch cat.features as feature left join fetch feature.values left join fetch cat.variations as variation left join fetch variation.variationValues where cat.catalog.id in (:idCatalogs) and cat.publishable=true and cat.deleted=false',
                    [idCatalogs:config.idCatalogs],
                    [readOnly: true, flushMode: FlushMode.MANUAL]
            ).toSet()

            List<MiraklCategory> hierarchies = []
            def values = []
            List<MiraklAttribute> attributes = []

            categories.each {category ->
                def parent = category.parent
                def hierarchyCode = extractMiraklExternalCode(category.externalCode) ?: miraklCategoryCode(category)
                def hierarchyPath = categoryFullPath(category)
                hierarchies << new MiraklCategory(
                        hierarchyCode,
                        category.name,
                        BulkAction.UPDATE,
                        parent ? Some.apply(new MiraklCategory(extractMiraklExternalCode(parent.externalCode) ?: miraklCategoryCode(parent), "")) : toScalaOption((MiraklCategory)null),
                        category.logisticClass,
                        category.uuid,
                        toScalaOption((String)null),
                        toScalaOption((String)null)
                )
                category.features?.each {feature ->
                    def featureCode = extractMiraklExternalCode(feature.externalCode) ?: "${hierarchyCode}_${sanitizeUrlService.sanitizeWithDashes(feature.name)}"
                    def featureLabel = "${feature.name} feature ($hierarchyPath)"
                    def featureListValues = new MiraklValue(
                            "$featureCode-list",
                            featureLabel
                    )
                    def codes = []
                    feature.values.collect { featureValue ->
                        def val = "${featureValue.value}"
                        def code = extractMiraklExternalCode(featureValue.externalCode) ?: sanitizeUrlService.sanitizeWithDashes(val)
                        codes << code
                        values << new MiraklValue(code, val, Some.apply(featureListValues))
                    }
                    attributes << new MiraklAttribute(new Attribute(
                            code: featureCode,
                            label: feature.name,
                            hierarchyCode: hierarchyCode,
                            type: AttributeType.LIST_MULTIPLE_VALUES,
                            typeParameter: featureListValues.code,
                            defaultValue: codes.join("|"),
                            variant: false,
                            required: false
                    ))
                }
                category.variations?.each { variation ->
                    def variationCode = extractMiraklExternalCode(variation.externalCode) ?: "${hierarchyCode}_${sanitizeUrlService.sanitizeWithDashes(variation.name)}"
                    def variationlabel = "${variation.name} variation ($hierarchyPath)"
                    def variationListValues = new MiraklValue(
                            "$variationCode-list",
                            variationlabel
                    )
                    attributes << new MiraklAttribute(new Attribute(
                            code: variationCode,
                            label: variation.name,
                            hierarchyCode: hierarchyCode,
                            type: AttributeType.LIST,
                            typeParameter: variationListValues.code,
                            variant: true,
                            required: false
                    ))
                    variation.variationValues.each { variationValue ->
                        def val = "${variationValue.value}"
                        def code = extractMiraklExternalCode(variationValue.externalCode) ?: sanitizeUrlService.sanitizeWithDashes(val)
                        values << new MiraklValue(code, val, Some.apply(variationListValues))
                    }
                }
            }

            if(!readOnly && env.operator){
                // 1. synchronize categories
                final synchronizeCategories = synchronizeCategories(config, hierarchies)
                final synchronizeCategoriesId = synchronizeCategories?.synchroId
                if(synchronizeCategoriesId){
                    MiraklSync.withTransaction {
                        def sync = new MiraklSync()
                        sync.miraklEnv = env
                        sync.company = company
                        sync.catalog = catalog
                        sync.type = MiraklSyncType.CATEGORIES
                        sync.status = MiraklSyncStatus.QUEUED
                        sync.timestamp = new Date()
                        sync.trackingId = synchronizeCategoriesId
                        sync.validate()
                        if(!sync.hasErrors()){
                            sync.save(flush: true)
                        }
                        synchronizeCategories.ids?.each{uuid ->
                            def cat = Category.findByUuid(uuid)
                            if(cat){
                                cat.miraklStatus = sync.status
                                cat.miraklTrackingId = sync.trackingId
                                cat.validate()
                                if(!cat.hasErrors()){
                                    cat.save(flush: true)
                                }
                            }
                        }
                    }
                }

                // 2. Import product Hierarchy
                final importHierarchiesId = synchronizeCategoriesId ? importHierarchies(config, hierarchies.collect {new MiraklHierarchy(it)})?.importId : null
                if(importHierarchiesId){
                    MiraklSync.withTransaction {
                        def sync = new MiraklSync()
                        sync.miraklEnv = env
                        sync.company = company
                        sync.catalog = catalog
                        sync.type = MiraklSyncType.HIERARCHIES
                        sync.status = MiraklSyncStatus.QUEUED
                        sync.timestamp = new Date()
                        sync.trackingId = importHierarchiesId.toString()
                        sync.validate()
                        if(!sync.hasErrors()){
                            sync.save(flush: true)
                        }
                    }
                }

                // 3. Import List of Values
                final importValuesId = importHierarchiesId ? importValues(config, values)?.importId : null
                if(importValuesId){
                    MiraklSync.withTransaction {
                        def sync = new MiraklSync()
                        sync.miraklEnv = env
                        sync.company = company
                        sync.catalog = catalog
                        sync.type = MiraklSyncType.VALUES
                        sync.status = MiraklSyncStatus.QUEUED
                        sync.timestamp = new Date()
                        sync.trackingId = importValuesId.toString()
                        sync.validate()
                        if(!sync.hasErrors()){
                            sync.save(flush: true)
                        }
                    }
                }

                // 4. Import Attributes
                final importAttributesId = importValuesId ? importAttributes(config, attributes)?.importId : null
                if(importAttributesId){
                    MiraklSync.withTransaction {
                        def sync = new MiraklSync()
                        sync.miraklEnv = env
                        sync.company = company
                        sync.catalog = catalog
                        sync.type = MiraklSyncType.ATTRIBUTES
                        sync.status = MiraklSyncStatus.QUEUED
                        sync.timestamp = new Date()
                        sync.trackingId = importAttributesId.toString()
                        sync.validate()
                        if(!sync.hasErrors()){
                            sync.save(flush: true)
                        }
                    }
                }
            }

            // 5. Import Offers + Products
            final List<String> offersHeader = []
            offersHeader.addAll([
                    MiraklApi.category(),
                    MiraklApi.identifier(),
                    MiraklApi.title(),
                    MiraklApi.variantIdentifier(),
                    MiraklApi.description(),
                    MiraklApi.media(),
                    MiraklApi.brand(),
                    MiraklApi.productReferences()
            ]) // mogobiz product attributes mapping
            offersHeader.addAll(attributes.collect {it.code}) // features + variations attributes
            offersHeader.addAll(MiraklApi.offersHeader().split(";")) // offer headers
            config.clientConfig.config = [:] << [offersHeader: offersHeader.unique {a, b -> a <=> b}.join(";")]
            def subscriber = new Subscriber<ImportOffersResponse>(){
                final long before = System.currentTimeMillis()

                @Override
                void onCompleted() {
                    log.info("export within ${System.currentTimeMillis() - before} ms")
                    AbstractRiverCache.purgeAll()

                    MiraklEnv.withTransaction {
                        env.refresh()
                        env.running = false
                        env.save(flush: true)
                    }
                }

                @Override
                void onError(Throwable th) {
                    AbstractRiverCache.purgeAll()
                    log.error(th.message, th)
                    MiraklEnv.withTransaction {
                        env.refresh()
                        env.running = false
                        env.save(flush: true)
                    }
                }

                @Override
                void onNext(ImportOffersResponse importOffersResponse) {
                    final importId = importOffersResponse?.importId
                    if(importId){
                        MiraklSync.withTransaction {
                            def sync = new MiraklSync()
                            sync.miraklEnv = env
                            sync.company = company
                            sync.catalog = catalog
                            sync.type = MiraklSyncType.OFFERS
                            sync.status = MiraklSyncStatus.QUEUED
                            sync.timestamp = new Date()
                            sync.trackingId = importId.toString()
                            sync.validate()
                            if(!sync.hasErrors()){
                                sync.save(flush: true)
                                importOffersResponse.ids?.each{ id ->
                                    def sku = TicketType.findAllByExternalCodeLikeOrUuid("%mirakl::$id%", id).find {it.product.category.catalog == catalog}
                                    if(sku){
                                        sku.miraklStatus = sync.status
                                        sku.miraklTrackingId = sync.trackingId
                                        sku.validate()
                                        if(!sku.hasErrors()){
                                            sku.save(flush: true)
                                        }
                                    }
                                }
                            }
                        }
                    }
                    final productImportId = importOffersResponse?.productImportId
                    if(productImportId){
                        MiraklSync.withTransaction {
                            def sync = new MiraklSync()
                            sync.miraklEnv = env
                            sync.company = company
                            sync.catalog = catalog
                            sync.type = MiraklSyncType.PRODUCTS
                            sync.status = MiraklSyncStatus.QUEUED
                            sync.timestamp = new Date()
                            sync.trackingId = productImportId.toString()
                            sync.validate()
                            if(!sync.hasErrors()){
                                sync.save(flush: true)
                            }
                        }
                    }
                    final productSynchroId = importOffersResponse?.productSynchroId
                    if(productSynchroId){
                        MiraklSync.withTransaction {
                            def sync = new MiraklSync()
                            sync.miraklEnv = env
                            sync.company = company
                            sync.catalog = catalog
                            sync.type = MiraklSyncType.PRODUCTS_SYNCHRO
                            sync.status = MiraklSyncStatus.QUEUED
                            sync.timestamp = new Date()
                            sync.trackingId = productSynchroId.toString()
                            sync.validate()
                            if(!sync.hasErrors()){
                                sync.save(flush: true)
                                importOffersResponse.productIds?.each{ productId ->
                                    def sku = TicketType.findAllByExternalCodeLikeOrUuid("%mirakl::$productId%", productId).find {it.product.category.catalog == catalog}
                                    if(sku){
                                        sku.miraklProductStatus = sync.status
                                        sku.miraklProductTrackingId = sync.trackingId
                                        sku.validate()
                                        if(!sku.hasErrors()){
                                            sku.save(flush: true)
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            GenericRiversFlow.synchronize(
                    new OfferRiver(),
                    config,
                    Math.max(1, Runtime.getRuntime().availableProcessors()),
                    new SubscriberAdapter(subscriber)
            )

        }
    }

    def synchronize(Catalog catalog){
        final readOnly = catalog?.readOnly
        if(readOnly){
            catalogService.refreshMiraklCatalog(catalog)

            def env = catalog.miraklEnv
            if (env.frontKey && env.remoteHost && env.remotePath && env.username && env.localPath && (env.password || env.keyPath)) {
                // TODO add creation validation
                // 1 - synchronize products
                startSynchronizeProducts(env, catalog)
                // 2 - synchronize offers
                synchronizeOffers(env, catalog)
            }
        }
        else {
            def excludedStatus = [MiraklSyncStatus.COMPLETE, MiraklSyncStatus.CANCELLED, MiraklSyncStatus.FAILED, MiraklSyncStatus.TRANSFORMATION_FAILED]
            def toSynchronize = catalog ?
                    MiraklSync.findAllByStatusNotInListAndCatalog(excludedStatus, catalog) :
                    MiraklSync.findAllByStatusNotInList(excludedStatus)
            def operators = !catalog ? MiraklEnv.findAllByOperator(true) : (toSynchronize.collect {
                it.miraklEnv
            }.flatten().findAll { it.operator }.unique { a, b -> a.id <=> b.id })
            // 1 - synchronize products and offers
            operators.each { env ->
                if (env.frontKey && env.remoteHost && env.remotePath && env.username && env.localPath && (env.password || env.keyPath)) {
                    // TODO add creation validation
                    // 1 - synchronize products
                    startSynchronizeProducts(env)
                    // 2 - synchronize offers
                    synchronizeOffers(env)
                }
            }
            // 2 - synchronize status
            toSynchronize.each { sync ->
                synchronizeStatus(sync)
            }
        }
    }

    private def void startSynchronizeProducts(MiraklEnv env, Catalog xcatalog = null) {
        // 1 - fetch mirakl imported products files
        File[] files = fetchMiraklImportedProductsFiles(env)
        RiverConfig riverConfig = new RiverConfig(
                debug: true,
                clientConfig: new ClientConfig(
                        merchant_url: env.url,
                        debug: true,
                        credentials: new Credentials(
                                apiKey: env.apiKey, // required for specific api calls
                                frontKey: env.frontKey
                        )
                )
        )
        final xcompany = env.company
        // 2 - load operator attributes
        List<Attribute> attributes = files.size() > 0 ? listAttributes(riverConfig)?.attributes : []
        // 3 - handle uploaded files
        files.each { file ->
            handleMiraklImportedProductsFile(env, file, riverConfig, xcompany, attributes, xcatalog)
        }
    }

    private def void synchronizeOffers(MiraklEnv env, Catalog xcatalog = null) {
        RiverConfig riverConfig = new RiverConfig(
                debug: true,
                clientConfig: new ClientConfig(
                        merchant_url: env.url,
                        debug: true,
                        credentials: new Credentials(
                                apiKey: env.apiKey, // required for specific api calls
                                frontKey: env.frontKey
                        )
                )
        )
        def offers = exportOffers(riverConfig, env.offersLastRequestDate)?.offers
        def xcompany = env.company
        offers?.findAll {
            it.shopId.toString() in env.shopIds?.split(",")
        }?.each { offer ->
            def shopId = offer.shopId.toString()
            xcatalog = xcatalog ?: Catalog.findByCompanyAndExternalCodeLikeAndReadOnlyAndDeleted(xcompany, "%mirakl::$shopId%", true, false)
            if(xcatalog){
                def code = offer.productSku
                TicketType xsku = TicketType.findAllByExternalCodeLikeOrUuid("%mirakl::$code%", code).find {
                    it.product.category.catalog == xcatalog
                }
                if(xsku){
                    // update price
                    if(offer.originPrice){
                        xsku.price = (offer.originPrice * 100)
                    }
                    // update start date
                    if(offer.availableStartDate){
                        Calendar startDate = Calendar.instance
                        startDate.setTime(offer.availableStartDate)
                        xsku.startDate = startDate
                    }
                    // update end date
                    if(offer.availableEndDate){
                        Calendar stopDate = Calendar.instance
                        stopDate.setTime(offer.availableEndDate)
                        xsku.stopDate = stopDate
                    }
                    // update stock
                    if(offer.quantity){ // quantity available
                        def stock = xsku.stock ?: new Stock()
                        stock.stockUnlimited = offer.quantity < 0
                        stock.stock = offer.quantity
                        stock.validate()
                        if(!stock.hasErrors()){
                            stock.save(flush: true)
                            xsku.stock = stock
                            xsku.nbSales = 0
                        }
                    }
                    if(offer.deleted || !offer.active){
                        xsku.available = false
                    }
                    if(offer.originPrice && (offer.discountPrice || offer.discountRanges)){
                        final externalCode = "mirakl::${code}"
                        def coupons = Coupon.executeQuery(
                                'select coupon FROM Coupon coupon join fetch coupon.rules left join coupon.ticketTypes as ticketType where (ticketType.id=:skuId and (ticketType.stopDate is null or ticketType.stopDate >= :today) and coupon.active=true and coupon.anonymous=true and coupon.externalCode like :externalCode)',
                                [skuId:xsku.id, externalCode:"%$externalCode%", today: Calendar.instance])
                        def coupon = coupons?.size() > 0 ? coupons.first() :
                                new Coupon(
                                        name: "Discount offer",
                                        code: code,
                                        externalCode: externalCode,
                                        anonymous: true,
                                        active: true,
                                        catalogWise: false,
                                        forSale: false,
                                        consumed: 0L,
                                        company: xcompany)
                        if(offer.discountStartDate){
                            Calendar startDate = Calendar.instance
                            startDate.setTime(offer.discountStartDate)
                            coupon.startDate = startDate
                        }
                        if(offer.discountEndDate){
                            Calendar endDate = Calendar.instance
                            endDate.setTime(offer.discountEndDate)
                            coupon.endDate = endDate
                        }
                        coupon.addToTicketTypes(xsku)
                        coupon.validate()
                        if(!coupon.hasErrors()){
                            coupon.save(flush:true)
                        def oldRules = coupon.rules
                        oldRules?.each {
                            coupon.removeFromRules(it)
                                it.delete(flush: true)
                        }
                            if(offer.discountPrice){
                                long discount = (offer.originPrice - offer.discountPrice) * 100
                                def rule = new ReductionRule(
                                        xtype: ReductionRuleType.DISCOUNT,
                                        discount: "-$discount"
                                )
                                rule.validate()
                                if(!rule.hasErrors()){
                                    rule.save(flush: true)
                                    coupon.addToRules(rule)
                                }
                            }
                            if(offer.discountRanges){
                                Map<Long, Double> discounts = [:]
                                offer.discountRanges.split(",").each {
                                    final tokens = it.split("\\|")
                                    if(tokens.length >= 2){
                                        def quantityMin = Long.parseLong(tokens.first().toString())
                                        if(quantityMin > 1 || !offer.discountPrice){
                                            long discount = (offer.originPrice - Double.parseDouble(tokens.drop(1).first().toString())) * 100
                                        def rule = new ReductionRule(
                                                quantityMin: quantityMin,
                                                xtype: ReductionRuleType.DISCOUNT,
                                                discount: "-$discount"
                                        )
                                        rule.validate()
                                        if(!rule.hasErrors()){
                                            rule.save(flush: true)
                                            coupon.addToRules(rule)
                                        }
                                    }
                                }
                            }
                        }
                            coupon.validate()
                            if(!coupon.hasErrors()){
                                coupon.save(flush: true)
                            }
                        }
                        else{
                            coupon.errors.allErrors.each {log.error(it.toString())}
                        }
                    }
                    xsku.validate()
                    if(!xsku.hasErrors()){
                        xsku.save(flush: true)
                    }
                }
            }
        }
        env.offersLastRequestDate = new Date()
        env.validate()
        if(!env.hasErrors()){
            env.save(flush: true)
        }
    }

    private def void synchronizeStatus(MiraklSync sync) {
        def company = sync.company
        def env = sync.miraklEnv ?: MiraklEnv.findAllByCompany(company).first()
        RiverConfig riverConfig = new RiverConfig(
                debug: true,
                clientConfig: new ClientConfig(
                        store: company.code,
                        merchant_id: env.shopId,
                        merchant_url: env.url,
                        debug: true,
                        credentials: new Credentials(
                                frontKey: env.frontKey as String,
                                apiKey: env.apiKey
                        )
                )
        )
        def trackingId = sync.trackingId as Long
        SynchronizationStatus synchronizationStatus = null
        String errorReport = null
        Long linesRead = 0L
        Long linesInError = 0L
        long linesInSuccess = 0L
        final waitingStatus = [SynchronizationStatus.QUEUED, SynchronizationStatus.WAITING, SynchronizationStatus.RUNNING, SynchronizationStatus.TRANSFORMATION_WAITING, SynchronizationStatus.TRANSFORMATION_RUNNING, SynchronizationStatus.WAITING_SYNCHRONIZATION_PRODUCT]
        int nbAttempts = 0
        final int maxAttempts = 5
        long millis = 100
        try {
            switch (sync.type) {
                case MiraklSyncType.CATEGORIES:
                    def synchronizationStatusResponse = refreshCategoriesSynchronizationStatus(riverConfig, trackingId)
                    while (nbAttempts < maxAttempts && synchronizationStatusResponse.status in waitingStatus) {
                        Thread.currentThread().sleep(millis)
                        synchronizationStatusResponse = refreshCategoriesSynchronizationStatus(riverConfig, trackingId)
                        nbAttempts++
                    }
                    synchronizationStatus = synchronizationStatusResponse.status
                    linesRead = synchronizationStatusResponse.linesRead
                    linesInError = synchronizationStatusResponse.linesInError
                    linesInSuccess = synchronizationStatusResponse.linesInSuccess
                    if (synchronizationStatusResponse.hasErrorReport) {
                        errorReport = loadCategoriesSynchronizationErrorReport(riverConfig, trackingId)
                        synchronizationStatus = SynchronizationStatus.FAILED
                    }
                    Category.findAllByMiraklTrackingId(trackingId.toString()).each { cat ->
                        cat.miraklStatus = MiraklSyncStatus.valueOf(synchronizationStatus?.toString() ?: sync.status.key)
                        cat.save(flush: true)
                    }
                    break
                case MiraklSyncType.HIERARCHIES:
                    def trackingImportStatus = trackHierarchiesImportStatusResponse(riverConfig, trackingId)
                    while (nbAttempts < maxAttempts && trackingImportStatus.importStatus in waitingStatus) {
                        Thread.currentThread().sleep(millis)
                        trackingImportStatus = trackHierarchiesImportStatusResponse(riverConfig, trackingId)
                        nbAttempts++
                    }
                    synchronizationStatus = trackingImportStatus.importStatus
                    if (trackingImportStatus.hasErrorReport) {
                        errorReport = loadHierarchiesSynchronizationErrorReport(riverConfig, trackingId)
                        synchronizationStatus = SynchronizationStatus.FAILED
                    }
                    break
                case MiraklSyncType.VALUES:
                    def trackingImportStatus = trackValuesImportStatusResponse(riverConfig, trackingId)
                    while (nbAttempts < maxAttempts && trackingImportStatus.importStatus in waitingStatus) {
                        Thread.currentThread().sleep(millis)
                        trackingImportStatus = trackValuesImportStatusResponse(riverConfig, trackingId)
                        nbAttempts++
                    }
                    synchronizationStatus = trackingImportStatus.importStatus
                    if (trackingImportStatus.hasErrorReport) {
                        errorReport = loadValuesSynchronizationErrorReport(riverConfig, trackingId)
                        synchronizationStatus = SynchronizationStatus.FAILED
                    }
                    break
                case MiraklSyncType.ATTRIBUTES:
                    def trackingImportStatus = trackAttributesImportStatusResponse(riverConfig, trackingId)
                    while (nbAttempts < maxAttempts && trackingImportStatus.importStatus in waitingStatus) {
                        Thread.currentThread().sleep(millis)
                        trackingImportStatus = trackAttributesImportStatusResponse(riverConfig, trackingId)
                        nbAttempts++
                    }
                    synchronizationStatus = trackingImportStatus.importStatus
                    if (trackingImportStatus.hasErrorReport) {
                        errorReport = loadAttributesSynchronizationErrorReport(riverConfig, trackingId)
                        synchronizationStatus = SynchronizationStatus.FAILED
                    }
                    break
                case MiraklSyncType.PRODUCTS:
                    def synchronizationStatusResponse = trackProductsImportStatusResponse(riverConfig, trackingId)
                    while (nbAttempts < maxAttempts && synchronizationStatusResponse.importStatus in waitingStatus) {
                        Thread.currentThread().sleep(millis)
                        synchronizationStatusResponse = trackProductsImportStatusResponse(riverConfig, trackingId)
                        nbAttempts++
                    }
                    synchronizationStatus = synchronizationStatusResponse.importStatus
                    linesRead = synchronizationStatusResponse.transformLinesRead
                    linesInError = synchronizationStatusResponse.transformLinesInError
                    linesInSuccess = synchronizationStatusResponse.transformLinesInSucces
                    if (synchronizationStatusResponse.hasErrorReport) {
                        errorReport = loadProductsImportSynchronizationErrorReport(riverConfig, trackingId)
                        synchronizationStatus = SynchronizationStatus.FAILED
                    } else if (synchronizationStatusResponse.hasTransformationErrorReport) {
                        errorReport = loadProductsImportTransformationErrorReport(riverConfig, trackingId)
                        synchronizationStatus = SynchronizationStatus.TRANSFORMATION_FAILED
                    }
                    break
                case MiraklSyncType.PRODUCTS_SYNCHRO:
                    def synchronizationStatusResponse = refreshProductsSynchronizationStatus(riverConfig, trackingId)
                    while (nbAttempts < maxAttempts && synchronizationStatusResponse.status in waitingStatus) {
                        Thread.currentThread().sleep(millis)
                        synchronizationStatusResponse = refreshProductsSynchronizationStatus(riverConfig, trackingId)
                        nbAttempts++
                    }
                    synchronizationStatus = synchronizationStatusResponse.status
                    linesRead = synchronizationStatusResponse.linesRead
                    linesInError = synchronizationStatusResponse.linesInError
                    linesInSuccess = synchronizationStatusResponse.linesInSuccess
                    if (synchronizationStatusResponse.hasErrorReport) {
                        errorReport = loadProductsSynchronizationErrorReport(riverConfig, trackingId)
                        synchronizationStatus = SynchronizationStatus.FAILED
                    }
                    TicketType.findAllByMiraklProductTrackingId(trackingId.toString()).each { sku ->
                        sku.miraklProductStatus = MiraklSyncStatus.valueOf(synchronizationStatus?.toString() ?: sync.status.key)
                        if (synchronizationStatus == SynchronizationStatus.COMPLETE) {
                            Map<String, String> externalCodes = extractExternalCodes(sku.externalCode)
                            if (!externalCodes.containsKey("mirakl")) {
                                externalCodes.put("mirakl", sku.uuid)
                                sku.externalCode = externalCodes.collect { "${it.key}::${it.value}" }.join(",")
                            }
                        }
                        sku.save(flush: true)
                    }
                    break
                case MiraklSyncType.OFFERS:
                    def trackingImportStatus = trackOffersImportStatusResponse(riverConfig, trackingId)
                    while (nbAttempts < maxAttempts && trackingImportStatus.status in waitingStatus) {
                        Thread.currentThread().sleep(millis)
                        trackingImportStatus = trackOffersImportStatusResponse(riverConfig, trackingId)
                        nbAttempts++
                    }
                    synchronizationStatus = trackingImportStatus.status
                    linesRead = trackingImportStatus.linesRead
                    linesInError = trackingImportStatus.linesInError
                    linesInSuccess = trackingImportStatus.linesInSuccess
                    if (trackingImportStatus.hasErrorReport) {
                        errorReport = loadOffersSynchronizationErrorReport(riverConfig, trackingId)
                        synchronizationStatus = SynchronizationStatus.FAILED
                    }
                    TicketType.findAllByMiraklTrackingId(trackingId.toString()).each { sku ->
                        sku.miraklStatus = MiraklSyncStatus.valueOf(synchronizationStatus?.toString() ?: sync.status.key)
                        if (synchronizationStatus == SynchronizationStatus.COMPLETE) {
                            Map<String, String> externalCodes = extractExternalCodes(sku.externalCode)
                            if (!externalCodes.containsKey("mirakl")) {
                                externalCodes.put("mirakl", sku.uuid)
                                sku.externalCode = externalCodes.collect { "${it.key}::${it.value}" }.join(",")
                            }
                        }
                        sku.save(flush: true)
                    }
                    break
                default:
                    break
            }
        }
        catch (Exception e) {
            RiverTools.log.error(e.message)
        }
        sync.status = MiraklSyncStatus.valueOf(synchronizationStatus?.toString() ?: sync.status.key)
        sync.errorReport = errorReport
        sync.linesRead = linesRead
        sync.linesInError = linesInError
        sync.linesInSuccess = linesInSuccess
        if (sync.validate()) {
            sync.save(flush: true)
        }
    }

    private def void handleMiraklImportedProductsFile(MiraklEnv env, File file, RiverConfig riverConfig, Company xcompany, List<Attribute> attributes, Catalog xcatalog = null) {
        final matcher = IMPORT_PRODUCTS.matcher(file.name)
        matcher.find()
        final shopId = matcher.group(1)
        RiverTools.log.info("START HANDLING PRODUCT FILE ${file.path} FOR MIRAKL SHOP $shopId")
        // 1 - create mirakl catalog for external publication
        MiraklEnv xenv = xcatalog?.miraklEnv ?: MiraklEnv.findByShopIdAndCompany(shopId, xcompany)
        if (!xenv) {
            xenv = env
            xcatalog = Catalog.findByCompanyAndExternalCodeLikeAndReadOnlyAndDeleted(xcompany, "%mirakl::$shopId%", true, false) ?: createMiraklCatalog(shopId, riverConfig, xcompany, xenv)
        }
        // 2 - load imported products
        Observable<CsvLine> lines = Reader.parseCsvFile(file)
        def results = lines.toBlocking()
        // 3 - handle each imported product
        List<MiraklReportItem> reportItems = []
        List<MiraklProduct> products = []
        results.forEach(new Action1<CsvLine>() {
            @Override
            void call(CsvLine csvLine) {
                MiraklImportedProductResult result = handleMiraklImportedProduct(csvLine, xcatalog, shopId, xcompany, attributes, xenv.shopIds.split(",").toList())
                if (result.product) {
                    products << result.product
                }
                reportItems << result.reportItem
            }
        })
        // 4 - send integration reports - P43
        sendProductIntegrationReports(riverConfig, file.name, SynchronizationStatus.COMPLETE, reportItems)
        // 5 - synchronize products - P21
        if (products.size() > 0) {
            def synchronizationResponse = synchronizeProducts(riverConfig, products)
            final productSynchroId = synchronizationResponse?.synchroId
            if (productSynchroId) {
                def sync = new MiraklSync()
                sync.miraklEnv = xenv
                sync.company = xcompany
                sync.catalog = xcatalog
                sync.type = MiraklSyncType.PRODUCTS_SYNCHRO
                sync.status = MiraklSyncStatus.QUEUED
                sync.timestamp = new Date()
                sync.trackingId = productSynchroId.toString()
                sync.validate()
                if (!sync.hasErrors()) {
                    sync.save(flush: true)
                    synchronizationResponse?.ids?.each { id ->
                        def sku = TicketType.findAllByExternalCodeLikeOrUuid("%mirakl::$id%", id).find {it.product.category.catalog == xcatalog}
                        if (sku) {
                            sku.miraklProductStatus = sync.status
                            sku.miraklProductTrackingId = sync.trackingId
                            sku.validate()
                            if (!sku.hasErrors()) {
                                sku.save(flush: true)
                            }
                        }
                    }
                }
            }
        }
        file.renameTo(new File("${file.absolutePath}.done"))
        RiverTools.log.info("END HANDLING PRODUCT FILE ${file.path} FOR MIRAKL SHOP $shopId")
    }

    private def MiraklImportedProductResult handleMiraklImportedProduct(CsvLine csvLine, Catalog xcatalog, String shopId, Company xcompany, List<Attribute> attributes, List<String> shopIds) {
        MiraklImportedProductResult result = new MiraklImportedProductResult()
        List<MiraklAttributeValue> vattributes = []
        Map<String, String> fields = csvLine.fields
        def errorMessage = none
        // check all required attributes by mirakl operator
        def error = attributes.any { attribute ->
            def ret = false
            final def code = attribute.code
            if (attribute.required && (!fields.containsKey(code) || fields.get(code).trim().isEmpty())) {
                errorMessage = toScalaOption("${code} is required")
                ret = true
            } else {
                vattributes << new MiraklAttributeValue(code, toScalaOption(fields.get(code)))
            }
            ret
        }
        // check product definition for mogobiz and import products and skus for external publications
        if (!error) {
            final categoryCode = fields.get(MiraklApi.category())
            final code = fields.get(MiraklApi.identifier())
            final label = fields.get(MiraklApi.title())
            final description = fields.get(MiraklApi.description())
            final variantGroupCode = fields.get(MiraklApi.variantIdentifier())
            final brandName = fields.get(MiraklApi.brand())
            final media = fields.get(MiraklApi.media())
            List<ProductReference> productReferences = []
            fields.get(MiraklApi.productReferences())?.each { k, v ->
                productReferences << new ProductReference(referenceType: k, reference: v)
            }

            // find sku and product
            TicketType xsku = TicketType.findAllByExternalCodeLike("%mirakl::$code%").find {
                def ret = xcatalog && it.product.category.catalog == xcatalog
                if (!xcatalog) {
                    ret = !it.product.category.catalog.deleted && it.miraklTrackingId
                    def e = ret ? MiraklSync.findByTrackingId(it.miraklTrackingId)?.miraklEnv : null
                    ret = ret && e?.shopId == shopId && e?.operator
                }
                ret
            }
            Product xproduct = xsku?.product
            Category xcategory = xproduct?.category ?: Category.findAllByExternalCodeLike("%mirakl::$categoryCode%").find {
                it.company == xcompany && (!xcatalog || it.catalog == xcatalog)
            }
            if (xcategory) {
                xcatalog = xcatalog ?: xcategory?.catalog
                if (!xsku) {
                    xproduct = Product.findAllByExternalCodeLike("%mirakl::$variantGroupCode%").find {
                        it.category == xcategory
                    }
                    if (!xproduct) {
                        xproduct = new Product(
                                uuid: variantGroupCode,
                                externalCode: "mirakl::$variantGroupCode",
                                code: variantGroupCode,
                                name: label,
                                xtype: ProductType.PRODUCT, //TODO add product type mapping
                                state: ProductState.ACTIVE,
                                description: description,
                                calendarType: ProductCalendar.NO_DATE, //TODO add calendar type mapping
                                sanitizedName: sanitizeUrlService.sanitizeWithDashes(label),
                                publishable: false,
                                category: xcategory,
                                company: xcompany,
                                brand: Brand.findByCompanyAndNameLike(xcompany, "%$brandName%")
                        )
                        xproduct.validate()
                        if (!xproduct.hasErrors()) {
                            xproduct.save(flush: true)
                        } else {
                            def message = xproduct.errors.allErrors.collect {
                                it.toString()
                            }.join(",")
                            errorMessage = toScalaOption(message)
                            xproduct = null
                        }
                    }
                    if (xproduct) {
                        xsku = createMiraklSku(media, xcompany, xcategory, fields, code, label, description, xproduct)
                        xsku.validate()
                        if (!xsku.hasErrors()) {
                            xsku.save(flush: true)
                        } else {
                            def message = xsku.errors.allErrors.collect { it.toString() }.join(",")
                            errorMessage = toScalaOption(message)
                        }
                    }
                }
                result.product = new MiraklProduct(
                        code,
                        label,
                        toScalaOption(description),
                        categoryCode,
                        toScalaOption(true),
                        toScalaList(productReferences),
                        toScalaList(([] << "$shopId|$code") as List<String>),
                        toScalaOption(brandName),
                        none,
                        toScalaOption(media),
                        toScalaList(shopIds), //([] << "$shopId") as List<String>
                        toScalaOption(variantGroupCode),
                        none, //TODO logistic-class
                        BulkAction.UPDATE,
                        toScalaList(vattributes)
                )
            } else {
                log.warn("category not found $categoryCode")
                errorMessage = toScalaOption("unknow category $categoryCode")
            }
        }
        result.reportItem = new MiraklReportItem(csvLine, errorMessage)
        result
    }

    private def TicketType createMiraklSku(String media, Company xcompany, Category xcategory, Map<String, String> fields, String code, String label, String description, Product xproduct) {
        TicketType xsku = null
        def xpicture = null
        if (media) {
            def name = new File(new URI(media).path).name
            def resource = new Resource(
                    name: name,
                    sanitizedName: sanitizeUrlService.sanitizeWithDashes(name),
                    xtype: ResourceType.PICTURE,
                    url: media,
                    company: xcompany
            )
            resource.validate()
            if (!resource.hasErrors()) {
                resource.save(flush: true)
                xpicture = resource
            }
        }
        // lookup sku variations
        Map<String, VariationValue> variationValues = [:]
        def xvariations = Variation.findAllByCategory(xcategory)
        xvariations?.eachWithIndex { Variation entry, int i ->
            def vcode = extractMiraklExternalCode(entry.externalCode)
            if (vcode) {
                String vvalue = fields.get(vcode)?.trim()
                if (vvalue?.length() > 0) {
                    def variationValue = VariationValue.findByVariationAndExternalCode(entry, "mirakl::$vvalue")
                    if (variationValue) {
                        variationValues.put("variation${i + 1}".toString(), variationValue)
                    }
                }
            }
        }
        def variation1 = variationValues.get("variation1")
        def variation2 = variationValues.get("variation2")
        def variation3 = variationValues.get("variation3")
        xsku = new TicketType(
                uuid: code,
                sku: code,
                externalCode: "mirakl::$code",
                name: label,
                description: description,
                publishable: false,
                product: xproduct,
                price: 0L, //TODO update during offers import
                picture: xpicture,
                variation1: variation1,
                variation2: variation2,
                variation3: variation3
        )
        xsku
    }

    private def Catalog createMiraklCatalog(String shopId, RiverConfig riverConfig, Company xcompany, MiraklEnv xenv) {
        def name = shopId
        def shops = MiraklClient.searchShops(riverConfig, new SearchShopsRequest(shopIds: ([] << shopId) as List<String>))
        if (shops?.shops?.size() > 0) {
            name = shops?.shops?.first()?.shopName
        }
        def xcatalog = new Catalog(
                name: name,
                externalCode: "mirakl::$shopId",
                company: xcompany,
                miraklEnv: xenv,
                readOnly: true,
                activationDate: new Date()
        )
        xcatalog.validate()
        if (!xcatalog.hasErrors()) {
            xcatalog.save(flush: true)
            def seller = Seller.findByCompanyAndActive(xcompany, true) //TODO create mirakl seller ?
            profileService.saveUserPermission(
                    seller,
                    true,
                    PermissionType.UPDATE_STORE_CATALOG,
                    xcompany.id as String,
                    xcatalog.id as String
            )
            catalogService.handleMiraklCategoriesByHierarchyAndLevel(xcatalog, riverConfig, seller)
        }
        xcatalog
    }

    private static def File[] fetchMiraklImportedProductsFiles(MiraklEnv env) {
        final String localPath = env.localPath
        def miraklSftpConfig = new MiraklSftpConfig(
                remoteHost: env.remoteHost,
                remotePath: env.remotePath,
                username: env.username,
                password: env.password,
                keyPath: env.keyPath,
                passphrase: env.passPhrase,
                localPath: localPath
        )
        synchronizeImports(miraklSftpConfig)
        // filter files uploaded
        def files = new File(localPath).listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                final matcher = IMPORT_PRODUCTS.matcher(name)
                return name.endsWith(".csv") && matcher.find() && matcher.groupCount() == 4 && matcher.group(1) in env.shopIds.split(",")
            }
        })
        files
    }

    def PagedList<MiraklSync> refreshSynchronization(Catalog catalog, PagedListCommand cmd, List<MiraklSyncType> includedTypes = [
            MiraklSyncType.PRODUCTS,
            MiraklSyncType.OFFERS,
            MiraklSyncType.PRODUCTS_SYNCHRO
    ]){
        int totalCount = MiraklSync.countByTypeInListAndCatalog(includedTypes, catalog)
        def list = MiraklSync.findAllByTypeInListAndCatalog(includedTypes, catalog, (cmd?.pagination ?: [:]) + [sort: "timestamp", order: "desc"])
        new PagedList<MiraklSync>(list: list, totalCount: totalCount)
    }

    def PagedList<OutputShop> searchShops(String url, String frontKey, SearchShopsRequest request = new SearchShopsRequest()){
        def clientConfig = new ClientConfig(merchant_url: url, credentials: new Credentials(frontKey: frontKey))
        def riverConfig = new RiverConfig(debug: true, clientConfig: clientConfig)
        def response = searchShops(riverConfig, request)
        new PagedList<OutputShop>(list: response.shops, totalCount: response.totalCount)
    }
}

class MiraklImportedProductResult {
    MiraklProduct product
    MiraklReportItem reportItem
}
