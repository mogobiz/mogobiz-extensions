/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.service

import com.mogobiz.common.client.BulkAction
import com.mogobiz.common.client.ClientConfig
import com.mogobiz.common.client.Credentials
import com.mogobiz.common.rivers.AbstractRiverCache
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.mirakl.client.domain.Attribute
import com.mogobiz.mirakl.client.domain.AttributeType
import com.mogobiz.mirakl.client.domain.MiraklApi
import com.mogobiz.mirakl.client.domain.MiraklAttribute
import com.mogobiz.mirakl.client.domain.SynchronizationStatus
import com.mogobiz.mirakl.client.io.ImportOffersResponse
import com.mogobiz.mirakl.rivers.MiraklRiverFlow
import com.mogobiz.mirakl.rivers.OfferRiver
import rx.Subscriber
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

    def publish(Company company, MiraklEnv env, Catalog catalog, boolean manual = false) {
        if (catalog?.name?.trim()?.toLowerCase() == "impex") {
            return
        }
        if (company && env && env.company == company && !env.running && catalog && catalog.company == company && (manual || catalog.activationDate < new Date())) {
            log.info("${manual ? "Manual " : ""}Export to Mirakl has started ...")
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
                    idCatalog: catalog.id,
                    languages: languages,
                    defaultLang: company.defaultLanguage
            )

            // 0. Load catalog categories
            Set<Category> categories = Category.executeQuery(
                    'select cat FROM Category cat left join fetch cat.parent left join fetch cat.features as feature left join fetch feature.values left join fetch cat.variations as variation left join fetch variation.variationValues where cat.catalog.id=:idCatalog and cat.publishable=true and cat.deleted=false',
                    [idCatalog:config.idCatalog],
                    [readOnly: true, flushMode: FlushMode.MANUAL]
            ).toSet()

            List<MiraklCategory> hierarchies = []
            def values = []
            List<MiraklAttribute> attributes = []

            categories.each {category ->
                def parent = category.parent
                def hierarchyCode = miraklCategoryCode(category)
                hierarchies << new MiraklCategory(
                        hierarchyCode,
                        category.name,
                        BulkAction.UPDATE,
                        parent ? Some.apply(new MiraklCategory(miraklCategoryCode(parent), "")) : toScalaOption(null),
                        category.logisticClass
                )
                category.features?.each {feature ->
                    def featureCode = "${hierarchyCode}_${sanitizeUrlService.sanitizeWithDashes(feature.name)}"
                    def featureLabel = "${feature.name}"
                    def featureListValues = new MiraklValue(
                            "$featureCode-list",
                            featureLabel
                    )
                    attributes << new MiraklAttribute(new Attribute(
                            code: featureCode,
                            label: featureLabel,
                            hierarchyCode: hierarchyCode,
                            type: AttributeType.LIST,
                            typeParameter: featureListValues.code,
                            variant: false,
                            required: false
                    ))
                    feature.values.collect { featureValue ->
                        def val = "${featureValue.value}"
                        values << new MiraklValue(sanitizeUrlService.sanitizeWithDashes(val), val, Some.apply(featureListValues))
                    }
                }
                category.variations?.each { variation ->
                    def variationCode = "${hierarchyCode}_${sanitizeUrlService.sanitizeWithDashes(variation.name)}"
                    def variationlabel = "${variation.name}"
                    def variationListValues = new MiraklValue(
                            "$variationCode-list",
                            variationlabel
                    )
                    attributes << new MiraklAttribute(new Attribute(
                            code: variationCode,
                            label: variationlabel,
                            hierarchyCode: hierarchyCode,
                            type: AttributeType.LIST,
                            typeParameter: variationListValues.code,
                            variant: true,
                            required: false
                    ))
                    variation.variationValues.each { variationValue ->
                        def val = "${variationValue.value}"
                        values << new MiraklValue(sanitizeUrlService.sanitizeWithDashes(val), val, Some.apply(variationListValues))
                    }
                }
            }

            // 1. synchronize categories
            final synchronizeCategoriesId = synchronizeCategories(config, hierarchies)?.synchroId
            if(synchronizeCategoriesId){
                MiraklSync.withTransaction {
                    def sync = new MiraklSync()
                    sync.company = company
                    sync.catalog = catalog
                    sync.type = MiraklSyncType.CATEGORIES
                    sync.status = MiraklSyncStatus.QUEUED
                    sync.timestamp = new Date()
                    sync.trackingId = synchronizeCategoriesId.toString()
                    sync.validate()
                    if(!sync.hasErrors()){
                        sync.save(flush: true)
                    }
                }
            }

            // 2. Import product Hierarchy
            final importHierarchiesId = synchronizeCategoriesId ? importHierarchies(config, hierarchies.collect {new MiraklHierarchy(it)})?.importId : null
            if(importHierarchiesId){
                MiraklSync.withTransaction {
                    def sync = new MiraklSync()
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

            // 5. Import Offers TODO + Products
            final List<String> offersHeader = []
            offersHeader .addAll(MiraklApi.offersHeader().split(";")) // offer headers
            offersHeader.addAll(attributes.collect {it.code}) // features + variations attributes
//            offersHeader.addAll(["category", "identifier", "title"]) // required product attributes FIXME handle attributes mapping
//            offersHeader.addAll(MiraklApi.productsHeader().split(";")) // product headers
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
                            sync.company = company
                            sync.catalog = catalog
                            sync.type = MiraklSyncType.OFFERS
                            sync.status = MiraklSyncStatus.QUEUED
                            sync.timestamp = new Date()
                            sync.trackingId = importId.toString()
                            sync.validate()
                            if(!sync.hasErrors()){
                                sync.save(flush: true)
                            }
                        }
                    }
                    final productImportId = importOffersResponse?.productImportId
                    if(productImportId){
                        MiraklSync.withTransaction {
                            def sync = new MiraklSync()
                            sync.company = company
                            sync.catalog = catalog
                            sync.type = MiraklSyncType.PRODCUCTS
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
                            sync.company = company
                            sync.catalog = catalog
                            sync.type = MiraklSyncType.PRODCUCTS //TODO add PRODUCTS_SYNCHRO type
                            sync.status = MiraklSyncStatus.QUEUED
                            sync.timestamp = new Date()
                            sync.trackingId = productSynchroId.toString()
                            sync.validate()
                            if(!sync.hasErrors()){
                                sync.save(flush: true)
                            }
                        }
                    }
                }
            }
            MiraklRiverFlow.synchronize(
                    new OfferRiver(),
                    config,
                    Math.min(1, Runtime.getRuntime().availableProcessors()),
                    10,
                    new SubscriberAdapter(subscriber)
            )

        }
    }

    def synchronize(Catalog catalog){
        def excludedStatus = [MiraklSyncStatus.COMPLETE, MiraklSyncStatus.CANCELLED, MiraklSyncStatus.FAILED]
        def toSynchronize = catalog ?
                MiraklSync.findAllByStatusNotInListAndCatalog(excludedStatus, catalog) :
                MiraklSync.findAllByStatusNotInList(excludedStatus)
        toSynchronize.each {sync ->
            def company = sync.company
            def env = MiraklEnv.findAllByCompany(company).first() // TODO retrieve from sync.miraklEnv
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
            def waitingStatus = [SynchronizationStatus.QUEUED, SynchronizationStatus.WAITING, SynchronizationStatus.RUNNING]
            switch(sync.type){
                case MiraklSyncType.CATEGORIES:
                    def synchronizationStatusResponse = refreshCategoriesSynchronizationStatus(riverConfig, trackingId)
                    while(synchronizationStatusResponse.status in waitingStatus){
                        synchronizationStatusResponse = refreshCategoriesSynchronizationStatus(riverConfig, trackingId)
                    }
                    synchronizationStatus = synchronizationStatusResponse.status
                    if(synchronizationStatusResponse.hasErrorReport){
                        errorReport = loadCategoriesSynchronizationErrorReport(riverConfig, trackingId)
                    }
                    break
                case MiraklSyncType.HIERARCHIES:
                    def trackingImportStatus = trackHierarchiesImportStatusResponse(riverConfig, trackingId)
                    while(trackingImportStatus.importStatus in waitingStatus){
                        trackingImportStatus = trackHierarchiesImportStatusResponse(riverConfig, trackingId)
                    }
                    synchronizationStatus = trackingImportStatus.importStatus
                    if(trackingImportStatus.hasErrorReport){
                        errorReport = loadHierarchiesSynchronizationErrorReport(riverConfig, trackingId)
                    }
                    break
                case MiraklSyncType.VALUES:
                    def trackingImportStatus = trackValuesImportStatusResponse(riverConfig, trackingId)
                    while(trackingImportStatus.importStatus in waitingStatus){
                        trackingImportStatus = trackValuesImportStatusResponse(riverConfig, trackingId)
                    }
                    synchronizationStatus = trackingImportStatus.importStatus
                    if(trackingImportStatus.hasErrorReport){
                        errorReport = loadValuesSynchronizationErrorReport(riverConfig, trackingId)
                    }
                    break
                case MiraklSyncType.ATTRIBUTES:
                    def trackingImportStatus = trackAttributesImportStatusResponse(riverConfig, trackingId)
                    while(trackingImportStatus.importStatus in waitingStatus){
                        trackingImportStatus = trackAttributesImportStatusResponse(riverConfig, trackingId)
                    }
                    synchronizationStatus = trackingImportStatus.importStatus
                    if(trackingImportStatus.hasErrorReport){
                        errorReport = loadAttributesSynchronizationErrorReport(riverConfig, trackingId)
                    }
                    break
                case MiraklSyncType.PRODCUCTS:
                    def synchronizationStatusResponse = refreshProductsSynchronizationStatus(riverConfig, trackingId)
                    while(synchronizationStatusResponse.status in waitingStatus){
                        synchronizationStatusResponse = refreshProductsSynchronizationStatus(riverConfig, trackingId)
                    }
                    synchronizationStatus = synchronizationStatusResponse.status
                    if(synchronizationStatusResponse.hasErrorReport){
                        errorReport = loadProductsSynchronizationErrorReport(riverConfig, trackingId)
                    }
                    break
                case MiraklSyncType.OFFERS:
                    def trackingImportStatus = trackOffersImportStatusResponse(riverConfig, trackingId)
                    while(trackingImportStatus.importStatus in waitingStatus){
                        trackingImportStatus = trackOffersImportStatusResponse(riverConfig, trackingId)
                    }
                    synchronizationStatus = trackingImportStatus.importStatus
                    if(trackingImportStatus.hasErrorReport){
                        errorReport = loadOffersSynchronizationErrorReport(riverConfig, trackingId)
                    }
                    break
                default:
                    break
            }
            sync.status = MiraklSyncStatus.valueOf(synchronizationStatus?.toString() ?: sync.status.key)
            sync.errorReport = errorReport
            if(sync.validate()){
                sync.save(flush: true)
            }
        }
    }
}
