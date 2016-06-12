package com.mogobiz.service

import com.mogobiz.common.client.ClientConfig
import com.mogobiz.common.client.Credentials
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.mirakl.client.domain.Attribute
import com.mogobiz.mirakl.client.domain.AttributeType
import com.mogobiz.mirakl.client.domain.MiraklAttribute

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

class MiraklService {

    static transactional = false

    def publish(Company company, MiraklEnv env, Catalog catalog, boolean manual = false) {
        if (catalog?.name?.trim()?.toLowerCase() == "impex") {
            return
        }
        boolean running = MiraklEnv.withTransaction {
            MiraklEnv.findByRunning(true) != null
        }
        if (!running && company && env && env.company == company && catalog && catalog.company == company && (manual || catalog.activationDate < new Date())) {
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
            def url = env.url
            def store = company.code
            def debug = true

            RiverConfig config = new RiverConfig(
                    clientConfig: new ClientConfig(
                            store: store,
                            url: url,
                            debug: debug,
                            credentials: new Credentials(apiKey: env.apiKey)
                    ),
                    idCatalog: catalog.id,
                    languages: languages,
                    defaultLang: company.defaultLanguage
            )

            final publishable = false // TODO publishable = true

            // 0. Load catalog categories
            def categories = Category.executeQuery(
                    'select cat FROM Category cat left join fetch cat.features as feature left join fetch feature.values left join fetch cat.variations as variation left join fetch variation.variationValues where cat.catalog.id=:idCatalog and cat.publishable=:publishable and cat.deleted=false',
                    [idCatalog:config.idCatalog, publishable: publishable],
                    [readOnly: true, flushMode: FlushMode.MANUAL]
            )

            def hierarchies = []
            def values = []
            def attributes = []

            categories.collect {category ->
                final parent = category.parent
                final hierarchyCode = "$store.${category.sanitizedName}"
                hierarchies << new MiraklHierarchy(
                        hierarchyCode,
                        category.name,
                        toScalaOption(
                                parent ? new MiraklCategory("${store}_${parent.sanitizedName}", "") : null
                        )
                )
                category.features?.each {feature ->
                    final featureCode = "${hierarchyCode}_${feature.name}"
                    final featureLabel = "${feature.name}"
                    final featureParent = new MiraklValue(
                            "$featureCode-list",
                            featureLabel
                    )
                    attributes << new MiraklAttribute(new Attribute(
                            code: featureCode,
                            label: featureLabel,
                            hierarchyCode: hierarchyCode,
                            type: AttributeType.TEXT,
                            valuesList: featureParent.code,
                            variant: false
                    ))
                    feature.values.collect {value ->
                        final val = "${value.value}"
                        values << new MiraklValue(val, val, toScalaOption(featureParent))
                    }
                }
                category.variations?.each { variation ->
                    final variationCode = "${hierarchyCode}_${variation.name}"
                    final variationlabel = "${variation.name}"
                    final variationParent = new MiraklValue(
                            "$variationCode-list",
                            variationlabel
                    )
                    attributes << new MiraklAttribute(new Attribute(
                            code: variationCode,
                            label: variationlabel,
                            hierarchyCode: hierarchyCode,
                            type: AttributeType.TEXT,
                            valuesList: variationParent.code,
                            variant: true
                    ))
                    variation.variationValues.each {value ->
                        final val = "${value.value}"
                        values << new MiraklValue(val, val, toScalaOption(variationParent))
                    }
                }
            }

            // 1. Import product Hierarchy
            MiraklSync.withTransaction {
                def sync = new MiraklSync()
                sync.company = company
                sync.catalog = catalog
                sync.type = MiraklSyncType.HIERARCHIES
                sync.status = MiraklSyncStatus.QUEUED
                sync.timestamp = new Date()
                sync.trackingId = "${importHierarchies(config, hierarchies).importId}"
                sync.validate()
                if(!sync.hasErrors()){
                    sync.save(flush: true)
                }
            }

            // 2. Import List of Values
            MiraklSync.withTransaction {
                def sync = new MiraklSync()
                sync.company = company
                sync.catalog = catalog
                sync.type = MiraklSyncType.HIERARCHIES
                sync.status = MiraklSyncStatus.QUEUED
                sync.timestamp = new Date()
                sync.trackingId = "${importValues(config, values).importId}"
                sync.validate()
                if(!sync.hasErrors()){
                    sync.save(flush: true)
                }
            }

            // 3. Import Attributes
            MiraklSync.withTransaction {
                def sync = new MiraklSync()
                sync.company = company
                sync.catalog = catalog
                sync.type = MiraklSyncType.HIERARCHIES
                sync.status = MiraklSyncStatus.QUEUED
                sync.timestamp = new Date()
                sync.trackingId = "${importAttributes(config, attributes).importId}"
                sync.validate()
                if(!sync.hasErrors()){
                    sync.save(flush: true)
                }
            }

            // TODO import offers
        }
    }
}
