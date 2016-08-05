/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.service

import com.mogobiz.common.client.BulkResponse
import com.mogobiz.common.client.ClientConfig
import com.mogobiz.common.client.Credentials
import com.mogobiz.common.rivers.AbstractRiverCache
import com.mogobiz.common.rivers.GenericRiversFlow
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.constant.IperConstant
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESIndexResponse
import com.mogobiz.elasticsearch.client.ESIndexSettings
import com.mogobiz.elasticsearch.client.ESRequest
import com.mogobiz.elasticsearch.client.ESSearchResponse
import com.mogobiz.elasticsearch.rivers.ESBORivers
import com.mogobiz.elasticsearch.rivers.ESRivers
import com.mogobiz.http.client.HTTPClient
import com.mogobiz.json.RenderUtil
import com.mogobiz.store.ProductSearchCriteria
import com.mogobiz.store.cmd.PagedListCommand
import com.mogobiz.store.domain.Catalog
import com.mogobiz.store.domain.Category
import com.mogobiz.store.domain.Company
import com.mogobiz.store.domain.EsEnv
import com.mogobiz.store.domain.EsSync
import com.mogobiz.store.domain.Product
import com.mogobiz.store.domain.ProductCalendar
import com.mogobiz.store.domain.Translation
import com.mogobiz.utils.DateUtilitaire
import com.mogobiz.utils.IperUtil
import com.mogobiz.utils.Page
import grails.converters.JSON
import groovy.json.JsonBuilder
import groovy.transform.Synchronized
import org.apache.commons.lang.StringUtils
import org.codehaus.groovy.grails.web.json.JSONObject
import org.quartz.CronExpression

import java.text.NumberFormat
import java.text.SimpleDateFormat

import rx.Subscriber
import rx.internal.reactivestreams.SubscriberAdapter

/**
 *
 */
class ElasticsearchService {

    static transactional = false

    public static final int DEFAULT_MAX_ITEMS = Integer.MAX_VALUE / 2

    ESClient client = ESClient.getInstance()

    HTTPClient httpClient = HTTPClient.getInstance()

    def grailsApplication

    /**
     * This method lists brands for the specified store
     * @param store - the store on which the search will be performed
     * @param lang - the language to use for translations
     * @param hidden - whether to include or not hidden brands
     * @param categoryPath - search brands for products which are associated to this category path
     * @param maxItems - max number of items
     */
    def List<Map> brands(String store, String lang, boolean hidden, String categoryPath, int maxItems = 100) {
        def results
        def query = [:]
        query << [size: maxItems]
        if (categoryPath) {
            results = []
            def included = ['brand']
            if (lang) {
                included = ['brand.id', 'brand.name', 'brand.twitter', 'brand.description', 'brand.hide', 'brand.website', 'brand.' + lang + '.*']
            }
            def filters = []
            filters << [regexp: ['category.path': categoryPath.toLowerCase() + '.*']]
            if (!hidden) {
                filters << [term: ['brand.hide': false]]
            }
            if (filters.size() > 1) {
                query << [query: [filtered: [filter: [and: filters]]]]
            } else {
                query << [query: [filtered: [filter: filters.get(0)]]]
            }
            def brands = new HashSet<Long>()
            search(store, 'product', query, included)?.hits?.each { result ->
                def brand = result['brand']
                def id = brand['id'] as Long
                if (!brands.contains(id)) {
                    brands << id
                    results << brand
                }
            }
        } else {
            def included = []
            if (lang) {
                included = ['id', 'name', 'twitter', 'description', 'hide', 'website', lang + '.*']
            }
            if (!hidden) {
                query << [query: [filtered: [filter: [term: [hide: false]]]]]
            }
            results = search(store, 'brand', query, included)?.hits
        }
        def mc = [compare: { a, b -> a["name"].toString().compareTo(b["name"].toString()) }] as Comparator
        Collections.sort(results, mc)
        results.collect { Map result -> translate(lang, result) }
    }

    /**
     * This method search products within a store
     * @param store - the store on which the search will be performed
     * @param lang - the language to use for translations
     * @param hidden - whether to include or not hidden products
     * @param criteria - search criteria
     */
    def Page products(String store, String lang, boolean hidden, ProductSearchCriteria criteria) {
        def query = [:]
        def filters = []

        def excluded = []
        if (lang) {
            excluded.addAll(getStoreLanguagesAsList(store).collect { l ->
                if (l != lang) {
                    return [l, '*.' + l]
                }
                []
            }.flatten())
        }

        def locale = Locale.getDefault()

        def country = criteria.country ?: locale.country

        def state = criteria.state ?: null

        def currencyCode = criteria.currencyCode ?: Currency.getInstance(locale).currencyCode

        def rates = search(store, 'rate', [:], ['code', 'rate'])?.hits

        def rate = rates?.find { r ->
            r['code'] == currencyCode
        } ?: ['rate': 0d]

        excluded.addAll(['skus', 'features', 'resources', 'datePeriods', 'intraDayPeriods'])
        if (!hidden) {
            filters << [term: [hide: false]]
        }
        if (criteria.brandId) {
            filters << [term: ['brand.id': criteria.brandId]]
        }
        if (criteria.categoryId) {
            filters << [term: ['category.id': criteria.categoryId]]
        }
        if (criteria.categoryPath) {
            filters << [regexp: ['category.path': criteria.categoryPath.toLowerCase() + '.*']]
        }
        if (criteria.code) {
            filters << [term: [code: criteria.code.toLowerCase()]]
        }
        if (criteria.creationDateMin) {
            String creationDateMin = new SimpleDateFormat('yyyy-MM-dd\'T\'HH:mm:ss\'Z\'').format(
                    IperUtil.parseCalendar(criteria.creationDateMin,
                            IperConstant.DATE_FORMAT_WITHOUT_HOUR).getTime())
            filters << [range: [creationDate: [gte: creationDateMin]]]
        }
        if (criteria.priceMin || criteria.priceMax) {
            def _range = [:]
            if (criteria.priceMin) {
                _range << [gte: criteria.priceMin]
            }
            if (criteria.priceMax) {
                _range << [lte: criteria.priceMax]
            }
            filters << [range: [price: _range]]
        }
        if (criteria.name) {
//                if(lang){// TODO add lang to search criteria
//                    filters << [regexp:[(lang + '.name'):'.*' + criteria.name.toLowerCase() + '.*']]
//                }
//                else{
//                    filters << [regexp:[name:'.*' + criteria.name.toLowerCase() + '.*']]
//                }
            filters << [regexp: [name: '.*' + criteria.name.toLowerCase() + '.*']]
        }
        if (criteria.xtype) {
            filters << [term: [xtype: criteria.xtype]]
        }
        if (criteria.tagName) {
            filters << [regexp: ['tags.name': '.*' + criteria.tagName.toLowerCase() + '.*']]
        }
        if (criteria.featured) {
            def today = new SimpleDateFormat('yyyy-MM-dd\'T\'HH:mm:ss\'Z\'').format(IperUtil.today().getTime())
            filters << [range: [startFeatureDate: [lte: today]]]
            filters << [range: [stopFeatureDate: [gte: today]]]
        }
        query << [sort: [(criteria.orderBy): (criteria.orderDirection.name().toLowerCase())]]
        query << [from: criteria.offset]
        query << [size: criteria.maxItemsPerPage]

        if (filters.size() > 1) {
            query << [query: [filtered: [filter: [and: filters]]]]
        } else if (filters.size() > 0) {
            query << [query: [filtered: [filter: filters.get(0)]]]
        }

        def response = search(store, 'product', query, null, excluded)

        IperUtil.createListePagine(
                response.hits?.collect { Map product ->
                    renderProduct(product, lang, country as String, state as String, locale, currencyCode, rate['rate'] as Double)
                },
                response.total,
                criteria.maxItemsPerPage,
                criteria.pageOffset
        )
    }

    def List<String> dates(String store, Long productId, String date) {
        List<String> results = []

        if (productId) {
            def query = [:]
            def filters = []

            def included = ['datePeriods', 'intraDayPeriods']

            filters << [term: ['id': productId]]

            query << [query: [filtered: [filter: filters.get(0)]]]

            def hits = search(store, 'product', query, included)?.hits

            List<Period> datePeriods = hits.size() > 0 ? hits.get(0)['datePeriods'].collect { datePeriod ->
                return new Period(
                        startDate: new SimpleDateFormat('yyyy-MM-dd\'T\'HH:mm:ss\'Z\'').parse(datePeriod['startDate'] as String),
                        endDate: new SimpleDateFormat('yyyy-MM-dd\'T\'HH:mm:ss\'Z\'').parse(datePeriod['endDate'] as String)
                )
            } : []

            List<DayPeriod> intraDayPeriods = hits.size() > 0 ? hits.get(0)['intraDayPeriods'].collect { intraDayPeriod ->
                return new DayPeriod(
                        startDate: new SimpleDateFormat('yyyy-MM-dd\'T\'HH:mm:ss\'Z\'').parse(intraDayPeriod['startDate'] as String),
                        endDate: new SimpleDateFormat('yyyy-MM-dd\'T\'HH:mm:ss\'Z\'').parse(intraDayPeriod['endDate'] as String),
                        weekday1: intraDayPeriod['weekday1'] as boolean,
                        weekday2: intraDayPeriod['weekday2'] as boolean,
                        weekday3: intraDayPeriod['weekday3'] as boolean,
                        weekday4: intraDayPeriod['weekday4'] as boolean,
                        weekday5: intraDayPeriod['weekday5'] as boolean,
                        weekday6: intraDayPeriod['weekday6'] as boolean,
                        weekday7: intraDayPeriod['weekday7'] as boolean
                )
            } : []

            def today = IperUtil.today()

            Calendar startCalendar = date ? IperUtil.parseCalendar(date, IperConstant.DATE_FORMAT_WITHOUT_HOUR) : today
            if (startCalendar.compareTo(today) < 0) {
                startCalendar = today
            }

            Calendar endCalendar = startCalendar.clone() as Calendar
            endCalendar.add(Calendar.MONTH, 1)


            Calendar currentDate = startCalendar.clone() as Calendar
            Calendar cdate = currentDate.clone() as Calendar
            cdate.clearTime()
            while (cdate.before(endCalendar)) {
                if (isDateIncluded(intraDayPeriods, cdate) && !isDateExcluded(datePeriods, cdate)) {
                    results << RenderUtil.asMapForJSON(cdate, IperConstant.DATE_FORMAT_WITHOUT_HOUR)
                }
                cdate.add(Calendar.DAY_OF_YEAR, 1)
            }
        }

        results
    }

    def List<Map> times(String store, Long productId, String date) {
        List<Map> results = []

        if (productId && date) {

            def today = IperUtil.today()

            Calendar startCalendar = IperUtil.parseCalendar(date, IperConstant.DATE_FORMAT_WITHOUT_HOUR)

            if (startCalendar.compareTo(today) >= 0) {
                def query = [:]
                def filters = []

                def included = ['intraDayPeriods']

                filters << [term: ['id': productId]]
                filters << [term: [calendarType: ProductCalendar.DATE_TIME.name()]]

                query << [query: [filtered: [filter: [and: filters]]]]

                def hits = search(store, 'product', query, included)?.hits
                results.addAll(hits.size() > 0 ? hits.get(0)['intraDayPeriods'].collect { intraDayPeriod ->
                    def period = new DayPeriod(
                            startDate: new SimpleDateFormat('yyyy-MM-dd\'T\'HH:mm:ss\'Z\'').parse(intraDayPeriod['startDate'] as String),
                            endDate: new SimpleDateFormat('yyyy-MM-dd\'T\'HH:mm:ss\'Z\'').parse(intraDayPeriod['endDate'] as String),
                            weekday1: intraDayPeriod['weekday1'] as boolean,
                            weekday2: intraDayPeriod['weekday2'] as boolean,
                            weekday3: intraDayPeriod['weekday3'] as boolean,
                            weekday4: intraDayPeriod['weekday4'] as boolean,
                            weekday5: intraDayPeriod['weekday5'] as boolean,
                            weekday6: intraDayPeriod['weekday6'] as boolean,
                            weekday7: intraDayPeriod['weekday7'] as boolean
                    )
                    def c = Calendar.getInstance()
                    c.setTime(period.startDate)
                    isDateIncluded([period], startCalendar) ? [
                            date     : date,
                            startTime: RenderUtil.asMapForJSON(c, IperConstant.TIME_FORMAT),
                            endTime  : new SimpleDateFormat(IperConstant.TIME_FORMAT).format(period.endDate)
                    ] : []
                }.flatten() as List<Map> : [])
            }
        }

        results
    }

    def List<String> getStoreLanguagesAsList(String store) {
        getStoreLanguages(store)?.languages as List<String>
    }

    def Map getStoreLanguages(String store) {
        search(store, 'i18n', [:], ['languages'], [], [debug: true]).hits?.get(0)
    }

    /**
     * This method performs a search on es
     * @param store - the store on which the search has to be performed
     * @param type - the type of objects
     * @param query - the query to perform on the specified store
     * @param included - the fields to include within the response
     * @param excluded - the fields to exclude from response
     * @param config - a configuration map for http client
     * @param aggregation - whether it is an aggregation search or not
     * @return search results as a list of Map
     */
    def ESSearchResponse search(
            String store,
            String type,
            Map query = [:],
            List<String> included = [],
            List<String> excluded = [],
            Map config = [debug: true],
            boolean aggregation = false) {
        ESRequest request = generateRequest(store, type, query, included, excluded, aggregation)
        request ? client.search(request, addSearchguardCredentials(config)) : new ESSearchResponse(total: 0, hits: [])
    }

    private def Map addSearchguardCredentials(Map config) {
        def searchguard = grailsApplication.config.searchguard as Map
        def active = searchguard?.active
        if (active) {
            config << [username: searchguard.username]
            config << [password: searchguard.password]
        }
        config
    }

    def String saveToLocalStorage(String store, String eventStart, boolean update = false) {
        String dir = "${grailsApplication.config.rootPath}/stores/${store}/"
        File root = new File(dir)
        if (!root.exists() || update) {
            if (root.exists()) root.deleteDir()
            root.mkdirs()

            // save products
            def productsDir = new File(root, "products")
            productsDir.mkdir()
            def products = products(store, null, true, new ProductSearchCriteria(maxItemsPerPage: DEFAULT_MAX_ITEMS)) as Page
            products?.list?.each { Map product ->
                def id = product.id as Long
                saveAsJSON(new File(productsDir, "product_${id}.json"), product)
                // save dates
                if (eventStart == null && product.startDate) {
                    eventStart = RenderUtil.format(RenderUtil.parseFromIso8601(product.startDate as String), IperConstant.DATE_FORMAT_WITHOUT_HOUR)
                }
                def dates = dates(store, id, eventStart)
                if (dates) {
                    saveAsJSON(new File(productsDir, "dates_product_${id}.json"), dates)
                    dates.each { String date ->
                        def times = times(store, id, date)
                        saveAsJSON(new File(productsDir, "date_product_${id}_${date.split(/\//).join("_")}.json"), times)
                    }
                }
            }

            // save brands
            def brandsDir = new File(root, "brands")
            brandsDir.mkdir()
            brands(store, null, true, null, DEFAULT_MAX_ITEMS)?.each { Map brand ->
                saveAsJSON(new File(brandsDir, "brand_${brand.id}.json"), brand)
            }

            // save brands logo
            def srcDir = new File((grailsApplication.config.resources.path as String) + '/brands/logos/' + store + '/')
            if (srcDir.exists()) {
                def destDir = new File(brandsDir, "logos")
                copyFile(srcDir, destDir)
            }

        }
        dir
    }

    def Set<String> retrievePreviousIndices(EsEnv env){
        def url = env.url
        def store = env.company.code
        def conf = [debug: true]
        client.retrieveAliasIndexes(url, "previous_$store", addSearchguardCredentials(conf))
    }

    @Synchronized
    def boolean activateIndex(String index, EsEnv env){
        boolean ret = false
        def url = env.url
        def store = env.company.code
        def conf = addSearchguardCredentials([debug: true])
        def activeIndex = env.activeIndex
        boolean running = EsEnv.withTransaction {
            EsEnv.findByRunning(true) != null
        }
        if(!running && activeIndex != index){
            EsEnv.withTransaction {
                env.refresh()
                env.running = true
                env.save(flush: true)
            }
            ret = client.removeAlias(conf, url, store, activeIndex)?.acknowledged &&
                    client.createAlias(conf, url, store, index)?.acknowledged &&
                    client.createAlias(conf, url, "previous_$store", activeIndex)?.acknowledged
            EsEnv.withTransaction {
                env.refresh()
                if(ret){
                    env.activeIndex = index
                }
                env.running = false
                env.save(flush: true)
            }
        }
        ret
    }

    def PagedList<EsSync> refreshSynchronization(Catalog catalog, PagedListCommand cmd){
        def totalCount = EsSync.executeQuery(
                'select count(*) FROM EsSync where target=:catalog or :catalog in elements(catalogs) ',
                [catalog: catalog])[0]
        def list = EsSync.executeQuery(
                'FROM EsSync where target=:catalog or :catalog in elements(catalogs) ',
                [catalog: catalog],
                (cmd?.pagination ?: [:]) + [sort: "timestamp", order: "desc"]
        )
        new PagedList<EsSync>(list: list, totalCount: totalCount as int)
    }

    def Map prepareSynchronizationAsMap(EsEnv env, Catalog catalog){
        Synchronization synchronization = prepareSynchronization(env, catalog)
        def map = [syncRequired: synchronization.syncRequired] as Map<String, Object>
        def categories = []
        synchronization.categories.each{cat ->
            categories << [id: cat.id, name: cat.name]
        }
        map << [categories: categories]
        def products = []
        synchronization.products.each{product ->
            products << [id: product.id, name: product.name]
        }
        map << [products: products]
    }

    def Synchronization prepareSynchronization(EsEnv env, Catalog catalog){
        Synchronization synchronization = new Synchronization()
        if(env && catalog && env.company == catalog.company){
            final company = env.company
            def syncs = EsSync.executeQuery(
                    'FROM EsSync where esEnv=:env and :catalog in elements(catalogs) ',
                    [env: env, catalog: catalog],
                    [sort: "timestamp", order: "desc"]
            )
            if(!syncs.empty){
                synchronization.previousSync = true
                def lastCompleteSync = syncs.first().timestamp
                def catalogCategories = Category.findAllByCatalogAndLastUpdatedGreaterThan(catalog, lastCompleteSync, [sort: "lastUpdated", order: "desc"])
                catalogCategories.each {cat ->
                    boolean none = EsSync.executeQuery(
                            'FROM EsSync where esEnv=:env and :category in elements(categories) and timestamp >= :timestamp',
                            [env: env, category: cat, timestamp: cat.lastUpdated]
                    ).isEmpty()
                    if(none){
                        synchronization.categories << cat
                    }
                }
                def catalogProducts = Product.findAllByCategoryInListAndLastUpdatedGreaterThan(Category.findAllByCatalog(catalog), lastCompleteSync, [sort: "lastUpdated", order: "desc"])
                catalogProducts.each { product ->
                    boolean none = EsSync.executeQuery(
                            'FROM EsSync where esEnv=:env and :product in elements(products) and timestamp >= :timestamp',
                            [env: env, product: product, timestamp: product.lastUpdated]
                    ).isEmpty()
                    if(none){
                        synchronization.products << product
                    }
                }
            }
        }
        synchronization
    }

    @Synchronized
    def void synchronize(Company company, EsEnv env, Catalog catalog){
        Synchronization synchronization = prepareSynchronization(env, catalog)
        if(!synchronization.previousSync){
            publish(company, env, catalog, true, null)
        }
        else{
            def sync = new EsSync(
                    company: company,
                    esEnv: env,
                    target: catalog
            )
            def syncRequired = synchronization.syncRequired
            final noPublicationRequired = "No run diff publication performed - No item has been modified since the last publication"
            if(!syncRequired){
                sync.timestamp = new Date()
                sync.report = noPublicationRequired
                sync.success = true
            }
            else{
                synchronization.categories.each{ sync.addToCategories(it) }
                synchronization.products.each{ sync.addToProducts(it) }
            }
            sync.validate()
            if(!sync.hasErrors()){
                sync.save(flush: true)
                if(syncRequired){
                    publish(company, env, catalog, true, sync)
                }
                else{
                    env.refresh()
                    env.extra = noPublicationRequired
                    env.validate()
                    if(!env.hasErrors()){
                        env.save(flush: true)
                    }
                }
            }
        }
    }

    @Synchronized
    def void publish(Company company, EsEnv env, Catalog catalog, boolean manual = false, EsSync sync = null) {
        if (catalog?.name?.trim()?.toLowerCase() == "impex") {
            return
        }
        if(sync){
            env = sync?.esEnv
        }
        boolean running = EsEnv.withTransaction {
            EsEnv.findByRunning(true) != null
        }
        if (!running && company && env && env.company == company && catalog && catalog.company == company && (manual || catalog.activationDate < new Date())) {
            log.info("${manual ? "Manual ":""}Export to Elastic Search has started ...")
            EsEnv.withTransaction {
                env.refresh()
                env.running = true
                env.save(flush: true)
            }
            int replicas = grailsApplication.config.elasticsearch.replicas as int ?: 1
            def languages = Translation.executeQuery('SELECT DISTINCT t.lang FROM Translation t WHERE t.companyId=:idCompany', [idCompany: company.id]) as List<String>
            if (languages.size() == 0) {
                languages = [company.defaultLanguage] as List<String>
            }
            def url = env.url
            def store = company.code
            def index = "${store.toLowerCase()}_${DateUtilitaire.format(Calendar.instance, "yyyy.MM.dd.HH.mm.ss")}"
            def debug = true

            // for mirakl catalogs or partial publication, we just perform upserts
            boolean mirakl = catalog.readOnly
            boolean partial = sync != null
            def conf = addSearchguardCredentials([debug: debug])
            def previousIndices = client.retrieveAliasIndexes(url, store, conf)
            List<Catalog> catalogs = []
            List<Long> idCatalogs = [] << catalog.id
            List<Long> idCategories = []
            List<Long> idProducts = []
            if((mirakl || partial) && previousIndices?.size() > 0){
                index = previousIndices.first()
            }
            else {
                partial = false
                catalogs << catalog
                if(!mirakl){
                    Catalog.findAllByCompanyAndReadOnlyAndDeleted(company, true, false).each {mcatalog ->
                        catalogs << mcatalog
                        idCatalogs << mcatalog.id
                    }
                }
            }
            log.info("idCatalogs -> "+idCatalogs.join(","))

            if(partial){
                sync.products.each {
                    idProducts << it.id
                    idCategories << it.category.id
                }
                sync.categories.each {
                    retrieveChildren(it, new HashSet<Category>()).each {cat ->
                        idCategories << cat.id
                        Product.findAllByCategory(cat).each {product ->
                            idProducts << product.id
                        }
                    }
                }
                idCategories.unique()
                log.info("idCategories -> "+idCategories.join(","))
                idProducts.unique()
                log.info("idProducts -> "+idProducts.join(","))
            }

            RiverConfig config = new RiverConfig(
                    clientConfig: new ClientConfig(
                            store: store,
                            url: url,
                            debug: debug,
                            config: [
                                    index   : index,
                                    replicas: replicas
                            ]
                    ),
                    idCatalogs: idCatalogs,
                    languages: languages,
                    defaultLang: company.defaultLanguage,
                    partial: partial,
                    idCompany: company.id,
                    idCategories: idCategories,
                    idProducts: idProducts,
                    bulkSize: 10 //TODO add to grails application configuration
            )
            def searchguard = grailsApplication.config.searchguard as Map
            def active = searchguard?.active
            if (active) {
                config.clientConfig.credentials = new Credentials(
                        client_id: searchguard.username,
                        client_secret: searchguard.password
                )
            }
            def conn = null
            try
            {
                def countries = []
                def http = HTTPClient.instance
                conn = http.doGet([debug:debug], new StringBuffer(grailsApplication.config.mogopay.url as String).append('country/countries-for-shipping').toString())
                if(conn.responseCode >=200 && conn.responseCode < 400){
                    def data = http.getText([debug:debug], conn)
                    if(data && !StringUtils.isEmpty(data.toString())){
                        List<JSONObject> res = JSON.parse(data.toString()) as List<JSONObject>
                        res?.each {JSONObject o ->
                            countries << o.get('code') as String
                        }
                    }
                    config.countries = countries
                }
            }
            catch(Throwable e){
                log.error(e.message)
            }
            finally {
                conn?.disconnect()
            }

            AbstractRiverCache.purgeAll()

            ESIndexResponse response = null

            try{
                response = ESRivers.instance.createCompanyIndex(config)
            }
            catch(Throwable th){
                log.error(th.message, th)
                EsEnv.withTransaction {
                    env.refresh()
                    env.running = false
                    env.save(flush: true)
                }
            }

            if(response?.acknowledged){
                final long before = System.currentTimeMillis()
                def subscriber = new Subscriber<BulkResponse>() {
                    @Override
                    void onCompleted() {
                        log.info("export within ${System.currentTimeMillis() - before} ms")
                        AbstractRiverCache.purgeAll()
                        def extra = ""
                        def success = true
                        if (mirakl || partial || previousIndices.empty || (!previousIndices.any { String previous -> !client.removeAlias(conf, url, store, previous).acknowledged })) {
                            if (!mirakl && !partial && !client.createAlias(conf, url, store, index)) {
                                def revert = env?.idx
                                if (previousIndices.any { previous -> revert = previous; client.createAlias(conf, url, store, previous) }) {
                                    success = false
                                    extra = """
Failed to create alias ${store} for ${index} -> revert to previous index ${revert}
The alias can be created by executing the following command :
curl -XPUT ${url}/$index/_alias/$store
"""
                                    log.warn(extra)
                                } else {
                                    success = false
                                    extra = """
Failed to create alias ${store} for ${index}.
The alias can be created by executing the following command :
curl -XPUT ${url}/$index/_alias/$store
"""
                                    log.warn(extra)
                                }
                            } else {
                                if(!mirakl && !partial){
                                    // on ajoute les indices précédants à l'alias previous_$store
                                    previousIndices.each {previousIndex -> client.createAlias(conf, url, "previous_$store", previousIndex)}
                                    // on récupère la liste des indices ayant l'alias previous_$store
                                    def previousStoreIndices = client.retrieveAliasIndexes(url, "previous_$store", conf)
                                    int nbPreviousStoreIndices = previousStoreIndices.size()
                                    int maxNbPreviousIndices = grailsApplication.config.elasticsearch.previous ?: 3
                                    // si plus d'indices que le maximum autorisé, alors on supprime les indices les plus anciens
                                    if(nbPreviousStoreIndices > maxNbPreviousIndices){
                                        def indexToDate = {String s ->
                                            def dateAsString = s.substring("${store.toLowerCase()}_".length())
                                            DateUtilitaire.parseToDate(dateAsString, "yyyy.MM.dd.HH.mm.ss")
                                        }
                                        previousStoreIndices.sort{a,b->
                                            indexToDate(a)<=>indexToDate(b) // indices triés du plus ancien au plus récent
                                        }.take(nbPreviousStoreIndices-maxNbPreviousIndices).each {
                                            client.removeAlias(conf, url, "previous_$store", it)
                                            client.removeIndex(url, it, conf)
                                        }
                                    }
                                }
                                // on met à jour le nombre de replicas dans l'index courrant
                                client.updateIndex(url, index, new ESIndexSettings(number_of_replicas: replicas), conf)
                                File dir = new File("${grailsApplication.config.resources.path}/stores/${store}")
                                dir.delete()
                                File file = new File("${grailsApplication.config.resources.path}/stores/${store}.zip")
                                file.getParentFile().mkdirs()
                                file.delete()
                                catalog.refresh()
                                extra = "${catalog.name} - ${DateUtilitaire.format(new Date(), "dd/MM/yyyy HH:mm")}"
                                log.info("End ${manual ? "Manual ":""}ElasticSearch export for ${store} -> ${index}")
                                try {
                                    log.info("Start clearing Jahia Cache")
                                    def jahiaClearCache = grailsApplication.config.external?.jahiaClearCache
                                    if(jahiaClearCache){
                                        def jahiaSecret = grailsApplication.config.external.jahiaSecret ?: '12345'
                                        try{
                                            conn = httpClient.doGet([debug: true], jahiaClearCache, ['secret': jahiaSecret, 'storeCode': company.code])
                                            log.info("call to $jahiaClearCache -> ${conn.responseCode}")
                                        }
                                        finally {
                                            try{
                                                conn?.disconnect()
                                            }
                                            catch(IOException ioe){
                                                log.error(ioe.message)
                                            }
                                        }
                                    }
                                    else{
                                        log.warn("grailsApplication.config.external.jahiaClearCache is undefined")
                                    }
                                    log.info("End clearing Jahia Cache")
                                }
                                catch (Exception ex) {
                                    log.warn("Unable to clear Jahia cache -> ${ex.message}")
                                }
                                log.info("Start cache")
                                int exit = 0
                                def cache = grailsApplication.config.cache as Map
                                final home = cache?.home as String
                                final version = cache?.version as String
                                final run = cache?.run as String
                                List<String> cacheUrls = env.cacheUrls?.split(",")?.toList() ?: []
                                if(home && version && run){
                                    cacheUrls << "$run/$store/products/\${product.id}"
                                    cacheUrls << "$run/$store/brands?brandId=\${brand.id}"
                                    cacheUrls << "$run/$store/categories?categoryPath=\${category.path|encode}"
                                    cacheUrls << "$run/$store/categories?parentId=\${category.parentId}"
                                    List<String> args = []
                                    args << "java"
                                    def jar = "$home/mogobiz-cache-${version}.jar".toString()
                                    def app = "$home/application.conf".toString()
                                    def log4j = "$home/log4j.xml".toString()
                                    args << "-cp"
                                    final pathSeparator = System.getProperty("path.separator") ?: ":"
                                    args << "$jar$pathSeparator$app$pathSeparator$log4j$pathSeparator.".toString()
                                    args << "com.mogobiz.cache.bin.ProcessCache"
                                    args << store
                                    cacheUrls.each { u ->
                                        u = u.replace(" ", "").trim()
                                        if(u.length() > 0){
                                            args << u.toString()
                                        }
                                    }
                                    BufferedReader br = null
                                    try{
                                        final ProcessBuilder pb = new ProcessBuilder(args)
                                        pb.directory(new File(home))
                                        pb.redirectErrorStream(true)
                                        log.info("ProcessCache Cmd -> "+pb.command().join(" "))
                                        final Process process = pb.start()
                                        br = new BufferedReader(new InputStreamReader(process.inputStream))
                                        String line
                                        while ((line = br.readLine()) != null) {
                                            log.info(line)
                                        }
                                        exit = process.exitValue()
                                    }
                                    catch(Throwable th){
                                        log.error(th.message, th)
                                    }
                                    finally{
                                        br?.close()
                                    }
                                }
                                log.info("End cache with code -> $exit")
                            }
                        }
                        EsEnv.withTransaction {
                            env.refresh()
                            env.running = false
                            env.success = success
                            if (success) {
                                env.idx = index
                                env.activeIndex = index
                            }
                            env.extra = extra
                            env.save(flush: true)
                        }
                        log.info("Begin sync")
                        EsSync.withTransaction {
                            if(!partial){
                                sync = new EsSync(
                                        company: company.refresh(),
                                        esEnv: env.refresh(),
                                        target: catalog.refresh()
                                )
                                catalogs.each {
                                    sync.addToCatalogs(it.refresh())
                                }
                                sync.validate()
                                if(!sync.hasErrors()){
                                    sync.save(flush: true)
                                }
                                else{
                                    sync.errors.allErrors.each{log.error(it.toString())}
                                }
                            }
                            else{
                                sync.refresh()
                            }
                            sync.success = success
                            sync.report = extra
                            sync.timestamp = new Date()
                            sync.save(flush: true)
                        }
                        log.info("End sync")
                    }

                    @Override
                    void onError(Throwable th) {
                        AbstractRiverCache.purgeAll()
                        log.error(th.message, th)
                        EsEnv.withTransaction {
                            env.refresh()
                            env.running = false
                            env.success = false
                            env.extra = th.message
                            env.save(flush: true)
                        }
                    }

                    @Override
                    void onNext(BulkResponse bulkResponse) {
                        log.info("export ${bulkResponse?.items?.size()} items -> ${bulkResponse?.items?.collect {"${it.type}::${it.id}"}?.join(",")}")
                    }
                }

                GenericRiversFlow.publish(ESRivers.getInstance(), config, 1, new SubscriberAdapter(subscriber))
            }
            else{
                log.error("an error occured while creating index ${response.error}")
            }

        }
    }

    @Synchronized
    def void mogopay(EsEnv env) {
        boolean running = EsEnv.withTransaction {
            EsEnv.findByRunning(true) != null
        }
        if (!running && env ) {
            log.info("Export to Mogopay has started ...")
            EsEnv.withTransaction {
                env.refresh()
                env.running = true
                env.save(flush: true)
            }
            int replicas = grailsApplication.config.mogopay?.elasticsearch?.replicas ?: 1
            def url = env.url
            def store = "mogopay"
            def index = "${store}_${DateUtilitaire.format(Calendar.instance, "yyyy.MM.dd.HH.mm.ss")}"
            def debug = true
            RiverConfig config = new RiverConfig(
                    clientConfig: new ClientConfig(
                            store: store,
                            url: url,
                            debug: debug,
                            config: [
                                    index   : index,
                                    replicas: replicas
                            ]
                    ),
                    idCatalogs: [] as List<Long>,
                    languages: ["fr"],
                    defaultLang: "fr"
            )

            ESIndexResponse response = null

            try{
                response = ESBORivers.instance.createCompanyIndex(config)
            }
            catch(Throwable th){
                log.error(th.message, th)
                EsEnv.withTransaction {
                    env.refresh()
                    env.running = false
                    env.save(flush: true)
                }
            }

            if(response?.acknowledged){
                final long before = System.currentTimeMillis()
                def subscriber = new Subscriber<BulkResponse>() {
                    @Override
                    void onCompleted() {
                        log.info("export within ${System.currentTimeMillis() - before} ms")
                        def extra = ""
                        def success = true
                        def conf = addSearchguardCredentials([debug: debug])
                        def previousIndices = client.retrieveAliasIndexes(url, store, conf)
                        if (previousIndices.empty || (!previousIndices.any { String previous -> !client.removeAlias(conf, url, store, previous).acknowledged })) {
                            if (!client.createAlias(conf, url, store, index)) {
                                def revert = env?.idx
                                if (previousIndices.any { previous -> revert = previous; client.createAlias(conf, url, store, previous) }) {
                                    success = false
                                    extra = """
Failed to create alias ${store} for ${index} -> revert to previous index ${revert}
The alias can be created by executing the following command :
curl -XPUT ${url}/$index/_alias/$store
"""
                                    log.warn(extra)
                                } else {
                                    success = false
                                    extra = """
Failed to create alias ${store} for ${index}.
The alias can be created by executing the following command :
curl -XPUT ${url}/$index/_alias/$store
"""
                                    log.warn(extra)
                                }
                            } else {
                                previousIndices.each { previous -> client.removeIndex(url, previous, conf) }
                                client.updateIndex(url, index, new ESIndexSettings(number_of_replicas: replicas), conf)
                                extra = "$store - ${DateUtilitaire.format(new Date(), "dd/MM/yyyy HH:mm")}"
                                log.info("End ElasticSearch export for ${store} -> ${index}")
                            }
                        }
                        EsEnv.withTransaction {
                            env.refresh()
                            env.running = false
                            env.success = success
                            if (success) {
                                env.idx = index
                            }
                            env.extra = extra
                            env.save(flush: true)
                        }
                    }

                    @Override
                    void onError(Throwable th) {
                        log.error(th.message, th)
                        EsEnv.withTransaction {
                            env.refresh()
                            env.running = false
                            env.success = false
                            env.extra = th.message
                            env.save(flush: true)
                        }
                    }

                    @Override
                    void onNext(BulkResponse bulkResponse) {
                        log.info("export ${bulkResponse?.items?.size()} items -> ${bulkResponse?.items?.collect {"${it.type}::${it.id}"}?.join(",")}")
                    }
                }

                GenericRiversFlow.publish(ESBORivers.getInstance(), config, 1, 10, new SubscriberAdapter(subscriber))
            }
            else{
                log.error("an error occured while creating index ${response.error}")
            }
        }
    }

    private static void saveAsJSON(File f, Object o) {
        def writer = new FileWriter(f)
        JsonBuilder builder = new JsonBuilder()
        builder.call(o)
        builder.writeTo(writer)
        writer.close()
    }

    private static void copyFile(File src, File dest) {
        if (src.isDirectory()) {
            if (!dest.exists()) {
                dest.mkdirs()
            }
            def files = src.list()
            files.each { String file ->
                //construct the src and dest file structure
                File srcFile = new File(src, file)
                File destFile = new File(dest, file)
                // copy file
                copyFile(srcFile, destFile)
            }
        } else {
            InputStream is = new FileInputStream(src)
            OutputStream os = new FileOutputStream(dest)
            byte[] buffer = new byte[1024]
            int len
            while ((len = is.read(buffer)) > 0) {
                os.write(buffer, 0, len)
            }
            is.close()
            os.close()
        }
    }

    private static boolean isDateIncluded(List<DayPeriod> periods, Calendar day) {
        periods.find { period ->
            def included = false
            def dow = day.get(Calendar.DAY_OF_WEEK)
            switch (dow) {
                case Calendar.MONDAY:
                    included = period.weekday1
                    break
                case Calendar.TUESDAY:
                    included = period.weekday2
                    break
                case Calendar.WEDNESDAY:
                    included = period.weekday3
                    break
                case Calendar.THURSDAY:
                    included = period.weekday4
                    break
                case Calendar.FRIDAY:
                    included = period.weekday5
                    break
                case Calendar.SATURDAY:
                    included = period.weekday6
                    break
                case Calendar.SUNDAY:
                    included = period.weekday7
                    break
            }
            Date startDate = period.startDate.clone() as Date
            Date endDate = period.endDate.clone() as Date
            startDate.clearTime()
            endDate.clearTime()
            included &&
                    day.getTime().compareTo(startDate) >= 0 &&
                    day.getTime().compareTo(endDate) <= 0
        }
    }

    private static boolean isDateExcluded(List<Period> periods, Calendar day) {
        periods.find { period ->
            day.getTime().compareTo(period.startDate) >= 0 && day.getTime().compareTo(period.endDate) <= 0
        }
    }

    /**
     *
     * @param store - the store on which the request has to be performed
     * @param type - the type of objects
     * @param query - the query to perform on the specified store
     * @param included - the fields to include within the response
     * @param excluded - the fields to exclude from response
     * @return elastic search request or null if no elastic search environment is associated to this store
     */
    private static ESRequest generateRequest(
            String store,
            String type,
            Map query = [:],
            List<String> included = [],
            List<String> excluded = [],
            boolean aggregation = false) {
        String url = getStoreUrl(store)
        if (url) {
            return new ESRequest(
                    url: url,
                    index: store,
                    type: type,
                    query: query,
                    included: included,
                    excluded: excluded,
                    aggregation: aggregation
            )
        }
        return null
    }

    /**
     * This method translates properties returned by the store to the specified language
     * @param lang - the lang to use for translation
     * @param result - the properties to be translated
     * @return the properties translated
     */
    private static Map translate(String lang, Map result) {
        if (lang) {
            result[lang]?.each { k, v ->
                result[k] = v
            }
            result.remove(lang)

            result.each { k, v ->
                if (v instanceof Map) {
                    translate(lang, v)
                } else if (v instanceof List<Map>) {
                    (v as List<Map>).each { m ->
                        translate(lang, m as Map)
                    }
                }
            }
        }
        result
    }

    /**
     * Format the given amount (in the Mogobiz unit) into the given currency by using
     * the number format of the given country
     * @param amount
     * @param currencyCode
     * @param locale
     * @return the given amount formated
     */
    private static String format(long amount, String currencyCode, Locale locale = Locale.default, double rate = 0) {
        NumberFormat numberFormat = NumberFormat.getCurrencyInstance(locale);
        numberFormat.setCurrency(Currency.getInstance(currencyCode));
        return numberFormat.format(amount * rate);
    }

    private static Map renderProduct(
            Map product,
            String lang,
            String country,
            String state,
            Locale locale,
            String currencyCode,
            double rate) {
        def localTaxRates = product['taxRate'] ? product['taxRate']['localTaxRates'] as List<Map> : []
        def localTaxRate = localTaxRates?.find { ltr ->
            ltr['countryCode'] == country && (!state || ltr['stateCode'] == state)
        }
        def taxRate = localTaxRate ? localTaxRate['rate'] : 0f
        def price = product['price'] ?: 0l
        def endPrice = (price as Long) + ((price * taxRate / 100f) as Long)
        product << ['localTaxRate'  : taxRate,
                    formatedPrice   : format(price as Long, currencyCode, locale, rate),
                    formatedEndPrice: format(endPrice, currencyCode, locale, rate)
        ]
        translate(lang, product)
    }


    private static String getStoreUrl(String store) {
        String url = null
        Collection envs = EsEnv.executeQuery(
                'from EsEnv env where env.active=true and env.company.code=:code', [code: store])
        if (envs && envs.size() > 0) {
            url = envs.get(0).url
        }
        url
    }

    def void publishAll() {
        def cal = Calendar.getInstance()
        cal.set(Calendar.SECOND, 0)
        cal.set(Calendar.MILLISECOND, 0)
        def now = cal.getTime()
        Company.findAll().each { Company company ->
            Collection<Catalog> catalogs = Catalog.findAllByActivationDateLessThanEqualsAndCompanyAndDeleted(
                    new Date(),
                    company,
                    false,
                    [sort: 'activationDate', order: 'desc'])
            Catalog catalog = catalogs.size() > 0 ? catalogs.get(0) : null
            if (catalog) {
                EsEnv.findAllByCompanyAndRunningAndActive(company, false, true).each { env ->
                    def cron = env.cronExpr
                    if (cron && cron.trim().length() > 0 && CronExpression.isValidExpression(cron)
                            && new CronExpression(cron).isSatisfiedBy(now)) {
                        publish(company, env, catalog, false, null)
                    }
                }
            }
        }

    }

    private def retrieveChildren = { Category cat, Set<Category> cats ->
        cats << cat
        cat.children?.each {
            retrieveChildren(it, cats)
        }
        cats
    }
}

class Period {
    Date startDate
    Date endDate
}

class DayPeriod extends Period {
    boolean weekday1
    boolean weekday2
    boolean weekday3
    boolean weekday4
    boolean weekday5
    boolean weekday6
    boolean weekday7
}

class Synchronization implements Serializable {
    List<Category> categories = []
    List<Product> products = []
    boolean previousSync = false
    def boolean isSyncRequired() {
        !categories.isEmpty() || !products.isEmpty()
    }
}