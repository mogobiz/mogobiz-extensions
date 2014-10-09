package com.mogobiz.service

import com.mogobiz.common.client.BulkAction
import com.mogobiz.common.client.BulkItem
import com.mogobiz.common.client.ClientConfig
import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.constant.IperConstant
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESIndexSettings
import com.mogobiz.elasticsearch.client.ESRequest
import com.mogobiz.elasticsearch.client.ESSearchResponse
import com.mogobiz.elasticsearch.rivers.ESRivers
import com.mogobiz.json.RenderUtil
import com.mogobiz.store.Comment
import com.mogobiz.store.CommentSearchCriteria
import com.mogobiz.store.HistorySearchCriteria
import com.mogobiz.store.OrderDirection
import com.mogobiz.store.ProductSearchCriteria
import com.mogobiz.store.customer.StoreSessionData
import com.mogobiz.store.domain.Catalog
import com.mogobiz.store.domain.Company
import com.mogobiz.store.domain.EsEnv
import com.mogobiz.store.domain.ProductCalendar
import com.mogobiz.store.domain.Translation
import com.mogobiz.utils.IperUtil
import com.mogobiz.utils.Page
import grails.plugin.cache.Cacheable
import grails.transaction.Transactional
import groovy.json.JsonBuilder
import scala.Function1
import scala.concurrent.ExecutionContext

import java.text.NumberFormat
import java.text.SimpleDateFormat

/**
 *
 * Created by smanciot on 01/09/14.
 */
@Transactional
class ElasticsearchService {

    ESClient client = ESClient.getInstance()

    UuidDataService uuidDataService

    def grailsApplication

    /**
     * This method lists countries for the specified store
     * @param store - the store on which the search will be performed
     * @param lang - the language to use for translations
     */
    def List<Map> countries(String store, String lang) {
        def included = []
        if (lang) {
            included = ['code', 'name', lang + '.*']
        }
        search(store, 'country', [:], included)?.hits?.collect { Map result ->
            translate(lang, result)
        }
    }

    /**
     * This method lists currencies for the specified store
     * @param store - the store on which the search will be performed
     * @param lang - the language to use for translations
     * @param currency - the current currency if any
     */
    def List<Map> currencies(String store, String lang, String currency) {
        def included = []
        if (lang) {
            included = ['code', 'name', 'rate', 'currencyFractionDigits', lang + '.*']
        }
        search(store, 'rate', [:], included)?.hits?.collect { Map result ->
            translate(lang, result) << [active: currency ? currency == result['code'] : false]
        }
    }

    /**
     * This method lists tags for the specified store
     * @param store - the store on which the search will be performed
     * @param categoryId - search tags for products which are associated to this category
     * @param tagName - the tag that has been clicked
     */
    def tags(String store, Long categoryId, String tagName) {
        def results
        if (categoryId) {
            def query = [query: [filtered: [filter: [[term: ['category.id': categoryId]]]]]]
            def tags = new HashSet<String>()
            results = []
            search(store, 'product', query, ['tags'])?.hits?.each { result ->
                (result['tags'] as List).each { tag ->
                    def name = tag['name'] as String
                    if (!tags.contains(name)) {
                        tags << name
                        results << tag
                    }
                }
            }
        } else if (tagName) {
            Set<Long> products = []
            Set<Long> brands = []
            Set<Long> categories = []
            def query = [query: [filtered: [filter: [[term: ['tags.name': tagName.toLowerCase()]]]]]]
            search(store, 'product', query, ['id', 'category', 'brand'])?.hits?.each { result ->
                products << (result['id'] as Long)
                categories << (result['category']['id'] as Long)
                brands << (result['brand']['id'] as Long)
            }
            def map = [script: 'ctx._source.increments+=1', params: [:]]
            String url = getStoreUrl(store)
            List<BulkItem> items = []
            products.each { id ->
                items << new BulkItem(type: 'product', id: id, map: map, action: BulkAction.UPDATE)
            }
            brands.each { id ->
                items << new BulkItem(type: 'brand', id: id, map: map, action: BulkAction.UPDATE)
            }
            categories.each { id ->
                items << new BulkItem(type: 'category', id: id, map: map, action: BulkAction.UPDATE)
            }
            // update tag increments
            query = [query: [filtered: [filter: [[term: ['name': tagName.toLowerCase()]]]]]]
            search(store, 'tag', query, ['id'])?.hits?.each { result ->
                items << new BulkItem(
                        type: 'tag',
                        id: result['id'],
                        map: [
                                script: 'ctx._source.increments+=increment',
                                params: [
                                        increment: products.size() + categories.size() + brands.size()
                                ]
                        ],
                        action: BulkAction.UPDATE
                )
            }
            ExecutionContext ec = ESRivers.dispatcher()
            RiverConfig config = new RiverConfig(
                    clientConfig: new ClientConfig(
                            url: url,
                            debug: true,
                            config: [index: store]
                    )
            )
            client.bulk(config, items, ec)

            // return immediately
            results = [:] << ['products': products, 'categories': categories, 'brands': brands]
        } else {
            results = search(store, 'tag', [:], ['name'])?.hits
        }
        results
    }

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
     * This method lists categories for the specified store
     * @param store - the store on which the search will be performed
     * @param lang - the language to use for translations
     * @param hidden - whether to include or not hidden categories
     * @param parentId - the parent category to which the categories are associated
     * @param brandId - the brand to which the categories are associated
     * @param category - the category which has to be looked up
     */
    def List<Map> categories(String store, String lang, boolean hidden, long parentId, long brandId, String category) {
        def included = []
        if (lang) {
            included = ['id', 'uuid', 'path', 'name', 'description', 'keywords', 'parentId', lang + '.*']
        }
        def query = [:]
        def filters = []
        if (brandId) {
            def prefix = 'category.'
            if (lang) {
                included = included.collect { it -> prefix + it }
            }
            filters << [term: ['brand.id': brandId]]
            if (!hidden) {
                filters << [term: ['category.hide': false]]
            }
            if (parentId) {
                filters << [term: ['category.parentId': parentId]]
            }
            if (category) {
                filters << [regexp: ['category.path': '.*' + category.toLowerCase() + '.*']]
            }
//            else if(!parentId) {
//                filters << [missing:[field:'category.parentId', existence:true, null_value:true]]
//            }
            if (filters.size() > 1) {
                query << [query: [filtered: [filter: [and: filters]]]]
            } else if (filters.size() > 0) {
                query << [query: [filtered: [filter: filters.get(0)]]]
            }
            def results = []
            def categories = new HashSet<Long>()
            search(store, 'product', query, included)?.hits?.each { result ->
                def cat = result['category']
                def id = cat['id'] as Long
                if (!categories.contains(id)) {
                    categories << id
                    results << cat
                }
            }
            results.collect { Map result -> translate(lang, result as Map) }
        } else {
            if (!hidden) {
                filters << [term: [hide: false]]
            }
            if (parentId) {
                filters << [term: [parentId: parentId]]
            }
            if (category) {
                filters << [regexp: [path: '.*' + category.toLowerCase() + '.*']]
            } else if (!parentId) {
                filters << [missing: [field: 'parentId', existence: true, null_value: true]]
            }
            if (filters.size() > 1) {
                query << [query: [filtered: [filter: [and: filters]]]]
            } else if (filters.size() > 0) {
                query << [query: [filtered: [filter: filters.get(0)]]]
            }
            search(store, 'category', query, included)?.hits?.collect { Map result -> translate(lang, result) }
        }
    }

    /**
     * This method search products within a store
     * @param session - the store session
     * @param store - the store on which the search will be performed
     * @param productId - the product id
     * @param lang - the language to use for translations
     * @param hidden - whether to include or not hidden products
     * @param addToHistory - whether the product should be added or not to history
     * @param criteria - search criteria
     */
    def Object products(StoreSessionData session, String store, Long productId, String lang, boolean hidden, boolean addToHistory, ProductSearchCriteria criteria) {
        def query = [:]
        def filters = []
        def storeSessionData = session ?: new StoreSessionData(companyCode: store)

        def excluded = []
        if (lang) {
            excluded.addAll(getStoreLanguagesAsList(store).collect { l ->
                if (l != lang) {
                    return [l, '*.' + l]
                }
                []
            }.flatten())
        }

        def locale = Locale.getDefault()//FIXME RequestContextUtils.getLocale(request)

        def country = criteria.country ?: storeSessionData.country ?: locale.country

        def state = criteria.state ?: storeSessionData.state ?: null

        def currencyCode = criteria.currencyCode ?: storeSessionData.currency ?: Currency.getInstance(locale).currencyCode

        def rates = search(store, 'rate', [:], ['code', 'rate'])?.hits

        def rate = rates?.find { r ->
            r['code'] == currencyCode
        } ?: ['rate': 0d]

        if (productId) {
            filters << [term: ['id': productId]]
            def today = new SimpleDateFormat('yyyy-MM-dd\'T\'HH:mm:ss\'Z\'').format(IperUtil.today().getTime())
            filters << [range: [startDate: [lte: today]]]
            filters << [range: [stopDate: [gte: today]]]
            if (addToHistory) {
                uuidDataService.addProduct(productId)
            }
        } else if (criteria.sku) {
            filters << [term: ['skus.sku': criteria.sku]]
            def today = new SimpleDateFormat('yyyy-MM-dd\'T\'HH:mm:ss\'Z\'').format(IperUtil.today().getTime())
            filters << [range: [startDate: [lte: today]]]
            filters << [range: [stopDate: [gte: today]]]
            if (criteria.categoryPath) {
                filters << [regexp: ['category.path': criteria.categoryPath.toLowerCase() + '.*']]
            }
            if (addToHistory) {
                uuidDataService.addProduct(productId)
            }
        } else {
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
        }

        if (filters.size() > 1) {
            query << [query: [filtered: [filter: [and: filters]]]]
        } else if (filters.size() > 0) {
            query << [query: [filtered: [filter: filters.get(0)]]]
        }

        def response = search(store, 'product', query, null, excluded)

        productId || criteria.sku ? (response.hits && response.hits.size() > 0 ? response.hits.collect { Map product ->
            renderProduct(product, lang, country as String, state as String, locale, currencyCode, rate['rate'] as Double)
        }.get(0) : [:]) : IperUtil.createListePagine(
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

    /**
     * Returns the list of visited products
     * @param session - the store session
     * @param store - the store on which the search will be performed
     * @param lang - the language to use for translations
     * @param hidden - whether to include or not hidden products
     * @param criteria - history criteria
     */
    def Page history(StoreSessionData session, String store, String lang, boolean hidden, HistorySearchCriteria criteria) {
        def query = [:]

        def excluded = ['skus', 'features', 'resources', 'datePeriods', 'intraDayPeriods']
        if (lang) {
            excluded.addAll(getStoreLanguagesAsList(store).collect { l ->
                if (l != lang) {
                    return [l, '*.' + l]
                }
                []
            }.flatten())
        }

        def filters = []
        if (!hidden) {
            filters << [term: [hide: false]]
        }

        def storeSessionData = session ?: new StoreSessionData(companyCode: store)

        def locale = Locale.getDefault()//FIXME RequestContextUtils.getLocale(request)

        def country = criteria.country ?: storeSessionData.country ?: locale.country

        def state = criteria.state ?: storeSessionData.state ?: null

        def currencyCode = criteria.currencyCode ?: storeSessionData.currency ?: Currency.getInstance(locale).currencyCode

        def rates = search(store, 'rate', [:], ['code', 'rate'])?.hits

        def rate = rates?.find { r ->
            r['code'] == currencyCode
        } ?: ['rate': 0d]

        def response
        String[] idProducts = uuidDataService.getProducts() ?: []

        if (idProducts && idProducts.length > 0) {
            filters << [terms: ['id': idProducts]]
            def today = new SimpleDateFormat('yyyy-MM-dd\'T\'HH:mm:ss\'Z\'').format(IperUtil.today().getTime())
            filters << [range: [startDate: [lte: today]]]
            filters << [range: [stopDate: [gte: today]]]
            query << [query: [filtered: [filter: [and: filters]]]]
            response = search(store, 'product', query, null, excluded)
        } else {
            response = new ESSearchResponse(total: 0, hits: [])
        }

        IperUtil.createListePagine(
                response.hits?.collect { Map product ->
                    renderProduct(product, lang, country as String, state as String, locale, currencyCode, rate['rate'] as Double)
                },
                response.total,
                criteria.maxItemsPerPage,
                criteria.pageOffset
        )
    }

    /**
     * create a new comment
     * @param store - the store on which the comment will be saved
     * @param comment - the comment to save
     * @return
     */
    def Map saveComment(String store, Comment comment) {
        def index = store + '_comment'
        String url = getStoreUrl(store)
        comment.validate()
        if (comment.hasErrors()) {
            def errors = []
            comment.errors.allErrors.each {
                errors << it.defaultMessage
            }
            [success: false, errors: errors]
        } else {
            RiverConfig config = new RiverConfig(
                    clientConfig: new ClientConfig(
                            url: url,
                            debug: true,
                            config: [index: index]
                    )
            )
            client.upsert(
                    config,
                    [
                            new Item(
                                    type: 'comment',
                                    id: comment.id,
                                    map: [
                                            userUuid   : comment.userUuid,
                                            nickname   : comment.nickname,
                                            notation   : comment.notation,
                                            subject    : comment.subject,
                                            comment    : comment.comment,
                                            productUuid: comment.productUuid,
                                            useful     : 0,
                                            notuseful  : 0,
                                            created    : RenderUtil.formatToIso8601(new Date())
                                    ]
                            )
                    ],
                    ESRivers.dispatcher()
            ).subscribe()
            [success: true]
        }
    }

    /**
     * update comment
     * @param store - the store on which the comment will be updated
     * @param commentId - the comment id
     * @param notation - 0 if not useful, 1 if useful
     * @return
     */
    def Map updateComment(String store, String commentId, short notation) {
//        def body = '{"script":"if(useful){ctx._source.useful +=1}else{ctx._source.notuseful +=1}","params":{"useful":' +
//                (notation > 0) + '}}'
        String url = getStoreUrl(store)
        def index = store + '_comment'
        RiverConfig config = new RiverConfig(
                clientConfig: new ClientConfig(
                        url: url,
                        debug: true,
                        config: [index: index]
                )
        )
        client.bulk(
                config,
                [
                        new BulkItem(
                                type: 'comment',
                                id: commentId,
                                action: BulkAction.UPDATE,
                                map: [
                                        script: 'if(useful){ctx._source.useful +=1}else{ctx._source.notuseful +=1}',
                                        params: [
                                                useful: notation > 0
                                        ]
                                ]
                        )
                ],
                ESRivers.dispatcher()
        ).subscribe()
        [success: true]
    }

    /**
     * load product comments
     * @param store - the store on which the search will be performed
     * @param criteria - search criteria
     * @param callback - jsonp callback
     * @return
     */
    def Object listProductComments(String store, CommentSearchCriteria criteria) {
        criteria.validate()
        if (criteria.hasErrors()) {
            def errors = []
            criteria.errors.allErrors.each {
                errors << it.defaultMessage
            }
            [success: false, errors: errors]
        } else {
            def query = [:]
            if (criteria.userUuid == null || criteria.userUuid.length() == 0) {
                query << [query: [match:/*[filtered: [filter: [term: */ [productUuid: criteria.productUuid]]]/*]]*/
                if (criteria.aggregation) {
                    query << [aggs: [rates_count: [terms: [field: 'notation']]]]
                }
            } else {
                query << [query: [filtered: [filter:
                                                     [and:
                                                              [
                                                                      [term: ['productUuid': criteria.productUuid]],
                                                                      [term: ['userUuid': criteria.userUuid]]
                                                              ]
                                                     ]
                ]]]
            }
            if (!criteria.aggregation) {
                query << [sort: ['created': (OrderDirection.DESC.name().toLowerCase())]]
                query << [from: criteria.offset]
                query << [size: criteria.maxItemsPerPage]
            }
            def response = client.search(
                    new ESRequest(
                            url: getStoreUrl(store),
                            index: store + '_comment',
                            type: 'comment',
                            query: query,
                            included: [],
                            excluded: ['userId'],
                            aggregation: criteria.aggregation
                    ),
                    [debug: true]
            )
            if (criteria.aggregation) {
                response.aggregations
            } else {
                IperUtil.createListePagine(
                        response.hits,
                        response.total,
                        criteria.maxItemsPerPage,
                        criteria.pageOffset
                )
            }
        }
    }

    /**
     *
     * @param store - the store on which the search will be performed
     * @param lang - the language to use
     * @param q - the terms to look for
     * @param highlight - whether to highlight or not the terms we are looking for
     */
    def Map find(String store, String lang, String q, boolean highlight) {

        def fields = ['name', 'description', 'descriptionAsText', 'keywords']
        def queryFields = []

        if (lang) {
            fields.each { field ->
                queryFields << field
                queryFields << lang + '.' + field
                //queryFields << '*.' + lang + '.' + field
            }
        } else {
            fields.each { field ->
                queryFields << field
                getStoreLanguagesAsList(store).each { l ->
                    queryFields << l + '.' + field
                    //queryFields << '*.' + l + '.' + field
                }
            }
        }

        def query = [query: [query_string: [fields: queryFields, query: q]]]
        def included = ['id']
        included.addAll(queryFields)
        def excluded = []

        if (highlight) {
            def m = [:]
            queryFields.each { field ->
                m.put(field, [:])
            }
            query << [highlight: [fields: m]]
        }

        def response = search(store, 'brand,category,product', query, included, excluded)
        List<Map> hits = response.hits
        [brands: hits['brand'], categories: hits['category'], products: hits['product']]
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
        request ? client.search(request, config) : new ESSearchResponse(total: 0, hits: [])
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
            def products = products(null, store, null, null, true, false, new ProductSearchCriteria(maxItemsPerPage: 999999)) as Page
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
            brands(store, null, true, null, 999999)?.each { Map brand ->
                saveAsJSON(new File(brandsDir, "brand_${brand.id}.json"), brand)
            }

            // save brands logo
            def srcDir = new File((grailsApplication.config.rootPath as String) + '/brands/logos/' + store + '/')
            if (srcDir.exists()) {
                def destDir = new File(brandsDir, "logos")
                copyFile(srcDir, destDir)
            }

        }
        dir
    }

    def publish(Company company, EsEnv env, Catalog catalog) {
        if (company && env && env.company == company && catalog && catalog.company == company && catalog.activationDate < new Date()) {
            log.info("Export to Elastic Search has started ...")
            int replicas = grailsApplication.config.elasticsearch.replicas as int ?: 1
            def languages = Translation.executeQuery('SELECT DISTINCT t.lang FROM Translation t WHERE t.companyId=:idCompany', [idCompany: company.id]) as String[]
            if (languages.length == 0) {
                languages = [company.defaultLanguage]
            }
            def url = env.url
            def store = company.code
            def index = "${store.toLowerCase()}_${System.currentTimeMillis()}"
            def debug = true
            def ec = ESRivers.dispatcher()
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
                    idCatalog: catalog.id,
                    languages: languages,
                    defaultLang: company.defaultLanguage
            )
            ESRivers.instance.export(config, ec).onComplete({
                def conf = [debug: debug]
                def previousIndices = client.retrieveAliasIndexes(url, store, conf)
                if (previousIndices.empty || (!previousIndices.any { String previous -> !client.removeAlias(conf, url, store, previous).acknowledged })) {
                    if (!client.createAlias(conf, url, store, index)) {
                        def revert = env?.idx
                        if (previousIndices.any { previous -> revert = previous; client.createAlias(conf, url, store, previous) }) {
                            log.warn("Failed to create alias ${store} for ${index} -> revert to previous index ${revert}")
                            log.warn("""
The alias can be created by executing the following command :
curl -XPUT ${url}/$index/_alias/$store
""")
                        } else {
                            log.fatal("Failed to create alias ${store} for ${index}.")
                            log.fatal("""
The alias can be created by executing the following command :
curl -XPUT ${url}/$index/_alias/$store
""")
                        }
                    } else {
                        EsEnv.withTransaction {
                            env?.idx = index
                            env?.save(flush: true)
                        }
                        previousIndices.each { previous -> client.removeIndex(url, previous, conf) }
                        client.updateIndex(url, index, new ESIndexSettings(number_of_replicas: replicas), conf)
                        File dir = new File("${grailsApplication.config.rootPath}/stores/${store}")
                        dir.delete()
                        File file = new File("${grailsApplication.config.rootPath}/stores/${store}.zip")
                        file.getParentFile().mkdirs()
                        file.delete()
                        log.info("End ElasticSearch export for ${store} -> ${index}")
                    }
                }
            } as Function1, ec)
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
    private ESRequest generateRequest(
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


    @Cacheable('globalCache')
    private String getStoreUrl(String store) {
        String url = null
        Collection envs = EsEnv.executeQuery(
                'from EsEnv env where env.active=true and env.company.code=:code', [code: store])
        if (envs && envs.size() > 0) {
            url = envs.get(0).url
        }
        url
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
