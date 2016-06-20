package com.mogobiz.elasticsearch.rivers

import com.mogobiz.cfp.CfpTools
import com.mogobiz.common.client.BulkResponse
import com.mogobiz.common.client.ClientConfig
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.http.client.HTTPClient
import com.mogobiz.store.domain.Brand
import com.mogobiz.store.domain.BrandProperty
import com.mogobiz.store.domain.BrandPropertyValidation
import com.mogobiz.store.domain.BrandRender
import com.mogobiz.store.domain.BrandValidation
import com.mogobiz.store.domain.Catalog
import com.mogobiz.store.domain.CatalogValidation
import com.mogobiz.store.domain.Category
import com.mogobiz.store.domain.CategoryRender
import com.mogobiz.store.domain.CategoryValidation
import com.mogobiz.store.domain.Company
import com.mogobiz.store.domain.CompanyValidation
import com.mogobiz.store.domain.Coupon
import com.mogobiz.store.domain.DatePeriod
import com.mogobiz.store.domain.DatePeriodRender
import com.mogobiz.store.domain.DatePeriodValidation
import com.mogobiz.store.domain.Feature
import com.mogobiz.store.domain.IntraDayPeriod
import com.mogobiz.store.domain.Product
import com.mogobiz.store.domain.Product2Resource
import com.mogobiz.store.domain.ProductProperty
import com.mogobiz.store.domain.ProductPropertyValidation
import com.mogobiz.store.domain.ProductRender
import com.mogobiz.store.domain.ProductValidation
import com.mogobiz.store.domain.Resource
import com.mogobiz.store.domain.Stock
import com.mogobiz.store.domain.StockCalendar
import com.mogobiz.store.domain.Suggestion
import com.mogobiz.store.domain.Tag
import com.mogobiz.store.domain.TagRender
import com.mogobiz.store.domain.TagValidation
import com.mogobiz.store.domain.TicketType
import com.mogobiz.store.domain.TicketTypeRender
import com.mogobiz.store.domain.TicketTypeValidation
import com.mogobiz.store.domain.Translation
import com.mogobiz.store.domain.TranslationValidation
import grails.test.mixin.Mock
import grails.test.mixin.TestMixin
import grails.test.mixin.support.GrailsUnitTestMixin
import grails.util.Holders
import groovy.json.JsonBuilder
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import spock.lang.Specification

import static java.util.concurrent.TimeUnit.SECONDS
import static org.elasticsearch.node.NodeBuilder.*
import org.elasticsearch.node.Node as ESNode

/**
 */
@TestMixin(GrailsUnitTestMixin)
@Mock([Company, Catalog, Brand, BrandProperty, Tag, Category, Product, ProductProperty, DatePeriod, Stock, TicketType,
        Translation, Suggestion, Coupon, Product2Resource, Resource, Feature, StockCalendar, IntraDayPeriod])
class CfpRiverSpec extends Specification{

    private static ESNode node = null

    def setupSpec(){
        grailsApplication.config.elasticsearch.serverURL = 'http://localhost:9200'
        grailsApplication.config.mogopay.url = "http://mogopay.ebiznext.com/pay/"
        node = nodeBuilder().node()
    }

    def cleanupSpec(){
        node.close()
    }

    def setup(){
        Company.metaClass.getCompanyValidation = {new CompanyValidation()}

        Catalog.metaClass.getCatalogValidation = {new CatalogValidation()}

        Brand.metaClass.getBrandValidation = {new BrandValidation()}
        BrandProperty.metaClass.getBrandPropertyValidation = {new BrandPropertyValidation()}
        Brand.metaClass.asMapForJSON = {List<String> included = [], List<String> excluded = [], String lang = 'fr' ->
            return new BrandRender().asMap(included, excluded, delegate as Brand, 'fr')
        }

        Tag.metaClass.getTagValidation = {new TagValidation()}
        def tagRender = new TagRender()
        Tag.metaClass.asMapForJSON = {List<String> included = [], List<String> excluded = [], String lang = 'fr' ->
            return tagRender.asMap(included, excluded, delegate as Tag, 'fr')
        }
        Tag.metaClass.toString = { ->
            return tagRender.asString(delegate as Tag)
        }

        Category.metaClass.getCategoryValidation = {new CategoryValidation()}
        Category.metaClass.asMapForJSON = {List<String> included = [], List<String> excluded = [], String lang = 'fr' ->
            return new CategoryRender().asMap(included, excluded, delegate as Category, 'fr')
        }

        Product.metaClass.getProductValidation = {new ProductValidation()}
        Product.metaClass.toString = { ->
            return new ProductRender().asString(delegate as Product)
        }
        Product.metaClass.asMapForJSON = {List<String> included = [], List<String> excluded = [], String lang = 'fr' ->
            return new ProductRender().asMap(included, excluded, delegate as Product, 'fr')
        }

        ProductProperty.metaClass.getProductPropertyValidation = {new ProductPropertyValidation()}

        DatePeriod.metaClass.getDatePeriodValidation = {new DatePeriodValidation()}
        DatePeriod.metaClass.asMapForJSON = {List<String> included = [], List<String> excluded = [], String lang = 'fr' ->
            return new DatePeriodRender().asMap(included, excluded, delegate as DatePeriod, 'fr')
        }

        TicketType.metaClass.getTicketTypeValidation = {new TicketTypeValidation()}
        TicketType.metaClass.asMapForJSON = {List<String> included = [], List<String> excluded = [], String lang = 'fr' ->
            return new TicketTypeRender().asMap(included, excluded, delegate as TicketType, 'fr')
        }

        Translation.metaClass.getTranslationValidation = {new TranslationValidation()}

//        ProductRiver.metaClass {
//            retrieveCatalogItems {RiverConfig config ->
//                    def company = Company.findByName("devoxx")
//                    rx.Observable.from(Product.findAllByCompanyAndState(company, ProductState.ACTIVE))
//            }
//        }
    }

    def cleanup(){
        def actions = []
        actions << [remove:
                            [
                                    alias:'devoxx',
                                    index:'devoxx_v1'
                            ]
        ]
        JsonBuilder builder = new JsonBuilder()
        builder.call([actions:actions])
        final String body = builder.toString()
        def client = HTTPClient.instance
        def conn = null
        try{
            conn = client.doPost(
                    [debug:true],
                    new StringBuffer(Holders.config.elasticsearch.serverURL as String).append('/_aliases').toString(),
                    null,
                    body)
        }
        finally{
            client.closeConnection(conn)
        }
        ESClient.instance.removeIndex(Holders.config.elasticsearch.serverURL as String, 'devoxx', [debug:true])
    }

    def "export cfp catalog should succeed" (){
        given:
        def cfpName = "devoxx"
        def cfpUrl = "http://cfp.devoxx.fr"
        def company = CfpTools.extractCompany(cfpName, cfpUrl)
        def catalog = Catalog.findAllByCompany(company).iterator().next()
        RiverConfig config = new RiverConfig(
                clientConfig: new ClientConfig(
                        store: company.code,
                        url:Holders.config.elasticsearch.serverURL as String,
                        debug:true,
                        config:[
                                version: 0,
                                shards:1,
                                replicas:1
                        ]
                ),
                idCatalog: catalog.id,
                languages: ['fr', 'en', 'de', 'es'],
                defaultLang: company.defaultLanguage
        )
        ExecutionContext ec = ESRivers.dispatcher()
        when:
        Future<Collection<BulkResponse>> futureResult = ESRivers.instance.export(config, ec)
        then:
        Collection<BulkResponse> result = Await.result(futureResult, Duration.create(10, SECONDS))
        result.size() > 0
        result.each {BulkResponse response ->
            response.responseCode >= 200 && response.responseCode < 400
        }

    }

}
