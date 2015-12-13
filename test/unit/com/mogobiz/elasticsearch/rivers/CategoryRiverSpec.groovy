package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.BulkItemResponse
import com.mogobiz.common.client.BulkResponse
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.client.ESIndexResponse
import com.mogobiz.elasticsearch.rivers.CategoryRiver
import com.mogobiz.elasticsearch.rivers.ESRivers
import com.mogobiz.store.domain.*
import com.mogobiz.service.SanitizeUrlService
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.http.client.HTTPClient
import grails.test.mixin.Mock
import grails.test.mixin.TestMixin
import grails.test.mixin.support.GrailsUnitTestMixin
import grails.util.Holders
import groovy.json.JsonBuilder
import org.elasticsearch.node.Node as ESNode
import rx.functions.Action1
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import spock.lang.Specification

import static java.util.concurrent.TimeUnit.SECONDS
import static org.elasticsearch.node.NodeBuilder.nodeBuilder

/**
 */
@TestMixin(GrailsUnitTestMixin)
@Mock([Company, Catalog, Category, Translation])
class CategoryRiverSpec extends Specification{

    private static ESNode node = null

    private static SanitizeUrlService sanitizeUrlService

    def setupSpec(){
        grailsApplication.config.elasticsearch.serverURL = 'http://localhost:9200'
        sanitizeUrlService = new SanitizeUrlService()
        node = nodeBuilder().node()
    }

    def cleanupSpec(){
        node.close()
    }

    def setup(){
        Company.metaClass.getCompanyValidation = {new CompanyValidation()}
        Catalog.metaClass.getCatalogValidation = {new CatalogValidation()}
        Category.metaClass.getCategoryValidation = {new CategoryValidation()}
        Category.metaClass.asMapForJSON = {List<String> included = [], List<String> excluded = [], String lang = 'fr' ->
            return new CategoryRender().asMap(included, excluded, delegate as Category, 'fr')
        }
        Translation.metaClass.getTranslationValidation = {new TranslationValidation()}
    }

    def cleanup(){
        def actions = []
        actions << [remove:
                [
                        alias:'test',
                        index:'test_v1'
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
                    new StringBuffer(Holders.config.elasticsearch.serverURL as String, ).append('/_aliases').toString(),
                    null,
                    body)
        }
        finally{
            client.closeConnection(conn)
        }
        ESClient.instance.removeIndex(Holders.config.elasticsearch.serverURL as String, 'test', 1, [debug:true])
    }

    def "upsert company categories should succeed" (){
        given:
            Company company = new Company(code:'TEST', name:'TEST', aesPassword: 'PASSWORD', onlineValidation: false).save()
            String index = company.code.toLowerCase() + '_v1'
            def categories = []
            Catalog catalog = new Catalog(
                    name:'CATALOGUE',
                    description: 'DESCRIPTION',
                    uuid: UUID.randomUUID().toString(),
                    activationDate:new Date(),
                    company:company,
                    social:false).save()
            RiverConfig config = new RiverConfig(
                    url:Holders.config.elasticsearch.serverURL as String,
                    index:company.code,
                    idCatalog: catalog.id,
                    debug:true,
                    languages: ['fr', 'en', 'de', 'es'],
                    defaultLang: 'fr')
            ESIndexResponse creationResponse = ESRivers.createCompanyIndex(config, company.index, 1, 1)
            Category parent = new Category(
                    name : 'parent',
                    description : 'description',
                    company : company,
                    catalog : catalog,
                    uuid : UUID.randomUUID().toString(),
                    sanitizedName : sanitizeUrlService.sanitizeWithDashes('parent'),
                    keywords : 'keywords',
                    hide : false,
                    position : 1
            ).save()
            categories << parent
            categories << new Category(
                    name : 'child',
                    description : 'description',
                    company : company,
                    catalog : catalog,
                    uuid : UUID.randomUUID().toString(),
                    sanitizedName : sanitizeUrlService.sanitizeWithDashes('child'),
                    keywords : 'keywords',
                    hide : false,
                    parent : parent,
                    position : 10
            ).save()
            ExecutionContext ec = ESRivers.dispatcher()
            Collection<Future<BulkResponse>> collectionOfMaps = []
        when:
            new CategoryRiver().upsertCatalogObjects(config, ec, categories).subscribe({
                collectionOfMaps << it
            } as Action1<Future<BulkResponse>>)
        then:
            true == creationResponse.acknowledged
            Future<Collection<BulkResponse>> futureResult = ESRivers.collect(collectionOfMaps, ec)
            Collection<BulkItemResponse> result = Await.result(futureResult, Duration.create(2, SECONDS))?.items
            2 == result.size()
            2 == result.findAll {BulkItemResponse response ->
                index.equals(response.index) && 'category'.equals(response.type)
            }.size()
    }

    def "bulk index company categories should succeed" (){
        given:
        Company company = new Company(code:'TEST', name:'TEST', aesPassword: 'PASSWORD', onlineValidation: false).save()
        Catalog catalog = new Catalog(
                name:'CATALOGUE',
                description: 'DESCRIPTION',
                uuid: UUID.randomUUID().toString(),
                activationDate:new Date(),
                company:company,
                social:false).save()
        RiverConfig config = new RiverConfig(
                url:Holders.config.elasticsearch.serverURL as String,
                index:company.code,
                idCatalog: catalog.id,
                debug:true,
                languages: ['fr', 'en', 'de', 'es'] as String[],
                defaultLang: 'fr')
        ESIndexResponse creationResponse = ESRivers.createCompanyIndex(config, company.index, 1, 1)
        Category parent = new Category(
                name : 'parent',
                description : 'description',
                company : company,
                catalog : catalog,
                uuid : UUID.randomUUID().toString(),
                sanitizedName : sanitizeUrlService.sanitizeWithDashes('parent'),
                keywords : 'keywords',
                hide : false,
                position : 1
        ).save()
        new Category(
                name : 'child',
                description : 'description',
                company : company,
                catalog : catalog,
                uuid : UUID.randomUUID().toString(),
                sanitizedName : sanitizeUrlService.sanitizeWithDashes('child'),
                keywords : 'keywords',
                hide : false,
                parent : parent,
                position : 10
        ).save()
        ExecutionContext ec = ESRivers.dispatcher()
        Collection<Future<BulkResponse>> collectionOfMaps = []
        when:
        new CategoryRiver().bulkIndexCatalogObjects(config, ec, 10).subscribe({
            collectionOfMaps << it
        } as Action1<Future<BulkResponse>>)
        then:
        true == creationResponse.acknowledged
        Future<Collection<BulkResponse>> futureResult = ESRivers.collect(collectionOfMaps, ec)
        Collection<BulkItemResponse> result = Await.result(futureResult, Duration.create(2, SECONDS))?.items
        1 == result.size()
    }
}
