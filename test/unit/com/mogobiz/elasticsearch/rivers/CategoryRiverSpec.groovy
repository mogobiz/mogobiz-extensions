package com.mogobiz.elasticsearch.rivers

import com.mogobiz.elasticsearch.rivers.CategoryRiver
import com.mogobiz.elasticsearch.rivers.ESRivers
import com.mogobiz.store.domain.*
import com.mogobiz.store.service.SanitizeUrlService
import com.mogobiz.client.HTTPClient
import com.mogobiz.rivers.elasticsearch.client.ESBulkIndexResponse
import com.mogobiz.rivers.elasticsearch.client.ESClient
import com.mogobiz.rivers.elasticsearch.client.ESIndexCreationResponse
import com.mogobiz.rivers.elasticsearch.client.ESResponse
import com.mogobiz.rivers.elasticsearch.client.ESUpsertResponse
import com.mogobiz.rivers.elasticsearch.spi.RiverConfig
import grails.test.mixin.Mock
import grails.test.mixin.TestMixin
import grails.test.mixin.support.GrailsUnitTestMixin
import grails.util.Holders
import groovy.json.JsonBuilder
import org.elasticsearch.node.Node as ESNode
import rx.util.functions.Action1
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import spock.lang.Specification

import static java.util.concurrent.TimeUnit.SECONDS
import static org.elasticsearch.node.NodeBuilder.nodeBuilder

/**
 * Created by stephane.manciot@ebiznext.com on 15/02/2014.
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
            ESIndexCreationResponse creationResponse = ESRivers.createCompanyIndex(config, company.index, 1, 1)
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
            Collection<Future<ESUpsertResponse>> collectionOfMaps = []
        when:
            new CategoryRiver().upsertCatalogObjects(config, ec, categories).subscribe({
                collectionOfMaps << it
            } as Action1<Future<ESUpsertResponse>>)
        then:
            true == creationResponse.indexResponse.acknowledged
            true == creationResponse.aliasResponse.acknowledged
            Future<Collection<ESResponse>> futureResult = ESClient.collect(collectionOfMaps, ec)
            Collection<ESResponse> result = Await.result(futureResult, Duration.create(2, SECONDS))
            2 == result.size()
            2 == result.findAll {ESUpsertResponse response ->
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
        ESIndexCreationResponse creationResponse = ESRivers.createCompanyIndex(config, company.index, 1, 1)
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
        Collection<Future<ESBulkIndexResponse>> collectionOfMaps = []
        when:
        new CategoryRiver().bulkIndexCatalogObjects(config, ec, 10).subscribe({
            collectionOfMaps << it
        } as Action1<Future<ESBulkIndexResponse>>)
        then:
        true == creationResponse.indexResponse.acknowledged
        true == creationResponse.aliasResponse.acknowledged
        Future<Collection<ESResponse>> futureResult = ESClient.collect(collectionOfMaps, ec)
        Collection<ESResponse> result = Await.result(futureResult, Duration.create(2, SECONDS))
        1 == result.size()
        false == (result[0] as ESBulkIndexResponse).errors
    }
}
