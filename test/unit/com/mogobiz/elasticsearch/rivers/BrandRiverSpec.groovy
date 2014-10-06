package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.BulkItemResponse
import com.mogobiz.common.client.BulkResponse
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.client.ESIndexResponse
import com.mogobiz.elasticsearch.rivers.BrandRiver
import com.mogobiz.elasticsearch.rivers.ESRivers
import com.mogobiz.store.domain.Brand
import com.mogobiz.store.domain.BrandRender
import com.mogobiz.store.domain.BrandValidation
import com.mogobiz.store.domain.Catalog
import com.mogobiz.store.domain.CatalogValidation
import com.mogobiz.store.domain.Company
import com.mogobiz.store.domain.CompanyValidation
import com.mogobiz.store.domain.Translation
import com.mogobiz.store.domain.TranslationValidation
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.http.client.HTTPClient
import grails.test.mixin.Mock
import grails.test.mixin.TestMixin
import grails.test.mixin.support.GrailsUnitTestMixin
import grails.util.Holders
import groovy.json.JsonBuilder
import rx.util.functions.Action1
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import spock.lang.Specification

import static java.util.concurrent.TimeUnit.SECONDS
import static org.elasticsearch.node.NodeBuilder.*
import org.elasticsearch.node.Node as ESNode

/**
 * Created by stephane.manciot@ebiznext.com on 02/02/2014.
 */
@TestMixin(GrailsUnitTestMixin)
@Mock([Company, Catalog, Brand, Translation])
class BrandRiverSpec extends Specification{

    private static ESNode node = null

    def setupSpec(){
        grailsApplication.config.elasticsearch.serverURL = 'http://localhost:9200'
        node = nodeBuilder().node()
    }

    def cleanupSpec(){
        node.close()
    }

    def setup(){
        Company.metaClass.getCompanyValidation = {new CompanyValidation()}
        Catalog.metaClass.getCatalogValidation = {new CatalogValidation()}
        Brand.metaClass.getBrandValidation = {new BrandValidation()}
        Brand.metaClass.asMapForJSON = {List<String> included = [], List<String> excluded = [], String lang = 'fr' ->
            return new BrandRender().asMap(included, excluded, delegate as Brand, 'fr')
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
                    new StringBuffer(Holders.config.elasticsearch.serverURL as String).append('/_aliases').toString(),
                    null,
                    body)
        }
        finally{
            client.closeConnection(conn)
        }
        ESClient.instance.removeIndex(Holders.config.elasticsearch.serverURL as String, 'test', 1, [debug:true])
    }

    def "upsert company brands should succeed" (){
        given:
            Company company = new Company(code:'TEST', index:0, name:'TEST', aesPassword: 'PASSWORD', onlineValidation: false).save()
            String index = company.code.toLowerCase() + '_v1'
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
            ESIndexResponse creationResponse = ESRivers.instance.createCompanyIndex(config, company.index, 1, 1)
            def nike = new Brand(name:'Nike',hide:false,website:'www.nike.fr',company:company).save()
            createTranslation(company, 'en', nike.id, [website:'www.nike.com'])
            def adidas = new Brand(name:'Addidas',hide:false,website:'www.addidas.fr',company:company).save()
            createTranslation(company, 'en', adidas.id, [website:'www.adidas.com'])
            def hidden = new Brand(name:'hideBrand',hide:true,website:'www.hideBrand.fr',company:company).save()
            createTranslation(company, 'en', hidden.id, [website:'www.hideBrand.com'])
            def brands = [nike, adidas, hidden]
            ExecutionContext ec = ESRivers.dispatcher()
            Collection<Future<BulkResponse>> collectionOfMaps = []
        when:
            new BrandRiver().upsertCatalogObjects(config, ec, brands).subscribe({
                collectionOfMaps << it
            } as Action1<Future<BulkResponse>>)
        then:
            true == creationResponse.acknowledged
            Future<Collection<BulkResponse>> futureResult = ESRivers.collect(collectionOfMaps, ec)
            Collection<BulkItemResponse> result = Await.result(futureResult, Duration.create(4, SECONDS))?.items
            3 == result.size()
            3 == result.findAll {BulkItemResponse response ->
                index.equals(response.index) && 'brand'.equals(response.type)
            }.size()
    }

    def "bulk index company brands should succeed" (){
        given:
        Company company = new Company(code:'TEST', index:0, name:'TEST', aesPassword: 'PASSWORD', onlineValidation: false).save()
        Catalog catalog = new Catalog(
                name:'CATALOGUE',
                description: 'DESCRIPTION',
                uuid: UUID.randomUUID().toString(),
                activationDate:new Date(),
                company:company).save()
        RiverConfig config = new RiverConfig(
                url:Holders.config.elasticsearch.serverURL as String,
                index:company.code,
                idCatalog: catalog.id,
                debug:true,
                languages: ['fr', 'en', 'de', 'es'] as String[],
                defaultLang: 'fr')
        ESIndexResponse creationResponse = ESRivers.createCompanyIndex(config, company.index, 1, 1)
        def nike = new Brand(name:'Nike',hide:false,website:'www.nike.fr',company:company).save()
        createTranslation(company, 'en', nike.id, [website:'www.nike.com'])
        def adidas = new Brand(name:'Addidas',hide:false,website:'www.addidas.fr',company:company).save()
        createTranslation(company, 'en', adidas.id, [website:'www.adidas.com'])
        def hidden = new Brand(name:'hideBrand',hide:true,website:'www.hideBrand.fr',company:company).save()
        createTranslation(company, 'en', hidden.id, [website:'www.hideBrand.com'])
        ExecutionContext ec = ESRivers.dispatcher()
        Collection<Future<BulkResponse>> collectionOfMaps = []
        when:
        new BrandRiver().exportCatalogItems(config, ec, 10).subscribe({
            collectionOfMaps << it
        } as Action1<Future<BulkResponse>>)
        then:
        true == creationResponse.acknowledged
        Future<Collection<BulkResponse>> futureResult = ESRivers.collect(collectionOfMaps, ec)
        Collection<BulkItemResponse> result = Await.result(futureResult, Duration.create(2, SECONDS))?.items
        1 == result.size()
    }

    private Translation createTranslation(Company company, String lang, long target, Map translations){
        Translation tr = new Translation(companyId: company?.id, lang: lang, target: target)
        JsonBuilder builder = new JsonBuilder()
        builder.call(translations)
        tr.value = builder.toString()
        tr.validate()
        if(!tr.hasErrors()){
            tr.save(flush:true)
        }
        else{
            tr.errors.allErrors.each {
                log.warn(it)
            }
        }
        tr
    }

}
