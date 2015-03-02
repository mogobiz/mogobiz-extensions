package com.mogobiz.cfp

import com.mogobiz.store.domain.Brand
import com.mogobiz.store.domain.BrandProperty
import com.mogobiz.store.domain.BrandPropertyValidation
import com.mogobiz.store.domain.BrandValidation
import com.mogobiz.store.domain.Catalog
import com.mogobiz.store.domain.CatalogValidation
import com.mogobiz.store.domain.Category
import com.mogobiz.store.domain.CategoryValidation
import com.mogobiz.store.domain.Company
import com.mogobiz.store.domain.CompanyValidation
import com.mogobiz.store.domain.DatePeriod
import com.mogobiz.store.domain.DatePeriodValidation
import com.mogobiz.store.domain.Product
import com.mogobiz.store.domain.ProductProperty
import com.mogobiz.store.domain.ProductPropertyValidation
import com.mogobiz.store.domain.ProductValidation
import com.mogobiz.store.domain.Stock
import com.mogobiz.store.domain.Tag
import com.mogobiz.store.domain.TagRender
import com.mogobiz.store.domain.TagValidation
import com.mogobiz.store.domain.TicketType
import com.mogobiz.store.domain.TicketTypeValidation
import com.mogobiz.utils.DateUtilitaire
import grails.test.mixin.Mock
import grails.test.mixin.TestMixin
import grails.test.mixin.support.GrailsUnitTestMixin
import spock.lang.Specification

/**
 * Created by smanciot on 28/07/14.
 */
@TestMixin(GrailsUnitTestMixin)
@Mock([Company, Catalog, Brand, BrandProperty, Tag, Category, Product, ProductProperty, DatePeriod, Stock, TicketType])
class CfpToolsSpec extends Specification{

    def setupSpec(){}

    def cleanupSpec(){}

    def setup(){
        Company.metaClass.getCompanyValidation = {new CompanyValidation()}
        Catalog.metaClass.getCatalogValidation = {new CatalogValidation()}
        Brand.metaClass.getBrandValidation = {new BrandValidation()}
        BrandProperty.metaClass.getBrandPropertyValidation = {new BrandPropertyValidation()}
        Tag.metaClass.getTagValidation = {new TagValidation()}
        Tag.metaClass.getTagRender = {new TagRender()}
        Category.metaClass.getCategoryValidation = {new CategoryValidation()}
        Product.metaClass.getProductValidation = {new ProductValidation()}
        ProductProperty.metaClass.getProductPropertyValidation = {new ProductPropertyValidation()}
        DatePeriod.metaClass.getDatePeriodValidation = {new DatePeriodValidation()}
        TicketType.metaClass.getTicketTypeValidation = {new TicketTypeValidation()}
    }

    def cleanup(){}

    def "company extraction should succeed" (){
        given:

        def cfpName = "devoxx"
        def cfpUrl = "http://cfp.devoxx.fr"
        def company

        when:

        company = CfpTools.extractCompany(cfpName, cfpUrl)
        Thread.sleep(16000)

        then:

        company != null
        Collection<Catalog> catalogs = Catalog.findAllByCompany(company)
        Catalog catalog = catalogs.iterator().next()
        catalog.name == "devoxxFR2015"
        Calendar c = DateUtilitaire.parseToCalendar("16/04/2014T09:30:00", "dd/MM/yyyy'T'HH:mm:ss", Locale.FRANCE)
        DateUtilitaire.compareDate(c.time, catalog.activationDate) == 0L
        Collection<Brand> brands = Brand.findAllByCompany(company)
        brands.size() > 0
        Collection<Category> categories = Category.findAllByCompanyAndCatalog(company, catalog)
        categories.size() > 0
        Collection<Tag> tags = Tag.findAll()
        tags.size() > 0
        Collection<Product> products = Product.findAllByCompany(company)
        products.size() > 0
        Product product = products.iterator().next()
        Collection<ProductProperty> properties = ProductProperty.findAllByProduct(product)
        properties.size() == 2
        properties.each {ProductProperty property ->
            log.info(property.name + "=" + property.value)
        }
    }

}
