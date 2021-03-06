/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.cfp

// cfp objects
import com.mogobiz.rivers.cfp.CfpClient
import com.mogobiz.rivers.cfp.CfpConferenceDetails
import com.mogobiz.rivers.cfp.CfpSchedule
import com.mogobiz.rivers.cfp.CfpSlot
import com.mogobiz.rivers.cfp.CfpSpeakerDetails
import com.mogobiz.rivers.cfp.CfpTalk
import com.mogobiz.rivers.cfp.CfpTalkSpeaker

// services
import com.mogobiz.service.SanitizeUrlService

// domain
import com.mogobiz.store.domain.Brand
import com.mogobiz.store.domain.BrandProperty
import com.mogobiz.store.domain.Catalog
import com.mogobiz.store.domain.Category
import com.mogobiz.store.domain.Company
import com.mogobiz.store.domain.IntraDayPeriod
import com.mogobiz.store.domain.Product
import com.mogobiz.store.domain.ProductCalendar
import com.mogobiz.store.domain.ProductProperty
import com.mogobiz.store.domain.ProductState
import com.mogobiz.store.domain.ProductType
import com.mogobiz.store.domain.Stock
import com.mogobiz.store.domain.Tag
import com.mogobiz.store.domain.TicketType
import com.mogobiz.tools.MimeTypeTools
import grails.util.Holders
import groovy.util.logging.Log4j
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import scala.Function1

import java.text.Normalizer
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING
import static java.nio.file.Files.*
import static java.nio.file.Paths.get


// rxjava-reactive-streams
import rx.Subscriber
import rx.internal.reactivestreams.SubscriberAdapter

// akka

import akka.actor.ActorSystem
import akka.dispatch.Futures

import java.util.concurrent.Callable


import static scala.collection.JavaConversions.*

/**
 */
final class CfpTools {

    private CfpTools(){}

    private static final SanitizeUrlService sanitizeUrlService = new SanitizeUrlService()

    private static final Log log = LogFactory.getLog(CfpTools.class.name)

    static final ActorSystem AVATARS = ActorSystem.create("AVATARS")

    static Company extractCompany(String cfpName, String cfpUrl){
        def company = null
        Company.withTransaction {
            def code = normalizeName(cfpName).toLowerCase()
            company = Company.findByCode(code)
            if(!company){
                def extractedCompany = new Company(
                        code: code,
                        name: cfpName,
                        website: cfpUrl,
                        aesPassword: "changeit"
                )
                extractedCompany.validate()
                if(!extractedCompany.hasErrors()){
                    company = extractedCompany
                    company.save()
                }
                else{
                    extractedCompany.errors.allErrors.each {log.error(it)}
                }
            }
            if(company){
                Subscriber<CfpConferenceDetails> subscriber = new Subscriber<CfpConferenceDetails>() {
                    @Override
                    public void onCompleted() {
                        log.info("-> finish Cfp ${cfpName} extraction from ${cfpUrl}")
                        //CfpClient.system().shutdown()
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        log.error(throwable.message, throwable)
                    }

                    @Override
                    public void onNext(CfpConferenceDetails conference) {
                        extractCatalog(company, conference)
                    }
                };
                CfpClient.loadAllConferences(cfpUrl, new SubscriberAdapter<CfpConferenceDetails>(subscriber));
            }
        }
        company
    }

    static Catalog extractCatalog(Company company, CfpConferenceDetails conference){
        def catalog = null
        def slots = extractSlots(conference)
        Catalog.withTransaction {
            catalog = Catalog.findByCompanyAndUuid(company, conference.eventCode())
            if(!catalog){
                CfpSlot firstSlot = slots.sort(new Comparator<CfpSlot>() {
                    @Override
                    int compare(CfpSlot o1, CfpSlot o2) {
                        return o1.fromTimeMillis().compareTo(o2.fromTimeMillis())
                    }
                }).get(0)
                Catalog extractedCatalog = new Catalog(
                        name:conference.eventCode(),
                        uuid:conference.eventCode(),
                        social:false,
                        activationDate:new Date(firstSlot.fromTimeMillis()),
                        company:company
                )
                extractedCatalog.validate()
                if(!extractedCatalog.hasErrors()) {
                    catalog = extractedCatalog
                    catalog.save()
                }
                else{
                    extractedCatalog.errors.allErrors.each {log.error(it)}
                }
            }
            if(catalog){
                extractBrands(company, conference)
                extractProducts(company, catalog, conference, slots)
            }
        }
        catalog
    }

    static Collection<Brand> extractBrands(Company company, CfpConferenceDetails conference){
        Collection<Brand> brands = []
        Brand.withTransaction {
            for(Iterator<CfpSpeakerDetails> speakers = asJavaIterator(conference.speakers().iterator()); speakers.hasNext();){
                CfpSpeakerDetails speaker = speakers.next()
                def brand = Brand.findByCompanyAndUuid(company, speaker.uuid())
                if(!brand){
                    def exportedBrand = new Brand(
                            uuid: speaker.uuid(),
                            name: speaker.name(),
                            website: speaker.blog(),
                            description: speaker.bio(),
                            twitter: speaker.twitter(),
                            company: company,
                            hide: false
                    )
                    exportedBrand.validate()
                    if(!exportedBrand.hasErrors()){
                        brand = exportedBrand
                        brand.save()
                    }
                    else{
                        brand.errors.allErrors.each {log.error(it)}
                    }
                }
                if(brand){
                    brands << brand
                    final avatarURL = speaker.avatarURL()
                    if(avatarURL){
                        BrandProperty property = BrandProperty.findByBrandAndName(brand, "avatarURL")
                        property?.delete(flush: true)
                        property = new BrandProperty(brand: brand, name:"avatarURL", value: avatarURL)
                        property.validate()
                        if(!property.hasErrors()){
                            property.save()

                            final String file = conference.avatarFile(speaker.uuid()).getOrElse(null)
                            if(file){
                                def dir = "${Holders.config.resources.path}/brands/logos/${company.code}"
                                File d = new File(dir)
                                d.mkdirs()
                                def to = "$dir/${new File(file).name}"
                                log.info("moving $file to $to")
                                move(get(file), get(to), REPLACE_EXISTING)
                            }
                            else{
                                def ec = AVATARS.dispatcher()

                                // download speaker avatar
                                Futures.future(new Callable<String>() {
                                    @Override
                                    String call() throws Exception {
                                        def dir = "${Holders.config.resources.path}/brands/logos/${company.code}"
                                        File d = new File(dir)
                                        d.mkdirs()
                                        def destination = "$dir/${brand.id}"
                                        download(avatarURL, destination)
                                        File f = new File(destination)
                                        def extension = MimeTypeTools.toFormat(f)
                                        if(extension){
                                            destination = "$dir/${brand.id}$extension"
                                            move(get(f.absolutePath), get(destination), REPLACE_EXISTING)
                                        }
                                        destination
                                    }
                                }, ec).onComplete({
                                    log.info("=> AVATAR ${avatarURL} DOWNLOADED")
                                }as Function1, ec)
                            }

                        }
                        else{
                            property.errors.allErrors.each {log.error(it)}
                        }
                    }
                }
            }
        }
        brands
    }

    static Collection<Product> extractProducts(Company company, Catalog catalog, CfpConferenceDetails conference, List<CfpSlot> slots = extractSlots(conference)){
        Collection<Product> products = []
        Product.withTransaction {
            slots.each{CfpSlot slot ->
                CfpTalk talk = slot.talk().get()
                Category category = Category.findByCompanyAndCatalogAndName(company, catalog, talk.talkType())
                if(!category){
                    Category extractedCategory = new Category(company: company, catalog: catalog, name: talk.talkType(), uuid: UUID.randomUUID().toString(), position:0)
                    extractedCategory.validate()
                    if(!extractedCategory.hasErrors()){
                        category = extractedCategory
                        category.save()
                    }
                    else{
                        extractedCategory.errors.allErrors.each {log.error(it)}
                    }
                }
                if(category){
                    Collection<Tag> tags = []
                    talk.track().split(",").each {
                        String name = it.trim()
                        Tag tag = Tag.findByName(name)
                        if(!tag){
                            Tag extractedTag = new Tag(name:name)
                            extractedTag.validate()
                            if(!extractedTag.hasErrors()){
                                tag = extractedTag
                                tag.save()
                            }
                            else{
                                tag.errors.allErrors.each {log.error(it)}
                            }
                        }
                        if(tag){
                            tags << tag
                        }
                    }
                    def code = normalizeName(talk.title()).toLowerCase()
                    Product product = Product.findByCompanyAndCode(company, code)
                    if(!product){
                        def extractedProduct = new Product(
                                company: company,
                                category: category,
                                uuid: UUID.randomUUID().toString(),
                                externalCode: talk.id(),
                                code: code,
                                name: talk.title(),
                                sanitizedName: sanitizeUrlService.sanitizeWithDashes(talk.title()),
                                xtype: ProductType.OTHER,
                                price: 0L,
                                description: talk.summaryAsHtml(),
                                descriptionAsText: talk.summary(),
                                calendarType: ProductCalendar.DATE_TIME,
                                startDate: fromTimeInMillis(slot.fromTimeMillis()),
                                stopDate: fromTimeInMillis(slot.toTimeMillis()),
                                keywords: talk.track(),
                                tags: tags,
                                state: ProductState.ACTIVE
                        )
                        extractedProduct.validate()
                        if(!extractedProduct.hasErrors()){
                            product = extractedProduct
                            product.save()
                        }
                        else{
                            extractedProduct.errors.allErrors.each {log.error(it)}
                        }
                    }
                    if(product){
                        products << product
                        // properties
                        def speakers = []
                        for(Iterator<CfpTalkSpeaker> talkSpeakers = asJavaIterator(talk.speakers().iterator()); talkSpeakers.hasNext();){
                            CfpTalkSpeaker speaker = talkSpeakers.next()
                            def uuid = speaker.uuid()
                            Brand brand = uuid.isSuccess() ? Brand.findByCompanyAndUuid(company, uuid.get()) : null
                            if(brand){
                                speakers << brand.id.toString()
//                                if (product.brand  == null) {
//                                    product.brand = brand
//                                    product.save()
//                                }
                            }
                        }
                        addProductProperty(product, "speakers", speakers.join(","))
                        addProductProperty(product, "room", slot.roomName())
                        // period
//                        DatePeriod period = new DatePeriod(
//                                product: product,
//                                startDate: product.startDate,
//                                endDate: product.stopDate
//                        )
                        IntraDayPeriod period = new IntraDayPeriod(
                                product: product,
                                startDate: product.startDate,
                                endDate: product.stopDate,
                                weekday1:true,
                                weekday2:true,
                                weekday3:true,
                                weekday4:true,
                                weekday5:true,
                                weekday6:true,
                                weekday7:true
                        )
                        period.validate()
                        if(!period.hasErrors()){
                            period.save()
                        }
                        else{
                            period.errors.allErrors.each {log.error(it)}
                        }
                        // stock
                        Stock stock = new Stock(stock: Math.max(0, slot.roomCapacity()), stockUnlimited: (slot.roomCapacity() <= 0));
                        stock.validate()
                        if(!stock.hasErrors()){
                            stock.save()
                            // sku
                            def sku = new TicketType(
                                    sku:UUID.randomUUID().toString(),
                                    name: product.name,
                                    price: product.price,
                                    minOrder: 1,
                                    maxOrder: -1,
                                    product: product,
                                    stock: stock,
                                    startDate: product.startDate,
                                    stopDate: product.stopDate);
                            sku.validate()
                            if(!sku.hasErrors()){
                                sku.save()
                            }
                            else{
                                sku.errors.allErrors.each {log.error(it)}
                            }
                        }
                        else{
                            stock.errors.allErrors.each {log.error(it)}
                        }
                    }
                }
            }
        }
        products
    }

    private static download(String url, String destination)
    {
        def out = null
        try{
            def file = new FileOutputStream(destination)
            out = new BufferedOutputStream(file)
            out << new URL(url).openStream()
        }
        catch(IOException e){
            log.error(e.message)
            out?.close()
        }
        finally{
            out?.close()
        }
    }

    private static List<CfpSlot> extractSlots(CfpConferenceDetails conference) {
        List<CfpSlot> slots = []
        for (Iterator<CfpSchedule> schedules = asJavaIterator(conference.schedules().iterator()); schedules.hasNext();) {
            CfpSchedule schedule = schedules.next()
            for (Iterator<CfpSlot> scheduleSlots = asJavaIterator(schedule.slots().iterator()); scheduleSlots.hasNext();) {
                def slot = scheduleSlots.next()
                if (slot.talk().isDefined()) {
                    slots << slot
                }
            }
        }
        slots
    }

    private static void addProductProperty(Product product, String name, String value) {
        if(value && value.trim().length() > 0){
            ProductProperty property = ProductProperty.findByProductAndName(product, name)
            property?.delete(flush: true)
            property = new ProductProperty(product: product, name: name, value: value)
            property.validate()
            if (!property.hasErrors()) {
                property.save()
            } else {
                property.errors.allErrors.each { log.error(it) }
            }
        }
    }

    private static Calendar fromTimeInMillis(long timeInMillis){
        def c = Calendar.getInstance()
        c.timeInMillis = timeInMillis
        c
    }

    private static String normalizeName(String cfpName) {
        return Normalizer.normalize(cfpName, Normalizer.Form.NFD)
                .replaceAll("\\s", "-").replaceAll("\\p{IsM}+", "").replaceAll("[^a-zA-Z0-9-]", "");
    }
}
