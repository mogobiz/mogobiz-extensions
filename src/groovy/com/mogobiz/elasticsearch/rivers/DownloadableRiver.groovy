/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESMapping
import com.mogobiz.elasticsearch.client.ESProperty
import com.mogobiz.elasticsearch.rivers.spi.AbstractESRiver
import com.mogobiz.store.domain.ProductState
import com.mogobiz.store.domain.TicketType
import com.mogobiz.tools.FileTools
import grails.util.Holders
import groovy.util.logging.Log4j
import org.hibernate.FlushMode
import rx.Observable
import rx.functions.Func1

/**
 *
 */
@Log4j
class DownloadableRiver extends AbstractESRiver<File>{

    @Override
    Observable<File> retrieveCatalogItems(RiverConfig config) {
        Calendar now = Calendar.getInstance()
        final args = [readOnly: true, flushMode: FlushMode.MANUAL]
        List<TicketType> list = config.partial ? TicketType.executeQuery('FROM TicketType as ticketType left join ticketType.product as product where (product.id in (:idProducts) and product.state=:productState and (ticketType.stopDate is null or ticketType.stopDate >= :today))',
                [idProducts:config.idProducts, productState:ProductState.ACTIVE, today: now], args).flatten() : TicketType.executeQuery('FROM TicketType as ticketType left join ticketType.product as product where (product.category.catalog.id in (:idCatalogs) and product.state=:productState and (ticketType.stopDate is null or ticketType.stopDate >= :today))',
                [idCatalogs:config.idCatalogs, productState:ProductState.ACTIVE, today: now], args).flatten()
        List<String> ticketTypes = new ArrayList<>(list.size())
        list.each {ticketType ->
            ticketTypes << (ticketType.id as String)
        }
        def f = {file -> ticketTypes.contains(file.name)}as Func1<File, Boolean>
        StringBuilder sb = new StringBuilder(Holders.config.resources.path as String)
                .append(File.separator)
                .append('resources')
                .append(File.separator)
        sb.append(config?.clientConfig?.store).append(File.separator).append('sku')
        final folder = sb.toString()
        log.debug("scaning downloadable elements from $folder")
        return Observable.from(FileTools.scan(folder)).filter(f)
    }

//    @Override
    Item asItem(File file, RiverConfig config) {
        new Item(id: file?.name, type: getType(), map: RiverTools.asDownloadableMap(file, config))
    }

    @Override
    ESMapping defineESMapping() {
        def fileProperties = []
        fileProperties << new ESProperty(name:'content', type:ESClient.TYPE.BINARY, index:ESClient.INDEX.NO, multilang:false)
        fileProperties << new ESProperty(name:'content_type', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        fileProperties << new ESProperty(name:'md5', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)

        new ESMapping(type:getType(),
                timestamp:true,
                properties: [] << new ESProperty(name:'file', type:ESClient.TYPE.OBJECT, properties: fileProperties)
        )
    }

    @Override
    String getType() {
        return "downloadable"
    }

}
