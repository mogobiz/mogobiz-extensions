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
 * Created by smanciot on 10/04/15.
 */
@Log4j
class DownloadableRiver extends AbstractESRiver<File>{

    @Override
    Observable<File> retrieveCatalogItems(RiverConfig config) {
        Set<String> ticketTypes = TicketType.executeQuery('FROM TicketType as ticketType left join ticketType.product as product where (product.category.catalog.id=:idCatalog and product.state=:productState)',
                [idCatalog:config.idCatalog, productState:ProductState.ACTIVE], [readOnly: true, flushMode: FlushMode.MANUAL]).collect {it. as String}.toSet()
        def f = {file -> ticketTypes.contains(file.name)}as Func1<File, Boolean>
        StringBuilder sb = new StringBuilder(Holders.config.resources.path as String)
                .append(File.separator)
                .append('resources')
                .append(File.separator)
        sb.append(config?.clientConfig?.store).append(File.separator).append('sku')
        return Observable.from(FileTools.scan(sb.toString())).filter(f)
    }

    @Override
    Item asItem(File file, RiverConfig config) {
        new Item(id: file?.name, type: getType(), map: RiverTools.asDownloadableMap(file, config))
    }

    @Override
    ESMapping defineESMapping() {
        new ESMapping(type:getType(),
                timestamp:true,
                properties: [] << new ESProperty(name:'file', type:ESClient.TYPE.ATTACHMENT, index:ESClient.INDEX.NO, multilang:false))
    }

    @Override
    String getType() {
        return "downloadable"
    }

}
