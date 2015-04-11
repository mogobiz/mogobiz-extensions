package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESMapping
import com.mogobiz.elasticsearch.client.ESProperty
import com.mogobiz.elasticsearch.rivers.spi.AbstractESRiver
import grails.util.Holders
import groovy.util.logging.Log4j
import rx.Observable

/**
 *
 * Created by smanciot on 10/04/15.
 */
@Log4j
class DownloadableRiver extends AbstractESRiver<File>{

    @Override
    Observable<File> retrieveCatalogItems(RiverConfig config) {
        StringBuilder sb = new StringBuilder(Holders.config.resources.path as String)
                .append(File.separator)
                .append('resources')
                .append(File.separator)
        sb.append(config?.clientConfig?.store).append(File.separator).append('sku')
        return Observable.from(RiverTools.extractFiles(sb.toString(), '*'))
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
