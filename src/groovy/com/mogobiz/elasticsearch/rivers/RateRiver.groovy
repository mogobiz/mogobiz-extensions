package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.rivers.spi.AbstractESRiver
import com.mogobiz.utils.MogopayRate
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESMapping
import com.mogobiz.elasticsearch.client.ESProperty

import rx.Observable

/**
 * Created by stephane.manciot@ebiznext.com on 24/02/2014.
 */
class RateRiver extends AbstractESRiver<MogopayRate>{

    @Override
    ESMapping defineESMapping() {
        new ESMapping(type:getType(),
                timestamp:true,
                properties: [] << new ESProperty(name:'code', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'uuid', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'rate', type:ESClient.TYPE.DOUBLE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'currencyFractionDigits', type:ESClient.TYPE.INTEGER, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'imported', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        )
    }

    @Override
    Observable<MogopayRate> retrieveCatalogItems(final RiverConfig config) {
        return Observable.from(RiverTools.retrieveRates(config))
    }

    @Override
    String getType() {
        return 'rate'
    }

    @Override
    Item asItem(MogopayRate rate, RiverConfig config) {
        new Item(id:UUID.randomUUID().toString(), type: getType(), map:RiverTools.asRateMap(rate, config))
    }
}
