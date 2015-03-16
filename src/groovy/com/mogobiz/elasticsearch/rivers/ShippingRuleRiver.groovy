package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESMapping
import com.mogobiz.elasticsearch.client.ESProperty
import com.mogobiz.elasticsearch.rivers.spi.AbstractESRiver
import com.mogobiz.store.domain.ShippingRule
import org.hibernate.FlushMode
import org.springframework.transaction.TransactionDefinition

/**
 *
 * Created by smanciot on 25/09/14.
 */
class ShippingRuleRiver extends AbstractESRiver<ShippingRule> {
    @Override
    rx.Observable<ShippingRule> retrieveCatalogItems(RiverConfig riverConfig) {
        return rx.Observable.from(ShippingRule.executeQuery("FROM ShippingRule sr WHERE sr.company.code =:code",
                [code:riverConfig.clientConfig.store], [readOnly: true, flushMode: FlushMode.MANUAL]))
    }

    @Override
    Item asItem(ShippingRule shippingRule, RiverConfig riverConfig) {
        new Item(id:shippingRule.uuid, type: getType(), map:
                ShippingRule.withTransaction([propagationBehavior: TransactionDefinition.PROPAGATION_SUPPORTS]) {
                    RiverTools.asShippingRuleMap(shippingRule, riverConfig)
                }
        )
    }

    @Override
    ESMapping defineESMapping() {
        new ESMapping(type:getType(),
                timestamp:true,
                properties: []
                        << new ESProperty(name:'uuid', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'dateCreated', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'lastUpdated', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'countryCode', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'minAmount', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'maxAmount', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'price', type:ESClient.TYPE.LONG, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'percentage', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'absolute', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        )
    }

    @Override
    String getType() {
        return "shipping_rule"
    }

    @Override
    String getUuid(ShippingRule r){
        r.uuid
    }

}
