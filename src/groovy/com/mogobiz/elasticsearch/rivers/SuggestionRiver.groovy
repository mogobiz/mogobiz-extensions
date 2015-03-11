package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.rivers.spi.AbstractESRiver
import com.mogobiz.store.domain.ProductState
import com.mogobiz.store.domain.Suggestion
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESMapping
import com.mogobiz.elasticsearch.client.ESProperty
import org.hibernate.FlushMode
import org.springframework.transaction.TransactionDefinition
import rx.Observable

/**
 * Created by stephane.manciot@ebiznext.com on 19/02/2014.
 */
class SuggestionRiver extends AbstractESRiver<Suggestion>{

    @Override
    Item asItem(Suggestion suggestion, RiverConfig config){
        Suggestion.withTransaction([propagationBehavior: TransactionDefinition.PROPAGATION_SUPPORTS]) {
            new Item(
                    id: suggestion.id,
                    type: getType(),
                    map: RiverTools.asSuggestionMap(suggestion, config),
                    parent: suggestion && suggestion.pack ? new Item(id: suggestion.pack.id, type: 'product') : null
            )
        }
    }

    @Override
    ESMapping defineESMapping() {
        ESMapping mapping = new ESMapping(type:getType(),
                timestamp:true)
        mapping.properties = new ProductRiver().defineESMapping().properties
        mapping.properties << new ESProperty(name:'required', type:ESClient.TYPE.BOOLEAN, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        mapping.properties << new ESProperty(name:'position', type:ESClient.TYPE.INTEGER, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        mapping.properties << new ESProperty(name:'discount', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        mapping.parent = 'product'
        mapping
    }

    @Override
    Observable<Suggestion> retrieveCatalogItems(RiverConfig config) {
        return Observable.from(Suggestion.executeQuery('FROM Suggestion s join fetch s.product WHERE s.pack.category.catalog.id=:idCatalog and s.product.state = :productState',
                [idCatalog:config.idCatalog, productState:ProductState.ACTIVE], [flushMode: FlushMode.MANUAL]))
    }

    @Override
    String getType() {
        return 'suggestion'
    }

    @Override
    String getUuid(Suggestion s){
        s.uuid
    }

}
