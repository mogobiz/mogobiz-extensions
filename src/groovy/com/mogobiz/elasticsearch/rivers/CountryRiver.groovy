package com.mogobiz.elasticsearch.rivers

import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.elasticsearch.rivers.spi.AbstractESRiver
import com.mogobiz.http.client.HTTPClient
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESMapping
import com.mogobiz.elasticsearch.client.ESProperty
import com.mogobiz.common.client.Item
import com.mogobiz.store.vo.Country
import grails.converters.JSON
import grails.util.Holders
import org.apache.commons.lang.StringUtils
import org.codehaus.groovy.grails.web.json.JSONObject
import rx.Observable

/**
 * Created by stephane.manciot@ebiznext.com on 24/02/2014.
 */
class CountryRiver extends AbstractESRiver<Country> {

    @Override
    ESMapping defineESMapping() {
        new ESMapping(type:getType(),
                timestamp:true,
                properties: [] << new ESProperty(name:'code', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
                        << new ESProperty(name:'name', type:ESClient.TYPE.STRING, index:ESClient.INDEX.NOT_ANALYZED, multilang:true)
                        << new ESProperty(name:'imported', type:ESClient.TYPE.DATE, index:ESClient.INDEX.NOT_ANALYZED, multilang:false)
        )
    }

    @Override
    Observable<Country> retrieveCatalogItems(final RiverConfig config) {
        def retrieveCountries = {
            def countries = []
            def conn = null
            try
            {
                def debug = config.debug
                def http = HTTPClient.instance
                conn = http.doGet([debug:debug], new StringBuffer(Holders.config.mogopay.url as String).append('country/countriesForShipping').toString())
                if(conn.responseCode >=200 && conn.responseCode < 400){
                    def data = http.getText([debug:debug], conn)
                    if(data && !StringUtils.isEmpty(data.toString())){
                        List<JSONObject> res = JSON.parse(data.toString()) as List<JSONObject>
                        res.each {JSONObject o ->
                            countries << new Country(-1L, o.get('code') as String, o.get('name') as String)
                        }
                    }
                }
            }
            finally {
                conn?.disconnect()
            }
            return countries
        }
        return Observable.from(retrieveCountries())
    }

    @Override
    String getType() {
        return 'country'
    }

    @Override
    Item asItem(Country country, RiverConfig config) {
        new Item(id:country.id, type: getType(), map:RiverTools.asCountryMap(country, config))
    }

}

