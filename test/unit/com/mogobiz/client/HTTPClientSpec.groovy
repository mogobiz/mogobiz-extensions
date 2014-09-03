
/**
 * 
 */
package com.mogobiz.client

import com.mogobiz.http.client.HTTPClient
import spock.lang.Specification
import static org.elasticsearch.node.NodeBuilder.*
import org.elasticsearch.node.Node as ESNode

/**
 * @author stephane.manciot@ebiznext.com
 *
 */
class HTTPClientSpec extends Specification {

    private static ESNode node = null

    def setupSpec(){
        node = nodeBuilder().node()
    }

    def cleanupSpec(){
        node.close()
    }

    def "201 when indexing new document" (){
        given:
            HTTPClient client = HTTPClient.instance
            Map config = [debug:true]
        when:
            HttpURLConnection conn = client.doPut(config, 'http://localhost:9200/cds/music/1', null, '{"title":"sample title"}')
        then:
            201 == conn.responseCode
            Map obj = client.parseTextAsJSON(config, conn)
            null != obj
            'cds' == obj['_index']
            'music' == obj['_type']
            '1' == obj['_id']
    }
}
