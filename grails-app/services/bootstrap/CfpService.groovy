package bootstrap

import com.mogobiz.cfp.CfpTools
import com.mogobiz.store.domain.Company
import com.mogobiz.store.domain.EsEnv
import grails.util.Holders

/**
 * Created by smanciot on 15/08/14.
 */
class CfpService {

    CommonService commonService

    def destroy(){}

    void init(){
        Company devoxx = CfpTools.extractCompany("devoxx", "http://cfp.devoxx.fr")
        EsEnv env = new EsEnv(
                name:'dev',
                url:Holders.config.elasticsearch.serverURL as String,
                cronExpr: Holders.config.elasticsearch.export.cron as String,
                company: devoxx,
                active: true
        )
        commonService.saveEntity(env)
    }
}
