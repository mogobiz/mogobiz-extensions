package com.mogobiz.service

import com.mogobiz.common.client.BulkAction
import com.mogobiz.common.client.BulkItem
import com.mogobiz.common.client.ClientConfig
import com.mogobiz.common.client.Item
import com.mogobiz.common.rivers.spi.RiverConfig
import com.mogobiz.constant.IperConstant
import com.mogobiz.elasticsearch.client.ESClient
import com.mogobiz.elasticsearch.client.ESIndexSettings
import com.mogobiz.elasticsearch.client.ESRequest
import com.mogobiz.elasticsearch.client.ESSearchResponse
import com.mogobiz.elasticsearch.rivers.ESRivers
import com.mogobiz.json.RenderUtil
import com.mogobiz.store.Comment
import com.mogobiz.store.CommentSearchCriteria
import com.mogobiz.store.HistorySearchCriteria
import com.mogobiz.store.OrderDirection
import com.mogobiz.store.ProductSearchCriteria
import com.mogobiz.store.customer.StoreSessionData
import com.mogobiz.store.domain.Catalog
import com.mogobiz.store.domain.Company
import com.mogobiz.store.domain.EsEnv
import com.mogobiz.store.domain.ProductCalendar
import com.mogobiz.store.domain.Translation
import com.mogobiz.utils.IperUtil
import com.mogobiz.utils.Page
import grails.plugin.cache.Cacheable
import grails.transaction.Transactional
import groovy.json.JsonBuilder
import org.quartz.CronExpression
import scala.Function1
import scala.concurrent.ExecutionContext

import java.text.NumberFormat
import java.text.SimpleDateFormat

/**
 *
 * Created by smanciot on 01/09/14.
 */
@Transactional
class ElasticsearchService {

    ESClient client = ESClient.getInstance()

    def grailsApplication

    def publish(Company company, EsEnv env, Catalog catalog) {
        if (company && env && env.company == company && catalog && catalog.company == company && catalog.activationDate < new Date()) {
            log.info("Export to Elastic Search has started ...")
            int replicas = grailsApplication.config.elasticsearch.replicas as int ?: 1
            def languages = Translation.executeQuery('SELECT DISTINCT t.lang FROM Translation t WHERE t.companyId=:idCompany', [idCompany: company.id]) as String[]
            if (languages.length == 0) {
                languages = [company.defaultLanguage]
            }
            def url = env.url
            def store = company.code
            def index = "${store.toLowerCase()}_${System.currentTimeMillis()}"
            def debug = true
            def ec = ESRivers.dispatcher()
            RiverConfig config = new RiverConfig(
                    clientConfig: new ClientConfig(
                            store: store,
                            url: url,
                            debug: debug,
                            config: [
                                    index   : index,
                                    replicas: replicas
                            ]
                    ),
                    idCatalog: catalog.id,
                    languages: languages,
                    defaultLang: company.defaultLanguage
            )
            ESRivers.instance.export(config, ec).onComplete({
                def conf = [debug: debug]
                def previousIndices = client.retrieveAliasIndexes(url, store, conf)
                if (previousIndices.empty || (!previousIndices.any { String previous -> !client.removeAlias(conf, url, store, previous).acknowledged })) {
                    if (!client.createAlias(conf, url, store, index)) {
                        def revert = env?.idx
                        if (previousIndices.any { previous -> revert = previous; client.createAlias(conf, url, store, previous) }) {
                            log.warn("Failed to create alias ${store} for ${index} -> revert to previous index ${revert}")
                            log.warn("""
The alias can be created by executing the following command :
curl -XPUT ${url}/$index/_alias/$store
""")
                        } else {
                            log.fatal("Failed to create alias ${store} for ${index}.")
                            log.fatal("""
The alias can be created by executing the following command :
curl -XPUT ${url}/$index/_alias/$store
""")
                        }
                    } else {
                        EsEnv.withTransaction {
                            env?.idx = index
                            env?.save(flush: true)
                        }
                        previousIndices.each { previous -> client.removeIndex(url, previous, conf) }
                        client.updateIndex(url, index, new ESIndexSettings(number_of_replicas: replicas), conf)
                        File dir = new File("${grailsApplication.config.resources.path}/stores/${store}")
                        dir.delete()
                        File file = new File("${grailsApplication.config.resources.path}/stores/${store}.zip")
                        file.getParentFile().mkdirs()
                        file.delete()
                        log.info("End ElasticSearch export for ${store} -> ${index}")
                    }
                }
            } as Function1, ec)
        }
    }


    private static void copyFile(File src, File dest) {
        if (src.isDirectory()) {
            if (!dest.exists()) {
                dest.mkdirs()
            }
            def files = src.list()
            files.each { String file ->
                //construct the src and dest file structure
                File srcFile = new File(src, file)
                File destFile = new File(dest, file)
                // copy file
                copyFile(srcFile, destFile)
            }
        } else {
            InputStream is = new FileInputStream(src)
            OutputStream os = new FileOutputStream(dest)
            byte[] buffer = new byte[1024]
            int len
            while ((len = is.read(buffer)) > 0) {
                os.write(buffer, 0, len)
            }
            is.close()
            os.close()
        }
    }


    /**
     * This method translates properties returned by the store to the specified language
     * @param lang - the lang to use for translation
     * @param result - the properties to be translated
     * @return the properties translated
     */
    private static Map translate(String lang, Map result) {
        if (lang) {
            result[lang]?.each { k, v ->
                result[k] = v
            }
            result.remove(lang)

            result.each { k, v ->
                if (v instanceof Map) {
                    translate(lang, v)
                } else if (v instanceof List<Map>) {
                    (v as List<Map>).each { m ->
                        translate(lang, m as Map)
                    }
                }
            }
        }
        result
    }

    /**
     * Format the given amount (in the Mogobiz unit) into the given currency by using
     * the number format of the given country
     * @param amount
     * @param currencyCode
     * @param locale
     * @return the given amount formated
     */
    private static String format(long amount, String currencyCode, Locale locale = Locale.default, double rate = 0) {
        NumberFormat numberFormat = NumberFormat.getCurrencyInstance(locale);
        numberFormat.setCurrency(Currency.getInstance(currencyCode));
        return numberFormat.format(amount * rate);
    }


    def publishAll() {
        def cal = Calendar.getInstance()
        cal.set(Calendar.SECOND, 0)
        cal.set(Calendar.MILLISECOND, 0)
        def now = cal.getTime()
        Company.findAll().each { Company company ->
            Collection<Catalog> catalogs = Catalog.findAllByActivationDateLessThanEqualsAndCompanyAndDeleted(
                    new Date(),
                    company,
                    false,
                    [sort:'activationDate', order:'desc'])
            Catalog catalog = catalogs.size() > 0 ? catalogs.get(0) : null
            if(catalog){
                EsEnv.findAllByCompany(company).each {env ->
                    def cron = env.cronExpr
                    if(CronExpression.isValidExpression(cron) && new CronExpression(cron).isSatisfiedBy(now)){
                        this.publish(company, env, catalog)
                    }
                }
            }
        }

    }
}
