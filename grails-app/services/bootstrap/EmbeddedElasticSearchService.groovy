/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package bootstrap

import grails.converters.JSON
import grails.util.Holders
import org.elasticsearch.common.io.FileSystemUtils
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.env.Environment
import org.elasticsearch.node.internal.InternalSettingsPreparer
import org.elasticsearch.plugins.PluginManager

import java.text.SimpleDateFormat

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder
import static org.elasticsearch.node.NodeBuilder.nodeBuilder

public class EmbeddedElasticSearchService {
    private static org.elasticsearch.node.Node node = null
    def destroy() {
        node?.close()
    }

	def init() {
        def embedded = Holders.config.elasticsearch?.embedded
        if (embedded?.active) {
            org.elasticsearch.common.collect.Tuple<Settings, Environment> initialSettings = InternalSettingsPreparer.prepareSettings(EMPTY_SETTINGS, true);
            if (!initialSettings.v2().pluginsFile().exists()) {
                FileSystemUtils.mkdirs(initialSettings.v2().pluginsFile());
            }
            String headPlugin = "mobz/elasticsearch-head"
            String url = null;
            PluginManager pluginManager = new PluginManager(initialSettings.v2(),url, PluginManager.OutputMode.VERBOSE, TimeValue.timeValueMillis(0));
            pluginManager.removePlugin(headPlugin)
            try {
                pluginManager.downloadAndExtract(headPlugin);
            }
            catch(IOException e) {
                // ignore if exists
                e.printStackTrace();
            }
            ImmutableSettings.Builder settings = settingsBuilder()
            def embeddedSettings = embedded.settings as Map
            String path
            embeddedSettings?.each { k, v ->
                k = ((String) k).replace('_', '.')
                settings.put(k, v)
                if (k == 'path.data') {
                    path = v
                }
                log.info(k + '->' + v)
            }
            if (grails.util.Environment.getCurrent() == grails.util.Environment.DEVELOPMENT && path) {
                new File(path).deleteDir()
            }
            node = nodeBuilder().local(true).settings(settings).node()
            node.start()
        }

        JSON.registerObjectMarshaller(Date) {
            return it?.format("dd/MM/yyyy HH:mm")
        }
        JSON.registerObjectMarshaller(Calendar) { Calendar c ->
            if (c == null) {
                return null;
            } else {
                SimpleDateFormat f = new SimpleDateFormat("dd/MM/yyyy HH:mm");
                return f.format(c.getTime());
            }
        }
    }
}
