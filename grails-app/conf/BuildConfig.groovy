mogobiz.version="${appVersion}"
grails.project.class.dir = "target/classes"
grails.project.test.class.dir = "target/test-classes"
grails.project.test.reports.dir = "target/test-reports"
//grails.plugin.location."mogobiz-core" = "../mogobiz-core"

grails.project.fork = [
    // configure settings for compilation JVM, note that if you alter the Groovy version forked compilation is required
    //  compile: [maxMemory: 256, minMemory: 64, debug: false, maxPerm: 256, daemon:true],

    // configure settings for the test-app JVM, uses the daemon by default
    test: [maxMemory: 768, minMemory: 64, debug: false, maxPerm: 256, daemon:true],
    // configure settings for the run-app JVM
    run: [maxMemory: 768, minMemory: 64, debug: false, maxPerm: 256, forkReserve:false],
    // configure settings for the run-war JVM
    war: [maxMemory: 768, minMemory: 64, debug: false, maxPerm: 256, forkReserve:false],
    // configure settings for the Console UI JVM
    console: [maxMemory: 768, minMemory: 64, debug: false, maxPerm: 256]
]

grails.project.dependency.resolver = "maven" // or ivy
grails.project.dependency.resolution = {
    // inherit Grails' default dependencies
    inherits("global") {
        // uncomment to disable ehcache
        excludes 'ehcache'
    }
    log "warn" // log level of Ivy resolver, either 'error', 'warn', 'info', 'debug' or 'verbose'
    repositories {
        grailsCentral()
        mavenLocal()
        mavenCentral()
        // uncomment the below to enable remote dependency resolution
        // from public Maven repositories
        //mavenRepo "http://repository.codehaus.org"
        //mavenRepo "http://download.java.net/maven/2/"
        //mavenRepo "http://repository.jboss.com/maven2/"
    }
    dependencies {
        // specify dependencies here under either 'build', 'compile', 'runtime', 'test' or 'provided' scopes eg.
        // runtime 'mysql:mysql-connector-java:5.1.27'
        compile 'org.elasticsearch:elasticsearch:1.7.3'
        compile ('org.elasticsearch:elasticsearch-analysis-icu:2.7.0') {
            excludes 'org.elasticsearch:elasticsearch'
        }

        compile (group:"com.mogobiz", name:"mogobiz-tools", version:"${mogobiz.version}")  {excludes "groovy-all"}

        compile "com.mogobiz.rivers:mogobiz-google-shopping:${mogobiz.version}"
        compile "com.mogobiz.rivers:mogobiz-elasticsearch:${mogobiz.version}"
        compile "com.mogobiz.rivers:mogobiz-cfp:${mogobiz.version}"
        compile "com.mogobiz.rivers:mogobiz-mirakl:${mogobiz.version}"
        compile "commons-beanutils:commons-beanutils:1.8.3"
        compile "org.twitter4j:twitter4j-async:2.2.5"
        compile "org.twitter4j:twitter4j-core:2.2.5"
        compile "org.twitter4j:twitter4j-media-support:2.2.5"
        compile "org.twitter4j:twitter4j-stream:2.2.5"
        compile "com.restfb:restfb:1.6.7"
        compile "org.apache.oltu.oauth2:org.apache.oltu.oauth2.common:0.31"
        compile "org.apache.oltu.oauth2:org.apache.oltu.oauth2.authzserver:0.31"
        compile "org.apache.oltu.oauth2:org.apache.oltu.oauth2.resourceserver:0.31"
        compile "org.apache.poi:poi-ooxml:3.10.1"

    }

    plugins {
        build(":release:3.1.2",
              ":rest-client-builder:2.1.1",
                ":hibernate:3.6.10.19",
                ":shiro:1.2.1",
                ":google-data:0.1.3",
                ":facebook-graph:0.14") {//"com.mogobiz:email-confirmation:${mogobiz.version}"
            export = false
        }
        compile ":mail:1.0.7"
        compile ":platform-core:1.0.0"
        compile "com.mogobiz:mogobiz-core:${mogobiz.version}"

    }
}
