package com.mogobiz.service

import akka.actor.ActorSystem
import akka.dispatch.Futures
import com.mogobiz.google.client.GoogleCategoryItem
import com.mogobiz.google.client.GoogleCategoryReader
import com.mogobiz.store.domain.GoogleCategory
import rx.functions.Action0
import rx.functions.Action1
import rx.functions.Func1
import scala.Function1
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import java.util.concurrent.Callable

class GoogleCategoryService {

    static transactional = false

    private static final ActorSystem GOOGLE_SYSTEM = ActorSystem.create("GOOGLE")

    def importGoogleCategories(
            final List<Locale> locales = [
                    Locale.GERMANY,
                    new Locale('es', 'ES'),
                    Locale.FRANCE,
                    Locale.ITALY,
                    Locale.US
            ],
            final int count = 100) {

        ExecutionContext ec = GOOGLE_SYSTEM.dispatcher()

        long expected = 0
        long total = 0

        Map<String, Set<String>> categories = [:]
        ['de', 'en', 'es', 'fr', 'it'].each {lang ->
            categories.put(
                    lang,
                    GoogleCategory.findAllByLang(lang).collect {
                        it.path
                    }
            )
        }

        def observables = []
        locales.each {Locale locale ->
            observables.addAll(GoogleCategoryReader.parseCategories(locale))
        }

        rx.Observable.merge(observables)./**bulk insert**/buffer(count).flatMap({items ->
            rx.Observable.from(Futures.future(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    int ret = 0
                    GoogleCategory.withTransaction {
                        items.each {GoogleCategoryItem item ->
                            final language = item.locale?.getLanguage()
                            String[] tokens = item.tokens
                            String path = tokens.join(' > ').trim()
                            if(!categories[language].contains(path)) {
                                expected++
                                def name = tokens[tokens.size()-1].trim()
                                def parentPath = (tokens-name).join(' > ').trim()
                                try{
                                    GoogleCategory googleCategory = new GoogleCategory(
                                            name:name,
                                            path:path,
                                            parentPath: parentPath,
                                            lang:language
                                    )
                                    googleCategory.validate()
                                    if(googleCategory.hasErrors()){
                                        googleCategory.errors.allErrors.each {
                                            log.error(it)
                                        }
                                    }
                                    else{
                                        googleCategory.save()
                                        ++ret
                                    }
                                }
                                catch(Throwable th){
                                    log.error(th.message, th)
                                }
                            }
                        }
                    }
                    ret
                }
            }, ec))
        }as Func1<List<GoogleCategoryItem>, rx.Observable<Future<Integer>>>).subscribe(
                { Future<Integer> future ->
                    future.onComplete({scala.util.Try<Integer> t ->
                        total += t.get()
                    } as Function1, ec)
                } as Action1<Future<Integer>>,
                {
                    Throwable th -> log.error(th.message)
                } as Action1<Throwable>,
                {
                    log.info('recorded ' + total + '/' + expected + ' lines')
                } as Action0
        )
    }

}
