package com.mogobiz.store

import grails.validation.Validateable

/**
 * Created by stephane.manciot@ebiznext.com on 15/03/2014.
 */
@Validateable
class HistorySearchCriteria extends Pagination implements Serializable{

    String orderBy = 'startDate'

    String country

    String state

    String currencyCode

}
