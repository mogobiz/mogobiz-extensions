package com.mogobiz.store

import grails.validation.Validateable

/**
 * Created by stephane.manciot@ebiznext.com on 01/05/2014.
 */
@Validateable
class CommentSearchCriteria extends Pagination implements Serializable{

    String productUuid
    String userUuid
    boolean aggregation = false

    static constraints = {
        productUuid (nullable: false)
        userUuid (nullable: true)
    }
}
