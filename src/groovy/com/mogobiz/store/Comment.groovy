package com.mogobiz.store

import grails.validation.Validateable

/**
 * Created by stephane.manciot@ebiznext.com on 29/04/2014.
 */
@Validateable
class Comment implements Serializable{

    /**
     * user uuid
     */
    String userUuid

    /**
     * user nickname
     */
    String nickname

    /**
     * notation
     */
    int notation = 0

    /**
     * subject
     */
    String subject

    /**
     * comment
     */
    String comment

    /**
     * product uuid
     */
    String productUuid

    static constraints = {
        userUuid (blank:false, nullable:false)
        nickname (nullable:true)
        notation (range:0..5)
        subject (nullable:true)
        comment (nullable:true)
        productUuid (nullable:false)
    }

    String getId() {productUuid + userUuid}
}
