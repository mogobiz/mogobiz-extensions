package com.mogobiz.store

import com.mogobiz.constant.IperConstant
import grails.validation.Validateable

/**
 * Created by stephane.manciot@ebiznext.com on 13/03/2014.
 */
@Validateable
class Pagination implements Serializable{

    Integer maxItemsPerPage = IperConstant.NUMBER_PRODUCT_PER_PAGE

    Integer pageOffset = 0

    Integer getOffset() {
        pageOffset * maxItemsPerPage
    }

    String orderBy

    OrderDirection orderDirection = OrderDirection.DESC
}

enum OrderDirection {
    ASC, DESC
}
