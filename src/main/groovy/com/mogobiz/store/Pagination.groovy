/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.store

import com.mogobiz.constant.IperConstant
import grails.validation.Validateable

/**
 */
class Pagination implements Serializable, Validateable{

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
