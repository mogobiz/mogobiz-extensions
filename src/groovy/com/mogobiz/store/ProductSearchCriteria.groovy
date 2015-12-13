/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.store

import grails.validation.Validateable

/**
 */
@Validateable
class ProductSearchCriteria extends Pagination implements Serializable{

    /**
     * product type
     * @see com.mogobiz.store.domain.ProductType
     */
    String xtype

    String name

    String code

    Long categoryId

    String categoryPath

    Long brandId

    String tagName

    Long priceMin

    Long priceMax

    String creationDateMin

    String orderBy = 'startDate'

    String country

    String state

    String currencyCode

    boolean featured = false

}
