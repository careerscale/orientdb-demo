package com.shaklee;

import com.orientechnologies.orient.core.record.OVertex;


public class Address {

    private Long id;
    private String street;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }


    // Employee Constants
    public static final String ADDRESS_ID = "id";
    public static final String ADDRESS = "Company";
    public static final String ADDRESS_STREET = "street";
    public static final String ADDRESS_COUNTRY = "country";

    public static String BONUS_AMOUNT = "amount";

    public static Address toAddress(OVertex addressVertex) {
        Address address = null;
        if (addressVertex != null) {
            address = new Address();
            address.setStreet(addressVertex.getProperty(ADDRESS_STREET));
        }
        return address;
    }



}
