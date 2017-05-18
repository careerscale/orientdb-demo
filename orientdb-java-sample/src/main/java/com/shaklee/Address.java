package com.shaklee;

import java.util.HashMap;
import java.util.Map;

public class Address {

    private Long id;
    private String street;
    private String zip;
    private String country;

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

    public String getZip() {
        return zip;
    }

    public void setZip(String zip) {
        this.zip = zip;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }


    // Employee Constants
    public static final String ADDRESS_ID = "id";
    public static final String ADDRESS = "Company";
    public static final String ADDRESS_STREET = "street";
    public static final String ADDRESS_COUNTRY = "country";



    public static Map<String, Object> buildEmployeeVertex(Address address) {
        Map<String, Object> props = new HashMap<String, Object>();

        // props.put(COMPANY_ID, company.getId());
        props.put(ADDRESS_STREET, address.getStreet());
        props.put(ADDRESS_COUNTRY, address.getStreet());
        return props;

    }

}
