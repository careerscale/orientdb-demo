package com.shaklee;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Employee {

    private Long id;
    private String name;
    private String type;
    private List<Address> address;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<Address> getAddress() {
        return address;
    }

    public void setAddress(List<Address> address) {
        this.address = address;
    }


    // Employee Constants
    public static final String EMPLOYEE_ID = "id";
    public static final String EMPLOYEE = "Company";
    public static final String EMPLOYEE_NAME = "name";
    public static final String EMPLOYEE_TYPE = "type";
    public static final String EMPLOYEE_ADDRESS_RELATION = "LIVES_IN";
    public static final String EMPLOYEE_BONUS_RELATION = "HAS_BONUS";


    public static Map<String, Object> buildEmployeeVertex(Employee employee) {
        Map<String, Object> props = new HashMap<String, Object>();

        // props.put(COMPANY_ID, company.getId());
        props.put(EMPLOYEE_NAME, employee.getName());
        props.put(EMPLOYEE_TYPE, employee.getType());
        return props;

    }


}
