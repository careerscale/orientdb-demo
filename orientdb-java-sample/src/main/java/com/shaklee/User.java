package com.shaklee;

import java.util.List;

import com.orientechnologies.orient.core.record.OVertex;

public class User {
    private Long id;
    private String name;

    List<Address> addresses;
    List<Bonus> bonuses;

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

    public List<Address> getAddresses() {
        return addresses;
    }

    public void setAddresses(List<Address> addresses) {
        this.addresses = addresses;
    }

    public List<Bonus> getBonuses() {
        return bonuses;
    }

    public void setBonuses(List<Bonus> bonuses) {
        this.bonuses = bonuses;
    }

    public static String USER_NAME = "name";
    public static String LIVES_IN = "LIVES_IN";
    public static String HAS = "HAS";

    public static User toUser(OVertex userVertex) {
        User user = null;
        if (userVertex != null) {
            user = new User();
            user.setName(userVertex.getProperty(USER_NAME));
        }
        return user;
    }

}
