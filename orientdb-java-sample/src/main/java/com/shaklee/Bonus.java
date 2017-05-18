package com.shaklee;

import java.util.Map;

public class Bonus {

    private Long id;
    private String bonusType;
    private Map<String, Double> volumes;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getBonusType() {
        return bonusType;
    }

    public void setBonusType(String bonusType) {
        this.bonusType = bonusType;
    }

    public Map<String, Double> getVolumes() {
        return volumes;
    }

    public void setVolumes(Map<String, Double> volumes) {
        this.volumes = volumes;
    }



}
