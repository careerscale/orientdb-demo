package com.shaklee;

import com.orientechnologies.orient.core.record.OVertex;


public class Bonus {

    private Long id;
    private Double amount;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public static String BONUS_AMOUNT = "amount";

    public static Bonus toBonus(OVertex bonusVertex) {
        Bonus bonus = null;
        if (bonusVertex != null) {
            bonus = new Bonus();
            bonus.setAmount(bonusVertex.getProperty(BONUS_AMOUNT));
        }
        return bonus;
    }

}
