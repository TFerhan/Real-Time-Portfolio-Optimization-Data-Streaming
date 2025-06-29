package com.kafmongo.kafmongo.model;
import java.io.Serializable;
import java.time.LocalDate;

public class DailyPriceId implements Serializable {
    private String ticker;
    private LocalDate created;

    // Constructeurs, getters, setters et equals/hashCode

    public DailyPriceId() {
    }

    public DailyPriceId(String ticker, LocalDate created) {
        this.ticker = ticker;
        this.created = created;
    }
}
