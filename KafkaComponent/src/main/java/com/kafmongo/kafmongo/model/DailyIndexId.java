package com.kafmongo.kafmongo.model;

import java.time.LocalDate;

public class DailyIndexId {

    private String Symbol;
    private LocalDate Date;

    public DailyIndexId() {
    }

    public DailyIndexId(String Symbol, LocalDate Date) {
        this.Symbol = Symbol;
        this.Date = Date;
    }

    public String getSymbol() {
        return Symbol;
    }

    public void setSymbol(String Symbol) {
        this.Symbol = Symbol;
    }

    public LocalDate getDate() {
        return Date;
    }

    public void setDate(LocalDate Date) {
        this.Date = Date;
    }
}