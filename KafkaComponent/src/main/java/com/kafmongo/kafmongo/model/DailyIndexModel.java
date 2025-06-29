package com.kafmongo.kafmongo.model;

import jakarta.persistence.*;

import java.time.LocalDate;

@Entity
@IdClass(DailyIndexId.class)
@Table(name = "DailyIndex")
public class DailyIndexModel {
    @Id
    @Column
    private String Symbol;

    @Id
    @Column(name = "Date")
    private LocalDate Date;

    @Column
    private String Index;

    @Column
    private float Value;

    public DailyIndexModel() {
    }

    public DailyIndexModel(String Symbol, LocalDate Date,String Index ,float Value) {
        this.Symbol = Symbol;
        this.Date = Date;
        this.Index = Index;
        this.Value = Value;
    }


    public String getSymbol() {
        return Symbol;
    }

    public void setSymbol(CharSequence Symbol) {
        this.Symbol = Symbol.toString();
    }

    public LocalDate getDate() {
        return Date;
    }

    public void setDate(CharSequence Date) {
        this.Date = parseDateSafe(Date, "1950-01-01");;
    }

    public String getIndex() {
        return Index;
    }

    public void setIndex(CharSequence Index) {
        this.Index = Index.toString();
    }

    public float getValue() {
        return Value;
    }

    public void setValue(CharSequence Value) {
        this.Value = new Float(Value.toString());
    }

    private LocalDate parseDateSafe(CharSequence value, String defaultDate) {
        try {
            return (value != null && !value.toString().equalsIgnoreCase("null"))
                    ? LocalDate.parse(value.toString())
                    : LocalDate.parse(defaultDate);
        } catch (Exception e) {
            return LocalDate.parse(defaultDate);
        }
    }




}