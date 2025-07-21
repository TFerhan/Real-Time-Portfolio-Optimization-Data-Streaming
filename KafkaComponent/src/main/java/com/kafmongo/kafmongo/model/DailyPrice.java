package com.kafmongo.kafmongo.model;

import java.time.LocalDate;
import java.lang.annotation.Target;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.io.Serializable;
import java.time.LocalDateTime;

import lombok.*;
import jakarta.persistence.*;





import java.time.LocalDateTime;

@Entity
@IdClass(DailyPriceId.class)
@Table(name = "DailyPrice")
public class DailyPrice {
    @Id
    @Column(name="ticker")
    private String ticker;

    @Id
    @Column(name = "created", nullable = false)
    private LocalDate created;

    @Column(name = "ratioConsolide", nullable = true)
    private float ratioConsolide;

    @Column(name = "totalTrades", nullable = true)
    private int totalTrades;

    @Column(name = "coursCourant", nullable = true)
    private float coursCourant;

    
    @Column(name = "libelleFR", nullable = true)
    private String libelleFR;

    @Column(name = "openingPrice", nullable = true)
    private float openingPrice;

    @Column(name = "lowPrice", nullable = true)
    private float lowPrice;

    @Column(name = "coursAjuste", nullable = true)
    private float coursAjuste;

    @Column(name = "highPrice", nullable = true)
    private float highPrice;

    @Column(name = "closingPrice", nullable = true)
    private float closingPrice;

    @Column(name = "cumulTitresEchanges", nullable = true)
    private float cumulTitresEchanges;

    @Column(name = "capitalisation", nullable = true)
    private float capitalisation;

    @Column(name = "cumulVolumeEchange", nullable = true)
    private float cumulVolumeEchange;

    // Constructor
    public DailyPrice() {
        // Initialize default values
        this.ratioConsolide = 0.0f;
        this.totalTrades = 0;
        this.coursCourant = 0.0f;
        this.openingPrice = 0.0f;
        this.lowPrice = 0.0f;
        this.coursAjuste = 0.0f;
        this.highPrice = 0.0f;
        this.closingPrice = 0.0f;
        this.cumulTitresEchanges = 0.0f;
        this.capitalisation = 0.0f;
        this.cumulVolumeEchange = 0.0f;
    }

 // Setters
    public void setRatioConsolide(CharSequence ratioConsolide) {
        this.ratioConsolide = parseFloatSafe(ratioConsolide, 0.0f);
    }

    public void setCreated(CharSequence created) {
        this.created = parseDateSafe(created, "1950-01-01");
    }

    public void setTotalTrades(CharSequence totalTrades) {
        this.totalTrades = parseIntSafe(totalTrades, 0);
    }

    public void setCoursCourant(CharSequence coursCourant) {
        this.coursCourant = parseFloatSafe(coursCourant, 0.0f);
    }

    public void setLibelleFR(CharSequence libelleFR) {
        this.libelleFR = (libelleFR != null) ? libelleFR.toString() : "";
    }
    
    public void setTicker(CharSequence ticker) {
        this.ticker = (ticker != null) ? ticker.toString() : "";
    }

    public void setOpeningPrice(CharSequence openingPrice) {
        this.openingPrice = parseFloatSafe(openingPrice, 0.0f);
    }

    public void setLowPrice(CharSequence lowPrice) {
        this.lowPrice = parseFloatSafe(lowPrice, 0.0f);
    }

    public void setCoursAjuste(CharSequence coursAjuste) {
        this.coursAjuste = parseFloatSafe(coursAjuste, 0.0f);
    }

    public void setHighPrice(CharSequence highPrice) {
        this.highPrice = parseFloatSafe(highPrice, 0.0f);
    }

    public void setClosingPrice(CharSequence closingPrice) {
        this.closingPrice = parseFloatSafe(closingPrice, 0.0f);
    }

    public void setCumulTitresEchanges(CharSequence cumulTitresEchanges) {
        this.cumulTitresEchanges = parseFloatSafe(cumulTitresEchanges, 0.0f);
    }

    public void setCapitalisation(CharSequence capitalisation) {
        this.capitalisation = parseFloatSafe(capitalisation, 0.0f);
    }

    public void setCumulVolumeEchange(CharSequence cumulVolumeEchange) {
        this.cumulVolumeEchange = parseFloatSafe(cumulVolumeEchange, 0.0f);
    }

    // Méthodes utilitaires pour éviter les erreurs de conversion
    private float parseFloatSafe(CharSequence value, float defaultValue) {
        try {
            return (value != null && !value.toString().equalsIgnoreCase("null")) 
                ? Float.parseFloat(value.toString()) 
                : defaultValue;
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private int parseIntSafe(CharSequence value, int defaultValue) {
        try {
            return (value != null && !value.toString().equalsIgnoreCase("null")) 
                ? Integer.parseInt(value.toString()) 
                : defaultValue;
        } catch (NumberFormatException e) {
            return defaultValue;
        }
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

    // Helper methods
    

    // Getters
    public float getRatioConsolide() {
        return ratioConsolide;
    }

    public LocalDate getCreated() {
        return created;
    }

    public int getTotalTrades() {
        return totalTrades;
    }

    public float getCoursCourant() {
        return coursCourant;
    }

    public String getLibelleFR() {
        return libelleFR;
    }

    public float getOpeningPrice() {
        return openingPrice;
    }

    public float getLowPrice() {
        return lowPrice;
    }

    public float getCoursAjuste() {
        return coursAjuste;
    }

    public float getHighPrice() {
        return highPrice;
    }

    public float getClosingPrice() {
        return closingPrice;
    }

    public float getCumulTitresEchanges() {
        return cumulTitresEchanges;
    }

    public float getCapitalisation() {
        return capitalisation;
    }

    public float getCumulVolumeEchange() {
        return cumulVolumeEchange;
    }
    
	public String getTicker() {
		return ticker;
	}

    @Override
    public String toString() {
        return "DailyPrice{" +
                ", created=" + created +
                ", ratioConsolide=" + ratioConsolide +
                ", totalTrades=" + totalTrades +
                ", coursCourant=" + coursCourant +
                ", libelleFR='" + libelleFR + '\'' +
                ", openingPrice=" + openingPrice +
                ", lowPrice=" + lowPrice +
                ", coursAjuste=" + coursAjuste +
                ", highPrice=" + highPrice +
                ", closingPrice=" + closingPrice +
                ", cumulTitresEchanges=" + cumulTitresEchanges +
                ", capitalisation=" + capitalisation +
                ", cumulVolumeEchange=" + cumulVolumeEchange +
                '}';
    }
}