package com.popupd.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.TimeZone;

public class StockReturn {
    private String ticker;
    private double returnValue;
    private String timestamp;

    private String portfolioId;


    public StockReturn() {}

    public StockReturn(String ticker, double returnValue, String timestamp, String portfolioId) {
        this.ticker = ticker;
        this.returnValue = returnValue;
        this.timestamp = timestamp;
        this.portfolioId = portfolioId;
    }

    public String getTicker() {
        return ticker;
    }

    public String getPortfolioId() {
        return portfolioId;
    }

    public void setPortfolioId(String portfolioId) {
        this.portfolioId = portfolioId;
    }

    public void setTicker(String ticker) {
        this.ticker = ticker;
    }

    public double getReturnValue() {
        return returnValue;
    }



    public void setReturnValue(double returnValue) {
        this.returnValue = returnValue;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String charSequence) {
        this.timestamp = charSequence;
    }



    @Override
    public String toString() {
        return "StockReturn{" +
                "ticker='" + ticker + '\'' +
                ", returnValue=" + returnValue +
                ", timestamp=" + timestamp +

                '}';
    }
}
