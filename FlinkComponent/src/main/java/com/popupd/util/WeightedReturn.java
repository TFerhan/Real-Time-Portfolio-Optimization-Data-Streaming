package com.popupd.util;

import java.time.Instant;

public class WeightedReturn {

    private String ticker;
    private double weightedReturn;
    private String timestamp;

    public WeightedReturn() {}

    public WeightedReturn(String ticker, double weightedReturn, String timestamp) {
        this.ticker = ticker;
        this.weightedReturn = weightedReturn;
        this.timestamp = timestamp;
    }

    public String getTicker() {
        return ticker;
    }

    public void setTicker(String ticker) {
        this.ticker = ticker;
    }

    public double getWeightedReturn() {
        return weightedReturn;
    }

    public void setWeightedReturn(double weightedReturn) {
        this.weightedReturn = weightedReturn;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "WeightedReturn{" +
                "ticker='" + ticker + '\'' +
                ", weightedReturn=" + weightedReturn +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}
