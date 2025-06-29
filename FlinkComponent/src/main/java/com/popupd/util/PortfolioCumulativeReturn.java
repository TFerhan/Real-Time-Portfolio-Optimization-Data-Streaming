package com.popupd.util;

public class PortfolioCumulativeReturn {

    private Double cumulativeReturn;
    private String timestamp;

    public PortfolioCumulativeReturn() {
        // Default constructor
    }

    public PortfolioCumulativeReturn(Double cumulativeReturn, String timestamp) {
        this.cumulativeReturn = cumulativeReturn;
        this.timestamp = timestamp;
    }

    public Double getCumulativeReturn() {
        return cumulativeReturn;
    }

    public void setCumulativeReturn(Double cumulativeReturn) {
        this.cumulativeReturn = cumulativeReturn;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "PortfolioCumulativeReturn{" +
                "cumulativeReturn=" + cumulativeReturn +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }


}
