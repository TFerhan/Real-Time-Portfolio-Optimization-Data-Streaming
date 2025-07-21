package com.kafmongo.kafmongo.utils;

import java.time.LocalDateTime;
import java.util.Date;

public class DailyMetrics {

    // Common field
    private String ticker;
    
    /**
     * This field can store the date value from different queries.
     * For example:
     * - Weekly returns: week_start
     * - Monthly returns: month_start
     * - Illiquidity & Daily returns: created
     */
    private Date date;
    
    // For weekly returns
    private Double weeklyLogReturn;
    private Double stdWeekly;
    private Double avgWeekly;
    
    // For monthly returns
    private Double monthlyLogReturn;
    private Double stdMonthly;
    private Double avgMonthly;
    
    // For daily returns
    private Double dreturns;
    private Double avgReturns;
    private Double stdReturns;
    
    // For illiquidity ratio
    private Double illiquidityRatio;

    // --- Constructors ---
    
    public DailyMetrics() {
    }
    
    
    public DailyMetrics(String ticker, Date date, Double dreturns) {
        this.ticker = ticker;
        this.date = date;
        this.dreturns = dreturns;
        this.monthlyLogReturn = dreturns;
        this.weeklyLogReturn = dreturns;
     
    }


    // You might want to add additional constructors depending on your needs

    // --- Getters and Setters ---

    public String getTicker() {
        return ticker;
    }

    public void setTicker(String ticker) {
        this.ticker = ticker;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Double getWeeklyLogReturn() {
        return weeklyLogReturn;
    }

    public void setWeeklyLogReturn(Double weeklyLogReturn) {
        this.weeklyLogReturn = weeklyLogReturn;
    }

    public Double getStdWeekly() {
        return stdWeekly;
    }

    public void setStdWeekly(Double stdWeekly) {
        this.stdWeekly = stdWeekly;
    }

    public Double getAvgWeekly() {
        return avgWeekly;
    }

    public void setAvgWeekly(Double avgWeekly) {
        this.avgWeekly = avgWeekly;
    }

    public Double getMonthlyLogReturn() {
        return monthlyLogReturn;
    }

    public void setMonthlyLogReturn(Double monthlyLogReturn) {
        this.monthlyLogReturn = monthlyLogReturn;
    }

    public Double getStdMonthly() {
        return stdMonthly;
    }

    public void setStdMonthly(Double stdMonthly) {
        this.stdMonthly = stdMonthly;
    }

    public Double getAvgMonthly() {
        return avgMonthly;
    }

    public void setAvgMonthly(Double avgMonthly) {
        this.avgMonthly = avgMonthly;
    }

    public Double getDailyReturns() {
        return dreturns;
    }

    public void setDailyReturns(Double dailyReturns) {
        this.dreturns = dailyReturns;
    }

    public Double getAvgReturns() {
        return avgReturns;
    }

    public void setAvgReturns(Double avgReturns) {
        this.avgReturns = avgReturns;
    }

    public Double getStdReturns() {
        return stdReturns;
    }

    public void setStdReturns(Double stdReturns) {
        this.stdReturns = stdReturns;
    }

    public Double getIlliquidityRatio() {
        return illiquidityRatio;
    }

    public void setIlliquidityRatio(Double illiquidityRatio) {
        this.illiquidityRatio = illiquidityRatio;
    }

    // Optionally, override toString() for debugging purposes.
    @Override
    public String toString() {
        return "DailyMetrics{" +
                "ticker='" + ticker + '\'' +
                ", date=" + date +
                ", weeklyLogReturn=" + weeklyLogReturn +
                ", stdWeekly=" + stdWeekly +
                ", avgWeekly=" + avgWeekly +
                ", monthlyLogReturn=" + monthlyLogReturn +
                ", stdMonthly=" + stdMonthly +
                ", avgMonthly=" + avgMonthly +
                ", dailyReturns=" + dreturns +
                ", avgReturns=" + avgReturns +
                ", stdReturns=" + stdReturns +
                ", illiquidityRatio=" + illiquidityRatio +
                '}';
    }
}
