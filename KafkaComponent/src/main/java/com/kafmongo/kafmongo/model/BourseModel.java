package com.kafmongo.kafmongo.model;

import org.springframework.data.annotation.Id;

import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.TimeSeries;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import org.springframework.data.mongodb.core.index.CompoundIndex;

//@CompoundIndex(name = "ticker_date", def = "{'ticker' : 1, 'fieldDateApplication' : 1}", unique = true)


@Document(collection = "DataBourseTime")
@TimeSeries(timeField = "fieldTransactTime", metaField = "ticker")

public class BourseModel {
	//@Id
	//private String id;

    

    @Field("fieldDateApplication")
    private Date fieldDateApplication;

    @Field("fieldClosingPrice")
    private Float fieldClosingPrice;

    @Field("fieldDifference")
    private Float fieldDifference;

    @Field("fieldVarVeille")
    private Float fieldVarVeille;

    @Field("fieldBestAskSize")
    private Float fieldBestAskSize;

    @Field("fieldTransactTime")
    private Date fieldTransactTime;

    @Field("fieldCoursCloture")
    private String fieldCoursCloture;

    @Field("fieldCoursAjuste")
    private Float fieldCoursAjuste;

    @Field("fieldCumulTitresEchanges")
    private Float fieldCumulTitresEchanges;

    @Field("fieldBestBidSize")
    private Float fieldBestBidSize;

    @Field("fieldCapitalisation")
    private Float fieldCapitalisation;

    @Field("fieldOpeningPrice")
    private Float fieldOpeningPrice;

    @Field("fieldTotalTrades")
    private Integer fieldTotalTrades;

    @Field("fieldCoursCourant")
    private Float fieldCoursCourant;

    @Field("fieldBestAskPrice")
    private Float fieldBestAskPrice;

    @Field("sousSecteur")
    private String sousSecteur;

    @Field("ticker")
    private String ticker;  

    @Field("fieldInstrumentVarYear")
    private String fieldInstrumentVarYear;

    @Field("fieldVarPto")
    private Float fieldVarPto;

    @Field("fieldLastTradedTime")
    private Date fieldLastTradedTime;

    @Field("label")
    private String label;

    @Field("fieldBestBidPrice")
    private Float fieldBestBidPrice;

    @Field("fieldDynamicReferencePrice")
    private Float fieldDynamicReferencePrice;

    @Field("fieldStaticReferencePrice")
    private Float fieldStaticReferencePrice;

    @Field("fieldLowPrice")
    private Float fieldLowPrice;

    @Field("fieldHighPrice")
    private Float fieldHighPrice;

    @Field("fieldCumulVolumeEchange")
    private Float fieldCumulVolumeEchange;

    @Field("fieldLastTradedPrice")
    private Float fieldLastTradedPrice;



    public void setFieldDateApplication(CharSequence charSequence) {
    	String dateString = charSequence.toString();

        // Define the date format for ISO 8601
        SimpleDateFormat isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        isoFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        Date date;
		try {
			date = isoFormat.parse(dateString);
			this.fieldDateApplication = date;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    public void setFieldClosingPrice(CharSequence charSequence) {
        this.fieldClosingPrice = new Float(charSequence.toString());
    }

    public void setFieldDifference(CharSequence charSequence) {
        this.fieldDifference = new Float(charSequence.toString());
    }

    public void setFieldVarVeille(CharSequence charSequence) {
        this.fieldVarVeille = new Float(charSequence.toString());
    }

    public void setFieldBestAskSize(CharSequence charSequence) {
        this.fieldBestAskSize = new Float(charSequence.toString());
    }

    public void setFieldTransactTime(CharSequence charSequence) {
    	
    	String dateString = charSequence.toString();

        // Define the date format for ISO 8601
        SimpleDateFormat isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        isoFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        Date date;
		try {
			date = isoFormat.parse(dateString);
			this.fieldTransactTime = date;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
    	
    }

    public void setFieldCoursCloture(CharSequence charSequence) {
        this.fieldCoursCloture = charSequence.toString();
    }

    public void setFieldCoursAjuste(CharSequence charSequence) {
        this.fieldCoursAjuste = new Float(charSequence.toString());
    }

    public void setFieldCumulTitresEchanges(CharSequence charSequence) {
        this.fieldCumulTitresEchanges = new Float(charSequence.toString());
    }

    public void setFieldBestBidSize(CharSequence charSequence) {
        this.fieldBestBidSize = new Float(charSequence.toString());
    }

    public void setFieldCapitalisation(CharSequence charSequence) {
        this.fieldCapitalisation = new Float(charSequence.toString());
    }

    public void setFieldOpeningPrice(CharSequence charSequence) {
        this.fieldOpeningPrice = new Float(charSequence.toString());
    }

    @SuppressWarnings("removal")
	public void setFieldTotalTrades(CharSequence charSequence) {
        this.fieldTotalTrades = new Integer(charSequence.toString());
    }

    public void setFieldCoursCourant(CharSequence charSequence) {
        this.fieldCoursCourant = new Float( charSequence.toString());
    }

    public void setFieldBestAskPrice(CharSequence charSequence) {
        this.fieldBestAskPrice = new Float(charSequence.toString());
    }

    public void setSousSecteur(CharSequence charSequence) {
        this.sousSecteur =  charSequence.toString();
    }

    public void setTicker(CharSequence charSequence) {
        this.ticker = charSequence.toString();
    }

    public void setFieldInstrumentVarYear(CharSequence charSequence) {
        this.fieldInstrumentVarYear = charSequence.toString();
    }

    public void setFieldVarPto(CharSequence charSequence) {
        this.fieldVarPto = new Float(charSequence.toString());
    }

    public void setFieldLastTradedTime(CharSequence charSequence) {
    	
    	String dateString = charSequence.toString();

        // Define the date format for ISO 8601
        SimpleDateFormat isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        isoFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        Date date;
		try {
			date = isoFormat.parse(dateString);
			this.fieldLastTradedTime = date;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    public void setLabel(CharSequence charSequence) {
        this.label = charSequence.toString();
    }

    public void setFieldBestBidPrice(CharSequence charSequence) {
        this.fieldBestBidPrice = new Float(charSequence.toString());
    }

    public void setFieldDynamicReferencePrice(CharSequence charSequence) {
        this.fieldDynamicReferencePrice = new Float(charSequence.toString());
    }

    public void setFieldStaticReferencePrice(CharSequence charSequence) {
        this.fieldStaticReferencePrice = new Float(charSequence.toString());
    }

    public void setFieldLowPrice(CharSequence charSequence) {
        this.fieldLowPrice = new Float(charSequence.toString());
    }

    public void setFieldHighPrice(CharSequence charSequence) {
        this.fieldHighPrice = new Float(charSequence.toString());
    }

    public void setFieldCumulVolumeEchange(CharSequence charSequence) {
        this.fieldCumulVolumeEchange = new Float(charSequence.toString());
    }

    public void setFieldLastTradedPrice(CharSequence charSequence) {
        this.fieldLastTradedPrice = new Float(charSequence.toString());
    }

    // Getters
    public Date getFieldDateApplication() {
        return fieldDateApplication;
    }

    public Float getFieldClosingPrice() {
        return fieldClosingPrice;
    }

    public Float getFieldDifference() {
        return fieldDifference;
    }

    public Float getFieldVarVeille() {
        return fieldVarVeille;
    }

    public Float getFieldBestAskSize() {
        return fieldBestAskSize;
    }

    public Date getFieldTransactTime() {
        return fieldTransactTime;
    }

    public String getFieldCoursCloture() {
        return fieldCoursCloture;
    }

    public Float getFieldCoursAjuste() {
        return fieldCoursAjuste;
    }

    public Float getFieldCumulTitresEchanges() {
        return fieldCumulTitresEchanges;
    }

    public Float getFieldBestBidSize() {
        return fieldBestBidSize;
    }

    public Float getFieldCapitalisation() {
        return fieldCapitalisation;
    }

    public Float getFieldOpeningPrice() {
        return fieldOpeningPrice;
    }

    public Integer getFieldTotalTrades() {
        return fieldTotalTrades;
    }

    public Float getFieldCoursCourant() {
        return fieldCoursCourant;
    }

    public Float getFieldBestAskPrice() {
        return fieldBestAskPrice;
    }

    public String getSousSecteur() {
        return sousSecteur;
    }

    public String getTicker() {
        return ticker;
    }

    public String getFieldInstrumentVarYear() {
        return fieldInstrumentVarYear;
    }

    public Float getFieldVarPto() {
        return fieldVarPto;
    }

    public Date getFieldLastTradedTime() {
        return fieldLastTradedTime;
    }

    public String getLabel() {
        return label;
    }

    public Float getFieldBestBidPrice() {
        return fieldBestBidPrice;
    }

    public Float getFieldDynamicReferencePrice() {
        return fieldDynamicReferencePrice;
    }

    public Float getFieldStaticReferencePrice() {
        return fieldStaticReferencePrice;
    }

    public Float getFieldLowPrice() {
        return fieldLowPrice;
    }

    public Float getFieldHighPrice() {
        return fieldHighPrice;
    }

    public Float getFieldCumulVolumeEchange() {
        return fieldCumulVolumeEchange;
    }

    public Float getFieldLastTradedPrice() {
        return fieldLastTradedPrice;
    }
	
	@Override
	public String toString() {
		return "BourseModel [fieldDateApplication=" + fieldDateApplication + ", fieldClosingPrice="
				+ fieldClosingPrice + ", fieldDifference=" + fieldDifference + ", fieldVarVeille=" + fieldVarVeille
				+ ", fieldBestAskSize=" + fieldBestAskSize + ", fieldTransactTime=" + fieldTransactTime
				+ ", fieldCoursCloture=" + fieldCoursCloture + ", fieldCoursAjuste=" + fieldCoursAjuste
				+ ", fieldCumulTitresEchanges=" + fieldCumulTitresEchanges + ", fieldBestBidSize=" + fieldBestBidSize
				+ ", fieldCapitalisation=" + fieldCapitalisation + ", fieldOpeningPrice=" + fieldOpeningPrice
				+ ", fieldTotalTrades=" + fieldTotalTrades + ", fieldCoursCourant=" + fieldCoursCourant
				+ ", fieldBestAskPrice=" + fieldBestAskPrice + ", sousSecteur=" + sousSecteur + ", ticker=" + ticker
				+ ", fieldInstrumentVarYear=" + fieldInstrumentVarYear + ", fieldVarPto=" + fieldVarPto
				+ ", fieldLastTradedTime=" + fieldLastTradedTime + ", label=" + label + ", fieldBestBidPrice="
				+ fieldBestBidPrice + ", fieldDynamicReferencePrice=" + fieldDynamicReferencePrice
				+ ", fieldStaticReferencePrice=" + fieldStaticReferencePrice + ", fieldLowPrice=" + fieldLowPrice
				+ ", fieldHighPrice=" + fieldHighPrice + ", fieldCumulVolumeEchange=" + fieldCumulVolumeEchange
				+ ", fieldLastTradedPrice=" + fieldLastTradedPrice + "]";
	}
}
