package com.kafmongo.kafmongo.model;


import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.TimeSeries;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@Document(collection = "DataIndexRT")
@TimeSeries(timeField = "fieldTransactTime", metaField = "index")

public class IndexRTModel {
	
	
	
    private Float fieldMarketCapitalisation;
    private Date fieldTransactTime;
    private Float fieldIndexValue;
    private Float fieldDivisor;
    private Float fieldIndexLowValue;
    private Float fieldIndexHighValue;
    private Float fieldVarYear;
    private Float fieldVarVeille;
    private Float veille;
    private String index;
    private String indexUrl;

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    public void setFieldMarketCapitalisation(CharSequence fieldMarketCapitalisation) {
        this.fieldMarketCapitalisation = new Float(fieldMarketCapitalisation.toString());
    }
    
    public void setFieldTransactTime(CharSequence charSequence) {
        String dateString = charSequence.toString();

        // Gérer les formats avec ou sans 'Z'
        SimpleDateFormat isoFormat;
        if (dateString.endsWith("Z")) {
            isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        } else {
            isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        }
        
        isoFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        try {
            this.fieldTransactTime = isoFormat.parse(dateString);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public void setFieldIndexValue(CharSequence fieldIndexValue) {
        this.fieldIndexValue = new Float(fieldIndexValue.toString());
    }

    public void setFieldDivisor(CharSequence fieldDivisor) {
        this.fieldDivisor = new Float(fieldDivisor.toString());
    }

    public void setFieldIndexLowValue(CharSequence fieldIndexLowValue) {
        this.fieldIndexLowValue = new Float(fieldIndexLowValue.toString());
    }

    public void setFieldIndexHighValue(CharSequence fieldIndexHighValue) {
        this.fieldIndexHighValue = new Float(fieldIndexHighValue.toString());
    }

    public void setFieldVarYear(CharSequence fieldVarYear) {
        this.fieldVarYear = new Float(fieldVarYear.toString());
    }

    public void setFieldVarVeille(CharSequence fieldVarVeille) {
        this.fieldVarVeille = new Float(fieldVarVeille.toString());
    }

    public void setVeille(CharSequence veille) {
        this.veille = new Float(veille.toString());
    }

    public void setIndex(CharSequence index) {
        this.index = index.toString();
    }

    public void setIndexUrl(CharSequence indexUrl) {
        this.indexUrl = indexUrl.toString();
    }

    // Getters (optional)
    public Float getFieldMarketCapitalisation() {
        return fieldMarketCapitalisation;
    }

    public Date getFieldTransactTime() {
        return fieldTransactTime;
    }

    public Float getFieldIndexValue() {
        return fieldIndexValue;
    }

    public Float getFieldDivisor() {
        return fieldDivisor;
    }

    public Float getFieldIndexLowValue() {
        return fieldIndexLowValue;
    }

    public Float getFieldIndexHighValue() {
        return fieldIndexHighValue;
    }

    public Float getFieldVarYear() {
        return fieldVarYear;
    }

    public Float getFieldVarVeille() {
        return fieldVarVeille;
    }

    public Float getVeille() {
        return veille;
    }

    public String getIndex() {
        return index;
    }

    public String getIndexUrl() {
        return indexUrl;
    }
}
