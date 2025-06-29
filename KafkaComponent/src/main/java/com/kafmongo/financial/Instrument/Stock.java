package com.kafmongo.financial.Instrument;
import java.util.Date;

public class Stock {
	
	private String ticker;
	private Date datePrice;
	private float openPrice;
	private float closePrice;
	private float highPrice;
	private float lowPrice;
	private long volume;
	private String sousSecteur;
	private float currentPrice;
	private float varVeille;
	
	public Stock() {}
	
	public Stock(String ticker, Date datePrice, float openPrice, float closePrice, float highPrice, float lowPrice,
			long volume, String sousSecteur, float currentPrice, float varVeille) {
		this.ticker = ticker;
		this.datePrice = datePrice;
		this.openPrice = openPrice;
		this.closePrice = closePrice;
		this.highPrice = highPrice;
		this.lowPrice = lowPrice;
		this.volume = volume;
		this.sousSecteur = sousSecteur;
		this.currentPrice = currentPrice;
		this.varVeille = varVeille;
	}
	
	public String getTicker() {
		return ticker;
	}
	
	public void setTicker(String ticker) {
		this.ticker = ticker;
	}
	
	public Date getDatePrice() {
		return datePrice;
	}
	
	public void setDatePrice(Date datePrice) {
		this.datePrice = datePrice;
	}
	
	
	public float getOpenPrice() {
		return openPrice;
	}
	
	public void setOpenPrice(float openPrice) {
		this.openPrice = openPrice;
	}
	
	public float getClosePrice() {
		return closePrice;
	}
	
	public void setClosePrice(float closePrice) {
		this.closePrice = closePrice;
	}
	
	public float getHighPrice() {
		return highPrice;
	}
	
	public void setHighPrice(float highPrice) {
		this.highPrice = highPrice;
	}
	
	public float getLowPrice() {
		return lowPrice;
	}
	
	public void setLowPrice(float lowPrice) {
		this.lowPrice = lowPrice;
	}
	
	public long getVolume() {
		return volume;
	}
	
	public void setVolume(long volume) {
		this.volume = volume;
	}
	
	public String getSousSecteur() {
		return sousSecteur;
	}
	
	public void setSousSecteur(String sousSecteur) {
		this.sousSecteur = sousSecteur;
	}
	
	public float getCurrentPrice() {
		return currentPrice;
	}
	
	public void setCurrentPrice(float currentPrice) {
		this.currentPrice = currentPrice;
	}
	
	public float getVarVeille() {
		return varVeille;
	}
	
	public void setVarVeille(float varVeille) {
		this.varVeille = varVeille;
	}
	

}
