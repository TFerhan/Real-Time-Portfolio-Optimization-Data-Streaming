package com.kafmongo.financial.Metrics;
import java.util.*;
import java.io.*;
import org.apache.commons.math3.linear.*;
import com.kafmongo.financial.Instrument.Stock;

public interface StocksMetrics {
	
	RealMatrix covarianceMatrix(Stock B);
	float correlation(Stock B);
	float beta(Stock B);
	float alpha(Stock B);
	float sharpeRatio(float riskFreeRate);
	float expectedReturn();
	float volatility();
	float valueAtRisk(float confidenceLevel);
	float kurtois();
	
	 
	
	
	
	
	
	
	
	

}
