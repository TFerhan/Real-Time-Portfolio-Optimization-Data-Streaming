package com.kafmongo.kafmongo.kafka.producer;

import com.kafmongo.kafmongo.api.DataFetchService;
import com.kafmongo.kafmongo.api.IndexRealTimeData;
import com.kafmongo.kafmongo.model.PortfolioStats;
import com.kafmongo.kafmongo.utils.WeightSchema;
import com.kafmongo.kafmongo.Service.DailyPriceService;
import com.kafmongo.kafmongo.api.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.HashMap;

import java.io.FileReader;
import java.lang.reflect.Type;

@Service
public class KafkaScheduledProducer {

    @Autowired
    private DataProducerService producerService;

    @Autowired
    private DataFetchService dataFetchService;

    @Autowired
    private DailyPriceService dailyPriceService;

    @Autowired
    private DailyBourseData dailyBourseData;

    @Autowired
    private IndexRealTimeData indexRealTimeData;
    
    @Autowired
    private DailyIndexData dailyIndexData;
    
    
    
    private boolean portfolioStatsSent = false;
    private boolean weightsSent = false;


    @Scheduled(fixedRate = 2000)  // Runs every 1 minute (60,000 ms)
    public void fetchDataAndSendToKafka() {
    	
    	LocalTime now = LocalTime.now();
        LocalTime marketOpen = LocalTime.of(9, 0);
        LocalTime marketClose = LocalTime.of(3, 0);

		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		String currentTime = LocalDateTime.now().format(formatter);
        //if (now.isAfter(marketOpen) && now.isBefore(marketClose)) {
	        try {
	            JSONArray bourseData = dataFetchService.aralya_data();
	            producerService.sendBourseDataToKafka(bourseData, "intraday-stock-prices");
	            
				
	
	            //Map<String, JSONArray> dailyPrices = dailyBourseData.getAllDataSymbols("2025-04-05", null, null);
	            //producerService.sendDailyPriceDataToKafka(dailyPrices, "daily-prices");
	
	            //JSONArray indexData = indexRealTimeData.aralya_data();
	            //producerService.sendIndexRTData(indexData, "intraday-index-prices");
	        	
	        	//Map<String, JSONArray> data = dailyIndexData.getDataOfAllIndex(null, null, 10000);
	        	//producerService.sendDailyIndexDataToKafka(data, "daily-index");
	        	JSONObject rawJson = new JSONObject(new JSONTokener(new FileReader("src/main/resources/covariance/covariance.json")));
	            Map<CharSequence, Map<CharSequence, Double>> matrix = new HashMap<>();

	            for (String key : rawJson.keySet()) {
	                JSONObject innerJson = rawJson.getJSONObject(key);
	                Map<CharSequence, Double> inner = new HashMap<>();
	                for (String innerKey : innerJson.keySet()) {
	                    inner.put(innerKey, innerJson.getDouble(innerKey));
	                }
	                matrix.put(key, inner);
	            }
	            
	            
	            PortfolioStats portfolioStats = new PortfolioStats();
	            portfolioStats.setPortfolioId("portf1");
	            
	            portfolioStats.setTimestamp(System.currentTimeMillis());
	            
	            Map<CharSequence, Double> meanReturns = new HashMap<>();
	            meanReturns.put("AKT", 0.005196372580299284);
	            meanReturns.put("ATW", 0.0033257078951037097);
	            meanReturns.put("BOA", 0.0028496011335707536);
	            meanReturns.put("BCP", 0.002401358466412518);
	            meanReturns.put("MDP", 0.0020257214709478617);
	            meanReturns.put("MUT", 0.002579434770790776);
	            meanReturns.put("IAM", 0.003763416625472717);
	            meanReturns.put("RIS", 0.00368023737040771);
	            meanReturns.put("TGC", 0.005088343492788441);
	            meanReturns.put("ADI", 0.004413783266491786);
	            meanReturns.put("IMO", 0.0019497313509424844);
	            portfolioStats.setMeanReturns(meanReturns);
	            
	            portfolioStats.setCovarianceMatrix(matrix);
	            if (!portfolioStatsSent) {
	                
	                producerService.sendPortfolioStatsToKafka(portfolioStats, "portfStats");
	               portfolioStatsSent = true;
	            }

	            
	            
	        	JSONArray initial_weights = new JSONArray();

	        	initial_weights.put(new JSONObject()
	        	    .put("ticker", "AKT")
	        	    .put("weight", "0.152062782216893")
	        	    .put("time", currentTime));

	        	initial_weights.put(new JSONObject()
	        	    .put("ticker", "ATW")
	        	    .put("weight", "0.0927224366804686")
	        	    .put("time", currentTime));

	        	initial_weights.put(new JSONObject()
	        	    .put("ticker", "BOA")
	        	    .put("weight", "0.0746007358155665")
	        	    .put("time", currentTime));

	        	initial_weights.put(new JSONObject()
	        	    .put("ticker", "BCP")
	        	    .put("weight", "0.0558472973286458")
	        	    .put("time", currentTime));

	        	initial_weights.put(new JSONObject()
	        	    .put("ticker", "MDP")
	        	    .put("weight", "0.0333249293085295")
	        	    .put("time", currentTime));

	        	initial_weights.put(new JSONObject()
	        	    .put("ticker", "MUT")
	        	    .put("weight", "0.0725159654480422")
	        	    .put("time", currentTime));

	        	initial_weights.put(new JSONObject()
	        	    .put("ticker", "IAM")
	        	    .put("weight", "0.0273890125096159")
	        	    .put("time", currentTime));

	        	initial_weights.put(new JSONObject()
	        	    .put("ticker", "RIS")
	        	    .put("weight", "0.0724843896208113")
	        	    .put("time", currentTime));

	        	initial_weights.put(new JSONObject()
	        	    .put("ticker", "TGC")
	        	    .put("weight", "0.1932864513908656")
	        	    .put("time", currentTime));

	        	initial_weights.put(new JSONObject()
	        	    .put("ticker", "ADI")
	        	    .put("weight", "0.1667890042224167")
	        	    .put("time", currentTime));

	        	initial_weights.put(new JSONObject()
	        	    .put("ticker", "IMO")
	        	    .put("weight", "0.0589769954581452")
	        	    .put("time", currentTime));
	        	
	        	Map<CharSequence, Double> weightsMap = new HashMap<>();

	        	for (int i = 0; i < initial_weights.length(); i++) {
	        	    JSONObject obj = initial_weights.getJSONObject(i);
	        	    CharSequence ticker = obj.getString("ticker");
	        	    double weight = Double.parseDouble(obj.getString("weight"));
	        	    weightsMap.put(ticker, weight);
	        	}
	        	
	        	WeightSchema weights = WeightSchema.newBuilder()
						.setPortfolioId("portf1")
						.setTimestamp(System.currentTimeMillis())
						.setWeights(weightsMap)
						.build();
	        	
	        	if (!weightsSent) {
					producerService.sendWeightsToKafka(weights, "Weights");
					weightsSent = true;
	        	}


	            //producerService.setInitialPoWeights(initial_weights, "stockWeights");
	
	            System.out.println("Data successfully sent to Kafka at: " + System.currentTimeMillis());
	
	        } catch (Exception e) {
	            e.printStackTrace();
	            System.err.println("Failed to fetch or process data from API");
	        }
	              //} else {
	            //	  System.out.println("Market is closed. Skipping Kafka data push.");
	             // }
    }
}
