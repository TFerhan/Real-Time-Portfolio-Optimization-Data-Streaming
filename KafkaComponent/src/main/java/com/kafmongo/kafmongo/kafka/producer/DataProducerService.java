package com.kafmongo.kafmongo.kafka.producer;

import com.kafmongo.kafmongo.utils.BourseData;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.kafmongo.kafmongo.model.PortfolioStats;
import com.kafmongo.kafmongo.utils.*;
import java.util.Map;

@Service
public class DataProducerService {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    public DataProducerService(KafkaTemplate<String, byte[]> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendBourseDataToKafka(JSONArray data, String topic) {
        

        for (int i = 0; i < data.length(); i++) {
            try {
                JSONObject record = (JSONObject) data.get(i);
                String ticker = record.getString("ticker");
                BourseData bourseData = new BourseData();
                for (String key : record.keySet()) {

                    bourseData.put(key, record.get(key));
                    
                }

                ByteArrayOutputStream out = new ByteArrayOutputStream();
                DatumWriter<BourseData> datumWriter = new SpecificDatumWriter<>(BourseData.class);
                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
                datumWriter.write(bourseData, encoder);
                encoder.flush();
                byte[] avroData = out.toByteArray();

                ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topic, ticker, avroData);
                producerRecord.headers().add("source", topic.getBytes(StandardCharsets.UTF_8));
                kafkaTemplate.send(producerRecord);
                
            } catch (Exception e) {
                e.printStackTrace();
                continue;
            }
        }
        System.out.println("Message sent to topic: " + topic);
    }
    
    public void sendDailyPriceDataToKafka(Map<String, JSONArray> data, String topic) {
    	int count = 0;
    	
    	for(String ticker : data.keySet()) {
    		JSONArray records = data.get(ticker);
    		for(int i = 0; i < records.length(); i++) {
    			try {
    				
                    JSONObject record = (JSONObject) records.get(i);
                    
                    
                     DailyPriceStock dailyStock = new DailyPriceStock();

                     dailyStock.put("ticker", ticker);
                    for (String key : record.keySet()) {
                    	try {
	                    	if(!(record.get(key) instanceof String)) {
	                    		dailyStock.put(key, String.valueOf(record.get(key)));
	                    		continue;
	                    	
	                    	}
	
	                    	dailyStock.put(key, record.get(key));
                    	}catch(Exception e) {
                    		System.out.println("Error in key: " + key);
                    		dailyStock.put(key, "0");
                    	}
                        
                    }

                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    DatumWriter<DailyPriceStock> datumWriter = new SpecificDatumWriter<>(DailyPriceStock.class);
                    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
                    datumWriter.write(dailyStock, encoder);
                    encoder.flush();
                    byte[] avroData = out.toByteArray();

                    ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topic, ticker, avroData);
                    producerRecord.headers().add("source", topic.getBytes(StandardCharsets.UTF_8));
                    kafkaTemplate.send(producerRecord);
                    
                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }
    		}
    	}
    	System.out.println("Message sent to topic: " + topic);
    }
    
    
    public void sendIndexRTData(JSONArray data, String topic) {
    	for(int i = 0; i < data.length(); i++) {
    		IndexRTSchema indexRt = new IndexRTSchema();
    		JSONObject record = (JSONObject) data.get(i);
    		String name_idx = record.getString("index");
    		for(String key : record.keySet()) {
    			try {
    				indexRt.put(key, record.get(key));
    				
				} catch (Exception e) {
					System.out.println("Error in key: " + key);
					indexRt.put(key, "0");
				}
    			
    			
    		}
    		
    		
    		try {
    			ByteArrayOutputStream out = new ByteArrayOutputStream();
        		DatumWriter<IndexRTSchema> datum = new SpecificDatumWriter<>(IndexRTSchema.class);
        		BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
				datum.write(indexRt, encoder);
				encoder.flush();
				byte[] avroData = out.toByteArray();
	    		ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topic, name_idx, avroData);
	    		producerRecord.headers().add("source", topic.getBytes(StandardCharsets.UTF_8));
	    		kafkaTemplate.send(producerRecord);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				continue;
			}
    		
    		System.out.println("Message sent to topic: " + topic);
    		
    		
    	}
    	
    	
    }
    
    public void sendDailyIndexDataToKafka(Map<String, JSONArray> data, String topic) {
      
        int processed = 0;
        int skipped = 0;

        for(String index : data.keySet()) {
            JSONArray records = data.get(index);
            for(int i = 0; i < records.length(); i++) {
                try {
                    JSONObject record = (JSONObject) records.get(i);
                    System.out.println("Processing record: " + record);

                    // Check if all required fields exist
                    if (record.has("date") && record.has("index") &&
                            record.has("symbol") && record.has("value")) {

                        // Get values with null safety
                        String dateVal = record.isNull("date") ? "00" : record.getString("date");
                        String indexVal = record.isNull("index") ? "00" : record.getString("index");
                        String symbolVal = record.isNull("symbol") ? "00" : record.getString("symbol");
                        String valueVal = record.isNull("value") ? "00" : record.getString("value");

                        // Use the Builder pattern with null-safe values
                        DailyIndex dailyindex = DailyIndex.newBuilder()
                                .setDate(dateVal)
                                .setIndex(indexVal)
                                .setSymbol(symbolVal)
                                .setValue(valueVal)
                                .build();
            
                        ByteArrayOutputStream out = new ByteArrayOutputStream();
                        DatumWriter<DailyIndex> datumWriter = new SpecificDatumWriter<>(DailyIndex.class);
                        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
                        datumWriter.write(dailyindex, encoder);
                        encoder.flush();
                        byte[] avroData = out.toByteArray();

                        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topic, index, avroData);
                        producerRecord.headers().add("source", topic.getBytes(StandardCharsets.UTF_8));
                  
                        kafkaTemplate.send(producerRecord);
                        processed++;
                    } else {
                        System.out.println("Skipping record with missing fields: " + record);
                        skipped++;
                        continue;
                    }

                } catch (Exception e) {
                    System.err.println("Error processing record: " + e.getMessage());
                    e.printStackTrace();
                    skipped++;
                    continue;
                }
            }
        }
        System.out.println("Message sending completed. Topic: " + topic +
                " | Processed: " + processed + " | Skipped: " + skipped);
    }
    
    public void setInitialPoWeights(JSONArray data, String topic) {
    	for(int i = 0; i < data.length(); i++) {
    		try {
    			JSONObject obj = (JSONObject) data.getJSONObject(i);
    			if(obj.has("time") && obj.has("ticker") && obj.has("weight")) {
    				String time = obj.getString("time");
    				String ticker = obj.getString("ticker");
    				String weight = obj.getString("weight");
    				String portfolioId = obj.has("portfolio_id") ? obj.getString("portfolio_id") : "portf1";
    				
    				WeightStockSchema weightStock = WeightStockSchema.newBuilder()
    						.setTicker(ticker)
    						.setWeight(weight)
    						.setTime(time)
    						.setPortfolioId(portfolioId)
    						.build();
    				
    				ByteArrayOutputStream out = new ByteArrayOutputStream();
    				DatumWriter<WeightStockSchema> datum = new SpecificDatumWriter<>(WeightStockSchema.class);
    				BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    				datum.write(weightStock, encoder);
    				encoder.flush();
    				byte[] avroData = out.toByteArray();
    				
    				ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topic, ticker, avroData);
    				producerRecord.headers().add("source", topic.getBytes(StandardCharsets.UTF_8));
    				kafkaTemplate.send(producerRecord);
    				
    				
    				
    				
    			}else {
                    System.out.println("Skipping record with missing fields: " );
                    continue;
                }
    			
    			
    			
    		}catch (Exception e) {
                System.err.println("Error processing record: " + e.getMessage());
                e.printStackTrace();
                continue;
            }
        }
    	
    	System.out.println("Message sending completed. Topic: " + topic );
    		
    		
    	
    }
    
    public void sendPortfolioStatsToKafka(PortfolioStats portf, String topic) {
    	try {
    		PortfolioStatsSchema portfolioStats = PortfolioStatsSchema.newBuilder()
    				.setTimestamp(portf.getTimestamp())
    				.setMeanReturns(portf.getMeanReturns())
    				.setCovarianceMatrix(portf.getCovarianceMatrix())
    				.setPortfolioId(portf.getPortfolioId())
    				.build();
    		ByteArrayOutputStream out = new ByteArrayOutputStream();
    		DatumWriter<PortfolioStatsSchema> datum = new SpecificDatumWriter<>(PortfolioStatsSchema.class);
    		BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    		datum.write(portfolioStats, encoder);
    		encoder.flush();
    		byte[] avroData = out.toByteArray();
    		ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topic, String.valueOf(portf.getTimestamp()), avroData);
    		producerRecord.headers().add("source", topic.getBytes(StandardCharsets.UTF_8));
    		kafkaTemplate.send(producerRecord);
    		System.out.println("Portfolio stats sent to topic: " + topic);
		} catch (Exception e) {
			System.err.println("Error processing record: " + e.getMessage());
			e.printStackTrace();
			return;
		}
    	
    }
    
    public void sendWeightsToKafka(WeightSchema weights, String topic) {
    	try {
    		
    		ByteArrayOutputStream out = new ByteArrayOutputStream();
    		DatumWriter<WeightSchema> datum = new SpecificDatumWriter<>(WeightSchema.class);
    		BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    		datum.write(weights, encoder);
    		encoder.flush();
    		byte[] avroData = out.toByteArray();
    		ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topic, String.valueOf(weights.getTimestamp()), avroData);
    		producerRecord.headers().add("source", topic.getBytes(StandardCharsets.UTF_8));
    		kafkaTemplate.send(producerRecord);
    		System.out.println("Portfolio stats sent to topic: " + topic);
		} catch (Exception e) {
			System.err.println("Error processing record: " + e.getMessage());
			e.printStackTrace();
			return;
		}
    	
    }
    
    
}
