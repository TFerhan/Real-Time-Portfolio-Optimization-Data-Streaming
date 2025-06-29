package com.kafmongo.kafmongo.kafka.consumer;

import com.kafmongo.kafmongo.utils.BourseData;
import com.kafmongo.kafmongo.utils.DailyIndex;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONObject;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import com.kafmongo.kafmongo.Repo.BourseRepo;
import com.kafmongo.kafmongo.Service.*;
import com.kafmongo.kafmongo.Service.DailyPriceService;
import com.kafmongo.kafmongo.model.BourseModel;
import com.kafmongo.kafmongo.model.DailyIndexModel;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import com.kafmongo.kafmongo.model.DailyPrice;
import com.kafmongo.kafmongo.model.IndexRTModel;
import com.kafmongo.kafmongo.utils.DailyPriceStock;
import com.kafmongo.kafmongo.utils.IndexRTSchema;
import com.kafmongo.kafmongo.Repo.*;

@Service
public class DataConsumerService {
	
	@Autowired
	private BourseService bourseService;
	
	@Autowired
	private DailyPriceService dailyPriceService;
	
	@Autowired
	private IndexRTRepo indexRTRepo;
	
	@Autowired
	private DailyIndexService dailyIndexService;
	
	@Autowired
	private IndexRTService indexRTService;
	
	//@KafkaListener(topics = "intraday-stock-prices", groupId = "mongo_db")
	public void consume(ConsumerRecord<String, byte[]> record) {
	        try {
	            // Directly use the byte array received from Kafka, no need for Base64 decoding
	            byte[] avroData = record.value();
	            

	            // Deserialize the Avro data to BourseData object
	            BourseData bourseData = deserializeFromAvro(avroData);
	            JSONObject jsonObject = new JSONObject(bourseData);
	            BourseModel bourseEntity = mapToEntity(bourseData);
	            bourseService.save(bourseEntity);
	            System.out.println("Saved mongodb: " + record.key());
	            // Here you can process the bourseData object
	            
	        } catch (Exception e) {
	            System.err.printf("Error processing record: %s\n", e.getMessage());
	            e.printStackTrace();
	        }
	    }
	
	//@KafkaListener(topics = "intraday-index-prices", groupId = "mongo_db")
	public void consumeIndexRt(ConsumerRecord<String, byte[]> record) {
		try {
			byte[] avroData = record.value();
			
			IndexRTSchema indexData = deserializeFromAvroIndexRT(avroData);
			JSONObject jsonObject = new JSONObject(indexData);
			IndexRTModel indexEntity = mapToEntityIndexRT(indexData);
			indexRTService.save(indexEntity);
			System.out.println("Saved in mongodb: " + record.key());
 		} catch (Exception e) {
 			            System.err.printf("Error processing record : %s\n", e.getMessage());
 			            e.printStackTrace();
 		}
	}
	
	//@KafkaListener(topics = "daily-index", groupId = "timescale_group")
	public void consumeDailyIndex(ConsumerRecord<String, byte[]> record) {
		System.out.println("Consuming daily index data ...");
		try {

			byte[] avroData = record.value();


			DailyIndex dailyindex = deserializeFromAvroDailyIndex(avroData);
			JSONObject jsonObject = new JSONObject(dailyindex);
			DailyIndexModel dailyIndexEntity = mapToEntityDailyIndex(dailyindex);
			dailyIndexService.saveDailyIndex(dailyIndexEntity);
			System.out.println("Saved in timescaledb: " + record.key());


		}catch (Exception e) {
			System.err.printf("Error processing record : %s\n", e.getMessage());
			e.printStackTrace();
		}
	}


    
    private IndexRTModel mapToEntityIndexRT(IndexRTSchema indexData) {
		// TODO Auto-generated method stub
		IndexRTModel entity = new IndexRTModel();
		entity.setIndex(indexData.getIndex());
		entity.setFieldDivisor(indexData.getFieldDivisor());
		entity.setFieldIndexHighValue(indexData.getFieldIndexHighValue());
		entity.setFieldIndexLowValue(indexData.getFieldIndexHighValue());
		entity.setFieldIndexValue(indexData.getFieldIndexValue());
		entity.setFieldMarketCapitalisation(indexData.getFieldMarketCapitalisation());
		entity.setFieldTransactTime(indexData.getFieldTransactTime());
		entity.setFieldVarVeille(indexData.getFieldVarVeille());
		entity.setFieldVarYear(indexData.getFieldVarYear());
		entity.setIndexUrl(indexData.getIndexUrl());
		entity.setVeille(indexData.getVeille());
		return entity;
	}

	private IndexRTSchema deserializeFromAvroIndexRT(byte[] avroData) throws Exception {
		
		ByteArrayInputStream in = new ByteArrayInputStream(avroData);
		DatumReader<IndexRTSchema> datum = new SpecificDatumReader<>(IndexRTSchema.class);
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
		
		
		return datum.read(null,  decoder);
		
	}

	//@KafkaListener(topics = "daily-prices", groupId = "timescale_group")
    public void consumeDailyPrices(ConsumerRecord<String, byte[]> record) {
    	try {
    		
    		byte[] avroData = record.value();
    		
    		
    		DailyPriceStock dailyStock = deserializeFromAvroDailyPrice(avroData);
    		JSONObject jsonObject = new JSONObject(dailyStock);
    		DailyPrice dailyStockEntity = mapToEntityDailyPrice(dailyStock);
    		dailyPriceService.saveDailyPrice(dailyStockEntity);
    		System.out.println("Saved in timescaledb: " + record.key());
    		
    		
    	}catch (Exception e) {
    		System.err.printf("Error processing record : %s\n", e.getMessage());
    		e.printStackTrace();
    	}
    }
    
    

    private BourseData deserializeFromAvro(byte[] data) throws Exception {
        
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        DatumReader<BourseData> datumReader = new SpecificDatumReader<>(BourseData.class);

        
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
        

        // Deserialize the Avro data into a BourseData object
        return datumReader.read(null, decoder);
    }
    
    
    private DailyPriceStock deserializeFromAvroDailyPrice(byte[] data) throws Exception {
    	ByteArrayInputStream in = new ByteArrayInputStream(data);
    	DatumReader<DailyPriceStock> datumReader = new SpecificDatumReader<>(DailyPriceStock.class);
    	BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
    	return datumReader.read(null, decoder);
    	
    }
    
    
    
    private DailyIndexModel mapToEntityDailyIndex(DailyIndex dailyindex) {

		DailyIndexModel entity = new DailyIndexModel();
		entity.setSymbol(dailyindex.getSymbol());
		entity.setDate(dailyindex.getDate());
		entity.setIndex(dailyindex.getIndex());
		entity.setValue(dailyindex.getValue());
		return entity;


	}
    
    private DailyIndex deserializeFromAvroDailyIndex(byte[] data) throws Exception {
		ByteArrayInputStream in = new ByteArrayInputStream(data);
		DatumReader<DailyIndex> datumReader = new SpecificDatumReader<>(DailyIndex.class);
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
		return datumReader.read(null, decoder);

	}

    
    private BourseModel mapToEntity(BourseData bourseData) {
        BourseModel entity = new BourseModel();
        entity.setTicker(bourseData.getTicker());
        entity.setLabel(bourseData.getLabel());
        entity.setFieldDateApplication(bourseData.getFieldDateApplication());
        entity.setFieldClosingPrice(bourseData.getFieldClosingPrice());
        entity.setFieldDifference(bourseData.getFieldDifference());
        entity.setFieldVarVeille(bourseData.getFieldVarVeille());
        entity.setFieldBestAskSize(bourseData.getFieldBestAskSize());
        entity.setFieldTransactTime(bourseData.getFieldTransactTime());
        entity.setFieldCoursCloture(bourseData.getFieldCoursCloture());
        entity.setFieldCoursAjuste(bourseData.getFieldCoursAjuste());
        entity.setFieldCumulTitresEchanges(bourseData.getFieldCumulTitresEchanges());
        entity.setFieldBestBidSize(bourseData.getFieldBestBidSize());
        entity.setFieldCapitalisation(bourseData.getFieldCapitalisation());
        entity.setFieldOpeningPrice(bourseData.getFieldOpeningPrice());
        entity.setFieldTotalTrades(bourseData.getFieldTotalTrades());
        entity.setFieldCoursCourant(bourseData.getFieldCoursCourant());
        entity.setFieldBestAskPrice(bourseData.getFieldBestAskPrice());
        entity.setSousSecteur(bourseData.getSousSecteur());
        entity.setFieldInstrumentVarYear(bourseData.getFieldInstrumentVarYear());
        entity.setFieldVarPto(bourseData.getFieldVarPto());
        entity.setFieldLastTradedTime(bourseData.getFieldLastTradedTime());
        entity.setFieldBestBidPrice(bourseData.getFieldBestBidPrice());
        entity.setFieldDynamicReferencePrice(bourseData.getFieldDynamicReferencePrice());
        entity.setFieldStaticReferencePrice(bourseData.getFieldStaticReferencePrice());
        entity.setFieldLowPrice(bourseData.getFieldLowPrice());
        entity.setFieldHighPrice(bourseData.getFieldHighPrice());
        entity.setFieldCumulVolumeEchange(bourseData.getFieldCumulVolumeEchange());
        entity.setFieldLastTradedPrice(bourseData.getFieldLastTradedPrice());
        return entity;
    }
    
    private DailyPrice mapToEntityDailyPrice(DailyPriceStock dailyStock) {
    	DailyPrice entity = new DailyPrice();
    	entity.setTicker(dailyStock.getTicker());
    	entity.setCapitalisation(dailyStock.getCapitalisation());
    	entity.setClosingPrice(dailyStock.getClosingPrice());
    	entity.setCoursAjuste(dailyStock.getCoursAjuste());
    	entity.setCoursCourant(dailyStock.getCoursCourant());
    	entity.setCreated(dailyStock.getCreated());
    	entity.setCumulTitresEchanges(dailyStock.getCumulTitresEchanges());
    	entity.setCumulVolumeEchange(dailyStock.getCumulVolumeEchange());
    	entity.setHighPrice(dailyStock.getHighPrice());
    	entity.setLibelleFR(dailyStock.getLibelleFR());
    	entity.setLowPrice(dailyStock.getLowPrice());
    	entity.setOpeningPrice(dailyStock.getOpeningPrice());
    	entity.setRatioConsolide(dailyStock.getRatioConsolide());
    	entity.setTotalTrades(dailyStock.getTotalTrades());
    	return entity;
    	
    }

}
