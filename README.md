# Real-Time-Portfolio-Optimization-Data-Streaming

This project implements a real-time portfolio optimization pipeline, specifically targeting assets listed on the Casablanca Stock Exchange. It’s designed as a fully backend system focused on data flow and analytics, with no frontend interface. The core objective is to stream real-time stock prices for 10 selected assets and dynamically adjust portfolio allocations based on calculated risk-return metrics.

Weights are initialized using the Black-Litterman model. As new prices are streamed, the system updates the covariance matrix and expected returns using the Welford online algorithm. These updated statistics are used to compute the Sharpe ratio, which serves as the primary optimization trigger. When a defined threshold is crossed, a reallocation process is initiated to optimize performance. The loop continuously runs to minimize latency and manage risk exposure, assuming a risk-aversion coefficient of one (configurable).

Due to lack of access to the official real-time API from the Casablanca Stock Exchange (which is regulated and paid), the project simulates real-time data using a scheduler-based polling approach on their public API. While the Moroccan market lacks high-frequency data like larger exchanges (e.g. NSE, LSE), the simulation is sufficient for system behavior testing and integration validation.

Data ingestion is handled through a structured AVRO schema, here example of complex Portfolio Stats AVRO schema :

```json
{
  "type": "record",
  "name": "PortfolioStatsSchema",
  "namespace": "com.kafmongo.kafmongo.utils",
  "fields": [
    {
      "name": "timestamp",
      "type": "long"
    },
    {
      "name": "portfolio_id",
      "type": "string"
    },
    {
      "name": "meanReturns",
      "type": {
        "type": "map",
        "values": "double"
      }
    },
    {
      "name": "covarianceMatrix",
      "type": {
        "type": "map",
        "values": {
          "type": "map",
          "values": "double"
        }
      }
    }
  ]
}
```

Serialized and transmitted via Kafka. Each asset price is encapsulated in a schema-compliant class to ensure compatibility across consumers. The AVRO schema definitions are stored in the Kafka component under the `resources` directory. Kafka topics are partitioned to enable parallel data processing and distributed scalability. Configuration files for the Kafka producer are located under the `Producer` directory :

```java
@Configuration
public class KafkaProducerConfig {
        @Bean
        public ProducerFactory<String, byte[]> producerFactory() {
            Map<String, Object> configProps = new HashMap<>();
            configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            configProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10240);
            configProps.put(ProducerConfig.ACKS_CONFIG, "all");
            configProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 12000);
            configProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1200);
            configProps.put(ProducerConfig.LINGER_MS_CONFIG, 200);
            configProps.put(ProducerConfig.COMPRESSION_GZIP_LEVEL_CONFIG, 9);
            configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
            configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            return new DefaultKafkaProducerFactory<>(configProps);
        }

        @Bean
        public KafkaTemplate<String, byte[]> kafkaTemplate() {
            return new KafkaTemplate<>(producerFactory());
        }
        
        @Bean
		public NewTopic intradayStock() {
			return new NewTopic("intraday-stock-prices", 3, (short) 1);
		}
        
        @Bean
		public NewTopic weights() {
			return new NewTopic("Weights", 3, (short) 1);
		}
        
        @Bean
		public NewTopic portfolioMetrics() {
			return new NewTopic("portfMetrics", 3, (short) 1);
		}
        
        @Bean
        public NewTopic portfolioStats() {
        	return new NewTopic("portfStats", 3, (short) 1);
        }
        
        @Bean
        public NewTopic portfUpdatedStats() {
        	return new NewTopic("portfUpdatedStats", 3, (short) 1);
        }
    }
```

and all relevant Kafka parameters are documented there.

Additional data sources, including index-level and daily price APIs, are used to support the initial weight allocation and can also be integrated into the live stream.

```java
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
```

 Once prices are flowing, the same pipeline infrastructure is applied to weights and portfolio statistics to establish the system state before switching to streaming-only for prices.

```java
@Scheduled(fixedRate = 2000)  
    public void fetchDataAndSendToKafka() {
	        try {
	            JSONArray bourseData = dataFetchService.real_time_data();
	            producerService.sendBourseDataToKafka(bourseData, "intraday-stock-prices");
	            
			
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
	        	    .put("weight", "0.14041")
	        	    .put("time", currentTime));

	        	initial_weights.put(new JSONObject()
	        	    .put("ticker", "ATW")
	        	    .put("weight", "0.08846")
	        	    .put("time", currentTime));

	        	initial_weights.put(new JSONObject()
	        	    .put("ticker", "BOA")
	        	    .put("weight", "0.07971")
	        	    .put("time", currentTime));

	        	initial_weights.put(new JSONObject()
	        	    .put("ticker", "BCP")
	        	    .put("weight", "0.0684")
	        	    .put("time", currentTime));

	        	initial_weights.put(new JSONObject()
	        	    .put("ticker", "MDP")
	        	    .put("weight", "0.06159")
	        	    .put("time", currentTime));

	        	initial_weights.put(new JSONObject()
	        	    .put("ticker", "MUT")
	        	    .put("weight", "0.06873")
	        	    .put("time", currentTime));

	        	initial_weights.put(new JSONObject()
	        	    .put("ticker", "IAM")
	        	    .put("weight", "0.10037")
	        	    .put("time", currentTime));

	        	initial_weights.put(new JSONObject()
	        	    .put("ticker", "RIS")
	        	    .put("weight", "0.09439")
	        	    .put("time", currentTime));

	        	initial_weights.put(new JSONObject()
	        	    .put("ticker", "TGC")
	        	    .put("weight", "0.13419")
	        	    .put("time", currentTime));

	        	initial_weights.put(new JSONObject()
	        	    .put("ticker", "ADI")
	        	    .put("weight", "0.10406")
	        	    .put("time", currentTime));

	        	initial_weights.put(new JSONObject()
	        	    .put("ticker", "IMO")
	        	    .put("weight", "0.0597")
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

	            System.out.println("Data successfully sent to Kafka at: " + System.currentTimeMillis());
	
	        } catch (Exception e) {
	            e.printStackTrace();
	            System.err.println("Failed to fetch or process data from API");
	        }
```

In the second phase, real-time prices are consumed by Apache Flink. Flink applies watermarks to each message and operates using event time semantics to ensure accurate alignment with trade timestamps, not ingestion time.

```java
OffsetsInitializer latest = OffsetsInitializer.latest();
OffsetsInitializer earliest = OffsetsInitializer.earliest();

KafkaSource<BourseData> priceSource = KafkaSource.<BourseData>builder()
        .setBootstrapServers("localhost:9092")
        .setTopics("intraday-stock-prices")
        .setGroupId("flink-prices-group")
        .setStartingOffsets(latest)
        .setDeserializer(new BourseDataDeserializationSchema())
        .build();

KafkaSource<WeightSchema> weightS = KafkaSource.<WeightSchema>builder()
        .setBootstrapServers("localhost:9092")
        .setTopics("Weights")
        .setGroupId("flink-weights-group")
        .setStartingOffsets(latest)
        .setDeserializer(new WeightDeserializationSchema())
        .build();

KafkaSource<PortfolioStatsSchema> portfolioStatsSource = KafkaSource.<PortfolioStatsSchema>builder()
        .setBootstrapServers("localhost:9092")
        .setTopics("portfStats")
        .setGroupId("flink-portfStats-group")
        .setStartingOffsets(latest)
        .setDeserializer(new PortfolioStatsDeserializationSchema())
        .build();

WatermarkStrategy<BourseData> watermarkPriceStrategy = WatermarkStrategy
        .<BourseData>forBoundedOutOfOrderness(Duration.ofMinutes(1))
        .withTimestampAssigner((event, timestamp) ->
                java.time.OffsetDateTime.parse(event.getFieldLastTradedTime())
                        .toInstant()
                        .toEpochMilli()
        );
```

 Consumers are configured to use the latest offsets to avoid replaying historical data. Once consumers and watermarks are initialized, Flink maps incoming data by portfolio ID (for weights and statistics) and by ticker (for prices).

```java
KeyedStream<PortfolioStatsSchema, String> keyedStatsStream = portfolioStatsStream
        .keyBy(data -> data.getPortfolioId().toString());

KeyedStream<BourseData, String> keyedPricesStream = pricesStream
        .keyBy(data -> data.getTicker().toString());
  
DataStream<StockReturn> logReturnsStream = keyedPricesStream.window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
        .process(new LogReturnWindowFunction());
```

The log returns are computed in a windowed aggregation function, using intervals to group streamed prices. A small epsilon is added to the base price to account for simulation noise. This logic is encapsulated in `LogReturnWindowFunction` within the Flink component's utils folder.

```java
public class LogReturnWindowFunction extends ProcessWindowFunction<BourseData, StockReturn, String, TimeWindow> {
    
    @Override
    public void process(String s, ProcessWindowFunction<BourseData, StockReturn, String, TimeWindow>.Context context, Iterable<BourseData> iterable, Collector<StockReturn> collector) throws Exception {

        Iterator<BourseData> iterator = iterable.iterator();
        if(iterator.hasNext()) {
            Random random = new Random();
            BourseData first = iterator.next();
            double firstPrice = Double.parseDouble(first.getFieldOpeningPrice().toString());

            firstPrice += random.nextGaussian();
            double lastPrice = firstPrice;

            String lastPriceTime = first.getFieldLastTradedTime().toString();
            while (iterator.hasNext()) {
                BourseData current = iterator.next();
                lastPrice = Double.parseDouble(current.getFieldCoursCourant().toString());
                lastPriceTime = current.getFieldLastTradedTime().toString();
            }
            
            if(firstPrice == 0.0){
                return;
            }

            double logReturn = Math.log(lastPrice / firstPrice);

            if(Double.isNaN(logReturn)) {

            System.out.println("Log return is zero or NaN " + logReturn + " for " + s + ", skipping.");

                return;
            }
            
            collector.collect(new StockReturn(s, logReturn, lastPriceTime, "portf1"));
        }

        }

    }
```

Once log returns are available, they are joined with existing portfolio statistics to update covariance and expected return values. 

```java
DataStream<PortfolioStatsSchema> updatedPortfolioStats = logReturnsStream.keyBy(StockReturn::getPortfolioId)
        .connect(keyedStatsStream)
        .process(new PortfolioUpdate())
        .name("Portfolio Stats Update");

KafkaSink<PortfolioStatsSchema> kafkaSink = KafkaSink.<PortfolioStatsSchema>builder()
        .setBootstrapServers("localhost:9092")
        .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                        .setTopic("portfUpdatedStats")
                        .setValueSerializationSchema(new PortfolioStatsAvroSerializationSchema())
                        .build()
        )
        .build();
        
updatedPortfolioStats.sinkTo(kafkaSink)
```

This is done by merging the return stream with the keyed portfolio stats stream using Flink’s stateful processing engine. A value state holds the portfolio stats object, which is updated incrementally using the Welford algorithm. The state can be persisted in RocksDB to allow checkpoint-based recovery (optional and not implemented here). The core logic for this operation resides in the `PortfolioUpdate` function, enabling consistent updates in near real-time.

```java
public class PortfolioUpdate extends KeyedCoProcessFunction<String, StockReturn, PortfolioStatsSchema, PortfolioStatsSchema> {

    private transient ValueState<PortfolioStatsSchema> globalStats;
    
    @Override
    public void open(Configuration config) {
        TypeInformation<PortfolioStatsSchema> typeInfo = TypeInformation.of(PortfolioStatsSchema.class);
        ValueStateDescriptor<PortfolioStatsSchema> statsDescriptor =
                new ValueStateDescriptor<>("globalPortfolioStats", typeInfo);

        globalStats = getRuntimeContext().getState(statsDescriptor);

    }

    @Override
    public void processElement1(
            StockReturn stockReturn,
            Context context,
            Collector<PortfolioStatsSchema> collector
    ) throws Exception {
        PortfolioStatsSchema currentStats = globalStats.value();

        if (currentStats == null) {
            return;
        }

        PortfolioStatsSchema updatedStats = updateStats(currentStats, stockReturn);
        globalStats.update(updatedStats);
        collector.collect(updatedStats);
        

    }

    @Override
    public void processElement2(PortfolioStatsSchema newStats, Context ctx, Collector<PortfolioStatsSchema> out) throws Exception {
        PortfolioStatsSchema currentStats = globalStats.value();
        if (currentStats == null) {
            globalStats.update(newStats);

        }

    }

    private PortfolioStatsSchema updateStats(PortfolioStatsSchema stats, StockReturn ret) {
        String symbol = ret.getTicker();
        double retValue = ret.getReturnValue();
        Utf8 utf8Symbol = new Utf8(symbol);
        Map<CharSequence, Double> meanReturns = stats.getMeanReturns();
        Map<CharSequence, Map<CharSequence, Double>> covMatrix = stats.getCovarianceMatrix();

        int count = 1000;
        int newCount = count + 1;

        double oldMean = meanReturns.getOrDefault(utf8Symbol, 0.0);

        double newMean = oldMean + (retValue - oldMean) / newCount;

        if(oldMean != 0.0 ) {
            meanReturns.put(utf8Symbol, newMean);
        }

        for (CharSequence otherSymbol : meanReturns.keySet()) {

            Utf8 utf8OtherSymbol = new Utf8(otherSymbol.toString());

            double otherMean = meanReturns.getOrDefault(utf8OtherSymbol, 0.0);

            if(otherMean == 0.0 ) {
                continue;
            }

            double covOld = covMatrix.getOrDefault(utf8Symbol, new HashMap<>())
                    .getOrDefault(utf8OtherSymbol, 0.0);

            if(covOld == 0.0 ) {
                continue;
            }

            double retOther = utf8Symbol.compareTo(utf8OtherSymbol) == 0 ? retValue : otherMean;

            double deltaCov = ((retValue - oldMean) * (retOther - otherMean) - covOld) / newCount;

            double covNew = covOld + deltaCov;

            covMatrix.computeIfAbsent(utf8Symbol, k -> new HashMap<>()).put(utf8OtherSymbol, covNew);
            covMatrix.computeIfAbsent(utf8OtherSymbol, k -> new HashMap<>()).put(utf8Symbol, covNew);

        }

        stats.setMeanReturns(meanReturns);
        stats.setCovarianceMatrix(covMatrix);
        stats.setTimestamp(Instant.now().toEpochMilli());

        return stats;
    }

}
```

Updated portfolio stats are published to a new Kafka topic. From this point, the system has all necessary components—current weights and updated statistics—to compute portfolio metrics. A dedicated coprocessing function, `PortfolioMetricsFunction`, consumes both streams and maintains three separate states: weights, stats, and metrics. Whenever an update occurs in either weights or stats, the Sharpe ratio is recalculated using the standard formula: (expected return - risk-free rate) / standard deviation (derived from covariance). This provides a real-time metric reflecting portfolio efficiency.

```java
public class PortfolioMetricsFunction extends KeyedCoProcessFunction<String, WeightSchema, PortfolioStatsSchema, PortfolioMetrics> {

    private transient ValueState<PortfolioStatsSchema> portfolioStatsState;

    private transient ValueState<WeightSchema> weightStockState;

    private transient ValueState<Map> lastEmittedMetrics;

    @Override
    public void open(Configuration config) throws Exception {

        TypeInformation<PortfolioStatsSchema> statsTypeInfo = TypeInformation.of(PortfolioStatsSchema.class);
        ValueStateDescriptor<PortfolioStatsSchema> statsDescriptor =
                new ValueStateDescriptor<>("portfolioStats", statsTypeInfo);
        portfolioStatsState = getRuntimeContext().getState(statsDescriptor);

        TypeInformation<WeightSchema> weightTypeInfo = TypeInformation.of(WeightSchema.class);
        ValueStateDescriptor<WeightSchema> weightDescriptor =
                new ValueStateDescriptor<>("weightStock", weightTypeInfo);
        weightStockState = getRuntimeContext().getState(weightDescriptor);

        ValueStateDescriptor<Map> lastEmittedHashDescriptor =
                new ValueStateDescriptor<>("lastEmittedHash", TypeInformation.of(Map.class));
        lastEmittedMetrics = getRuntimeContext().getState(lastEmittedHashDescriptor);
    }

    @Override
    public void processElement1(WeightSchema w8, KeyedCoProcessFunction<String, WeightSchema, PortfolioStatsSchema, PortfolioMetrics>.Context context, Collector<PortfolioMetrics> collector) throws Exception {
        weightStockState.update(w8);

        System.out.println("Received weight update: " + w8);

        PortfolioStatsSchema currentStats = portfolioStatsState.value();

        if (currentStats == null) {

            return;
        }

        PortfolioMetrics updatedMetrics = calculateMetrics(w8, currentStats);
        Map<Double, Double> updated = new HashMap<>();
        updated.put(updatedMetrics.getExpectedReturn(), updatedMetrics.getRisk());

        if (shouldEmit(updated)) {
            collector.collect(updatedMetrics);
        }

    }

    private PortfolioMetrics calculateMetrics(WeightSchema w8, PortfolioStatsSchema currentStats) {

        PortfolioMetrics metrics =  new PortfolioMetrics();
        Map<CharSequence, Double> weights = w8.getWeights();
        Map<CharSequence, Double> meanReturns = currentStats.getMeanReturns();
        Map<CharSequence, Map<CharSequence, Double>> covMatrix = currentStats.getCovarianceMatrix();

        double expectedReturns = weights.entrySet().stream()
                .mapToDouble(entry -> entry.getValue() * meanReturns.getOrDefault((Utf8)  entry.getKey(), 0.0))
                .sum();

        System.out.println("expected return : " + expectedReturns );

        double variance = 0.0;
        for(Map.Entry<CharSequence, Double> entry1 : weights.entrySet()){

            Utf8 i = (Utf8) entry1.getKey();
            double w_i = entry1.getValue();

            for(Map.Entry<CharSequence, Double> entry2 : weights.entrySet()){
                Utf8 j = (Utf8) entry2.getKey();
                double w_j = entry2.getValue();
                double cov_ij = covMatrix.getOrDefault(i, Map.of()).getOrDefault(j, 0.0);
                variance += w_i * w_j * cov_ij;

            }

        }

        double risk = Math.sqrt(variance);

        double sharpeRatio = Math.sqrt(52)*(expectedReturns  - 0.0018) / risk;

        metrics.setExpectedReturn(expectedReturns);
        metrics.setRisk(risk);
        metrics.setSharpRatio(sharpeRatio);
        metrics.setTimestamp(System.currentTimeMillis());
        metrics.setPortfolioId(w8.getPortfolioId());

        return metrics;

    }

    @Override
    public void processElement2(PortfolioStatsSchema portfolioStatsSchema, KeyedCoProcessFunction<String, WeightSchema, PortfolioStatsSchema, PortfolioMetrics>.Context context, Collector<PortfolioMetrics> collector) throws Exception {
        portfolioStatsState.update(portfolioStatsSchema);

        WeightSchema currentWeight = weightStockState.value();

        if (currentWeight == null){
            return;
        }

        PortfolioMetrics updatedMetrics = calculateMetrics(currentWeight, portfolioStatsSchema);
        Map<Double, Double> updated = new HashMap<>();
        updated.put(updatedMetrics.getExpectedReturn(), updatedMetrics.getRisk());

        if (shouldEmit(updated)) {

            collector.collect(updatedMetrics);
        }

    }

    private boolean shouldEmit(Map<Double, Double> metrics) throws IOException {
        Map<Double, Double> lastEmitted = lastEmittedMetrics.value();

        if (lastEmitted != null && lastEmitted.equals(metrics)) {
            return false;
        } else {
            lastEmittedMetrics.update(metrics);

            return true;
        }
    }
}
```

To make the Sharpe ratio actionable, a threshold is defined. When breached, a notification or downstream trigger is initiated. This is especially relevant for scenarios requiring full automation (e.g. alerts while traveling). The final processing layer is handled in Python, where the Kafka sink containing updated portfolio metrics is consumed.

The Python component includes a consumer class for weights, portfolio statistics, and metrics. Once the Sharpe ratio crosses the predefined threshold, the system invokes PyPortfolioOpt to re-optimize the asset weights using mean-variance optimization with a regularization term to promote diversification. The new weights are then published back to the Kafka weights topic, closing the loop and maintaining a fully reactive system capable of minimizing risk at fine-grained intervals.

```python
from confluent_kafka import Consumer, Producer
from fastavro import schemaless_reader, schemaless_writer
from pypfopt import EfficientFrontier, risk_models, expected_returns, objective_functions
import io
import json
import pandas as pd
import numpy as np
import time

import warnings
warnings.filterwarnings("ignore")

class PortfolioOptimizer:
    def __init__(self):
        self.SHARPE_THRESHOLD = 1.0
        self.MAX_ITERATIONS = 5  
        self.GAMMA_INCREMENT = 0.001  
        self.current_gamma = 0.002
        
        # Load schemas
        with open('avro/PortfolioMetrics.avsc') as f:
            self.portfolio_metrics_schema = json.load(f)
        with open('avro/PortfolioStats.avsc') as f:
            self.portfolio_stats_schema = json.load(f)
        with open('avro/Weights.avsc') as f:
            self.portfolio_weights_schema = json.load(f)
            
        # Kafka setup
        conf_consumer = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'reAllocation-group',
            'auto.offset.reset': 'latest'
        }
        producer_conf = {'bootstrap.servers': 'localhost:9092'}
        
        self.consumer_metrics = Consumer(conf_consumer)
        self.consumer_stats = Consumer(conf_consumer)
        self.producer = Producer(producer_conf)
        
        self.consumer_metrics.subscribe(['portfMetrics'])
        self.consumer_stats.subscribe(['portfUpdatedStats'])
        
        self.latest_stats = None
        self.iteration_count = 0

    def iterative_optimization(self, portfolio_stats):
        
        mean_returns = pd.Series(portfolio_stats['meanReturns'])
        cov_matrix = pd.DataFrame(portfolio_stats['covarianceMatrix'])
        
        best_sharpe = -np.inf
        best_weights = None
        
        for i in range(self.MAX_ITERATIONS):
            try:
                ef = EfficientFrontier(mean_returns, cov_matrix)
                
                
                current_gamma = self.current_gamma + (i * self.GAMMA_INCREMENT)
                ef.add_objective(objective_functions.L2_reg, gamma=current_gamma)
                
                
                if i == 0:
                    weights = ef.max_sharpe()
                elif i == 1:
                    weights = ef.efficient_risk(target_volatility=0.15)
                elif i == 2:
                    weights = ef.efficient_return(target_return=mean_returns.mean() * 1.1)
                else:
                    
                    perturbed_returns = mean_returns * (1 + np.random.normal(0, 0.01, len(mean_returns)))
                    ef_perturbed = EfficientFrontier(perturbed_returns, cov_matrix)
                    ef_perturbed.add_objective(objective_functions.L2_reg, gamma=current_gamma)
                    weights = ef_perturbed.max_sharpe()
                    ef = ef_perturbed
                
                expected_return, annual_volatility, sharpe_ratio = ef.portfolio_performance(verbose=False)
                
                if sharpe_ratio > best_sharpe:
                    best_sharpe = sharpe_ratio
                    best_weights = dict(ef.clean_weights())
                
                
                
                if sharpe_ratio >= self.SHARPE_THRESHOLD:
                    
                    break
                    
            except Exception as e:
                print(f"Optimization iteration {i+1} failed: {e}")
                continue
        
        if best_weights is None:
            raise ValueError("All optimization attempts failed")
            
        print(f"Final Sharpe: {best_sharpe:.4f} after {min(i+1, self.MAX_ITERATIONS)} iterations")
        return best_weights, best_sharpe

    def reallocation(self, portfolio_stats):
        
        try:
            weights, final_sharpe = self.iterative_optimization(portfolio_stats)
            
            new_weight_msg = {
                'weights': weights,
                'timestamp': portfolio_stats['timestamp'],
                'portfolio_id': portfolio_stats['portfolio_id'],
                'expected_sharpe': final_sharpe  
            }
            return new_weight_msg
            
        except Exception as e:
            print(f"Reallocation failed: {e}")
            return None

    def produce_weights(self, weights):
        if weights is None:
            return
        
        latest_weights = self.get_latest_weights()
        
        if latest_weights and latest_weights.get('portfolio_id') == weights["portfolio_id"]:
            if latest_weights.get('weights') == weights["weights"]:
                return
        else:
            self.save_latest_weights(weights)

            
        bytes_writer = io.BytesIO()
        print(weights)
        schemaless_writer(bytes_writer, self.portfolio_weights_schema, weights)
        
        self.producer.produce('Weights', bytes_writer.getvalue(), 
                            weights["portfolio_id"].encode("utf-8"))
        bytes_writer.close()
        self.producer.flush()
    
    def get_latest_weights(self):
        try:
            with open('latestWeights.json', 'r') as f:
                cont = f.read().strip()
                if not cont:
                    return None
                return json.loads(cont)
        except (FileNotFoundError, json.JSONDecodeError):
            return None
    
    def save_latest_weights(self, weights):
        with open('latestWeights.json', 'w') as f:
            json.dump(weights, f, indent=4)

    def run(self):
        try:
            while True:
                
                msg_stats = self.consumer_stats.poll(timeout=1.0)
                if msg_stats and not msg_stats.error():
                    bytes_reader = io.BytesIO(msg_stats.value())
                    self.latest_stats = schemaless_reader(bytes_reader, self.portfolio_stats_schema)
                    
                
                msg_metrics = self.consumer_metrics.poll(timeout=1.0)
                if msg_metrics is None:
                    continue
                if msg_metrics.error():
                    continue

                try:
                    bytes_reader = io.BytesIO(msg_metrics.value())
                    metrics = schemaless_reader(bytes_reader, self.portfolio_metrics_schema)

                    current_sharpe = metrics['sharpRatio']
                    print(f"Current Sharpe: {current_sharpe:.4f}")

                    
                    if (current_sharpe < self.SHARPE_THRESHOLD and self.latest_stats ):

                        new_weights = self.reallocation(self.latest_stats)
                        if new_weights:
                            self.produce_weights(new_weights)
                           
   
                except Exception as e:
                    print(f"Failed processing: {e}")
                    

        except KeyboardInterrupt:
            print("Stopping...")
        finally:
            self.consumer_metrics.close()
            self.consumer_stats.close()

if __name__ == "__main__":
    optimizer = PortfolioOptimizer()
    optimizer.run()
```

All Kafka topics—including weights and metrics—are visualized in real-time using InfluxDB and Grafana. 

```python
public class InfluxDbWeightSink extends RichSinkFunction<WeightSchema> {

    private InfluxDBClient client;
    String token = "m3_SrG3juGe3jm1LMlCLwScIGcN44OBq2AhxYHjCbMwoKvxnVenNeszMk-ak6n7iXGUzDdogqCZNLMeIM415MA==";

    @Override
    public void open(Configuration parameters) {
        client = InfluxDBClientFactory.create("http://localhost:8086", token.toCharArray(), "my-org", "my-bucket");
    }

    @Override
    public void invoke(WeightSchema value, Context context) {

        WriteApiBlocking writeApi = client.getWriteApiBlocking();
        for (Map.Entry<CharSequence, Double> entry : value.getWeights().entrySet()) {
            String ticker = entry.getKey().toString();
            Double weight = entry.getValue();
            System.out.println("value : " + weight);

            if (weight != null) {
                Point point = Point
                        .measurement(ticker)  // Use the ticker as measurement
                        .addTag("Portfolio ID", value.getPortfolioId().toString())
                        .addField("weight", weight)  // Common field name
                        .time(value.getTimestamp(), WritePrecision.MS);

                writeApi.writePoint("weights", "my-org", point);
            }

        }

    }
```

Grafana supports custom alerts (e.g. email notifications) when specific conditions are met. Final outputs can be optionally persisted to time-series databases such as TimeScaleDB (now TigerDB) or MongoDB Timeseries for historical analysis and auditability.

From a DevOps perspective, Kafka and Flink components are containerized and orchestrated via Kubernetes to ensure secure communication and high availability. Grafana and InfluxDB are also deployed as managed services. Although cloud deployment has not been finalized, the project is designed with future AWS deployment in mind, leveraging AWS Managed Kafka, Flink, and EC2-based configurations.
