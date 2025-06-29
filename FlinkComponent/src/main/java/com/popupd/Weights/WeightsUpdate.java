package com.popupd.Weights;

//import com.popupd.Visualize.InfluxDBSink;
import com.popupd.util.BourseDataDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import com.popupd.util.BourseData;
import com.popupd.util.*;

import java.time.Duration;
import java.util.UUID;

public class WeightsUpdate {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.enableCheckpointing(60000); // every 60 seconds

//        String checkpointPath = "file:///C:/Users/carin/my-flink-checkpoint";
//        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(checkpointPath, true);
//        env.setStateBackend(rocksDBStateBackend);

        // Configure watermark interval
        env.getConfig().setAutoWatermarkInterval(1000);
        env.setParallelism(3);

        OffsetsInitializer latest = OffsetsInitializer.latest();
        OffsetsInitializer earliest = OffsetsInitializer.earliest();

        KafkaSource<BourseData> priceSource = KafkaSource.<BourseData>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("intraday-stock-prices")
                .setGroupId("flink-prices-group")
                .setStartingOffsets(latest)
                .setDeserializer(new BourseDataDeserializationSchema())
                .build();

//        KafkaSource<WeightStockSchema> weightSource = KafkaSource.<WeightStockSchema>builder()
//                .setBootstrapServers("localhost:9092")
//                .setTopics("stockWeights")
//                .setGroupId("flink-weights-"+ UUID.randomUUID().toString())
//                .setStartingOffsets(OffsetsInitializer.latest())
//                .setDeserializer(new WeightStockDeserializationSchema())
//                .build();

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


        WatermarkStrategy<PortfolioStatsSchema> watermarkStatsStrategy = WatermarkStrategy
                .<PortfolioStatsSchema>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp());


        WatermarkStrategy<WeightStockSchema> watermarkWeightStrategy = WatermarkStrategy
                .<WeightStockSchema>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner((event, timestamp) -> {
                    String timestampStr = event.getTime().toString();

                    // Check if the timestamp contains a time zone information, if not, add 'Z' for UTC
                    if (!timestampStr.contains("T") || !timestampStr.contains("Z")) {
                        // Assuming the timestamp should be in UTC if it does not have a time zone
                        timestampStr = timestampStr.replace(" ", "T") + "Z";  // Adding 'Z' to indicate UTC
                    }

                    // Parse using OffsetDateTime
                    return java.time.OffsetDateTime.parse(timestampStr)
                            .toInstant()
                            .toEpochMilli();
                });



        //WatermarkStrategy<PortfolioStatsSchema> watermarkPortfolioStatsStrategy = WatermarkStrategy
                //.<PortfolioStatsSchema>forBoundedOutOfOrderness(Duration.ofMinutes(1))  // max lateness
                //.withTimestampAssigner((event, timestamp) -> event.getTimestamp());

        DataStreamSource<BourseData> pricesStream = (DataStreamSource<BourseData>) env.fromSource(
                priceSource,
                watermarkPriceStrategy,
                "Kafka Prices Source"
        ).returns(BourseData.class);

        pricesStream.print();

//        DataStreamSource<WeightStockSchema> weightStream = (DataStreamSource<WeightStockSchema>) env.fromSource(
//                weightSource,
//                watermarkWeightStrategy,
//                "Kafka Weights Source"
//        ).returns(WeightStockSchema.class);

        DataStreamSource<PortfolioStatsSchema> portfolioStatsStream = (DataStreamSource<PortfolioStatsSchema>) env.fromSource(
                portfolioStatsSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Portfolio Stats Source"
        ).returns(PortfolioStatsSchema.class);

        DataStreamSource<WeightSchema> w8Stream = (DataStreamSource<WeightSchema>) env.fromSource(
                weightS,
                WatermarkStrategy.noWatermarks(),
                "Weights Source"
        ).returns(WeightSchema.class);

//        w8Stream.print();



//        SingleOutputStreamOperator<Tuple2<String, PortfolioStatsSchema>> keyedStatsStream =
//                portfolioStatsStream
//                        .map(stats -> Tuple2.of("portfolio", stats))
//                        .returns(Types.TUPLE(Types.STRING, TypeInformation.of(PortfolioStatsSchema.class)))
//                        .keyBy(t -> t.f0).iterate();


//        portfolioStatsStream.print();

//        ValueStateDescriptor<PortfolioStatsSchema> portfolioStatsDescriptor =
//                new ValueStateDescriptor<>("portfolio-stats",TypeInformation.of(PortfolioStatsSchema.class));
//
//        BroadcastStream<PortfolioStatsSchema> broadcastPortfolioStats = portfolioStatsStream
//                .broadcast(portfolioStatsDescriptor);


        KeyedStream<PortfolioStatsSchema, String> keyedStatsStream = portfolioStatsStream
                .keyBy(data -> data.getPortfolioId().toString());

        KeyedStream<BourseData, String> keyedPricesStream = pricesStream
                .keyBy(data -> data.getTicker().toString());

//        KeyedStream<WeightStockSchema, String> keyedWeightsStream = weightStream
//                .keyBy(data -> data.getTicker().toString());

        DataStream<StockReturn> logReturnsStream = keyedPricesStream.window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .process(new LogReturnWindowFunction());









//        logReturnsStream.print();

//        DataStream<PortfolioStatsSchema> updatedPortfolioStats = logReturnsStream
//                .connect(keyedStatsStream)
//                .process(new PortfolioUpdate()).setParallelism(1);

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
//
//
        updatedPortfolioStats.sinkTo(kafkaSink);

//        updatedPortfolioStats.print();


        DataStream<PortfolioMetrics> portfolioMetric = w8Stream
                .keyBy(WeightSchema::getPortfolioId)
                .connect(updatedPortfolioStats.keyBy(PortfolioStatsSchema::getPortfolioId))
                .process(new PortfolioMetricsFunction());





        KafkaSink<PortfolioMetrics> portfMetricSink = KafkaSink.<PortfolioMetrics>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("portfMetrics")
                                .setValueSerializationSchema(new PortfolioMetricsAvroSerialization())
                                .build()
                )
                .build();

        portfolioMetric.sinkTo(portfMetricSink)
                .name("Kafka Portfolio Metrics Sink");


        portfolioMetric.print();



//        updatedPortfolioStats.print();

//        DataStream<WeightedReturn> weightedReturns =  keyedWeightsStream
//                .connect(logReturnsStream.keyBy(StockReturn::getTicker))
//                .process(new WeightReturnMultiplicationFunction()).setParallelism(1);
//
//
//
//
//        DataStream<PortfolioCumulativeReturn> portfolioReturn = weightedReturns
//                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .process(new CumulativeWeightedReturnFunction()).setParallelism(1);


                ;

//        portfolioReturn.print();




        //keyedPricesStream
                //.window(SlidingEventTimeWindows.of(Time.seconds(15), Time.seconds(5))) // Slide every minute
                //.process(new LogReturnWindowFunction())
                //.addSink(new InfluxDBSink());












        //pricesStream.print();
        //weightStream.print();

        env.execute("Weights update job");


















    }
}
