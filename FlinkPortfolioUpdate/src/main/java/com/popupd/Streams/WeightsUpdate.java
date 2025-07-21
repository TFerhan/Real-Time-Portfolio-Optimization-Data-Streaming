package com.popupd.Streams;

//import com.popupd.Visualize.InfluxDBSink;
import com.popupd.Visualize.InfluxDBPortfolioMetricsSink;
import com.popupd.Visualize.InfluxDbWeightSink;
import com.popupd.util.BourseDataDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import com.popupd.util.BourseData;
import com.popupd.util.*;

import java.time.Duration;

public class WeightsUpdate {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


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




        DataStreamSource<BourseData> pricesStream = (DataStreamSource<BourseData>) env.fromSource(
                priceSource,
                watermarkPriceStrategy,
                "Kafka Prices Source"
        ).returns(BourseData.class);



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


        w8Stream.addSink(new InfluxDbWeightSink());




        KeyedStream<PortfolioStatsSchema, String> keyedStatsStream = portfolioStatsStream
                .keyBy(data -> data.getPortfolioId().toString());

        KeyedStream<BourseData, String> keyedPricesStream = pricesStream
                .keyBy(data -> data.getTicker().toString());



        DataStream<StockReturn> logReturnsStream = keyedPricesStream.window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .process(new LogReturnWindowFunction());



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



        DataStream<PortfolioMetrics> portfolioMetric = w8Stream
                .keyBy(WeightSchema::getPortfolioId)
                .connect(updatedPortfolioStats.keyBy(PortfolioStatsSchema::getPortfolioId))
                .process(new PortfolioMetricsFunction());

        portfolioMetric.addSink(new InfluxDBPortfolioMetricsSink());



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





















        //pricesStream.print();
        //weightStream.print();

        env.execute("Weights update job");


















    }
}
