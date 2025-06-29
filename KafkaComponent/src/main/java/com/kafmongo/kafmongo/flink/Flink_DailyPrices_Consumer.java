package com.kafmongo.kafmongo.flink;

import com.kafmongo.kafmongo.utils.BourseData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class Flink_DailyPrices_Consumer {

    /**
     * POJO class to store processed stock data
     * Using String for date to avoid serialization issues with ZonedDateTime
     */
    public static class StockData implements Serializable {
        private static final long serialVersionUID = 1L;

        private String date;           // Store date as ISO-8601 string
        private String ticker;
        private double openingPrice;
        private double closingPrice;
        private double currentPrice;
        private double priceChangeAbsolute;
        private double priceChangePercent;

        // Default constructor required by Flink
        public StockData() {}

        // Constructor with all fields
        public StockData(String date, String ticker, double openingPrice, double closingPrice,
                         double currentPrice, double priceChangeAbsolute, double priceChangePercent) {
            this.date = date;
            this.ticker = ticker;
            this.openingPrice = openingPrice;
            this.closingPrice = closingPrice;
            this.currentPrice = currentPrice;
            this.priceChangeAbsolute = priceChangeAbsolute;
            this.priceChangePercent = priceChangePercent;
        }

        // Getters and setters
        public String getDate() {
            return date;
        }

        public void setDate(String date) {
            this.date = date;
        }

        public String getTicker() {
            return ticker;
        }

        public void setTicker(String ticker) {
            this.ticker = ticker;
        }

        public double getOpeningPrice() {
            return openingPrice;
        }

        public void setOpeningPrice(double openingPrice) {
            this.openingPrice = openingPrice;
        }

        public double getClosingPrice() {
            return closingPrice;
        }

        public void setClosingPrice(double closingPrice) {
            this.closingPrice = closingPrice;
        }

        public double getCurrentPrice() {
            return currentPrice;
        }

        public void setCurrentPrice(double currentPrice) {
            this.currentPrice = currentPrice;
        }

        public double getPriceChangeAbsolute() {
            return priceChangeAbsolute;
        }

        public void setPriceChangeAbsolute(double priceChangeAbsolute) {
            this.priceChangeAbsolute = priceChangeAbsolute;
        }

        public double getPriceChangePercent() {
            return priceChangePercent;
        }

        public void setPriceChangePercent(double priceChangePercent) {
            this.priceChangePercent = priceChangePercent;
        }

        @Override
        public String toString() {
            return String.format(
                    "Stock[%s] | Date: %s | Open: %.2f | Close: %.2f | Current: %.2f | Change: %.2f (%.2f%%)",
                    ticker, date, openingPrice, closingPrice, currentPrice, priceChangeAbsolute, priceChangePercent
            );
        }
    }

    public static void main(String[] args) throws Exception {
        final Configuration flinkConfig = new Configuration();
        flinkConfig.setString("classloader.check-leaked-classloader", "false");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // print the environnement found is it on docker or local
        String envType = System.getProperty("os.name").toLowerCase().contains("linux") ? "Docker" : "Local";
        System.out.println("Flink environment found: " + envType);
        System.out.println("Flink environment created successfully");

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProps.setProperty("group.id", "flink-index-consumer");
        kafkaProps.setProperty("auto.offset.reset", "latest");

        // Create Kafka consumer
        FlinkKafkaConsumer<byte[]> kafkaConsumer = new FlinkKafkaConsumer<>(
                "intraday-stock-prices",
                new KafkaAvroByteArrayDeserializationSchema(),
                kafkaProps
        );

        // Source: Kafka byte array stream
        System.out.println("Adding Kafka source to Flink environment");
        DataStream<byte[]> avroByteStream = env.addSource(kafkaConsumer);

        // First transformation: Convert byte array to BourseData
        System.out.println("Transforming Avro byte array to BourseData objects");
        DataStream<BourseData> bourseDataStream = avroByteStream
                .map(new AvroDeserializerFunction())
                .returns(new GenericTypeInfo<>(BourseData.class));

        // Second transformation: BourseData to StockData POJO
        System.out.println("Processing data into StockData POJOs");
        DataStream<StockData> stockDataStream = bourseDataStream
                .map(new StockDataTransformer())
                .returns(TypeInformation.of(StockData.class));

        // Print results
        stockDataStream.print("Stock Data");

        try {
            env.execute("Flink Stock Data Processing");
            System.out.println("Flink job executed successfully");
        } catch (Exception e) {
            System.err.println("Execution error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Deserializes Kafka records into raw byte arrays
     */
    public static class KafkaAvroByteArrayDeserializationSchema implements KafkaDeserializationSchema<byte[]> {
        @Override
        public boolean isEndOfStream(byte[] nextElement) {
            return false;
        }

        @Override
        public byte[] deserialize(ConsumerRecord<byte[], byte[]> record) {
            return record.value();
        }

        @Override
        public TypeInformation<byte[]> getProducedType() {
            return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
        }
    }

    /**
     * Deserializes Avro byte arrays into BourseData objects
     */
    public static class AvroDeserializerFunction extends RichMapFunction<byte[], BourseData> {
        private transient DatumReader<BourseData> datumReader;
        private transient BinaryDecoder decoder;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            datumReader = new SpecificDatumReader<>(BourseData.class);
        }

        @Override
        public BourseData map(byte[] avroBytes) throws Exception {
            try {
                ByteArrayInputStream inputStream = new ByteArrayInputStream(avroBytes);
                decoder = DecoderFactory.get().binaryDecoder(inputStream, decoder);
                return datumReader.read(null, decoder);
            } catch (Exception e) {
                System.err.println("Error deserializing Avro data: " + e.getMessage());
                throw e;
            }
        }
    }

    /**
     * Transforms BourseData into StockData POJO with proper type conversions
     * Keeps date as String to avoid serialization issues
     */
    public static class StockDataTransformer implements MapFunction<BourseData, StockData> {
        @Override
        public StockData map(BourseData data) throws Exception {
            StockData stockData = new StockData();

            try {
                // Keep date as string to avoid serialization issues
                stockData.setDate(toString(data.getFieldDateApplication()));
                stockData.setTicker(toString(data.getTicker()));

                // Convert numeric values to double
                stockData.setOpeningPrice(parseDouble(toString(data.getFieldOpeningPrice())));
                stockData.setClosingPrice(parseDouble(toString(data.getFieldClosingPrice())));
                stockData.setCurrentPrice(parseDouble(toString(data.getFieldCoursCourant())));
                stockData.setPriceChangeAbsolute(parseDouble(toString(data.getFieldDifference())));
                stockData.setPriceChangePercent(parseDouble(toString(data.getFieldVarVeille())));

                return stockData;
            } catch (Exception e) {
                System.err.println("Error transforming data: " + e.getMessage() +
                        " for ticker: " + (data.getTicker() != null ? data.getTicker().toString() : "unknown"));
                throw e;
            }
        }

        // Helper method to safely convert CharSequence to String
        private String toString(CharSequence seq) {
            return seq != null ? seq.toString() : "";
        }

        // Helper method to safely parse double values
        private double parseDouble(String value) {
            if (value == null || value.isEmpty() || "-".equals(value.trim())) {
                return 0.0;
            }
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e) {
                return 0.0;
            }
        }
    }
}
