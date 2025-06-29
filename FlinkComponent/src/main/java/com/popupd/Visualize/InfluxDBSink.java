//package com.popupd.Visualize;
//
//import com.influxdb.client.InfluxDBClient;
//import com.influxdb.client.InfluxDBClientFactory;
//import com.influxdb.client.WriteApiBlocking;
//import com.influxdb.client.domain.WritePrecision;
//import com.influxdb.client.write.Point;
//import com.popupd.util.StockReturn;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//
//import org.apache.flink.configuration.Configuration;
//
//import java.time.Instant;
//
//public class InfluxDBSink extends RichSinkFunction<StockReturn> {
//    private InfluxDBClient client;
//    String token = "m3_SrG3juGe3jm1LMlCLwScIGcN44OBq2AhxYHjCbMwoKvxnVenNeszMk-ak6n7iXGUzDdogqCZNLMeIM415MA==";
//
//    @Override
//    public void open(Configuration parameters) {
//        client = InfluxDBClientFactory.create("http://localhost:8086", token.toCharArray(), "my-org", "my-bucket");
//    }
//
//    @Override
//    public void invoke(StockReturn value, Context context) {
//        Point point = Point
//                .measurement("log_returns")
//                .addTag("ticker", value.getTicker())
//                .addField("return", value.getReturnValue())
//                .time(Instant.parse(value.getTimestamp()), WritePrecision.MS);
//
//        WriteApiBlocking writeApi = client.getWriteApiBlocking();
//        writeApi.writePoint("my-bucket", "my-org", point);
//    }
//
//    @Override
//    public void close() {
//        client.close();
//    }
//}
