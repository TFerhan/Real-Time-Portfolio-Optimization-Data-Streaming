package com.popupd.Visualize;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.popupd.util.PortfolioMetrics;
import com.popupd.util.StockReturn;
import com.popupd.util.WeightSchema;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.apache.flink.configuration.Configuration;

import java.time.Instant;
import java.util.Map;

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
}
