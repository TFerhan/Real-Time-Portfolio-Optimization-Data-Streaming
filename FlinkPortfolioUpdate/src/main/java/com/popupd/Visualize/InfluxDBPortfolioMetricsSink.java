package com.popupd.Visualize;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.popupd.util.PortfolioMetrics;
import com.popupd.util.StockReturn;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.apache.flink.configuration.Configuration;

import java.time.Instant;

public class InfluxDBPortfolioMetricsSink extends RichSinkFunction<PortfolioMetrics> {
    private InfluxDBClient client;
    String token = "m3_SrG3juGe3jm1LMlCLwScIGcN44OBq2AhxYHjCbMwoKvxnVenNeszMk-ak6n7iXGUzDdogqCZNLMeIM415MA==";

    @Override
    public void open(Configuration parameters) {
        client = InfluxDBClientFactory.create("http://localhost:8086", token.toCharArray(), "my-org", "my-bucket");
    }

    @Override
    public void invoke(PortfolioMetrics value, Context context) {
        Point sharpeRatio = Point
                .measurement("Sharpe_Ratio")
                .addTag("Portfolio ID", value.getPortfolioId().toString())
                .addField("Sharpe Ratio", value.getSharpRatio())
                .time(value.getTimestamp(), WritePrecision.MS);

        Point risk = Point
                .measurement("Risk")
                .addTag("Portfolio ID", value.getPortfolioId().toString())
                .addField("Risk", value.getRisk())
                .time(value.getTimestamp(), WritePrecision.MS);

        Point expectedReturn = Point
                .measurement("Expected_Return")
                .addTag("Portfolio ID", value.getPortfolioId().toString())
                .addField("Expected Return", value.getExpectedReturn())
                .time(value.getTimestamp(), WritePrecision.MS);

        WriteApiBlocking writeApi = client.getWriteApiBlocking();
        writeApi.writePoint("my-bucket", "my-org", sharpeRatio);
        writeApi.writePoint("my-bucket", "my-org", risk);
        writeApi.writePoint("my-bucket", "my-org", expectedReturn);

    }

    @Override
    public void close() {
        client.close();
    }
}
