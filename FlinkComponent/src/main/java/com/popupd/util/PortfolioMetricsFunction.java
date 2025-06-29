package com.popupd.util;

import org.apache.avro.util.Utf8;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
                .mapToDouble(entry -> entry.getValue() * meanReturns.getOrDefault(entry.getKey(), 0.0))
                .sum();


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

        double sharpeRatio = (expectedReturns  - 0.0018) / risk;


        metrics.setExpectedReturn(expectedReturns);
        metrics.setRisk(risk);
        metrics.setSharpRatio(sharpeRatio);
        metrics.setTimestamp(System.currentTimeMillis());

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
