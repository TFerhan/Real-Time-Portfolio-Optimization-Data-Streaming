package com.popupd.util;

import org.apache.flink.api.common.state.*;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.time.Instant;


public class PortfolioStatsUpdater extends KeyedBroadcastProcessFunction<String, StockReturn, PortfolioStatsSchema, PortfolioStatsSchema> {

    private final MapStateDescriptor<String, PortfolioStatsSchema> statsDescriptor;

    public PortfolioStatsUpdater(MapStateDescriptor<String, PortfolioStatsSchema> descriptor) {
        this.statsDescriptor = descriptor;
    }

    @Override
    public void processElement(StockReturn returnEvent, ReadOnlyContext ctx, Collector<PortfolioStatsSchema> out) throws Exception {
        ReadOnlyBroadcastState<String, PortfolioStatsSchema> statsState = ctx.getBroadcastState(statsDescriptor);

        PortfolioStatsSchema stats = statsState.get("portfolio"); // only one in your case

        if (stats != null) {
            // Compute updated stats
            PortfolioStatsSchema updated = updateStats(stats, returnEvent);
            out.collect(updated); // send to Kafka sink

            BroadcastState<String, PortfolioStatsSchema> writableState = (BroadcastState<String, PortfolioStatsSchema>) ctx.getBroadcastState(statsDescriptor);
            writableState.put("portfolio", updated);

            System.out.println("Updated PortfolioStats: " + updated);
        }
    }

    @Override
    public void processBroadcastElement(PortfolioStatsSchema incomingStats, Context ctx, Collector<PortfolioStatsSchema> out) throws Exception {
        BroadcastState<String, PortfolioStatsSchema> state = ctx.getBroadcastState(statsDescriptor);
        state.put("portfolio", incomingStats); // replace or update
        System.out.println("Broadcast PortfolioStats updated: " + incomingStats);
    }

    private PortfolioStatsSchema updateStats(PortfolioStatsSchema stats, StockReturn ret) {
        String symbol = ret.getTicker();
        double retValue = ret.getReturnValue();

        Map<CharSequence, Double> meanReturns = new HashMap<>(stats.getMeanReturns());
        Map<CharSequence, Map<CharSequence, Double>> covMatrix = deepCopyCovMatrix(stats.getCovarianceMatrix());

        int count = 100;  // or compute from timestamp if needed
        int newCount = count + 1;

        // Step 1: Old mean
        double oldMean = meanReturns.getOrDefault(symbol, 0.0);

        // Step 2: Update mean
        double newMean = oldMean + (retValue - oldMean) / newCount;
        meanReturns.put(symbol, newMean);

        // Step 3: Update covariance matrix
        for (CharSequence otherSymbol : meanReturns.keySet()) {
            double otherMean = meanReturns.get(otherSymbol);
            double covOld = covMatrix.getOrDefault(symbol, new HashMap<>())
                    .getOrDefault(otherSymbol, 0.0);

            // Return of other asset — assume 0 if no return yet (or keep previous)
            double retOther = 0.0;
            if (symbol.equals(otherSymbol)) {
                retOther = retValue;  // self-covariance
            } else {
                // estimate using old mean if no current return
                retOther = otherMean;
            }

            double deltaCov = ((retValue - oldMean) * (retOther - otherMean) - covOld) / newCount;
            double covNew = covOld + deltaCov;

            // symmetric matrix
            covMatrix.computeIfAbsent(symbol, k -> new HashMap<>()).put(otherSymbol, covNew);
            covMatrix.computeIfAbsent(otherSymbol, k -> new HashMap<>()).put(symbol, covNew);
        }

        PortfolioStatsSchema updated = new PortfolioStatsSchema();
        updated.setMeanReturns(meanReturns);
        updated.setCovarianceMatrix(covMatrix);
        updated.setTimestamp(Instant.now().toEpochMilli());


        return updated;
    }

    private Map<CharSequence, Map<CharSequence, Double>> deepCopyCovMatrix(Map<CharSequence, Map<CharSequence, Double>> original) {
        Map<CharSequence, Map<CharSequence, Double>> copy = new HashMap<>();
        for (Map.Entry<CharSequence, Map<CharSequence, Double>> entry : original.entrySet()) {
            Map<CharSequence, Double> innerCopy = new HashMap<>(entry.getValue());
            copy.put(entry.getKey(), innerCopy);
        }
        return copy;
    }


}

