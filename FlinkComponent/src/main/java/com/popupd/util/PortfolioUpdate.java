package com.popupd.util;

import org.apache.avro.util.Utf8;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
            return;
        }


//        globalStats.update(newStats);




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
