package com.popupd.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import com.popupd.util.PortfolioCumulativeReturn;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CumulativeWeightedReturnFunction
        extends ProcessAllWindowFunction<WeightedReturn, PortfolioCumulativeReturn, TimeWindow> {

    @Override
    public void process(ProcessAllWindowFunction<WeightedReturn, PortfolioCumulativeReturn, TimeWindow>.Context context, Iterable<WeightedReturn> iterable, Collector<PortfolioCumulativeReturn> collector) throws Exception {
        Map<String, WeightedReturn> weightedReturnsMap = new HashMap<>();

        for(WeightedReturn weRt : iterable){
            String ticker = weRt.getTicker();
            weightedReturnsMap.put(ticker, weRt);

        }

        double totalReturn = 0.0;
        String timestamp = "";

        for (WeightedReturn wr : weightedReturnsMap.values()){
            totalReturn += wr.getWeightedReturn();
            timestamp = wr.getTimestamp();
        }

        collector.collect(new PortfolioCumulativeReturn(totalReturn, timestamp));
    }
}
