package com.popupd.util;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class LogReturnCalculator extends KeyedProcessFunction<String, BourseData, StockReturn> {

    private transient ValueState<Double> lastPriceState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>("lastPrice", Double.class);
        lastPriceState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(BourseData data, Context ctx, Collector<StockReturn> out) throws Exception {
        Double lastPrice = lastPriceState.value();
        if (lastPrice != null && lastPrice > 0) {
            double logReturn = Math.log(Double.parseDouble(data.getFieldCoursCourant().toString()) / lastPrice);
            //out.collect(new StockReturn(data.getTicker().toString(), logReturn, data.getFieldLastTradedTime()));
        }
        //lastPriceState.update(data.getPrice());
    }
}
