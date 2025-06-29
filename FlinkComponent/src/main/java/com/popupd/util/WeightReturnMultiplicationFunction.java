package com.popupd.util;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class WeightReturnMultiplicationFunction
        extends KeyedCoProcessFunction<String, WeightStockSchema, StockReturn, WeightedReturn> {

    private ValueState<Double> latestWeight;
    private ValueState<Double> latestReturn;
    private ValueState<String> latestReturnTimestamp;


    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Double> weightDescriptor =
                new ValueStateDescriptor<>("latestWeight", Double.class);
        latestWeight = getRuntimeContext().getState(weightDescriptor);
        ValueStateDescriptor<Double> returnDescriptor =
                new ValueStateDescriptor<>("latestReturn", Double.class);
        latestReturn = getRuntimeContext().getState(returnDescriptor);

        ValueStateDescriptor<String> timestampDescriptor =
                new ValueStateDescriptor<>("latestReturnTimestamp", String.class);
        latestReturnTimestamp = getRuntimeContext().getState(timestampDescriptor);

    }

    // When a new weight arrives
    @Override
    public void processElement1(WeightStockSchema weight,
                                Context ctx,
                                Collector<WeightedReturn> out) throws Exception {
        double parsedWeight = Double.parseDouble(weight.getWeight().toString());
        latestWeight.update(parsedWeight);

        // If return is available, emit now
        Double returnVal = latestReturn.value();
        String timestamp = latestReturnTimestamp.value();
        if (returnVal != null && timestamp != null) {
            double result = parsedWeight * returnVal ;
            out.collect(new WeightedReturn(weight.getTicker().toString(), result, timestamp));
        }
    }


    // When a new log return arrives
    @Override
    public void processElement2(StockReturn returnVal,
                                Context ctx,
                                Collector<WeightedReturn> out) throws Exception {
        Double weight = latestWeight.value();
        latestReturn.update(returnVal.getReturnValue());
        latestReturnTimestamp.update(returnVal.getTimestamp());

        if (weight != null) {
            double result = weight * returnVal.getReturnValue();
            out.collect(new WeightedReturn(returnVal.getTicker(), result, returnVal.getTimestamp()));
        }
    }
}

