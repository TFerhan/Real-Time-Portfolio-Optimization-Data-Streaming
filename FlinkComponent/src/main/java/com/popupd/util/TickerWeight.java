package com.popupd.util;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class TickerWeight extends ProcessFunction {
    @Override
    public void processElement(Object o, Context context, Collector collector) throws Exception {

    }
}
