package com.popupd.util;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class MaxMinWindowFunction extends ProcessWindowFunction<BourseData, Double, String, GlobalWindow> {

    @Override
    public void process(String key, Context context, Iterable<BourseData> elements, Collector<Double> out) throws Exception {
        double minPrice = Double.MAX_VALUE;
        double maxPrice = Double.MIN_VALUE;

        for (BourseData data : elements) {
            double price = Double.parseDouble(data.getFieldCoursCourant().toString());
            if (price < minPrice) minPrice = price;
            if (price > maxPrice) maxPrice = price;
        }



        double range = maxPrice - minPrice;
        out.collect(range); // emit the difference
        // Or: System.out.println("Max - Min = " + range); if you just want to print
    }
}


