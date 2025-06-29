package com.popupd.util;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

public class LogReturnWindowFunction extends ProcessWindowFunction<BourseData, StockReturn, String, TimeWindow> {



    @Override
    public void process(String s, ProcessWindowFunction<BourseData, StockReturn, String, TimeWindow>.Context context, Iterable<BourseData> iterable, Collector<StockReturn> collector) throws Exception {

        Iterator<BourseData> iterator = iterable.iterator();
        if(iterator.hasNext()) {
            Random random = new Random();
            BourseData first = iterator.next();
            double firstPrice = Double.parseDouble(first.getFieldOpeningPrice().toString());

            firstPrice += random.nextGaussian();
            double lastPrice = firstPrice;


            String lastPriceTime = first.getFieldLastTradedTime().toString();
            while (iterator.hasNext()) {
                BourseData current = iterator.next();
                lastPrice = Double.parseDouble(current.getFieldCoursCourant().toString());
                lastPriceTime = current.getFieldLastTradedTime().toString();
            }



            if(firstPrice == 0.0){
                return;
            }

            double logReturn = Math.log(lastPrice / firstPrice);


            if(Double.isNaN(logReturn)) {

            System.out.println("Log return is zero or NaN " + logReturn + " for " + s + ", skipping.");

                return;
            }



            collector.collect(new StockReturn(s, logReturn, lastPriceTime, "portf1"));
        }

        }

    }





