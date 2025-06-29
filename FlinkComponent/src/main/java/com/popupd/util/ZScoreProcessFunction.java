package com.popupd.util;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;


public class ZScoreProcessFunction extends KeyedProcessFunction<String, BourseData, ZScoreProcessFunction.DriftEvent> {

    // States to hold mean, M2 (for variance), and count
    private transient ValueState<Double> lastPriceState;
    private transient ValueState<ZScoreState> zScoreState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Double> lastPriceDescriptor = new ValueStateDescriptor<>(
                "lastPrice", Double.class
        );
        lastPriceState = getRuntimeContext().getState(lastPriceDescriptor);

        ValueStateDescriptor<ZScoreState> zScoreDescriptor = new ValueStateDescriptor<>(
                "zscoreState", ZScoreState.class
        );
        zScoreState = getRuntimeContext().getState(zScoreDescriptor);
    }


    @Override
    public void processElement(BourseData value, Context ctx, Collector<DriftEvent> out) throws Exception {
        Double lastPrice = lastPriceState.value();
        double currentPrice = Double.parseDouble(value.getFieldLastTradedPrice().toString());

        if (lastPrice != null && currentPrice > 0) {
            double logReturn = Math.log(currentPrice / lastPrice);

            // Update Z-Score state
            ZScoreState zs = zScoreState.value();
            if (zs == null) zs = new ZScoreState();

            double delta = logReturn - zs.mean;
            zs.count++;
            zs.mean += delta / zs.count;
            zs.M2 += delta * (logReturn - zs.mean);

            double std = Math.sqrt(zs.M2 / zs.count);
            double z = (logReturn - zs.mean) / std;

            if (Math.abs(z) > 2) {
                out.collect(new DriftEvent(value.getTicker().toString(), z));
            }

            zScoreState.update(zs);
        }

        lastPriceState.update(currentPrice);
    }

    public static class ZScoreState {
        public double mean = 0.0;
        public double M2 = 0.0;
        public long count = 0;
    }

    public static class DriftEvent {
        private final String stockId;
        private final double zScore;

        public DriftEvent(String stockId, double zScore) {
            this.stockId = stockId;
            this.zScore = zScore;
        }

        @Override
        public String toString() {
            return "DriftEvent{" +
                    "stockId='" + stockId + '\'' +
                    ", zScore=" + zScore +
                    '}';
        }
    }
}
