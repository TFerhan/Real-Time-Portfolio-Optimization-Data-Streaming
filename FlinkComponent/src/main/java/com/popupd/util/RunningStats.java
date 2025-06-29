package com.popupd.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class RunningStats implements Serializable {
    public Map<CharSequence, Double> mean = new HashMap<>();
    public Map<CharSequence, Long> count = new HashMap<>();
    public Map<CharSequence, Map<CharSequence, Double>> covariance = new HashMap<>();

    public void update(String ticker, double value) {
        long n = count.getOrDefault(ticker, 0L) + 1;
        double oldMean = mean.getOrDefault(ticker, 0.0);
        double newMean = oldMean + (value - oldMean) / n;
        mean.put(ticker, newMean);
        count.put(ticker, n);

        for (CharSequence other : mean.keySet()) {
            if (other.equals(ticker)) continue;

            double cov = getCov(ticker, other);
            double otherMean = mean.get(other);
            cov += ((value - newMean) * (getLast(other) - otherMean) - cov) / n;

            setCov(ticker, other, cov);
        }
    }

    private double getCov(CharSequence a, CharSequence b) {
        return covariance.getOrDefault(a, new HashMap<>()).getOrDefault(b, 0.0);
    }

    private void setCov(CharSequence a, CharSequence b, double val) {
        covariance.computeIfAbsent(a, k -> new HashMap<>()).put(b, val);
        covariance.computeIfAbsent(b, k -> new HashMap<>()).put(a, val);
    }

    private double getLast(CharSequence ticker) {
        return mean.getOrDefault(ticker, 0.0);
    }
}
