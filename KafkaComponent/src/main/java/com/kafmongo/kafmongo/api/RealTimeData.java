package com.kafmongo.kafmongo.api;

import org.json.JSONArray;
import org.json.JSONObject;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Service;



import org.json.JSONArray;
import org.json.JSONObject;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import java.util.concurrent.ThreadLocalRandom;
import java.util.UUID;


import org.json.JSONArray;
import org.json.JSONObject;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

@Service
public class RealTimeData {

    public JSONArray real_time_data() {
        List<String> tickers = Arrays.asList("TGC", "ATW", "IAM", "BOA", "ADI", "MDP", "AKT", "IMO", "MUT", "RIS", "BCP");

        JSONArray results = new JSONArray();
        String now = OffsetDateTime.now(ZoneOffset.UTC).toString();;

        for (String ticker : tickers) {
            JSONObject obj = new JSONObject();

            // Only include fields defined in BourseData schema
            obj.put("field_date_application", now);
            obj.put("field_closing_price", format(100 + Math.random() * 20));
            obj.put("field_difference", format(-1 + Math.random() * 2));
            obj.put("field_var_veille", format(Math.random() * 3));
            obj.put("field_best_ask_size", format(randomBetween(5, 100)));
            obj.put("field_transact_time", now);
            obj.put("field_cours_cloture", "-");
            obj.put("field_cours_ajuste", format(100 + Math.random() * 20));
            obj.put("field_cumul_titres_echanges", format(randomBetween(1000, 50000)));
            obj.put("field_best_bid_size", format(randomBetween(5, 100)));
            obj.put("field_capitalisation", format(1e9 + Math.random() * 1e9));
            obj.put("field_opening_price", format(100 + Math.random() * 20));
            obj.put("field_total_trades", format(randomBetween(50, 200)));
            obj.put("field_cours_courant", format(100 + Math.random() * 20));
            obj.put("field_best_ask_price", format(100 + Math.random() * 5));
            obj.put("sous_secteur", "Secteur Simulation");
            obj.put("ticker", ticker);
            obj.put("field_instrument_var_year", "-");
            obj.put("field_var_pto", format(-1 + Math.random() * 2));
            obj.put("field_last_traded_time", now);
            obj.put("label", "Mock Label " + ticker);
            obj.put("field_best_bid_price", format(95 + Math.random() * 5));
            obj.put("field_dynamic_reference_price", format(100 + Math.random() * 2));
            obj.put("field_static_reference_price", format(100 + Math.random() * 2));
            obj.put("field_low_price", format(90 + Math.random() * 5));
            obj.put("field_high_price", format(105 + Math.random() * 5));
            obj.put("field_cumul_volume_echange", format(randomBetween(1e6, 10e6)));
            obj.put("field_last_traded_price", format(100 + Math.random() * 20));

            results.put(obj);
        }

        return results;
    }

    private String format(double val) {
        return String.format(Locale.US, "%.10f", val);
    }

    private double randomBetween(double min, double max) {
        return min + Math.random() * (max - min);
    }
}



