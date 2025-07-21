package com.kafmongo.kafmongo.api;


import okhttp3.*;
import org.json.*;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.time.ZoneOffset;
import org.springframework.stereotype.Service;

@Service
public class DataFetchService {

    private static final OkHttpClient client = new OkHttpClient();


    public JSONArray real_time_data(){
        String url = "http://www.casablanca-bourse.com/api/proxy/fr/api/bourse/dashboard/ticker?marche=59&class[0]=25";

        Headers headers = new Headers.Builder()
                .add("accept", "application/vnd.api+json")
                .add("content-type", "application/vnd.api+json")
                .add("referer", "https://www.casablanca-bourse.com/fr/live-market/marche-actions-groupement")
                .add("sec-ch-ua", "\"Chromium\";v=\"130\", \"Google Chrome\";v=\"130\", \"Not?A_Brand\";v=\"99\"")
                .add("sec-ch-ua-mobile", "?0")
                .add("sec-ch-ua-platform", "\"Windows\"")
                .add("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36")
                .build();

        Request request = new Request.Builder().url(url).headers(headers).build();

        try (Response response = client.newCall(request).execute()){
            if (!response.isSuccessful()){
                throw new IOException("Erreur dans la recherche de données"+ response);
            }
            String responseBody = response.body().string();
            JSONObject jsonResponse = new JSONObject(responseBody);
            JSONArray values = jsonResponse.getJSONObject("data").getJSONArray("values");


            JSONArray cleanedValues = nettoyer(values);
            return cleanedValues;
        } catch (Exception e){
            e.printStackTrace();
            return new JSONArray();
        }


    }

    private static JSONArray nettoyer(JSONArray values){
        List<String> colonnes_supp = Arrays.asList("id", "uuid", "type", "code", "published",
                "is_flagged", "metatag", "internal_user",
                "redirect_alias", "field_etat_cot_val",
                "field_pto", "field_last_transactions",
                "has_alert", "field_market_id", "field_symbol","field_ratio_ajustement","field_ratio_consolide");

        JSONArray nett = new JSONArray();

        for (int i =0; i < values.length();i++){
            JSONObject obj = values.getJSONObject(i);

            for (String col: colonnes_supp){
                obj.remove(col);
            }

            for (String key : obj.keySet()){
                Object valeur = obj.get(key);
                if (valeur instanceof JSONArray){
                    obj.put(key, "0");
                }
            }





            changer_type(obj);



            nett_dates(obj);

            nett.put(obj);
        }
        return nett;
    }

    private static void nett_dates(JSONObject obj) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

        List<String> dateFields = Arrays.asList("field_date_application", "field_last_traded_time", "field_transact_time");

        for (String field : dateFields) {
            if (obj.has(field)) {
                try {
                    String value = obj.getString(field);
                    if ("0".equals(value) || value.isEmpty()) {
                        Instant fallbackInstant = Instant.parse("1999-01-01T00:00:00Z");
                        obj.put(field, fallbackInstant.toString());
                    } else {
                        LocalDateTime date = LocalDateTime.parse(value, formatter);
                        Instant instant = date.toInstant(ZoneOffset.UTC);
                        obj.put(field, instant.toString());
                    }
                } catch (Exception e) {
                    Instant fallbackInstant = Instant.parse("1999-01-01T00:00:00Z");
                    obj.put(field, fallbackInstant.toString());
                }
            }
        }
    }


    private static void changer_type(JSONObject obj){

        List<String> col_float = Arrays.asList("field_best_ask_price", "field_best_ask_size", "field_best_bid_price",
                "field_best_bid_size", "field_capitalisation", "field_closing_price",
                "field_cours_ajuste", "field_cours_courant",
                "field_cumul_titres_echanges", "field_cumul_volume_echange",
                "field_difference", "field_dynamic_reference_price", "field_high_price",
                "field_last_traded_price", "field_low_price", "field_opening_price",
                "field_static_reference_price", "field_total_trades", "field_var_pto",
                "field_var_veille");

        for (String col : col_float){
            if (obj.has(col)){
                try{
                    obj.put(col, obj.getString(col));
                } catch (Exception e){
                    obj.put(col, "0.0");
                }
            }
        }


    }


}
