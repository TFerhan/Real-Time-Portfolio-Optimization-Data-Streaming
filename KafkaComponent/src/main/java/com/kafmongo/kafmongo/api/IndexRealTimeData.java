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
public class IndexRealTimeData {
	
	private static final OkHttpClient client = new OkHttpClient();
	public static JSONArray aralya_data(){
        String url = "https://www.casablanca-bourse.com/api/proxy/fr/api/bourse/dashboard/grouped_index_watch?";

        Headers headers = new Headers.Builder()
                .add("accept", "application/vnd.api+json")
                .add("content-type", "application/vnd.api+json")
                .add("referer","https://www.casablanca-bourse.com/fr/live-market/marche-cash/indices")
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
            JSONArray data = jsonResponse.getJSONArray("data");
            JSONArray indexPrix = new JSONArray();

            for (int i = 0; i < data.length(); i++) {
                JSONObject d = data.getJSONObject(i);
                JSONArray items = d.getJSONArray("items");

                for (int j = 0; j < items.length(); j++) {
                    JSONObject t = items.getJSONObject(j);
                    indexPrix.put(t);
                }
            }



            
            return indexPrix;
        } catch (Exception e){
            e.printStackTrace();
            return new JSONArray();
        }


    }
	
	public static void main(String[] args) {
		JSONArray data = aralya_data();
		System.out.println(data);
	}

}
