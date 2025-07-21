package com.kafmongo.kafmongo.api;

import org.json.JSONObject;
import org.json.JSONArray;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import okhttp3.*;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.springframework.stereotype.Service;


@Service
public class DailyBourseData {
	private static final OkHttpClient client = new OkHttpClient.Builder()
		    .connectTimeout(30, TimeUnit.SECONDS) // Increase connection timeout
		    .readTimeout(30, TimeUnit.SECONDS)    // Increase read timeout
		    .writeTimeout(30, TimeUnit.SECONDS)   // Increase write timeout
		    .build();
	private static final String pathSymbols = "src/main/resources/symbol_ticker/symbols.json";

    

    public static String defineParams(String symbol, String start, String finish, Integer period) {
        try {
            if (symbol == null || symbol.isEmpty()) {
                throw new IllegalArgumentException("Symbol is empty");
            }

            if (period != null) {
                Calendar cal = Calendar.getInstance();
                cal.add(Calendar.MONTH, -period);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                start = sdf.format(cal.getTime());
            }

            if (finish == null) {
                finish = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime());
            }

            if (start == null) {
                start = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime());
            }

            String params = String.format(
                    "fields%%5Binstrument_history%%5D=symbol%%2Ccreated%%2CopeningPrice%%2CcoursCourant%%2ChighPrice%%2ClowPrice%%2CcumulTitresEchanges%%2CcumulVolumeEchange%%2CtotalTrades%%2Ccapitalisation%%2CcoursAjuste%%2CclosingPrice%%2CratioConsolide"
                            + "&fields%%5Binstrument%%5D=symbol%%2ClibelleFR%%2ClibelleAR%%2ClibelleEN%%2Cemetteur_url%%2Cinstrument_url"
                            + "&fields%%5Btaxonomy_term--bourse_emetteur%%5D=name&include=symbol&sort%%5Bdate-seance%%5D%%5Bpath%%5D=created"
                            + "&sort%%5Bdate-seance%%5D%%5Bdirection%%5D=DESC"
                            + "&filter%%5Bfilter-historique-instrument-emetteur%%5D%%5Bcondition%%5D%%5Bpath%%5D=symbol.codeSociete.meta.drupal_internal__target_id"
                            + "&filter%%5Bfilter-historique-instrument-emetteur%%5D%%5Bcondition%%5D%%5Bvalue%%5D=-1"
                            + "&filter%%5Bfilter-historique-instrument-emetteur%%5D%%5Bcondition%%5D%%5Boperator%%5D=%%3D"
                            + "&filter%%5Binstrument-history-class%%5D%%5Bcondition%%5D%%5Bpath%%5D=symbol.codeClasse.field_code"
                            + "&filter%%5Binstrument-history-class%%5D%%5Bcondition%%5D%%5Bvalue%%5D=1"
                            + "&filter%%5Binstrument-history-class%%5D%%5Bcondition%%5D%%5Boperator%%5D=%%3D"
                            + "&filter%%5Bpublished%%5D=1&page%%5Boffset%%5D=0"
                            + "&filter%%5Bfilter-date-start-vh%%5D%%5Bcondition%%5D%%5Bpath%%5D=field_seance_date"
                            + "&filter%%5Bfilter-date-start-vh%%5D%%5Bcondition%%5D%%5Boperator%%5D=%%3E%%3D"
                            + "&filter%%5Bfilter-date-start-vh%%5D%%5Bcondition%%5D%%5Bvalue%%5D=2010-01-01"
                            + "&filter%%5Bfilter-date-end-vh%%5D%%5Bcondition%%5D%%5Bpath%%5D=field_seance_date"
                            + "&filter%%5Bfilter-date-end-vh%%5D%%5Bcondition%%5D%%5Boperator%%5D=%%3C%%3D"
                            + "&filter%%5Bfilter-date-end-vh%%5D%%5Bcondition%%5D%%5Bvalue%%5D=%s"
                            + "&filter%%5Bfilter-historique-instrument-emetteur%%5D%%5Bcondition%%5D%%5Bpath%%5D=symbol.meta.drupal_internal__target_id"
                            + "&filter%%5Bfilter-historique-instrument-emetteur%%5D%%5Bcondition%%5D%%5Boperator%%5D=%%3D"
                            + "&filter%%5Bfilter-historique-instrument-emetteur%%5D%%5Bcondition%%5D%%5Bvalue%%5D=%s"
                            + "&page%%5Boffset%%5D=0&page%%5Blimit%%5D=1000000000000",
                     finish, symbol);

            return params;

        } catch (Exception e) {
            return "";
        }
    }

    public static String constructUrl(String symbol, String start, String finish, Integer period) {
        String updatedParams = defineParams(symbol, start, finish, period);

        if (updatedParams.isEmpty()) {
            return "";
        }

        String baseUrl = "https://www.casablanca-bourse.com/api/proxy/fr/api/bourse_data/instrument_history?";
        return baseUrl + updatedParams;
    }

    public static JSONArray getData(int symbol, String start, String finish, Integer period) {
        try {
        	//change int to String
        	String symbol_str = String.valueOf(symbol);
            String urlString = constructUrl(symbol_str, start, finish, period);
            if (urlString.isEmpty()) {
                throw new IllegalArgumentException("Invalid URL constructed");
            }

            URL url = new URL(urlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = in.readLine()) != null) {
                response.append(line);
            }
            in.close();

            JSONObject jsonResponse = new JSONObject(response.toString());
            JSONArray result = jsonResponse.getJSONArray("data");
            String libelleFR = jsonResponse.getJSONArray("included").getJSONObject(0).getJSONObject("attributes").getString("libelleFR");

            JSONArray data = new JSONArray();
            for (int i = 0; i < result.length(); i++) {
                JSONObject obj = result.getJSONObject(i).getJSONObject("attributes");
                obj.put("libelleFR", libelleFR);
                data.put(obj);
            }

            return data;

        } catch (Exception e) {
            e.printStackTrace();
            return new JSONArray();
        }
    }
    
    public static JSONObject getSymbols() {
    	String url = "https://www.casablanca-bourse.com/_next/data/IbUpocJOxFNFDIMh9atcw/fr/live-market/marche-actions-listing.json?slug=live-market&slug=marche-actions-listing";
    	String url_second =  "https://www.casablanca-bourse.com/api/proxy/fr/api/bourse_data/last_market_watches/action?page%5Blimit%5D=50&page%5Boffset%5D=50";
    	Request request_one = new Request.Builder().url(url).build();
    	Request request_two = new Request.Builder().url(url_second).build();
    	try (Response response_one = client.newCall(request_one).execute();
    			Response response_two = client.newCall(request_two).execute()){
    			if (!response_one.isSuccessful() || !response_two.isSuccessful()){
                    throw new IOException("Erreur dans la recherche de données"+ response_one + " " + response_two);
                }
    			String responseBody_one = response_one.body().string();
    			JSONObject jsonResponse_one = new JSONObject(responseBody_one);
    			JSONObject node = jsonResponse_one.getJSONObject("pageProps").getJSONObject("node");
    			JSONObject marketData_one = new JSONObject(node.getJSONArray("field_vactory_paragraphs")
                        .getJSONObject(2).getJSONObject("field_vactory_component").getString("widget_data"));
    			String responseBody_two = response_two.body().string();
    			JSONObject marketData_two = new JSONObject(responseBody_two);
    			JSONObject data_one = getSymbolInfo(marketData_one);
    			JSONObject data_two = getSymbolInfoP2(marketData_two);
    			//merge the two jsons
    			mergeJSONArrays(data_one, "symbols", data_two.getJSONArray("symbols"));
    			mergeJSONArrays(data_one, "libelles", data_two.getJSONArray("libelles"));
    			mergeJSONArrays(data_one, "tickers", data_two.getJSONArray("tickers"));
    			//save them in a file as json like update a file like a type
    			return data_one;
    			
    			
    			
    		}catch (Exception e){
                e.printStackTrace();
                return new JSONObject();
            }
    	}

    
    public static JSONObject getSymbolInfo(JSONObject marketData) {
        JSONObject data = new JSONObject();

        JSONObject collectionData = marketData.getJSONObject("extra_field").getJSONObject("collection")
                .getJSONObject("data");

        JSONArray makretData = collectionData.getJSONArray("data");
        JSONArray included = collectionData.getJSONArray("included");

        List<Integer> symbols = new ArrayList<>();
        List<String> libelles = new ArrayList<>();
        List<String> tickers = new ArrayList<>();

        for (int i = 0; i < makretData.length(); i++) {
            JSONObject symbolData = makretData.getJSONObject(i);
            int symbol = symbolData.getJSONObject("relationships").getJSONObject("symbol").getJSONObject("data")
                    .getJSONObject("meta").getInt("drupal_internal__target_id");

            String url = extractTickerUrl(included.getJSONObject(i));
            String libelle = included.getJSONObject(i).getJSONObject("attributes").getString("libelleFR");

            symbols.add(symbol);
            libelles.add(libelle);
            tickers.add(url);
        }

        data.put("symbols", new JSONArray(symbols));
        data.put("libelles", new JSONArray(libelles));
        data.put("tickers", new JSONArray(tickers));

        return data;
    }
    
    public static JSONObject getSymbolInfoP2(JSONObject marketData) {
        JSONObject data = new JSONObject();

        // Extract the data and included arrays from marketData
        JSONObject dataField = marketData.getJSONObject("data");
        JSONArray marketDataArray = dataField.getJSONArray("data");
        JSONArray included = dataField.getJSONArray("included");

        // Lists to hold symbols, libelles, and tickers
        List<Integer> symbols = new ArrayList<>();
        List<String> libelles = new ArrayList<>();
        List<String> tickers = new ArrayList<>();

        // Iterate through the market data array and extract the needed information
        for (int i = 0; i < marketDataArray.length(); i++) {
            JSONObject symbolData = marketDataArray.getJSONObject(i);

            // Extract the symbol
            int symbol = symbolData.getJSONObject("relationships").getJSONObject("symbol")
                    .getJSONObject("data").getJSONObject("meta").getInt("drupal_internal__target_id");

            // Extract the URL using regex to capture the last part of the URL
            String url = extractTickerUrl(included.getJSONObject(i));

            // Extract the libelle (label)
            String libelle = included.getJSONObject(i).getJSONObject("attributes").getString("libelleFR");

            // Add extracted values to the respective lists
            symbols.add(symbol);
            libelles.add(libelle);
            tickers.add(url);
        }

        // Put the lists into the final JSON object
        data.put("symbols", new JSONArray(symbols));
        data.put("libelles", new JSONArray(libelles));
        data.put("tickers", new JSONArray(tickers));

        return data;
    }
    
    private static String extractTickerUrl(JSONObject includedItem) {
        String url = includedItem.getJSONObject("attributes").getString("instrument_url");
        Pattern pattern = Pattern.compile("(?<=/)([^/]+)$");
        Matcher matcher = pattern.matcher(url);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }
    
	public static void saveSymbols( JSONObject symbols, String filePath) {
		try {
			
			File file = new File(filePath);
			//check if jsonObject already exists in  the file
			String jsonString = symbols.toString(4);
			
			if (file.exists()) {
                // Step 2: Read the existing content of the file
                String existingContent = new String(Files.readAllBytes(Paths.get(filePath)));

                // Step 3: Compare the existing content with the new JSON object
                if (existingContent.trim().equals(jsonString)) {
                    System.out.println("JSON data is already up-to-date. No changes made.");
                    return; // Exit the function without rewriting
                }
            }
			
			
			
			try (FileWriter fileWriter = new FileWriter(filePath)) {
                fileWriter.write(symbols.toString(4));
                
            }
			System.out.println("JSON data saved/updated successfully in " + filePath);
			
		}catch (IOException e) {
            e.printStackTrace();
            System.err.println("Error while reading or writing the JSON file.");
        }
	}
	
	
	public static Map<String, JSONArray> getAllDataSymbols(String start, String finish, Integer period) {
		
		File file = new File(pathSymbols);
		Map<String, JSONArray> dataSymbols = new HashMap<>();
		if (!file.exists()) {
			JSONObject symbols = getSymbols();
			saveSymbols(symbols, pathSymbols);
			JSONArray DataSymbols = new JSONArray();
            for(int i = 0; i < symbols.getJSONArray("symbols").length(); i++) {
            	int symbol = symbols.getJSONArray("symbols").getInt(i);
            	String ticker = symbols.getJSONArray("tickers").getString(i);
            	JSONArray data = getData(symbol, start, finish,period);
            	dataSymbols.put(ticker, data);
		}
            return dataSymbols;}
		try {
            String content = new String(Files.readAllBytes(Paths.get(pathSymbols)));
            JSONObject symbols = new JSONObject(content);
            JSONArray DataSymbols = new JSONArray();
            for(int i = 0; i < symbols.getJSONArray("symbols").length(); i++) {
            	int symbol = symbols.getJSONArray("symbols").getInt(i);
            	String ticker = symbols.getJSONArray("tickers").getString(i);
            	JSONArray data = getData(symbol, start, finish,period);
            	dataSymbols.put(ticker, data);
            	
            	
            	}
            return dataSymbols;
            }catch (IOException e){
            	e.printStackTrace();
            	return new HashMap<>();
            }
		
	}
	
	public static Map<String, JSONArray> getSymbolData(String ticker, String start, String finish, Integer period){
		File file = new File(pathSymbols);
		Map<String, JSONArray> dataSymbols = new HashMap<>();
		if (!file.exists()) {
			JSONObject symbols = getSymbols();
			saveSymbols(symbols, pathSymbols);
			int indexTicker  = symbols.getJSONArray("tickers").toList().indexOf(ticker);
			int symbol = symbols.getJSONArray("symbols").getInt(indexTicker);
			if (symbol == -1) {
                throw new IllegalArgumentException("Invalid symbol");
            }
			JSONArray data = getData(symbol, start, finish, period);
			dataSymbols.put(ticker, data);
			return dataSymbols;
			
	}
	try {
		String content = new String(Files.readAllBytes(Paths.get(pathSymbols)));
		JSONObject symbols = new JSONObject(content);
		int indexTicker = symbols.getJSONArray("tickers").toList().indexOf(ticker);
		int symbol = symbols.getJSONArray("symbols").getInt(indexTicker);
		if (symbol == -1) {
			throw new IllegalArgumentException("Invalid symbol");
		}
		JSONArray data = getData(symbol, start, finish, period);
		dataSymbols.put(ticker, data);
		return dataSymbols;
	} catch (IOException e) {
		e.printStackTrace();
		return new HashMap<>();
	}
}
	
	private static void mergeJSONArrays(JSONObject target, String key, JSONArray newArray) {
	    if (target.has(key)) {
	        JSONArray existingArray = target.getJSONArray(key);
	        for (int i = 0; i < newArray.length(); i++) {
	            existingArray.put(newArray.get(i)); // Add each element from newArray to existingArray
	        }
	    } else {
	        target.put(key, newArray); // If the key doesn't exist, just add the new array
	    }
	}
   
}