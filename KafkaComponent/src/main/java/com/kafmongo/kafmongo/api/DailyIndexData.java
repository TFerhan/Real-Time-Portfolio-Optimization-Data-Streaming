package com.kafmongo.kafmongo.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;

@Service
public class DailyIndexData {

    private static final OkHttpClient client = new OkHttpClient();
    private static final String pathSymbols = "src/main/resources/IndexSymbol/symbolsindex.json";
    
    public static void main(String[] args) {
        // Example usage
        try {
            String start = null;
            String finish = null;
            Integer period = 1;
            //JSONArray indices = fetchSymbolsIndex();
            //System.out.println(indices);
            //saveToJsonFile(indices);
            Map<String, JSONArray> data = getDataOfAllIndex(start, finish, period);
            System.out.println(data);
            System.out.println("Data Size"+data.size());

            //List<Map<String, Object>> dataSymbols = getDataOfAllIndex(start, finish, period);
            //System.out.println("SIZE : "+dataSymbols.size());

            //System.out.println(dataSymbols);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String defineParams(String symbol, String start, String finish, Integer period) {
        try {
            // Validate symbol
            if (symbol == null || symbol.isEmpty()) {
                System.out.println("Symbol vide");
                return "";
            }

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Calendar cal = Calendar.getInstance();

            // Handle period: subtract months from today's date
            if (period != null) {
                cal.add(Calendar.MONTH, -period);
                start = sdf.format(cal.getTime());
            }

            // Default values for start and finish
            if (finish == null) {
                finish = sdf.format(Calendar.getInstance().getTime());
            }
            if (start == null) {
                start = sdf.format(Calendar.getInstance().getTime());
            }

            // Correctly format the URL parameters
            String params = String.format(
                    "fields%%5Bbourse_data--index_history%%5D=drupal_internal__id%%2Cfield_seance_date%%2Cfield_index_value" +
                            "&sort%%5Bdate-seance%%5D%%5Bpath%%5D=field_seance_date" +
                            "&sort%%5Bdate-seance%%5D%%5Bdirection%%5D=DESC" +
                            "&page%%5Boffset%%5D=0&page%%5Blimit%%5D=25000" +
                            "&filter%%5Bpublished%%5D=1" +
                            "&filter%%5Bfilter-date-start-vh-select%%5D%%5Bcondition%%5D%%5Bpath%%5D=field_seance_date" +
                            "&filter%%5Bfilter-date-start-vh-select%%5D%%5Bcondition%%5D%%5Boperator%%5D=%%3E%%3D" +
                            "&filter%%5Bfilter-date-start-vh-select%%5D%%5Bcondition%%5D%%5Bvalue%%5D=%s" + // finish
                            "&filter%%5Bindex-historique-code%%5D%%5Bcondition%%5D%%5Bpath%%5D=field_index_code.tid" +
                            "&filter%%5Bindex-historique-code%%5D%%5Bcondition%%5D%%5Boperator%%5D=%%3D" +
                            "&filter%%5Bindex-historique-code%%5D%%5Bcondition%%5D%%5Bvalue%%5D=512335" +
                            "&filter%%5Bfilter-date-start-vh-select%%5D%%5Bcondition%%5D%%5Bpath%%5D=field_seance_date" +
                            "&filter%%5Bfilter-date-start-vh-select%%5D%%5Bcondition%%5D%%5Boperator%%5D=%%3E%%3D" +
                            "&filter%%5Bfilter-date-start-vh-select%%5D%%5Bcondition%%5D%%5Bvalue%%5D=%s" + // start
                            "&filter%%5Bindex-historique-code%%5D%%5Bcondition%%5D%%5Bpath%%5D=field_index_code.tid" +
                            "&filter%%5Bindex-historique-code%%5D%%5Bcondition%%5D%%5Boperator%%5D=%%3D" +
                            "&filter%%5Bindex-historique-code%%5D%%5Bcondition%%5D%%5Bvalue%%5D=%s", // symbol
                    finish, start, symbol);

            return params;

        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    public static String constructUrl(String symbol, String start, String finish, Integer period) {
        String updatedParams = defineParams(symbol, start, finish, period);

        //System.out.println("params : "+updatedParams);

        if (updatedParams.isEmpty()) {
            return "";
        }

        String baseUrl = "https://www.casablanca-bourse.com/api/proxy/fr/api/bourse_data/index_history?";
        return baseUrl + updatedParams;
    }

    public static List<Map<String, Object>> getAttributesIndex(Map<String, Object> response) {
        List<Map<String, Object>> data = new ArrayList<>();

        if (response.containsKey("data")) {
            List<Map<String, Object>> dataList = (List<Map<String, Object>>) response.get("data");

            for (Map<String, Object> item : dataList) {
                if (item.containsKey("attributes")) {
                    Map<String, Object> attributes = (Map<String, Object>) item.get("attributes");
                    data.add(attributes);
                }
            }
        }

        return data;
    }
    
    public static Map<String, JSONArray> getDataIndex(String index, String start, String finish, Integer period) {
        String symbol = getSymbolFromIndex(index);
        String urlString = constructUrl(symbol, start, finish, period);

        if (urlString.isEmpty()) {
            return Collections.emptyMap();
        }

        try {
            Request request = new Request.Builder()
                .url(urlString)
                .header("Accept", "application/json")
                .build();

            try (Response response = client.newCall(request).execute()) {
                if (!response.isSuccessful()) {
                    throw new RuntimeException("Failed : HTTP error code : " + response.code());
                }

                ObjectMapper objectMapper = new ObjectMapper();
                Map<String, Object> responseData = objectMapper.readValue(response.body().string(), Map.class);

                // Get attributes
                List<Map<String, Object>> data = getAttributesIndex(responseData);

                // Transform and store in JSONArray
                JSONArray jsonArray = new JSONArray();
                for (Map<String, Object> entry : data) {
                    JSONObject jsonObject = new JSONObject(entry);
                    jsonObject.put("index", index);
                    jsonObject.put("symbol", symbol);
                    jsonObject.remove("drupal_internal__id");

                    // Rename fields
                    if (jsonObject.has("field_seance_date")) {
                        jsonObject.put("date", jsonObject.remove("field_seance_date"));
                    }
                    if (jsonObject.has("field_index_value")) {
                        jsonObject.put("value", jsonObject.remove("field_index_value"));
                    }

                    jsonArray.put(jsonObject);
                }

                // Store in the map
                Map<String, JSONArray> result = new HashMap<>();
                result.put(index, jsonArray);
                return result;
            }

        } catch (IOException e) {
            e.printStackTrace();
            return Collections.emptyMap();
        }
    }

    public static JSONObject getSymbolsIndex() {
        String url = "https://www.casablanca-bourse.com/_next/data/IbUpocJOxFNFDIMh9atcw/fr/historique-des-indices.json?slug=historique-des-indices";

        Request request = new Request.Builder().url(url).build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Erreur dans la recherche de données: " + response);
            }

            String responseBody = response.body().string();
            JSONObject jsonResponse = new JSONObject(responseBody);

            // Extraire la structure JSON désirée
            JSONObject pageProps = jsonResponse.getJSONObject("pageProps");
            JSONObject node = pageProps.getJSONObject("node");
            JSONArray fieldVactoryParagraphs = node.getJSONArray("field_vactory_paragraphs");

            // Récupérer le widget_data
            String widgetDataString = fieldVactoryParagraphs
                    .getJSONObject(1) // Accès au deuxième élément
                    .getJSONObject("field_vactory_component")
                    .getString("widget_data");

            // Convertir widget_data en JSON
            JSONObject widgetData = new JSONObject(widgetDataString);
            JSONArray indices = widgetData.getJSONArray("components").getJSONObject(0).getJSONArray("indices");

            // Construire un objet JSON sans "uuid"
            JSONArray cleanedIndices = new JSONArray();
            for (int i = 0; i < indices.length(); i++) {
                JSONObject indexObject = indices.getJSONObject(i);
                indexObject.remove("uuid"); // Supprime la clé "uuid"
                cleanedIndices.put(indexObject);
            }

            // Créer l'objet JSON final
            JSONObject result = new JSONObject();
            result.put("indices", cleanedIndices);
            return result;

        } catch (Exception e) {
            e.printStackTrace();
            return new JSONObject();
        }
    }

    public static String getSymbolFromIndex(String index) {
        try {
            // Charger le fichier JSON local
            JSONArray localData = loadLocalJson();

            // Vérifier si l'index existe déjà
            for (int i = 0; i < localData.length(); i++) {
                JSONObject obj = localData.getJSONObject(i);
                if (obj.getString("label").equalsIgnoreCase(index)) {
                    return obj.getString("id"); // Retourne le symbole trouvé
                }
            }

            // Si l'index n'est pas trouvé, appeler l'API
            JSONObject symbolsData = getSymbolsIndex();
            JSONArray symbols = symbolsData.getJSONArray("indices");

            // Chercher l'index dans les nouvelles données
            for (int i = 0; i < symbols.length(); i++) {
                JSONObject obj = symbols.getJSONObject(i);
                if (obj.getString("label").equalsIgnoreCase(index)) {
                    String symbolId = obj.getString("id");

                    // Ajouter la nouvelle entrée au fichier JSON local
                    localData.put(obj);
                    saveLocalJson(localData);

                    return symbolId;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null; // Retourne null si l'index n'est pas trouvé
    }

    private static JSONArray loadLocalJson() {
        try {
            if (!Files.exists(Paths.get(pathSymbols))) {
                return new JSONArray();
            }
            String content = new String(Files.readAllBytes(Paths.get(pathSymbols)));
            return new JSONArray(content);
        } catch (IOException e) {
            e.printStackTrace();
            return new JSONArray();
        }
    }

    private static void saveLocalJson(JSONArray data) {
        try (FileWriter file = new FileWriter(pathSymbols)) {
            file.write(data.toString(4)); // Beautify JSON output
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static JSONArray fetchSymbolsIndex() throws IOException {
        Request request = new Request.Builder()
            .url("https://www.casablanca-bourse.com/_next/data/IbUpocJOxFNFDIMh9atcw/fr/historique-des-indices.json?slug=historique-des-indices")
            .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Failed to fetch data: " + response);
            }

            String responseBody = response.body().string();
            JSONObject jsonResponse = new JSONObject(responseBody);
            
            String jsonString = jsonResponse.getJSONObject("pageProps")
                .getJSONObject("node")
                .getJSONArray("field_vactory_paragraphs")
                .getJSONObject(1)
                .getJSONObject("field_vactory_component")
                .getString("widget_data");

            JSONObject parsedData = new JSONObject(jsonString);
            JSONArray indices = parsedData.getJSONArray("components").getJSONObject(0).getJSONArray("indices");

            JSONArray formattedIndices = new JSONArray();
            for (int i = 0; i < indices.length(); i++) {
                JSONObject index = indices.getJSONObject(i);
                JSONObject formattedIndex = new JSONObject();
                formattedIndex.put("id", index.getString("id"));
                formattedIndex.put("label", index.getString("label"));
                formattedIndices.put(formattedIndex);
            }
            return formattedIndices;
        }
    }

    public static void saveToJsonFile(JSONArray jsonArray) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);

        File file = Paths.get(pathSymbols).toFile();
        file.getParentFile().mkdirs(); // Ensure the directories exist
        Files.write(file.toPath(), mapper.writeValueAsBytes(jsonArray.toList()));
    }

    public static Map<String, JSONArray> getDataOfAllIndex(String start, String finish, Integer period) {
        Map<String, JSONArray> allIndexData = new HashMap<>();

        try {
            // Load all indices from the JSON file
            JSONArray indices = loadLocalJson();

            // If local file is empty, fetch from API
            if (indices.length() == 0) {
                indices = fetchSymbolsIndex();
                saveLocalJson(indices);
            }

            // Iterate through each index and collect data
            for (int i = 0; i < indices.length(); i++) {
                JSONObject indexObj = indices.getJSONObject(i);
                String label = indexObj.getString("label"); // e.g., "MASI"
                String id = indexObj.getString("id");       // e.g., "001"

                // Get data for this index
                Map<String, JSONArray> indexData = getDataIndex(label, start, finish, period);

                // Add to combined result
                if (indexData != null && !indexData.isEmpty()) {
                    allIndexData.putAll(indexData);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return allIndexData;
    }
}