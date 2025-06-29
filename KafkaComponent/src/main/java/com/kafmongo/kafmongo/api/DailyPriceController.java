package com.kafmongo.kafmongo.api;

import com.kafmongo.kafmongo.Service.DailyPriceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.json.JSONArray;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;

@RestController
@RequestMapping("/api/dailyprices")  // Base URL for the API
public class DailyPriceController {

    @Autowired
    private DailyPriceService dailyPriceService;

    // Endpoint to get all daily prices
    @GetMapping("/all")
    public ResponseEntity<String> getAllDailyPrices() {
        JSONObject result = dailyPriceService.getAllDailyPrices();
        return ResponseEntity.ok(result.toString());
    }

    // Endpoint to get data by ticker (e.g., stock symbol) and date range
    @GetMapping("/{ticker}")
    public ResponseEntity<String> getByTicker(
            @PathVariable String ticker,
            @RequestParam String start,
            @RequestParam String finish) {
        JSONObject result = dailyPriceService.getByTicker(ticker, start, finish);
        
        return ResponseEntity.ok(result.toString());
    }

    // Endpoint to get weekly returns
    @GetMapping("/weekly/{ticker}")
    public ResponseEntity<String> getWeeklyDataReturns(
            @PathVariable String ticker,
            @RequestParam String start,
            @RequestParam String finish) {
        JSONObject result = dailyPriceService.getWeeklyDataReturns(ticker, start, finish);
        return ResponseEntity.ok(result.toString());
    }

    // Endpoint to get monthly returns
    @GetMapping("/monthly/{ticker}")
    public ResponseEntity<String> getMonthlyDataReturns(
            @PathVariable String ticker,
            @RequestParam String start,
            @RequestParam String finish) {
        JSONObject result = dailyPriceService.getMonthlyDataReturns(ticker, start, finish);
        return ResponseEntity.ok(result.toString());
    }

    // Endpoint to get daily log returns
    @GetMapping("/daily-log-returns/{ticker}")
    public ResponseEntity<String> getDailyLogReturns(
            @PathVariable String ticker,
            @RequestParam String start,
            @RequestParam String finish) {
        JSONObject result = dailyPriceService.getDailyLogReturns(ticker, start, finish);
        return ResponseEntity.ok(result.toString());
    }
    
    @GetMapping("/all-daily-log-returns")
	public ResponseEntity<String> getAllDailyLogReturns(@RequestParam String start, @RequestParam String finish) {
		JSONObject result = dailyPriceService.getAllDailyLogReturns(start, finish);
		return ResponseEntity.ok(result.toString());
	}
    
	// Endpoint to get liquidity ratios by ticker
	@GetMapping("/liquidity-ratios-volume")
	public ResponseEntity<String> getLiquidityRatiosByTicker() {
		JSONObject result = dailyPriceService.getLiquidityVolume();
		return ResponseEntity.ok(result.toString());
	}
	
	@GetMapping("/latest-capitalisations")
	public ResponseEntity<String> findLatestCapitalisation() {
		JSONObject result = dailyPriceService.getLatestCapitalisation();
		return ResponseEntity.ok(result.toString());
	}

	// Endpoint to get average titres echanges by ticker
	@GetMapping("/average-titres-echanges")
	public ResponseEntity<String> getAverageTitresEchangesByTicker() {
		JSONObject result = dailyPriceService.getLiquidityTitre();
		return ResponseEntity.ok(result.toString());
	}


}
