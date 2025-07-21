package com.kafmongo.kafmongo.api;

import org.json.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.http.ResponseEntity;
@RestController
public class ApiController {

    @Autowired
    private DataFetchService dataFetchService;

    @GetMapping("/fetchData")
    public ResponseEntity<JSONArray> fetchData() {
        JSONArray data = dataFetchService.real_time_data();
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        return new ResponseEntity<>(data, headers, HttpStatus.OK);
    }
}