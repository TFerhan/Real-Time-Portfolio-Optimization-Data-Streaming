package com.kafmongo.kafmongo.kafka.producer;

import com.kafmongo.kafmongo.api.DataFetchService;
import com.kafmongo.kafmongo.api.IndexRealTimeData;

import org.json.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.client.RestTemplate;
import com.kafmongo.kafmongo.Service.DailyPriceService;
import com.kafmongo.kafmongo.api.DailyBourseData;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import java.util.Map;

@SpringBootApplication
@EnableScheduling  // Enable scheduling in your Spring Boot app
public class KafkaProducerApplication {

    public static void main(String[] args) {
        //SpringApplication.run(KafkaProducerApplication.class, args);
    }
}