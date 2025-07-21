package com.kafmongo.kafmongo.kafka.producer;

import com.kafmongo.kafmongo.utils.BourseData;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.HashMap;
import java.util.Map;


@Configuration
public class KafkaProducerConfig {
        @Bean
        public ProducerFactory<String, byte[]> producerFactory() {
            Map<String, Object> configProps = new HashMap<>();
            configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            configProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10240);
            configProps.put(ProducerConfig.ACKS_CONFIG, "all");
            configProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 12000);
            configProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1200);
            configProps.put(ProducerConfig.LINGER_MS_CONFIG, 200);
            configProps.put(ProducerConfig.COMPRESSION_GZIP_LEVEL_CONFIG, 9);
            configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
            configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            return new DefaultKafkaProducerFactory<>(configProps);
        }

        @Bean
        public KafkaTemplate<String, byte[]> kafkaTemplate() {
            return new KafkaTemplate<>(producerFactory());
        }
        
        @Bean
		public NewTopic intradayStock() {
			return new NewTopic("intraday-stock-prices", 3, (short) 1);
		}
        
        @Bean
		public NewTopic weights() {
			return new NewTopic("Weights", 3, (short) 1);
		}
        
        @Bean
		public NewTopic portfolioMetrics() {
			return new NewTopic("portfMetrics", 3, (short) 1);
		}
        
        @Bean
        public NewTopic portfolioStats() {
        	return new NewTopic("portfStats", 3, (short) 1);
        }
        
        @Bean
        public NewTopic portfUpdatedStats() {
        	return new NewTopic("portfUpdatedStats", 3, (short) 1);
        }
        
        
        
        
        
    }


