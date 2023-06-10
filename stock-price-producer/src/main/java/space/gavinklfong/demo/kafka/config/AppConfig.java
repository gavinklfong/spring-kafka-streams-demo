package space.gavinklfong.demo.kafka.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
//import space.gavinklfong.demo.kafka.schema.StockPrice;
import space.gavinklfong.demo.kafka.model.StockPrice;
import space.gavinklfong.demo.kafka.service.StockPriceTask;

import java.util.List;

@Configuration
public class AppConfig {
    @Bean
    public List<StockPriceTask> stockPriceTasks(AppProperties appProperties, KafkaProducer<String, StockPrice> kafkaProducer) {
        return appProperties.getTickers().entrySet().stream()
                .map(entry -> new StockPriceTask(entry.getKey(), entry.getValue(), kafkaProducer, appProperties))
                .toList();
    }
}
