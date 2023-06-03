package space.gavinklfong.demo.kafka.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import space.gavinklfong.demo.kafka.config.service.StockPriceTask;

import java.util.List;

@Configuration
public class AppConfig {
    @Bean
    public List<StockPriceTask> stockPriceTasks(AppProperties appProperties, KafkaTemplate kafkaTemplate) {
//        return new StockPriceTask("IBM", "data/IBM.csv", kafkaTemplate);
        return appProperties.getTickers().entrySet().stream()
                .map(entry -> new StockPriceTask(entry.getKey(), entry.getValue(), kafkaTemplate))
                .toList();
    }
}
