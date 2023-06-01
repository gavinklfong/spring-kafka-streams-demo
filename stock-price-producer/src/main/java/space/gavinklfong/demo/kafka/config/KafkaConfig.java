package space.gavinklfong.demo.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;

@EnableKafka
@Configuration
public class KafkaConfig {
    public static final String STOCK_PRICE_TOPIC = "stock-price";

    @Bean
    public NewTopic stockPriceTopic() {
        return TopicBuilder.name(STOCK_PRICE_TOPIC).build();
    }

}
