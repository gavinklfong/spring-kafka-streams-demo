package space.gavinklfong.demo.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.TopicBuilder;

@EnableKafka
@EnableKafkaStreams
@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic stockPriceTopic() {
        return TopicBuilder.name("stock-price-output").build();
    }

    @Bean
    public NewTopic transformedStockPriceOutputTopic() {
        return TopicBuilder.name("transformed-stock-price").build();
    }
    
    @Bean
    public NewTopic stockPriceBranchingOutputTopic() {
        return TopicBuilder.name("stock-price-branching-output").build();
    }

    @Bean
    public NewTopic stockPriceBranchingTechOutputTopic() {
        return TopicBuilder.name("stock-price-branching-tech").build();
    }

    @Bean
    public NewTopic stockPriceBranchingHealthOutputTopic() {
        return TopicBuilder.name("stock-price-branching-health").build();
    }

    @Bean
    public NewTopic stockPriceBranchingOthersOutputTopic() {
        return TopicBuilder.name("stock-price-branching-others").build();
    }

    @Bean
    public NewTopic stockPriceRSIOutputTopic() {
        return TopicBuilder.name("stock-price-10m-rsi").build();
    }

    @Bean
    public NewTopic countByTickerOutputTopic() {
        return TopicBuilder.name("count-by-ticker").build();
    }
}
