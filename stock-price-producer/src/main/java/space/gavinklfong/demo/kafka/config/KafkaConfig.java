package space.gavinklfong.demo.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;

@EnableKafka
@Configuration
public class KafkaConfig {
    
    @Autowired
    private AppProperties appProperties;

    @Bean
    public NewTopic stockPriceTopic() {
        return TopicBuilder.name(appProperties.getTopic()).build();
    }

}
