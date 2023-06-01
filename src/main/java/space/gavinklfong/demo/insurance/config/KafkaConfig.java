package space.gavinklfong.demo.insurance.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.converter.ByteArrayJsonMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.*;

@EnableKafka
@Configuration
public class KafkaConfig {
    public static final String CLAIM_SUBMITTED_TOPIC = "claim-submitted";
    public static final String CLAIM_UPDATED_TOPIC = "claim-updated";

    @Bean
    public NewTopic claimSubmittedTopic() {
        return TopicBuilder.name(CLAIM_SUBMITTED_TOPIC).build();
    }

    @Bean
    public NewTopic claimUpdatedTopic() {
        return TopicBuilder.name(CLAIM_UPDATED_TOPIC).build();
    }

    @Bean
    public RecordMessageConverter recordMessageConverter() {
        return new ByteArrayJsonMessageConverter();
    }

//    @Bean NewTopic streamSourceTopic() { return TopicBuilder.name("streams-plaintext-input").build(); }
//
//    @Bean NewTopic streamOutputTopic() { return TopicBuilder.name("streams-wordcount-output").build(); }
}
