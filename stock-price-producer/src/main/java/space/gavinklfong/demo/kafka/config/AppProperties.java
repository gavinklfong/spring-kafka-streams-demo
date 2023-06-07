package space.gavinklfong.demo.kafka.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Data
@ConfigurationProperties(prefix = "app")
public class AppProperties {
    private Map<String, String> tickers;
    private int periodMs = 1000;
    private int threadPoolSize = 4;
    private String topic;
}
