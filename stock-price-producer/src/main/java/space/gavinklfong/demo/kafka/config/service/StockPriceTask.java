package space.gavinklfong.demo.kafka.config.service;

import com.github.javafaker.Stock;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import space.gavinklfong.demo.kafka.schema.StockPrice;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;

import static java.util.Objects.isNull;
import static space.gavinklfong.demo.kafka.config.KafkaConfig.STOCK_PRICE_TOPIC;

@Slf4j
public class StockPriceTask implements Runnable {
    private final String stockTicker;
    private final BufferedReader reader;
    private final KafkaTemplate<String, StockPrice> kafkaTemplate;

    public StockPriceTask(String stockTicker, String file, KafkaTemplate<String, StockPrice> kafkaTemplate) {
        log.info("initialize StockPriceTask with file {}", file);
        ClassLoader classLoader = getClass().getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(file);
        this.reader = new BufferedReader(new InputStreamReader(inputStream));

        this.stockTicker = stockTicker;
        this.kafkaTemplate = kafkaTemplate;
    }

    @SneakyThrows
    @Override
    public void run() {
        String line = reader.readLine();
        log.info("Reading next record: {}", line);
        if (isNull(line)) throw new RuntimeException("No more data");
        String[] fields = line.split(",");
        StockPrice stockPrice = StockPriceCSVMapper.mapTo(fields);
        log.info("Sending stock price to topic: {}", stockPrice);
        kafkaTemplate.send(STOCK_PRICE_TOPIC, stockTicker, stockPrice);
    }
}
