package space.gavinklfong.demo.kafka.service;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import space.gavinklfong.demo.kafka.schema.StockPrice;

import java.io.BufferedReader;
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
        assert inputStream != null;
        this.reader = new BufferedReader(new InputStreamReader(inputStream));

        this.stockTicker = stockTicker;
        this.kafkaTemplate = kafkaTemplate;
    }

    @SneakyThrows
    @Override
    public void run() {
        String line = reader.readLine();
        log.debug("Reading next record: {}", line);
        if (isNull(line)) {
            reader.close();
            throw new RuntimeException("No more data");
        }
        String[] fields = line.split(",");

        try {
            StockPrice stockPrice = StockPriceCSVMapper.mapTo(fields);
            log.info("Sending stock price [{}] to topic: {}", stockTicker, stockPrice);
            kafkaTemplate.send(STOCK_PRICE_TOPIC, stockTicker, stockPrice);
        } catch (Throwable t) {
            log.error("Error occurred", t);
        }
    }
}
