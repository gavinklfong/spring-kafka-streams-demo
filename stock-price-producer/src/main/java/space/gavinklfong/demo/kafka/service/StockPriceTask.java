package space.gavinklfong.demo.kafka.service;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import space.gavinklfong.demo.kafka.config.AppProperties;
import space.gavinklfong.demo.kafka.model.StockPrice;
//import space.gavinklfong.demo.kafka.schema.StockPrice;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import static java.util.Objects.isNull;

@Slf4j
public class StockPriceTask implements Runnable {
    private final String stockTicker;
    private final BufferedReader reader;

    private final KafkaProducer<String, StockPrice> kafkaProducer;
    private final String topic;

    public StockPriceTask(String stockTicker, String file, KafkaProducer<String, StockPrice> kafkaProducer,
                          AppProperties appProperties) {
        log.info("initialize StockPriceTask with file {}", file);
        ClassLoader classLoader = getClass().getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(file);
        assert inputStream != null;
        this.reader = new BufferedReader(new InputStreamReader(inputStream));

        this.stockTicker = stockTicker;
        this.kafkaProducer = kafkaProducer;
        this.topic = appProperties.getTopic();
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
            ProducerRecord<String, StockPrice> producerRecord = new ProducerRecord<>(topic, stockTicker, stockPrice);
            kafkaProducer.send(producerRecord).get();
        } catch (Throwable t) {
            log.error("Error occurred", t);
        }
    }

    public void close() {
        try {
            log.info("Closing task for {}", stockTicker);
            reader.close();
        } catch (Throwable t) {
            log.warn("Fail to close file reader for {}", stockTicker, t);
        }
    }
}
