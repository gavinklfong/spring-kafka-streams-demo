package space.gavinklfong.demo.kafka.config.service;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import space.gavinklfong.demo.kafka.dto.StockPrice;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;

@Slf4j
public class StockPriceTask implements Runnable {

    private final BufferedReader reader;

    public StockPriceTask(String file) {
        log.info("initialize StockPriceTask with file {}", file);
        ClassLoader classLoader = getClass().getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(file);
        reader = new BufferedReader(new InputStreamReader(inputStream));
    }

    @SneakyThrows
    @Override
    public void run() {
        String line = reader.readLine();
        String[] fields = line.split(",");
        StockPrice stockPrice = StockPriceCSVMapper.mapTo(fields);

    }
}
