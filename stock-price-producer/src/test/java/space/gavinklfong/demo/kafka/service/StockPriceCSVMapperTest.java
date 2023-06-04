package space.gavinklfong.demo.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import space.gavinklfong.demo.kafka.schema.StockPrice;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
class StockPriceCSVMapperTest {

    @ParameterizedTest
    @ValueSource(strings = {"data/IBM.csv", "data/GOOG.csv"})
    void mapperTest(String file) throws URISyntaxException, IOException {
        Path path = Paths.get(getClass().getClassLoader()
                .getResource(file).toURI());

        Stream<String> lines = Files.lines(path);
        List<StockPrice> stockPrices = lines.skip(1)
                .map(line -> line.split(","))
                .map(StockPriceCSVMapper::mapTo)
                .peek(stockPrice -> log.info(stockPrice.toString()))
                .toList();
    }
}
