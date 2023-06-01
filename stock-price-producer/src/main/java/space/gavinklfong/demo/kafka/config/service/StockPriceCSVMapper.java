package space.gavinklfong.demo.kafka.config.service;

import space.gavinklfong.demo.kafka.dto.StockPrice;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class StockPriceCSVMapper {

    public static StockPrice mapTo(String[] fields) {
        // time,open,high,low,close,volume
        return StockPrice.builder()
                .open(new BigDecimal(fields[0]))
                .high(new BigDecimal(fields[1]))
                .low(new BigDecimal(fields[2]))
                .close(new BigDecimal(fields[3]))
                .volume(new BigInteger((fields[4])))
                .build();
    }
}
