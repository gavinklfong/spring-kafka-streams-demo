package space.gavinklfong.demo.kafka.service;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import space.gavinklfong.demo.kafka.schema.StockPrice;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class StockPriceCSVMapper {

    public static StockPrice mapTo(String[] fields) {
        // time,open,high,low,close,volume
        return StockPrice.newBuilder()
                .setTimestamp(LocalDateTime.parse(fields[0], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                        .atZone(ZoneId.systemDefault())
                        .toInstant())
                .setOpen(Double.parseDouble(fields[1]))
                .setHigh(Double.parseDouble(fields[2]))
                .setLow(Double.parseDouble(fields[3]))
                .setClose(Double.parseDouble(fields[4]))
                .setVolume(Long.parseLong((fields[5])))
                .build();
    }
}
