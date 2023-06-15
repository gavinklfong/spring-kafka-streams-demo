package space.gavinklfong.demo.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Value;

import java.time.Instant;

@Builder
@NoArgsConstructor(force = true)
@AllArgsConstructor
@Value
public class TickerAndCategory {
    String ticker;
    StockCategory category;
}
