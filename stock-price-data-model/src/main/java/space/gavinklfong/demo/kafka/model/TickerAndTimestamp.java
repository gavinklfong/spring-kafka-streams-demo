package space.gavinklfong.demo.kafka.model;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;

import java.time.Instant;

@NoArgsConstructor(force = true)
@AllArgsConstructor
@Value
public class TickerAndTimestamp {
    String ticker;
    Instant timestamp;
}
