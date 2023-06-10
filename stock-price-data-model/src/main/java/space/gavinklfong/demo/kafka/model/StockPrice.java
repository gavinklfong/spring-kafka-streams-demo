package space.gavinklfong.demo.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Value;

import java.time.Instant;

@AllArgsConstructor
@NoArgsConstructor(force = true)
@Builder
@Value
public class StockPrice implements JSONSerdeCompatible {
    Instant timestamp;
    Double open;
    Double high;
    Double low;
    Double close;
    Long volume;
}
