package space.gavinklfong.demo.kafka.dto;

import lombok.Builder;
import lombok.Value;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;

@Builder
@Value
public class StockPrice {
    LocalDateTime timestamp;
    BigDecimal open;
    BigDecimal high;
    BigDecimal low;
    BigDecimal close;
    BigInteger volume;
}
