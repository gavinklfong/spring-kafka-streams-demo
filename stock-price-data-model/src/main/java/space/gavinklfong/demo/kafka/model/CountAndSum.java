package space.gavinklfong.demo.kafka.model;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;

import java.math.BigDecimal;

@NoArgsConstructor(force = true)
@AllArgsConstructor
@Value
public class CountAndSum {
    Long count;
    BigDecimal sum;
}
