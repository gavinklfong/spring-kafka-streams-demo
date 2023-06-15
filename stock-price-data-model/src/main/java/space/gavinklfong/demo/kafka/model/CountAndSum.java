package space.gavinklfong.demo.kafka.model;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;

@NoArgsConstructor(force = true)
@AllArgsConstructor
@Value
public class CountAndSum {
    Long count;
    Double sum;
}
