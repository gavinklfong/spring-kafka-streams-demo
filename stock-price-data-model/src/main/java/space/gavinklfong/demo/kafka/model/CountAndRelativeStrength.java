package space.gavinklfong.demo.kafka.model;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.With;

@With
@NoArgsConstructor(force = true)
@AllArgsConstructor
@Value
public class CountAndRelativeStrength {
    Long count;
    Double totalGain;
    Double totalLoss;
}
