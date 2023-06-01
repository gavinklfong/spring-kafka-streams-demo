package space.gavinklfong.demo.insurance.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import space.gavinklfong.demo.insurance.dto.Product;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ClaimReviewResult {

    String claimId;
    Product product;
    String customerId;
    Status status;
    Double claimAmount;
    String remarks;
}
