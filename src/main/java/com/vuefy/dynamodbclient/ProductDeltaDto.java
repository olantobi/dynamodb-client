package com.vuefy.dynamodbclient;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.Data;

@Data
public class ProductDeltaDto {
  private String operation;
  @JsonProperty("product_ids")
  private List<Integer> productIds;
}
