package com.vuefy.dynamodbclient;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UnbxdResponse {
  private String code;
  private String status;
  private String uploadId;
}
