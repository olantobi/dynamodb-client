package com.vuefy.dynamodbclient;

import java.util.Objects;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondarySortKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

@DynamoDbBean
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProductDelta {
  private String operation;

  private long timestamp;

  private Set<Integer> productIds;
  private String is_deleted;

  @DynamoDbPartitionKey
  public String getOperation() {
    return operation;
  }

  public void setOperation(String operation) {
    this.operation = operation;
  }

  @DynamoDbSortKey
  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public Set<Integer> getProductIds() {
    return productIds;
  }

  public void setProductIds(Set<Integer> productIds) {
    this.productIds = productIds;
  }

  @DynamoDbSecondarySortKey(indexNames = "is_deleted-index")
  public String getIs_deleted() {
    return is_deleted;
  }

  public void setIs_deleted(String is_deleted) {
    this.is_deleted = is_deleted;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProductDelta that = (ProductDelta) o;
    return timestamp == that.timestamp && is_deleted == that.is_deleted && operation.equals(
        that.operation) && Objects.equals(productIds, that.productIds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(operation, timestamp, productIds, is_deleted);
  }
}
