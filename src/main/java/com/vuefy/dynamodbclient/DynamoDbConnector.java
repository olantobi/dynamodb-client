package com.vuefy.dynamodbclient;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;

@Service
@RequiredArgsConstructor
@Slf4j
public class DynamoDbConnector {
  private final AwsConfig awsConfig;

  static DynamoDbEnhancedAsyncClient enhancedAsyncClient = DynamoDbEnhancedAsyncClient.create();

  public void createItems(String operation, Set<Integer> productIds) {
    DynamoDbAsyncTable<ProductDelta> productTable = enhancedAsyncClient.table(awsConfig.getDdbTableName(),
        TableSchema.fromBean(ProductDelta.class));

    ProductDelta productDelta = ProductDelta.builder()
        .operation(operation)
        .timestamp(System.currentTimeMillis())
        .productIds(productIds)
        .is_deleted("false")
        .build();

    CompletableFuture<Void> voidCompletableFuture = productTable.putItem(productDelta);
    voidCompletableFuture.exceptionally(ex -> {
      log.error("Exception saving delta", ex);
      return null;
    });
  }

}
