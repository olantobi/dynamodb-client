package com.vuefy.dynamodbclient;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;

@Service
@Slf4j
public class DynamoDbConnector {
  private final AwsConfig awsConfig;
  private final DynamoDbAsyncTable<ProductDelta> productTable;

  public DynamoDbConnector(AwsConfig awsConfig) {
    this.awsConfig = awsConfig;
    productTable = enhancedAsyncClient.table(awsConfig.getDdbTableName(),
        TableSchema.fromBean(ProductDelta.class));
  }

  static DynamoDbEnhancedAsyncClient enhancedAsyncClient = DynamoDbEnhancedAsyncClient.create();

  public void createItems(String operation, Set<Integer> productIds) {
    ProductDelta productDelta = ProductDelta.builder()
        .operation(operation)
        .timestamp(System.currentTimeMillis())
        .productIds(productIds)
        .isDeleted("false")
        .build();

    CompletableFuture<Void> voidCompletableFuture = productTable.putItem(productDelta);
    voidCompletableFuture.exceptionally(ex -> {
      log.error("Exception saving delta", ex);
      return null;
    });
  }

  public long getItemsWithinInterval(int timeoutMin) {
    Instant instant = Instant.now();
    Instant from = Instant.now().minus(timeoutMin, ChronoUnit.MINUTES);
    log.info("From: {}, To: {} ", from.toEpochMilli(), instant.toEpochMilli());

//    QueryEnhancedRequest queryEnhancedRequest = QueryEnhancedRequest.builder()
//        .queryConditional(QueryConditional.keyEqualTo(Key.builder()..build()))
////        .filterExpression()
//        .build();
//    productTable.query()
    return instant.toEpochMilli() - from.toEpochMilli();
  }

}
