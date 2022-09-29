package com.vuefy.dynamodbclient;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteItemEnhancedRequest.Builder;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteResult;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;

@Service
@Slf4j
public class DynamoDbDelete {
  private final AwsConfig awsConfig;
  private final DynamoDbAsyncTable<ProductDelta> productTable;
  private final DynamoDbEnhancedAsyncClient dynamoDbenhancedAsyncClient;

  public DynamoDbDelete(AwsConfig awsConfig, DynamoDbEnhancedAsyncClient dynamoDbenhancedAsyncClient) {
    this.awsConfig = awsConfig;
    this.dynamoDbenhancedAsyncClient = dynamoDbenhancedAsyncClient;
    productTable = dynamoDbenhancedAsyncClient.table(awsConfig.getDdbTableName(),
        TableSchema.fromBean(ProductDelta.class));
  }

  int BATCH_SIZE = 25;

  public void deleteItems(List<Long> deltaKeys) {
    int totalSize = deltaKeys.size();
    int chunks = totalSize / BATCH_SIZE;

      try {
        IntStream.range(0, chunks+1)
            .mapToObj(n -> deltaKeys.subList(n * BATCH_SIZE, n == chunks ? totalSize : (n+1) * BATCH_SIZE))
            .forEach(batch -> {
                Builder batchBuilder = BatchWriteItemEnhancedRequest.builder();

                batch.forEach(deltaTimestamp ->
                  batchBuilder.addWriteBatch(WriteBatch.builder(ProductDelta.class)
                    .mappedTableResource(productTable)
                    .addDeleteItem(Key.builder().partitionValue("update").sortValue(deltaTimestamp).build())
                    .build())
                );

              CompletableFuture<BatchWriteResult> completableFuture = dynamoDbenhancedAsyncClient.batchWriteItem(
                  batchBuilder.build());
              completableFuture.exceptionally(ex -> {
                log.error("Exception deleting deltas", ex);
                return null;
              });
            });
      } catch (Exception ex) {
        log.warn("Error deleting items", ex);
      }
  }
}
