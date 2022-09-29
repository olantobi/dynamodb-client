package com.vuefy.dynamodbclient;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncIndex;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.PagePublisher;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;

@Service
@Slf4j
public class DynamoDbConnector {
  private final DynamoDbAsyncTable<ProductDelta> productTable;
  private final ProductDeltaSubscriber productDeltaSubscriber;

  private static final Set<Integer> productIds = ConcurrentHashMap.newKeySet();
  private static final AtomicInteger counter = new AtomicInteger();

  public void addProductId(Integer productId) {
    if (productIds.add(productId)) {
      counter.incrementAndGet();
    }
  }

  public DynamoDbConnector(AwsConfig awsConfig, ProductDeltaSubscriber productDeltaSubscriber,
      DynamoDbEnhancedAsyncClient dynamoDbenhancedAsyncClient) {
    productTable = dynamoDbenhancedAsyncClient.table(awsConfig.getDdbTableName(),
        TableSchema.fromBean(ProductDelta.class));
    this.productDeltaSubscriber = productDeltaSubscriber;
  }

  @Async
  public void createItems(String operation, Set<Integer> productIds) {
    Instant instant = Instant.now();
    ProductDelta productDelta = ProductDelta.builder()
        .operation(operation)
        .timestamp(instant.toEpochMilli())
        .productIds(productIds)
        .isDeleted("false")
        .build();

    CompletableFuture<Void> voidCompletableFuture = productTable.putItem(productDelta);
    voidCompletableFuture.exceptionally(ex -> {
      log.error("Exception saving delta", ex);
      return null;
    });
  }

  public void createItemsAsync() {
    if (productIds.isEmpty()) {
      log.info("Empty products list. Returning back to base.");
      return;
    }
    log.info("Pushing {} product ids to dynamodb. Counter: {}", productIds.size(), counter.get());

    Instant instant = Instant.now();
    CompletableFuture<Void> voidCompletableFuture = null;
    ProductDelta productDelta = ProductDelta.builder()
        .operation("update")
        .timestamp(instant.toEpochMilli())
        .productIds(productIds)
        .isDeleted("false")
        .build();

//    lock.lock();
//    try {
      voidCompletableFuture = productTable.putItem(productDelta);
      productIds.clear();
//    } finally {
//      lock.unlock();
//    }

    if (Objects.nonNull(voidCompletableFuture)) {
      voidCompletableFuture.exceptionally(ex -> {
        log.error("Exception saving delta", ex);
        return null;
      });
    }
  }

  public long getItemsWithinInterval(int timeoutMin) {
    Instant from = Instant.now().minus(timeoutMin, ChronoUnit.MINUTES);

    QueryEnhancedRequest queryEnhancedRequest = QueryEnhancedRequest.builder()
        .queryConditional(QueryConditional.keyEqualTo(Key.builder()
            .partitionValue("update")
            .build()))
        .build();

    PagePublisher<ProductDelta> queryPage = productTable.query(queryEnhancedRequest);
    queryPage.subscribe(productDeltaSubscriber);

    return 0;
  }

  public long getDeletedItems() {
    DynamoDbAsyncIndex<ProductDelta> secIndex = productTable.index("is_deleted-index");
    SdkPublisher<Page<ProductDelta>> queryPage = secIndex.query(QueryEnhancedRequest.builder()
        .queryConditional(QueryConditional.keyEqualTo(Key.builder()
                .partitionValue("update").sortValue("true").build()))
        .build());

    queryPage.subscribe(new Subscriber<Page<ProductDelta>>() {

      @Override
      public void onSubscribe(Subscription subscription) {
        subscription.request(100);
      }

      @Override
      public void onNext(Page<ProductDelta> productDeltaPage) {
        List<ProductDelta> deltas = productDeltaPage.items();
        log.info("No of Deleted records: {}", deltas.size());
        deltas.forEach(System.out::println);
      }

      @Override
      public void onError(Throwable throwable) {
        log.warn("Error getting response from DDB ", throwable);
      }

      @Override
      public void onComplete() {
        log.info("Data fetching completed");
      }
    });

    return 0;
//    return queryPage.items();
  }

//  public long updateItemsWithinInterval(int timeoutMin) {
//    productTable.
//
//
////    QueryEnhancedRequest queryEnhancedRequest = QueryEnhancedRequest.builder()
////        .queryConditional(QueryConditional.keyEqualTo(Key.builder()..build()))
//////        .filterExpression()
////        .build();
////    productTable.query()
//    return instant.toEpochMilli() - from.toEpochMilli();
//  }

}
