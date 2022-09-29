package com.vuefy.dynamodbclient;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;

@Slf4j
@Component
@RequiredArgsConstructor
public class ProductDeltaSubscriber implements Subscriber<Page<ProductDelta>> {
  private final DynamoDbDelete dynamoDbDelete;
  private List<Long> deltaKeys;

  @Override
  public void onSubscribe(Subscription subscription) {
    subscription.request(100);
  }

  @Override
  public void onNext(Page<ProductDelta> productDeltaPage) {
    List<ProductDelta> deltas = productDeltaPage.items();
    log.info("No of records: {}", deltas.size());

    if (Objects.isNull(deltaKeys)) {
      deltaKeys = new ArrayList<>();
    }
    deltaKeys.addAll(deltas.stream().map(ProductDelta::getTimestamp).collect(Collectors.toList()));
  }

  @Override
  public void onError(Throwable throwable) {
    log.warn("Error getting response from DDB ", throwable);
  }

  @Override
  public void onComplete() {
    dynamoDbDelete.deleteItems(deltaKeys);
  }
}
