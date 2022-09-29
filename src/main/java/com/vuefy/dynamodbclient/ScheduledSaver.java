package com.vuefy.dynamodbclient;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ScheduledSaver {
  private final DynamoDbConnector dbConnector;

  @Scheduled(fixedDelay = 60 * 1000)
  public void scheduleFixedDelayTask() {
    dbConnector.createItemsAsync();
  }
}
