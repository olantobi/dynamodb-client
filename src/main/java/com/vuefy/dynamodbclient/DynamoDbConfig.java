package com.vuefy.dynamodbclient;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;

@Component
public class DynamoDbConfig {

  @Bean
  @Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
  public DynamoDbEnhancedAsyncClient getDynamoDbEnhancedAsyncClient() {
    return DynamoDbEnhancedAsyncClient.create();
  }
}
