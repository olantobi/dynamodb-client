package com.vuefy.dynamodbclient;

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;

public class DynamoDbOperator {
  static DynamoDbEnhancedAsyncClient enhancedAsyncClient = DynamoDbEnhancedAsyncClient.create();

}
