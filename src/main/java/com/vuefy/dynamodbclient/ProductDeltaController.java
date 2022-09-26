package com.vuefy.dynamodbclient;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class ProductDeltaController {

  private final DynamoDbConnector dbConnector;

  @PostMapping("/add-item")
  public ResponseEntity<?> saveProductDelta(@RequestBody ProductDeltaDto productDeltaDto) {
    dbConnector.createItems(productDeltaDto.getOperation(), new HashSet<>(productDeltaDto.getProductIds()));

    return ResponseEntity.ok("Created successfully");
  }

  @PostMapping("/api/{site_key}/upload/feed")
  public ResponseEntity<?> saveProductDelta(@PathVariable("site_key") String siteKey,
      @RequestBody ProductDeltaDto2 productDeltaDto) {
    dbConnector.createItems("update", Set.of(productDeltaDto.getUniqueId()));

    return ResponseEntity.ok(new UnbxdResponse("200", "success", UUID.randomUUID().toString()));
  }

  @GetMapping("/get-items")
  public ResponseEntity<?> getProducts() {
    long itemsWithinInterval = dbConnector.getItemsWithinInterval(5);

    return ResponseEntity.ok(itemsWithinInterval);
  }
}
