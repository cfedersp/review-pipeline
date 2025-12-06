package com.example.reviewpipeline.publisher;

import com.example.reviewpipeline.entity.ReviewQueue;
import com.example.reviewpipeline.model.KafkaReviewMessage;
import lombok.extern.slf4j.Slf4j;

/**
 * Example demonstrating the composite partition key functionality.
 */
@Slf4j
public class CompositePartitionKeyExample {

    /**
     * Demonstrates how the composite partition key is generated.
     */
    public static void main(String[] args) {
        // Create a ReviewQueue with composite key components
        ReviewQueue review = new ReviewQueue();
        review.setClientFk("client-123");
        review.setAccountId("account-456");
        review.setOperation("UPDATE");
        review.setReviewType("TYPE_A");
        review.setReviewMessage("{\"data\":\"test\"}");

        // The partition key is automatically generated as: client-123:account-456:UPDATE
        String partitionKey = review.getPartitionKey();
        log.info("ReviewQueue partition key: {}", partitionKey);
        // Output: ReviewQueue partition key: client-123:account-456:UPDATE

        // Create a KafkaReviewMessage with the same components
        KafkaReviewMessage kafkaMessage = new KafkaReviewMessage(
            "client-123",
            "account-456",
            "UPDATE",
            "TYPE_A",
            "{\"data\":\"test\"}"
        );

        // Both will have the same partition key
        String kafkaPartitionKey = kafkaMessage.getPartitionKey();
        log.info("KafkaReviewMessage partition key: {}", kafkaPartitionKey);
        // Output: KafkaReviewMessage partition key: client-123:account-456:UPDATE

        // Verify they match
        boolean keysMatch = partitionKey.equals(kafkaPartitionKey);
        log.info("Partition keys match: {}", keysMatch);
        // Output: Partition keys match: true

        // Different operations will have different partition keys
        ReviewQueue review2 = new ReviewQueue();
        review2.setClientFk("client-123");
        review2.setAccountId("account-456");
        review2.setOperation("DELETE");

        String partitionKey2 = review2.getPartitionKey();
        log.info("Different operation partition key: {}", partitionKey2);
        // Output: Different operation partition key: client-123:account-456:DELETE

        log.info("Keys are different: {}", !partitionKey.equals(partitionKey2));
        // Output: Keys are different: true
    }
}
