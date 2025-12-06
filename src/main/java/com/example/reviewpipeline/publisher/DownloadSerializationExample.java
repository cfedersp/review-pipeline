package com.example.reviewpipeline.publisher;

import com.example.reviewpipeline.entity.ReviewQueue;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Example demonstrating how download operations are serialized per account
 * while other operations can run in parallel.
 */
@Slf4j
public class DownloadSerializationExample {

    /**
     * Demonstrates the nested grouping behavior:
     * 1. First level: Group by accountId (separate pipelines per account)
     * 2. Second level: Within each account, serialize download operations
     *
     * Example scenario:
     * Items received:
     * 1. client-A:account-1:download  → Account-1 pipeline → Download queue (position 1)
     * 2. client-B:account-1:download  → Account-1 pipeline → Download queue (position 2, waits for #1)
     * 3. client-C:account-1:update    → Account-1 pipeline → Parallel processing (concurrent with #1/#2)
     * 4. client-D:account-2:download  → Account-2 pipeline → Download queue (independent, runs in parallel)
     * 5. client-E:account-1:delete    → Account-1 pipeline → Parallel processing (concurrent with others)
     * 6. client-F:account-1:download  → Account-1 pipeline → Download queue (position 3, waits for #2)
     *
     * Processing behavior:
     * - Account-1 downloads (#1, #2, #6): Process sequentially, one at a time
     * - Account-1 other ops (#3, #5): Process in parallel up to maxConcurrency
     * - Account-2 downloads (#4): Process independently from Account-1
     */
    public static void demonstrateGrouping() {
        log.info("=== Download Serialization Example ===");
        log.info("Items with operation='download' will be serialized per account");
        log.info("Other operations will process in parallel within each account\n");

        // Simulated items
        ReviewQueue item1 = createReview(1L, "client-A", "account-1", "download");
        ReviewQueue item2 = createReview(2L, "client-B", "account-1", "download");
        ReviewQueue item3 = createReview(3L, "client-C", "account-1", "update");
        ReviewQueue item4 = createReview(4L, "client-D", "account-2", "download");
        ReviewQueue item5 = createReview(5L, "client-E", "account-1", "delete");
        ReviewQueue item6 = createReview(6L, "client-F", "account-1", "download");
        ReviewQueue item7 = createReview(7L, "client-G", "account-1", "insert");

        Flux<ReviewQueue> publisher = Flux.just(item1, item2, item3, item4, item5, item6, item7);

        // Process with the same logic as ParallelPublisherProcessor
        publisher
                .groupBy(ReviewQueue::getAccountId)
                .flatMap(accountGroup -> {
                    String accountId = accountGroup.key();
                    log.info("\n>>> Created pipeline for accountId: {}", accountId);

                    return accountGroup
                            .groupBy(item -> {
                                if ("download".equalsIgnoreCase(item.getOperation())) {
                                    return accountId + ":download";
                                }
                                return accountId + ":" + item.getOperation() + ":" + item.getId();
                            })
                            .flatMap(operationGroup -> {
                                String groupKey = operationGroup.key();
                                boolean isDownload = groupKey.contains(":download");

                                if (isDownload) {
                                    log.info("    [{}] Download queue - items will process sequentially", accountId);
                                    return operationGroup
                                            .flatMap(item -> processItem(item), 1); // Concurrency = 1
                                } else {
                                    log.info("    [{}] Parallel queue for {} - can process concurrently",
                                            accountId, operationGroup.key());
                                    return operationGroup
                                            .flatMap(item -> processItem(item), 10); // Concurrency = 10
                                }
                            });
                })
                .blockLast();

        log.info("\n=== Processing Complete ===");
    }

    /**
     * Simulates processing an item with timing information.
     */
    private static Mono<ReviewQueue> processItem(ReviewQueue item) {
        return Mono.defer(() -> {
            long startTime = System.currentTimeMillis();
            log.info("      [{}] [{}] START - Review {} ({})",
                    item.getAccountId(),
                    item.getOperation().toUpperCase(),
                    item.getId(),
                    item.getPartitionKey());

            // Simulate processing time
            return Mono.delay(Duration.ofMillis(500))
                    .thenReturn(item)
                    .doOnNext(i -> {
                        long duration = System.currentTimeMillis() - startTime;
                        log.info("      [{}] [{}] END   - Review {} (took {}ms)",
                                i.getAccountId(),
                                i.getOperation().toUpperCase(),
                                i.getId(),
                                duration);
                    });
        });
    }

    /**
     * Helper to create a ReviewQueue item.
     */
    private static ReviewQueue createReview(Long id, String clientFk, String accountId, String operation) {
        ReviewQueue review = new ReviewQueue();
        review.setId(id);
        review.setClientFk(clientFk);
        review.setAccountId(accountId);
        review.setOperation(operation);
        review.setReviewType("TYPE_A");
        review.setReviewMessage("{}");
        return review;
    }

    public static void main(String[] args) {
        demonstrateGrouping();
    }
}
