package com.example.reviewpipeline.publisher;

/**
 * Interface for entities that support partitioning by a composite key.
 * Implementations of this interface can be used with partitioned publishers
 * to ensure items with the same partition key are processed sequentially.
 */
public interface Partitionable {

    /**
     * Returns the client ID for this entity.
     *
     * @return The client ID
     */
    String getClientId();

    /**
     * Returns the account ID for this entity.
     *
     * @return The account ID
     */
    String getAccountId();

    /**
     * Returns the operation type for this entity.
     *
     * @return The operation type
     */
    String getOperation();

    /**
     * Returns the composite partition key for this entity.
     * Items with the same partition key will be processed sequentially,
     * ensuring only one partition is active at a time.
     *
     * The partition key is a composite of clientId, accountId, and operation.
     *
     * @return The composite partition key
     */
    default String getPartitionKey() {
        return String.format("%s:%s:%s", getClientId(), getAccountId(), getOperation());
    }
}
