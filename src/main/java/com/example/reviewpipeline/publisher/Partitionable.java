package com.example.reviewpipeline.publisher;

/**
 * Interface for entities that support partitioning by a partition key.
 * Implementations of this interface can be used with partitioned publishers
 * to ensure items with the same partition key are processed sequentially.
 */
public interface Partitionable {

    /**
     * Returns the partition key for this entity.
     * Items with the same partition key will be processed sequentially,
     * ensuring only one partition is active at a time.
     *
     * @return The partition key (typically a client ID or similar identifier)
     */
    String getPartitionKey();
}
