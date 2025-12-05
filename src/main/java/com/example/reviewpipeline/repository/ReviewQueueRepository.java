package com.example.reviewpipeline.repository;

import com.example.reviewpipeline.entity.ReviewQueue;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ReviewQueueRepository extends JpaRepository<ReviewQueue, Long> {

    @Query("SELECT DISTINCT r.clientFk FROM ReviewQueue r WHERE r.processed = false")
    List<String> findDistinctUnprocessedClientFks();

    List<ReviewQueue> findByClientFkAndProcessedOrderByCreatedDateAsc(String clientFk, Boolean processed);

    @Query("SELECT r FROM ReviewQueue r WHERE r.clientFk = :clientFk AND r.processed = false ORDER BY r.createdDate ASC")
    List<ReviewQueue> findUnprocessedByClientFk(String clientFk);
}
