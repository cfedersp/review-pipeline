package com.example.reviewpipeline.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Slf4j
public class ClientPartitionManager {

    private final ConcurrentMap<String, AtomicBoolean> clientLocks = new ConcurrentHashMap<>();

    public boolean tryAcquireClient(String clientId) {
        AtomicBoolean lock = clientLocks.computeIfAbsent(clientId, k -> new AtomicBoolean(false));
        boolean acquired = lock.compareAndSet(false, true);

        if (acquired) {
            log.debug("Acquired lock for client: {}", clientId);
        } else {
            log.debug("Client {} is already being processed", clientId);
        }

        return acquired;
    }

    public void releaseClient(String clientId) {
        AtomicBoolean lock = clientLocks.get(clientId);
        if (lock != null) {
            lock.set(false);
            log.debug("Released lock for client: {}", clientId);
        }
    }
}
