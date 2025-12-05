package com.example.reviewpipeline.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ClientPartitionManagerTest {

    private ClientPartitionManager manager;

    @BeforeEach
    void setUp() {
        manager = new ClientPartitionManager();
    }

    @Test
    void testAcquireClient_Success() {
        String clientId = "CLIENT_001";

        boolean acquired = manager.tryAcquireClient(clientId);

        assertTrue(acquired, "Should successfully acquire lock for new client");
    }

    @Test
    void testAcquireClient_AlreadyLocked() {
        String clientId = "CLIENT_001";

        manager.tryAcquireClient(clientId);
        boolean secondAttempt = manager.tryAcquireClient(clientId);

        assertFalse(secondAttempt, "Should not acquire lock for already locked client");
    }

    @Test
    void testReleaseClient() {
        String clientId = "CLIENT_001";

        manager.tryAcquireClient(clientId);
        manager.releaseClient(clientId);
        boolean reacquire = manager.tryAcquireClient(clientId);

        assertTrue(reacquire, "Should be able to reacquire lock after release");
    }

    @Test
    void testMultipleClients() {
        String client1 = "CLIENT_001";
        String client2 = "CLIENT_002";

        boolean acquired1 = manager.tryAcquireClient(client1);
        boolean acquired2 = manager.tryAcquireClient(client2);

        assertTrue(acquired1, "Should acquire lock for first client");
        assertTrue(acquired2, "Should acquire lock for second client");
    }
}
