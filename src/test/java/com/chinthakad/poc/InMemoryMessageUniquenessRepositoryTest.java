package com.chinthakad.poc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("InMemoryMessageUniquenessRepository Tests")
class InMemoryMessageUniquenessRepositoryTest {

    private InMemoryMessageUniquenessRepository repository;

    @BeforeEach
    void setUp() throws Exception {
        repository = new InMemoryMessageUniquenessRepository();
        // Set test properties using reflection
        setField(repository, "sizeThreshold", 100);
        setField(repository, "timeThresholdMillis", 1000L);
    }
    
    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    @Test
    @DisplayName("Should return false when checking non-existent key")
    void testExists_NonExistentKey_ReturnsFalse() {
        // Given
        SenderMessageIdKey key = new SenderMessageIdKey("sender-1", UUID.randomUUID());

        // When
        boolean exists = repository.exists(key);

        // Then
        assertFalse(exists);
    }

    @Test
    @DisplayName("Should return true when checking existing key")
    void testExists_ExistingKey_ReturnsTrue() {
        // Given
        SenderMessageIdKey key = new SenderMessageIdKey("sender-1", UUID.randomUUID());
        repository.store(key);

        // When
        boolean exists = repository.exists(key);

        // Then
        assertTrue(exists);
    }

    @Test
    @DisplayName("Should store key successfully")
    void testStore_NewKey_StoresSuccessfully() {
        // Given
        SenderMessageIdKey key = new SenderMessageIdKey("sender-1", UUID.randomUUID());

        // When
        repository.store(key);

        // Then
        assertTrue(repository.exists(key));
        assertEquals(1, repository.size());
    }

    @Test
    @DisplayName("Should allow same message_id from different senders")
    void testStore_SameMessageIdDifferentSenders_StoresBoth() {
        // Given
        UUID messageId = UUID.randomUUID();
        SenderMessageIdKey key1 = new SenderMessageIdKey("sender-1", messageId);
        SenderMessageIdKey key2 = new SenderMessageIdKey("sender-2", messageId);

        // When
        repository.store(key1);
        repository.store(key2);

        // Then
        assertTrue(repository.exists(key1));
        assertTrue(repository.exists(key2));
        assertEquals(2, repository.size());
    }

    @Test
    @DisplayName("Should detect duplicate for same sender and message_id")
    void testStore_DuplicateKey_DetectsDuplicate() {
        // Given
        SenderMessageIdKey key = new SenderMessageIdKey("sender-1", UUID.randomUUID());
        repository.store(key);

        // When
        boolean exists = repository.exists(key);

        // Then
        assertTrue(exists);
    }

    @Test
    @DisplayName("Should remove key successfully")
    void testRemove_ExistingKey_RemovesSuccessfully() {
        // Given
        SenderMessageIdKey key = new SenderMessageIdKey("sender-1", UUID.randomUUID());
        repository.store(key);
        assertTrue(repository.exists(key));

        // When
        repository.remove(key);

        // Then
        assertFalse(repository.exists(key));
        assertEquals(0, repository.size());
    }

    @Test
    @DisplayName("Should handle removal of non-existent key gracefully")
    void testRemove_NonExistentKey_HandlesGracefully() {
        // Given
        SenderMessageIdKey key = new SenderMessageIdKey("sender-1", UUID.randomUUID());

        // When/Then - should not throw exception
        assertDoesNotThrow(() -> repository.remove(key));
    }

    @Test
    @DisplayName("Should return correct size after storing multiple keys")
    void testSize_MultipleKeys_ReturnsCorrectSize() {
        // Given
        int count = 10;
        for (int i = 0; i < count; i++) {
            SenderMessageIdKey key = new SenderMessageIdKey("sender-1", UUID.randomUUID());
            repository.store(key);
        }

        // When
        int size = repository.size();

        // Then
        assertEquals(count, size);
    }

    @Test
    @DisplayName("Should return zero size for empty repository")
    void testSize_EmptyRepository_ReturnsZero() {
        // When
        int size = repository.size();

        // Then
        assertEquals(0, size);
    }

    @Test
    @DisplayName("Should clear all entries")
    void testClear_WithEntries_ClearsAll() {
        // Given
        for (int i = 0; i < 10; i++) {
            SenderMessageIdKey key = new SenderMessageIdKey("sender-1", UUID.randomUUID());
            repository.store(key);
        }
        assertEquals(10, repository.size());

        // When
        repository.clear();

        // Then
        assertEquals(0, repository.size());
    }

    @Test
    @DisplayName("Should evict entries older than time threshold")
    void testEviction_TimeBased_EvictsOldEntries() throws Exception {
        // Given
        setField(repository, "timeThresholdMillis", 100L); // 100ms threshold
        SenderMessageIdKey key1 = new SenderMessageIdKey("sender-1", UUID.randomUUID());
        SenderMessageIdKey key2 = new SenderMessageIdKey("sender-1", UUID.randomUUID());
        
        repository.store(key1);
        Thread.sleep(150); // Wait longer than threshold
        
        // When - storing new key should trigger eviction
        repository.store(key2);

        // Then
        assertFalse(repository.exists(key1), "Old entry should be evicted");
        assertTrue(repository.exists(key2), "New entry should exist");
    }

    @Test
    @DisplayName("Should evict oldest entries when size threshold is exceeded")
    void testEviction_SizeBased_EvictsOldestEntries() throws Exception {
        // Given
        setField(repository, "sizeThreshold", 5);
        
        // Store 10 entries
        for (int i = 0; i < 10; i++) {
            SenderMessageIdKey key = new SenderMessageIdKey("sender-1", UUID.randomUUID());
            repository.store(key);
            try {
                Thread.sleep(10); // Small delay to ensure different timestamps
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // When
        int size = repository.size();

        // Then - should be at or below threshold
        try {
            Field sizeThresholdField = repository.getClass().getDeclaredField("sizeThreshold");
            sizeThresholdField.setAccessible(true);
            int sizeThreshold = sizeThresholdField.getInt(repository);
            assertTrue(size <= sizeThreshold, "Size should be at or below threshold");
        } catch (Exception e) {
            fail("Failed to access sizeThreshold field: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Should handle concurrent stores correctly")
    void testConcurrency_MultipleThreads_HandlesCorrectly() throws InterruptedException {
        // Given
        int threadCount = 10;
        int keysPerThread = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        // When
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < keysPerThread; j++) {
                        SenderMessageIdKey key = new SenderMessageIdKey("sender-" + threadId, UUID.randomUUID());
                        repository.store(key);
                        if (repository.exists(key)) {
                            successCount.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        // Then
        assertEquals(threadCount * keysPerThread, successCount.get());
        try {
            Field sizeThresholdField = repository.getClass().getDeclaredField("sizeThreshold");
            sizeThresholdField.setAccessible(true);
            int sizeThreshold = sizeThresholdField.getInt(repository);
            assertTrue(repository.size() <= sizeThreshold || repository.size() == threadCount * keysPerThread);
        } catch (Exception e) {
            fail("Failed to access sizeThreshold field: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Should handle concurrent exists checks correctly")
    void testConcurrency_ConcurrentExists_HandlesCorrectly() throws InterruptedException {
        // Given
        SenderMessageIdKey key = new SenderMessageIdKey("sender-1", UUID.randomUUID());
        repository.store(key);
        
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger trueCount = new AtomicInteger(0);

        // When
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    if (repository.exists(key)) {
                        trueCount.incrementAndGet();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(2, TimeUnit.SECONDS);
        executor.shutdown();

        // Then
        assertEquals(threadCount, trueCount.get());
    }

    @Test
    @DisplayName("Should handle eviction by time then by size")
    void testEviction_TimeThenSize_EvictsCorrectly() throws Exception {
        // Given
        setField(repository, "sizeThreshold", 3);
        setField(repository, "timeThresholdMillis", 100L);
        
        // Store old entries
        SenderMessageIdKey oldKey1 = new SenderMessageIdKey("sender-1", UUID.randomUUID());
        SenderMessageIdKey oldKey2 = new SenderMessageIdKey("sender-1", UUID.randomUUID());
        repository.store(oldKey1);
        repository.store(oldKey2);
        
        Thread.sleep(150); // Wait for time threshold
        
        // Store new entries to trigger eviction
        for (int i = 0; i < 5; i++) {
            SenderMessageIdKey key = new SenderMessageIdKey("sender-1", UUID.randomUUID());
            repository.store(key);
        }

        // Then - old entries should be evicted, size should be at threshold
        assertFalse(repository.exists(oldKey1));
        assertFalse(repository.exists(oldKey2));
        try {
            Field sizeThresholdField = repository.getClass().getDeclaredField("sizeThreshold");
            sizeThresholdField.setAccessible(true);
            int sizeThreshold = sizeThresholdField.getInt(repository);
            assertTrue(repository.size() <= sizeThreshold);
        } catch (Exception e) {
            fail("Failed to access sizeThreshold field: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Should handle multiple removes correctly")
    void testRemove_MultipleKeys_RemovesCorrectly() {
        // Given
        SenderMessageIdKey key1 = new SenderMessageIdKey("sender-1", UUID.randomUUID());
        SenderMessageIdKey key2 = new SenderMessageIdKey("sender-1", UUID.randomUUID());
        SenderMessageIdKey key3 = new SenderMessageIdKey("sender-2", UUID.randomUUID());
        
        repository.store(key1);
        repository.store(key2);
        repository.store(key3);
        assertEquals(3, repository.size());

        // When
        repository.remove(key1);
        repository.remove(key3);

        // Then
        assertFalse(repository.exists(key1));
        assertTrue(repository.exists(key2));
        assertFalse(repository.exists(key3));
        assertEquals(1, repository.size());
    }

    @Test
    @DisplayName("Should maintain correct state after clear and reuse")
    void testClear_AfterClear_CanReuse() {
        // Given
        SenderMessageIdKey key1 = new SenderMessageIdKey("sender-1", UUID.randomUUID());
        repository.store(key1);
        repository.clear();

        // When
        SenderMessageIdKey key2 = new SenderMessageIdKey("sender-1", UUID.randomUUID());
        repository.store(key2);

        // Then
        assertFalse(repository.exists(key1));
        assertTrue(repository.exists(key2));
        assertEquals(1, repository.size());
    }
}

