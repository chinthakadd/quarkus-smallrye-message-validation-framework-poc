package com.chinthakad.poc;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * In-memory implementation of MessageUniquenessRepository.
 * Supports both size-based and time-based eviction of old entries.
 * 
 * This implementation uses a fixed-size data structure that evicts entries when:
 * - The size threshold is reached (oldest entries are removed first)
 * - Entries are older than the time threshold
 */
@ApplicationScoped
public class InMemoryMessageUniquenessRepository implements MessageUniquenessRepository {
    
    // Map to store message_id -> entry timestamp
    private final ConcurrentMap<UUID, Long> messageIdStore = new ConcurrentHashMap<>();
    
    // Lock for compound eviction operations to ensure atomicity
    // ConcurrentHashMap is thread-safe for individual operations, but we need
    // to atomically: check size -> calculate eviction -> perform eviction -> store new entry
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    
    @Inject
    @ConfigProperty(name = "message.uniqueness_store.size.threshold", defaultValue = "10000")
    int sizeThreshold;
    
    @Inject
    @ConfigProperty(name = "message.uniqueness_store.time.threshold.millis", defaultValue = "300000")
    long timeThresholdMillis;
    
    @Override
    public boolean exists(UUID messageId) {
        // ConcurrentHashMap.containsKey() is thread-safe, no lock needed
        return messageIdStore.containsKey(messageId);
    }
    
    @Override
    public void store(UUID messageId) {
        lock.writeLock().lock();
        try {
            long currentTime = System.currentTimeMillis();
            
            // Store the message_id with current timestamp
            messageIdStore.put(messageId, currentTime);
            
            // Perform eviction if needed
            evictIfNeeded(currentTime);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    @Override
    public void remove(UUID messageId) {
        // ConcurrentHashMap.remove() is thread-safe, no lock needed
        messageIdStore.remove(messageId);
    }
    
    @Override
    public int size() {
        // ConcurrentHashMap.size() is thread-safe, no lock needed
        return messageIdStore.size();
    }
    
    @Override
    public void clear() {
        // ConcurrentHashMap.clear() is thread-safe, no lock needed
        messageIdStore.clear();
    }
    
    /**
     * Evicts entries if size threshold is exceeded or entries are too old.
     * This method should be called while holding the write lock.
     */
    private void evictIfNeeded(long currentTime) {
        // First, remove entries that are older than the time threshold
        evictByTime(currentTime);
        
        // Then, if still over size threshold, remove oldest entries
        evictBySize();
    }
    
    /**
     * Removes entries that are older than the time threshold.
     */
    private void evictByTime(long currentTime) {
        messageIdStore.entrySet().removeIf(entry -> {
            long age = currentTime - entry.getValue();
            return age > timeThresholdMillis;
        });
    }
    
    /**
     * Removes oldest entries if the size exceeds the threshold.
     * Removes entries until the size is below the threshold.
     */
    private void evictBySize() {
        int currentSize = messageIdStore.size();
        if (currentSize <= sizeThreshold) {
            return;
        }
        
        // Calculate how many entries to remove
        int entriesToRemove = currentSize - sizeThreshold;
        
        // Sort entries by timestamp (oldest first), collect keys to remove, then remove them
        List<UUID> keysToRemove = messageIdStore.entrySet().stream()
                .sorted((e1, e2) -> Long.compare(e1.getValue(), e2.getValue()))
                .limit(entriesToRemove)
                .map(entry -> entry.getKey())
                .collect(Collectors.toList());
        
        // Remove the collected keys
        keysToRemove.forEach(messageIdStore::remove);
        
        // Optional: Rebuild Bloom filter to reduce false positives after eviction
        // This is expensive but keeps the Bloom filter accurate. For now, we skip it
        // and rely on map verification for accuracy.
    }
}

