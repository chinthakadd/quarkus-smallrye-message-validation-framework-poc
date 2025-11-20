package com.chinthakad.poc;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Default;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * In-memory implementation of MessageUniquenessRepository using Bloom filter for fast duplicate detection.
 * Supports both size-based and time-based eviction of old entries.
 * 
 * This implementation uses:
 * - Bloom filter for fast negative checks (definitely not duplicate)
 * - ConcurrentHashMap for positive verification and timestamp tracking
 * 
 * Eviction occurs when:
 * - The size threshold is reached (oldest entries are removed first)
 * - Entries are older than the time threshold
 * 
 * Duplicate detection is performed per sender using composite keys (sender, message_id).
 */
@ApplicationScoped
@Default
public class InMemoryBloomFilterUniquenessRepository implements MessageUniquenessRepository {
    
    // Bloom filter for fast duplicate detection (no false negatives, some false positives)
    // Uses string representation of (sender, message_id) composite key
    private volatile BloomFilter<String> bloomFilter;
    
    // Map to store (sender, message_id) -> entry timestamp for verification and eviction
    private final ConcurrentMap<SenderMessageIdKey, Long> keyStore = new ConcurrentHashMap<>();
    
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
    
    @Inject
    @ConfigProperty(name = "message.uniqueness_store.bloom_filter.fpp", defaultValue = "0.01")
    double bloomFilterFalsePositiveProbability;
    
    /**
     * Initializes the Bloom filter with expected insertions based on size threshold.
     * Uses a slightly larger capacity to account for growth before eviction.
     */
    private BloomFilter<String> createBloomFilter() {
        // Create Bloom filter with expected insertions slightly larger than threshold
        // to handle growth before eviction kicks in
        int expectedInsertions = (int) (sizeThreshold * 1.2);
        return BloomFilter.create(
                Funnels.stringFunnel(StandardCharsets.UTF_8),
                expectedInsertions,
                bloomFilterFalsePositiveProbability
        );
    }
    
    public InMemoryBloomFilterUniquenessRepository() {
        // Initialize with default size, will be reconfigured after injection
        this.bloomFilter = BloomFilter.create(
                Funnels.stringFunnel(StandardCharsets.UTF_8),
                10000,
                0.01
        );
    }
    
    @jakarta.annotation.PostConstruct
    public void init() {
        // Recreate Bloom filter with configured size after injection
        this.bloomFilter = createBloomFilter();
    }
    
    /**
     * Converts a SenderMessageIdKey to a string representation for the Bloom filter.
     */
    private String keyToString(SenderMessageIdKey key) {
        return key.getSender() + ":" + key.getMessageId().toString();
    }
    
    @Override
    public boolean exists(SenderMessageIdKey key) {
        String keyStr = keyToString(key);
        
        // Fast path: Bloom filter can definitively say "not present" (no false negatives)
        // If Bloom filter says not present, we know it's definitely not a duplicate
        if (!bloomFilter.mightContain(keyStr)) {
            return false;
        }
        
        // Bloom filter says "might be present" - verify with actual map
        // This handles false positives from the Bloom filter
        return keyStore.containsKey(key);
    }
    
    @Override
    public void store(SenderMessageIdKey key) {
        lock.writeLock().lock();
        try {
            long currentTime = System.currentTimeMillis();
            String keyStr = keyToString(key);
            
            // Add to Bloom filter first (thread-safe for writes)
            bloomFilter.put(keyStr);
            
            // Store the (sender, message_id) key with current timestamp
            keyStore.put(key, currentTime);
            
            // Perform eviction if needed
            evictIfNeeded(currentTime);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    @Override
    public void remove(SenderMessageIdKey key) {
        // Note: Bloom filters don't support deletion, so we only remove from the map
        // The Bloom filter may have false positives, but that's okay - we verify with the map
        keyStore.remove(key);
    }
    
    @Override
    public int size() {
        // ConcurrentHashMap.size() is thread-safe, no lock needed
        return keyStore.size();
    }
    
    @Override
    public void clear() {
        lock.writeLock().lock();
        try {
            // Clear both Bloom filter and map
            keyStore.clear();
            bloomFilter = createBloomFilter();
        } finally {
            lock.writeLock().unlock();
        }
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
        keyStore.entrySet().removeIf(entry -> {
            long age = currentTime - entry.getValue();
            return age > timeThresholdMillis;
        });
    }
    
    /**
     * Removes oldest entries if the size exceeds the threshold.
     * Removes entries until the size is below the threshold.
     * 
     * Note: We don't remove from Bloom filter (it doesn't support deletion).
     * The Bloom filter may have some false positives, but we always verify with the map.
     * Optionally, we could rebuild the Bloom filter after eviction, but that's expensive.
     */
    private void evictBySize() {
        int currentSize = keyStore.size();
        if (currentSize <= sizeThreshold) {
            return;
        }
        
        // Calculate how many entries to remove
        int entriesToRemove = currentSize - sizeThreshold;
        
        // Sort entries by timestamp (oldest first), collect keys to remove, then remove them
        List<SenderMessageIdKey> keysToRemove = keyStore.entrySet().stream()
                .sorted((e1, e2) -> Long.compare(e1.getValue(), e2.getValue()))
                .limit(entriesToRemove)
                .map(entry -> entry.getKey())
                .collect(Collectors.toList());
        
        // Remove the collected keys
        keysToRemove.forEach(keyStore::remove);
        
        // Optional: Rebuild Bloom filter to reduce false positives after eviction
        // This is expensive but keeps the Bloom filter accurate. For now, we skip it
        // and rely on map verification for accuracy.
    }
}

