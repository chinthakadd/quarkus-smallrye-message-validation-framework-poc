package com.chinthakad.poc;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Verifier that ensures records are processed in order within each partition.
 * This verifier reuses the existing offset ordering logic from MessageCheckpointImpl.
 */
@ApplicationScoped
public class TopicPartitionOffsetOrderVerifier implements Verifier {
    
    // Map to store the latest processed offset for each partition
    private final ConcurrentMap<TopicPartition, AtomicLong> partitionOffsets = new ConcurrentHashMap<>();
    
    @Override
    public VerificationResultType verify(ConsumerRecord<String, String> record) {
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        
        // Get the latest processed offset for this partition
        AtomicLong latestOffset = partitionOffsets.get(partition);
        
        // If this is the first record for this partition, allow it
        if (latestOffset == null) {
            System.out.println("[TopicPartitionOffsetOrderVerifier] First record for partition " + partition + " - allowing");
            return VerificationResultType.SUCCESS;
        }
        
        // Check if this record's offset is greater than the latest processed offset
        // Partition level ordering by offset
        boolean isInOrder = record.offset() > latestOffset.get();
        
        if (isInOrder) {
            System.out.println("[TopicPartitionOffsetOrderVerifier] Record in order for partition " + partition + 
                             " - Offset: " + record.offset() + 
                             " (Previous: " + latestOffset.get() + ")");
        } else {
            System.out.println("[TopicPartitionOffsetOrderVerifier] Record out of order for partition " + partition + 
                             " - Offset: " + record.offset() + 
                             " (Previous: " + latestOffset.get() + ")");
            return VerificationResultType.FAILED_LATE_ARRIVAL;
        }
        
        return VerificationResultType.SUCCESS;
    }
    
    @Override
    public void storeRecord(ConsumerRecord<String, String> record) {
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        
        // Update the latest offset for this partition
        partitionOffsets.computeIfAbsent(partition, k -> new AtomicLong(-1))
                        .set(record.offset());
        
        System.out.println("[TopicPartitionOffsetOrderVerifier] Stored record for partition " + partition + 
                         " - Offset: " + record.offset() + 
                         " - Timestamp: " + Instant.ofEpochMilli(record.timestamp()));
        
        // Log current state
        System.out.println("[TopicPartitionOffsetOrderVerifier] Current state - Partition " + partition + 
                         " latest offset: " + partitionOffsets.get(partition).get());
    }
    
    @Override
    public int priority() {
        // High priority (low number) for offset ordering verification
        return 1;
    }
    
    /**
     * Gets the latest processed offset for a specific partition.
     */
    public long getLatestOffset(TopicPartition partition) {
        AtomicLong offset = partitionOffsets.get(partition);
        return offset != null ? offset.get() : -1;
    }
    
}
