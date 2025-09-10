package com.chinthakad.poc;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Interface for managing message checkpoints and watermarks.
 * This interface provides methods to verify and store consumer records
 * for watermark management across partitions.
 */
public interface MessageCheckpoint {
    
    /**
     * Verifies if a consumer record should be processed based on watermark criteria.
     * This method will be implemented to check if the record meets the watermark requirements.
     * 
     * @param record the consumer record to verify
     * @return true if the record should be processed, false otherwise
     */
    boolean verify(ConsumerRecord<String, String> record);
    
    /**
     * Stores a consumer record for watermark management.
     * This method will be implemented to store the record in the appropriate data structure
     * for tracking watermarks across partitions.
     * 
     * @param record the consumer record to store
     */
    void storeRecord(ConsumerRecord<String, String> record);
}
