package com.chinthakad.poc;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Interface for verifiers that can validate and store consumer records.
 * Verifiers are executed in priority order by MessageCheckpointImpl.
 */
public interface Verifier {
    
    /**
     * Verifies if a consumer record should be processed based on this verifier's criteria.
     * 
     * @param record the consumer record to verify
     * @return true if the record should be processed, false otherwise
     */
    boolean verify(ConsumerRecord<String, String> record);
    
    /**
     * Stores a consumer record for this verifier's tracking purposes.
     * 
     * @param record the consumer record to store
     */
    void storeRecord(ConsumerRecord<String, String> record);
    
    /**
     * Returns the priority of this verifier for execution order.
     * Lower numbers indicate higher priority (executed first).
     * 
     * @return the priority value
     */
    int priority();
}
