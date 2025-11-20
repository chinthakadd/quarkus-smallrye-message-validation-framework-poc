package com.chinthakad.poc;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.UUID;

/**
 * Verifier that checks for duplicate messages based on the "message_id" Kafka header.
 * If a message_id header exists and contains a UUID, this verifier checks if the message
 * has been seen before. Duplicate messages are rejected with FAILED_DUPLICATE.
 */
@ApplicationScoped
public class UniquenessVerifier implements Verifier {
    
    @Inject
    MessageUniquenessRepository uniquenessRepository;
    
    @Override
    public VerificationResultType verify(ConsumerRecord<String, String> record) {
        String messageIdStr = getHeaderValue(record, "message_id");
        
        // If no message_id header exists, allow the record (not all messages may have it)
        if (messageIdStr == null || messageIdStr.trim().isEmpty()) {
            System.out.println("[UniquenessVerifier] No message_id header found - allowing");
            return VerificationResultType.SUCCESS;
        }
        
        try {
            UUID messageId = UUID.fromString(messageIdStr.trim());
            
            // Check if this message_id already exists
            if (uniquenessRepository.exists(messageId)) {
                System.out.println("[UniquenessVerifier] DUPLICATE message detected - message_id: " + messageId);
                return VerificationResultType.FAILED_DUPLICATE;
            }
            
            System.out.println("[UniquenessVerifier] Unique message - message_id: " + messageId);
            return VerificationResultType.SUCCESS;
            
        } catch (IllegalArgumentException e) {
            // Invalid UUID format - log and allow (could be strict and reject)
            System.out.println("[UniquenessVerifier] Invalid UUID format in message_id header: " + messageIdStr + " - allowing");
            return VerificationResultType.SUCCESS;
        }
    }
    
    @Override
    public void storeRecord(ConsumerRecord<String, String> record) {
        String messageIdStr = getHeaderValue(record, "message_id");
        
        // If no message_id header exists, skip storage
        if (messageIdStr == null || messageIdStr.trim().isEmpty()) {
            System.out.println("[UniquenessVerifier] No message_id header found - skipping storage");
            return;
        }
        
        try {
            UUID messageId = UUID.fromString(messageIdStr.trim());
            
            // Store the message_id for future duplicate detection
            uniquenessRepository.store(messageId);
            
            System.out.println("[UniquenessVerifier] Stored message_id: " + messageId + 
                             " (repository size: " + uniquenessRepository.size() + ")");
            
        } catch (IllegalArgumentException e) {
            System.out.println("[UniquenessVerifier] Invalid UUID format in message_id header: " + messageIdStr + " - skipping storage");
        }
    }
    
    @Override
    public int priority() {
        // High priority (low number) - should run early to catch duplicates before other processing
        // Running before offset ordering might be too early, so let's use priority 0 (highest)
        return 0;
    }
    
    private String getHeaderValue(ConsumerRecord<String, String> record, String headerName) {
        if (record.headers() != null) {
            var header = record.headers().lastHeader(headerName);
            if (header != null) {
                return new String(header.value());
            }
        }
        return null;
    }
}

