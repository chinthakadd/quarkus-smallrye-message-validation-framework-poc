package com.chinthakad.poc;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.UUID;

/**
 * Verifier that checks for duplicate messages based on the "message_id" Kafka header per sender.
 * If a message_id header exists and contains a UUID, this verifier checks if the message
 * from the same sender has been seen before. Duplicate messages are rejected with FAILED_DUPLICATE.
 * 
 * Duplicate detection is performed per sender, so the same message_id from different senders
 * is considered unique.
 */
@ApplicationScoped
public class UniquenessVerifier implements Verifier {
    
    @Inject
    MessageUniquenessRepository uniquenessRepository;
    
    @Override
    public VerificationResultType verify(ConsumerRecord<String, String> record) {
        String sender = getHeaderValue(record, "sender");
        String messageIdStr = getHeaderValue(record, "message_id");
        
        // If no sender or message_id header exists, allow the record
        if (sender == null || sender.trim().isEmpty()) {
            System.out.println("[UniquenessVerifier] No sender header found - allowing");
            return VerificationResultType.SUCCESS;
        }
        
        if (messageIdStr == null || messageIdStr.trim().isEmpty()) {
            System.out.println("[UniquenessVerifier] No message_id header found - allowing");
            return VerificationResultType.SUCCESS;
        }
        
        try {
            UUID messageId = UUID.fromString(messageIdStr.trim());
            SenderMessageIdKey key = new SenderMessageIdKey(sender.trim(), messageId);
            
            // Check if this (sender, message_id) combination already exists
            if (uniquenessRepository.exists(key)) {
                System.out.println("[UniquenessVerifier] DUPLICATE message detected - sender: " + sender + ", message_id: " + messageId);
                return VerificationResultType.FAILED_DUPLICATE;
            }
            
            System.out.println("[UniquenessVerifier] Unique message - sender: " + sender + ", message_id: " + messageId);
            return VerificationResultType.SUCCESS;
            
        } catch (IllegalArgumentException e) {
            // Invalid UUID format - log and allow (could be strict and reject)
            System.out.println("[UniquenessVerifier] Invalid UUID format in message_id header: " + messageIdStr + " - allowing");
            return VerificationResultType.SUCCESS;
        }
    }
    
    @Override
    public void storeRecord(ConsumerRecord<String, String> record) {
        String sender = getHeaderValue(record, "sender");
        String messageIdStr = getHeaderValue(record, "message_id");
        
        // If no sender or message_id header exists, skip storage
        if (sender == null || sender.trim().isEmpty()) {
            System.out.println("[UniquenessVerifier] No sender header found - skipping storage");
            return;
        }
        
        if (messageIdStr == null || messageIdStr.trim().isEmpty()) {
            System.out.println("[UniquenessVerifier] No message_id header found - skipping storage");
            return;
        }
        
        try {
            UUID messageId = UUID.fromString(messageIdStr.trim());
            SenderMessageIdKey key = new SenderMessageIdKey(sender.trim(), messageId);
            
            // Store the (sender, message_id) combination for future duplicate detection
            uniquenessRepository.store(key);
            
            System.out.println("[UniquenessVerifier] Stored - sender: " + sender + ", message_id: " + messageId + 
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

