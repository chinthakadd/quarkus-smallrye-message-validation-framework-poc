package com.chinthakad.poc;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Implementation of MessageCheckpoint that delegates to a list of Verifiers.
 * Verifiers are executed in priority order (lower priority number = higher priority).
 */
@ApplicationScoped
public class MessageCheckpointImpl implements MessageCheckpoint {
    
    @Inject
    Instance<Verifier> verifiers;
    
    @Override
    public boolean verify(ConsumerRecord<String, String> record) {
        System.out.println("[MessageCheckpointImpl] Starting verification for record offset: " + record.offset());
        
        // Sort verifiers by priority (lower number = higher priority)
        List<Verifier> sortedVerifiers = new ArrayList<>();
        sortedVerifiers.addAll(verifiers.stream().toList());
        sortedVerifiers.sort(Comparator.comparingInt(Verifier::priority));
        
        // Execute all verifiers in priority order
        for (Verifier verifier : sortedVerifiers) {
            System.out.println("[MessageCheckpointImpl] Executing verifier: " + verifier.getClass().getSimpleName() + 
                             " (priority: " + verifier.priority() + ")");
            
            boolean verified = verifier.verify(record);
            if (!verified) {
                System.out.println("[MessageCheckpointImpl] Verification failed by: " + verifier.getClass().getSimpleName());
                return false;
            }
        }
        
        System.out.println("[MessageCheckpointImpl] All verifiers passed - record verified");
        return true;
    }
    
    @Override
    public void storeRecord(ConsumerRecord<String, String> record) {
        System.out.println("[MessageCheckpointImpl] Starting storage for record offset: " + record.offset());
        
        // Sort verifiers by priority (lower number = higher priority)
        List<Verifier> sortedVerifiers = new ArrayList<>();
        sortedVerifiers.addAll(verifiers.stream().toList());
        sortedVerifiers.sort(Comparator.comparingInt(Verifier::priority));
        
        // Execute all verifiers in priority order
        for (Verifier verifier : sortedVerifiers) {
            System.out.println("[MessageCheckpointImpl] Storing with verifier: " + verifier.getClass().getSimpleName() + 
                             " (priority: " + verifier.priority() + ")");
            verifier.storeRecord(record);
        }
        
        System.out.println("[MessageCheckpointImpl] All verifiers completed storage");
    }
    
}
