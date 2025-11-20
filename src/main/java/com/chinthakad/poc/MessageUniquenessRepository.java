package com.chinthakad.poc;

/**
 * Repository interface for storing and checking message uniqueness based on sender and message_id.
 * Duplicate detection is performed per sender, so the same message_id from different senders
 * is considered unique.
 * This interface is designed to be flexible for future database persistence implementations.
 */
public interface MessageUniquenessRepository {
    
    /**
     * Checks if a (sender, message_id) combination already exists in the repository.
     * 
     * @param key the composite key (sender, message_id) to check
     * @return true if the combination exists (duplicate), false otherwise
     */
    boolean exists(SenderMessageIdKey key);
    
    /**
     * Stores a (sender, message_id) combination with its entry timestamp.
     * 
     * @param key the composite key (sender, message_id) to store
     */
    void store(SenderMessageIdKey key);
    
    /**
     * Removes a (sender, message_id) combination from the repository.
     * 
     * @param key the composite key (sender, message_id) to remove
     */
    void remove(SenderMessageIdKey key);
    
    /**
     * Gets the current size of the repository.
     * 
     * @return the number of (sender, message_id) combinations stored
     */
    int size();
    
    /**
     * Clears all entries from the repository.
     */
    void clear();
}

