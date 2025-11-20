package com.chinthakad.poc;

import java.util.UUID;

/**
 * Repository interface for storing and checking message uniqueness based on message_id.
 * This interface is designed to be flexible for future database persistence implementations.
 */
public interface MessageUniquenessRepository {
    
    /**
     * Checks if a message_id already exists in the repository.
     * 
     * @param messageId the UUID to check
     * @return true if the message_id exists (duplicate), false otherwise
     */
    boolean exists(UUID messageId);
    
    /**
     * Stores a message_id with its entry timestamp.
     * 
     * @param messageId the UUID to store
     */
    void store(UUID messageId);
    
    /**
     * Removes a message_id from the repository.
     * 
     * @param messageId the UUID to remove
     */
    void remove(UUID messageId);
    
    /**
     * Gets the current size of the repository.
     * 
     * @return the number of message_ids stored
     */
    int size();
    
    /**
     * Clears all entries from the repository.
     */
    void clear();
}

