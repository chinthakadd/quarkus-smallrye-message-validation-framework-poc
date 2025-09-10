package com.chinthakad.poc;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Verifier that tracks watermarks based on sender-timestamp headers.
 * Detects late arrivals by comparing sender-timestamp with the latest recorded timestamp
 * for each (sender, topic, partition) combination.
 */
@ApplicationScoped
public class SenderWatermarkOrderVerifier implements Verifier {
    
    // Key: (sender, topic, partition) -> Value: latest sender-timestamp
    private final ConcurrentMap<SenderPartitionKey, Long> senderWatermarks = new ConcurrentHashMap<>();
    
    @Override
    public boolean verify(ConsumerRecord<String, String> record) {
        String sender = getHeaderValue(record, "sender");
        String senderTimestampStr = getHeaderValue(record, "sender-timestamp");
        
        // If no sender or timestamp headers, allow the record
        if (sender == null || senderTimestampStr == null) {
            System.out.println("[SenderWatermarkOrderVerifier] No sender or sender-timestamp headers - allowing");
            return true;
        }
        
        try {
            long senderTimestamp = Long.parseLong(senderTimestampStr);
            SenderPartitionKey key = new SenderPartitionKey(sender, record.topic(), record.partition());
            
            // Get the latest watermark for this sender-partition combination
            Long latestWatermark = senderWatermarks.get(key);
            
            if (latestWatermark == null) {
                // First record for this sender-partition combination
                System.out.println("[SenderWatermarkOrderVerifier] First record for sender-partition " + key + 
                                 " - allowing (timestamp: " + Instant.ofEpochMilli(senderTimestamp) + ")");
                return true;
            }
            
            // Check if this is a late arrival
            boolean isLateArrival = senderTimestamp < latestWatermark;
            
            if (isLateArrival) {
                System.out.println("[SenderWatermarkOrderVerifier] LATE ARRIVAL detected for " + key + 
                                 " - Current timestamp: " + Instant.ofEpochMilli(senderTimestamp) + 
                                 " - Latest watermark: " + Instant.ofEpochMilli(latestWatermark) + 
                                 " - Time difference: " + (latestWatermark - senderTimestamp) + "ms");
                return false; // Reject late arrivals
            } else {
                System.out.println("[SenderWatermarkOrderVerifier] Record in order for " + key + 
                                 " - Current timestamp: " + Instant.ofEpochMilli(senderTimestamp) + 
                                 " - Previous watermark: " + Instant.ofEpochMilli(latestWatermark) + 
                                 " - Time difference: " + (senderTimestamp - latestWatermark) + "ms");
                return true;
            }
            
        } catch (NumberFormatException e) {
            System.out.println("[SenderWatermarkOrderVerifier] Invalid sender-timestamp format: " + senderTimestampStr + " - allowing");
            return true;
        }
    }
    
    @Override
    public void storeRecord(ConsumerRecord<String, String> record) {
        String sender = getHeaderValue(record, "sender");
        String senderTimestampStr = getHeaderValue(record, "sender-timestamp");
        
        // If no sender or timestamp headers, skip storage
        if (sender == null || senderTimestampStr == null) {
            System.out.println("[SenderWatermarkOrderVerifier] No sender or sender-timestamp headers - skipping storage");
            return;
        }
        
        try {
            long senderTimestamp = Long.parseLong(senderTimestampStr);
            SenderPartitionKey key = new SenderPartitionKey(sender, record.topic(), record.partition());
            
            // Update the watermark for this sender-partition combination
            Long previousWatermark = senderWatermarks.put(key, senderTimestamp);
            
            if (previousWatermark != null) {
                System.out.println("[SenderWatermarkOrderVerifier] Updated watermark for " + key + 
                                 " - New timestamp: " + Instant.ofEpochMilli(senderTimestamp) + 
                                 " - Previous: " + Instant.ofEpochMilli(previousWatermark) + 
                                 " - Time difference: " + (senderTimestamp - previousWatermark) + "ms");
            } else {
                System.out.println("[SenderWatermarkOrderVerifier] Set initial watermark for " + key + 
                                 " - Timestamp: " + Instant.ofEpochMilli(senderTimestamp));
            }
            
        } catch (NumberFormatException e) {
            System.out.println("[SenderWatermarkOrderVerifier] Invalid sender-timestamp format: " + senderTimestampStr + " - skipping storage");
        }
    }
    
    @Override
    public int priority() {
        // Lower priority than TopicPartitionOffsetOrderVerifier
        // This should run after offset ordering is verified
        return 2;
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
    
    /**
     * Gets the current watermark for a specific sender-partition combination.
     */
    public Long getWatermark(String sender, String topic, int partition) {
        SenderPartitionKey key = new SenderPartitionKey(sender, topic, partition);
        return senderWatermarks.get(key);
    }
    
    /**
     * Gets all current watermarks.
     */
    public ConcurrentMap<SenderPartitionKey, Long> getAllWatermarks() {
        return new ConcurrentHashMap<>(senderWatermarks);
    }
    
    /**
     * Key class for (sender, topic, partition) combinations.
     */
    public static class SenderPartitionKey {
        private final String sender;
        private final String topic;
        private final int partition;
        
        public SenderPartitionKey(String sender, String topic, int partition) {
            this.sender = sender;
            this.topic = topic;
            this.partition = partition;
        }
        
        public String getSender() { return sender; }
        public String getTopic() { return topic; }
        public int getPartition() { return partition; }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SenderPartitionKey that = (SenderPartitionKey) o;
            return partition == that.partition &&
                   sender.equals(that.sender) &&
                   topic.equals(that.topic);
        }
        
        @Override
        public int hashCode() {
            int result = sender.hashCode();
            result = 31 * result + topic.hashCode();
            result = 31 * result + partition;
            return result;
        }
        
        @Override
        public String toString() {
            return "SenderPartitionKey{" +
                   "sender='" + sender + '\'' +
                   ", topic='" + topic + '\'' +
                   ", partition=" + partition +
                   '}';
        }
    }
}
