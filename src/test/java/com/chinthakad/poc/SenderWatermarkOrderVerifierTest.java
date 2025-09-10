package com.chinthakad.poc;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class SenderWatermarkOrderVerifierTest {

    private SenderWatermarkOrderVerifier verifier;

    @BeforeEach
    void setUp() {
        verifier = new SenderWatermarkOrderVerifier();
    }

    @Test
    void testVerifyFirstRecord() {
        // Create a test record with sender headers
        ConsumerRecord<String, String> record = createRecordWithHeaders("sender-1", "test-topic", 0, 0L, "value1", 1000L);
        
        // First record should be verified
        assertTrue(verifier.verify(record));
    }

    @Test
    void testVerifyInOrderRecords() {
        long timestamp1 = 1000L;
        long timestamp2 = 2000L;
        
        // First record
        ConsumerRecord<String, String> record1 = createRecordWithHeaders("sender-1", "test-topic", 0, 0L, "value1", timestamp1);
        assertTrue(verifier.verify(record1));
        verifier.storeRecord(record1);
        
        // Second record (in order)
        ConsumerRecord<String, String> record2 = createRecordWithHeaders("sender-1", "test-topic", 0, 1L, "value2", timestamp2);
        assertTrue(verifier.verify(record2));
        verifier.storeRecord(record2);
        
        // Verify watermark is updated
        Long watermark = verifier.getWatermark("sender-1", "test-topic", 0);
        assertNotNull(watermark);
        assertEquals(timestamp2, watermark);
    }

    @Test
    void testVerifyLateArrival() {
        long timestamp1 = 2000L;
        long timestamp2 = 1000L; // Earlier timestamp - late arrival
        
        // First record
        ConsumerRecord<String, String> record1 = createRecordWithHeaders("sender-1", "test-topic", 0, 0L, "value1", timestamp1);
        assertTrue(verifier.verify(record1));
        verifier.storeRecord(record1);
        
        // Late arrival record (should fail verification)
        ConsumerRecord<String, String> record2 = createRecordWithHeaders("sender-1", "test-topic", 0, 1L, "value2", timestamp2);
        assertFalse(verifier.verify(record2));
        
        // Verify watermark is still the first timestamp
        Long watermark = verifier.getWatermark("sender-1", "test-topic", 0);
        assertEquals(timestamp1, watermark);
    }

    @Test
    void testVerifyDifferentSenders() {
        long timestamp1 = 1000L;
        long timestamp2 = 500L; // Earlier timestamp but different sender
        
        // First sender
        ConsumerRecord<String, String> record1 = createRecordWithHeaders("sender-1", "test-topic", 0, 0L, "value1", timestamp1);
        assertTrue(verifier.verify(record1));
        verifier.storeRecord(record1);
        
        // Different sender with earlier timestamp (should be allowed)
        ConsumerRecord<String, String> record2 = createRecordWithHeaders("sender-2", "test-topic", 0, 1L, "value2", timestamp2);
        assertTrue(verifier.verify(record2));
        verifier.storeRecord(record2);
        
        // Verify both watermarks are tracked independently
        Long watermark1 = verifier.getWatermark("sender-1", "test-topic", 0);
        Long watermark2 = verifier.getWatermark("sender-2", "test-topic", 0);
        
        assertEquals(timestamp1, watermark1);
        assertEquals(timestamp2, watermark2);
    }

    @Test
    void testVerifyDifferentPartitions() {
        long timestamp1 = 1000L;
        long timestamp2 = 500L; // Earlier timestamp but different partition
        
        // First partition
        ConsumerRecord<String, String> record1 = createRecordWithHeaders("sender-1", "test-topic", 0, 0L, "value1", timestamp1);
        assertTrue(verifier.verify(record1));
        verifier.storeRecord(record1);
        
        // Different partition with earlier timestamp (should be allowed)
        ConsumerRecord<String, String> record2 = createRecordWithHeaders("sender-1", "test-topic", 1, 0L, "value2", timestamp2);
        assertTrue(verifier.verify(record2));
        verifier.storeRecord(record2);
        
        // Verify both watermarks are tracked independently
        Long watermark1 = verifier.getWatermark("sender-1", "test-topic", 0);
        Long watermark2 = verifier.getWatermark("sender-1", "test-topic", 1);
        
        assertEquals(timestamp1, watermark1);
        assertEquals(timestamp2, watermark2);
    }

    @Test
    void testVerifyMissingHeaders() {
        // Record without sender headers
        ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "key1", "value1");
        
        // Should be allowed (no headers to verify)
        assertTrue(verifier.verify(record));
        verifier.storeRecord(record);
    }

    @Test
    void testVerifyInvalidTimestamp() {
        // Record with invalid timestamp format
        ConsumerRecord<String, String> record = createRecordWithHeaders("sender-1", "test-topic", 0, 0L, "value1", "invalid-timestamp");
        
        // Should be allowed (invalid format)
        assertTrue(verifier.verify(record));
        verifier.storeRecord(record);
    }

    private ConsumerRecord<String, String> createRecordWithHeaders(String sender, String topic, int partition, long offset, String value, long timestamp) {
        ConsumerRecord<String, String> record = new ConsumerRecord<String,String>(topic, partition, offset, topic, value);
        record.headers().add("sender", sender.getBytes());
        record.headers().add("sender-timestamp", String.valueOf(timestamp).getBytes());
        return record;
    }

    private ConsumerRecord<String, String> createRecordWithHeaders(String sender, String topic, int partition, long offset, String value, String timestamp) {
        ConsumerRecord<String, String> record = new ConsumerRecord<String,String>(topic, partition, offset, topic, value);
        record.headers().add("sender", sender.getBytes());
        record.headers().add("sender-timestamp", timestamp.getBytes());
        return record;
    }
}
