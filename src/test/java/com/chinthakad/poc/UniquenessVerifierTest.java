package com.chinthakad.poc;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("UniquenessVerifier Tests")
class UniquenessVerifierTest {

    private UniquenessVerifier verifier;
    private MessageUniquenessRepository repository;

    @BeforeEach
    void setUp() {
        verifier = new UniquenessVerifier();
        repository = mock(MessageUniquenessRepository.class);
        verifier.uniquenessRepository = repository;
    }

    @Test
    @DisplayName("Should return SUCCESS when message_id header is missing")
    void testVerify_MissingMessageIdHeader_ReturnsSuccess() {
        // Given
        ConsumerRecord<String, String> record = createRecord("sender-1", null, "value");

        // When
        VerificationResultType result = verifier.verify(record);

        // Then
        assertEquals(VerificationResultType.SUCCESS, result);
        verify(repository, never()).exists(any());
    }

    @Test
    @DisplayName("Should return SUCCESS when sender header is missing")
    void testVerify_MissingSenderHeader_ReturnsSuccess() {
        // Given
        ConsumerRecord<String, String> record = createRecord(null, UUID.randomUUID().toString(), "value");

        // When
        VerificationResultType result = verifier.verify(record);

        // Then
        assertEquals(VerificationResultType.SUCCESS, result);
        verify(repository, never()).exists(any());
    }

    @Test
    @DisplayName("Should return SUCCESS when both headers are missing")
    void testVerify_MissingBothHeaders_ReturnsSuccess() {
        // Given
        ConsumerRecord<String, String> record = createRecord(null, null, "value");

        // When
        VerificationResultType result = verifier.verify(record);

        // Then
        assertEquals(VerificationResultType.SUCCESS, result);
        verify(repository, never()).exists(any());
    }

    @Test
    @DisplayName("Should return SUCCESS for unique message")
    void testVerify_UniqueMessage_ReturnsSuccess() {
        // Given
        String sender = "sender-1";
        UUID messageId = UUID.randomUUID();
        ConsumerRecord<String, String> record = createRecord(sender, messageId.toString(), "value");
        SenderMessageIdKey key = new SenderMessageIdKey(sender, messageId);
        
        when(repository.exists(key)).thenReturn(false);

        // When
        VerificationResultType result = verifier.verify(record);

        // Then
        assertEquals(VerificationResultType.SUCCESS, result);
        verify(repository).exists(key);
    }

    @Test
    @DisplayName("Should return FAILED_DUPLICATE for duplicate message")
    void testVerify_DuplicateMessage_ReturnsFailedDuplicate() {
        // Given
        String sender = "sender-1";
        UUID messageId = UUID.randomUUID();
        ConsumerRecord<String, String> record = createRecord(sender, messageId.toString(), "value");
        SenderMessageIdKey key = new SenderMessageIdKey(sender, messageId);
        
        when(repository.exists(key)).thenReturn(true);

        // When
        VerificationResultType result = verifier.verify(record);

        // Then
        assertEquals(VerificationResultType.FAILED_DUPLICATE, result);
        verify(repository).exists(key);
    }

    @Test
    @DisplayName("Should return SUCCESS for same message_id from different sender")
    void testVerify_SameMessageIdDifferentSender_ReturnsSuccess() {
        // Given
        UUID messageId = UUID.randomUUID();
        ConsumerRecord<String, String> record1 = createRecord("sender-1", messageId.toString(), "value1");
        ConsumerRecord<String, String> record2 = createRecord("sender-2", messageId.toString(), "value2");
        
        SenderMessageIdKey key1 = new SenderMessageIdKey("sender-1", messageId);
        SenderMessageIdKey key2 = new SenderMessageIdKey("sender-2", messageId);
        
        when(repository.exists(key1)).thenReturn(false);
        when(repository.exists(key2)).thenReturn(false);

        // When
        VerificationResultType result1 = verifier.verify(record1);
        VerificationResultType result2 = verifier.verify(record2);

        // Then
        assertEquals(VerificationResultType.SUCCESS, result1);
        assertEquals(VerificationResultType.SUCCESS, result2);
        verify(repository).exists(key1);
        verify(repository).exists(key2);
    }

    @Test
    @DisplayName("Should return SUCCESS for invalid UUID format")
    void testVerify_InvalidUuidFormat_ReturnsSuccess() {
        // Given
        ConsumerRecord<String, String> record = createRecord("sender-1", "invalid-uuid", "value");

        // When
        VerificationResultType result = verifier.verify(record);

        // Then
        assertEquals(VerificationResultType.SUCCESS, result);
        verify(repository, never()).exists(any());
    }

    @Test
    @DisplayName("Should return SUCCESS for empty message_id string")
    void testVerify_EmptyMessageId_ReturnsSuccess() {
        // Given
        ConsumerRecord<String, String> record = createRecord("sender-1", "", "value");

        // When
        VerificationResultType result = verifier.verify(record);

        // Then
        assertEquals(VerificationResultType.SUCCESS, result);
        verify(repository, never()).exists(any());
    }

    @Test
    @DisplayName("Should return SUCCESS for whitespace-only message_id")
    void testVerify_WhitespaceMessageId_ReturnsSuccess() {
        // Given
        ConsumerRecord<String, String> record = createRecord("sender-1", "   ", "value");

        // When
        VerificationResultType result = verifier.verify(record);

        // Then
        assertEquals(VerificationResultType.SUCCESS, result);
        verify(repository, never()).exists(any());
    }

    @Test
    @DisplayName("Should store record with valid headers")
    void testStoreRecord_ValidHeaders_StoresSuccessfully() {
        // Given
        String sender = "sender-1";
        UUID messageId = UUID.randomUUID();
        ConsumerRecord<String, String> record = createRecord(sender, messageId.toString(), "value");
        SenderMessageIdKey key = new SenderMessageIdKey(sender, messageId);
        
        when(repository.size()).thenReturn(1);

        // When
        verifier.storeRecord(record);

        // Then
        verify(repository).store(key);
        verify(repository).size();
    }

    @Test
    @DisplayName("Should skip storage when message_id header is missing")
    void testStoreRecord_MissingMessageId_SkipsStorage() {
        // Given
        ConsumerRecord<String, String> record = createRecord("sender-1", null, "value");

        // When
        verifier.storeRecord(record);

        // Then
        verify(repository, never()).store(any());
    }

    @Test
    @DisplayName("Should skip storage when sender header is missing")
    void testStoreRecord_MissingSender_SkipsStorage() {
        // Given
        ConsumerRecord<String, String> record = createRecord(null, UUID.randomUUID().toString(), "value");

        // When
        verifier.storeRecord(record);

        // Then
        verify(repository, never()).store(any());
    }

    @Test
    @DisplayName("Should skip storage when both headers are missing")
    void testStoreRecord_MissingBothHeaders_SkipsStorage() {
        // Given
        ConsumerRecord<String, String> record = createRecord(null, null, "value");

        // When
        verifier.storeRecord(record);

        // Then
        verify(repository, never()).store(any());
    }

    @Test
    @DisplayName("Should skip storage for invalid UUID format")
    void testStoreRecord_InvalidUuid_SkipsStorage() {
        // Given
        ConsumerRecord<String, String> record = createRecord("sender-1", "invalid-uuid", "value");

        // When
        verifier.storeRecord(record);

        // Then
        verify(repository, never()).store(any());
    }

    @Test
    @DisplayName("Should trim whitespace from sender and message_id")
    void testStoreRecord_WithWhitespace_TrimsCorrectly() {
        // Given
        UUID messageId = UUID.randomUUID();
        ConsumerRecord<String, String> record = createRecord("  sender-1  ", "  " + messageId.toString() + "  ", "value");
        SenderMessageIdKey expectedKey = new SenderMessageIdKey("sender-1", messageId);

        // When
        verifier.storeRecord(record);

        // Then
        verify(repository).store(expectedKey);
    }

    @Test
    @DisplayName("Should return priority 0 (highest priority)")
    void testPriority_ReturnsZero() {
        // When
        int priority = verifier.priority();

        // Then
        assertEquals(0, priority);
    }

    @Test
    @DisplayName("Should handle duplicate detection for same sender and message_id")
    void testVerify_SameSenderAndMessageId_DuplicateDetected() {
        // Given
        String sender = "sender-1";
        UUID messageId = UUID.randomUUID();
        ConsumerRecord<String, String> record1 = createRecord(sender, messageId.toString(), "value1");
        ConsumerRecord<String, String> record2 = createRecord(sender, messageId.toString(), "value2");
        
        SenderMessageIdKey key = new SenderMessageIdKey(sender, messageId);
        
        when(repository.exists(key)).thenReturn(false).thenReturn(true);

        // When
        VerificationResultType result1 = verifier.verify(record1);
        verifier.storeRecord(record1);
        VerificationResultType result2 = verifier.verify(record2);

        // Then
        assertEquals(VerificationResultType.SUCCESS, result1);
        assertEquals(VerificationResultType.FAILED_DUPLICATE, result2);
        verify(repository, times(2)).exists(key);
        verify(repository).store(key);
    }

    @Test
    @DisplayName("Should handle multiple unique messages from same sender")
    void testVerify_MultipleUniqueMessages_SameSender_AllSuccess() {
        // Given
        String sender = "sender-1";
        UUID messageId1 = UUID.randomUUID();
        UUID messageId2 = UUID.randomUUID();
        UUID messageId3 = UUID.randomUUID();
        
        ConsumerRecord<String, String> record1 = createRecord(sender, messageId1.toString(), "value1");
        ConsumerRecord<String, String> record2 = createRecord(sender, messageId2.toString(), "value2");
        ConsumerRecord<String, String> record3 = createRecord(sender, messageId3.toString(), "value3");
        
        SenderMessageIdKey key1 = new SenderMessageIdKey(sender, messageId1);
        SenderMessageIdKey key2 = new SenderMessageIdKey(sender, messageId2);
        SenderMessageIdKey key3 = new SenderMessageIdKey(sender, messageId3);
        
        when(repository.exists(key1)).thenReturn(false);
        when(repository.exists(key2)).thenReturn(false);
        when(repository.exists(key3)).thenReturn(false);

        // When
        VerificationResultType result1 = verifier.verify(record1);
        VerificationResultType result2 = verifier.verify(record2);
        VerificationResultType result3 = verifier.verify(record3);

        // Then
        assertEquals(VerificationResultType.SUCCESS, result1);
        assertEquals(VerificationResultType.SUCCESS, result2);
        assertEquals(VerificationResultType.SUCCESS, result3);
        verify(repository).exists(key1);
        verify(repository).exists(key2);
        verify(repository).exists(key3);
    }

    @Test
    @DisplayName("Should handle empty sender string")
    void testVerify_EmptySender_ReturnsSuccess() {
        // Given
        ConsumerRecord<String, String> record = createRecord("", UUID.randomUUID().toString(), "value");

        // When
        VerificationResultType result = verifier.verify(record);

        // Then
        assertEquals(VerificationResultType.SUCCESS, result);
        verify(repository, never()).exists(any());
    }

    @Test
    @DisplayName("Should handle null headers gracefully")
    void testVerify_NullHeaders_ReturnsSuccess() {
        // Given
        ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 0, 0L, "key", "value");

        // When
        VerificationResultType result = verifier.verify(record);

        // Then
        assertEquals(VerificationResultType.SUCCESS, result);
        verify(repository, never()).exists(any());
    }

    private ConsumerRecord<String, String> createRecord(String sender, String messageId, String value) {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", value);
        RecordHeaders headers = (RecordHeaders) record.headers();
        
        if (sender != null) {
            headers.add("sender", sender.getBytes());
        }
        if (messageId != null) {
            headers.add("message_id", messageId.getBytes());
        }
        
        return record;
    }
}

