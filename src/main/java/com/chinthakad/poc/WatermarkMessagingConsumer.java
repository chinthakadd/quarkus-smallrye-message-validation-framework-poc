package com.chinthakad.poc;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.UUID;

@ApplicationScoped
public class WatermarkMessagingConsumer {

    @Inject
    @Channel("watermark-out")
    Emitter<String> emitter;
    
    @Inject
    MessageCheckpoint messageCheckpoint;

    private final Random random = new Random();

    /**
     * On startup, send a few test messages to the watermark-topic with Kafka
     * headers including message_id for uniqueness verification
     */
    void onStart(@Observes StartupEvent ev) {
        // Generate some UUIDs for testing, including one duplicate to test duplicate detection
        UUID messageId1 = UUID.randomUUID();
        UUID messageId2 = UUID.randomUUID();
        UUID messageId3 = UUID.randomUUID();
        UUID duplicateMessageId = messageId2; // This will be a duplicate
        
        List<MessageData> messages = List.of(
                new MessageData("Watermark message 1", messageId1),
                new MessageData("Watermark message 2", messageId2),
                new MessageData("Watermark message 3", messageId3),
                new MessageData("Watermark message 4 (duplicate)", duplicateMessageId), // Duplicate of message 2
                new MessageData("Watermark message 5", UUID.randomUUID()));

        messages.forEach(msg -> {
            String sender = random.nextBoolean() ? "sender-1" : "sender-2";
            long timestamp = Instant.now().toEpochMilli();
            // Create the OutgoingKafkaRecordMetadata with headers
            OutgoingKafkaRecordMetadata<String> metadata = OutgoingKafkaRecordMetadata.<String>builder()
                    .addHeaders(
                        new RecordHeader("sender", sender.getBytes()),
                        new RecordHeader("sender-timestamp", String.valueOf(timestamp).getBytes()),
                        new RecordHeader("message_id", msg.messageId.toString().getBytes())
                    )
                    .build();
            emitter.send(Message.of(msg.content).addMetadata(metadata));
        });
    }
    
    /**
     * Helper class to hold message content and message_id
     */
    private static class MessageData {
        final String content;
        final UUID messageId;
        
        MessageData(String content, UUID messageId) {
            this.content = content;
            this.messageId = messageId;
        }
    }

    /**
     * Consume messages from the watermark-topic using ConsumerRecord
     */
    @Incoming("watermark-in")
    public void onMessage(ConsumerRecord<String, String> record) {
        System.out.println("=== Watermark Message Received ===");
        System.out.println("Topic: " + record.topic());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());
        System.out.println("Key: " + record.key());
        System.out.println("Value: " + record.value());

        // BEFORE CONSUMING: Verify the record using MessageCheckpoint
        System.out.println("\n🔍 BEFORE CONSUMING - Verifying record...");
        VerificationResultType result = messageCheckpoint.verify(record);
        
        if (!result.verified()) {
            System.out.println("Record verification failed - " + result + " - skipping processing");
            System.out.println("=================================");
            return;
        }
        
        System.out.println("Record verification passed - " + result + " - proceeding with processing");

        // Read Kafka headers
        String sender = getHeaderValue(record, "sender");
        String senderTimestamp = getHeaderValue(record, "sender-timestamp");
        String messageId = getHeaderValue(record, "message_id");

        if (sender != null) {
            System.out.println("Sender: " + sender);
        }

        if (senderTimestamp != null) {
            try {
                Instant instant = Instant.ofEpochMilli(Long.parseLong(senderTimestamp));
                System.out.println("Sender Timestamp: " + instant);
            } catch (NumberFormatException e) {
                System.out.println("Invalid sender timestamp: " + senderTimestamp);
            }
        }
        
        if (messageId != null) {
            System.out.println("Message ID: " + messageId);
        }

        // AFTER CONSUMING: Store the record using MessageCheckpoint
        System.out.println("\n AFTER CONSUMING - Storing record...");
        messageCheckpoint.storeRecord(record);
        System.out.println("Record stored successfully");

        System.out.println("=================================");
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
