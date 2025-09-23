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
     * headers
     */
    void onStart(@Observes StartupEvent ev) {
        List<String> messages = List.of(
                "Watermark message 1",
                "Watermark message 2",
                "Watermark message 3",
                "Watermark message 4",
                "Watermark message 5");

        messages.forEach(message -> {
            String sender = random.nextBoolean() ? "sender-1" : "sender-2";
            long timestamp = Instant.now().toEpochMilli();
            // Create the OutgoingKafkaRecordMetadata with headers
            OutgoingKafkaRecordMetadata<String> metadata = OutgoingKafkaRecordMetadata.<String>builder()
                    .addHeaders(
                        new RecordHeader("sender", sender.getBytes()),
                        new RecordHeader("sender-timestamp", String.valueOf(timestamp).getBytes())
                    )
                    .build();
            emitter.send(Message.of(message).addMetadata(metadata));
        });
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
