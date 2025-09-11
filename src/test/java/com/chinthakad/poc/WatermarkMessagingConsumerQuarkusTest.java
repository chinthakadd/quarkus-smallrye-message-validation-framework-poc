package com.chinthakad.poc;

import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.reactive.messaging.memory.InMemoryConnector;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class WatermarkMessagingConsumerQuarkusTest {

    @Inject
    TopicPartitionOffsetOrderVerifier offsetVerifier;

    @Inject
    SenderWatermarkOrderVerifier watermarkVerifier;

    @Inject
    InMemoryConnector inMemoryConnector;

    private ConsumerRecord<String, String> record(String sender, String topic, int partition, long offset, String value, long senderTs) {
        ConsumerRecord<String, String> r = new ConsumerRecord<>(topic, partition, offset, null, value);
        RecordHeaders headers = (RecordHeaders) r.headers();
        headers.add("sender", sender.getBytes());
        headers.add("sender-timestamp", String.valueOf(senderTs).getBytes());
        return r;
    }

    @BeforeEach
    void reset() throws InterruptedException {
        // small delay to let previous messages (if any) drain
        Thread.sleep(50);
    }

    @Test
    @DisplayName("Processes in-order offsets and updates sender watermark")
    void inOrderOffsetsAndWatermark() throws InterruptedException {
        String topic = "watermark-topic";
        int partition = 0;

        ConsumerRecord<String, String> r1 = record("sender-1", topic, partition, 0, "v1", 1_000L);
        ConsumerRecord<String, String> r2 = record("sender-1", topic, partition, 1, "v2", 2_000L);

        // Send via in-memory channel (payload type matches @Incoming method)
        inMemoryConnector.source("watermark-in").send(r1);
        inMemoryConnector.source("watermark-in").send(r2);

        Thread.sleep(200);

        // Offset verifier: latest offset should be 1 for topic-partition
        org.apache.kafka.common.TopicPartition tp = new org.apache.kafka.common.TopicPartition(topic, partition);
        long latest = offsetVerifier.getLatestOffset(tp);
        assertEquals(1L, latest, "latest offset should be updated to 1");

        // Watermark verifier: watermark for sender-1 should be 2000
        Long wm = watermarkVerifier.getWatermark("sender-1", topic, partition);
        assertNotNull(wm);
        assertEquals(2_000L, wm.longValue());
    }

    @Test
    @DisplayName("Rejects late arrival based on sender-timestamp while allowing newer ones")
    void rejectsLateArrival() throws InterruptedException {
        String topic = "watermark-topic";
        int partition = 0;

        ConsumerRecord<String, String> r1 = record("sender-2", topic, partition, 10, "v1", 5_000L);
        ConsumerRecord<String, String> late = record("sender-2", topic, partition, 11, "vLate", 4_000L); // late by ts
        ConsumerRecord<String, String> newer = record("sender-2", topic, partition, 12, "vNew", 6_000L);

        inMemoryConnector.source("watermark-in").send(r1);
        inMemoryConnector.source("watermark-in").send(late);
        inMemoryConnector.source("watermark-in").send(newer);

        Thread.sleep(200);

        Long wm = watermarkVerifier.getWatermark("sender-2", topic, partition);
        assertNotNull(wm);
        assertEquals(6_000L, wm.longValue(), "watermark should reflect latest accepted sender timestamp");
    }
}


