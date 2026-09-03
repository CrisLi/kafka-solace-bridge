package com.example.kafkasolacebridge;

import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.kafka.support.Acknowledgment;

/** One Kafka record waiting for confirmation from every destination. Bit {@code i} of {@link #confirmed} = destination {@code i} done. */
final class Pending {

    private final int partition;
    private final long offset;
    private final long timestamp;
    private final String key;
    private final byte[] payload;
    private final Acknowledgment ack;
    private final AtomicInteger confirmed = new AtomicInteger();

    Pending(int partition, long offset, long timestamp, String key, byte[] payload, Acknowledgment ack) {
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.key = key;
        this.payload = payload;
        this.ack = ack;
    }

    int partition() { return partition; }
    long offset() { return offset; }
    long timestamp() { return timestamp; }
    String key() { return key; }
    byte[] payload() { return payload; }
    Acknowledgment ack() { return ack; }

    void confirm(int destinationIndex) {
        confirmed.accumulateAndGet(1 << destinationIndex, (a, b) -> a | b);
    }

    void confirmAll(int allMask) {
        confirmed.set(allMask);
    }

    boolean isComplete(int allMask) {
        return confirmed.get() == allMask;
    }
}
