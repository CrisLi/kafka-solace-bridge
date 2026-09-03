package com.example.kafkasolacebridge;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One (partition, destination) stream: a single thread owning one transacted JMS {@link Session}.
 *
 * <p>Persistent publishes inside a transaction are pipelined (window 255); only {@code commit()} is a
 * broker round trip. A failed commit rolls back the whole batch on the broker, so the batch is re-sent
 * in the original order — this is what keeps per-partition ordering across retries.
 */
final class DestinationSender implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(DestinationSender.class);
    private static final long IDLE_POLL_MS = 200;

    private final String destination;
    private final int index;
    private final PartitionWindow window;
    private final BlockingQueue<Pending> queue;
    private final Session session;
    private final MessageProducer producer;
    private final CircuitBreaker breaker;
    private final int batchSize;
    private final long lingerNanos;
    private final BridgeMetrics.Destination metrics;
    private volatile boolean running = true;

    DestinationSender(String destination, int index, PartitionWindow window, Session session, MessageProducer producer,
                      CircuitBreaker breaker, AppProperties.Batch batch, BridgeMetrics.Destination metrics) {
        this.destination = destination;
        this.index = index;
        this.window = window;
        this.queue = window.queue(index);
        this.session = session;
        this.producer = producer;
        this.breaker = breaker;
        this.batchSize = batch.size();
        this.lingerNanos = batch.linger().toNanos();
        this.metrics = metrics;
    }

    @Override
    public void run() {
        try {
            while (running) {
                var batch = nextBatch();
                if (!batch.isEmpty()) {
                    deliver(batch);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    void stop() {
        running = false;
    }

    private List<Pending> nextBatch() throws InterruptedException {
        var first = queue.poll(IDLE_POLL_MS, TimeUnit.MILLISECONDS);
        if (first == null) {
            return List.of();
        }
        var batch = new ArrayList<Pending>(batchSize);
        batch.add(first);
        long deadline = System.nanoTime() + lingerNanos;
        while (batch.size() < batchSize) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                break;
            }
            var next = queue.poll(remaining, TimeUnit.NANOSECONDS);
            if (next == null) {
                break;
            }
            batch.add(next);
        }
        return batch;
    }

    private void deliver(List<Pending> batch) throws InterruptedException {
        while (running && !window.isRevoked()) {
            var permit = breaker.awaitDecision();
            switch (permit.decision()) {
                case DISCARD -> {
                    metrics.discarded().increment(batch.size());
                    window.confirm(index, batch);
                    return;
                }
                case SEND -> {
                    try {
                        for (var p : batch) {
                            producer.send(toMessage(p));
                        }
                        session.commit();
                        breaker.onSuccess();
                        metrics.sent().increment(batch.size());
                        window.confirm(index, batch);
                        return;
                    } catch (JMSException | RuntimeException e) {
                        if (!running || window.isRevoked()) {
                            return; // our own shutdown/rebalance closed the session; not a destination failure
                        }
                        metrics.rolledBack().increment();
                        log.warn("{} commit failed for partition {} offsets {}..{} ({} msgs): {}", destination,
                                batch.getFirst().partition(), batch.getFirst().offset(), batch.getLast().offset(),
                                batch.size(), e.toString());
                        rollbackQuietly();
                        breaker.onFailure(permit.round());
                    }
                }
            }
        }
    }

    private Message toMessage(Pending p) throws JMSException {
        var m = session.createBytesMessage();
        if (p.payload() != null) { // Kafka tombstone: empty body, properties still identify the record
            m.writeBytes(p.payload());
        }
        m.setIntProperty("kafka_partition", p.partition());
        m.setLongProperty("kafka_offset", p.offset());
        m.setLongProperty("kafka_timestamp", p.timestamp());
        if (p.key() != null) {
            m.setStringProperty("kafka_key", p.key());
        }
        return m;
    }

    /** The broker already rolled back on a failed commit; this covers a failure inside send(). */
    private void rollbackQuietly() {
        try {
            session.rollback();
        } catch (JMSException | RuntimeException e) {
            log.debug("{} rollback after failure: {}", destination, e.toString());
        }
    }
}
