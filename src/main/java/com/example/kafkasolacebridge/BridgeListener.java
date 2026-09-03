package com.example.kafkasolacebridge;

import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.annotation.PreDestroy;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Kafka side of the bridge. The listener thread only transforms and enqueues; all Solace IO happens on the
 * {@link DestinationSender} threads. Partition assignment/revocation drives {@link PartitionWorker} lifecycle.
 */
@Component
class BridgeListener implements ConsumerAwareRebalanceListener, SmartLifecycle {

    static final String LISTENER_ID = "bridge";
    private static final Logger log = LoggerFactory.getLogger(BridgeListener.class);

    private final ConnectionFactory connectionFactory;
    private final AppProperties props;
    private final Transformer transformer;
    private final BridgeMetrics metrics;
    private final KafkaListenerEndpointRegistry registry;
    private final List<CircuitBreaker> breakers;
    private final Map<TopicPartition, PartitionWorker> workers = new ConcurrentHashMap<>();
    private volatile boolean running;

    BridgeListener(ConnectionFactory connectionFactory, AppProperties props, Transformer transformer,
                   BridgeMetrics metrics, KafkaListenerEndpointRegistry registry,
                   @Value("${spring.lifecycle.timeout-per-shutdown-phase:30s}") Duration shutdownPhaseTimeout) {
        // stop() drains inside one lifecycle phase; if the phase times out, Spring stops the Kafka container
        // concurrently and the ordering this class relies on is lost.
        if (props.shutdownDrain().plusSeconds(5).compareTo(shutdownPhaseTimeout) > 0) {
            throw new IllegalArgumentException("app.shutdown-drain (" + props.shutdownDrain()
                    + ") must be at least 5s shorter than spring.lifecycle.timeout-per-shutdown-phase ("
                    + shutdownPhaseTimeout + ")");
        }
        this.connectionFactory = connectionFactory;
        this.props = props;
        this.transformer = transformer;
        this.metrics = metrics;
        this.registry = registry;
        this.breakers = props.solace().destinations().stream()
                .map(d -> new CircuitBreaker(d, props.breaker(), Clock.systemUTC()))
                .toList();
        breakers.forEach(metrics::registerBreaker);
    }

    // idIsGroup=false: the consumer group comes from spring.kafka.consumer.group-id, not from the listener id
    @KafkaListener(id = LISTENER_ID, idIsGroup = false, topics = "${app.kafka.topic}",
            concurrency = "${app.kafka.concurrency}")
    void onRecord(ConsumerRecord<String, byte[]> rec, Acknowledgment ack) {
        var window = ensureWorker(new TopicPartition(rec.topic(), rec.partition())).window();
        byte[] payload;
        try {
            payload = transformer.apply(rec.value());
        } catch (RuntimeException e) {
            metrics.transformFailed().increment();
            log.error("transform failed for partition {} offset {}; record skipped", rec.partition(), rec.offset(), e);
            window.addSkipped(new Pending(rec.partition(), rec.offset(), rec.timestamp(), rec.key(), null, ack));
            return;
        }
        window.add(new Pending(rec.partition(), rec.offset(), rec.timestamp(), rec.key(), payload, ack));
    }

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        for (var tp : partitions) {
            try {
                ensureWorker(tp);
            } catch (RuntimeException e) {
                // Same broker for every partition, so the rest would fail the same way and each attempt costs a
                // connect timeout on this consumer thread. onRecord retries through the container error handler.
                log.error("cannot start worker for {}; remaining partitions deferred to onRecord", tp, e);
                return;
            }
        }
    }

    @Override
    public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        for (var tp : partitions) {
            var worker = workers.remove(tp);
            if (worker != null) {
                worker.close();
            }
        }
    }

    List<CircuitBreaker> breakers() {
        return breakers;
    }

    private PartitionWorker ensureWorker(TopicPartition tp) {
        return workers.computeIfAbsent(tp, key -> {
            try {
                return new PartitionWorker(key, connectionFactory, props, breakers, container(), metrics);
            } catch (JMSException e) {
                throw new IllegalStateException("cannot open Solace sessions for " + key, e);
            }
        });
    }

    private MessageListenerContainer container() {
        return registry.getListenerContainer(LISTENER_ID);
    }

    // --- SmartLifecycle: stop before the Kafka containers so in-flight batches can commit and be acknowledged

    @Override
    public int getPhase() {
        return AbstractMessageListenerContainer.DEFAULT_PHASE + 1;
    }

    @Override
    public void start() {
        running = true;
    }

    @Override
    public void stop() {
        var container = container();
        if (container != null) {
            container.pause();
        }
        long deadline = System.nanoTime() + props.shutdownDrain().toNanos();
        while (System.nanoTime() < deadline && workers.values().stream().anyMatch(w -> w.window().size() > 0)) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Safety net: the container's unsubscribe normally revokes every partition first. */
    @PreDestroy
    void closeAll() {
        workers.values().forEach(PartitionWorker::close);
        workers.clear();
    }
}
