package com.example.kafkasolacebridge;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

@Component
class BridgeMetrics {

    record Destination(Counter sent, Counter rolledBack, Counter discarded) {}

    private final MeterRegistry registry;
    private final Counter transformFailed;
    private final Map<TopicPartition, Gauge> windowGauges = new ConcurrentHashMap<>();

    BridgeMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.transformFailed = registry.counter("bridge.transform.failed");
    }

    Destination destination(String name) {
        return new Destination(
                registry.counter("bridge.sent", "destination", name),
                registry.counter("bridge.rolledback", "destination", name),
                registry.counter("bridge.discarded", "destination", name));
    }

    Counter transformFailed() {
        return transformFailed;
    }

    void registerBreaker(CircuitBreaker breaker) {
        Gauge.builder("bridge.breaker.state", breaker, b -> b.state().ordinal())
                .description("0=CLOSED 1=RETRYING 2=OPEN 3=HALF_OPEN")
                .tag("destination", breaker.name())
                .register(registry);
    }

    void registerWindow(TopicPartition tp, PartitionWindow window) {
        windowGauges.put(tp, Gauge.builder("bridge.window.size", window, PartitionWindow::size)
                .tag("partition", Integer.toString(tp.partition()))
                .register(registry));
    }

    void unregisterWindow(TopicPartition tp) {
        var gauge = windowGauges.remove(tp);
        if (gauge != null) {
            registry.remove(gauge);
        }
    }
}
