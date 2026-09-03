package com.example.kafkasolacebridge;

import java.time.Duration;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("app")
public record AppProperties(Kafka kafka, Solace solace, Window window, Batch batch, Breaker breaker,
                            Duration shutdownDrain) {

    /** Solace broker default for max-messages-per-transaction; raising it is a Controlled Availability feature. */
    static final int SOLACE_MAX_MESSAGES_PER_TRANSACTION = 256;

    public AppProperties {
        if (batch.size() < 1 || batch.size() > SOLACE_MAX_MESSAGES_PER_TRANSACTION) {
            throw new IllegalArgumentException("app.batch.size must be 1.." + SOLACE_MAX_MESSAGES_PER_TRANSACTION);
        }
        if (window.low() >= window.high()) {
            throw new IllegalArgumentException("app.window.low must be smaller than app.window.high");
        }
        if (solace.destinations().isEmpty() || solace.destinations().size() > 31) {
            throw new IllegalArgumentException("app.solace.destinations must hold 1..31 names");
        }
        // shutdownDrain is validated against spring.lifecycle.timeout-per-shutdown-phase in BridgeListener
    }

    public record Kafka(String topic, int concurrency) {}

    public record Solace(List<String> destinations) {}

    /** Pause the partition at {@code high} unconfirmed records, resume at {@code low}. */
    public record Window(int high, int low) {}

    /** One JMS transaction holds at most {@code size} messages or {@code linger} of wall time, whichever comes first. */
    public record Batch(int size, Duration linger) {}

    /** Per-destination circuit breaker: {@code maxRetries} retries spaced {@code retryWait} apart, then OPEN for {@code openDuration}. */
    public record Breaker(Duration retryWait, int maxRetries, Duration openDuration) {}
}
