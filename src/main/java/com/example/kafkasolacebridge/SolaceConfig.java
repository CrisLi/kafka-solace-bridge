package com.example.kafkasolacebridge;

import com.solacesystems.jms.SolConnectionFactory;
import jakarta.jms.ConnectionFactory;
import org.springframework.context.annotation.Configuration;

/**
 * Tunes the {@link SolConnectionFactory} the Solace starter auto-configures. These are setters on the
 * factory, not {@code solace.jms.apiProperties} keys, so they are applied here rather than in application.yaml.
 */
@Configuration
class SolaceConfig {

    SolaceConfig(ConnectionFactory connectionFactory) {
        if (connectionFactory instanceof SolConnectionFactory solace) {
            // createConnection() runs on the Kafka consumer thread (partition assignment). It must fail fast, not
            // retry: a blocked consumer thread exceeds max.poll.interval.ms and is fenced from the group.
            // The container error handler re-attempts every 5 s instead (KafkaConfig).
            solace.setConnectRetries(0);
            solace.setConnectTimeoutInMillis(10_000);
            // An established connection reconnects forever on the Solace API's own thread; sender threads
            // observe the outage as rolled-back commits and go through the breaker.
            solace.setReconnectRetries(-1);
            solace.setReconnectRetryWaitInMillis(3000);
            // Sliding ack window for transacted publishes (the broker maximum). Non-transacted persistent sends ignore it.
            solace.setSendADWindowSize(255);
        }
    }
}
