package com.example.kafkasolacebridge;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * The listener container factory itself is Spring Boot's auto-configured one. It picks up
 * {@code spring.kafka.listener.ack-mode=manual} from application.yaml, the unique
 * {@link org.springframework.kafka.listener.ConsumerAwareRebalanceListener} bean ({@link BridgeListener})
 * and the {@link CommonErrorHandler} bean below.
 */
@Configuration
class KafkaConfig {

    /**
     * A listener exception means Solace sessions could not be opened. Nothing can be delivered anyway,
     * so keep re-seeking to the same record rather than skipping it.
     */
    @Bean
    CommonErrorHandler kafkaErrorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(5000, FixedBackOff.UNLIMITED_ATTEMPTS));
    }
}
