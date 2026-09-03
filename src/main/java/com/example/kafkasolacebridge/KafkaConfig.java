package com.example.kafkasolacebridge;

import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
class KafkaConfig {

    @Bean
    ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ConsumerFactory<Object, Object> consumerFactory,
            BridgeListener listener) {
        var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
        configurer.configure(factory, consumerFactory);
        // The design depends on these; they are not left to application.yml.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.getContainerProperties().setConsumerRebalanceListener(listener);
        // A listener exception means Solace sessions could not be opened. Nothing can be delivered anyway,
        // so keep re-seeking to the same record rather than skipping it.
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(5000, FixedBackOff.UNLIMITED_ATTEMPTS)));
        return factory;
    }
}
