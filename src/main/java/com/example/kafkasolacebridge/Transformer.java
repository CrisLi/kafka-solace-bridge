package com.example.kafkasolacebridge;

import org.springframework.stereotype.Component;

/**
 * The "simple functional processing" step. Pure function, no IO. Applied once per Kafka record on the
 * consumer thread; the same result is published to every Solace destination.
 */
@Component
public class Transformer {

    public byte[] apply(byte[] value) {
        return value; // TODO: replace with the real transformation
    }
}
