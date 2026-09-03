package com.example.kafkasolacebridge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

/**
 * End-to-end through a real (embedded) Kafka broker and the fake transacted JMS layer. Proves the two
 * properties the design rests on: every destination receives every offset at least once, and the
 * consumer group offset only advances to the watermark — yet still advances when one destination is
 * permanently rejecting and its breaker is OPEN.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.group-id=" + BridgeIntegrationTest.GROUP,
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
        "spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.ByteArraySerializer",
        "app.kafka.topic=" + BridgeIntegrationTest.TOPIC,
        "app.kafka.concurrency=1",
        "app.solace.destinations=d1,d2,d3",
        "app.window.high=1000",
        "app.window.low=500",
        "app.batch.size=10",
        "app.batch.linger=50ms",
        "app.breaker.retry-wait=0s",
        "app.breaker.max-retries=3",
        "app.breaker.open-duration=1h",
        "app.shutdown-drain=1s"
})
@EmbeddedKafka(partitions = 2, topics = BridgeIntegrationTest.TOPIC, kraft = true)
class BridgeIntegrationTest {

    static final String TOPIC = "bridge-in";
    static final String GROUP = "it-bridge";
    private static final List<TopicPartition> PARTITIONS =
            List.of(new TopicPartition(TOPIC, 0), new TopicPartition(TOPIC, 1));

    @TestConfiguration
    static class FakeSolaceConfig {
        @Bean
        FakeSolace fakeSolace() {
            return new FakeSolace();
        }

        @Bean
        ConnectionFactory connectionFactory(FakeSolace fake) throws JMSException {
            return fake.connectionFactory();
        }
    }

    @Autowired KafkaTemplate<String, byte[]> template;
    @Autowired KafkaAdmin kafkaAdmin;
    @Autowired FakeSolace solace;
    @Autowired BridgeListener listener;

    @Test
    void everyDestinationGetsEveryOffsetAndTheGroupOffsetFollowsTheWatermark() throws Exception {
        // d2 rejects two commits: those batches are rolled back and re-sent, nothing is lost or reordered
        solace.failNextCommits("d2", 2);
        var first = produce(100);
        for (var d : List.of("d1", "d2", "d3")) {
            await().atMost(Duration.ofSeconds(30)).untilAsserted(
                    () -> assertThat(solace.committed(d)).as(d).containsAll(first));
        }
        awaitGroupOffsetsAtLogEnd();
        assertThat(listener.breakers().get(1).state()).isEqualTo(CircuitBreaker.State.CLOSED);

        // d3 rejects everything: after 3 retries its breaker opens, its records are discarded,
        // the other destinations keep receiving and the group offset keeps advancing
        solace.failAllCommits("d3");
        var second = produce(100);
        for (var d : List.of("d1", "d2")) {
            await().atMost(Duration.ofSeconds(30)).untilAsserted(
                    () -> assertThat(solace.committed(d)).as(d).containsAll(second));
        }
        awaitGroupOffsetsAtLogEnd();
        assertThat(listener.breakers().get(2).state()).isEqualTo(CircuitBreaker.State.OPEN);
        assertThat(solace.committed("d3")).doesNotContainAnyElementsOf(second);
    }

    private Set<FakeSolace.Sent> produce(int count) throws Exception {
        var sent = new HashSet<FakeSolace.Sent>();
        for (int i = 0; i < count; i++) {
            var meta = template.send(TOPIC, "key-" + i, ("payload-" + i).getBytes()).get().getRecordMetadata();
            sent.add(new FakeSolace.Sent(meta.partition(), meta.offset()));
        }
        return sent;
    }

    private void awaitGroupOffsetsAtLogEnd() throws Exception {
        try (var admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                var groupOffsets = admin.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata().get();
                var endOffsets = admin.listOffsets(Map.of(
                        PARTITIONS.get(0), OffsetSpec.latest(), PARTITIONS.get(1), OffsetSpec.latest())).all().get();
                for (var tp : PARTITIONS) {
                    assertThat(groupOffsets.get(tp)).as("committed offset for %s", tp).isNotNull();
                    assertThat(groupOffsets.get(tp).offset()).as(tp.toString()).isEqualTo(endOffsets.get(tp).offset());
                }
            });
        }
    }
}
