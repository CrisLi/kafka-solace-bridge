package com.example.kafkasolacebridge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.jms.BytesMessage;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TransactionRolledBackException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;

class DestinationSenderTest {

    private final Session session = mock(Session.class);
    private final MessageProducer producer = mock(MessageProducer.class);
    private final List<Long> sentOffsets = Collections.synchronizedList(new ArrayList<>());
    private final PartitionWindow window =
            new PartitionWindow(new TopicPartition("t", 0), 1, 1000, 500, mock(MessageListenerContainer.class));
    private final CircuitBreaker breaker = new CircuitBreaker("d",
            new AppProperties.Breaker(Duration.ZERO, 3, Duration.ofMinutes(5)), Clock.systemUTC());
    private final BridgeMetrics metrics = new BridgeMetrics(new SimpleMeterRegistry());
    private final DestinationSender sender = new DestinationSender("d", 0, window, session, producer, breaker,
            new AppProperties.Batch(10, Duration.ofMillis(50)), metrics.destination("d"));
    private final Thread thread = new Thread(sender, "sender-under-test");

    @BeforeEach
    void recordOffsetsOfBuiltMessages() throws JMSException {
        when(session.createBytesMessage()).thenAnswer(inv -> {
            var m = mock(BytesMessage.class);
            doAnswer(a -> {
                if ("kafka_offset".equals(a.getArgument(0))) {
                    sentOffsets.add(a.getArgument(1));
                }
                return null;
            }).when(m).setLongProperty(anyString(), anyLong());
            return m;
        });
    }

    @AfterEach
    void stopSender() throws InterruptedException {
        sender.stop();
        thread.interrupt();
        thread.join(1000);
    }

    private static Pending pending(long offset) {
        return new Pending(0, offset, 0L, null, new byte[] {1}, mock(Acknowledgment.class));
    }

    @Test
    void failedCommitResendsTheWholeBatchInTheOriginalOrder() throws Exception {
        doThrow(new TransactionRolledBackException("503 spool over quota")).doNothing().when(session).commit();
        var records = List.of(pending(0), pending(1), pending(2));
        records.forEach(window::add);

        thread.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> window.size() == 0);

        assertThat(sentOffsets).containsExactly(0L, 1L, 2L, 0L, 1L, 2L);
        verify(session, times(2)).commit();
        verify(session, times(1)).rollback();
        verify(records.get(2).ack()).acknowledge();
        verify(records.get(0).ack(), never()).acknowledge();
        assertThat(breaker.state()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    @Test
    void anExceptionCausedByOurOwnShutdownIsNotCountedAsADestinationFailure() throws Exception {
        var commitEntered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        doAnswer(inv -> {
            commitEntered.countDown();
            release.await();
            throw new IllegalStateException("session closed"); // what a closed JMS session throws
        }).when(session).commit();
        var record = pending(0);
        window.add(record);

        thread.start();
        assertThat(commitEntered.await(5, TimeUnit.SECONDS)).isTrue();
        sender.stop();          // shutdown/rebalance begins while commit() is blocked
        release.countDown();    // the blocked commit now fails
        thread.join(2000);

        assertThat(breaker.state()).isEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(metrics.destination("d").rolledBack().count()).isZero();
        verify(record.ack(), never()).acknowledge();
    }

    @Test
    void afterTheRetriesAreExhaustedTheBatchIsDiscardedAndTheWatermarkStillMoves() throws Exception {
        doThrow(new TransactionRolledBackException("503 spool over quota")).when(session).commit();
        var records = List.of(pending(0), pending(1), pending(2));
        records.forEach(window::add);

        thread.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> window.size() == 0);

        verify(session, times(4)).commit(); // initial attempt + 3 retries
        verify(records.get(2).ack()).acknowledge();
        assertThat(breaker.state()).isEqualTo(CircuitBreaker.State.OPEN);
        assertThat(metrics.destination("d").discarded().count()).isEqualTo(3);
        assertThat(metrics.destination("d").sent().count()).isZero();
    }
}
