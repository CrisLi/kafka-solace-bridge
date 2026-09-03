package com.example.kafkasolacebridge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;

class PartitionWindowTest {

    private static final int DESTINATIONS = 2;
    private static final int HIGH = 4;
    private static final int LOW = 2;

    private final TopicPartition tp = new TopicPartition("t", 3);
    private final MessageListenerContainer container = mock(MessageListenerContainer.class);
    private final PartitionWindow window = new PartitionWindow(tp, DESTINATIONS, HIGH, LOW, container);

    private static Pending pending(long offset) {
        return new Pending(3, offset, 0L, null, new byte[0], mock(Acknowledgment.class));
    }

    @Test
    void watermarkAdvancesOnlyWhenEveryDestinationConfirmed() {
        var p0 = pending(0);
        var p1 = pending(1);
        var p2 = pending(2);
        List.of(p0, p1, p2).forEach(window::add);

        window.confirm(0, List.of(p0, p1, p2));
        verify(p0.ack(), never()).acknowledge();

        window.confirm(1, List.of(p0));
        verify(p0.ack()).acknowledge();
        assertThat(window.size()).isEqualTo(2);

        window.confirm(1, List.of(p1, p2));
        verify(p1.ack(), never()).acknowledge(); // commits are cumulative: only the watermark record is acked
        verify(p2.ack()).acknowledge();
        assertThat(window.size()).isZero();
    }

    @Test
    void confirmationBehindAnUnconfirmedHeadWaits() {
        var p0 = pending(0);
        var p1 = pending(1);
        var p2 = pending(2);
        List.of(p0, p1, p2).forEach(window::add);

        window.confirm(0, List.of(p0, p1, p2));
        window.confirm(1, List.of(p1, p2));
        verify(p2.ack(), never()).acknowledge();
        assertThat(window.size()).isEqualTo(3);

        window.confirm(1, List.of(p0));
        verify(p2.ack(), times(1)).acknowledge();
        verify(p0.ack(), never()).acknowledge();
        assertThat(window.size()).isZero();
    }

    @Test
    void pausesAtHighWatermarkAndResumesAtLow() {
        var records = List.of(pending(0), pending(1), pending(2), pending(3), pending(4));
        records.forEach(window::add);
        verify(container, times(1)).pausePartition(tp);
        assertThat(window.isPaused()).isTrue();

        window.confirm(0, records);
        window.confirm(1, records.subList(0, 2)); // size 3 > low
        verify(container, never()).resumePartition(tp);

        window.confirm(1, records.subList(2, 3)); // size 2 == low
        verify(container, times(1)).resumePartition(tp);
        assertThat(window.isPaused()).isFalse();
    }

    @Test
    void skippedRecordPassesTheWatermarkInOrder() {
        var p0 = pending(0);
        var skipped = pending(1);
        window.add(p0);
        window.addSkipped(skipped);
        verify(skipped.ack(), never()).acknowledge(); // p0 ahead of it is still unconfirmed

        window.confirm(0, List.of(p0));
        window.confirm(1, List.of(p0));
        verify(skipped.ack()).acknowledge();
        verify(p0.ack(), never()).acknowledge();
        assertThat(window.queue(0)).containsExactly(p0); // skipped records are never handed to senders
    }

    /** One consumer thread adding while several sender threads confirm out of step with each other. */
    @RepeatedTest(5)
    void concurrentConfirmationsAcknowledgeMonotonicallyAndReachTheEnd() throws Exception {
        int destinations = 3;
        int count = 20_000;
        var acked = Collections.synchronizedList(new ArrayList<Long>());
        var window = new PartitionWindow(tp, destinations, count + 1, 0, container);

        var producer = new Thread(() -> {
            for (long offset = 0; offset < count; offset++) {
                long o = offset;
                window.add(new Pending(3, o, 0L, null, new byte[0], () -> acked.add(o)));
            }
        });
        var senders = new ArrayList<Thread>();
        for (int d = 0; d < destinations; d++) {
            int destination = d;
            var queue = window.queue(d);
            senders.add(new Thread(() -> {
                int seen = 0;
                var batch = new ArrayList<Pending>();
                while (seen < count) {
                    batch.clear();
                    queue.drainTo(batch, 37);
                    if (batch.isEmpty()) {
                        Pending p;
                        try {
                            p = queue.poll(100, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            return;
                        }
                        if (p == null) {
                            continue;
                        }
                        batch.add(p);
                    }
                    seen += batch.size();
                    window.confirm(destination, batch);
                }
            }));
        }
        senders.forEach(Thread::start);
        producer.start();
        producer.join(10_000);
        for (var t : senders) {
            t.join(10_000);
        }

        assertThat(window.size()).isZero();
        assertThat(acked).isNotEmpty();
        assertThat(acked.getLast()).isEqualTo(count - 1L);
        for (int i = 1; i < acked.size(); i++) {
            assertThat(acked.get(i)).as("ack #%d", i).isGreaterThan(acked.get(i - 1));
        }
    }

    @Test
    void revokedWindowNeverAcknowledgesAgain() {
        var p0 = pending(0);
        window.add(p0);
        window.revoke();

        window.confirm(0, List.of(p0));
        window.confirm(1, List.of(p0));
        verify(p0.ack(), never()).acknowledge();
        assertThat(window.size()).isZero();
        assertThat(window.queue(0)).isEmpty();
        assertThat(window.isRevoked()).isTrue();
    }
}
