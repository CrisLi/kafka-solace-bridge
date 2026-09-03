package com.example.kafkasolacebridge;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

class CircuitBreakerTest {

    static final class MutableClock extends Clock {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");

        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }

        void advance(Duration d) { now = now.plus(d); }
    }

    private static final Duration RETRY_WAIT = Duration.ofMinutes(1);
    private static final Duration OPEN_FOR = Duration.ofMinutes(5);

    private final MutableClock clock = new MutableClock();
    private final CircuitBreaker breaker =
            new CircuitBreaker("d", new AppProperties.Breaker(RETRY_WAIT, 3, OPEN_FOR), clock);

    /** A sender obtains a permit and its commit fails. */
    private void failOnce() throws InterruptedException {
        breaker.onFailure(breaker.awaitDecision().round());
    }

    @Test
    void closedSendsImmediately() throws InterruptedException {
        assertThat(breaker.awaitDecision().decision()).isEqualTo(CircuitBreaker.Decision.SEND);
        assertThat(breaker.state()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    @Test
    void firstFailureWaitsThenRetriesAndSuccessCloses() throws InterruptedException {
        failOnce();
        assertThat(breaker.state()).isEqualTo(CircuitBreaker.State.RETRYING);

        clock.advance(RETRY_WAIT);
        assertThat(breaker.awaitDecision().decision()).isEqualTo(CircuitBreaker.Decision.SEND);

        breaker.onSuccess();
        assertThat(breaker.state()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    @Test
    void threeFailedRetriesOpenTheBreakerAndBatchesAreDiscarded() throws InterruptedException {
        driveToOpen();
        assertThat(breaker.state()).isEqualTo(CircuitBreaker.State.OPEN);
        assertThat(breaker.awaitDecision().decision()).isEqualTo(CircuitBreaker.Decision.DISCARD);
    }

    @Test
    void failuresFromOtherSendersInTheSameRoundCountOnce() throws InterruptedException {
        var a = breaker.awaitDecision(); // three partitions' senders start their batches together
        var b = breaker.awaitDecision();
        var c = breaker.awaitDecision();
        breaker.onFailure(a.round());
        breaker.onFailure(b.round());
        breaker.onFailure(c.round());
        assertThat(breaker.state()).isEqualTo(CircuitBreaker.State.RETRYING);

        // still needs the full 3 spaced retries before opening
        clock.advance(RETRY_WAIT);
        failOnce();
        clock.advance(RETRY_WAIT);
        failOnce();
        assertThat(breaker.state()).isEqualTo(CircuitBreaker.State.RETRYING);
        clock.advance(RETRY_WAIT);
        failOnce();
        assertThat(breaker.state()).isEqualTo(CircuitBreaker.State.OPEN);
    }

    @Test
    void aFailureReportedLateFromAnEarlierRoundIsIgnored() throws InterruptedException {
        var straggler = breaker.awaitDecision(); // this sender's commit() will hang for minutes
        failOnce();                              // another sender fails first: CLOSED -> RETRYING

        clock.advance(RETRY_WAIT);
        failOnce();                              // retry 1 fails
        clock.advance(RETRY_WAIT);
        breaker.onFailure(straggler.round());    // the hung commit finally returns: must not count as a retry
        failOnce();                              // retry 2 fails
        assertThat(breaker.state()).as("would be OPEN if the straggler had counted").isEqualTo(CircuitBreaker.State.RETRYING);

        clock.advance(RETRY_WAIT);
        failOnce();                              // retry 3 fails
        assertThat(breaker.state()).isEqualTo(CircuitBreaker.State.OPEN);
    }

    @Test
    void openBecomesHalfOpenAfterOpenDurationAndAProbeSuccessCloses() throws InterruptedException {
        driveToOpen();
        clock.advance(OPEN_FOR);

        assertThat(breaker.awaitDecision().decision()).isEqualTo(CircuitBreaker.Decision.SEND);
        assertThat(breaker.state()).isEqualTo(CircuitBreaker.State.HALF_OPEN);

        breaker.onSuccess();
        assertThat(breaker.state()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    @Test
    void halfOpenProbeFailureReopensWithoutRetrying() throws InterruptedException {
        driveToOpen();
        clock.advance(OPEN_FOR);
        var probe = breaker.awaitDecision();

        breaker.onFailure(probe.round());
        assertThat(breaker.state()).isEqualTo(CircuitBreaker.State.OPEN);
        assertThat(breaker.awaitDecision().decision()).isEqualTo(CircuitBreaker.Decision.DISCARD);
    }

    @Test
    void aStaleFailureDoesNotReopenAHalfOpenBreaker() throws InterruptedException {
        var beforeOpen = breaker.awaitDecision();
        driveToOpen();
        clock.advance(OPEN_FOR);
        breaker.awaitDecision(); // -> HALF_OPEN

        breaker.onFailure(beforeOpen.round());
        assertThat(breaker.state()).isEqualTo(CircuitBreaker.State.HALF_OPEN);
    }

    @Test
    void retryingBlocksTheCallerUntilTheBreakerChanges() throws InterruptedException {
        failOnce();
        var result = new AtomicReference<CircuitBreaker.Decision>();
        var t = new Thread(() -> {
            try {
                result.set(breaker.awaitDecision().decision());
            } catch (InterruptedException ignored) {
            }
        });
        t.start();
        t.join(200);
        assertThat(t.isAlive()).as("blocked while RETRYING").isTrue();

        breaker.onSuccess(); // another partition's sender succeeded
        t.join(2000);
        assertThat(result.get()).isEqualTo(CircuitBreaker.Decision.SEND);
    }

    private void driveToOpen() throws InterruptedException {
        failOnce();
        for (int i = 0; i < 3; i++) {
            clock.advance(RETRY_WAIT);
            failOnce();
        }
    }
}
