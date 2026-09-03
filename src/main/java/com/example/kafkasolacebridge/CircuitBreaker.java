package com.example.kafkasolacebridge;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * Per-destination state machine, shared by every partition's sender for that destination (a Solace
 * "spool over quota" affects the whole topic, not one Kafka partition).
 *
 * <pre>
 * CLOSED --commit fails--> RETRYING: wait retryWait, retry the same batch, at most maxRetries times
 *                              |-- any success --> CLOSED
 *                              '-- all retries fail --> OPEN
 * OPEN: for openDuration every batch is discarded (confirmed without sending) so the watermark keeps moving
 * OPEN --openDuration elapsed--> HALF_OPEN: live batches are sent as probes (one per partition that is
 *                              ready at that moment; they are not serialised)
 *                              |-- any success --> CLOSED
 *                              '-- any failure --> OPEN again
 * </pre>
 *
 * <p>Several partitions fail in the same round. Each {@link Permit} carries the round it was issued in and
 * {@link #onFailure(int)} counts a failure only if that round is still current, so a round is counted once
 * no matter how many senders report it or how long a hung {@code commit()} takes to return.
 */
final class CircuitBreaker {

    enum State { CLOSED, RETRYING, OPEN, HALF_OPEN }

    enum Decision { SEND, DISCARD }

    /** What the sender may do now, and the round it was decided in (hand it back to {@link #onFailure(int)}). */
    record Permit(Decision decision, int round) {}

    private final String name;
    private final Clock clock;
    private final Duration retryWait;
    private final int maxRetries;
    private final Duration openDuration;

    private State state = State.CLOSED;
    private int retries;
    private int round;
    private Instant notBefore = Instant.MIN;

    CircuitBreaker(String name, AppProperties.Breaker settings, Clock clock) {
        this.name = name;
        this.clock = clock;
        this.retryWait = settings.retryWait();
        this.maxRetries = settings.maxRetries();
        this.openDuration = settings.openDuration();
    }

    /** Blocks while RETRYING until the retry time; otherwise returns immediately. */
    synchronized Permit awaitDecision() throws InterruptedException {
        while (true) {
            Instant now = clock.instant();
            switch (state) {
                case CLOSED, HALF_OPEN -> {
                    return new Permit(Decision.SEND, round);
                }
                case OPEN -> {
                    if (now.isBefore(notBefore)) {
                        return new Permit(Decision.DISCARD, round);
                    }
                    state = State.HALF_OPEN;
                    round++;
                    return new Permit(Decision.SEND, round);
                }
                case RETRYING -> {
                    if (!now.isBefore(notBefore)) {
                        return new Permit(Decision.SEND, round);
                    }
                    wait(Math.max(1, Duration.between(now, notBefore).toMillis()));
                }
            }
        }
    }

    synchronized void onSuccess() {
        state = State.CLOSED;
        retries = 0;
        notifyAll();
    }

    /** @param permitRound the round of the {@link Permit} the failed attempt was made under */
    synchronized void onFailure(int permitRound) {
        if (permitRound != round) {
            return; // belongs to a round that has already been counted
        }
        Instant now = clock.instant();
        switch (state) {
            case CLOSED -> {
                state = State.RETRYING;
                retries = 0;
                notBefore = now.plus(retryWait);
            }
            case RETRYING -> {
                if (++retries >= maxRetries) {
                    open(now);
                } else {
                    notBefore = now.plus(retryWait);
                }
            }
            case HALF_OPEN -> open(now);
            case OPEN -> {
                return;
            }
        }
        round++;
        notifyAll();
    }

    synchronized State state() {
        return state;
    }

    String name() {
        return name;
    }

    private void open(Instant now) {
        state = State.OPEN;
        notBefore = now.plus(openDuration);
    }
}
