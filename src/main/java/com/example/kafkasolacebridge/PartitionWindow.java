package com.example.kafkasolacebridge;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.MessageListenerContainer;

/**
 * In-memory window of one Kafka partition's records that are not yet confirmed by every destination.
 *
 * <p>The Kafka offset is a single number, so it can only be committed up to the highest record that
 * <em>all</em> destinations have confirmed (the watermark). {@link #inOrder} is that commit-ordered view;
 * {@link #perDestination} gives each sender thread its own cursor over the same {@link Pending} objects.
 *
 * <p>The window is bounded: at {@code high} records the partition is paused on the consumer, at {@code low}
 * it is resumed. Without the bound a slow destination would grow the heap without limit.
 */
final class PartitionWindow {

    private final TopicPartition tp;
    private final int allMask;
    private final int high;
    private final int low;
    private final MessageListenerContainer container;
    private final ArrayDeque<Pending> inOrder = new ArrayDeque<>();
    private final List<BlockingQueue<Pending>> perDestination;
    private boolean paused;
    private boolean revoked;

    PartitionWindow(TopicPartition tp, int destinationCount, int high, int low, MessageListenerContainer container) {
        this.tp = tp;
        this.allMask = (1 << destinationCount) - 1;
        this.high = high;
        this.low = low;
        this.container = container;
        var queues = new ArrayList<BlockingQueue<Pending>>(destinationCount);
        for (int i = 0; i < destinationCount; i++) {
            queues.add(new LinkedBlockingQueue<>());
        }
        this.perDestination = List.copyOf(queues);
    }

    /** Consumer thread: enqueue a record for every destination. */
    void add(Pending p) {
        synchronized (this) {
            if (revoked) {
                return;
            }
            inOrder.addLast(p);
            pauseIfFull();
        }
        for (var q : perDestination) {
            q.add(p);
        }
    }

    /** Consumer thread: record that will never be sent (transform failed) but must still pass the watermark in order. */
    void addSkipped(Pending p) {
        synchronized (this) {
            if (revoked) {
                return;
            }
            p.confirmAll(allMask);
            inOrder.addLast(p);
            advanceLocked();
            pauseIfFull();
        }
    }

    BlockingQueue<Pending> queue(int destinationIndex) {
        return perDestination.get(destinationIndex);
    }

    /** Sender thread: destination {@code destinationIndex} has committed (or discarded) {@code batch}. */
    void confirm(int destinationIndex, Collection<Pending> batch) {
        for (var p : batch) {
            p.confirm(destinationIndex);
        }
        synchronized (this) {
            if (revoked) {
                return;
            }
            advanceLocked();
        }
    }

    /** Partition taken away by a rebalance: stop acknowledging, drop everything. The new owner replays from the watermark. */
    synchronized void revoke() {
        revoked = true;
        inOrder.clear();
        perDestination.forEach(Collection::clear);
    }

    synchronized boolean isRevoked() {
        return revoked;
    }

    synchronized int size() {
        return inOrder.size();
    }

    synchronized boolean isPaused() {
        return paused;
    }

    private void advanceLocked() {
        Pending last = null;
        while (!inOrder.isEmpty() && inOrder.peekFirst().isComplete(allMask)) {
            last = inOrder.pollFirst();
        }
        if (last != null) {
            // Kafka commits are cumulative: acknowledging the watermark record commits everything before it.
            last.ack().acknowledge();
        }
        if (paused && inOrder.size() <= low) {
            paused = false;
            container.resumePartition(tp);
        }
    }

    private void pauseIfFull() {
        if (!paused && inOrder.size() >= high) {
            paused = true;
            container.pausePartition(tp);
        }
    }
}
