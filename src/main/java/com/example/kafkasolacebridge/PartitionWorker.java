package com.example.kafkasolacebridge;

import java.util.ArrayList;
import java.util.List;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.DeliveryMode;
import jakarta.jms.JMSException;
import jakarta.jms.Session;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;

/**
 * Everything owned by one assigned Kafka partition: one JMS {@link Connection} (one TCP socket), one
 * transacted {@link Session} + sender thread per destination, and the {@link PartitionWindow}.
 * Created on partition assignment, closed on revocation.
 *
 * <p>One connection per partition keeps each connection at 5 transacted sessions, under the Solace
 * client-profile default {@code max-transacted-sessions=10}.
 */
final class PartitionWorker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PartitionWorker.class);

    private final TopicPartition tp;
    private final Connection connection;
    private final PartitionWindow window;
    private final List<DestinationSender> senders = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();
    private final BridgeMetrics metrics;

    PartitionWorker(TopicPartition tp, ConnectionFactory connectionFactory, AppProperties props,
                    List<CircuitBreaker> breakers, MessageListenerContainer container, BridgeMetrics metrics)
            throws JMSException {
        this.tp = tp;
        this.metrics = metrics;
        var destinations = props.solace().destinations();
        this.window = new PartitionWindow(tp, destinations.size(), props.window().high(), props.window().low(), container);
        this.connection = connectionFactory.createConnection();
        try {
            for (int i = 0; i < destinations.size(); i++) {
                var session = connection.createSession(true, Session.SESSION_TRANSACTED);
                var producer = session.createProducer(session.createTopic(destinations.get(i)));
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                producer.setDisableMessageID(true);
                producer.setDisableMessageTimestamp(true);
                senders.add(new DestinationSender(destinations.get(i), i, window, session, producer, breakers.get(i),
                        props.batch(), metrics.destination(destinations.get(i))));
            }
            for (int i = 0; i < senders.size(); i++) {
                var t = new Thread(senders.get(i), "solace-p" + tp.partition() + "-" + destinations.get(i));
                t.start();
                threads.add(t);
            }
        } catch (JMSException | RuntimeException e) {
            senders.forEach(DestinationSender::stop);
            threads.forEach(Thread::interrupt);
            closeQuietly();
            throw e;
        }
        metrics.registerWindow(tp, window);
        log.info("partition {} assigned: {} sender threads started", tp, threads.size());
    }

    PartitionWindow window() {
        return window;
    }

    /**
     * Called on the Kafka consumer thread (rebalance callback), so nothing here may block: a sender stuck in
     * {@code commit()} against a stalled socket is unblocked by the connection close, which therefore runs on
     * its own thread. Closing the connection also rolls back any uncommitted transaction.
     */
    @Override
    public void close() {
        window.revoke();
        senders.forEach(DestinationSender::stop);
        threads.forEach(Thread::interrupt);
        Thread.ofPlatform().daemon().name("solace-close-p" + tp.partition()).start(this::closeQuietly);
        metrics.unregisterWindow(tp);
        log.info("partition {} revoked: senders stopped, connection close requested", tp);
    }

    private void closeQuietly() {
        try {
            connection.close();
        } catch (JMSException | RuntimeException e) {
            log.debug("partition {} connection close: {}", tp, e.toString());
        }
    }
}
