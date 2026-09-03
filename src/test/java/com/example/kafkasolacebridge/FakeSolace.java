package com.example.kafkasolacebridge;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.Topic;
import jakarta.jms.TransactionRolledBackException;

/**
 * Mockito-backed JMS {@link ConnectionFactory} with transacted-session semantics: sends are staged per
 * session, {@code commit()} moves them to the destination's committed list or fails and drops them
 * (which is what the Solace broker does on a failed commit).
 */
final class FakeSolace {

    record Sent(int partition, long offset) {}

    private final Map<String, List<Sent>> committed = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> commitFailures = new ConcurrentHashMap<>();

    /** Fail the next {@code n} commits to {@code destination}. */
    void failNextCommits(String destination, int n) {
        commitFailures.put(destination, new AtomicInteger(n));
    }

    /** Fail every commit to {@code destination} until further notice. */
    void failAllCommits(String destination) {
        commitFailures.put(destination, new AtomicInteger(-1));
    }

    List<Sent> committed(String destination) {
        return List.copyOf(committed.getOrDefault(destination, List.of()));
    }

    ConnectionFactory connectionFactory() throws JMSException {
        var cf = mock(ConnectionFactory.class);
        when(cf.createConnection()).thenAnswer(inv -> connection());
        return cf;
    }

    private Connection connection() throws JMSException {
        var c = mock(Connection.class);
        when(c.createSession(anyBoolean(), anyInt())).thenAnswer(inv -> session());
        return c;
    }

    private Session session() throws JMSException {
        var s = mock(Session.class);
        var destination = new AtomicReference<String>();
        var staged = new ArrayList<Sent>();
        when(s.createTopic(anyString())).thenAnswer(inv -> {
            destination.set(inv.getArgument(0));
            return mock(Topic.class);
        });
        when(s.createProducer(any())).thenAnswer(inv -> {
            var p = mock(MessageProducer.class);
            doAnswer(a -> {
                Message m = a.getArgument(0);
                staged.add(new Sent(m.getIntProperty("kafka_partition"), m.getLongProperty("kafka_offset")));
                return null;
            }).when(p).send(any(Message.class));
            return p;
        });
        when(s.createBytesMessage()).thenAnswer(inv -> message());
        doAnswer(inv -> {
            if (shouldFail(destination.get())) {
                staged.clear();
                throw new TransactionRolledBackException("503 spool over quota");
            }
            committed.computeIfAbsent(destination.get(), k -> new CopyOnWriteArrayList<>()).addAll(staged);
            staged.clear();
            return null;
        }).when(s).commit();
        doAnswer(inv -> {
            staged.clear();
            return null;
        }).when(s).rollback();
        return s;
    }

    private static BytesMessage message() throws JMSException {
        var m = mock(BytesMessage.class);
        var props = new HashMap<String, Object>();
        doAnswer(inv -> props.put(inv.getArgument(0), inv.getArgument(1))).when(m).setLongProperty(anyString(), anyLong());
        doAnswer(inv -> props.put(inv.getArgument(0), inv.getArgument(1))).when(m).setIntProperty(anyString(), anyInt());
        when(m.getLongProperty(anyString())).thenAnswer(inv -> (Long) props.get(inv.getArgument(0)));
        when(m.getIntProperty(anyString())).thenAnswer(inv -> (Integer) props.get(inv.getArgument(0)));
        return m;
    }

    private boolean shouldFail(String destination) {
        var remaining = commitFailures.get(destination);
        if (remaining == null) {
            return false;
        }
        if (remaining.get() < 0) {
            return true;
        }
        return remaining.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0;
    }
}
