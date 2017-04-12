package com.netflix.exhibitor.core.config.consul;

import com.google.common.base.Optional;
import com.orbitz.consul.Consul;
import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.model.kv.Value;
import com.orbitz.consul.model.session.ImmutableSession;
import com.orbitz.consul.option.QueryOptions;

import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

public class ConsulKvLock {
    private final Consul consul;
    private final String path;
    private final String name;
    private final String ttl;
    private String sessionId;

    /**
     * @param consul consul client instance for connecting to consul cluster
     * @param path consul key-value path to lock
     * @param name a descriptive name for the lock
     * @param ttl TTL, in seconds, for the consul session underpinning the lock
     */
    public ConsulKvLock(Consul consul, String path, String name, Integer ttl) {
        this.consul = consul;
        this.path = path;
        this.name = name;
        this.ttl = ttl != null ? String.format("%ds", ttl) : null;
        this.sessionId = null;
    }

    /**
     * @param consul consul client instance for connecting to consul cluster
     * @param path consul key-value path to lock
     * @param name a descriptive name for the lock
     */
    public ConsulKvLock(Consul consul, String path, String name) {
        this(consul, path, name, 60);
    }

    private String createSession() {
        final ImmutableSession session = ImmutableSession.builder()
                .name(name)
                .ttl(Optional.fromNullable(ttl))
                .build();
        return consul.sessionClient().createSession(session).getId();
    }

    private void destroySession() {
        consul.sessionClient().destroySession(sessionId);
        sessionId = null;
    }

    public boolean acquireLock(long maxWait, TimeUnit unit) {
        KeyValueClient kv = consul.keyValueClient();
        sessionId = createSession();

        Optional<Value> value = kv.getValue(path);

        if (kv.acquireLock(path, sessionId)) {
            return true;
        }

        BigInteger index = BigInteger.valueOf(value.get().getModifyIndex());
        kv.getValue(path, QueryOptions.blockMinutes((int) unit.toMinutes(maxWait), index).build());

        if (!kv.acquireLock(path, sessionId)) {
            destroySession();
            return false;
        } else {
            return true;
        }
    }

    public void releaseLock() {
        try {
            KeyValueClient kv = consul.keyValueClient();
            Optional<Value> value = kv.getValue(path);

            if (value.isPresent()) {
                Optional<String> session = value.get().getSession();
                if (session.isPresent()) {
                    kv.releaseLock(path, session.get());
                }
            }
        } finally {
            destroySession();
        }
    }
}
