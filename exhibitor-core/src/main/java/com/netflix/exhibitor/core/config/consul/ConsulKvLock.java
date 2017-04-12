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

    /**
     * @param consul consul client instance for connecting to consul cluster
     * @param path consul key-value path to lock
     * @param name a descriptive name for the lock
     */
    public ConsulKvLock(Consul consul, String path, String name) {
        this.consul = consul;
        this.path = path;
        this.name = name;
    }

    private String createSession() {
        final ImmutableSession session = ImmutableSession.builder().name(name).build();
        return consul.sessionClient().createSession(session).getId();
    }

    public boolean acquireLock(long maxWait, TimeUnit unit) {
        KeyValueClient kv = consul.keyValueClient();
        String sessionId = createSession();

        Optional<Value> value = kv.getValue(path);

        if (kv.acquireLock(path, sessionId)) {
            return true;
        }

        BigInteger index = BigInteger.valueOf(value.get().getModifyIndex());
        kv.getValue(path, QueryOptions.blockMinutes((int) unit.toMinutes(maxWait), index).build());
        return kv.acquireLock(path, sessionId);
    }

    public void releaseLock() {
        KeyValueClient kv = consul.keyValueClient();
        Optional<Value> value = kv.getValue(path);

        if (value.isPresent()) {
            Optional<String> session = value.get().getSession();
            if (session.isPresent()) {
                kv.releaseLock(path, session.get());
            }
        }
    }
}
