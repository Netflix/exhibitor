package com.netflix.exhibitor.core.config.consul;

import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.config.PseudoLock;
import com.orbitz.consul.Consul;

import java.util.concurrent.TimeUnit;

public class ConsulPseudoLock implements PseudoLock {

    private final ConsulKvLock lock;

    public ConsulPseudoLock(Consul consul, String prefix) {
        String path = prefix.endsWith("/") ? prefix + "pseudo-lock" : prefix + "/pseudo-lock";
        this.lock = new ConsulKvLock(consul, path, "pseudo-lock");
    }

    @Override
    public boolean lock(ActivityLog log, long maxWait, TimeUnit unit) throws Exception {
        if (!lock.acquireLock(maxWait, unit)) {
            log.add(ActivityLog.Type.ERROR,
                    String.format("Could not acquire lock within %d ms", unit.toMillis(maxWait)));
            return false;
        }

        return true;
    }

    @Override
    public void unlock() throws Exception {
        lock.releaseLock();
    }
}
