package com.netflix.exhibitor.core.config;

import java.util.concurrent.TimeUnit;

/**
 * An approximation of a lock using S3
 */
public interface PseudoLock
{
    /**
     * Acquire the lock, blocking until it is acquired
     *
     * @throws Exception errors
     */
    public void  lock() throws Exception;

    /**
     * Acquire the lock, blocking at most <code>maxWait</code> until it is acquired
     *
     * @param maxWait max time to wait
     * @param unit time unit
     * @return true if the lock was acquired
     * @throws Exception errors
     */
    public boolean lock(long maxWait, TimeUnit unit) throws Exception;

    /**
     * Release the lock
     *
     * @throws Exception errors
     */
    public void unlock() throws Exception;
}
