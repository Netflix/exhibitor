/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.exhibitor.core.config;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.activity.ActivityLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public abstract class PseudoLockBase implements PseudoLock
{
    private static final Logger log = LoggerFactory.getLogger(PseudoLockBase.class);

    private final String                    lockPrefix;
    private final int                       timeoutMs;
    private final int                       pollingMs;
    private final int                       settlingMs;

    // all guarded by sync
    private long      lastUpdateMs = 0;
    private boolean   ownsTheLock;
    private String    key;
    private long      lockStartMs = 0;

    private static final String         content = Exhibitor.getHostname();

    private static final Random             random = new SecureRandom();

    private static final String     SEPARATOR = "_";
    private static final int        DEFAULT_SETTLING_MS = 5000;

    private static final int        MISSING_KEY_FACTOR = 10;

    /**
     * @param lockPrefix key prefix
     * @param timeoutMs max age for locks
     * @param pollingMs how often to poll S3
     */
    public PseudoLockBase(String lockPrefix, int timeoutMs, int pollingMs)
    {
        this(lockPrefix, timeoutMs, pollingMs, DEFAULT_SETTLING_MS);
    }

    /**
     * @param lockPrefix key prefix
     * @param timeoutMs max age for locks
     * @param pollingMs how often to poll S3
     * @param settlingMs how long to wait for S3 to reach consistency
     */
    public PseudoLockBase(String lockPrefix, int timeoutMs, int pollingMs, int settlingMs)
    {
        this.settlingMs = settlingMs;
        Preconditions.checkArgument(!lockPrefix.contains(SEPARATOR), "lockPrefix cannot contain " + SEPARATOR);

        this.pollingMs = pollingMs;
        this.lockPrefix = lockPrefix;
        this.timeoutMs = timeoutMs;
    }

    /**
     * Acquire the lock, blocking at most <code>maxWait</code> until it is acquired
     *
     *
     * @param log the logger
     * @param maxWait max time to wait
     * @param unit time unit
     * @return true if the lock was acquired
     * @throws Exception errors
     */
    public synchronized boolean lock(ActivityLog log, long maxWait, TimeUnit unit) throws Exception
    {
        if ( ownsTheLock )
        {
            throw new IllegalStateException("Already locked");
        }

        lockStartMs = System.currentTimeMillis();

        key = lockPrefix + SEPARATOR + newRandomSequence();

        long        startMs = System.currentTimeMillis();
        boolean     hasMaxWait = (unit != null);
        long        maxWaitMs = hasMaxWait ? TimeUnit.MILLISECONDS.convert(maxWait, unit) : Long.MAX_VALUE;
        Preconditions.checkState(maxWaitMs >= settlingMs, String.format("The maxWait ms (%d) is less than the settling ms (%d)", maxWaitMs, settlingMs));

        createFile(key, content.getBytes());

        for(;;)
        {
            checkUpdate();
            if ( ownsTheLock )
            {
                break;
            }
            long        thisWaitMs;
            if ( hasMaxWait )
            {
                long        elapsedMs = System.currentTimeMillis() - startMs;
                thisWaitMs = maxWaitMs - elapsedMs;
                if ( thisWaitMs <= 0 )
                {
                    log.add(ActivityLog.Type.ERROR, String.format("Could not acquire lock within %d ms, polling: %d ms, key: %s", maxWaitMs, pollingMs, key));
                    break;
                }
            }
            else
            {
                thisWaitMs = pollingMs;
            }
            wait(Math.min(pollingMs, thisWaitMs));
        }
        return ownsTheLock;
    }

    /**
     * Release the lock
     *
     * @throws Exception errors
     */
    public synchronized void unlock() throws Exception
    {
        deleteFile(key);
        notifyAll();
        ownsTheLock = false;
    }

    protected abstract void createFile(String key, byte[] contents) throws Exception;

    protected abstract void deleteFile(String key) throws Exception;

    protected abstract List<String> getFileNames(String lockPrefix) throws Exception;

    public String getLockPrefix()
    {
        return lockPrefix;
    }

    private void checkUpdate() throws Exception
    {
        if ( (System.currentTimeMillis() - lastUpdateMs) < pollingMs )
        {
            return;
        }

        List<String>        keys = getFileNames(lockPrefix);
        log.debug(String.format("keys: %s", keys));
        keys = cleanOldObjects(keys);
        log.debug(String.format("cleaned keys: %s", keys));
        Collections.sort(keys);

        if ( keys.size() > 0 )
        {
            String      lockerKey = keys.get(0);
            long        lockerAge = System.currentTimeMillis() - getEpochStampForKey(key);
            ownsTheLock = (lockerKey.equals(key) && (lockerAge >= settlingMs));
        }
        else
        {
            long        elapsed = System.currentTimeMillis() - lockStartMs;
            if ( elapsed > (settlingMs * MISSING_KEY_FACTOR) )
            {
                throw new Exception(String.format("Our key is missing. Key: %s, Elapsed: %d, Max Wait: %d", key, elapsed, settlingMs * MISSING_KEY_FACTOR));
            }
        }

        lastUpdateMs = System.currentTimeMillis();

        notifyAll();
    }

    private List<String>    cleanOldObjects(List<String> keys) throws Exception
    {
        List<String>        newKeys = Lists.newArrayList();
        for ( String key : keys )
        {
            long    epochStamp = getEpochStampForKey(key);
            if ( !key.equals(this.key) && ((System.currentTimeMillis() - epochStamp) > timeoutMs) )
            {
                deleteFile(key);
            }
            else
            {
                newKeys.add(key);
            }
        }
        return newKeys;
    }

    private static long getEpochStampForKey(String key)
    {
        String[]        parts = key.split(SEPARATOR);
        long            millisecondStamp = 0;
        try
        {
            millisecondStamp = Long.parseLong(parts[1]);
        }
        catch ( NumberFormatException ignore )
        {
            // ignore
        }
        return millisecondStamp;
    }

    private String newRandomSequence()
    {
        return "" + System.currentTimeMillis() + SEPARATOR + Math.abs(random.nextLong());
    }
}
