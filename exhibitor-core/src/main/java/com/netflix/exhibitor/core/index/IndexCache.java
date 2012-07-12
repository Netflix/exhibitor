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

package com.netflix.exhibitor.core.index;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.netflix.exhibitor.core.activity.ActivityLog;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
public class IndexCache implements Closeable
{
    private final LoadingCache<File, IndexMetaData> metaDataCache = CacheBuilder
        .newBuilder()
        .expireAfterAccess(MAX_CACHE_MS, TimeUnit.MILLISECONDS)
        .build
        (
            new CacheLoader<File, IndexMetaData>()
            {
                @Override
                public IndexMetaData load(File indexDirectory) throws Exception
                {
                    return IndexMetaData.read(IndexMetaData.getMetaDataFile(indexDirectory));
                }
            }
        );

    private final ConcurrentMap<File, LogSearchHolder>  indexCache = Maps.newConcurrentMap();
    private final AtomicBoolean                         isOpen = new AtomicBoolean(true);
    private final ActivityLog                           log;

    private static class LogSearchHolder
    {
        // all protected by synchronization
        private LogSearch               logSearch;
        private int                     useCount = 0;
        private long                    lastUse = System.currentTimeMillis();
        private boolean                 markedForDeletion = false;
    }

    private static final int        MAX_CACHE_MS = (int)TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

    public IndexCache(ActivityLog log)
    {
        this.log = log;
    }

    @Override
    public void close() throws IOException
    {
        Preconditions.checkArgument(isOpen.compareAndSet(true, false), "Cache is closed");
        clean();
    }

    public IndexMetaData    getMetaData(File indexDirectory) throws Exception
    {
        Preconditions.checkArgument(isOpen.get(), "Cache is closed");

        return metaDataCache.get(indexDirectory);
    }

    public void     markForDeletion(File indexDirectory)
    {
        Preconditions.checkArgument(isOpen.get(), "Cache is closed");

        LogSearchHolder     holder = getHolder(indexDirectory);
        synchronized(holder)
        {
            holder.markedForDeletion = true;
        }
        clean();
    }

    public LogSearch        getLogSearch(File indexDirectory) throws Exception
    {
        Preconditions.checkArgument(isOpen.get(), "Cache is closed");

        clean();

        final LogSearchHolder     holder = getHolder(indexDirectory);

        LogSearch                 logSearch;
        synchronized(holder)
        {
            if ( holder.logSearch == null )
            {
                Preconditions.checkArgument(holder.useCount == 0, "use count is non zero but instance is null: " + holder.useCount);
                holder.logSearch = new LogSearch(indexDirectory);
            }

            ++holder.useCount;
            Preconditions.checkArgument(holder.useCount > 0, "use count has rolled over: " + holder.useCount);

            holder.lastUse = System.currentTimeMillis();

            logSearch = holder.logSearch;
        }

        return logSearch;
    }

    public void             releaseLogSearch(File indexDirectory)
    {
        Preconditions.checkArgument(isOpen.get(), "Cache is closed");

        LogSearchHolder     holder = indexCache.get(indexDirectory);
        holder = Preconditions.checkNotNull(holder, "No entry found for index being released: " + indexDirectory);

        synchronized(holder)
        {
            Preconditions.checkArgument(holder.useCount > 0, "non positive use count in release: " + holder.useCount);
            --holder.useCount;
        }
    }

    private LogSearchHolder getHolder(File indexDirectory)
    {
        LogSearchHolder newHolder = new LogSearchHolder();
        LogSearchHolder oldHolder = indexCache.putIfAbsent(indexDirectory, newHolder);
        return (oldHolder != null) ? oldHolder : newHolder;
    }

    public void delete(File indexDirectory)
    {
        File[] files = indexDirectory.listFiles();
        if ( files != null )
        {
            for ( File f : files )
            {
                if ( !f.delete() )
                {
                    log.add(ActivityLog.Type.ERROR, "Could not delete: " + f);
                }
            }
        }
        if ( !indexDirectory.delete() )
        {
            log.add(ActivityLog.Type.ERROR, "Could not delete: " + indexDirectory);
        }
        File    metaDataFile = IndexMetaData.getMetaDataFile(indexDirectory);
        if ( !metaDataFile.delete() )
        {
            log.add(ActivityLog.Type.ERROR, "Could not delete: " + metaDataFile);
        }

        log.add(ActivityLog.Type.INFO, "Index deleted: " + indexDirectory.getName());
    }

    private void        clean()
    {
        Iterator<Map.Entry<File, LogSearchHolder>> iterator = indexCache.entrySet().iterator();
        while ( iterator.hasNext() )
        {
            final Map.Entry<File, LogSearchHolder> entry = iterator.next();
            final LogSearchHolder     holder = entry.getValue();
            synchronized(holder)
            {
                if ( holder.useCount == 0 )
                {
                    if ( !isOpen.get() || holder.markedForDeletion || ((System.currentTimeMillis() - holder.lastUse) > MAX_CACHE_MS) )
                    {
                        if ( holder.logSearch != null )
                        {
                            holder.logSearch.close();
                            holder.logSearch = null;
                        }
                        iterator.remove();

                        if ( holder.markedForDeletion )
                        {
                            delete(entry.getKey());
                        }
                    }
                }
            }
        }
    }
}
