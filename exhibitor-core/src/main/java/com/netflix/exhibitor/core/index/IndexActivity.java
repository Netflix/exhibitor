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

import com.google.common.io.Closeables;
import com.netflix.exhibitor.core.activity.Activity;
import com.netflix.exhibitor.core.activity.ActivityLog;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IndexActivity implements Activity
{
    private final LogIndexer indexer;
    private final ActivityLog log;
    private final CompletionListener completionListener;

    public interface CompletionListener
    {
        public void completed();
    }

    public IndexActivity(LogIndexer indexer, ActivityLog log, CompletionListener completionListener)
    {
        this.indexer = indexer;
        this.log = log;
        this.completionListener = completionListener;
    }

    @Override
    public void completed(boolean wasSuccessful)
    {
        Closeables.closeQuietly(indexer);
        if ( completionListener != null )
        {
            completionListener.completed();
        }
    }

    @Override
    public Boolean call() throws Exception
    {
        ExecutorService     server = Executors.newSingleThreadExecutor();
        server.submit
        (
            new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    try
                    {
                        while ( !Thread.currentThread().isInterrupted() )
                        {
                            Thread.sleep(1000);
                            log.add(ActivityLog.Type.INFO, "Indexing " + indexer.getLogSourceName() + " " + indexer.getPercentDone() + "%");
                        }
                    }
                    catch ( InterruptedException e )
                    {
                        Thread.currentThread().interrupt();
                    }
                    return null;
                }
            }
        );
        try
        {
            indexer.index();
            log.add(ActivityLog.Type.INFO, "Indexing " + indexer.getLogSourceName() + " done");
        }
        catch ( Exception e )
        {
            log.add(ActivityLog.Type.ERROR, "Indexing " + indexer.getLogSourceName(), e);
        }
        server.shutdownNow();
        return true;
    }
}
