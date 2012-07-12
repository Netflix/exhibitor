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

import com.google.common.io.InputSupplier;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.activity.QueueGroups;
import com.netflix.exhibitor.core.config.InstanceConfig;
import com.netflix.exhibitor.core.config.StringConfigs;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class IndexerUtil
{
    public static void      startIndexing(Exhibitor exhibitor, File path) throws Exception
    {
        startIndexing(exhibitor, path, null);
    }

    public static void      startIndexing(Exhibitor exhibitor, final File path, IndexActivity.CompletionListener listener) throws Exception
    {
        if ( path.isDirectory() )
        {
            final DirectoryInputStream      stream = new DirectoryInputStream(path);    // doesn't actually open any streams until reading starts
            InputSupplier<InputStream>      source = new InputSupplier<InputStream>()
            {
                @Override
                public InputStream getInput() throws IOException
                {
                    return stream;
                }
            };
            startIndexing(exhibitor, source, path.getName(), stream.length(), listener);
        }
        else
        {
            InputSupplier<InputStream>  source = new InputSupplier<InputStream>()
            {
                @Override
                public InputStream getInput() throws IOException
                {
                    return new BufferedInputStream(new FileInputStream(path));
                }
            };
            startIndexing(exhibitor, source, path.getName(), path.length(), listener);
        }
    }

    private static void      startIndexing(Exhibitor exhibitor, InputSupplier<InputStream> source, String name, long length, IndexActivity.CompletionListener listener) throws Exception
    {
        InstanceConfig  config = exhibitor.getConfigManager().getConfig();

        File indexDirectory = new File(config.getString(StringConfigs.LOG_INDEX_DIRECTORY), "exhibitor-" + System.currentTimeMillis());

        LogIndexer      logIndexer;
        try
        {
            logIndexer = new LogIndexer(exhibitor, source, name, length, indexDirectory);
        }
        catch ( Exception e )
        {
            if ( listener != null )
            {
                listener.completed();
            }
            throw e;
        }
        if ( logIndexer.isValid() )
        {
            IndexActivity   activity = new IndexActivity(logIndexer, exhibitor.getLog(), listener);
            exhibitor.getActivityQueue().add(QueueGroups.MAIN, activity);
        }
        else
        {
            exhibitor.getLog().add(ActivityLog.Type.ERROR, "Invalid log file can't be indexed: " + name);
            if ( listener != null )
            {
                listener.completed();
            }
        }
    }
    
    private IndexerUtil()
    {
    }
}
