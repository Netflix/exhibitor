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

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.backup.BackupMetaData;
import com.netflix.exhibitor.core.backup.BackupStream;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class IndexProcessor
{
    private final Exhibitor exhibitor;

    public IndexProcessor(Exhibitor exhibitor)
    {
        this.exhibitor = exhibitor;
    }

    public void     process(File directory) throws Exception
    {
        if ( !directory.exists() && !directory.mkdirs() )
        {
            throw new IOException("Index Build: could not make directory: " + directory);
        }

        Exception           exception = null;
        IndexBuilder        builder = new IndexBuilder(directory);
        try
        {
            builder.open();
            addBackups(builder);
            addActive(builder);

            builder.writeMetaData();
        }
        catch ( Exception e )
        {
            exception = e;
        }
        finally
        {
            builder.close();
            if ( exception != null )
            {
                cleanDirectory(directory);
                //noinspection ThrowFromFinallyBlock
                throw exception;
            }
        }

        if ( builder.getCurrentCount() == 0 )
        {
            exhibitor.getLog().add(ActivityLog.Type.INFO, "Index is empty and will be deleted: " + directory);
            cleanDirectory(directory);
        }
        else
        {
            exhibitor.getLog().add(ActivityLog.Type.INFO, "Index completed: " + directory);
        }
    }

    private void addActive(IndexBuilder builder) throws Exception
    {
        ZooKeeperLogFiles       zooKeeperLogFiles = new ZooKeeperLogFiles(exhibitor);
        List<File>              paths = zooKeeperLogFiles.getPaths();
        Collections.sort
        (
            paths,
            new Comparator<File>()
            {
                @Override
                public int compare(File o1, File o2)
                {
                    long        diff = o1.lastModified() - o2.lastModified();
                    return (diff < 0) ? -1 : ((diff > 0) ? 1 : 0);
                }
            }
        );

        int     index = 0;
        for ( File f : paths )
        {
            exhibitor.getLog().add(ActivityLog.Type.INFO, String.format("Indexing active log %d of %d", ++index, paths.size()));

            if ( f.exists() )
            {
                InputStream         in = new BufferedInputStream(new FileInputStream(f));
                try
                {
                    builder.add(in);
                }
                finally
                {
                    Closeables.closeQuietly(in);
                }
            }
        }
    }

    private void addBackups(IndexBuilder builder) throws Exception
    {
        exhibitor.getLog().add(ActivityLog.Type.ERROR, "Index Build: Getting available backups");
        List<BackupMetaData> availableBackups = Lists.newArrayList(exhibitor.getBackupManager().getAvailableBackups());
        Collections.sort
        (
            availableBackups,
            new Comparator<BackupMetaData>()
            {
                @Override
                public int compare(BackupMetaData o1, BackupMetaData o2)
                {
                    long diff = o1.getModifiedDate() - o2.getModifiedDate();
                    return (diff < 0) ? -1 : ((diff > 0) ? 1 : 0);
                }
            }
        );
        exhibitor.getLog().add(ActivityLog.Type.ERROR, "Index Build: there are " + availableBackups.size() + " available backups");

        int     index = 0;
        for ( BackupMetaData metaData : availableBackups )
        {
            exhibitor.getLog().add(ActivityLog.Type.INFO, String.format("Index Build: indexing backup log %d of %d", ++index, availableBackups.size()));

            BackupStream backupStream = exhibitor.getBackupManager().getBackupStream(metaData);
            if ( backupStream != null )
            {
                try
                {
                    builder.add(backupStream.getStream());
                }
                finally
                {
                    Closeables.closeQuietly(backupStream);
                }
            }
        }
    }

    private void cleanDirectory(File directory)
    {
        File[] files = directory.listFiles();
        if ( files != null )
        {
            for ( File f : files )
            {
                if ( f.isDirectory() )
                {
                    cleanDirectory(f);
                }
                if ( !f.delete() )
                {
                    exhibitor.getLog().add(ActivityLog.Type.ERROR, "Index Build: could not delete: " + f);
                }
            }
        }

        if ( !directory.delete() )
        {
            exhibitor.getLog().add(ActivityLog.Type.ERROR, "Index Build: could not delete: " + directory);
        }
    }
}
