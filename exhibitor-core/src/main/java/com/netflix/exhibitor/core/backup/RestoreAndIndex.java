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

package com.netflix.exhibitor.core.backup;

import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.activity.Activity;
import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.index.IndexActivity;
import com.netflix.exhibitor.core.index.IndexerUtil;
import java.io.File;

public class RestoreAndIndex implements Activity
{
    private final Exhibitor exhibitor;
    private final BackupMetaData backup;

    /**
     * @param exhibitor instance
     * @param backup the backup to restore
     */
    public RestoreAndIndex(Exhibitor exhibitor, BackupMetaData backup)
    {
        this.exhibitor = exhibitor;
        this.backup = backup;
    }

    @Override
    public void completed(boolean wasSuccessful)
    {
    }

    @Override
    public Boolean call() throws Exception
    {
        File        destinationFile = null;
        try
        {
            destinationFile = File.createTempFile("exhibitor", ".tmp");
            exhibitor.getBackupManager().restore(backup, destinationFile);

            final File        finalDestinationFile = destinationFile;
            IndexActivity.CompletionListener listener = new IndexActivity.CompletionListener()
            {
                @Override
                public void completed()
                {
                    deleteTempFile(finalDestinationFile);
                }
            };
            IndexerUtil.startIndexing(exhibitor, destinationFile, listener);
        }
        catch ( Exception e )
        {
            deleteTempFile(destinationFile);
            exhibitor.getLog().add(ActivityLog.Type.ERROR, "Could not complete restore/index: " + backup, e);
        }

        return true;
    }

    private void deleteTempFile(File f)
    {
        if ( !f.delete() )
        {
            exhibitor.getLog().add(ActivityLog.Type.ERROR, "Could not delete temp file: " + f);
        }
    }
}
