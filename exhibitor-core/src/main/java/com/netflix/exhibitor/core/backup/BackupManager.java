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

import com.google.common.base.Optional;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.activity.Activity;
import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.activity.OnOffRepeatingActivity;
import com.netflix.exhibitor.core.activity.QueueGroups;
import com.netflix.exhibitor.core.activity.RepeatingActivity;
import com.netflix.exhibitor.core.activity.RepeatingActivityImpl;
import com.netflix.exhibitor.core.config.ConfigListener;
import com.netflix.exhibitor.core.config.EncodedConfigParser;
import com.netflix.exhibitor.core.config.InstanceConfig;
import com.netflix.exhibitor.core.config.IntConfigs;
import com.netflix.exhibitor.core.config.StringConfigs;
import com.netflix.exhibitor.core.controlpanel.ControlPanelTypes;
import com.netflix.exhibitor.core.index.ZooKeeperLogFiles;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

public class BackupManager implements Closeable
{
    private final Exhibitor exhibitor;
    private final Optional<BackupProvider> backupProvider;
    private final RepeatingActivity repeatingActivity;
    private final AtomicLong lastRollCheck = new AtomicLong(0);

    /**
     * @param exhibitor main instance
     * @param backupProvider provider
     */
    public BackupManager(final Exhibitor exhibitor, BackupProvider backupProvider)
    {
        this.exhibitor = exhibitor;
        this.backupProvider = Optional.fromNullable(backupProvider);

        final Activity activity = new Activity()
        {
            @Override
            public void completed(boolean wasSuccessful)
            {
            }

            @Override
            public Boolean call() throws Exception
            {
                doBackup();
                return true;
            }
        };

        repeatingActivity = new OnOffRepeatingActivity
        (
            new OnOffRepeatingActivity.Factory()
            {
                @Override
                public RepeatingActivity newRepeatingActivity(long timePeriodMs)
                {
                    return new RepeatingActivityImpl(exhibitor.getLog(), exhibitor.getActivityQueue(), QueueGroups.IO, activity, getBackupPeriodMs());
                }
            },
            getBackupPeriodMs()
        );
    }

    private int getBackupPeriodMs()
    {
        InstanceConfig config = exhibitor.getConfigManager().getConfig();
        return config.getInt(IntConfigs.BACKUP_PERIOD_MS);
    }

    /**
     * Manager must be started
     */
    public void start()
    {
        if ( isActive() )
        {
            repeatingActivity.start();
            exhibitor.getConfigManager().addConfigListener
                (
                    new ConfigListener()
                    {
                        @Override
                        public void configUpdated()
                        {
                            repeatingActivity.setTimePeriodMs(exhibitor.getConfigManager().getConfig().getInt(IntConfigs.BACKUP_PERIOD_MS));
                        }
                    }
                );
        }
    }

    @Override
    public void close() throws IOException
    {
        if ( isActive() )
        {
            repeatingActivity.close();
        }
    }

    /**
     * Returns true if backup has been configured. Running Exhibitor without backups is supported
     *
     * @return true/false
     */
    public boolean isActive()
    {
        return backupProvider.isPresent();
    }

    /**
     * Return list of available backups
     *
     * @return backups
     * @throws Exception errors
     */
    public List<BackupMetaData> getAvailableBackups() throws Exception
    {
        Map<String, String>       config = getBackupConfig();
        return backupProvider.get().getAvailableBackups(exhibitor, config);
    }

    /**
     * Return a stream for the specified backup
     *
     * @param metaData the backup to get
     * @return the stream or null if the stream doesn't exist
     * @throws Exception errors
     */
    public BackupStream getBackupStream(BackupMetaData metaData) throws Exception
    {
        return backupProvider.get().getBackupStream(exhibitor, metaData, getBackupConfig());
    }

    /**
     * Return the stored backup config
     *
     * @return backup config
     */
    public EncodedConfigParser getBackupConfigParser()
    {
        return new EncodedConfigParser(exhibitor.getConfigManager().getConfig().getString(StringConfigs.BACKUP_EXTRA));
    }

    /**
     * Return the names of backup config, display info, etc.
     *
     * @return specs
     */
    public List<BackupConfigSpec> getConfigSpecs()
    {
        return backupProvider.get().getConfigs();
    }

    /**
     * Restore the given key to the given file
     *
     * @param backup the backup to pull down
     * @param destinationFile the file
     * @throws Exception errors
     */
    public void restore(BackupMetaData backup, File destinationFile) throws Exception
    {
        File                    tempFile = File.createTempFile("exhibitor-backup", ".tmp");
        OutputStream            out = new FileOutputStream(tempFile);
        InputStream in = null;
        try
        {
            backupProvider.get().downloadBackup(exhibitor, backup, out, getBackupConfig());
            Closeables.closeQuietly(out);
            out = null;

            out = new FileOutputStream(destinationFile);
            in = new GZIPInputStream(new FileInputStream(tempFile));

            ByteStreams.copy(in, out);
        }
        finally
        {
            Closeables.closeQuietly(in);
            Closeables.closeQuietly(out);
            if ( !tempFile.delete() )
            {
                exhibitor.getLog().add(ActivityLog.Type.ERROR, "Could not delete temp file (for restore): " + tempFile);
            }
        }
    }

    private void doBackup() throws Exception
    {
        if ( !exhibitor.getControlPanelValues().isSet(ControlPanelTypes.BACKUPS) )
        {
            return;
        }

        ZooKeeperLogFiles zooKeeperLogFiles = new ZooKeeperLogFiles(exhibitor);
        if ( !zooKeeperLogFiles.isValid() )
        {
            return;
        }

        Map<String, String> config = getBackupConfig();

        BackupProvider      provider = backupProvider.get();
        if ( !provider.isValidConfig(exhibitor, config) )
        {
            return;
        }

        for ( File f : zooKeeperLogFiles.getPaths() )
        {
            TempCompressedFile      tempCompressedFile = new TempCompressedFile(f);
            try
            {
                tempCompressedFile.compress();

                BackupMetaData          metaData = new BackupMetaData(f.getName(), f.lastModified());
                BackupProvider.UploadResult result = provider.uploadBackup(exhibitor, metaData, tempCompressedFile.getTempFile(), config);
                switch ( result )
                {
                    case SUCCEEDED:
                    {
                        exhibitor.getLog().add(ActivityLog.Type.DEBUG, "Backing up: " + f);
                        break;
                    }

                    case DUPLICATE:
                    {
                        // ignore
                        break;
                    }

                    case REPLACED_OLD_VERSION:
                    {
                        exhibitor.getLog().add(ActivityLog.Type.DEBUG, "Updated back up for: " + f);
                        break;
                    }
                }
            }
            finally
            {
                if ( !tempCompressedFile.getTempFile().delete() )
                {
                    exhibitor.getLog().add(ActivityLog.Type.ERROR, "Could not delete temp file: " + tempCompressedFile.getTempFile());
                }
            }
        }

        doRoll(config);
    }

    private Map<String, String> getBackupConfig()
    {
        String              backupExtra = exhibitor.getConfigManager().getConfig().getString(StringConfigs.BACKUP_EXTRA);
        EncodedConfigParser encodedConfigParser = new EncodedConfigParser(backupExtra);
        return encodedConfigParser.getSortedMap();
    }

    private void doRoll(Map<String, String> config) throws Exception
    {
        long        elapsed = System.currentTimeMillis() - lastRollCheck.get();
        if ( elapsed < (exhibitor.getConfigManager().getConfig().getInt(IntConfigs.BACKUP_MAX_STORE_MS) / 3) )
        {
            return;
        }

        exhibitor.getLog().add(ActivityLog.Type.DEBUG, "Checking for elapsed backups");

        List<BackupMetaData>        availableBackups = backupProvider.get().getAvailableBackups(exhibitor, config);
        for ( BackupMetaData backup : availableBackups )
        {
            long        age = System.currentTimeMillis() - backup.getModifiedDate();
            if ( age > exhibitor.getConfigManager().getConfig().getInt(IntConfigs.BACKUP_MAX_STORE_MS) )
            {
                exhibitor.getLog().add(ActivityLog.Type.DEBUG, "Cleaning backup: " + backup);
                backupProvider.get().deleteBackup(exhibitor, backup, config);
            }
        }

        lastRollCheck.set(System.currentTimeMillis());
    }
}
