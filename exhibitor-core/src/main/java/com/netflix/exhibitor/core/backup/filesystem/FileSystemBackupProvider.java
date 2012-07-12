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

package com.netflix.exhibitor.core.backup.filesystem;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.netflix.exhibitor.core.backup.BackupMetaData;
import com.netflix.exhibitor.core.backup.BackupProvider;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.backup.BackupConfigSpec;
import java.io.File;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FileSystemBackupProvider implements BackupProvider
{
    private static final BackupConfigSpec CONFIG_DIRECTORY = new BackupConfigSpec("directory", "Destination Path", "The path of the directory where backups are written to", "", BackupConfigSpec.Type.STRING);

    private static final List<BackupConfigSpec> BACKUP_CONFIGS = Arrays.asList(CONFIG_DIRECTORY);

    @Override
    public List<BackupConfigSpec> getConfigs()
    {
        return BACKUP_CONFIGS;
    }

    @Override
    public boolean isValidConfig(Exhibitor exhibitor, Map<String, String> configValues)
    {
        String value = configValues.get(CONFIG_DIRECTORY.getKey());
        return (value != null) && (value.trim().length() > 0);
    }

    @Override
    public UploadResult uploadBackup(Exhibitor exhibitor, BackupMetaData backup, File source, Map<String, String> configValues) throws Exception
    {
        String      path = configValues.get(CONFIG_DIRECTORY.getKey());
        if ( path == null )
        {
            exhibitor.getLog().add(ActivityLog.Type.ERROR, "No backup directory set in config");
            return UploadResult.FAILED;
        }
        File        directory = new File(path);
        File        destinationDirectory = new File(directory, backup.getName());
        File        destinationFile = new File(destinationDirectory, Long.toString(backup.getModifiedDate()));
        if ( destinationFile.exists() )
        {
            return UploadResult.DUPLICATE;
        }
        
        if ( !destinationDirectory.isDirectory() && !destinationDirectory.mkdirs() )
        {
            exhibitor.getLog().add(ActivityLog.Type.ERROR, "Could not create backup directory: " + destinationDirectory);
            return UploadResult.FAILED;
        }

        List<BackupMetaData>    availableBackups = getAvailableBackups(exhibitor, configValues);

        Files.copy(source, destinationFile);

        UploadResult        result = UploadResult.SUCCEEDED;
        for ( BackupMetaData existing : availableBackups )
        {
            if ( existing.getName().equals(backup.getName()) )
            {
                deleteBackup(exhibitor, existing, configValues);
                result = UploadResult.REPLACED_OLD_VERSION;
            }
        }
        return result;
    }

    @Override
    public List<BackupMetaData> getAvailableBackups(Exhibitor exhibitor, Map<String, String> configValues) throws Exception
    {
        ImmutableList.Builder<BackupMetaData>   builder = ImmutableList.builder();
        String                                  pathname = configValues.get(CONFIG_DIRECTORY.getKey());
        if ( pathname != null )
        {
            File                                    directory = new File(pathname);
            if ( directory.isDirectory() )
            {
                File[] files = directory.listFiles();
                if ( files != null )
                {
                    for ( File nameDir : files )
                    {
                        if ( nameDir.isDirectory() )
                        {
                            File[] subFiles = nameDir.listFiles();
                            if ( subFiles != null )
                            {
                                for ( File version : subFiles )
                                {
                                    if ( version.isFile() )
                                    {
                                        builder.add(new BackupMetaData(nameDir.getName(), Long.parseLong(version.getName())));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return builder.build();
    }

    @Override
    public void deleteBackup(Exhibitor exhibitor, BackupMetaData backup, Map<String, String> configValues) throws Exception
    {
        File        directory = new File(configValues.get(CONFIG_DIRECTORY.getKey()));
        File        destinationDirectory = new File(directory, backup.getName());
        File        destinationFile = new File(destinationDirectory, Long.toString(backup.getModifiedDate()));
        if ( !destinationFile.delete() )
        {
            exhibitor.getLog().add(ActivityLog.Type.ERROR, "Could not delete old backup: " + destinationFile);
        }
    }

    @Override
    public void downloadBackup(Exhibitor exhibitor, BackupMetaData backup, OutputStream destination, Map<String, String> configValues) throws Exception
    {
        File        directory = new File(configValues.get(CONFIG_DIRECTORY.getKey()));
        File        nameDirectory = new File(directory, backup.getName());
        File        source = new File(nameDirectory, Long.toString(backup.getModifiedDate()));
        Files.copy(source, destination);
    }
}
