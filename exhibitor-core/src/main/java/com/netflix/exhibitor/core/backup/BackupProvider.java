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
import java.io.File;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public interface BackupProvider
{
    /**
     * Return set of provider-specific config needed
     *
     * @return configs (or empty list)
     */
    public List<BackupConfigSpec> getConfigs();

    public enum UploadResult
    {
        FAILED,
        SUCCEEDED,
        DUPLICATE,
        REPLACED_OLD_VERSION
    }

    /**
     * Upload an object into the backup.
     *
     * @param exhibitor instance
     * @param metaData identity of the backup
     * @param source the source file
     * @param configValues values for provider-specific config
     * @return the upload result
     * @throws Exception any errors
     */
    public UploadResult uploadBackup(Exhibitor exhibitor, BackupMetaData metaData, File source, Map<String, String> configValues) throws Exception;

    /**
     * Return the set of available backups
     *
     * @param exhibitor instance
     * @param configValues values for provider-specific config
     * @return backups
     * @throws Exception any errors
     */
    public List<BackupMetaData> getAvailableBackups(Exhibitor exhibitor, Map<String, String> configValues) throws Exception;

    /**
     * Return a stream for the specified backup
     *
     * @param exhibitor instance
     * @param metaData the backup to get
     * @param configValues values for provider-specific config
     * @return the stream or null if the stream doesn't exist
     * @throws Exception errors
     */
    public BackupStream getBackupStream(Exhibitor exhibitor, BackupMetaData metaData, Map<String, String> configValues) throws Exception;

    /**
     * Delete the given backup
     *
     * @param exhibitor instance
     * @param backup backup to delete
     * @param configValues values for provider-specific config
     * @throws Exception any errors
     */
    public void     deleteBackup(Exhibitor exhibitor, BackupMetaData backup, Map<String, String> configValues) throws Exception;

    /**
     * Download a backed-up object
     *
     *
     *
     * @param exhibitor instance
     * @param backup the backup to pull down
     * @param destination destination stream
     * @param configValues values for provider-specific config
     * @throws Exception any errors
     */
    public void     downloadBackup(Exhibitor exhibitor, BackupMetaData backup, OutputStream destination, Map<String, String> configValues) throws Exception;

    /**
     * Determine if the provider-specific config is in a good state. If not, backups/restores will be disallowed
     *
     * @param exhibitor instance
     * @param configValues values for provider-specific config
     * @return true/false
     */
    public boolean  isValidConfig(Exhibitor exhibitor, Map<String, String> configValues);
}
