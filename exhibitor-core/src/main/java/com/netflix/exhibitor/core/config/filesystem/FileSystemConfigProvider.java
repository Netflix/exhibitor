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

package com.netflix.exhibitor.core.config.filesystem;

import com.google.common.io.Closeables;
import com.netflix.exhibitor.core.config.AutoManageLockArguments;
import com.netflix.exhibitor.core.config.ConfigCollection;
import com.netflix.exhibitor.core.config.ConfigProvider;
import com.netflix.exhibitor.core.config.LoadedInstanceConfig;
import com.netflix.exhibitor.core.config.PropertyBasedInstanceConfig;
import com.netflix.exhibitor.core.config.PseudoLock;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.channels.FileLock;
import java.util.Properties;

public class FileSystemConfigProvider implements ConfigProvider
{
    private final File propertiesDirectory;
    private final String propertyFileName;
    private final Properties defaults;
    private final AutoManageLockArguments autoManageLockArguments;

    /**
     *
     *
     *
     * @param propertiesDirectory where to store the properties
     * @param propertyFileName name of the file to store properties in
     * @param autoManageLockArguments config arguments for auto managing instances
     * @throws IOException if directory cannot be created
    */
    public FileSystemConfigProvider(File propertiesDirectory, String propertyFileName, AutoManageLockArguments autoManageLockArguments) throws IOException
    {
        this(propertiesDirectory, propertyFileName, new Properties(), autoManageLockArguments);
    }

    /**
     *
     *
     *
     * @param propertiesDirectory where to store the properties
     * @param propertyFileName name of the file to store properties in
     * @param defaults default values  @throws IOException if directory cannot be created
     * @param autoManageLockArguments config arguments for auto managing instances
     * @throws IOException if directory cannot be created
     */
    public FileSystemConfigProvider(File propertiesDirectory, String propertyFileName, Properties defaults, AutoManageLockArguments autoManageLockArguments) throws IOException
    {
        this.propertiesDirectory = propertiesDirectory;
        this.propertyFileName = propertyFileName;
        this.defaults = defaults;
        this.autoManageLockArguments = autoManageLockArguments;

        if ( propertiesDirectory.exists() && !propertiesDirectory.isDirectory() )
        {
            throw new IllegalArgumentException("The file argument should be a directory: " + propertiesDirectory);
        }

        if ( !propertiesDirectory.exists() && !propertiesDirectory.mkdirs() )
        {
            throw new IOException("Could not create directory: " + propertiesDirectory);
        }
    }

    @Override
    public void start() throws Exception
    {
        // NOP
    }

    @Override
    public void close() throws IOException
    {
        // NOP
    }

    @Override
    public PseudoLock newPseudoLock() throws Exception
    {
        return new FileSystemPseudoLock(propertiesDirectory, autoManageLockArguments.getPrefix(), autoManageLockArguments.getTimeoutMs(), autoManageLockArguments.getPollingMs());
    }

    @Override
    public LoadedInstanceConfig loadConfig() throws Exception
    {
        File            propertiesFile = new File(propertiesDirectory, propertyFileName);
        Properties      properties = new Properties();
        if ( propertiesFile.exists() )
        {
            RandomAccessFile    raf = new RandomAccessFile(propertiesFile, "rw");
            try
            {
                FileLock        lock = raf.getChannel().lock();
                try
                {
                    properties.load(Channels.newInputStream(raf.getChannel()));
                }
                finally
                {
                    lock.release();
                }
            }
            finally
            {
                Closeables.closeQuietly(raf);
            }
        }
        PropertyBasedInstanceConfig config = new PropertyBasedInstanceConfig(properties, defaults);
        return new LoadedInstanceConfig(config, propertiesFile.lastModified());
    }

    @Override
    public LoadedInstanceConfig storeConfig(ConfigCollection config, long compareVersion) throws Exception
    {
        File                            propertiesFile = new File(propertiesDirectory, propertyFileName);
        if ( propertiesFile.lastModified() != compareVersion )
        {
            return null;
        }

        PropertyBasedInstanceConfig     propertyBasedInstanceConfig = new PropertyBasedInstanceConfig(config);

        long                lastModified = 0;
        FileOutputStream    fileStream = new FileOutputStream(propertiesFile);
        OutputStream        out = new BufferedOutputStream(fileStream);
        try
        {
            FileLock lock = fileStream.getChannel().lock();
            try
            {
                propertyBasedInstanceConfig.getProperties().store(out, "Auto-generated by Exhibitor");
                lastModified = propertiesFile.lastModified();
            }
            finally
            {
                lock.release();
            }
        }
        finally
        {
            Closeables.closeQuietly(out);
        }

        return new LoadedInstanceConfig(propertyBasedInstanceConfig, lastModified);
    }
}
