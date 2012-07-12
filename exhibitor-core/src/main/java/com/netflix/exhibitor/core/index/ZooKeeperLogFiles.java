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

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closeables;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.config.StringConfigs;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;

public class ZooKeeperLogFiles
{
    private final List<File>        paths;
    private final boolean           isValid;

    public static File      getDataDir(Exhibitor exhibitor)
    {
        String      path = exhibitor.getConfigManager().getConfig().getString(StringConfigs.ZOOKEEPER_DATA_DIRECTORY);
        return new File(path, "version-2");
    }

    public ZooKeeperLogFiles(Exhibitor exhibitor) throws Exception
    {
        ImmutableList.Builder<File> builder = ImmutableList.builder();

        File        path = getDataDir(exhibitor);
        isValid = path.isDirectory();
        if ( isValid )
        {
            File[]      logs = path.listFiles();
            if ( logs != null )
            {
                for ( File f : logs )
                {
                    if ( isLogFile(f) )
                    {
                        builder.add(f);
                    }
                }
            }
        }

        paths = builder.build();
    }

    public boolean isValid()
    {
        return isValid;
    }

    public List<File> getPaths()
    {
        return paths;
    }

    private boolean isLogFile(File f) throws Exception
    {
        InputStream         log = new BufferedInputStream(new FileInputStream(f));
        try
        {
            ZooKeeperLogParser  logParser = new ZooKeeperLogParser(log);
            return logParser.isValid();
        }
        finally
        {
            Closeables.closeQuietly(log);
        }
    }
}
