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

import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.activity.Activity;
import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.config.StringConfigs;
import java.io.File;

public class IndexProcessorActivity implements Activity
{
    private Exhibitor exhibitor;

    public IndexProcessorActivity(Exhibitor exhibitor)
    {
        this.exhibitor = exhibitor;
    }

    @Override
    public void completed(boolean wasSuccessful)
    {
    }

    @Override
    public Boolean call() throws Exception
    {
        try
        {
            File            indexDirectory = new File(exhibitor.getConfigManager().getConfig().getString(StringConfigs.LOG_INDEX_DIRECTORY), "exhibitor-" + System.currentTimeMillis());
            IndexProcessor  processor = new IndexProcessor(exhibitor);
            processor.process(indexDirectory);
        }
        catch ( Exception e )
        {
            exhibitor.getLog().add(ActivityLog.Type.ERROR, "Building Index", e);
        }
        return null;
    }
}
