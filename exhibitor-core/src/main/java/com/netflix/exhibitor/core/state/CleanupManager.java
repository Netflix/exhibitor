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

package com.netflix.exhibitor.core.state;

import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.activity.Activity;
import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.activity.OnOffRepeatingActivity;
import com.netflix.exhibitor.core.activity.QueueGroups;
import com.netflix.exhibitor.core.activity.RepeatingActivity;
import com.netflix.exhibitor.core.activity.RepeatingActivityImpl;
import com.netflix.exhibitor.core.config.ConfigListener;
import com.netflix.exhibitor.core.config.IntConfigs;
import com.netflix.exhibitor.core.controlpanel.ControlPanelTypes;
import java.io.Closeable;
import java.io.IOException;

public class CleanupManager implements Closeable
{
    private final RepeatingActivity repeatingActivity;
    private final Exhibitor exhibitor;

    public CleanupManager(final Exhibitor exhibitor)
    {
        this.exhibitor = exhibitor;
        final Activity activity = new Activity()
        {
            @Override
            public void completed(boolean wasSuccessful)
            {
                // NOP
            }

            @Override
            public Boolean call() throws Exception
            {
                if ( exhibitor.getControlPanelValues().isSet(ControlPanelTypes.CLEANUP) )
                {
                    try
                    {
                        exhibitor.getProcessOperations().cleanupInstance();
                    }
                    catch ( Exception e )
                    {
                        exhibitor.getLog().add(ActivityLog.Type.ERROR, "Doing cleanup", e);
                    }
                }
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
                    return new RepeatingActivityImpl(exhibitor.getLog(), exhibitor.getActivityQueue(), QueueGroups.IO, activity, exhibitor.getConfigManager().getConfig().getInt(IntConfigs.CLEANUP_PERIOD_MS));
                }
            },
            exhibitor.getConfigManager().getConfig().getInt(IntConfigs.CLEANUP_PERIOD_MS)
        );
    }

    public void start()
    {
        repeatingActivity.start();
        exhibitor.getConfigManager().addConfigListener
        (
            new ConfigListener()
            {
                @Override
                public void configUpdated()
                {
                    repeatingActivity.setTimePeriodMs(exhibitor.getConfigManager().getConfig().getInt(IntConfigs.CLEANUP_PERIOD_MS));
                }
            }
        );
    }

    @Override
    public void close() throws IOException
    {
        repeatingActivity.close();
    }
}
