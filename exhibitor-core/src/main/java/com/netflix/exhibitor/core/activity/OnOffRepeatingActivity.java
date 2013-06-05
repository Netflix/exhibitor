/*
 * Copyright 2013 Netflix, Inc.
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

package com.netflix.exhibitor.core.activity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class OnOffRepeatingActivity implements RepeatingActivity
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final AtomicReference<RepeatingActivity> actualRepeatingActivity = new AtomicReference<RepeatingActivity>(null);
    private final Factory factory;
    private final long initialTimePeriodMs;

    public interface Factory
    {
        public RepeatingActivity newRepeatingActivity(long timePeriodMs);
    }

    public OnOffRepeatingActivity(Factory factory, long initialTimePeriodMs)
    {
        this.factory = factory;
        this.initialTimePeriodMs = initialTimePeriodMs;
    }

    @Override
    public void start()
    {
        if ( initialTimePeriodMs > 0 )
        {
            actualRepeatingActivity.set(factory.newRepeatingActivity(initialTimePeriodMs));
            actualRepeatingActivity.get().start();
        }
    }

    @Override
    public void setTimePeriodMs(long newTimePeriodMs)
    {
        if ( actualRepeatingActivity.get() != null )
        {
            if ( newTimePeriodMs == 0 )
            {
                try
                {
                    close();
                }
                catch ( IOException e )
                {
                    log.error("Closing activity", e);
                }
            }
            else
            {
                actualRepeatingActivity.get().setTimePeriodMs(newTimePeriodMs);
            }
        }
        else if ( newTimePeriodMs > 0 )
        {
            actualRepeatingActivity.set(factory.newRepeatingActivity(newTimePeriodMs));
            actualRepeatingActivity.get().start();
        }
    }

    @Override
    public void close() throws IOException
    {
        RepeatingActivity activity = actualRepeatingActivity.getAndSet(null);
        if ( activity != null )
        {
            activity.close();
        }
    }
}
