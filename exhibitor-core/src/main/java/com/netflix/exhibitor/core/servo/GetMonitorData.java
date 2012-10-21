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

package com.netflix.exhibitor.core.servo;

import com.google.common.collect.Maps;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.activity.Activity;
import com.netflix.exhibitor.core.state.FourLetterWord;
import com.netflix.servo.monitor.BasicCounter;
import java.util.List;
import java.util.Map;

public class GetMonitorData implements Activity
{
    private final Exhibitor exhibitor;
    private final ZookeeperMonitoredData monitor;

    public GetMonitorData(Exhibitor exhibitor, ZookeeperMonitoredData monitor)
    {
        this.exhibitor = exhibitor;
        this.monitor = monitor;
    }

    @Override
    public void completed(boolean wasSuccessful)
    {
        // NOP
    }

    @Override
    public Boolean call() throws Exception
    {
        List<String>            lines = new FourLetterWord(FourLetterWord.Word.MNTR, exhibitor.getThisJVMHostname(), exhibitor.getConfigManager().getConfig(), exhibitor.getConnectionTimeOutMs()).getResponseLines();
        doUpdate(lines);

        return true;
    }

    public void doUpdate(List<String> lines)
    {
        Map<String, Integer> values = Maps.newHashMap();
        for ( String line : lines )
        {
            String[]        parts = line.split("\\s");
            if ( parts.length == 2 )
            {
                try
                {
                    String  name = parts[0];
                    int     value = Integer.parseInt(parts[1]);
                    values.put(name, value);
                }
                catch ( NumberFormatException ignore )
                {
                    // ignore
                }
            }
        }

        monitor.updateValues(values);
    }
}
