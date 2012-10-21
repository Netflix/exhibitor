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

import com.google.common.collect.ImmutableMap;
import com.netflix.servo.annotations.Monitor;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.netflix.servo.annotations.DataSourceType.*;

public class ZookeeperMonitoredData
{
    private final Map<String, AtomicInteger>        fieldMap;

    /*
        See http://zookeeper.apache.org/doc/r3.4.4/zookeeperAdmin.html#sc_zkCommands
    */
    @Monitor(name="zk_avg_latency", type=GAUGE)
    public final AtomicInteger     zk_avg_latency = new AtomicInteger(0);

    @Monitor(name="zk_max_latency", type=GAUGE)
    public final AtomicInteger     zk_max_latency = new AtomicInteger(0);

    @Monitor(name="zk_min_latency", type=GAUGE)
    public final AtomicInteger     zk_min_latency = new AtomicInteger(0);

    @Monitor(name="zk_packets_received", type=COUNTER)
    public final AtomicInteger     zk_packets_received = new AtomicInteger(0);

    @Monitor(name="zk_packets_sent", type=COUNTER)
    public final AtomicInteger     zk_packets_sent = new AtomicInteger(0);

    @Monitor(name="zk_outstanding_requests", type=COUNTER)
    public final AtomicInteger     zk_outstanding_requests = new AtomicInteger(0);

    @Monitor(name="zk_znode_count", type=COUNTER)
    public final AtomicInteger     zk_znode_count = new AtomicInteger(0);

    @Monitor(name="zk_watch_count", type=COUNTER)
    public final AtomicInteger     zk_watch_count = new AtomicInteger(0);

    @Monitor(name="zk_ephemerals_count", type=COUNTER)
    public final AtomicInteger     zk_ephemerals_count = new AtomicInteger(0);

    @Monitor(name="zk_approximate_data_size", type=COUNTER)
    public final AtomicInteger     zk_approximate_data_size = new AtomicInteger(0);

    @Monitor(name="zk_followers", type=COUNTER)
    public final AtomicInteger     zk_followers = new AtomicInteger(0);

    @Monitor(name="zk_synced_followers", type=COUNTER)
    public final AtomicInteger     zk_synced_followers = new AtomicInteger(0);

    @Monitor(name="zk_pending_syncs", type=COUNTER)
    public final AtomicInteger     zk_pending_syncs = new AtomicInteger(0);

    @Monitor(name="zk_open_file_descriptor_count", type=COUNTER)
    public final AtomicInteger     zk_open_file_descriptor_count = new AtomicInteger(0);

    @Monitor(name="zk_max_file_descriptor_count", type=COUNTER)
    public final AtomicInteger     zk_max_file_descriptor_count = new AtomicInteger(0);

    public ZookeeperMonitoredData()
    {
        ImmutableMap.Builder<String, AtomicInteger>         builder = ImmutableMap.builder();
        try
        {
            for ( Field f : getClass().getDeclaredFields() )
            {
                if ( f.getName().startsWith("zk_") )
                {
                    builder.put(f.getName(), (AtomicInteger)f.get(this));
                }
            }
        }
        catch ( IllegalAccessException e )
        {
            throw new RuntimeException(e);  // should never get here
        }

        fieldMap = builder.build();
    }

    public void         updateValues(Map<String, Integer> newValues)
    {
        for ( Map.Entry<String, Integer> entry : newValues.entrySet() )
        {
            AtomicInteger       value = fieldMap.get(entry.getKey());
            if ( value != null )
            {
                value.set(entry.getValue());
            }
        }
    }
}
