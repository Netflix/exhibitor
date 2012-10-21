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

import com.netflix.exhibitor.core.Exhibitor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Arrays;
import java.util.List;

public class TestGetMonitorData
{
    @Test
    public void     testSimple()
    {
        final String[]      lines =
        {
            "zk_version	3.4.4-1386507, built on 09/17/2012 08:33 GMT",
            "zk_avg_latency	0",
            "zk_max_latency	0",
            "zk_min_latency	0",
            "zk_packets_received	1",
//            "zk_packets_sent	0",     purposely removed
            "zk_num_alive_connections	1",
            "zk_outstanding_requests	0",
            "zk_server_state	standalone",
            "zk_znode_count	5",
            "zk_watch_count	0",
            "zk_ephemerals_count	0",
            "zk_approximate_data_size	34",
            "zk_open_file_descriptor_count	46",
            "zk_max_file_descriptor_count	10240"
        };

        ZookeeperMonitoredData zookeeperMonitoredData = new ZookeeperMonitoredData();
        Assert.assertEquals(zookeeperMonitoredData.zk_ephemerals_count.get(), 0);
        Assert.assertEquals(zookeeperMonitoredData.zk_approximate_data_size.get(), 0);
        zookeeperMonitoredData.zk_packets_sent.set(10101);

        List<String>        linesList = Arrays.asList(lines);
        GetMonitorData      getMonitorData = new GetMonitorData(Mockito.mock(Exhibitor.class), zookeeperMonitoredData);
        getMonitorData.doUpdate(linesList);

        Assert.assertEquals(zookeeperMonitoredData.zk_ephemerals_count.get(), 0);
        Assert.assertEquals(zookeeperMonitoredData.zk_packets_received.get(), 1);
        Assert.assertEquals(zookeeperMonitoredData.zk_approximate_data_size.get(), 34);
        Assert.assertEquals(zookeeperMonitoredData.zk_max_file_descriptor_count.get(), 10240);
        Assert.assertEquals(zookeeperMonitoredData.zk_packets_sent.get(), 10101); // assert that it hasn't changed
    }
/*

    @Test
    public void testAnnotations()
    {
        AnnotationUtils.validate(new ZookeeperMonitoredData());
    }
*/
}
