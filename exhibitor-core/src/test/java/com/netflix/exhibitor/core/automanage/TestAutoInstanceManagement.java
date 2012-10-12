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

package com.netflix.exhibitor.core.automanage;

import com.google.common.collect.Lists;
import com.netflix.exhibitor.core.config.IntConfigs;
import com.netflix.exhibitor.core.config.StringConfigs;
import com.netflix.exhibitor.core.entities.ServerStatus;
import com.netflix.exhibitor.core.state.InstanceStateTypes;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestAutoInstanceManagement
{
    @Test
    public void     testUnstable() throws Exception
    {
        MockExhibitorInstance       mockExhibitorInstance = new MockExhibitorInstance("new");
        mockExhibitorInstance.getMockConfigProvider().setConfig(StringConfigs.SERVERS_SPEC, "1:a,2:b,3:c");
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES, 1);
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES_SETTLING_PERIOD_MS, 100000);

        List<ServerStatus>          statuses = Lists.newArrayList();
        statuses.add(new ServerStatus("a", InstanceStateTypes.SERVING.getCode(), "", true));
        statuses.add(new ServerStatus("b", InstanceStateTypes.SERVING.getCode(), "", false));
        statuses.add(new ServerStatus("c", InstanceStateTypes.SERVING.getCode(), "", false));
        Mockito.when(mockExhibitorInstance.getMockForkJoinPool().invoke(Mockito.isA(ClusterStatusTask.class))).thenReturn(statuses);

        final AtomicBoolean         configWasChanged = new AtomicBoolean(false);
        AutomaticInstanceManagement management = new AutomaticInstanceManagement(mockExhibitorInstance.getMockExhibitor())
        {
            @Override
            void adjustConfig(String newSpec, String leaderHostname) throws Exception
            {
                super.adjustConfig(newSpec, leaderHostname);
                configWasChanged.set(true);
            }
        };
        management.call();

        Assert.assertFalse(configWasChanged.get()); // hasn't settled yet

        statuses = Lists.newArrayList();
        statuses.add(new ServerStatus("a", InstanceStateTypes.DOWN.getCode(), "", false));
        statuses.add(new ServerStatus("b", InstanceStateTypes.SERVING.getCode(), "", false));
        statuses.add(new ServerStatus("c", InstanceStateTypes.SERVING.getCode(), "", false));
        Mockito.when(mockExhibitorInstance.getMockForkJoinPool().invoke(Mockito.isA(ClusterStatusTask.class))).thenReturn(statuses);
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES_SETTLING_PERIOD_MS, 0);

        management.call();

        Assert.assertFalse(configWasChanged.get());

        statuses = Lists.newArrayList();
        statuses.add(new ServerStatus("a", InstanceStateTypes.SERVING.getCode(), "", true));
        statuses.add(new ServerStatus("b", InstanceStateTypes.SERVING.getCode(), "", false));
        statuses.add(new ServerStatus("c", InstanceStateTypes.SERVING.getCode(), "", false));
        Mockito.when(mockExhibitorInstance.getMockForkJoinPool().invoke(Mockito.isA(ClusterStatusTask.class))).thenReturn(statuses);
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES_SETTLING_PERIOD_MS, 2000);

        management.call();

        Assert.assertFalse(configWasChanged.get());

        Thread.sleep(3000);

        management.call();

        Assert.assertTrue(configWasChanged.get());
    }

    @Test
    public void     testNoServers() throws Exception
    {
        MockExhibitorInstance       mockExhibitorInstance = new MockExhibitorInstance("a");
        mockExhibitorInstance.getMockConfigProvider().setConfig(StringConfigs.SERVERS_SPEC, "1:a,2:b,3:c");
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES, 1);
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES_SETTLING_PERIOD_MS, 0);

        List<ServerStatus>          statuses = Lists.newArrayList();
        statuses.add(new ServerStatus("a", InstanceStateTypes.DOWN.getCode(), "", true));
        statuses.add(new ServerStatus("b", InstanceStateTypes.DOWN.getCode(), "", false));
        statuses.add(new ServerStatus("c", InstanceStateTypes.DOWN.getCode(), "", false));
        Mockito.when(mockExhibitorInstance.getMockForkJoinPool().invoke(Mockito.isA(ClusterStatusTask.class))).thenReturn(statuses);

        final AtomicBoolean         configWasChanged = new AtomicBoolean(false);
        AutomaticInstanceManagement management = new AutomaticInstanceManagement(mockExhibitorInstance.getMockExhibitor())
        {
            @Override
            void adjustConfig(String newSpec, String leaderHostname) throws Exception
            {
                super.adjustConfig(newSpec, leaderHostname);
                configWasChanged.set(true);
            }
        };
        management.call();

        Assert.assertFalse(configWasChanged.get());
    }

    @Test
    public void     testNoChange() throws Exception
    {
        MockExhibitorInstance       mockExhibitorInstance = new MockExhibitorInstance("a");
        mockExhibitorInstance.getMockConfigProvider().setConfig(StringConfigs.SERVERS_SPEC, "1:a,2:b,3:c");
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES, 1);
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES_SETTLING_PERIOD_MS, 0);

        List<ServerStatus>          statuses = Lists.newArrayList();
        statuses.add(new ServerStatus("a", InstanceStateTypes.SERVING.getCode(), "", true));
        statuses.add(new ServerStatus("b", InstanceStateTypes.SERVING.getCode(), "", false));
        statuses.add(new ServerStatus("c", InstanceStateTypes.SERVING.getCode(), "", false));
        Mockito.when(mockExhibitorInstance.getMockForkJoinPool().invoke(Mockito.isA(ClusterStatusTask.class))).thenReturn(statuses);

        final AtomicBoolean         configWasChanged = new AtomicBoolean(false);
        AutomaticInstanceManagement management = new AutomaticInstanceManagement(mockExhibitorInstance.getMockExhibitor())
        {
            @Override
            void adjustConfig(String newSpec, String leaderHostname) throws Exception
            {
                super.adjustConfig(newSpec, leaderHostname);
                configWasChanged.set(true);
            }
        };
        management.call();

        Assert.assertFalse(configWasChanged.get());
    }

    @Test
    public void     testRemoval() throws Exception
    {
        MockExhibitorInstance       mockExhibitorInstance = new MockExhibitorInstance("a");
        mockExhibitorInstance.getMockConfigProvider().setConfig(StringConfigs.SERVERS_SPEC, "1:a,2:b,3:c");
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES, 1);
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES_SETTLING_PERIOD_MS, 0);

        List<ServerStatus>          statuses = Lists.newArrayList();
        statuses.add(new ServerStatus("a", InstanceStateTypes.SERVING.getCode(), "", true));
        statuses.add(new ServerStatus("b", InstanceStateTypes.DOWN.getCode(), "", false));
        statuses.add(new ServerStatus("c", InstanceStateTypes.SERVING.getCode(), "", false));
        Mockito.when(mockExhibitorInstance.getMockForkJoinPool().invoke(Mockito.isA(ClusterStatusTask.class))).thenReturn(statuses);

        AutomaticInstanceManagement management = new AutomaticInstanceManagement(mockExhibitorInstance.getMockExhibitor());
        management.call();

        Assert.assertEquals(mockExhibitorInstance.getMockExhibitor().getConfigManager().getConfig().getString(StringConfigs.SERVERS_SPEC), "1:a,3:c");
    }

    @Test
    public void     testInitialAddition() throws Exception
    {
        MockExhibitorInstance       mockExhibitorInstance = new MockExhibitorInstance("new");
        mockExhibitorInstance.getMockConfigProvider().setConfig(StringConfigs.SERVERS_SPEC, "");
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES, 1);
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES_SETTLING_PERIOD_MS, 0);

        List<ServerStatus>          statuses = Lists.newArrayList();
        Mockito.when(mockExhibitorInstance.getMockForkJoinPool().invoke(Mockito.isA(ClusterStatusTask.class))).thenReturn(statuses);

        AutomaticInstanceManagement management = new AutomaticInstanceManagement(mockExhibitorInstance.getMockExhibitor());
        management.call();

        Assert.assertEquals(mockExhibitorInstance.getMockExhibitor().getConfigManager().getConfig().getString(StringConfigs.SERVERS_SPEC), "1:new");
    }

    @Test
    public void     testAddition() throws Exception
    {
        MockExhibitorInstance       mockExhibitorInstance = new MockExhibitorInstance("new");
        mockExhibitorInstance.getMockConfigProvider().setConfig(StringConfigs.SERVERS_SPEC, "1:a,2:b,3:c");
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES, 1);
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES_SETTLING_PERIOD_MS, 0);

        List<ServerStatus>          statuses = Lists.newArrayList();
        statuses.add(new ServerStatus("a", InstanceStateTypes.SERVING.getCode(), "", true));
        statuses.add(new ServerStatus("b", InstanceStateTypes.SERVING.getCode(), "", false));
        statuses.add(new ServerStatus("c", InstanceStateTypes.SERVING.getCode(), "", false));
        Mockito.when(mockExhibitorInstance.getMockForkJoinPool().invoke(Mockito.isA(ClusterStatusTask.class))).thenReturn(statuses);

        AutomaticInstanceManagement management = new AutomaticInstanceManagement(mockExhibitorInstance.getMockExhibitor());
        management.call();

        Assert.assertEquals(mockExhibitorInstance.getMockExhibitor().getConfigManager().getConfig().getString(StringConfigs.SERVERS_SPEC), "1:a,2:b,3:c,4:new");
    }

    @Test
    public void     testAdditionWithRemoval() throws Exception
    {
        MockExhibitorInstance       mockExhibitorInstance = new MockExhibitorInstance("new");
        mockExhibitorInstance.getMockConfigProvider().setConfig(StringConfigs.SERVERS_SPEC, "1:a,2:b,3:c");
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES, 1);
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES_SETTLING_PERIOD_MS, 0);

        List<ServerStatus>          statuses = Lists.newArrayList();
        statuses.add(new ServerStatus("a", InstanceStateTypes.SERVING.getCode(), "", true));
        statuses.add(new ServerStatus("b", InstanceStateTypes.SERVING.getCode(), "", false));
        statuses.add(new ServerStatus("c", InstanceStateTypes.DOWN.getCode(), "", false));
        Mockito.when(mockExhibitorInstance.getMockForkJoinPool().invoke(Mockito.isA(ClusterStatusTask.class))).thenReturn(statuses);

        AutomaticInstanceManagement management = new AutomaticInstanceManagement(mockExhibitorInstance.getMockExhibitor());
        management.call();

        Assert.assertEquals(mockExhibitorInstance.getMockExhibitor().getConfigManager().getConfig().getString(StringConfigs.SERVERS_SPEC), "1:a,2:b,4:new");
    }
}
