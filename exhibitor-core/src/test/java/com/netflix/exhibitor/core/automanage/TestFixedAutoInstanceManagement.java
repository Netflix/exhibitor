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

public class TestFixedAutoInstanceManagement
{
    @Test
    public void testSimpleAddition() throws Exception
    {
        MockExhibitorInstance       mockExhibitorInstance = new MockExhibitorInstance("new");
        mockExhibitorInstance.getMockConfigProvider().setConfig(StringConfigs.SERVERS_SPEC, "1:a,2:b");
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES, 1);
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES_FIXED_ENSEMBLE_SIZE, 3);
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES_SETTLING_PERIOD_MS, 0);

        List<ServerStatus> statuses = Lists.newArrayList();
        statuses.add(new ServerStatus("a", InstanceStateTypes.SERVING.getCode(), "", true));
        statuses.add(new ServerStatus("b", InstanceStateTypes.SERVING.getCode(), "", false));
        Mockito.when(mockExhibitorInstance.getMockForkJoinPool().invoke(Mockito.isA(ClusterStatusTask.class))).thenReturn(statuses);

        final AtomicBoolean configWasChanged = new AtomicBoolean(false);
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

        Assert.assertTrue(configWasChanged.get());
        Assert.assertEquals(mockExhibitorInstance.getMockExhibitor().getConfigManager().getConfig().getString(StringConfigs.SERVERS_SPEC), "1:a,2:b,3:new");
    }

    @Test
    public void testReplacement() throws Exception
    {
        MockExhibitorInstance       mockExhibitorInstance = new MockExhibitorInstance("new");
        mockExhibitorInstance.getMockConfigProvider().setConfig(StringConfigs.SERVERS_SPEC, "1:a,2:b,3:c");
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES, 1);
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES_FIXED_ENSEMBLE_SIZE, 3);
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES_SETTLING_PERIOD_MS, 0);

        List<ServerStatus> statuses = Lists.newArrayList();
        statuses.add(new ServerStatus("a", InstanceStateTypes.SERVING.getCode(), "", true));
        statuses.add(new ServerStatus("b", InstanceStateTypes.DOWN.getCode(), "", false));
        statuses.add(new ServerStatus("c", InstanceStateTypes.SERVING.getCode(), "", false));
        Mockito.when(mockExhibitorInstance.getMockForkJoinPool().invoke(Mockito.isA(ClusterStatusTask.class))).thenReturn(statuses);

        final AtomicBoolean configWasChanged = new AtomicBoolean(false);
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

        Assert.assertTrue(configWasChanged.get());
        Assert.assertEquals(mockExhibitorInstance.getMockExhibitor().getConfigManager().getConfig().getString(StringConfigs.SERVERS_SPEC), "1:a,3:c,4:new");
    }

    @Test
    public void testNoRoom() throws Exception
    {
        MockExhibitorInstance       mockExhibitorInstance = new MockExhibitorInstance("new");
        mockExhibitorInstance.getMockConfigProvider().setConfig(StringConfigs.SERVERS_SPEC, "1:a,2:b,3:c");
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES, 1);
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES_FIXED_ENSEMBLE_SIZE, 3);
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES_SETTLING_PERIOD_MS, 0);

        List<ServerStatus> statuses = Lists.newArrayList();
        statuses.add(new ServerStatus("a", InstanceStateTypes.SERVING.getCode(), "", true));
        statuses.add(new ServerStatus("b", InstanceStateTypes.SERVING.getCode(), "", false));
        statuses.add(new ServerStatus("c", InstanceStateTypes.SERVING.getCode(), "", false));
        Mockito.when(mockExhibitorInstance.getMockForkJoinPool().invoke(Mockito.isA(ClusterStatusTask.class))).thenReturn(statuses);

        final AtomicBoolean configWasChanged = new AtomicBoolean(false);
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
        Assert.assertEquals(mockExhibitorInstance.getMockExhibitor().getConfigManager().getConfig().getString(StringConfigs.SERVERS_SPEC), "1:a,2:b,3:c");
    }

    @Test
    public void     testBecomesObserver() throws Exception
    {
        MockExhibitorInstance       mockExhibitorInstance = new MockExhibitorInstance("new");
        mockExhibitorInstance.getMockConfigProvider().setConfig(StringConfigs.SERVERS_SPEC, "1:a,2:b,3:c");
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES, 1);
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES_SETTLING_PERIOD_MS, 0);
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.AUTO_MANAGE_INSTANCES_FIXED_ENSEMBLE_SIZE, 4);
        mockExhibitorInstance.getMockConfigProvider().setConfig(IntConfigs.OBSERVER_THRESHOLD, 3);

        List<ServerStatus>          statuses = Lists.newArrayList();
        statuses.add(new ServerStatus("a", InstanceStateTypes.SERVING.getCode(), "", true));
        statuses.add(new ServerStatus("b", InstanceStateTypes.SERVING.getCode(), "", false));
        statuses.add(new ServerStatus("c", InstanceStateTypes.SERVING.getCode(), "", false));
        Mockito.when(mockExhibitorInstance.getMockForkJoinPool().invoke(Mockito.isA(ClusterStatusTask.class))).thenReturn(statuses);

        AutomaticInstanceManagement management = new AutomaticInstanceManagement(mockExhibitorInstance.getMockExhibitor());
        management.call();

        Assert.assertEquals(mockExhibitorInstance.getMockExhibitor().getConfigManager().getConfig().getString(StringConfigs.SERVERS_SPEC), "1:a,2:b,3:c,O:4:new");
    }
}
