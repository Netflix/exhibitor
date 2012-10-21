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

import com.netflix.servo.MonitorRegistry;

public class ServoRegistration
{
    private final MonitorRegistry       monitorRegistry;
    private final int                   zookeeperPollMs;

    /**
     * @param monitorRegistry which monitor registry to use
     * @param zookeeperPollMs how often to poll zookeeper for state (in milliseconds)
     */
    public ServoRegistration(MonitorRegistry monitorRegistry, int zookeeperPollMs)
    {
        this.monitorRegistry = monitorRegistry;
        this.zookeeperPollMs = zookeeperPollMs;
    }

    public MonitorRegistry getMonitorRegistry()
    {
        return monitorRegistry;
    }

    public int getZookeeperPollMs()
    {
        return zookeeperPollMs;
    }
}
