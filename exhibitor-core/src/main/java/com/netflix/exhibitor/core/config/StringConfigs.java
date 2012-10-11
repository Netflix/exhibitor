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

package com.netflix.exhibitor.core.config;

public enum StringConfigs
{
    /**
     * Path to stored indexed log files
     */
    LOG_INDEX_DIRECTORY()
    {
        @Override
        public boolean isRestartSignificant()
        {
            return false;
        }
    },

    /**
     * The base director of the zookeeper installation
     */
    ZOOKEEPER_INSTALL_DIRECTORY
    {
        @Override
        public boolean isRestartSignificant()
        {
            return true;
        }
    },

    /**
     * Where to store ZooKeeper snapshots
     */
    ZOOKEEPER_DATA_DIRECTORY
    {
        @Override
        public boolean isRestartSignificant()
        {
            return true;
        }
    },

    /**
     * Where to store ZooKeeper transaction logs
     */
    ZOOKEEPER_LOG_DIRECTORY
    {
        @Override
        public boolean isRestartSignificant()
        {
            return true;
        }
    },

    /**
     * List of servers in the cluster of the form [id]:[hostname],[id]:[hostname],...
     */
    SERVERS_SPEC
    {
        @Override
        public boolean isRestartSignificant()
        {
            return false;   // handled by the servers list
        }
    },

    /**
     * Used to store the provider-specific backup config
     */
    BACKUP_EXTRA
    {
        @Override
        public boolean isRestartSignificant()
        {
            return false;
        }
    },

    /**
     * Additional properties to add to zoo.cfg
     */
    ZOO_CFG_EXTRA
    {
        @Override
        public boolean isRestartSignificant()
        {
            return true;
        }
    },

    /**
     * Contents for the java.env file
     */
    JAVA_ENVIRONMENT
    {
        @Override
        public boolean isRestartSignificant()
        {
            return true;
        }
    },

    LOG4J_PROPERTIES
    {
        @Override
        public boolean isRestartSignificant()
        {
            return true;
        }
    }
    ;

    /**
     * Return true if a change to this config requires that the ZK instance be restarted
     *
     * @return true/false
     */
    public abstract boolean     isRestartSignificant();
}
