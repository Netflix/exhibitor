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

import java.io.Closeable;

public interface ConfigProvider extends Closeable
{
    /**
     * Called to start the provider
     *
     * @throws Exception errors
     */
    public void start() throws Exception;

    /**
     * Load and return the config
     *
     * @return config
     * @throws Exception errors
     */
    public LoadedInstanceConfig loadConfig() throws Exception;

    /**
     * Store the config
     *
     * @param config config
     * @param compareVersion modified value to compare with. If the config storage doesn't
     *                            match, the new config is not stored and null is returned
     * @throws Exception errors
     * @return return updated loaded values or null
     */
    public LoadedInstanceConfig storeConfig(ConfigCollection config, long compareVersion) throws Exception;

    /**
     * Allocate a new pseudo-lock for the given prefix
     *
     * @return new lock
     * @throws Exception errors
     */
    public PseudoLock   newPseudoLock() throws Exception;
}
