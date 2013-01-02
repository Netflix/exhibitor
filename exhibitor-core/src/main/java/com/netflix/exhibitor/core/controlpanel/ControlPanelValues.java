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

package com.netflix.exhibitor.core.controlpanel;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.prefs.Preferences;

public class ControlPanelValues
{
    private final LoadingCache<ControlPanelTypes, Boolean> cache;
    private final Preferences preferences;

    private static final String         BASE_KEY = "com.netflix.exhibitor.control-panel.";

    public ControlPanelValues(final Preferences preferences)
    {
        this.preferences = preferences;
        cache = CacheBuilder.newBuilder().build
        (
            new CacheLoader<ControlPanelTypes, Boolean>()
            {
                @Override
                public Boolean load(ControlPanelTypes type) throws Exception
                {
                    return preferences.getBoolean(makeKey(type), true);
                }
            }
        );
    }

    public boolean      isSet(ControlPanelTypes type) throws Exception
    {
        return cache.get(type);
    }

    public void         set(ControlPanelTypes type, boolean newValue) throws Exception
    {
        cache.put(type, newValue);
        preferences.putBoolean(makeKey(type), newValue);
        preferences.flush();
    }

    private String makeKey(ControlPanelTypes type)
    {
        return BASE_KEY + type.name().toLowerCase();
    }
}
