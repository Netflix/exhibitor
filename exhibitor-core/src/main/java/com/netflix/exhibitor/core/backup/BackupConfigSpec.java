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

package com.netflix.exhibitor.core.backup;

public class BackupConfigSpec
{
    private final String        key;
    private final String        displayName;
    private final String        helpText;
    private final String        defaultValue;
    private final Type          type;

    /**
     * The config type - used by the UI to determine which editing widget to use
     */
    public enum Type
    {
        STRING,
        INTEGER
    }

    /**
     * @param key unique key to use when storing the value
     * @param displayName the end-user name of the config
     * @param helpText long description of the config
     * @param defaultValue default value for the config
     * @param type type
     */
    public BackupConfigSpec(String key, String displayName, String helpText, String defaultValue, Type type)
    {
        this.key = key;
        this.displayName = displayName;
        this.helpText = helpText;
        this.defaultValue = defaultValue;
        this.type = type;
    }

    public String getKey()
    {
        return key;
    }

    public String getDisplayName()
    {
        return displayName;
    }

    public String getHelpText()
    {
        return helpText;
    }

    public String getDefaultValue()
    {
        return defaultValue;
    }

    public Type getType()
    {
        return type;
    }
}
