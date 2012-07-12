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

package com.netflix.exhibitor.core.state;

public enum ServerType
{
    STANDARD("S", "", ""),
    OBSERVER("O", "O:", ":observer")
    ;

    private final String code;
    private final String codePrefix;
    private final String zookeeperConfigValue;

    public String getCode()
    {
        return code;
    }

    public String getCodePrefix()
    {
        return codePrefix;
    }

    public String getZookeeperConfigValue()
    {
        return zookeeperConfigValue;
    }

    public static ServerType        fromCode(String code)
    {
        for ( ServerType type : values() )
        {
            if ( code.equalsIgnoreCase(type.getCode()) )
            {
                return type;
            }
        }
        return STANDARD;
    }

    private ServerType(String code, String codePrefix, String zookeeperConfigValue)
    {
        this.code = code;
        this.codePrefix = codePrefix;
        this.zookeeperConfigValue = zookeeperConfigValue;
    }
}
