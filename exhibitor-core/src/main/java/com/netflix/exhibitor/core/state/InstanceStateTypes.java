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

public enum InstanceStateTypes
{
    LATENT(0),
    DOWN(1),
    NOT_SERVING(2),
    SERVING(3),
    UNLISTED_DOWN(4),
    NO_RESTARTS_DOWN(5)
    ;

    private final int code;

    public int getCode()
    {
        return code;
    }

    public String   getDescription()
    {
        return name().toLowerCase().replace("_", " ");
    }

    private InstanceStateTypes(int code)
    {
        this.code = code;
    }
}
