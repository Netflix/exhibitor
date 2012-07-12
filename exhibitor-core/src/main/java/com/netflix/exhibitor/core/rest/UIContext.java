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

package com.netflix.exhibitor.core.rest;

import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.rest.jersey.JerseySupport;
import com.sun.jersey.api.core.ResourceConfig;

public class UIContext
{
    private final Exhibitor exhibitor;

    /**
     * @param exhibitor the Exhibitor singleton
     */
    public UIContext(Exhibitor exhibitor)
    {
        this.exhibitor = exhibitor;
    }

    public Exhibitor getExhibitor()
    {
        return exhibitor;
    }
}
