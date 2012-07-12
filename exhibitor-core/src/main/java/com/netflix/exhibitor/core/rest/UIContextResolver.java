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
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

@Provider
/**
 * You must inject an instance of this as a Singleton in your REST app.
 */
public class UIContextResolver implements ContextResolver<UIContext>
{
    private final UIContext context;

    /**
     * @param context the UIContext singleton
     */
    public UIContextResolver(UIContext context)
    {
        this.context = context;
    }

    @Override
    public UIContext getContext(Class<?> type)
    {
        return context;
    }
}
