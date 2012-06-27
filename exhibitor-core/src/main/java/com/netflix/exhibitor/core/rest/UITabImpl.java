/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.exhibitor.core.rest;

import javax.ws.rs.core.UriInfo;

/**
 * Used to add additional tabs to the Exhibitor UI
 */
@SuppressWarnings("UnusedDeclaration")
public class UITabImpl implements UITab
{
    private final String        name;
    private final String        content;

    public UITabImpl()
    {
        name = "";
        content = "";
    }

    public UITabImpl(String name)
    {
        this.name = name;
        content = "";
    }

    public UITabImpl(String name, String content)
    {
        this.name = name;
        this.content = content;
    }

    /**
     * Return the tab name
     *
     * @return name
     */
    @Override
    public String       getName()
    {
        return name;
    }

    /**
     * Return the content (as text/plain) for the tab
     * @return content
     * @throws Exception errors
     * @param info request info
     */
    @Override
    public String       getContent(UriInfo info) throws Exception
    {
        return content;
    }

    @Override
    public boolean contentIsHtml()
    {
        return false;
    }
}
