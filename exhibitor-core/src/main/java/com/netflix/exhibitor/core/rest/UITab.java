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

import javax.ws.rs.core.UriInfo;

public interface UITab
{
    /**
     * Return the tab name
     *
     * @return name
     */
    public String       getName();

    /**
     * Return the content (as text/plain) for the tab
     *
     * @param info uri context
     * @return content
     * @throws Exception errors
     */
    public String       getContent(UriInfo info) throws Exception;

    /**
     * Return true if the content of this tab is HTML that should be integrated
     * into the page (do NOT include &lt;html&gt; tags, etc.). Otherwise it will
     * be treated as plain text.
     *
     * @return true/false
     */
    public boolean      contentIsHtml();
}
