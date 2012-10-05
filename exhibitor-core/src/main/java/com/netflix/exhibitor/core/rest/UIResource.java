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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.netflix.exhibitor.core.backup.BackupConfigSpec;
import com.netflix.exhibitor.core.config.EncodedConfigParser;
import com.netflix.exhibitor.core.entities.UITabSpec;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import javax.activation.MimetypesFileTypeMap;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.ContextResolver;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Path("exhibitor/v1/ui")
public class UIResource
{
    private final UIContext context;
    private final List<UITab> tabs;

    private static final String     jQueryUiPrefix = "css/jquery/";

    private static final String     TEXT_UI_TAB_BASE_URL = "tab/";
    private static final String     HTML_UI_TAB_BASE_URL = "tab-html/";

    public UIResource(@Context ContextResolver<UIContext> resolver)
    {
        context = resolver.getContext(UIContext.class);
        tabs = buildTabs();
    }

    @Path("{file:.*}")
    @GET
    public Response getResource(@PathParam("file") String fileName) throws IOException
    {
        if ( fileName.startsWith(jQueryUiPrefix) )
        {
            String      stripped = fileName.substring(jQueryUiPrefix.length());
            fileName = "css/jquery/" + context.getExhibitor().getJQueryStyle().name().toLowerCase() + "/" + stripped;
        }

        URL resource;
        try
        {
            resource = Resources.getResource("com/netflix/exhibitor/core/ui/" + fileName);
        }
        catch ( IllegalArgumentException dummy )
        {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        String resourceFile = resource.getFile();
        String contentType;
        if ( resourceFile.endsWith(".png") )
        {
            contentType = "image/png";  // not in default mime types
        }
        else if ( resourceFile.endsWith(".js") )
        {
            contentType = "text/javascript";  // not in default mime types
        }
        else if ( resourceFile.endsWith(".css") )
        {
            contentType = "text/css";  // not in default mime types
        }
        else
        {
            contentType = MimetypesFileTypeMap.getDefaultFileTypeMap().getContentType(resourceFile);
        }
        Object entity;
        if ( contentType.startsWith("text/") )
        {
            entity = Resources.toString(resource, Charset.forName("UTF-8"));
        }
        else
        {
            entity = Resources.toByteArray(resource);
        }
        return Response.ok(entity).type(contentType).build();
    }

    @Path("tabs")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAdditionalTabSpecs()
    {
        final AtomicInteger     index = new AtomicInteger(0);
        List<UITabSpec>         names = Lists.transform
        (
            tabs,
            new Function<UITab, UITabSpec>()
            {
                @Override
                public UITabSpec apply(UITab tab)
                {
                    String base = tab.contentIsHtml() ? HTML_UI_TAB_BASE_URL : TEXT_UI_TAB_BASE_URL;
                    return new UITabSpec(tab.getName(), base + index.getAndIncrement(), tab.contentIsHtml());
                }
            }
        );
        names = Lists.newArrayList(names);  // move out of Google's TransformingRandomAccessList

        GenericEntity<List<UITabSpec>> entity = new GenericEntity<List<UITabSpec>>(names){};
        return Response.ok(entity).build();
    }

    @Path("tab-html/{index}")
    @GET
    @Produces(MediaType.TEXT_HTML)
    public Response getAdditionalTabContent(@Context UriInfo info, @PathParam("index") int index) throws Exception
    {
        return getAdditionalTabHtmlContent(info, index);
    }

    @Path("tab/{index}")
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Response getAdditionalTabHtmlContent(@Context UriInfo info, @PathParam("index") int index) throws Exception
    {
        if ( (index < 0) || (index >= tabs.size()) )
        {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        return Response.ok(tabs.get(index).getContent(info)).build();
    }

    @Path("backup-config")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getBackupConfig() throws Exception
    {
        ArrayNode           node = JsonNodeFactory.instance.arrayNode();

        if ( context.getExhibitor().getBackupManager().isActive() )
        {
            EncodedConfigParser parser = context.getExhibitor().getBackupManager().getBackupConfigParser();
            List<BackupConfigSpec>  configs = context.getExhibitor().getBackupManager().getConfigSpecs();
            for ( BackupConfigSpec c : configs )
            {
                ObjectNode      n = JsonNodeFactory.instance.objectNode();
                String          value = parser.getValue(c.getKey());

                n.put("key", c.getKey());
                n.put("name", c.getDisplayName());
                n.put("help", c.getHelpText());
                n.put("value", (value != null) ? value : "");
                n.put("type", c.getType().name().toLowerCase().substring(0, 1));

                node.add(n);
            }
        }

        return JsonUtil.writeValueAsString(node);
    }

    @Path("shutdown")
    @GET
    public Response shutdown()
    {
        Runnable shutdownProc = context.getExhibitor().getShutdownProc();
        if ( shutdownProc == null )
        {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        shutdownProc.run();
        return Response.ok().build();
    }

    static String getLog(UIContext context)
    {
        List<String> log = context.getExhibitor().getLog().toDisplayList("\t", context.getExhibitor().getLogDirection());
        StringBuilder str = new StringBuilder();
        for ( String s : log )
        {
            str.append(s).append("\n");
        }

        return str.toString();
    }

    private ImmutableList<UITab> buildTabs()
    {
        ImmutableList.Builder<UITab> builder = ImmutableList.builder();

        builder.add
        (
            new UITabImpl("Log")
            {
                @Override
                public String getContent(UriInfo info) throws Exception
                {
                    return getLog(context);
                }
            }
        );
        Collection<UITab> additionalUITabs = context.getExhibitor().getAdditionalUITabs();
        if ( additionalUITabs != null )
        {
            builder.addAll(additionalUITabs);
        }

        return builder.build();
    }

    static String fixName(Enum c)
    {
        StringBuilder   str = new StringBuilder();
        String[]        parts = c.name().toLowerCase().split("_");
        for ( String p : parts )
        {
            if ( p.length() > 0 )
            {
                if ( str.length() > 0 )
                {
                    str.append(p.substring(0, 1).toUpperCase());
                    if ( p.length() > 1 )
                    {
                        str.append(p.substring(1));
                    }
                }
                else
                {
                    str.append(p);
                }
            }
        }
        return str.toString();
    }
}
