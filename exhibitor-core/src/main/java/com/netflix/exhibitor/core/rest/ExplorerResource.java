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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.netflix.curator.utils.ZKPaths;
import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.analyze.Analysis;
import com.netflix.exhibitor.core.analyze.PathAnalyzer;
import com.netflix.exhibitor.core.analyze.PathAndMax;
import com.netflix.exhibitor.core.analyze.PathComplete;
import com.netflix.exhibitor.core.analyze.UsageListing;
import com.netflix.exhibitor.core.entities.IdList;
import com.netflix.exhibitor.core.entities.PathAnalysis;
import com.netflix.exhibitor.core.entities.PathAnalysisNode;
import com.netflix.exhibitor.core.entities.PathAnalysisRequest;
import com.netflix.exhibitor.core.entities.Result;
import com.netflix.exhibitor.core.entities.UsageListingRequest;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.type.TypeReference;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ContextResolver;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Path("exhibitor/v1/explorer")
public class ExplorerResource
{
    private final UIContext         context;
    private final ExecutorService   executorService = Executors.newCachedThreadPool();

    private static final String         ERROR_KEY = "*";

    public ExplorerResource(@Context ContextResolver<UIContext> resolver)
    {
        context = resolver.getContext(UIContext.class);
    }

    public static String bytesToString(byte[] bytes)
    {
        StringBuilder       bytesStr = new StringBuilder();
        for ( byte b : bytes )
        {
            bytesStr.append(Integer.toHexString(b & 0xff)).append(" ");
        }
        return bytesStr.toString();
    }

    @DELETE
    @Path("znode/{path:.*}")
    @Produces("application/json")
    public Response deleteNode
        (
            @PathParam("path") String path,
            @HeaderParam("netflix-user-name") String trackingUserName,
            @HeaderParam("netflix-ticket-number") String trackingTicketNumber,
            @HeaderParam("netflix-reason") String trackingReason
        )
    {
        Response    response;
        do
        {
            path = "/" + path;
            context.getExhibitor().getLog().add(ActivityLog.Type.INFO, String.format("Delete node request received. Path [%s], Username [%s], Ticket Number [%s], Reason [%s]", path, trackingUserName, trackingTicketNumber, trackingReason));

            if ( !context.getExhibitor().nodeMutationsAllowed() )
            {
                response = Response.status(Response.Status.FORBIDDEN).build();
                break;
            }

            try
            {
                recursivelyDelete(path);
            }
            catch ( Exception e )
            {
                response = Response.ok(new Result(e)).build();
                break;
            }

            response = Response.ok(new Result("OK", true)).build();
        } while ( false );

        return response;
    }

    private void recursivelyDelete(String path) throws Exception
    {
        List<String>        children = context.getExhibitor().getLocalConnection().getChildren().forPath(path);
        for ( String name : children )
        {
            recursivelyDelete(ZKPaths.makePath(path, name));
        }

        context.getExhibitor().getLocalConnection().delete().forPath(path);
        context.getExhibitor().getLog().add(ActivityLog.Type.INFO, String.format("deleteNode() deleted node [%s]", path));
    }

    @PUT
    @Path("znode/{path:.*}")
    @Produces("application/json")
    @Consumes("application/json")
    public Response createNode
        (
            @PathParam("path") String path,
            @HeaderParam("netflix-user-name") String trackingUserName,
            @HeaderParam("netflix-ticket-number") String trackingTicketNumber,
            @HeaderParam("netflix-reason") String trackingReason,
            String binaryDataStr
        )
    {
        Response    response;
        do
        {
            path = "/" + path;
            context.getExhibitor().getLog().add(ActivityLog.Type.INFO, String.format("Create/update node request received. Path [%s], Username [%s], Ticket Number [%s], Reason [%s]", path, trackingUserName, trackingTicketNumber, trackingReason));

            if ( !context.getExhibitor().nodeMutationsAllowed() )
            {
                response = Response.status(Response.Status.FORBIDDEN).build();
                break;
            }

            try
            {
                binaryDataStr = binaryDataStr.replace(" ", "");
                byte[]      data = new byte[binaryDataStr.length() / 2];
                for ( int i = 0; i < data.length; ++i )
                {
                    String  hex = binaryDataStr.substring(i * 2, (i * 2) + 2);
                    int     val = Integer.parseInt(hex, 16);
                    data[i] = (byte)(val & 0xff);
                }

                try
                {
                    context.getExhibitor().getLocalConnection().setData().forPath(path, data);
                    context.getExhibitor().getLog().add(ActivityLog.Type.INFO, String.format("createNode() updated node [%s] to data [%s]", path, binaryDataStr));
                }
                catch ( KeeperException.NoNodeException dummy )
                {
                    context.getExhibitor().getLocalConnection().create().creatingParentsIfNeeded().forPath(path, data);
                    context.getExhibitor().getLog().add(ActivityLog.Type.INFO, String.format("createNode() created node [%s] with data [%s]", path, binaryDataStr));
                }
            }
            catch ( Exception e )
            {
                response = Response.ok(new Result(e)).build();
                break;
            }

            response = Response.ok(new Result("OK", true)).build();
        } while ( false );

        return response;
    }

    @GET
    @Path("node-data")
    @Produces("application/json")
    public String   getNodeData(@QueryParam("key") String key) throws Exception
    {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        try
        {
            Stat stat = context.getExhibitor().getLocalConnection().checkExists().forPath(key);
            byte[]          bytes = context.getExhibitor().getLocalConnection().getData().storingStatIn(stat).forPath(key);

            String          bytesStr = bytesToString(bytes);

            node.put("bytes", bytesStr);
            node.put("str", new String(bytes, "UTF-8"));
            node.put("stat", reflectToString(stat));
        }
        catch ( KeeperException.NoNodeException dummy )
        {
            node.put("bytes", "");
            node.put("str", "");
            node.put("stat", "* not found * ");
        }
        catch ( Throwable e )
        {
            node.put("bytes", "");
            node.put("str", "Exception");
            node.put("stat", e.getMessage());
        }
        return node.toString();
    }

    @GET
    @Path("node")
    @Produces("application/json")
    public String   getNode(@QueryParam("key") String key) throws Exception
    {
        ArrayNode children = JsonNodeFactory.instance.arrayNode();
        try
        {
            List<String> childrenNames = context.getExhibitor().getLocalConnection().getChildren().forPath(key);
            SmartSort.sortChildren(childrenNames);
            for ( String name : childrenNames )
            {
                ObjectNode  node = children.addObject();
                node.put("title", name);
                node.put("key", ZKPaths.makePath(key, name));
                node.put("isLazy", true);
                node.put("expand", false);
            }
        }
        catch ( Throwable e )
        {
            context.getExhibitor().resetLocalConnection();
            context.getExhibitor().getLog().add(ActivityLog.Type.ERROR, "getNode: " + key, e);

            ObjectNode  node = children.addObject();
            node.put("title", "* Exception *");
            node.put("key", ERROR_KEY);
            node.put("isLazy", false);
            node.put("expand", false);
        }

        return children.toString();
    }

    @GET
    @Path("usage-listing")
    @Produces("text/plain")
    public Response     getUsageListing(@QueryParam("request") String json) throws Exception
    {
        ObjectMapper        mapper = new ObjectMapper();
        UsageListingRequest usageListingRequest = mapper.getJsonFactory().createJsonParser(json).readValueAs(UsageListingRequest.class);
        return usageListing(usageListingRequest);
    }

    @POST
    @Path("usage-listing")
    @Consumes("application/json")
    @Produces("text/plain")
    public Response     usageListing(UsageListingRequest usageListingRequest) throws Exception
    {
        context.getExhibitor().getLog().add(ActivityLog.Type.INFO, "Starting usage listing");

        final UsageListing        usageListing = new UsageListing(context.getExhibitor(), usageListingRequest.getStartPath(), usageListingRequest.getMaxChildrenForTraversal());
        usageListing.generate();

        final PipedInputStream      in = new PipedInputStream();
        final PipedOutputStream     pipedOutputStream = new PipedOutputStream(in);
        executorService.submit
        (
            new Runnable()
            {
                @Override
                public void run()
                {
                    PrintStream     out = null;
                    try
                    {
                        out = new PrintStream(pipedOutputStream);
                        out.println("Path\tCreateDate\tChildQty\tDeepChildQty");

                        Iterator<String> iterator = usageListing.getPaths();
                        while ( iterator.hasNext() )
                        {
                            String                  path = iterator.next();
                            UsageListing.NodeEntry  details = usageListing.getNodeDetails(path);
                            out.println(path + "\t" + details.getCreationDate() + "\t" + details.getDirectChildQty() + "\t" + details.getDeepChildQty());
                        }
                    }
                    catch ( Exception e )
                    {
                        context.getExhibitor().getLog().add(ActivityLog.Type.ERROR, "Generating usage listing", e);
                    }
                    finally
                    {
                        Closeables.closeQuietly(out);
                    }
                }
            }
        );

        return Response.ok(in)
            .header("content-disposition", "attachment; filename=listing_tab_delimited.txt")
            .build();
    }

    @GET
    @Path("analyze")
    @Produces("application/json")
    public Response     getAnalyze(@QueryParam("request") String json) throws Exception
    {
        ObjectMapper                                mapper = new ObjectMapper();
        TypeReference<List<PathAnalysisRequest>>    ref = new TypeReference<List<PathAnalysisRequest>>(){};
        List<PathAnalysisRequest>                   paths = mapper.getJsonFactory().createJsonParser(json).readValueAs(ref);
        return analyze(paths);
    }

    @POST
    @Path("analyze")
    @Consumes("application/json")
    @Produces("application/json")
    public Response     analyze(List<PathAnalysisRequest> paths) throws Exception
    {
        context.getExhibitor().getLog().add(ActivityLog.Type.INFO, "Starting analysis");

        List<PathAndMax>    pathAndMaxes = Lists.transform
        (
            paths,
            new Function<PathAnalysisRequest, PathAndMax>()
            {
                @Override
                public PathAndMax apply(PathAnalysisRequest request)
                {
                    return new PathAndMax(request.getPath(), request.getMax());
                }
            }
        );
        PathAnalyzer        analyzer = new PathAnalyzer(context.getExhibitor(), pathAndMaxes);
        Analysis            analysis = analyzer.analyze();

        Iterable<PathAnalysisNode>  transformed = Iterables.transform
        (
            analysis.getCompleteData(),
            new Function<PathComplete, PathAnalysisNode>()
            {
                @Override
                public PathAnalysisNode apply(PathComplete pathComplete)
                {
                    return new PathAnalysisNode(pathComplete.getPath(), pathComplete.getMax(), pathComplete.getChildIds());
                }
            }
        );
        Iterable<IdList>   transformedPossibleCycles = Iterables.transform
        (
            analysis.getPossibleCycles(),
            new Function<Set<String>, IdList>()
            {
                @Override
                public IdList apply(Set<String> s)
                {
                    return new IdList(Lists.newArrayList(s));
                }
            }
        );
        String              error = analysis.getError();
        PathAnalysis        response;
        try
        {
            response = new PathAnalysis((error != null) ? error : "", Lists.newArrayList(transformed), Lists.newArrayList(transformedPossibleCycles));
        }
        catch ( Exception e )
        {
            context.getExhibitor().getLog().add(ActivityLog.Type.ERROR, "Error performing analysis", e);
            throw e;
        }
        return Response.ok(response)
            .header("content-disposition", "attachment; filename=analysis.txt")
            .build();
    }

    private String  reflectToString(Object obj) throws Exception
    {
        StringBuilder       str = new StringBuilder();
        for ( Field f : obj.getClass().getDeclaredFields() )
        {
            f.setAccessible(true);

            if ( str.length() > 0 )
            {
                str.append(", ");
            }
            str.append(f.getName()).append(": ");
            str.append(f.get(obj));
        }
        return str.toString();
    }
}
