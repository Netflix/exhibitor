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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import com.netflix.exhibitor.core.backup.BackupConfigSpec;
import com.netflix.exhibitor.core.config.ConfigManager;
import com.netflix.exhibitor.core.config.EncodedConfigParser;
import com.netflix.exhibitor.core.config.InstanceConfig;
import com.netflix.exhibitor.core.config.IntConfigs;
import com.netflix.exhibitor.core.config.PseudoLock;
import com.netflix.exhibitor.core.config.StringConfigs;
import com.netflix.exhibitor.core.entities.Result;
import com.netflix.exhibitor.core.state.FourLetterWord;
import com.netflix.exhibitor.core.state.ServerList;
import com.netflix.exhibitor.core.state.ServerSpec;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ContextResolver;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Path("exhibitor/v1/config")
public class ConfigResource
{
    private final UIContext context;

    private static final String CANT_UPDATE_CONFIG_MESSAGE = "It appears that another process has updated the config. Your change was not committed.";

    public ConfigResource(@Context ContextResolver<UIContext> resolver)
    {
        context = resolver.getContext(UIContext.class);
    }

    @Path("get-state")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSystemState(@Context Request request) throws Exception
    {
        InstanceConfig              config = context.getExhibitor().getConfigManager().getConfig();

        String                      response = new FourLetterWord(FourLetterWord.Word.RUOK, config, context.getExhibitor().getConnectionTimeOutMs()).getResponse();
        ServerList                  serverList = new ServerList(config.getString(StringConfigs.SERVERS_SPEC));
        ServerSpec                  us = Iterables.find(serverList.getSpecs(), ServerList.isUs(context.getExhibitor().getThisJVMHostname()), null);

        ObjectNode                  mainNode = JsonNodeFactory.instance.objectNode();
        ObjectNode                  configNode = JsonNodeFactory.instance.objectNode();
        ObjectNode                  controlPanelNode = JsonNodeFactory.instance.objectNode();

        mainNode.put("version", context.getExhibitor().getVersion());
        mainNode.put("running", "imok".equals(response));
        mainNode.put("backupActive", context.getExhibitor().getBackupManager().isActive());
        mainNode.put("standaloneMode", context.getExhibitor().getConfigManager().isStandaloneMode());
        mainNode.put("extraHeadingText", context.getExhibitor().getExtraHeadingText());
        mainNode.put("nodeMutationsAllowed", context.getExhibitor().nodeMutationsAllowed());

        configNode.put("rollInProgress", context.getExhibitor().getConfigManager().isRolling());
        configNode.put("rollStatus", context.getExhibitor().getConfigManager().getRollingConfigState().getRollingStatus());
        configNode.put("rollPercentDone", context.getExhibitor().getConfigManager().getRollingConfigState().getRollingPercentDone());

        configNode.put("hostname", context.getExhibitor().getThisJVMHostname());
        configNode.put("serverId", (us != null) ? us.getServerId() : -1);
        for ( StringConfigs c : StringConfigs.values() )
        {
            configNode.put(fixName(c), config.getString(c));
        }
        for ( IntConfigs c : IntConfigs.values() )
        {
            String fixedName = fixName(c);
            int value = config.getInt(c);

            configNode.put(fixedName, value);
        }

        EncodedConfigParser     zooCfgParser = new EncodedConfigParser(config.getString(StringConfigs.ZOO_CFG_EXTRA));
        ObjectNode              zooCfgNode = JsonNodeFactory.instance.objectNode();
        for ( EncodedConfigParser.FieldValue fv : zooCfgParser.getFieldValues() )
        {
            zooCfgNode.put(fv.getField(), fv.getValue());
        }
        configNode.put("zooCfgExtra", zooCfgNode);

        if ( context.getExhibitor().getBackupManager().isActive() )
        {
            ObjectNode          backupExtraNode = JsonNodeFactory.instance.objectNode();
            EncodedConfigParser parser = context.getExhibitor().getBackupManager().getBackupConfigParser();
            List<BackupConfigSpec> configs = context.getExhibitor().getBackupManager().getConfigSpecs();
            for ( BackupConfigSpec c : configs )
            {
                String value = parser.getValue(c.getKey());
                backupExtraNode.put(c.getKey(), (value != null) ? value : "");
            }
            configNode.put("backupExtra", backupExtraNode);
        }

        configNode.put("controlPanel", controlPanelNode);
        mainNode.put("config", configNode);

        String      json = JsonUtil.writeValueAsString(mainNode);
        EntityTag   tag = new EntityTag(Hashing.sha1().hashString(json).toString());

        Response.ResponseBuilder    builder = request.evaluatePreconditions(tag);
        if ( builder == null )
        {
            builder = Response.ok(json).tag(tag);
        }

        return builder.build();
    }

    @Path("rollback-rolling")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response rollbackRolling() throws Exception
    {
        context.getExhibitor().getConfigManager().cancelRollingConfig(ConfigManager.CancelMode.ROLLBACK);
        return Response.ok(new Result("OK", true)).build();
    }

    @Path("force-commit-rolling")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response forceCommitRolling() throws Exception
    {
        context.getExhibitor().getConfigManager().cancelRollingConfig(ConfigManager.CancelMode.FORCE_COMMIT);
        return Response.ok(new Result("OK", true)).build();
    }

    @Path("set-rolling")
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response setConfigRolling(String newConfigJson) throws Exception
    {
        InstanceConfig  wrapped = parseToConfig(newConfigJson);

        Result  result = null;
        try
        {
            PseudoLock  lock = context.getExhibitor().getConfigManager().newConfigBasedLock();
            try
            {
                if ( lock.lock(context.getExhibitor().getLog(), 10, TimeUnit.SECONDS) )  // TODO consider making configurable in the future
                {
                    if ( context.getExhibitor().getConfigManager().startRollingConfig(wrapped, null) )
                    {
                        result = new Result("OK", true);
                    }
                }
            }
            finally
            {
                lock.unlock();
            }

            if ( result == null )
            {
                result = new Result("Another process has updated the config.", false);
            }
            context.getExhibitor().resetLocalConnection();
        }
        catch ( Exception e )
        {
            result = new Result(e);
        }

        return Response.ok(result).build();
    }

    @Path("set")
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response setConfig(String newConfigJson) throws Exception
    {
        InstanceConfig wrapped = parseToConfig(newConfigJson);

        Result  result = null;
        try
        {
            PseudoLock  lock = context.getExhibitor().getConfigManager().newConfigBasedLock();
            try
            {
                if ( lock.lock(context.getExhibitor().getLog(), 10, TimeUnit.SECONDS) )  // TODO consider making configurable in the future
                {
                    if ( context.getExhibitor().getConfigManager().updateConfig(wrapped) )
                    {
                        result = new Result("OK", true);
                    }
                }
            }
            finally
            {
                lock.unlock();
            }

            if ( result == null )
            {
                result = new Result(CANT_UPDATE_CONFIG_MESSAGE, false);
            }
            context.getExhibitor().resetLocalConnection();
        }
        catch ( Exception e )
        {
            result = new Result(e);
        }

        return Response.ok(result).build();
    }

    private InstanceConfig parseToConfig(String newConfigJson) throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();
        final JsonNode tree = mapper.readTree(mapper.getJsonFactory().createJsonParser(newConfigJson));

        String                backupExtraValue = "";
        if ( tree.get("backupExtra") != null )
        {
            List<EncodedConfigParser.FieldValue>    values = Lists.newArrayList();
            JsonNode                backupExtra = tree.get("backupExtra");
            Iterator<String> fieldNames = backupExtra.getFieldNames();
            while ( fieldNames.hasNext() )
            {
                String      name = fieldNames.next();
                String      value = backupExtra.get(name).getTextValue();
                values.add(new EncodedConfigParser.FieldValue(name, value));
            }
            backupExtraValue = new EncodedConfigParser(values).toEncoded();
        }

        List<EncodedConfigParser.FieldValue>    zooCfgValues = Lists.newArrayList();
        JsonNode                zooCfgExtra = tree.get("zooCfgExtra");
        Iterator<String>        fieldNames = zooCfgExtra.getFieldNames();
        while ( fieldNames.hasNext() )
        {
            String      name = fieldNames.next();
            String      value = zooCfgExtra.get(name).getTextValue();
            zooCfgValues.add(new EncodedConfigParser.FieldValue(name, value));
        }
        final String          zooCfgExtraValue = new EncodedConfigParser(zooCfgValues).toEncoded();

        final String          finalBackupExtraValue = backupExtraValue;
        return new InstanceConfig()
        {
            @Override
            public String getString(StringConfigs config)
            {
                switch ( config )
                {
                    case BACKUP_EXTRA:
                    {
                        return finalBackupExtraValue;
                    }

                    case ZOO_CFG_EXTRA:
                    {
                        return zooCfgExtraValue;
                    }

                    default:
                    {
                        // NOP
                        break;
                    }
                }

                JsonNode node = tree.get(fixName(config));
                if ( node == null )
                {
                    return "";
                }
                return node.getTextValue();
            }

            @Override
            public int getInt(IntConfigs config)
            {
                JsonNode node = tree.get(fixName(config));
                if ( node == null )
                {
                    return 0;
                }
                try
                {
                    return Integer.parseInt(node.getTextValue());
                }
                catch ( NumberFormatException e )
                {
                    // ignore
                }
                return 0;
            }
        };
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
