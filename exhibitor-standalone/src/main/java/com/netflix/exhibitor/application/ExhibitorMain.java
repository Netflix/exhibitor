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

package com.netflix.exhibitor.application;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.netflix.exhibitor.servlet.ExhibitorServletFilter;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.ExhibitorArguments;
import com.netflix.exhibitor.core.RemoteConnectionConfiguration;
import com.netflix.exhibitor.core.backup.BackupProvider;
import com.netflix.exhibitor.core.config.ConfigProvider;
import com.netflix.exhibitor.core.rest.UIContext;
import com.netflix.exhibitor.core.rest.jersey.JerseySupport;
import com.netflix.exhibitor.standalone.ExhibitorCreator;
import com.netflix.exhibitor.standalone.ExhibitorCreatorExit;
import com.netflix.exhibitor.standalone.SecurityArguments;
import com.sun.jersey.api.client.filter.ClientFilter;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.api.client.filter.HTTPDigestAuthFilter;
import com.sun.jersey.api.core.DefaultResourceConfig;
import org.apache.curator.utils.CloseableUtils;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.ContextHandler;
import org.mortbay.jetty.security.HashUserRealm;
import org.mortbay.jetty.security.SecurityHandler;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.jetty.webapp.WebXmlConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExhibitorMain implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Server server;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final Exhibitor exhibitor;
    private final AtomicBoolean shutdownSignaled = new AtomicBoolean(false);
    private final Map<String, String> users = Maps.newHashMap();

    public static void main(String[] args) throws Exception
    {
        ExhibitorCreator creator;
        try
        {
            creator = new ExhibitorCreator(args);
        }
        catch ( ExhibitorCreatorExit exit )
        {
            if ( exit.getError() != null )
            {
                System.err.println(exit.getError());
            }

            exit.getCli().printHelp();
            return;
        }

        SecurityArguments securityArguments = new SecurityArguments(creator.getSecurityFile(), creator.getRealmSpec(), creator.getRemoteAuthSpec());
        ExhibitorMain exhibitorMain = new ExhibitorMain
        (
            creator.getBackupProvider(),
            creator.getConfigProvider(),
            creator.getBuilder(),
            creator.getHttpPort(),
            creator.getSecurityHandler(),
            securityArguments
        );
        setShutdown(exhibitorMain);

        exhibitorMain.start();
        try
        {
            exhibitorMain.join();
        }
        finally
        {
            exhibitorMain.close();

            for ( Closeable closeable : creator.getCloseables() )
            {
                CloseableUtils.closeQuietly(closeable);
            }
        }
    }

    public ExhibitorMain(BackupProvider backupProvider, ConfigProvider configProvider, ExhibitorArguments.Builder builder, int httpPort, SecurityHandler security, SecurityArguments securityArguments) throws Exception
    {
        HashUserRealm realm = makeRealm(securityArguments);
        if ( securityArguments.getRemoteAuthSpec() != null )
        {
            addRemoteAuth(builder, securityArguments.getRemoteAuthSpec());
        }

        builder.shutdownProc(makeShutdownProc(this));
        exhibitor = new Exhibitor(configProvider, null, backupProvider, builder.build());
        exhibitor.start();

        DefaultResourceConfig   application = JerseySupport.newApplicationConfig(new UIContext(exhibitor));
        ServletContainer        container = new ServletContainer(application);
        server = new Server(httpPort);
        Context root = new Context(server, "/", Context.SESSIONS);
        root.addFilter(ExhibitorServletFilter.class, "/", Handler.ALL);
        root.addServlet(new ServletHolder(container), "/*");
        if ( security != null )
        {
            root.setSecurityHandler(security);
        }
        else if ( securityArguments.getSecurityFile() != null )
        {
            addSecurityFile(realm, securityArguments.getSecurityFile(), root);
        }
    }

    private void addRemoteAuth(ExhibitorArguments.Builder builder, String remoteAuthSpec)
    {
        String[] parts = remoteAuthSpec.split(":");
        Preconditions.checkArgument(parts.length == 2, "Badly formed remote client authorization: " + remoteAuthSpec);

        String type = parts[0].trim();
        String userName = parts[1].trim();

        String password = Preconditions.checkNotNull(users.get(userName), "Realm user not found: " + userName);

        ClientFilter filter;
        if ( type.equals("basic") )
        {
            filter = new HTTPBasicAuthFilter(userName, password);
        }
        else if ( type.equals("digest") )
        {
            filter = new HTTPDigestAuthFilter(userName, password);
        }
        else
        {
            throw new IllegalStateException("Unknown remote client authorization type: " + type);
        }

        builder.remoteConnectionConfiguration(new RemoteConnectionConfiguration(Arrays.asList(filter)));
    }

    public void start() throws Exception
    {
        server.start();
    }

    public void join()
    {
        try
        {
            while ( !shutdownSignaled.get() && !Thread.currentThread().isInterrupted() )
            {
                Thread.sleep(5000);
            }
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() throws IOException
    {
        if ( isClosed.compareAndSet(false, true) )
        {
            log.info("Shutting down");

            CloseableUtils.closeQuietly(exhibitor);
            try
            {
                server.stop();
            }
            catch ( Exception e )
            {
                log.error("Error shutting down Jetty", e);
            }
            server.destroy();
        }
    }

    private static void setShutdown(final ExhibitorMain exhibitorMain)
    {
        Runtime.getRuntime().addShutdownHook
        (
            new Thread
            (
                makeShutdownProc(exhibitorMain)
            )
        );
    }

    private static Runnable makeShutdownProc(final ExhibitorMain exhibitorMain)
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                exhibitorMain.shutdownSignaled.set(true);
            }
        };
    }

    private void addSecurityFile(HashUserRealm realm, String securityFile, Context root) throws Exception
    {
        // create a temp Jetty context to parse the security portion of the web.xml file

        /*
            TODO

            This code assumes far too much internal knowledge of Jetty. I don't know
            of simple way to parse the web.xml though and don't want to write it myself.
         */

        final URL url = new URL("file", null, securityFile);
        final WebXmlConfiguration webXmlConfiguration = new WebXmlConfiguration();
        WebAppContext context = new WebAppContext();
        context.setServer(server);
        webXmlConfiguration.setWebAppContext(context);
        ContextHandler contextHandler = new ContextHandler("/")
        {
            @Override
            protected void startContext() throws Exception
            {
                super.startContext();
                setServer(server);
                webXmlConfiguration.configure(url.toString());
            }
        };
        try
        {
            SecurityHandler securityHandler = webXmlConfiguration.getWebAppContext().getSecurityHandler();

            if ( realm != null )
            {
                securityHandler.setUserRealm(realm);
            }

            root.setSecurityHandler(securityHandler);
            contextHandler.start();
        }
        finally
        {
            contextHandler.stop();
        }
    }

    private HashUserRealm makeRealm(SecurityArguments securityArguments) throws Exception
    {
        if ( securityArguments.getRealmSpec() == null )
        {
            return null;
        }

        String[] parts = securityArguments.getRealmSpec().split(":");
        if ( parts.length != 2 )
        {
            throw new Exception("Bad realm spec: " + securityArguments.getRealmSpec());
        }

        return new HashUserRealm(parts[0].trim(), parts[1].trim())
        {
            @Override
            public Object put(Object name, Object credentials)
            {
                users.put(String.valueOf(name), String.valueOf(credentials));

                return super.put(name, credentials);
            }
        };
    }
}
