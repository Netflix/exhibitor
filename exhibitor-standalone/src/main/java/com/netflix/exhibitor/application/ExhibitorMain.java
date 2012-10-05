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

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.netflix.curator.ensemble.exhibitor.DefaultExhibitorRestClient;
import com.netflix.curator.ensemble.exhibitor.ExhibitorEnsembleProvider;
import com.netflix.curator.ensemble.exhibitor.Exhibitors;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.api.ACLProvider;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.ExhibitorArguments;
import com.netflix.exhibitor.core.backup.BackupProvider;
import com.netflix.exhibitor.core.backup.filesystem.FileSystemBackupProvider;
import com.netflix.exhibitor.core.backup.s3.S3BackupProvider;
import com.netflix.exhibitor.core.config.AutoManageLockArguments;
import com.netflix.exhibitor.core.config.ConfigProvider;
import com.netflix.exhibitor.core.config.DefaultProperties;
import com.netflix.exhibitor.core.config.JQueryStyle;
import com.netflix.exhibitor.core.config.filesystem.FileSystemConfigProvider;
import com.netflix.exhibitor.core.config.none.NoneConfigProvider;
import com.netflix.exhibitor.core.config.s3.S3ConfigArguments;
import com.netflix.exhibitor.core.config.s3.S3ConfigAutoManageLockArguments;
import com.netflix.exhibitor.core.config.s3.S3ConfigProvider;
import com.netflix.exhibitor.core.config.zookeeper.ZookeeperConfigProvider;
import com.netflix.exhibitor.core.rest.UIContext;
import com.netflix.exhibitor.core.rest.jersey.JerseySupport;
import com.netflix.exhibitor.core.s3.PropertyBasedS3Credential;
import com.netflix.exhibitor.core.s3.S3ClientFactoryImpl;
import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.security.BasicAuthenticator;
import org.mortbay.jetty.security.Constraint;
import org.mortbay.jetty.security.ConstraintMapping;
import org.mortbay.jetty.security.Credential;
import org.mortbay.jetty.security.HashUserRealm;
import org.mortbay.jetty.security.SecurityHandler;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.netflix.exhibitor.application.ExhibitorCLI.*;

public class ExhibitorMain implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Server server;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final Exhibitor exhibitor;
    private final AtomicBoolean shutdownSignaled = new AtomicBoolean(false);

    public static void main(String[] args) throws Exception
    {
        ExhibitorCLI        cli = new ExhibitorCLI();

        CommandLine         commandLine;
        try
        {
            CommandLineParser   parser = new PosixParser();
            commandLine = parser.parse(cli.getOptions(), args);
            if ( commandLine.hasOption('?') || commandLine.hasOption(HELP) || (commandLine.getArgList().size() > 0) )
            {
                cli.printHelp();
                return;
            }
        }
        catch ( UnrecognizedOptionException e)
        {
            System.err.println("Unknown option: " + e.getOption());
            cli.printHelp();
            return;
        }
        catch ( ParseException e )
        {
            cli.printHelp();
            return;
        }

        if ( !checkMutuallyExclusive(cli, commandLine, S3_BACKUP, FILESYSTEMBACKUP) )
        {
            return;
        }

        PropertyBasedS3Credential awsCredentials = null;
        if ( commandLine.hasOption(S3_CREDENTIALS) )
        {
            awsCredentials = new PropertyBasedS3Credential(new File(commandLine.getOptionValue(S3_CREDENTIALS)));
        }

        BackupProvider backupProvider = null;
        if ( "true".equalsIgnoreCase(commandLine.getOptionValue(S3_BACKUP)) )
        {
            backupProvider = new S3BackupProvider(new S3ClientFactoryImpl(), awsCredentials);
        }
        else if ( "true".equalsIgnoreCase(commandLine.getOptionValue(FILESYSTEMBACKUP)) )
        {
            backupProvider = new FileSystemBackupProvider();
        }

        int timeoutMs = Integer.parseInt(commandLine.getOptionValue(TIMEOUT, "30000"));
        int logWindowSizeLines = Integer.parseInt(commandLine.getOptionValue(LOGLINES, "1000"));
        int configCheckMs = Integer.parseInt(commandLine.getOptionValue(CONFIGCHECKMS, "30000"));
        String useHostname = commandLine.getOptionValue(HOSTNAME, cli.getHostname());
        int httpPort = Integer.parseInt(commandLine.getOptionValue(HTTP_PORT, "8080"));
        String extraHeadingText = commandLine.getOptionValue(EXTRA_HEADING_TEXT, null);
        boolean allowNodeMutations = "true".equalsIgnoreCase(commandLine.getOptionValue(NODE_MUTATIONS));

        String configType = commandLine.hasOption(SHORT_CONFIG_TYPE) ? commandLine.getOptionValue(SHORT_CONFIG_TYPE) : (commandLine.hasOption(CONFIG_TYPE) ? commandLine.getOptionValue(CONFIG_TYPE) : null);
        if ( configType == null )
        {
            System.err.println("Configuration type (-" + SHORT_CONFIG_TYPE + " or --" + CONFIG_TYPE + ") must be specified");
            cli.printHelp();
            return;
        }

        ConfigProvider configProvider = makeConfigProvider(configType, cli, commandLine, awsCredentials, backupProvider, useHostname);
        if ( configProvider == null )
        {
            cli.printHelp();
            return;
        }
        boolean        isNoneConfigProvider = (configProvider instanceof NoneConfigProvider);
        if ( isNoneConfigProvider )
        {
            backupProvider = null;
        }

        JQueryStyle jQueryStyle;
        try
        {
            jQueryStyle = JQueryStyle.valueOf(commandLine.getOptionValue(JQUERY_STYLE, "red").toUpperCase());
        }
        catch ( IllegalArgumentException e )
        {
            cli.printHelp();
            return;
        }

        String realm = commandLine.getOptionValue(BASIC_AUTH_REALM);
        String user = commandLine.getOptionValue(CONSOLE_USER);
        String password = commandLine.getOptionValue(CONSOLE_PASSWORD);
        String curatorUser = commandLine.getOptionValue(CURATOR_USER);
        String curatorPassword = commandLine.getOptionValue(CURATOR_PASSWORD);
        SecurityHandler handler = null;
        if ( notNullOrEmpty(realm) && notNullOrEmpty(user) && notNullOrEmpty(password) && notNullOrEmpty(curatorUser) && notNullOrEmpty(curatorPassword) )
        {
            handler = getSecurityHandler(realm, user, password, curatorUser, curatorPassword);
        }

        String      aclId = commandLine.getOptionValue(ACL_ID);
        String      aclScheme = commandLine.getOptionValue(ACL_SCHEME);
        String      aclPerms = commandLine.getOptionValue(ACL_PERMISSIONS);
        ACLProvider aclProvider = null;
        if ( notNullOrEmpty(aclId) || notNullOrEmpty(aclScheme) || notNullOrEmpty(aclPerms) )
        {
            aclProvider = getAclProvider(cli, aclId, aclScheme, aclPerms);
            if ( aclProvider == null )
            {
                return;
            }
        }

        ExhibitorArguments.Builder builder = ExhibitorArguments.builder()
            .connectionTimeOutMs(timeoutMs)
            .logWindowSizeLines(logWindowSizeLines)
            .thisJVMHostname(useHostname)
            .configCheckMs(configCheckMs)
            .extraHeadingText(extraHeadingText)
            .allowNodeMutations(allowNodeMutations)
            .jQueryStyle(jQueryStyle)
            .restPort(httpPort)
            .aclProvider(aclProvider);

        ExhibitorMain exhibitorMain = new ExhibitorMain(backupProvider, configProvider, builder, httpPort, handler);
        setShutdown(exhibitorMain);

        exhibitorMain.start();
        try
        {
            exhibitorMain.join();
        }
        finally
        {
            exhibitorMain.close();
        }
    }

    public ExhibitorMain(BackupProvider backupProvider, ConfigProvider configProvider, ExhibitorArguments.Builder builder, int httpPort) throws Exception
    {
        this(backupProvider,configProvider,builder,httpPort,null);
    }

    private static ConfigProvider makeConfigProvider(String configType, ExhibitorCLI cli, CommandLine commandLine, PropertyBasedS3Credential awsCredentials, BackupProvider backupProvider, String useHostname) throws Exception
    {
        ConfigProvider      configProvider;
        if ( configType.equals("s3") )
        {
            configProvider = getS3Provider(cli, commandLine, awsCredentials, useHostname);
        }
        else if ( configType.equals("file") )
        {
            configProvider = getFileSystemProvider(commandLine, backupProvider, useHostname);
        }
        else if ( configType.equals("zookeeper") )
        {
            configProvider = getZookeeperProvider(commandLine, useHostname);
        }
        else if ( configType.equals("none") )
        {
            System.out.println("Warning: you have intentionally turned off shared configuration. This mode is meant for special purposes only. Please verify that this is your intent.");
            configProvider = getNoneProvider(commandLine);
        }
        else
        {
            configProvider = null;
            System.err.println("Unknown configtype: " + configType);
        }
        return configProvider;
    }

    private static ConfigProvider getNoneProvider(CommandLine commandLine)
    {
        if ( !commandLine.hasOption(NONE_CONFIG_DIRECTORY) )
        {
            System.err.println(NONE_CONFIG_DIRECTORY + " is required when configtype is \"none\"");
            return null;
        }

        return new NoneConfigProvider(commandLine.getOptionValue(NONE_CONFIG_DIRECTORY));
    }

    private static ConfigProvider getZookeeperProvider(CommandLine commandLine, String useHostname) throws Exception
    {
        String      connectString = commandLine.getOptionValue(ZOOKEEPER_CONFIG_INITIAL_CONNECT_STRING);
        String      path = commandLine.getOptionValue(ZOOKEEPER_CONFIG_BASE_PATH);
        String      retrySpec = commandLine.getOptionValue(ZOOKEEPER_CONFIG_RETRY, DEFAULT_ZOOKEEPER_CONFIG_RETRY);
        if ( (path == null) || (connectString == null) )
        {
            System.err.println("Both " + ZOOKEEPER_CONFIG_INITIAL_CONNECT_STRING + " and " + ZOOKEEPER_CONFIG_INITIAL_CONNECT_STRING + " are required when the configtype is zookeeper");
            return null;
        }

        try
        {
            PathUtils.validatePath(path);
        }
        catch ( IllegalArgumentException e )
        {
            System.err.println("Invalid " + ZOOKEEPER_CONFIG_BASE_PATH + ": " + path);
            return null;
        }

        String[]    retryParts = retrySpec.split("\\:");
        if ( retryParts.length != 2 )
        {
            System.err.println("Bad " + ZOOKEEPER_CONFIG_RETRY + " value: " + retrySpec);
            return null;
        }

        int         baseSleepTimeMs;
        int         maxRetries;
        try
        {
            baseSleepTimeMs = Integer.parseInt(retryParts[0]);
            maxRetries = Integer.parseInt(retryParts[1]);
        }
        catch ( NumberFormatException e )
        {
            System.err.println("Bad " + ZOOKEEPER_CONFIG_RETRY + " value: " + retrySpec);
            return null;
        }

        int         pollingMs;
        try
        {
            pollingMs = Integer.parseInt(commandLine.getOptionValue(ZOOKEEPER_CONFIG_POLLING, DEFAULT_ZOOKEEPER_CONFIG_POLLING));
        }
        catch ( NumberFormatException e )
        {
            System.err.println("Bad " + ZOOKEEPER_CONFIG_POLLING + " value: " + commandLine.getOptionValue(ZOOKEEPER_CONFIG_POLLING, DEFAULT_ZOOKEEPER_CONFIG_POLLING));
            return null;
        }

        CuratorFramework client = makeCurator(connectString, baseSleepTimeMs, maxRetries, pollingMs);
        if ( client == null )
        {
            return null;
        }

        return new ZookeeperConfigProvider(client, path, new Properties(), useHostname);
    }

    private static ACLProvider getAclProvider(ExhibitorCLI cli, String aclId, String aclScheme, String aclPerms)
    {
        int     perms;
        if ( notNullOrEmpty(aclPerms) )
        {
            perms = 0;
            for ( String verb : aclPerms.split(",") )
            {
                verb = verb.trim();
                if ( verb.equalsIgnoreCase("read") )
                {
                    perms |= ZooDefs.Perms.READ;
                }
                else if ( verb.equalsIgnoreCase("write") )
                {
                    perms |= ZooDefs.Perms.WRITE;
                }
                else if ( verb.equalsIgnoreCase("create") )
                {
                    perms |= ZooDefs.Perms.CREATE;
                }
                else if ( verb.equalsIgnoreCase("delete") )
                {
                    perms |= ZooDefs.Perms.DELETE;
                }
                else if ( verb.equalsIgnoreCase("admin") )
                {
                    perms |= ZooDefs.Perms.ADMIN;
                }
                else
                {
                    System.err.println("Unknown ACL perm value: " + verb);
                    cli.printHelp();
                    return null;
                }
            }
        }
        else
        {
            perms = ZooDefs.Perms.ALL;
        }

        if ( aclId == null )
        {
            aclId = "";
        }
        if ( aclScheme == null )
        {
            aclScheme = "";
        }

        final ACL       acl = new ACL(perms, new Id(aclScheme, aclId));
        return new ACLProvider()
        {
            @Override
            public List<ACL> getDefaultAcl()
            {
                return Collections.singletonList(acl);
            }

            @Override
            public List<ACL> getAclForPath(String path)
            {
                return Collections.singletonList(acl);
            }
        };
    }

    private static ConfigProvider getFileSystemProvider(CommandLine commandLine, BackupProvider backupProvider, String hostname) throws IOException
    {
        File directory = commandLine.hasOption(FILESYSTEM_CONFIG_DIRECTORY) ? new File(commandLine.getOptionValue(FILESYSTEM_CONFIG_DIRECTORY)) : new File(System.getProperty("user.dir"));
        String name = commandLine.hasOption(FILESYSTEM_CONFIG_NAME) ? commandLine.getOptionValue(FILESYSTEM_CONFIG_NAME) : DEFAULT_FILESYSTEMCONFIG_NAME;
        String prefix = commandLine.hasOption(FILESYSTEM_CONFIG_PREFIX) ? commandLine.getOptionValue(FILESYSTEM_CONFIG_PREFIX) : DEFAULT_FILESYSTEMCONFIG_PREFIX;
        String lockPrefix = commandLine.hasOption(FILESYSTEM_CONFIG_LOCK_PREFIX) ? commandLine.getOptionValue(FILESYSTEM_CONFIG_LOCK_PREFIX) : DEFAULT_FILESYSTEMCONFIG_LOCK_PREFIX;
        return new FileSystemConfigProvider(directory, name, prefix, DefaultProperties.get(backupProvider), new AutoManageLockArguments(lockPrefix), hostname);
    }

    private static ConfigProvider getS3Provider(ExhibitorCLI cli, CommandLine commandLine, PropertyBasedS3Credential awsCredentials, String hostname) throws Exception
    {
        ConfigProvider provider;
        String  prefix = cli.getOptions().hasOption(S3_CONFIG_PREFIX) ? commandLine.getOptionValue(S3_CONFIG_PREFIX) : DEFAULT_FILESYSTEMCONFIG_PREFIX;
        provider = new S3ConfigProvider(new S3ClientFactoryImpl(), awsCredentials, getS3Arguments(cli, commandLine.getOptionValue(S3_CONFIG), prefix), hostname);
        return provider;
    }

    private static boolean checkMutuallyExclusive(ExhibitorCLI cli, CommandLine commandLine, String option1, String option2)
    {
        if ( commandLine.hasOption(option1) && commandLine.hasOption(option2) )
        {
            System.err.println(option1 + " and " + option2 + " cannot be used at the same time");
            cli.printHelp();
            return false;
        }
        return true;
    }

    private static S3ConfigArguments getS3Arguments(ExhibitorCLI cli, String value, String prefix)
    {
        String[]        parts = value.split(":");
        if ( parts.length != 2 )
        {
            System.err.println("Bad s3config argument: " + value);
            cli.printHelp();
            return null;
        }
        return new S3ConfigArguments(parts[0].trim(), parts[1].trim(), prefix, new S3ConfigAutoManageLockArguments(prefix + "-lock-"));
    }

    private static CuratorFramework makeCurator(final String connectString, int baseSleepTimeMs, int maxRetries, int pollingMs)
    {
        List<String>    hostnames = Lists.newArrayList();
        int             port = 0;
        String[]        parts = connectString.split(",");
        for ( String spec : parts )
        {
            String[]        subParts = spec.split(":");
            int             thisPort;
            try
            {
                thisPort = Integer.parseInt(subParts[1]);
                if ( subParts.length != 2 )
                {
                    System.err.println("Bad connection string: " + connectString);
                    return null;
                }
            }
            catch ( NumberFormatException e )
            {
                System.err.println("Bad connection string: " + connectString);
                return null;
            }

            hostnames.add(subParts[0]);
            if ( (port != 0) && (port != thisPort) )
            {
                System.err.println("The ports in the connection string must all be the same: " + connectString);
                return null;
            }
            port = thisPort;
        }

        ExponentialBackoffRetry                     retryPolicy = new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries);
        Exhibitors.BackupConnectionStringProvider   backupConnectionStringProvider = new Exhibitors.BackupConnectionStringProvider()
        {
            @Override
            public String getBackupConnectionString() throws Exception
            {
                return connectString;
            }
        };
        Exhibitors                                  exhibitors = new Exhibitors(hostnames, port, backupConnectionStringProvider);
        return CuratorFrameworkFactory
            .builder()
            .connectString(connectString)
            .ensembleProvider(new ExhibitorEnsembleProvider(exhibitors, new DefaultExhibitorRestClient(), "/exhibitor/v1/cluster/list", pollingMs, retryPolicy))
            .retryPolicy(retryPolicy)
            .build();
    }


    public ExhibitorMain(BackupProvider backupProvider, ConfigProvider configProvider, ExhibitorArguments.Builder builder, int httpPort, SecurityHandler security) throws Exception
    {
        builder.shutdownProc(makeShutdownProc(this));
        exhibitor = new Exhibitor(configProvider, null, backupProvider, builder.build());
        exhibitor.start();

        DefaultResourceConfig   application = JerseySupport.newApplicationConfig(new UIContext(exhibitor));
        ServletContainer        container = new ServletContainer(application);
        server = new Server(httpPort);
        Context root = new Context(server, "/", Context.SESSIONS);
        root.addServlet(new ServletHolder(container), "/*");
        if(security != null)
        {
            root.setSecurityHandler(security);
        }
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

            Closeables.closeQuietly(exhibitor);
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

    private static boolean notNullOrEmpty(String arg)
    {
        return arg != null && (! "".equals(arg));
    }

    private static SecurityHandler getSecurityHandler(String realm, String consoleUser, String consolePassword, String curatorUser, String curatorPassword)
    {

        HashUserRealm userRealm = new HashUserRealm(realm);
        userRealm.put(consoleUser, Credential.getCredential(consolePassword));
        userRealm.addUserToRole(consoleUser,"console");
        userRealm.put(curatorUser, Credential.getCredential(curatorPassword));
        userRealm.addUserToRole(curatorUser, "curator");

        Constraint console = new Constraint();
        console.setName("consoleauth");
        console.setRoles(new String[]{"console"});
        console.setAuthenticate(true);

        Constraint curator = new Constraint();
        curator.setName("curatorauth");
        curator.setRoles(new String[]{"curator", "console"});
        curator.setAuthenticate(true);

        ConstraintMapping consoleMapping = new ConstraintMapping();
        consoleMapping.setConstraint(console);
        consoleMapping.setPathSpec("/*");

        ConstraintMapping curatorMapping = new ConstraintMapping();
        curatorMapping.setConstraint(curator);
        curatorMapping.setPathSpec("/exhibitor/v1/cluster/list");

        SecurityHandler handler = new SecurityHandler();
        handler.setUserRealm(userRealm);
        handler.setConstraintMappings(new ConstraintMapping[]{consoleMapping,curatorMapping});
        handler.setAuthenticator(new BasicAuthenticator());

        return handler;
    }
}
