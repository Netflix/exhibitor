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

package com.netflix.exhibitor.standalone;

import com.google.common.collect.Lists;
import com.netflix.curator.ensemble.exhibitor.DefaultExhibitorRestClient;
import com.netflix.curator.ensemble.exhibitor.ExhibitorEnsembleProvider;
import com.netflix.curator.ensemble.exhibitor.Exhibitors;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.api.ACLProvider;
import com.netflix.curator.retry.ExponentialBackoffRetry;
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
import com.netflix.exhibitor.core.s3.PropertyBasedS3Credential;
import com.netflix.exhibitor.core.s3.S3ClientFactoryImpl;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.mortbay.jetty.security.BasicAuthenticator;
import org.mortbay.jetty.security.Constraint;
import org.mortbay.jetty.security.ConstraintMapping;
import org.mortbay.jetty.security.Credential;
import org.mortbay.jetty.security.HashUserRealm;
import org.mortbay.jetty.security.SecurityHandler;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class ExhibitorCreator
{
    private final ExhibitorArguments.Builder builder;
    private final SecurityHandler securityHandler;
    private final BackupProvider backupProvider;
    private final ConfigProvider configProvider;
    private final int httpPort;

    public ExhibitorCreator(String[] args) throws Exception
    {
        ExhibitorCLI        cli = new ExhibitorCLI();

        CommandLine commandLine;
        try
        {
            CommandLineParser parser = new PosixParser();
            commandLine = parser.parse(cli.getOptions(), args);
            if ( commandLine.hasOption('?') || commandLine.hasOption(ExhibitorCLI.HELP) || (commandLine.getArgList().size() > 0) )
            {
                throw new ExhibitorCreatorExit(cli);
            }
        }
        catch ( UnrecognizedOptionException e)
        {
            throw new ExhibitorCreatorExit("Unknown option: " + e.getOption(), cli);
        }
        catch ( ParseException e )
        {
            throw new ExhibitorCreatorExit(cli);
        }

        checkMutuallyExclusive(cli, commandLine, ExhibitorCLI.S3_BACKUP, ExhibitorCLI.FILESYSTEMBACKUP);

        PropertyBasedS3Credential awsCredentials = null;
        if ( commandLine.hasOption(ExhibitorCLI.S3_CREDENTIALS) )
        {
            awsCredentials = new PropertyBasedS3Credential(new File(commandLine.getOptionValue(ExhibitorCLI.S3_CREDENTIALS)));
        }

        BackupProvider backupProvider = null;
        if ( "true".equalsIgnoreCase(commandLine.getOptionValue(ExhibitorCLI.S3_BACKUP)) )
        {
            backupProvider = new S3BackupProvider(new S3ClientFactoryImpl(), awsCredentials);
        }
        else if ( "true".equalsIgnoreCase(commandLine.getOptionValue(ExhibitorCLI.FILESYSTEMBACKUP)) )
        {
            backupProvider = new FileSystemBackupProvider();
        }

        int timeoutMs = Integer.parseInt(commandLine.getOptionValue(ExhibitorCLI.TIMEOUT, "30000"));
        int logWindowSizeLines = Integer.parseInt(commandLine.getOptionValue(ExhibitorCLI.LOGLINES, "1000"));
        int configCheckMs = Integer.parseInt(commandLine.getOptionValue(ExhibitorCLI.CONFIGCHECKMS, "30000"));
        String useHostname = commandLine.getOptionValue(ExhibitorCLI.HOSTNAME, cli.getHostname());
        int httpPort = Integer.parseInt(commandLine.getOptionValue(ExhibitorCLI.HTTP_PORT, "8080"));
        String extraHeadingText = commandLine.getOptionValue(ExhibitorCLI.EXTRA_HEADING_TEXT, null);
        boolean allowNodeMutations = "true".equalsIgnoreCase(commandLine.getOptionValue(ExhibitorCLI.NODE_MUTATIONS));

        String configType = commandLine.hasOption(ExhibitorCLI.SHORT_CONFIG_TYPE) ? commandLine.getOptionValue(ExhibitorCLI.SHORT_CONFIG_TYPE) : (commandLine.hasOption(ExhibitorCLI.CONFIG_TYPE) ? commandLine.getOptionValue(ExhibitorCLI.CONFIG_TYPE) : null);
        if ( configType == null )
        {
            throw new MissingConfigurationTypeException("Configuration type (-" + ExhibitorCLI.SHORT_CONFIG_TYPE + " or --" + ExhibitorCLI.CONFIG_TYPE + ") must be specified", cli);
        }

        ConfigProvider configProvider = makeConfigProvider(configType, cli, commandLine, awsCredentials, backupProvider, useHostname);
        if ( configProvider == null )
        {
            throw new ExhibitorCreatorExit(cli);
        }
        boolean        isNoneConfigProvider = (configProvider instanceof NoneConfigProvider);
        if ( isNoneConfigProvider )
        {
            backupProvider = null;
        }

        JQueryStyle jQueryStyle;
        try
        {
            jQueryStyle = JQueryStyle.valueOf(commandLine.getOptionValue(ExhibitorCLI.JQUERY_STYLE, "red").toUpperCase());
        }
        catch ( IllegalArgumentException e )
        {
            throw new ExhibitorCreatorExit(cli);
        }

        String realm = commandLine.getOptionValue(ExhibitorCLI.BASIC_AUTH_REALM);
        String user = commandLine.getOptionValue(ExhibitorCLI.CONSOLE_USER);
        String password = commandLine.getOptionValue(ExhibitorCLI.CONSOLE_PASSWORD);
        String curatorUser = commandLine.getOptionValue(ExhibitorCLI.CURATOR_USER);
        String curatorPassword = commandLine.getOptionValue(ExhibitorCLI.CURATOR_PASSWORD);
        SecurityHandler handler = null;
        if ( notNullOrEmpty(realm) && notNullOrEmpty(user) && notNullOrEmpty(password) && notNullOrEmpty(curatorUser) && notNullOrEmpty(curatorPassword) )
        {
            handler = getSecurityHandler(realm, user, password, curatorUser, curatorPassword);
        }

        String      aclId = commandLine.getOptionValue(ExhibitorCLI.ACL_ID);
        String      aclScheme = commandLine.getOptionValue(ExhibitorCLI.ACL_SCHEME);
        String      aclPerms = commandLine.getOptionValue(ExhibitorCLI.ACL_PERMISSIONS);
        ACLProvider aclProvider = null;
        if ( notNullOrEmpty(aclId) || notNullOrEmpty(aclScheme) || notNullOrEmpty(aclPerms) )
        {
            aclProvider = getAclProvider(cli, aclId, aclScheme, aclPerms);
            if ( aclProvider == null )
            {
                throw new ExhibitorCreatorExit(cli);
            }
        }

        this.builder = ExhibitorArguments.builder()
            .connectionTimeOutMs(timeoutMs)
            .logWindowSizeLines(logWindowSizeLines)
            .thisJVMHostname(useHostname)
            .configCheckMs(configCheckMs)
            .extraHeadingText(extraHeadingText)
            .allowNodeMutations(allowNodeMutations)
            .jQueryStyle(jQueryStyle)
            .restPort(httpPort)
            .aclProvider(aclProvider);

        this.securityHandler = handler;
        this.backupProvider = backupProvider;
        this.configProvider = configProvider;
        this.httpPort = httpPort;
    }

    public ExhibitorArguments.Builder getBuilder()
    {
        return builder;
    }

    public int getHttpPort()
    {
        return httpPort;
    }

    public ConfigProvider getConfigProvider()
    {
        return configProvider;
    }

    public SecurityHandler getSecurityHandler()
    {
        return securityHandler;
    }

    public BackupProvider getBackupProvider()
    {
        return backupProvider;
    }

    private ConfigProvider makeConfigProvider(String configType, ExhibitorCLI cli, CommandLine commandLine, PropertyBasedS3Credential awsCredentials, BackupProvider backupProvider, String useHostname) throws Exception
    {
        ConfigProvider      configProvider;
        if ( configType.equals("s3") )
        {
            configProvider = getS3Provider(cli, commandLine, awsCredentials, useHostname);
        }
        else if ( configType.equals("file") )
        {
            configProvider = getFileSystemProvider(commandLine, backupProvider);
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

    private ConfigProvider getNoneProvider(CommandLine commandLine)
    {
        if ( !commandLine.hasOption(ExhibitorCLI.NONE_CONFIG_DIRECTORY) )
        {
            System.err.println(ExhibitorCLI.NONE_CONFIG_DIRECTORY + " is required when configtype is \"none\"");
            return null;
        }

        return new NoneConfigProvider(commandLine.getOptionValue(ExhibitorCLI.NONE_CONFIG_DIRECTORY));
    }

    private ConfigProvider getZookeeperProvider(CommandLine commandLine, String useHostname) throws Exception
    {
        String      connectString = commandLine.getOptionValue(ExhibitorCLI.ZOOKEEPER_CONFIG_INITIAL_CONNECT_STRING);
        String      path = commandLine.getOptionValue(ExhibitorCLI.ZOOKEEPER_CONFIG_BASE_PATH);
        String      retrySpec = commandLine.getOptionValue(ExhibitorCLI.ZOOKEEPER_CONFIG_RETRY, ExhibitorCLI.DEFAULT_ZOOKEEPER_CONFIG_RETRY);
        if ( (path == null) || (connectString == null) )
        {
            System.err.println("Both " + ExhibitorCLI.ZOOKEEPER_CONFIG_INITIAL_CONNECT_STRING + " and " + ExhibitorCLI.ZOOKEEPER_CONFIG_INITIAL_CONNECT_STRING + " are required when the configtype is zookeeper");
            return null;
        }

        try
        {
            PathUtils.validatePath(path);
        }
        catch ( IllegalArgumentException e )
        {
            System.err.println("Invalid " + ExhibitorCLI.ZOOKEEPER_CONFIG_BASE_PATH + ": " + path);
            return null;
        }

        String[]    retryParts = retrySpec.split("\\:");
        if ( retryParts.length != 2 )
        {
            System.err.println("Bad " + ExhibitorCLI.ZOOKEEPER_CONFIG_RETRY + " value: " + retrySpec);
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
            System.err.println("Bad " + ExhibitorCLI.ZOOKEEPER_CONFIG_RETRY + " value: " + retrySpec);
            return null;
        }

        int         pollingMs;
        try
        {
            pollingMs = Integer.parseInt(commandLine.getOptionValue(ExhibitorCLI.ZOOKEEPER_CONFIG_POLLING, ExhibitorCLI.DEFAULT_ZOOKEEPER_CONFIG_POLLING));
        }
        catch ( NumberFormatException e )
        {
            System.err.println("Bad " + ExhibitorCLI.ZOOKEEPER_CONFIG_POLLING + " value: " + commandLine.getOptionValue(ExhibitorCLI.ZOOKEEPER_CONFIG_POLLING, ExhibitorCLI.DEFAULT_ZOOKEEPER_CONFIG_POLLING));
            return null;
        }

        CuratorFramework client = makeCurator(connectString, baseSleepTimeMs, maxRetries, pollingMs);
        if ( client == null )
        {
            return null;
        }

        return new ZookeeperConfigProvider(client, path, new Properties(), useHostname);
    }

    private ACLProvider getAclProvider(ExhibitorCLI cli, String aclId, String aclScheme, String aclPerms) throws ExhibitorCreatorExit
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
                    throw new ExhibitorCreatorExit(cli);
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

        final ACL acl = new ACL(perms, new Id(aclScheme, aclId));
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

    private ConfigProvider getFileSystemProvider(CommandLine commandLine, BackupProvider backupProvider) throws IOException
    {
        File directory = commandLine.hasOption(ExhibitorCLI.FILESYSTEM_CONFIG_DIRECTORY) ? new File(commandLine.getOptionValue(ExhibitorCLI.FILESYSTEM_CONFIG_DIRECTORY)) : new File(System.getProperty("user.dir"));
        String name = commandLine.hasOption(ExhibitorCLI.FILESYSTEM_CONFIG_NAME) ? commandLine.getOptionValue(ExhibitorCLI.FILESYSTEM_CONFIG_NAME) : ExhibitorCLI.DEFAULT_FILESYSTEMCONFIG_NAME;
        String lockPrefix = commandLine.hasOption(ExhibitorCLI.FILESYSTEM_CONFIG_LOCK_PREFIX) ? commandLine.getOptionValue(ExhibitorCLI.FILESYSTEM_CONFIG_LOCK_PREFIX) : ExhibitorCLI.DEFAULT_FILESYSTEMCONFIG_LOCK_PREFIX;
        return new FileSystemConfigProvider(directory, name, DefaultProperties.get(backupProvider), new AutoManageLockArguments(lockPrefix));
    }

    private ConfigProvider getS3Provider(ExhibitorCLI cli, CommandLine commandLine, PropertyBasedS3Credential awsCredentials, String hostname) throws Exception
    {
        String  prefix = cli.getOptions().hasOption(ExhibitorCLI.S3_CONFIG_PREFIX) ? commandLine.getOptionValue(ExhibitorCLI.S3_CONFIG_PREFIX) : ExhibitorCLI.DEFAULT_PREFIX;
        return new S3ConfigProvider(new S3ClientFactoryImpl(), awsCredentials, getS3Arguments(cli, commandLine.getOptionValue(ExhibitorCLI.S3_CONFIG), prefix), hostname);
    }

    private void checkMutuallyExclusive(ExhibitorCLI cli, CommandLine commandLine, String option1, String option2) throws ExhibitorCreatorExit
    {
        if ( commandLine.hasOption(option1) && commandLine.hasOption(option2) )
        {
            System.err.println(option1 + " and " + option2 + " cannot be used at the same time");
            throw new ExhibitorCreatorExit(cli);
        }
    }

    private S3ConfigArguments getS3Arguments(ExhibitorCLI cli, String value, String prefix) throws ExhibitorCreatorExit
    {
        String[]        parts = value.split(":");
        if ( parts.length != 2 )
        {
            System.err.println("Bad s3config argument: " + value);
            throw new ExhibitorCreatorExit(cli);
        }
        return new S3ConfigArguments(parts[0].trim(), parts[1].trim(), new S3ConfigAutoManageLockArguments(prefix + "-lock-"));
    }

    private CuratorFramework makeCurator(final String connectString, int baseSleepTimeMs, int maxRetries, int pollingMs)
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

        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries);
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

    private boolean notNullOrEmpty(String arg)
    {
        return arg != null && (! "".equals(arg));
    }

    private SecurityHandler getSecurityHandler(String realm, String consoleUser, String consolePassword, String curatorUser, String curatorPassword)
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
