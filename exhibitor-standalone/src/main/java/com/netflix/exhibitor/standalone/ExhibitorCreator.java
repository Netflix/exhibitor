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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.netflix.exhibitor.core.ExhibitorArguments;
import com.netflix.exhibitor.core.backup.BackupProvider;
import com.netflix.exhibitor.core.backup.filesystem.FileSystemBackupProvider;
import com.netflix.exhibitor.core.backup.s3.S3BackupProvider;
import com.netflix.exhibitor.core.config.AutoManageLockArguments;
import com.netflix.exhibitor.core.config.ConfigProvider;
import com.netflix.exhibitor.core.config.DefaultProperties;
import com.netflix.exhibitor.core.config.IntConfigs;
import com.netflix.exhibitor.core.config.JQueryStyle;
import com.netflix.exhibitor.core.config.PropertyBasedInstanceConfig;
import com.netflix.exhibitor.core.config.StringConfigs;
import com.netflix.exhibitor.core.config.consul.ConsulConfigProvider;
import com.netflix.exhibitor.core.config.filesystem.FileSystemConfigProvider;
import com.netflix.exhibitor.core.config.none.NoneConfigProvider;
import com.netflix.exhibitor.core.config.s3.S3ConfigArguments;
import com.netflix.exhibitor.core.config.s3.S3ConfigAutoManageLockArguments;
import com.netflix.exhibitor.core.config.s3.S3ConfigProvider;
import com.netflix.exhibitor.core.config.zookeeper.ZookeeperConfigProvider;
import com.netflix.exhibitor.core.s3.PropertyBasedS3ClientConfig;
import com.netflix.exhibitor.core.s3.PropertyBasedS3Credential;
import com.netflix.exhibitor.core.s3.S3ClientFactoryImpl;
import com.netflix.exhibitor.core.servo.ServoRegistration;
import com.netflix.servo.jmx.JmxMonitorRegistry;
import com.orbitz.consul.Consul;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.curator.ensemble.exhibitor.DefaultExhibitorRestClient;
import org.apache.curator.ensemble.exhibitor.ExhibitorEnsembleProvider;
import org.apache.curator.ensemble.exhibitor.Exhibitors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static com.netflix.exhibitor.standalone.ExhibitorCLI.*;

public class ExhibitorCreator
{
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ExhibitorArguments.Builder builder;
    private final SecurityHandler securityHandler;
    private final BackupProvider backupProvider;
    private final ConfigProvider configProvider;
    private final int httpPort;
    private final String listenAddress;
    private final List<Closeable> closeables = Lists.newArrayList();
    private final String securityFile;
    private final String realmSpec;
    private final String remoteAuthSpec;

    public ExhibitorCreator(String[] args) throws Exception
    {
        ExhibitorCLI        cli = new ExhibitorCLI();

        CommandLine commandLine;
        try
        {
            CommandLineParser parser = new PosixParser();
            commandLine = parser.parse(cli.getOptions(), args);
            if ( commandLine.hasOption('?') || commandLine.hasOption(HELP) || (commandLine.getArgList().size() > 0) )
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

        checkMutuallyExclusive(cli, commandLine, S3_BACKUP, FILESYSTEMBACKUP);

        String                        s3Region = commandLine.getOptionValue(S3_REGION, null);
        PropertyBasedS3Credential     awsCredentials = null;
        PropertyBasedS3ClientConfig   awsClientConfig = null;
        if ( commandLine.hasOption(S3_CREDENTIALS) )
        {
            awsCredentials = new PropertyBasedS3Credential(new File(commandLine.getOptionValue(S3_CREDENTIALS)));
        }

        if ( commandLine.hasOption(S3_PROXY) )
        {
            awsClientConfig = new PropertyBasedS3ClientConfig(new File(commandLine.getOptionValue(S3_PROXY)));
        }

        BackupProvider backupProvider = null;
        if ( "true".equalsIgnoreCase(commandLine.getOptionValue(S3_BACKUP)) )
        {
            backupProvider = new S3BackupProvider(new S3ClientFactoryImpl(), awsCredentials, awsClientConfig, s3Region);
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
        String listenAddress = commandLine.getOptionValue(LISTEN_ADDRESS, "0.0.0.0");
        String extraHeadingText = commandLine.getOptionValue(EXTRA_HEADING_TEXT, null);
        boolean allowNodeMutations = "true".equalsIgnoreCase(commandLine.getOptionValue(NODE_MUTATIONS, "true"));

        String configType = commandLine.hasOption(SHORT_CONFIG_TYPE) ? commandLine.getOptionValue(SHORT_CONFIG_TYPE) : (commandLine.hasOption(CONFIG_TYPE) ? commandLine.getOptionValue(CONFIG_TYPE) : null);
        if ( configType == null )
        {
            throw new MissingConfigurationTypeException("Configuration type (-" + SHORT_CONFIG_TYPE + " or --" + CONFIG_TYPE + ") must be specified", cli);
        }

        ConfigProvider configProvider = makeConfigProvider(configType, cli, commandLine, awsCredentials, awsClientConfig, backupProvider, useHostname, s3Region);
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
            jQueryStyle = JQueryStyle.valueOf(commandLine.getOptionValue(JQUERY_STYLE, "red").toUpperCase());
        }
        catch ( IllegalArgumentException e )
        {
            throw new ExhibitorCreatorExit(cli);
        }

        securityFile = commandLine.getOptionValue(SECURITY_FILE);
        realmSpec = commandLine.getOptionValue(REALM);
        remoteAuthSpec = commandLine.getOptionValue(REMOTE_CLIENT_AUTHORIZATION);

        String realm = commandLine.getOptionValue(BASIC_AUTH_REALM);
        String user = commandLine.getOptionValue(CONSOLE_USER);
        String password = commandLine.getOptionValue(CONSOLE_PASSWORD);
        String curatorUser = commandLine.getOptionValue(CURATOR_USER);
        String curatorPassword = commandLine.getOptionValue(CURATOR_PASSWORD);
        SecurityHandler handler = null;
        if ( notNullOrEmpty(realm) && notNullOrEmpty(user) && notNullOrEmpty(password) && notNullOrEmpty(curatorUser) && notNullOrEmpty(curatorPassword) )
        {
            log.warn(Joiner.on(", ").join(BASIC_AUTH_REALM, CONSOLE_USER, CONSOLE_PASSWORD, CURATOR_USER, CURATOR_PASSWORD) + " - have been deprecated. Use TBD instead");
            handler = makeSecurityHandler(realm, user, password, curatorUser, curatorPassword);
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
                throw new ExhibitorCreatorExit(cli);
            }
        }

        ServoRegistration   servoRegistration = null;
        if ( "true".equalsIgnoreCase(commandLine.getOptionValue(SERVO_INTEGRATION, "false")) )
        {
            servoRegistration = new ServoRegistration(new JmxMonitorRegistry("exhibitor"), 60000);
        }

        String              preferencesPath = commandLine.getOptionValue(PREFERENCES_PATH);

        this.builder = ExhibitorArguments.builder()
            .connectionTimeOutMs(timeoutMs)
            .logWindowSizeLines(logWindowSizeLines)
            .thisJVMHostname(useHostname)
            .configCheckMs(configCheckMs)
            .extraHeadingText(extraHeadingText)
            .allowNodeMutations(allowNodeMutations)
            .jQueryStyle(jQueryStyle)
            .restPort(httpPort)
            .aclProvider(aclProvider)
            .servoRegistration(servoRegistration)
            .preferencesPath(preferencesPath)
        ;

        this.securityHandler = handler;
        this.backupProvider = backupProvider;
        this.configProvider = configProvider;
        this.httpPort = httpPort;
        this.listenAddress = listenAddress;
    }

    public ExhibitorArguments.Builder getBuilder()
    {
        return builder;
    }

    public int getHttpPort()
    {
        return httpPort;
    }
    
    public String getListenAddress()
    {
        return listenAddress;
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

    public List<Closeable> getCloseables()
    {
        return closeables;
    }

    public String getSecurityFile()
    {
        return securityFile;
    }

    public String getRealmSpec()
    {
        return realmSpec;
    }

    public String getRemoteAuthSpec()
    {
        return remoteAuthSpec;
    }

    private ConfigProvider makeConfigProvider(String configType, ExhibitorCLI cli, CommandLine commandLine, PropertyBasedS3Credential awsCredentials, PropertyBasedS3ClientConfig awsClientConfig, BackupProvider backupProvider, String useHostname, String s3Region) throws Exception
    {
        Properties          defaultProperties = makeDefaultProperties(commandLine, backupProvider);

        ConfigProvider      configProvider;
        if ( configType.equals("s3") )
        {
            configProvider = getS3Provider(cli, commandLine, awsCredentials, awsClientConfig, useHostname, defaultProperties, s3Region);
        }
        else if ( configType.equals("file") )
        {
            configProvider = getFileSystemProvider(commandLine, defaultProperties);
        }
        else if ( configType.equals("zookeeper") )
        {
            configProvider = getZookeeperProvider(commandLine, useHostname, defaultProperties);
        }
        else if ( configType.equals("consul") )
        {
            configProvider = getConsulProvider(cli, commandLine, defaultProperties);
        }
        else if ( configType.equals("none") )
        {
            log.warn("Warning: you have intentionally turned off shared configuration. This mode is meant for special purposes only. Please verify that this is your intent.");
            configProvider = getNoneProvider(commandLine, defaultProperties);
        }
        else
        {
            configProvider = null;
            log.error("Unknown configtype: " + configType);
        }
        return configProvider;
    }

    private Properties makeDefaultProperties(CommandLine commandLine, BackupProvider backupProvider) throws IOException
    {
        Properties          properties = new Properties();
        properties.putAll(DefaultProperties.get(backupProvider));   // put in standard defaults first

        addInitialConfigFile(commandLine, properties);

        return new PropertyBasedInstanceConfig(properties, new Properties()).getProperties();
    }

    private void addInitialConfigFile(CommandLine commandLine, Properties properties) throws IOException
    {
        Properties          defaultProperties = new Properties();
        String              defaultConfigFile = commandLine.getOptionValue(INITIAL_CONFIG_FILE);
        if ( defaultConfigFile == null )
        {
            return;
        }

        InputStream in = new BufferedInputStream(new FileInputStream(defaultConfigFile));
        try
        {
            defaultProperties.load(in);
        }
        finally
        {
            CloseableUtils.closeQuietly(in);
        }

        Set<String> propertyNames = Sets.newHashSet();
        for ( StringConfigs config : StringConfigs.values() )
        {
            propertyNames.add(PropertyBasedInstanceConfig.toName(config, ""));
        }
        for ( IntConfigs config : IntConfigs.values() )
        {
            propertyNames.add(PropertyBasedInstanceConfig.toName(config, ""));
        }

        for ( String name : defaultProperties.stringPropertyNames() )
        {
            if ( propertyNames.contains(name) )
            {
                String value = defaultProperties.getProperty(name);
                properties.setProperty(PropertyBasedInstanceConfig.ROOT_PROPERTY_PREFIX + name, value);
            }
            else
            {
                log.warn("Ignoring unknown config: " + name);
            }
        }
    }

    private ConfigProvider getNoneProvider(CommandLine commandLine, Properties defaultProperties)
    {
        if ( !commandLine.hasOption(NONE_CONFIG_DIRECTORY) )
        {
            log.error(NONE_CONFIG_DIRECTORY + " is required when configtype is \"none\"");
            return null;
        }

        return new NoneConfigProvider(commandLine.getOptionValue(NONE_CONFIG_DIRECTORY), defaultProperties);
    }

    private ConfigProvider getZookeeperProvider(CommandLine commandLine, String useHostname, Properties defaultProperties) throws Exception
    {
        String      connectString = commandLine.getOptionValue(ZOOKEEPER_CONFIG_INITIAL_CONNECT_STRING);
        String      path = commandLine.getOptionValue(ZOOKEEPER_CONFIG_BASE_PATH);
        String      retrySpec = commandLine.getOptionValue(ZOOKEEPER_CONFIG_RETRY, DEFAULT_ZOOKEEPER_CONFIG_RETRY);
        if ( (path == null) || (connectString == null) )
        {
            log.error("Both " + ZOOKEEPER_CONFIG_INITIAL_CONNECT_STRING + " and " + ZOOKEEPER_CONFIG_BASE_PATH + " are required when the configtype is zookeeper");
            return null;
        }

        try
        {
            PathUtils.validatePath(path);
        }
        catch ( IllegalArgumentException e )
        {
            log.error("Invalid " + ZOOKEEPER_CONFIG_BASE_PATH + ": " + path);
            return null;
        }

        String[]    retryParts = retrySpec.split("\\:");
        if ( retryParts.length != 2 )
        {
            log.error("Bad " + ZOOKEEPER_CONFIG_RETRY + " value: " + retrySpec);
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
            log.error("Bad " + ZOOKEEPER_CONFIG_RETRY + " value: " + retrySpec);
            return null;
        }

        int         exhibitorPort;
        try
        {
            exhibitorPort = commandLine.hasOption(ZOOKEEPER_CONFIG_EXHIBITOR_PORT) ? Integer.parseInt(commandLine.getOptionValue(ZOOKEEPER_CONFIG_EXHIBITOR_PORT)) : 0;
        }
        catch ( NumberFormatException e )
        {
            log.error("Bad " + ZOOKEEPER_CONFIG_EXHIBITOR_PORT + " value: " + commandLine.getOptionValue(ZOOKEEPER_CONFIG_EXHIBITOR_PORT));
            return null;
        }

        int         pollingMs;
        try
        {
            pollingMs = Integer.parseInt(commandLine.getOptionValue(ZOOKEEPER_CONFIG_POLLING, DEFAULT_ZOOKEEPER_CONFIG_POLLING));
        }
        catch ( NumberFormatException e )
        {
            log.error("Bad " + ZOOKEEPER_CONFIG_POLLING + " value: " + commandLine.getOptionValue(ZOOKEEPER_CONFIG_POLLING, DEFAULT_ZOOKEEPER_CONFIG_POLLING));
            return null;
        }

        String              exhibitorRestPath = commandLine.getOptionValue(ZOOKEEPER_CONFIG_EXHIBITOR_URI_PATH, DEFAULT_ZOOKEEPER_CONFIG_EXHIBITOR_URI_PATH);
        CuratorFramework    client = makeCurator(connectString, baseSleepTimeMs, maxRetries, exhibitorPort, exhibitorRestPath, pollingMs);
        if ( client == null )
        {
            return null;
        }

        client.start();
        closeables.add(client);
        return new ZookeeperConfigProvider(client, path, defaultProperties, useHostname);
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
                    log.error("Unknown ACL perm value: " + verb);
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

    private ConfigProvider getFileSystemProvider(CommandLine commandLine, Properties defaultProperties) throws IOException
    {
        File directory = commandLine.hasOption(FILESYSTEM_CONFIG_DIRECTORY) ? new File(commandLine.getOptionValue(FILESYSTEM_CONFIG_DIRECTORY)) : new File(System.getProperty("user.dir"));
        String name = commandLine.hasOption(FILESYSTEM_CONFIG_NAME) ? commandLine.getOptionValue(FILESYSTEM_CONFIG_NAME) : DEFAULT_FILESYSTEMCONFIG_NAME;
        String lockPrefix = commandLine.hasOption(FILESYSTEM_CONFIG_LOCK_PREFIX) ? commandLine.getOptionValue(FILESYSTEM_CONFIG_LOCK_PREFIX) : DEFAULT_FILESYSTEMCONFIG_LOCK_PREFIX;
        return new FileSystemConfigProvider(directory, name, defaultProperties, new AutoManageLockArguments(lockPrefix));
    }

    private ConfigProvider getS3Provider(ExhibitorCLI cli, CommandLine commandLine, PropertyBasedS3Credential awsCredentials, PropertyBasedS3ClientConfig awsClientConfig, String hostname, Properties defaultProperties, String s3Region) throws Exception
    {
        String  prefix = cli.getOptions().hasOption(S3_CONFIG_PREFIX) ? commandLine.getOptionValue(S3_CONFIG_PREFIX) : DEFAULT_PREFIX;
        return new S3ConfigProvider(new S3ClientFactoryImpl(), awsCredentials, awsClientConfig, getS3Arguments(cli, commandLine.getOptionValue(S3_CONFIG), prefix), hostname, defaultProperties, s3Region);
    }

    private ConfigProvider getConsulProvider(ExhibitorCLI cli, CommandLine commandLine, Properties defaultProperties) throws Exception {
        String host = commandLine.getOptionValue(CONSUL_CONFIG_HOST, "localhost");
        Integer port = Integer.valueOf(commandLine.getOptionValue(CONSUL_CONFIG_PORT, "8500"));
        String prefix = commandLine.getOptionValue(CONSUL_CONFIG_KEY_PREFIX, "exhibitor/");

        Boolean sslEnabled = Boolean.valueOf(commandLine.getOptionValue(CONSUL_CONFIG_SSL, "false"));
        String protocol = sslEnabled ? "https" : "http";
        String url = String.format("%s://%s:%d", protocol, host, port);

        Consul.Builder builder = Consul.builder().withUrl(url);
        if (sslEnabled) {
            if (!Boolean.valueOf(commandLine.getOptionValue(CONSUL_CONFIG_SSL_VERIFY_HOSTNAME, "true"))) {
                builder.withHostnameVerifier(new NullHostnameVerifier());
            }

            String sslProtocol = commandLine.getOptionValue(CONSUL_CONFIG_SSL_PROTOCOL, "TLSv1.2");
            String caCertPath = commandLine.getOptionValue(CONSUL_CONFIG_SSL_CA_CERT);
            log.debug("SSL enabled for consul connections; protocol = %s, cacert = %s",
                    sslProtocol, caCertPath);

            // Load cacert file
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            X509Certificate cacert = (X509Certificate) cf.generateCertificate(new FileInputStream(caCertPath));

            KeyStore trustStore = KeyStore.getInstance("JKS");
            trustStore.load(null);
            trustStore.setCertificateEntry("caCert", cacert);

            TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
            tmf.init(trustStore);

            SSLContext sslContext = SSLContext.getInstance(sslProtocol);
            sslContext.init(null, tmf.getTrustManagers(), null);

            builder.withSslContext(sslContext);
        }
        else {
            log.debug("SSL is disabled for consul connections");
        }

        if (commandLine.hasOption(CONSUL_CONFIG_ACL_TOKEN)) {
            builder.withAclToken(commandLine.getOptionValue(CONSUL_CONFIG_ACL_TOKEN));
        }

        Consul consul = builder.build();
        return new ConsulConfigProvider(consul, prefix, defaultProperties);
    }

    private void checkMutuallyExclusive(ExhibitorCLI cli, CommandLine commandLine, String option1, String option2) throws ExhibitorCreatorExit
    {
        if ( commandLine.hasOption(option1) && commandLine.hasOption(option2) )
        {
            log.error(option1 + " and " + option2 + " cannot be used at the same time");
            throw new ExhibitorCreatorExit(cli);
        }
    }

    private S3ConfigArguments getS3Arguments(ExhibitorCLI cli, String value, String prefix) throws ExhibitorCreatorExit
    {
        String[]        parts = value.split(":");
        if ( parts.length != 2 )
        {
            log.error("Bad s3config argument: " + value);
            throw new ExhibitorCreatorExit(cli);
        }
        return new S3ConfigArguments(parts[0].trim(), parts[1].trim(), new S3ConfigAutoManageLockArguments(prefix + "-lock-"));
    }

    private CuratorFramework makeCurator(final String connectString, int baseSleepTimeMs, int maxRetries, int exhibitorPort, String exhibitorRestPath, int pollingMs)
    {
        List<String>    hostnames = Lists.newArrayList();
        String[]        parts = connectString.split(",");
        for ( String spec : parts )
        {
            String[]        subParts = spec.split(":");
            try
            {
                if ( subParts.length != 2 )
                {
                    log.error("Bad connection string: " + connectString);
                    return null;
                }
            }
            catch ( NumberFormatException e )
            {
                log.error("Bad connection string: " + connectString);
                return null;
            }

            hostnames.add(subParts[0]);
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

        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory
            .builder()
            .connectString(connectString)
            .retryPolicy(retryPolicy);
        if ( exhibitorPort > 0 )
        {
            Exhibitors                  exhibitors = new Exhibitors(hostnames, exhibitorPort, backupConnectionStringProvider);
            ExhibitorEnsembleProvider   ensembleProvider = new ExhibitorEnsembleProvider(exhibitors, new DefaultExhibitorRestClient(), exhibitorRestPath + "exhibitor/v1/cluster/list", pollingMs, retryPolicy);
            builder = builder.ensembleProvider(ensembleProvider);
        }
        else
        {
            log.warn("Exhibitor on the shared ZooKeeper config ensemble is not being used.");
        }
        return builder.build();
    }

    private boolean notNullOrEmpty(String arg)
    {
        return arg != null && (! "".equals(arg));
    }

    private SecurityHandler makeSecurityHandler(String realm, String consoleUser, String consolePassword, String curatorUser, String curatorPassword)
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
