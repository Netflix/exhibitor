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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.io.Closeables;
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
import com.netflix.exhibitor.core.config.s3.S3ConfigArguments;
import com.netflix.exhibitor.core.config.s3.S3ConfigAutoManageLockArguments;
import com.netflix.exhibitor.core.config.s3.S3ConfigProvider;
import com.netflix.exhibitor.core.rest.UIContext;
import com.netflix.exhibitor.core.rest.jersey.JerseySupport;
import com.netflix.exhibitor.core.s3.PropertyBasedS3Credential;
import com.netflix.exhibitor.core.s3.S3ClientFactoryImpl;
import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExhibitorMain implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Server server;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final Exhibitor exhibitor;
    private final AtomicBoolean shutdownSignaled = new AtomicBoolean(false);

    private static final String FILESYSTEMCONFIG_DIRECTORY = "fsconfigdir";
    private static final String FILESYSTEMCONFIG_NAME = "fsconfigname";
    private static final String FILESYSTEMCONFIG_PREFIX = "fsconfigprefix";
    private static final String FILESYSTEMCONFIG_LOCK_PREFIX = "fsconfiglockprefix";
    private static final String S3_CREDENTIALS = "s3credentials";
    private static final String S3_BACKUP = "s3backup";
    private static final String S3_CONFIG = "s3config";
    private static final String S3_CONFIG_PREFIX = "s3configprefix";
    private static final String FILESYSTEMBACKUP = "filesystembackup";
    private static final String TIMEOUT = "timeout";
    private static final String LOGLINES = "loglines";
    private static final String HOSTNAME = "hostname";
    private static final String CONFIGCHECKMS = "configcheckms";
    private static final String HELP = "help";
    private static final String ALT_HELP = "?";
    private static final String HTTP_PORT = "port";
    private static final String EXTRA_HEADING_TEXT = "headingtext";
    private static final String NODE_MUTATIONS = "nodemodification";
    private static final String JQUERY_STYLE = "jquerystyle";

    private static final String DEFAULT_FILESYSTEMCONFIG_NAME = "exhibitor.properties";
    private static final String DEFAULT_FILESYSTEMCONFIG_PREFIX = "exhibitor-";
    private static final String DEFAULT_FILESYSTEMCONFIG_LOCK_PREFIX = "exhibitor-lock-";

    public static void main(String[] args) throws Exception
    {
        String      hostname = Exhibitor.getHostname();

        Options     options  = new Options();
        options.addOption(null, FILESYSTEMCONFIG_DIRECTORY, true, "Directory to store Exhibitor properties (cannot be used with s3config). Exhibitor uses file system locks so you can specify a shared location so as to enable complete ensemble management. Default location is " + System.getProperty("user.dir"));
        options.addOption(null, FILESYSTEMCONFIG_NAME, true, "The name of the file to store config in. Used in conjunction with " + FILESYSTEMCONFIG_DIRECTORY + ". Default is " + DEFAULT_FILESYSTEMCONFIG_NAME);
        options.addOption(null, FILESYSTEMCONFIG_PREFIX, true, "A prefix for various config values such as heartbeats. Used in conjunction with " + FILESYSTEMCONFIG_DIRECTORY + ". Default is " + DEFAULT_FILESYSTEMCONFIG_PREFIX);
        options.addOption(null, FILESYSTEMCONFIG_LOCK_PREFIX, true, "A prefix for a locking mechanism. Used in conjunction with " + FILESYSTEMCONFIG_DIRECTORY + ". Default is " + DEFAULT_FILESYSTEMCONFIG_LOCK_PREFIX);
        options.addOption(null, S3_CREDENTIALS, true, "Required if you use s3backup or s3config. Argument is the path to an AWS credential properties file with two properties: " + PropertyBasedS3Credential.PROPERTY_S3_KEY_ID + " and " + PropertyBasedS3Credential.PROPERTY_S3_SECRET_KEY);
        options.addOption(null, S3_BACKUP, true, "If true, enables AWS S3 backup of ZooKeeper log files (s3credentials must be provided as well).");
        options.addOption(null, S3_CONFIG, true, "Enables AWS S3 shared config files as opposed to file system config files (s3credentials must be provided as well). Argument is [bucket name]:[key].");
        options.addOption(null, S3_CONFIG_PREFIX, true, "When using AWS S3 shared config files, the prefix to use for values such as heartbeats. Default is " + DEFAULT_FILESYSTEMCONFIG_PREFIX);
        options.addOption(null, FILESYSTEMBACKUP, true, "If true, enables file system backup of ZooKeeper log files.");
        options.addOption(null, TIMEOUT, true, "Connection timeout (ms) for ZK connections. Default is 30000.");
        options.addOption(null, LOGLINES, true, "Max lines of logging to keep in memory for display. Default is 1000.");
        options.addOption(null, HOSTNAME, true, "Hostname to use for this JVM. Default is: " + hostname);
        options.addOption(null, CONFIGCHECKMS, true, "Period (ms) to check config file. Default is: 30000");
        options.addOption(null, HTTP_PORT, true, "Port for the HTTP Server. Default is: 8080");
        options.addOption(null, EXTRA_HEADING_TEXT, true, "Extra text to display in UI header");
        options.addOption(null, NODE_MUTATIONS, true, "If true, the Explorer UI will allow nodes to be modified (use with caution).");
        options.addOption(null, JQUERY_STYLE, true, "Styling used for the JQuery-based UI. Currently available options: " + getStyleOptions());
        options.addOption(ALT_HELP, HELP, false, "Print this help");

        CommandLine         commandLine;
        try
        {
            CommandLineParser   parser = new PosixParser();
            commandLine = parser.parse(options, args);
            if ( commandLine.hasOption('?') || commandLine.hasOption(HELP) || (commandLine.getArgList().size() > 0) )
            {
                printHelp(options);
                return;
            }
        }
        catch ( ParseException e )
        {
            printHelp(options);
            return;
        }

        if ( !checkMutuallyExclusive(options, commandLine, S3_BACKUP, FILESYSTEMBACKUP) )
        {
            return;
        }
        if ( !checkMutuallyExclusive(options, commandLine, S3_CONFIG, FILESYSTEMCONFIG_DIRECTORY) )
        {
            return;
        }

        PropertyBasedS3Credential   awsCredentials = null;
        if ( commandLine.hasOption(S3_CREDENTIALS) )
        {
            awsCredentials = new PropertyBasedS3Credential(new File(commandLine.getOptionValue(S3_CREDENTIALS)));
        }

        BackupProvider      backupProvider = null;
        if ( "true".equalsIgnoreCase(commandLine.getOptionValue(S3_BACKUP)) )
        {
            if ( awsCredentials == null )
            {
                System.err.println("s3backup not specified");
                printHelp(options);
                return;
            }
            backupProvider = new S3BackupProvider(new S3ClientFactoryImpl(), awsCredentials);
        }
        else if ( "true".equalsIgnoreCase(commandLine.getOptionValue(FILESYSTEMBACKUP)) )
        {
            backupProvider = new FileSystemBackupProvider();
        }

        int         timeoutMs = Integer.parseInt(commandLine.getOptionValue(TIMEOUT, "30000"));
        int         logWindowSizeLines = Integer.parseInt(commandLine.getOptionValue(LOGLINES, "1000"));
        int         configCheckMs = Integer.parseInt(commandLine.getOptionValue(CONFIGCHECKMS, "30000"));
        String      useHostname = commandLine.getOptionValue(HOSTNAME, hostname);
        int         httpPort = Integer.parseInt(commandLine.getOptionValue(HTTP_PORT, "8080"));
        String      extraHeadingText = commandLine.getOptionValue(EXTRA_HEADING_TEXT, null);
        boolean     allowNodeMutations = "true".equalsIgnoreCase(commandLine.getOptionValue(NODE_MUTATIONS));

        ConfigProvider      provider;
        if ( commandLine.hasOption(S3_CONFIG) )
        {
            provider = getS3Provider(options, commandLine, awsCredentials, useHostname);
        }
        else
        {
            provider = getFileSystemProvider(commandLine, backupProvider);
        }
        if ( provider == null )
        {
            printHelp(options);
            return;
        }
        
        JQueryStyle jQueryStyle;
        try
        {
            jQueryStyle = JQueryStyle.valueOf(commandLine.getOptionValue(JQUERY_STYLE, "red").toUpperCase());
        }
        catch ( IllegalArgumentException e )
        {
            printHelp(options);
            return;
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
        ;

        ExhibitorMain exhibitorMain = new ExhibitorMain(backupProvider, provider, builder, httpPort);
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

    private static String getStyleOptions()
    {
        Iterable<String> transformed = Iterables.transform
        (
            Arrays.asList(JQueryStyle.values()),
            new Function<JQueryStyle, String>()
            {
                @Override
                public String apply(JQueryStyle style)
                {
                    return style.name().toLowerCase();
                }
            }
        );
        return Joiner.on(", ").join(transformed);
    }

    private static ConfigProvider getFileSystemProvider(CommandLine commandLine, BackupProvider backupProvider) throws IOException
    {
        File directory = commandLine.hasOption(FILESYSTEMCONFIG_DIRECTORY) ? new File(commandLine.getOptionValue(FILESYSTEMCONFIG_DIRECTORY)) : new File(System.getProperty("user.dir"));
        String name = commandLine.hasOption(FILESYSTEMCONFIG_NAME) ? commandLine.getOptionValue(FILESYSTEMCONFIG_NAME) : DEFAULT_FILESYSTEMCONFIG_NAME;
        String prefix = commandLine.hasOption(FILESYSTEMCONFIG_PREFIX) ? commandLine.getOptionValue(FILESYSTEMCONFIG_PREFIX) : DEFAULT_FILESYSTEMCONFIG_PREFIX;
        String lockPrefix = commandLine.hasOption(FILESYSTEMCONFIG_LOCK_PREFIX) ? commandLine.getOptionValue(FILESYSTEMCONFIG_LOCK_PREFIX) : DEFAULT_FILESYSTEMCONFIG_LOCK_PREFIX;
        return new FileSystemConfigProvider(directory, name, prefix, DefaultProperties.get(backupProvider), new AutoManageLockArguments(lockPrefix));
    }

    private static ConfigProvider getS3Provider(Options options, CommandLine commandLine, PropertyBasedS3Credential awsCredentials, String hostname) throws Exception
    {
        ConfigProvider provider;
        if ( awsCredentials == null )
        {
            System.err.println("s3backup not specified");
            provider = null;
        }
        else
        {
            String  prefix = options.hasOption(S3_CONFIG_PREFIX) ? commandLine.getOptionValue(S3_CONFIG_PREFIX) : DEFAULT_FILESYSTEMCONFIG_PREFIX;
            provider = new S3ConfigProvider(new S3ClientFactoryImpl(), awsCredentials, getS3Arguments(commandLine.getOptionValue(S3_CONFIG), options, prefix), hostname);
        }
        return provider;
    }

    private static boolean checkMutuallyExclusive(Options options, CommandLine commandLine, String option1, String option2)
    {
        if ( commandLine.hasOption(option1) && commandLine.hasOption(option2) )
        {
            System.err.println(option1 + " and " + option2 + " cannot be used at the same time");
            printHelp(options);
            return false;
        }
        return true;
    }

    private static S3ConfigArguments getS3Arguments(String value, Options options, String prefix)
    {
        String[]        parts = value.split(":");
        if ( parts.length != 2 )
        {
            System.err.println("Bad s3config argument: " + value);
            printHelp(options);
            return null;
        }
        return new S3ConfigArguments(parts[0].trim(), parts[1].trim(), prefix, new S3ConfigAutoManageLockArguments(prefix + "-lock-"));
    }

    public ExhibitorMain(BackupProvider backupProvider, ConfigProvider configProvider, ExhibitorArguments.Builder builder, int httpPort) throws Exception
    {
        builder.shutdownProc(makeShutdownProc(this));
        exhibitor = new Exhibitor(configProvider, null, backupProvider, builder.build());
        exhibitor.start();

        DefaultResourceConfig   application = JerseySupport.newApplicationConfig(new UIContext(exhibitor));
        ServletContainer        container = new ServletContainer(application);
        server = new Server(httpPort);
        Context root = new Context(server, "/", Context.SESSIONS);
        root.addServlet(new ServletHolder(container), "/*");
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

    private static void printHelp(Options options)
    {
        HelpFormatter       formatter = new HelpFormatter();
        formatter.printHelp("ExhibitorMain", options);
        System.exit(0);
    }
}
