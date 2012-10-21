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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.config.JQueryStyle;
import com.netflix.exhibitor.core.s3.PropertyBasedS3Credential;
import com.netflix.exhibitor.core.state.ManifestVersion;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

public class ExhibitorCLI
{
    private final Logger    log = LoggerFactory.getLogger(getClass());

    private final Options options;
    private final String hostname;
    private final Options generalOptions;
    private final List<OptionSection> sections = Lists.newArrayList();

    private static class OptionSection
    {
        private final String sectionName;
        private final Options options;

        private OptionSection(String sectionName, Options options)
        {
            this.sectionName = sectionName;
            this.options = options;
        }
    }

    public static final String CONFIG_TYPE = "configtype";
    public static final String SHORT_CONFIG_TYPE = "c";

    public static final String FILESYSTEM_CONFIG_DIRECTORY = "fsconfigdir";
    public static final String FILESYSTEM_CONFIG_NAME = "fsconfigname";
    public static final String FILESYSTEM_CONFIG_LOCK_PREFIX = "fsconfiglockprefix";
    public static final String S3_CREDENTIALS = "s3credentials";
    public static final String S3_BACKUP = "s3backup";
    public static final String S3_CONFIG = "s3config";
    public static final String S3_CONFIG_PREFIX = "s3configprefix";
    public static final String ZOOKEEPER_CONFIG_INITIAL_CONNECT_STRING = "zkconfigconnect";
    public static final String ZOOKEEPER_CONFIG_BASE_PATH = "zkconfigzpath";
    public static final String ZOOKEEPER_CONFIG_RETRY = "zkconfigretry";
    public static final String ZOOKEEPER_CONFIG_POLLING = "zkconfigpollms";
    public static final String NONE_CONFIG_DIRECTORY = "noneconfigdir";

    public static final String FILESYSTEMBACKUP = "filesystembackup";
    public static final String TIMEOUT = "timeout";
    public static final String LOGLINES = "loglines";
    public static final String HOSTNAME = "hostname";
    public static final String CONFIGCHECKMS = "configcheckms";
    public static final String HELP = "help";
    public static final String ALT_HELP = "?";
    public static final String HTTP_PORT = "port";
    public static final String EXTRA_HEADING_TEXT = "headingtext";
    public static final String NODE_MUTATIONS = "nodemodification";
    public static final String JQUERY_STYLE = "jquerystyle";
    public static final String BASIC_AUTH_REALM = "basicauthrealm";
    public static final String CONSOLE_USER = "consoleuser";
    public static final String CURATOR_USER = "curatoruser";
    public static final String CONSOLE_PASSWORD = "consolepassword";
    public static final String CURATOR_PASSWORD = "curatorpassword";
    public static final String ACL_SCHEME = "aclscheme";
    public static final String ACL_ID = "aclid";
    public static final String ACL_PERMISSIONS = "aclperms";
    public static final String SERVO_INTEGRATION = "servo";

    public static final String DEFAULT_FILESYSTEMCONFIG_NAME = "exhibitor.properties";
    public static final String DEFAULT_PREFIX = "exhibitor-";
    public static final String DEFAULT_FILESYSTEMCONFIG_LOCK_PREFIX = "exhibitor-lock-";
    public static final String DEFAULT_ZOOKEEPER_CONFIG_RETRY = "1000:3";
    public static final String DEFAULT_ZOOKEEPER_CONFIG_POLLING = "10000";

    public ExhibitorCLI()
    {
        hostname = Exhibitor.getHostname();

        Options authOptions = new Options();
        authOptions.addOption(null, BASIC_AUTH_REALM, true, "Basic Auth Realm to Protect the Exhibitor UI");
        authOptions.addOption(null, CONSOLE_USER, true, "Basic Auth Username to Protect the Exhibitor UI");
        authOptions.addOption(null, CONSOLE_PASSWORD, true, "Basic Auth Password to Protect the Exhibitor UI");
        authOptions.addOption(null, CURATOR_USER, true, "Basic Auth Password to Protect the cluster list api");
        authOptions.addOption(null, CURATOR_PASSWORD, true, "Basic Auth Password to Protect cluster list api");

        Options fileConfigOptions = new Options();
        fileConfigOptions.addOption(null, FILESYSTEM_CONFIG_DIRECTORY, true, "Directory to store Exhibitor properties (cannot be used with s3config). Exhibitor uses file system locks so you can specify a shared location so as to enable complete ensemble management. Default location is " + System.getProperty("user.dir"));
        fileConfigOptions.addOption(null, FILESYSTEM_CONFIG_NAME, true, "The name of the file to store config in. Used in conjunction with " + FILESYSTEM_CONFIG_DIRECTORY + ". Default is " + DEFAULT_FILESYSTEMCONFIG_NAME);
        fileConfigOptions.addOption(null, FILESYSTEM_CONFIG_LOCK_PREFIX, true, "A prefix for a locking mechanism. Used in conjunction with " + FILESYSTEM_CONFIG_DIRECTORY + ". Default is " + DEFAULT_FILESYSTEMCONFIG_LOCK_PREFIX);

        Options s3ConfigOptions = new Options();
        s3ConfigOptions.addOption(null, S3_CONFIG, true, "The bucket name and key to store the config (s3credentials may be provided as well). Argument is [bucket name]:[key].");
        s3ConfigOptions.addOption(null, S3_CONFIG_PREFIX, true, "When using AWS S3 shared config files, the prefix to use for values such as locks. Default is " + DEFAULT_PREFIX);

        Options zookeeperConfigOptions = new Options();
        zookeeperConfigOptions.addOption(null, ZOOKEEPER_CONFIG_INITIAL_CONNECT_STRING, true, "The initial connection string for ZooKeeper shared config storage. E.g: \"host1:2181,host2:2181...\"");
        zookeeperConfigOptions.addOption(null, ZOOKEEPER_CONFIG_BASE_PATH, true, "The base ZPath that Exhibitor should use. E.g: \"/exhibitor/config\"");
        zookeeperConfigOptions.addOption(null, ZOOKEEPER_CONFIG_RETRY, true, "The retry values to use in the form sleep-ms:retry-qty. The default is: " + DEFAULT_ZOOKEEPER_CONFIG_RETRY);
        zookeeperConfigOptions.addOption(null, ZOOKEEPER_CONFIG_POLLING, true, "The period in ms to check for changes in the config ensemble. The default is: " + DEFAULT_ZOOKEEPER_CONFIG_POLLING);

        Options noneConfigOptions = new Options();
        noneConfigOptions.addOption(null, NONE_CONFIG_DIRECTORY, true, "Directory to store the local configuration file. Config type \"none\" is a special purpose type that should only be used when running a second ZooKeeper ensemble that is used for storing config. DO NOT USE THIS MODE for a normal ZooKeeper ensemble.");

        Options backupOptions = new Options();
        backupOptions.addOption(null, S3_BACKUP, true, "If true, enables AWS S3 backup of ZooKeeper log files (s3credentials may be provided as well).");
        backupOptions.addOption(null, FILESYSTEMBACKUP, true, "If true, enables file system backup of ZooKeeper log files.");

        Options s3Options = new Options();
        s3Options.addOption(null, S3_CREDENTIALS, true, "Optional credentials to use for s3backup or s3config. Argument is the path to an AWS credential properties file with two properties: " + PropertyBasedS3Credential.PROPERTY_S3_KEY_ID + " and " + PropertyBasedS3Credential.PROPERTY_S3_SECRET_KEY);

        generalOptions = new Options();
        generalOptions.addOption(null, TIMEOUT, true, "Connection timeout (ms) for ZK connections. Default is 30000.");
        generalOptions.addOption(null, LOGLINES, true, "Max lines of logging to keep in memory for display. Default is 1000.");
        generalOptions.addOption(null, HOSTNAME, true, "Hostname to use for this JVM. Default is: " + hostname);
        generalOptions.addOption(null, HTTP_PORT, true, "Port for the HTTP Server. Default is: 8080");
        generalOptions.addOption(null, EXTRA_HEADING_TEXT, true, "Extra text to display in UI header");
        generalOptions.addOption(null, NODE_MUTATIONS, true, "If true, the Explorer UI will allow nodes to be modified (use with caution).");
        generalOptions.addOption(null, JQUERY_STYLE, true, "Styling used for the JQuery-based UI. Currently available options: " + getStyleOptions());
        generalOptions.addOption(ALT_HELP, HELP, false, "Print this help");
        generalOptions.addOption(SHORT_CONFIG_TYPE, CONFIG_TYPE, true, "Defines which configuration type you want to use. Choices are: \"file\", \"s3\", \"zookeeper\" or \"none\". Additional config will be required depending on which type you are using.");
        generalOptions.addOption(null, CONFIGCHECKMS, true, "Period (ms) to check for shared config updates. Default is: 30000");
        generalOptions.addOption(null, SERVO_INTEGRATION, true, "true/false (default is false). If enabled, ZooKeeper will be queried once a minute for its state via the 'mntr' four letter word (this requires ZooKeeper 3.4.x+). Servo will be used to publish this data via JMX.");

        Options aclOptions = new Options();
        aclOptions.addOption(null, ACL_ID, true, "Enable ACL for Exhibitor's internal ZooKeeper connection. This sets the ACL's ID.");
        aclOptions.addOption(null, ACL_SCHEME, true, "Enable ACL for Exhibitor's internal ZooKeeper connection. This sets the ACL's Scheme.");
        aclOptions.addOption(null, ACL_PERMISSIONS, true, "Enable ACL for Exhibitor's internal ZooKeeper connection. This sets the ACL's Permissions - a comma list of possible permissions. If this isn't specified the permission is set to ALL. Values: read, write, create, delete, admin");

        options = new Options();
        addAll("S3 Options", s3Options);
        addAll("Configuration Options for Type \"s3\"", s3ConfigOptions);
        addAll("Configuration Options for Type \"zookeeper\"", zookeeperConfigOptions);
        addAll("Configuration Options for Type \"file\"", fileConfigOptions);
        addAll("Configuration Options for Type \"none\"", noneConfigOptions);
        addAll("Backup Options", backupOptions);
        addAll("Authorization Options", authOptions);
        addAll("ACL Options", aclOptions);
        addAll(null, generalOptions);
    }

    public Options getOptions()
    {
        return options;
    }

    public String getHostname()
    {
        return hostname;
    }

    public void logHelp(String prefix)
    {
        ManifestVersion     manifestVersion = new ManifestVersion();

        log.info("Exhibitor properties (version: " + manifestVersion.getVersion() + ")");
        logOptions(null, prefix, generalOptions);
        for ( OptionSection section : sections )
        {
            logOptions(section.sectionName, prefix, section.options);
        }
    }

    public void printHelp()
    {
        ManifestVersion     manifestVersion = new ManifestVersion();
        System.out.println("Exhibitor " + manifestVersion.getVersion());
        HelpFormatter formatter = new HelpFormatter()
        {
            @Override
            public void printUsage(PrintWriter pw, int width, String cmdLineSyntax)
            {
            }

            @Override
            public void printUsage(PrintWriter pw, int width, String app, Options options)
            {
            }
        };
        formatter.printHelp("ExhibitorMain", generalOptions);
        for ( OptionSection section : sections )
        {
            formatter.printHelp(" ", "\n== " + section.sectionName + " ==", section.options, null);
        }
    }

    private void logOptions(String sectionName, String prefix, Options options)
    {
        if ( sectionName != null )
        {
            log.info("== " + sectionName + " ==");
        }

        //noinspection unchecked
        for ( Option option : (Iterable<? extends Option>)options.getOptions() )
        {
            if ( option.hasLongOpt() )
            {
                if ( option.hasArg() )
                {
                    log.info(prefix + option.getLongOpt() + " <arg> - " + option.getDescription());
                }
                else
                {
                    log.info(prefix + option.getLongOpt() + " - " + option.getDescription());
                }
            }
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

    private void addAll(String sectionName, Options adding)
    {
        //noinspection unchecked
        for ( Option o : (Iterable<? extends Option>)adding.getOptions() )
        {
            options.addOption(o);
        }

        if ( sectionName != null )
        {
            sections.add(new OptionSection(sectionName, adding));
        }
    }
}
