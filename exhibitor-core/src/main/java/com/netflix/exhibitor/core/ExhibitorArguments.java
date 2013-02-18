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

package com.netflix.exhibitor.core;

import com.google.common.base.Preconditions;
import com.netflix.curator.framework.api.ACLProvider;
import com.netflix.exhibitor.core.config.JQueryStyle;
import com.netflix.exhibitor.core.servo.ServoRegistration;
import com.sun.jersey.api.client.Client;

public class ExhibitorArguments
{
    final int connectionTimeOutMs;
    final int logWindowSizeLines;
    final int configCheckMs;
    final String extraHeadingText;
    final String thisJVMHostname;
    final boolean allowNodeMutations;
    final JQueryStyle jQueryStyle;
    final int restPort;
    final String restPath;
    final String restScheme;
    final Runnable shutdownProc;
    final LogDirection logDirection;
    final ACLProvider aclProvider;
    final ServoRegistration servoRegistration;
    final String preferencesPath;
    final RemoteConnectionConfiguration remoteConnectionConfiguration;

    public enum LogDirection
    {
        NATURAL,
        INVERTED
    }

    public static class Builder
    {
        private ExhibitorArguments arguments = new ExhibitorArguments();

        /**
         * @param connectionTimeOutMs the connection time to pass use when making internal connections to ZK, etc.
         * @return this
         */
        public Builder connectionTimeOutMs(int connectionTimeOutMs)
        {
            arguments = new ExhibitorArguments(connectionTimeOutMs, arguments.logWindowSizeLines, arguments.configCheckMs, arguments.extraHeadingText, arguments.thisJVMHostname, arguments.allowNodeMutations, arguments.jQueryStyle, arguments.restPort, arguments.restPath, arguments.restScheme, arguments.shutdownProc, arguments.logDirection, arguments.aclProvider, arguments.servoRegistration, arguments.preferencesPath, arguments.remoteConnectionConfiguration);
            return this;
        }

        /**
         * @param logWindowSizeLines max lines for the log
         * @return this
         */
        public Builder logWindowSizeLines(int logWindowSizeLines)
        {
            arguments = new ExhibitorArguments(arguments.connectionTimeOutMs, logWindowSizeLines, arguments.configCheckMs, arguments.extraHeadingText, arguments.thisJVMHostname, arguments.allowNodeMutations, arguments.jQueryStyle, arguments.restPort, arguments.restPath, arguments.restScheme, arguments.shutdownProc, arguments.logDirection, arguments.aclProvider, arguments.servoRegistration, arguments.preferencesPath, arguments.remoteConnectionConfiguration);
            return this;
        }

        /**
         * @param configCheckMs how often to check for shared config changes
         * @return this
         */
        public Builder configCheckMs(int configCheckMs)
        {
            arguments = new ExhibitorArguments(arguments.connectionTimeOutMs, arguments.logWindowSizeLines, configCheckMs, arguments.extraHeadingText, arguments.thisJVMHostname, arguments.allowNodeMutations, arguments.jQueryStyle, arguments.restPort, arguments.restPath, arguments.restScheme, arguments.shutdownProc, arguments.logDirection, arguments.aclProvider, arguments.servoRegistration, arguments.preferencesPath, arguments.remoteConnectionConfiguration);
            return this;
        }

        /**
         * @param extraHeadingText any extra text to display in the web UI
         * @return this
         */
        public Builder extraHeadingText(String extraHeadingText)
        {
            arguments = new ExhibitorArguments(arguments.connectionTimeOutMs, arguments.logWindowSizeLines, arguments.configCheckMs, extraHeadingText, arguments.thisJVMHostname, arguments.allowNodeMutations, arguments.jQueryStyle, arguments.restPort, arguments.restPath, arguments.restScheme, arguments.shutdownProc, arguments.logDirection, arguments.aclProvider, arguments.servoRegistration, arguments.preferencesPath, arguments.remoteConnectionConfiguration);
            return this;
        }

        /**
         * @param thisJVMHostname the hostname of this instance/JVM
         * @return this
         */
        public Builder thisJVMHostname(String thisJVMHostname)
        {
            arguments = new ExhibitorArguments(arguments.connectionTimeOutMs, arguments.logWindowSizeLines, arguments.configCheckMs, arguments.extraHeadingText, thisJVMHostname, arguments.allowNodeMutations, arguments.jQueryStyle, arguments.restPort, arguments.restPath, arguments.restScheme, arguments.shutdownProc, arguments.logDirection, arguments.aclProvider, arguments.servoRegistration, arguments.preferencesPath, arguments.remoteConnectionConfiguration);
            return this;
        }

        /**
         * @param allowNodeMutations if true, the web UI will enable the modification button in the Explorer
         * @return this
         */
        public Builder allowNodeMutations(boolean allowNodeMutations)
        {
            arguments = new ExhibitorArguments(arguments.connectionTimeOutMs, arguments.logWindowSizeLines, arguments.configCheckMs, arguments.extraHeadingText, arguments.thisJVMHostname, allowNodeMutations, arguments.jQueryStyle, arguments.restPort, arguments.restPath, arguments.restScheme, arguments.shutdownProc, arguments.logDirection, arguments.aclProvider, arguments.servoRegistration, arguments.preferencesPath, arguments.remoteConnectionConfiguration);
            return this;
        }

        /**
         * @param jQueryStyle the style to use for the web UI
         * @return this
         */
        public Builder jQueryStyle(JQueryStyle jQueryStyle)
        {
            arguments = new ExhibitorArguments(arguments.connectionTimeOutMs, arguments.logWindowSizeLines, arguments.configCheckMs, arguments.extraHeadingText, arguments.thisJVMHostname, arguments.allowNodeMutations, jQueryStyle, arguments.restPort, arguments.restPath, arguments.restScheme, arguments.shutdownProc, arguments.logDirection, arguments.aclProvider, arguments.servoRegistration, arguments.preferencesPath, arguments.remoteConnectionConfiguration);
            return this;
        }

        /**
         * @param restPort port that Exhibitor REST calls listen on
         * @return this
         */
        public Builder restPort(int restPort)
        {
            arguments = new ExhibitorArguments(arguments.connectionTimeOutMs, arguments.logWindowSizeLines, arguments.configCheckMs, arguments.extraHeadingText, arguments.thisJVMHostname, arguments.allowNodeMutations, arguments.jQueryStyle, restPort, arguments.restPath, arguments.restScheme, arguments.shutdownProc, arguments.logDirection, arguments.aclProvider, arguments.servoRegistration, arguments.preferencesPath, arguments.remoteConnectionConfiguration);
            return this;
        }

        /**
         * @param restPath additional path portion of REST calls
         * @return this
         */
        public Builder restPath(String restPath)
        {
            arguments = new ExhibitorArguments(arguments.connectionTimeOutMs, arguments.logWindowSizeLines, arguments.configCheckMs, arguments.extraHeadingText, arguments.thisJVMHostname, arguments.allowNodeMutations, arguments.jQueryStyle, arguments.restPort, restPath, arguments.restScheme, arguments.shutdownProc, arguments.logDirection, arguments.aclProvider, arguments.servoRegistration, arguments.preferencesPath, arguments.remoteConnectionConfiguration);
            return this;
        }

        /**
         * @param restScheme http or https
         * @return this
         */
        public Builder restScheme(String restScheme)
        {
            arguments = new ExhibitorArguments(arguments.connectionTimeOutMs, arguments.logWindowSizeLines, arguments.configCheckMs, arguments.extraHeadingText, arguments.thisJVMHostname, arguments.allowNodeMutations, arguments.jQueryStyle, arguments.restPort, arguments.restPath, restScheme, arguments.shutdownProc, arguments.logDirection, arguments.aclProvider, arguments.servoRegistration, arguments.preferencesPath, arguments.remoteConnectionConfiguration);
            return this;
        }

        /**
         * @param shutdownProc functor used to shutdown the Exhibitor service
         * @return this
         */
        public Builder shutdownProc(Runnable shutdownProc)
        {
            arguments = new ExhibitorArguments(arguments.connectionTimeOutMs, arguments.logWindowSizeLines, arguments.configCheckMs, arguments.extraHeadingText, arguments.thisJVMHostname, arguments.allowNodeMutations, arguments.jQueryStyle, arguments.restPort, arguments.restPath, arguments.restScheme, shutdownProc, arguments.logDirection, arguments.aclProvider, arguments.servoRegistration, arguments.preferencesPath, arguments.remoteConnectionConfiguration);
            return this;
        }

        /**
         * @param logDirection change the display direction for Exhibitor logs
         * @return this
         */
        public Builder logDirection(LogDirection logDirection)
        {
            logDirection = Preconditions.checkNotNull(logDirection, "logDirection cannot be null");
            arguments = new ExhibitorArguments(arguments.connectionTimeOutMs, arguments.logWindowSizeLines, arguments.configCheckMs, arguments.extraHeadingText, arguments.thisJVMHostname, arguments.allowNodeMutations, arguments.jQueryStyle, arguments.restPort, arguments.restPath, arguments.restScheme, arguments.shutdownProc, logDirection, arguments.aclProvider, arguments.servoRegistration, arguments.preferencesPath, arguments.remoteConnectionConfiguration);
            return this;
        }

        /**
         * If your ZooKeeper cluster has ACL enabled you need to set the ACL in Exhibitor so that it can successfully connect to the cluster
         *
         * @param aclProvider the acl provider to use
         * @return this
         */
        public Builder aclProvider(ACLProvider aclProvider)
        {
            arguments = new ExhibitorArguments(arguments.connectionTimeOutMs, arguments.logWindowSizeLines, arguments.configCheckMs, arguments.extraHeadingText, arguments.thisJVMHostname, arguments.allowNodeMutations, arguments.jQueryStyle, arguments.restPort, arguments.restPath, arguments.restScheme, arguments.shutdownProc, arguments.logDirection, aclProvider, arguments.servoRegistration, arguments.preferencesPath, arguments.remoteConnectionConfiguration);
            return this;
        }

        /**
         * To add Netflix Servo support pass the servo registration information
         *
         * @param servoRegistration servo details
         * @return this
         */
        public Builder servoRegistration(ServoRegistration servoRegistration)
        {
            arguments = new ExhibitorArguments(arguments.connectionTimeOutMs, arguments.logWindowSizeLines, arguments.configCheckMs, arguments.extraHeadingText, arguments.thisJVMHostname, arguments.allowNodeMutations, arguments.jQueryStyle, arguments.restPort, arguments.restPath, arguments.restScheme, arguments.shutdownProc, arguments.logDirection, arguments.aclProvider, servoRegistration, arguments.preferencesPath, arguments.remoteConnectionConfiguration);
            return this;
        }

        /**
         * Certain values (such as Control Panel values) are stored in a preferences file. By default, <code>Preferences.userRoot()</code> is used. Use this
         * method to specify a different file path.
         *
         * @param preferencesPath path for the preferences file
         * @return this
         */
        public Builder preferencesPath(String preferencesPath)
        {
            arguments = new ExhibitorArguments(arguments.connectionTimeOutMs, arguments.logWindowSizeLines, arguments.configCheckMs, arguments.extraHeadingText, arguments.thisJVMHostname, arguments.allowNodeMutations, arguments.jQueryStyle, arguments.restPort, arguments.restPath, arguments.restScheme, arguments.shutdownProc, arguments.logDirection, arguments.aclProvider, arguments.servoRegistration, preferencesPath, arguments.remoteConnectionConfiguration);
            return this;
        }

        /**
         * Exhibitor remotely connects to each of the instances in the ensemble. The RemoteConnectionConfiguration specifies
         * configuration values for the remote client (which uses the Jersey {@link Client})
         *
         * @param remoteConnectionConfiguration remote connection configuration
         * @return this
         */
        public Builder remoteConnectionConfiguration(RemoteConnectionConfiguration remoteConnectionConfiguration)
        {
            arguments = new ExhibitorArguments(arguments.connectionTimeOutMs, arguments.logWindowSizeLines, arguments.configCheckMs, arguments.extraHeadingText, arguments.thisJVMHostname, arguments.allowNodeMutations, arguments.jQueryStyle, arguments.restPort, arguments.restPath, arguments.restScheme, arguments.shutdownProc, arguments.logDirection, arguments.aclProvider, arguments.servoRegistration, arguments.preferencesPath, remoteConnectionConfiguration);
            return this;
        }

        public ExhibitorArguments build()
        {
            Preconditions.checkArgument(arguments.thisJVMHostname != null, "thisJVMHostname cannot be null");
            Preconditions.checkArgument(arguments.connectionTimeOutMs > 0, "connectionTimeOutMs must be a positive number");
            Preconditions.checkArgument(arguments.logWindowSizeLines > 0, "logWindowSizeLines must be a positive number");
            Preconditions.checkArgument(arguments.configCheckMs > 0, "configCheckMs must be a positive number");
            Preconditions.checkArgument(arguments.restPort > 0, "restPort must be a positive number");
            Preconditions.checkArgument(arguments.restPath != null, "restPath cannot be null");
            Preconditions.checkArgument(arguments.remoteConnectionConfiguration != null, "remoteConnectionConfiguration cannot be null");

            return arguments;
        }

        private Builder()
        {
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    private ExhibitorArguments()
    {
        this(30000, 1000, 5000, null, null, false, JQueryStyle.RED, 0, "/", "http", null, LogDirection.INVERTED, null, null, null, new RemoteConnectionConfiguration());
    }

    public ExhibitorArguments(int connectionTimeOutMs, int logWindowSizeLines, int configCheckMs, String extraHeadingText, String thisJVMHostname, boolean allowNodeMutations, JQueryStyle jQueryStyle, int restPort, String restPath, String restScheme, Runnable shutdownProc, LogDirection logDirection, ACLProvider aclProvider, ServoRegistration servoRegistration, String preferencesPath, RemoteConnectionConfiguration remoteConnectionConfiguration)
    {
        this.connectionTimeOutMs = connectionTimeOutMs;
        this.logWindowSizeLines = logWindowSizeLines;
        this.configCheckMs = configCheckMs;
        this.extraHeadingText = extraHeadingText;
        this.thisJVMHostname = thisJVMHostname;
        this.allowNodeMutations = allowNodeMutations;
        this.jQueryStyle = jQueryStyle;
        this.restPort = restPort;
        this.restPath = restPath;
        this.restScheme = restScheme;
        this.shutdownProc = shutdownProc;
        this.logDirection = logDirection;
        this.aclProvider = aclProvider;
        this.servoRegistration = servoRegistration;
        this.preferencesPath = preferencesPath;
        this.remoteConnectionConfiguration = remoteConnectionConfiguration;
    }
}
