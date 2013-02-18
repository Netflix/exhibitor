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
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.activity.ActivityQueue;
import com.netflix.exhibitor.core.activity.QueueGroups;
import com.netflix.exhibitor.core.activity.RepeatingActivity;
import com.netflix.exhibitor.core.activity.RepeatingActivityImpl;
import com.netflix.exhibitor.core.automanage.AutomaticInstanceManagement;
import com.netflix.exhibitor.core.automanage.RemoteInstanceRequestClient;
import com.netflix.exhibitor.core.automanage.RemoteInstanceRequestClientImpl;
import com.netflix.exhibitor.core.backup.BackupManager;
import com.netflix.exhibitor.core.backup.BackupProvider;
import com.netflix.exhibitor.core.config.ConfigListener;
import com.netflix.exhibitor.core.config.ConfigManager;
import com.netflix.exhibitor.core.config.ConfigProvider;
import com.netflix.exhibitor.core.config.IntConfigs;
import com.netflix.exhibitor.core.config.JQueryStyle;
import com.netflix.exhibitor.core.controlpanel.ControlPanelValues;
import com.netflix.exhibitor.core.controlpanel.FileBasedPreferences;
import com.netflix.exhibitor.core.index.IndexCache;
import com.netflix.exhibitor.core.processes.ProcessMonitor;
import com.netflix.exhibitor.core.processes.ProcessOperations;
import com.netflix.exhibitor.core.processes.StandardProcessOperations;
import com.netflix.exhibitor.core.rest.UITab;
import com.netflix.exhibitor.core.servo.GetMonitorData;
import com.netflix.exhibitor.core.servo.ZookeeperMonitoredData;
import com.netflix.exhibitor.core.state.CleanupManager;
import com.netflix.exhibitor.core.state.ManifestVersion;
import com.netflix.exhibitor.core.state.MonitorRunningInstance;
import com.netflix.servo.monitor.CompositeMonitor;
import com.netflix.servo.monitor.Monitors;
import jsr166y.ForkJoinPool;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;
import java.util.prefs.Preferences;

public class Exhibitor implements Closeable
{
    private final ActivityLog                   log;
    private final ActivityQueue                 activityQueue = new ActivityQueue();
    private final MonitorRunningInstance        monitorRunningInstance;
    private final Collection<UITab>             additionalUITabs;
    private final ProcessOperations             processOperations;
    private final CleanupManager                cleanupManager;
    private final AtomicReference<State>        state = new AtomicReference<State>(State.LATENT);
    private final IndexCache                    indexCache;
    private final ControlPanelValues            controlPanelValues;
    private final BackupManager                 backupManager;
    private final ConfigManager                 configManager;
    private final ExhibitorArguments            arguments;
    private final ProcessMonitor                processMonitor;
    private final RepeatingActivity             autoInstanceManagement;
    private final RepeatingActivity             servoMonitoring;
    private final CompositeMonitor<?>           servoCompositeMonitor;
    private final ManifestVersion               manifestVersion = new ManifestVersion();
    private final ForkJoinPool                  forkJoinPool = new ForkJoinPool();
    private final RemoteInstanceRequestClient   remoteInstanceRequestClient;

    public static final int        AUTO_INSTANCE_MANAGEMENT_PERIOD_MS = 60000;

    private CuratorFramework    localConnection;    // protected by synchronization

    private enum State
    {
        LATENT,
        STARTED,
        STOPPED
    }

    /**
     * Return this VM's hostname if possible
     *
     * @return hostname
     */
    public static String getHostname()
    {
        String      host = "unknown";
        try
        {
            return InetAddress.getLocalHost().getHostName();
        }
        catch ( UnknownHostException e )
        {
            // ignore
        }
        return host;
    }

    /**
     * @param configProvider config source
     * @param additionalUITabs any additional tabs in the UI (can be null)
     * @param backupProvider backup provider or null
     * @param arguments startup arguments
     * @throws IOException errors
     */
    public Exhibitor(ConfigProvider configProvider, Collection<? extends UITab> additionalUITabs, BackupProvider backupProvider, ExhibitorArguments arguments) throws Exception
    {
        System.out.println(getVersion());

        this.arguments = arguments;
        log = new ActivityLog(arguments.logWindowSizeLines);
        this.configManager = new ConfigManager(this, configProvider, arguments.configCheckMs);
        this.additionalUITabs = (additionalUITabs != null) ? ImmutableList.copyOf(additionalUITabs) : ImmutableList.<UITab>of();
        this.processOperations = new StandardProcessOperations(this);
        monitorRunningInstance = new MonitorRunningInstance(this);
        cleanupManager = new CleanupManager(this);
        indexCache = new IndexCache(log);
        processMonitor = new ProcessMonitor(this);
        autoInstanceManagement = new RepeatingActivityImpl(log, activityQueue, QueueGroups.MAIN, new AutomaticInstanceManagement(this), getAutoInstanceManagementPeriod());

        remoteInstanceRequestClient = new RemoteInstanceRequestClientImpl(arguments.remoteConnectionConfiguration);

        AtomicReference<CompositeMonitor<?>>    theMonitor = new AtomicReference<CompositeMonitor<?>>();
        servoMonitoring = initServo(this, log, activityQueue, arguments, theMonitor);
        servoCompositeMonitor = theMonitor.get();

        controlPanelValues = new ControlPanelValues(getPreferences());

        this.backupManager = new BackupManager(this, backupProvider);
    }

    public String   getVersion()
    {
        return manifestVersion.getVersion();
    }

    /**
     * @return logging manager
     */
    public ActivityLog getLog()
    {
        return log;
    }

    /**
     * @return cache of indexed log files
     */
    public IndexCache getIndexCache()
    {
        return indexCache;
    }

    /**
     * Start the app
     *
     * @throws Exception errors
     */
    public void start() throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED));

        activityQueue.start();
        configManager.start();
        monitorRunningInstance.start();
        cleanupManager.start();
        backupManager.start();
        autoInstanceManagement.start();
        if ( servoMonitoring != null )
        {
            servoMonitoring.start();
        }

        configManager.addConfigListener
        (
            new ConfigListener()
            {
                @Override
                public void configUpdated()
                {
                    try
                    {
                        resetLocalConnection();
                    }
                    catch ( IOException e )
                    {
                        log.add(ActivityLog.Type.ERROR, "Resetting connection", e);
                    }
                }
            }
        );
    }

    public String getExtraHeadingText()
    {
        return arguments.extraHeadingText;
    }

    @Override
    public void close() throws IOException
    {
        Preconditions.checkState(state.compareAndSet(State.STARTED, State.STOPPED));

        if ( (arguments.servoRegistration != null) && (servoCompositeMonitor != null) )
        {
            arguments.servoRegistration.getMonitorRegistry().unregister(servoCompositeMonitor);
        }

        Closeables.closeQuietly(servoMonitoring);
        Closeables.closeQuietly(autoInstanceManagement);
        Closeables.closeQuietly(processMonitor);
        Closeables.closeQuietly(indexCache);
        Closeables.closeQuietly(backupManager);
        Closeables.closeQuietly(cleanupManager);
        Closeables.closeQuietly(monitorRunningInstance);
        Closeables.closeQuietly(configManager);
        Closeables.closeQuietly(activityQueue);
        Closeables.closeQuietly(remoteInstanceRequestClient);
        closeLocalConnection();
    }

    /**
     * @return any additionally configured tabs
     */
    public Collection<UITab> getAdditionalUITabs()
    {
        return additionalUITabs;
    }

    public JQueryStyle  getJQueryStyle()
    {
        return arguments.jQueryStyle;
    }

    public ConfigManager getConfigManager()
    {
        return configManager;
    }

    public ActivityQueue getActivityQueue()
    {
        return activityQueue;
    }

    public ProcessOperations getProcessOperations()
    {
        return processOperations;
    }

    /**
     * Return the configured ZK connection timeout in ms
     *
     * @return timeout
     */
    public int getConnectionTimeOutMs()
    {
        return arguments.connectionTimeOutMs;
    }

    public String getThisJVMHostname()
    {
        return arguments.thisJVMHostname;
    }

    public boolean nodeMutationsAllowed()
    {
        return arguments.allowNodeMutations;
    }

    /**
     * Closes/resets the ZK connection or does nothing if it hasn't been opened yet
     *
     * @throws IOException errors
     */
    public synchronized void resetLocalConnection() throws IOException
    {
        closeLocalConnection();
    }

    /**
     * Return a connection ot the ZK instance (creating it if needed)
     *
     * @return connection
     * @throws IOException errors
     */
    public synchronized CuratorFramework getLocalConnection() throws IOException
    {
        if ( localConnection == null )
        {
            CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
                .connectString("localhost:" + configManager.getConfig().getInt(IntConfigs.CLIENT_PORT))
                .sessionTimeoutMs(arguments.connectionTimeOutMs * 10)
                .connectionTimeoutMs(arguments.connectionTimeOutMs)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3));

            if ( arguments.aclProvider != null )
            {
                builder = builder.aclProvider(arguments.aclProvider);
            }

            localConnection = builder.build();
            localConnection.start();
        }
        return localConnection;
    }

    public ControlPanelValues getControlPanelValues()
    {
        return controlPanelValues;
    }

    public BackupManager getBackupManager()
    {
        return backupManager;
    }

    public ProcessMonitor getProcessMonitor()
    {
        return processMonitor;
    }

    public MonitorRunningInstance getMonitorRunningInstance()
    {
        return monitorRunningInstance;
    }

    public int getRestPort()
    {
        return arguments.restPort;
    }

    public String getRestPath()
    {
        return arguments.restPath;
    }

    public String getRestScheme()
    {
        return arguments.restScheme;
    }

    public Runnable getShutdownProc()
    {
        return arguments.shutdownProc;
    }

    public RemoteInstanceRequestClient getRemoteInstanceRequestClient()
    {
        return remoteInstanceRequestClient;
    }

    public ExhibitorArguments.LogDirection getLogDirection()
    {
        return arguments.logDirection;
    }

    public ForkJoinPool getForkJoinPool()
    {
        return forkJoinPool;
    }

    private Preferences getPreferences() throws IOException
    {
        if ( arguments.preferencesPath != null )
        {
            return new FileBasedPreferences(new File(arguments.preferencesPath));
        }
        return Preferences.userRoot();
    }

    private synchronized void closeLocalConnection()
    {
        Closeables.closeQuietly(localConnection);
        localConnection = null;
    }

    private static int getAutoInstanceManagementPeriod()
    {
        return AUTO_INSTANCE_MANAGEMENT_PERIOD_MS + (int)(AUTO_INSTANCE_MANAGEMENT_PERIOD_MS * Math.random());  // add some randomness to avoid overlap with other Exhibitors
    }

    private static RepeatingActivity initServo(Exhibitor exhibitor, ActivityLog log, ActivityQueue activityQueue, ExhibitorArguments arguments, AtomicReference<CompositeMonitor<?>> theMonitor)
    {
        theMonitor.set(null);

        RepeatingActivity localServoMonitoring = null;
        if ( arguments.servoRegistration != null )
        {
            ZookeeperMonitoredData  zookeeperMonitoredData = new ZookeeperMonitoredData();
            CompositeMonitor<?>     compositeMonitor = Monitors.newObjectMonitor(zookeeperMonitoredData);

            GetMonitorData          getMonitorData = new GetMonitorData(exhibitor, zookeeperMonitoredData);
            localServoMonitoring = new RepeatingActivityImpl(log, activityQueue, QueueGroups.IO, getMonitorData, arguments.servoRegistration.getZookeeperPollMs());
            arguments.servoRegistration.getMonitorRegistry().register(compositeMonitor);

            theMonitor.set(compositeMonitor);
        }
        return localServoMonitoring;
    }
}
