package com.netflix.exhibitor.core.state;

import com.google.common.collect.Iterables;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.config.InstanceConfig;
import com.netflix.exhibitor.core.config.StringConfigs;
import com.netflix.exhibitor.core.controlpanel.ControlPanelTypes;
import java.util.List;

/**
 * Determines the state of the ZK server
 */
public class Checker
{
    private final Exhibitor exhibitor;
    private final String hostname;

    public Checker(Exhibitor exhibitor)
    {
        this(exhibitor, "localhost");
    }
    
    public Checker(Exhibitor exhibitor, String hostname)
    {
        this.exhibitor = exhibitor;
        this.hostname = hostname;
    }

    public InstanceStateTypes calculateState() throws Exception
    {
        InstanceConfig          config = exhibitor.getConfigManager().getConfig();

        if ( !isSet(config, StringConfigs.ZOOKEEPER_DATA_DIRECTORY) || !isSet(config, StringConfigs.ZOOKEEPER_INSTALL_DIRECTORY) )
        {
            return InstanceStateTypes.LATENT;
        }

        InstanceStateTypes      potentialState = InstanceStateTypes.DOWN;
        ServerList              serverList = new ServerList(config.getString(StringConfigs.SERVERS_SPEC));
        ServerSpec              us = Iterables.find(serverList.getSpecs(), ServerList.isUs(exhibitor.getThisJVMHostname()), null);
        if ( us != null )
        {
            if ( !exhibitor.getControlPanelValues().isSet(ControlPanelTypes.RESTARTS) )
            {
                potentialState = InstanceStateTypes.NO_RESTARTS_DOWN;
            }
        }
        else
        {
            if ( !exhibitor.getControlPanelValues().isSet(ControlPanelTypes.UNLISTED_RESTARTS) )
            {
                potentialState = InstanceStateTypes.UNLISTED_DOWN;
            }
        }

        InstanceStateTypes      actualState = potentialState;
        String                  ruok = new FourLetterWord(FourLetterWord.Word.RUOK, hostname, config, exhibitor.getConnectionTimeOutMs()).getResponse();
        if ( "imok".equals(ruok) )
        {
            // The following code depends on inside knowledge of the "srvr" response. If they change it
            // this code might break

            List<String> lines = new FourLetterWord(FourLetterWord.Word.SRVR, hostname, config, exhibitor.getConnectionTimeOutMs()).getResponseLines();
            for ( String line : lines )
            {
                if ( line.contains("not currently serving") )
                {
                    actualState = InstanceStateTypes.NOT_SERVING;
                    break;
                }

                if ( line.toLowerCase().startsWith("mode") )
                {
                    actualState = InstanceStateTypes.SERVING;
                    break;
                }
            }
        }

        return actualState;
    }

    private boolean isSet(InstanceConfig config, StringConfigs type)
    {
        String s = config.getString(type);
        return (s != null) && (s.trim().length() > 0);
    }
}
