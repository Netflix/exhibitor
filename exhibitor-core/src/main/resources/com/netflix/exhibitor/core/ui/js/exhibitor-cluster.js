var currentServersSpec = null;
var currentHostname = null;
var currentServersList = null;
var serverItemsVersion = 0;

var STATE_LATENT = 0;
var STATE_DOWN = 1;
var STATE_NOT_SERVING = 2;
var STATE_SERVING = 3;

function makeServersList()
{
    var serverList = new Array();

    var specs = (systemConfig.serversSpec.length > 0) ? systemConfig.serversSpec.split(",") : new Array();
    var foundUs = false;
    for ( var i = 0; i < specs.length; ++i )
    {
        var parts = specs[i].split(":");

        var thisSpec = {};
        var index = (parts.length > 2) ? 1 : 0;
        thisSpec.serverId = parseInt(parts[index++]);
        thisSpec.hostname = parts[index];
        if ( thisSpec.hostname === systemConfig.hostname )
        {
            foundUs = true;
        }

        serverList.push(thisSpec);
    }
    if ( !foundUs )
    {
        thisSpec = {};
        thisSpec.serverId = -1;
        thisSpec.hostname = systemConfig.hostname;
        serverList.push(thisSpec);
    }

    serverList.sort(function(a, b){
        return a.serverId - b.serverId;
    });

    return serverList;
}

function buildServerItems()
{
    var serversList = makeServersList();
    if ( (currentServersSpec != systemConfig.serversSpec) || (currentHostname != systemConfig.hostname) )
    {
        ++serverItemsVersion;
        internalBuildServerItems(serversList);
    }
    updateServerState(serversList);
}

function destroyOldServerItems()
{
    for ( var i = 0; i < currentServersList.length; ++i )
    {
        var domId = '#cp-' + i;
        $(domId + '-power-button').button("destroy");
        $(domId + '-4ltr-button').button("destroy");
        $(domId + '-log-button').button("destroy");

        $(domId + '-power-button').unbind("click");
        $(domId + '-4ltr-button').unbind("click");
        $(domId + '-log-button').unbind("click");
        $(domId + '-open-window').unbind("click");
    }
}

function internalBuildServerItems(serversList)
{
    if ( currentServersList != null )
    {
        destroyOldServerItems();
    }

    currentServersSpec = systemConfig.serversSpec;
    currentHostname = systemConfig.hostname;
    currentServersList = serversList;

    var content = "";
    for ( var i = 0; i < serversList.length; ++i )
    {
        var spec = serversList[i];
        var domId = 'cp-' + i;

        var thisItem = $('#base-server-item-base').html();
        thisItem = thisItem.replace("$SERVER_ID$", spec.serverId);
        thisItem = thisItem.replace("$SERVER_HOSTNAME$", spec.hostname);
        thisItem = thisItem.replace(/\$BASE_ID\$/gi, domId);

        content += thisItem;
    }
    $('#base-server-item').html(content);

    for ( i = 0; i < serversList.length; ++i )
    {
        spec = serversList[i];
        domId = '#cp-' + i;

        makeLightSwitch(domId + '-instance-restarts-enabled', null, true);
        makeLightSwitch(domId + '-cleanup-enabled', null, true);
        makeLightSwitch(domId + '-backups-enabled', null, true);
        if ( systemState.backupActive )
        {
            $(domId + '-backups-enabled-control').show();
        }

        var serverIdContent = "Server Id: " + spec.serverId;
        var hostnameContent = "Hostname: " + spec.hostname;
        if ( spec.hostname === systemConfig.hostname )
        {
            hostnameContent += '<span class="cp-this-server">(This server)</span>';
            $(domId + '-log-button').hide();
            $(domId + '-open-window').hide();
        }
        else
        {
            $(domId + '-open-window').click(openWindowForInstance(spec.hostname));
        }

        $(domId + '-server-id').html(serverIdContent);
        $(domId + '-hostname').html(hostnameContent);

        $(domId + '-power-button').button({
            disabled: true,
            icons:{
                primary:"ui-icon-alert"
            }
        });

        $(domId + '-4ltr-button').button({
            disabled: true,
            icons:{
                primary:"ui-icon-circle-zoomin"
            }
        }).click(word4ltrDialog(spec.hostname));

        $(domId + '-log-button').button({
            disabled: true,
            icons:{
                primary:"ui-icon-info"
            }
        }).click(logDialog(spec.hostname));
    }

    $('#base-server-item').colorTip();
}

function stopStartDialog(hostname)
{
    return function() {
        okCancelDialog(hostname, "Are you sure you want to restart this server?", function(){
            makeRemoteCall(URL_CLUSTER_RESTART_BASE, hostname);
        });
    };
}

function stopDialog(hostname)
{
    return function() {
        okCancelDialog(hostname, "Are you sure you want to stop this server?", function(){
            makeRemoteCall(URL_CLUSTER_STOP_BASE, hostname);
        });
    };
}

function startDialog(hostname)
{
    return function() {
        okCancelDialog(hostname, "Are you sure you want to start this server?", function(){
            makeRemoteCall(URL_CLUSTER_START_BASE, hostname);
        });
    };
}

function logDialog(hostname)
{
    return function() {
        $('#log-text').text("Loading...");

        function getLog() {
            makeRemoteCall(URL_CLUSTER_LOG_BASE, hostname, function(text){
                $('#log-text').text(text);
            });
        }
        getLog();

        $('#log-refresh-button').unbind("click");
        $('#log-refresh-button').click(getLog);

        $('#log-dialog').dialog("option", "title", hostname);
        $('#log-dialog').dialog("open");
    };
}

function openWindowForInstance(hostname)
{
    return function() {
        var replceHostname = hostname;
        if ( location.port )
        {
            replceHostname += ':' + location.port;
        }
        var thisUrl = location.href;
        var newUrl = thisUrl.replace(location.host, replceHostname);
        window.open(newUrl, '_blank');
    };
}

function word4ltrDialog(hostname)
{
    return function() {
        $('#word-4ltr-button').unbind("click");

        $('#word-4ltr-button').click(function(){
            $('#word-4ltr-text').text("Loading...");
            makeRemoteCall(URL_CLUSTER_4LTR_BASE + $('#word-4ltr').val() + "/", hostname, function(text){
                $('#word-4ltr-text').text(text);
            })
        });

        $('#word-4ltr-text').text("");
        $('#word-4ltr-dialog').dialog("option", "title", hostname);
        $('#word-4ltr-dialog').dialog("open");
    };
}

function makeRemoteCall(baseUrl, hostname, callback)
{
    $.getJSON(baseUrl + hostname + '?ts=' + Date.now(), function(data){
        if ( data.success )
        {
            if ( callback )
            {
                callback(eval(data.response));
            }
        }
        else
        {
            messageDialog("Error", "Could not complete message to " + hostname + ". Message: " + data.errorMessage);
        }
    });
}

function handleSwitch(hostname, type)
{
    return function(isChecked) {
        makeRemoteCall(URL_CLUSTER_SET_CONFIG_BASE + type + "/" + isChecked + "/", hostname);
    };
}

function updateOneServerState(index, data, hostname)
{
    var statusColor;
    var statusMessage = "";

    domId = '#cp-' + index;
    if ( data.success )
    {
        var isRunning = ((data.response.state == STATE_SERVING) || (data.response.state == STATE_NOT_SERVING));

        $(domId + '-power-button').button("option", "disabled", false);
        $(domId + '-4ltr-button').button("option", "disabled", !isRunning);
        $(domId + '-log-button').button("option", "disabled", false);

        var spec = currentServersList[index];
        if ( data.response.switches.restarts )
        {
            $(domId + '-power-button').button("option", "label", "Restart...");
            $(domId + '-power-button').click(stopStartDialog(spec.hostname));
        }
        else if ( systemState.running )
        {
            $(domId + '-power-button').button("option", "label", "Stop...");
            $(domId + '-power-button').click(stopDialog(spec.hostname));
        }
        else
        {
            $(domId + '-power-button').button("option", "label", "Start...");
            $(domId + '-power-button').click(startDialog(spec.hostname));
        }

        ableLightSwitch(domId + '-instance-restarts-enabled', handleSwitch(hostname, "restarts"));
        ableLightSwitch(domId + '-cleanup-enabled', handleSwitch(hostname, "cleanup"));
        ableLightSwitch(domId + '-backups-enabled', handleSwitch(hostname, "backups"));

        checkLightSwitch(domId + '-instance-restarts-enabled', data.response.switches.restarts);
        checkLightSwitch(domId + '-cleanup-enabled', data.response.switches.cleanup);
        checkLightSwitch(domId + '-backups-enabled', data.response.switches.backups);

        statusMessage = data.response.description;
        switch ( data.response.state )
        {
            default:
            case STATE_LATENT:
            {
                statusColor = "#FFF";
                break;
            }

            case STATE_SERVING:
            {
                statusColor = "#0E0";
                break;
            }

            case STATE_NOT_SERVING:
            {
                statusColor = "#FF0";
                break;
            }

            case STATE_DOWN:
            {
                statusColor = "#F00";
                break;
            }
        }
    }
    else
    {
        $(domId + '-power-button').button("option", "disabled", true);
        $(domId + '-4ltr-button').button("option", "disabled", true);
        $(domId + '-log-button').button("option", "disabled", true);

        ableLightSwitch(domId + '-instance-restarts-enabled', null, false);
        ableLightSwitch(domId + '-cleanup-enabled', null, false);
        ableLightSwitch(domId + '-backups-enabled', null, false);

        statusColor = "#F00";
        statusMessage = data.errorMessage;
    }
    $(domId + '-status-indicator').css('background-color', statusColor);
    $(domId + '-status-message').html("Status: " + statusMessage);
}

function updateServerState(serversList)
{
    var localServerItemsVersion = serverItemsVersion;
    for ( var i = 0; i < serversList.length; ++i )
    {
        var spec = serversList[i];
        var thisHostname = spec.hostname;
        if ( thisHostname === systemConfig.hostname )
        {
            thisHostname = "localhost";
        }
        var callback = function(index, hostname) {
            return function(data) {
                if ( serverItemsVersion === localServerItemsVersion )
                {
                    updateOneServerState(index, data, hostname);
                }
            };
        };
        $.getJSON(URL_CLUSTER_GET_STATE_BASE + thisHostname + '?ts=' + Date.now(), callback(i, thisHostname));
    }
}
