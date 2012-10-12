var BUILTIN_TAB_QTY = 4;
var AUTO_REFRESH_PERIOD = 5000;
var UPDATE_STATE_PERIOD = 2500;

var URL_GET_BACKUPS = "../index/get-backups";
var URL_NEW_INDEX = "../index/new-index";
var URL_DELETE_INDEX_BASE = "../index/";
var URL_INDEX_DATA_BASE = "../index/dataTable/";
var URL_GET_INDEX_BASE = "../index/get/";
var URL_GET_INDEXES = "../index/indexed-logs";
var URL_CACHE_INDEX_SEARCH = "../index/cache-search";
var URL_RELEASE_CACHE_INDEX_SEARCH_BASE = "../index/release-cache/";

var URL_CLUSTER_LOG_BASE = "../cluster/log/";
var URL_CLUSTER_RESTART_BASE = "../cluster/restart/";
var URL_CLUSTER_START_BASE = "../cluster/start/";
var URL_CLUSTER_STOP_BASE = "../cluster/stop/";
var URL_CLUSTER_4LTR_BASE = "../cluster/4ltr/";
var URL_CLUSTER_SET_CONFIG_BASE = "../cluster/set/";
var URL_CLUSTER_GET_STATE_BASE = "../cluster/state/";

var URL_EXPLORER_NODE_DATA = "../explorer/node-data";
var URL_EXPLORER_NODE = "../explorer/node";
var URL_EXPLORER_ZNODE_BASE = "../explorer/znode";
var URL_EXPLORER_ANALYZE = "../explorer/analyze";

var URL_GET_STATE = "../config/get-state";
var URL_SET_CONFIG = "../config/set";
var URL_SET_CONFIG_ROLLING = "../config/set-rolling";
var URL_ROLLBACK_ROLLING = "../config/rollback-rolling";
var URL_FORCE_COMMIT_ROLLING = "../config/force-commit-rolling";

var URL_GET_BACKUP_CONFIG = "backup-config";
var URL_GET_TABS = "tabs";
var URL_RESTART = "stop";

var AUTOMATIC_INSTANCE_MANAGEMENT_HELP_TEXT = "When on, as new instances read the shared config they will automatically add themselves to the ensemble via a rolling release. Additionally, old instances will be automatically removed via rolling release.";

var doConfigUpdates = true;
var configChangesBeingSubmitted = false;

var backupTabIndex = 3;

function messageDialog(title, message, noIcon)
{
    if ( noIcon )
    {
        $('#message-dialog-icon').hide();
    }
    else
    {
        $('#message-dialog-icon').show();
    }
    $('#message-dialog-text').html(message);
    $("#message-dialog").dialog("option", "title", title);
    $("#message-dialog").dialog("option", "buttons",
        {
            "OK":function ()
            {
                $(this).dialog("close");
            }
        }
    );
    $("#message-dialog").dialog("open");
}

function okCancelDialog(title, message, okFunction, noIcon)
{
    if ( noIcon )
    {
        $('#message-dialog-icon').hide();
    }
    else
    {
        $('#message-dialog-icon').show();
    }

    $('#message-dialog-text').html(message);
    $("#message-dialog").dialog("option", "title", title);
    $("#message-dialog").dialog("option", "buttons",
        {
            "Cancel":function ()
            {
                $(this).dialog("close");
            },

            "OK":function ()
            {
                $(this).dialog("close");
                okFunction();
            }
        }
    );
    $("#message-dialog").dialog("open");
}

var hasBackupConfig = false;
var systemState = {};
var systemConfig = {};
var connectedToExhibitor = true;
var currentVersion = null;
var backupTabRemoved = false;
function updateState()
{
    if ( !hasBackupConfig )
    {
        $.getJSON(URL_GET_BACKUP_CONFIG + '?ts=' + Date.now(), function(data){
            hasBackupConfig = true;
            addBackupExtraConfig(data);
        });
        return;
    }

    $.ajax({
        url: URL_GET_STATE,
        cache: false,
        type: 'GET',
        success: function (data, dummy, jqXHR){
            systemState = data;

            if ( doConfigUpdates && !configChangesBeingSubmitted ) {
                systemConfig = systemState.config;
            }

            if ( currentVersion === null )
            {
                currentVersion = systemState.version;
            }
            else if ( systemState.version != currentVersion )
            {
                currentVersion = systemState.version;
                location.reload();
                return;
            }

            if ( !connectedToExhibitor )
            {
                connectedToExhibitor = true;
                messageDialog("", "Connection with the " + $('#app-name').html() + " server re-established.");
            }

            if ( systemState.backupActive )
            {
                $('#config-backups-fieldset').show();
                $('#backups-enabled-control').show();
            }
            else
            {
                if ( !backupTabRemoved )
                {
                    backupTabRemoved = true;
                    $("#tabs").tabs("remove", backupTabIndex);
                    backupTabIndex = -1;
                    --BUILTIN_TAB_QTY;
                }
                $('#config-backups-fieldset').hide();
                $('#backups-enabled-control').hide();
            }

            if ( systemState.standaloneMode )
            {
                $('#standalone-mode-message').show();
                $('#fieldset-automatic-instance-management').hide();
            }

            if ( systemState.nodeMutationsAllowed )
            {
                $('#explorer-buttons').show();
                $('#index-query-clear-restore-button').show();
            }
            else
            {
                $('#explorer-buttons').hide();
                $('#index-query-clear-restore-button').hide();
            }

            $.unblockUI();

            if ( systemState.extraHeadingText )
            {
                $('#page-title-extra').html(systemState.extraHeadingText);
            }
            $('#version').html(systemState.version);
            $('#not-connected-alert').hide();
            $('#instance-hostname').html(systemConfig.hostname);
            $('#instance-id').html((
                systemConfig.serverId > 0
                ) ? systemConfig.serverId : "n/a");

            updateConfig();
            buildServerItems();
        },

        error: function(jqXHR, status){
            if ( connectedToExhibitor )
            {
                $.blockUI({
                    css: {
                        zIndex: 99998,
                        cursor: 'default'
                    },
                    message: null,
                    overlayCSS: {
                        backgroundColor: '#333',
                        cursor: 'default'
                    }
                });

                $('#not-connected-alert').show();
                connectedToExhibitor = false;
                messageDialog("Error", "The browser lost connection with the " + $('#app-name').html() + " server.");
            }
        }
    });
}

var configExtraTab = new Array();

function addBackupExtraConfig(data)
{
    configExtraTab = $.makeArray(data);
    var extra = "";
    for ( var i = 0; i < configExtraTab.length; ++i )
    {
        var c = configExtraTab[i];
        var id = getBackupExtraId(c);
        var fieldSize = (c.type === "s") ? 60 : 5;
        extra += '<label for="' + id + '">' + c.name + '</label><input type="text" id="' + id + '" name="' + id + '" size="' + fieldSize + '" title="' + c.help + '"><br clear="all"/>';
    }

    $('#config-backups-extra').html(extra);
    for ( i = 0; i < configExtraTab.length; ++i )
    {
        c = configExtraTab[i];
        if ( c.type === "i" )
        {
            id = getBackupExtraId(c);
            $('#' + id).keyfilter($.fn.keyfilter.defaults.masks['pint']);
        }
    }

    updateState();

    var isChecked = $('#config-editable').prop('checked');
    ableConfig(isChecked);
}

function buildNewConfig()
{
    var newConfig = {};
    newConfig.zookeeperInstallDirectory = $('#config-zookeeper-install-dir').val();
    newConfig.zookeeperDataDirectory = $('#config-zookeeper-snapshot-dir').val();
    newConfig.zookeeperLogDirectory = $('#config-zookeeper-log-dir').val();
    newConfig.logIndexDirectory = $('#config-log-index-dir').val();
    newConfig.autoManageInstancesSettlingPeriodMs = $('#config-automatic-management-period-ms').val();
    newConfig.observerThreshold = $('#config-observer-threshold').val();
    newConfig.serversSpec = $('#config-servers-spec').val();
    newConfig.javaEnvironment = $('#config-java-env').val();
    newConfig.log4jProperties = $('#config-java-log4j').val();
    newConfig.clientPort = $('#config-client-port').val();
    newConfig.connectPort = $('#config-connect-port').val();
    newConfig.electionPort = $('#config-election-port').val();
    newConfig.checkMs = $('#config-check-ms').val();
    newConfig.cleanupPeriodMs = $('#config-cleanup-ms').val();
    newConfig.cleanupMaxFiles = $('#config-cleanup-max-files').val();
    newConfig.backupPeriodMs = $('#config-backup-ms').val();
    newConfig.backupMaxStoreMs = $('#config-backup-max-store-ms').val();
    newConfig.autoManageInstances = $('#cp-auto-init-instances').prop("checked") ? "1" : "0";

    var zooCfgTab = $('#config-custom').val().split("\n");
    newConfig.zooCfgExtra = {};
    for ( var i = 0; i < zooCfgTab.length; ++i )
    {
        var zooCfgParts = zooCfgTab[i].split("=");
        if ( zooCfgParts.length == 2 )
        {
            newConfig.zooCfgExtra[zooCfgParts[0]] = zooCfgParts[1];
        }
    }

    newConfig.backupExtra = {};
    for ( i = 0; i < configExtraTab.length; ++i )
    {
        var c = configExtraTab[i];
        var id = getBackupExtraId(c);
        newConfig.backupExtra[c.key] = $('#' + id).val();
    }

    newConfig.connectionTimeoutMs = systemConfig.connectionTimeoutMs;
    newConfig.serverId = systemConfig.serverId;

    return newConfig;
}

function turnOffEditableSwitch()
{
    checkLightSwitch('#config-editable', false);
    handleEditableSwitch();
}

function hideShowConfigProcessingDialog(showIt)
{
    $('#updating-config-dialog').dialog(showIt ? "open" : "close");
}

function submitConfigChanges(rolling)
{
    if ( configChangesBeingSubmitted )
    {
        messageDialog("Please Wait", "A previous config change is still being submitted");
        return;
    }

    var newConfig = buildNewConfig();

    systemConfig = newConfig;

    var payload = JSON.stringify(newConfig);

    configChangesBeingSubmitted = true;
    hideShowConfigProcessingDialog(true);
    $.ajax({
        type: 'POST',
        url: rolling ? URL_SET_CONFIG_ROLLING : URL_SET_CONFIG,
        cache: false,
        data: payload,
        contentType: 'application/json',
        success:function(data){
            hideShowConfigProcessingDialog(false);
            configChangesBeingSubmitted = false;
            if ( !data.succeeded )
            {
                messageDialog("Error", data.message);
            }
        },
        error:function(){
            hideShowConfigProcessingDialog(false);
            configChangesBeingSubmitted = false;
            messageDialog("There was a communication error. The config change may not have committed.")
        }
    });
    turnOffEditableSwitch();

    if ( rolling )
    {
        messageDialog("Rolling Config", "The rolling configuration change has been submitted and will start in a moment.", true);
    }
}

function getBackupExtraId(obj)
{
    return 'config-backup-extra-' + obj.key;
}

function ableConfig(enable)
{
    ableLightSwitch('#cp-auto-init-instances', null, enable);

    $('#config-zookeeper-install-dir').prop('disabled', !enable);
    $('#config-zookeeper-snapshot-dir').prop('disabled', !enable);
    $('#config-zookeeper-log-dir').prop('disabled', !enable);
    $('#config-automatic-management-period-ms').prop('disabled', !enable);
    $('#config-observer-threshold').prop('disabled', !enable);
    $('#config-log-index-dir').prop('disabled', !enable);
    $('#config-servers-spec').prop('disabled', !enable);
    $('#config-java-env').prop('disabled', !enable);
    $('#config-java-log4j').prop('disabled', !enable);
    $('#config-custom').prop('disabled', !enable);
    $('#config-client-port').prop('disabled', !enable);
    $('#config-connect-port').prop('disabled', !enable);
    $('#config-election-port').prop('disabled', !enable);
    $('#config-check-ms').prop('disabled', !enable);
    $('#config-cleanup-ms').prop('disabled', !enable);
    $('#config-cleanup-max-files').prop('disabled', !enable);
    $('#config-backup-ms').prop('disabled', !enable);
    $('#config-backup-max-store-ms').prop('disabled', !enable);

    for ( var i = 0; i < configExtraTab.length; ++i )
    {
        var c = configExtraTab[i];
        var id = getBackupExtraId(c);
        $('#' + id).prop('disabled', !enable);
    }

    $("#config-button").button((enable && !systemConfig.rollInProgress) ? "enable" : "disable");
}

function updateConfig()
{
    if ( !doConfigUpdates || configChangesBeingSubmitted ) {
        return;
    }

    var configExtra = "";
    for ( var p in systemConfig.zooCfgExtra )
    {
        configExtra += p + "=" + systemConfig.zooCfgExtra[p] + "\n";
    }

    checkLightSwitch('#cp-auto-init-instances', (systemConfig.autoManageInstances != "0"));
    $('#config-zookeeper-install-dir').val(systemConfig.zookeeperInstallDirectory);
    $('#config-zookeeper-snapshot-dir').val(systemConfig.zookeeperDataDirectory);
    $('#config-zookeeper-log-dir').val(systemConfig.zookeeperLogDirectory);
    $('#config-automatic-management-period-ms').val(systemConfig.autoManageInstancesSettlingPeriodMs);
    $('#config-observer-threshold').val(systemConfig.observerThreshold);
    $('#config-log-index-dir').val(systemConfig.logIndexDirectory);
    $('#config-servers-spec').val(systemConfig.serversSpec);
    $('#config-java-env').val(systemConfig.javaEnvironment);
    $('#config-java-log4j').val(systemConfig.log4jProperties);
    $('#config-custom').val(configExtra);
    $('#config-client-port').val(systemConfig.clientPort);
    $('#config-connect-port').val(systemConfig.connectPort);
    $('#config-election-port').val(systemConfig.electionPort);
    $('#config-check-ms').val(systemConfig.checkMs);
    $('#config-cleanup-ms').val(systemConfig.cleanupPeriodMs);
    $('#config-cleanup-max-files').val(systemConfig.cleanupMaxFiles);
    $('#config-backup-ms').val(systemConfig.backupPeriodMs);
    $('#config-backup-max-store-ms').val(systemConfig.backupMaxStoreMs);

    $('#rolling-config-floater-status').html(systemConfig.rollStatus);
    if ( systemConfig.rollInProgress )
    {
        $('#rolling-config-progressbar').progressbar("option", "value", systemConfig.rollPercentDone);
        $('#rolling-config-floater').show();
    }
    else
    {
        if ( $('#rolling-config-progressbar').progressbar("option", "value") != 100 )
        {
            $('#rolling-config-progressbar').progressbar("option", "value", 100);
        }
        else
        {
            $('#rolling-config-floater').hide();
        }
    }

    for ( i = 0; i < configExtraTab.length; ++i )
    {
        c = configExtraTab[i];
        id = getBackupExtraId(c);
        $('#' + id).val(systemConfig.backupExtra[c.key]);
    }
}

function refreshCurrentTab()
{
    var selected = $("#tabs").tabs("option", "selected");
    if ( selected == backupTabIndex )
    {
        var radio = $('input:radio:checked[name="restore-item-radio"]');
        updateRestoreItems(radio.val());
    }
    else if ( selected >= BUILTIN_TAB_QTY )
    {
        var index = selected - BUILTIN_TAB_QTY;
        $("#" + customTabs[index].contentId).load(customTabs[index].url);
    }
}

function updateCalculatorValue(lhs)
{
    var lhsValue = parseInt($('#lhs-millisecond-calculator-value').val());
    var lhsUnit = parseInt($('#lhs-millisecond-calculator-unit').val());
    var rhsValue = parseInt($('#rhs-millisecond-calculator-value').val());
    var rhsUnit = parseInt($('#rhs-millisecond-calculator-unit').val());

    if ( lhs )
    {
        $('#rhs-millisecond-calculator-value').val((lhsValue * lhsUnit) / rhsUnit);
    }
    else
    {
        $('#lhs-millisecond-calculator-value').val((rhsValue * rhsUnit) / lhsUnit);
    }
}

function checkLightSwitch(selector, check)
{
    $(selector).onOff("setChecked", check);
}

function ableLightSwitch(selector, func, enable)
{
    $(selector).onOff("setEnabled", enable || (enable == undefined));
    if ( func )
    {
        $(selector).onOff("setCallback", func);
    }
}

function makeLightSwitch(selector, func, disable)
{
    if ( disable === undefined )
    {
        disable = false;
    }

    $(selector).onOff({
        labelClass: 'on-off-label',
        callback: func
    });
    if ( disable )
    {
        ableLightSwitch(selector, func, false);
    }
    else
    {
        $(selector).onOff('setCallback', func);
    }
}

function handleEditableSwitch(isChecked)
{
    doConfigUpdates = !isChecked;

    if ( !isChecked ) {
        updateConfig();
    }
    ableConfig(isChecked);
}

function cancelRollingConfig(forceCommit)
{
    $('#rolling-config-floater').hide();
    $.get(forceCommit ? URL_FORCE_COMMIT_ROLLING : URL_ROLLBACK_ROLLING);
}

function checkConfigConfirmation()
{
    var     newConfig = buildNewConfig();
    var     hasEnsembleLevelChange =
        (newConfig.zookeeperInstallDirectory != systemConfig.zookeeperInstallDirectory)
        || (newConfig.zookeeperDataDirectory != systemConfig.zookeeperDataDirectory)
        || (newConfig.zookeeperLogDirectory != systemConfig.zookeeperLogDirectory)
        || (newConfig.serversSpec != systemConfig.serversSpec)
        || (newConfig.clientPort != systemConfig.clientPort)
        || (newConfig.connectPort != systemConfig.connectPort)
        || (newConfig.electionPort != systemConfig.electionPort)
        || (newConfig.javaEnvironment != systemConfig.javaEnvironment)
        || (newConfig.log4jProperties != systemConfig.log4jProperties)
        || (JSON.stringify(newConfig.zooCfgExtra) != JSON.stringify(systemConfig.zooCfgExtra))
    ;

    if ( !hasEnsembleLevelChange )
    {
        for ( var p in systemConfig.zooCfgExtra )
        {
            if ( newConfig.zooCfgExtra[p] != systemConfig.zooCfgExtra[p] )
            {
                hasEnsembleLevelChange = true;
                break;
            }
        }
    }

    if ( hasEnsembleLevelChange )
    {
        if ( systemState.standaloneMode )
        {
            $('#standalone-config-commit-dialog').dialog("open");
        }
        else
        {
            $('#config-commit-dialog').dialog("open");
        }
    }
    else
    {
        okCancelDialog("Commit", "The changes will not require any server restarts and so will be applied immediately", function(){
            submitConfigChanges(false);
        });
    }
}

var customTabs = new Array();
$(function ()
{
    $.getJSON(URL_GET_TABS + '?ts=' + Date.now(), function (data){
        var uiTabSpec = $.makeArray(data);
        for ( var i = 0; i < uiTabSpec.length; ++i )
        {
            var tabData = {};
            tabData.id = 'tabs-custom-' + i;
            tabData.contentId = 'tabs-custom-content' + i;
            tabData.url = uiTabSpec[i].url;
            customTabs[i] = tabData;

            var tabContentClass = uiTabSpec[i].html ? 'tab-html' : 'tab-text';
            var tabContent = '<div id="' + tabData.id + '" class="ui-helper-hidden"><div id="' + tabData.contentId + '" class="' + tabContentClass + '"></div></div>';
            $('#tabs').append(tabContent);
            $('#tabs-list').append('<li><a href="#' + tabData.id + '">' + uiTabSpec[i].name + '</a></li>');
        }
        $('#tabs').tabs({
            panelTemplate:'<div><div class="text"></div></div>',

            show:function (event, ui)
            {
                refreshCurrentTab();
            },

            create:function (event, ui)
            {
                initExplorer();
                initModifyUi();
            }
        });
    });

    $("#stop-button").button({
        icons:{
            primary:"ui-icon-alert"
        }
    }).click(function ()
        {
            okCancelDialog("Restart ZooKeeper", "Are you sure you want to restart ZooKeeper?", function ()
            {
                $.get(URL_RESTART);
                messageDialog("Restart ZooKeeper", "Stop request sent. Check the log for details.");
            });
            return false;
        });

    $("#start-button").button().click(function ()
    {
        var selected = $("#tabs").tabs("option", "selected");
        $("#tabs").tabs("load", selected);
        return false;
    });

    $("#message-dialog").dialog({
        modal: true,
        width: 400,
        autoOpen: false,
        zIndex: 99999
    });

    $("#config-button").button({
        icons:{
            primary:"ui-icon-disk"
        }
    }).click(function(){
        checkConfigConfirmation();
        return false;
    });

    $('#calculator-button').button({
        icons:{
            primary:"ui-icon-calculator"
        }
    }).click(function(){
        updateCalculatorValue();
        $('#millisecond-calculator-dialog').dialog("open");
    });

    $('#not-connected-message').html("Not connected to " + $('#app-name').html() + " server");

    window.setInterval("updateState()", UPDATE_STATE_PERIOD);
    updateState();
    ableConfig(false);

    $('#config-group').colorTip();

    $("#millisecond-calculator-dialog").dialog({
        modal: true,
        title: 'Converter',
        autoOpen: false,
        zIndex: 9999
    });
    $('#lhs-millisecond-calculator-value').keyup(function(){
        updateCalculatorValue(true);
    });
    $('#lhs-millisecond-calculator-unit').change(function(){
        updateCalculatorValue(false);
    });
    $('#rhs-millisecond-calculator-value').keyup(function(){
        updateCalculatorValue(false);
    });
    $('#rhs-millisecond-calculator-unit').change(function(){
        updateCalculatorValue(true);
    });

    $('#word-4ltr-dialog').dialog({
        modal: true,
        autoOpen: false,
        title: "4LTR",
        width: 600,
        height: 400
    });
    $("#word-4ltr-button").button();

    $('#log-dialog').dialog({
        modal: true,
        autoOpen: false,
        title: "Log",
        width: 600,
        height: 400
    });
    $("#log-refresh-button").button({
        icons:{
            primary: "ui-icon-refresh"
        }
    });

    $('#rolling-config-cancel-dialog').dialog({
        width: 500,
        height: 200,
        modal: true,
        autoOpen: false,
        title: "Cancel"
    });
    $("#rolling-config-cancel-dialog").dialog("option", "buttons", {
            'Continue Release': function (){
                $(this).dialog("close");
            },

            'Rollback': function (){
                $(this).dialog("close");
                cancelRollingConfig(false);
            },

            'Force Commit': function (){
                $(this).dialog("close");
                cancelRollingConfig(true);
            }
        }
    );

    $('#rolling-config-floater-button').button();
    $('#rolling-config-floater-button').click(function(){
        $('#rolling-config-cancel-dialog').dialog("open");
    });

    $('#config-zookeeper-install-dir-help-button').button();
    $('#config-zookeeper-install-dir-help-button').click(function(){
        messageDialog(
            "ZooKeeper Install Dir",
            "<p>To support upgrading the ZooKeeper version, you can specify only a parent directory for ZooKeeper Install Dir. </p>" +
            "<p>" +
            "Add '*' to the end of the value and Exhibitor will search for the latest version of ZooKeeper in that directory. " +
            "It does this by choosing the directory with the highest version number in the name. i.e. 'zookeeper-3.4.3' will be chosen over 'zookeeper.3.3.5'. " +
            "</p>",
            true
        );
    });

    $('#rolling-config-progressbar').progressbar({
        value: 1
    });

    $('#config-commit-dialog').dialog({
        width: 500,
        height: 250,
        modal: true,
        autoOpen: false,
        title: "Config Change Warning"
    });
    $("#config-commit-dialog").dialog("option", "buttons", {
            'Cancel': function (){
                $(this).dialog("close");
            },

            'All At Once...': function (){
                okCancelDialog("All At Once", "Are you sure you want to commit all at once?", function() {
                    $('#config-commit-dialog').dialog("close");
                    submitConfigChanges(false);
                });
            },

            'Rolling Release...': function (){
                okCancelDialog("Rolling Release", "Are you sure you want to do a rolling release?", function() {
                    $('#config-commit-dialog').dialog("close");
                    submitConfigChanges(true);
                });
            }
        }
    );

    $('#standalone-config-commit-dialog').dialog({
        width: 500,
        height: 250,
        modal: true,
        autoOpen: false,
        title: "Config Change Warning"
    });
    $("#standalone-config-commit-dialog").dialog("option", "buttons", {
            'Cancel': function (){
                $(this).dialog("close");
            },

            'OK': function (){
                $(this).dialog("close");
                submitConfigChanges(false);
            }
        }
    );

    $('#updating-config-progressbar').progressbar({
        value: 100
    });
    $('#updating-config-dialog').dialog({
        width: 300,
        height: 100,
        modal: true,
        autoOpen: false,
        title: "Config change in progress...",
        resizable: false
    });

    $('#cp-auto-init-instances-help').attr("title", AUTOMATIC_INSTANCE_MANAGEMENT_HELP_TEXT);
    $('#cp-auto-init-instances-help-button').button().click(function(){
        messageDialog("Automatic Server List Add/Remove", AUTOMATIC_INSTANCE_MANAGEMENT_HELP_TEXT);
    });

    makeLightSwitch('#config-editable', handleEditableSwitch);
    makeLightSwitch('#cp-auto-init-instances');
    turnOffEditableSwitch();

    initRestoreUI();
    updateState();
    window.setInterval("refreshCurrentTab()", AUTO_REFRESH_PERIOD);
});
