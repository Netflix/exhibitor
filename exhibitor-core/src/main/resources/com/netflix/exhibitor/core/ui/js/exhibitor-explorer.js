var explorerSelectedPath = null;
var explorerSelectedBytes = null;

var analyzePathsContainerContent = null;
var analyzePathsContainerQty = 0;

function removeFromAnalyzePathsContainer()
{
    if ( analyzePathsContainerQty > 1 )
    {
        --analyzePathsContainerQty;
        $('#analyze-paths-' + analyzePathsContainerQty).remove();
    }
}

function addToAnalyzePathsContainer()
{
    var requestData = buildRequestData();

    var     newItem = new String(analyzePathsContainerContent);
    newItem = newItem.replace(/\$INDEX\$/g, analyzePathsContainerQty);
    newItem = newItem.replace(/\$NUMBER\$/g, analyzePathsContainerQty + 1);

    var currentContent = $('#analyze-paths-container').html();
    $('#analyze-paths-container').html(currentContent + newItem);

    for ( var i = 0; i < requestData.length; ++i )
    {
        $('#analyze-node-name-' + i).val(requestData[i].path);
        $('#analyze-max-' + i).val(requestData[i].max);
    }

    ++analyzePathsContainerQty;
}

function buildRequestData()
{
    var requestTab = new Array();
    for ( var i = 0; i < analyzePathsContainerQty; ++i )
    {
        var requestItem = {};
        requestItem.path = $('#analyze-node-name-' + i).val();
        requestItem.max = parseInt($('#analyze-max-' + i).val());
        requestTab.push(requestItem);
    }
    return requestTab;
}
function performUsageListing()
{
    var request = {};
    request.maxChildrenForTraversal = parseInt($('#usage-listing-max').val());
    request.startPath = $('#usage-listing-path').val();
    var json = JSON.stringify(request);
    window.open("loading.html?method=usage-listing&json=" + encodeURIComponent(json), "_blank", "status=0,toolbar=0,location=0,menubar=0,directories=0,width=500,height=300");
}

function performAnalyze()
{
    var requestTab = buildRequestData();
    var json = JSON.stringify(requestTab);
    window.open("loading.html?method=analyze&json=" + encodeURIComponent(json), "_blank", "status=0,toolbar=0,location=0,menubar=0,directories=0,width=500,height=300");
}

function openUsageListingDialog(path)
{
    $('#usage-listing-path').val(path);

    $("#get-usage-listing-dialog").dialog("option", "buttons", {
            "Cancel": function(){
                $(this).dialog("close");
            },

            "OK": function(){
                $(this).dialog("close");
                performUsageListing();
            }
        }
    );
    $("#get-usage-listing-dialog").dialog("open");
}

function openAnalyzeDialog(path)
{
    if ( !analyzePathsContainerContent )
    {
        analyzePathsContainerContent = $('#analyze-paths-container').html();
        $('#analyze-paths-container').html("");
        addToAnalyzePathsContainer();
    }

    $('#analyze-node-name-0').val(path);

    $("#analyze-dialog").dialog("option", "buttons", {
            "Cancel": function(){
                $(this).dialog("close");
            },

            "OK": function(){
                $(this).dialog("close");
                performAnalyze();
            }
        }
    );
    $("#analyze-dialog").dialog("open");
}

function initExplorer()
{
    $("#tree").dynatree({
        onActivate:function (node)
        {
            $.ajax
            (
                {
                    url: URL_EXPLORER_NODE_DATA,
                    data: {"key":node.data.key},
                    cache: false,
                    dataType: 'json',
                    success:function (data){
                        explorerSelectedPath = node.data.key;
                        explorerSelectedBytes = data.bytes;

                        $("#path").text(node.data.key);
                        $("#stat").text(data.stat);
                        $("#data-bytes").text(data.bytes);
                        $("#data-str").text(data.str);
                    }
                }
            );
        },

        selectMode:1,

        children:[
            {title:"/", isFolder:true, isLazy:true, key:"/", expand:false, noLink:true}
        ],

        onLazyRead:function (node)
        {
            node.appendAjax
                (
                    {
                        url: URL_EXPLORER_NODE,
                        data:{"key":node.data.key},
                        cache:false
                    }
                );
        },

        onClick:function (node, event)
        {
            if ( node.getEventTargetType(event) == "expander" )
            {
                node.reloadChildren(function (node, isOk){
                });
            }
            return true;
        },

        persist:false
    });

    $("#explorer-button-modify").button({
        icons:{
            primary:"ui-icon-pencil"
        }
    }).click(function(){
        var localData = explorerSelectedBytes;
        var localDataType;
        if ( !localData || !localData.length )
        {
            localData = "";
            localDataType = "string";
        }
        else
        {
            localDataType = "binary";
        }

        openModifyDialog("update", explorerSelectedPath, localData, localDataType);
        return false;
    });

    $("#explorer-button-analyze").button({
        icons:{
            primary:"ui-icon-search"
        }
    }).click(function(){
        openAnalyzeDialog(explorerSelectedPath);
        return false;
    });

    $("#explorer-button-usage-listing").button({
        icons:{
            primary:"ui-icon-gear"
        }
    }).click(function(){
        openUsageListingDialog(explorerSelectedPath);
        return false;
    });

    $("#analyze-dialog").dialog({
        modal: true,
        autoOpen: false,
        width: 500,
        resizable: false,
        title: 'Analyze'
    });

    $("#analyze-plus").button({
        text: false,
        icons:{
            primary:"ui-icon-plus"
        }
    }).click(function(){
        addToAnalyzePathsContainer();
        return false;
    });

    $("#analyze-minus").button({
        text: false,
        icons:{
            primary:"ui-icon-minus"
        }
    }).click(function(){
        removeFromAnalyzePathsContainer();
        return false;
    });

    $('#get-usage-listing-dialog').dialog({
        modal: true,
        autoOpen: false,
        width: 500,
        resizable: false,
        title: 'Usage Listing'
    });
}

