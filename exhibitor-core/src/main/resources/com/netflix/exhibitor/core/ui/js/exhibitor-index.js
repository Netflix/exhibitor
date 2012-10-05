var ACTION_NAMES = [
    "Create-Persistent",
    "Create-Ephemeral",
    "Delete",
    "Set Data"
];

function startIndex()
{
    $.getJSON(URL_NEW_INDEX + '?ts=' + Date.now());
}

function initRestoreUI()
{
    $('#restore-open-button').button({
        icons:{
            primary:"ui-icon-folder-open"
        }
    }).click(function(){
            var radio = $('input:radio:checked[name="restore-item-radio"]');
            openIndex(radio.val());
            return false;
        });

    $('#restore-delete-button').button({
        icons:{
            primary:"ui-icon-trash"
        }
    }).click(function(){
            okCancelDialog("Delete Index", "Are you sure you to delete the selected index? This CANNOT be un-done.", function ()
            {
                var radio = $('input:radio:checked[name="restore-item-radio"]');
                $.ajax({
                    type: 'DELETE',
                    cache: false,
                    url: URL_DELETE_INDEX_BASE + radio.val()
                });
                messageDialog('Index', 'Index is marked for deletion. Check the log for details.');
            });
            return false;
        });

    $('#restore-index-button').button({
        icons:{
            primary:"ui-icon-document"
        }
    }).click(function(){
            okCancelDialog("Index", "All backups and the current ZooKeeper logs will be added to the index. Check the log for the index progress.", function(){
                startIndex();
            }, true);
            return false;
        });

    $('#index-query-dialog').dialog({
        modal: true,
        autoOpen: false,
        title: 'Search Index',
        minWidth: 400
    });
    $('#index-query-from').datepicker({
        dateFormat: "yy-mm-dd"
    });
    $('#index-query-to').datepicker({
        dateFormat: "yy-mm-dd"
    });
    $('#index-query-dialog').dialog("option", "buttons",
        {
            "Cancel":function ()
            {
                $(this).dialog("close");
            },

            "OK":function ()
            {
                var indexName = $('#index-query-dialog').attr("indexName");
                var indexHandle = $('#index-query-dialog').attr("indexHandle");
                var searchRequest = buildSearchRequestFromFilter(indexName, indexHandle);
                $(this).dialog("close");
                filterIndex(indexName, searchRequest, true);
            }
        }
    );

    $('#index-query-results-dialog').dialog({
        modal: true,
        autoOpen: false,
        title: 'Results',
        minWidth: 800,
        minHeight: 400
    });
}

var currentRestoreItemsContent = null;
var currentRestoreItemsDataTable = null;
function updateRestoreItems(selectedRadio)
{
    $.getJSON(URL_GET_INDEXES + '?ts=' + Date.now(), function(data){
        var itemsTab = data ? $.makeArray(data) : new Array();

        var needsCheck = true;
        var content = "";
        for ( var i = 0; i < itemsTab.length; ++i )
        {
            var item = itemsTab[i];
            content += '<tr>';
            content += '<td><input type="radio" id="restore-item-radio-' + i + '" name="restore-item-radio" value="' + item.name + '"';
            if ( selectedRadio )
            {
                if ( selectedRadio === item.name )
                {
                    content += " CHECKED";
                    needsCheck = false;
                }
            }
            content += '></td>';
            content += '<td>' + item.name + '</td>';
            content += '<td>' + item.from + '</td>';
            content += '<td>' + item.to + '</td>';
            content += '<td>' + item.entryCount + '</td>';
            content += '</tr>';
        }

        if ( content != currentRestoreItemsContent )
        {
            currentRestoreItemsContent = content;

            if ( currentRestoreItemsDataTable )
            {
                currentRestoreItemsDataTable.fnDestroy();
            }
            $('#restore-items-table-body').html(content);
            currentRestoreItemsDataTable = $('#restore-items-table').dataTable({
                bPaginate: false,
                bLengthChange: false,
                bSort: false,
                bInfo: false,
                bFilter: false,
                bAutoWidth: false
            });

            if ( needsCheck && (itemsTab.length > 0) )
            {
                $('#restore-item-radio-0').prop("checked", true);
            }

            $("#restore-open-button").button((itemsTab.length > 0) ? "enable" : "disable");
            $("#restore-delete-button").button((itemsTab.length > 0) ? "enable" : "disable");
        }
    });
}

var selectedIndexData = null;
function applySelectedValue(data)
{
    selectedIndexData = data;

    $('#index-query-results-selected-path').html(data.path);
    $('#index-query-results-selected-date').html(data.date);
    $('#index-query-results-selected-data-bytes').html(data.dataBytes);
    $('#index-query-results-selected-data-string').html(data.dataAsString);
}

var viewIndexTableNeedsRebuild = false;
var viewIndexTableExists = false;
var viewIndexIsFiltered = false;
function buildViewIndexTable(indexName, indexHandle)
{
    if ( viewIndexTableExists )
    {
        viewIndexTableExists = false;
        $('#index-query-results-table').dataTable().fnDestroy(true);
    }

    var template = $('#index-query-table-template').html();
    template = template.replace(/\$FOO\$/g, '');
    $('#index-query-results-dialog-content').html(template);

    var height = ($('#index-query-results-dialog').height() - 190) + "px";

    var emptyData = {
        type: 0,
        date: "",
        path: "",
        dataBytes: "",
        dataAsString: ""
    };
    applySelectedValue(emptyData);
    var selectedRowId = -1;
    var table = $('#index-query-results-table').dataTable({
        sAjaxSource: URL_INDEX_DATA_BASE + indexName + "/" + indexHandle,
        bProcessing: true,
        bServerSide: true,
        bStateSave: false,
        bDestroy: true,
        bFilter: false,
        bScrollCollapse: true,
        bSort: false,
        sScrollY: height,
        sDom: "frtiS",
        bDeferRender: true,
        iDeferLoading: false,
        aoColumns:[
            {sTitle: "TYPE"},
            {sTitle: "DATE"},
            {sTitle: "PATH"}
        ],
        fnRowCallback: function( row, data, index ) {
            if ( data.DT_RowId === selectedRowId ) {
                $(row).addClass('row_selected');
            }
            return row;
        }
    });
    viewIndexTableExists = true;

    $('#index-query-results-table tbody tr').live('click', function () {
        var id = this.id;
        if ( id != selectedRowId )
        {
            if ( selectedRowId != -1 )
            {
                $('#' + selectedRowId).removeClass('row_selected');
            }

            selectedRowId = id;
            $(this).addClass('row_selected');

            var docId = selectedRowId.split('-').pop();
            $.getJSON(URL_GET_INDEX_BASE + indexName + "/" + docId + '?ts=' + Date.now(), applySelectedValue);
            $('#index-query-clear-restore-button').button("option", "disabled", false);
        }
    });

    $('#index-query-filter-button').button({
        icons:{
            primary:"ui-icon-search"
        }
    }).click(function(){
            $('#index-query-dialog').dialog("open");
            return false;
        });
    $('#index-query-clear-filter-button').button({
        icons:{
            primary: "ui-icon-close"
        },
        disabled: true
    }).click(function(){
            var indexName = $('#index-query-dialog').attr("indexName");
            openIndex(indexName);
            return false;
        });
    $('#index-query-clear-restore-button').button({
        icons:{
            primary: "ui-icon-alert"
        },
        disabled: true
    }).click(function(){
            openModifyDialog((selectedIndexData.type == 2) ? "delete" : "update", selectedIndexData.path, selectedIndexData.dataBytes, "binary");
            return false;
        });

    $('#index-query-clear-filter-button').button("option", "disabled", !viewIndexIsFiltered);
    $('#index-query-clear-restore-button').button("option", "disabled", true);

    table.fnDraw(false);
}

function viewIndex(indexName, indexHandle)
{
    $('#index-query-dialog').attr("indexName", indexName);
    $('#index-query-dialog').attr("indexHandle", indexHandle);

    var intervalHandle;

    $('#index-query-results-dialog').bind('dialogclose', function(event, ui) {
        viewIndexTableNeedsRebuild = false;
        viewIndexIsFiltered = false;
        window.clearInterval(intervalHandle);
        $.get(URL_RELEASE_CACHE_INDEX_SEARCH_BASE + indexName + '/' + indexHandle);
    });
    $('#index-query-results-dialog').bind('dialogresizestop', function(event, ui){
        viewIndexTableNeedsRebuild = true;
    });

    $('#index-query-results-dialog').dialog("option", "title", 'Results for ' + indexName);
    $('#index-query-results-dialog').dialog("option", "maxHeight", 600);
    $('#index-query-results-dialog').dialog("open");
    viewIndexTableNeedsRebuild = true;

    intervalHandle = window.setInterval(function(){
            if ( viewIndexTableNeedsRebuild )
            {
                viewIndexTableNeedsRebuild = false;

                buildViewIndexTable($('#index-query-dialog').attr("indexName"), $('#index-query-dialog').attr("indexHandle"));

                window.setTimeout(function(){
                        var table = $('#index-query-results-table').dataTable();
                        table.fnAdjustColumnSizing();
                    },
                    2500
                );
            }
        },
        50
    );
}

function buildSearchRequestFromFilter(indexName, indexHandle)
{
    var searchRequest = {};
    searchRequest.indexName = indexName;
    searchRequest.reuseHandle = indexHandle;
        searchRequest.pathPrefix = $('#index-query-path').val().trim();
    searchRequest.operationType = ($('#index-query-type').val() === "") ? null : $('#index-query-type').val();
    searchRequest.maxResults = $('#index-query-max').val();
    searchRequest.firstDate = $('#index-query-from').datepicker("getDate");
    searchRequest.secondDate = $('#index-query-to').datepicker("getDate");

    return searchRequest;
}

function filterIndex(indexName, searchRequest, isFromFilter)
{
    var payload = JSON.stringify(searchRequest);
    $.ajax({
        type: 'POST',
        cache: false,
        url: URL_CACHE_INDEX_SEARCH,
        data: payload,
        contentType: 'application/json',
        success: function(data){
            viewIndexIsFiltered = isFromFilter;
            viewIndex(indexName, data.id);
        }
    });
}

function openIndex(indexName)
{
    var searchRequest = {};
    searchRequest.indexName = indexName;
    searchRequest.pathPrefix = "";
    searchRequest.operationType = -1;
    searchRequest.reuseHandle = null;
    searchRequest.maxResults = 0;

    viewIndexIsFiltered = false;
    filterIndex(indexName, searchRequest, false);
}
