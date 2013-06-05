function completeModifyDialog(localPath, isUpdate, userName, ticketNumber, reason)
{
    var method = isUpdate ? "PUT" : "DELETE";
    var data = $('#node-data').val().trim();
    if ( $('#node-data-type').val() === 'string' )
    {
        data = toBinary(data);
    }

    $.ajax({
        type: method,
        url: URL_EXPLORER_ZNODE_BASE + localPath,
        cache: false,
        data: data,
        contentType: 'application/json',
        headers: {
            'netflix-user-name': userName,
            'netflix-ticket-number': ticketNumber,
            'netflix-reason': reason
        },
        success:function(data)
        {
            if ( data.succeeded )
            {
                $("#tree").dynatree("getTree").reload();
                messageDialog("Success", "The change has been made.");
            }
            else
            {
                messageDialog("Error", data.message);
            }
        }
    });
}

function continueModifyDialog()
{
    var localPath = $('#node-name').val().trim();
    if ( !localPath.match(/\/.+.*/) )
    {
        messageDialog("Error", "Invalid path.");
        return;
    }

    var isUpdate = ($('#node-action').val() === "update");
    var userName = $('#node-data-user').val().trim();
    var ticketNumber = $('#node-data-ticket').val().trim();
    var reason = $('#node-data-reason').val().trim();
    if ( (userName.length == 0) || (ticketNumber.length == 0) || (reason.length == 0) )
    {
        messageDialog("Error", "The tracking fields are required.");
        return;
    }

    $('#validate-modify-node-action').html(isUpdate ? "Create/update\n" : "Delete\n");
    $('#validate-modify-node-description').html(localPath);

    $("#validate-modify-node-dialog").dialog("option", "buttons", {
            "Cancel": function(){
                $(this).dialog("close");
            },

            "OK": function(){
                $(this).dialog("close");
                completeModifyDialog(localPath, isUpdate, userName, ticketNumber, reason);
            }
        }
    );
    $('#validate-modify-node-dialog').dialog("open");
}

function openModifyDialog(action, path, data, dataType)
{
    $('#node-action').val(action);
    $('#node-name').val(path);
    $('#node-data').val(data);
    if ( dataType )
    {
        $('#node-data-type').val(dataType);
    }
    hideShowDataContainer();

    $("#get-node-data-dialog").dialog("option", "buttons", {
            'Cancel': function (){
                $(this).dialog("close");
            },

            'Next...': function (){
                $(this).dialog("close");

                continueModifyDialog();
            }
        }
    );
    $("#get-node-data-dialog").dialog("open");
}

function toBinary(str)
{
    var converted = "";
    for ( var i = 0; i < str.length; ++i )
    {
        var code = str.charCodeAt(i);
        if ( code < 16 )
        {
            converted += "0";
        }
        converted += code.toString(16) + " ";
    }
    return converted;
}

function fromBinary(str)
{
    var trimmed = "";
    for ( var i = 0; i < str.length; ++i )
    {
        var c = str.charAt(i);
        if ( c != ' ' )
        {
            trimmed += c;
        }
    }

    var converted = "";
    for ( i = 0; (i + 1) < trimmed.length; i += 2 )
    {
        var code = parseInt(trimmed.substring(i, i + 2), 16);
        converted += String.fromCharCode(code);
    }
    return converted;
}

function hideShowDataContainer()
{
    if ( $('#node-action').val() == 'update' )
    {
        $('#node-data-container').show();
    }
    else
    {
        $('#node-data-container').hide();
    }
}
function initModifyUi()
{
    $("#get-node-data-dialog").dialog({
        modal: true,
        autoOpen: false,
        width: 525,
        resizable: false,
        title: 'Modify Node'
    });

    $("#validate-modify-node-dialog").dialog({
        modal: true,
        autoOpen: false,
        width: 475,
        resizable: false,
        title: 'Modify Node'
    });

    $('#node-data').keyfilter(function(c){
        if ( $('#node-data-type').val() === 'binary' )
        {
            return ("0123456789abcdefABCDEF ".indexOf(c) >= 0);
        }
        return true;
    });
    $('#node-data-type').change(function() {
        var currentValue = $('#node-data').val();
        var newValue = "";
        if ( $('#node-data-type').val() === 'binary' )
        {
            newValue = toBinary(currentValue);
        }
        else
        {
            newValue = fromBinary(currentValue);
        }
        $('#node-data').val(newValue);
    });
    $('#node-action').change(function() {
        hideShowDataContainer();
    });
}
