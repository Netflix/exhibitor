function completeModifyDialog(localPath, isUpdate, userName, ticketNumber, reason)
{
    var method = isUpdate ? "PUT" : "DELETE";
    var data = $('#node-data').val().trim();
    if ( $('#node-data-type').val() === 'string' )
    {
        data = toBinary(data);
    }

    headers = {};

    if (systemConfig.enableAcls == 1) {
        headers['acls'] = encodeACLsAsString();
    }

    if (systemConfig.enableTracking == 1) {
        headers['netflix-user-name'] = userName;
        headers['netflix-ticket-number'] = ticketNumber;
        headers['netflix-reason'] = reason;
    }

    $.ajax({
        type: method,
        url: URL_EXPLORER_ZNODE_BASE + localPath,
        cache: false,
        data: data,
        contentType: 'application/json',
        headers: headers,
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
    if ( systemConfig.enableTracking && ((userName.length == 0) || (ticketNumber.length == 0) || (reason.length == 0)) )
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

function openModifyDialog(action, path, data, dataType, acls)
{
    if (systemConfig.enableAcls == 1) {
        $nodeaclstable = $('#node-acls-table');
        $nodeaclstable.removeClass('ui-helper-hidden');
        $nodeaclstable.empty();
        $nodeaclstable.append($("#node-acls-table-headers").clone());

        if (acls != null) {
            $.each(acls, function(i, acl) {
                $tr = $('#get-node-data-acls-table-row').find("tr[name='aclrow']").clone();

                $schemeInput = $tr.find("input[name='schemeinput']");
                $schemeInput.val(acl.scheme);
                $schemeInput.hide();
                $tr.find("span[name='schemeinputtext']").html($schemeInput.val());

                $idInput = $tr.find("input[name='idinput']");
                $idInput.val(acl.id);
                $idInput.hide();
                $tr.find("span[name='idinputtext']").html($idInput.val());

                if ((acl.perms & PERM_READ) != 0) {
                    $tr.find("input[name='readperm']")[0].setAttribute("checked", "");
                }

                if ((acl.perms & PERM_WRITE) != 0) {
                    $tr.find("input[name='writeperm']")[0].setAttribute("checked", "");
                }

                if ((acl.perms & PERM_CREATE) != 0) {
                    $tr.find("input[name='createperm']")[0].setAttribute("checked", "");
                }

                if ((acl.perms & PERM_DELETE) != 0) {
                    $tr.find("input[name='deleteperm']")[0].setAttribute("checked", "");
                }

                if ((acl.perms & PERM_ADMIN) != 0) {
                    $tr.find("input[name='adminperm']")[0].setAttribute("checked", "");
                }

                $('#node-acls-table tr:last').after($tr);
            });
        }
    }

    $('#node-action').val(action);
    $('#node-name').val(path);
    $('#node-data').val(data);
    if ( dataType )
    {
        $('#node-data-type').val(dataType);
    }
    hideShowDataContainer();

    $("#add-acl").button({
        text: false,
        icons:{
            primary: "ui-icon-plus"
        }
    }).click(function(){
        addAcl();
        return false;
    });

    $("#remove-acls").button({
        text: false,
        icons:{
            primary: "ui-icon-minus"
        }
    }).click(function() {
        removeAcls();
        return false;
    })

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
    $('#get-node-data-dialog').dialog("open");
}

function addAcl() {
    $lastTr = $("#node-acls-table").find('tr:last');

    if ($lastTr.find("input[name='schemeinput']").val() != "" && $lastTr.find("input[name='idinput'").val() != "") {
        $tr = $('#get-node-data-acls-table-row').find("tr[name='aclrow']").clone();
        $tr.find('span[name="scheme"]').hide();
        $tr.find('span[name="id"]').hide();

        $lastTr.after($tr);
    } else {
        if ($lastTr.hidden) {
            $lastTr.toggle();
        }
    }
}

function removeAcls() {
    $('#node-acls-table tr:has(td)').each(function (index, tr) {
        if ($(tr).find("input[name='deleteacl']").prop("checked")) {
            $(tr).remove();
        }
   });
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

function encodeACLsAsString() {

    var myRows = [];

    // Loop through grabbing everything
    $('#node-acls-table tr:has(td)').each(function (index, tr) {
       var schemeText = $(tr).find("input[name='schemeinput']").val();
        var idText = $(tr).find("input[name='idinput']").val();

        // If there is no scheme or id, then we don't add this ACL
        if (schemeText != "" && idText != "") {
            var bitmask = 0;

            $(tr).find("input:checked").each(function (i, inputField) {
                bitmask = bitmask + parseInt(inputField.attributes.getNamedItem("data-bitmask").value);
            });

            myRows[index] = {
                scheme: schemeText,
                id: idText,
                bitmask: bitmask
            };
        }
   });

    return btoa(JSON.stringify(myRows));
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
        width: 590,
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
