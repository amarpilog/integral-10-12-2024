/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


function dataMigratorOperations(gridId) {
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    var labelObject = {};
    $.ajax({
        type: 'POST',
        traditional: true,
        dataType: 'html',
        cache: false,
        url: 'getDataMigrationMenus',
        data: {gridId: gridId},
        success: function (response) {
            ajaxStop();
            if (response != null) {
                console.log("response:::" + response);
                response = JSON.parse(response);

                var menuResult = response.menuResult;
                var tabsDiv = response.tabsDiv;
                var treeDiv = response.treeDiv;
                var contentDiv = response.contentDiv;
                $('#dataMigratorPopup').html(tabsDiv);
                $('#tabs-1').html(treeDiv + contentDiv);

                $("#dataMigratorPopup").dialog({
                    title: (labelObject['Data Piping'] != null ? labelObject['Data Piping'] : 'Data Piping'),
                    modal: true,
                    minHeight: 450,
                    maxHeight: 600,
                    minWidth: 970,
                    maxWidth: 1200,
                    fluid: true,

                    open: function () {
                        $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                        $(".visionHeaderMain").css("z-index", "999");
                        $(".visionFooterMain").css("z-index", "999");
                        $("#dataMigrationTabs").jqxTabs({theme: 'ui-redmond'});
                        //18-02
                        $("#sub-tabs-2").jqxTabs({theme: 'ui-redmond'});
                        $('#sub-tabs-2').jqxTabs('val', 0);
                        var treeMenuData = JSON.parse(menuResult);
                        var source =
                                {
                                    datatype: "json",
                                    datafields: [
                                        {name: 'MENU_ID'},
                                        {name: 'PARENT_ID'},
                                        {name: 'PARENT_MENU_ID'},
                                        {name: 'MENU_DESCRIPTION'},
                                        {name: 'icon'},
                                        {name: 'value'},
                                        {name: 'iconsize'},
                                        {name: 'TOOL_TIP'},
                                        {name: 'MAIN_DESCRIPTION'}
                                    ],
                                    localdata: treeMenuData
                                };
                        // create data adapter.
                        var dataAdapter = new $.jqx.dataAdapter(source);
                        // perform Data Binding.
                        dataAdapter.dataBind();
                        var records = dataAdapter.getRecordsHierarchy('MENU_ID',
                                'PARENT_ID',
                                'items',
                                [{name: 'MENU_DESCRIPTION', map: 'html'}]
                                );
//                        'MENU_ID','PARENT_MENU_ID','TOOL_TIP');
                        $('#dataMigratorTree').jqxTree({source: records, height: '300px', width: '180px', toggleMode: 'click', theme: 'energyblue'});
                        $('#dataMigratorTree').css('visibility', 'visible');
                        $("#dataMigratorTree").on('select', function (event) {
                            var item = $('#dataMigratorTree').jqxTree('getItem', event.args.element);
                            if (item != null && !jQuery.isEmptyObject(item) && item['value'] != null
                                    && item['value'] != ''
                                    && item['value'] != '#') {
                                eval(item['value']);
                            }

                        });
                        $('.visionProgressSteps').hide();

                    },
                    beforeClose: function (event, ui)
                    {
                        $(".visionHeaderMain").css("z-index", "99999");
                        $(".visionFooterMain").css("z-index", "99999");
                    },
                    close: function (event, ui)
                    {
                        try {
                             $("#sub-tabs-2").jqxTabs("destroy");
                        } catch (e) {
                        }
                        $(this).html("");
                        $(this).dialog("close");
                        $(this).dialog("destroy");
                    }

                });

                var fileslist = [];
                $("html").on("dragover", function (e) {
                    e.preventDefault();
                    e.stopPropagation();

                });
                $("html").on("drop", function (e) {
                    e.preventDefault();
                    e.stopPropagation();
                });
                $('.visionDMFileUploadclass').on('drop', function (e) {
                    $("#wait").css("display", "block");
                    console.log("iam in drop functionality");
                    var filetype = $('#selectedTypeName').val();
                    dmFileUpload("Y", filetype);
                    console.log("i am in drop functionality1" + filename);
                });


                $("#visionDmFileUpload").click(function (e) {
                    $("#wait").css("display", "block");
                    var filetype = $('#selectedTypeName').val();
                    console.log("iam in clickable ");
                    dmFileUpload("N", filetype);

                });

                $("#visionDMFiles").on('change', function (event) {
                    console.log("iam in files change ");
                    var filetype = $('#selectedTypeName').val();
                    dmFileUpload("N", filetype);

                });


            }



        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });
}
function getDataBase(type, typeName) {
    $(".visionDbTable :input").each(function () {
        var textid = $(this).attr("id");
        $('#' + textid).val('');
    });
    $('#selectedType').val(type);
    $('#selectedTypeName').val(typeName);
    if (type == 'DB') {
        $('.visionConnectToDbDiv').show();
        $('.visionUploadFileDiv').hide();
        $('#showConnectionType').html("<div><span style='font-weight:bold;padding-right: 25px;'>Connection Type :  </span>" + typeName + "</div>");
        $('.visionTablesComboBox').hide();
        $('#mappingColumns').hide();
        $('#connectDbTd').show();
        $('#saveDbTd').hide();
        $('.visionDatabaseMain').show();
        $('.visionProgressSteps').show();
        $('.visionProgressFilesSteps').hide();
        $('#mapColumns').removeClass("active");
        $("#fetchTables").removeClass("active");
        $('#showERPConnectionType').hide();
        $(".visionERPInner").hide();
    } else if (type == 'FILE') {

        $('.visionDatabaseMain').hide();
        $('.visionUploadFileDiv').show();
        $('.visionConnectToDbDiv').hide();
        $('#fileTypeName').html(typeName);
        $('.visionTablesComboBox').hide();
        $('#mappingColumns').hide();
        $('#visionFileMapCols').hide();
        $('.visionProgressSteps').hide();
        $('#visionDMFileUploadDiv').show();
        $('#showFileType').html("<div><span style='font-weight:bold'>File Type :  </span>" + typeName + "</div>");
        $('.VisionDMUploadFileContent').html("Upload Files(" + typeName + ") Here");
        $('.visionProgressFilesSteps').show();
        $('#mapFileCols').removeClass("active");
        $('#showERPConnectionType').hide();
        $(".visionERPInner").hide();
        $('#VisionOracleErp').hide();
    }  else if (type == 'ERP') {
        $('#visionERPMain').show();
        $('.visionDatabaseMain').hide();
        $('.visionUploadFileDiv').hide();
        $('.visionConnectToDbDiv').hide();
        // $('#fileTypeName').html(typeName);
        $('.visionTablesComboBox').hide();
        $('#mappingColumns').hide();
        $('#connectDbTd').show();
        $('#visionFileMapCols').hide();
        $('.visionProgressSteps').show();
        $('#visionDMFileUploadDiv').hide();
        $('#showERPConnectionType').html("<div><span style='font-weight:bold;padding-right: 25px;'>Connection Type :  </span>" + typeName + "</div>");
//        $('#showERPConnectionType').html("<div><span style='font-weight:bold;padding-right: 25px;'>Connection Type :  </span>" + typeName + "</div>");
        //$('.VisionDMUploadFileContent').html("Upload Files(" + typeName + ") Here");
        $('.visionProgressFilesSteps').hide();
        $('#mapColumns').removeClass("active");
        $("#fetchTables").removeClass("active");
        $('#mapFileCols').removeClass("active");
        $(".visionERPInner").show();
        $('#VisionOracleErp').hide();
        $('#showERPConnectionType').show();
    } else if (typeName == 'Oracle_ERP') {
        $('#showERPConnectionType').hide();
        $('.visionDatabaseMain').show();
        $('.visionUploadFileDiv').hide();

        // $('#fileTypeName').html(typeName);
        $('.visionTablesComboBox').hide();
        $('#mappingColumns').hide();
        $('#connectDbTd').show();
        $('#visionFileMapCols').hide();
        $('.visionProgressSteps').show();
        $('#visionDMFileUploadDiv').hide();
        $('#showConnectionType').html("<div><span style='font-weight:bold;padding-right: 25px;'>Connection Type :  </span>" + typeName + "</div>");

        //  $('#showOracleERPConnectionType').html("<div><span style='font-weight:bold'>File Type :  </span>" + typeName + "</div>");
        //$('.VisionDMUploadFileContent').html("Upload Files(" + typeName + ") Here");
        $('.visionProgressFilesSteps').hide();
        $('#mapColumns').removeClass("active");
        $("#fetchTables").removeClass("active");
        $('#mapFileCols').removeClass("active");
        $(".visionERPInner").hide();
        $('.visionConnectToDbDiv').show();
        //  $(".visionOracleERPInner").show();
        //    $('#VisionOracleErp').show();
        // $('#showOracleERPConnectionType').show();
        //  $('#VisionOracleErp').show();
    } else {

        $('.visionUploadFileDiv').hide();
        $('.visionConnectToDbDiv').hide();
        $('.visionTablesComboBox').hide();
        $('#mappingColumns').hide();
        $('.visionDatabaseMain').hide();
        $('.visionProgressSteps').hide();
        $('.visionProgressFilesSteps').hide();



    }

}
//function getDataBase(type, typeName) {
//    $(".visionDbTable :input").each(function () {
//        var textid = $(this).attr("id");
//        $('#' + textid).val('');
//    });
//    $('#selectedType').val(type);
//    $('#selectedTypeName').val(typeName);
//    if (type == 'DB') {
//        $('.visionConnectToDbDiv').show();
//        $('.visionUploadFileDiv').hide();
//        $('#showConnectionType').html("<div><span style='font-weight:bold;padding-right: 25px;'>Connection Name :  </span>" + typeName + "</div>");
//        $('.visionTablesComboBox').hide();
//        $('#mappingColumns').hide();
//        $('#connectDbTd').show();
//        $('#saveDbTd').hide();
//        $('.visionDatabaseMain').show();
//        $('.visionProgressSteps').show();
//        $('.visionProgressFilesSteps').hide();
//        $('#mapColumns').removeClass("active");
//        $("#fetchTables").removeClass("active");
//        $('#visionERPMain').hide();
//    } else if (type == 'FILE') {
//        $('.visionDatabaseMain').hide();
//        $('.visionUploadFileDiv').show();
//        $('.visionConnectToDbDiv').hide();
//        $('#fileTypeName').html(typeName);
//        $('.visionTablesComboBox').hide();
//        $('#mappingColumns').hide();
//        $('#visionFileMapCols').hide();
//        $('.visionProgressSteps').hide();
//        $('#visionDMFileUploadDiv').show();
//        $('#showFileType').html("<div><span style='font-weight:bold'>File Type :  </span>" + typeName + "</div>");
//        $('.VisionDMUploadFileContent').html("Upload Files(" + typeName + ") Here");
//        $('.visionProgressFilesSteps').show();
//        $('#mapFileCols').removeClass("active");
//        $('#visionERPMain').hide();
//    } else if (type == 'ERP') {
//        $('#visionERPMain').show();
//        $('.visionDatabaseMain').hide();
//        $('.visionUploadFileDiv').hide();
//        $('.visionConnectToDbDiv').hide();
//        // $('#fileTypeName').html(typeName);
//        $('.visionTablesComboBox').hide();
//        $('#mappingColumns').hide();
//        $('#visionFileMapCols').hide();
//        $('.visionProgressSteps').hide();
//        $('#visionDMFileUploadDiv').hide();
//        $('#showERPConnectionType').html("<div><span style='font-weight:bold'>File Type :  </span>" + typeName + "</div>");
//        //$('.VisionDMUploadFileContent').html("Upload Files(" + typeName + ") Here");
//        $('.visionProgressFilesSteps').hide();
//        $('#mapColumns').removeClass("active");
//        $("#fetchTables").removeClass("active");
//        $('#mapFileCols').removeClass("active");
//    } else {
//        $('.visionUploadFileDiv').hide();
//        $('.visionConnectToDbDiv').hide();
//        $('.visionTablesComboBox').hide();
//        $('#mappingColumns').hide();
//        $('.visionDatabaseMain').hide();
//        $('.visionProgressSteps').hide();
//        $('.visionProgressFilesSteps').hide();
//        $('#visionERPMain').hide();
//    }
//
//}
function connectDatabase() {
    var selectedDbType = $('#selectedTypeName').val();
    //18-02
    var ConnectionType = $('#selectedType').val();
    var conName = $('#DbConnectionName').val();
    var hostName = $('#DbHostName').val();
    var port = $('#DbPort').val();
    var userName = $('#DbUserName').val();
    var password = $('#DbPassword').val();
    var serviceName = $('#DbServiceName').val();
    var checkedVal = $('#checkBoxChecked').is(':checked');
    var auditId = $('#auditId').val();
    if (!conName) {
        $('#DbConnectionNameError').html("Please enter Connection Name");
    }
    if (!hostName) {
        $('#DbHostNameError').html("Please enter Host Name");
    }
    if (!port) {
        $('#DbPortError').html("Please enter Port No");
    }
    if (!userName) {
        $('#DbUserNameError').html("Please enter Username");
    }
    if (!password) {
        $('#DbPasswordError').html("Please enter Password");
    }
    if (!serviceName) {
        $('#DbServiceNameError').html("Please enter Service Name");
    } else {

        $('.dataMigrationInputError').hide();
        $('.visionDataMigrationError').hide();
        //18-02
        connectDatabaseProcess(conName, hostName, port, userName, password, serviceName, checkedVal, selectedDbType, auditId, "", ConnectionType);
    }
}
//18-02
function connectDatabaseProcess(conName, hostName, port, userName, password, serviceName, checkedVal, selectedDbType, auditId, filerTableValue, ConnectionType) {
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'html',
        cache: false,
        url: 'getConnectionDetails',
        async: true,
        data: {
            connectionName: conName,
            hostName: hostName,
            port: port,
            userName: userName,
            password: password,
            serviceName: serviceName,
            checkedVal: checkedVal,
            selectedItemLabel: selectedDbType,
            auditId: auditId,
            filerTableValue: filerTableValue,
            ConnectionType: ConnectionType

        },
        success: function (response) {
            ajaxStop();
            if (response != null) {
                var responseObj = JSON.parse(response);
                if (responseObj != null && responseObj.connectionFlag == 'Y') {
                    var dbDetails = JSON.stringify(responseObj.dbObj);
                    $('#dbDetails').val(dbDetails);
                    var tablesArray = responseObj.tablesList;
                    var dragAnddropDiv = "<div class=\"sourceTablesHeader\">Total available tables</div><input id='searchTables' class='visionTablesSearchInput' type='text' placeholder='Search for more tables..' onkeyup='searchTables()'><img src='images/crossicon.png' title='Clear Data' onclick='clearTableInput()' class='visionClearFieldBtn'><div id=\"sourceFields\" class=\"sourceFields\" style='margin-top: 30px;height:272px;'>" + tablesArray + "</div>"
                            + " <div class=\"destinationTablesHeader\">Selected tables</div><div id=\"destinationFields\" class=\"destinationFields\">"
                            + "</div><div class=\"tablesProcessButton\"><input type='button' value='Process' name='Process' id='processTablesBtn' onclick = 'processTables()' class='visionTablesProcessButton'></div>";
                    $("#fieldChooser").html(dragAnddropDiv);
//                    $("#dataMigratorTree").jqxTree('val', selectedDbType);
//                    $("#dataMigratorTree").jqxTree('expandItem', $("#sidemenu_DM_DATABASE")[0]);
                    $('#dataMigratorTree').jqxTree('collapseAll');
                    var items = $('#dataMigratorTree').jqxTree('getItems');
                    var label = [];
                    var id = [];
                    for (var i = 0; i < items.length; i++) {
                        var allItems = items[i];
                        label.push(allItems.label);
                        id.push(allItems.id);
                        if (selectedDbType != null && selectedDbType == label[i]) {
                            $("#dataMigratorTree").jqxTree('expandItem', $("#" + id[i])[0]);
                            $("#dataMigratorTree").jqxTree('selectItem', $("#" + id[i])[0]);
                            break;

                        }

                    }
                    $('#fieldChooser').show();
//                    $('.visionTablesComboBox').show();
//                    $('.visionUploadFileDiv').hide();
                    $('.visionDatabaseMain').show();
                    $('#visionUploadDocs').hide();
                    $('.visionConnectToDbDiv').hide();
                    $('#mappingColumns').hide();
                    $('#visionERPMain').hide();
                    dragTables();
//                    var sourceFields = $("#sourceFields");
//                    var destinationFields = $("#destinationFields");
//                    var chooser = $("#fieldChooser").fieldChooser(sourceFields, destinationFields);
                    var tabsOpenValue = $('#dataMigrationTabs').val();
                    if (tabsOpenValue != null && tabsOpenValue != '' && tabsOpenValue == 1) {
                        $('#dataMigrationTabs').jqxTabs('val', 0);
                    }
                } else {
                    showMessagePopup(responseObj.connectionMessage);

                }
                $('.visionProgressSteps').show();
                $("#fetchTables").addClass("active");
                $('.visionProgressFilesSteps').hide();

            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });
}
//18-02
function savedConnection() {
    $('#sub-tabs-2').jqxTabs('val', 0);
    showSavedConnection();
}
//18-02
function showSavedConnection() {

    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'html',
        cache: false,
        url: 'getSavedConnections',
        async: true,
        success: function (response) {
            ajaxStop();
            if (response != null) {

                $('#savedConnectionTable').html(response);
                $('#savedConnectionTable').show();
                $('.savedErrorMsg').hide();
                $('#ERPSavedConnectionTable').hide();
                $('#onlineServiceSavedConnectionTable').hide();
            } else {
                $('.savedErrorMsg').show();
            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });
}
function showERPSavedConnection() {
    $('#ERPSavedConnectionTable').show();
    $('#savedConnectionTable').hide();
    $('#ERPSavedConnectionTable').html("<p>No saved Connections</p>");
    $('#onlineServiceSavedConnectionTable').hide();

}
function showOnlineServiceSavedConnection() {
    $('#ERPSavedConnectionTable').hide();
    $('#savedConnectionTable').hide();
    $('#onlineServiceSavedConnectionTable').show();
    $('#onlineServiceSavedConnectionTable').html("<p>No saved Connections</p>");


}


function processTables() {
    var tablesArray = [];
    var gridId = $('#gridIdValue').val();

    var dbDetails = $('#dbDetails').val();
    dbDetails = JSON.parse(dbDetails);
    // var conName = dbDetails.conName;
    var hostName = dbDetails.hostName;
    var port = dbDetails.port;
    var userName = dbDetails.userName;
    var password = dbDetails.password;
    var serviceName = dbDetails.serviceName;
    var selectedDbType = dbDetails.selectedItemLabel;
    $('#destinationFields div').each(function () {
        var tableName = $(this).attr('data-table-name');
        tablesArray.push(tableName);
    })
    var sourceex = $("#" + gridId).jqxGrid('source');
    var dataFeilds = [];
    dataFeilds = sourceex._source.datafields;
    var tableName = sourceex._source.data.tableName;

    var destnTableLength = $('#destinationFields').children('div').length;
    var tablesErrMsg = '';
    if (destnTableLength != null && destnTableLength > 5) {
        tablesErrMsg = "Please select 5 tables only ";
        showMessagePopup(tablesErrMsg);

    } else if (!(destnTableLength != null && destnTableLength != 0)) {
        tablesErrMsg = "Please select atleast one table to process ";
        showMessagePopup(tablesErrMsg);

    } else {


        $.ajax({
            type: 'post',
            traditional: true,
            dataType: 'html',
            cache: false,
            url: 'fetchTableColumns',
            async: true,
            data: {tablesArray: JSON.stringify(tablesArray),
                hostName: hostName,
                port: port,
                userName: userName,
                password: password,
                serviceName: serviceName,
                selectedItemLabel: selectedDbType,
                gridId: gridId,
                tableName: tableName,
                dataFeilds: JSON.stringify(dataFeilds)},

            success: function (response) {
                ajaxStop();
                if (response != null) {
                    $('.visionTablesComboBox').hide();
                    $('.visionUploadFileDiv').hide();
                    $('.visionConnectToDbDiv').hide();
                    $('#visionERPMain').hide();
                    var response = JSON.parse(response);
                    if (response != null && response.connectionFlag == 'Y') {
                        var columnStr = response['columnStr'];
                        var tablesStr = response['selectedTables'];
                        $('#mappingColumns').show();
                        $('#showSourceTablesList').html(tablesStr);
                        $('#MappedTable').html(columnStr);
                        var matchedSelectStr = response['matchedSelectStr'];
                        $('#mapColumns').addClass("active");
                        $('.visionProgressFilesSteps').hide();

                    } else {
                        showMessagePopup(response.connectionMessage);

                    }


                }
            },
            error: function (e)
            {
                sessionTimeout(e);
            }

        });

    }






}
function fetchSelectedColumns() {
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    var labelObject = {};
    var dbDetails = $('#dbDetails').val();
    var whereCondition = $("#TABLE_INPUT0").val();
    dbDetails = JSON.parse(dbDetails);
    var hostName = dbDetails.hostName;
    var port = dbDetails.port;
    var client = dbDetails.ClientId;
    var ERPSystemId = dbDetails.ERPSystemId;
    var LanguageId = dbDetails.LanguageId;
    var userName = dbDetails.userName;
    var password = dbDetails.password;
    var serviceName = dbDetails.serviceName;
    var selectedDbType = dbDetails.selectedItemLabel;
    var i = 0;
    var colsObj = {};
    var defaultValObj = {};
    var destColArr = [];
    var flag = true;
    $("#sourceDestColsTableId tbody tr").each(function () {
        var sourceColNameStr = $("#SOURCE_SELECT_" + i).val();
        var destColNameStr = $("#DEST_SELECT_" + i).val();
        var sourceColName = "";
        var defaultVal = $("#DEFAULTVALUES_" + i).val();
        if (sourceColNameStr != null) {
            sourceColName = sourceColNameStr.split(":")[1];
            if (destColNameStr != null) {
//                destColObj["TABLE_NAME"] = destColNameStr.split(":")[0];
//                destColObj["COLUMN_NAME"] = destColNameStr.split(":")[1];

                console.log("destColArr::::" + destColArr);
                var col = destColArr.includes(destColNameStr);
                if (destColArr != null && destColArr != '' && destColArr.includes(destColNameStr)) {
                    var message = "Mapped Column is already exist";
                    showMessagePopup(message);

                    flag = false;
                    return;
                } else {
                    destColArr.push(destColNameStr);
                    if (destColNameStr != null && destColNameStr != '' && destColNameStr != 'Select') {
                        colsObj[sourceColName] = destColNameStr;
                    }

                }

            }

            if (defaultVal != null && defaultVal != '')
                defaultValObj[sourceColName] = defaultVal;
        }

        ++i;

    });

    var j = 0;
    var tablesObj = {};

    $("#selectedTables tbody tr").each(function () {
        var sourceTable = "";
        var sourceTableName = $("#SELECT_TABLE_" + j).text();
        var tableInputValue = $("#TABLE_INPUT" + j).val();
        tablesObj[sourceTableName] = tableInputValue;
        ++j;
    });

    console.log("defaultValObj:::::::" + defaultValObj);
    var gridId = $('#gridIdValue').val();
    var sourceex = $("#" + gridId).jqxGrid('source');
    var tableName = sourceex._source.data.tableName;
    if (flag) {
        if (colsObj != null && colsObj != '') {
            $.ajax({
                type: "post",
                traditional: true,
                dataType: 'html',
                cache: false,
                url: "mappingSourceColsWithDestCols",
                data: {hostName: hostName,
                    port: port,
                    ClientId: client,
                    ERPSystemId: ERPSystemId,
                    LanguageId: LanguageId,
                    userName: userName,
                    password: password,
                    serviceName: serviceName,
                    selectedItemLabel: selectedDbType,
                    tableName: tableName,
                    columnsObj: JSON.stringify(colsObj),
                    tablesObj: JSON.stringify(tablesObj),
                    defaultValObj: JSON.stringify(defaultValObj),
                    whereCondition: whereCondition
                },
                success: function (response) {
                    if (response != null)
                    {

                        var result = JSON.parse(response);
                        if (result != null && result.connectionFlag == 'Y')
                        {
                            //var message = result.Message;
                            $('#dialog').html(result.Message);



                        } else {
                            $('#dialog').html(result.connectionMessage);
                        }
                        $("#dialog").dialog({
                            title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
                            modal: true,
                            width: 300,
                            height: 135,
                            fluid: true,
                            buttons: [{
                                    text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                                    click: function () {
                                        $('.visionProgressSteps').hide();
                                        $('.visionProgressFilesSteps').hide();
                                        $('#mappingColumns').hide();
                                        $(this).html("");
                                        $(this).dialog("close");
                                        $(this).dialog("destroy");

                                    }

                                }],
                            open: function () {
                                $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                                $(".visionHeaderMain").css("z-index", "999");
                                $(".visionFooterMain").css("z-index", "999");

                            },
                            beforeClose: function (event, ui)
                            {
                                $(".visionHeaderMain").css("z-index", "99999");
                                $(".visionFooterMain").css("z-index", "99999");
                            }
                        });
                    }
                },
                error: function (e) {
                    sessionTimeout(e);
                }
            });

        }
    }
}
//function fetchSelectedColumns() {
//    try {
//        labelObject = JSON.parse($("#labelObjectHidden").val());
//    } catch (e) {
//
//    }
//    var labelObject = {};
//    var dbDetails = $('#dbDetails').val();
//    dbDetails = JSON.parse(dbDetails);
//    var hostName = dbDetails.hostName;
//    var port = dbDetails.port;
//    var userName = dbDetails.userName;
//    var password = dbDetails.password;
//    var serviceName = dbDetails.serviceName;
//    var selectedDbType = dbDetails.selectedItemLabel;
//    var i = 0;
//    var colsObj = {};
//    var defaultValObj = {};
//    var destColArr = [];
//    var flag = true;
//    $("#sourceDestColsTableId tbody tr").each(function () {
//        var sourceColNameStr = $("#SOURCE_SELECT_" + i).val();
//        var destColNameStr = $("#DEST_SELECT_" + i).val();
//        var sourceColName = "";
//        var defaultVal = $("#DEFAULTVALUES_" + i).val();
//        if (sourceColNameStr != null) {
//            sourceColName = sourceColNameStr.split(":")[1];
//            if (destColNameStr != null) {
////                destColObj["TABLE_NAME"] = destColNameStr.split(":")[0];
////                destColObj["COLUMN_NAME"] = destColNameStr.split(":")[1];
//
//                console.log("destColArr::::" + destColArr);
//                var col = destColArr.includes(destColNameStr);
//                if (destColArr != null && destColArr != '' && destColArr.includes(destColNameStr)) {
//                    var message = "Mapped Column is already exist";
//                    showMessagePopup(message);
//
//                    flag = false;
//                    return;
//                } else {
//                 
//                    if (destColNameStr != null && destColNameStr != '' && destColNameStr != 'Select') {
//                           destColArr.push(destColNameStr);
//                        colsObj[sourceColName] = destColNameStr;
//                    }
//
//                }
//
//            }
//
//            if (defaultVal != null && defaultVal != '')
//                defaultValObj[sourceColName] = defaultVal;
//        }
//
//        ++i;
//
//    });
//
//    var j = 0;
//    var tablesObj = {};
//
//    $("#selectedTables tbody tr").each(function () {
//        var sourceTable = "";
//        var sourceTableName = $("#SELECT_TABLE_" + j).text();
//        var tableInputValue = $("#TABLE_INPUT" + j).val();
//        tablesObj[sourceTableName] = tableInputValue;
//        ++j;
//    });
//
//    console.log("defaultValObj:::::::" + defaultValObj);
//    var gridId = $('#gridIdValue').val();
//    var sourceex = $("#" + gridId).jqxGrid('source');
//    var tableName = sourceex._source.data.tableName;
//    if (flag) {
//        if (colsObj != null && colsObj != '') {
//            $.ajax({
//                type: "post",
//                traditional: true,
//                dataType: 'html',
//                cache: false,
//                url: "mappingSourceColsWithDestCols",
//                data: {hostName: hostName,
//                    port: port,
//                    userName: userName,
//                    password: password,
//                    serviceName: serviceName,
//                    selectedItemLabel: selectedDbType,
//                    tableName: tableName,
//                    columnsObj: JSON.stringify(colsObj),
//                    tablesObj: JSON.stringify(tablesObj),
//                    defaultValObj: JSON.stringify(defaultValObj)
//                },
//                success: function (response) {
//                    if (response != null)
//                    {
//
//                        var result = JSON.parse(response);
//                        if (result != null && result.connectionFlag == 'Y')
//                        {
//                            //var message = result.Message;
//                            $('#dialog').html(result.Message);
//
//
//
//                        } else {
//                            $('#dialog').html(result.connectionMessage);
//                        }
//                        $("#dialog").dialog({
//                            title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
//                            modal: true,
//                            width: 300,
//                            height: 135,
//                            fluid: true,
//                            buttons: [{
//                                    text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
//                                    click: function () {
//                                        $('.visionProgressSteps').hide();
//                                        $('.visionProgressFilesSteps').hide();
//                                        $('#mappingColumns').hide();
//                                        $('#visionERPMain').hide();
//                                        $(this).html("");
//                                        $(this).dialog("close");
//                                        $(this).dialog("destroy");
//
//                                    }
//
//                                }],
//                            open: function () {
//                                $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
//                                $(".visionHeaderMain").css("z-index", "999");
//                                $(".visionFooterMain").css("z-index", "999");
//
//                            },
//                            beforeClose: function (event, ui)
//                            {
//                                $(".visionHeaderMain").css("z-index", "99999");
//                                $(".visionFooterMain").css("z-index", "99999");
//                            }
//                        });
//                    }
//                },
//                error: function (e) {
//                    sessionTimeout(e);
//                }
//            });
//
//        }
//    }
//
//
//
//}

function editDBConnection(conName, hostName, port, userName, password, serviceName, checkedVal, selectedDbType, auditId) {
    $('#showConnectionType').html("<div><span style='font-weight:bold;padding-right: 25px;'>Connection Name :  </span>" + selectedDbType + "</div>");
    $('#selectedTypeName').val(selectedDbType);
    $('#DbConnectionName').val(conName);
    $('#DbHostName').val(hostName);
    $('#DbPort').val(port);
    $('#DbUserName').val(userName);
    $('#DbPassword').val(password);
    $('#DbServiceName').val(serviceName);
    $('#connectionName').html(selectedDbType);
    $('#auditId').val(auditId);
    $('#visionUploadDocs').hide();
    $('.visionProgressSteps').show();
    $('#mapColumns').removeClass("active");
    $("#fetchTables").removeClass("active");
    $('.dataMigrationInputError').hide();
    $('.visionDataMigrationError').hide();
    if (auditId != null && auditId != '') {
        $('.visionConnectToDbDiv').show();
        $('.visionDatabaseMain').show();
        var tabsOpenValue = $('#dataMigrationTabs').val();
//        var saveBtn = '<input type=\"button\" value=\"Save\" name=\"Save\" class=\"visionInputSaveButton\" onclick = \'updateDatabaseDetails("' + conName + '",  "' + hostName + '" , "' + port + '", "' + userName + '", "' + password + '", "' + serviceName + '", "' + checkedVal + '", "' + selectedDbType + '", "' + auditId + '")\'>';
//        $('#saveDbTd').html(saveBtn);
//        $('#saveDbTd').show();
        $('#connectDbTd').show();
        $("#fieldChooser").hide();
        $('#mappingColumns').hide();
        $('.visionProgressFilesSteps').hide();
        $('#visionERPMain').hide();
    }

    if (tabsOpenValue != null && tabsOpenValue != '' && tabsOpenValue == 1) {
        $('#dataMigrationTabs').jqxTabs('val', 0);

    }

}

//function updateDatabaseDetails(conName, hostName, port, userName, password, serviceName, checkedVal, selectedDbType, auditId) {
//
//    var conName = $('#DbConnectionName').val();
//    var hostName = $('#DbHostName').val();
//    var port = $('#DbPort').val();
//    var userName = $('#DbUserName').val();
//    var password = $('#DbPassword').val();
//    var serviceName = $('#DbServiceName').val();
//    //var auditId =  $('#auditId').val(auditId);
//    $.ajax({
//        type: 'post',
//        traditional: true,
//        dataType: 'html',
//        cache: false,
//        url: 'updateConnectionDetails',
//        async: true,
//        data: {
//            connectionName: conName,
//            hostName: hostName,
//            port: port,
//            userName: userName,
//            password: password,
//            serviceName: serviceName,
//            checkedVal: checkedVal,
//            selectedItemLabel: selectedDbType,
//            auditId: auditId
//
//        },
//        success: function (response) {
//            if (response != null) {
//                var result = JSON.parse(response);
//                $("#dialog").html(result.updateMessage);
//                $("#dialog").dialog({
//                    title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
//                    modal: true,
//                    width: 300,
//                    height: 135,
//                    fluid: true,
//                    buttons: [{
//                            text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
//                            click: function () {
//                                $('.visionConnectToDbDiv').hide();
//                                $(this).html("");
//                                $(this).dialog("close");
//                                $(this).dialog("destroy");
//
//                            }
//
//                        }],
//                    open: function () {
//                        $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
//                        $(".visionHeaderMain").css("z-index", "999");
//                        $(".visionFooterMain").css("z-index", "999");
//
//                    },
//                    beforeClose: function (event, ui)
//                    {
//                        $(".visionHeaderMain").css("z-index", "99999");
//                        $(".visionFooterMain").css("z-index", "99999");
//                    }
//                });
//            }
//        },
//        error: function (e) {
//            sessionTimeout(e);
//        }
//    });
//}
function deleteDBConnection(conName, hostName, port, userName, password, serviceName, checkedVal, selectedDbType, auditId) {
    //08/01
    $("#dialog").html("Are you sure you want to delete this connection?");
    $("#dialog").dialog({
        modal: true,
        title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
        height: 'auto',
        minHeight: 'auto',
        minWidth: 350,
        maxWidth: 'auto',
        fluid: true,
        buttons: [{
                text: (labelObject['Yes'] != null ? labelObject['Yes'] : 'Yes'),
                click: function () {
                    $(this).html("");
                    $(this).dialog("close");
                    $(this).dialog("destroy");
                    $.ajax({
                        type: 'post',
                        traditional: true,
                        dataType: 'html',
                        cache: false,
                        url: 'deleteDatabaseDetails',
                        async: true,
                        data: {
                            connectionName: conName,
                            hostName: hostName,
                            port: port,
                            userName: userName,
                            password: password,
                            serviceName: serviceName,
                            checkedVal: checkedVal,
                            selectedItemLabel: selectedDbType,
                            auditId: auditId

                        },
                        success: function (response) {
                            if (response != null) {
                                $("#dialog").html(response);
                                $("#dialog").dialog({
                                    title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
                                    modal: true,
                                    width: 300,
                                    height: 135,
                                    fluid: true,
                                    buttons: [{
                                            text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                                            click: function () {
                                                showSavedConnection();
                                                $(this).html("");
                                                $(this).dialog("close");
                                                $(this).dialog("destroy");

                                            }

                                        }],
                                    open: function () {
                                        $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                                        $(".visionHeaderMain").css("z-index", "999");
                                        $(".visionFooterMain").css("z-index", "999");

                                    },
                                    beforeClose: function (event, ui)
                                    {
                                        $(".visionHeaderMain").css("z-index", "99999");
                                        $(".visionFooterMain").css("z-index", "99999");
                                    }
                                });
                            }
                        },
                        error: function (e) {
                            sessionTimeout(e);
                        }
                    });

                }
            },
            {
                text: (labelObject['No'] != null ? labelObject['No'] : 'No'),
                click: function () {
                    $(this).html("");
                    $(this).dialog("close");


                }
            }
        ],
        open: function () {
            $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
            $(this).closest(".ui-dialog").find(".ui-button").eq(2).addClass("dialogno");
            $(this).closest(".ui-dialog").addClass("visionCommonDialog");
            $(".visionHeaderMain").css("z-index", "999");
            $(".visionFooterMain").css("z-index", "999");
        },
        beforeClose: function (event, ui)
        {
            $(".visionHeaderMain").css("z-index", "99999");
            $(".visionFooterMain").css("z-index", "99999");

        }
    });

}



/*DM file upload start*/
function dmFileUpload(draganddropInd, filetype) {

    var validExtension = [];
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }

    var secretKey = $('meta[name=keygeneration]').attr("content");
    var params = {};
    var gridId = $('#gridIdStr').val();

    var browseId = "#importDMFile";
    var sourceex = $("#" + gridId).jqxGrid('source');
    var dataFeilds = [];
    dataFeilds = sourceex._source.datafields;

    params['gridId'] = $('#gridIdStr').val();
    params['selectedGridId'] = $('#gridIdStr').val();
    params['tableName'] = $('#tableName').val();
    params['selectedFiletype'] = filetype;
    var dataFeildsStr = JSON.stringify(dataFeilds);
    if (dataFeildsStr != null && dataFeildsStr != '' && dataFeildsStr != '{}') {
        dataFeildsStr = CryptoJS.AES.encrypt(dataFeildsStr, secretKey);
        params['dataFeilds'] = dataFeildsStr;
    }


    $(browseId).ajaxfileupload({
        'action': 'importDMFile',
        params: params,

        'onComplete': function (response) {
            $("#Loader").css("display", "none");
            $("body").css({"pointer-events": "auto"});
            var status = response.status;
            var message = response.message;
            if (message != null && message.indexOf("{") > -1 && message.indexOf("}") > -1) {
                message = message.substring(message.indexOf("{"), message.indexOf("}") + 1);
            }
            message = JSON.parse(message);
            // var tableStr = message.columnStr;
            var tableStr = jQuery('<div />').html(message.columnStr).text();
            var filePath = message.filePath;
            $('#filePathValue').val(filePath);
            var result = message.result;
            if (status == true && result != null && result == "File imported successfully") {
                $("#importDMFile").remove();

                var fileImprtDiv = "<input type='file' name='importDMFile' id='importDMFile' style='display:none'>";
                $("#visionDmFileUpload").parent().append(fileImprtDiv);
                var processBtn = "<input type='button' value= 'process' id= 'processMappedFileCols' onclick = 'processMappedFileCols()' class='visionProcessMappedFileBtn'>";
                $('#visionFileMapCols').html(tableStr + processBtn);

                $('#visionDMFileUploadDiv').hide();
                $('#visionFileMapCols').show();
                $('.visionProgressFilesSteps').show();
                $('#mapFileCols').addClass("active");
            } else {
                $("#dialog").html(result);
                $("#dialog").dialog({
                    modal: true,
                    width: 270,
                    height: 135,
                    fluid: true,
                    buttons: {
                        Ok: function () {
                            $(this).dialog("close");


                        }
                    },
                    open: function () {
                        $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                        $(".visionHeaderMain").css("z-index", "999");
                        $(".visionFooterMain").css("z-index", "999");
                    },
                    beforeClose: function (event, ui)
                    {
                        $("#importDMFile").remove();
                        var fileImprtDiv = "<input type='file' name='importDMFile' id='importDMFile' style='display:none'>";
                        $("#visionDmFileUpload").parent().append(fileImprtDiv);
                        $(".visionHeaderMain").css("z-index", "99999");
                        $(".visionFooterMain").css("z-index", "99999");
                    }
                });


            }
            console.log("On Complete::");

        },
        'onStart': function () {
            $('#Loader').show();
            $("body").css({"pointer-events": "none"});
            $("#Loader").css("display", "block");
            // $('#message').hide();
        }
    });

    if (draganddropInd == 'N') {
        $("#importDMFile").click();

        //var filename = 
    }
}

function processMappedFileCols() {
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    var labelObject = {};
    var i = 0;
    var mappedFileColsObj = {};

    var defaultValObj = {};
    $("#sourceDestColsFilesTableId tbody tr").each(function () {
        var destColNameStr = $("#DEST_FILE_SELECT_" + i).val();
        var sourceColNameStr = $("#SOURCE_FILE_SELECT_" + i).val();

        var defaultVal = $("#DEFAULTVALUESFILES_" + i).val();
        if (destColNameStr != null && destColNameStr != '' && destColNameStr != 'Select'
                && sourceColNameStr != null && sourceColNameStr != '' && sourceColNameStr != 'Select') {
            mappedFileColsObj[destColNameStr] = sourceColNameStr;
        }
        if (defaultVal != null && defaultVal != '') {
            defaultValObj[destColNameStr] = defaultVal;
        }

        ++i;
    });
    var filePath = $('#filePathValue').val();
    var gridId = $('#gridIdStr').val();
    var tableName = $('#tableName').val();
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'html',
        cache: false,
        url: 'mappingSourceFileColsWithDestCols',
        async: true,
        data: {
            'mappedFileColsObj': JSON.stringify(mappedFileColsObj),
            'defaultValObj': JSON.stringify(defaultValObj),
            'filePath': filePath,
            'gridId': gridId,
            'tableName': tableName
        },
        success: function (response) {
            if (response != null) {
                $("#dialog").html(response);
                $("#dialog").dialog({
                    title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
                    modal: true,
                    width: 300,
                    height: 135,
                    fluid: true,
                    buttons: [{
                            text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                            click: function () {
                                $('#visionFileMapCols').hide();
                                //   $('.visionUploadFileDiv').css('margin-top', '20%');
                                $('#visionDMFileUploadDiv').show();
                                $(this).html("");
                                $(this).dialog("close");
                                $(this).dialog("destroy");

                            }

                        }],
                    open: function () {
                        $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                        $(".visionHeaderMain").css("z-index", "999");
                        $(".visionFooterMain").css("z-index", "999");

                    },
                    beforeClose: function (event, ui)
                    {
                        $(".visionHeaderMain").css("z-index", "99999");
                        $(".visionFooterMain").css("z-index", "99999");
                    }
                });
            }
        },
        error: function (e) {
            sessionTimeout(e);
        }
    });

}

function showMessagePopup(message) {
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    var labelObject = {};
    $("#dialog").html(message);
    $("#dialog").dialog({
        title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
        modal: true,
        width: 300,
        height: 135,
        fluid: true,
        buttons: [{
                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                click: function () {
                    $(this).html("");
                    $(this).dialog("close");
                    $(this).dialog("destroy");

                }

            }],
        open: function () {
            $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
            $(".visionHeaderMain").css("z-index", "999");
            $(".visionFooterMain").css("z-index", "999");

        },
        beforeClose: function (event, ui)
        {
            $(".visionHeaderMain").css("z-index", "99999");
            $(".visionFooterMain").css("z-index", "99999");
        }
    });
}



function searchTables() {
    var filerTableValue = $('#searchTables').val().toLowerCase();
    var filerTableValueLen = $('#searchTables').val().length;
//    $("#sourceFields div").filter(function () {
//        $(this).toggle($(this).text().toLowerCase().indexOf(filerTableValue) > -1)
//    });

    if ((filerTableValueLen != null && filerTableValueLen >= 3) || filerTableValueLen == 0) {

        var dbDetails = $('#dbDetails').val();
        dbDetails = JSON.parse(dbDetails);
        var conName = dbDetails.conName;
        var hostName = dbDetails.hostName;
        var port = dbDetails.port;
        var userName = dbDetails.userName;
        var password = dbDetails.password;
        var serviceName = dbDetails.serviceName;
        var selectedDbType = dbDetails.selectedItemLabel;
        var checkedVal = $('#checkBoxChecked').is(':checked');
        var auditId = $('#auditId').val();
        getFilterTables(conName, hostName, port, userName, password, serviceName, false, selectedDbType, auditId, filerTableValue);

    }


}
function getFilterTables(conName, hostName, port, userName, password, serviceName, checkedVal, selectedDbType, auditId, filerTableValue) {
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'html',
        cache: false,
        url: 'getConnectionDetails',
        async: true,
        data: {
            connectionName: conName,
            hostName: hostName,
            port: port,
            userName: userName,
            password: password,
            serviceName: serviceName,
            checkedVal: checkedVal,
            selectedItemLabel: selectedDbType,
            auditId: auditId,
            filerTableValue: filerTableValue
        },
        success: function (response) {
            ajaxStop();
            if (response != null) {
                var responseObj = JSON.parse(response);
                if (responseObj != null && responseObj.connectionFlag == 'Y') {
                    $("#sourceFields").html("");
                    var tablesArray = responseObj.tablesList;
                    $("#sourceFields").html(tablesArray);
                    var dbDetails = JSON.stringify(responseObj.dbObj);
                    $('#dbDetails').val(dbDetails);
                    dragTables();
//                    var sourceFields = $("#sourceFields");
//                    var destinationFields = $("#destinationFields");
//                    var chooser = $("#fieldChooser").fieldChooser(sourceFields, destinationFields);
//                    var tabsOpenValue = $('#dataMigrationTabs').val();
//                    if (tabsOpenValue != null && tabsOpenValue != '' && tabsOpenValue == 1) {
//                        $('#dataMigrationTabs').jqxTabs('val', 0);
//                    }
                } else {
                    showMessagePopup(responseObj.connectionMessage);

                }
//                $('.visionProgressSteps').show();
//                $("#fetchTables").addClass("active");
//                $('.visionProgressFilesSteps').hide();

            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });
}

function clearTableInput() {
    $("#searchTables").val('');
    searchTables();
}

function dragTables() {
    $("#sourceFields div").draggable({
        revert: "invalid",
        refreshPositions: true,
        cursor: 'move',
        zindex: false,
        opacity: false,
        appendto: "parent",
        drag: function (event, ui) {
            ui.helper.addClass("draggableTable");
        },
        stop: function (event, ui) {
            ui.helper.removeClass("draggableTable");

        }
    });
    $("#destinationFields").droppable({
        revert: "invalid",
        refreshPositions: true,
        cursor: 'move',
        drop: function (event, ui) {
            if ($("#destinationFields div").length == 0) {
                $("#destinationFields").html("");
            }
            ui.draggable.addClass("droppedTable");

            var droppableId = ui.draggable[0].id;
            $("#" + droppableId).children().show();
            $("#destinationFields").append(ui.draggable);



        }
    });
}

function moveToSource(table) {
    var tablename = table.id;
    $("#sourceFields").append($("#" + tablename));
    $("#" + tablename).children().hide();
}

function connectErpDatabase() {
    var ConnectionType = $('#selectedType').val();
    var selectedDbType = $('#selectedTypeName').val();
    var connectionName = $('#ErpDbConnectionName').val();
    var ClientId = $('#ERPClientName').val();
    var hostName = $('#ERPHostName').val();
    var userName = $('#ERPUserName').val();
    var password = $('#ERPPassword').val();
    var LanguageId = $('#ERPLanguageId').val();
    var ERPSystemId = $('#ERPSystemId').val();
    var checkedVal = $('#checkBoxChecked').is(':checked');
    var auditId = $('#auditId').val();
    if (!ClientId) {
        $('#ERPClientNameError').html("Please enter ClientId");
    }
    if (!hostName) {
        $('#ERPHostNameError').html("Please enter Host Name");
    }
    if (!userName) {
        $('#ERPUserNameError').html("Please enter Username");
    }
    if (!password) {
        $('#ERPPasswordError').html("Please enter Password");
    }
    if (!LanguageId) {
        $('#ERPLanguageIdError').html("Please enter LanguageId");
    }
    if (!ERPSystemId) {
        $('#ERPSystemIdError').html("Please enter ERPSystemId");
    } else {

        $('.dataMigrationInputError').hide();
        $('.visionDataMigrationError').hide();
        connectErpDatabaseProcess(connectionName, hostName, ClientId, userName, password, ERPSystemId, checkedVal, selectedDbType, LanguageId, auditId, "", ConnectionType);
    }
}
function connectErpDatabaseProcess(connectionName, hostName, ClientId, userName, password, ERPSystemId, checkedVal, selectedDbType, LanguageId, auditId, filerTableValue, ConnectionType) {
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'html',
        cache: false,
        url: 'getErpConnectionDetails',
        async: true,
        data: {
            connectionName: connectionName,
            ClientId: ClientId,
            hostName: hostName,
            userName: userName,
            password: password,
            LanguageId: LanguageId,
            ERPSystemId: ERPSystemId,
            checkedVal: checkedVal,
            selectedItemLabel: selectedDbType,
            auditId: auditId,
            filerTableValue: filerTableValue,
            ConnectionType: ConnectionType

        },
        success: function (response) {
            ajaxStop();
            if (response != null) {
                var responseObj = JSON.parse(response);
                if (responseObj != null && responseObj.connectionFlag == 'Y') {
                    var dbDetails = JSON.stringify(responseObj.dbObj);
                    $('#dbDetails').val(dbDetails);
                    var tablesArray = responseObj.tablesList;
                    var dragAnddropDiv = "<div class=\"sourceTablesHeader\">Total available tables</div><input id='searchErpTables' class='visionTablesSearchInput' type='text' placeholder='Search for more tables..' onkeyup='searchErpTables()'><img src='images/crossicon.png' title='Clear Data' onclick='clearTableInput()' class='visionClearFieldBtn'><div id=\"ErpsourceFields\" class=\"sourceFields\" style='margin-top: 30px;height:272px;'>" + tablesArray + "</div>"
                            + " <div class=\"destinationTablesHeader\">Selected tables</div><div id=\"ErpdestinationFields\" class=\"destinationFields\">"
                            + "</div><div class=\"erptablesProcessButton\"><input type='button' value='Process' name='Process' id='ErpprocessTablesBtn' onclick = 'processERPTables()' class='visionTablesProcessButton'></div>";
                    $("#fieldChooser").html(dragAnddropDiv);
//                    $("#dataMigratorTree").jqxTree('val', selectedDbType);
//                    $("#dataMigratorTree").jqxTree('expandItem', $("#sidemenu_DM_DATABASE")[0]);
                    $('#dataMigratorTree').jqxTree('collapseAll');
                    var items = $('#dataMigratorTree').jqxTree('getItems');
                    var label = [];
                    var id = [];
                    for (var i = 0; i < items.length; i++) {
                        var allItems = items[i];
                        label.push(allItems.label);
                        id.push(allItems.id);
                        if (selectedDbType != null && selectedDbType == label[i]) {
                            $("#dataMigratorTree").jqxTree('expandItem', $("#" + id[i])[0]);
                            $("#dataMigratorTree").jqxTree('selectItem', $("#" + id[i])[0]);
                            break;

                        }

                    }
                    $('#fieldChooser').show();
//                    $('.visionTablesComboBox').show();
//                    $('.visionUploadFileDiv').hide();
                    $('.visionDatabaseMain').show();
                    $('#visionUploadDocs').hide();
                    $('.visionConnectToDbDiv').hide();
                    $('#mappingColumns').hide();
                    $('.visionERPInner').hide();
                    $('#showERPConnectionType').hide();
                    dragErpTables();
//                    var sourceFields = $("#sourceFields");
//                    var destinationFields = $("#destinationFields");
//                    var chooser = $("#fieldChooser").fieldChooser(sourceFields, destinationFields);
                    var tabsOpenValue = $('#dataMigrationTabs').val();
                    if (tabsOpenValue != null && tabsOpenValue != '' && tabsOpenValue == 1) {
                        $('#dataMigrationTabs').jqxTabs('val', 0);
                    }
                } else {
                    showMessagePopup(responseObj.connectionMessage);

                }
                $('.visionProgressSteps').show();
                $("#fetchTables").addClass("active");
                $('.visionProgressFilesSteps').hide();

            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });
}
function showErpSavedConnection() {

    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'html',
        cache: false,
        url: 'getSavedErpConnections',
        async: true,
        success: function (response) {
            ajaxStop();
            if (response != null) {
                $('#savedConnectionTable').hide();
                $('#OracleERPSavedConnectionTable').hide();
                $('#ERPSavedConnectionTable').html(response);
                $('#ERPSavedConnectionTable').show();
                $('.savedErrorMsg').hide();

            } else {
                $('.savedErrorMsg').show();
            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });
}
function editErpConnection(conName, hostName, client, userName, password, ERPSystemId, checkedVal, selectedDbType, LanguageId, auditId) {
    $('#showConnectionType').html("<div><span style='font-weight:bold;padding-right: 25px;'>Connection Name :  </span>" + selectedDbType + "</div>");
    $('#selectedTypeName').val(selectedDbType);
    $('#ErpDbConnectionName').val(conName);
    $('#ERPHostName').val(hostName);
    $('#ERPClientName').val(client);
    $('#ERPUserName').val(userName);
    $('#ERPPassword').val(password);
    $('#ERPLanguageId').val(LanguageId);
    $('#ERPSystemId').val(ERPSystemId);
    $('#auditId').val(auditId);
    $('#ErpConnectionName').html(selectedDbType);
    $('#visionUploadDocs').hide();
    $('.visionProgressSteps').show();
    $('.visionERPInner').show();
    $('#mapColumns').removeClass("active");
    $("#fetchTables").removeClass("active");
    $('.dataMigrationInputError').hide();
    $('.visionDataMigrationError').hide();
    if (auditId != null && auditId != '') {
        $('.visionConnectToDbDiv').show();

        $('.visionDatabaseMain').hide();
        $('#visionERPMain').show();
        var tabsOpenValue = $('#dataMigrationTabs').val();
//        var saveBtn = '<input type=\"button\" value=\"Save\" name=\"Save\" class=\"visionInputSaveButton\" onclick = \'updateDatabaseDetails("' + conName + '",  "' + hostName + '" , "' + port + '", "' + userName + '", "' + password + '", "' + serviceName + '", "' + checkedVal + '", "' + selectedDbType + '", "' + auditId + '")\'>';
//        $('#saveDbTd').html(saveBtn);
//        $('#saveDbTd').show();
        $('#connectDbTd').show();
        $("#fieldChooser").hide();
        $('#mappingColumns').hide();
        $('.visionProgressFilesSteps').hide();
    }

    if (tabsOpenValue != null && tabsOpenValue != '' && tabsOpenValue == 1) {
        $('#dataMigrationTabs').jqxTabs('val', 0);

    }

}
function deleteERPConnection(conName, hostName, client, userName, password, ERPSystemId, checkedVal, selectedDbType, LanguageId, auditId) {
    //08/01
    $("#dialog").html("Are you sure you want to delete this connection?");
    $("#dialog").dialog({
        modal: true,
        title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
        height: 'auto',
        minHeight: 'auto',
        minWidth: 350,
        maxWidth: 'auto',
        fluid: true,
        buttons: [{
                text: (labelObject['Yes'] != null ? labelObject['Yes'] : 'Yes'),
                click: function () {
                    $(this).html("");
                    $(this).dialog("close");
                    $(this).dialog("destroy");
                    $.ajax({
                        type: 'post',
                        traditional: true,
                        dataType: 'html',
                        cache: false,
                        url: 'deleteErpDetails',
                        async: true,
                        data: {
                            connectionName: conName,
                            hostName: hostName,
                            client: client,
                            userName: userName,
                            password: password,
                            ERPSystemId: ERPSystemId,
                            LanguageId: LanguageId,
                            checkedVal: checkedVal,
                            selectedItemLabel: selectedDbType,
                            auditId: auditId

                        },
                        success: function (response) {
                            if (response != null) {
                                $("#dialog").html(response);
                                $("#dialog").dialog({
                                    title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
                                    modal: true,
                                    width: 300,
                                    height: 135,
                                    fluid: true,
                                    buttons: [{
                                            text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                                            click: function () {
                                                showSavedConnection();
                                                $(this).html("");
                                                $(this).dialog("close");
                                                $(this).dialog("destroy");

                                            }

                                        }],
                                    open: function () {
                                        $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                                        $(".visionHeaderMain").css("z-index", "999");
                                        $(".visionFooterMain").css("z-index", "999");

                                    },
                                    beforeClose: function (event, ui)
                                    {
                                        $(".visionHeaderMain").css("z-index", "99999");
                                        $(".visionFooterMain").css("z-index", "99999");
                                    }
                                });
                            }
                        },
                        error: function (e) {
                            sessionTimeout(e);
                        }
                    });

                }
            },
            {
                text: (labelObject['No'] != null ? labelObject['No'] : 'No'),
                click: function () {
                    $(this).html("");
                    $(this).dialog("close");


                }
            }
        ],
        open: function () {
            $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
            $(this).closest(".ui-dialog").find(".ui-button").eq(2).addClass("dialogno");
            $(this).closest(".ui-dialog").addClass("visionCommonDialog");
            $(".visionHeaderMain").css("z-index", "999");
            $(".visionFooterMain").css("z-index", "999");
        },
        beforeClose: function (event, ui)
        {
            $(".visionHeaderMain").css("z-index", "99999");
            $(".visionFooterMain").css("z-index", "99999");

        }
    });

}
function processERPTables() {
    var tablesArray = [];
    var gridId = $('#gridIdValue').val();
    var dbDetails = $('#dbDetails').val();
    dbDetails = JSON.parse(dbDetails);
    // var conName = dbDetails.conName;
    var hostName = dbDetails.hostName;
    var client = dbDetails.ClientId;
    var userName = dbDetails.userName;
    var password = dbDetails.password;
    var ErpSystemId = dbDetails.ERPSystemId;
    var LanguageId = dbDetails.LanguageId;
    var selectedDbType = dbDetails.selectedItemLabel;
    $('#ErpdestinationFields div').each(function () {
        var tableName = $(this).attr('data-table-name');
        tablesArray.push(tableName);
    })
    var sourceex = $("#" + gridId).jqxGrid('source');
    var dataFeilds = [];
    dataFeilds = sourceex._source.datafields;
    var tableName = sourceex._source.data.tableName;

    var destnTableLength = $('#ErpdestinationFields').children('div').length;
    var tablesErrMsg = '';
    if (destnTableLength != null && destnTableLength > 5) {
        tablesErrMsg = "Please select 5 tables only ";
        showMessagePopup(tablesErrMsg);

    } else if (!(destnTableLength != null && destnTableLength != 0)) {
        tablesErrMsg = "Please select atleast one table to process ";
        showMessagePopup(tablesErrMsg);

    } else {


        $.ajax({
            type: 'post',
            traditional: true,
            dataType: 'html',
            cache: false,
            url: 'fetchErpTableColumns',
            async: true,
            data: {tablesArray: JSON.stringify(tablesArray),
                hostName: hostName,
                ClientId: client,
                userName: userName,
                password: password,
                ERPSystemId: ErpSystemId,
                LanguageId: LanguageId,
                selectedItemLabel: selectedDbType,
                gridId: gridId,
                tableName: tableName,
                dataFeilds: JSON.stringify(dataFeilds)},

            success: function (response) {
                ajaxStop();
                if (response != null) {
                    $('.visionTablesComboBox').hide();
                    $('.visionUploadFileDiv').hide();
                    $('.visionConnectToDbDiv').hide();
                    var response = JSON.parse(response);
                    if (response != null && response.connectionFlag == 'Y') {
                        var columnStr = response['columnStr'];
                        var tablesStr = response['selectedTables'];
                        $('#mappingColumns').show();
                        $('#showSourceTablesList').html(tablesStr);
                        $('#MappedTable').html(columnStr);
                        var matchedSelectStr = response['matchedSelectStr'];
                        $('#mapColumns').addClass("active");
                        $('.visionProgressFilesSteps').hide();

                    } else {
                        showMessagePopup(response.connectionMessage);

                    }


                }
            },
            error: function (e)
            {
                sessionTimeout(e);
            }

        });

    }






}
function dragErpTables() {
    $("#ErpsourceFields div").draggable({
        revert: "invalid",
        refreshPositions: true,
        cursor: 'move',
        zindex: false,
        opacity: false,
        appendto: "parent",
        drag: function (event, ui) {
            ui.helper.addClass("draggableTable");
        },
        stop: function (event, ui) {
            ui.helper.removeClass("draggableTable");

        }
    });
    $("#ErpdestinationFields").droppable({
        revert: "invalid",
        refreshPositions: true,
        cursor: 'move',
        drop: function (event, ui) {
            if ($("#ErpdestinationFields div").length == 0) {
                $("#ErpdestinationFields").html("");
            }
            ui.draggable.addClass("droppedTable");

            var droppableId = ui.draggable[0].id;
            $("#" + droppableId).children().show();
            $("#ErpdestinationFields").append(ui.draggable);



        }
    });
}
function moveToErpSource(table) {
    var tablename = table.id;
    $("#ErpsourceFields").append($("#" + tablename));
    $("#" + tablename).children().hide();
}
function searchErpTables() {
    var filerTableValue = $('#searchErpTables').val().toUpperCase();
    var filerTableValueLen = $('#searchErpTables').val().length;
//    $("#sourceFields div").filter(function () {
//        $(this).toggle($(this).text().toLowerCase().indexOf(filerTableValue) > -1)
//    });

    if ((filerTableValueLen != null && filerTableValueLen >= 2) || filerTableValueLen == 0) {

        var dbDetails = $('#dbDetails').val();
        dbDetails = JSON.parse(dbDetails);
        var conName = dbDetails.conName;
        var hostName = dbDetails.hostName;
        var client = dbDetails.ClientId;
        var userName = dbDetails.userName;
        var password = dbDetails.password;
        var ErpSystemId = dbDetails.ERPSystemId;
        var LanguageId = dbDetails.LanguageId;
        var selectedDbType = dbDetails.selectedItemLabel;
        var checkedVal = $('#checkBoxChecked').is(':checked');
        var auditId = $('#auditId').val();
        getFilterErpTables(conName, hostName, client, userName, password, ErpSystemId, false, selectedDbType, LanguageId, auditId, filerTableValue);

    }

}
function getFilterErpTables(conName, hostName, client, userName, password, ErpSystemId, checkedVal, selectedDbType, LanguageId, auditId, filerTableValue) {
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'html',
        cache: false,
        url: 'getErpConnectionDetails',
        async: true,
        data: {
            connectionName: conName,
            hostName: hostName,
            ClientId: client,
            userName: userName,
            password: password,
            ERPSystemId: ErpSystemId,
            LanguageId: LanguageId,
            checkedVal: checkedVal,
            selectedItemLabel: selectedDbType,
            auditId: auditId,
            filerTableValue: filerTableValue
        },
        success: function (response) {
            ajaxStop();
            if (response != null) {
                var responseObj = JSON.parse(response);
                if (responseObj != null && responseObj.connectionFlag == 'Y') {
                    $("#ErpsourceFields").html("");
                    var tablesArray = responseObj.tablesList;
                    $("#ErpsourceFields").html(tablesArray);
                    var dbDetails = JSON.stringify(responseObj.dbObj);
                    $('#dbDetails').val(dbDetails);
                    dragErpTables();
//                    var sourceFields = $("#sourceFields");
//                    var destinationFields = $("#destinationFields");
//                    var chooser = $("#fieldChooser").fieldChooser(sourceFields, destinationFields);
//                    var tabsOpenValue = $('#dataMigrationTabs').val();
//                    if (tabsOpenValue != null && tabsOpenValue != '' && tabsOpenValue == 1) {
//                        $('#dataMigrationTabs').jqxTabs('val', 0);
//                    }
                } else {
                    showMessagePopup(responseObj.connectionMessage);

                }
//                $('.visionProgressSteps').show();
//                $("#fetchTables").addClass("active");
//                $('.visionProgressFilesSteps').hide();

            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });
}
function showOracleErpSavedConnection() {

    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'html',
        cache: false,
        url: 'getOracleErpSavedConnections',
        async: true,
        success: function (response) {
            ajaxStop();
            if (response != null) {
                $('#savedConnectionTable').hide();
                $('#ERPSavedConnectionTable').hide();
                $('#OracleERPSavedConnectionTable').html(response);
                $('#OracleERPSavedConnectionTable').show();
                $('.savedErrorMsg').hide();

            } else {
                $('.savedErrorMsg').show();
            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });
}



