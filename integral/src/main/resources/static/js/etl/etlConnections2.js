/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

$(document).ready(function () {
//    $('#conTabs').jqxTabs({width: "100%", height: "100%", position: 'top'});
//    $('#conTabs').on('selected', function (event)
//    {
//        var selectedTab = event.args.item;
//        var content = $('#conTabs').jqxTabs('getContentAt', selectedTab);
//        var id = $(content).attr("id");
//        var tabName = $('#conTabs').jqxTabs('getTitleAt', selectedTab);
//        if (tabName == "FAV") {
//            getFavSchemas();
//        }
//
//
//    });
});

function getAvaliableConn(connectionType) {
    ajaxStart();
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'getAvaliableConnections',
        cache: false,
        data: {
            connectionType: connectionType,
        },
        success: function (response) {
            ajaxStop();

            if (response != null && response != "") {
                savedDBData = response['connectionObj'];
                $("#savedConnectionsModifiedInner").remove();

                var htmlDiv = $("#ConnInnerDiv").html(response['htmlDiv']);
                $("#ConnInnerPreviousButtonDiv").html(response['previousButtonDiv']);
                if (connectionType == "parentDiv") {
                    $(".etlConnImgs1").removeClass('etlConnImgs1').addClass('etlConnImgs');
                } else {
                    $(".etlConnImgs").removeClass('etlConnImgs').addClass('etlConnImgs1');
                }
//                $(".etlConnImgs").addClass('etlConnImgs1')
                $("#previousIcon").click(function (evt) {
                    getAvaliableConn("parentDiv");
                });
//                   $("#savedConnectionsModified img").on('mousedown', function (event) {
//                    var target = $(event.target).parents()[0];
//                    var connectionName = target['id'];
//                    var schemaType = $("#" + connectionName).attr("div-type");
//                    var rightClick = isRightClick(event);
//                    if (rightClick && target != null) {
//                        var menuItems = "";
//                        menuItems += "<li onclick=viewDbConnection('" + connectionName + "','" + schemaType + "')>View</li>";
//                        menuItems += "<li onclick=deleteDbConnection('" + connectionName + "','" + schemaType + "','" + schema + "')>Delete</li>";
//                        menuItems += "<li onclick=viewSQLEditor('" + connectionName + "','" + schema + "')>SQL</li>";
//                        var menuHeight = 3;
//                        $("#jqxMenu1").remove();
//                        $(".etl-page-body").append("<div id='jqxMenu1'><ul></ul></div>");
//                        $("#jqxMenu1 ul").html(menuItems);
//                        var contextMenu = $("#jqxMenu1").jqxMenu({width: '120px', height: menuHeight * 27.5 + 'px', autoOpenPopup: false, mode: 'popup', theme: 'energyblue'}); // ravi start
//                        var scrollTop = $(window).scrollTop();
//                        var scrollLeft = $(window).scrollLeft();
//                        contextMenu.jqxMenu('open', parseInt(event.clientX) + 5 + scrollLeft, parseInt(event.clientY) + 5 + scrollTop);
//                        return false;
//                    }
//                });
            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });
}

function getDbSchemas(schema, type) {
    var schemaImage = "";
    if (schema == "Oracle") {
        schemaImage = 'images/DM_ORACLE.png';
    } else if (schema == "SQLSERVER") {
        schemaImage = 'images/DM_MSSQL.png';
    } else if (schema == "SQLSERVER") {
        schemaImage = 'images/DM_MYSQL.png';
    } else if (schema == "ERP") {
        schemaImage = 'images/DM_ERP.png';
    }
    ajaxStart();
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'getAvaliableSchemas',
        cache: false,
        data: {
            schema: schema,
            type: type,
            schemaImage: schemaImage
        },
        success: function (response) {
            ajaxStop();
            if (response != null && response != "") {
                savedDBData = response['connectionObj'];
                $("#savedConnectionsModifiedInner").remove();
                var htmlDiv = $("#ConnInnerDiv").html(response['htmlDiv']);
                $("#previousButtonDiv").remove();
                $("#ConnInnerPreviousButtonDiv").prepend('<div id="previousButtonDiv" class="previousButtonDivClass" previousdivid="DB">  <span class="previousIconSpan"><img id="previousIcon" src="images\\previous_icon.svg" previousdivid="DB" title="previous" style="width:10px;height: 10px;cursor:pointer;"> </span><span>' + type.toUpperCase() + '</span><span>/' + schema.toUpperCase() + '</span></div>')
                $("#savedConnectionsModified img").on('mousedown', function (event) {
                    var target = $(event.target).parents()[0];
                    var connectionName = target['id'];
                    var schemaType = $("#" + connectionName).attr("div-type");
                    var rightClick = isRightClick(event);
                    if (rightClick && target != null) {
                        var menuItems = "";
                        menuItems += "<li onclick=viewDbConnection('" + connectionName + "','" + schemaType + "')>View</li>";
                        menuItems += "<li onclick=deleteDbConnection('" + connectionName + "','" + schemaType + "','" + schema + "')>Delete</li>";
                        menuItems += "<li onclick=viewSQLEditor('" + connectionName + "','" + schema + "')>SQL</li>";
                        menuItems += "<li onclick=addSchemaToFav('" + connectionName + "','" + schemaType + "','" + schema + "')>Add To Fav</li>";
                        var menuHeight = 4;
                        $("#jqxMenu1").remove();
                        $(".etl-page-body").append("<div id='jqxMenu1'><ul></ul></div>");
                        $("#jqxMenu1 ul").html(menuItems);
                        var contextMenu = $("#jqxMenu1").jqxMenu({width: '120px', height: menuHeight * 27.5 + 'px', autoOpenPopup: false, mode: 'popup',
//                            theme: 'energyblue'
                        }); // ravi start
                        var scrollTop = $(window).scrollTop();
                        var scrollLeft = $(window).scrollLeft();
                        contextMenu.jqxMenu('open', parseInt(event.clientX) + 5 + scrollLeft, parseInt(event.clientY) + 5 + scrollTop);
                        return false;
                    }
                });
//                $("#previousIcon").click(function (evt) {
//                    $(".schemaTypeClass span").text("");
//                    if (type == "ERP") {
//                        getAvaliableConn("parentDiv");
//                    } else {
//                        getAvaliableConn("DB");
//                    }
//
//                });

                $("#previousIcon").click(function (evt) {
                    $(".schemaTypeClass span").text("");
                    if (type == "ERP") {
                        getAvaliableConn('ERP');
//                        getAvaliableConn("parentDiv");
                    } else {
                        getAvaliableConn("DB");
                    }

                });



            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });
}

function getDBtables(dbName, DbType, schemaType, priorityType) {
    var schemaImage = "";
    if (DbType == "Oracle") {
        schemaImage = 'images/DM_ORACLE.png';
    } else if (DbType == "SQLSERVER") {
        schemaImage = 'images/DM_MSSQL.png';
    } else if (DbType == "SQLSERVER") {
        schemaImage = 'images/DM_MYSQL.png';
    } else if (DbType == "ERP") {
        schemaImage = 'images/DM_ERP.png';
    } else if (schemaType == "ERP") {
        schemaImage = 'images/DM_ERP.png';
    }

    ajaxStart();
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'getDatabaseTables',
        cache: false,
        data: {
            dbName: dbName,
            DbType: DbType,
            schemaType: schemaType,
            schemaImage: schemaImage
        },
        success: function (response) {
            ajaxStop();
            if (response != null && response != "") {
                response = response['htmlDiv'];
                $("#savedConnectionsModifiedInner").remove();
                if (priorityType != null && priorityType == "FAVORITE") {

                    $('#conTabs').jqxTabs('select', 0);
                }
                var htmlDiv = $("#ConnInnerDiv").html(response);
                $("#previousButtonDiv").remove();
                if (schemaType == "ERP") {
                    $("#ConnInnerPreviousButtonDiv").prepend('<div id="previousButtonDiv" class="previousButtonDivClass" previousdivid="DB">  <span class="previousIconSpan"><img id="previousIcon" src="images\\previous_icon.svg" previousdivid="DB" title="previous" style="width:10px;height: 10px;cursor:pointer;"> </span><span>' + schemaType.toUpperCase() + '</span><span class="dbNameClass">/' + dbName.toUpperCase() + '</span></div>')
                } else {
                    $("#ConnInnerPreviousButtonDiv").prepend('<div id="previousButtonDiv" class="previousButtonDivClass" previousdivid="DB">  <span class="previousIconSpan"><img id="previousIcon" src="images\\previous_icon.svg" previousdivid="DB" title="previous" style="width:10px;height: 10px;cursor:pointer;"> </span><span>' + schemaType.toUpperCase() + '</span><span>/' + DbType.toUpperCase() + '</span><span class="dbNameClass">/' + dbName.toUpperCase() + '</span></div>')
                }
//                $("#previousIcon").click(function (evt) {
//                    $(".dbNameClass span").text("");
//                    if (schemaType == "ERP") {
//                        getAvaliableConn(schemaType)
//                    } else {
//                        getDbSchemas(DbType, schemaType);
//                    }
//
//                });

                $("#previousIcon").click(function (evt) {
                    $(".dbNameClass span").text("");
                    if (schemaType == "ERP") {
                        getDbSchemas(DbType, schemaType);
//                         getDBtables(dbName, DbType, schemaType, priorityType);
//                        getAvaliableConn(schemaType)
                    } else {
                        getDbSchemas(DbType, schemaType);
                    }

                });


            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }
    });
}
function viewDbConnection(connectonName) {
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'html',
        cache: false,
        url: 'getConnDetails',
        async: true,
        data: {
            connectonName: connectonName,
        },
        success: function (response) {
            stopLoader();
            if (response != null) {
                $("#dialog1").html(response);
                $("#dialog1").dialog({
                    title: (labelObject['View Connection'] != null ? labelObject['View Connection'] : 'View Connection'),
                    modal: true,
                    height: 'auto',
                    minWidth: 500,
                    maxWidth: 'auto',
                    fluid: true,
                    buttons: [],
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
        error: function (e)
        {
            sessionTimeout(e);
        }

    });
}
function deleteDbConnection(connectionName, schemaType, schema) {
    $("#dialog").html(labelObject['Are you sure you want to delete connection?'] != null ? labelObject['Are you sure you want to delete connection?'] : 'Are you sure you want to delete connection?');
    $("#dialog").dialog({
        title: (labelObject['Confirmation'] != null ? labelObject['Confirmation'] : 'Confirmation'),
        modal: true,
        height: 'auto',
        minWidth: 300,
        maxWidth: 'auto',
        fluid: true,
        buttons: [{
                text: (labelObject['Yes'] != null ? labelObject['Yes'] : 'Yes'),
                click: function () {
                    $(this).html("");
//                    $(this).dialog("close");
                    $(this).dialog("destroy");
                    $.ajax({
                        type: 'post',
                        traditional: true,
                        dataType: 'JSON',
                        cache: false,
                        url: 'deleteDatabaseDetails',
                        async: true,
                        data: {

                            connectionName: connectionName
                        },
                        success: function (response) {
                            stopLoader();
                            if (response != null) {
                                showMesg(response['message']);
                                if (response['deleteSucessful'] == "Y") {
                                    getDbSchemas(schema, schemaType)
                                }
                            }
                        },
                        error: function (e)
                        {
                            sessionTimeout(e);
                        }

                    });
                }},
            {
                text: (labelObject['No'] != null ? labelObject['No'] : 'No'),
                click: function () {
                    $(this).html("");
//                    $(this).dialog("close");
                    $(this).dialog("destroy");
                }}],
        open: function () {
            $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
            $(this).closest(".ui-dialog").find(".ui-button").eq(2).addClass("dialogno");
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
function getAvaliableFiles(fileType, type) {
    ajaxStart();
    var fileImage = "";
    if (fileType == "xml") {
        fileImage = 'images/XML-Icon.svg';
    } else if (fileType == "xlsx") {
        fileImage = 'images/XLSX-Icon.svg';
    } else if (fileType == "csv") {
        fileImage = 'images/CSV-Icon.svg';
    } else if (fileType == "json") {
        fileImage = 'images/JSON_Icon.svg';
    } else if (fileType == "pdf") {
        fileImage = 'images/ETL_PDFIcon.png';
    } else if (fileType == "image") {
        fileImage = 'images/ETL_Image.png';
    }
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'getAvaliableFiles',
        cache: false,
        data: {
            fileType: fileType,
            fileImage: fileImage,
            type: type
        },
        success: function (response) {
            ajaxStop();
            if (response != null && response != "") {
                $("#savedConnectionsModifiedInner").remove();
                var htmlDiv = $("#ConnInnerDiv").html(response['htmlDiv']);
                $("#previousButtonDiv").remove();
                $("#ConnInnerPreviousButtonDiv").prepend('<div id="previousButtonDiv" class="previousButtonDivClass" previousdivid="DB">  <span class="previousIconSpan"><img id="previousIcon" src="images\\previous_icon.svg" previousdivid="DB" title="previous" style="width:10px;height: 10px;cursor:pointer;"> </span><span>' + type.toUpperCase() + '</span><span class="fileTypeClass">/' + fileType.toUpperCase() + '</span></div>')
                $(".etlConnImgs1").removeClass('etlConnImgs1').addClass('etlConnImgs2');
//                $("#previousButtonDiv").append("<span class='fileTypeClass'>/"+fileType.toUpperCase()+"</span>");

                $(".visionObjectNameDiv").on('mousedown', function (event) {
                    var target = $(event.target).parents()[0];
                    var fileName = target['attributes']['fileoriginalname']['value'];
                    var filePath = target['attributes']['filename']['value'];
                    var filePathValue = target['attributes']['filepath']['value'];
                    $("#filePathValueHidden").val(filePathValue);
                    var fileType1 = "";
                    var fileExtensions = [".xlsx", ".xls", ".XLS", ".XLSX", ".txt", ".csv", ".xml", ".TXT", ".CSV", ".XML", ".JSON", ".json", ".PDF", ".pdf" , ".JPEG", ".jpeg", ".PNG", ".png"];
                    if (fileName != null && fileName != '') {
                        for (var i = 0; i < fileExtensions.length; i++) {
                            if (fileName.endsWith(fileExtensions[i])) {
                                fileType1 = fileExtensions[i];
                                break;
                            }
                        }
                    }
                    var fileType = "." + fileName.substr((fileName.lastIndexOf('.') + 1));
                    var fileObj = {};
                    if (filePath != null && filePath.lastIndexOf("\\") > -1) {
                        filePath = filePath.substring(filePath.lastIndexOf("\\") + 1);
                    }
                    fileObj['filePath'] = filePath;
                    fileObj['fileType'] = fileType1;
                    fileObj['type'] = type;
                    fileObj['fileValueType'] = fileType;
                    for (var entitykey in HtmlEntities) {
                        var entity = HtmlEntities[entitykey];
                        var regex = new RegExp(entitykey, 'g');
                        fileName = fileName.replace(regex, entity);
                    }
                    fileObj['fileName'] = fileName;
                    var rightClick = isRightClick(event);
                    if (rightClick && target != null) {
                        var menuItems = "";
                        menuItems += "<li onclick=deleteCustFile('" + JSON.stringify(fileObj) + "')>Delete</li>";
                        menuItems += "<li onclick=viewFileData('" + JSON.stringify(fileObj) + "')>View Data</li>";
                        menuItems += "<li onclick=addFilesToFav('" + JSON.stringify(fileObj) + "')>Add To Fav</li>";

                        var menuHeight = 3;
                        $("#jqxMenu1").remove();
                        $(".etl-page-body").append("<div id='jqxMenu1'><ul></ul></div>");
                        $("#jqxMenu1 ul").html(menuItems);
                        var contextMenu = $("#jqxMenu1").jqxMenu({width: '120px', height: menuHeight * 27.5 + 'px', autoOpenPopup: false, mode: 'popup',
//                            theme: 'energyblue'
                        }); // ravi start
                        var scrollTop = $(window).scrollTop();
                        var scrollLeft = $(window).scrollLeft();
                        contextMenu.jqxMenu('open', parseInt(event.clientX) + 5 + scrollLeft, parseInt(event.clientY) + 5 + scrollTop);
                        return false;
                    }
                });

                $(".visionObjectNameDiv").on('dblclick', function (event) {
                    ajaxStart();
                    var target = $(event.target).parents()[0];
                    var fileName = target['attributes']['fileoriginalname']['value'];
                    var filePath = target['attributes']['filename']['value'];
                    var filePathValue = target['attributes']['filepath']['value'];

                    $("#filePathValueHidden").val(filePathValue);
                    var fileType1 = "";
                    var fileExtensions = [".xlsx", ".xls", ".XLS", ".XLSX", ".txt", ".csv", ".xml", ".TXT", ".CSV", ".XML", ".JSON", ".json"];
                    if (fileName != null && fileName != '') {
                        for (var i = 0; i < fileExtensions.length; i++) {
                            if (fileName.endsWith(fileExtensions[i])) {
                                fileType1 = fileExtensions[i];
                                break;
                            }
                        }
                    }
                    var fileType1 = "." + fileName.substr((fileName.lastIndexOf('.') + 1));
                    var fileObj = {};
                    if (filePath != null && filePath.lastIndexOf("\\") > -1) {
                        filePath = filePath.substring(filePath.lastIndexOf("\\") + 1);
                    }
                    fileObj['filePath'] = filePath;
                    fileObj['fileType'] = fileType1;
                    fileObj['type'] = type;
                    fileObj['fileValueType'] = fileType;
                    for (var entitykey in HtmlEntities) {
                        var entity = HtmlEntities[entitykey];
                        var regex = new RegExp(entitykey, 'g');
                        fileName = fileName.replace(regex, entity);
                    }
                    fileObj['fileName'] = fileName;
                    viewFileData(JSON.stringify(fileObj));

                });

                $("div.visionObjectNameDiv").draggable({
                    cursor: "move",
                    opacity: 0.7,
                    helper: 'clone',
                    zIndex: 1000,
                    helper: function (event, ui) {

                        var $this = $(this);
                        var innerText = $this.text();
                        var title = innerText;
                        var descripttion = $this.attr("title");
                        var filename = $this.attr("filename");
                        var fileoriginalname = $this.attr("fileoriginalname");
                        var filepath = $this.attr("filepath");
//                        var fileType = $this.attr("value");
                        var fileType = $this.attr("fileType");

                        var title = fileoriginalname;

                        var connectionObj = {};
                        connectionObj['filePath'] = filepath;
                        connectionObj['fileType'] = fileType;
                        connectionObj['fileName'] = fileoriginalname;
                        var fileObj = {};
                        var filePath = filepath;
                        if (filePath != null && filePath.lastIndexOf("\\") > -1) {
                            filePath = filePath.substring(filePath.lastIndexOf("\\") + 1);
                        }
                        fileObj['filePath'] = filePath;
                        fileObj['fileType'] = fileType;

//                        for (var entitykey in HtmlEntities) {
//                            var entity = HtmlEntities[entitykey];
//                            var regex = new RegExp(entitykey, 'g');
//                            title = title.replace(regex, entity);
//                        }

                        fileObj['fileName'] = title;
                        var imageIcon = "images/TEXT_.svg";

                        if (fileType == 'xlsx'
                                || fileType == 'XLSX'
                                ) {
                            imageIcon = "images/XLSX-Icon.svg"
                        } else if (fileType == 'xml'
                                || fileType == 'XML') {
                            imageIcon = "images/XML-Icon.svg";

                        } else if (fileType == 'CSV'
                                || fileType == 'csv') {
                            imageIcon = "images/CSV-Icon.svg";
                        } else if (fileType == 'JSON'
                                || fileType == 'json') {
                            imageIcon = "images/JSON_Icon.svg";
                        } else if (fileType == 'xls'
                                || fileType == 'XLS') {
                            imageIcon = "images/xls-Icon.svg";
                        }

                        $("#draggableOperatorId").remove();
                        var body = '<div id="draggableOperatorId" class="draggableClass"  title="' + title + '" class=""><div><img src="' + imageIcon + '"'
                                + 'class="visionOpIcons" title="Double click here to view the data" ondblclick=viewFileData(\'' + JSON.stringify(fileObj) + '\')  style="width:18px;height: 18px;"/></div><div class="visionOpLabelDiv" title="Double click to Rename" ondblclick="renameOperatorDisplayLabel(\'' + title + '\',this)" style="width:' + (title.length * 5) + 'px;margin-left:' + (((title.length * -5) / 2) + 14) + 'px;">' + title + '</div>';
                        $("body").append(body);

                        return  $("#draggableOperatorId");


                    },
                    stop: function (e, ui) {

                        var $this = $(this);
                        var innerText = $this.text();
                        var title = innerText;
                        var descripttion = $this.attr("title");
                        var filename = $this.attr("filename");
                        var fileoriginalname = $this.attr("fileoriginalname");
                        var filepath = $this.attr("filepath");
                        var fileType = $this.attr("fileType");
                        var connectionObj;
                        $(".flowchart-operator-connector-label").hide();

                        var contentSplitterStyle = $("#contentSplitter").css("display");

                        var contentSplitter1Style = $("#contentSplitter1").css("display");

                        var $flowchart;
                        var $container;
                        if (contentSplitterStyle == "block") {
                            $flowchart = $('#' + flowChartWorkSpaceId);
                            $container = $('#' + flowChartWorkSpaceId);
                        } else if (contentSplitter1Style == "block") {
                            $flowchart = $('#dataModellerFlowchartworkSourcesspace');
                            $container = $('#dataModellerFlowchartworkSourcesspace');
                        }
                        var elOffset = ui.offset;
                        if ($container != null) {
                            var containerOffset = $container.offset();
                            if (elOffset.left > containerOffset.left &&
                                    elOffset.top > containerOffset.top &&
                                    elOffset.left < containerOffset.left + $container.width() &&
                                    elOffset.top < containerOffset.top + $container.height()) {
                                var flowchartOffset = $flowchart.offset();
                                var relativeLeft = elOffset.left - flowchartOffset.left;
                                var relativeTop = elOffset.top - flowchartOffset.top;
                                var positionRatio = $flowchart.flowchart('getPositionRatio');
                                relativeLeft /= positionRatio;
                                relativeTop /= positionRatio;
                                elOffset.left = relativeLeft;
                                elOffset.top = relativeTop;
                            }
                            var top = elOffset.top;



                            var top = elOffset.top;


                            if ((fileType == "xml" || fileType == "csv" || fileType == "xlsx" || fileType == "xls" || fileType == "json")) {
                                var data = {
                                    top: top,
                                    left: elOffset.left,
                                    dragType: type
                                };
                                var title = fileoriginalname;

                                var connectionObj = {};
                                connectionObj['filePath'] = filepath;
                                connectionObj['fileType'] = fileType;
                                connectionObj['fileName'] = fileoriginalname;
                                var fileObj = {};
                                var filePath = filepath;
                                if (filePath != null && filePath.lastIndexOf("\\") > -1) {
                                    filePath = filePath.substring(filePath.lastIndexOf("\\") + 1);
                                }
                                fileObj['filePath'] = filePath;
                                fileObj['fileType'] = fileType;

                                for (var entitykey in HtmlEntities) {
                                    var entity = HtmlEntities[entitykey];
                                    var regex = new RegExp(entitykey, 'g');
                                    title = title.replace(regex, entity);
                                }



                                fileObj['fileName'] = title;
                                var imageIcon = "images/Notepad.png";
                                if (fileType == 'xlsx'
                                        || fileType == 'XLSX'
                                        ) {
                                    imageIcon = "images/XLSX-Icon.svg"
                                } else if (fileType == 'xml'
                                        || fileType == 'XML') {
                                    imageIcon = "images/XML-Icon.svg";

                                } else if (fileType == 'CSV'
                                        || fileType == 'csv') {
                                    imageIcon = "images/CSV-Icon.svg";
                                } else if (fileType == 'JSON'
                                        || fileType == 'json') {
                                    imageIcon = "images/JSON_Icon.svg";
                                } else if (fileType == 'xls'
                                        || fileType == 'XLS') {
                                    imageIcon = "images/xls-Icon.svg";
                                }
                                connectionObj['imageIcon'] = imageIcon;
                                data['connObj'] = connectionObj;
                                data['filePath'] = filePath;
                                data['properties'] = {

                                    body: '<div class="draggableClass"  title="' + title + '" class=""><div><img src="' + imageIcon + '"'
                                            + 'class="visionOpIcons" title="Double click here to view the data" ondblclick=viewFileData(\'' + JSON.stringify(fileObj) + '\')  style="width:18px;height: 18px;"/></div><div class="visionOpLabelDiv" title="Double click to Rename" ondblclick="renameOperatorDisplayLabel(\'' + title + '\',this)" style="width:' + (title.length * 5) + 'px;margin-left:' + (((title.length * -5) / 2) + 14) + 'px;">' + title + '</div>',
                                    inputs: {
                                        input_1: {
                                            label: '',
                                        }
                                    },
                                    outputs: {
                                        output_1: {
                                            label: '',
                                        }
                                    }
                                }

                                var operatorId = $flowchart.flowchart('addOperator', data);
                                $(".flowchart-operator-connector-label").hide();
                                $(".flowchart-operator-title").hide();
                                trfmRulesChanged = true;
                                top = top + 70;
//                                var component = $(this).attr("component");
//                                if (component != "Y") {
//                                    inputOutPutPopUp(operatorId);
//                                }
                            }
                        }


                    }
                });

                $("#previousIcon").click(function (evt) {
                    $(".fileTypeClass span").text("");
                    getAvaliableConn("FILE");
                });


                $(document).on('contextmenu', function (e) {
                    if ($(e.target).parents('.jqx-tree').length > 0) {
                        return false;
                    }
                    if ($(event.target).parents('div.visionObjectNameDiv').length > 0 || $(event.target).hasClass('visionObjectNameDiv')) {
                        return false;
                    }

                    return true;
                });

            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });
}
function deleteCustFile(fileDataObjStr) {
    if (fileDataObjStr != null && fileDataObjStr != '') {
        var fileDataObj = JSON.parse(fileDataObjStr);
        var filePath = fileDataObj['filePath'];
        var fileName = fileDataObj['fileName'];
        var type = fileDataObj['type'];
        var fileValueType = fileDataObj['fileValueType'];
        var filePathValue = $("#filePathValueHidden").val();
        $("#dialog").html(labelObject['Are you sure you want to delete file?'] != null ? labelObject['Are you sure you want to delete file?'] : 'Are you sure you want to delete file?');
        $("#dialog").dialog({
            title: (labelObject['Confirmation'] != null ? labelObject['Confirmation'] : 'Confirmation'),
            modal: true,
            height: 'auto',
            minWidth: 300,
            maxWidth: 'auto',
            fluid: true,
            buttons: [{
                    text: (labelObject['Yes'] != null ? labelObject['Yes'] : 'Yes'),
                    click: function () {
                        $(this).html("");
//                        $(this).dialog("close");
                        $(this).dialog("destroy");
                        $.ajax({
                            type: 'post',
                            traditional: true,
                            dataType: 'html',
                            cache: false,
                            url: 'deleteFile',
                            async: true,
                            data: {
                                fileName: fileName,
                                filePath: filePath
                            },
                            success: function (response) {
                                stopLoader();
                                if (response.indexOf("Deleted Succesfully") > 0) {
                                    getAvaliableFiles(fileValueType, type);
                                }
                                if (response != null) {
                                    showMesg(response);
                                }
                            },
                            error: function (e)
                            {
                                sessionTimeout(e);
                            }
                        });
                    }},
                {
                    text: (labelObject['No'] != null ? labelObject['No'] : 'No'),
                    click: function () {
                        $(this).html("");
//                        $(this).dialog("close");
                        $(this).dialog("destroy");
                    }}],
            open: function () {
                $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                $(this).closest(".ui-dialog").find(".ui-button").eq(2).addClass("dialogno");
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
}

function getSelectedDbTables(parentKey, schenaName, schema, schemaType) {
    var schemaImage = "";
    if (schema == "Oracle") {
        schemaImage = 'images/DM_ORACLE.png';
    } else if (schema == "SQLSERVER") {
        schemaImage = 'images/DM_MSSQL.png';
    } else if (schema == "SQLSERVER") {
        schemaImage = 'images/DM_MYSQL.png';
    } else if (schema == "ERP") {
        schemaImage = 'images/DM_ERP.png';
    }
    var divId = ('div-' + schenaName + '-' + parentKey);
    var connectionObj;

    if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
    {
        var connObj = savedDBData[schenaName];
        if (connObj != null && !jQuery.isEmptyObject(connObj)) {
            connectionObj = connObj;
        }
    }
    var columnsObj = globalTreeObj['treeColumnObj'];
    var data = {
        parentkey: parentKey,
        connectionObj: JSON.stringify(connectionObj),
        startIndex: 0,
        endIndex: $("#treePageSize").val(),
        columnsObj: JSON.stringify(columnsObj)
    };
    var url;
    if (schemaType == "ERP") {
        url = 'getTreeErpConnectionDetails'

    } else {
        url = 'fetchSchemaTables'
    }
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: url,
        cache: false,
        data: data,

        success: function (response) {
            ajaxStop();
            if (response != null && response != "") {
                $("#savedConnectionsModifiedInner").remove();
                var htmlDiv = $("#ConnInnerDiv").html(response['htmlDiv']);
                $("#previousButtonDiv").remove();
                if (schemaType == "ERP") {
                    var titleData = schemaType + '/' + schenaName + '/' + parentKey;
                } else {
                    var titleData = schemaType + '/' + schema + '/' + schenaName + '/' + parentKey;
                }
                $("#ConnInnerPreviousButtonDiv").prepend('<div id="previousButtonDiv" class="previousButtonDivClass" previousdivid="DB" title=' + titleData + '>  <span class="previousIconSpan"><img id="previousIcon" src="images\\previous_icon.svg" previousdivid="DB" title="previous" style="width:10px;height: 10px;cursor:pointer;"> </span><span>../</span><span>../</span><span>' + schenaName.toUpperCase() + '</span><span class="parentKeyClass">/' + parentKey.toUpperCase() + '</span></div>')
                var filterId = divId.replace("div-", "filter-");
                var tableListHtml = '';
                tableListHtml += "<div class =\"filterObjDiv\"><input id=\"" + filterId + "\" class=\"filterObjInputClass\" onkeyup=\"filterTablesCust(event,'" + schemaType + "')\" type=\"text\" autocomplete=\"off\" placeholder=\"Search Tables\" value=\"\"/></div>";



                tableListHtml += "<div class='schemaTablesObjDivClass' id =\"" + divId + "\">";
                $.each(response, function (index) {
                    if (this.value == "Show More") {

                    } else {


                        tableListHtml += '<div title="' + connectionObj['CONNECTION_NAME'] + ' . ' + this.label + '" class = "visionObjectNameDivClass" style="cursor:pointer">'
                                + '<img class="visionTableIcon" src="images/GridDB.svg"/>'
                                + '<span> ' + this.label + '</span></div>';
                    }
                });
                tableListHtml += '</div>';

                var htmlDiv = $("#ConnInnerDiv").html(tableListHtml);
//                $("#previousIcon").click(function (evt) {
//                    $(".parentKeyClass").text("");
//                    if (schemaType == "ERP") {
//                        getDBtables(schenaName, schemaType, schemaType);
//                    } else {
//                        getDBtables(schenaName, schema, schemaType);
//                    }
//
//                });


                $("#previousIcon").click(function (evt) {
                    $(".parentKeyClass").text("");
                    if (schemaType == "ERP") {
                        getDBtables(schenaName, schema, schemaType);
                    } else {
                        getDBtables(schenaName, schema, schemaType);
                    }

                });

                $(".schemaTablesObjDivClass").on('mousedown', function (event) {
                    var target;
                    var tableName;
                    if ($(event.target).hasClass('visionObjectNameDivClass')) {
                        target = $(event.target);
                        tableName = target[0].textContent.trim();

                    } else {
                        target = $(event.target).parents('div.visionObjectNameDivClass')[0];
                        tableName = target.textContent.trim();
                    }
                    var rightClick = isRightClick(event);
                    if (rightClick && target != null) {
                        $(".visionObjectNameDivClass").removeClass("visionSelectedObject");
                        $(target).addClass("visionSelectedObject");
                        $(".visionObjectNameDivClass").find('span').removeClass("visionHighlightTables");
                        $(target).find('span').addClass("visionHighlightTables");
                        var menuItems = "";
                        var menuHeight;
                        var columnObj = globalTreeObj['treeColumnObj'][5];
                        var initParams = columnObj.TREE_INIT_PARAMS;
                        if (initParams != null) {
                            var rightClickFunc = initParams.uuu_RightClickFunc;
                            if (rightClickFunc != null) {
                                var options = rightClickFunc.split(";");
                                menuHeight = options.length;
                                $.each(options, function (index) {
                                    menuItems += "<li onclick=viewTableDataFromConnTabs('" + tableName + "','" + schenaName + "','" + schema + "','" + schemaType + "')>View Data</li>";
                                });
                            }
                        }
                        $("#jqxMenu").remove();
                        $(".etl-page-body").append("<div id='jqxMenu'><ul></ul></div>");
                        $("#jqxMenu ul").html(menuItems);
                        var contextMenu = $("#jqxMenu").jqxMenu({width: '120px', height: menuHeight * 30 + 'px', autoOpenPopup: false, mode: 'popup', theme: 'energyblue'});
                        var scrollTop = $(window).scrollTop();
                        var scrollLeft = $(window).scrollLeft();
                        contextMenu.jqxMenu('open', parseInt(event.clientX) + 5 + scrollLeft, parseInt(event.clientY) + 5 + scrollTop);
                        return false;
                    }
                });


                $("div.visionObjectNameDivClass").draggable({
                    cursor: "move",
                    opacity: 0.7,
                    helper: 'clone',
//                                appendTo: 'body',
                    zIndex: 1000,
                    helper: function (event, ui) {
                        var $this = $(this);
                        var innerText = $this.text();
                        var title = innerText;
                        var descripttion = $this.attr("title");
                        var value = this.parentElement.id.split("-")[1];
                        var connectionObj;
                        if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
                        {
                            var conObj = savedDBData[value];
                            if (conObj != null && !jQuery.isEmptyObject(conObj)) {
                                connectionObj = conObj;
                            }

                        }

                        var operatorData = {
                            top: event.screenX,
                            left: event.screenY,
                            statusLabel: innerText.trim(),
                            tableName: (connectionObj['CONN_CUST_COL1'] != 'SAP_ECC' || connectionObj['CONN_CUST_COL1'] != 'SAP_HANA') ? (connectionObj['CONN_USER_NAME'] + '.' + innerText.trim()) : innerText.trim(),
                            dragType: "Table",
                            CONNECTION_NAME: connectionObj['CONNECTION_NAME'], //CONNECTION_NAME
                            CONN_DB_NAME: connectionObj['CONN_DB_NAME'], //CONN_DB_NAME
                            CONN_CUST_COL1: connectionObj['CONN_CUST_COL1'], //CONN_CUST_COL1
                            connObj: connectionObj,
                            properties: {
                                body: '<div  title="' + connectionObj['CONNECTION_NAME'] + ' . ' + title + '" class=""><div title="Double Click here to view the Transformations rules(' + connectionObj['CONNECTION_NAME'] + '.' + title + ')" style="cursor:pointer"><img src="images/GridDB.svg"'
                                        + 'class="visionOpIcons"  ondblclick=viewTrfmRules()  style="width:18px;height: 18px;padding-top: 6px;"/></div>'

                                        + '</div><div class="visionOpLabelDiv" title="Double click to Rename" ondblclick="renameOperatorDisplayLabel(\'' + title + '\',this)" style="width:' + (title.length * 5) + 'px;margin-left:' + (((title.length * -5) / 2) + 14) + 'px;">' + title + '</div>',
                                inputs: {
                                    input_1: {
                                        label: '',
                                        multipleLinks: true
                                    }
                                },
                                outputs: {
                                    output_1: {
                                        label: '',

                                    }
                                }
                            }
                        };
                        var contentSplitterStyle = $("#contentSplitter").css("display");

                        var contentSplitter1Style = $("#contentSplitter1").css("display");

                        var $flowchart;

                        if (contentSplitterStyle == "block") {
                            $flowchart = $('#' + flowChartWorkSpaceId);

                        } else if (contentSplitter1Style == "block") {
                            $flowchart = $('#dataModellerFlowchartworkSourcesspace');

                        }
                        $("#draggableOperatorId").remove();
                        var body = '<div id="draggableOperatorId" title="' + connectionObj['CONNECTION_NAME'] + ' . ' + this.label + '" class = "visionObjectNameDiv" style="cursor:pointer">'
                                + '<img class="visionTableIcon" src="images/GridDB.svg"/>'
                                + '<span > ' + innerText + '</span></div>';
                        $("body").append(body);
                        return  $("#draggableOperatorId");

                    },
                    stop: function (e, ui) {
                        var selItems = [];
                        if (!(selectedItems != null && selectedItems.length != null && selectedItems.length > 0)) {
                            selItems[0] = this.innerText;
                        } else {
                            selItems = selectedItems;

                        }
                        $(".flowchart-operator-connector-label").hide();

                        var contentSplitterStyle = $("#contentSplitter").css("display");

                        var contentSplitter1Style = $("#contentSplitter1").css("display");

                        var $flowchart;
                        var $container;
                        if (contentSplitterStyle == "block") {
                            $flowchart = $('#' + flowChartWorkSpaceId);
                            $container = $('#' + flowChartWorkSpaceId);
                        } else if (contentSplitter1Style == "block") {
                            $flowchart = $('#dataModellerFlowchartworkSourcesspace');
                            $container = $('#dataModellerFlowchartworkSourcesspace');
                        }
                        var elOffset = ui.offset;
                        var containerOffset = $container.offset();
                        if (elOffset.left > containerOffset.left &&
                                elOffset.top > containerOffset.top &&
                                elOffset.left < containerOffset.left + $container.width() &&
                                elOffset.top < containerOffset.top + $container.height()) {
                            var flowchartOffset = $flowchart.offset();
                            var relativeLeft = elOffset.left - flowchartOffset.left;
                            var relativeTop = elOffset.top - flowchartOffset.top;
                            var positionRatio = $flowchart.flowchart('getPositionRatio');
                            relativeLeft /= positionRatio;
                            relativeTop /= positionRatio;
                            elOffset.left = relativeLeft;
                            elOffset.top = relativeTop;
                        }
                        var top = elOffset.top;
                        for (var j = 0; j < selItems.length; j++) {
                            // ravi start
                            trfmRulesChanged = true;
                            // ravi end
                            var $this = $(this);
                            var innerText = selItems[j];
                            var title = innerText;
                            var value = this.parentElement.id.split("-")[1];
                            var connectionObj;
                            if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
                            {
                                var conObj = savedDBData[value];
                                if (conObj != null && !jQuery.isEmptyObject(conObj)) {
                                    connectionObj = conObj;
                                }

                            }

                            var data = {
                                top: top,
                                left: elOffset.left,
                                statusLabel: innerText.trim(),
                                tableName: (connectionObj['CONN_CUST_COL1'] != 'SAP_ECC' || connectionObj['CONN_CUST_COL1'] != 'SAP_HANA') ? (connectionObj['CONN_USER_NAME'] + '.' + innerText.trim()) : innerText.trim(),
                                CONNECTION_NAME: connectionObj['CONNECTION_NAME'], //CONNECTION_NAME
                                CONN_DB_NAME: connectionObj['CONN_DB_NAME'], //CONN_DB_NAME
                                CONN_CUST_COL1: connectionObj['CONN_CUST_COL1'], //CONN_CUST_COL1
                                dragType: "Table",
                                connObj: connectionObj,
                                properties: {

                                    body: '<div  title="' + connectionObj['CONNECTION_NAME'] + ' . ' + title + '" class=""><div title="Double Click here to view the Transformations rules(' + connectionObj['CONNECTION_NAME'] + '.' + title + ')" style="cursor:pointer"><img src="images/GridDB.svg"'
                                            + 'class="visionOpIcons"  ondblclick=viewTrfmRules()  style="width:18px;height: 18px;padding-top: 6px;"/></div>'

                                            + '</div><div class="visionOpLabelDiv" title="Double click to Rename" ondblclick="renameOperatorDisplayLabel(\'' + title + '\',this)" style="width:' + (title.length * 5) + 'px;margin-left:' + (((title.length * -5) / 2) + 14) + 'px;">' + title + '</div>',
                                    inputs: {
                                        input_1: {
                                            label: 'I-MAP',
                                            multipleLinks: true
                                        }
                                    },
                                    outputs: {
                                        output_1: {
                                            label: 'O-MAP',

                                        }
                                    }
                                }
                            };
                            var operatorId = $flowchart.flowchart('addOperator', data);
//                        $dataModellerFlowchart.flowchart('addOperator', data);
                            $(".flowchart-operator-connector-label").hide();
                            $(".flowchart-operator-title").hide();
                            top = top + 70;
//                            var component = $(this).attr("component");
//                            if (component != "Y") {
//                                inputOutPutPopUp(operatorId);
//                            }
                        }
                    }

                });
                var timeout;
                $("#savedConnectionsModified").unbind("scroll").on("scroll", function (event) {
                    console.log("scrolled ;;")
                    if (($(this).scrollTop() + $(this).innerHeight() >= $(this)[0].scrollHeight) && ($(this)[0].scrollHeight > $(this).innerHeight())) {
                        console.log("iam in scroll functionality...........");
                        clearTimeout(timeout);
                        timeout = setTimeout(function () {
                            loadDataOnScroll(event, schemaType);
                        }, 50);
                    }
                });



            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }
    });

}

function loadDataOnScroll(event, schemaType) {

    var childDiv = $(event.target).children();
    var childDivArr = childDiv[1];
    if (childDivArr != null && childDivArr != 'undefined') {
        var childDiv = childDivArr['children'];


        var divId = childDiv[1]['id'];

        // filter issue
        var filterDivId = divId.replace("div-", "filter-");
        var filterValue = $("#" + filterDivId).val();
        if (filterValue != null && filterValue != "") {
            filterValue = "%" + filterValue + "%";
        }
// filter issue
        var value = divId.split("-")[1];
        var parentkey = divId.split("-")[2].toUpperCase();

        var level = '4';
        var arr = $("[id='" + divId + "']").find(".visionObjectNameDivClass");
        var startIndex = arr.length + 1;
        var endIndex = arr.length + parseInt($("#treePageSize").val());
        var connectionObj;
        var tableListHtml = '';
        if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
        {
            var conObj = savedDBData[value];
            if (conObj != null && !jQuery.isEmptyObject(conObj)) {
                connectionObj = conObj;
            }

        }


        var columnsObj = globalTreeObj['treeColumnObj'];
        var extTreeParams = $("#extTreeParams").val();
        var data = {
            parentkey: parentkey,
            treeId: globalTreeObj['treeId'],
            level: level,
            filterValue: filterValue,
            filterCondition: "LIKE",
            extTreeParams: extTreeParams,
            columnsObj: JSON.stringify(columnsObj),
            connectionObj: JSON.stringify(connectionObj),
            startIndex: startIndex,
            endIndex: endIndex
        };
        if (schemaType == "ERP") {
            url = 'getTreeErpConnectionDetails'

        } else if (schemaType == "Oracle_ERP") {
            url = 'fetchSchemaTables'
        } else {
            url = 'fetchSchemaTables'
        }



        $.ajax({
            type: "post",
            traditional: true,
            dataType: 'json',
            url: url,
            cache: false,
            data: data,
            success: function (data, status, xhr) {

                $.each(data, function (index) {
                    if (this.label == "Show More...") {

                    } else {
                        tableListHtml += '<div title=' + connectionObj['CONNECTION_NAME'] + ' . ' + this.label + ' class = "visionObjectNameDivClass">\n\
                                     <img class="visionTableIcon" src="images/GridDB.svg"/><span> ' + this.label + '</span></div>'
                    }
                });
                $("#" + divId).append(tableListHtml);
                var i = 0;
                var singleSelectedItem;
                $("div.visionObjectNameDivClass").click(function (e) {
                    if (e.ctrlKey) {
                        $(this).toggleClass('selectedTabs', true);
                        if (singleSelectedItem !== null && singleSelectedItem !== '' && singleSelectedItem !== undefined) {
                            selectedItems[0] = singleSelectedItem;
                        }
                        selectedItems[i] = $(this).text();
                        i++;
                    } else {
                        $(this).addClass("selectedTabs").siblings().removeClass('selectedTabs');
                        singleSelectedItem = $(this).text();
                        selectedItems = [];
                        if (singleSelectedItem !== null && singleSelectedItem !== '' && singleSelectedItem !== undefined) {
                            i = 1;
                        } else {
                            i = 0;
                        }
                    }
                });
                $("div.visionObjectNameDivClass").draggable({
                    cursor: "move",
                    opacity: 0.7,
                    helper: 'clone',
//                                appendTo: 'body',
                    zIndex: 1000,
                    helper: function (event, ui) {
                        var $this = $(this);
                        var innerText = $this.text();
                        var title = innerText;
                        var descripttion = $this.attr("title");
                        var value = this.parentElement.id.split("-")[1];
                        var connectionObj;
                        if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
                        {
                            var conObj = savedDBData[value];
                            if (conObj != null && !jQuery.isEmptyObject(conObj)) {
                                connectionObj = conObj;
                            }

                        }

                        var operatorData = {
                            top: event.screenX,
                            left: event.screenY,
                            statusLabel: innerText.trim(),
                            tableName: (connectionObj['CONN_CUST_COL1'] != 'SAP_ECC' || connectionObj['CONN_CUST_COL1'] != 'SAP_HANA') ? (connectionObj['CONN_USER_NAME'] + '.' + innerText.trim()) : innerText.trim(),
                            dragType: "Table",
                            CONNECTION_NAME: connectionObj['CONNECTION_NAME'], //CONNECTION_NAME
                            CONN_DB_NAME: connectionObj['CONN_DB_NAME'], //CONN_DB_NAME
                            CONN_CUST_COL1: connectionObj['CONN_CUST_COL1'], //CONN_CUST_COL1
                            connObj: connectionObj,
                            properties: {
                                body: '<div  title="' + connectionObj['CONNECTION_NAME'] + ' . ' + title + '" class=""><div title="Double Click here to view the Transformations rules(' + connectionObj['CONNECTION_NAME'] + '.' + title + ')" style="cursor:pointer"><img src="images/GridDB.svg"'
                                        + 'class="visionOpIcons"  ondblclick=viewTrfmRules()  style="width:18px;height: 18px;padding-top: 6px;"/></div>'

                                        + '</div><div class="visionOpLabelDiv" title="Double click to Rename" ondblclick="renameOperatorDisplayLabel(\'' + title + '\',this)" style="width:' + (title.length * 5) + 'px;margin-left:' + (((title.length * -5) / 2) + 14) + 'px;">' + title + '</div>',
                                inputs: {
                                    input_1: {
                                        label: '',
                                        multipleLinks: true
                                    }
                                },
                                outputs: {
                                    output_1: {
                                        label: '',

                                    }
                                }
                            }
                        };
                        var contentSplitterStyle = $("#contentSplitter").css("display");

                        var contentSplitter1Style = $("#contentSplitter1").css("display");

                        var $flowchart;


                        $flowchart = $('#' + flowChartWorkSpaceId);


                        $("#draggableOperatorId").remove();
                        var body = '<div id="draggableOperatorId" title="' + connectionObj['CONNECTION_NAME'] + ' . ' + this.label + '" class = "visionObjectNameDiv" style="cursor:pointer">'
                                + '<img class="visionTableIcon" src="images/GridDB.svg"/>'
                                + '<span> ' + innerText + '</span></div>';
                        $("body").append(body);
                        return  $("#draggableOperatorId");

                    },
                    stop: function (e, ui) {
                        var selItems = [];
                        if (!(selectedItems != null && selectedItems.length != null && selectedItems.length > 0)) {
                            selItems[0] = this.innerText;
                        } else {
                            selItems = selectedItems;

                        }
                        $(".flowchart-operator-connector-label").hide();

                        var contentSplitterStyle = $("#contentSplitter").css("display");

                        var contentSplitter1Style = $("#contentSplitter1").css("display");

                        var $flowchart;
                        var $container;
                        if (contentSplitterStyle == "block") {
                            $flowchart = $('#' + flowChartWorkSpaceId);
                            $container = $('#' + flowChartWorkSpaceId);
                        } else if (contentSplitter1Style == "block") {
                            $flowchart = $('#dataModellerFlowchartworkSourcesspace');
                            $container = $('#dataModellerFlowchartworkSourcesspace');
                        }
                        var elOffset = ui.offset;
                        var containerOffset = $container.offset();
                        if (elOffset.left > containerOffset.left &&
                                elOffset.top > containerOffset.top &&
                                elOffset.left < containerOffset.left + $container.width() &&
                                elOffset.top < containerOffset.top + $container.height()) {
                            var flowchartOffset = $flowchart.offset();
                            var relativeLeft = elOffset.left - flowchartOffset.left;
                            var relativeTop = elOffset.top - flowchartOffset.top;
                            var positionRatio = $flowchart.flowchart('getPositionRatio');
                            relativeLeft /= positionRatio;
                            relativeTop /= positionRatio;
                            elOffset.left = relativeLeft;
                            elOffset.top = relativeTop;
                        }
                        var top = elOffset.top;
                        for (var j = 0; j < selItems.length; j++) {
                            // ravi start
                            trfmRulesChanged = true;
                            // ravi end
                            var $this = $(this);
                            var innerText = selItems[j];
                            var title = innerText;
                            var value = this.parentElement.id.split("-")[1];
                            var connectionObj;
                            if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
                            {
                                var conObj = savedDBData[value];
                                if (conObj != null && !jQuery.isEmptyObject(conObj)) {
                                    connectionObj = conObj;
                                }

                            }


                            var data = {
                                top: top,
                                left: elOffset.left,
                                statusLabel: innerText.trim(),
                                tableName: (connectionObj['CONN_CUST_COL1'] != 'SAP_ECC' || connectionObj['CONN_CUST_COL1'] != 'SAP_HANA') ? (connectionObj['CONN_USER_NAME'] + '.' + innerText.trim()) : innerText.trim(),
                                CONNECTION_NAME: connectionObj['CONNECTION_NAME'], //CONNECTION_NAME
                                CONN_DB_NAME: connectionObj['CONN_DB_NAME'], //CONN_DB_NAME
                                CONN_CUST_COL1: connectionObj['CONN_CUST_COL1'], //CONN_CUST_COL1
                                dragType: "Table",
                                connObj: connectionObj,
                                properties: {

                                    body: '<div  title="' + connectionObj['CONNECTION_NAME'] + ' . ' + title + '" class=""><div title="Double Click here to view the Transformations rules(' + connectionObj['CONNECTION_NAME'] + '.' + title + ')" style="cursor:pointer"><img src="images/GridDB.svg"'
                                            + 'class="visionOpIcons"  ondblclick=viewTrfmRules()  style="width:18px;height: 18px;padding-top: 6px;"/></div>'

                                            + '</div><div class="visionOpLabelDiv" title="Double click to Rename" ondblclick="renameOperatorDisplayLabel(\'' + title + '\',this)" style="width:' + (title.length * 5) + 'px;margin-left:' + (((title.length * -5) / 2) + 14) + 'px;">' + title + '</div>',
                                    inputs: {
                                        input_1: {
                                            label: 'I-MAP',
                                            multipleLinks: true
                                        }
                                    },
                                    outputs: {
                                        output_1: {
                                            label: 'O-MAP',

                                        }
                                    }
                                }
                            };
                            var operatorId = $flowchart.flowchart('addOperator', data);
//                        $dataModellerFlowchart.flowchart('addOperator', data);
                            $(".flowchart-operator-connector-label").hide();
                            $(".flowchart-operator-title").hide();
                            top = top + 70;
//                            var component = $(this).attr("component");
//                            if (component != "Y") {
//                                inputOutPutPopUp(operatorId);
//                            }
                        }
                    }

                });
                stopLoader();
            },
            error: function (e) {
                console.log(e);
                sessionTimeout(e);
                stopLoader();
            }
        });
    }
}


function filterTablesCust(event, schemaType) {

    clearTimeout(timeOut);
    timeOut = setTimeout(function () {
        showLoader();
        var inputValue = $(event.target).val();
//if (inputValue.length<3){
//    return false;
//}
        var filterValue = $(event.target).val();
        if (filterValue != null && filterValue != "") {
            filterValue = "%" + filterValue + "%";
        }
        var inputId = $(event.target).attr("id");
        var divId = inputId.replace("filter-", "div-");
        var dbLabel = inputId.split("-")[1];
        var selectedLevelValue = inputId.split("-")[2].toUpperCase();
        var selectConnObj = savedDBData[dbLabel];
        var selectedLevel = '4'
        var selectColumnsObj = globalTreeObj['treeColumnObj']
        var selectBoxValue = "LIKE"

        selectedLevelValue = selectedLevelValue.toUpperCase();
        var item = $("#savedConnections").jqxTree('getSelectedItem');


//        var url = 'getTreePagingDataOpt';
        var url = 'getETLTreePagingDataOpt';


        if (schemaType == "ERP") {
            url = 'getTreeErpConnectionDetails'

        } else if (schemaType == "Oracle_ERP") {
            url = 'fetchSchemaTables'
        } else {
            url = 'fetchSchemaTables'
        }

        $.ajax({
            type: "post",
            traditional: true,
            dataType: 'json',
            url: url,
            cache: false,
            data: {
                parentkey: ((selectedLevelValue != null && selectedLevelValue != '') ? selectedLevelValue.toUpperCase() : ""),
                level: selectedLevel,
                columnsObj: JSON.stringify(selectColumnsObj),
                connectionObj: JSON.stringify(selectConnObj),
                filterValue: filterValue,
                filterCondition: selectBoxValue,
                startIndex: 0,
                endIndex: $("#treePageSize").val()
            },
            success: function (data, status, xhr) {
                var tableListHtml = "";
                stopLoader();
                $.each(data, function (index) {
                    if (this.label == "Show More...") {

                    } else {
                        tableListHtml += '<div class = "visionObjectNameDivClass" style="cursor:pointer"><img class="visionTableIcon" src="images/GridDB.svg"/><span> ' + this.label + '</span></div>'
                    }
                });


                $("[id='" + divId + "']").html(tableListHtml);
                selectedItems = [];
                var i = 0;
                var singleSelectedItem;
                $("div.visionObjectNameDivClass").click(function (e) {
                    $(document).find("input").blur();
                    if (e.ctrlKey) {
                        $(this).toggleClass("selectedTabs");
                        if (singleSelectedItem !== null && singleSelectedItem !== '' && singleSelectedItem !== undefined) {
                            selectedItems[0] = singleSelectedItem;
                        }
                        selectedItems[i] = $(this).text();
                        i++;
                    } else {
                        $(this).addClass("selectedTabs").siblings().removeClass('selectedTabs');
                        singleSelectedItem = $(this).text();
                        selectedItems = [];
                        if (singleSelectedItem !== null && singleSelectedItem !== '' && singleSelectedItem !== undefined) {
                            i = 1;
                        } else {
                            i = 0;
                        }

                    }
                });
                $("div.visionObjectNameDivClass").draggable({
                    cursor: "move",
                    opacity: 0.7,
                    helper: 'clone',
//                                appendTo: 'body',
                    zIndex: 1000,
                    helper: function (event, ui) {
                        var $this = $(this);
                        var innerText = $this.text();
                        var title = innerText;
                        var descripttion = $this.attr("title");
                        var value = this.parentElement.id.split("-")[1];
                        var connectionObj;
                        if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
                        {
                            var conObj = savedDBData[value];
                            if (conObj != null && !jQuery.isEmptyObject(conObj)) {
                                connectionObj = conObj;
                            }

                        }

                        var operatorData = {
                            top: event.screenX,
                            left: event.screenY,
                            statusLabel: innerText.trim(),
                            tableName: (connectionObj['CONN_CUST_COL1'] != 'SAP_ECC' || connectionObj['CONN_CUST_COL1'] != 'SAP_HANA') ? (connectionObj['CONN_USER_NAME'] + '.' + innerText.trim()) : innerText.trim(),
                            dragType: "Table",
                            CONNECTION_NAME: connectionObj['CONNECTION_NAME'], //CONNECTION_NAME
                            CONN_DB_NAME: connectionObj['CONN_DB_NAME'], //CONN_DB_NAME
                            CONN_CUST_COL1: connectionObj['CONN_CUST_COL1'], //CONN_CUST_COL1
                            connObj: connectionObj,
                            properties: {
                                body: '<div  title="' + connectionObj['CONNECTION_NAME'] + ' . ' + title + '" class=""><div title="Double Click here to view the Transformations rules(' + connectionObj['CONNECTION_NAME'] + '.' + title + ')" style="cursor:pointer"><img src="images/GridDB.svg"'
                                        + 'class="visionOpIcons"  ondblclick=viewTrfmRules()  style="width:18px;height: 18px;padding-top: 6px;"/></div>'

                                        + '</div><div class="visionOpLabelDiv" title="Double click to Rename" ondblclick="renameOperatorDisplayLabel(\'' + title + '\',this)" style="width:' + (title.length * 5) + 'px;margin-left:' + (((title.length * -5) / 2) + 14) + 'px;">' + title + '</div>',
                                inputs: {
                                    input_1: {
                                        label: '',
                                        multipleLinks: true
                                    }
                                },
                                outputs: {
                                    output_1: {
                                        label: '',

                                    }
                                }
                            }
                        };
                        var contentSplitterStyle = $("#contentSplitter").css("display");

                        var contentSplitter1Style = $("#contentSplitter1").css("display");

                        var $flowchart;

                        if (contentSplitterStyle == "block") {
                            $flowchart = $('#' + flowChartWorkSpaceId);

                        } else if (contentSplitter1Style == "block") {
                            $flowchart = $('#dataModellerFlowchartworkSourcesspace');

                        }
                        $("#draggableOperatorId").remove();
                        var body = '<div title="' + connectionObj['CONNECTION_NAME'] + ' . ' + this.label + '" class = "visionObjectNameDiv" style="cursor:pointer">'
                                + '<img class="visionTableIcon" src="images/GridDB.svg"/>'
                                + '<span class="draggableText"> ' + innerText + '</span></div>';
                        $("body").append(body);
                        return  $("#draggableOperatorId");

                    },
                    stop: function (e, ui) {
                        var selItems = [];
                        if (!(selectedItems != null && selectedItems.length != null && selectedItems.length > 0)) {
                            selItems[0] = this.innerText;
                        } else {
                            selItems = selectedItems;

                        }
                        $(".flowchart-operator-connector-label").hide();

                        var contentSplitterStyle = $("#contentSplitter").css("display");

                        var contentSplitter1Style = $("#contentSplitter1").css("display");

                        var $flowchart;
                        var $container;
                        if (contentSplitterStyle == "block") {
                            $flowchart = $('#' + flowChartWorkSpaceId);
                            $container = $('#' + flowChartWorkSpaceId);
                        } else if (contentSplitter1Style == "block") {
                            $flowchart = $('#dataModellerFlowchartworkSourcesspace');
                            $container = $('#dataModellerFlowchartworkSourcesspace');
                        }
                        var elOffset = ui.offset;
                        var containerOffset = $container.offset();
                        if (elOffset.left > containerOffset.left &&
                                elOffset.top > containerOffset.top &&
                                elOffset.left < containerOffset.left + $container.width() &&
                                elOffset.top < containerOffset.top + $container.height()) {
                            var flowchartOffset = $flowchart.offset();
                            var relativeLeft = elOffset.left - flowchartOffset.left;
                            var relativeTop = elOffset.top - flowchartOffset.top;
                            var positionRatio = $flowchart.flowchart('getPositionRatio');
                            relativeLeft /= positionRatio;
                            relativeTop /= positionRatio;
                            elOffset.left = relativeLeft;
                            elOffset.top = relativeTop;
                        }
                        var top = elOffset.top;
                        for (var j = 0; j < selItems.length; j++) {
                            // ravi start
                            trfmRulesChanged = true;
                            // ravi end
                            var $this = $(this);
                            var innerText = selItems[j];
                            var title = innerText;
                            var value = this.parentElement.id.split("-")[1];
                            var connectionObj;
                            if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
                            {
                                var conObj = savedDBData[value];
                                if (conObj != null && !jQuery.isEmptyObject(conObj)) {
                                    connectionObj = conObj;
                                }

                            }


                            var data = {
                                top: top,
                                left: elOffset.left,
                                statusLabel: innerText.trim(),
                                tableName: (connectionObj['CONN_CUST_COL1'] != 'SAP_ECC' || connectionObj['CONN_CUST_COL1'] != 'SAP_HANA') ? (connectionObj['CONN_USER_NAME'] + '.' + innerText.trim()) : innerText.trim(),
                                CONNECTION_NAME: connectionObj['CONNECTION_NAME'], //CONNECTION_NAME
                                CONN_DB_NAME: connectionObj['CONN_DB_NAME'], //CONN_DB_NAME
                                CONN_CUST_COL1: connectionObj['CONN_CUST_COL1'], //CONN_CUST_COL1
                                dragType: "Table",
                                connObj: connectionObj,
                                properties: {

                                    body: '<div  title="' + connectionObj['CONNECTION_NAME'] + ' . ' + title + '" class=""><div title="Double Click here to view the Transformations rules(' + connectionObj['CONNECTION_NAME'] + '.' + title + ')" style="cursor:pointer"><img src="images/GridDB.svg"'
                                            + 'class="visionOpIcons"  ondblclick=viewTrfmRules()  style="width:18px;height: 18px;padding-top: 6px;"/></div>'

                                            + '</div><div class="visionOpLabelDiv" title="Double click to Rename" ondblclick="renameOperatorDisplayLabel(\'' + title + '\',this)" style="width:' + (title.length * 5) + 'px;margin-left:' + (((title.length * -5) / 2) + 14) + 'px;">' + title + '</div>',
                                    inputs: {
                                        input_1: {
                                            label: 'I-MAP',
                                            multipleLinks: true
                                        }
                                    },
                                    outputs: {
                                        output_1: {
                                            label: 'O-MAP',

                                        }
                                    }
                                }
                            };

                            var operatorId = $flowchart.flowchart('addOperator', data);
//                        $dataModellerFlowchart.flowchart('addOperator', data);
                            $(".flowchart-operator-connector-label").hide();
                            $(".flowchart-operator-title").hide();
                            top = top + 70;
//                            var component = $(this).attr("component");
//                            if (component != "Y") {
//                                inputOutPutPopUp(operatorId);
//                            }
                        }
                    }

                });
                stopLoader();
            },
            error: function (e) {
                console.log(e);
                sessionTimeout(e);
                stopLoader();
            }
        });
    }, 1000);
}


function addSchemaToFav(connectionName, schemaType, schema) {
    $("#dialog").html(labelObject['you want to add these schema to favorites?'] != null ? labelObject['you want to add these schema to favorites?'] : 'you want to add these schema to favorites?');
    $("#dialog").dialog({
        title: (labelObject['Confirmation'] != null ? labelObject['Confirmation'] : 'Confirmation'),
        modal: true,
        height: 'auto',
        minWidth: 300,
        maxWidth: 'auto',
        fluid: true,
        buttons: [{
                text: (labelObject['Yes'] != null ? labelObject['Yes'] : 'Yes'),
                click: function () {
                    $(this).html("");
//                    $(this).dialog("close");
                    $(this).dialog("destroy");
                    ajaxStart();
                    $.ajax({
                        type: "post",
                        traditional: true,
                        dataType: 'html',
                        url: 'addSchemaToFav',
                        cache: false,
                        data: {
                            connectionName: connectionName,
                            schemaType: schemaType,
                            schema: schema,
                            schemaPriority: "FAVORITE"
                        },
                        success: function (response) {
                            ajaxStop();
                            if (response != null && response != "") {
                                $("#dialog1").html(response);
                                $("#dialog1").dialog({
                                    title: (labelObject["Message"] != null ? labelObject["Message"] : "Message"),
                                    modal: true,
                                    height: 'auto',
                                    minWidth: 300,
                                    maxWidth: 1000,
                                    fluid: true,
                                    buttons: [{
                                            text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                                            click: function () {
                                                $(this).html("");
//                                                $(this).dialog("close");
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
                        error: function (e)
                        {
                            sessionTimeout(e);
                        }
                    });
                }},
            {
                text: (labelObject['No'] != null ? labelObject['No'] : 'No'),
                click: function () {
                    $(this).html("");
//                    $(this).dialog("close");
                    $(this).dialog("destroy");
                }}],
        open: function () {
            $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
            $(this).closest(".ui-dialog").find(".ui-button").eq(2).addClass("dialogno");
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

function getFavSchemas() {
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'fetchFavSchemas',
        cache: false,
        data: {
            schemaPriority: "FAVORITE"
        },
        success: function (response) {
            ajaxStop();
            if (response != null && response != "") {

                $("#favSchemasDivInner").html(response['htmlDiv']);
                $("#savedConnectionsModifiedInnerFav img").on('mousedown', function (event) {
                    var target = $(event.target).parents()[0];
                    var connectionName = target['id'];
                    var schemaType = $("#" + connectionName).attr("div-type");
                    connectionName = connectionName.replace('Fav_', '');
                    var rightClick = isRightClick(event);
                    if (rightClick && target != null) {
                        var menuItems = "";
                        var menuHeight;
                        if (schemaType == null) {
                            var fileOrgName = target['id'];
                            var target = $(event.target).parents()[0];
                            var fileName = target['attributes']['fileoriginalname']['value'];
                            var filePath = target['attributes']['filename']['value'];
                            var filePathValue = target['attributes']['filepath']['value'];
                            $("#filePathValueHidden").val(filePathValue);
                            var fileObj = {
                                fileName: fileName,
                                filePath: fileOrgName,
                                fileType: "." + fileName.substr((fileName.lastIndexOf('.') + 1))
                            }
                            menuItems += "<li onclick=removeFavFile('" + fileName + "','" + fileOrgName + "')>Remove Fav</li>";
                            menuItems += "<li onclick=viewFileData('" + JSON.stringify(fileObj) + "')>view Data</li>";
                            menuHeight = 2;
                        } else {
                            menuItems += "<li onclick=removeFavSchema('" + connectionName + "','" + schemaType + "')>Remove Fav</li>";
                            menuHeight = 1;
                        }

                        $("#jqxMenu1").remove();
                        $(".etl-page-body").append("<div id='jqxMenu1'><ul></ul></div>");
                        $("#jqxMenu1 ul").html(menuItems);
                        var contextMenu = $("#jqxMenu1").jqxMenu({width: '120px', height: menuHeight * 27.5 + 'px', autoOpenPopup: false, mode: 'popup',
//                            theme: 'energyblue'
                        }); // ravi start
                        var scrollTop = $(window).scrollTop();
                        var scrollLeft = $(window).scrollLeft();
                        contextMenu.jqxMenu('open', parseInt(event.clientX) + 5 + scrollLeft, parseInt(event.clientY) + 5 + scrollTop);
                        return false;
                    }
                });

            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }
    });

}
function removeFavSchema(connectionName, schemaType) {
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        url: 'removeFavSchema',
        cache: false,
        data: {
            connectionName: connectionName,
            schemaType: schemaType
        },
        success: function (response) {
            ajaxStop();
            if (response != null && response != "") {
                getFavSchemas();

            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }
    });


}
function addFilesToFav(fileObjStr) {
    if (fileObjStr != null && fileObjStr != '') {
        var fileDataObj = JSON.parse(fileObjStr);
        if (fileDataObj != null) {
            var filePath = fileDataObj['filePath'];
            var fileName = fileDataObj['fileName'];
            var fileType = fileDataObj['fileType'];
        }

        $("#dialog").html(labelObject['you want to add these file to favorites?'] != null ? labelObject['you want to add these file to favorites?'] : 'you want to add these file to favorites?');
        $("#dialog").dialog({
            title: (labelObject['Confirmation'] != null ? labelObject['Confirmation'] : 'Confirmation'),
            modal: true,
            height: 'auto',
            minWidth: 300,
            maxWidth: 'auto',
            fluid: true,
            buttons: [{
                    text: (labelObject['Yes'] != null ? labelObject['Yes'] : 'Yes'),
                    click: function () {
                        $(this).html("");
//                        $(this).dialog("close");
                        $(this).dialog("destroy");
                        ajaxStart();
                        $.ajax({
                            type: "post",
                            traditional: true,
                            dataType: 'html',
                            url: 'addFilesToFav',
                            cache: false,
                            data: {
                                filePath: filePath,
                                fileName: fileName,
                                fileType: fileType,
                                filePriority: "FAVORITE"
                            },
                            success: function (response) {
                                ajaxStop();
                                if (response != null && response != "") {
                                    $("#dialog1").html(response);
                                    $("#dialog1").dialog({
                                        title: (labelObject["Message"] != null ? labelObject["Message"] : "Message"),
                                        modal: true,
                                        height: 'auto',
                                        minWidth: 300,
                                        maxWidth: 1000,
                                        fluid: true,
                                        buttons: [{
                                                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                                                click: function () {
                                                    $(this).html("");
//                                                    $(this).dialog("close");
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
                            error: function (e)
                            {
                                sessionTimeout(e);
                            }
                        });
                    }},
                {
                    text: (labelObject['No'] != null ? labelObject['No'] : 'No'),
                    click: function () {
                        $(this).html("");
//                        $(this).dialog("close");
                        $(this).dialog("destroy");
                    }}],
            open: function () {
                $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                $(this).closest(".ui-dialog").find(".ui-button").eq(2).addClass("dialogno");
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
}

function removeFavFile(fileName, fileOrgName) {
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        url: 'removeFavFile',
        cache: false,
        data: {
            fileName: fileName,
            fileOrgName: fileOrgName

        },
        success: function (response) {
            ajaxStop();
            if (response != null && response != "") {
                getFavSchemas();

            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }
    });
}

function viewTableDataFromConnTabs(parentkey, DBValue, connName, schemaType, connectionObj) {
    var tableName;
    var extTreeParams = $("#extTreeParams").val();
    var columnsObj = globalTreeObj['treeColumnObj'];
    var target = $("#jqxtabs").find(".visionSelectedObject")[0];
    if ((parentkey != null && parentkey != '' && DBValue != null && DBValue != '')) {
        tableName = parentkey;
    }

    var contentDivId = ("divGrid-" + DBValue + "-" + tableName).replace(/\s/g, '');
    var selectedItemIndex = $('#dataViewDiv').jqxTabs('selectedItem');
    if (selectedItemIndex != null) {
        var html = $('#dataViewDiv').html();
        if (html.indexOf('"' + contentDivId + '"') > -1
                || html.indexOf('"' + contentDivId.replace(/\//g, '') + '"') > -1) {
            var length = $('#dataViewDiv').jqxTabs('length');
            for (var i = 0; i < length; i++) {
                var content = $('#dataViewDiv').jqxTabs('getContentAt', i);
                var id = $(content).children('div:nth-child(3)').attr("id");
                if (id == null) {
                    id = $(content).children('div:first').children('div:nth-child(3)').attr("id");
                }
                if (id == contentDivId || id == contentDivId.replace(/\//g, '')) {

                    switchETLDesignTabs("li_contentView", "dataViewDiv");

                    $('#dataViewDiv').jqxTabs('select', i);
                    if (connName == "SAP") {
                        $('#dataViewDiv').jqxTabs('removeAt', i);

                    }
                    break;
                }
            }
            if (connName == "SAP") {
            } else {
                return false;
            }

        }

    }

    if (connectionObj == null || connectionObj == 'undefined') {
        if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
        {
            var conObj = savedDBData[DBValue];
            if (conObj != null && !jQuery.isEmptyObject(conObj)) {
                connectionObj = conObj;
            }

        }
    }

    var data = {
        parentkey: parentkey,
        treeId: globalTreeObj['treeId'],
        level: '5',
        extTreeParams: extTreeParams,
        columnsObj: JSON.stringify(columnsObj),
        connectionObj: JSON.stringify(connectionObj),
        startIndex: 0,
        endIndex: $("#treePageSize").val(),
        DBValue: DBValue,
        tableName: tableName
    };

    if (connName == null) {
        connName = connectionObj['CONN_CUST_COL1'];
    }

    viewTableDataGrid(data);
}
