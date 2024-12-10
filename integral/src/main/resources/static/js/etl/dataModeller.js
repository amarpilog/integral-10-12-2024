/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


//UTTEZZ


function createCubeTables() {

    var contentSplitterStyle = $("#contentSplitter").css("display");
    var contentSplitter1Style = $("#contentSplitter1").css("display");
    var $flowchart;
    var $container;
    if (contentSplitterStyle == "block") {
        $flowchart = $('#flowchartworkSourcesspace');
        $container = $('#feedContentArea');
    } else if (contentSplitter1Style == "block") {
        $flowchart = $('#dataModellerFlowchartworkSourcesspace');
        $container = $('#feedContentArea1');
    }
    showLoader();
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    var selectedToOperatorId = $("#currentTrnsOpId").val();
    try {
        if (selectedToOperatorId != null && selectedToOperatorId != '') {
            selectedToOperatorId = parseInt(selectedToOperatorId);
        }
    } catch (e) {
    }
    var t_rules = {};

    var previousTargetOperatorData = $flowchart.flowchart('getOperatorData', selectedToOperatorId);

    var isEmpty = jQuery.isEmptyObject(previousTargetOperatorData);

    if (!isEmpty) {
        if (previousTargetOperatorData['trfmRules-data'] != null) {
            t_rules = previousTargetOperatorData['trfmRules-data'];
        }

        var componentOperator = {};
        var fromOpLinkArray = $flowchart.flowchart('getAllFromOperatorsByToOpId', selectedToOperatorId);
        if (fromOpLinkArray != null) {
            if (fromOpLinkArray != null && fromOpLinkArray.length != 0) {
                for (var i = 0; i < fromOpLinkArray.length; i++) {
                    var operatorData = fromOpLinkArray[i];
                    if (operatorData != null
                            && operatorData['iconType'] != null
                            && operatorData['iconType'] != '') {
                        componentOperator = operatorData;
                        break;
                    }

                }
            }

        }


        if (componentOperator['iconType'] == "MAP") {
            var destinationColumnEmpty = false;

            var colMappingsData = []

            var rowCount = $('#sourceDestColsTableId tbody tr').length;
            var columnCount = $('#sourceDestColsTableId th').length;
            for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                var rowData = {};
                var tr = $('#sourceDestColsTableId tbody tr')[rowIndex];
                var destinationColumnStyle = $(tr.children[2]).attr("style");

                var destinationColumnVal = $(tr.children[2]).find('input').val();

                if (destinationColumnStyle == "display:none;" || destinationColumnStyle == "display:none") {
                    destinationColumnVal = "N/A:N/A";
                }
                if (destinationColumnVal == null || destinationColumnVal == "") {
                    destinationColumnEmpty = true;
                }


                if (destinationColumnEmpty) {
                    showMesg("Destination Column Cannot be empty");
                    throw new Error();
                }

                //        if (destinationColumnVal != null &&
                //                destinationColumnVal != "" &&
                //                destinationColumnVal != "undefined"
                //                && destinationColumnVal.indexOf(":")==-1){
                //            destinationColumnVal = "N/A:"+destinationColumnVal; // ravi process job issues 
                //        }

                var dataFunobjStr = $(tr.children[5]).find('input').attr("data-funobjstr");
                //funobjstr
                var funObj = {};
                if (dataFunobjStr != null && dataFunobjStr != '' && dataFunobjStr != 'null') {
                    funObj['funobjstr'] = dataFunobjStr;
                }
                rowData['primaryKey'] = $(tr.children[1]).find('input').prop('checked') ? "Y" : "N";
                rowData['destinationColumn'] = destinationColumnVal;
                rowData['sourceColumn'] = $(tr.children[3]).find('input').val();
                rowData['defaultValue'] = $(tr.children[4]).find('input').val();
                rowData['appendValue'] = $(tr.children[5]).find('input').val();
                rowData['columnClause'] = (dataFunobjStr != null && dataFunobjStr != '' && dataFunobjStr != 'null') ? funObj : $(tr.children[6]).find('input').val();
                rowData['data-funobjstr'] = $(tr.children[6]).find('input').attr("data-funobjstr"); // ravi change new
                rowData['data-columnClause'] = $(tr.children[6]).find('input').val(); // ravi change new
                colMappingsData[rowIndex] = rowData;
                if (rowData['primaryKey'] == 'Y') {
                    t_rules['primaryKey'] = "Y";
                }
            }

            t_rules['colMappingsData'] = colMappingsData;

            // ---------------------------------

            var masterTable = "";
            var childTables = [];
            var fromTables = [];

            var rowCount = $('#EtlMappingTable thead tr').length;
            for (var rowIndex = 1; rowIndex < rowCount; rowIndex++) {
                var rowData = {};
                var tr = $('#EtlMappingTable thead tr')[rowIndex];
                fromTables.push($(tr.children[0]).find('select').val());
                if (rowIndex == 1) {
                    masterTable = $(tr.children[0]).find('select').val();
                } else {
                    childTables.push($(tr.children[0]).find('select').val());
                }
            }

            if (masterTable == "") {

                var mapingObj = $flowchart.flowchart('getMapOperatorData', selectedToOperatorId);
                if (mapingObj != null && !jQuery.isEmptyObject(mapingObj)) {
                    var fromOpArray = mapingObj['fromOpArray'];
                }
                if (fromOpArray != null && fromOpArray.length != 0) {
                    masterTable = fromOpArray[0]['tableName'];
                }
                //            try{
                //               masterTable = $($(".visionOpIcons")[0]).parent().next().text().trim();
                //            }catch(e){}
            }

            t_rules['masterTableName'] = masterTable;
            t_rules['masterTableName'] = masterTable;
            t_rules['childTables'] = childTables;
            t_rules['fromTables'] = fromTables;
            //------------------------------------

            var joinClauseData = []
            var mapIcons = $("#EtlMappingTable").find(".visionEtlJoinClauseMapIcon");

            $.each(mapIcons, function (index) {
                var mappedColumnData = $(this).attr('data-mappedcolumns');
                if (mappedColumnData != null
                        && mappedColumnData != ''
                        && mappedColumnData != 'null'
                        && mappedColumnData != '{}') {
//                    if (mappedColumnData == "") {
//                        mappedColumnData = "{}";
//                    }
                    joinClauseData[index] = mappedColumnData;
                }
            });
            if (joinClauseData.length > 0) {
                t_rules['joinClauseData'] = joinClauseData;
                var fromTablesList = $(".sourceJoinColsTd").find("select");
                if (fromTablesList.lenght > 0) {
                    var reOrderFromTables = [];
                    $.each(fromTablesList, function (i) {
                        reOrderFromTables.push(this.value);
                    })
                    t_rules['fromTables'] = reOrderFromTables;
                }
            }

            //------------------------------------
            var whereClauseData = []
            var mapIcons = $("#selectedTables").find(".visionEtlWhereClauseMapIcon");
            $.each(mapIcons, function (index) {
                var mappedColumnData = $(this).attr('data-whereclause');
                if (mappedColumnData != null) {
                    if (mappedColumnData == "") {
                        mappedColumnData = "{}";
                    }
                    whereClauseData[index] = mappedColumnData;
                }

            });
            if (whereClauseData.length > 0) {
                t_rules['whereClauseData'] = whereClauseData;
            }
            //---------------------------------------------------- advanced settings  
            var selectTabObj = {};
            var uniqueRowsFlag = "N";
            if ($("#distinctRowsInput").is(":checked")) {
                uniqueRowsFlag = "Y";
            }
            selectTabObj['uniqueRowsFlag'] = uniqueRowsFlag;

            var minRows = $("#rowsCountFromInput").val();
            var maxRows = $("#rowsCountToInput").val();
            if (!(minRows != null && minRows != '') && (maxRows != null && maxRows != '')) {
                minRows = 0;
            }
            selectTabObj['minRows'] = minRows;
            selectTabObj['maxRows'] = maxRows;

            var operatorType = $("#operatorType").val();
            selectTabObj['operatorType'] = operatorType;

            var scdType = $('input[name="scdType"]:checked').val();
            if (scdType != null) {
                selectTabObj['scdType'] = scdType;
            }

            t_rules['selectTabObj'] = selectTabObj;

            //------------------------------------

            var orderByData = []

            var rowCount = $('#fromTablesOrderCauseTable tbody tr').length;
            for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                var rowData = {};
                var tr = $('#fromTablesOrderCauseTable tbody tr')[rowIndex];
                rowData['columnName'] = $(tr.children[1]).find('input').val();
                rowData['order'] = $(tr.children[2]).find('select').val();
                orderByData[rowIndex] = rowData;
            }
            t_rules['orderByData'] = orderByData;
            //------------------------------------

            var groupByData = []

            var rowCount = $('#fromTablesGroupCauseTable tbody tr').length;
            for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                var rowData = {};
                var tr = $('#fromTablesGroupCauseTable tbody tr')[rowIndex];
                rowData['columnName'] = $(tr.children[1]).find('input').val();
                groupByData[rowIndex] = rowData;
            }
            t_rules['groupByData'] = groupByData;

        } else if (componentOperator['iconType'] == "UNGROUP") {

            var normalizeOptionsObj = {};
            normalizeOptionsObj['normalizeFlag'] = "normalize";
            normalizeOptionsObj['itemSeparator'] = $("#itemSeparator").val();
            normalizeOptionsObj['normalizeColumn'] = $("#selectNormalizeColHeader").val();
            t_rules['normalizeOptionsObj'] = normalizeOptionsObj;
            var colMappingsData = [];
            var fromColsArray = $("#selectNormalizeColHeader").find("option");
            if (fromColsArray != null && fromColsArray.length > 0) {
                $.each(fromColsArray, function (rowIndex) {
                    if (rowIndex != 0) {
                        var rowData = {};
                        var destFileHeaders = t_rules['fileHeaders'];
                        if (destFileHeaders != null) {
                            var destinationColumn = destFileHeaders[rowIndex];
                        }
                        rowData['sourceColumn'] = this.value;
                        rowData['destinationColumn'] = (destinationColumn != null) ? destinationColumn : this.value;
                        colMappingsData[rowIndex - 1] = rowData;
                    }

                })

                t_rules['colMappingsData'] = colMappingsData;
            }
        } else if (componentOperator['iconType'] == "GROUP") {

            var deNormalizeOptionsObj = {};
            deNormalizeOptionsObj['normalizeFlag'] = "deNormalize";
            deNormalizeOptionsObj['delimiter'] = $("#delimiter").val();
            deNormalizeOptionsObj['deNormalizeColumn'] = $("#selectDenormalizeColHeader").val();

            deNormalizeOptionsObj['keyColumn'] = $("#selectDenormalizeKeyColumn").val(); // ravi updated code

            t_rules['normalizeOptionsObj'] = deNormalizeOptionsObj;
            var colMappingsData = [];
            var fromColsArray = $("#selectDenormalizeColHeader").find("option");
            if (fromColsArray != null && fromColsArray.length > 0) {
                $.each(fromColsArray, function (rowIndex) {
                    if (rowIndex != 0) {
                        var rowData = {};
                        var destFileHeaders = t_rules['fileHeaders'];
                        if (destFileHeaders != null) {
                            var destinationColumn = destFileHeaders[rowIndex];
                        }
                        rowData['sourceColumn'] = this.value;
                        rowData['destinationColumn'] = (destinationColumn != null) ? destinationColumn : this.value;
                        colMappingsData[rowIndex - 1] = rowData;
                    }

                })

                t_rules['colMappingsData'] = colMappingsData;
            }
        }

        previousTargetOperatorData['trfmRules-data'] = t_rules;
        previousTargetOperatorData['targetOperator'] = "Y";
        $flowchart.flowchart('setOperatorData', selectedToOperatorId, previousTargetOperatorData);
        var previousTargetOperatorData2 = $flowchart.flowchart('getOperatorData', selectedToOperatorId);
        $(".flowchart-operator-connector-label").hide();
        $(".flowchart-operator-title").hide();
        var data = $('#flowchartworkSourcesspace').flowchart('getData');

        var operators = data['operators'];
        $.each(operators, function (i) {
            if (this['targetOperator'] == 'Y') {
                this['timeStamp'] = new Date().getTime();
                $flowchart.flowchart('setOperatorData', parseInt(this['operatorId']), this);
            }
        });
    }


    var data = {};
    data = $flowchart.flowchart('getData');

    showLoader();
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'json',
        cache: false,
        url: 'createCubeTable',
        async: true,
        data: {
            flowChartData: JSON.stringify(data)
        },
        success: function (response) {
            stopLoader();
            if (response != null) {
                var responeStr = "";

                $.each(response, function (indx) {
                    responeStr += this['message'] + '</br>';
                })
                $("#dialog").html(responeStr);
                $("#dialog").dialog({
                    title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
                    modal: true,
                    height: 'auto',
                    minWidth: 300,
                    maxWidth: 'auto',
                    fluid: true,
                    buttons: [{
                            text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                            click: function () {
                                if (response['messageFlag']) {
                                    $('#dialog').dialog('close');
                                    $(this).html("");
                                    $(this).dialog("close");
                                    $(this).dialog("destroy");
                                }
                                $(this).html("");
                                $(this).dialog("close");
                                $(this).dialog("destroy");
                            }}],
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
    //end
}

function saveCubeTable(createTableObj, dialogId) {
    var data = {};
    data = $('#dataModellerFlowchartworkSourcesspace').flowchart('getData');


}




function setCubeConnection($this, cubeType) {
    var selectedOperatorId = $('#dataModellerFlowchartworkSourcesspace').flowchart('getSelectedOperatorId');
    var selectedOperatorData = $('#dataModellerFlowchartworkSourcesspace').flowchart('getOperatorData', selectedOperatorId);
    var tableName = selectedOperatorData['tableName'];
    var connObj = selectedOperatorData['connObj'];
    var connName = selectedOperatorData['connName'];
    var connObjStr = (connObj != null) ? JSON.stringify(connObj) : "";
    connName = (connName != null) ? connName : "";

    var image, inputVal;
    if (cubeType == 'FACT') {
        image = 'images/F_icon.svg';
        inputVal = (tableName != null) ? tableName : 'FACT_';

    } else if (cubeType == 'DIMENSION') {
        image = 'images/D_icon.svg';
        inputVal = (tableName != null) ? tableName : 'DIM_';
    } else if (cubeType == 'AGGREGATE') {
        image = 'images/A_icon.svg';
        inputVal = (tableName != null) ? tableName : 'AGG_';
    }

    var property = selectedOperatorData['properties'];
    var body = $(property['body']);
    var removeImage = body.find('img').attr("src", "");
    var addImage = body.find('img').attr("src", image);
    var eleStr = body[0].outerHTML;
    selectedOperatorData['properties']['body'] = eleStr;
    selectedOperatorData['cubeType'] = cubeType;

    $('#dataModellerFlowchartworkSourcesspace').flowchart('setOperatorData', selectedOperatorId, selectedOperatorData);
    var str = '<div><table class="visionEtlCreateSQLDS" id="selectSourceDMConnection" style="width: 100%;" border="1"><tbody><tr><th width="50%" class="" style="background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center">Table Name</th><th width="50%" class="" style="background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center"> Connection</th></tr><tr><td><input id="cubeTableNAME" value="' + inputVal + '"/></td><td><input id="sourceConn_0" class="visionDSMappingInput" type="text" value="' + connName + '" data-conobjstr="' + connObjStr + '" data-tosystype="" readonly="true"><img src="images/tree_icon.svg" class="visionETLColMapImage " onclick="selectFunColumnValue(this,\'ALL_SCHEMA\',\'Data Source\',\'DB\')" style=""></td></tr>\n\
        </tbody></table></div>';
    $("#dialog").html(str);
    $("#dialog").dialog({
        modal: true,
        width: 500,
        height: 'auto',
        maxHeight: 700,
        fluid: true,
        buttons: {
            Ok: function () {
                $(this).dialog("close");
                var tableName = $("#cubeTableNAME").val();

                var connObj = $("#sourceConn_0").attr("data-conobjstr");
                connObj = JSON.parse(connObj);
                var connName = $("#sourceConn_0").val();

                selectedOperatorData['tableName'] = tableName;
                selectedOperatorData['connObj'] = connObj;
                selectedOperatorData['connName'] = connName;
                $('#dataModellerFlowchartworkSourcesspace').flowchart('setOperatorData', selectedOperatorId, selectedOperatorData);

                console.log("tName::" + tableName);
                console.log("conObj::" + connObj);
//                var selectedOperatorData = $('#dataModellerFlowchartworkSourcesspace').flowchart('getOperatorData', selectedOperatorId);
//                selectedOperatorData['cubeTableName'] = tName;
//                selectedOperatorData['cubeConnObj'] = conObj;
////               $('#dataModellerFlowchartworkSourcesspace').flowchart('setOperatorData', selectedOperatorId, selectedOperatorData);
            }
        },
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
//viewTrfmRules1();
}
function showCubeFACT($this) {
    var selectedOperatorId = $('#dataModellerFlowchartworkSourcesspace').flowchart('getSelectedOperatorId');
    var selectedOperatorData = $('#dataModellerFlowchartworkSourcesspace').flowchart('getOperatorData', selectedOperatorId);
    var property = selectedOperatorData['properties'];
    var body = $(property['body']);
    var removeImage = body.find('img').attr("src", "");
    var addImage = body.find('img').attr("src", "images/F_icon.svg");
    var eleStr = body[0].outerHTML;
    selectedOperatorData['properties']['body'] = eleStr;
    selectedOperatorData['cubeType'] = 'FACT';

    $('#dataModellerFlowchartworkSourcesspace').flowchart('setOperatorData', selectedOperatorId, selectedOperatorData);
    var str = '<div><table class="visionEtlCreateSQLDS" id="selectSourceDMConnection" style="width: 100%;" border="1"><tbody><tr><th width="50%" class="" style="background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center">Table Name</th><th width="50%" class="" style="background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center"> Connection</th></tr><tr><td><input id=\'cubeTableNAME\' value=\'FACT_\'/></td><td><input id="sourceConn_0" class="visionDSMappingInput" type="text" value="" data-conobjstr="" data-tosystype="" readonly="true"><img src="images/tree_icon.svg" class="visionETLColMapImage " onclick="selectFunColumnValue(this,\'ALL_SCHEMA\',\'Data Source\',\'DB\')" style=""></td></tr>\n\
        </tbody></table></div>';
    $("#dialog").html(str);
    $("#dialog").dialog({
        modal: true,
        width: 500,
        height: 'auto',
        maxHeight: 700,
        fluid: true,
        buttons: {
            Ok: function () {
                $(this).dialog("close");
                var tName = $("#cubeTableNAME").val();

                var conObj = $("#sourceConn_0").attr("data-conobjstr");

                selectedOperatorData['tName'] = tName;
                selectedOperatorData['conObj'] = conObj;
                $('#dataModellerFlowchartworkSourcesspace').flowchart('setOperatorData', selectedOperatorId, selectedOperatorData);

                console.log("tName::" + tName);
                console.log("conObj::" + conObj);
//                var selectedOperatorData = $('#dataModellerFlowchartworkSourcesspace').flowchart('getOperatorData', selectedOperatorId);
//                selectedOperatorData['cubeTableName'] = tName;
//                selectedOperatorData['cubeConnObj'] = conObj;
////               $('#dataModellerFlowchartworkSourcesspace').flowchart('setOperatorData', selectedOperatorId, selectedOperatorData);
            }
        },
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
//viewTrfmRules1();
}
function showCubeDIM($this) {
    var selectedOperatorId = $('#dataModellerFlowchartworkSourcesspace').flowchart('getSelectedOperatorId');
    var selectedOperatorData = $('#dataModellerFlowchartworkSourcesspace').flowchart('getOperatorData', selectedOperatorId);
    var property = selectedOperatorData['properties'];
    var body = $(property['body']);
    var removeImage = body.find('img').attr("src", "");
    var addImage = body.find('img').attr("src", "images/D_icon.svg");
    var eleStr = body[0].outerHTML;
    selectedOperatorData['properties']['body'] = eleStr;
    selectedOperatorData['cubeType'] = 'DIM';
    $('#dataModellerFlowchartworkSourcesspace').flowchart('setOperatorData', selectedOperatorId, selectedOperatorData);
    var str = '<div><table class="visionEtlCreateSQLDS" id="selectSourceDMConnection" style="width: 100%;" border="1"><tbody><tr><th width="50%" class="" style="background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center">Table Name</th><th width="50%" class="" style="background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center"> Connection</th></tr><tr><td><input id=\'cubeTableNAME\' value=\'DIM_\'/></td><td><input id="sourceConn_0" class="visionDSMappingInput" type="text" value="" data-conobjstr="" data-tosystype="" readonly="true"><img src="images/tree_icon.svg" class="visionETLColMapImage " onclick="selectFunColumnValue(this,\'ALL_SCHEMA\',\'Data Source\',\'DB\')" style=""></td></tr>\n\
        </tbody></table></div>';
    $("#dialog").html(str);
    $("#dialog").dialog({
        modal: true,
        width: 500,
        height: 'auto',
        maxHeight: 700,
        fluid: true,
        buttons: {
            Ok: function () {
                $(this).dialog("close");
                var tName = $("#cubeTableNAME").val();

                var conObj = $("#sourceConn_0").attr("data-conobjstr");
                selectedOperatorData['tName'] = tName;
                selectedOperatorData['conObj'] = conObj;
                $('#dataModellerFlowchartworkSourcesspace').flowchart('setOperatorData', selectedOperatorId, selectedOperatorData);

                console.log("tName::" + tName);
                console.log("conObj::" + conObj);
//                var selectedOperatorData = $('#dataModellerFlowchartworkSourcesspace').flowchart('getOperatorData', selectedOperatorId);
//                selectedOperatorData['cubeTableName'] = tName;
//                selectedOperatorData['cubeConnObj'] = conObj;
////               $('#dataModellerFlowchartworkSourcesspace').flowchart('setOperatorData', selectedOperatorId, selectedOperatorData);
            }
        },
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
    //viewTrfmRules1();
}
function showCubeAGG($this) {
    var selectedOperatorId = $('#dataModellerFlowchartworkSourcesspace').flowchart('getSelectedOperatorId');
    var selectedOperatorData = $('#dataModellerFlowchartworkSourcesspace').flowchart('getOperatorData', selectedOperatorId);
    var property = selectedOperatorData['properties'];
    var body = $(property['body']);
    var removeImage = body.find('img').attr("src", "");
    var addImage = body.find('img').attr("src", "images/A_icon.svg");
    var eleStr = body[0].outerHTML;
    selectedOperatorData['properties']['body'] = eleStr;
    selectedOperatorData['cubeType'] = 'AGG';
    $('#dataModellerFlowchartworkSourcesspace').flowchart('setOperatorData', selectedOperatorId, selectedOperatorData);
//viewTrfmRules1();
    var str = '<div><table class="visionEtlCreateSQLDS" id="selectSourceDMConnection" style="width: 100%;" border="1"><tbody><tr><th width="50%" class="" style="background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center">Table Name</th><th width="50%" class="" style="background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center"> Connection</th></tr><tr><td><input id=\'cubeTableNAME\' value=\'AGG_\'/></td><td><input id="sourceConn_0" class="visionDSMappingInput" type="text" value="" data-conobjstr="" data-tosystype="" readonly="true"><img src="images/tree_icon.svg" class="visionETLColMapImage " onclick="selectFunColumnValue(this,\'ALL_SCHEMA\',\'Data Source\',\'DB\')" style=""></td></tr>\n\
        </tbody></table></div>';
    $("#dialog").html(str);
    $("#dialog").dialog({
        modal: true,
        width: 500,
        height: 'auto',
        maxHeight: 700,
        fluid: true,
        buttons: {
            Ok: function () {
                $(this).dialog("close");
                var tName = $("#cubeTableNAME").val();

                var conObj = $("#sourceConn_0").attr("data-conobjstr");
                selectedOperatorData['tName'] = tName;
                selectedOperatorData['conObj'] = conObj;
                $('#dataModellerFlowchartworkSourcesspace').flowchart('setOperatorData', selectedOperatorId, selectedOperatorData);

                console.log("tName::" + tName);
                console.log("conObj::" + conObj);
//                var selectedOperatorData = $('#dataModellerFlowchartworkSourcesspace').flowchart('getOperatorData', selectedOperatorId);
//                selectedOperatorData['cubeTableName'] = tName;
//                selectedOperatorData['cubeConnObj'] = conObj;
////               $('#dataModellerFlowchartworkSourcesspace').flowchart('setOperatorData', selectedOperatorId, selectedOperatorData);
            }
        },
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


    function viewTrfmRules1() {
        var previousTargetOperatorData = $('#dataModellerFlowchartworkSourcesspace').flowchart('getOperatorData', cubePreviousOperatorId);
        var selectedOperatorId = $('#dataModellerFlowchartworkSourcesspace').flowchart('getSelectedOperatorId');
        var dataSourceName = $("#sourceConn_0").val();
        var dataSourceObj = $("#sourceConn_0").attr("data-conobjstr");
        var rowCount = $('#sourceDestColsTableId >tbody >tr').length;
        if (rowCount != 0) {
            var createTableObj = {};
//            createTableObj['tableName'] = tableName;
            createTableObj['dataSourceName'] = dataSourceName;
            createTableObj['dataSourceObj'] = dataSourceObj;
            var tableColsArray = [];
            $("#sourceDestColsTableId tbody tr").each(function () {
                var tdArray = this.cells;
                if (tdArray != null && tdArray.length != 0) {
                    var columnName = $(tdArray[2]).find("input").val();
                    columnName = columnName.substring(columnName.indexOf(":") + 1);
                    var dataType = "VARCHAR2(1600 CHAR)";
                    if (columnName != null && columnName != '' && dataType != null && dataType != '') {
                        var colsObj = {};
                        colsObj['COLUMN_NAME'] = columnName.substring(columnName.indexOf(":") + 1);

                        if ($(tdArray[1]).find("input[type=\"checkbox\"]").is(":checked")) {//input[type="checkbox"]:checked
                            colsObj['PK'] = "Y";
                        } else {
                            colsObj['PK'] = "N";
                        }
                        colsObj['DATA_TYPE'] = "VARCHAR2(1600 CHAR)";

                        tableColsArray.push(colsObj);
                    }

                }
            });
            createTableObj['colsObj'] = tableColsArray;
        }

        if (cubePreviousOperatorId != null) {
            var previousTargetOperatorData = $('#dataModellerFlowchartworkSourcesspace').flowchart('getOperatorData', cubePreviousOperatorId);
            previousTargetOperatorData['colArray'] = tableColsArray;
            if (previousTargetOperatorData != null) {
                $('#dataModellerFlowchartworkSourcesspace').flowchart('setOperatorData', cubePreviousOperatorId, previousTargetOperatorData);
            }
        }
        selectedOperatorId = parseInt(selectedOperatorId);
        if (selectedOperatorId == 0) {
            selectedOperatorId == selectedOperatorId + 1;
        }
        $("#currentTrnsOpId").val(selectedOperatorId);
        var selectedOperatorData = $('#dataModellerFlowchartworkSourcesspace').flowchart('getOperatorData', selectedOperatorId);
        if (selectedOperatorData != null && selectedOperatorData['iconType'] == 'SQL') {
            if (selectedOperatorData['dragType'] == null) {
                showMesg("Please select target table/Create target table by Right clicking on SQL icon.");
                return false;
            }
        }

        try {
            if (prevTargetOperatorId != null) { // new issues changes
                prevTargetOperatorId = parseInt(prevTargetOperatorId);
            }

        } catch (e) {
        }
        if (prevTargetOperatorId != null && parseInt(selectedOperatorId) != prevTargetOperatorId) { // latest changes
            var t_rules = {};
            var previousTargetOperatorData = $('#dataModellerFlowchartworkSourcesspace').flowchart('getOperatorData', prevTargetOperatorId);
            var isEmpty = jQuery.isEmptyObject(previousTargetOperatorData);
            if (!jQuery.isEmptyObject(previousTargetOperatorData)) {

                if (previousTargetOperatorData['trfmRules-data'] != null) {
                    t_rules = previousTargetOperatorData['trfmRules-data'];
                }

                var componentOperator = {};
                var fromOpLinkArray = $('#dataModellerFlowchartworkSourcesspace').flowchart('getAllFromOperatorsByToOpId', prevTargetOperatorId);
                if (fromOpLinkArray != null) {
                    if (fromOpLinkArray != null && fromOpLinkArray.length != 0) {
                        for (var i = 0; i < fromOpLinkArray.length; i++) {
                            var operatorData = fromOpLinkArray[i];
                            if (operatorData != null
                                    && operatorData['iconType'] != null
                                    && operatorData['iconType'] != '') {
                                componentOperator = operatorData;
                                break;
                            }

                        }
                    }

                }


                if (componentOperator['iconType'] == "MAP") {
                    var destinationColumnEmpty = false;
                    var colMappingsData = []

                    var rowCount = $('#sourceDestColsTableId tbody tr').length;
                    var columnCount = $('#sourceDestColsTableId th').length;
                    for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                        var rowData = {};
                        var tr = $('#sourceDestColsTableId tbody tr')[rowIndex];
                        var destinationColumnStyle = $(tr.children[2]).attr("style");

                        var destinationColumnVal = $(tr.children[2]).find('input').val();

                        if (destinationColumnStyle == "display:none;" || destinationColumnStyle == "display:none") {
                            destinationColumnVal = "N/A:N/A";
                        }
                        if (destinationColumnVal == null || destinationColumnVal == "") {
                            destinationColumnEmpty = true;
                        }


                        if (destinationColumnEmpty) {
                            showMesg("Destination Column Cannot be empty");
                            throw new Error();
                        }

                        //        if (destinationColumnVal != null &&
                        //                destinationColumnVal != "" &&
                        //                destinationColumnVal != "undefined"
                        //                && destinationColumnVal.indexOf(":")==-1){
                        //            destinationColumnVal = "N/A:"+destinationColumnVal; // ravi process job issues 
                        //        }

                        var dataFunobjStr = $(tr.children[5]).find('input').attr("data-funobjstr");
                        //funobjstr
                        var funObj = {};
                        if (dataFunobjStr != null && dataFunobjStr != '' && dataFunobjStr != 'null') {
                            funObj['funobjstr'] = dataFunobjStr;
                        }
                        rowData['primaryKey'] = $(tr.children[1]).find('input').prop('checked') ? "Y" : "N";
                        rowData['destinationColumn'] = destinationColumnVal;
                        rowData['sourceColumn'] = $(tr.children[3]).find('input').val();
                        rowData['defaultValue'] = $(tr.children[4]).find('input').val();
                        rowData['appendValue'] = $(tr.children[5]).find('input').val();
                        rowData['columnClause'] = (dataFunobjStr != null && dataFunobjStr != '' && dataFunobjStr != 'null') ? funObj : $(tr.children[6]).find('input').val();
                        rowData['data-funobjstr'] = $(tr.children[6]).find('input').attr("data-funobjstr"); // ravi change new
                        rowData['data-columnClause'] = $(tr.children[6]).find('input').val(); // ravi change new
                        colMappingsData[rowIndex] = rowData;
                        if (rowData['primaryKey'] == 'Y') {
                            t_rules['primaryKey'] = "Y";
                        }
                    }

                    t_rules['colMappingsData'] = colMappingsData;
                    // ---------------------------------

                    var masterTable = "";
                    var childTables = [];
                    var fromTables = [];
                    var rowCount = $('#EtlMappingTable thead tr').length;
                    for (var rowIndex = 1; rowIndex < rowCount; rowIndex++) {
                        var rowData = {};
                        var tr = $('#EtlMappingTable thead tr')[rowIndex];
                        fromTables.push($(tr.children[0]).find('select').val());
                        if (rowIndex == 1) {
                            masterTable = $(tr.children[0]).find('select').val();
                        } else {
                            childTables.push($(tr.children[0]).find('select').val());
                        }
                    }

                    if (masterTable == "") {

                        var mapingObj = $('#dataModellerFlowchartworkSourcesspace').flowchart('getMapOperatorData', prevTargetOperatorId);
                        if (mapingObj != null && !jQuery.isEmptyObject(mapingObj)) {
                            var fromOpArray = mapingObj['fromOpArray'];
                        }
                        if (fromOpArray != null && fromOpArray.length != 0) {
                            masterTable = fromOpArray[0]['tableName'];
                        }
                    }

                    t_rules['masterTableName'] = masterTable;
                    t_rules['masterTableName'] = masterTable;
                    t_rules['childTables'] = childTables;
                    t_rules['fromTables'] = fromTables;
                    //------------------------------------

                    var joinClauseData = []
                    var mapIcons = $("#EtlMappingTable").find(".visionEtlJoinClauseMapIcon");
                    $.each(mapIcons, function (index) {
                        var mappedColumnData = $(this).attr('data-mappedcolumns');
                        if (mappedColumnData != null
                                && mappedColumnData != ''
                                && mappedColumnData != 'null'
                                && mappedColumnData != '{}') {
//                        if (mappedColumnData == "") {
//                            mappedColumnData = "{}";
//                        }
                            joinClauseData[index] = mappedColumnData;
                        }
                    });
                    if (joinClauseData.length > 0) {
                        t_rules['joinClauseData'] = joinClauseData;
                        var fromTablesList = $(".sourceJoinColsTd").find("select");
                        if (fromTablesList.lenght > 0) {
                            var reOrderFromTables = [];
                            $.each(fromTablesList, function (i) {
                                reOrderFromTables.push(this.value);
                            })
                            t_rules['fromTables'] = reOrderFromTables;
                        }
                    }

//------------------------------------
                    var whereClauseData = []
                    var mapIcons = $("#selectedTables").find(".visionEtlWhereClauseMapIcon");
                    $.each(mapIcons, function (index) {
                        var mappedColumnData = $(this).attr('data-whereclause');
                        if (mappedColumnData != null) {
                            if (mappedColumnData == "") {
                                mappedColumnData = "{}";
                            }
                            whereClauseData[index] = mappedColumnData;
                        }

                    });
                    if (whereClauseData.length > 0) {
                        t_rules['whereClauseData'] = whereClauseData;
                    }
//---------------------------------------------------- advanced settings  
                    var selectTabObj = {};
                    var uniqueRowsFlag = "N";
                    if ($("#distinctRowsInput").is(":checked")) {
                        uniqueRowsFlag = "Y";
                    }
                    selectTabObj['uniqueRowsFlag'] = uniqueRowsFlag;
                    var minRows = $("#rowsCountFromInput").val();
                    var maxRows = $("#rowsCountToInput").val();
                    if (!(minRows != null && minRows != '') && (maxRows != null && maxRows != '')) {
                        minRows = 0;
                    }
                    selectTabObj['minRows'] = minRows;
                    selectTabObj['maxRows'] = maxRows;
                    var operatorType = $("#operatorType").val();
                    selectTabObj['operatorType'] = operatorType;

                    var scdType = $('input[name="scdType"]:checked').val();
                    if (scdType != null) {
                        selectTabObj['scdType'] = scdType;
                    }

                    t_rules['selectTabObj'] = selectTabObj;
                    //------------------------------------

                    var orderByData = []

                    var rowCount = $('#fromTablesOrderCauseTable tbody tr').length;
                    for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                        var rowData = {};
                        var tr = $('#fromTablesOrderCauseTable tbody tr')[rowIndex];
                        rowData['columnName'] = $(tr.children[1]).find('input').val();
                        rowData['order'] = $(tr.children[2]).find('select').val();
                        orderByData[rowIndex] = rowData;
                    }
                    t_rules['orderByData'] = orderByData;
                    //------------------------------------

                    var groupByData = []

                    var rowCount = $('#fromTablesGroupCauseTable tbody tr').length;
                    for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                        var rowData = {};
                        var tr = $('#fromTablesGroupCauseTable tbody tr')[rowIndex];
                        rowData['columnName'] = $(tr.children[1]).find('input').val();
                        groupByData[rowIndex] = rowData;
                    }
                    t_rules['groupByData'] = groupByData;
                } else if (componentOperator['iconType'] == "UNGROUP") {

                    var normalizeOptionsObj = {};
                    normalizeOptionsObj['normalizeFlag'] = "normalize";
                    normalizeOptionsObj['itemSeparator'] = $("#itemSeparator").val();
                    normalizeOptionsObj['normalizeColumn'] = $("#selectNormalizeColHeader").val();
                    t_rules['normalizeOptionsObj'] = normalizeOptionsObj;
                    var colMappingsData = [];
                    var fromColsArray = $("#selectNormalizeColHeader").find("option");
                    if (fromColsArray != null && fromColsArray.length > 0) {
                        $.each(fromColsArray, function (rowIndex) {
                            if (rowIndex != 0) {
                                var rowData = {};
                                var destFileHeaders = t_rules['fileHeaders'];
                                if (destFileHeaders != null) {
                                    var destinationColumn = destFileHeaders[rowIndex];
                                }
                                rowData['sourceColumn'] = this.value;
                                rowData['destinationColumn'] = (destinationColumn != null) ? destinationColumn : this.value;
                                colMappingsData[rowIndex - 1] = rowData;
                            }

                        })

                        t_rules['colMappingsData'] = colMappingsData;
                    }
                } else if (componentOperator['iconType'] == "GROUP") {

                    var deNormalizeOptionsObj = {};
                    deNormalizeOptionsObj['normalizeFlag'] = "deNormalize";
                    deNormalizeOptionsObj['delimiter'] = $("#delimiter").val();
                    deNormalizeOptionsObj['deNormalizeColumn'] = $("#selectDenormalizeColHeader").val();
                    deNormalizeOptionsObj['keyColumn'] = $("#selectDenormalizeKeyColumn").val(); // ravi updated code

                    t_rules['normalizeOptionsObj'] = deNormalizeOptionsObj;
                    var colMappingsData = [];
                    var fromColsArray = $("#selectDenormalizeColHeader").find("option");
                    if (fromColsArray != null && fromColsArray.length > 0) {
                        $.each(fromColsArray, function (rowIndex) {
                            if (rowIndex != 0) {
                                var rowData = {};
                                var destFileHeaders = t_rules['fileHeaders'];
                                if (destFileHeaders != null) {
                                    var destinationColumn = destFileHeaders[rowIndex];
                                }
                                rowData['sourceColumn'] = this.value;
                                rowData['destinationColumn'] = (destinationColumn != null) ? destinationColumn : this.value;
                                colMappingsData[rowIndex - 1] = rowData;
                            }

                        })

                        t_rules['colMappingsData'] = colMappingsData;
                    }
                }



                previousTargetOperatorData['trfmRules-data'] = t_rules;
                previousTargetOperatorData['targetOperator'] = "Y";
                $('#dataModellerFlowchartworkSourcesspace').flowchart('setOperatorData', prevTargetOperatorId, previousTargetOperatorData);
                var previousTargetOperatorData2 = $('#dataModellerFlowchartworkSourcesspace').flowchart('getOperatorData', prevTargetOperatorId);
                $(".flowchart-operator-connector-label").hide();
                $(".flowchart-operator-title").hide();
            }
        }
        var fromOpLinkArray = $('#dataModellerFlowchartworkSourcesspace').flowchart('getLinksTo', selectedOperatorId);
        if (fromOpLinkArray.length != 0) {
            var mapOperatorId = fromOpLinkArray[0]['operatorId']; // latest changes
            if (mapOperatorId == null) {
                mapOperatorId = fromOpLinkArray[0]['fromOperator']; // latest changes
            }

            var mapOperatorData = $('#flowchartworkSourcesspace').flowchart('getOperatorData', mapOperatorId);
            if (mapOperatorData['iconType'] == "GROUP") {
                deNormalizeData(mapOperatorId);
                return false;
            } else if (mapOperatorData['iconType'] == "UNGROUP") {
                normalizeData(mapOperatorId);
                return false;
            }

            prevTargetOperatorId = selectedOperatorId;
            var fromOpLinkArrayToMap = $('#dataModellerFlowchartworkSourcesspace').flowchart('getLinksTo', mapOperatorId);
            var fromTableName = "";
            var fromConnectionObj = {};
            var fromOpArray = [];
            var toOpArray = [];
            $.each(fromOpLinkArrayToMap, function (index) {
                var fromOperator = $('#dataModellerFlowchartworkSourcesspace').flowchart('getOperatorData', this.fromOperator);
                fromOpArray.push(fromOperator);
                fromTableName += fromOperator['tableName'];
                fromConnectionObj = fromOperator["connObj"];
                if (index != fromOpLinkArrayToMap.length - 1) {
                    fromTableName += ",";
                }
            });
            var trfmRulesId = selectedOperatorData['trfmRulesId'];
            var trfmRulesData = selectedOperatorData['trfmRules-data'];
            var toTableName = selectedOperatorData['tableName'];
            var toIconType = selectedOperatorData['iconType'];
            var toConnectionObj = selectedOperatorData['connObj'];
            toOpArray.push(selectedOperatorData);
            // ravi end
            if ((fromConnectionObj != null && !jQuery.isEmptyObject(fromConnectionObj))) {
                showLoader();
                if (!(toConnectionObj != null && !jQuery.isEmptyObject(toConnectionObj))) {
                    toConnectionObj = {};
                }
                $.ajax({
                    type: 'post',
                    traditional: true,
                    dataType: 'html',
                    cache: false,
                    url: 'fetchTransformationRules',
                    async: true,
                    data: {
                        fromTable: fromTableName,
                        toTable: toTableName,
                        fromConnObj: JSON.stringify(fromConnectionObj),
                        toConnObj: JSON.stringify(toConnectionObj),
                        sourceTables: JSON.stringify(fromTableName.split(",")),
                        toIconType: toIconType,
                        createTableObj: null,
                        trfmRulesId: trfmRulesId,
                        trfmRulesData: JSON.stringify(trfmRulesData),
                        trfmRulesChanged: (trfmRulesChanged == true) ? "Y" : "N",
                        toOpArray: JSON.stringify(toOpArray),
                        fromOpArray: JSON.stringify(fromOpArray),
                    },
                    success: function (response) {
                        stopLoader();
                        if (response != null) {
                            $('.visionTablesComboBox').hide();
                            $('.visionUploadFileDiv').hide();
                            $('.visionConnectToDbDiv').hide();
                            $('#visionERPMain').hide();
                            var response = JSON.parse(response);
                            if (response != null && response.connectionFlag == 'Y') {
                                $("#feedContentArea1").html(response['tabsString']);
                                $("#dataMigrationTabs").jqxTabs({width: "100%", height: "130px", position: 'top', theme: 'ui-redmond', reorder: true});
                                $('#dataMigrationTabs').unbind('selected').on('selected', function (event) {
                                    $('#iconsdiv').attr('style', 'margin-top:4px !important');
                                    // scroll bar issue fixed by SHI
                                    var feedContentArea1Height = $("#feedContentArea1").height();
//                    $("#dataMigrationTabs").jqxTabs({height:feedContentArea1Height})
                                    var tbodyHeight = feedContentArea1Height - 90;
                                    $(".visionColMappScrollDiv1").css("max-height", tbodyHeight);
                                    $(".visionEtlJoinClauseTablesDiv").css("max-height", tbodyHeight);
//                                $(".visionEtlMappingTablesDiv").css("max-height", parseInt(feedContentArea1Height) - 130);
                                    $(".visionEtlJoinClauseTablesDivScroll").css("max-height", parseInt(feedContentArea1Height) - 105);
                                    $(".visionSqlViewQuery1").css("max-height", parseInt(feedContentArea1Height) - 67);
                                    $(".visionEtlJoinrClauseTablesDiv").css("max-height", parseInt(feedContentArea1Height) - 130);
                                    $(".visionEtlwhereClauseTablesDiv").css("max-height", parseInt(feedContentArea1Height) - 69);
                                    var joinTablesHeight = $(".visionEtlJoinrClauseTablesDiv").height();
                                    $(".viewJoinQueryOuterDivClass").css("max-height", parseInt(feedContentArea1Height) - (parseInt(joinTablesHeight) + 82));
                                    // scroll bar issue fixed by SHI
                                    //end code
                                });
//                        $("#dataMigrationTabs").jqxTabs({width: "100%", height: "100%", position: 'top', theme: 'ui-redmond', reorder: true});
                                var columnStr = response['columnMapping'];
                                var selectedJoinTables = response['selectedJoinTables'];
                                $('#tabs-1').html(columnStr);
                                $('#tabs-1').append("<div id='selectedColumnStr' style='display:none'></div>");
                                $("#toTableColsArray_hidden").remove();
                                $("#fromTableColsArray_hidden").remove();
                                var hiddenData = "<input type='hidden' id='toTableColsArray_hidden'/><input type='hidden' id='fromTableColsArray_hidden'/>";
                                $('#tabs-1').append(hiddenData);
                                $("#toTableColsArray_hidden").val(JSON.stringify(response['toTableColsArray']));
                                $("#fromTableColsArray_hidden").val(JSON.stringify(response['fromTableColsArray']));
                                $('#tabs-2').html(selectedJoinTables);
                                $('#mapColumns').addClass("active");
                                $('.visionProgressFilesSteps').hide();
                                $("#selectedColumnStr").html(response['colMapTrString']); //colMapTrString
                                var selectedTableWhereClause = response['selectedTableWhereClause'];
                                if (selectedTableWhereClause != null) {
                                    var hiddenDataWhere = "<input type='hidden' id='whereClauseTableColsArray_hidden'/><input type='hidden' id='currentClauseMapId'/>"
                                            + "<div id='wherClauseTrString' style='display:none;'></div><div id='wherClauseColsString' style='display:none;'></div>";
                                    $('#tabs-3').html(selectedTableWhereClause['whereClauseCondition'] + hiddenDataWhere); //whereClauseCondition
                                    $("#whereClauseTableColsArray_hidden").val(JSON.stringify(selectedTableWhereClause['fromTableColsArray']));
                                }
                                var selectedTableOrderGroupClause = response['selectedTableOrderGroupClause'];
                                if (selectedTableOrderGroupClause != null) {
                                    var hiddenDataOrderBy = ""
                                            + "<div id='orderClauseTrString' style='display:none;'></div>";
                                    $('#tabs-4').html(selectedTableOrderGroupClause['orderByCondition'] + hiddenDataOrderBy); //
                                    var hiddenDataGroupBy = ""
                                            + "<div id='groupClauseTrString' style='display:none;'></div>";
                                    $('#tabs-5').html(selectedTableOrderGroupClause['groupByCondition'] + hiddenDataGroupBy); //
                                    $("#groupClauseTrString").html(selectedTableOrderGroupClause['groupByTrString']);
                                    $("#orderClauseTrString").html(selectedTableOrderGroupClause['orderByTrString']);
                                }
                                // sql Editor
//                        var sqlEditor = ace.edit("tabs-6");
//                        sqlEditor.setOptions({
//                            enableBasicAutocompletion: true, // the editor completes the statement when you hit Ctrl + Space
//                            showPrintMargin: false, // hides the vertical limiting strip
//                            fontSize: "100%" // ensures that the editor fits in the environment
//                        });
//
//                        sqlEditor.getSession().setMode("ace/mode/sql");
// scroll bar issue fixed by SHI
                                var feedContentArea1Height = $("#feedContentArea1").height();
                                if ($('#dataMigrationTabs').length) {
                                    $("#dataMigrationTabs").jqxTabs({height: feedContentArea1Height});
                                }
//                            $("#dataMigrationTabs").jqxTabs({height: feedContentArea1Height})
                                var tbodyHeight = feedContentArea1Height - 90;
                                $(".visionColMappScrollDiv1").css("max-height", tbodyHeight);
                                $(".visionEtlJoinClauseTablesDiv").css("max-height", tbodyHeight);
//                            $(".visionEtlMappingTablesDiv").css("max-height", parseInt(feedContentArea1Height) - 130);
                                $(".visionEtlJoinClauseTablesDivScroll").css("max-height", parseInt(feedContentArea1Height) - 105);
                                $(".visionSqlViewQuery1").css("max-height", parseInt(feedContentArea1Height) - 67);
                                $(".visionEtlJoinrClauseTablesDiv").css("max-height", parseInt(feedContentArea1Height) - 130);
                                $(".visionEtlwhereClauseTablesDiv").css("max-height", parseInt(feedContentArea1Height) - 69);
                                var joinTablesHeight = $(".visionEtlJoinrClauseTablesDiv").height();
                                $(".viewJoinQueryOuterDivClass").css("max-height", parseInt(feedContentArea1Height) - (parseInt(joinTablesHeight) + 82));
                                // scroll bar issue fixed by SHI
                                $("#dataMigrationTabs").change(function (event) {
                                    trfmRulesChanged = true;
                                });
                                if (response['uniqueRowsFlag'] == "Y") {
                                    $("#distinctRowsInput").prop("checked", true);
                                }

                                $("#rowsCountFromInput").val(response['minRows']);
                                $("#rowsCountToInput").val(response['maxRows']);
                                if (response['operatorType'] != null && response['operatorType'] != '') {
                                    $("#operatorType").val(response['operatorType']);
                                }

//                        //importColMapFile
//                        var fileslist = [];
//                        $("#importColMapFile").on('change', function (event) {
//                            console.log("iam in files change ");
//                           fileslist  = event.target.files;
//                            srsFileNames(fileslist);
//
//                        });
                                $(".visionETLColMapImage").mousedown(function (event) {
                                    treeIconClickEvent = event;
                                });
                                try {
                                    $("#sourceDestColsTableId").colResizable({
                                        disable: true
                                    });
                                } catch (e) {
                                }
                                $("#sourceDestColsTableId").colResizable();
                                $('#dataMigrationTabs').on('selected', function (event) {
                                    var selectedTab = event.args.item;
                                    if (selectedTab == '0' || selectedTab == 0) {// mapping
                                        try {
                                            $("#sourceDestColsTableId").colResizable({
                                                disable: true
                                            });
                                        } catch (e) {
                                        }
                                        $("#sourceDestColsTableId").colResizable();
                                    } else if (selectedTab == '1' || selectedTab == 1) {// join
                                        try {
                                            $("#EtlMappingTable").colResizable({
                                                disable: true
                                            });
                                        } catch (e) {
                                        }
                                        $("#EtlMappingTable").colResizable();
                                    } else if (selectedTab == '2' || selectedTab == 2) {//where clause
                                        try {
                                            $("#selectedTables").colResizable({
                                                disable: true
                                            });
                                        } catch (e) {
                                        }
                                        $("#selectedTables").colResizable();
                                    } else if (selectedTab == '3' || selectedTab == 3) {//order clause
                                        try {
                                            $("#fromTablesOrderCauseTable").colResizable({
                                                disable: true
                                            });
                                        } catch (e) {
                                        }
                                        $("#fromTablesOrderCauseTable").colResizable();
                                    } else if (selectedTab == '4' || selectedTab == 4) {//group clause
                                        try {
                                            $("#fromTablesGroupCauseTable").colResizable({
                                                disable: true
                                            });
                                        } catch (e) {
                                        }
                                        $("#fromTablesGroupCauseTable").colResizable();
                                    }
                                });
                            } else {
                                stopLoader();
                                showMessagePopup(response.connectionMessage);
                            }


                        }
                        cubePreviousOperatorId = selectedOperatorId;
                    },
                    error: function (e)
                    {
                        stopLoader();
                        sessionTimeout(e);
                    }

                });
            } else {
                showMesg("Please map from table.");
                stopLoader();
            }
        }
    }
}

function saveAsETLJob(jobName, jobId) {

    var data = {};
    try {
        data = $('#dataModellerFlowchartworkSourcesspace').flowchart('getData');
    } catch (e) {
    }

    if (jobName == null) {
        jobName = $("#currentJobName").val();
    }
    if (jobId == null) {
        jobId = $("#currentJobId").val();
    }


    var jobDetails = {};
    jobDetails['jobType'] = "ETL";

    if (jobName == null) {

        var response = "<div id='textReason'>";
        response += "<textarea id='reasonId' class='visionDeleteReason'></textarea></div>";
        response += "<div id='dailog_error_id' style='display:none;color:red'>" + (labelObject['Please give any Job Name'] != null ? labelObject['Please give new Job Name'] : 'Please give new Job Name') + "</div>";

        $("#dialog").html(response);
        $("#dialog").dialog({
            title: (labelObject['Job Name'] != null ? labelObject['Job Name'] : 'Job Name'),
            modal: true,
            height: 'auto',
            minWidth: 300,
            maxWidth: 'auto',
            fluid: true,
            buttons: [{
                    text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                    click: function () {
                        jobName = $("#reasonId").val();
                        if (jobName != null && jobName != '') {
                            $("#dailog_error_id").hide();
                            $(this).html("");
                            $(this).dialog("close");
                            $(this).dialog("destroy");
                            saveJob(jobName);

                        } else {
                            $("#dailog_error_id").show();
                        }

                    }},
                {
                    text: (labelObject['Cancel'] != null ? labelObject['Cancel'] : 'Cancel'),
                    click: function () {
                        $(this).html("");
                        $(this).dialog("close");
                        $(this).dialog("destroy");
                        // $("#labeld").empty();

                    }

                }],
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
        return false;
    }
    console.log("Data::::" + JSON.stringify(data))

    showLoader();
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        cache: false,
        url: "saveMappings",
        data: {
            mappedData: JSON.stringify(data),
            jobName: jobName,
            jobId: jobId,
            jobDetails: JSON.stringify(jobDetails)
                    //trfmRulesArray: JSON.stringify(trfmRulesArray)
        },
        success: function (response) {
            stopLoader();
            var response = JSON.parse(response);
            showSavedJobs();
            showMesg(response['message']);


            // ravi end
        },
        error: function (e) {
            sessionTimeout(e);
        }
    });
}