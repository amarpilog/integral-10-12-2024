/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

var processStepsInterval = null;
var processBarInterval = null;
function getAllMappedData(processFlag) {
     var flowChartData = $('#' + flowChartWorkSpaceId).flowchart('getData');
    var operators = flowChartData['operators'];
    var groupJob = false;
    if (operators != null && !jQuery.isEmptyObject(operators)) {

        for (var key of Object.keys(operators)) {
            var operator = operators[key];
            if (operator['iconType'] == 'GROUP_JOB') {
                groupJob = true;
            }
            break;

        }
        if (groupJob) {
            var jobName = $("#currentGroupJobName").val();
            if (jobName == null || jobName == 'undefined' || jobName == '') {
                setGroupJobName(processFlag);
            } else if (jobName != null && jobName != '' && processFlag == 'Y') {
                processJob();
            } else if (processFlag == undefined) {
                updateGroupJob();
            }
            return false;
        }

    }

    saveTrfmRulesToOp();
    data = $('#' + flowChartWorkSpaceId).flowchart('getData');
    var contentSplitterStyle = $("#contentSplitter").css("display");
    var contentSplitter1Style = $("#contentSplitter1").css("display");
    var flowChartId;
    var containerId;
    if (contentSplitterStyle == "block") {
        flowChartId = 'flowchartworkSourcesspace';
        containerId = 'feedHeader';
    } else if (contentSplitter1Style == "block") {
        flowChartId = 'dataModellerFlowchartworkSourcesspace';
        containerId = 'feedHeader1';
    }

    var currentTrnsOpId = $("#currentTrnsOpId").val();



    if (flowChartData != null && !jQuery.isEmptyObject(flowChartData)) {
        var operators = flowChartData['operators'];
        var links = flowChartData['links'];
        if (operators != null && !jQuery.isEmptyObject(operators)
//                && links != null && !jQuery.isEmptyObject(links)
                ) {


            var jobId = $('#' + flowChartWorkSpaceId).attr("jobId");
            var jobName = $('#' + flowChartWorkSpaceId).attr("jobName");
            var folderName = $('#' + flowChartWorkSpaceId).attr("folderName");
            var folderId = $('#' + flowChartWorkSpaceId).attr("folderId");
            showLoader();
			$.ajax({
				type: 'post',
				traditional: true,
				dataType: 'json',
				cache: false,
				url: 'systemJobVerification',
				async: true,
				data: {
					folderName: folderName,
					folderId: folderId
				},
				success: function(response) {
					stopLoader();
					if (response != null && response['accessMessage'] == "accessDone") {
						if (jobId != null && jobId != "" && jobId != "null" && jobId != "undefined"
							&& jobName != null && jobName != "" && jobName != "null" && jobName != "undefined") {
							$("#currentJobId").val(jobId);
							saveJob(jobName, jobId, data, folderName, folderId, processFlag);
							return;
						}
						// ravi end

						var response = "<div id='textReason'> <table> <td><input id='selectFolderName' style='width: 250px;' class='visionDSMappingInput' type='text' value='Saved Jobs' folderId='C8DE75F32D56288CE0554B610B40A4A3'  readonly='true'><img src='images/tree_icon.svg' class='visionETLColMapImage'  onclick='selectFolder(this)' style=''> </td></table>";
						//UK folder
						response += "<textarea id='reasonId' class='visionDeleteReason'></textarea><br>\n\
                        </div>";
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
								click: function() {
									//var processJobData = {};
									var jobName = $("#reasonId").val();
									var folderName = $("#selectFolderName").val();//UK folder
									var folderId = $("#selectFolderName").attr("folderId");//UK folder
									$("#folderNameHidden").val(folderName);
									$("#folderIdHidden").val(folderId);
									if (jobName != null && jobName != '') {
										$("#dailog_error_id").hide();
										$(this).html("");
										//                                $(this).dialog("close");
										$(this).dialog("destroy");
										saveJob(jobName, jobId, data, folderName, folderId, processFlag);

									} else {
										$("#dailog_error_id").show();
									}

								}
							},
							{
								text: (labelObject['Cancel'] != null ? labelObject['Cancel'] : 'Cancel'),
								click: function() {
									$(this).html("");
									//                            $(this).dialog("close");
									$(this).dialog("destroy");
									// $("#labeld").empty();

								}

							}],
							open: function() {
								$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
								$(this).closest(".ui-dialog").find(".ui-button").eq(2).addClass("dialogno");
								$(".visionHeaderMain").css("z-index", "999");
								$(".visionFooterMain").css("z-index", "999");
							},
							beforeClose: function(event, ui) {
								$(".visionHeaderMain").css("z-index", "99999");
								$(".visionFooterMain").css("z-index", "99999");
							}
						});
					} else {
						showMesg(response['accessMessage']);
						return;
					}
				},
				error: function(e) {
					stopLoader();
					console.log(e);
					sessionTimeout(e);
				}
			});
        } else {
            $("#dialog").html(labelObject['No Mapped Objects to Save'] != null ? labelObject['No Mapped Objects to Save'] : 'No Mapped Objects to Save');
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
                            $(this).html("");
//                            $(this).dialog("close");
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

    } else {
        $("#dialog").html(labelObject['No Mapped Objects to Save'] != null ? labelObject['No Mapped Objects to Save'] : 'No Mapped Objects to Save');
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

function processJob() {
    saveTrfmRulesToOp();
    var $flowchart = $('#' + flowChartWorkSpaceId);
     var flowChartData;
	  if (  $("#"+flowChartWorkSpaceId).find(".operatorDisabled").length > 0 ) {
			flowChartData = getActiveFlowChartData();
            } else {
                     flowChartData = $flowchart.flowchart('getData');
            }

    var operators = flowChartData['operators'];
    if (operators != null && !jQuery.isEmptyObject(operators)) {
        var groupJob = false;

        for (var key of Object.keys(operators)) {
            var operatorData = operators[key];

            var body = operatorData['properties']['body'];
            var ele = document.createElement('div');
            ele.innerHTML = body;
            if ($(ele).find(".operatorProcessStatus").length > 0) {
                    $(ele).find(".operatorProcessStatus").remove();
            }
            if ($(ele).find(".visionJobReconciliation").length > 0) {
                    $(ele).find(".visionJobReconciliation").remove();
            }
            if ($(ele).find(".etlProcessComponentLoader").length > 0) {
                    $(ele).find(".etlProcessComponentLoader").remove();
            }

            operatorData['properties']['body'] = $(ele).html();
            $('#' + flowChartWorkSpaceId).flowchart('setOperatorData', operatorData['operatorId'], operatorData);

            if (operatorData['iconType'] == 'GROUP_JOB') {
              //  groupJob = true;
            }

           // break;
        }
        if (groupJob) {
            executeProcessGroupJob();
            return false;
        }
    }
    validateTransformationRules(flowChartData);
   // validateMergeComponentTrfmRules()
    var jobId = $('#' + flowChartWorkSpaceId).attr("jobid");
    openLogFile();
    processStepsInterval = setInterval(function() {
			// this will run after every 1 seconds
				refreshOperatorProcessStatus(jobId);
			}, 2000);

//    openETLProgressBar();
    showLoader();
	var formData = new FormData();

		// Extract files from flowChartData (if available) and append them to FormData
		$.each(flowChartData.operators, function(key, operator){
					if (operator.files && operator.files.length > 0) {
						// If files exist for the operator, loop through each and append to FormData
						$.each(Array.from(operator.files), function(fileIndex, file){
							formData.append('files', file);
							console.log("Appending file:", file.name, "Size:", file.size);
						});
					} else {
						console.log("No files found for operator", operator);
					}
				});
		
		// Append other flowchart and job-related data
		formData.append('currentConnObj', JSON.stringify(savedDBData['Current_V10']));
		formData.append('flowchartData', JSON.stringify(flowChartData));  // Add flowchart structure
		formData.append('jobId', $('#' + flowChartWorkSpaceId).attr("jobid"));

		//    openETLProgressBar();
		showLoader();
		$.ajax({
			type: 'post',
			url: 'processJobComponents',
			data: formData,
			cache: false,
			contentType: false,  // Required for file uploads
			processData: false,  // Prevent jQuery from processing FormData
			async: true,
			success: function(response) {
				stopLoader();
				if (response != null) {

				//var flowChartData = $('#' + flowChartWorkSpaceId).flowchart('getData');
				$.each(flowChartData.operators, function(key, operator){
					var toOperatorsArray = $('#' + flowChartWorkSpaceId).flowchart('getAllToOperatorsByFromOpId', operator.operatorId);
					if (toOperatorsArray.length ==0) {

							var subJobId = this['subJobId'];

							var body = $('#' + flowChartWorkSpaceId).flowchart('getOperatorBody', (this['operatorId']));
							var ele = document.createElement('div');
							ele.innerHTML = body;
							$(ele).find('.visionJobReconciliation').remove();

							$(ele).append("<img class='visionJobReconciliation' id='recon_"+subJobId+"' title='Column Reconciliation' src='images/etl/threedots-ver.png' onclick=showReconOptions('"+jobId+"','"+subJobId+"','recon_"+subJobId+"') />");

							//$(ele).append("<img class='visionJobReconciliation' title='Reconciliation' src='images/etl/reconciliation.png' onclick=showJobReconciliation('"+subJobId+"') />");
							//$(ele).append("<img class='visionJobReconciliation' title='Column Reconciliation' src='images/etl/reconciliation.png' onclick=showSubJobLog('"+subJobId+"') />");
							this['properties']['body'] = $(ele).html();
							$('#' + flowChartWorkSpaceId).flowchart('setOperatorData', parseInt(this['operatorId']), this);

					}
				})
            }
        },
        error: function (e)
        {

		if (processStepsInterval != null) {
				clearInterval(processStepsInterval);
		}
            sessionTimeout(e);
        }

    });
}

function showSubJobLog(jobId, subJobId){
        showLoader();
	 $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'json',
        cache: false,
        url: 'fetchSubJobProcessLog',
        async: true,
        data: {
             jobId: jobId,
             subJobId:subJobId
        },
        success: function (response) {
            stopLoader();
            if (response != null) {
				var processLogTable =  "<table id='processlogTable' class='logtable' style='width:100%'>"
					+ "<thead>"
					+ "<tr>"
					+ "<th width='5%'></th>"
					+ "<th width='25%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;'>Time Stamp</th>"
					+ "<th width='70%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;'>Message</th>"
					+ "</tr>"
					+ "</thead>"
					+ "<tbody>"
					+ "</tbody>"
					+ "</table>"


					$("#dialogLogFile").html(processLogTable);
				$("#dialogLogFile").dialog({
					title: (labelObject['Log File'] != null ? labelObject['Log File'] : 'Log File'),
					modal: true,
					width: 'auto',
					maxWidth: 500,
					height: 'auto',
					maxHeight: 500,
					//position: {
			        //        my: 'right bottom',
			        //        at: 'right bottom-40',
			        //        of: window
			        //        },
					fluid: true,
					buttons: [{
						text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
						click: function() {


							$(this).html("");
							//                    $(this).dialog("close");
							$(this).dialog("destroy");
						}

					}],
					open: function() {

						var logText = response['logTxt'];
						$("#processlogTable tbody").append(logText);

						$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
						$(".visionHeaderMain").css("z-index", "999");
						$(".visionFooterMain").css("z-index", "999");
						$(".ui-dialog").addClass('visionDMTreePopup');

					},
					beforeClose: function(event, ui) {

						$(".visionHeaderMain").css("z-index", "99999");
						$(".visionFooterMain").css("z-index", "99999");
					}, close: function(event, ui) {

						//                        $(this).html("");
						//                        $(this).dialog("close");
						//                        $(this).dialog("destroy");
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

function showReconOptions(jobId, subJobId, selectorId){
	$("#popover").remove();
	$("body").append("<div id='popover' ></div>")

    $("#popover").jqxPopover({

        offset: {left: -5, top: 0},
        arrowOffsetValue: 0,
        position: 'right',
        //title: "Components",
        showCloseButton: true,
        selector: $("#" + selectorId)
    });
    $("#popover").jqxPopover('open');
    $("#popover").find(".jqx-popover-title").hide();
    $("#popover").find(".jqx-popover-arrow").hide();
    $("#popover").css("border-radius","50px");
    $("#popover").find(".jqx-popover.right").css("margin-left","10px");

    var content = "<img class='visionJobReconciliationOptions' title='Reconciliation' src='images/etl/reconciliation.png' onclick=showJobReconciliation('"+subJobId+"') />";
    		content += "<img class='visionJobReconciliationOptions' title='Column Reconciliation' src='images/etl/reconciliation.png' onclick=showColumnReconciliation('"+subJobId+"') />"
    		content += "<img class='visionJobReconciliationOptions' title='Sub Job Log' src='images/logicon.svg' onclick=showSubJobLog('"+jobId+"','"+subJobId+"') />";

    $("#popover").find(".jqx-popover-content").html(content);
    //$("#"+selectorId).trigger("click");
     $("#popover").click(function(e) {
		 $("#popover").remove();
	});

	}

function saveTrfmRulesToOp() {
	classifySubJobs();
    var selectedOperatorId = $("#currentTrnsOpId").val();
    if (selectedOperatorId != null && selectedOperatorId != "") {
        var $flowchart = $('#' + flowChartWorkSpaceId);
        var operatorData = $flowchart.flowchart('getOperatorData', selectedOperatorId);
        var iconType = operatorData['iconType'];
        var component = operatorData['component'];
        var sourceOrTarget = operatorData['sourceOrTarget'];
//        var connectedFrom = operatorData['connectedFrom'];
        var connectedFrom = getConnectedFromOpIds(selectedOperatorId)
//        if (component == "Y" && iconType != null && iconType == 'SAPJOIN') {
//            var joinTrfmRules = (operatorData['joinTrfmRules'] != null) ? operatorData['joinTrfmRules'] : {};
//            var joinClauseCondition = $("#joinClauseCondition").val();
//            var filterClauseCondition = $("#filterClauseCondition").val();
//            joinTrfmRules['joinClauseCondition'] = joinClauseCondition;
//            joinTrfmRules['filterClauseCondition'] = filterClauseCondition;
//            operatorData['joinTrfmRules'] = joinTrfmRules;
//            $flowchart.flowchart('setOperatorData', selectedOperatorId, operatorData);
//        }
        if (component == "Y" && iconType != null && iconType == 'MERGE') {

            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
            var operatorType = $("#operatorType").val()!=null ? $("#operatorType").val() : trfmRules['operatorType'];
            trfmRules['operatorType'] = operatorType;
            var simpleColumnsList = [];
            var rowCount = $('#sourceDestColsTableId tbody tr').length;
            if (rowCount != null && rowCount > 0) {
                var colMappingsData = [];
                var columnCount = $('#sourceDestColsTableId th').length;
                trfmRules['primaryKey'] = "";
                trfmRules['updateKey'] = "";
                for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                    var rowData = {};
                    var tr = $('#sourceDestColsTableId tbody tr')[rowIndex];
                    var destinationColumnStyle = $(tr.children[2+1]).attr("style");

                    var destinationColumnVal = $(tr.children[2+1]).find('input').val();
                    if (destinationColumnVal != null && destinationColumnVal != "") {
                        if (destinationColumnVal.indexOf(":") > -1) {
                            simpleColumnsList.push(destinationColumnVal.split(":")[1]);
                        } else {
                            simpleColumnsList.push(destinationColumnVal);
                        }
                    }
                    var destinationColumnActualValue = $(tr.children[2+1]).find('input').attr("actual-value");
                    var dataFunobjStr = $(tr.children[6+1]).find('input').attr("data-funobjstr");
                    var dataFunTables = $(tr.children[6+1]).find('input').attr("tableName");
                    var funObj = {};
                    if (dataFunobjStr != null && dataFunobjStr != '' && dataFunobjStr != 'null') {
                        funObj['funobjstr'] = dataFunobjStr;
                    }
                    rowData['primaryKey'] = $(tr.children[1]).find('input').prop('checked') ? "Y" : "N";
                    rowData['updateKey'] = $(tr.children[1+1]).find('input').prop('checked') ? "Y" : "N";
                    rowData['destinationColumn'] = destinationColumnVal;
                    rowData['destinationColumnActualValue'] = destinationColumnActualValue;
                    rowData['destTable'] = $(tr.children[2+1]).find('input').attr("tableName");
                    rowData['sourceColumn'] = $(tr.children[3+1]).find('input').val();
                    rowData['sourceColumnActualValue'] = $(tr.children[3+1]).find('input').attr("actual-value");
                    rowData['sourceTable'] = $(tr.children[3+1]).find('input').attr("tableName");
                    rowData['defaultValue'] = $(tr.children[4+1]).find('input').val();
                    rowData['appendValue'] = $(tr.children[5+1]).find('input').val();
                    rowData['columnClause'] = $(tr.children[6+1]).find('input').val();
                    rowData['columnClauseActualValue'] = $(tr.children[6+1]).find('input').attr("actual-value");
                    rowData['funcolumnslist'] = $(tr.children[6+1]).find('input').attr("funcolumnslist");
                    rowData['dataFunTables'] = $(tr.children[6+1]).find('input').attr("tableName");
                    rowData['data-funobjstr'] = $(tr.children[6+1]).find('input').attr("data-funobjstr"); // ravi change new
                    rowData['data-columnClause'] = $(tr.children[6+1]).find('input').val(); // ravi change new
                    colMappingsData[rowIndex] = rowData;
                    if (rowData['primaryKey'] == 'Y') {
                        trfmRules['primaryKey'] = "Y";
                    }
                    if (rowData['updateKey'] == 'Y') {
                        trfmRules['updateKey'] = "Y";
                    }

                }



               trfmRules['colMappingsData'] = colMappingsData;
               var skipRecordsCheckBoxVal = $("#skipRejectedRecords").prop('checked');
                var skipRejectedRecords = (skipRecordsCheckBoxVal==true) ? "Y": "N";
                 trfmRules['skipRejectedRecords'] = skipRejectedRecords;

            }
            operatorData['simpleColumnsList'] = simpleColumnsList;
            operatorData['trfmRules'] = trfmRules;
            $flowchart.flowchart('setOperatorData', selectedOperatorId, operatorData);

        }


		  if (component == "Y" && iconType != null && iconType == 'API') {

		    var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};

           		if ( $("#apiUrl").length >0 ) {
					var apiUrl = $("#apiUrl").val();
			     	 var relativePath = $("#relativePath").val();
			     	 var httpMethod = $("#httpMethod").val();
			     	 var acceptType = $("#acceptType").val();
			     	 var parametersObj = {};

			     	 $("#apiParametersTable").find("tr").each(function(i){
						if (i > 0) {
							var key = $($(this).find("td")[0]).find("input").val();
							var value = $($(this).find("td")[1]).find("input").val();
							parametersObj[key] = value;
						}
					 })


			     	 trfmRules['apiUrl'] = apiUrl;
			     	 trfmRules['relativePath'] = relativePath;
			     	 trfmRules['httpMethod'] = httpMethod;
			     	 trfmRules['acceptType'] = acceptType;
			     	 trfmRules['parametersObj'] = parametersObj;
			     	 var simpleColumnsList = getAPIColumns(operatorData);
			     	 operatorData['simpleColumnsList'] = simpleColumnsList;
				}

	            operatorData['trfmRules'] = trfmRules;
	            $flowchart.flowchart('setOperatorData', selectedOperatorId, operatorData);

			}

        if (component == "Y" && iconType != null && iconType == 'SCD') {
            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
            var operatorType = $("#operatorType").val();
            var selectedSCDType = $("#selectSCDTypesRadioButton").find("input:radio:checked").val();

            if (selectedSCDType != null) {
                trfmRules['selectedSCDType'] = selectedSCDType;
                if (selectedSCDType == "SCD1") {
                    trfmRules['operatorType'] = "Insert Or Update";
                } else if (selectedSCDType == "SCD2") {

                } else if (selectedSCDType == "SCD3") {
                    var historyCols = [];
                    $("#scd3HistoryColsTable > tbody input:checked").each(function (i) {
                        var historyColVal = $(this).val();
                        historyCols.push(historyColVal);
                    })
                    trfmRules['historyCols'] = historyCols;
                } else if (selectedSCDType == "SCD6") {
                    var historyCols = [];
                    $("#scd6HistoryColsTable > tbody input:checked").each(function (i) {
                        var historyColVal = $(this).val();
                        historyCols.push(historyColVal);
                    })
                    trfmRules['historyCols'] = historyCols;
                }
            }


            operatorData['trfmRules'] = trfmRules;
            $flowchart.flowchart('setOperatorData', selectedOperatorId, operatorData);
        }
                if (component == "Y" && iconType != null && iconType == 'PIVOT') {
            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
            var createTableName = $("#createTableName").val();
            var pivotColumnLabel = $("#pivotColumnLabel").val();
            var pivotAggFun = $("#pivotAggFun").val();
            var pivotColumnValue = $("#pivotColumnValue").val();
            var pivotRowLabel = $("#pivotRowLabel").val();
            var fromTableColumns = $("#fromTableColumns").val();


            if (trfmRules != null && !jQuery.isEmptyObject(trfmRules)) {
                trfmRules['createTableName'] = createTableName != null ? createTableName : trfmRules['createTableName'];
                trfmRules['pivotColumnLabel'] = pivotColumnLabel != null ? pivotColumnLabel : trfmRules['pivotColumnLabel'];
                trfmRules['pivotAggFun'] = pivotAggFun != null ? pivotAggFun : trfmRules['pivotAggFun'];
                trfmRules['pivotColumnValue'] = pivotColumnValue != null ? pivotColumnValue : trfmRules['pivotColumnValue'];
                trfmRules['pivotRowLabel'] = pivotRowLabel != null ? pivotRowLabel : trfmRules['pivotRowLabel'];
                trfmRules['fromTableColumns'] = fromTableColumns != null ? fromTableColumns : trfmRules['fromTableColumns'];
//                trfmRules['pivotFilterLabel'] = pivotFilterLabel!=null ? pivotFilterLabel : trfmRules['pivotFilterLabel'];
            } else {

                trfmRules['createTableName'] = createTableName != null ? createTableName : "";
                trfmRules['pivotColumnLabel'] = pivotColumnLabel != null ? pivotColumnLabel : "";
                trfmRules['pivotAggFun'] = pivotAggFun != null ? pivotAggFun : "";
                trfmRules['pivotColumnValue'] = pivotColumnValue != null ? pivotColumnValue : "";
                trfmRules['pivotRowLabel'] = pivotRowLabel != null ? pivotRowLabel : "";
                trfmRules['fromTableColumns'] = fromTableColumns != null ? fromTableColumns : "";
//              trfmRules['pivotFilterLabel'] = pivotFilterLabel!=null ? pivotFilterLabel : trfmRules['pivotFilterLabel'];
            }
            operatorData['trfmRules'] = trfmRules;
            $flowchart.flowchart('setOperatorData', selectedOperatorId, operatorData);
        }
        if (component == "Y" && iconType != null && iconType == 'UNPIVOT') {
            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};

            var createTableName = $("#createTableName").val();
            var unpivotColumnsLabel = $("#unpivotColumnsLabel").val();
            var unpivotValuesLabel = $("#unpivotValuesLabel").val();
            var fromTableColumns = $("#fromTableColumns").val();
            var unpivotColumns = [];
            if ($("#unpivotColumnsComboBox").length > 0) {
                var unpivotColumnsObj = $("#unpivotColumnsComboBox").jqxComboBox('getSelectedItems');
                $.each(unpivotColumnsObj, function(i){
                    var columnval = this['value']
                    unpivotColumns.push(columnval);
                })
            }

            if (trfmRules != null && !jQuery.isEmptyObject(trfmRules)) {
                trfmRules['createTableName'] = createTableName != null ? createTableName : trfmRules['createTableName'];
                trfmRules['unpivotColumnsLabel'] = unpivotColumnsLabel != null ? unpivotColumnsLabel : trfmRules['unpivotColumnsLabel'];
                trfmRules['unpivotValuesLabel'] = unpivotValuesLabel != null ? unpivotValuesLabel : trfmRules['unpivotValuesLabel'];
                trfmRules['unpivotColumns'] = (unpivotColumns != null && unpivotColumns.length>0)  ? unpivotColumns : trfmRules['unpivotColumns'];
                trfmRules['fromTableColumns'] = fromTableColumns != null ? fromTableColumns : trfmRules['fromTableColumns'];
            } else {
                trfmRules['createTableName'] = createTableName != null ? createTableName : "";
                trfmRules['unpivotColumnsLabel'] = unpivotColumnsLabel != null ? unpivotColumnsLabel : "";
                trfmRules['unpivotValuesLabel'] = unpivotValuesLabel != null ? unpivotValuesLabel : "";
                trfmRules['unpivotColumns'] = (unpivotColumns != null && unpivotColumns.length>0) ? unpivotColumns : [];
                trfmRules['fromTableColumns'] = fromTableColumns != null ? fromTableColumns : "";

            }
            operatorData['trfmRules'] = trfmRules;
            $flowchart.flowchart('setOperatorData', selectedOperatorId, operatorData);
        }
        if (component == "Y" && iconType != null && iconType == 'UNIQUE') {
            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
            var uniqueKeyColsArray = [];
            var columnsList = [];
            var dataTypeList = [];
            var columnsListArray = $(".visionUniqueKeyColSelectBox");
            var uniqueKeyColsChbxArray = $(".visionUniqueKeyColSelectBox:checkbox:checked");
            $.each(uniqueKeyColsChbxArray, function (i) {
                var value = $(this).val();
                uniqueKeyColsArray.push(value);
            })
            $.each(columnsListArray, function (i) {
                var value = $(this).val();
                var dataType = $(this).attr("dataType");
                columnsList.push(value);
                dataTypeList.push(dataType);
            })
            if (columnsListArray.length >0){
				trfmRules['uniqueKeys'] = uniqueKeyColsArray;
	            trfmRules['columnsList'] = columnsList;
	            trfmRules['dataTypeList'] = dataTypeList;
	            operatorData['trfmRules'] = trfmRules;
	            $flowchart.flowchart('setOperatorData', selectedOperatorId, operatorData);
			}

        }
        if (component == "Y" && iconType != null && iconType == 'FILTER') {
            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
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
                trfmRules['whereClauseData'] = whereClauseData;
            }
            operatorData['trfmRules'] = trfmRules;
            $flowchart.flowchart('setOperatorData', selectedOperatorId, operatorData);
        }
     if (component == "Y" && iconType != null && iconType == 'SAPJOIN') {
//            validateDuplicateDestCols();
            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};

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
                var mapingObj = $flowchart.flowchart('getMapOperatorData', prevTargetOperatorId);
                if (mapingObj != null && !jQuery.isEmptyObject(mapingObj)) {
                    var fromOpArray = mapingObj['fromOpArray'];
                }
                if (fromOpArray != null && fromOpArray.length != 0) {
                    masterTable = fromOpArray[0]['tableName'];
                }
            }

            trfmRules['masterTableName'] = masterTable;
            trfmRules['childTables'] = childTables;
            trfmRules['fromTables'] = fromTables;
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
                trfmRules['joinClauseData'] = joinClauseData;
                var fromTablesList = $(".sourceJoinColsTd").find("select");
                if (fromTablesList.length > 0) {
                    var reOrderFromTables = [];
                    $.each(fromTablesList, function (i) {
                        reOrderFromTables.push(this.value);
                    })
                    trfmRules['fromTables'] = reOrderFromTables;
                }
            }
            operatorData['trfmRules'] = trfmRules;

            $flowchart.flowchart('setOperatorData', selectedOperatorId, operatorData);
        }
        if (component == "Y" && iconType != null && iconType == 'JOIN') {
            validateDuplicateDestCols();
            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
            var simpleColumnsList = [];
            var rowCount = $('#sourceDestColsTableId tbody tr').length;
            if (rowCount != null && rowCount > 0) {
                var colMappingsData = [];
                var columnCount = $('#sourceDestColsTableId th').length;
                trfmRules['primaryKey'] = "";
                for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                    var rowData = {};
                    var tr = $('#sourceDestColsTableId tbody tr')[rowIndex];
                    var destinationColumnStyle = $(tr.children[2]).attr("style");


                    var destinationColumnVal = $(tr.children[2]).find('input').val();
                    if (destinationColumnVal != null && destinationColumnVal != "") {
                        if (destinationColumnVal.indexOf(":") > -1) {
                            simpleColumnsList.push(destinationColumnVal.split(":")[1]);
                        } else {
                            simpleColumnsList.push(destinationColumnVal);
                        }
                    }
                    var destinationColumnActualValue = $(tr.children[2]).find('input').attr("actual-value");
                    var dataFunobjStr = $(tr.children[6]).find('input').attr("data-funobjstr");
                    var dataFunTables = $(tr.children[6]).find('input').attr("tableName");
                    var funObj = {};
                    if (dataFunobjStr != null && dataFunobjStr != '' && dataFunobjStr != 'null') {
                        funObj['funobjstr'] = dataFunobjStr;
                    }
                    rowData['primaryKey'] = $(tr.children[1]).find('input').prop('checked') ? "Y" : "N";
                    rowData['destinationColumn'] = destinationColumnVal;
                    rowData['destinationColumnActualValue'] = destinationColumnActualValue;
                    rowData['destTable'] = $(tr.children[2]).find('input').attr("tableName");
                    rowData['sourceColumn'] = $(tr.children[3]).find('input').val();
                    rowData['sourceColumnActualValue'] = $(tr.children[3]).find('input').attr("actual-value");
                    rowData['sourceTable'] = $(tr.children[3]).find('input').attr("tableName");
                    rowData['defaultValue'] = $(tr.children[4]).find('input').val();
                    rowData['appendValue'] = $(tr.children[5]).find('input').val();
                    rowData['columnClause'] = $(tr.children[6]).find('input').val();
                    rowData['columnClauseActualValue'] = $(tr.children[6]).find('input').attr("actual-value");
                    rowData['funcolumnslist'] = $(tr.children[6]).find('input').attr("funcolumnslist");
                    rowData['dataFunTables'] = $(tr.children[6]).find('input').attr("tableName");
                    rowData['data-funobjstr'] = $(tr.children[6]).find('input').attr("data-funobjstr"); // ravi change new
                    rowData['data-columnClause'] = $(tr.children[6]).find('input').val(); // ravi change new
                    colMappingsData[rowIndex] = rowData;
                    if (rowData['primaryKey'] == 'Y') {
                        trfmRules['primaryKey'] = "Y";
                    }
                }

                trfmRules['colMappingsData'] = colMappingsData;
                operatorData['simpleColumnsList'] = simpleColumnsList;
            }



            var rowCount = $('#EtlMappingTable thead tr').length;
			if (rowCount != null && rowCount > 0) {
				var masterTable = "";
				var childTables = [];
				var fromTables = [];
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
					var mapingObj = $flowchart.flowchart('getMapOperatorData', prevTargetOperatorId);
					if (mapingObj != null && !jQuery.isEmptyObject(mapingObj)) {
						var fromOpArray = mapingObj['fromOpArray'];
					}
					if (fromOpArray != null && fromOpArray.length != 0) {
						masterTable = fromOpArray[0]['tableName'];
					}
				}

				trfmRules['masterTableName'] = masterTable;
				trfmRules['childTables'] = childTables;
				trfmRules['fromTables'] = fromTables;

				//------------------------------------

				var joinClauseData = []
				var mapIcons = $("#EtlMappingTable").find(".visionEtlJoinClauseMapIcon");

				$.each(mapIcons, function(index) {
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
					trfmRules['joinClauseData'] = joinClauseData;
					var fromTablesList = $(".sourceJoinColsTd").find("select");
					if (fromTablesList.length > 0) {
						var reOrderFromTables = [];
						$.each(fromTablesList, function(i) {
							reOrderFromTables.push(this.value);
						})
						trfmRules['fromTables'] = reOrderFromTables;
					}
				}
			}
            operatorData['trfmRules'] = trfmRules;
            $flowchart.flowchart('setOperatorData', selectedOperatorId, operatorData);
        }

        if (component == "Y" && iconType != null && iconType == 'SORT') {

            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
            var orderByData = []
            var rowCount = $('#fromTablesOrderCauseTable tbody tr').length;
            for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                var rowData = {};
                var tr = $('#fromTablesOrderCauseTable tbody tr')[rowIndex];
                rowData['columnName'] = $(tr.children[1]).find('input').val();
                rowData['columnNameActualValue'] = $(tr.children[1]).find('input').attr("actual-value");
                rowData['order'] = $(tr.children[2]).find('select').val();
                orderByData[rowIndex] = rowData;
           	trfmRules['orderByData'] = orderByData;
            operatorData['trfmRules'] = trfmRules;
            $flowchart.flowchart('setOperatorData', selectedOperatorId, operatorData);
        }
            }

        if (component == "Y" && iconType != null && iconType == 'UNGROUP') {
            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
            var normalizeOptionsObj = (trfmRules['normalizeOptionsObj'] != null) ? trfmRules['normalizeOptionsObj'] : {};
            normalizeOptionsObj['normalizeColumn'] = $("#selectNormalizeColHeader").val();
            normalizeOptionsObj['itemSeparator'] = $("#itemSeparator").val();
            trfmRules['normalizeOptionsObj'] = normalizeOptionsObj;
            operatorData['trfmRules'] = trfmRules;
            $flowchart.flowchart('setOperatorData', selectedOperatorId, operatorData);
        }
        if (component == "Y" && iconType != null && iconType == 'GROUP') {
            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
            var normalizeOptionsObj = (trfmRules['normalizeOptionsObj'] != null) ? trfmRules['normalizeOptionsObj'] : {};
            normalizeOptionsObj['denormalizeColumn'] = $("#selectDenormalizeColHeader").val();
            normalizeOptionsObj['keyColumn'] = $("#selectDenormalizeKeyColumn").val();
            normalizeOptionsObj['delimiter'] = $("#delimiter").val();
            trfmRules['normalizeOptionsObj'] = normalizeOptionsObj;
            operatorData['trfmRules'] = trfmRules;
            $flowchart.flowchart('setOperatorData', selectedOperatorId, operatorData);
        }
        if (component == "Y" && iconType != null && iconType == 'GROUPBY') {

            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
            var simpleColumnsList = [];
            var rowCount = $('#sourceDestColsTableId tbody tr').length;
            if (rowCount != null && rowCount > 0) {
                var colMappingsData = [];
                var columnCount = $('#sourceDestColsTableId th').length;
                trfmRules['primaryKey'] = "";
                for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                    var rowData = {};
                    var tr = $('#sourceDestColsTableId tbody tr')[rowIndex];
                    var destinationColumnStyle = $(tr.children[2]).attr("style");

                    var destinationColumnVal = $(tr.children[2]).find('input').val();
                    if (destinationColumnVal != null && destinationColumnVal != "") {
                        if (destinationColumnVal.indexOf(":") > -1) {
                            simpleColumnsList.push(destinationColumnVal.split(":")[1]);
                        } else {
                            simpleColumnsList.push(destinationColumnVal);
                        }
                    }
                    var destinationColumnActualValue = $(tr.children[2]).find('input').attr("actual-value");
                    var dataFunobjStr = $(tr.children[6]).find('input').attr("data-funobjstr");
                    var dataFunTables = $(tr.children[6]).find('input').attr("tableName");
                    var funObj = {};
                    if (dataFunobjStr != null && dataFunobjStr != '' && dataFunobjStr != 'null') {
                        funObj['funobjstr'] = dataFunobjStr;
                    }
                    rowData['primaryKey'] = $(tr.children[1]).find('input').prop('checked') ? "Y" : "N";
                    rowData['destinationColumn'] = destinationColumnVal;
                    rowData['destinationColumnActualValue'] = destinationColumnActualValue;
                    rowData['destTable'] = $(tr.children[2]).find('input').attr("tableName");
                    rowData['sourceColumn'] = $(tr.children[3]).find('input').val();
                    rowData['sourceColumnActualValue'] = $(tr.children[3]).find('input').attr("actual-value");
                    rowData['sourceTable'] = $(tr.children[3]).find('input').attr("tableName");
                    rowData['defaultValue'] = $(tr.children[4]).find('input').val();
                    rowData['appendValue'] = $(tr.children[5]).find('input').val();
                    rowData['columnClause'] = $(tr.children[6]).find('input').val();
                    rowData['columnClauseActualValue'] = $(tr.children[6]).find('input').attr("actual-value");
                    rowData['funcolumnslist'] = $(tr.children[6]).find('input').attr("funcolumnslist");
                    rowData['dataFunTables'] = $(tr.children[6]).find('input').attr("tableName");
                    rowData['data-funobjstr'] = $(tr.children[6]).find('input').attr("data-funobjstr"); // ravi change new
                    rowData['data-columnClause'] = $(tr.children[6]).find('input').val(); // ravi change new
                    colMappingsData[rowIndex] = rowData;
                    if (rowData['primaryKey'] == 'Y') {
                        trfmRules['primaryKey'] = "Y";
                    }
                }

                trfmRules['colMappingsData'] = colMappingsData;
            }



            var rowCount = $('#fromTablesGroupCauseTable tbody tr').length;
            if (rowCount!=null && rowCount>0){
                 var groupByData = []
                for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                    var rowData = {};
                    var tr = $('#fromTablesGroupCauseTable tbody tr')[rowIndex];
                    rowData['columnName'] = $(tr.children[1]).find('input').val();
                    rowData['columnNameActualValue'] = $(tr.children[1]).find('input').attr("actual-value");
                    groupByData[rowIndex] = rowData;
                }
                trfmRules['groupByData'] = groupByData;
            }


            operatorData['simpleColumnsList'] = simpleColumnsList;
            operatorData['trfmRules'] = trfmRules;
            $flowchart.flowchart('setOperatorData', selectedOperatorId, operatorData);
        }
        if (component == "Y" && iconType != null && iconType == 'QUERY') {

            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
//            var queryData = (trfmRules['queryData']!=null)?trfmRules['queryData']:[];

            var queryString = ($("#queryTextArea").val()!=null &&  $("#queryTextArea").val()!="") ? $("#queryTextArea").val()  : trfmRules['queryData'];
            trfmRules['queryData'] = queryString;


            operatorData['trfmRules'] = trfmRules;
            $flowchart.flowchart('setOperatorData', selectedOperatorId, operatorData);
        } else if (component == "Y" && iconType == "OUTPUT") {

            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
            var simpleColumnsList = [];
            var rowCount = $('#sourceDestColsTableId tbody tr').length;
            if (rowCount != null && rowCount > 0) {
                var colMappingsData = [];
                var columnCount = $('#sourceDestColsTableId th').length;
                trfmRules['primaryKey'] = "";
                for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                    var rowData = {};
                    var tr = $('#sourceDestColsTableId tbody tr')[rowIndex];
                    var destinationColumnStyle = $(tr.children[2]).attr("style");

                    var destinationColumnVal = $(tr.children[2]).find('input').val();
                    if (destinationColumnVal != null && destinationColumnVal != "") {
                        if (destinationColumnVal.indexOf(":") > -1) {
                            simpleColumnsList.push(destinationColumnVal.split(":")[1]);
                        } else {
                            simpleColumnsList.push(destinationColumnVal);
                        }
                    }
                    var destinationColumnActualValue = $(tr.children[2]).find('input').attr("actual-value");
                    var dataFunobjStr = $(tr.children[6]).find('input').attr("data-funobjstr");
                    var dataFunTables = $(tr.children[6]).find('input').attr("tableName");
                    var funObj = {};
                    if (dataFunobjStr != null && dataFunobjStr != '' && dataFunobjStr != 'null') {
                        funObj['funobjstr'] = dataFunobjStr;
                    }
                    rowData['primaryKey'] = $(tr.children[1]).find('input').prop('checked') ? "Y" : "N";
                    rowData['destinationColumn'] = destinationColumnVal;
                    rowData['destinationColumnActualValue'] = destinationColumnActualValue;
                    rowData['destTable'] = $(tr.children[2]).find('input').attr("tableName");
                    rowData['sourceColumn'] = $(tr.children[3]).find('input').val();
                    rowData['sourceColumnActualValue'] = $(tr.children[3]).find('input').attr("actual-value");
                    rowData['sourceTable'] = $(tr.children[3]).find('input').attr("tableName");
                    rowData['defaultValue'] = $(tr.children[4]).find('input').val();
                    rowData['appendValue'] = $(tr.children[5]).find('input').val();
                    rowData['columnClause'] = $(tr.children[6]).find('input').val();
                    rowData['columnClauseActualValue'] = $(tr.children[6]).find('input').attr("actual-value");
                    rowData['funcolumnslist'] = $(tr.children[6]).find('input').attr("funcolumnslist");
                    rowData['dataFunTables'] = $(tr.children[6]).find('input').attr("tableName");
                    rowData['data-funobjstr'] = $(tr.children[6]).find('input').attr("data-funobjstr"); // ravi change new
                    rowData['data-columnClause'] = $(tr.children[6]).find('input').val(); // ravi change new
                    colMappingsData[rowIndex] = rowData;
                    if (rowData['primaryKey'] == 'Y') {
                        trfmRules['primaryKey'] = "Y";
                    }
                }

                trfmRules['colMappingsData'] = colMappingsData;
            }
            operatorData['simpleColumnsList'] = simpleColumnsList;
            operatorData['trfmRules'] = trfmRules;
            $flowchart.flowchart('setOperatorData', selectedOperatorId, operatorData);
        } else if (component == "Y" && iconType == "STAGING") {

            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
            var simpleColumnsList = [];
            var rowCount = $('#sourceDestColsTableId tbody tr').length;
            if (rowCount != null && rowCount > 0) {
                var colMappingsData = [];
                var columnCount = $('#sourceDestColsTableId th').length;
                trfmRules['primaryKey'] = "";
                for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                    var rowData = {};
                    var tr = $('#sourceDestColsTableId tbody tr')[rowIndex];
                    var destinationColumnStyle = $(tr.children[2]).attr("style");

                    var destinationColumnVal = $(tr.children[2]).find('input').val();
                    if (destinationColumnVal != null && destinationColumnVal != "") {
                        if (destinationColumnVal.indexOf(":") > -1) {
                            simpleColumnsList.push(destinationColumnVal.split(":")[1]);
                        } else {
                            simpleColumnsList.push(destinationColumnVal);
                        }
                    }
                    var destinationColumnActualValue = $(tr.children[2]).find('input').attr("actual-value");
                    var dataFunobjStr = $(tr.children[6]).find('input').attr("data-funobjstr");
                    var dataFunTables = $(tr.children[6]).find('input').attr("tableName");
                    var funObj = {};
                    if (dataFunobjStr != null && dataFunobjStr != '' && dataFunobjStr != 'null') {
                        funObj['funobjstr'] = dataFunobjStr;
                    }
                    rowData['primaryKey'] = $(tr.children[1]).find('input').prop('checked') ? "Y" : "N";
                    rowData['destinationColumn'] = destinationColumnVal;
                    rowData['destinationColumnActualValue'] = destinationColumnActualValue;
                    rowData['destTable'] = $(tr.children[2]).find('input').attr("tableName");
                    rowData['sourceColumn'] = $(tr.children[3]).find('input').val();
                    rowData['sourceColumnActualValue'] = $(tr.children[3]).find('input').attr("actual-value");
                    rowData['sourceTable'] = $(tr.children[3]).find('input').attr("tableName");
                    rowData['defaultValue'] = $(tr.children[4]).find('input').val();
                    rowData['appendValue'] = $(tr.children[5]).find('input').val();
                    rowData['columnClause'] = $(tr.children[6]).find('input').val();
                    rowData['columnClauseActualValue'] = $(tr.children[6]).find('input').attr("actual-value");
                    rowData['funcolumnslist'] = $(tr.children[6]).find('input').attr("funcolumnslist");
                    rowData['dataFunTables'] = $(tr.children[6]).find('input').attr("tableName");
                    rowData['data-funobjstr'] = $(tr.children[6]).find('input').attr("data-funobjstr"); // ravi change new
                    rowData['data-columnClause'] = $(tr.children[6]).find('input').val(); // ravi change new
                    colMappingsData[rowIndex] = rowData;
                    if (rowData['primaryKey'] == 'Y') {
                        trfmRules['primaryKey'] = "Y";
                    }
                }

                trfmRules['colMappingsData'] = colMappingsData;
			}
			operatorData['simpleColumnsList'] = simpleColumnsList;
			operatorData['trfmRules'] = trfmRules;
			$flowchart.flowchart('setOperatorData', selectedOperatorId, operatorData);
		} else if (component == "Y" && iconType != null && iconType == 'VALIDATE') {

            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
            var simpleColumnsList = [];
            var rowCount = $('#sourceDestColsTableId tbody tr').length;
            if (rowCount != null && rowCount > 0) {
                var colMappingsData = [];
                var columnCount = $('#sourceDestColsTableId th').length;
                for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                    var rowData = {};
                    var tr = $('#sourceDestColsTableId tbody tr')[rowIndex];


                    rowData['sourceColumn'] = $(tr.children[1]).find('input').val();
                    rowData['sourceColumnActualValue'] = $(tr.children[1]).find('input').attr("actual-value");
                    rowData['sourceTable'] = $(tr.children[1]).find('input').attr("tableName");
                    rowData['destinationColumn'] = $(tr.children[2]).find('input').val();
                    rowData['destinationColumnActualValue'] = $(tr.children[2]).find('input').attr("actual-value");
                    rowData['destinationConnObj'] = $(tr.children[2]).find('input').attr("connObj");
                    rowData['validationType'] = $(tr.children[2+1]).find("option:selected").val();
                    rowData['parameter'] = $(tr.children[3+1]).find('input').val();
                    rowData['tableColumnValidate'] = $(tr.children[4+1]).find('input').val();
                    rowData['tableColumnValidateActualValue'] = $(tr.children[4+1]).find('input').attr("actual-value");
                    colMappingsData[rowIndex] = rowData;

                }



               trfmRules['colMappingsData'] = colMappingsData;

            }
            operatorData['trfmRules'] = trfmRules;
            $flowchart.flowchart('setOperatorData', selectedOperatorId, operatorData);
		} else if (component == "Y" && iconType != null && iconType == 'FETCH') {

			var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
			var simpleColumnsList = [];
			var rowCount = $('#sourceDestColsTableId tbody tr').length;
			if (rowCount != null && rowCount > 0) {
				var colMappingsData = [];
				for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
					var rowData = {};
					var tr = $('#sourceDestColsTableId tbody tr')[rowIndex];

					var sourceColumns = [];
					if ($("#fetchColumnsComboBox").length > 0) {
						var unpivotColumnsObj = $("#fetchColumnsComboBox").jqxComboBox('getSelectedItems');
						$.each(unpivotColumnsObj, function(i) {
							var columnval = this['value']
							sourceColumns.push(columnval);
						});
						rowData['sourceColumns'] = sourceColumns;

						rowData['compareColumn'] = $(tr.children[2]).find('input').val();
						rowData['compareColumnActualValue'] = $(tr.children[2]).find('input').attr("actual-value");
						rowData['tableColumnValidate'] = $(tr.children[3]).find('input').val();
						rowData['tableColumnValidateActualValue'] = $(tr.children[3]).find('input').attr("actual-value");
						colMappingsData[rowIndex] = rowData;

					}

					colMappingsData[rowIndex] = rowData;
				}



				trfmRules['colMappingsData'] = colMappingsData;

			}
			operatorData['trfmRules'] = trfmRules;
			$flowchart.flowchart('setOperatorData', selectedOperatorId, operatorData);
		} else if (component != "Y" && connectedFrom != null && connectedFrom.length > 0) {

			var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
			var simpleColumnsList = [];
            var rowCount = $('#sourceDestColsTableId tbody tr').length;
            if (rowCount != null && rowCount > 0) {
                var colMappingsData = [];
                var columnCount = $('#sourceDestColsTableId th').length;
                trfmRules['primaryKey'] = "";
                for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                    var rowData = {};
                    var tr = $('#sourceDestColsTableId tbody tr')[rowIndex];
                    var destinationColumnStyle = $(tr.children[2]).attr("style");

                    var destinationColumnVal = $(tr.children[2]).find('input').val();
                    if (destinationColumnVal != null && destinationColumnVal != "") {
                        if (destinationColumnVal.indexOf(":") > -1) {
                            simpleColumnsList.push(destinationColumnVal.split(":")[1]);
                        } else {
                            simpleColumnsList.push(destinationColumnVal);
                        }
                    }
                    var destinationColumnActualValue = $(tr.children[2]).find('input').attr("actual-value");
                    var dataFunobjStr = $(tr.children[6]).find('input').attr("data-funobjstr");
                    var dataFunTables = $(tr.children[6]).find('input').attr("tableName");
                    var funObj = {};
                    if (dataFunobjStr != null && dataFunobjStr != '' && dataFunobjStr != 'null') {
                        funObj['funobjstr'] = dataFunobjStr;
                    }
                    rowData['primaryKey'] = $(tr.children[1]).find('input').prop('checked') ? "Y" : "N";
                    rowData['destinationColumn'] = destinationColumnVal;
                    rowData['destinationColumnActualValue'] = destinationColumnActualValue;
                    rowData['destTable'] = $(tr.children[2]).find('input').attr("tableName");
                    rowData['sourceColumn'] = $(tr.children[3]).find('input').val();
                    rowData['sourceColumnActualValue'] = $(tr.children[3]).find('input').attr("actual-value");
                    rowData['sourceTable'] = $(tr.children[3]).find('input').attr("tableName");
                    rowData['defaultValue'] = $(tr.children[4]).find('input').val();
                    rowData['appendValue'] = $(tr.children[5]).find('input').val();
                    rowData['columnClause'] = $(tr.children[6]).find('input').val();
                    rowData['columnClause'] = rowData['columnClause'].replace(/'/g, '&apos;');
                    rowData['columnClauseActualValue'] = $(tr.children[6]).find('input').val();
                    rowData['funcolumnslist'] = $(tr.children[6]).find('input').attr("funcolumnslist");
                    rowData['dataFunTables'] = $(tr.children[6]).find('input').attr("tableName");
                    rowData['data-funobjstr'] = $(tr.children[6]).find('input').attr("data-funobjstr"); // ravi change new
                    rowData['data-columnClause'] = $(tr.children[6]).find('input').val(); // ravi change new
                    colMappingsData[rowIndex] = rowData;
                    if (rowData['primaryKey'] == 'Y') {
                        trfmRules['primaryKey'] = "Y";
                    }
                }

                trfmRules['colMappingsData'] = colMappingsData;
				var skipRecordsCheckBoxVal = $("#skipRejectedRecords").prop('checked');
				var skipRejectedRecords = (skipRecordsCheckBoxVal == true) ? "Y" : "N";
				trfmRules['skipRejectedRecords'] = skipRejectedRecords;
				var truncateDestinationCheckBoxVal = $("#truncateDestination").prop('checked');
				var truncateDestination = (truncateDestinationCheckBoxVal == true) ? "Y" : "N";
				trfmRules['truncateDestination'] = truncateDestination;

            }
            operatorData['simpleColumnsList'] = simpleColumnsList;
            operatorData['trfmRules'] = trfmRules;
            $flowchart.flowchart('setOperatorData', selectedOperatorId, operatorData);
        }
    }
}

function viewTrfmRules() {
    saveTrfmRulesToOp();
    var $flowchart = $('#' + flowChartWorkSpaceId);
    var selectedOperatorId = $flowchart.flowchart('getSelectedOperatorId');
    try {
        selectedOperatorId = parseInt(selectedOperatorId);
    } catch (e) {
    }
    $("#currentTrnsOpId").val(selectedOperatorId);
    var url = 'columnMappingTrfmRules';

    var selectedOperatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", selectedOperatorId);
    var toIconType = selectedOperatorData['iconType'];

    if (selectedOperatorData['iconType'] == 'SQL' && selectedOperatorData['tableName']==null) {
        showMesg("Please create Table");
        throw new Error("Please create Table");
    }

    if (toIconType == 'OUTPUT') {
        url = "columnMappingTrfmRulesForComponent";
    }
//    var connectedFrom = selectedOperatorData['connectedFrom'];

    var connectedFrom = getConnectedFromOpIds(selectedOperatorId)
    if (connectedFrom.length ==0){

             showMesg("Transformation Rules does not Apply");
            throw new Error("Transformation Rules does not Apply");
             // because it is source operator
    }
    if (connectedFrom != null) {
        var fromOperatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", connectedFrom[0]);
        var fromOperatorIconType = fromOperatorData['iconType'];
        if (fromOperatorIconType != null && (fromOperatorIconType == 'SAPLOAD' || fromOperatorIconType == 'SAPLOADSTANDARD' || fromOperatorIconType == 'SAPLOADREVERSE' || fromOperatorIconType == 'SAPRESUME' || fromOperatorIconType == 'SAPJOIN') ) {
            var fromOperatorId = fromOperatorData['operatorId'];
            connectedFrom = getConnectedFromOpIds(fromOperatorId)
        }

         if (fromOperatorIconType != null && fromOperatorIconType =='MERGE_FILES' ) {
            var fromOperatorId = fromOperatorData['operatorId'];
            connectedFrom = getConnectedFromOpIds(fromOperatorId)
        }

        if (fromOperatorIconType != null && fromOperatorIconType =='MERGE' ) {
             showMesg("Transformation Rules does not Apply");
            throw new Error("Transformation Rules does not Apply");
        }
		
		if (fromOperatorIconType != null && fromOperatorIconType == 'FILE_FOLDER') {
			showMesg("Transformation Rules does not Apply");
			throw new Error("Transformation Rules does not Apply");
		}

    }

    if (connectedFrom != null) {
        var sourceOperators = [];
        $.each(connectedFrom, function (i) {
            var operatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", connectedFrom[i]);
            sourceOperators.push(operatorData);
            var iconType = operatorData['iconType'];

            if (iconType == 'QUERY') {
                url = "columnMappingTrfmRulesQueryComp";
            }
            if (iconType == 'API'){
				 url ="columnMappingTrfmRulesAPIComp"
			}
        })



        var flowchartData = $flowchart.flowchart('getData');
        showLoader();
        $.ajax({
            type: 'post',
            traditional: true,
            dataType: 'json',
            cache: false,
            url: url,
            async: true,
            data: {
                selectedOperatorId: selectedOperatorId,
                flowchartData: JSON.stringify(flowchartData),
                sourceOperators: JSON.stringify(sourceOperators)
            },
            success: function (response) {
                stopLoader();
                if (response != null) {

                    var simpleColumnsList = response['simpleColumnsList'];
                    var selectedOperatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", selectedOperatorId);
                    selectedOperatorData['simpleColumnsList'] = simpleColumnsList;
                    $("#" + flowChartWorkSpaceId).flowchart("setOperatorData", selectedOperatorId, selectedOperatorData);

                    var colMappingStr = response['colMappingStr'];
                    $("#dataMigrationTabs").remove();
                    var tabsDiv = "<div id='dataMigrationTabs' class='dataMigrationTabs'><ul class='dataMigrationTabsHeader'>"
		                            + "<li class='dataMigrationTabsli'>Mapping</li>"
		                            + "</ul>"
		                            + "<div id='tabs-1' class='dataMigrationsTabsInner'>"
		                            + colMappingStr
		                            + " </div>";

                    $("#feedContentArea").html(tabsDiv);
                    $("#dataMigrationTabs").jqxTabs({theme: 'ui-redmond'});

					//setTabStyle('dataMigrationTabs');

                    $("#toTableColsArray_hidden").remove();
                    $("#fromTableColsArray_hidden").remove();
                    var hiddenData = "<input type='hidden' id='toTableColsArray_hidden'/><input type='hidden' id='fromTableColsArray_hidden'/>";
                    $('#tabs-1').append(hiddenData);
                    $("#toTableColsArray_hidden").val(JSON.stringify(response['toTableColsArray']));
                    $("#fromTableColsArray_hidden").val(JSON.stringify(response['fromTableColsArray']));
                    $('#tabs-1').append("<div id='selectedColumnStr' style='display:none'></div>");
                    $("#selectedColumnStr").html(response['selectedColumnStr']);//colMapTrString

                    $("#sourceTablesArray").remove();
                    $("#destinationTablesArray").remove();
                    var hiddenData = "<input type='hidden' id='sourceTablesArray'/><input type='hidden' id='destinationTablesArray'/>";
                    $('#tabs-1').append(hiddenData);
                    $("#sourceTablesArray").val(JSON.stringify(response['sourceTablesArray']));
                    $("#destinationTablesArray").val(JSON.stringify(response['destinationTablesArray']));
                    $("#contentSplitter").resize();

                }
            },
            error: function (e)
            {
                sessionTimeout(e);
            }

        });
    }
}

function stagingComponentTrfmRules() {
    saveTrfmRulesToOp();
    var $flowchart = $('#' + flowChartWorkSpaceId);
    var selectedOperatorId = $flowchart.flowchart('getSelectedOperatorId');
    try {
        selectedOperatorId = parseInt(selectedOperatorId);
    } catch (e) {
    }
    $("#currentTrnsOpId").val(selectedOperatorId);
    var url = 'columnMappingTrfmRulesForComponent';

    var selectedOperatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", selectedOperatorId);

//    var connectedFrom = selectedOperatorData['connectedFrom'];
    var connectedFrom = getConnectedFromOpIds(selectedOperatorId)

    var sourceOperators = [];
    $.each(connectedFrom, function (i) {
        var operatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", connectedFrom[i]);
        sourceOperators.push(operatorData);
        var iconType = operatorData['iconType'];

        if (iconType == 'QUERY') {
            url = "columnMappingTrfmRulesQueryComp";
        }
    })

    var flowchartData = $flowchart.flowchart('getData');
    showLoader();
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'json',
        cache: false,
        url: url,
        async: true,
        data: {
            selectedOperatorId: selectedOperatorId,
            flowchartData: JSON.stringify(flowchartData),
            sourceOperators: JSON.stringify(sourceOperators)
        },
        success: function (response) {
            stopLoader();
            if (response != null) {

                var simpleColumnsList = response['simpleColumnsList'];
                var selectedOperatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", selectedOperatorId);
                selectedOperatorData['simpleColumnsList'] = simpleColumnsList;
                $("#" + flowChartWorkSpaceId).flowchart("setOperatorData", selectedOperatorId, selectedOperatorData);

                var colMappingStr = response['colMappingStr'];
                $("#dataMigrationTabs").remove();
                var tabsDiv = "<div id='dataMigrationTabs' class='dataMigrationTabs'><ul class='dataMigrationTabsHeader'>"
                        + "<li class='dataMigrationTabsli'>Mapping</li>"
                        + "</ul>"
                        + "<div id='tabs-1' class='dataMigrationsTabsInner'>"
                        + colMappingStr
                        + " </div>";

                $("#feedContentArea").html(tabsDiv);
                $("#dataMigrationTabs").jqxTabs({theme: 'ui-redmond'});

					//setTabStyle('dataMigrationTabs');

                $("#toTableColsArray_hidden").remove();
                $("#fromTableColsArray_hidden").remove();
                var hiddenData = "<input type='hidden' id='toTableColsArray_hidden'/><input type='hidden' id='fromTableColsArray_hidden'/>";
                $('#tabs-1').append(hiddenData);
                $("#toTableColsArray_hidden").val(JSON.stringify(response['toTableColsArray']));
                $("#fromTableColsArray_hidden").val(JSON.stringify(response['fromTableColsArray']));
                $('#tabs-1').append("<div id='selectedColumnStr' style='display:none'></div>");
                $("#selectedColumnStr").html(response['selectedColumnStr']);//colMapTrString

                $("#sourceTablesArray").remove();
                $("#destinationTablesArray").remove();
                var hiddenData = "<input type='hidden' id='sourceTablesArray'/><input type='hidden' id='destinationTablesArray'/>";
                $('#tabs-1').append(hiddenData);
                $("#sourceTablesArray").val(JSON.stringify(response['sourceTablesArray']));
                $("#destinationTablesArray").val(JSON.stringify(response['destinationTablesArray']));


            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });

}

function mergeComponentTrfmRules() {

    saveTrfmRulesToOp();
    var $flowchart = $('#' + flowChartWorkSpaceId);
    var selectedOperatorId = $flowchart.flowchart('getSelectedOperatorId');
    try {
        selectedOperatorId = parseInt(selectedOperatorId);
    } catch (e) {
    }
    $("#currentTrnsOpId").val(selectedOperatorId);

    var selectedOperatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", selectedOperatorId);

//    var fromOperatorsArray = $flowchart.flowchart('getAllFromOperatorsByToOpId', selectedOperatorId);
//    var toOperatorsArray = $flowchart.flowchart('getAllToOperatorsByToOpId', selectedOperatorId);
//    var fromOperatorsArray = selectedOperatorData['connectedFrom'];
    var fromOperatorsArray = getConnectedFromOpIds(selectedOperatorId)

    var toOperatorsArray = getConnectedToOpIds(selectedOperatorId);
    var toOperatorData = $('#' + flowChartWorkSpaceId).flowchart('getOperatorData', toOperatorsArray[0]);
    if (toOperatorData['iconType'] == 'SQL' && toOperatorData['tableName'] == null){
            showMesg("Please Create target table");
            throw new Error("Please Create target table");
    }

    var fromOperatorId = fromOperatorsArray[0];
    var fromOperator = $flowchart.flowchart('getOperatorData', fromOperatorId);
    var tableName = fromOperator['statusLabel'];
    var connObj = fromOperator['connObj'];

    var flowchartData = $flowchart.flowchart('getData');

    var selectedOperatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", selectedOperatorId);
//    var connectedFromOperatorsArray = $flowchart.flowchart('getAllConnFromOperatorsByOpId', selectedOperatorId, []);
//    var sourceOperators = [];
//    $.each(connectedFromOperatorsArray, function (i) {
//        var component = this['component'];
//        if (component != "Y") {
//            sourceOperators.push(this);
//        }
//    })
//    var connectedFrom = selectedOperatorData['connectedFrom'];

    var sourceOperators = [];
    $.each(fromOperatorsArray, function (i) {
        var operatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", fromOperatorsArray[i]);
        sourceOperators.push(operatorData);
    })

   showLoader();
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'json',
        cache: false,
        url: 'mergeComponentTrfmRules',
        async: true,
        data: {
            tableName: tableName,
            connObj: JSON.stringify(connObj),
            selectedOperatorId: selectedOperatorId,
            sourceOperators: JSON.stringify(sourceOperators),
            flowchartData: JSON.stringify(flowchartData)

        },
        success: function (response) {
            stopLoader();
            if (response != null) {



                var simpleColumnsList = response['simpleColumnsList'];

                selectedOperatorData['simpleColumnsList'] = simpleColumnsList;
                $("#" + flowChartWorkSpaceId).flowchart("setOperatorData", selectedOperatorId, selectedOperatorData);

                var colMappingStr = response['colMappingStr'];
                $("#dataMigrationTabs").remove();
                var tabsDiv = "<div id='dataMigrationTabs' class='dataMigrationTabs'>"
                        + "<ul class='dataMigrationTabsHeader'>"
                        + "<li class='dataMigrationTabsli'>Merge</li>"

                        + "</ul>"
                        + "<div id='tabs-1' class='dataMigrationsTabsInner'>"
//                        + colMappingStr
                        + " </div>"

                        + " </div>";

                $("#feedContentArea").html(tabsDiv);
                $("#dataMigrationTabs").jqxTabs({theme: 'ui-redmond'});
				//setTabStyle('dataMigrationTabs');
                $("#toTableColsArray_hidden").remove();
                $("#fromTableColsArray_hidden").remove();
                var hiddenData = "<input type='hidden' id='toTableColsArray_hidden'/><input type='hidden' id='fromTableColsArray_hidden'/>";
                $('#tabs-1').append(colMappingStr);
                $('#tabs-1').append(hiddenData);

                $("#toTableColsArray_hidden").val(JSON.stringify(response['toTableColsArray']));
                $("#fromTableColsArray_hidden").val(JSON.stringify(response['fromTableColsArray']));
                $('#tabs-1').append("<div id='selectedColumnStr' style='display:none'></div>");
                $("#selectedColumnStr").html(response['selectedColumnStr']);//colMapTrString

                $("#sourceTablesArray").remove();
                $("#destinationTablesArray").remove();

                var hiddenData = "<input type='hidden' id='sourceTablesArray'/><input type='hidden' id='destinationTablesArray'/>";
                $('#tabs-1').append(hiddenData);
                $("#sourceTablesArray").val(JSON.stringify(response['sourceTablesArray']));
                $("#destinationTablesArray").val(JSON.stringify(response['destinationTablesArray']));

               // var joinTableString = response['joinTableString'];

               // $("#tabs-2").append(joinTableString);

//                $('#dataMigrationTabs').unbind('selected').on('selected', function (event) {
////                    $("#mainSplitter").resize();
//                    $(".pcoded-main-container").css("margin-top", "127px");
//                });
//                $("#contentSplitter").resize();
//
//                var simpleColumnsList = response['simpleColumnsList'];
//
//                selectedOperatorData['simpleColumnsList'] = simpleColumnsList;
//                $("#" + flowChartWorkSpaceId).flowchart("setOperatorData", selectedOperatorId, selectedOperatorData);
//
//                var result = response['result'];
//                $("#dataMigrationTabs").remove();
//                var tabsDiv = "<div id='dataMigrationTabs' class='dataMigrationTabs'><ul class='dataMigrationTabsHeader'>"
//                        + "<li class='dataMigrationTabsli'><a href='#tabs-1'><img src ='images/Unique_Icon.png' style='width:15px;'/> Merge</a></li>"
//                        + "</ul>"
//                        + "<div id='tabs-1' class='dataMigrationsTabsInner visionMergeCols' >"
//                        + result
//                        + " </div>";
//
//                $("#feedContentArea").html(tabsDiv);
//                $("#dataMigrationTabs").jqxTabs({theme: 'ui-redmond'});
				//setTabStyle('dataMigrationTabs');
                $("#uniqueCompSelectAll").click(function (event) {
                    $(".visionPKSelectCbx:checkbox").each(function (i) {
                        if ($("#uniqueCompSelectAll:checkbox").prop("checked")) {
                            $(this).prop("checked", true);
                        } else {
                            $(this).prop("checked", false);
                        }

                    })
                })

                $("#updateCompSelectAll").click(function (event) {
                    $(".visionUpdateSelectCbx:checkbox").each(function (i) {
                        if ($("#updateCompSelectAll:checkbox").prop("checked")) {
                            $(this).prop("checked", true);
                        } else {
                            $(this).prop("checked", false);
                        }
                    });
                    $(".visionPKSelectCbx:checkbox").each(function () {
                        var $updateCheckbox = $(this).closest("tr").find(".visionUpdateSelectCbx:checkbox");
                        if ($(this).prop("checked")) {
                            $updateCheckbox.prop("checked", false).attr("disabled", true);
                        } else {
                            $updateCheckbox.removeAttr("disabled");
                        }
                    });
                });

                $("#operatorType").change(function (event) {
                    var value = $("#operatorType").val();
                    if (value != null && value == "Insert") {
                        $("#updateCompSelectAll:checkbox").prop("checked", false);
                        $("#updateCompSelectAll:checkbox").prop("disabled", true);
                        $(".visionUpdateSelectCbx:checkbox").each(function (i) {
                            $(this).prop("checked", false);
                            $(this).attr("disabled", true);
                        });
                    } else {

                        $("#updateCompSelectAll:checkbox").prop("disabled", false);
                        $(".visionUpdateSelectCbx:checkbox").each(function (i) {
                            $(this).attr("disabled", false);
                        });
                        $(".visionPKSelectCbx:checkbox").each(function () {
                            var $updateCheckbox = $(this).closest("tr").find(".visionUpdateSelectCbx:checkbox");
                            if ($(this).prop("checked")) {
                                $updateCheckbox.prop("checked", false).attr("disabled", true);
                            } else {
                                $updateCheckbox.removeAttr("disabled");
                            }
                        });
                    }

                });
                    $(".visionPKSelectCbx:checkbox").click(function (event) {
                    var $visionUpdateSelectCbx = $(this).closest("tr").find(".visionUpdateSelectCbx:checkbox");
                    if ($(this).prop("checked")) {
                        showMesg("Primary key column can't be update key");
                        $visionUpdateSelectCbx.prop("checked", false);
                        $visionUpdateSelectCbx.attr("disabled", true);
                    } else {
                        $visionUpdateSelectCbx.prop("checked", false);
                        $visionUpdateSelectCbx.attr("disabled", false);
                        var value = $("#operatorType").val();
                        if (value != null && value == "Insert") {
                            $("#updateCompSelectAll:checkbox").prop("checked", false);
                            $("#updateCompSelectAll:checkbox").prop("disabled", true);
                            $(".visionUpdateSelectCbx:checkbox").each(function (i) {
                                $(this).prop("checked", false);
                                $(this).attr("disabled", true);
                            });
                        }
                    }
                });
            }
        },
        error: function (e)
        {
            stopLoader();
            sessionTimeout(e);
        }

    });
    prevTargetOperatorId = selectedOperatorId

}


function scdComponentTrfmRules() {

    saveTrfmRulesToOp();
    var $flowchart = $('#' + flowChartWorkSpaceId);
    var selectedOperatorId = $flowchart.flowchart('getSelectedOperatorId');
    try {
        selectedOperatorId = parseInt(selectedOperatorId);
    } catch (e) {
    }
    $("#currentTrnsOpId").val(selectedOperatorId);
    var selectedOperatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", selectedOperatorId);
    var trfmRules = selectedOperatorData['trfmRules'];
    var selectedSCDType = "";
    if (trfmRules != null) {
        selectedSCDType = trfmRules['selectedSCDType'];
    }

    var scdTypesDiv = "<div id='selectSCDTypesRadioButton' class='visionSCDTypesRadioButton'><input type='radio' id='SCD1' name='SCD1' value='SCD1' >"
            + "<label for='SCD1'>SCD Type 1</label>"
            + "<input type='radio' id='SCD2' name='SCD2' value='SCD2'  >"
            + "<label for='SCD2'>SCD Type 2</label>"
            + "<input type='radio' id='SCD3' name='SCD3' value='SCD3' >"
            + "<label for='SCD3'>SCD Type 3</label>"
            + "<input type='radio' id='SCD4' name='SCD4' value='SCD4' >"
            + "<label for='SCD3'>SCD Type 4</label>"
            + "<input type='radio' id='SCD6' name='SCD6' value='SCD6' >"
            + "<label for='SCD6'>Hybrid Type</label>"
            + "</div>"

    $("#dataMigrationTabs").remove();
    var tabsDiv = "<div id='dataMigrationTabs' class='dataMigrationTabs'><ul class='dataMigrationTabsHeader'>"
            + "<li class='dataMigrationTabsli'><img src ='images/Unique_Icon.png' style='width:15px;'/> SCD </li>"
            + "</ul>"
            + "<div id='tabs-1' class='dataMigrationsTabsInner'>"
            + scdTypesDiv
            + " </div>";

    $("#feedContentArea").html(tabsDiv);
    $("#dataMigrationTabs").jqxTabs({theme: 'ui-redmond'});
    //setTabStyle('dataMigrationTabs');
    $("#contentSplitter").resize();

    $("#selectSCDTypesRadioButton :input").on("change", function (event) {
        $("#selectSCDTypesRadioButton").find("input:radio").prop("checked", false);
        $(event.target).prop("checked", true);
        var target = $(event.target);
        var inputval = $(event.target).val();
        if (inputval != null && (inputval == "SCD3" || inputval == "SCD6")) {
            var url = "";
            if (inputval == "SCD3") {
                url = 'scdType3TrfmRules';
            } else if (inputval == "SCD6") {
                url = 'scdType6TrfmRules';
            }
            var fromOperatorsArray = getConnectedFromOpIds(selectedOperatorId)
            var fromOperatorId = fromOperatorsArray[0];
            var fromOperator = $flowchart.flowchart('getOperatorData', fromOperatorId);
            var tableName = fromOperator['statusLabel'];
            var connObj = fromOperator['connObj'];

            var flowchartData = $flowchart.flowchart('getData');
            showLoader();
            $.ajax({
                type: 'post',
                traditional: true,
                dataType: 'json',
                cache: false,
                url: url,
                async: true,
                data: {
                    tableName: tableName,
                    connObj: JSON.stringify(connObj),
                    selectedOperatorId: selectedOperatorId,
                    flowchartData: JSON.stringify(flowchartData)

                },
                success: function (response) {
                    stopLoader();
                    if (response != null) {
                        $(".visionSCDTypeColsDiv").remove();
                        var simpleColumnsList = response['simpleColumnsList'];

                        selectedOperatorData['simpleColumnsList'] = simpleColumnsList;
                        $("#" + flowChartWorkSpaceId).flowchart("setOperatorData", selectedOperatorId, selectedOperatorData);

                        var result = response['result'];
                        $("#tabs-1").append(result);
                    }
                },
                error: function (e)
                {
                    stopLoader();
                    sessionTimeout(e);
                }

            });
        } else {
            $(".visionSCDTypeColsDiv").remove();

        }

    })

//    $("#"+selectedSCDType).prop('checked',true);
    $("#" + selectedSCDType).trigger("click");

    prevTargetOperatorId = selectedOperatorId

}


function uniqueComponentTrfmRules() {

    saveTrfmRulesToOp();
    var $flowchart = $('#' + flowChartWorkSpaceId);
    var selectedOperatorId = $flowchart.flowchart('getSelectedOperatorId');
    try {
        selectedOperatorId = parseInt(selectedOperatorId);
    } catch (e) {
    }
    $("#currentTrnsOpId").val(selectedOperatorId);

    var selectedOperatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", selectedOperatorId);

//    var fromOperatorsArray = $flowchart.flowchart('getAllFromOperatorsByToOpId', selectedOperatorId);
//    var toOperatorsArray = $flowchart.flowchart('getAllToOperatorsByToOpId', selectedOperatorId);
//    var fromOperatorsArray = selectedOperatorData['connectedFrom'];
    var fromOperatorsArray = getConnectedFromOpIds(selectedOperatorId)
    var fromOperatorId = fromOperatorsArray[0];
    var fromOperator = $flowchart.flowchart('getOperatorData', fromOperatorId);
    var tableName = fromOperator['statusLabel'];
    var connObj = fromOperator['connObj'];

    var flowchartData = $flowchart.flowchart('getData');
    showLoader();
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'json',
        cache: false,
        url: 'uniqueComponentTrfmRules',
        async: true,
        data: {
            tableName: tableName,
            connObj: JSON.stringify(connObj),
            selectedOperatorId: selectedOperatorId,
            flowchartData: JSON.stringify(flowchartData)

        },
        success: function (response) {
            stopLoader();
            if (response != null) {

                var simpleColumnsList = response['simpleColumnsList'];

                selectedOperatorData['simpleColumnsList'] = simpleColumnsList;
                $("#" + flowChartWorkSpaceId).flowchart("setOperatorData", selectedOperatorId, selectedOperatorData);

                var result = response['result'];
                $("#dataMigrationTabs").remove();
                var tabsDiv = "<div id='dataMigrationTabs' class='dataMigrationTabs visionUniqueCols'><ul class='dataMigrationTabsHeader'>"
                        + "<li class='dataMigrationTabsli'><img src ='images/Unique_Icon.png' style='width:15px;'/> Unique Key</li>"
                        + "</ul>"
                        + "<div id='tabs-1' class='dataMigrationsTabsInner'>"
                        + result
                        + " </div>";

                $("#feedContentArea").html(tabsDiv);
                $("#dataMigrationTabs").jqxTabs({theme: 'ui-redmond'});
               // setTabStyle('dataMigrationTabs');
                $("#contentSplitter").resize();
            }
        },
        error: function (e)
        {
            stopLoader();
            sessionTimeout(e);
        }

    });
    prevTargetOperatorId = selectedOperatorId

}

function filterComponentTrfmRules() {

    saveTrfmRulesToOp();
    var $flowchart = $('#' + flowChartWorkSpaceId);
    var selectedOperatorId = $flowchart.flowchart('getSelectedOperatorId');
    try {
        selectedOperatorId = parseInt(selectedOperatorId);
    } catch (e) {
    }
    $("#currentTrnsOpId").val(selectedOperatorId);
    var selectedOperatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", selectedOperatorId);
//    var connectedFromOperatorsArray = $flowchart.flowchart('getAllConnFromOperatorsByOpId', selectedOperatorId, []);
//    var sourceOperators = [];
//    $.each(connectedFromOperatorsArray, function (i) {
//        var component = this['component'];
//        if (component != "Y") {
//            sourceOperators.push(this);
//        }
//    })
//    var connectedFrom = selectedOperatorData['connectedFrom'];
    var connectedFrom = getConnectedFromOpIds(selectedOperatorId)

    var sourceOperators = [];
    $.each(connectedFrom, function (i) {
        var operatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", connectedFrom[i]);
        sourceOperators.push(operatorData);
    })

    var flowchartData = $flowchart.flowchart('getData');
    showLoader();
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'json',
        cache: false,
        url: 'filterComponentTrfmRules',
        async: true,
        data: {
            selectedOperatorId: selectedOperatorId,
            flowchartData: JSON.stringify(flowchartData),
            sourceOperators: JSON.stringify(sourceOperators)

        },
        success: function (response) {
            stopLoader();
            if (response != null) {
                var simpleColumnsList = response['simpleColumnsList'];

                selectedOperatorData['simpleColumnsList'] = simpleColumnsList;
                $("#" + flowChartWorkSpaceId).flowchart("setOperatorData", selectedOperatorId, selectedOperatorData);

                var whereClauseCondition = response['whereClauseCondition'];

                $("#dataMigrationTabs").remove();
                var tabsDiv = "<div id='dataMigrationTabs' class='dataMigrationTabs'><ul class='dataMigrationTabsHeader'>"
                        + "<li class='dataMigrationTabsli'><a href='#tabs-1'>Filter</a></li>"
                        + "</ul>"
                        + "<div id='tabs-1' class='dataMigrationsTabsInner'>"
                        + whereClauseCondition
                        + " </div>";

                $("#feedContentArea").html(tabsDiv);
                $("#dataMigrationTabs").jqxTabs({theme: 'ui-redmond'});
				//setTabStyle('dataMigrationTabs');
                $("#whereClauseTableColsArray_hidden").remove();
                var hiddenData = "<input type='hidden' id='whereClauseTableColsArray_hidden'/>";
                $('#tabs-1').append(hiddenData);

                var hiddenDataWhere = "<input type='hidden' id='whereClauseTableColsArray_hidden'/><input type='hidden' id='currentClauseMapId'/>"
                        + "<div id='wherClauseTrString' style='display:none;'></div><div id='wherClauseColsString' style='display:none;'></div>";
                $('#tabs-1').html(response['whereClauseCondition'] + hiddenDataWhere);//whereClauseCondition
                $("#whereClauseTableColsArray_hidden").val(JSON.stringify(response['fromTableColsArray']));


//                $("#whereClauseTableColsArray_hidden").val(JSON.stringify(response['fromTableColsArray']));
//                $('#tabs-1').append("<div id='selectedColumnStr' style='display:none'></div>");
//                $("#selectedColumnStr").html(response['selectedColumnStr']);//colMapTrString
                $("#sourceTablesArray").remove();
                $("#destinationTablesArray").remove();
                var hiddenData = "<input type='hidden' id='sourceTablesArray'/><input type='hidden' id='destinationTablesArray'/>";
                $('#tabs-1').append(hiddenData);
                $("#sourceTablesArray").val(JSON.stringify(response['sourceTablesArray']));
//                $("#destinationTablesArray").val(JSON.stringify(response['destinationTablesArray']));
                $("#contentSplitter").resize();
            }
        },
        error: function (e)
        {
            stopLoader();
            sessionTimeout(e);
        }

    });
    prevTargetOperatorId = selectedOperatorId

}

function inputOutPutPopUp(operatorId) {
    var operatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", operatorId);
    var tableName = operatorData['statusLabel'];
    var html = '<input type="radio" id="inputOperator" name="sourceOrTarget" value="SOURCE"><label for="inputOperator">Input</label><br>'
            + '<input type="radio" id="outputOperator" name="sourceOrTarget" value="TARGET"><label for="outputOperator">OutPut</label><br>'
    $("#dialog").append("<div id='dialog" + operatorId + "'></div>")
    $("#dialog" + operatorId).html(html);
    $("#dialog" + operatorId).dialog({
        title: (labelObject[tableName] != null ? labelObject[tableName] : tableName),
        modal: true,
        width: 300,
        height: 135,
        fluid: true,
        buttons: [{
                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                click: function () {

                    var sourceOrTarget = $('input[name="sourceOrTarget"]:checked').val();
                    operatorData['sourceOrTarget'] = sourceOrTarget;
                    $("#" + flowChartWorkSpaceId).flowchart("setOperatorData", operatorId, operatorData);

                    $(this).html("");
//                    $(this).dialog("close");
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


function setTransformationRulesToOperator(selectedOperatorId, component) {
    if (component == "UNIQUE") {

    }
    var OperatorData = $('#' + flowChartWorkSpaceId).flowchart('getSelectedOperatorId');

}


function joinComponentTrfmRules() {
    saveTrfmRulesToOp();
    var $flowchart = $('#' + flowChartWorkSpaceId);
    var selectedOperatorId = $flowchart.flowchart('getSelectedOperatorId');
    try {
        selectedOperatorId = parseInt(selectedOperatorId);
    } catch (e) {
    }
    $("#currentTrnsOpId").val(selectedOperatorId);
    var selectedOperatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", selectedOperatorId);

//    var connectedFrom = selectedOperatorData['connectedFrom'];
    var connectedFrom = getConnectedFromOpIds(selectedOperatorId)

    var sourceOperators = [];
    $.each(connectedFrom, function (i) {
        var operatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", connectedFrom[i]);
        sourceOperators.push(operatorData);
    })


    var flowchartData = $flowchart.flowchart('getData');
    showLoader();
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'json',
        cache: false,
        url: 'joinComponentTrfmRules',
        async: true,
        data: {
            selectedOperatorId: selectedOperatorId,
            flowchartData: JSON.stringify(flowchartData),
            sourceOperators: JSON.stringify(sourceOperators)

        },
        success: function (response) {
            stopLoader();
            if (response != null) {
                var simpleColumnsList = response['simpleColumnsList'];

                selectedOperatorData['simpleColumnsList'] = simpleColumnsList;
                $("#" + flowChartWorkSpaceId).flowchart("setOperatorData", selectedOperatorId, selectedOperatorData);

                var colMappingStr = response['colMappingStr'];
                $("#dataMigrationTabs").remove();
                var tabsDiv = "<div id='dataMigrationTabs' class='dataMigrationTabs'>"
                        + "<ul class='dataMigrationTabsHeader'>"
                        + "<li class='dataMigrationTabsli'>Mapping</li>"
                        + "<li class='dataMigrationTabsli'>Joins</li>"
                        + "</ul>"
                        + "<div id='tabs-1' class='dataMigrationsTabsInner'>"
//                        + colMappingStr
                        + " </div>"
                        + "<div id='tabs-2' class='dataMigrationsTabsInner'>"

                        + " </div>"
                        + " </div>";

                $("#feedContentArea").html(tabsDiv);
                $("#dataMigrationTabs").jqxTabs({theme: 'ui-redmond'});
				//setTabStyle('dataMigrationTabs');
                $("#toTableColsArray_hidden").remove();
                $("#fromTableColsArray_hidden").remove();
                var hiddenData = "<input type='hidden' id='toTableColsArray_hidden'/><input type='hidden' id='fromTableColsArray_hidden'/>";
                $('#tabs-1').append(colMappingStr);
                $('#tabs-1').append(hiddenData);

                $("#toTableColsArray_hidden").val(JSON.stringify(response['toTableColsArray']));
                $("#fromTableColsArray_hidden").val(JSON.stringify(response['fromTableColsArray']));
                $('#tabs-1').append("<div id='selectedColumnStr' style='display:none'></div>");
                $("#selectedColumnStr").html(response['selectedColumnStr']);//colMapTrString

                $("#sourceTablesArray").remove();
                $("#destinationTablesArray").remove();

                var hiddenData = "<input type='hidden' id='sourceTablesArray'/><input type='hidden' id='destinationTablesArray'/>";
                $('#tabs-1').append(hiddenData);
                $("#sourceTablesArray").val(JSON.stringify(response['sourceTablesArray']));
                $("#destinationTablesArray").val(JSON.stringify(response['destinationTablesArray']));

                var joinTableString = response['joinTableString'];

                $("#tabs-2").append(joinTableString);

                $('#dataMigrationTabs').unbind('selected').on('selected', function (event) {
//                    $("#mainSplitter").resize();
                    $(".pcoded-main-container").css("margin-top", "127px");
                });
                $("#contentSplitter").resize();
            }
        },
        error: function (e)
        {
            stopLoader();
            sessionTimeout(e);
        }

    });
    prevTargetOperatorId = selectedOperatorId
}


function sortComponentTrfmRules() {
    saveTrfmRulesToOp();
    var $flowchart = $('#' + flowChartWorkSpaceId);
    var selectedOperatorId = $flowchart.flowchart('getSelectedOperatorId');
    try {
        selectedOperatorId = parseInt(selectedOperatorId);
    } catch (e) {
    }
    $("#currentTrnsOpId").val(selectedOperatorId);

    var selectedOperatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", selectedOperatorId);

//    var connectedFrom = selectedOperatorData['connectedFrom'];
    var connectedFrom = getConnectedFromOpIds(selectedOperatorId)

    var sourceOperators = [];
    $.each(connectedFrom, function (i) {
        var operatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", connectedFrom[i]);
        sourceOperators.push(operatorData);
    })


    var flowchartData = $flowchart.flowchart('getData');
    showLoader();
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'json',
        cache: false,
        url: 'sortComponentTrfmRules',
        async: true,
        data: {
            selectedOperatorId: selectedOperatorId,
            flowchartData: JSON.stringify(flowchartData),
            sourceOperators: JSON.stringify(sourceOperators)

        },
        success: function (response) {
            stopLoader();
            if (response != null) {

                var simpleColumnsList = response['simpleColumnsList'];

                selectedOperatorData['simpleColumnsList'] = simpleColumnsList;
                $("#" + flowChartWorkSpaceId).flowchart("setOperatorData", selectedOperatorId, selectedOperatorData);

                var orderByClause = response['orderByCondition'];

                $("#dataMigrationTabs").remove();
                var tabsDiv = "<div id='dataMigrationTabs' class='dataMigrationTabs'><ul class='dataMigrationTabsHeader'>"
                        + "<li class='dataMigrationTabsli'>Sort</li>"
                        + "</ul>"
                        + "<div id='tabs-1' class='dataMigrationsTabsInner'>"
                        + orderByClause
                        + " </div>";

                $("#feedContentArea").html(tabsDiv);
                $("#dataMigrationTabs").jqxTabs({theme: 'ui-redmond'});
				//setTabStyle('dataMigrationTabs');
                $("#fromTableColsArray_hidden").remove();
                var fromTableColsArray = "<input type='hidden' id='fromTableColsArray_hidden'/>";

                var hiddenDataOrderBy = ""
                        + "<div id='orderClauseTrString' style='display:none;'></div>";

                $('#tabs-1').append(hiddenDataOrderBy + fromTableColsArray);
                $("#fromTableColsArray_hidden").val(JSON.stringify(response['fromTableColsArray']));
                $("#orderClauseTrString").html(response['orderByTrString']);
                $("#contentSplitter").resize();
            }
        },
        error: function (e)
        {
            stopLoader();
            sessionTimeout(e);
        }

    });
    prevTargetOperatorId = selectedOperatorId
}


function groupByComponentTrfmRules() {
    saveTrfmRulesToOp();
    var $flowchart = $('#' + flowChartWorkSpaceId);
    var selectedOperatorId = $flowchart.flowchart('getSelectedOperatorId');
    try {
        selectedOperatorId = parseInt(selectedOperatorId);
    } catch (e) {
    }
    $("#currentTrnsOpId").val(selectedOperatorId);

    var selectedOperatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", selectedOperatorId);

//    var connectedFrom = selectedOperatorData['connectedFrom'];
    var connectedFrom = getConnectedFromOpIds(selectedOperatorId)

    var sourceOperators = [];
    $.each(connectedFrom, function (i) {
        var operatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", connectedFrom[i]);
        sourceOperators.push(operatorData);
    })



    var flowchartData = $flowchart.flowchart('getData');
    showLoader();
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'json',
        cache: false,
        url: 'groupByComponentTrfmRules',
        async: true,
        data: {
            selectedOperatorId: selectedOperatorId,
            flowchartData: JSON.stringify(flowchartData),
            sourceOperators: JSON.stringify(sourceOperators)

        },
        success: function (response) {
            stopLoader();
            if (response != null) {
                var simpleColumnsList = response['simpleColumnsList'];

                selectedOperatorData['simpleColumnsList'] = simpleColumnsList;
                $("#" + flowChartWorkSpaceId).flowchart("setOperatorData", selectedOperatorId, selectedOperatorData);

                var colMappingStr = response['colMappingStr'];
                $("#dataMigrationTabs").remove();
                var tabsDiv = "<div id='dataMigrationTabs' class='dataMigrationTabs'>"
                        + "<ul class='dataMigrationTabsHeader'>"
                        + "<li class='dataMigrationTabsli'>Mapping</li>"
                        + "<li class='dataMigrationTabsli'>Group By</li>"
                        + "</ul>"
                        + "<div id='tabs-1' class='dataMigrationsTabsInner'>"
//                        + colMappingStr
                        + " </div>"
                        + "<div id='tabs-2' class='dataMigrationsTabsInner'>"

                        + " </div>"
                        + " </div>";

                $("#feedContentArea").html(tabsDiv);
                $("#dataMigrationTabs").jqxTabs({theme: 'ui-redmond'});
				//setTabStyle('dataMigrationTabs');

                $("#toTableColsArray_hidden").remove();
                $("#fromTableColsArray_hidden").remove();
                var hiddenData = "<input type='hidden' id='toTableColsArray_hidden'/><input type='hidden' id='fromTableColsArray_hidden'/>";
                $('#tabs-1').append(colMappingStr);
                $('#tabs-1').append(hiddenData);

                $("#toTableColsArray_hidden").val(JSON.stringify(response['toTableColsArray']));
                $("#fromTableColsArray_hidden").val(JSON.stringify(response['fromTableColsArray']));
                $('#tabs-1').append("<div id='selectedColumnStr' style='display:none'></div>");
                $("#selectedColumnStr").html(response['selectedColumnStr']);//colMapTrString

                $("#sourceTablesArray").remove();
                $("#destinationTablesArray").remove();

                var hiddenData = "<input type='hidden' id='sourceTablesArray'/><input type='hidden' id='destinationTablesArray'/>";
                $('#tabs-1').append(hiddenData);
                $("#sourceTablesArray").val(JSON.stringify(response['sourceTablesArray']));
                $("#destinationTablesArray").val(JSON.stringify(response['destinationTablesArray']));

                var groupByCondition = response['groupByCondition'];

                $("#tabs-2").append(groupByCondition);
                var hiddenDataGroupBy = ""
                        + "<div id='groupClauseTrString' style='display:none;'></div>";
                $('#tabs-2').append(hiddenDataGroupBy);
                $("#groupClauseTrString").html(response['groupClauseTrString']);

                $('#dataMigrationTabs').unbind('selected').on('selected', function (event) {
//                    $("#mainSplitter").resize();
                    $(".pcoded-main-container").css("margin-top", "127px");
                });
                $("#contentSplitter").resize();
            }
        },
        error: function (e)
        {
            stopLoader();
            sessionTimeout(e);
        }

    });
    prevTargetOperatorId = selectedOperatorId
}

function queryComponentTrfmRules() {
    saveTrfmRulesToOp();
    var $flowchart = $('#' + flowChartWorkSpaceId);
    var selectedOperatorId = $flowchart.flowchart('getSelectedOperatorId');
    try {
        selectedOperatorId = parseInt(selectedOperatorId);
    } catch (e) {
    }
    $("#currentTrnsOpId").val(selectedOperatorId);
    var selectedOperatorData = $flowchart.flowchart('getOperatorData', selectedOperatorId);

    var trfmRules = selectedOperatorData['trfmRules']
	var queryData = "";
	if(trfmRules != null && trfmRules != undefined){
		queryData = (trfmRules['queryData'] != null) ? trfmRules['queryData'] : "";
	}
    var flowchartData = $flowchart.flowchart('getData');

    var queryTextArea = '<textarea id="queryTextArea" name="queryTextArea" rows="9" class="visionEtlQueryTextArea">' + queryData + '</textarea>';

    $("#dataMigrationTabs").remove();
    var tabsDiv = "<div id='dataMigrationTabs' class='dataMigrationTabs'><ul class='dataMigrationTabsHeader'>"
            + "<li class='dataMigrationTabsli'>Query</li>"
            + "</ul>"
            + "<div id='tabs-1' class='dataMigrationsTabsInner'>"
            + queryTextArea
            + " </div>";

    $("#feedContentArea").html(tabsDiv);
    $("#dataMigrationTabs").jqxTabs({theme: 'ui-redmond'});
    //setTabStyle('dataMigrationTabs');
    $("#contentSplitter").resize();
//    $("#queryTextArea").val(queryData);
    prevTargetOperatorId = selectedOperatorId
}


function normalizeTrfmRules() {

    saveTrfmRulesToOp();
    var $flowchart = $('#' + flowChartWorkSpaceId);
    var selectedOperatorId = $flowchart.flowchart('getSelectedOperatorId');
    try {
        selectedOperatorId = parseInt(selectedOperatorId);
    } catch (e) {
    }
    $("#currentTrnsOpId").val(selectedOperatorId);

    var selectedOperatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", selectedOperatorId);

//    var connectedFrom = selectedOperatorData['connectedFrom'];
    var connectedFrom = getConnectedFromOpIds(selectedOperatorId)

    var sourceOperators = [];
    $.each(connectedFrom, function (i) {
        var operatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", connectedFrom[i]);
        sourceOperators.push(operatorData);
    })


    var flowchartData = $flowchart.flowchart('getData');
    showLoader();
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'json',
        cache: false,
        url: 'normalizeTrfmRules',
        async: true,
        data: {
            selectedOperatorId: selectedOperatorId,
            flowchartData: JSON.stringify(flowchartData),
            sourceOperators: JSON.stringify(sourceOperators)

        },
        success: function (response, status, xhr) {
            stopLoader();

            if (response != null) {
                var simpleColumnsList = response['simpleColumnsList'];
                selectedOperatorData['simpleColumnsList'] = simpleColumnsList;
                $("#" + flowChartWorkSpaceId).flowchart("setOperatorData", selectedOperatorId, selectedOperatorData);


                var divStr = response['divStr'];
                $("#feedContentArea").html(divStr);
                $("#dataMigrationTabs").jqxTabs({width: "100%", height: "100%", position: 'top', theme: 'ui-redmond', reorder: true});
				//setTabStyle('dataMigrationTabs');
            }
        },
        error: function (e)
        {
            stopLoader();
            sessionTimeout(e);
        }

    });

}

function denormalizeTrfmRules() {
    saveTrfmRulesToOp();
    var $flowchart = $('#' + flowChartWorkSpaceId);
    var selectedOperatorId = $flowchart.flowchart('getSelectedOperatorId');
    try {
        selectedOperatorId = parseInt(selectedOperatorId);
    } catch (e) {
    }
    $("#currentTrnsOpId").val(selectedOperatorId);

    var selectedOperatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", selectedOperatorId);

//    var connectedFrom = selectedOperatorData['connectedFrom'];
    var connectedFrom = getConnectedFromOpIds(selectedOperatorId)

    var sourceOperators = [];
    $.each(connectedFrom, function (i) {
        var operatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", connectedFrom[i]);
        sourceOperators.push(operatorData);
    })


    var flowchartData = $flowchart.flowchart('getData');
    showLoader();
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'json',
        cache: false,
        url: 'deNormalizeTrfmRules',
        async: true,
        data: {
            selectedOperatorId: selectedOperatorId,
            flowchartData: JSON.stringify(flowchartData),
            sourceOperators: JSON.stringify(sourceOperators)
        },
        success: function (response, status, xhr) {
            stopLoader();

            if (response != null) {
                var simpleColumnsList = response['simpleColumnsList'];
                selectedOperatorData['simpleColumnsList'] = simpleColumnsList;
                $("#" + flowChartWorkSpaceId).flowchart("setOperatorData", selectedOperatorId, selectedOperatorData);


                var divStr = response['divStr'];
                $("#feedContentArea").html(divStr);
                $("#dataMigrationTabs").jqxTabs({width: "100%", height: "100%", position: 'top', theme: 'ui-redmond', reorder: true});
				//setTabStyle('dataMigrationTabs');
            }
        },
        error: function (e)
        {
            stopLoader();
            sessionTimeout(e);
        }

    });

}


function validateDuplicateDestCols() {
    var destColsArray = [];
    var duplicateCols = [];
    var duplicateColsTr = [];
    var firstDuplicateTr;
    var duplicateColFlag = false;
    $('#sourceDestColsTableId tbody tr').each(function (i) {
        var destCol = $(this.children[2]).find('input').val();
        destCol = destCol.toUpperCase();
        destCol = destCol.indexOf(":") > -1 ? destCol.split(":")[1] : destCol;
        if (destColsArray.indexOf(destCol) > -1) {
            if (firstDuplicateTr == null) {
                firstDuplicateTr = this;
            }

//            $(this.children[2]).find('input').effect("highlight", {color: "yellow"}, 3000);
            $(this.children[2]).find('input').css("background", "yellow");
            duplicateColFlag = true;
            duplicateCols.push(destCol);
            duplicateColsTr.push(this);
        }
        destColsArray.push(destCol);
    })
    if (duplicateColFlag) {
        $(firstDuplicateTr.children[2]).find('input').focus();
        $('html, body').animate({
            scrollTop: $(firstDuplicateTr.children[2]).find('input').offset().top
        }, 1000);
        //showMesg("Duplicate destination Columns ::" + duplicateCols);

        $("#dialog").html("Duplicate destination Columns found. Do you want to delete them?" + duplicateCols);
		$("#dialog").dialog({
			title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
			modal: true,
			html:true,
			height: 'auto',
			minWidth: 300,
			maxWidth: 300,
			fluid: true,
			buttons: [{
				text: (labelObject['Yes'] != null ? labelObject['Yes'] : 'Yes'),
				click: function() {

					$.each(duplicateColsTr, function(i){
						$(this).remove();
					})
					$(this).html("");
					//                        //$(this).dialog("close");
					$(this).dialog("destroy");
				}
				},
				{
				text: (labelObject['No'] != null ? labelObject['No'] : 'No'),
				click: function() {
					$(this).html("");
					//                        //$(this).dialog("close");
					$(this).dialog("destroy");
				}
				}],
			open: function() {
				$(this).closest(".ui-dialog").css("z-index", "9999")
				$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
				$(".visionHeaderMain").css("z-index", "999");
				$(".visionFooterMain").css("z-index", "999");
			},
			beforeClose: function(event, ui) {
				$(".visionHeaderMain").css("z-index", "99999");
				$(".visionFooterMain").css("z-index", "99999");
			}
		});


        throw new Error("Duplicate destination Column Name ::");
    }

}

function checkDataTypeValidations(){
	classifySubJobs();
	showLoader();
        var targetOpId;
        var sourceOpId;
	var flowChartData = $('#' + flowChartWorkSpaceId).flowchart('getData');
        var operators = flowChartData['operators'];
        $.each(operators, function(i){
            var operator = operators[i];
            var operatorId = operator['operatorId'];
            var fromOperatorsArray = getConnectedFromOpIds(operatorId);
            if (fromOperatorsArray.length > 0) {
                targetOpId = operatorId;
            }
            if (fromOperatorsArray.length == 0){
                sourceOpId = operatorId;
            }
        })

	$.ajax({
        type: 'post',
        traditional: true,
        dataType: 'json',
        cache: false,
        url: 'checkDataTypeValidations',
        async: true,
        data: {
            flowChartData: JSON.stringify(flowChartData),
        },
        success: function (response) {
			stopLoader();
            try {
				if (response!=null){
					if (response['dataTypeMismatch']) {
						//showMesg("<span class='messageStatusErrorSpan' >Error </span><br>"+response['misMatchTable'], 300, 600);

						$("#dialog").html("<div><img src='images/etl/warning.png' class='messageStatusIcon' ><span class='messageStatusErrorSpan' >Datatypes Mismatch </span>"+response['misMatchTable']+"</div>");
						$("#dialog").dialog({
							title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
							modal: true,
							html:true,
							minHeight:300,
							maxHeight:500,
							width:800,
							fluid: true,
							buttons: [{
								text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
								click: function() {
									$(this).html("");
									//                        //$(this).dialog("close");
									$(this).dialog("destroy");
								}
							}],
							open: function() {
								$(this).closest(".ui-dialog").css("z-index", "9999")
								$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
								$(".visionHeaderMain").css("z-index", "999");
								$(".visionFooterMain").css("z-index", "999");
							},
							beforeClose: function(event, ui) {
								$(".visionHeaderMain").css("z-index", "99999");
								$(".visionFooterMain").css("z-index", "99999");
							}
						});

					} else {
						//showMesg("<span class='messageStatusSuccessSpan' >Success </span>");
						$("#dialog").html("<div><img src='images/etl/success.png' class='messageStatusIcon' ><span class='messageStatusSucessSpan' >Datatypes Matched </span></div>");
						$("#dialog").dialog({
							title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
							modal: true,
							html:true,
							height: 'auto',
							minWidth: 300,
							maxWidth: 300,
							fluid: true,
							buttons: [{
								text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
								click: function() {
									$(this).html("");
									//                        //$(this).dialog("close");
									$(this).dialog("destroy");
								}
							}],
							open: function() {
								$(this).closest(".ui-dialog").css("z-index", "9999")
								$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
								$(".visionHeaderMain").css("z-index", "999");
								$(".visionFooterMain").css("z-index", "999");
							},
							beforeClose: function(event, ui) {
								$(".visionHeaderMain").css("z-index", "99999");
								$(".visionFooterMain").css("z-index", "99999");
							}
						});

					}
				}
            } catch (e) {
            }
        }

    });
}

function getConnectedFromOpIds(selectedOperatorId) {
    var flowChartData = $("#" + flowChartWorkSpaceId).flowchart("getData");
    var links = flowChartData['links'];
    var connectedFrom = [];
    $.each(links, function (k, v) {
        var LinkData = links[k];
        var toOperator = LinkData['toOperator'];
        if (toOperator.toString() == selectedOperatorId.toString()) {
            connectedFrom.push(LinkData['fromOperator'])
        }
    })
    return connectedFrom;
}

function getConnectedToOpIds(selectedOperatorId) {
    var flowChartData = $("#" + flowChartWorkSpaceId).flowchart("getData");
    var links = flowChartData['links'];
    var connectedTo = [];

    $.each(links, function (k, v) {
        var LinkData = links[k];
        var fromOperator = LinkData['fromOperator'];
        if (fromOperator.toString() == selectedOperatorId.toString()) {
            connectedTo.push(LinkData['toOperator'])
        }
    })
    return connectedTo;
}


function validateTransformationRules(flowChartData) {
    var operators = flowChartData['operators'];
    $.each(operators, function (k, v) {

        var selectedOperatorId = k;
        var operatorData = v;
        var component = operatorData['component'];
        var iconType = operatorData['iconType'];
        var executionSequence = operatorData['executionSequence'];
        if (executionSequence == null || executionSequence < 0) {

            showMesg("Please provide execution Sequence");
            throw new Error("Please select Primary Key Columns ::");
        }
        var connectedFrom = getConnectedFromOpIds(selectedOperatorId);
        var connectedTo = getConnectedToOpIds(selectedOperatorId);

        if (component == "Y" && iconType != null && iconType == 'MERGE') {


            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
            var colMappingsData = trfmRules['colMappingsData'];
            validateColumnMappings(colMappingsData);

            //var operatorType = $("#operatorType").val();
            var operatorType = trfmRules['operatorType'];

             if (trfmRules['primaryKey']!="Y"){
                    showMesg("Please Select Primary key");
                    throw new Error("Please Select Primary key");
              }
              if (operatorType == "Insert Or Update" || operatorType == "Update") {
                  if (trfmRules['updateKey']!="Y") {
                        showMesg("Please Select update Columns");
                        throw new Error("Please Select update Columns");
                    }
              }

//            var uniqueKeyColsArray = trfmRules['uniqueKeys'];
//            if (uniqueKeyColsArray == null || uniqueKeyColsArray.length == 0) {
//               // showMesg("Please select Primary Key Columns ");
//               // throw new Error("Please select Primary Key Columns ::");
//            }
//            if (operatorType != null && operatorType != "Insert") {
//                var updateColsList = trfmRules['updateColsList'];
//                if (updateColsList == null || updateColsList.length == 0) {
//                 //   showMesg("Please select Update Columns ")
//                 //   throw new Error("Please select Update Columns");
//                }
//            }

        }
        if (component == "Y" && iconType != null && iconType == 'SCD') {
            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
            var selectedSCDType = trfmRules['selectedSCDType'];
            if (selectedSCDType == null) {
                showMesg("Please select SCD Type ")
                throw new Error("Please select SCD Type");
            }
            if (selectedSCDType != null) {
                trfmRules['selectedSCDType'] = selectedSCDType;
                if (selectedSCDType == "SCD3") {
                    var historyCols = trfmRules['historyCols'];
                    if (historyCols == null || historyCols.length == 0) {
                        showMesg("Please select history Columns ")
                        throw new Error("Please select history Columns");
                    }

                } else if (selectedSCDType == "SCD6") {
                    var historyCols = trfmRules['historyCols'];
                    if (historyCols == null || historyCols.length == 0) {
                        showMesg("Please select history Columns ")
                        throw new Error("Please select history Columns");
                    }
                }
            }
        }
        if (component == "Y" && iconType != null && iconType == 'UNIQUE') {
            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
            var uniqueKeyColsArray = trfmRules['uniqueKeys'];
            if (uniqueKeyColsArray == null || uniqueKeyColsArray.length == 0) {
                showMesg("Please select history Columns ")
                throw new Error("Please select history Columns");
            }

        }
        if (component == "Y" && iconType != null && iconType == 'FILTER') {
            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
            var whereClauseData = trfmRules['whereClauseData'];
            if (whereClauseData == null || whereClauseData.length == 0) {

                showMesg("Please Apply Filter ")
                throw new Error("Please select history Columns");
            }
            var whereClauseDataObj = whereClauseData[0];
            if (whereClauseDataObj == null || whereClauseDataObj == "{}" || jQuery.isEmptyObject(whereClauseDataObj)) {
                showMesg("Please Apply Filter ")
                throw new Error("Please select history Columns");
            }

        }

        if (component == "Y" && iconType != null && iconType == 'JOIN') {
            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
            var colMappingsData = trfmRules['colMappingsData'];
            validateColumnMappings(colMappingsData);

            var joinClauseData = trfmRules['joinClauseData'];
            if (joinClauseData == null || joinClauseData.length == 0) {
                showMesg("Please Apply Join Conditions ")
                throw new Error("Please Apply Join Conditions");
            }
            $.each(joinClauseData, function (i) {
                var joinClauseDataObj = joinClauseData[i];
                if (joinClauseDataObj == null || joinClauseDataObj == "{}" || jQuery.isEmptyObject(joinClauseDataObj)) {
                    showMesg("Please Apply Join Conditions ")
                    throw new Error("Please Apply Join Conditions");
                }
            })

        }
         if (component == "Y" && iconType != null && iconType == 'SAPJOIN') {
            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
//            var colMappingsData = trfmRules['colMappingsData'];
//            validateColumnMappings(colMappingsData);

            var joinClauseData = trfmRules['joinClauseData'];
            if (joinClauseData == null || joinClauseData.length == 0) {
                showMesg("Please Apply Join Conditions ")
                throw new Error("Please Apply Join Conditions");
            }
            $.each(joinClauseData, function (i) {
                var joinClauseDataObj = joinClauseData[i];
                if (joinClauseDataObj == null || joinClauseDataObj == "{}" || jQuery.isEmptyObject(joinClauseDataObj)) {
                    showMesg("Please Apply Join Conditions ")
                    throw new Error("Please Apply Join Conditions");
                }
            })

        }
        if (component == "Y" && iconType != null && iconType == 'SORT') {

            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
            var orderByData = trfmRules['orderByData'];
            if (orderByData == null || orderByData.length == 0) {
                showMesg("Please Select Sort column")
                throw new Error("Please Select Sort column");
            }
            $.each(orderByData, function (i) {
                var rowData = orderByData[i];
                if (rowData == null || rowData == "{}" || jQuery.isEmptyObject(rowData)) {
                    showMesg("Please Select Sort column")
                    throw new Error("Please Select Sort column");
                }
                var columnNameActualValue = rowData['columnNameActualValue'];
                if (columnNameActualValue == null || columnNameActualValue == "") {
                    showMesg("Please Select Sort column")
                    throw new Error("Please Select Sort column");
                }

            })

        }
        if (component == "Y" && iconType != null && iconType == 'UNGROUP') {
            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
            var normalizeOptionsObj = trfmRules['normalizeOptionsObj'];
            if (normalizeOptionsObj == null || normalizeOptionsObj == "{}" || jQuery.isEmptyObject(normalizeOptionsObj)) {
                showMesg("Please Select Normalize Column")
                throw new Error("Please Select Normalize Column");
            }
            var normalizeColumn = normalizeOptionsObj['normalizeColumn'];
            if (normalizeColumn == null || normalizeColumn == "") {
                showMesg("Please Select Normalize Column")
                throw new Error("Please Select Normalize Column");
            }
            var itemSeparator = normalizeOptionsObj['itemSeparator'];
            if (itemSeparator == null || itemSeparator == "") {
                showMesg("Please Select itemSeparator")
                throw new Error("Please Select itemSeparator");
            }

        }
        if (component == "Y" && iconType != null && iconType == 'GROUP') {

            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
            var normalizeOptionsObj = trfmRules['normalizeOptionsObj'];
            if (normalizeOptionsObj == null || normalizeOptionsObj == "{}" || jQuery.isEmptyObject(normalizeOptionsObj)) {
                showMesg("Please Select denormalize Column")
                throw new Error("Please Select denormalize Column");
            }
            var denormalizeColumn = normalizeOptionsObj['denormalizeColumn'];
            if (denormalizeColumn == null || denormalizeColumn == "") {
                showMesg("Please Select denormalize Column")
                throw new Error("Please Select denormalize Column");
            }
            var keyColumn = normalizeOptionsObj['keyColumn'];
            if (keyColumn == null || keyColumn == "") {
                showMesg("Please Select key Column")
                throw new Error("Please Select key Column");
            }
            var delimiter = normalizeOptionsObj['delimiter'];
            if (delimiter == null || delimiter == "") {
                showMesg("Please Select delimiter")
                throw new Error("Please Select delimiter");
            }

        }
        if (component == "Y" && iconType != null && iconType == 'GROUPBY') {

            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
            var colMappingsData = trfmRules['colMappingsData'];
            validateColumnMappings(colMappingsData);

            var groupByData = trfmRules['groupByData'];

            if (groupByData == null || groupByData.length == 0) {
                showMesg("Please Select Group By column")
                throw new Error("Please Select Group By column");
            }
            $.each(groupByData, function (i) {
                var rowData = groupByData[i];
                if (rowData == null || rowData == "{}" || jQuery.isEmptyObject(rowData)) {
                    showMesg("Please Select  Group By column")
                    throw new Error("Please Select  Group By column");
                }
                var columnNameActualValue = rowData['columnNameActualValue'];
                if (columnNameActualValue == null || columnNameActualValue == "") {
                    showMesg("Please Select  Group By column")
                    throw new Error("Please Select  Group By column");
                }

            })

        }
        if (component == "Y" && iconType != null && iconType == 'GROUP_JOB') {

            var jobId = operatorData['jobId'];
            if (jobId == null || jobId == "") {
                showMesg("Please Select Job");
                throw new Error("Please Select Job");
            }
            var mapNo = operatorData['executionSequence'];
            if (mapNo == null || mapNo == "") {
                showMesg("Please Select Job Execution Seq");
                throw new Error("Please Select Job Execution Seq");
            }
        }

        if (component == "Y" && iconType != null && iconType == 'QUERY') {
            var connObj = operatorData['connObj'];
            if (connObj == null || connObj == "{}" || jQuery.isEmptyObject(connObj)) {
                showMesg("Please Select Connection")
                throw new Error("Please Select Connection");
            }
            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
            var queryString = trfmRules['queryData'];
            if (queryString == null || queryString == "") {
                showMesg("Please provide query")
                throw new Error("Please provide query");
            }

        } else if (component == "Y" && iconType == "OUTPUT") {
            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
            var colMappingsData = trfmRules['colMappingsData'];
            validateColumnMappings(colMappingsData);

        } else if (component == "Y" && iconType == "STAGING") {
            var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
            var colMappingsData = trfmRules['colMappingsData'];
            validateColumnMappings(colMappingsData);

        } else if (component == "Y" && ( iconType == "SAPLOAD" ||  iconType == "SAPLOADSTANDARD" || iconType == "SAPLOADREVERSE" ) ) {
            var connectedFrom = getConnectedFromOpIds(selectedOperatorId)
            if (connectedFrom != null && connectedFrom.length == 1) {

                var fromOperatorData = $('#' + flowChartWorkSpaceId).flowchart('getOperatorData', connectedFrom[0]);
//                var fromConnObj = fromOperatorData['connObj'];
                if (fromOperatorData != null && fromOperatorData['connObj'] != null
                        && fromOperatorData['connObj']['CONN_CUST_COL1'] != null && ( fromOperatorData['connObj']['CONN_CUST_COL1'] == 'SAP_ECC' || fromOperatorData['connObj']['CONN_CUST_COL1'] == 'SAP_HANA' )) {
                } else {
                    showMesg("Please connect to single SAP table only")
                    throw new Error("Please connect to single SAP table only");
                }
            } else {
                showMesg("Please connect to single SAP table only")
                throw new Error("Please connect to single SAP table only");
            }
            var connectedTo = getConnectedToOpIds(selectedOperatorId)
            if (connectedTo != null) {
                var toOperatorData = $('#' + flowChartWorkSpaceId).flowchart('getOperatorData', connectedTo[0]);
//                 var toIconType = toOperatorData['iconType'];
                if (toOperatorData != null && toOperatorData['iconType'] == 'SQL' || (toOperatorData['dragType'] == 'Table' || ( toOperatorData['connObj']!=null && (toOperatorData['connObj']['CONN_CUST_COL1'] == 'SAP_ECC' || toOperatorData['connObj']['CONN_CUST_COL1'] == 'SAP_HANA') )) ) {

                } else {
                    showMesg("Please connect to DB Tables only")
                    throw new Error("Please connect to DB Tables only");
                }
            }
        } else if (component == "Y" && iconType == "SAPRESUME") {
            var connectedFrom = getConnectedFromOpIds(selectedOperatorId)
            if (connectedFrom != null && connectedFrom.length == 1) {

                var fromOperatorData = $('#' + flowChartWorkSpaceId).flowchart('getOperatorData', connectedFrom[0]);
//                var fromConnObj = fromOperatorData['connObj'];
                if (fromOperatorData != null && fromOperatorData['connObj'] != null
                        && fromOperatorData['connObj']['CONN_CUST_COL1'] != null && ( fromOperatorData['connObj']['CONN_CUST_COL1'] == 'SAP_ECC' || fromOperatorData['connObj']['CONN_CUST_COL1'] == 'SAP_HANA' )) {
                } else {
                    showMesg("Please connect to single SAP table only")
                    throw new Error("Please connect to single SAP table only");
                }
            } else {
                showMesg("Please connect to single SAP table only")
                throw new Error("Please connect to single SAP table only");

            }
            var connectedTo = getConnectedToOpIds(selectedOperatorId)
            if (connectedTo != null) {
                var toOperatorData = $('#' + flowChartWorkSpaceId).flowchart('getOperatorData', connectedTo[0]);
//                 var toIconType = toOperatorData['iconType'];
                if (toOperatorData != null && (toOperatorData['iconType'] == 'SQL' || toOperatorData['dragType'] == 'Table')) {

                } else {
                    showMesg("Please connect to DB Tables only")
                    throw new Error("Please connect to DB Tables only");
                }
            }
        } else if (component == "Y" && iconType == "SAPJOIN") {
            var connectedFrom = getConnectedFromOpIds(selectedOperatorId)
            if (connectedFrom != null) {

                var fromOperatorData = $('#' + flowChartWorkSpaceId).flowchart('getOperatorData', connectedFrom[0]);
//                var fromConnObj = fromOperatorData['connObj'];
                if (fromOperatorData != null && fromOperatorData['connObj'] != null
                        && fromOperatorData['connObj']['CONN_CUST_COL1'] != null && ( fromOperatorData['connObj']['CONN_CUST_COL1'] == 'SAP_ECC' || fromOperatorData['connObj']['CONN_CUST_COL1'] == 'SAP_HANA' )) {
                } else {
                    showMesg("Please connect to  SAP table only")
                    throw new Error("Please connect to SAP table only");
                }
            }
            var connectedTo = getConnectedToOpIds(selectedOperatorId)
            if (connectedTo != null) {
                var toOperatorData = $('#' + flowChartWorkSpaceId).flowchart('getOperatorData', connectedTo[0]);
//                 var toIconType = toOperatorData['iconType'];
                if (toOperatorData != null && (toOperatorData['iconType'] == 'SQL' || toOperatorData['dragType'] == 'Table')) {

                } else {
                    showMesg("Please connect to DB Tables only")
                    throw new Error("Please connect to DB Tables only");
                }
            }
        } else if (component != "Y" && connectedFrom != null && connectedFrom.length > 0) {

			var fromOperatorData = $('#' + flowChartWorkSpaceId).flowchart('getOperatorData', connectedFrom[0]);
			if (fromOperatorData['iconType'] == 'QUERY' ) {
				var fromOptrfmRules = operatorData['trfmRules'];
				fromOpqueryString = fromOptrfmRules['queryData'];
				if (fromOpqueryString!=null && fromOpqueryString.toUpperCase().indexOf("SELECT") == 0)  {
					var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
		            var colMappingsData = trfmRules['colMappingsData'];
		            validateColumnMappings(colMappingsData);
				} else {

				}
			} else if (fromOperatorData['iconType'] == 'GROUP_JOB') {

			} else {

//                            var connectedFrom = getConnectedFromOpIds(selectedOperatorId)
//                            if (connectedFrom != null && connectedFrom.length == 1) {
//
//                                var fromOperatorData = $('#' + flowChartWorkSpaceId).flowchart('getOperatorData', connectedFrom[0]);
//                                if (fromOperatorData['iconType'] != 'MERGE') {
//
//                                     showMesg("Transformation Rules does not apply")
//                                     throw new Error("Transformation Rules does not apply");
////                                    var trfmRules = (operatorData['trfmRules'] != null) ? operatorData['trfmRules'] : {};
////                                    var colMappingsData = trfmRules['colMappingsData'];
////                                    validateColumnMappings(colMappingsData);
//
//                                }
//                            }

			}

        }

    })

}

function validateColumnMappings(colMappingsData) {
    if (colMappingsData == null || colMappingsData.length == 0) {
        showMesg("Please Select Column Mappings ");
        throw new Error("Please Select Column Mappings");
    }
    $.each(colMappingsData, function (i) {
        var rowData = colMappingsData[i];
        var destinationColumnVal = rowData['destinationColumn'];
        if (destinationColumnVal == null || destinationColumnVal == "") {
            showMesg("Please Select destination Column ");
            throw new Error("Please Select destination Column");
        }
        var sourceColumnActualValue = rowData['sourceColumnActualValue'];
        var defaultValue = rowData['defaultValue'];
        var columnClauseActualValue = rowData['columnClauseActualValue'];
        if ((sourceColumnActualValue == null || sourceColumnActualValue == "")
                && (defaultValue == null || defaultValue == "")
                && (columnClauseActualValue == null || columnClauseActualValue == "")) {
            showMesg("Please Select source Column");
            throw new Error("Please Select source Column");
        }

    })
}

function executeProcessGroupJob() {
    showLoader();
    try {
        var flowChartData = $('#' + flowChartWorkSpaceId).flowchart('getData');
    } catch (e) {
    }
    var jobId = $('#' + flowChartWorkSpaceId).attr("jobId");
    openLogFile();
    processStepsInterval = setInterval(function() {
			// this will run after every 1 seconds
				refreshOperatorProcessStatus(jobId);
			}, 2000);
//    openETLProgressBar();
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        cache: false,
        url: "ProcessGroupJobData",
        async: true,
        data: {
            flowchartData: JSON.stringify(flowChartData),
            jobId: jobId
        },
        success: function (response) {
            stopLoader();
            try {
//                response = JSON.parse(response);
//                var jobId = response["jobId"];
//                $("#currentJobId").val(jobId);
            } catch (e) {
            }
        }

    });



}


function openETLProgressBar() {
    var flowChartData = $("#" + flowChartWorkSpaceId).flowchart("getData");
    var operators = flowChartData['operators'];

    var keys = Object.keys(operators);
    var count = keys.length;
    $("#bar_container").remove();
    var processBarBody = '<div class="bar_container" style="display:flex;">';
    var keyIndex = 0;
    for (var i in keys) {
        keyIndex++;
        if (keyIndex < count) {

            processBarBody += '<div id="main_container' + i + '">'
                    + '<div id="pbar' + i + '" class="progress-pie-chart" data-percent="0">'
                    + '<div class="ppc-progress">'
                    + '<div id="ppcprogressfill' + i + '" class="ppc-progress-fill"></div>'
                    + '</div>'
                    + '<div class="ppc-percents">'
                    + '<div class="pcc-percents-wrapper"><span id ="ppcpercentsspan' + i + '" >%</span></div>'
                    + '</div>'
                    + '</div>'
                    + '<progress style="display: none" id="progress_bar' + i + '" value="0" max="100"></progress>'

                    + '</div>'
                    + '<div class="progress-pie-chart-middle" >'
                    + '</div>';
        } else {
            processBarBody += '<div id="main_container' + i + '">'
                    + '<div id="pbar' + i + '" class="progress-pie-chart" data-percent="0">'
                    + '<div class="ppc-progress">'
                    + '<div id="ppcprogressfill' + i + '" class="ppc-progress-fill"></div>'
                    + '</div>'
                    + '<div class="ppc-percents">'
                    + '<div class="pcc-percents-wrapper"><span id ="ppcpercentsspan' + i + '" >%</span></div>'
                    + '</div>'
                    + '</div>'
                    + '<progress style="display: none" id="progress_bar' + i + '" value="0" max="100"></progress>'
                    + '</div>';
        }
    }
    processBarBody += '<input type="hidden" id="currentProcessBarIndex"/>'
            + '<input type="hidden" id="previousProcessBarIndex"/>'
            + '</div>';

//    var body = '<div id="myProgress"><div id="myBar"></div></div>';
    var modalObj = {
        title: 'Processing...',
        body: processBarBody
    };
    var buttonArray = [
        {
        }
    ];

    modalObj['buttons'] = buttonArray;
    createModal("showExtendPdfTableData", modalObj);
    $(".modal-dialog").addClass("modal-xs");
    $(".modal-dialog").css("height", "350");
    $(".modal-content").css("height", "350");
    $('.modal-dialog').addClass('growETLProcessBarModel');
    var max = 100;
    var responseTime = 30;
    var intervel = (1000 / max) * responseTime;
    var value = 0;
    var currentStepOp;
    var loading = function () {
        var jobId = $("#currentJobId").val();
        var currentProcessBarIndex = $("#currentProcessBarIndex").val();
        showLoader();
        $.ajax({
            type: 'post',
            traditional: true,
            dataType: 'html',
            cache: false,
            url: 'etlProgressBarInfo',
            async: true,
            data: {
                currentProcessBarIndex: ((currentProcessBarIndex != null && currentProcessBarIndex != '') ? currentProcessBarIndex : ""),
                jobId: jobId
            },
            success: function (response) {
                stopLoader();
                if (response != null && response != '') {
                    var resultObj = JSON.parse(response);
                    if (resultObj != null && !jQuery.isEmptyObject(resultObj)) {
                        var processFlag = resultObj['processFlag'];
                        if (processFlag != null && processFlag == 'N') {
                            console.error("process complete :: " + processFlag)
                            clearInterval(animate);
                        } else {
                            var stepFlag = resultObj['stepFlag'];
                            var i = resultObj['stepOperatorId'];
                            if (stepFlag == "N") {

                                currentStepOp = i;
                            }


                            var responseTime = resultObj['stepCompletionTime'] != null ? parseInt(resultObj['stepCompletionTime']) : 20;

                            console.error("process running :: " + processFlag)

//                                showRecordCountOnOp(resultObj);
                            $("#previousProcessBarIndex").val($("#currentProcessBarIndex").val());
                            $("#currentProcessBarIndex").val(resultObj['currentProcessBarIndex']);

                            if ($("#currentProcessBarIndex").val() != $("#previousProcessBarIndex").val() && stepFlag == "Y") {
                                value = max;

                                var progressbar = $('#progress_bar' + currentStepOp);
                                var addValue = progressbar.val(value);
                                var $ppc = $('#pbar' + currentStepOp),
                                        deg = 360 * value / 100;
                                if (value > 50) {
                                    $ppc.addClass('gt-50');
                                }
                                $('#ppcprogressfill' + currentStepOp).css('transform', 'rotate(' + deg + 'deg)');
                                $('#ppcpercentsspan' + currentStepOp).html(value + '%');
                                if (processFlag == 'Y') {
                                    value = 0;
                                }

                            } else {
                                if (value < 100) {
                                    value += 1;

                                    var progressbar = $('#progress_bar' + currentStepOp);
                                    var addValue = progressbar.val(value);
                                    var $ppc = $('#pbar' + currentStepOp),
                                            deg = 360 * value / 100;
                                    if (value > 50) {
                                        $ppc.addClass('gt-50');
                                    }
                                    $('#ppcprogressfill' + currentStepOp).css('transform', 'rotate(' + deg + 'deg)');
                                    $('#ppcpercentsspan' + currentStepOp).html(value + '%');
                                }
                            }
                        }
                    } else {
                        if (currentStepOp != null) {

                            if (value < 100) {
                                value += 1;

                                var progressbar = $('#progress_bar' + currentStepOp);
                                var addValue = progressbar.val(value);
                                var $ppc = $('#pbar' + currentStepOp),
                                        deg = 360 * value / 100;
                                if (value > 50) {
                                    $ppc.addClass('gt-50');
                                }
                                $('#ppcprogressfill' + currentStepOp).css('transform', 'rotate(' + deg + 'deg)');
                                $('#ppcpercentsspan' + currentStepOp).html(value + '%');
                            }

                        }


                    }
                }
            },
            error: function (e)
            {
                sessionTimeout(e);
                stopLoader();

            }

        });
    }
    var animate = setInterval(function () {
        loading();
    }, intervel);

}


function sapJoinsTrfmRules() {
      saveTrfmRulesToOp();
    var $flowchart = $('#' + flowChartWorkSpaceId);
    var selectedOperatorId = $flowchart.flowchart('getSelectedOperatorId');
    try {
        selectedOperatorId = parseInt(selectedOperatorId);
    } catch (e) {
    }
    $("#currentTrnsOpId").val(selectedOperatorId);
    var selectedOperatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", selectedOperatorId);

//    var connectedFrom = selectedOperatorData['connectedFrom'];
    var connectedFrom = getConnectedFromOpIds(selectedOperatorId)

    var sourceOperators = [];
    $.each(connectedFrom, function (i) {
        var operatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", connectedFrom[i]);
        sourceOperators.push(operatorData);
    })


    var flowchartData = $flowchart.flowchart('getData');
    showLoader();
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'json',
        cache: false,
        url: 'joinComponentTrfmRules',
        async: true,
        data: {
            selectedOperatorId: selectedOperatorId,
            flowchartData: JSON.stringify(flowchartData),
            sourceOperators: JSON.stringify(sourceOperators)

        },
        success: function (response) {
            stopLoader();
            if (response != null) {
                var simpleColumnsList = response['simpleColumnsList'];

                selectedOperatorData['simpleColumnsList'] = simpleColumnsList;
                $("#" + flowChartWorkSpaceId).flowchart("setOperatorData", selectedOperatorId, selectedOperatorData);

                var colMappingStr = response['colMappingStr'];
                $("#dataMigrationTabs").remove();
                var tabsDiv = "<div id='dataMigrationTabs' class='dataMigrationTabs'>"
                        + "<ul class='dataMigrationTabsHeader'>"
//                        + "<li class='dataMigrationTabsli'><a href='#tabs-1'>Mapping</a></li>"
                        + "<li class='dataMigrationTabsli'>Joins</li>"
                        + "</ul>"
//                        + "<div id='tabs-1' class='dataMigrationsTabsInner'>"
////                        + colMappingStr
//                        + " </div>"
                        + "<div id='tabs-2' class='dataMigrationsTabsInner'>"

                        + " </div>"
                        + " </div>";

                $("#feedContentArea").html(tabsDiv);
                $("#dataMigrationTabs").jqxTabs({theme: 'ui-redmond'});
				//setTabStyle('dataMigrationTabs');
//                $("#toTableColsArray_hidden").remove();
//                $("#fromTableColsArray_hidden").remove();
//                var hiddenData = "<input type='hidden' id='toTableColsArray_hidden'/><input type='hidden' id='fromTableColsArray_hidden'/>";
//                $('#tabs-1').append(colMappingStr);
//                $('#tabs-1').append(hiddenData);

//                $("#toTableColsArray_hidden").val(JSON.stringify(response['toTableColsArray']));
//                $("#fromTableColsArray_hidden").val(JSON.stringify(response['fromTableColsArray']));
//                $('#tabs-1').append("<div id='selectedColumnStr' style='display:none'></div>");
//                $("#selectedColumnStr").html(response['selectedColumnStr']);//colMapTrString

                $("#sourceTablesArray").remove();
                $("#destinationTablesArray").remove();

                var hiddenData = "<input type='hidden' id='sourceTablesArray'/><input type='hidden' id='destinationTablesArray'/>";
                $('#tabs-1').append(hiddenData);
                $("#sourceTablesArray").val(JSON.stringify(response['sourceTablesArray']));
                $("#destinationTablesArray").val(JSON.stringify(response['destinationTablesArray']));

                var joinTableString = response['joinTableString'];

                $("#tabs-2").append(joinTableString);

                $('#dataMigrationTabs').unbind('selected').on('selected', function (event) {
//                    $("#mainSplitter").resize();
                    $(".pcoded-main-container").css("margin-top", "127px");
                });
                $("#contentSplitter").resize();
            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });
    prevTargetOperatorId = selectedOperatorId
}


function pivotComponentTrfmRules() {
    saveTrfmRulesToOp();
    var $flowchart = $('#' + flowChartWorkSpaceId);
    var selectedOperatorId = $flowchart.flowchart('getSelectedOperatorId');
    try {
        selectedOperatorId = parseInt(selectedOperatorId);
    } catch (e) {
    }
    $("#currentTrnsOpId").val(selectedOperatorId);
    var selectedOperatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", selectedOperatorId);
//    var connectedFrom = getConnectedFromOpIds(selectedOperatorId);
    var flowchartData = $flowchart.flowchart('getData');
    showLoader();
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'json',
        cache: false,
        url: 'pivotComponentTrfmRules',
        async: true,
        data: {
            selectedOperatorId: selectedOperatorId,
            flowchartData: JSON.stringify(flowchartData)
        },
        success: function (response) {
            stopLoader();
            if (response != null) {
                var columnsList = response['columnsList'];
                var selectColumnsOptionsHtml = ""
                        + "<option>Select</option>";
                $.each(columnsList, function (i) {
                    selectColumnsOptionsHtml += "<option value='" + columnsList[i] + "'>" + columnsList[i] + "</option>";
                })
                selectColumnsOptionsHtml += "";

                var selectAggFunHtml = "<select id='pivotAggFun'>"
//                        + "<option>Select</option>"
                        + "<option>Count Of</option>"
                        + "</select>";

                var divStr = "<table>"
                        + "<tr><td>Create Table Name</td><td><input id='createTableName' value='' /></td></tr>"
                        + "<tr><td>Column Labels</td><td><select id='pivotColumnLabel'>" + selectColumnsOptionsHtml + "</select></td></tr>"
                        + "<tr><td>Values</td><td>" + selectAggFunHtml + "<select id='pivotColumnValue' >" + selectColumnsOptionsHtml + "</select></td></tr>"
                        + "<tr><td>Row Labels</td><td><select id='pivotRowLabel'>" + selectColumnsOptionsHtml + "</select></td></tr>"
                        + "<tr style='display:none;'><td>ColumnsList</td><td><input id='fromTableColumns' value='"+JSON.stringify(columnsList)+"' /></td></tr>"
                        + "</table>"
                $("#dataMigrationTabs").remove();
                var tabsDiv = "<div id='dataMigrationTabs' class='dataMigrationTabs'>"
                        + "<ul class='dataMigrationTabsHeader'>"
                        + "<li class='dataMigrationTabsli'>Pivot</li>"
                        + "</ul>"
                        + "<div id='tabs-1' class='dataMigrationsTabsInner'>"
                        + divStr
                        + " </div>";

                $("#feedContentArea").html(tabsDiv);
                $("#dataMigrationTabs").jqxTabs({theme: 'ui-redmond'});

				//setTabStyle('dataMigrationTabs');

                $("#contentSplitter").resize();

                var trfmRules = selectedOperatorData['trfmRules'];
                if (trfmRules != null) {
                    $("#createTableName").val(trfmRules['createTableName'] != null ? trfmRules['createTableName'] : "");
                    $("#pivotColumnLabel").val(trfmRules['pivotColumnLabel'] != null ? trfmRules['pivotColumnLabel'] : "");
                    $("#pivotColumnValue").val(trfmRules['pivotColumnValue'] != null ? trfmRules['pivotColumnValue'] : "");
                    $("#pivotRowLabel").val(trfmRules['pivotRowLabel'] != null ? trfmRules['pivotRowLabel'] : "");

                }


            }
        },
        error: function (e)
        {
            stopLoader();
            sessionTimeout(e);
        }

    });
    prevTargetOperatorId = selectedOperatorId
}

function unpivotComponentTrfmRules() {
    saveTrfmRulesToOp();
    var $flowchart = $('#' + flowChartWorkSpaceId);
    var selectedOperatorId = $flowchart.flowchart('getSelectedOperatorId');
    try {
        selectedOperatorId = parseInt(selectedOperatorId);
    } catch (e) {
    }
    $("#currentTrnsOpId").val(selectedOperatorId);
    var selectedOperatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", selectedOperatorId);
//    var connectedFrom = getConnectedFromOpIds(selectedOperatorId);
    var flowchartData = $flowchart.flowchart('getData');
    showLoader();
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'json',
        cache: false,
        url: 'unpivotComponentTrfmRules',
        async: true,
        data: {
            selectedOperatorId: selectedOperatorId,
            flowchartData: JSON.stringify(flowchartData)
        },
        success: function (response) {
            stopLoader();
            if (response != null) {
                var columnsList = response['columnsList'];

                var divStr = "<table>"
                        + "<tr><td>Create Table Name</td><td><input id='createTableName' value='' /></td></tr>"
                        + "<tr><td>Unpivot Columns Label</td><td><input id='unpivotColumnsLabel' value='' /></td></td></tr>"
                        + "<tr><td>Unpivot Values Label</td><td><input id='unpivotValuesLabel' value='' /></td></tr>"
                        + "<tr><td>Select Unpivot Columns</td><td><div id='unpivotColumnsComboBox'></div></td></tr>"
                        + "<tr style='display:none;'><td>ColumnsList</td><td><input id='fromTableColumns' value='"+JSON.stringify(columnsList)+"' /></td></tr>"
                        + "</table>"
                $("#dataMigrationTabs").remove();
                var tabsDiv = "<div id='dataMigrationTabs' class='dataMigrationTabs'>"
                        + "<ul class='dataMigrationTabsHeader'>"
                        + "<li class='dataMigrationTabsli'>Pivot</li>"
                        + "</ul>"
                        + "<div id='tabs-1' class='dataMigrationsTabsInner'>"
                        + divStr
                        + " </div>";

                $("#feedContentArea").html(tabsDiv);
                $("#dataMigrationTabs").jqxTabs({theme: 'ui-redmond'});
				//setTabStyle('dataMigrationTabs');
                $("#contentSplitter").resize();

                $("#unpivotColumnsComboBox").jqxComboBox({source: columnsList, multiSelect: true, width: 300, height: 20, });

                var trfmRules = selectedOperatorData['trfmRules'];
                if (trfmRules != null) {
                    $("#createTableName").val(trfmRules['createTableName'] != null ? trfmRules['createTableName'] : "");
                    $("#unpivotColumnsLabel").val(trfmRules['unpivotColumnsLabel'] != null ? trfmRules['unpivotColumnsLabel'] : "");
                    $("#unpivotValuesLabel").val(trfmRules['unpivotValuesLabel'] != null ? trfmRules['unpivotValuesLabel'] : "");
                    var selectedItems = trfmRules['unpivotColumns'] != null ? trfmRules['unpivotColumns'] : [];
                    $.each(selectedItems, function (i) {
                        $("#unpivotColumnsComboBox").jqxComboBox('selectItem', selectedItems[i]);
                    })
                }
            }
        },
        error: function (e)
        {
            stopLoader();
            sessionTimeout(e);
        }

    });
    prevTargetOperatorId = selectedOperatorId
}


function classifySubJobs(){
		var flowChartData = $('#' + flowChartWorkSpaceId).flowchart('getData');

		$.each(flowChartData['operators'], function(key, operator) {
			operator['subJobId'] = null;
		})

		$.each(flowChartData['operators'], function(key, operator) {

			var toOperatorsArray = $('#' + flowChartWorkSpaceId).flowchart('getAllToOperatorsByFromOpId', operator.operatorId);
			var fromOperatorsArray = $('#' + flowChartWorkSpaceId).flowchart('getAllFromOperatorsByToOpId', operator.operatorId);

			if (toOperatorsArray.length ==0) {
				operator['targetOperator'] = "Y";
				var subJobId = operator['subJobId'];
				if (subJobId == null) {
					subJobId = genHexString(32);
				}

				operator['subJobId'] = subJobId;
				$('#' + flowChartWorkSpaceId).flowchart('setOperatorData', parseInt(operator['operatorId']), operator);

				var allConnectedFromOperators = $('#' + flowChartWorkSpaceId).flowchart('getAllConnFromOperatorsByOpId', operator.operatorId, []);


				$.each(allConnectedFromOperators, function(i){
					this['subJobId'] = subJobId;
					$('#' + flowChartWorkSpaceId).flowchart('setOperatorData', parseInt(this['operatorId']), this);
				})
			}
			if (fromOperatorsArray.length == 0 ) {
				operator['sourceOperator'] = "Y";
				$('#' + flowChartWorkSpaceId).flowchart('setOperatorData', parseInt(operator['operatorId']), operator);
			}
			/*$('#' + flowChartWorkSpaceId).find('.flowchart-default-operator').each(function() {
				if ($(this).find('.flowchart-operator-outputs').length = 0) {
					$(this).find('.flowchart-operator-outputs').remove();
				}
				if ($(this).find('.flowchart-operator-inputs').length = 0) {
					$(this).find('.flowchart-operator-inputs').remove();
				}
			});*/
		})
}

function refreshOperatorProcessStatus(jobId) {
	//var jobId = $("#currentJobId").val();
	showLoader();

	$.ajax({
		type: 'post',
		traditional: true,
		dataType: 'html',
		cache: false,
		url: 'refreshOperatorProcessStatus',
		async: true,
		data: {
			jobId: jobId
		},
		success: function(response) {
                    stopLoader();
			if (response != null && response != '') {
				var resultObj = JSON.parse(response);
				if (resultObj != null && !jQuery.isEmptyObject(resultObj)) {

					var processingOperators = resultObj['processingOperators'];
					var processedOperators = resultObj['processedOperators'];
					var failedOperators = resultObj['failedOperators'];
					if (processingOperators != null && processingOperators.length > 0) {
						//setStatusIconOnOperator(processingOperators);

						try {
							setProcessIconOnOperator(processingOperators, processedOperators, failedOperators, jobId);
						} catch(e){
						}

					}

					if (resultObj['processFlag'] == 'N') {
						clearInterval(processStepsInterval);
						$(".etlProcessComponentLoader").remove();
					}
				}
			}
		},
		error: function(e) {
			sessionTimeout(e);
			stopLoader();
			if (processStepsInterval != null) {
				clearInterval(processStepsInterval);
			}
		}

	});
}

function setProcessIconOnOperator(processingOperators, processedOperators, failedOperators, jobId) {
	//$(".etlProcessComponentLoader").remove();
	$.each(processingOperators, function(i) {
		var operatorIdStr = processingOperators[i];
		var operatorId = parseInt(operatorIdStr);
		var operatorData = $('#workSpace_' +jobId).flowchart('getOperatorData', operatorId);
		var body = operatorData['properties']['body'];
		var ele = document.createElement('div');
		ele.innerHTML = body;
		if ($(ele).find(".etlProcessComponentLoader").length > 0) {

		} else {
			$(ele).append('<div class="etlProcessComponentLoader"></div>');
			operatorData['properties']['body'] = $(ele).html();
			$('#workSpace_' +jobId).flowchart('setOperatorData', operatorId, operatorData);
		}

	})

	$.each(processedOperators, function(i) {
		var operatorIdStr = processedOperators[i];
		var operatorId = parseInt(operatorIdStr);
		var operatorData = $('#workSpace_' +jobId).flowchart('getOperatorData', operatorId);
		var body = operatorData['properties']['body'];
		var ele = document.createElement('div');
		ele.innerHTML = body;
		if ($(ele).find(".etlProcessComponentLoader").length > 0) {
			$(ele).find(".etlProcessComponentLoader").remove();

		}
		if ($(ele).find(".operatorProcessStatus").length > 0) {

		} else {
			$(ele).append("<div class='operatorProcessStatus' ><img src='images/etl/etl_check_icon.png' /></div>");
		}
		operatorData['properties']['body'] = $(ele).html();
		$('#workSpace_' +jobId).flowchart('setOperatorData', operatorId, operatorData);
	})

	$.each(failedOperators, function(i) {
		var operatorIdStr = failedOperators[i];
		var operatorId = parseInt(operatorIdStr);
		var operatorData = $('#workSpace_' +jobId).flowchart('getOperatorData', operatorId);
		var body = operatorData['properties']['body'];
		var ele = document.createElement('div');
		ele.innerHTML = body;
		if ($(ele).find(".etlProcessComponentLoader").length > 0) {
			$(ele).find(".etlProcessComponentLoader").remove();

		}
		if ($(ele).find(".operatorProcessStatus").length > 0) {

		} else {
			$(ele).append("<div class='operatorProcessStatus' ><img src='images/etl/etl_fail_icon.png' /></div>");
		}
		operatorData['properties']['body'] = $(ele).html();
		$('#workSpace_' +jobId).flowchart('setOperatorData', operatorId, operatorData);
	})
}

function validateMergeComponentTrfmRules(){
	var flowchartData = $("#"+flowChartWorkSpaceId).flowchart('getData');
        showLoader();
	$.ajax({
		type: 'post',
		traditional: true,
		dataType: 'json',
		cache: false,
		url: 'validateMergeComponentTrfmRules',
		async: true,
		data: {
			flowchartData: JSON.stringify(flowchartData)
		},
		success: function(response) {
                    stopLoader();
			var responseMessage = response['message'];
			if (responseMessage == "Mismatch") {
				showMesg("Columns Mismatched")
        		throw new Error("Columns Mismatched");
			}

		},
		error: function(e) {

		}

	});
}

function setTabStyle(tabId) {
	setTimeout(function(){
		var width = $("#"+tabId).find("li.jqx-tabs-title-selected-top").width()
		$("#"+tabId).find("li").find("style").remove();
		$("#"+tabId).find("li.jqx-tabs-title-selected-top").append('<style>.etl-page-body #'+tabId+' .jqx-tabs-title-selected-top::after{margin-left:'+(width+8)+'px !important;}</style>');

	},300);

	$('#'+tabId).unbind('selected').on('selected', function(event) {
		var width = $("#"+tabId).find("li.jqx-tabs-title-selected-top").width()
		$("#"+tabId).find("li").find("style").remove();
		$("#"+tabId).find("li.jqx-tabs-title-selected-top").append('<style>.etl-page-body #'+tabId+' .jqx-tabs-title-selected-top::after{margin-left:'+(width+8)+'px !important;}</style>');

	})

	$('#'+tabId).unbind('add').on('add', function(event) {
		var width = $("#"+tabId).find("li.jqx-tabs-title-selected-top").width()
		$("#"+tabId).find("li").find("style").remove();
		$("#"+tabId).find("li.jqx-tabs-title-selected-top").append('<style>.etl-page-body #'+tabId+' .jqx-tabs-title-selected-top::after{margin-left:'+(width+8)+'px !important;}</style>');

	})
}

function apiCompnentTrfmRules() {

    saveTrfmRulesToOp();
    var $flowchart = $('#' + flowChartWorkSpaceId);
    var selectedOperatorId = $flowchart.flowchart('getSelectedOperatorId');
    try {
        selectedOperatorId = parseInt(selectedOperatorId);
    } catch (e) {
    }
    $("#currentTrnsOpId").val(selectedOperatorId);

    var selectedOperatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", selectedOperatorId);

    var fromOperatorsArray = getConnectedFromOpIds(selectedOperatorId)
    var fromOperatorId = fromOperatorsArray[0];
    var fromOperator = $flowchart.flowchart('getOperatorData', fromOperatorId);
    var tableName = fromOperator['statusLabel'];
    var connObj = fromOperator['connObj'];

    var flowchartData = $flowchart.flowchart('getData');
    showLoader();
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'json',
        cache: false,
        url: 'apiCompnentTrfmRules',
        async: true,
        data: {
            tableName: tableName,
            connObj: JSON.stringify(connObj),
            selectedOperatorId: selectedOperatorId,
            flowchartData: JSON.stringify(flowchartData)

        },
        success: function (response) {
            stopLoader();
            if (response != null) {

                var simpleColumnsList = response['simpleColumnsList'];

                selectedOperatorData['simpleColumnsList'] = simpleColumnsList;

                selectedOperatorData['tableName'] = "ZZ_TEMP_" + selectedOperatorData['timeStamp'];
				selectedOperatorData['statusLabel'] = selectedOperatorData['tableName'];
				selectedOperatorData['tableNameLabel'] = selectedOperatorData['iconType'] + "_OUTPUT_" + selectedOperatorData['operatorId'];
				if (selectedOperatorData['connObj'] == null) {
					selectedOperatorData['connObj'] = response["currentConnObj"];
				}

                $("#" + flowChartWorkSpaceId).flowchart("setOperatorData", selectedOperatorId, selectedOperatorData);

                var result = response['result'];
                $("#dataMigrationTabs").remove();

                var tabsDiv = "<div id='dataMigrationTabs' class='dataMigrationTabs'><ul class='dataMigrationTabsHeader'>"
                        + "<li class='dataMigrationTabsli'><img src ='images/etl/ETL_Api_Icon.png' style='width:15px;'/> API Details</li>"
                        + "</ul>"
                        + "<div id='tabs-1' class='dataMigrationsTabsInner visionMergeCols' >"
                        + result
                        + " </div>";

                $("#feedContentArea").html(tabsDiv);
                $("#dataMigrationTabs").jqxTabs({theme: 'ui-redmond'});
				setTabStyle('dataMigrationTabs');

             /*  $("#fromTableColsArray_hidden").remove();
                var fromTableColsArray = "<input type='hidden' id='fromTableColsArray_hidden'/>";

                $('#tabs-1').append(fromTableColsArray);
                $("#fromTableColsArray_hidden").val(JSON.stringify(response['fromTableColsArray']));
               */

				$("#addApiParameter").click(function(event){
					var trow = "<tr><td width='50%' ><input style='width:100%;' class='apiParameterKey' type='text' value='' /></td><td width='50%' ><input style='width:100%;' class='apiParameterVal' type='text' value='' /></td></tr>";
					$("#apiParametersTable").find("tbody").append(trow);
				})

            }
        },
        error: function (e)
        {
            sessionTimeout(e);
            stopLoader();
        }

    });
    prevTargetOperatorId = selectedOperatorId

}

function getAPIColumns(operatorData){
	var simpleColumnsList = [];
        showLoader();
	 $.ajax({
            type: 'post',
            traditional: true,
            dataType: 'json',
            cache: false,
            url: 'getAPIColumns',
            async: false,
            data: {
                operatorData: JSON.stringify(operatorData)
            },
        success: function (response) {
            stopLoader();
            if (response != null) {
                simpleColumnsList =  response['simpleColumnsList'];
            }
        },
        error: function (e)
        {
            sessionTimeout(e);
            stopLoader();
             return  [];
        }

    });
    return simpleColumnsList;
}


function fetchFolderData(divId,level,instance){

	var fileType = $("#etlSchemaExplorerInner_" + instance).attr("data-fileType");
	var connectionType = $("#etlSchemaExplorerInner_" + instance).attr("data-conntype");
	var schemaObjectType = $("#etlSchemaExplorerInner_" + instance).attr("data-schemaobjecttype");
	var connectionName = $("#etlSchemaExplorerInner_" + instance).attr("data-connname");
	var connectionObjStr = $("#etlSchemaExplorerInner_" + instance).attr("data-connobj");


		var filterValue = $("#etlSchemaExplorerSearchInput_" + instance).val();
		var startIndex = $("#etlSchemaExplorerStartIndex_" + instance).val();

		loadConnections(instance, divId, level, fileType, connectionType, schemaObjectType, connectionName, connectionObjStr, filterValue, startIndex);
	}


function validateTrfmRules() {
	saveTrfmRulesToOp();
    var $flowchart = $('#' + flowChartWorkSpaceId);
    var selectedOperatorId = $flowchart.flowchart('getSelectedOperatorId');
    try {
        selectedOperatorId = parseInt(selectedOperatorId);
    } catch (e) {
    }
    $("#currentTrnsOpId").val(selectedOperatorId);
    var url = 'validateComponentTrfmRules';

    var selectedOperatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", selectedOperatorId);
    var connectedFrom = getConnectedFromOpIds(selectedOperatorId)

    var sourceOperators = [];
    $.each(connectedFrom, function (i) {
        var operatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", connectedFrom[i]);
        sourceOperators.push(operatorData);
    })

    var flowchartData = $flowchart.flowchart('getData');
    showLoader();
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'json',
        cache: false,
        url: url,
        async: true,
        data: {
            selectedOperatorId: selectedOperatorId,
            flowchartData: JSON.stringify(flowchartData),
            sourceOperators: JSON.stringify(sourceOperators)
        },
        success: function (response) {
            stopLoader();
            if (response != null) {

                var simpleColumnsList = response['simpleColumnsList'];
                var selectedOperatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", selectedOperatorId);
                selectedOperatorData['simpleColumnsList'] = simpleColumnsList;
                $("#" + flowChartWorkSpaceId).flowchart("setOperatorData", selectedOperatorId, selectedOperatorData);

                var colMappingStr = response['colMappingStr'];
                $("#dataMigrationTabs").remove();
                var tabsDiv = "<div id='dataMigrationTabs' class='dataMigrationTabs'><ul class='dataMigrationTabsHeader'>"
                        + "<li class='dataMigrationTabsli'>Mapping</li>"
                        + "</ul>"
                        + "<div id='tabs-1' class='dataMigrationsTabsInner'>"
                        + colMappingStr
                        + " </div>";

                $("#feedContentArea").html(tabsDiv);
                $("#dataMigrationTabs").jqxTabs({theme: 'ui-redmond'});


					//setTabStyle('dataMigrationTabs');

                $("#toTableColsArray_hidden").remove();
                $("#fromTableColsArray_hidden").remove();
                var hiddenData = "<input type='hidden' id='toTableColsArray_hidden'/><input type='hidden' id='fromTableColsArray_hidden'/>";
                $('#tabs-1').append(hiddenData);
                $("#toTableColsArray_hidden").val(JSON.stringify(response['toTableColsArray']));
                $("#fromTableColsArray_hidden").val(JSON.stringify(response['fromTableColsArray']));
                $('#tabs-1').append("<div id='selectedColumnStr' style='display:none'></div>");
                $("#selectedColumnStr").html(response['selectedColumnStr']);//colMapTrString

                $("#sourceTablesArray").remove();
                $("#destinationTablesArray").remove();
                var hiddenData = "<input type='hidden' id='sourceTablesArray'/><input type='hidden' id='destinationTablesArray'/>";
                $('#tabs-1').append(hiddenData);
                $("#sourceTablesArray").val(JSON.stringify(response['sourceTablesArray']));
                $("#destinationTablesArray").val(JSON.stringify(response['destinationTablesArray']));


            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });
}

function fetchTrfmRules() {
	saveTrfmRulesToOp();
    var $flowchart = $('#' + flowChartWorkSpaceId);
    var selectedOperatorId = $flowchart.flowchart('getSelectedOperatorId');
    try {
        selectedOperatorId = parseInt(selectedOperatorId);
    } catch (e) {
    }
    $("#currentTrnsOpId").val(selectedOperatorId);
    var url = 'fetchComponentTrfmRules';

    var selectedOperatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", selectedOperatorId);
    var connectedFrom = getConnectedFromOpIds(selectedOperatorId)

    var sourceOperators = [];
    $.each(connectedFrom, function (i) {
        var operatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", connectedFrom[i]);
        sourceOperators.push(operatorData);
    })

    var flowchartData = $flowchart.flowchart('getData');
    showLoader();
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'json',
        cache: false,
        url: url,
        async: true,
        data: {
            selectedOperatorId: selectedOperatorId,
            flowchartData: JSON.stringify(flowchartData),
            sourceOperators: JSON.stringify(sourceOperators)
        },
        success: function (response) {
            stopLoader();
            if (response != null) {

                var simpleColumnsList = response['simpleColumnsList'];
                var selectedOperatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", selectedOperatorId);
                selectedOperatorData['simpleColumnsList'] = simpleColumnsList;
                $("#" + flowChartWorkSpaceId).flowchart("setOperatorData", selectedOperatorId, selectedOperatorData);

                var colMappingStr = response['colMappingStr'];
                $("#dataMigrationTabs").remove();
                var tabsDiv = "<div id='dataMigrationTabs' class='dataMigrationTabs'><ul class='dataMigrationTabsHeader'>"
                        + "<li class='dataMigrationTabsli'>Mapping</li>"
                        + "</ul>"
                        + "<div id='tabs-1' class='dataMigrationsTabsInner'>"
                        + colMappingStr
                        + " </div>";

                $("#feedContentArea").html(tabsDiv);
                $("#dataMigrationTabs").jqxTabs({theme: 'ui-redmond'});
                $("#fetchColumnsComboBox").jqxComboBox({source: simpleColumnsList, multiSelect: true, width: 300, height: 20, });
                var trfmRules = selectedOperatorData['trfmRules'];
                if (trfmRules != null) {
					var columnMappingData = trfmRules['colMappingsData'];
					for(var i=0;i<columnMappingData.length;i++){
						var selectedItems = columnMappingData[i]['sourceColumns'] != null ? columnMappingData[i]['sourceColumns'] : [];
                    $.each(selectedItems, function (i) {
                        $("#fetchColumnsComboBox").jqxComboBox('selectItem', selectedItems[i]);
                    })
					}

                }

					//setTabStyle('dataMigrationTabs');

                $("#toTableColsArray_hidden").remove();
                $("#fromTableColsArray_hidden").remove();
                var hiddenData = "<input type='hidden' id='toTableColsArray_hidden'/><input type='hidden' id='fromTableColsArray_hidden'/>";
                $('#tabs-1').append(hiddenData);
                $("#toTableColsArray_hidden").val(JSON.stringify(response['toTableColsArray']));
                $("#fromTableColsArray_hidden").val(JSON.stringify(response['fromTableColsArray']));
                $('#tabs-1').append("<div id='selectedColumnStr' style='display:none'></div>");
                $("#selectedColumnStr").html(response['selectedColumnStr']);//colMapTrString

                $("#sourceTablesArray").remove();
                $("#destinationTablesArray").remove();
                var hiddenData = "<input type='hidden' id='sourceTablesArray'/><input type='hidden' id='destinationTablesArray'/>";
                $('#tabs-1').append(hiddenData);
                $("#sourceTablesArray").val(JSON.stringify(response['sourceTablesArray']));
                $("#destinationTablesArray").val(JSON.stringify(response['destinationTablesArray']));


            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });
}

function selectDataType($this) {
	var dataTypes = [
		"BFILE", "BLOB", "CHAR", "CLOB", "DATE", "FLOAT", "LONG", "LONG RAW",
		"LONGRAW", "MLSLABEL", "NCHAR", "NCLOB", "NUMBER", "NVARCHAR2", "RAW",
		"ROWID", "TIMESTAMP", "VARCHAR", "VARCHAR2"
	];

	$("#dialog").html("<div id='dataTypeList'></div>");
	$("#dialog").dialog({
		title: (labelObject['Data Types'] != null ? labelObject['Data Types'] : 'Data Types'),
		modal: true,
		height: 400,
		minWidth: 300,
		maxWidth: 'auto',
		fluid: true,
		buttons: [{
			text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
			click: function() {
				var item = $("#dataTypeList").jqxListBox('getCheckedItems');
				$($this).parents("td").find("input").val(item[0].value);
				$($this).parents("td").find("input").attr("title", item[0].value);
				$($this).parents("td").find("input").attr("actual-value", item[0].value);
				$(this).html("");
				$(this).dialog("destroy");

			}
		}],
		open: function() {
			
			$("#dataTypeList").jqxListBox({
				checkboxes: true,
				filterable: true,
				source: dataTypes,
				width: 280,
				height: 280
			});
			$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
			$(this).closest(".ui-dialog").find(".ui-button").eq(2).addClass("dialogno");
			$(".visionHeaderMain").css("z-index", "999");
			$(".visionFooterMain").css("z-index", "999");
		},
		beforeClose: function(event, ui) {
			$(".visionHeaderMain").css("z-index", "99999");
			$(".visionFooterMain").css("z-index", "99999");
		}
	});
}

function sapHanaLoadTrfmRules() {
	saveTrfmRulesToOp();
	    var $flowchart = $('#' + flowChartWorkSpaceId);
	    var selectedOperatorId = $flowchart.flowchart('getSelectedOperatorId');
	    try {
	        selectedOperatorId = parseInt(selectedOperatorId);
	    } catch (e) {
	    }
	    $("#currentTrnsOpId").val(selectedOperatorId);
	    var url = 'validateComponentTrfmRules';

	    var selectedOperatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", selectedOperatorId);
	    var connectedFrom = getConnectedFromOpIds(selectedOperatorId)

	    var sourceOperators = [];
	    $.each(connectedFrom, function (i) {
	        var operatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", connectedFrom[i]);
	        sourceOperators.push(operatorData);
	    })

	    var flowchartData = $flowchart.flowchart('getData');
	    showLoader();
	    $.ajax({
	        type: 'post',
	        traditional: true,
	        dataType: 'json',
	        cache: false,
	        url: url,
	        async: true,
	        data: {
	            selectedOperatorId: selectedOperatorId,
	            flowchartData: JSON.stringify(flowchartData),
	            sourceOperators: JSON.stringify(sourceOperators)
	        },
	        success: function (response) {
	            stopLoader();
	            if (response != null) {

	                var simpleColumnsList = response['simpleColumnsList'];
	                var selectedOperatorData = $("#" + flowChartWorkSpaceId).flowchart("getOperatorData", selectedOperatorId);
	                selectedOperatorData['simpleColumnsList'] = simpleColumnsList;
	                $("#" + flowChartWorkSpaceId).flowchart("setOperatorData", selectedOperatorId, selectedOperatorData);

	                var colMappingStr = response['colMappingStr'];
	                $("#dataMigrationTabs").remove();
	                var tabsDiv = "<div id='dataMigrationTabs' class='dataMigrationTabs'><ul class='dataMigrationTabsHeader'>"
	                        + "<li class='dataMigrationTabsli'>Mapping</li>"
	                        + "</ul>"
	                        + "<div id='tabs-1' class='dataMigrationsTabsInner'>"
	                        + colMappingStr
	                        + " </div>";

	                $("#feedContentArea").html(tabsDiv);
	                $("#dataMigrationTabs").jqxTabs({theme: 'ui-redmond'});


						//setTabStyle('dataMigrationTabs');

	                $("#toTableColsArray_hidden").remove();
	                $("#fromTableColsArray_hidden").remove();
	                var hiddenData = "<input type='hidden' id='toTableColsArray_hidden'/><input type='hidden' id='fromTableColsArray_hidden'/>";
	                $('#tabs-1').append(hiddenData);
	                $("#toTableColsArray_hidden").val(JSON.stringify(response['toTableColsArray']));
	                $("#fromTableColsArray_hidden").val(JSON.stringify(response['fromTableColsArray']));
	                $('#tabs-1').append("<div id='selectedColumnStr' style='display:none'></div>");
	                $("#selectedColumnStr").html(response['selectedColumnStr']);//colMapTrString

	                $("#sourceTablesArray").remove();
	                $("#destinationTablesArray").remove();
	                var hiddenData = "<input type='hidden' id='sourceTablesArray'/><input type='hidden' id='destinationTablesArray'/>";
	                $('#tabs-1').append(hiddenData);
	                $("#sourceTablesArray").val(JSON.stringify(response['sourceTablesArray']));
	                $("#destinationTablesArray").val(JSON.stringify(response['destinationTablesArray']));


	            }
	        },
	        error: function (e)
	        {
	            sessionTimeout(e);
	        }

	    });
}



