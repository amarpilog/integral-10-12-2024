/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


var axisColumnsObj = {};
var axisLabelsObj = {};
var multiSelectCubeLabelsObj = {};
var selectedCubesArrayObj = {};
var globalSceneObj = {};
var rotateFlagCW = false;
var rotateFlagAntiCW = false;
var cubeClickFlag = false;
var line;
var material;
var cubeClickValues = {};
var globalMaterial = {};
var divArray = [];
var globalCubeRenderer = {};
var globalCubeLabelRenderer = {};
var magnifyFlag = true;
var chartValueAsDollar = "";
//var scene,camera,labelRenderer,renderer;


var seqArray = [];
var seqObj = {};
var previousDiv;
var windowWidth = $(window).width();
var scrollStartIndexFlag = "N";
var chartFilterItems = {};
var selectedTitle = "";
var selectedTitleValue = "";
var nestedChartLevel = 0;
var cockpitViewErrorFlag = false;
var JQXImageContent = {};
var jsonChartImg = {};




//var firstClick = 0;
//var secondClick = 0;

var parentObj = {};
var scrollEventX;
//cube//
var line;
var MAX_POINTS = 500;
var drawCount;
var value = 1;
var delta = -0.01;

$(document).ready(function () {
    $("#cubeIcon3dDiv").css("display", "block");
    $("#cubeIcon3d").css("display", "block");
})
//cube//
function getTabsData(selectTabIndex, selectGridIndex)
{
    var gridIdStr = $("#gridIdStr").val();
    var compTypeStr = $("#compTypeStr").val();
    var tabNameStr = $("#tabNameStr").val();
    var tabNameArray = [];
    if (tabNameStr != null && tabNameStr != '' && tabNameStr != undefined) {
        tabNameArray = tabNameStr.split(",");
    }
    var gridIdArray = [];
    if (gridIdStr != null && gridIdStr != '' && gridIdStr != undefined) {
        gridIdArray = gridIdStr.split(",");
    }
    var tabId = tabNameArray[selectTabIndex];
    var selectedGridId = gridIdArray[selectGridIndex];
    var compTypeArray = compTypeStr.split(",");
    var selectedCompType = compTypeArray[selectGridIndex];
    if (selectedCompType == 'DIY_ANALYTICS') {
        getDIYAnalyticsData(tabId, selectedGridId);
    } else {
        chartsData(tabId, selectedGridId, "Y");
    }

}
function chartsData(tabId, selectedGridId, scrollFlagName)
{

    cockpitViewErrorFlag = false;
    seqArray = [];
    JQXImageContent = {};
    seqObj = {};
    seqObj["TAB_ID"] = tabId;
    $("#" + tabId).empty();
    $("#tabChartsId").val(tabId);
    $("#tabComponentId").val(selectedGridId);
    $("#" + tabId + "div").remove();
    try {
        $("#" + previousDiv).empty();
    } catch (e) {
    }
    previousDiv = tabId;
    $("#wait").css("display", "block");
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        cache: false,
        url: "dataAnalyticsCharts",
        data: {
            'tabComponentId': selectedGridId,
            'scrollFlagName': scrollFlagName
        },
        success: function (response) {
            if (response != null)
            {
                var result = JSON.parse(response);
                var dataObj = result['chartList'];
                var tabHeight = $("#tabHeight").val(); //cube mdrm
                if (tabHeight != null && tabHeight != '' && tabHeight != undefined)
                {
                    $('#jqxTabs').jqxTabs({height: tabHeight + "px"});
                }
                $(".visionReportandDashboard").css("height", "250px", "important");
                $("#startIndex").val(result['startIndex']);
                $("#endIndex").val(result['endIndex']);
                $("#pageSize").val(result['pageSize']);
                $("#totalCount").val(result['totalCount']);
                $("#scrollFlag").val(result['scrollFlag']);
                $("#visionAnalyticCheckBoxData").empty();
                $("#visionAnalyticCheckBoxData").html(result['checkBoxStr']);
                $("#" + tabId).empty();
                $("#" + tabId + " div").remove();
                if (dataObj != null) {
                    var jqwidgetFlag = dataObj[0]['36'];
                    if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Y')
                    {
                        getJqwidgetChart(dataObj[0], tabId, 0, dataObj);
                    } else if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Q')
                    {
                        getShowCube(dataObj[0], tabId, 0, dataObj);
                    } else
                    {
                        getChartByChart(dataObj[0], tabId, 0, dataObj);
                    }
//                    getChartByChart(dataObj[0], tabId,0,dataObj);
//                    for (var i = 0; i < dataObj.length; i++)
//                    {
//                        var chartData = dataObj[i];
//                        $("#wait").css("display", "block");
//                        getChartByChart(chartData, tabId);
//                    }
                }
            }
        },
        error: function (e) {
            sessionTimeout(e);
        }
    });
}

function getChartByChart(chartData, tabId, i, chartDataList)
{
    var dataApiObjStr = $("#dataApiObjStr").val();
    var paramsMapEncStr = $("#paramsMapStr").val();

    $("#wait").css("display", "block");
    var filterItem;
    if (chartFilterItems != null && !jQuery.isEmptyObject(chartFilterItems))
    {
        filterItem = JSON.parse(chartFilterItems);
    }
    var data = {
        'chartData': chartData,
        'chartFilterItems': JSON.stringify(filterItem),
        dataApiObjStr: dataApiObjStr
    };
    if (paramsMapEncStr != null && paramsMapEncStr != '') {
        var paramsMapwords = CryptoJS.enc.Base64.parse(paramsMapEncStr);
        var paramsMapStr = CryptoJS.enc.Utf8.stringify(paramsMapwords);
        var paramsMap = JSON.parse(paramsMapStr);
        console.log("paramsMap::" + paramsMap);
        if (paramsMap != null && !jQuery.isEmptyObject(paramsMap)) {
            for (var parmKey in paramsMap) {
                if (paramsMap[parmKey] != null) {
                    data[parmKey] = paramsMap[parmKey];
                }

            }
        }
    }
    i++;
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        url: "getAnalyticCharts",
        cache: false,
        data: data,
        success: function (response) {

            $("#wait").css("display", "none");
            if (response != null && response != '' && response != undefined) {
                try {
                    var resultData = JSON.parse(response);
                    if (resultData != null)
                    {
                        var result = resultData[0];
                        if (result != null) {
                            var ErrorMsg = result['ErrorMsg'];
                            var analysisType = result['analysisType'];
                            if (analysisType != null && analysisType != undefined && analysisType != "" && analysisType == 'ABC') {
                                $("#A_Pct").val(result['A_Pct']);
                                $("#ACount").val(result['ACount']);
                                $("#BCount").val(result['BCount']);
                                $("#CCount").val(result['CCount']);
                                $("#B_Pct").val(result['B_Pct']);
                                $("#C_Pct").val(result['C_Pct']);
                                $("#A_Value_TP").val(result['A_Value_TP']);
                                $("#B_Value_TP").val(result['B_Value_TP']);
                                $("#C_Value_TP").val(result['C_Value_TP']);
                                $("#Total_CPP_Pct").val(result['Total_CPP_Pct']);
                                $("#A_Value").val(result['A_Value']);
                                $("#B_Value").val(result['B_Value']);
                                $("#C_Value").val(result['C_Value']);
                            } else if (analysisType != null && analysisType != undefined && analysisType != "" && analysisType == 'XYZ') {
                                $("#X").val(result['X']);
                                $("#Y").val(result['Y']);
                                $("#Z").val(result['Z']);
                                $("#X_COUNT").val(result['X_COUNT(%)']);
                                $("#X_VALUE").val(result['X_VALUE(%)']);
                                $("#Y_VALUE").val(result['Y_VALUE(%)']);
                                $("#Z_VALUE").val(result['Z_VALUE(%)']);
                                $("#Y_COUNT").val(result['Y_COUNT(%)']);
                                $("#Z_COUNT").val(result['Z_COUNT(%)']);
                                $("#X_Total_Stock").val(result['X_Total_Stock(%)']);
                                $("#Y_Total_Stock").val(result['Y_Total_Stock(%)']);
                                $("#Z_Total_Stock").val(result['Z_Total_Stock(%)']);
                                $("#TOTAL_STOCK").val(result['TOTAL_STOCK(Total)']);
                            } else if (analysisType != null && analysisType != undefined && analysisType != "" && analysisType == 'FMSN') {
                                $("#F").val(result['F']);
                                $("#S").val(result['S']);
                                $("#N").val(result['N']);
                                $("#F_COUNT").val(result['F_COUNT(%)']);
                                $("#S_COUNT").val(result['S_COUNT(%)']);
                                $("#N_COUNT").val(result['N_COUNT(%)']);
                                $("#F_Total_Stock").val(result['F_Total_Stock(%)']);
                                $("#S_Total_Stock").val(result['S_Total_Stock(%)']);
                                $("#N_Total_Stock").val(result['N_Total_Stock(%)']);
                                $("#TOTAL_STOCK").val(result['TOTAL_STOCK(Total)']);
                            } else if (analysisType != null && analysisType != undefined && analysisType != "" && analysisType == 'HML') {
                                $("#H").val(result['H']);
                                $("#M").val(result['M']);
                                $("#L").val(result['L']);
                                $("#H_COUNT").val(result['H_COUNT(%)']);
                                $("#M_COUNT").val(result['M_COUNT(%)']);
                                $("#L_COUNT").val(result['L_COUNT(%)']);
                                $("#H_VALUE").val(result['H_VALUE(%)']);
                                $("#M_VALUE").val(result['M_VALUE(%)']);
                                $("#L_VALUE").val(result['L_VALUE(%)']);
                                $("#H_CUM_PCT").val(result['H_CUM_PCT(%)']);
                                $("#M_CUM_PCT").val(result['M_CUM_PCT(%)']);
                                $("#L_CUM_PCT").val(result['L_CUM_PCT(%)']);
                            } else if (analysisType != null && analysisType != undefined && analysisType != "" && analysisType == 'VED') {
                                $("#V").val(result['V']);
                                $("#E").val(result['E']);
                                $("#D").val(result['D']);
                                $("#V_COUNT").val(result['V_COUNT(%)']);
                                $("#E_COUNT").val(result['E_COUNT(%)']);
                                $("#D_COUNT").val(result['D_COUNT(%)']);
                            }
                            if (ErrorMsg != null && ErrorMsg != '' && ErrorMsg != undefined)
                            {
                                var dialogSplitMessage = dialogSplitIconText(ErrorMsg, "Y");
                                if (!cockpitViewErrorFlag) {
                                    showDialog(dialogSplitMessage);
                                    return;
                                } else if (cockpitViewErrorFlag)
                                {
                                    return;
                                }
                            }
                            var divid = result['divId'];
                            var seqNo = result['seqNo'];
                            var chartId = result['chartId'];
                            // var tabId = result['tabId'];
                            //-----------------------------
                            var insertDivBefore = 0;
                            for (var j = 0; j < seqArray.length; j++) {
                                if (parseInt(seqNo) < seqArray[j]) {
                                    insertDivBefore = seqArray[j];
                                    break;
                                }
                            }
                            if (insertDivBefore == 0) {
                                if (seqObj["TAB_ID"] == tabId) {
                                    if ($('#' + tabId + "_" + chartId + "_" + seqNo).length) {
                                    } else {
                                        $("#" + tabId).append("<div id='" + tabId + "_" + chartId + "_" + seqNo + "'>" + divid + "</div>");
                                        seqArray.push(parseInt(seqNo));
                                        seqObj[parseInt(seqNo)] = tabId + "_" + chartId + "_" + seqNo;
                                    }
                                }
                            } else {
                                if (seqObj["TAB_ID"] == tabId) {
                                    if ($('#' + tabId + "_" + chartId + "_" + seqNo).length) {
                                    } else {
                                        $("<div id='" + tabId + "_" + chartId + "_" + seqNo + "'>" + divid + "</div>").insertBefore($("#" + seqObj[insertDivBefore]));
                                        seqArray.push(parseInt(seqNo));
                                        seqObj[parseInt(seqNo)] = tabId + "_" + chartId + "_" + seqNo;
                                    }
                                }
                            }
                            seqArray = seqArray.sort(function (a, b) {
                                return a - b;
                            });
                            //-------------------
                            var dataArray = result['dataArray'];
                            var options = result['options'];
                            var objInit = result['objInit'];
                            var sizeOfChart = result['sizeOfChart'];
                            var description = result['description'];
                            var chartInitParamObj = result['chartInitParamObj'];
                            var labelChartArr = result['labelChartArr'];
                            var calenderFlag = "";
                            var orgChartFlag = "";
                            var showFilterFlag = "";

                            if (chartInitParamObj != null)
                            {
                                calenderFlag = chartInitParamObj['uuu_CalenderFlag'];
                                orgChartFlag = chartInitParamObj['uuu_orgChartFlag'];
                                chartValueAsDollar = chartInitParamObj['uuu_chartValueAsDollar'];
                                showFilterFlag = chartInitParamObj['uuu_showFilterFlag'];
                            }
                            $("#" + chartId + "_Data").val(dataArray);
                            $("#" + chartId + "_options").val(JSON.stringify(options));
                            $("#" + chartId + "_description").val(JSON.stringify(description));
                            $("#" + chartId + "_chartInitParams").val(JSON.stringify(chartInitParamObj));
                            $("#" + chartId + "_Chart").attr('showFilterFlag', showFilterFlag);
                            var dataObjArr = {};
                            var dataObj = $("#" + chartId + "_Data").val();
                            if (dataObj != null && dataObj != '' && dataObj != undefined)
                            {
                                dataObjArr = JSON.parse(dataObj);
                            }
                            if (sizeOfChart != null && sizeOfChart != '' && sizeOfChart != undefined) {
                                $("#" + chartId + "_Chart").css("width", "" + sizeOfChart + "%", "important");
                            } else if (windowWidth > 1024)
                            {
                                $("#" + chartId + "_Chart").css('width', '32%');
                                options['width'] = '90%';
                            } else if (windowWidth < 1024 && windowWidth > 700)
                            {
                                $("#" + chartId + "_Chart").css('width', '49%');
                                options['width'] = '90%';
                            } else if (windowWidth < 699 && windowWidth > 320)
                            {
                                $("#" + chartId + "_Chart").css('width', '98%');
                                options['width'] = '90%';
                            }
                            var data = [];
                            if (dataObjArr != null && dataObjArr.length > 0)
                            {
                                if (calenderFlag != null && calenderFlag != '' && calenderFlag == "Y") {
                                    try {
                                        var dataObjStr = calenderChartData(dataObjArr);
                                        data = google.visualization.arrayToDataTable(dataObjStr);
                                        if (chartValueAsDollar != null && chartValueAsDollar != '' && chartValueAsDollar == "Y") {
                                            var formatter = new google.visualization.NumberFormat(
                                                    {prefix: '$'});
                                            formatter.format(data, 1);
                                        }
                                    } catch (e) {
                                    }
                                } else {
                                    data = google.visualization.arrayToDataTable(dataObjArr);
                                    if (chartValueAsDollar != null && chartValueAsDollar != '' && chartValueAsDollar == "Y") {
                                        var formatter = new google.visualization.NumberFormat(
                                                {prefix: '$'});
                                        formatter.format(data, 1);
                                    }
                                }
                                var objInit1 = eval(objInit);
                                var chart = objInit1;
                                if (orgChartFlag != null && orgChartFlag != '' && orgChartFlag != undefined
                                        && orgChartFlag == "Y")
                                {
                                    $("#" + chartId + "_Chart").addClass('visionOrgCharts');
//                                   $("#" + chartId + "_Chart").css('height', options['height'], "important");
                                    chart.draw(data, options);
                                    var dataValue = data.getValue(0, 0);
                                    var colorValue = $("#color_" + dataValue).val();
                                    if (colorValue != null && colorValue != '' && colorValue != undefined)
                                    {
                                        data.setRowProperty(0, 'style', 'background-color:' + colorValue + ';background-image:none');
                                        chart.draw(data, options);
                                    }
                                } else
                                {
                                    if (labelChartArr != null && !jQuery.isEmptyObject(labelChartArr))
                                    {
                                        var view = new google.visualization.DataView(data);
                                        view.setColumns(labelChartArr);
                                        google.visualization.events.addListener(chart, 'ready', function () {
                                            chart.innerHTML = chart.getImageURI();
                                            $("#" + chartId).attr("chartImageData", chart.getImageURI());
                                            JQXImageContent[i] = chart.getImageURI();
                                            $("#" + chartId).attr("chartNum", i);
                                        });
                                        chart.draw(view, options);
                                    } else
                                    {
                                        google.visualization.events.addListener(chart, 'ready', function () {
                                            chart.innerHTML = chart.getImageURI();
                                            $("#" + chartId).attr("chartImageData", chart.getImageURI());
                                            JQXImageContent[i] = chart.getImageURI();
                                            jsonChartImg[i] = chart.getImageURI();
                                            $("#" + chartId).attr("chartNum", i);
                                        });
                                        chart.draw(data, options);
                                    }
//                                    chart.draw(data, options);
                                }
                                var chartType = result['chartType'];
                                var replacChartType = result['replacChartType'];
                                if (replacChartType != null && replacChartType != '' && replacChartType != undefined)
                                {
                                    $("#" + chartId + "_chartType").val(replacChartType);
                                } else
                                {
                                    $("#" + chartId + "_chartType").val(chartType);
                                }
                                var tabGridId = result['tabGridId'];
                                if (chartType != null && chartType != '' && chartType != undefined)
                                {
                                    $("#" + chartId + "_types").val(chartType);
                                }

                                if (orgChartFlag != null && orgChartFlag != '' && orgChartFlag != undefined
                                        && orgChartFlag == "Y")
                                {
                                    $("#" + chartId).on("click", function (event) {
                                        var target = event.target;
                                        var href = target.href;
                                        if (chart.getSelection().length > 0) {
                                            if (!(href != null && href != '' && href != undefined) ||
                                                    (href != null && href != '' && href != undefined && href.indexOf("images") > -1))
                                            {
                                                chartStartTabLoader();
                                                orgChart(chart, data, tabGridId, chartId, options, event, this);
                                            } else
                                            {
                                                navigateToOrgChart(chart, data, tabGridId, chartId);
                                            }
                                        }
                                    });
                                } else
                                {
                                    google.visualization.events.addListener(chart, 'select', function () {
                                        selectHandler(chart, data, chartId, tabGridId);
                                    });
                                }

//                                google.visualization.events.addListener(chart, 'select', function () {
//                                    if (orgChartFlag != null && orgChartFlag != '' && orgChartFlag != undefined
//                                            && orgChartFlag == "Y")
//                                    {
//                                        rowValue = chart.getSelection()[0].row;
//                                       var date = new Date();
//                                        var millis = date.getTime();
//                                        if (millis - secondClick > 1000) {
//                                            // add dealyed check if a single click occured
//                                            setTimeout(function () {
//                                                // no second fast enough, it is a single click
//                                                if (secondClick == 0) {
//                                                    console.log("single click");
//                                                    orgChart(chart, data, tabGridId, result['childId'], chartId);
//                                                    console.log("click");
//                                                }
//                                            }, 200);
//                                        }
//                                        // try to measure if double-clicked
//                                        if (millis - firstClick < 200) {
//                                            firstClick = 0;
//                                            secondClick = millis;
//                                            console.log("doubleclick");
//                                            navigateToOrgChart(chart, data, tabGridId, result['childId'], chartId);
//                                        } else {
//                                            firstClick = millis;
//                                            secondClick = 0;
//                                        }
//                                        chart.setSelection([]);
//                                    } else {
//                                        selectHandler(chart, data, chartId, tabGridId);
//                                    }
//                                });

                            } else
                            {
                                if (!(options['width'].indexOf("%") > -1))
                                {
                                    options['width'] = options['width'] + "px";
                                }
                                $("#" + chartId + "_Chart").html("<div class ='analyticChartNoDataClass' style ='width:" + options['width'] + ";'><p>" + options['title'] + "</p><span class='analyticsChartNoDataText'>No Data Found</span></div>");
                            }
                        }
                        $("#wait").css("display", "none");

                        if (i < chartDataList.length) {
                            var jqwidgetFlag = chartDataList[i]['36'];
                            if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Y')
                            {
                                getJqwidgetChart(chartDataList[i], tabId, i, chartDataList);
                            } else if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Q')
                            {
                                getShowCube(chartDataList[i], tabId, i, chartDataList);
                            } else
                            {
                                getChartByChart(chartDataList[i], tabId, i, chartDataList);
                            }
//                            getChartByChart(chartDataList[i], tabId, i, chartDataList);
                        }

                    }
                } catch (e)
                {
                    if (i < chartDataList.length) {
                        var jqwidgetFlag = chartDataList[i]['36'];
                        if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Y')
                        {
                            getJqwidgetChart(chartDataList[i], tabId, i, chartDataList);
                        } else if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Q')
                        {
                            getShowCube(chartDataList[i], tabId, i, chartDataList);
                        } else
                        {
                            getChartByChart(chartDataList[i], tabId, i, chartDataList);
                        }
//                        getChartByChart(chartDataList[i], tabId, i, chartDataList);
                    }
                    $("#wait").css("display", "none");
                }
            }

        },
        error: function (e) {
            if (i < chartDataList.length) {
                var jqwidgetFlag = chartDataList[i]['36'];
                if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Y')
                {
                    getJqwidgetChart(chartDataList[i], tabId, i, chartDataList);
                } else if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Q')
                {
                    getShowCube(chartDataList[i], tabId, i, chartDataList);
                } else
                {
                    getChartByChart(chartDataList[i], tabId, i, chartDataList);
                }
//                getChartByChart(chartDataList[i], tabId, i, chartDataList);
            }
            sessionTimeout(e);
            $("#wait").css("display", "none");
        }

    });
}
function analyticsTypeDropdown(chartVal, tabChartId, chartId)
{
    console.log("iam in analyticsTypeDropdown");
    var jqxChartNum = $("#" + chartId).attr("jqxChartNum");
    var chartNum = $("#" + chartId).attr("chartNum");
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        cache: false,
        url: "analyticDropdown",
        data: {
            'tabComponentId': chartId,
            'tabId': tabChartId,
            'chartTypeId': chartVal
        },
        success: function (response) {
            if (response != null)
            {
                var result = JSON.parse(response);
                var dataObj = result['chartList'];
                for (var i = 0; i < dataObj.length; i++)
                {
                    $("#wait").css("display", "block");
                    var tabId = $("#tabChartsId").val();
                    var chartData = dataObj[i];
                    var paramArrData = [];
                    var paramArray = $("#" + chartId + "_filterVal").val();
                    if (paramArray != null && paramArray != '' && paramArray != undefined)
                    {
                        paramArrData = JSON.parse(paramArray);
                    }
//                    getChartDropdownData(chartData, tabId, chartVal, paramArrData);
                    var jqwidgetFlag = chartData['36'];
                    if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Y')
                    {
                        getJqwidgetChartDropdownData(chartData, tabId, chartVal, paramArrData, jqxChartNum);//jqwidgets
                    } else {
                        getChartDropdownData(chartData, tabId, chartVal, paramArrData, chartNum);
                    }
                }
            }
        },
        error: function (e) {
            sessionTimeout(e);
        }
    });
}


function getChartDropdownData(chartData, tabId, chartVal, paramArray, chartNum)
{
    var dataApiObjStr = $("#dataApiObjStr").val();
    var paramsMapEncStr = $("#paramsMapStr").val();

    var data = {
        'chartData': chartData,
        'ddwFlag': 'Y',
        'chartVal': chartVal,
        'paramArray': JSON.stringify(paramArray),
        'chartFilterItems': JSON.stringify(filterItem),
        'dataApiObjStr': dataApiObjStr

    };
    if (paramsMapEncStr != null && paramsMapEncStr != '') {
        var paramsMapwords = CryptoJS.enc.Base64.parse(paramsMapEncStr);
        var paramsMapStr = CryptoJS.enc.Utf8.stringify(paramsMapwords);
        var paramsMap = JSON.parse(paramsMapStr);
        console.log("paramsMap::" + paramsMap);
        if (paramsMap != null && !jQuery.isEmptyObject(paramsMap)) {
            for (var parmKey in paramsMap) {
                if (paramsMap[parmKey] != null) {
                    data[parmKey] = paramsMap[parmKey];
                }

            }
        }
    }
    var filterItem;
    if (chartFilterItems != null && !jQuery.isEmptyObject(chartFilterItems))
    {
        filterItem = JSON.parse(chartFilterItems);
    }
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        url: "getAnalyticCharts",
        async: true,
        cache: false,
        data: data,
        success: function (response) {
            $("#wait").css("display", "none");
            if (response != null && response != '' && response != undefined) {
                var resultData = JSON.parse(response);
                if (resultData != null)
                {
                    var result = resultData[0];
                    if (result != null) {
                        var divid = result['divId'];
                        var chartId = result['chartId'];
                        var seqNo = result['seqNo'];
                        var paramArrData = [];
                        var paramArr = $("#" + chartId + "_filterVal").val();
                        if (paramArr != null && paramArr != '' && paramArr != undefined)
                        {
                            paramArrData = JSON.parse(paramArr);
                        }
                        $("#" + chartId + "_Chart").empty();
                        $("#" + chartId + "_Chart").html(divid);
                        var dataArray = result['dataArray'];
                        var options = result['options'];
                        var objInit = result['objInit'];
                        $("#" + chartId + "_options").val(JSON.stringify(options));
                        var sizeOfChart = result['sizeOfChart'];
                        var chartInitParamObj = result['chartInitParamObj'];
                        var calenderFlag = "";
                        if (chartInitParamObj != null)
                        {
                            calenderFlag = chartInitParamObj['uuu_CalenderFlag'];
                            chartValueAsDollar = chartInitParamObj['uuu_chartValueAsDollar'];
                        }
                        var chartId = result['chartId'];
                        var description = result['description'];
                        $("#" + chartId + "_description").val(JSON.stringify(description));
                        $("#" + chartId + "_chartInitParams").val(JSON.stringify(chartInitParamObj));
                        $("#" + chartId + "_Data").val(dataArray);
                        $("#" + chartId + "_ddwVal").val(chartVal);
                        $("#" + chartId + "_filterVal").val(JSON.stringify(paramArrData));
                        var dataObjArr = {};
                        var dataObj = $("#" + chartId + "_Data").val();
                        var showFiletrText = $("#" + chartId + "_Chart").attr('showFilterText');
                        var data = [];
                        if (dataObj != null && dataObj != '' && dataObj != undefined)
                        {
                            dataObjArr = JSON.parse(dataObj);
                        }
                        if (sizeOfChart != null && sizeOfChart != '' && sizeOfChart != undefined) {
                            $("#" + chartId + "_Chart").css("width", "" + sizeOfChart + "%", "important");
                        } else if (windowWidth > 1024)
                        {
                            $("#" + chartId + "_Chart").css('width', '32%');
                            options['width'] = '100%';
                        } else if (windowWidth < 1024 && windowWidth > 700)
                        {
                            $("#" + chartId + "_Chart").css('width', '49%');
                            options['width'] = '90%';
                        } else if (windowWidth < 699 && windowWidth > 320)
                        {
                            $("#" + chartId + "_Chart").css('width', '98%');
                            options['width'] = '90%';
                        }
                        $("#" + chartId + "_filter_text").html(showFiletrText);
                        if (dataObjArr != null && dataObjArr.length > 0) {
                            if (calenderFlag != null && calenderFlag != '' && calenderFlag == "Y") {
                                try {
                                    var dataObjStr = calenderChartData(dataObjArr);
                                    data = google.visualization.arrayToDataTable(dataObjStr);
                                    if (chartValueAsDollar != null && chartValueAsDollar != '' && chartValueAsDollar == "Y") {
                                        var formatter = new google.visualization.NumberFormat(
                                                {prefix: '$'});
                                        formatter.format(data, 1);
                                    }
                                } catch (e) {
                                }
                            } else {
                                data = google.visualization.arrayToDataTable(dataObjArr);
                                if (chartValueAsDollar != null && chartValueAsDollar != '' && chartValueAsDollar == "Y") {
                                    var formatter = new google.visualization.NumberFormat(
                                            {prefix: '$'});
                                    formatter.format(data, 1);
                                }
                            }
                            var objInit1 = eval(objInit);
                            var chart = objInit1;
                            google.visualization.events.addListener(chart, 'ready', function () {
                                chart.innerHTML = '<img src="' + chart.getImageURI() + '">';

                                $("#" + chartId).attr("chartImageData", chart.getImageURI());
                                JQXImageContent[chartNum] = chart.getImageURI();
                            });
                            chart.draw(data, options);
                            $("#" + chartId).attr("chartNum", chartNum);
                            $("#" + chartId + "_types").val(chartVal);
                            var replacChartType = result['replacChartType'];
                            if (replacChartType != null && replacChartType != '' && replacChartType != undefined)
                            {
                                $("#" + chartId + "_chartType").val(replacChartType);
                            } else
                            {
                                $("#" + chartId + "_chartType").val(chartVal);
                            }
                            var tabGridId = result['tabGridId'];
                            var tabComponentId = $("#" + chartId + '_tabGridId').val();
                            if (tabComponentId != null && tabComponentId != '' && tabComponentId != undefined)
                            {
                                tabGridId = tabComponentId;
                            }
                            google.visualization.events.addListener(chart, 'select', function () {
                                selectHandler(chart, data, chartId, tabGridId);
                            });
                        } else
                        {
                            if (!(options['width'].indexOf("%") > -1))
                            {
                                options['width'] = options['width'] + "px";
                            }
                            $("#" + chartId).html("<div class ='analyticChartNoDataClass'><span class='analyticsChartNoDataText' style ='width:" + options['width'] + ";'>No Data Found</span></div>");
                        }

                    }
                    $("#wait").css("display", "none");
                }
            }

        },
        error: function (e) {
            sessionTimeout(e);
        }

    });
}
function calenderChartData(dataObjArr)
{
    for (var i = 0; i < dataObjArr.length; i++)
    {
        if (i != 0) {
            var dataObj = dataObjArr[i];
            if (dataObj != null) {
                var dataObjStr = dataObj[0].replace(/"/g, "");
                dataObj[0] = eval(dataObjStr);
            }
        }
    }
    return dataObjArr;
}
function navigateToChartGrid(chartType, tabGridId, chartId, gridId)
{
    try {
        $("#chartId").remove();
        $("#gridId").remove();
        $("#auditGridId").remove();
        $("#chartParamArray").remove();
    } catch (e) {
    }
    console.log("gridId is:::" + gridId + ":::chartId:::" + chartId);
    $("#submitForm").attr("action", 'grid');
    $("#submitForm").attr("method", 'post');
    $("#submitForm").attr("target", '_blank');
    $("#submitForm").append("<input type='hidden' id='chartId' name='chartId' value='" + chartId + "'/>");
    $("#submitForm").append("<input type='hidden' id='gridId' name='gridId' value='" + gridId + "'/>");
    $("#submitForm").append("<input type='hidden' id='auditGridId' name='auditGridId' value='" + gridId + "'/>");
    $("#submitForm").append("<input type='hidden' name='chartParamArray' id='chartParamArray' value=''/>");
    $("#submitForm").append("<input type='hidden' id='headerFlag' name='headerFlag' value='N'/>");
    var chartItems = {};
    var filterItem;
    var paramArray = [];
    if (chartFilterItems != null && !jQuery.isEmptyObject(chartFilterItems))
    {
        filterItem = JSON.parse(chartFilterItems);
        for (var key in filterItem) {
            var paramObj = {};
            console.log(key + " is " + filterItem[key]);
            var value = filterItem[key];
            paramObj.column = key;
            paramObj.operator = "EQUALS";
            paramObj.value = value;
            paramArray.push(paramObj);
        }
        chartItems.chartParamObj = paramArray;
    }
    var paramStr = $("#" + chartId + "_filterVal").val();
    if (paramStr != null && paramStr != '' && paramStr != undefined)
    {
        chartItems.chartParamObj = JSON.parse(paramStr);
    }
    if (paramStr != null && paramStr != '' && paramStr != undefined && paramArray != null)
    {
        var jsonParamArr = JSON.parse(paramStr);
        for (var obj in paramArray)
        {
            jsonParamArr.push(paramArray[obj]);
        }
        chartItems.chartParamObj = jsonParamArr;
    }

    $("#chartParamArray").val(JSON.stringify(chartItems));
    $("#submitForm").submit();
}

function navigateToEditForm(chartType, tabGridId, chartId, gridId)
{
    console.log("gridId is:::" + gridId + ":::chartId:::" + chartId);
    $.ajax({
        type: "post",
        traditional: true,
        // dataType: 'json',
        url: "getAnalyticChartForm",
        cache: false,
        data: {
            'selectedGridId': gridId,
            'chartId': chartId

        },
        success: function (response) {
            if (response != null && response != '') {
                var filterFormObj = JSON.parse(response);
                chartEditForm(filterFormObj, gridId, chartId, tabGridId, chartType);
                $("#importsearchcriteria").html(filterFormObj['importButtonDiv']);
                var jsDateItems = filterFormObj['dateObjArray'];
                for (var i = 0; i < jsDateItems.length; i++) {
                    console.log("tbid:::" + jsDateItems[i].tbid);
                    $("#" + gridId + "_" + jsDateItems[i].tbid).datepicker(
                            {dateFormat: "dd-mm-yy",
                                changeMonth: true,
                                changeYear: true})
                            .on('changeDate', function (ev) {
                                if (jsDateItems[i].type == 'min') {
                                    console.log($("#" + jsDateItems[i].tbid).datepicker("getDate"));
                                    $("#" + gridId + "_" + jsDateItems[i].tbid).datepicker(
                                            {
                                                minDate: $("#" + gridId + "_" + jsDateItems[i].tbid).datepicker("getDate")
                                            });
                                } else {
                                }
                            });
                }
                $("#ui-datepicker-div").addClass("ui-datepickerReports");
                selectedTitle = "";
                selectedTitleValue = "";
                var lovColumns = filterFormObj['lovColumns'];
                for (var j = 0; j < lovColumns.length; j++) {
                    var lovColumnanme = lovColumns[j];
                    $("#" + lovColumnanme + "_dropdown dt a").on('click', function () {
                        $("#" + lovColumnanme + "_dropdown dd ul").slideToggle('fast');
                    });
                    $("#" + lovColumnanme + "_dropdown dd ul li a").on('click', function () {
                        $("#" + lovColumnanme + "_dropdown dd ul").hide();
                    });
                    $(document).bind('click', function (e) {
                        var $clicked = $(e.target);
                        if (!$clicked.parents().hasClass("visionFilterGridDropdown"))
                            $("#" + lovColumnanme + "_dropdown dd ul").hide();
                    });
                    $('#' + lovColumnanme + '_mutliSelect input[type="checkbox"]').on('click', function () {
                        var title = $(this).val() + ",";
                        var titleValue = $(this).attr("data-processvalue") + ",";
                        if ($(this).is(':checked')) {
                            selectedTitle += title;
                            selectedTitleValue += titleValue;
                            $("#" + lovColumnanme + "_LABELS").html(selectedTitle);
                            $("#" + lovColumnanme).val(selectedTitleValue);
                        } else {
                            console.log("Pop::B:" + selectedTitle);
                            console.log("Pop:selectedTitleValue:B:" + selectedTitleValue);
                            if (selectedTitle != null && selectedTitle != '') {
                                selectedTitle = selectedTitle.replace(title, "");
                            }
                            if (selectedTitleValue != null && selectedTitleValue != '') {
                                selectedTitleValue = selectedTitleValue.replace(titleValue, "");
                            }

                            console.log("Pop::A:" + selectedTitle);
                            console.log("Pop:selectedTitleValue:A:" + selectedTitleValue);
                            $("#" + lovColumnanme + "_LABELS").html(selectedTitle);
                            $("#" + lovColumnanme).val(selectedTitleValue);
                        }
                        if (selectedTitleValue != null && selectedTitleValue != ''
                                && selectedTitleValue.indexOf(",") > -1) {
                            var filterGridFlagCount = $("#" + lovColumnanme).attr("data-filtergridflag-count");
                            console.log("filterGridFlagCount:::" + filterGridFlagCount);
                            console.log("filterGridFlagCount:::" + filterGridFlagCount);
                            var operatorId = "operator" + $("#" + lovColumnanme).attr("data-viewid") + filterGridFlagCount;
                            console.log("operator id::" + operatorId);
                            $("#" + operatorId).val("IN");
                        }
                    });
                }
            }

        },
        error: function (e) {
            sessionTimeout(e);
        }
    });
}
function chartEditForm(filterFormObj, gridId, chartId, tabGridId, chartType)
{
    $("#dialog").html(filterFormObj['result']);
    $("#dialog").dialog({
        modal: true,
        title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
        height: 'auto',
        minHeight: 'auto',
        minWidth: '600',
        maxWidth: 'auto',
        fluid: true,
        buttons: {
            OK: function () {

                $("#wait").css("display", "block");
                updateAnalyticChart(chartId, gridId, tabGridId, chartType);
                $(this).html("");
                $(this).dialog("close");
                $(this).dialog("destroy");
            },
            Close: function () {
                $(this).html("");
                $(this).dialog("close");
                $(this).dialog("destroy");
            }

        },
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
//function updateAnalyticChart(chartId, gridId, tabGridId, chartType)
//{
//    var i = 0;
//    var paramArray = [];
//    $("#" + gridId + "_CHART_FORM_TABLE tbody tr").each(function () {
//        var isAllow = false;
//        var paramObj = {};
//        var colname = $(this).attr('data-colname');
//        var dataRange = $(this).attr('data-range');
//        var value = $("#" + colname).val();
//        var minvalue = $("#" + colname + "_MIN").val();
//        var maxvalue = $("#" + colname + "_MAX").val();
//        if (value != null && value != '') {
//            isAllow = true;
//        } else if (dataRange != null && dataRange == 'Y'
//                && ((minvalue != null && minvalue != '')
//                        || (maxvalue != null && maxvalue != ''))
//                ) {
//            isAllow = true;
//        }
//        var type = $("#" + colname).attr("type");
//        if (type != null && type == 'checkbox') {
//            var textval = "N";
//            if ($("#" + colname).is(':checked')) {
//                isAllow = true;
//            } else {
//                isAllow = false;
//            }
//        }
//        console.log("isAllow::::" + isAllow);
//        if (isAllow) {
//            paramObj.column = $.trim($(this).attr('data-colname'));
//            if (type != null && type == 'checkbox') {
//                var textval = "N";
//                if ($("#" + colname).is(':checked')) {
//                    textval = "Y";
//                } else {
//                    textval = "N";
//                }
//                paramObj.value = textval;
//            } else {
//                paramObj.value = $.trim($("#" + colname).val());
//            }
//            paramObj.operator = $("#operator" + gridId + i).val();
//            paramObj.symbol = $.trim($("#operator" + gridId + i).find('option:selected').text());
//            paramObj.rangeFlag = dataRange;
//            if (dataRange != null && dataRange == 'Y') {
//                paramObj.minvalue = minvalue;
//                paramObj.maxvalue = maxvalue;
//            } else {
//                paramObj.minvalue = "";
//                paramObj.maxvalue = "";
//            }
//            paramArray.push(paramObj);
//        }
//        ++i;
//    });
//    if (paramArray != null && paramArray.length > 0) {
//
//        alert("selectedGridId:::" + gridId);
//        $.ajax({
//            type: "post",
//            traditional: true,
//            dataType: 'html',
//            cache: false,
//            url: "analyticDropdown",
//            data: {
//                'gridId': gridId,
//                'tabComponentId': chartId,
//                'tabId': tabGridId,
//                'chartTypeId': chartType
//            },
//            success: function (response) {
//                if (response != null)
//                {
//                    var result = JSON.parse(response);
//                    var dataObj = result['chartList'];
//                    for (var i = 0; i < dataObj.length; i++)
//                    {
//                        var tabId = $("#tabChartsId").val();
//                        var chartData = dataObj[i];
//                        getChartUpdateData(chartData, tabId, paramArray, tabGridId);
//                    }
//                }
//            },
//            error: function (e) {
//                sessionTimeout(e);
//            }
//        });
//    } else {
//        $("#wait").css("display", "none");
//        var dialogSplitMessage = dialogSplitIconText("Please provide at least one value to Search.", "Y");
//        showDialog(dialogSplitMessage);
//    }
//}

//updated charts method
function updateAnalyticChart(chartId, gridId, tabGridId, chartType)
{
    var i = 0;
    var paramArray = [];
    var filterText = "";
    var showFilterFlag = $("#" + chartId + "_Chart").attr('showFilterFlag');
    $("#" + gridId + "_CHART_FORM_TABLE tbody tr").each(function () {
        var isAllow = false;
        var paramObj = {};
        var colname = $(this).attr('data-colname');
        var dataRange = $(this).attr('data-range');
        var value = $("#" + gridId + "_" + colname).val();
        var minvalue = $("#" + gridId + "_" + colname + "_MIN").val();
        var maxvalue = $("#" + gridId + "_" + colname + "_MAX").val();
        if (value != null && value != '') {
            isAllow = true;
        } else if (dataRange != null && dataRange == 'Y'
                && ((minvalue != null && minvalue != '')
                        || (maxvalue != null && maxvalue != ''))
                ) {
            isAllow = true;
        }
        var type = $("#" + gridId + "_" + colname).attr("type");
        if (type != null && type == 'checkbox') {
            var textval = "N";
            if ($("#" + gridId + "_" + colname).is(':checked')) {
                isAllow = true;
            } else {
                isAllow = false;
            }
        }
        console.log("isAllow::::" + isAllow);
        if (isAllow) {
            paramObj.column = $.trim($(this).attr('data-colname'));
            if (type != null && type == 'checkbox') {
                var textval = "N";
                if ($("#" + gridId + "_" + colname).is(':checked')) {
                    textval = "Y";
                } else {
                    textval = "N";
                }
                paramObj.value = textval;
            } else {
                paramObj.value = $.trim($("#" + gridId + "_" + colname).val());
            }
            paramObj.operator = $("#operator" + gridId + i).val();
            paramObj.symbol = $.trim($("#operator" + gridId + i).find('option:selected').text());
            paramObj.rangeFlag = dataRange;
            if (showFilterFlag == 'Y')
            {
                if (colname != null && colname != '' && colname != undefined && colname.indexOf("DATE") > -1)
                {
                    if (dataRange == 'Y' && minvalue != null && minvalue != '' && minvalue != undefined
                            && maxvalue != null && maxvalue != '' && maxvalue != undefined)
                    {
                        filterText += '<span>From Date :</span>' + minvalue + ' <span> To Date :</span>' + maxvalue;
                    } else if (paramObj.value != null && paramObj.value != '' && paramObj.value != undefined)
                    {
                        filterText += '<span> Date :</span>' + $.trim($("#" + gridId + "_" + colname).val());
                    }
                }
            }
            if (dataRange != null && dataRange == 'Y') {
                paramObj.minvalue = minvalue;
                paramObj.maxvalue = maxvalue;
            } else {
                paramObj.minvalue = "";
                paramObj.maxvalue = "";
            }
            paramArray.push(paramObj);
        }
        ++i;
    });
    if (paramArray != null && paramArray.length > 0) {
        $("#" + chartId + "_Chart").attr('showFilterText', filterText);
        alert("selectedGridId:::" + gridId);
        $.ajax({
            type: "post",
            traditional: true,
            dataType: 'html',
            cache: false,
            url: "analyticDropdown",
            data: {
                'gridId': gridId,
                'tabComponentId': chartId,
                'tabId': tabGridId,
                'chartTypeId': chartType
            },
            success: function (response) {
                if (response != null)
                {
                    var result = JSON.parse(response);
                    var dataObj = result['chartList'];
                    for (var i = 0; i < dataObj.length; i++)
                    {
                        var tabId = $("#tabChartsId").val();
                        var chartData = dataObj[i];
//                        getChartUpdateData(chartData, tabId, paramArray, tabGridId);
                        var jqwidgetFlag = chartData['36'];
                        if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Y')
                        {
                            getJqwidgetChartUpdateData(chartData, tabId, paramArray, tabGridId);
                        } else {
                            getChartUpdateData(chartData, tabId, paramArray, tabGridId);
                        }
                    }
                }
            },
            error: function (e) {
                sessionTimeout(e);
            }
        });
    } else {
        $("#wait").css("display", "none");
        var dialogSplitMessage = dialogSplitIconText("Please provide at least one value to Search.", "Y");
        showDialog(dialogSplitMessage);
    }
}
//updated charts method

function getChartUpdateData(chartData, tabId, paramArray, tabGridId)
{
    var filterItem;
    if (chartFilterItems != null && !jQuery.isEmptyObject(chartFilterItems))
    {
        filterItem = JSON.parse(chartFilterItems);
    }
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        url: "getAnalyticCharts",
        async: true,
        cache: false,
        data: {
            'chartData': chartData,
            'ddwFlag': 'Y',
            'editFlag': 'Y',
            'paramArray': JSON.stringify(paramArray),
            'chartFilterItems': JSON.stringify(filterItem)
        },
        success: function (response) {
            $("#wait").css("display", "none");
            if (response != null && response != '' && response != undefined) {
                var resultData = JSON.parse(response);
                if (resultData != null)
                {
                    var result = resultData[0];
                    if (result != null) {
                        var divid = result['divId'];
                        var chartId = result['chartId'];
                        var chartVal = $("#" + chartId + "_ddwVal").val();
                        $("#" + chartId + "_Chart").empty();
                        $("#" + chartId + "_Chart").html(divid);
                        var dataArray = result['dataArray'];
                        var showFilterText = $("#" + chartId + "_Chart").attr('showFilterText');
                        var options = result['options'];
                        var objInit = result['objInit'];
                        var sizeOfChart = result['sizeOfChart'];
                        var chartId = result['chartId'];
                        var seqNo = result['seqNo'];
                        var description = result['description'];
                        var labelChartArr = result['labelChartArr'];
                        var chartInitParamObj = result['chartInitParamObj'];
                        var calenderFlag = "";
                        if (chartInitParamObj != null)
                        {
                            calenderFlag = chartInitParamObj['uuu_CalenderFlag'];
                            chartValueAsDollar = chartInitParamObj['uuu_chartValueAsDollar'];
                        }
                        $("#" + chartId + "_description").val(JSON.stringify(description));
                        $("#" + chartId + "_chartInitParams").val(JSON.stringify(chartInitParamObj));
                        $("#" + chartId + "_options").val(JSON.stringify(options));
                        $("#" + chartId + "_Data").val(dataArray);
                        $("#" + chartId + "_ddwVal").val(chartVal);
                        $("#" + chartId + "_filterVal").val(JSON.stringify(paramArray));
                        $("#" + chartId + "_filter_text").html(showFilterText);
                        var dataObjArr = {};
                        var dataObj = $("#" + chartId + "_Data").val();
                        if (dataObj != null && dataObj != '' && dataObj != undefined)
                        {
                            dataObjArr = JSON.parse(dataObj);
                        }
                        if (sizeOfChart != null && sizeOfChart != '' && sizeOfChart != undefined) {
                            $("#" + chartId + "_Chart").css("width", "" + sizeOfChart + "%", "important");
                        } else if (windowWidth > 1024)
                        {
                            $("#" + chartId + "_Chart").css('width', '32%');
                            options['width'] = '100%';
                        } else if (windowWidth < 1024 && windowWidth > 700)
                        {
                            $("#" + chartId + "_Chart").css('width', '49%');
                            options['width'] = '90%';
                        } else if (windowWidth < 699 && windowWidth > 320)
                        {
                            $("#" + chartId + "_Chart").css('width', '98%');
                            options['width'] = '90%';
                        }
                        var data = [];
                        if (dataObjArr != null && dataObjArr.length > 0)
                        {
                            if (calenderFlag != null && calenderFlag != '' && calenderFlag == "Y") {
                                try {
                                    var dataObjStr = calenderChartData(dataObjArr);
                                    data = google.visualization.arrayToDataTable(dataObjStr);
                                    if (chartValueAsDollar != null && chartValueAsDollar != '' && chartValueAsDollar == "Y") {
                                        var formatter = new google.visualization.NumberFormat(
                                                {prefix: '$'});
                                        formatter.format(data, 1);
                                    }
                                } catch (e) {
                                }
                            } else {
                                data = google.visualization.arrayToDataTable(dataObjArr);
                                if (chartValueAsDollar != null && chartValueAsDollar != '' && chartValueAsDollar == "Y") {
                                    var formatter = new google.visualization.NumberFormat(
                                            {prefix: '$'});
                                    formatter.format(data, 1);
                                }
                            }
                            var objInit1 = eval(objInit);
                            var chart = objInit1;
//                            chart.draw(data, options);
                            if (labelChartArr != null && !jQuery.isEmptyObject(labelChartArr))
                            {
                                var view = new google.visualization.DataView(data);
                                view.setColumns(labelChartArr);
                                chart.draw(view, options);
                            } else
                            {
                                chart.draw(data, options);
                            }
                            $("#" + chartId + "_types").val(chartVal);
                            var replacChartType = result['replacChartType'];
                            if (replacChartType != null && replacChartType != '' && replacChartType != undefined)
                            {
                                $("#" + chartId + "_chartType").val(replacChartType);
                            } else
                            {
                                $("#" + chartId + "_chartType").val(chartVal);
                            }
                            google.visualization.events.addListener(chart, 'select', function () {
                                selectHandler(chart, data, chartId, tabGridId);
                            });
                        } else
                        {
                            if (!(options['width'].indexOf("%") > -1))
                            {
                                options['width'] = options['width'] + "px";
                            }
                            $("#" + chartId).html("<div class ='analyticChartNoDataClass' style ='width:" + options['width'] + ";'><span class='analyticsChartNoDataText'>No Data Found</span></div>");
                        }

                    }
                    $("#wait").css("display", "none");
                }
            }

        },
        error: function (e) {
            sessionTimeout(e);
        }

    });
}
function refreshChart(chartType, tabGridId, chartId, gridId)
{
    console.log("chartId:::" + chartId);
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        cache: false,
        url: "dataAnalyticsCharts",
        data: {
            'tabComponentId': tabGridId,
            'chartId': chartId,
            'resetFlag': 'Y'
        },
        success: function (response) {
            if (response != null)
            {
                var result = JSON.parse(response);
                var dataObj = result['chartList'];
                if (dataObj != null) {
                    var chartData = dataObj[0];
                    $("#wait").css("display", "block");
//                    getRefreshChartData(chartData, tabGridId, chartId);

                    var jqwidgetFlag = chartData['36'];
                    if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Y')
                    {
                        getJqwidgetsRefreshChartData(chartData, tabGridId, chartId);
                    } else {
                        getRefreshChartData(chartData, tabGridId, chartId);//jqwidgets
                    }
                }
            }
        },
        error: function (e) {
            sessionTimeout(e);
        }
    });
}
function getRefreshChartData(chartData, tabChartId, chartId)
{
    var filterItem;
    if (chartFilterItems != null && !jQuery.isEmptyObject(chartFilterItems))
    {
        filterItem = JSON.parse(chartFilterItems);
    }
    var paramArrData = [];
    var paramArray = $("#" + chartId + "_Nested_filterVal").val();
    if (paramArray != null && paramArray != '' && paramArray != undefined)
    {
        paramArrData = JSON.parse(paramArray);
    }
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        url: "getAnalyticCharts",
        async: true,
        cache: false,
        data: {
            'chartData': chartData,
            'resetFlag': 'Y',
            'paramArray': JSON.stringify(paramArrData),
            'chartFilterItems': JSON.stringify(filterItem)
        },
        success: function (response) {
            if (response != null && response != '' && response != undefined) {
                $("#wait").css("display", "none");
                var resultData = JSON.parse(response);
                if (resultData != null)
                {
                    var result = resultData[0];
                    if (result != null) {
                        var divid = result['divId'];
                        var chartId = result['chartId'];
                        $("#" + chartId + "_Chart").empty();
                        $("#" + chartId + "_Chart").html(divid);
                        var dataArray = result['dataArray'];
                        var options = result['options'];
                        var objInit = result['objInit'];
                        var seqNo = result['seqNo'];
                        var sizeOfChart = result['sizeOfChart'];
                        var description = result['description'];
                        var labelChartArr = result['labelChartArr'];
                        var chartInitParamObj = result['chartInitParamObj'];
                        var calenderFlag = "";
                        if (chartInitParamObj != null)
                        {
                            calenderFlag = chartInitParamObj['uuu_CalenderFlag'];
                            chartValueAsDollar = chartInitParamObj['uuu_chartValueAsDollar'];
                        }
                        $("#" + chartId + "_description").val(JSON.stringify(description));
                        $("#" + chartId + "_chartInitParams").val(JSON.stringify(chartInitParamObj));
                        $("#" + chartId + "_Data").val(dataArray);
                        $("#" + chartId + "_options").val(JSON.stringify(options));
                        var dataObjArr = {};
                        var dataObj = $("#" + chartId + "_Data").val();
                        if (dataObj != null && dataObj != '' && dataObj != undefined)
                        {
                            dataObjArr = JSON.parse(dataObj);
                        }
                        var data = [];
                        if (calenderFlag != null && calenderFlag != '' && calenderFlag == "Y") {
                            try {
                                var dataObjStr = calenderChartData(dataObjArr);
                                data = google.visualization.arrayToDataTable(dataObjStr);
                                if (chartValueAsDollar != null && chartValueAsDollar != '' && chartValueAsDollar == "Y") {
                                    var formatter = new google.visualization.NumberFormat(
                                            {prefix: '$'});
                                    formatter.format(data, 1);
                                }
                            } catch (e) {
                            }
                        } else {
                            data = google.visualization.arrayToDataTable(dataObjArr);
                            if (chartValueAsDollar != null && chartValueAsDollar != '' && chartValueAsDollar == "Y") {
                                var formatter = new google.visualization.NumberFormat(
                                        {prefix: '$'});
                                formatter.format(data, 1);
                            }
                        }
                        var objInit1 = eval(objInit);
                        var chart = objInit1;
                        if (sizeOfChart != null && sizeOfChart != '' && sizeOfChart != undefined) {
                            $("#" + chartId + "_Chart").css("width", "" + sizeOfChart + "%", "important");
                        } else if (windowWidth > 1024)
                        {
                            $("#" + chartId + "_Chart").css('width', '32%');
                            options['width'] = '100%';
                        } else if (windowWidth < 1024 && windowWidth > 700)
                        {
                            $("#" + chartId + "_Chart").css('width', '49%');
                            options['width'] = '90%';
                        } else if (windowWidth < 699 && windowWidth > 320)
                        {
                            $("#" + chartId + "_Chart").css('width', '98%');
                            options['width'] = '90%';
                        }
//                        chart.draw(data, options);
                        if (labelChartArr != null && !jQuery.isEmptyObject(labelChartArr))
                        {
                            var view = new google.visualization.DataView(data);
                            view.setColumns(labelChartArr);
                            chart.draw(view, options);
                        } else
                        {
                            chart.draw(data, options);
                        }
                        var chartType = result['chartType'];
                        if (chartType != null && chartType != '' && chartType != undefined)
                        {
                            $("#" + chartId + "_types").val(chartType);
                        }
                        var replacChartType = result['replacChartType'];
                        if (replacChartType != null && replacChartType != '' && replacChartType != undefined)
                        {
                            $("#" + chartId + "_chartType").val(replacChartType);
                        } else
                        {
                            $("#" + chartId + "_chartType").val(chartType);
                        }
                        google.visualization.events.addListener(chart, 'select', function () {
                            selectHandler(chart, data, chartId, tabChartId);
                        });
                    }

                    $("#wait").css("display", "none");
                }
            }

        },
        error: function (e) {
            sessionTimeout(e);
        }

    });
}
function navigatetoNestedComponent(chartId, selectType, selectedValue, componentId, paramArray, nestedUrl)
{
    console.log("gridId is:::" + componentId);
    if (selectedValue != null && selectedValue != '' && selectedValue != undefined)
    {
        if (componentId != null && componentId != '' && componentId != undefined)
        {
            var componentIdArr = componentId.split(";");
            for (var i = 0; i < componentIdArr.length; i++)
            {
                var componentIdStr = componentIdArr[i];
                if (componentIdStr != null && componentIdStr != '' && componentIdStr != undefined)
                {
                    if (componentIdStr.indexOf(":") > -1) {
                        var componentIdStrArr = componentIdStr.split(":");
                        if (componentIdStrArr != null && componentIdStrArr.length > 0)
                        {
                            var componentArr = componentIdStrArr[0];
                            if (componentArr != null && componentArr != '' && componentArr != undefined)
                            {
                                var componentIdArray = componentArr.split(",");
                                if (componentIdArray != null && componentIdArray.length > 0)
                                {
                                    if (componentIdArray.indexOf(selectedValue) > -1)
                                    {
                                        componentId = componentIdStrArr[1];
                                        break;
                                    }
                                }
                            }
                        }
                    } else
                    {
                        break;
                    }
                }
            }
        }
    }
    var chartItems = {};
    var filterItem;
    if (chartFilterItems != null && !jQuery.isEmptyObject(chartFilterItems))
    {
        filterItem = JSON.parse(chartFilterItems);
        for (var key in filterItem) {
            var paramObj = {};
            console.log(key + " is " + filterItem[key]);
            var value = filterItem[key];
            paramObj.column = key;
            paramObj.operator = "EQUALS";
            paramObj.value = value;
            paramArray.push(paramObj);
        }
//        chartItems.chartParamObj = paramArray;
    }
    chartItems.chartParamObj = paramArray;
    var paramStr = $("#" + chartId + "_filterVal").val();
    if (paramStr != null && paramStr != '' && paramStr != undefined)
    {
        chartItems.chartParamObj = JSON.parse(paramStr);
    }
    if (paramStr != null && paramStr != '' && paramStr != undefined && paramArray != null)
    {
        var jsonParamArr = JSON.parse(paramStr);
        for (var obj in paramArray)
        {
            jsonParamArr.push(paramArray[obj]);
        }
        chartItems.chartParamObj = jsonParamArr;
    }
    if (selectType != null && selectType != '' && (selectType == "GRID" || selectType == "FILTER_GRID"))
    {
        $("#gridId").remove();
        $("#chartParamArray").remove();
        $("#headerFlag").remove();
        $("#orgSubmitForm").attr("action", nestedUrl);
        $("#orgSubmitForm").attr("method", 'post');
        $("#orgSubmitForm").append("<input type='hidden' id='gridId' name='gridId' value='" + componentId + "'/>");
        $("#orgSubmitForm").append("<input type='hidden' name='chartParamArray' id='chartParamArray' value=''/>");
        $("#orgSubmitForm").append("<input type='hidden' id='headerFlag' name='headerFlag' value='N'/>");
        if (selectType == "FILTER_GRID")
        {
            $("#gridCompType").remove();
            $("#orgSubmitForm").append("<input type='hidden' id='gridCompType' name='gridCompType' value='" + selectType + "'/>");
        }
        $("#chartParamArray").val(JSON.stringify(chartItems));
    } else if (selectType != null && selectType != '' && selectType == "TAB")
    {
        $("#tabid").remove();
        $("#orgChartParamArray").remove();
        $("#orgSubmitForm").attr("action", nestedUrl);
        $("#orgSubmitForm").attr("method", 'post');
        $("#orgSubmitForm").append("<input type='hidden' id='tabid' name='tabid' value='" + componentId + "'/>");
        $("#orgSubmitForm").append("<input type='hidden' name='orgChartParamArray' id='orgChartParamArray' value=''/>");
        $("#orgChartParamArray").val(JSON.stringify(chartItems['chartParamObj']));
    } else if (selectType != null && selectType != '' && selectType == "CLUSTER")
    {
        $("#clusterId").remove();
        $("#orgChartParamArray").remove();
        $("#orgSubmitForm").attr("action", nestedUrl);
        $("#orgSubmitForm").attr("method", 'post');
        $("#orgSubmitForm").append("<input type='hidden' id='clusterId' name='clusterId' value='" + componentId + "'/>");
        $("#orgSubmitForm").append("<input type='hidden' name='orgChartParamArray' id='orgChartParamArray' value=''/>");
        $("#orgChartParamArray").val(JSON.stringify(chartItems['chartParamObj']));
    }

    $("#orgSubmitForm").submit();
}

function nestedPageCharts(chartId, paramArray, tabGridId)
{
    $("#wait").css("display", "block");
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        cache: false,
        url: "getAnalyticsNestedCharts",
        data: {
            'tabComponentId': chartId,
            'nestedFlag': 'Y',
            'paramArray': JSON.stringify(paramArray)
        },
        success: function (response) {
            if (response != null)
            {
                var result = JSON.parse(response);
                var dataObj = result['chartList'];
                for (var i = 0; i < dataObj.length; i++)
                {
                    var chartPrimaryIdDiv = '';
                    var chartData = dataObj[i];
                    if (i == 0)
                    {
                        var parentId = $("#" + tabGridId + "_Tab").parent().attr("id");
                        if (!(parentId != null && parentId != '' && parentId != undefined && parentId != NaN))
                        {
                            parentId = ++nestedChartLevel;
                            chartPrimaryIdDiv = "<div id ='level_" + parentId + "'><div id= '" + tabGridId + "_Tab'></div></div>";
                            $("#nestedChartDialog").append(chartPrimaryIdDiv);
                        } else
                        {
                            parentId = parentId.replace("level_", "");
                            parentId = parseInt(parentId);
                            var parentDivId = parseInt(parentId);
                            for (; parentDivId <= nestedChartLevel; parentDivId++)
                            {
                                $("#level_" + parentDivId).remove();
                            }
                            chartPrimaryIdDiv = "<div id ='level_" + parentId + "'><div id= '" + tabGridId + "_Tab'></div></div>";
                            $("#nestedChartDialog").append(chartPrimaryIdDiv);
                        }
                    }
                    var jqwidgetFlag = chartData['36']; //click event
                    if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Y')
                    {
                        getJqwidgetNestedChartData(chartData, paramArray, tabGridId);
                    } else {
                        getNestedChartData(chartData, paramArray, tabGridId);
                    }
                }
                $("#wait").css("display", "none");
            }
        },
        error: function (e) {
            sessionTimeout(e);
        }
    });
}

function navigateToNestedPageChart(chartId, paramArray, tabGridId)
{
    var dataApiObjStr = $("#dataApiObjStr").val();
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        cache: false,
        url: "getAnalyticsNestedCharts",
        data: {
            'tabComponentId': chartId,
            'nestedFlag': 'Y',
            dataApiObjStr: dataApiObjStr,
            'paramArray': JSON.stringify(paramArray)
        },
        success: function (response) {
            if (response != null)
            {
                var result = JSON.parse(response);
                var dataObj = result['chartList'];
                for (var i = 0; i < dataObj.length; i++)
                {
                    var chartPrimaryIdDiv = '';
                    var chartData = dataObj[i];
                    if (paramArray != null && paramArray != '' && paramArray != undefined)
                    {
                        var chartParamArray = JSON.parse(paramArray);
                    }
                    if (i == 0)
                    {
                        chartPrimaryIdDiv = "<div id ='level_" + nestedChartLevel + "'><div id= '" + tabGridId + "_Tab'></div></div>";
                        $("#nestedChartDialog").append(chartPrimaryIdDiv);
                        $("#level_" + nestedChartLevel).css({'float': 'left', 'width': '100%'});
                    }
                    var jqwidgetFlag = chartData['36']; //click event
                    if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Y')
                    {
                        getJqwidgetNestedChartData(chartData, chartParamArray, tabGridId);
                    } else {
                        getNestedChartData(chartData, chartParamArray, tabGridId);
                    }
                }
            }
        },
        error: function (e) {
            sessionTimeout(e);
        }
    });
}
function navigatetoNestedChart(chartId, paramArray, tabGridId)
{
    var filterItem;
    if (chartFilterItems != null && !jQuery.isEmptyObject(chartFilterItems))
    {
        filterItem = JSON.parse(chartFilterItems);
    }
    $("#nestedChartsubmitForm").attr("action", 'nestedChartData');
    $("#nestedChartsubmitForm").attr("method", 'post');
    $("#nestedChartsubmitForm").attr("target", "thatframe");
    $("#nestedtabComponentId").val(chartId);
    $("#nestedclickedChartId").val(tabGridId);
    $("#nestedPageChartParams").val(JSON.stringify(paramArray));
    $("#nestedDhChartParams").val(JSON.stringify(filterItem));
    $("#nestedChartsubmitForm").submit();
}

function getNestedChartData(chartData, paramArray, tabGridId)
{
    var dataApiObjStr = $("#dataApiObjStr").val();
    var paramsMapStr = $("#paramsMap").val();
    var data = {
        'chartData': chartData,
        'nestedFlag': 'Y',
        'paramArray': JSON.stringify(paramArray),
        dataApiObjStr: dataApiObjStr

    };
    if (paramsMapStr != null && paramsMapStr != '') {
        var paramsMap = JSON.parse(paramsMapStr);
        if (paramsMap != null && !jQuery.isEmptyObject(paramsMap)) {
            for (var paramName in paramsMap) {
                data[paramName] = paramsMap[paramName];
            }
        }
    }

    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        url: "getAnalyticCharts",
        async: true,
        cache: false,
        data: data,
        success: function (response) {
            if (response != null && response != '' && response != undefined) {
                $("#wait").css("display", "block");
                var resultData = JSON.parse(response);
                if (resultData != null)
                {
                    for (var i = 0; i < resultData.length; i++) {
                        var result = resultData[i];
                        if (result != null) {
                            var divid = result['divId'];
                            var chartId = result['chartId'];
                            $("#" + tabGridId + "_Tab").append(divid);
                            $("#" + tabGridId + "_Tab").append("<input type='hidden' id='" + chartId + "_Nested_filterVal' value=''/>");
                            //$("#nestedChartDialog").append("<input type='hidden' id='" + chartId + "_tabGridId' value='" + tabGridId + "'/>");
                            $("#" + tabGridId + "_Tab").append("<input type='hidden' id='" + chartId + "_Nested_flag' value='Y'/>");
                            var dataArray = result['dataArray'];
                            var options = result['options'];
                            var objInit = result['objInit'];
                            var seqNo = result['seqNo'];
                            var sizeOfChart = result['sizeOfChart'];
                            var description = result['description'];
                            var chartInitParamObj = result['chartInitParamObj'];
                            var calenderFlag = "";
                            if (chartInitParamObj != null)
                            {
                                calenderFlag = chartInitParamObj['uuu_CalenderFlag'];
                            }
                            $("#" + chartId + "_description").val(JSON.stringify(description));
                            $("#" + chartId + "_chartInitParams").val(JSON.stringify(chartInitParamObj));
                            $("#" + chartId + "_Data").val(dataArray);
                            $("#" + chartId + "_options").val(JSON.stringify(options));
                            $("#" + chartId + "_filterVal").val(JSON.stringify(paramArray));
                            $("#" + chartId + "_Nested_filterVal").val(JSON.stringify(paramArray));
                            var dataObjArr = {};
                            var dataObj = $("#" + chartId + "_Data").val();
                            if (dataObj != null && dataObj != '' && dataObj != undefined)
                            {
                                dataObjArr = JSON.parse(dataObj);
                            }
                            var data = [];
                            if (dataObjArr != null && dataObjArr.length > 0) {
                                if (calenderFlag != null && calenderFlag != '' && calenderFlag == "Y") {
                                    try {
                                        var dataObjStr = calenderChartData(dataObjArr);
                                        data = google.visualization.arrayToDataTable(dataObjStr);
                                    } catch (e) {
                                    }
                                } else {
                                    data = google.visualization.arrayToDataTable(dataObjArr);
                                }
                                var objInit1 = eval(objInit);
                                var chart = objInit1;
                                if (sizeOfChart != null && sizeOfChart != '' && sizeOfChart != undefined) {
                                    $("#" + chartId + "_Chart").css("width", "" + sizeOfChart + "%", "important");
                                } else if (windowWidth > 1024)
                                {
                                    $("#" + chartId + "_Chart").css('width', '32%');
                                    options['width'] = '100%';
                                } else if (windowWidth < 1024 && windowWidth > 700)
                                {
                                    $("#" + chartId + "_Chart").css('width', '49%');
                                    options['width'] = '90%';
                                } else if (windowWidth < 699 && windowWidth > 320)
                                {
                                    $("#" + chartId + "_Chart").css('width', '98%');
                                    options['width'] = '90%';
                                }
                                chart.draw(data, options);
                                var chartType = result['chartType'];
                                if (sizeOfChart != null && sizeOfChart != '' && sizeOfChart != undefined) {
                                    $("#" + chartId + "_Chart").css("width", "" + sizeOfChart + "%", "important");
                                }
                                if (chartType != null && chartType != '' && chartType != undefined)
                                {
                                    $("#" + chartId + "_types").val(chartType);
                                }
                                var replaceChartType = result['replacChartType'];
                                if (replaceChartType != null && replaceChartType != '' && replaceChartType != undefined)
                                {
                                    $("#" + chartId + "_chartType").val(replaceChartType);
                                } else
                                {
                                    $("#" + chartId + "_chartType").val(chartType);
                                }
                                var tabcomponentId = result['tabGridId'];
                                google.visualization.events.addListener(chart, 'select', function () {
                                    selectHandler(chart, data, chartId, tabcomponentId);
                                });
                            } else
                            {
                                if (sizeOfChart != null && sizeOfChart != '' && sizeOfChart != undefined) {
                                    $("#" + chartId + "_Chart").css("width", "" + sizeOfChart + "%", "important");
                                }
                                $("#" + chartId + "_Chart").html("<div class ='analyticChartNoDataClass' style ='width:" + options['width'] + "px;'><span class='analyticsChartNoDataText'>No Data Found</span></div>");
                            }

                        }

                        $("#wait").css("display", "none");
                    }
                    $("#wait").css("display", "none");
                }
            }

        },
        error: function (e) {
            sessionTimeout(e);
        }

    });
}
function selectHandler(chart, data, chartId, tabGridId) {
    try {
        var selection = chart.getSelection();
        var message = '';
        var value;
        var count;
        var paramArray = [];
        var paramObj = {};
        for (var i = 0; i < selection.length; i++) {
            var item = selection[i];
            if (item.row != null && item.column != null) {
                var columnNames = $("#" + chartId + "_columnName").val();
                if (columnNames != null && columnNames != '' && columnNames != undefined)
                {
                    var columnVal = item.column - 1;
                    var colsArr = columnNames.split(",");
                    paramObj.column = $.trim(colsArr[columnVal]);
                }
                count = data.getFormattedValue(item.row, item.column);
                value = data.getValue(chart.getSelection()[0].row, 0);
            } else if (item.row != null) {
                var columnNames = $("#" + chartId + "_columnName").val();
                if (columnNames != null && columnNames != '' && columnNames != undefined)
                {
                    var colsArr = columnNames.split(",");
                    paramObj.column = $.trim(colsArr[0]);
                }
                count = data.getValue(chart.getSelection()[0].row, 1);
                value = data.getFormattedValue(item.row, 0);
            } else if (item.column != null) {
                value = data.getFormattedValue(0, item.column);
            }
            paramObj.value = $.trim(value);
            paramObj.operator = "EQUALS";
            paramArray.push(paramObj);
            var nestedId = $("#" + chartId + "_nestedId").val();
            var chartNestedType = $("#" + chartId + "_nestedType").val();
            if (chartNestedType != null && chartNestedType != '' && chartNestedType != undefined) {
                var nestedType = chartNestedType.split("?")[0];
                var nestedUrl = chartNestedType.split("?")[1];
                if (nestedType != null && nestedType != '' && nestedType == "CHART")
                {
                    var nestedChartFlag = $("#" + chartId + "_Nested_flag").val();
                    if (nestedChartFlag != null && nestedChartFlag != '' && nestedChartFlag == "Y")
                    {
                        nestedPageCharts(nestedId, paramArray, tabGridId);
                    } else
                    {
                        navigatetoNestedChart(nestedId, paramArray, tabGridId);
                    }

                } else if (nestedType != null && nestedType != '' && nestedType != undefined)
                {
                    navigatetoNestedComponent(chartId, nestedType, value, nestedId, paramArray, nestedUrl);
                }
            }

        }
        chart.setSelection([]);
    } catch (e)
    {

    }

}

function chartdialogopen(chartId)
{
//    google.charts.setOnLoadCallback(function () {
//        openChartDialog(chartId);
//    });
    openChartDialog(chartId);
}

function flip(chartId) {
    var description = $("#" + chartId + "_description").val();
    var descr = JSON.parse(description);
    var screenWidth = (descr.length > 100) ? '800' : 'auto';
    var screenHeigth = (descr.length > 400) ? '400' : 'auto';
    $("#dialog").html(descr);
    $("#dialog").dialog({
        modal: true,
        title: (labelObject['Description'] != null ? labelObject['Description'] : 'Description'),
        height: screenHeigth,
        minHeight: 'auto',
        minWidth: screenWidth,
        maxWidth: 'auto',
        fluid: true,
        buttons: {
            Close: function () {
                $(this).html("");
                $(this).dialog("close");
                $(this).dialog("destroy");
            }
        },
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

function openChartDialog(chartId) {
    var widthScreen;
    var heightScreen;
    var widthChartScreen;
    var heightChartScreen;
    var dataObjArr = {};
    var dataObj = $("#" + chartId + "_Data").val();
    var chartInitParams = $("#" + chartId + "_chartInitParams").val();
    var chartInitParamObj = {};
    if (chartInitParams != null && chartInitParams != '' && chartInitParams != undefined)
    {
        chartInitParamObj = JSON.parse(chartInitParams);
    }
    var calenderFlag = "";
    var orgChartFlag = "";
    if (chartInitParamObj != null)
    {
        calenderFlag = chartInitParamObj['uuu_CalenderFlag'];
        orgChartFlag = chartInitParamObj['uuu_orgChartFlag'];
    }
    var chartType = $("#" + chartId + "_chartType").val();
    if (dataObj != null && dataObj != '' && dataObj != undefined)
    {
        dataObjArr = JSON.parse(dataObj);
    }
    var data = [];
    if (calenderFlag != null && calenderFlag != '' && calenderFlag == "Y") {
        try {
            var dataObjStr = calenderChartData(dataObjArr);
            data = google.visualization.arrayToDataTable(dataObjStr);
        } catch (e) {
        }
    } else {
        data = google.visualization.arrayToDataTable(dataObjArr);
    }
    var optionsData = $("#" + chartId + "_options").val();
    var options = {};
    widthScreen = $(window).width() - 30;
    heightScreen = $(window).height() - 30;
    widthChartScreen = widthScreen - 100;
    heightChartScreen = heightScreen - 100;
    if (optionsData != null && optionsData != '' && optionsData != undefined)
    {
        options = JSON.parse(optionsData);
        options['height'] = heightChartScreen;
        options['width'] = windowWidth - 100;
    }
    $("#expandDialog").html("<div id = '" + chartId + "_Expand' class = 'visionDataAnalyticsChartExpandImage'></div>");
    $("#expandDialog").dialog({
        title: 'Expand Image',
        modal: true,
        width: widthScreen,
        height: heightScreen,
        fluid: true,
        buttons: [
            {
                text: "Close",
                "class": 'dialogyes',
                click: function () {
                    $(this).html("");
                    $(this).dialog("close");
                    $(this).dialog("destroy");
                }
            }
        ],
        open: function (event, ui)
        {
            $(".visionHeaderMain").css("z-index", "999");
            $(".visionFooterMain").css("z-index", "999");
            $(this).closest(".ui-dialog").addClass("visionAnalyticgraphDialog");
            $(window).resize(function () {
                $(".visionReportFlipcontainer").css("width", "100%");
            }).resize();
        },
        beforeClose: function (event, ui)
        {
            $(".visionHeaderMain").css("z-index", "99999");
            $(".visionFooterMain").css("z-index", "99999");
        }
    });
    var chart = "new google.visualization." + chartType + "(document.getElementById('" + chartId + "_Expand'))";
    var chart1 = eval(chart);
    if (orgChartFlag != null && orgChartFlag != '' && orgChartFlag == "Y") {
        var rows = data.getNumberOfRows();
        options['allowCollapse'] = false;
        for (var k = 0; k < rows; k++)
        {
            var orgValue = data.getValue(k, 0);
            var color = $("#color_" + orgValue).val();
            data.setRowProperty(k, 'style', 'background-color:' + color + ';background-image:none');
        }
    }
    chart1.draw(data, options);
}
$(window).resize(function () {
    var windowWidth = $(this).width() - 30;
    var windowHeight = $(this).height() - 30;
    $(".visionAnalyticgraphDialog").css({
        width: windowWidth + "px",
        height: windowHeight + "px"
    });
}).resize();
function chartCheckBoxProcess(tabComponentId) {
    console.log("iam in chartCheckBoxProcess");
    var checkBoxIds = $('.visionAnalyticCheckBoxVal:checked').map(function () {
        return this.value;
    }).get();
    if (checkBoxIds != null && checkBoxIds.length > 0) {
        $.ajax({
            type: "post",
            traditional: true,
            dataType: 'html',
            cache: false,
            url: "dataAnalyticsCharts",
            data: {
                'tabComponentId': tabComponentId,
                'chartId': JSON.stringify(checkBoxIds),
                'checkBoxFlag': 'Y'
            },
            success: function (response) {
                $("#wait").css("display", "none");
                if (response != null)
                {
                    var result = JSON.parse(response);
                    var dataObj = result['chartList'];
                    var tabId = $("#tabChartsId").val();
                    $("#" + tabId).empty();
                    $("#" + tabId + "div").remove();
                    $("#startIndex").val(checkBoxIds.length);
                    var startIndex = $("#startIndex").val();
                    var pageSize = $("#pageSize").val();
                    if (startIndex != null && startIndex != '' && startIndex != undefined && pageSize != null && pageSize != '' && pageSize != undefined)
                    {
                        $("#endIndex").val(parseInt(startIndex) + parseInt(pageSize));
                    }
                    scrollStartIndexFlag = "Y";
                    if (dataObj != null) {
                        seqArray = [];
                        seqObj = {};
                        seqObj["TAB_ID"] = tabId;

                        getChartByChart(dataObj[0], tabId, 0, dataObj);

//                        for (var i = 0; i < dataObj.length; i++)
//                        {
//                            var chartData = dataObj[i];
//                            $("#wait").css("display", "block");
//                            getChartByChart(chartData, tabId);
//                        }
                    }
                }
            },
            error: function (e) {
                sessionTimeout(e);
            }
        });
    } else {
        $("#wait").css("display", "none");
        var dialogSplitMessage = dialogSplitIconText("Please provide at least one value to Process.", "Y");
        showDialog(dialogSplitMessage);
    }

}


function getChartsonScroll(tabId, tabComponentId, startIndex, endIndex, pageSize) {
    console.log("iam in getChartsOnScroll");
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        cache: false,
        url: "dataAnalyticsChartsonScroll",
        data: {
            'tabChartId': tabId,
            'tabComponentId': tabComponentId,
            'startIndex': startIndex,
            'endIndex': endIndex,
            'pageSize': pageSize
        },
        success: function (response) {
            $("#wait").css("display", "none");
            if (response != null)
            {
                var result = JSON.parse(response);
                var dataObj = result['chartList'];
                var startIndex = result['startIndex'];
                var endIndex = result['endIndex'];
                var pageSize = result['pageSize'];
                var checkBoxStr = result['checkBoxStr'];
                $("#startIndex").val(startIndex);
                $("#endIndex").val(endIndex);
                $("#pageSize").val(pageSize);
                $("#visionAnalyticCheckBoxData").empty();
                $("#visionAnalyticCheckBoxData").html(checkBoxStr);
                if (dataObj != null) {
                    seqObj["TAB_ID"] = tabId;
                    getChartByChart(dataObj[0], tabId, 0, dataObj);
//                    for (var i = 0; i < dataObj.length; i++)
//                    {
//                        var chartData = dataObj[i];
//                        $("#wait").css("display", "block");
//                        getChartByChart(chartData, tabId);
//                    }
                }
            }
        },
        error: function (e) {
            sessionTimeout(e);
        }
    });
}


$(function () {
    $("#accordion").accordion({
        active: 0
    });
    $('div.visionAnalyticChartsTab').scroll(function () {
        var scrollFlag = $("#scrollFlag").val();
        if (scrollFlag != null && scrollFlag != '' && scrollFlag == "Y") {
            if (($(this).scrollTop() + $(this).innerHeight() >= $(this)[0].scrollHeight) && ($(this)[0].scrollHeight > $(this).innerHeight())) {
                console.log("iam in scroll functionality...........");
                var tabChartId = $("#tabChartsId").val();
                var tabComponentId = $("#tabComponentId").val();
                var startIndex = $("#startIndex").val();
                var endIndex = $("#endIndex").val();
                var pageSize = $("#pageSize").val();
                var totalCount = $("#totalCount").val();
                if (startIndex != null && startIndex != '' && startIndex != undefined && parseInt(startIndex) >= 0) {
                    if (scrollStartIndexFlag != null && scrollStartIndexFlag != '' && scrollStartIndexFlag == "N")
                    {
                        startIndex = (parseInt(startIndex) + parseInt(pageSize));
                    }
                    endIndex = (parseInt(startIndex) + parseInt(pageSize));
                    if (totalCount != null && totalCount != '' && parseInt(totalCount) >= parseInt(startIndex)) {
                        getChartsonScroll(tabChartId, tabComponentId, startIndex, endIndex, pageSize);
                        scrollStartIndexFlag = "N";
                    }
                }
            }
        }
    });
});
function formAnalytic(tabId, gridId) {
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    var basicIds = [];
    chartFilterItems = {};
    $("#mat_creation_form_table :input").each(function () {
        var textid = $(this).attr("id");
        var displayAttr = $("#" + textid).attr("display");
        var type = $(this).attr("type");
        var textval = $(this).val();
        if (type != null && type != "" && type != undefined && type != 'hidden' && type != 'button') {
            if (textval != null && textval != '') {
                textval = textval.toUpperCase();
            }
            if (type != null && type == 'checkbox') {//
                if ($("#" + textid).is(':checked')) {
                    textval = "Y";
                } else {
                    textval = "N";
                }
            }
            basicIds.push(textid);
            if (textval != null && textval != '') {
                chartFilterItems[textid] = textval;
            }
        }

    });
    if (chartFilterItems != null && !jQuery.isEmptyObject(chartFilterItems))
    {
        chartFilterItems = JSON.stringify(chartFilterItems);
    }
    chartsData(tabId, gridId, "N");
}


function getcockpitFilterForm(gridId)
{
    console.log("gridId is :::" + gridId);
    $.ajax({
        type: "post",
        traditional: true,
        // dataType: 'json',
        url: "getCockitAnalyticFilterForm",
        cache: false,
        data: {
            'gridId': gridId
        },
        success: function (response) {
            if (response != null && response != '') {
                var filterFormObj = JSON.parse(response);
                $("#cockpitFilterForm").show();
                $("#cockpitChartGridForm").hide();
                $("#cockpitGridForm").hide();
                $("#cockpitFilterForm").html(filterFormObj['formStr']);
                $("#settings_panel").html(filterFormObj['persData']);
            }

        },
        error: function (e) {
            sessionTimeout(e);
        }
    });
}
function onReportSearch(gridId)
{
    var paramObj = {};
    $("#mat_cock_pit_table tbody tr").each(function () {
        var colId = $(this).attr('id');
        if ($("#" + colId).is(":visible")) {
            colId = colId.replace("_tr_Form", "");
            paramObj.vnamesearchtype = $("#vnamesearchtype_" + colId).val();
            paramObj[colId] = $("#" + colId).val();
        }
    });
    var sapNumber = $("#sapNumber").val();
    if (sapNumber != null && sapNumber != '' && sapNumber != undefined) {
        $.ajax({
            type: "post",
            traditional: true,
            url: "getCockitAnalyticGridForm",
            cache: false,
            data: {
                'gridId': gridId,
                'erpNo': $("#sapNumber").val(),
                'Plant': $("#Plant").val(),
                'erpNosearchType': $("#vnamesearchtype_sapNumber").val(),
                'PlantsearchType': $("#vnamesearchtype_Plant").val()
            },
            success: function (response) {
                if (response != null && response != '') {
                    var gridFormObj = JSON.parse(response);
                    // $("#cockpitFilterForm").hide(); 
                    $("#cockpitGridForm").html('');
                    $("#cockpitChartsDiv").html('');
                    var formObj = gridFormObj['formStr'];
                    if (formObj != null && formObj != "" && formObj != undefined)
                    {
                        $("#cockpitChartGridForm").show();
                        $("#cockpitGridForm").show();
                        $("#cockpitGridForm").html(gridFormObj['formStr']);
                        var analyticId = gridFormObj['analyticId'];
                        if (analyticId != null && analyticId != '' && analyticId != undefined) {
                            $("#cockpitChartsDiv").html("<div id='" + analyticId + "'></div>");
                            chartFilterItems = JSON.stringify(paramObj);
                            chartsData(analyticId, analyticId, "N");
                        }

                    } else
                    {
                        $("#cockpitChartGridForm").hide();
                        var errMsg = gridFormObj['errMsg'];
                        var dialogSplitMessage = dialogSplitIconText(errMsg, "Y");
                        showDialog(dialogSplitMessage);
                    }

                }

            },
            error: function (e) {
                sessionTimeout(e);
            }
        });
    } else
    {
        var dialogSplitMessage = dialogSplitIconText("Please Enter SAP Number.", "Y");
        showDialog(dialogSplitMessage);
    }
}
function slideSettings() {
    $('#settings_panel').toggle('slide', {direction: 'right'}, 500);
    $("#personalizeid").toggleClass("ui-icon-triangle-1-s");
}
function showDialog(message)
{
    cockpitViewErrorFlag = true;
    $("#dialog1").append(message);
    $("#dialog1").dialog({
        title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
        modal: true,
        height: 140,
        width: 330,
        fluid: true,
        buttons: [
            {
                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                click: function () {
                    ajaxStop();
                    $("#dialog1").empty();
                    $("#dialog1").dialog('close');
                    $("#dialog").empty();
                    $("#dialog").dialog('close');
                }
            }
        ],
        open: function () {
            $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
            //    $(this).closest(".ui-dialog").find(".ui-button").eq(2).addClass("dialogno");
            $(this).closest(".ui-dialog").addClass("visionSearchSaveDialog");
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

function navigateToCockpitChartGrid(chartType, tabGridId, chartId, gridId)
{
    try {
        $("#submitForm").empty();
        $("#cockPitRecordNo").remove();
        $("#gridId").remove();
        $("#auditGridId").remove();
        $("#cockPitChartType").remove();
        $("#cockPitChartParams").remove();
    } catch (e) {
    }
    var paramObj = {};
    $("#mat_cock_pit_table tbody tr").each(function () {
        var colId = $(this).attr('id');
        if ($("#" + colId).is(":visible")) {
            colId = colId.replace("_tr_Form", "");
            paramObj.vnamesearchtype = $("#vnamesearchtype_" + colId).val();
            paramObj[colId] = $("#" + colId).val();
        }
    });
    paramObj = JSON.stringify(paramObj);
    $("#submitForm").attr("action", 'grid');
    $("#submitForm").attr("method", 'post');
    $("#submitForm").attr("target", 'thatframe');
    var csrfToken = $("input[name='_csrf']").val();
    if (csrfToken != null && csrfToken != '') {
        var csrf = "<input type='hidden' name='_csrf' value='" + csrfToken + "'/>";
        $("#submitForm").append(csrf);
    }
    $("#submitForm").append("<input type='hidden' name='gridId' value='" + gridId + "'/>");
    $("#submitForm").append("<input type='hidden' name='auditGridId' value='" + gridId + "'/>");
    $("#submitForm").append("<input type='hidden' name='cockPitRecordNo' value='" + $("#sapNumber").val() + "'/>");
    $("#submitForm").append("<input type='hidden' name='cockPitChartType' value='" + chartId + "'/>");
    $("#submitForm").append("<input type='hidden' name='cockPitChartParams' value='" + paramObj + "'/>");
    $("#submitForm").submit();
}

function updateGridPersonalize(tbid) {
    console.log("tbid::" + tbid);
    console.log("datacol:::" + $("#" + tbid).attr("data-col"));
    var columnName = tbid + "_Form";
    if ($("#" + tbid).prop('checked')) {
        $("#" + columnName).show();
    } else {
        $("#" + columnName).hide();
    }
}

function AddKeyPress(event) {
    event = event || window.event;
    if (event.keyCode == 13) {
        document.getElementById('vendorSearch').click();
        return false;
    }
    return true;
}

function reportclearSearch()
{
    $("#sapNumber").val('');
    $("#Plant").val('');
    $("#Warehouse").val('');
    $("#StorageLocation").val('');
    $("#vnamesearchtype_sapNumber").val()
    $("#vnamesearchtype_Plant").val()
    $("#vnamesearchtype_Warehouse").val()
    $("#vnamesearchtype_StorageLocation").val()
    $("#cockpitGridForm").hide();
    $(".visionCockpitSpecTabInner").hide();
    $("#cockpitChartsDiv").hide();
    $("#cockpitGridForm").html("");
    $("#cockpitChartsDiv").html("");
}

function navigateToOrgChart(chart, data, tabGridId, parentId)
{
    var row = chart.getSelection()[0].row;
    var value = data.getValue(row, 0);
    console.log("value  is ::" + value);
    var compUrl;
    var compClickType;
    var compClickTypeId = $("#compType_" + value).val();
    if (compClickTypeId != null && compClickTypeId != '' && compClickTypeId != undefined)
    {
        compClickType = compClickTypeId.split("?")[0];
        compUrl = compClickTypeId.split("?")[1];
    }
    var compClickId = $("#compId_" + value).val();
    if (compUrl != null && compUrl != '' && compUrl != undefined) {
        $("#orgSubmitForm").attr("action", compUrl);
        $("#orgSubmitForm").attr("method", 'post');
        $("#orgSubmitForm").attr("target", 'thatframe');
        if (compClickType != null && compClickType != '' && compClickType != undefined && compClickType == "TAB")
        {
            $("#tabid").remove();
            $("#orgChartParamArray").remove();
            $("#orgSubmitForm").append("<input type='hidden' id='tabid' name='tabid' value='" + compClickId + "'/>");
            $("#orgSubmitForm").append("<input type='hidden' name='orgChartParamArray' id='orgChartParamArray' value=''/>");
            var paramArray = $("#paramArray_" + value).val();
            $("#orgChartParamArray").val(paramArray);
        } else if (compClickType != null && compClickType != '' && compClickType != undefined && (compClickType == "GRID" || compClickType == "FILTER_GRID"))
        {
            var paramArray = $("#paramArray_" + value).val();
            var chartItems = {};
            if (paramArray != null && paramArray != '' && paramArray != undefined)
            {
                paramArray = JSON.parse(paramArray);
                chartItems.chartParamObj = paramArray;
            }
            $("#gridId").remove();
            $("#chartParamArray").remove();
            $("#headerFlag").remove();
            $("#orgSubmitForm").append("<input type='hidden' id='headerFlag' name='headerFlag' value='N'/>");
            $("#orgSubmitForm").append("<input type='hidden' name='chartParamArray' id='chartParamArray' value=''/>");
            $("#chartParamArray").val(JSON.stringify(chartItems));
            if (compClickType == "FILTER_GRID")
            {
                $("#gridCompType").remove();
                $("#orgSubmitForm").append("<input type='hidden' id='gridCompType' name='gridCompType' value='" + compClickType + "'/>");
            }
            $("#orgSubmitForm").append("<input type='hidden' id='gridId' name='gridId' value='" + compClickId + "'/>");
        } else if (compClickType != null && compClickType != '' && compClickType != undefined && compClickType == "CLUSTER")
        {
            $("#clusterId").remove();
            $("#chartParamArray").remove();
            $("#orgSubmitForm").append("<input type='hidden' id='clusterId' name='clusterId' value='" + compClickId + "'/>");
            $("#orgSubmitForm").append("<input type='hidden' name='orgChartParamArray' id='orgChartParamArray' value=''/>");
            var paramArray = $("#paramArray_" + value).val();
            $("#orgChartParamArray").val(paramArray);
        } else if (compClickType != null && compClickType != '' && compClickType != undefined && compClickType == "FORM")
        {
            $("#items").remove();
            $("#orgSubmitForm").append("<input type='hidden' id='items' name='items' value=''/>");
            var paramArray = $("#paramArray_" + value).val();
            $("#items").val(paramArray);
        }
        $("#orgSubmitForm").submit();
    }
    chart.setSelection([]);
}

function orgChart(chart, data, tabGridId, parentId, options, event, eventTarget)
{
//    chartStartTabLoader();
    if (window.getSelection) {
        window.getSelection().removeAllRanges();
    } else if (document.selection) {
        document.selection.empty();
    }
    var row = chart.getSelection()[0].row;
    var value = data.getValue(row, 0);
    var colorCode = "#0071c5";
    var childId = $("#levelId_" + value).val();
    var whereCondCol = $("#whereCondCol_" + value).val();
    var parentVal = $("#parent_" + value).val();
//    var childId = $("#child_" + value).val();
    if (parentObj[value + ":" + parentVal] == null || parentObj[value + ":" + parentVal] == "" || parentObj[value + ":" + parentVal] == undefined) {
        console.log(value + ":" + parentVal + ":::" + parentObj[value + ":" + parentVal]);
        parentObj[value + ":" + parentVal] = true;
        var obj = [];
        var ObjData = [];
        var colName = data.getColumnLabel(0);
        var ObjeData = $("#" + parentId + "_Data").val();
        ObjData = JSON.parse(ObjeData);
        if (childId != null && childId != '' && childId != undefined) {
            $.ajax({
                type: "post",
                traditional: true,
                dataType: 'html',
                cache: false,
                url: "nestedOrgData",
                data: {
                    'tabComponentId': tabGridId,
                    'childId': childId,
                    'params': whereCondCol,
                    'dependData': value,
//                    'columName': colName,
                    'parentId': parentId,
                    'chartData': JSON.stringify(ObjData)
                },
                success: function (response) {
                    if (response != null && response != '' && response != undefined)
                    {
                        var responseObj = JSON.parse(response);
                        var orgData = responseObj['data'];
                        var count = responseObj['size'];
                        var rows = data.getNumberOfRows();
                        if (orgData != null && orgData != '' && orgData != undefined)
                        {
                            obj = JSON.parse(orgData);
                            if (obj != null && !jQuery.isEmptyObject(obj)) {
                                for (var j = 0; j < obj.length; j++)
                                {
                                    ObjData.push(obj[j]);
                                }
                                data.addRows(obj);
                                $("#" + parentId + "_Data").val(JSON.stringify(ObjData));
                                //data.insertRows(row, obj);
                                chart.draw(data, {allowHtml: true, allowCollapse: true});
                                for (var i = 0; i < count; i++)
                                {
                                    var dataValue = data.getValue(rows, 0);
                                    var colorValue = $("#color_" + dataValue).val();
                                    if (colorValue != null && colorValue != '' && colorValue != undefined)
                                    {
                                        data.setRowProperty(rows, 'style', 'background-color:' + colorValue + ';background-image:none');
                                    }
                                    rows++;
                                }
                                data.setRowProperty(row, 'style', 'background-color:' + colorCode + ';background-image:none');
                                chart.draw(data, {allowHtml: true, allowCollapse: true});
                                $("#" + parentId + "_Chart").css('height', options['height'], "important");
                                for (var expandObj in parentObj)
                                {
                                    var expandValue = expandObj.split(":")[0];
                                    $("#" + expandValue + "_down").hide();
                                    $("#" + expandValue + "_up").show();
                                    if ($("#fOrgValue_" + expandValue).hasClass('visionNoAnchorOrgChart'))
                                    {
                                        $("#fOrgValue_" + expandValue).removeClass("visionNoAnchorOrgChart");
                                        $("#fOrgValue_" + expandValue).addClass("visionNoAnchorChangeOrgChart");
                                    } else
                                    {
                                        $("#fOrgValue_" + expandValue).removeClass("visionAnchorOrgChart");
                                        $("#fOrgValue_" + expandValue).addClass("visionAnchorChangeOrgChart");
                                    }
                                }
                                //------for showing in the middle the clicked node

                                var targetNodeDivId = $(event.target).attr('id');
                                if (targetNodeDivId == null)
                                    targetNodeDivId = $(event.target).next().attr('id');
                                if (targetNodeDivId == null)
                                    targetNodeDivId = $(event.target).prev().attr('id');
                                if (targetNodeDivId == null)
                                    targetNodeDivId = $(event.target).find('div').attr('id');
                                var nodeAfter = $("#" + targetNodeDivId).closest(".google-visualization-orgchart-node");
                                var tdPrevArray = $(nodeAfter[0]).prevAll('td');
                                var colSpan = 0;
                                var scrollWidth = $(eventTarget)[0].scrollWidth;
                                for (i = 0; i < tdPrevArray.length; i++) {
                                    colSpan = colSpan + parseInt(tdPrevArray[i].colSpan);
                                }
                                var nodeTop = $(nodeAfter[0]);
                                try {
                                    var top = nodeTop[0].offsetTop;
                                } catch (e) {
                                }
                                $("#" + parentId + "_Chart").animate({
                                    scrollLeft: 0
                                }, 100);
                                $("#" + parentId + "_Chart").animate({
                                    scrollLeft: (colSpan - 3) * 16.30 - 500
                                }, 100);
                                $("#" + parentId + "_Chart").animate({
                                    scrollTop: top
                                }, 100);


                            } else
                            {
                                delete parentObj[value + ":" + parentVal];
                            }
                        }
                    }
                    chartEndTabLoader();
                },
                error: function (e) {
                    sessionTimeout(e);
                    chartEndTabLoader();
                }
            });
        }
    } else
    {
        var counter = 0;
        var childs1 = [];
        console.log(value + "_" + parentVal + ":::" + parentObj[value + ":" + parentVal]);
        delete parentObj[value + ":" + parentVal];
        childs1 = chart.getChildrenIndexes(row);
        counter = childs1.length;
        for (var i = 0; i < counter; i++)
        {
            var childVal = data.getValue(childs1[i], 0);
            if (childVal != null && childVal != '' && childVal != undefined)
            {
                var parentVal = $("#parent_" + childVal).val();
                delete parentObj[childVal + ":" + parentVal];
            }
            var childs2 = chart.getChildrenIndexes(childs1[i]);
            if (childs2 != null && childs2.length > 0)
            {
                childs1 = childs1.concat(childs2);
                counter += childs2.length;
            }
        }
        childs1 = childs1.sort(function (a, b) {
            return a - b;
        });
        if (childs1.length > 0) {
            var ObjeData = $("#" + parentId + "_Data").val();
            ObjData = JSON.parse(ObjeData);
            for (var i = childs1.length - 1; i >= 0; i--)
            {
                data.removeRow(childs1[i]);
                ObjData.splice(childs1[i] + 1, 1);
            }
            $("#" + parentId + "_Data").val(JSON.stringify(ObjData));
            var colorValue = $("#color_" + value).val();
            data.setRowProperty(row, 'style', 'background-color:' + colorValue + ';background-image:none');
            chart.draw(data, {allowHtml: true, allowCollapse: true});
            for (var expandObj in parentObj)
            {
                var expandValue = expandObj.split(":")[0];
                $("#" + expandValue + "_up").show();
                $("#" + expandValue + "_down").hide();
                if ($("#fOrgValue_" + expandValue).hasClass('visionNoAnchorOrgChart'))
                {
                    $("#fOrgValue_" + expandValue).removeClass("visionNoAnchorOrgChart");
                    $("#fOrgValue_" + expandValue).addClass("visionNoAnchorChangeOrgChart");
                } else if ($("#fOrgValue_" + value).hasClass('visionAnchorOrgChart'))
                {
                    $("#fOrgValue_" + expandValue).removeClass("visionAnchorOrgChart");
                    $("#fOrgValue_" + expandValue).addClass("visionAnchorChangeOrgChart");
                }
            }

            if ($("#fOrgValue_" + value).hasClass('visionNoAnchorChangeOrgChart'))
            {
                $("#fOrgValue_" + value).removeClass("visionNoAnchorChangeOrgChart");
                $("#fOrgValue_" + value).addClass("visionNoAnchorOrgChart");
            } else if ($("#fOrgValue_" + value).hasClass('visionAnchorChangeOrgChart'))
            {
                $("#fOrgValue_" + value).removeClass("visionAnchorChangeOrgChart");
                $("#fOrgValue_" + value).addClass("visionAnchorOrgChart");
            }
        }
        counter = 0;
        childs1 = [];
        chartEndTabLoader();
    }
    chart.setSelection([]);
}

//for funnelCharts
function getJqwidgetChart(chartData, tabId, i, chartDataList)
{
    var tabHeight = $("#tabHeight").val(); //cube mdrm
    if (tabHeight != null && tabHeight != '' && tabHeight != undefined)
    {
        $('#jqxTabs').jqxTabs({height: tabHeight + "px"});
    }
    $(".visionReportandDashboard").css("height", "250px", "important");
    $("#wait").css("display", "block");
    var filterItem;
    if (chartFilterItems != null && !jQuery.isEmptyObject(chartFilterItems))
    {
        filterItem = JSON.parse(chartFilterItems);
    }
    i++;
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        url: "getJqwidgetAnalyticCharts",
        cache: false,
        data: {
            'chartData': chartData,
            'chartFilterItems': JSON.stringify(filterItem)
        },
        success: function (response) {
            $("#wait").css("display", "none");
            if (response != null && response != '' && response != undefined) {
                try {
                    var resultData = JSON.parse(response);
                    if (resultData != null)
                    {
                        var result = resultData;
                        if (result != null) {
                            var chartTypesObj = result['chartTypesObj'];
                            var chartsObj = result['chartsObj'];
                            var divid = chartsObj['divId'];
                            var seqNo = chartsObj['seqNo'];
                            var chartId = chartsObj['chartId'];
                            var sizeOfChart = chartsObj['sizeOfChart'];
                            var height = chartsObj['height'];
                            var width = chartsObj['width'];
                            var tabComponentId = chartsObj['tabGridId'];
                            var flag = chartData[36];
                            $("#" + chartId).attr("jqwidgetChartFlag", flag);
                            $("#" + chartId).attr("jqwidgetChartFlag");
//                            $("#" + chartId).jqxChart(settings);

                            // var tabId = result['tabId'];
                            //-----------------------------
                            var insertDivBefore = 0;
                            for (var j = 0; j < seqArray.length; j++) {
                                if (parseInt(seqNo) < seqArray[j]) {
                                    insertDivBefore = seqArray[j];
                                    break;
                                }
                            }
                            if (insertDivBefore == 0) {
                                if (seqObj["TAB_ID"] == tabId) {
                                    if ($('#' + tabId + "_" + chartId + "_" + seqNo).length) {
                                    } else {
                                        $("#" + tabId).append("<div id='" + tabId + "_" + chartId + "_" + seqNo + "'>" + divid + "</div>");
                                        seqArray.push(parseInt(seqNo));
                                        seqObj[parseInt(seqNo)] = tabId + "_" + chartId + "_" + seqNo;
                                    }
                                }
                            } else {
                                if (seqObj["TAB_ID"] == tabId) {
                                    if ($('#' + tabId + "_" + chartId + "_" + seqNo).length) {
                                    } else {
                                        $("<div id='" + tabId + "_" + chartId + "_" + seqNo + "'>" + divid + "</div>").insertBefore($("#" + seqObj[insertDivBefore]));
                                        seqArray.push(parseInt(seqNo));
                                        seqObj[parseInt(seqNo)] = tabId + "_" + chartId + "_" + seqNo;
                                    }
                                }
                            }
                            seqArray = seqArray.sort(function (a, b) {
                                return a - b;
                            });

                            //----------------------------------------

                            var finalChartObj = {};
                            finalChartObj = result['chartTypesObj'];
                            var chartDataObj = result['chartDataObj'];
                            finalChartObj.source = chartDataObj['data'];
                            var settings = {};
                            settings = finalChartObj;
                            $("#" + chartId + "_chart").css("width", "" + sizeOfChart + "", "important");
                            $("#" + chartId).css("width", "" + width + "", "important");
                            $("#" + chartId).css("height", "" + height + "", "important");
                            $("#" + chartId).jqxChart(settings);
                            var imageContent = $("#" + chartId).jqxChart('saveAsJPEG', 'myChart.jpeg', "test", true);
                            JQXImageContent[i] = "data:image/png;base64," + imageContent;
                            $("#" + chartId).attr("jqxChartNum", i);
                            $("#" + chartId).on('click', function (e) //click event
                            {
                                if (e.args != null) {
                                    selectJqwidgetHandler(e.args.serie.dataField, e.args.elementValue, chartId, tabComponentId);
                                }
                            });


                        }

                        if (i < chartDataList.length) {
                            var jqwidgetFlag = chartDataList[i]['36'];
                            if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Y')
                            {
                                getJqwidgetChart(chartDataList[i], tabId, i, chartDataList);
                            } else if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Q')
                            {
                                getShowCube(chartDataList[i], tabId, i, chartDataList);
                            } else
                            {
                                getChartByChart(chartDataList[i], tabId, i, chartDataList);
                            }
                        }
                    }
                } catch (e)
                {
                    if (i < chartDataList.length) {
                        var jqwidgetFlag = chartDataList[i]['36'];
                        if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Y')
                        {
                            getJqwidgetChart(chartDataList[i], tabId, i, chartDataList);
                        } else if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Q')
                        {
                            getShowCube(chartDataList[i], tabId, i, chartDataList);
                        } else
                        {
                            getChartByChart(chartDataList[i], tabId, i, chartDataList);
                        }
                    }
                    $("#wait").css("display", "none");
                }
            }

        },
        error: function (e) {
            if (i < chartDataList.length) {
                var jqwidgetFlag = chartDataList[i]['36'];
                if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Y')
                {
                    getJqwidgetChart(chartDataList[i], tabId, i, chartDataList);
                } else if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Q')
                {
                    getShowCube(chartDataList[i], tabId, i, chartDataList);
                } else
                {
                    getChartByChart(chartDataList[i], tabId, i, chartDataList);
                }
            }
            sessionTimeout(e);
            $("#wait").css("display", "none");
        }

    });
}
function getJqwidgetsRefreshChartData(chartData, tabChartId, chartId)
{
    var filterItem;
    if (chartFilterItems != null && !jQuery.isEmptyObject(chartFilterItems))
    {
        filterItem = JSON.parse(chartFilterItems);
    }
    var paramArrData = [];
    var paramArray = $("#" + chartId + "_Nested_filterVal").val();
    if (paramArray != null && paramArray != '' && paramArray != undefined)
    {
        paramArrData = JSON.parse(paramArray);
    }
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        url: "getJqwidgetAnalyticCharts",
        async: true,
        cache: false,
        data: {
            'chartData': chartData,
            'resetFlag': 'Y',
            'paramArray': JSON.stringify(paramArrData),
            'chartFilterItems': JSON.stringify(filterItem)
        },
        success: function (response) {
            if (response != null && response != '' && response != undefined) {
                $("#wait").css("display", "none");
                var resultData = JSON.parse(response);
                if (resultData != null)
                {
                    var result = resultData;
                    if (result != null) {
                        var chartsObj = result['chartsObj'];
                        var divid = chartsObj['divId'];
                        var seqNo = chartsObj['seqNo'];
                        var chartId = chartsObj['chartId'];
                        var sizeOfChart = chartsObj['sizeOfChart'];
                        var height = chartsObj['height'];
                        var width = chartsObj['width'];
                        $("#" + chartId + "_Chart").empty();
                        $("#" + chartId + "_Chart").html(divid);

                        var description = chartsObj['description'];
                        var chartInitParamObj = chartsObj['chartInitParamObj'];

                        $("#" + chartId + "_description").val(JSON.stringify(description));
                        $("#" + chartId + "_chartInitParams").val(JSON.stringify(chartInitParamObj));
                        $("#" + chartId + "_chart").css("width", "" + sizeOfChart + "", "important");
                        $("#" + chartId).css("width", "" + width + "", "important");
                        $("#" + chartId).css("height", "" + height + "", "important");
                        var finalChartObj = {};
                        finalChartObj = result['chartTypesObj'];
                        var chartDataObj = result['chartDataObj'];
                        finalChartObj.source = chartDataObj['data'];
                        var settings = {};
                        settings = finalChartObj;
                        $("#" + chartId).jqxChart(settings);
                        var chartType = result['chartType'];
                        if (chartType != null && chartType != '' && chartType != undefined)
                        {
                            $("#" + chartId + "_types").val(chartType);
                        }
                        var replacChartType = result['replacChartType'];
                        if (replacChartType != null && replacChartType != '' && replacChartType != undefined)
                        {
                            $("#" + chartId + "_chartType").val(replacChartType);
                        } else
                        {
                            $("#" + chartId + "_chartType").val(chartType);
                        }
                        $("#" + chartId).on('click', function (e)  //click event
                        {
                            if (e.args != null) {
                                selectJqwidgetHandler(e.args.serie.dataField, e.args.elementValue, chartId, tabChartId);
                            }
                        });

                    }

                    $("#wait").css("display", "none");
                }
            }

        },
        error: function (e) {
            sessionTimeout(e);
        }

    });
}
function getJqwidgetChartDropdownData(chartData, tabId, chartVal, paramArray, jqxChartNum)
{
    var filterItem;
    if (chartFilterItems != null && !jQuery.isEmptyObject(chartFilterItems))
    {
        filterItem = JSON.parse(chartFilterItems);
    }
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        url: "getJqwidgetAnalyticCharts",
        async: true,
        cache: false,
        data: {
            'chartData': chartData,
            'ddwFlag': 'Y',
            'chartVal': chartVal,
            'paramArray': JSON.stringify(paramArray),
            'chartFilterItems': JSON.stringify(filterItem)
        },
        success: function (response) {
            $("#wait").css("display", "none");
            if (response != null && response != '' && response != undefined) {
                var resultData = JSON.parse(response);
                if (resultData != null)
                {
                    var result = resultData;
                    if (result != null) {
                        var chartsObj = result['chartsObj'];
                        var divid = chartsObj['divId'];
                        var seqNo = chartsObj['seqNo'];
                        var chartId = chartsObj['chartId'];
                        var sizeOfChart = chartsObj['sizeOfChart'];
                        var height = chartsObj['height'];
                        var width = chartsObj['width'];
                        var chartFlag = chartData[36];
                        var tset = $("#" + chartId).attr("jqwidgetChartFlag", chartData[36]);
                        var test = $("#" + chartId).attr("jqwidgetChartFlag");
                        var showFilterText = $("#" + chartId + "_Chart").attr('showFilterText');
//                        $("#" + chartId).jqxChart(settings);


                        var paramArrData = [];
                        var paramArr = $("#" + chartId + "_filterVal").val();
                        if (paramArr != null && paramArr != '' && paramArr != undefined)
                        {
                            paramArrData = JSON.parse(paramArr);
                        }
                        $("#" + chartId + "_Chart").empty();
                        $("#" + chartId + "_Chart").html(divid);

                        var chartInitParamObj = chartsObj['chartInitParamObj'];


                        var description = chartsObj['description'];
                        $("#" + chartId + "_description").val(JSON.stringify(description));
                        $("#" + chartId + "_chartInitParams").val(JSON.stringify(chartInitParamObj));
                        $("#" + chartId + "_ddwVal").val(chartVal);
                        $("#" + chartId + "_filterVal").val(JSON.stringify(paramArrData));

                        $("#" + chartId + "_chart").css("width", "" + sizeOfChart + "", "important");
                        $("#" + chartId).css("width", "" + width + "", "important");
                        $("#" + chartId).css("height", "" + height + "", "important");
                        $("#" + chartId + "_filter_text").html(showFilterText);
                        var finalChartObj = {};
                        finalChartObj = result['chartTypesObj'];
                        var chartDataObj = result['chartDataObj'];
                        finalChartObj.source = chartDataObj['data'];
                        var settings = {};
                        settings = finalChartObj;
                        $("#" + chartId).jqxChart(settings);
                        var imageContent = $("#" + chartId).jqxChart('saveAsJPEG', 'myChart.jpeg', "test", true);
                        JQXImageContent[jqxChartNum] = "data:image/png;base64," + imageContent;
                        $("#" + chartId + "_types").val(chartVal);
                        var replacChartType = result['replacChartType'];
                        var tabGridId = result['tabGridId'];
                        if (replacChartType != null && replacChartType != '' && replacChartType != undefined)
                        {
                            $("#" + chartId + "_chartType").val(replacChartType);
                        } else
                        {
                            $("#" + chartId + "_chartType").val(chartVal);
                        }
                        $("#" + chartId).on('click', function (e)  //click event
                        {
                            if (e.args != null) {
                                selectJqwidgetHandler(e.args.serie.dataField, e.args.elementValue, chartId, tabGridId);
                            }
                        });

                    } else
                    {
                        if (!(width.indexOf("%") > -1))
                        {
                            width = width + "px";
                        }
                        $("#" + chartId).html("<div class ='analyticChartNoDataClass'><span class='analyticsChartNoDataText' style ='width:" + width + ";'>No Data Found</span></div>");
                    }
                    $("#" + chartId).attr("jqxChartNum", jqxChartNum);
                }
                $("#wait").css("display", "none");
            }


        },
        error: function (e) {
            sessionTimeout(e);
        }

    });
}
function getJqwidgetChartUpdateData(chartData, tabId, paramArray, tabGridId)
{
    var filterItem;
    if (chartFilterItems != null && !jQuery.isEmptyObject(chartFilterItems) && chartFilterItems != "")
    {
        filterItem = JSON.parse(chartFilterItems);
    }
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        url: "getJqwidgetAnalyticCharts",
        async: true,
        cache: false,
        data: {
            'chartData': chartData,
            'ddwFlag': 'Y',
            'editFlag': 'Y',
            'paramArray': JSON.stringify(paramArray),
            'chartFilterItems': JSON.stringify(filterItem)
        },
        success: function (response) {
            $("#wait").css("display", "none");
            if (response != null && response != '' && response != undefined) {
                var resultData = JSON.parse(response);
                if (resultData != null)
                {
                    var result = resultData;
                    if (result != null) {
                        var chartsObj = result['chartsObj'];
                        var divid = chartsObj['divId'];
                        var seqNo = chartsObj['seqNo'];
                        var chartId = chartsObj['chartId'];
                        var sizeOfChart = chartsObj['sizeOfChart'];
                        var height = chartsObj['height'];
                        var width = chartsObj['width'];
                        var paramArrData = [];
                        var paramArr = $("#" + chartId + "_filterVal").val();
                        if (paramArr != null && paramArr != '' && paramArr != undefined)
                        {
                            paramArrData = JSON.parse(paramArr);
                        }
                        var chartVal = $("#" + chartId + "_ddwVal").val();
                        $("#" + chartId + "_Chart").empty();
                        $("#" + chartId + "_Chart").html(divid);


                        var chartInitParamObj = chartsObj['chartInitParamObj'];
                        var description = chartsObj['description'];
                        var showFilterText = $("#" + chartId + "_Chart").attr('showFilterText');
                        $("#" + chartId + "_description").val(JSON.stringify(description));
                        $("#" + chartId + "_chartInitParams").val(JSON.stringify(chartInitParamObj));
                        $("#" + chartId + "_ddwVal").val(chartVal);
                        $("#" + chartId + "_filterVal").val(JSON.stringify(paramArray));

                        $("#" + chartId + "_chart").css("width", "" + sizeOfChart + "", "important");
                        $("#" + chartId).css("width", "" + width + "", "important");
                        $("#" + chartId).css("height", "" + height + "", "important");
                        $("#" + chartId + "_filter_text").html(showFilterText);
                        var finalChartObj = {};

                        finalChartObj = result['chartTypesObj'];
                        var chartDataObj = result['chartDataObj'];
                        finalChartObj.source = chartDataObj['data'];
                        var settings = {};
                        settings = finalChartObj;
                        $("#" + chartId).jqxChart(settings);
                        $("#" + chartId + "_types").val(chartVal);
                        var replacChartType = result['replacChartType'];
                        if (replacChartType != null && replacChartType != '' && replacChartType != undefined)
                        {
                            $("#" + chartId + "_chartType").val(replacChartType);
                        } else
                        {
                            $("#" + chartId + "_chartType").val(chartVal);
                        }
                        $("#" + chartId).on('click', function (e) //click event
                        {
                            if (e.args != null) {
                                selectJqwidgetHandler(e.args.serie.dataField, e.args.elementValue, chartId, tabGridId);
                            }
                        });

                    } else
                    {
                        if (!(width.indexOf("%") > -1))
                        {
                            width = width + "px";
                        }
                        $("#" + chartId).html("<div class ='analyticChartNoDataClass' style ='width:" + width + ";'><span class='analyticsChartNoDataText'>No Data Found</span></div>");
                    }

                }
                $("#wait").css("display", "none");
            }


        },
        error: function (e) {
            sessionTimeout(e);
        }

    });
}
//for funnelCharts
function getDIYAnalyticsData(tabId, selectedGridId, scrollFlagName)
{
    cockpitViewErrorFlag = false;
    seqArray = [];
    seqObj = {};
    seqObj["TAB_ID"] = tabId;
    $("#" + tabId).empty();
    $("#tabChartsId").val(tabId);
    $("#tabComponentId").val(selectedGridId);
    $("#" + tabId + "div").remove();
    try {
        $("#" + previousDiv).empty();
    } catch (e) {
    }
    previousDiv = tabId;
    $("#wait").css("display", "block");
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        cache: false,
        url: "dataAnalyticsCharts",
        data: {
            'tabComponentId': selectedGridId,
            'scrollFlagName': scrollFlagName,
            'tabComponentType': 'DIY_ANALYTICS'
        },
        success: function (response) {
            if (response != null)
            {
                var result = JSON.parse(response);
                var dataObj = result['chartList'];
                $("#startIndex").val(result['startIndex']);
                $("#endIndex").val(result['endIndex']);
                $("#pageSize").val(result['pageSize']);
                $("#totalCount").val(result['totalCount']);
                $("#scrollFlag").val(result['scrollFlag']);
                $("#visionAnalyticCheckBoxData").empty();
                $("#visionAnalyticCheckBoxData").html(result['checkBoxStr']);
                $("#" + tabId).empty();
                $("#" + tabId + " div").remove();
                if (dataObj != null) {
                    var jqwidgetFlag = dataObj[0]['36'];
                    if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Y')
                    {
                        getJqwidgetChart(dataObj[0], tabId, 0, dataObj);
                    } else
                    {
                        getDIYChartByChart(dataObj[0], tabId, 0, dataObj);
                    }
                }
            }
        },
        error: function (e) {
            sessionTimeout(e);
        }
    });
}
function getDIYChartByChart(chartData, tabId, i, chartDataList)
{
    $("#wait").css("display", "block");
    var filterItem;
    if (chartFilterItems != null && !jQuery.isEmptyObject(chartFilterItems))
    {
        filterItem = JSON.parse(chartFilterItems);
    }
    i++;
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        url: "getAnalyticCharts",
        cache: false,
        data: {
            'chartData': chartData,
            'chartFilterItems': JSON.stringify(filterItem)
        },
        success: function (response) {

            $("#wait").css("display", "none");
            if (response != null && response != '' && response != undefined) {
                try {
                    var resultData = JSON.parse(response);
                    if (resultData != null)
                    {
                        var result = resultData[0];
                        if (result != null) {
                            var divid = result['divId'];
                            var seqNo = result['seqNo'];
                            var chartId = result['chartId'];
                            //-----------------------------
                            var insertDivBefore = 0;
                            for (var j = 0; j < seqArray.length; j++) {
                                if (parseInt(seqNo) < seqArray[j]) {
                                    insertDivBefore = seqArray[j];
                                    break;
                                }
                            }
                            if (insertDivBefore == 0) {
                                if (seqObj["TAB_ID"] == tabId) {
                                    if ($('#' + tabId + "_" + chartId + "_" + seqNo).length) {
                                    } else {
                                        $("#" + tabId).append("<div id='" + tabId + "_" + chartId + "_" + seqNo + "'>" + divid + "</div>");
                                        seqArray.push(parseInt(seqNo));
                                        seqObj[parseInt(seqNo)] = tabId + "_" + chartId + "_" + seqNo;
                                    }
                                }
                            } else {
                                if (seqObj["TAB_ID"] == tabId) {
                                    if ($('#' + tabId + "_" + chartId + "_" + seqNo).length) {
                                    } else {
                                        $("<div id='" + tabId + "_" + chartId + "_" + seqNo + "'>" + divid + "</div>").insertBefore($("#" + seqObj[insertDivBefore]));
                                        seqArray.push(parseInt(seqNo));
                                        seqObj[parseInt(seqNo)] = tabId + "_" + chartId + "_" + seqNo;
                                    }
                                }
                            }
                            seqArray = seqArray.sort(function (a, b) {
                                return a - b;
                            });
                            //-------------------
                            var dataArray = result['dataArray'];
                            var options = result['options'];
                            var objInit = result['objInit'];
                            var sizeOfChart = result['sizeOfChart'];
                            var description = result['description'];
                            var chartInitParamObj = result['chartInitParamObj'];
                            var labelChartArr = result['labelChartArr'];
                            var calenderFlag = "";
                            var orgChartFlag = "";
                            if (chartInitParamObj != null)
                            {
                                calenderFlag = chartInitParamObj['uuu_CalenderFlag'];
                                orgChartFlag = chartInitParamObj['uuu_orgChartFlag'];
                            }
                            $("#" + chartId + "_Data").val(dataArray);
                            $("#" + chartId + "_options").val(JSON.stringify(options));
                            $("#" + chartId + "_description").val(JSON.stringify(description));
                            $("#" + chartId + "_chartInitParams").val(JSON.stringify(chartInitParamObj));
                            var dataObjArr = {};
                            var dataObj = $("#" + chartId + "_Data").val();
                            if (dataObj != null && dataObj != '' && dataObj != undefined)
                            {
                                dataObjArr = JSON.parse(dataObj);
                            }
                            if (sizeOfChart != null && sizeOfChart != '' && sizeOfChart != undefined) {
                                $("#" + chartId + "_Chart").css("width", "" + sizeOfChart + "%", "important");
                            } else if (windowWidth > 1024)
                            {
                                $("#" + chartId + "_Chart").css('width', '32%');
                                options['width'] = '90%';
                            } else if (windowWidth < 1024 && windowWidth > 700)
                            {
                                $("#" + chartId + "_Chart").css('width', '49%');
                                options['width'] = '90%';
                            } else if (windowWidth < 699 && windowWidth > 320)
                            {
                                $("#" + chartId + "_Chart").css('width', '98%');
                                options['width'] = '90%';
                            }
                            var data = [];
                            if (dataObjArr != null && dataObjArr.length > 0)
                            {
                                if (calenderFlag != null && calenderFlag != '' && calenderFlag == "Y") {
                                    try {
                                        var dataObjStr = calenderChartData(dataObjArr);
                                        data = google.visualization.arrayToDataTable(dataObjStr);
                                    } catch (e) {
                                    }
                                } else {
                                    data = google.visualization.arrayToDataTable(dataObjArr);
                                }
                                var objInit1 = eval(objInit);
                                var chart = objInit1;
                                if (orgChartFlag != null && orgChartFlag != '' && orgChartFlag != undefined
                                        && orgChartFlag == "Y")
                                {
                                    $("#" + chartId + "_Chart").addClass('visionOrgCharts');
//                                   $("#" + chartId + "_Chart").css('height', options['height'], "important");
                                    chart.draw(data, options);
                                    var dataValue = data.getValue(0, 0);
                                    var colorValue = $("#color_" + dataValue).val();
                                    if (colorValue != null && colorValue != '' && colorValue != undefined)
                                    {
                                        data.setRowProperty(0, 'style', 'background-color:' + colorValue + ';background-image:none');
                                        chart.draw(data, options);
                                    }
                                } else
                                {
                                    if (labelChartArr != null && !jQuery.isEmptyObject(labelChartArr))
                                    {
                                        var view = new google.visualization.DataView(data);
                                        view.setColumns(labelChartArr);
                                        chart.draw(view, options);
                                    } else
                                    {
                                        chart.draw(data, options);
                                    }
//                                    chart.draw(data, options);
                                }
                                var chartType = result['chartType'];
                                var replacChartType = result['replacChartType'];
                                if (replacChartType != null && replacChartType != '' && replacChartType != undefined)
                                {
                                    $("#" + chartId + "_chartType").val(replacChartType);
                                } else
                                {
                                    $("#" + chartId + "_chartType").val(chartType);
                                }
                                var tabGridId = result['tabGridId'];
                                if (chartType != null && chartType != '' && chartType != undefined)
                                {
                                    $("#" + chartId + "_types").val(chartType);
                                }

                                if (orgChartFlag != null && orgChartFlag != '' && orgChartFlag != undefined
                                        && orgChartFlag == "Y")
                                {
                                    $("#" + chartId).on("click", function (event) {
                                        var target = event.target;
                                        var href = target.href;
                                        if (chart.getSelection().length > 0) {
                                            if (!(href != null && href != '' && href != undefined) ||
                                                    (href != null && href != '' && href != undefined && href.indexOf("images") > -1))
                                            {
                                                chartStartTabLoader();
                                                orgChart(chart, data, tabGridId, chartId, options, event, this);
                                            } else
                                            {
                                                navigateToOrgChart(chart, data, tabGridId, chartId);
                                            }
                                        }
                                    });
                                } else
                                {
                                    google.visualization.events.addListener(chart, 'select', function () {
                                        selectHandler(chart, data, chartId, tabGridId);
                                    });
                                }
                            } else
                            {
                                if (!(options['width'].indexOf("%") > -1))
                                {
                                    options['width'] = options['width'] + "px";
                                }
                                $("#" + chartId + "_Chart").html("<div class ='analyticChartNoDataClass' style ='width:" + options['width'] + ";'><p>" + options['title'] + "</p><span class='analyticsChartNoDataText'>No Data Found</span></div>");
                            }
                        }
                        $("#wait").css("display", "none");

                        if (i < chartDataList.length) {
                            var jqwidgetFlag = chartDataList[i]['36'];
                            if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Y')
                            {
                                getJqwidgetChart(chartDataList[i], tabId, i, chartDataList);
                            } else
                            {
                                getDIYChartByChart(chartDataList[i], tabId, i, chartDataList);
                            }
                        }

                    }
                } catch (e)
                {
                    if (i < chartDataList.length) {
                        var jqwidgetFlag = chartDataList[i]['36'];
                        if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Y')
                        {
                            getJqwidgetChart(chartDataList[i], tabId, i, chartDataList);
                        } else
                        {
                            getDIYChartByChart(chartDataList[i], tabId, i, chartDataList);
                        }
                    }
                    $("#wait").css("display", "none");
                }
            }

        },
        error: function (e) {
            if (i < chartDataList.length) {
                var jqwidgetFlag = chartDataList[i]['36'];
                if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Y')
                {
                    getJqwidgetChart(chartDataList[i], tabId, i, chartDataList);
                } else
                {
                    getDIYChartByChart(chartDataList[i], tabId, i, chartDataList);
                }
            }
            sessionTimeout(e);
            $("#wait").css("display", "none");
        }

    });
}
//for delete chart data
function deleteChart(chartType, tabGridId, chartId, gridId) {
    $("#dialog").html("Are you sure you want to delete the Chart ??");
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
                        url: 'deleteAnalyticsChart',
                        async: true,
                        data: {
                            chartId: chartId,
                            selectedId: tabGridId
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
                                                //  var divId = chartId +"_Chart";
                                                $("#" + chartId + "_Chart").remove();
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
function showDetails(chartId, tabGridId) {
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        cache: false,
        url: "chartDetails",
        data: {
            'chartId': chartId,
            selectedId: tabGridId
        },
        success: function (response) {
            if (response != null)
            {
                $("#dialog").html(response);
                $("#dialog").dialog({
                    title: (labelObject['Analytics Chart Details'] != null ? labelObject['Analytics Chart Details'] : 'Analytics Chart Details'),
                    modal: true,
                    width: 450,
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
        },
        error: function (e) {
            sessionTimeout(e);
        }
    });

}
function selectJqwidgetHandler(selectedValue, selectedCount, chartId, tabGridId) {
    try {
        var value;
        var count;
        var paramArray = [];
        var paramObj = {};
        var columnName = $("#" + chartId + "_columnName").val();
        paramObj.column = $.trim(columnName);
        paramObj.value = $.trim(selectedValue);
        paramObj.operator = "EQUALS";
        paramArray.push(paramObj);
        var nestedId = $("#" + chartId + "_nestedId").val();
        var chartNestedType = $("#" + chartId + "_nestedType").val();
        if (chartNestedType != null && chartNestedType != '' && chartNestedType != undefined) {
            var nestedType = chartNestedType.split("?")[0];
            var nestedUrl = chartNestedType.split("?")[1];
            if (nestedType != null && nestedType != '' && nestedType == "CHART")
            {
                var nestedChartFlag = $("#" + chartId + "_Nested_flag").val();
                if (nestedChartFlag != null && nestedChartFlag != '' && nestedChartFlag == "Y")
                {
                    nestedPageCharts(nestedId, paramArray, tabGridId);
                } else
                {
                    navigatetoNestedChart(nestedId, paramArray, tabGridId);
                }

            } else if (nestedType != null && nestedType != '' && nestedType != undefined)
            {
                navigatetoNestedComponent(chartId, nestedType, value, nestedId, paramArray, nestedUrl);
            }
        }

    } catch (e)
    {

    }

}
function selectJqwidgetHandler(selectedValue, selectedCount, chartId, tabGridId) {
    try {
        var value;
        var count;
        var paramArray = [];
        var paramObj = {};
        var columnName = $("#" + chartId + "_columnName").val();
        paramObj.column = $.trim(columnName);
        paramObj.value = $.trim(selectedValue);
        paramObj.operator = "EQUALS";
        paramArray.push(paramObj);
        var nestedId = $("#" + chartId + "_nestedId").val();
        var chartNestedType = $("#" + chartId + "_nestedType").val();
        if (chartNestedType != null && chartNestedType != '' && chartNestedType != undefined) {
            var nestedType = chartNestedType.split("?")[0];
            var nestedUrl = chartNestedType.split("?")[1];
            if (nestedType != null && nestedType != '' && nestedType == "CHART")
            {
                var nestedChartFlag = $("#" + chartId + "_Nested_flag").val();
                if (nestedChartFlag != null && nestedChartFlag != '' && nestedChartFlag == "Y")
                {
                    nestedPageCharts(nestedId, paramArray, tabGridId);
                } else
                {
                    navigatetoNestedChart(nestedId, paramArray, tabGridId);
                }

            } else if (nestedType != null && nestedType != '' && nestedType != undefined)
            {
                navigatetoNestedComponent(chartId, nestedType, value, nestedId, paramArray, nestedUrl);
            }
        }

    } catch (e)
    {

    }

}
//for delete chart data
//cube code starts//
//function getShowCube(chartData, tabId, q, chartDataList) {
//    q++;
//    var aggId = "B940D2E1679C45A6E053210110ACAAA6";
//    $.ajax({
//        type: "post",
//        traditional: true,
//        dataType: 'JSON',
//        cache: false,
//        url: "getQubeAnalytics",
//        data: {
//            'chartData': chartData,
//        },
//        success: function (response) {
//
//            try {
//                var result = response;
//                if (result != null)
//                {
//
//                    var chartsObj = result['chartsObj'];
//                    var divid = chartsObj['divId'];
//                    var seqNo = chartsObj['seqNo'];
//                    var chartId = chartsObj['chartId'];
//                    var sizeOfChart = chartsObj['sizeOfChart'];
//                    var height = chartsObj['height'];
//                    var width = chartsObj['width'];
//                    var factTable = chartsObj['factTable'];
//                    var cubeValueCol = chartsObj['cubeValueCol'];
//                    var axisColumns = chartsObj['axisColumns'];
//                    var axisLabels = chartsObj['axisLabels'];
//                    var labelsArray = chartsObj['labelsObject'];
//                    var title = chartsObj['title']; //multi cube 
//                    var cubeCheckBoxDivId = chartsObj['cubeCheckBoxDivId']; //multi cube 
//                    var checkedIdsObj = chartsObj['checkedIdsObj']; //multi cube 
//                    var cubeCheckBoxObj = chartsObj['cubeCheckBoxObj']; //multi cube 
//                    var dropdownCubeObj = chartsObj['dropdownCubeObj']; //multi cube 
//                    var colorsObj = chartsObj['colorsObj'];
//                    //-----------------------------
//                    var insertDivBefore = 0;
//
//                    for (var j = 0; j < seqArray.length; j++) {
//                        if (parseInt(seqNo) < seqArray[j]) {
//                            insertDivBefore = seqArray[j];
//                            break;
//                        }
//                    }
//                    if (insertDivBefore == 0) {
//                        if (seqObj["TAB_ID"] == tabId) {
//                            if ($('#' + tabId + "_" + chartId + "_" + seqNo).length) {
//                            } else {
//                                $("#" + tabId).append("<div id='" + tabId + "_" + chartId + "_" + seqNo + "'>" + divid + "</div>");
//                                seqArray.push(parseInt(seqNo));
//                                seqObj[parseInt(seqNo)] = tabId + "_" + chartId + "_" + seqNo;
//                            }
//                        }
//                    } else {
//                        if (seqObj["TAB_ID"] == tabId) {
//                            if ($('#' + tabId + "_" + chartId + "_" + seqNo).length) {
//                            } else {
//                                $("<div id='" + tabId + "_" + chartId + "_" + seqNo + "'>" + divid + "</div>").insertBefore($("#" + seqObj[insertDivBefore]));
//                                seqArray.push(parseInt(seqNo));
//                                seqObj[parseInt(seqNo)] = tabId + "_" + chartId + "_" + seqNo;
//                            }
//                        }
//                    }
//                    seqArray = seqArray.sort(function (a, b) {
//                        return a - b;
//                    });
//                    //----------------------------------------
//                    $("#" + chartId + "_factTable").val(factTable);
//                    $("#" + chartId + "_cubeValueCol").val(cubeValueCol);
//                    $("#" + chartId + "_axisColumns").val(JSON.stringify(axisColumns));
//                    $("#" + chartId + "_axisLabels").val(JSON.stringify(axisLabels));
////                    var multiSelectCubeLabels = {};
////                    $("#" + chartId + "_multiSelectCubeLabels").val(JSON.stringify(multiSelectCubeLabels));
//                    $("#cubeChartId").val(chartId + "_Chart"); //multi cube
//                    $("#visionAnalyticCubeChartCheckBox").html(cubeCheckBoxDivId);
//                    for (var key in dropdownCubeObj)
//                    {
//                        var source = dropdownCubeObj[key];
//                        $('#visionAnalyticCubeDropdownLabel' + key).jqxDropDownList({
//                            source: source,
//                            theme: 'energyblue',
//                            displayMember: 'text',
//                            valueMember: 'value'
//                        });
//                        $("#visionAnalyticCubeDropdownLabel" + key).jqxDropDownList('selectItem', key);
//                    }
//                    for (var key in cubeCheckBoxObj)
//                    {
//                        var source = cubeCheckBoxObj[key];
//                        $("#visionAnalyticCubeCheck" + key).jqxListBox({
//                            filterable: true,
//                            checkboxes: true,
//                            source: source,
//                            theme: 'energyblue',
//                            displayMember: 'text',
//                            valueMember: 'value'
//                        });
//                        var ids = checkedIdsObj[key];
//                        for (var id = 0; id < ids.length; id++)
//                        {
//                            $("#visionAnalyticCubeCheck" + key).jqxListBox('checkItem', ids[id]);
//                        }
//                    }
//                    if (sizeOfChart != null && sizeOfChart != '' && sizeOfChart != undefined) {
//                        $("#" + chartId + "_Chart").css("width", "" + sizeOfChart + "%", "important");
//                    } else if (windowWidth > 1024)
//                    {
//                        $("#" + chartId + "_Chart").css('width', '99%', 'important');
//                    } else if (windowWidth < 1024 && windowWidth > 700)
//                    {
//                        $("#" + chartId + "_Chart").css('width', '49%');
//                    } else if (windowWidth < 699 && windowWidth > 320)
//                    {
//                        $("#" + chartId + "_Chart").css('width', '98%');
//                    }
//
//                    $("#" + chartId).html("<div id ='" + chartId + "_cubeContainer' class='VisionShowCubeChart'></div>");
//                    $("#" + chartId + "_cube_types").val(title); //multi cube
//                    var checkBoxObjCheckedVal = {};
//                    var dropdownBoxObj = {};
//                    alert("checked");
//                    var selectedXvalues = $("#visionAnalyticCubeDropdownLabelx").jqxDropDownList('getSelectedItem');
//                    var selectedYvalues = $("#visionAnalyticCubeDropdownLabely").jqxDropDownList('getSelectedItem');
//                    var selectedZvalues = $("#visionAnalyticCubeDropdownLabelz").jqxDropDownList('getSelectedItem');
//                    var checkedXvalues = $("#visionAnalyticCubeCheckx").jqxListBox('getCheckedItems');
//                    var checkedYvalues = $("#visionAnalyticCubeChecky").jqxListBox('getCheckedItems');
//                    var checkedZvalues = $("#visionAnalyticCubeCheckz").jqxListBox('getCheckedItems');
//                    var xValues = [];
//                    var yValues = [];
//                    var zValues = [];
//                    $.each(checkedXvalues, function (index) {
//                        xValues.push(this.value);
//                    });
//                    $.each(checkedYvalues, function (index) {
//                        yValues.push(this.value);
//                    });
//                    $.each(checkedZvalues, function (index) {
//                        zValues.push(this.value);
//                    });
//                    dropdownBoxObj[selectedXvalues.label] = 'x';
//                    dropdownBoxObj[selectedYvalues.label] = 'y';
//                    dropdownBoxObj[selectedZvalues.label] = 'z';
//                    checkBoxObjCheckedVal[selectedXvalues.label] = xValues;
//                    checkBoxObjCheckedVal[selectedYvalues.label] = yValues;
//                    checkBoxObjCheckedVal[selectedZvalues.label] = zValues;
//                    $("#" + chartId + "_checkBoxChecked").remove();
//                    $("#" + chartId + "_dropdownselected").remove();
//                    $("#cubeCheckBoxCheckedValues").append("<input type='hidden' id='" + chartId + "_checkBoxChecked' value=''/>");
//                    $("#cubeCheckBoxCheckedValues").append("<input type='hidden' id='" + chartId + "_dropdownselected' value=''/>");
//                    $("#" + chartId + "_checkBoxChecked").val(JSON.stringify(checkBoxObjCheckedVal));
//                    $("#" + chartId + "_dropdownselected").val(JSON.stringify(dropdownBoxObj));
//                    var noOfCubes = 3;
//                    var cubeSize = 100;
//                    var scene = new THREE.Scene();
//                    var camera = new THREE.PerspectiveCamera(45, 2.5, 1, 10000);
//
//
//                    var renderer = new THREE.WebGLRenderer({antialias: true});
//                    renderer.setSize(parseInt(width) + 600, height);
//                    scene.background = new THREE.Color(0xFFFFFF);
////                     scene.position.set(300, 300, -300);
//
//                    document.querySelector("#" + chartId + "_cubeContainer").appendChild(renderer.domElement);
////                    camera.position.set(1800, 1000, 600);
//                    camera.position.set(1800 * 5, 1000 * 5, 600 * 5);
//
//
//                    var raycaster = new THREE.Raycaster();
//                    var mouse = new THREE.Vector2();
//                    const axesHelper = new THREE.AxesHelper((noOfCubes + 5) * 100);
////                    scene.add(axesHelper);
//                    renderer.setPixelRatio(window.devicePixelRatio);
//                    renderer.setSize(parseInt(width) + 600, height);
//                    var labelRenderer = new CSS2DRenderer();
//                    labelRenderer.setSize(parseInt(width) + 600, height);
//                    labelRenderer.domElement.style.position = 'absolute';
//                    labelRenderer.domElement.style.top = '0px';
//                    document.querySelector('#' + chartId + "_cubeContainer").appendChild(labelRenderer.domElement);
//                    var domEvents = new THREEx.DomEvents(camera, labelRenderer.domElement);
//                    var axisArray = [];
//                    var xlen = labelsArray['x'].length;
//                    var ylen = labelsArray['y'].length;
//                    var zlen = labelsArray['z'].length;
//
//                    var xIndex = 1;
//                    for (var j = -xlen / 2; j < xlen / 2; j++) {
//                        var sign = j / Math.abs(j);
//                        var elem = document.createElement('div');
//                        elem.className = 'label';
//                        elem.style.marginTop = '-1em';
//                        var Label = new CSS2DObject(elem);
//                        var xpoints = [];
//                        if (j != (xlen / 2 - 1)) {
//                            xpoints.push(new THREE.Vector3((j - 0.5) * cubeSize, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
//                            xpoints.push(new THREE.Vector3((j + 0.5) * cubeSize, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
//                        } else {
//
//                            xpoints.push(new THREE.Vector3((j - 0.5) * cubeSize, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
//                            xpoints.push(new THREE.Vector3((j + 0.5) * cubeSize * 2, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
//
////                            xpoints.push(new THREE.Vector3((j) * cubeSize, (j/Math.abs(j))* (ylen/2)* cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
////                            xpoints.push(new THREE.Vector3((j * cubeSize) + 1.5 * cubeSize, (j/Math.abs(j))* (ylen/2)* cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
//                        }
//                        var xmaterial = new THREE.LineBasicMaterial({color: 0x0071C5});
//                        var xgeometry = new THREE.BufferGeometry().setFromPoints(xpoints);
//                        var xaxis = new THREE.Line(xgeometry, xmaterial);
//                        scene.add(xaxis);
//                        Label.position.set((j + 0.5) * cubeSize, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize);
//                        elem.textContent = labelsArray['x'][xIndex - 1][0];
//                        elem.id = chartId + "_x_" + labelsArray['x'][xIndex - 1][1];
//                        xaxis.add(Label);
//                        axisArray.push(xaxis)
//                        xIndex++;
//                    }
//
//
//                    var yIndex = 1;
//                    for (var j = -(ylen / 2); j < ylen / 2; j++) {
//
//                        var elem = document.createElement('div');
//                        elem.className = 'label';
//                        elem.style.marginTop = '-1em';
//                        var Label = new CSS2DObject(elem);
//                        var ypoints = [];
//                        if (j != (ylen / 2 - 1)) {
//
//                            ypoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize, (j - 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
//                            ypoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize, (j + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
//
////                            ypoints.push(new THREE.Vector3((j/Math.abs(j))* (xlen/2)* cubeSize, (j - 1) * cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
////                            ypoints.push(new THREE.Vector3((j/Math.abs(j))* (xlen/2)* cubeSize, (j * cubeSize), (j/Math.abs(j))* (zlen/2)* cubeSize));
//                        } else {
//
//                            ypoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize, (j - 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
//                            ypoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize, (j + 0.5) * cubeSize * 1.5, (zlen / 2 + 0.5) * cubeSize));
//
////                            ypoints.push(new THREE.Vector3((j/Math.abs(j))* (xlen/2)* cubeSize, (j - 1) * cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
////                            ypoints.push(new THREE.Vector3((j/Math.abs(j))* (xlen/2)* cubeSize, (j * cubeSize) + 1 * cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
//                        }
//
//                        var ymaterial = new THREE.LineBasicMaterial({color: 0x0071C5});
//                        var ygeometry = new THREE.BufferGeometry().setFromPoints(ypoints);
//                        var yaxis = new THREE.Line(ygeometry, ymaterial);
//                        scene.add(yaxis);
//                        Label.position.set(-(xlen / 2 + 0.5) * cubeSize, (j + 0.5) * cubeSize - 20, (zlen / 2 + 0.5) * cubeSize);
//                        elem.textContent = labelsArray['y'][yIndex - 1][0];
//                        elem.id = chartId + "_y_" + labelsArray['y'][yIndex - 1][1];
//                        yaxis.add(Label);
//                        axisArray.push(yaxis)
//                        yIndex++;
//
//                    }
//
//
//                    var zIndex = 1;
//                    for (var j = -(zlen / 2); j < zlen / 2; j++) {
//                        var noOfCubes = labelsArray['x'].length;
//                        var elem = document.createElement('div');
//                        elem.className = 'label';
//                        elem.style.marginTop = '-1em';
//                        var Label = new CSS2DObject(elem);
//                        var zpoints = [];
//                        if (j != (zlen / 2 - 1)) {
//
//                            zpoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j - 0.5) * cubeSize));
//                            zpoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j + 0.5) * cubeSize));
//
////                            zpoints.push(new THREE.Vector3((noOfCubes * cubeSize + cubeSize), (j/Math.abs(j))* (ylen/2)* cubeSize, -(j - 1) * cubeSize));
////                            zpoints.push(new THREE.Vector3((noOfCubes * cubeSize + cubeSize), (j/Math.abs(j))* (ylen/2)* cubeSize, -(j * cubeSize)));
//                        } else {
//
//                            zpoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j - 0.5) * cubeSize));
//                            zpoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j + 0.5) * cubeSize * 1.5));
//
////                            zpoints.push(new THREE.Vector3((noOfCubes * cubeSize + cubeSize), (j/Math.abs(j))* (ylen/2)* cubeSize, -(j - 1) * cubeSize));
////                            zpoints.push(new THREE.Vector3((noOfCubes * cubeSize + cubeSize), (j/Math.abs(j))* (ylen/2)* cubeSize, -((j * cubeSize) + 1 * cubeSize)));
//                        }
//
//                        var zmaterial = new THREE.LineBasicMaterial({color: 0x0071C5});
//                        var zgeometry = new THREE.BufferGeometry().setFromPoints(zpoints);
//                        var zaxis = new THREE.Line(zgeometry, zmaterial);
//                        scene.add(zaxis);
//                        Label.position.set(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j + 0.5) * cubeSize);
////                        Label.position.set(noOfCubes * cubeSize + cubeSize, (j/Math.abs(j))* (ylen/2)* cubeSize, -j * cubeSize);
//                        elem.textContent = labelsArray['z'][zIndex - 1][0];
//                        elem.id = chartId + "_z_" + labelsArray['z'][zIndex - 1][1];
//                        zaxis.add(Label);
//                        axisArray.push(zaxis)
//                        zIndex++;
//
//                    }
//
//
//                    var geometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
//                    // material
//                    material = new THREE.MeshBasicMaterial({
//                        color: 0xffffff,
//                        vertexColors: true,
//                        opacity: 0.5,
//                        transparent: true
//                    });
//                    if (colorsObj != null && !jQuery.isEmptyObject(colorsObj))
//                    {
//                        red = new THREE.Color(colorsObj['x']);
//                        green = new THREE.Color(colorsObj['y']);
//                        blue = new THREE.Color(colorsObj['z']);
//                    } else
//                    {
//                        red = new THREE.Color(0x0071C5);
//                        green = new THREE.Color(0x99e6ff);
//                        blue = new THREE.Color(0xEAF4FD);
//                    }
//                    var colors = [red, green, blue];
//                    for (var i = 0; i < 3; i++) {
//                        geometry.faces[4 * i].color = colors[i];
//                        geometry.faces[4 * i + 1].color = colors[i];
//                        geometry.faces[4 * i + 2].color = colors[i];
//                        geometry.faces[4 * i + 3].color = colors[i];
//                    }
//
//                    var relativeX = 0;
//                    var relativeY = 0;
//                    var relativeZ = 0;
//                    for (var i = -(xlen / 2); i < xlen / 2; i++) {
//
//                        relativeX = relativeX + cubeSize;
//                        relativeY = 0;
//                        relativeZ = 0;
//                        for (var j = -(ylen / 2); j < ylen / 2; j++) {
//                            relativeY = relativeY + cubeSize;
//                            relativeZ = 0;
//                            for (var k = -(zlen / 2); k < zlen / 2; k++) {
//                                relativeZ = relativeZ - cubeSize;
//                                var cube = new THREE.Mesh(geometry, material);
//                                scene.add(cube);
//                                var edges = new THREE.EdgesGeometry(geometry);
//                                line = new THREE.LineSegments(edges, new THREE.LineBasicMaterial({color: 0x000000, opacity: 0.5, transparent: true}));
//                                cube.add(line);
//                                cube.position.set((i + 0.5) * cubeSize, (j + 0.5) * cubeSize, -(k + 0.5) * cubeSize);
//                                var relativePosition = {};
//                                relativePosition.x = relativeX;
//                                relativePosition.y = relativeY;
//                                relativePosition.z = relativeZ;
//
//                                cube.userData['relativePosition'] = relativePosition;
//                                // CLICK EVENT----->>>>>
//
//                                domEvents.addEventListener(cube, 'click', function (event) {
//                                    console.log("clicked");
//                                    event.origDomEvent.preventDefault();
//                                    cubeClickFlag = true;
//                                    material.opacity = 0.5;
//                                    var originalcubePosition = event.target.position;
//                                    var cubePosition = event.target.userData['relativePosition'];
////                                    cubePosition.x = cubePosition.x+300;
////                                    cubePosition.y = cubePosition.y+300;
////                                    cubePosition.z = cubePosition.z-300;
//
//                                    var xindex = Math.abs((cubePosition.x) / cubeSize);
//                                    var xlabel = labelsArray['x'][xindex - 1][0];
//                                    var xlabelId = labelsArray['x'][xindex - 1][1];
//
//                                    var yindex = Math.abs((cubePosition.y) / cubeSize);
//                                    var ylabel = labelsArray['y'][yindex - 1][0];
//                                    var ylabelId = labelsArray['y'][yindex - 1][1];
//
//                                    var zindex = Math.abs((cubePosition.z) / cubeSize);
//                                    var zlabel = labelsArray['z'][zindex - 1][0];
//                                    var zlabelId = labelsArray['z'][zindex - 1][1];
//
//                                    alert("X::" + xlabel + "  Y::" + ylabel + "  Z::" + zlabel);
//
//
//                                    var clickedLabels = {}
//                                    clickedLabels['x'] = xlabel;
//                                    clickedLabels['y'] = ylabel;
//                                    clickedLabels['z'] = zlabel;
//
//                                    var clickedLabelIds = {}
//                                    clickedLabelIds['x'] = xlabelId;
//                                    clickedLabelIds['y'] = ylabelId;
//                                    clickedLabelIds['z'] = zlabelId;
//
//                                    var firstSelectedCubeLebel = {};
//
//                                    if (event.origDomEvent.shiftKey) {
//
//                                        var multiSelectCubeLabels = multiSelectCubeLabelsObj[chartId];
//                                        if (multiSelectCubeLabels == null || jQuery.isEmptyObject(multiSelectCubeLabels)) {
//                                            multiSelectCubeLabels = {};
//                                        }
//
//                                        var selectedCubesArray = selectedCubesArrayObj[chartId];
//                                        if (selectedCubesArray == null || selectedCubesArray.length == 0) {
//                                            selectedCubesArray = [];
//                                        }
//
//                                        if (multiSelectCubeLabels != null && Object.keys(multiSelectCubeLabels).length == 1) {
//                                            firstSelectedCubeLebel = multiSelectCubeLabels
//                                            var matchCount = 0;
//                                            var values = Object.values(multiSelectCubeLabels)[0];
//                                            var staticAxi = [];
//
//                                            var sliceAxis;
//                                            if (values['x'] == clickedLabelIds['x']) {
//                                                matchCount++;
//                                                staticAxi.push('x');
//                                            } else {
//                                                sliceAxis = 'x';
//                                            }
//                                            if (values['y'] == clickedLabelIds['y']) {
//                                                matchCount++;
//                                                staticAxi.push('y');
//                                            } else {
//                                                sliceAxis = 'y';
//                                            }
//                                            if (values['z'] == clickedLabelIds['z']) {
//                                                matchCount++;
//                                                staticAxi.push('z');
//                                            } else {
//                                                sliceAxis = 'z';
//                                            }
//                                            if (matchCount == 5) {
//
//                                                $(".label").removeClass("highLightLabel");
//
//                                                $.each(selectedCubesArray, function (index) {
//
//                                                    scene.remove(selectedCubesArray[index]);
//                                                    $(".cubeLabel").remove();
//                                                })
//
//
//                                                labelsArray[sliceAxis];
////                                                var selectedCubesArray = selectedCubesArrayObj[chartId];
////                                                 if (selectedCubesArray==null || selectedCubesArray.length==0){
////                                                     selectedCubesArray = [];
////                                                 }
//                                                var selectCubeLabels = {};
//                                                var addOrder;
//                                                var sliceAxisPos;
//                                                var newCubePosition = {};
//                                                var cubeReached = false;
//                                                for (var i = 0; i < labelsArray[sliceAxis].length; i++) {
//
//                                                    if (!cubeReached) {
//                                                        if (i == 0) {
//                                                            var yellowCube = selectedCubesArray[0];
//                                                            newCubePosition = yellowCube.position;
//                                                            sliceAxisPos = newCubePosition[sliceAxis];
//                                                            if (Math.abs(cubePosition[sliceAxis]) - Math.abs(newCubePosition[sliceAxis]) >= 0) {
//                                                                addOrder = "plus";
//                                                            } else {
//                                                                addOrder = "minus";
//                                                            }
//
//                                                        } else {
//                                                            if (addOrder == "plus") {
//                                                                newCubePosition[sliceAxis] = sliceAxisPos + (sliceAxisPos / Math.abs(sliceAxisPos) * i * cubeSize);
//                                                                if (newCubePosition[sliceAxis] == event.target.position[sliceAxis]) {
//                                                                    cubeReached = true;
//                                                                }
//                                                            } else if (addOrder == "minus") {
//
//                                                                newCubePosition[sliceAxis] = sliceAxisPos - (sliceAxisPos / Math.abs(sliceAxisPos) * i * cubeSize);
//                                                                if (newCubePosition[sliceAxis] == event.target.position[sliceAxis]) {
//                                                                    cubeReached = true;
//                                                                }
//                                                            }
//
//                                                        }
//                                                        if (sliceAxis == 'z') {
//                                                            var sliceAxisindex = Math.abs((newCubePosition[sliceAxis]) / cubeSize);
//                                                        } else {
//                                                            var sliceAxisindex = Math.abs((newCubePosition[sliceAxis]) / cubeSize);
//                                                        }
//
//                                                        var sliceAxislabel = labelsArray[sliceAxis][sliceAxisindex - 1][0];
//                                                        var sliceAxislabelId = labelsArray[sliceAxis][sliceAxisindex - 1][1];
//
//                                                        var staticAxis1 = staticAxi[0];
//                                                        if (staticAxis1 == 'z') {
//                                                            var staticAxis1index = Math.abs((newCubePosition[staticAxis1]) / cubeSize);
//                                                        } else {
//                                                            var staticAxis1index = Math.abs((newCubePosition[staticAxis1]) / cubeSize);
//                                                        }
//
//                                                        var staticAxis1label = labelsArray[staticAxis1][staticAxis1index - 1][0];
//                                                        var staticAxis1labelId = labelsArray[staticAxis1][staticAxis1index - 1][1];
//
//                                                        var staticAxis2 = staticAxi[1];
//                                                        if (staticAxis2 == 'z') {
//                                                            var staticAxis2index = Math.abs((newCubePosition[staticAxis2]) / cubeSize);
//                                                        } else {
//                                                            var staticAxis2index = Math.abs((newCubePosition[staticAxis2]) / cubeSize);
//                                                        }
//
//                                                        var staticAxis2label = labelsArray[staticAxis2][staticAxis2index - 1][0];
//                                                        var staticAxis2labelId = labelsArray[staticAxis2][staticAxis2index - 1][1];
//
//                                                        var labels = {}
//                                                        labels[sliceAxis] = sliceAxislabel;
//                                                        labels[staticAxis1] = staticAxis1label;
//                                                        labels[staticAxis2] = staticAxis2label;
//
//                                                        var labelIds = {}
//                                                        labelIds[sliceAxis] = sliceAxislabelId;
//                                                        labelIds[staticAxis1] = staticAxis1labelId;
//                                                        labelIds[staticAxis2] = staticAxis2labelId;
//
////                                                        selectCubeLabels[sliceAxislabelId + staticAxis1labelId + staticAxis2labelId] = labelIds;
//                                                        selectCubeLabels[newCubePosition.x + "," + newCubePosition.y + "," + newCubePosition.z] = labelIds;
////                                                       selectCubeLabels[newCubePosition[sliceAxis] +","+ newCubePosition[staticAxis1] +","+ newCubePosition[staticAxis2]] = labelIds;
//
//                                                        var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
//                                                        var Nmaterial = new THREE.MeshBasicMaterial({
//                                                            color: "#FFFF00",
//                                                            vertexColors: true,
//                                                            opacity: 0.6,
//                                                            transparent: true
//                                                        });
//
//                                                        var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
//                                                        ncube.position.set(newCubePosition.x - ((xlen / 2 * cubeSize) + cubeSize / 2), newCubePosition.y - ((ylen / 2 * cubeSize) + cubeSize / 2), newCubePosition.z + ((zlen / 2 * cubeSize) + cubeSize / 2));
//                                                        scene.add(ncube);
//                                                        selectedCubesArray.push(ncube);
//
//                                                    }
//                                                }
//
//                                                $.each(selectCubeLabels, function (indx) {
//                                                    var selectedLableIds = this;
//                                                    $("#" + chartId + "_" + sliceAxis + "_" + selectedLableIds[sliceAxis]).addClass("highLightLabel");
//                                                })
//
//                                                selectCubeLabels['lineSlice'] = 'Y';
//                                                multiSelectCubeLabelsObj[chartId] = selectCubeLabels;
//                                                selectedCubesArrayObj[chartId] = selectedCubesArray;
//
//
//
//                                            } else {
//                                                //single dice code 
//                                                //showDialog("Incorrect Selection");
//
//
//                                                var selectedCubes = multiSelectCubeLabels;
//                                                var pos = cubePosition;
//                                                var prevCubePosArr = Object.keys(multiSelectCubeLabels)[0].split(",");
//                                                var prevCubePos = {};
//                                                prevCubePos['x'] = parseInt(prevCubePosArr[0]);
//                                                prevCubePos['y'] = parseInt(prevCubePosArr[1]);
//                                                prevCubePos['z'] = parseInt(prevCubePosArr[2]);
//
//                                                resetCubes("", chartId);
//
//
//                                                var zDiff = cubePosition.z / 100 - prevCubePos['z'] / 100;
//                                                var yDiff = cubePosition.y / 100 - prevCubePos['y'] / 100;
//                                                var xDiff = cubePosition.x / 100 - prevCubePos['x'] / 100;
//                                                var zeroDiffCount = 0;
//                                                if (zDiff == 0) {
//                                                    zeroDiffCount++;
//                                                }
//                                                if (yDiff == 0) {
//                                                    zeroDiffCount++;
//                                                }
//                                                if (xDiff == 0) {
//                                                    zeroDiffCount++;
//                                                }
//                                                if (zeroDiffCount == 2) {
//                                                    multiSelectCubeLabels['lineSlice'] = 'Y';
//                                                }
//                                                var colorCode = "";
//                                                if (zeroDiffCount == 0)
//                                                {
//                                                    colorCode = $("#BAR_CHART_CUBE_DICE_AXIS_COLORS").val();
//                                                } else if (zeroDiffCount == 1)
//                                                {
//                                                    colorCode = $("#BAR_CHART_CUBE_SLICE_AXIS_COLORS").val();
//                                                } else if (zeroDiffCount == 2)
//                                                {
//                                                    colorCode = $("#BAR_CHART_CUBE_SLICE_AXIS_COLORS").val();
//                                                }
//                                                for (var k = 0; k <= Math.abs(zDiff); k++) {
//                                                    for (var j = 0; j <= Math.abs(yDiff); j++) {
//
//                                                        for (var i = 0; i <= Math.abs(xDiff); i++) {
//
//
//                                                            var currentCubePos = {};
//
//                                                            currentCubePos['x'] = prevCubePos.x + ((xDiff != 0) ? (xDiff / Math.abs(xDiff) * i * 100) : 0);
//                                                            currentCubePos['y'] = prevCubePos.y + ((yDiff != 0) ? (yDiff / Math.abs(yDiff) * j * 100) : 0);
//                                                            currentCubePos['z'] = prevCubePos.z + ((zDiff != 0) ? (zDiff / Math.abs(zDiff) * k * 100) : 0);
//
//                                                            var xindex = Math.abs(currentCubePos.x / cubeSize);
//                                                            var xlabel = labelsArray['x'][xindex - 1][0];
//                                                            var xlabelId = labelsArray['x'][xindex - 1][1];
//
//                                                            var yindex = Math.abs(currentCubePos.y / cubeSize);
//                                                            var ylabel = labelsArray['y'][yindex - 1][0];
//                                                            var ylabelId = labelsArray['y'][yindex - 1][1];
//
//                                                            var zindex = Math.abs(currentCubePos.z / cubeSize);
//                                                            var zlabel = labelsArray['z'][zindex - 1][0];
//                                                            var zlabelId = labelsArray['z'][zindex - 1][1];
//
//                                                            alert("X::" + xlabel + "  Y::" + ylabel + "  Z::" + zlabel);
//                                                            var clickedLabels = {}
//                                                            clickedLabels['x'] = xlabel;
//                                                            clickedLabels['y'] = ylabel;
//                                                            clickedLabels['z'] = zlabel;
//                                                            var clickedLabelIds = {}
//                                                            clickedLabelIds['x'] = xlabelId;
//                                                            clickedLabelIds['y'] = ylabelId;
//                                                            clickedLabelIds['z'] = zlabelId;
//
////                                                          multiSelectCubeLabels[xlabelId + ylabelId + zlabelId] = clickedLabelIds;
//                                                            multiSelectCubeLabels[currentCubePos.x + "," + currentCubePos.y + "," + currentCubePos.z] = clickedLabelIds;
//                                                            multiSelectCubeLabels['Dimension'] = '3D';
//
//                                                            $("#" + chartId + "_x_" + clickedLabelIds['x']).addClass("highLightLabel");
//                                                            $("#" + chartId + "_y_" + clickedLabelIds['y']).addClass("highLightLabel");
//                                                            $("#" + chartId + "_z_" + clickedLabelIds['z']).addClass("highLightLabel");
//
//                                                            material.opacity = 0.1;
//
//                                                            var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
//                                                            var Nmaterial = new THREE.MeshBasicMaterial({
//                                                                color: colorCode,
//                                                                vertexColors: true,
//                                                                opacity: 0.6,
//                                                                transparent: true
//                                                            });
//
//                                                            var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
//
//                                                            ncube.position.set(currentCubePos.x - ((xlen / 2 * cubeSize) + cubeSize / 2), currentCubePos.y - ((ylen / 2 * cubeSize) + cubeSize / 2), currentCubePos.z + ((zlen / 2 * cubeSize) + cubeSize / 2));
//                                                            scene.add(ncube);
//                                                            selectedCubesArray.push(ncube);
//                                                        }
//                                                    }
//                                                }
////                                                }
//
//
//
//                                                multiSelectCubeLabelsObj[chartId] = multiSelectCubeLabels;
//
//                                                selectedCubesArrayObj[chartId] = selectedCubesArray;
//                                                stopLoader();
//                                                //return false;
//                                                //single dice code 
//                                            }
//
//
//
//                                        }
//                                        // ravi dice code start
//
//                                        // ravi dice code end
//                                        else {
//                                            resetCubes("", chartId);
//                                            var multiSelectCubeLabels = multiSelectCubeLabelsObj[chartId];
//                                            if (multiSelectCubeLabels == null || jQuery.isEmptyObject(multiSelectCubeLabels)) {
//                                                multiSelectCubeLabels = {};
//                                            }
//
//                                            var selectedCubesArray = selectedCubesArrayObj[chartId];
//                                            if (selectedCubesArray == null || selectedCubesArray.length == 0) {
//                                                selectedCubesArray = [];
//                                            }
//
//                                            multiSelectCubeLabels = {};
//                                            selectedCubesArray = [];
////                                            multiSelectCubeLabels[xlabelId + ylabelId + zlabelId] = clickedLabelIds;
//                                            multiSelectCubeLabels[cubePosition.x + "," + cubePosition.y + "," + cubePosition.z] = clickedLabelIds;
//
//                                            var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
//                                            var Nmaterial = new THREE.MeshBasicMaterial({
//                                                color: $("#BAR_CHART_CUBE_CLICK_AXIS_COLORS").val(),
//                                                vertexColors: true,
//                                                opacity: 0.6,
//                                                transparent: true
//                                            });
//
//
//                                            var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
//                                            ncube.position.set(cubePosition.x - ((xlen / 2 * cubeSize) + cubeSize / 2), cubePosition.y - ((ylen / 2 * cubeSize) + cubeSize / 2), cubePosition.z + ((zlen / 2 * cubeSize) + cubeSize / 2));
//                                            ncube.position.setscene.add(ncube);
//                                            multiSelectCubeLabelsObj[chartId] = multiSelectCubeLabels;
//
//                                            selectedCubesArray.push(ncube);
//                                            selectedCubesArrayObj[chartId] = selectedCubesArray;
//
//                                        }
//                                    } else if (event.origDomEvent.ctrlKey) {
//
//                                        var multiSelectCubeLabels = multiSelectCubeLabelsObj[chartId];
//                                        if (multiSelectCubeLabels == null || jQuery.isEmptyObject(multiSelectCubeLabels)) {
//                                            multiSelectCubeLabels = {};
//                                        }
//
//                                        var selectedCubesArray = selectedCubesArrayObj[chartId];
//                                        if (selectedCubesArray == null || selectedCubesArray.length == 0) {
//                                            selectedCubesArray = [];
//                                        }
//
////                                        multiSelectCubeLabels[xlabelId + ylabelId + zlabelId] = clickedLabelIds;
//                                        multiSelectCubeLabels[cubePosition.x + "," + cubePosition.y + "," + cubePosition.z] = clickedLabelIds;
//
//                                        var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
//                                        var Nmaterial = new THREE.MeshBasicMaterial({
//                                            color: $("#BAR_CHART_CUBE_CLICK_AXIS_COLORS").val(),
//                                            vertexColors: true,
//                                            opacity: 0.6,
//                                            transparent: true
//                                        });
//
//
//                                        var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
//                                        ncube.position.set(cubePosition.x - ((xlen / 2 * cubeSize) + cubeSize / 2), cubePosition.y - ((ylen / 2 * cubeSize) + cubeSize / 2), cubePosition.z + ((zlen / 2 * cubeSize) + cubeSize / 2));
//
//                                        scene.add(ncube);
//                                        multiSelectCubeLabelsObj[chartId] = multiSelectCubeLabels;
//
//                                        selectedCubesArray.push(ncube);
//                                        selectedCubesArrayObj[chartId] = selectedCubesArray;
//
//                                    } else {
//                                        resetCubes("", chartId);
//                                        var multiSelectCubeLabels = multiSelectCubeLabelsObj[chartId];
//                                        if (multiSelectCubeLabels == null || jQuery.isEmptyObject(multiSelectCubeLabels)) {
//                                            multiSelectCubeLabels = {};
//                                        }
//
//                                        var selectedCubesArray = selectedCubesArrayObj[chartId];
//                                        if (selectedCubesArray == null || selectedCubesArray.length == 0) {
//                                            selectedCubesArray = [];
//                                        }
//
//
//                                        multiSelectCubeLabels = {};
////                                        multiSelectCubeLabels[xlabelId + ylabelId + zlabelId] = clickedLabelIds;
//                                        multiSelectCubeLabels[cubePosition.x + "," + cubePosition.y + "," + cubePosition.z] = clickedLabelIds;
//
//                                        var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
//                                        var Nmaterial = new THREE.MeshBasicMaterial({
//                                            color: $("#BAR_CHART_CUBE_CLICK_AXIS_COLORS").val(),
//                                            vertexColors: true,
//                                            opacity: 0.6,
//                                            transparent: true
//                                        });
//
//
//                                        var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
//                                        ncube.position.set(cubePosition.x - ((xlen / 2 * cubeSize) + cubeSize / 2), cubePosition.y - ((ylen / 2 * cubeSize) + cubeSize / 2), cubePosition.z + ((zlen / 2 * cubeSize) + cubeSize / 2));
//
//                                        scene.add(ncube);
//                                        multiSelectCubeLabelsObj[chartId] = multiSelectCubeLabels;
//
//                                        selectedCubesArray.push(ncube);
//                                        selectedCubesArrayObj[chartId] = selectedCubesArray;
//                                    }
//
//                                    // for slice or Dice
//
//                                    var lineSlice = multiSelectCubeLabelsObj[chartId]['lineSlice'];
//
//
//                                    var lineSliceWhrClause = "";
//                                    var xWhereClause = "";
//                                    var yWhereClause = "";
//                                    var zWhereClause = "";
//
//                                    $.each(multiSelectCubeLabelsObj[chartId], function (key, val) {
//                                        if (key != "Dimension" && key != "lineSlice") {
//                                            if (xWhereClause.indexOf("'" + this['x'] + "',") == -1) {
//                                                xWhereClause += "'" + this['x'] + "',";
//                                            }
//                                            if (yWhereClause.indexOf("'" + this['y'] + "',") == -1) {
//                                                yWhereClause += "'" + this['y'] + "',";
//                                            }
//                                            if (zWhereClause.indexOf("'" + this['z'] + "',") == -1) {
//                                                zWhereClause += "'" + this['z'] + "',";
//                                            }
//                                        }
//                                    })
//
//                                    //cube changes
//                                    lineSliceWhrClause = axisColumns['x']['column'] + " IN (" + xWhereClause.slice(0, -1) + ") AND " + axisColumns['y']['column'] + " IN (" + yWhereClause.slice(0, -1) + ") AND " + axisColumns['z']['column'] + " IN (" + zWhereClause.slice(0, -1) + ")";
//
//
//                                    if (multiSelectCubeLabelsObj != null && ((Object.keys(multiSelectCubeLabelsObj[chartId]).length > 0 && multiSelectCubeLabelsObj[chartId]['Dimension'] !='3D' && multiSelectCubeLabelsObj[chartId]['Dimension'] !='2D' ) || 
//                                                (lineSlice == "Y"))) {
//
//                                        $.ajax({
//                                            type: "post",
//                                            traditional: true,
//                                            dataType: 'JSON',
//                                            cache: false,
//                                            url: "getCubeValue",
//                                            data: {
//                                                factTable: factTable,
//                                                cubeValueCol: cubeValueCol,
//                                                clickedLabels: JSON.stringify(clickedLabelIds),
//                                                axisColumns: JSON.stringify(axisColumns),
//                                                lineSlice: lineSlice,
//                                                lineSliceWhrClause: lineSliceWhrClause
//                                            },
//                                            success: function (response) {
//
//
//                                                var cubeValue = response['cubeValue']
////                                           scene.remove( event.target );
////                                           scene.dispose( event.target );
//                                                //event.target.dispose();
//
//                                                var elem = document.createElement('div');
//
//                                                elem.className = 'cubeLabel';
//                                                //elem.style.marginTop = '-1em';
//                                                var Label = new CSS2DObject(elem);
//                                                //Label.position.set(0,0,0);
////                                            elem.textContent = cubeValue;
//
//                                                elem.innerHTML = cubeValue;
//                                                elem.id = chartId + "_CUBEVAL_" + cubeValue;
//
//                                                ncube.add(Label);
////                                            var selectedCubesArray = selectedCubesArrayObj[chartId];
////                                            if (selectedCubesArray == null || selectedCubesArray.length == 0) {
////                                                selectedCubesArray = [];
////                                            }
////                                            selectedCubesArray.push(ncube);
////                                            selectedCubesArrayObj[chartId] = selectedCubesArray;
//
//
//                                                $("#" + chartId + "_x_" + clickedLabelIds['x']).addClass("highLightLabel");
//                                                $("#" + chartId + "_y_" + clickedLabelIds['y']).addClass("highLightLabel");
//                                                $("#" + chartId + "_z_" + clickedLabelIds['z']).addClass("highLightLabel");
//
//
//                                                stopLoader();
//
//                                            },
//                                            error: function (e) {
//                                                stopLoader();
//                                                sessionTimeout(e);
//                                            }
//                                        });
//
//                                    }
//
//                                    globalSceneObj[chartId] = scene;
//                                });
//
//
//                                domEvents.addEventListener(cube, 'dblclick', function (event) {
//                                    console.log("dbl clicked");
//                                    event.origDomEvent.preventDefault();
//                                    var cubePosition = event.target.position;
//                                    var xindex = Math.abs(cubePosition.x / cubeSize);
//                                    var xlabel = labelsArray['x'][xindex - 1][0];
//                                    var xlabelId = labelsArray['x'][xindex - 1][1];
//                                    var yindex = Math.abs(cubePosition.y / cubeSize);
//                                    var ylabel = labelsArray['y'][yindex - 1][0];
//                                    var ylabelId = labelsArray['y'][yindex - 1][1];
//                                    var zindex = Math.abs(cubePosition.z / cubeSize);
//                                    var zlabel = labelsArray['z'][zindex - 1][0];
//                                    var zlabelId = labelsArray['z'][zindex - 1][1];
//                                    alert("X::" + xlabel + "  Y::" + ylabel + "  Z::" + zlabel);
//                                    var clickedLabels = {}
//                                    clickedLabels['x'] = xlabel;
//                                    clickedLabels['y'] = ylabel;
//                                    clickedLabels['z'] = zlabel;
//                                    var clickedLabelIds = {}
//                                    clickedLabelIds['x'] = xlabelId;
//                                    clickedLabelIds['y'] = ylabelId;
//                                    clickedLabelIds['z'] = zlabelId;
//                                    var multiSelectCubeLabels = {};
//                                    multiSelectCubeLabels[xlabelId + ylabelId + zlabelId] = clickedLabelIds;
//
//                                    multiSelectCubeLabelsObj[chartId] = multiSelectCubeLabels;
//                                    showReport('', chartId);
//
//
//                                    globalSceneObj[chartId] = scene;
//                                });
//                            }
//                        }
//                    }
//
//
//
//
//                    $('#visionAnalyticCubeDropdownLabelx').on('select', function (event) {
//                        if (event.args) {
//                            var item = event.args.item;
//                            if (item != null) {
//                                var value = item.value;
//                                var source = cubeCheckBoxObj[value];
//                                $("#visionAnalyticCubeCheckx").jqxListBox({
//                                    filterable: true,
//                                    checkboxes: true,
//                                    source: source,
//                                    theme: 'energyblue',
//                                    displayMember: 'text',
//                                    valueMember: 'value'
//                                });
//                                var ids = checkedIdsObj[value];
//                                for (var id = 0; id < ids.length; id++)
//                                {
//                                    $("#visionAnalyticCubeCheckx").jqxListBox('checkItem', ids[id]);
//                                }
//                            }
//                        }
//                    });
//                    $('#visionAnalyticCubeDropdownLabely').on('select', function (event) {
//                        if (event.args) {
//                            var item = event.args.item;
//                            if (item != null) {
//                                var value = item.value;
//                                var source = cubeCheckBoxObj[value];
//                                $("#visionAnalyticCubeChecky").jqxListBox({
//                                    filterable: true,
//                                    checkboxes: true,
//                                    source: source,
//                                    theme: 'energyblue',
//                                    displayMember: 'text',
//                                    valueMember: 'value'
//                                });
//                                var ids = checkedIdsObj[value];
//                                for (var id = 0; id < ids.length; id++)
//                                {
//                                    $("#visionAnalyticCubeChecky").jqxListBox('checkItem', ids[id]);
//                                }
//                            }
//                        }
//                    });
//                    $('#visionAnalyticCubeDropdownLabelz').on('select', function (event) {
//                        if (event.args) {
//                            var item = event.args.item;
//                            if (item != null) {
//                                var value = item.value;
//                                var source = cubeCheckBoxObj[value];
//                                $("#visionAnalyticCubeCheckz").jqxListBox({
//                                    filterable: true,
//                                    checkboxes: true,
//                                    source: source,
//                                    theme: 'energyblue',
//                                    displayMember: 'text',
//                                    valueMember: 'value'
//                                });
//                                var ids = checkedIdsObj[value];
//                                for (var id = 0; id < ids.length; id++)
//                                {
//                                    $("#visionAnalyticCubeCheckz").jqxListBox('checkItem', ids[id]);
//                                }
//                            }
//                        }
//                    });
//                    var controls = new THREE.OrbitControls(camera, labelRenderer.domElement);
//                    controls.update();
//                    controls.target.set(0, 0, 0);
//                    function render() {
//
//                        if (rotateFlagCW) {
//                            $("#" + chartId + "_cubeContainer").find("div").css("display", "none");
//
////                            1800*5, 1000*5, 600*5)
//                            camera.position.x -= 180 / 2;
//                            camera.position.y -= 100 / 2;
//                            camera.position.z -= 60 / 2;
//
//                            scene.rotation.x += 0.1;
//                            scene.rotation.y += 0.1;
//                            scene.rotation.z += 0.1;
//                        }
//
//                        requestAnimationFrame(render);
//                        renderer.render(scene, camera);
//                        labelRenderer.render(scene, camera);
//
//                    }
//                    render();
//                    rotateCube(scene, camera, chartId);
//                    var qubeChartId = chartId + "_cubeContainer";
//                    var pos = $("#" + qubeChartId).css({marginLeft: -664});
//                }
//                if (q < chartDataList.length) {
//                    var jqwidgetFlag = chartDataList[q]['36'];
//                    if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Y')
//                    {
//                        getJqwidgetChart(chartDataList[q], tabId, q, chartDataList);
//                    } else if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Q')
//                    {
//                        getShowCube(chartDataList[q], tabId, q, chartDataList);
//                    } else
//                    {
//                        getChartByChart(chartDataList[q], tabId, q, chartDataList);
//                    }
//                }
//                $("#wait").css("display", "none");
//            } catch (e) {
//                if (q < chartDataList.length) {
//                    var jqwidgetFlag = chartDataList[q]['36'];
//                    if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Y')
//                    {
//                        getJqwidgetChart(chartDataList[q], tabId, q, chartDataList);
//                    } else if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Q')
//                    {
//                        getShowCube(chartDataList[q], tabId, q, chartDataList);
//                    } else
//                    {
//                        getChartByChart(chartDataList[q], tabId, q, chartDataList);
//                    }
//                }
//            }
//        },
//        error: function (e) {
//            if (q < chartDataList.length) {
//                var jqwidgetFlag = chartDataList[q]['36'];
//                if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Y')
//                {
//                    getJqwidgetChart(chartDataList[q], tabId, q, chartDataList);
//                } else if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Q')
//                {
//                    getShowCube(chartDataList[q], tabId, q, chartDataList);
//                } else
//                {
//                    getChartByChart(chartDataList[q], tabId, q, chartDataList);
//                }
//            }
//            sessionTimeout(e);
//        }
//    });
//}

function getShowCube(chartData, tabId, q, chartDataList) {
    $('#jqxTabs').jqxTabs({height: 'auto'}); //cube mdrm
    $(".visionReportandDashboard").css("height", "auto", "important");
    q++;
    var aggId = "B940D2E1679C45A6E053210110ACAAA6";
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'JSON',
        cache: false,
        url: "getQubeAnalytics",
        data: {
            'chartData': chartData,
        },
        success: function (response) {

            try {
                var result = response;
                if (result != null)
                {

                    var chartsObj = result['chartsObj'];
                    var divid = chartsObj['divId'];
                    var seqNo = chartsObj['seqNo'];
                    var chartId = chartsObj['chartId'];
                    var sizeOfChart = chartsObj['sizeOfChart'];
                    var height = chartsObj['height'];
                    var width = chartsObj['width'];
                    var factTable = chartsObj['factTable'];
                    var cubeValueCol = chartsObj['cubeValueCol'];
                    var axisColumns = chartsObj['axisColumns'];
                    var axisLabels = chartsObj['axisLabels'];
                    var labelsArray = chartsObj['labelsObject'];
                    var title = chartsObj['title']; //multi cube 
                    var cubeCheckBoxDivId = chartsObj['cubeCheckBoxDivId']; //multi cube 
                    var checkedIdsObj = chartsObj['checkedIdsObj']; //multi cube 
                    var cubeCheckBoxObj = chartsObj['cubeCheckBoxObj']; //multi cube 
                    var dropdownCubeObj = chartsObj['dropdownCubeObj']; //multi cube 
                    var colorsObj = chartsObj['colorsObj'];
                    var cubeCheckBoxLabelObj = chartsObj['cubeCheckBoxLabelObj']; //cube dimesnions 
                    var chartVal = chartsObj['chartVal']; //cube mdrm 
                    var factType = chartsObj['factType']; //cube mdrm 
                    //-----------------------------
                    var insertDivBefore = 0;
                    for (var j = 0; j < seqArray.length; j++) {
                        if (parseInt(seqNo) < seqArray[j]) {
                            insertDivBefore = seqArray[j];
                            break;
                        }
                    }
                    if (insertDivBefore == 0) {
                        if (seqObj["TAB_ID"] == tabId) {
                            if ($('#' + tabId + "_" + chartId + "_" + seqNo).length) {
                            } else {
                                $("#" + tabId).append("<div id='" + tabId + "_" + chartId + "_" + seqNo + "'>" + divid + "</div>");
                                seqArray.push(parseInt(seqNo));
                                seqObj[parseInt(seqNo)] = tabId + "_" + chartId + "_" + seqNo;
                            }
                        }
                    } else {
                        if (seqObj["TAB_ID"] == tabId) {
                            if ($('#' + tabId + "_" + chartId + "_" + seqNo).length) {
                            } else {
                                $("<div id='" + tabId + "_" + chartId + "_" + seqNo + "'>" + divid + "</div>").insertBefore($("#" + seqObj[insertDivBefore]));
                                seqArray.push(parseInt(seqNo));
                                seqObj[parseInt(seqNo)] = tabId + "_" + chartId + "_" + seqNo;
                            }
                        }
                    }
                    seqArray = seqArray.sort(function (a, b) {
                        return a - b;
                    });
                    //----------------------------------------
                    $("#" + chartId + "_factTable").val(factTable);
                    $("#" + chartId + "_cubeValueCol").val(cubeValueCol);
                    $("#" + chartId + "_factType").val(factType);
                    $("#" + chartId + "_axisColumns").val(JSON.stringify(axisColumns));
                    $("#" + chartId + "_axisLabels").val(JSON.stringify(axisLabels));
                    $("#" + chartId + "_cubeListBoxLabelsObj").val(JSON.stringify(cubeCheckBoxLabelObj));
                    $("#cubeChartId").val(chartId + "_Chart"); //multi cube
                    $("#visionAnalyticCubeChartCheckBox").html(cubeCheckBoxDivId);
                    for (var key in dropdownCubeObj)
                    {
                        var source = dropdownCubeObj[key];
                        if ($('#visionAnalyticCubeDropdownLabel' + key).length != 0) {
                            $('#visionAnalyticCubeDropdownLabel' + key).jqxDropDownList({
                                source: source,
                                theme: 'energyblue',
                                displayMember: 'text',
                                valueMember: 'value',
                                dropDownHeight: 105,
                                dropDownWidth: 107
                            });
                            $("#visionAnalyticCubeDropdownLabel" + key).jqxDropDownList('selectItem', key);
                        }
                    }
                    for (var key in cubeCheckBoxObj)
                    {
                        var source = cubeCheckBoxObj[key];
                        if ($("#visionAnalyticCubeCheck" + key).length != 0) {
                            $("#visionAnalyticCubeCheck" + key).jqxListBox({
                                filterable: true,
                                checkboxes: true,
                                source: source,
                                theme: 'energyblue',
                                displayMember: 'text',
                                valueMember: 'value'
                            });
                            var ids = checkedIdsObj[key];
                            if (ids != null && !jQuery.isEmptyObject(ids)) {
                                for (var id = 0; id < ids.length; id++)
                                {
                                    $("#visionAnalyticCubeCheck" + key).jqxListBox('checkItem', ids[id]);
                                }
                            }
                        }
                    }
                    if (sizeOfChart != null && sizeOfChart != '' && sizeOfChart != undefined) {
                        $("#" + chartId + "_Chart").css("width", "" + sizeOfChart + "%", "important");
                    } else if (windowWidth > 1024)
                    {
                        $("#" + chartId + "_Chart").css('width', '99%', 'important');
                    } else if (windowWidth < 1024 && windowWidth > 700)
                    {
                        $("#" + chartId + "_Chart").css('width', '49%');
                    } else if (windowWidth < 699 && windowWidth > 320)
                    {
                        $("#" + chartId + "_Chart").css('width', '98%');
                    }

                    $("#" + chartId).html("<div id ='" + chartId + "_cubeContainer' class='VisionShowCubeChart'></div>");
                    $("#" + chartId + "_cube_types").val(title); //multi cube
                    var checkBoxObjCheckedVal = {};
                    alert("checked");
                    var selectedXvalues = $("#visionAnalyticCubeDropdownLabelx").jqxDropDownList('getSelectedItem');
                    var selectedYvalues = $("#visionAnalyticCubeDropdownLabely").jqxDropDownList('getSelectedItem');
                    var selectedZvalues = $("#visionAnalyticCubeDropdownLabelz").jqxDropDownList('getSelectedItem');
                    var checkedXvalues = $("#visionAnalyticCubeCheckx").jqxListBox('getCheckedItems');
                    var checkedYvalues = $("#visionAnalyticCubeChecky").jqxListBox('getCheckedItems');
                    var checkedZvalues = $("#visionAnalyticCubeCheckz").jqxListBox('getCheckedItems');
                    var xValues = [];
                    var yValues = [];
                    var zValues = [];
                    $.each(checkedXvalues, function (index) {
                        xValues.push(this.value);
                    });
                    $.each(checkedYvalues, function (index) {
                        yValues.push(this.value);
                    });
                    $.each(checkedZvalues, function (index) {
                        zValues.push(this.value);
                    });
                    checkBoxObjCheckedVal['x'] = xValues;
                    checkBoxObjCheckedVal['y'] = yValues;
                    checkBoxObjCheckedVal['z'] = zValues;
                    $("#" + chartId + "_checkBoxChecked").remove();
                    $("#cubeCheckBoxCheckedValues").append("<input type='hidden' id='" + chartId + "_checkBoxChecked' value=''/>");
                    $("#" + chartId + "_checkBoxChecked").val(JSON.stringify(checkBoxObjCheckedVal));
                    var noOfCubes = 3;
                    var cubeSize = 100;
                    var scene = new THREE.Scene();
                    var camera = new THREE.PerspectiveCamera(45, 2.5, 1, 10000);
                    var renderer = new THREE.WebGLRenderer({antialias: true});
                    renderer.setSize(parseInt(width) + 600, height);
                    scene.background = new THREE.Color(0xFFFFFF);
//                     scene.position.set(300, 300, -300);

                    document.querySelector("#" + chartId + "_cubeContainer").appendChild(renderer.domElement);
//                    camera.position.set(1800, 1000, 600);
                    camera.position.set(1800 * 5, 1000 * 5, 600 * 5);
                    var raycaster = new THREE.Raycaster();
                    var mouse = new THREE.Vector2();
                    const axesHelper = new THREE.AxesHelper((noOfCubes + 5) * 100);
//                    scene.add(axesHelper);
                    renderer.setPixelRatio(window.devicePixelRatio);
                    renderer.setSize(parseInt(width) + 600, height);
                    var labelRenderer = new CSS2DRenderer();
                    labelRenderer.setSize(parseInt(width) + 600, height);
                    labelRenderer.domElement.style.position = 'absolute';
                    labelRenderer.domElement.style.top = '0px';
                    document.querySelector('#' + chartId + "_cubeContainer").appendChild(labelRenderer.domElement);
                    var domEvents = new THREEx.DomEvents(camera, labelRenderer.domElement);
                    var axisArray = [];
                    var xlen = labelsArray['x'].length;
                    var ylen = labelsArray['y'].length;
                    var zlen = labelsArray['z'].length;
                    var xIndex = 1;
                    for (var j = -xlen / 2; j < xlen / 2; j++) {
                        var sign = j / Math.abs(j);
                        var elem = document.createElement('div');
                        elem.className = 'label';
                        elem.style.marginTop = '-1em';
                        var Label = new CSS2DObject(elem);
                        var xpoints = [];
                        if (j != (xlen / 2 - 1)) {
                            xpoints.push(new THREE.Vector3((j - 0.5) * cubeSize, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
                            xpoints.push(new THREE.Vector3((j + 0.5) * cubeSize, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
                        } else {

                            xpoints.push(new THREE.Vector3((j - 0.5) * cubeSize, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
                            xpoints.push(new THREE.Vector3((j + 0.5) * cubeSize * 2, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
//                            xpoints.push(new THREE.Vector3((j) * cubeSize, (j/Math.abs(j))* (ylen/2)* cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
//                            xpoints.push(new THREE.Vector3((j * cubeSize) + 1.5 * cubeSize, (j/Math.abs(j))* (ylen/2)* cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
                        }
                        var xmaterial = new THREE.LineBasicMaterial({color: 0x0071C5});
                        var xgeometry = new THREE.BufferGeometry().setFromPoints(xpoints);
                        var xaxis = new THREE.Line(xgeometry, xmaterial);
                        scene.add(xaxis);
                        Label.position.set((j + 0.5) * cubeSize, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize);
                        elem.textContent = labelsArray['x'][xIndex - 1][0];
                        elem.id = chartId + "_x_" + labelsArray['x'][xIndex - 1][1];
                        xaxis.add(Label);
                        axisArray.push(xaxis)
                        xIndex++;
                    }


                    var yIndex = 1;
                    for (var j = -(ylen / 2); j < ylen / 2; j++) {

                        var elem = document.createElement('div');
                        elem.className = 'label';
                        elem.style.marginTop = '-1em';
                        var Label = new CSS2DObject(elem);
                        var ypoints = [];
                        if (j != (ylen / 2 - 1)) {

                            ypoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize, (j - 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
                            ypoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize, (j + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
//                            ypoints.push(new THREE.Vector3((j/Math.abs(j))* (xlen/2)* cubeSize, (j - 1) * cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
//                            ypoints.push(new THREE.Vector3((j/Math.abs(j))* (xlen/2)* cubeSize, (j * cubeSize), (j/Math.abs(j))* (zlen/2)* cubeSize));
                        } else {

                            ypoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize, (j - 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
                            ypoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize, (j + 0.5) * cubeSize * 1.5, (zlen / 2 + 0.5) * cubeSize));
//                            ypoints.push(new THREE.Vector3((j/Math.abs(j))* (xlen/2)* cubeSize, (j - 1) * cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
//                            ypoints.push(new THREE.Vector3((j/Math.abs(j))* (xlen/2)* cubeSize, (j * cubeSize) + 1 * cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
                        }

                        var ymaterial = new THREE.LineBasicMaterial({color: 0x0071C5});
                        var ygeometry = new THREE.BufferGeometry().setFromPoints(ypoints);
                        var yaxis = new THREE.Line(ygeometry, ymaterial);
                        scene.add(yaxis);
                        Label.position.set(-(xlen / 2 + 0.5) * cubeSize, (j + 0.5) * cubeSize - 20, (zlen / 2 + 0.5) * cubeSize);
                        elem.textContent = labelsArray['y'][yIndex - 1][0];
                        elem.id = chartId + "_y_" + labelsArray['y'][yIndex - 1][1];
                        yaxis.add(Label);
                        axisArray.push(yaxis)
                        yIndex++;
                    }


                    var zIndex = 1;
                    for (var j = -(zlen / 2); j < zlen / 2; j++) {
                        var noOfCubes = labelsArray['x'].length;
                        var elem = document.createElement('div');
                        elem.className = 'label';
                        elem.style.marginTop = '-1em';
                        var Label = new CSS2DObject(elem);
                        var zpoints = [];
                        if (j != (zlen / 2 - 1)) {

                            zpoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j - 0.5) * cubeSize));
                            zpoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j + 0.5) * cubeSize));
//                            zpoints.push(new THREE.Vector3((noOfCubes * cubeSize + cubeSize), (j/Math.abs(j))* (ylen/2)* cubeSize, -(j - 1) * cubeSize));
//                            zpoints.push(new THREE.Vector3((noOfCubes * cubeSize + cubeSize), (j/Math.abs(j))* (ylen/2)* cubeSize, -(j * cubeSize)));
                        } else {

                            zpoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j - 0.5) * cubeSize));
                            zpoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j + 0.5) * cubeSize * 1.5));
//                            zpoints.push(new THREE.Vector3((noOfCubes * cubeSize + cubeSize), (j/Math.abs(j))* (ylen/2)* cubeSize, -(j - 1) * cubeSize));
//                            zpoints.push(new THREE.Vector3((noOfCubes * cubeSize + cubeSize), (j/Math.abs(j))* (ylen/2)* cubeSize, -((j * cubeSize) + 1 * cubeSize)));
                        }

                        var zmaterial = new THREE.LineBasicMaterial({color: 0x0071C5});
                        var zgeometry = new THREE.BufferGeometry().setFromPoints(zpoints);
                        var zaxis = new THREE.Line(zgeometry, zmaterial);
                        scene.add(zaxis);
                        Label.position.set(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j + 0.5) * cubeSize);
//                        Label.position.set(noOfCubes * cubeSize + cubeSize, (j/Math.abs(j))* (ylen/2)* cubeSize, -j * cubeSize);
                        elem.textContent = labelsArray['z'][zIndex - 1][0];
                        elem.id = chartId + "_z_" + labelsArray['z'][zIndex - 1][1];
                        zaxis.add(Label);
                        axisArray.push(zaxis)
                        zIndex++;
                    }


                    var geometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                    // material
                    var material = new THREE.MeshBasicMaterial({
                        color: 0xffffff,
                        vertexColors: true,
                        opacity: 0.5,
                        transparent: true
                    });
                    if (colorsObj != null && !jQuery.isEmptyObject(colorsObj)) //cube mdrm
                    {
                        red = new THREE.Color(colorsObj['x']);
                        green = new THREE.Color(colorsObj['y']);
                        blue = new THREE.Color(colorsObj['z']);
                    }
                    var colors = [red, green, blue];
                    for (var i = 0; i < 3; i++) {
                        geometry.faces[4 * i].color = colors[i];
                        geometry.faces[4 * i + 1].color = colors[i];
                        geometry.faces[4 * i + 2].color = colors[i];
                        geometry.faces[4 * i + 3].color = colors[i];
                    }
                    var cubeObjects = [];
                    var relativeX = 0;
                    var relativeY = 0;
                    var relativeZ = 0;
                    for (var i = -(xlen / 2); i < xlen / 2; i++) {

                        relativeX = relativeX + cubeSize;
                        relativeY = 0;
                        relativeZ = 0;
                        for (var j = -(ylen / 2); j < ylen / 2; j++) {
                            relativeY = relativeY + cubeSize;
                            relativeZ = 0;
                            for (var k = -(zlen / 2); k < zlen / 2; k++) {
                                relativeZ = relativeZ - cubeSize;
                                var cube = new THREE.Mesh(geometry, material);
                                scene.add(cube);
                                var edges = new THREE.EdgesGeometry(geometry);
                                line = new THREE.LineSegments(edges, new THREE.LineBasicMaterial({color: 0x000000, opacity: 0.5, transparent: true}));
                                cube.add(line);
                                cube.position.set((i + 0.5) * cubeSize, (j + 0.5) * cubeSize, -(k + 0.5) * cubeSize);
                                var relativePosition = {};
                                relativePosition.x = relativeX;
                                relativePosition.y = relativeY;
                                relativePosition.z = relativeZ;
                                cube.userData['relativePosition'] = relativePosition;
                                cubeObjects.push(cube);

                                // CLICK EVENT----->>>>>

                                domEvents.addEventListener(cube, 'click', function (event) {


                                    console.log("clicked");
                                    event.origDomEvent.preventDefault();
                                    cubeClickFlag = true;
                                    material.opacity = 0.5;
                                    globalMaterial[chartId] = material;
                                    var originalcubePosition = event.target.position;
                                    var cubePosition = event.target.userData['relativePosition'];
//                                    cubePosition.x = cubePosition.x+300;
//                                    cubePosition.y = cubePosition.y+300;
//                                    cubePosition.z = cubePosition.z-300;
                                    var xindex = Math.abs((cubePosition.x) / cubeSize);
                                    var xlabel = labelsArray['x'][xindex - 1][0];
                                    var xlabelId = labelsArray['x'][xindex - 1][1];
                                    var yindex = Math.abs((cubePosition.y) / cubeSize);
                                    var ylabel = labelsArray['y'][yindex - 1][0];
                                    var ylabelId = labelsArray['y'][yindex - 1][1];
                                    var zindex = Math.abs((cubePosition.z) / cubeSize);
                                    var zlabel = labelsArray['z'][zindex - 1][0];
                                    var zlabelId = labelsArray['z'][zindex - 1][1];
                                    alert("X::" + xlabel + "  Y::" + ylabel + "  Z::" + zlabel);
                                    var clickedLabels = {}
                                    clickedLabels['x'] = xlabel;
                                    clickedLabels['y'] = ylabel;
                                    clickedLabels['z'] = zlabel;

                                    var clickedLabelIds = {}
                                    clickedLabelIds['x'] = xlabelId;
                                    clickedLabelIds['y'] = ylabelId;
                                    clickedLabelIds['z'] = zlabelId;

                                    var firstSelectedCubeLebel = {};

                                    if (event.origDomEvent.shiftKey) {

                                        var multiSelectCubeLabels = multiSelectCubeLabelsObj[chartId];
                                        if (multiSelectCubeLabels == null || jQuery.isEmptyObject(multiSelectCubeLabels)) {
                                            multiSelectCubeLabels = {};
                                        }

                                        var selectedCubesArray = selectedCubesArrayObj[chartId];
                                        if (selectedCubesArray == null || selectedCubesArray.length == 0) {
                                            selectedCubesArray = [];
                                        }

                                        if (multiSelectCubeLabels != null && Object.keys(multiSelectCubeLabels).length == 1) {
                                            firstSelectedCubeLebel = multiSelectCubeLabels
                                            var matchCount = 0;
                                            var values = Object.values(multiSelectCubeLabels)[0];
                                            var staticAxi = [];
                                            var sliceAxis;
                                            if (values['x'] == clickedLabelIds['x']) {
                                                matchCount++;
                                                staticAxi.push('x');
                                            } else {
                                                sliceAxis = 'x';
                                            }
                                            if (values['y'] == clickedLabelIds['y']) {
                                                matchCount++;
                                                staticAxi.push('y');
                                            } else {
                                                sliceAxis = 'y';
                                            }
                                            if (values['z'] == clickedLabelIds['z']) {
                                                matchCount++;
                                                staticAxi.push('z');
                                            } else {
                                                sliceAxis = 'z';
                                            }
                                            if (matchCount == 5) {

                                                $(".label").removeClass("highLightLabel");
                                                $.each(selectedCubesArray, function (index) {

                                                    scene.remove(selectedCubesArray[index]);
                                                    $(".cubeLabel").remove();
                                                    $(".cubeValueLabel").remove();
                                                })


                                                labelsArray[sliceAxis];
//                                                var selectedCubesArray = selectedCubesArrayObj[chartId];
//                                                 if (selectedCubesArray==null || selectedCubesArray.length==0){
//                                                     selectedCubesArray = [];
//                                                 }
                                                var selectCubeLabels = {};
                                                var addOrder;
                                                var sliceAxisPos;
                                                var newCubePosition = {};
                                                var cubeReached = false;
                                                for (var i = 0; i < labelsArray[sliceAxis].length; i++) {

                                                    if (!cubeReached) {
                                                        if (i == 0) {
                                                            var yellowCube = selectedCubesArray[0];
                                                            newCubePosition = yellowCube.position;
                                                            sliceAxisPos = newCubePosition[sliceAxis];
                                                            if (Math.abs(cubePosition[sliceAxis]) - Math.abs(newCubePosition[sliceAxis]) >= 0) {
                                                                addOrder = "plus";
                                                            } else {
                                                                addOrder = "minus";
                                                            }

                                                        } else {
                                                            if (addOrder == "plus") {
                                                                newCubePosition[sliceAxis] = sliceAxisPos + (sliceAxisPos / Math.abs(sliceAxisPos) * i * cubeSize);
                                                                if (newCubePosition[sliceAxis] == event.target.position[sliceAxis]) {
                                                                    cubeReached = true;
                                                                }
                                                            } else if (addOrder == "minus") {

                                                                newCubePosition[sliceAxis] = sliceAxisPos - (sliceAxisPos / Math.abs(sliceAxisPos) * i * cubeSize);
                                                                if (newCubePosition[sliceAxis] == event.target.position[sliceAxis]) {
                                                                    cubeReached = true;
                                                                }
                                                            }

                                                        }
                                                        if (sliceAxis == 'z') {
                                                            var sliceAxisindex = Math.abs((newCubePosition[sliceAxis]) / cubeSize);
                                                        } else {
                                                            var sliceAxisindex = Math.abs((newCubePosition[sliceAxis]) / cubeSize);
                                                        }

                                                        var sliceAxislabel = labelsArray[sliceAxis][sliceAxisindex - 1][0];
                                                        var sliceAxislabelId = labelsArray[sliceAxis][sliceAxisindex - 1][1];

                                                        var staticAxis1 = staticAxi[0];
                                                        if (staticAxis1 == 'z') {
                                                            var staticAxis1index = Math.abs((newCubePosition[staticAxis1]) / cubeSize);
                                                        } else {
                                                            var staticAxis1index = Math.abs((newCubePosition[staticAxis1]) / cubeSize);
                                                        }

                                                        var staticAxis1label = labelsArray[staticAxis1][staticAxis1index - 1][0];
                                                        var staticAxis1labelId = labelsArray[staticAxis1][staticAxis1index - 1][1];

                                                        var staticAxis2 = staticAxi[1];
                                                        if (staticAxis2 == 'z') {
                                                            var staticAxis2index = Math.abs((newCubePosition[staticAxis2]) / cubeSize);
                                                        } else {
                                                            var staticAxis2index = Math.abs((newCubePosition[staticAxis2]) / cubeSize);
                                                        }

                                                        var staticAxis2label = labelsArray[staticAxis2][staticAxis2index - 1][0];
                                                        var staticAxis2labelId = labelsArray[staticAxis2][staticAxis2index - 1][1];

                                                        var labels = {}
                                                        labels[sliceAxis] = sliceAxislabel;
                                                        labels[staticAxis1] = staticAxis1label;
                                                        labels[staticAxis2] = staticAxis2label;

                                                        var labelIds = {}
                                                        labelIds[sliceAxis] = sliceAxislabelId;
                                                        labelIds[staticAxis1] = staticAxis1labelId;
                                                        labelIds[staticAxis2] = staticAxis2labelId;

//                                                        selectCubeLabels[sliceAxislabelId + staticAxis1labelId + staticAxis2labelId] = labelIds;
                                                        selectCubeLabels[newCubePosition.x + "," + newCubePosition.y + "," + newCubePosition.z] = labelIds;
//                                                       selectCubeLabels[newCubePosition[sliceAxis] +","+ newCubePosition[staticAxis1] +","+ newCubePosition[staticAxis2]] = labelIds;

                                                        var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                                                        var Nmaterial = new THREE.MeshBasicMaterial({
                                                            color: "#FFFF00",
                                                            vertexColors: true,
                                                            opacity: 0.6,
                                                            transparent: true
                                                        });

                                                        var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
                                                        ncube.position.set(newCubePosition.x - ((xlen / 2 * cubeSize) + cubeSize / 2), newCubePosition.y - ((ylen / 2 * cubeSize) + cubeSize / 2), newCubePosition.z + ((zlen / 2 * cubeSize) + cubeSize / 2));
                                                        scene.add(ncube);
                                                        selectedCubesArray.push(ncube);

                                                    }
                                                }

                                                $.each(selectCubeLabels, function (indx) {
                                                    var selectedLableIds = this;
                                                    $("#" + chartId + "_" + sliceAxis + "_" + selectedLableIds[sliceAxis]).addClass("highLightLabel");
                                                })

                                                selectCubeLabels['lineSlice'] = 'Y';
                                                multiSelectCubeLabelsObj[chartId] = selectCubeLabels;
                                                selectedCubesArrayObj[chartId] = selectedCubesArray;



                                            } else {
                                                //single dice code 
                                                //showDialog("Incorrect Selection");


                                                var selectedCubes = multiSelectCubeLabels;
                                                var pos = cubePosition;
                                                var prevCubePosArr = Object.keys(multiSelectCubeLabels)[0].split(",");
                                                var prevCubePos = {};
                                                prevCubePos['x'] = parseInt(prevCubePosArr[0]);
                                                prevCubePos['y'] = parseInt(prevCubePosArr[1]);
                                                prevCubePos['z'] = parseInt(prevCubePosArr[2]);

                                                resetCubes("", chartId);


                                                var zDiff = cubePosition.z / 100 - prevCubePos['z'] / 100;
                                                var yDiff = cubePosition.y / 100 - prevCubePos['y'] / 100;
                                                var xDiff = cubePosition.x / 100 - prevCubePos['x'] / 100;
                                                var zeroDiffCount = 0;
                                                if (zDiff == 0) {
                                                    zeroDiffCount++;
                                                }
                                                if (yDiff == 0) {
                                                    zeroDiffCount++;
                                                }
                                                if (xDiff == 0) {
                                                    zeroDiffCount++;
                                                }
                                                if (zeroDiffCount == 2) {
                                                    multiSelectCubeLabels['lineSlice'] = 'Y';
                                                }
                                                var colorCode = "";
                                                if (zeroDiffCount == 0)
                                                {
                                                    colorCode = $("#BAR_CHART_CUBE_DICE_AXIS_COLORS").val();
                                                } else if (zeroDiffCount == 1)
                                                {
                                                    colorCode = $("#BAR_CHART_CUBE_SLICE_AXIS_COLORS").val();
                                                } else if (zeroDiffCount == 2)
                                                {
                                                    colorCode = $("#BAR_CHART_CUBE_SLICE_AXIS_COLORS").val();
                                                }
                                                for (var k = 0; k <= Math.abs(zDiff); k++) {
                                                    for (var j = 0; j <= Math.abs(yDiff); j++) {

                                                        for (var i = 0; i <= Math.abs(xDiff); i++) {


                                                            var currentCubePos = {};

                                                            currentCubePos['x'] = prevCubePos.x + ((xDiff != 0) ? (xDiff / Math.abs(xDiff) * i * 100) : 0);
                                                            currentCubePos['y'] = prevCubePos.y + ((yDiff != 0) ? (yDiff / Math.abs(yDiff) * j * 100) : 0);
                                                            currentCubePos['z'] = prevCubePos.z + ((zDiff != 0) ? (zDiff / Math.abs(zDiff) * k * 100) : 0);

                                                            var xindex = Math.abs(currentCubePos.x / cubeSize);
                                                            var xlabel = labelsArray['x'][xindex - 1][0];
                                                            var xlabelId = labelsArray['x'][xindex - 1][1];

                                                            var yindex = Math.abs(currentCubePos.y / cubeSize);
                                                            var ylabel = labelsArray['y'][yindex - 1][0];
                                                            var ylabelId = labelsArray['y'][yindex - 1][1];

                                                            var zindex = Math.abs(currentCubePos.z / cubeSize);
                                                            var zlabel = labelsArray['z'][zindex - 1][0];
                                                            var zlabelId = labelsArray['z'][zindex - 1][1];

                                                            alert("X::" + xlabel + "  Y::" + ylabel + "  Z::" + zlabel);
                                                            var clickedLabels = {}
                                                            clickedLabels['x'] = xlabel;
                                                            clickedLabels['y'] = ylabel;
                                                            clickedLabels['z'] = zlabel;
                                                            var clickedLabelIds = {}
                                                            clickedLabelIds['x'] = xlabelId;
                                                            clickedLabelIds['y'] = ylabelId;
                                                            clickedLabelIds['z'] = zlabelId;

//                                                          multiSelectCubeLabels[xlabelId + ylabelId + zlabelId] = clickedLabelIds;
                                                            multiSelectCubeLabels[currentCubePos.x + "," + currentCubePos.y + "," + currentCubePos.z] = clickedLabelIds;
                                                            multiSelectCubeLabels['Dimension'] = '3D';

                                                            $("#" + chartId + "_x_" + clickedLabelIds['x']).addClass("highLightLabel");
                                                            $("#" + chartId + "_y_" + clickedLabelIds['y']).addClass("highLightLabel");
                                                            $("#" + chartId + "_z_" + clickedLabelIds['z']).addClass("highLightLabel");

                                                            material.opacity = 0.1;
                                                            globalMaterial[chartId] = material;

                                                            var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                                                            var Nmaterial = new THREE.MeshBasicMaterial({
                                                                color: colorCode,
                                                                vertexColors: true,
                                                                opacity: 0.6,
                                                                transparent: true
                                                            });

                                                            var ncube = new THREE.Mesh(Ngeometry, Nmaterial);

                                                            ncube.position.set(currentCubePos.x - ((xlen / 2 * cubeSize) + cubeSize / 2), currentCubePos.y - ((ylen / 2 * cubeSize) + cubeSize / 2), currentCubePos.z + ((zlen / 2 * cubeSize) + cubeSize / 2));
                                                            scene.add(ncube);
                                                            selectedCubesArray.push(ncube);
                                                        }
                                                    }
                                                }
//                                                }



                                                multiSelectCubeLabelsObj[chartId] = multiSelectCubeLabels;

                                                selectedCubesArrayObj[chartId] = selectedCubesArray;
                                                stopLoader();
//return false;
//single dice code 
                                            }



                                        }
// ravi dice code start

// ravi dice code end
                                        else {
                                            resetCubes("", chartId);
                                            var multiSelectCubeLabels = multiSelectCubeLabelsObj[chartId];
                                            if (multiSelectCubeLabels == null || jQuery.isEmptyObject(multiSelectCubeLabels)) {
                                                multiSelectCubeLabels = {};
                                            }

                                            var selectedCubesArray = selectedCubesArrayObj[chartId];
                                            if (selectedCubesArray == null || selectedCubesArray.length == 0) {
                                                selectedCubesArray = [];
                                            }

                                            multiSelectCubeLabels = {};
                                            selectedCubesArray = [];
//                                            multiSelectCubeLabels[xlabelId + ylabelId + zlabelId] = clickedLabelIds;
                                            multiSelectCubeLabels[cubePosition.x + "," + cubePosition.y + "," + cubePosition.z] = clickedLabelIds;

                                            var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                                            var Nmaterial = new THREE.MeshBasicMaterial({
                                                color: $("#BAR_CHART_CUBE_CLICK_AXIS_COLORS").val(),
                                                vertexColors: true,
                                                opacity: 0.6,
                                                transparent: true
                                            });


                                            var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
                                            ncube.position.set(cubePosition.x - ((xlen / 2 * cubeSize) + cubeSize / 2), cubePosition.y - ((ylen / 2 * cubeSize) + cubeSize / 2), cubePosition.z + ((zlen / 2 * cubeSize) + cubeSize / 2));
                                            ncube.position.setscene.add(ncube);
                                            multiSelectCubeLabelsObj[chartId] = multiSelectCubeLabels;

                                            selectedCubesArray.push(ncube);
                                            selectedCubesArrayObj[chartId] = selectedCubesArray;

                                        }

                                    } else {//cube mdrm
                                        resetCubes("", chartId);
                                        var multiSelectCubeLabels = multiSelectCubeLabelsObj[chartId];
                                        if (multiSelectCubeLabels == null || jQuery.isEmptyObject(multiSelectCubeLabels)) {
                                            multiSelectCubeLabels = {};
                                        }

                                        var selectedCubesArray = selectedCubesArrayObj[chartId];
                                        if (selectedCubesArray == null || selectedCubesArray.length == 0) {
                                            selectedCubesArray = [];
                                        }


                                        multiSelectCubeLabels = {};
//                                        multiSelectCubeLabels[xlabelId + ylabelId + zlabelId] = clickedLabelIds;
                                        multiSelectCubeLabels[cubePosition.x + "," + cubePosition.y + "," + cubePosition.z] = clickedLabelIds;

                                        var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                                        var Nmaterial = new THREE.MeshBasicMaterial({
                                            color: $("#BAR_CHART_CUBE_CLICK_AXIS_COLORS").val(),
                                            vertexColors: true,
                                            opacity: 0.6,
                                            transparent: true
                                        });


                                        var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
                                        ncube.position.set(cubePosition.x - ((xlen / 2 * cubeSize) + cubeSize / 2), cubePosition.y - ((ylen / 2 * cubeSize) + cubeSize / 2), cubePosition.z + ((zlen / 2 * cubeSize) + cubeSize / 2));

                                        scene.add(ncube);
                                        multiSelectCubeLabelsObj[chartId] = multiSelectCubeLabels;

                                        selectedCubesArray.push(ncube);
                                        selectedCubesArrayObj[chartId] = selectedCubesArray;
                                        if ((axisColumns['x']['column']).indexOf("Q_ID") > -1)
                                        {
                                            var clickedValue = clickedLabelIds['x'];
                                            $("#" + chartId + "_cubeQuarterClickValue").val(clickedValue);

                                        } else if ((axisColumns['y']['column']).indexOf("Q_ID") > -1)
                                        {
                                            var clickedValue = clickedLabelIds['y'];
                                            $("#" + chartId + "_cubeQuarterClickValue").val(clickedValue);
                                        } else if ((axisColumns['z']['column']).indexOf("Q_ID") > -1)
                                        {
                                            var clickedValue = clickedLabelIds['z'];
                                            $("#" + chartId + "_cubeQuarterClickValue").val(clickedValue);
                                        }
                                    }

// for slice or Dice

                                    var lineSlice = multiSelectCubeLabelsObj[chartId]['lineSlice'];


                                    var lineSliceWhrClause = "";
                                    var xWhereClause = "";
                                    var yWhereClause = "";
                                    var zWhereClause = "";

                                    $.each(multiSelectCubeLabelsObj[chartId], function (key, val) {
                                        if (key != "Dimension" && key != "lineSlice") {
                                            if (xWhereClause.indexOf("'" + this['x'] + "',") == -1) {
                                                xWhereClause += "'" + this['x'] + "',";
                                            }
                                            if (yWhereClause.indexOf("'" + this['y'] + "',") == -1) {
                                                yWhereClause += "'" + this['y'] + "',";
                                            }
                                            if (zWhereClause.indexOf("'" + this['z'] + "',") == -1) {
                                                zWhereClause += "'" + this['z'] + "',";
                                            }
                                        }
                                    })

//cube changes
                                    lineSliceWhrClause = axisColumns['x']['column'] + " IN (" + xWhereClause.slice(0, -1) + ") AND " + axisColumns['y']['column'] + " IN (" + yWhereClause.slice(0, -1) + ") AND " + axisColumns['z']['column'] + " IN (" + zWhereClause.slice(0, -1) + ")";

                                    if (multiSelectCubeLabelsObj != null && ((Object.keys(multiSelectCubeLabelsObj[chartId]).length > 0 && multiSelectCubeLabelsObj[chartId]['Dimension'] != '3D' && multiSelectCubeLabelsObj[chartId]['Dimension'] != '2D') ||
                                            (lineSlice == "Y"))) {

                                        $.ajax({
                                            type: "post",
                                            traditional: true,
                                            dataType: 'JSON',
                                            cache: false,
                                            url: "getCubeValue",
                                            data: {
                                                factTable: factTable,
                                                cubeValueCol: cubeValueCol,
                                                clickedLabels: JSON.stringify(clickedLabelIds),
                                                axisColumns: JSON.stringify(axisColumns),
                                                lineSlice: lineSlice,
                                                lineSliceWhrClause: lineSliceWhrClause,
                                                chartId: chartId,
                                                factType: factType
                                            },
                                            success: function (response) {

                                                var cubeValue = response['cubeValue']
                                                var elem = document.createElement('div');
                                                elem.className = 'cubeValueLabel';

                                                var Label = new CSS2DObject(elem);


                                                elem.innerHTML = cubeValue;
                                                elem.id = chartId + "_CUBEVAL_" + cubeValue;
                                                ncube.add(Label);



                                                $("#" + chartId + "_x_" + clickedLabelIds['x']).addClass("highLightLabel");
                                                $("#" + chartId + "_y_" + clickedLabelIds['y']).addClass("highLightLabel");
                                                $("#" + chartId + "_z_" + clickedLabelIds['z']).addClass("highLightLabel");
                                                stopLoader();
                                            },
                                            error: function (e) {
                                                stopLoader();
                                                sessionTimeout(e);
                                            }
                                        });

                                    }
                                    globalSceneObj[chartId] = scene;

                                });


                                domEvents.addEventListener(cube, 'dblclick', function (event) {
                                    console.log("dbl clicked");
                                    event.origDomEvent.preventDefault();
                                    var cubePosition = event.target.position;
                                    var xindex = Math.abs(cubePosition.x / cubeSize);
                                    var xlabel = labelsArray['x'][xindex - 1][0];
                                    var xlabelId = labelsArray['x'][xindex - 1][1];
                                    var yindex = Math.abs(cubePosition.y / cubeSize);
                                    var ylabel = labelsArray['y'][yindex - 1][0];
                                    var ylabelId = labelsArray['y'][yindex - 1][1];
                                    var zindex = Math.abs(cubePosition.z / cubeSize);
                                    var zlabel = labelsArray['z'][zindex - 1][0];
                                    var zlabelId = labelsArray['z'][zindex - 1][1];
                                    alert("X::" + xlabel + "  Y::" + ylabel + "  Z::" + zlabel);
                                    var clickedLabels = {}
                                    clickedLabels['x'] = xlabel;
                                    clickedLabels['y'] = ylabel;
                                    clickedLabels['z'] = zlabel;
                                    var clickedLabelIds = {}
                                    clickedLabelIds['x'] = xlabelId;
                                    clickedLabelIds['y'] = ylabelId;
                                    clickedLabelIds['z'] = zlabelId;
                                    var multiSelectCubeLabels = {};
                                    multiSelectCubeLabels[xlabelId + ylabelId + zlabelId] = clickedLabelIds;

                                    multiSelectCubeLabelsObj[chartId] = multiSelectCubeLabels;
                                    showReport('', chartId);


                                    globalSceneObj[chartId] = scene;
                                });
                            }
                        }
                    }




                    $('#visionAnalyticCubeDropdownLabelx').on('select', function (event) {
                        if (event.args) {
                            var item = event.args.item;
                            if (item != null) {
                                var cubeListBoxLabelsObj = $("#" + chartId + "_cubeListBoxLabelsObj").val();
                                if (cubeListBoxLabelsObj != null && cubeListBoxLabelsObj != '' && cubeListBoxLabelsObj != undefined)
                                {
                                    cubeListBoxLabelsObj = JSON.parse(cubeListBoxLabelsObj);
                                }
                                var label = item.label;
                                var selectValue = item.value;
                                var value = cubeListBoxLabelsObj[label];
                                var source = cubeCheckBoxObj[selectValue];
                                var xaxisColumnsVal = axisColumns['x'];
                                var xaxisLable = xaxisColumnsVal.label;
                                cubeListBoxLabelsObj[label] = 'x';
                                cubeListBoxLabelsObj[xaxisLable] = value;
                                var axisColumnsVal = axisColumns[value];
                                axisColumns['x'] = axisColumnsVal;
                                axisColumns[value] = xaxisColumnsVal;
                                var xaxisLablesVal = axisLabels['x'];
                                var axisLablesVal = axisLabels[value];
                                axisLabels['x'] = axisLablesVal;
                                axisLabels[value] = xaxisLablesVal;
                                $("#" + chartId + "_axisLabels").val(JSON.stringify(axisLabels));
                                $("#" + chartId + "_axisColumns").val(JSON.stringify(axisColumns));
                                $("#" + chartId + "_cubeListBoxLabelsObj").val(JSON.stringify(cubeListBoxLabelsObj));
                                $("#visionAnalyticCubeCheckx").jqxListBox({
                                    filterable: true,
                                    checkboxes: true,
                                    source: source,
                                    theme: 'energyblue',
                                    displayMember: 'text',
                                    valueMember: 'value'
                                });
                                var ids = checkedIdsObj[value];
                                var xids = checkedIdsObj['x'];
                                checkedIdsObj['x'] = ids;
                                checkedIdsObj[value] = xids;
                                $("#" + chartId + "_checkedIdsObj").val(JSON.stringify(checkedIdsObj));
                                for (var id = 0; id < ids.length; id++)
                                {
                                    $("#visionAnalyticCubeCheckx").jqxListBox('checkItem', ids[id]);
                                }
                            }
                        }
                    });
                    $('#visionAnalyticCubeDropdownLabely').on('select', function (event) {
                        if (event.args) {
                            var item = event.args.item;
                            if (item != null) {
                                var cubeListBoxLabelsObj = $("#" + chartId + "_cubeListBoxLabelsObj").val();
                                if (cubeListBoxLabelsObj != null && cubeListBoxLabelsObj != '' && cubeListBoxLabelsObj != undefined)
                                {
                                    cubeListBoxLabelsObj = JSON.parse(cubeListBoxLabelsObj);
                                }
                                var label = item.label;
                                var selectValue = item.value;
                                var value = cubeListBoxLabelsObj[label];
                                var source = cubeCheckBoxObj[selectValue];
                                var yaxisColumnsVal = axisColumns['y'];
                                var yaxisLable = yaxisColumnsVal.label;
                                cubeListBoxLabelsObj[label] = 'y';
                                cubeListBoxLabelsObj[yaxisLable] = value;
                                var axisColumnsVal = axisColumns[value];
                                axisColumns['y'] = axisColumnsVal;
                                axisColumns[value] = yaxisColumnsVal;
                                var yaxisLablesVal = axisLabels['y'];
                                var axisLablesVal = axisLabels[value];
                                axisLabels['y'] = axisLablesVal;
                                axisLabels[value] = yaxisLablesVal;
                                $("#" + chartId + "_axisLabels").val(JSON.stringify(axisLabels));
                                $("#" + chartId + "_axisColumns").val(JSON.stringify(axisColumns));
                                $("#" + chartId + "_cubeListBoxLabelsObj").val(JSON.stringify(cubeListBoxLabelsObj));
                                $("#visionAnalyticCubeChecky").jqxListBox({
                                    filterable: true,
                                    checkboxes: true,
                                    source: source,
                                    theme: 'energyblue',
                                    displayMember: 'text',
                                    valueMember: 'value'
                                });
                                var ids = checkedIdsObj[value];
                                var yids = checkedIdsObj['y'];
                                checkedIdsObj['y'] = ids;
                                checkedIdsObj[value] = yids;
                                $("#" + chartId + "_checkedIdsObj").val(JSON.stringify(checkedIdsObj));
                                for (var id = 0; id < ids.length; id++)
                                {
                                    $("#visionAnalyticCubeChecky").jqxListBox('checkItem', ids[id]);
                                }
                            }
                        }
                    });
                    $('#visionAnalyticCubeDropdownLabelz').on('select', function (event) {
                        if (event.args) {
                            var item = event.args.item;
                            if (item != null) {
                                var cubeListBoxLabelsObj = $("#" + chartId + "_cubeListBoxLabelsObj").val();
                                if (cubeListBoxLabelsObj != null && cubeListBoxLabelsObj != '' && cubeListBoxLabelsObj != undefined)
                                {
                                    cubeListBoxLabelsObj = JSON.parse(cubeListBoxLabelsObj);
                                }
                                var label = item.label;
                                var selectValue = item.value;
                                var value = cubeListBoxLabelsObj[label];
                                var source = cubeCheckBoxObj[selectValue];
                                var zaxisColumnsVal = axisColumns['z'];
                                var zaxisLable = zaxisColumnsVal.label;
                                cubeListBoxLabelsObj[label] = 'z';
                                cubeListBoxLabelsObj[zaxisLable] = value;
                                var axisColumnsVal = axisColumns[value];
                                axisColumns['z'] = axisColumnsVal;
                                axisColumns[value] = zaxisColumnsVal;
                                var zaxisLablesVal = axisLabels['z'];
                                var axisLablesVal = axisLabels[value];
                                axisLabels['z'] = axisLablesVal;
                                axisLabels[value] = zaxisLablesVal;
                                $("#" + chartId + "_axisLabels").val(JSON.stringify(axisLabels));
                                $("#" + chartId + "_axisColumns").val(JSON.stringify(axisColumns));
                                $("#" + chartId + "_cubeListBoxLabelsObj").val(JSON.stringify(cubeListBoxLabelsObj));
                                $("#visionAnalyticCubeCheckz").jqxListBox({
                                    filterable: true,
                                    checkboxes: true,
                                    source: source,
                                    theme: 'energyblue',
                                    displayMember: 'text',
                                    valueMember: 'value'
                                });
                                var ids = checkedIdsObj[value];
                                var zids = checkedIdsObj['z'];
                                checkedIdsObj['z'] = ids;
                                checkedIdsObj[value] = zids;
                                $("#" + chartId + "_checkedIdsObj").val(JSON.stringify(checkedIdsObj));
                                for (var id = 0; id < ids.length; id++)
                                {
                                    $("#visionAnalyticCubeCheckz").jqxListBox('checkItem', ids[id]);
                                }
                            }
                        }
                    });
                    var controls = new THREE.OrbitControls(camera, labelRenderer.domElement);
                    controls.update();
                    controls.target.set(0, 0, 0);

                    function render() {

                        if (rotateFlagCW) {
                            $("#" + chartId + "_cubeContainer").find("div").css("display", "none");
//                            1800*5, 1000*5, 600*5)
                            camera.position.x -= 180 / 2;
                            camera.position.y -= 100 / 2;
                            camera.position.z -= 60 / 2;
                            scene.rotation.x += 0.1;
                            scene.rotation.y += 0.1;
                            scene.rotation.z += 0.1;
                        }

                        requestAnimationFrame(render);
                        renderer.render(scene, camera);
                        labelRenderer.render(scene, camera);

                    }
                    render();
                    rotateCube(scene, camera, chartId);
                    var qubeChartId = chartId + "_cubeContainer";
                    var pos = $("#" + qubeChartId).css({marginLeft: -664});
                    var dragControls = new THREE.DragControls(cubeObjects, camera, labelRenderer.domElement);
                    var tempCubes = [];
                    dragControls.addEventListener('dragstart', function (event) {

                        if (event.object.type == 'LineSegments') {
                            var cubePosition = event.object.position;
                            var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                            var Nmaterial = new THREE.MeshBasicMaterial({
                                color: 0xffffff,
                                vertexColors: true,
                                opacity: 0.5,
                                transparent: true
                            });
                            for (var i = 0; i < 3; i++) {
                                Ngeometry.faces[4 * i].color = colors[i];
                                Ngeometry.faces[4 * i + 1].color = colors[i];
                                Ngeometry.faces[4 * i + 2].color = colors[i];
                                Ngeometry.faces[4 * i + 3].color = colors[i];
                            }
                            var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
                            ncube.position.set(event.object.position.x, event.object.position.y, event.object.position.z);

                            tempCubes.push(ncube);
                            cubeObjects.push(ncube);
                            ncube.userData['relativePosition'] = cubePosition;
                        }
                        if (event.object.type == 'Mesh') {
                            controls.enabled = false;
                            var cubePosition = event.object.position;
                            var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                            var Nmaterial = new THREE.MeshBasicMaterial({
                                color: 0xffffff,
                                vertexColors: true,
                                opacity: 0.5,
                                transparent: true
                            });
                            for (var i = 0; i < 3; i++) {
                                Ngeometry.faces[4 * i].color = colors[i];
                                Ngeometry.faces[4 * i + 1].color = colors[i];
                                Ngeometry.faces[4 * i + 2].color = colors[i];
                                Ngeometry.faces[4 * i + 3].color = colors[i];
                            }
                            var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
                            ncube.position.set(event.object.position.x, event.object.position.y, event.object.position.z);
                            scene.add(ncube);
                            tempCubes = [];
                            tempCubes.push(ncube);
                            cubeObjects.push(ncube);
                            ncube.userData['relativePosition'] = cubePosition;

                        }


                    });
                    dragControls.addEventListener('drag', function (event) {

                        if (event.object.type == 'LineSegments') {

                        }
                        if (event.object.type == 'Mesh') {
                            controls.enabled = false;
                            $("#dropAreaId").empty();
                            $("#dropAreaId").remove();
                            var str = "<div id ='dropAreaId' class='cubecheckclass'>"
                                    + "<div id='spanDiv' style='text-align: center; margin: 0 auto;'><span>Drop here</span></div>"
                                    + "<div id='cubeDropId'></div></div>";
                            $("#" + chartId + "_Parent").append(str);
                            $("#dropAreaId").css('height', '350px');
                            $("#dropAreaId").css('width', '350px');
                            $("#dropAreaId").css('position', 'absolute');
                            $("#dropAreaId").css('z-index', '9999');
                            $('#dropAreaId').css('border', '1px solid black');
                            $('#dropAreaId').css({marginLeft: 450});
                            $('#dropAreaId').css({marginTop: 50});
                            $("#spanDiv").css('height', '30px', '!important');


                        }


                    });
                    var xlabelId = '';
                    var ylabelId = '';
                    var zlabelId = '';
                    var n = 0;
                    divArray = [];
                    dragControls.addEventListener('dragend', function (event) {
                        if (event.object.type == 'LineSegments') {
                            var tempCube = tempCubes[0];
                            event.object.position.set(tempCube.position.x, tempCube.position.y, tempCube.position.z);
                            tempCube.geometry.dispose();
                            tempCube.material.dispose();
                            scene.remove(tempCube);
                        }

                        if (event.object.type == 'Mesh') {
                            controls.enabled = true;
                            var cubePosition = event.object.userData['relativePosition'];
                            if (cubePosition != null) {
                                var xindex = Math.abs((cubePosition.x) / cubeSize);
                                var xlabel = labelsArray['x'][xindex - 1][0];
                                xlabelId = labelsArray['x'][xindex - 1][1];
                                var yindex = Math.abs((cubePosition.y) / cubeSize);
                                var ylabel = labelsArray['y'][yindex - 1][0];
                                ylabelId = labelsArray['y'][yindex - 1][1];
                                var zindex = Math.abs((cubePosition.z) / cubeSize);
                                var zlabel = labelsArray['z'][zindex - 1][0];
                                zlabelId = labelsArray['z'][zindex - 1][1];
                            }
                            var tempCube = tempCubes[0];
                            event.object.position.set(tempCube.position.x, tempCube.position.y, tempCube.position.z);
                            tempCube.geometry.dispose();
                            tempCube.material.dispose();
                            scene.remove(tempCube);
                            if (divArray.length >= 2)
                            {
                                removeFirstCube();
                            }
                            n++;
                            var clickedValuesObj = {};
                            clickedValuesObj['x'] = xlabelId;
                            clickedValuesObj['y'] = ylabelId;
                            clickedValuesObj['z'] = zlabelId;
                            setTimeout(function () {
                                make_canvas(tabId, chartId, chartVal, clickedValuesObj, n);
                            }, 10);

                        }

                    });

//ravinder uncommented code for click purpose by jagadish //cube mdrm
//                    var pos = $("#" + qubeChartId).find("canvas").position();
//                    var pos1 = $("#" + qubeChartId).find("div:first").position();
//                    $("#" + qubeChartId).find("div:first").css({top: pos.top, left: pos.left});
//                    var pos2 = $("#" + qubeChartId).find("div:first").position();
//
//                    $("#" + tabId).scroll(function () {
//                        var pos = $("#" + qubeChartId).find("canvas").position();
//                        var pos1 = $("#" + qubeChartId).find("div:first").position();
//                        $("#" + qubeChartId).find("div:first").css({top: pos.top, left: pos.left});
//                        var pos2 = $("#" + qubeChartId).find("div:first").position();
//                    })
                }
                if (q < chartDataList.length) {
                    var jqwidgetFlag = chartDataList[q]['36'];
                    if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Y')
                    {
                        getJqwidgetChart(chartDataList[q], tabId, q, chartDataList);
                    } else if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Q')
                    {
                        getShowCube(chartDataList[q], tabId, q, chartDataList);
                    } else
                    {
                        getChartByChart(chartDataList[q], tabId, q, chartDataList);
                    }
                }
                $("#wait").css("display", "none");
            } catch (e) {
                if (q < chartDataList.length) {
                    var jqwidgetFlag = chartDataList[q]['36'];
                    if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Y')
                    {
                        getJqwidgetChart(chartDataList[q], tabId, q, chartDataList);
                    } else if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Q')
                    {
                        getShowCube(chartDataList[q], tabId, q, chartDataList);
                    } else
                    {
                        getChartByChart(chartDataList[q], tabId, q, chartDataList);
                    }
                }
            }
        },
        error: function (e) {
            if (q < chartDataList.length) {
                var jqwidgetFlag = chartDataList[q]['36'];
                if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Y')
                {
                    getJqwidgetChart(chartDataList[q], tabId, q, chartDataList);
                } else if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Q')
                {
                    getShowCube(chartDataList[q], tabId, q, chartDataList);
                } else
                {
                    getChartByChart(chartDataList[q], tabId, q, chartDataList);
                }
            }
            sessionTimeout(e);
        }
    });
}
function showReport(analyticId, chartId) {
    var labelsObject = {};
    var cubeValueCol = $("#" + chartId + "_cubeValueCol").val();
    var factTable = $("#" + chartId + "_factTable").val();
    var axisColumnsStr = $("#" + chartId + "_axisColumns").val();
    var axisLabelsStr = $("#" + chartId + "_axisLabels").val();
    var checkBoxObjCheckedVal = $("#" + chartId + "_checkBoxChecked").val(); //cube changes
    var factType = $("#" + chartId + "_factType").val(); //cube changes
    var axisColumns = {};
    if (axisColumnsStr != null && axisColumnsStr != '' && axisColumnsStr != undefined)
    {
        axisColumns = JSON.parse(axisColumnsStr);
    }
    var axisLabels = {};
    if (axisLabelsStr != null && axisLabelsStr != '' && axisLabelsStr != undefined)
    {
        axisLabels = JSON.parse(axisLabelsStr);
    }
    var multiSelectCubeLabels = multiSelectCubeLabelsObj[chartId];
    var multiWhereCond = [];
    var exportIcon = '<div id="exportDropdown" class="visionCubeReportExport exportDropdown visionSearchExport visionSearchExportDiv" style="vertical-align: bottom; display: inline-block; padding-top: 8px; float:left;"><table style="vertical-align: bottom; display: inline-block;" class="visionSearchExportTable"><tbody><tr><td><select id="exportsearchResults" onchange="getExportType(\'searchResults\')"><option data-optlabel="Select" value="" selected="selected">Select</option><option data-optlabel="XLSX" value="Xlsx">XLSX</option><option data-optlabel="CSV" value="CSV">CSV</option></select></td></tr></tbody></table> <input value="Export" id="excelExportsearchResults" onclick="finalExport(\'searchResults\',\'S\')" class="exportClass visionSearchExportButton" type="button" style="background: url(&quot;images/export_as_xlsx_icon_blue.png&quot;) 5px center no-repeat rgb(255, 255, 255);"></div>';

    if (!jQuery.isEmptyObject(multiSelectCubeLabels)) {
        var xStatic = false;
        var yStatic = false;
        var zStatic = false;
        var prevx = "";
        var prevy = "";
        var prevz = "";
        var xCol = axisColumns['x']['column']; //cube changes
        var yCol = axisColumns['y']['column'];
        var zCol = axisColumns['z']['column'];
        var nonStaticWhereCond = "";
        var nonStaticCol = "";
        var nonStaticAxis = "";
        var url;
        var count = Object.keys(multiSelectCubeLabels).length;
        if (count == 1) {
            var values = Object.values(multiSelectCubeLabels)[0];
            labelsObject[xCol] = values['x']
            labelsObject[yCol] = values['y']
            labelsObject[zCol] = values['z']
            url = "getCubeData";
        } else if (count > 1) {

            if (multiSelectCubeLabels['Dimension'] == '3D') {
                var diceWhereClause = "";
                var xWhereClause = "";
                var yWhereClause = "";
                var zWhereClause = "";
                url = "getCubeDiceData";
                $.each(multiSelectCubeLabels, function (key, val) {
                    if (key != "Dimension" && key != "lineSlice") {
                        if (xWhereClause.indexOf("'" + this['x'] + "',") == -1) {
                            xWhereClause += "'" + this['x'] + "',";
                        }
                        if (yWhereClause.indexOf("'" + this['y'] + "',") == -1) {
                            yWhereClause += "'" + this['y'] + "',";
                        }
                        if (zWhereClause.indexOf("'" + this['z'] + "',") == -1) {
                            zWhereClause += "'" + this['z'] + "',";
                        }

                    }
                })
//cube changes
                diceWhereClause = axisColumns['x']['column'] + " IN (" + xWhereClause.slice(0, -1) + ") AND " + axisColumns['y']['column'] + " IN (" + yWhereClause.slice(0, -1) + ") AND " + axisColumns['z']['column'] + " IN (" + zWhereClause.slice(0, -1) + ")";
            } else if (multiSelectCubeLabels['Dimension'] == '2D') {
                url = "getCubeSliceData";
                $.each(multiSelectCubeLabels, function (key, val) {
                    if (key != "Dimension" && key != "lineSlice") {
                        if (prevx == val['x']) {
                            prevx = val['x'];
                            xStatic = true;
                            labelsObject[xCol] = val['x'];
                        } else {
                            if (prevx != "") {

                                nonStaticAxis = "x";
                                nonStaticCol = factTable + "." + xCol;
                            }
                            prevx = val['x'];
                            xStatic = false;
                            delete labelsObject[xCol];
                        }

                        if (prevy == val['y']) {
                            prevy = val['y'];
                            yStatic = true;
                            labelsObject[yCol] = val['y'];
                        } else {
                            if (prevy != "") {
                                nonStaticAxis = "y";
                                nonStaticCol = factTable + "." + yCol;
                            }
                            prevy = val['y'];
                            yStatic = false;
                            delete labelsObject[yCol];
                        }
                        if (prevz == val['z']) {
                            prevz = val['z'];
                            zStatic = true;
                            labelsObject[zCol] = val['z'];
                        } else {
                            if (prevx != "") {
                                nonStaticAxis = "z";
                                nonStaticCol = factTable + "." + zCol;
                            }
                            prevz = val['z'];
                            zStatic = false;
                            delete labelsObject[zCol];
                        }
                    }
                })
            } else {
                url = "getMultiCubeData";
                $.each(multiSelectCubeLabels, function (key, val) {
                    if (key != "Dimension" && key != "lineSlice") {
                        var multiWhereCondition = "";
                        var valuesX = labelsObject[xCol];
                        if (valuesX == null)
                        {
                            valuesX = [];
                        }
                        multiWhereCondition += factTable + "." + xCol + "=" + val['x'] + " AND ";
                        valuesX.push(val['x']);
                        labelsObject[xCol] = valuesX;

                        var valuesY = labelsObject[yCol];
                        if (valuesY == null)
                        {
                            valuesY = [];
                        }
                        valuesY.push(val['y']);
                        labelsObject[yCol] = valuesY;
                        multiWhereCondition += factTable + "." + yCol + "=" + val['y'] + " AND ";

                        var valuesZ = labelsObject[zCol];
                        if (valuesZ == null)
                        {
                            valuesZ = [];
                        }
                        valuesZ.push(val['z']);
                        labelsObject[zCol] = valuesZ;
                        multiWhereCondition += factTable + "." + zCol + "=" + val['z'];
                        multiWhereCond.push(multiWhereCondition);

                    }
                })


                $.each(multiSelectCubeLabels, function (key, val) {
                    if (key != "Dimension" && key != "lineSlice") {
                        if (nonStaticAxis != "") {
                            nonStaticWhereCond += "'" + val[nonStaticAxis] + "',";
                        }
                    }

                })
                if (nonStaticWhereCond != "") {
                    nonStaticWhereCond = nonStaticWhereCond.substring(0, nonStaticWhereCond.length - 1);
                    nonStaticWhereCond = " AND " + nonStaticCol + " IN (" + nonStaticWhereCond + ") ";
                }
            }

        }


        $.ajax({
            type: "post",
            traditional: true,
            dataType: 'JSON',
            cache: false,
            url: url,
            data: {
                factTable: factTable,
                cubeValueCol: cubeValueCol,
                clickedLabelIds: JSON.stringify(labelsObject),
                axisColumns: JSON.stringify(axisColumns),
                axisLabels: JSON.stringify(axisLabels),
                nonStaticWhereCond: nonStaticWhereCond,
                diceWhereClause: diceWhereClause,
                checkBoxObjCheckedVal: checkBoxObjCheckedVal, //cube changes
                multiWhereCond: JSON.stringify(multiWhereCond),
                factType: factType
            },
            success: function (response) {
                $("#treeGrid").remove();
                var treeGridStr = "<div >" + exportIcon + "<div id ='treeGrid'></div></div>";
                $("#dialog").html(treeGridStr);
                var dataFields = response['dataFields'];
                var columns = response['columns'];
                var data = response['data'];
                var hierarchy = response['hierarchy'];
                var source =
                        {
                            dataType: "json",
                            dataFields: dataFields,
                            hierarchy: {
                                root: 'children'
                            },
                            id: 'ID',
                            localData: data,
                        };
                var dataAdapter = new $.jqx.dataAdapter(source);
                $("#dialog").dialog({
                    title: (labelObject['Report'] != null ? labelObject['Report'] : 'Report'),
                    modal: true,
                    height: "auto",
//                    maxHeight: 500,
                    width: 1038,
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


// create jqxGrid.
                        $("#treeGrid").jqxTreeGrid(
                                {
                                    source: dataAdapter,
                                    width: "100%",
                                    columnsResize: true,
                                    sortable: true,
                                    editable: true,
                                    columns: columns,
                                    pageable: true,
                                    pagerMode: 'advanced',
                                    theme: 'energyblue'
                                });

                        $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                        $(this).closest(".ui-dialog").addClass("VisionCubeviewreportbox");
                        $(".visionHeaderMain").css("z-index", "999");
                        $(".visionFooterMain").css("z-index", "999");
                    },
                    beforeClose: function (event, ui)
                    {
                        $(".visionHeaderMain").css("z-index", "99999");
                        $(".visionFooterMain").css("z-index", "99999");
                    }
                });



            },
            error: function (e) {
                sessionTimeout(e);
            }
        });

    } else {

        $.ajax({
            type: "post",
            traditional: true,
            dataType: 'JSON',
            cache: false,
            url: 'getAllCubesData',
            data: {
                factTable: factTable,
                cubeValueCol: cubeValueCol,
                clickedLabelIds: JSON.stringify(labelsObject),
                axisColumns: JSON.stringify(axisColumns),
                axisLabels: JSON.stringify(axisLabels),
                nonStaticWhereCond: nonStaticWhereCond,
                checkBoxObjCheckedVal: checkBoxObjCheckedVal, //cube changes
                factType: factType
            },
            success: function (response) {
                $("#treeGrid").remove();
                var treeGridStr = "<div >" + exportIcon + "<div id ='treeGrid'></div></div>";
                $("#dialog").html(treeGridStr);
                var dataFields = response['dataFields'];
                var columns = response['columns'];
                var data = response['data'];
                var hierarchy = response['hierarchy'];
                var source =
                        {
                            dataType: "json",
                            dataFields: dataFields,
                            hierarchy: {
                                root: 'children'
                            },
                            id: 'ID',
                            localData: data,
                        };
                var dataAdapter = new $.jqx.dataAdapter(source);
                $("#dialog").dialog({
                    title: (labelObject['Report'] != null ? labelObject['Report'] : 'Report'),
                    modal: true,
                    height: "auto",
                    maxHeight: 500,
                    width: 1038,
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


// create jqxGrid.
                        $("#treeGrid").jqxTreeGrid(
                                {
                                    source: dataAdapter,
                                    width: "100%",
                                    columnsResize: true,
                                    columns: columns,
                                    pageable: true,
                                    sortable: true,
                                    filterable: true,
                                    pagerMode: 'advanced',
                                    theme: 'energyblue'
                                });


                        $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                        $(this).closest(".ui-dialog").addClass("VisionCubeviewreportbox");
                        $(".visionHeaderMain").css("z-index", "999");
                        $(".visionFooterMain").css("z-index", "999");
                    },
                    beforeClose: function (event, ui)
                    {
                        $(".visionHeaderMain").css("z-index", "99999");
                        $(".visionFooterMain").css("z-index", "99999");
                    }
                });



            },
            error: function (e) {
                sessionTimeout(e);
            }
        });

    }

}

function resetCubes(analyticId, chartId) {
    cubeClickFlag = false;
    var material = globalMaterial[chartId];
    material.opacity = 0.5;
    globalMaterial[chartId] = material;
    var selectedCubesArray = selectedCubesArrayObj[chartId];
    if (selectedCubesArray != null) {
        $.each(selectedCubesArray, function (index) {
            var scene = globalSceneObj[chartId];
            scene.remove(this);
            $(".cubeLabel").remove();
            $(".cubeValueLabel").remove();
            $(".label").removeClass("highLightLabel");
            $(".label").css("display", "block");
        })
    }

    multiSelectCubeLabelsObj[chartId] = {};
    selectedCubesArrayObj[chartId] = [];

    $("#" + chartId + "_cubeClickFlag").val(false);

}

//cube code ends//
//show chart image and pdf//
function downloadChartImage(chartId)
{
    $(".chartDownloadDialog").attr("chartImageData");
    $("#source-listbox").show();
    var imageData = $("#" + chartId).attr("chartImageData");
    $("#imageData").val(imageData);
    $("#dataIMG").attr("chartId", chartId);
    var chartFlag = $("#" + chartId).attr("jqwidgetChartFlag");
    $(".chartDownloadDialog").dialog({
        title: 'Download Image',
        width: 320,
        height: 150,
        buttons: {
            "Sumbit": function () {
                var selectedItemText = $("#selectedChart option:selected").text();
                var opt = $("#selectedChart option:selected").val();
                var inputType = "";
                var analysisType = $("#analysisType").val();
                if (analysisType != null && analysisType != undefined && analysisType != "" && analysisType == 'ABC') {
                    inputType = "<input type='hidden' name='aValue' value='" + $("#aValue").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='cValue' value='" + $("#cValue").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='A_Pct' value='" + $("#A_Pct").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='ACount' value='" + $("#ACount").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='BCount' value='" + $("#BCount").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='CCount' value='" + $("#CCount").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='A_Value' value='" + $("#A_Value").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='B_Value' value='" + $("#B_Value").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='C_Value' value='" + $("#C_Value").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='B_Pct' value='" + $("#B_Pct").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='C_Pct' value='" + $("#C_Pct").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='A_Value_TP' value='" + $("#A_Value_TP").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='B_Value_TP' value='" + $("#B_Value_TP").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='C_Value_TP' value='" + $("#C_Value_TP").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='Total_CPP_Pct' value='" + $("#Total_CPP_Pct").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);

                } else if (analysisType != null && analysisType != undefined && analysisType != "" && analysisType == 'XYZ') {
                    inputType = "<input type='hidden' name='X' value='" + $("#X").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='Y' value='" + $("#Y").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='Z' value='" + $("#Z").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='X_COUNT' value='" + $("#X_COUNT").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='Y_COUNT' value='" + $("#Y_COUNT").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='Z_COUNT' value='" + $("#Z_COUNT").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='X_Total_Stock' value='" + $("#X_Total_Stock").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='Y_Total_Stock' value='" + $("#Y_Total_Stock").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='Z_Total_Stock' value='" + $("#Z_Total_Stock").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='TOTAL_STOCK' value='" + $("#TOTAL_STOCK").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='X_VALUE' value='" + $("#X_VALUE").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='Y_VALUE' value='" + $("#Y_VALUE").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='Z_VALUE' value='" + $("#Z_VALUE").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);

                } else if (analysisType != null && analysisType != undefined && analysisType != "" && analysisType == 'FMSN') {
                    inputType = "<input type='hidden' name='F' value='" + $("#F").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='S' value='" + $("#S").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='N' value='" + $("#N").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='F_COUNT' value='" + $("#F_COUNT").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='S_COUNT' value='" + $("#S_COUNT").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='N_COUNT' value='" + $("#N_COUNT").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='F_Total_Stock' value='" + $("#F_Total_Stock").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='S_Total_Stock' value='" + $("#S_Total_Stock").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='N_Total_Stock' value='" + $("#N_Total_Stock").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='TOTAL_STOCK' value='" + $("#TOTAL_STOCK").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                } else if (analysisType != null && analysisType != undefined && analysisType != "" && analysisType == 'HML') {

                    inputType = "<input type='hidden' name='H' value='" + $("#H").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='M' value='" + $("#M").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='L' value='" + $("#L").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='H_COUNT' value='" + $("#H_COUNT").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='M_COUNT' value='" + $("#M_COUNT").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='L_COUNT' value='" + $("#L_COUNT").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='H_VALUE' value='" + $("#H_VALUE").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='M_VALUE' value='" + $("#M_VALUE").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='L_VALUE' value='" + $("#L_VALUE").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='H_CUM_PCT' value='" + $("#H_CUM_PCT").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='M_CUM_PCT' value='" + $("#M_CUM_PCT").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='L_CUM_PCT' value='" + $("#L_CUM_PCT").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);

                } else if (analysisType != null && analysisType != undefined && analysisType != "" && analysisType == 'VED') {
                    inputType = "<input type='hidden' name='V' value='" + $("#V").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='E' value='" + $("#E").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='D' value='" + $("#D").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='V_COUNT' value='" + $("#V_COUNT").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='E_COUNT' value='" + $("#E_COUNT").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                    inputType = "<input type='hidden' name='D_COUNT' value='" + $("#D_COUNT").val() + "'/>";
                    $("#pdfbodyForm").append(inputType);
                }
                inputType = "<input type='hidden' name='tableName' value='" + $("#tableName").val() + "'/>";
                $("#pdfbodyForm").append(inputType);
                inputType = "<input type='hidden' name='analysisType' value='" + $("#analysisType").val() + "'/>";
                $("#pdfbodyForm").append(inputType);
                inputType = "<input type='hidden' name='fromDate' value='" + $("#fromDate").val() + "'/>";
                $("#pdfbodyForm").append(inputType);
                inputType = "<input type='hidden' name='toDate' value='" + $("#toDate").val() + "'/>";
                $("#pdfbodyForm").append(inputType);
                inputType = "<input type='hidden' name='colsArray' value='" + $("#colsArray").val() + "'/>";
                $("#pdfbodyForm").append(inputType);
                inputType = "<input type='hidden' name='gridId' value='" + $("#gridId").val() + "'/>";
                $("#pdfbodyForm").append(inputType);

                if (opt == "PNG") {
                    if (chartFlag != null && chartFlag != '' && chartFlag != undefined && chartFlag == 'Y') {
                        var imageContent = $("#" + chartId).jqxChart('saveAsJPEG', 'myChart.jpeg', "test", true);
                        imageContent = "data:image/png;base64," + imageContent;
                        var showImageChart = $("<a>").attr("href", imageContent).attr("download", "img.png").appendTo("body");
                        showImageChart[0].click();
                        showImageChart.remove();
                        $(this).dialog('close');
                    } else {
                        downloadChartAsImage(chartId);
//                        $("#" + chartId).jqxChart('saveAsPNG', 'myChart.png', 'https://www.jqwidgets.com/export_server/export.php');
                        $(this).dialog('close');
                    }
                }
                if (opt == "PDF") {
                    if (chartFlag != null && chartFlag != '' && chartFlag != undefined && chartFlag == 'Y') {
                        var imageContent;
                        imageContent = "data:image/png;base64," + $("#" + chartId).jqxChart('saveAsJPEG', 'myChart.jpeg', "test", true);
                        $("#" + chartId).attr("jqxChartImageData", imageContent);
                        var jqxImageData1 = $("#" + chartId).attr("jqxChartImageData");
                        $("#jqxImageData1").val(jqxImageData1);
                        showAsJQXPdfform();
                        $(this).dialog('close');

                    } else {
                        showAsPdfform();
                        $(this).dialog('close');
                    }
                }
            }
        }
    });
}
function showAsJQXPdfform() {
    $("#jqxPdfbodyForm").submit();
}
function showAsPdfform() {
    $("#pdfbodyForm").submit();

}

function downloadChartAsImage(chartId)
{
    var imageData = $("#" + chartId).attr("chartImageData");
    var chartImage = $("<a>").attr("href", imageData).attr("download", "img.png").appendTo("body");
    chartImage[0].click();
    chartImage.remove();
}
function chartObjImg() {

    if (JQXImageContent != null && !$.isEmptyObject(JQXImageContent) && JQXImageContent != '' && JQXImageContent != undefined) {
        $("#chartImageObj").val(JSON.stringify(JQXImageContent));
        $("#pdfChartForm").submit();
    } else {
        $("#chartImageObj").val(JSON.stringify(jsonChartImg));
        $("#pdfChartForm").submit();
    }


    /*
     $("#chartImageObj").val(JSON.stringify(jsonChartImg));
     console.log($("#chartImageObj").val());
     //    $("#chartImageObj").attr("test", JSON.stringify(jsonChartImg));
     $("#pdfChartForm").submit();
     //    $(this).html(" ");
     //  $("#chartImageObj").empty();  
     console.log("test:" + jsonChartImg);
     */
}
//show chart image and pdf//



function multiCubeDropdown(analytic_Ids, chartVal)
{
    var analytic_Id;
    var analytic_Comp_Id;
    if (analytic_Ids != null && analytic_Ids != '' && analytic_Ids != undefined)
    {
        var chartIds = analytic_Ids.split(",");
        analytic_Id = chartIds[0]
        analytic_Comp_Id = chartIds[1]
    }
    console.log("iam in analyticsTypeDropdown");
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        cache: false,
        url: "analyticDropdown",
        data: {
            'tabComponentId': analytic_Comp_Id,
            'tabId': analytic_Id,
            'chartTypeId': chartVal
        },
        success: function (response) {
            if (response != null)
            {
                var result = JSON.parse(response);
                var dataObj = result['chartList'];
                for (var i = 0; i < dataObj.length; i++)
                {
                    $("#wait").css("display", "block");
                    var chartData = dataObj[i];
                    getCubeDropdownData(chartData, analytic_Id, analytic_Comp_Id, chartVal); //cube mdrm
                }
            }
        },
        error: function (e) {
            sessionTimeout(e);
        }
    });
}

function getCubeDropdownData(chartData, tabId, chartId, chartVal) {
    var checkBoxObjCheckedVal = $("#" + chartId + "_checkBoxChecked").val();
    var chartOPtAllObj = $("#" + chartId + "_colorObj").val(); //cube changes
    var clickedcolorsObj = $("#" + chartId + "_clickedcolorObj").val();
    var axisColumns = $("#" + chartId + "_axisColumns").val();
    var axisLabels = $("#" + chartId + "_axisLables").val();
    var data = {
        'chartData': chartData,
        'ddwFlag': 'Y',
        'chartVal': chartVal,
        checkBoxObjCheckedVal: checkBoxObjCheckedVal,
        cubeColors: chartOPtAllObj, //cube changes 
        cubeClickedColors: clickedcolorsObj,
        'axisColumns': axisColumns,
        'axisLabels': axisLabels
    };
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        url: "getQubeAnalytics",
        async: true,
        cache: false,
        data: data,
        success: function (response) {
            $("#wait").css("display", "none");
            if (response != null && response != '' && response != undefined) {
                var resultData = JSON.parse(response);
                if (resultData != null)
                {
                    var result = resultData;
                    if (result != null) {
                        $("#wait").css("display", "none");
                        var chartsObj = result['chartsObj'];
                        var divid = chartsObj['divId'];
                        var seqNo = chartsObj['seqNo'];
                        var chartId = chartsObj['chartId'];
                        var sizeOfChart = chartsObj['sizeOfChart'];
                        var height = chartsObj['height'];
                        var width = chartsObj['width'];
                        var factTable = chartsObj['factTable'];
                        var cubeValueCol = chartsObj['cubeValueCol'];
                        var axisColumns = chartsObj['axisColumns'];
                        var axisLabels = chartsObj['axisLabels'];
                        var labelsArray = chartsObj['labelsObject'];
                        var title = chartsObj['title'];
                        var factType = chartsObj['factType']; //cube mdrm 
                        var cubeChartId = $("#cubeChartId").val();
                        $("#" + cubeChartId).empty();
                        $('#' + cubeChartId).attr('id', chartId + '_Chart');
                        $("#cubeChartId").val(chartId + "_Chart");
                        var cubeCheckBoxDivId = chartsObj['cubeCheckBoxDivId'];
                        var checkedIdsObj = chartsObj['checkedIdsObj']; //multi cube 
                        var cubeCheckBoxObj = chartsObj['cubeCheckBoxObj']; //multi cube 
                        var dropdownCubeObj = chartsObj['dropdownCubeObj']; //multi cube 
                        var colorsObj = chartsObj['colorsObj']; //cube changes
                        var cubeCheckBoxLabelObj = chartsObj['cubeCheckBoxLabelObj'];
                        $("#" + chartId + "_Chart").html(divid);
                        $("#" + chartId + "_factTable").val(factTable);
                        $("#" + chartId + "_factType").val(factType);
                        $("#" + chartId + "_cubeValueCol").val(cubeValueCol);
                        $("#" + chartId + "_axisColumns").val(JSON.stringify(axisColumns));
                        $("#" + chartId + "_axisLabels").val(JSON.stringify(axisLabels));
                        $("#" + chartId + "_cubeCheckBoxObj").val(JSON.stringify(cubeCheckBoxObj));
                        $("#" + chartId + "_cubeListBoxLabelsObj").val(JSON.stringify(cubeCheckBoxLabelObj));
                        $("#" + chartId + "_checkedIdsObj").val(JSON.stringify(checkedIdsObj));
                        $("#visionAnalyticCubeChartCheckBox").html(cubeCheckBoxDivId);
                        for (var key in dropdownCubeObj)
                        {
                            var source = dropdownCubeObj[key];
                            if ($('#visionAnalyticCubeDropdownLabel' + key).length != 0) {
                                $('#visionAnalyticCubeDropdownLabel' + key).jqxDropDownList("destroy");
                                $('#visionAnalyticCubeDropdownLabel' + key).jqxDropDownList({
                                    source: source,
                                    theme: 'energyblue',
                                    displayMember: 'text',
                                    valueMember: 'value',
                                    dropDownHeight: 105,
                                    dropDownWidth: 107
                                });
                                $("#visionAnalyticCubeDropdownLabel" + key).jqxDropDownList('selectItem', key);
                            }
                        }
                        for (var key in cubeCheckBoxObj)
                        {
                            var source = cubeCheckBoxObj[key];
                            if ($("#visionAnalyticCubeCheck" + key).length != 0) {
                                $("#visionAnalyticCubeCheck" + key).jqxListBox({
                                    filterable: true,
                                    checkboxes: true,
                                    source: source,
                                    theme: 'energyblue',
                                    displayMember: 'text',
                                    valueMember: 'value'
                                });
                                var ids = checkedIdsObj[key];
                                if (ids != null && !jQuery.isEmptyObject(ids)) {
                                    for (var id = 0; id < ids.length; id++)
                                    {
                                        $("#visionAnalyticCubeCheck" + key).jqxListBox('checkItem', ids[id]);
                                    }
                                }
                            }
                        }
                        if (sizeOfChart != null && sizeOfChart != '' && sizeOfChart != undefined) {
                            $("#" + chartId + "_Chart").css("width", "" + sizeOfChart + "%", "important");
                        } else if (windowWidth > 1024)
                        {
                            $("#" + chartId + "_Chart").css('width', '99%', 'important');
                        } else if (windowWidth < 1024 && windowWidth > 700)
                        {
                            $("#" + chartId + "_Chart").css('width', '49%');
                        } else if (windowWidth < 699 && windowWidth > 320)
                        {
                            $("#" + chartId + "_Chart").css('width', '98%');
                        }

                        $("#" + chartId).html("<div id ='" + chartId + "_cubeContainer' class='VisionShowCubeChart'></div>");
                        $("#" + chartId + "_cube_types").val(title);



                        var checkBoxObjCheckedVal = {}; //cube changes
                        alert("checked");
                        var selectedXvalues = $("#visionAnalyticCubeDropdownLabelx").jqxDropDownList('getSelectedItem');
                        var selectedYvalues = $("#visionAnalyticCubeDropdownLabely").jqxDropDownList('getSelectedItem');
                        var selectedZvalues = $("#visionAnalyticCubeDropdownLabelz").jqxDropDownList('getSelectedItem');
                        var checkedXvalues = $("#visionAnalyticCubeCheckx").jqxListBox('getCheckedItems');
                        var checkedYvalues = $("#visionAnalyticCubeChecky").jqxListBox('getCheckedItems');
                        var checkedZvalues = $("#visionAnalyticCubeCheckz").jqxListBox('getCheckedItems');
                        var xValues = [];
                        var yValues = [];
                        var zValues = [];
                        $.each(checkedXvalues, function (index) {
                            xValues.push(this.value);
                        });
                        $.each(checkedYvalues, function (index) {
                            yValues.push(this.value);
                        });
                        $.each(checkedZvalues, function (index) {
                            zValues.push(this.value);
                        });
                        checkBoxObjCheckedVal['x'] = xValues;
                        checkBoxObjCheckedVal['y'] = yValues;
                        checkBoxObjCheckedVal['z'] = zValues;
                        $("#" + chartId + "_checkBoxChecked").remove();
                        $("#cubeCheckBoxCheckedValues").append("<input type='hidden' id='" + chartId + "_checkBoxChecked' value=''/>");
                        $("#" + chartId + "_checkBoxChecked").val(JSON.stringify(checkBoxObjCheckedVal));



                        var noOfCubes = 3;
                        var cubeSize = 100;
                        multiSelectCubeLabelsObj = {};
                        selectedCubesArrayObj = {};
                        var scene = new THREE.Scene();
                        var camera = new THREE.PerspectiveCamera(45, 2.5, 1, 10000);
                        var renderer = new THREE.WebGLRenderer({antialias: true});
                        renderer.setSize(parseInt(width) + 600, height);
                        scene.background = new THREE.Color(0xFFFFFF);

//                    addLight(-1, 2, 4);
//                    addLight(1, -1, -2);


                        document.querySelector("#" + chartId + "_cubeContainer").appendChild(renderer.domElement);
                        camera.position.set(1800 * 5, 1000 * 5, 600 * 5);

//x: 899.4776633185764
//y: 292.1430570995144
//z: 324.94979209024876


                        var raycaster = new THREE.Raycaster();
                        var mouse = new THREE.Vector2();
                        const axesHelper = new THREE.AxesHelper((noOfCubes + 5) * 100);
                        renderer.setPixelRatio(window.devicePixelRatio);
                        renderer.setSize(parseInt(width) + 600, height);
                        var labelRenderer = new CSS2DRenderer();
                        labelRenderer.setSize(parseInt(width) + 600, height);
                        labelRenderer.domElement.style.position = 'absolute';
                        labelRenderer.domElement.style.top = '0px';
                        var controls = new THREE.OrbitControls(camera, labelRenderer.domElement);
                        controls.update();
                        controls.target.set(300, 300, -300);
                        document.querySelector('#' + chartId + "_cubeContainer").appendChild(labelRenderer.domElement);
                        var domEvents = new THREEx.DomEvents(camera, labelRenderer.domElement);

                        var axisArray = [];
                        var xlen = labelsArray['x'].length;
                        var ylen = labelsArray['y'].length;
                        var zlen = labelsArray['z'].length;

                        var xIndex = 1;
                        for (var j = -xlen / 2; j < xlen / 2; j++) {
                            var sign = j / Math.abs(j);
                            var elem = document.createElement('div');
                            elem.className = 'label';
                            elem.style.marginTop = '-1em';
                            var Label = new CSS2DObject(elem);
                            var xpoints = [];
                            if (j != (xlen / 2 - 1)) {
                                xpoints.push(new THREE.Vector3((j - 0.5) * cubeSize, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
                                xpoints.push(new THREE.Vector3((j + 0.5) * cubeSize, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
                            } else {

                                xpoints.push(new THREE.Vector3((j - 0.5) * cubeSize, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
                                xpoints.push(new THREE.Vector3((j + 0.5) * cubeSize * 2, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
//                            xpoints.push(new THREE.Vector3((j) * cubeSize, (j/Math.abs(j))* (ylen/2)* cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
//                            xpoints.push(new THREE.Vector3((j * cubeSize) + 1.5 * cubeSize, (j/Math.abs(j))* (ylen/2)* cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
                            }
                            var xmaterial = new THREE.LineBasicMaterial({color: 0x0071C5});
                            var xgeometry = new THREE.BufferGeometry().setFromPoints(xpoints);
                            var xaxis = new THREE.Line(xgeometry, xmaterial);
                            scene.add(xaxis);
                            Label.position.set((j + 0.5) * cubeSize, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize);
                            elem.textContent = labelsArray['x'][xIndex - 1][0];
                            elem.id = chartId + "_x_" + labelsArray['x'][xIndex - 1][1];
                            xaxis.add(Label);
                            axisArray.push(xaxis)
                            xIndex++;
                        }


                        var yIndex = 1;
                        for (var j = -(ylen / 2); j < ylen / 2; j++) {

                            var elem = document.createElement('div');
                            elem.className = 'label';
                            elem.style.marginTop = '-1em';
                            var Label = new CSS2DObject(elem);
                            var ypoints = [];
                            if (j != (ylen / 2 - 1)) {

                                ypoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize, (j - 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
                                ypoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize, (j + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
//                            ypoints.push(new THREE.Vector3((j/Math.abs(j))* (xlen/2)* cubeSize, (j - 1) * cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
//                            ypoints.push(new THREE.Vector3((j/Math.abs(j))* (xlen/2)* cubeSize, (j * cubeSize), (j/Math.abs(j))* (zlen/2)* cubeSize));
                            } else {

                                ypoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize, (j - 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
                                ypoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize, (j + 0.5) * cubeSize * 1.5, (zlen / 2 + 0.5) * cubeSize));
//                            ypoints.push(new THREE.Vector3((j/Math.abs(j))* (xlen/2)* cubeSize, (j - 1) * cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
//                            ypoints.push(new THREE.Vector3((j/Math.abs(j))* (xlen/2)* cubeSize, (j * cubeSize) + 1 * cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
                            }

                            var ymaterial = new THREE.LineBasicMaterial({color: 0x0071C5});
                            var ygeometry = new THREE.BufferGeometry().setFromPoints(ypoints);
                            var yaxis = new THREE.Line(ygeometry, ymaterial);
                            scene.add(yaxis);
                            Label.position.set(-(xlen / 2 + 0.5) * cubeSize, (j + 0.5) * cubeSize - 20, (zlen / 2 + 0.5) * cubeSize);
                            elem.textContent = labelsArray['y'][yIndex - 1][0];
                            elem.id = chartId + "_y_" + labelsArray['y'][yIndex - 1][1];
                            yaxis.add(Label);
                            axisArray.push(yaxis)
                            yIndex++;

                        }


                        var zIndex = 1;
                        for (var j = -(zlen / 2); j < zlen / 2; j++) {
                            var noOfCubes = labelsArray['x'].length;
                            var elem = document.createElement('div');
                            elem.className = 'label';
                            elem.style.marginTop = '-1em';
                            var Label = new CSS2DObject(elem);
                            var zpoints = [];
                            if (j != (zlen / 2 - 1)) {

                                zpoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j - 0.5) * cubeSize));
                                zpoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j + 0.5) * cubeSize));
//                            zpoints.push(new THREE.Vector3((noOfCubes * cubeSize + cubeSize), (j/Math.abs(j))* (ylen/2)* cubeSize, -(j - 1) * cubeSize));
//                            zpoints.push(new THREE.Vector3((noOfCubes * cubeSize + cubeSize), (j/Math.abs(j))* (ylen/2)* cubeSize, -(j * cubeSize)));
                            } else {

                                zpoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j - 0.5) * cubeSize));
                                zpoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j + 0.5) * cubeSize * 1.5));
//                            zpoints.push(new THREE.Vector3((noOfCubes * cubeSize + cubeSize), (j/Math.abs(j))* (ylen/2)* cubeSize, -(j - 1) * cubeSize));
//                            zpoints.push(new THREE.Vector3((noOfCubes * cubeSize + cubeSize), (j/Math.abs(j))* (ylen/2)* cubeSize, -((j * cubeSize) + 1 * cubeSize)));
                            }

                            var zmaterial = new THREE.LineBasicMaterial({color: 0x0071C5});
                            var zgeometry = new THREE.BufferGeometry().setFromPoints(zpoints);
                            var zaxis = new THREE.Line(zgeometry, zmaterial);
                            scene.add(zaxis);
                            Label.position.set(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j + 0.5) * cubeSize);
//                        Label.position.set(noOfCubes * cubeSize + cubeSize, (j/Math.abs(j))* (ylen/2)* cubeSize, -j * cubeSize);
                            elem.textContent = labelsArray['z'][zIndex - 1][0];
                            elem.id = chartId + "_z_" + labelsArray['z'][zIndex - 1][1];
                            zaxis.add(Label);
                            axisArray.push(zaxis)
                            zIndex++;

                        }


                        var geometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
// material
                        var material = new THREE.MeshBasicMaterial({
                            color: 0xffffff,
                            vertexColors: true,
                            opacity: 0.5,
                            transparent: true
                        });
                        if (colorsObj != null && !jQuery.isEmptyObject(colorsObj)) //cube mdrm
                        {
                            red = new THREE.Color(colorsObj['x']);
                            green = new THREE.Color(colorsObj['y']);
                            blue = new THREE.Color(colorsObj['z']);
                        }
                        var colors = [red, green, blue];
                        for (var i = 0; i < 3; i++) {
                            geometry.faces[4 * i].color = colors[i];
                            geometry.faces[4 * i + 1].color = colors[i];
                            geometry.faces[4 * i + 2].color = colors[i];
                            geometry.faces[4 * i + 3].color = colors[i];
                        }
                        var cubeObjects = [];
                        var relativeX = 0;
                        var relativeY = 0;
                        var relativeZ = 0;
                        for (var i = -(xlen / 2); i < xlen / 2; i++) {

                            relativeX = relativeX + cubeSize;
                            relativeY = 0;
                            relativeZ = 0;
                            for (var j = -(ylen / 2); j < ylen / 2; j++) {
                                relativeY = relativeY + cubeSize;
                                relativeZ = 0;
                                for (var k = -(zlen / 2); k < zlen / 2; k++) {
                                    relativeZ = relativeZ - cubeSize;
                                    var cube = new THREE.Mesh(geometry, material);
                                    scene.add(cube);
                                    var edges = new THREE.EdgesGeometry(geometry);
                                    line = new THREE.LineSegments(edges, new THREE.LineBasicMaterial({color: 0x000000, opacity: 0.5, transparent: true}));
                                    cube.add(line);
                                    cube.position.set((i + 0.5) * cubeSize, (j + 0.5) * cubeSize, -(k + 0.5) * cubeSize);
                                    var relativePosition = {};
                                    relativePosition.x = relativeX;
                                    relativePosition.y = relativeY;
                                    relativePosition.z = relativeZ;

                                    cube.userData['relativePosition'] = relativePosition;
                                    cubeObjects.push(cube);

// CLICK EVENT----->>>>>

                                    domEvents.addEventListener(cube, 'click', function (event) {
                                        console.log("clicked");
                                        event.origDomEvent.preventDefault();
                                        cubeClickFlag = true;
                                        material.opacity = 0.5;
                                        globalMaterial[chartId] = material;
                                        var originalcubePosition = event.target.position;
                                        var cubePosition = event.target.userData['relativePosition'];
//                                    cubePosition.x = cubePosition.x+300;
//                                    cubePosition.y = cubePosition.y+300;
//                                    cubePosition.z = cubePosition.z-300;

                                        var xindex = Math.abs((cubePosition.x) / cubeSize);
                                        var xlabel = labelsArray['x'][xindex - 1][0];
                                        var xlabelId = labelsArray['x'][xindex - 1][1];
                                        var yindex = Math.abs((cubePosition.y) / cubeSize);
                                        var ylabel = labelsArray['y'][yindex - 1][0];
                                        var ylabelId = labelsArray['y'][yindex - 1][1];
                                        var zindex = Math.abs((cubePosition.z) / cubeSize);
                                        var zlabel = labelsArray['z'][zindex - 1][0];
                                        var zlabelId = labelsArray['z'][zindex - 1][1];
                                        alert("X::" + xlabel + "  Y::" + ylabel + "  Z::" + zlabel);
                                        var clickedLabels = {}
                                        clickedLabels['x'] = xlabel;
                                        clickedLabels['y'] = ylabel;
                                        clickedLabels['z'] = zlabel;

                                        var clickedLabelIds = {}
                                        clickedLabelIds['x'] = xlabelId;
                                        clickedLabelIds['y'] = ylabelId;
                                        clickedLabelIds['z'] = zlabelId;

                                        var firstSelectedCubeLebel = {};

                                        if (event.origDomEvent.shiftKey) {

                                            var multiSelectCubeLabels = multiSelectCubeLabelsObj[chartId];
                                            if (multiSelectCubeLabels == null || jQuery.isEmptyObject(multiSelectCubeLabels)) {
                                                multiSelectCubeLabels = {};
                                            }

                                            var selectedCubesArray = selectedCubesArrayObj[chartId];
                                            if (selectedCubesArray == null || selectedCubesArray.length == 0) {
                                                selectedCubesArray = [];
                                            }

                                            if (multiSelectCubeLabels != null && Object.keys(multiSelectCubeLabels).length == 1) {
                                                firstSelectedCubeLebel = multiSelectCubeLabels
                                                var matchCount = 0;
                                                var values = Object.values(multiSelectCubeLabels)[0];
                                                var staticAxi = [];
                                                var sliceAxis;
                                                if (values['x'] == clickedLabelIds['x']) {
                                                    matchCount++;
                                                    staticAxi.push('x');
                                                } else {
                                                    sliceAxis = 'x';
                                                }
                                                if (values['y'] == clickedLabelIds['y']) {
                                                    matchCount++;
                                                    staticAxi.push('y');
                                                } else {
                                                    sliceAxis = 'y';
                                                }
                                                if (values['z'] == clickedLabelIds['z']) {
                                                    matchCount++;
                                                    staticAxi.push('z');
                                                } else {
                                                    sliceAxis = 'z';
                                                }
                                                if (matchCount == 5) {

                                                    $(".label").removeClass("highLightLabel");
                                                    $.each(selectedCubesArray, function (index) {

                                                        scene.remove(selectedCubesArray[index]);
                                                        $(".cubeLabel").remove();
                                                        $(".cubeValueLabel").remove();
                                                    })


                                                    labelsArray[sliceAxis];
//                                                var selectedCubesArray = selectedCubesArrayObj[chartId];
//                                                 if (selectedCubesArray==null || selectedCubesArray.length==0){
//                                                     selectedCubesArray = [];
//                                                 }
                                                    var selectCubeLabels = {};
                                                    var addOrder;
                                                    var sliceAxisPos;
                                                    var newCubePosition = {};
                                                    var cubeReached = false;
                                                    for (var i = 0; i < labelsArray[sliceAxis].length; i++) {

                                                        if (!cubeReached) {
                                                            if (i == 0) {
                                                                var yellowCube = selectedCubesArray[0];
                                                                newCubePosition = yellowCube.position;
                                                                sliceAxisPos = newCubePosition[sliceAxis];
                                                                if (Math.abs(cubePosition[sliceAxis]) - Math.abs(newCubePosition[sliceAxis]) >= 0) {
                                                                    addOrder = "plus";
                                                                } else {
                                                                    addOrder = "minus";
                                                                }

                                                            } else {
                                                                if (addOrder == "plus") {
                                                                    newCubePosition[sliceAxis] = sliceAxisPos + (sliceAxisPos / Math.abs(sliceAxisPos) * i * cubeSize);
                                                                    if (newCubePosition[sliceAxis] == event.target.position[sliceAxis]) {
                                                                        cubeReached = true;
                                                                    }
                                                                } else if (addOrder == "minus") {

                                                                    newCubePosition[sliceAxis] = sliceAxisPos - (sliceAxisPos / Math.abs(sliceAxisPos) * i * cubeSize);
                                                                    if (newCubePosition[sliceAxis] == event.target.position[sliceAxis]) {
                                                                        cubeReached = true;
                                                                    }
                                                                }

                                                            }
                                                            if (sliceAxis == 'z') {
                                                                var sliceAxisindex = Math.abs((newCubePosition[sliceAxis]) / cubeSize);
                                                            } else {
                                                                var sliceAxisindex = Math.abs((newCubePosition[sliceAxis]) / cubeSize);
                                                            }

                                                            var sliceAxislabel = labelsArray[sliceAxis][sliceAxisindex - 1][0];
                                                            var sliceAxislabelId = labelsArray[sliceAxis][sliceAxisindex - 1][1];

                                                            var staticAxis1 = staticAxi[0];
                                                            if (staticAxis1 == 'z') {
                                                                var staticAxis1index = Math.abs((newCubePosition[staticAxis1]) / cubeSize);
                                                            } else {
                                                                var staticAxis1index = Math.abs((newCubePosition[staticAxis1]) / cubeSize);
                                                            }

                                                            var staticAxis1label = labelsArray[staticAxis1][staticAxis1index - 1][0];
                                                            var staticAxis1labelId = labelsArray[staticAxis1][staticAxis1index - 1][1];

                                                            var staticAxis2 = staticAxi[1];
                                                            if (staticAxis2 == 'z') {
                                                                var staticAxis2index = Math.abs((newCubePosition[staticAxis2]) / cubeSize);
                                                            } else {
                                                                var staticAxis2index = Math.abs((newCubePosition[staticAxis2]) / cubeSize);
                                                            }

                                                            var staticAxis2label = labelsArray[staticAxis2][staticAxis2index - 1][0];
                                                            var staticAxis2labelId = labelsArray[staticAxis2][staticAxis2index - 1][1];

                                                            var labels = {}
                                                            labels[sliceAxis] = sliceAxislabel;
                                                            labels[staticAxis1] = staticAxis1label;
                                                            labels[staticAxis2] = staticAxis2label;

                                                            var labelIds = {}
                                                            labelIds[sliceAxis] = sliceAxislabelId;
                                                            labelIds[staticAxis1] = staticAxis1labelId;
                                                            labelIds[staticAxis2] = staticAxis2labelId;

//                                                        selectCubeLabels[sliceAxislabelId + staticAxis1labelId + staticAxis2labelId] = labelIds;
                                                            selectCubeLabels[newCubePosition.x + "," + newCubePosition.y + "," + newCubePosition.z] = labelIds;
//                                                       selectCubeLabels[newCubePosition[sliceAxis] +","+ newCubePosition[staticAxis1] +","+ newCubePosition[staticAxis2]] = labelIds;

                                                            var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                                                            var Nmaterial = new THREE.MeshBasicMaterial({
                                                                color: "#FFFF00",
                                                                vertexColors: true,
                                                                opacity: 0.6,
                                                                transparent: true
                                                            });

                                                            var ncube = new THREE.Mesh(Ngeometry, Nmaterial);

                                                            ncube.position.set(newCubePosition.x - ((xlen / 2 * cubeSize) + cubeSize / 2), newCubePosition.y - ((ylen / 2 * cubeSize) + cubeSize / 2), newCubePosition.z + ((zlen / 2 * cubeSize) + cubeSize / 2));

                                                            scene.add(ncube);
                                                            selectedCubesArray.push(ncube);

                                                        }
                                                    }

                                                    $.each(selectCubeLabels, function (indx) {
                                                        var selectedLableIds = this;
                                                        $("#" + chartId + "_" + sliceAxis + "_" + selectedLableIds[sliceAxis]).addClass("highLightLabel");
                                                    })

                                                    selectCubeLabels['lineSlice'] = 'Y';
                                                    multiSelectCubeLabelsObj[chartId] = selectCubeLabels;
                                                    selectedCubesArrayObj[chartId] = selectedCubesArray;



                                                } else {
                                                    //single dice code 
                                                    //showDialog("Incorrect Selection");


                                                    var selectedCubes = multiSelectCubeLabels;
                                                    var pos = cubePosition;
                                                    var prevCubePosArr = Object.keys(multiSelectCubeLabels)[0].split(",");
                                                    var prevCubePos = {};
                                                    prevCubePos['x'] = parseInt(prevCubePosArr[0]);
                                                    prevCubePos['y'] = parseInt(prevCubePosArr[1]);
                                                    prevCubePos['z'] = parseInt(prevCubePosArr[2]);

                                                    resetCubes("", chartId);


                                                    var zDiff = cubePosition.z / 100 - prevCubePos['z'] / 100;
                                                    var yDiff = cubePosition.y / 100 - prevCubePos['y'] / 100;
                                                    var xDiff = cubePosition.x / 100 - prevCubePos['x'] / 100;
                                                    var zeroDiffCount = 0;
                                                    if (zDiff == 0) {
                                                        zeroDiffCount++;
                                                    }
                                                    if (yDiff == 0) {
                                                        zeroDiffCount++;
                                                    }
                                                    if (xDiff == 0) {
                                                        zeroDiffCount++;
                                                    }
                                                    if (zeroDiffCount == 2) {
                                                        multiSelectCubeLabels['lineSlice'] = 'Y';
                                                    }
                                                    var colorCode = "";
                                                    if (zeroDiffCount == 0)
                                                    {
                                                        colorCode = $("#BAR_CHART_CUBE_DICE_AXIS_COLORS").val();
                                                    } else if (zeroDiffCount == 1)
                                                    {
                                                        colorCode = $("#BAR_CHART_CUBE_SLICE_AXIS_COLORS").val();
                                                    } else if (zeroDiffCount == 2)
                                                    {
                                                        colorCode = $("#BAR_CHART_CUBE_SLICE_AXIS_COLORS").val();
                                                    }

                                                    for (var k = 0; k <= Math.abs(zDiff); k++) {
                                                        for (var j = 0; j <= Math.abs(yDiff); j++) {

                                                            for (var i = 0; i <= Math.abs(xDiff); i++) {


                                                                var currentCubePos = {};

                                                                currentCubePos['x'] = prevCubePos.x + ((xDiff != 0) ? (xDiff / Math.abs(xDiff) * i * 100) : 0);
                                                                currentCubePos['y'] = prevCubePos.y + ((yDiff != 0) ? (yDiff / Math.abs(yDiff) * j * 100) : 0);
                                                                currentCubePos['z'] = prevCubePos.z + ((zDiff != 0) ? (zDiff / Math.abs(zDiff) * k * 100) : 0);

                                                                var xindex = Math.abs(currentCubePos.x / cubeSize);
                                                                var xlabel = labelsArray['x'][xindex - 1][0];
                                                                var xlabelId = labelsArray['x'][xindex - 1][1];

                                                                var yindex = Math.abs(currentCubePos.y / cubeSize);
                                                                var ylabel = labelsArray['y'][yindex - 1][0];
                                                                var ylabelId = labelsArray['y'][yindex - 1][1];

                                                                var zindex = Math.abs(currentCubePos.z / cubeSize);
                                                                var zlabel = labelsArray['z'][zindex - 1][0];
                                                                var zlabelId = labelsArray['z'][zindex - 1][1];

                                                                alert("X::" + xlabel + "  Y::" + ylabel + "  Z::" + zlabel);
                                                                var clickedLabels = {}
                                                                clickedLabels['x'] = xlabel;
                                                                clickedLabels['y'] = ylabel;
                                                                clickedLabels['z'] = zlabel;
                                                                var clickedLabelIds = {}
                                                                clickedLabelIds['x'] = xlabelId;
                                                                clickedLabelIds['y'] = ylabelId;
                                                                clickedLabelIds['z'] = zlabelId;

//                                                          multiSelectCubeLabels[xlabelId + ylabelId + zlabelId] = clickedLabelIds;
                                                                multiSelectCubeLabels[currentCubePos.x + "," + currentCubePos.y + "," + currentCubePos.z] = clickedLabelIds;
                                                                multiSelectCubeLabels['Dimension'] = '3D';

                                                                $("#" + chartId + "_x_" + clickedLabelIds['x']).addClass("highLightLabel");
                                                                $("#" + chartId + "_y_" + clickedLabelIds['y']).addClass("highLightLabel");
                                                                $("#" + chartId + "_z_" + clickedLabelIds['z']).addClass("highLightLabel");

                                                                material.opacity = 0.1;
                                                                globalMaterial[chartId] = material;

                                                                var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                                                                var Nmaterial = new THREE.MeshBasicMaterial({
                                                                    color: colorCode,
                                                                    vertexColors: true,
                                                                    opacity: 0.6,
                                                                    transparent: true
                                                                });

                                                                var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
                                                                ncube.position.set(currentCubePos.x - ((xlen / 2 * cubeSize) + cubeSize / 2), currentCubePos.y - ((ylen / 2 * cubeSize) + cubeSize / 2), currentCubePos.z + ((zlen / 2 * cubeSize) + cubeSize / 2));

                                                                scene.add(ncube);
                                                                selectedCubesArray.push(ncube);
                                                            }
                                                        }
                                                    }
//                                                }



                                                    multiSelectCubeLabelsObj[chartId] = multiSelectCubeLabels;

                                                    selectedCubesArrayObj[chartId] = selectedCubesArray;
                                                    stopLoader();
//return false;
//single dice code 
                                                }



                                            }
// ravi dice code start

// ravi dice code end
                                            else {
                                                resetCubes("", chartId);
                                                var multiSelectCubeLabels = multiSelectCubeLabelsObj[chartId];
                                                if (multiSelectCubeLabels == null || jQuery.isEmptyObject(multiSelectCubeLabels)) {
                                                    multiSelectCubeLabels = {};
                                                }

                                                var selectedCubesArray = selectedCubesArrayObj[chartId];
                                                if (selectedCubesArray == null || selectedCubesArray.length == 0) {
                                                    selectedCubesArray = [];
                                                }

                                                multiSelectCubeLabels = {};
                                                selectedCubesArray = [];
//                                            multiSelectCubeLabels[xlabelId + ylabelId + zlabelId] = clickedLabelIds;
                                                multiSelectCubeLabels[cubePosition.x + "," + cubePosition.y + "," + cubePosition.z] = clickedLabelIds;

                                                var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                                                var Nmaterial = new THREE.MeshBasicMaterial({
                                                    color: $("#BAR_CHART_CUBE_CLICK_AXIS_COLORS").val(),
                                                    vertexColors: true,
                                                    opacity: 0.6,
                                                    transparent: true
                                                });


                                                var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
                                                ncube.position.set(cubePosition.x - ((xlen / 2 * cubeSize) + cubeSize / 2), cubePosition.y - ((ylen / 2 * cubeSize) + cubeSize / 2), cubePosition.z + ((zlen / 2 * cubeSize) + cubeSize / 2));

                                                scene.add(ncube);
                                                multiSelectCubeLabelsObj[chartId] = multiSelectCubeLabels;

                                                selectedCubesArray.push(ncube);
                                                selectedCubesArrayObj[chartId] = selectedCubesArray;

                                            }


                                        } else {//cube mdrm
                                            resetCubes("", chartId);
                                            var multiSelectCubeLabels = multiSelectCubeLabelsObj[chartId];
                                            if (multiSelectCubeLabels == null || jQuery.isEmptyObject(multiSelectCubeLabels)) {
                                                multiSelectCubeLabels = {};
                                            }

                                            var selectedCubesArray = selectedCubesArrayObj[chartId];
                                            if (selectedCubesArray == null || selectedCubesArray.length == 0) {
                                                selectedCubesArray = [];
                                            }


                                            multiSelectCubeLabels = {};
//                                        multiSelectCubeLabels[xlabelId + ylabelId + zlabelId] = clickedLabelIds;
                                            multiSelectCubeLabels[cubePosition.x + "," + cubePosition.y + "," + cubePosition.z] = clickedLabelIds;

                                            var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                                            var Nmaterial = new THREE.MeshBasicMaterial({
                                                color: $("#BAR_CHART_CUBE_CLICK_AXIS_COLORS").val(),
                                                vertexColors: true,
                                                opacity: 0.6,
                                                transparent: true
                                            });


                                            var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
                                            ncube.position.set(cubePosition.x - ((xlen / 2 * cubeSize) + cubeSize / 2), cubePosition.y - ((ylen / 2 * cubeSize) + cubeSize / 2), cubePosition.z + ((zlen / 2 * cubeSize) + cubeSize / 2));

                                            scene.add(ncube);
                                            multiSelectCubeLabelsObj[chartId] = multiSelectCubeLabels;

                                            selectedCubesArray.push(ncube);
                                            selectedCubesArrayObj[chartId] = selectedCubesArray;
                                            if ((axisColumns['x']['column']).indexOf("Q_ID") > -1)
                                            {
                                                var clickedValue = clickedLabelIds['x'];
                                                $("#" + chartId + "_cubeQuarterClickValue").val(clickedValue);

                                            } else if ((axisColumns['y']['column']).indexOf("Q_ID") > -1)
                                            {
                                                var clickedValue = clickedLabelIds['y'];
                                                $("#" + chartId + "_cubeQuarterClickValue").val(clickedValue);
                                            } else if ((axisColumns['z']['column']).indexOf("Q_ID") > -1)
                                            {
                                                var clickedValue = clickedLabelIds['z'];
                                                $("#" + chartId + "_cubeQuarterClickValue").val(clickedValue);
                                            }
                                        }

// for slice or Dice

                                        var lineSlice = multiSelectCubeLabelsObj[chartId]['lineSlice'];


                                        var lineSliceWhrClause = "";
                                        var xWhereClause = "";
                                        var yWhereClause = "";
                                        var zWhereClause = "";

                                        $.each(multiSelectCubeLabelsObj[chartId], function (key, val) {
                                            if (key != "Dimension" && key != "lineSlice") {
                                                if (xWhereClause.indexOf("'" + this['x'] + "',") == -1) {
                                                    xWhereClause += "'" + this['x'] + "',";
                                                }
                                                if (yWhereClause.indexOf("'" + this['y'] + "',") == -1) {
                                                    yWhereClause += "'" + this['y'] + "',";
                                                }
                                                if (zWhereClause.indexOf("'" + this['z'] + "',") == -1) {
                                                    zWhereClause += "'" + this['z'] + "',";
                                                }
                                            }
                                        })

//cube changes
                                        lineSliceWhrClause = axisColumns['x']['column'] + " IN (" + xWhereClause.slice(0, -1) + ") AND " + axisColumns['y']['column'] + " IN (" + yWhereClause.slice(0, -1) + ") AND " + axisColumns['z']['column'] + " IN (" + zWhereClause.slice(0, -1) + ")";


                                        if (multiSelectCubeLabelsObj != null && ((Object.keys(multiSelectCubeLabelsObj[chartId]).length > 0 && multiSelectCubeLabelsObj[chartId]['Dimension'] != '3D' && multiSelectCubeLabelsObj[chartId]['Dimension'] != '2D') ||
                                                (lineSlice == "Y"))) {

                                            $.ajax({
                                                type: "post",
                                                traditional: true,
                                                dataType: 'JSON',
                                                cache: false,
                                                url: "getCubeValue",
                                                data: {
                                                    factTable: factTable,
                                                    cubeValueCol: cubeValueCol,
                                                    clickedLabels: JSON.stringify(clickedLabelIds),
                                                    axisColumns: JSON.stringify(axisColumns),
                                                    lineSlice: lineSlice,
                                                    lineSliceWhrClause: lineSliceWhrClause,
                                                    chartId: chartId,
                                                    factType: factType
                                                },
                                                success: function (response) {

                                                    var cubeValue = response['cubeValue']
                                                    var elem = document.createElement('div');
                                                    elem.className = 'cubeValueLabel';

                                                    var Label = new CSS2DObject(elem);


                                                    elem.innerHTML = cubeValue;
                                                    elem.id = chartId + "_CUBEVAL_" + cubeValue;
                                                    ncube.add(Label);



                                                    $("#" + chartId + "_x_" + clickedLabelIds['x']).addClass("highLightLabel");
                                                    $("#" + chartId + "_y_" + clickedLabelIds['y']).addClass("highLightLabel");
                                                    $("#" + chartId + "_z_" + clickedLabelIds['z']).addClass("highLightLabel");
                                                    stopLoader();
                                                },
                                                error: function (e) {
                                                    stopLoader();
                                                    sessionTimeout(e);
                                                }
                                            });

                                        }
                                        globalSceneObj[chartId] = scene;
                                    });


                                    domEvents.addEventListener(cube, 'dblclick', function (event) {
                                        console.log("dbl clicked");
                                        event.origDomEvent.preventDefault();
                                        var cubePosition = event.target.position;
                                        var xindex = Math.abs(cubePosition.x / cubeSize);
                                        var xlabel = labelsArray['x'][xindex - 1][0];
                                        var xlabelId = labelsArray['x'][xindex - 1][1];
                                        var yindex = Math.abs(cubePosition.y / cubeSize);
                                        var ylabel = labelsArray['y'][yindex - 1][0];
                                        var ylabelId = labelsArray['y'][yindex - 1][1];
                                        var zindex = Math.abs(cubePosition.z / cubeSize);
                                        var zlabel = labelsArray['z'][zindex - 1][0];
                                        var zlabelId = labelsArray['z'][zindex - 1][1];
                                        alert("X::" + xlabel + "  Y::" + ylabel + "  Z::" + zlabel);
                                        var clickedLabels = {}
                                        clickedLabels['x'] = xlabel;
                                        clickedLabels['y'] = ylabel;
                                        clickedLabels['z'] = zlabel;
                                        var clickedLabelIds = {}
                                        clickedLabelIds['x'] = xlabelId;
                                        clickedLabelIds['y'] = ylabelId;
                                        clickedLabelIds['z'] = zlabelId;
                                        var multiSelectCubeLabels = {};
                                        multiSelectCubeLabels[xlabelId + ylabelId + zlabelId] = clickedLabelIds;

                                        multiSelectCubeLabelsObj[chartId] = multiSelectCubeLabels;
                                        showReport('', chartId);


                                        globalSceneObj[chartId] = scene;
                                    });
                                }
                            }
                        }
                        $('#visionAnalyticCubeDropdownLabelx').on('select', function (event) {
                            if (event.args) {
                                var item = event.args.item;
                                if (item != null) {
                                    var value = item.value;
                                    var source = cubeCheckBoxObj[value];
                                    $("#visionAnalyticCubeCheckx").jqxListBox({
                                        filterable: true,
                                        checkboxes: true,
                                        source: source,
                                        theme: 'energyblue',
                                        displayMember: 'text',
                                        valueMember: 'value'
                                    });
                                    var ids = checkedIdsObj[value];
                                    for (var id = 0; id < ids.length; id++)
                                    {
                                        $("#visionAnalyticCubeCheckx").jqxListBox('checkItem', ids[id]);
                                    }
                                }
                            }
                        });
                        $('#visionAnalyticCubeDropdownLabely').on('select', function (event) {
                            if (event.args) {
                                var item = event.args.item;
                                if (item != null) {
                                    var value = item.value;
                                    var source = cubeCheckBoxObj[value];
                                    $("#visionAnalyticCubeChecky").jqxListBox({
                                        filterable: true,
                                        checkboxes: true,
                                        source: source,
                                        theme: 'energyblue',
                                        displayMember: 'text',
                                        valueMember: 'value'
                                    });
                                    var ids = checkedIdsObj[value];
                                    for (var id = 0; id < ids.length; id++)
                                    {
                                        $("#visionAnalyticCubeChecky").jqxListBox('checkItem', ids[id]);
                                    }
                                }
                            }
                        });
                        $('#visionAnalyticCubeDropdownLabelz').on('select', function (event) {
                            if (event.args) {
                                var item = event.args.item;
                                if (item != null) {
                                    var value = item.value;
                                    var source = cubeCheckBoxObj[value];
                                    $("#visionAnalyticCubeCheckz").jqxListBox({
                                        filterable: true,
                                        checkboxes: true,
                                        source: source,
                                        theme: 'energyblue',
                                        displayMember: 'text',
                                        valueMember: 'value'
                                    });
                                    var ids = checkedIdsObj[value];
                                    for (var id = 0; id < ids.length; id++)
                                    {
                                        $("#visionAnalyticCubeCheckz").jqxListBox('checkItem', ids[id]);
                                    }
                                }
                            }
                        });

                        controls.update();
                        controls.target.set(0, 0, 0);
                        function render() {

                            if (rotateFlagCW) {
                                $("#" + chartId + "_cubeContainer").find("div").css("display", "none");
//                            1800*5, 1000*5, 600*5)
                                camera.position.x -= 180 / 2;
                                camera.position.y -= 100 / 2;
                                camera.position.z -= 60 / 2;
                                scene.rotation.x += 0.1;
                                scene.rotation.y += 0.1;
                                scene.rotation.z += 0.1;
                            }

                            requestAnimationFrame(render);
                            renderer.render(scene, camera);
                            labelRenderer.render(scene, camera);

                        }
                        render();
                        rotateCube(scene, camera, chartId);

                        var qubeChartId = chartId + "_cubeContainer";
                        var pos = $("#" + qubeChartId).css({marginLeft: -664});

                        var dragControls = new THREE.DragControls(cubeObjects, camera, labelRenderer.domElement);
                        var tempCubes = [];
                        dragControls.addEventListener('dragstart', function (event) {

                            if (event.object.type == 'LineSegments') {
                                var cubePosition = event.object.position;
                                var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                                var Nmaterial = new THREE.MeshBasicMaterial({
                                    color: 0xffffff,
                                    vertexColors: true,
                                    opacity: 0.5,
                                    transparent: true
                                });
                                for (var i = 0; i < 3; i++) {
                                    Ngeometry.faces[4 * i].color = colors[i];
                                    Ngeometry.faces[4 * i + 1].color = colors[i];
                                    Ngeometry.faces[4 * i + 2].color = colors[i];
                                    Ngeometry.faces[4 * i + 3].color = colors[i];
                                }
                                var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
                                ncube.position.set(event.object.position.x, event.object.position.y, event.object.position.z);

                                tempCubes.push(ncube);
                                cubeObjects.push(ncube);
                                ncube.userData['relativePosition'] = cubePosition;
                            }
                            if (event.object.type == 'Mesh') {
                                controls.enabled = false;
                                var cubePosition = event.object.position;
                                var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                                var Nmaterial = new THREE.MeshBasicMaterial({
                                    color: 0xffffff,
                                    vertexColors: true,
                                    opacity: 0.5,
                                    transparent: true
                                });
                                for (var i = 0; i < 3; i++) {
                                    Ngeometry.faces[4 * i].color = colors[i];
                                    Ngeometry.faces[4 * i + 1].color = colors[i];
                                    Ngeometry.faces[4 * i + 2].color = colors[i];
                                    Ngeometry.faces[4 * i + 3].color = colors[i];
                                }
                                var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
                                ncube.position.set(event.object.position.x, event.object.position.y, event.object.position.z);
                                scene.add(ncube);
                                tempCubes = [];
                                tempCubes.push(ncube);
                                cubeObjects.push(ncube);
                                ncube.userData['relativePosition'] = cubePosition;

                            }


                        });
                        dragControls.addEventListener('drag', function (event) {

                            if (event.object.type == 'LineSegments') {

                            }
                            if (event.object.type == 'Mesh') {
                                controls.enabled = false;
                                $("#dropAreaId").empty();
                                $("#dropAreaId").remove();
                                var str = "<div id ='dropAreaId' class='cubecheckclass'>"
                                        + "<div id='spanDiv' style='text-align: center; margin: 0 auto;'><span>Drop here</span></div>"
                                        + "<div id='cubeDropId'></div></div>";
                                $("#" + chartId + "_Parent").append(str);
                                $("#dropAreaId").css('height', '350px');
                                $("#dropAreaId").css('width', '350px');
                                $("#dropAreaId").css('position', 'absolute');
                                $("#dropAreaId").css('z-index', '9999');
                                $('#dropAreaId').css('border', '1px solid black');
                                $('#dropAreaId').css({marginLeft: 450});
                                $('#dropAreaId').css({marginTop: 50});
                                $("#spanDiv").css('height', '30px', '!important');


                            }


                        });
                        var xlabelId = '';
                        var ylabelId = '';
                        var zlabelId = '';
                        var n = 0;
                        divArray = [];
                        dragControls.addEventListener('dragend', function (event) {
                            if (event.object.type == 'LineSegments') {
                                var tempCube = tempCubes[0];
                                event.object.position.set(tempCube.position.x, tempCube.position.y, tempCube.position.z);
                                tempCube.geometry.dispose();
                                tempCube.material.dispose();
                                scene.remove(tempCube);
                            }

                            if (event.object.type == 'Mesh') {
                                controls.enabled = true;
                                var cubePosition = event.object.userData['relativePosition'];
                                if (cubePosition != null) {
                                    var xindex = Math.abs((cubePosition.x) / cubeSize);
                                    var xlabel = labelsArray['x'][xindex - 1][0];
                                    xlabelId = labelsArray['x'][xindex - 1][1];
                                    var yindex = Math.abs((cubePosition.y) / cubeSize);
                                    var ylabel = labelsArray['y'][yindex - 1][0];
                                    ylabelId = labelsArray['y'][yindex - 1][1];
                                    var zindex = Math.abs((cubePosition.z) / cubeSize);
                                    var zlabel = labelsArray['z'][zindex - 1][0];
                                    zlabelId = labelsArray['z'][zindex - 1][1];
                                }
                                var tempCube = tempCubes[0];
                                event.object.position.set(tempCube.position.x, tempCube.position.y, tempCube.position.z);
                                tempCube.geometry.dispose();
                                tempCube.material.dispose();
                                scene.remove(tempCube);
                                if (divArray.length >= 2)
                                {
                                    removeFirstCube();
                                }
                                n++;
                                var clickedValuesObj = {};
                                clickedValuesObj['x'] = xlabelId;
                                clickedValuesObj['y'] = ylabelId;
                                clickedValuesObj['z'] = zlabelId;
                                setTimeout(function () {
                                    make_canvas(tabId, chartId, chartVal, clickedValuesObj, n);
                                }, 10);

                            }

                        });

//ravinder uncommented code for click purpose by jagadish //cube mdrm
//                        var pos = $("#" + qubeChartId).find("canvas").position();
//                        var pos1 = $("#" + qubeChartId).find("div:first").position();
//                        $("#" + qubeChartId).find("div:first").css({top: pos.top, left: pos.left});
//                        var pos2 = $("#" + qubeChartId).find("div:first").position();
//
//                        $("#" + tabId).scroll(function () {
//                            var pos = $("#" + qubeChartId).find("canvas").position();
//                            var pos1 = $("#" + qubeChartId).find("div:first").position();
//                            $("#" + qubeChartId).find("div:first").css({top: pos.top, left: pos.left});
//                            var pos2 = $("#" + qubeChartId).find("div:first").position();
//                        })
                    }
                    $("#wait").css("display", "none");
                }
            }

        },
        error: function (e) {
            sessionTimeout(e);
        }

    });
}



function showCubeCheckBox(cubeCheckBoxId, key, imgId) {
    if (key == 'x')
    {
        $("#visionAnalyticCubeChecky").hide();
        $("#visionAnalyticCubeCheckz").hide();
    } else if (key == 'y')
    {
        $("#visionAnalyticCubeCheckx").hide();
        $("#visionAnalyticCubeCheckz").hide();
    } else if (key == 'z')
    {
        $("#visionAnalyticCubeCheckx").hide();
        $("#visionAnalyticCubeChecky").hide();
    }

    $("#cubeCheckBoxImgx").attr("src", "images/rightArrow.svg"); //cube changes
    $("#cubeCheckBoxImgy").attr("src", "images/rightArrow.svg");
    $("#cubeCheckBoxImgz").attr("src", "images/rightArrow.svg");

    if ($("#" + cubeCheckBoxId).is(":visible")) {
        $("#" + imgId).attr("src", "images/rightArrow.svg");
    } else {
        $("#" + imgId).attr("src", "images/downArrow.svg");
    }
    $("#" + cubeCheckBoxId).toggle();
}

function showCheckBoxCubeData(tabId, chartId, chartVal)
{
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        cache: false,
        url: "analyticDropdown",
        data: {
            'tabComponentId': chartId,
            'tabId': tabId,
            'chartTypeId': chartVal
        },
        success: function (response) {
            if (response != null)
            {
                var result = JSON.parse(response);
                var dataObj = result['chartList'];
                for (var i = 0; i < dataObj.length; i++)
                {
                    $("#wait").css("display", "block");
                    var chartData = dataObj[i];
                    getshowCheckBoxCubeData(chartData, tabId, chartId);
                }
            }
        },
        error: function (e) {
            sessionTimeout(e);
        }
    });
}

function getshowCheckBoxCubeData(chartData, tabId, chartId) {
    var checkBoxObjCheckedVal = {};
    alert("checked");
    var axisColumns = $("#" + chartId + "_axisColumns").val();
    var axisLabels = $("#" + chartId + "_axisLabels").val();
    var checkedXvalues = $("#visionAnalyticCubeCheckx").jqxListBox('getCheckedItems');
    var checkedYvalues = $("#visionAnalyticCubeChecky").jqxListBox('getCheckedItems');
    var checkedZvalues = $("#visionAnalyticCubeCheckz").jqxListBox('getCheckedItems');
    var xValues = [];
    var yValues = [];
    var zValues = [];
    $.each(checkedXvalues, function (index) {
        xValues.push(this.value);
    });
    $.each(checkedYvalues, function (index) {
        yValues.push(this.value);
    });
    $.each(checkedZvalues, function (index) {
        zValues.push(this.value);
    });
    checkBoxObjCheckedVal['x'] = xValues;
    checkBoxObjCheckedVal['y'] = yValues;
    checkBoxObjCheckedVal['z'] = zValues;
    $("#" + chartId + "_checkBoxChecked").remove();
    $("#" + chartId + "_colorObj").remove();
    $("#" + chartId + "_clickedcolorObj").remove();
    $("#cubeCheckBoxCheckedValues").append("<input type='hidden' id='" + chartId + "_checkBoxChecked' value=''/>");
    $("#cubeCheckBoxCheckedValues").append("<input type='hidden' id='" + chartId + "_colorObj' value=''/>"); //cube changes
    $("#cubeCheckBoxCheckedValues").append("<input type='hidden' id='" + chartId + "_clickedcolorObj' value=''/>"); //cube changes
    $("#" + chartId + "_checkBoxChecked").val(JSON.stringify(checkBoxObjCheckedVal));
    var chartOptAllObj = {}; //cube personalize
    var colorX = $("#BAR_CHART_CUBE_X_AXIS_COLORS").val();
    var colorY = $("#BAR_CHART_CUBE_Y_AXIS_COLORS").val();
    var colorZ = $("#BAR_CHART_CUBE_Z_AXIS_COLORS").val();
    chartOptAllObj['x'] = colorX;
    chartOptAllObj['y'] = colorY;
    chartOptAllObj['z'] = colorZ;
    $("#" + chartId + "_colorObj").val(JSON.stringify(chartOptAllObj));//cube changes
    var clickedcolorsObj = {}; //cube personalize
    var clickColor = $("#BAR_CHART_CUBE_CLICK_AXIS_COLORS").val();
    var sliceColor = $("#BAR_CHART_CUBE_SLICE_AXIS_COLORS").val();
    var diceColor = $("#BAR_CHART_CUBE_DICE_AXIS_COLORS").val();
    clickedcolorsObj['click'] = clickColor;
    clickedcolorsObj['slice'] = sliceColor;
    clickedcolorsObj['dice'] = diceColor;
    $("#" + chartId + "_clickedcolorObj").val(JSON.stringify(clickedcolorsObj));//cube changes
    var data = {
        'chartData': chartData,
        'ddwFlag': 'Y',
        checkBoxObjCheckedVal: JSON.stringify(checkBoxObjCheckedVal),
        cubeColors: JSON.stringify(chartOptAllObj),
        cubeClickedColors: JSON.stringify(clickedcolorsObj),
        axisColumns: axisColumns,
        axisLabels: axisLabels
    };
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        url: "getQubeAnalytics",
        async: true,
        cache: false,
        data: data,
        success: function (response) {
            $("#wait").css("display", "none");
            if (response != null && response != '' && response != undefined) {
                var resultData = JSON.parse(response);
                if (resultData != null)
                {
                    var result = resultData;
                    if (result != null) {
                        $("#wait").css("display", "none");
                        var chartsObj = result['chartsObj'];
                        var divid = chartsObj['divId'];
                        var seqNo = chartsObj['seqNo'];
                        var chartId = chartsObj['chartId'];
                        var sizeOfChart = chartsObj['sizeOfChart'];
                        var height = chartsObj['height'];
                        var width = chartsObj['width'];
                        var factTable = chartsObj['factTable'];
                        var cubeValueCol = chartsObj['cubeValueCol'];
                        var axisColumns = chartsObj['axisColumns'];
                        var axisLabels = chartsObj['axisLabels'];
                        var labelsArray = chartsObj['labelsObject'];
                        var title = chartsObj['title'];
                        var factType = chartsObj['factType']; //cube mdrm 
                        var cubeChartId = $("#cubeChartId").val();
                        $("#" + cubeChartId).empty();
                        $('#' + cubeChartId).attr('id', chartId + '_Chart');
                        $("#cubeChartId").val(chartId + "_Chart");
                        var cubeCheckBoxDivId = chartsObj['cubeCheckBoxDivId'];
                        $("#" + chartId + "_Chart").html(divid);
                        $("#" + chartId + "_factTable").val(factTable);
                        $("#" + chartId + "_factType").val(factType);
                        $("#" + chartId + "_cubeValueCol").val(cubeValueCol);
                        $("#" + chartId + "_axisColumns").val(JSON.stringify(axisColumns));
                        $("#" + chartId + "_axisLabels").val(JSON.stringify(axisLabels));
                        var checkedIdsObj = chartsObj['checkedIdsObj']; //multi cube 
                        var cubeCheckBoxObj = chartsObj['cubeCheckBoxObj']; //multi cube 
                        var dropdownCubeObj = chartsObj['dropdownCubeObj']; //multi cube 
                        var colorsObj = chartsObj['colorsObj'];
                        var cubeCheckBoxLabelObj = chartsObj['cubeCheckBoxLabelObj'];
                        var chartVal = chartsObj['chartVal']; //cube mdrm 
                        $("#" + chartId + "_cubeCheckBoxObj").val(JSON.stringify(cubeCheckBoxObj));
                        $("#" + chartId + "_cubeListBoxLabelsObj").val(JSON.stringify(cubeCheckBoxLabelObj));
                        $("#" + chartId + "_checkedIdsObj").val(JSON.stringify(checkedIdsObj));
                        $("#visionAnalyticCubeChartCheckBox").html(cubeCheckBoxDivId); //cube personalize above row remove
                        for (var key in dropdownCubeObj)
                        {
                            var source = dropdownCubeObj[key];
                            if ($('#visionAnalyticCubeDropdownLabel' + key).length != 0) {
                                $('#visionAnalyticCubeDropdownLabel' + key).jqxDropDownList("destroy");
                                $('#visionAnalyticCubeDropdownLabel' + key).jqxDropDownList({
                                    source: source,
                                    theme: 'energyblue',
                                    displayMember: 'text',
                                    valueMember: 'value',
                                    dropDownHeight: 105,
                                    dropDownWidth: 107
                                });
                                $("#visionAnalyticCubeDropdownLabel" + key).jqxDropDownList('selectItem', key);
                            }
                        }
                        for (var key in cubeCheckBoxObj)
                        {
                            var source = cubeCheckBoxObj[key];
                            if ($("#visionAnalyticCubeCheck" + key).length != 0) {
                                $("#visionAnalyticCubeCheck" + key).jqxListBox({
                                    filterable: true,
                                    checkboxes: true,
                                    source: source,
                                    theme: 'energyblue',
                                    displayMember: 'text',
                                    valueMember: 'value'
                                });
                                var ids = checkedIdsObj[key];
                                if (ids != null && !jQuery.isEmptyObject(ids)) {
                                    for (var id = 0; id < ids.length; id++)
                                    {
                                        $("#visionAnalyticCubeCheck" + key).jqxListBox('checkItem', ids[id]);
                                    }
                                }
                            }
                        }
                        if (sizeOfChart != null && sizeOfChart != '' && sizeOfChart != undefined) {
                            $("#" + chartId + "_Chart").css("width", "" + sizeOfChart + "%", "important");
                        } else if (windowWidth > 1024)
                        {
                            $("#" + chartId + "_Chart").css('width', '99%', 'important');
                        } else if (windowWidth < 1024 && windowWidth > 700)
                        {
                            $("#" + chartId + "_Chart").css('width', '49%');
                        } else if (windowWidth < 699 && windowWidth > 320)
                        {
                            $("#" + chartId + "_Chart").css('width', '98%');
                        }

                        $("#" + chartId).html("<div id ='" + chartId + "_cubeContainer' class='VisionShowCubeChart'></div>");
                        $("#" + chartId + "_cube_types").val(title);

                        var noOfCubes = 3;
                        var cubeSize = 100;
                        multiSelectCubeLabelsObj = {};
                        selectedCubesArrayObj = {};
                        var scene = new THREE.Scene();
                        var camera = new THREE.PerspectiveCamera(45, 2.5, 1, 10000);
                        var renderer = new THREE.WebGLRenderer({antialias: true});
                        renderer.setSize(parseInt(width) + 600, height);
                        scene.background = new THREE.Color(0xFFFFFF);

                        document.querySelector("#" + chartId + "_cubeContainer").appendChild(renderer.domElement);
                        camera.position.set(1800 * 5, 1000 * 5, 600 * 5);


                        var raycaster = new THREE.Raycaster();
                        var mouse = new THREE.Vector2();
                        const axesHelper = new THREE.AxesHelper((noOfCubes + 5) * 100);
                        renderer.setPixelRatio(window.devicePixelRatio);
                        renderer.setSize(parseInt(width) + 600, height);
                        var labelRenderer = new CSS2DRenderer();
                        labelRenderer.setSize(parseInt(width) + 600, height);
                        labelRenderer.domElement.style.position = 'absolute';
                        labelRenderer.domElement.style.top = '0px';
                        document.querySelector('#' + chartId + "_cubeContainer").appendChild(labelRenderer.domElement);
                        var controls = new THREE.OrbitControls(camera, labelRenderer.domElement);
                        controls.update();
                        controls.target.set(0, 0, 0);

                        var domEvents = new THREEx.DomEvents(camera, labelRenderer.domElement);
                        var axisArray = [];
                        var xlen = labelsArray['x'].length;
                        var ylen = labelsArray['y'].length;
                        var zlen = labelsArray['z'].length;

                        var xIndex = 1;
                        for (var j = -xlen / 2; j < xlen / 2; j++) {
                            var sign = j / Math.abs(j);
                            var elem = document.createElement('div');
                            elem.className = 'label';
                            elem.style.marginTop = '-1em';
                            var Label = new CSS2DObject(elem);
                            var xpoints = [];
                            if (j != (xlen / 2 - 1)) {
                                xpoints.push(new THREE.Vector3((j - 0.5) * cubeSize, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
                                xpoints.push(new THREE.Vector3((j + 0.5) * cubeSize, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
                            } else {

                                xpoints.push(new THREE.Vector3((j - 0.5) * cubeSize, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
                                xpoints.push(new THREE.Vector3((j + 0.5) * cubeSize * 2, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
//                            xpoints.push(new THREE.Vector3((j) * cubeSize, (j/Math.abs(j))* (ylen/2)* cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
//                            xpoints.push(new THREE.Vector3((j * cubeSize) + 1.5 * cubeSize, (j/Math.abs(j))* (ylen/2)* cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
                            }
                            var xmaterial = new THREE.LineBasicMaterial({color: 0x0071C5});
                            var xgeometry = new THREE.BufferGeometry().setFromPoints(xpoints);
                            var xaxis = new THREE.Line(xgeometry, xmaterial);
                            scene.add(xaxis);
                            Label.position.set((j + 0.5) * cubeSize, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize);
                            elem.textContent = labelsArray['x'][xIndex - 1][0];
                            elem.id = chartId + "_x_" + labelsArray['x'][xIndex - 1][1];
                            xaxis.add(Label);
                            axisArray.push(xaxis)
                            xIndex++;
                        }


                        var yIndex = 1;
                        for (var j = -(ylen / 2); j < ylen / 2; j++) {

                            var elem = document.createElement('div');
                            elem.className = 'label';
                            elem.style.marginTop = '-1em';
                            var Label = new CSS2DObject(elem);
                            var ypoints = [];
                            if (j != (ylen / 2 - 1)) {

                                ypoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize, (j - 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
                                ypoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize, (j + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
//                            ypoints.push(new THREE.Vector3((j/Math.abs(j))* (xlen/2)* cubeSize, (j - 1) * cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
//                            ypoints.push(new THREE.Vector3((j/Math.abs(j))* (xlen/2)* cubeSize, (j * cubeSize), (j/Math.abs(j))* (zlen/2)* cubeSize));
                            } else {

                                ypoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize, (j - 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
                                ypoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize, (j + 0.5) * cubeSize * 1.5, (zlen / 2 + 0.5) * cubeSize));
//                            ypoints.push(new THREE.Vector3((j/Math.abs(j))* (xlen/2)* cubeSize, (j - 1) * cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
//                            ypoints.push(new THREE.Vector3((j/Math.abs(j))* (xlen/2)* cubeSize, (j * cubeSize) + 1 * cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
                            }

                            var ymaterial = new THREE.LineBasicMaterial({color: 0x0071C5});
                            var ygeometry = new THREE.BufferGeometry().setFromPoints(ypoints);
                            var yaxis = new THREE.Line(ygeometry, ymaterial);
                            scene.add(yaxis);
                            Label.position.set(-(xlen / 2 + 0.5) * cubeSize, (j + 0.5) * cubeSize - 20, (zlen / 2 + 0.5) * cubeSize);
                            elem.textContent = labelsArray['y'][yIndex - 1][0];
                            elem.id = chartId + "_y_" + labelsArray['y'][yIndex - 1][1];
                            yaxis.add(Label);
                            axisArray.push(yaxis)
                            yIndex++;

                        }


                        var zIndex = 1;
                        for (var j = -(zlen / 2); j < zlen / 2; j++) {
                            var noOfCubes = labelsArray['x'].length;
                            var elem = document.createElement('div');
                            elem.className = 'label';
                            elem.style.marginTop = '-1em';
                            var Label = new CSS2DObject(elem);
                            var zpoints = [];
                            if (j != (zlen / 2 - 1)) {

                                zpoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j - 0.5) * cubeSize));
                                zpoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j + 0.5) * cubeSize));
//                            zpoints.push(new THREE.Vector3((noOfCubes * cubeSize + cubeSize), (j/Math.abs(j))* (ylen/2)* cubeSize, -(j - 1) * cubeSize));
//                            zpoints.push(new THREE.Vector3((noOfCubes * cubeSize + cubeSize), (j/Math.abs(j))* (ylen/2)* cubeSize, -(j * cubeSize)));
                            } else {

                                zpoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j - 0.5) * cubeSize));
                                zpoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j + 0.5) * cubeSize * 1.5));
//                            zpoints.push(new THREE.Vector3((noOfCubes * cubeSize + cubeSize), (j/Math.abs(j))* (ylen/2)* cubeSize, -(j - 1) * cubeSize));
//                            zpoints.push(new THREE.Vector3((noOfCubes * cubeSize + cubeSize), (j/Math.abs(j))* (ylen/2)* cubeSize, -((j * cubeSize) + 1 * cubeSize)));
                            }

                            var zmaterial = new THREE.LineBasicMaterial({color: 0x0071C5});
                            var zgeometry = new THREE.BufferGeometry().setFromPoints(zpoints);
                            var zaxis = new THREE.Line(zgeometry, zmaterial);
                            scene.add(zaxis);
                            Label.position.set(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j + 0.5) * cubeSize);
//                        Label.position.set(noOfCubes * cubeSize + cubeSize, (j/Math.abs(j))* (ylen/2)* cubeSize, -j * cubeSize);
                            elem.textContent = labelsArray['z'][zIndex - 1][0];
                            elem.id = chartId + "_z_" + labelsArray['z'][zIndex - 1][1];
                            zaxis.add(Label);
                            axisArray.push(zaxis)
                            zIndex++;

                        }


                        var geometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
// material
                        var material = new THREE.MeshBasicMaterial({
                            color: 0xffffff,
                            vertexColors: true,
                            opacity: 0.5,
                            transparent: true
                        });
                        if (colorsObj != null && !jQuery.isEmptyObject(colorsObj)) //cube mdrm
                        {
                            red = new THREE.Color(colorsObj['x']);
                            green = new THREE.Color(colorsObj['y']);
                            blue = new THREE.Color(colorsObj['z']);
                        }
                        var colors = [red, green, blue];
                        for (var i = 0; i < 3; i++) {
                            geometry.faces[4 * i].color = colors[i];
                            geometry.faces[4 * i + 1].color = colors[i];
                            geometry.faces[4 * i + 2].color = colors[i];
                            geometry.faces[4 * i + 3].color = colors[i];
                        }
                        var cubeObjects = [];
                        var relativeX = 0;
                        var relativeY = 0;
                        var relativeZ = 0;
                        for (var i = -(xlen / 2); i < xlen / 2; i++) {

                            relativeX = relativeX + cubeSize;
                            relativeY = 0;
                            relativeZ = 0;
                            for (var j = -(ylen / 2); j < ylen / 2; j++) {
                                relativeY = relativeY + cubeSize;
                                relativeZ = 0;
                                for (var k = -(zlen / 2); k < zlen / 2; k++) {
                                    relativeZ = relativeZ - cubeSize;
                                    var cube = new THREE.Mesh(geometry, material);
                                    scene.add(cube);
                                    var edges = new THREE.EdgesGeometry(geometry);
                                    line = new THREE.LineSegments(edges, new THREE.LineBasicMaterial({color: 0x000000, opacity: 0.5, transparent: true}));
                                    cube.add(line);
                                    cube.position.set((i + 0.5) * cubeSize, (j + 0.5) * cubeSize, -(k + 0.5) * cubeSize);
                                    var relativePosition = {};
                                    relativePosition.x = relativeX;
                                    relativePosition.y = relativeY;
                                    relativePosition.z = relativeZ;

                                    cube.userData['relativePosition'] = relativePosition;
                                    cubeObjects.push(cube);

// CLICK EVENT----->>>>>

                                    domEvents.addEventListener(cube, 'click', function (event) {
                                        console.log("clicked");
                                        event.origDomEvent.preventDefault();
                                        cubeClickFlag = true;
                                        material.opacity = 0.5;
                                        globalMaterial[chartId] = material;
                                        var originalcubePosition = event.target.position;
                                        var cubePosition = event.target.userData['relativePosition'];
//                                    cubePosition.x = cubePosition.x+300;
//                                    cubePosition.y = cubePosition.y+300;
//                                    cubePosition.z = cubePosition.z-300;

                                        var xindex = Math.abs((cubePosition.x) / cubeSize);
                                        var xlabel = labelsArray['x'][xindex - 1][0];
                                        var xlabelId = labelsArray['x'][xindex - 1][1];
                                        var yindex = Math.abs((cubePosition.y) / cubeSize);
                                        var ylabel = labelsArray['y'][yindex - 1][0];
                                        var ylabelId = labelsArray['y'][yindex - 1][1];
                                        var zindex = Math.abs((cubePosition.z) / cubeSize);
                                        var zlabel = labelsArray['z'][zindex - 1][0];
                                        var zlabelId = labelsArray['z'][zindex - 1][1];
                                        alert("X::" + xlabel + "  Y::" + ylabel + "  Z::" + zlabel);
                                        var clickedLabels = {}
                                        clickedLabels['x'] = xlabel;
                                        clickedLabels['y'] = ylabel;
                                        clickedLabels['z'] = zlabel;

                                        var clickedLabelIds = {}
                                        clickedLabelIds['x'] = xlabelId;
                                        clickedLabelIds['y'] = ylabelId;
                                        clickedLabelIds['z'] = zlabelId;

                                        var firstSelectedCubeLebel = {};

                                        if (event.origDomEvent.shiftKey) {

                                            var multiSelectCubeLabels = multiSelectCubeLabelsObj[chartId];
                                            if (multiSelectCubeLabels == null || jQuery.isEmptyObject(multiSelectCubeLabels)) {
                                                multiSelectCubeLabels = {};
                                            }

                                            var selectedCubesArray = selectedCubesArrayObj[chartId];
                                            if (selectedCubesArray == null || selectedCubesArray.length == 0) {
                                                selectedCubesArray = [];
                                            }

                                            if (multiSelectCubeLabels != null && Object.keys(multiSelectCubeLabels).length == 1) {
                                                firstSelectedCubeLebel = multiSelectCubeLabels
                                                var matchCount = 0;
                                                var values = Object.values(multiSelectCubeLabels)[0];
                                                var staticAxi = [];
                                                var sliceAxis;
                                                if (values['x'] == clickedLabelIds['x']) {
                                                    matchCount++;
                                                    staticAxi.push('x');
                                                } else {
                                                    sliceAxis = 'x';
                                                }
                                                if (values['y'] == clickedLabelIds['y']) {
                                                    matchCount++;
                                                    staticAxi.push('y');
                                                } else {
                                                    sliceAxis = 'y';
                                                }
                                                if (values['z'] == clickedLabelIds['z']) {
                                                    matchCount++;
                                                    staticAxi.push('z');
                                                } else {
                                                    sliceAxis = 'z';
                                                }
                                                if (matchCount == 5) {

                                                    $(".label").removeClass("highLightLabel");
                                                    $.each(selectedCubesArray, function (index) {

                                                        scene.remove(selectedCubesArray[index]);
                                                        $(".cubeLabel").remove();
                                                        $(".cubeValueLabel").remove();
                                                    })


                                                    labelsArray[sliceAxis];
//                                                var selectedCubesArray = selectedCubesArrayObj[chartId];
//                                                 if (selectedCubesArray==null || selectedCubesArray.length==0){
//                                                     selectedCubesArray = [];
//                                                 }
                                                    var selectCubeLabels = {};
                                                    var addOrder;
                                                    var sliceAxisPos;
                                                    var newCubePosition = {};
                                                    var cubeReached = false;
                                                    for (var i = 0; i < labelsArray[sliceAxis].length; i++) {

                                                        if (!cubeReached) {
                                                            if (i == 0) {
                                                                var yellowCube = selectedCubesArray[0];
                                                                newCubePosition = yellowCube.position;
                                                                sliceAxisPos = newCubePosition[sliceAxis];
                                                                if (Math.abs(cubePosition[sliceAxis]) - Math.abs(newCubePosition[sliceAxis]) >= 0) {
                                                                    addOrder = "plus";
                                                                } else {
                                                                    addOrder = "minus";
                                                                }

                                                            } else {
                                                                if (addOrder == "plus") {
                                                                    newCubePosition[sliceAxis] = sliceAxisPos + (sliceAxisPos / Math.abs(sliceAxisPos) * i * cubeSize);
                                                                    if (newCubePosition[sliceAxis] == event.target.position[sliceAxis]) {
                                                                        cubeReached = true;
                                                                    }
                                                                } else if (addOrder == "minus") {

                                                                    newCubePosition[sliceAxis] = sliceAxisPos - (sliceAxisPos / Math.abs(sliceAxisPos) * i * cubeSize);
                                                                    if (newCubePosition[sliceAxis] == event.target.position[sliceAxis]) {
                                                                        cubeReached = true;
                                                                    }
                                                                }

                                                            }
                                                            if (sliceAxis == 'z') {
                                                                var sliceAxisindex = Math.abs((newCubePosition[sliceAxis]) / cubeSize);
                                                            } else {
                                                                var sliceAxisindex = Math.abs((newCubePosition[sliceAxis]) / cubeSize);
                                                            }

                                                            var sliceAxislabel = labelsArray[sliceAxis][sliceAxisindex - 1][0];
                                                            var sliceAxislabelId = labelsArray[sliceAxis][sliceAxisindex - 1][1];

                                                            var staticAxis1 = staticAxi[0];
                                                            if (staticAxis1 == 'z') {
                                                                var staticAxis1index = Math.abs((newCubePosition[staticAxis1]) / cubeSize);
                                                            } else {
                                                                var staticAxis1index = Math.abs((newCubePosition[staticAxis1]) / cubeSize);
                                                            }

                                                            var staticAxis1label = labelsArray[staticAxis1][staticAxis1index - 1][0];
                                                            var staticAxis1labelId = labelsArray[staticAxis1][staticAxis1index - 1][1];

                                                            var staticAxis2 = staticAxi[1];
                                                            if (staticAxis2 == 'z') {
                                                                var staticAxis2index = Math.abs((newCubePosition[staticAxis2]) / cubeSize);
                                                            } else {
                                                                var staticAxis2index = Math.abs((newCubePosition[staticAxis2]) / cubeSize);
                                                            }

                                                            var staticAxis2label = labelsArray[staticAxis2][staticAxis2index - 1][0];
                                                            var staticAxis2labelId = labelsArray[staticAxis2][staticAxis2index - 1][1];

                                                            var labels = {}
                                                            labels[sliceAxis] = sliceAxislabel;
                                                            labels[staticAxis1] = staticAxis1label;
                                                            labels[staticAxis2] = staticAxis2label;

                                                            var labelIds = {}
                                                            labelIds[sliceAxis] = sliceAxislabelId;
                                                            labelIds[staticAxis1] = staticAxis1labelId;
                                                            labelIds[staticAxis2] = staticAxis2labelId;

//                                                        selectCubeLabels[sliceAxislabelId + staticAxis1labelId + staticAxis2labelId] = labelIds;
                                                            selectCubeLabels[newCubePosition.x + "," + newCubePosition.y + "," + newCubePosition.z] = labelIds;
//                                                       selectCubeLabels[newCubePosition[sliceAxis] +","+ newCubePosition[staticAxis1] +","+ newCubePosition[staticAxis2]] = labelIds;

                                                            var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                                                            var Nmaterial = new THREE.MeshBasicMaterial({
                                                                color: "#FFFF00",
                                                                vertexColors: true,
                                                                opacity: 0.6,
                                                                transparent: true
                                                            });

                                                            var ncube = new THREE.Mesh(Ngeometry, Nmaterial);

                                                            ncube.position.set(newCubePosition.x - ((xlen / 2 * cubeSize) + cubeSize / 2), newCubePosition.y - ((ylen / 2 * cubeSize) + cubeSize / 2), newCubePosition.z + ((zlen / 2 * cubeSize) + cubeSize / 2));

                                                            scene.add(ncube);
                                                            selectedCubesArray.push(ncube);

                                                        }
                                                    }

                                                    $.each(selectCubeLabels, function (indx) {
                                                        var selectedLableIds = this;
                                                        $("#" + chartId + "_" + sliceAxis + "_" + selectedLableIds[sliceAxis]).addClass("highLightLabel");
                                                    })

                                                    selectCubeLabels['lineSlice'] = 'Y';
                                                    multiSelectCubeLabelsObj[chartId] = selectCubeLabels;
                                                    selectedCubesArrayObj[chartId] = selectedCubesArray;



                                                } else {
                                                    //single dice code 
                                                    //showDialog("Incorrect Selection");


                                                    var selectedCubes = multiSelectCubeLabels;
                                                    var pos = cubePosition;
                                                    var prevCubePosArr = Object.keys(multiSelectCubeLabels)[0].split(",");
                                                    var prevCubePos = {};
                                                    prevCubePos['x'] = parseInt(prevCubePosArr[0]);
                                                    prevCubePos['y'] = parseInt(prevCubePosArr[1]);
                                                    prevCubePos['z'] = parseInt(prevCubePosArr[2]);

                                                    resetCubes("", chartId);


                                                    var zDiff = cubePosition.z / 100 - prevCubePos['z'] / 100;
                                                    var yDiff = cubePosition.y / 100 - prevCubePos['y'] / 100;
                                                    var xDiff = cubePosition.x / 100 - prevCubePos['x'] / 100;
                                                    var zeroDiffCount = 0;
                                                    if (zDiff == 0) {
                                                        zeroDiffCount++;
                                                    }
                                                    if (yDiff == 0) {
                                                        zeroDiffCount++;
                                                    }
                                                    if (xDiff == 0) {
                                                        zeroDiffCount++;
                                                    }
                                                    if (zeroDiffCount == 2) {
                                                        multiSelectCubeLabels['lineSlice'] = 'Y';
                                                    }
                                                    var colorCode = "";
                                                    if (zeroDiffCount == 0)
                                                    {
                                                        colorCode = $("#BAR_CHART_CUBE_DICE_AXIS_COLORS").val();
                                                    } else if (zeroDiffCount == 1)
                                                    {
                                                        colorCode = $("#BAR_CHART_CUBE_SLICE_AXIS_COLORS").val();
                                                    } else if (zeroDiffCount == 2)
                                                    {
                                                        colorCode = $("#BAR_CHART_CUBE_SLICE_AXIS_COLORS").val();
                                                    }
                                                    for (var k = 0; k <= Math.abs(zDiff); k++) {
                                                        for (var j = 0; j <= Math.abs(yDiff); j++) {

                                                            for (var i = 0; i <= Math.abs(xDiff); i++) {


                                                                var currentCubePos = {};

                                                                currentCubePos['x'] = prevCubePos.x + ((xDiff != 0) ? (xDiff / Math.abs(xDiff) * i * 100) : 0);
                                                                currentCubePos['y'] = prevCubePos.y + ((yDiff != 0) ? (yDiff / Math.abs(yDiff) * j * 100) : 0);
                                                                currentCubePos['z'] = prevCubePos.z + ((zDiff != 0) ? (zDiff / Math.abs(zDiff) * k * 100) : 0);

                                                                var xindex = Math.abs(currentCubePos.x / cubeSize);
                                                                var xlabel = labelsArray['x'][xindex - 1][0];
                                                                var xlabelId = labelsArray['x'][xindex - 1][1];

                                                                var yindex = Math.abs(currentCubePos.y / cubeSize);
                                                                var ylabel = labelsArray['y'][yindex - 1][0];
                                                                var ylabelId = labelsArray['y'][yindex - 1][1];

                                                                var zindex = Math.abs(currentCubePos.z / cubeSize);
                                                                var zlabel = labelsArray['z'][zindex - 1][0];
                                                                var zlabelId = labelsArray['z'][zindex - 1][1];

                                                                alert("X::" + xlabel + "  Y::" + ylabel + "  Z::" + zlabel);
                                                                var clickedLabels = {}
                                                                clickedLabels['x'] = xlabel;
                                                                clickedLabels['y'] = ylabel;
                                                                clickedLabels['z'] = zlabel;
                                                                var clickedLabelIds = {}
                                                                clickedLabelIds['x'] = xlabelId;
                                                                clickedLabelIds['y'] = ylabelId;
                                                                clickedLabelIds['z'] = zlabelId;

//                                                          multiSelectCubeLabels[xlabelId + ylabelId + zlabelId] = clickedLabelIds;
                                                                multiSelectCubeLabels[currentCubePos.x + "," + currentCubePos.y + "," + currentCubePos.z] = clickedLabelIds;
                                                                multiSelectCubeLabels['Dimension'] = '3D';

                                                                $("#" + chartId + "_x_" + clickedLabelIds['x']).addClass("highLightLabel");
                                                                $("#" + chartId + "_y_" + clickedLabelIds['y']).addClass("highLightLabel");
                                                                $("#" + chartId + "_z_" + clickedLabelIds['z']).addClass("highLightLabel");

                                                                material.opacity = 0.1;
                                                                globalMaterial[chartId] = material;
                                                                var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                                                                var Nmaterial = new THREE.MeshBasicMaterial({
                                                                    color: colorCode,
                                                                    vertexColors: true,
                                                                    opacity: 0.6,
                                                                    transparent: true
                                                                });

                                                                var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
                                                                ncube.position.set(currentCubePos.x - ((xlen / 2 * cubeSize) + cubeSize / 2), currentCubePos.y - ((ylen / 2 * cubeSize) + cubeSize / 2), currentCubePos.z + ((zlen / 2 * cubeSize) + cubeSize / 2));

                                                                scene.add(ncube);
                                                                selectedCubesArray.push(ncube);
                                                            }
                                                        }
                                                    }
//                                                }



                                                    multiSelectCubeLabelsObj[chartId] = multiSelectCubeLabels;

                                                    selectedCubesArrayObj[chartId] = selectedCubesArray;
                                                    stopLoader();
//return false;
//single dice code 
                                                }



                                            }
// ravi dice code start

// ravi dice code end
                                            else {
                                                resetCubes("", chartId);
                                                var multiSelectCubeLabels = multiSelectCubeLabelsObj[chartId];
                                                if (multiSelectCubeLabels == null || jQuery.isEmptyObject(multiSelectCubeLabels)) {
                                                    multiSelectCubeLabels = {};
                                                }

                                                var selectedCubesArray = selectedCubesArrayObj[chartId];
                                                if (selectedCubesArray == null || selectedCubesArray.length == 0) {
                                                    selectedCubesArray = [];
                                                }

                                                multiSelectCubeLabels = {};
                                                selectedCubesArray = [];
//                                            multiSelectCubeLabels[xlabelId + ylabelId + zlabelId] = clickedLabelIds;
                                                multiSelectCubeLabels[cubePosition.x + "," + cubePosition.y + "," + cubePosition.z] = clickedLabelIds;

                                                var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                                                var Nmaterial = new THREE.MeshBasicMaterial({
                                                    color: $("#BAR_CHART_CUBE_CLICK_AXIS_COLORS").val(),
                                                    vertexColors: true,
                                                    opacity: 0.6,
                                                    transparent: true
                                                });


                                                var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
                                                ncube.position.set(cubePosition.x - ((xlen / 2 * cubeSize) + cubeSize / 2), cubePosition.y - ((ylen / 2 * cubeSize) + cubeSize / 2), cubePosition.z + ((zlen / 2 * cubeSize) + cubeSize / 2));

                                                scene.add(ncube);
                                                multiSelectCubeLabelsObj[chartId] = multiSelectCubeLabels;

                                                selectedCubesArray.push(ncube);
                                                selectedCubesArrayObj[chartId] = selectedCubesArray;

                                            }
                                        } else {
                                            resetCubes("", chartId);
                                            var multiSelectCubeLabels = multiSelectCubeLabelsObj[chartId];
                                            if (multiSelectCubeLabels == null || jQuery.isEmptyObject(multiSelectCubeLabels)) {
                                                multiSelectCubeLabels = {};
                                            }

                                            var selectedCubesArray = selectedCubesArrayObj[chartId];
                                            if (selectedCubesArray == null || selectedCubesArray.length == 0) {
                                                selectedCubesArray = [];
                                            }


                                            multiSelectCubeLabels = {};
//                                        multiSelectCubeLabels[xlabelId + ylabelId + zlabelId] = clickedLabelIds;
                                            multiSelectCubeLabels[cubePosition.x + "," + cubePosition.y + "," + cubePosition.z] = clickedLabelIds;

                                            var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                                            var Nmaterial = new THREE.MeshBasicMaterial({
                                                color: $("#BAR_CHART_CUBE_CLICK_AXIS_COLORS").val(),
                                                vertexColors: true,
                                                opacity: 0.6,
                                                transparent: true
                                            });


                                            var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
                                            ncube.position.set(cubePosition.x - ((xlen / 2 * cubeSize) + cubeSize / 2), cubePosition.y - ((ylen / 2 * cubeSize) + cubeSize / 2), cubePosition.z + ((zlen / 2 * cubeSize) + cubeSize / 2));

                                            scene.add(ncube);
                                            multiSelectCubeLabelsObj[chartId] = multiSelectCubeLabels;

                                            selectedCubesArray.push(ncube);
                                            selectedCubesArrayObj[chartId] = selectedCubesArray;
                                            if ((axisColumns['x']['column']).indexOf("Q_ID") > -1)
                                            {
                                                var clickedValue = clickedLabelIds['x'];
                                                $("#" + chartId + "_cubeQuarterClickValue").val(clickedValue);

                                            } else if ((axisColumns['y']['column']).indexOf("Q_ID") > -1)
                                            {
                                                var clickedValue = clickedLabelIds['y'];
                                                $("#" + chartId + "_cubeQuarterClickValue").val(clickedValue);
                                            } else if ((axisColumns['z']['column']).indexOf("Q_ID") > -1)
                                            {
                                                var clickedValue = clickedLabelIds['z'];
                                                $("#" + chartId + "_cubeQuarterClickValue").val(clickedValue);
                                            }
                                        }

// for slice or Dice

                                        var lineSlice = multiSelectCubeLabelsObj[chartId]['lineSlice'];


                                        var lineSliceWhrClause = "";
                                        var xWhereClause = "";
                                        var yWhereClause = "";
                                        var zWhereClause = "";

                                        $.each(multiSelectCubeLabelsObj[chartId], function (key, val) {
                                            if (key != "Dimension" && key != "lineSlice") {
                                                if (xWhereClause.indexOf("'" + this['x'] + "',") == -1) {
                                                    xWhereClause += "'" + this['x'] + "',";
                                                }
                                                if (yWhereClause.indexOf("'" + this['y'] + "',") == -1) {
                                                    yWhereClause += "'" + this['y'] + "',";
                                                }
                                                if (zWhereClause.indexOf("'" + this['z'] + "',") == -1) {
                                                    zWhereClause += "'" + this['z'] + "',";
                                                }
                                            }
                                        })

//cube changes
                                        lineSliceWhrClause = axisColumns['x']['column'] + " IN (" + xWhereClause.slice(0, -1) + ") AND " + axisColumns['y']['column'] + " IN (" + yWhereClause.slice(0, -1) + ") AND " + axisColumns['z']['column'] + " IN (" + zWhereClause.slice(0, -1) + ")";

                                        if (multiSelectCubeLabelsObj != null && ((Object.keys(multiSelectCubeLabelsObj[chartId]).length > 0 && multiSelectCubeLabelsObj[chartId]['Dimension'] != '3D' && multiSelectCubeLabelsObj[chartId]['Dimension'] != '2D') ||
                                                (lineSlice == "Y"))) {

                                            $.ajax({
                                                type: "post",
                                                traditional: true,
                                                dataType: 'JSON',
                                                cache: false,
                                                url: "getCubeValue",
                                                data: {
                                                    factTable: factTable,
                                                    cubeValueCol: cubeValueCol,
                                                    clickedLabels: JSON.stringify(clickedLabelIds),
                                                    axisColumns: JSON.stringify(axisColumns),
                                                    lineSlice: lineSlice,
                                                    lineSliceWhrClause: lineSliceWhrClause,
                                                    chartId: chartId,
                                                    factType: factType
                                                },
                                                success: function (response) {

                                                    var cubeValue = response['cubeValue']
                                                    var elem = document.createElement('div');
                                                    elem.className = 'cubeValueLabel';

                                                    var Label = new CSS2DObject(elem);


                                                    elem.innerHTML = cubeValue;
                                                    elem.id = chartId + "_CUBEVAL_" + cubeValue;
                                                    ncube.add(Label);



                                                    $("#" + chartId + "_x_" + clickedLabelIds['x']).addClass("highLightLabel");
                                                    $("#" + chartId + "_y_" + clickedLabelIds['y']).addClass("highLightLabel");
                                                    $("#" + chartId + "_z_" + clickedLabelIds['z']).addClass("highLightLabel");
                                                    stopLoader();
                                                },
                                                error: function (e) {
                                                    stopLoader();
                                                    sessionTimeout(e);
                                                }
                                            });

                                        }
                                        globalSceneObj[chartId] = scene;
                                    });


                                    domEvents.addEventListener(cube, 'dblclick', function (event) {
                                        console.log("dbl clicked");
                                        event.origDomEvent.preventDefault();
                                        var cubePosition = event.target.position;
                                        var xindex = Math.abs(cubePosition.x / cubeSize);
                                        var xlabel = labelsArray['x'][xindex - 1][0];
                                        var xlabelId = labelsArray['x'][xindex - 1][1];
                                        var yindex = Math.abs(cubePosition.y / cubeSize);
                                        var ylabel = labelsArray['y'][yindex - 1][0];
                                        var ylabelId = labelsArray['y'][yindex - 1][1];
                                        var zindex = Math.abs(cubePosition.z / cubeSize);
                                        var zlabel = labelsArray['z'][zindex - 1][0];
                                        var zlabelId = labelsArray['z'][zindex - 1][1];
                                        alert("X::" + xlabel + "  Y::" + ylabel + "  Z::" + zlabel);
                                        var clickedLabels = {}
                                        clickedLabels['x'] = xlabel;
                                        clickedLabels['y'] = ylabel;
                                        clickedLabels['z'] = zlabel;
                                        var clickedLabelIds = {}
                                        clickedLabelIds['x'] = xlabelId;
                                        clickedLabelIds['y'] = ylabelId;
                                        clickedLabelIds['z'] = zlabelId;
                                        var multiSelectCubeLabels = {};
                                        multiSelectCubeLabels[xlabelId + ylabelId + zlabelId] = clickedLabelIds;

                                        multiSelectCubeLabelsObj[chartId] = multiSelectCubeLabels;
                                        showReport('', chartId);


                                        globalSceneObj[chartId] = scene;
                                    });

                                }
                            }
                        }
                        $('#visionAnalyticCubeDropdownLabelx').on('select', function (event) {
                            if (event.args) {
                                var item = event.args.item;
                                if (item != null) {
                                    var cubeListBoxLabelsObj = $("#" + chartId + "_cubeListBoxLabelsObj").val();
                                    if (cubeListBoxLabelsObj != null && cubeListBoxLabelsObj != '' && cubeListBoxLabelsObj != undefined)
                                    {
                                        cubeListBoxLabelsObj = JSON.parse(cubeListBoxLabelsObj);
                                    }
                                    var label = item.label;
                                    var selectValue = item.value;
                                    var value = cubeListBoxLabelsObj[label];
                                    var source = cubeCheckBoxObj[selectValue];
                                    var xaxisColumnsVal = axisColumns['x'];
                                    var xaxisLable = xaxisColumnsVal.label;
                                    cubeListBoxLabelsObj[label] = 'x';
                                    cubeListBoxLabelsObj[xaxisLable] = value;
                                    var axisColumnsVal = axisColumns[value];
                                    axisColumns['x'] = axisColumnsVal;
                                    axisColumns[value] = xaxisColumnsVal;
                                    var xaxisLablesVal = axisLabels['x'];
                                    var axisLablesVal = axisLabels[value];
                                    axisLabels['x'] = axisLablesVal;
                                    axisLabels[value] = xaxisLablesVal;
                                    $("#" + chartId + "_axisLabels").val(JSON.stringify(axisLabels));
                                    $("#" + chartId + "_axisColumns").val(JSON.stringify(axisColumns));
                                    $("#" + chartId + "_cubeListBoxLabelsObj").val(JSON.stringify(cubeListBoxLabelsObj));
                                    $("#visionAnalyticCubeCheckx").jqxListBox({
                                        filterable: true,
                                        checkboxes: true,
                                        source: source,
                                        theme: 'energyblue',
                                        displayMember: 'text',
                                        valueMember: 'value'
                                    });
                                    var ids = checkedIdsObj[value];
                                    var xids = checkedIdsObj['x'];
                                    checkedIdsObj['x'] = ids;
                                    checkedIdsObj[value] = xids;
                                    $("#" + chartId + "_checkedIdsObj").val(JSON.stringify(checkedIdsObj));
                                    for (var id = 0; id < ids.length; id++)
                                    {
                                        $("#visionAnalyticCubeCheckx").jqxListBox('checkItem', ids[id]);
                                    }
                                }
                            }
                        });
                        $('#visionAnalyticCubeDropdownLabely').on('select', function (event) {
                            if (event.args) {
                                var item = event.args.item;
                                if (item != null) {
                                    var cubeListBoxLabelsObj = $("#" + chartId + "_cubeListBoxLabelsObj").val();
                                    if (cubeListBoxLabelsObj != null && cubeListBoxLabelsObj != '' && cubeListBoxLabelsObj != undefined)
                                    {
                                        cubeListBoxLabelsObj = JSON.parse(cubeListBoxLabelsObj);
                                    }
                                    var label = item.label;
                                    var selectValue = item.value;
                                    var value = cubeListBoxLabelsObj[label];
                                    var source = cubeCheckBoxObj[selectValue];
                                    var yaxisColumnsVal = axisColumns['y'];
                                    var yaxisLable = yaxisColumnsVal.label;
                                    cubeListBoxLabelsObj[label] = 'y';
                                    cubeListBoxLabelsObj[yaxisLable] = value;
                                    var axisColumnsVal = axisColumns[value];
                                    axisColumns['y'] = axisColumnsVal;
                                    axisColumns[value] = yaxisColumnsVal;
                                    var yaxisLablesVal = axisLabels['y'];
                                    var axisLablesVal = axisLabels[value];
                                    axisLabels['y'] = axisLablesVal;
                                    axisLabels[value] = yaxisLablesVal;
                                    $("#" + chartId + "_axisLabels").val(JSON.stringify(axisLabels));
                                    $("#" + chartId + "_axisColumns").val(JSON.stringify(axisColumns));
                                    $("#" + chartId + "_cubeListBoxLabelsObj").val(JSON.stringify(cubeListBoxLabelsObj));
                                    $("#visionAnalyticCubeChecky").jqxListBox({
                                        filterable: true,
                                        checkboxes: true,
                                        source: source,
                                        theme: 'energyblue',
                                        displayMember: 'text',
                                        valueMember: 'value'
                                    });
                                    var ids = checkedIdsObj[value];
                                    var yids = checkedIdsObj['y'];
                                    checkedIdsObj['y'] = ids;
                                    checkedIdsObj[value] = yids;
                                    $("#" + chartId + "_checkedIdsObj").val(JSON.stringify(checkedIdsObj));
                                    for (var id = 0; id < ids.length; id++)
                                    {
                                        $("#visionAnalyticCubeChecky").jqxListBox('checkItem', ids[id]);
                                    }
                                }
                            }
                        });
                        $('#visionAnalyticCubeDropdownLabelz').on('select', function (event) {
                            if (event.args) {
                                var item = event.args.item;
                                if (item != null) {
                                    var cubeListBoxLabelsObj = $("#" + chartId + "_cubeListBoxLabelsObj").val();
                                    if (cubeListBoxLabelsObj != null && cubeListBoxLabelsObj != '' && cubeListBoxLabelsObj != undefined)
                                    {
                                        cubeListBoxLabelsObj = JSON.parse(cubeListBoxLabelsObj);
                                    }
                                    var label = item.label;
                                    var selectValue = item.value;
                                    var value = cubeListBoxLabelsObj[label];
                                    var source = cubeCheckBoxObj[selectValue];
                                    var zaxisColumnsVal = axisColumns['z'];
                                    var zaxisLable = zaxisColumnsVal.label;
                                    cubeListBoxLabelsObj[label] = 'z';
                                    cubeListBoxLabelsObj[zaxisLable] = value;
                                    var axisColumnsVal = axisColumns[value];
                                    axisColumns['z'] = axisColumnsVal;
                                    axisColumns[value] = zaxisColumnsVal;
                                    var zaxisLablesVal = axisLabels['z'];
                                    var axisLablesVal = axisLabels[value];
                                    axisLabels['z'] = axisLablesVal;
                                    axisLabels[value] = zaxisLablesVal;
                                    $("#" + chartId + "_axisLabels").val(JSON.stringify(axisLabels));
                                    $("#" + chartId + "_axisColumns").val(JSON.stringify(axisColumns));
                                    $("#" + chartId + "_cubeListBoxLabelsObj").val(JSON.stringify(cubeListBoxLabelsObj));
                                    $("#visionAnalyticCubeCheckz").jqxListBox({
                                        filterable: true,
                                        checkboxes: true,
                                        source: source,
                                        theme: 'energyblue',
                                        displayMember: 'text',
                                        valueMember: 'value'
                                    });
                                    var ids = checkedIdsObj[value];
                                    var zids = checkedIdsObj['z'];
                                    checkedIdsObj['z'] = ids;
                                    checkedIdsObj[value] = zids;
                                    $("#" + chartId + "_checkedIdsObj").val(JSON.stringify(checkedIdsObj));
                                    for (var id = 0; id < ids.length; id++)
                                    {
                                        $("#visionAnalyticCubeCheckz").jqxListBox('checkItem', ids[id]);
                                    }
                                }
                            }
                        });

                        var controls = new THREE.OrbitControls(camera, labelRenderer.domElement);
                        controls.update();
                        controls.target.set(0, 0, 0);
                        function render() {

                            if (rotateFlagCW) {
                                $("#" + chartId + "_cubeContainer").find("div").css("display", "none");
//                            1800*5, 1000*5, 600*5)
                                camera.position.x -= 180 / 2;
                                camera.position.y -= 100 / 2;
                                camera.position.z -= 60 / 2;
                                scene.rotation.x += 0.1;
                                scene.rotation.y += 0.1;
                                scene.rotation.z += 0.1;
                            }

                            requestAnimationFrame(render);
                            renderer.render(scene, camera);
                            labelRenderer.render(scene, camera);

                        }
                        render();
                        rotateCube(scene, camera, chartId);
                        var qubeChartId = chartId + "_cubeContainer";
                        var pos = $("#" + qubeChartId).css({marginLeft: -664});


                        var dragControls = new THREE.DragControls(cubeObjects, camera, labelRenderer.domElement);
                        var tempCubes = [];
                        dragControls.addEventListener('dragstart', function (event) {

                            if (event.object.type == 'LineSegments') {
                                var cubePosition = event.object.position;
                                var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                                var Nmaterial = new THREE.MeshBasicMaterial({
                                    color: 0xffffff,
                                    vertexColors: true,
                                    opacity: 0.5,
                                    transparent: true
                                });
                                for (var i = 0; i < 3; i++) {
                                    Ngeometry.faces[4 * i].color = colors[i];
                                    Ngeometry.faces[4 * i + 1].color = colors[i];
                                    Ngeometry.faces[4 * i + 2].color = colors[i];
                                    Ngeometry.faces[4 * i + 3].color = colors[i];
                                }
                                var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
                                ncube.position.set(event.object.position.x, event.object.position.y, event.object.position.z);

                                tempCubes.push(ncube);
                                cubeObjects.push(ncube);
                                ncube.userData['relativePosition'] = cubePosition;
                            }
                            if (event.object.type == 'Mesh') {
                                controls.enabled = false;
                                var cubePosition = event.object.position;
                                var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                                var Nmaterial = new THREE.MeshBasicMaterial({
                                    color: 0xffffff,
                                    vertexColors: true,
                                    opacity: 0.5,
                                    transparent: true
                                });
                                for (var i = 0; i < 3; i++) {
                                    Ngeometry.faces[4 * i].color = colors[i];
                                    Ngeometry.faces[4 * i + 1].color = colors[i];
                                    Ngeometry.faces[4 * i + 2].color = colors[i];
                                    Ngeometry.faces[4 * i + 3].color = colors[i];
                                }
                                var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
                                ncube.position.set(event.object.position.x, event.object.position.y, event.object.position.z);
                                scene.add(ncube);
                                tempCubes = [];
                                tempCubes.push(ncube);
                                cubeObjects.push(ncube);
                                ncube.userData['relativePosition'] = cubePosition;

                            }


                        });
                        dragControls.addEventListener('drag', function (event) {

                            if (event.object.type == 'LineSegments') {

                            }
                            if (event.object.type == 'Mesh') {
                                controls.enabled = false;
                                $("#dropAreaId").empty();
                                $("#dropAreaId").remove();
                                var str = "<div id ='dropAreaId' class='cubecheckclass'>"
                                        + "<div id='spanDiv' style='text-align: center; margin: 0 auto;'><span>Drop here</span></div>"
                                        + "<div id='cubeDropId'></div></div>";
                                $("#" + chartId + "_Parent").append(str);
                                $("#dropAreaId").css('height', '350px');
                                $("#dropAreaId").css('width', '350px');
                                $("#dropAreaId").css('position', 'absolute');
                                $("#dropAreaId").css('z-index', '9999');
                                $('#dropAreaId').css('border', '1px solid black');
                                $('#dropAreaId').css({marginLeft: 450});
                                $('#dropAreaId').css({marginTop: 50});
                                $("#spanDiv").css('height', '30px', '!important');


                            }


                        });
                        var xlabelId = '';
                        var ylabelId = '';
                        var zlabelId = '';
                        var n = 0;
                        divArray = [];
                        dragControls.addEventListener('dragend', function (event) {
                            if (event.object.type == 'LineSegments') {
                                var tempCube = tempCubes[0];
                                event.object.position.set(tempCube.position.x, tempCube.position.y, tempCube.position.z);
                                tempCube.geometry.dispose();
                                tempCube.material.dispose();
                                scene.remove(tempCube);
                            }

                            if (event.object.type == 'Mesh') {
                                controls.enabled = true;
                                var cubePosition = event.object.userData['relativePosition'];
                                if (cubePosition != null) {
                                    var xindex = Math.abs((cubePosition.x) / cubeSize);
                                    var xlabel = labelsArray['x'][xindex - 1][0];
                                    xlabelId = labelsArray['x'][xindex - 1][1];
                                    var yindex = Math.abs((cubePosition.y) / cubeSize);
                                    var ylabel = labelsArray['y'][yindex - 1][0];
                                    ylabelId = labelsArray['y'][yindex - 1][1];
                                    var zindex = Math.abs((cubePosition.z) / cubeSize);
                                    var zlabel = labelsArray['z'][zindex - 1][0];
                                    zlabelId = labelsArray['z'][zindex - 1][1];
                                }
                                var tempCube = tempCubes[0];
                                event.object.position.set(tempCube.position.x, tempCube.position.y, tempCube.position.z);
                                tempCube.geometry.dispose();
                                tempCube.material.dispose();
                                scene.remove(tempCube);
                                if (divArray.length >= 2)
                                {
                                    removeFirstCube();
                                }
                                n++;
                                var clickedValuesObj = {};
                                clickedValuesObj['x'] = xlabelId;
                                clickedValuesObj['y'] = ylabelId;
                                clickedValuesObj['z'] = zlabelId;
                                setTimeout(function () {
                                    make_canvas(tabId, chartId, chartVal, clickedValuesObj, n);
                                }, 10);

                            }

                        });
//ravinder uncommented code for click purpose by jagadish //cube mdrm
//                        var pos = $("#" + qubeChartId).find("canvas").position();
//                        var pos1 = $("#" + qubeChartId).find("div:first").position();
//                        $("#" + qubeChartId).find("div:first").css({top: pos.top, left: pos.left});
//                        var pos2 = $("#" + qubeChartId).find("div:first").position();
//
//                        $("#" + tabId).scroll(function () {
//                            var pos = $("#" + qubeChartId).find("canvas").position();
//                            var pos1 = $("#" + qubeChartId).find("div:first").position();
//                            $("#" + qubeChartId).find("div:first").css({top: pos.top, left: pos.left});
//                            var pos2 = $("#" + qubeChartId).find("div:first").position();
//                        })
                    }
                    $("#wait").css("display", "none");
                }
            }

        },
        error: function (e) {
            sessionTimeout(e);
        }

    });
}

function cubeslideSettings()
{
    $('#visionAnalyticCubeChartCheckBox').toggle('slide', {direction: 'right'}, 500);
    $("#cubepersonalizeid").toggleClass("ui-icon-triangle-1-s");

}
function populateSelectedColor(id, inputId, selectionFlag) {
    inputId = $.trim(inputId);
    if (selectionFlag == 'S') {
        $("#" + inputId).val($("#" + id).val());
    }
}

function rotateCube(scene, camera, chartId) {

    rotateFlagCW = true;
    setTimeout(function () {
        $("#" + chartId + "_cubeContainer").find("div").css("display", "block");
        rotateFlagCW = false;
        camera.position.set(1800 * 0.8, 1000 * 0.8, 600 * 0.8);
        scene.rotation.x = 0;
        scene.rotation.y = 0;
        scene.rotation.z = 0;
    }, 1400)

}
function resetCubeColorData(tabId, chartId, chartVal)
{
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        cache: false,
        url: "analyticDropdown",
        data: {
            'tabComponentId': chartId,
            'tabId': tabId,
            'chartTypeId': chartVal
        },
        success: function (response) {
            if (response != null)
            {
                var result = JSON.parse(response);
                var dataObj = result['chartList'];
                for (var i = 0; i < dataObj.length; i++)
                {
                    $("#wait").css("display", "block");
                    var chartData = dataObj[i];
                    getResetCubeColorData(chartData, tabId, chartId);
                }
            }
        },
        error: function (e) {
            sessionTimeout(e);
        }
    });
}
function getResetCubeColorData(chartData, tabId, chartId) {
    var checkBoxObjCheckedVal = {};
    alert("checked");
    var axisColumns = $("#" + chartId + "_axisColumns").val();
    var axisLabels = $("#" + chartId + "_axisLabels").val();
    var checkedXvalues = $("#visionAnalyticCubeCheckx").jqxListBox('getCheckedItems');
    var checkedYvalues = $("#visionAnalyticCubeChecky").jqxListBox('getCheckedItems');
    var checkedZvalues = $("#visionAnalyticCubeCheckz").jqxListBox('getCheckedItems');
    var xValues = [];
    var yValues = [];
    var zValues = [];
    $.each(checkedXvalues, function (index) {
        xValues.push(this.value);
    });
    $.each(checkedYvalues, function (index) {
        yValues.push(this.value);
    });
    $.each(checkedZvalues, function (index) {
        zValues.push(this.value);
    });
    checkBoxObjCheckedVal['x'] = xValues;
    checkBoxObjCheckedVal['y'] = yValues;
    checkBoxObjCheckedVal['z'] = zValues;
    $("#" + chartId + "_checkBoxChecked").remove();
    $("#" + chartId + "_colorObj").remove();
    $("#" + chartId + "_clickedcolorObj").remove();
    $("#cubeCheckBoxCheckedValues").append("<input type='hidden' id='" + chartId + "_checkBoxChecked' value=''/>");
    $("#cubeCheckBoxCheckedValues").append("<input type='hidden' id='" + chartId + "_colorObj' value=''/>"); //cube changes
    $("#cubeCheckBoxCheckedValues").append("<input type='hidden' id='" + chartId + "_clickedcolorObj' value=''/>"); //cube changes
    $("#" + chartId + "_checkBoxChecked").val(JSON.stringify(checkBoxObjCheckedVal));
    var chartOptAllObj = {}; //cube personalize
    var colorX = $("#BAR_CHART_CUBE_X_AXIS_COLORS").val();
    var colorY = $("#BAR_CHART_CUBE_Y_AXIS_COLORS").val();
    var colorZ = $("#BAR_CHART_CUBE_Z_AXIS_COLORS").val();
    chartOptAllObj['x'] = colorX;
    chartOptAllObj['y'] = colorY;
    chartOptAllObj['z'] = colorZ;
    $("#" + chartId + "_colorObj").val(JSON.stringify(chartOptAllObj));//cube changes
    var clickedcolorsObj = {}; //cube personalize
    var clickColor = $("#BAR_CHART_CUBE_CLICK_AXIS_COLORS").val();
    var sliceColor = $("#BAR_CHART_CUBE_SLICE_AXIS_COLORS").val();
    var diceColor = $("#BAR_CHART_CUBE_DICE_AXIS_COLORS").val();
    clickedcolorsObj['click'] = clickColor;
    clickedcolorsObj['slice'] = sliceColor;
    clickedcolorsObj['dice'] = diceColor;
    $("#" + chartId + "_clickedcolorObj").val(JSON.stringify(clickedcolorsObj));//cube changes
    var data = {
        'chartData': chartData,
        'ddwFlag': 'Y',
        checkBoxObjCheckedVal: JSON.stringify(checkBoxObjCheckedVal),
        cubeColors: JSON.stringify(chartOptAllObj),
        cubeClickedColors: JSON.stringify(clickedcolorsObj),
        axisColumns: axisColumns,
        axisLabels: axisLabels
    };
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        url: "getQubeAnalytics",
        async: true,
        cache: false,
        data: data,
        success: function (response) {
            $("#wait").css("display", "none");
            if (response != null && response != '' && response != undefined) {
                var resultData = JSON.parse(response);
                if (resultData != null)
                {
                    var result = resultData;
                    if (result != null) {
                        $("#wait").css("display", "none");
                        var chartsObj = result['chartsObj'];
                        var divid = chartsObj['divId'];
                        var seqNo = chartsObj['seqNo'];
                        var chartId = chartsObj['chartId'];
                        var sizeOfChart = chartsObj['sizeOfChart'];
                        var height = chartsObj['height'];
                        var width = chartsObj['width'];
                        var factTable = chartsObj['factTable'];
                        var cubeValueCol = chartsObj['cubeValueCol'];
                        var axisColumns = chartsObj['axisColumns'];
                        var axisLabels = chartsObj['axisLabels'];
                        var labelsArray = chartsObj['labelsObject'];
                        var title = chartsObj['title'];
                        var factType = chartsObj['factType']; //cube mdrm 
                        var chartVal = chartsObj['chartVal']; //cube mdrm 
                        var cubeChartId = $("#cubeChartId").val();
                        $("#" + cubeChartId).empty();
                        $('#' + cubeChartId).attr('id', chartId + '_Chart');
                        $("#cubeChartId").val(chartId + "_Chart");
                        var cubeCheckBoxDivId = chartsObj['cubeCheckBoxDivId'];
                        $("#" + chartId + "_Chart").html(divid);
                        $("#" + chartId + "_factTable").val(factTable);
                        $("#" + chartId + "_factType").val(factType);
                        $("#" + chartId + "_cubeValueCol").val(cubeValueCol);
                        $("#" + chartId + "_axisColumns").val(JSON.stringify(axisColumns));
                        $("#" + chartId + "_axisLabels").val(JSON.stringify(axisLabels));
                        var checkedIdsObj = chartsObj['checkedIdsObj']; //multi cube 
                        var cubeCheckBoxObj = chartsObj['cubeCheckBoxObj']; //multi cube 
                        var dropdownCubeObj = chartsObj['dropdownCubeObj']; //multi cube 
                        var colorsObj = chartsObj['colorsObj'];
//                        var multiSelectCubeLabels = {};
//                        $("#" + chartId + "_multiSelectCubeLabels").val(JSON.stringify(multiSelectCubeLabels));
                        $("#visionAnalyticCubeChartCheckBox").html(cubeCheckBoxDivId); //cube personalize above row remove
                        for (var key in dropdownCubeObj)
                        {
                            var source = dropdownCubeObj[key];
                            if ($('#visionAnalyticCubeDropdownLabel' + key).length != 0) {
                                $('#visionAnalyticCubeDropdownLabel' + key).jqxDropDownList("destroy");
                                $('#visionAnalyticCubeDropdownLabel' + key).jqxDropDownList({
                                    source: source,
                                    theme: 'energyblue',
                                    displayMember: 'text',
                                    valueMember: 'value',
                                    dropDownHeight: 105,
                                    dropDownWidth: 107
                                });
                                $("#visionAnalyticCubeDropdownLabel" + key).jqxDropDownList('selectItem', key);
                            }
                        }
                        for (var key in cubeCheckBoxObj)
                        {
                            var source = cubeCheckBoxObj[key];
                            if ($("#visionAnalyticCubeCheck" + key).length != 0) {
                                $("#visionAnalyticCubeCheck" + key).jqxListBox({
                                    filterable: true,
                                    checkboxes: true,
                                    source: source,
                                    theme: 'energyblue',
                                    displayMember: 'text',
                                    valueMember: 'value'
                                });
                                var ids = checkedIdsObj[key];
                                if (ids != null && !jQuery.isEmptyObject(ids)) {
                                    for (var id = 0; id < ids.length; id++)
                                    {
                                        $("#visionAnalyticCubeCheck" + key).jqxListBox('checkItem', ids[id]);
                                    }
                                }
                            }
                        }
                        if (sizeOfChart != null && sizeOfChart != '' && sizeOfChart != undefined) {
                            $("#" + chartId + "_Chart").css("width", "" + sizeOfChart + "%", "important");
                        } else if (windowWidth > 1024)
                        {
                            $("#" + chartId + "_Chart").css('width', '99%', 'important');
                        } else if (windowWidth < 1024 && windowWidth > 700)
                        {
                            $("#" + chartId + "_Chart").css('width', '49%');
                        } else if (windowWidth < 699 && windowWidth > 320)
                        {
                            $("#" + chartId + "_Chart").css('width', '98%');
                        }

                        $("#" + chartId).html("<div id ='" + chartId + "_cubeContainer' class='VisionShowCubeChart'></div>");
                        $("#" + chartId + "_cube_types").val(title);

                        var noOfCubes = 3;
                        var cubeSize = 100;
                        multiSelectCubeLabelsObj = {};
                        selectedCubesArrayObj = {};
                        var scene = new THREE.Scene();
                        var camera = new THREE.PerspectiveCamera(45, 2.5, 1, 10000);
                        var renderer = new THREE.WebGLRenderer({antialias: true});
                        renderer.setSize(parseInt(width) + 600, height);
                        scene.background = new THREE.Color(0xFFFFFF);

                        document.querySelector("#" + chartId + "_cubeContainer").appendChild(renderer.domElement);
                        camera.position.set(1800 * 5, 1000 * 5, 600 * 5);


                        var raycaster = new THREE.Raycaster();
                        var mouse = new THREE.Vector2();
                        const axesHelper = new THREE.AxesHelper((noOfCubes + 5) * 100);
                        renderer.setPixelRatio(window.devicePixelRatio);
                        renderer.setSize(parseInt(width) + 600, height);
                        var labelRenderer = new CSS2DRenderer();
                        labelRenderer.setSize(parseInt(width) + 600, height);
                        labelRenderer.domElement.style.position = 'absolute';
                        labelRenderer.domElement.style.top = '0px';
                        document.querySelector('#' + chartId + "_cubeContainer").appendChild(labelRenderer.domElement);
                        var controls = new THREE.OrbitControls(camera, labelRenderer.domElement);
                        controls.update();
                        controls.target.set(0, 0, 0);

                        var domEvents = new THREEx.DomEvents(camera, labelRenderer.domElement);
                        var axisArray = [];
                        var xlen = labelsArray['x'].length;
                        var ylen = labelsArray['y'].length;
                        var zlen = labelsArray['z'].length;

                        var xIndex = 1;
                        for (var j = -xlen / 2; j < xlen / 2; j++) {
                            var sign = j / Math.abs(j);
                            var elem = document.createElement('div');
                            elem.className = 'label';
                            elem.style.marginTop = '-1em';
                            var Label = new CSS2DObject(elem);
                            var xpoints = [];
                            if (j != (xlen / 2 - 1)) {
                                xpoints.push(new THREE.Vector3((j - 0.5) * cubeSize, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
                                xpoints.push(new THREE.Vector3((j + 0.5) * cubeSize, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
                            } else {

                                xpoints.push(new THREE.Vector3((j - 0.5) * cubeSize, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
                                xpoints.push(new THREE.Vector3((j + 0.5) * cubeSize * 2, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
//                            xpoints.push(new THREE.Vector3((j) * cubeSize, (j/Math.abs(j))* (ylen/2)* cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
//                            xpoints.push(new THREE.Vector3((j * cubeSize) + 1.5 * cubeSize, (j/Math.abs(j))* (ylen/2)* cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
                            }
                            var xmaterial = new THREE.LineBasicMaterial({color: 0x0071C5});
                            var xgeometry = new THREE.BufferGeometry().setFromPoints(xpoints);
                            var xaxis = new THREE.Line(xgeometry, xmaterial);
                            scene.add(xaxis);
                            Label.position.set((j + 0.5) * cubeSize, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize);
                            elem.textContent = labelsArray['x'][xIndex - 1][0];
                            elem.id = chartId + "_x_" + labelsArray['x'][xIndex - 1][1];
                            xaxis.add(Label);
                            axisArray.push(xaxis)
                            xIndex++;
                        }


                        var yIndex = 1;
                        for (var j = -(ylen / 2); j < ylen / 2; j++) {

                            var elem = document.createElement('div');
                            elem.className = 'label';
                            elem.style.marginTop = '-1em';
                            var Label = new CSS2DObject(elem);
                            var ypoints = [];
                            if (j != (ylen / 2 - 1)) {

                                ypoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize, (j - 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
                                ypoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize, (j + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
//                            ypoints.push(new THREE.Vector3((j/Math.abs(j))* (xlen/2)* cubeSize, (j - 1) * cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
//                            ypoints.push(new THREE.Vector3((j/Math.abs(j))* (xlen/2)* cubeSize, (j * cubeSize), (j/Math.abs(j))* (zlen/2)* cubeSize));
                            } else {

                                ypoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize, (j - 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
                                ypoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize, (j + 0.5) * cubeSize * 1.5, (zlen / 2 + 0.5) * cubeSize));
//                            ypoints.push(new THREE.Vector3((j/Math.abs(j))* (xlen/2)* cubeSize, (j - 1) * cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
//                            ypoints.push(new THREE.Vector3((j/Math.abs(j))* (xlen/2)* cubeSize, (j * cubeSize) + 1 * cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
                            }

                            var ymaterial = new THREE.LineBasicMaterial({color: 0x0071C5});
                            var ygeometry = new THREE.BufferGeometry().setFromPoints(ypoints);
                            var yaxis = new THREE.Line(ygeometry, ymaterial);
                            scene.add(yaxis);
                            Label.position.set(-(xlen / 2 + 0.5) * cubeSize, (j + 0.5) * cubeSize - 20, (zlen / 2 + 0.5) * cubeSize);
                            elem.textContent = labelsArray['y'][yIndex - 1][0];
                            elem.id = chartId + "_y_" + labelsArray['y'][yIndex - 1][1];
                            yaxis.add(Label);
                            axisArray.push(yaxis)
                            yIndex++;

                        }


                        var zIndex = 1;
                        for (var j = -(zlen / 2); j < zlen / 2; j++) {
                            var noOfCubes = labelsArray['x'].length;
                            var elem = document.createElement('div');
                            elem.className = 'label';
                            elem.style.marginTop = '-1em';
                            var Label = new CSS2DObject(elem);
                            var zpoints = [];
                            if (j != (zlen / 2 - 1)) {

                                zpoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j - 0.5) * cubeSize));
                                zpoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j + 0.5) * cubeSize));
//                            zpoints.push(new THREE.Vector3((noOfCubes * cubeSize + cubeSize), (j/Math.abs(j))* (ylen/2)* cubeSize, -(j - 1) * cubeSize));
//                            zpoints.push(new THREE.Vector3((noOfCubes * cubeSize + cubeSize), (j/Math.abs(j))* (ylen/2)* cubeSize, -(j * cubeSize)));
                            } else {

                                zpoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j - 0.5) * cubeSize));
                                zpoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j + 0.5) * cubeSize * 1.5));
//                            zpoints.push(new THREE.Vector3((noOfCubes * cubeSize + cubeSize), (j/Math.abs(j))* (ylen/2)* cubeSize, -(j - 1) * cubeSize));
//                            zpoints.push(new THREE.Vector3((noOfCubes * cubeSize + cubeSize), (j/Math.abs(j))* (ylen/2)* cubeSize, -((j * cubeSize) + 1 * cubeSize)));
                            }

                            var zmaterial = new THREE.LineBasicMaterial({color: 0x0071C5});
                            var zgeometry = new THREE.BufferGeometry().setFromPoints(zpoints);
                            var zaxis = new THREE.Line(zgeometry, zmaterial);
                            scene.add(zaxis);
                            Label.position.set(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j + 0.5) * cubeSize);
//                        Label.position.set(noOfCubes * cubeSize + cubeSize, (j/Math.abs(j))* (ylen/2)* cubeSize, -j * cubeSize);
                            elem.textContent = labelsArray['z'][zIndex - 1][0];
                            elem.id = chartId + "_z_" + labelsArray['z'][zIndex - 1][1];
                            zaxis.add(Label);
                            axisArray.push(zaxis)
                            zIndex++;

                        }


                        var geometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
// material
                        var material = new THREE.MeshBasicMaterial({
                            color: 0xffffff,
                            vertexColors: true,
                            opacity: 0.5,
                            transparent: true
                        });

                        red = new THREE.Color('#40875e'); //cube mdrm
                        green = new THREE.Color('#a8a240');
                        blue = new THREE.Color('#f0ab00');

                        $("#" + chartId + "_colorObj").remove();
                        $("#cubeCheckBoxCheckedValues").append("<input type='hidden' id='" + chartId + "_colorObj' value=''/>");
                        var chartOptAllObj = {}; //cube personalize
                        $("#BAR_CHART_CUBE_X_AXIS_COLORS").val('#40875e');
                        $("#BAR_CHART_CUBE_Y_AXIS_COLORS").val('#a8a240');
                        $("#BAR_CHART_CUBE_Z_AXIS_COLORS").val('#f0ab00');
                        chartOptAllObj['x'] = '#40875e';
                        chartOptAllObj['y'] = '#a8a240';
                        chartOptAllObj['z'] = '#f0ab00';
                        $("#" + chartId + "_colorObj").val(JSON.stringify(chartOptAllObj));

                        var colors = [red, green, blue];
                        for (var i = 0; i < 3; i++) {
                            geometry.faces[4 * i].color = colors[i];
                            geometry.faces[4 * i + 1].color = colors[i];
                            geometry.faces[4 * i + 2].color = colors[i];
                            geometry.faces[4 * i + 3].color = colors[i];
                        }
                        var cubeObjects = [];
                        var relativeX = 0;
                        var relativeY = 0;
                        var relativeZ = 0;
                        for (var i = -(xlen / 2); i < xlen / 2; i++) {

                            relativeX = relativeX + cubeSize;
                            relativeY = 0;
                            relativeZ = 0;
                            for (var j = -(ylen / 2); j < ylen / 2; j++) {
                                relativeY = relativeY + cubeSize;
                                relativeZ = 0;
                                for (var k = -(zlen / 2); k < zlen / 2; k++) {
                                    relativeZ = relativeZ - cubeSize;
                                    var cube = new THREE.Mesh(geometry, material);
                                    scene.add(cube);
                                    var edges = new THREE.EdgesGeometry(geometry);
                                    line = new THREE.LineSegments(edges, new THREE.LineBasicMaterial({color: 0x000000, opacity: 0.5, transparent: true}));
                                    cube.add(line);
                                    cube.position.set((i + 0.5) * cubeSize, (j + 0.5) * cubeSize, -(k + 0.5) * cubeSize);
                                    var relativePosition = {};
                                    relativePosition.x = relativeX;
                                    relativePosition.y = relativeY;
                                    relativePosition.z = relativeZ;

                                    cube.userData['relativePosition'] = relativePosition;
                                    cubeObjects.push(cube);

// CLICK EVENT----->>>>>

                                    domEvents.addEventListener(cube, 'click', function (event) {
                                        console.log("clicked");
                                        event.origDomEvent.preventDefault();
                                        cubeClickFlag = true;
                                        material.opacity = 0.5;
                                        globalMaterial[chartId] = material;
                                        var originalcubePosition = event.target.position;
                                        var cubePosition = event.target.userData['relativePosition'];
//                                    cubePosition.x = cubePosition.x+300;
//                                    cubePosition.y = cubePosition.y+300;
//                                    cubePosition.z = cubePosition.z-300;

                                        var xindex = Math.abs((cubePosition.x) / cubeSize);
                                        var xlabel = labelsArray['x'][xindex - 1][0];
                                        var xlabelId = labelsArray['x'][xindex - 1][1];
                                        var yindex = Math.abs((cubePosition.y) / cubeSize);
                                        var ylabel = labelsArray['y'][yindex - 1][0];
                                        var ylabelId = labelsArray['y'][yindex - 1][1];
                                        var zindex = Math.abs((cubePosition.z) / cubeSize);
                                        var zlabel = labelsArray['z'][zindex - 1][0];
                                        var zlabelId = labelsArray['z'][zindex - 1][1];
                                        alert("X::" + xlabel + "  Y::" + ylabel + "  Z::" + zlabel);
                                        var clickedLabels = {}
                                        clickedLabels['x'] = xlabel;
                                        clickedLabels['y'] = ylabel;
                                        clickedLabels['z'] = zlabel;

                                        var clickedLabelIds = {}
                                        clickedLabelIds['x'] = xlabelId;
                                        clickedLabelIds['y'] = ylabelId;
                                        clickedLabelIds['z'] = zlabelId;

                                        var firstSelectedCubeLebel = {};

                                        if (event.origDomEvent.shiftKey) {

                                            var multiSelectCubeLabels = multiSelectCubeLabelsObj[chartId];
                                            if (multiSelectCubeLabels == null || jQuery.isEmptyObject(multiSelectCubeLabels)) {
                                                multiSelectCubeLabels = {};
                                            }

                                            var selectedCubesArray = selectedCubesArrayObj[chartId];
                                            if (selectedCubesArray == null || selectedCubesArray.length == 0) {
                                                selectedCubesArray = [];
                                            }

                                            if (multiSelectCubeLabels != null && Object.keys(multiSelectCubeLabels).length == 1) {
                                                firstSelectedCubeLebel = multiSelectCubeLabels
                                                var matchCount = 0;
                                                var values = Object.values(multiSelectCubeLabels)[0];
                                                var staticAxi = [];
                                                var sliceAxis;
                                                if (values['x'] == clickedLabelIds['x']) {
                                                    matchCount++;
                                                    staticAxi.push('x');
                                                } else {
                                                    sliceAxis = 'x';
                                                }
                                                if (values['y'] == clickedLabelIds['y']) {
                                                    matchCount++;
                                                    staticAxi.push('y');
                                                } else {
                                                    sliceAxis = 'y';
                                                }
                                                if (values['z'] == clickedLabelIds['z']) {
                                                    matchCount++;
                                                    staticAxi.push('z');
                                                } else {
                                                    sliceAxis = 'z';
                                                }
                                                if (matchCount == 5) {

                                                    $(".label").removeClass("highLightLabel");
                                                    $.each(selectedCubesArray, function (index) {

                                                        scene.remove(selectedCubesArray[index]);
                                                        $(".cubeLabel").remove();
                                                        $(".cubeValueLabel").remove();
                                                    })


                                                    labelsArray[sliceAxis];
//                                                var selectedCubesArray = selectedCubesArrayObj[chartId];
//                                                 if (selectedCubesArray==null || selectedCubesArray.length==0){
//                                                     selectedCubesArray = [];
//                                                 }
                                                    var selectCubeLabels = {};
                                                    var addOrder;
                                                    var sliceAxisPos;
                                                    var newCubePosition = {};
                                                    var cubeReached = false;
                                                    for (var i = 0; i < labelsArray[sliceAxis].length; i++) {

                                                        if (!cubeReached) {
                                                            if (i == 0) {
                                                                var yellowCube = selectedCubesArray[0];
                                                                newCubePosition = yellowCube.position;
                                                                sliceAxisPos = newCubePosition[sliceAxis];
                                                                if (Math.abs(cubePosition[sliceAxis]) - Math.abs(newCubePosition[sliceAxis]) >= 0) {
                                                                    addOrder = "plus";
                                                                } else {
                                                                    addOrder = "minus";
                                                                }

                                                            } else {
                                                                if (addOrder == "plus") {
                                                                    newCubePosition[sliceAxis] = sliceAxisPos + (sliceAxisPos / Math.abs(sliceAxisPos) * i * cubeSize);
                                                                    if (newCubePosition[sliceAxis] == event.target.position[sliceAxis]) {
                                                                        cubeReached = true;
                                                                    }
                                                                } else if (addOrder == "minus") {

                                                                    newCubePosition[sliceAxis] = sliceAxisPos - (sliceAxisPos / Math.abs(sliceAxisPos) * i * cubeSize);
                                                                    if (newCubePosition[sliceAxis] == event.target.position[sliceAxis]) {
                                                                        cubeReached = true;
                                                                    }
                                                                }

                                                            }
                                                            if (sliceAxis == 'z') {
                                                                var sliceAxisindex = Math.abs((newCubePosition[sliceAxis]) / cubeSize);
                                                            } else {
                                                                var sliceAxisindex = Math.abs((newCubePosition[sliceAxis]) / cubeSize);
                                                            }

                                                            var sliceAxislabel = labelsArray[sliceAxis][sliceAxisindex - 1][0];
                                                            var sliceAxislabelId = labelsArray[sliceAxis][sliceAxisindex - 1][1];

                                                            var staticAxis1 = staticAxi[0];
                                                            if (staticAxis1 == 'z') {
                                                                var staticAxis1index = Math.abs((newCubePosition[staticAxis1]) / cubeSize);
                                                            } else {
                                                                var staticAxis1index = Math.abs((newCubePosition[staticAxis1]) / cubeSize);
                                                            }

                                                            var staticAxis1label = labelsArray[staticAxis1][staticAxis1index - 1][0];
                                                            var staticAxis1labelId = labelsArray[staticAxis1][staticAxis1index - 1][1];

                                                            var staticAxis2 = staticAxi[1];
                                                            if (staticAxis2 == 'z') {
                                                                var staticAxis2index = Math.abs((newCubePosition[staticAxis2]) / cubeSize);
                                                            } else {
                                                                var staticAxis2index = Math.abs((newCubePosition[staticAxis2]) / cubeSize);
                                                            }

                                                            var staticAxis2label = labelsArray[staticAxis2][staticAxis2index - 1][0];
                                                            var staticAxis2labelId = labelsArray[staticAxis2][staticAxis2index - 1][1];

                                                            var labels = {}
                                                            labels[sliceAxis] = sliceAxislabel;
                                                            labels[staticAxis1] = staticAxis1label;
                                                            labels[staticAxis2] = staticAxis2label;

                                                            var labelIds = {}
                                                            labelIds[sliceAxis] = sliceAxislabelId;
                                                            labelIds[staticAxis1] = staticAxis1labelId;
                                                            labelIds[staticAxis2] = staticAxis2labelId;

//                                                        selectCubeLabels[sliceAxislabelId + staticAxis1labelId + staticAxis2labelId] = labelIds;
                                                            selectCubeLabels[newCubePosition.x + "," + newCubePosition.y + "," + newCubePosition.z] = labelIds;
//                                                       selectCubeLabels[newCubePosition[sliceAxis] +","+ newCubePosition[staticAxis1] +","+ newCubePosition[staticAxis2]] = labelIds;

                                                            var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                                                            var Nmaterial = new THREE.MeshBasicMaterial({
                                                                color: "#FFFF00",
                                                                vertexColors: true,
                                                                opacity: 0.6,
                                                                transparent: true
                                                            });

                                                            var ncube = new THREE.Mesh(Ngeometry, Nmaterial);

                                                            ncube.position.set(newCubePosition.x - ((xlen / 2 * cubeSize) + cubeSize / 2), newCubePosition.y - ((ylen / 2 * cubeSize) + cubeSize / 2), newCubePosition.z + ((zlen / 2 * cubeSize) + cubeSize / 2));

                                                            scene.add(ncube);
                                                            selectedCubesArray.push(ncube);

                                                        }
                                                    }

                                                    $.each(selectCubeLabels, function (indx) {
                                                        var selectedLableIds = this;
                                                        $("#" + chartId + "_" + sliceAxis + "_" + selectedLableIds[sliceAxis]).addClass("highLightLabel");
                                                    })

                                                    selectCubeLabels['lineSlice'] = 'Y';
                                                    multiSelectCubeLabelsObj[chartId] = selectCubeLabels;
                                                    selectedCubesArrayObj[chartId] = selectedCubesArray;



                                                } else {
                                                    //single dice code 
                                                    //showDialog("Incorrect Selection");


                                                    var selectedCubes = multiSelectCubeLabels;
                                                    var pos = cubePosition;
                                                    var prevCubePosArr = Object.keys(multiSelectCubeLabels)[0].split(",");
                                                    var prevCubePos = {};
                                                    prevCubePos['x'] = parseInt(prevCubePosArr[0]);
                                                    prevCubePos['y'] = parseInt(prevCubePosArr[1]);
                                                    prevCubePos['z'] = parseInt(prevCubePosArr[2]);

                                                    resetCubes("", chartId);


                                                    var zDiff = cubePosition.z / 100 - prevCubePos['z'] / 100;
                                                    var yDiff = cubePosition.y / 100 - prevCubePos['y'] / 100;
                                                    var xDiff = cubePosition.x / 100 - prevCubePos['x'] / 100;
                                                    var zeroDiffCount = 0;
                                                    if (zDiff == 0) {
                                                        zeroDiffCount++;
                                                    }
                                                    if (yDiff == 0) {
                                                        zeroDiffCount++;
                                                    }
                                                    if (xDiff == 0) {
                                                        zeroDiffCount++;
                                                    }
                                                    if (zeroDiffCount == 2) {
                                                        multiSelectCubeLabels['lineSlice'] = 'Y';
                                                    }
                                                    var colorCode = "";
                                                    if (zeroDiffCount == 0)
                                                    {
                                                        colorCode = $("#BAR_CHART_CUBE_DICE_AXIS_COLORS").val();
                                                    } else if (zeroDiffCount == 1)
                                                    {
                                                        colorCode = $("#BAR_CHART_CUBE_SLICE_AXIS_COLORS").val();
                                                    } else if (zeroDiffCount == 2)
                                                    {
                                                        colorCode = $("#BAR_CHART_CUBE_SLICE_AXIS_COLORS").val();
                                                    }
                                                    for (var k = 0; k <= Math.abs(zDiff); k++) {
                                                        for (var j = 0; j <= Math.abs(yDiff); j++) {

                                                            for (var i = 0; i <= Math.abs(xDiff); i++) {


                                                                var currentCubePos = {};

                                                                currentCubePos['x'] = prevCubePos.x + ((xDiff != 0) ? (xDiff / Math.abs(xDiff) * i * 100) : 0);
                                                                currentCubePos['y'] = prevCubePos.y + ((yDiff != 0) ? (yDiff / Math.abs(yDiff) * j * 100) : 0);
                                                                currentCubePos['z'] = prevCubePos.z + ((zDiff != 0) ? (zDiff / Math.abs(zDiff) * k * 100) : 0);

                                                                var xindex = Math.abs(currentCubePos.x / cubeSize);
                                                                var xlabel = labelsArray['x'][xindex - 1][0];
                                                                var xlabelId = labelsArray['x'][xindex - 1][1];

                                                                var yindex = Math.abs(currentCubePos.y / cubeSize);
                                                                var ylabel = labelsArray['y'][yindex - 1][0];
                                                                var ylabelId = labelsArray['y'][yindex - 1][1];

                                                                var zindex = Math.abs(currentCubePos.z / cubeSize);
                                                                var zlabel = labelsArray['z'][zindex - 1][0];
                                                                var zlabelId = labelsArray['z'][zindex - 1][1];

                                                                alert("X::" + xlabel + "  Y::" + ylabel + "  Z::" + zlabel);
                                                                var clickedLabels = {}
                                                                clickedLabels['x'] = xlabel;
                                                                clickedLabels['y'] = ylabel;
                                                                clickedLabels['z'] = zlabel;
                                                                var clickedLabelIds = {}
                                                                clickedLabelIds['x'] = xlabelId;
                                                                clickedLabelIds['y'] = ylabelId;
                                                                clickedLabelIds['z'] = zlabelId;

//                                                          multiSelectCubeLabels[xlabelId + ylabelId + zlabelId] = clickedLabelIds;
                                                                multiSelectCubeLabels[currentCubePos.x + "," + currentCubePos.y + "," + currentCubePos.z] = clickedLabelIds;
                                                                multiSelectCubeLabels['Dimension'] = '3D';

                                                                $("#" + chartId + "_x_" + clickedLabelIds['x']).addClass("highLightLabel");
                                                                $("#" + chartId + "_y_" + clickedLabelIds['y']).addClass("highLightLabel");
                                                                $("#" + chartId + "_z_" + clickedLabelIds['z']).addClass("highLightLabel");

                                                                material.opacity = 0.1;
                                                                globalMaterial[chartId] = material;
                                                                var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                                                                var Nmaterial = new THREE.MeshBasicMaterial({
                                                                    color: colorCode,
                                                                    vertexColors: true,
                                                                    opacity: 0.6,
                                                                    transparent: true
                                                                });

                                                                var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
                                                                ncube.position.set(currentCubePos.x - ((xlen / 2 * cubeSize) + cubeSize / 2), currentCubePos.y - ((ylen / 2 * cubeSize) + cubeSize / 2), currentCubePos.z + ((zlen / 2 * cubeSize) + cubeSize / 2));

                                                                scene.add(ncube);
                                                                selectedCubesArray.push(ncube);
                                                            }
                                                        }
                                                    }
//                                                }



                                                    multiSelectCubeLabelsObj[chartId] = multiSelectCubeLabels;

                                                    selectedCubesArrayObj[chartId] = selectedCubesArray;
                                                    stopLoader();
//return false;
//single dice code 
                                                }



                                            }
// ravi dice code start

// ravi dice code end
                                            else {
                                                resetCubes("", chartId);
                                                var multiSelectCubeLabels = multiSelectCubeLabelsObj[chartId];
                                                if (multiSelectCubeLabels == null || jQuery.isEmptyObject(multiSelectCubeLabels)) {
                                                    multiSelectCubeLabels = {};
                                                }

                                                var selectedCubesArray = selectedCubesArrayObj[chartId];
                                                if (selectedCubesArray == null || selectedCubesArray.length == 0) {
                                                    selectedCubesArray = [];
                                                }

                                                multiSelectCubeLabels = {};
                                                selectedCubesArray = [];
//                                            multiSelectCubeLabels[xlabelId + ylabelId + zlabelId] = clickedLabelIds;
                                                multiSelectCubeLabels[cubePosition.x + "," + cubePosition.y + "," + cubePosition.z] = clickedLabelIds;

                                                var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                                                var Nmaterial = new THREE.MeshBasicMaterial({
                                                    color: $("#BAR_CHART_CUBE_CLICK_AXIS_COLORS").val(),
                                                    vertexColors: true,
                                                    opacity: 0.6,
                                                    transparent: true
                                                });


                                                var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
                                                ncube.position.set(cubePosition.x - ((xlen / 2 * cubeSize) + cubeSize / 2), cubePosition.y - ((ylen / 2 * cubeSize) + cubeSize / 2), cubePosition.z + ((zlen / 2 * cubeSize) + cubeSize / 2));

                                                scene.add(ncube);
                                                multiSelectCubeLabelsObj[chartId] = multiSelectCubeLabels;

                                                selectedCubesArray.push(ncube);
                                                selectedCubesArrayObj[chartId] = selectedCubesArray;


                                            }
                                        } else {//cube mdrm
                                            resetCubes("", chartId);
                                            var multiSelectCubeLabels = multiSelectCubeLabelsObj[chartId];
                                            if (multiSelectCubeLabels == null || jQuery.isEmptyObject(multiSelectCubeLabels)) {
                                                multiSelectCubeLabels = {};
                                            }

                                            var selectedCubesArray = selectedCubesArrayObj[chartId];
                                            if (selectedCubesArray == null || selectedCubesArray.length == 0) {
                                                selectedCubesArray = [];
                                            }


                                            multiSelectCubeLabels = {};
//                                        multiSelectCubeLabels[xlabelId + ylabelId + zlabelId] = clickedLabelIds;
                                            multiSelectCubeLabels[cubePosition.x + "," + cubePosition.y + "," + cubePosition.z] = clickedLabelIds;

                                            var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                                            var Nmaterial = new THREE.MeshBasicMaterial({
                                                color: $("#BAR_CHART_CUBE_CLICK_AXIS_COLORS").val(),
                                                vertexColors: true,
                                                opacity: 0.6,
                                                transparent: true
                                            });


                                            var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
                                            ncube.position.set(cubePosition.x - ((xlen / 2 * cubeSize) + cubeSize / 2), cubePosition.y - ((ylen / 2 * cubeSize) + cubeSize / 2), cubePosition.z + ((zlen / 2 * cubeSize) + cubeSize / 2));

                                            scene.add(ncube);
                                            multiSelectCubeLabelsObj[chartId] = multiSelectCubeLabels;

                                            selectedCubesArray.push(ncube);
                                            selectedCubesArrayObj[chartId] = selectedCubesArray;
                                            if ((axisColumns['x']['column']).indexOf("Q_ID") > -1)
                                            {
                                                var clickedValue = clickedLabelIds['x'];
                                                $("#" + chartId + "_cubeQuarterClickValue").val(clickedValue);

                                            } else if ((axisColumns['y']['column']).indexOf("Q_ID") > -1)
                                            {
                                                var clickedValue = clickedLabelIds['y'];
                                                $("#" + chartId + "_cubeQuarterClickValue").val(clickedValue);
                                            } else if ((axisColumns['z']['column']).indexOf("Q_ID") > -1)
                                            {
                                                var clickedValue = clickedLabelIds['z'];
                                                $("#" + chartId + "_cubeQuarterClickValue").val(clickedValue);
                                            }
                                        }

// for slice or Dice

                                        var lineSlice = multiSelectCubeLabelsObj[chartId]['lineSlice'];


                                        var lineSliceWhrClause = "";
                                        var xWhereClause = "";
                                        var yWhereClause = "";
                                        var zWhereClause = "";

                                        $.each(multiSelectCubeLabelsObj[chartId], function (key, val) {
                                            if (key != "Dimension" && key != "lineSlice") {
                                                if (xWhereClause.indexOf("'" + this['x'] + "',") == -1) {
                                                    xWhereClause += "'" + this['x'] + "',";
                                                }
                                                if (yWhereClause.indexOf("'" + this['y'] + "',") == -1) {
                                                    yWhereClause += "'" + this['y'] + "',";
                                                }
                                                if (zWhereClause.indexOf("'" + this['z'] + "',") == -1) {
                                                    zWhereClause += "'" + this['z'] + "',";
                                                }
                                            }
                                        })

//cube changes
                                        lineSliceWhrClause = axisColumns['x']['column'] + " IN (" + xWhereClause.slice(0, -1) + ") AND " + axisColumns['y']['column'] + " IN (" + yWhereClause.slice(0, -1) + ") AND " + axisColumns['z']['column'] + " IN (" + zWhereClause.slice(0, -1) + ")";

                                        if (multiSelectCubeLabelsObj != null && ((Object.keys(multiSelectCubeLabelsObj[chartId]).length > 0 && multiSelectCubeLabelsObj[chartId]['Dimension'] != '3D' && multiSelectCubeLabelsObj[chartId]['Dimension'] != '2D') ||
                                                (lineSlice == "Y"))) {

                                            $.ajax({
                                                type: "post",
                                                traditional: true,
                                                dataType: 'JSON',
                                                cache: false,
                                                url: "getCubeValue",
                                                data: {
                                                    factTable: factTable,
                                                    cubeValueCol: cubeValueCol,
                                                    clickedLabels: JSON.stringify(clickedLabelIds),
                                                    axisColumns: JSON.stringify(axisColumns),
                                                    lineSlice: lineSlice,
                                                    lineSliceWhrClause: lineSliceWhrClause,
                                                    chartId: chartId,
                                                    factType: factType
                                                },
                                                success: function (response) {

                                                    var cubeValue = response['cubeValue']
                                                    var elem = document.createElement('div');
                                                    elem.className = 'cubeValueLabel';

                                                    var Label = new CSS2DObject(elem);


                                                    elem.innerHTML = cubeValue;
                                                    elem.id = chartId + "_CUBEVAL_" + cubeValue;
                                                    ncube.add(Label);



                                                    $("#" + chartId + "_x_" + clickedLabelIds['x']).addClass("highLightLabel");
                                                    $("#" + chartId + "_y_" + clickedLabelIds['y']).addClass("highLightLabel");
                                                    $("#" + chartId + "_z_" + clickedLabelIds['z']).addClass("highLightLabel");
                                                    stopLoader();
                                                },
                                                error: function (e) {
                                                    stopLoader();
                                                    sessionTimeout(e);
                                                }
                                            });

                                        }
                                        globalSceneObj[chartId] = scene;
                                    });


                                    domEvents.addEventListener(cube, 'dblclick', function (event) {
                                        console.log("dbl clicked");
                                        event.origDomEvent.preventDefault();
                                        var cubePosition = event.target.position;
                                        var xindex = Math.abs(cubePosition.x / cubeSize);
                                        var xlabel = labelsArray['x'][xindex - 1][0];
                                        var xlabelId = labelsArray['x'][xindex - 1][1];
                                        var yindex = Math.abs(cubePosition.y / cubeSize);
                                        var ylabel = labelsArray['y'][yindex - 1][0];
                                        var ylabelId = labelsArray['y'][yindex - 1][1];
                                        var zindex = Math.abs(cubePosition.z / cubeSize);
                                        var zlabel = labelsArray['z'][zindex - 1][0];
                                        var zlabelId = labelsArray['z'][zindex - 1][1];
                                        alert("X::" + xlabel + "  Y::" + ylabel + "  Z::" + zlabel);
                                        var clickedLabels = {}
                                        clickedLabels['x'] = xlabel;
                                        clickedLabels['y'] = ylabel;
                                        clickedLabels['z'] = zlabel;
                                        var clickedLabelIds = {}
                                        clickedLabelIds['x'] = xlabelId;
                                        clickedLabelIds['y'] = ylabelId;
                                        clickedLabelIds['z'] = zlabelId;
                                        var multiSelectCubeLabels = {};
                                        multiSelectCubeLabels[xlabelId + ylabelId + zlabelId] = clickedLabelIds;

                                        multiSelectCubeLabelsObj[chartId] = multiSelectCubeLabels;
                                        showReport('', chartId);


                                        globalSceneObj[chartId] = scene;
                                    });

                                }
                            }
                        }
                        $('#visionAnalyticCubeDropdownLabelx').on('select', function (event) {
                            if (event.args) {
                                var item = event.args.item;
                                if (item != null) {
                                    var cubeListBoxLabelsObj = $("#" + chartId + "_cubeListBoxLabelsObj").val();
                                    if (cubeListBoxLabelsObj != null && cubeListBoxLabelsObj != '' && cubeListBoxLabelsObj != undefined)
                                    {
                                        cubeListBoxLabelsObj = JSON.parse(cubeListBoxLabelsObj);
                                    }
                                    var label = item.label;
                                    var selectValue = item.value;
                                    var value = cubeListBoxLabelsObj[label];
                                    var source = cubeCheckBoxObj[selectValue];
                                    var xaxisColumnsVal = axisColumns['x'];
                                    var xaxisLable = xaxisColumnsVal.label;
                                    cubeListBoxLabelsObj[label] = 'x';
                                    cubeListBoxLabelsObj[xaxisLable] = value;
                                    var axisColumnsVal = axisColumns[value];
                                    axisColumns['x'] = axisColumnsVal;
                                    axisColumns[value] = xaxisColumnsVal;
                                    var xaxisLablesVal = axisLabels['x'];
                                    var axisLablesVal = axisLabels[value];
                                    axisLabels['x'] = axisLablesVal;
                                    axisLabels[value] = xaxisLablesVal;
                                    $("#" + chartId + "_axisLabels").val(JSON.stringify(axisLabels));
                                    $("#" + chartId + "_axisColumns").val(JSON.stringify(axisColumns));
                                    $("#" + chartId + "_cubeListBoxLabelsObj").val(JSON.stringify(cubeListBoxLabelsObj));
                                    $("#visionAnalyticCubeCheckx").jqxListBox({
                                        filterable: true,
                                        checkboxes: true,
                                        source: source,
                                        theme: 'energyblue',
                                        displayMember: 'text',
                                        valueMember: 'value'
                                    });
                                    var ids = checkedIdsObj[value];
                                    var xids = checkedIdsObj['x'];
                                    checkedIdsObj['x'] = ids;
                                    checkedIdsObj[value] = xids;
                                    $("#" + chartId + "_checkedIdsObj").val(JSON.stringify(checkedIdsObj));
                                    for (var id = 0; id < ids.length; id++)
                                    {
                                        $("#visionAnalyticCubeCheckx").jqxListBox('checkItem', ids[id]);
                                    }
                                }
                            }
                        });
                        $('#visionAnalyticCubeDropdownLabely').on('select', function (event) {
                            if (event.args) {
                                var item = event.args.item;
                                if (item != null) {
                                    var cubeListBoxLabelsObj = $("#" + chartId + "_cubeListBoxLabelsObj").val();
                                    if (cubeListBoxLabelsObj != null && cubeListBoxLabelsObj != '' && cubeListBoxLabelsObj != undefined)
                                    {
                                        cubeListBoxLabelsObj = JSON.parse(cubeListBoxLabelsObj);
                                    }
                                    var label = item.label;
                                    var selectValue = item.value;
                                    var value = cubeListBoxLabelsObj[label];
                                    var source = cubeCheckBoxObj[selectValue];
                                    var yaxisColumnsVal = axisColumns['y'];
                                    var yaxisLable = yaxisColumnsVal.label;
                                    cubeListBoxLabelsObj[label] = 'y';
                                    cubeListBoxLabelsObj[yaxisLable] = value;
                                    var axisColumnsVal = axisColumns[value];
                                    axisColumns['y'] = axisColumnsVal;
                                    axisColumns[value] = yaxisColumnsVal;
                                    var yaxisLablesVal = axisLabels['y'];
                                    var axisLablesVal = axisLabels[value];
                                    axisLabels['y'] = axisLablesVal;
                                    axisLabels[value] = yaxisLablesVal;
                                    $("#" + chartId + "_axisLabels").val(JSON.stringify(axisLabels));
                                    $("#" + chartId + "_axisColumns").val(JSON.stringify(axisColumns));
                                    $("#" + chartId + "_cubeListBoxLabelsObj").val(JSON.stringify(cubeListBoxLabelsObj));
                                    $("#visionAnalyticCubeChecky").jqxListBox({
                                        filterable: true,
                                        checkboxes: true,
                                        source: source,
                                        theme: 'energyblue',
                                        displayMember: 'text',
                                        valueMember: 'value'
                                    });
                                    var ids = checkedIdsObj[value];
                                    var yids = checkedIdsObj['y'];
                                    checkedIdsObj['y'] = ids;
                                    checkedIdsObj[value] = yids;
                                    $("#" + chartId + "_checkedIdsObj").val(JSON.stringify(checkedIdsObj));
                                    for (var id = 0; id < ids.length; id++)
                                    {
                                        $("#visionAnalyticCubeChecky").jqxListBox('checkItem', ids[id]);
                                    }
                                }
                            }
                        });
                        $('#visionAnalyticCubeDropdownLabelz').on('select', function (event) {
                            if (event.args) {
                                var item = event.args.item;
                                if (item != null) {
                                    var cubeListBoxLabelsObj = $("#" + chartId + "_cubeListBoxLabelsObj").val();
                                    if (cubeListBoxLabelsObj != null && cubeListBoxLabelsObj != '' && cubeListBoxLabelsObj != undefined)
                                    {
                                        cubeListBoxLabelsObj = JSON.parse(cubeListBoxLabelsObj);
                                    }
                                    var label = item.label;
                                    var selectValue = item.value;
                                    var value = cubeListBoxLabelsObj[label];
                                    var source = cubeCheckBoxObj[selectValue];
                                    var zaxisColumnsVal = axisColumns['z'];
                                    var zaxisLable = zaxisColumnsVal.label;
                                    cubeListBoxLabelsObj[label] = 'z';
                                    cubeListBoxLabelsObj[zaxisLable] = value;
                                    var axisColumnsVal = axisColumns[value];
                                    axisColumns['z'] = axisColumnsVal;
                                    axisColumns[value] = zaxisColumnsVal;
                                    var zaxisLablesVal = axisLabels['z'];
                                    var axisLablesVal = axisLabels[value];
                                    axisLabels['z'] = axisLablesVal;
                                    axisLabels[value] = zaxisLablesVal;
                                    $("#" + chartId + "_axisLabels").val(JSON.stringify(axisLabels));
                                    $("#" + chartId + "_axisColumns").val(JSON.stringify(axisColumns));
                                    $("#" + chartId + "_cubeListBoxLabelsObj").val(JSON.stringify(cubeListBoxLabelsObj));
                                    $("#visionAnalyticCubeCheckz").jqxListBox({
                                        filterable: true,
                                        checkboxes: true,
                                        source: source,
                                        theme: 'energyblue',
                                        displayMember: 'text',
                                        valueMember: 'value'
                                    });
                                    var ids = checkedIdsObj[value];
                                    var zids = checkedIdsObj['z'];
                                    checkedIdsObj['z'] = ids;
                                    checkedIdsObj[value] = zids;
                                    $("#" + chartId + "_checkedIdsObj").val(JSON.stringify(checkedIdsObj));
                                    for (var id = 0; id < ids.length; id++)
                                    {
                                        $("#visionAnalyticCubeCheckz").jqxListBox('checkItem', ids[id]);
                                    }
                                }
                            }
                        });

                        var controls = new THREE.OrbitControls(camera, labelRenderer.domElement);
                        controls.update();
                        controls.target.set(0, 0, 0);
                        function render() {

                            if (rotateFlagCW) {
                                $("#" + chartId + "_cubeContainer").find("div").css("display", "none");
//                            1800*5, 1000*5, 600*5)
                                camera.position.x -= 180 / 2;
                                camera.position.y -= 100 / 2;
                                camera.position.z -= 60 / 2;
                                scene.rotation.x += 0.1;
                                scene.rotation.y += 0.1;
                                scene.rotation.z += 0.1;
                            }

                            requestAnimationFrame(render);
                            renderer.render(scene, camera);
                            labelRenderer.render(scene, camera);

                        }
                        render();
                        rotateCube(scene, camera, chartId);
                        var qubeChartId = chartId + "_cubeContainer";
                        var pos = $("#" + qubeChartId).css({marginLeft: -664});

                        var dragControls = new THREE.DragControls(cubeObjects, camera, labelRenderer.domElement);
                        var tempCubes = [];
                        dragControls.addEventListener('dragstart', function (event) {

                            if (event.object.type == 'LineSegments') {
                                var cubePosition = event.object.position;
                                var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                                var Nmaterial = new THREE.MeshBasicMaterial({
                                    color: 0xffffff,
                                    vertexColors: true,
                                    opacity: 0.5,
                                    transparent: true
                                });
                                for (var i = 0; i < 3; i++) {
                                    Ngeometry.faces[4 * i].color = colors[i];
                                    Ngeometry.faces[4 * i + 1].color = colors[i];
                                    Ngeometry.faces[4 * i + 2].color = colors[i];
                                    Ngeometry.faces[4 * i + 3].color = colors[i];
                                }
                                var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
                                ncube.position.set(event.object.position.x, event.object.position.y, event.object.position.z);

                                tempCubes.push(ncube);
                                cubeObjects.push(ncube);
                                ncube.userData['relativePosition'] = cubePosition;
                            }
                            if (event.object.type == 'Mesh') {
                                controls.enabled = false;
                                var cubePosition = event.object.position;
                                var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                                var Nmaterial = new THREE.MeshBasicMaterial({
                                    color: 0xffffff,
                                    vertexColors: true,
                                    opacity: 0.5,
                                    transparent: true
                                });
                                for (var i = 0; i < 3; i++) {
                                    Ngeometry.faces[4 * i].color = colors[i];
                                    Ngeometry.faces[4 * i + 1].color = colors[i];
                                    Ngeometry.faces[4 * i + 2].color = colors[i];
                                    Ngeometry.faces[4 * i + 3].color = colors[i];
                                }
                                var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
                                ncube.position.set(event.object.position.x, event.object.position.y, event.object.position.z);
                                scene.add(ncube);
                                tempCubes = [];
                                tempCubes.push(ncube);
                                cubeObjects.push(ncube);
                                ncube.userData['relativePosition'] = cubePosition;

                            }


                        });
                        dragControls.addEventListener('drag', function (event) {

                            if (event.object.type == 'LineSegments') {

                            }
                            if (event.object.type == 'Mesh') {
                                controls.enabled = false;
                                $("#dropAreaId").empty();
                                $("#dropAreaId").remove();
                                var str = "<div id ='dropAreaId' class='cubecheckclass'>"
                                        + "<div id='spanDiv' style='text-align: center; margin: 0 auto;'><span>Drop here</span></div>"
                                        + "<div id='cubeDropId'></div></div>";
                                $("#" + chartId + "_Parent").append(str);
                                $("#dropAreaId").css('height', '350px');
                                $("#dropAreaId").css('width', '350px');
                                $("#dropAreaId").css('position', 'absolute');
                                $("#dropAreaId").css('z-index', '9999');
                                $('#dropAreaId').css('border', '1px solid black');
                                $('#dropAreaId').css({marginLeft: 450});
                                $('#dropAreaId').css({marginTop: 50});
                                $("#spanDiv").css('height', '30px', '!important');


                            }


                        });
                        var xlabelId = '';
                        var ylabelId = '';
                        var zlabelId = '';
                        var n = 0;
                        divArray = [];
                        dragControls.addEventListener('dragend', function (event) {
                            if (event.object.type == 'LineSegments') {
                                var tempCube = tempCubes[0];
                                event.object.position.set(tempCube.position.x, tempCube.position.y, tempCube.position.z);
                                tempCube.geometry.dispose();
                                tempCube.material.dispose();
                                scene.remove(tempCube);
                            }

                            if (event.object.type == 'Mesh') {
                                controls.enabled = true;
                                var cubePosition = event.object.userData['relativePosition'];
                                if (cubePosition != null) {
                                    var xindex = Math.abs((cubePosition.x) / cubeSize);
                                    var xlabel = labelsArray['x'][xindex - 1][0];
                                    xlabelId = labelsArray['x'][xindex - 1][1];
                                    var yindex = Math.abs((cubePosition.y) / cubeSize);
                                    var ylabel = labelsArray['y'][yindex - 1][0];
                                    ylabelId = labelsArray['y'][yindex - 1][1];
                                    var zindex = Math.abs((cubePosition.z) / cubeSize);
                                    var zlabel = labelsArray['z'][zindex - 1][0];
                                    zlabelId = labelsArray['z'][zindex - 1][1];
                                }
                                var tempCube = tempCubes[0];
                                event.object.position.set(tempCube.position.x, tempCube.position.y, tempCube.position.z);
                                tempCube.geometry.dispose();
                                tempCube.material.dispose();
                                scene.remove(tempCube);
                                if (divArray.length >= 2)
                                {
                                    removeFirstCube();
                                }
                                n++;
                                var clickedValuesObj = {};
                                clickedValuesObj['x'] = xlabelId;
                                clickedValuesObj['y'] = ylabelId;
                                clickedValuesObj['z'] = zlabelId;
                                setTimeout(function () {
                                    make_canvas(tabId, chartId, chartVal, clickedValuesObj, n);
                                }, 10);

                            }

                        });
//ravinder uncommented code for click purpose by jagadish //cube mdrm
//                        var pos = $("#" + qubeChartId).find("canvas").position();
//                        var pos1 = $("#" + qubeChartId).find("div:first").position();
//                        $("#" + qubeChartId).find("div:first").css({top: pos.top, left: pos.left});
//                        var pos2 = $("#" + qubeChartId).find("div:first").position();
//
//                        $("#" + tabId).scroll(function () {
//                            var pos = $("#" + qubeChartId).find("canvas").position();
//                            var pos1 = $("#" + qubeChartId).find("div:first").position();
//                            $("#" + qubeChartId).find("div:first").css({top: pos.top, left: pos.left});
//                            var pos2 = $("#" + qubeChartId).find("div:first").position();
//                        })
                    }
                    $("#wait").css("display", "none");
                }
            }

        },
        error: function (e) {
            sessionTimeout(e);
        }

    });
}


function addMultiDimCubeData(analytic_Id, analytic_Comp_Id, chartVal)
{
    $("#wait").css("display", "block");
    console.log("iam in analyticsTypeDropdown");
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        cache: false,
        url: "addCubeMultiDimensions",
        data: {
            analytic_Id: analytic_Id,
            analytic_Comp_Id: analytic_Comp_Id
        },
        success: function (response) {
            if (response != null)
            {
                $("#wait").css("display", "none");
                var result = JSON.parse(response);
                var formStr = result['formStr'];
                $("#dialog").html(formStr);
                var hiddenData = "<input type='hidden' id='factTableArray_hidden'/>"
                        + "<input type='hidden' id='dimTableArray_hidden'/><input type='hidden' id='factColsArray_hidden'/>"
                        + " <input type='hidden' id='dimColsArray_hidden'/>";
                $('#visionCubeMultiConfigColumnForm').append(hiddenData);
                $("#dialog").dialog({
                    modal: true,
                    title: (labelObject['Add Additional Dimensions'] != null ? labelObject['Add Additional Dimensions'] : 'Add Additional Dimensions'),
                    height: 'auto',
                    minHeight: 'auto',
                    minWidth: '600',
                    maxWidth: 'auto',
                    fluid: true,
                    buttons: {
                        Add: function () {
                            $("#wait").css("display", "block");
                            var axisColumns = {};
                            var axisLabels = {};
                            var multiDimCount = $("#" + analytic_Comp_Id + "_cubeDimCount").val();
                            var z = 0;
                            var y = 0;
                            if (multiDimCount != null && multiDimCount != '' && multiDimCount != undefined)
                            {
                                z = parseInt(multiDimCount);
                                y = parseInt(multiDimCount);
                            }
                            var axisColumns = {};
                            $("#VisionCubePossibelMultiFactTableId tbody tr").each(function () {
                                var tdArray = this.cells;
                                if (tdArray != null && tdArray.length != 0) {
                                    var axisColObj = {};
                                    var factTable = $(tdArray[1]).find("input").val();
                                    var factColumn = $(tdArray[2]).find("input").val();
                                    var factColLabel = $(tdArray[3]).find("input").val();
                                    if (factColumn != null && factColumn != '' && factColumn != undefined)
                                    {
                                        factColumn = factColumn.split(":")[1];
                                    }
                                    axisColObj['column'] = factColumn;
                                    axisColObj['label'] = factColLabel;
                                    axisColObj['table'] = factTable;
                                    axisColumns[z] = axisColObj;
                                }
                                z++;
                            });
                            $("#VisionCubePossibelMultiDimTableId tbody tr").each(function () {
                                var tdArray = this.cells;
                                if (tdArray != null && tdArray.length != 0) {
                                    var axisLabelObj = {};
                                    var dimTable = $(tdArray[1]).find("input").val();
                                    var dimColumn = $(tdArray[2]).find("input").val();
                                    if (dimColumn != null && dimColumn != '' && dimColumn != undefined)
                                    {
                                        dimColumn = dimColumn.split(":")[1];
                                    }
                                    axisLabelObj['column'] = dimColumn;
                                    axisLabelObj['table'] = dimTable;
                                    axisLabels[y] = axisLabelObj;
                                }
                                y++;
                            });
                            addCubeMultiDimData(analytic_Id, analytic_Comp_Id, chartVal, axisColumns, axisLabels, z);
                            $(this).html("");
                            $(this).dialog("close");
                            $(this).dialog("destroy");
                        },
                        Close: function () {
                            $(this).html("");
                            $(this).dialog("close");
                            $(this).dialog("destroy");
                        }

                    },
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
                var factTablesColsArray = result['factTablesColsArray'];
                var dimTablesColsArray = result['dimTablesColsArray'];
                var factTablesArray = result['factTablesArray'];
                var dimTablesArray = result['dimTablesArray'];
                $("#factTableArray_hidden").val(JSON.stringify(factTablesArray));
                $("#dimTableArray_hidden").val(JSON.stringify(dimTablesArray));
                $("#factColsArray_hidden").val(JSON.stringify(factTablesColsArray));
                $("#dimColsArray_hidden").val(JSON.stringify(dimTablesColsArray));

            }
        },
        error: function (e) {
            sessionTimeout(e);
        }
    });
}
function addCubeMultiDimData(analytic_Id, analytic_Comp_Id, chartVal, axisColumns, axisLabels, count)
{
    var axisColsStr = $("#" + analytic_Comp_Id + "_axisColumns").val();
    var axisLabelsStr = $("#" + analytic_Comp_Id + "_axisLabels").val();
    $("#" + analytic_Comp_Id + "_cubeDimCount").val(count);
    if (axisColsStr != null && axisColsStr != '' && axisColsStr != undefined)
    {
        var axisColsObj = JSON.parse(axisColsStr);
        if (axisColsObj != null && !jQuery.isEmptyObject(axisColsObj))
        {
            $.each(axisColsObj, function (key, val) {
                axisColumns[key] = val;
            });
        }
    }
    if (axisLabelsStr != null && axisLabelsStr != '' && axisLabelsStr != undefined)
    {
        var axisLabelsObj = JSON.parse(axisLabelsStr);
        if (axisLabelsObj != null && !jQuery.isEmptyObject(axisLabelsObj))
        {
            $.each(axisLabelsObj, function (key, val) {
                axisLabels[key] = val;
            });
        }
    }
    $("#" + analytic_Comp_Id + "_axisColumns").val(JSON.stringify(axisColumns));
    $("#" + analytic_Comp_Id + "_axisLabels").val(JSON.stringify(axisLabels));
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        cache: false,
        url: "addCubeMultiFactDimensions",
        data: {
            analytic_Id: analytic_Id,
            analytic_Comp_Id: analytic_Comp_Id,
            axisColumns: JSON.stringify(axisColumns),
            axisLabels: JSON.stringify(axisLabels),
            count: count
        },
        success: function (response) {
            if (response != null)
            {
                $("#wait").css("display", "none");
                var result = JSON.parse(response);
                var messageStr = result['messageStr'];
                $("#dialog").html(messageStr);
                $("#dialog").dialog({
                    modal: true,
                    title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
                    height: 'auto',
                    minHeight: 'auto',
                    minWidth: '600',
                    maxWidth: 'auto',
                    fluid: true,
                    buttons: {
                        OK: function () {
                            showCheckBoxCubeData(analytic_Id, analytic_Comp_Id, chartVal);
                            $(this).html("");
                            $(this).dialog("close");
                            $(this).dialog("destroy");
                        },
                        Close: function () {
                            $(this).html("");
                            $(this).dialog("close");
                            $(this).dialog("destroy");
                        }

                    },
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
        },
        error: function (e) {
            sessionTimeout(e);
        }
    });
}


function addColumnCubePossibleMultiFactValue(event, $this)
{
    var selectedColumnStr = "<tr style=\"height: 1px\">"
            + "<td width=\"1%\" class=\"visionCubePossibleMultiFactImgTd1\">"
            + "<img src=\"images/Detele Red Icon.svg\" onclick=\"deleteCubeSelectedRow(this)\" class=\"visionCubePossibleMultiFactImg\" title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\">"
            + "</td>"
            + "<td width=\"19%\"><input class=\"visionCubePossibleMultiFactColInput\" type=\"text\" value=\"\" readonly=\"true\">"
            + "<img title='Select Table' src=\"images/tree_icon.svg\" class=\"visionCubeAddDimColMapImage\""
            + "onclick=\"selectAdditionalDimColumn(this,'factTable')\" style=\"\"></td>"
            + "<td width=\"20%\"><input class=\"visionCubePossibleMultiFactColInput\" type=\"text\" value=\"\" readonly=\"true\">"
            + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionCubeAddDimColMapImage\""
            + "onclick=\"selectAdditionalDimColumn(this,'factColumn')\" style=\"\"></td>"
            + "<td width=\"20%\"><input class=\"visionCubePossibleMultiFactColInput\" type=\"text\" value=\"\" readonly=\"true\"></td>"
            + "</tr>";
    $("#VisionCubePossibelMultiFactTableId tbody").append(selectedColumnStr);
}
function addColumnCubePossibleMultiDimValue(event, $this)
{
    var selectedColumnStr = "<tr style=\"height: 1px\">"
            + "<td width=\"1%\" class=\"visionCubePossibleMultiDimImgTd1\">"
            + "<img src=\"images/Detele Red Icon.svg\" onclick=\"deleteCubeSelectedRow(this)\" class=\"visionCubePossibleMultiDimImg\" title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\">"
            + "</td>"
            + "<td width=\"19%\"><input class=\"visionCubeMultiDimColInput\" type=\"text\" value=\"\" readonly=\"true\">"
            + "<img title='Select Table' src=\"images/tree_icon.svg\" class=\"visionCubeAddDimColMapImage\""
            + "onclick=\"selectAdditionalDimColumn(this,'dimTable')\" style=\"\"></td>"
            + "<td width=\"20%\"><input class=\"visionCubeMultiDimColInput\" type=\"text\" value=\"\" readonly=\"true\">"
            + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionCubeAddDimColMapImage\""
            + "onclick=\"selectAdditionalDimColumn(this,'dimColumn')\" style=\"\"></td>"
            + "</tr>";
    $("#VisionCubePossibelMultiDimTableId tbody").append(selectedColumnStr);
}
function deleteCubeSelectedRow($this)//cube mdrm
{
    $($this).closest("tr").remove();
}
function selectAdditionalDimColumn($this, tableColType, dataArray) {
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
//dddw
// prepare the data
    var data = [];
    if (tableColType == 'factColumn') {
        var dataStr = $("#factColsArray_hidden").val();
        if (dataStr != null && dataStr != '') {
            data = JSON.parse(dataStr);
        }
    } else if (tableColType == 'dimColumn') {
        var dataStr = $("#dimColsArray_hidden").val();
        if (dataStr != null && dataStr != '') {
            data = JSON.parse(dataStr);
        }
    } else if (tableColType == 'factTable') {
        var dataStr = $("#factTableArray_hidden").val();
        if (dataStr != null && dataStr != '') {
            data = JSON.parse(dataStr);
        }
    } else if (tableColType == 'dimTable') {
        var dataStr = $("#dimTableArray_hidden").val();
        if (dataStr != null && dataStr != '') {
            data = JSON.parse(dataStr);
        }
    }

    var source =
            {
                datatype: "json",
                datafields: [
                    {name: 'id'},
                    {name: 'parentid'},
                    {name: 'text'},
                    {name: 'icon'},
                    {name: 'value'}
                ],
                id: 'id',
                icon: 'icon',
                localdata: data
            };
// create data adapter.
    var dataAdapter = new $.jqx.dataAdapter(source);
// perform Data Binding.
    dataAdapter.dataBind();
// get the tree items. The first parameter is the item's id. The second parameter is the parent item's id. The 'items' parameter represents 
// the sub items collection name. Each jqxTree item has a 'label' property, but in the JSON data, we have a 'text' field. The last parameter 
// specifies the mapping between the 'text' and 'label' fields.  
    var records = dataAdapter.getRecordsHierarchy('id', 'parentid', 'items', [{name: 'text', map: 'label'}]);
    $("#columnMappingDialog").html("<div class='treeSearchInputDiv'><input id='treeSearchValue' type='text' class='treeSearchValueInput' placeholder='Search'/>\n\
    <img id='treeNodeSearchIconId' src='images/icon.png' style='height:12px;width:12px;cursor:pointer;' onclick=searchTreeNode('columnMappingTree')  /><div id='searchTreeErrorMesg' style='color:red;'></div></div>\n\
<div id='columnMappingTree' class='columnMappingTree' style='height:180px;overflow-y:auto;'></div>");
    $("#columnMappingDialog").dialog({
        title: (labelObject['Columns'] != null ? labelObject['Columns'] : 'Columns'),
        modal: true,
        width: 'auto',
        maxWidth: 500,
        height: 330,
        fluid: true,
        buttons: [{
                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                click: function () {
                    var selectedItem = $("#columnMappingTree").jqxTree('getSelectedItem');
                    if (selectedItem != null) {
                        $($this).parents("td").find("input").val(selectedItem['value']);
                        $($this).parents("td").find("input").attr("title", selectedItem['value']);
//                       $($this).parent.find("input").val(selectedItem['value']); 
                        trfmRulesChanged = true;
                    }

                    $(this).html("");
                    $(this).dialog("close");
                    $(this).dialog("destroy");
                }

            }],
        open: function () {
            $('#columnMappingTree').jqxTree({source: records, width: 300,
                toggleMode: 'click', theme: 'energyblue',
                enableHover: true,
                incrementalSearch: true,
                keyboardNavigation: true
            });
            $("#treeSearchValue").keyup(function (event) {
                if (event.keyCode === 13) {
                    // Cancel the default action, if needed
//            event.preventDefault();
                    $("#treeNodeSearchIconId").click();
                }
            });
            $('#columnMappingTree').jqxTree('expandItem', $("#columnMappingTree").find('li:first')[0]);
            $("#columnMappingTree li").on('dblclick', function (event) {
                var selectedItem = $("#columnMappingTree").jqxTree('getSelectedItem');
                if (selectedItem != null && !(selectedItem['icon'] != null && selectedItem['icon'] != '')) {
                    $($this).parents("td").find("input").val(selectedItem['value']);
                    $($this).parents("td").find("input").attr("title", selectedItem['value']);
                    $("#columnMappingDialog").dialog("close");
                    $("#columnMappingDialog").dialog("destroy");
                    trfmRulesChanged = true;
                }

            });
            var inputFieldsArray = [];
            var columnIndex = treeIconClickEvent.target.parentElement.cellIndex;
            var trs = treeIconClickEvent.target.parentElement.offsetParent.children[1].children;
            ;
            $.each(trs, function (index) {
                var inputVal = this.children[columnIndex].children[0].value.trim()
                inputFieldsArray.push(inputVal);
            })

            var columnsArray = $(this).find('li');
            $.each(columnsArray, function (index) {
                var _this = this;
                var colVal = this.id.trim();
                $.each(inputFieldsArray, function (index) {
                    var inputVal = this.trim();
                    if (colVal == inputVal) {
                        if ($(_this).find(".tickMark").length == 0) {
                            $(_this.children[0]).prepend("<div class='tickMark'></div>");
                        }
                    }
                })
            });
            $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
            $(".visionHeaderMain").css("z-index", "999");
            $(".visionFooterMain").css("z-index", "999");
            $(".ui-dialog").addClass('visionDMTreePopup');
        },
        beforeClose: function (event, ui)
        {

            $(".visionHeaderMain").css("z-index", "99999");
            $(".visionFooterMain").css("z-index", "99999");
        }, close: function (event, ui)
        {

            $(this).html("");
            try {
                $("#columnMappingTree").jqxTree('destroy');
                $("#columnMappingTree").remove();
                $("#treeSearchInputDiv").remove();
            } catch (e) {
            }

        }

    });
}





function cubeClickDrilldown(tabId, chartId, chartVal, clickedValuesObj, n)
{
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        cache: false,
        url: "analyticDropdown",
        data: {
            'tabComponentId': chartId,
            'tabId': tabId,
            'chartTypeId': chartVal
        },
        success: function (response) {
            if (response != null)
            {
                var result = JSON.parse(response);
                var dataObj = result['chartList'];
                for (var i = 0; i < dataObj.length; i++)
                {
                    $("#wait").css("display", "block");
                    var chartData = dataObj[i];
                    if (chartData != null && !jQuery.isEmptyObject(chartData)) {
                        var analytic_comp_id = chartData[19];
                        var drag_comp_id = analytic_comp_id + "_Sub_Cube" + n;
                        chartData[19] = drag_comp_id;
                        var dargCompId = analytic_comp_id;
                        getshowSubCubeData(chartData, tabId, chartId, dargCompId, clickedValuesObj, n);
                    }
                }
            }
        },
        error: function (e) {
            sessionTimeout(e);
        }
    });

}

function getshowSubCubeData(chartData, tabId, analytic_Comp_Id, dargCompId, clickedValuesObj, n) {
    var axisColumns = $("#" + analytic_Comp_Id + "_axisColumns").val();
    var axisLabels = $("#" + analytic_Comp_Id + "_axisLabels").val();
    var checkBoxObjCheckedVal = $("#" + analytic_Comp_Id + "_checkBoxChecked").val();
    var chartOptAllObj = $("#" + analytic_Comp_Id + "_colorObj").val();//cube changes
    var factTable = $("#" + analytic_Comp_Id + "_factTable").val();//cube changes
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'JSON',
        cache: false,
        url: "getSubQubeAnalytics",
        data: {
            'chartData': chartData,
            'axisColumns': axisColumns,
            'axisLabels': axisLabels,
            'checkBoxObjCheckedVal': checkBoxObjCheckedVal,
            'cubeColors': chartOptAllObj,
            'cubeQuarterValue': JSON.stringify(clickedValuesObj),
            'factTable': factTable


        },
        success: function (response) {

            try {
                var result = response;
                if (result != null)
                {
                    $("#dropAreaId").empty();
                    $("#dropAreaId").remove();
//                    resetCubes('', analytic_Comp_Id);
                    var chartsObj = result['chartsObj'];
                    var divid = chartsObj['divId'];
                    var chartId = chartsObj['chartId'];
                    var sizeOfChart = chartsObj['sizeOfChart'];
                    var height = chartsObj['height'];
                    var width = chartsObj['width'];
                    var factTable = chartsObj['factTable'];
                    var cubeValueCol = chartsObj['cubeValueCol'];
                    var axisColumns = chartsObj['axisColumns'];
                    var axisLabels = chartsObj['axisLabels'];
                    var labelsArray = chartsObj['labelsObject'];
                    var title = chartsObj['title']; //multi cube 
                    var colorsObj = chartsObj['colorsObj'];
                    var factType = chartsObj['factType'];

                    var subCubeButtonDiv = "<div id='subCubeSettings' class='subCubeSettingClass'> "
                            + "<img src=\"images/Maximize_Icon.png\" class='subCubeMagnifyClass' id ='subCubeMagnifyId' onclick = 'subCubeMagnify(" + dargCompId + ")' title='Maximize'/>"
                            + "<img src=\"images/Minimize_Icon.png\" class='subCubeMagnifyClass' id ='subCubeNormalId' onclick = 'subCubeNormal(" + dargCompId + ")' title='Minimize'/>"
                            + "</div>";
                    if (magnifyFlag)
                    {
                        $("#" + dargCompId + "_Sub_Cube_Id").append(subCubeButtonDiv);
                    }
                    magnifyFlag = false;
                    $("#" + dargCompId + "_Sub_Cube_Id").append(divid);
                    $("#" + chartId + "_factTable").val(factTable);
                    $("#" + chartId + "_factType").val(factType);
                    $("#" + chartId + "_cubeValueCol").val(cubeValueCol);
                    $("#" + chartId + "_axisColumns").val(JSON.stringify(axisColumns));
                    $("#" + chartId + "_axisLabels").val(JSON.stringify(axisLabels));

                    $("#cubeChartId").val(chartId + "_Chart"); //multi cube
                    var dropChartId = chartId;
                    divArray.push(dropChartId);

                    if (sizeOfChart != null && sizeOfChart != '' && sizeOfChart != undefined) {
                        $("#" + chartId + "_Chart").css("width", "" + sizeOfChart + "%", "important");
                    } else if (windowWidth > 1024)
                    {
                        $("#" + chartId + "_Chart").css('width', '99%', 'important');
                    } else if (windowWidth < 1024 && windowWidth > 700)
                    {
                        $("#" + chartId + "_Chart").css('width', '49%');
                    } else if (windowWidth < 699 && windowWidth > 320)
                    {
                        $("#" + chartId + "_Chart").css('width', '98%');
                    }
                    $("#" + chartId + "_Chart").css('height', '35%', 'important');
                    $("#" + chartId).append("<div id ='" + chartId + "_cubeContainer' class='VisionShowSubCubeChart'></div>");
                    $("#" + chartId + "_cube_types").val(title); //multi cube
                    $("#" + chartId + "_cubeContainer").html("<div id='" + dropChartId + "_buttons' class='visionCubeDragButtons' style='float: left;'>"
                            + "<button id='cubeReport' class='cubedeleteclass' onclick='showSubReport(" + chartId + ")'> Report</button>"
                            + "<button id='cubeReset' class='cubedeleteclass' onclick='subcubeReset(" + chartId + ")'> Reset</button>"
                            + "<button id='cubeDelete' class ='cubedeleteclass' onclick='cubeDelete(" + chartId + "," + dargCompId + ")'>Delete</button></div>");

                    var noOfCubes = 3;
                    var cubeSize = 100;
                    var scene = new THREE.Scene();
                    var camera = new THREE.PerspectiveCamera(45, 2.5, 1, 10000);
                    var renderer = new THREE.WebGLRenderer({antialias: true});
                    renderer.setSize(parseInt(width) - 600, parseInt(height) - 400);
                    scene.background = new THREE.Color(0xFFFFFF);
//                     scene.position.set(300, 300, -300);

                    document.querySelector("#" + chartId + "_cubeContainer").appendChild(renderer.domElement);
                    camera.position.set(1800, 1000, 600);
                    var raycaster = new THREE.Raycaster();
                    var mouse = new THREE.Vector2();
                    const axesHelper = new THREE.AxesHelper((noOfCubes + 5) * 100);
//                    scene.add(axesHelper);
                    renderer.setPixelRatio(window.devicePixelRatio);
                    renderer.setSize(parseInt(width) - 600, parseInt(height) - 400);
                    var labelRenderer = new CSS2DRenderer();
                    labelRenderer.setSize(parseInt(width) - 600, parseInt(height) - 400);
                    labelRenderer.domElement.style.position = 'absolute';
                    labelRenderer.domElement.style.top = '0px';
                    document.querySelector('#' + chartId + "_cubeContainer").appendChild(labelRenderer.domElement);
                    var domEvents = new THREEx.DomEvents(camera, labelRenderer.domElement);
                    var axisArray = [];
                    var xlen = labelsArray['x'].length;
                    var ylen = labelsArray['y'].length;
                    var zlen = labelsArray['z'].length;
                    var xIndex = 1;
                    for (var j = -xlen / 2; j < xlen / 2; j++) {
                        var sign = j / Math.abs(j);
                        var elem = document.createElement('div');
                        elem.className = 'label';
                        elem.style.marginTop = '-1em';
                        var Label = new CSS2DObject(elem);
                        var xpoints = [];
                        if (j != (xlen / 2 - 1)) {
                            xpoints.push(new THREE.Vector3((j - 0.5) * cubeSize, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
                            xpoints.push(new THREE.Vector3((j + 0.5) * cubeSize, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
                        } else {

                            xpoints.push(new THREE.Vector3((j - 0.5) * cubeSize, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
                            xpoints.push(new THREE.Vector3((j + 0.5) * cubeSize * 2, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
//                            xpoints.push(new THREE.Vector3((j) * cubeSize, (j/Math.abs(j))* (ylen/2)* cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
//                            xpoints.push(new THREE.Vector3((j * cubeSize) + 1.5 * cubeSize, (j/Math.abs(j))* (ylen/2)* cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
                        }
                        var xmaterial = new THREE.LineBasicMaterial({color: 0x0071C5});
                        var xgeometry = new THREE.BufferGeometry().setFromPoints(xpoints);
                        var xaxis = new THREE.Line(xgeometry, xmaterial);
                        scene.add(xaxis);
                        Label.position.set((j + 0.5) * cubeSize, -(ylen / 2 + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize);
                        elem.textContent = labelsArray['x'][xIndex - 1][0];
                        elem.id = chartId + "_x_" + labelsArray['x'][xIndex - 1][1];
                        xaxis.add(Label);
                        axisArray.push(xaxis)
                        xIndex++;
                    }


                    var yIndex = 1;
                    for (var j = -(ylen / 2); j < ylen / 2; j++) {

                        var elem = document.createElement('div');
                        elem.className = 'label';
                        elem.style.marginTop = '-1em';
                        var Label = new CSS2DObject(elem);
                        var ypoints = [];
                        if (j != (ylen / 2 - 1)) {

                            ypoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize, (j - 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
                            ypoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize, (j + 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
//                            ypoints.push(new THREE.Vector3((j/Math.abs(j))* (xlen/2)* cubeSize, (j - 1) * cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
//                            ypoints.push(new THREE.Vector3((j/Math.abs(j))* (xlen/2)* cubeSize, (j * cubeSize), (j/Math.abs(j))* (zlen/2)* cubeSize));
                        } else {

                            ypoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize, (j - 0.5) * cubeSize, (zlen / 2 + 0.5) * cubeSize));
                            ypoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize, (j + 0.5) * cubeSize * 1.5, (zlen / 2 + 0.5) * cubeSize));
//                            ypoints.push(new THREE.Vector3((j/Math.abs(j))* (xlen/2)* cubeSize, (j - 1) * cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
//                            ypoints.push(new THREE.Vector3((j/Math.abs(j))* (xlen/2)* cubeSize, (j * cubeSize) + 1 * cubeSize, (j/Math.abs(j))* (zlen/2)* cubeSize));
                        }

                        var ymaterial = new THREE.LineBasicMaterial({color: 0x0071C5});
                        var ygeometry = new THREE.BufferGeometry().setFromPoints(ypoints);
                        var yaxis = new THREE.Line(ygeometry, ymaterial);
                        scene.add(yaxis);
                        Label.position.set(-(xlen / 2 + 0.5) * cubeSize, (j + 0.5) * cubeSize - 20, (zlen / 2 + 0.5) * cubeSize);
                        elem.textContent = labelsArray['y'][yIndex - 1][0];
                        elem.id = chartId + "_y_" + labelsArray['y'][yIndex - 1][1];
                        yaxis.add(Label);
                        axisArray.push(yaxis)
                        yIndex++;
                    }


                    var zIndex = 1;
                    for (var j = -(zlen / 2); j < zlen / 2; j++) {
                        var noOfCubes = labelsArray['x'].length;
                        var elem = document.createElement('div');
                        elem.className = 'label';
                        elem.style.marginTop = '-1em';
                        var Label = new CSS2DObject(elem);
                        var zpoints = [];
                        if (j != (zlen / 2 - 1)) {

                            zpoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j - 0.5) * cubeSize));
                            zpoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j + 0.5) * cubeSize));
//                            zpoints.push(new THREE.Vector3((noOfCubes * cubeSize + cubeSize), (j/Math.abs(j))* (ylen/2)* cubeSize, -(j - 1) * cubeSize));
//                            zpoints.push(new THREE.Vector3((noOfCubes * cubeSize + cubeSize), (j/Math.abs(j))* (ylen/2)* cubeSize, -(j * cubeSize)));
                        } else {

                            zpoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j - 0.5) * cubeSize));
                            zpoints.push(new THREE.Vector3(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j + 0.5) * cubeSize * 1.5));
//                            zpoints.push(new THREE.Vector3((noOfCubes * cubeSize + cubeSize), (j/Math.abs(j))* (ylen/2)* cubeSize, -(j - 1) * cubeSize));
//                            zpoints.push(new THREE.Vector3((noOfCubes * cubeSize + cubeSize), (j/Math.abs(j))* (ylen/2)* cubeSize, -((j * cubeSize) + 1 * cubeSize)));
                        }

                        var zmaterial = new THREE.LineBasicMaterial({color: 0x0071C5});
                        var zgeometry = new THREE.BufferGeometry().setFromPoints(zpoints);
                        var zaxis = new THREE.Line(zgeometry, zmaterial);
                        scene.add(zaxis);
                        Label.position.set(-(xlen / 2 + 0.5) * cubeSize + noOfCubes * cubeSize + cubeSize, -(ylen / 2 + 0.5) * cubeSize, -(j + 0.5) * cubeSize);
//                        Label.position.set(noOfCubes * cubeSize + cubeSize, (j/Math.abs(j))* (ylen/2)* cubeSize, -j * cubeSize);
                        elem.textContent = labelsArray['z'][zIndex - 1][0];
                        elem.id = chartId + "_z_" + labelsArray['z'][zIndex - 1][1];
                        zaxis.add(Label);
                        axisArray.push(zaxis)
                        zIndex++;
                    }


                    var geometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                    // material
                    var material = new THREE.MeshBasicMaterial({
                        color: 0xffffff,
                        vertexColors: true,
                        opacity: 0.5,
                        transparent: true
                    });
                    if (colorsObj != null && !jQuery.isEmptyObject(colorsObj)) //cube mdrm
                    {
                        red = new THREE.Color(colorsObj['x']);
                        green = new THREE.Color(colorsObj['y']);
                        blue = new THREE.Color(colorsObj['z']);
                    }
                    var colors = [red, green, blue];
                    for (var i = 0; i < 3; i++) {
                        geometry.faces[4 * i].color = colors[i];
                        geometry.faces[4 * i + 1].color = colors[i];
                        geometry.faces[4 * i + 2].color = colors[i];
                        geometry.faces[4 * i + 3].color = colors[i];
                    }

                    var relativeX = 0;
                    var relativeY = 0;
                    var relativeZ = 0;
                    for (var i = -(xlen / 2); i < xlen / 2; i++) {

                        relativeX = relativeX + cubeSize;
                        relativeY = 0;
                        relativeZ = 0;
                        for (var j = -(ylen / 2); j < ylen / 2; j++) {
                            relativeY = relativeY + cubeSize;
                            relativeZ = 0;
                            for (var k = -(zlen / 2); k < zlen / 2; k++) {
                                relativeZ = relativeZ - cubeSize;
                                var cube = new THREE.Mesh(geometry, material);
                                scene.add(cube);
                                var edges = new THREE.EdgesGeometry(geometry);
                                line = new THREE.LineSegments(edges, new THREE.LineBasicMaterial({color: 0x000000, opacity: 0.5, transparent: true}));
                                cube.add(line);
                                cube.position.set((i + 0.5) * cubeSize, (j + 0.5) * cubeSize, -(k + 0.5) * cubeSize);
                                var relativePosition = {};
                                relativePosition.x = relativeX;
                                relativePosition.y = relativeY;
                                relativePosition.z = relativeZ;
                                cube.userData['relativePosition'] = relativePosition;
                                // CLICK EVENT----->>>>>

                                domEvents.addEventListener(cube, 'click', function (event) {


                                    console.log("clicked");
                                    event.origDomEvent.preventDefault();
                                    cubeClickFlag = true;
                                    material.opacity = 0.5;
                                    globalMaterial[chartId] = material;
                                    var originalcubePosition = event.target.position;
                                    var cubePosition = event.target.userData['relativePosition'];


                                    var xindex = Math.abs((cubePosition.x) / cubeSize);
                                    var xlabel = labelsArray['x'][xindex - 1][0];
                                    var xlabelId = labelsArray['x'][xindex - 1][1];
                                    var yindex = Math.abs((cubePosition.y) / cubeSize);
                                    var ylabel = labelsArray['y'][yindex - 1][0];
                                    var ylabelId = labelsArray['y'][yindex - 1][1];
                                    var zindex = Math.abs((cubePosition.z) / cubeSize);
                                    var zlabel = labelsArray['z'][zindex - 1][0];
                                    var zlabelId = labelsArray['z'][zindex - 1][1];
                                    alert("X::" + xlabel + "  Y::" + ylabel + "  Z::" + zlabel);
                                    var clickedLabels = {}
                                    clickedLabels['x'] = xlabel;
                                    clickedLabels['y'] = ylabel;
                                    clickedLabels['z'] = zlabel;

                                    var clickedLabelIds = {}
                                    clickedLabelIds['x'] = xlabelId;
                                    clickedLabelIds['y'] = ylabelId;
                                    clickedLabelIds['z'] = zlabelId;

                                    var firstSelectedCubeLebel = {};

                                    if (event.origDomEvent.shiftKey) {

                                        var multiSelectCubeLabels = multiSelectCubeLabelsObj[chartId];
                                        if (multiSelectCubeLabels == null || jQuery.isEmptyObject(multiSelectCubeLabels)) {
                                            multiSelectCubeLabels = {};
                                        }

                                        var selectedCubesArray = selectedCubesArrayObj[chartId];
                                        if (selectedCubesArray == null || selectedCubesArray.length == 0) {
                                            selectedCubesArray = [];
                                        }

                                        if (multiSelectCubeLabels != null && Object.keys(multiSelectCubeLabels).length == 1) {
                                            firstSelectedCubeLebel = multiSelectCubeLabels
                                            var matchCount = 0;
                                            var values = Object.values(multiSelectCubeLabels)[0];
                                            var staticAxi = [];
                                            var sliceAxis;
                                            if (values['x'] == clickedLabelIds['x']) {
                                                matchCount++;
                                                staticAxi.push('x');
                                            } else {
                                                sliceAxis = 'x';
                                            }
                                            if (values['y'] == clickedLabelIds['y']) {
                                                matchCount++;
                                                staticAxi.push('y');
                                            } else {
                                                sliceAxis = 'y';
                                            }
                                            if (values['z'] == clickedLabelIds['z']) {
                                                matchCount++;
                                                staticAxi.push('z');
                                            } else {
                                                sliceAxis = 'z';
                                            }
                                            if (matchCount == 5) {

                                                $(".label").removeClass("highLightLabel");
                                                $.each(selectedCubesArray, function (index) {

                                                    scene.remove(selectedCubesArray[index]);
                                                    $(".cubeLabel").remove();
                                                    $(".cubeValueLabel").remove();
                                                })


                                                labelsArray[sliceAxis];
                                                var selectCubeLabels = {};
                                                var addOrder;
                                                var sliceAxisPos;
                                                var newCubePosition = {};
                                                var cubeReached = false;
                                                for (var i = 0; i < labelsArray[sliceAxis].length; i++) {

                                                    if (!cubeReached) {
                                                        if (i == 0) {
                                                            var yellowCube = selectedCubesArray[0];
                                                            newCubePosition = yellowCube.position;
                                                            sliceAxisPos = newCubePosition[sliceAxis];
                                                            if (Math.abs(cubePosition[sliceAxis]) - Math.abs(newCubePosition[sliceAxis]) >= 0) {
                                                                addOrder = "plus";
                                                            } else {
                                                                addOrder = "minus";
                                                            }

                                                        } else {
                                                            if (addOrder == "plus") {
                                                                newCubePosition[sliceAxis] = sliceAxisPos + (sliceAxisPos / Math.abs(sliceAxisPos) * i * cubeSize);
                                                                if (newCubePosition[sliceAxis] == event.target.position[sliceAxis]) {
                                                                    cubeReached = true;
                                                                }
                                                            } else if (addOrder == "minus") {

                                                                newCubePosition[sliceAxis] = sliceAxisPos - (sliceAxisPos / Math.abs(sliceAxisPos) * i * cubeSize);
                                                                if (newCubePosition[sliceAxis] == event.target.position[sliceAxis]) {
                                                                    cubeReached = true;
                                                                }
                                                            }

                                                        }
                                                        if (sliceAxis == 'z') {
                                                            var sliceAxisindex = Math.abs((newCubePosition[sliceAxis]) / cubeSize);
                                                        } else {
                                                            var sliceAxisindex = Math.abs((newCubePosition[sliceAxis]) / cubeSize);
                                                        }

                                                        var sliceAxislabel = labelsArray[sliceAxis][sliceAxisindex - 1][0];
                                                        var sliceAxislabelId = labelsArray[sliceAxis][sliceAxisindex - 1][1];

                                                        var staticAxis1 = staticAxi[0];
                                                        if (staticAxis1 == 'z') {
                                                            var staticAxis1index = Math.abs((newCubePosition[staticAxis1]) / cubeSize);
                                                        } else {
                                                            var staticAxis1index = Math.abs((newCubePosition[staticAxis1]) / cubeSize);
                                                        }

                                                        var staticAxis1label = labelsArray[staticAxis1][staticAxis1index - 1][0];
                                                        var staticAxis1labelId = labelsArray[staticAxis1][staticAxis1index - 1][1];

                                                        var staticAxis2 = staticAxi[1];
                                                        if (staticAxis2 == 'z') {
                                                            var staticAxis2index = Math.abs((newCubePosition[staticAxis2]) / cubeSize);
                                                        } else {
                                                            var staticAxis2index = Math.abs((newCubePosition[staticAxis2]) / cubeSize);
                                                        }

                                                        var staticAxis2label = labelsArray[staticAxis2][staticAxis2index - 1][0];
                                                        var staticAxis2labelId = labelsArray[staticAxis2][staticAxis2index - 1][1];

                                                        var labels = {}
                                                        labels[sliceAxis] = sliceAxislabel;
                                                        labels[staticAxis1] = staticAxis1label;
                                                        labels[staticAxis2] = staticAxis2label;

                                                        var labelIds = {}
                                                        labelIds[sliceAxis] = sliceAxislabelId;
                                                        labelIds[staticAxis1] = staticAxis1labelId;
                                                        labelIds[staticAxis2] = staticAxis2labelId;

//                                                        selectCubeLabels[sliceAxislabelId + staticAxis1labelId + staticAxis2labelId] = labelIds;
                                                        selectCubeLabels[newCubePosition.x + "," + newCubePosition.y + "," + newCubePosition.z] = labelIds;
//                                                       selectCubeLabels[newCubePosition[sliceAxis] +","+ newCubePosition[staticAxis1] +","+ newCubePosition[staticAxis2]] = labelIds;

                                                        var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                                                        var Nmaterial = new THREE.MeshBasicMaterial({
                                                            color: "#FFFF00",
                                                            vertexColors: true,
                                                            opacity: 0.6,
                                                            transparent: true
                                                        });

                                                        var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
                                                        ncube.position.set(newCubePosition.x - ((xlen / 2 * cubeSize) + cubeSize / 2), newCubePosition.y - ((ylen / 2 * cubeSize) + cubeSize / 2), newCubePosition.z + ((zlen / 2 * cubeSize) + cubeSize / 2));
                                                        scene.add(ncube);
                                                        selectedCubesArray.push(ncube);

                                                    }
                                                }

                                                $.each(selectCubeLabels, function (indx) {
                                                    var selectedLableIds = this;
                                                    $("#" + chartId + "_" + sliceAxis + "_" + selectedLableIds[sliceAxis]).addClass("highLightLabel");
                                                })

                                                selectCubeLabels['lineSlice'] = 'Y';
                                                multiSelectCubeLabelsObj[chartId] = selectCubeLabels;
                                                selectedCubesArrayObj[chartId] = selectedCubesArray;



                                            } else {
                                                var selectedCubes = multiSelectCubeLabels;
                                                var pos = cubePosition;
                                                var prevCubePosArr = Object.keys(multiSelectCubeLabels)[0].split(",");
                                                var prevCubePos = {};
                                                prevCubePos['x'] = parseInt(prevCubePosArr[0]);
                                                prevCubePos['y'] = parseInt(prevCubePosArr[1]);
                                                prevCubePos['z'] = parseInt(prevCubePosArr[2]);

                                                resetCubes("", chartId);


                                                var zDiff = cubePosition.z / 100 - prevCubePos['z'] / 100;
                                                var yDiff = cubePosition.y / 100 - prevCubePos['y'] / 100;
                                                var xDiff = cubePosition.x / 100 - prevCubePos['x'] / 100;
                                                var zeroDiffCount = 0;
                                                if (zDiff == 0) {
                                                    zeroDiffCount++;
                                                }
                                                if (yDiff == 0) {
                                                    zeroDiffCount++;
                                                }
                                                if (xDiff == 0) {
                                                    zeroDiffCount++;
                                                }
                                                if (zeroDiffCount == 2) {
                                                    multiSelectCubeLabels['lineSlice'] = 'Y';
                                                }
                                                var colorCode = "";
                                                if (zeroDiffCount == 0)
                                                {
                                                    colorCode = $("#BAR_CHART_CUBE_DICE_AXIS_COLORS").val();
                                                } else if (zeroDiffCount == 1)
                                                {
                                                    colorCode = $("#BAR_CHART_CUBE_SLICE_AXIS_COLORS").val();
                                                } else if (zeroDiffCount == 2)
                                                {
                                                    colorCode = $("#BAR_CHART_CUBE_SLICE_AXIS_COLORS").val();
                                                }
                                                for (var k = 0; k <= Math.abs(zDiff); k++) {
                                                    for (var j = 0; j <= Math.abs(yDiff); j++) {

                                                        for (var i = 0; i <= Math.abs(xDiff); i++) {


                                                            var currentCubePos = {};

                                                            currentCubePos['x'] = prevCubePos.x + ((xDiff != 0) ? (xDiff / Math.abs(xDiff) * i * 100) : 0);
                                                            currentCubePos['y'] = prevCubePos.y + ((yDiff != 0) ? (yDiff / Math.abs(yDiff) * j * 100) : 0);
                                                            currentCubePos['z'] = prevCubePos.z + ((zDiff != 0) ? (zDiff / Math.abs(zDiff) * k * 100) : 0);

                                                            var xindex = Math.abs(currentCubePos.x / cubeSize);
                                                            var xlabel = labelsArray['x'][xindex - 1][0];
                                                            var xlabelId = labelsArray['x'][xindex - 1][1];

                                                            var yindex = Math.abs(currentCubePos.y / cubeSize);
                                                            var ylabel = labelsArray['y'][yindex - 1][0];
                                                            var ylabelId = labelsArray['y'][yindex - 1][1];

                                                            var zindex = Math.abs(currentCubePos.z / cubeSize);
                                                            var zlabel = labelsArray['z'][zindex - 1][0];
                                                            var zlabelId = labelsArray['z'][zindex - 1][1];

                                                            alert("X::" + xlabel + "  Y::" + ylabel + "  Z::" + zlabel);
                                                            var clickedLabels = {}
                                                            clickedLabels['x'] = xlabel;
                                                            clickedLabels['y'] = ylabel;
                                                            clickedLabels['z'] = zlabel;
                                                            var clickedLabelIds = {}
                                                            clickedLabelIds['x'] = xlabelId;
                                                            clickedLabelIds['y'] = ylabelId;
                                                            clickedLabelIds['z'] = zlabelId;

//                                                          multiSelectCubeLabels[xlabelId + ylabelId + zlabelId] = clickedLabelIds;
                                                            multiSelectCubeLabels[currentCubePos.x + "," + currentCubePos.y + "," + currentCubePos.z] = clickedLabelIds;
                                                            multiSelectCubeLabels['Dimension'] = '3D';

                                                            $("#" + chartId + "_x_" + clickedLabelIds['x']).addClass("highLightLabel");
                                                            $("#" + chartId + "_y_" + clickedLabelIds['y']).addClass("highLightLabel");
                                                            $("#" + chartId + "_z_" + clickedLabelIds['z']).addClass("highLightLabel");

                                                            material.opacity = 0.1;
                                                            globalMaterial[chartId] = material;
                                                            var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                                                            var Nmaterial = new THREE.MeshBasicMaterial({
                                                                color: colorCode,
                                                                vertexColors: true,
                                                                opacity: 0.6,
                                                                transparent: true
                                                            });

                                                            var ncube = new THREE.Mesh(Ngeometry, Nmaterial);

                                                            ncube.position.set(currentCubePos.x - ((xlen / 2 * cubeSize) + cubeSize / 2), currentCubePos.y - ((ylen / 2 * cubeSize) + cubeSize / 2), currentCubePos.z + ((zlen / 2 * cubeSize) + cubeSize / 2));
                                                            scene.add(ncube);
                                                            selectedCubesArray.push(ncube);
                                                        }
                                                    }
                                                }
                                                multiSelectCubeLabelsObj[chartId] = multiSelectCubeLabels;

                                                selectedCubesArrayObj[chartId] = selectedCubesArray;
                                                stopLoader();
                                            }



                                        } else {
                                            resetCubes("", chartId);
                                            var multiSelectCubeLabels = multiSelectCubeLabelsObj[chartId];
                                            if (multiSelectCubeLabels == null || jQuery.isEmptyObject(multiSelectCubeLabels)) {
                                                multiSelectCubeLabels = {};
                                            }

                                            var selectedCubesArray = selectedCubesArrayObj[chartId];
                                            if (selectedCubesArray == null || selectedCubesArray.length == 0) {
                                                selectedCubesArray = [];
                                            }

                                            multiSelectCubeLabels = {};
                                            selectedCubesArray = [];
//                                            multiSelectCubeLabels[xlabelId + ylabelId + zlabelId] = clickedLabelIds;
                                            multiSelectCubeLabels[cubePosition.x + "," + cubePosition.y + "," + cubePosition.z] = clickedLabelIds;

                                            var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                                            var Nmaterial = new THREE.MeshBasicMaterial({
                                                color: $("#BAR_CHART_CUBE_CLICK_AXIS_COLORS").val(),
                                                vertexColors: true,
                                                opacity: 0.6,
                                                transparent: true
                                            });


                                            var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
                                            ncube.position.set(cubePosition.x - ((xlen / 2 * cubeSize) + cubeSize / 2), cubePosition.y - ((ylen / 2 * cubeSize) + cubeSize / 2), cubePosition.z + ((zlen / 2 * cubeSize) + cubeSize / 2));
                                            ncube.position.setscene.add(ncube);
                                            multiSelectCubeLabelsObj[chartId] = multiSelectCubeLabels;

                                            selectedCubesArray.push(ncube);
                                            selectedCubesArrayObj[chartId] = selectedCubesArray;

                                        }

                                    } else {//cube mdrm
                                        resetCubes("", chartId);
                                        var multiSelectCubeLabels = multiSelectCubeLabelsObj[chartId];
                                        if (multiSelectCubeLabels == null || jQuery.isEmptyObject(multiSelectCubeLabels)) {
                                            multiSelectCubeLabels = {};
                                        }

                                        var selectedCubesArray = selectedCubesArrayObj[chartId];
                                        if (selectedCubesArray == null || selectedCubesArray.length == 0) {
                                            selectedCubesArray = [];
                                        }


                                        multiSelectCubeLabels = {};
//                                        multiSelectCubeLabels[xlabelId + ylabelId + zlabelId] = clickedLabelIds;
                                        multiSelectCubeLabels[cubePosition.x + "," + cubePosition.y + "," + cubePosition.z] = clickedLabelIds;

                                        var Ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
                                        var Nmaterial = new THREE.MeshBasicMaterial({
                                            color: $("#BAR_CHART_CUBE_CLICK_AXIS_COLORS").val(),
                                            vertexColors: true,
                                            opacity: 0.6,
                                            transparent: true
                                        });


                                        var ncube = new THREE.Mesh(Ngeometry, Nmaterial);
                                        ncube.position.set(cubePosition.x - ((xlen / 2 * cubeSize) + cubeSize / 2), cubePosition.y - ((ylen / 2 * cubeSize) + cubeSize / 2), cubePosition.z + ((zlen / 2 * cubeSize) + cubeSize / 2));

                                        scene.add(ncube);
                                        multiSelectCubeLabelsObj[chartId] = multiSelectCubeLabels;

                                        selectedCubesArray.push(ncube);
                                        selectedCubesArrayObj[chartId] = selectedCubesArray;
                                    }

// for slice or Dice

                                    var lineSlice = multiSelectCubeLabelsObj[chartId]['lineSlice'];


                                    var lineSliceWhrClause = "";
                                    var xWhereClause = "";
                                    var yWhereClause = "";
                                    var zWhereClause = "";

                                    $.each(multiSelectCubeLabelsObj[chartId], function (key, val) {
                                        if (key != "Dimension" && key != "lineSlice") {
                                            if (xWhereClause.indexOf("'" + this['x'] + "',") == -1) {
                                                xWhereClause += "'" + this['x'] + "',";
                                            }
                                            if (yWhereClause.indexOf("'" + this['y'] + "',") == -1) {
                                                yWhereClause += "'" + this['y'] + "',";
                                            }
                                            if (zWhereClause.indexOf("'" + this['z'] + "',") == -1) {
                                                zWhereClause += "'" + this['z'] + "',";
                                            }
                                        }
                                    })

//cube changes
                                    lineSliceWhrClause = axisColumns['x']['column'] + " IN (" + xWhereClause.slice(0, -1) + ") AND " + axisColumns['y']['column'] + " IN (" + yWhereClause.slice(0, -1) + ") AND " + axisColumns['z']['column'] + " IN (" + zWhereClause.slice(0, -1) + ")";



                                    if (multiSelectCubeLabelsObj != null && ((Object.keys(multiSelectCubeLabelsObj[chartId]).length > 0 && multiSelectCubeLabelsObj[chartId]['Dimension'] != '3D' && multiSelectCubeLabelsObj[chartId]['Dimension'] != '2D') ||
                                            (lineSlice == "Y"))) {

                                        $.ajax({
                                            type: "post",
                                            traditional: true,
                                            dataType: 'JSON',
                                            cache: false,
                                            url: "getCubeValue",
                                            data: {
                                                factTable: factTable,
                                                cubeValueCol: cubeValueCol,
                                                clickedLabels: JSON.stringify(clickedLabelIds),
                                                axisColumns: JSON.stringify(axisColumns),
                                                lineSlice: lineSlice,
                                                lineSliceWhrClause: lineSliceWhrClause,
                                                chartId: chartId,
                                                factType: factType
                                            },
                                            success: function (response) {

                                                var cubeValue = response['cubeValue']
                                                var elem = document.createElement('div');
                                                elem.className = 'cubeValueLabel';

                                                var Label = new CSS2DObject(elem);


                                                elem.innerHTML = cubeValue;
                                                elem.id = chartId + "_CUBEVAL_" + cubeValue;
                                                ncube.add(Label);



                                                $("#" + chartId + "_x_" + clickedLabelIds['x']).addClass("highLightLabel");
                                                $("#" + chartId + "_y_" + clickedLabelIds['y']).addClass("highLightLabel");
                                                $("#" + chartId + "_z_" + clickedLabelIds['z']).addClass("highLightLabel");
                                                stopLoader();
                                            },
                                            error: function (e) {
                                                stopLoader();
                                                sessionTimeout(e);
                                            }
                                        });

                                    }
                                    globalSceneObj[chartId] = scene;
                                });


                                domEvents.addEventListener(cube, 'dblclick', function (event) {
                                    console.log("dbl clicked");
                                    event.origDomEvent.preventDefault();
                                    var cubePosition = event.target.position;
                                    var xindex = Math.abs(cubePosition.x / cubeSize);
                                    var xlabel = labelsArray['x'][xindex - 1][0];
                                    var xlabelId = labelsArray['x'][xindex - 1][1];
                                    var yindex = Math.abs(cubePosition.y / cubeSize);
                                    var ylabel = labelsArray['y'][yindex - 1][0];
                                    var ylabelId = labelsArray['y'][yindex - 1][1];
                                    var zindex = Math.abs(cubePosition.z / cubeSize);
                                    var zlabel = labelsArray['z'][zindex - 1][0];
                                    var zlabelId = labelsArray['z'][zindex - 1][1];
                                    alert("X::" + xlabel + "  Y::" + ylabel + "  Z::" + zlabel);
                                    var clickedLabels = {};
                                    clickedLabels['x'] = xlabel;
                                    clickedLabels['y'] = ylabel;
                                    clickedLabels['z'] = zlabel;
                                    var clickedLabelIds = {}
                                    clickedLabelIds['x'] = xlabelId;
                                    clickedLabelIds['y'] = ylabelId;
                                    clickedLabelIds['z'] = zlabelId;
                                    var multiSelectCubeLabels = {};
                                    multiSelectCubeLabels[xlabelId + ylabelId + zlabelId] = clickedLabelIds;

                                    multiSelectCubeLabelsObj[chartId] = multiSelectCubeLabels;
                                    showReport('', chartId);


                                    globalSceneObj[chartId] = scene;
                                });
                            }
                        }
                    }



                    var controls = new THREE.OrbitControls(camera, labelRenderer.domElement);
                    controls.update();
                    controls.target.set(0, 0, 0);
                    globalCubeRenderer[chartId] = renderer;
                    globalCubeLabelRenderer[chartId] = labelRenderer;
                    function subCuberender() {

//                        if (rotateFlagCW) {
//                            $("#" + chartId + "_cubeContainer").find("div").css("display", "none");
////                            1800*5, 1000*5, 600*5)
//                            camera.position.x -= 180 / 2;
//                            camera.position.y -= 100 / 2;
//                            camera.position.z -= 60 / 2;
//                            scene.rotation.x += 0.1;
//                            scene.rotation.y += 0.1;
//                            scene.rotation.z += 0.1;
//                        }

                        requestAnimationFrame(subCuberender);
                        renderer.render(scene, camera);
                        labelRenderer.render(scene, camera);

                    }
                    subCuberender();
//                    subCuberender(renderer, labelRenderer, scene, camera,chartId);
//                    rotateSubCube(scene, camera, chartId);
                    var qubeChartId = chartId + "_cubeContainer";
                    var pos = $("#" + chartId).css({marginLeft: 0});
//                    $("#" + qubeChartId).css({marginTop: -120}); 
                    var pos = $("#" + qubeChartId).find("canvas").position();
                    var pos1 = $("#" + qubeChartId).find("div:first").position();
                    $("#" + qubeChartId).find("div:nth-of-type(2)").css({top: pos.top, left: pos.left});
                    var pos2 = $("#" + qubeChartId).find("div:first").position();
//ravinder uncommented code for click purpose by jagadish //cube mdrm
//                    var pos = $("#" + qubeChartId).find("canvas").position();
//                    var pos1 = $("#" + qubeChartId).find("div:first").position();
//                    $("#" + qubeChartId).find("div:first").css({top: pos.top, left: pos.left});
//                    var pos2 = $("#" + qubeChartId).find("div:first").position();
//
//                    $("#" + tabId).scroll(function () {
//                        var pos = $("#" + qubeChartId).find("canvas").position();
//                        var pos1 = $("#" + qubeChartId).find("div:first").position();
//                        $("#" + qubeChartId).find("div:first").css({top: pos.top, left: pos.left});
//                        var pos2 = $("#" + qubeChartId).find("div:first").position();
//                    })
                }

                $("#wait").css("display", "none");
            } catch (e) {

            }
        },
        error: function (e) {
            sessionTimeout(e);
        }
    });
}
function rotateSubCube(scene, camera, chartId) {

    rotateFlagCW = true;
    setTimeout(function () {
        $("#" + chartId + "_cubeContainer").find("div").css("display", "block");
        rotateFlagCW = false;
        camera.position.set(1800 * 0.8, 1000 * 0.8, 600 * 0.8);
        scene.rotation.x = 0;
        scene.rotation.y = 0;
        scene.rotation.z = 0;
    }, 1400)

}

function cubeDelete(divId, subCubeId)
{
    var chartId = divId['id'];
    var cubeId = subCubeId['id'];
    $("#" + chartId + "_Chart").empty();
    $("#" + chartId + "_Chart").remove();
    divArray.splice($.inArray(chartId, divArray), 1);
    $.each(divArray, function (index, value)
    {
        var pos = $("#" + value).find("canvas").position();
        var convas = $("#" + value).find("canvas");
        var left = convas['0']['offsetLeft'];
        $("#" + value).find("div:first").css({top: pos.top, left: left});
        var pos2 = $("#" + value).find("div:first").position();
    });
    if ($("#" + cubeId + "_Sub_Cube_Id > div").length == 1)
    {
        $("#" + cubeId).show();
        $("#subCubeSettings").hide();
        magnifyFlag = true;
        $("#" + cubeId + "_Sub_Cube_Id").empty();
    }
}
function removeFirstCube()
{
    var chartId = divArray[0];
    $("#" + chartId + "_Chart").empty();
    $("#" + chartId + "_Chart").remove();
    divArray.splice($.inArray(chartId, divArray), 1);
    $.each(divArray, function (index, value)
    {
        var pos = $("#" + value).find("canvas").position();
        var convas = $("#" + value).find("canvas");
        var left = convas['0']['offsetLeft'];
        $("#" + value).find("div:first").css({top: pos.top, left: left});
        var pos2 = $("#" + value).find("div:first").position();
    });
}
function make_canvas(tabId, chartId, chartVal, clickedValuesObj, n) {
    var height = 315;
    var width = 300;
    var noOfCubes = 2;
    var cubeSize = 150;

    var arrayData1 = ['Umar.shaik', '1002'];
    var arrayData2 = ['100', '1003'];
    var arrayData3 = ['Q2-2021', '1006'];
    var finaldata = [];
    finaldata.push(arrayData1);
    var finaldata1 = [];
    finaldata1.push(arrayData2);
    var finaldata2 = [];
    finaldata2.push(arrayData3);


    var labelsArray = {};
    labelsArray['x'] = finaldata;
    labelsArray['y'] = finaldata1;
    labelsArray['z'] = finaldata2;
    var scene = new THREE.Scene();
    var camera = new THREE.PerspectiveCamera(45, 2.5, 1, 1000);
    var renderer = new THREE.WebGLRenderer({antialias: true});
    renderer.setSize(width, height);
    scene.background = new THREE.Color(0xFFFFFF);
    document.getElementById("cubeDropId").appendChild(renderer.domElement);
    camera.position.set(500, 300, 200);
    scene.add(new THREE.AmbientLight(0xffffff, 0.6));
    renderer.setPixelRatio(window.devicePixelRatio);
    renderer.setSize(parseInt(width), height);
    var labelRenderer = new CSS2DRenderer();
    labelRenderer.setSize(parseInt(width), height);
    labelRenderer.domElement.style.position = 'absolute';
    labelRenderer.domElement.style.top = '0px';
    document.getElementById("cubeDropId").appendChild(renderer.domElement);


    var colorsObj = {};
    colorsObj['x'] = '#40875e';
    colorsObj['y'] = '#a8a240';
    colorsObj['z'] = '#f0ab00';
    var ngeometry = new THREE.BoxGeometry(cubeSize, cubeSize, cubeSize);
    // material
    var nmaterial = new THREE.MeshBasicMaterial({
        color: 0xffffff,
        vertexColors: true,
        opacity: 0.5,
        transparent: true
    });
    if (colorsObj != null && !jQuery.isEmptyObject(colorsObj)) //cube mdrm
    {
        var red = new THREE.Color(colorsObj['x']);
        var green = new THREE.Color(colorsObj['y']);
        var blue = new THREE.Color(colorsObj['z']);
    }
    var colors = [red, green, blue];
    for (var i = 0; i < 3; i++) {
        ngeometry.faces[4 * i].color = colors[i];
        ngeometry.faces[4 * i + 1].color = colors[i];
        ngeometry.faces[4 * i + 2].color = colors[i];
        ngeometry.faces[4 * i + 3].color = colors[i];
    }

    var ncube = new THREE.Mesh(ngeometry, nmaterial);
    ncube.position.set(0, 0, 0);
    scene.add(ncube);



    var orbitControls = new THREE.OrbitControls(camera, renderer.domElement);
    function dropanimate()
    {
        requestAnimationFrame(dropanimate);
        renderer.render(scene, camera);
        labelRenderer.render(scene, camera);
    }

    dropanimate();
    $("#wait").css("display", "block");
    setTimeout(function () {
        cubeClickDrilldown(tabId, chartId, chartVal, clickedValuesObj, n)
    }, 1000);
}
function subcubeReset(divId)
{
    var chartId = divId['id'];
    resetCubes('', chartId);


//    var controls = globalCubeControls[chartId];
//    controls.dIn(4);
//    controls.update();

}

function showSubReport(divId)
{
    var chartId = divId['id'];
    showReport('', chartId);
}
function subCubeMagnify(divId)
{
    var chartId = divId['id'];
    $("#" + chartId).hide();
    $("#" + chartId + "_Sub_Cube_Id").addClass('visionSubCubeMagnifyClass');
    $(".subCubeSettingClass").css({marginLeft: 480});
    $.each(divArray, function (index, value)
    {
        $("#" + value + "_Chart").css("width", '75%', 'important');
        var renderer = globalCubeRenderer[value];
        var labelRenderer = globalCubeLabelRenderer[value];
        renderer.setSize(1400, 700);
        labelRenderer.setSize(1400, 700);
        var cubeContainer = value + "_cubeContainer";
        $("#" + cubeContainer).addClass('visionSubCubeNormalClass');
        $("#" + value + "_buttons").addClass('visionSubCubeMagnifyInnerButtons');

    });
}
function subCubeNormal(divId)
{
    var chartId = divId['id'];
    $("#" + chartId).show();
    $("#" + chartId + "_Sub_Cube_Id").removeClass('visionSubCubeMagnifyClass');
    $(".subCubeSettingClass").css({marginLeft: 445});
    $.each(divArray, function (index, value)
    {
        $("#" + value + "_Chart").css("width", '35%', 'important');
        var renderer = globalCubeRenderer[value];
        var labelRenderer = globalCubeLabelRenderer[value];
        renderer.setSize(800, 300);
        labelRenderer.setSize(800, 300);
        var cubeContainer = value + "_cubeContainer";
        $("#" + cubeContainer).removeClass('visionSubCubeNormalClass');
        $("#" + value + "_buttons").removeClass('visionSubCubeMagnifyInnerButtons');

    });
}











