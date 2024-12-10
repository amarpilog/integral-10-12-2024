/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

function refreshSurveyChart()
{
    $("#insightsSurveyAnalyticsFilterDataId").hide();
    $("#SurveyChildData").hide();
    var dataObj = {};
    dataObj['key'] = 'value';
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'getVotingAnalytics',
        cache: false,
        data: {
            dataObj: JSON.stringify(dataObj)
        },
        success: function (response) {
            stopLoader();
            var result = response;
            getVotingChart("insightsSurveyAnalyticsChartId", result);
        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
}
function getVotingChart(chartId, chartData)
{
    var chartDataArr = [];
    if (chartData != null && !jQuery.isEmptyObject(chartData))
    {
        var labels = chartData['labels'];
        chartData['type'] = 'pie';
        chartData['hoverinfo'] = "label+percent";
        chartData['textinfo'] = "label+percent";
        chartData['textposition'] = "inside";
        chartData['marker'] = {
            colors: generateRandomColourArray(labels)
        };
        chartDataArr.push(chartData);
    }
    var data = chartDataArr;
    var layout = {
        width: 330,
        height: 330,
        title: 'Party wise Votes',
        margin: {
            l: 50,
            b: 100,
            t: 50,
            r: 10
        },
        showlegend: false,
        legend: {"orientation": "h"}
    };
    var config = {
        responsive: true,
        displayModeBar: true,
        downloadImage: true,
        displaylogo: false,

        modeBarButtonsToRemove: ['zoom2d', 'pan', 'pan2d', 'zoomIn2d', 'zoomOut2d', 'resetViewMapbox', 'resetScale2d', 'sendDataToCloud', 'hoverClosestCartesian', 'autoScale2d', 'lasso2d', 'select2d', 'zoom2d']
    };

    Plotly.newPlot(chartId, data, layout, config);
    var myPlot = document.getElementById(chartId);
    myPlot.on('plotly_click', function (data) {
        var filterType = data;
        votingDeatilsBasedOnField(chartId, data, filterType);
    });
}
function generateRandomColourArray(labels) {
    var rgb = [];

    for (var i = 0; i < labels.length; i++) {
//     rgb.push(Math.floor(Math.random() * 255));
//     rgb.push('#'+ Math.floor(Math.random() * 19777215).toString(16));
        var colors = ['red', 'blue', 'grey', 'lightgreen',
            'violet', 'Tomato', 'DodgerBlue', 'MediumSeaGreen', 'SlateBlue', 'LightGray', 'purple'];
        if (labels[i] != null && labels[i] != '' && labels[i] != undefined && labels[i].indexOf("TRS") > -1) {
            rgb.push('pink');
        } else if (labels[i] != null && labels[i] != '' && labels[i] != undefined && labels[i].indexOf("Congress") > -1) {
            rgb.push('skyblue');
        } else if (labels[i] != null && labels[i] != '' && labels[i] != undefined && labels[i].indexOf("BJP") > -1) {
            rgb.push('orange');
        } else if (labels[i] != null && labels[i] != '' && labels[i] != undefined && labels[i].indexOf("BSP") > -1) {
            rgb.push('blue');
        } else if (labels[i] != null && labels[i] != '' && labels[i] != undefined && labels[i].indexOf("AIMIM") > -1) {
            rgb.push('lightgreen');
        } else if (labels[i] != null && labels[i] != '' && labels[i] != undefined && labels[i].indexOf("CPI") > -1) {
            rgb.push('red');
        } else if (labels[i] != null && labels[i] != '' && labels[i] != undefined && labels[i].indexOf("CPI(M)") > -1) {
            rgb.push('red');
        } else {
            rgb.push(colors[Math.floor(Math.random() * colors.length)]);
        }
    }
    return rgb;
}
function votingDeatilsBasedOnField(chartId, data, filterType)
{
    var filterTypes = {};
    filterTypes['DISTRICT'] = "District";
    filterTypes['AC'] = "Assembly Constituency";
    filterTypes['AREA'] = "Area";
    filterTypes['GENDER'] = "Gender";
    filterTypes['RELIGION'] = "Religion";
    filterTypes['CASTE'] = "Caste";
    filterTypes['OCCUPATION'] = "Occupation";
    var div = "";
    $.each(filterTypes, function (key, value) {
        if (key != null && key != '' && key != undefined && key != filterType) {
            div += "<li onclick=applyVotingFilterFunctions('" + key + "')>" + value + "</li>";
        } else if (!(key != null && key != '' && key != undefined)) {
            div += "<li onclick=applyVotingFilterFunctions('" + key + "')>" + value + "</li>";
        }
    });

    $("#insightsVotingAnalyticsChartFilters").remove();
    $('body').append("<div id='insightsVotingAnalyticsChartFilters'><ul></ul></div>");
    $("#insightsVotingAnalyticsChartFilters ul").html(div);
    var contextMenu = $("#insightsVotingAnalyticsChartFilters").jqxMenu({width: '90px', height: 140 + 'px', autoOpenPopup: false, mode: 'popup'});
    contextMenu.jqxMenu('open', parseInt(event.clientX) + 5, parseInt(event.clientY) + 5);
}
function surveyAnalticChartFilter()
{
    changeFiletrImg();
    $("#insightsSurveyAnalyticsFilterDataId").toggle();
    $("#SurveyChildData").hide();
}

function getChildData(parentId, appendId, liId)
{
    var position = $("#" + liId).position();
    var parent = '<ul class="surveyDropdown">'
            + '<li><a class="dropdown-item" onclick="getSubChart(\'' + parentId + '\',\'DISTRICT\')">District</a></li>'
            + '<li><a class="dropdown-item" onclick="getSubChart(\'' + parentId + '\',\'AC\')">Assembly</a></li>'
            + '<li><a class="dropdown-item" onclick="getSubChart(\'' + parentId + '\',\'AREA\')">Area</a></li>'
            + '<li><a class="dropdown-item" onclick="getSubChart(\'' + parentId + '\',\'GENDER\')">Gender</a></li>'
            + '<li><a class="dropdown-item" onclick="getSubChart(\'' + parentId + '\',\'RELIGION\')">Religion</a></li>'
            + '<li><a class="dropdown-item" onclick="getSubChart(\'' + parentId + '\',\'CASTE\')">Caste</a></li>'
            + '<li><a class="dropdown-item" onclick="getSubChart(\'' + parentId + '\',\'OCCUPATION\')">Occupation</a></li>'
            + '<li><a class="dropdown-item" onclick="getSubChart(\'' + parentId + '\',\'PARTY_LOKSABHA_ELECTION\')">PC 2019</a></li>'
            + '<li><a class="dropdown-item" onclick="getSubChart(\'' + parentId + '\',\'PARTY_LAST_ASSEMBLY_ELECTION\')">AC 2018</a></li>'
            + '<li><a class="dropdown-item" onclick="getSubChart(\'' + parentId + '\',\'PARTY_UPCOMING_ASSEMBLY_ELECTI\')">AC 2023</a></li>'
            + '</ul>';
    $("#" + appendId).html(parent);
    $("#" + appendId).css({top: position.top + 44, left: 306});
    $("#" + appendId).show();
    changeFiletrImg();
    $('#' + liId + "Img").attr('src', 'images/Right-Arrow.png');


}
function changeFiletrImg()
{
    $("#trsSurveyChildDataImg").attr('src', 'images/threedotsBlue.png');
    $("#congressSurveyChildDataImg").attr('src', 'images/threedotsBlue.png');
    $("#bjpSurveyChildDataImg").attr('src', 'images/threedotsBlue.png');
    $("#bspSurveyChildDataImg").attr('src', 'images/threedotsBlue.png');
    $("#aimimSurveyChildDataImg").attr('src', 'images/threedotsBlue.png');
    $("#cpiSurveyChildDataImg").attr('src', 'images/threedotsBlue.png');
    $("#cpimSurveyChildDataImg").attr('src', 'images/threedotsBlue.png');
    $("#otherSurveyChildDataImg").attr('src', 'images/threedotsBlue.png');
}

function getSubChart(filterValue, filterColumn)
{
    $("#insightsSurveyAnalyticsFilterDataId").hide();
    $("#SurveyChildData").hide();
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'getSubChartVotingAnalytics',
        cache: false,
        data: {
            filterColumn: filterColumn,
            filterValue: filterValue
        },
        success: function (response) {
            stopLoader();
            var result = response;
            getVotingChart("insightsSurveyAnalyticsChartId", result);
        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
}





