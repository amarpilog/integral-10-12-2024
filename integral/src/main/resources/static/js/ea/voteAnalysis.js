/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

var timeOut = null;
$(function () {
//     google.charts.load('current', {'packages':['corechart']});
//      google.charts.setOnLoadCallback(drawChart);

})
$(document).ready(function () {

})

//function getVoteDetails() {
//    $("#Loader").css("display", "block"); 
//    $(".homeSocialTrends").hide();
//    $(".visualizationDashboardView").hide();
//    $.ajax({
//        type: "post",
//        traditional: true,
//        dataType: 'json',
//        url: 'getVoteAnalysisTemplate',
//        data: {
//            dataObj: "{}"
//        },
//        success: function (response) {
//            stopLoader();
//
//            var pageLayout = response['pageLayout'];
////            $("#pageBodyContent").remove();
//            $("#pageBodyContent>div").hide();
//            $("#voteAnalysisPageBody").remove();
//            $("#pageBodyContent").append('<div id ="voteAnalysisPageBody" class="voteAnalysis-page-body"></div>');
////            $("#pageBody").append('<div class="page-body-content" id="pageBodyContent"></div></div>');
//            $("#voteAnalysisPageBody").append(pageLayout);
//            $("#selectState").val("TELANGANA");
//            $("#selectElectionYear").val("2018")
//            $("#selectACorPC").val("AC")
//
//            var censusLayout = response['censusLayout'];
//            loadCensusLayout(censusLayout);
//            fetchPoliticalSummaryInner("2018", "AC", "TELANGANA");
//
//            // State
//            var dataObj = {};
//            dataObj['optionsLabel'] = "STATE";
//            dataObj['optionsTable'] = "IS_ELECTION_RESULTS";
//
//            $("#selectState").jqxDropDownList({theme: "arctic", filterable: true, filterPlaceHolder: "Search", placeHolder: "Select State", width: 200, height: 30});
//            generateOptionsSource("selectState", dataObj);
//            $("#selectState").jqxDropDownList('selectItem', "TELANGANA");
//
//            //ACPC
//            var acpcSource = ['AC', 'PC'];
//            $("#selectACorPC").jqxDropDownList({theme: "arctic", filterable: true, filterPlaceHolder: "Search", selectedIndex: 3, placeHolder: "Select AC/PC", source: acpcSource, width: 200, height: 30});
//            $("#selectACorPC").jqxDropDownList('selectItem', "AC");
//
//            // YEAR
//            var dataObj = {};
//            dataObj['optionsLabel'] = "YEAR";
//            dataObj['optionsTable'] = "IS_ELECTION_RESULTS";
//
//            var whereClauseArray = [];
//            var whereClauseObj = {};
//            whereClauseObj['whereClauseColumn'] = 'STATE';
//            whereClauseObj['whereClauseColumnValue'] = 'TELANGANA';
//            whereClauseArray.push(whereClauseObj);
//
//            var whereClauseObj1 = {};
//            whereClauseObj1['whereClauseColumn'] = 'PC_AC';
//            whereClauseObj1['whereClauseColumnValue'] = 'AC';
//            whereClauseArray.push(whereClauseObj1);
//            dataObj['whereClauseArray'] = whereClauseArray;
//
//            $("#selectElectionYear").jqxDropDownList({theme: "arctic", filterable: true, filterPlaceHolder: "Search", placeHolder: "Select Year", width: 200, height: 30});
//            generateOptionsSource("selectElectionYear", dataObj);
//            $("#selectElectionYear").jqxDropDownList('selectItem', "2018");
//
//
//            $("#selectState").on('select', function (event) {
//                var state = $("#selectState").val();
//                var year = $("#selectElectionYear").val()
//                var acpc = $("#selectACorPC").val()
//
//                var dataObj = {};
//                dataObj['optionsLabel'] = "YEAR";
//                dataObj['optionsTable'] = "IS_ELECTION_RESULTS";
//
//                var whereClauseArray = [];
//                var whereClauseObj = {};
//                whereClauseObj['whereClauseColumn'] = "STATE";
//                whereClauseObj['whereClauseColumnValue'] = state;
//                whereClauseArray.push(whereClauseObj)
//
//                var whereClauseObj2 = {};
//                whereClauseObj2['whereClauseColumn'] = "PC_AC";
//                whereClauseObj2['whereClauseColumnValue'] = acpc;
//                whereClauseArray.push(whereClauseObj2)
//
//                dataObj['whereClauseArray'] = whereClauseArray;
//                var yearSource = generateOptionsSource("selectElectionYear", dataObj);
//
//                $("#selectElectionYear").jqxDropDownList('selectIndex', 0)
//                var year = $("#selectElectionYear").val()
//
//                fetchPoliticalSummaryInner(year, acpc, state)
//            });
//
//            $("#selectACorPC").on('select', function (event) {
//                var state = $("#selectState").val();
//                var acpc = $("#selectACorPC").val();
////                var year = $("#selectElectionYear").val();
//
//                var dataObj = {};
//                dataObj['optionsLabel'] = "YEAR";
//                dataObj['optionsTable'] = "IS_ELECTION_RESULTS";
//
//                var whereClauseArray = [];
//                var whereClauseObj1 = {};
//                whereClauseObj1['whereClauseColumn'] = "STATE";
//                whereClauseObj1['whereClauseColumnValue'] = state;
//                whereClauseArray.push(whereClauseObj1);
//
//                var whereClauseObj2 = {};
//                whereClauseObj2['whereClauseColumn'] = "PC_AC";
//                whereClauseObj2['whereClauseColumnValue'] = acpc;
//                whereClauseArray.push(whereClauseObj2)
//
//                dataObj['whereClauseArray'] = whereClauseArray;
//                generateOptionsSource("selectElectionYear", dataObj);
//                $("#selectElectionYear").jqxDropDownList('selectIndex', 0);
//                var year = $("#selectElectionYear").val();
//
////                if (acpc == "PC") {
////                    $('#selectElectionYear option').remove();
////                    $("#selectElectionYear").prepend("<option value='2019'>2019</option><option value='2014'>2014</option>");
////                } else if (acpc == "AC"){
////                    $('#selectElectionYear option').remove();
////                    $("#selectElectionYear").prepend("<option value='2018'>2018</option><option value='2014'>2014</option>");
////                }
//
//                fetchPoliticalSummaryInner(year, acpc, state)
//            });
//
//            $("#selectElectionYear").on('select', function (event) {
//                var state = $("#selectState").val();
//                var year = $("#selectElectionYear").val()
//                var acpc = $("#selectACorPC").val()
//                fetchPoliticalSummaryInner(year, acpc, state)
//            });
//
//            $("#toggleEAtabsButton").click(function (event) {
//                var summarySection = $(".summarySection");
//                var totalWidth = $(".summarySection").width();
//                if ($(".slimScrollDiv").parent("div").hasClass("col-sm-2")) {
//                    $(".slimScrollDiv").parent("div").removeClass();
//                    $(".slimScrollDiv").parent("div").css("width", "50px");
//                    $(".slimScrollDiv").css("width", "50px");
//                    $(".css-889cso").css("margin-left", "50px");
//                    $(".nav-link-text").hide();
//                    
//                    $(".tabcontentSummary").parent("div").removeClass();
//                    $(".tabcontentSummary").parent("div").css("width", totalWidth-50);
////                    $(".eaSelectbox").css("width","92.5%");
//                   
//                    $("#toggleEAtabsButton").find("path").attr("d", "M10.294 9.698a.988.988 0 010-1.407 1.01 1.01 0 011.419 0l2.965 2.94a1.09 1.09 0 010 1.548l-2.955 2.93a1.01 1.01 0 01-1.42 0 .988.988 0 010-1.407l2.318-2.297-2.327-2.307z");
//
//                } else {
//                    $(".tabcontentSummary").parent("div").addClass("col-sm-10 col-md-10 col-lg-10");
//                    $(".slimScrollDiv").parent("div").addClass("col-sm-2 col-md-2 col-lg-2");
//                    $(".slimScrollDiv").css("width", "230px");
//                    $(".css-889cso").css("margin-left", "230px");
////                    $(".eaSelectbox").css("width","85.5%");
//                    $(".nav-link-text").show();
//                    $("#toggleEAtabsButton").find("path").attr("d", "M13.706 9.698a.988.988 0 000-1.407 1.01 1.01 0 00-1.419 0l-2.965 2.94a1.09 1.09 0 000 1.548l2.955 2.93a1.01 1.01 0 001.42 0 .988.988 0 000-1.407l-2.318-2.297 2.327-2.307z");
//                }
//            })
//
//            $(".pollingBoothIconDiv").popover({
//                trigger: "hover",
//                //title: "Event Timings", 
//                content: function () {
//                    return '<div>Click to to view Polling Booths data</div>'
//                },
//                html: true,
//                placement: "left",
//            })
//            $(".pollingBoothIconDiv").on('hidden.bs.popover', function () {
//                $(".popover").remove();
//            });
//
//
//        },
//        error: function (e) {
//            console.log(e);
//            sessionTimeout(e);
//            stopLoader();
//        }
//    });
//}

function fetchPoliticalSummaryInner(year, acpc, state) {
    cleardataView();

    //  map div start
    if (acpc == "AC") {
//        loadDistrictsMap(year, state, "mapViewDiv");
        initMap(year, state, "mapViewDiv");
    } else if (acpc == "PC") {
        initMapPC(year, state, "mapViewDiv");
//        loadPCConstituencies(year, acpc, state, "mapViewDiv");
    }

    //  DataView start

    // subdata view 1 
    $("#subDataView1").show();
    var chartObj1 = {};
//    var query = "SELECT PARTY, COUNT(PARTY) AS COUNT_NUM FROM IS_AC_PC_CANDIDATE_POSITION WHERE YEAR='" + year + "' AND PC_AC='" + acpc + "'  AND STATE='"+state+"'   GROUP BY PARTY  ORDER BY COUNT_NUM DESC";
    var query = "SELECT WINNER_PARTY, COUNT(WINNER_PARTY) FROM IS_ELECTION_RESULTS WHERE YEAR='" + year + "' AND PC_AC='" + acpc + "' AND STATE='" + state + "' GROUP BY WINNER_PARTY";
    chartObj1['query'] = query;
//    chartObj1['heading'] = 'Seats summary';
    chartObj1['title'] = 'Party wise Seats';
    createChartVoteAnalysis("chartDiv1", chartObj1);


    var dataObj1 = {};
//    var query = "SELECT PARTY, COUNT_NUM, ROWNUM FROM (SELECT PARTY ,  COUNT(PARTY) AS COUNT_NUM FROM IS_AC_PC_CANDIDATE_POSITION WHERE YEAR='" + year + "' AND PC_AC='" + acpc + "' AND STATE='"+state+"' GROUP BY PARTY ORDER BY COUNT_NUM DESC)";
    var query = "SELECT WINNER_PARTY, COUNT_NUM, ROWNUM FROM (SELECT WINNER_PARTY ,  COUNT(WINNER_PARTY) AS COUNT_NUM FROM IS_ELECTION_RESULTS WHERE YEAR='" + year + "' AND PC_AC='" + acpc + "' AND STATE='" + state + "' GROUP BY WINNER_PARTY ORDER BY COUNT_NUM DESC)";
    var columnLabelsList = ['Party', 'Seats', 'Rank'];
    var columnList = ['WINNER_PARTY', 'COUNT_NUM', 'ROWNUM'];
    var columnWidthArray = [222, 222, 222];
    dataObj1['query'] = query;
    dataObj1['columnLabelsList'] = columnLabelsList;
    dataObj1['columnList'] = columnList;
    dataObj1['columnWidthArray'] = columnWidthArray;
//    dataObj1['filterTable'] = "Y";
//    dataObj1['sortTable'] = "Y";

//    dataObj1['heading'] = 'Seats Summary';
    createDataTable("dataDiv1", dataObj1);

    // subdata view 2
    $("#subDataView2").show();
    var chartObj2 = {};
//    var query = "SELECT PARTY, SUM(VOTES) FROM IS_AC_PC_CANDIDATE_POSITION WHERE YEAR='" + year + "' AND PC_AC='" + acpc + "' AND STATE='"+state+"' GROUP BY PARTY";
    var query = "SELECT WINNER_PARTY, SUM(WINNER_VOTES) FROM IS_ELECTION_RESULTS WHERE YEAR='" + year + "' AND PC_AC='" + acpc + "' AND STATE='" + state + "' GROUP BY WINNER_PARTY";
    chartObj2['query'] = query;
//    chartObj2['heading'] = 'Votes summary';
    chartObj2['title'] = 'Party wise Votes';
    createChartVoteAnalysis("chartDiv2", chartObj2);

    var dataObj2 = {};
//    var query = "SELECT PARTY, COUNT_NUM, ROWNUM FROM (SELECT PARTY,  SUM(VOTES) AS COUNT_NUM FROM  IS_AC_PC_CANDIDATE_POSITION WHERE YEAR='" + year + "' AND PC_AC='" + acpc + "' AND STATE='"+state+"' GROUP BY PARTY ORDER BY COUNT_NUM DESC)";
    var query = "SELECT WINNER_PARTY, COUNT_NUM, ROWNUM FROM (SELECT WINNER_PARTY,  SUM(WINNER_VOTES) AS COUNT_NUM FROM  IS_ELECTION_RESULTS WHERE YEAR='" + year + "' AND PC_AC='" + acpc + "' AND STATE='" + state + "' GROUP BY WINNER_PARTY ORDER BY COUNT_NUM DESC)";
    var columnLabelsList = ['Party', 'Votes', 'Rank'];
    var columnList = ['WINNER_PARTY', 'COUNT_NUM', 'ROWNUM'];
    var columnWidthArray = [222, 222, 222];
    dataObj2['query'] = query;
    dataObj2['columnLabelsList'] = columnLabelsList;
    dataObj2['columnList'] = columnList;
//    dataObj2['heading'] = 'Votes Summary';
    dataObj2['columnWidthArray'] = columnWidthArray;
//    dataObj2['filterTable'] = "Y";
//    dataObj2['sortTable'] = "Y";
    createDataTable("dataDiv2", dataObj2);

    // subdata view 3
    var dataObj3 = {};
    var query = "SELECT ASSEMBLY_CONSTITUENCY,  WINNER_CANDIDATE,  WINNER_PARTY, WINNER_VOTES, RUNNER_CANDIDATE, RUNNER_PARTY, RUNNER_VOTES, MARGIN FROM IS_ELECTION_RESULTS WHERE YEAR='" + year + "' AND PC_AC='" + acpc + "' AND STATE='" + state + "' ";
    dataObj3['query'] = query;
    var columnLabelsList = ['Constituency', 'Winner', 'Winner Party', 'Winner Votes', 'Runner', 'Runner Party', 'Runner votes', 'Margin'];
    var columnList = ['ASSEMBLY_CONSTITUENCY', 'WINNER_CANDIDATE', 'WINNER_PARTY', 'WINNER_VOTES', 'RUNNER_CANDIDATE',
        'RUNNER_PARTY', 'RUNNER_VOTES', 'MARGIN'];
    dataObj3['columnLabelsList'] = columnLabelsList;
    dataObj3['heading'] = (acpc == "AC" ? "Assembly" : "Parliament") + ' Constituencies';
    dataObj3['columnList'] = columnList;
    var columnWidthArray = [200, 300, 200, 200, 300, 200, 200, 100];
    dataObj3['columnWidthArray'] = columnWidthArray;
    dataObj3['filterTable'] = "Y";
    dataObj3['sortTable'] = "Y";
    dataObj3['checkBoxColumn'] = "Y";
    dataObj3['aggrigateColumns'] = ['Winner Votes', 'Runner votes', 'Margin'];
//    dataObj3['tableWidth'] = getTotalWidth(columnWidthArray);
//    dataObj3['tableheight'] = "500px";
    createDataTable("dataDiv3", dataObj3);


    var censusDataObj = {};
    var queries = {};
    queries['Total Constituencies'] = "SELECT COUNT(PC_AC_NAME) FROM IS_AGGREGATE_DATA WHERE STATE = '" + state + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
    queries['Electors'] = "SELECT SUM(ELECTORS) FROM IS_AGGREGATE_DATA WHERE STATE = '" + state + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "' ";
    queries['Female Electors'] = "SELECT SUM(FEMALE_ELECTORS) FROM IS_AGGREGATE_DATA WHERE STATE = '" + state + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
    queries['Male Electors'] = "SELECT SUM(MALE_ELECTORS) FROM IS_AGGREGATE_DATA WHERE STATE = '" + state + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
    queries['Polled Votes'] = "SELECT SUM(TOTAL_VOTES_POLLED) FROM IS_AGGREGATE_DATA WHERE STATE = '" + state + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
    queries['Polled Vote Percent'] = "SELECT ROUND(AVG(TOTAL_VOTES_POLLED_PCT),2) FROM IS_AGGREGATE_DATA WHERE STATE = '" + state + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
//    queries['Valid Votes'] = "SELECT SUM(VALID_VOTES) FROM IS_AGGREGATE_DATA WHERE STATE = '" + state + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
//    queries['Valid Votes Percent'] = "SELECT ROUND(AVG(VALID_VOTES_PCT),2) FROM IS_AGGREGATE_DATA WHERE STATE = '" + state + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
    queries['Nota Votes'] = "SELECT SUM(NOTA_VOTES) FROM IS_AGGREGATE_DATA WHERE STATE = '" + state + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
    queries['Nota Votes Percent'] = "SELECT ROUND(AVG(NOTA_VOTES_PCT),2) FROM IS_AGGREGATE_DATA WHERE STATE = '" + state + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";

    queries['SC Voters'] = "SELECT SUM(SC_VOTERS) FROM IS_AGGREGATE_DATA WHERE STATE = '" + state + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
    queries['ST Voters'] = "SELECT SUM(ST_VOTERS) FROM IS_AGGREGATE_DATA WHERE STATE = '" + state + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
    queries['Rural Voters'] = "SELECT SUM(RURAL_VOTERS) FROM IS_AGGREGATE_DATA WHERE STATE = '" + state + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
    queries['Urban Voters'] = "SELECT SUM(URBAN_VOTERS) FROM IS_AGGREGATE_DATA WHERE STATE = '" + state + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";

    censusDataObj['queryColumns'] = ['Total Constituencies', 'Electors', 'Female Electors', 'Male Electors',
        'Polled Votes', 'Polled Vote Percent', 'Nota Votes', 'Nota Votes Percent',
        'SC Voters', 'ST Voters', 'Rural Voters', 'Urban Voters'];

    censusDataObj['queries'] = queries;
    censusDataObj['heading'] = state.initCap() + " electoral Data";
    createCensusDataTable("acpcDetails", censusDataObj);

}

function createChartVoteAnalysis(divId, dataObj) {
    showLoader();
//    var htmlstr = "<div id='" + divId + "'>"
    var htmlstr = "";
    if (dataObj['heading'] != null && dataObj['heading'] != '') {
        htmlstr += "<h4>" + dataObj['heading'] + "</h4>";
    }
    htmlstr += "<div id='" + divId + "_Graph'></div>";
//            +"</div>";
    $("#" + divId).html(htmlstr);
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'getChartDataVoteAnalysis',
        cache: false,
        data: {
            dataObj: JSON.stringify(dataObj)
        },
        success: function (response) {
            stopLoader();
            $("#" + divId).css("display", "block");
            var values = response['values'];
            var labels = response['labels'];
            var data = [{
                    y: values,
                    x: labels,
                    type: 'bar',
                    textinfo: "percent",
                    marker: {
//                        color:['#2F6345', '#40875E', '#58B07E', '#C48C00', '#F0AB00', '#FFBE1D', '#FFCC4B', '#827E32', '#A8A240', '#C5C169']
                        color: generateRandomColourArray(labels)
                    }
                }];

            var layout = {
                height: 350,
                width: 440,
                title: dataObj['title'],
            };

            Plotly.newPlot(divId + "_Graph", data, layout);
        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });

}

function createDataTable(divId, dataObj) {
    showLoader();
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'getTemplateDataTable',
        cache: false,
        async: false,
        data: {
            dataObj: JSON.stringify(dataObj)
        },
        success: function (response) {
            stopLoader();
            var tableStr = response['tableStr'];
            $("#" + divId).css("display", "block");
            $("#" + divId).html(tableStr);
            var inputStr = JSON.stringify(dataObj);

            var regex = new RegExp("'", 'g');
            inputStr = inputStr.replace(regex, "&apos;");
            inputStr = "<input type='hidden' id='" + divId + "_input' value='" + inputStr + "'>";
            var content = tableStr + inputStr;
      
//            appendToDataTable($("#" + divId), $("#" + divId), content, dataObj);
            appendToDataTable(divId, divId, content, dataObj);

//            $("#" + divId).append("<input type='hidden' id='" + divId + "_input' value='" + inputStr + "'>");



        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
}


function createCensusDataTable(divId, dataObj) {
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'getCensusDataTable',
        cache: false,
        data: {
            dataObj: JSON.stringify(dataObj),

        },
        success: function (response) {
            stopLoader();
            var tableStr = response['tableStr'];
            var heading = "";

            $("#" + divId).css("display", "block");
            $("#" + divId).html(tableStr);
        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
}

function switchTabs(event, divId) {
    $(".eaTabs ").removeClass("eaSelectedTab");
    $(event.currentTarget).addClass("eaSelectedTab")
    $(".tabcontentSummary").css("display", "none");
    $("#" + divId).css("display", "block");

}
function switchStrongWeakTabs(event, divId) {
    $(".eaTabsSW ").removeClass("eaSelectedTab");
    if (event.currentTarget != null) {
        $(event.currentTarget).addClass("eaSelectedTab")
    } else {
        $(event.target).addClass("eaSelectedTab")
    }

    $(".strongWeakTabsSummary").css("display", "none");
    $("#" + divId).css("display", "block");
}


function getValueData(value, column) {
    if (value != null) {
        value = value.toUpperCase();
    }
    var state = $("#selectState").val();
    var year = $("#selectElectionYear").val();
    var acpc = $("#selectACorPC").val();
    var acpcLabel = $("#selectACorPC option:selected").text();

    var dataObj = {};

    if (column == "Party" || (column.indexOf("Party") > -1)) {
        cleardataView();
        var dataObj = {};
        var query = "SELECT YEAR, COUNT(YEAR) FROM IS_ELECTION_RESULTS WHERE WINNER_PARTY ='" + value + "' AND YEAR='" + year + "' AND PC_AC='" + acpc + "' AND STATE='" + state + "' GROUP BY YEAR";
        dataObj['query'] = query;
        dataObj['title'] = 'Year wise Seats';
        dataObj['heading'] = column + " : " + value;
        createChartVoteAnalysis("chartDiv1", dataObj);

        var query = "SELECT YEAR, COUNT(YEAR) FROM IS_ELECTION_RESULTS WHERE WINNER_PARTY ='" + value + "' AND YEAR='" + year + "' AND PC_AC='" + acpc + "' AND STATE='" + state + "' GROUP BY YEAR";
        dataObj['query'] = query;
//        dataObj['heading'] = column + " : " + value;
        var columnLabelsList = ['Year', 'Seats'];
        dataObj['columnLabelsList'] = columnLabelsList;
        dataObj['columnWidthArray'] = [300, 300];
        dataObj['filterTable'] = "Y";
        dataObj['sortTable'] = "Y";
        createDataTable("dataDiv1", dataObj);

    } else if (column == "Constituency") {
        cleardataView();

        var chartDataObj = {};
//        var query = "SELECT PARTY, SUM(VOTES) FROM IS_AC_PC_CANDIDATE_POSITION WHERE PC_AC_NAME ='" + value + "' AND YEAR='" + year + "' AND PC_AC='" + acpc + "'  AND STATE='"+state+"'  GROUP BY PARTY";
        var query = "SELECT PARTY,  VOTES_SUM FROM (SELECT PARTY, SUM(VOTES) AS VOTES_SUM FROM IS_AC_PC_CANDIDATE_POSITION WHERE PC_AC_NAME ='" + value + "' AND YEAR='" + year + "' AND PC_AC='" + acpc + "'  AND STATE='" + state + "'  GROUP BY PARTY  ORDER BY VOTES_SUM DESC) WHERE ROWNUM<6"
        chartDataObj['query'] = query;
        chartDataObj['title'] = 'Party wise Votes';
        chartDataObj['heading'] = acpcLabel + " " + column + " : " + value.initCap();
        createChartVoteAnalysis("chartDiv0", chartDataObj);

        var dataObj = {};
        var query = "SELECT TO_NUMBER(POSITION), CANDIDATE_NAME, VOTES, VOTES_PCT, PARTY FROM IS_AC_PC_CANDIDATE_POSITION WHERE PC_AC_NAME ='" + value + "' AND YEAR='" + year + "' AND PC_AC='" + acpc + "' AND STATE='" + state + "' ORDER BY TO_NUMBER(POSITION) ";
        dataObj['query'] = query;
        dataObj['heading'] = acpcLabel + " " + column + " : " + value.initCap();
        var columnLabelsList = ['Position', 'Candidate', 'Votes', 'Votes Percent', 'Party'];
        var columnList = ['POSITION', 'CANDIDATE_NAME', 'VOTES', 'VOTES_PCT', 'PARTY'];
        dataObj['columnLabelsList'] = columnLabelsList;
        dataObj['columnList'] = columnList;
        dataObj['columnWidthArray'] = [100, 400, 120, 200, 400];
        dataObj['filterTable'] = "Y";
        dataObj['sortTable'] = "Y";
        dataObj['checkBoxColumn'] = "Y";
        dataObj['aggrigateColumns'] = ['Votes', 'Votes Percent'];
        createDataTable("dataDiv3", dataObj);

        var censusDataObj = {};
        var queries = {};
        var mlamp = acpc == "AC" ? "MLA" : "MP";
        queries[mlamp] = "SELECT WINNER_CANDIDATE FROM IS_ELECTION_RESULTS WHERE ASSEMBLY_CONSTITUENCY = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
        queries['Party'] = "SELECT WINNER_PARTY FROM IS_ELECTION_RESULTS WHERE ASSEMBLY_CONSTITUENCY = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
        queries['District'] = "SELECT DISTRICT FROM IS_AGGREGATE_DATA WHERE PC_AC_NAME = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
        queries['Total Contestants'] = "SELECT COUNT(PC_AC_NAME) FROM IS_AC_PC_CANDIDATE_POSITION WHERE PC_AC_NAME = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
        queries['Electors'] = "SELECT ELECTORS FROM IS_AGGREGATE_DATA WHERE PC_AC_NAME = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
        queries['Female Electors'] = "SELECT FEMALE_ELECTORS FROM IS_AGGREGATE_DATA WHERE PC_AC_NAME = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
        queries['Male Electors'] = "SELECT MALE_ELECTORS FROM IS_AGGREGATE_DATA WHERE PC_AC_NAME = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
        queries['Polled Votes'] = "SELECT TOTAL_VOTES_POLLED FROM IS_AGGREGATE_DATA WHERE PC_AC_NAME = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
        queries['Polled Vote Percent'] = "SELECT TOTAL_VOTES_POLLED_PCT FROM IS_AGGREGATE_DATA WHERE PC_AC_NAME = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
//        queries['Valid Votes'] = "SELECT VALID_VOTES FROM IS_AGGREGATE_DATA WHERE PC_AC_NAME = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
//        queries['Valid Votes Percent'] = "SELECT VALID_VOTES_PCT FROM IS_AGGREGATE_DATA WHERE PC_AC_NAME = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
        queries['Nota Votes'] = "SELECT NOTA_VOTES FROM IS_AGGREGATE_DATA WHERE PC_AC_NAME = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
        queries['Nota Votes Percent'] = "SELECT NOTA_VOTES_PCT FROM IS_AGGREGATE_DATA WHERE PC_AC_NAME = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";

        queries['SC Voters'] = "SELECT SC_VOTERS FROM IS_AGGREGATE_DATA WHERE PC_AC_NAME = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
        queries['ST Voters'] = "SELECT ST_VOTERS FROM IS_AGGREGATE_DATA WHERE PC_AC_NAME = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
        queries['Rural Voters'] = "SELECT RURAL_VOTERS FROM IS_AGGREGATE_DATA WHERE PC_AC_NAME = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
        queries['Urban Voters'] = "SELECT URBAN_VOTERS FROM IS_AGGREGATE_DATA WHERE PC_AC_NAME = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";

        censusDataObj['queryColumns'] = [mlamp, 'Party', 'District', 'Total Contestants', 'Electors', 'Female Electors', 'Male Electors',
            'Polled Votes', 'Polled Vote Percent', 'Nota Votes', 'Nota Votes Percent',
            'SC Voters', 'ST Voters', 'Rural Voters', 'Urban Voters'];
        censusDataObj['queries'] = queries;
        censusDataObj['heading'] = value.initCap() + " electoral Data";
        createCensusDataTable("acpcDetails", censusDataObj);

    } else if (column == "District") {
        cleardataView();

        var dataObj = {};
//        var query = "SELECT WINNER_PARTY, SUM(WINNER_VOTES) FROM IS_ELECTION_RESULTS WHERE DISTRICT='" + value + "' AND YEAR='" + year + "' AND PC_AC='" + acpc + "'  AND STATE='"+state+"'  GROUP BY WINNER_PARTY";
        var query = "SELECT PARTY, VOTES_SUM FROM (SELECT PARTY,SUM(VOTES) AS VOTES_SUM FROM IS_AC_PC_CANDIDATE_POSITION WHERE DISTRICT='" + value + "' AND YEAR='" + year + "' AND PC_AC='" + acpc + "'  AND STATE='" + state + "' GROUP BY PARTY,DISTRICT ORDER BY VOTES_SUM DESC) WHERE ROWNUM <6"
        dataObj['query'] = query;
        dataObj['title'] = 'Party wise Votes';
        dataObj['heading'] = column + " : " + value;
        createChartVoteAnalysis("chartDiv0", dataObj);

        var query = "SELECT ASSEMBLY_CONSTITUENCY, WINNER_CANDIDATE, WINNER_PARTY,  TOTAL_ELECTORS, POLLED_VOTES,  WINNER_VOTES, MARGIN FROM IS_ELECTION_RESULTS WHERE YEAR='" + year + "' AND DISTRICT='" + value + "' AND PC_AC='" + acpc + "' AND STATE='" + state + "'";
        dataObj['query'] = query;
        dataObj['heading'] = column + " : " + value;
        var columnLabelsList = ['Constituency', 'Winner', 'Winner Party', 'Total Votes', 'Polled Votes', 'Winner Votes', "Margin"];
        var columnList = ['ASSEMBLY_CONSTITUENCY', 'WINNER_CANDIDATE', 'WINNER_PARTY', 'TOTAL_ELECTORS', 'POLLED_VOTES',
            'WINNER_VOTES', "MARGIN"];
        dataObj['columnLabelsList'] = columnLabelsList;
        dataObj['columnList'] = columnList;
        dataObj['columnWidthArray'] = [300, 300, 300, 300, 300, 300, 300];
        dataObj['filterTable'] = "Y";
        dataObj['sortTable'] = "Y";
        dataObj['checkBoxColumn'] = "Y";
        dataObj['aggrigateColumns'] = ['Total Votes', 'Polled Votes', 'Winner Votes', 'Margin'];

        createDataTable("dataDiv3", dataObj);

        var censusDataObj = {};
        var queries = {};
        queries['Total Constituencies'] = "SELECT COUNT(PC_AC_NAME) FROM IS_AGGREGATE_DATA WHERE DISTRICT = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
        queries['Electors'] = "SELECT SUM(ELECTORS) FROM IS_AGGREGATE_DATA WHERE DISTRICT = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "' ";
        queries['Female Electors'] = "SELECT SUM(FEMALE_ELECTORS) FROM IS_AGGREGATE_DATA WHERE DISTRICT = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
        queries['Male Electors'] = "SELECT SUM(MALE_ELECTORS) FROM IS_AGGREGATE_DATA WHERE DISTRICT = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
        queries['Polled Votes'] = "SELECT SUM(TOTAL_VOTES_POLLED) FROM IS_AGGREGATE_DATA WHERE DISTRICT = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
        queries['Polled Vote Percent'] = "SELECT ROUND(AVG(TOTAL_VOTES_POLLED_PCT),2) FROM IS_AGGREGATE_DATA WHERE DISTRICT = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
//        queries['Valid Votes'] = "SELECT SUM(VALID_VOTES) FROM IS_AGGREGATE_DATA WHERE DISTRICT = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
//        queries['Valid Votes Percent'] = "SELECT ROUND(AVG(VALID_VOTES_PCT),2) FROM IS_AGGREGATE_DATA WHERE DISTRICT = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
        queries['Nota Votes'] = "SELECT SUM(NOTA_VOTES) FROM IS_AGGREGATE_DATA WHERE DISTRICT = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
        queries['Nota Votes Percent'] = "SELECT ROUND(AVG(NOTA_VOTES_PCT),2) FROM IS_AGGREGATE_DATA WHERE DISTRICT = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";

        queries['SC Voters'] = "SELECT SUM(SC_VOTERS) FROM IS_AGGREGATE_DATA WHERE PC_AC_NAME = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
        queries['ST Voters'] = "SELECT SUM(ST_VOTERS) FROM IS_AGGREGATE_DATA WHERE PC_AC_NAME = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
        queries['Rural Voters'] = "SELECT SUM(RURAL_VOTERS) FROM IS_AGGREGATE_DATA WHERE PC_AC_NAME = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";
        queries['Urban Voters'] = "SELECT SUM(URBAN_VOTERS) FROM IS_AGGREGATE_DATA WHERE PC_AC_NAME = '" + value + "' AND YEAR = '" + year + "' AND PC_AC = '" + acpc + "'";

        censusDataObj['queryColumns'] = ['Total Constituencies', 'Electors', 'Female Electors', 'Male Electors',
            'Polled Votes', 'Polled Vote Percent', 'Nota Votes', 'Nota Votes Percent',
            'SC Voters', 'ST Voters', 'Rural Voters', 'Urban Voters'];

        censusDataObj['queries'] = queries;
        censusDataObj['heading'] = value.initCap() + " electoral Data";
        createCensusDataTable("acpcDetails", censusDataObj);
    } else if (column == "Candidate" || column == "Winner" || column == "Runner") {
        value = value.replace(/\u00a0/g, " "); // replace html entities with space
        $("#candidateInfo").remove();
        var form = "<form id='candidateInfo' >"
                + "<input id='searchCandidate' name='searchCandidate' value='" + value + "'/>"
                + "</form>";

        $("body").append(form);
        $("#candidateInfo").attr("target", "_blank");
        $("#candidateInfo").attr("method", "post");
        $("#candidateInfo").attr("action", "homePage");

        $("#candidateInfo").submit();
//        $("#voteAnalysisPageBody").remove();
//         value = value.replace(/&nbsp;/g, ' ');
//        getProfileUserNames(value);
//         window.open("http://localhost:8080/insights/homePage", "_blank");
    }

}

function  cleardataView() {
    $("#chartDiv0").html("");
    $("#chartDiv0").css("display", "none");

    $("#acpcDetails").html("");
    $("#acpcDetails").css("display", "none");


    $("#chartDiv1").html("");
    $("#chartDiv1").css("display", "none");
    $("#dataDiv1").html("");
    $("#dataDiv1").css("display", "none");
    $("#subDataView1").hide();

    $("#chartDiv2").html("");
    $("#chartDiv2").css("display", "none");
    $("#dataDiv2").html("");
    $("#dataDiv2").css("display", "none");
    $("#subDataView2").hide();

    $("#dataDiv3").html("");
    $("#dataDiv3").css("display", "none");
}

String.prototype.initCap = function () {
    return this.toLowerCase().replace(/(?:^|\s)[a-z]/g, function (m) {
        return m.toUpperCase();
    });
};

function generateRandomColourArray(labels) {
    var rgb = [];

    for (var i = 0; i < labels.length; i++) {
//     rgb.push(Math.floor(Math.random() * 255));
//     rgb.push('#'+ Math.floor(Math.random() * 19777215).toString(16));
        var colors = ['red', 'blue', 'grey', 'lightgreen',
            'violet', 'Tomato', 'DodgerBlue', 'MediumSeaGreen', 'SlateBlue', 'LightGray', 'purple'];
        if (labels[i] == "TRS") {
            rgb.push('pink');
        } else if (labels[i] == "INC") {
            rgb.push('skyblue');
        } else if (labels[i] == "BJP") {
            rgb.push('orange');
        } else if (labels[i] == "TDP") {
            rgb.push('yellow');
        } else if (labels[i] == "AIMIM") {
            rgb.push('lightgreen');
        } else if (labels[i] == "AIFB") {
            rgb.push('red');
        } else {
            rgb.push(colors[Math.floor(Math.random() * colors.length)]);
        }


    }
    return rgb;
}

function generateOptionsSource(selectBoxId, dataObj) {

    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'generateOptionsSource',
        cache: false,
        async: false,
        data: {
            dataObj: JSON.stringify(dataObj),
        },
        success: function (response) {
            stopLoader();
            if (response != null) {
                var source = response['source'];
                $("#" + selectBoxId).jqxDropDownList({source: source});

//                $("#" + selectBoxId).jqxDropDownList('selectIndex', 0 )
//                var optionStr = response['optionsStr'];
//                if (optionStr != null && optionStr != "") {
//                    $("#" + selectBoxId).html(optionStr);
//                } else {
//                    $("#" + selectBoxId).html("<option value='2019'>2019</option>");
//                }
            }

        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });

}


function searchDataTable(event, columnName) {
    clearTimeout(timeOut);
    timeOut = setTimeout(function () {

        var filterVal = $(event.target).val();
//     var divId = $(event.target).closest('table');
//     var divId = $(event.target).closest('table').parent('div').parent('div');
        var divId = $(event.target).closest('.eaDataTableDiv').parent('div').attr("id");
        var inputId = divId + "_input";
        var dataObjStr = $("#" + inputId).val()
        var dataObj = JSON.parse(dataObjStr);
        var query = dataObj['query'];
        var whereClause = "";
        var filterinput = $(event.target).closest('table').find(".eaFilterInput");

        $.each(filterinput, function (i) {
            var val = $(this).val()
            if (val != null && val != "") {
                var col = $(this).attr("id");
                whereClause += " UPPER(" + col + ") LIKE '%" + val.toUpperCase() + "%' AND ";
            }
            ;
        });
        if (whereClause != "") {
            whereClause = whereClause.substring(0, whereClause.length - 5);

            query = "SELECT * FROM (" + query + ") WHERE " + whereClause;
        }
        dataObj['query'] = query;
        showLoader();
        $.ajax({
            type: "post",
            traditional: true,
            dataType: 'json',
            url: 'searchDataTable',
            cache: false,
            async: false,
            data: {
                dataObj: JSON.stringify(dataObj),
            },
            success: function (response) {
                stopLoader();
                if (response != null) {
                    var tableStr = response['tableStr'];
//                    $(event.target).closest('table').find('tbody').html(tableStr);
                    var appendDivId = getRanHex(6);
                            $(event.target).closest('table').find('tbody').attr("id",appendDivId);
               
//                    appendToDataTable($(event.target).closest('table').find('tbody'), $("#" + divId), tableStr, dataObj);
                    appendToDataTable(appendDivId, divId, tableStr, dataObj);

                }
            },
            error: function (e) {
                console.log(e);
                sessionTimeout(e);
                stopLoader();
            }
        });

    }, 1000);
}


function sortDataTable(event, columnName, order) {

    var divId = $(event.target).closest('.eaDataTableDiv').parent('div').attr("id");
    var inputId = divId + "_input";
    var dataObjStr = $("#" + inputId).val()
    var dataObj = JSON.parse(dataObjStr);
    var query = dataObj['query'];
    // filter
    var whereClause = "";
    var filterinput = $(event.target).closest('table').find(".eaFilterInput");

    $.each(filterinput, function (i) {
        var val = $(this).val()
        if (val != null && val != "") {
            var col = $(this).attr("id");
            whereClause += " UPPER(" + col + ") LIKE '%" + val.toUpperCase() + "%' AND ";
        }
        ;
    });
    if (whereClause != "") {
        whereClause = whereClause.substring(0, whereClause.length - 5);

        query = "SELECT * FROM (" + query + ") WHERE " + whereClause;
    }
    // filter
    query = "SELECT * FROM (" + query + ") ORDER BY " + columnName + " " + order;

    dataObj['query'] = query;
    showLoader();
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'searchDataTable',
        cache: false,
        async: false,
        data: {
            dataObj: JSON.stringify(dataObj),
        },
        success: function (response) {
            stopLoader();
            if (response != null) {
                var tableStr = response['tableStr'];
//                $(event.target).closest('table').find('tbody').html(tableStr);
                var appendDivId = getRanHex(6);
                        $(event.target).closest('table').find('tbody').attr("id",appendDivId);
               
//                    appendToDataTable($(event.target).closest('table').find('tbody'), $("#" + divId), tableStr, dataObj);
                    appendToDataTable(appendDivId, divId, tableStr, dataObj);

            }
        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });

}

function showPopoverContent(candidate) {
//var resultDiv = "";
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'getCandidateProfileInfo',
        cache: false,
        data: {
            candidate: candidate
        },
        success: function (response) {
            stopLoader();
            var infoStr = response['infoStr'];
            infoStr = "<div class='candidateProfileInfo'>" + infoStr + "</div>"
            $(".popover-body").html(infoStr);
        }
    });
}

function loadCandidateProfilePic($this) {
    var candidate = $this.closest("td").find("span").text();
    console.log("candidate " + candidate)
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'getCandidateProfilePicString',
        cache: false,
        data: {
            candidate: candidate
        },
        success: function (response) {
            stopLoader();
            var imgStr = response['imgStr'];
            if (imgStr == "") {
                $this.attr("src", " ");
            } else {
                $this.attr("src", "data:image/png;base64," + imgStr);
            }
//                         console.log("imgStr "+imgStr)

        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
}

function showBoothData() {
    $(".popover").remove();
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'getBoothAnalysisTemplate',
        data: {
            dataObj: "{}"
        },
        success: function (response) {
            stopLoader();

            var pageLayout = response['pageLayout'];
//            $("#pageBodyContent").remove();
            $("#pageBodyContent>div").hide();
            $("#voteAnalysisPageBody").remove();
            $("#pageBodyContent").append('<div id ="voteAnalysisPageBody" class="voteAnalysis-page-body"></div>');
//            $("#pageBody").append('<div class="page-body-content" id="pageBodyContent"></div></div>');
            $("#voteAnalysisPageBody").append(pageLayout);

            // State
            var dataObj = {};
            dataObj['optionsLabel'] = "STATE";
            dataObj['optionsTable'] = "IS_POLLING_BOOTH";

            $("#selectState").jqxDropDownList({theme: "arctic", filterable: true, filterPlaceHolder: "Search", placeHolder: "Select State", width: 200, height: 30});
            generateOptionsSource("selectState", dataObj);
            $("#selectState").jqxDropDownList('selectItem', "TELANGANA");

            //ACPC
            var acpcSource = ['AC', 'PC'];
            $("#selectACorPC").jqxDropDownList({theme: "arctic", filterable: true, filterPlaceHolder: "Search", placeHolder: "Select AC/PC", source: acpcSource, width: 200, height: 30});
            $("#selectACorPC").jqxDropDownList('selectItem', "AC");

            // YEAR
            var dataObj = {};
            dataObj['optionsLabel'] = "YEAR";
            dataObj['optionsTable'] = "IS_POLLING_BOOTH";

            var whereClauseArray = [];
            var whereClauseObj = {};
            whereClauseObj['whereClauseColumn'] = 'STATE';
            whereClauseObj['whereClauseColumnValue'] = 'TELANGANA';
            whereClauseArray.push(whereClauseObj);

            var whereClauseObj1 = {};
            whereClauseObj1['whereClauseColumn'] = 'PC_AC';
            whereClauseObj1['whereClauseColumnValue'] = 'AC';
            whereClauseArray.push(whereClauseObj1);
            dataObj['whereClauseArray'] = whereClauseArray;

            $("#selectElectionYear").jqxDropDownList({theme: "arctic", filterable: true, filterPlaceHolder: "Search", placeHolder: "Select Year", width: 200, height: 30});
            generateOptionsSource("selectElectionYear", dataObj);


            // CONSTITUENCY
            $("#selectConstituency").jqxDropDownList({theme: "arctic", filterable: true, filterPlaceHolder: "Search", placeHolder: "Select Constituency", width: 200, height: 30});


            // BOOTH

            $("#selectBooth").jqxDropDownList({theme: "arctic", filterable: true, filterPlaceHolder: "Search", width: 200, placeHolder: "Select Booth", height: 30});



            $("#selectState").on('select', function (event) {
                var state = $("#selectState").val();
                var year = $("#selectElectionYear").val()
                var acpc = $("#selectACorPC").val()
                $("#selectConstituency").val("");
                 $("#selectConstituency").jqxDropDownList({source: []})
                $("#selectBooth").val("");
                 $("#selectBooth").jqxDropDownList({source: []})

                var dataObj = {};
                dataObj['optionsLabel'] = "YEAR";
                dataObj['optionsTable'] = "IS_ELECTION_RESULTS";

                var whereClauseArray = [];
                var whereClauseObj = {};
                whereClauseObj['whereClauseColumn'] = "STATE";
                whereClauseObj['whereClauseColumnValue'] = state;
                whereClauseArray.push(whereClauseObj)

                var whereClauseObj2 = {};
                whereClauseObj2['whereClauseColumn'] = "PC_AC";
                whereClauseObj2['whereClauseColumnValue'] = acpc;
                whereClauseArray.push(whereClauseObj2)

                dataObj['whereClauseArray'] = whereClauseArray;
                generateOptionsSource("selectElectionYear", dataObj);

                var year = $("#selectElectionYear").val()

                boothSummary(year, acpc, state)
            });

            $("#selectACorPC").on('select', function (event) {
                var state = $("#selectState").val();
                var acpc = $("#selectACorPC").val();
                 $("#selectConstituency").val("");
                 $("#selectConstituency").jqxDropDownList({source: []})
                $("#selectBooth").val("");
                 $("#selectBooth").jqxDropDownList({source: []})
//                var year = $("#selectElectionYear").val();

                var dataObj = {};
                dataObj['optionsLabel'] = "YEAR";
                dataObj['optionsTable'] = "IS_ELECTION_RESULTS";

                var whereClauseArray = [];
                var whereClauseObj1 = {};
                whereClauseObj1['whereClauseColumn'] = "STATE";
                whereClauseObj1['whereClauseColumnValue'] = state;
                whereClauseArray.push(whereClauseObj1);

                var whereClauseObj2 = {};
                whereClauseObj2['whereClauseColumn'] = "PC_AC";
                whereClauseObj2['whereClauseColumnValue'] = acpc;
                whereClauseArray.push(whereClauseObj2)

                dataObj['whereClauseArray'] = whereClauseArray;
                generateOptionsSource("selectElectionYear", dataObj);

                var year = $("#selectElectionYear").val();
                boothSummary(year, acpc, state)
            });

            $("#selectElectionYear").on('select', function (event) {
                var state = $("#selectState").val();
                var year = $("#selectElectionYear").val()
                var acpc = $("#selectACorPC").val()
                $("#selectConstituency").val("");
                 $("#selectConstituency").jqxDropDownList({source: []})
                $("#selectBooth").val("");
                 $("#selectBooth").jqxDropDownList({source: []})

                var dataObj = {};
                dataObj['optionsLabel'] = "PC_AC_NAME";
                dataObj['optionsTable'] = "IS_POLLING_BOOTH";
//                dataObj['includeSelectOption'] = "Y";

                var whereClauseArray = [];
                var whereClauseObj1 = {};
                whereClauseObj1['whereClauseColumn'] = "STATE";
                whereClauseObj1['whereClauseColumnValue'] = state;
                whereClauseArray.push(whereClauseObj1);

                var whereClauseObj2 = {};
                whereClauseObj2['whereClauseColumn'] = "PC_AC";
                whereClauseObj2['whereClauseColumnValue'] = acpc;
                whereClauseArray.push(whereClauseObj2)

                var whereClauseObj3 = {};
                whereClauseObj3['whereClauseColumn'] = "YEAR";
                whereClauseObj3['whereClauseColumnValue'] = year;
                whereClauseArray.push(whereClauseObj3)

                dataObj['whereClauseArray'] = whereClauseArray;
                dataObj['boothData'] = "Y";
                generateOptionsSource("selectConstituency", dataObj);

//                var constituency = $("#selectConstituency").val();
                boothSummary(year, acpc, state)
            });

            $("#selectConstituency").on('select', function (event) {
                var state = $("#selectState").val();
                var year = $("#selectElectionYear").val();
                var acpc = $("#selectACorPC").val();
                var constituency = $("#selectConstituency").val();
                $("#selectBooth").val("");
                 $("#selectBooth").jqxDropDownList({source: []})
                var dataObj = {};
                dataObj['optionsLabel'] = "POLLING_BOOTH";
                dataObj['optionsTable'] = "IS_POLLING_BOOTH";
//                    dataObj['includeSelectOption'] = "Y";

                var whereClauseArray = [];
                var whereClauseObj1 = {};
                whereClauseObj1['whereClauseColumn'] = "STATE";
                whereClauseObj1['whereClauseColumnValue'] = state;
                whereClauseArray.push(whereClauseObj1);

                var whereClauseObj2 = {};
                whereClauseObj2['whereClauseColumn'] = "PC_AC";
                whereClauseObj2['whereClauseColumnValue'] = acpc;
                whereClauseArray.push(whereClauseObj2)

                var whereClauseObj3 = {};
                whereClauseObj3['whereClauseColumn'] = "YEAR";
                whereClauseObj3['whereClauseColumnValue'] = year;
                whereClauseArray.push(whereClauseObj3)

                var whereClauseObj4 = {};
                whereClauseObj4['whereClauseColumn'] = "PC_AC_NAME";
                whereClauseObj4['whereClauseColumnValue'] = constituency;
                whereClauseArray.push(whereClauseObj4)

                dataObj['whereClauseArray'] = whereClauseArray;
                dataObj['boothData'] = "Y";
                generateOptionsSource("selectBooth", dataObj);
                boothSummary(year, acpc, state, constituency);


            });

            $("#selectBooth").on('select', function (event) {
                var state = $("#selectState").val();
                var year = $("#selectElectionYear").val();
                var acpc = $("#selectACorPC").val();
                var constituency = $("#selectConstituency").val();
                var poolingBooth = $("#selectBooth").val();

                boothSummary(year, acpc, state, constituency, poolingBooth)
            });

            $("#selectElectionYear").jqxDropDownList('selectIndex', 0);
            var year = $("#selectElectionYear").val();
            var acpc = $("#selectACorPC").val();
            var state = $("#selectState").val();
            boothSummary(year, acpc, state);

            // STRONG WEEK SELECTBOX START +++++++++++++=

            // State
            var dataObj = {};
            dataObj['optionsLabel'] = "STATE";
            dataObj['optionsTable'] = "IS_POLLING_BOOTH";

            $("#selectStateSW").jqxDropDownList({theme: "arctic", filterable: true, filterPlaceHolder: "Search", placeHolder: "Select State", width: 200, height: 30});
            generateOptionsSource("selectStateSW", dataObj);
            $("#selectStateSW").jqxDropDownList('selectItem', "TELANGANA");

            //ACPC
            var acpcSource = ['AC', 'PC'];
            $("#selectACorPCSW").jqxDropDownList({theme: "arctic", filterable: true, filterPlaceHolder: "Search", selectedIndex: 3, placeHolder: "Select AC/PC", source: acpcSource, width: 200, height: 30});
            $("#selectACorPCSW").jqxDropDownList('selectItem', "AC");

            // YEAR
            var dataObj = {};
            dataObj['optionsLabel'] = "YEAR";
            dataObj['optionsTable'] = "IS_POLLING_BOOTH";

            var whereClauseArray = [];
            var whereClauseObj = {};
            whereClauseObj['whereClauseColumn'] = 'STATE';
            whereClauseObj['whereClauseColumnValue'] = 'TELANGANA';
            whereClauseArray.push(whereClauseObj);

            var whereClauseObj1 = {};
            whereClauseObj1['whereClauseColumn'] = 'PC_AC';
            whereClauseObj1['whereClauseColumnValue'] = 'AC';
            whereClauseArray.push(whereClauseObj1);
            dataObj['whereClauseArray'] = whereClauseArray;

            $("#selectElectionYearSW").jqxDropDownList({theme: "arctic", filterable: true, filterPlaceHolder: "Search", placeHolder: "Select Year", width: 200, height: 30});
            generateOptionsSource("selectElectionYearSW", dataObj);
//            $("#selectElectionYearSW").jqxDropDownList('selectItem', "2018");

            // CONSTITURNCY

            $("#selectConstituencySW").jqxDropDownList({theme: "arctic", filterable: true, filterPlaceHolder: "Search", placeHolder: "Select Constituency", width: 200, height: 30});

            // PARTY
            $("#selectParty").jqxDropDownList({theme: "arctic", filterable: true, filterPlaceHolder: "Search", placeHolder: "Select Party", width: 200, height: 30});

            $("#selectStateSW").on('select', function (event) {
                var state = $("#selectStateSW").val();
//                var year = $("#selectElectionYearSW").val()
                var acpc = $("#selectACorPCSW").val();

                $("#selectConstituencySW").jqxDropDownList({source: []})
                $("#selectParty").jqxDropDownList({source: []})


                var dataObj = {};
                dataObj['optionsLabel'] = "YEAR";
                dataObj['optionsTable'] = "IS_ELECTION_RESULTS";

                var whereClauseArray = [];
                var whereClauseObj = {};
                whereClauseObj['whereClauseColumn'] = "STATE";
                whereClauseObj['whereClauseColumnValue'] = state;
                whereClauseArray.push(whereClauseObj)

                var whereClauseObj2 = {};
                whereClauseObj2['whereClauseColumn'] = "PC_AC";
                whereClauseObj2['whereClauseColumnValue'] = acpc;
                whereClauseArray.push(whereClauseObj2)

                dataObj['whereClauseArray'] = whereClauseArray;
                generateOptionsSource("selectElectionYearSW", dataObj);

                var year = $("#selectElectionYearSW").val()
                $("#strongBoothTab").trigger("click");
                $(".strongWeakTabsSummary").html("");
//                boothSummary(year, acpc, state)
            });

            $("#selectACorPCSW").on('select', function (event) {
                var state = $("#selectStateSW").val();
                var acpc = $("#selectACorPCSW").val();
                $("#selectConstituencySW").jqxDropDownList({source: []})
                $("#selectParty").jqxDropDownList({source: []})

//                var year = $("#selectElectionYear").val();

                var dataObj = {};
                dataObj['optionsLabel'] = "YEAR";
                dataObj['optionsTable'] = "IS_ELECTION_RESULTS";

                var whereClauseArray = [];
                var whereClauseObj1 = {};
                whereClauseObj1['whereClauseColumn'] = "STATE";
                whereClauseObj1['whereClauseColumnValue'] = state;
                whereClauseArray.push(whereClauseObj1);

                var whereClauseObj2 = {};
                whereClauseObj2['whereClauseColumn'] = "PC_AC";
                whereClauseObj2['whereClauseColumnValue'] = acpc;
                whereClauseArray.push(whereClauseObj2)

                dataObj['whereClauseArray'] = whereClauseArray;
                generateOptionsSource("selectElectionYearSW", dataObj);

                var year = $("#selectElectionYearSW").val();
                $("#strongBoothTab").trigger("click");
                $(".strongWeakTabsSummary").html("");
//                boothSummary(year, acpc, state)
            });

            $("#selectElectionYearSW").on('select', function (event) {
                var state = $("#selectStateSW").val();
                var year = $("#selectElectionYearSW").val()
                var acpc = $("#selectACorPCSW").val()
                $("#strongBoothTab").trigger("click");
                $("#selectConstituencySW").jqxDropDownList({source: []})
                $("#selectParty").jqxDropDownList({source: []})


                var dataObj = {};
                dataObj['optionsLabel'] = "PC_AC_NAME";
                dataObj['optionsTable'] = "IS_POLLING_BOOTH";
//                dataObj['includeSelectOption'] = "Y";

                var whereClauseArray = [];
                var whereClauseObj1 = {};
                whereClauseObj1['whereClauseColumn'] = "STATE";
                whereClauseObj1['whereClauseColumnValue'] = state;
                whereClauseArray.push(whereClauseObj1);

                var whereClauseObj2 = {};
                whereClauseObj2['whereClauseColumn'] = "PC_AC";
                whereClauseObj2['whereClauseColumnValue'] = acpc;
                whereClauseArray.push(whereClauseObj2)

                var whereClauseObj3 = {};
                whereClauseObj3['whereClauseColumn'] = "YEAR";
                whereClauseObj3['whereClauseColumnValue'] = year;
                whereClauseArray.push(whereClauseObj3)

                dataObj['whereClauseArray'] = whereClauseArray;
                dataObj['boothData'] = "Y";
                generateOptionsSource("selectConstituencySW", dataObj);
                $("#strongBoothTab").trigger("click");
                $(".strongWeakTabsSummary").html("");
//                var constituency = $("#selectConstituency").val();
//                boothSummary(year, acpc, state)
            });

            $("#selectConstituencySW").on('select', function (event) {
                var state = $("#selectStateSW").val();
                var year = $("#selectElectionYearSW").val();
                var acpc = $("#selectACorPCSW").val();
                var constituency = $("#selectConstituencySW").val();
                $("#selectParty").jqxDropDownList({source: []})

                var dataObj = {};
                dataObj['optionsLabel'] = "PARTY";
                dataObj['optionsTable'] = "IS_POLLING_BOOTH";
//                dataObj['includeSelectOption'] = "Y";

                var whereClauseArray = [];
                var whereClauseObj1 = {};
                whereClauseObj1['whereClauseColumn'] = "STATE";
                whereClauseObj1['whereClauseColumnValue'] = state;
                whereClauseArray.push(whereClauseObj1);

                var whereClauseObj2 = {};
                whereClauseObj2['whereClauseColumn'] = "PC_AC";
                whereClauseObj2['whereClauseColumnValue'] = acpc;
                whereClauseArray.push(whereClauseObj2)

                var whereClauseObj3 = {};
                whereClauseObj3['whereClauseColumn'] = "YEAR";
                whereClauseObj3['whereClauseColumnValue'] = year;
                whereClauseArray.push(whereClauseObj3)

                var whereClauseObj4 = {};
                whereClauseObj4['whereClauseColumn'] = "PC_AC_NAME";
                whereClauseObj4['whereClauseColumnValue'] = constituency;
                whereClauseArray.push(whereClauseObj4)

                dataObj['whereClauseArray'] = whereClauseArray;
                dataObj['boothData'] = "Y";
                generateOptionsSource("selectParty", dataObj);
                $("#strongBoothTab").trigger("click");
                $(".strongWeakTabsSummary").html("");
//                    strongWeakBoothsData(year, acpc, state, constituency, party)

            });

            $("#selectParty").on('select', function (event) {
                var state = $("#selectStateSW").val();
                var year = $("#selectElectionYearSW").val();
                var acpc = $("#selectACorPCSW").val();
                var constituency = $("#selectConstituencySW").val();
                var party = $("#selectParty").val();

                strongWeakBoothsData(year, acpc, state, constituency, party);
                $("#strongBoothTab").trigger("click");
            });

            $("#selectElectionYearSW").jqxDropDownList('selectIndex', 0);

            $("#toggleEAtabsButton").click(function (event) {
                if ($(".slimScrollDiv").parent("div").hasClass("col-sm-2")) {
                    $(".slimScrollDiv").parent("div").removeClass();
                    $(".slimScrollDiv").parent("div").css("width", "50px");
                    $(".slimScrollDiv").css("width", "50px");
                    $(".css-889cso").css("margin-left", "50px");
                    $(".nav-link-text").hide();
                    $("#toggleEAtabsButton").find("path").attr("d", "M10.294 9.698a.988.988 0 010-1.407 1.01 1.01 0 011.419 0l2.965 2.94a1.09 1.09 0 010 1.548l-2.955 2.93a1.01 1.01 0 01-1.42 0 .988.988 0 010-1.407l2.318-2.297-2.327-2.307z");

                } else {
                    $(".slimScrollDiv").parent("div").addClass("col-sm-2 col-md-2 col-lg-2");
                    $(".slimScrollDiv").css("width", "230px");
                    $(".css-889cso").css("margin-left", "230px");
                    $(".nav-link-text").show();
                    $("#toggleEAtabsButton").find("path").attr("d", "M13.706 9.698a.988.988 0 000-1.407 1.01 1.01 0 00-1.419 0l-2.965 2.94a1.09 1.09 0 000 1.548l2.955 2.93a1.01 1.01 0 001.42 0 .988.988 0 000-1.407l-2.318-2.297 2.327-2.307z");
                }
            })
        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
}

function boothSummary(year, acpc, state, constituency, booth) {
    if (booth != null) {
        var dataObj = {};
        var query = "SELECT CANDIDATE, PARTY, VOTES_POLLED, VOTES_PCT FROM IS_POLLING_BOOTH "
                + " WHERE YEAR='" + year + "' AND PC_AC='" + acpc + "' AND STATE='" + state + "'  "
                + " AND PC_AC_NAME='" + constituency + "' AND POLLING_BOOTH='" + booth + "'";
        dataObj['query'] = query;
        var columnLabelsList = ['Candidate', 'Party', 'Votes Polled', 'Votes percent'];
        var columnList = ['CANDIDATE', 'PARTY', 'VOTES_POLLED', 'VOTES_PCT'];
        dataObj['columnLabelsList'] = columnLabelsList;
//        dataObj['heading'] = 'Polling Booths';
        dataObj['columnList'] = columnList;
        dataObj['columnWidthArray'] = [300, 300, 300, 300];
        dataObj['filterTable'] = "Y";
        dataObj['sortTable'] = "Y";
        createDataTable("dataDiv1", dataObj);
    } else if (constituency != null) {
        var dataObj = {};
        var query = "SELECT CANDIDATE, PARTY, SUM(VOTES_POLLED), ROUND(AVG(VOTES_PCT),2) FROM IS_POLLING_BOOTH  "
                + " WHERE YEAR='" + year + "' AND PC_AC='" + acpc + "' AND STATE='" + state + "'  "
                + " AND PC_AC_NAME = '" + constituency + "' "
                + " GROUP BY CANDIDATE, PARTY, PC_AC_NAME";
        dataObj['query'] = query;
        var columnLabelsList = ['Candidate', 'Party', 'Votes Polled', 'Votes percent'];
        var columnList = ['CANDIDATE', 'PARTY', 'VOTES_POLLED', 'VOTES_PCT'];
        dataObj['columnLabelsList'] = columnLabelsList;
//        dataObj['heading'] = 'Polling Booths';
        dataObj['columnList'] = columnList;
        dataObj['columnWidthArray'] = [300, 300, 300, 300];
        dataObj['filterTable'] = "Y";
        dataObj['sortTable'] = "Y";
        createDataTable("dataDiv1", dataObj);
    } else {
        var dataObj = {};
        var query = "SELECT WINNER_PARTY, SUM(WINNER_VOTES) , ROUND(SUM(WINNER_VOTES)/SUM(POLLED_VOTES)*100, 2) AS VOTES_PCT FROM IS_ELECTION_RESULTS "
                + " WHERE YEAR='" + year + "' AND PC_AC='" + acpc + "' AND STATE='" + state + "'  "
                + " GROUP BY WINNER_PARTY";
        dataObj['query'] = query;
        var columnLabelsList = ['Party', 'Votes Polled', 'Votes percent'];
        var columnList = ['WINNER_PARTY', 'WINNER_VOTES', 'VOTES_PCT'];
        dataObj['columnLabelsList'] = columnLabelsList;
//        dataObj['heading'] = 'Polling Booths';
        dataObj['columnList'] = columnList;
        dataObj['columnWidthArray'] = [300, 300, 300];
        dataObj['filterTable'] = "Y";
        dataObj['sortTable'] = "Y";
        createDataTable("dataDiv1", dataObj);
    }

}

function strongWeakBoothsData(year, acpc, state, constituency, party) {

//        switchStrongWeakTabs(, divId)
    // strong booths
    var dataObj = {};

    var query = "SELECT POLLING_BOOTH, VOTES_POLLED, VOTES_PCT FROM IS_POLLING_BOOTH WHERE ( VOTES_POLLED, POLLING_BOOTH ) IN "
            + "( SELECT MAX(TO_NUMBER(VOTES_POLLED)) AS TOT_VOTES,POLLING_BOOTH FROM IS_POLLING_BOOTH "
            + " WHERE YEAR='" + year + "' AND PC_AC='" + acpc + "' AND STATE='" + state + "' AND PC_AC_NAME='" + constituency + "'"
            + "GROUP BY POLLING_BOOTH ) "
            + " AND PARTY = '" + party + "'";
    dataObj['query'] = query;
    var columnLabelsList = ['Booth Name', 'Votes Polled', 'Votes percent'];
    var columnList = ['POLLING_BOOTH', 'VOTES_POLLED', 'VOTES_PCT'];
    dataObj['columnLabelsList'] = columnLabelsList;
    dataObj['columnList'] = columnList;
    createDataTable("strongBoothsContent", dataObj);

    // weak booths
    var dataObj = {};
    var query = "SELECT  POLLING_BOOTH, VOTES_POLLED, VOTES_PCT FROM IS_POLLING_BOOTH WHERE ( VOTES_POLLED, POLLING_BOOTH ) IN "
            + "( SELECT MIN(TO_NUMBER(VOTES_POLLED)) AS TOT_VOTES,POLLING_BOOTH FROM IS_POLLING_BOOTH "
            + " WHERE YEAR='" + year + "' AND PC_AC='" + acpc + "' AND STATE='" + state + "' AND PC_AC_NAME='" + constituency + "'"
            + "GROUP BY POLLING_BOOTH ) "
            + " AND PARTY = '" + party + "'";
    dataObj['query'] = query;
    var columnLabelsList = ['Booth Name', 'Votes Polled', 'Votes percent'];
    var columnList = ['POLLING_BOOTH', 'VOTES_POLLED', 'VOTES_PCT'];
    dataObj['columnLabelsList'] = columnLabelsList;
    dataObj['columnList'] = columnList;
    createDataTable("weakBoothsContent", dataObj);
//        $(".strongWeakTabsSummary").css("display", "none");
    $("#weakBoothsContent").css("display", "none");
}

function getTotalWidth(widthArray) {
    var totalWidth;
    for (var i = 0; i < widthArray.length; i++) {
        totalWidth += widthArray[i];
    }
    return totalWidth;
}


function appendToDataTable(appendDiv, tableParentDiv, content, dataObj) {
    $(".candidateInfo").popover('hide');
    $("#" + appendDiv).html(content);


    $("#" + appendDiv).find(".candidateProfilePic").each(function (i) {
        var $this = $(this);
        loadCandidateProfilePic($this);

    })

    $(".candidateInfo").unbind("mouseenter").on("mouseenter", function (event) {
        clearTimeout(timeOut);
        var $this = this;
        timeOut = setTimeout(function () {
            $(".candidateInfo").popover('hide');
            $($this).popover({
                trigger: "click",
//                title: "Event Timings", 
                content: function () {
                    var candidateName = $($this).closest('td').find('span').text();
                    showPopoverContent(candidateName);
                    return '<div class="popoverContentDiv"><div class="voteAnalysisLoader"></div></div>'
                },
                html: true,
                placement: "right",
//                    height:250px,
            });
//        $(this).on('shown.bs.popover', function () {
////                $(".popover-body").css("width","100px");
////                $(".popover-body").css("height","100px");
//        })
            $($this).popover('show');

            $(".popover").css("max-width", "365px");
            $(".popover").css("min-height", "100px");
        }, 500);
    });

    $("#" + tableParentDiv).find(".candidateInfo").unbind("mouseleave").on("mouseleave", function (event) {
        clearTimeout(timeOut);
    });

    $("#pageBodyContent").unbind("mousedown").on("mousedown", function (event) {
        if (!$(event.target).closest(".popover-body").length > 0) {
            $(".candidateInfo").popover('hide');
        }

    })

    $("#" + tableParentDiv).find('#headerCheckBox').change(function () {
        $("#" + tableParentDiv).find('.tdCheckBox').prop('checked', this.checked);
    });

    $("#" + tableParentDiv).find('.tdCheckBox').change(function () {
        if ($("#" + tableParentDiv).find('.tdCheckBox:checked').length == $('.tdCheckBox').length) {
            $("#" + tableParentDiv).find('#headerCheckBox').prop('checked', true);
        } else {
            $("#" + tableParentDiv).find('#headerCheckBox').prop('checked', false);
        }
    });



    $("#" + tableParentDiv).find(".tdCheckBox, .headerCheckBox").change(function (event) {
        var aggrigateColumns = dataObj['aggrigateColumns'];
        if (aggrigateColumns != null) {
            var columnLabelsList = dataObj['columnLabelsList'];
            for (var i = 0; i < aggrigateColumns.length; i++) {
                var aggrigateVal = 0;
                var persAggrigateVal = 0;
                var column = aggrigateColumns[i];
                var columnIndex = columnLabelsList.indexOf(column);


                if (column.indexOf("Percent") > -1) {
                    $("#" + tableParentDiv).find(".tdCheckBox").each(function (index) {
                        var value = $($(this).closest("tr").find("td")[columnIndex + 1]).text();
                        if (value != null && value != "") {
                            value = value.replace(/,/g, "");
                            value = parseFloat(value);
                            persAggrigateVal += value;
                        }

                    });
                }

                $("#" + tableParentDiv).find(".tdCheckBox:checked").each(function (index) {
                    var value = $($(this).closest("tr").find("td")[columnIndex + 1]).text();
                    if (value != null && value != "") {
                        value = value.replace(/,/g, "");
                        if (column.indexOf("Percent") > -1) {
                            value = parseFloat(value);
                            aggrigateVal = aggrigateVal + (value / persAggrigateVal) * 100;

                        } else {
                            value = parseInt(value);
                            aggrigateVal += value;
                        }

                    }

                });

                if (column.indexOf("Percent") > -1) {

                    aggrigateVal = aggrigateVal.toFixed(2);
                } else {
//                    aggrigateVal = aggrigateVal.toString().replace(/\B(?<!\.\d*)(?=(\d{3})+(?!\d))/g, ",");
                    aggrigateVal = aggrigateVal.toLocaleString();
                }

                $($("#" + tableParentDiv).find("table").find(".aggrigateValuesRow").find("th")[columnIndex + 1]).text(aggrigateVal);

            }
        }
    });

    var table = $("#" + appendDiv).find("table");
    if (table.length > 0) {
        $("#" + appendDiv).find('#headerCheckBox').trigger("click");
    } else {

        $("#" + appendDiv).closest("table").find('#headerCheckBox').prop('checked', false);
        $("#" + appendDiv).closest("table").find('#headerCheckBox').trigger("click");
    }


}



const getRanHex = size => {
  let result = [];
  let hexRef = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'];

  for (let n = 0; n < size; n++) {
    result.push(hexRef[Math.floor(Math.random() * 16)]);
  }
  return result.join('');
}