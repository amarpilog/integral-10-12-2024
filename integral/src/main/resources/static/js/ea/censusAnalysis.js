/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

function loadCensusLayout(censusLayout) {
    $("#CensusSummary").html(censusLayout);
    $("#selectStateCensus").val("TELANGANA");
    var state = $("#selectStateCensus").val();
//    loadCensusMap();
    var map = initMapCensus(state, "censusMapViewDiv");

    loadStateCensusDetails("TELANGANA");
    // State
    var dataObj = {};
    dataObj['optionsLabel'] = "STATE";
    dataObj['optionsTable'] = "CENSUS_DATA";

    $("#selectStateCensus").jqxDropDownList({theme: "arctic", filterable: true, filterPlaceHolder: "Search", placeHolder: "Select State", width: 200, height: 30});
    generateOptionsSource("selectStateCensus", dataObj);
    $("#selectStateCensus").jqxDropDownList('selectItem', "TELANGANA");

    //District
    var dataObj = {};
    dataObj['optionsLabel'] = "DISTRICT";
    dataObj['optionsTable'] = "CENSUS_DATA";
//            dataObj['includeSelectOption'] = "Y";
    var whereClauseArray = [];
    var whereClauseObj = {};
    whereClauseObj['whereClauseColumn'] = 'STATE';
    whereClauseObj['whereClauseColumnValue'] = 'TELANGANA';
    whereClauseArray.push(whereClauseObj);

    $("#selectDistrictCensus").jqxDropDownList({theme: "arctic", filterable: true, filterPlaceHolder: "Search", placeHolder: "Select District", width: 200, height: 30});
    generateOptionsSource("selectDistrictCensus", dataObj);

    // Mandal

    $("#selectMandalCensus").jqxDropDownList({theme: "arctic", filterable: true, filterPlaceHolder: "Search", placeHolder: "Select SubDistrict", width: 200, height: 30});

    // Village

    $("#selectVillageCensus").jqxDropDownList({theme: "arctic", filterable: true, filterPlaceHolder: "Search", placeHolder: "Select Village", width: 200, height: 30});

    $("#selectStateCensus").on('select', function (event) {
        var state = $("#selectStateCensus").val();
//                var district = $("#selectDistrictCensus").val()
//                var mandal = $("#selectMandalCensus").val()
//                var village = $("#selectVillageCensus").val()

        $("#selectMandalCensus").jqxDropDownList({source: []})
        $("#selectVillageCensus").jqxDropDownList({source: []})

        var dataObj = {};
        dataObj['optionsLabel'] = "DISTRICT";
        dataObj['optionsTable'] = "CENSUS_DATA";
//        dataObj['includeSelectOption'] = "Y";

        var whereClauseArray = [];
        var whereClauseObj = {};
        whereClauseObj['whereClauseColumn'] = "STATE";
        whereClauseObj['whereClauseColumnValue'] = state;
        whereClauseArray.push(whereClauseObj)


        dataObj['whereClauseArray'] = whereClauseArray;
        generateOptionsSource("selectDistrictCensus", dataObj);

        loadStateCensusDetails(state)
    });

    $("#selectDistrictCensus").on('select', function (event) {
        var mapLoad = false;
        var state = $("#selectStateCensus").val();
        var district = $("#selectDistrictCensus").val()
//                var mandal = $("#selectMandalCensus").val()
//                var village = $("#selectVillageCensus").val()

        $("#selectMandalCensus").jqxDropDownList({source: []})
        $("#selectVillageCensus").jqxDropDownList({source: []})

        var dataObj = {};

        dataObj['optionsLabel'] = "SUB_DISTRICT";
        dataObj['optionsTable'] = "CENSUS_DATA";
//        dataObj['includeSelectOption'] = "Y";

        var whereClauseArray = [];
        var whereClauseObj = {};
        whereClauseObj['whereClauseColumn'] = "STATE";
        whereClauseObj['whereClauseColumnValue'] = state;
        whereClauseArray.push(whereClauseObj)


        var whereClauseObj2 = {};
        whereClauseObj2['whereClauseColumn'] = "DISTRICT";
        whereClauseObj2['whereClauseColumnValue'] = district;
        whereClauseArray.push(whereClauseObj2)

        dataObj['whereClauseArray'] = whereClauseArray;
        generateOptionsSource("selectMandalCensus", dataObj);

        loadDistrictCensusDetails(state, district)
        map.setZoom(8);
        map.data.setStyle(function (feature) {
            var fillOpacity = 0.5
            if (!mapLoad && feature.getProperty("Dist_Name").toUpperCase() == district) {
                var geometry = feature.getGeometry();
                var position = geometry['g'][0]['g'][0];
                map.setCenter(position);
                fillOpacity = 0.7;
                mapLoad = true;
            }
            return {
                fillColor: 'orange',
                strokeColor: 'white',
                fillOpacity: fillOpacity,
                strokeWeight: 1
            }
        });
    });

    $("#selectMandalCensus").on('select', function (event) {
        var mapLoad = false;
        var state = $("#selectStateCensus").val();
        var district = $("#selectDistrictCensus").val()
        var mandal = $("#selectMandalCensus").val()
//      var village = $("#selectVillageCensus").val()

        $("#selectVillageCensus").jqxDropDownList({source: []})

        var dataObj = {};

        dataObj['optionsLabel'] = "VILLAGE";
        dataObj['optionsTable'] = "CENSUS_DATA";
//        dataObj['includeSelectOption'] = "Y";

        var whereClauseArray = [];
        var whereClauseObj = {};
        whereClauseObj['whereClauseColumn'] = "STATE";
        whereClauseObj['whereClauseColumnValue'] = state;
        whereClauseArray.push(whereClauseObj)


        var whereClauseObj2 = {};
        whereClauseObj2['whereClauseColumn'] = "DISTRICT";
        whereClauseObj2['whereClauseColumnValue'] = district;
        whereClauseArray.push(whereClauseObj2)


        var whereClauseObj3 = {};
        whereClauseObj3['whereClauseColumn'] = "SUB_DISTRICT";
        whereClauseObj3['whereClauseColumnValue'] = mandal;
        whereClauseArray.push(whereClauseObj3)

        dataObj['whereClauseArray'] = whereClauseArray;
        generateOptionsSource("selectVillageCensus", dataObj);

        loadMandalCensusDetails(state, district, mandal);

        map.setZoom(11);
        console.log("waiting stated");

        map.data.setStyle(function (feature) {
            var fillOpacity = 0.5
            if (!mapLoad && feature.getProperty("Dist_Name") != null && feature.getProperty("Dist_Name").toUpperCase() == district
                    && feature.getProperty("Sub_Dist_Name") != null && feature.getProperty("Sub_Dist_Name").toUpperCase() == mandal) {
                var geometry = feature.getGeometry();
                var position = geometry['g'][0]['g'][0]['g'];
                if (position != null) {
                    position = geometry['g'][0]['g'][0]['g'][0];
                } else {
                    position = geometry['g'][0]['g'][0];
                }
                map.setCenter(position);
                fillOpacity = 0.7;
                mapLoad = true;
            }
            return {
                fillColor: 'orange',
                strokeColor: 'white',
                fillOpacity: fillOpacity,
                strokeWeight: 1
            }
        });

//        map.data.forEach(function (feature) {
//            var subDist = feature.getProperty("Sub_Dist_Name");
//            if (subDist.toUpperCase() == mandal) {
//                var geometry = feature.getGeometry();
//                var position = geometry['g'][0]['g'][0];
//                map.setCenter(position);
////                  feature.setStyle
//                map.data.setStyle(function (featureInn) {
//                    var fillOpacity = 0.5
//                    if (featureInn.getProperty("Sub_Dist_Name").toUpperCase() == mandal) {
//
//                        fillOpacity = 0.7;
//                    }
//                    return {
//                        fillColor: 'orange',
//                        strokeColor: 'white',
//                        fillOpacity: fillOpacity,
//                        strokeWeight: 1
//                    }
//                });
//            }
//
//
//        });
    });

    $("#selectVillageCensus").on('select', function (event) {
        var mapLoad = false;
        var state = $("#selectStateCensus").val();
        var district = $("#selectDistrictCensus").val()
        var mandal = $("#selectMandalCensus").val()
        var village = $("#selectVillageCensus").val()

        loadVillageCensusDetails(state, district, mandal, village);
        console.log("started");
        map.setZoom(14);

        map.data.setStyle(function (feature) {

            var fillOpacity = 0.5
            if (!mapLoad && feature.getProperty("Dist_Name") != null && feature.getProperty("Dist_Name").toUpperCase() == district
                    && feature.getProperty("Sub_Dist_Name") != null && feature.getProperty("Sub_Dist_Name").toUpperCase() == mandal
                    && feature.getProperty("Village_Name") != null && feature.getProperty("Village_Name").toUpperCase() == village) {
                var geometry = feature.getGeometry();
                var position = geometry['g'][0]['g'][0]['g'];
                if (position != null) {
                    position = geometry['g'][0]['g'][0]['g'][0];
                } else {
                    position = geometry['g'][0]['g'][0];
                }
                map.setCenter(position);
                fillOpacity = 0.7;
                mapLoad = true;
//                            resolve('wait over');
            }
            return {
                fillColor: 'orange',
                strokeColor: 'white',
                fillOpacity: fillOpacity,
                strokeWeight: 1
            }

        });


//         map.data.forEach(function (feature) {
//          
//            if (feature.getProperty("Village_Name").toUpperCase() == village) {
//                var geometry = feature.getGeometry();
//                var position = geometry['g'][0]['g'][0];
//                map.setCenter(position);
////                  feature.setStyle
//                map.data.setStyle(function (featureInn) {
//                    var fillOpacity = 0.5
//                    if (featureInn.getProperty("Village_Name").toUpperCase() == village) {
//                        fillOpacity = 0.7;
//                    }
//                    return {
//                        fillColor: 'orange',
//                        strokeColor: 'white',
//                        fillOpacity: fillOpacity,
//                        strokeWeight: 1
//                    }
//                });
//            }
//        });
    });
}

function loadStateCensusDetails(state) {
    $("#censusChartDiv").show();
    var chartObj1 = {};
    var query = "SELECT DISTRICT, SUM(TOTAL_POPULATION) FROM CENSUS_DATA WHERE STATE='" + state + "' GROUP BY DISTRICT";
    chartObj1['query'] = query;
    chartObj1['heading'] = 'State : ' + state.initCap();
    chartObj1['title'] = 'District wise population';
//    createChartVoteAnalysis("censusChartDiv", chartObj1);

    $("#censusAggDetails").show();
    var censusDataObj = {};
    var queries = {};
    queries['No of Mandals'] = "SELECT COUNT(DISTINCT SUB_DISTRICT) FROM CENSUS_DATA WHERE STATE='" + state + "'";
    queries['No of Villages'] = "SELECT COUNT(VILLAGE) FROM CENSUS_DATA WHERE STATE='" + state + "'";
    queries['Total Households'] = "SELECT SUM(NUMBER_OF_HOUSEHOLDS) FROM CENSUS_DATA WHERE STATE='" + state + "'";
    queries['Total Population'] = "SELECT SUM(TOTAL_POPULATION) FROM CENSUS_DATA WHERE STATE='" + state + "'";
    queries['Male Population'] = "SELECT SUM(MALE_POPULATION) FROM CENSUS_DATA WHERE STATE='" + state + "'";
    queries['Female Population'] = "SELECT SUM(FEMALE_POPULATION) FROM CENSUS_DATA WHERE STATE='" + state + "'";
    queries['SC Population'] = "SELECT SUM(SC_POPULATION) FROM CENSUS_DATA WHERE STATE='" + state + "'";
    queries['ST Population'] = "SELECT SUM(ST_POPULATION) FROM CENSUS_DATA WHERE STATE='" + state + "'";
    queries['Literates'] = "SELECT SUM(LITERATES) FROM CENSUS_DATA WHERE STATE='" + state + "'";
    queries['Illiterates'] = "SELECT SUM(ILLITERATES) FROM CENSUS_DATA WHERE STATE='" + state + "'";
    queries['Literacy Rate'] = "SELECT ROUND((SUM(ILLITERATES)/SUM(TOTAL_POPULATION))*100,2) FROM CENSUS_DATA WHERE STATE='" + state + "'";

    censusDataObj['queryColumns'] = ['No of Mandals', 'No of Villages', 'Total Households', 'Total Population',
        'Male Population', 'Female Population', 'SC Population', 'ST Population', 'Literates', 'Illiterates',
        'Literacy Rate'];

    censusDataObj['queries'] = queries;
    censusDataObj['heading'] = state.initCap() + " Census Data";
    createCensusDataTable("censusAggDetails", censusDataObj);

    $("#censusDataTableDiv").show();
    var dataObj = {};
    var query = "SELECT  "
            + " DISTRICT, SUM(NUMBER_OF_HOUSEHOLDS), SUM(TOTAL_POPULATION), SUM(MALE_POPULATION),"
            + " SUM(FEMALE_POPULATION), SUM(SC_POPULATION), SUM(ST_POPULATION),  SUM(LITERATES), SUM(ILLITERATES), "
            + " SUM(TOTAL_WORKERS), SUM(CULTIVATORS), SUM(AGRICULTURAL_LABOURERS), SUM(HOUSEHOLD_INDUSTRIES) "
            + " FROM CENSUS_DATA WHERE STATE='" + state + "' GROUP BY DISTRICT";
    dataObj['query'] = query;
    dataObj['heading'] = "State" + " : " + state.initCap();
    var columnLabelsList = ['District', 'Total Households', 'Tot Population', 'Male Population', 'Female population', 'SC Population', 'ST Population', 'Literates', 'Illiterates', 'Tot Workers', 'Cultivators', 'Argricultural Labours', 'Household Industries'];
    var columnList = ['DISTRICT', 'NUMBER_OF_HOUSEHOLDS', 'TOTAL_POPULATION', 'MALE_POPULATION', 'FEMALE_POPULATION', 'SC_POPULATION', 'ST_POPULATION', 'LITERATES', 'ILLITERATES', 'TOTAL_WORKERS', 'CULTIVATORS', 'AGRICULTURAL_LABOURERS', 'HOUSEHOLD_INDUSTRIES'];
    dataObj['columnLabelsList'] = columnLabelsList;
    dataObj['columnList'] = columnList;
    dataObj['columnWidthArray'] = [150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150];
    dataObj['filterTable'] = "Y";
    dataObj['sortTable'] = "Y";
    dataObj['dataTableDataType'] = "CENSUS";
    dataObj['checkBoxColumn'] = "Y";
    dataObj['aggrigateColumns'] = ['Total Households', 'Tot Population', 'Male Population', 'Female population', 'SC Population', 'ST Population', 'Literates', 'Illiterates', 'Tot Workers', 'Cultivators', 'Argricultural Labours', 'Household Industries'];

    createDataTable("censusDataTableDiv", dataObj);
}

function loadDistrictCensusDetails(state, district) {
    $("#censusChartDiv").show();
    var chartObj1 = {};
    var query = "SELECT SUB_DISTRICT, SUM(TOTAL_POPULATION) FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' GROUP BY SUB_DISTRICT";
    chartObj1['query'] = query;
    chartObj1['heading'] = 'District : ' + district.initCap();
    chartObj1['title'] = 'Mandal wise population';
//    createChartVoteAnalysis("censusChartDiv", chartObj1);

    $("#censusAggDetails").show();
    var censusDataObj = {};
    var queries = {};
    queries['No of Mandals'] = "SELECT COUNT(DISTINCT SUB_DISTRICT) FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "'";
    queries['No of Villages'] = "SELECT COUNT(VILLAGE) FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "'";
    queries['Total Households'] = "SELECT SUM(NUMBER_OF_HOUSEHOLDS) FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "'";
    queries['Total Population'] = "SELECT SUM(TOTAL_POPULATION) FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "'";
    queries['Male Population'] = "SELECT SUM(MALE_POPULATION) FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "'";
    queries['Female Population'] = "SELECT SUM(FEMALE_POPULATION) FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "'";
    queries['SC Population'] = "SELECT SUM(SC_POPULATION) FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "'";
    queries['ST Population'] = "SELECT SUM(ST_POPULATION) FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "'";
    queries['Literates'] = "SELECT SUM(LITERATES) FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "'";
    queries['Illiterates'] = "SELECT SUM(ILLITERATES) FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "'";
    queries['Literacy Rate'] = "SELECT ROUND((SUM(ILLITERATES)/SUM(TOTAL_POPULATION))*100,2) FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "'";

    censusDataObj['queryColumns'] = ['No of Mandals', 'No of Villages', 'Total Households', 'Total Population',
        'Male Population', 'Female Population', 'SC Population', 'ST Population', 'Literates', 'Illiterates',
        'Literacy Rate'];

    censusDataObj['queries'] = queries;
    censusDataObj['heading'] = district.initCap() + " Census Data";
    createCensusDataTable("censusAggDetails", censusDataObj);

    $("#censusDataTableDiv").show();
    var dataObj = {};
    var query = "SELECT  "
            + " SUB_DISTRICT, SUM(NUMBER_OF_HOUSEHOLDS), SUM(TOTAL_POPULATION), SUM(MALE_POPULATION),"
            + " SUM(FEMALE_POPULATION), SUM(SC_POPULATION), SUM(ST_POPULATION),  SUM(LITERATES), SUM(ILLITERATES), "
            + " SUM(TOTAL_WORKERS), SUM(CULTIVATORS), SUM(AGRICULTURAL_LABOURERS), SUM(HOUSEHOLD_INDUSTRIES) "
            + " FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' GROUP BY SUB_DISTRICT";
    dataObj['query'] = query;
    dataObj['heading'] = "District" + " : " + district.initCap();
    var columnLabelsList = ['Sub District', 'Total Households', 'Tot Population', 'Male Population', 'Female population', 'SC Population', 'ST Population', 'Literates', 'Illiterates', 'Tot Workers', 'Cultivators', 'Argricultural Labours', 'Household Industries'];
    var columnList = ['SUB_DISTRICT', 'NUMBER_OF_HOUSEHOLDS', 'TOTAL_POPULATION', 'MALE_POPULATION', 'FEMALE_POPULATION', 'SC_POPULATION', 'ST_POPULATION', ' LITERATES', 'ILLITERATES', 'TOTAL_WORKERS', 'CULTIVATORS', 'AGRICULTURAL_LABOURERS', 'HOUSEHOLD_INDUSTRIES'];
    dataObj['columnLabelsList'] = columnLabelsList;
    dataObj['columnList'] = columnList;
    dataObj['columnWidthArray'] = [150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150];

    dataObj['filterTable'] = "Y";
    dataObj['sortTable'] = "Y";
    dataObj['dataTableDataType'] = "CENSUS";
    dataObj['checkBoxColumn'] = "Y";
    dataObj['aggrigateColumns'] = ['Total Households', 'Tot Population', 'Male Population', 'Female population', 'SC Population', 'ST Population', 'Literates', 'Illiterates', 'Tot Workers', 'Cultivators', 'Argricultural Labours', 'Household Industries'];
    createDataTable("censusDataTableDiv", dataObj);
}


function loadMandalCensusDetails(state, district, mandal) {
    $("#censusChartDiv").show();
    var chartObj1 = {};
    var query = "SELECT VILLAGE, SUM(TOTAL_POPULATION) FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' AND SUB_DISTRICT='" + mandal + "' GROUP BY VILLAGE";
    chartObj1['query'] = query;
    chartObj1['heading'] = 'Sub District : ' + mandal.initCap();
    chartObj1['title'] = 'Village wise population';
//    createChartVoteAnalysis("censusChartDiv", chartObj1);

    $("#censusAggDetails").show();
    var censusDataObj = {};
    var queries = {};
    queries['District'] = "SELECT DISTRICT FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' AND SUB_DISTRICT='" + mandal + "' ";
    queries['No of Villages'] = "SELECT COUNT(VILLAGE) FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' AND SUB_DISTRICT='" + mandal + "' ";
    queries['Total Households'] = "SELECT SUM(NUMBER_OF_HOUSEHOLDS) FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' AND SUB_DISTRICT='" + mandal + "' ";
    queries['Total Population'] = "SELECT SUM(TOTAL_POPULATION) FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' AND SUB_DISTRICT='" + mandal + "' ";
    queries['Male Population'] = "SELECT SUM(MALE_POPULATION) FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' AND SUB_DISTRICT='" + mandal + "' ";
    queries['Female Population'] = "SELECT SUM(FEMALE_POPULATION) FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' AND SUB_DISTRICT='" + mandal + "' ";
    queries['SC Population'] = "SELECT SUM(SC_POPULATION) FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' AND SUB_DISTRICT='" + mandal + "' ";
    queries['ST Population'] = "SELECT SUM(ST_POPULATION) FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' AND SUB_DISTRICT='" + mandal + "' ";
    queries['Literates'] = "SELECT SUM(LITERATES) FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' AND SUB_DISTRICT='" + mandal + "' ";
    queries['Illiterates'] = "SELECT SUM(ILLITERATES) FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' AND SUB_DISTRICT='" + mandal + "' ";
    queries['Literacy Rate'] = "SELECT ROUND((SUM(ILLITERATES)/SUM(TOTAL_POPULATION))*100,2) FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' AND SUB_DISTRICT='" + mandal + "' ";

    censusDataObj['queryColumns'] = ['District', 'No of Villages', 'Total Households', 'Total Population',
        'Male Population', 'Female Population', 'SC Population', 'ST Population', 'Literates', 'Illiterates',
        'Literacy Rate'];

    censusDataObj['queries'] = queries;
    censusDataObj['heading'] = mandal.initCap() + " Census Data";
    createCensusDataTable("censusAggDetails", censusDataObj);

    $("#censusDataTableDiv").show();
    var dataObj = {};
    var query = "SELECT  "
            + " VILLAGE, SUM(NUMBER_OF_HOUSEHOLDS), SUM(TOTAL_POPULATION), SUM(MALE_POPULATION),"
            + " SUM(FEMALE_POPULATION), SUM(SC_POPULATION), SUM(ST_POPULATION),  SUM(LITERATES), SUM(ILLITERATES), "
            + " SUM(TOTAL_WORKERS), SUM(CULTIVATORS), SUM(AGRICULTURAL_LABOURERS), SUM(HOUSEHOLD_INDUSTRIES) "
            + " FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "'  AND SUB_DISTRICT='" + mandal + "' GROUP BY VILLAGE";
    dataObj['query'] = query;
    dataObj['heading'] = "Sub District" + " : " + mandal.initCap();

    var columnLabelsList = ['Village', 'Total Households', 'Tot Population', 'Male Population', 'Female population', 'SC Population', 'ST Population', 'Literates', 'Illiterates', 'Tot Workers', 'Cultivators', 'Argricultural Labours', 'Household Industries'];

    var columnList = ['VILLAGE', 'NUMBER_OF_HOUSEHOLDS', 'TOTAL_POPULATION', 'MALE_POPULATION', 'FEMALE_POPULATION', 'SC_POPULATION', 'ST_POPULATION', ' LITERATES', 'ILLITERATES', 'TOTAL_WORKERS', 'CULTIVATORS', 'AGRICULTURAL_LABOURERS', 'HOUSEHOLD_INDUSTRIES'];
    dataObj['columnLabelsList'] = columnLabelsList;
    dataObj['columnList'] = columnList;
    dataObj['columnWidthArray'] = [150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150];

    dataObj['filterTable'] = "Y";
    dataObj['sortTable'] = "Y";
    dataObj['dataTableDataType'] = "CENSUS";
    dataObj['checkBoxColumn'] = "Y";
    dataObj['aggrigateColumns'] = ['Total Households', 'Tot Population', 'Male Population', 'Female population', 'SC Population', 'ST Population', 'Literates', 'Illiterates', 'Tot Workers', 'Cultivators', 'Argricultural Labours', 'Household Industries'];

    createDataTable("censusDataTableDiv", dataObj);
}

function loadVillageCensusDetails(state, district, mandal, village) {
    $("#censusChartDiv").show();
    var chartObj1 = {};
    var query = "SELECT VILLAGE, SUM(TOTAL_POPULATION) FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' AND SUB_DISTRICT='" + mandal + "' AND VILLAGE='" + village + "' GROUP BY VILLAGE";
    chartObj1['query'] = query;
    chartObj1['heading'] = 'Village : ' + village.initCap();
    chartObj1['title'] = 'Village population';
//    createChartVoteAnalysis("censusChartDiv", chartObj1);

    $("#censusAggDetails").show();
    var censusDataObj = {};
    var queries = {};
    queries['District'] = "SELECT DISTRICT FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' AND SUB_DISTRICT='" + mandal + "' AND VILLAGE='" + village + "'";
    queries['Sub District'] = "SELECT SUB_DISTRICT FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' AND SUB_DISTRICT='" + mandal + "' AND VILLAGE='" + village + "'";
    queries['Total Households'] = "SELECT NUMBER_OF_HOUSEHOLDS FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' AND SUB_DISTRICT='" + mandal + "' AND VILLAGE='" + village + "'";
    queries['Total Households'] = "SELECT NUMBER_OF_HOUSEHOLDS FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' AND SUB_DISTRICT='" + mandal + "' AND VILLAGE='" + village + "'";
    queries['Total Population'] = "SELECT TOTAL_POPULATION FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' AND SUB_DISTRICT='" + mandal + "' AND VILLAGE='" + village + "' ";
    queries['Male Population'] = "SELECT MALE_POPULATION FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' AND SUB_DISTRICT='" + mandal + "'  AND VILLAGE='" + village + "'";
    queries['Female Population'] = "SELECT FEMALE_POPULATION FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' AND SUB_DISTRICT='" + mandal + "' AND VILLAGE='" + village + "' ";
    queries['SC Population'] = "SELECT SC_POPULATION FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' AND SUB_DISTRICT='" + mandal + "'  AND VILLAGE='" + village + "'";
    queries['ST Population'] = "SELECT ST_POPULATION FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' AND SUB_DISTRICT='" + mandal + "'  AND VILLAGE='" + village + "'";
    queries['Literates'] = "SELECT LITERATES FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' AND SUB_DISTRICT='" + mandal + "'  AND VILLAGE='" + village + "'";
    queries['Illiterates'] = "SELECT ILLITERATES FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' AND SUB_DISTRICT='" + mandal + "'  AND VILLAGE='" + village + "'";
    queries['Literacy Rate'] = "SELECT ROUND((ILLITERATES/TOTAL_POPULATION)*100,2) FROM CENSUS_DATA WHERE STATE='" + state + "' AND DISTRICT='" + district + "' AND SUB_DISTRICT='" + mandal + "'  AND VILLAGE='" + village + "'";

    censusDataObj['queryColumns'] = ['District', 'Sub District', 'Total Households', 'Total Population',
        'Male Population', 'Female Population', 'SC Population', 'ST Population', 'Literates', 'Illiterates',
        'Literacy Rate'];

    censusDataObj['queries'] = queries;
    censusDataObj['heading'] = village.initCap() + " Census Data";
    createCensusDataTable("censusAggDetails", censusDataObj);

    $("#censusDataTableDiv").hide();
}


function getValueCensusData(value, column) {
    column = column.replace(/\u00a0/g, " ");
    if (column == "District") {
        $("#selectDistrictCensus").val(value);
        $("#selectDistrictCensus").trigger("select");

    } else if (column == "Sub District" || column == "Mandal") {
        $("#selectMandalCensus").val(value);
        $("#selectMandalCensus").trigger("select");
    } else if (column == "Village") {

        $("#selectVillageCensus").val(value);
        var value = $("#selectVillageCensus").val();

        $("#selectVillageCensus").trigger("select");
    }
}

function loadCensusMap() {
    require([
        "esri/WebMap",
        "esri/Map",
        "esri/layers/GeoJSONLayer",
        "esri/views/MapView",
        "esri/widgets/Legend",
        "esri/PopupTemplate",
        "esri/widgets/Editor",
        "esri/widgets/Expand",
        "esri/request",
    ], (
            WebMap,
            Map,
            GeoJSONLayer,
            MapView,
            Legend,
            PopupTemplate,
            Editor,
            Expand,
            esriRequest
            ) => {

        var selectedFeature;

        var censusMandalGeoJsonLayer;
        var censusDistrictGeoJsonLayer;
        var censusVillageGeoJsonLayer;
        var globalScale = 1000000;
        var state = $("#selectStateCensus").val();
        loadDistrictCensusMap(state);

        function loadDistrictCensusMap(state) {

//        const url = "js/geojson/Telangana_districts.json";
            var query = "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='DISTRICTS' AND YEAR='2018'";
            const url = "getGeoJsonFiles?query=" + query;

            const renderer = getSimpleRenderer("Dist_name", "skyblue");
            const geojsonLayer = new GeoJSONLayer({
                url: url,
                renderer: renderer,
                opacity: 0.8,
                title: "Districts"
            });
            censusDistrictGeoJsonLayer = geojsonLayer;

            const map = new Map({
//            basemap: "gray-vector",
                layers: [geojsonLayer]
            });

            const view = new MapView({
                container: "censusMapViewDiv",
//            highlightOptions: {
//            color: [255, 241, 58],
//            fillOpacity: 0.4
//          },
                center: [79, 18],
                zoom: 7,
                map: map
            });


            geojsonLayer.labelingInfo = [getLabelClass('Dist_Name')];

            const legend = addLegend(Expand, Legend, view, "bottom-left");

            const template = getPopupTemplate(PopupTemplate);
            geojsonLayer.popupTemplate = template;

            view.popup.watch("selectedFeature", (graphic) => {
                console.log("selected Feature Dist_Name");
                if (graphic && graphic.attributes['Dist_Name'] != null
                        && selectedFeature != graphic && graphic.attributes['Dist_Name']
                        && globalScale > 500000) {
                    loadDistrictCensusDetails(state, graphic.attributes['Dist_Name'].toUpperCase());
                }
            });

            view.whenLayerView(geojsonLayer).then((layerView) => {   // loadDistricts
                console.log("layer loaded")
                setTimeout(function () {
                    $(".esri-legend__symbol").find("path").attr("d", "M 10,-10 L 10,0 L 10,10 L -10,10 L -10,-10 Z");
                }, 600);
            });

            //	view.popup.viewModel.autoOpenEnabled = false;

// ZOOOMING
            var previousScale;
            view.watch("scale", (newValue) => {
                console.log("scale :: " + newValue)

                if (previousScale > 500000 && newValue < 500000) {
                    loadMandalsCensusMap(view, map, legend, state);
                }

                if (previousScale < 500000 && newValue > 500000) {
                    view.map.layers = [];
                    view.map.layers.push(geojsonLayer);

                }

                previousScale = newValue;
                globalScale = newValue;
            });
        }

        function loadMandalsCensusMap(view, map, legend, state) {

            var query = "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='SUB_DISTRICTS' AND YEAR='2018'";
            const url = "getGeoJsonFiles?query=" + query;

            const renderer = getSimpleRenderer("Sub_Dist_Name", "#90EE90");
            var geojsonLayer;
            if (censusMandalGeoJsonLayer == null) {
                showEAMapLoader();
                geojsonLayer = new GeoJSONLayer({
                    url: url,
                    renderer: renderer,
                    opacity: 0.8,
                    title: "Mandals"
                });

                censusMandalGeoJsonLayer = geojsonLayer;


                view.popup.watch("selectedFeature", (graphic) => {
                    console.log("selectedFeatureeeee");
                    if (graphic && graphic.attributes['Sub_Dist_Name'] != null
                            && globalScale < 500000 && globalScale > 100000) {
                        console.log("selectedFeatureeeee :: " + graphic.attributes['Sub_Dist_Name']);
                        loadMandalCensusDetails(state, graphic.attributes['Dist_Name'].toUpperCase(), graphic.attributes['Sub_Dist_Name'].toUpperCase())
                    }
                });

                view.whenLayerView(geojsonLayer).then((layerView) => {   // loadDistricts
                    console.log("layer loaded")
                    stopEAMapLoader();
                    setTimeout(function () {
                        $(".esri-legend__symbol").find("path").attr("d", "M 10,-10 L 10,0 L 10,10 L -10,10 L -10,-10 Z");
                    }, 600);
                });

            } else {
                geojsonLayer = censusMandalGeoJsonLayer;
            }
            view.map.layers = [];

            view.map.layers.push(geojsonLayer);

            geojsonLayer.renderer = renderer;
            geojsonLayer.labelingInfo = [getLabelClass('Sub_Dist_Name')];

            legend.view = view;

            const template = getPopupTemplate(PopupTemplate, "Sub_Dist_Name");
            geojsonLayer.popupTemplate = template;



            //	view.popup.viewModel.autoOpenEnabled = false;

// ZOOOMING
            var previousScale;
            view.watch("scale", (newValue) => {
                console.log("scale :: " + newValue)

                if (previousScale > 100000 && newValue < 100000) {
                    loadVillagesCensusMap(view, map, legend, state);
                }

                if (previousScale < 100000 && newValue > 100000) {
                    view.map.layers = [];
                    view.map.layers.push(geojsonLayer);
                }

                previousScale = newValue;
                globalScale = newValue;
            });
        }

        function loadVillagesCensusMap(view, map, legend, state) {

            var query = "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='VILLAGES' AND YEAR='2018'";
            const url = "getGeoJsonFiles?query=" + query;

            const renderer = getSimpleRenderer("Village_Name", "#FAF884");
            var geojsonLayer;
            if (censusVillageGeoJsonLayer == null) {
                showEAMapLoader();
                geojsonLayer = new GeoJSONLayer({
                    url: url,
                    renderer: renderer,
                    opacity: 0.8,
                    title: "Villages"
                });
                censusVillageGeoJsonLayer = geojsonLayer;

                view.popup.watch("selectedFeature", (graphic) => {
                    console.log("selectedFeatureeeee");
                    if (graphic && graphic.attributes['Village_Name'] != null && globalScale < 100000) {
                        console.log("selectedFeatureeeee :: " + graphic.attributes['Village_Name']);
                        loadVillageCensusDetails(state, graphic.attributes['Dist_Name'].toUpperCase(), graphic.attributes['Sub_Dist_Name'].toUpperCase(), graphic.attributes['Village_Name'].toUpperCase())
                    }
                });

                view.whenLayerView(geojsonLayer).then((layerView) => {   // loadDistricts
                    console.log("layer loaded")
                    stopEAMapLoader();
                    setTimeout(function () {
                        $(".esri-legend__symbol").find("path").attr("d", "M 10,-10 L 10,0 L 10,10 L -10,10 L -10,-10 Z");
                    }, 600);
                });

            } else {
                geojsonLayer = censusVillageGeoJsonLayer;
            }
            view.map.layers = [];

            view.map.layers.push(geojsonLayer);

            geojsonLayer.renderer = renderer;
            geojsonLayer.labelingInfo = [getLabelClass('Village_Name')];

            legend.view = view;

            const template = getPopupTemplate(PopupTemplate, "Village_Name");
            geojsonLayer.popupTemplate = template;



            //	view.popup.viewModel.autoOpenEnabled = false;

// ZOOOMING
            var previousScale;
            view.watch("scale", (newValue) => {
                console.log("scale :: " + newValue)

                if (previousScale > 100000 && newValue < 100000) {
//                    loadVillagesCensusMap(view, map, legend, state);
                }

                if (previousScale < 100000 && newValue > 100000) { // zoomout
//                     view.map.layers = [];
//                     view.map.layers.push(geojsonLayer);
                }

                previousScale = newValue;
                globalScale = newValue;
            });
        }

    });

}

function resolveAfter2Seconds() {
    return new Promise(resolve => {
        setTimeout(() => {
            console.log("waiting");
            resolve("resolved");
        }, 2000);
    });
}

function checkdataLoad() {
    const result = resolveAfter2Seconds();
    console.log("waiting over");
    return true;
}






