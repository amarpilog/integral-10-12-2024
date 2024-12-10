/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
var mousePosition;
var timeOut = null;
var markers = [];
var dataLoad = false;

function initMap(year, state, containerDiv) {
    markers = [];
    var source = ['DISTRICTS', 'ASSEMBLY_CONSTITUENCIES', 'SUB DISTRICTS', 'VILLAGES'];
//            $("#selectLayer").jqxDropDownList({source: source,  width: '130px', height: 20});
//            $("#selectLayer").jqxDropDownList('selectItem', "DISTRICTS");
    var query = "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='DISTRICTS'"
//                +" AND YEAR='" + year + "'";
    const url = "getGeoJsonFiles?query=" + query;
    var location = getLocation(state);
    const map = new google.maps.Map(document.getElementById(containerDiv), {
        zoom: 8,
        disableDoubleClickZoom: true,
        center: {lat: location[1], lng: location[0]},
    });
    // NOTE: This uses cross-domain XHR, and may not work on older browsers.
//  const url = "Telangana_pc.json";
//    map.data.loadGeoJson(
//            url
//            //   "https://storage.googleapis.com/mapsdevsite/json/google.json"
//            );

    $.getJSON(url, function (data) {
        var features = map.data.addGeoJson(data);
    });
    fetchMapMarkersData(map, "POLITICAL");
//        map.data.setStyle({
//        fillColor: 'orange',
//                strokeColor: 'white',
//                fillOpacity: 0.7,
//                strokeWeight:1
//        });

//      var partiesList = getPartiesList(state, "AC", year);

    map.data.setStyle(function (feature) {
        var fillColor = 'orange';
        var fillOpacity = 0.5;
        var party = feature.getProperty('party');
        if (party != null) {
            fillOpacity = 0.5;
            party = party.toUpperCase();
            fillColor = partyColorsObj[party];
            if (fillColor == null) {
                partyColorsObj[party] = '#' + Math.floor(Math.random() * 19777215).toString(16);
                fillColor = partyColorsObj[party];
            }
        }
//        else {
//            var colors = ['red', 'green', 'blue', 'orange', 'yellow'];
//            fillColor = colors[Math.floor(Math.random() * colors.length)];
//            feature.setProperty('fillColor', fillColor);
//        }
        return {
            fillColor: fillColor,
            strokeColor: 'white',
            fillOpacity: fillOpacity,
            strokeWeight: 1
        }
    });

    const infowindow = new google.maps.InfoWindow();

//    var mapLabel = new MapLabel({
//    position: new google.maps.LatLng(18, 79),
//    text: 'test',
//    zIndex: 101} );
//
//$(mapLabel.getPanes().mapPane).css('z-index', mapLabel.zIndex);

    function loadEvents(map) {
        var actFillColour;
        map.data.addListener('mouseover', function (event) {
            var isClicked = event.feature.getProperty('isClicked');
            if (!isClicked) {
//                actFillColour = event.feature.getProperty
                map.data.overrideStyle(event.feature, {fillOpacity: 0.7, strokeWeight: 2});
            }

            var field;
            var fieldColumn;
            field = event.feature.getProperty('Village_Name');
            fieldColumn = "Village";
            if (field == null) {
                field = event.feature.getProperty('Sub_Dist_Name');
                fieldColumn = "Sub District";
            }
            if (field == null) {
                field = event.feature.getProperty('ac_name');
                fieldColumn = "Constituency";
            }
            if (field == null) {
                field = event.feature.getProperty('Dist_Name');
                fieldColumn = "District";
            }

            clearTimeout(timeOut);
            timeOut = setTimeout(function () {
//		  console.log(e);
                console.log(field);
                $("#mapLayerlabel").remove();
                $("#" + containerDiv).append("<div  id=\"mapLayerlabel\"' ></div>");
                $("#mapLayerlabel").text(field);

                $("#mapLayerlabel").css("top", mousePosition.top - 30);
                $("#mapLayerlabel").css("left", mousePosition.left - 30);
                var width = $("#mapLayerlabel").width();
                $("#mapLayerlabel").css("width", width + 10);


            }, 500);

        })

        map.data.addListener('mouseout', function (event) {
            clearTimeout(timeOut);
            if (event.domEvent.toElement != null && event.domEvent.toElement.id != "mapLayerlabel") {
                var isClicked = event.feature.getProperty('isClicked');
                if (!isClicked) {
                    map.data.overrideStyle(event.feature, {fillOpacity: 0.5, strokeWeight: 1});
                }
            }
            $("#mapLayerlabel").remove();

        })

        map.data.addListener('click', function (event) {
            console.log(event);
            map.data.revertStyle();
//           map.data.remove(event.feature);
//           map.data.setStyle({visible: false});
            map.data.forEach(function (feature) {
                feature.removeProperty('isClicked');
            });
            event.feature.setProperty('isClicked', true);
            var field;
            var fieldColumn;
            field = event.feature.getProperty('Village_Name');
            fieldColumn = "Village";
            if (field == null) {
                field = event.feature.getProperty('Sub_Dist_Name');
                fieldColumn = "Sub District";
            }
            if (field == null) {
                field = event.feature.getProperty('ac_name');
                fieldColumn = "Constituency";
            }
            if (field == null) {
                field = event.feature.getProperty('Dist_Name');
                fieldColumn = "District";
            }

            $("#mapLayerlabel").remove();
            $("#" + containerDiv).append("<div  id=\"mapLayerlabel\"' ></div>");
            $("#mapLayerlabel").text(field);

            $("#mapLayerlabel").css("top", mousePosition.top - 30);
            $("#mapLayerlabel").css("left", mousePosition.left - 30);
            var width = $("#mapLayerlabel").width();
            $("#mapLayerlabel").css("width", width + 10);

            console.log(field);
            map.data.overrideStyle(event.feature, {fillOpacity: 0.7, strokeWeight: 2});
            getValueData(field, fieldColumn);

        });

        map.data.addListener('dblclick', function (event) {

            const district = event.feature.getProperty('Dist_Name');
            console.log(district);
//        infowindow.open(map);
//        infowindow.setContent(district);
//        infowindow.setPosition(event.latLng);
//            map.data.overrideStyle(event.feature, {fillColor: 'yellow'});

            addMarker(event.latLng, map);
//            addMarker({ lat: -25.363, lng: 131.044 }, map);
        })
    }

    loadEvents(map);

    var previousZoom = 8;
    map.addListener("zoom_changed", () => {
        console.log("Zoom" + map.getZoom());

        // zoom innnn
        if (previousZoom <= 8 && map.getZoom() > 8) {
            showEAMapLoader(containerDiv);
            var query = "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='ASSEMBLY_CONSTITUENCIES'"
//                +" AND YEAR='" + year + "'";
            const url = "getGeoJsonFiles?query=" + query;
            map.data.forEach(function (feature) {
                map.data.remove(feature);
            });
            $.getJSON(url, function (data) {
                var features = map.data.addGeoJson(data);
                $("#selectLayer" + "_" + containerDiv).text("ASSEMBLY CONSTITUENCIES".initCap());
                stopEAMapLoader(containerDiv);
            });

        }
        ;
        if (previousZoom <= 10 && map.getZoom() > 10) {
            showEAMapLoader(containerDiv);
            var query = "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='SUB_DISTRICTS'"
//                +" AND YEAR='" + year + "'";
            const url = "getGeoJsonFiles?query=" + query;
            map.data.forEach(function (feature) {
                map.data.remove(feature);
            });
            $.getJSON(url, function (data) {
                var features = map.data.addGeoJson(data);
                $("#selectLayer" + "_" + containerDiv).text("SUB DISTRICTS".initCap());
                stopEAMapLoader(containerDiv);
            });

        }
        ;
        if (previousZoom <= 12 && map.getZoom() > 12) {
            showEAMapLoader(containerDiv);
            var query = "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='VILLAGES'"
//                +" AND YEAR='" + year + "'";
            const url = "getGeoJsonFiles?query=" + query;
            map.data.forEach(function (feature) {
                map.data.remove(feature);
            });
            $.getJSON(url, function (data) {
                var features = map.data.addGeoJson(data);
                $("#selectLayer" + "_" + containerDiv).text("VILLAGES".initCap());
                stopEAMapLoader(containerDiv);
            });

        }
        ;

        // zoom out
        if (previousZoom > 12 && map.getZoom() <= 12) {
            showEAMapLoader(containerDiv);
            var query = "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='SUB_DISTRICTS'"
//                +" AND YEAR='" + year + "'";
            const url = "getGeoJsonFiles?query=" + query;
            map.data.forEach(function (feature) {
                map.data.remove(feature);
            });
            $.getJSON(url, function (data) {
                var features = map.data.addGeoJson(data);
                $("#selectLayer" + "_" + containerDiv).text("SUB DISTRICTS".initCap());
                stopEAMapLoader(containerDiv);
            });

        }
        ;

        if (previousZoom > 10 && map.getZoom() <= 10) {
            showEAMapLoader(containerDiv);
            var query = "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='ASSEMBLY_CONSTITUENCIES'"
//                +" AND YEAR='" + year + "'";
            const url = "getGeoJsonFiles?query=" + query;
            map.data.forEach(function (feature) {
                map.data.remove(feature);
            });
            $.getJSON(url, function (data) {
                var features = map.data.addGeoJson(data);
                $("#selectLayer" + "_" + containerDiv).text("ASSEMBLY CONSTITUENCIES".initCap());
                stopEAMapLoader(containerDiv);
            });

        }
        ;

        if (previousZoom > 8 && map.getZoom() <= 8) {
            showEAMapLoader(containerDiv);
            var query = "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='DISTRICTS'"
//                +" AND YEAR='" + year + "'";
            const url = "getGeoJsonFiles?query=" + query;
            map.data.forEach(function (feature) {
                map.data.remove(feature);
            });
            $.getJSON(url, function (data) {
                var features = map.data.addGeoJson(data);
                $("#selectLayer" + "_" + containerDiv).text("DISTRICTS".initCap());
                stopEAMapLoader(containerDiv);
            });

        }
        ;

//        loadEvents(map);

        previousZoom = map.getZoom();
    });
//  window.initMap = initMap;

    // add event listeners for the buttons
//  document.getElementById("show-markers").addEventListener("click", showMarkers(map));
//  document.getElementById("hide-markers").addEventListener("click", hideMarkers(map));
//  document.getElementById("delete-markers").addEventListener("click", deleteMarkers(map));
    $("#show-markers" + "_" + containerDiv).click(function (event) {
        showMarkers(map)
    })
    $("#hide-markers" + "_" + containerDiv).click(function (event) {
        hideMarkers("POLITICAL")
    })
    $("#delete-markers" + "_" + containerDiv).click(function (event) {
        deleteMarkers("POLITICAL")
    })


    $("#" + containerDiv).on('mousemove', function (event) {
        mousePosition = {'top': event.offsetY, 'left': event.offsetX};
    })
}


function initMapPC(year, state, containerDiv) {
    $("#selectLayer" + "_" + containerDiv).text("PARLIAMENT CONSTITUENCIES");
    markers = [];
    var source = ['PARLIAMENT CONSTITUENCIES', 'SUB DISTRICTS', 'VILLAGES'];
//            $("#selectLayer").jqxDropDownList({source: source,  width: '130px', height: 20});
//            $("#selectLayer").jqxDropDownList('selectItem', "DISTRICTS");
    var query = "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='PARLIAMENT_CONSTITUENCIES'"
//                +" AND YEAR='" + year + "'";
    const url = "getGeoJsonFiles?query=" + query;
    var location = getLocation(state);
    const map = new google.maps.Map(document.getElementById(containerDiv), {
        zoom: 8,
        disableDoubleClickZoom: true,
        center: {lat: location[1], lng: location[0]},
    });
    // NOTE: This uses cross-domain XHR, and may not work on older browsers.
//  const url = "Telangana_pc.json";
//    map.data.loadGeoJson(
//            url
//            //   "https://storage.googleapis.com/mapsdevsite/json/google.json"
//            );

    $.getJSON(url, function (data) {
        var features = map.data.addGeoJson(data);

    });
    fetchMapMarkersData(map, "POLITICAL");
//        map.data.setStyle({
//        fillColor: 'orange',
//                strokeColor: 'white',
//                fillOpacity: 0.7,
//                strokeWeight:1
//        });

//      var partiesList = getPartiesList(state, "AC", year);

    map.data.setStyle(function (feature) {
        var fillColor = 'orange';
        var fillOpacity = 0.5;
        var party = feature.getProperty('party');
        if (party != null) {
            fillOpacity = 0.5;
            party = party.toUpperCase();
            fillColor = partyColorsObj[party];
            if (fillColor == null) {
                partyColorsObj[party] = '#' + Math.floor(Math.random() * 19777215).toString(16);
                fillColor = partyColorsObj[party];
            }
        }
//        else {
//            var colors = ['red', 'green', 'blue', 'orange', 'yellow'];
//            fillColor = colors[Math.floor(Math.random() * colors.length)];
//            feature.setProperty('fillColor', fillColor);
//        }
        return {
            fillColor: fillColor,
            strokeColor: 'white',
            fillOpacity: fillOpacity,
            strokeWeight: 1
        }
    });

    const infowindow = new google.maps.InfoWindow();

//    var mapLabel = new MapLabel({
//    position: new google.maps.LatLng(18, 79),
//    text: 'test',
//    zIndex: 101} );
//
//$(mapLabel.getPanes().mapPane).css('z-index', mapLabel.zIndex);

    function loadEventsPC(map) {
        map.data.addListener('mouseover', function (event) {
            var isClicked = event.feature.getProperty('isClicked');
            if (!isClicked) {
                map.data.overrideStyle(event.feature, {fillOpacity: 0.7, strokeWeight: 2});
            }

            var field;
            var fieldColumn;
            field = event.feature.getProperty('Village_Name');
            fieldColumn = "Village";
            if (field == null) {
                field = event.feature.getProperty('Sub_Dist_Name');
                fieldColumn = "Sub District";
            }
            if (field == null) {
                field = event.feature.getProperty('pc_name');
                fieldColumn = "Constituency";
            }


            clearTimeout(timeOut);
            timeOut = setTimeout(function () {
//		  console.log(e);
                console.log(field);
                $("#mapLayerlabel").remove();
                $("#" + containerDiv).append("<div  id=\"mapLayerlabel\"' ></div>");
                $("#mapLayerlabel").text(field);

                $("#mapLayerlabel").css("top", mousePosition.top - 30);
                $("#mapLayerlabel").css("left", mousePosition.left - 30);
                var width = $("#mapLayerlabel").width();
                $("#mapLayerlabel").css("width", width + 10);


            }, 500);

        })

        map.data.addListener('mouseout', function (event) {
            clearTimeout(timeOut);
            if (event.domEvent.toElement != null && event.domEvent.toElement.id != "mapLayerlabel") {
                var isClicked = event.feature.getProperty('isClicked');
                if (!isClicked) {
                    map.data.overrideStyle(event.feature, {fillOpacity: 0.5, strokeWeight: 1});
                }
            }
            $("#mapLayerlabel").remove();

        })

        map.data.addListener('click', function (event) {
            console.log(event);
            map.data.revertStyle();
//           map.data.remove(event.feature);
//           map.data.setStyle({visible: false});
            map.data.forEach(function (feature) {
                feature.removeProperty('isClicked');
            });
            event.feature.setProperty('isClicked', true);
            var field;
            var fieldColumn;
            field = event.feature.getProperty('Village_Name');
            fieldColumn = "Village";
            if (field == null) {
                field = event.feature.getProperty('Sub_Dist_Name');
                fieldColumn = "Sub District";
            }
            if (field == null) {
                field = event.feature.getProperty('pc_name');
                fieldColumn = "Constituency";
            }

            $("#mapLayerlabel").remove();
            $("#" + containerDiv).append("<div  id=\"mapLayerlabel\"' ></div>");
            $("#mapLayerlabel").text(field);

            $("#mapLayerlabel").css("top", mousePosition.top - 30);
            $("#mapLayerlabel").css("left", mousePosition.left - 30);
            var width = $("#mapLayerlabel").width();
            $("#mapLayerlabel").css("width", width + 10);


            console.log(field);
            map.data.overrideStyle(event.feature, {fillOpacity: 0.7, strokeWeight: 2});
            getValueData(field.toUpperCase(), fieldColumn);

        });

        map.data.addListener('dblclick', function (event) {

            const district = event.feature.getProperty('Dist_Name');
            console.log(district);
//        infowindow.open(map);
//        infowindow.setContent(district);
//        infowindow.setPosition(event.latLng);
//            map.data.overrideStyle(event.feature, {fillColor: 'yellow'});

            addMarker(event.latLng, map);
//            addMarker({ lat: -25.363, lng: 131.044 }, map);
        })
    }

    loadEventsPC(map);

    var previousZoom = 8;
    map.addListener("zoom_changed", () => {
        console.log("Zoom" + map.getZoom());

        // zoom innnn

        if (previousZoom <= 10 && map.getZoom() > 10) {
            showEAMapLoader();
            var query = "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='SUB_DISTRICTS'"
//                +" AND YEAR='" + year + "'";
            const url = "getGeoJsonFiles?query=" + query;
            map.data.forEach(function (feature) {
                map.data.remove(feature);
            });
            $.getJSON(url, function (data) {
                var features = map.data.addGeoJson(data);
                $("#selectLayer" + "_" + containerDiv).text("SUB DISTRICTS".initCap());
                stopEAMapLoader();
            });

        }
        ;
        if (previousZoom <= 12 && map.getZoom() > 12) {
            showEAMapLoader();
            var query = "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='VILLAGES'"
//                +" AND YEAR='" + year + "'";
            const url = "getGeoJsonFiles?query=" + query;
            map.data.forEach(function (feature) {
                map.data.remove(feature);
            });
            $.getJSON(url, function (data) {
                var features = map.data.addGeoJson(data);
                $("#selectLayer" + "_" + containerDiv).text("VILLAGES".initCap());
                stopEAMapLoader();
            });

        }
        ;

        // zoom out
        if (previousZoom > 12 && map.getZoom() <= 12) {
            showEAMapLoader();
            var query = "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='SUB_DISTRICTS'"
//                +" AND YEAR='" + year + "'";
            const url = "getGeoJsonFiles?query=" + query;
            map.data.forEach(function (feature) {
                map.data.remove(feature);
            });
            $.getJSON(url, function (data) {
                var features = map.data.addGeoJson(data);
                $("#selectLayer" + "_" + containerDiv).text("SUB DISTRICTS".initCap());
                stopEAMapLoader();
            });

        }
        ;


        if (previousZoom > 10 && map.getZoom() <= 10) {
            showEAMapLoader();
            var query = "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='PARLIAMENT_CONSTITUENCIES'"
//                +" AND YEAR='" + year + "'";
            const url = "getGeoJsonFiles?query=" + query;
            map.data.forEach(function (feature) {
                map.data.remove(feature);
            });
            $.getJSON(url, function (data) {
                var features = map.data.addGeoJson(data);
                $("#selectLayer" + "_" + containerDiv).text("PARLIAMENT CONSTITUENCIES".initCap());
                stopEAMapLoader();
            });

        }
        ;

//        loadEvents(map);

        previousZoom = map.getZoom();
    });
//  window.initMap = initMap;

    // add event listeners for the buttons
//  document.getElementById("show-markers").addEventListener("click", showMarkers(map));
//  document.getElementById("hide-markers").addEventListener("click", hideMarkers(map));
//  document.getElementById("delete-markers").addEventListener("click", deleteMarkers(map));
    $("#show-markers" + "_" + containerDiv).click(function (event) {
        showMarkers(map)
    })
    $("#hide-markers" + "_" + containerDiv).click(function (event) {
        hideMarkers("POLITICAL")
    })
    $("#delete-markers" + "_" + containerDiv).click(function (event) {
        deleteMarkers("POLITICAL")
    })


    $("#" + containerDiv).on('mousemove', function (event) {
        mousePosition = {'top': event.offsetY, 'left': event.offsetX};
    })
}

function addMarker(location, map) {

    var divstr = "<div><input id='markerLabelId' type='input' placeholder='Provide Marker Label' style='width:100%' /></div>"
    var modalObj = {
        title: 'Marker Label',
        body: divstr
    };
    var buttonArray = [
        {
            text: 'ok',
            click: function () {
                var label = $("#markerLabelId").val();
                // Add the marker at the clicked location, and add the next-available label
                // from the array of alphabetical characters.
                var marker = new google.maps.Marker({
                    position: location,
                    label: label,
                    map: map,
                });

                markers.push(marker);
                saveMapMarkersData(location, label, "POLITICAL");
            },
            isCloseButton: true
        }
    ];

    modalObj['buttons'] = buttonArray;
    $("#modalDailogDiv").remove();
    $("body").append("<div id='modalDailogDiv'></div>");
    createModal("modalDailogDiv", modalObj);
    $("#modalDailogDiv").find(".modal-body").css("max-height", "350px");
    $("#modalDailogDiv").find(".modal-body").css("overflow-y", "auto");
    $("#modalDailogDiv").find(".modal-dialog").css("width", "300px");

}


// Sets the map on all markers in the array.
function setMapOnAll(map) {
    for (let i = 0; i < markers.length; i++) {
        markers[i].setMap(map);
    }
}

// Removes the markers from the map, but keeps them in the array.
function hideMarkers() {
    setMapOnAll(null);
}

// Shows any markers currently in the array.
function showMarkers(map) {
    setMapOnAll(map);
}

// Deletes all markers in the array by removing references to them.
function deleteMarkers(mapType) {
    hideMarkers();
    markers = [];
    deleteMapMarkers(mapType);
}

var partyColorsObj = {
    "TRS": "pink",
    "INC": "skyblue",
    "TDP": "yellow",
    "BJP": "orange",
    "AIMIM": "green"
}

function getPartiesList(state, acpc, year, filterColumn, filterValue) {
    var partyList = [];
    var dataObj = {};
    var query = "SELECT DISTINCT WINNER_PARTY FROM IS_ELECTION_RESULTS WHERE STATE='" + state + "' AND PC_AC='" + acpc + "'";
    if (year != null) {
        query += " AND YEAR='" + year + "' ";
    }

    if (filterColumn != null && filterColumn != '' && filterColumn.toUpperCase() == "DISTRICT") {
        query += "AND DISTRICT = '" + filterValue.toUpperCase() + "'";
    }

    dataObj['query'] = query;
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'getListValues',
        cache: false,
        async: false,
        data: {
            dataObj: JSON.stringify(dataObj),
        },
        success: function (response) {
            stopLoader();
            if (response != null) {
                partyList = response['list'];

            }
        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
    return partyList;
}

function saveMapMarkersData(location, markerLabel, mapType) {
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'saveMapMarkersData',
        cache: false,
        async: false,
        data: {
            locationObj: JSON.stringify(location),
            markerLabel: markerLabel,
            mapType: mapType,
        },
        success: function (response) {
            stopLoader();
            if (response != null) {

            }
        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
}

function fetchMapMarkersData(map, mapType) {
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'fetchMapMarkersData',
        cache: false,
        async: false,
        data: {
            mapType: mapType,
        },
        success: function (response) {
            stopLoader();
            if (response != null) {
                var markersList = response['markersList']
                for (var i = 0; i < markersList.length; i++) {
                    var markerData = markersList[i];
                    var label = markerData[0];
                    var position = markerData[1];
                    position = JSON.parse(position);
                    var marker = new google.maps.Marker({
                        position: position,
                        label: label,
                        map: map,
                    });
                    markers.push(marker);
                }
            }
        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
}

function deleteMapMarkers(mapType) {
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'deleteMapMarkers',
        cache: false,
        async: false,
        data: {
            mapType: mapType
        },
        success: function (response) {
            stopLoader();

        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
}


// --------------- census ---------


function initMapCensus(state, containerDiv) {
    markers = [];
    var source = ['DISTRICTS', 'SUB DISTRICTS', 'VILLAGES'];
//            $("#selectLayer").jqxDropDownList({source: source,  width: '130px', height: 20});
//            $("#selectLayer").jqxDropDownList('selectItem', "DISTRICTS");
    var query = "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='DISTRICTS'"
//                +" AND YEAR='" + year + "'";
    const url = "getGeoJsonFiles?query=" + query;
    var location = getLocation(state);
    const map = new google.maps.Map(document.getElementById(containerDiv), {
        zoom: 8,
        disableDoubleClickZoom: true,
        center: {lat: location[1], lng: location[0]},
    });
    // NOTE: This uses cross-domain XHR, and may not work on older browsers.
//  const url = "Telangana_pc.json";
//    map.data.loadGeoJson(
//            url
//            //   "https://storage.googleapis.com/mapsdevsite/json/google.json"
//            );

    $.getJSON(url, function (data) {
        var features = map.data.addGeoJson(data);

    });
    fetchMapMarkersData(map, "CENSUS");
//        map.data.setStyle({
//        fillColor: 'orange',
//                strokeColor: 'white',
//                fillOpacity: 0.7,
//                strokeWeight:1
//        });

//      var partiesList = getPartiesList(state, "AC", year);

    map.data.setStyle(function (feature) {
        var fillColor = 'orange';
        var fillOpacity = 0.5;
        var party = feature.getProperty('party');
        if (party != null) {
            fillOpacity = 0.5;
            party = party.toUpperCase();
            fillColor = partyColorsObj[party];
            if (fillColor == null) {
                partyColorsObj[party] = '#' + Math.floor(Math.random() * 19777215).toString(16);
                fillColor = partyColorsObj[party];
            }
        }
//        else {
//            var colors = ['red', 'green', 'blue', 'orange', 'yellow'];
//            fillColor = colors[Math.floor(Math.random() * colors.length)];
//            feature.setProperty('fillColor', fillColor);
//        }
        return {
            fillColor: fillColor,
            strokeColor: 'white',
            fillOpacity: fillOpacity,
            strokeWeight: 1
        }
    });

    const infowindow = new google.maps.InfoWindow();

//    var mapLabel = new MapLabel({
//    position: new google.maps.LatLng(18, 79),
//    text: 'test',
//    zIndex: 101} );
//
//$(mapLabel.getPanes().mapPane).css('z-index', mapLabel.zIndex);

    function loadCensusEvents(map) {
        map.data.addListener('mouseover', function (event) {
            var isClicked = event.feature.getProperty('isClicked');
            if (!isClicked) {
                map.data.overrideStyle(event.feature, {fillOpacity: 0.7, strokeWeight: 2}); //  fillColor:"#166A2F" --> green
            }

            var field;
            var fieldColumn;
            field = event.feature.getProperty('Village_Name');
            fieldColumn = "Village";
            if (field == null) {
                field = event.feature.getProperty('Sub_Dist_Name');
                fieldColumn = "Sub District";
            }
            if (field == null) {
                field = event.feature.getProperty('ac_name');
                fieldColumn = "Constituency";
            }
            if (field == null) {
                field = event.feature.getProperty('Dist_Name');
                fieldColumn = "District";
            }

            clearTimeout(timeOut);
            timeOut = setTimeout(function () {
//		  console.log(e);
                console.log(field);
                $("#mapLayerlabel").remove();
                $("#" + containerDiv).append("<div  id=\"mapLayerlabel\"' ></div>");
                $("#mapLayerlabel").text(field);

                $("#mapLayerlabel").css("top", mousePosition.top - 30);
                $("#mapLayerlabel").css("left", mousePosition.left - 30);
                var width = $("#mapLayerlabel").width();
                $("#mapLayerlabel").css("width", width + 10);


            }, 500);

        })

        map.data.addListener('mouseout', function (event) {
            clearTimeout(timeOut);
            if (event.domEvent.toElement != null && event.domEvent.toElement.id != "mapLayerlabel") {
                var isClicked = event.feature.getProperty('isClicked');
                if (!isClicked) {
                    map.data.overrideStyle(event.feature, {fillOpacity: 0.5, strokeWeight: 1});
                }
            }
            $("#mapLayerlabel").remove();

        })

        map.data.addListener('click', function (event) {
            console.log(event);
            map.data.revertStyle();
//           map.data.remove(event.feature);
//           map.data.setStyle({visible: false});
            map.data.forEach(function (feature) {
                feature.removeProperty('isClicked');
            });
            event.feature.setProperty('isClicked', true);
            var field;
            var fieldColumn;
            field = event.feature.getProperty('Village_Name');
            if (field != null) {
                loadVillageCensusDetails(state,
                        event.feature.getProperty('Dist_Name').toUpperCase(),
                        event.feature.getProperty('Sub_Dist_Name').toUpperCase(),
                        event.feature.getProperty('Village_Name').toUpperCase());
                fieldColumn = "Village";
            }

            if (field == null) {
                field = event.feature.getProperty('Sub_Dist_Name');
                if (field != null) {
                    fieldColumn = "Sub District";
                    loadMandalCensusDetails(state,
                            event.feature.getProperty('Dist_Name').toUpperCase(),
                            event.feature.getProperty('Sub_Dist_Name').toUpperCase());
                }

            }
            if (field == null) {
                field = event.feature.getProperty('Dist_Name');
                if (field != null) {
                    fieldColumn = "District";
                    loadDistrictCensusDetails(state, event.feature.getProperty('Dist_Name').toUpperCase());
                }
            }

            $("#mapLayerlabel").remove();
            $("#" + containerDiv).append("<div  id=\"mapLayerlabel\"' ></div>");
            $("#mapLayerlabel").text(field);

            $("#mapLayerlabel").css("top", mousePosition.top - 30);
            $("#mapLayerlabel").css("left", mousePosition.left - 30);
            var width = $("#mapLayerlabel").width();
            $("#mapLayerlabel").css("width", width + 10);

            console.log(field);
            map.data.overrideStyle(event.feature, {fillOpacity: 0.7, strokeWeight: 2});
//            getValueCensusData(field.toUpperCase(), fieldColumn);

        });

        map.data.addListener('dblclick', function (event) {

            const district = event.feature.getProperty('Dist_Name');
            console.log(district);
//        infowindow.open(map);
//        infowindow.setContent(district);
//        infowindow.setPosition(event.latLng);
//            map.data.overrideStyle(event.feature, {fillColor: 'yellow'});

            addMarker(event.latLng, map);
//            addMarker({ lat: -25.363, lng: 131.044 }, map);
        })
    }

    loadCensusEvents(map);

    var previousZoom = 8;
    map.addListener("zoom_changed", () => {
        console.log("Zoom" + map.getZoom());

        // zoom innnn

        if (previousZoom <= 10 && map.getZoom() > 10) {
            showEAMapLoader(containerDiv);
            var query = "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='SUB_DISTRICTS'"
//                +" AND YEAR='" + year + "'";
            const url = "getGeoJsonFiles?query=" + query;
            map.data.forEach(function (feature) {
                map.data.remove(feature);
            });
            $.getJSON(url, function (data) {
                var features = map.data.addGeoJson(data);
                $("#selectLayer" + "_" + containerDiv).text("SUB DISTRICTS");
                stopEAMapLoader(containerDiv);
                dataLoad = true;
            });

        }
        ;
        if (previousZoom <= 12 && map.getZoom() > 12) {
            showEAMapLoader(containerDiv);
            var query = "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='VILLAGES'"
//                +" AND YEAR='" + year + "'";
            const url = "getGeoJsonFiles?query=" + query;
            map.data.forEach(function (feature) {
                map.data.remove(feature);
            });
            $.getJSON(url, function (data) {
                var features = map.data.addGeoJson(data);
                $("#selectLayer" + "_" + containerDiv).text("VILLAGES");
                stopEAMapLoader(containerDiv);
                dataLoad = true;
            });

        }
        ;

        // zoom out
        if (previousZoom > 12 && map.getZoom() <= 12) {
            showEAMapLoader(containerDiv);
            var query = "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='SUB_DISTRICTS'"
//                +" AND YEAR='" + year + "'";
            const url = "getGeoJsonFiles?query=" + query;
            map.data.forEach(function (feature) {
                map.data.remove(feature);
            });
            $.getJSON(url, function (data) {
                var features = map.data.addGeoJson(data);
                $("#selectLayer" + "_" + containerDiv).text("SUB DISTRICTS");
                stopEAMapLoader(containerDiv);
                dataLoad = true;
            });

        }
        ;



        if (previousZoom > 10 && map.getZoom() <= 10) {
            showEAMapLoader(containerDiv);
            var query = "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='DISTRICTS'"
//                +" AND YEAR='" + year + "'";
            const url = "getGeoJsonFiles?query=" + query;
            map.data.forEach(function (feature) {
                map.data.remove(feature);
            });
            $.getJSON(url, function (data) {
                var features = map.data.addGeoJson(data);
                $("#selectLayer" + "_" + containerDiv).text("DISTRICTS");
                stopEAMapLoader(containerDiv);
                dataLoad = true;
            });

        }
        ;

//        loadEvents(map);

        previousZoom = map.getZoom();
    });
//  window.initMap = initMap;

    // add event listeners for the buttons
//  document.getElementById("show-markers").addEventListener("click", showMarkers(map));
//  document.getElementById("hide-markers").addEventListener("click", hideMarkers(map));
//  document.getElementById("delete-markers").addEventListener("click", deleteMarkers(map));
    $("#show-markers" + "_" + containerDiv).click(function (event) {
        showMarkers(map)
    })
    $("#hide-markers" + "_" + containerDiv).click(function (event) {
        hideMarkers("POLITICAL")
    })
    $("#delete-markers" + "_" + containerDiv).click(function (event) {
        deleteMarkers("POLITICAL")
    })

    $("#" + containerDiv).on('mousemove', function (event) {
        mousePosition = {'top': event.offsetY, 'left': event.offsetX};
    })
    return map;
}





