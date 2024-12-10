var selectedDist;
var isConstituency = false;
var globalDistGeoJsonLayer;
var mandalGeoJsonLayer;
var villageGeoJsonLayer;
var constituencyLayer;

function loadDistrictsMap(year, state, containerDiv) {

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

//        const url = "js/geojson/Telangana_districts.json";
        var query = "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='DISTRICTS'"
//                +" AND YEAR='" + year + "'";
        const url = "getGeoJsonFiles?query=" + query;
        //   "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson";

        const renderer = getSimpleRenderer("Dist_name", "#FED8B1");
        const geojsonLayer = new GeoJSONLayer({
            url: url,
//            sources: getJSONdata(url),
            renderer: renderer,
            opacity: 0.8,
            title: "Districts"
        });
        distGeoJsonLayer = geojsonLayer;


        const map = new Map({
//            basemap: "gray-vector",
            layers: [geojsonLayer]
        });
        var location = getLocation(state);
        const view = new MapView({
            container: containerDiv,
//            highlightOptions: {
//            color: [255, 241, 58],
//            fillOpacity: 0.4
//          },
//            center: [79, 18],
            center: location,
            zoom: 7,
            map: map
        });

        // connect.connect(map, "onClick", myClickHandler);
        // view.on("click", function(event){
        // view.hitTest(event).then(({ results }) => {
        // console.log(results);
        // });
        // })



        /******************************************************************
         *
         * Add layers to layerInfos on the legend
         *
         ******************************************************************/
        const legend = addLegend(Expand, Legend, view, "bottom-left");


        var latitude;
        var longitude;
        view.on("click", function (event) {
            latitude = event.mapPoint.latitude
            longitude = event.mapPoint.longitude

            console.log(" dist clickkkkk");

        });

        view.popup.watch("selectedFeature", (graphic) => {
            console.log("selectedFeatureeeee");
            if (graphic) {

//                const graphicTemplate = graphic.getEffectivePopupTemplate();
//            showConstituencyDetails(graphic.attributes);
                var district = graphic.attributes.Dist_Name
                if (district != null && selectedDist != district) {

                    geojsonLayer.definitionExpression = "Dist_Name <> '" + district + "'";
                    selectedDist = district;
                    isConstituency = false;
                    loadConstituenciesInDist(WebMap,
                            Map,
                            GeoJSONLayer,
                            MapView,
                            Legend,
                            PopupTemplate,
                            Editor,
                            geojsonLayer,
                            view,
                            legend,
                            state,
                            year,
                            'Dist_Name',
                            district);
                    getValueData(district, "District");

//                    view.center = [longitude - 0.2, latitude];
                }
//                    loadMap(dist);
//                    view.ui.remove(legend);
//                    geojsonLayer.definitionExpression = "Dist_Name = '"+district+"'";

            }
        });

//        view.when(legend).then((legend) => {
//            console.log("legend loaded")
//        });
//         view.when(() => {
//            console.log("all loaded")
//        });

        view.whenLayerView(geojsonLayer).then((layerView) => {   // loadDistricts
            console.log("layer loaded")
            setTimeout(function () {
                $(".esri-legend__symbol").find("path").attr("d", "M 10,-10 L 10,0 L 10,10 L -10,10 L -10,-10 Z");
                
            }, 600)
        });



        //	view.popup.viewModel.autoOpenEnabled = false;


        const template = new PopupTemplate({
            outFields: ["*"],
            title: "District: {Dist_Name}",
            content: []
        });
        geojsonLayer.popupTemplate = template;

        geojsonLayer.labelingInfo = [getLabelClass('Dist_Name')];

        const editor = new Editor({
            view: view,
//            // Pass in the configurations created above
//            layerInfos: [editConfigCrimeLayer, editConfigPoliceLayer],
//            // Override the default template behavior of the Editor widget
//            supportingWidgetDefaults: {
//              featureTemplates: {
//                groupBy: "Constituency"
//              }
//            }
        });

        // Add widget to the view
        view.ui.add(editor, "top-right");
        setTimeout(function () {

            $(".esri-editor").html("<div id='jqxDropDownList'></div>");
            var source = ["Districts", "Assembly Constituencies"];
            $("#jqxDropDownList").jqxDropDownList({source: source, placeHolder: "Select", width: '100%', height: 30});
            $("#jqxDropDownList").jqxDropDownList('selectItem', "Districts");
            $('#jqxDropDownList').on('change', function (event)
            {
                var year = $("#selectElectionYear").val();
                var args = event.args;
                if (args) {

                    var value = args.item.value;

                    if (value == "Districts") {
                        loadDistrictsMap(year, state, "mapViewDiv");
                    } else if (value == "Assembly Constituencies") {
                        loadACConstituencies(year, state, "mapViewDiv")
                    }
//                    else if (value == "Parliament Constituencies") {
//                        
//                         loadPCConstituencies(year, "AC", "mapViewDiv");
//                    }
                }
            });


        }, 1000)


// ZOOOMING
        var previousScale;
        view.watch("scale", (newValue) => {
            console.log("scale :: " + newValue)

            if (previousScale > 500000 && newValue < 500000) {
                showEAMapLoader()
                if (mandalGeoJsonLayer == null) {
                    mandalGeoJsonLayer = new GeoJSONLayer({
                        url: "getGeoJsonFiles?query=" + "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='SUB_DISTRICTS' AND YEAR='" + year + "'",
                        //            sources: getJSONdata(url),
                        renderer: getSimpleRenderer("Sub_Dist_Name"),
                        opacity: 0.5,
                        title: "Mandals"
                    });
                }

                view.map.layers = [];
                view.map.layers.push(geojsonLayer);
                if (constituencyLayer != null) {
                    view.map.layers.push(constituencyLayer);
                }
                view.map.layers.push(mandalGeoJsonLayer);
                mandalGeoJsonLayer.labelingInfo = [getLabelClass('Sub_Dist_Name')];
//                const legend = addLegend(Expand, Legend, view, "bottom-left");
                legend.view = view;

                view.whenLayerView(mandalGeoJsonLayer).then((layerView) => {   // loadDistricts
                    console.log("layer loaded");
                    stopEAMapLoader()
                    setTimeout(function () {
                        $(".esri-legend__symbol").find("path").attr("d", "M 10,-10 L 10,0 L 10,10 L -10,10 L -10,-10 Z");
                    }, 600)
                });

            }


            if (previousScale < 500000 && newValue > 500000) {
                showEAMapLoader();
                view.map.layers = [];
//                view.map.layers.push(distGeoJsonLayer);
                view.map.layers.push(geojsonLayer);
                if (constituencyLayer != null) {
                    view.map.layers.push(constituencyLayer);
                }

                distGeoJsonLayer.labelingInfo = [getLabelClass('Dist_Name')];
//                const legend = addLegend(Expand, Legend, view, "bottom-left");
                legend.view = view;

                view.whenLayerView(distGeoJsonLayer).then((layerView) => {   // loadDistricts
                    console.log("layer loaded")
                    stopEAMapLoader();
                    setTimeout(function () {
                        $(".esri-legend__symbol").find("path").attr("d", "M 10,-10 L 10,0 L 10,10 L -10,10 L -10,-10 Z");
                    }, 600)
                });
            }

            if (previousScale > 100000 && newValue < 100000) {
                showEAMapLoader();
                if (villageGeoJsonLayer == null) {
                    villageGeoJsonLayer = new GeoJSONLayer({
                        url: "getGeoJsonFiles?query=" + "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='VILLAGES' AND YEAR='" + year + "'",
//            sources: getJSONdata(url),
                        renderer: getSimpleRenderer("Village_Name"),
                        opacity: 0.5,
                        title: "Villages"
                    });
                }


                view.map.layers = [];
                view.map.layers.push(geojsonLayer);
                if (constituencyLayer != null) {
                    view.map.layers.push(constituencyLayer);
                }
                view.map.layers.push(villageGeoJsonLayer);
                villageGeoJsonLayer.labelingInfo = [getLabelClass('Village_Name')];
//                 const legend = addLegend(Expand, Legend, view, "bottom-left");
                legend.view = view;

                view.whenLayerView(villageGeoJsonLayer).then((layerView) => {   // loadDistricts
                    console.log("layer loaded");
                    stopEAMapLoader();
                    setTimeout(function () {
                        $(".esri-legend__symbol").find("path").attr("d", "M 10,-10 L 10,0 L 10,10 L -10,10 L -10,-10 Z");
                    }, 600)
                });
            }

            if (previousScale < 100000 && newValue > 100000) {
                showEAMapLoader()
                if (mandalGeoJsonLayer == null) {
                    mandalGeoJsonLayer = new GeoJSONLayer({
                        url: "getGeoJsonFiles?query=" + "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='SUB_DISTRICTS' AND YEAR='" + year + "'",
                        //            sources: getJSONdata(url),
                        renderer: getSimpleRenderer("Sub_Dist_Name"),
                        opacity: 0.5,
                        title: "Mandals"
                    });
                }
                view.map.layers = [];
                view.map.layers.push(geojsonLayer);
                if (constituencyLayer != null) {
                    view.map.layers.push(constituencyLayer);
                }
                view.map.layers.push(mandalGeoJsonLayer);
                mandalGeoJsonLayer.labelingInfo = [getLabelClass('Sub_Dist_Name')];
//                 const legend = addLegend(Expand, Legend, view, "bottom-left");
                legend.view = view;

                view.whenLayerView(mandalGeoJsonLayer).then((layerView) => {   // loadDistricts
                    console.log("layer loaded")
                    stopEAMapLoader();
                    setTimeout(function () {
                        $(".esri-legend__symbol").find("path").attr("d", "M 10,-10 L 10,0 L 10,10 L -10,10 L -10,-10 Z");
                    }, 600)
                });
            }
            previousScale = newValue;
        });

    });


}

function loadConstituenciesInDist(WebMap,
        Map,
        GeoJSONLayer,
        MapView,
        Legend,
        PopupTemplate,
        Editor,
        distGeojsonLayer,
        view,
        legend,
        state,
        year,
        filterColumn,
        filterValue) {
    // If GeoJSON files are not on the same domain as your website, a CORS enabled server
    // or a proxy is required.
//    const url = "js/geojson/telangana_acgeo.json";
    var query = "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='ASSEMBLY_CONSTITUENCIES' AND YEAR='" + year + "'";
    const url = "getGeoJsonFiles?query=" + query;
    //   "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson";

    const renderer = getRenderer(state, year, "AC", filterColumn, filterValue);

    const geojsonLayer = new GeoJSONLayer({
        url: url,
        renderer: renderer,
        opacity: 0.8,
        title: "Constituencies"
    });
    constituencyLayer = geojsonLayer;

    view.map.layers = [];
    if (distGeojsonLayer != null) {
        view.map.layers.push(distGeojsonLayer);
    }
    view.map.layers.push(geojsonLayer);

    view.popup.watch("selectedFeature", (graphic) => {

        if (graphic) {
            var color = '';
//            $.each(renderer.classBreakInfos, function () {
//                if (this.minValue == graphic.attributes.party_code) {
//                    color = this.symbol.color;
//                    return false;
//                }
//            })
            var highlightOptions = {
                color: getPartyHighlightColour(graphic.attributes.party_code),
                fillOpacity: 0.5
            };
            view.highlightOptions = highlightOptions;
            isConstituency = true;
//                const graphicTemplate = graphic.getEffectivePopupTemplate();
            showConstituencyDetails(graphic.attributes);

        }
    });

    view.on("click", function (event) {

        console.log(" const clickkkkk");

    });
    //	view.popup.viewModel.autoOpenEnabled = false;
    // view.on("click", function(event) {
    // view.popup.open({
    // var abc = "";
    // // Set the popup
    // });
    // });

    const template = new PopupTemplate({
        outFields: ["*"],
        title: "Constituency: {ac_name}",
        content: []
    });
    geojsonLayer.popupTemplate = template;
    const labelClass = {
        // autocasts as new LabelClass()
        symbol: {
            type: "text", // autocasts as new TextSymbol()
            color: "black",
            font: {
                // autocast as new Font()
//              family: "Playfair Display",
                family: "segeo-ui,-apple-system,Roboto,Helvetica Neue,sans-serif",
                size: 8,
//              weight: "bold"
            }

        },
        labelPlacement: "above-center",
        labelExpressionInfo: {
            expression: "$feature.ac_name",
        }
    };

    geojsonLayer.labelingInfo = [labelClass];
    if (filterColumn != null) {
        geojsonLayer.definitionExpression = filterColumn + " = '" + filterValue + "'";
    }
    
    view.whenLayerView(geojsonLayer).then((layerView) => { // loadConstituenciesInDist
        console.log("layer loaded")
        setTimeout(function () {
            $(".esri-legend__symbol").find("path").attr("d", "M 10,-10 L 10,0 L 10,10 L -10,10 L -10,-10 Z");
            $(".esri-legend__symbol, .esri-legend__layer-row").click(function (event) {
                console.log("legend__symbol click")
                var party = $(event.currentTarget).closest(".esri-legend__layer-row").find(".esri-legend__layer-cell--info").text();
                console.log("party " + party);
                if (party == "") {
                    geojsonLayer.definitionExpression = "'-' ='+'";
                    if (distGeojsonLayer != null) {
                        distGeojsonLayer.definitionExpression = "Dist_name <> 'Dist_name'";
                    }
                } else {
                    geojsonLayer.definitionExpression = "party" + " = '" + party + "'";
                    if (distGeojsonLayer != null) {
                        distGeojsonLayer.definitionExpression = "'-' ='+'";
                    }
                }

            })
        }, 600)

    });

}


function loadACConstituencies(year, state, containerDiv) {

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

//        const url = "js/geojson/Telangana_districts.json";
        var query = "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='ASSEMBLY_CONSTITUENCIES' AND YEAR='" + year + "'";
//                +" AND YEAR='" + year + "'";
        const url = "getGeoJsonFiles?query=" + query;
        //   "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson";

        const renderer = getRenderer(state, year, "AC");
        const geojsonLayer = new GeoJSONLayer({
            url: url,
//            sources: getJSONdata(url),
            renderer: renderer,
            opacity: 0.8,
            title: "Assembly Constituencies"
        });
 
        const map = new Map({
//            basemap: "gray-vector",
            layers: [geojsonLayer]
        });
        var location = getLocation(state);
        const view = new MapView({
            container: containerDiv,
//            highlightOptions: {
//            color: [255, 241, 58],
//            fillOpacity: 0.4
//          },
//            center: [79, 18],
            center: location,
            zoom: 7,
            map: map
        });

     
        const legend = addLegend(Expand, Legend, view, "bottom-left");

        var latitude;
        var longitude;
        view.on("click", function (event) {
            latitude = event.mapPoint.latitude;
            longitude = event.mapPoint.longitude;
            console.log(" dist clickkkkk");
        });

           view.popup.watch("selectedFeature", (graphic) => {
        if (graphic) {
            isConstituency = true;
            var color = '';
            var highlightOptions = {
                color: getPartyHighlightColour(graphic.attributes.party_code),
                fillOpacity: 0.5
            };
            view.highlightOptions = highlightOptions;
//                const graphicTemplate = graphic.getEffectivePopupTemplate();
            showConstituencyDetails(graphic.attributes);
        }
    });


        view.whenLayerView(geojsonLayer).then((layerView) => {   // loadDistricts
            console.log("layer loaded")
            setTimeout(function () {
                $(".esri-legend__symbol").find("path").attr("d", "M 10,-10 L 10,0 L 10,10 L -10,10 L -10,-10 Z");
                
                $(".esri-legend__symbol, .esri-legend__layer-row").click(function (event) {
                console.log("legend__symbol click")
                var party = $(event.currentTarget).closest(".esri-legend__layer-row").find(".esri-legend__layer-cell--info").text();
                console.log("party " + party);
                if (party == "") {
                    geojsonLayer.definitionExpression = "'-' ='+'";
                } else {
                    geojsonLayer.definitionExpression = "party" + " = '" + party + "'";
                }

            })
            
            }, 600)
        });

        //	view.popup.viewModel.autoOpenEnabled = false;


        const template = new PopupTemplate({
            outFields: ["*"],
            title: "COnstituency: {ac_name}",
            content: []
        });
        geojsonLayer.popupTemplate = template;

        geojsonLayer.labelingInfo = [getLabelClass('ac_name')];

        const editor = new Editor({
            view: view,
//            // Pass in the configurations created above
//            layerInfos: [editConfigCrimeLayer, editConfigPoliceLayer],
//            // Override the default template behavior of the Editor widget
//            supportingWidgetDefaults: {
//              featureTemplates: {
//                groupBy: "Constituency"
//              }
//            }
        });

        // Add widget to the view
        view.ui.add(editor, "top-right");
        setTimeout(function () {

            $(".esri-editor").html("<div id='jqxDropDownList'></div>");
            var source = ["Districts", "Assembly Constituencies"];
            $("#jqxDropDownList").jqxDropDownList({source: source, placeHolder: "Select", width: '100%', height: 30});
            $("#jqxDropDownList").jqxDropDownList('selectItem', "Assembly Constituencies");
            $('#jqxDropDownList').on('change', function (event)
            {
                var year = $("#selectElectionYear").val();
                var args = event.args;
                if (args) {

                    var value = args.item.value;

                    if (value == "Districts") {
                        loadDistrictsMap(year, state, "mapViewDiv");
                    } else if (value == "Assembly Constituencies") {
                        loadACConstituencies(year, state, "mapViewDiv")
                    }
//                    else if (value == "Parliament Constituencies") {
//                        
//                         loadPCConstituencies(year, "AC", "mapViewDiv");
//                    }
                }
            });


        }, 1000)


// ZOOOMING
        var previousScale;
        view.watch("scale", (newValue) => {
            console.log("scale :: " + newValue)

            if (previousScale > 500000 && newValue < 500000) {
                showEAMapLoader()
                if (mandalGeoJsonLayer == null) {
                    mandalGeoJsonLayer = new GeoJSONLayer({
                        url: "getGeoJsonFiles?query=" + "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='SUB_DISTRICTS' AND YEAR='" + year + "'",
                        //            sources: getJSONdata(url),
                        renderer: getSimpleRenderer("Sub_Dist_Name"),
                        opacity: 0.5,
                        title: "Mandals"
                    });
                }

                view.map.layers = [];
                view.map.layers.push(geojsonLayer);
                view.map.layers.push(mandalGeoJsonLayer);
                mandalGeoJsonLayer.labelingInfo = [getLabelClass('Sub_Dist_Name')];
//                const legend = addLegend(Expand, Legend, view, "bottom-left");
                legend.view = view;

                view.whenLayerView(mandalGeoJsonLayer).then((layerView) => {   
                    console.log("layer loaded");
                    stopEAMapLoader()
                    setTimeout(function () {
                        $(".esri-legend__symbol").find("path").attr("d", "M 10,-10 L 10,0 L 10,10 L -10,10 L -10,-10 Z");
                        
                        $(".esri-legend__symbol, .esri-legend__layer-row").click(function (event) {
                            console.log("legend__symbol click")
                            var party = $(event.currentTarget).closest(".esri-legend__layer-row").find(".esri-legend__layer-cell--info").text();
                            console.log("party " + party);
                            if (party == "") {
                                geojsonLayer.definitionExpression = "'-' ='+'";
                            } else {
                                geojsonLayer.definitionExpression = "party" + " = '" + party + "'";
                            }

                        });
                        
                    }, 600)
                });

            }


            if (previousScale < 500000 && newValue > 500000) {
                showEAMapLoader();
                view.map.layers = [];
                view.map.layers.push(geojsonLayer);
  
                legend.view = view;

                view.whenLayerView(geojsonLayer).then((layerView) => {   // loadDistricts
                    console.log("layer loaded")
                    stopEAMapLoader();
                    setTimeout(function () {
                        $(".esri-legend__symbol").find("path").attr("d", "M 10,-10 L 10,0 L 10,10 L -10,10 L -10,-10 Z");
                        
                        $(".esri-legend__symbol, .esri-legend__layer-row").click(function (event) {
                            console.log("legend__symbol click")
                            var party = $(event.currentTarget).closest(".esri-legend__layer-row").find(".esri-legend__layer-cell--info").text();
                            console.log("party " + party);
                            if (party == "") {
                                geojsonLayer.definitionExpression = "'-' ='+'";
                            } else {
                                geojsonLayer.definitionExpression = "party" + " = '" + party + "'";
                            }

                        });
                    }, 600)
                });
            }

            if (previousScale > 100000 && newValue < 100000) {
                showEAMapLoader();
                if (villageGeoJsonLayer == null) {
                    villageGeoJsonLayer = new GeoJSONLayer({
                        url: "getGeoJsonFiles?query=" + "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='VILLAGES' AND YEAR='" + year + "'",
//            sources: getJSONdata(url),
                        renderer: getSimpleRenderer("Village_Name"),
                        opacity: 0.5,
                        title: "Villages"
                    });
                }


                view.map.layers = [];
                view.map.layers.push(geojsonLayer);
                view.map.layers.push(villageGeoJsonLayer);
                villageGeoJsonLayer.labelingInfo = [getLabelClass('Village_Name')];
//                 const legend = addLegend(Expand, Legend, view, "bottom-left");
                legend.view = view;

                view.whenLayerView(villageGeoJsonLayer).then((layerView) => {   // loadDistricts
                    console.log("layer loaded");
                    stopEAMapLoader();
                    setTimeout(function () {
                        $(".esri-legend__symbol").find("path").attr("d", "M 10,-10 L 10,0 L 10,10 L -10,10 L -10,-10 Z");
                        
                        $(".esri-legend__symbol, .esri-legend__layer-row").click(function (event) {
                            console.log("legend__symbol click")
                            var party = $(event.currentTarget).closest(".esri-legend__layer-row").find(".esri-legend__layer-cell--info").text();
                            console.log("party " + party);
                            if (party == "") {
                                geojsonLayer.definitionExpression = "'-' ='+'";
                            } else {
                                geojsonLayer.definitionExpression = "party" + " = '" + party + "'";
                            }

                        });
                        
                    }, 600)
                });
            }

            if (previousScale < 100000 && newValue > 100000) {
                showEAMapLoader()
                if (mandalGeoJsonLayer == null) {
                    mandalGeoJsonLayer = new GeoJSONLayer({
                        url: "getGeoJsonFiles?query=" + "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='SUB_DISTRICTS' AND YEAR='" + year + "'",
                        //            sources: getJSONdata(url),
                        renderer: getSimpleRenderer("Sub_Dist_Name"),
                        opacity: 0.5,
                        title: "Mandals"
                    });
                }
                view.map.layers = [];
                view.map.layers.push(geojsonLayer);
                view.map.layers.push(mandalGeoJsonLayer);
                mandalGeoJsonLayer.labelingInfo = [getLabelClass('Sub_Dist_Name')];
//                 const legend = addLegend(Expand, Legend, view, "bottom-left");
                legend.view = view;

                view.whenLayerView(mandalGeoJsonLayer).then((layerView) => {   // loadDistricts
                    console.log("layer loaded")
                    stopEAMapLoader();
                    setTimeout(function () {
                        $(".esri-legend__symbol").find("path").attr("d", "M 10,-10 L 10,0 L 10,10 L -10,10 L -10,-10 Z");
                    }, 600)
                });
            }
            previousScale = newValue;
        });

    });


}


//function loadACConstituencies(state, year, containerDiv) {
//     require([
//        "esri/WebMap",
//        "esri/Map",
//        "esri/layers/GeoJSONLayer",
//        "esri/views/MapView",
//        "esri/widgets/Legend",
//        "esri/PopupTemplate",
//        "esri/widgets/Editor",
//        "esri/widgets/Expand",
//        "esri/request",
//    ], (
//            WebMap,
//            Map,
//            GeoJSONLayer,
//            MapView,
//            Legend,
//            PopupTemplate,
//            Editor,
//            Expand,
//            esriRequest
//            ) => {
//    // If GeoJSON files are not on the same domain as your website, a CORS enabled server
//    // or a proxy is required.
////    const url = "js/geojson/telangana_acgeo.json";
//    var query = "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='ASSEMBLY_CONSTITUENCIES' AND YEAR='" + year + "'";
//    const url = "getGeoJsonFiles?query=" + query;
//    //   "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson";
//
//
//    const renderer = getRenderer(state, year, "AC");
//    const geojsonLayer = new GeoJSONLayer({
//        url: url,
//        renderer: renderer,
//        opacity: 0.8,
//        title: "Constituencies"
//    });
//     const map = new Map({
////            basemap: "gray-vector",
//            layers: [geojsonLayer]
//        });
//        var location = getLocation(state);
//        const view = new MapView({
//            container: containerDiv,
////            highlightOptions: {
////            color: [255, 241, 58],
////            fillOpacity: 0.4
////          },
////            center: [79, 18],
//            center: location,
//            zoom: 7,
//            map: map
//        });
//    view.map.layers = [];
//    view.map.layers.push(geojsonLayer);
//
//    view.popup.watch("selectedFeature", (graphic) => {
//        if (graphic) {
//            isConstituency = true;
//            var color = '';
//            var highlightOptions = {
//                color: getPartyHighlightColour(graphic.attributes.party_code),
//                fillOpacity: 0.5
//            };
//            view.highlightOptions = highlightOptions;
////                const graphicTemplate = graphic.getEffectivePopupTemplate();
//            showConstituencyDetails(graphic.attributes);
//        }
//    });
//
//
//
//    view.on("click", function (event) {
//
//        console.log(" const clickkkkk");
//
//    });
//
//
//    const template = new PopupTemplate({
//        outFields: ["*"],
//        title: "Constituency: {ac_name}",
//        content: []
//    });
//    geojsonLayer.popupTemplate = template;
//    const labelClass = {
//        // autocasts as new LabelClass()
//        symbol: {
//            type: "text", // autocasts as new TextSymbol()
//            color: "black",
//            font: {
//                // autocast as new Font()
////              family: "Playfair Display",
//                family: "segeo-ui,-apple-system,Roboto,Helvetica Neue,sans-serif",
//                size: 8,
////              weight: "bold"
//            }
//
//        },
//        labelPlacement: "above-center",
//        labelExpressionInfo: {
//            expression: "$feature.ac_name",
//        }
//    };
//
//    geojsonLayer.labelingInfo = [labelClass];
//
//    view.whenLayerView(geojsonLayer).then((layerView) => { // loadACConstituencies
//        console.log("layer loaded")
//        setTimeout(function () {
//            $(".esri-legend__symbol").find("path").attr("d", "M 10,-10 L 10,0 L 10,10 L -10,10 L -10,-10 Z");
//            $(".esri-legend__symbol").click(function (event) {
//                console.log("legend__symbol click")
//                console.log("party " + party)
//
//                var party = $(event.currentTarget).closest(".esri-legend__layer-row").find(".esri-legend__layer-cell--info").text();
//                geojsonLayer.definitionExpression = 'party' + " = '" + party + "'";
//
//            })
//        }, 600)
//
//    });
//    });
//}

function loadPCConstituencies(year, acpc, state, containerDiv) {

    require([
        "esri/WebMap",
        "esri/Map",
        "esri/layers/GeoJSONLayer",
        "esri/views/MapView",
        "esri/widgets/Legend",
        "esri/PopupTemplate",
        "esri/widgets/Editor"
    ], (
            WebMap,
            Map,
            GeoJSONLayer,
            MapView,
            Legend,
            PopupTemplate,
            Editor
            ) => {

        var query = "SELECT FILE_CONTENT FROM IS_GEOJSON_FILES WHERE STATE='" + state + "' AND FIELD_NAME='PARLIAMENT_CONSTITUENCIES' ";
            if (year!=null){
//                query += "AND YEAR='" + year + "'";
            }
        const url = "getGeoJsonFiles?query=" + query;
        //   "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson";

        const renderer = getRenderer(state, year, "PC");
//        const renderer = getSimpleRenderer("pc_name");
        const geojsonLayer = new GeoJSONLayer({
            url: url,
            renderer: renderer,
            opacity: 0.8,
            title: "Parliament Constituencies"
        });

        const map = new Map({
//            basemap: "gray-vector",
            layers: [geojsonLayer]
        });

       var location = getLocation(state);
        const view = new MapView({
            container: containerDiv,
//            highlightOptions: {
//            color: [255, 241, 58],
//            fillOpacity: 0.4
//          },
            center: location,
            zoom: 7,
            map: map
        });


        const legend = new Legend({
            view: view
        });
        view.ui.add(legend, "bottom-left");
        view.popup.watch("selectedFeature", (graphic) => {
            if (graphic) {
                isConstituency = true;
                var color = '';
                var highlightOptions = {
                    color: getPartyHighlightColour(graphic.attributes.party_code),
                    fillOpacity: 0.5
                };
                view.highlightOptions = highlightOptions;
//                const graphicTemplate = graphic.getEffectivePopupTemplate();
                showConstituencyDetails(graphic.attributes);
            }
        });


        view.on("click", function (event) {

            console.log(" const clickkkkk");

        });


        const template = new PopupTemplate({
            outFields: ["*"],
            title: "Constituency: {pc_name}",
            content: []
        });
        geojsonLayer.popupTemplate = template;
        const labelClass = {
            // autocasts as new LabelClass()
            symbol: {
                type: "text", // autocasts as new TextSymbol()
                color: "black",
                font: {
                    // autocast as new Font()
//              family: "Playfair Display",
                    family: "segeo-ui,-apple-system,Roboto,Helvetica Neue,sans-serif",
                    size: 8,
//              weight: "bold"
                }

            },
            labelPlacement: "above-center",
            labelExpressionInfo: {
                expression: "$feature.pc_name",
            }
        };

        geojsonLayer.labelingInfo = [labelClass];

        // Add widget to the view
        const editor = new Editor({
            view: view,
//            // Pass in the configurations created above
//            layerInfos: [editConfigCrimeLayer, editConfigPoliceLayer],
//            // Override the default template behavior of the Editor widget
//            supportingWidgetDefaults: {
//              featureTemplates: {
//                groupBy: "Constituency"
//              }
//            }
        });

        // Add widget to the view
        view.ui.add(editor, "top-right");
        view.ui.add(editor, "top-right");
        setTimeout(function () {

            $(".esri-editor").html("<div id='jqxDropDownList'></div>");
            var source = ["Parliament Constituencies"];
            $("#jqxDropDownList").jqxDropDownList({source: source, placeHolder: "Select", width: '100%', height: 30});
            $("#jqxDropDownList").jqxDropDownList('selectItem', "Parliament Constituencies");
            $('#jqxDropDownList').on('change', function (event)
            {
                var year = $("#selectElectionYear").val();
                var args = event.args;
                if (args) {

                    var value = args.item.value;

//                    if (value == "Districts") {
//                        loadDistrictsMap(year, "AC", "mapViewDiv");
//                    } else if (value == "Assembly Constituencies") {
//                        loadACConstituencies(Map, GeoJSONLayer, MapView, Legend, PopupTemplate, Editor, view, legend)
//                    } else
                    if (value == "Parliament Constituencies") {
                        loadPCConstituencies(year, "PC", state, "mapViewDiv");
                    }
                }
            });

        }, 1000)
        
        view.whenLayerView(geojsonLayer).then((layerView) => { // loadPCConstituencies
            console.log("layer loaded")
            setTimeout(function () {
                $(".esri-legend__symbol").find("path").attr("d", "M 10,-10 L 10,0 L 10,10 L -10,10 L -10,-10 Z");
                $(".esri-legend__symbol").click(function (event) {
                    console.log("legend__symbol click")
                    console.log("party " + party)

                    var party = $(event.currentTarget).closest(".esri-legend__layer-row").find(".esri-legend__layer-cell--info").text();
                    geojsonLayer.definitionExpression = "party" + " = '" + party + "'";

                })
            }, 600)

        });

    });
}

function showConstituencyDetails(assemblyDetailsObj) {
//        $(".esri-popup").hide();
    console.log("click event :: " + assemblyDetailsObj);
    var costituency = assemblyDetailsObj['ac_name'];
    if (costituency == null) {
        costituency = assemblyDetailsObj['pc_name'];
    }
    if (costituency != null) {

        getValueData(costituency.toUpperCase(), 'Constituency');
    }
}

function getPartyHighlightColour(partyCode) {

    var color = '';
    if (partyCode == 1) { // TRS
        color = '#FF0CB9';
    } else if (partyCode == 2) { // INC
        color = '#0085D5';
    } else if (partyCode == 3) { // AIMIM
        color = '#2AB801';
    } else if (partyCode == 4) { // TDP
        color = '#FAE803';
    } else if (partyCode == 5) { // AIFB
        color = '#C11B1B';
    } else if (partyCode == 6) { // BJP
        color = '#FF9700';
    } else if (partyCode == 7) { // IND
        color = '#8A8A8A';
    } else {
        color = '#00ffff';
    }
    return color;
}

function getRenderer(state, year, acpc, filterColumn, filterValue) {

    var partiesObj = [];
    const TRS = {
        type: "simple-fill", // autocasts as new SimpleFillSymbol()
        color: "pink",
        style: "solid",
        outline: {
            width: 0.2,
//            color: [255, 255, 255, 0.5]
            color: [0, 0, 0, 0.5]
        }
    };
    const INC = {
        type: "simple-fill", // autocasts as new SimpleFillSymbol()
        color: "skyblue",
        style: "solid",
        outline: {
            width: 0.2,
//            color: [255, 255, 255, 0.5]
            color: [0, 0, 0, 0.5]
        }
    };

    const AIMIM = {
        type: "simple-fill", // autocasts as new SimpleFillSymbol()
        color: "#90EE90",
        style: "solid",
        outline: {
            width: 0.2,
//            color: [255, 255, 255, 0.5]
            color: [0, 0, 0, 0.5]
        }
    };
    const TDP = {
        type: "simple-fill", // autocasts as new SimpleFillSymbol()
        color: "#FAF884",
        style: "solid",
        outline: {
            width: 0.2,
//            color: [255, 255, 255, 0.5]
            color: [0, 0, 0, 0.5]
        }
    };

    const AIFB = {
        type: "simple-fill", // autocasts as new SimpleFillSymbol()
        color: "#FF7276",
        style: "solid",
        outline: {
            width: 0.2,
//            color: [255, 255, 255, 0.5]
            color: [0, 0, 0, 0.5]
        }
    };
    const BJP = {
        type: "simple-fill", // autocasts as new SimpleFillSymbol()
        color: "#FFD580",
        style: "solid",
        outline: {
            width: 0.2,
//            color: [255, 255, 255, 0.5]
            color: [0, 0, 0, 0.5]
        }
    };

    const IND = {
        type: "simple-fill", // autocasts as new SimpleFillSymbol()
        color: "white",
        style: "solid",
        outline: {
            width: 0.2,
//            color: [255, 255, 255, 0.5]
            color: [0, 0, 0, 0.5]
        }
    };
    
    partiesObj['TRS'] = TRS;
    partiesObj['INC'] = INC;
    partiesObj['AIMIM'] = AIMIM;
    partiesObj['TDP'] = TDP;
    partiesObj['AIFB'] = AIFB;
    partiesObj['BJP'] = BJP;
//    partiesObj['IND'] = IND;
    var dataObj = {};
    var uniqueValueInfos = [];
    var query = "SELECT DISTINCT WINNER_PARTY FROM IS_ELECTION_RESULTS WHERE STATE='" + state + "' AND PC_AC='" + acpc + "'";
        if (year!=null){
            query += " AND YEAR='" + year + "' ";
        }
    
    if (filterColumn!=null && filterColumn!='' && filterColumn.toUpperCase() == "DISTRICT"){
        query += "AND DISTRICT = '"+filterValue.toUpperCase()+"'";
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
                var partylist = response['list'];
                for (var i = 0; i < partylist.length; i++) {
                    var party = partylist[i];
                    var partyNamesArray = Object.keys(partiesObj);
                    var uniqueValObj = {};
                    uniqueValObj['value'] = party;
                    
                    if (partyNamesArray.indexOf(party) > -1) {
                        uniqueValObj['symbol'] = partiesObj[party];
                        uniqueValueInfos.push(uniqueValObj);
                    } else {
                         var other = {
                            type: "simple-fill", // autocasts as new SimpleFillSymbol()
                            color: '#' + Math.floor(Math.random() * 19777215).toString(16),
                            style: "solid",
                            outline: {
                                width: 0.2,
                    //            color: [255, 255, 255, 0.5]
                                color: [0, 0, 0, 0.5]
                            }
                        };
                        uniqueValObj['symbol'] = other;
                      
                        uniqueValueInfos.push(uniqueValObj);
                    }
                }
            }
        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });

    const renderer = {
        type: "unique-value",
        field: "party",
        legendOptions: {
            title: "Parties"
        },
        defaultSymbol: {
            type: "simple-fill", // autocasts as new SimpleFillSymbol()
            color: "white",
            style: "backward-diagonal",
            outline: {
                width: 0.5,
                color: [50, 50, 50, 0.6]
            }
        },
//        defaultLabel: "no data",
        uniqueValueInfos: uniqueValueInfos
    };

    return renderer;
}

function getLabelClass(label) {
    const labelClass = {
        // autocasts as new LabelClass()
        symbol: {
            type: "text", // autocasts as new TextSymbol()
            color: "black",
            font: {
                // autocast as new Font()
                family: "segeo-ui,-apple-system,Roboto,Helvetica Neue,sans-serif",
                size: 8,
//              weight: "bold"
            }
        },
        labelPlacement: "above-center",
        labelExpressionInfo: {
            expression: "$feature." + label + "",
        }
    };
    return labelClass;
}

function addLegend(Expand, Legend, view, position) {
    const legend = new Expand({
        content: new Legend({
            view: view,
            style: "classic" // other styles include 'card'
        }),
        view: view,
        expanded: true
    });
    view.ui.add(legend, position);
    return legend;
}

function getSimpleRenderer(field, color) {
    const renderer = {
        type: "simple",
        field: field,
        symbol: {
            type: "simple-fill", // autocasts as new SimpleFillSymbol()
//            color: "#FED8B1",
            color: color,
            style: "solid",
            outline: {
                width: 0.2,
//            color: [255, 255, 255, 0.5]
                color: [0, 0, 0, 0.5]
            }
        }
    };
    return renderer;
}

function getPopupTemplate(PopupTemplate) {

    const template = new PopupTemplate({
        outFields: ["*"],
        title: "Constituency: {ac_name}",
        content: []
    });
    return template;
}

function showEAMapLoader(containerDiv) {
    $(".voteAnalysisMapLoader").css("display", "block");
    $("#"+containerDiv).css("pointer-events", "none");
}

function stopEAMapLoader(containerDiv) {
    $(".voteAnalysisMapLoader").css("display", "none");
    $("#"+containerDiv).css("pointer-events", "auto");
}

function getLocation(state) {
    var location = [];
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'getLocationCoordinates',
        cache: false,
        async: false,
        data: {
            state: state,
        },
        success: function (response) {
            stopLoader();
            location.push(parseFloat(response['longitude']));
            location.push(parseFloat(response['latitude']));
        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
    return location;
}


