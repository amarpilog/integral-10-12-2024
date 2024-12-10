/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
var trCount = 1;
function getKanbanData(gridId) {
    $(".searchMainWrap").hide();
    $(".languageSelectionBox").hide();
    $(".settingheaderImage").hide();
    $.ajax({
        type: "POST",
        url: 'getCloudGrid',
        data: {
            gridId: gridId,
        },
        traditional: true,
        cache: false,
        success: function (result) {

            var divstr = "<div id='KanbanTabs' class='KanbanTabsClass'>"
                    + "<div id='kanbanGridId' class='visionKanbanDesignTabContent'></div>"
                    + "<div id='kanbanViewId' class='visionKanbanDesignTabContent' style='display:none'></div>"
                    + "<div id='designViewTabHeading' class='visionKanbanDesignTabHeadingsDiv'>"
                    + "<ul class='visionKanbanDesignTabHeadings'>"
                    + "<li title='Grid View' id='li_GridView' class='visionKanbanDesignTab visionKanbanDesignTabHighLight' onclick=\"switchKanbanDesignTabs('li_GridView', 'kanbanGridId')\"><span>Grid View</span></li>"
                    + "<li title='Kanban view' id='li_KanbanView' class='visionKanbanDesignTab' onclick=\"switchKanbanDesignTabs('li_KanbanView', 'kanbanViewId')\"><span>Kanban View</span></li>"
                    + "</ul>"
                    + "</div>"
                    + "</div>";

            $("#pageBodyContent").remove();
            $("#pageBody").append('<div class="page-body-content" id="pageBodyContent">' + divstr + '</div></div>');

            var paramObj = {};
            kanbanGridConfig(result, "", paramObj, gridId);

        }
    });
}
function switchKanbanDesignTabs(liId, divId) {

    $("#" + liId).parent().find('li.visionKanbanDesignTabHighLight').removeClass('visionKanbanDesignTabHighLight');
    $("#" + liId).addClass('visionKanbanDesignTabHighLight');
    $("#kanbanGridId").hide();
    $("#kanbanViewId").hide();
    $("#" + divId).show();
}

function kanbanGridConfig(gridResultObj, selectedGridIndex, paramObj, selectedGridId) {

    ajaxStart();
    // ravi start 
    globalTabId = gridResultObj['gridId'];
    // ravi end 
    console.log(":293::gridConfig::");
    var parentDiv = "kanbanGridId";

    $("#" + parentDiv).html("<div id='" + gridResultObj['gridId'] + "'></div>");

    try {
        // if(true) {
        try {
            $("#" + gridResultObj['gridId']).jqxGrid('updatebounddata', 'cells');
            $('#' + gridResultObj['gridId']).jqxGrid('clearfilters');
        } catch (e) {

        }

        // $('#' + gridResultObj['gridId']).jqxGrid('destroy');
        alert(subTabId + "::::" + $("#" + gridResultObj['gridId']).length);

        if (gridResultObj != null) {
            //need to assign all hidden fields like hrefColumn,linkedColumns,stripValue,imageColumn,imageTable,imageTableColumn
            var hrefObj = {}; //hrefObj
            hrefObj = gridResultObj['hrefObj'];
            $("#hrefColumn").val(hrefObj['hrefColumn'] != null ? hrefObj['hrefColumn'] : "");
//                        $("#hrefColumn").val(hrefObj['hrefColumn']);
            $("#linkedColumns").val(hrefObj['linkedColumns']);
            $("#stripValue").val(hrefObj['stripValue']);
            $("#imageColumn").val(hrefObj['imageColumn']);
            $("#imageTable").val(hrefObj['imageTable']);
            $("#imageTableColumn").val(hrefObj['imageTableColumn']);
            $("#defaultValues").val(gridResultObj['defaultValues']);

            var gridInitParamObj = {}; //gridInitParamObj
            gridInitParamObj = gridResultObj['gridInitParamObj'];

            if (gridInitParamObj != null && !jQuery.isEmptyObject(gridInitParamObj)) {
                $("#" + gridResultObj['gridId']).attr("data-gridinitparamobj", JSON.stringify(gridInitParamObj));
            }
            $("#processClassAndMethod").val(gridInitParamObj['uuu_processClassAndMethod'] != null ? gridInitParamObj['uuu_processClassAndMethod'] : "");
            var batchInd = gridInitParamObj["uuu_BatchInd"];
            $("#massColumnHide").val(gridInitParamObj['massColumnHide']);
            $("#massValidateComment").val(gridInitParamObj['uuu_ValidateComment']);
            $("#batchIndicator").val(batchInd);
            var tableName = gridResultObj['tableName'];
            $("#tableName").val(tableName);
            var barCodeColumnName = gridInitParamObj['uuu_BarCodeColumn'];
            $("#barCodeColumnName").val(barCodeColumnName);

            if (gridInitParamObj['uuu_exportRangeCount'] != null && gridInitParamObj['uuu_exportRangeCount'] != '') {
                $("#ssExportCount").val(gridInitParamObj['uuu_exportRangeCount']);
            }

            var columnInitParamObj = {};
            columnInitParamObj = gridResultObj['columnInitParamsObj'];
            $("#columnInitParams").val(JSON.stringify(columnInitParamObj));

            var dropDownListData = gridResultObj.dropDownListData;

            //  alert("hrefObj:::::"+JSON.stringify(hrefObj));
            if (gridResultObj != null && gridResultObj.datafields) {


                var dataFeilds = gridResultObj.datafields;

                var hrefObj = gridResultObj.hrefObj;
                var localData = gridResultObj.data;
                var formId = gridResultObj.formId;
                var panelId = gridResultObj.panelId;
                var gridOperation = gridResultObj.gridOperation;
                //////////////////console.log("gridOperation:::"+gridOperation);

                ////////////////////console.log("formId::::::"+formId);
                $('#formId').val(formId);
                $('#panelId').val(panelId);
                var gridPropObj = {};
                gridPropObj = gridResultObj.gridPropObj;
                var hiddenObj = gridResultObj['hiddenObj'];
                if (hiddenObj != null) {
                    $("#hiddenObj").val(JSON.stringify(hiddenObj));
                }
                if (gridPropObj != null) {
                    fieldsArray.length = 0;
                    fieldsArray = gridResultObj.columns;
                    gridPropObj.columns = gridResultObj.columns;
                    var headerTooltipRenderer = function (element) {
                        $(element).parent().jqxTooltip({position: 'mouse',
                            position: 'bottom-right',
                            showArrow: false, content: $(element).text()});
                    }
                    var dataSheetRendered = function (element) {

                        // $(element).html("<div class='show_detail' ></div>");
                        $(element).addClass("show_detail");
                        $(element).parent().jqxTooltip({position: 'mouse',
                            position: 'bottom-right',
                            showArrow: false,
                            content: "Data Sheet"});
                        //content: $(element).text()});
                    }
                    var renderToolbar = gridPropObj.renderToolbar;

                    gridPropObj.renderToolbar = eval('(' + renderToolbar + ')');

                    var buttonRanderer
                            = function (row, columnfield, value, defaulthtml, columnproperties) {
                                return "<button onclick=registerSelectedItem('" + gridResultObj['gridId'] + "','" + row + "') style='width: 100px;'>Register</button>"
                                        + "<button onclick=suggestedVendorsList('" + gridResultObj['gridId'] + "','" + row + "') style='width: 120px;'>Suggested Vendors</button>"
                                        + "<button onclick=vendorsList('" + gridResultObj['gridId'] + "','" + row + "') style='width: 100px;'>Vendors List</button>"
                                        + "<button onclick=linkVendor('" + gridResultObj['gridId'] + "','" + row + "') style='width: 100px;'>Link Vendor</button>";
                            };





                    var editable = gridPropObj.editable;
                    if (editable) {
                        for (var i = 0; i < dataFeilds.length; i++) {
                            if (typeof dataFeilds[i].values != "undefined" && dataFeilds[i].values != null) {
                                var listboxData = eval(dataFeilds[i].values.source);
                                var listboxSource =
                                        {
                                            datatype: "json",
                                            datafields: [
                                                {name: 'ListboxValue', type: 'string'},
                                                {name: 'id', type: 'string'}
                                            ],
                                            localdata: listboxData
                                        };
                                var listBoxAdapter = new $.jqx.dataAdapter(listboxSource);
                                gridResultObj.datafields[i].values.source = listBoxAdapter.records;
                                // gridResultObj.datafields[i].values.source = listBoxAdapter.records;
                            }
                        }
                    }
                    for (var i = 0; i < gridPropObj.columns.length; i++) {
                        if (gridPropObj.columns [i].cellsrenderer != null) {
                            gridPropObj.columns [i].cellsrenderer = eval(gridPropObj.columns [i].cellsrenderer);
                        }
                        if (gridPropObj.columns[i].rendered != null) {
                            gridPropObj.columns[i].rendered = eval('(' + gridPropObj.columns[i].rendered + ')');
                        }

                        if (gridPropObj.columns[i].createeditor != null) {
                            gridPropObj.columns[i].createeditor = eval('(' + gridPropObj.columns[i].createeditor + ')');
                        }
                        if (gridPropObj.columns[i].initeditor != null) {
                            gridPropObj.columns[i].initeditor = eval('(' + gridPropObj.columns[i].initeditor + ')');
                        }
                        if (gridPropObj.columns[i].geteditorvalue != null) {
                            gridPropObj.columns[i].geteditorvalue = eval('(' + gridPropObj.columns[i].geteditorvalue + ')');
                        }
                        if (gridPropObj.columns[i].cellvaluechanging != null) {
                            gridPropObj.columns[i].cellvaluechanging = eval('(' + gridPropObj.columns[i].cellvaluechanging + ')');
                        }
                        if (gridPropObj.columns[i].cellbeginedit != null) {
                            gridPropObj.columns[i].cellbeginedit = eval('(' + gridPropObj.columns[i].cellbeginedit + ')');
                        }
                    }



                    //   gridPropObj.rendergridrows=function(obj) {return obj.data;};   
                    // for work flow start
                    if (gridPropObj.rendergridrows != null && gridPropObj.rendergridrows != "") {

                        gridPropObj.rendergridrows = eval('(' + gridPropObj.rendergridrows + ')');
                    }

                    var paramArray = [];
//
//                paramObj['value'] = gridResultObj['className'];
//                paramObj['column'] = 'TERM';
//                paramObj['operator'] = 'LIKE';
                    paramArray.push(paramObj);

                    var subTabId = "jqxTabs";
                    console.log("before dataFeilds" + JSON.stringify(dataFeilds));
                    var data = {
                        gridId: gridResultObj['gridId'],
                        colsArray: JSON.stringify(gridResultObj['colsArray']),
                        tableName: gridResultObj['tableName'],
                        paramArray: JSON.stringify(paramArray),
                        gridInitParamObj: JSON.stringify(gridInitParamObj),
                        columnInitParamObj: JSON.stringify(columnInitParamObj),
                        processClassAndMethod: $("#processClassAndMethod").val()

                    };
                    var source =
                            {
                                type: 'POST',
//                                                async: false,
                                datatype: "json",
                                datafields: dataFeilds,
                                data: data,
                                url: 'cloudGridResults',
                                cache: false,
                                root: 'Rows',
                                processdata: function (data) {
                                    data.multiSortColsArray = ($("#" + gridResultObj['gridId'] + "_sort_columns").val() != null
                                            ? $("#" + gridResultObj['gridId'] + "_sort_columns").val() : "");
                                    if (gridInitParamObj != null
                                            && !jQuery.isEmptyObject(gridInitParamObj)
                                            && gridInitParamObj['uuu_FilterGridFormPopup'] == 'Y') {//
                                        data.paramArray = ($("#" + gridResultObj['gridId'] + "_filter_columns").val() != null
                                                ? $("#" + gridResultObj['gridId'] + "_filter_columns").val() : "");
                                    }
                                    var crmresult = $("#crmresult").val();
                                    if (crmresult != null && crmresult != '') {
                                        var columnName = $("#crmresult").attr("data-search-columnname");
                                        var crmParamArray = [];
                                        if (data.paramArray != null && data.paramArray != '') {
                                            crmParamArray = JSON.parse(data.paramArray);
                                        }
                                        var crpParamObj = {};
                                        crpParamObj['value'] = crmresult;
                                        crpParamObj['column'] = columnName;
                                        crpParamObj['operator'] = 'LIKE';
                                        crpParamObj['rangeFlag'] = 'N'
                                        crpParamObj['minvalue'] = "";
                                        crpParamObj['maxvalue'] = "";
                                        crmParamArray.push(crpParamObj);
                                        if (crmParamArray != null && crmParamArray.length != 0) {
                                            data.paramArray = JSON.stringify(crmParamArray);
                                        }

                                    }
                                },
                                beforeSend: function () {
                                    ajaxStart();
                                }, loadError: function (xhr, status, error) {
                                    ajaxStop();
                                    // throw new Error(error);
                                }, loadComplete: function (data)
                                {
                                    ajaxStop();
                                },
                                beforeprocessing: function (data) {
                                    //ajaxStart();//1
//                                                    try{
//                                                     $("#" + gridResultObj['gridId']).jqxGrid('clearselection');
//                                                 }
//                                                 catch(e){}

                                    //   alert("beforeprocessing::::" + JSON.stringify(data));
                                    if (data[0] != null) {
                                        //  alert(data.JSONObjectList[0].TotalRows);
                                        source.totalrecords = data[0].TotalRows;
                                        $("#excelExport" + gridResultObj['gridId']).attr("disabled", true);
//                                                        $("#excelExport").removeAttr("disabled");
                                        $("#drop" + gridResultObj['gridId']).removeAttr("disabled");
                                        $("#drop" + gridResultObj['gridId']).removeAttr("opacity");
                                        $("#export" + gridResultObj['gridId']).removeAttr("disabled");
                                        $("#export" + gridResultObj['gridId']).removeAttr("opacity");

//                                                        $("#drop").attr("disabled", false);
                                        // $("#export").attr("disabled", false);
//                                                        $("#export").attr("disabled", false);
                                        console.log("data[0] != null:::: $(\"#drop\").attr(\"disabled\":::::" + $("#drop" + gridResultObj['gridId']).attr("disabled"));
                                        console.log("data[0] != null::: $(\"#export\").attr(\"disabled\":::::" + $("#export" + gridResultObj['gridId']).attr("disabled"));
                                    } else {

                                        source.totalrecords = 0;
                                        $("#excelExport" + gridResultObj['gridId']).attr("disabled", true);
                                        $("#approvebutt" + gridResultObj['gridId']).attr("disabled", true);
                                        $("#drop" + gridResultObj['gridId']).attr("disabled", true);
                                        $("#drop" + gridResultObj['gridId']).css("opacity", "0.5");
                                        $("#export" + gridResultObj['gridId']).attr("disabled", true);
                                        $("#export" + gridResultObj['gridId']).css("opacity", "0.5");
                                    }

//                                        var selectedItemTitle = $('#cloudTabs').jqxTabs('getTitleAt', $('#cloudTabs').jqxTabs('selectedItem'));
                                    try {
//                                                    $("#" + gridResultObj['gridId']).jqxGrid('clearselection');
                                    } catch (e) {
                                    }


                                    ajaxStop();
                                },
                                sort: function ()
                                {
//                                                $("#" + gridResultObj['gridId'] + "_sort_columns").remove();
                                    $("#" + gridResultObj['gridId']).jqxGrid('updatebounddata', 'sort');
                                    try {
                                        $("#" + gridResultObj['gridId']).jqxGrid('clearselection');
                                    } catch (e) {
                                    }
                                    ajaxStop();
                                },
                                filter: function () {

                                    $("#" + gridResultObj['gridId']).jqxGrid('updatebounddata', 'filter');
                                    try {
                                        $("#" + gridResultObj['gridId']).jqxGrid('clearselection');
                                    } catch (e) {
                                    }
                                    ajaxStop();
                                }


                            };
                    var dataAdapter = new $.jqx.dataAdapter(source);
                    gridPropObj.source = dataAdapter;
                    var srsRegiterButton = gridInitParamObj['registerButtonFlag'];
                    var hideToolBar = gridInitParamObj['uuu_hideToolBar'];
                    // var hideToolBar = gridInitParamObj['hideToolBar'];
//                                var srsRegiterButton = gridInitParamObj['srsRegisterFlag'];
                    if ((srsRegiterButton != null && srsRegiterButton != undefined && srsRegiterButton == 'Y')
                            || (hideToolBar != null && hideToolBar != undefined && hideToolBar == 'Y'))
                    {
                        gridPropObj.showtoolbar = true;
                        console.log("iam in if condition in toolbar 846" + srsRegiterButton);
                    } else
                    {
                        gridPropObj.showtoolbar = false;
                        console.log("iam in else condition in toolbar 852" + srsRegiterButton);
                    }
                    // gridPropObj.showtoolbar = false;
                    gridPropObj.rowdetails = true;
                    gridPropObj.rendergridrows = function () {
                        return dataAdapter.records;
                    };

                    $("#submitDropdown").html(gridResultObj['buttonObj']);
                    $("#exportDropdown").html(gridResultObj['gridOperation']);
                    gridPropObj.rowdetails = false;
                    alert("Before Grid");
                    //subTabId

                    if (editable)
                    {
                        $('#gridRefreshButton').hide();
//                                        $('div#submitDropdown > img').remove();
                    }

                    $("#currentGridpageNum").val(0);
                    gridPropObj['height'] = "630";
                    gridPropObj['pagesize'] = "50";
//                var columns = gridPropObj['columns'];
//                $.each(columns, function(i){
//                   if (this['datafield']=='VENDOR'){
//                         this['columntype'] = 'button';
//                        columnsObject['cellsrenderer'] = showActionButton;
//                        columnsObject['buttonclick'] = registerItem;
//                   }
//                })

//                gridPropObj['selectionmode'] = "none";
                    $('#' + gridResultObj['gridId']).jqxGrid(gridPropObj);
//                $('#' + gridResultObj['gridId']).jqxGrid('selectionmode', 'singlerow');

//                var height = $('#' + gridResultObj['gridId']).jqxGrid('height');
//                $('#' + gridResultObj['gridId']).jqxGrid({height: 630});
//                $('#' + gridResultObj['gridId']).jqxGrid({ pagesize: 50}); 

                    $("#" + gridResultObj['gridId']).on('cellbeginedit', function (event) {
                        var args = event.args;
                        var columntype = args.columntype
                        var dataField = args.datafield;
                        var columnindex = args.columnindex;
                        var rowBoundIndex = args.rowindex;
                        var cellValue = args.value;
                        currentDataField = dataField;
                        currentRowIndex = rowBoundIndex;
                        currentColumnindex = columnindex;

                    });
                    $('#' + gridResultObj['gridId']).on('cellclick', function (event) {
//                                    $('#searchResults').bind('cellclick', function (event) {
                        var panelId = $("#panelId").val();
                        console.log(panelId + ":::event.args.column.datafield:::::" + event.args.column.datafield);

                        // navigateToForm(event.args.column.datafield, $('#searchResults').jqxGrid('getrowdata', event.args.rowindex), 'form', gridResultObj['gridId']);
                    });
                    $('#' + gridResultObj['gridId']).on('celldoubleclick', function (event) {
                        var args = event.args;
                        var dataField = args.datafield;
                        var dataField1 = args.text;
                        var rowIndex = args.rowindex;
                        var cellValue = args.value;
                        var column = $('#' + gridResultObj['gridId']).jqxGrid('getcolumn', event.args.datafield).text;
                        popupedit(column, cellValue);
                    });
                    $('#' + gridResultObj['gridId']).unbind('rowunselect').on('rowunselect', function (event) {
                        $("#linkVendorDxpButton").parent().hide();
                        $("#vendorsListDxpButton").parent().hide();
//                                    showSelectedRows(gridResultObj['gridId'],null,gridInitParamObj['uuu_GridNtfnFlag']);
                    });
                    $('#' + gridResultObj['gridId']).unbind('rowselect').on('rowselect', function (event) {
                        globalSelectedRow = event.args.rowindex;
                        var selectedrowindexes = $('#' + gridResultObj['gridId']).jqxGrid('selectedrowindexes');
                        var selectedRowIndex = event.args.rowindex;
                        $.each(selectedrowindexes, function (i) {
                            if (selectedrowindexes[i] != selectedRowIndex) {
                                $('#' + gridResultObj['gridId']).jqxGrid('unselectrow', selectedrowindexes[i]);
                            }
                        })
                        $("#linkVendorDxpButton").parent().show();
                        $("#vendorsListDxpButton").parent().show();
//                    $('#' + gridResultObj['gridId']).jqxGrid({selectedrowindex: event.args.rowindex});
//                                    showSelectedRows(gridResultObj['gridId'], event.args.rowindex,gridInitParamObj['uuu_GridNtfnFlag']);
                    });

                    $('#' + gridResultObj['gridId']).on("pagechanged", function (event) {
                        var oldPageNum = $("#currentGridpageNum").val();
                        console.log("oldPageNum:::" + oldPageNum + "::::Current Page Num:::" + event.args.pagenum);
                        // event arguments.
                        var args = event.args;
                        // page number.
                        var pagenum = args.pagenum;
                        // page size.
                        var pagesize = args.pagesize;
                        if (parseInt(event.args.pagenum) != parseInt(oldPageNum)) {
                            var selectedrowindexes = $('#' + gridResultObj['gridId']).jqxGrid('selectedrowindexes');
//                                        console.log("searchResults:::selectedrowindexes:::" + selectedrowindexes);
                            try {
                                if (selectedrowindexes != null
                                        && selectedrowindexes.length != 0
                                        && selectedrowindexes[0] != -1) {
                                    $('#' + gridResultObj['gridId']).jqxGrid('clearselection');
                                }

                            } catch (e) {
                            }
                        }
                        $("#currentGridpageNum").val(event.args.pagenum);
                    });
                    $('#' + gridResultObj['gridId']).on("pagesizechanged", function (event) {
                        console.log("::pagesizechanged:::" + event.args.pagenum);
                        $("#currentGridpageNum").val(0);
                    });

                    alert("604 Grid");
                    $(window).resize(function () {
                        if (gridResultObj != null && $('#' + gridResultObj['gridId']).length > 0) {
                            var windowWidth = $(this).width();
                            if (windowWidth <= 415)
                            {
                                $('#' + gridResultObj['gridId']).jqxGrid({pagerheight: 70});
                            } else if (windowWidth >= 416 && windowWidth <= 500)
                            {
                                $('#' + gridResultObj['gridId']).jqxGrid({pagerheight: 40});
                            } else
                            {
                                $('#' + gridResultObj['gridId']).jqxGrid({pagerheight: 30});
                                $('#' + gridResultObj['gridId']).jqxGrid({scrollbarsize: 8});
                            }
                        }

                    }).resize();
                    $('#' + gridResultObj['gridId']).parent().css("padding-top", "3px", "important");
                    $('#' + gridResultObj['gridId']).parent().css("padding-bottom", "3px", "important");
                    //   $('#' + gridResultObj['gridId']).jqxGrid('showtoolbar', false);
                    if ((srsRegiterButton != null && srsRegiterButton != undefined && srsRegiterButton == 'Y')
                            || (hideToolBar != null && hideToolBar != undefined && hideToolBar == 'Y'))

                    {
                        $('#' + gridResultObj['gridId']).jqxGrid('showtoolbar', true);
                        console.log("iam in if grid condition in toolbar 1016" + srsRegiterButton);
                    } else
                    {
                        $('#' + gridResultObj['gridId']).jqxGrid('showtoolbar', false);
                        console.log("iam in else grid condition in toolbar 1021" + srsRegiterButton);
                    }
                    alert("683 Grid");
                }// end if(gridPropObj != null)

            }
        }
    } catch (e) {
        ajaxStop();
    }
    ajaxStop();

}// end of function gridConfig(-)



function loadKanbanViewData(gridId)
{
    ajaxStart();

    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: "kanbanViewDataColumns",
        data: {
            gridId: gridId
        },
        cache: false,
        success: function (response) {
            if (response != null && !jQuery.isEmptyObject(response))
            {
                var colsDiv = response['colsDiv'];
                var tablesDiv = response['tablesDiv'];
                var kanbanDiv = "<div class='visionKanbanColsClass'>"
                        + "<div id='kanbanColsDiv' class='kanbanColsDivClass'>" + colsDiv + "</div>"
                        + "<div id='kanbanDragDiv' class='kanbanDragDivClass'>" + tablesDiv + "</div>"
                        + "</div>";
                $("#dialog").html(kanbanDiv);
                $("#dialog").dialog({
                    title: (labelObject['Columns'] != null ? labelObject['Columns'] : 'Columns'),
                    modal: true,
                    width: '85%',
                    maxWidth: 500,
                    height: 450,
                    maxHeight: 500,
                    fluid: true,
                    buttons: [{
                            text: (labelObject['Apply'] != null ? labelObject['Apply'] : 'Apply'),
                            click: function () {
                                trCount = 1;
                                var headerObj = {};
                                var nameObj = {};
                                var columns = [];
                                var j = 0;
                                $('#kanbanHeaderId div').each(function (i, ele) {
                                    console.log(i + ': ' + ele);
                                    var divId = $(this).attr("id");
                                    var columnName = $("#" + divId).attr("data-column-name");
                                    var label = $("#" + divId).attr("data-column-label");
                                    if (columnName != null && columnName != '') {
                                        headerObj['columnName'] = columnName;
                                        headerObj['label'] = label;
                                        columns[j] = headerObj;
                                        j++;
                                    }
                                });
                                $('#kanbanNameId div').each(function (i, ele) {
                                    console.log(i + ': ' + ele);
                                    var divId = $(this).attr("id");
                                    var columnName = $("#" + divId).attr("data-column-name");
                                    var label = $("#" + divId).attr("data-column-label");
                                    if (columnName != null && columnName != '') {
                                        nameObj['columnName'] = columnName;
                                        nameObj['label'] = label;
                                        columns[j] = nameObj;
                                        j++;
                                    }
                                });
                                $('[id^="kanbanBodyColId"] div').each(function (i, ele) {
                                    console.log(i + ': ' + ele);
                                    var divId = $(this).attr("id");
                                    var columnName = $("#" + divId).attr("data-column-name");
                                    var aggragateColumnName = $("#" + divId).attr("data-aggregate-name");
                                    var label = $("#" + divId).attr("data-column-label");
                                    if (columnName != null && columnName != '') {
                                        var columnObj = {};
                                        columnObj['columnName'] = columnName;
                                        columnObj['aggragateColumnName'] = aggragateColumnName;
                                        columnObj['label'] = label;
                                        columns[j] = columnObj;
                                        j++;
                                    }
                                });
                                loadKanbanDataView(gridId, headerObj, nameObj, columns);
                                $(this).html("");
                                $(this).dialog("close");
                                $(this).dialog("destroy");
                            }

                        }, {
                            text: (labelObject['Cancel'] != null ? labelObject['Cancel'] : 'Cancel'),
                            click: function () {
                                trCount = 1;
                                $(this).html("");
                                $(this).dialog("close");
                                $(this).dialog("destroy");
                            }

                        }],
                    open: function () {

                        $(".VisionImageKanbanTableClass").unbind().click(function () {
                            $(this).parent().parent().find('ul').toggle();
                            if ($(this).parent().parent().find('ul').is(":visible")) {

                                $(this).css({transform: "rotate(90deg)"});
                            } else {
                                $(this).css({transform: "rotate(360deg)"});
                            }
                        });

                        var tableNameId = "visionKanbanTableToggleClass li";
                        $('.' + tableNameId).draggable({//cube changes 
                            revert: "invalid",
                            refreshPositions: true,
                            helper: "clone",
                            cursor: 'move',
                            zindex: false,
                            opacity: false,
                            drag: function (event, ui) {
                                ui.helper.addClass("draggableTable");
                            },
                            stop: function (event, ui) {
                                ui.helper.removeClass("draggableTable");

                            }

                        });

                        $('#kanbanHeaderId').droppable({//filters field
                            revert: "invalid",
                            refreshPositions: true,
                            cursor: 'move',
                            drop: function (event, ui) {
                                if ($("#" + this.id).is(':visible')) {
                                    var id = ui.draggable[0].id;
                                    var label = ui.draggable[0].innerText;
                                    if (id != null && id != '' && id != undefined)
                                    {
                                        id = id.replace("kanban_", "");
                                        var columnData = '<div id="KANBAN_' + id + '" class="KanbanInnerDivData"' //group
                                                + ' title="' + label + '"  '
                                                + ' data-column-name="' + id + '" data-column-label ="' + label + '"><span class="visionColsText" >' + label
                                                + '</span><img src="images/close_white.png" title="Remove Column"'
                                                + ' onclick="removeKanbanColumn(this,\'' + id + '\')" class="visionCloseColumnBtn" style="display: inline;">'
                                                + '</div>';

                                        $("#" + this.id).html(columnData);
                                    }

                                }
                            }

                        });

                        $('#kanbanNameId').droppable({//filters field
                            revert: "invalid",
                            refreshPositions: true,
                            cursor: 'move',
                            drop: function (event, ui) {
                                if ($("#" + this.id).is(':visible')) {
                                    var id = ui.draggable[0].id;
                                    var label = ui.draggable[0].innerText;
                                    if (id != null && id != '' && id != undefined)
                                    {
                                        id = id.replace("kanban_", "");
                                        var columnData = '<div id="KANBAN_' + id + '" class="KanbanInnerDivData"' //group
                                                + ' title="' + label + '"  '
                                                + ' data-column-name="' + id + '" data-column-label ="' + label + '"><span class="visionColsText" >' + label
                                                + '</span><img src="images/close_white.png" title="Remove Column"'
                                                + ' onclick="removeKanbanColumn(this,\'' + id + '\')" class="visionCloseColumnBtn" style="display: inline;">'
                                                + '</div>';

                                        $("#" + this.id).html(columnData);
                                    }

                                }
                            }

                        });

                        $('[id^="kanbanBodyColId"]').droppable({//filters field
                            revert: "invalid",
                            refreshPositions: true,
                            cursor: 'move',
                            drop: function (event, ui) {
                                if ($("#" + this.id).is(':visible')) {
                                    var id = ui.draggable[0].id;
                                    var label = ui.draggable[0].innerText;
                                    if (id != null && id != '' && id != undefined)
                                    {
                                        var parentId = this.id;
                                        id = id.replace("kanban_", "");
                                        var columnData = '<div id="KANBAN_' + id + '" class="KanbanInnerDivData"' //group
                                                + ' title="' + label + '"  '
                                                + ' data-column-name="' + id + '" data-column-label ="' + label + '"><span class="visionColsText" >' + label
                                                + '</span><img src="images/Horizontal_Dots.svg" title="Aggregate Functions"'
                                                + ' onclick=\"getKanbanAggregateFunctions(this,\'' + parentId + '\',\'' + id + '\',\'' + label + '\')\" class="visionAggregateColumnBtn" style="display: inline; "><img src="images/close_white.png" title="Remove Column"'
                                                + ' onclick="removeKanbanColumn(this,\'' + id + '\')" class="visionCloseColumnBtn" style="display: inline;">'
                                                + '</div>';

                                        $("#" + this.id).html(columnData);
                                    }

                                }
                            }

                        });


                        $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                        $(".visionHeaderMain").css("z-index", "999");
                        $(".visionFooterMain").css("z-index", "999");
                        $(".ui-dialog").addClass('kanbangridColumsnPopup');
                    },
                    beforeClose: function (event, ui)
                    {
                        trCount = 1;
                        $(".visionHeaderMain").css("z-index", "99999");
                        $(".visionFooterMain").css("z-index", "99999");
                    }

                });
            }
        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
}

function loadKanbanDataView(gridId, headerObj, nameObj, columns)
{
    ajaxStart();
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: "loadKanbanDataView",
        data: {
            gridId: gridId,
            headerObj: JSON.stringify(headerObj),
            nameObj: JSON.stringify(nameObj),
            columns: JSON.stringify(columns),
        },
        cache: false,
        success: function (response) {
            if (response != null && !jQuery.isEmptyObject(response))
            {
                var resourceArr = response['resourceData'];
                var sourceArr = response['sourceData'];
                var headerArr = response['headerArr'];
                var fields = [
                    {name: "id", type: "string"},
                    {name: "status", map: "state", type: "string"},
                    {name: "text", map: "label", type: "string"},
                    {name: "color", map: "hex", type: "string"},
                    {name: "resourceId", type: "string"}
                ];
                var source = {};
                source['localData'] = sourceArr;
                source['dataType'] = "array";
                source['dataFields'] = fields;
                var dataAdapter = new $.jqx.dataAdapter(source);

                var resourcesSource = {};
                resourcesSource['localData'] = resourceArr;
                resourcesSource['dataType'] = "array";
                var resourceFieldsArr = [
                    {name: "id", type: "number"},
                ];
                var template = "<div class='jqx-kanban-item' id=''>";
                $.each(nameObj, function (key, val) {
                    var obj = {};
                    if (key == 'columnName')
                    {
                        obj['name'] = nameObj['columnName'];
                        obj['type'] = 'string';
                        resourceFieldsArr.push(obj);
                        template += "<div class='jqx-kanban-item-color-status'></div>";
                    }
                });
                for (var j = 2; j < columns.length - 1; j++)
                {
                    var objData = columns[j];
                    if (objData != null && !jQuery.isEmptyObject(objData))
                    {
                        var obj = {};
                        obj['name'] = objData['columnName'];
                        obj['type'] = 'string';
                        resourceFieldsArr.push(obj);
                        template += "<div class='jqx-kanban-item-" + objData['columnName'] + "'></div>";
                    }
                }
                template += "<div class='jqx-kanban-item-text'></div></div>"
                resourcesSource['dataFields'] = resourceFieldsArr;
                var resourcesDataAdapter = new $.jqx.dataAdapter(resourcesSource);



                var kanbanDiv = "<div id='kanban' class='visionKanbanDialog'></div>";
                $("#dialog").html(kanbanDiv);
                $("#dialog").dialog({
                    title: (labelObject['Kanban View'] != null ? labelObject['Kanban View'] : 'Kanban View'),
                    modal: true,
                    width: '95%',
                    height: 600,
                    maxHeight: 700,
                    fluid: true,
                    buttons: [{
                            text: (labelObject['Save'] != null ? labelObject['Save'] : 'Save'),
                            click: function () {
                                saveKanbanData(gridId, headerObj, nameObj, columns);
                                $(this).html("");
                                $(this).dialog("close");
                                $(this).dialog("destroy");
                            }

                        }, {
                            text: (labelObject['Cancel'] != null ? labelObject['Cancel'] : 'Cancel'),
                            click: function () {
                                $(this).html("");
                                $(this).dialog("close");
                                $(this).dialog("destroy");
                            }

                        }],
                    open: function () {

                        $('#kanban').jqxKanban({
                            width: '100%',
                            height: '600px',
                            template: template,
                            resources: resourcesDataAdapter,
                            source: dataAdapter,
                            itemRenderer: function (element, data, resource) {
                                $.each(nameObj, function (key, val) {
                                    if (key == 'columnName')
                                    {
                                        $(element).find(".jqx-kanban-item-color-status").html("<span style='line-height: 23px; margin-left: 5px;'>" + resource[nameObj['columnName']] + "</span>");
                                    }
                                });
                                for (var j = 2; j < columns.length - 1; j++)
                                {
                                    var objData = columns[j];
                                    if (objData != null && !jQuery.isEmptyObject(objData))
                                    {
                                        $(element).find(".jqx-kanban-item-" + objData['columnName'] + "").html("<div class='jqx-kanban-item-" + objData['columnName'] + "' style='line-height: 23px; margin-left: 5px;'>" + objData['label'] + ":" + (resource[objData['columnName']] != null ? resource[objData['columnName']] : "Data not Available") + "</div>");
                                    }
                                }
                                element.on('dblclick', function (event) {
                                    if ($(event.target).hasClass("visionKanbanLeads")) {
                                        var text = $(event.target).text();
                                        var textData = data;
                                        var textResource = resource;
                                        getLeadsData(text, textData, textResource);
                                    }
                                    if ($(event.target).hasClass("visionKanbanRegion")) {
                                        var text = $(event.target).text();
                                        var textData = data;
                                        var textResource = resource;
                                        getRegionData(text, textData, textResource);
                                    }
                                });
                            },
                            columns: headerArr,
                            columnRenderer: function (element, collapsedElement, column) {

                                var kanbanId = element[0]['id'];
                                var kanbanCol;
                                if (kanbanId != null && kanbanId != '' && kanbanId != undefined)
                                {
                                    kanbanCol = kanbanId.replace("kanban-column-header-", "");
                                }
                                //$("<div id ='kanban-column-header-count-" + kanbanCol + "' kanban-column-header-count='" + kanbanCol + "' class='kanban-column-header-count-class'>$" + finalCount[kanbanCol] + "</div>").insertAfter($("#" + kanbanId));


                            }
                        });
                        ajaxStop();


                        $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                        $(".visionHeaderMain").css("z-index", "999");
                        $(".visionFooterMain").css("z-index", "999");
                        $(".ui-dialog").addClass('kanbanViewDataPopup'); 
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
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
}



function saveKanbanData(gridId, headerObj, nameObj, columns)
{
    var response = "<div id='textReason'><textarea id='reasonId' class='visionDeleteReason'></textarea><br></div>";
    response += "<div id='dailog_error_id' style='display:none;color:red'>" + (labelObject['Please give any Kanban View Name'] != null ? labelObject['Please give any Kanban View Name'] : 'Please give any Kanban View Name') + "</div>";

    $("#dialog1").html(response);
    $("#dialog1").dialog({
        title: (labelObject['Kanban View Name'] != null ? labelObject['Kanban View Name'] : 'Kanban View Name'),
        modal: true,
        height: 'auto',
        minWidth: 300,
        maxWidth: 'auto',
        fluid: true,
        buttons: [{
                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                click: function () {
                    var KanbanViewName = $("#reasonId").val();
                    if (KanbanViewName != null && KanbanViewName != '') {
                        $("#dailog_error_id").hide();
                        $(this).html("");
                        $(this).dialog("destroy");
                        $.ajax({
                            type: "post",
                            traditional: true,
                            dataType: 'json',
                            url: "saveKanbanDataView",
                            data: {
                                gridId: gridId,
                                headerObj: JSON.stringify(headerObj),
                                nameObj: JSON.stringify(nameObj),
                                columns: JSON.stringify(columns),
                                KanbanViewName: KanbanViewName
                            },
                            cache: false,
                            success: function (response) {
                                if (response != null && !jQuery.isEmptyObject(response))
                                {
                                    var result = response['result'];
                                    $("#dialog").html(result);
                                    $("#dialog").dialog({
                                        title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
                                        modal: true,
                                        width: 350,
                                        height: 150,
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
                                            $(".ui-dialog").addClass('visionDMTreePopup');
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
                                console.log(e);
                                sessionTimeout(e);
                                stopLoader();
                            }
                        });

                    } else {
                        $("#dailog_error_id").show();
                    }

                }},
            {
                text: (labelObject['Cancel'] != null ? labelObject['Cancel'] : 'Cancel'),
                click: function () {
                    $(this).html("");
                    $(this).dialog("destroy");
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







}


function loadKanbanView()
{
    ajaxStart();
    $(".searchMainWrap").hide();
    $(".languageSelectionBox").hide();
    $(".settingheaderImage").hide();
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: "kanbanViewData",
        cache: false,
        success: function (response) {
            if (response != null && !jQuery.isEmptyObject(response))
            {
                $("#pageBodyContent").remove();
                $("#pageBody").append('<div class="page-body-content" id="pageBodyContent"><div id ="KanbanPageBody" class="Kanban-page-body"></div></div></div>');
                $("#KanbanPageBody").html("<div id='kanban'></div>");
                loadKanban(response);
            }
        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
}
function loadKanban(result) {
    var resourceArr = result['resourceData'];
    var sourceArr = result['sourceData'];
    var fields = [
        {name: "id", type: "string"},
        {name: "status", map: "state", type: "string"},
        {name: "text", map: "label", type: "string"},
        {name: "color", map: "hex", type: "string"},
        {name: "resourceId", type: "string"}
    ];
    var source = {};
    source['localData'] = sourceArr;
    source['dataType'] = "array";
    source['dataFields'] = fields;
    var dataAdapter = new $.jqx.dataAdapter(source);

    var resourcesSource = {};
    resourcesSource['localData'] = resourceArr;
    resourcesSource['dataType'] = "array";
    var resourceFieldsArr = [
        {name: "id", type: "number"},
        {name: "name", type: "string"},
        {name: "QuotedAmount", type: "string"},
        {name: "FinalAmount", type: "string"},
        {name: "Leads", type: "string"}
    ];
    resourcesSource['dataFields'] = resourceFieldsArr;
    var resourcesDataAdapter = new $.jqx.dataAdapter(resourcesSource);

    $('#kanban').jqxKanban({
        width: '100%',
        height: '600px',
        template: "<div class='jqx-kanban-item' id=''>"
                + "<div class='jqx-kanban-item-color-status'></div>"
                + "<div class='jqx-kanban-item-estimate-value'></div>"
                + "<div class='jqx-kanban-item-final-value'></div>"
                + "<div class='jqx-kanban-item-leads'></div>"
                + "<div class='jqx-kanban-item-text'></div>"
                + "</div>",
        resources: resourcesDataAdapter,
        source: dataAdapter,
        itemRenderer: function (element, data, resource) {
            $(element).find(".jqx-kanban-item-color-status").html("<span style='line-height: 23px; margin-left: 5px;'>" + resource.name + "</span>");
            $(element).find(".jqx-kanban-item-estimate-value").html("<div class='jqx-kanban-item-estimate-value' style='line-height: 23px; margin-left: 5px;'>Estimate Amount :" + (resource.QuotedAmount != null ? resource.QuotedAmount : "Data not Available") + "</div>");
            $(element).find(".jqx-kanban-item-final-value").html("<div class='jqx-kanban-item-final-value' style='line-height: 23px; margin-left: 5px;'>Final Amount :" + (resource.FinalAmount != null ? resource.FinalAmount : "Data not Available") + "</div>");
            $(element).find(".jqx-kanban-item-leads").html("<div class='jqx-kanban-item-leads' style='line-height: 23px; margin-left: 5px;'>Leads :" + resource.Leads + "</div>");
            element.on('dblclick', function (event) {
                if ($(event.target).hasClass("visionKanbanLeads")) {
                    var text = $(event.target).text();
                    var textData = data;
                    var textResource = resource;
                    getLeadsData(text, textData, textResource);
                }
                if ($(event.target).hasClass("visionKanbanRegion")) {
                    var text = $(event.target).text();
                    var textData = data;
                    var textResource = resource;
                    getRegionData(text, textData, textResource);
                }
            });

        },
        columns: [
            {text: "DEALS WON", dataField: "DEALS WON"},
            {text: "DEALS LOST", dataField: "DEALS LOST"},
            {text: "ONHOLD", dataField: "ONHOLD"},
            {text: "CANCELLED", dataField: "CANCELLED", collapseDirection: "right"}
        ],
        columnRenderer: function (element, collapsedElement, column) {

            var kanbanId = element[0]['id'];
            var kanbanCol;
            if (kanbanId != null && kanbanId != '' && kanbanId != undefined)
            {
                kanbanCol = kanbanId.replace("kanban-column-header-", "");
            }
            //$("<div id ='kanban-column-header-count-" + kanbanCol + "' kanban-column-header-count='" + kanbanCol + "' class='kanban-column-header-count-class'>$" + finalCount[kanbanCol] + "</div>").insertAfter($("#" + kanbanId));


        }
    });
    ajaxStop();

//    $("#kanban").on('itemAttrClicked', function (event) {
//        var args = event.args;
//        var items = args.item;
//        var text = items.text;
//        var itemId = args.itemId;
//        var resourceId = items.resourceId;
//        var status = items.status;
//        var resources = $('#kanban').jqxKanban('resources');
//        var records = resources['records'];
//        for (var i = 0; i < records.length; i++)
//        {
//            var resObj = records[i];
//            if (resObj['id'] == resourceId)
//            {
//                $("#estimateTime").val(resObj['EstimateAmount']);
//            }
//        }
//
//
//    });
}
function getLeadsData(filterData, data, resource)
{
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: "kanbanViewLeadsData",
        data: {
            filterData: filterData,
            data: JSON.stringify(data),
            resource: JSON.stringify(resource)
        },
        cache: false,
        success: function (response) {
            if (response != null && !jQuery.isEmptyObject(response))
            {

                var resourceArr = response['resourceData'];
                var sourceArr = response['sourceData'];
                var status = response['status'];
                var fields = [
                    {name: "id", type: "string"},
                    {name: "status", map: "state", type: "string"},
                    {name: "text", map: "label", type: "string"},
                    {name: "color", map: "hex", type: "string"},
                    {name: "resourceId", type: "string"}
                ];
                var source = {};
                source['localData'] = sourceArr;
                source['dataType'] = "array";
                source['dataFields'] = fields;
                var dataAdapter = new $.jqx.dataAdapter(source);

                var resourcesSource = {};
                resourcesSource['localData'] = resourceArr;
                resourcesSource['dataType'] = "array";
                var resourceFieldsArr = [
                    {name: "id", type: "number"},
                    {name: "name", type: "string"},
                    {name: "QuotedAmount", type: "string"},
                    {name: "FinalAmount", type: "string"},
                    {name: "ProductService", type: "string"},
                    {name: "AccountName", type: "string"}
                ];
                resourcesSource['dataFields'] = resourceFieldsArr;
                var resourcesDataAdapter = new $.jqx.dataAdapter(resourcesSource);
                var leadsDiv = "<div id='kanbanLeads' class='kanbanLeadsClass'></div>";
                $("#dialog").html(leadsDiv);
                $("#dialog").dialog({
                    title: (labelObject['Columns'] != null ? labelObject['Columns'] : 'Columns'),
                    modal: true,
                    width: '85%',
                    maxWidth: 500,
                    height: 450,
                    maxHeight: 500,
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
                        $('#kanbanLeads').jqxKanban({
                            width: '98%',
                            height: '100%',
                            template: "<div class='jqx-kanban-item' id=''>"
                                    + "<div class='jqx-kanban-item-color-status'></div>"
                                    + "<div class='jqx-kanban-item-estimate-value'></div>"
                                    + "<div class='jqx-kanban-item-final-value'></div>"
                                    + "<div class='jqx-kanban-item-product-service'></div>"
                                    + "<div class='jqx-kanban-item-Account-name'></div>"
                                    + "<div class='jqx-kanban-item-text'></div>"
                                    + "</div>",
                            resources: resourcesDataAdapter,
                            source: dataAdapter,
                            itemRenderer: function (element, data, resource) {
                                $(element).find(".jqx-kanban-item-color-status").html("<span style='line-height: 23px; margin-left: 5px;'>" + resource.name + "</span>");
                                $(element).find(".jqx-kanban-item-estimate-value").html("<div class='jqx-kanban-item-estimate-value' style='line-height: 23px; margin-left: 5px;'>Estimate Amount :" + (resource.QuotedAmount != null ? resource.QuotedAmount : "Data not Available") + "</div>");
                                $(element).find(".jqx-kanban-item-final-value").html("<div class='jqx-kanban-item-final-value' style='line-height: 23px; margin-left: 5px;'>Final Amount :" + (resource.FinalAmount != null ? resource.FinalAmount : "Data not Available") + "</div>");
                                $(element).find(".jqx-kanban-item-product-service").html("<div class='jqx-kanban-item-product-service' style='line-height: 23px; margin-left: 5px;'>Service :" + resource.ProductService + "</div>");
                                $(element).find(".jqx-kanban-item-Account-name").html("<div class='jqx-kanban-item-Account-name' style='line-height: 23px; margin-left: 5px;'>Account Name :" + resource.AccountName + "</div>");
                            },
                            columns: [
                                {text: status, dataField: status}
                            ],
                            columnRenderer: function (element, collapsedElement, column) {

                                var kanbanId = element[0]['id'];
                                var kanbanCol;
                                if (kanbanId != null && kanbanId != '' && kanbanId != undefined)
                                {
                                    kanbanCol = kanbanId.replace("kanban-column-header-", "");
                                }
                                //$("<div id ='kanban-column-header-count-" + kanbanCol + "' kanban-column-header-count='" + kanbanCol + "' class='kanban-column-header-count-class'>$" + finalCount[kanbanCol] + "</div>").insertAfter($("#" + kanbanId));


                            }
                        });
                        ajaxStop();
                        $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                        $(".visionHeaderMain").css("z-index", "999");
                        $(".visionFooterMain").css("z-index", "999");
                        $(".ui-dialog").addClass('visionDMTreePopup');
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
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
}
function getRegionData(filterData, data, resource)
{
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: "kanbanViewRegionsData",
        data: {
            filterData: filterData,
            data: JSON.stringify(data),
            resource: JSON.stringify(resource)
        },
        cache: false,
        success: function (response) {
            if (response != null && !jQuery.isEmptyObject(response))
            {
                var resourceArr = response['resourceData'];
                var sourceArr = response['sourceData'];
                var status = response['status'];
                var fields = [
                    {name: "id", type: "string"},
                    {name: "status", map: "state", type: "string"},
                    {name: "text", map: "label", type: "string"},
                    {name: "color", map: "hex", type: "string"},
                    {name: "resourceId", type: "string"}
                ];
                var source = {};
                source['localData'] = sourceArr;
                source['dataType'] = "array";
                source['dataFields'] = fields;
                var dataAdapter = new $.jqx.dataAdapter(source);

                var resourcesSource = {};
                resourcesSource['localData'] = resourceArr;
                resourcesSource['dataType'] = "array";
                var resourceFieldsArr = [
                    {name: "id", type: "number"},
                    {name: "name", type: "string"},
                    {name: "QuotedAmount", type: "string"},
                    {name: "FinalAmount", type: "string"},
                    {name: "ProductService", type: "string"},
                    {name: "AccountName", type: "string"}
                ];
                resourcesSource['dataFields'] = resourceFieldsArr;
                var resourcesDataAdapter = new $.jqx.dataAdapter(resourcesSource);
                var leadsDiv = "<div id='kanbanRegion' class='kanbanRegionClass'></div>";
                $("#dialog").html(leadsDiv);
                $("#dialog").dialog({
                    title: (labelObject['Columns'] != null ? labelObject['Columns'] : 'Columns'),
                    modal: true,
                    width: '85%',
                    maxWidth: 500,
                    height: 450,
                    maxHeight: 500,
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
                        $('#kanbanRegion').jqxKanban({
                            width: '98%',
                            height: '100%',
                            template: "<div class='jqx-kanban-item' id=''>"
                                    + "<div class='jqx-kanban-item-color-status'></div>"
                                    + "<div class='jqx-kanban-item-estimate-value'></div>"
                                    + "<div class='jqx-kanban-item-final-value'></div>"
                                    + "<div class='jqx-kanban-item-product-service'></div>"
                                    + "<div class='jqx-kanban-item-Account-name'></div>"
                                    + "<div class='jqx-kanban-item-text'></div>"
                                    + "</div>",
                            resources: resourcesDataAdapter,
                            source: dataAdapter,
                            itemRenderer: function (element, data, resource) {
                                $(element).find(".jqx-kanban-item-color-status").html("<span style='line-height: 23px; margin-left: 5px;'>" + resource.name + "</span>");
                                $(element).find(".jqx-kanban-item-estimate-value").html("<div class='jqx-kanban-item-estimate-value' style='line-height: 23px; margin-left: 5px;'>Estimate Amount :" + (resource.QuotedAmount != null ? resource.QuotedAmount : "Data not Available") + "</div>");
                                $(element).find(".jqx-kanban-item-final-value").html("<div class='jqx-kanban-item-final-value' style='line-height: 23px; margin-left: 5px;'>Final Amount :" + (resource.FinalAmount != null ? resource.FinalAmount : "Data not Available") + "</div>");
                                $(element).find(".jqx-kanban-item-product-service").html("<div class='jqx-kanban-item-product-service' style='line-height: 23px; margin-left: 5px;'>Service :" + resource.ProductService + "</div>");
                                $(element).find(".jqx-kanban-item-Account-name").html("<div class='jqx-kanban-item-Account-name' style='line-height: 23px; margin-left: 5px;'>Account Name :" + resource.AccountName + "</div>");
                            },
                            columns: [
                                {text: status, dataField: status}
                            ],
                            columnRenderer: function (element, collapsedElement, column) {

                                var kanbanId = element[0]['id'];
                                var kanbanCol;
                                if (kanbanId != null && kanbanId != '' && kanbanId != undefined)
                                {
                                    kanbanCol = kanbanId.replace("kanban-column-header-", "");
                                }
                                //$("<div id ='kanban-column-header-count-" + kanbanCol + "' kanban-column-header-count='" + kanbanCol + "' class='kanban-column-header-count-class'>$" + finalCount[kanbanCol] + "</div>").insertAfter($("#" + kanbanId));


                            }
                        });
                        ajaxStop();
                        $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                        $(".visionHeaderMain").css("z-index", "999");
                        $(".visionFooterMain").css("z-index", "999");
                        $(".ui-dialog").addClass('visionDMTreePopup');
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
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
}
function deleteSelectedRow($this)
{
    $($this).parents("tr").remove();
}
function addNewJoinsRow(event, $this)
{
    var selectedColumnStr = "<tr>"
            + "<td class='kanbanDragClass'>"
            + "<img src=\"images/Detele Red Icon.svg\" onclick=\"deleteSelectedRow(this)\" class=\"visionKanbanHeaderImg\" title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\">"
            + "</td>"
            + "<td class='kanbanDragClass'><div id='kanbanBodyColId_" + trCount + "' class='kanbanInputClass' style='width:98%;height:98%'></div></tr>";

    $("#visionKanbanBodyTableId tbody").append(selectedColumnStr);
    trCount++;
    $('[id^="kanbanBodyColId"]').droppable({//filters field
        revert: "invalid",
        refreshPositions: true,
        cursor: 'move',
        drop: function (event, ui) {
            if ($("#" + this.id).is(':visible')) {
                var id = ui.draggable[0].id;
                var label = ui.draggable[0].innerText;
                if (id != null && id != '' && id != undefined)
                {
                    var parentId = this.id;
                    id = id.replace("kanban_", "");
                    var columnData = '<div id="KANBAN_' + id + '" class="KanbanInnerDivData"' //group
                            + ' title="' + label + '"  '
                            + ' data-column-name="' + id + '" data-column-label ="' + label + '"><span class="visionColsText" >' + label
                            + '</span><img src="images/Horizontal_Dots.svg" title="Aggregate Functions"'
                            + ' onclick=\"getKanbanAggregateFunctions(this,\'' + parentId + '\',\'' + id + '\', \'' + label + '\')\" class="visionAggregateColumnBtn" style="display: inline; "><img src="images/close_white.png" title="Remove Column"'
                            + ' onclick="removeKanbanColumn(this,\'' + id + '\')" class="visionCloseColumnBtn" style="display: inline;">'
                            + '</div>';

                    $("#" + this.id).html(columnData);
                }

            }
        }

    });
}

function removeKanbanColumn($this, id)
{
    $("#KANBAN_" + id).remove();
}
function getKanbanAggregateFunctions($this, parentId, id, label)
{
    var div = "<li onclick=\"applyKanbanAggregateFunctions('AVG','" + id + "','" + label + "','" + parentId + "')\">Average</li>"
            + "<li onclick=\"applyKanbanAggregateFunctions('Count','" + id + "','" + label + "','" + parentId + "')\">Count</li>"
            + "<li onclick=\"applyKanbanAggregateFunctions('Sum','" + id + "','" + label + "','" + parentId + "')\">Sum</li>"
            + "<li onclick=\"applyKanbanAggregateFunctions('Max','" + id + "','" + label + "','" + parentId + "')\">Max</li>"
            + "<li onclick=\"applyKanbanAggregateFunctions('Min','" + id + "','" + label + "','" + parentId + "')\">Min</li>";
    $("#jqxAggregate").remove();
    $('body').append("<div id='jqxAggregate'><ul></ul></div>");
    $("#jqxAggregate ul").html(div);
    var contextMenu = $("#jqxAggregate").jqxMenu({width: '90px', height: 140 + 'px', autoOpenPopup: false, mode: 'popup'});
    contextMenu.jqxMenu('open', parseInt(event.clientX) + 5, parseInt(event.clientY) + 5);
}
function applyKanbanAggregateFunctions(aggregateType, id, label, parentId)
{
    $("#KANBAN_" + id).remove();
    var aggregateLabel = aggregateType + "(" + label + ")";
    var columnData = '<div id="KANBAN_' + id + '" class="KanbanInnerDivData"' //group
            + ' title="' + label + '"  '
            + ' data-column-name="' + id + '" data-aggregate-name="' + aggregateType + '" data-column-label ="' + label + '" ><span class="visionColsText" >' + aggregateLabel
            + '</span><img src="images/Horizontal_Dots.svg" title="Aggregate Functions"'
            + ' onclick=\"getKanbanAggregateFunctions(this,\'' + parentId + '\',\'' + id + '\',\'' + label + '\')\" class="visionAggregateColumnBtn" style="display: inline; "><img src="images/close_white.png" title="Remove Column"'
            + ' onclick="removeKanbanColumn(this,\'' + id + '\')" class="visionCloseColumnBtn" style="display: inline;">'
            + '</div>';
    $("#" + parentId).html(columnData);
}



