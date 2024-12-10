/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


$(function () {

    $.ajax({
        type: "POST",
        url: 'vendorNotificationsCount',
        data: {
        },
        traditional: true,
        cache: false,
        async: false,
        success: function (response) {
            stopLoader();
            if (response != null && response != '') {
                var notificationsCount = response['notificationCount'];
                if (notificationsCount != null && notificationsCount != 0) {
                    $("#vendorNotificationCount").html("<span class='vendorNotificationCountSpan'>" + notificationsCount + "</span>")
                    $("#vendorNotificationCount").css("margin-top", "-58px");
                    $("#vendorNotificationCount").css("margin-left", "13px");
                    $("#vendorNotificationCount").css("color", "red");
                }
            }
        },
        error: function (e) {
            console.log(e);
            // ajaxStop();
            sessionTimeout(e);
        }

    });

    $("#vendorOnboarding").click(function (event) {
        $.ajax({
            datatype: "json",
            type: "POST",
            url: 'vendorOnboardingForm',
            data: {

            },
            traditional: true,
            cache: false,
            success: function (response) {
                ajaxStop();
                if (response != null && !jQuery.isEmptyObject(response)) {
                    var tableStr = response['tableStr'];
                    var modalObj = {

                        title: 'Vendor Onboarding',
                        body: tableStr
                    };
                    var buttonArray = [
                        {
                        }
                    ];
                    modalObj['buttons'] = buttonArray;
                    $("#modalDailogDiv").remove();
                    $("body").append("<div id='modalDailogDiv'></div>");
                    createModal("modalDailogDiv", modalObj);
                    $("#modalDailogDiv").find(".modal-dialog").addClass("model-cust-rec-reg modal-xl opacity-animate3");

                    $("#SELECT_COUNTRY").change(function (event) {
                        var country = $("#SELECT_COUNTRY").val();
                        $.ajax({
                            datatype: "json",
                            type: "POST",
                            url: 'getStateList',
                            data: {
                                country: country
                            },
                            traditional: true,
                            cache: false,
                            success: function (response) {
                                ajaxStop();
                                if (response != null) {

                                    var selectBox = "<option>Select</option>";
                                    $.each(response, function (i) {
                                        var stateCode = this[0];
                                        var stateLabel = this[1];
                                        selectBox += "<option value='" + stateCode + "'>" + stateLabel + "</option>"
                                    })
                                    selectBox += "";
                                    $("#SELECT_STATE").html(selectBox);

                                    $("#SELECT_STATE").change(function (event) {
                                        var country = $("#SELECT_COUNTRY").val();
                                        var state = $("#SELECT_STATE").val();
                                        $.ajax({
                                            datatype: "json",
                                            type: "POST",
                                            url: 'getCityList',
                                            data: {
                                                country: country,
                                                state: state
                                            },
                                            traditional: true,
                                            cache: false,
                                            success: function (response) {
                                                ajaxStop();
                                                if (response != null) {
                                                    var selectBox = "<option>Select</option>";
                                                    $.each(response, function (i) {
                                                        selectBox += "<option>" + this + "</option>"
                                                    })
                                                    selectBox += "";
                                                    $("#SELECT_CITY").html(selectBox);
                                                }


                                            },
                                            error: function (e) {
                                                ajaxStop();
                                                alert('Error: ' + e);

                                            }
                                        });
                                    })


                                }

                            },
                            error: function (e) {
                                ajaxStop();
                                alert('Error: ' + e);

                            }
                        });
                    })

                    var unspscValues = response['COMBOBOX_UNSPSC'];
                    $("#COMBOBOX_UNSPSC").jqxComboBox({
                        source: unspscValues,
                        width: '436px',
                        height: '28px',
                        animationType: 'slide',
                        placeHolder: "Select Table"
                    });
                    $("#COMBOBOX_UNSPSC").find("input").attr("fieldName", "UNSPSC");

                }

                $("#registerVendor").click(function (event) {
                    ajaxStart();
                    var vendorDetails = {};
                    var fieldsArray = $("#msform").find(".form-group");
                    $.each(fieldsArray, function (i) {
                        var key, value;
                        if ($(this).find("input").length > 0) {
                            key = $(this).find("input").attr("fieldName");
                            value = $(this).find("input").val();
                        } else if ($(this).find("select").length > 0) {
                            key = $(this).find("select").attr("fieldName");
                            value = $(this).find("select").val();
                        }
                        vendorDetails[key] = value;
                    })
                    $.ajax({
                        datatype: "json",
                        type: "POST",
                        url: 'registerVendor',
                        data: {
                            vendorDetails: JSON.stringify(vendorDetails)
                        },
                        traditional: true,
                        cache: false,
                        success: function (response) {
                            ajaxStop();
                            if (response != null) {
                                if (response['errorFlag'] != null && response['errorFlag'] == "Y") {
                                    var message = response['message']

                                    $("#vendorOnboardingErrorMessage").css("display", "block");
                                    $("#vendorOnboardingErrorMessage").html(message)
                                } else {
                                    $("#vendorOnboardingErrorMessage").css("display", "none");
                                    $("#vendorOnboardingErrorMessage").html("")
                                    var message = response['message']
                                    var modalObj = {
                                        title: 'Message',
                                        body: message
                                    };
                                    var buttonArray = [
                                        {
                                            text: 'Close',
                                            click: function () {
                                            },
                                            isCloseButton: true
                                        }
                                    ];
                                    modalObj['buttons'] = buttonArray;
                                    $("#modalDailogDiv").remove();
                                    $("body").append("<div id='modalDailogDiv'></div>");
                                    createModal("modalDailogDiv", modalObj);
                                    $("#modalDailogDiv").find(".modal-dialog").addClass("modal-xs");
                                }

                            }
                        },
                        error: function (e) {
                            ajaxStop();
                            alert('Error: ' + e);

                        }
                    });
                })

                var current_fs, next_fs, previous_fs; //fieldsets
                var opacity;

                $(".next").click(function () {

                    current_fs = $(this).parent();
                    next_fs = $(this).parent().next();

                    //Add Class Active
                    $("#progressbar li").eq($("fieldset").index(next_fs)).addClass("active");

                    //show the next fieldset
                    next_fs.show();
                    //hide the current fieldset with style
                    current_fs.animate({opacity: 0}, {
                        step: function (now) {
                            // for making fielset appear animation
                            opacity = 1 - now;
                            current_fs.css({
                                'display': 'none',
                                'position': 'relative'
                            });
                            next_fs.css({'opacity': opacity});
                        },
                        duration: 600
                    });
                });

                $(".previous").click(function () {

                    current_fs = $(this).parent();
                    previous_fs = $(this).parent().prev();

                    //Remove class active
                    $("#progressbar li").eq($("fieldset").index(current_fs)).removeClass("active");

                    //show the previous fieldset
                    previous_fs.show();

                    //hide the current fieldset with style
                    current_fs.animate({opacity: 0}, {
                        step: function (now) {
                            // for making fielset appear animation
                            opacity = 1 - now;

                            current_fs.css({
                                'display': 'none',
                                'position': 'relative'
                            });
                            previous_fs.css({'opacity': opacity});
                        },
                        duration: 600
                    });
                });

                $('.radio-group .radio').click(function () {
                    $(this).parent().find('.radio').removeClass('selected');
                    $(this).addClass('selected');
                });

                $(".submit").click(function () {
                    return false;
                })

            },
            error: function (e) {
                ajaxStop();
                alert('Error: ' + e);

            }
        });

    });

});

function showMasterDataRegistry() {
    var gridId = 'DXP_UNIFICATION_PORTAL_GRID';
    $.ajax({
        type: "POST",
        url: 'getCloudGrid',
        data: {
            gridId: gridId,
        },
        traditional: true,
        cache: false,
        success: function (result) {

            var divstr = "<div class='dataUnificationMainDiv'>"
                    + "<div id='tabsdiv'>"
                    + "<div class ='registryTabHeader active' id='defaultTab' style='display:none;'  onclick='switchRegistryTabs(\"defaultTab\", \"treeGridDiv\")' ><p class='oddClass'>Default</p></div>"
                    + "<div class ='registryTabHeader' id='refernceDataTab' style='display:none;'  onclick='switchRegistryTabs(\"refernceDataTab\", \"refernceDataTabContent\")' ><p class='evenClass'>Reference</p></div>"
                    + "<div class ='registryTabHeader' id='charDataTab' style='display:none;'  onclick='switchRegistryTabs(\"charDataTab\", \"charDataTabContent\")' ><p class='oddClass'>Characteristics</p></div>"
//                    + "<div class ='registryTabHeader' id='classAllocationTab' style='display:none;'  onclick='switchRegistryTabs(\"classAllocationTab\", \"classAllocationTabContent\")' ><p class='evenClass'>Class Allocation</p></div>"
                    + "<div class ='registryTabHeader' id='dataProfilingTab' style='display:none;'  onclick='switchRegistryTabs(\"dataProfilingTab\", \"dataProfilingTabContent\")' ><p class='oddClass'>Data Profiling</p></div>"
                    + "<div class ='registryTabHeader' id='dataHealthAssessmentTab' style='display:none;'  onclick='switchRegistryTabs(\"dataHealthAssessmentTab\", \"dataHealthAssessmentTabContent\")' ><p class='evenClass'>DHA</p></div>"
                    + "<div class ='registryTabHeader' id='duplicateCheckTab' style='display:none;'  onclick='switchRegistryTabs(\"duplicateCheckTab\", \"duplicateCheckTabContent\")' ><p class='evenClass'>Duplicates</p></div>"
                    + "</div>"
                    + "<div id='tabsContentdiv'>"
                    + "<div class ='registryTabContent' id='treeGridDiv'></div>"
                    + "<div class ='registryTabContent' id='refernceDataTabContent'></div>"
                    + "<div class ='registryTabContent' id='charDataTabContent'></div>"
//                    + "<div class ='registryTabContent' id='classAllocationTabContent'></div>"
                    + "<div class ='registryTabContent' id='dataProfilingTabContent'></div>"
                    + "<div class ='registryTabContent' id='dataHealthAssessmentTabContent'></div>"
                    + "<div class ='registryTabContent' id='duplicateCheckTabContent'></div>"
                    + "</div>"
                    + "</div>";

            $("#pageBodyContent").remove();
            $("#pageBody").append('<div class="page-body-content" id="pageBodyContent">' + divstr + '</div></div>');

            var paramObj = {};

//          paramObj.column = 'RECORD_NO';
//          paramObj.value = '3120002935423';
            paramObj.column = '1';
            paramObj.value = '1';
            paramObj.operator = "EQUALS";
            paramObj.symbol = "Euqals";
            gridNewConfigPoc(result, "", paramObj, gridId);
        }
    });
}

function gridNewConfigPoc(gridResultObj, selectedGridIndex, paramObj, selectedGridId) {

    ajaxStart();
    // ravi start 
    globalTabId = gridResultObj['gridId'];
    // ravi end 
    console.log(":293::gridConfig::");
    var parentDiv;
    if (gridResultObj['gridId'] == "MM_REFERENCE") {
        parentDiv = "refernceDataTabContent";
    } else if (gridResultObj['gridId'] == "MM_MASTER_O_RECORD_CHAR") {
        parentDiv = "charDataTabContent";
    } else {
        parentDiv = "treeGridDiv"
    }
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


function showReferenceDataInputForm(gridId, operationName) {
    var selectBatchIdsList = "";
    $.ajax({
        type: "POST",
        url: 'getBatchIdList',
        dataType: 'json',
        data: {
            tableName: 'O_RECORD_DATA_UNIFICATION_STG'
        },
        traditional: true,
        cache: false,
        success: function (response) {
            if (response != null) {
                var batchIdList = response['batchIdList'];
                selectBatchIdsList = "<select id= 'selectBatchId'>"
                $.each(batchIdList, function (i) {
                    selectBatchIdsList += "<option>" + this + "</option>";
                })
            }
            selectBatchIdsList += "</select>";
            var body = "<div> Select BatchId : " + selectBatchIdsList + "</div>";
            var modalObj = {
                title: 'Select Batch Id',
                body: body
            };
            var buttonArray = [
                {
                    text: 'Submit',
                    click: function () {
                        var batchId = $("#selectBatchId").val();
                        getReferenceData(gridId, operationName, batchId);
                    },
                    isCloseButton: true
                }
            ];
            modalObj['buttons'] = buttonArray;
            $("#modalDailogDiv").remove();
            $("body").append("<div id='modalDailogDiv'></div>");
            createModal("modalDailogDiv", modalObj);
            $("#modalDailogDiv").find(".modal-dialog").addClass("modal-xs");
        }
    });

}
function getReferenceData(gridId, operationName, batchId) {
    showLoader();
    $.ajax({
        type: "POST",
        url: 'refernceDataUpdate',
        data: {
            'gridId': gridId,
            'batchId': batchId,
            'operationName': operationName
        },
        traditional: true,
        cache: false,
        success: function (result) {
            stopLoader();
            var modalObj = {
                title: 'Reference Data',
                body: result + "<br><span onclick=\"viewGridData('MM_REFERENCE','refernceDataTabContent')\">View Data</span>"
            };
            var buttonArray = [
                {
                    text: 'Close',
                    click: function () {
//                        switchRegistryTabs('refernceDataTab', 'refernceDataTabContent');
                    },
                    isCloseButton: true
                }
            ];
            modalObj['buttons'] = buttonArray;
            $("#modalDailogDiv").remove();
            $("body").append("<div id='modalDailogDiv'></div>");
            createModal("modalDailogDiv", modalObj);
            $("#modalDailogDiv").find(".modal-dialog").addClass("modal-xs");
        },
        error: function (error) {
            console.error(error);
        }
    });
}


function showClassAllocDataInputForm(gridId, operationName) {

    var selectBatchIdsList = "";
    $.ajax({
        type: "POST",
        url: 'getBatchIdList',
        dataType: 'json',
        data: {
            tableName: 'O_RECORD_DATA_UNIFICATION_STG'
        },
        traditional: true,
        cache: false,
        success: function (response) {

            if (response != null) {
                var batchIdList = response['batchIdList'];
                selectBatchIdsList = "<select id= 'selectBatchId'>"
                $.each(batchIdList, function (i) {
                    selectBatchIdsList += "<option>" + this + "</option>";
                })
            }
            selectBatchIdsList += "</select>";
            var body = "<div> Select BatchId : " + selectBatchIdsList + "</div>";
            var modalObj = {
                title: 'Select Batch Id',
                body: body
            };
            var buttonArray = [
                {
                    text: 'Submit',
                    click: function () {
                        var batchId = $("#selectBatchId").val();
//                        getReferenceData(gridId, operationName, batchId);
                        getClassAllocationData(gridId, operationName, batchId);
                    },
                    isCloseButton: true
                }
            ];
            modalObj['buttons'] = buttonArray;
            $("#modalDailogDiv").remove();
            $("body").append("<div id='modalDailogDiv'></div>");
            createModal("modalDailogDiv", modalObj);
            $("#modalDailogDiv").find(".modal-dialog").addClass("modal-xs");
        }
    });

}
function getClassAllocationData(gridId, operationName, batchId) {
    showLoader();
    $.ajax({
        type: "POST",
        url: 'classAllowcationUpdate',
        data: {
            'gridId': gridId,
            'operationName': operationName,
            'batchId': batchId
        },
        traditional: true,
        cache: false,
        success: function (result) {
            stopLoader();
            var modalObj = {
                title: 'Class Allocation',
                body: result
            };
            var buttonArray = [
                {
                    text: 'Close',
                    click: function () {
//                        switchRegistryTabs()
                    },
                    isCloseButton: true
                }
            ];
            modalObj['buttons'] = buttonArray;
            $("#modalDailogDiv").remove();
            $("body").append("<div id='modalDailogDiv'></div>");
            createModal("modalDailogDiv", modalObj);
            $("#modalDailogDiv").find(".modal-dialog").addClass("modal-xs");
        }
    });
}

function showCharDataInputForm(gridId, operationName) {
    showLoader();
    var selectBatchIdsList = "";
    $.ajax({
        type: "POST",
        url: 'getBatchIdList',
        dataType: 'json',
        data: {
            tableName: 'O_RECORD_DATA_UNIFICATION_STG'
        },
        traditional: true,
        cache: false,
        success: function (response) {
            stopLoader();
            if (response != null) {
                var batchIdList = response['batchIdList'];
                selectBatchIdsList = "<select id= 'selectBatchId'>"
                $.each(batchIdList, function (i) {
                    selectBatchIdsList += "<option>" + this + "</option>";
                })
            }
            selectBatchIdsList += "</select>";
            var body = "<div> Select BatchId : " + selectBatchIdsList + "</div>";
            var modalObj = {
                title: 'Select Batch Id',
                body: body
            };
            var buttonArray = [
                {
                    text: 'Submit',
                    click: function () {
                        var batchId = $("#selectBatchId").val();
//                        getReferenceData(gridId, operationName, batchId);
                        getCharData(gridId, operationName, batchId);
                    },
                    isCloseButton: true
                }
            ];
            modalObj['buttons'] = buttonArray;
            $("#modalDailogDiv").remove();
            $("body").append("<div id='modalDailogDiv'></div>");
            createModal("modalDailogDiv", modalObj);
            $("#modalDailogDiv").find(".modal-dialog").addClass("modal-xs");
        }
    });

}
function getCharData(gridId, operationName, batchId) {
    showLoader();
    $.ajax({
        type: "POST",
        url: 'charDataUpdate',
        data: {
            'gridId': gridId,
            'operationName': operationName,
            'batchId': batchId
        },
        traditional: true,
        cache: false,
        success: function (result) {
            stopLoader();
            var modalObj = {
                title: 'Char Data',
                body: result + "<br><span onclick=\"viewGridData('MM_MASTER_O_RECORD_CHAR','charDataTabContent')\">View Data</span>"
            };
            var buttonArray = [
                {
                    text: 'Close',
                    click: function () {
//                        switchRegistryTabs('charDataTab', 'charDataTabContent');
                    },
                    isCloseButton: true
                }
            ];
            modalObj['buttons'] = buttonArray;
            $("#modalDailogDiv").remove();
            $("body").append("<div id='modalDailogDiv'></div>");
            createModal("modalDailogDiv", modalObj);
            $("#modalDailogDiv").find(".modal-dialog").addClass("modal-xs");
        }
    });
}

function showDataProfilingInputForm(gridId, operationName) {
    var selectBatchIdsList = "";
    $.ajax({
        type: "POST",
        url: 'getBatchIdList',
        dataType: 'json',
        data: {
            tableName: 'O_RECORD_DATA_UNIFICATION_STG'
        },
        traditional: true,
        cache: false,
        success: function (response) {
            if (response != null) {
                var batchIdList = response['batchIdList'];
                selectBatchIdsList = "<select id= 'selectBatchId'>"
                $.each(batchIdList, function (i) {
                    selectBatchIdsList += "<option>" + this + "</option>";
                })
            }
            selectBatchIdsList += "</select>";
            var body = "<div> Select BatchId : " + selectBatchIdsList + "</div>";
            var modalObj = {
                title: 'Select Batch Id',
                body: body
            };
            var buttonArray = [
                {
                    text: 'Submit',
                    click: function () {
                        var batchId = $("#selectBatchId").val();
                        getDataProfiling(gridId, operationName, batchId);
                    },
                    isCloseButton: true
                }
            ];
            modalObj['buttons'] = buttonArray;
            $("#modalDailogDiv").remove();
            $("body").append("<div id='modalDailogDiv'></div>");
            createModal("modalDailogDiv", modalObj);
            $("#modalDailogDiv").find(".modal-dialog").addClass("modal-xs");
        }
    });

}

function getDataProfiling(gridId, operationName, batchId) {
    showLoader();
    $.ajax({
        type: "POST",
        url: 'dataProfilingUpdate',
        data: {
            gridId: gridId,
            operationName: operationName,
            batchId: batchId
        },
        traditional: true,
        cache: false,
        success: function (result) {
            stopLoader();
            switchRegistryTabs('dataProfilingTab', 'dataProfilingTabContent');
            $("#dataProfilingTabContent").html("");
//            $("#dataProfilingTabContent").append(result);

            switchRegistryTabs('dataProfilingTab', 'dataProfilingTabContent');
            var pdfContent = "<iframe id='dataProfilingIframe' style='width: 100%; height:650px;' srcdoc='' onload='setSrcdoc()'></iframe>";
            $("#dataProfilingTabContent").html(pdfContent);
            var downloadDataButton = "<div id='downloadiFrameHtmlButton'><button onclick=downloadiFrameHtml('dataProfilingIframe')>Download</button></div>"
            $('#dataProfilingIframe').attr('srcdoc', result);
            $("#dataProfilingTabContent").append(downloadDataButton);


        }
    });
}

function downloadiFrameHtml(iframeId) {
    var elHtml = $("#" + iframeId).attr("srcdoc");
    var link = document.createElement('a');
    var mimeType = 'text/html';

    link.setAttribute('download', 'dataProfiling');
    link.setAttribute('href', 'data:' + mimeType + ';charset=utf-8,' + encodeURIComponent(elHtml));
    link.click();
}

function showDHAInputForm(gridId, operationName) {
    var selectBatchIdsList = "";
    $.ajax({
        type: "POST",
        url: 'getBatchIdList',
        dataType: 'json',
        data: {
            tableName: 'O_RECORD_DATA_UNIFICATION_STG'
        },
        traditional: true,
        cache: false,
        success: function (response) {
            if (response != null) {
                var batchIdList = response['batchIdList'];
                selectBatchIdsList = "<select id= 'selectBatchId'>"
                $.each(batchIdList, function (i) {
                    selectBatchIdsList += "<option>" + this + "</option>";
                })
            }
            selectBatchIdsList += "</select>";
            var body = "<div> Select BatchId : " + selectBatchIdsList + "</div>";
            var modalObj = {
                title: 'Select Batch Id',
                body: body
            };
            var buttonArray = [
                {
                    text: 'Submit',
                    click: function () {
                        var batchId = $("#selectBatchId").val();
                        getDHAData(gridId, operationName, batchId);
                    },
                    isCloseButton: true
                }
            ];
            modalObj['buttons'] = buttonArray;
            $("#modalDailogDiv").remove();
            $("body").append("<div id='modalDailogDiv'></div>");
            createModal("modalDailogDiv", modalObj);
            $("#modalDailogDiv").find(".modal-dialog").addClass("modal-xs");
        }
    });
}

function getDHAData(gridId, operationName, batchId) {
    var iframeLoadFlaf = false;
    switchRegistryTabs('dataHealthAssessmentTab', 'dataHealthAssessmentTabContent');
    var pdfContent = "<iframe id='DHAIframe' style='width: 100%; height:650px;' src='dataHealthAssessmentUpdate?gridId=" + gridId + "&batchId=" + batchId + "&operationName=" + operationName + "' ></iframe>";
    $("#dataHealthAssessmentTabContent").html(pdfContent);
    var body = '<div id="myProgress"><div id="myBar"></div></div>';
    var modalObj = {
        title: 'Processing...',
        body: body
    };
    var buttonArray = [
        {
        }
    ];
    modalObj['buttons'] = buttonArray;
    $("#modalDailogDiv").remove();
    $("body").append("<div id='modalDailogDiv'></div>");
    createModal("modalDailogDiv", modalObj);
    $("#modalDailogDiv").find(".modal-dialog").addClass("modal-xs");
    move('myBar', 50000);

    $('#DHAIframe').on("load", function () {
        iframeLoadFlaf = true;

    });

    function move(divId, time) {
        var i = 0;
        var intervel = time / 1000;
        if (i == 0) {
            i = 1;
            var elem = document.getElementById(divId);
            var width = 0;
            var id = setInterval(frame, intervel);
            function frame() {
                if (width >= 100) {
                    clearInterval(id);
                    i = 0;
                } else {

                    if (iframeLoadFlaf) {
                        width = 100;
                        elem.style.width = width + "%";
                        $('#dialog').modal('hide');
                    }
                    width = width + 0.1;
                    elem.style.width = width + "%";
                }
            }
        }
    }


}



function viewGridData(gridId, appendDiv) {
    $.ajax({
        type: "POST",
        url: 'getCloudGrid',
        data: {
            gridId: gridId,
        },
        traditional: true,
        cache: false,
        success: function (result) {
            if (appendDiv == null) {
                appendDiv = 'treeGridDiv';
            }
//            $("#treeGridDiv").html("");
//            $("#treeGridDiv").html("<div id='" + gridId + "'></div>");
            $("#" + appendDiv).html("");
            $("#" + appendDiv).html("<div id='" + gridId + "'></div>");
//            formGrid(gridId, result);
//                $("#" + gridId).jqxGrid(result.gridConfigObj);
            var paramObj = {};

//                paramObj.column = 'RECORD_NO';
//                paramObj.value = '3120002935423';
            paramObj.column = '1';
            paramObj.value = '1';
            paramObj.operator = "EQUALS";
            paramObj.symbol = "Euqals";
            gridNewConfig(result, "", paramObj, gridId);
            if (gridId == "MM_REFERENCE") {
                switchRegistryTabs('refernceDataTab', 'refernceDataTabContent');
            } else if (gridId == "MM_MASTER_O_RECORD_CHAR") {
                switchRegistryTabs('charDataTab', 'charDataTabContent');
            }

            $('#dialog').modal('hide');

        }
    });
}

function switchRegistryTabs(tabHeaderId, tabContentDivId) {
    $(".registryTabHeader").removeClass("active");
    $(".registryTabContent").hide();
    $("#" + tabHeaderId).addClass("active");

    $("#tabsdiv").css("width", "2%");
    $("#tabsContentdiv").css("width", "97%");
    $("#defaultTab").show();
    $("#" + tabHeaderId).show();
    $("#" + tabContentDivId).show();
}

function linkVendor(gridId, row) {
//    var selectedrowindex = $("#" + gridId).jqxGrid('selectedrowindex');
//    if (selectedrowindex == -1) {
//        selectedrowindex = globalSelectedRow;
//    }
    var rowData = $("#" + gridId).jqxGrid('getrowdata', row);
    var recordNo = rowData['RECORD_NO'];
    $("#" + gridId).jqxGrid()
    $.ajax({
        type: "POST",
        url: 'linkVendor',
        dataType: 'json',
        data: {
            gridId: gridId,
            recordNo: recordNo
        },
        traditional: true,
        cache: false,
        success: function (response) {
            if (response != null) {
                var message = response['message'];

                var modalObj = {
                    title: 'Link Vendor',
                    body: message
                };
                var buttonArray = [
                    {
                        text: 'ok',
                        click: function () {

                        },
                        isCloseButton: true
                    }
                ];
                modalObj['buttons'] = buttonArray;
                $("#modalDailogDiv").remove();
                $("body").append("<div id='modalDailogDiv'></div>");
                createModal("modalDailogDiv", modalObj);
                $("#modalDailogDiv").find(".modal-dialog").addClass("modal-xs");
            }

        }
    });
}

function vendorsList(gridId, selectedrowindex) {
//    var selectedrowindex = $("#" + gridId).jqxGrid('selectedrowindex');
//    if (selectedrowindex == -1) {
//        selectedrowindex = globalSelectedRow;
//    }
    var rowData = $("#" + gridId).jqxGrid('getrowdata', selectedrowindex);
    var recordNo = rowData['RECORD_NO'];
    $("#" + gridId).jqxGrid()
    $.ajax({
        type: "POST",
        url: 'fetchVendorsList',
        dataType: 'json',
        data: {
            gridId: gridId,
            recordNo: recordNo
        },
        traditional: true,
        cache: false,
        success: function (response) {
            if (response != null) {
                var vendorsList = response['vendorsList'];
                var divstr = "";
                if (vendorsList != null) {
                    divstr = '<div id="accordion" class="vendorListPopUp">';
                    $.each(vendorsList, function (i) {
                        var vendorName = vendorsList[i];
                        var regex = new RegExp(" ", 'g');
                        vendorName = vendorName.replace(regex, "&nbsp;");
                        divstr += '<div class="card card-vendorDetails">'
                                + '<div class="card-header" id="headingOne_' + i + '">'
                                + '<h5 class="mb-0">'
                                + '<button class="btn btn-link" data-toggle="collapse" data-target="#collapseOne_' + i + '" aria-expanded="true" aria-controls="collapseOne_' + i + '" onclick=fetchVendorDetails(\'' + vendorName + '\',\'cardBodyId_' + i + '\',\'' + gridId + '\',\'' + selectedrowindex + '\')>'
                                + vendorName + '<i class="fa fa-angle-down" aria-hidden="true"></i>'
                                + '</button>'
                                + '</h5>'
                                + '</div>'
                                + '<div id="collapseOne_' + i + '" class="collapse" aria-labelledby="headingOne_' + i + '" data-parent="#accordion" style="">'
                                + '<div class="card-body" id="cardBodyId_' + i + '" >'
                                + vendorName
                                + '</div>'
                                + '</div>'
                                + '</div>'

                        //                    divstr += "<div><span onclick=fetchVendorDetails(\'" + vendorName + "\') style='cursor: pointer; font-weight: bold;'>" + vendorName + "</span><div id='vendorDetailsDiv_" + vendorName + "' style='display:none;'></div></div><br>"
                    })
                    divstr += '</div>';
                } else {
                    divstr = 'No vendors Linked';
                }

                var modalObj = {
                    title: 'Linked Vendors',
                    body: divstr
                };
                var buttonArray = [
                    {
                        text: 'ok',
                        click: function () {
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
                $("#modalDailogDiv").find(".modal-dialog").addClass("modal-xs");
            }

        }
    });
}

function suggestedVendorsList(gridId, selectedrowindex) {
//    var selectedrowindex = $("#" + gridId).jqxGrid('selectedrowindex');
//    if (selectedrowindex == -1) {
//        selectedrowindex = globalSelectedRow;
//    }
    var rowData = $("#" + gridId).jqxGrid('getrowdata', selectedrowindex);
    var UNSPSC = rowData['UNSPSC'];
    $("#" + gridId).jqxGrid()
    $.ajax({
        type: "POST",
        url: 'fetchSuggestedVendorsList',
        dataType: 'json',
        data: {
            gridId: gridId,
            UNSPSC: UNSPSC
        },
        traditional: true,
        cache: false,
        success: function (response) {
            if (response != null) {
                var vendorsList = response['vendorsList'];
                var divstr = "";
                if (vendorsList != null) {
                    divstr = '<div id="accordion" class="vendorListPopUp">';
                    $.each(vendorsList, function (i) {
                        var vendorName = vendorsList[i];
                        var regex = new RegExp(" ", 'g');
                        vendorName = vendorName.replace(regex, "&nbsp;");
                        divstr += '<div class="card card-vendorDetails">'
                                + '<div class="card-header" id="headingOne_' + i + '">'
                                + '<h5 class="mb-0">'
                                + '<button class="btn btn-link" data-toggle="collapse" data-target="#collapseOne_' + i + '" aria-expanded="true" aria-controls="collapseOne_' + i + '" onclick=fetchVendorDetails(\'' + vendorName + '\',\'cardBodyId_' + i + '\',\'' + gridId + '\',\'' + selectedrowindex + '\')>'
                                + vendorName + '<i class="fa fa-angle-down" aria-hidden="true"></i>'
                                + '</button>'
                                + '</h5>'
                                + '</div>'
                                + '<div id="collapseOne_' + i + '" class="collapse" aria-labelledby="headingOne_' + i + '" data-parent="#accordion" style="">'
                                + '<div class="card-body" id="cardBodyId_' + i + '" >'
                                + vendorName
                                + '</div>'
                                + '</div>'
                                + '</div>'

                        //                    divstr += "<div><span onclick=fetchVendorDetails(\'" + vendorName + "\') style='cursor: pointer; font-weight: bold;'>" + vendorName + "</span><div id='vendorDetailsDiv_" + vendorName + "' style='display:none;'></div></div><br>"
                    })
                    divstr += '</div>';

                } else {
                    divstr = 'No Data';
                }
                var modalObj = {
                    title: 'Suggested Vendors',
                    body: divstr
                };
                var buttonArray = [
                    {
                        text: 'ok',
                        click: function () {

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
                $("#modalDailogDiv").find(".modal-dialog").addClass("modal-xs");
            }

        }
    });
}

function fetchVendorDetails(vendor, tabContentId, gridId, selectedrowindex) {

    $.ajax({
        type: "POST",
        url: 'fetchVendorDetails',
        dataType: 'json',
        data: {
            vendor: vendor,
            gridId: gridId,
            selectedrowindex: selectedrowindex
        },
        traditional: true,
        cache: false,
        success: function (response) {
            if (response != null) {
                var tableStr = response['tableStr'];


                $("#" + tabContentId).html("");
                $("#" + tabContentId).html(tableStr);
                var width = $('.modal-dialog').css('max-width');
                $('.modal-dialog').removeClass('normal');
                $('.modal-dialog').addClass('grow');
//                    $('.modal-dialog').css('max-width', '1200px');
//                    $("#vendorDetailsDiv_" + vendor).show();
                ;

            }

        }
    });


}

function duplicateCheckInputForm(gridId, operationName) {
    var selectBatchIdsList = "";
    $.ajax({
        type: "POST",
        url: 'getBatchIdList',
        dataType: 'json',
        data: {
            tableName: 'O_RECORD_DATA_UNIFICATION_STG'
        },
        traditional: true,
        cache: false,
        success: function (response) {
            if (response != null) {
                var batchIdList = response['batchIdList'];
                selectBatchIdsList = "<select id= 'selectBatchId'>"
                $.each(batchIdList, function (i) {
                    selectBatchIdsList += "<option>" + this + "</option>";
                })
            }
            selectBatchIdsList += "</select>";

            var duplicateCheckTypes = "<div> Select Duplicate Check Type : "
                    + "<select id= 'selectDuplicateCheckType'>"
                    + "<option>Select</option>"
                    + "<option value='POMATCH' >Description</option>"
//                                    +"<option value='PDR1' >Characteristics</option>"
                    + "<option value='PDR2' >Reference Data</option>"
                    + "</select>"
                    + "</div>";

            var body = duplicateCheckTypes + "<div> Select BatchId : " + selectBatchIdsList + "</div>";
            var modalObj = {
                title: 'Select Batch Id',
                body: body
            };
            var buttonArray = [
                {
                    text: 'Submit',
                    click: function () {
                        var batchId = $("#selectBatchId").val();
                        var duplicateCheckType = $("#selectDuplicateCheckType").val();
                        if (duplicateCheckType != null && duplicateCheckType != "" && duplicateCheckType == "POMATCH") {
                            duplicateCheckPOMatch("DISCRIPTION_MATCHING_GRID", operationName, batchId, duplicateCheckType);
                        } else if (duplicateCheckType != null && duplicateCheckType != "" && duplicateCheckType == "PDR2") {
                            duplicateCheckPDR2("REFERENCE_DATA_MATCHING_GRID", operationName, batchId, duplicateCheckType);
                        }

                    },
                    isCloseButton: true
                }
            ];
            modalObj['buttons'] = buttonArray;
            $("#modalDailogDiv").remove();
            $("body").append("<div id='modalDailogDiv'></div>");
            createModal("modalDailogDiv", modalObj);
            $("#modalDailogDiv").find(".modal-dialog").addClass("modal-xs");
        }
    });
}

function bulkRegisterInputForm(gridId) {
    var selectBatchIdsList = "";
    $.ajax({
        type: "POST",
        url: 'getBatchIdList',
        dataType: 'json',
        data: {
            tableName: 'O_RECORD_DATA_UNIFICATION_STG'
        },
        traditional: true,
        cache: false,
        success: function (response) {
            if (response != null) {
                var batchIdList = response['batchIdList'];
                selectBatchIdsList = "<select id= 'selectBatchId'>"
                $.each(batchIdList, function (i) {
                    selectBatchIdsList += "<option>" + this + "</option>";
                })
            }
            selectBatchIdsList += "</select>";
            var body = "<div> Select BatchId : " + selectBatchIdsList + "</div>";
            var modalObj = {
                title: 'Select Batch Id',
                body: body
            };
            var buttonArray = [
                {
                    text: 'Submit',
                    click: function () {
                        var batchId = $("#selectBatchId").val();
                        bulkRegister(gridId, batchId);
                    },
                    isCloseButton: true
                }
            ];
            modalObj['buttons'] = buttonArray;
            $("#modalDailogDiv").remove();
            $("body").append("<div id='modalDailogDiv'></div>");
            createModal("modalDailogDiv", modalObj);
            $("#modalDailogDiv").find(".modal-dialog").addClass("modal-xs");
        }
    });
}

function bulkRegister(gridId, batchId) {
    showLoader();
    $.ajax({
        type: "POST",
        url: 'bulkRegister',
        dataType: 'json',
        data: {
            gridId: gridId,
            batchId: batchId
        },
        traditional: true,
        cache: false,
        success: function (response) {
            stopLoader();
//            switchRegistryTabs('duplicateCheckTab', 'duplicateCheckTabContent');
            if (response != null) {
                var message = response['message'];

                var modalObj = {
                    title: 'Message',
                    body: message
                };
                var buttonArray = [
                    {
                        text: 'Ok',
                        click: function () {

                        },
                        isCloseButton: true
                    }
                ];
                modalObj['buttons'] = buttonArray;
                $("#modalDailogDiv").remove();
                $("body").append("<div id='modalDailogDiv'></div>");
                createModal("modalDailogDiv", modalObj);
                $("#modalDailogDiv").find(".modal-dialog").addClass("modal-xs");
            }

        },
        error: function (error) {

        }
    });
}

function duplicateCheckPOMatch(gridId, operationName, batchId, duplicateCheckType) {
    showLoader();
    $.ajax({
        type: "POST",
        url: 'duplicateCheck',
        dataType: 'json',
        data: {
            gridId: gridId,
            operationName: operationName,
            batchId: batchId,
            duplicateCheckType: duplicateCheckType,
        },
        traditional: true,
        cache: false,
        success: function (response) {
            stopLoader();
            switchRegistryTabs('duplicateCheckTab', 'duplicateCheckTabContent');
            if (response != null) {
                $("#duplicateCheckTabContent").html("<div id='" + gridId + "'></div>");
                var dataArray = response['dataArray'];
//                var columnsList = response['columnsList'];
                var dataFieldsArray = [];
                var columnsArray = [];

                var showActionButton = function (row, column, value) {
                    return "Register";
                };
                var registerItem = function (row) {

                    registerSelectedItem(gridId, row, "POMATCH");
                };
                
                var colsString = "INPUT_RECORD_NO,RECORD_NO,INPUT_LONG_DESCRIPTION,LONG_TEXT,REFERENCE_NO,REFERENCE_TYPE,VENDOR_NAME,UNMATCHED_WORDS,INPUT_DESC_PERCENTAGE,EXISTING_SRC_DESC_PERCENTAGE,GROUP,BATCH_ID,Action";
            
                var colsLabelsString = "New Record No,Existing Record No,New Description,Existing Description,Existing Reference No,Existing Reference Type,Existing Vendor Name,Unmatched Words,Match Percentage,Existing Source Match ,Group,Batch Id,Action";
                
                var columnsList = colsString.split(",");
                var colsLabelsList = colsLabelsString.split(",");
                $.each(columnsList, function (i) {
                    var dataFieldsObj = {};
                    dataFieldsObj['name'] = this;
                    dataFieldsObj['type'] = 'string';
                    dataFieldsArray.push(dataFieldsObj);

                    var columnsObject = {};
                    columnsObject['text'] = colsLabelsList[i];
                    columnsObject['datafield'] = this;
                    columnsObject['width'] = 100;
                    columnsObject['sortable'] = true;
                    if (this == "INPUT_LONG_DESCRIPTION" || this == "LONG_TEXT" || this == "Unmatched_words") {
                        columnsObject['width'] = 400;
                    }
                    if (this == "BATCH_ID" || this == "Unmatched_words" || this == "GROUP") {
                        columnsObject['hidden'] = true;
                    }
                    if (this == "Action") {
                        columnsObject['columntype'] = 'button';
                        columnsObject['cellsrenderer'] = showActionButton;
                        columnsObject['buttonclick'] = registerItem;
                    }
                    columnsArray.push(columnsObject);
                })
                var source =
                        {
                            localdata: dataArray,
                            datatype: "array",
                            datafields: dataFieldsArray,
                            sort: function ()
                            {
//                                                $("#" + gridResultObj['gridId'] + "_sort_columns").remove();
                                $("[id='" + gridId + "']").jqxGrid('updatebounddata', 'sort');
                                try {
                                    $("[id='" + gridId + "']").jqxGrid('clearselection');
                                } catch (e) {
                                }
                                stopLoader();
                            },
                            filter: function () {

                                $("[id='" + gridId + "']").jqxGrid('updatebounddata', 'filter');
                                try {
                                    $("[id='" + gridId + "']").jqxGrid('clearselection');
                                } catch (e) {
                                }
                                stopLoader();
                            }
                        };

                var dataAdapter = new $.jqx.dataAdapter(source);
//                var tabHeight = $("#" + gridId).closest(".jqx-tabs-content-element").height();

                $("[id='" + gridId + "']").jqxGrid(
                        {
                            width: "100%",
                            height: "620",
//                            theme: 'energyblue',
                            autoshowloadelement: false,
                            source: dataAdapter,
                            pageable: true,
                            pagesize: 50,
                            showfilterrow: true,
                            filterable: true,
                            sortable: true,
                            virtualmode: true,
                            pagesizeoptions: ['50', '100', '500', '1000', '5000', '10000', '50000'],
                            enabletooltips: true,
                            enablemousewheel: true,
                            enablehover: true,
                            selectionmode: 'checkbox',
                            enablebrowserselection: true,
                            rendergridrows: function (params) {
                                return params.data;
                            },
                            columnsresize: true,
                            columns: columnsArray
                        });

            }

        },
        error: function (error) {

        }
    });
}


function duplicateCheckPDR2(gridId, operationName, batchId, duplicateCheckType) {
    showLoader();
    $.ajax({
        type: "POST",
        url: 'duplicateCheck',
        dataType: 'json',
        data: {
            gridId: gridId,
            operationName: operationName,
            batchId: batchId,
            duplicateCheckType: duplicateCheckType,
        },
        traditional: true,
        cache: false,
        success: function (response) {
            stopLoader();
            switchRegistryTabs('duplicateCheckTab', 'duplicateCheckTabContent');
            if (response != null) {
                $("#duplicateCheckTabContent").html("<div id='" + gridId + "'></div>");
                var dataArray = response['dataArray'];
//                var columnsList = response['columnsList'];
                var dataFieldsArray = [];
                var columnsArray = [];

                var showActionButton = function (row, column, value) {
                    return "Update Char/Ref Data";
                };
                var updateCharRefData = function (row) {
//                    var duplicateCheckFlag = "Y";
                    updateCharRefDataPDR2(gridId, row, "PDR2");
                };

                var colsString = "SOURCE_RECORD_NO,RECORD_NO,REFERENCE_NO,REFERENCE_TYPE,REGISTERED_RECORD_NO,CLASS,LONG_TEXT,Action";
                var colsLabelsString = "Source Record No,Matched Record No,Reference No,Reference Type,Registered Record No,Class,Long Text,Action";
                var columnsList = colsString.split(",");
                var colsLabelsList = colsLabelsString.split(",");
                $.each(columnsList, function (i) {
                    var dataFieldsObj = {};
                    dataFieldsObj['name'] = this;
                    dataFieldsObj['type'] = 'string';
                    dataFieldsArray.push(dataFieldsObj);

                    var columnsObject = {};
                    columnsObject['text'] = colsLabelsList[i];
                    columnsObject['datafield'] = this;
                    columnsObject['width'] = 300;
                    columnsObject['sortable'] = true;
//                    
                    if (this == "REGISTERED_RECORD_NO" || this == "CLASS" || this == "LONG_TEXT") {
                        columnsObject['hidden'] = true;
                    }
                    if (this == "Action") {
                        columnsObject['columntype'] = 'button';
                        columnsObject['cellsrenderer'] = showActionButton;
                        columnsObject['buttonclick'] = updateCharRefData;
                    }
                    columnsArray.push(columnsObject);
                })
                var source =
                        {
                            localdata: dataArray,
                            datatype: "array",
                            datafields: dataFieldsArray,
                            sort: function ()
                            {
//                                                $("#" + gridResultObj['gridId'] + "_sort_columns").remove();
                                $("[id='" + gridId + "']").jqxGrid('updatebounddata', 'sort');
                                try {
                                    $("[id='" + gridId + "']").jqxGrid('clearselection');
                                } catch (e) {
                                }
                                stopLoader();
                            },
                            filter: function () {

                                $("[id='" + gridId + "']").jqxGrid('updatebounddata', 'filter');
                                try {
                                    $("[id='" + gridId + "']").jqxGrid('clearselection');
                                } catch (e) {
                                }
                                stopLoader();
                            }
                        };

                var dataAdapter = new $.jqx.dataAdapter(source);
//                var tabHeight = $("#" + gridId).closest(".jqx-tabs-content-element").height();

                $("[id='" + gridId + "']").jqxGrid(
                        {
                            width: "100%",
                            height: "620",
//                            theme: 'energyblue',
                            autoshowloadelement: false,
                            source: dataAdapter,
                            pageable: true,
                            pagesize: 50,
                            showfilterrow: true,
                            filterable: true,
                            sortable: true,
                            virtualmode: true,
                            pagesizeoptions: ['50', '100', '500', '1000', '5000', '10000', '50000'],
                            enabletooltips: true,
                            enablemousewheel: true,
                            enablehover: true,
                            selectionmode: 'checkbox',
                            enablebrowserselection: true,
                            rendergridrows: function (params) {
                                return params.data;
                            },
                            columnsresize: true,
                            columns: columnsArray
                        });

            }

        },
        error: function (error) {

        }
    });
}

function registerSelectedItem(gridId, row, duplicateCheckFlag) {
    var rowData = $("#" + gridId).jqxGrid('getrowdata', row);

    $.ajax({
        type: "POST",
        url: 'checkRecordRegistered',
        dataType: 'json',
        data: {
            rowData: JSON.stringify(rowData)
        },
        traditional: true,
        cache: false,
        success: function (response) {
            stopLoader();
            if (response['recordRegistered'] != "Y") {
                var description = "";

                description = rowData['LONG_TEXT']
                var currentDataContent = 1;
                var body = "<div id='charRefData'>"
                        + "<div ><textarea id='recordDescription'  rows='3' cols='120' >" + description + "</textarea></div>"
                        + "<div id='characteristicsData' class='recordDataConentTabs' data-function=showCharacteristics('" + gridId + "','" + row + "') ></div>"
                        + "<div id='referenceData'  class='recordDataConentTabs' data-function=showReferenceData('" + gridId + "','" + row + "') style='display:none;'></div>"
                        + "</div>"
                        + "<div id='buttonsDiv' style='float:right;margin:10px;'>"
                        + "<button id='prevButton'>Prev</button>"
                        + "<button id='nextButton'>Next</button>"
                        + "<button id='processButton'>Process</button>"
                        + "</div>";
                var modalObj = {
                    title: 'characteristics Data',
                    body: body
                };
                var buttonArray = [
                    {
                    }
                ];
                modalObj['buttons'] = buttonArray;
                $("#modalDailogDiv").remove();
                $("body").append("<div id='modalDailogDiv'></div>");
                createModal("modalDailogDiv", modalObj);
//            $("#dialog").find(".modal-dialog").addClass("modal-xs");
                $("#modalDailogDiv").find(".modal-dialog").addClass("model-cust-rec-reg");

                showCharacteristics(gridId, row, duplicateCheckFlag);


                $("#prevButton").click(function (event) {
                    var visibleTabId = $($(".recordDataConentTabs:visible")[0]).attr("id");

                    if (visibleTabId != null && $("#" + visibleTabId).prev("div").length > 0) {
                        $(".recordDataConentTabs").hide();
                        $("#" + visibleTabId).prev("div").show();
                        var datafunction = $("#" + visibleTabId).prev("div").attr("data-function");
                        if (datafunction != null) {
                            eval(datafunction)
                        }
                    }
                })

                $("#nextButton").click(function (event) {
                    var visibleTabId = $($(".recordDataConentTabs:visible")[0]).attr("id");

                    if (visibleTabId != null && $("#" + visibleTabId).next("div").length > 0) {
                        $(".recordDataConentTabs").hide();
                        $("#" + visibleTabId).next("div").show();
                        var datafunction = $("#" + visibleTabId).next("div").attr("data-function");
                        if (datafunction != null) {
                            eval(datafunction)
                        }
                    }
                })

                $("#processButton").click(function (event) {
                    var charData = [];

                    $("#characteristicsData").find("table").find("tbody").find("tr").each(function (i) {
                        var charRowData = {};
//            charRowData['RECORD_NO'] = $($(this).find('td')[0]).text();
                        charRowData['PROPERTY_NAME'] = $($(this).find('td')[0]).text();
                        charRowData['PROPERTY_VALUE1'] = $($(this).find('td')[1]).find('input').val();
                        charRowData['PROPERTY_UOM'] = $($(this).find('td')[2]).find('input').val();
                        charData.push(charRowData);
                    })

                    var refData = [];

                    $("#referenceData").find("table").find("tbody").find("tr").each(function (i) {
                        var refRowData = {};
                        refRowData['RECORD_NO'] = $($(this).find('td')[0]).text();
                        refRowData['REFERENCE_NO'] = $($(this).find('td')[1]).find('input').val();
                        refRowData['REFERENCE_TYPE'] = $($(this).find('td')[2]).find('input').val();
                        refRowData['VENDOR_NAME'] = $($(this).find('td')[3]).find('input').val();
                        refRowData['R_STXT_FLAG'] = $($(this).find('td')[4]).find('input').val();
                        refRowData['R_LTXT_FLAG'] = $($(this).find('td')[5]).find('input').val();
                        refData.push(refRowData);
                    })

                    $.ajax({
                        type: "POST",
                        url: 'processRecord',
                        dataType: 'json',
                        data: {
                            charactertisticsData: JSON.stringify(charData),
                            referenceData: JSON.stringify(refData),
                            rowData: JSON.stringify(rowData)
                        },
                        traditional: true,
                        cache: false,
                        success: function (response) {

                            stopLoader();
                            var message = response['message'];
                            $("#dialog").find(".modal-title").text("Message");
                            $("#dialog").find(".modal-body").text(message);

                        },
                        error: function (error) {

                        }
                    });
                })
            } else {
                var modalObj = {
                    title: 'Message',
                    body: "Item already registered for this record"
                };
                var buttonArray = [
                    {
                    }
                ];
                modalObj['buttons'] = buttonArray;
                $("#modalDailogDiv").remove();
                $("body").append("<div id='modalDailogDiv'></div>");
                createModal("modalDailogDiv", modalObj);
//            $("#dialog").find(".modal-dialog").addClass("modal-xs");
                $("#modalDailogDiv").find(".modal-dialog").addClass("model-cust-rec-reg");
            }


        },
        error: function (error) {

        }
    });



}

function updateCharRefDataPDR2(gridId, row, duplicateCheckFlag) {
    var rowData = $("#" + gridId).jqxGrid('getrowdata', row);
    var description = rowData['LONG_TEXT']
    var currentDataContent = 1;
    var body = "<div id='charRefData'>"
            + "<div ><textarea id='recordDescription'  rows='3' cols='120' >" + description + "</textarea></div>"
            + "<div id='characteristicsData' class='recordDataConentTabs' data-function=showCharacteristics('" + gridId + "','" + row + "') ></div>"
            + "<div id='referenceData'  class='recordDataConentTabs' data-function=showReferenceData('" + gridId + "','" + row + "') style='display:none;'></div>"
            + "</div>"
            + "<div id='buttonsDiv' style='float:right;margin:10px;'>"
            + "<button id='prevButton'>Prev</button>"
            + "<button id='nextButton'>Next</button>"
            + "<button id='processButton'>Update</button>"
            + "</div>";
    var modalObj = {
        title: 'characteristics Data',
        body: body
    };
    var buttonArray = [
        {
        }
    ];
    modalObj['buttons'] = buttonArray;
    $("#modalDailogDiv").remove();
    $("body").append("<div id='modalDailogDiv'></div>");
    createModal("modalDailogDiv", modalObj);
//            $("#dialog").find(".modal-dialog").addClass("modal-xs");
    $("#modalDailogDiv").find(".modal-dialog").addClass("model-cust-rec-reg");

    showCharacteristics(gridId, row, duplicateCheckFlag);


    $("#prevButton").click(function (event) {
        var visibleTabId = $($(".recordDataConentTabs:visible")[0]).attr("id");

        if (visibleTabId != null && $("#" + visibleTabId).prev("div").length > 0) {
            $(".recordDataConentTabs").hide();
            $("#" + visibleTabId).prev("div").show();
            var datafunction = $("#" + visibleTabId).prev("div").attr("data-function");
            if (datafunction != null) {
                eval(datafunction)
            }
        }
    })

    $("#nextButton").click(function (event) {
        var visibleTabId = $($(".recordDataConentTabs:visible")[0]).attr("id");

        if (visibleTabId != null && $("#" + visibleTabId).next("div").length > 0) {
            $(".recordDataConentTabs").hide();
            $("#" + visibleTabId).next("div").show();
            var datafunction = $("#" + visibleTabId).next("div").attr("data-function");
            if (datafunction != null) {
                eval(datafunction)
            }
        }
    })

    $("#processButton").click(function (event) {
        var charData = [];

        $("#characteristicsData").find("table").find("tbody").find("tr").each(function (i) {
            var charRowData = {};
//            charRowData['RECORD_NO'] = $($(this).find('td')[0]).text();
            charRowData['PROPERTY_NAME'] = $($(this).find('td')[0]).text();
            charRowData['PROPERTY_VALUE1'] = $($(this).find('td')[1]).find('input').val();
            charRowData['PROPERTY_UOM'] = $($(this).find('td')[2]).find('input').val();
            charData.push(charRowData);
        })

        var refData = [];

        $("#referenceData").find("table").find("tbody").find("tr").each(function (i) {
            var refRowData = {};
            refRowData['RECORD_NO'] = $($(this).find('td')[0]).text();
            refRowData['REFERENCE_NO'] = $($(this).find('td')[1]).find('input').val();
            refRowData['REFERENCE_TYPE'] = $($(this).find('td')[2]).find('input').val();
            refRowData['VENDOR_NAME'] = $($(this).find('td')[3]).find('input').val();
            refRowData['R_STXT_FLAG'] = $($(this).find('td')[4]).find('input').val();
            refRowData['R_LTXT_FLAG'] = $($(this).find('td')[5]).find('input').val();
            refData.push(refRowData);
        })

        $.ajax({
            type: "POST",
            url: 'updateCharRefData',
            dataType: 'json',
            data: {
                charactertisticsData: JSON.stringify(charData),
                referenceData: JSON.stringify(refData),
                rowData: JSON.stringify(rowData)
            },
            traditional: true,
            cache: false,
            success: function (response) {

                stopLoader();
                var message = response['message'];
                $("#dialog").find(".modal-title").text("Message");
                $("#dialog").find(".modal-body").text(message);

            },
            error: function (error) {

            }
        });
    })
}

function showCharacteristics(gridId, row, duplicateCheckFlag) {
    $("#dialog").find(".modal-title").text("Charecteristics Data");
    if (!$("#characteristicsDataTable").length > 0) {
        var rowData = $("#" + gridId).jqxGrid("getRowData", row);
        var recordNo = rowData['RECORD_NO'];
        $.ajax({
            type: "POST",
            url: 'charecteristicsData',
            dataType: 'json',
            data: {
                recordNo: recordNo,
                rowData: JSON.stringify(rowData),
                duplicateCheckFlag: duplicateCheckFlag
            },
            traditional: true,
            cache: false,
            success: function (response) {

                stopLoader();
                var charDataList = response['charDataList'];
                var tableStr = "<table id='characteristicsDataTable' style='width: 90%;'>"
                        + "<thead style='background: #f1f1f1;'>"
                        + "<tr>"
                        + "<th>Characteristic</th>"
                        + "<th>Value</th>"
                        + "<th>UOM</th>"
                        + "</tr>"
                        + "</thead>"
                        + "<tbody>";
                $.each(charDataList, function (i) {
                    var rowData = charDataList[i];
                    var propertyName = rowData[0] != null ? rowData[0] : "";
                    var propertyValue = rowData[1] != null ? rowData[1] : "";
                    var propertyUOM = rowData[2] != null ? rowData[2] : "";
                    tableStr += "<tr>"
                            + "<td>" + propertyName + "</td>"
                            + "<td><input style='width: 100%;' type='text' value='" + propertyValue + "' /></td>"
                            + "<td><input style='width: 100%;' type='text' value='" + propertyUOM + "' /></td>"
                            + "</tr>"

                });
                tableStr += "</tbody></table>"

                $("#characteristicsData").html(tableStr);
            },
            error: function (error) {

            }
        });
    }

}

function showReferenceData(gridId, row) {
    $("#dialog").find(".modal-title").text("Reference Data");
    if (!$("#referenceDataTable").length > 0) {
        var rowData = $("#" + gridId).jqxGrid("getRowData", row);
        var recordNo = rowData['RECORD_NO'];
        $.ajax({
            type: "POST",
            url: 'recordReferenceData',
            dataType: 'json',
            data: {
                recordNo: recordNo
            },
            traditional: true,
            cache: false,
            success: function (response) {

                stopLoader();
                var referenceData = response['referenceData'];
                var tableStr = "<table id='referenceDataTable' style='width: 100%;'>"
                        + "<thead style='background: #f1f1f1;'>"
                        + "<tr>"
                        + "<th style='display:none;'>RecordNo</th>"
                        + "<th>Reference No</th>"
                        + "<th>Reference Type</th>"
                        + "<th>Vendor Name</th>"
                        + "<th>Include In SFD</th>"
                        + "<th>Include In POD</th>"
                        + "</tr>"
                        + "</thead>"
                        + "<tbody>";
                $.each(referenceData, function (i) {
                    var rowData = this;
                    tableStr += "<tr>"
                            + "<td style='display:none;' >" + rowData[0] + "</td>"
                            + "<td><input style='width: 100%;' type='text' value='" + rowData[1] + "' /></td>"
                            + "<td><input style='width: 100%;' type='text' value='" + rowData[2] + "' /></td>"
                            + "<td><input style='width: 100%;' type='text' value='" + rowData[3] + "' /></td>"
                            + "<td><input style='width: 100%;' type='text' value='" + rowData[4] + "' /></td>"
                            + "<td><input style='width: 100%;' type='text' value='" + rowData[5] + "' /></td>"
                            + "</tr>"

                });
                tableStr += "</tbody></table>"

                $("#referenceData").html(tableStr);


            },
            error: function (error) {

            }
        });
    }
}

function importFile(gridId) {
    var fileType = $("#import" + gridId).val();
    if (fileType == null || fileType == "") {
        showMesg("Please select File type to import");
        return false;
    }
    $("#importTreeDMFile").unbind('change').on('change', function (event) {
        var files = event.target.files;
        uploadFile(files[0], fileType);
    });

    $("#importTreeDMFile").click();

}

function uploadFile(files, fileType) {

    var data;
    var url;
    var fileData = files['name'];
    var xlsxETLFileData = new FormData();
    xlsxETLFileData.append("importTreeDMFile", files);
    xlsxETLFileData.append("selectedFiletype", fileType);
    xlsxETLFileData.append("fileLocalPath", "");
    data = xlsxETLFileData;
    url = "importTreeDMFile";
    $.ajax({
        url: 'importTreeDMFile',
        type: "POST",
        data: xlsxETLFileData,
        enctype: 'multipart/form-data',
        processData: false,
        contentType: false,
        success: function (response) {
            response = JSON.parse(response);
            var resultFlag = response['flag']
            if (resultFlag != null && resultFlag == "Success") {
                console.log("The result is:::" + response);
                $("#Loader").css("display", "none");
                $("body").css({"pointer-events": "auto"});

                var fileName = response.fileName;
                var result = response.result;
                if (fileType.toUpperCase() == "PDF") {

                    var gridId = ('divGrid-' + fileName.replace(fileType, '').replace('.csv', '')).replace(/\s/g, '');
                    gridId = gridId.replace(/\//g, '');
                    gridId = gridId.replaceAll(/\./g, '')
                    var divHtml = "<div style='height:400px; overflow:scroll;'>"
                            + "<div id='options" + gridId + "' style='display:flex;'>"
                            + "<button onclick=pdfToHtmlPOC('" + fileName + "','" + fileType + "','" + gridId + "')>View as Html</button>"
                            + "</div>"
                            + "<div id='" + gridId + "'></div>"
                            + "</div>"
                    $("#dialog").remove();
                    $("body").append("<div id='dialog'></div>");
                    $("#dialog").html(divHtml);

                    $("#dialog").dialog({
                        title: (labelObject['View Data'] != null ? labelObject['View Data'] : 'View Data'),
                        modal: false,
                        minHeight: 100,
                        maxHeight: 500,
                        minWidth: 1000,
                        maxWidth: 'auto',
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
                            var pdfContent = "<iframe id='viewPDFData' style='width: 100%; height:500px;' src='readPDF?filePath=" + fileName + "' ></iframe>";
                            $("#" + gridId).html(pdfContent);

                            $('#viewPDFData').on("load", function () {
                                stopLoader();
                            });

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
                } else if (fileType.toUpperCase() == "XLSX" || fileType.toUpperCase() == "XLS") {
                    importDataToStgTable(fileName, fileType, 'O_RECORD_DATA_UNIFICATION_STG');
                } else if (fileType.toUpperCase() == "IMAGE") {
                    
                    var gridId = "imagePreviewDiv";
                    var divHtml = "<div style='height:400px; overflow:scroll;'>"
                            + "<button onclick=imageToHtmlPOC('" + fileName + "','" + fileType + "','" + gridId + "')>View as Html</button>"
                            + "<div id='" + gridId + "'></div>"
                            + "<div>";
                    $("#dialog").remove();
                    $("body").append("<div id='dialog'></div>");
                    $("#dialog").html(divHtml);

                    $("#dialog").dialog({
                        title: (labelObject['Preview'] != null ? labelObject['Preview'] : 'Preview'),
                        modal: false,
                        minHeight: 100,
                        maxHeight: 500,
                        minWidth: 1000,
                        maxWidth: 'auto',
                        buttons: [{
                                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                                click: function () {
                                    $(this).html("");
                                    $(this).dialog("close");
                                    $(this).dialog("destroy");
                                }
                            }],
                        open: function () {
                            $.ajax({
                                type: "post",
                                traditional: true,
                                dataType: 'html',
                                url: 'getImageString',
                                cache: false,
                                data: {
                                    fileName: fileName,
                                    fileType: response.fileType
                                },
                                success: function (result) {
                                    stopLoader();
                                    $("#imagePreviewDiv").html("<img src='" + result + "' />");
                                },
                                error: function (e) {
                                    console.log(e);
                                    sessionTimeout(e);
                                    stopLoader();
                                }
                            });

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

            } else if (resultFlag != null && resultFlag == "Fail") {
                $("#dialog").remove();
                $("body").append("<div id='dialog'></div>");
                $("#dialog").html(response.result);
                $("#dialog").dialog({
                    title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
                    modal: false,
                    minHeight: 100,
                    maxHeight: 500,
                    minWidth: 1000,
                    maxWidth: 'auto',
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



        }, error: function (e) {
            console.log("The Error Message is:::" + e.message);
            sessionTimeout(e);
        }
    });
}

function importDataToStgTable(fileName, fileType, tableName) {
    showLoader();
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'importDataToStgTable',
        cache: false,
        data: {
            tableName: tableName,
            fileName: fileName
        },
        success: function (result) {
            stopLoader();
            var message = result['message'];
            $("#dialog").remove();
            $("body").append("<div id='dialog'></div>");
            $("#dialog").html(message);

            $("#dialog").dialog({
                title: (labelObject['Message'] != null ? labelObject['View Data'] : 'Message'),
                modal: false,
                height: 135,
                minWidth: 300,
                maxWidth: 'auto',
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
        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
}

function pdfToHtmlPOC(fileName, fileType, parentDivId) {
    showLoader();
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        url: 'convertPDFToHTML',
        cache: false,
        data: {
            filePath: fileName
        },
        success: function (result) {
            stopLoader();
            var pdfContent = "<iframe id='pdfToHTMLData' style='width: 100%; height:650px;' srcdoc='' onload='setSrcdoc()'></iframe>";
            $("#" + parentDivId).html(pdfContent);
            $('#pdfToHTMLData').attr('srcdoc', result);
        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
}

function imageToHtmlPOC(fileName, fileType, parentDivId) {
    showLoader();
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        url: 'convertImageToHTML',
        cache: false,
        data: {
            filePath: fileName
        },
        success: function (result) {
            stopLoader();
            var imageContent = "<iframe id='imageToHTMLData' style='width: 100%; height:650px;' srcdoc='' ></iframe>";
            $("#" + parentDivId).html(imageContent);
            $('#imageToHTMLData').attr('srcdoc', result);
        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
}

function updateStagingTable(gridId, rowIndex) {
    showLoader();
    var rowData = $("#" + gridId).jqxGrid('getrowdata', rowIndex);
//    var commit =  $("#" + gridId).jqxGrid('endupdate');
    $("#" + gridId).jqxGrid('endcelledit', rowIndex, currentCellDataField, false);
//    var commit =  $("#" + gridId).jqxGrid('endrowedit', rowIndex, true);
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'insertStagingTable',
        cache: false,
        async: false,
        data: {
            rowData: JSON.stringify(rowData)
        },
        success: function (result) {
            stopLoader();
            var message = result['message'];
            $("#dialog").remove();
            $("body").append("<div id='dialog'></div>");
            $("#dialog").html(message);
            $("#dialog").dialog({
                title: (labelObject['Message'] != null ? labelObject['View Data'] : 'Message'),
                modal: false,
                height: 135,
                minWidth: 300,
                maxWidth: 'auto',
                fluid: true,
                buttons: [{
                        text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                        click: function () {
                            gridoperations(gridId, 'refresh')
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
        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
}

function SearchInRepository1(gridId, operation) {
    var selectedrowindex = $("#" + gridId).jqxGrid('selectedrowindex');
    if (selectedrowindex == -1) {
        selectedrowindex = globalSelectedRow;
    }
    var rowdata = $("#" + gridId).jqxGrid('getrowdata', selectedrowindex);
    var referenceData = rowdata['REFERENCE_DATA']

    $("#settingheaderImage").trigger("click");
    $("#dxpAdavanceSearchOptions").val("PRA");
    var val = $("#dxpAdavanceSearchOptions").val();
//    $("#SearchResult").val(referenceData);
    var homepageDIvStr = '<div class="mainDxpSplitter" id="mainDxpSplitter" style="display:none"><div class="firstDxpSplitter" id="firstDxpSplitter" style="overflow-y: auto;"><div class="firstDxpSplitterData" id="firstDxpSplitterData"><span></span></div><div class="mainSecondDxpSplitter" id="mainSecondDxpSplitter"><div class="secondDxpSplitter" id="secondDxpSplitter" style="overflow-y: auto;"><div class="secondDxpSplitterData" id="secondDxpSplitterData" style="overflow-y: auto;"></div><div><div class="thirdDxpSplitter" id="thirdDxpSplitter" style="overflow-y: auto;"><div id="searchGrid"></div><div class="fourthDxpSplitter" id="fourthDxpSplitter" ><span></span></div></div></div></div></div></div></div><div class="searchDxpSplitter" id="searchDxpSplitter" style="display:none;"><div class="searchDefaultSplitter" id="searchDefaultSplitter"><div id="searchsettingsSplitter" class="searchsettingsSplitter"><div style="overflow: hidden;" id="jqxTreeDiv" class="visionjqxTreeDiv"><div style="border: none;" id="jqxTree"></div></div><div class="dxpDictionaryFormClassData"></div><div id="searchTypeSplitter" class="searchTypeSplitterClass"></div><div class="personaliseoption visionSearchPersonaliseoption"><div onclick="slideSettings();" class="layoutoptions ui-accordion"><h3 class="ui-accordion-header1"><span class="ui-accordion-header-icon ui-icon1 ui-icon-triangle-1-e" id="personalizeid"></span><img alt="" class="navIcon gear" src="images/Settings_Icon.svg" title="Personalise"/></h3></div><div id="settings_panel" class="search_settings_panel" ></div></div></div><div id="accordioncover" class="acrdn visionMaterialGeneric" ></div></div><div class="searchresultsSplitter" id="searchresultsSplitter"><div id="searchResults"></div></div></div><div class="dxpClassficationAppendClass" id="dxpClassficationAppendClass"></div><section class="visualizationDashboardView" style="display:none"><div class="container-fluid"><div class="row"><div class="col-12"><div id="visionCardView" class="visionCardViewClass"></div></div></div><div class="row" id ="visualizechartId"></div></section></div>'
    $(".page-body-content").html(homepageDIvStr);
    getParamForm('FDXP_GENERIC_SEARCH');
    $("#PRAREFERENCE_NO").find(".value_td").find("input").val(referenceData);
    $("#PRAsearchbtn").trigger("click");

//    smartTextSearch('SearchResult', 'SelectedValue', 'N')
}

function SearchInRepository(gridId, operation) {
    var selectedrowindex = $("#" + gridId).jqxGrid('selectedrowindex');
    if (selectedrowindex == -1) {
        selectedrowindex = globalSelectedRow;
    }
//    var selectedrowindex = $("#" + gridId).jqxGrid('selectedrowindex');
    var rowdata = $("#" + gridId).jqxGrid('getrowdata', selectedrowindex);
    var referenceData = rowdata['REFERENCE_DATA']
    var classData = rowdata['CLASS']

    $.ajax({
        type: "POST",
        url: 'getParamSearchForm',
        data: {
            'searchId': 'FDXP_GENERIC_SEARCH',
            'reqType': 'PRA',
//            searchName: searchName
        },
        traditional: true,
        cache: false,
        async: false,
        success: function (response) {

            if (response != null && response != '') {
                var responseObj = JSON.parse(response);
                $("#settingheaderImage").trigger("click");
//                getPersonalizationDataOpt('PRA');
//                getClassificationSuggetions();
//                    dxpTreeSearch();

                $("#jqxSplitter").remove();
                $("#searchResults").remove();
                $("#searchDefaultSplitter").remove();
                $("#searchresultsSplitter").remove();

                var body = "<div id='jqxSplitter'>"
                        + "<div class ='searchDefaultSplitter' id='searchDefaultSplitter' ></div>"
                        + "<div class='searchresultsSplitter' id='searchresultsSplitter'><div id='searchResults'></div></div>"
                        + "</div>"

                var parametricSearchForm = responseObj['formString']
                var modalObj = {
                    title: 'Search in Repository',
                    body: body
                };
                var buttonArray = [
                    {
                    }
                ];
                modalObj['buttons'] = buttonArray;
                $("#modalDailogDiv").remove();
                $("body").append("<div id='modalDailogDiv'></div>");
                createModal("modalDailogDiv", modalObj);
                $("#modalDailogDiv").find(".modal-dialog").addClass("modal-xl");

//                $("#jqxSplitter").jqxSplitter({theme: 'summer'});
//                $("#jqxSplitter").jqxSplitter({
//                    width: 'auto',
//                    orientation: 'vertical',
//                    splitBarSize: 5,
//                    panels: [{size: 100}]
//                });
                setTimeout(function () {
                    $("#jqxSplitter").jqxSplitter({width: '99.7%',
                        splitBarSize: '5px',
                        panels: [{size: "40%"}, {min: '60%', size: "60%"}]})
                    $("#searchDefaultSplitter").html(parametricSearchForm)
                    $("#DxpParamSplitterDotsClass").remove();
                    $("#PRAREFERENCE_NO").find(".value_td").find("input").val(referenceData);
                    $("#PRATERM").find(".value_td").find("input").val(classData);
                }, 500);
            }
        },
        error: function (e) {
            console.log(e);
            // ajaxStop();
            sessionTimeout(e);
        }

    });
}


function sendOrderMail(organisationName, gridId, selectedrowindex, recordNo) {
    var rowdata = $("#" + gridId).jqxGrid('getrowdata', selectedrowindex);

    var body = "<div id='orderDetailsDiv' style='border: 2px solid #cacaca;border-radius: 10px;padding: 15px;'><table>"
            + "<tr><td>Class : </td><td><input type='text' value='" + rowdata['CLASS'] + "'/></td>"
            + "<td>Description : </td><td><textarea  rows='3' cols='30'>" + rowdata['LONG_TEXT'] + "</textarea></td>"
            + "<td>UNSPSC : </td><td><input type='text' value='" + rowdata['UNSPSC'] + "'/></td>"
            + "<td>Quantity : </td><td><input id='orderQuantity' type='text' value='' /></td></tr>"
            + "</table></div>"
    var modalObj = {
        title: 'Send Request',
        body: body,
        recordNo: recordNo
    };
    var buttonArray = [
        {
            text: 'Send Request',
            click: function () {
                showLoader();
                var orderQuantity = $("#orderQuantity").val();
                $("#orderQuantity").attr("value", orderQuantity);

                $.ajax({
                    type: "POST",
                    url: 'sendOrderRquest',
                    data: {
                        mailBody: $("#orderDetailsDiv").html(),
                        recordNo: recordNo,
                        organisationName: organisationName,
                        rowdata: JSON.stringify(rowdata),
                        orderQuantity: orderQuantity
                    },
                    traditional: true,
                    cache: false,
                    async: false,
                    success: function (response) {

                        stopLoader();
                        if (response != null && response != '') {

                            var message = response['message'];
                            var modalObj = {
                                title: 'Message',
                                body: message
                            };
                            var buttonArray = [
                                {
                                }
                            ];
                            modalObj['buttons'] = buttonArray;
                            createModal("dialog1", modalObj);
                            $("#dialog1").find(".modal-dialog").addClass("modal-xs");

                        }
                    },
                    error: function (e) {
                        console.log(e);
                        // ajaxStop();
                        sessionTimeout(e);
                    }

                });
            },
            isCloseButton: true
        }
    ];
    modalObj['buttons'] = buttonArray;
    createModal("dialog1", modalObj);
    $("#dialog1").find(".modal-dialog").addClass("modal-xl");
    $("#orderQuantity").focus();
}

function downloadTemplate(gridId, templateId) {
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    var form = '<form id="importData"  method="post">'
            + '<input type="hidden" name="importDataHidden" id="importDataHidden" value=""/>'
            + '</form>';
    $("body").append(form);


    var columnsList = 'RECORD_NO,UOM,LONG_TEXT,REFERENCE_DATA,VENDOR';
    $('#importDataHidden').val(columnsList);
    $("#importData").attr("action", "downloadRegistryTemplate");
    $("#importData").submit();

}

function showVendorNotifications() {
    $.ajax({
        type: "POST",
        url: 'vendorNotifications',
        data: {
        },
        traditional: true,
        cache: false,
        async: false,
        success: function (response) {
            stopLoader();
            if (response != null && response != '') {

                var notificationsList = response['notifications'];
                var body = "";
                if (notificationsList != null && notificationsList.length > 0) {
                    $("#vendorNotificationCount").css("display", "none");
                    body += "<table><thead style='background: #f1f1f1;' ><tr>"
                            + "<th>SNO</th>"
                            + "<th>CLIENT</th>"
                            + "<th>CLASS</th>"
                            + "<th>DESCRIPTION</th>"
                            + "<th>UNSPSC</th>"
                            + "<th>QUANTITY</th>"
                            + "</tr></thead><tbody>";

                    $.each(notificationsList, function (i) {
                        var notification = notificationsList[i];
                        body += "<tr>"
                                + "<td>" + (i + 1) + "</td>"
                                + "<td><input type='text' value=" + (notification[0] != null ? notification[0] : "") + " /></td>"
                                + "<td><input type='text' value=" + (notification[1] != null ? notification[1] : "") + " /></td>"
                                + "<td><textarea  rows='2' cols='30' >" + (notification[2] != null ? notification[2] : "") + "</textarea></td>"
                                + "<td><input type='text' value=" + (notification[3] != null ? notification[3] : "") + " /></td>"
                                + "<td><input type='text' value=" + (notification[4] != null ? notification[4] : "") + " /></td>"
                                + "</tr>"
                    })
                    body += "</tbody></table>";
                }
                var modalObj = {
                    title: 'Notifications',
                    body: body
                };
                var buttonArray = [
                    {
                        text: 'Ok',
                        click: function () {
                        },
                        isCloseButton: true
                    }
                ];
                modalObj['buttons'] = buttonArray;
                createModal("dialog1", modalObj);
                $("#dialog1").find(".modal-dialog").addClass("model-cust-rec-reg modal-xl opacity-animate3");

            }
        },
        error: function (e) {
            console.log(e);
            // ajaxStop();
            sessionTimeout(e);
        }
    });
}


function getAnalysisForm(gridId, formId) {
    showLoader();
    $.ajax({
        url: "getEmptyForm",
        type: "post",
        data: {
            gridId: formId
        },
        success: function (response) {
            stopLoader();
            if (!isNullOrUndefined(response) && !jQuery.isEmptyObject(response)) {
                var r = JSON.parse(response);
                $("#analysisDialog").remove();
                $("body").append("<div id='analysisDialog'></div>");
                $('#analysisDialog').html(r['emptyFormStr']);
                $("#analysisDialog").dialog({
                    title: (labelObject['Input Parameters'] != null ? labelObject['Input Parameters'] : 'Input Parameters'),
                    modal: true,
                    height: "auto",
                    width: 800,
                    height : 400,
                    top : 100,
                    fluid: true,
                    buttons: [{
                            text: (labelObject['Run Analysis'] != null ? labelObject['Run Analysis'] : 'Run Analysis'),
                            click: function () {
                                setParamsData(gridId, formId);
                                setAssetCriticalRanking(gridId);
                                $(this).html("");
                                $(this).dialog("destroy");

                            }
                        }]
                });
                $(".ui-dialog").addClass("AnalysisPopupForm");
            }
        },
        error: function (error) {

        }
    });
}
function setParamsData(gridId, formId) {
    var paramsObj = {};
    var analysisType = $("#analysisType").val();
    var BatchId = $("#BATCH_ID").val();
    var $form = $("#" + formId + "_InputTableForm").find("input");
//      var $form = $("#" + formId + "_InputTableForm").find(".item :input");
//    paramsObj['analysisType'] = analysisType;
//    paramsObj['analysisType'] = "ABC";
    paramsObj['tableName'] = $("#tableName").val();
    var columnArr = [];
    $form.each(function (index, element) {
        var textid = $(this).attr("id");
        var type = $(this).attr("type");
        var textval = $(this).val();
        if (type !== 'hidden') {
            if (textval !== null && textval !== '') {
                textval = textval.toUpperCase();
            }
        }
        if (textid === 'Plant' && textval.indexOf(",") > -1) {
            textval = textval.split(',');
            textval = JSON.stringify(textval);
            textval = textval.replace(/"/g, '').replace(/\'/g, '');
        }
        paramsObj[textid] = textval;
//            paramsObj['BATCH_ID'] = BatchId;
//        if (textval == 'VED' || textval == 'SDE') {
        paramsObj['BATCH_ID'] = BatchId;
//        }

        columnArr.push(textid);
    });
    if (validateAnalysis(analysisType)) {
        runAnalysis(gridId, paramsObj, columnArr);
    }
}
function validateAnalysis(a) {
    let source = '';
    let destination = '';
    switch (a) {
        case "ABC" :
            source = $("#A").val();
            destination = $("#C").val();
            break;
        case "HML" :
            source = $("#H").val();
            destination = $("#L").val();
            break;
        case "FSN" :
            break;
        case "XYZ" :
            break;
        case "SDE" :
            break;
    }
    if (+source > +destination) {
        var errorMessage = `${destination} Value must be greater than ${source}`;
        analysisFormPopup(errorMessage);
        return false;
    }
    return true;
}
function setAssetCriticalRanking(gridId) {
    showLoader();

    var gridRows = $("#" + gridId).jqxGrid('getrows');

    // Iterate through each row in the grid
    for (var i = 0; i < gridRows.length; i++) {
        var rowData = gridRows[i];
        // Calculate total for each row (Value1 + Value2)
        var total = parseInt(rowData.FAILURE_CONSEQUENCES_SCORE) * parseInt(rowData.IMPORTANCE_SCORE) * parseInt(rowData.RELIABILITY_SCORE) * parseInt(rowData.UTILIZATION_SCORE);
        // Update the 'Total' column in the row data
        gridRows[i].ASSET_CRITICALITY_INDEX = total;

        if (total == 0 || total <= 2) {
            gridRows[i].ASSET_CRITICALITY_RANKING = "Critical Equipment";
            gridRows[i].EQUIPMENT_RANKING = "3";

        } else if (total == 3 || total < 24) {
            gridRows[i].ASSET_CRITICALITY_RANKING = "Important Equipment";
            gridRows[i].EQUIPMENT_RANKING = "2";
        } else if (total > 25) {
            gridRows[i].ASSET_CRITICALITY_RANKING = "Ordinary Equipment";
            gridRows[i].EQUIPMENT_RANKING = "1";
        }

    }

    // Update the grid with the modified data
//    $("#"+gridId).jqxGrid('updatebounddata');
   // stopLoader();

}
function runAnalysis(gridId, p) {
    showLoader();
    $.ajax({
        url: "runAnalysis",
        type: "post",
        data: {
            requestBody: JSON.stringify(p),
            gridId: gridId
        },
        success: function (response) {
            stopLoader();
            var message = '';

            var resultobj = JSON.parse(response);
            var reports = resultobj['report']
            message = reports;
            $("#analysisResultObj").remove();
            $('body').append(`<input type='hidden' id='analysisResultObj' value='${response}'>`);

            if (reports != null && reports != '') {
                var modalObj = {
                    title: labelObject['Message'] != null ? labelObject['Message'] : 'Data Analysis',
                    body: '<iframe id="dataProfilingIframe" style="width:100%;height:100%;" srcdoc=""></iframe>'
                };
                var buttonArray = [
                    {
                        text: labelObject['Close'] != null ? labelObject['Close'] : 'Close',
                        click: function () {
                            gridoperations(gridId, "refresh");
                        },
                        isCloseButton: true
                    }
                ];
                modalObj['buttons'] = buttonArray;
                createModal("intiateRequestClass", modalObj);
                $("#intiateRequestClass").addClass("vendorProfilingPopupHTML");
                $("#intiateRequestClass .modal-dialog").addClass("modal-xl");
                $("#dataProfilingIframe").attr("srcdoc", message);


            } else {
                if (!isNullOrUndefined(response)) {
                    message = getDialogDivAfterAnalasis();
                }

                var modalObj = {
                    title: labelObject['Message'] != null ? labelObject['Message'] : 'Data Analysis',
                    body: message
                };
                var buttonArray = [
                    {
                        text: labelObject['Close'] != null ? labelObject['Close'] : 'Close',
                        click: function () {
                            gridoperations(gridId, "refresh");
                        },
                        isCloseButton: true
                    }
                ];
               modalObj['buttons'] = buttonArray;
                               createModal("modalDailogDiv", modalObj);
                                $(".modal-dialog").addClass("modal-xs");
                               getChartAfterAnalyasis();
            }
        }
    });

}

function getChartAfterAnalyasis(){
    var chartDataStr = $("#analysisResultObj").val();
    if(chartDataStr != null && chartDataStr != undefined && chartDataStr != ''){
    var chartDataObj = JSON.parse(chartDataStr);
    var ChartTitle = Object.keys(chartDataObj[0]);
    var AnalysisData = chartDataObj[0]['CHART'];
    var labels =[];
    var countArr=[], percentArr=[];
    var totalrecords = AnalysisData['Total Count'];
    for (var key in AnalysisData) {
    if (key.includes("Count") && key!= "Total Count") {
      countArr.push(AnalysisData[key]);
    }
    if (key.includes("Percentage")) {
     percentArr.push(AnalysisData[key]);
        labels.push(key.replace(new RegExp('Percentage', 'i'), '').replaceAll("_",""));
    }
}
     var layout = {
         title : AnalysisData['tittle'].replace("_"," "),
              height: 400,
              width: 400,

            };
        var config = {
    displayModeBar: true,
    displaylogo: false
  };
    var colorsArr = ["#EAC117","#347C2C","#806517","#E66C2C","#5C3317","#C11B17"]
    var chartType=$('#analysisChartTypeDropdown').val();
    switch (chartType){
        case 'pie':
            var data = [{
              values: percentArr,
              labels: labels,
              type: 'pie',
               textposition:'inside',
              hovertemplate: '%{label}: %{percent}<extra></extra>',
                textinfo: 'percent+label' ,
                marker:{
                    colors:colorsArr,
                }
            }];
            break;
        case 'bar':
            var data = [{
                  x: labels,
                  y: percentArr,
                  type: 'bar',
              hovertemplate: '%{label}: %{y}%<extra></extra>',
                textinfo: 'percent+label',
                 marker:{
                    color:colorsArr,
                }
                }];
            break;
        case "lines":
           var data = [{
              x: labels,
              y: percentArr,
              mode: 'lines',
              type: 'scatter',
              hovertemplate: '%{x}: %{y}%<extra></extra>',
                textinfo: 'percent+label',
               line:{
                    color:colorsArr[0],
                }
            }];
            break;
        case "donut":
           var data = [{
              values: percentArr,
              labels: labels,
              type: 'pie',
              hole: 0.5,
              hovertemplate: '%{label}: %{percent}<extra></extra>',
                textinfo: 'percent+label',
                marker:{
                    colors:colorsArr,
                }
            }];

            break;
        case "waterfall":
            var data = [{
                  x: labels,
                  y: percentArr,
                  type: 'waterfall',
                orientation: 'v',
                name:labels.join(""),
                textposition: "outside",
             hovertemplate: '%{x}: %{y}%<extra></extra>',
                textinfo: 'percent+label',
                increasing: {
                        'marker': {
                            'color': colorsArr[0],
                        }
                    }
                }];
           break;
        case "boxplot":
            var data = [{
                //x: labels,
                y: percentArr,
                boxpoints: 'all',
                jitter: 0.3,
                pointpos: -1.8,
                type: 'box',
                name:labels.join(""),
               hovertemplate: '%{y}%<extra></extra>',
              textinfo: 'percent+label',
                marker:{
                    color:colorsArr[0],
                }
            }];
            break;
        case "scatter":
            var data = [{
              x: labels,
              y: percentArr,
              mode: 'markers',
              type: 'scatter',
             hovertemplate: '%{x}: %{y}%<extra></extra>',
            textinfo: 'percent+label',
                marker:{
                    color:colorsArr[0],
                }
            }];
            break;
        case "AreaChart":
            var data = [{
              x: labels,
              y: percentArr,
              fill: 'tozeroy',
              type: 'scatter',
              hovertemplate: '%{x}: %{y}<extra></extra>',
              textinfo: 'percent+label',
            fillcolor: colorsArr[0],
            }];
        default :
           var data = [{
              x: labels,
              y: percentArr,
              fill: 'tozeroy',
              type: 'scatter',
              hovertemplate: '%{x}: %{y}<extra></extra>',
              textinfo: 'percent+label',
               fillcolor: colorsArr[0],
            }];
            break;

    }
    //data[0].marker={colors:["#EAC117","#347C2C","#806517","#E66C2C","#5C3317","#C11B17"]};
        Plotly.newPlot('visionVisualChartForAnalaysis', data, layout, config);
        convertPlotlyChartToBase64('visionVisualChartForAnalaysis');

    }
}
function getDialogDivAfterAnalasis(){
    var dialogBoxDiv = `<div style='display:flex'><label for="chartTypeDropdown"></label>
      <select id="analysisChartTypeDropdown" onchange="getChartAfterAnalyasis()">
        <option value="pie">Pie Chart</option>
        <option value="bar">Bar Chart</option>
        <option value="lines">Line Chart</option>
        <option value="donut">Donut Chart</option>
        <option value="waterfall">Waterfall Chart</option>
        <option value="boxplot">Boxplot</option>
        <option value="scatter">Scatter Plot</option>
        <option value="AreaChart">Area Chart</option>
  </select>

<div id='pdfReportForAnalysis' style='margin-left:50%'> <img src='images/pdficon.png'  class='imagepdfReportForAnalysis' onclick=getPdfReportAfterAnalysis('visionVisualChartForAnalaysis')  title='Pdf Report' style='width:20px;height:20px'/></div>
</div>
<div id='visionVisualChartForAnalaysis'></div>`;



    return dialogBoxDiv;

}
function getPdfReportAfterAnalysis(chartId,chartFlag) {
    $('#pdfbodyForm').remove();
    $('body').append(`<form action='downloadChartImagePDF' id='pdfbodyForm'  method='POST' target='_blank' style="display:none">
            <input type='hidden' id='imageData' name='imageData' value="">

        </form>`);

    //$(".chartDownloadDialog").attr("chartImageData");
   // $("#source-listbox").show();
    var imageData = $("#" + chartId).attr("chartImageData");
    $("#imageData").val(imageData);
    //$("#dataIMG").attr("chartId", chartId);
    //var chartFlag = $("#" + chartId).attr("jqwidgetChartFlag");
    var chartDataStr = $("#analysisResultObj").val();
     var chartDataObj = JSON.parse(chartDataStr);
    var ChartTitle = Object.keys(chartDataObj)[0];
   var analysisType1 = chartDataObj[0]['CHART']['tittle'];
    var AnalysisData = chartDataObj[0]['PDF'];
    var imageData = $("#" + chartId).attr("chartImageData");
    $("#imageData").val(imageData);
    for (var key in AnalysisData) {
    if (AnalysisData.hasOwnProperty(key)) {
        var inputType = "<input type='hidden' name='" + key + "' value='" + AnalysisData[key] + "'/>";
        $("#pdfbodyForm").append(inputType);
    }
}
   // var selectedItemText = $("#selectedChart option:selected").text();
    var opt = 'PDF';//$("#selectedChart option:selected").val();
    var inputType = "";
    var analysisType = analysisType1.split('_')[0];//$("#analysisType").val();
    if(analysisType === 'ASSET'){
         analysisType ="ASSET CRITICALITY ASSESSMENT";
    }
    $("#analysisType").val(analysisType);

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

//     if (opt == "PNG") {
//         if (chartFlag != null && chartFlag != '' && chartFlag != undefined && chartFlag == 'Y') {
//             var imageContent = $("#" + chartId).jqxChart('saveAsJPEG', 'myChart.jpeg', "test", true);
//             imageContent = "data:image/png;base64," + imageContent;
//             var showImageChart = $("<a>").attr("href", imageContent).attr("download", "img.png").appendTo("body");
//             showImageChart[0].click();
//             showImageChart.remove();
//             $(this).dialog('close');
//         } else {
//             downloadChartAsImage(chartId);
// //                        $("#" + chartId).jqxChart('saveAsPNG', 'myChart.png', 'https://www.jqwidgets.com/export_server/export.php');
//             $(this).dialog('close');
//         }
//     }
    if (opt == "PDF") {
        if (chartFlag != null && chartFlag != '' && chartFlag != undefined && chartFlag == 'Y') {
            var imageContent;
            imageContent = "data:image/png;base64," + $("#" + chartId).jqxChart('saveAsJPEG', 'myChart.jpeg', "test", true);
            $("#" + chartId).attr("jqxChartImageData", imageContent);
            var jqxImageData1 = $("#" + chartId).attr("jqxChartImageData");
            $("#jqxImageData1").val(jqxImageData1);
            //showAsJQXPdfform();
            showAsPdfform();


        } else {
            showAsPdfform();

        }
    }

}
function convertPlotlyChartToBase64(chartId) {
    Plotly.toImage(chartId, { format: 'png', height: 500, width: 700 })
      .then(function (uri) {

        $("#" + chartId).attr("chartImageData", uri);
      });
  }
  function showAsJQXPdfform() {
      $("#jqxPdfbodyForm").submit();
  }
  function showAsPdfform() {
      $("#pdfbodyForm").submit();

  }

