$(document).ready(function () {
    $(".slider").hide();
    $(".goto").click(function () {
        $("#table-of-content").hide();
        var switchTo = $(this).attr("switch-to");
        //alert(switchTo);

        if (switchTo) {
            $(".slider").not("." + switchTo).hide();
            $("." + switchTo).show();
            lastSwitch = switchTo;
        } else {
            $(".slider").hide();
        }
        // alert(switchTo);
        var slickOpts = {
            autoplay: false,
            infinite: false,
            slidesToShow: 1,
            slidesToScroll: 1,
            // centerMode: true,
            easing: 'swing', // see http://api.jquery.com/animate/
            speed: 1000,
            dots: true,
            customPaging: function (slick, index) {
                return '<a>' + (index + 1) + '</a>';
            }
        };
        $('[inner-slide]').slick(slickOpts);
        // $('.slider').slick(slickOpts);
    });
});
// Init slick carousel

var tabsOldData = {};
function backFunction() {
    $(".slider").hide();
    $("." + lastSwitch).hide();
    $('[inner-slide]').slick('unslick');
    $("#table-of-content").show();
}
var service_last_Switch;
var fieldsArray = new Array();
$(document).ready(function () {
    $(".slider").hide();
    $(".goto-service-contract").click(function () {
        $("#table-of-content").hide();
        var switchTo = $(this).attr("switch-to");
        //alert(switchTo);

        if (switchTo) {
            $(".slider").not("." + switchTo).hide();
            $("." + switchTo).show();
            service_last_Switch = switchTo;
        } else {
            $(".slider").hide();
        }
        // alert(switchTo);
        var slickOpts = {
            autoplay: false,
            infinite: false,
            slidesToShow: 1,
            slidesToScroll: 1,
            // centerMode: true,
            easing: 'swing', // see http://api.jquery.com/animate/
            speed: 1000,
            dots: true,
            customPaging: function (slick, index) {
                return '<a>' + (index + 1) + '</a>';
            }
        };
        $('[service-contract]').slick(slickOpts);
        // $('.slider').slick(slickOpts);
    });
});

function seriveBack() {
    $(".slider").hide();
    $("." + service_last_Switch).hide();
    $('[service-contract]').slick('unslick');
    $("#table-of-content").show();
}
var bpo_last_Switch;
$(document).ready(function () {
    $(".slider").hide();
    $(".goto-bpo").click(function () {
        $("#table-of-content").hide();
        var switchTo = $(this).attr("switch-to");
        //alert(switchTo);

        if (switchTo) {
            $(".slider").not("." + switchTo).hide();
            $("." + switchTo).show();
            bpo_last_Switch = switchTo;
        } else {
            $(".slider").hide();
        }
        // alert(switchTo);
        var slickOpts = {
            autoplay: false,
            infinite: false,
            slidesToShow: 1,
            slidesToScroll: 1,
            // centerMode: true,
            easing: 'swing', // see http://api.jquery.com/animate/
            speed: 1000,
            dots: true,
            customPaging: function (slick, index) {
                return '<a>' + (index + 1) + '</a>';
            }
        };
        $('[bpo-inner-slide]').slick(slickOpts);
        // $('.slider').slick(slickOpts);
    });
});

function bpoBack() {
    $(".slider").hide();
    $("." + bpo_last_Switch).hide();
    $('[bpo-inner-slide]').slick('unslick');
    $("#table-of-content").show();
}
var bppo_last_Switch;
$(document).ready(function () {
    $(".slider").hide();
    $(".goto-bppo").click(function () {
        $("#table-of-content").hide();
        var switchTo = $(this).attr("switch-to");
        //alert(switchTo);

        if (switchTo) {
            $(".slider").not("." + switchTo).hide();
            $("." + switchTo).show();
            bppo_last_Switch = switchTo;
        } else {
            $(".slider").hide();
        }
        // alert(switchTo);
        var slickOpts = {
            autoplay: false,
            infinite: false,
            slidesToShow: 1,
            slidesToScroll: 1,
            // centerMode: true,
            easing: 'swing', // see http://api.jquery.com/animate/
            speed: 1000,
            dots: true,
            customPaging: function (slick, index) {
                return '<a>' + (index + 1) + '</a>';
            }
        };
        $('[bppo-inner-slide]').slick(slickOpts);
        // $('.slider').slick(slickOpts);
    });
});


function bppoBack() {
    $(".slider").hide();
    $("." + bppo_last_Switch).hide();
    $('[bppo-inner-slide]').slick('unslick');
    $("#table-of-content").show();
}
var bpdm_last_Switch;
$(document).ready(function () {
    $(".slider").hide();
    $(".goto-bpdm").click(function () {
        $("#table-of-content").hide();
        var switchTo = $(this).attr("switch-to");
        //alert(switchTo);

        if (switchTo) {
            $(".slider").not("." + switchTo).hide();
            $("." + switchTo).show();
            bpo_last_Switch = switchTo;
        } else {
            $(".slider").hide();
        }
        // alert(switchTo);
        var slickOpts = {
            autoplay: false,
            infinite: false,
            slidesToShow: 1,
            slidesToScroll: 1,
            // centerMode: true,
            easing: 'swing', // see http://api.jquery.com/animate/
            speed: 1000,
            dots: true,
            customPaging: function (slick, index) {
                return '<a>' + (index + 1) + '</a>';
            }
        };
        $('[bpdm-inner-slide]').slick(slickOpts);
        // $('.slider').slick(slickOpts);
    });
});


function bpdmBack() {
    $(".slider").hide();
    $("." + bpdm_last_Switch).hide();
    $('[bpdm-inner-slide]').slick('unslick');
    $("#table-of-content").show();
}
var bpr_lastSwitch;
$(document).ready(function () {
    $(".slider").hide();
    $(".goto-bpr").click(function () {
        $("#table-of-content").hide();
        var switchTo = $(this).attr("switch-to");
        //alert(switchTo);

        if (switchTo) {
            $(".slider").not("." + switchTo).hide();
            $("." + switchTo).show();
            bpr_lastSwitch = switchTo;
        } else {
            $(".slider").hide();
        }
        // alert(switchTo);
        var slickOpts = {
            autoplay: false,
            infinite: false,
            slidesToShow: 1,
            slidesToScroll: 1,
            // centerMode: true,
            easing: 'swing', // see http://api.jquery.com/animate/
            speed: 1000,
            dots: true,
            customPaging: function (slick, index) {
                return '<a>' + (index + 1) + '</a>';
            }
        };
        $('[bpr-inner-slide]').slick(slickOpts);
        // $('.slider').slick(slickOpts);
    });
});

function bprBack() {
    $(".slider").hide();
    $("." + bpr_lastSwitch).hide();
    $('[bpr-inner-slide]').slick('unslick');
    $("#table-of-content").show();
}
$(document).ready(function () {
    var ssUsername = $("#ssUsername").val();
    var apiSubscriptionStatus = $("#apiSubscriptionStatus").val();
    var apiSubscriptionMessage = $("#apiSubscriptionMessage").val();
    if (ssUsername != null && ssUsername != '' && ssUsername != 'null') {
        if (apiSubscriptionStatus != null && apiSubscriptionStatus != '' && apiSubscriptionStatus != 'null' && apiSubscriptionStatus == "true") {
            $("#secondRow").css("display", "");
            $("#buyButton").css("display", "none");
        }
    }
    var spanId = "";
    $("#navItemDiv").on('click', function () {

        var selectedRole = "";
//         $("#secondRow").css("display","");
        if (ssUsername != null && ssUsername != '' && ssUsername != 'null') {


            console.log(this.id);
            spanId = this.id;
            if (apiSubscriptionStatus != null && apiSubscriptionStatus != '' && apiSubscriptionStatus != 'null' && apiSubscriptionStatus == "true") {

                $("#secondRow").css("display", "");
                $("#buyButton").css("display", "none");
            } else {
                var modalObj = {
                    title: 'Message',
                    body: apiSubscriptionMessage
                };
                var buttonArray = [
                    {
                        text: 'Ok',
                        click: function () {
                            //location.reload();
                        },
                        isCloseButton: true
                    }
                ];
                modalObj['buttons'] = buttonArray;
                createModal("modalDailogDiv", modalObj);

            }
        } else {
            $('#loginModel').modal('show');
        }
//    alert(this.id);
    });


});
function getSubscription() {
    var ssUsername = $("#ssUsername").val();
    var apiSubscriptionStatus = $("#apiSubscriptionStatus").val();
//    var apiSubscriptionMessage = $("#apiSubscriptionMessage").val();
    if (ssUsername != null && ssUsername != '' && ssUsername != 'null') {
        if (apiSubscriptionStatus != null && apiSubscriptionStatus != '' && apiSubscriptionStatus != 'null' && apiSubscriptionStatus == "true") {
            $("#secondRow").css("display", "");
        }
    }

    if (ssUsername != null && ssUsername != '' && ssUsername != 'null') {
        $.ajax({
            type: 'POST',
            dataType: 'HTML',
            url: 'getCloudSubscriptionForm',
            cache: false,
            data: {
                highLevelMenu: $("#highLevelMenu").val(),
                formTpe: 'CLOUD_FORM'
            },
            success: function (response) {
                var modalObj = {
//                    title: 'Subscription',
                    title: labelObject['Subscription'] != null ? labelObject['Subscription'] : 'Subscription',
                    modelSize: 'modal-xl',
                    body: response,

                };
                cloudFormpopup("modalDailogDiv", modalObj);

            }, //success
            error: function (e) {

            }
        });//ajax
    } else {
        $('#loginModel').modal('show');
    }

}


function fetchTabGrid(selectedIndex) {

    ajaxStart();
    console.log("selectedTabIndex::" + selectedIndex);
    if (selectedIndex != null) {
        //  alert("::::"+selectedIndex);
        var gridIdStr = $("#gridIdStr").val();
        var gridIdArry = gridIdStr.split(",");
        for (var i = 0; i < gridIdArry.length; i++) {
            $("#" + gridIdArry[i] + "_FILTER_FORM").html("");
        }
        var compTypeStr = $("#compTypeStr").val();
        var compTypeArray = compTypeStr.split(",");
        var selectedGridCompType = compTypeArray[selectedIndex];
        var selectedGridId = gridIdArry[selectedIndex];
        var highLevelMenu = $("#highLevelMenu").val();
        $('#exportGridId').val(selectedGridId);
        if (selectedGridId != null && selectedGridId != '') {
            $('#' + selectedGridId).off('cellclick');
            alert("selectedGridId:::" + selectedGridId);
            $.ajax({
                type: "post",
                traditional: true,
                dataType: 'json',
                url: "getTabDataByCompId",
                cache: false,
                data: {
                    selectedCompId: selectedGridId,
                    selectedCompType: selectedGridCompType,
                    highLevelMenu: highLevelMenu,
                    modifySubscriptionFlag: $('#modifySubscriptionFlag').val(),
                    renewSubscriptionFlag: $('#renewSubscriptionFlag').val(),
                    subscriptionId: $('#subscriptionId').val()
                },
                success: function (response) {
                    ajaxStop();
                    if (selectedGridCompType == 'HTML')
                    {
                        $("#" + selectedGridId).html(response['htmlStr']);
                    } else if (selectedGridCompType == "CHART") {
                        getChartsData(response, selectedGridId); //charts
                    } else if (selectedGridCompType == "SUBSCRIPTION") {
                        $("#" + selectedGridId).html(response['subStr']);
                    } else if (selectedGridCompType == "API") {
                        var divstr = '<div class="cloudHTMLFormChildDiv" id="' + selectedGridId + '_UPCOMMING_DIV" ><span class="" style="color: gray; cursor:pointer;" onclick="fetchSubscriptions(\'' + selectedGridId + '_UPCOMMING_DIV\', \'UPCOMING\',\'' + highLevelMenu + '\',\'' + selectedGridId + '\')">&#9654; Upcoming</span><div style="padding-top: 10px; display:none;"></div></div>';
                        divstr += '<div class="cloudHTMLFormChildDiv" id="' + selectedGridId + '_HISTORY_DIV" ><span class="" style="color: gray; cursor:pointer;" onclick="fetchSubscriptions(\'' + selectedGridId + '_HISTORY_DIV\', \'HISTORY\',\'' + highLevelMenu + '\',\'' + selectedGridId + '\')">&#9654; History</span><div style="padding-top: 10px; display:none;"></div></div>';
                        $("#" + selectedGridId).html('<div class="cloudHTMLFormChildDiv">' + response['apiStr'] + '</div>' + divstr);
                    } else if (selectedGridCompType == 'FILTER_GRID') {
                        $("#" + selectedGridId + "_FILTER_GRID").html(response['filterString']);
                        $("#" + selectedGridId + "_ACCORDIAN").accordion({
                            collapsible: true,
                            heightStyle: "content",
                            active: false,
                            autoHeight: false
                        });
                        $("#" + selectedGridId + "_ACCORDIAN  h3").bind('click', function () {
                            var self = this;
                            setTimeout(function () {
                                var theOffset = $(self).offset();
                                $('body,html').animate({scrollTop: theOffset.top - 40});
                            }, 310); // ensure the collapse animation is done
                        });
                        getFilterGridForm(selectedGridId, '', selectedIndex);
                        $("#" + selectedGridId + "_ACCORDIAN").accordion({active: 0});
                    } else {
                        gridConfig(response, 0, [], selectedGridId);
                    }

                },
                error: function (e) {
                    sessionTimeout(e);
                }// Error function in Ajax
            }); // end ajax call
        }// end if(selectedGridId != null && selectedGridId != '')
    }// end if (selectedIndex != null)

    $("#modifySubscriptionFlag").val("");
    $("#renewSubscriptionFlag").val("");
    $("#subscriptionId").val("");

}
function gridConfig(gridResultObj, selectedGridIndex, paramArray, selectedGridId) {

    ajaxStart();
    // ravi start 
    globalTabId = gridResultObj['gridId'];
    // ravi end 
    console.log(":293::gridConfig::");

    $("#searchGrid").html("<div id='" + gridResultObj['gridId'] + "'></div>");
//        $("#" + gridResultObj['gridId']).jqxGrid({ scrollbarsize: 2});
//            var length = $('#cloudTabs').jqxTabs('length');
//            console.log("::selectedGridIndex:::::" + selectedGridIndex + ":::::length:::::" + length);
//            if (length >= 1) {
//                try {
//                    $("#" + gridResultObj['gridId']).jqxGrid("destroy");
//                    $("#" + gridResultObj['gridId'] + "_sort_columns").remove();
//                    // $(".jqx-clear .jqx-border-reset .jqx-overflow-hidden .jqx-max-size .jqx-position-relative").remove();
//                } catch (e) {
//                }
//                $("#" + gridResultObj['gridId']).remove();
//                console.log(gridResultObj['gridId'] + ":::::$(gridResultObj['gridId']).length::::::" + $("#" + gridResultObj['gridId']).length);
//                if ($("#" + gridResultObj['gridId']).length == 0) {
//                    //it doesn't exist
//                    var compTypeStr = $("#compTypeStr").val();
//                    var compTypeArray = compTypeStr.split(",");
//                    var selectedGridCompType = compTypeArray[selectedGridIndex];
//                    if (selectedGridCompType == 'FILTER_GRID') {
//                        $("#" + gridResultObj['gridId'] + "_DIV").html("<div id='" + gridResultObj['gridId'] + "'></div>");
//                    } else {
//                        $("#" + gridResultObj['gridId'] + "_DIV_GRID").html("<div id='" + gridResultObj['gridId'] + "'></div>");
//                    }
//
//                }
//            }



    try {
        // if(true) {
        try {
            $("#" + gridResultObj['gridId']).jqxGrid('updatebounddata', 'cells');
            $('#' + gridResultObj['gridId']).jqxGrid('clearfilters');
        } catch (e) {

        }

        // $('#' + gridResultObj['gridId']).jqxGrid('destroy');
        alert(subTabId + "::::" + $("#" + gridResultObj['gridId']).length);

//                var selectedItem = $('#cloudTabs').jqxTabs("selectedItem") + 1;
//                var defalultImg = $("#cloudTabs ul li:nth-child(" + selectedItem + ")").find('img').attr('src');
////                if (defalultImg != null) {
//                    var n = defalultImg.indexOf("_white");
//                    if (!(n > -1)) {
//                        var mainnewimage = defalultImg.replace(".png", "").replace(/_white/g, "");
//                        var attributes = $("#cloudTabs ul li:nth-child(" + selectedItem + ")").attr("id");
//                        $('#' + attributes).find('img').attr('src', mainnewimage + '_white.png');
//                    }
//                }

        // ravi end
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

//                                var imagerenderer = function (row, datafield, value) {
//
//                                    return '<img src="" id="ind' + row + '" class="indimage"><label id="imgLabel' + row + '" class="indimage">Show Image</label>';
//                                };
            }
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
//                gridPropObj.autorowheight = true;
                // console.log("renderToolbar::::"+renderToolbar);
                //  alert("renderToolbar:::"+renderToolbar);
                gridPropObj.renderToolbar = eval('(' + renderToolbar + ')');
                //      var defaultTabName = $("#defaultTabName").val();
                var htmlContentRender = function (row, columnfield, value, defaulthtml, columnproperties, rowData) {
                    var data = "<xmp>" + value + "</xmp>";
                    var element = $(data);
                    element.addClass('visionSearchWrapDescrDiv');
                    var gridRowHeight = $("#" + gridResultObj['gridId']).jqxGrid('rowsheight');
                    if (gridRowHeight != null && parseInt(gridRowHeight) <= 50) {
                        element.css('overflow-y', 'scroll');//overflow-y:scroll !important;
                    }
                    if (columnInitParamObj != null && !jQuery.isEmptyObject(columnInitParamObj)) {
                        var selectedColumnInitParamObj = columnInitParamObj[columnfield];
                        if (selectedColumnInitParamObj != null && !jQuery.isEmptyObject(selectedColumnInitParamObj)) {
                            var uuu_TitleValueColumn = selectedColumnInitParamObj['uuu_TitleValueColumn'];
                            if (uuu_TitleValueColumn != null && uuu_TitleValueColumn != '' &&
                                    rowData != null
                                    && rowData[uuu_TitleValueColumn] != null
                                    && rowData[uuu_TitleValueColumn] != ''
                                    ) {//REQUIRED_FLAG
                                element.removeAttr('title');
                                element.attr('title', rowData[uuu_TitleValueColumn]);
                            }
                        }

                    }
                    return element[0].outerHTML;
                };
                var urlRender
                        = function (row, columnfield, value, defaulthtml, columnproperties, rowData) {
                            var element = $(defaulthtml);
                            element.attr("onclick", "openURLInTab('" + value + "')");
                            element.addClass("visionSearchUrlLink");
                            return element[0].outerHTML;

                        };
                var imageRender
                        = function (row, columnfield, value, defaulthtml, columnproperties, rowData) {
                            if (value != "" && value != null)
                            {
                                return  "<img  title='View the attachment Logo' style='cursor:pointer;'"
                                        + " src='" + value + "' class='imageStyle'  id='dtlul_" + row + "'" + "'"
                                        + " onmouseover=imageMouseHover('dtlul_" + row + "') onmouseout=imageMouseOut() >";
                            }
                        };
                var descoptrender
                        = function (row, columnfield, value, defaulthtml, columnproperties, rowData) {
                            var element = $(defaulthtml);
                            element.addClass('visionSearchWrapDescrDiv');
                            var gridRowHeight = $('#' + gridResultObj['gridId']).jqxGrid('rowsheight');
                            if (gridRowHeight != null && parseInt(gridRowHeight) <= 50) {
//                                         element.css('overflow', 'scroll');
                                element.css('overflow-y', 'scroll');//overflow-y:scroll !important;

                            }
                            if (columnInitParamObj != null && !jQuery.isEmptyObject(columnInitParamObj)) {
                                var selectedColumnInitParamObj = columnInitParamObj[columnfield];
                                if (selectedColumnInitParamObj != null && !jQuery.isEmptyObject(selectedColumnInitParamObj)) {
                                    var uuu_TitleValueColumn = selectedColumnInitParamObj['uuu_TitleValueColumn'];
                                    if (uuu_TitleValueColumn != null && uuu_TitleValueColumn != '' &&
                                            rowData != null
                                            && rowData[uuu_TitleValueColumn] != null
                                            && rowData[uuu_TitleValueColumn] != ''
                                            ) {//REQUIRED_FLAG
                                        element.removeAttr('title');
                                        element.attr('title', rowData[uuu_TitleValueColumn]);
                                    }
                                }

                            }
//                                            element.css('overflow', 'scroll');
                            return element[0].outerHTML;
                        };
                var replaceRenderer
                        = function (row, columnfield, value, defaulthtml, columnproperties, rowData) {
                            var element = $(defaulthtml);
                            if (columnInitParamObj != null && !jQuery.isEmptyObject(columnInitParamObj)) {
                                var selectedColumnInitParamObj = columnInitParamObj[columnfield];
                                if (selectedColumnInitParamObj != null && !jQuery.isEmptyObject(selectedColumnInitParamObj)) {
                                    var uuu_TitleValueColumn = selectedColumnInitParamObj['uuu_TitleValueColumn'];
                                    if (uuu_TitleValueColumn != null && uuu_TitleValueColumn != '' &&
                                            rowData != null
                                            && rowData[uuu_TitleValueColumn] != null
                                            && rowData[uuu_TitleValueColumn] != ''
                                            ) {//REQUIRED_FLAG
                                        element.removeAttr('title');
                                        element.attr('title', rowData[uuu_TitleValueColumn]);
                                    }
                                }

                            }
                            return element[0].outerHTML;
                        };

                var charColorRender
                        = function (row, columnfield, value, defaulthtml, columnproperties, rowData) {
                            var element = $(defaulthtml);
                            if (columnInitParamObj != null && !jQuery.isEmptyObject(columnInitParamObj)) {
                                var selectedColumnInitParamObj = columnInitParamObj[columnfield];
                                if (selectedColumnInitParamObj != null && !jQuery.isEmptyObject(selectedColumnInitParamObj)) {
                                    var mandColumn = selectedColumnInitParamObj['uuu_CharMandColumn'];
                                    if (!(mandColumn != null && mandColumn != '')) {
                                        mandColumn = 'REQUIRED_FLAG';
                                    }
                                    if (rowData != null && (rowData[mandColumn] == 'Y'
                                            || rowData[mandColumn] == 'M'
                                            )) {//REQUIRED_FLAG
                                        element.addClass('visionSearchCharRedDiv');

                                    }
                                    var uuu_TitleValueColumn = selectedColumnInitParamObj['uuu_TitleValueColumn'];
                                    if (uuu_TitleValueColumn != null && uuu_TitleValueColumn != '' &&
                                            rowData != null
                                            && rowData[uuu_TitleValueColumn] != null
                                            && rowData[uuu_TitleValueColumn] != ''
                                            ) {//REQUIRED_FLAG
                                        element.removeAttr('title');
                                        element.attr('title', rowData[uuu_TitleValueColumn]);
                                    }
                                }

                            }
                            return element[0].outerHTML;
                        };
                var charValueColorRender
                        = function (row, columnfield, value, defaulthtml, columnproperties, rowData) {
                            var element = $(defaulthtml);

                            if (columnInitParamObj != null && !jQuery.isEmptyObject(columnInitParamObj)) {
                                var selectedColumnInitParamObj = columnInitParamObj[columnfield];
                                if (selectedColumnInitParamObj != null && !jQuery.isEmptyObject(selectedColumnInitParamObj)) {
                                    var mandColumn = selectedColumnInitParamObj['uuu_CharValueMandColumn'];
                                    if (!(mandColumn != null && mandColumn != '')) {
                                        mandColumn = 'REQUIRED_FLAG';
                                    }
                                    if (rowData != null && (rowData[mandColumn] == 'Y'
                                            || rowData[mandColumn] == 'M'
                                            )) {//REQUIRED_FLAG
                                        element.addClass('visionSearchCharValRedDiv');
                                    }
                                    var uuu_TitleValueColumn = selectedColumnInitParamObj['uuu_TitleValueColumn'];
                                    if (uuu_TitleValueColumn != null && uuu_TitleValueColumn != '' &&
                                            rowData != null
                                            && rowData[uuu_TitleValueColumn] != null
                                            && rowData[uuu_TitleValueColumn] != ''
                                            ) {//REQUIRED_FLAG
                                        element.removeAttr('title');
                                        element.attr('title', rowData[uuu_TitleValueColumn]);
                                    }
                                }

                            }
                            return element[0].outerHTML;
                        };
                var xmlRenderer
                        = function (row, columnfield, value, defaulthtml, columnproperties) {
                            console.log("xmlRenderer::");
                            if (value != "" && value != null)
                            {
                                return  "<img src ='images/xml_icon.png' style='cursor:pointer; width: 20px; height: 20px;position: fixed; title='Click to view the Payload' style='cursor:pointer;' onclick=viewXml('" + gridResultObj['gridId'] + "','" + row + "','" + columnfield + "','" + gridResultObj['tableName'] + "')  class='imageStyle visionTemplete'  id='xmldtlul_" + row + "' >";
                            }
                        };
                var titleRender
                        = function (row, columnfield, value, defaulthtml, columnproperties, rowData) {
                            var element = $(defaulthtml);
                            if (columnInitParamObj != null && !jQuery.isEmptyObject(columnInitParamObj)) {
                                var selectedColumnInitParamObj = columnInitParamObj[columnfield];
                                if (selectedColumnInitParamObj != null && !jQuery.isEmptyObject(selectedColumnInitParamObj)) {
                                    var uuu_TitleValueColumn = selectedColumnInitParamObj['uuu_TitleValueColumn'];
                                    if (uuu_TitleValueColumn != null && uuu_TitleValueColumn != '' &&
                                            rowData != null
                                            && rowData[uuu_TitleValueColumn] != null
                                            && rowData[uuu_TitleValueColumn] != ''
                                            ) {//REQUIRED_FLAG
                                        element.removeAttr('title');
                                        element.attr('title', rowData[uuu_TitleValueColumn]);
                                    }
                                }

                            }
                            return element[0].outerHTML;
                        };

                var documentRanderer
                        = function (row, columnfield, value, defaulthtml, columnproperties) {
                            //return '<textarea readonly class="ta_style" rows=1 >' + value + '</textarea>';
                            console.log("hiiiii");
                            return '<div onclick=viewDocument("' + value + '") style="cursor:pointer;">View Document</div>';
                        };

                var editable = gridPropObj.editable;
                var gridDrpdownRenderor = function (row, columnfield, value, defaulthtml, columnproperties) {
                    var cellValue = $("#" + gridResultObj['gridId']).jqxGrid('getcellvalue', row, columnfield);
                    var viewType = "GRID-VIEW";
                    var ddwData = gridResultObj.dropDowndData;
                    var ddwObj = ddwData[columnfield];
                    var dependencyparams = ddwObj.dependencyparams;
                    if (editable) {
                        //    return "<div class='visionGridDataAlign'><div class='visionGridDataAlignInfo'>" + cellValue + "</div><div class='visionGridDataAlignImage'><img src='images/search_icon_color_2.png'  onclick=visionDropdown('" + ddwObj.ddwId.trim() + "','" + dependencyparams + "','" + viewType + "','" + ddwObj.gridId + "','" + columnfield + "','" + row + "')></div></div>";
                        return "<div class='visionGridDataAlign'><div class='visionGridDataAlignInfo'>" + cellValue + "</div><div class='visionGridDataAlignImage'><img src='images/search_icon_color_2.png' id='dd" + gridResultObj['gridId'] + columnfield + "' onclick=visionDropdown('" + ddwObj.ddwId.trim() + "','" + dependencyparams + "','" + viewType + "','" + ddwObj.gridId + "','" + columnfield + "','" + row + "')></div></div>";
                    } else
                    {
                        return "<div class='visionGridDataAlign'>" + cellValue + "</div>";
                    }

                };

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

                var paramObj = {};

                paramObj['value'] = gridResultObj['className'];
                var domainValue = $('#currentDomain').val();
                if (domainValue != null && domainValue != '' && domainValue != undefined
                        && domainValue == 'VENDOR') {
                    paramObj['column'] = 'SUPPLIER_NAME';
                    paramObj['operator'] = 'LIKE';
                                    paramArray.push(paramObj);
                }
//else {
//                    paramObj['column'] = 'TERM';
//                }

//                paramObj['operator'] = 'LIKE';
//                paramArray.push(paramObj);

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
                                throw new Error(error);
                            }, loadComplete: function (data)
                            {
                                ajaxStop();
                                changeThemeVisualization();
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

                $('#' + gridResultObj['gridId']).jqxGrid(gridPropObj);
                $("#" + gridResultObj['gridId']).on("cellclick", function (event)
                {
                    var args = event.args;
                    var rowBoundIndex = args.rowindex;
                    $("#currentRowIndex").val(rowBoundIndex);
                    $("#currentGridId").val(gridResultObj['gridId']);
                    var columnindex = args.columnindex;
                    var dataField = args.datafield;
                    sessionStorage.setItem('dataField', dataField);
                    sessionStorage.setItem('rowBoundIndex', rowBoundIndex);
                    if (columnindex == 1) {
                        navigateToForm(event.args.column.datafield, $('#' + gridResultObj['gridId']).jqxGrid('getrowdata', event.args.rowindex), 'form', gridResultObj['gridId'], event.args.rowindex);
                    }

                });
//                $("#searchgrid").on("cellclick", function (event)
//                {
//                    var args = event.args;
//                    var rowBoundIndex = args.rowindex;
//                    var columnindex = args.columnindex;
//                    var dataField = args.datafield;
//                    navigateToForm(event.args.column.datafield, $('#' + gridResultObj['gridId']).jqxGrid('getrowdata', event.args.rowindex), 'form', gridResultObj['gridId'], event.args.rowindex);
//                });
                $('#' + gridResultObj['gridId']).on('celldoubleclick', function (event) {
                    var args = event.args;
                    var dataField = args.datafield;
                    var dataField1 = args.text;
                    var rowIndex = args.rowindex;
                    var cellValue = args.value;
                    var column = $('#' + gridResultObj['gridId']).jqxGrid('getcolumn', event.args.datafield).text;
                    popupedit(column, cellValue);
                });
                $('#' + gridResultObj['gridId']).on('rowunselect', function (event) {
//                                    showSelectedRows(gridResultObj['gridId'],null,gridInitParamObj['uuu_GridNtfnFlag']);
                });
                $('#' + gridResultObj['gridId']).on('rowselect', function (event) {
                    showClassBasedButtons(gridResultObj, event.args.rowindex, gridInitParamObj);

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


                //  
                alert("604 Grid");
                $(window).resize(function () {
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
//                $('#' + gridResultObj['gridId']).jqxGrid('pagermode', 'simple');


//                $(window).resize(function ()
//                {
//                    var windowHeight = $(this).height();
//                    console.log("windowHeight:::" + windowHeight);
//
//                    if (parseInt(windowHeight) <= 450) {
//                        $('#' + gridResultObj['gridId']).jqxGrid('height', windowHeight + "px");
//
//                    } else {
//
//                        if ($("#accordioncover").length != 0) {// if enable all search
//                            // var crmSearchHeight = $('.visionMaterialGeneric').height();
//                            $('#' + gridResultObj['gridId']).jqxGrid('height', '90%');
////                                            $('#' + gridResultObj['gridId']).jqxGrid('width', '100%');
//                        } else {
//                            $('#' + gridResultObj['gridId']).jqxGrid('height', "90%");
//                            
//                        }
//
//                    }
//
//                }).resize();
//                alert("680 Grid");
// navigation clicking 
                //   ajaxStop(); /* for loader we commented by RGA */
                alert("683 Grid");
            }// end if(gridPropObj != null)

        }
        //  }

    } catch (e) {
        ajaxStop();
    }
    ajaxStop();

}// end of function gridConfig(-)
function getFilterGridForm(selectedGridId, selectedTabId, selectedGridIndex) {
    $.ajax({
        type: "post",
        traditional: true,
        // dataType: 'json',
        url: "getFilterGridForm",
        cache: false,
        data: {
            selectedGridId: selectedGridId,
            selectedTabId: selectedTabId,
            selectedGridIndex: selectedGridIndex

        },
        success: function (response) {
            if (response != null && response != '') {
                $("#" + selectedGridId + "_FILTER_FORM").html(response['result']);
                $("#importsearchcriteria").html(response['importButtonDiv']);
                var jsDateItems = response['dateObjArray'];
                for (var i = 0; i < jsDateItems.length; i++) {
                    console.log("tbid:::" + jsDateItems[i].tbid);
                    $("#" + jsDateItems[i].tbid).datepicker(
                            {dateFormat: "dd-mm-yy",
                                changeMonth: true,
                                changeYear: true})
                            .on('changeDate', function (ev) {
                                if (jsDateItems[i].type == 'min') {
                                    console.log($("#" + jsDateItems[i].tbid).datepicker("getDate"));
                                    $("#" + jsDateItems[i].tbid).datepicker(
                                            {
                                                minDate: $("#" + jsDateItems[i].tbid).datepicker("getDate")
                                            });
                                } else {
                                }
                            });
                }

                selectedTitle = "";
                selectedTitleValue = "";
                var lovColumns = response['lovColumns'];
                if (lovColumns != null && !jQuery.isEmptyObject(lovColumns)) {
                    for (var lovColumnanme in lovColumns) {
                        if (lovColumnanme != null && lovColumnanme != '') {
                            var comboBoxOptions = {
                                searchMode: 'containsignorecase',
                                width: 315,
                                height: 20,
                                dropDownHeight: 100,
                                autoComplete: true
                            };
                            if (lovColumns[lovColumnanme] == true) {
                                comboBoxOptions['multiSelect'] = true;
                                // multiSelect: true,
                            }
                            $("#" + lovColumnanme).jqxComboBox(comboBoxOptions);
                            $("#" + lovColumnanme).on('select', function (event) {
                                var args = event.args;
                                if (args) {
                                    // index represents the item's index.                          
                                    var index = args.index;
                                    var item = args.item;
                                    if (item != null) {
                                        var label = item.label;
                                        var value = item.value;
                                        if (value != null && value != '') {
                                            var filterGridFlagCount = $("#" + lovColumnanme + "_jqxComboBox").attr("data-filtergridflag-count"); //data-filtergridflag-count
                                            var operatorId = "operator" + $("#" + lovColumnanme + "_jqxComboBox").attr("data-viewid") + filterGridFlagCount;
                                            $("#" + operatorId).val("IN");
                                        }
                                    }
                                    // get item's label and value.
                                }
                            });
                        }
                    }
                }
            }

        },
        error: function (e) {
            sessionTimeout(e);
        }// Error function in Ajax
    }); // end ajax call
}
function getEnterKeyFilterSearch(event, selectedGridId, selectedGridIndex, selectedTabId) {
    if (event.which == 13) {
        ajaxStart();
        getFilterGridResults(selectedGridId, selectedGridIndex, selectedTabId);
        ajaxStop();
    }
}
function getFilterGridResults(selectedGridId, selectedGridIndex, selectedTabId, selectedColumn, isImport) {
    if (selectedGridId != null && selectedGridId != '') {
        var i = 0;
        var paramArray = [];
        if (isImport != null && isImport != '' && isImport == 'Y') {
            var paramObj = {};
            paramObj.column = selectedColumn;
            paramObj.operator = $("#operator" + selectedGridId + i).val();
            paramObj.symbol = $.trim($("#operator" + selectedGridId + i).find('option:selected').text());
            paramObj.isImport = isImport;
            paramArray.push(paramObj);
        } else {
            $("#" + selectedGridId + "_FILTER_FORM_TABLE tbody tr").each(function () {
                var isAllow = false;
                var paramObj = {};
                var colname = $(this).attr('data-colname');
                var dataRange = $(this).attr('data-range');
                var value = $("#" + selectedGridId + "_" + colname).val();
                var dataColType = $(this).attr('data-coltype');
                if (dataColType == 'L') {
                    value = "";
                    var selectBoxValue = $("#" + selectedGridId + "_" + colname).jqxComboBox('getSelectedItems');
                    if (selectBoxValue != null) {
                        for (var j = 0; j < selectBoxValue.length; j++)
                        {
                            value += selectBoxValue[j].value;
                            if (j != selectBoxValue.length - 1) {
                                value += ",";
                            }
                        }
                    }
                }
                var minvalue = $("#" + selectedGridId + "_" + colname + "_MIN").val();
                var maxvalue = $("#" + selectedGridId + "_" + colname + "_MAX").val();
                if (value != null && value != '') {
                    isAllow = true;
                } else if (dataRange != null && dataRange == 'Y'
                        && ((minvalue != null && minvalue != '')
                                || (maxvalue != null && maxvalue != ''))
                        ) {
                    isAllow = true;
                }
                var type = $("#" + selectedGridId + "_" + colname).attr("type");
                if (type != null && type == 'checkbox') {
                    var textval = "N";
                    if ($("#" + selectedGridId + "_" + colname).is(':checked')) {
                        isAllow = true;
                    } else {
                        isAllow = false;
                    }
                }
                console.log("isAllow::::" + isAllow);
                if (isAllow) {
                    paramObj.column = $.trim($(this).attr('data-colname'));
                    if (dataColType == 'L') {
                        var value = "";
                        var selectBoxValue = $("#" + selectedGridId + "_" + colname).jqxComboBox('getSelectedItems');
                        if (selectBoxValue != null) {
                            for (var j = 0; j < selectBoxValue.length; j++)
                            {
                                value += selectBoxValue[j].value;
                                if (j != selectBoxValue.length - 1) {
                                    value += ",";
                                }
                            }
                        }
                        paramObj.value = value;
                    } else if (type != null && type == 'checkbox') {
                        var textval = "N";
                        if ($("#" + selectedGridId + "_" + colname).is(':checked')) {
                            textval = "Y";
                        } else {
                            textval = "N";
                        }
                        paramObj.value = textval;
                    } else {
                        paramObj.value = $.trim($("#" + selectedGridId + "_" + colname).val());
                    }
                    paramObj.operator = $("#operator" + selectedGridId + i).val();
                    paramObj.symbol = $.trim($("#operator" + selectedGridId + i).find('option:selected').text());
                    paramObj.rangeFlag = dataRange;
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
        }
        if (paramArray != null && paramArray.length > 0) {
            $('#' + selectedGridId).off('cellclick');
            alert("selectedGridId:::" + selectedGridId);
            $.ajax({
                type: "post",
                traditional: true,
                dataType: 'json',
                url: "getTabDataByGridId",
                cache: false,
                data: {
                    gridId: selectedGridId,
                    selectedGridCompType: "GRID",

                },
                success: function (response) {
                    $("#" + selectedGridId + "_ACCORDIAN").accordion({active: 1});
                    gridConfig(response, selectedGridIndex, paramArray);
                },
                error: function (e) {
                    sessionTimeout(e);
                }// Error function in Ajax
            }); // end ajax call
        } else {
            var dialogSplitMessage = dialogSplitIconText("Please provide at least one value to Search.", "Y");
            $("#dialog1").append(dialogSplitMessage);
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
                        }
                    }
                ],
                open: function () {
                    $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                    //    $(this).closest(".ui-dialog").find(".ui-button").eq(2).addClass("dialogno");
                    $(this).closest(".ui-dialog").addClass("visionSearchSaveDialog");
                    $(".visionHeaderMain").css("z-index", "999");
                    $(".visionFooterMain").css("z-index", "999");
                }
                ,
                beforeClose: function (event, ui)
                {
                    $(".visionHeaderMain").css("z-index", "99999");
                    $(".visionFooterMain").css("z-index", "99999");
                }
            });
        }
    }
}
function clearFilterGridSearch(selectedGridId) {
    $("#" + selectedGridId + "_filter_columns").remove();
    $("#" + selectedGridId + "_FILTER_FORM_TABLE input[type=checkbox]").prop('checked', false);
    $("#" + selectedGridId + "_FILTER_FORM_TABLE input[type=text]").val('');
    $("#" + selectedGridId + "_FILTER_FORM_TABLE input[type=text]").removeAttr('disabled');
    $("#" + selectedGridId + "_FILTER_FORM_TABLE :input").each(function () {
        var textid = $(this).attr("id");
        var type = $(this).attr("type");
        if (type == 'hidden') {
            $("#" + textid + "_LABELS").html("");
            $("#" + textid + "_LABELS").html($(this).attr("data-defaultlabel"));//defaultlabel
            $("#" + textid).val("");
        } else {
            if (textid != null && textid.indexOf("_jqxComboBox") > -1) {
                var comboBoxId = textid.replace("_jqxComboBox", "");
                $("#" + comboBoxId).jqxComboBox('clearSelection');
            } else {
                $("#" + textid + "_MIN").hide();
                $("#" + textid + "_MIN").css("display", "none");
                $("#" + textid + "_MAX").hide();
                $(".filtergridtd_range").hide();
                $(".filtergridtd_range").css("display", "none");
                $("#" + textid + "_MAX").css("display", "none");
                $("#" + textid + "_TO").hide();
            }

        }
    });
    var i = 0;
    $('select').each(function () {
        $(this).attr('data-staged', 'N');
        $(".fs-label").html("");
        $(".fs-option").removeClass('selected');
        $(".fs-label").html("Select");
        $("#" + "operator" + selectedGridId + i).prop('selectedIndex', 0);
        i++;
    });
    ajaxStop();
}
function navigateToForm(datafield, data, redirectType, gridId, selectedTabId, selectingrowindex) {
    ajaxStart();
    showLoader();
    var navigationGridId = "";
    firstPanelShowFlag = true;
    secondPanelShowFlag = true;
    //  var datafield = column.datafield;
    $('#firstDxpSplitter').jqxSplitter({width: '100%', height: '635', orientation: 'vertical', splitBarSize: 0, panels: [{size: 30}]});
    $('#secondDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 0}]});
    $('#thirdDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 0}]});
    $('.decendingOrder').hide();
    $('.mainBasketBookMark').show();
    $('.searchIconsList').hide();
    $('.decendingFirstOrder').hide();
    $('.searchFirstResultsList').hide();
    $('.searchDXPCreate').hide();
    $('.dxpMaterialListClass').hide();
    $('.viewFormBasketDiv').hide();
    $('.searchResultsList activeResult').hide();
    $('.searchResultMaterialResults').hide();
    $('.viewFormDiv').removeClass('active');
    $('.viewFormBasketDiv').removeClass('active');
    $('.viewClassDiv').removeClass('active');
    $('.viewClassBasketDiv').removeClass('active');
    var items = {};
    var linkedColumns = "RECORD_NO,INSTANCE,BUSINESS_UNIT";
    $("#linkedColumns").val(linkedColumns);
    if (linkedColumns != null && linkedColumns != '') {
        for (var key in data) {
            if (key.lastIndexOf("PLANT") > -1) {
                items[key] = data[key];
            }
            if (linkedColumns.lastIndexOf(key) > -1) {

                var value = data[key];
                //    console.log("key::::" + key + ":::value::::" + value);
                value = value.replace(/\s/gi, "_");
                value = value.replace(/[#]/g, "_");
                //  console.log("key::::" + key + ":::value::::" + value);
                items[key] = value;
            }
        }
    }
    var stripValue = $("#stripValue").val();
    if (stripValue == null || stripValue != undefined || stripValue == '') {
        stripValue = "CONCEPT_ID,#;";
        $("#stripValue").val(stripValue);
    }
    var stripValueObjArray = [];
    if (stripValue != null) {
        var stripValObj = stripValue.split(";");
        for (var i = 0; i < stripValObj.length; i++)
        {
            var stripValueObj = {};
            if (stripValObj[i] != null && stripValObj[i] != '' && typeof stripValObj[i] != 'undefined') {
                if (stripValObj[i].indexOf(",") > -1) {
                    var stripVal = stripValObj[i].split(",");
                    //                                     if (stripVal[0] != null && stripVal[1] != null) {
                    stripValueObj.columnName = stripVal[0];
                    stripValueObj.value = stripVal[1];
//                                        stripValueObj.value = encodeURIComponent(stripVal[1]);
                    stripValueObjArray.push(stripValueObj);
                }

            }

        }

    }

    var hiddenObjStr = $("#hiddenObj").val();
    if (hiddenObjStr != null) {
        var hiddenObj = JSON.parse(hiddenObjStr);
        for (var key in hiddenObj) {
            var value = hiddenObj[key];
            // alert(key+":::::"+value);
            if (value != null && value != '' && value != 'null') {
                if (key != null && key.lastIndexOf("HIDDEN") > -1) {

                    var columnsArray = value.split(",");
                    //  alert("columnsArray:::"+columnsArray);
                    var hiddenIds = key.split("HIDDEN_");
                    var hiddenVal = data[hiddenIds[1]];
                    //alert("hiddenIds[1]:::"+hiddenIds[1]);
                    //  alert("hiddenVal:::"+hiddenVal);
                    for (var i = 0; i < columnsArray.length; i++) {
                        if (columnsArray[i] != 'NAME1') {
                            items[columnsArray[i]] = hiddenVal;
//                                                items[columnsArray[i]] = encodeURIComponent(hiddenVal);

                        }
                    }

                }
            } else {
                //alert("Value is null");
            }
        }
    }
    var panelId = "";
    var currentGridId = $("#currentGridId").val();
    var currentDomain = $("#currentDomain").val();
    var operationName = $("#operationName").val();
    if (operationName == null || operationName == undefined || operationName == '') {
        if (currentDomain != null && currentDomain != '' && currentDomain != undefined
                && (currentDomain == 'All' || currentDomain == 'PRODUCT')) {
            panelId = "MM_PANEL_MGR_PENDING_REG";
        } else if (currentDomain != null && currentDomain != '' && currentDomain != undefined
                && currentDomain == 'SERVICE') {
            panelId = "SM_PANEL_MGR_PENDING_REG";

        } else if (currentDomain != null && currentDomain != '' && currentDomain != undefined
                && currentDomain == 'VENDOR') {
            items.SUPPLIER_NAME = data['SUPPLIER_NAME'];
            panelId = "VM_PANEL_MGR_REG_ACCEPTED_BY_ERP";
//        $("#panelId").val(panelId);
        }
    } else {
        if (operationName == 'Extend') {
            panelId = "MM_PANEL_MGR_NEW_EXT";
            currentGridId = "MM_NEW_EXT_MGR";
            searchExtend();
        } else if (operationName == 'delete') {
            panelId = "MM_PANEL_NEW_DEL_MGR";
            currentGridId = "MM_NEW_DEL_MGR";
            deleteRequest();
        } else if (operationName == 'UnDelete') {
            panelId = "MM_PANEL_NEW_UDEL_MGR";
            currentGridId = "MM_NEW_UDEL_MGR";
            undeleteRequest();
        } else if (operationName == 'Change') {
            panelId = "MM_PANEL_NEW_CHNG_MGR";
            currentGridId = "MM_NEW_CHNG_MGR";
            newChangeRequest();
        }

    }
    $("#panelId").val(panelId);
    items.stripValue = stripValueObjArray;
    items.imageTable = $("#imageTable").val();
    items.imageTableColumn = $("#imageTableColumn").val();
    items.imageColumn = $("#imageColumn").val();
    items.CONCEPT_ID = $("#classConceptId").val();
    items.linkedColumns = $("#linkedColumns").val();
    items.gridId = currentGridId;
    items.panelId = panelId;
    items.SUPPLIER_NAME = data['SUPPLIER_NAME'];
    if (data['TERM'] != null && data['TERM'] != '') {
        items.TERM = data['TERM'];
    }
    if (data['CLASS_TERM'] != null && data['CLASS_TERM'] != '') {
        items.CLASS_TERM = data['CLASS_TERM'];
    }
    items.BUSINESS_UNIT = data['BUSINESS_UNIT'];
    items.INSTANCE = data['INSTANCE'];
    items.selectingrowindex = data['boundindex'];
    items.showFlag = $("#showFlag").val();
//        items.tabId = "MM_PENDING_REQ_REG_MGR_TAB";
//    var datainformation = $('#' + items.gridId).jqxGrid('getdatainformation');
//    var rowscount = datainformation.rowscount;
//    items.selectingrowindex = selectingrowindex;
//    items.rowscount = rowscount;
    var itemsstring = JSON.stringify(items);
    $("#itemsstring").val(itemsstring);
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: "formData",
        cache: false,
        data: {
            items: itemsstring,
            data: JSON.stringify(data)
        },
        success: function (response) {
            ajaxStop();
            stopLoader();
            var form = response['formStr'];
//             var tapForm = form['topForm'];
//            var formObj = {};
//            formObj = 
            //fourthDxpSplitterData
            $(".dxpGridHideShow").show();
            $("#fourthDxpSplitter").show();
            var dxpAdavanceSearchOptions = $('#dxpAdavanceSearchOptions').val();
            if (dxpAdavanceSearchOptions != null && dxpAdavanceSearchOptions != undefined
                    && dxpAdavanceSearchOptions != '' && (dxpAdavanceSearchOptions == 'C' || dxpAdavanceSearchOptions == 'P' || dxpAdavanceSearchOptions == 'PRA')) {
                $("#searchresultsSplitter").html(form);
            } else if (dxpAdavanceSearchOptions != null && dxpAdavanceSearchOptions != undefined
                    && dxpAdavanceSearchOptions != '' && (dxpAdavanceSearchOptions == 'S' || dxpAdavanceSearchOptions == 'D' ||
                            dxpAdavanceSearchOptions == 'PR')) {
                $("#fourthDxpSplitter").html(form);
            } else {
                $("#fourthDxpSplitter").html(form);
            }
            $('.viewClassDiv').removeClass('active');
            $('.viewClassBasketDiv').removeClass('active');
            $('.viewGridDiv').removeClass('active');
            $('.viewGridBasketDiv').removeClass('active');
            $(".searchIconsList").hide();
            $(".searchResultsList").hide();
            $('#thirdDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 0}]});
            $(".accordian").accordion({
                theme: 'energyblue',
                collapsible: true,
                heightStyle: "content",
                active: false,
                autoHeight: false,
                animate: 300
            });
            $('.searchResultMaterialResults').show();
            $('.accordian h3').bind('click', function () {
                var self = this;
                setTimeout(function () {
                    var theOffset = $(self).offset();
                    $('body,html').animate({scrollTop: theOffset.top - 40});
                }, 310); // ensure the collapse animation is done
            });
            $("#CLASS_TERM").val(data['TERM']);
            if (operationName == 'Extend') {

                searchExtend();
            } else if (operationName == 'delete') {

                deleteRequest();
            } else if (operationName == 'UnDelete') {

                undeleteRequest();
            } else if (operationName == 'Change') {

                newChangeRequest();
            }
            validWorkflow();
            if (data['SOURCE'] == "CHANGE") {
                $("#accordion").find(".ui-state-disabled").hide();
                for (let a = 2; a <= 15; a += 2) {
                    b = a + 1;
                    $("#regRorm" + b).prepend("<b style=\"font-weight:700\">OLD Data.</b> ");
                    $("#regRorm" + b).appendTo($("#regRorm" + a));
                    $("#regRorm" + b).show();
                }
            }
            saveOldPanelData();


        },
        error: function (e) {
            sessionTimeout(e);
        }// Error function in Ajax
    });

}
function showCollapse(id)
{
    var value = id;
    var icons = $("#accordion").accordion("option", "icons");
    // alert(value);
    if (value === 'expandAll')
    {
        $('#expandAll').css("display", "none");
        $('#collapseAll').css("display", "inline-block");
        $("#accordion").children(".visionRegisterMaterialTableTab").attr("aria-hidden", "false");
        $("#accordion").children(".visionRegisterMaterialTableTab").css("display", "block");
        $("#accordion").children("h3").attr("aria-selected", "true");
        $("#accordion").children("h3").attr("aria-expanded", "true");
        $("#accordion").children("h3").attr("tabindex", "0");
        $("#accordion").children("h3").removeClass("ui-accordion-header-collapsed ui-corner-all ui-state-focus");
        $("#accordion").children("h3").addClass("ui-accordion-header-active ui-state-active ui-state-hover");


        var userIds = [];
        $('.ui-accordion-header').each(function (index, element) {
            var onclickFunction = $(this).attr('onclick');
            userIds.push(onclickFunction);
        });

        for (var i = 0; i < userIds.length; i++)
        {
            eval(userIds[i]);
        }
        $('.ui-accordion-header').removeClass('ui-corner-all').addClass('ui-accordion-header-active ui-state-active ui-corner-top').attr({
            'aria-selected': 'true',
            'tabindex': '0'
        });
        $('.ui-accordion-header-icon').addClass(icons.activeHeader);
//        $('.ui-accordion-header-icon').removeClass(icons.header).addClass(icons.headerSelected);
        $('.ui-accordion-content').addClass('ui-accordion-content-active').attr({
            'aria-expanded': 'true',
            'aria-hidden': 'false'
        }).show();
        $(".ui-accordion-header").addClass("ui-state-disabled");

    } else
    {
        $('#collapseAll').css("display", "none");
        $("#accordion").children(".visionRegisterMaterialTableTab").attr("aria-hidden", "true");
        $("#accordion").children(".visionRegisterMaterialTableTab").css("display", "none");
        $("#accordion").children("h3").attr("aria-selected", "false");
        $("#accordion").children("h3").attr("aria-expanded", "false");
        $("#accordion").children("h3").attr("tabindex", "-1");
        $("#accordion").children("h3").removeClass("ui-accordion-header-active ui-state-active ui-state-hover");
        $("#accordion").children("h3").addClass("ui-accordion-header-collapsed ui-corner-all ui-state-focus");
        $('#expandAll').css("display", "inline-block");
        $('.ui-accordion-header').removeClass('ui-accordion-header-active ui-state-active ui-corner-top').addClass('ui-corner-all').attr({
            'aria-selected': 'false',
            'tabindex': '-1'
        });
        $('.ui-accordion-header-icon').removeClass(icons.activeHeader);
        $('.ui-accordion-content').removeClass('ui-accordion-content-active').attr({
            'aria-expanded': 'false',
            'aria-hidden': 'true'
        }).hide();
        $(".ui-accordion-header").removeClass("ui-state-disabled");
    }
}

//function showCollapse(id)
//{
//    var value = id;
//    // alert(value);
//    if (value === 'expandAll')
//    {
//        $('#expandAll').css("display", "none");
//        $('#collapseAll').css("display", "inline-block");
//
//    } else
//    {
//        $('#collapseAll').css("display", "none");
//        $('#expandAll').css("display", "inline-block");
//    }
//}
$(window).scroll(function () {
    $("#top_arrow").show();
    $("#bottom_arrow").hide();
    var scroll = $(window).scrollTop();
    //console.log(scroll);
    if (scroll <= 0)
    {
        $("#top_arrow").hide();
        $("#bottom_arrow").show();


    } else {
        $("#top_arrow").show();
        $("#bottom_arrow").hide();
    }
});
$(document).ready(function () {
    var icons = $("#accordion").accordion("option", "icons");
    $('.expandAll').click(function () {
        var userIds = $('.ui-accordion-header').map(function () {
            return $(this).data('onclick');
        }).get();

        for (var i = 0; i < userIds.length; i++)
        {
            eval(userIds[i]);
        }

        $('.ui-accordion-header').removeClass('ui-corner-all').addClass('ui-accordion-header-active ui-state-active ui-corner-top').attr({
            'aria-selected': 'true',
            'tabindex': '0'
        });
        $('.ui-accordion-header-icon').removeClass(icons.header).addClass(icons.headerSelected);
        $('.ui-accordion-content').addClass('ui-accordion-content-active').attr({
            'aria-expanded': 'true',
            'aria-hidden': 'false'
        }).show();
        $(this).attr("disabled", "disabled");
        $('.collapseAll').removeAttr("disabled");
        $(".ui-accordion-header").addClass("ui-state-disabled");
    });
    $('.collapseAll').click(function () {
        $('.ui-accordion-header').removeClass('ui-accordion-header-active ui-state-active ui-corner-top').addClass('ui-corner-all').attr({
            'aria-selected': 'false',
            'tabindex': '-1'
        });
        $('.ui-accordion-header-icon').removeClass(icons.headerSelected).addClass(icons.header);
        $('.ui-accordion-content').removeClass('ui-accordion-content-active').attr({
            'aria-expanded': 'false',
            'aria-hidden': 'true'
        }).hide();
        $(this).attr("disabled", "disabled");
        $('.expandAll').removeAttr("disabled");
        $(".ui-accordion-header").removeClass("ui-state-disabled");
    });
    $('.ui-accordion-header').click(function () {
        $('.expandAll').removeAttr("disabled");
        $('.collapseAll').removeAttr("disabled");
        //                                        $('html, body').animate({
        ////                                            scrollTop: $(document).height()
        //                                        }, 2300);
    });
    $('.visionRegisterMaterialTableTab').on("click", "li", function () {
        var self = this;
        setTimeout(function () {
            var theOffset = $(self).offset();
            $('body,html').animate({scrollTop: theOffset.top - 80});
            $(this).next().visionTabMenuFormData('show', 20);
        }, 310); // ensure the collapse animation is done
    });
});
function newDxpClassCreation(className, typedValue, abbrivation, content, conceptId, unspscCode, recordGroup) {
    showLoader();
    $('#firstDxpSplitter').jqxSplitter({width: '100%', height: '635', orientation: 'vertical', splitBarSize: 0, panels: [{size: 30}]});
    $('#secondDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 0}]});
    $('#thirdDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 1450}]});
    $("#fourthDxpSplitter").html('');
    $('.searchFirstResultsList').hide();
    $('.searchResultsList').hide();
    $('.searchDXPCreate').hide();
    $('.decendingFirstOrder').hide();
    var itemsObj = {};
    var data = {};
    var linkedColumns = "CONTENT,TERM,CLASS,CLASS_TERM,DEFINITION,uid,UID,HIDDEN_GRID_ID";
    if (linkedColumns != null && linkedColumns != '') {
        for (var key in data) {
            if (linkedColumns.lastIndexOf(key) > -1) {
                var value = data[key];
                value = value.replace(/\s/gi, "_");
                value = value.replace(/[#]/g, "_");
                itemsObj[key] = value;
            }
        }
    }
    var gridId = '';
    var currentrole = sessionStorage.getItem("currentRole");
    var currentDomain = $("#currentDomain").val();
    if (currentDomain != null && currentDomain != '' && currentDomain != undefined
            && currentDomain == 'PRODUCT') {
        gridId = 'MM_SAP_NEW_REG';
    } else if (currentDomain != null && currentDomain != '' && currentDomain != undefined
            && currentDomain == 'SERVICE') {
        gridId = 'SM_SAP_NEW_REG';
    }
    itemsObj.currentrole = currentrole;
    itemsObj.linkedColumns = linkedColumns;
    itemsObj.CLASS = className;
    itemsObj.CLASS_TERM = className;
    itemsObj.typedValue = typedValue;
    itemsObj.gridId = gridId;
    itemsObj.ABBREVIATION = abbrivation;
    itemsObj.CONTENT = content;
    itemsObj.CONCEPT_ID = conceptId;
    itemsObj.UNSPSC_CODE = unspscCode;
    itemsObj.RECORD_GROUP = recordGroup;
//    itemsObj.panelId = "MM_PANEL_MGR_PENDING_REG";
    var items = JSON.stringify(itemsObj);
    $("#itemsstring").val(items);
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: "itemRegisterForm",
        cache: false,
        data: {
            items: items,
            data: JSON.stringify(data)
        },
        success: function (response) {
            stopLoader();
            var form = response['formStr'];
            if (form != null && form != undefined && form != '') {
                $('#thirdDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 600}]});
                $("#thirdDxpSplitter").show();
                $("#thirdDxpSplitter").jqxSplitter('collapse');
                $("#fourthDxpSplitter").show();
                $("#fourthDxpSplitter").html(form);
                $("#treeGridDiv").show();
                $("#treeGridDiv").html(form);
                registerClickFunction();
                stopLoader();
            } else {
                var message = response['message'];
                var modalObj = {
//                    title: 'Message',
                    title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
                    body: message
                };
                var buttonArray = [
                    {
                        text: 'Ok',
                        click: function () {
                            $('#loginModel').modal('show');
                        },
                        isCloseButton: true
                    }
                ];
                modalObj['buttons'] = buttonArray;
                createModal("dataDxpSplitterValue", modalObj);
            }

        },
        error: function (e) {
            sessionTimeout(e);
        }// Error function in Ajax
    });
}
function registerClickFunction() {
    showLoader();
    $("#Register").click(function () {
        labelObject = {};
        try {
            labelObject = JSON.parse($("#labelObjectHidden").val());
        } catch (e) {

        }
        var errorCount = 0;


        if (errorCount == 0) {


            //  ////alert("CALL AJAX");
            var basicIds = [];
            var basicData = {};
            var roleId = '';
            var currentDomain = $("#currentDomain").val();
            if (currentDomain != null && currentDomain != '' && currentDomain != undefined
                    && currentDomain == 'PRODUCT') {
                roleId = 'MM_MANAGER';
            } else if (currentDomain != null && currentDomain != '' && currentDomain != undefined
                    && currentDomain == 'SERVICE') {
                roleId = 'SM_MANAGER';
            } else if (currentDomain != null && currentDomain != '' && currentDomain != undefined
                    && currentDomain == 'VENDOR') {
                roleId = 'VM_MANAGER';
            }
            var roleStartsWith = roleId.substring(0, 2);
            var duplCheck = $("#Register").attr('data-dupl-flag');
            var dataReturnReason = $("#Register").attr('data-returnreason');
            $("#mat_creation_form_table :input").each(function () {
                var textid = $(this).attr("id");
                var displayAttr = $("#" + textid).attr("display");
                //  console.log(textid+"::::displayAttr:::"+displayAttr);
                var type = $(this).attr("type");
                var textval = $(this).val();
                if (type != 'hidden') {
//                if ((type != null && type != 'hidden') || type == null || type == '' || type == undefined || type == 'undefined' ) {
                    if (textval != null && textval != '') {
                        textval = textval.toUpperCase();
                    }
                }
                if (type != null && type == 'checkbox') {//
                    if ($("#" + textid).is(':checked')) {
                        textval = "Y";
                    } else {
                        textval = "N";
                    }
                }
                ("column nameL:::" + textid);
                console.log("column Value:::" + textval);

                basicIds.push(textid);
//                  jsonOBJ.ids.push(textid.toLowerCase());
                if (textid != null && textid != 'CREATE_DATE') {

                    basicData[textid] = textval;

                }

                if (textid != null && textid.lastIndexOf("HIDDEN") > -1) {
                    var columnNames = $("#" + textid).val();
                    var columnsArray = columnNames.split(",");

                    var hiddenIds = textid.split("HIDDEN_");
                    var hiddenVal = $("#" + hiddenIds[1]).val();
                    for (var i = 0; i < columnsArray.length; i++) {
                        basicIds.push(columnsArray[i]);
                        if (hiddenVal != null) {
                            hiddenVal = hiddenVal.toUpperCase();
                        }
                        basicData[columnsArray[i]] = hiddenVal;
//                        basicData[columnsArray[i]] = encodeURIComponent(hiddenVal);
                    }

                }


            });


            alert("Basic_data#" + JSON.stringify(basicData));
            // return false;
            //  ////alert("before ajax call");
            var resultArray = registerValidation();
            alert("resultArray:::" + JSON.stringify(resultArray));
            if (resultArray != null && Object.keys(resultArray).length == 0) {
                $(".allErrors").hide();

                //var registerValidateColumn = basicData['registerValidateColumn'];
                if ((roleStartsWith == "VM" || roleStartsWith == "CM") && duplCheck != null && duplCheck != ''
                        && duplCheck == 'Y') {
                    var vmDuplOnSubmit = "";
                    vmDuplOnSubmit = $("#vmDuplOnSubmit").val();

                    if (vmDuplOnSubmit == null) {
                        vmDuplOnSubmit = "";

                    }

                    // alert(vmDuplOnSubmit);

                    if (true) {
//            if (vmDuplOnSubmit == 'Y') {

//alert(vmDuplOnSubmit);
                        var req = {};
                        req.type = 'POST';
                        req.traditional = true;
                        req.dataType = 'html';

                        req.url = 'vmCmRegDuplicateCheck';
                        req.data = {
                            basicData: JSON.stringify(basicData),
//                    vendorName: $("#SUPPLIER_NAME").val().toUpperCase()
                        };

                        req.success = function (result) {
                            ajaxStop();
                            var dataObj = JSON.parse(result);

                            if (!dataObj['flag']) {
                                registerCheckValidation(basicData);
                            } else {

                                $("#dialog2").html("");
                                $("#dialog2").html(dataObj['message']);
                                $("#dialog2").dialog({
                                    title: (labelObject['Duplicates Found'] != null ? labelObject['Duplicates Found'] : 'Duplicates Found'),
                                    opacity: 5.5,
                                    zIndex: 10000,
                                    width: '800',
                                    fluid: true,
                                    buttons: [{
                                            text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                                            click: function () {
                                                var vmDuplPopUp = "";
                                                vmDuplPopUp = $("#vmDuplPopUp").val();
                                                if (vmDuplPopUp == null) {
                                                    vmDuplPopUp = "";

                                                }

                                                var role = $("#rolehid").val();
                                                if (dataReturnReason != null && dataReturnReason != '') {
//                                            try {
//                                                if (controlInd != null && controlInd != '') {
//                                                    controlInd = controlInd.toUpperCase();
//                                                }
//
//                                            } catch (e) {
//
//                                            }
//                                            console.log(controlInd + ":::1531:::::::::::::::");

                                                    var msgTitle = "Duplicate comment";

                                                    msgTitle = (labelObject[msgTitle] != null ? labelObject[msgTitle] : msgTitle);
                                                    var rejectType = $("#rejectType").val();



                                                    if (rejectType == 0)
                                                    {
                                                        response = "";

                                                        $("#textReason").html("");

                                                        response += "<div id='textReason'>";
                                                        response += "<textarea id='reasonId' class='visionDeleteReason'></textarea></div>";
                                                        response += "<div id='dailog_error_id' style='display:none;color:red'>" + (labelObject['Please give any reason'] != null ? labelObject['Please give any reason'] : 'Please give any reason') + "</div>";


                                                        $("#dialog").html(response);




                                                    } else if (rejectType == 1)
                                                    {
                                                        response = "";
                                                        $("#reasonDialog").html("");
                                                        var rejectData = $("#rejectData").val();
                                                        console.log(rejectData);
                                                        response += "<div id='rejectComboBox' class='visionRejectFormComboBox'></div>";
                                                        response += "<div id='dailog_error_id' style='display:none;color:red'>" + (labelObject['Please give any reason'] != null ? labelObject['Please give any reason'] : 'Please give any reason') + "</div>";

                                                        $("#dialog").html(response);
                                                        if (rejectData != null && rejectData != '') {
                                                            var rejectDataArray = JSON.parse(rejectData);
                                                            $("#rejectComboBox").jqxComboBox({source: rejectDataArray, searchMode: 'contains', multiSelect: true, width: 280, height: 25});
                                                        }
                                                    } else if (rejectType == 4)
                                                    {
                                                        response = "";
                                                        $("#reasonDialog").html("");
                                                        var rejectData = $("#rejectData").val();
                                                        console.log(rejectData);
                                                        response += "<div id='rejectComboBox'  class='visionRejectFormComboBox'></div>";

                                                        $("#textReason").html("");

                                                        response += "<div id='textReason'>";
                                                        response += "<textarea id='reasonId' class='visionDeleteReason'></textarea></div>";
                                                        response += "<div id='dailog_error_id' style='display:none;color:red'>" + (labelObject['Please give any reason'] != null ? labelObject['Please give any reason'] : 'Please give any reason') + "</div>";

                                                        $("#dialog").html(response);
                                                        if (rejectData != null && rejectData != '') {
                                                            var rejectDataArray = JSON.parse(rejectData);
                                                            $("#rejectComboBox").jqxComboBox({source: rejectDataArray,
                                                                searchMode: 'containsignorecase',
                                                                multiSelect: true,
                                                                autoComplete: true,
                                                                theme: 'energyblue',
                                                                openDelay: 1,
                                                                closeDelay: 1,
                                                                enableSelection: true,
                                                                width: 280, height: 25});
                                                        }


                                                    } else if (rejectType != null && rejectType != '')
                                                    {
                                                        response = "";

                                                        $("#textReason").html("");

                                                        response += "<div id='textReason'>";
                                                        response += "<textarea id='reasonId' class='visionDeleteReason'></textarea></div>";
                                                        response += "<div id='dailog_error_id' style='display:none;color:red'>" + (labelObject['Please give any reason'] != null ? labelObject['Please give any reason'] : 'Please give any reason') + "</div>";


                                                        $("#dialog").html(response);
                                                    }

                                                    $("#dialog").dialog({
                                                        title: msgTitle,
                                                        modal: true,
                                                        height: 'auto',
                                                        minWidth: 300,
                                                        maxWidth: 'auto',
                                                        fluid: true,
                                                        buttons: [{
                                                                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                                                                click: function () {
                                                                    endTabLoader();
                                                                    var retReasonText = "";
                                                                    var returnReasonData = "";
//                        var selectReason = $("#selectReason").val();
                                                                    var checkBoxdata = "";
                                                                    if (rejectType == 0)
                                                                    {
                                                                        var textBoxData = $("#reasonId").val();
                                                                        retReasonText = textBoxData;
                                                                    } else if (rejectType == 1)
                                                                    {
                                                                        var selectReason = $("#rejectComboBox").jqxComboBox('getSelectedItems');
                                                                        for (var i = 0; i < selectReason.length; i++)
                                                                        {
                                                                            checkBoxdata += selectReason[i].value;
                                                                            checkBoxdata += ",";
                                                                        }
                                                                        if (checkBoxdata != null && checkBoxdata != '')
                                                                        {
                                                                            var returnReasonData = checkBoxdata.substring(0, checkBoxdata.length - 1);
                                                                            retReasonText = returnReasonData;
                                                                        }
                                                                    } else if (rejectType == 4)
                                                                    {
                                                                        var selectReason = $("#rejectComboBox").jqxComboBox('getSelectedItems');
                                                                        for (var i = 0; i < selectReason.length; i++)
                                                                        {
                                                                            checkBoxdata += selectReason[i].value;
                                                                            checkBoxdata += ",";
                                                                        }
                                                                        if (checkBoxdata != null && checkBoxdata != '')
                                                                        {

                                                                            var returnReasonData = checkBoxdata.substring(0, checkBoxdata.length - 1);
                                                                            retReasonText = returnReasonData;
                                                                            var textBoxData = $("#reasonId").val();
                                                                            if (returnReasonData != null && returnReasonData != '')
                                                                            {
                                                                                retReasonText = returnReasonData + ", " + textBoxData;
                                                                            } else {
                                                                                retReasonText = textBoxData;
                                                                            }


                                                                        } else
                                                                        {
                                                                            var textBoxData = $("#reasonId").val();
                                                                            if (returnReasonData != null && returnReasonData != '')
                                                                            {
                                                                                retReasonText = returnReasonData + ", " + textBoxData;
                                                                            } else {
                                                                                retReasonText = textBoxData;
                                                                            }
                                                                        }
                                                                    } else if (rejectType != null && rejectType != '')
                                                                    {
                                                                        var textBoxData = $("#reasonId").val();
                                                                        retReasonText = textBoxData;
                                                                    }
                                                                    ////////////////////////////////////alert("rettext:::"+retReasonText);
                                                                    if (!retReasonText)
                                                                    {

                                                                        $("#dailog_error_id").show();
                                                                    } else if (retReasonText != null)
                                                                    {
                                                                        $("#dailog_error_id").hide();
                                                                        $(this).html("");
                                                                        $(this).dialog("close");
                                                                        $(this).dialog("destroy");
                                                                        var commentVal = $("#rejColumn").val();
                                                                        var rejColumn = "rejColumn";
                                                                        var rejectComment = "rejectComment";
                                                                        var ACCEPT_COMMENT = "ACCEPT_COMMENT";

                                                                        basicData[rejColumn] = commentVal;
                                                                        basicData[rejectComment] = retReasonText;
                                                                        basicData[ACCEPT_COMMENT] = retReasonText;
                                                                        registerCheckValidation(basicData);

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

                                                }
                                                $(this).dialog("close");

                                            }},
                                        {
                                            text: (labelObject['Cancel'] != null ? labelObject['Cancel'] : 'Cancel'),
                                            click: function () {
                                                $(this).html("");
                                                $(this).dialog("close");
                                                $(this).dialog("destroy");
                                            }
                                        }],
                                    open: function () {
                                        $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                                        $(this).closest(".ui-dialog").find(".ui-button").eq(2).addClass("dialogno");
                                        $(this).closest(".ui-dialog").addClass("visionFormDuplicateDialog");
                                        $(".visionHeaderMain").css("z-index", "999");
                                        $(".visionFooterMain").css("z-index", "999");
                                    },
                                    beforeClose: function (event, ui)
                                    {
                                        $(".visionHeaderMain").css("z-index", "99999");
                                        $(".visionFooterMain").css("z-index", "99999");
                                    },
                                    close: function () {
                                        $(this).html("");
                                        $(this).dialog("close");
                                        $(this).dialog("destroy");

                                    }


                                });
                                $("#dialog2").dialog('open');
                                var heightGrid;
                                if (dataObj['count'] >= 5)
                                {
                                    heightGrid = "250";
                                } else
                                {
                                    heightGrid = "auto";
                                }
                                $("#dialog2").dialog({
                                    height: heightGrid
                                });

                            }

                        };
                        req.error = function (e) {
                            sessionTimeout(e);
                        };

                        $.ajax(req);

                    } else {
                        registerCheckValidation(basicData);
                    }

                } else
                {
                    registerCheckValidation(basicData);
                    stopLoader();
                }



            } else {
                for (var textIdKey in resultArray) {
                    //allErrors
                    console.log(":::::::::#error_" + textIdKey);
                    //$("#dis" + resultArray[i]).html("Should not be null.");
                    $("#dis" + textIdKey).html(resultArray[textIdKey]);
                    $("#dis" + textIdKey).show();

                }
            }
        }
    });
}
function registerValidation() {

    alert("registerValidation");
    var result = [];
    var validationObj = {};
    $("#mat_creation_form_table th :input:not(:hidden)").each(function () {

        var textid = $(this).attr("id");
        //  alert("textid:::"+textid);
        var displayAttr = $("#" + textid).attr("display");

        var type = $(this).attr("type");
        var textval = $(this).val();
        if (textval != null && textval != '') {
            textval = textval.trim();
        }
        if (type != 'hidden') {
            if (textval != null && textval != '') {
                textval = textval.toUpperCase();
            }
        }
        if (type != null && type == 'checkbox') {//
            if ($("#" + textid).is(':checked')) {
                textval = "Y";
            } else {
                textval = "N";
            }
        }
        var mandatory = $("#" + textid).attr("data-mandatory");
        if (textid != null && textval == '' && (mandatory != null && mandatory == 'M')) {
//        if (textid != null && textval == '' && (textid != 'SITE_VISIT' && textid != 'LIFNR' && textid != 'RECORD_NO' && textid != 'ANID' && textid != 'SIPM_ID' && textid != 'ERP_COMMENT' && textid != 'REQUEST_COMMENT' && textid != 'ACCEPT_COMMENT' && textid != 'APPROVER_NAME' && textid != 'STEWARD')) {

            if (textid == "REMARK_TAX")
                validationObj[textid] = 'Mention Exact Nature of Service Provided by Vendor';

            else if (textid == "ERP_NO")
            {
                var recordType = $('#RECORD_TYPE').val();
                if (recordType == 'ZROH' || recordType == 'ZFRT' || recordType == 'ZHLB' || recordType == 'ZFUE' || recordType == 'ZPAC' || recordType == 'ZUNB')
                {
                    validationObj[textid] = 'Please enter SAP No';
                } else if (recordType == 'ZSPA' || recordType == 'ZCON')
                {
                    $("#" + textid).val("");
                    $("#" + textid).attr("data-mandatory", "O");
                    $("#" + textid).attr("data-inputmandatory", "O");

                }
            } else
            {
                validationObj[textid] = 'Should not be Blank';
            }
        }
        if (Object.keys(validationObj).length == 0 && textid != null && textval != '') {
            if (textval != null && textval != '') {
                textval = textval.trim();
            }

            var resultFlag = true;
            var regex = $(this).attr("data-regex");
//            alert("regex:::" + regex);
            regex = (regex == "null") ? null : regex.replace(/\\\\/g, "\\");
//            alert("regex:::" + regex);
            if (regex != null && regex != '') {
                var patt = new RegExp(regex);
                resultFlag = patt.test(textval);
            }
            if (!resultFlag) {

                validationObj[textid] = $("#" + textid).attr("data-regex-msg");
                //result.push(validationObj);
            }
        }


    });
//    alert(Object.keys(validationObj).length);
    return validationObj;
}
function registerCheckValidation(basicData) {
    showLoader();
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    var registerValidateColumn = basicData['registerValidateColumn'];
    alert("registerValidateColumn:::" + registerValidateColumn);
    if (registerValidateColumn != null && registerValidateColumn != '') {
        var conf_mesg = $("#Register").attr('data-conf');
        var success_msg = $("#Register").attr('data-success-conf');
        basicData['REG_CONF_MESG'] = conf_mesg;
        basicData['REG_SUCCESS_MSG'] = success_msg;
        $.ajax({
            type: "post",
            url: "registerValidation",
            cache: false,
            data: {'basicData': JSON.stringify(basicData)
            },
            traditional: true,
            dataType: 'html',
            success: function (response) {
                stopLoader();
                var jsonObj = JSON.parse(response);
                var message = jsonObj['message'];
                var flag = jsonObj['validateFlag'];

                if (!flag) {
                    var dialogSplitMessage = dialogSplitIconText(message, flag);
                    $("#dialogsucess").html(dialogSplitMessage);
                    var dailogProps = {};
                    dailogProps.title = (labelObject['Message'] != null ? labelObject['Message'] : 'Message');
                    dailogProps.modal = true;
                    var messagecount = message.length;
                    if (messagecount >= 600)
                    {
                        dailogProps.height = 300;
                        dailogProps.width = 400;
                    } else
                    {
                        dailogProps.height = "auto";
                        dailogProps.width = "auto";
                    }
                    dailogProps.buttons = [];
                    dailogProps.buttons.push({
                        text: (labelObject['Yes'] != null ? labelObject['Yes'] : 'Yes'),
                        click: function () {
                            $(this).html("");
                            $(this).dialog("close");
                            $(this).dialog("destroy");
                            registration();
                        }
                    }, {
                        text: (labelObject['No'] != null ? labelObject['No'] : 'No'),
                        click: function () {
                            $(this).html("");
                            $(this).dialog("close");
                            $(this).dialog("destroy");
                        }
                    });
                    dailogProps.fluid = true;
                    dailogProps.open = function () {
                        $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                        $(this).closest(".ui-dialog").find(".ui-button").eq(2).addClass("dialogno");
                        $(this).closest(".ui-dialog").addClass("visionFormDataDialogSuccess");
                        $(".visionHeaderMain").css("z-index", "999");
                        $(".visionFooterMain").css("z-index", "999");
                    };
                    dailogProps.beforeClose = function () {
                        $(".visionHeaderMain").css("z-index", "99999");
                        $(".visionFooterMain").css("z-index", "99999");
                    };
                    $("#dialogsucess").dialog(dailogProps);
                } else {
                    registration();
                }
            },
            error: function (e) {
                sessionTimeout(e);
            }
        });
    } else {
        registration();
    }
}
function registration() {
    showLoader();
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    var roleId = $("#rolehid").val();
    var basicIds = [];
    var basicData = {};
    $("#mat_creation_form_table :input").each(function () {
        var textid = $(this).attr("id");
        var displayAttr = $("#" + textid).attr("display");
        //  console.log(textid+"::::displayAttr:::"+displayAttr);
        var type = $(this).attr("type");
        var textval = $(this).val();
        if (type != 'hidden') {
            if (textval != null && textval != '') {
                textval = textval.toUpperCase();
            }
        }
        if (type != null && type == 'checkbox') {//
            if ($("#" + textid).is(':checked')) {
                textval = "Y";
            } else {
                textval = "N";
            }
        }
        ("column nameL:::" + textid);
        console.log("column Value:::" + textval);

        basicIds.push(textid);
//                  jsonOBJ.ids.push(textid.toLowerCase());
        if (textid != null && textid != 'CREATE_DATE') {

            basicData[textid] = textval;
        }
        if (textid != null && textid.lastIndexOf("HIDDEN") > -1) {
            var columnNames = $("#" + textid).val();
            var columnsArray = columnNames.split(",");

            var hiddenIds = textid.split("HIDDEN_");
            var hiddenVal = $("#" + hiddenIds[1]).val();
            for (var i = 0; i < columnsArray.length; i++) {
                basicIds.push(columnsArray[i]);
                if (hiddenVal != null) {
                    hiddenVal = hiddenVal.toUpperCase();
                }
                basicData[columnsArray[i]] = hiddenVal;
//                        basicData[columnsArray[i]] = encodeURIComponent(hiddenVal);
            }
        }
    });
    var stripValue = $("#stripValue").val();
    if (stripValue == null || stripValue != undefined || stripValue == '') {
        stripValue = "CONCEPT_ID,#;";
        $("#stripValue").val(stripValue);
    }
    var stripValueObjArray = [];
    if (stripValue != null) {
        var stripValObj = stripValue.split(";");
        for (var i = 0; i < stripValObj.length; i++)
        {
            var stripValueObj = {};
            if (stripValObj[i] != null && stripValObj[i] != '' && typeof stripValObj[i] != 'undefined') {
                if (stripValObj[i].indexOf(",") > -1) {
                    var stripVal = stripValObj[i].split(",");
                    //                                     if (stripVal[0] != null && stripVal[1] != null) {
                    stripValueObj.columnName = stripVal[0];
                    stripValueObj.value = stripVal[1];
//                                        stripValueObj.value = encodeURIComponent(stripVal[1]);
                    stripValueObjArray.push(stripValueObj);
                }

            }

        }

    }
    basicData['stripValue'] = stripValueObjArray;
    basicData['imageTable'] = $("#imageTable").val();
    basicData['imageTableColumn'] = $("#imageTableColumn").val();
    basicData['imageColumn'] = $("#imageColumn").val();
    basicData['CONCEPT_ID'] = $("#classConceptId").val();
    basicData['linkedColumns'] = $("#linkedColumns").val();
    var panelId = "";
    var currentrole = sessionStorage.getItem("currentRole");
    var currentDomain = $("#currentDomain").val();
    var vendorOnBoardFlag = $("#vendorOnBoardFlag").val();
//    if (currentDomain != null && currentDomain != '' && currentDomain != undefined
//            && currentDomain == 'PRODUCT') {
//        panelId = "MM_PANEL_MGR_PENDING_REG";
//        basicData['panelId'] = panelId;
//    } else 
    if (currentrole != null && currentrole != '' && currentrole != undefined
            && currentrole == 'MM_REQUESTOR') {
        var gridId = "MM_PENDING_REG";
        panelId = "MM_PANEL_REQ_PENDING_REG";
    } else if (currentrole != null && currentrole != '' && currentrole != undefined
            && currentrole == 'MM_APPROVER') {
        var gridId = "MM_PENDING_APP_REG";
        panelId = "MM_PANEL_APP_PENDING_REG";
    } else if (currentrole != null && currentrole != '' && currentrole != undefined
            && currentrole == 'MM_MANAGER') {
        var gridId = "MM_PENDING_MGR_REG";
        panelId = "MM_PANEL_MGR_PENDING_REG";
    }
    basicData['panelId'] = panelId;
    basicData['gridId'] = gridId;
    if (currentDomain != null && currentDomain != '' && currentDomain != undefined
            && currentDomain == 'SERVICE') {
        panelId = "SM_PANEL_MGR_PENDING_REG";
        basicData['panelId'] = panelId;
    } else if (currentDomain != null && currentDomain != '' && currentDomain != undefined
            && currentDomain == 'VENDOR') {
        panelId = 'VM_PANEL_MGR_NEW_REG';
        basicData['panelId'] = panelId;
    }
    basicData['controlType'] = 'Register';
    basicData['vendorOnBoardFlag'] = vendorOnBoardFlag;
    $.ajax({
        url: "registration",
        type: "post",
        traditional: true,
        dataType: 'html',
        cache: false,
        data: {
            basicData: JSON.stringify(basicData),
            basicIds: JSON.stringify(basicIds),
            panelId: basicData['panelId'],
            classconceptid: $("#CONCEPT_ID").val()
        },
        success: function (result) {
            //   ////alert(result);
            ajaxStop();
            stopLoader();
            $('.searchDXPCreate').show();
            $('.searchIconsList').show();
            $('.decendingOrder').show();
            $('.decendingOrder').show();
            $('.searchResultsList').show();
            $('.searchResultMaterialResults').show();
            $('#contentDXP_SEARCH_VIEW').show();
            $('#jqxScrollThumbhorizontalScrollBarDXP_SEARCH_VIEW').show();
            $('#pagerDXP_SEARCH_VIEW').show();
            secondPanelShowFlag = false;
            firstPanelShowFlag = true;
            $(".loaderwait").hide();
            if (result != null && result.indexOf("Failed") > -1 || result.indexOf("Exist") > -1) {
                var modalObj = {
                    title: 'Message',
                    body: result
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
                createModal("modalInfoDailogDiv", modalObj);
            } else {
                var jsonResponse = JSON.parse(result);
                var message = jsonResponse.MESSAGE;
                var messageFlag = jsonResponse.messageFlag;
                var recordNo = jsonResponse.RECORD_NO;
                var status = jsonResponse.O_STATUS;

                if (!messageFlag) {

                    var modalObj = {
                        title: 'Message',
                        body: message
                    };
                    createModal("dataDxpSplitterValue", modalObj);
                } else {
                    //mmFetchPropertiesTabData(recordProperties);
                    $("#RECORD_NO").val(recordNo);
                    var modalObj = {
                        title: 'Message',
                        body: message
                    };
                    var buttonArray = [
                        {
                            text: 'Ok',
                            click: function () {
//                                showLoader();
                                registerPanels(jsonResponse['basicdata'], jsonResponse['formData']);
//                                stopLoader();
                            },
                            isCloseButton: true
                        }
                    ];
                    modalObj['buttons'] = buttonArray;
                    createModal("modalInfoDailogDiv", modalObj);
                }
                $('#register').attr("disabled", false);
                $("body").css({"pointer-events": "auto"});
            }
        },

        error: function (e) {// 
            sessionTimeout(e);
        }

    });

}
function registerPanels(itemsString, data) {
    showLoader();
    $("#fourthDxpSplitter").val('');
    var itemsstring = JSON.stringify(itemsString);
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: "formData",
        cache: false,
        data: {
            items: itemsstring,
            data: JSON.stringify(data)
        },
        success: function (response) {
            stopLoader();
            var form = response['formStr'];
            $("#fourthDxpSplitter").html(form);

            $(".accordian").accordion({
                theme: 'energyblue',
                collapsible: true,
                heightStyle: "content",
                active: false,
                autoHeight: false,
                animate: 300
            });
            $('.accordian h3').bind('click', function () {
                var self = this;
                setTimeout(function () {
                    var theOffset = $(self).offset();
                    $('body,html').animate({scrollTop: theOffset.top - 40});
                }, 310); // ensure the collapse animation is done
            });
//            $("#TERM").val(data['TERM']);
            validWorkflow();

        },
        error: function (e) {
            sessionTimeout(e);
        }// Error function in Ajax
    });
}

function showClassBasedButtons(gridIdresultObj, selectedIndex, gridInitParamObj) {
    showLoader();
    var labelObj = {};
    try {
        labelObj = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {
    }
    if (gridIdresultObj['gridId'] != null && gridIdresultObj['gridId'] != '') {
        var selectedRowIndexes = $("#" + gridIdresultObj['gridId']).jqxGrid('getselectedrowindexes');
        var totalRowIndex = selectedRowIndexes.length;
        var datainformations = $('#' + gridIdresultObj['gridId']).jqxGrid('getdatainformation');
        if (datainformations != null) {
            var paginginformation = datainformations['paginginformation'];
            if (paginginformation != null) {
                var pagesize = paginginformation['pagesize'];
                if (pagesize != null && parseInt(pagesize) < totalRowIndex) {
                    totalRowIndex = parseInt(pagesize);
                }

            }
        }
    }
    if (totalRowIndex > 0) {
        $.ajax({
            datatype: "json",
            type: "POST",
            url: 'getRenderedToolBarData',
            data: {
                'items': JSON.stringify(gridIdresultObj),
            },
            traditional: true,
            cache: false,
            success: function (response) {
                stopLoader();
                var toolbar = eval('(' + response + ')');
                $("#" + gridIdresultObj['gridId']).jqxGrid({rendertoolbar: toolbar});

            }
        });
    }
}
function getExportType(gridId)
{
    var exportType = $('#export' + gridId).val();
    if (exportType == "CSV")
    {
//        $("input.exportClass").css('background', '#fff url("images/export_as_csv_icon_blue.png") no-repeat 5px center');
        $("input.exportClass").hover(
                function () {
//                    $("input.exportClass").css('background', '#0071c5 url("images/export_as_csv_icon_white.png") no-repeat 5px center', 'important');
                }, function () {
//            $("input.exportClass").css('background', '#fff url("images/export_as_csv_icon_blue.png") no-repeat 5px center', 'important');
        });
        var exportvalue = "yes";
        $("#excelExport" + gridId).attr("disabled", false);
    } else if (exportType == "Xlsx" || exportType == "Xls")
    {
//        $("input.exportClass").css('background', '#fff url("images/export_as_xlsx_icon_blue.png")  no-repeat 5px center', 'important');
        $("input.exportClass").hover(
                function () {
//                    $("input.exportClass").css('background', '#0071c5 url("images/export_as_xls_white.png")  no-repeat 5px center', 'important');
                }, function () {
//            $("input.exportClass").css('background', '#fff url("images/export_as_xlsx_icon_blue.png")  no-repeat 5px center', 'important');
        });
        var exportvalue = "yes";
        $("#excelExport" + gridId).attr("disabled", false);
    } else if ((exportType == "PDF"))
    {
//        $("input.exportClass").css('background', '#fff url("images/export_icon_blue.png")  no-repeat 5px center', 'important');
        $("input.exportClass").hover(
                function () {
//                    $("input.exportClass").css('background', '#0071c5 url("images/export_icon_white.png")  no-repeat 5px center', 'important');
                }, function () {
//            $("input.exportClass").css('background', '#fff url("images/export_icon_blue.png")  no-repeat 5px center', 'important');
        });
        $("#excelExport" + gridId).attr("disabled", false);
    } else if ((exportType == "XML"))
    {
//        $("input.exportClass").css('background', '#fff url("images/export_icon_blue.png") no-repeat 5px center');
        $("input.exportClass").hover(
                function () {
//                    $("input.exportClass").css('background', '#0071c5 url("images/export_icon_white.png") no-repeat 5px center', 'important');
                }, function () {
//            $("input.exportClass").css('background', '#fff url("images/export_icon_blue.png") no-repeat 5px center', 'important');
        });
        var exportvalue = "yes";
        $("#excelExport" + gridId).attr("disabled", false);
    } else {
        $("#excelExport" + gridId).attr("disabled", true);
    }
    ajaxStop();
}
function finalExport(gridId)
{
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    var dataArray = [];
    var selectedrowindexes = $('#' + gridId).jqxGrid('selectedrowindexes');
    if (selectedrowindexes.length != 0)
    {
        var exportType = $('#export' + gridId).val();
        if (exportType == '')
        {
            var modalObj = {
                title: 'Export Record(s)',
                body: "Please select an option to Export Process"
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
            createModal("profileShowModal", modalObj);
        } else
        {
            // need to write Selected data export
            exportProcess(gridId, 'selected');
        }
    } else
    {
        var exportRangeVal = $("#ssExportRange").val();
        var exportRangeCount = $("#ssExportCount").val();
        if (exportRangeVal != null && exportRangeVal != '' && exportRangeVal == 'Y')
        {
            if (!(exportRangeCount != null && exportRangeCount != '' && exportRangeCount != 0))

            {
                exportRangeCount = 20000;
            }
            $("#exportRangeCount").val(exportRangeCount);
            var sourceex = $('#' + gridId).jqxGrid('source');
            var totalRecords = sourceex.totalrecords;
            console.log('totalRecords:::' + totalRecords);
            var exportMesg = "<div>Select Records to Export:<select id='exportRecordsCount'>";

            if (parseInt(totalRecords) != 0 && parseInt(totalRecords) <= exportRangeCount) {
                exportMesg += "<option value='0'>1-" + totalRecords.toLocaleString() + "</option>";
            } else {

                var totalPages = parseFloat((parseInt(totalRecords) / exportRangeCount));
                var totalPagesForInt = parseInt((parseInt(totalRecords) / exportRangeCount));
                var finalPages = totalPages - totalPagesForInt;
                var j = 0;
                var i = 0;
                if (!(finalPages != null && finalPages != '' && finalPages != 0)) {
                    for (i = 0; i < totalPages; i++) {
                        exportMesg += "<option value='" + ((i * exportRangeCount) + 1) + "'>" + ((i * exportRangeCount) + 1).toLocaleString() + " - " + ((i + 1) * exportRangeCount).toLocaleString() + "</option>";
                    }
                } else {
                    for (i = 0; i < totalPages - 1; i++) {
                        exportMesg += "<option value='" + ((i * exportRangeCount) + 1) + "'>" + ((i * exportRangeCount) + 1).toLocaleString() + " - " + ((i + 1) * exportRangeCount).toLocaleString() + "</option>";
                    }
                }
                var lastRecords = totalPages - totalPagesForInt;
                if (lastRecords != null && lastRecords != '' && lastRecords != 0) {
                    exportMesg += "<option value='" + ((i * exportRangeCount) + 1) + "'>" + ((i * exportRangeCount) + 1).toLocaleString() + " - " + totalRecords.toLocaleString() + "</option>"
                }

            }
            exportMesg += "</select></div>";
            var modalObj = {
                title: 'Message',
                body: exportMesg
            };
            var buttonArray = [
                {
                    text: 'Yes',
                    click: function () {
                        $("#exportRange").val($("#exportRecordsCount").val());
                        exportProcess(gridId, 'all');
                    },
                    text: 'Close',
                    click: function () {

                    },
                    isCloseButton: true
                }
            ];
            modalObj['buttons'] = buttonArray;
            createModal("profileShowModal", modalObj);
        } else
        {
            exportProcess(gridId, 'all');
        }

    }
}
// process the export functionality
// process the export functionality
function exportProcess(gridId, selectType) {
    showLoader();
    var ssExportColFlag = $("#ssExportColFlag").val();
    $("#selectType").val(selectType);
    var exportType = $('#export' + gridId).val();
    alert(selectType + ":::exportType:::" + exportType);
    if (exportType != null) {
        if (selectType != null && selectType == 'selected') {

            if (ssExportColFlag != null && ssExportColFlag != '' && ssExportColFlag == 'Y') {
                exportSelectedColsProcess(gridId, selectType);
            } else {
                var exportJson = {};
                var count = 0;
                exportJson['headers'] = fieldsArray;
                var selectedRowsData = [];
                var selectedrowindexes = $('#' + gridId).jqxGrid('selectedrowindexes');
                if (selectedrowindexes.length != 0) {
                    var totalRowIndex = selectedrowindexes.length;
                    var datainformations = $('#' + gridId).jqxGrid('getdatainformation');
                    if (datainformations != null) {
                        var paginginformation = datainformations['paginginformation'];
                        var pagenum = paginginformation.pagenum;
                        if (paginginformation != null) {
                            var pagesize = paginginformation['pagesize'];
                            if (pagesize != null && parseInt(pagesize) < totalRowIndex) {
                                totalRowIndex = parseInt(pagesize);
                            }

                            if (pagenum != null && pagenum > 0) {
                                count = pagenum * pagesize;
                                totalRowIndex = count + pagesize;
                            }

                        }
                    }
                    for (var i = count; i < totalRowIndex; i++) {
                        selectedRowsData.push($('#' + gridId).jqxGrid('getrowdata', selectedrowindexes[i]));
                    }
                    exportJson['data'] = selectedRowsData;
                    $('#downloadDatajsonData').val(JSON.stringify(exportJson));
                    processExportRequest(gridId, exportType);
                    stopLoader();
                }// end if
            }
        } else {


            if (ssExportColFlag != null && ssExportColFlag != '' && ssExportColFlag == 'Y') {
                exportSelectedColsProcess(gridId, selectType);
            } else {
                var exportJson = {};
                exportJson['headers'] = fieldsArray;
                // exportJson['data'] = rowsData;
                $('#downloadDatajsonData').val(JSON.stringify(exportJson));
                processExportRequest(gridId, exportType);
            }
            //  var data = 

        }
    } else {
        alert(":::exportType::Not selected:");
    }


}// end of the function

function exportSelectedColsProcess(gridId, selectType) {
    var exportType = $('#export' + gridId).val();
    var ssExportColFlag = $("#ssExportColFlag").val();
    if (ssExportColFlag != null && ssExportColFlag != '' && ssExportColFlag == 'Y') {
        var fieldsData = fieldsArray;
        var inputString = "<div><table id ='gridColumns' border='1' style='width:100%;' class='gridImportColumns'><tr><td><input type='checkbox' class ='visionSelectAllCheckBox' id='selectall' value='selectall' checked></td><td style='text-align:left'>All</td> </tr>";
        if (fieldsData != null && fieldsData != '') {
            for (var i = 0; i < fieldsData.length; i++) {
                var hiddenVal = fieldsData[i].hidden;
                if (fieldsData[i].text != null && fieldsData[i].text != '' &&
                        !(fieldsData[i].datafield.startsWith("HIDDEN_") || fieldsData[i].datafield.endsWith("_HIDDEN"))) {
                    //if (!(fieldsData[i].hidden) || !(hiddenVal)){
                    if (!(hiddenVal)) {
                        inputString += "<tr><td><input type='checkbox' class ='visionSelectCheckBox' id='" + fieldsData[i].datafield + "' value='" + fieldsData[i].text + "' checked></td>"
                                + "<td style='text-align:left'>" + fieldsData[i].text + "</td> </tr>";
                    }
                }
            }
            inputString += "</table></div>";
            console.log("inputString::::" + inputString);
        }
        var modalObj = {
            title: 'Selected Grid Colums',
            body: inputString
        };
        var buttonArray = [
            {
                text: 'Ok',
                click: function () {
                    var checkBoxVals = $('.visionSelectCheckBox:checked').map(function () {
                        return this.value;
                    }).get();

                    var headersArray = [];
                    if ($('#selectallCheck').is(':checked')) {
                        headersArray = fieldsArray;
                    } else {
                        if (fieldsArray != null && fieldsArray != '') {
                            for (var i = 0; i < fieldsArray.length; i++) {
                                for (var j = 0; j < checkBoxVals.length; j++) {
                                    if (fieldsArray[i].text == checkBoxVals[j])
                                        headersArray.push(fieldsArray[i]);
                                }

                            }
                        }
                    }
                    if (selectType != null && selectType != '' && selectType == 'selected') {
                        var exportJson = {};
                        var count = 0;
                        exportJson['headers'] = headersArray;
                        var selectedRowsData = [];
                        var selectedrowindexes = $('#' + gridId).jqxGrid('selectedrowindexes');
                        if (selectedrowindexes.length != 0) {
                            var totalRowIndex = selectedrowindexes.length;
                            var datainformations = $('#' + gridId).jqxGrid('getdatainformation');
                            if (datainformations != null) {
                                var paginginformation = datainformations['paginginformation'];
                                var pagenum = paginginformation.pagenum;
                                if (paginginformation != null) {
                                    var pagesize = paginginformation['pagesize'];
                                    if (pagesize != null && parseInt(pagesize) < totalRowIndex) {
                                        totalRowIndex = parseInt(pagesize);
                                    }

                                    if (pagenum != null && pagenum > 0) {
                                        count = pagenum * pagesize;
                                        totalRowIndex = count + pagesize;
                                    }

                                }
                            }
                            for (var i = count; i < totalRowIndex; i++) {
                                selectedRowsData.push($('#' + gridId).jqxGrid('getrowdata', selectedrowindexes[i]));
                            }
                            exportJson['data'] = selectedRowsData;
                            $('#downloadDatajsonData').val(JSON.stringify(exportJson));
                            processExportRequest(gridId, exportType);
                        }// end if
                    } else {
                        var exportJson = {};
                        exportJson['headers'] = headersArray;
                        // exportJson['data'] = rowsData;
                        $('#downloadDatajsonData').val(JSON.stringify(exportJson));
                        processExportRequest(gridId, exportType);

                    }
                },
                text: 'Close',
                click: function () {

                },
                isCloseButton: true
            }
        ];
        modalObj['buttons'] = buttonArray;
        createModal("profileShowModal", modalObj);

        $("#selectall").click(function () {
            $(".visionSelectCheckBox").prop('checked', $(this).prop('checked'));

        });

    }
}        // end of function
function processExportRequest(gridId, exportType) {
    ajaxStop();
    $("#exportGridId").val(gridId);
    var jsonString = $('#downloadDatajsonData').val();
    $("#downloadData").attr("jsonexpdata", jsonString);
    if (exportType == 'Xlsx') {
        $("#downloadData").attr("action", "exportXlsxData");
        $("#downloadData").submit();

    } else if (exportType == 'CSV') {
//        $("#downloadData").attr("jsonExpData", jsonString);
        $("#downloadData").attr("action", "exportCSVData");
        $("#downloadData").submit();

    } else if (exportType == 'PDF') {
//        $("#downloadData").attr("jsonExpData", jsonString);
        $("#downloadData").attr("action", "exportPDFData");
        // $("#downloadData").submit();

    } else if (exportType == 'XML') {
//        $("#downloadData").attr("jsonExpData", jsonString);
        $("#downloadData").attr("action", "exportXMLData");
        $("#downloadData").submit();

    }
    $('#' + gridId).jqxGrid('clearselection');
}
function showbrowsepopup() {
    $("#importreccount").attr("data-isSearch", "N");
    $("#importreccount").html("");
    var importButton = '<input type="button" value="Upload" class="visionFileUpload" onclick="importParamSearch()" width="4px">'
            + '<input id="browsecols" name="importFile" class="upload" type="file" value="Import file" style="display:none;">'
            + '<input type="hidden" id="browsecolsHidden" value="">'
            + '<input type="hidden" id="dlovcolname" value="">'
            + '<input type="hidden" id="typeSelectStr" value="">';
    $("#uploadButtonDiv").html(importButton);

    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {
        labelObject = [];
    }
    var selectBoxOptions = "";
    $("#paramsearch tbody tr").each(function () {
        if ($(this).css('display') != 'none') {
            var dataColumnName = $(this).attr('data-colname');
            var i = $("#" + dataColumnName).attr('data-columnindex');
            var typeSelectStr = $("#typeSelectStr" + i).val();
            var dlovcolname = $("#typeSelectStr" + i).attr("data-dlovcolname");
            console.log(dataColumnName + "::::::" + typeSelectStr);
            selectBoxOptions += "<option value='" + $(this).attr('data-colname') + "' data-typeSelectStr='" + typeSelectStr + "' data-dlovcolname='" + dlovcolname + "'  >" + $(this).children('td').eq(0).text() + "</option>";
        }
    });
    $("#browsecolsddw").html(selectBoxOptions);
    $("#importsearchcriteria").show();
    $("#visionImportErrorMsg").show();
    $("#importsearchcriteria").dialog({
        title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
        modal: true,
        height: 220,
        minWidth: 500,
        maxWidth: 'auto',
        fluid: true,
        buttons: [
            {
                text: (labelObject['Search'] != null ? labelObject['Search'] : 'Search'),
                click: function () {
                    //  ajaxStop();

                    var selectedColumn = $("#browsecolsHidden").val();
                    var typeSelectStr = $("#typeSelectStr").val();
                    var dlovcolname = $("#dlovcolname").val();
                    var importreccount = $("#importreccount").attr("data-issearch");

                    if (importreccount == 'Y') {
                        var paramArray = [];
                        var paramObj = {};
                        paramObj.datatype = "string";
                        paramObj.column = selectedColumn;
                        paramObj.rangeflag = "N";
                        paramObj.minvalue = "";
                        paramObj.maxvalue = "";
                        paramObj.value = "";
                        paramObj.dlovcolname = dlovcolname;
                        paramObj.typeSelectStr = typeSelectStr;

                        paramObj.staged = "Y";
//                        paramObj.operator = "IN";
                        paramArray.push(paramObj);

                        var i = 0;
                        console.log("getParamSearchResults::::" + reqType);
                        var reqType = "";
                        var tableId = "paramsearch";
                        if (reqType == 'ppr') {
                            tableId = "pprsearch";
                        } else if (reqType == 'spec') {
                            tableId = "specsearch";
                        }

                        if (selectedColumn != null && selectedColumn != '' && selectedColumn != 'LOCALE') {
                            var localeCount = 0;
                            $("#" + tableId + " tbody tr").each(function () {
                                var isAllow = false;
                                var paramObj = {};
                                var colname = $(this).attr('data-colname');
                                if (colname != null && colname == 'LOCALE') {
                                    localeCount = 1;
                                    var tbmin = $("#" + reqType + "tbmin" + i).val();
                                    var tbmax = $("#" + reqType + "tbmax" + i).val();
                                    var value = $("#" + reqType + "tb" + i).val();
                                    var andOrOperator = $("#" + reqType + "andOrOperator" + i).val();
                                    var typeSelectStr = $("#" + reqType + "typeSelectStr" + i).val();
                                    var dlovcolname = $("#" + reqType + "typeSelectStr" + i).attr("data-dlovcolname");
                                    console.log("colname::" + colname + "::value::" + value + "::tbmin::" + tbmin + ":::tbmax:::" + tbmax);
                                    if (value != null && value != '') {
                                        isAllow = true;
                                    } else if (tbmin != null && tbmax != null && tbmin != '' && tbmax != '') {
                                        isAllow = true;
                                    }
                                    var type = $("#" + reqType + "tb" + i).attr("type");
                                    if (type != null && type == 'checkbox') {
                                        var textval = "N";
                                        if ($("#" + reqType + "tb" + i).is(':checked')) {
                                            isAllow = true;
                                        } else {
                                            isAllow = false;
                                        }
                                    }
                                    console.log("isAllow::::" + isAllow);
                                    if (isAllow) {
                                        paramObj.datatype = $.trim($(this).attr('data-type'));
                                        paramObj.column = $.trim($(this).attr('data-colname'));
                                        paramObj.rangeflag = $.trim($(this).attr('data-range')) == 'Y' ? 'Y' : 'N';
                                        paramObj.minvalue = $.trim($("#" + reqType + "tbmin" + i).val());
                                        paramObj.maxvalue = $.trim($("#" + reqType + "tbmax" + i).val());
                                        var type = $("#" + reqType + "tb" + i).attr("type");
                                        if (type != null && type == 'checkbox') {
                                            var textval = "N";
                                            if ($("#" + reqType + "tb" + i).is(':checked')) {
                                                textval = "Y";
                                            } else {
                                                textval = "N";
                                            }
                                            paramObj.value = textval;
                                        } else {
                                            paramObj.value = $.trim($("#" + reqType + "tb" + i).val());
                                        }
                                        paramObj.operator = $("#" + reqType + "ddw" + i).val();
                                        paramObj.symbol = $.trim($("#" + reqType + "ddw" + i).find('option:selected').text());
                                        paramObj.staged = $("#" + reqType + "ddw" + i).attr('data-staged') == "Y" ? "Y" : "N";
                                        paramObj.andOrOperator = andOrOperator;
                                        paramObj.typeSelectStr = typeSelectStr;
                                        paramObj.dlovcolname = dlovcolname;
                                        paramObj.valuestripflag = $("#" + colname).attr("data-valuestripflag");
                                        paramObj.valuetrimcharflag = $("#" + colname).attr("data-valuetrimcharflag");
                                        paramArray.push(paramObj);
                                    }
                                    ++i;
                                }

                            });
                            if (localeCount == 0) {
                                paramObj = {};
                                paramObj.datatype = "string";
                                paramObj.column = "LOCALE";
                                paramObj.rangeflag = "N";
                                paramObj.minvalue = "";
                                paramObj.maxvalue = "";
                                paramObj.value = $("#sessionLocale").val();
                                paramObj.symbol = "=";
                                paramObj.operator = "EQUALS";
                                paramObj.staged = "N";
                                paramObj.andOrOperator = "AND";
                                paramObj.typeSelectStr = "";
                                paramObj.dlovcolname = "";
                                paramObj.valuestripflag = "N";
                                paramObj.valuetrimcharflag = "N";
                                paramArray.push(paramObj);
                            }
                        }
                        searchResults("P", '', paramArray, "");
                    }
                    $("#importsearchcriteria").hide();
                    $("#importsearchcriteria").dialog('close');

                }
            },
            {
                text: (labelObject['Cancel'] != null ? labelObject['Cancel'] : 'Cancel'),
                click: function () {
                    $("#importsearchcriteria").hide();
                    $("#importsearchcriteria").dialog('close');

                }
            }
        ],

        open: function () {
            $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
            $(this).closest(".ui-dialog").find(".ui-button").eq(2).addClass("dialogno");
            $(this).closest(".ui-dialog").addClass("visionSearchImportDialog");
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
function onimportddwChange() {

    var selectedImportColumn = $("#browsecolsddw").val();
    console.log("selectedImportColumn::" + selectedImportColumn);
    if (selectedImportColumn != null && selectedImportColumn != '') {
        // importParamSearch(selectedImportColumn);
        $("#browsecolsHidden").val(selectedImportColumn);
        $("#typeSelectStr").val($("#browsecolsddw").find(':selected').attr("data-typeselectstr"));
        $("#dlovcolname").val($("#browsecolsddw").find(':selected').attr("data-dlovcolname"));
    }
}
function importParamSearch() {

    var selectedImportColumn = $("#browsecolsddw").val();
    $("#importreccount").attr("data-isSearch", "N");
    var selectedColumn = $("#browsecolsddw").val();
    $("#browsecolsHidden").val(selectedImportColumn);
    $("#typeSelectStr").val($("#browsecolsddw").find(':selected').attr("data-typeselectstr"));
    $("#dlovcolname").val($("#browsecolsddw").find(':selected').attr("data-dlovcolname"));
    console.log("importParamSearch::::" + selectedColumn);
    var params = {
        selectedColumn: selectedColumn
    };
    $("#browsecols").ajaxfileupload({
        'action': "importParamSearch",
        params: params,
        valid_extensions: ['xls', 'xlsx', 'XLS', 'XLSX', 'PNG'],
        'onComplete': function (response) {
            console.log("response:::" + JSON.stringify(response));
            if (response != null && response['message'] != '') {
                $("#importreccount").attr("data-isSearch", "Y");
                $("#importreccount").html(response['message']);
            }
            ajaxStop();
        },
        'onStart': function () {
            ajaxStart();
        }
    });
    $("#browsecols").click();
}
function showPdfData(gridId, rowIndex) {
    ajaxStart();
    showLoader();
    var rowData = $('#' + gridId).jqxGrid('getrowdata', rowIndex);
    var itemsstring = JSON.parse($('#itemsstring').val());
    rowData.imageTable = itemsstring['imageTable'];
    rowData.imageTableColumn = itemsstring['imageTableColumn'];
    rowData.imageColumn = itemsstring['imageColumn'];
    rowData.CONCEPT_ID = itemsstring['CONCEPT_ID'];
    $.ajax({
        type: "POST",
        url: 'genericDataSheet',
        data: {
            'items': JSON.stringify(rowData),
            'gridId': gridId,
            'rowIndex': rowIndex,
        },
        traditional: true,
        cache: false,
        success: function (result) {
            ajaxStop();
            stopLoader();
            var responseObj = JSON.parse(result);
            var modalObj = {
                title: 'Pdf View',
                body: responseObj['DATASHEET']
            };
            var buttonArray = [
                {
                }
            ];
            modalObj['buttons'] = buttonArray;
            createModal("showExtendPdfTableData", modalObj);
            $('#showExtendPdfTableData').find('.modal-title').replaceWith(responseObj['DATASHEETBUTTONS']);
//            $(".modal-dialog").css("width", "900px");
//            $("#searchresultsSplitter").html();
            $(".modal-dialog").addClass("modal-xl pdfBodyScroll opacity-animate3");
        }

    });
}
function showFormExtendView() {
    var extendData = $("#dxpExtendFormViewData").val();
    var modalObj = {
        title: 'Extend View',
        body: extendData
    };
    var buttonArray = [
        {
        }
    ];
    modalObj['buttons'] = buttonArray;
    createModal("showExtendPdfTableData", modalObj);
    $(".modal-dialog").addClass("modal-xl opacity-animate3");

}
function showDefaultForm() {
    $(".mainBookMark").hide();
    firstPanelShowFlag = true;
    getFirstPanelShow(event);
    $("#secondDxpSplitter").hide();
}
function showClassesForm() {
    $("#secondDxpSplitter").show();
    firstPanelShowFlag = false;
    secondPanelShowFlag = true;
    getFirstPanelShow(event);
    getSecondPanelShow(event);
    $("#thirdDxpSplitter").hide();
    $("#fourthDxpSplitter").hide();
    $('.viewClassDiv').addClass('active');
    $('.viewFormDiv').removeClass('active');
    $('.viewGridDiv').removeClass('active');
    $('.defaultDiv').removeClass('active');

}
function showGridForm() {
    secondPanelShowFlag = false;
    getSecondPanelShow(event);
    thirdPanelShowFlag = true;
    $('#fourthDxpSplitter').hide();
    showThirdPanel();
    $('.viewFormDiv').removeClass('active');
    $('.viewGridDiv').addClass('active');
    $('.viewClassDiv').removeClass('active');
    $('.defaultDiv').removeClass('active');
    setTimeout(resizable, 200);
    $("#searchGrid").css("visibility", "visible");
    gridoperations('DXP_SEARCH_VIEW', 'refresh');
}
function showFormData() {
    $('#thirdDxpSplitter').show();
    thirdPanelShowFlag = false;
    showThirdPanel();
    $('#fourthDxpSplitter').show();
    $('.viewFormDiv').addClass('active');
    $('.viewGridDiv').removeClass('active');
    $('.viewClassDiv').removeClass('active');
    $('.defaultDiv').removeClass('active');

}
function savePdf() {
    $("#pdfbodyFormId").submit();
}

$(document).ready(function () {
    $(document).on('show.bs.modal', '.modal', function () {
        const zIndex = 1040 + 10 * $('.modal:visible').length;
        $(this).css('z-index', zIndex);
        setTimeout(() => $('.modal-backdrop').not('.modal-stack').css('z-index', zIndex - 1).addClass('modal-stack'));
    });

});
function sendasEmail() {
    showLoader();
    ajaxStart();
    $.ajax({
        url: 'mailDataSheet',
        type: 'POST',
        success: function (data) {
            stopLoader();
            ajaxStop();
            labelObject = {};
            try {
                labelObject = JSON.parse($("#labelObjectHidden").val());
            } catch (e) {
            }
            var modalObj = {
                title: 'Email',
                body: data
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
            createModal("dialog", modalObj);
            $(".modal-dialog").addClass("modal-xl opacity-animate3");
        },
        error: function (e) {}
    });

}

function printDataSheet() {

    try {
        window.print();
    } catch (e) {
        console.log(e);
    }
}
function newDxpVendorClassCreation(supplierName, gridId, ssDomain, ssRole) {
    showLoader();
    $('#firstDxpSplitter').jqxSplitter({width: '100%', height: '635', orientation: 'vertical', splitBarSize: 0, panels: [{size: 30}]});
    $('#secondDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 0}]});
    $('#thirdDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 1450}]});
    $("#fourthDxpSplitter").html('');
    $('.searchFirstResultsList').hide();
    $('.searchResultsList').hide();
    $('.searchDXPCreate').hide();
    $('.decendingFirstOrder').hide();
    $("#rolehid").val(ssRole);
    var items = {};
    items['SUPPLIER_NAME'] = supplierName;
    items['gridId'] = gridId;
    items['ssDomain'] = ssDomain;
    items['ssRole'] = ssRole;
    var items = JSON.stringify(items);
    $("#itemsstring").val(items);
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: "itemRegisterForm",
        cache: false,
        data: {
            items: items,
        },
        success: function (response) {
            stopLoader();
            var form = response['formStr'];
            if (form != null && form != undefined && form != '') {
                $('#thirdDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 600}]});
                $("#thirdDxpSplitter").show();
                $("#thirdDxpSplitter").jqxSplitter('collapse');
                $("#fourthDxpSplitter").show();
                $("#fourthDxpSplitter").html(form);
                $("#treeGridDiv").show();
                $("#ACC_GRP_DESCR").val("ADOM");
                $("#COMPANY_CDE").val("1000");
                $("#PURCHASE_ORG").val("9010");
                $("#treeGridDiv").html(form);
                registerClickFunction();
            } else {
                var message = response['message'];
                var modalObj = {
                    title: 'Message',
                    body: message
                };
                var buttonArray = [
                    {
                        text: 'Ok',
                        click: function () {
                            $('#loginModel').modal('show');
                        },
                        isCloseButton: true
                    }
                ];
                modalObj['buttons'] = buttonArray;
                createModal("dataDxpSplitterValue", modalObj);
            }

        },
        error: function (e) {
            sessionTimeout(e);
        }// Error function in Ajax
    });

}
function showFormResultExtendView(event) {
//    $("#showExtendPdfTableData").
    $("#showExtendPdfTableData").modal('hide');
}
function addNewImage(conceptId, term) {
    var params = {
        conceptId: conceptId,
        term: term
    };
    $("#browsecols").ajaxfileupload({
        'action': "feedbackUpload",
        params: params,
        valid_extensions: ['png', 'jpg', 'jpeg'],
        'onComplete': function (response) {
            console.log(JSON.stringify(response));
            var message = JSON.stringify(response.message);
            message = message.replace(/^"(.+)"$/, '$1');
            var modalObj = {
                title: 'File upload status message',
                body: message
            };
            var buttonArray = [
                {
                    text: 'Ok',
                    click: function () {
                        var dataField = sessionStorage.getItem('dataField');
                        var rowBoundIndex = sessionStorage.getItem('rowBoundIndex');
                        var gridId = $('#gridId').val();
                        $('#loginModel').modal('show');
                        navigateToForm(dataField, $('#' + gridId).jqxGrid('getrowdata', rowBoundIndex), 'form', gridId, '', rowBoundIndex);
                    },
                    isCloseButton: true
                }
            ];
            modalObj['buttons'] = buttonArray;
            createModal("dataDxpSplitterValue", modalObj);
            $(".modal-dialog").addClass("modal-xs opacity-animate3");
        },
        'onStart': function () {
            ajaxStart();
        }

    });
    $("#browsecols").click();
}
function getDxpGridDataCompare(gridId, operationName) {
    showLoader();
    if (gridId == null && gridId == undefined && gridId == undefined) {
        gridId = $("#currentGridId").val();
    }
    var selectedrowindexes = $('#' + gridId).jqxGrid('selectedrowindexes');
    if (selectedrowindexes != null && selectedrowindexes != undefined
            && selectedrowindexes != undefined && selectedrowindexes.length == 2) {
        var firstRowData = $('#' + gridId).jqxGrid('getrowdata', selectedrowindexes[0]);
        var secondRowData = $('#' + gridId).jqxGrid('getrowdata', selectedrowindexes[1]);
        firstRowData['RECORD_NO2'] = secondRowData['RECORD_NO'];
        firstRowData['operationName'] = operationName;
        $.ajax({
            type: "post",
            traditional: true,
            dataType: 'json',
            url: "getDxpCompareRecors",
            cache: false,
            data: {
                firstRowData: JSON.stringify(firstRowData),
                secondRowData: JSON.stringify(secondRowData),
                selectedrowindexes: selectedrowindexes,
                gridId: "DXP_SEARCH_VIEW",
            },
            success: function (response) {
                stopLoader();
                var gridResultObj = response;
                var pageSize = 10;
                var gridPropObj = gridResultObj['gridPropObj'];
                try {
                    var pagesizeoptions = gridPropObj['pagesizeoptions'];
                    pageSize = pagesizeoptions[0];
                } catch (e) {
                }
                consolidationResolutionGrid(firstRowData, gridResultObj, 0, pageSize, 30);
            }
        })


    } else {
        var modalObj = {
            title: 'Message',
            body: "Please select two records to compare."
        };
        var buttonArray = [
            {
                text: 'Ok',
                click: function () {
                    $('#loginModel').modal('show');
                },
                isCloseButton: true
            }
        ];
        modalObj['buttons'] = buttonArray;
        createModal("dataDxpSplitterValue", modalObj);
        $(".modal-dialog").addClass("modal-xs");
    }

}
function consolidationResolutionGrid(basicData, gridResultObj, recordstartindex, pagesize, recordendindex) {

    if (gridResultObj != null) {
        if (recordstartindex != null && parseInt(recordstartindex) != 0) {
            recordstartindex = parseInt(recordstartindex) - 1;
        }
        $("#selectedCols").val(gridResultObj['totalColumnsArray']);
        var selectedCols = $("#selectedCols").val();
        var paginationHidden = $("#paginationHidden").val();
        var gridPropObj = gridResultObj['gridPropObj'];
        var data = {
            gridId: gridResultObj['gridId'],
            colsArray: JSON.stringify(gridResultObj['columnsArray']),
            totalColumnsArray: JSON.stringify(gridResultObj['totalColumnsArray']),
            gridEditFlag: gridPropObj['GRID_EDIT_FLAG'],
            gridPropertyObj: JSON.stringify(gridPropObj),
            pagesizeoptions: JSON.stringify(gridPropObj['pagesizeoptions']),
            selectionmode: gridPropObj['SELECTION_TYPE'],
            tableName: gridPropObj['GRID_REF_TABLE'],
            columns: JSON.stringify(gridResultObj['columnListObj']),
            basicData: JSON.stringify(basicData),
            selectedCols: selectedCols,
            recordstartindex: recordstartindex,
            pagesize: pagesize,
            recordendindex: recordendindex,
            currentPage: $("#currentPage").val()
        };

        $.ajax({
            type: 'POST',
            // async: false,
            url: 'consolidationResolutionGrid',
            data: data,
            traditional: true,
            dataType: 'html',
            beforeSend: function () {
                ajaxStart();
            }, loadError: function (xhr, status, error) {
                ajaxStop();
                throw new Error(error);
            }, loadComplete: function (data)
            {
                ajaxStop();
            },
            success: function (response) {
                ajaxStop();

                if (response != null) {
                    var resultObj = JSON.parse(response);
                    $("#matrixGridDivId").show();
//                    $("#matrixGridId").html(resultObj['tabString']); //tabString
                    $("#resetConsolidation").show();
                    $("#processActionButton").show();
                    $("#createSubGroup").show();
                    $("#visionSearchExportButton").show();
                    var modalObj = {
                        title: 'Records Compare Info',
                        body: resultObj['tabString']
                    };
                    var buttonArray = [
                        {

                        }
                    ];
                    modalObj['buttons'] = buttonArray;
                    createModal("showCompareGridClass", modalObj);
                    $(".modal-dialog").addClass("modal-xl dxpCompareGridRowsData");
                    $('.showCompareGridClass').css('padding-top', '0');

                }

            },
            error: function (e) {
                sessionTimeout(e);
            }

        });
    }

}
function getViewColumnForm(row) {
    var currentRowIndex = $("#currentRowIndex").val();
    var vendorGridId = 'DXP_VM_SEARCH_VIEW';
    var currentGridId = $("#currentGridId").val()
    navigateToVendorForm('SUPPLIER_NAME', $('#' + currentGridId).jqxGrid('getrowdata', currentRowIndex), 'form', vendorGridId, '', currentRowIndex);
}
function navigateToVendorForm(datafield, data, redirectType, vendorGridId, selectedTabId, selectingrowindex) {
    showLoader();
    var items = {};
    var linkedColumns = "SUPPLIER_NAME";
    if (linkedColumns != null && linkedColumns != '') {
        // var linkedColumnArray = linkedColumns.split(",");

        for (var key in data) {
            if (linkedColumns.lastIndexOf(key) > -1) {
                var value = data[key];
                //    console.log("key::::" + key + ":::value::::" + value);
                value = value.replace(/\s/gi, "_");
                value = value.replace(/[#]/g, "_");
                //  console.log("key::::" + key + ":::value::::" + value);
                items[key] = value;
            }
        }
    }
//    items.stripValue = stripValueObjArray
//    items.imageColumn = $("#" + gridId + "_imageColumn").val();
//    items.imageTable = $("#" + gridId + "_imageTable").val();
//    items.imageTableColumn = $("#" + gridId + "_imageTableColumn").val();
    items.linkedColumns = linkedColumns;
    items.gridId = vendorGridId;
    items.SUPPLIER_NAME = 'BOSCH';
    items.panelId = 'VM_PANEL_MGR_REG_ACCEPTED_BY_ERP';
    items.formId = 'VM_FRM_RECORD_ACCEPTED_ERP_MGR';
    items.tabId = selectedTabId;
    items.vendorFormView = 'Y';
//    var datainformation = $('#' + gridId).jqxGrid('getdatainformation');
//    var rowscount = datainformation.rowscount;
//    items.selectingrowindex = selectingrowindex;
//    items.rowscount = rowscount;
    var itemsstring = JSON.stringify(items);
    $("#itemsstring").val(itemsstring);
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: "formData",
        cache: false,
        data: {
            items: itemsstring,
            data: JSON.stringify(data)
        },
        success: function (response) {
            stopLoader();
//            var responseObj = JSON.parse(response);
            var modalObj = {
                title: 'Vendor Info',
                body: response['formStr']
            };
            var buttonArray = [
                {

                }
            ];
            modalObj['buttons'] = buttonArray;
            createModal("showCompareGridClass", modalObj);
            $(".modal-dialog").addClass("modal-xl dxpCompareGridRowsData");
            $('#showCompareGridClass').css("padding-top", "0");
            $('.dxpCompareGridRowsData').css("margin-top", "30");
            $(".accordian").accordion({
                theme: 'energyblue',
                collapsible: true,
                heightStyle: "content",
                active: false,
                autoHeight: false,
                animate: 300
            });
            $('.accordian h3').bind('click', function () {
                var self = this;
                setTimeout(function () {
                    var theOffset = $(self).offset();
                    $('body,html').animate({scrollTop: theOffset.top - 40});
                }, 310); // ensure the collapse animation is done
            });
        }
    });
}
function initialize() {

    var body = '<div id="map_canvas" style="width:100%; height:100%;"></div>';
    var modalObj = {
        title: 'Vendor Location',
        body: body
    };
    var buttonArray = [
        {

        }
    ];
    modalObj['buttons'] = buttonArray;
    createModal("dxpClassficationAppendClass", modalObj);
    $(".modal-dialog").addClass("modal-xl dxpCompareGridRowsData");
    $(".dxpClassficationAppendClass").css("padding-top", "0");
    $(".dxpCompareGridRowsData .modal-dialog").css("margin-top", "20");
    $(".dxpCompareGridRowsData .modal-body").addClass("mapData");
//    var lat = '77.609255';
//    var lng = '30.19232525792222';
//     showMap(lat,lng);
    var points = [
        ['name1', 77.609255, 30.19232525792222, 12, 'https://docs.jsfiddle.net/use-cases/code-snippets-hosting'],
        ['name2', 59.941412822085645, 30.263564729357767, 11, 'https://jsfiddle.net/about'],
        ['name3', 59.939177197629455, 30.273554411974955, 10, 'https://docs.jsfiddle.net/']
    ];
    var myOptions = {
        center: new google.maps.LatLng(59.91823239768787, 30.243222856188822),
        zoom: 12,
        mapTypeId: google.maps.MapTypeId.ROADMAP
    };
    var map = new google.maps.Map(document.getElementById("map_canvas"), myOptions);
    setMarkers(map, points);
}
function setMarkers(map, locations) {
    var shape = {
        coord: [1, 1, 1, 20, 18, 20, 18, 1],
        type: 'poly'
    };

    for (var i = 0; i < locations.length; i++) {
        var place = locations[i];
        var myLatLng = new google.maps.LatLng(place[1], place[2]);
        var marker = new google.maps.Marker({
            position: myLatLng,
            map: map,
            shape: shape,
            title: place[0],
            zIndex: place[3],
            url: place[4]
        });
        google.maps.event.addListener(marker, 'click', function () {
            window.location.href = this.url;
        });
    }
}
function showMap(lat, lng) {
    var url = "https://maps.google.com/?q=" + lat + "," + lng;
    window.open(url);
}
function newDxpServiceClassCreation(className, gridId, domain, role, serviceCategory,
        subCategory, uom, recordGroup, sacCode, conceptId) {
    $('#firstDxpSplitter').jqxSplitter({width: '100%', height: '635', orientation: 'vertical', splitBarSize: 0, panels: [{size: 30}]});
    $('#secondDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 0}]});
    $('#thirdDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 1450}]});
    $("#fourthDxpSplitter").html('');
    $('.searchFirstResultsList').hide();
    $('.searchResultsList').hide();
    $('.searchDXPCreate').hide();
    $('.decendingFirstOrder').hide();
    var itemsObj = {};
    var data = {};
    var linkedColumns = "CONTENT,TERM,CLASS,CLASS_TERM,DEFINITION,uid,UID,HIDDEN_GRID_ID";
    if (linkedColumns != null && linkedColumns != '') {
        for (var key in data) {
            if (linkedColumns.lastIndexOf(key) > -1) {
                var value = data[key];
                value = value.replace(/\s/gi, "_");
                value = value.replace(/[#]/g, "_");
                itemsObj[key] = value;
            }
        }
    }
    itemsObj.linkedColumns = linkedColumns;
    itemsObj.CLASS_TERM = className;
    itemsObj.gridId = gridId;
    itemsObj.DOMAIN = domain;
    itemsObj.ROLE = role;
    itemsObj.ASTYP_DESC = serviceCategory;
    itemsObj.LBNUM_DESC = subCategory;
    itemsObj.CONCEPT_ID = conceptId;
    itemsObj.UOM = uom;
    itemsObj.SAC_CODE = sacCode;
    itemsObj.RECORD_GROUP = recordGroup;
    itemsObj.panelId = "SM_PANEL_SAP_NEW_REG";
    var items = JSON.stringify(itemsObj);
    $("#itemsstring").val(items);
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: "itemRegisterForm",
        cache: false,
        data: {
            items: items,
            data: JSON.stringify(data)
        },
        success: function (response) {
            var form = response['formStr'];
            if (form != null && form != undefined && form != '') {
                $('#thirdDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 600}]});
                $("#thirdDxpSplitter").show();
                $("#thirdDxpSplitter").jqxSplitter('collapse');
                $("#fourthDxpSplitter").show();
                $("#fourthDxpSplitter").html(form);
                $("#treeGridDiv").show();
                $("#treeGridDiv").html(form);
                registerClickFunction();
            } else {
                var message = response['message'];
                var modalObj = {
                    title: 'Message',
                    body: message
                };
                var buttonArray = [
                    {
                        text: 'Ok',
                        click: function () {
                            $('#loginModel').modal('show');
                        },
                        isCloseButton: true
                    }
                ];
                modalObj['buttons'] = buttonArray;
                createModal("dataDxpSplitterValue", modalObj);
            }

        },
        error: function (e) {
            sessionTimeout(e);
        }// Error function in Ajax
    });
}
function newDxpVendorOnBoardClassCreation(currentGridId, gridId, panelId, operationName) {
    $('#firstDxpSplitter').jqxSplitter({width: '100%', height: '635', orientation: 'vertical', splitBarSize: 0, panels: [{size: 30}]});
    $('#secondDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 0}]});
    $('#thirdDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 0}]});
    $('.decendingOrder').hide();
    $('.decendingFirstOrder').hide();
    $('.searchFirstResultsList').hide();
    $('.searchDXPCreate').hide();
    $('.searchResultsList activeResult').hide();
    $('.searchResultMaterialResults').hide();
    $('.viewFormDiv').removeClass('active');
    $('.viewClassDiv').removeClass('active');
    var searchedValue = $("#searchedValue").val();
    var items = {};
    items['operationName'] = operationName;
    items['gridId'] = gridId;
    items['panelId'] = panelId;
    items['SUPPLIER_NAME'] = searchedValue;
    items['vendorOnBoardFlag'] = 'Y';
    $("#vendorOnBoardFlag").val('Y');
    var items = JSON.stringify(items);
    $("#itemsstring").val(items);
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: "itemRegisterForm",
        cache: false,
        data: {
            items: items,
        },
        success: function (response) {
            var form = response['formStr'];
            if (form != null && form != undefined && form != '') {
                $('.viewClassDiv').removeClass('active');
                $('.viewGridDiv').removeClass('active');
                $(".searchResultsList").hide();
                $('#thirdDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 0}]});
                $("#fourthDxpSplitter").show();
                $("#fourthDxpSplitter").html(form);
//                $("#treeGridDiv").show();
//                $("#ACC_GRP_DESCR").val("ADOM");
//                $("#COMPANY_CDE").val("1000");
//                $("#PURCHASE_ORG").val("9010");
//                $("#treeGridDiv").html(form);
                registerClickFunction();
            } else {
                var message = response['message'];
                var modalObj = {
                    title: 'Message',
                    body: message
                };
                var buttonArray = [
                    {
                        text: 'Ok',
                        click: function () {
                            $('#loginModel').modal('show');
                        },
                        isCloseButton: true
                    }
                ];
                modalObj['buttons'] = buttonArray;
                createModal("dataDxpSplitterValue", modalObj);
            }

        },
        error: function (e) {
            sessionTimeout(e);
        }// Error function in Ajax
    });

}
function genieEffect() {
    var direction = "";
    var extendData = $("#dxpExtendFormViewData").val();
    var elementHtml = "<div id='mainDiv' class='geniemodalContainer'><div class='genieModalHeader'>Extend View"
            + "<img src='images/Collase-Outline-Icon.svg' title='Close' id='closeGeniePopup' class='genieCloseClass'/></div>"
            + "<div class='genieModalbodyContent'>" + extendData + "</div></div>";
    $('#temp-wrapper-content').append(elementHtml);

    $('#genie-target').css({
        width: $('#temp-wrapper-content').outerWidth(),
        height: $('#temp-wrapper-content').outerHeight()
    })
    $('[genie-source]').click(function () {
//        showFormExtendView();
        if (!($("#genie-target").hasClass("genie"))) {
            $(this).htmlGenieExpand($('#genie-target'), $('#temp-wrapper-content'), [this.className]);
            direction = this.className;
        }
    });

    $('#genie-target').click(function (event) {
        var target = event.target;
        var closeButtonId = target['id'];
        if (closeButtonId == 'closeGeniePopup') {
//        var cla = $("." + direction);
            $(this).htmlGenieCollapse($("." + direction), [direction]);
            $("#genie-target").removeClass("genie");
        }
    });
}
function tabOperation(tableName, operation) {
    if (operation == 'download') {
        downloadCodeOfConduct(tableName)
    } else if (operation == 'SOWUpdate') {
        updateScopeOfWork(tableName);
    } else if (operation == 'SOWPreview') {
        previewScopeOfWork(tableName);
    } else if (operation == 'fillDown') {
        populateTabGridFillDownData(tableName);
    } else if (operation != null && operation == 'LapsTimeReport') {
        getLapsTimeReport(tableName);
    } else {
        labelObject = {};
        try {
            labelObject = JSON.parse($("#labelObjectHidden").val());
        } catch (e) {
        }
        var dataView = $("#" + tableName + "_Update").attr("data-view");
        alert("operation:::" + operation + ":::dataView::::" + dataView);
        if (dataView == null) {
            try {
                var sourceex = $('#' + tableName).jqxGrid('source');
                if (sourceex != null) {
                    dataView = "GRID-VIEW";
                } else {
                    dataView = "FORM-VIEW";
                }
            } catch (e) {
            }
        }
        alert("operation::A:" + operation + ":::dataView::::" + dataView);
        var basicData = {};
        var basicDataAudit = {};
        $("#mat_creation_form_table :input").each(function () {
            var textid = $(this).attr("id");
            var type = $(this).attr("type");
            var textval = $(this).val();
            if (type != 'hidden') {
                if (textval != null && textval != '') {
                    textval = textval.toUpperCase();
                }
            }
            if (textid != null && textid != 'CREATE_DATE') {

                basicData[textid] = textval;
            }
            if (textid != null) {
                basicDataAudit[textid] = textval;
            }
            if (textid != null && textid.lastIndexOf("HIDDEN") > -1) {
                var columnNames = $("#" + textid).val();
                var columnsArray = columnNames.split(",");

                var hiddenIds = textid.split("HIDDEN_");
                var hiddenVal = $("#" + hiddenIds[1]).val();
                for (var i = 0; i < columnsArray.length; i++) {
                    if (hiddenVal != null) {
                        hiddenVal = hiddenVal.toUpperCase();
                    }
                    basicData[columnsArray[i]] = hiddenVal;
                    basicDataAudit[columnsArray[i]] = hiddenVal;
                }
            }
        });
        if (operation == "update" || operation == 'checkingTabData')
        {
            // ajaxStart();//1
            var lasteditedfield = $('#' + tableName).attr('data-last-ed-field');
            var lasteditedrow = $('#' + tableName).attr('data-last-ed-row');
            try {
                $('#' + tableName).jqxGrid('endcelledit', lasteditedrow, lasteditedfield, false);
            } catch (e) {
            }
            hideErrors();
            var errorCount = 0;
            if (dataView == "FORM-VIEW")
            {
                errorCount = 0;
                var v_ag = $("#hiddenAccountGroup").val();

                if (v_ag != null && (v_ag == "Material & Service (Foreign)")) {
                    $("#BANKL").attr("data-mandatory", "O");
                    $("#BANKL").prop("readonly", true);
                }
                var jsonOBJ = {};
                var erpDataGridId = $("#erpDataGridId").val();
                var selectedTabOldData = tabsOldData[tableName];
                $("table#" + tableName + "_TABLE :input").each(function ()
                {
                    var id = $(this).attr('id');
                    var mand = $(this).attr("data-mandatory");
                    var label = $(this).attr("data-label");
                    mand = (mand === "M") ? "M" : "O";
                    if (label != null && label == "Bank Key(IFSC)" && (v_ag != null && v_ag == "Material & Service (Foreign)")) {
                        $("#BANKL").attr("data-regex", "");
                    }
                    var regex = $(this).attr("data-regex");
                    var returnBoolean = regexFunction(id, regex, mand, tableName, label);
                    if (returnBoolean == false)
                    {
                        errorCount++;
                        return false;
                    }
                });
                console.log("errorCount:::" + errorCount);
                if (errorCount == 0) {
                    jsonOBJ.feildIds = [];
                    jsonOBJ.feildValues = [];
                    console.log(tableName + ":::textid:::");
                    var matchedCount = 0;
                    var gridIdHiddenValue = "UPDATE";
                    $("table#" + tableName + "_TABLE :input").each(function () {
                        var textid = $(this).attr("id");
                        var type = $(this).attr("type");
                        var textval = $(this).val();
                        console.log("textid:::" + textid);
                        if (type != 'hidden') {
                            if (textval != null && textval != '') {
                                textval = textval.toUpperCase();
                            }
                        }
                        jsonOBJ.feildIds.push(textid);
                        if (type != null && type == 'checkbox') {//
                            if ($("#" + textid).is(':checked')) {
                                textval = "Y";
                            } else {
                                textval = "N";
                            }
                        }
                        jsonOBJ.feildValues.push(textval);
                        if (textid != null && textid.indexOf("AUDIT_ID") > -1)
                        {
                            basicData[textid] = textval;
                        }
                        var textOldVal = "";
                        if (selectedTabOldData != null) {
                            textOldVal = selectedTabOldData[textid];

                        }
                        console.log(textval + ":::" + textid + "::" + textOldVal);
                        if (textval != textOldVal) {
                            matchedCount++;
                        }
                        var tableNameHidden = tableName + "_HIDDEN";
                        if (textid == tableNameHidden) {
                            gridIdHiddenValue = $("#" + textid).val();
                        }
                    });
                    console.log("jsonOBJ:::" + JSON.stringify(jsonOBJ));

                    if (gridIdHiddenValue == 'INSERT' && matchedCount == 0) {
                        matchedCount = 1;
                    }
                    if (matchedCount > 0 || operation == 'checkingTabData') {
                        jsonOBJ.basicData = basicData;
                        console.log("jsonOBJ.feildIds:::" + JSON.stringify(jsonOBJ.feildIds));
                        console.log("jsonOBJ.feildValues:::" + JSON.stringify(jsonOBJ.feildValues));
                        var jsonArray = [];
                        jsonArray.push(jsonOBJ);
                        UpdateOrDelete(JSON.stringify(jsonArray), dataView, tableName, operation);
                    } else {
                        ajaxStop();//8
                        var results = "No Changes to Save";
                        results = (labelObject[results] != null ? labelObject[results] : results);
                        var dialogSplitMessage = dialogSplitIconText(results, "Y");
                        var modalObj = {
                            title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
                            body: labelObject[dialogSplitMessage] != null ? labelObject[dialogSplitMessage] : dialogSplitMessage,
                        };
                        var buttonArray = [
                            {
                                text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                                click: function () {
                                },
                                isCloseButton: true
                            }
                        ];
                        modalObj['buttons'] = buttonArray;
                        createModal("dataDxpSplitterValue", modalObj);
                        $(".modal-dialog").addClass("modal-xs opacity-animate3");
                    }
                }
            } else if (dataView == "TABLE-VIEW") {

                selectedDataArray = gridOperation(operation, tableName);
                if (Array.isArray(selectedDataArray) && selectedDataArray.length == 0) {
                    ajaxStop();//9
                    var results = "No Changes to Save";
                    results = (labelObject[results] != null ? labelObject[results] : results);
                    var dialogSplitMessage = dialogSplitIconText(results, "Y");
                    var modalObj = {
                        title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
                        body: labelObject[dialogSplitMessage] != null ? labelObject[dialogSplitMessage] : dialogSplitMessage,
                    };
                    var buttonArray = [
                        {
                            text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                            click: function () {
                                fetchTabData(tableName);
                                $(tableName).jqxGrid('clearselection');
                            },
                            isCloseButton: true
                        }
                    ];
                    modalObj['buttons'] = buttonArray;
                    createModal("dataDxpSplitterValue", modalObj);
                    $(".modal-dialog").addClass("modal-xs");
                } else if (!(Array.isArray(selectedDataArray))
                        && selectedDataArray.errorMesssage != null
                        && selectedDataArray.errorMesssage != '') {
                    var errorMessageTable = "<table style='width: 100%;' border='1'>"
                            + "<tr><th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Property Name</th>"
                            + "<th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Error Message</th>";
                    errorMessageTable += selectedDataArray.errorMesssage;
                    errorMessageTable += '</table>';

                    labelObject = {};
                    try {
                        labelObject = JSON.parse($("#labelObjectHidden").val());
                    } catch (e) {
                    }
                    console.log(errorMessageTable + "::::::::::::::::::");
                    if (errorMessageTable !== "" && errorMessageTable !== null)
                    {
                        errorMessageTable = (labelObject[errorMessageTable] != null ? labelObject[errorMessageTable] : errorMessageTable);
                        var dialogSplitMessage = dialogSplitIconText(errorMessageTable, "false");
                        var modalObj = {
                            title: labelObject['Error'] != null ? labelObject['Error'] : 'Error',
                            body: labelObject[errorMessageTable] != null ? labelObject[errorMessageTable] : errorMessageTable,
                        };
                        var buttonArray = [
                            {
                                text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                                click: function () {
                                },
                                isCloseButton: true
                            }
                        ];
                        modalObj['buttons'] = buttonArray;
                        createModal("dataDxpSplitterValue", modalObj);
                        $(".modal-dialog").addClass("modal-xs opacity-animate3");
                    }

                } else {
                    endoperation(selectedDataArray, tableName, dataView, operation, basicData);
                }
            } else if (dataView == "GRID-VIEW") {
                if (operation == 'checkingTabData') {
                    selectedDataArray = $('#' + tableName).jqxGrid('getdisplayrows');
                } else {
                    selectedDataArray = gridOperation(operation, tableName);
                }
                console.log("selectedDataArray::::" + selectedDataArray.length);
                console.log("selectedDataArray::758::" + JSON.stringify(selectedDataArray));
                alert(selectedDataArray.length);
                //console.log("selectedDataArray size:::::" + JSON.stringify(selectedDataArray));
                if (selectedDataArray == 0) {
                    ajaxStop();//10
                    var results = "No Changes to Save";
                    results = (labelObject[results] != null ? labelObject[results] : results);
                    var dialogSplitMessage = dialogSplitIconText(results, "Y");
                    var modalObj = {
                        title: labelObject['Error'] != null ? labelObject['Error'] : 'Error',
                        body: labelObject[dialogSplitMessage] != null ? labelObject[dialogSplitMessage] : dialogSplitMessage,
                    };
                    var buttonArray = [
                        {
                            text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                            click: function () {
                                $(tableName).jqxGrid('clearselection');
                            },
                            isCloseButton: true
                        }
                    ];
                    modalObj['buttons'] = buttonArray;
                    createModal("dataDxpSplitterValue", modalObj);
                    $(".modal-dialog").addClass("modal-xs opacity-animate3");

                } else {
                    endoperation(selectedDataArray, tableName, dataView, operation, basicData);
                }
            }//if 
        } else if (operation == "calculateStock")
        {
            var lasteditedfield = $('#' + tableName).attr('data-last-ed-field');
            var lasteditedrow = $('#' + tableName).attr('data-last-ed-row');
            try {
                $('#' + tableName).jqxGrid('endcelledit', lasteditedrow, lasteditedfield, false);
            } catch (e) {
            }
            hideErrors();
            var errorCount = 0;
            if (dataView == "FORM-VIEW")
            {
                errorCount = 0;
                var v_ag = $("#hiddenAccountGroup").val();

                if (v_ag != null && (v_ag == "Material & Service (Foreign)")) {
                    $("#BANKL").attr("data-mandatory", "O");
//                $("#BANKL").prop("disabled", "disabled");
                    $("#BANKL").prop("readonly", true);
                }
                var jsonOBJ = {};
                var erpDataGridId = $("#erpDataGridId").val();
//            
                var selectedTabOldData = tabsOldData[tableName];
                $("table#" + tableName + "_TABLE :input").each(function ()
                {

                    var id = $(this).attr('id');
                    var mand = $(this).attr("data-mandatory");
                    var label = $(this).attr("data-label");
                    mand = (mand === "M") ? "M" : "O";
                    var regex = $(this).attr("data-regex");
                    if (id != null && id != '' && id != 'undefined')
                    {
                        if (id == 'CRITICALITY' || id == 'SETSIZE' || id == 'INSTALLED_QUANTITY' || id == 'PREDICTABILITY' || id == 'MTBF')
                        {
                            var returnBoolean = regexFunction(id, regex, mand, tableName, label);
                            if (returnBoolean == false)
                            {
                                errorCount++;
                                return false;
                            }
                        }
                    }
                });
                console.log("errorCount:::" + errorCount);
                if (errorCount == 0) {
                    jsonOBJ.feildIds = [];
                    jsonOBJ.feildValues = [];
                    console.log(tableName + ":::textid:::");
                    var matchedCount = 0;
                    var gridIdHiddenValue = "UPDATE";
                    $("table#" + tableName + "_TABLE :input").each(function () {
                        var textid = $(this).attr("id");
                        var type = $(this).attr("type");
                        var textval = $(this).val();
                        console.log("textid:::" + textid);
                        if (type != 'hidden') {
                            if (textval != null && textval != '') {
                                textval = textval.toUpperCase();
                            }
                        }
                        jsonOBJ.feildIds.push(textid);
                        if (type != null && type == 'checkbox') {//
                            if ($("#" + textid).is(':checked')) {
                                textval = "Y";
                            } else {
                                textval = "N";
                            }
                        }
                        jsonOBJ.feildValues.push(textval);
                        if (textid != null && textid.indexOf("AUDIT_ID"))
                        {
                            basicData[textid] = textval;
                        }
                        var textOldVal = "";
                        if (selectedTabOldData != null) {
                            textOldVal = selectedTabOldData[textid];

                        }
                        console.log(textval + ":::" + textid + "::" + textOldVal);
                        if (textval != textOldVal) {
                            matchedCount++;
                        }
                        var tableNameHidden = tableName + "_HIDDEN";
                        if (textid == tableNameHidden) {
                            gridIdHiddenValue = $("#" + textid).val();
                        }
                    });
                    console.log("jsonOBJ:::" + JSON.stringify(jsonOBJ));

                    if (gridIdHiddenValue == 'INSERT' && matchedCount == 0) {
                        matchedCount = 1;
                    }
                    jsonOBJ.basicData = basicData;
                    console.log("jsonOBJ.feildIds:::" + JSON.stringify(jsonOBJ.feildIds));
                    console.log("jsonOBJ.feildValues:::" + JSON.stringify(jsonOBJ.feildValues));
                    var jsonArray = [];
                    jsonArray.push(jsonOBJ);
                    UpdateOrDelete(JSON.stringify(jsonArray), dataView, tableName, operation);
                }
            } else if (dataView == "TABLE-VIEW") {
                selectedDataArray = gridOperation(operation, tableName);
                alert(selectedDataArray.length);
                if (selectedDataArray.length == 0) {
                    ajaxStop();//9
                    var results = "No Changes to Save";
                    results = (labelObject[results] != null ? labelObject[results] : results);
                    var dialogSplitMessage = dialogSplitIconText(results, "Y");
                    var modalObj = {
                        title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
                        body: labelObject[dialogSplitMessage] != null ? labelObject[dialogSplitMessage] : dialogSplitMessage,
                    };
                    var buttonArray = [
                        {
                            text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                            click: function () {
                                fetchTabData(tableName);
                                $(tableName).jqxGrid('clearselection');
                            },
                            isCloseButton: true
                        }
                    ];
                    modalObj['buttons'] = buttonArray;
                    createModal("dataDxpSplitterValue", modalObj);
                    $(".modal-dialog").addClass("modal-xs opacity-animate3");
                } else {
                    endoperation(selectedDataArray, tableName, dataView, operation, basicData);
                }
            } else if (dataView == "GRID-VIEW") {

                selectedDataArray = gridOperation(operation, tableName);
                console.log("selectedDataArray::::" + selectedDataArray.length);
                console.log("selectedDataArray::758::" + JSON.stringify(selectedDataArray));
                if (selectedDataArray == 0) {
                    ajaxStop();//10
                    var results = "No Changes to Save";
                    results = (labelObject[results] != null ? labelObject[results] : results);
                    var dialogSplitMessage = dialogSplitIconText(results, "Y");
                    var modalObj = {
                        title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
                        body: labelObject[dialogSplitMessage] != null ? labelObject[dialogSplitMessage] : dialogSplitMessage,
                    };
                    var buttonArray = [
                        {
                            text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                            click: function () {
                                $(tableName).jqxGrid('clearselection');
                            },
                            isCloseButton: true
                        }
                    ];
                    modalObj['buttons'] = buttonArray;
                    createModal("dataDxpSplitterValue", modalObj);
                    $(".modal-dialog").addClass("modal-xs opacity-animate3");
                } else {

                    endoperation(selectedDataArray, tableName, dataView, operation, basicData);
                }
            }
        } else if (operation == "add")
        {
            var lasteditedfield = $('#' + tableName).attr('data-last-ed-field');
            var lasteditedrow = $('#' + tableName).attr('data-last-ed-row');
            try {
                $('#' + tableName).jqxGrid('endcelledit', lasteditedrow, lasteditedfield, false);
            } catch (e) {

            }
            $("table#" + tableName + "_Operators").show();
            $("table#" + tableName + "_TABLE :input").each(function ()
            {
                $(this).val('');
                $(this).prop("checked", false);
            });
            $("table#" + tableName + "_TABLE select").each(function ()
            {
                $(this).prop('selectedIndex', 0);
                var id = $(this).attr('id');
                var onChange = $("#" + id).attr('onchange');
                if (onChange != null && onChange != "undefined") {
                    eval(onChange);
                }
            });

            $("#" + tableName + "_HIDDEN").val("INSERT");
            if (dataView == "FORM-VIEW")
            {
                $("#" + tableName + "_TABLE").show();
                $("#" + tableName).show();
                dataView = "FORM-VIEW";
                $("#" + tableName + "_Update").attr("data-view", "FORM-VIEW");
                $("#" + tableName + "_Grid_View").show();
                $("#" + tableName + "_Delete").show();
                $("#" + tableName + "_Update").show();
                var formDefaultValues = $("#" + tableName + "_defaultValues").val();
                if (formDefaultValues != null && formDefaultValues != '') {

                } else {
                    formDefaultValues = $("#defaultValues").val();
                }
//            alert("formDefaultValues:::form::" + formDefaultValues);
                var currentValue;
                var currentColumn;
                if (formDefaultValues != null) {
                    var formDefaultValuesArray = formDefaultValues.split(",");
                    for (var i = 0; i < formDefaultValuesArray.length; i++) {
                        currentValue = formDefaultValuesArray[i];
                        currentColumn = currentValue.split(":");
                        if (currentColumn[0] != null && currentColumn[0] != '/') {
                            var type = $("#" + currentColumn[0]).attr("type");
                            if (type == 'checkbox') {
                                if (currentColumn[1] == 'Y') {
                                    $("#" + currentColumn[0]).prop("checked", true);
                                } else {
                                    $("#" + currentColumn[0]).prop("checked", false);
                                }
                            } else {
                                $("#" + currentColumn[0]).val(currentColumn[1]);
                            }

                        }
                    }
                }
                priceConroleOnAddOperation();
                var reciepientType = "OT";
                var panCharTop;
                panCharTop = $("#PAN_NUMBER").val();
                if (panCharTop && (panCharTop.charAt(3) == "C" || panCharTop.charAt(3) == "c")) {
                    reciepientType = "CO";
                }
                $("#QSREC").val(reciepientType);
                var tabsChangeOldObject = {};

                $("#" + tableName + "_TABLE" + " :input").each(function ()
                {
                    var textid = $(this).attr("id");
                    var type = $(this).attr("type");
                    var textval = $(this).val();
                    if (type != 'hidden') {
                        if (textval != null && textval != '') {
                            textval = textval.toUpperCase();
                        }
                    }
                    if (type != null && type == 'checkbox') {//
                        if ($("#" + textid).is(':checked')) {
                            textval = "Y";
                        } else {
                            textval = "N";
                        }
                    }
//                  jsonOBJ.ids.push(textid.toLowerCase());
                    if (textid != null && textid != 'CREATE_DATE') {
                        tabsChangeOldObject[textid] = textval;
                    }
                });
                if (tabsChangeOldObject != null) {
                    tabsOldData[tableName] = tabsChangeOldObject;
                }
                setTimeout(changeflagFuction, 300);
                console.log(" add clicked change flag " + changeflag);
            }
            if (dataView == "GRID-VIEW")
            {
                $("#" + tableName + "_TABLE").hide();
                $("#" + tableName).show();
                dataView = "GRID-VIEW";
                $("#" + tableName + "_Update").attr("data-view", "GRID-VIEW");
                $("#" + tableName + "_Grid_View").show();
//            $("#" + tableName + "_Delete").hide();
                $("#" + tableName + "_Update").show();
                insertGridRow(tableName, dataView, tableName);
            }

        } else if (operation == "delete")
        {
            var wrappedData = [];
            var jsonOBJ = {};
            if (dataView == "FORM-VIEW")
            {
                ajaxStop();//11
                jsonOBJ.feildIds = [];
                jsonOBJ.feildValues = [];
                $("#" + tableName + "_TABLE :input").each(function () {
                    var textid = $(this).attr("id");
                    var type = $(this).attr("type");
                    var textval = $(this).val();
                    if (type != 'hidden') {
                        if (textval != null && textval != '') {
                            textval = textval.toUpperCase();
                        }
                    }
                    if (textid.indexOf("AUDIT_ID"))
                    {
                        basicData[textid] = textval;
                    }
                    jsonOBJ.feildIds.push(textid);
                    jsonOBJ.feildValues.push(textval);
                });
                jsonOBJ.basicData = basicData;
                ajaxStop();//12
                var results = "Are you sure you want to Delete this Record?";
                results = (labelObject[results] != null ? labelObject[results] : results);
                var dialogSplitMessage = dialogSplitIconText(results, "Y");
                var modalObj = {
                    title: labelObject['Confirmation'] != null ? labelObject['Confirmation'] : 'Confirmation',
                    body: labelObject[dialogSplitMessage] != null ? labelObject[dialogSplitMessage] : dialogSplitMessage,
                };
                var buttonArray = [
                    {
                        text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                        click: function () {
                            var jsonArray = [];
                            jsonArray.push(jsonOBJ);
                            UpdateOrDelete(JSON.stringify(jsonArray), dataView, tableName, operation);
                        },
                        isCloseButton: true
                    }
                ];
                modalObj['buttons'] = buttonArray;
                createModal("dataDxpSplitterValue", modalObj);
                $(".modal-dialog").addClass("modal-xs opacity-animate3");
            } else {
                var rowsSelected = [];
//            var rowsSelected = getGridSelectedRowsData(tableName);
                var indexes = $("#" + tableName).jqxGrid('selectedrowindexes');
                if (indexes.length > 0) {
                    var totalRowIndex = indexes.length;
                    var datainformations = $('#' + tableName).jqxGrid('getdatainformation');
//                    var datainformations = $('#' + gridId).jqxGrid('getdatainformation');
                    if (datainformations != null) {
                        var paginginformation = datainformations['paginginformation'];
                        if (paginginformation != null) {
                            var pagesize = paginginformation['pagesize'];
                            if (pagesize != null && parseInt(pagesize) < totalRowIndex) {
                                totalRowIndex = parseInt(pagesize);
                            }
                        }
                    }
                    for (var i = 0; i < totalRowIndex; i++) {
                        var data = $("#" + tableName).jqxGrid('getrowdata', indexes[i]);
                        rowsSelected.push(data);
                    }
                }
                alert("63fhdjh" + rowsSelected);
                if (rowsSelected == null || rowsSelected.length == 0) {
                    ajaxStop();//13
                    console.log("rowsSelected::::");
                    var results = "No Record(s) to Delete";
                    results = (labelObject[results] != null ? labelObject[results] : results);
                    var dialogSplitMessage = dialogSplitIconText(results, "Y");
                    var modalObj = {
                        title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
                        body: labelObject[dialogSplitMessage] != null ? labelObject[dialogSplitMessage] : dialogSplitMessage,
                    };
                    var buttonArray = [
                        {
                            text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                            click: function () {
                                $("#" + tableName).jqxGrid('updatebounddata', 'cells');
                                $("#" + tableName).jqxGrid('clearselection');
                            },
                            isCloseButton: true
                        }
                    ];
                    modalObj['buttons'] = buttonArray;
                    createModal("dataDxpSplitterValue", modalObj);
                    $(".modal-dialog").addClass("modal-xs opacity-animate3");
                } else {
                    ajaxStop();//14
                    var results = "Are you sure you want to Delete this Record?";
                    results = (labelObject[results] != null ? labelObject[results] : results);
                    var dialogSplitMessage = dialogSplitIconText(results, "Y");
                    var modalObj = {
                        title: labelObject['Confirmation'] != null ? labelObject['Confirmation'] : 'Confirmation',
                        body: labelObject[dialogSplitMessage] != null ? labelObject[dialogSplitMessage] : dialogSplitMessage,
                    };
                    var buttonArray = [
                        {
                            text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                            click: function () {
                                var dataArray = [];
                                console.log("rowsSelected::::" + JSON.stringify(rowsSelected));
                                for (var i = 0; i < rowsSelected.length; i++) {
                                    var jsonData = {};
                                    obj = rowsSelected[i];
                                    jsonData.feildIds = [];
                                    jsonData.feildValues = [];

                                    for (var key in obj) {
                                        var value = obj[key];
                                        console.log("key:::::" + key + "::::::value::::" + value);
                                        jsonData.feildIds.push(key);
                                        jsonData.feildValues.push(value);
                                    }
                                    jsonData.basicData = basicData;
                                    dataArray.push(jsonData);
                                }
                                console.log("::::dataArray::::" + JSON.stringify(dataArray));
                                UpdateOrDelete(JSON.stringify(dataArray), dataView, tableName, operation);
                            },
                            isCloseButton: true
                        }
                    ];
                    modalObj['buttons'] = buttonArray;
                    createModal("dataDxpSplitterValue", modalObj);
                    $(".modal-dialog").addClass("modal-xs opacity-animate3");
                }
            }
        } else if (operation == "refresh") {
            ajaxStart();//4
            setTimeout(changeflagFuction, 300);
            if (dataView == "FORM-VIEW") {
                if (tableName != null && tableName.indexOf("ERP") > -1) {
                    fetchErpTab(tableName, '');
                } else {
                    fetchTabData(tableName);
                }
            }
            if (dataView == "TABLE-VIEW") {
                var opName = $("#" + tableName).val();
                if (opName == 'INSERT') {
                    selectedDataArray = gridOperation("insert", tableName);
                } else {
                    selectedDataArray = gridOperation("update", tableName);
                }
                if (selectedDataArray.length != 0) {
                    ajaxStop();//15
                    var results = "Do you want to save your changes?";
                    results = (labelObject[results] != null ? labelObject[results] : results);
                    var dialogSplitMessage = dialogSplitIconText(results, "Y");
                    var modalObj = {
                        title: labelObject['Confirmation'] != null ? labelObject['Confirmation'] : 'Confirmation',
                        body: labelObject[dialogSplitMessage] != null ? labelObject[dialogSplitMessage] : dialogSplitMessage,
                    };
                    var buttonArray = [
                        {
                            text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                            click: function () {
                                endoperation(selectedDataArray, tableName, dataView, operation, basicData);
                            },
                            text: (labelObject['Cancel'] != null ? labelObject['Cancel'] : 'Cancel'),
                            click: function () {
                                if (tableName != null && tableName.indexOf("ERP") > -1) {
                                    fetchErpTab(tableName, '');
                                } else {
                                    fetchTabData(tableName);
                                    $(tableName).jqxGrid('clearselection');
                                }
                            },
                            isCloseButton: true
                        }
                    ];
                    modalObj['buttons'] = buttonArray;
                    createModal("dataDxpSplitterValue", modalObj);
                    $(".modal-dialog").addClass("modal-xs opacity-animate3");
                    ajaxStop();//16
                } else {
//                 $("#grid").jqxGrid('updatebounddata', 'cells');
                    $("#" + tableName).jqxGrid('updatebounddata', 'cells');
                    $("#" + tableName).jqxGrid('clearselection');
//                    $("#" + tableName).jqxGrid('clearfilters');
                    ajaxStop();//17
                }
            } else if (dataView == "GRID-VIEW") {
                var editable = $("#" + tableName).jqxGrid('editable');
                if (editable) {
                    var opName = $("#" + tableName).val();
                    if (opName == 'INSERT') {
                        selectedDataArray = gridOperation("insert", tableName);
                    } else {
                        selectedDataArray = gridOperation("update", tableName);
                    }
                    if (selectedDataArray.length != 0) {
                        ajaxStop();//18
                        var results = "Do you want to save your changes?";
                        results = (labelObject[results] != null ? labelObject[results] : results);
                        var dialogSplitMessage = dialogSplitIconText(results, "Y");
                        var modalObj = {
                            title: labelObject['Confirmation'] != null ? labelObject['Confirmation'] : 'Confirmation',
                            body: labelObject[dialogSplitMessage] != null ? labelObject[dialogSplitMessage] : dialogSplitMessage,
                        };
                        var buttonArray = [
                            {
                                text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                                click: function () {
                                    endoperation(selectedDataArray, tableName, dataView, operation, basicData);
                                },
                                text: (labelObject['Cancel'] != null ? labelObject['Cancel'] : 'Cancel'),
                                click: function () {
                                    if (tableName != null && tableName.indexOf("ERP") > -1) {
                                        fetchErpTab(tableName, '');
                                    } else {
                                        if (tableName.indexOf("DESCRIPTIONS") > -1) {
                                            refreshDecriptionTab(tableName);
                                        } else {
                                            fetchTabData(tableName);
                                            $(tableName).jqxGrid('clearselection');
                                        }
                                    }
                                },
                                isCloseButton: true
                            }
                        ];
                        modalObj['buttons'] = buttonArray;
                        createModal("dataDxpSplitterValue", modalObj);
                        $(".modal-dialog").addClass("modal-xs opacity-animate3");
                    } else {
                        $("#" + tableName).jqxGrid('updatebounddata', 'cells');
                        $("#" + tableName).jqxGrid('clearselection');
                        ajaxStop();//19
                    }
                } else {
                    if (tableName.indexOf("DESCRIPTIONS") > -1) {
                        refreshDecriptionTab(tableName);
                    } else {
                        fetchTabData(tableName);
                    }
                }
            }
        } else if (operation == "Grid_View")
        {
            fetchTabData(tableName);
        } else if (operation == "audit") {
            var clauseColumns = $("#" + tableName + "_audit").attr("clauseColumns");
            navigateToAuditView(tableName, basicDataAudit, clauseColumns);
        }
    }
}
function hideErrors() {
    $(".allErrors").html("");
    $(".allErrors").hide();
}
function regexFunction(ele, regex, mandatory, basket, label) {

    var ele = ele;
    var splitid;
    var str;
    var tabId = $("#" + ele).attr("data-viewID");
    var inputtype = $("#" + ele).attr("data-inputtype");
    var inputHidden = $("#" + ele).attr("data-column");
    var mandatory = $("#" + ele).attr("data-mandatory");
    var regex = $("#" + ele).attr("data-regex");
    var label = $("#" + ele).attr("data-label");
    var datatype = $("#" + ele).attr("data-type");
    var type = $("#" + ele).attr("type");
    var regex = (regex == "null") ? null : regex;
    if (datatype == "C") {
        if ($("#" + ele).is(":checked")) {
            str = true;
        } else {
            str = false;
        }
    } else {
        str = $("#" + ele).val();

        if (str != null)
        {
            str = str.replace(/(^\s*)/gi, "");
            str = str.replace(/[ ]{2,}/gi, " ");
            str = str.trim();
            $("#" + ele).val(str);
        }
    }
    var id = '#dis' + ele;
    if (true) {
        var splitcount = 0;
        var valCount;
        var nrCount;
        var hiddenValue = "";
        if (inputtype == 'MT' && mandatory == 'O') {
            valCount = 0;
            nrCount = 0;
            var mtDependency = $("#" + ele).attr("data-mtdependency");
            var mtApplicable = "";
            var mtRequired = "";
            if (mtDependency == "D") {
                splitid = $("#" + ele).attr("data-column");
                splitcount = $("#" + ele).attr("splitcount");
                for (var i = 1; i <= splitcount; i++) {
                    mtRequired = $("#" + splitid + "" + i).attr("data-mtrequired");
                    mtApplicable = $("#" + ele).attr("data-mtapplicable");
                    if (mtRequired == 'NR') {
//                        ////////////////alert("NR");
                        ++nrCount;
                        ++valCount;
                    }
//                    //////////alert($("#" + splitid[i]).val());
                    if ($("#" + splitid + "" + i).val() != "" && mtRequired == 'R') {
//                        ////////////////alert("R");
                        ++valCount;
                    }
                    if (mtApplicable == "NA") {
                        var j = i - 1;
                        var splitidPrev = splitid + "" + j;
                        if (j > 0) {
                            if (i > 1 && $("#" + splitidPrev).val().toUpperCase() == "NA") {
                                valCount = splitcount;
                                $("#" + splitid + "" + i).val('');
//                                $("#" + splitid + "" + i).attr('disabled', true);
                                $("#" + splitid + "" + i).attr('readonly', true);
                                $(".allErrors").hide();
                            }
                            if (i > 1 && $("#" + splitidPrev).val().toUpperCase() != "NA") {
                                $("#" + splitid + "" + i).attr('readonly', false);
                            }
                        }
                    }
                    hiddenValue += $("#" + splitid + "" + i).val();
                    if (i < splitcount) {
                        hiddenValue += "-";
                    }
                }
                $("#" + inputHidden).val(hiddenValue);
            }
            if (mtDependency == "ND") {
                splitid = $("#" + ele).attr("data-column");
                splitcount = $("#" + ele).attr("splitcount");
                for (var i = 1; i <= splitcount; i++) {
                    hiddenValue += $("#" + splitid + "" + i).val();
                    if (i < splitcount) {
                        hiddenValue += "-";
                    }
                }
                $("#" + inputHidden).val(hiddenValue);
            }
            if (nrCount != valCount && valCount != splitcount) {
                var msg = (labelObject['Enter Valid' + label] != null ? labelObject['Enter Valid' + label] : 'Enter Valid' + label);
                errorMessage(id, msg);
                return false;
            }
        }
        if (inputtype == 'MT' && mandatory == 'M') {
            valCount = 0;
            nrCount = 0;
            var mtDependency = $("#" + ele).attr("data-mtdependency");
            var mtApplicable = "";
            var mtRequired = "";
            if (mtDependency == "D") {
                splitid = $("#" + ele).attr("data-column");
                splitcount = $("#" + ele).attr("splitcount");
                for (var i = 1; i <= splitcount; i++) {
                    mtRequired = $("#" + splitid + "" + i).attr("data-mtrequired");
                    mtApplicable = $("#" + ele).attr("data-mtapplicable");
                    if (mtRequired == 'NR') {
                        ++nrCount;
                        ++valCount;
                    }
                    if ($("#" + splitid + "" + i).val() != "" && mtRequired == 'R') {
                        ++valCount;
                    }
                    if (mtApplicable == "NA") {
                        if (i > 1) {
                            var j = i - 1;
                            var splitidPrev = splitid + "" + j;
                            if ($("#" + splitidPrev).val().toUpperCase() == "NA") {
                                valCount = splitcount;
                                $("#" + splitid + "" + i).val('');
                                $("#" + splitid + "" + i).attr('readonly', true);
                                $(".allErrors").hide();
                                $("#" + splitid + "" + i).attr("data-mandatory", 'O');
                            }
                            if ($("#" + splitidPrev).val().toUpperCase() != "NA") {
                                $("#" + splitid + "" + i).attr('readonly', false);
                                $("#" + splitid + "" + i).attr("data-mandatory", 'M');
                            }
                        }
                    }
                    hiddenValue += $("#" + splitid + "" + i).val();
                    if (i < splitcount) {
                        hiddenValue += "-";
                    }

                }
                $("#" + inputHidden).val(hiddenValue);
            }
            if (mtDependency == "ND") {
                splitid = $("#" + ele).attr("data-column");
                splitcount = $("#" + ele).attr("splitcount");
                for (var i = 1; i <= splitcount; i++) {
                    hiddenValue += $("#" + splitid + "" + i).val();
                    if (i < splitcount) {
                        hiddenValue += "-";
                    }
                }
                $("#" + inputHidden).val(hiddenValue);
            }
            if (nrCount != valCount && valCount != splitcount) {
                var msg = (labelObject['Enter Valid' + label] != null ? labelObject['Enter Valid' + label] : 'Enter Valid' + label);
                errorMessage(id, msg);
                return false;
            }
        }
        if (!str && mandatory == 'M' && type != 'hidden') {
            var msg = "";
            if (datatype == "L" || datatype == "C") {
                msg = (labelObject['Should be Selected'] != null ? labelObject['Should be Selected'] : 'Should be Selected');
            } else {
                msg = (labelObject['Should not be Blank'] != null ? labelObject['Should not be Blank'] : 'Should not be Blank');
                ajaxStop();//7
            }
            errorMessage(id, msg);
            return false;
        }
        var res;
        if (regex != null) {
            var patt = new RegExp(regex);
            res = patt.test(str);
        } else {
            res = true;
        }
        splitid = $("#" + ele).attr("data-column");
        splitcount = $("#" + ele).attr("splitcount");
        if (inputtype == 'MT' && splitcount == '3') {
            if ($("#LAND1").val() == "IN") {
                var split2Val = $("#" + splitid + '2').val();
                var split3Val = $("#" + splitid + '3').val();
                if (split2Val && split3Val) {
                    var splitVal = split2Val + split3Val;
                    var regex = "^[0-9]{10}$";
                    var patt = new RegExp(regex);
                    res = patt.test(splitVal);
                    if (res == false) {
                        errorMessage(id, (labelObject['STD + Telephone No. length Should be 10'] != null ? labelObject['STD + Telephone No. length Should be 10'] : 'STD + Telephone No. length Should be 10'));
                        return false;
                    }
                }
            }
        }
        psCount(tabId);
        var msg = $("#" + ele).attr("data-regex-msg");
        if (str && res == false)
        {
            errorMessage(id, msg);
            return false;
        }
        if (ele == 'GST_NUMBER' || ele == 'STCD3') {
            return is_GST_All(ele);
        }
        $(id).html("");
        $(id).hide();
        if (ele == 'O_REORDER_POINT' || ele == 'O_MAX_INV_LEV') {
            onChangMrpValidation();
        }
        return true;
    } else {
        psCount(tabId);
        return false;
    }
}
function errorMessage(id, msg) {
    $(id).fadeIn(1000).html(msg);
}
function onChangMrpValidation(ele) {

    if ($('#O_MRP_TYPE').val() != $('#DISMM').val()
            || $('#MINBE').val() != $('#O_REORDER_POINT').val()
            || $('#MABST').val() != $('#O_MAX_INV_LEV').val())
    {
        $("#JUSTIFICATION_COMMENTS").attr('data-mandatory', "M");
        $("#JUSTIFICATION_COMMENTS").parent("th").prev().addClass("labelMandColorRed");
        $("#JUSTIFICATION_COMMENTS").removeClass("visionInputDisable");
        $("#JUSTIFICATION_COMMENTS").attr('readonly', false);
    } else {
        $("#JUSTIFICATION_COMMENTS").attr('data-mandatory', "O");
        $("#JUSTIFICATION_COMMENTS").parent("th").prev().removeClass("labelMandColorRed");
        $("#JUSTIFICATION_COMMENTS").addClass("visionInputDisable");
        $("#JUSTIFICATION_COMMENTS").attr('readonly', true);
    }
}
function UpdateOrDelete(data, dataView, tabId, operation) {
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {
    }
    var jsondata = {};
    var basicData = {};
    var reviewIndFV;
    var vendorCode = $("#vendorCode").val();
    var locatCode = $("#locatcode").val();
    var companyCode = $("#compCode").val();
    var accountGroup = $("#accountGroup").val();
    var purchaseOrg = $("#purchOrg").val();
    var purchaseOrg = $("#purchOrg").val();
    var baskettype = $('#baskettypehid').val();
    var requestNumber = $("#requestNumber").val();
    var vendorCode = $("#vendorCode").val();
    if ($('#foreignReviewIndicator').is(':checked')) {
        reviewIndFV = "Y";
    } else
    {
        reviewIndFV = "N";
    }
    var reviewIndCA = "";
    if ($('#caReviewIndicator').is(':checked')) {
        reviewIndCA = "Y";
    } else
    {
        reviewIndCA = "N";
    }
    var newIfsc = "";
    if ($('#NEW_BNK').is(':checked'))
    {
        newIfsc = "Y";
    } else
    {
        newIfsc = "N";
    }
    $("#mat_creation_form_table :input").each(function () {
        var textid = $(this).attr("id");
        var textval = "";
        if ($("#" + textid).val() !== null && $("#" + textid).val() !== "") {
            var type = $(this).attr("type");
            textval = $(this).val();
            if (type != 'hidden') {
                if (textval != null && textval != '') {
                    textval = textval.toUpperCase();
                }
            }
        }
        if (textid != null && textid != 'CREATE_DATE') {
            basicData[textid] = textval;
        }
        if (textid != null && textid.lastIndexOf("HIDDEN") > -1) {
            var columnNames = $("#" + textid).val();
            var columnsArray = columnNames.split(",");

            var hiddenIds = textid.split("HIDDEN_");
            var hiddenVal = $("#" + hiddenIds[1]).val();
            for (var i = 0; i < columnsArray.length; i++) {
                if (hiddenVal != null) {
                    hiddenVal = hiddenVal.toUpperCase();
                }
                basicData[columnsArray[i]] = hiddenVal;
            }
        }
    });
    var jsonOBJ = {};
    var dataArray = [];
    var finalData = "";
    if (dataView != "GRID-VIEW") {
        jsonOBJ = JSON.parse(data);
        jsonOBJ.basicData = basicData;
        dataArray.push(jsonOBJ);
        finalData = JSON.stringify(jsonOBJ);
    } else
    {
        jsonOBJ = {};
        var gridData = JSON.parse(data);
        finalData = JSON.stringify(gridData);
    }
    var url = "";
    if (operation == "update" || operation == 'checkingTabData') {
        url = "updateRecord";
    } else if (operation == "delete")
    {
        url = "deleteRecord";
    } else if (operation == "calculateStock")
    {
        fetchCalculateStock(finalData, tabId, dataView);
    }
    if (operation != 'calculateStock') {
        var reqNumber = $("#REQ_NUMBER").val() != null ? $("#REQ_NUMBER").val() : "";
        var status = $("#STATUS").val() != null ? $("#STATUS").val() : "";
        $.ajax({
            type: "POST",
            url: url,
            data: {
                dataView: dataView,
                jsonData: finalData,
                gridId: tabId,
                panelId: $("#panelId").val(),
                'STATUS': status,
                'REQ_NUMBER': reqNumber,
                checkAttachType: ($("#checkAttachType").val() != null ? $("#checkAttachType").val() : ""),
                initParamSource: ($("#initParamSource").val() != null ? $("#initParamSource").val() : "")
            },
            traditional: true,
            cache: false,
            success: function (result) {

                var resultMessage;
                var response = JSON.parse(result);
                var resultNew = response.Message;
                var flag = response.messageFlag;
                if (result == null || result == "") {
                    result = "Failed to Update!"
                    result = (labelObject[result] != null ? labelObject[result] : result);
                }
                var hiddenGridId = $('#' + tabId + "_HIDDEN").val();

                if (hiddenGridId != null && hiddenGridId == "INSERT" && operation == "delete" && resultNew.lastIndexOf("Failed") > -1) {
                    resultMessage = "No Record to Delete.";
                    resultMessage = (labelObject[resultMessage] != null ? labelObject[resultMessage] : resultMessage);
                } else
                {
                    resultMessage = response.Message;
                }
                if (operation == 'checkingTabData') {
                    checkingTabData(tabId, basicData, dataView);
                } else {
                    ajaxStop();//23
                    var dialogSplitMessage = dialogSplitIconText(resultMessage, flag);
                    var modalObj = {
                        title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
                        body: labelObject[dialogSplitMessage] != null ? labelObject[dialogSplitMessage] : dialogSplitMessage,
                    };
                    var buttonArray = [
                        {
                            text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                            click: function () {
                                if (flag) {
                                    if (tabId != null && tabId.indexOf("ERP") > -1) {
                                        fetchErpTab(tabId, '');
                                    } else if (tabId != null
                                            && (tabId.indexOf("MM_ATTACHMENTS") > -1
                                                    || tabId.indexOf("SM_ATTACHMENTS") > -1
                                                    || tabId.indexOf("SPEC_ATTACHMENTS") > -1)) {
                                        fetchAttachmentsTabGridData(tabId);
                                    } else {
                                        fetchTabData(tabId, '');
                                        var role = $("#rolehid").val();
                                    }
                                } else {
                                    if (dataView == "GRID-VIEW") {
                                        if (tabId != null &&
                                                (tabId.indexOf("MM_ATTACHMENTS") > -1
                                                        || tabId.indexOf("SM_ATTACHMENTS") > -1
                                                        || tabId.indexOf("SPEC_ATTACHMENTS") > -1)) {
                                            fetchAttachmentsTabGridData(tabId);
                                            $('#' + tabId).jqxGrid('clearselection');
                                        } else if (tabId != null && tabId.indexOf("ERP") > -1) {
                                            fetchErpTab(tabId, '');
                                        } else {
                                            fetchTabData(tabId);
                                            $('#' + tabId).jqxGrid('clearselection');
                                        }
                                    } else if (dataView == "FORM-VIEW") {
                                        if (hiddenGridId != null && hiddenGridId == "INSERT" && operation == "delete") {
                                            fetchTabData(tabId);
                                        }
                                    }
                                }
                            },
                            isCloseButton: true
                        }
                    ];
                    modalObj['buttons'] = buttonArray;
                    createModal("dataDxpSplitterValue", modalObj);
                    $(".modal-dialog").addClass("modal-xs");
                    if (tabId != null && tabId.indexOf("GENERAL") > -1 && flag) {
                        var gstCodeTax;
                        gstCodeTax = $("#GST_CODE_GEN").val();
                        $("#GST_CODE_BASE").val(gstCodeTax);
                    }
                    if (tabId != null && tabId.indexOf("TAXATION") > -1 && flag) {
                        var reciepientType = "OT";
                        var panCharTop, panCharTax;
                        panCharTax = $("#O_1IPANNO").val();
                        $("#PAN_NUMBER").val(panCharTax);
                        panCharTop = $("#PAN_NUMBER").val();
                        if (panCharTop && panCharTop.charAt(3) == "C") {
                            reciepientType = "CO";
                        }
                        $("#QSREC").val(reciepientType);
                    }
                }
            },
            error: function (e) {
                console.log(e);
                sessionTimeout(e);
            }
        });
    }
    setTimeout(changeflagFuction, 300);
}// updateOrDelete fun
function checkingTabData(selectedGridId, basicData, dataView) {
//    ajaxStart();
    var dataArray = [];
    if (selectedGridId != null && selectedGridId != '') {
        if (dataView == 'GRID-VIEW') {
            dataArray = $('#' + selectedGridId).jqxGrid('getrows');
        } else {
            var dataObj = {};
            var gridIdHiddenValue = "UPDATE";
            $("table#" + selectedGridId + "_TABLE :input").each(function () {
                var textid = $(this).attr("id");
                var type = $(this).attr("type");
                var textval = $(this).val();
                console.log("textid:::" + textid);
                if (type != 'hidden') {
                    if (textval != null && textval != '') {
                        textval = textval.toUpperCase();
                    }
                }

                if (type != null && type == 'checkbox') {//
                    if ($("#" + textid).is(':checked')) {
                        textval = "Y";
                    } else {
                        textval = "N";
                    }
                }
                dataObj[textid] = textval;
            });
            dataArray.push(dataObj);
        }

        if (dataArray != null && dataArray.length != 0) {
            $.ajax({
                type: "post",
                url: "checkingTabData",
                cache: false,
                data: {'basicData': JSON.stringify(basicData),
                    gridId: selectedGridId,
                    dataArray: JSON.stringify(dataArray)
                },
                traditional: true,
                dataType: 'html',
                success: function (response) {
                    ajaxStop();
                    if (response != null && response != '') {
                        var resultObj = JSON.parse(response);
                        if (resultObj != null) {
                            var messageFlag = resultObj['messageFlag'];
                            var dialogSplitMessage = dialogSplitIconText(resultObj['Message'], "Y");
                            var modalObj = {
                                title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
                                body: labelObject[dialogSplitMessage] != null ? labelObject[dialogSplitMessage] : dialogSplitMessage,
                            };
                            var buttonArray = [
                                {
                                    text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                                    click: function () {
                                        if (messageFlag) {
                                            fetchTabData(selectedGridId);
                                        }
                                    },
                                    isCloseButton: true
                                }
                            ];
                            modalObj['buttons'] = buttonArray;
                            createModal("dataDxpSplitterValue", modalObj);
                            $(".modal-dialog").addClass("modal-xs");
                        }
                    }
                },
                error: function (e) {
                    ajaxStop();
                    sessionTimeout(e);
                }
            });
        } else {
        }
    }
}
function fetchErpTab(selectedGridId, erpTabGridId) {
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {
    }
    tabsOldData = {};
    globalErpTab = selectedGridId;
    alert("erpTabGridId::::" + erpTabGridId);
    console.log("erpTabGridId:::" + erpTabGridId);
    var selectedErpGridData = null;
    if (erpTabGridId == '') {// || erpTabGridId == 'undefined'
        erpTabGridId = $("#erpTabGridId").val();
    }
    try {
        selectedErpGridData = $('#' + erpTabGridId + "_TABLE").jqxGrid('getrowdata', $('#' + erpTabGridId + "_TABLE").jqxGrid('getselectedrowindex'));
    } catch (e) {
    }
    alert(erpTabGridId + ":::fetchErpTabData::::" + selectedGridId);
    alert("selectedErpGridData::::" + JSON.stringify(selectedErpGridData));
    var editableFlag = "N";
    var basicData = {};
    $("#mat_creation_form_table :input").each(function () {
        var textid = $(this).attr("id");
        var type = $(this).attr("type");
        var textval = $(this).val();
        if (type != 'hidden') {
            if (textval != null && textval != '') {
                textval = textval.toUpperCase();
            }
        }
        if (textid != null && textid != 'CREATE_DATE') {
            if (selectedErpGridData != null && selectedErpGridData[textid] != null) {
                textval = selectedErpGridData[textid];
            }
            basicData[textid] = textval;
        }
        if (textid != null && textid.lastIndexOf("HIDDEN") > -1) {
            var columnNames = $("#" + textid).val();
            var columnsArray = columnNames.split(",");

            var hiddenIds = textid.split("HIDDEN_");
            var hiddenVal = $("#" + hiddenIds[1]).val();
            if (selectedErpGridData != null && selectedErpGridData[hiddenIds[1]] != null) {
                hiddenVal = selectedErpGridData[hiddenIds[1]];
            }
            for (var i = 0; i < columnsArray.length; i++) {
                basicData[columnsArray[i]] = hiddenVal;
            }
        }
    });
    alert("basicData::::" + JSON.stringify(basicData));
    var instance = $("#INSTANCE").val();
    var plant = $("#BUSINESS_UNIT").val();
    alert(basicData['INSTANCE'] + "::::instance:::" + instance);
    alert(basicData['BUSINESS_UNIT'] + "::::plant:::" + plant);
    var rolehid = $("#rolehid").val();
    if (rolehid != null && (rolehid.indexOf("VM_") > -1 || rolehid.indexOf("CM_") > -1)) {
        editableFlag = "Y";
    } else {
        if (basicData != null && basicData['INSTANCE'] == instance && plant == basicData['BUSINESS_UNIT']) {
            if (erpTabGridId != null && erpTabGridId.indexOf("_OLD") > -1) {
                editableFlag = "N";
            } else {
                editableFlag = "Y";
            }
        }
    }
    basicData['editableFlag'] = editableFlag;
    basicData['erpTabGridId'] = erpTabGridId;
    if (selectedGridId != null) {
        $.ajax({
            type: "POST",
            url: 'fetchERPTabaData',
            data: {
                'gridId': selectedGridId,
                'basicData': JSON.stringify(basicData),
                'panelId': $("#panelId").val(),
                'erpTabGridId': erpTabGridId
            },
            traditional: true, cache: false,
            success: function (response) {
                ajaxStop();//26
                alert("response::::" + response);
                var erpDataObj = JSON.parse(response);
                $('#' + selectedGridId + "Icon").html(erpDataObj['tabOperationIcon']);
                if (erpDataObj['intiobj'] != null) {
                    var materialTyp = $("#RECORD_TYPE").val();
                    var initObj = erpDataObj['intiobj'];
                    var calButtonType = initObj['uuu_CalButtonType'];
                    if (calButtonType == null || calButtonType == '') {
                        calButtonType = 'ALL';
                    }
                    console.log("calButtonType2:::" + calButtonType);
                    if ((calButtonType == 'ALL' || calButtonType.indexOf(materialTyp) > -1)) {

                        $('#' + selectedGridId + '_CalculateStock').show();
                    } else {
                        $('#' + selectedGridId + '_CalculateStock').hide();
                    }
                }
                console.log(erpDataObj['dataLength']);
                var formDefaultValues = "";
                formDefaultValues = erpDataObj['defaultValues'];
                $("#defaultValues").val(formDefaultValues);
                $("#" + selectedGridId + "_defaultValues").remove();
                $("#mat_creation_form_table").append("<input type='hidden' id='" + selectedGridId + "_defaultValues' />");
                $("#" + selectedGridId + "_defaultValues").val(formDefaultValues);
                if (erpDataObj['dataLength'] != 1) {
                    $("#" + selectedGridId + "Icon").hide();
                    formGrid(selectedGridId, JSON.parse(erpDataObj['data']), erpDataObj['erpData']);
                } else {
                    try {
                        $("#" + selectedGridId).jqxGrid('destroy');
                    } catch (e) {
                    }
                    $("#" + selectedGridId + "_FORM").html(erpDataObj['data']);
                    if (editableFlag != null && editableFlag == 'Y') {
                        $('#' + selectedGridId + "Icon").show();
                        $("#" + selectedGridId + "_FORM" + " :input[data-type='D']").each(function ()
                        {
                            var id = $(this).attr('id');

                            var isEditable = $("#" + id).attr('data-editable');
                            if (isEditable == "Y") {
                                $("#" + id).datepicker({
                                    changeMonth: true,
                                    changeYear: true,
                                    dateFormat: "dd-mm-yy",
                                    showOn: "button",
                                    buttonImage: 'images/date_picker_icon.png',
                                    buttonImageOnly: true
                                });
                            }
                        });
                        var tabOldObj = {};
                        $("#" + selectedGridId + "_TABLE" + " :input").each(function ()
                        {
                            var textid = $(this).attr("id");
                            var type = $(this).attr("type");
                            var textval = $(this).val();
                            if (type != 'hidden') {
                                if (textval != null && textval != '') {
                                    textval = textval.toUpperCase();
                                }
                            }
                            if (type != null && type == 'checkbox') {//
                                if ($("#" + textid).is(':checked')) {
                                    textval = "Y";
                                } else {
                                    textval = "N";
                                }
                            }
                            if (textid != null && textid != 'CREATE_DATE') {
                                tabOldObj[textid] = textval;
                            }
                        });
                        if (tabOldObj != null) {
                            tabsOldData[selectedGridId] = tabOldObj;
                        }
                    } else {
                        $("#" + selectedGridId + "Icon").hide();
                    }
                }
            },
            error: function (e) {
                sessionTimeout(e);
            }
        });
    }
}
function formGrid(tabId, jsnobj, erpDataFlag) {
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    //var erpDataFlag = jsnobj['erpData'];
    console.log("erpDataFlag::::" + erpDataFlag);
    $("#" + tabId + "_Update").attr("data-view", "GRID-VIEW");
    $("#" + tabId + "_Delete").attr("data-view", "GRID-VIEW");
    $("#" + tabId).show();
    $("#" + tabId + '_TABLE').hide();
    var columns = jsnobj.columns;
    var datafields = jsnobj.datafields;
    var localdata = jsnobj.data;
    var dropDownListData = jsnobj.dropDownListData;
    grioldDataObj.oldData = localdata;
    var tableName = "";
    if (jsnobj['panelData'] != null && jsnobj['panelData'][13] != null) {
        tableName = jsnobj['panelData'][13];
    }
    var columnInitParamsObj = jsnobj['columnInitParamsObj'];
    var listTypeColName = [];
    var listTypeColNameId = [];
    var newLocalData = [];
    var gridConfigObj = {};
    var gridPropObj = {};
    gridPropObj = jsnobj.gridPropObj;
    var renderToolbar = gridConfigObj.renderToolbar;
    var gridInitParamObj = {};
    gridInitParamObj = jsnobj['gridInitParamObj'];
    gridConfigObj.renderToolbar = eval('(' + renderToolbar + ')');

    var dateRenderer = function (row, columnfield, value, defaulthtml, columnproperties, rowData) {
        var cellValue = $("#" + tabId).jqxGrid('getcellvalue', row, columnfield);
        // console.log("cellValue::"+cellValue);
        if (cellValue != null && cellValue != '') {
            var dateValue = $.jqx.dataFormat.formatdate(value, 'dd-MM-yyyy', $("#" + tabId).jqxGrid('gridlocalization'));
            //console.log("dateValue:::"+dateValue);
            cellValue = dateValue;
        }
        return cellValue;
    };
    var headerTooltipRenderer = function (element) {
        $(element).parent().jqxTooltip({position: 'mouse', theme: 'energyblue',
            position: 'bottom-right',
            showArrow: false, content: $(element).text()});
    }
    var attachmentImageRenderer = function (row, columnfield, value, defaulthtml, columnproperties) {

        if (value != "" && value != null)
        {
            return  "<img title=" + labelobj['Click to view the attachment'] != null ? labelobj['Click to view the attachment'] : 'Click to view the attachment' + "  style='cursor:pointer;' onclick=viewAttachment('" + tabId + "'," + row + ",'" + tableName + "')  src='" + value + "' class='imageStyle visionTemplete'  id='dtlul_" + row + "' >";

        } else {
            return "<div class='visionCoFileImage'>"
                    + "<input name='colFileImage' type='file' id ='visionColFileId' style ='display:none'/>"
                    + "<img src='images/attach_pin_icon_blue.png' onclick=showBrowseIdButton('" + tabId + "') style='cursor:pointer;margin-left: 30%;'/>"
                    + "</div>";

        }
    };
    var dataSheetRendered = function (element) {
        $(element).parent().jqxTooltip({position: 'mouse', theme: 'energyblue',
            position: 'bottom-right',
            showArrow: false, content: $(element).text()});
    }
    var gridDrpdownRenderor = function (row, columnfield, value, defaulthtml, columnproperties) {
        var cellValue = $("#" + tabId).jqxGrid('getcellvalue', row, columnfield);

        var viewType = "GRID-VIEW";
        var editable = gridConfigObj.editable;
        if (columnInitParamsObj != null && columnInitParamsObj != '' && columnInitParamsObj != undefined)
        {
            var columnParams = columnInitParamsObj[columnfield];
            if (columnParams != null && columnParams != '' && columnParams != undefined) {
                var editableFlag = columnParams['uuu_editable'];
                var hiddenType = $('#' + tabId).jqxGrid('getcellvalue', row, tabId + "_HIDDEN");
            }
        }
        if (editable) {
            if (editableFlag != null && editableFlag != '' && editableFlag == "N")
            {
                if (hiddenType != null && hiddenType != '' && hiddenType != undefined && hiddenType != "INSERT") {
                    var ddwData = jsnobj.dropDowndData;
                    var ddwObj = ddwData[columnfield];
                    var dependencyparams = ddwObj.dependencyparams;
                    return "<div  class='visionGridDataAlign' >" + cellValue + "</div>";
                } else
                {
                    var ddwData = jsnobj.dropDowndData;
                    var ddwObj = ddwData[columnfield];
                    var dependencyparams = ddwObj.dependencyparams;
                    $("#" + tabId).jqxGrid('setcolumnproperty', columnfield, 'editable', false);
                    //return "<div  style='width:99.5%;vertical-align:middle;height:100%;padding:2px 12px 2px 3px;' >" + cellValue + "<img class='prop_imgClass' src='images/search_icon_color_2.png' style='width:15px;height:15px;float:right;' onclick=visionDropdown('" + ddwObj.ddwId.trim() + "','" + dependencyparams + "','" + viewType + "','" + ddwObj.gridId + "','" + columnfield + "','" + row + "')></div>";
                    return "<div  class='visionGridDataAlign'><div class='visionGridDataAlignInfo'> " + cellValue + "</div><div class='visionGridDataAlignImage'><img id='dd" + tabId + columnfield + "' src='images/search_icon_color_2.png'  onclick=visionDropdown('" + ddwObj.ddwId.trim() + "','" + dependencyparams + "','" + viewType + "','" + ddwObj.gridId + "','" + columnfield + "','" + row + "')></div></div>";
                }
            } else
            {
                var ddwData = jsnobj.dropDowndData;
                var ddwObj = ddwData[columnfield];
                var dependencyparams = ddwObj.dependencyparams;
                $("#" + tabId).jqxGrid('setcolumnproperty', columnfield, 'editable', false);
                //return "<div  style='width:99.5%;vertical-align:middle;height:100%;padding:2px 12px 2px 3px;' >" + cellValue + "<img class='prop_imgClass' src='images/search_icon_color_2.png' style='width:15px;height:15px;float:right;' onclick=visionDropdown('" + ddwObj.ddwId.trim() + "','" + dependencyparams + "','" + viewType + "','" + ddwObj.gridId + "','" + columnfield + "','" + row + "')></div>";
                return "<div  class='visionGridDataAlign'><div class='visionGridDataAlignInfo'> " + cellValue + "</div><div class='visionGridDataAlignImage'><img src='images/search_icon_color_2.png'  onclick=visionDropdown('" + ddwObj.ddwId.trim() + "','" + dependencyparams + "','" + viewType + "','" + ddwObj.gridId + "','" + columnfield + "','" + row + "')></div></div>";
            }
        } else
        {
            var ddwData = jsnobj.dropDowndData;
            var ddwObj = ddwData[columnfield];
            var dependencyparams = ddwObj.dependencyparams;
            return "<div  class='visionGridDataAlign' >" + cellValue + "</div>";
        }
    };

    /* Renderer for textbox with dropdown*/
    var TB_DDW = function (row, columnfield, value, defaulthtml, columnproperties) {
        var ddwData = jsnobj.dropDowndData;
        console.log("ddwData::" + JSON.stringify(ddwData));
        var ddwObj = ddwData[columnfield];
        var dependencyparams = ddwObj.dependencyparams;
        var tbid = ddwObj.gridId + row;
        value = $("#" + ddwObj.gridId).jqxGrid('getcellvalue', row, 'PROPERTY_VALUE1');
        console.log("renderer::" + row + "::" + value);
        var viewType = "GRID-VIEW";
        if (value == null || value == 'null') {
            value = "";
        }
        return "<div  class='visionGridDataAlignInput' data-recid='' data-prop=''>"
                //  + "<input type='text' style='width:100%;' value='" + value + "' id='" + ddwObj.gridId + row + "'>"
                + "<div class='visionGridDataAlignInputField'>"
                + "<input type='text'"
                + " onkeyup=propValKeyUp1('" + tbid + "'," + row + ",'none','" + ddwObj.gridId + "','" + columnfield + "',event)"
                + " value='" + value + "' id='" + ddwObj.gridId + row + "'/>"
                + "</div><div class='visionGridDataAlignInputImage'>"
                + " <img src='images/icon.png' "
                + " onclick=visionDropdown('" + ddwObj.ddwId.trim() + "','" + dependencyparams + "','" + viewType + "','" + ddwObj.gridId + "','" + columnfield + "'," + row + ",'" + ddwObj.gridId + row + "')></div></div>";

    };
    var descrender = function (row, columnfield, value, defaulthtml, columnproperties) {

        return '<div style="height:' + $("#" + tabId).jqxGrid('rowsheight') + 'px" class="ta_style ta_style_Desc"  ><pre>' + value + '</pre></div>';
    };
    var descsaprender = function (row, columnfield, value, defaulthtml, columnproperties) {

        return '<div style="height:' + $("#" + tabId).jqxGrid('rowsheight') + 'px" class="ta_style ta_style_Desc"  ><pre>' + value + '</pre></div>';
    };
    if (gridPropObj.rowsheight != null) {
        gridPropObj.rowsheight = parseInt(gridPropObj.rowsheight);
        // gridPropObj.autorowheight = true;
    }
    var charRenderer = function (row, columnfield, value, defaulthtml, columnproperties) {
//DEFINITION
        // alert("Entered Char Renderer");
        var tooltip = "";
        var ddwData = jsnobj.dropDowndData;
        console.log("ddwData::" + JSON.stringify(ddwData));
        var ddwObj = ddwData[columnfield];
        var dependencyparams = ddwObj.dependencyparams;
        var property = value;
        var mand_ind;
        var highlevelid;
        mand_ind = $('#' + ddwObj.gridId).jqxGrid('getcellvalue', row, "REQUIRED_FLAG");
        highlevelid = $('#' + ddwObj.gridId).jqxGrid('getcellvalue', row, "HIGH_LEVEL_FLAG");
        console.log("highlevelid:::" + highlevelid);
        try {
            tooltip = $('#' + ddwObj.gridId).jqxGrid('getcellvalue', row, "DEFINITION");
        } catch (e) {
        }
        if (highlevelid == 'Y')
        {
            highlevelid = "<div><span id='span" + row + "' class='ui-icon ui-icon-plus'"
                    + " style='display:inline-block;cursor:pointer;' "
                    + "onclick=propertyHierarchy(" + row + ",'" + ddwObj.gridId + "','" + property.replace(/\s/g, "_") + "','PROPERTY_VALUE1')></span></div>";
        } else
        {
            highlevelid = "";
        }
        //alert('mand_ind::'+mand_ind);
        if (mand_ind == 'Y')
        {
            return  "<div title='" + tooltip + "' style='width:100%' class='propMandatory'> <div style='width:90%'>" + property + "</div>" + highlevelid + "</div>";
        } else
        {
            return  "<div title='" + tooltip + "' style='width:100%' class='propNormal'> <div style='width:100%'>" + property + highlevelid + "</div>";
        }
    };
    /*Renderer For Highlighting Mandatory Properties and Showing Multilevel Dr if applicable in Characteristic Tab */

    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }


    for (var i = 0; i < datafields.length; i++) {
        if (typeof datafields[i].values != "undefined" && datafields[i].values != null) {

            var listboxData = eval(datafields[i].values.source);

            var dataFeildName = datafields[i].name;
            // var dataFeildNameId=dataFeildName+"_ID";
            if (dataFeildName.indexOf("_DLOV") > -1) {
                listTypeColNameId.push(dataFeildName);
            } else {
                listTypeColName.push(dataFeildName);
            }

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
            datafields[i].values.source = listBoxAdapter.records;
            var changeFunObj = datafields[i].values;
            if (changeFunObj != null && changeFunObj['onchangeFunName'] != null && changeFunObj['onchangeFunName'] != '') {
            }
        }
    }
    var newLocalData = [];
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    var initDefaultFlag = $("#defaultFlag").val();
    var initattachType = $("#checkAttachType").val();
    if (!(initattachType != null && initattachType != '' && initattachType != undefined))
    {
        if (gridInitParamObj != null) {
            var attachInitParams = gridInitParamObj["uuu_attachInitParams"];
            if (attachInitParams != null && attachInitParams != '' && attachInitParams != undefined)
            {
                var initParams = attachInitParams.split(":");
                if (initParams != null && initParams != '' && initParams != undefined) {
                    $("#checkAttachType").val(initParams[1]);
                }
            }
        }
    }
    var source = $("#SOURCE").val();
    if (!(source != null && source != '' && source != undefined))
    {
        if (gridInitParamObj != null) {
            var initParamSource = gridInitParamObj["uuu_Source"];
            $("#initParamSource").val(initParamSource);
        }

    }

    console.log("labelobj:::" + labelObject);
    if (localdata != null && localdata.length > 0 && listTypeColName.length > 0) {
        for (var i = 0; i < localdata.length; i++) {
            var dataObj = localdata[i];
            for (var j = 0; j < listTypeColName.length; j++) {
                dataObj[listTypeColNameId[j]] = dataObj[listTypeColName[j]];
                var displayKeyValuObj = dropDownListData[listTypeColName[j]];
                for (var k = 0; k < displayKeyValuObj.length > 0; k++) {
                    var displayFieldObj = displayKeyValuObj[k];
                    if (displayFieldObj != null && displayFieldObj != "" && displayFieldObj.id == dataObj[listTypeColName[j]]) {
                        //console.log("listbox::::"+labelObject[displayFieldObj.ListboxValue]+"::::"+listTypeColName[j]);
                        dataObj[listTypeColName[j]] = displayFieldObj.ListboxValue;

                    }

                }
            }
            var defaultFlag = dataObj['DEFAULT_FLAG'];
            if (defaultFlag)
            {
                defaultFlag = "Y";
            } else
            {
                defaultFlag = "N";
            }
            var attachType = dataObj['ATTACH_TYPE'];
            if (attachType != null && attachType != '' &&
                    initattachType != null && initattachType != '' && attachType == initattachType) {
                if (defaultFlag != null && defaultFlag != '' && initDefaultFlag != null
                        && initDefaultFlag != '' && initDefaultFlag == defaultFlag)
                {
                    var imgSource = dataObj['CONTENT'];
                    $("#descImage").attr("src", imgSource);
                }
            }
            newLocalData.push(dataObj);
        }
        if (newLocalData != null && newLocalData.length > 0) {
            localdata = [];
            localdata = newLocalData;
        }
    }
    //   console.log(JSON.stringify(localdata));
    var source =
            {
                datatype: "array",
                localdata: localdata,
                datafields: datafields
            };
    var dataAdapter = new $.jqx.dataAdapter(source);
    var isExportable = true;
    gridConfigObj = jsnobj.gridPropObj;

    for (var i = 0; i < columns.length; i++) {
        if (columns[i].cellsrenderer != null) {
            columns[i].cellsrenderer = eval('(' + columns[i].cellsrenderer + ')');
        }

        if (columns[i].createeditor != null) {
            columns[i].createeditor = eval('(' + columns[i].createeditor + ')');
        }

        if (columns[i].initeditor != null) {
            columns[i].initeditor = eval('(' + columns[i].initeditor + ')');
        }
        if (columns[i].geteditorvalue != null) {
            columns[i].geteditorvalue = eval('(' + columns[i].geteditorvalue + ')');
        }
        if (columns[i].cellbeginedit != null) {
            columns[i].cellbeginedit = eval('(' + columns[i].cellbeginedit + ')');
        }
        if (columns[i].rendered != null) {
            columns[i].rendered = eval('(' + columns[i].rendered + ')');
        }

    }

    gridConfigObj.source = dataAdapter;
    gridConfigObj.columns = columns;
    var paginationFlag = gridConfigObj['pageable'];
    if (paginationFlag) {
        gridConfigObj.virtualmode = false;

    }
    if (gridConfigObj['rowsheight'] != null && gridConfigObj['rowsheight'] != '') {//rowsheight
        gridConfigObj.autorowheight = true;
    }
    var renderToolbar = gridConfigObj.renderToolbar;
    gridConfigObj.renderToolbar = eval('(' + renderToolbar + ')');
    try {
        $("#" + tabId).remove();
    } catch (e) {

    }
    if (erpDataFlag != 'Y') {

        $("#" + tabId + "_TABLE").after("<div id='" + tabId + "'></div>");
    } else {
        $("#" + tabId + "_FORM").after("<div id='" + tabId + "'></div>");
    }
    var pagerMode = $("#" + tabId).jqxGrid('pagermode');
//    gridConfigObj.enabletooltips = false;
    gridConfigObj.cellhover = function (element, pageX, pageY)
    {
    };
    $("#" + tabId).jqxGrid(gridConfigObj);

    $('#' + tabId).on('celldoubleclick', function (event) {
        var args = event.args;
        var dataField = args.datafield;
        var dataField1 = args.text;
        var rowIndex = args.rowindex;
        var cellValue = args.value;
        var isEditable = $('#' + tabId).jqxGrid('getcolumnproperty', dataField, 'editable');
        console.log("isEditable::::" + isEditable)
        var editable = gridConfigObj.editable;
        if (!isEditable || !editable) {
            var column = $('#' + tabId).jqxGrid('getcolumn', event.args.datafield).text;
            popupedit(column, cellValue);
        }

    });
    var dataLength = source.localdata.length;
    try {
        if (dataLength <= 5) {
            $("#" + tabId).jqxGrid({autoheight: true});
        }
    } catch (e) {
        console.log(e);
    }
    if (jsnobj.tbDdwEditFlag == true) {
        $("#" + tabId).jqxGrid('editable', false);
        $("#" + tabId).jqxGrid('selectionmode', 'multiple');
        //  gridConfigObj.editable = false;
    }
    var checkBoxFlag = false;
    $("#" + tabId).on('cellvaluechanged', function (event)
    {
        console.log("cell value changed");
        changeflag = true;
        if (checkBoxFlag)
        {
            checkBoxFlag = false;
            $("#" + tabId).jqxGrid('setcellvalue', event.args.rowindex, event.args.datafield, event.args.oldvalue);

        }
        var oldvalue = event.args.oldvalue;
        var newvalue = "";
        if (event.args.newvalue != null) {
            newvalue = event.args.newvalue.value
        }
        if (oldvalue != null && oldvalue != '' && oldvalue != undefined
                && newvalue != null && newvalue != '' && newvalue != undefined && oldvalue == newvalue) {
            changeflag = false;
        }
    });
    var fieldVal;
    $("#" + tabId).on('cellbeginedit', function (event)
    {
        $("#" + tabId).attr('data-last-ed-field', event.args.datafield);
        $("#" + tabId).attr('data-last-ed-row', event.args.rowindex);
        // event arguments.
        var args = event.args;
        // column data field.
        var dataField = event.args.datafield;
        // row's bound index.
        var rowBoundIndex = event.args.rowindex;
        // cell value
        var value = args.value;
        cellOldValue = value;
        // cell old value.
        var oldvalue = args.oldvalue;

        // row's data.
        var rowData = args.row;
        var columntype = args.columntype;
        try {
            if (columntype == "dropdownlist")
            {
                fieldVal = rowData[dataField.replace("_DLOV", "")];
            }
        } catch (e) {
        }
        var columnType = event.args.columntype;
        if (columnInitParamsObj != null && columnInitParamsObj != '' && columnInitParamsObj != undefined)
        {
            var columnParams;
            if (columnType == 'dropdownlist')
            {
                columnParams = columnInitParamsObj[dataField.replace("_DLOV", "")];
            } else
            {
                columnParams = columnInitParamsObj[dataField];
            }
            if (columnParams != null && columnParams != '' && columnParams != undefined) {
                var editable = columnParams['uuu_editable'];
                if (editable != null && editable != '' && editable == "N")
                {
                    var hiddenType = $('#' + tabId).jqxGrid('getcellvalue', rowBoundIndex, tabId + "_HIDDEN");
                    if (hiddenType != null && hiddenType != '' && hiddenType != undefined && hiddenType != "INSERT") {
                        $("#" + tabId).jqxGrid('endcelledit', rowBoundIndex, dataField, true);
                        if (columnType == "checkbox")
                        {
                            checkBoxFlag = true;
                        }
                    }
                }
            }
        }
        $("#" + tabId).jqxGrid('selectrow', rowBoundIndex);
        $("#" + tabId + "_Update").show();
        //   //console.log("cell began event");
    });

    $("#" + tabId).bind('rowselect', function (event) {
        var selectedrowindexes = $("#" + tabId).jqxGrid('selectedrowindexes');

        var rwindex = event.args.rowindex;
        if (selecteIndexes.indexOf(rwindex) == -1) {
            selecteIndexes.push(rwindex)
        }
        var column = event.args.column;
        if (selecteIndexes.length != 0 && selectedrowindexes.length != 0) {
            $("#" + tabId + "_Delete").show();
            $("#" + tabId + "_Update").show();
        } else
        {
            $("#" + tabId + "_Delete").hide();
            $("#" + tabId + "_Update").hide();
        }
        if (selectedrowindexes.length == 0) {
            selecteIndexes.length = 0;
        }
        // ////console.log("PUSH:::::selecteIndexes.length:::" + selecteIndexes.length);
    });
    var onChangeFunctions = jsnobj.onChangeFunctions;

    $("#" + tabId).on('change', function (event) {

        var args = event.args;
        var currentTarget = event.currentTarget;
        var currentDataField = currentTarget.dataset.lastEdField;
        var currentRowIndex = currentTarget.dataset.lastEdRow;

        console.log("Select Changed ");
        if (args != null && args != '' && args.item != null && args.item != '' && fieldVal != args.item.label) {
            $("#" + tabId).jqxGrid('endcelledit', currentRowIndex, currentDataField, false);
        }
        if (onChangeFunctions != null) {
            var functionName = onChangeFunctions[currentDataField];
            if (functionName != null) {
                functionName = functionName.replace("'rowIndex'", currentRowIndex);
                eval(functionName);
            }
        }
    });
    $("#" + tabId).bind('rowunselect', function (event) {
        var selectedrowindexes = $("#" + tabId).jqxGrid('selectedrowindexes');
        var rwindex = event.args.rowindex;
        selecteIndexes.pop(rwindex)
        if (selecteIndexes.length != 0 && selectedrowindexes.length != 0) {
            $("#" + tabId + "_Delete").show();
            $("#" + tabId + "_Update").show();
        } else
        {
            $("#" + tabId + "_Delete").hide();
            $("#" + tabId + "_Update").hide();
        }
        if (selectedrowindexes.length == 0) {
            selecteIndexes.length = 0;
        }
    });
    $("#" + tabId).on('rowclick', function (event) {

        $("#" + tabId + '_Update').show();
        $("#" + tabId + '_Delete').show();
    });
    $("#" + tabId + "_MO_COUNT").text("");
    $("#" + tabId + "_ICON").hide();
}// end of formGrid()
function gridOperation(operationName, tabId) {
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {
    }
    console.log("operationName:::" + operationName);
    selectedDataArray = [];
    var errorMessageTable = "";
    var errorMesssageDataArray = [];
    var errorMessagesCount = 0;
    if (operationName != null && operationName == 'update' || operationName == 'checkingTabData') {
        var selectedobject;
        if ($("#" + tabId + "_Update").attr('data-view') == 'TABLE-VIEW') {
            var matched = false;
            matched = false;
            initialTblViewData = JSON.parse($("#" + tabId + "_Update").attr('data-localdata'));
            initialTblViewCols = JSON.parse($("#" + tabId + "_Update").attr('data-datafields'));
            for (var rowIndex = 0; rowIndex < initialTblViewData.length; rowIndex++) {
                var oldDataObj = initialTblViewData[rowIndex];
                var dataObj = {};
                matched = false;
                if (initialTblViewCols != null && initialTblViewCols.length != 0) {
                    for (var i = 0; i < initialTblViewCols.length; i++) {
                        var initialTblViewColsObj = initialTblViewCols[i];
                        var dataType = $("#td" + initialTblViewColsObj['datafield'] + rowIndex).attr("data-type");
                        var fieldname = initialTblViewColsObj['datafield'];
                        var rangeFlag = $("#td" + initialTblViewColsObj['datafield'] + rowIndex).attr("data-isrange");
                        var regex = $("#td" + initialTblViewColsObj['datafield'] + rowIndex).attr('data-regex');
                        var regexMessage = $("#td" + initialTblViewColsObj['datafield'] + rowIndex).attr('data-regex-msg');
                        if (dataType == 'tb') {
                            var columnValue = $("#tb" + initialTblViewColsObj['datafield'] + rowIndex).val();

                            dataObj[fieldname] = columnValue;
                            if (oldDataObj[fieldname] != columnValue) {
                                matched = true;
                                if (columnValue != null
                                        && columnValue != ''
                                        && columnValue != undefined) {
                                    var pattern = new RegExp(regex);
                                    var res = pattern.test(columnValue);
                                    if (res == false)
                                    {
                                        errorMessageTable += '<tr><td>' + oldDataObj.PROPERTY_NAME + '</td><td>' + regexMessage + '</td></tr>';
                                        errorMessagesCount++;
                                    }
                                }
                            }
                            if (rangeFlag == 'Y'
                                    && (fieldname == 'PROPERTY_VALUE1'
                                            || fieldname == 'PROPERTY_VALUE2')) {
                                columnValue = $("#maxtb" + initialTblViewColsObj['datafield'] + rowIndex).val();

                                var minValue = $("#tbPROPERTY_VALUE1" + rowIndex).val();
                                var maxValue = $("#maxtbPROPERTY_VALUE2" + rowIndex).val();
                                dataObj['PROPERTY_VALUE2'] = maxValue;
                                if (!(minValue != null && minValue != '') && (maxValue != null && maxValue != '')) {
                                    errorMessageTable += '<tr><td>' + oldDataObj.PROPERTY_NAME + '</td><td>Min Value Should not be Blank</td></tr>';
                                    errorMessagesCount++;
                                } else
                                if (minValue != null && minValue != '' && !(maxValue != null && maxValue != '')) {
                                    errorMessageTable += '<tr><td>' + oldDataObj.PROPERTY_NAME + '</td><td>Max Value Should not be Blank</td></tr>';
                                    errorMessagesCount++;
                                } else
                                if (minValue != null && minValue != ''
                                        && (maxValue != null && maxValue != ''
                                                && parseInt(minValue) > parseInt(maxValue))
                                        ) {
                                    errorMessageTable += '<tr><td>' + oldDataObj.PROPERTY_NAME + '</td><td>Min Value Should be less than max value</td></tr>';
                                    errorMessagesCount++;
                                }
                            }


                        } else if (initialTblViewColsObj['columntype'] == "checkbox" && !initialTblViewColsObj['hidden']) {
                            var columnValue = $("#tb" + initialTblViewColsObj['datafield'] + rowIndex).is(':checked') == true ? "Y" : "N";
                            var value = $("#tb" + initialTblViewColsObj['datafield'] + rowIndex).is(':checked');
                            dataObj[fieldname] = columnValue;

                            if (oldDataObj[fieldname] != value) {
                                matched = true;
                            }
                        } else {
                            if (fieldname != 'PROPERTY_VALUE2') {
                                var value = $("#td" + initialTblViewColsObj['datafield'] + rowIndex).val();
                                if (fieldname == 'DATA_TYPE') {
                                    dataObj[fieldname] = $("#tdDATA_TYPE" + rowIndex).text();
                                } else {
                                    dataObj[fieldname] = $("#td" + initialTblViewColsObj['datafield'] + rowIndex).text();
                                }
                            }

                        }
                    }
                    if (dataObj != null && !jQuery.isEmptyObject(dataObj)) {
                        if (matched) {
                            if (parseInt(errorMessagesCount) == 0) {
                                selectedDataArray.push(dataObj);
                            }

                        }
                    }
                }
                // end if td array
            }// end loop
        } else {
            var lasteditedfield = $('#' + tabId).attr('data-last-ed-field');
            var lasteditedrow = $('#' + tabId).attr('data-last-ed-row');
            $('#' + tabId).jqxGrid('endcelledit', lasteditedrow, lasteditedfield, false);
            var jsonDataArray = grioldDataObj.oldData;
            // alert("jsonDataArray:::");
            var rwindex = selecteIndexes[0];
            var rowindexes = $('#' + tabId).jqxGrid('getselectedrowindexes');
            if (operationName == 'checkingTabData') {

                selectedDataArray = $('#' + tabId).jqxGrid('getdisplayrows');

            } else {
                console.log("rowindexes:::" + rowindexes);
                if (rowindexes != null) {
                    var insertCount = 0;
                    var totalRowIndex = rowindexes.length;
                    var datainformations = $('#' + tabId).jqxGrid('getdatainformation');
//                    var datainformations = $('#' + gridId).jqxGrid('getdatainformation');
                    if (datainformations != null) {
                        var paginginformation = datainformations['paginginformation'];
                        if (paginginformation != null) {
                            var pagesize = paginginformation['pagesize'];
                            if (pagesize != null && parseInt(pagesize) < totalRowIndex) {
                                totalRowIndex = parseInt(pagesize);
                            }

                        }
                    }
                    for (var m = 0; m < totalRowIndex; m++) {
                        if (rowindexes[m] != -1) {
                            var updateGridJsonObj = {};
                            var newGriddata = $('#' + tabId).jqxGrid('getrowdata', rowindexes[m]);
                            var oldGridData = jsonDataArray[rowindexes[m - insertCount]];
                            var matchCount = 0;
                            var gridIdHidden = tabId + "_HIDDEN";
                            var objectKeys = Object.keys(newGriddata);
                            for (var i = 0; i < objectKeys.length; i++)
                            {
                                try {
                                    if ($("#" + tabId).jqxGrid('getcolumnproperty', objectKeys[i], 'columntype') == 'datetimeinput') {
                                        if (newGriddata[objectKeys[i]] == 'aN-aN-NaN')
                                        {
                                            newGriddata[objectKeys[i]] = "";
                                        } else {
                                            newGriddata[objectKeys[i]] = castDate(newGriddata[objectKeys[i]]);
                                        }
                                    }
                                } catch (e) {
                                    console.log("error in js::" + e);
                                }
                            }
                            if (newGriddata[gridIdHidden] != null && newGriddata[gridIdHidden] != '' && newGriddata[gridIdHidden] != 'INSERT') {
                                for (var key in oldGridData) {
                                    var oldgridvalue = oldGridData[key];
                                    var newgridValue = newGriddata[key];
                                    if (key != 'show_detail') {
                                        if (newgridValue != oldgridvalue) {
                                            matchCount++;
                                        }
                                    }
                                }
                                if (matchCount != 0) {
                                    selectedDataArray.push(newGriddata);
                                    matchCount == 0;
                                }
                            } else {
                                insertCount++;
                                selectedDataArray.push(newGriddata);
                            }
                        }
                    }
                }
            }
        }
    } else if (operationName != null && operationName == 'insert') {
        var lasteditedfield = $('#' + tabId).attr('data-last-ed-field');
        var lasteditedrow = $('#' + tabId).attr('data-last-ed-row');
        $('#' + tabId).jqxGrid('endcelledit', lasteditedrow, lasteditedfield, false);
        selectedDataArray.length = 0;

        for (var i = 0; i < selecteIndexes.length; i++) {

            var rwindex = selecteIndexes[i];

            var newdata = $('#' + tabId).jqxGrid('getrowdata', rwindex);
            var jsonDataArray = grioldDataObj.oldData;
            var oldData = jsonDataArray[rwindex];
            selectedDataArray.push(newdata);
            console.log("UPdated selecteIndexes:::" + i + "::::rwindex:::" + rwindex);
            console.log("UPdated NEW data::::" + i + ":::::rwindex::::::" + rwindex + "::" + JSON.stringify(newdata));
            console.log("UPdated OLD data::::" + i + "::::::rwindex:::::" + rwindex + "::" + JSON.stringify(oldData));
        }
    }
    console.log("selectedDataArray::::2998:::" + JSON.stringify(selectedDataArray));
    if (errorMessageTable != null && errorMessageTable != '') {
        errorMessageDataObj.errorMesssage = errorMessageTable;
        return errorMessageDataObj;
    }
    return selectedDataArray;
}
function endoperation(selectedDataArray1, tableName, dataView, operation, basicData) {
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    selectedDataArray = [];
    selectedDataArray = selectedDataArray1;
    //console.log("selectedDataArray1::::" + selectedDataArray.length)
    if (dataView == 'TABLE-VIEW') {
        var fieldIds = [];
        var fieldVals = [];
        var dataArray = [];
        var jsOpsObj = null;
        for (var i = 0; i < selectedDataArray1.length; i++) {
            fieldIds = [];
            fieldVals = [];
            for (var j = 0; j < initialTblViewCols.length; j++) {
                fieldIds.push(initialTblViewCols[j].datafield);
                fieldVals.push(selectedDataArray1[i][initialTblViewCols[j].datafield]);
            }
            jsOpsObj = new Object();
            jsOpsObj.feildIds = fieldIds;
            jsOpsObj.feildValues = fieldVals;
            jsOpsObj.basicData = basicData;
            dataArray.push(jsOpsObj);
        }
        UpdateOrDelete(JSON.stringify(dataArray), dataView, tableName, "update");
    } else {
        var resultObj = {};
        var validatioFlag = true;
        var errorMsg = "";
        for (var i = 0; i < selectedDataArray.length; i++) {
            resultObj = {};
            var dataString = "";

            dataString = JSON.stringify(selectedDataArray[i]);
            resultObj = genericGridValidatin(dataString, tableName);

            obj = JSON.parse(resultObj);
            if (obj.errorCount != 0) {
                errorMsg = obj.errorMsg;
                validatioFlag = false;
                break;
            } else {
                continue;
            }
        }
        if (validatioFlag) {
            var dataArray = [];
            if (Array.isArray(selectedDataArray)) {
                for (var i = 0; i < selectedDataArray.length; i++) {
                    var jsonOBJ = {}
                    jsonOBJ.feildIds = [];
                    jsonOBJ.feildValues = [];
                    jsonOBJ.basicData = {};
                    var objectr = selectedDataArray[i];

                    jsonOBJ.feildIds = Object.keys(objectr);
                    for (var k = 0; k < jsonOBJ.feildIds.length; k++) {
                        jsonOBJ.feildValues.push(objectr[jsonOBJ.feildIds[k]]);

                    }
                    jsonOBJ.basicData = basicData;
                    dataArray.push(jsonOBJ);
                }
            }

            UpdateOrDelete(JSON.stringify(dataArray), dataView, tableName, (operation != null && operation != 'checkingTabData' ? "update" : "checkingTabData"));
        } else {

            genericGridValidationMessage(errorMsg, tableName);
        }
    }
}

function paymentProcess(cost, subscribeType) {
    var ssUsername = $("#ssUsername").val();
    var selectedRole = "";
    if (ssUsername != null && ssUsername != '' && ssUsername != 'null') {
        var subScriptionObjArray = [];
        if (cost != null && cost != undefined && cost != "") {
            var options = {};
            $.ajax({
                datatype: "json",
                type: "POST",
                url: "processCheckout",
                data: {
                    cost: cost,
                    subscribeType: subscribeType
                },
                traditional: true,
                cache: false,
                success: function (response) {  
                    ajaxStop();
                    if (response != null && !jQuery.isEmptyObject(response)) {
                        options = response;
                        options.handler = function (successResponse) {
                            paymentResponse(successResponse, subScriptionObjArray, response, "Y", selectedRole);
                        };
                        options.notes = {
                            "address": "note value"
                        };
                        options.theme = {
                            "color": "#9932CC"
                        };
                        var propay = new Razorpay(options);
                        propay.on('payment.failed', function (failResponse) {
                            paymentResponse(failResponse, subScriptionObjArray, response, "N", selectedRole);
                        });

                        propay.open();
                    }
                },
                error: function (e) {
                    ajaxStop();
                    alert('Error: ' + e);

                }
            });
        } else {
            var modalObj = {
                title: 'Message',
                body: 'Please select atleast one api to checkout.'
            };
            var buttonArray = [
                {
                    text: 'Ok',
                    isCloseButton: true,

                }
            ];
            modalObj['buttons'] = buttonArray;
            createModal("modalDailogDiv1", modalObj);
        }

    } else {
        $('#loginModel').modal('show');
    }

}
function paymentResponse(payResponse, subScriptionObjArray, paymentOptions, paymentFlag, selectedRole) {
    if (payResponse != null) {
        var paymentResultObj = {};
        if (paymentFlag == 'Y') {
            paymentResultObj['org_name'] = payResponse['org_name'];
            paymentResultObj['razorpay_order_id'] = payResponse['razorpay_order_id'];
            paymentResultObj['razorpay_payment_id'] = payResponse['razorpay_payment_id'];
            paymentResultObj['payment_id'] = payResponse['razorpay_payment_id'];
            paymentResultObj['razorpay_signature'] = payResponse['razorpay_signature'];
        } else {
            paymentResultObj['errorcode'] = payResponse.error.code;
            paymentResultObj['errordescription'] = payResponse.error.description;
            paymentResultObj['errorreason'] = payResponse.error.reason;
            paymentResultObj['errorsource'] = payResponse.error.source;
            paymentResultObj['errorstep'] = payResponse.error.step;
            paymentResultObj['order_id'] = payResponse.error.metadata.order_id;
            paymentResultObj['payment_id'] = payResponse.error.metadata.payment_id;
        }

        $.ajax({
            datatype: "json",
            type: "POST",
            url: "saveSubscriptionDetails",
            data: {
                subScriptionObjArray: JSON.stringify(subScriptionObjArray),
                paymentOptions: JSON.stringify(paymentOptions),
                paymentResultObj: JSON.stringify(paymentResultObj),
                paymentFlag: paymentFlag,
                selectedRole: selectedRole,
                apiId: subScriptionObjArray[0]['apiId'],
                subscriptionId: subScriptionObjArray[0]['subscriptionId']
            },
            traditional: true,
            cache: false,
            success: function (response) {
                ajaxStop();
                if (response != null && !jQuery.isEmptyObject(response)) {
                    var modalObj = {
                        title: 'Message',
                        body: response['message']
                    };
                    var buttonArray = [
                        {
                            text: 'Ok',
                            click: function () {
                                if (response['flag']) {
                                    fetchTabGrid(5);
                                }

                            },
                            isCloseButton: true
                        }
                    ];
                    modalObj['buttons'] = buttonArray;
                    createModal("modalDailogDiv", modalObj);
                }
            },
            error: function (e) {
                ajaxStop();
                alert('Error: ' + e);

            }
        });
    }

}






