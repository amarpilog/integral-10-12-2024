/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
$(function () {

    $(document).ajaxSend(function (e, xhr, options) {
        var token = $("input[name='_csrf']").val();
        var header = "X-CSRF-TOKEN";
        xhr.setRequestHeader(header, token);
        xhr.setRequestHeader("Access-Control-Allow-Origin", "*");
    });
    $("html").addClass('visionScrollBar');
//    var request = new XMLHttpRequest();
//document.referrer = "http://www.google.com";
//    XMLHttpRequest.setRequestHeader("Referer", "http://www.google.com");
    var buttonValue = $('#visionHideBreadcrumbInput').val();
    if (buttonValue == '>>') {
        $(".visionHideBreadcrumbBtn").attr("title", "Click here to show Bread Crumb & Quick Links");
    } else {
        $(".visionHideBreadcrumbBtn").attr("title", "Click here to hide Bread Crumb & Quick Links");
    }
//    if ($(".visionHideBreadcrumbBtn").hasClass("inactive_toggle")) {
////        $(".visionBodyContentInner").addClass("visionExpandBodyContentInner");
//        $(".visionHideBreadcrumbBtn").attr("title", "Click here to show Bread Crumb & Quick Links");
//    } else {
////        $(".visionBodyContentInner").removeClass("visionExpandBodyContentInner");
//        $(".visionHideBreadcrumbBtn").attr("title", "Click here to hide Bread Crumb & Quick Links");
//    }
});
//console.log = function () {};
alert = function () {};
confirm = function () {};
prompt = function () {};
var validNavigation = false;
var dialogValueResize = "true";
var labelObject = {};
var resultFlag = true;
var imageid = 0;
var basicDatas = {};
var firstPanelShowFlag = false;
var secondPanelShowFlag = false;
var thirdPanelShowFlag = false;
var fourthPanelShowFlag = true;
var searchPanelShowFlag = false;
var formPanelShowFlag = true;
var paramPanelShowFlag = false;
var classificatePanelShowFlag = false;
var selectedColumnData;
var fieldsArray = [];
var HtmlEntities = {
    " ": "&nbsp;"
};
function logout()
{
	 generateNewSession();
    $('#signOut').modal('hide');
//    navigationMenuUrl("cloudLogout");
localStorage.removeItem("chatBotLanguage");

    window.location.href = "cloudLogout";
}
$(window).resize(function () {
    fluidDialog();
});
$(document).on("dialogopen", ".ui-dialog", function (event, ui) {
    fluidDialog();
});
function fluidDialog()
{
    var $visible = $(".ui-dialog:visible");
    $visible.each(function () {
        var $this = $(this);
        var dialog = $this.find(".ui-dialog-content").data("ui-dialog");
        if (dialog.options.fluid)
        {
            var wWidth = $(window).width();
            if (wWidth < (parseInt(dialog.options.maxWidth) + 50))
            {
                $this.css("max-width", "90%");
            } else
            {
                $this.css("max-width", dialog.options.maxWidth + "px");
            }
            dialog.option("position", dialog.options.position);
        }
    });
}
function dialogSplitIconText()
{
    var changeDialogText;
    if (arguments[1] == true)
    {
        changeDialogText = "<div class='visionDialogSperate'><table class='visionDialogSplitTable'><tr><td class='visionDialogSplitData'><div class='visionDialogSperateImage'><img src='images/help.png'></div></td><td class='visionDialogSplitData'><div class='visionDialogSperateContent'><span>" + arguments[0] + "</span></div></td></tr></table></div>";
    } else if (arguments[1] == "D") {
        changeDialogText = "<div class='visionDialogSperate'><table class='visionDialogSplitTable'><tr><td class='visionDialogSplitData'><div class='visionDialogSperateImage'><img src='images/delete_icon.svg'></div></td><td class='visionDialogSplitData'><div class='visionDialogSperateContent'><span>" + arguments[0] + "</span></div></td></tr></table></div>";
    } else if (arguments[1] == "P") {
        changeDialogText = "<div class='visionDialogSperate'><table class='visionDialogSplitTable'><tr><td class='visionDialogSplitData'><div class='visionDialogSperateImage'><img src='images/Duplicate-Icon.svg'></div></td><td class='visionDialogSplitData'><div class='visionDialogSperateContent'><span>" + arguments[0] + "</span></div></td></tr></table></div>";
    } else if (arguments[1] == "S") {
        changeDialogText = "<div class='visionDialogSperate'><table class='visionDialogSplitTable'><tr><td class='visionDialogSplitData'><div class='visionDialogSperateImage'><img src='images/like_blue.png' style = 'width: 20px;' ></div></td><td class='visionDialogSplitData'><div class='visionDialogSperateContent'><span>" + arguments[0] + "</span></div></td></tr></table></div>";
    } else if (arguments[1] == "R") {
        changeDialogText = "<div class='visionDialogSperate'><table class='visionDialogSplitTable'><tr><td class='visionDialogSplitData'><div class='visionDialogSperateImage' ><img src='images/dislike_blue.png' style = 'width: 20px;' ></div></td><td class='visionDialogSplitData'><div class='visionDialogSperateContent'><span>" + arguments[0] + "</span></div></td></tr></table></div>";
    } else if (arguments[1] == "C") {
        changeDialogText = "<div class='visionDialogSperate'><table class='visionDialogSplitTable'><tr><td class='visionDialogSplitData'><div class='visionDialogSperateImage' ><img src='images/Copy-Icon.svg'></div></td><td class='visionDialogSplitData'><div class='visionDialogSperateContent'><span>" + arguments[0] + "</span></div></td></tr></table></div>";
    } else if (arguments[1] == "V") {
        changeDialogText = "<div class='visionDialogSperate'><table class='visionDialogSplitTable'><tr><td class='visionDialogSplitData'><div class='visionDialogSperateImage' ><img src='images/Save-Icon.svg'></div></td><td class='visionDialogSplitData'><div class='visionDialogSperateContent'><span>" + arguments[0] + "</span></div></td></tr></table></div>";
    } else if (arguments[1] == "T") {
        changeDialogText = "<div class='visionDialogSperate'><table class='visionDialogSplitTable'><tr><td class='visionDialogSplitData'><div class='visionDialogSperateImage' ><img src='images/transfer_to_ERP.png' style = 'width: 20px;' ></div></td><td class='visionDialogSplitData'><div class='visionDialogSperateContent'><span>" + arguments[0] + "</span></div></td></tr></table></div>";
    } else if (arguments[1] == "SB") {
        changeDialogText = "<div class='visionDialogSperate'><table class='visionDialogSplitTable'><tr><td class='visionDialogSplitData'><div class='visionDialogSperateImage' ><img src='images/submit_icon_blue.png' style = 'width: 25px;' ></div></td><td class='visionDialogSplitData'><div class='visionDialogSperateContent'><span>" + arguments[0] + "</span></div></td></tr></table></div>";
    } else if (arguments[1] == "I") {
        changeDialogText = "<div class='visionDialogSperate'><table class='visionDialogSplitTable'><tr><td class='visionDialogSplitData'><div class='visionDialogSperateImage' ><img src='images/Re-Instantantia-OutLine-Icon-01.svg' style = 'width: 25px;' ></div></td><td class='visionDialogSplitData'><div class='visionDialogSperateContent'><span>" + arguments[0] + "</span></div></td></tr></table></div>";
    } else {
        changeDialogText = "<div class='visionDialogSperate'><table class='visionDialogSplitTable'><tr><td class='visionDialogSplitData'><div class='visionDialogSperateImage'><img src='images/Change-icon-OutLine.svg' style = 'width: 25px;' ></div></td><td class='visionDialogSplitData'><div class='visionDialogSperateContent'><span>" + arguments[0] + "</span></div></td></tr></table></div>";
    }
    return changeDialogText;
}
/* breadcrumb menu change */
$(document).ready(function ()
{
    $("#breadCrumbMenu").change(function ()
    {
        if ($(this).val() !== "none")
        {
            $('#breadCrumbCheck').val($(this).val());
        }
    });
    $("#BreadCrumbButton").unbind("click").click(function ()
    {
        var selectedValueItem = $("#breadCrumbMenu").jqxComboBox('getSelectedItem');
        var selectedValue = selectedValueItem['value'];
//        var selectedValue = $("#breadCrumbMenu").val();
        if (selectedValue != null && selectedValue !== "none" && selectedValue !== "")
        {
            breadCrumbCheck(selectedValue);
        }
    });
    $("#BreadCrumbSearchButton").unbind("click").click(function ()
    {
        var selectedValueItem = $("#breadCrumbMenuSearch").jqxComboBox('getSelectedItem');
        var selectedValue = selectedValueItem['value'];
        if (selectedValue != null && selectedValue !== "none" && selectedValue !== "")
        {
            breadCrumbSearchCheck(selectedValue);
        }
    });

});
function breadCrumbSearchCheck(selectedValue)
{
    if ($("#breadCrumbSearchCheck").is(':checked'))
    {
        navigationMenuUrl(selectedValue, 'Y');
    } else
    {
        navigationMenuUrl(selectedValue);
    }
}
function breadCrumbCheck(selectedValue)
{
    if ($("#breadCrumbCheck").is(':checked'))
    {
//        var checkBox = $('#breadCrumbCheck').val();
//        window.open(checkBox, '_blank');
        navigationMenuUrl(selectedValue, 'Y');
    } else
    {
//        var checkBox = $('#breadCrumbCheck').val();
//        window.open(checkBox, '_self');
        navigationMenuUrl(selectedValue);
    }
}

function startTabLoader() {
    console.log("in start startTabLoader ");
    $("#wait").css("opacity", "0.99");
    $("#wait").css("display", "block");
    $("body").css("pointer-events", "none");
}

function endTabLoader() {
    console.log("in endTabLoaderregister");
    $("#wait").css("display", "none");
    $("body").css("pointer-events", "auto");
}


function callStartAjax() {
    $("#wait").css("opacity", "0.99");
    $("#wait").css("display", "block");
    $("body").css("pointer-events", "none");
}
function callEndAjax()
{
    $("#wait").css("display", "none");
    $("body").css("pointer-events", "auto");
}
function ajaxStart() {
    $(document).ajaxStart(function () {
        $("#Loader").css("opacity", "0.99");
        $("#Loader").css("display", "block");
        $("body").css("pointer-events", "none");
    });
}
function ajaxStop() {
    $(document).ajaxStop(function () {
        $("#Loader").css("display", "none");
        $("body").css("pointer-events", "auto");
    });
}


function sessionTimeout(sessionStatus)
{
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    if (sessionStatus.status == 1) {
        $("#logoutDailog").html("<div style='margin-left: 5%;'><div style='float:left'><img height='30px' src='images/help.png'></img></div><div style='float:left;margin-left: 10%;'>" + (labelObject['Session Timeout'] != null ? labelObject['Session Timeout'] : 'Session Timeout') + ".</div></div>");
    } else {
        $("#logoutDailog").html("<div style='margin-left: 5%;'><div style='float:left'><img height='30px' src='images/help.png'></img></div><div style='float:left;margin-left: 10%;'>" + (labelObject[sessionStatus.statusText] != null ? labelObject[sessionStatus.statusText] : sessionStatus.statusText) + ".</div></div>");
//         window.location.href = "httpError";
//          navigationMenuUrl('httpError');
//        $("#logoutDailog").html("<div style='margin-left: 5%;'><div style='float:left'><img height='30px' src='images/help.png'></img></div><div style='float:left;margin-left: 10%;'>" + (labelObject['Access is forbidden to the requested page'] != null ? labelObject['Access is forbidden to the requested page'] : 'Access is forbidden to the requested page') + ".</div></div>");
    }
    $("#logoutDailog").dialog({
        title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
        modal: true,
        width: 270,
        height: 135,
        fluid: true,
        buttons: [{
                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                click: function () {
                    $(this).html("");
                    $(this).dialog("close");
                    $(this).dialog("destroy");
//                     navigationMenuUrl('timeout');
                    if (sessionStatus.status == 1) {
//                        navigationMenuUrl("timeout");
                        window.location.href = "timeout";
                    }


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


function navigationSubMenuUrl() {
    $('.tooltip-inner').hide();
    $('.showDXPSplitterSecondGovdata').hide();
    $('.showDXPSplitterTaxonomyDxpdata').hide();
    var url = arguments[0];
    var moduleCode = "";
    var role = "";
    if (url != null) {
        var urlArray = url.split("?");
        if (urlArray[0] != null) {
            url = urlArray[0];

            if (urlArray[1] != null) {
                var parametersArray = urlArray[1].split("&");

                if (parametersArray != null && parametersArray.length > 0) {
                    for (var i = 0; i < parametersArray.length; i++) {
                        console.log("parametersArray[i]:::" + parametersArray[i]);
                        if (parametersArray[i] != null && parametersArray[i].indexOf("target=blank") == -1) {
                            var paramsNamesArray = parametersArray[i].split("=");
                            if (paramsNamesArray[0] != null) {
                                moduleCode = paramsNamesArray[0];
                                role = paramsNamesArray[1];
                            }

                        }

                    }

                }
            }
            $.ajax({
                datatype: "json",
                type: "POST",
                url: url,
                data: {
                    'moduleCode': moduleCode,
                    'role': role
                },
                traditional: true,
                cache: false,
                success: function (response) {
                    ajaxStop();
                    if (response != null && !jQuery.isEmptyObject(response)) {
                        $("#showDXPSplitterTaxonomyDxpdata").hide();
                        $("#dxpTaxonomyRepositories").html(response);
                        $("#dxpTaxonomyRepositories").show();
                    }

                },
                error: function (e) {
                    ajaxStop();
                    alert('Error: ' + e);

                }
            });
        }
    }


}

function dialogWidthResize(message, setWidth)
{
    if (message != null && message != '') {
        var messagecount = message.toString().length;
        // var messagecount = message.length;
        if (messagecount <= 25) {
            setWidth = 275;
        } else if ((messagecount > 25) && (messagecount <= 32)) {
            setWidth = 300;
        } else if ((messagecount >= 32) && (messagecount <= 42)) {
            setWidth = 330;
        } else if ((messagecount >= 43) && (messagecount <= 52))
        {
            setWidth = 390;
        } else if ((messagecount >= 51) && (messagecount <= 62))
        {
            setWidth = 455;
        } else {
            setWidth = 500;
        }
        if ($(window).width() > setWidth) {

            return setWidth;
        } else if ($(window).width() < setWidth) {

            setWidth = $(window).width() - 5;
            return setWidth;
        }

    }
}

function popupedit(column, cellValue) {
    if (true) {
        var message = cellValue;
        var setWidth;
        $("#dialog1").dialog({
            modal: true,
            title: column,
            width: dialogWidthResize(message, setWidth),
            height: 'auto',
            minHeight: 'auto',
            maxHeight: 250,
            fluid: true,
            buttons: {
                Ok: function () {
                    $(this).html("");
                    $(this).dialog("close");
                    $(this).dialog("destroy");
                }
            },
            open: function () {
                $(this).html(cellValue);
                $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                $(".visionHeaderMain").css("z-index", "999");
                $(".visionFooterMain").css("z-index", "999");
            },
            beforeClose: function (event, ui)
            {
                $(".visionHeaderMain").css("z-index", "99999");
                $(".visionFooterMain").css("z-index", "99999");
            }
        }); //end confirm dialog


        console.log(column + " : " + cellValue);
    }

}

function showContent(id) {
    var column = $("#" + id).attr("data-label");
    var cellValue = $("#" + id).val();
    if (cellValue != null && cellValue != '') {
        popupedit(column, cellValue);
    }

}
function clearFieldData(id) {
    //$("#"+id).attr("value", "");
    $("#" + id).val('');
}
function clearComboBoxField(id) {
    $("#" + id).jqxComboBox('clearSelection');
}
function showConsolidationContent(index, columnName, type) {
    var cellValue = "";
    if (type == 'table') {
        cellValue = $("#" + columnName + "_" + index).html();
    } else {
        cellValue = $("#hidden_" + columnName + "_" + index).val();
    }
//    cellValue = $("#hidden_" + columnName + "_" + index).val();  
    var column = $("#label_" + columnName).html();
    if (cellValue != null && cellValue != '' && cellValue != 'null') {
        popupedit(column, cellValue);
    }

}
function selectAllCheckBox(id) {
    $(".recordCheckBox").prop('checked', $("#" + id).prop('checked'));
}

function navigateToIconURL(href, gridId, selectedRecordData) {
//    alert("::navigateCocpitView::::"+href);
    if (href != null) {
        console.log("Before:::" + href);
        console.log("After:::" + href);
        var hrefArray = href.split("?");
        $("#navigationUrlForm").attr("action", hrefArray[0]);
        $("#navigationUrlForm").attr("target", "_blank");
        var token = $("input[name='_csrf']").val();
        if (!(selectedRecordData != null && !jQuery.isEmptyObject(selectedRecordData)) && gridId != null && gridId != '') {
            var selectedrowindexes = $('#' + gridId).jqxGrid('getselectedrowindexes');
            if (selectedrowindexes != null && selectedrowindexes.length != 0) {
                selectedRecordData = $('#' + gridId).jqxGrid('getrowdata', selectedrowindexes[0]);
            } else {
                selectedRecordData = $('#' + gridId).jqxGrid('getrowdata', 0);
            }
        }
        $("#navigationUrlForm").find('input').remove();
        if (hrefArray[1] != null) {
            var inputParams = hrefArray[1].split("&");
            for (var i = 0; i < inputParams.length; i++) {
                if (inputParams[i] != null) {
                    if (inputParams[i] != null && inputParams[i] != '') {

                        if (inputParams[i] != null && inputParams[i] != '' && inputParams[i].indexOf("items={") > -1) {
                            var inputParamsArray = inputParams[i].split("=");
                            var value = inputParamsArray[1];
                            if (selectedRecordData != null && !jQuery.isEmptyObject(selectedRecordData)
                                    && value != null && value.indexOf("<<--") > -1) {
                                for (var columnName in selectedRecordData) {
                                    var matchedColumn = "<<--" + columnName + "-->>";
                                    if (value.indexOf(matchedColumn) > -1) {
                                        value = value.replace(matchedColumn, selectedRecordData[columnName]);
                                    }
                                }
                            }
                            var inputType = "<input type='hidden' name='" + inputParamsArray[0] + "' value='" + value + "'/>";
                            $("#navigationUrlForm").append(inputType);
                        } else {
                            var inputArray = inputParams[i].split("=");
                            var value = inputArray[1];
                            if (selectedRecordData != null && !jQuery.isEmptyObject(selectedRecordData)
                                    && value != null && value.indexOf("<<--") > -1) {
                                for (var columnName in selectedRecordData) {
                                    var matchedColumn = "<<--" + columnName + "-->>";
                                    if (value.indexOf(matchedColumn) > -1) {
                                        value = value.replace(matchedColumn, selectedRecordData[columnName]);
                                    }
                                }
                            }
                            var inputType = "<input type='hidden' name='" + inputArray[0] + "' value='" + value + "'/>";
                            $("#navigationUrlForm").append(inputType);
                        }

                    }

                }

            }
            inputType = "<input type='hidden' name='_csrf' value='" + token + "'/>";
            $("#navigationUrlForm").append(inputType);
        }

        $("#navigationUrlForm").submit();
    }

}

function showHelp(gridId, opName) {
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    $.ajax({
        type: "POST",
        url: 'helpHref',
        dataType: 'html',
        data: {
            'compId': gridId

        },
        traditional: true,
        cache: false,
        success: function (response) {
            if (response != null && response != '') {
                var ssThemesURL = $("#ssThemesURL").val();
                var fullPath = ssThemesURL + '/' + response;
                $("#dialog").html('<iframe style="border: 0px; " src="' + fullPath + '" width="940px" height="400px"></iframe>').dialog({
                    title: (labelObject[opName] != null ? labelObject[opName] : opName),
                    modal: true,
                    height: 'auto',
                    minHeight: 500,
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
            console.log(e);
            sessionTimeout(e);
        }

    })


}

function viewXml(gridId, index, columnfield, tableName) {
    var data = $('#' + gridId).jqxGrid('getrowdata', index);
    if (data != null) {
        var sequenceNo = data['SEQUENCE_NO'];
        var recordNo = data['RECORD_NO'];
        var plant = data['PLANT'];
        $("#urlSubmitForm").find('input').remove();
        var csrfToken = $("input[name='_csrf']").val();
        if (csrfToken != null && csrfToken != '') {
            var csrf = "<input type='hidden' name='_csrf' value='" + csrfToken + "'/>";
            $("#urlSubmitForm").append(csrf);
        }
        //$("#urlSubmitForm").attr("action", "viewPayLoad");
        $("#urlSubmitForm").attr("action", "viewXml");
        $("#urlSubmitForm").attr("target", "_blank");
        $("#urlSubmitForm").find('input').remove();
        if (columnfield != null && columnfield != '') {
            var selectingDataField = "<input type='hidden' name='selectingDataField' value='" + columnfield + "'/>";
            $("#urlSubmitForm").append(selectingDataField);
        }
        if (tableName != null && tableName != '') {
            var gridTableName = "<input type='hidden' name='tableName' value='" + tableName + "'/>";
            $("#urlSubmitForm").append(gridTableName);
        }
        var inputType = "<input type='hidden' name='xmlJSONData' value='" + JSON.stringify(data) + "'/>";
        $("#urlSubmitForm").append(inputType);
        $("#urlSubmitForm").submit();
    }

}

function showSelectedRows(gridId, selectedIndex, uuu_GridNtfnFlag) {
    var labelObj = {};
    // labelObject = {};
    try {
        labelObj = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    $('#' + gridId + '_showRowsCount').remove();
    //$("#pager"+gridId).remove("<div class='showRowsCount'></div>");
//     clearTimeout(5000);
    if (gridId != null && gridId != '') {
        var selectedRowIndexes = $("#" + gridId).jqxGrid('getselectedrowindexes');
        var totalRowIndex = selectedRowIndexes.length;
        var datainformations = $('#' + gridId).jqxGrid('getdatainformation');
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
//$('.showRowsCount').show();
        // $('#showRowsCount').show();
        var ntfnFlag = "Y";
        var nthRowNtfn = "Y";
        if (uuu_GridNtfnFlag != null && uuu_GridNtfnFlag != '') {
            var uuu_GridNtfnFlagArray = uuu_GridNtfnFlag.split(":");
            if (uuu_GridNtfnFlagArray != null && uuu_GridNtfnFlagArray.length != 0) {
                try {
                    ntfnFlag = (uuu_GridNtfnFlagArray[0] != null && uuu_GridNtfnFlagArray[0] != '' ? uuu_GridNtfnFlagArray[0] : "Y");
                } catch (e) {
                }
                try {
                    nthRowNtfn = (uuu_GridNtfnFlagArray[1] != null && uuu_GridNtfnFlagArray[1] != '' ? uuu_GridNtfnFlagArray[1] : "Y");
                } catch (e) {
                }
            }
        }
        var divCode = "";
//         divCode = '<div class="showRowsCountInner">';
        if (ntfnFlag == 'Y') {
//            divCode += '<div class="visionCountNtfctnDiv">'
//                    + '<img src="images/about_us_white.png" alt="Notification" class="visionCountNtfctnIcon">'
//                    + '</div>'
            divCode += '<p>You have selected ' + totalRowIndex + ' row(s)';
            // divCode += '<div class="visionCancelDiv"><img src="images/cancel_icon.png" onclick="hideRows()" alt="Notification" class="visionCancelIcon">'

        }
        if (nthRowNtfn == 'Y') {
            if (selectedIndex != null) {
                if ($.isArray(selectedIndex)) {
                    selectedIndex = -1;
                } else {
                    try {
                        selectedIndex = parseInt(selectedIndex);
                    } catch (e) {
                    }
                }

//                if (selectedIndex == -1) {
//                    selectedIndex = 0;
//                }
                if (selectedIndex != -1) {
                    selectedIndex = parseInt(selectedIndex) + 1;
                    var supTag = "<sup>th</sup>";
                    switch (selectedIndex) {
                        case 1:
                            supTag = "<sup>st</sup>"
                            break;
                        case 2:
                            supTag = "<sup>nd</sup>"
                            break;
                        case 3:
                            supTag = "<sup>rd</sup>"
                            break;
                        default:
                            supTag = "<sup>th</sup>"
                            break;
                    }
//                divCode += '<div class="visionCountNtfctnDiv">'
//                        + '<img src="images/about_us_white.png" alt="Notification" class="visionSelectedNtfctnIcon">'
//                        + '</div>'
                    divCode += ' , ' + selectedIndex + supTag + ' row</p>';
                }


            }
//            divCode += '<div class="visionCancelDiv"><img src="images/cancel_icon.png" onclick="hideRows()" alt="Notification" class="visionCancelIcon">'
//                    + '</div>';
        }
        if (divCode != null && divCode != '') {
            // divCode = '<div class="showRowsCountInner">'+divCode+'</div>';
            // $('#showRowsCount').html(divCode);
            $("#pager" + gridId).append("<div class='showRowsCount' id='" + gridId + "_showRowsCount'>" + divCode + "</div>");
//            setTimeout(function () {
//                $('#showRowsCount').hide();
//            }, 5000);
        } else {
            $("#pager" + gridId).remove("<div class='showRowsCount'>" + divCode + "</div>");
        }
    } else {
        $("#pager" + gridId).remove("<div class='showRowsCount'>" + divCode + "</div>");
    }

}
//function showSelectedRows(gridId, selectedIndex, uuu_GridNtfnFlag) {
//    var labelObj = {};
//    // labelObject = {};
//    try {
//        labelObj = JSON.parse($("#labelObjectHidden").val());
//    } catch (e) {
//
//    }
////     clearTimeout(5000);
//    if (gridId != null && gridId != '') {
//        var selectedRowIndexes = $("#" + gridId).jqxGrid('getselectedrowindexes');
//        var totalRowIndex = selectedRowIndexes.length;
//        var datainformations = $('#' + gridId).jqxGrid('getdatainformation');
//        if (datainformations != null) {
//            var paginginformation = datainformations['paginginformation'];
//            if (paginginformation != null) {
//                var pagesize = paginginformation['pagesize'];
//                if (pagesize != null && parseInt(pagesize) < totalRowIndex) {
//                    totalRowIndex = parseInt(pagesize);
//                }
//
//            }
//        }
//    }
//    if (totalRowIndex > 0) {
//
//        $('#showRowsCount').show();
//        var ntfnFlag = "Y";
//        var nthRowNtfn = "Y";
//        if (uuu_GridNtfnFlag != null && uuu_GridNtfnFlag != '') {
//            var uuu_GridNtfnFlagArray = uuu_GridNtfnFlag.split(":");
//            if (uuu_GridNtfnFlagArray != null && uuu_GridNtfnFlagArray.length != 0) {
//                try {
//                   ntfnFlag =  (uuu_GridNtfnFlagArray[0] != null && uuu_GridNtfnFlagArray[0] != '' ? uuu_GridNtfnFlagArray[0]:"Y") ;
//                } catch (e) {
//                }
//                try {
//                    nthRowNtfn = (uuu_GridNtfnFlagArray[1] != null && uuu_GridNtfnFlagArray[1] != '' ? uuu_GridNtfnFlagArray[1] : "Y");
//                } catch (e) {
//                }
//            }
//        }
//         var divCode = "";
//         
////         divCode = '<div class="showRowsCountInner">';
//        if (ntfnFlag == 'Y') {
//            divCode += '<div class="visionCountNtfctnDiv">'
//                    + '<img src="images/about_us_white.png" alt="Notification" class="visionCountNtfctnIcon">'
//                    + '</div>'
//                    + '<p>You have selected ' + totalRowIndex + ' row(s)</p>';
//            divCode += '<div class="visionCancelDiv"><img src="images/cancel_icon.png" onclick="hideRows()" alt="Notification" class="visionCancelIcon">'
//
//        }
//             if (nthRowNtfn == 'Y') {
//            if (selectedIndex != null) {
//                try {
//                    selectedIndex = parseInt(selectedIndex);
//                } catch (e) {
//                }
//                if (selectedIndex == -1) {
//                    selectedIndex = 0;
//                }
//                selectedIndex = parseInt(selectedIndex) + 1;
//                var supTag = "<sup>th</sup>";
//                switch (selectedIndex) {
//                    case 1:
//                        supTag = "<sup>st</sup>"
//                        break;
//                    case 2:
//                        supTag = "<sup>nd</sup>"
//                        break;
//                    case 3:
//                        supTag = "<sup>rd</sup>"
//                        break;
//                    default:
//                        supTag = "<sup>th</sup>"
//                        break;
//                }
//                divCode += '<div class="visionCountNtfctnDiv">'
//                        + '<img src="images/about_us_white.png" alt="Notification" class="visionSelectedNtfctnIcon">'
//                        + '</div>'
//                divCode += '<p>You have selected ' + selectedIndex + supTag + ' row</p>';
//
//            }
//            divCode += '<div class="visionCancelDiv"><img src="images/cancel_icon.png" onclick="hideRows()" alt="Notification" class="visionCancelIcon">'
//                    + '</div>';
//        }
//        if (divCode != null && divCode != '') {
//            divCode = '<div class="showRowsCountInner">'+divCode+'</div>';
//            $('#showRowsCount').html(divCode);
//            setTimeout(function () {
//                $('#showRowsCount').hide();
//            }, 5000);
//        }else{
//            $('#showRowsCount').hide();
//        }
//    } else {
//        $('#showRowsCount').hide();
//    }
//
//}
function hideRows() {
    $('#showRowsCount').hide();
}

function chartEndTabLoader() {
    console.log("in endTabLoaderregister");
    $("#wait").css("display", "none");
    $("body").css("pointer-events", "auto");
}

function chartStartTabLoader() {
    console.log("in start startTabLoader ");
    setTimeout(function () {
        $("#wait").css("opacity", "0.99");
        $("#wait").css("display", "block");
        $("body").css("pointer-events", "none");
    }, 50);
}

function hideBreadcrumb() {
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }


    $(".visionBreadcrumMain").toggle();
    if ($(".visionBreadcrumMain").is(":visible")) {
        $('#visionHideBreadcrumbInput').val('<<'); //visionHideBreadcrumbInput
        $(".visionHideBreadcrumbBtn").attr("title", "Click here to hide Bread Crumb & Quick Links");
        $(".visionHideBreadcrumbBtn").removeClass("visionHideBreadcrumbBtnRes");
    } else {
        $('#visionHideBreadcrumbInput').val('>>');
        $(".visionHideBreadcrumbBtn").attr("title", "Click here to show Bread Crumb & Quick Links");
        $(".visionHideBreadcrumbBtn").addClass("visionHideBreadcrumbBtnRes");
    }
}
function showLoader() {
    $("#Loader").css("opacity", "0.99");
    $("#Loader").css("display", "block");
    $("body").css("pointer-events", "none");
}
function stopLoader()
{
    $("#Loader").css("display", "none");
    $("body").css("pointer-events", "auto");
}
function showHelpMenu(helpMenuURL, buttonType, desc) {
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {
    }
    buttonType = buttonType + ":";
    if (helpMenuURL != null
            && helpMenuURL != ''
            && helpMenuURL != 'null'
            && helpMenuURL != '#'
            && helpMenuURL.indexOf(buttonType) > -1) {
        var ssRole = $("#helpMenuRole").val();
        var ssThemesURL = $("#helpMenuThemesURL").val();
        var ssBusinessRole = $("#helpMenuBuRole").val();
        if (ssBusinessRole != null && ssBusinessRole != '' && ssBusinessRole != 'null') {
            ssRole = ssBusinessRole;
        }
        var helpMenuURLArray = helpMenuURL.split(";");
        if (helpMenuURLArray != null && helpMenuURLArray.length != 0) {
            var helpURL = "";
            for (var i = 0; i < helpMenuURLArray.length; i++) {
                var helpURLStr = helpMenuURLArray[i];
                if (helpURLStr != null
                        && helpURLStr != ''
                        && helpURLStr != 'null'
                        && helpURLStr != '#'
                        && helpURLStr.indexOf(buttonType) > -1) {
                    helpURLStr = helpURLStr.replace(buttonType, "");
                    if (helpURLStr.match("^http")
                            || helpURLStr.match("^Http")
                            ) {//str.match("^Hello")
                        helpURL = helpURLStr;
                    } else {
                        if (buttonType == 'H:') {
                            helpURL = ssThemesURL + "Help/" + ssRole + "/" + helpURLStr.replace(buttonType, "");

                        } else if (buttonType == 'I:') {
                            helpURL = ssThemesURL + "Info/" + ssRole + "/" + helpURLStr.replace(buttonType, "");
                        } else if (buttonType == 'V:') {
                            helpURL = ssThemesURL + "Video/" + ssRole + "/" + helpURLStr.replace(buttonType, "");
                        }
                    }
                    console.log("helpURL::" + helpURL);
                    break;
                }
            }
            var title = helpMenuURLArray[3];
            var titleArray = title.split(":");
            if (helpURL != null && helpURL != '' && helpURL != 'null') {
                var ifmHelpStr = "<div ><iframe  id='ifmHelp' width ='100%' src='" + helpURL + "'  height='500' frameborder='0' allowfullscreen > </iframe> </div>";
                var modalObj = {
                    title: titleArray[1],
                    body: ifmHelpStr
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
                createVideoModal("modalInfoDailogDiv", modalObj);

//  $("#ifmHelp").attr('src', helpURL);        
            }
//        }
        }
    }
}
function createVideoModal(modalDivId, modalObj) {
    $("#" + modalDivId).html("");
    var buttonsArray = modalObj['buttons'];
    $("#" + modalDivId).addClass("modal fade");
    $("#" + modalDivId).attr("role", "dialog");
    $("#" + modalDivId).attr("data-backdrop", "static");
    $("#" + modalDivId).addClass("videoDataPopup");
    var modalContant = ''
            + '<div class="modal-dialog modal-lg">'
            + '<div class="modal-content">'
            + '<div class="modal-header">'
            + '<h4 class="modal-title text-center">' + modalObj['title'] + '</h4>'
            + '<button type="button" class="close" data-toggle="modal" data-target="#' + modalDivId + '"><span aria-hidden="true">&times;</span><span class="sr-only">Close</span></button>'
            + '</div>'
            + '<div class="modal-body">'
            + modalObj['body']
            + ' </div>';
    modalContant += '</div>'
            + '</div>';
    $("#" + modalDivId).html(modalContant);
    if (buttonsArray != null && buttonsArray.length != 0) {
        var footer = $('<div class="modal-footer">');
        for (var i = 0; i < buttonsArray.length; i++) {
            var buttonObj = buttonsArray[i];
            if (buttonObj != null && !jQuery.isEmptyObject(buttonObj)) {
                var onclickButtonfunction = "";
                var isDismissButton = "";
                if (buttonObj['isCloseButton'] == true) {
                    isDismissButton = " data-dismiss=\"modal\" ";
                }
                var button = $('<button type="button" class="btn btn-primary" ' + isDismissButton + ' >' + buttonObj['text'] + '</button>');
                if (buttonObj['click'] != null && buttonObj['click'] != '') {
                    $(button).click(buttonObj['click']);
                }
                $(footer).append(button);
            }
        }
        $("#" + modalDivId + " .modal-content").append(footer);
    } else {
        $("#" + modalDivId + " .modal-content").append('<div class="modal-footer">'
                + ' <button type="button" class="btn btn-primary" data-dismiss="modal">Ok</button>'
                + '</div>');
    }
    $('#' + modalDivId).modal('show');
}
function createModal(modalDivId, modalObj) {
    $("#" + modalDivId).html("");
    var buttonsArray = modalObj['buttons'];
    $("#" + modalDivId).addClass("modal fade");
    $("#" + modalDivId).attr("role", "dialog");
    $("#" + modalDivId).attr("data-backdrop", "static");
    $("#" + modalDivId).css("padding-top", "58px");
    var modalContant = ''
            + '<div class="modal-dialog opacity-animate3">'
            + '<div class="modal-content">'
            + '<div class="modal-header">'
            + '<h4 class="modal-title text-center">' + modalObj['title'] + '</h4>'
            + '<button type="button" class="close" data-toggle="modal" data-target="#' + modalDivId + '"><span aria-hidden="true">&times;</span><span class="sr-only">Close</span></button>'
            + '</div>'
            + '<div class="modal-body">'
            + modalObj['body']
            + ' </div>';
    modalContant += '</div>'
            + '</div>';
    $("#" + modalDivId).html(modalContant);
    if (buttonsArray != null && buttonsArray.length != 0) {
        var footer = $('<div class="modal-footer">');
        for (var i = 0; i < buttonsArray.length; i++) {
            var buttonObj = buttonsArray[i];
            if (buttonObj != null && !jQuery.isEmptyObject(buttonObj)) {
                var onclickButtonfunction = "";
                var isDismissButton = "";
                if (buttonObj['isCloseButton'] == true) {
                    isDismissButton = " data-dismiss=\"modal\" ";
                }
                var button = $('<button type="button" class="btn btn-primary" ' + isDismissButton + ' >' + buttonObj['text'] + '</button>');
                if (buttonObj['click'] != null && buttonObj['click'] != '') {
                    $(button).click(buttonObj['click']);
                }
                $(footer).append(button);
            }
        }
        $("#" + modalDivId + " .modal-content").append(footer);
    } else {
        $("#" + modalDivId + " .modal-content").append('<div class="modal-footer">'
                + ' <button type="button" class="btn btn-primary" data-dismiss="modal">Ok</button>'
                + '</div>');
    }
     $("#" + modalDivId + " .modal-dialog").draggable({
            handle: ".modal-header"
        });
    $('#' + modalDivId).modal('show');
}
function cloudFormpopup(modalDivId, modalObj) {
    $("#" + modalDivId).html("");
    var buttonsArray = modalObj['buttons'];
    var modelSize = modalObj['modelSize'];
    $("#" + modalDivId).addClass("modal fade");
    $("#" + modalDivId).attr("role", "dialog");
    var modalContant = ''
    if (modelSize != null && modelSize != undefined && modelSize != "" &&
            modelSize != "null" && modelSize == "modal-xl") {
        modalContant += '<div class="modal-dialog ' + modelSize + '">';
    } else {
        modalContant += '<div class="modal-dialog">';
    }
    modalContant += '<div class="modal-content">'
            + '<div class="modal-header">'
            + '<h4 class="modal-title text-center">' + modalObj['title'] + '</h4>'
            + '<button type="button" class="close" data-dismiss="modal">&times;</button>'
            + '</div>'
            + '<div class="modal-body">'
            + modalObj['body']
            //            + '<div id="modalDailogDiv1" class="modal fade" style="padding-top:58px">'

            + '</div>'
            + ' </div>';
    modalContant += '</div>'
            + '</div>';

    $("#" + modalDivId).html(modalContant);
    $('#' + modalDivId).modal('show');
    $('.cloudSubFormDiv').addClass("cloudFormDiv");
}
function navigationMenuUrl(url) {
//    var url = arguments[0];
    var nextPage = arguments[1];
    var searchId = '';
    alert(nextPage + ":::navigationMenuUrl::::" + url);
    $("#navigationUrlForm").empty();
    if (nextPage != null && nextPage == 'Y' && arguments.length > 1) {
        $("#navigationUrlForm").attr("target", "_blank");
    } else {
        $("#navigationUrlForm").removeAttr("target");
        if (url != null && url.indexOf("target=blank") > -1) {
            $("#navigationUrlForm").attr("target", "_blank");
            url = url.replace("?target=blank", "");
            url = url.replace("&target=blank", "");
        }
    }
    if (url != null) {
        var urlArray = url.split("?");
        if (urlArray[0] != null) {
            $("#navigationUrlForm").attr("action", urlArray[0]);
            if (urlArray[1] != null) {
                var parametersArray = urlArray[1].split("&");
                if (parametersArray != null && parametersArray.length > 0) {
                    for (var i = 0; i < parametersArray.length; i++) {
                        console.log("parametersArray[i]:::" + parametersArray[i]);
                        if (parametersArray[i] != null && parametersArray[i].indexOf("target=blank") == -1) {
                            var paramsNamesArray = parametersArray[i].split("=");
                            if (paramsNamesArray[0] != null) {
                                searchId = paramsNamesArray[1];
                                $("#navigationUrlForm").append("<input type='hidden' name='" + paramsNamesArray[0] + "' value='" + paramsNamesArray[1] + "'/>");
                            }

                        }

                    }

                }
            }
            var csrfToken = $("input[name='_csrf']").val();
            if (csrfToken != null && csrfToken != '') {
                var csrf = "<input type='hidden' name='_csrf' value='" + csrfToken + "'/>";
                $("#navigationUrlForm").append(csrf);
            }
            if (urlArray[0] != null && urlArray[0] != undefined && urlArray[0] != '' && urlArray[0] == 'GenericSearch') {
                creationProcess(urlArray[0], searchId)
            } else {
                $("#navigationUrlForm").submit();
            }

        }
    }
}
function showDXPButtons(value) {
    $(".showDXPSplitterSecondGovdata").hide();
    $.ajax({
        datatype: "json",
        type: "POST",
        url: 'showSecondSplitterMenu',
        data: {
            'moduleCode': "",
            'role': "role"
        },
        traditional: true,
        cache: false,
        success: function (response) {
            ajaxStop();
            if (response != null && !jQuery.isEmptyObject(response)) {

                $(".showDXPSplitterSecondGovdata").show();
                $(".dxpTaxonomyRepositories").show();
                $("#secondDxpSplitterData").html(response);

            }

        },
        error: function (e) {
            ajaxStop();
            alert('Error: ' + e);

        }
    });
}
function showDXPTaxButtons() {
    $.ajax({
        datatype: "json",
        type: "POST",
        url: 'showTaxonomyRepositorySubData',
        data: {
            'moduleCode': "",
            'role': "role"
        },
        traditional: true,
        cache: false,
        success: function (response) {
            ajaxStop();
            if (response != null && !jQuery.isEmptyObject(response)) {
                $(".dxpTaxonomyRepositories").show();
                $("#secondDxpSplitterData").html(response);

            }

        },
        error: function (e) {
            ajaxStop();
            alert('Error: ' + e);

        }
    });
}
function showDXPGovButtons() {
    var resultObj = {};
    $.ajax({
        datatype: "json",
        type: "POST",
        url: 'GenericSearch',
        data: {
            'moduleCode': "",
            'role': "role"
        },
        traditional: true,
        cache: false,
        success: function (response) {
            ajaxStop();
            if (response != null && !jQuery.isEmptyObject(response)) {
                resultObj = JSON.parse(response);
//                $(".mainThirdDxpSplitter").html('<div id="accordioncover" class="acrdn visionMaterialGeneric"></div>');
                $("#searchDefaultSplitter").show();
                $("#accordioncoverSplitter").html('<div id="accordioncover" class="acrdn visionMaterialGeneric"></div>');
                $("#accordioncover").html(resultObj['searchInfo']);
                $("#accordion").accordion(
                        {active: 0,
                            collapsible: true,
                            heightStyle: 'content', activate: function (event, ui) {

                            }}
                );
                $("#childAccordion").accordion(
                        {active: 0, collapsible: true,
                            heightStyle: 'content'}
                );
                getPersonalizationDataOpt('P');

            }

        },
        error: function (e) {
            ajaxStop();
            alert('Error: ' + e);

        }
    });
}

//function smartTextSearch(typediId, domainId, enterFlag) {
//    ajaxStart();
//    showLoader();
//    $("#intellisense").hide();
//    $("#intelliSenseSelectBoxHideShow").hide();
//    $(".homeSocialTrends").hide();
//    $("#mainDxpSplitter").show();
//    var typedValue = $("#" + typediId).val();
//    var domainValue = $("#" + domainId).val();
//    if (enterFlag != null && enterFlag != '' && enterFlag != undefined
//            && enterFlag == 'Y') {
//        typedValue = typediId;
//        domainValue = domainId;
//    }
//    console.log('value');
//    $.ajax({
//        datatype: "json",
//        type: "POST",
//        url: 'searchTextResults',
//        data: {
//            'typedValue': typedValue, 
//            'domain': domainValue
//        },
//        traditional: true,
//        cache: false,
//        success: function (response) {
////            $('#firstDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 250}]});
//            $("#pageBodyContent").html(response);
//            $('#firstDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 250}]});
//
//
//        }
//    });
//}
function getShowDxpSearchResults(typedValue, domainValue, sortFlag) {
    ajaxStart();
    showLoader();
    $(".mainBookMark").show();
    $(".dxpDefaultHideShow").show();
    $(".dxpClassHideShow").hide();
    $(".dxpGridHideShow").hide();
    $(".dxpFormHideShow").hide();
    $("#intellisense").hide();
    $("#secondDxpSplitter").show();
    $("#thirdDxpSplitter").hide();
    $("#fourthDxpSplitter").hide();
    $('#firstDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 30}]});

    $(".searchFirstResultsList").hide();
    $(".decendingFirstOrder").hide();
    firstPanelShowFlag = true;
    $.ajax({
        datatype: "json",
        type: "POST",
        url: 'showSearchDxpResults',
        data: {
            'typedValue': typedValue,
            'domainValue': domainValue,
            'sortFlag': sortFlag
        },
        traditional: true,
        cache: false,
        success: function (response) {
            stopLoader();
            secondPanelShowFlag = true;
//            getSecondPanelShow(event);

            $("#secondDxpSplitterData").html(response);

            $(".searchDXPCreate").hide();
            $('#secondDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: '100%'}]});

//            $("#secondDxpSplitter").hide();

//            $("#fourthDxpSplitterData").html(response); 

        }
    });
}
function getShowDxpClassSearchResults(className, typedValue, abbr, def, conceptId, unspsc, recordgroup, domainValue) {
    ajaxStart();
    showLoader();
    $("#intellisense").hide();
    $("#thirdDxpSplitter").val('');
    $("#fourthDxpSplitter").val('');
    $("#classConceptId").val(conceptId);
    $("#thirdDxpSplitter").show();
    $('.viewClassDiv').removeClass('active');
    $("#excelExportsearchResults").show();
    var selectedValue = $("#SelectedValue").val();
    $('#thirdDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: '100%'}]});
    $('#secondDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 0}]});
    $(".searchResultsList").hide();
    $(".searchDXPCreate").hide();
    secondPanelShowFlag = true;
//    $('#fourthDxpSplitter').jqxSplitter('collapse');
    $(".searchResultsList").addClass('activeResult');
    $("#currentClass").val(className);
    $("#currentTypedValue").val(typedValue);
    var gridId = '';
    $("#currentDomain").val(domainValue);
    if (domainValue != null && domainValue != '' && domainValue != undefined
            && (domainValue == 'All' || domainValue == 'PRODUCT')) {
        gridId = "DXP_SEARCH_VIEW";
        $("#currentGridId").val(gridId);
    } else if (domainValue != null && domainValue != '' && domainValue != undefined
            && domainValue == 'SERVICE') {
        gridId = "DXP_SM_SEARCH_VIEW";
        $("#currentGridId").val(gridId);
    } else if (domainValue != null && domainValue != '' && domainValue != undefined
            && domainValue == 'VENDOR') {
//        $('#secondDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 30}]});
        gridId = "DXP_VM_SEARCH_VIEW";
        $("#currentGridId").val(gridId);
    }
    $.ajax({
        datatype: "json",
        type: "POST",
        url: 'showSearchDxpClassResults',
        data: {
            'typedValue': typedValue,
            'className': className,
            'gridId': gridId
        },
        traditional: true,
        cache: false,
        success: function (response) {
            ajaxStop();
            stopLoader();
            var resultObj = {};
            resultObj = JSON.parse(response);
            resultObj['className'] = className;
            resultObj['typedValue'] = typedValue;
            var customizeToolbar = resultObj['customizeToolbar'];
            var checkBoxIds = resultObj['checkBoxIds'];
            $("#customizeToolbarData").val(customizeToolbar);
            $("#checkBoxIds").val(checkBoxIds);
            $("#thirdDxpSplitter").val('');
            gridConfig(resultObj, 0, [], "");
            $(".searchDXPCreate").show();
            $("#searchGrid").show();
            $("#searchGrid").css("visibility", "visible");
            $('#secondDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 0}]});
//            $("#thirdDxpSplitter").hide();
            $(".dxpClassHideShow").show();
            $('#thirdDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 1550}]});//31122
            $("#fourthDxpSplitter").hide();
            $("#thirdDxpSplitter").show();
//            $(".mainThirdDxpSplitter").html(resultObj);

        }
    });
}

function creationProcess(url, searchId) {
    var searchId = searchId.toLowerCase();
    searchId = searchId.replace(/^./, searchId => searchId.toUpperCase())
    $('#SearchResult').attr('placeholder', 'Search & ' + searchId);
    keySearch(event);
    $(".searchResultsDiv").hide();
}
function getSearchResults(typedValue, domainValue, sortFlag) {
    var searchValue = $("#secondSplitterSearchId").val();
    $.ajax({
        datatype: "json",
        type: "POST",
        url: 'showSearchDxpResults',
        data: {
            'typedValue': typedValue,
            'domainValue': domainValue,
            'sortFlag': sortFlag,
            'searchValue': searchValue,
            'typeValueChange': 'Y'
        },
        traditional: true,
        cache: false,
        success: function (response) {
            $("#secondDxpSplitterData").html(response);
            $(".searchDXPCreate").hide();
        }
    });
}

function getDxpFilterResults(typedValue, domainValue, sortFlag) {
    $.ajax({
        datatype: "json",
        type: "POST",
        url: 'showFilterSearchDxpResults',
        data: {
            'typedValue': typedValue,
            'domainValue': domainValue,
            'sortFlag': sortFlag,
        },
        traditional: true,
        cache: false,
        success: function (response) {
//            $("#dxpFilterPopOver").show();
            $("#filterDxpResults").html(response);
            $('#dxpFilterPopOver').jqxPopover('open');
            if (resultFlag) {
                $("#dxpFilterPopOver").jqxPopover({
                    offset: {left: -50, top: 0},
                    arrowOffsetValue: 50,
                    title: "Filter Data",
                    showCloseButton: true,
                    selector: $("#Filter"),

                });
                $('#dxpFilterPopOver').jqxPopover('open');
                $("#filterDxpResults").html(response);
                resultFlag = false;

            }
            $(".dxpFilterSearchCheckClass").change(function (event)
            {
                var checkedValues = $(".dxpFilterSearchCheckClass:checked").map(function () {
                    return this.value;
                }).get().join(",");
                if (checkedValues != null && checkedValues != '' && checkedValues != undefined) {
                    getDxpFilterpopupResults(typedValue, domainValue, sortFlag, checkedValues);
                }
            });

//            $("#dxpFilterSearchCheckId").jqxCheckBox({ width: 120, height: 25 });
//              $("#dxpFilterPopOver").show();
//       $("#dxpFilterPopOver").css("display","block",'!important');
        }
    });
}
function getDxpFilterpopupResults(typedValue, domainValue, sortFlag, checkedValues) {
    $.ajax({
        datatype: "json",
        type: "POST",
        url: 'showFilterpopoverDxpResults',
        data: {
            'typedValue': typedValue,
            'domainValue': domainValue,
            'sortFlag': sortFlag,
            'checkedValues': checkedValues
        },
        traditional: true,
        cache: false,
        success: function (response) {
            $("#secondDxpSplitterData").html(response);
            $(".searchDXPCreate").hide();
        }
    });
}
function getFirstPanelShow(event) {
    if (firstPanelShowFlag) {
        $('#firstDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 250}]});
//        $('#fourthDxpSplitter').jqxSplitter('collapse');
        $('.decendingFirstOrder').show();
        $('.mainBookMark').hide();
        $('.searchIconsList').show();
        $('.searchFirstResultsList').show();
        $('#contentDXP_SEARCH_VIEW').show();
        $('#jqxScrollThumbhorizontalScrollBarDXP_SEARCH_VIEW').show();
        $('#pagerDXP_SEARCH_VIEW').show();
        firstPanelShowFlag = false;
    } else {
        $('#firstDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 30}]});
//        $('#fourthDxpSplitter').jqxSplitter('collapse');
        $('.decendingFirstOrder').hide();
        $('.mainBookMark').show();
        $('.searchIconsList').show();
        $('.searchFirstResultsList').hide();
        firstPanelShowFlag = true;
    }

}
function getSecondPanelShow(event) {
    if (secondPanelShowFlag) {
        $('#secondDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 270}]});
//        $('#fourthDxpSplitter').jqxSplitter('collapse');
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
    } else {
        $('#secondDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 0}]});
//        $('#fourthDxpSplitter').jqxSplitter('collapse');
//        $('#fourthDxpSplitter').show();
        $('.searchDXPCreate').hide();
        $('.decendingFirstOrder').hide();
        $('.searchIconsList').show();
        $('.searchResultsList activeResult').hide();
        $('.searchResultMaterialResults').hide();
//        $('#thirdDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: "100%"}]});
        secondPanelShowFlag = true;
    }
}
//function showThirdPanel(event) {
//    if (thirdPanelShowFlag) {
//        $('#thirdDxpSplitter').show();
//        $('#thirdDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 500}]});
////        $('#fourthDxpSplitter').jqxSplitter('collapse');contentDXP_SEARCH_VIEW
//        $('#contentDXP_SEARCH_VIEW').show();
//        $('.decendingOrder').show();
//        $('.decendingOrder').show();
//
//        $('#jqxScrollThumbhorizontalScrollBarDXP_SEARCH_VIEW').show();
//        $('#pagerDXP_SEARCH_VIEW').show();
//        $('.searchIconsList').show();
////        $('#fourthDxpSplitter').hide();
//        thirdPanelShowFlag = false;
//    } else {
//        $('#thirdDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 20}]});
////        $('#thirdDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: "100%"}]});
//        $('#contentDXP_SEARCH_VIEW').hide();
//        $('#pagerDXP_SEARCH_VIEW').hide();
//        $('.decendingFirstOrder').hide();
//        $('.searchIconsList').show();
//        $('#jqxScrollThumbhorizontalScrollBarDXP_SEARCH_VIEW').hide();
//        thirdPanelShowFlag = true;
//    }
//
//}
//function showThirdPanel(event) {
//    if (thirdPanelShowFlag) {
//        setTimeout(resizable, 500);
//        //19122
//        //hiding all 4thpanel elements except the panel button(3 dots)
//        $("#formSplitterDotsId").show();
//        $(".formDxpDuplicates").hide();
//        $(".visionRegistartionGeneric").hide();
//        //20122ThirdPanelOpen
//        $('#thirdDxpSplitter').show();
//        $('#thirdDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 1400}]});/*19122*/
////        $('#fourthDxpSplitter').jqxSplitter('collapse');contentDXP_SEARCH_VIEW
//        $('#contentDXP_SEARCH_VIEW').show();
//        $('.decendingOrder').show();
//        $('.decendingOrder').show();
//        $('#jqxScrollThumbhorizontalScrollBarDXP_SEARCH_VIEW').show();
//        $('#pagerDXP_SEARCH_VIEW').show();
//        $('.searchIconsList').show();
//        thirdPanelShowFlag = false;
//        formPanelShowFlag = true;
//    } else {
//        //ThirdPanelClose
//        $('#thirdDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 20}]});
////        $('#thirdDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', 	splitBarSize: 0, panels: [{size: "100%"}]});
//        $('#contentDXP_SEARCH_VIEW').hide();
//        $('#pagerDXP_SEARCH_VIEW').hide();
//        $('.decendingFirstOrder').hide();
//        $('.searchIconsList').show();
//        $('#jqxScrollThumbhorizontalScrollBarDXP_SEARCH_VIEW').hide();
//        thirdPanelShowFlag = true;
//        formPanelShowFlag = false;
//        //20122FourthPanelShow
//        $(".formDxpDuplicates").show();
//        $(".visionRegistartionGeneric").show();
//    }
//}
function showThirdPanel(event) {
    reqType = $("#dxpAdavanceSearchOptions").val();
    //21122added resizing for the resitory search too

    if (thirdPanelShowFlag) {
        //19122
        //hiding all 4thpanel elements except the panel button(3 dots)
        $("#formSplitterDotsId").show();
        $(".formDxpDuplicates").hide();
        $(".visionRegistartionGeneric").hide();
        //$("#fourthDxpSplitter").prev().css('left', '1550px');

        //20122ThirdPanelOpen
        $('#thirdDxpSplitter').show();
//        $('#thirdDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: '100%'}]});/*19122*/
//        $('#fourthDxpSplitter').jqxSplitter('collapse');contentDXP_SEARCH_VIEW
        $('#thirdDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 1550}]});

        if (reqType == 'PR') {
            $('#contentDXP_REP_SEARCH_VIEW').show();
            $('.repdecendingOrder').show();
            $('.repdecendingOrder').show();

            $('#jqxScrollThumbhorizontalScrollBarDXP_REP_SEARCH_VIEW').show();
            $('#pagerDXP_REP_SEARCH_VIEW').show();

        }
        $('#contentDXP_SEARCH_VIEW').show();
        $('.decendingOrder').show();
        $('.decendingOrder').show();

        $('#jqxScrollThumbhorizontalScrollBarDXP_SEARCH_VIEW').show();
        $('#pagerDXP_SEARCH_VIEW').show();
        $('.searchIconsList').show();
        setTimeout(resizable, 500);
        thirdPanelShowFlag = false;
        formPanelShowFlag = true;
    }
    //ThirdPanelClose


    else {
//        /*24122 MAIN CHANGE*/$('#thirdDxpSplitter').jqxSplitter('collapse');
        $("#toolbarDXP_SEARCH_VIEW").show();
        $('#thirdDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: '0%'}]});
        if (reqType == 'PR') {
            $("#contentDXP_REP_SEARCH_VIEW").hide();
            $(".repSearchResultsList").hide();
            $('#pagerDXP_REP_SEARCH_VIEW').hide();
            $('.repdecendingFirstOrder').hide();
            $('#jqxScrollThumbhorizontalScrollBarDXP_REP_SEARCH_VIEW').hide();
        }
        //$('#thirdDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: "100%"}]});
        $('#contentDXP_SEARCH_VIEW').hide();
        $('#pagerDXP_SEARCH_VIEW').hide();
        $('.decendingFirstOrder').hide();
        $('.searchIconsList').show();
        $('#jqxScrollThumbhorizontalScrollBarDXP_SEARCH_VIEW').hide();
        thirdPanelShowFlag = true;
        formPanelShowFlag = false;
        //20122FourthPanelShow
        $(".formDxpDuplicates").show();
        $(".visionRegistartionGeneric").show();
    }

}
function advancedSearches(event) {
    stopLoader();
    $("#filterDownArrowIconID").show();
    $(".closeSearchFeild").hide();
//    clearTextSearch();
    var domainValue = $('#SelectedValue').val();
    $.ajax({
       dataType: 'html',
        type: "POST",
        url: 'advancedSearches',
        data: {
            'domainValue': domainValue   
        },
        traditional: true,
        cache: false,
        success: function (response) {
            keySearch(event);
            stopLoader();
            $("#filterDownArrowIconID").html(response);
            $("#intellisense").hide();
            $("#dxpAdavanceSearchOptions").focus("");

//            $("#dxpAdavanceSearchOptions").click(); 
//            $(".searchDXPCreate").hide(); 
        }
    });
}
function getParametricResults() {
    var domainValue = $('#SelectedValue').val();
    $.ajax({
        datatype: "json",
        type: "POST",
        url: 'getParametricForm',
        data: {
            'domainValue': domainValue
        },
        traditional: true,
        cache: false,
        success: function (response) {
            $("#searchMainDxpSplitter").show();
            $("#searchDxpSplitter").html(response);

//            keySearch(event);
//         $("#filterDownArrowIconID").html(response);
//         $("#intellisense").hide();
//            $(".searchDXPCreate").hide(); 
        }
    });
}
function getPersonalizationDataOpt(searchType) {
    console.log("searchType:::" + searchType);
    $.ajax({
        type: "post",
        traditional: true,
        cache: false,
        url: "getPersonalizationData",
        data: {
            searchType: searchType,
            searchName: $("#savedSearchName").val()
        },
        success: function (response) {
            ajaxStop();
//            console.log("response:::" + response);
            if (response != null) {
                var typesCheckBox = {};
                $("#personalize_types :input").each(function () {
                    var textid = $(this).attr("id");
                    var type = $(this).attr("type");
                    var textval = $(this).val();
                    if (type != null && type == 'checkbox') {
                        if ($("#" + textid).is(':checked')) {
                            typesCheckBox[textid] = "Y";
                        }
                    }
                });
                $("#settings_panel").html(response);
                if (typesCheckBox != null) {
                    for (var key in typesCheckBox) {
                        $("#" + key).attr('checked', true);
                    }
                }
                $('.visionSearchPersonalise table').each(function () {
                    if ($(this).find('thead').length > 0 && $(this).find('th').length > 0) {
                        // Clone <thead>
                        var $w = $(window),
                                $t = $(this),
                                $thead = $t.find('thead').clone(),
                                $col = $t.find('thead, tbody').clone();
                        // Add class, remove margins, reset width and wrap table
                        $t
                                .addClass('sticky-enabled')
                                .css({
                                    margin: 0,
                                    width: '100%'
                                }).wrap('<div class="sticky-wrap" />');
                        if ($t.hasClass('overflow-y'))
                            $t.removeClass('overflow-y').parent().addClass('overflow-y');
                        // Create new sticky table head (basic)
                        $t.after('<table class="sticky-thead" />');
                        // If <tbody> contains <th>, then we create sticky column and intersect (advanced)
                        if ($t.find('tbody th').length > 0) {
                            $t.after('<table class="sticky-col" /><table class="sticky-intersect" />');
                        }
                        // Create shorthand for things
                        var $stickyHead = $(this).siblings('.sticky-thead'),
                                $stickyCol = $(this).siblings('.sticky-col'),
                                $stickyInsct = $(this).siblings('.sticky-intersect'),
                                $stickyWrap = $(this).parent('.sticky-wrap');
                        $stickyHead.append($thead);
                        $stickyCol
                                .append($col)
                                .find('thead th:gt(0)').remove()
                                .end()
                                .find('tbody td').remove();
                        $stickyInsct.html('<thead><tr><th>' + $t.find('thead th:first-child').html() + '</th></tr></thead>');
                        // Set widths
                        var setWidths = function () {
                            $t
                                    .find('thead th').each(function (i) {
                                $stickyHead.find('th').eq(i).width($(this).width());
                            })
                                    .end()
                                    .find('tr').each(function (i) {
                                $stickyCol.find('tr').eq(i).height($(this).height());
                            });
                            // Set width of sticky table head
                            $stickyHead.width("100%");
//                                        console.log($t.width());                                       
                            // Set width of sticky table col
                            $stickyCol.find('th').add($stickyInsct.find('th')).width($t.find('thead th').width())
                        },
                                repositionStickyHead = function () {
                                    // Return value of calculated allowance
                                    var allowance = calcAllowance();
                                    // Check if wrapper parent is overflowing along the y-axis
                                    if ($t.height() > $stickyWrap.height()) {
                                        // If it is overflowing (advanced layout)
                                        // Position sticky header based on wrapper scrollTop()
                                        if ($stickyWrap.scrollTop() > 0) {
                                            // When top of wrapping parent is out of view
                                            $stickyHead.add($stickyInsct).css({
                                                opacity: 1,
                                                top: $stickyWrap.scrollTop()
                                            });
//                                $(".visionHeaderMain").css("position", "absolute");
                                        } else {
                                            // When top of wrapping parent is in view
                                            $stickyHead.add($stickyInsct).css({
                                                opacity: 0,
                                                top: 0
                                            });
//                                $(".visionHeaderMain").css("position", "fixed");
                                        }
                                    } else {
                                        // If it is not overflowing (basic layout)
                                        // Position sticky header based on viewport scrollTop
                                        if ($w.scrollTop() > $t.offset().top && $w.scrollTop() < $t.offset().top + $t.outerHeight() - allowance) {
                                            // When top of viewport is in the table itself
                                            $stickyHead.add($stickyInsct).css({
                                                opacity: 1,
                                                top: $w.scrollTop() - $t.offset().top
                                            });
//                                $(".visionHeaderMain").css("position", "absolute");

                                        } else {
                                            // When top of viewport is above or below table
                                            $stickyHead.add($stickyInsct).css({
                                                opacity: 0,
                                                top: 0
                                            });
//                                $(".visionHeaderMain").css("position", "fixed");

                                        }
                                    }
                                },
                                repositionStickyCol = function () {
                                    if ($stickyWrap.scrollLeft() > 0) {
                                        // When left of wrapping parent is out of view
                                        $stickyCol.add($stickyInsct).css({
                                            opacity: 1,
                                            left: $stickyWrap.scrollLeft()
                                        });
                                    } else {
                                        // When left of wrapping parent is in view
                                        $stickyCol
                                                .css({opacity: 0})
                                                .add($stickyInsct).css({left: 0});
                                    }
                                },
                                calcAllowance = function () {
                                    var a = 0;
                                    // Calculate allowance
                                    $t.find('tbody tr:lt(3)').each(function () {
                                        a += $(this).height();
                                    });
                                    // Set fail safe limit (last three row might be too tall)
                                    // Set arbitrary limit at 0.25 of viewport height, or you can use an arbitrary pixel value
                                    if (a > $w.height() * 0.25) {
                                        a = $w.height() * 0.25;
                                    }

                                    // Add the height of sticky header
                                    a += $stickyHead.height();
                                    return a;
                                };
                        setWidths();
//                        $t.parent('.sticky-wrap').scroll($.throttle(250, function () {
//                            repositionStickyHead();
//                            repositionStickyCol();
//                        }));
//                        $w
//                                .load(setWidths)
//                                .resize($.debounce(250, function () {
//                                    setWidths();
//                                    repositionStickyHead();
//                                    repositionStickyCol();
//                                }))
//                                .scroll($.throttle(250, repositionStickyHead));
                    }
                });
            }
        },
        error: function (e) {
            sessionTimeout(e);
        }

    });
}
function slideSettings() {
//  $("#settings_panel").toggle(100)
    $('#settings_panel').toggle('slide', {direction: 'right'}, 500);
    $("#personalizeid").toggleClass("ui-icon-triangle-1-s");
    var slideSettingsInd = $("#accordion").attr("data-slidesettingsind");
//    if (slideSettingsInd == 'N') {
//         $("#accordion").attr("data-slidesettingsind","Y");
//        getPersonalizationData();
//    }

    //$('#showcriteria').toggle();
}
function toggleTabs(tabid) {
    console.log("toggling " + tabid);
    $("#" + tabid).toggle();
    $("#" + tabid).next().hide();
}
function getParametricSearchData(searchId) {
    var parametricInd = $("#accordion").attr("data-parametric");
    console.log("parametricInd::::" + parametricInd);
    if (parametricInd == 'N') {
        $("#accordion").attr("data-parametric", "Y");
        getParamForm(searchId, 'paramSearchCover', '');

    }
    getPersonalizationDataOpt('P');
    ajaxStop();
}
function getParamForm(searchId, divId, reqType, searchName) {
    $("#SearchResult").val("");
    $("#SelectedValue").val("All");
    $("#mainDxpSplitter").hide();
    $("#searchDxpSplitter").hide();
    $("#dxpClassficationAppendClass").hide();

    reqType = $("#dxpAdavanceSearchOptions").val();
    if (reqType != null && reqType != undefined && reqType != '' && (reqType == 'PR' || reqType == 'S')) {
        $("#searchDxpSplitter").hide();
        $("#firstDxpSplitter").jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 130}]});
        $("#secondDxpSplitter").jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 270}]});
        $("#thirdDxpSplitter").jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: '100%'}]});
    } else if (reqType != null && reqType != undefined && reqType != '' && reqType == 'C') {
        $("#searchDxpSplitter").css("display", "none");
        $(".visionjqxTreeDiv").show();
        $("#dxpClassficationAppendClass").show();
        getCategoryTreeForm(searchId, reqType);

    } else {
        if (reqType != 'C' || reqType != 'PR' || reqType != 'S') {
            $(".visionjqxTreeDiv").hide();
        }
        $("#dxpClassficationAppendClass").hide();
        $("#searchDxpSplitter").show();
        $("#searchDefaultSplitter").show();
        $("#mainDxpSplitter").hide();
        $("#searchsettingsSplitter").val('');
        $(".dxpDictionaryDotsButton").hide();
        $(".dxpDictionaryFormData").hide();
        $("#searchresultsSplitter").html('');
        $("#searchResults").remove();
        $("#intellisense").hide();
        $("#searchDxpSplitter").css("display", "block");
        $("#searchDxpSplitter").jqxSplitter({
            width: '100%',
            height: '100%',
            orientation: 'vertical',
            splitBarSize: 0,
            panels: [{size: 500}]
        });
        $.ajax({
            type: "POST",
            url: 'getParamSearchForm',
            data: {
                'searchId': searchId,
                'reqType': reqType,
//            searchName: searchName
            },
            traditional: true,
            cache: false,
            success: function (response) {
                $("#dxpClassficationAppendClass").hide();
                $("#searchDxpSplitter").show();
                $(".visionjqxTreeDiv").hide();
//                $("#dxpClassficationAppendClass").hide();
                $("#DxpParamSplitterDotsClass").show();
                if (response != null && response != '') {
                    console.log("divId:::" + divId);
                    var responseObj = JSON.parse(response);
                    getPersonalizationDataOpt(reqType);
                    getClassificationSuggetions();
//                    dxpTreeSearch();

                    $("#searchTypeSplitter").html(responseObj['formString']);
                    var jsDateItems = responseObj['dateArray'];
                    var dateArraySize = responseObj['dateArraySize'];
//                console.log("jsDateItems:::" + dateArraySize);
                    if (dateArraySize != null && dateArraySize != 0) {

                        for (var i = 0; i < jsDateItems.length; i++) {
                            console.log("tbid:::" + jsDateItems[i].tbid);
                            $("#" + jsDateItems[i].tbid).on('change', function () {
                                $(this).focus();
                            })

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
//                                                minDate: $("#pprtbmin" + i).datepicker("getDate")
                                                    });
                                        } else {
                                        }
                                    });
                        }

                    }


                }
            },
            error: function (e) {
                console.log(e);
                // ajaxStop();
                sessionTimeout(e);
            }

        });
    }
}
function getParamSearchResults(reqType, searchId) {
    showLoader();
    var paramArray = [];
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    $("#searchId").val(searchId);
    var i = 0;
    console.log("getParamSearchResults::::" + reqType);

    var tableId = "paramsearch";
    if (reqType == 'ppr' || reqType == 'PRA') {
        tableId = "pprsearch";
    } else if (reqType == 'spec') {
        tableId = "specsearch";
    }


    $("#" + tableId + " tbody tr").each(function () {
        var isAllow = false;
        var paramObj = {};
        var colname = $(this).attr('data-colname')
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
    });


    console.log(":::paramArray:::" + JSON.stringify(paramArray));
    if (paramArray != null && paramArray.length != 0) {
        var searchName = $("#searchddw").val();
        if (reqType == 'ppr' || reqType == 'PRA') {
            searchResults('PRA', '', paramArray);
        } else if (reqType == 'spec') {
            searchResults('M', '', paramArray, '', searchName);
        } else {
            searchResults('P', '', paramArray, '', searchName);
        }
    } else {
        var modalObj = {
            title: 'Message',
            body: "Please provide at least one value to Search."
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
        createModal("dataDxpSplitterValue", modalObj);
        $(".modal-dialog").addClass("opacity-animate3");
    }
}

function searchResults(searchType, reqType, paramsArray, cattype, searchName) {
    showLoader();
    var searchId = $("#searchId").val();
    var isFirstTime = true;
    showLoader();
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    var locale = $("#localedd").find(':selected').data('code');
    if (locale != null && locale != '') {

    } else {
        locale = "";
    }
    var langID = $("#localedd").val() != null ? $("#localedd").val() : "";

    $('#search_count').text("");
    var resultStartTime = new Date().getTime();
    $.ajax({
        type: "POST",
        url: 'genericSearchGrid',
        data: {
            'searchType': searchType,
            'langID': langID,
//            'langID': $("#localedd").val(),
            'locale': locale,
//            'locale': locale,
            searchName: searchName
        },
        traditional: true,
        cache: false,
        success: function (gridResultObj) {
            ajaxStart();
            stopLoader();
//            $("#searchDxpSplitter").jqxSplitter({
//                width: '100%',
//                height: '100%',
//                orientation: 'vertical',
//                splitBarSize: 0,
//                panels: [{size: 400}]
//            });
            if (gridResultObj != null) {
                $("#accordion").accordion({active: parseInt($("#accordion").attr("data-searchresults"))});
//                $("#accordion").accordion({active: 4});
                try {
                    $("#searchResults").jqxGrid("destroy");
                    $("#searchResults").remove();
                } catch (e) {
                }
                $("#searchresultsSplitter").show();
                $("#searchresultsSplitter").html("<div id='searchResults'></div>");
                $("#treeGridDiv").html("<div id='searchResults'></div>");
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
                $("#hrefGridId").val(hrefObj['hrefGridId']);
                var hiddenObj = gridResultObj['hiddenObj'];
                if (hiddenObj != null) {
                    $("#hiddenObj").val(JSON.stringify(hiddenObj));
                }
                var searchButtonObj = gridResultObj['searchButtonObj'];
                if (searchButtonObj != null) {
                    $("#searchButtonObj").val(JSON.stringify(searchButtonObj));
                }
                var gridInitParamObj = {}; //gridInitParamObj
                gridInitParamObj = gridResultObj['gridInitParamObj'];
                if (gridInitParamObj['uuu_smartSearchexportRangeCount'] != null && gridInitParamObj['uuu_smartSearchexportRangeCount'] != '' && (searchType == "S" || searchType == "s")) {
                    $("#ssExportCount").val(gridInitParamObj['uuu_smartSearchexportRangeCount']);
                } else {
                    if (gridInitParamObj['uuu_exportRangeCount'] != null && gridInitParamObj['uuu_exportRangeCount'] != '') {
                        $("#ssExportCount").val(gridInitParamObj['uuu_exportRangeCount']);
                    }
                }
                if (gridResultObj != null && gridResultObj.datafields) {
                }
                var dataFeilds = gridResultObj.datafields;
                var hrefObj = gridResultObj.hrefObj;
                var localData = gridResultObj.data;
                var formId = gridResultObj.formId;
                var panelId = gridResultObj.panelId;
                var gridOperation = gridResultObj.gridOperation;
                $('#formId').val(formId);
                $('#panelId').val(panelId);
                var gridPropObj = {};
                gridPropObj = gridResultObj.gridPropObj;
                var hiddenObj = gridResultObj['hiddenObj'];
                if (hiddenObj != null) {
                    $("#hiddenObj").val(JSON.stringify(hiddenObj));
                }
                var fieldsInitParamObj = gridResultObj['fieldsInitParamObj'];
                if (fieldsInitParamObj != null) {
                    $("#fieldsInitParamObjStr").val(JSON.stringify(fieldsInitParamObj));
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

                    var renderToolbar = gridPropObj.renderToolbar;
                    // console.log("renderToolbar::::"+renderToolbar);
                    //  alert("renderToolbar:::"+renderToolbar);
                    gridPropObj.renderToolbar = eval('(' + renderToolbar + ')');
                    //      var defaultTabName = $("#defaultTabName").val();
                    var descoptrender
                            = function (row, columnfield, value, defaulthtml, columnproperties, rowData) {
                                var element = $(defaulthtml);
                                element.addClass('visionSearchWrapDescrDiv');
                                element.removeProp('overflow');
                                element.css('overflow-y', 'scroll');
                                return element[0].outerHTML;
                            };
                    var searchDateRenderer = function (row, columnfield, value, defaulthtml, columnproperties, rowData) {
                        var cellValue = $("#searchResults").jqxGrid('getcellvalue', row, columnfield);
                        console.log("cellValue::" + cellValue);
                        if (cellValue != null && cellValue != '') {
                            var cellsformat = columnproperties['cellsformat'];
                            if (cellsformat != null && cellsformat != '') {

                            } else {
                                cellsformat = "dd-MM-yyyy HH:mm:ss";
                            }
                            var dateValue = $.jqx.dataFormat.formatdate(value, cellsformat, $("#searchResults").jqxGrid('gridlocalization'));
                            console.log("dateValue:::" + dateValue);
                            cellValue = dateValue;
                        }
                        var element = $(defaulthtml);
                        element.html(cellValue);
                        return element[0].outerHTML;
//                       return '<div style="overflow: hidden; text-overflow: ellipsis; padding-bottom: 2px; text-align: left; margin-right: 2px; margin-left: 4px; margin-top: 15px;">'+cellValue+'</div>';
                        //return cellValue;
                    };
                    for (var i = 0; i < gridPropObj.columns.length; i++) {
                        if (gridPropObj.columns [i].cellsrenderer != null) {
                            gridPropObj.columns [i].cellsrenderer = eval(gridPropObj.columns [i].cellsrenderer);
                        }
                        if (gridPropObj.columns[i].rendered != null) {
                            gridPropObj.columns[i].rendered = eval('(' + gridPropObj.columns[i].rendered + ')');
                        }
                    }
                    if (gridPropObj.rendergridrows != null && gridPropObj.rendergridrows != "") {

                        gridPropObj.rendergridrows = eval('(' + gridPropObj.rendergridrows + ')');
                    }
                    var searchText = $("#result").val() != null ? $("#result").val() : "";
                    $("#colsArrayStr").val(JSON.stringify(gridResultObj['colsArray']));

                    $("#currentSmartSearchData").val(searchText);
                    $("#currentSearchData").val(JSON.stringify(paramsArray));
                    $("#currentSearchType").val(searchType);
                    $("#currentSearchReqType").val(reqType);
                    $("#currentSearchCatType").val(cattype);
                    var source =
                            {
                                type: 'POST',
//                                                async: false,
                                datatype: "json",
                                datafields: dataFeilds,
                                data: {
                                    gridId: gridResultObj['gridId'],
                                    colsArray: JSON.stringify(gridResultObj['colsArray']),
                                    tableName: gridResultObj['tableName'],
                                    searchType: searchType,
                                    reqType: reqType,
                                    searchText: searchText,
                                    'langID': langID,
                                    searchId: searchId,
                                    'cattype': cattype,
                                    'fuzzyFlag': $("#isFuzzy").is(':checked'),
                                    paramsArray: JSON.stringify(paramsArray)

                                },
                                url: 'genericSearchGridResults',
                                cache: false,
                                beforeSend: function () {
                                    ajaxStart();
                                }, loadError: function (xhr, status, error) {
                                    ajaxStop();
                                    throw new Error(error);
                                }, loadComplete: function (data)
                                {
                                    ajaxStop();
                                },
                                downloadComplete: function (data, status, xhr) {
                                    return data;
                                },
                                beforeprocessing: function (data) {
                                    ajaxStart();

                                    if (data[0] != null) {
                                        //  alert(data.JSONObjectList[0].TotalRows);
                                        if (data[0].TotalRows != null) {
                                            source.totalrecords = data[0].TotalRows;
                                            if (isFirstTime) {
                                                isFirstTime = false;
                                                $("#search_count").attr('totalRecords', data[0].TotalRows.toLocaleString());
                                                $("#search_count").html("(" + (labelObject['Fetched'] != null ? labelObject['Fetched'] : 'Fetched') + " "
                                                        + $("#search_count").attr('totalRecords') + " "
                                                        + (labelObject['records in'] != null ? labelObject['records in'] : 'records in') + " "
                                                        + (new Date().getTime() - resultStartTime) / 1000 + " sec)");
                                            }
                                        } else {
                                            source.totalrecords = 0;
//                                            var modalObj = {
//                                                title: 'Message',
//                                                body: data[0],
//                                            };
//                                            var buttonArray = [
//                                                {
//                                                    text: 'Close',
//                                                    click: function () {
//                                                    },
//                                                    isCloseButton: true
//                                                }
//                                            ];
//                                            modalObj['buttons'] = buttonArray;
//                                            createModal("dataDxpSplitterValue", modalObj);
                                        }

                                        ajaxStop();
                                    } else {
                                        $("#search_count").attr('totalRecords', 0);
                                        source.totalrecords = 0;
                                        if (isFirstTime) {
                                            isFirstTime = false;
                                            $("#search_count").html("(" + (labelObject['No record(s) found'] != null ? labelObject['No record(s) found'] : 'No record(s) found') + ")");
                                        }
                                        // $("#search_count").text("(No record(s) found)");
                                        ajaxStop();
                                    }

                                },
                                sort: function ()
                                {
                                    $("#searchResults").jqxGrid('updatebounddata', 'sort');
                                    try {
                                        $("#searchResults").jqxGrid('clearselection');
                                    } catch (e) {
                                    }
                                    ajaxStop();
                                },
                                filter: function () {

                                    $("#searchResults").jqxGrid('updatebounddata', 'filter');
                                    try {
                                        $("#searchResults").jqxGrid('clearselection');
                                    } catch (e) {
                                    }
                                    ajaxStop();
                                }


                            };
                    var dataAdapter = new $.jqx.dataAdapter(source);
                    gridPropObj.source = dataAdapter;
                    gridPropObj.showtoolbar = true;
                    gridPropObj.rowdetails = true;
                    gridPropObj.rendergridrows = function () {
                        return dataAdapter.records;
                    };
                    var renderToolbar = gridPropObj.renderToolbar;
                    gridPropObj.renderToolbar = eval('(' + renderToolbar + ')');
                    gridPropObj.rowdetailstemplate = {
                        rowdetails: "<div style='margin-top:0.2%' class='visionSearchRowDtl'></div>",
                        rowdetailsheight: 32
                    };
                    //autoheight
                    gridPropObj.autoheight = false;
                    //  ajaxStart();
                    gridPropObj.initrowdetails = function (index, parentElement, gridElement, datarecord) {
                        ajaxStart();
                        $.ajax({
                            type: "POST",
                            url: 'fetchRowDetails',
                            data: {
                                'selectedRowData': JSON.stringify(datarecord),
                                searchType: searchType
                            },
                            traditional: true,
                            cache: false,
                            success: function (response) {
                                if (response != null && response != '') {
                                    console.log("response::Row Detail::" + response);
                                    var responseObj = JSON.parse(response);
                                    if (responseObj['rowDetailFlag']) {
                                        //   var dataCopyRecord = datarecord;
                                        var details = $($(parentElement).children()[0]);
                                        console.log("response::rowDetailMessage::" + responseObj['rowDetailMessage']);
                                        details.html('<div class="searchRowDtl">' + responseObj['rowDetailMessage'] + '<div>');
                                        var detailTypes = responseObj['detailTypes'];
                                        var initParamObj = responseObj['initParamObj'];

                                        $("#initParamObj").val(JSON.stringify(initParamObj));
                                        var detailTypesArray = detailTypes.split(",");
                                        var dataRecordConverted = {};
                                        //delete datarecord['ERPSFD'];
                                        //  delete datarecord['PURCHASE'];
                                        for (var key in datarecord) {
                                            if (key != null && key.indexOf("DATE") > -1) {

                                                dataRecordConverted[key] = datarecord[key];
                                            } else {
                                                if (datarecord[key] != null && datarecord[key] != '' && isNaN(datarecord[key])) {
                                                    datarecord[key] = datarecord[key].replace(/&/g, '&amp;');
                                                    datarecord[key] = datarecord[key].replace(/"/g, '&quot;');
                                                    datarecord[key] = datarecord[key].replace(/\\/g, '&bsol;');
                                                    //  .replace(/\\/g, '/');
                                                    for (var entitykey in HtmlEntities) {
                                                        var entity = HtmlEntities[entitykey];
                                                        var regex = new RegExp(entitykey, 'g');
                                                        datarecord[key] = datarecord[key].replace(regex, entity);
                                                    }
                                                    dataRecordConverted[key] = datarecord[key];
                                                } else {
                                                    dataRecordConverted[key] = datarecord[key];
                                                }
                                            }

                                        }
                                        // var dataStr = JSON.stringify(dataRecordConverted);
                                        for (var i = 0; i < detailTypesArray.length; i++) {
                                            if (detailTypesArray[i] != null && detailTypesArray[i] != '') {
                                                var dataDetailObj = responseObj[detailTypesArray[i]];
                                                if (dataDetailObj != null) {
                                                    var functionName = "";
                                                    if (detailTypesArray[i] != 'COPY') {

                                                        functionName = "navigateToFormIcon('" + dataDetailObj['dataField'] + "','" + JSON.stringify(dataRecordConverted) + "','form','" + dataDetailObj['gridId'] + "','" + dataDetailObj['panelId'] + "','" + dataDetailObj['basketType'] + "','" + detailTypesArray[i] + "');";

//                                                        functionName = "navigateToFormIcon('" + dataDetailObj['dataField'] + "','" + JSON.stringify(dataRecordConverted) + "','form','" + dataDetailObj['gridId'] + "','" + dataDetailObj['panelId'] + "','" + dataDetailObj['basketType'] + "');";
                                                    } else {
                                                        $("#copyFormId").val(dataDetailObj['formId']);
                                                        dataRecordConverted['formId'] = dataDetailObj['formId'];
                                                        dataRecordConverted['gridId'] = dataDetailObj['gridId'];
                                                        dataRecordConverted['panelId'] = dataDetailObj['panelId'];
                                                        dataRecordConverted['baskettype'] = dataDetailObj['basketType'];
                                                        dataRecordConverted['objectid'] = dataDetailObj['formId'];
//                                                       
                                                        functionName = "copyItem('" + dataDetailObj['copyId'] + "','" + JSON.stringify(dataRecordConverted) + "');";
                                                        //copyFormId
                                                    }
                                                    $('#' + detailTypesArray[i] + '_' + datarecord['uid']).attr('onClick', functionName);
                                                    if (detailTypesArray[i] == 'SOW') {//onkar SOW
                                                        functionName = "sowDocDownload('" + JSON.stringify(dataRecordConverted) + "','" + dataDetailObj['basketType'] + "')";
                                                        $('#' + detailTypesArray[i] + '_' + datarecord['uid']).attr('onClick', functionName);
                                                    }
                                                }


                                            }

                                        }

                                    } else {
                                        var modalObj = {
                                            title: 'Message',
                                            body: responseObj['rowDetailMessage'],
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
                                        createModal("dataDxpSplitterValue", modalObj);
                                    }
                                }
                                ajaxStop();
                            },
                            error: function (e) {
                                //  alert('Error: ' + JSON.stringify(e));
                                ajaxStop();
                                console.log(e);
                                var meg = e.statusText;
                                var status = e.status;
                            }

                        });
                    };
                    $("#currentGridpageNum").val(0);
                    $('#searchResults').jqxGrid(gridPropObj);
                    $('#searchResults').parent().css("padding-top", "3px", "important");
                    $('#searchResults').parent().css("padding-bottom", "3px", "important");
                    $('#searchResults').jqxGrid('showtoolbar', true);
//                    $('#' + gridResultObj['gridId']).jqxGrid('pagermode', 'simple');
                    $('#searchResults').on('cellclick', function (event) {
//                                    $('#searchResults').bind('cellclick', function (event) {
                        var panelId = $("#panelId").val();
                        console.log(panelId + ":::event.args.column.datafield:::::" + event.args.column.datafield);
                        navigateToForm(event.args.column.datafield, $('#searchResults').jqxGrid('getrowdata', event.args.rowindex), 'form', hrefObj['hrefGridId'], panelId, 'Search View');
                        // navigateToForm(event.args.column.datafield, $('#searchResults').jqxGrid('getrowdata', event.args.rowindex), 'form', gridResultObj['gridId']);
                    });
                    $("#searchResults").on('celldoubleclick', function (event) {
                        var args = event.args;
                        var dataField = args.datafield;
                        var dataField1 = args.text;
                        var rowIndex = args.rowindex;
                        var cellValue = args.value;
                        var isEditable = $('#searchResults').jqxGrid('getcolumnproperty', dataField, 'editable');
                        console.log("isEditable::::" + isEditable)
                        if (!isEditable) {
                            var column = $("#searchResults").jqxGrid('getcolumn', event.args.datafield).text;
                            popupedit(column, cellValue);
                        }

                    });
                    $('#searchResults').on("pagechanged", function (event) {
                        var oldPageNum = $("#currentGridpageNum").val();
                        console.log("oldPageNum:::" + oldPageNum + "::::Current Page Num:::" + event.args.pagenum);
                        // event arguments.
                        var args = event.args;
                        // page number.
                        var pagenum = args.pagenum;
                        // page size.
                        var pagesize = args.pagesize;
                        if (parseInt(event.args.pagenum) != parseInt(oldPageNum)) {
                            var selectedrowindexes = $('#searchResults').jqxGrid('selectedrowindexes');
                            console.log("searchResults:::selectedrowindexes:::" + selectedrowindexes);
                            try {
                                if (selectedrowindexes != null
                                        && selectedrowindexes.length != 0
                                        && selectedrowindexes[0] != -1) {
                                    $('#searchResults').jqxGrid('clearselection');
                                }

                            } catch (e) {
                            }
                        }
                        $("#currentGridpageNum").val(event.args.pagenum);
                    });
                    $('#searchResults').on("pagesizechanged", function (event) {
                        console.log("::pagesizechanged:::" + event.args.pagenum);
                        $("#currentGridpageNum").val(0);
                    });


                }// end if(gridPropObj != null)


            }
        },
        error: function (e) {
            ajaxStop();
            console.log(e);
            sessionTimeout(e);
        }


    });
    //ajaxStop();
    if (parseInt($("#accordion").attr("data-templateresults")) != 0) {
        ajaxStart();
        // fetchTemplateResults(searchType, paramsArray, cattype);
    }

}
function getEnterKeySearch(event, reqType, id, selectType, domain)
{
    if (event.which == 13) {
        if (reqType == 'D') {
            dictionarySearchResults();
        } else if (reqType == 'C')
        {
            classificationSearchDetails();
        } else if (reqType == 'I') {

            categoryClassSearch(id, selectType, domain);
        }
        //console.log("reqType is :::" + reqType);
        else {
            getParamSearchResults(reqType);
        }
    }

}
function updatePersonalize(tbid) {
    console.log("tbid::" + tbid);
    console.log("datacol:::" + $("#" + tbid).attr("data-col"));
    if (tbid.toString().indexOf('search') > -1) {
        if ($("#" + tbid).prop('checked')) {
            $("#P" + $("#" + tbid).attr("data-col")).show();
        } else {
            $("#P" + $("#" + tbid).attr("data-col")).hide();
        }

    } else if (tbid.toString().indexOf('display') > -1) {
        var columnName = tbid.toString().replace("_display", "");
        try {

            if ($("#" + tbid).is(':checked')) {
                console.log("showing;;;" + columnName);
                $('#searchResults').jqxGrid('showcolumn', columnName);
            } else {

                $('#searchResults').jqxGrid('hidecolumn', columnName);
            }

        } catch (e) {

        }
        //   $("#" + $("#" + tbid).attr("data-col")).toggle();

    } else if (tbid.toString().indexOf('freeeze') > -1) {
        var columnName = tbid.toString().replace("_freeeze", "");
        console.log("pinning column::" + columnName);

        try {
            if ($("#" + tbid).is(":checked")) {
                $('#searchResults').jqxGrid('pincolumn', columnName);
            } else {
                $('#searchResults').jqxGrid('unpincolumn', columnName);
            }
        } catch (e) {
        }
    }
}
function classificationSearchDetails() {
    ajaxStart();
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {
        labelObject = [];
    }
    var classificationTypeLbl = $("#classificationtype").find(':selected').data('optlabel');
    $.ajax({
        type: "GET",
        url: 'classificationSearch',
        data: {
            'classificationCode': $('#classificationcode').val(),
            'classTerm': $("#classficationtext").val(),
            'classificationType': $('#classificationtype').val()
        },
        traditional: true,
        success: function (gridResultObj) {
            $('#searchresultsSplitter').html("");
            $('#searchresultsSplitter').append("<div id='classificationinfo' style='opacity:0.99 !important'></div>");
            $("#searchDxpSplitter").jqxSplitter({
                width: '100%',
                height: '100%',
                orientation: 'vertical',
                splitBarSize: 0,
                panels: [{size: 300}]
            });
            if (gridResultObj != null) {
                if (gridResultObj != null) {
                    try {
                        $("#classificationinfo").jqxGrid("destroy");
                        $("#classificationinfo").remove();
                    } catch (e) {
                    }
//                    $("#searchresultsSplitter").html("<div id='searchResults'></div>");
                    $("#searchresultsSplitter").html("<div id='classificationinfo' style='opacity:0.99 !important'></div>");
                    //need to assign all hidden fields like hrefColumn,linkedColumns,stripValue,imageColumn,imageTable,imageTableColumn
                    var hrefObj = {}; //hrefObj
                    hrefObj = gridResultObj['hrefObj'];

                    if (gridResultObj != null && gridResultObj.datafields) {

                        var imagerenderer = function (row, datafield, value) {

                            return '<img src="" id="ind' + row + '" class="indimage"><label id="imgLabel' + row + '" class="indimage">Show Image</label>';
                        };
                    }
                    var dataFeilds = gridResultObj.datafields;
                    // var hrefObj = gridResultObj.hrefObj;
                    var localData = gridResultObj.data;
                    var formId = gridResultObj.formId;
                    var panelId = gridResultObj.panelId;
                    var gridOperation = gridResultObj.gridOperation;
                    var gridPropObj = {};
                    gridPropObj = gridResultObj.gridPropObj;
                    if (gridPropObj != null) {
                        gridPropObj.columns = gridResultObj.columns;
                        var headerTooltipRenderer = function (element) {
                            $(element).parent().jqxTooltip({position: 'mouse',
                                position: 'bottom-right',
                                showArrow: false, content: $(element).text()});
                        }

                        var renderToolbar = gridPropObj.renderToolbar;

                        gridPropObj.renderToolbar = eval('(' + renderToolbar + ')');
                        //      var defaultTabName = $("#defaultTabName").val();


                        var descrender
                                = function (row, columnfield, value, defaulthtml, columnproperties) {
                                    //return '<textarea readonly class="ta_style" rows=1 >' + value + '</textarea>';
                                    console.log("hiiiii");
                                    return '<div style="height:' + $('#classificationinfo').jqxGrid('rowsheight') + 'px" class="ta_style" rows=1 >' + value + '</div>';
                                };
                        var classTermRender
                                = function (row, columnfield, value, defaulthtml, columnproperties) {
                                    //return '<textarea readonly class="ta_style" rows=1 >' + value + '</textarea>';
                                    console.log("hiiiii");
                                    return '<div style="cursor:pointer;" class="vend_style">' + value + '</div>';
                                };
                        var descriptorImageRenderer = function (row, columnfield, value, defaulthtml, columnproperties) {
                            return  "<img title='Click to create a record' style='cursor:pointer;'  src='" + value + "' class='imageStyle visionTemplete' data-count='" + $('#classificationinfo').jqxGrid('pagesize') + "' id='dtlul_"
                                    + row + "' onmouseover=templeteMouseOver('dtlul_" + row + "'," + $("#classificationinfo").jqxGrid("pagesize") + ") onmouseout=templeteMouseOut('dtlul_" + row + "'," + $("#classificationinfo").jqxGrid("pagesize") + ")>";
                        };
                        var gridDrpdownRenderor = function (row, columnfield, value, defaulthtml, columnproperties) {
                            var cellValue = $("#classificationinfo").jqxGrid('getcellvalue', row, columnfield);
                            var viewType = "GRID-VIEW";
                            var ddwData = gridResultObj.dropDowndData;
                            var ddwObj = ddwData[columnfield];
                            var dependencyparams = ddwObj.dependencyparams;
                            var editable = gridPropObj.editable;
                            if (editable) {
                                return "<div class='visionGridDataAlign'><div class='visionGridDataAlignInfo'>" + cellValue + "</div><div class='visionGridDataAlignImage'><img id='dd" + "classificationinfo" + columnfield + "' src='images/search_icon_color_2.png'  onclick=visionDropdown('" + ddwObj.ddwId.trim() + "','" + dependencyparams + "','" + viewType + "','" + ddwObj.gridId + "','" + columnfield + "','" + row + "')></div></div>";
//                                return "<div class='visionGridDataAlign'><div class='visionGridDataAlignInfo'>" + cellValue + "</div><div class='visionGridDataAlignImage'><img src='images/search_icon_color_2.png' onclick=visionDropdown('" + ddwObj.ddwId.trim() + "','" + dependencyparams + "','" + viewType + "','" + ddwObj.gridId + "','" + columnfield + "','" + row + "')></div></div>";
                            } else
                            {
                                return "<div class='visionGridDataAlign'>" + cellValue + "</div>";
                            }

                        };
                        var viewParentRenderer = function (row, columnfield, value, defaulthtml, columnproperties) {
                            var codifcode = $('#classificationinfo').jqxGrid('getcellvalue', row, 'CODE');
                            console.log("viewParentRenderer:::" + codifcode);
                            return "<div class='visionGridDataAlign' style='text-align:center;cursor:pointer;'><input id='fetchtree" + codifcode + "' type='checkbox' style='width:15px;  height: 17px; margin-top: 5px;' onclick=onChecked(" + codifcode + ",'fetchtree" + codifcode + "') > </div>";
                        };
                        var descriptorrenderer = function (row, columnfield, value, defaulthtml, columnproperties) {
                            var descriptorClass = $('#classificationinfo').jqxGrid('getcellvalue', row, "TERM");
                            //  console.log("Descriptor Class:" + descriptorClass);
                            var descriptorClass1 = descriptorClass.replace(/\s/g, "+");
                            var conceptid = $('#classificationinfo').jqxGrid('getcellvalue', row, "CONCEPT_ID");
                            console.log("conceptid:descriptorrenderer:" + conceptid);
                            return "<div class='visionGridDataAlign' style='text-align: left;text-decoration: underline;cursor:pointer' onclick=fetchDescriptorMaterials('" + descriptorClass1 + "','" + conceptid + "')>" + descriptorClass + " </div>";
                        };
                        for (var i = 0; i < gridPropObj.columns.length; i++) {
                            if (gridPropObj.columns [i].cellsrenderer != null) {
                                gridPropObj.columns [i].cellsrenderer = eval(gridPropObj.columns [i].cellsrenderer);
                            }
                            if (gridPropObj.columns[i].rendered != null) {
                                gridPropObj.columns[i].rendered = eval('(' + gridPropObj.columns[i].rendered + ')');
                            }
                        }
                        if (gridPropObj.rendergridrows != null && gridPropObj.rendergridrows != "") {

                            gridPropObj.rendergridrows = eval('(' + gridPropObj.rendergridrows + ')');
                        }
                        if (gridPropObj.rowsheight != null) {
                            gridPropObj.rowsheight = parseInt(gridPropObj.rowsheight);
                            //  gridPropObj.autorowheight = true;
                            $('.show_detail').css('height', parseInt(gridPropObj.rowsheight + "px"));
                        } else {
                            $('.show_detail').css('height', '25px');
                        }

                        var source =
                                {
                                    type: 'POST',
                                    datatype: "json",
                                    datafields: dataFeilds,
                                    data: {
                                        gridId: gridResultObj['gridId'],
                                        colsArray: JSON.stringify(gridResultObj['colsArray']),
                                        tableName: gridResultObj['tableName'],
                                        'classificationCode': $('#classificationcode').val(),
                                        'classTerm': $("#classficationtext").val(),
                                        'classificationType': $('#classificationtype').val(),
                                        classificationTypeLbl: classificationTypeLbl

                                    },
                                    url: 'getClassificationSearchresults',
                                    cache: false,
                                    beforeSend: function () {
                                        ajaxStart();
                                    }, loadError: function (xhr, status, error) {
                                        ajaxStop();
                                        throw new Error(error);
                                    }, loadComplete: function (data)
                                    {
                                        ajaxStop();
                                    },
                                    beforeprocessing: function (data) {
                                        ajaxStart();
                                        if (data[0] != null) {
                                            //  alert(data.JSONObjectList[0].TotalRows);
                                            source.totalrecords = data[0].TotalRows;
                                        } else {
//                                        $("#search_count").attr('totalRecords', 0);
                                            source.totalrecords = 0;
                                        }
                                        try {
//                                            $("#classificationinfo").jqxGrid('clearselection');
                                        } catch (e) {
                                        }

                                        ajaxStop();
                                    },
                                    sort: function ()
                                    {
                                        $("#classificationinfo").jqxGrid('updatebounddata', 'sort');
                                        try {
                                            $("#classificationinfo").jqxGrid('clearselection');
                                        } catch (e) {
                                        }

                                    },
                                    filter: function () {

                                        $("#classificationinfo").jqxGrid('updatebounddata', 'filter');
                                        try {
                                            $("#classificationinfo").jqxGrid('clearselection');
                                        } catch (e) {
                                        }
                                    }


                                };

                        var dataAdapter = new $.jqx.dataAdapter(source);
                        gridPropObj.source = dataAdapter;
                        gridPropObj.showtoolbar = false;
                        gridPropObj.rowdetails = false;
                        gridPropObj.rendergridrows = function () {
                            return dataAdapter.records;
                        };
                        gridPropObj.rendertoolbar = function (toolbar) {
                            // appends buttons to the tool bar.
                            var container = $("<div style='overflow: hidden; position: relative; margin: 5px;'></div>");
//                        var buttonTemplate = "<div  class='visionRefreshIndexBtn' style='float: left; padding: 3px; margin: 2px;'></div>";

                            var reloadButton = $("<div style='float: left;margin-left: 5px;'><img style='position: relative;width:16px;height:16px; margin-top: 2px;' src='images/refresh_icon.png'/></div>");
                            container.append(reloadButton);
                            // reload grid data.
                            reloadButton.click(function () {
//                                    $('#classificationinfo').jqxGrid({source: dataAdapter});
//                                    $("#" + gridResultObj['gridId']).jqxGrid('updatebounddata', 'cells');
                                $("#classificationinfo").jqxGrid('clearselection');
                                $("#classificationinfo").jqxGrid('clearfilters');
                            });
                            reloadButton.jqxButton({cursor: "pointer", enableDefault: false});
//                    reloadButton.find('div:first').addClass("visionRefreshIndexBtn");
                            reloadButton.jqxTooltip({position: 'bottom', content: "Refresh"});
                            //toolbar.append(container);
                        };
//                        gridPropObj.enabletooltips = false;
                        gridPropObj.cellhover = function (element, pageX, pageY)
                        {

                            var cell = $('#classificationinfo').jqxGrid('getcellatposition', pageX, pageY);
                            //\\alert("hello"+cell.row);
                            var datainformation = $('#classificationinfo').jqxGrid('getdatainformation');
                            var paginginformation = datainformation.paginginformation;
                            var rowscount = paginginformation.pagesize;
                            var pagenum = paginginformation.pagenum;
                            var cellRow = cell.row;
//                              alert(cellRow);
                            if (cellRow >= rowscount && pagenum > 0)
                            {
                                var cellvalue = rowscount * pagenum;
                                if (cellRow == rowscount)
                                {
                                    cellRow = (cellRow - cellvalue);
                                } else
                                {
                                    cellRow = (cellRow - cellvalue);
                                }
                                console.log(cellRow + "cellRow");
                            } else
                            {
                                cellRow = cellRow;
                            }
                        };
                        gridPropObj.rowdetails = false;
                        gridPropObj.autorowheight = false;
                        $('#classificationinfo').jqxGrid(gridPropObj);
                        $('#classificationinfo').parent().css("padding-top", "3px", "important");
                        $('#classificationinfo').parent().css("padding-bottom", "3px", "important");
                        $('#classificationinfo').jqxGrid('showtoolbar', false);

                    }// end if(gridPropObj != null)
                }
            }
        },
        error: function (e) {
            console.log(e);
            ajaxStop();
            sessionTimeout(e);
        }
    });
}
function getClassificationSuggetions() {
    $('.ui-autocomplete').html('');
    $(document).ready(function ()
    {
        $("#classficationtext").autocomplete(
                {
                    source: function (request, response) {
                        ajaxStart();
                        $.ajax({
                            url: "descriptorSuggestions",
                            dataType: "json",
                            data: {
                                term: $("#classficationtext").val()
                            },
                            success: function (item) {
                                ajaxStop();
                                response(item);
                            }
                        });
                    },
//                source: "descriptorSuggestions?searchLimit=15",
                    minLength: 2,
                    params: {
                        'classification': function () {
                            return $('#classificationtype').val();
                        }},
                    create: function () {
                        $(this).data('ui-autocomplete')._renderItem = function (ul, item) {
                            imageid = parseInt(imageid) + 1;
                            return $('<li>')
                                    .append("<table style='color:inherit;background-color:inherit;width:100%;border:none'><tr><td style='width:70%;color:inherit;background-color:inherit;'> " + item.value + "</td><td style='width:30%'> <img id='img" + imageid + "' "
                                            + " onmouseover='templeteMouseOver(id)' onmouseout=templeteMouseOut(id)  style='color:inherit;background-color:inherit;height:40px;width:40px' src='" + item.content + "'/></td></tr></table>")
                                    .appendTo(ul);
                        };
                    },
                    open: function () {
                        $('.ui-autocomplete').css('width', '400px'); // HERE
                        $('.ui-autocomplete').addClass('repositoryAutoCompleteClass');
                    },
                    select: function (event, ui) {
                        $("#classficationtext").attr("data-conceptid", $.trim(ui.item.termid));
                        onClassificationchng();
                    }
                });
        $("#classificationcode").autocomplete(
                {
                    source: function (request, response) {
                        ajaxStart();
                        $.ajax({
                            url: "classificationSuggestions",
                            dataType: "json",
                            data: {
                                classification: $('#classificationtype').val(),
                                term: $("#classificationcode").val()
                            },
                            success: function (item) {
                                ajaxStop();
                                response(item);
                            }
                        });
                    },
//                source: "classificationSuggestions?searchLimit=15&classification=" + $('#classificationtype').val(),
                    minLength: 2,
                    select: function (event, ui) {
                        console.log("selectedValue::: " + $.trim(ui.item.value));
                        console.log("Term Id::: " + $.trim(ui.item.termid));
                        console.log("Term Id::: " + $.trim(ui.item.term));
                        //  console.log("classificationdesc Id::: " + $.trim(ui.item.classificationdesc));
                        $('#classficationtext').val($.trim(ui.item.term));
                        $('#classficationtext').attr('data-conceptid', $.trim(ui.item.termid));
                        $('#classificationcode').val($.trim(ui.item.value));
                        $("#classificationdesc").val($.trim(ui.item.classificationdesc));
                    }
                });
        $("#dictionarytext").autocomplete(
                {
                    source: function (request, response) {
                        ajaxStart();
                        $.ajax({
                            url: "descriptorSuggestions",
                            dataType: "json",
                            data: {
                                term: $("#dictionarytext").val()
                            },
                            success: function (item) {
                                ajaxStop();
                                response(item);
                            }
                        });
                    },
//            {source: "descriptorSuggestions?searchLimit=15",
                    minLength: 2,
                    create: function () {
                        $(this).data('ui-autocomplete')._renderItem = function (ul, item) {
                            $(".clear_input").show();
                            // console.log(JSON.stringify(item));
                            imageid = parseInt(imageid) + 1;
                            return $('<li>')
                                    .append("<table style='color:inherit;background-color:inherit;width:100%;border:none'><tr><td style='width:70%;color:inherit;background-color:inherit;'> " + item.value + "</td><td style='width:30%'> <img id='img" + imageid + "' "
                                            // +" onmouseover='onAcImageFocus(id)'"
                                            + " onmouseover='templeteMouseOver(id)' onmouseout=templeteMouseOut(id)  style='color:inherit;background-color:inherit;width:40px;height:40px' src='" + item.content + "'/></td></tr></table>")
                                    .appendTo(ul);

                        };
                    },
                    select: function (event, ui) {
                        console.log("selectedValue::: " + $.trim(ui.item.value));
                        console.log("Term Id::: " + $.trim(ui.item.termid));
                        $("#dictionarytext").attr("data-conceptid", $.trim(ui.item.termid));
                        getPropertiesByClassName(ui.item.value, ui.item.termid);
                    }
                });
        $("#categorytextfield").keydown(function (e) {
            $(".clear_category_input").show();
            var imageid = 0;
            $("#categorytextfield").autocomplete({
                source: function (request, response) {
                    $.ajax({
                        url: "categorySearchSuggestions",
                        dataType: "json",
                        data: {
                            categorytextfieldval: $('#categorytextfieldval').val(),
                            disciplinetextfieldval: $('#disciplinetextfieldval').val(),
                            subDisciplinetextfieldval: $('#subDisciplinetextfieldval').val(),
                            term: $("#categorytextfield").val()
                        },
                        success: function (item) {
                            response(item);
                        }
                    });
                },
                minLength: 2,
                create: function () {
                    $(this).data('ui-autocomplete')._renderItem = function (ul, item) {
                        $(".clear_category_input").show();
                        imageid = parseInt(imageid) + 1;
                        return $('<li>')
                                .append("<table style='color:inherit;background-color:inherit;width:100%;border:none'><tr><td style='width:70%;color:inherit;background-color:inherit;'> " + item.value + "</td><td style='width:30%'> <img id='img" + imageid + "' "
                                        + " onmouseover='templeteMouseOver(id)' onmouseout=templeteMouseOut(id)  style='color:inherit;background-color:inherit;width:40px;height:40px' src='" + item.content + "'/></td></tr></table>")
                                .appendTo(ul);

                    };
                },
                select: function (event, ui) {
                    console.log("selectedValue::: " + $.trim(ui.item.value));
                    console.log("Term Id::: " + $.trim(ui.item.termid));
                    $("#categorytextfield").attr("data-conceptid", $.trim(ui.item.termid));
                    categoryTextSearchResults(ui.item.value);
                }
            });
        });

    });

}
function templeteMouseOver(result1, result2)
{
    var templeteId = result1;
    var imagePath = $("#" + templeteId).attr("src");
    console.log(templeteId + 'templeteId');
    $("#visionTempleteHoverImage").empty();
//    console.log(imagePath + 'imagePath');
    var imagePaths = "<span><img src='" + imagePath + "'></span>";
    $("#visionTempleteHoverImage").show();
    $("#visionTempleteHoverImage").append(imagePaths);
}
function templeteMouseOut(result1, result2)
{
    $("#visionTempleteHoverImage").empty();
    $("#visionTempleteHoverImage").hide();
}
function onClassificationchng() {
    ajaxStart();
    var classification = $("#classificationtype").val();
    var conceptId = $("#classficationtext").attr('data-conceptid');
    $("#classificationcode").autocomplete("option", "source",
            "classificationSuggestions?searchLimit=15&classification=" + $('#classificationtype').val());
    $.ajax({
        type: "GET",
        url: 'getDescClassification',
        data: {
            conceptId: conceptId,
            classification: classification
        },
        traditional: true, cache: false,
        success: function (response) {
            $("#classificationcode").val(response.code);
            $("#classificationdesc").val(response.desc);
            ajaxStop();
        },
        error: function (e) {
            console.log(e);
            ajaxStop();
            sessionTimeout(e);
        }

    });
}
function fetchDescriptorMaterials(descriptor, descriptorid) {
    ajaxStart();
    var dataObject = {};
    var dataArray = [];
    var dataObject = {};
    dataObject.property = "";
    dataObject.propertyid = "";
    dataObject.datatype = "";
    dataObject.value = "";
    dataObject.uom = "";
    dataObject.range = "";
    dataObject.minvalue = "";
    dataObject.maxvalue = "";
    // var conceptId = $("#dictionarytext").attr("data-conceptid");
    if (descriptorid != null && descriptorid != '') {
        dataObject.conceptId = descriptorid;
        dataObject.classTerm = descriptor.toUpperCase().replace(/\+/g, " ");
    } else {
        dataObject.conceptId = descriptorid;
        dataObject.classTerm = descriptor.toUpperCase().replace(/\+/g, " ") + "%";
    }

    dataArray.push(dataObject);
    searchResults('D', '', dataArray, '');

}
function dictionarySearchResults() {
    ajaxStart();
    var dataArray = [];
    var classTerm = $("#SearchResult").val();
    if (classTerm != null && classTerm != '') {
        var i = 0;
        $("#dictionarytbl tbody tr").each(function () {
            console.log("$(#propval + i).val():::" + $("#propval" + i).val());
            console.log("$(#propval + i).val():::" + $("#propuom" + i).val());
            if (!($.trim($("#propval" + i).val()) == "" && $.trim($("#propuom" + i).val() == ""))) {
                var dataObject = {};
                dataObject.property = $.trim($(this).find("td:eq(0)").text());
                dataObject.propertyid = $.trim($("#propertyid" + i).attr("data-propertyid"));
                dataObject.datatype = $.trim($(this).find("td:eq(1)").text());
                dataObject.value = $.trim($("#propval" + i).val());
                dataObject.uom = $.trim($("#propuom" + i).val());
                dataObject.range = $.trim($("#trdic" + i).attr('data-isrange'));
                dataObject.minvalue = $.trim($("#propminval" + i).val());
                dataObject.maxvalue = $.trim($("#propmaxval" + i).val());
                dataObject.conceptId = $("#dictionarytext").attr("data-conceptid");
                dataObject.classTerm = classTerm.toUpperCase();
                dataObject.valueOp = $("#dictddw" + i).val();
                dataArray.push(dataObject);
            }
            i++;
        });
        if (dataArray.length == 0) {
            var dataObject = {};
            dataObject.property = "";
            dataObject.propertyid = "";
            dataObject.datatype = "";
            dataObject.value = "";
            dataObject.uom = "";
            dataObject.range = "";
            dataObject.minvalue = "";
            dataObject.maxvalue = "";
            var conceptId = $("#SearchResult").attr("data-conceptid");
            if (conceptId != null && conceptId != '') {
                dataObject.conceptId = $("#SearchResult").attr("data-conceptid");
                dataObject.classTerm = classTerm.toUpperCase();
            } else {
                dataObject.conceptId = $("#SearchResult").attr("data-conceptid");
                dataObject.classTerm = classTerm.toUpperCase() + "%";
            }

            dataArray.push(dataObject);
        }
        console.log("dataArray:::" + JSON.stringify(dataArray));
        searchResults('D', '', dataArray, '');
    } else {
        searchResults('D', '', dataArray, '');
        console.log("Class Term Empty");
    }

}
function dictionaryreset() {
    $("#dictionarytext").val("");
    $("#classProperties").html("");
    $("#dictionarytext").attr("data-conceptid", "");
    $(".clear_input").hide();
    ajaxStop();
}
function resetDictionarySearch() {
    $("#dictionarytbl tbody tr").each(function () {
        $(this).find('td').each(function () {
            if ($(this).find("input[type=text]").length > 0) {
                $(this).find("input[type=text]").val("");
            }
            if ($(this).find("select").length > 0) {
                $(this).find("select").val("EQUALS");
            }
        });
    });
}
function getPropertiesByClassName(classTerm, conceptId, domainValue) {

    $("#searchresultsSplitter").html("");
    console.log("classTerm:::" + classTerm + "::conceptId:::" + conceptId);
    if (conceptId != null && conceptId != '') {
        ajaxStart();
        $.ajax({
            type: "POST",
            url: 'descriptorProperties',
            data: {
                conceptId: conceptId,
                classTerm: classTerm,
                domainValue: domainValue,
            },
            traditional: true,
            cache: false,
            success: function (response) {
                var responceObj = JSON.parse(response);
                console.log('Response::' + response);
                $(".dxpDictionaryFormClassData").html(responceObj['formResult']);
                $("#searchresultsSplitter").html(responceObj['nextPageResult']);
                $("#classProperties").html(responceObj['result']);
                $('.dictionaryclasssearch table').each(function () {

                    if ($(this).find('thead').length > 0 && $(this).find('th').length > 0) {
                        // Clone <thead>
                        var $w = $(window),
                                $t = $(this),
                                $thead = $t.find('thead').clone(),
                                $col = $t.find('thead, tbody').clone();
                        // Add class, remove margins, reset width and wrap table
                        $t
                                .addClass('sticky-enabled')
                                .css({
                                    margin: 0,
                                    width: '100%'
                                }).wrap('<div class="sticky-wrap" />');
                        if ($t.hasClass('overflow-y'))
                            $t.removeClass('overflow-y').parent().addClass('overflow-y');
                        // Create new sticky table head (basic)
                        $t.after('<table class="sticky-thead" />');
                        // If <tbody> contains <th>, then we create sticky column and intersect (advanced)
                        if ($t.find('tbody th').length > 0) {
                            $t.after('<table class="sticky-col" /><table class="sticky-intersect" />');
                        }

                        // Create shorthand for things
                        var $stickyHead = $(this).siblings('.sticky-thead'),
                                $stickyCol = $(this).siblings('.sticky-col'),
                                $stickyInsct = $(this).siblings('.sticky-intersect'),
                                $stickyWrap = $(this).parent('.sticky-wrap');
                        $stickyHead.append($thead);
                        $stickyCol
                                .append($col)
                                .find('thead th:gt(0)').remove()
                                .end()
                                .find('tbody td').remove();
                        $stickyInsct.html('<thead><tr><th>' + $t.find('thead th:first-child').html() + '</th></tr></thead>');
                        // Set widths
                        var setWidths = function () {
                            $t
                                    .find('thead th').each(function (i) {
                                $stickyHead.find('th').eq(i).width($(this).width());
                            })
                                    .end()
                                    .find('tr').each(function (i) {
                                $stickyCol.find('tr').eq(i).height($(this).height());
                            });
                            // Set width of sticky table head
                            $stickyHead.width("100%");
//                                        console.log($t.width());                                       
                            // Set width of sticky table col
                            $stickyCol.find('th').add($stickyInsct.find('th')).width($t.find('thead th').width())
                        },
                                repositionStickyHead = function () {
                                    // Return value of calculated allowance
                                    var allowance = calcAllowance();
                                    // Check if wrapper parent is overflowing along the y-axis
                                    if ($t.height() > $stickyWrap.height()) {
                                        // If it is overflowing (advanced layout)
                                        // Position sticky header based on wrapper scrollTop()
                                        if ($stickyWrap.scrollTop() > 0) {
                                            // When top of wrapping parent is out of view
                                            $stickyHead.add($stickyInsct).css({
                                                opacity: 1,
                                                top: $stickyWrap.scrollTop()
                                            });
                                            $(".visionHeaderMain").css("position", "absolute");
                                        } else {
                                            // When top of wrapping parent is in view
                                            $stickyHead.add($stickyInsct).css({
                                                opacity: 0,
                                                top: 0
                                            });
                                            $(".visionHeaderMain").css("position", "fixed");
                                        }
                                    } else {
                                        // If it is not overflowing (basic layout)
                                        // Position sticky header based on viewport scrollTop
                                        if ($w.scrollTop() > $t.offset().top && $w.scrollTop() < $t.offset().top + $t.outerHeight() - allowance) {
                                            // When top of viewport is in the table itself
                                            $stickyHead.add($stickyInsct).css({
                                                opacity: 1,
                                                top: $w.scrollTop() - $t.offset().top
                                            });
                                            $(".visionHeaderMain").css("position", "absolute");
                                        } else {
                                            // When top of viewport is above or below table
                                            $stickyHead.add($stickyInsct).css({
                                                opacity: 0,
                                                top: 0
                                            });
                                            $(".visionHeaderMain").css("position", "fixed");
                                        }
                                    }
                                },
                                repositionStickyCol = function () {
                                    if ($stickyWrap.scrollLeft() > 0) {
                                        // When left of wrapping parent is out of view
                                        $stickyCol.add($stickyInsct).css({
                                            opacity: 1,
                                            left: $stickyWrap.scrollLeft()
                                        });
                                    } else {
                                        // When left of wrapping parent is in view
                                        $stickyCol
                                                .css({opacity: 0})
                                                .add($stickyInsct).css({left: 0});
                                    }
                                },
                                calcAllowance = function () {
                                    var a = 0;
                                    // Calculate allowance
                                    $t.find('tbody tr:lt(3)').each(function () {
                                        a += $(this).height();
                                    });
                                    // Set fail safe limit (last three row might be too tall)
                                    // Set arbitrary limit at 0.25 of viewport height, or you can use an arbitrary pixel value
                                    if (a > $w.height() * 0.25) {
                                        a = $w.height() * 0.25;
                                    }

                                    // Add the height of sticky header
                                    a += $stickyHead.height();
                                    return a;
                                };
                        setWidths();
//                        $t.parent('.sticky-wrap').scroll($.throttle(250, function () {
//                            repositionStickyHead();
//                            repositionStickyCol();
//                        }));
//                        $w.load(setWidths)
//                                .resize($.debounce(250, function () {
//                                    setWidths();
//                                    repositionStickyHead();
//                                    repositionStickyCol();
//                                }))
//                                .scroll($.throttle(250, repositionStickyHead));
                    }
                });
                ajaxStop();
//                dictionarySearchResults();
            },
            error: function (e) {
                console.log(e);
                ajaxStop();
                sessionTimeout(e);
            }

        });
    }
}
function handleClick(typeOfCategory, searchType) {
    ajaxStart();
    console.log("typeOfCategory:::" + typeOfCategory + "::::searchType:::" + searchType);
    $("#navigationIcons").hide();
    $("#categorytextfield").hide();
    $("#categorytextfield").val('');
    $(".clear_category_input").hide();
    $("#categorysearchresult").hide();
    if (typeOfCategory != null && typeOfCategory != '') {
        $.ajax({
            type: "POST",
            url: 'getCategoryData',
            data: {
                'typeOfCategory': typeOfCategory,
                'searchType': searchType
            },
            traditional: true, cache: false,
            success: function (response) {
//                console.log('Response::' + response);
                if (response != '') {
                    categoryAutoComplete();
                    var categoryObj = JSON.parse(response);
                    $("#categorysrchnvgn").html(categoryObj['breadCrumb']);
                    if (typeOfCategory == 'UNSPSC') {
                        $("#disciplineTypes").html(categoryObj['unspscStr']);

                    } else {
                        $("#disciplineTypes").html(categoryObj['disciplineTypes']);
                        $(".folder-cover").click(function (event) {
                            //  e.preventDefault();
                            console.log("content::" + $("#" + $(this).attr("id")).attr("data-content"));
                            var discipline = $("#" + $(this).attr("id")).attr("data-content").replace(/_/g, " ");
                            console.log('$(".discipline-heading").text()::' + $(".discipline-heading").text());
                            discplineTypes(discipline, typeOfCategory, 'N');
                            $(this).off(event);
                        });
                    }

                    $("#categorysrchnvgn").show();

                }
                ajaxStop();
            },
            error: function (e) {
                sessionTimeout(e);
                ajaxStop();
            }

        });
    }

}
function discplineTypes(discipline, typeOfCategory, isnested) {
    ajaxStart();
    console.log("discipline:::" + discipline + ":::typeOfCategory:::" + typeOfCategory + ":::isnested:::" + isnested);
    $("#navigationIcons").hide();
    $("#categorytextfield").hide();
    $("#categorytextfield").val('');
    $(".clear_category_input").hide();
    $("#categorysearchresult").hide();
    if (discipline != null && discipline != '') {
        $.ajax({
            type: "POST",
            url: 'getDiscplineTypes',
            data: {
                'typeOfCategory': typeOfCategory,
                'discipline': discipline,
                isNested: isnested
            },
            traditional: true, cache: false,
            success: function (response) {
//                console.log('Response::' + response);
                if (response != '') {
                    var categoryObj = JSON.parse(response);
                    $("#categorysrchnvgn").html(categoryObj['breadCrumb']);
                    $("#disciplineTypes").html(categoryObj['disciplineTypes']);
                    categoryAutoComplete();
//                    data-totalrecords='0' data-startindex='0'

                }
                ajaxStop();
            },
            error: function (e) {
                ajaxStop();
                sessionTimeout(e);
            }

        });
    }

}
function fetchClassbyDiscipline(typeOfCategory, discipline, subDiscipline, startindex, endindex) {
    console.log("subDiscipline:::" + subDiscipline + "::::discipline:::" + discipline + ":::typeOfCategory:::" + typeOfCategory + ":::startindex:::" + startindex + ":::endindex:" + endindex);
    ajaxStart();
    $('#categorytextfieldval').val(typeOfCategory);
    $('#disciplinetextfieldval').val(discipline);
    $('#subDisciplinetextfieldval').val(subDiscipline);
    $("#categorytextfield").val('');
    if (discipline != null && discipline != '') {
        $.ajax({
            type: "POST",
            url: 'getClassByDiscipline',
            data: {
                'typeOfCategory': typeOfCategory,
                'discipline': discipline,
                subDiscipline: subDiscipline,
                startIndex: startindex,
                endIndex: endindex
            },
            traditional: true, cache: false,
            success: function (response) {
                if (response != '') {
                    var categoryObj = JSON.parse(response);
                    $("#categorysrchnvgn").html(categoryObj['breadCrumb']);
                    $("#disciplineTypes").html(categoryObj['disciplineTypes']);

                    if (parseInt(categoryObj['startIndex']) == 0) {
                        $("#navigationIcons").attr("data-totalrecords", categoryObj['totalCount']);
                        $("#navigationIcons").html(categoryObj['navigationIcons']);
                    } else {
                        $("#navigationIcons").attr("data-startindex", categoryObj['startIndex']);
                        $("#navigationIcons").attr("data-endindex", parseInt(categoryObj['startIndex']) + 9);
                        if (parseInt(categoryObj['startIndex']) + 9 > parseInt(categoryObj['totalCount']))
                        {
                            $("#categoryMinIndex").html(parseInt(categoryObj['startIndex']));
                            $("#categoryMaxIndex").html(parseInt(categoryObj['totalCount']));
                        } else
                        {
                            $("#categoryMinIndex").html(parseInt(categoryObj['startIndex']));
                            $("#categoryMaxIndex").html(parseInt(categoryObj['startIndex']) + 9);
                        }
                    }
                    $("#navigationIcons").show();
                    $("#categorytextautocomplete").show();
                    $("#categorysearchmarkbox").remove();
                    $(".clear_category_input").hide();
                    $("#categorytextautocompleteBox").html(categoryObj['searchBox']);
                    $("#categorytextautocomplete").append(categoryObj['textBox']);
                    $("#categorytextfield").show();
                    categoryAutoComplete();
                }
                ajaxStop();
            },
            error: function (e) {
                ajaxStop();
                sessionTimeout(e);
            }

        });
    }
}
function getNextRecords(typeOfCategory, discipline, subDiscipline, domain) {
    ajaxStart();
    if (parseInt($("#navigationIcons").attr('data-endindex')) < parseInt($("#navigationIcons").attr('data-totalrecords')))
    {
        fetchClassbyDiscipline(typeOfCategory, discipline, subDiscipline,
                parseInt($("#navigationIcons").attr('data-endindex')) + 1,
                parseInt($("#navigationIcons").attr('data-endindex')) + 10
                );

    }
}
function getPrevRecords(typeOfCategory, discipline, subDiscipline, domain) {
    ajaxStart();
    alert("Entered Prev records");
    if (parseInt($("#navigationIcons").attr('data-startindex')) > 1) {
        fetchClassbyDiscipline(typeOfCategory, discipline, subDiscipline,
                parseInt($("#navigationIcons").attr('data-startindex')) - 10,
                parseInt($("#navigationIcons").attr('data-startindex')) - 1
                );

    }
}
function categoryTextSearchResults(classTerm)
{
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {
        labelObject = [];
    }
    ajaxStart();
    var ImageType = $("#ImageType").val();
    console.log("ImageType::::;" + ImageType);
    var paramArray = [];
    var operator = "";
    var i = 0;
    var count = 0;
    var paramObj = {};
    paramObj.datatype = "string";
    paramObj.column = "TERM";
    paramObj.rangeflag = "N";
    paramObj.minvalue = "";
    paramObj.maxvalue = "";
    paramObj.value = classTerm;
    if (i > 1) {
        paramObj.symbol = "In";
        paramObj.operator = "IN";
    } else {
        paramObj.symbol = "=";
        paramObj.operator = "EQUALS";
    }
    paramObj.staged = "N";
    paramArray.push(paramObj);

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
    paramArray.push(paramObj);

    console.log("classTerm::::" + classTerm + ":::count:::" + count);
    searchResults('I', '', paramArray, "");

}
function categoryClassSearch(id, selectionType, cattype, domain) {
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {
        labelObject = [];
    }
    ajaxStart();
    console.log("selectionType::::" + selectionType);
    var ImageType = $("#ImageType").val();
    console.log("ImageType::::;" + ImageType);
    var paramArray = [];
    var classTerm = "";
    var operator = "";
    var i = 0;
    var count = 0;
    //data-cattype
    if (selectionType == 'M') {
        // count = 1;

        $('#categorySearchpanel input:checked').each(function () {

            classTerm += $(this).attr('data-content');
            //   selected.push($(this).attr('name'));
            classTerm += "#";
            cattype = $(this).attr('data-cattype');
            //var countId = "disciplinetype"+i+"img";
            count += parseInt($(this).attr('data-count'));
            i++;
        });


    } else {
        classTerm = $("#" + id).attr('data-content');
        count = $("#" + id).attr('data-count'); //data-count
    }
    var paramObj = {};
    paramObj.datatype = "string";
    paramObj.column = "TERM";
    paramObj.rangeflag = "N";
    paramObj.minvalue = "";
    paramObj.maxvalue = "";
    paramObj.value = classTerm;
    if (i > 1) {
        paramObj.symbol = "In";
        paramObj.operator = "IN";
    } else {
        paramObj.symbol = "=";
        paramObj.operator = "EQUALS";
    }
    paramObj.staged = "N";
    paramArray.push(paramObj);

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
    paramArray.push(paramObj);

    console.log("classTerm::::" + classTerm + ":::count:::" + count);
    if (parseInt(count) != 0) {
        searchResults('I', '', paramArray, cattype);
    } else {
        ajaxStop();
        var message = labelObject['No Records found'] != null ? labelObject['No Records found'] : 'No Records found';
        var dialogSplitMessage = dialogSplitIconText(message, "Y");
        $("#dialog1").html(dialogSplitMessage);
        $("#dialog1").dialog({
            title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
            modal: true,
            height: 'auto',
            minHeight: 'auto',
            width: 300,
            fluid: true,
            buttons: {
                Ok: function () {
                    $("#dialog1").empty();
                    $("#dialog1").dialog('close');
                    if (parseInt($("#accordion").attr("data-templateresults")) != 0) {
                        $("#accordion").accordion({active: parseInt($("#accordion").attr("data-templateresults"))});
                        //ajaxStart();
                        fetchTemplateResults("I", paramArray, cattype);
                        //  ajaxStop();
                    }
                }
            }
        });
    }

}
function getunspscByHighLevel(unspscCode, nvgnFlag) {
    ajaxStart();
    console.log("unspscCode:::" + unspscCode);
    $.ajax({
        type: "post",
        traditional: true,
        cache: false,
        url: "unspscCategories",
        data: {
            highLevelCode: unspscCode,
            unspschighcode: $("#categorysrchnvgn").attr("data-unspschighcode")
        },
        success: function (response) {
            console.log("response:::" + response);
            if (response != null && response != '') {
                var categoryObj = JSON.parse(response);
                //$("#categorysrchnvgn").html(categoryObj['breadCrumb']);
                $("#disciplineTypes").html(categoryObj['unspscStr']);

                if (nvgnFlag) {
                    $("#categorysrchnvgn ul")
                            .append("<li class='uSeparator'><span></span></li>"
                                    + "<li>"
                                    + "<a href='#'  "
                                    + "onclick=getunspscByHighLevel('" + unspscCode + "'," + false + ")>"
                                    + unspscCode + "</a>"
                                    + "</li>");
                } else {
                    var deleteflag = false;
                    $("#categorysrchnvgn ul li").each(function () {
                        if (deleteflag) {
                            $(this).remove();
                        } else {
                            if ($(this).find('a').text() == unspscCode)
                            {
                                deleteflag = true;

                            }
                        }
                    });
                }

            }

            ajaxStop();
        },
        error: function (e) {
            ajaxStop();
            sessionTimeout(e);
        }

    });
}
function resetCategorySearch() {
    $("#categorytextfield").hide();
    $("#categorytextautocomplete").hide();
    $(".clear_category_input").hide();
    $('input[name=ImageType]').attr('checked', false);
    $("#categorysrchnvgn").hide();
    $("#navigationIcons").hide();
    $("#disciplineTypes").empty();
    $("#disciplineSubTypes").attr('data-category', "");
    $("#disciplineSubTypes").attr('data-subcategory', "");
    $("#disciplineSubTypes").empty();

}
function fetchMaterialDataAfterEnter(event, typeOfCategory, discipline, subDiscipline, startIndex, endIndex)
{
    if (event.which == 13) {

        $(".ui-autocomplete").css("display", "none");
        categoryTextSearchResult(typeOfCategory, discipline, subDiscipline, startIndex, endIndex);
    }
}
function categoryTextSearchResult(typeOfCategory, discipline, subDiscipline, startindex, endindex)
{
    if (startindex != null && startindex != '' && startindex != undefined && startindex == 1) {
        startindex = 0;
    }
    var searchTerm = $("#categorytextfield").val();
    ajaxStart();
    if (searchTerm != null && searchTerm != '' && searchTerm != undefined) {
        $.ajax({
            type: "POST",
            url: 'getClassByDiscipline',
            data: {
                'typeOfCategory': typeOfCategory,
                'discipline': discipline,
                subDiscipline: subDiscipline,
                startIndex: startindex,
                endIndex: endindex,
                searchTerm: searchTerm
            },
            traditional: true, cache: false,
            success: function (response) {
                if (response != '') {
                    var categoryObj = JSON.parse(response);
                    $("#categorysrchnvgn").html(categoryObj['breadCrumb']);
                    $("#disciplineTypes").html(categoryObj['disciplineTypes']);
                    if (parseInt(categoryObj['startIndex']) == 0) {
                        $("#navigationIcons").attr("data-totalrecords", categoryObj['totalCount']);
                        $("#navigationIcons").html(categoryObj['navigationIcons']);
                    } else {
                        $("#navigationIcons").attr("data-startindex", categoryObj['startIndex']);
                        $("#navigationIcons").attr("data-endindex", parseInt(categoryObj['startIndex']) + 9);
                        if (parseInt(categoryObj['startIndex']) + 9 > parseInt(categoryObj['totalCount']))
                        {
                            $("#categoryMinIndex").html(parseInt(categoryObj['startIndex']));
                            $("#categoryMaxIndex").html(parseInt(categoryObj['totalCount']));
                        } else
                        {
                            $("#categoryMinIndex").html(parseInt(categoryObj['startIndex']));
                            $("#categoryMaxIndex").html(parseInt(categoryObj['startIndex']) + 9);
                        }
                    }
                    $("#categorytextfield").show();
                    $("#categorysearchmarkbox").remove();
//                    $("#categorytextautocompleteBox").append(categoryObj['searchBox']);
                    $("#categorytextautocompleteBox").html(categoryObj['searchBox']);
                    $("#categorytextautocomplete").append(categoryObj['textBox']);
                    $("#categorytextfield").show();

                    $(".clear_category_input").show();
                    $("#categorytextfield").val(searchTerm);
                    categoryAutoComplete();
                }
                ajaxStop();
            },
            error: function (e) {
                ajaxStop();
                sessionTimeout(e);
            }

        });
    }
}
function categoryAutoComplete() {
    showLoader();
    $("#categorytextfield").keydown(function (e) {
        $(".clear_category_input").show();
        var imageid = 0;
        $("#categorytextfield").autocomplete({
            source: function (request, response) {
                $.ajax({
                    url: "categorySearchSuggestions",
                    dataType: "json",
                    data: {
                        categorytextfieldval: $('#categorytextfieldval').val(),
                        disciplinetextfieldval: $('#disciplinetextfieldval').val(),
                        subDisciplinetextfieldval: $('#subDisciplinetextfieldval').val(),
                        term: $("#categorytextfield").val()
                    },
                    success: function (item) {
                        response(item);
                    }
                });
            },
            minLength: 2,
            create: function () {
                $(this).data('ui-autocomplete')._renderItem = function (ul, item) {
                    $(".clear_category_input").show();
                    imageid = parseInt(imageid) + 1;
                    return $('<li>')
                            .append("<table style='color:inherit;background-color:inherit;width:100%;border:none'><tr><td style='width:70%;color:inherit;background-color:inherit;'> " + item.value + "</td><td style='width:30%'> <img id='img" + imageid + "' "
                                    + " onmouseover='templeteMouseOver(id)' onmouseout=templeteMouseOut(id)  style='color:inherit;background-color:inherit;width:40px;height:40px' src='" + item.content + "'/></td></tr></table>")
                            .appendTo(ul);

                };
            },
            open: function () {
                $('.ui-autocomplete').css('width', '600px'); // HERE
            },
            select: function (event, ui) {
                console.log("selectedValue::: " + $.trim(ui.item.value));
                console.log("Term Id::: " + $.trim(ui.item.termid));
                $("#categorytextfield").attr("data-conceptid", $.trim(ui.item.termid));
                categoryTextSearchResults(ui.item.value);
            }
        });
    });
}
function dictionaryAutoComplete(domainValue) {
    showLoader();
    var dxpAdavanceSearchOptions = $('#dxpAdavanceSearchOptions');
    if (dxpAdavanceSearchOptions.val() == 'D') {
        dxpAdavanceSearchOptions.on('change', function () {
//            $("#SearchResult").autocomplete("destroy");
        });
    }
    $("#SearchResult").autocomplete(
            {
                source: function (request, response) {
                    ajaxStart();
                    $.ajax({
                        url: "descriptorSuggestions",
                        dataType: "json",
                        data: {
                            term: $("#SearchResult").val(),
                            domainValue: domainValue,
                        },
                        success: function (item) {
                            ajaxStop();
                            stopLoader();
                            response(item);
                        }
                    });
                },
//            {source: "descriptorSuggestions?searchLimit=15",
                minLength: 2,
                create: function () {
                    $(this).data('ui-autocomplete')._renderItem = function (ul, item) {
                        $(".clear_input").show();
                        // console.log(JSON.stringify(item));
                        imageid = parseInt(imageid) + 1;
                        return $('<li>')

                                .append("<div class='dxpDictionaryMainClass'>"
                                        + "<div class='media'>"
                                        + "<img id='img" + imageid + "' onmouseover='templeteMouseOver(id)' onmouseout=templeteMouseOut(id) src='" + item.content + "' class='align-self-start mr-3' style='width:60px'>"
                                        + "<div class='media-body'>"
                                        + "<h4>" + item.value + " <span class='recordText' style='color:green;'>Active Records</span><span class='badge badge-pill badge-info'>" + item.count + "</span></h4>"
                                        + "<p>" + item.definition + "</p>"
                                        + "</div>"
                                        + "</div>")
//                                +"<div class='dxpDictionaryImageClass' style='width:30%'> <img id='img" + imageid + "' onmouseover='templeteMouseOver(id)' onmouseout=templeteMouseOut(id)  style='color:inherit;background-color:inherit;width:40px;' src='" + item.content + "'/></div>"
//                                         +"<div class='dxpDictionaryValueClass' style='width:70%;color:inherit;background-color:inherit;' title='"+item.value+"'> " + item.value + "</div>"
//                                         +"<div class='dxpDictionaryCountClass' style='width:70%;color:inherit;background-color:inherit;'> " + item.count + "</div>"
//                                         +"<div class='dxpDictionaryDefClass' style='width:70%;color:inherit;background-color:inherit;' title='"+item.definition+"'> " + item.definition + "</div></div>")
                                .appendTo(ul);

                    };
                },
                open: function () {
                    $('.ui-autocomplete').css('width', '600px'); // HERE
                },
                select: function (event, ui) {
                    console.log("selectedValue::: " + $.trim(ui.item.value));
                    console.log("Term Id::: " + $.trim(ui.item.termid));
                    var termId = $.trim(ui.item.termid);
                    termId = termId.toLocaleLowerCase();
                    $("#SearchResult").attr("data-conceptid", termId);
                    getPropertiesByClassName(ui.item.value, ui.item.termid, domainValue);
                }
            });
}
function repositoryAutoComplete(event) {
    showLoader();
    var paramsArray = [];
    var paramObj = {};
    paramObj.datatype = "string";
    paramObj.rangeflag = "N";
    paramObj.column = "PURCHASE";
    paramObj.value = $("#SearchResult").val();
    paramObj.operator = "LIKE";
    paramObj.symbol = "Like";
    paramObj.staged = "N";
    paramObj.andOrOperator = "AND";
    paramsArray.push(paramObj);
    paramObj.datatype = "string";
    paramObj.rangeflag = "N";
    paramObj.column = "TERM";
    paramObj.value = $("#SearchResult").val();
    paramObj.operator = "LIKE";
    paramObj.symbol = "Like";
    paramObj.staged = "N";
    paramObj.andOrOperator = "AND";
    paramsArray.push(paramObj);
    var dxpAdavanceSearchOptions = $('#dxpAdavanceSearchOptions');
    if (dxpAdavanceSearchOptions.val() == 'D') {
        dxpAdavanceSearchOptions.on('change', function () {
            $("#SearchResult").autocomplete("destroy");
        });
    }
    var keyCode = event.keyCode;
    if (keyCode == 13) {
        getRepositorySmartSearchResults($("#SearchResult").val());
    } else {
        $("#SearchResult").autocomplete(
                {
                    source: function (request, response) {
                        ajaxStart();
                        $.ajax({
                            url: "repositorySuggestions",
                            dataType: "json",
                            data: {
                                term: $("#SearchResult").val(),
                                paramsArray: JSON.stringify(paramsArray),
                            },
                            success: function (item) {
                                ajaxStop();
                                response(item);
                            }
                        });
                    },
                    open: function () {
                        $('.ui-autocomplete').css('width', '600px'); // HERE
                    },
//            {source: "descriptorSuggestions?searchLimit=15",
                    minLength: 2,
//                    create: function () {
//                        $(this).data('ui-autocomplete')._renderItem = function (ul, item) {
//                            $(".clear_input").show();
//                            return $('<li>')
//                                    .append("<div class='dxpRespoClass' style='color:inherit;background-color:inherit;width:100%;border:none'>"
////                                         + "<div class='dxpItemClassName' style='width:70%;color:inherit;background-color:inherit;'> " + item.class + "</div> "
//                                            + "<div class='dxpRepoItemPurchaseName' style='color:inherit;background-color:inherit;'> " + item.purchase + "</div></div>")
//                                    .appendTo(ul);
//
//                        };
//                    },
                    create: function () {
                        // var userSearch = $("#SearchResult").val();
                        $(this).data('ui-autocomplete')._renderItem = function (ul, item) {

                            $(".clear_input").show();

                            //13122
                            var replacedTermPurchase;
                            var userSearch = $("#SearchResult").val();
                            $.each(item, function (index, value) {
                                var termPurchase = value;
                                var userSearchedUppCase = userSearch;
                                replacedTermPurchase = termPurchase.replace(userSearch, "<b style= 'color: red;'>" + userSearchedUppCase + "</b>");
                                console.log(replacedTermPurchase);
                                item.purchase = replacedTermPurchase;
                            });

                            return $('<li>')
                                    .append("<div class='dxpRespoClass' style='color:inherit;background-color:inherit;width:100%;border:none'>"
//                                         + "<div class='dxpItemClassName' style='width:70%;color:inherit;background-color:inherit;'> " + item.class + "</div> "
                                            + "<div class='dxpRepoItemPurchaseName' style='color:inherit;background-color:inherit;'> " + item.purchase + "</div></div>")
                                    .appendTo(ul);

                        };
                    },

                    select: function (event, ui) {
                        console.log("selectedValue::: " + $.trim(ui.item.value));
                        console.log("Term Id::: " + $.trim(ui.item.termid));
                        var termId = $.trim(ui.item.termid);
                        termId = termId.toLocaleLowerCase();
                        $("#SearchResult").attr("data-conceptid", termId);
                        getRepositorySmartSearchResults($("#SearchResult").val());
                    }
                });
    }
}
function getDictionarySearchResults(className, reqType) {
    showLoader();
    var i = 0;
    searchPanelShowFlag = true;
    var paramArray = [];
    var paramObj = {};
    var newObj = {};
    newObj.datatype = "STRING";
    newObj.column = "TERM";
    newObj.operator = "EQUALS";
    newObj.symbol = "Equals";
    newObj.value = className;
    paramArray.push(newObj);
    $("#dictionarytbl tbody tr").each(function () {
        var isAllow = false;

        var colname = $(this).attr('data-colname')
        var value = $("#tb" + i).val();
//        var andOrOperator = $("#" + reqType + "andOrOperator" + i).val();
//        var typeSelectStr = $("#" + reqType + "typeSelectStr" + i).val();
//        var dlovcolname = $("#" + reqType + "typeSelectStr" + i).attr("data-dlovcolname");
        if (value != null && value != '') {
            isAllow = true;
        }
        var type = $("#tb" + i).attr("type");
        if (type != null && type == 'checkbox') {
            var textval = "N";
            if ($("#tb" + i).is(':checked')) {
                isAllow = true;
            } else {
                isAllow = false;
            }
        }
        console.log("isAllow::::" + isAllow);
        if (isAllow) {
            paramObj.datatype = $.trim($(this).attr('data-type'));
            paramObj.column = $.trim($(this).attr('data-colname'));
            paramObj.operator = "CONTAINING";
            paramObj.symbol = "containing";
//            paramObj.rangeflag = $.trim($(this).attr('data-range')) == 'Y' ? 'Y' : 'N';
            var type = $("#tb" + i).attr("type");
            if (type != null && type == 'checkbox') {
                var textval = "N";
                if ($("#" + reqType + "tb" + i).is(':checked')) {
                    textval = "Y";
                } else {
                    textval = "N";
                }
                paramObj.value = textval;
            } else {
                paramObj.value = $.trim($("#tb" + i).val());
            }
            paramArray.push(paramObj);
        }
        ++i;
    });

    if (paramArray != null && paramArray.length != 0) {
        var searchName = 'Select One';
        searchResults(reqType, '', paramArray, '', searchName);
    } else {
        var modalObj = {
            title: 'Message',
            body: "Please provide at least one value to Search."
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
        createModal("dataDxpSplitterValue", modalObj);
    }

}
function getDictionaryPanelShow(value) {
    if (paramPanelShowFlag) {
        $("#searchDxpSplitter").jqxSplitter({
            width: '100%',
            height: '100%',
            theme: 'energyblue',
            orientation: 'vertical',
            splitBarSize: 0,
            panels: [{size: 1000}]
        });
        paramPanelShowFlag = false;
    } else {
        $("#searchDxpSplitter").jqxSplitter({
            width: '100%',
            height: '100%',
            orientation: 'vertical',
            splitBarSize: 0,
            panels: [{size: 400}]
        });
        paramPanelShowFlag = true;
    }
}
function getParamPanelShow(value) {
    if (searchPanelShowFlag) {
        $("#searchDxpSplitter").jqxSplitter({
            width: '100%',
            height: '100%',
            theme: 'energyblue',
            orientation: 'vertical',
            splitBarSize: 0,
            panels: [{size: 1000}]
        });
        searchPanelShowFlag = false;
    } else {
        $("#searchDxpSplitter").jqxSplitter({
            width: '100%',
            height: '100%',
            orientation: 'vertical',
            splitBarSize: 0,
            panels: [{size: 400}]
        });
        searchPanelShowFlag = true;
    }
}
function getClassficationPanelShow(value) {
    if (classificatePanelShowFlag) {
        $("#searchDxpSplitter").jqxSplitter({
            width: '100%',
            height: '100%',
            theme: 'energyblue',
            orientation: 'vertical',
            splitBarSize: 0,
            panels: [{size: 1000}]
        });
        classificatePanelShowFlag = false;
    } else {
        $("#searchDxpSplitter").jqxSplitter({
            width: '100%',
            height: '100%',
            orientation: 'vertical',
            splitBarSize: 0,
            panels: [{size: 400}]
        });
        classificatePanelShowFlag = true;
    }
}
function clearParamSearch(id) {
    $("#" + id + " input[type=checkbox]").prop('checked', false);
    $("#" + id + " input[type=text]").val('');
    $("#" + id + " input[type=text]").removeAttr('disabled');
    $('select').each(function () {
        $(this).prop('selectedIndex', 0);
        $(this).attr('data-staged', 'N');
        toggleOperatorLOV($(this).attr('id').toString().replace("ddw", ""));
    });
}
function toggleOperatorLOV(rowid) {
    if ($.trim($("#ddw" + rowid).val()) == 'BETWEEN') {
        $("#tbmin" + rowid).show();
        $("#tbmax" + rowid).show();
        $("#tbmaxddw" + rowid).show();
        $("#tbminddw" + rowid).show();
        $("#to" + rowid).show();
        $("#tb" + rowid).hide();
        $("#tbddw" + rowid).hide();
        $("#tb" + rowid).removeAttr('disabled');
    } else if ($.trim($("#ddw" + rowid).val()) == 'IS' || $.trim($("#ddw" + rowid).val()) == 'IS NOT') {
        $("#tbddw" + rowid).show();
        $("#tbmin" + rowid).hide();
        $("#tbmax" + rowid).hide();
        $("#to" + rowid).hide();
        $("#tb" + rowid).show();
        $("#tbmaxddw" + rowid).hide();
        $("#tbminddw" + rowid).hide();
        $("#tb" + rowid).val('NULL');
        $("#tb" + rowid).attr('disabled', 'disabled');
    } else {
        $("#tbddw" + rowid).show();
        $("#tbmin" + rowid).hide();
        $("#tbmax" + rowid).hide();
        $("#to" + rowid).hide();
        $("#tb" + rowid).show();
        $("#tbmaxddw" + rowid).hide();
        $("#tbminddw" + rowid).hide();
        $("#tb" + rowid).removeAttr('disabled');
        if ($.trim($("#tb" + rowid).val()) == 'NULL')
        {
            $("#tb" + rowid).val('');
        }
    }
}
function clearDictSearch(result1, result1) {
    var i = 0;
    $("#tb" + i).val('');
}
function getFormPanelShow() {
    $("#thirdDxpSplitter").hide();
    $(".dxpFormHideShow").show();
}
function saveSearchConfirm(searchType) {
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {
        labelObject = [];
    }
    $("#dialog1").html("");
    var persConfirmation = "Review personalization info before saving the search";
    persConfirmation = labelObject[persConfirmation] != null ? labelObject[persConfirmation] : persConfirmation;
    var results = "<div style='display:block;margin-bottom: 1%;'>" + persConfirmation + "</div>"
            + "<input type='text' id='searchName' class='saveSrchinput jqx-widget-content jqx-widget-content-arctic jqx-input jqx-input-arctic jqx-widget jqx-widget-arctic jqx-rc-all jqx-rc-all-arctic' placeholder='Name your search'/>"
            + "<div id='searchNameError' style='color:red;display:none;'>Should not be null</div>";
    var dialogSplitMessage = dialogSplitIconText(results, "Y");
    var modalObj = {
        title: 'Search Criteria',
        body: dialogSplitMessage
    };
    var buttonArray = [
        {
            text: 'Ok',
            click: function () {
                ajaxStop();
                var searchName = $("#searchName").val();
                if (searchName != null && searchName != '') {
                    $("#searchNameError").hide();
                    saveSearch(searchType, searchName);
                    $("#dialog1").empty();
                    $("#dialog1").dialog('close');
                } else {
                    $("#searchNameError").show();
                }
            },
            text: 'Close',
            click: function () {

            },
            isCloseButton: true
        }
    ];
    modalObj['buttons'] = buttonArray;
    createModal("dataDxpSplitterValue", modalObj);
    $(".modal-dialog").addClass("opacity-animate3");
}
function saveSearch(searchType, searchName) {
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {
        labelObject = [];
    }

    searchName = searchName.replace(/^\s+|\s+$/g, "");
    var savedSearchObject = null;
    var savedSearchArray = [];
    var tableId = "paramsearch";
    $("#pers_criteria  > tbody  > tr").each(function () {
        //$.trim($(this).find("td:eq(1)").find("input[type='checkbox']").is(':checked'));
        // console.log(++i);
        console.log("Column Stat:: Ready" + $.trim($(this).find("td:eq(2)").find("input[type='checkbox']").attr("data-property")));
        savedSearchObject = new Object();
        var columnName = $(this).find("td:eq(0)").attr('data-col');
        savedSearchObject.persCol = columnName;
        savedSearchObject.searchFlag = $(this).find("td:eq(1)").find("input[type='checkbox']").is(':checked') == true ? "Y" : "N";
        savedSearchObject.displayFlag = $("#" + columnName + "_display").is(':checked') == true ? "Y" : "N";
        savedSearchObject.freezeFlag = $("#" + columnName + "_freeeze").is(':checked') == true ? "Y" : "N";
        savedSearchObject.defaultFlag = $("#" + $(this).find("td:eq(0)").attr('data-col')).css('display') == 'none' ? "N" : "Y";

        var i = 0;
        $("#" + tableId + " tbody tr").each(function () {
            var isAllow = false;
            var colname = $(this).attr('data-colname');
            if (colname == columnName) {
                var tbmin = $("#tbmin" + i).val();
                var tbmax = $("#tbmax" + i).val();
                var value = $("#tb" + i).val();
                console.log("colname::" + colname + "::value::" + value + "::tbmin::" + tbmin + ":::tbmax:::" + tbmax);
                if (value != null && value != '') {
                    isAllow = true;
                } else if (tbmin != null && tbmax != null && tbmin != '' && tbmax != '') {
                    isAllow = true;
                }
                var type = $("#tb" + i).attr("type");
                if (type != null && type == 'checkbox') {
                    var textval = "N";
                    if ($("#tb" + i).is(':checked')) {
                        isAllow = true;
                    } else {
                        isAllow = false;
                    }
                }
                console.log("isAllow::::" + isAllow);
                if (isAllow) {
                    var andOrOperator = $("#andOrOperator" + i).val();
                    var typeSelectStr = $("#typeSelectStr" + i).val();
                    savedSearchObject.rangeflag = $.trim($(this).attr('data-range')) == 'Y' ? 'Y' : 'N';
                    savedSearchObject.minvalue = $.trim($("#tbmin" + i).val());
                    savedSearchObject.maxvalue = $.trim($("#tbmax" + i).val());
                    var type = $("#tb" + i).attr("type");
                    if (type != null && type == 'checkbox') {
                        var textval = "N";
                        if ($("#tb" + i).is(':checked')) {
                            textval = "Y";
                        } else {
                            textval = "N";
                        }
                        savedSearchObject.value = textval;
                    } else {
                        savedSearchObject.value = $.trim($("#tb" + i).val());
                    }
                    savedSearchObject.andOrOperator = andOrOperator;
                    savedSearchObject.typeSelectStr = typeSelectStr;
                    savedSearchObject.operator = $("#ddw" + i).val();
                    savedSearchObject.symbol = $.trim($("#ddw" + i).find('option:selected').text());
                    savedSearchObject.staged = $("#ddw" + i).attr('data-staged') == "Y" ? "Y" : "N";

                }

                return false;
            }
            ++i;
        });
        savedSearchArray.push(savedSearchObject);

        // }
    });
    $.ajax({
        type: "POST",
        url: 'saveSrchCritera',
        data: {
            searchName: searchName,
            searchType: searchType,
            searchitems: JSON.stringify(savedSearchArray)
        },
        traditional: true,
        cache: false,
        success: function (response) {
            if (response == "success") {
                var results = "Saved search criteria Successfully";
                var dialogSplitMessage = dialogSplitIconText(results, "Y");
                var modalObj = {
                    title: 'Search Criteria',
                    body: dialogSplitMessage
                };
                var buttonArray = [
                    {
                        text: 'Ok',
                        click: function () {
                            ajaxStop();
                            $("#savedSearchName").val(searchName.toString().toUpperCase());
                            getPersonalizationDataOpt(searchType);
                        },
                        text: 'Close',
                        click: function () {

                        },
                        isCloseButton: true
                    }
                ];
                modalObj['buttons'] = buttonArray;
                createModal("dataDxpSplitterValue", modalObj);
            } else if (response == "duplicatename") {
                var results = "Please provide different name for search criteria as this name exists";
                var dialogSplitMessage = dialogSplitIconText(results, "Y");
                var modalObj = {
                    title: 'Search Criteria',
                    body: dialogSplitMessage
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
                createModal("dataDxpSplitterValue", modalObj);
            } else if (response == "failure") {
                var results = "Unable to save search criteria";
                var dialogSplitMessage = dialogSplitIconText(results, "Y");
                var modalObj = {
                    title: 'Search Criteria',
                    body: dialogSplitMessage
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
                createModal("dataDxpSplitterValue", modalObj);
            } else if (response == "blank") {
                var results = "Search criteria name can't be blank";
                var dialogSplitMessage = dialogSplitIconText(results, "Y");
                var modalObj = {
                    title: 'Search Criteria',
                    body: dialogSplitMessage
                };
                var buttonArray = [
                    {
                        text: 'Close',
                        click: function () {
                            ajaxStop();
                        },
                        isCloseButton: true
                    }
                ];
                modalObj['buttons'] = buttonArray;
                createModal("dataDxpSplitterValue", modalObj);
            }
        },
    });
}
function getCategoryTreeForm(searchId, reqType) {
    searchId = "DXP_CATEGORY_TREE";
    $("#searchsettingsSplitter").children("#jqxTreeDiv").appendTo($("#expanderDesc").children("#jqxTreeDiv"));
    $("#searchsettingsSplitter").children("#jqxTreeDiv").removeAttr('id');
    $.ajax({
        type: "POST",
        url: 'getGenericDxpTree',
        data: {
            'treeId': searchId,
        },
        traditional: true,
        cache: false,
        success: function (treeObject) {
            $("#searchDefaultSplitter").hide();

//            $("#pageBodyContent").html(treeObject['divid']);
            $("#dxpClassficationAppendClass").html(treeObject['divid']);
//                $("#DxpParamSplitterDotsClass").show();
            if (treeObject != null && !jQuery.isEmptyObject(treeObject)) {
                $("#expanderDesc").html(treeObject['treeDesc']);//treeDesc
                var extTreeParams = {};
                extTreeParams = treeObject['extTreeParams'];
                $("#selectedFldValue").val((treeObject['treeColumnObj'])[0]['HL_FLD_NAME']);
                selectedColumnData = (treeObject['treeColumnObj'])[0];
                if (extTreeParams != null && !jQuery.isEmptyObject(extTreeParams)) {
                    $("#extTreeParams").val(JSON.stringify(extTreeParams));
                }
                $("#firstDxpSplitterTree").jqxSplitter({
                    width: '100%',
                    height: '100%',
                    orientation: 'vertical',
                    splitBarSize: 0,
                    panels: [{size: 400}]
                });

                treeConfig(treeObject);
                dxpTreeSearch();
            }
        }
    });
}
function dxpTreeSearch() {
    $("#dxptreeSearchResult").autocomplete(
            {
                source: function (request, response) {
                    ajaxStart();
                    $.ajax({
                        url: "classificationAllSuggestions",
                        dataType: "json",
                        data: {
                            term: $("#dxptreeSearchResult").val()
                        },
                        success: function (item) {
                            ajaxStop();
                            response(item);
                        }
                    });
                },
                minLength: 2,
//                create: function () {
//                    $(this).data('ui-autocomplete')._renderItem = function (ul, item) {
//                        $(".clear_input").show();
//                        // console.log(JSON.stringify(item));
//                        imageid = parseInt(imageid) + 1;
//                        return $('<li>')
//
//                                .append("<div class='dxpClassificationMainClass'>"
//                                        + "<div class='media'>"
//                                        + "<div class='media-body' title='" + item.definition + "' style='font-size: 10px;'>" + item.value + "</div>"
//                                        + "<img id='img" + imageid + "' onmouseover='templeteMouseOver(id)' onmouseout=templeteMouseOut(id) src='" + item.content + "' class='align-self-start mr-3' style='width:22px'>"
//                                        + "</div>"
//                                        + "</div>")
//                                .appendTo(ul);
//
//                    };
//                },
                create: function () {
                    $(this).data('ui-autocomplete')._renderItem = function (ul, item) {
                        $(".clear_input").show();


                        //13122
                        var replacedTermPurchase;
                        var userSearch = $("#dxptreeSearchResult").val();
                        $.each(item, function (index, value) {

                            var termValue = item['value'];
                            var userSearchedTreeUppCase = userSearch.toUpperCase();
                            replacedTermValue = termValue.replace(userSearchedTreeUppCase, "<b style= 'color: red;'>" + userSearchedTreeUppCase + "</b>");
                            console.log(replacedTermPurchase);
                            item['newValue'] = replacedTermValue;
                        });

                        // console.log(JSON.stringify(item));
                        imageid = parseInt(imageid) + 1;
                        return $('<li>')

                                .append("<div class='dxpClassificationMainClass'>"
                                        + "<div class='media'>"
                                        + "<div class='media-body' title='" + item.definition + "' style='font-size: 10px;'>" + item['newValue'] + "</div>"
                                        + "<img id='img" + imageid + "' onmouseover='templeteMouseOver(id)' onmouseout=templeteMouseOut(id) src='" + item["content"] + "' class='align-self-start mr-3' style='width:22px'>"
                                        + "</div>"
                                        + "</div>")
                                .appendTo(ul);

                    };
                },
                open: function () {
                    $('.ui-autocomplete').css('width', '260px'); // HERE
                },
                select: function (event, ui) {
                    console.log("selectedValue::: " + $.trim(ui.item.value));
                    console.log("Term Id::: " + $.trim(ui.item.termid));
                    var termId = $.trim(ui.item.termid);
                    termId = termId.toLocaleLowerCase();
                    $("#dxptreeSearchResult").attr("data-conceptid", termId);
                    updateIntellisense(ui.item.termid, ui.item.value);
                }
            });

//    $("#dxptreeSearchResult").keyup(function (e) {
//        var resultVal12 = $("#dxptreeSearchResult").val();
//         $(".dxpTreesearchinnerclass").show();
//        if ($("#dxptreeSearchResult").val() != null && $("#dxptreeSearchResult").val() != '') {
//            $(".clear_searchField").show();
//        } else {
//            $(".clear_searchField").hide();
//        }
//        console.log('Keyevent raised:::' + e.keyCode);
//        var ajaxTime = "";
//        var totalTime = "";
//        var SelectedTabData = $("#localedd").val();
//        //var
//        if (e.keyCode == 32 //Space
//                || e.keyCode == 45 //Insert
//                || e.keyCode == 33 //Page Up
//                || e.keyCode == 34 //Page Down
//                || e.keyCode == 36//Home
//                || e.keyCode == 16 //Shift
//                || e.keyCode == 17 //Ctrl
//                || e.keyCode == 18 //Alt
//                || e.keyCode == 35//End
//                || e.keyCode == 37 //Left arrow
//                || e.keyCode == 38 //Up arrow
//                || e.keyCode == 39 //Right arrow
//                || e.keyCode == 40//Down arrow
//                || e.keyCode == 89//left click
//                ) {
//            console.log('Ajax Not sent');
//        } else {
//            if (e.keyCode == 13 //Enter
//                    && $(this).val().length > 2) {
//                $("#intellisenseTree").html("");
//                $("#jqxTreeDiv").html("");
//
//                //  delay(function () {
//                var resultVal = $("#dxptreeSearchResult").val();
//                resultVal = resultVal.replace(/\s\s+/g, ' ');
//                $("#dxptreeSearchResult").val(resultVal);
//                if (resultVal != null && resultVal != '' && resultVal.length > 2) {
//                    DxpSearchGridResults();
//                } else {
//                    var results = "Enter a keyword of at least 3 chars,ignoring special chars(@.,;:/etc)  to search";
//                    var dialogSplitMessage = dialogSplitIconText(results, "Y");
//                    var modalObj = {
//                        title: 'Message',
//                        body: dialogSplitMessage
//                    };
//                    var buttonArray = [
//                        {
//                            text: 'Close',
//                            click: function () {
//                                ajaxStop();
//                            },
//                            isCloseButton: true
//                        }
//                    ];
//                    modalObj['buttons'] = buttonArray;
//                    createModal("dataDxpSplitterValue", modalObj);
//                }
//            } else {
//                userval = $("#dxptreeSearchResult").val();
//                userval = userval.replace(/\s\s+/g, ' ');
//                if (userval != null && userval != '') {
//                    $.ajax({
//                        type: "POST",
//                        url: "dxpsearchSuggestion",
//                        data: {
//                            searchtext: userval,
//                            SelectedListData: SelectedTabData
//                        },
//                        success: function (response) {
//                            if (response != null && response != "") {
//                                $("#intellisenseTree").html("");
//                                var responseObj = JSON.parse(response);
//                                if (responseObj != null && response != '') {
//                                    $("#intellisenseTree").html(responseObj['suggestion']);
//                                    totalTime = new Date().getTime() - ajaxTime;
//                                    totalTime = parseInt(totalTime) / 1000;
//                                    $("#dxpTreeintellisense").show();
//                                    // $("#jqxTreeDiv").hide();
//                                }
//                            } else {
//                                $("#text_count").text("No record(s) found");
//                                $("#tooltipdiv").html("");
//                                $("#tooltipdiv").jqxTooltip({'content': 'No record(s) found', theme: 'energyblue'});
//                                $("#tooltipdiv").jqxTooltip("open");
//                                $("#dxpTreeintellisense").hide();
//                                $("#jqxTreeDiv").show();
//                            }
//                        },
//                        error: function (e) {
//                            console.log(e);
//                            ajaxStop();
//                            sessionTimeout(e);
//                        }
//
//                    });
//                } else {
//                    $("#intellisenseTree").html("");
//                }
//            }
//        }
//
//    });

}
$(document).ready(function ()
{
    $("#settingheaderImage").click(function () {
        $("#SearchResult").val("");
    });
})
function getRepositorySmartSearchResults(searchedValue) {
//    showLoader();
    ajaxStart();//30322
    $.ajax({
        type: "POST",
        url: 'showDxpRepositorySearchResults',
        data: {
            'typedValue': searchedValue,
            'domainValue': 'PRODUCT',
            'sortFlag': "",
            'typeValueChange': 'Y'
        },
        traditional: true,
        cache: false,
        success: function (result) {
            ajaxStop();
            $("#mainDxpSplitter").show();
            $('#firstDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 0}]});
            $("#thirdDxpSplitter").hide();
            $("#secondDxpSplitterData").html(result);

        }
    });
}
function getShowDxpRepClassSearchResults(className, typedValue) {
//    $("#ui-id-1").hide();
    $("#intellisense").hide();
    $("#thirdDxpSplitter").val('');
    $("#fourthDxpSplitter").val('');
    $("#thirdDxpSplitter").show();
    $("#excelExportsearchResults").show();
    $(".searchResultsList").hide();
    $(".searchDXPCreate").hide();
//    $('#thirdDxpSplitter').jqxSplitter('expand');
    secondPanelShowFlag = true;
    $(".searchResultsList").addClass('activeResult');

    $.ajax({
        datatype: "json",
        type: "POST",
        url: 'showSearchDxpClassResults',
        data: {
            'typedValue': typedValue,
            'className': className,
            'gridId': "DXP_REP_SEARCH_VIEW"
        },
        traditional: true,
        cache: false,
        success: function (response) {
            var resultObj = {};
            resultObj = JSON.parse(response);
            resultObj['className'] = className;
            resultObj['typedValue'] = typedValue;
            gridConfig(resultObj, 0, [], "");
            $("#searchGrid").show();

        }
    });
}
function navigateSearchButton(id, isFormURL, buttonObjId) {

    var url = "PopulateCatalogForm('MM_SEARCH_CATALOG_DATA','MM_PANEL_CAT_NEW_REG')";
    if (url != '' && url.indexOf("<<--") > -1 && url.indexOf("-->>")) {
        var resultId = url.substring(url.indexOf("<<--") + 4, url.indexOf("-->>"));
        var replacedStr = "<<--" + resultId + "-->>";
        var replacedValue = $("#" + resultId).val();
        if (replacedValue != null && replacedValue != '') {
            replacedValue = replacedValue.replace(/&/g, '&amp;');
            replacedValue = replacedValue.replace(/"/g, '&quot;');
            replacedValue = replacedValue.replace(/\\/g, '&bsol;');
            for (var entitykey in HtmlEntities) {
                var entity = HtmlEntities[entitykey];
                var regex = new RegExp(entitykey, 'g');
                replacedValue = replacedValue.replace(regex, entity);
            }
        }

        url = url.replace(replacedStr, replacedValue);

    }
    if (isFormURL == 'N') {
        //  eval(url);
        console.log("url::::" + url);
        PopulateCatalogForm('MM_SEARCH_CATALOG_DATA', 'MM_PANEL_CAT_NEW_REG');
    } else {
        navigateToFormURL(url);
    }
}
function navigateCatalogueButton() {

}
function PopulateCatalogForm(catCopy, panelId, formId) {
    //var attrType = "";
    if (!(formId != null && formId != '')) {
        formId = "";
    }
    var tabName = "";
    if (formId == 'Y') {
        tabName = "searchResults";
    } else {
        tabName = "DXP_REP_SEARCH_VIEW";
    }
    var catalogArray = [];
    var selectedrowindexes = $('#' + tabName).jqxGrid('selectedrowindexes');
    console.log(selectedrowindexes.length);

    if (selectedrowindexes.length != 0) {
        var totalRowIndex = selectedrowindexes.length;
        var datainformations = $('#' + tabName).jqxGrid('getdatainformation');
        if (datainformations != null) {
            var paginginformation = datainformations['paginginformation'];
            if (paginginformation != null) {
                var pagesize = paginginformation['pagesize'];
                if (pagesize != null && parseInt(pagesize) < totalRowIndex) {
                    totalRowIndex = parseInt(pagesize);
                }

            }
        }
        for (var i = 0; i < totalRowIndex; i++)
        {
            var data = $('#' + tabName).jqxGrid('getrowdata', selectedrowindexes[i]);
            catalogArray.push(data);

        }
        $.ajax({
            type: "POST",
            url: 'getcatalogformdata',
            data: {
                'data': JSON.stringify(catalogArray),
                'gridId': catCopy,
                'formId': formId
            },

            success: function (response) {

                var resultstring = response.updateResultString;
                // var hidden fields
                var message = response.message;
                if (message != null && message != "") {
                    var dialogSplitMessage = dialogSplitIconText(message, "Y");
                    var modalObj = {
                        title: 'Message',
                        body: dialogSplitMessage
                    };
                    var buttonArray = [
                        {
                            text: 'Ok',
                            click: function () {
                                $("#searchResults").jqxGrid('clearSelection');
                            },
                            text: 'Close',
                            click: function () {
                                ajaxStop();
                            },
                            isCloseButton: true
                        }
                    ];
                    modalObj['buttons'] = buttonArray;
                    createModal("dataDxpSplitterValue", modalObj);
                    $(".modal-dialog").addClass("opacity-animate3");

                } else {
                    $("#addCgetShowDxpRepClassSearchResultsatalogue").addClass("addToCatalogue");
                    var modalObj = {
                        title: 'Data Sheet',
                        body: resultstring
                    };
                    var buttonArray = [
                        {
                            text: 'Process',
                            click: function () {
                                Process(catCopy, catalogArray, panelId, formId);
                            },
//                            isCloseButton: true
                        }
                    ];
                    modalObj['buttons'] = buttonArray;
                    createModal("dataDxpSplitterValue", modalObj);
                    $(".modal-dialog").addClass("modal-xl opacity-animate3");
                    $("#RECORD_GROUP").val('789654');
                }
            },
            error: function (e) {
                sessionTimeout(e);
            }
        });
    } else {
        var modalObj = {
            title: 'Message',
            body: 'Please select an option to Process'
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
        createModal("dataDxpSplitterValue", modalObj);
        $(".modal-dialog").css("width", "500px");
        $(".modal-dialog").addClass("opacity-animate3");
    }

}
function Process(catCopy, catalogArray, panelId, formId) {
    var resultArray = registerValidation();
    if (resultArray != null && Object.keys(resultArray).length == 0) {
        $(".allErrors").hide();
        labelObject = {};
        try {
            labelObject = JSON.parse($("#labelObjectHidden").val());
        } catch (e) {
            labelObject = [];
        }
        var baskettype = $("#baskettypehid").val();
        var basicData = {};
//            window.open('dupRes?recordNo_Text=' + $("#RECORD_NO").val());
        $("#mat_creation_form_table :input").each(function () {

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
            var controlType = "controlType";
            var commentVal = $("#rejColumn").val();
            var rejColumn = "rejColumn";
            var rejectComment = "rejectComment";
            var ACCEPT_COMMENT = "ACCEPT_COMMENT";

            console.log("textid:::" + textid);
//                  jsonOBJ.ids.push(textid.toLowerCase());
            if (textid != null && textid != 'CREATE_DATE' && textid != 'CREATE_BY') {
                basicData[textid] = textval;
                basicData[rejColumn] = commentVal;
            }
            if (textid != null && textid.lastIndexOf("HIDDEN") > -1) {
                var columnNames = $("#" + textid).val();
                var columnsArray = columnNames.split(",");
                var hiddenIds = textid.split("HIDDEN_");
                // ////////alert("hiddenIds:::" + hiddenIds);
                console.log("textid::::" + textid);
                var hiddenVal = $("#" + hiddenIds[1]).val();
                for (var i = 0; i < columnsArray.length; i++) {
                    if (hiddenVal != null) {
                        hiddenVal = hiddenVal.toUpperCase();
                    }
                    basicData[columnsArray[i]] = hiddenVal;
//                    basicDatas[columnsArray[i]] = encodeURIComponent(hiddenVal);
                }

            }

        });
        basicData['baskettype'] = 'New Registrations';
        basicData['controlType'] = 'CatalogRegister';
        basicData['baskettypehid'] = 'New Registrations';
        basicData['objectid'] = formId;
        basicData['panelId'] = panelId;
        basicData['gridId'] = catCopy;
        ajaxStart();
        $.ajax({
            type: "POST",
            url: 'addToCatalogue',
            data: {
                'data': JSON.stringify(catalogArray),
                'basicData': JSON.stringify(basicData),
                'PANEL_ID': panelId,
                'GRID_ID': catCopy,
                'formId': formId,
            },
            traditional: true,
            cache: false,
            success: function (response) {
                ajaxStop();
                console.log("success" + response);
                var jsonObj = JSON.parse(response);
                var message = jsonObj.message;
                var flag = jsonObj.messageFlag;
                var dialogSplitMessage = "";
                if (message != null && message.indexOf("<table") > -1) {
                    dialogSplitMessage = message;
                } else {
                    dialogSplitMessage = dialogSplitIconText(message, flag);
                }
                var modalObj = {
                    title: 'Message',
                    body: dialogSplitMessage
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
                createModal("dataDxpSplitterValue", modalObj);
                $(".modal-dialog").css("width", "500px");
                $("#addCatalogue").html(dialogSplitMessage);
            },
            error: function (e) {
                ajaxStop();
                var modalObj = {
                    title: 'Message',
                    body: 'Unable to catalog item'
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
                createModal("dataDxpSplitterValue", modalObj);
                sessionTimeout(e);
            }

        });
    } else {
        for (var textIdKey in resultArray) {
            $("#dis" + textIdKey).html(resultArray[textIdKey]);
            $("#dis" + textIdKey).show();


        }
    }


}
function getFormSplitterClose(event) {
    $('#thirdDxpSplitter').show();
    $('#thirdDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: '100%'}]});
    $("#fourthDxpSplitter").hide();
}
function gridoperations(gridId, operationName) {
    var selectedrowindexes = $('#' + gridId).jqxGrid('selectedrowindexes');

    if (selectedrowindexes != null && selectedrowindexes != '' && selectedrowindexes != undefined) {
        var data = $('#' + gridId).jqxGrid('getrowdata', selectedrowindexes);
        $("#operationName").val(operationName);
        if (operationName == 'Add') {
            var modalObj = {
                title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
                body: labelObject['Do you want to create the record?'] != null ? labelObject['Do you want to create the record?'] : 'Do you want to create the record?',
            };
            var buttonArray = [
                {
                    text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                    click: function () {
                        newDxpClassCreation(data['TERM']);
                    },
                    isCloseButton: true
                }
            ];
            modalObj['buttons'] = buttonArray;
            createModal("dataDxpSplitterValue", modalObj);
            $(".modal-dialog").addClass("modal-xs");

        } else if (operationName == 'Copy') {
            var modalObj = {
                title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
                body: labelObject['Do you want to copy the record?'] != null ? labelObject['Do you want to copy the record?'] : 'Do you want to copy the record?',
            };
            var buttonArray = [
                {
                    text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                    click: function () {
                        copyItem("MM_REG_COPY_MGR", JSON.stringify(data));
                    },
                    isCloseButton: true
                }
            ];
            modalObj['buttons'] = buttonArray;
            createModal("dataDxpSplitterValue", modalObj);
            $(".modal-dialog").addClass("modal-xs");
        } else if (operationName == 'Extend') {
            var modalObj = {
                title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
                body: labelObject['Do you want to extend the record?'] != null ? labelObject['Do you want to extend the record?'] : 'Do you want to extend the record?',
            };
            var buttonArray = [
                {
                    text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                    click: function () {
                        $("#showFlag").val('N');
                        data['CLASS_TERM'] = data['TERM'];
                        navigateToForm("RECORD_NO", data, 'form', '', '', 'extension');

                    },
                    isCloseButton: true
                }
            ];
            modalObj['buttons'] = buttonArray;
            createModal("dataDxpSplitterValue", modalObj);
            $(".modal-dialog").addClass("modal-xs");
            searchExtend();
        } else if (operationName == 'Delete') {
            var modalObj = {
                title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
                body: labelObject['Do you want to delete the record?'] != null ? labelObject['Do you want to delete the record?'] : 'Do you want to delete the record?',
            };
            var buttonArray = [
                {
                    text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                    click: function () {
                        $("#showFlag").val('N');
                        data['CLASS_TERM'] = data['TERM'];
                        navigateToForm("RECORD_NO", data, 'form', '', '', 'new deletion');
                        deleteRequest();
                    },
                    isCloseButton: true
                }
            ];
            modalObj['buttons'] = buttonArray;
            createModal("dataDxpSplitterValue", modalObj);
            $(".modal-dialog").addClass("modal-xs");
            deleteRequest();
        } else if (operationName == 'UnDelete') {
            var modalObj = {
                title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
                body: labelObject['Do you want to undelete the record?'] != null ? labelObject['Do you want to undelete the record?'] : 'Do you want to undelete the record?',
            };
            var buttonArray = [
                {
                    text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                    click: function () {
                        $("#showFlag").val('N');
                        data['CLASS_TERM'] = data['TERM'];
                        navigateToForm("RECORD_NO", data, 'form', '', '', 'new Undeletion');
                        undeleteRequest();
                    },
                    isCloseButton: true
                }
            ];
            modalObj['buttons'] = buttonArray;
            createModal("dataDxpSplitterValue", modalObj);
            $(".modal-dialog").addClass("modal-xs");
            undeleteRequest();
        } else if (operationName == 'Change') {
            var modalObj = {
                title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
                body: labelObject['Do you want to Change the record?'] != null ? labelObject['Do you want to Change the record?'] : 'Do you want to Change the record?',
            };
            var buttonArray = [
                {
                    text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                    click: function () {
                        $("#showFlag").val('N');
                        data['CLASS_TERM'] = data['TERM'];
                        navigateToForm("RECORD_NO", data, 'form', '', '', 'Change');
                        newChangeRequest();
//                        undeleteRequest();
                    },
                    isCloseButton: true
                }
            ];
            modalObj['buttons'] = buttonArray;
            createModal("dataDxpSplitterValue", modalObj);
            $(".modal-dialog").addClass("modal-xs");
            newChangeRequest();
//            undeleteRequest();
        }

    } else if (operationName == 'refresh') {
        refreshGrid(gridId);
    } else {
        var modalObj = {
            title: 'Message',
            body: 'Please select a record to ' + operationName + '.'
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
        createModal("dataDxpSplitterValue", modalObj);
        $(".modal-dialog").addClass("modal-xs");
    }
}
function resizable() {
    reqType = $("#dxpAdavanceSearchOptions").val();
    var currentGridId = $("#currentGridId").val();
//    $("#thirdDxpSplitter").jqxSplitter('expand');
    if (reqType == 'PR') {
        $("#DXP_REP_SEARCH_VIEW").css('width', '100%');
        $("#contentDXP_REP_SEARCH_VIEW").css('width', '100%');
        $("#contentDXP_SEARCH_VIEW").css('width', '100%');
        // $("#contentDXP_SEARCH_VIEW").next().css('width','100%'); 
        $("#contentDXP_REP_SEARCH_VIEW").first().css('width', '100%');
        //$("#contentDXP_SEARCH_VIEW::nth-child(2)").css('width','100%'); 
        $("#toolbarDXP_REP_SEARCH_VIEW").css('width', '100%');
        $("#contenttableDXP_REP_SEARCH_VIEW").css('width', '100%');
        $("#jqxScrollWrapverticalScrollBarDXP_REP_SEARCH_VIEW").hide();
        $("#pagerDXP_REP_SEARCH_VIEW").css('width', '100%');
        $("#columntableDXP_REP_SEARCH_VIEW").parent().css('width', '100%');
        $("#columntableDXP_REP_SEARCH_VIEW").css('width', '100%');
    } else {
        $("#" + currentGridId).css('width', '100%');
        $("#content" + currentGridId).css('width', '100%');
        // $("#contentDXP_SEARCH_VIEW").next().css('width','100%'); 
        $("#content" + currentGridId).first().css('width', '100%');
        //$("#contentDXP_SEARCH_VIEW::nth-child(2)").css('width','100%'); 
        $("#toolbar" + currentGridId).css('width', '100%');
        $("#contenttable" + currentGridId).css('width', '100%');
        //31122$("#jqxScrollWrapverticalScrollBarDXP_SEARCH_VIEW").hide();
        $("#pager" + currentGridId).css('width', '100%');
        $("#columntable" + currentGridId).parent().css('width', '100%');
        $("#columntable" + currentGridId).css('width', '100%');
    }
}
function dxpDetailViewData(industry) {
    showLoader();
    $.ajax({
        type: "POST",
        url: 'getIndustryClasses',
        data: {
            'industry': industry,
        },
        traditional: true,
        cache: false,
        success: function (response) {
            stopLoader();
            var modalObj = {
                title: 'Industry Info',
                body: response
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
            createModal("showExtendPdfTableData", modalObj);
            $(".modal-dialog").addClass("modal-xl opacity-animate3");
        }
    });

}
function changeSelect() {
//resetting the contents
    $("#searchedDxpFirstSearchResults").find('.searchFirstResultsList').remove();
    $("#searchedDxpSearchResults").find('.searchResultsList .activeResult').remove();
    $("#searchedDxpSearchResults").find('.searchResultsList').remove();
    $("#contentDXP_SEARCH_VIEW").remove();
    $("#firstDxpSplitter").hide();
    $("#intellisense").hide();
    $("#SearchResult").val('');

    //resizing splitters
    $("#firstDxpSplitter").jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 130}]});
    $("#secondDxpSplitter").jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 270}]});
    $("#thirdDxpSplitter").jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: '100%'}]});
    $("#secondDxpSplitter").hide();
    $("#thirdDxpSplitter").hide();
    $("#firstDxpSplitter").hide();
}
function ajaxStartAttachments() {
    $("#Loader").css("opacity", "0.99");
    $("#Loader").css("display", "block");
    $("body").css("pointer-events", "none");
}
function ajaxStopAttachments() {

    $("#Loader").css("display", "none");
    $("body").css("pointer-events", "auto");
}
function getShowDxpServiceClassSearchResults(className, typedValue, serviceCategory,
        subCategory, uom, recordGroup, sacCode, conceptId, domainValue) {
    ajaxStart();
    $("#intellisense").hide();
    $("#thirdDxpSplitter").val('');
    $("#fourthDxpSplitter").val('');
    $("#classConceptId").val(conceptId);
    $("#thirdDxpSplitter").show();
    $('.viewClassDiv').removeClass('active');
    $("#excelExportsearchResults").show();
    var selectedValue = $("#SelectedValue").val();
    $('#thirdDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: '100%'}]});
    $('#secondDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 0}]});
    $(".searchResultsList").hide();
    $(".searchDXPCreate").hide();
    secondPanelShowFlag = true;
//    $('#fourthDxpSplitter').jqxSplitter('collapse');
    $(".searchResultsList").addClass('activeResult');
    var gridId = '';
    $("#currentDomain").val(domainValue);
    if (domainValue != null && domainValue != '' && domainValue != undefined
            && domainValue == 'All') {
        gridId = "DXP_SEARCH_VIEW";
        $("#currentGridId").val(gridId);
    } else if (domainValue != null && domainValue != '' && domainValue != undefined
            && domainValue == 'SERVICE') {
        gridId = "DXP_SM_SEARCH_VIEW";
        $("#currentGridId").val(gridId);
    }
    $.ajax({
        datatype: "json",
        type: "POST",
        url: 'showSearchDxpClassResults',
        data: {
            'typedValue': typedValue,
            'className': className,
            'gridId': gridId
        },
        traditional: true,
        cache: false,
        success: function (response) {
            ajaxStop();
            var resultObj = {};
            resultObj = JSON.parse(response);
            resultObj['className'] = className;
            resultObj['typedValue'] = typedValue;
            $("#thirdDxpSplitter").val('');
            gridConfig(resultObj, 0, [], "");
            $(".searchDXPCreate").show();
            $("#searchGrid").show();
            $('#secondDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 0}]});
//            $("#thirdDxpSplitter").hide();
            $(".dxpClassHideShow").show();
            $('#thirdDxpSplitter').jqxSplitter({width: '100%', height: '100%', orientation: 'vertical', splitBarSize: 0, panels: [{size: 1550}]});//31122
            $("#fourthDxpSplitter").hide();
            $("#thirdDxpSplitter").show();
//            $(".mainThirdDxpSplitter").html(resultObj);

        }
    });
}
function copyItem(copyid, jsonDataStr) {
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {
        labelObject = [];
    }

    if (copyid == '') {
        var copyMessage = labelObject['Copy id not configured'] != null ? labelObject['Copy id not configured'] : 'Copy id not configured';
        var dialogSplitMessage = dialogSplitIconText(copyMessage, true);
        var modalObj = {
            title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
            body: dialogSplitMessage,
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
        $(".modal-dialog").addClass("modal-xs");
        ajaxStop();
    } else {
        var jsonData = JSON.parse(jsonDataStr);

        var items = {};
        items['CONCEPT_ID'] = $("#classConceptId").val();
        var linkedColumns = $("#linkedColumns").val();
        if (linkedColumns != null && linkedColumns != '') {
            for (var key in jsonData) {
                if (linkedColumns.lastIndexOf(key) > -1) {
                    var value = jsonData[key];
                    if (value != null && value != '') {
                        value = value.replace(/\s/gi, "_");
                        value = value.replace(/[#]/g, "_");
                    }
                    //    console.log("key::::" + key + ":::value::::" + value);

                    //  console.log("key::::" + key + ":::value::::" + value);
                    items[key] = value;
                }
            }
        }
//        items.baskettype = jsonData['basketType'];
//        items.baskettype = "Search View";
        var stripValueStr = $("#stripValue").val();
        var stripValueObjArray = [];
        if (stripValueStr != null) {
            var stripValObj = stripValueStr.split(";");
            for (var i = 0; i < stripValObj.length; i++)
            {
                var stripValueObj = {};
                if (stripValObj[i] != null && stripValObj[i] != '' && typeof stripValObj[i] != 'undefined') {
                    if (stripValObj[i].indexOf(",") > -1) {
                        var stripVal = stripValObj[i].split(",");
                        stripValueObj.columnName = stripVal[0];
                        stripValueObj.value = stripVal[1];
                        stripValueObjArray.push(stripValueObj);
                    }
                }
            }
        }//

        var hiddenObjStr = $("#hiddenObj").val();
        if (hiddenObjStr != null && hiddenObjStr != '') {
            var hiddenObj = JSON.parse(hiddenObjStr);
            for (var key in hiddenObj) {
                var value = hiddenObj[key];
                // alert(key+":::::"+value);
                if (value != null && value != '' && value != 'null') {
                    if (key != null && key.lastIndexOf("HIDDEN") > -1) {

                        var columnsArray = value.split(",");
                        //  alert("columnsArray:::"+columnsArray);
                        var hiddenIds = key.split("HIDDEN_");
                        var hiddenVal = jsonData[hiddenIds[1]];
                        //alert("hiddenIds[1]:::"+hiddenIds[1]);
                        //  alert("hiddenVal:::"+hiddenVal);
                        for (var i = 0; i < columnsArray.length; i++) {
                            if (columnsArray[i] != 'NAME1') {
                                items[columnsArray[i]] = hiddenVal;
                                jsonData[columnsArray[i]] = hiddenVal;
                            }
                        }

                    }
                } else {
//alert("Value is null");
                }
            }
        }
        items.stripValue = stripValueObjArray
        items.imageColumn = $("#imageColumn").val();
        items.imageTable = $("#imageTable").val();
        items.imageTableColumn = $("#imageTableColumn").val();
        items.linkedColumns = linkedColumns;
        items.gridId = $("#currentGridId").val();
//        items.gridId = $("#hrefGridId").val();
        items.panelId = $("#panelId").val();
        $("#objectid").val("MM_FRM_RECORD_REG_MGR");
        items.objectid = "MM_FRM_RECORD_REG_MGR";
        items.formId = "MM_FRM_RECORD_REG_MGR";
        items.conceptId = $("#classConceptId").val();
        jsonData.formId = "MM_FRM_RECORD_REG_MGR";
        jsonData.objectid = "MM_FRM_RECORD_REG_MGR";
        jsonData.gridId = $("#currentGridId").val();
        jsonData.conceptId = $("#classConceptId").val();
//        items.panelId = $("#panelId").val();
        items.tabId = "";
        //  console.log("items:::" + JSON.stringify(items));
        //   console.log("data:::" + JSON.stringify(data));
        var itemsstring = JSON.stringify(items);
        ajaxStart();
        instanceDropDownMM(itemsstring, copyid, jsonData);
    }
}
function instanceDropDownMM(itemsstring, copyId, jsonData) {
    showLoader();
    var jsCopyObject1 = new Object();
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    $.ajax({
        type: "POST",
        traditional: true,
        dataType: 'json',
        url: "instanceDataMgr",
        data: {
            'jsonString': itemsstring,
        },
        cache: false,
        success: function (response) {
            stopLoader();
            console.log("response::::4061:::;" + response);
//alert(response);
            var plantdata = response['plantdata'];
            if (response != null && response['instantPopupFlag'] != '') {
                var instantPopupFlag = response['instantPopupFlag'];
                if (instantPopupFlag != null && instantPopupFlag != ''
                        && instantPopupFlag == 'N') {
                    plantdata = '';

                    ajaxStart();

                    var jsCopyObject = jsonData;
                    console.log("jsCopyObject::::" + JSON.stringify(jsCopyObject));
                    jsCopyObject.NEW_PLANT = "ALL";
                    jsCopyObject.NEW_BUSINESS_UNIT = "ALL";
                    jsCopyObject.NEW_INSTANCE = "100";
                    jsCopyObject1.formdata = jsCopyObject;
//                    jsCopyObject1.ssFromObject = ssFromObject;
                    //     jsonData['INSTANCE'] = selectedInstance[0];
                    // var jsonString = JSON.stringify(jsonData);

                    console.log("jsonString::::" + JSON.stringify(jsCopyObject1));
                    $.ajax({
                        type: 'post',
                        url: 'copyRecords',
                        //            async: false,
                        data: {
                            'jsonData': JSON.stringify(jsCopyObject1),
                            'copyId': copyId,
                            'controlType': 'COPY',
                            'isSearch': 'Y',
                            'formId': jsCopyObject['formId']
                        },
                        success: function (response) {
                            alert("JSON.parse(response)::::" + response);
                            var jsonData = {};
                            var jsonObj = JSON.parse(response);
                            response = jsonObj.Message;
                            var flag = jsonObj.messageFlag;
                            var dialogSplitMessage = dialogSplitIconText(response, flag);
                            jsonData = jsonObj.ssfromobject;
                            if (jsonData != null) {
                                var stripValue = jsonData['stripValue'];
                                console.log("stripValue:::" + stripValue);
                                if (stripValue != null && stripValue.length != 0) {
                                    var stripValueObjArray = [];
                                    for (var i = 0, max = 10; i < stripValue.length; i++) {
                                        var stripValueObj = {};
                                        var stripObj = stripValue[i];
                                        stripValueObj.columnName = stripObj['columnName'];
                                        stripValueObj.value = stripObj['value'];
                                        stripValueObjArray.push(stripValueObj);
                                    }
                                }
                                jsonData['stripValue'] = stripValueObjArray;
                                //stripValue
                            }
                            ajaxStop();
                            // ////alert("JSON.stringify(jsonData))::::"+JSON.stringify(jsonData));
                            //var baskettype1 = $('#baskettypehid1').val();

                            $("#dialog").html(dialogSplitMessage);
                            $("#dialog").dialog({
                                title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
                                modal: true,
                                height: 'auto',
                                minHeight: 'auto',
                                minWidth: 500,
                                maxWidth: 'auto',
                                fluid: true,
                                buttons: [{
                                        text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                                        click: function () {
                                            $(this).html("");
                                            $(this).dialog("close");
                                            $(this).dialog("destroy");
                                            if (flag)
                                            {
                                                jsonData.baskettype = jsCopyObject['baskettype'];
                                                jsonData.gridId = jsCopyObject['gridId'];
                                                $("#items").val(JSON.stringify(jsonData));
                                                $("#submitForm").attr("action", "formData");
                                                $("#submitForm").submit();
                                            }

                                        }
                                    }],
                                open: function () {
                                    $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                                    $(".visionHeaderMain").css("z-index", "999");
                                    $(".visionFooterMain").css("z-index", "999");
                                    $(".ui-dialog").addClass("copyIconDialog");
                                },
                                beforeClose: function (event, ui)
                                {
                                    $(".visionHeaderMain").css("z-index", "99999");
                                    $(".visionFooterMain").css("z-index", "99999");
                                }
                            });
                        },
                        error: function (e)
                        {
                            ajaxStop();
                            sessionTimeout(e);
                        }

                    });
                    // extensions(jsonString, success_msg);
                }
            }
            if (response != null && plantdata != '') {

                var instanceDropDownDiv = "<div class='visionFormExtendDropdown'><div class='visionFormExtendTitle'>"
                        + (labelObject['Instance : Plant'] != null ? labelObject['Instance : Plant'] : 'Instance : Plant')
                        + "</div><div id='instance_div' class='visionFormExtendInfo'>" +
                        "" + response['plantdata'] +
                        "</div></div>";
                $("#instanceDialogBox").html(instanceDropDownDiv);
                $("#selectedInstance").chosen({allow_single_deselect: true});
                ajaxStop();
                $("#instanceDialogBox").dialog({
                    modal: true,
                    title: (labelObject['Instance : Plant'] != null ? labelObject['Instance : Plant'] : 'Instance : Plant'),
//                        minHeight: 0,
//                        minWidth: 300,
                    width: 300,
                    maxWidth: 'auto',
                    height: 'auto',
                    fluid: true,
                    buttons: [{
                            text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                            click: function () {

                                showLoader();
                                var selectedInstance = $('#selectedInstance').val();
                                console.log("selectedInstance::::" + selectedInstance);
                                selectedInstance = selectedInstance.split(':');
                                $(this).html("");
                                $(this).dialog("close");
                                $(this).dialog("destroy");
                                if (selectedInstance != null && selectedInstance != '') {
                                    var operationName = $('#operationName').val();
                                    if (operationName != 'Extend') {
                                        var jsCopyObject = jsonData;
                                        console.log("jsCopyObject::::" + JSON.stringify(jsCopyObject));
                                        jsCopyObject.NEW_PLANT = selectedInstance[1];
                                        jsCopyObject.NEW_BUSINESS_UNIT = selectedInstance[1];
                                        jsCopyObject.NEW_INSTANCE = selectedInstance[0];
                                        jsCopyObject1.formdata = jsCopyObject;
                                        jsCopyObject1.ssFromObject = jsonData;
                                        //     jsonData['INSTANCE'] = selectedInstance[0];
                                        // var jsonString = JSON.stringify(jsonData);

                                        console.log("jsonString::::" + JSON.stringify(jsCopyObject1));
                                        $.ajax({
                                            type: 'post',
                                            url: 'copyRecords',
//            async: false,
                                            data: {
                                                'jsonData': JSON.stringify(jsCopyObject1),
                                                'copyId': copyId,
                                                'controlType': 'COPY',
                                                'isSearch': 'Y',
                                                'formId': jsCopyObject['formId']
                                            },
                                            success: function (response) {
                                                stopLoader();
                                                alert("JSON.parse(response)::::" + response);
                                                var jsonData = {};
                                                var jsonObj = JSON.parse(response);
                                                response = jsonObj.Message;
                                                var flag = jsonObj.messageFlag;
                                                var dialogSplitMessage = dialogSplitIconText(response, flag);
                                                jsonData = jsonObj.ssfromobject;
                                                if (jsonData != null) {
                                                    var stripValue = jsonData['stripValue'];
                                                    console.log("stripValue:::" + stripValue);
                                                    if (stripValue != null && stripValue.length != 0) {
                                                        var stripValueObjArray = [];
                                                        for (var i = 0, max = 10; i < stripValue.length; i++) {
                                                            var stripValueObj = {};
                                                            var stripObj = stripValue[i];
                                                            stripValueObj.columnName = stripObj['columnName'];
                                                            stripValueObj.value = stripObj['value'];
                                                            stripValueObjArray.push(stripValueObj);
                                                        }
                                                    }
                                                    jsonData['stripValue'] = stripValueObjArray;
                                                    //stripValue
                                                }
                                                ajaxStop();
                                                // ////alert("JSON.stringify(jsonData))::::"+JSON.stringify(jsonData));
                                                //var baskettype1 = $('#baskettypehid1').val();
                                                var modalObj = {
                                                    title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
                                                    body: dialogSplitMessage,
                                                };
                                                var buttonArray = [
                                                    {
                                                        text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                                                        click: function () {
                                                            if (flag)
                                                            {
                                                                jsonData.baskettype = jsCopyObject['baskettype'];
                                                                jsonData.gridId = jsCopyObject['gridId'];
                                                                jsonData.TERM = jsCopyObject['TERM'];
                                                                jsonData.CLASS_TERM = jsCopyObject['TERM'];
                                                                jsonData.CONCEPT_ID = jsCopyObject['conceptId'];
                                                                var stripValue = "CONCEPT_ID,#;";
                                                                var stripValueObjArray = [];
                                                                if (stripValue != null) {
                                                                    var stripValObj = stripValue.split(";");
                                                                    for (var i = 0; i < stripValObj.length; i++)
                                                                    {
                                                                        var stripValueObj = {};
                                                                        if (stripValObj[i] != null && stripValObj[i] != '' && typeof stripValObj[i] != 'undefined') {
                                                                            if (stripValObj[i].indexOf(",") > -1) {
                                                                                var stripVal = stripValObj[i].split(",");
                                                                                stripValueObj.columnName = stripVal[0];
                                                                                stripValueObj.value = stripVal[1];
                                                                                stripValueObjArray.push(stripValueObj);
                                                                            }

                                                                        }

                                                                    }

                                                                }//
                                                                jsonData.stripValue = stripValueObjArray;
                                                                $("#TERM").val(jsCopyObject['TERM']);
                                                                registerPanels(jsonData, jsCopyObject);
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
                                                                $(".dxpGridHideShow").show();
                                                                $("#fourthDxpSplitter").show();
                                                                $('.viewClassDiv').removeClass('active');
                                                                $('.viewGridDiv').removeClass('active');
                                                                $("#TERM").val(jsCopyObject['TERM']);

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
//                                                                $("#items").val(JSON.stringify(jsonData));
//                                                                $("#submitForm").attr("action", "formData");
//                                                                $("#submitForm").submit();
                                                            }
                                                        },
                                                        isCloseButton: true
                                                    }
                                                ];
                                                modalObj['buttons'] = buttonArray;
                                                createModal("dataDxpSplitterValue", modalObj);
                                                $(".modal-dialog").addClass("modal-xs");
                                            },
                                            error: function (e)
                                            {
                                                ajaxStop();
                                                sessionTimeout(e);
                                            }

                                        });
                                    } else {
                                        var basicData = {};
                                        basicData = JSON.parse(itemsstring);
                                        basicData.NEW_PLANT = selectedInstance[1];
                                        basicData.NEW_BUSINESS_UNIT = selectedInstance[1];
                                        basicData.NEW_INSTANCE = selectedInstance[0];
                                        extensions(JSON.stringify(basicData), jsonData['success_msg'], selectedInstance);
                                    }
                                    // extensions(jsonString, success_msg);
                                }

                            }
                        }],
                    open: function () {
                        $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                        $(".visionHeaderMain").css("z-index", "999");
                        $(".visionFooterMain").css("z-index", "999");
                        $(".ui-dialog").addClass("copyIconDialog");
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
            ajaxStop();
            //  ////////alert(e.message)
            sessionTimeout(e);
        }
    });
}
function searchExtend() {
    $("#Extend").click(function () {
        var conf_mesg = $("#Extend").attr('data-conf');
        var resultArray = registerValidation();
        //  alert("resultArray:::"+resultArray);
        if (resultArray != null && Object.keys(resultArray).length == 0) {
            $(".allErrors").hide();
            var baskettype = $('#baskettypehid').val();
            var success_msg = $("#Extend").attr('data-success-conf');

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
                        basicData[columnsArray[i]] = hiddenVal;
                    }
                }
            });
            console.log("panaloldData::::" + JSON.stringify(basicData));
            //  basicData['NEW_PLANT'] = basicData['PLANT'];
            basicData['NEW_PURCHASE_ORG'] = basicData['PURCHASE_ORG'];
            basicData['NEW_COMPANY_CDE'] = basicData['COMPANY_CDE'];
            // basicData['NEW_INSTANCE'] = basicData['INSTANCE'];
            /// FOR CM
            basicData['NEW_DISTRIBUTION_CHANNEL'] = basicData['DISTRIBUTION_CHANNEL'];
            basicData['NEW_DIVISION'] = basicData['DIVISION'];
            basicData['NEW_SALES_ORG'] = basicData['SALES_ORG'];
//        delete basicData['PLANT'];
//        delete basicData['PURCHASE_ORG'];
//        delete basicData['COMPANY_CDE'];
//        delete basicData['INSTANCE'];
//        delete basicData['DISTRIBUTION_CHANNEL'];
//        delete basicData['SALES_ORG'];
//        delete basicData['DIVISION'];
//        basicData['PLANT'] = panaloldData['PLANT'];
//        basicData['PURCHASE_ORG'] = panaloldData['PURCHASE_ORG'];
//        basicData['COMPANY_CDE'] = panaloldData['COMPANY_CDE'];
//        basicData['INSTANCE'] = panaloldData['INSTANCE'];
//        basicData['DISTRIBUTION_CHANNEL'] = panaloldData['DISTRIBUTION_CHANNEL'];
//        basicData['DIVISION'] = panaloldData['DIVISION'];
//        basicData['SALES_ORG'] = panaloldData['SALES_ORG'];
//        basicData['controlType'] = "Extend";

            var role = $("#rolehid").val();
            var roleStartsWith = role.substring(0, 2);
            var dialogSplitMessage = dialogSplitIconText(conf_mesg, "Y");
            var modalObj = {
                title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
                body: dialogSplitMessage,
            };
            var buttonArray = [
                {
                    text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                    click: function () {
                        if (roleStartsWith == "VM" && basicData['NEW_PURCHASE_ORG'] == basicData['PURCHASE_ORG'] &&
                                basicData['NEW_COMPANY_CDE'] == basicData['COMPANY_CDE']) {
                            basicData['NEW_ACCOUNT_GROUP'] = panaloldData['ACCOUNT_GROUP'];
                            // for instance level
                            instanceDropDown(basicData);
//            } else if (role.startsWith("VM")) {
                        } else if (roleStartsWith == "CM" && basicData['NEW_SALES_ORG'] == basicData['SALES_ORG']
                                && basicData['NEW_COMPANY_CDE'] == basicData['COMPANY_CDE']
                                && basicData['NEW_DISTRIBUTION_CHANNEL'] == basicData['DISTRIBUTION_CHANNEL']
                                && basicData['NEW_DIVISION'] == basicData['DIVISION']
                                ) {
                            basicData['NEW_ACCOUNT_GROUP'] = panaloldData['ACCOUNT_GROUP'];
                            // for instance level
                            instanceDropDown(basicData);
                        } else if (roleStartsWith == "VM" || roleStartsWith == "CM") {
                            // for company code and Purchase Org Level Exdtension In Manager And Steward.
                            delete basicData['NEW_PLANT'];
                            delete basicData['NEW_INSTANCE'];
                            basicData['NEW_PLANT'] = panaloldData['PLANT'];
                            basicData['NEW_INSTANCE'] = panaloldData['PLANT'];
                            basicData['NEW_ACCOUNT_GROUP'] = panaloldData['ACCOUNT_GROUP'];
                            // basicData['NEW_PURCHASE_ORG'] = basicData['PURCHASE_ORG'];
                            var jsonString = JSON.stringify(basicData);

                            console.log("jsonString::::" + JSON.stringify(jsonString));
                            extensions(jsonString, success_msg);
                        } else {
                            var operationName = $("#operationName").val();
                            basicData['success_msg'] = success_msg;
                            basicData['operationName'] = operationName;
                            basicData['controlType'] = operationName;
                            instanceDropDownMM(JSON.stringify(basicData), '', success_msg);
                        }
                    },
                    isCloseButton: true
                }
            ];
            modalObj['buttons'] = buttonArray;
            createModal("dataDxpSplitterValue", modalObj);
            $(".modal-dialog").addClass("modal-xs");
        } else {
            for (var textIdKey in resultArray) {
                console.log(":::::::::#error_" + textIdKey);
                $("#dis" + textIdKey).html(resultArray[textIdKey]);
                $("#dis" + textIdKey).show();

            }
        }
    });
}
function extensions(jsonString, success_msg, selectedInstance) {
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    console.log("extensions:::" + success_msg);
    if (jsonString != null) {
        $.ajax({
            type: "POST",
            traditional: true,
            dataType: 'html',
            url: "extension",
            cache: false,
            data: {'basicData': jsonString,
                'selectedInstance': (selectedInstance != null ? selectedInstance : "")

            },
            success: function (response) {

                var jsonData = {};
                var jsonObj = JSON.parse(response);
                if (jsonObj['Message'] != null && jsonObj['Message'] != '') {
                    success_msg = jsonObj['Message'];
                }
                // response = jsonObj.Message;
                var flag = jsonObj.messageFlag;
                console.log("message:::::" + response);
                console.log("message:::::" + flag);
                ///////alert("JSON.parse(response)::::"+jsonObj.Message);
                jsonData = jsonObj.ssfromobject;
                if (jsonData != null) {
                    var stripValue = jsonData['stripValue'];
                    console.log("stripValue:::" + stripValue);
                    if (stripValue != null && stripValue.length != 0) {
                        var stripValueObjArray = [];
                        for (var i = 0; i < stripValue.length; i++) {
                            var stripValueObj = {};
                            if (stripValue[i] != null && stripValue[i] != '' && typeof stripValue[i] != 'undefined') {
                                // if (stripValue[i].indexOf(",") > -1) {
                                var stripObj = stripValue[i];
                                stripValueObj.columnName = stripObj['columnName'];

//                                    stripValueObj.value = encodeURIComponent(stripObj['value']);
                                stripValueObj.value = stripObj['value'];
//                                    stripValueObj.value = encodeURIComponent(stripObj['value']);
                                stripValueObjArray.push(stripValueObj);
                                //  }
                            }

                        }


                    }

                    jsonData['stripValue'] = stripValueObjArray;
                    //stripValue
                }

                // ////alert("JSON.stringify(jsonData))::::"+JSON.stringify(jsonData));
                var baskettype1 = $('#baskettypehid1').val();
                var dialogSplitMessage = "";
//                    if (success_msg != null && success_msg != "")
                if (!flag)
                {
                    dialogSplitMessage = dialogSplitIconText((labelObject[success_msg] != null ? labelObject[success_msg] : success_msg), flag);
                } else
                {
                    dialogSplitMessage = dialogSplitIconText((labelObject[success_msg] != null ? labelObject[success_msg] : success_msg), flag);
                }
                var modalObj = {
                    title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
                    body: labelObject[dialogSplitMessage] != null ? labelObject[dialogSplitMessage] : dialogSplitMessage,
                };
                var buttonArray = [
                    {
                        text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                        click: function () {
                            registerPanels(jsonData, JSON.stringify(jsonData));
//                            $("#CLASS_TERM").val(jsonData['TERM']);
                        },
                        isCloseButton: true
                    }
                ];
                modalObj['buttons'] = buttonArray;
                createModal("dataDxpSplitterValue", modalObj);
                $(".modal-dialog").addClass("modal-xs");
            },
            error: function (e) {
                sessionTimeout(e);
            }
        });
    }
}
function deleteRequest() {
    $("#Delete_Request").click(function () {
        var success_msg = $("#Delete_Request").attr('data-success-conf');
        var conf_mesg = $("#Delete_Request").attr('data-conf');
        var resultArray = registerValidation();
        //  alert("resultArray:::"+resultArray);
        if (resultArray != null && Object.keys(resultArray).length == 0) {

            $(".allErrors").hide();
            // alert("Undelete_Request");
            var baskettype = $('#baskettypehid').val();
            var basicIds = [];
            var basicData = {};

            $("#mat_creation_form_table :input").each(function () {
                var textid = $(this).attr("id");
                var displayAttr = $("#" + textid).attr("display");
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
                basicIds.push(textid);
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
//                    basicData[columnsArray[i]] = encodeURIComponent(hiddenVal);
                    }

                }


            });
//            console.log("panaloldData::::" + JSON.stringify(panaloldData));
            basicData['NEW_PURCHASE_ORG'] = basicData['PURCHASE_ORG'];
            basicData['NEW_COMPANY_CDE'] = basicData['COMPANY_CDE'];
//            delete basicData['PLANT'];
//            delete basicData['PURCHASE_ORG'];
//            delete basicData['COMPANY_CDE'];
//            delete basicData['INSTANCE'];
//            basicData['PLANT'] = panaloldData['PLANT'];
//            basicData['PURCHASE_ORG'] = panaloldData['PURCHASE_ORG'];
//            basicData['COMPANY_CDE'] = panaloldData['COMPANY_CDE'];
//            basicData['INSTANCE'] = panaloldData['INSTANCE'];
            basicData['controlType'] = "Delete Request";
            var role = $("#rolehid").val();
            var jsonString = JSON.stringify(basicData);
            console.log("jsonString::::" + JSON.stringify(jsonString));
            var dialogSplitMessage = dialogSplitIconText(conf_mesg, "Y");
            var modalObj = {
                title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
                body: dialogSplitMessage,
            };
            var buttonArray = [
                {
                    text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                    click: function () {
                        $("#showFlag").val('N');
                        basicData['CLASS_TERM'] = basicData['TERM'];
                        changeRequest(jsonString, 'deleteRequest', success_msg);

                    },
                    isCloseButton: true
                }
            ];
            modalObj['buttons'] = buttonArray;
            createModal("dataDxpSplitterValue", modalObj);
            $(".modal-dialog").addClass("modal-xs");
        } else {
            for (var textIdKey in resultArray) {
                //allErrors
                console.log(":::::::::#error_" + textIdKey);
                //$("#dis" + resultArray[i]).html("Should not be null.");
                $("#dis" + textIdKey).html(resultArray[textIdKey]);
                $("#dis" + textIdKey).show();

            }
        }
    });
}
function changeRequest(jsonString, reqType, success_msg) {
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    console.log("panaloldData::::" + jsonString);
    if (jsonString != null) {
        $.ajax({
            type: "POST",
            traditional: true,
            dataType: 'html',
            url: reqType,
            cache: false,
            data: {'basicData': jsonString},
            success: function (response) {

                var jsonData = {};
                var jsonObj = JSON.parse(response);
                var message = jsonObj.Message;
                jsonData = jsonObj.ssfromobject;
                var flag = jsonObj.messageFlag;
                var conformProceedFlag = jsonObj['conformProceedFlag'];
                if (jsonData != null) {
                    var stripValue = jsonData['stripValue'];
                    console.log("stripValue:::" + stripValue);
                    if (stripValue != null && stripValue.length != 0) {
                        var stripValueObjArray = [];
                        for (var i = 0, max = 10; i < stripValue.length; i++) {
                            var stripValueObj = {};
                            var stripObj = stripValue[i];
                            stripValueObj.columnName = stripObj['columnName'];
                            stripValueObj.value = stripObj['value'];
//                                stripValueObj.value = encodeURIComponent(stripObj['value']);
                            stripValueObjArray.push(stripValueObj);
                        }

                    }
                    jsonData['stripValue'] = stripValueObjArray;
                }

                var baskettype1 = $('#baskettypehid1').val();
//                    var dialogSplitMessage = "";
//                    var resultMessage ="";
//                    //  alert(success_msg);
//                    dialogSplitMessage = dialogSplitIconText((labelObject[response] != null ? labelObject[response] : response), flag);
//                    if (response != null && response != '' && response.indexOf("<table") > -1) {
//                        resultMessage=response;
//                    } else {
//                        resultMessage=dialogSplitMessage;
//                    }

                if (conformProceedFlag) {
                    var modalObj = {
                        title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
                        body: message,
                    };
                    var buttonArray = [
                        {
                            text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                            click: function () {
                                openDocsProceedChangeRequest(jsonString, reqType);

                            },
                            isCloseButton: true
                        }
                    ];
                    modalObj['buttons'] = buttonArray;
                    createModal("dataDxpSplitterValue", modalObj);
                    $(".modal-dialog").addClass("modal-xs");
                } else {
                    var modalObj = {
                        title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
                        body: message,
                    };
                    var buttonArray = [
                        {
                            text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                            click: function () {
                                if (flag) {
                                    registerPanels(jsonData, JSON.stringify(jsonData));
                                }

                            },
                            isCloseButton: true
                        }
                    ];
                    modalObj['buttons'] = buttonArray;
                    createModal("intiateRequestClass", modalObj);
                    $(".modal-dialog").addClass("modal-xs");
                }
            },
            error: function (e) {
                sessionTimeout(e);
            }
        });
    }
}
function undeleteRequest() {
    ajaxStart();
    $("#Undelete_Request").click(function () {
        var conf_mesg = $("#Undelete_Request").attr('data-conf');
        var success_msg = $("#Undelete_Request").attr('data-success-conf');
        // alert("Undelete_Request");
        var baskettype = $('#baskettypehid').val();
        var basicIds = [];
        var basicData = {};

        $("#mat_creation_form_table :input").each(function () {
            var textid = $(this).attr("id");
            var displayAttr = $("#" + textid).attr("display");
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
            basicIds.push(textid);
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
//                    basicData[columnsArray[i]] = encodeURIComponent(hiddenVal);
                }

            }


        });
//        console.log("panaloldData::::" + JSON.stringify(panaloldData));
        basicData['NEW_PURCHASE_ORG'] = basicData['PURCHASE_ORG'];
        basicData['NEW_COMPANY_CDE'] = basicData['COMPANY_CDE'];
//        delete basicData['PLANT'];
//        delete basicData['PURCHASE_ORG'];
//        delete basicData['COMPANY_CDE'];
//        delete basicData['INSTANCE'];
//        basicData['PLANT'] = panaloldData['PLANT'];
//        basicData['PURCHASE_ORG'] = panaloldData['PURCHASE_ORG'];
//        basicData['COMPANY_CDE'] = panaloldData['COMPANY_CDE'];
//        basicData['INSTANCE'] = panaloldData['INSTANCE'];
        basicData['controlType'] = "Undelete Request";
        var role = $("#rolehid").val();
        var jsonString = JSON.stringify(basicData);
        console.log("jsonString::::" + JSON.stringify(jsonString));
        var dialogSplitMessage = dialogSplitIconText(conf_mesg, "Y");
        var modalObj = {
            title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
            body: dialogSplitMessage,
        };
        var buttonArray = [
            {
                text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                click: function () {
                    $("#showFlag").val('N');
                    basicData['CLASS_TERM'] = basicData['TERM'];
                    changeRequest(jsonString, 'undeleteRequest', success_msg);
                    undeleteRequest();
                },
                isCloseButton: true
            }
        ];
        modalObj['buttons'] = buttonArray;
        createModal("showExtendPdfTableData", modalObj);
        $(".modal-dialog").addClass("modal-xs");

    });
}
function newChangeRequest() {
    $("#Change").click(function () {

        var success_msg = $("#Change").attr('data-success-conf');
        var conf_mesg = $("#Change").attr('data-conf');
        var baskettype = $('#baskettypehid').val();
        var basicIds = [];
        var basicData = {};


        $("#mat_creation_form_table :input").each(function () {
            var textid = $(this).attr("id");
            var displayAttr = $("#" + textid).attr("display");
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
            basicIds.push(textid);
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
//                    basicData[columnsArray[i]] = encodeURIComponent(hiddenVal);
                }

            }


        });
//        console.log("panaloldData::::" + JSON.stringify(panaloldData));
        var baskettype = $('#baskettypehid').val();
        var basicIds = [];
        var basicData = {};

        $("#mat_creation_form_table :input").each(function () {
            var textid = $(this).attr("id");
            var displayAttr = $("#" + textid).attr("display");
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
            basicIds.push(textid);
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
//                    basicData[columnsArray[i]] = encodeURIComponent(hiddenVal);
                }

            }


        });
//        console.log("panaloldData::::" + JSON.stringify(panaloldData));
        basicData['NEW_PURCHASE_ORG'] = basicData['PURCHASE_ORG'];
        basicData['NEW_COMPANY_CDE'] = basicData['COMPANY_CDE'];
        basicData['NEW_DISTRIBUTION_CHANNEL'] = basicData['DISTRIBUTION_CHANNEL'];
        basicData['NEW_SALES_ORG'] = basicData['SALES_ORG'];
//        delete basicData['PLANT'];
//        delete basicData['PURCHASE_ORG'];
//        delete basicData['COMPANY_CDE'];
//        delete basicData['INSTANCE'];
//        delete basicData['DISTRIBUTION_CHANNEL'];
//        delete basicData['SALES_ORG'];
//        basicData['PLANT'] = panaloldData['PLANT'];
//        basicData['PURCHASE_ORG'] = panaloldData['PURCHASE_ORG'];
//        basicData['COMPANY_CDE'] = panaloldData['COMPANY_CDE'];
//        basicData['INSTANCE'] = panaloldData['INSTANCE'];
//        basicData['DISTRIBUTION_CHANNEL'] = panaloldData['DISTRIBUTION_CHANNEL'];
//        basicData['SALES_ORG'] = panaloldData['SALES_ORG'];
        basicData['controlType'] = "Change";
        var role = $("#rolehid").val();
        var jsonString = JSON.stringify(basicData);
        console.log("jsonString::::" + JSON.stringify(jsonString));
        var dialogSplitMessage = dialogSplitIconText(conf_mesg, "Y");
        var modalObj = {
            title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
            body: dialogSplitMessage,
        };
        var buttonArray = [
            {
                text: labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok',
                click: function () {
                    $("#showFlag").val('N');
                    basicData['CLASS_TERM'] = basicData['TERM'];
                    changeRequest(jsonString, 'changeRequest', success_msg);
//                        undeleteRequest();
                },
                isCloseButton: true
            }
        ];
        modalObj['buttons'] = buttonArray;
        createModal("showExtendPdfTableData", modalObj);
        $(".modal-dialog").addClass("modal-xs");

    });
}
function refreshGrid(gridId) {
    $('#' + gridId).jqxGrid('clearselection');
    $('#' + gridId).jqxGrid('clearfilters');
}

function toggleSearchDiv() {
    $('.innerSearchDiv').show();
    $('.backbutton').show();
    $('.selectDropDown').show();
    $('.closeSearchFeild').toggle();
    /*$('.settingsIconList').show();
    $('.header-notification').show();*/
    $('.closeSearchFeild').show();
    $('.searchFeildIconandText').hide();
}
function togglecloseDiv() {

    $('.closeSearchFeild').hide();
    $('.innerSearchDiv').hide();
    $('.searchFeildIconandText').show();
    /*$('.settingsIconList').hide();
    $('.header-notification').hide();*/
    $("#toggleSearchFeildDiv").removeAttr('onclick','searchToggleGuide()');
}

function generateNewSession(){
	showLoader();
	 $.ajax({
            type: "POST",
             dataType: 'JSON',
            url: "generateNewSession",
            	cache: false,
             traditional: true,
             data: {"flag" : "Y"},
            success: function (response) {
				stopLoader();
				var newLoginSession = response['newLoginSession'];
				localStorage.setItem("newLoginSession", newLoginSession);

            }, 

        });
}
function createModalWidthHeight(modalDivId, modalObj,width,height) {
    $("#" + modalDivId).html("");
    var buttonsArray = modalObj['buttons'];
    $("#" + modalDivId).addClass("modal fade");
    $("#" + modalDivId).attr("role", "dialog");
    $("#" + modalDivId).attr("data-backdrop", "static");
    $("#" + modalDivId).css("padding-top", "58px");
    var modalContant = ''
            + '<div class="modal-dialog opacity-animate3" style="width:'+width+'px;height:'+height+'px;margin-top:2rem";>'
            + '<div class="modal-content">'
            + '<div class="modal-header">'
            + '<h4 class="modal-title text-center">' + modalObj['title'] + '</h4>'
            + '<button type="button" class="close" data-toggle="modal" data-target="#' + modalDivId + '"><span aria-hidden="true">&times;</span><span class="sr-only">Close</span></button>'
            + '</div>'
            + '<div class="modal-body">'
            + modalObj['body']
            + ' </div>';
    modalContant += '</div>'
            + '</div>';
    $("#" + modalDivId).html(modalContant);
    if (buttonsArray != null && buttonsArray.length != 0) {
        var footer = $('<div class="modal-footer">');
        for (var i = 0; i < buttonsArray.length; i++) {
            var buttonObj = buttonsArray[i];
            if (buttonObj != null && !jQuery.isEmptyObject(buttonObj)) {
                var onclickButtonfunction = "";
                var isDismissButton = "";
                if (buttonObj['isCloseButton'] == true) {
                    isDismissButton = " data-dismiss=\"modal\" ";
                }
                var button = $('<button type="button" class="btn btn-primary" ' + isDismissButton + ' >' + buttonObj['text'] + '</button>');
                if (buttonObj['click'] != null && buttonObj['click'] != '') {
                    $(button).click(buttonObj['click']);
                }
                $(footer).append(button);
            }
        }
        $("#" + modalDivId + " .modal-content").append(footer);
    } else {
        $("#" + modalDivId + " .modal-content").append('<div class="modal-footer">'
                + ' <button type="button" class="btn btn-primary" data-dismiss="modal">Ok</button>'
                + '</div>');
    }
     $("#" + modalDivId + " .modal-dialog").draggable({
            handle: ".modal-header"
        });
    $('#' + modalDivId).modal('show');
}


function createModalWidthHeight(modalDivId, modalObj,width,height) {
    $("#" + modalDivId).html("");
    var buttonsArray = modalObj['buttons'];
    $("#" + modalDivId).addClass("modal fade");
    $("#" + modalDivId).attr("role", "dialog");
    $("#" + modalDivId).attr("data-backdrop", "static");
    $("#" + modalDivId).css("padding-top", "58px");
    var modalContant = ''
            + '<div class="modal-dialog opacity-animate3" style="width:'+width+'px;height:'+height+'px;margin-top:2rem";>'
            + '<div class="modal-content">'
            + '<div class="modal-header">'
            + '<h4 class="modal-title text-center">' + modalObj['title'] + '</h4>'
            + '<button type="button" class="close" data-toggle="modal" data-target="#' + modalDivId + '"><span aria-hidden="true">&times;</span><span class="sr-only">Close</span></button>'
            + '</div>'
            + '<div class="modal-body">'
            + modalObj['body']
            + ' </div>';
    modalContant += '</div>'
            + '</div>';
    $("#" + modalDivId).html(modalContant);
    if (buttonsArray != null && buttonsArray.length != 0) {
        var footer = $('<div class="modal-footer">');
        for (var i = 0; i < buttonsArray.length; i++) {
            var buttonObj = buttonsArray[i];
            if (buttonObj != null && !jQuery.isEmptyObject(buttonObj)) {
                var onclickButtonfunction = "";
                var isDismissButton = "";
                if (buttonObj['isCloseButton'] == true) {
                    isDismissButton = " data-dismiss=\"modal\" ";
                }
                var button = $('<button type="button" class="btn btn-primary" ' + isDismissButton + ' >' + buttonObj['text'] + '</button>');
                if (buttonObj['click'] != null && buttonObj['click'] != '') {
                    $(button).click(buttonObj['click']);
                }
                $(footer).append(button);
            }
        }
        $("#" + modalDivId + " .modal-content").append(footer);
    } else {
        $("#" + modalDivId + " .modal-content").append('<div class="modal-footer">'
                + ' <button type="button" class="btn btn-primary" data-dismiss="modal">Ok</button>'
                + '</div>');
    }
     $("#" + modalDivId + " .modal-dialog").draggable({
            handle: ".modal-header"
        });
    $('#' + modalDivId).modal('show');
}