/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


var bodyTop = 20;
var bodySourceTop = 20;
var sourceFirst = true;
var operatorI = 0;
var sourceOperatorI = 0;
var linkedDataObj = {};
var savedDBData = {};
var sourceTables = [];
var windowHeight;
var pageHeight;
var pageOperations;
var gridHeight;
var gridHeightInner;
var tabHeightInner;
var buttonGroups = "";
var $pageBodycontainer;
var pageBodycontinerPadding;
var TabExcelStatus;
var pageOperations1;
var gridMainHeight;
var selectConnObj;
var selectColumnsObj;
var selectedDataBase;
var globalTreeObj;
var currentTabId;
var processLogInterval = null;
var processScheduledJobLogInterval = null;
var trfmRulesChanged = false;
var previousOperatorId;
var treeIconClickEvent;
var operatorDoublClick = false;
var prevTargetOperatorId = null;
var globalFileTreeItem = {};
var selectedItems = [];
var HtmlEntities = {
    " ": "&nbsp;"
};
$(document).on('keydown', function (e) {
    // if key is Delete/Back Space
    if (e.which == 46 || e.which == 8) {
        // remove the same div you clicked on
        var selectedTypeVal = e.currentTarget.activeElement.type;
        if (selectedTypeVal != null
                && (selectedTypeVal == 'input'
                        || selectedTypeVal == 'textarea'
                        || selectedTypeVal == 'text'
                        || selectedTypeVal == 'checkbox'
                        || selectedTypeVal == 'password'
                        || selectedTypeVal == 'hidden'
                        || selectedTypeVal == 'date'
                        || selectedTypeVal == 'number'
                        || selectedTypeVal == 'select')) {
            // console.log('selectedClassVal::::'+selectedClassVal);
        } else {
            // remove the same div you clicked on
            deleteSelectedOporLink();
        }

    } else if (e.which == 17 && e.which == 13) {
        var activeElement = e.currentTarget.activeElement;
        var currentEditorId = activeElement.id;
        if (currentEditorId != null && currentEditorId.indexOf("_editor_") > -1) {
            executeEditorScripts("editorViewDiv");
        }
    }
});
$(function () {
//visionTdETLIcons
    $("#treeSearchInputDiv").hide();
    $("#availableMapTools div").draggable({
        cursor: "move",
        opacity: 0.7,
        helper: 'clone',
        containment: "#jqxWidget",
//                                appendTo: 'body',
        zIndex: 1000,
        helper: function (event, ui) {
            var $this = $(this);
            var innerText = $this.text();
            var descripttion = $this.attr("title");
            var operatorData = {
                top: event.screenX,
                left: event.screenY,

                properties: {
                    body: '<div title="' + descripttion + '" class="visionOpLabelDiv">' + innerText + '</div>',
                    inputs: {
                        input_1: {
                            label: '',
                            multipleLinks: true
                        }
                    },
                    outputs: {
                        output_1: {
                            label: '',
                            multipleLinks: true

                        }
                    }
                }
            };
//            var obj = $('#flowchartworkSourcesspace').flowchart('getOperatorElement', operatorData);
            return  $('#flowchartworkSourcesspace').flowchart('getOperatorElement', operatorData);
        },
        stop: function (e, ui) {
            $(".flowchart-operator-connector-label").hide();
            var $flowchart = $('#flowchartworkSourcesspace');
            var $container = $('#flowchartworkSourcesspace');
            var $this = $(this);
            var innerText = $this.html();
            var title = $(this).attr("data-optitle");
            if (!(title != null && title != '')) {
                title = $(this).attr("title");
            }
            var imgsrc = $(this).attr("data-imgsrc");
            var iconType = $(this).attr("data-type");
            var functionName = $(this).attr("data-functionname");
            if (!(functionName != null && functionName != '')) {
//                functionName = "showTranformationRules(this)";
            }
            var jobId;
            var jobName;
            if (previousOperatorId != null) {
                var previousMapOperatorData = $('#flowchartworkSourcesspace').flowchart('getOperatorData', previousOperatorId);
                jobId = previousMapOperatorData['jobId'];
                jobName = previousMapOperatorData['jobName'];
            }
            trfmRulesChanged = true;
//            var title = "Click here to add transformation rules";

            var elOffset = ui.offset;
            var containerOffset = $container.offset();
            if (elOffset.left > containerOffset.left &&
                    elOffset.top > containerOffset.top &&
                    elOffset.left < containerOffset.left + $container.width() &&
                    elOffset.top < containerOffset.top + $container.height()) {
                var flowchartOffset = $flowchart.offset();
                var relativeLeft = elOffset.left - flowchartOffset.left;
                var relativeTop = elOffset.top - flowchartOffset.top;
                var positionRatio = $flowchart.flowchart('getPositionRatio');
                relativeLeft /= positionRatio;
                relativeTop /= positionRatio;
                elOffset.left = relativeLeft;
                elOffset.top = relativeTop;
            }
//            var divId = "mapDivId_" + 0;
//            if ($("#" + divId).length == 0) {
//                //it doesn't exist
//            }else{
//                
//            }
            //mapDivId:'',
            var data = {
                top: elOffset.top,
                left: elOffset.left,
                iconType: iconType,
                jobName: jobName,
                jobId: jobName,
                timeStamp: new Date().getTime(),
//                statusLebel: innerText,
                properties: {

//                                title: innerText,
                    body: '<div  title="' + title + '" ondblclick=' + functionName
                            + ' class="visionMapOperator"><img src="' + imgsrc + '" class="visionETLIcons" '
                            + 'style="width:30px;height: 30px;"/></div>',
                    inputs: {
                        input_1: {
                            label: 'I-' + iconType,
                            multipleLinks: true
                        }
                    },
                    outputs: {
                        output_1: {
                            label: 'O-' + iconType,
                            multipleLinks: true

                        }
                    }
                }
            };
            var operatorId = $flowchart.flowchart('addOperator', data);
            $(".flowchart-operator-connector-label").hide();
            $(".flowchart-operator-title").hide();
            console.log("operatorId:::" + operatorId);

        }
    });
    $("#treeSearchValue").keyup(function (event) {
        if (event.keyCode === 13) {
            // Cancel the default action, if needed
//            event.preventDefault();
            $("#treeNodeSearchIconId").click();
        }
    });

});
$(document).ready(function () {
    showSavedJobs();
// $('#designViewTab').jqxTabs({width: "100%", height: "100%", position: 'bottom', theme: 'ui-redmond', reorder: true});

    $("#savedConnections").on('mousedown', function (event) {


        var target = $(event.target).parents('li:first')[0];
        var rightClick = isRightClick(event);
        if (rightClick && target != null) {
            $("#savedConnections").jqxTree('selectItem', target);

            var selectedItem = $('#savedConnections').jqxTree('getSelectedItem');
            if (selectedItem.level == 5 || selectedItem.level == 4) {
                var menuItems = "";
                var menuHeight;
                var columnObj = globalTreeObj['treeColumnObj'][selectedItem.level];
                var initParams = columnObj.TREE_INIT_PARAMS;
                if (initParams != null) {
                    var rightClickFunc = initParams.uuu_RightClickFunc;
                    if (rightClickFunc != null) {
                        var options = rightClickFunc.split(";");
                        menuHeight = options.length;
                        $.each(options, function (index) {
                            var menuItem = options[index].split(":")[0];
                            var funcName = options[index].split(":")[1];
                            menuItems += "<li onclick='" + funcName + "'>" + menuItem + "</li>"
                        });
                    }
                }
                $("#jqxMenu").remove();
                $(".visionMainPage").append("<div id='jqxMenu'><ul></ul></div>");
                $("#jqxMenu ul").html(menuItems);
                var contextMenu = $("#jqxMenu").jqxMenu({width: '120px', height: menuHeight * 30 + 'px', autoOpenPopup: false, mode: 'popup', theme: 'energyblue'}); // ravi start




            } else if (selectedItem.level == 3) {// ravi start
                var height = 1;
                var parentListItem = selectedItem.parentElement.parentElement.parentElement;
                if (parentListItem != null) {
                    var selectedParentItem = $('#savedConnections').jqxTree('getItem', parentListItem);
                }

                if (selectedParentItem != null && selectedParentItem.label == "Files") {
                    var menuItems = "<li onclick='deleteFile()'>Delete</li>"
                    $("#jqxMenu").remove();
                    $(".visionMainPage").append("<div id='jqxMenu'><ul></ul></div>");
                    $("#jqxMenu ul").html(menuItems);
                    var contextMenu = $("#jqxMenu").jqxMenu({width: '140px', height: height * 30 + 'px', autoOpenPopup: false, mode: 'popup', theme: 'energyblue'}); // ravi start

                } else {
                    var menuItems = "";
                    height = 1;
                    if (selectedItem.value != "Current_V10") {
                        menuItems += "<li onclick='viewConnection()'>View</li>";
                        menuItems += "<li onclick='deleteConnection()'>Delete</li>";
                        height = 3;
                    }
                    menuItems += "<li onclick=viewSQLEditor('" + selectedItem.value + "')>SQL</li>";
                    $("#jqxMenu").remove();
                    $(".visionMainPage").append("<div id='jqxMenu'><ul></ul></div>");
                    $("#jqxMenu ul").html(menuItems);
                    var contextMenu = $("#jqxMenu").jqxMenu({width: '140px', height: height * 30 + 'px', autoOpenPopup: false, mode: 'popup', theme: 'energyblue'}); // ravi start
                }
            } else { // ravi end
                return false;
            }


            var scrollTop = $(window).scrollTop();
            var scrollLeft = $(window).scrollLeft();

            contextMenu.jqxMenu('open', parseInt(event.clientX) + 5 + scrollLeft, parseInt(event.clientY) + 5 + scrollTop);
            return false;
        }
    });

// ravi start
    $("#schemaObjectsDiv").on('mousedown', function (event) {

        var target;
        if ($(event.target).hasClass('visionObjectNameDiv')) {
            target = $(event.target);


        } else {
            target = $(event.target).parents('div.visionObjectNameDiv')[0];
        }



        var rightClick = isRightClick(event);
        if (rightClick && target != null) {

            $(".visionObjectNameDiv").removeClass("visionSelectedObject");
            $(target).addClass("visionSelectedObject");
            $(".visionObjectNameDiv").find('span').removeClass("visionHighlightTables");
            $(target).find('span').addClass("visionHighlightTables");
            var menuItems = "";
            var menuHeight;
            var columnObj = globalTreeObj['treeColumnObj'][5];
            var initParams = columnObj.TREE_INIT_PARAMS;
            if (initParams != null) {
                var rightClickFunc = initParams.uuu_RightClickFunc;
                if (rightClickFunc != null) {
                    var options = rightClickFunc.split(";");
                    menuHeight = options.length;
                    $.each(options, function (index) {
                        var menuItem = this.split(":")[0];
                        var funcName = this.split(":")[1];
                        menuItems += "<li onclick='" + funcName + "'>" + menuItem + "</li>"
                    });
                }
            }

            $("#jqxMenu").remove();
            $(".visionMainPage").append("<div id='jqxMenu'><ul></ul></div>");
            $("#jqxMenu ul").html(menuItems);
            var contextMenu = $("#jqxMenu").jqxMenu({width: '120px', height: menuHeight * 30 + 'px', autoOpenPopup: false, mode: 'popup', theme: 'energyblue'}); // ravi start

            var scrollTop = $(window).scrollTop();
            var scrollLeft = $(window).scrollLeft();

            contextMenu.jqxMenu('open', parseInt(event.clientX) + 5 + scrollLeft, parseInt(event.clientY) + 5 + scrollTop);
            return false;
        }
    });
    $("#availableJobsId").on('mousedown', function (event) {


        var target = $(event.target).parents('li:first')[0];
        var treeItem = $(event.target).closest('.visionETLAvailableJobs');
        var rightClick = isRightClick(event);
        if (rightClick && target != null && treeItem.length > 0) {
            $("#avaialableJobsTree").jqxTree('selectItem', target);

            var selectedItem = $('#avaialableJobsTree').jqxTree('getSelectedItem');
            var jobId = $(target).find("div.visionETLAvailableJobs").attr("id");
            var menuItems = "<li onclick=\"rightClickProcessJob('" + jobId + "')\">Execute</li>";
            menuItems += "<li onclick=\"deleteJob('" + jobId + "')\">Delete</li>";
            menuItems += "<li onclick=\"copyJob('" + jobId + "')\">Copy</li>"; // -----------------ravi copy job
            menuItems += "<li onclick=\"scheduleJob('" + jobId + "')\">Schedule</li>";
            var menuHeight = 4;

            $("#jqxMenu").remove();
            $(".visionMainPage").append("<div id='jqxMenu'><ul></ul></div>");
            $("#jqxMenu ul").html(menuItems);
            var contextMenu = $("#jqxMenu").jqxMenu({width: '120px', height: menuHeight * 27.5 + 'px', autoOpenPopup: false, mode: 'popup', theme: 'energyblue'}); // ravi start


            var scrollTop = $(window).scrollTop();
            var scrollLeft = $(window).scrollLeft();

            contextMenu.jqxMenu('open', parseInt(event.clientX) + 5 + scrollLeft, parseInt(event.clientY) + 5 + scrollTop);
            return false;
        }
        if (rightClick && target != null && treeItem.length == 0) { // // -----------------ravi new job
            $("#avaialableJobsTree").jqxTree('selectItem', target);

            var selectedItem = $('#avaialableJobsTree').jqxTree('getSelectedItem');

            var menuItems = "<li onclick=\"createNewJob()\">New</li>";

            var menuHeight = 1;

            $("#jqxMenu").remove();
            $(".visionMainPage").append("<div id='jqxMenu'><ul></ul></div>");
            $("#jqxMenu ul").html(menuItems);
            var contextMenu = $("#jqxMenu").jqxMenu({width: '120px', height: menuHeight * 30 + 'px', autoOpenPopup: false, mode: 'popup', theme: 'energyblue'}); // ravi start

            var scrollTop = $(window).scrollTop();
            var scrollLeft = $(window).scrollLeft();

            contextMenu.jqxMenu('open', parseInt(event.clientX) + 5 + scrollLeft, parseInt(event.clientY) + 5 + scrollTop);
            return false;
        }
    });

// disable the default browser's context menu.
    $(document).on('contextmenu', function (e) {
        if ($(e.target).parents('.jqx-tree').length > 0) {
            return false;
        }
        if ($(event.target).parents('div.visionObjectNameDiv').length > 0 || $(event.target).hasClass('visionObjectNameDiv')) {
            return false;
        }

        return true;
    });

    function isRightClick(event) {
        var rightclick;
        if (!event)
            var event = window.event;
        if (event.which)
            rightclick = (event.which == 3);
        else if (event.button)
            rightclick = (event.button == 2);
        return rightclick;
    }

// showSavedJobs();
    $('#editorViewDiv').jqxTabs({width: "100%",
        height: "100%",
        position: 'top',
        theme: 'energyblue',
        reorder: true,
        showCloseButtons: true
    });
    // sql Editor
    $("#Current_V10_editor_1").attr("data-connction-name", "Current_V10");
    var sqlMainEditor = ace.edit("Current_V10_editor_1", {
        mode: "ace/mode/sql",
//        enableBasicAutocompletion: true, // the editor completes the statement when you hit Ctrl + Space
//        showPrintMargin: true, // hides the vertical limiting strip
        fontSize: "100%", // ensures that the editor fits in the environment
        minLines: 5,
        maxLines: 20,
        wrap: true,
        autoScrollEditorIntoView: true
    });
//    sqlMainEditor.getSession().setMode("ace/mode/sql");
    //Current_V10_editor_1_icons_execute
    $("#scriptsExecute").click(function () {
        executeEditorScripts("editorViewDiv");
    });
    $('#Current_V10_editor_1_splitter').jqxSplitter({width: '100%', height: '100%',
        orientation: 'horizontal',
        panels: [{size: "50%", min: 50}, {min: 50, size: "50%"}]});
    $('#jobSchedulingViewSplitter').jqxSplitter({width: '100%', height: '100%',
        orientation: 'vertical',
        panels: [{size: "20%", min: 50}, {min: 50, size: "80%"}]});
    //Current_V10_editor_1_splitter
});

$(window).resize(function ()
{
    screenHeight = screen.height;
    windowHeight = $(window).height();
    pageHeight = $(".visionHeader").height() + $(".visionFooterMain").height() + $(".visionBreadcrumMain").height();
    pageOperations = $(".visionGenericTabSubmit").outerHeight(true) > 0 ? $(".visionGenericTabSubmit").outerHeight(true) : "0";
    pageOperations1 = $(".visionGenericTabExport").outerHeight(true) > 0 ? $(".visionGenericTabExport").outerHeight(true) : "0";
    pageOperations = pageOperations >= pageOperations1 ? pageOperations : pageOperations1;
    $pageBodycontainer = $('.visionBodyContent');
    pageBodycontinerPadding = parseInt($pageBodycontainer.css('padding-top')) + parseInt($pageBodycontainer.css('padding-bottom'));
    gridHeight = windowHeight - pageHeight - pageOperations - pageBodycontinerPadding - 5;
//                        if (parseInt(screenHeight) >= 769) {
//                            gridHeight = windowHeight - pageHeight - pageOperations - pageBodycontinerPadding - 200;
//                            console.log("screen height is::::864:::" + screenHeight);
//                        } else {
//                            gridHeight = windowHeight - pageHeight - pageOperations - pageBodycontinerPadding - 150;
//                            console.log("screen height is::::768:::" + screenHeight);
//                        }
    gridHeightInner = gridHeight - $(".visionGenericTabGrid .jqx-tabs-headerWrapper").outerHeight(true);
    gridMainHeight = gridHeightInner - $(".visionGenericTabGrid .jqx-tabs-headerWrapper").outerHeight(true);

}).resize();

function splitterAdjustment(treeObj)
{
    globalTreeObj = treeObj;
    var levelTabIdHeight = $("#levelTabId").height();
    var visionClusterSpliterMainHeight = $("#visionClusterSpliterMain").height();
    var levelTabIdHeight = $("#levelTabId").height();
    var visionClusterSpliterMainHeight = $("#visionClusterSpliterMain").height();
    var pageHeight = $(".visionHeader").height() + $(".visionFooterMain").height() + $(".visionBreadcrumMain").height();
    var pageHeight1 = $(".visionHeader").height() + $(".visionFooterMain").height();
    var splitterHeight = (parseInt($(window).height()) - parseInt(pageHeight1)) - 10;
    hideBreadcrumb();
    var rss = (function ($) {
        var createWidgets = function () {
            $('#mainSplitter').jqxSplitter({width: '99.3%',
                height: parseInt(splitterHeight),
                panels: [{size: "0%"}, {min: '60%', size: "99.3%"}]});
            $('#contentSplitter').jqxSplitter({width: '100%', height: '100%',
                orientation: 'horizontal',
                panels: [{size: "70%", min: 200}, {min: 30, size: "30%"}]});
            var contentSplitterHeight = $("#feedListExpander").height();
            console.log("contentSplitterHeight:::" + contentSplitterHeight);
            $(".flowchart-example-container").css('height', parseInt(contentSplitterHeight) - 22);
//            $('#connectionsTabs').jqxRibbon({width:5, height: "100%", position: 'left',selectionMode: 'click',mode: 'popup'});
//            $('#connectionsTabs')jqxTabs.({width: "100%", height: "100%", position: 'top', theme: 'energyblue', reorder: true});
//            $('#designViewTab').jqxTabs({width: "100%", height: "100%",position: 'bottom',theme:'energyblue',reorder: true});
            treeSourcesMenus('ETL_DM_TREE_SOURCES', 'savedSources');
//            showUploadedFiles('MM_TREE_UPLOADED_FILES', 'uploadSourceFiles');
            treeSavedConnections(treeObj, 'savedConnections');
            $('#contentSplitter').on('resize', function (event) {
                var feedContentAreaHeight = $('#feedContentArea').height();
                $('table#sourceDestColsTableId tbody').css('height', parseInt(feedContentAreaHeight) - 70);
                $('table#sourceDestColsTableId tbody').css('overflow-y', 'scroll');
                var contentSplitterHeight = $("#feedListExpander").height();
                console.log("contentSplitterHeight:::" + contentSplitterHeight);
                $(".flowchart-example-container").css('height', parseInt(contentSplitterHeight) - 22);
                if (event.args != null) {
                    var panels = event.args.panels;
                    if (panels != null) {
                        // get first panel.
                        var panel1 = panels[0].size;
                        // get second panel.
                        var panel2 = panels[1].size;
                        try {
                            // commented by Ashwini for scroll bar issue
//                            console.log("Tabs Height:Before:" + $("#dataMigrationTabs").jqxTabs("height"));
//                            $("#dataMigrationTabs").jqxTabs({height: ((parseInt(panel2) - 35) + "px")});
//                            console.log("Tabs Height:After:" + $("#dataMigrationTabs").jqxTabs("height"));
//                            $("#dataMigrationTabs .jqx-tabs-content").attr('style', 'height: ' + $("#dataMigrationTabs").jqxTabs("height") + 'px !important');

                        } catch (e) {
                        }
                    }

                }

                // for scrol bar issue
                var feedContentAreaHeight = $("#feedContentArea").height();
                if ($('#dataMigrationTabs').length) {
                    $("#dataMigrationTabs").jqxTabs({height: feedContentAreaHeight});
                }

                var tbodyHeight = feedContentAreaHeight - 90;
                $(".visionColMappScrollDiv1").css("max-height", tbodyHeight);
                $(".visionEtlJoinClauseTablesDiv").css("max-height", tbodyHeight);
//                $(".visionEtlMappingTablesDiv").css("max-height", parseInt(feedContentAreaHeight) - 130);
                $(".visionEtlJoinClauseTablesDivScroll").css("max-height", parseInt(feedContentAreaHeight) - 105);
                $(".visionSqlViewQuery1").css("max-height", parseInt(feedContentAreaHeight) - 67)
                $(".visionEtlJoinrClauseTablesDiv").css("max-height", parseInt(feedContentAreaHeight) - 130);
                $(".visionEtlwhereClauseTablesDiv").css("max-height", parseInt(feedContentAreaHeight) - 69);
                var joinTablesHeight = $(".visionEtlJoinrClauseTablesDiv").height();
                $(".viewJoinQueryOuterDivClass").css("max-height", parseInt(feedContentAreaHeight) - (parseInt(joinTablesHeight) + 82));

                var flowchartworkSourcesspaceHeight = $("#feedListExpander").height();
                $("#feedHeader").css("height", parseInt(flowchartworkSourcesspaceHeight));
                $("#feedHeader").css("overflow-y", "auto");
                $("#flowchartworkSourcesspace").css("height", "");
                // for scrol bar issue

//                console.log("Tabs Height:Before:" + $("#dataMigrationTabs").jqxTabs("height"));
//                $("#dataMigrationTabs").jqxTabs({height: ((parseInt(panel2) - 35) + "px")});
//                console.log("Tabs Height:After:" + $("#dataMigrationTabs").jqxTabs("height"));
//                $("#dataMigrationTabs .jqx-tabs-content").attr('style', 'height: ' + $("#dataMigrationTabs").jqxTabs("height") + 'px !important');

            });
            $('#mainSplitter').on('resize', function (event) {
                var height = $("#connectionsDiv").height();
                var width = $("#connectionsDiv").width();
                $("#schemaObjectsDiv").css("height", height);
                $("#schemaObjectsDiv").css("width", width);

                var contentSplitterWidth = $("#contentSplitter").width();
                if (contentSplitterWidth > 1121) {
                    $(".visionColJoinMappingInput").attr('style', 'width:84% !important');
                    $(".visionColMappingInput").attr('style', 'width:84% !important');

                } else if (contentSplitterWidth > 1100 && contentSplitterWidth < 1120) {
                    $(".visionColJoinMappingInput").attr('style', 'width:81% !important');
                    $(".visionColMappingInput").attr('style', 'width:84% !important');


                } else if (contentSplitterWidth > 1000 && contentSplitterWidth < 1100) {
                    $(".visionColJoinMappingInput").attr('style', 'width:81% !important');
                    $(".visionColMappingInput").attr('style', 'width:84% !important');


                } else if (contentSplitterWidth > 900 && contentSplitterWidth < 1000) {
                    $(".visionColJoinMappingInput").attr('style', 'width:80% !important');
                    $(".visionColMappingInput").attr('style', 'width:83% !important');

                } else if (contentSplitterWidth > 800 && contentSplitterWidth < 900) {
                    $(".visionColJoinMappingInput").attr('style', 'width:78% !important');
                    $(".visionColMappingInput").attr('style', 'width:80% !important');

                } else if (contentSplitterWidth > 700 && contentSplitterWidth < 800) {
                    $(".visionColJoinMappingInput").attr('style', 'width:75% !important');
                    $(".visionColMappingInput").attr('style', 'width:78% !important');

                }

            });

            $('#flowchartworkSourcesspace').flowchart({
                linkWidth: 2,
                defaultSelectedLinkColor: '#000055',
                grid: 10,
                distanceFromArrow: 0,
//                defaultOperatorClass:'VisionWorkFlowTest',
//                multipleLinksOnInput: true,
//                multipleLinksOnOutput: true,
                defaultSelectedLinkColor: 'red',
                onOperatorContextMenu: function (operatorId) {
                    console.log("operatorId:::" + operatorId);
                    openOpeartorContextMenu(operatorId);
                    return true;
                },
            });

            $('#flowchartworkSourcesspace').flowchart({
//                onOperatorSelect: function (operatorId) {
//                    openOpeartorContextMenu(operatorId);
//                    return true;
//                },
                onAfterChange: function (changeType) {

                    if (!operatorDoublClick) {
                        trfmRulesChanged = true;
                        console.error("trfmRulesChanged : " + trfmRulesChanged);
                        return true;
                    }
                }, onOperatorSelect: function (changeType) {
                    $(document).find("input").blur();
                    return true;

                },
                onLinkSelect: function (changeType) {
                    $(document).find("input").blur();
                    return true;
                }
            });

        };
        return {
            init: function () {
                createWidgets();
            }
        }
    }(jQuery));
    rss.init();
//    setTimeout(function(event){
//        window.dispatchEvent(new Event('resize'));
//    }, 200);
}
function createImageSourcesFlowchart(item, title, type)
{
//    var itemId = item.id;
//    var items = $("#" + itemId);
//    var childDiv = items[0].children[1];
//    var divId = childDiv.id;
    var levelType;
    var divTitle;
    var connectionObj;
    var fileType = "";
    var fileExtensions = [".xls", ".xlsx", ".XLS", ".XLSX", ".txt", ".csv", ".xml", ".TXT", ".CSV", ".XML", ".JSON", ".json"];
    if (item != null && !item.hasItems && title != null && title != '') {
        for (var i = 0; i < fileExtensions.length; i++) {
            if (title.indexOf(fileExtensions[i]) > -1) {
                type = 'File';
                fileType = fileExtensions[i];
                break;
            }
        }
    }
    if (type == 'File' || type == 'FileData')
    {
        levelType = 3;
    } else if (type == 'Table')
    {
        sourceTables.push(title);
        var parentElement = item.parentElement;
        var parent = $(parentElement)[0].parentElement;
        var childObj = parent.parentElement.children[1];
        var valueObj = childObj.treeItem['value'];
        divTitle = valueObj;
        levelType = 5;
    }
    if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
    {
        var connObj = savedDBData[divTitle];
        if (connObj != null && !jQuery.isEmptyObject(connObj)) {
            connectionObj = connObj;
        }
    }
    if (item.level == levelType) {
        $("#savedConnections").off('dragEnd').on('dragEnd', function (event) {
            $(".flowchart-operator-connector-label").hide();
            var $flowchart = $('#flowchartworkSourcesspace');
            var $container = $('#flowchartworkSourcesspace');
            var $this = $(this);
            var ui = event.args.originalEvent;
            var elOffset = ui.position;
            var containerOffset = $container.offset();
            if (elOffset.left > containerOffset.left &&
                    elOffset.top > containerOffset.top &&
                    elOffset.left < containerOffset.left + $container.width() &&
                    elOffset.top < containerOffset.top + $container.height()) {
                var flowchartOffset = $flowchart.offset();
                var relativeLeft = elOffset.left - flowchartOffset.left;
                var relativeTop = elOffset.top - flowchartOffset.top;
                var positionRatio = $flowchart.flowchart('getPositionRatio');
                relativeLeft /= positionRatio;
                relativeTop /= positionRatio;
                elOffset.left = relativeLeft;
                elOffset.top = relativeTop;
            }
            var data = {
                top: elOffset.top,
                left: elOffset.left,

                dragType: type

            };

            if (connectionObj != null) {
                var items = [];
                var itemcount = 0;
                var top = elOffset.top;
                items[0] = item;
                var checkedItems = $('#savedConnections').jqxTree('getCheckedItems');
                if (checkedItems != null && checkedItems.length != null && checkedItems.length > 0) {
                    for (var i = 0; i < checkedItems.length; i++) {
                        var itemLevel = checkedItems[i].level;
                        if (itemLevel == 5 &&
                                !(checkedItems[i].label == "VIEWS" ||
                                        checkedItems[i].label == "SYNONYMS" ||
                                        checkedItems[i].label == "TABLES")) {
                            items[itemcount] = checkedItems[i];
                            itemcount++;
                        }
                    }
                }
                for (var i = 0; i < items.length; i++) {
                    var dragedItem = items[i];
                    if (!(dragedItem.label == "VIEWS" || dragedItem.label == "SYNONYMS" || dragedItem.label == "TABLES")) {
                        var itemTreeLevel = dragedItem.level;
                        var title = dragedItem.label;
                        if (itemTreeLevel == 5) {

                            var data = {
                                top: top,
                                left: elOffset.left,
                                dragType: type
                            };

                            data['CONNECTION_NAME'] = connectionObj['CONNECTION_NAME']; //CONNECTION_NAME
                            data['CONN_DB_NAME'] = connectionObj['CONN_DB_NAME']; //CONN_DB_NAME
                            data['CONN_CUST_COL1'] = connectionObj['CONN_CUST_COL1']; //CONN_CUST_COL1
                            data['connObj'] = connectionObj;
                            data['statusLebel'] = title;
                            data['tableName'] = (connectionObj['CONN_CUST_COL1'] != 'SAP') ? (connectionObj['CONN_USER_NAME'] + '.' + title) : title
//                            data['tableName'] = connectionObj['CONN_USER_NAME'] + '.' + title;
                            data['properties'] = {
                                body: '<div  title="' + connectionObj['CONNECTION_NAME'] + '.' + title + '" class=""><div title="Double Click here to view the Transformations rules(' + connectionObj['CONNECTION_NAME'] + '.' + title + ')" style="cursor:pointer"><img src="images/GridDB.png"'
                                        + 'class="visionOpIcons"  ondblclick=viewTrfmRules()  style="width:18px;height: 18px;"/></div><div class="visionOpLabelDiv">' + title + '</div>'

                                        + '</div>',
                                inputs: {
                                    input_1: {
                                        label: '',
                                    }
                                },
                                outputs: {
                                    output_1: {
                                        label: '',
                                    }
                                }
                            }
                            $flowchart.flowchart('addOperator', data);
                            $(".flowchart-operator-connector-label").hide();
                            $(".flowchart-operator-title").hide();
                            trfmRulesChanged = true;
                            top = top + 70;
                        }
                    }
                }


            } else {
                var items = [];
                var top = elOffset.top;
                items[0] = item;
                var itemcount = 0;
                var checkedItems = $('#savedConnections').jqxTree('getCheckedItems');
                if (checkedItems != null && checkedItems.length != null && checkedItems.length > 0) {
                    for (var i = 0; i < checkedItems.length; i++) {
                        var itemLevel = checkedItems[i].level;
                        if (itemLevel == 3 && !(item.label == "xml" || item.label == "csv" || item.label == "xlsx" || item.label == "xls" || item.label == "json")) {
                            items[itemcount] = checkedItems[i];
                            itemcount++;
                        }

                    }
                }
                for (var i = 0; i < items.length; i++) {
                    item = items[i];
                    if (!(item.label == "xml" || item.label == "csv" || item.label == "xlsx" || item.label == "xls" || item.label == "json")) {
                        var itemTreeLevel = item.level;
                        if (itemTreeLevel == 3) {
                            var data = {
                                top: top,
                                left: elOffset.left,
                                dragType: type
                            };
                            var title = item.label;

                            connectionObj = {};
                            connectionObj['filePath'] = item['value'];
                            connectionObj['fileType'] = fileType;
                            connectionObj['fileName'] = title;
                            var fileObj = {};
                            var filePath = item['value'];
                            if (filePath != null && filePath.lastIndexOf("\\") > -1) {
                                filePath = filePath.substring(filePath.lastIndexOf("\\") + 1);
                            }
                            fileObj['filePath'] = filePath;
                            fileObj['fileType'] = fileType;
                            fileObj['fileName'] = title;
                            var imageIcon = "images/Notepad.png";
                            if (fileType == '.xls'
                                    || fileType == '.xlsx'
                                    || fileType == '.XLS'
                                    || fileType == '.XLSX'
                                    ) {
                                imageIcon = "images/xlsx Icon-01.svg"
                            } else if (fileType == '.xml'
                                    || fileType == '.XML') {
                                imageIcon = "images/XML Icon-01.svg";

                            } else if (fileType == '.CSV'
                                    || fileType == '.csv') {
                                imageIcon = "images/CSV ICON-01.svg";
                            } else if (fileType == '.JSON'
                                    || fileType == '.json') {
                                imageIcon = "images/JSON_Isons-02.svg";
                            }
                            connectionObj['imageIcon'] = imageIcon;
                            data['connObj'] = connectionObj;
                            data['filePath'] = item['value'];
                            data['properties'] = {

                                body: '<div  title="' + title + '" class=""><div><img src="' + imageIcon + '"'
                                        + 'class="visionOpIcons" title="Double click here to view the data" ondblclick=viewFileData(\'' + JSON.stringify(fileObj) + '\')  style="width:18px;height: 18px;"/></div><div class="visionOpLabelDiv">' + title + '</div></div>',
                                inputs: {
                                    input_1: {
                                        label: '',
                                    }
                                },
                                outputs: {
                                    output_1: {
                                        label: '',
                                    }
                                }
                            }

                            $flowchart.flowchart('addOperator', data);
                            $(".flowchart-operator-connector-label").hide();
                            $(".flowchart-operator-title").hide();
                            trfmRulesChanged = true;
                            top = top + 70;
                        }
                    }
                }


            }
//            $flowchart.flowchart('addOperator', data);
//            $(".flowchart-operator-connector-label").hide();
//            $(".flowchart-operator-title").hide();
//            trfmRulesChanged = true;
        });
    }

}

function createImageDestinationFlowchart(item, title, type)
{
    var levelType;
    var divTitle;
    var connectionObj;
    if (type == 'File' || type == 'FileData')
    {
        levelType = 1;
    } else if (type == 'Table')
    {
        levelType = 3;
        var parentElement = item.parentElement;
        var parent = $(parentElement)[0].parentElement;
        var div = parent.parentElement.children[1];
        divTitle = div.title;
    }
    if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
    {
        for (var i = 0; i < savedDBData.length; i++)
        {
            var connObj = savedDBData[i];
            if (connObj.hasOwnProperty(divTitle)) {
                connectionObj = connObj[divTitle];
            }
        }

    }
    if (item.level == levelType) {

        var operatorId = 'Dest_operator_' + sourceOperatorI;
        if (true)
        {
            var defaultFlowchartData = {
                operators: {
                    op2: {
                        top: 20,
                        left: 400,
                        statusLebel: title,
                        tableName: title,
                        dragType: type,
                        connObj: connectionObj,
                        properties: {
                            body: '<div  title="' + title + '" class="visionOpLabelDiv">' + title + '</div>',
                            inputs: {
                                input_1: {
                                    label: 'I-' + title,
                                    multipleLinks: true
                                }
                            },
                            outputs: {
                                output_1: {
                                    label: 'O-' + title,
                                    multipleLinks: true
                                }
                            }
                        }
                    }
                },
            };
            $('#flowchartworkSourcesspace').flowchart({
                linkWidth: 2,
                defaultSelectedLinkColor: '#000055',
                grid: 10,
                distanceFromArrow: 0,
                multipleLinksOnInput: true,
                multipleLinksOnOutput: true,
                defaultSelectedLinkColor: 'red',
                onOperatorContextMenu: function (operatorId) {
                    console.log("operatorId:::" + operatorId);
                    openOpeartorContextMenu(operatorId);
                    return true;
                },
            });
            $('#flowchartworkSourcesspace').flowchart({
//                                onOperatorSelect: function(operatorId) {
//                                return true;
//                                },
                onAfterChange: function (changeType) {

                    if (!operatorDoublClick) {
                        trfmRulesChanged = true;
                        console.error("trfmRulesChanged : " + trfmRulesChanged);
                        return true;
                    }
                }, onOperatorSelect: function (changeType) {
                    $(document).find("input").blur();
                    return true;

                },
                onLinkSelect: function (changeType) {
                    $(document).find("input").blur();
                    return true;
                }
            });
            $('#flowchartworkSourcesspace').flowchart('setData', defaultFlowchartData);
            sourceFirst = false;
        } else
        {
            var operatorId = 'destination_operator_' + operatorI;
            var operatorData = {
                top: bodyTop,
                left: 400,
                statusLebel: title,
                tableName: title,
                dragType: type,
                connObj: connectionObj,
                properties: {
                    body: '<div  title="' + title + '" class="visionOpLabelDiv">' + title + '</div>',
                    inputs: {
                        input_1: {
                            label: '',
                        }
                    },
                    outputs: {
                        output_1: {
                            label: '',
                        }
                    }
                }
            };
            bodyTop = bodyTop + 80;
            $('#flowchartworkSourcesspace').flowchart('createOperator', operatorId, operatorData);
        }

        operatorI++;
        $(".flowchart-operator-connector-label").hide();
        $(".flowchart-operator-title").hide();
    }
}


function addJoinTableRow(connObj, tableId, rowId)
{
    console.log("rowId :::" + rowId);
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'html',
        cache: false,
        url: 'getJoinTableRows',
        async: true,
        data: {
            sourceTables: JSON.stringify(sourceTables),
            rowId: rowId,
            connObj: connObj
        },
        success: function (response) {
            stopLoader();
            if (response != null) {
                var responseObj = JSON.parse(response);
                if (responseObj != null) {
                    $('#selectedJoinTables tr:last').after(responseObj.rowData);
                    $("#showSourceJoinIcon").html(responseObj.rowId);
                }


            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });

}
function getTreeDMTableTabs(fromTable, toTable, fromConnObj, toConnObj)
{
    var labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    if ((fromConnObj != null && !jQuery.isEmptyObject(fromConnObj)) && (toConnObj != null && !jQuery.isEmptyObject(toConnObj))) {
        $.ajax({
            type: 'post',
            traditional: true,
            dataType: 'html',
            cache: false,
            url: 'fetchTreeDMTableColumns',
            async: true,
            data: {
                fromTable: fromTable,
                toTable: toTable,
                fromConnObj: JSON.stringify(fromConnObj),
                toConnObj: JSON.stringify(toConnObj),
                sourceTables: JSON.stringify(sourceTables)
            },
            success: function (response) {
                stopLoader();
                if (response != null) {
                    var tabsDiv = "<div id='dataMigrationTabs' class='dataMigrationTabs'><ul class='dataMigrationTabsHeader'>"
                            + "<li class='dataMigrationTabsli'><a href='#tabs-1'>Mapped Columns</a></li>"
                            + "<li class='dataMigrationTabsli'><a href='#tabs-2'>Join Mapping</a></li>"
                            + "<li class='dataMigrationTabsli'><a href='#tabs-3'>Where Clause Mapping</a></li>"
                            + "<li class='dataMigrationTabsli'><a href='#tabs-4'>Sql</a></li>"
                            + "</ul>"
                            + "<div id='tabs-1' class='dataMigrationsTabsInner'>"
                            + " </div>"
                            + "<div id='tabs-2' class='dataMigrationsTabsInner'>"
                            + "</div>"
                            + "<div id='tabs-3' class='dataMigrationsTabsInner'>"
                            + "</div>"
                            + "<div id='tabs-4' class='dataMigrationsTabsInner'>"
                            + "</div>";
                    $('.visionTablesComboBox').hide();
                    $('.visionUploadFileDiv').hide();
                    $('.visionConnectToDbDiv').hide();
                    $('#visionERPMain').hide();
                    var response = JSON.parse(response);
                    if (response != null && response.connectionFlag == 'Y') {
                        $("#dialog").html(tabsDiv);
                        $("#dialog").dialog({
                            title: (labelObject['Transformation Rules'] != null ? labelObject['Transformation Rules'] : 'Transformation Rules'),
                            modal: true,
                            width: 1000,
                            height: 'auto',
                            fluid: true,
                            open: function () {
                                $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");

                                $(".visionHeaderMain").css("z-index", "999");
                                $(".visionFooterMain").css("z-index", "999");
                                $(".ui-dialog").addClass('visionDMTreePopup');
                                $("#dataMigrationTabs").jqxTabs({theme: 'ui-redmond'});

                            },
                            beforeClose: function (event, ui)
                            {
                                $(".visionHeaderMain").css("z-index", "99999");
                                $(".visionFooterMain").css("z-index", "99999");

                            }, close: function (event, ui)
                            {

                                $(this).html("");
                                $(this).dialog("close");
                                $(this).dialog("destroy");
                            }
                        });
                        var columnStr = response['columnStr'];
                        var tablesStr = response['selectedTables'];
                        var selectedJoinTables = response['selectedJoinTables'];

                        $('#tabs-3').html(tablesStr);
                        $('#tabs-1').html(columnStr);
                        $('#tabs-2').html(selectedJoinTables);
                        $('#mapColumns').addClass("active");
                        $('.visionProgressFilesSteps').hide();
                    } else {
                        showMessagePopup(response.connectionMessage);
                    }


                }
            },
            error: function (e)
            {
                sessionTimeout(e);
            }

        });
    }
}


function showMessagePopup(message) {
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    var labelObject = {};
    $("#dialog").html(message);
    $("#dialog").dialog({
        title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
        modal: true,
        width: 300,
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

function fetchTreeDMSelectedColumns() {
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    var labelObject = {};
    var fromConnObj = $('#fromConnObj').val();
    var toConnObj = $('#toConnObj').val();
    var fromTable = $('#fromTable').val();
    var toTable = $('#toTable').val();

    var i = 0;
    var colsObj = {};
    var defaultValObj = {};
    var destColArr = [];
    var flag = true;
    $("#sourceDestColsTableId tbody tr").each(function () {
        var sourceColNameStr = $("#SOURCE_SELECT_" + i).val();
        var destColNameStr = $("#DEST_SELECT_" + i).val();
        var sourceColName = "";
        var defaultVal = $("#DEFAULTVALUES_" + i).val();
        if (sourceColNameStr != null) {
            sourceColName = sourceColNameStr.split(":")[1];
            if (destColNameStr != null) {
//                destColObj["TABLE_NAME"] = destColNameStr.split(":")[0];
//                destColObj["COLUMN_NAME"] = destColNameStr.split(":")[1];

                console.log("destColArr::::" + destColArr);
                var col = destColArr.includes(destColNameStr);
                if (destColArr != null && destColArr != '' && destColArr.includes(destColNameStr)) {
                    var message = "Mapped Column is already exist";
                    showMessagePopup(message);
                    flag = false;
                    return;
                } else {
                    destColArr.push(destColNameStr);
                    if (destColNameStr != null && destColNameStr != '' && destColNameStr != 'Select') {
                        colsObj[sourceColName] = destColNameStr;
                    }

                }

            }

            if (defaultVal != null && defaultVal != '')
                defaultValObj[sourceColName] = defaultVal;
        }

        ++i;
    });
    var j = 0;
    var tablesObj = {};
    $("#selectedTables tbody tr").each(function () {
        var sourceTableName = $("#SELECT_TABLE_" + j).text();
        var tableInputValue = $("#TABLE_INPUT" + j).val();
        tablesObj[sourceTableName] = tableInputValue;
        ++j;
    });
    var k = 0;
    var joinTablesArr = [];
    $("#selectedJoinTables tbody tr").each(function () {
        var joinTablesObj = {};
        var fromJoinTable = $("#SOURCE_SELECT_JOIN_LEFT_" + k).val();
        var join = $("#TABLE_JOIN_INPUT_" + k).val();
        var joinCondition = $("#TABLE_JOIN_COND_INPUT_" + k).val();
        var toJoinTable = $("#SOURCE_SELECT_JOIN_RIGHT_" + k).val();
        joinTablesObj['fromJoinTable'] = fromJoinTable;
        joinTablesObj['join'] = join;
        joinTablesObj['joinCondition'] = joinCondition;
        joinTablesObj['toJoinTable'] = toJoinTable;
        joinTablesArr.push(joinTablesObj);
        ++k;
    });
    console.log("defaultValObj:::::::" + defaultValObj);
    if (flag) {
        if (colsObj != null && colsObj != '') {
            $.ajax({
                type: "post",
                traditional: true,
                dataType: 'html',
                cache: false,
                url: "mappingTreeDMSourceColsWithDestCols",
                data: {
                    fromConnObj: fromConnObj,
                    toConnObj: toConnObj,
                    fromTable: fromTable,
                    toTable: toTable,
                    columnsObj: JSON.stringify(colsObj),
                    tablesObj: JSON.stringify(tablesObj),
                    defaultValObj: JSON.stringify(defaultValObj),
                    joinTablesObj: JSON.stringify(joinTablesArr),
                    sourceTables: JSON.stringify(sourceTables)
                },
                success: function (response) {
                    if (response != null)
                    {

                        var result = JSON.parse(response);
                        if (result != null && result.connectionFlag == 'Y')
                        {
                            //var message = result.Message;
                            $('#dialog').html(result.Message);
                        } else {
                            $('#dialog').html(result.connectionMessage);
                        }
                        $("#dialog").dialog({
                            title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
                            modal: true,
                            width: 300,
                            height: 135,
                            fluid: true,
                            buttons: [{
                                    text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                                    click: function () {
                                        $('.visionProgressSteps').hide();
                                        $('.visionProgressFilesSteps').hide();
                                        $('#mappingColumns').hide();
                                        $('#visionERPMain').hide();
                                        $("#flowchartworkSourcesspace").val("");
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
    }



}

function getDMTableColumnsData(fromTable, toTable, fromConnObj, toConnObj)
{
    var labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }

    if ((fromConnObj != null && !jQuery.isEmptyObject(fromConnObj)) && (toConnObj != null && !jQuery.isEmptyObject(toConnObj))) {
        $.ajax({
            type: 'post',
            traditional: true,
            dataType: 'html',
            cache: false,
            url: 'fetchTreeDMTableColumns',
            async: true,
            data: {
                fromTable: fromTable,
                toTable: toTable,
                fromConnObj: JSON.stringify(fromConnObj),
                toConnObj: JSON.stringify(toConnObj),
                sourceTables: JSON.stringify(sourceTables)
            },
            success: function (response) {
                stopLoader();
                if (response != null) {
                    var tableData = "<div id='mappingColumns' class='visionColumnsMapping' style='display:none'>"
                            //            +    "<div id= 'showConnectionName' style='margin-left:15px;margin-top:15px'>  </div>"
                            + "<div id='showSourceTablesList' class='visionSourceTablesMain'></div>"
                            + "<div id='showSourceJoinIcon' class='visionSourceJoinIcon'></div>"
                            + "<div id='showSourceTablesJoinList' class='visionSourceTablesMain'></div>"
                            + "<div class='visionMappedTable' id= 'MappedTable'></div>"
                            + "<div class='visionProcessCols'>"
                            + "<input type='button' value='Process' name='Process' id='processCols' onclick = 'fetchTreeDMSelectedColumns()' class='visionProcessColsBtn'>"
                            + "</div>"
                            + "</div>";
                    $('.visionTablesComboBox').hide();
                    $('.visionUploadFileDiv').hide();
                    $('.visionConnectToDbDiv').hide();
                    $('#visionERPMain').hide();
                    var response = JSON.parse(response);
                    if (response != null && response.connectionFlag == 'Y') {
                        $("#dialog").html(tableData);
                        $("#dialog").dialog({
                            title: (labelObject['Table Columns'] != null ? labelObject['Table Columns'] : 'Table Columns'),
                            modal: true,
                            width: 900,
                            height: 'auto',
                            fluid: true,
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

                            }, close: function (event, ui)
                            {

                                $(this).html("");
                                $(this).dialog("close");
                                $(this).dialog("destroy");
                            }
                        });


                        var columnStr = response['columnStr'];
                        var tablesStr = response['selectedTables'];
                        var selectedJoinTables = response['selectedJoinTables'];
                        var showSourceJoinIcon = response['showSourceJoinIcon'];
                        $('#mappingColumns').show();
                        $('#showSourceTablesList').html(tablesStr);
                        $('#MappedTable').html(columnStr);
                        // var matchedSelectStr = response['matchedSelectStr'];
                        $("#showSourceJoinIcon").html(showSourceJoinIcon);
                        $('#showSourceTablesJoinList').html(selectedJoinTables);
                        $('#mapColumns').addClass("active");
                        $('.visionProgressFilesSteps').hide();
                    } else {
                        showMessagePopup(response.connectionMessage);
                    }


                }
            },
            error: function (e)
            {
                sessionTimeout(e);
            }

        });
    }
}
function getDMFileColumnsData(fromTable, selectType, fromConnObj)
{
    var labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }

    if (fromConnObj != null && !jQuery.isEmptyObject(fromConnObj)) {
        $("#selectType").val(selectType);
        if (selectType == 'XLSX' || selectType == 'XLS') {
            //$("#downloadData").attr("action");
            $("#downloadData").attr("action", "exportDMXlsxData");
            $("#downloadData").submit();
        } else if (selectType == 'CSV') {
            $("#downloadData").attr("action", "exportDMCSVData");
            $("#downloadData").submit();
        } else if (selectType == 'PDF') {
            $("#downloadData").attr("action", "exportPDFData");
            // $("#downloadData").submit();
        } else if (selectType == 'XML') {
            $("#downloadData").attr("action", "exportDMXMLData");
            $("#downloadData").submit();

        }
    }
}


function getDMFileDataColumnsData(fromTable, toTable, connectionObj)
{
    var labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    if (connectionObj != null && !jQuery.isEmptyObject(connectionObj)) {
        $.ajax({
            type: 'post',
            traditional: true,
            dataType: 'html',
            cache: false,
            url: 'fetchTreeDMTableFileColumns',
            async: true,
            data: {
                fromTable: fromTable,
                toTable: toTable,
                fromConnObj: JSON.stringify(connectionObj),
            },
            success: function (response) {
                stopLoader();
                if (response != null) {
                    var tableData = "<div id='mappingColumns' class='visionColumnsMapping' style='display:none'>"
                            //            +    "<div id= 'showConnectionName' style='margin-left:15px;margin-top:15px'>  </div>"
                            + "<div id='showSourceTablesList' class='visionSourceTablesMain'></div>"
                            + "<div class='visionMappedTable' id= 'MappedTable'></div>"
                            + "<div class='visionProcessCols'>"
                            + "<input type='button' value='Process' name='Process' id='processCols' onclick = 'fetchTreeDMFileSelectedColumns()' class='visionProcessColsBtn'>"
                            + "</div>"
                            + "</div>";
                    $('.visionTablesComboBox').hide();
                    $('.visionUploadFileDiv').hide();
                    $('.visionConnectToDbDiv').hide();
                    $('#visionERPMain').hide();
                    var response = JSON.parse(response);
                    if (response != null && response.connectionFlag == 'Y') {
                        $("#dialog").html(tableData);
                        $("#dialog").dialog({
                            title: (labelObject['Table Columns'] != null ? labelObject['Table Columns'] : 'Table Columns'),
                            modal: true,
                            width: 800,
                            height: 'auto',
                            fluid: true,
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

                            }, close: function (event, ui)
                            {

                                $(this).html("");
                                $(this).dialog("close");
                                $(this).dialog("destroy");
                            }
                        });


                        var columnStr = response['columnStr'];
                        var tablesStr = response['selectedTables'];
                        $('#mappingColumns').show();
                        $('#showSourceTablesList').html(tablesStr);
                        $('#MappedTable').html(columnStr);
                        var matchedSelectStr = response['matchedSelectStr'];
                        $('#mapColumns').addClass("active");
                        $('.visionProgressFilesSteps').hide();
                    } else {
                        showMessagePopup(response.connectionMessage);
                    }


                }
            },
            error: function (e)
            {
                sessionTimeout(e);
            }

        });
    }
}
function showUploadedFiles(treeId, divId)
{
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: "getTreeParentFile",
        cache: false,
        data: {
            treeId: treeId,
            divId: divId,
        },
        success: function (data, status, xhr) {
            if (data != null && data != '' && data != undefined) {
                var treeConfigObj = data['treeConfigObj'];
                var dragEndFunction = data['dragEnd'];
                treeConfigObj.allowDrag = true;
                treeConfigObj.allowDrop = true;
                treeConfigObj.dragEnd = eval('(' + dragEndFunction + ')');
                $('#' + divId).jqxTree(treeConfigObj);
                $('#' + divId).jqxTree('focus');
                $('#' + divId).on('expand', function (event) {
                    var parentItem = $('#' + divId).jqxTree('getItem', event.args.element);
                    var $element = $(event.args.element);
                    var loader = false;
                    var loaderItem = null;
                    var children = $element.find('ul:first').children();
                    $.each(children, function () {
                        var item = $('#' + divId).jqxTree('getItem', this);
                        if (item && item.value == 'ajax') {
                            loaderItem = item;
                            loader = true;
                            return false;
                        }

                    });
                    if (loaderItem != null) {
                        var level = parentItem.level;
                        $('#' + divId).jqxTree('removeItem', loaderItem.element);
                        console.log("level is :::" + level);
                        $.ajax({
                            type: "post",
                            traditional: true,
                            dataType: 'json',
                            url: "getTreeUploadedFiles",
                            cache: false,
                            data: {
                                treeId: divId,
                            },
                            success: function (data, status, xhr) {
                                if (data != null && data != '' && data != undefined) {
                                    $('#' + divId).jqxTree('addTo', data, $element[0]);
                                    var items = $('#' + divId).jqxTree('getItems');
                                    $.each(items, function () {
                                        $(this.titleElement).attr('title', this.label);

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


function fetchTreeDMFileSelectedColumns() {
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    var labelObject = {};
    var fromConnObj = $('#fromConnObj').val();
    var fromTable = $('#fromTable').val();
    var toTable = $('#toTable').val();

    var i = 0;
    var mappedFileColsObj = {};
    var defaultValObj = {};
    $("#sourceDestColsFilesTableId tbody tr").each(function () {
        var destColNameStr = $("#DEST_FILE_SELECT_" + i).val();
        var sourceColNameStr = $("#SOURCE_FILE_SELECT_" + i).val();
        var defaultVal = $("#DEFAULTVALUESFILES_" + i).val();
        if (destColNameStr != null && destColNameStr != '' && destColNameStr != 'Select'
                && sourceColNameStr != null && sourceColNameStr != '' && sourceColNameStr != 'Select') {
            mappedFileColsObj[destColNameStr] = sourceColNameStr;
        }
        if (defaultVal != null && defaultVal != '') {
            defaultValObj[destColNameStr] = defaultVal;
        }

        ++i;
    });

    if (mappedFileColsObj != null && !jQuery.isEmptyObject(mappedFileColsObj)) {
        $.ajax({
            type: "post",
            traditional: true,
            dataType: 'html',
            cache: false,
            url: "mappingTreeDMFileColsWithDestCols",
            data: {
                fromConnObj: fromConnObj,
                fromTable: fromTable,
                toTable: toTable,
                'mappedFileColsObj': JSON.stringify(mappedFileColsObj),
                'defaultValObj': JSON.stringify(defaultValObj),
            },
            success: function (response) {
                if (response != null)
                {
                    $('#dialog').html(response);
                    $("#dialog").dialog({
                        title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
                        modal: true,
                        width: 300,
                        height: 135,
                        fluid: true,
                        buttons: [{
                                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                                click: function () {
                                    $('.visionProgressSteps').hide();
                                    $('.visionProgressFilesSteps').hide();
                                    $('#mappingColumns').hide();
                                    $('#visionERPMain').hide();
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




}

function treeSavedConnections(treeObj, treeId)
{
//    var connectionObj;
    var treeConfigObj = treeObj['treeConfigObj'];
    var dragEndFunction = treeObj['dragEndFunction'];
    treeConfigObj.allowDrag = true;
    treeConfigObj.allowDrop = true;
    treeConfigObj.hasThreeStates = true;
    treeConfigObj.checkboxes = true;
    treeConfigObj.dragEnd = eval('(' + dragEndFunction + ')');
    var columnsObj = treeObj['treeColumnObj'];
    $('#' + treeId).jqxTree(treeConfigObj);
    $('#' + treeId).jqxTree('focus');
    var filterItems = ['TABLES', 'VIEWS', 'SYNONYMS'];
    $('#' + treeId).on('select', function (event) {
        var selectedItem = $('#' + treeId).jqxTree('getItem', event.args.element);
        var $selectedElement = $(event.args.element);
        if (selectedItem != null && !jQuery.isEmptyObject(selectedItem) && selectedItem['value'] != null
                && selectedItem['value'] != '' && filterItems.indexOf(selectedItem['value']) > -1) {
            selectColumnsObj = columnsObj
            var parentSelectId = selectedItem['parentId'];
            var parentDiv = $("#" + parentSelectId);
            var childObj = parentDiv[0].children[1];
            var valueObj = childObj.treeItem['value'];
            selectedDataBase = valueObj;
            if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
            {

                var selectConnsObj = savedDBData[valueObj];
                if (selectConnsObj != null && !jQuery.isEmptyObject(selectConnsObj))
                {
                    selectConnObj = selectConnsObj;
                }
            }
            var hiddenFieldId = "DATABASE_" + selectedDataBase + "_" + selectedItem.value + "_hidden";
            hiddenFieldId = hiddenFieldId.replace(/ /g, '_');
            hiddenFieldId = hiddenFieldId.replace(/\//g, '_');
            var filterValue = $("#" + hiddenFieldId).val();
            if (filterValue != null && filterValue != '' && filterValue != undefined)
            {
                $("#treeETLFilterImage").attr('src', "images/Filter Icon2-01.svg");
            } else
            {
                $("#treeETLFilterImage").attr('src', "images/Filter Icon-01.svg");
            }
        } else if (selectedItem != null && !jQuery.isEmptyObject(selectedItem) && selectedItem['value'] != null
                && selectedItem['value'] != '' && selectedItem['value'] == "Show More") {
            selectColumnsObj = columnsObj;
            var valueObj;
            var selectedValue;
            var selectedLevel;
            var parentElement = event.args.element.parentElement.parentElement;
            var parent = $('#' + treeId).jqxTree('getItem', parentElement);
            var $selectedElement = $(parentElement);
            if (parent != null) {
                selectedValue = parent.value;
                selectedLevel = parent.level;
                var parentSelectId = parent['parentId'];
                var parentDiv = $("#" + parentSelectId);
                var childObj = parentDiv[0].children[1];
                valueObj = childObj.treeItem['value'];
            }
            if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
            {

                var selectConnsObj = savedDBData[valueObj];
                if (selectConnsObj != null && !jQuery.isEmptyObject(selectConnsObj))
                {
                    selectConnObj = selectConnsObj;
                }
            }
            var parentEventItem = parentElement;
            for (var i = selectedLevel; i > 1; i--)
            {
                parentEventItem = parentEventItem.parentElement.parentElement;
            }
            var selectedItem = $('#' + treeId).jqxTree('getItem', parentEventItem);
            var selectedParentValue = selectedItem['value'];
            treePaging(selectedValue, selectConnObj, $selectedElement[0], selectedLevel, selectColumnsObj, valueObj, selectedParentValue);
//            treePaging(selectedValue, selectConnObj, $selectedElement[0], selectedLevel, selectColumnsObj, valueObj);
        }
    });
    $('#' + treeId).on('expand', function (event) {
        var connectionObj;
        var parentItem = $('#' + treeId).jqxTree('getItem', event.args.element);
        //------------ file expand code
        if (parentItem.level == '2') {
            globalFileTreeItem[parentItem.value.toUpperCase()] = parentItem.element;
        }

        var $element = $(event.args.element);
        var loader = false;
        var loaderItem = null;
        var children = $element.find('ul:first').children();
        $.each(children, function () {
            var item = $('#' + treeId).jqxTree('getItem', this);
            if (item && item.value == 'ajax') {
                loaderItem = item;
                loader = true;
                return false
            }
        });
        if (loaderItem != null) {
            showLoader();
            var extTreeParams = $("#extTreeParams").val();
            var value;
            var level = parentItem.level;
            $('#' + treeId).jqxTree('removeItem', loaderItem.element);
            if (parentItem.level > 3)
            {
                //var prevObj = parentItem['prevItem'];
//                value = parentItem['value'];
                var parentEventItem = event.args.element;
                for (var i = level; i > 3; i--)
                {
                    parentEventItem = parentEventItem.parentElement.parentElement;
                }
                var selectedItem = $('#' + treeId).jqxTree('getItem', parentEventItem);
                value = selectedItem['value'];
            }

            if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
            {
                var conObj = savedDBData[value];
                if (conObj != null && !jQuery.isEmptyObject(conObj)) {
                    connectionObj = conObj;
                }

            }
            var data = {
                parentkey: parentItem.value,
                treeId: treeObj['treeId'],
                level: parentItem.level,
                extTreeParams: extTreeParams,
                columnsObj: JSON.stringify(columnsObj),
                connectionObj: JSON.stringify(connectionObj),
                startIndex: 0,
                endIndex: $("#treePageSize").val()
            };
            // -------------  JAGADISH CODE START ------------ line 1764

            if (parentItem.level == '3') { // JAGADISH CODE START ETL_QUERY 
                var superParentEle = parentItem['parentElement'];
                var superParentValue = superParentEle['children'][2];
                data['superParentValue'] = superParentValue['innerText'];
            }
            // JAGADISH CODE END
            var hiddenFieldId = "DATABASE_" + value + "_" + parentItem.value + "_hidden";
            var hiddenPagingId = "DATABASE_" + value + "_" + parentItem.value + "paging__hidden";
            hiddenFieldId = hiddenFieldId.replace(/ /g, '_');
            hiddenPagingId = hiddenPagingId.replace(/ /g, '_');
            hiddenFieldId = hiddenFieldId.replace(/\//g, '_');
            hiddenPagingId = hiddenPagingId.replace(/\//g, '_');
            var hiddenPagingField = "<input type='hidden' id='" + hiddenPagingId + "'value=''/>";
            $("#savedConnections").append(hiddenPagingField);
            var prevFilterVal = $("#" + hiddenFieldId).val();
            if (prevFilterVal != null && prevFilterVal != '' && prevFilterVal != undefined)
            {
                prevFilterVal = JSON.parse(prevFilterVal);
                data['filterValue'] = prevFilterVal['filterType'];
                data['filterCondition'] = prevFilterVal['filterValue'];
            }
            var url = "getTreePagingDataOpt";
            var treePagingObj = {};
            treePagingObj['startIndex'] = '0';
            treePagingObj['endIndex'] = $("#treePageSize").val();
            $("#" + hiddenPagingId).val(JSON.stringify(treePagingObj));
//            var url = "getTreeDataOpt";
            var filesParentElement = parentItem['parentElement'];

            var selectedFilesItem = $('#' + treeId).jqxTree('getItem', filesParentElement);
            if (parentItem.value == 'FILES'
                    || (selectedFilesItem != null
                            && selectedFilesItem['value'] == 'FILES'))
            {
                url = 'getTreeUploadedFiles';
            }
            if (parentItem.level > 3)
            {
                var parentEventItem = event.args.element;
                for (var i = level; i > 1; i--)
                {
                    parentEventItem = parentEventItem.parentElement.parentElement;
                }
                var selectedItem = $('#' + treeId).jqxTree('getItem', parentEventItem);
                var selectedParentValue = selectedItem['value'];
                if (selectedParentValue != null && selectedParentValue != '' &&
                        selectedParentValue != undefined && selectedParentValue == 'ERP')
                {
                    url = 'getTreeErpConnectionDetails';
                }
            }
            $.ajax({
                type: "post",
                traditional: true,
                dataType: 'json',
                url: url,
                cache: false,
                data: data,
                success: function (data, status, xhr) {
                    stopLoader();
                    if (parentItem.level == 2) {
                        var parentParentItem = $('#' + treeId).jqxTree("getItem", parentItem.parentElement);
                        if (parentParentItem.value != "FILES") {

                            if (data != null)
                            {
                                savedDBData = data[0].connectionObj;
                            }
                        }
                    }

                    $('#' + treeId).jqxTree('addTo', data, $element[0]);
                    var items = $('#' + treeId).jqxTree('getItems');
                    $.each(items, function () {
                        $(this.titleElement).attr('title', this.label);
                        $("#" + this.titleElement[0].id).removeClass('visionETLParentHighight');
                        if (this.value == "Show More")
                        {
                            var childLength = this.element.children['length'];
                            var expandDiv = this.element.children[0];
                            if (childLength > 3) {
                                var div = this.element.children[1];
                                var checkBoxDiv = this.element.children[2];
                                $("#" + expandDiv.id).remove();
                                $("#" + div.id).remove();
                                $("#" + checkBoxDiv.id).addClass('visionETLCheckboxEmptySpace');
                            } else
                            {
                                $("#" + expandDiv.id).remove();
                            }
                        }
                    });
                    if (parentItem != null)
                    {
                        var parentItemEle = event.args.element;
                        for (var p = level; p >= 0; p--)
                        {
                            var parentItemId = $('#' + treeId).jqxTree('getItem', parentItemEle);
                            if (parentItemId != null)
                            {
                                var divItemId = parentItemId.titleElement[0];
                                $("#" + divItemId.id).addClass('visionETLParentHighight');
                            }
                            parentItemEle = parentItemEle.parentElement.parentElement;
                        }
                    }
                    $('#mainSplitter').resize();
                },
                error: function (e) {
                    console.log(e);
                    sessionTimeout(e);
                    stopLoader();
                }
            });
        }



    });
}

function treeRefreshSavedConnections()
{
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: "getTreeDataPiping",
        cache: false,
        data: {
            treeId: 'MM_SOURCE_SAVED_CONNECTION_TREE',
        },
        success: function (response) {

            if (response != null && !jQuery.isEmptyObject(response))
            {
                $("#savedConnections").html("");
                treeSavedConnections(response, 'savedConnections');
            }

        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
}

function treeDestinationConnections()
{
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: "getTreeDataPiping",
        cache: false,
        data: {
            treeId: 'MM_SOURCE_SAVED_CONNECTION_TREE',
        },
        success: function (response) {

            if (response != null && !jQuery.isEmptyObject(response))
            {
                $("#descSplitterContainerHeader").html("");
                treeSavedConnections(response, 'descSplitterContainerHeader');
            }

        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
}


function treeSourcesMenus(menuId, treeId)
{
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    var labelObject = {};
    $.ajax({
        type: 'POST',
        traditional: true,
        dataType: 'html',
        cache: false,
        url: 'getTreeDataMigrationMenus',
        data: {menuId: menuId},
        success: function (response) {
            stopLoader();
            if (response != null) {
//                console.log("response:::" + response);
                response = JSON.parse(response);
                var menuResult = response.menuResult;
                var treeMenuData = JSON.parse(menuResult);
                var source =
                        {
                            datatype: "json",
                            datafields: [
                                {name: 'MENU_ID'},
                                {name: 'PARENT_ID'},
                                {name: 'PARENT_MENU_ID'},
                                {name: 'MENU_DESCRIPTION'},
                                {name: 'icon'},
                                {name: 'value'},
                                {name: 'iconsize'},
                                {name: 'TOOL_TIP'},
                                {name: 'MAIN_DESCRIPTION'}
                            ],
                            localdata: treeMenuData
                        };
                // create data adapter.
                var dataAdapter = new $.jqx.dataAdapter(source);
                // perform Data Binding.
                dataAdapter.dataBind();
                var records = dataAdapter.getRecordsHierarchy('MENU_ID',
                        'PARENT_ID',
                        'items',
                        [{name: 'MENU_DESCRIPTION', map: 'html'}]
                        );
//                        'MENU_ID','PARENT_MENU_ID','TOOL_TIP');

                if (treeId != null && treeId != "" && treeId != undefined && treeId == "savedSources")
                {
                    $('#' + treeId).jqxTree({source: records, allowDrag: true, allowDrop: true, height: '90%', width: '100%', toggleMode: 'click', theme: 'energyblue',
                        dragEnd: function (item) {
                            createImageSourcesFlowchart(item, item.value, 'File');
                            return true;
                        }
                    });

                } else if (treeId != null && treeId != "" && treeId != undefined && treeId == "descSplitterContainer")
                {
                    $('#' + treeId).jqxTree({source: records, allowDrag: true,
                        allowDrop: true, height: '90%', width: '100%',
                        toggleMode: 'click', theme: 'energyblue',
                        dragEnd: function (item) {
                            createImageDestinationFlowchart(item, item.value, 'File');
                            return true;
                        }
                    });
                }
                $('#' + treeId).css('visibility', 'visible');
                $('#' + treeId).on('select', function (event) {
                    var item = $('#' + treeId).jqxTree('getItem', event.args.element);
                    if (item != null && !jQuery.isEmptyObject(item) && item['value'] != null
                            && item['value'] != ''
                            && item['value'] != '#') {
                        eval(item['value']);
                    }

                });
                $('.visionProgressSteps').hide();
            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }
    });
}

function getTreeDataBase(type, typeName)
{
    console.log(type + ":::iam in getTreeDataBasemethod:::" + typeName);
    if (type != null && type != "" && type != undefined && type == "FILE")
    {
        var response = "<div id ='treeDMFileId' class ='treeDMFileDivClass'>"
                + "<div id='visionShowFileUploadMsg'></div>";
        response += "<input type='file' name='importTreeDMFile'  id='importTreeDMFile' class='visionTreeDMFilesInput'/>";
        response += "<div class='visionTreeDMFileUploadclass' id='visionTreeDmFileUpload'>";
        response += "<input type='hidden' id='selectedTreeTypeName' value=''>";
        response += "<input type='hidden' id='selectedTreeType' value=''>";
        response += "<p class='VisionDMUploadFileContent'>Upload Files(" + typeName + ") Here</p></div></div>";
        uploadFilePopup(response, type, typeName);
    } else if (type != null && type != "" && type != undefined && type == "DB") {
        var formString = "<div class='visionEtlConnectDbMain'>"
                + "<div id='visionShowConnectionMsg'></div>"
                + "<table class='visionEtlDbTable' autocomplete='false'>"
                + "<tr><td><p style='font-weight:bold'>Connection Type:</p></td>"
                + "<td>" + typeName + "</td></tr>"
                + "<tr>"
                + "<td> <label class='visionDbLabels'>Connection Name</label></td>"
                + "<td> <input type='text'  name='ConnectionName' id='DbEtlConnectionName' class='visionInputDbFields' autocomplete='false'>"
                + "<div class='dataMigrationInputError' id='DbEtlConnectionNameError'></div></td>"
                + " </tr>"
                + "<tr>"
                + "<td> <label class='visionDbLabels'>Host Name</label></td>"
                + "<td> <input type='text'  name='HostName' id='DbEtlHostName' class='visionInputDbFields' autocomplete='false'>"
                + "<div class='dataMigrationInputError' id='DbEtlHostNameError'></div></td>"
                + "</tr>"
                + "<tr>"
                + "<td>  <label class='visionDbLabels'>Port</label></td>"
                + "<td><input type='text' value='' name='Port' id='DbEtlPort' class='visionInputDbFields' autocomplete='false'>"
                + "<div class='dataMigrationInputError' id='DbEtlPortError'></div></td>"
                + "</tr>"
                + "<tr>"
                + "<td>  <label class='visionDbLabels'>Username</label></td>"
                + "<td> <input type='text'  name='EtlUsername' id='DbEtlUserName' class='visionInputDbFields' autocomplete='false'>"
                + "<div class='dataMigrationInputError' id='DbEtlUserNameError'></div></td>"
                + "</tr>"
                + " <tr>"
                + "<td>  <label class='visionDbLabels'>Password</label></td>"
                + "<td>    <input type='password'   name='EtlPassword' id='DbEtlPassword' class='visionInputDbFields' autocomplete='false'>"
                + "<div class='dataMigrationInputError' id='DbEtlPasswordError'></div></td>"
                + "</tr>"
                + "<tr>"
                + "<td>  <label class='visionDbLabels'>Database/Service Name</label></td>"
                + "<td>    <input type='text'  name='ServiceName' id='DbEtlServiceName' class='visionInputDbFields' autocomplete='false'>"
                + "<div class='dataMigrationInputError' id='DbEtlServiceNameError'></div></td>"
                + "</tr>"
                + "<tr style='display:none'>"
                + "<td>  <label class='visionDbLabels'>Audit Id</label></td>"
                + "<td>    <input type='hidden'  name='auditId' id='EtlAuditId' class='visionInputDbFields'></td>"
                + "</tr>"
                + "<tr><td><input type='checkbox' name='checkBoxDetails' id = 'EtlCheckBoxChecked' value='checked' checked>Save Details"
                + "<div class='visionDataMigrationError' style='display:none'>Please check the box</div></td></tr>"
                + "<tr>"
                + "<td class='visionDbConnectBtn' id='connectEtlDbTd' colspan = '2'><input type='button' value='Connect' name='Connect'  onclick = \"connectEtlDatabase('" + type + "','" + typeName + "')\" class='visionInputDbButton'></td>"

                + "</tr>"
                + "</table></div>";
        ShowEtlConnectionPopup(formString, type, typeName, 'Connect Database');
    } else if (type != null && type != "" && type != undefined && type == "ERP" && typeName == "SAP") {
        var erpTable = "<div class='visionEtlErpDiv'>"
                + "<div id='visionShowErpEtlMsg'></div>"
                + "<table class='visionERPEtlTable'>"
                + "<tr><td><p style='font-weight:bold'>Connection Type:</p></td>"
                + "<td>" + typeName + "</td></tr>"
                + " <tr class='visionERPDbTr'>"
                + "<td class='visionERPDbTd'> <label class='visionERPDbLabels'>Connection Name</label></td>"
                + "<td class='visionERPDbTd'> <input type='text' value='' name='ConnectionName' id='ErpEtlDbConnectionName' class='visionInputDbFields'>"
                + "<div class='dataMigrationInputError' id='ErpEtlDbConnectionNameError'></div></td>"
                + " </tr>"
                + " <tr class='visionERPDbTr'>"
                + "<td class='visionERPDbTd'> <label class='visionERPDbLabels'>Client</label></td>"
                + "<td class='visionERPDbTd'> <input type='text' value='' name='Client' id='ERPEtlClientName' class='visionInputDbFields'>"
                + "<div class='dataMigrationInputError' id='ERPEtlClientNameError'></div></td>"
                + " </tr>"
                + "<tr class='visionERPDbTr'>"
                + "<td class='visionERPDbTd'> <label class='visionERPDbLabels'>Host Name</label></td>"
                + "<td class='visionERPDbTd'> <input type='text' value='' name='ERP HostName' id='ERPEtlHostName' class='visionInputDbFields'>"
                + "<div class='dataMigrationInputError' id='ERPEtlHostNameError'></div></td>"
                + "</tr>"
                + "<tr class='visionERPDbTr'>"
                + "<td class='visionERPDbTd'>  <label class='visionERPDbLabels'>Username</label></td>"
                + "<td class='visionERPDbTd'> <input type='text' value='' name='Username' id='ERPEtlUserName' class='visionInputDbFields'>"
                + "<div class='dataMigrationInputError' id='ERPEtlUserNameError'></div></td>"
                + "</tr>"
                + " <tr class='visionERPDbTr'>"
                + "<td class='visionERPDbTd'>  <label class='visionERPDbLabels'>Password</label></td>"
                + "<td class='visionERPDbTd'>    <input type='password' value='' name='Password' id='ERPEtlPassword' class='visionInputDbFields'>"
                + "<div class='dataMigrationInputError' id='ERPEtlPasswordError'></div></td>"
                + "</tr>"
                + "<tr class='visionERPDbTr'>"
                + "<td class='visionERPDbTd'>  <label class='visionERPDbLabels'>Language Id</label></td>"
                + "<td class='visionERPDbTd'>    <input type='text' value='' name='languageId' id='ERPEtlLanguageId' class='visionInputDbFields'>"
                + "<div class='dataMigrationInputError' id='ERPEtlLanguageIdError'></div></td>"
                + "</tr>"
                + "<tr class='visionERPDbTr'>"
                + "<tr class='visionERPDbTr' style='display:none'>"
                + "<td class='visionERPDbTr'>  <label class='visionERPDbLabels'>Audit Id</label></td>"
                + "<td class='visionERPDbTr'>    <input type='hidden' value='' name='auditId' id='ErpEtlauditId' class='visionInputDbFields'></td>"
                + "</tr>"
                + "<tr class='visionERPDbTr'>"
                + "<td class='visionERPDbTd'>  <label class='visionERPDbLabels'>System Id</label></td>"
                + "<td class='visionERPDbTd'>    <input type='text' value='' name='ERPSystemId' id='ERPEtlSystemId' class='visionInputDbFields'>"
                + "<div class='dataMigrationInputError' id='ERPEtlSystemIdError'></div></td>"
                + "</tr>"
                + "<tr class='visionERPDbTr'><td><input type='checkbox' name='checkBoxDetails' id = 'checkBoxChecked' value='checked' checked>Save Details"
                + "<div class='visionDataMigrationError' style='display:none'>Please check the box</div></td></tr>"
                + "<tr class='visionERPDbTr'>"
                + "<td class='visionERPDbTd visionERPDbConnectBtn' id='connectERPEtlDbTd' colspan = '2'><input type='button' value='Connect' name='Connect'  onclick = \"connectErpEtlDatabase('" + type + "','" + typeName + "')\" class='visionInputDbButton'></td>"

                + "</tr>"
                + "</table>";
        ShowEtlConnectionPopup(erpTable, type, typeName, 'Connect ERP');

    } else if (typeName == 'Oracle_ERP') {
        var formString = "<div class='visionEtlConnectDbMain'>"
                + "<div id='visionShowConnectionMsg'></div>"
                + "<table class='visionErpEtlDbTable' autocomplete='false'>"
                + "<tr><td><p style='font-weight:bold'>Connection Type:</p></td>"
                + "<td>" + typeName + "</td></tr>"
                + "<tr>"
                + "<td> <label class='visionDbLabels'>Connection Name</label></td>"
                + "<td> <input type='text'  name='ConnectionName' id='DbEtlConnectionName' class='visionInputDbFields' autocomplete='false'>"
                + "<div class='dataMigrationInputError' id='DbEtlConnectionNameError'></div></td>"
                + " </tr>"
                + "<tr>"
                + "<td> <label class='visionDbLabels'>Host Name</label></td>"
                + "<td> <input type='text'  name='HostName' id='DbEtlHostName' class='visionInputDbFields' autocomplete='false'>"
                + "<div class='dataMigrationInputError' id='DbEtlHostNameError'></div></td>"
                + "</tr>"
                + "<tr>"
                + "<td>  <label class='visionDbLabels'>Port</label></td>"
                + "<td><input type='text' value='' name='Port' id='DbEtlPort' class='visionInputDbFields' autocomplete='false'>"
                + "<div class='dataMigrationInputError' id='DbEtlPortError'></div></td>"
                + "</tr>"
                + "<tr>"
                + "<td>  <label class='visionDbLabels'>Username</label></td>"
                + "<td> <input type='text'  name='EtlUsername' id='DbEtlUserName' class='visionInputDbFields' autocomplete='false'>"
                + "<div class='dataMigrationInputError' id='DbEtlUserNameError'></div></td>"
                + "</tr>"
                + " <tr>"
                + "<td>  <label class='visionDbLabels'>Password</label></td>"
                + "<td>    <input type='password'   name='EtlPassword' id='DbEtlPassword' class='visionInputDbFields' autocomplete='false'>"
                + "<div class='dataMigrationInputError' id='DbEtlPasswordError'></div></td>"
                + "</tr>"
                + "<tr>"
                + "<td>  <label class='visionDbLabels'>Database/Service Name</label></td>"
                + "<td>    <input type='text'  name='ServiceName' id='DbEtlServiceName' class='visionInputDbFields' autocomplete='false'>"
                + "<div class='dataMigrationInputError' id='DbEtlServiceNameError'></div></td>"
                + "</tr>"
                + "<tr style='display:none'>"
                + "<td>  <label class='visionDbLabels'>Audit Id</label></td>"
                + "<td>    <input type='hidden'  name='auditId' id='EtlAuditId' class='visionInputDbFields'></td>"
                + "</tr>"
                + "<tr><td><input type='checkbox' name='checkBoxDetails' id = 'EtlCheckBoxChecked' value='checked' checked>Save Details"
                + "<div class='visionDataMigrationError' style='display:none'>Please check the box</div></td></tr>"
                + "<tr>"
                + "<td class='visionDbConnectBtn' id='connectEtlDbTd' colspan = '2'><input type='button' value='Connect' name='Connect'  onclick = \"connectEtlDatabase('" + type + "','" + typeName + "')\" class='visionInputDbButton'></td>"

                + "</tr>"
                + "</table></div>";
        ShowEtlConnectionPopup(formString, type, typeName, 'Connect ERP');
    } else if (type == 'WEB_SERVICE') {
        if (typeName == 'SOAP') {
            var formString = "<div class='visionEtlConnectDbMain'>"
                    + "<div id='visionShowConnectionMsg'></div>"
                    + "<table class='visionErpEtlDbTable' autocomplete='false'>"
                    + "<tr><td><p style='font-weight:bold'>Webservice Type:</p></td>"
                    + "<td>" + typeName + "</td></tr>"
                    + "<tr>"
                    + "<td> <label class='visionDbLabels'>WSDL URL</label></td>"
                    + "<td> <input type='text'  name='wsdlURL' id='wsdlURL' class='visionInputDbFields' autocomplete='false'>"
                    + "<div class='dataMigrationInputError' id='wsdlURLError'></div></td>"
                    + " </tr>"
                    + "<tr>"
                    + "<td> <label class='visionDbLabels'>End Point URL</label></td>"
                    + "<td> <input type='text'  name='endPointURL' id='endPointURL' class='visionInputDbFields' autocomplete='false'>"
                    + "<div class='dataMigrationInputError' id='endPointURLError'></div></td>"
                    + "</tr>"
                    + "<tr>"
                    + "<tr style='display:none'>"
                    + "<td>  <label class='visionDbLabels'>Audit Id</label></td>"
                    + "<td>    <input type='hidden'  name='auditId' id='EtlAuditId' class='visionInputDbFields'></td>"
                    + "</tr>"
                    + "<tr><td><input type='checkbox' name='checkBoxDetails' id = 'EtlCheckBoxChecked' value='checked' checked>Save Details"
                    + "<div class='visionDataMigrationError' style='display:none'>Please check the box</div></td></tr>"
                    + "<tr>"
                    + "<td class='visionDbConnectBtn' id='connectEtlDbTd' colspan = '2'><input type='button' value='Import' name='Import'  onclick = \"connectEtlWSSOAP('" + type + "','" + typeName + "')\" class='visionInputDbButton'></td>"

                    + "</tr>"
                    + "</table></div>";
        } else if (typeName == 'REST') {

        }
        ShowEtlConnectionPopup(formString, type, typeName, 'Connect Webservice');
    }
}

function uploadFilePopup(response, type, typeName)
{
    var labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    $("#dialog").html(response);
    $("#dialog").dialog({
        title: (labelObject['Upload'] != null ? labelObject['Upload'] : 'Upload'),
//        modal: true,
        width: 500,
        height: 350,
        fluid: true,
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
    $("#selectedTreeType").val(type);
    $("#selectedTreeTypeName").val(typeName);
    $("#importTreeDMFile").hide();
    setTimeout(function () {
        $("html").on("dragover", function (e) {
            e.preventDefault();
            e.stopPropagation();
        });
        $("html").on("drop", function (e) {
            e.preventDefault();
            e.stopPropagation();
        });
        $('.treeDMFileDivClass').on('drop', function () {
            $("#wait").css("display", "block");
            var filetype = $('#selectedTreeTypeName').val();
            dmTreeFileUpload("Y", filetype);
        });
        $("#visionTreeDmFileUpload").click(function () {
            var filetype = $('#selectedTreeTypeName').val();
            console.log("iam in clickable ");
            dmTreeFileUpload("N", filetype);
        });
        $("#importTreeDMFile").on('change', function () {
            var filetype = $('#selectedTreeTypeName').val();
            console.log("iam in files change ");
            dmTreeFileUpload("N", filetype);
        });
    }, 300);
}

function dmTreeFileUpload(draganddropInd, fileType)
{
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    var params = {};
    params['selectedFiletype'] = fileType;
    var browseId = "#importTreeDMFile";
    $(browseId).ajaxfileupload({
        'action': 'importTreeDMFile',
        params: params,
        'onComplete': function (response) {
            $("#Loader").css("display", "none");
            $("body").css({"pointer-events": "auto"});
            var message = response.message;
            if (message != null && message.indexOf("{") > -1 && message.indexOf("}") > -1) {
                message = message.substring(message.indexOf("{"), message.indexOf("}") + 1);
            }
            message = JSON.parse(message);
            var fileName = message.fileName;
            var result = message.result;
            if (result != null && result.indexOf("successfully") > -1) {
                $("#visionShowFileUploadMsg").html('<p style="color:green">' + result + '</p>');
            } else {
                $("#visionShowFileUploadMsg").html('<p style="color:red">' + result + '</p>');
            }
            $("#importTreeDMFile").remove();
            var fileImprtDiv = "<input type='file' name='importTreeDMFile' id='importTreeDMFile' style='display:none'>";
            $("#visionTreeDmFileUpload").parent().append(fileImprtDiv);
            console.log("On Complete::");
            // files expand ravi start
            var treeItemElement = globalFileTreeItem[fileType];
            if (treeItemElement != null) {
                var $element = $(treeItemElement);
                var children = $element.find('ul:first').children();
                if (children != null && children.length > 0) {
                    $.each(children, function (index) {
                        $('#savedConnections').jqxTree('removeItem', this)
                    })
                    $('#savedConnections').jqxTree('addTo', {label: 'dummy', value: "ajax"}, treeItemElement);
                    $('#savedConnections').jqxTree('collapseItem', treeItemElement);
                    $('#savedConnections').jqxTree('expandItem', treeItemElement);
                }
            }
//            var treeItemElement = globalFileTreeItem[fileType];
//            var $element = $(treeItemElement);
//            var children = $element.find('ul:first').children();
//            $.each(children, function (index) {
//                $('#savedConnections').jqxTree('removeItem', this)
//            })
//            $('#savedConnections').jqxTree('addTo', {label: 'dummy', value: "ajax"}, treeItemElement);
//            $('#savedConnections').jqxTree('collapseItem', treeItemElement);
//            $('#savedConnections').jqxTree('expandItem', treeItemElement);
            // files expand ravi end 
        },
        'onStart': function () {
            $('#Loader').show();
            $("body").css({"pointer-events": "none"});
            $("#Loader").css("display", "block");
        }
    });
    if (draganddropInd == 'N') {
        $("#importTreeDMFile").click();
    }

}

function refreshMappingArea()
{

    $("#dialog").html("Are you sure you want to clear the Mapping Area??");
    $("#dialog").dialog({
        title: (labelObject['Confirmation'] != null ? labelObject['Confirmation'] : 'Confirmation'),
        modal: true,
        height: 'auto',
        minWidth: 300,
        maxWidth: 'auto',
        fluid: true,
        buttons: [{
                text: (labelObject['Yes'] != null ? labelObject['Yes'] : 'Yes'),
                click: function () {

                    $(this).html("");
                    $(this).dialog("close");
                    $(this).dialog("destroy");
                    $("#currentJobId").val("");
                    $("#currentJobName").val("");
                    $("#flowchartworkSourcesspace").remove();
                    $("#feedHeader").append('<div  id="flowchartworkSourcesspace" class="flowchart-example-container"></div>');
                    $('#flowchartworkSourcesspace').flowchart({
                        linkWidth: 2,
                        defaultSelectedLinkColor: '#000055',
                        grid: 10,
                        distanceFromArrow: 0,
                        //                defaultOperatorClass:'VisionWorkFlowTest',
                        //                multipleLinksOnInput: true,
                        //                multipleLinksOnOutput: true,
                        defaultSelectedLinkColor: 'red',
                        onOperatorContextMenu: function (operatorId) {
                            console.log("operatorId:::" + operatorId);
                            openOpeartorContextMenu(operatorId);
                            return true;
                        },
                    });
                    $('#flowchartworkSourcesspace').flowchart({
//                                onOperatorSelect: function(operatorId) {
//                                return true;
//                                },
                        onAfterChange: function (changeType) {

                            if (!operatorDoublClick) {
                                trfmRulesChanged = true;
                                console.error("trfmRulesChanged : " + trfmRulesChanged);
                                return true;
                            }
                        }, onOperatorSelect: function (changeType) {
                            $(document).find("input").blur();
                            return true;

                        },
                        onLinkSelect: function (changeType) {
                            $(document).find("input").blur();
                            return true;
                        }
                    });
//                    $("#dataMigrationTabs").html("");
                    $("#dataMigrationTabs").remove();
                    $("#normalizeOptionsTabs").remove(); // ravi normalise
                    $("#deNormalizeOptionsTabs").remove();
                    previousOperatorId = null;
                    prevTargetOperatorId = null;

                    $("#currentTrnsOpId").val(""); // ravi updated code changes

                }},
            {
                text: (labelObject['No'] != null ? labelObject['No'] : 'No'),
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
function deleteSelectedOporLink() {
    $("#flowchartworkSourcesspace .selected").each(function () {
        var selectedOpeartorId = $(this).data("operator_id");
        console.log(":::::;" + selectedOpeartorId);
        $('#flowchartworkSourcesspace').flowchart('deleteOperator', selectedOpeartorId);
    });
    try {
        $('#flowchartworkSourcesspace').flowchart('deleteSelected');
    } catch (e) {
    }
    trfmRulesChanged = true;
//    

}

function saveJob(jobName, jobId, data, processJobData, processFlag) { // ravi save new job
    showLoader();
// ravi start
    var data = {};
    try {
        data = $('#flowchartworkSourcesspace').flowchart('getData');
    } catch (e) {
    }

    console.log("Data::::" + JSON.stringify(data))
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        cache: false,
        url: "saveMappings",
        data: {
            mappedData: JSON.stringify(data),
            jobName: jobName,
            jobId: jobId, // ravi save new job
            processJobData: JSON.stringify(processJobData)
                    //trfmRulesArray: JSON.stringify(trfmRulesArray)
        },
        success: function (response) {
//            if (processFlag == 'Y') {
//                return false;
//            }
            // ravi start
            var response = JSON.parse(response);
            showSavedJobs();
            var message = response['message'];
            var jobId = response['jobId'];
            var jobName = response['jobName'];
            $("#currentJobId").val(jobId);
            $("#currentJobName").val(jobName);
            var flowChartData = JSON.parse(response['flowChartData']);

            $("#flowchartworkSourcesspace").remove();
            $("#feedHeader").append('<div  id="flowchartworkSourcesspace" class="flowchart-example-container"></div>');

            $('#flowchartworkSourcesspace').flowchart({
                linkWidth: 2,
                defaultSelectedLinkColor: '#000055',
                grid: 10,
                distanceFromArrow: 0,
                multipleLinksOnInput: true,
                multipleLinksOnOutput: true,
                defaultSelectedLinkColor: 'red',
                onOperatorContextMenu: function (operatorId) {
                    console.log("operatorId:::" + operatorId);
                    openOpeartorContextMenu(operatorId);
                    return true;
                },
            });
            $('#flowchartworkSourcesspace').flowchart({
//                                onOperatorSelect: function(operatorId) {
//                                return true;
//                                },
                onAfterChange: function (changeType) {

                    if (!operatorDoublClick) {
                        trfmRulesChanged = true;
                        console.error("trfmRulesChanged : " + trfmRulesChanged);
                        return true;
                    }
                }, onOperatorSelect: function (changeType) {
                    $(document).find("input").blur();
                    return true;

                },
                onLinkSelect: function (changeType) {
                    $(document).find("input").blur();
                    return true;
                }
            });

            var count = 0;
            var OperatorsData = {};
            var linksData = {};
            try {
                for (var key in flowChartData.operators) {

                    OperatorsData[count] = flowChartData.operators[key];
                    for (var i in flowChartData.links) {
                        if (parseInt(key) == flowChartData.links[i].fromOperator) {
                            flowChartData.links[i].fromOperator = count;
                        }
                        if (parseInt(key) == flowChartData.links[i].toOperator) {
                            flowChartData.links[i].toOperator = count;
                        }

                    }

                    count = count + 1;
                }
                for (var key in OperatorsData) {
                    $('#flowchartworkSourcesspace').flowchart('addOperator', OperatorsData[key]);
                }
            } catch (e) {
            }



            var count = 0;
            //   try {

            for (var key in flowChartData.links) {

                linksData[count] = flowChartData.links[key];
                count = count + 1;

            }
            for (var key in linksData) {
                $('#flowchartworkSourcesspace').flowchart('addLink', linksData[key]);
            }

            $(".flowchart-operator-connector-label").hide();
            $(".flowchart-operator-title").hide();


            // ravi end

            stopLoader();
            ajaxStop();

            trfmRulesChanged = false;
            if (processFlag == "Y") {
                processJob();
            } else {
                showMesg(message);
            }
            // ravi end
        },
        error: function (e) {
            sessionTimeout(e);
        }
    });
}
function showEtlList(treeId, listId) {
    $('#mainSplitter').jqxSplitter({width: '99.1%',
        panels: [{size: "20%", min: 10}, {min: "60%", size: "80%"}]});
    $("#savedSources").hide();
    $("#savedConnectionsIconsDiv").hide();
    $("#availableJobsId").hide();
    $("#schemaObjectsDiv").hide();
    $("#" + listId).parent().find('li.visionHighlightEtlIcons').removeClass('visionHighlightEtlIcons');
    $("#" + listId).addClass('visionHighlightEtlIcons');
    var mainSplitterHeight = $('#mainSplitter').height();
    console.log("mainSplitter:::" + mainSplitterHeight);
    var styleAttr = $(".savedConnectionsClass").attr('style');
    if (styleAttr != null && styleAttr != '') {
        styleAttr += ';height:' + (parseInt(mainSplitterHeight) - 20) + 'px !important;';
    } else {
        styleAttr = 'height:' + (parseInt(mainSplitterHeight) - 20) + 'px !important;';
    }
    $(".savedConnectionsClass").attr('style', styleAttr);
    $("#" + treeId).show();
    $(".visionObjectNameDiv").removeClass("visionSelectedObject");
    $('#mainSplitter').resize();
    // ravi tree issue
    try {
        $('#savedConnections').jqxTree('refresh'); //---------code edit
    } catch (e) {
    }
}
function showTranformationRules($this) {
    showLoader();
    console.log("::showTranformationRules::");
    var t_rules = {};
    var selectedMapOperatorId = $('#flowchartworkSourcesspace').flowchart('getSelectedOperatorId');
    $("#currentTrnsOpId").val(selectedMapOperatorId);
    var normalizeOptionsObj = {};
    var deNormalizeOptionsObj = {};

    if (previousOperatorId != null) {
        var previousMapOperatorData = $('#flowchartworkSourcesspace').flowchart('getOperatorData', previousOperatorId);
        if (previousMapOperatorData['trfmRules-data'] != null) {
            t_rules = previousMapOperatorData['trfmRules-data'];
        }
    }

    var destinationColumnEmpty = false;
    var colMappingsData = []
    var rowCount = $('#sourceDestColsTableId tbody tr').length;
    var columnCount = $('#sourceDestColsTableId th').length;
    for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        var rowData = {};
        var tr = $('#sourceDestColsTableId tbody tr')[rowIndex];
        var destinationColumnStyle = $(tr.children[1]).attr("style");
        ;
        var destinationColumnVal = $(tr.children[1]).find('input').val();

        if (destinationColumnStyle == "display:none;" || destinationColumnStyle == "display:none") {
            destinationColumnVal = "N/A:N/A";
        }
        if (destinationColumnVal == null || destinationColumnVal == "") {
            destinationColumnEmpty = true;
        }
        var dataFunobjStr = $(tr.children[5]).find('input').attr("data-funobjstr");
        //funobjstr
        var funObj = {};
        if (dataFunobjStr != null && dataFunobjStr != '' && dataFunobjStr != 'null') {
            funObj['funobjstr'] = dataFunobjStr;
        }
        rowData['primaryKey'] = $(tr.children[1]).find('input').prop('checked') ? "Y" : "N";
        rowData['destinationColumn'] = destinationColumnVal;
        rowData['sourceColumn'] = $(tr.children[3]).find('input').val();
        rowData['defaultValue'] = $(tr.children[4]).find('input').val();
        rowData['appendValue'] = $(tr.children[5]).find('input').val();
        rowData['columnClause'] = (dataFunobjStr != null && dataFunobjStr != '' && dataFunobjStr != 'null') ? funObj : $(tr.children[6]).find('input').val();
        rowData['data-funobjstr'] = $(tr.children[6]).find('input').attr("data-funobjstr"); // ravi change new
        rowData['data-columnClause'] = $(tr.children[6]).find('input').val(); // ravi change new
        colMappingsData[rowIndex] = rowData;


        if (rowData['primaryKey'] == 'Y') {
            t_rules['primaryKey'] = "Y";
        }
    }

    t_rules['colMappingsData'] = colMappingsData;
    // ---------------------------------
    var masterTable = "";
    var childTables = [];
    var fromTables = [];

    var rowCount = $('#EtlMappingTable thead tr').length;
    for (var rowIndex = 1; rowIndex < rowCount; rowIndex++) {
        var rowData = {};
        var tr = $('#EtlMappingTable thead tr')[rowIndex];
        fromTables.push($(tr.children[0]).find('select').val());
        if (rowIndex == 1) {
            masterTable = $(tr.children[0]).find('select').val();
        } else {
            childTables.push($(tr.children[0]).find('select').val());
        }
    }

    if (masterTable == "") {

        var mapingObj = $('#flowchartworkSourcesspace').flowchart('getMapOperatorData', previousOperatorId);
        if (mapingObj != null && !jQuery.isEmptyObject(mapingObj)) {
            var fromOpArray = mapingObj['fromOpArray'];
        }
        if (fromOpArray != null && fromOpArray.length != 0) {
            masterTable = fromOpArray[0]['tableName'];
        }
//            try{
//               masterTable = $($(".visionOpIcons")[0]).parent().next().text().trim();
//            }catch(e){}
    }

    t_rules['masterTableName'] = masterTable;
    t_rules['masterTableName'] = masterTable;
    t_rules['childTables'] = childTables;
    t_rules['fromTables'] = fromTables;
    //------------------------------------
    var joinClauseData = []
    var mapIcons = $("#EtlMappingTable").find(".visionEtlJoinClauseMapIcon");
    $.each(mapIcons, function (index) {
        var mappedColumnData = $(this).attr('data-mappedcolumns');
        if (mappedColumnData != null
                && mappedColumnData != ''
                && mappedColumnData != 'null'
                && mappedColumnData != '{}') {
//            if (mappedColumnData == "") {
//                mappedColumnData = "{}";
//            }
            joinClauseData[index] = mappedColumnData;
        }
    });
    if (joinClauseData.length > 0) {
        t_rules['joinClauseData'] = joinClauseData;
    }

    //------------------------------------
    var whereClauseData = []
    var mapIcons = $("#selectedTables").find(".visionEtlWhereClauseMapIcon");
    $.each(mapIcons, function (index) {
        var mappedColumnData = $(this).attr('data-whereclause');
        if (mappedColumnData != null) {
            if (mappedColumnData == "") {
                mappedColumnData = "{}";
            }
            whereClauseData[index] = mappedColumnData;
        }

    });
    if (whereClauseData.length > 0) {
        t_rules['whereClauseData'] = whereClauseData;
    }
    //------------------------------------

    var orderByData = []

    var rowCount = $('#fromTablesOrderCauseTable tbody tr').length;
    for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        var rowData = {};
        var tr = $('#fromTablesOrderCauseTable tbody tr')[rowIndex];
        rowData['columnName'] = $(tr.children[1]).find('input').val();
        rowData['order'] = $(tr.children[2]).find('select').val();
        orderByData[rowIndex] = rowData;
    }
    t_rules['orderByData'] = orderByData;
    //------------------------------------

    var groupByData = []

    var rowCount = $('#fromTablesGroupCauseTable tbody tr').length;
    for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        var rowData = {};
        var tr = $('#fromTablesGroupCauseTable tbody tr')[rowIndex];
        rowData['columnName'] = $(tr.children[1]).find('input').val();
        groupByData[rowIndex] = rowData;
    }
    t_rules['groupByData'] = groupByData;
    //t_rules['trfmRulesId'] = currentMapId;
    var uniqueRowsFlag = $("#distinctRowsInput").val();
    t_rules['uniqueRowsFlag'] = (uniqueRowsFlag == true) ? "Y" : "N";
    var rowsCountFrom = $("#rowsCountFromInput").val();
    t_rules['rowsCountFrom'] = rowsCountFrom;
    var rowsCountTo = $("#rowsCountToInput").val();
    t_rules['rowsCountTo'] = rowsCountTo;
    var normalizeOptionsObj = {};
    var deNormalizeOptionsObj = {};
    var normalizeColsObj = {};
    if (previousOperatorId != null) {

        if (previousMapOperatorData.iconType == "UNGROUP") {


            var normalizeOptionsObj = {};
            var colsObj = {};
            var mapingObj = $('#flowchartworkSourcesspace').flowchart('getMapOperatorData', previousOperatorId);
            console.log("::::::::" + JSON.stringify(mapingObj));
            if (mapingObj != null && !jQuery.isEmptyObject(mapingObj)) {
                var toOpArray = mapingObj['toOpArray'];
                var fromOpArray = mapingObj['fromOpArray'];
            }

            var fromOperator = fromOpArray[0];
            var toOperator = toOpArray[0];

            var cols = $("#selectNormalizeColHeader").find("option");
            if (cols != null && cols.length > 0) {
                if (toOperator.dragType != null && toOperator.dragType == "Table") {
                    $.each(cols, function (index) {
                        if (index != 0) {
                            colsObj[this.value.split(":")[1]] = this.value;
                        }
                    })

                } else {
                    $.each(cols, function (index) {
                        if (index != 0) {
                            colsObj[this.value] = this.value;
                        }
                    })
                }

                normalizeColsObj = colsObj;
                normalizeOptionsObj['colsObj'] = colsObj;
                normalizeOptionsObj['normalizeFlag'] = "normalize";
                normalizeOptionsObj['itemSeparator'] = $("#itemSeparator").val();
                normalizeOptionsObj['normalizeColumn'] = $("#selectNormalizeColHeader").val();

                t_rules['normalizeOptionsObj'] = normalizeOptionsObj;
                previousMapOperatorData['normalizeOptionsObj'] = normalizeOptionsObj;
            }

        } else if (previousMapOperatorData.iconType == "GROUP") {
            var deNormalizeOptionsObj = {};
            var colsObj = {};
            var mapingObj = $('#flowchartworkSourcesspace').flowchart('getMapOperatorData', previousOperatorId);
            console.log("::::::::" + JSON.stringify(mapingObj));
            if (mapingObj != null && !jQuery.isEmptyObject(mapingObj)) {
                var toOpArray = mapingObj['toOpArray'];
                var fromOpArray = mapingObj['fromOpArray'];
            }

            var fromOperator = fromOpArray[0];
            var toOperator = toOpArray[0];

            var cols = $("#selectDenormalizeColHeader").find("option");
            if (cols != null && cols.length > 0) {
                if (toOperator.dragType != null && toOperator.dragType == "Table") {
                    $.each(cols, function (index) {
                        if (index != 0) {
                            colsObj[this.value.split(":")[1]] = this.value;
                        }
                    })

                } else {
                    $.each(cols, function (index) {
                        if (index != 0) {
                            colsObj[this.value] = this.value;
                        }
                    })
                }

                normalizeColsObj = colsObj;
                deNormalizeOptionsObj['colsObj'] = colsObj;
                deNormalizeOptionsObj['normalizeFlag'] = "deNormalize";
                deNormalizeOptionsObj['delimiter'] = $("#delimiter").val();
                deNormalizeOptionsObj['deNormalizeColumn'] = $("#selectDenormalizeColHeader").val();

                deNormalizeOptionsObj['keyColumn'] = $("#selectDenormalizeKeyColumn").val(); // ravi updated code

                t_rules['normalizeOptionsObj'] = deNormalizeOptionsObj;
                previousMapOperatorData['normalizeOptionsObj'] = deNormalizeOptionsObj;
            }

        }

//        var previousMapOperatorData = $('#flowchartworkSourcesspace').flowchart('getOperatorData', previousOperatorId);
        previousMapOperatorData['trfmRules-data'] = t_rules;
        $('#flowchartworkSourcesspace').flowchart('setOperatorData', previousOperatorId, previousMapOperatorData);
        var previousMapOperatorData2 = $('#flowchartworkSourcesspace').flowchart('getOperatorData', previousOperatorId);
        $(".flowchart-operator-connector-label").hide();
        $(".flowchart-operator-title").hide();
    }
//    if (previousOperatorId != null) {
//        var previousMapOperatorData = $('#flowchartworkSourcesspace').flowchart('getOperatorData', previousOperatorId);
//        previousMapOperatorData['trfmRules-data'] = t_rules
//        $('#flowchartworkSourcesspace').flowchart('setOperatorData', previousOperatorId, previousMapOperatorData);
//        var previousMapOperatorData2 = $('#flowchartworkSourcesspace').flowchart('getOperatorData', previousOperatorId);
//        $(".flowchart-operator-connector-label").hide();
//        $(".flowchart-operator-title").hide();
//    }


    if (destinationColumnEmpty && previousOperatorId != selectedMapOperatorId) {
        showMesg("Destination Column Cannot be empty");
        return false;
    }


    previousOperatorId = selectedMapOperatorId;

//    var selectedMapOperatorId = $('#flowchartworkSourcesspace').flowchart('getSelectedOperatorId');
    var selectedMapOperatorData = $('#flowchartworkSourcesspace').flowchart('getOperatorData', selectedMapOperatorId);
    if (selectedMapOperatorData['iconType'] == "UNGROUP") {
        normalizeData();
        return false;
    } else if (selectedMapOperatorData['iconType'] == "GROUP") {
        deNormalizeData();
        return false;
    }
    var mapingObj = $('#flowchartworkSourcesspace').flowchart('getMapOperatorData', selectedMapOperatorId);
    console.log("::::::::" + JSON.stringify(mapingObj));
    if (mapingObj != null && !jQuery.isEmptyObject(mapingObj)) {
        var toOpArray = mapingObj['toOpArray'];
        var fromOpArray = mapingObj['fromOpArray'];
    }

    if (toOpArray != null && toOpArray.length != 0) {

        var processFlag = true;
        var toTableName = "";
        var noSQLCount = 0;
        for (var i = 0; i < toOpArray.length != 0; i++) {
            var toOperator = toOpArray[i];
            if (toOperator['dragType'] == 'Table'
                    ) {
                var toConnectionObj = toOperator['connObj'];
                toTableName += toOperator['tableName'];
                if (toOperator['tableName'] != null && toOperator['tableName'] != '') {
                    noSQLCount++;
                    if (i != toOpArray.length - 1) {
                        toTableName += ",";
                    }
                }

            } else if (toOperator['dragType'] == 'SQL'
                    || toOperator['iconType'] == 'SQL') {
                toTableName += toOperator['tableName'];
                if (toOperator['tableName'] != null && toOperator['tableName'] != '') {
                    noSQLCount++;
                    if (i != toOpArray.length - 1) {
                        toTableName += ",";
                    }
                } else {
                    noSQLCount = 0;
                    break;
                }

            } else {
                noSQLCount++;
            }
//            if (processFlag && toOperator['iconType'] == 'SQL') {
//                var toTableName = toOperator['tableName'];
//                
//            }
        }

        if (noSQLCount == 0) {
            processFlag = false;
        }
        if (fromOpArray != null && fromOpArray.length != 0) {
            var fromConnectionObj = {};
            var fromTableName = "";
            for (var i = 0; i < fromOpArray.length; i++) {
                var fromOperator = fromOpArray[i];
                if (fromOperator != null && !jQuery.isEmptyObject(fromOperator)) {
                    fromConnectionObj = fromOperator['connObj'];
                    fromTableName += fromOperator['tableName'];
                    if (i != fromOpArray.length - 1) {
                        fromTableName += ",";
                    }
                }

            }
            if (processFlag) {
                displayTransformationTabsOpt(fromConnectionObj,
                        toConnectionObj,
                        fromTableName,
                        toTableName,
                        toOperator['iconType'],
                        toOperator['createTableObj'],
                        toOpArray,
                        fromOpArray
                        );
            } else {
                stopLoader();
                showMesg("Please select target table/Create target table by double click on SQL icon.");
            }

        }
    } else {
        stopLoader();
        showMesg("Source Table/File not mapped");
        console.log("Source Table/File not mapped")
    }



}
function filterMappingTables()
{
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    var item = $("#savedConnections").jqxTree('getSelectedItem');
    //['Tables', 'Views', 'Synonyms'];
    if (item != null && (item['value'] == 'TABLES'
            || item['value'] == 'VIEWS'
            || item['value'] == 'SYNONYMS')) {
        var hiddenFieldId = "DATABASE_" + selectedDataBase + "_" + item['value'] + "_hidden";
        hiddenFieldId = hiddenFieldId.replace(/ /g, '_');
        hiddenFieldId = hiddenFieldId.replace(/\//g, '_');
        var prevFilterVal = $("#" + hiddenFieldId).val();
        var tableData = "<input type='text' id='tableId' class='visionETLFilterInputs' readonly=true value='" + item['label'] + "'/>"
                + "<select id='operatorId' class='visionETLFilterInputs'>"
                + "<option value='LIKE'>LIKE</option>"
                + "<option value='NOT LIKE'>NOT LIKE</option>"
                + "<option value='='>=</option>"
                + "<option value='!='>!=</option>"
                + "</select>"
                + "<input type='text' id='valueId' class='visionETLFilterInputs' value= ''/>";
        $("#dialog").html(tableData);
        $("#dialog").dialog({
            title: (labelObject[item['label'] + ' Filter Form'] != null ? labelObject[item['label'] + ' Filter Form'] : item['label'] + ' Filter Form'),
            modal: true,
            width: 600,
            height: 150,
            fluid: true,
            buttons: [{
                    text: (labelObject['Search'] != null ? labelObject['Search'] : 'Search'),
                    id: 'tableSearchButton', // --------code edit---
                    click: function () {
                        var value = $("#valueId").val();
                        var selectBoxValue = $("#operatorId option:selected").text();
                        $("#" + hiddenFieldId).remove();
                        var hiddenField = "<input type='hidden' id='" + hiddenFieldId + "' value=''/>";
                        $("#savedConnections").append(hiddenField);
                        var jsFilterObj = {};
                        jsFilterObj.filterType = selectBoxValue;
                        jsFilterObj.filterValue = value;
                        $("#" + hiddenFieldId).val(JSON.stringify(jsFilterObj));
                        showLoader();
                        getFilteredValues(value, item['value'], selectConnObj, item['level'], selectColumnsObj, selectBoxValue);

                        $(this).html("");
                        $(this).dialog("close");
                        $(this).dialog("destroy");
                    }

                }, {
                    text: (labelObject['Reset'] != null ? labelObject['Reset'] : 'Reset'),
                    click: function () {
                        $("#valueId").val("");
                        $("#operatorId").val("LIKE");
                        $("#" + hiddenFieldId).remove();
                        var hiddenField = "<input type='hidden' id='" + hiddenFieldId + "' value=''/>";
                        $("#savedConnections").append(hiddenField);
                        $("#treeETLFilterImage").attr('src', "images/Filter Icon-01.svg");
                    }

                }],
            open: function () {
                //-----------  enter func start---------------
                $("#valueId").keyup(function (event) {
                    if (event.keyCode === 13) {
                        $("#tableSearchButton").click();
                    }
                });
                $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                $(this).closest(".ui-dialog").find(".ui-button").eq(2).addClass("dialogno");
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


            }
        });
        if (prevFilterVal != null && prevFilterVal != '' && prevFilterVal != undefined)
        {
            prevFilterVal = JSON.parse(prevFilterVal);
            $("#operatorId").val(prevFilterVal['filterType']);
            $("#valueId").val(prevFilterVal['filterValue']);
        }
    } else {
        $("#dialog").html(labelObject['Please Select Views/Tables/Synonyms for filter'] != null ? labelObject['Please Select Views/Tables/Synonyms for filter'] : 'Please Select Views/Tables/Synonyms for filter');
        $("#dialog").dialog({
            title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
            modal: true,
            width: 300,
            height: 120,
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
            }, close: function (event, ui)
            {

            }
        });
    }

}


function getFilteredValues(value, selectedLevelValue, selectConnObj, selectedLevel, selectColumnsObj, selectBoxValue)
{
    selectedLevelValue = selectedLevelValue.toUpperCase();
    var item = $("#savedConnections").jqxTree('getSelectedItem');
    var url = 'getTreePagingDataOpt';
    if (item != null)
    {
        var level = item['level'];
        var parentEventItem = item['parentElement'];
        for (var i = level; i > 0; i--)
        {
            parentEventItem = parentEventItem['parentElement'];
        }
        var selectedItem = $('#savedConnections').jqxTree('getItem', parentEventItem);
        var selectedParentValue = selectedItem['value'];
        if (selectedParentValue != null && selectedParentValue != ''
                && selectedParentValue != undefined && selectedParentValue == 'ERP')
        {
            url = 'getTreeErpConnectionDetails';
        }
    }
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: url,
        cache: false,
        data: {
            parentkey: ((selectedLevelValue != null && selectedLevelValue != '') ? selectedLevelValue.toUpperCase() : ""),
            level: selectedLevel,
            columnsObj: JSON.stringify(selectColumnsObj),
            connectionObj: JSON.stringify(selectConnObj),
            filterValue: value,
            filterCondition: selectBoxValue,
            startIndex: 0,
            endIndex: $("#treePageSize").val()
        },
        success: function (data, status, xhr) {
            stopLoader();
            var selectedItem = $("#savedConnections").jqxTree('getSelectedItem');
            var count;
            var children;
            if (selectedItem != null) {
                children = $(selectedItem.element).find('li');
                count = children.length;
                for (var i = 0; i < count; i += 1) {
                    if (i < count - 1) {
                        $("#savedConnections").jqxTree('removeItem', children[i], false);
                    } else {
                        $("#savedConnections").jqxTree('removeItem', children[i], true);
                    }
                }
            }


            $("#savedConnections").jqxTree('addTo', data, selectedItem);
            $('#savedConnections').jqxTree('expandItem', selectedItem);
            var items = $("#savedConnections").jqxTree('getItems');
            $.each(items, function () {
                $(this.titleElement).attr('title', this.label);

            });
            var itemsList = $("#" + selectedItem.id);
            try {
                var itemsLists = itemsList[0].children[3];
                var childrens = itemsLists['children'];
                var childData = childrens[childrens['length'] - 1];
                var childrenData = $("#savedConnections").jqxTree('getItem', childData);
                if (childrenData['value'] == "Show More")
                {
                    var expandDiv = childrenData.element.children[0];
                    var div = childrenData.element.children[1];
                    var checkBoxDiv = childrenData.element.children[2];
                    $("#" + expandDiv.id).remove();
                    $("#" + div.id).remove();
                    $("#" + checkBoxDiv.id).addClass('visionETLCheckboxEmptySpace');
                }
            } catch (e)
            {

            }
            $("#treeETLFilterImage").attr('src', "images/Filter Icon2-01.svg");
//            $("#savedConnections").jqxTree('selectItem', null);
            var hiddenPagingId = "DATABASE_" + selectedDataBase + "_" + selectedLevelValue + "paging__hidden";
            hiddenPagingId = hiddenPagingId.replace(/ /g, '_');
            hiddenPagingId = hiddenPagingId.replace(/\//g, '_');
            $("#" + hiddenPagingId).remove();
            var hiddenPagingField = "<input type='hidden' id='" + hiddenPagingId + "'value=''/>";
            $("#savedConnections").append(hiddenPagingField);
            var dataPagingObj = {};
            dataPagingObj['startIndex'] = '0';
            dataPagingObj['endIndex'] = $("#treePageSize").val();
            $("#" + hiddenPagingId).val(JSON.stringify(dataPagingObj));
        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
}
function displayTransformationTabsOpt(fromConnectionObj,
        toConnectionObj,
        fromTableName,
        toTableName,
        toIconType,
        createTableObj,
        toOpArray,
        fromOpArray) {
    // ravi start
    var selectedMapOperatorId = $('#flowchartworkSourcesspace').flowchart('getSelectedOperatorId');
    var selectedMapOperatorData = $('#flowchartworkSourcesspace').flowchart('getOperatorData', selectedMapOperatorId);
    var trfmRulesId = selectedMapOperatorData['trfmRulesId'];
    var trfmRulesData = selectedMapOperatorData['trfmRules-data'];
    // ravi end
    if ((fromConnectionObj != null && !jQuery.isEmptyObject(fromConnectionObj))) {
        showLoader();
        if (!(toConnectionObj != null && !jQuery.isEmptyObject(toConnectionObj))) {
            toConnectionObj = {};
        }
        $.ajax({
            type: 'post',
            traditional: true,
            dataType: 'html',
            cache: false,
            url: 'fetchTransformationRules',
            async: true,
            data: {
                fromTable: fromTableName,
                toTable: toTableName,
                fromConnObj: JSON.stringify(fromConnectionObj),
                toConnObj: JSON.stringify(toConnectionObj),
                sourceTables: JSON.stringify(fromTableName.split(",")),
                toIconType: toIconType,
                createTableObj: JSON.stringify(createTableObj),
                trfmRulesId: trfmRulesId,
                trfmRulesData: JSON.stringify(trfmRulesData),
                trfmRulesChanged: (trfmRulesChanged == true) ? "Y" : "N",
                toOpArray: JSON.stringify(toOpArray),
                fromOpArray: JSON.stringify(fromOpArray),
            },
            success: function (response) {
                stopLoader();
                if (response != null) {
                    $('.visionTablesComboBox').hide();
                    $('.visionUploadFileDiv').hide();
                    $('.visionConnectToDbDiv').hide();
                    $('#visionERPMain').hide();
                    var response = JSON.parse(response);
                    if (response != null && response.connectionFlag == 'Y') {
                        $("#feedContentArea").html(response['tabsString']);
                        $("#dataMigrationTabs").jqxTabs({width: "100%", height: "130px", position: 'top', theme: 'ui-redmond', reorder: true});

                        $('#dataMigrationTabs').unbind('selected').on('selected', function (event) {
                            $('#iconsdiv').attr('style', 'margin-top:4px !important');
                        });
//                        $("#dataMigrationTabs").jqxTabs({width: "100%", height: "100%", position: 'top', theme: 'ui-redmond', reorder: true});
                        var columnStr = response['columnMapping'];
                        var selectedJoinTables = response['selectedJoinTables'];
                        $('#tabs-1').html(columnStr);
                        $('#tabs-1').append("<div id='selectedColumnStr' style='display:none'></div>");
                        $("#toTableColsArray_hidden").remove();
                        $("#fromTableColsArray_hidden").remove();
                        var hiddenData = "<input type='hidden' id='toTableColsArray_hidden'/><input type='hidden' id='fromTableColsArray_hidden'/>";
                        $('#tabs-1').append(hiddenData);
                        $("#toTableColsArray_hidden").val(JSON.stringify(response['toTableColsArray']));
                        $("#fromTableColsArray_hidden").val(JSON.stringify(response['fromTableColsArray']));
                        $('#tabs-2').html(selectedJoinTables);
                        $('#mapColumns').addClass("active");
                        $('.visionProgressFilesSteps').hide();
                        $("#selectedColumnStr").html(response['colMapTrString']);//colMapTrString
                        var selectedTableWhereClause = response['selectedTableWhereClause'];
                        if (selectedTableWhereClause != null) {
                            var hiddenDataWhere = "<input type='hidden' id='whereClauseTableColsArray_hidden'/><input type='hidden' id='currentClauseMapId'/>"
                                    + "<div id='wherClauseTrString' style='display:none;'></div><div id='wherClauseColsString' style='display:none;'></div>";
                            $('#tabs-3').html(selectedTableWhereClause['whereClauseCondition'] + hiddenDataWhere);//whereClauseCondition
                            $("#whereClauseTableColsArray_hidden").val(JSON.stringify(selectedTableWhereClause['fromTableColsArray']));
                        }
                        var selectedTableOrderGroupClause = response['selectedTableOrderGroupClause'];
                        if (selectedTableOrderGroupClause != null) {
                            var hiddenDataOrderBy = ""
                                    + "<div id='orderClauseTrString' style='display:none;'></div>";
                            $('#tabs-4').html(selectedTableOrderGroupClause['orderByCondition'] + hiddenDataOrderBy);//
                            var hiddenDataGroupBy = ""
                                    + "<div id='groupClauseTrString' style='display:none;'></div>";
                            $('#tabs-5').html(selectedTableOrderGroupClause['groupByCondition'] + hiddenDataGroupBy);//
                            $("#groupClauseTrString").html(selectedTableOrderGroupClause['groupByTrString']);
                            $("#orderClauseTrString").html(selectedTableOrderGroupClause['orderByTrString']);
                        }
                        // sql Editor
//                        var sqlEditor = ace.edit("tabs-6");
//                        sqlEditor.setOptions({
//                            enableBasicAutocompletion: true, // the editor completes the statement when you hit Ctrl + Space
//                            showPrintMargin: false, // hides the vertical limiting strip
//                            fontSize: "100%" // ensures that the editor fits in the environment
//                        });
//
//                        sqlEditor.getSession().setMode("ace/mode/sql");
                        $("#dataMigrationTabs").change(function (event) {
                            trfmRulesChanged = true;
                        });
                        if (response['uniqueRowsFlag'] == "Y") {
                            $("#distinctRowsInput").prop("checked", true);
                        }

                        $("#rowsCountFromInput").val(response['rowsFrom']);
                        $("#rowsCountToInput").val(response['rowsTo']);
//                        //importColMapFile
//                        var fileslist = [];
//                        $("#importColMapFile").on('change', function (event) {
//                            console.log("iam in files change ");
//                           fileslist  = event.target.files;
//                            srsFileNames(fileslist);
//
//                        });
                        $(".visionETLColMapImage").mousedown(function (event) {
                            treeIconClickEvent = event;
                        });

                        try {
                            $("#sourceDestColsTableId").colResizable({
                                disable: true
                            });
                            $("#sourceDestColsTableId").tableDnD({
                                onDragStyle: null,
                                onDropStyle: null,
                                onDragClass: "tDnD_whileDrag",
                            });
                        } catch (e) {
                        }
                        $("#sourceDestColsTableId").colResizable();

                        $('#dataMigrationTabs').on('selected', function (event) {
                            var selectedTab = event.args.item;
                            if (selectedTab == '0' || selectedTab == 0) {// mapping
                                try {
                                    $("#sourceDestColsTableId").colResizable({
                                        disable: true
                                    });
                                    $("#sourceDestColsTableId").tableDnD({
                                        onDragStyle: null,
                                        onDropStyle: null,
                                        onDragClass: "tDnD_whileDrag",
                                    });
                                } catch (e) {
                                }
                                $("#sourceDestColsTableId").colResizable();
                            } else if (selectedTab == '1' || selectedTab == 1) {// join
                                try {
                                    $("#EtlMappingTable").colResizable({
                                        disable: true
                                    });
                                } catch (e) {
                                }
                                $("#EtlMappingTable").colResizable();
                            } else if (selectedTab == '2' || selectedTab == 2) {//where clause
                                try {
                                    $("#selectedTables").colResizable({
                                        disable: true
                                    });
                                } catch (e) {
                                }
                                $("#selectedTables").colResizable();
                            } else if (selectedTab == '3' || selectedTab == 3) {//order clause
                                try {
                                    $("#fromTablesOrderCauseTable").colResizable({
                                        disable: true
                                    });
                                } catch (e) {
                                }
                                $("#fromTablesOrderCauseTable").colResizable();
                            } else if (selectedTab == '4' || selectedTab == 4) {//group clause
                                try {
                                    $("#fromTablesGroupCauseTable").colResizable({
                                        disable: true
                                    });
                                } catch (e) {
                                }
                                $("#fromTablesGroupCauseTable").colResizable();
                            }
                        });

                    } else {
                        stopLoader();
                        showMessagePopup(response.connectionMessage);
                    }


                }
            },
            error: function (e)
            {
                stopLoader();
                sessionTimeout(e);
            }

        });
    } else {
        stopLoader();
    }


}
function displayTransformationTabs(fromConnectionObj, toConnectionObj, fromTableName, toTableName, toIconType, createTableObj) {
    // ravi start
    var selectedMapOperatorId = $('#flowchartworkSourcesspace').flowchart('getSelectedOperatorId');
    var selectedMapOperatorData = $('#flowchartworkSourcesspace').flowchart('getOperatorData', selectedMapOperatorId);
    var trfmRulesId = selectedMapOperatorData['trfmRulesId'];
    var trfmRulesData = selectedMapOperatorData['trfmRules-data'];
    // ravi end
    if ((fromConnectionObj != null && !jQuery.isEmptyObject(fromConnectionObj))) {
        showLoader();
        if (!(toConnectionObj != null && !jQuery.isEmptyObject(toConnectionObj))) {
            toConnectionObj = {};
        }
        $.ajax({
            type: 'post',
            traditional: true,
            dataType: 'html',
            cache: false,
            url: 'fetchTransformationRules',
            async: true,
            data: {
                fromTable: fromTableName,
                toTable: toTableName,
                fromConnObj: JSON.stringify(fromConnectionObj),
                toConnObj: JSON.stringify(toConnectionObj),
                sourceTables: JSON.stringify(fromTableName.split(",")),
                toIconType: toIconType,
                createTableObj: JSON.stringify(createTableObj),
                trfmRulesId: trfmRulesId,
                trfmRulesData: JSON.stringify(trfmRulesData),
                trfmRulesChanged: (trfmRulesChanged == true) ? "Y" : "N"
            },
            success: function (response) {
                stopLoader();
                if (response != null) {
                    $('.visionTablesComboBox').hide();
                    $('.visionUploadFileDiv').hide();
                    $('.visionConnectToDbDiv').hide();
                    $('#visionERPMain').hide();
                    var response = JSON.parse(response);
                    if (response != null && response.connectionFlag == 'Y') {
                        $("#feedContentArea").html(response['tabsString']);
                        $("#dataMigrationTabs").jqxTabs({width: "100%", height: "130px", position: 'top', theme: 'ui-redmond', reorder: true});

                        $('#dataMigrationTabs').unbind('selected').on('selected', function (event) {
                            $('#iconsdiv').attr('style', 'margin-top:4px !important');
                        });
//                        $("#dataMigrationTabs").jqxTabs({width: "100%", height: "100%", position: 'top', theme: 'ui-redmond', reorder: true});
                        var columnStr = response['columnMapping'];
                        var selectedJoinTables = response['selectedJoinTables'];
                        $('#tabs-1').html(columnStr);
                        $('#tabs-1').append("<div id='selectedColumnStr' style='display:none'></div>");
                        $("#toTableColsArray_hidden").remove();
                        $("#fromTableColsArray_hidden").remove();
                        var hiddenData = "<input type='hidden' id='toTableColsArray_hidden'/><input type='hidden' id='fromTableColsArray_hidden'/>";
                        $('#tabs-1').append(hiddenData);
                        $("#toTableColsArray_hidden").val(JSON.stringify(response['toTableColsArray']));
                        $("#fromTableColsArray_hidden").val(JSON.stringify(response['fromTableColsArray']));
                        $('#tabs-2').html(selectedJoinTables);
                        $('#mapColumns').addClass("active");
                        $('.visionProgressFilesSteps').hide();
                        $("#selectedColumnStr").html(response['colMapTrString']);//colMapTrString
                        var selectedTableWhereClause = response['selectedTableWhereClause'];
                        if (selectedTableWhereClause != null) {
                            var hiddenDataWhere = "<input type='hidden' id='whereClauseTableColsArray_hidden'/><input type='hidden' id='currentClauseMapId'/>"
                                    + "<div id='wherClauseTrString' style='display:none;'></div><div id='wherClauseColsString' style='display:none;'></div>";
                            $('#tabs-3').html(selectedTableWhereClause['whereClauseCondition'] + hiddenDataWhere);//whereClauseCondition
                            $("#whereClauseTableColsArray_hidden").val(JSON.stringify(selectedTableWhereClause['fromTableColsArray']));
                        }
                        var selectedTableOrderGroupClause = response['selectedTableOrderGroupClause'];
                        if (selectedTableOrderGroupClause != null) {
                            var hiddenDataOrderBy = ""
                                    + "<div id='orderClauseTrString' style='display:none;'></div>";
                            $('#tabs-4').html(selectedTableOrderGroupClause['orderByCondition'] + hiddenDataOrderBy);//
                            var hiddenDataGroupBy = ""
                                    + "<div id='groupClauseTrString' style='display:none;'></div>";
                            $('#tabs-5').html(selectedTableOrderGroupClause['groupByCondition'] + hiddenDataGroupBy);//
                            $("#groupClauseTrString").html(selectedTableOrderGroupClause['groupByTrString']);
                            $("#orderClauseTrString").html(selectedTableOrderGroupClause['orderByTrString']);
                        }
                        // sql Editor
//                        var sqlEditor = ace.edit("tabs-6");
//                        sqlEditor.setOptions({
//                            enableBasicAutocompletion: true, // the editor completes the statement when you hit Ctrl + Space
//                            showPrintMargin: false, // hides the vertical limiting strip
//                            fontSize: "100%" // ensures that the editor fits in the environment
//                        });
//
//                        sqlEditor.getSession().setMode("ace/mode/sql");
                        $("#dataMigrationTabs").change(function (event) {
                            trfmRulesChanged = true;
                        });
                        if (response['uniqueRowsFlag'] == "Y") {
                            $("#distinctRowsInput").prop("checked", true);
                        }

                        $("#rowsCountFromInput").val(response['rowsFrom']);
                        $("#rowsCountToInput").val(response['rowsTo']);
//                        //importColMapFile
//                        var fileslist = [];
//                        $("#importColMapFile").on('change', function (event) {
//                            console.log("iam in files change ");
//                           fileslist  = event.target.files;
//                            srsFileNames(fileslist);
//
//                        });
                        $(".visionETLColMapImage").mousedown(function (event) {
                            treeIconClickEvent = event;
                        })
                    } else {
                        stopLoader();
                        showMessagePopup(response.connectionMessage);
                    }


                }
            },
            error: function (e)
            {
                stopLoader();
                sessionTimeout(e);
            }

        });
    } else {
        stopLoader();
    }


}

function treePaging(selectedLevelValue, selectConnObj, selectedItem, selectedLevel, selectColumnsObj, selectedDataBase, selectedParentValue)
{
    showLoader();
    var hiddenPagingId = "DATABASE_" + selectedDataBase + "_" + selectedLevelValue + "paging__hidden";
    hiddenPagingId = hiddenPagingId.replace(/ /g, '_');
    hiddenPagingId = hiddenPagingId.replace(/\//g, '_');
    var pagingStr = $("#" + hiddenPagingId).val();
    if (pagingStr != null && pagingStr != '' && pagingStr != undefined)
    {
        var pagingObj = JSON.parse(pagingStr);
        var startIndex = pagingObj['startIndex'];
        var endIndex = pagingObj['endIndex'];
        var pagesize = $("#treePageSize").val();
        if (startIndex != null && startIndex != '' && startIndex != undefined &&
                endIndex != null && endIndex != '' && endIndex != undefined)
        {
            startIndex = parseInt(startIndex);
            endIndex = parseInt(endIndex);
            startIndex = endIndex + 1;
            pagesize = parseInt(pagesize);
            endIndex = pagesize + endIndex;
        }
    }
    var hiddenFieldId = "DATABASE_" + selectedDataBase + "_" + selectedLevelValue + "_hidden";
    hiddenFieldId = hiddenFieldId.replace(/ /g, '_');
    hiddenFieldId = hiddenFieldId.replace(/\//g, '_');
    var prevFilterVal = $("#" + hiddenFieldId).val();
    var filterType;
    var filterValue;
    if (prevFilterVal != null && prevFilterVal != '' && prevFilterVal != undefined)
    {
        prevFilterVal = JSON.parse(prevFilterVal);
        filterType = prevFilterVal['filterType'];
        filterValue = prevFilterVal['filterValue'];
    }
    selectedLevelValue = selectedLevelValue.toUpperCase();
    var url = 'getTreePagingDataOpt';
    if (selectedParentValue != null && selectedParentValue != '' && selectedParentValue != undefined
            && selectedParentValue == 'ERP')
    {
        url = 'getTreeErpConnectionDetails';
    }
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: url,
        cache: false,
        data: {
            parentkey: selectedLevelValue,
            level: selectedLevel,
            columnsObj: JSON.stringify(selectColumnsObj),
            connectionObj: JSON.stringify(selectConnObj),
            startIndex: startIndex,
            endIndex: endIndex,
            filterValue: filterValue,
            filterCondition: filterType
        },
        success: function (data, status, xhr) {
            stopLoader();
            if (data != null) {


                var children = $(selectedItem).find('li');
                var count = children.length;
                for (var i = count - 2; i < count; i += 1) {
                    $("#savedConnections").jqxTree('removeItem', children[i], false);
                }

                $('#savedConnections').jqxTree('addTo', data, selectedItem);
                var items = $('#savedConnections').jqxTree('getItems');
                $.each(items, function () {
                    $(this.titleElement).attr('title', this.label);
                });
                try {
                    var itemsLists = selectedItem.children[3];
                    var childrens = itemsLists['children'];
                    var childData = childrens[childrens['length'] - 1];
                    var childrenData = $('#savedConnections').jqxTree('getItem', childData);
                    if (childrenData['value'] == "Show More")
                    {
                        var expandDiv = childrenData.element.children[0];
                        var div = childrenData.element.children[1];
                        var checkBoxDiv = childrenData.element.children[2];
                        $("#" + expandDiv.id).remove();
                        $("#" + div.id).remove();
                        $("#" + checkBoxDiv.id).addClass('visionETLCheckboxEmptySpace');
                    }
                } catch (e)
                {

                }
//                $('#savedConnections').jqxTree('selectItem', null);
                $("#" + hiddenPagingId).remove();
                var hiddenPagingField = "<input type='hidden' id='" + hiddenPagingId + "'value=''/>";
                $("#savedConnections").append(hiddenPagingField);
                var dataPagingObj = {};
                dataPagingObj['startIndex'] = startIndex;
                dataPagingObj['endIndex'] = endIndex;
                $("#" + hiddenPagingId).val(JSON.stringify(dataPagingObj));
            }
        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
}
function showJoinsPopup(event, tableName, tableMapId, iconIndex) {

    var dbObj = $("#SOURCE_SELECT_JOIN_TABLES_" + iconIndex).attr("data-table-db");
    showLoader();
    var prevTables = [];
    $(event.target).closest('tr').prevAll('tr').each(function (index) {
        if (this.rowIndex != 0) {
            var tableName = this.cells[0].children[0].value;
            prevTables.push(tableName)
        }

    });
    tableName = $("#SOURCE_SELECT_JOIN_TABLES_" + iconIndex).val();
    var joinColumnMapping = $("#" + tableMapId).attr("data-mappedcolumns");
    // ravi start
    var trfmRulesId = null;
    var trfmRulesData = null;
    if (joinColumnMapping == null || joinColumnMapping == "" || joinColumnMapping == "{}") {

        // var selectedMapOperatorId = $('#flowchartworkSourcesspace').flowchart('getSelectedOperatorId');
        var selectedMapOperatorData = $('#flowchartworkSourcesspace').flowchart('getOperatorData', previousOperatorId);
        trfmRulesData = selectedMapOperatorData['trfmRules-data'];
        trfmRulesId = selectedMapOperatorData['trfmRulesId'];
        if (trfmRulesData != null && trfmRulesData['joinClauseData'] != null) {
            var joinColumnMappings = trfmRulesData['joinClauseData']
            var joinMapId = event.target.id;
            var joinMapIndexStr = joinMapId.substring(joinMapId.indexOf("_") + 1, joinMapId.length);
            var joinMapIndex = parseInt(joinMapIndexStr) - 1;
            joinColumnMapping = trfmRulesData['joinClauseData'][joinMapIndex];

        }


    }
    var currentJoinTableId = $("#currentJoinTableId").val();
    if (currentJoinTableId != null && currentJoinTableId != '') {
        var columnJoinMapping = {};
        var joinType = $("#joinType").val();
        var i = 1;
        $("#etlJoinClauseTable tbody tr").each(function () {
            var tdArray = this.cells;
            if (tdArray != null && tdArray.length != 0) {
                var joinObj = {};
                var childTableColumn = $(tdArray[1]).find("input").val();
                var operator = $(tdArray[2]).find("option:selected").val();
                var masterTableColumn = $(tdArray[3]).find("input").val();
                var staticValue = $(tdArray[4]).find("input").val();
                var andOrOperator = $(tdArray[5]).find("option:selected").val();
                if (childTableColumn != null && childTableColumn != '') {
                    joinObj['childTableColumn'] = (childTableColumn != null && childTableColumn == 'Select') ? "" : childTableColumn;
                    joinObj['operator'] = operator;
                    joinObj['masterTableColumn'] = (masterTableColumn != null && masterTableColumn == 'Select') ? "" : masterTableColumn;
                    joinObj['andOrOperator'] = andOrOperator;
                    joinObj['staticValue'] = staticValue;
                    joinObj['joinType'] = joinType;
                    //$("#operatorId option:selected").text();
                    console.log("childTableColumn::" + childTableColumn);
                    console.log("operator::" + operator);
                    console.log("masterTableColumn::" + masterTableColumn);
                    console.log("andOrOperator::" + andOrOperator);
                    columnJoinMapping[i] = joinObj;
                    i++;
                }
            }

        });
        if (columnJoinMapping != null) {
            $("#" + currentJoinTableId).attr("data-mappedcolumns", JSON.stringify(columnJoinMapping));

        }
    }
    var joinDBStr = $("#EtlMappingTable").attr("data-join-db");
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'html',
        cache: false,
        url: 'fetchJoinTablesData',
        async: true,
        data: {
            tableName: tableName,
            dbObj: dbObj,
            sourceTables: JSON.stringify(prevTables),
            iconIndex: iconIndex,
            joinColumnMapping: joinColumnMapping,
            trfmRulesId: trfmRulesId, // ravi start
            trfmRulesData: JSON.stringify(trfmRulesData),
            trfmRulesChanged: (trfmRulesChanged == true) ? "Y" : "N",
            joinDBStr: joinDBStr
                    //joinConditionsMapId: $("#" + currentMapId).attr("trfmRules-data")
        },
        success: function (response) {
            stopLoader();
            if (response != null) {
                var response = JSON.parse(response);
                var joinsDataStr = response['joinsDataStr'];
                var trString = response['trString'];
                $("#joinMapColumnsDivId").html(joinsDataStr);
                var hiddenData = "<input type='hidden' id='childTableColsArray_hidden'/>\n\
<input type='hidden' id='masterTableColsArray_hidden'/><input type='hidden' id='currentJoinTableId'/>";
                $('#joinMapColumnsDivId').append(hiddenData);
                $("#childTableColsArray_hidden").val(JSON.stringify(response['childTableColsArray']));
                $("#masterTableColsArray_hidden").val(JSON.stringify(response['masterTableColsArray']));
                $("#currentJoinTableId").val(tableMapId);
                $("#joinTableColumnTr").html(trString);
                var contentSplitterWidth = $("#contentSplitter").width();
                if (contentSplitterWidth > 1121) {
                    $(".visionColJoinMappingInput").attr('style', 'width:84% !important');

                } else if (contentSplitterWidth > 1100 && contentSplitterWidth < 1120) {
                    $(".visionColJoinMappingInput").attr('style', 'width:81% !important');


                } else if (contentSplitterWidth > 1000 && contentSplitterWidth < 1100) {
                    $(".visionColJoinMappingInput").attr('style', 'width:81% !important');

                } else if (contentSplitterWidth > 900 && contentSplitterWidth < 1000) {
                    $(".visionColJoinMappingInput").attr('style', 'width:80% !important');

                } else if (contentSplitterWidth > 800 && contentSplitterWidth < 900) {
                    $(".visionColJoinMappingInput").attr('style', 'width:78% !important');

                } else if (contentSplitterWidth > 700 && contentSplitterWidth < 800) {
                    $(".visionColJoinMappingInput").attr('style', 'width:75% !important');

                }
                var currentJoinTableId = $("#currentJoinTableId").val();
                if (currentJoinTableId != null && currentJoinTableId != '') {
                    var columnJoinMapping = {};
                    var joinType = $("#joinType").val();
                    var i = 1;

                    $("#etlJoinClauseTable tbody tr").each(function () {
                        var tdArray = this.cells;
                        if (tdArray != null && tdArray.length != 0) {
                            var joinObj = {};
                            var childTableColumn = $(tdArray[1]).find("input").val();
                            var operator = $(tdArray[2]).find("option:selected").val();
                            var masterTableColumn = $(tdArray[3]).find("input").val();
                            var staticValue = $(tdArray[4]).find("input").val();
                            var andOrOperator = $(tdArray[5]).find("option:selected").val();
                            if (childTableColumn != null && childTableColumn != '') {
                                joinObj['childTableColumn'] = (childTableColumn != null && childTableColumn == 'Select') ? "" : childTableColumn;
                                joinObj['operator'] = operator;
                                joinObj['masterTableColumn'] = (masterTableColumn != null && masterTableColumn == 'Select') ? "" : masterTableColumn;
                                joinObj['andOrOperator'] = andOrOperator;
                                joinObj['staticValue'] = staticValue;
                                joinObj['joinType'] = joinType;
                                //$("#operatorId option:selected").text();
                                console.log("childTableColumn::" + childTableColumn);
                                console.log("operator::" + operator);
                                console.log("masterTableColumn::" + masterTableColumn);
                                console.log("andOrOperator::" + andOrOperator);
                                columnJoinMapping[i] = joinObj;
                                i++;
                            }


                        }

                    });
                    if (columnJoinMapping != null) {
                        $("#" + currentJoinTableId).attr("data-mappedcolumns", JSON.stringify(columnJoinMapping));

                    }
                }
                try {
                    var feedContentAreaHeight = $("#feedContentArea").height();
                    $(".visionEtlJoinClauseTablesDivScroll").css("max-height", parseInt(feedContentAreaHeight) - 105);

                } catch (e) {
                }
                try {
                    $("#etlJoinClauseTable").colResizable({
                        disable: true
                    });
                } catch (e) {
                }
                $("#etlJoinClauseTable").colResizable();
                $(".visionETLColMapImage").mousedown(function (event) {
                    treeIconClickEvent = event;
                });
            }

        },
        error: function (e)
        {
            sessionTimeout(e);
            stopLoader();
        }

    });

}
function addNewJoinsRow(event, dbObject, id) {
    var trstring = $("#joinTableColumnTr").html();
    $("#etlJoinClauseTable tbody").append(trstring);
    $("#tabs-2").animate({
        scrollTop: $("#tabs-2").prop("scrollHeight")
    }, 1000);

}

function deleteSelectedRow($this) {
//    $(this).parents("tr").remove();
    $($this).parents("tr").remove();
    // ravi file headers start
    try {
        var trArray = $("#fileColumnHeadersTable tbody tr");
        $.each(trArray, function (index) {
            $(this.children[1]).html(this.rowIndex);
        })
    } catch (e) {
    }
    // ravi file headers end
    console.log("$this");
    trfmRulesChanged = true;  // ravi edit new
    try {
        $("#mainSplitter").resize();
    } catch (e) {
    }
}

function processJob() {

    showLoader();
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    var selectedToOperatorId = $("#currentTrnsOpId").val();
    try {
        if (selectedToOperatorId != null && selectedToOperatorId != '') {
            selectedToOperatorId = parseInt(selectedToOperatorId);
        }
    } catch (e) {
    }
    var t_rules = {};

    var previousTargetOperatorData = $('#flowchartworkSourcesspace').flowchart('getOperatorData', selectedToOperatorId);

    var isEmpty = jQuery.isEmptyObject(previousTargetOperatorData);

    if (!isEmpty) {
        if (previousTargetOperatorData['trfmRules-data'] != null) {
            t_rules = previousTargetOperatorData['trfmRules-data'];
        }

        var componentOperator = {};
        var fromOpLinkArray = $('#flowchartworkSourcesspace').flowchart('getAllFromOperatorsByToOpId', selectedToOperatorId);
        if (fromOpLinkArray != null) {
            if (fromOpLinkArray != null && fromOpLinkArray.length != 0) {
                for (var i = 0; i < fromOpLinkArray.length; i++) {
                    var operatorData = fromOpLinkArray[i];
                    if (operatorData != null
                            && operatorData['iconType'] != null
                            && operatorData['iconType'] != '') {
                        componentOperator = operatorData;
                        break;
                    }

                }
            }

        }


        if (componentOperator['iconType'] == "MAP") {
            var destinationColumnEmpty = false;

            var colMappingsData = []

            var rowCount = $('#sourceDestColsTableId tbody tr').length;
            var columnCount = $('#sourceDestColsTableId th').length;
            for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                var rowData = {};
                var tr = $('#sourceDestColsTableId tbody tr')[rowIndex];
                var destinationColumnStyle = $(tr.children[1]).attr("style");

                var destinationColumnVal = $(tr.children[1]).find('input').val();

                if (destinationColumnStyle == "display:none;" || destinationColumnStyle == "display:none") {
                    destinationColumnVal = "N/A:N/A";
                }
                if (destinationColumnVal == null || destinationColumnVal == "") {
                    destinationColumnEmpty = true;
                }


                if (destinationColumnEmpty) {
                    showMesg("Destination Column Cannot be empty");
                    throw new Error();
                }

                //        if (destinationColumnVal != null &&
                //                destinationColumnVal != "" &&
                //                destinationColumnVal != "undefined"
                //                && destinationColumnVal.indexOf(":")==-1){
                //            destinationColumnVal = "N/A:"+destinationColumnVal; // ravi process job issues 
                //        }

                var dataFunobjStr = $(tr.children[5]).find('input').attr("data-funobjstr");
                //funobjstr
                var funObj = {};
                if (dataFunobjStr != null && dataFunobjStr != '' && dataFunobjStr != 'null') {
                    funObj['funobjstr'] = dataFunobjStr;
                }
                rowData['primaryKey'] = $(tr.children[1]).find('input').prop('checked') ? "Y" : "N";
                rowData['destinationColumn'] = destinationColumnVal;
                rowData['sourceColumn'] = $(tr.children[3]).find('input').val();
                rowData['defaultValue'] = $(tr.children[4]).find('input').val();
                rowData['appendValue'] = $(tr.children[5]).find('input').val();
                rowData['columnClause'] = (dataFunobjStr != null && dataFunobjStr != '' && dataFunobjStr != 'null') ? funObj : $(tr.children[6]).find('input').val();
                rowData['data-funobjstr'] = $(tr.children[6]).find('input').attr("data-funobjstr"); // ravi change new
                rowData['data-columnClause'] = $(tr.children[6]).find('input').val(); // ravi change new
                colMappingsData[rowIndex] = rowData;


                if (rowData['primaryKey'] == 'Y') {
                    t_rules['primaryKey'] = "Y";
                }
            }

            t_rules['colMappingsData'] = colMappingsData;

            // ---------------------------------

            var masterTable = "";
            var childTables = [];
            var fromTables = [];

            var rowCount = $('#EtlMappingTable thead tr').length;
            for (var rowIndex = 1; rowIndex < rowCount; rowIndex++) {
                var rowData = {};
                var tr = $('#EtlMappingTable thead tr')[rowIndex];
                fromTables.push($(tr.children[0]).find('select').val());
                if (rowIndex == 1) {
                    masterTable = $(tr.children[0]).find('select').val();
                } else {
                    childTables.push($(tr.children[0]).find('select').val());
                }
            }

            if (masterTable == "") {

                var mapingObj = $('#flowchartworkSourcesspace').flowchart('getMapOperatorData', selectedToOperatorId);
                if (mapingObj != null && !jQuery.isEmptyObject(mapingObj)) {
                    var fromOpArray = mapingObj['fromOpArray'];
                }
                if (fromOpArray != null && fromOpArray.length != 0) {
                    masterTable = fromOpArray[0]['tableName'];
                }
                //            try{
                //               masterTable = $($(".visionOpIcons")[0]).parent().next().text().trim();
                //            }catch(e){}
            }

            t_rules['masterTableName'] = masterTable;
            t_rules['masterTableName'] = masterTable;
            t_rules['childTables'] = childTables;
            t_rules['fromTables'] = fromTables;
            //------------------------------------

            var joinClauseData = []
            var mapIcons = $("#EtlMappingTable").find(".visionEtlJoinClauseMapIcon");

            $.each(mapIcons, function (index) {
                var mappedColumnData = $(this).attr('data-mappedcolumns');
                if (mappedColumnData != null
                        && mappedColumnData != ''
                        && mappedColumnData != 'null'
                        && mappedColumnData != '{}') {
//                    if (mappedColumnData == "") {
//                        mappedColumnData = "{}";
//                    }
                    joinClauseData[index] = mappedColumnData;
                }
            });
            if (joinClauseData.length > 0) {
                t_rules['joinClauseData'] = joinClauseData;
                var fromTablesList = $(".sourceJoinColsTd").find("select");
                if (fromTablesList.lenght > 0) {
                    var reOrderFromTables = [];
                    $.each(fromTablesList, function (i) {
                        reOrderFromTables.push(this.value);
                    })
                    t_rules['fromTables'] = reOrderFromTables;
                }
            }

            //------------------------------------
            var whereClauseData = []
            var mapIcons = $("#selectedTables").find(".visionEtlWhereClauseMapIcon");
            $.each(mapIcons, function (index) {
                var mappedColumnData = $(this).attr('data-whereclause');
                if (mappedColumnData != null) {
                    if (mappedColumnData == "") {
                        mappedColumnData = "{}";
                    }
                    whereClauseData[index] = mappedColumnData;
                }

            });
            if (whereClauseData.length > 0) {
                t_rules['whereClauseData'] = whereClauseData;
            }
            //---------------------------------------------------- advanced settings  
            var selectTabObj = {};
            var uniqueRowsFlag = "N";
            if ($("#distinctRowsInput").is(":checked")) {
                uniqueRowsFlag = "Y";
            }
            selectTabObj['uniqueRowsFlag'] = uniqueRowsFlag;

            var minRows = $("#rowsCountFromInput").val();
            var maxRows = $("#rowsCountToInput").val();
            if (!(minRows != null && minRows != '') && (maxRows != null && maxRows != '')) {
                minRows = 0;
            }
            selectTabObj['minRows'] = minRows;
            selectTabObj['maxRows'] = maxRows;

            var operatorType = $("#operatorType").val();
            selectTabObj['operatorType'] = operatorType;

            t_rules['selectTabObj'] = selectTabObj;

            //------------------------------------

            var orderByData = []

            var rowCount = $('#fromTablesOrderCauseTable tbody tr').length;
            for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                var rowData = {};
                var tr = $('#fromTablesOrderCauseTable tbody tr')[rowIndex];
                rowData['columnName'] = $(tr.children[1]).find('input').val();
                rowData['order'] = $(tr.children[2]).find('select').val();
                orderByData[rowIndex] = rowData;
            }
            t_rules['orderByData'] = orderByData;
            //------------------------------------

            var groupByData = []

            var rowCount = $('#fromTablesGroupCauseTable tbody tr').length;
            for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                var rowData = {};
                var tr = $('#fromTablesGroupCauseTable tbody tr')[rowIndex];
                rowData['columnName'] = $(tr.children[1]).find('input').val();
                groupByData[rowIndex] = rowData;
            }
            t_rules['groupByData'] = groupByData;

        } else if (componentOperator['iconType'] == "UNGROUP") {

            var normalizeOptionsObj = {};
            normalizeOptionsObj['normalizeFlag'] = "normalize";
            normalizeOptionsObj['itemSeparator'] = $("#itemSeparator").val();
            normalizeOptionsObj['normalizeColumn'] = $("#selectNormalizeColHeader").val();
            t_rules['normalizeOptionsObj'] = normalizeOptionsObj;
            var colMappingsData = [];
            var fromColsArray = $("#selectNormalizeColHeader").find("option");
            if (fromColsArray != null && fromColsArray.length > 0) {
                $.each(fromColsArray, function (rowIndex) {
                    if (rowIndex != 0) {
                        var rowData = {};
                        var destFileHeaders = t_rules['fileHeaders'];
                        if (destFileHeaders != null) {
                            var destinationColumn = destFileHeaders[rowIndex];
                        }
                        rowData['sourceColumn'] = this.value;
                        rowData['destinationColumn'] = (destinationColumn != null) ? destinationColumn : this.value;
                        colMappingsData[rowIndex - 1] = rowData;
                    }

                })

                t_rules['colMappingsData'] = colMappingsData;
            }
        } else if (componentOperator['iconType'] == "GROUP") {

            var deNormalizeOptionsObj = {};
            deNormalizeOptionsObj['normalizeFlag'] = "deNormalize";
            deNormalizeOptionsObj['delimiter'] = $("#delimiter").val();
            deNormalizeOptionsObj['deNormalizeColumn'] = $("#selectDenormalizeColHeader").val();

            deNormalizeOptionsObj['keyColumn'] = $("#selectDenormalizeKeyColumn").val(); // ravi updated code

            t_rules['normalizeOptionsObj'] = deNormalizeOptionsObj;
            var colMappingsData = [];
            var fromColsArray = $("#selectDenormalizeColHeader").find("option");
            if (fromColsArray != null && fromColsArray.length > 0) {
                $.each(fromColsArray, function (rowIndex) {
                    if (rowIndex != 0) {
                        var rowData = {};
                        var destFileHeaders = t_rules['fileHeaders'];
                        if (destFileHeaders != null) {
                            var destinationColumn = destFileHeaders[rowIndex];
                        }
                        rowData['sourceColumn'] = this.value;
                        rowData['destinationColumn'] = (destinationColumn != null) ? destinationColumn : this.value;
                        colMappingsData[rowIndex - 1] = rowData;
                    }

                })

                t_rules['colMappingsData'] = colMappingsData;
            }
        }

        previousTargetOperatorData['trfmRules-data'] = t_rules;
        previousTargetOperatorData['targetOperator'] = "Y";
        $('#flowchartworkSourcesspace').flowchart('setOperatorData', selectedToOperatorId, previousTargetOperatorData);
        var previousTargetOperatorData2 = $('#flowchartworkSourcesspace').flowchart('getOperatorData', selectedToOperatorId);
        $(".flowchart-operator-connector-label").hide();
        $(".flowchart-operator-title").hide();

    }
    var data = $('#flowchartworkSourcesspace').flowchart('getData');
    var operators = data['operators'];
    $.each(operators, function (i) {
        if (this['targetOperator'] == 'Y') {
            this['timeStamp'] = new Date().getTime();
            $('#flowchartworkSourcesspace').flowchart('setOperatorData', parseInt(this['operatorId']), this);
        }
    });

    var data = {};
    try {
        data = $('#flowchartworkSourcesspace').flowchart('getData');
    } catch (e) {
    }
    console.log("Mapped Data::::" + JSON.stringify(data));
    if (data != null && !jQuery.isEmptyObject(data)) {
        //sourceDestColsTableId
        var operators = data['operators'];
        var links = data['links'];
        if (operators != null && !jQuery.isEmptyObject(operators)
                && links != null && !jQuery.isEmptyObject(links)) {

            var jobId = $("#currentJobId").val();
            var processJobData = {
                mappedData: JSON.stringify(data),
                jobId: jobId
            }

            $.ajax({
                type: 'post',
                traditional: true,
                dataType: 'json',
                cache: false,
                url: 'processETLData',
                async: true,
                data: processJobData,
                success: function (response) {
                    stopLoader();

                    try {
                        $("#currentJobId").val(response['jobId']);
                        $("#currentJobName").val(response['jobName']);
                    } catch (e) {
                    }

                    openLogFile();

                },
                error: function (e)
                {
                    sessionTimeout(e);
                    stopLoader();
                    if (processLogInterval != null) {
                        clearInterval(processLogInterval);
                    }
                }

            });
        } else {
            stopLoader();
            $("#dialog").html(labelObject['No Mapped Objects to Process'] != null ? labelObject['No Mapped Objects to Process'] : 'No Mapped Objects to Process');
            $("#dialog").dialog({
                title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
                modal: true,
                height: 'auto',
                minWidth: 300,
                maxWidth: 'auto',
                fluid: true,
                buttons: [{
                        text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                        click: function () {
                            $(this).html("");
                            $(this).dialog("close");
                            $(this).dialog("destroy");

                        }}],
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

    } else {
        stopLoader();
        $("#dialog").html(labelObject['No Mapped Objects to Process'] != null ? labelObject['No Mapped Objects to Process'] : 'No Mapped Objects to Process');
        $("#dialog").dialog({
            title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
            modal: true,
            height: 'auto',
            minWidth: 300,
            maxWidth: 'auto',
            fluid: true,
            buttons: [{
                    text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                    click: function () {
                        $(this).html("");
                        $(this).dialog("close");
                        $(this).dialog("destroy");

                    }}],
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

}

function selectColumn($this, tableColType, dataArray) {
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    //dddw
    // prepare the data
    var data = [];
    if (tableColType == 'fromColumn') {
        var dataStr = $("#fromTableColsArray_hidden").val();
        if (dataStr != null && dataStr != '') {
            data = JSON.parse(dataStr);
        }
    } else if (tableColType == 'toColumn') {
        var dataStr = $("#toTableColsArray_hidden").val();
        if (dataStr != null && dataStr != '') {
            data = JSON.parse(dataStr);
        }
    } else if (tableColType == 'childColumn') {
        var dataStr = $("#childTableColsArray_hidden").val();
        if (dataStr != null && dataStr != '') {
            data = JSON.parse(dataStr);
        }
    } else if (tableColType == 'masterColumn') {
        var dataStr = $("#masterTableColsArray_hidden").val();
        if (dataStr != null && dataStr != '') {
            data = JSON.parse(dataStr);
        }
    } else if (tableColType == 'fromWhereClauseColumn') {
        var dataStr = $("#whereClauseTableColsObj_hidden").val();
        if (dataStr != null && dataStr != '') {
            data = JSON.parse(dataStr);
        }
    } else if (tableColType == 'LOOKUP_TABLE,COLUMN') {
        data = dataArray;
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
<div id='columnMappingTree' class='columnMappingTree'></div>");
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
                        // $(_this).addClass("visionHighlightColumn");
                        // $(_this).parent().css("background-color","yellow");
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
//            try {
//                $(this).dialog("destroy");
//            } catch (e) {
//            }
//                       
//                        
//                       
        }
//        , create: function (event, ui) {
//            $("#columnMappingTree li").on('dblclick', function (event) {
//                var selectedItem = $("#columnMappingTree").jqxTree('getSelectedItem');
//                if (selectedItem != null && !(selectedItem['icon'] != null && selectedItem['icon'] != '')) {
//                    $($this).parents("td").find("input").val(selectedItem['value']);
//                    $($this).parents("td").find("input").attr("title", selectedItem['value']);
//                }
//                $(this).dialog("close");
//                $(this).dialog("destroy");
//            });
//        }
    });

}

function addColumnMapping(event, $this) {
    var selectedColumnStr = $("#selectedColumnStr").html();
    $("#sourceDestColsTableId tbody").append(selectedColumnStr);
    $("#tabs-1").animate({
        scrollTop: $("#tabs-1").prop("scrollHeight")
    }, 1000);
}

function saveJoinMapping(event, id) {
    var currentJoinTableId = $("#currentJoinTableId").val();
    if (currentJoinTableId != null && currentJoinTableId != '') {
        var columnJoinMapping = {};
        var joinType = $("#joinType").val();
        var i = 1;

        $("#etlJoinClauseTable tbody tr").each(function () {
            var tdArray = this.cells;
            if (tdArray != null && tdArray.length != 0) {
                var joinObj = {};
                var childTableColumn = $(tdArray[1]).find("input").val();
                var operator = $(tdArray[2]).find("option:selected").val();
                var masterTableColumn = $(tdArray[3]).find("input").val();
                var staticValue = $(tdArray[4]).find("input").val();
                var andOrOperator = $(tdArray[5]).find("option:selected").val();
                if (childTableColumn != null && childTableColumn != '') {
                    joinObj['childTableColumn'] = (childTableColumn != null && childTableColumn == 'Select') ? "" : childTableColumn;
                    joinObj['operator'] = operator;
                    joinObj['masterTableColumn'] = (masterTableColumn != null && masterTableColumn == 'Select') ? "" : masterTableColumn;
                    joinObj['andOrOperator'] = andOrOperator;
                    joinObj['staticValue'] = staticValue;
                    joinObj['joinType'] = joinType;
                    //$("#operatorId option:selected").text();
                    console.log("childTableColumn::" + childTableColumn);
                    console.log("operator::" + operator);
                    console.log("masterTableColumn::" + masterTableColumn);
                    console.log("andOrOperator::" + andOrOperator);
                    columnJoinMapping[i] = joinObj;
                    i++;
                }


            }

        });
        if (columnJoinMapping != null) {
            $("#" + currentJoinTableId).attr("data-mappedcolumns", JSON.stringify(columnJoinMapping));

        }
    }
    var dialogSplitMessage = dialogSplitIconText((labelObject['Saved Successfully'] != null ? labelObject['Saved Successfully'] : 'Saved Successfully'), "Y");
    $("#dialog").html(dialogSplitMessage);
    $("#dialog").dialog({
        title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
        modal: true,
        height: 'auto',
        minWidth: 300,
        maxWidth: 'auto',
        fluid: true,
        buttons: [{
                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                click: function () {
                    $(this).html("");
                    $(this).dialog("close");
                    $(this).dialog("destroy");

                }}],
        open: function () {
            stopLoader();
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

function viewJoinQuery() {
    //EtlMappingTable
    var query = "<ul><li> <font color='#0071c5'>FROM</font> ";
    var i = 0;

    $("#EtlMappingTable thead tr").each(function () {
        if (i != 0) {
            var tdArray = this.cells;
            if (tdArray != null && tdArray.length != 0) {
                var tableName = $(tdArray[0]).find("option:selected").val();
                tableName = "<font color='#F911E9'>" + tableName + "</font>"
                if (i == 1) {
                    query += " " + tableName + "</li><li><ul>";
                } else {
                    var mappedcolumnsObjStr = $(tdArray[1]).find("img").attr("data-mappedcolumns");
                    //data-mappedcolumns  
                    console.log("mappedcolumnsObjStr:::" + mappedcolumnsObjStr);
                    if (mappedcolumnsObjStr != null && mappedcolumnsObjStr != '') {
                        var mappedcolumnsObj = JSON.parse(mappedcolumnsObjStr);
                        if (mappedcolumnsObj != null && !jQuery.isEmptyObject(mappedcolumnsObj)) {
                            var j = 0;
                            var mapObjLength = Object.keys(mappedcolumnsObj).length;
                            for (var key in mappedcolumnsObj) {
                                var mappedColObj = mappedcolumnsObj[key];

                                if (mappedColObj != null && !jQuery.isEmptyObject(mappedColObj)) {
                                    var childTableColumn = mappedColObj['childTableColumn'];
                                    if (childTableColumn != null && childTableColumn != '') {
                                        var childTableColumnArray = childTableColumn.split(":");
                                        childTableColumn = "<font color='#F911E9'>" + childTableColumnArray[0] + "</font>." + childTableColumnArray[1];
                                    }
                                    var masterTableColumn = mappedColObj['masterTableColumn'];
                                    if (masterTableColumn != null && masterTableColumn != '') {
                                        var masterTableColumnArray = masterTableColumn.split(":");
                                        masterTableColumn = "<font color='#F911E9'>" + masterTableColumnArray[0] + "</font>." + masterTableColumnArray[1];
                                    }
                                    if (j == 0) {
                                        query += "<li><font color='#0071c5'> " + mappedColObj['joinType'] + "</font> " + tableName + " <font color='#0071c5'>ON</font> </li><li><ul>"
                                    }
                                    var colValue = mappedColObj['staticValue'];
                                    if (colValue != null && colValue != '' && colValue != 'null') {
                                        if (mappedColObj['operator'] == 'IN' || mappedColObj['operator'] == 'NOT IN') {

                                            if (colValue.indexOf("##") > -1) {
                                                colValue = "('" + colValue.replace(/#{2,}/g, "','") + "')";
                                            } else {
                                                colValue = "('" + colValue + "')";

                                            }
                                        } else {
                                            colValue = "'" + colValue + "'";
                                        }
                                    }
                                    query += " <li> " + childTableColumn + " <font color='#0071c5'>" + mappedColObj['operator'] + "</font> "
                                            + ((mappedColObj['staticValue'] != null && mappedColObj['staticValue'] != '') ? (" <font color='#FF0000'>" + colValue + "</font> ") : masterTableColumn);//staticValue

                                    if (j != parseInt(mapObjLength) - 1) {
                                        query += " <font color='#0071c5'>" + mappedColObj['andOrOperator'] + "</font> ";
                                    }
                                    query += "</li>";
                                }
                                j++;
                            }
                            query += "</ul></li>";
                        }
                    }
                }


            }
        }

        i++;
    });
    query += "</ul></li></ul>";

    console.log("Query:::" + query);
    $("#viewJoinQueryId").html(query);
}

function showWhereClausePopup(event, tableName, id, $this, index) {
    showLoader();

    var prevMapIconId = $("#currentClauseMapId").val();
    if (prevMapIconId != null && prevMapIconId != '') {
        var query = "";
        //fromTablesWhereCauseTable
        var columnClauseMapping = {};
        var i = 1;
        var rowCount = $('#fromTablesWhereCauseTable >tbody >tr').length;
        $("#fromTablesWhereCauseTable tbody tr").each(function () {
            var tdArray = this.cells;
            if (tdArray != null && tdArray.length != 0) {
                var clauseObj = {};
                var columnName = $(tdArray[1]).find("input").val();
                var operator = $(tdArray[2]).find("option:selected").val();
                var staticValue = $(tdArray[3]).find("input").val();
                var andOrOperator = $(tdArray[4]).find("option:selected").val();
                var staticValueMod = staticValue; // ravi whereclause issue
                if (columnName != null && columnName != '') {
                    if (staticValueMod != null && staticValueMod != '' && staticValueMod != 'null' && staticValueMod != 'NULL') {
                        if (operator == 'IN' || operator == 'NOT IN') {
                            if (staticValueMod.indexOf("##") > -1) {
                                staticValueMod = "('" + staticValueMod.replace(/#{2,}/g, "','") + "')";
                            } else {
                                staticValueMod = "('" + staticValueMod + "')";
                            }
                        } else {
                            staticValueMod = "'" + staticValueMod + "'";
                        }
                    }
                    query += " " + columnName.replace(":", ".") + " " + operator + " " + staticValueMod + " ";
//                    query += " " + columnName.replace(":", ".") + " " + operator + " '" + staticValue + "' ";
                    if ((parseInt(i) - 1) != (parseInt(rowCount) - 1)) {
                        query += " " + andOrOperator;
                    }
                    clauseObj['columnName'] = columnName;
                    clauseObj['operator'] = operator;
                    clauseObj['andOrOperator'] = andOrOperator;
                    clauseObj['staticValue'] = staticValue;
                    columnClauseMapping[i] = clauseObj;
                    i++;
                }


            }

        });
        if (columnClauseMapping != null) {
            var currentClauseAreaId = prevMapIconId.replace("whereClauseConditionsMap_", "whereClauseConditionsArea_");
            $("#" + currentClauseAreaId).val(query);
            $("#" + prevMapIconId).attr("data-whereclause", JSON.stringify(columnClauseMapping));

        }
    }
    var whereClauseTableColsArrayStr = $("#whereClauseTableColsArray_hidden").val();
    var currentTableTreeArray = [];
    if (whereClauseTableColsArrayStr != null && whereClauseTableColsArrayStr != '') {
        var whereClauseTableColsArrayObj = JSON.parse(whereClauseTableColsArrayStr);
        if (whereClauseTableColsArrayObj != null && !jQuery.isEmptyObject(whereClauseTableColsArrayObj)) {
            currentTableTreeArray = whereClauseTableColsArrayObj[tableName];
        }
    }
    var whereClauseStr = $("#" + id).attr("data-whereclause");
    // ravi start
    var trfmRulesData;
    var trfmRulesId;
    if (whereClauseStr == null || whereClauseStr == "" || whereClauseStr == "{}") {
        var selectedMapOperatorData = $('#flowchartworkSourcesspace').flowchart('getOperatorData', previousOperatorId);
        trfmRulesData = selectedMapOperatorData['trfmRules-data'];
        trfmRulesId = selectedMapOperatorData['trfmRulesId'];


        if (trfmRulesData != null && trfmRulesData['whereClauseData'] != null) {

            var whereClasesConditions = trfmRulesData['whereClauseData']
            var whereClauseMapId = event.target.id;
            var whereClauseMapIndexStr = whereClauseMapId.substring(whereClauseMapId.indexOf("_") + 1, whereClauseMapId.length);
            var whereClauseMapIndex = parseInt(whereClauseMapIndexStr);
            whereClauseStr = trfmRulesData['whereClauseData'][whereClauseMapIndex];
        }
    }

    // ravi end
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'html',
        cache: false,
        url: 'showingClauseColumns',
        async: true,
        data: {
            whereClauseStr: whereClauseStr,
            trfmRulesId: trfmRulesId,
            trfmRulesChanged: (trfmRulesChanged == true) ? "Y" : "N"
        },
        success: function (response) {
            stopLoader();
            if (response != null) {
                var response = JSON.parse(response);
                $("#currentClauseMapId").val(id);
                $("#whereClauseMapColumnsDivId").html("");
                $("#whereClauseMapColumnsDivId").html(response['whereClauseColsCondition']);
                $("#wherClauseTrString").html(response['trString']);
                $("#whereClauseMapColumnsDivId").append("<input type='hidden' id='whereClauseTableColsObj_hidden'/>");
                $("#whereClauseTableColsObj_hidden").val(JSON.stringify(currentTableTreeArray));
                var contentSplitterWidth = $("#contentSplitter").width();
                if (contentSplitterWidth > 1121) {
                    $(".visionColJoinMappingInput").attr('style', 'width:84%');

                } else if (contentSplitterWidth > 1100 && contentSplitterWidth < 1120) {
                    $(".visionColJoinMappingInput").attr('style', 'width:81%');


                } else if (contentSplitterWidth > 1000 && contentSplitterWidth < 1100) {
                    $(".visionColJoinMappingInput").attr('style', 'width:81%');

                } else if (contentSplitterWidth > 900 && contentSplitterWidth < 1000) {
                    $(".visionColJoinMappingInput").attr('style', 'width:80%');

                } else if (contentSplitterWidth > 800 && contentSplitterWidth < 900) {
                    $(".visionColJoinMappingInput").attr('style', 'width:78%');

                } else if (contentSplitterWidth > 700 && contentSplitterWidth < 800) {
                    $(".visionColJoinMappingInput").attr('style', 'width:75%');

                }
                var prevMapIconId = $("#currentClauseMapId").val();
                if (prevMapIconId != null && prevMapIconId != '') {
                    var query = "";
                    //fromTablesWhereCauseTable
                    var columnClauseMapping = {};
                    var i = 1;
                    var rowCount = $('#fromTablesWhereCauseTable >tbody >tr').length;
                    $("#fromTablesWhereCauseTable tbody tr").each(function () {
                        var tdArray = this.cells;
                        if (tdArray != null && tdArray.length != 0) {
                            var clauseObj = {};
                            var columnName = $(tdArray[1]).find("input").val();
                            var operator = $(tdArray[2]).find("option:selected").val();
                            var staticValue = $(tdArray[3]).find("input").val();
                            var andOrOperator = $(tdArray[4]).find("option:selected").val();
                            var staticValueMod = staticValue; // ravi whereclause issue
                            if (columnName != null && columnName != '') {
                                if (staticValueMod != null && staticValueMod != '' && staticValueMod != 'null' && staticValueMod != 'NULL') {
                                    if (operator == 'IN' || operator == 'NOT IN') {
                                        if (staticValueMod.indexOf("##") > -1) {
                                            staticValueMod = "('" + staticValueMod.replace(/#{2,}/g, "','") + "')";
                                        } else {
                                            staticValueMod = "('" + staticValueMod + "')";
                                        }
                                    } else {
                                        staticValueMod = "'" + staticValueMod + "'";
                                    }
                                }
                                query += " " + columnName.replace(":", ".") + " " + operator + " " + staticValueMod + " ";
                                if ((parseInt(i) - 1) != (parseInt(rowCount) - 1)) {
                                    query += " " + andOrOperator;
                                }
                                clauseObj['columnName'] = columnName;
                                clauseObj['operator'] = operator;
                                clauseObj['andOrOperator'] = andOrOperator;
                                clauseObj['staticValue'] = staticValue;
                                columnClauseMapping[i] = clauseObj;
                                i++;
                            }


                        }

                    });
                    if (columnClauseMapping != null) {
                        var currentClauseAreaId = prevMapIconId.replace("whereClauseConditionsMap_", "whereClauseConditionsArea_");
                        $("#" + currentClauseAreaId).val(query);
                        $("#" + prevMapIconId).attr("data-whereclause", JSON.stringify(columnClauseMapping));

                    }
                }
                // scroll bar issue fixed by SHI
                var feedContentAreaHeight = $("#feedContentArea").height();
                var tbodyHeight = feedContentAreaHeight - 105;
                $(".visionEtlJoinClauseTablesDivScroll").css("max-height", parseInt(feedContentAreaHeight) - 105);
                // scroll bar issue fixed by SHI
                try {
                    $("#fromTablesWhereCauseTable").colResizable({
                        disable: true
                    });
                } catch (e) {
                }
                $("#fromTablesWhereCauseTable").colResizable();


            }

        },
        error: function (e)
        {
            sessionTimeout(e);
            stopLoader();
        }

    });


}
function addNewClauseRow(event, id, $this) {
    var trstring = $("#wherClauseTrString").html();
    $("#fromTablesWhereCauseTable tbody").append(trstring);
    $("#tabs-3").animate({
        scrollTop: $("#tabs-3").prop("scrollHeight")
    }, 1000);
}
function addNewOrderClauseRow(event, id, $this) {
    var trstring = $("#orderClauseTrString").html();
    $("#fromTablesOrderCauseTable tbody").append(trstring);
    $("#tabs-4").animate({
        scrollTop: $("#tabs-4").prop("scrollHeight")
    }, 1000);
}
function addNewGroupClauseRow(event, id, $this) {
    var trstring = $("#groupClauseTrString").html();
    $("#fromTablesGroupCauseTable tbody").append(trstring);
    $("#tabs-5").animate({
        scrollTop: $("#tabs-5").prop("scrollHeight")
    }, 1000);
}

function saveClauseMapping(event, id, $this) {
    var currentClauseMapId = $("#currentClauseMapId").val();
    if (currentClauseMapId != null && currentClauseMapId != '') {
        //fromTablesWhereCauseTable
        var columnClauseMapping = {};

        var i = 1;
        var rowCount = $('#fromTablesWhereCauseTable >tbody >tr').length;
        var query = "";
        $("#fromTablesWhereCauseTable tbody tr").each(function () {
            var tdArray = this.cells;
            if (tdArray != null && tdArray.length != 0) {
                var clauseObj = {};
                var columnName = $(tdArray[1]).find("input").val();
                var operator = $(tdArray[2]).find("option:selected").val();
                var staticValue = $(tdArray[3]).find("input").val();
                var andOrOperator = $(tdArray[4]).find("option:selected").val();
                var staticValueMod = staticValue; // ravi whereclause issue
                if (columnName != null && columnName != '') {
                    if (staticValueMod != null && staticValueMod != '' && staticValueMod != 'null' && staticValueMod != 'NULL') {
                        if (operator == 'IN' || operator == 'NOT IN') {
                            if (staticValueMod.indexOf("##") > -1) {
                                staticValueMod = "('" + staticValueMod.replace(/#{2,}/g, "','") + "')";
                            } else {
                                staticValueMod = "('" + staticValueMod + "')";
                            }
                        } else {
                            staticValueMod = "'" + staticValueMod + "'";
                        }
                    }
                    query += " " + columnName.replace(":", ".") + " " + operator + " " + staticValueMod + " ";
                    if ((parseInt(i) - 1) != (parseInt(rowCount) - 1)) {
                        query += " " + andOrOperator;
                    }
                    clauseObj['columnName'] = columnName;
                    clauseObj['operator'] = operator;
                    clauseObj['andOrOperator'] = andOrOperator;
                    clauseObj['staticValue'] = staticValue;
                    columnClauseMapping[i] = clauseObj;
                    i++;
                }


            }

        });
        if (columnClauseMapping != null) {
//            var whereClauseConditionsArea_
            var currentClauseAreaId = currentClauseMapId.replace("whereClauseConditionsMap_", "whereClauseConditionsArea_");
            $("#" + currentClauseAreaId).val(query);
//          var tdArray = this.cells;  $($this).parents("tr").find("textarea").val(query);
            $("#" + currentClauseMapId).attr("data-whereclause", JSON.stringify(columnClauseMapping));

        }
    }
    var dialogSplitMessage = dialogSplitIconText((labelObject['Saved Successfully'] != null ? labelObject['Saved Successfully'] : 'Saved Successfully'), "Y");
    $("#dialog").html(dialogSplitMessage);
    $("#dialog").dialog({
        title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
        modal: true,
        height: 'auto',
        minWidth: 300,
        maxWidth: 'auto',
        fluid: true,
        buttons: [{
                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                click: function () {
                    $(this).html("");
                    $(this).dialog("close");
                    $(this).dialog("destroy");

                }}],
        open: function () {
            stopLoader();
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

function viewTotalSQLQuery($this, sqlQueryTabId) {
    var selectQuery = "";
    // col mappings
    var query = "<font color='#0071c5'>SELECT</font> ";
    selectQuery += "SELECT ";
    var selectTabObj = {};
    if ($("#distinctRowsInput").is(":checked")) {
        query += "<font color='#0071c5'>DISTINCT</font>  ";
        selectQuery += " DISTINCT "
    }
//    selectTabObj['uniqueRowsFlag'] = uniqueRowsFlag;
//    selectTabObj['minRows'] = $("#rowsCountFromInput").val();
//    selectTabObj['maxRows'] = $("#rowsCountToInput").val();

    var i = 0;
    var rowCount = $('#sourceDestColsTableId >tbody >tr').length;
    $("#sourceDestColsTableId tbody tr").each(function () {
        var tdArray = this.cells;
        if (tdArray != null && tdArray.length != 0) {
            var toTableColumn = $(tdArray[1]).find("input").val();
            var fromTableColumn = $(tdArray[2]).find("input").val();
            var defaultValue = $(tdArray[3]).find("input").val();
            var appendValue = $(tdArray[4]).find("input").val();
            var columnClause = $(tdArray[5]).find("input").val();
            if ((fromTableColumn != null && fromTableColumn != '')
                    || (defaultValue != null && defaultValue != '')
                    || (columnClause != null && columnClause != '')
                    ) {
                if (fromTableColumn != null && fromTableColumn != '') {
                    query += " " + fromTableColumn.replace(":", ".") + "  ";
                    selectQuery += " " + fromTableColumn.replace(":", ".") + "  ";

                } else if (defaultValue != null && defaultValue != '') {
                    query += " <font color='#FF0000'>'" + defaultValue + "'</font> AS " + toTableColumn.replace(":", ".") + "  ";
                    selectQuery += " '" + defaultValue + "' AS " + toTableColumn.split(":")[1] + "  ";

                } else if (columnClause != null && columnClause != '') {
//                    if (columnClause.indexOf("'") > -1) {
//                        var quotetedStr = "'"+columnClause.substring(columnClause.indexOf("'") + 1, 
//                        columnClause.indexOf("'", columnClause.indexOf("'") + 1))+"'";
//                        columnClause = columnClause.replace(quotetedStr,"<font color='#FF0000'>"+quotetedStr+"</font>");
////                        var columnClauseArray 
//                    }
                    query += " (" + columnClause + ") AS " + toTableColumn.replace(":", ".") + "  ";
                    selectQuery += " (" + columnClause + ") AS " + toTableColumn.split(":")[1] + "  ";
                }
                if ((parseInt(i)) != (parseInt(rowCount) - 1)) {
                    query += " , ";
                    selectQuery += " , ";
                }
            }

        }

        i++;
    });
    query += " <ul><li> <font color='#0071c5'>FROM</font> ";
    selectQuery += " FROM ";
    var i = 0;
    var joinQuery = "";
    $("#EtlMappingTable thead tr").each(function () {
        if (i != 0) {
            var tdArray = this.cells;
            if (tdArray != null && tdArray.length != 0) {
                var tableName = $(tdArray[0]).find("option:selected").val();
                tableName = "<font color='#F911E9'>" + tableName + "</font>"
                if (i == 1) {
                    joinQuery += " " + tableName + "</li><li><ul>";
                    selectQuery += " " + tableName.replace("<font color='#F911E9'>", "").replace("</font>", "") + " ";
                } else {
                    var mappedcolumnsObjStr = $(tdArray[1]).find("img").attr("data-mappedcolumns");
                    //data-mappedcolumns  
                    console.log("mappedcolumnsObjStr:::" + mappedcolumnsObjStr);
                    if (mappedcolumnsObjStr != null && mappedcolumnsObjStr != '') {
                        var mappedcolumnsObj = JSON.parse(mappedcolumnsObjStr);
                        if (mappedcolumnsObj != null && !jQuery.isEmptyObject(mappedcolumnsObj)) {
                            var j = 0;
                            var mapObjLength = Object.keys(mappedcolumnsObj).length;
                            for (var key in mappedcolumnsObj) {
                                var mappedColObj = mappedcolumnsObj[key];
                                if (mappedColObj != null && !jQuery.isEmptyObject(mappedColObj)) {
                                    var childTableColumn = mappedColObj['childTableColumn'];
                                    if (childTableColumn != null && childTableColumn != '') {
                                        var childTableColumnArray = childTableColumn.split(":");
                                        childTableColumn = "<font color='#F911E9'>" + childTableColumnArray[0] + "</font>." + childTableColumnArray[1];
                                    }
                                    var masterTableColumn = mappedColObj['masterTableColumn'];
                                    if (masterTableColumn != null && masterTableColumn != '') {
                                        var masterTableColumnArray = masterTableColumn.split(":");
                                        masterTableColumn = "<font color='#F911E9'>" + masterTableColumnArray[0] + "</font>." + masterTableColumnArray[1];
                                    }
                                    if (j == 0) {
                                        joinQuery += "<li><font color='#0071c5'> " + mappedColObj['joinType'] + "</font> " + tableName + " <font color='#0071c5'>ON</font> </li><li><ul>"
                                        selectQuery += " " + mappedColObj['joinType'] + "  " + tableName.replace("<font color='#F911E9'>", "").replace("</font>", "") + "  ON ";
                                    }
                                    var colValue = mappedColObj['staticValue'];
                                    if (colValue != null && colValue != '' && colValue != 'null') {
                                        if (mappedColObj['operator'] == 'IN' || mappedColObj['operator'] == 'NOT IN') {

                                            if (colValue.indexOf("##") > -1) {
                                                colValue = "('" + colValue.replace(/#{2,}/g, "','") + "')";
                                            } else {
                                                colValue = "('" + colValue + "')";

                                            }
                                        } else {
                                            colValue = "'" + colValue + "'";
                                        }
                                    }
                                    joinQuery += " <li> " + childTableColumn + " <font color='#0071c5'>" + mappedColObj['operator'] + "</font> "
                                            + ((mappedColObj['staticValue'] != null && mappedColObj['staticValue'] != '') ? (" <font color='#FF0000'>'" + mappedColObj['staticValue'] + "'</font> ") : masterTableColumn); //staticValue
                                    selectQuery += "  " + childTableColumn.replace("<font color='#F911E9'>", "").replace("</font>", "") + "  " + mappedColObj['operator'] + "  "
                                            + ((mappedColObj['staticValue'] != null && mappedColObj['staticValue'] != '') ? ("  " + colValue + "  ") : masterTableColumn.replace("<font color='#F911E9'>", "").replace("</font>", "")); //staticValue

                                    if (j != parseInt(mapObjLength) - 1) {
                                        joinQuery += " <font color='#0071c5'>" + mappedColObj['andOrOperator'] + "</font> ";
                                        selectQuery += "  " + mappedColObj['andOrOperator'] + "  ";
                                    }
                                    joinQuery += "</li>";
                                }
                                j++;
                            }
                            joinQuery += "</ul></li>";
                        }
                    }
                }


            }
        }

        i++;
    }
    );
    joinQuery += "</ul></li></ul>";
    query += joinQuery;
// where clause selectedTables
    var whereClauseCondition = "";
    var j = 0;
    var rowCount = $('#selectedTables >tbody >tr').length;
    $("#selectedTables tbody tr").each(function () {
        var tdArray = this.cells;
        if (tdArray != null && tdArray.length != 0) {
            var tableName = $(tdArray[0]).text();
            if (rowCount == 1) {
                query += " <font color='#F911E9'>" + tableName + "</font>";
                selectQuery += "  " + tableName + " ";
            }
            var condition = $("#whereClauseConditionsArea_" + j).val();
            if (condition != null && condition != '' && $.trim(condition)) {
                if (whereClauseCondition != null && whereClauseCondition != '') {
                    whereClauseCondition += "AND " + condition;
                } else {
                    whereClauseCondition += condition
                }

            }

        }
        j++;
    });
    if (whereClauseCondition != null && whereClauseCondition != '') {
        query += "<font color='#0071c5'> WHERE </font>" + whereClauseCondition;
        selectQuery += "  WHERE  " + whereClauseCondition;
    }
    var orderByObj = {};
    var i = 1;
    var rowCount = $('#fromTablesOrderCauseTable >tbody >tr').length;
    $("#fromTablesOrderCauseTable tbody tr").each(function () {
        var tdArray = this.cells;
        if (tdArray != null && tdArray.length != 0) {
            var columnName = $(tdArray[1]).find("input").val();
            var direction = $(tdArray[2]).find("option:selected").val();
            if (columnName != null && columnName != '') {
                if (i == 1) {
                    query += " <font color='#0071c5'> ORDER BY </font>";
                    selectQuery += "   ORDER BY  ";
                }
                var columnsArray = columnName.split(":");
                query += " <font color='#F911E9'>" + columnsArray[0] + "</font>." + columnsArray[1] + " <font color='#0071c5'>" + direction + "</font>";
                selectQuery += "  " + columnsArray[0] + "." + columnsArray[1] + "  " + direction + " ";
                if (i != parseInt(rowCount)) {
                    query += " , ";
                    selectQuery += " , ";
                }
            }
            i++;
        }
    });
    var groupByColumns = "";
    var rowCount = $('#fromTablesGroupCauseTable >tbody >tr').length;
    var i = 0;
    $("#fromTablesGroupCauseTable tbody tr").each(function () {
        var tdArray = this.cells;
        if (tdArray != null && tdArray.length != 0) {
            var columnName = $(tdArray[1]).find("input").val();
            if (columnName != null && columnName != '') {
                var columnsArray = columnName.split(":");
                groupByColumns += " <font color='#F911E9'>" + columnsArray[0] + "</font>." + columnsArray[1];
                if (i != parseInt(rowCount) - 1) {
                    groupByColumns += ",";
                }
            }

            i++;
        }
    });
    if (groupByColumns != null && groupByColumns != '') {
        query += "<font color='#0071c5'> GROUP BY </font> " + groupByColumns;
        selectQuery += " GROUP BY  " + groupByColumns.replace("<font color='#F911E9'>", "").replace("</font>", "");
    }

    $("#" + sqlQueryTabId).html('<div id = "visionSqlViewQuery" class = "visionSqlViewQuery1"><div id="sqlQueryIcons"><img src="images/SQL ICON-01.svg" id="validateTotalQuery" '
            + 'onclick="validateQuery()" title="Click here to validate query" style="width:15px;height: 15px;cursor:pointer;">'
            + '<input type="hidden" id="generatedTotalQuery"/></div>' + query + '</div>');
    $("#generatedTotalQuery").val(selectQuery);
}
function showTablesListInTab() {
    showLoader();
    //var startIndex = parseInt(startIndx);
    var endIndex = $("#treePageSize").val();
    var connectionObj;
    var tableListHtml = '';
    var selectedItem = $('#savedConnections').jqxTree('selectedItem');

    var selecteddbLiItem = $('#savedConnections').jqxTree('getItem', selectedItem.parentElement);
    var parentItemLabel = selecteddbLiItem.value;
    var selectedItemLabel = selectedItem.label;
    var liId = ('li-' + parentItemLabel + '-' + selectedItemLabel);
    var divId = ('div-' + parentItemLabel + '-' + selectedItemLabel);
    currentTabId = divId;
    var splitterHeight = (parseInt($(window).height()) - parseInt(pageHeight));
    var value = parentItemLabel;
    var parentkey = selectedItem.value;
    var level = selectedItem.level;

    var filterId = divId.replace("div-", "filter-");
    var filterInput = '<div class ="visionFilterObjectsDiv"><input id="' + filterId + '" class="visionFilterObjectsInput" onkeyup="filterTables(event)" type="text" autocomplete="off" placeholder="Search Tables" value=""/></div>'

    var selectedItemIndex = $('#jqxtabs').jqxTabs('selectedItem');

    if (selectedItemIndex != null) {
        var html = $('#jqxtabs').html();
        if (html.indexOf(divId) > -1) {
            var length = $('#jqxtabs').jqxTabs('length');
            for (var i = 0; i < length; i++) {
                var content = $('#jqxtabs').jqxTabs('getContentAt', i);
                var id = $(content).attr("id");
                if ($(content).attr("id") == divId) {
                    showEtlList("schemaObjectsDiv", "schemaObjects");
                    $('#jqxtabs').jqxTabs('select', i);
                    break;
                }
            }
            stopLoader();
            return false;
        }

    }

    if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
    {
        var conObj = savedDBData[value];
        if (conObj != null && !jQuery.isEmptyObject(conObj)) {
            connectionObj = conObj;
        }

    }
    var columnsObj = globalTreeObj['treeColumnObj'];
    var extTreeParams = $("#extTreeParams").val();
    var data = {
        parentkey: parentkey,
        treeId: globalTreeObj['treeId'],
        level: level,
        extTreeParams: extTreeParams,
        columnsObj: JSON.stringify(columnsObj),
        connectionObj: JSON.stringify(connectionObj),
        startIndex: 0,
        endIndex: endIndex
    };
    var url = 'getTreePagingDataOpt';
    if (selectedItem.level > 3)
    {
        var parentEventItem = selectedItem.element;
        for (var i = level; i > 1; i--)
        {
            parentEventItem = parentEventItem.parentElement.parentElement;
        }
        var selectedSuperParentItem = $('#savedConnections').jqxTree('getItem', parentEventItem);
        var selectedParentValue = selectedSuperParentItem['value'];
        if (selectedParentValue != null && selectedParentValue != '' &&
                selectedParentValue != undefined && selectedParentValue == 'ERP')
        {
            url = 'getTreeErpConnectionDetails';
        }
    }
//    if (selecteddbLiItem != null) {
//        var selectedParentValue = selecteddbLiItem['value'];
//        if (selectedParentValue != null && selectedParentValue != ''
//                && selectedParentValue != undefined && selectedParentValue == 'ERP')
//        {
//            url = 'getTreeErpConnectionDetails';
//        }
//    }
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: url,
        cache: false,
        data: data,
        success: function (data, status, xhr) {
//            $(".showEtlIcons").css("margin-top", "223px");
//            $(".showEtlIcons").css("margin-left", "-265");
            stopLoader();
            showEtlList("schemaObjectsDiv", "schemaObjects");
            stopLoader();
            tableListHtml += '<div id ="' + divId + '-div">';
            $.each(data, function (index) {
                if (this.value == "Show More") {

                } else {
                    tableListHtml += '<div title="' + connectionObj['CONNECTION_NAME'] + ' . ' + this.label + '" class = "visionObjectNameDiv" style="cursor:pointer">'
                            + '<img class="visionTableIcon" src="images/GridDB.png"/>'
                            + '<span> ' + this.label + '</span></div>'
                }
            });
            tableListHtml += '</div>';
            if (selectedItemIndex != null) {
                $('#jqxtabs').jqxTabs('addLast', selectedItem.label, tableListHtml);


            } else {
                $("#jqxtabs").find("ul").append('<li title="' + liId.replace("li-", "").replace("-", ".") + '">' + selectedItem.label + ' </li>');// <img src="images/delete_icon_white.png" id="'+liId.replace("li_","close_")+'" class ="visionCloseTab" onclick = "closeCurrentTab(event)" />
                $("#jqxtabs").append('<div id="' + divId + '" class="visionObjectsListDiv">' + filterInput + tableListHtml + '</div>');
                $('#jqxtabs').jqxTabs({width: "100%", height: "100%", position: 'top', theme: 'ui-redmond', reorder: true});
                $('#jqxtabs').jqxTabs('showAllCloseButtons');
                $("#jqxtabs").find("li.jqx-tabs-title").find("div.jqx-tabs-close-button").css("background-image", "url(images/close.png)");
                $("#jqxtabs").find("li.jqx-tabs-title-selected-top").find("div.jqx-tabs-close-button").css("background-image", "url(images/close_white.png)");
                var height = $('#connectionsDiv').height();
                var width = $('#connectionsDiv').width();

                $('#schemaObjectsDiv').css("height", height);
                $('#schemaObjectsDiv').css("width", width);
            }

            $('#jqxtabs').unbind('selected').on('selected', function (event) {
                var $thid = this;
                $("#jqxtabs").find("li.jqx-tabs-title").find("div.jqx-tabs-close-button").css("background-image", "url(images/close.png)");
                $("#jqxtabs").find("li.jqx-tabs-title-selected-top").find("div.jqx-tabs-close-button").css("background-image", "url(images/close_white.png)");

            });

            $('#jqxtabs').unbind('add').on('add', function (event) {
                var tabContent = $('#jqxtabs').jqxTabs('getContentAt', event.args.item);
//                tabContent.id = currentTabId;
                $(tabContent).attr("class", "visionObjectsListDiv");
                $(tabContent).attr("id", currentTabId);
//                tabContent.attr("class", "visionObjectsListDiv");

                var filterId = currentTabId.replace("div-", "filter-");
                var filterInput = '<div class ="visionFilterObjectsDiv"><input id="' + filterId + '" class="visionFilterObjectsInput" onkeyup="filterTables(event)" autocomplete="off"  type="text" placeholder="Search Tables" value=""/></div>'
                $(tabContent).prepend(filterInput);
                $("#jqxtabs").find("li.jqx-tabs-title").find("div.jqx-tabs-close-button").css("background-image", "url(images/close.png)");
                $("#jqxtabs").find("li.jqx-tabs-title-selected-top").find("div.jqx-tabs-close-button").css("background-image", "url(images/close_white.png)");

                //var title = $('#jqxtabs').jqxTabs('getTitleAt',event.args.item);
                var selectedTabLi = $("#jqxtabs").find("li.jqx-tabs-title-selected-top");
//                var cancelButton = selectedTabLi.find("div.jqx-tabs-close-button-selected");
//                cancelButton.css("background-image","url(images/close_white.png)");
                $(selectedTabLi).attr("title", currentTabId.replace("div-", "").replace("-", "."));


                // $('#jqxtabs').jqxTabs('select', event.args.item); 
            });

            var i = 0;
            var singleSelectedItem;
            $("div.visionObjectNameDiv").click(function (e) {
                if (e.ctrlKey) {
                    $(this).toggleClass("selectedTabs");
                    if (singleSelectedItem !== null && singleSelectedItem !== '' && singleSelectedItem !== undefined) {
                        selectedItems[0] = singleSelectedItem;
                    }
                    selectedItems[i] = $(this).text();
                    i++;
                } else {
                    $(this).addClass("selectedTabs").siblings().removeClass('selectedTabs');
                    singleSelectedItem = $(this).text();
                    selectedItems = [];

                    if (singleSelectedItem !== null && singleSelectedItem !== '' && singleSelectedItem !== undefined) {
                        i = 1;
                    } else {
                        i = 0;
                    }

                }
            });

            var timeout;
            $(".visionObjectsListDiv").unbind("scroll").on("scroll", function (event) {
                console.log("scrolled ;;")
                if (($(this).scrollTop() + $(this).innerHeight() >= $(this)[0].scrollHeight) && ($(this)[0].scrollHeight > $(this).innerHeight())) {
                    console.log("iam in scroll functionality...........");
                    clearTimeout(timeout);
                    timeout = setTimeout(function () {
                        loadOnScrollDown(event);
                    }, 50);

                }
            });


            $("div.visionObjectNameDiv").draggable({
                cursor: "move",
                opacity: 0.7,
                helper: 'clone',
//                                appendTo: 'body',
                zIndex: 1000,
                helper: function (event, ui) {
                    var $this = $(this);
                    var innerText = $this.text();
                    var title = innerText;
                    var descripttion = $this.attr("title");
                    var value = this.parentElement.id.split("-")[1];
                    var connectionObj;
                    if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
                    {
                        var conObj = savedDBData[value];
                        if (conObj != null && !jQuery.isEmptyObject(conObj)) {
                            connectionObj = conObj;
                        }

                    }

                    var operatorData = {
                        top: event.screenX,
                        left: event.screenY,
                        statusLebel: innerText.trim(),
                        tableName: (connectionObj['CONN_CUST_COL1'] != 'SAP') ? (connectionObj['CONN_USER_NAME'] + '.' + innerText.trim()) : innerText.trim(),
//                        tableName: connectionObj['CONN_USER_NAME'] + "." + innerText.trim(),
                        dragType: "Table",
                        CONNECTION_NAME: connectionObj['CONNECTION_NAME'], //CONNECTION_NAME
                        CONN_DB_NAME: connectionObj['CONN_DB_NAME'], //CONN_DB_NAME
                        CONN_CUST_COL1: connectionObj['CONN_CUST_COL1'], //CONN_CUST_COL1
                        connObj: connectionObj,
                        properties: {
                            //body: '<div title="' + innerText + '" class="visionOpLabelDiv">' + innerText + '</div>',
                            body: '<div  title="' + connectionObj['CONNECTION_NAME'] + ' . ' + title + '" class=""><div title="Double Click here to view the Transformations rules(' + connectionObj['CONNECTION_NAME'] + '.' + title + ')" style="cursor:pointer"><img src="images/GridDB.png"'
                                    + 'class="visionOpIcons"  ondblclick=viewTrfmRules()  style="width:18px;height: 18px;"/></div><div class="visionOpLabelDiv">' + title + '</div>'

                                    + '</div>',
                            inputs: {
                                input_1: {
                                    label: '',
                                    multipleLinks: true
                                }
                            },
                            outputs: {
                                output_1: {
                                    label: '',

                                }
                            }
                        }
                    };
//            var obj = $('#flowchartworkSourcesspace').flowchart('getOperatorElement', operatorData);
                    return  $('#flowchartworkSourcesspace').flowchart('getOperatorElement', operatorData);
                },
                stop: function (e, ui) {
                    var selItems = [];
                    if (!(selectedItems != null && selectedItems.length != null && selectedItems.length > 0)) {
                        selItems[0] = this.innerText;
                    } else {
                        selItems = selectedItems;
//                        for (var i = 0; i < selectedItems.length; i++) {
//                            selItems[i] = selectedItems[i];
//                        }
                    }
                    $(".flowchart-operator-connector-label").hide();
                    var $flowchart = $('#flowchartworkSourcesspace');
                    var $container = $('#flowchartworkSourcesspace');
                    var elOffset = ui.offset;
                    var containerOffset = $container.offset();
                    if (elOffset.left > containerOffset.left &&
                            elOffset.top > containerOffset.top &&
                            elOffset.left < containerOffset.left + $container.width() &&
                            elOffset.top < containerOffset.top + $container.height()) {
                        var flowchartOffset = $flowchart.offset();
                        var relativeLeft = elOffset.left - flowchartOffset.left;
                        var relativeTop = elOffset.top - flowchartOffset.top;
                        var positionRatio = $flowchart.flowchart('getPositionRatio');
                        relativeLeft /= positionRatio;
                        relativeTop /= positionRatio;
                        elOffset.left = relativeLeft;
                        elOffset.top = relativeTop;
                    }
                    var top = elOffset.top;
                    for (var j = 0; j < selItems.length; j++) {
                        // ravi start
                        trfmRulesChanged = true;
                        // ravi end
                        var $this = $(this);
                        var innerText = selItems[j];
                        var title = innerText;
                        var value = this.parentElement.id.split("-")[1];
                        var connectionObj;
                        if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
                        {
                            var conObj = savedDBData[value];
                            if (conObj != null && !jQuery.isEmptyObject(conObj)) {
                                connectionObj = conObj;
                            }

                        }


                        var data = {
                            top: top,
                            left: elOffset.left,
                            statusLebel: innerText.trim(),
                            tableName: (connectionObj['CONN_CUST_COL1'] != 'SAP') ? (connectionObj['CONN_USER_NAME'] + '.' + innerText.trim()) : innerText.trim(),
//                            tableName: connectionObj['CONN_USER_NAME'] + "." + innerText.trim(),
                            CONNECTION_NAME: connectionObj['CONNECTION_NAME'], //CONNECTION_NAME
                            CONN_DB_NAME: connectionObj['CONN_DB_NAME'], //CONN_DB_NAME
                            CONN_CUST_COL1: connectionObj['CONN_CUST_COL1'], //CONN_CUST_COL1
                            dragType: "Table",
                            connObj: connectionObj,
//                statusLebel: innerText,
                            properties: {

//                                title: innerText,
                                //  body: '<div  title="' + title + '"  class="visionOpLabelDiv">' + innerText + '</div>',
                                body: '<div  title="' + connectionObj['CONNECTION_NAME'] + ' . ' + title + '" class=""><div title="Double Click here to view the Transformations rules(' + connectionObj['CONNECTION_NAME'] + '.' + title + ')" style="cursor:pointer"><img src="images/GridDB.png"'
                                        + 'class="visionOpIcons"  ondblclick=viewTrfmRules()  style="width:18px;height: 18px;"/></div><div class="visionOpLabelDiv">' + title + '</div>'

                                        + '</div>',
                                inputs: {
                                    input_1: {
                                        label: 'I-MAP',
                                        multipleLinks: true
                                    }
                                },
                                outputs: {
                                    output_1: {
                                        label: 'O-MAP',

                                    }
                                }
                            }
                        };
                        $flowchart.flowchart('addOperator', data);
                        $(".flowchart-operator-connector-label").hide();
                        $(".flowchart-operator-title").hide();
                        top = top + 70;
                    }
                }
//                stop: function (e, ui) {
//                    var selItems = [];
//                    if (!(selectedItems != null && selectedItems.length != null && selectedItems.length > 0)) {
//                        selItems[0] = this.innerText;
//                    } else {
//                        selItems = selectedItems;                       
//                    }
//                    $(".flowchart-operator-connector-label").hide();
//                    var $flowchart = $('#flowchartworkSourcesspace');
//                    var $container = $('#flowchartworkSourcesspace');
//                    // ravi start
//                    trfmRulesChanged = true;
//                    // ravi end
//                    var $this = $(this);
//                    var innerText = this.innerText;
//                    var title = innerText;
//                    var value = this.parentElement.id.split("-")[1];
//                    var connectionObj;
//                    if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
//                    {
//                        var conObj = savedDBData[value];
//                        if (conObj != null && !jQuery.isEmptyObject(conObj)) {
//                            connectionObj = conObj;
//                        }
//
//                    }
//
//
//                    var elOffset = ui.offset;
//                    var containerOffset = $container.offset();
//                    if (elOffset.left > containerOffset.left &&
//                            elOffset.top > containerOffset.top &&
//                            elOffset.left < containerOffset.left + $container.width() &&
//                            elOffset.top < containerOffset.top + $container.height()) {
//                        var flowchartOffset = $flowchart.offset();
//                        var relativeLeft = elOffset.left - flowchartOffset.left;
//                        var relativeTop = elOffset.top - flowchartOffset.top;
//                        var positionRatio = $flowchart.flowchart('getPositionRatio');
//                        relativeLeft /= positionRatio;
//                        relativeTop /= positionRatio;
//                        elOffset.left = relativeLeft;
//                        elOffset.top = relativeTop;
//                    }
//
//                    var data = {
//                        top: elOffset.top,
//                        left: elOffset.left,
//                        statusLebel: innerText.trim(),
//                        tableName: connectionObj['CONN_USER_NAME'] + "." + innerText.trim(),
//                        CONNECTION_NAME: connectionObj['CONNECTION_NAME'], //CONNECTION_NAME
//                        CONN_DB_NAME: connectionObj['CONN_DB_NAME'], //CONN_DB_NAME
//                        CONN_CUST_COL1: connectionObj['CONN_CUST_COL1'], //CONN_CUST_COL1
//                        dragType: "Table",
//                        connObj: connectionObj,
////                statusLebel: innerText,
//                        properties: {
//
////                                title: innerText,
//                            //  body: '<div  title="' + title + '"  class="visionOpLabelDiv">' + innerText + '</div>',
//                            body: '<div  title="' + connectionObj['CONNECTION_NAME'] + ' . ' + title + '" class=""><div title="Double Click here to view the Transformations rules(' + connectionObj['CONNECTION_NAME'] + '.' + title + ')" style="cursor:pointer"><img src="images/GridDB.png"'
//                                    + 'class="visionOpIcons"  ondblclick=viewTrfmRules()  style="width:18px;height: 18px;"/></div><div class="visionOpLabelDiv">' + title + '</div>'
//
//                                    + '</div>',
//                            inputs: {
//                                input_1: {
//                                    label: 'I-MAP',
//                                    multipleLinks: true
//                                }
//                            },
//                            outputs: {
//                                output_1: {
//                                    label: 'O-MAP',
//
//                                }
//                            }
//                        }
//                    };
//                    $flowchart.flowchart('addOperator', data);
//                    $(".flowchart-operator-connector-label").hide();
//                    $(".flowchart-operator-title").hide();
//                }
            });



        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });


}

var timeOut = null;
function filterTables(event) {

    clearTimeout(timeOut);

    timeOut = setTimeout(function () {
        showLoader();
        var inputValue = $(event.target).val();
//if (inputValue.length<3){
//    return false;
//}
        var filterValue = $(event.target).val();
        if (filterValue != null && filterValue != "") {
            filterValue = "%" + filterValue + "%";
        }
        var inputId = $(event.target).attr("id");
        var divId = inputId.replace("filter-", "div-");
        var dbLabel = inputId.split("-")[1];

        var selectedLevelValue = inputId.split("-")[2].toUpperCase();
        var selectConnObj = savedDBData[dbLabel];
        var selectedLevel = '4'
        var selectColumnsObj = globalTreeObj['treeColumnObj']
        var selectBoxValue = "LIKE"

        selectedLevelValue = selectedLevelValue.toUpperCase();
        var item = $("#savedConnections").jqxTree('getSelectedItem');
        var url = 'getTreePagingDataOpt';
        if (item != null)
        {
            var level = item['level'];
            var parentEventItem = item['parentElement'];
            for (var i = level; i > 0; i--)
            {
                parentEventItem = parentEventItem['parentElement'];
            }
            var selectedItem = $('#savedConnections').jqxTree('getItem', parentEventItem);
            var selectedParentValue = selectedItem['value'];
            if (selectedParentValue != null && selectedParentValue != ''
                    && selectedParentValue != undefined && selectedParentValue == 'ERP')
            {
                url = 'getTreeErpConnectionDetails';
            }
        }
        $.ajax({
            type: "post",
            traditional: true,
            dataType: 'json',
            url: url,
            cache: false,
            data: {
                parentkey: ((selectedLevelValue != null && selectedLevelValue != '') ? selectedLevelValue.toUpperCase() : ""),
                level: selectedLevel,
                columnsObj: JSON.stringify(selectColumnsObj),
                connectionObj: JSON.stringify(selectConnObj),
                filterValue: filterValue,
                filterCondition: selectBoxValue,
                startIndex: 0,
                endIndex: $("#treePageSize").val()
            },
            success: function (data, status, xhr) {
                var tableListHtml = "";
                stopLoader();

                $.each(data, function (index) {
                    if (this.label == "Show More...") {

                    } else {
                        tableListHtml += '<div class = "visionObjectNameDiv" style="cursor:pointer"><img class="visionTableIcon" src="images/GridDB.png"/><span> ' + this.label + '</span></div>'
                    }
                });

//            var filterInput = '<div class ="visionFilterObjectsDiv"><input id="' + inputId + '" class="visionFilterObjectsInput" onkeyup="filterTables(event)" type="text" placeholder="Search Tables" /></div>'
//            $("[id='" + divId + "']").html(filterInput + tableListHtml);
//            $("[id='" + inputId + "']").focus();
//            $("[id='" + inputId + "']").val(inputValue);

                $("[id='" + divId + "-div']").html(tableListHtml);
                selectedItems = [];
                var i = 0;
                var singleSelectedItem;
                $("div.visionObjectNameDiv").click(function (e) {
                    $(document).find("input").blur();
                    if (e.ctrlKey) {
                        $(this).toggleClass("selectedTabs");
                        if (singleSelectedItem !== null && singleSelectedItem !== '' && singleSelectedItem !== undefined) {
                            selectedItems[0] = singleSelectedItem;
                        }
                        selectedItems[i] = $(this).text();
                        i++;
                    } else {
                        $(this).addClass("selectedTabs").siblings().removeClass('selectedTabs');
                        singleSelectedItem = $(this).text();
                        selectedItems = [];

                        if (singleSelectedItem !== null && singleSelectedItem !== '' && singleSelectedItem !== undefined) {
                            i = 1;
                        } else {
                            i = 0;
                        }

                    }
                });
                $("div.visionObjectNameDiv").draggable({
                    cursor: "move",
                    opacity: 0.7,
                    helper: 'clone',
//                                appendTo: 'body',
                    zIndex: 1000,
                    helper: function (event, ui) {
                        var $this = $(this);
                        var innerText = $this.text();
                        var title = innerText;
                        var descripttion = $this.attr("title");
                        var value = this.parentElement.id.split("-")[1];
                        var connectionObj;
                        if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
                        {
                            var conObj = savedDBData[value];
                            if (conObj != null && !jQuery.isEmptyObject(conObj)) {
                                connectionObj = conObj;
                            }

                        }

                        var operatorData = {
                            top: event.screenX,
                            left: event.screenY,
                            statusLebel: innerText.trim(),
                            tableName: (connectionObj['CONN_CUST_COL1'] != 'SAP') ? (connectionObj['CONN_USER_NAME'] + '.' + innerText.trim()) : innerText.trim(),
//                            tableName: connectionObj['CONN_USER_NAME'] + "." + innerText.trim(),
                            dragType: "Table",
                            CONNECTION_NAME: connectionObj['CONNECTION_NAME'], //CONNECTION_NAME
                            CONN_DB_NAME: connectionObj['CONN_DB_NAME'], //CONN_DB_NAME
                            CONN_CUST_COL1: connectionObj['CONN_CUST_COL1'], //CONN_CUST_COL1
                            connObj: connectionObj,
                            properties: {
                                //body: '<div title="' + innerText + '" class="visionOpLabelDiv">' + innerText + '</div>',

                                body: '<div  title="' + connectionObj['CONNECTION_NAME'] + ' . ' + title + '" class=""><div title="Double Click here to view the Transformations rules(' + connectionObj['CONNECTION_NAME'] + '.' + title + ')" style="cursor:pointer"><img src="images/GridDB.png"'
                                        + 'class="visionOpIcons"  ondblclick=viewTrfmRules()  style="width:18px;height: 18px;"/></div><div class="visionOpLabelDiv">' + title + '</div>'

                                        + '</div>',
                                inputs: {
                                    input_1: {
                                        label: '',
                                        multipleLinks: true
                                    }
                                },
                                outputs: {
                                    output_1: {
                                        label: '',

                                    }
                                }
                            }
                        };
//            var obj = $('#flowchartworkSourcesspace').flowchart('getOperatorElement', operatorData);
                        return  $('#flowchartworkSourcesspace').flowchart('getOperatorElement', operatorData);
                    },
                    stop: function (e, ui) {
                        $(document).find("input").blur();
                        var selItems = [];
                        if (!(selectedItems != null && selectedItems.length != null && selectedItems.length > 0)) {
                            selItems[0] = this.innerText;
                        } else {
                            selItems = [];
                            for (var i = 0; i < selectedItems.length; i++) {
                                selItems[i] = selectedItems[i];
                            }
                        }
                        $(".flowchart-operator-connector-label").hide();
                        var $flowchart = $('#flowchartworkSourcesspace');
                        var $container = $('#flowchartworkSourcesspace');
                        var elOffset = ui.offset;
                        var containerOffset = $container.offset();
                        if (elOffset.left > containerOffset.left &&
                                elOffset.top > containerOffset.top &&
                                elOffset.left < containerOffset.left + $container.width() &&
                                elOffset.top < containerOffset.top + $container.height()) {
                            var flowchartOffset = $flowchart.offset();
                            var relativeLeft = elOffset.left - flowchartOffset.left;
                            var relativeTop = elOffset.top - flowchartOffset.top;
                            var positionRatio = $flowchart.flowchart('getPositionRatio');
                            relativeLeft /= positionRatio;
                            relativeTop /= positionRatio;
                            elOffset.left = relativeLeft;
                            elOffset.top = relativeTop;
                        }
                        var top = elOffset.top;
                        for (var j = 0; j < selItems.length; j++) {
                            var $this = $(this);
                            var innerText = selItems[j];
                            var title = innerText;
                            var value = this.parentElement.id.split("-")[1];
                            var connectionObj;
                            if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
                            {
                                var conObj = savedDBData[value];
                                if (conObj != null && !jQuery.isEmptyObject(conObj)) {
                                    connectionObj = conObj;
                                }

                            }



//            var divId = "mapDivId_" + 0;
//            if ($("#" + divId).length == 0) {
//                //it doesn't exist
//            }else{
//                
//            }
                            //mapDivId:'',
                            var data = {
                                top: top,
                                left: elOffset.left,
                                statusLebel: innerText.trim(),
                                tableName: (connectionObj['CONN_CUST_COL1'] != 'SAP') ? (connectionObj['CONN_USER_NAME'] + '.' + innerText.trim()) : innerText.trim(),
//                                tableName: connectionObj['CONN_USER_NAME'] + "." + innerText.trim(),
                                dragType: "Table",
                                connObj: connectionObj,
                                CONNECTION_NAME: connectionObj['CONNECTION_NAME'], //CONNECTION_NAME
                                CONN_DB_NAME: connectionObj['CONN_DB_NAME'], //CONN_DB_NAME
                                CONN_CUST_COL1: connectionObj['CONN_CUST_COL1'], //CONN_CUST_COL1
//                statusLebel: innerText,
                                properties: {

//                                title: innerText,
                                    //  body: '<div  title="' + title + '"  class="visionOpLabelDiv">' + innerText + '</div>',
                                    body: '<div  title="' + connectionObj['CONNECTION_NAME'] + ' . ' + title + '" class=""><div title="Double Click here to view the Transformations rules(' + connectionObj['CONNECTION_NAME'] + '.' + title + ')" style="cursor:pointer"><img src="images/GridDB.png"'
                                            + 'class="visionOpIcons"  ondblclick=viewTrfmRules()  style="width:18px;height: 18px;"/></div><div class="visionOpLabelDiv">' + title + '</div>'

                                            + '</div>',
                                    inputs: {
                                        input_1: {
                                            label: 'I-MAP',
                                            multipleLinks: true
                                        }
                                    },
                                    outputs: {
                                        output_1: {
                                            label: 'O-MAP',

                                        }
                                    }
                                }
                            };
                            $flowchart.flowchart('addOperator', data);
                            $(".flowchart-operator-connector-label").hide();
                            $(".flowchart-operator-title").hide();
                            top = top + 70;
                        }
                    }
                });

                stopLoader();
            },
            error: function (e) {
                console.log(e);
                sessionTimeout(e);
                stopLoader();
            }
        });

    }, 1000);
}
//function filterTables(event) {
//
//    clearTimeout(timeOut);
//
//    timeOut = setTimeout(function () {
//        showLoader();
//        var inputValue = $(event.target).val();
////if (inputValue.length<3){
////    return false;
////}
//        var filterValue = $(event.target).val();
//        if (filterValue != null && filterValue != "") {
//            filterValue = "%" + filterValue + "%";
//        }
//        var inputId = $(event.target).attr("id");
//        var divId = inputId.replace("filter-", "div-");
//        var dbLabel = inputId.split("-")[1];
//
//        var selectedLevelValue = inputId.split("-")[2].toUpperCase();
//        var selectConnObj = savedDBData[dbLabel];
//        var selectedLevel = '4'
//        var selectColumnsObj = globalTreeObj['treeColumnObj']
//        var selectBoxValue = "LIKE"
//
//        selectedLevelValue = selectedLevelValue.toUpperCase();
//        var item = $("#savedConnections").jqxTree('getSelectedItem');
//        var url = 'getTreePagingDataOpt';
//        if (item != null)
//        {
//            var level = item['level'];
//            var parentEventItem = item['parentElement'];
//            for (var i = level; i > 0; i--)
//            {
//                parentEventItem = parentEventItem['parentElement'];
//            }
//            var selectedItem = $('#savedConnections').jqxTree('getItem', parentEventItem);
//            var selectedParentValue = selectedItem['value'];
//            if (selectedParentValue != null && selectedParentValue != ''
//                    && selectedParentValue != undefined && selectedParentValue == 'ERP')
//            {
//                url = 'getTreeErpConnectionDetails';
//            }
//        }
//        $.ajax({
//            type: "post",
//            traditional: true,
//            dataType: 'json',
//            url: url,
//            cache: false,
//            data: {
//                parentkey: ((selectedLevelValue != null && selectedLevelValue != '') ? selectedLevelValue.toUpperCase() : ""),
//                level: selectedLevel,
//                columnsObj: JSON.stringify(selectColumnsObj),
//                connectionObj: JSON.stringify(selectConnObj),
//                filterValue: filterValue,
//                filterCondition: selectBoxValue,
//                startIndex: 0,
//                endIndex: $("#treePageSize").val()
//            },
//            success: function (data, status, xhr) {
//                var tableListHtml = "";
//                stopLoader();
//
//                $.each(data, function (index) {
//                    if (this.label == "Show More...") {
//
//                    } else {
//                        tableListHtml += '<div class = "visionObjectNameDiv" style="cursor:pointer"><img class="visionTableIcon" src="images/GridDB.png"/><span> ' + this.label + '</span></div>'
//                    }
//                });
//
////            var filterInput = '<div class ="visionFilterObjectsDiv"><input id="' + inputId + '" class="visionFilterObjectsInput" onkeyup="filterTables(event)" type="text" placeholder="Search Tables" /></div>'
////            $("[id='" + divId + "']").html(filterInput + tableListHtml);
////            $("[id='" + inputId + "']").focus();
////            $("[id='" + inputId + "']").val(inputValue);
//                $("[id='" + divId + "-div']").html(tableListHtml);
//                $("div.visionObjectNameDiv").draggable({
//                    cursor: "move",
//                    opacity: 0.7,
//                    helper: 'clone',
////                                appendTo: 'body',
//                    zIndex: 1000,
//                    helper: function (event, ui) {
//                        var $this = $(this);
//                        var innerText = $this.text();
//                        var title = innerText;
//                        var descripttion = $this.attr("title");
//                        var value = this.parentElement.id.split("-")[1];
//                        var connectionObj;
//                        if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
//                        {
//                            var conObj = savedDBData[value];
//                            if (conObj != null && !jQuery.isEmptyObject(conObj)) {
//                                connectionObj = conObj;
//                            }
//
//                        }
//
//                        var operatorData = {
//                            top: event.screenX,
//                            left: event.screenY,
//                            statusLebel: innerText.trim(),
//                            tableName: connectionObj['CONN_USER_NAME'] + "." + innerText.trim(),
//                            dragType: "Table",
//                            CONNECTION_NAME: connectionObj['CONNECTION_NAME'], //CONNECTION_NAME
//                            CONN_DB_NAME: connectionObj['CONN_DB_NAME'], //CONN_DB_NAME
//                            CONN_CUST_COL1: connectionObj['CONN_CUST_COL1'], //CONN_CUST_COL1
//                            connObj: connectionObj,
//                            properties: {
//                                //body: '<div title="' + innerText + '" class="visionOpLabelDiv">' + innerText + '</div>',
//
//                                body: '<div  title="' + connectionObj['CONNECTION_NAME'] + ' . ' + title + '" class=""><div title="Double Click here to view the Transformations rules(' + connectionObj['CONNECTION_NAME'] + '.' + title + ')" style="cursor:pointer"><img src="images/GridDB.png"'
//                                        + 'class="visionOpIcons"  ondblclick=viewTrfmRules()  style="width:18px;height: 18px;"/></div><div class="visionOpLabelDiv">' + title + '</div>'
//
//                                        + '</div>',
//                                inputs: {
//                                    input_1: {
//                                        label: '',
//                                        multipleLinks: true
//                                    }
//                                },
//                                outputs: {
//                                    output_1: {
//                                        label: '',
//
//                                    }
//                                }
//                            }
//                        };
////            var obj = $('#flowchartworkSourcesspace').flowchart('getOperatorElement', operatorData);
//                        return  $('#flowchartworkSourcesspace').flowchart('getOperatorElement', operatorData);
//                    },
//                    stop: function (e, ui) {
//                        $(".flowchart-operator-connector-label").hide();
//                        var $flowchart = $('#flowchartworkSourcesspace');
//                        var $container = $('#flowchartworkSourcesspace');
//                        var $this = $(this);
//                        var innerText = this.innerText;
//                        var title = innerText;
//                        var value = this.parentElement.id.split("-")[1];
//                        var connectionObj;
//                        if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
//                        {
//                            var conObj = savedDBData[value];
//                            if (conObj != null && !jQuery.isEmptyObject(conObj)) {
//                                connectionObj = conObj;
//                            }
//
//                        }
//
//
//                        var elOffset = ui.offset;
//                        var containerOffset = $container.offset();
//                        if (elOffset.left > containerOffset.left &&
//                                elOffset.top > containerOffset.top &&
//                                elOffset.left < containerOffset.left + $container.width() &&
//                                elOffset.top < containerOffset.top + $container.height()) {
//                            var flowchartOffset = $flowchart.offset();
//                            var relativeLeft = elOffset.left - flowchartOffset.left;
//                            var relativeTop = elOffset.top - flowchartOffset.top;
//                            var positionRatio = $flowchart.flowchart('getPositionRatio');
//                            relativeLeft /= positionRatio;
//                            relativeTop /= positionRatio;
//                            elOffset.left = relativeLeft;
//                            elOffset.top = relativeTop;
//                        }
////            var divId = "mapDivId_" + 0;
////            if ($("#" + divId).length == 0) {
////                //it doesn't exist
////            }else{
////                
////            }
//                        //mapDivId:'',
//                        var data = {
//                            top: elOffset.top,
//                            left: elOffset.left,
//                            statusLebel: innerText.trim(),
//                            tableName: connectionObj['CONN_USER_NAME'] + "." + innerText.trim(),
//                            dragType: "Table",
//                            connObj: connectionObj,
//                            CONNECTION_NAME: connectionObj['CONNECTION_NAME'], //CONNECTION_NAME
//                            CONN_DB_NAME: connectionObj['CONN_DB_NAME'], //CONN_DB_NAME
//                            CONN_CUST_COL1: connectionObj['CONN_CUST_COL1'], //CONN_CUST_COL1
////                statusLebel: innerText,
//                            properties: {
//
////                                title: innerText,
//                                //  body: '<div  title="' + title + '"  class="visionOpLabelDiv">' + innerText + '</div>',
//                                body: '<div  title="' + connectionObj['CONNECTION_NAME'] + ' . ' + title + '" class=""><div title="Double Click here to view the Transformations rules(' + connectionObj['CONNECTION_NAME'] + '.' + title + ')" style="cursor:pointer"><img src="images/GridDB.png"'
//                                        + 'class="visionOpIcons"  ondblclick=viewTrfmRules()  style="width:18px;height: 18px;"/></div><div class="visionOpLabelDiv">' + title + '</div>'
//
//                                        + '</div>',
//                                inputs: {
//                                    input_1: {
//                                        label: 'I-MAP',
//                                        multipleLinks: true
//                                    }
//                                },
//                                outputs: {
//                                    output_1: {
//                                        label: 'O-MAP',
//
//                                    }
//                                }
//                            }
//                        };
//                        $flowchart.flowchart('addOperator', data);
//                        $(".flowchart-operator-connector-label").hide();
//                        $(".flowchart-operator-title").hide();
//                    }
//                });
//
//                stopLoader();
//            },
//            error: function (e) {
//                console.log(e);
//                sessionTimeout(e);
//                stopLoader();
//            }
//        });
//
//    }, 1000);
//}
function loadOnScrollDown(event) {
    showLoader();
    var divId = $(event.target).attr("id");
    // filter issue
    var filterDivId = divId.replace("div-", "filter-");
    var filterValue = $("#" + filterDivId).val();
    if (filterValue != null && filterValue != "") {
        filterValue = "%" + filterValue + "%";
    }
    // filter issue
    var value = divId.split("-")[1];
    var parentkey = divId.split("-")[2].toUpperCase();
    ;
    var level = '4';
    var arr = $("[id='" + divId + "']").find(".visionObjectNameDiv");

    var startIndex = arr.length + 1;
    var endIndex = arr.length + parseInt($("#treePageSize").val());
    var connectionObj;
    var tableListHtml = '';

    if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
    {
        var conObj = savedDBData[value];
        if (conObj != null && !jQuery.isEmptyObject(conObj)) {
            connectionObj = conObj;
        }

    }
    var item = $("#savedConnections").jqxTree('getSelectedItem');
    var url = 'getTreePagingDataOpt';
    if (item != null)
    {
        var level = item['level'];
        var parentEventItem = item['parentElement'];
        for (var i = level; i > 0; i--)
        {
            parentEventItem = parentEventItem['parentElement'];
        }
        var selectedItem = $('#savedConnections').jqxTree('getItem', parentEventItem);
        var selectedParentValue = selectedItem['value'];
        if (selectedParentValue != null && selectedParentValue != ''
                && selectedParentValue != undefined && selectedParentValue == 'ERP')
        {
            url = 'getTreeErpConnectionDetails';
        }
    }
    var columnsObj = globalTreeObj['treeColumnObj'];
    var extTreeParams = $("#extTreeParams").val();
    var data = {
        parentkey: parentkey,
        treeId: globalTreeObj['treeId'],
        level: level,
        filterValue: filterValue,
        filterCondition: "LIKE",
        extTreeParams: extTreeParams,
        columnsObj: JSON.stringify(columnsObj),
        connectionObj: JSON.stringify(connectionObj),
        startIndex: startIndex,
        endIndex: endIndex
    };

    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: url,
        cache: false,
        data: data,
        success: function (data, status, xhr) {

            $.each(data, function (index) {
                if (this.label == "Show More...") {

                } else {
                    tableListHtml += '<div title=' + connectionObj['CONNECTION_NAME'] + ' . ' + this.label + ' class = "visionObjectNameDiv">\n\
                                     <img class="visionTableIcon" src="images/GridDB.png"/><span> ' + this.label + '</span></div>'
                }
            });
            $("#" + divId + "-div").append(tableListHtml);
            var i = 0;
            var singleSelectedItem;
            $("div.visionObjectNameDiv").click(function (e) {
                if (e.ctrlKey) {
                    $(this).toggleClass('selectedTabs', true);
                    if (singleSelectedItem !== null && singleSelectedItem !== '' && singleSelectedItem !== undefined) {
                        selectedItems[0] = singleSelectedItem;
                    }
                    selectedItems[i] = $(this).text();
                    i++;
                } else {
                    $(this).addClass("selectedTabs").siblings().removeClass('selectedTabs');
                    singleSelectedItem = $(this).text();
                    selectedItems = [];

                    if (singleSelectedItem !== null && singleSelectedItem !== '' && singleSelectedItem !== undefined) {
                        i = 1;
                    } else {
                        i = 0;
                    }
                }
            });

            $("div.visionObjectNameDiv").draggable({
                cursor: "move",
                opacity: 0.7,
                helper: 'clone',
//                                appendTo: 'body',
                zIndex: 1000,
                helper: function (event, ui) {
                    var $this = $(this);
                    var innerText = $this.text();
                    var title = innerText;
                    var descripttion = $this.attr("title");
                    var value = this.parentElement.id.split("-")[1];
                    var connectionObj;
                    if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
                    {
                        var conObj = savedDBData[value];
                        if (conObj != null && !jQuery.isEmptyObject(conObj)) {
                            connectionObj = conObj;
                        }

                    }

                    var operatorData = {
                        top: event.screenX,
                        left: event.screenY,
                        statusLebel: innerText.trim(),
                        tableName: innerText.trim(),
                        dragType: "Table",
                        connObj: connectionObj,
                        properties: {
                            //body: '<div title="' + innerText + '" class="visionOpLabelDiv">' + innerText + '</div>',

                            body: '<div  title="' + title + '" class=""><div><img src="images/GridDB.png"\n\
                            class="visionOpIcons"  style="width:18px;height: 18px;"/></div><div class="visionOpLabelDiv">' + title + '</div></div>',
                            inputs: {
                                input_1: {
                                    label: '',
                                    multipleLinks: true
                                }
                            },
                            outputs: {
                                output_1: {
                                    label: '',

                                }
                            }
                        }
                    };
//            var obj = $('#flowchartworkSourcesspace').flowchart('getOperatorElement', operatorData);
                    return  $('#flowchartworkSourcesspace').flowchart('getOperatorElement', operatorData);
                },
                stop: function (e, ui) {
                    var selItems = [];
                    if (!(selectedItems != null && selectedItems.length != null && selectedItems.length > 0)) {
                        selItems[0] = this.innerText;
                    } else {
                        selItems = [];
                        for (var i = 0; i < selectedItems.length; i++) {
                            selItems[i] = selectedItems[i];
                        }
                    }
                    $(".flowchart-operator-connector-label").hide();
                    var $flowchart = $('#flowchartworkSourcesspace');
                    var $container = $('#flowchartworkSourcesspace');
                    var elOffset = ui.offset;
                    var containerOffset = $container.offset();
                    if (elOffset.left > containerOffset.left &&
                            elOffset.top > containerOffset.top &&
                            elOffset.left < containerOffset.left + $container.width() &&
                            elOffset.top < containerOffset.top + $container.height()) {
                        var flowchartOffset = $flowchart.offset();
                        var relativeLeft = elOffset.left - flowchartOffset.left;
                        var relativeTop = elOffset.top - flowchartOffset.top;
                        var positionRatio = $flowchart.flowchart('getPositionRatio');
                        relativeLeft /= positionRatio;
                        relativeTop /= positionRatio;
                        elOffset.left = relativeLeft;
                        elOffset.top = relativeTop;
                    }
                    var top = elOffset.top;
                    for (var j = 0; j < selItems.length; j++) {
                        // ravi start
                        trfmRulesChanged = true;
                        // ravi end
                        var $this = $(this);
                        var innerText = selItems[j];
                        var title = innerText;
                        var value = this.parentElement.id.split("-")[1];
                        var connectionObj;
                        if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
                        {
                            var conObj = savedDBData[value];
                            if (conObj != null && !jQuery.isEmptyObject(conObj)) {
                                connectionObj = conObj;
                            }

                        }



//            var divId = "mapDivId_" + 0;
//            if ($("#" + divId).length == 0) {
//                //it doesn't exist
//            }else{
//                
//            }
                        //mapDivId:'',
                        var data = {
                            top: top,
                            left: elOffset.left,
                            statusLebel: innerText.trim(),
                            tableName: (connectionObj['CONN_CUST_COL1'] != 'SAP') ? (connectionObj['CONN_USER_NAME'] + '.' + innerText.trim()) : innerText.trim(),
//                            tableName: connectionObj['CONN_USER_NAME'] + "." + innerText.trim(),
                            dragType: "Table",
                            CONNECTION_NAME: connectionObj['CONNECTION_NAME'], //CONNECTION_NAME
                            CONN_DB_NAME: connectionObj['CONN_DB_NAME'], //CONN_DB_NAME
                            CONN_CUST_COL1: connectionObj['CONN_CUST_COL1'], //CONN_CUST_COL1
                            connObj: connectionObj,
//                statusLebel: innerText,
                            properties: {

//                                title: innerText,
                                //  body: '<div  title="' + title + '"  class="visionOpLabelDiv">' + innerText + '</div>',
                                body: '<div  title="' + connectionObj['CONNECTION_NAME'] + ' . ' + title + '" class=""><div title="Double Click here to view the Transformations rules(' + connectionObj['CONNECTION_NAME'] + '.' + title + ')" style="cursor:pointer"><img src="images/GridDB.png"'
                                        + 'class="visionOpIcons"  ondblclick=viewTrfmRules()  style="width:18px;height: 18px;"/></div><div class="visionOpLabelDiv">' + title + '</div>'

                                        + '</div>',
                                inputs: {
                                    input_1: {
                                        label: 'I-MAP',
                                        multipleLinks: true
                                    }
                                },
                                outputs: {
                                    output_1: {
                                        label: 'O-MAP',

                                    }
                                }
                            }
                        };
                        $flowchart.flowchart('addOperator', data);
                        $(".flowchart-operator-connector-label").hide();
                        $(".flowchart-operator-title").hide();
                        top = top + 70;
                    }
                }
            });
            stopLoader();
        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });


}
//function loadOnScrollDown(event) {
//    showLoader();
//    var divId = $(event.target).attr("id");
//    // filter issue
//    var filterDivId = divId.replace("div-", "filter-");
//    var filterValue = $("#" + filterDivId).val();
//    if (filterValue != null && filterValue != "") {
//        filterValue = "%" + filterValue + "%";
//    }
//    // filter issue
//    var value = divId.split("-")[1];
//    var parentkey = divId.split("-")[2].toUpperCase();
//    ;
//    var level = '4';
//    var arr = $("[id='" + divId + "']").find(".visionObjectNameDiv");
//
//    var startIndex = arr.length + 1;
//    var endIndex = arr.length + parseInt($("#treePageSize").val());
//    var connectionObj;
//    var tableListHtml = '';
//
//    if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
//    {
//        var conObj = savedDBData[value];
//        if (conObj != null && !jQuery.isEmptyObject(conObj)) {
//            connectionObj = conObj;
//        }
//
//    }
//    var item = $("#savedConnections").jqxTree('getSelectedItem');
//    var url = 'getTreePagingDataOpt';
//    if (item != null)
//    {
//        var level = item['level'];
//        var parentEventItem = item['parentElement'];
//        for (var i = level; i > 0; i--)
//        {
//            parentEventItem = parentEventItem['parentElement'];
//        }
//        var selectedItem = $('#savedConnections').jqxTree('getItem', parentEventItem);
//        var selectedParentValue = selectedItem['value'];
//        if (selectedParentValue != null && selectedParentValue != ''
//                && selectedParentValue != undefined && selectedParentValue == 'ERP')
//        {
//            url = 'getTreeErpConnectionDetails';
//        }
//    }
//    var columnsObj = globalTreeObj['treeColumnObj'];
//    var extTreeParams = $("#extTreeParams").val();
//    var data = {
//        parentkey: parentkey,
//        treeId: globalTreeObj['treeId'],
//        level: level,
//        filterValue: filterValue,
//        filterCondition: "LIKE",
//        extTreeParams: extTreeParams,
//        columnsObj: JSON.stringify(columnsObj),
//        connectionObj: JSON.stringify(connectionObj),
//        startIndex: startIndex,
//        endIndex: endIndex
//    };
//
//    $.ajax({
//        type: "post",
//        traditional: true,
//        dataType: 'json',
//        url: url,
//        cache: false,
//        data: data,
//        success: function (data, status, xhr) {
//
//            $.each(data, function (index) {
//                if (this.label == "Show More...") {
//
//                } else {
//                    tableListHtml += '<div title=' + connectionObj['CONNECTION_NAME'] + ' . ' + this.label + ' class = "visionObjectNameDiv">\n\
//                                     <img class="visionTableIcon" src="images/GridDB.png"/><span> ' + this.label + '</span></div>'
//                }
//            });
//            $("#" + divId + "-div").append(tableListHtml);
//
//
//            $("div.visionObjectNameDiv").draggable({
//                cursor: "move",
//                opacity: 0.7,
//                helper: 'clone',
////                                appendTo: 'body',
//                zIndex: 1000,
//                helper: function (event, ui) {
//                    var $this = $(this);
//                    var innerText = $this.text();
//                    var title = innerText;
//                    var descripttion = $this.attr("title");
//                    var value = this.parentElement.id.split("-")[1];
//                    var connectionObj;
//                    if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
//                    {
//                        var conObj = savedDBData[value];
//                        if (conObj != null && !jQuery.isEmptyObject(conObj)) {
//                            connectionObj = conObj;
//                        }
//
//                    }
//
//                    var operatorData = {
//                        top: event.screenX,
//                        left: event.screenY,
//                        statusLebel: innerText.trim(),
//                        tableName: innerText.trim(),
//                        dragType: "Table",
//                        connObj: connectionObj,
//                        properties: {
//                            //body: '<div title="' + innerText + '" class="visionOpLabelDiv">' + innerText + '</div>',
//
//                            body: '<div  title="' + title + '" class=""><div><img src="images/GridDB.png"\n\
//                            class="visionOpIcons"  style="width:18px;height: 18px;"/></div><div class="visionOpLabelDiv">' + title + '</div></div>',
//                            inputs: {
//                                input_1: {
//                                    label: '',
//                                    multipleLinks: true
//                                }
//                            },
//                            outputs: {
//                                output_1: {
//                                    label: '',
//
//                                }
//                            }
//                        }
//                    };
////            var obj = $('#flowchartworkSourcesspace').flowchart('getOperatorElement', operatorData);
//                    return  $('#flowchartworkSourcesspace').flowchart('getOperatorElement', operatorData);
//                },
//                stop: function (e, ui) {
//                    $(".flowchart-operator-connector-label").hide();
//                    var $flowchart = $('#flowchartworkSourcesspace');
//                    var $container = $('#flowchartworkSourcesspace');
//                    // ravi start
//                    trfmRulesChanged = true;
//                    // ravi end
//                    var $this = $(this);
//                    var innerText = this.innerText;
//                    var title = innerText;
//                    var value = this.parentElement.id.split("-")[1];
//                    var connectionObj;
//                    if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
//                    {
//                        var conObj = savedDBData[value];
//                        if (conObj != null && !jQuery.isEmptyObject(conObj)) {
//                            connectionObj = conObj;
//                        }
//
//                    }
//
//
//                    var elOffset = ui.offset;
//                    var containerOffset = $container.offset();
//                    if (elOffset.left > containerOffset.left &&
//                            elOffset.top > containerOffset.top &&
//                            elOffset.left < containerOffset.left + $container.width() &&
//                            elOffset.top < containerOffset.top + $container.height()) {
//                        var flowchartOffset = $flowchart.offset();
//                        var relativeLeft = elOffset.left - flowchartOffset.left;
//                        var relativeTop = elOffset.top - flowchartOffset.top;
//                        var positionRatio = $flowchart.flowchart('getPositionRatio');
//                        relativeLeft /= positionRatio;
//                        relativeTop /= positionRatio;
//                        elOffset.left = relativeLeft;
//                        elOffset.top = relativeTop;
//                    }
////            var divId = "mapDivId_" + 0;
////            if ($("#" + divId).length == 0) {
////                //it doesn't exist
////            }else{
////                
////            }
//                    //mapDivId:'',
//                    var data = {
//                        top: elOffset.top,
//                        left: elOffset.left,
//                        statusLebel: innerText.trim(),
//                        tableName: connectionObj['CONN_USER_NAME'] + "." + innerText.trim(),
//                        dragType: "Table",
//                        CONNECTION_NAME: connectionObj['CONNECTION_NAME'], //CONNECTION_NAME
//                        CONN_DB_NAME: connectionObj['CONN_DB_NAME'], //CONN_DB_NAME
//                        CONN_CUST_COL1: connectionObj['CONN_CUST_COL1'], //CONN_CUST_COL1
//                        connObj: connectionObj,
////                statusLebel: innerText,
//                        properties: {
//
////                                title: innerText,
//                            //  body: '<div  title="' + title + '"  class="visionOpLabelDiv">' + innerText + '</div>',
//                            body: '<div  title="' + connectionObj['CONNECTION_NAME'] + ' . ' + title + '" class=""><div title="Double Click here to view the Transformations rules(' + connectionObj['CONNECTION_NAME'] + '.' + title + ')" style="cursor:pointer"><img src="images/GridDB.png"'
//                                    + 'class="visionOpIcons"  ondblclick=viewTrfmRules()  style="width:18px;height: 18px;"/></div><div class="visionOpLabelDiv">' + title + '</div>'
//
//                                    + '</div>',
//                            inputs: {
//                                input_1: {
//                                    label: 'I-MAP',
//                                    multipleLinks: true
//                                }
//                            },
//                            outputs: {
//                                output_1: {
//                                    label: 'O-MAP',
//
//                                }
//                            }
//                        }
//                    };
//                    $flowchart.flowchart('addOperator', data);
//                    $(".flowchart-operator-connector-label").hide();
//                    $(".flowchart-operator-title").hide();
//                }
//            });
//            stopLoader();
//        },
//        error: function (e) {
//            console.log(e);
//            sessionTimeout(e);
//            stopLoader();
//        }
//    });
//
//
//}

function viewTableData(DBValue, parentkey, connectionObj) {

//    var DBValue;
    var tableName;
    var connName;
//    var parentkey;
    var extTreeParams = $("#extTreeParams").val();
    var columnsObj = globalTreeObj['treeColumnObj'];
    var target = $("#jqxtabs").find(".visionSelectedObject")[0];
    if (!(parentkey != null && parentkey != '' && DBValue != null && DBValue != '')) {
        if (target != null) {
            tableName = target.textContent.trim();
            var div = $(target).parents('div.visionObjectsListDiv')[0];
            var divId = div.id;

            parentkey = target.textContent.trim().toUpperCase();

            DBValue = divId.split("-")[1];
            var liItems = $("#savedConnections").find("li :contains('" + DBValue + "')");
            $.each(liItems, function (index) {
                var liItem = $('#savedConnections').jqxTree('getItem', this);
                if (liItem != null && liItem.level == 3 && liItem.value == DBValue) {

                    var connItem = this.parentElement.parentElement;
                    var selectedConnItem = $('#savedConnections').jqxTree('getItem', connItem);
                    if (selectedConnItem != null && selectedConnItem.level == 2) {
                        connName = selectedConnItem.value;
                    }
                    stopLoader();
                    return false;
                }

            })

            // var selectedDBItem = $('#savedConnections').jqxTree('getItem', dbItem);
            //DBValue = selectedDBItem['value'];

//        var connItem = dbItem.parentElement.parentElement;
//        var selectedConnItem = $('#savedConnections').jqxTree('getItem', connItem);
//        if (selectedConnItem.level==2){
//             connName = selectedConnItem.value;
//        }

//            tableName = target.textContent.trim();
//            var div = $(target).parents('div.visionObjectsListDiv')[0];
//            var divId = div.id;
//
//            parentkey = target.textContent.trim().toUpperCase();
//
//            DBValue = divId.split("-")[1];
        } else {
            var parentItem = $('#savedConnections').jqxTree('getSelectedItem');
            tableName = parentItem.label;
            parentkey = parentItem.value
            var $element = $(parentItem.element);

            var dbItem = parentItem.element.parentElement.parentElement.parentElement.parentElement;

            //var dbItemvalue = parentItem.element.parentElement.parentElement.parentElement.parentElement.children[2].title;

            var selectedDBItem = $('#savedConnections').jqxTree('getItem', dbItem);
            DBValue = selectedDBItem['value'];

            var connItem = dbItem.parentElement.parentElement;
            var selectedConnItem = $('#savedConnections').jqxTree('getItem', connItem);
            if (selectedConnItem.level == 2) {
                connName = selectedConnItem.value;
            }

//            var parentItem = $('#savedConnections').jqxTree('getSelectedItem');
//            tableName = parentItem.label;
//            parentkey = parentItem.value
//            var $element = $(parentItem.element);
//
//            var dbItem = parentItem.element.parentElement.parentElement.parentElement.parentElement
//            //var dbItemvalue = parentItem.element.parentElement.parentElement.parentElement.parentElement.children[2].title;
//
//            var selectedDBItem = $('#savedConnections').jqxTree('getItem', dbItem);
//            DBValue = selectedDBItem['value'];
        }
    }

    if (!(tableName != null && tableName != '')) {
        tableName = parentkey;
    }



    var contentDivId = ("divGrid-" + DBValue + "-" + tableName).replace(/\s/g, '');
    var selectedItemIndex = $('#dataViewDiv').jqxTabs('selectedItem');
    if (selectedItemIndex != null) {
        var html = $('#dataViewDiv').html();
        if (html.indexOf(contentDivId) > -1
                || html.indexOf('"' + contentDivId.replace(/\//g, '') + '"') > -1) {
            var length = $('#dataViewDiv').jqxTabs('length');
            for (var i = 0; i < length; i++) {
                var content = $('#dataViewDiv').jqxTabs('getContentAt', i);
                var id = $(content).children('div:nth-child(3)').attr("id");
                if (id == null) {
                    id = $(content).children('div:first').children('div:nth-child(3)').attr("id");
                }
                if (id == contentDivId || id == contentDivId.replace(/\//g, '')) {

                    switchETLDesignTabs("li_contentView", "dataViewDiv");

                    $('#dataViewDiv').jqxTabs('select', i);
                    if (connName == "SAP") {
                        $('#dataViewDiv').jqxTabs('removeAt', i);

                    }


                    break;

                }

            }
            if (connName == "SAP") {
            } else {
                return false;
            }

        }

    }
//    if (selectedItemIndex != null) {
//        var html = $('#dataViewDiv').html();
//        if (html.indexOf(contentDivId) > -1
//                || html.indexOf('"' + contentDivId.replace(/\//g, '') + '"') > -1) {
//            var length = $('#dataViewDiv').jqxTabs('length');
//            for (var i = 0; i < length; i++) {
//                var content = $('#dataViewDiv').jqxTabs('getContentAt', i);
//                var id = $(content).children('div:nth-child(3)').attr("id");
//                if (id == null) {
//                    id = $(content).children('div:first').children('div:nth-child(3)').attr("id");
//                }
//                if (id == contentDivId || id == contentDivId.replace(/\//g, '')) {
//                    switchETLDesignTabs("li_contentView", "dataViewDiv");
//                    $('#dataViewDiv').jqxTabs('select', i);
//                    break;
//                }
//            }
//            return false;
//        }
//
//    }
//    if (selectedItemIndex != null) {
//        var html = $('#dataViewDiv').html();
//        if (html.indexOf(contentDivId) > -1
//                || html.indexOf('"' + contentDivId.replace(/\//g, '') + '"') > -1) {
//            var length = $('#dataViewDiv').jqxTabs('length');
//            for (var i = 0; i < length; i++) {
//                var content = $('#dataViewDiv').jqxTabs('getContentAt', i);
//                var id = $(content).attr("id");
//                if (id == null) {
//                    id = $(content).children('div:first').attr("id");
//                }
//                if (id == contentDivId || id == contentDivId.replace(/\//g, '')) {
//                    switchETLDesignTabs("li_contentView", "dataViewDiv");
//                    $('#dataViewDiv').jqxTabs('select', i);
//                    break;
//                }
//            }
//            return false;
//        }
//
//    }


    if (connectionObj == null || connectionObj == 'undefined') {
        if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
        {
            var conObj = savedDBData[DBValue];
            if (conObj != null && !jQuery.isEmptyObject(conObj)) {
                connectionObj = conObj;
            }

        }
    }

//    if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
//    {
//        var conObj = savedDBData[DBValue];
//        if (conObj != null && !jQuery.isEmptyObject(conObj)) {
//            connectionObj = conObj;
//        }
//
//    }

    var data = {
        parentkey: parentkey,
        treeId: globalTreeObj['treeId'],
        level: '5',
        extTreeParams: extTreeParams,
        columnsObj: JSON.stringify(columnsObj),
        connectionObj: JSON.stringify(connectionObj),
        startIndex: 0,
        endIndex: $("#treePageSize").val(),
        DBValue: DBValue,
        tableName: tableName
    };

//    var data = {
//        parentkey: parentkey,
//        treeId: globalTreeObj['treeId'],
//        level: '5',
//        extTreeParams: extTreeParams,
//        columnsObj: JSON.stringify(columnsObj),
//        connectionObj: JSON.stringify(connectionObj),
//        startIndex: 0,
//        endIndex: $("#treePageSize").val(),
//         tableName:tableName
//    };
    if (connName == null) { // ----------------ravi sap issue
        connName = connectionObj['CONN_CUST_COL1'];
    }
//    if (connName == "SAP") {
//        selectSapTableColumns(data);
//    } else {
//        viewTableDataGrid(data);
//    }
    viewTableDataGrid(data);
//    $.ajax({
//        type: "post",
//        traditional: true,
//        dataType: 'json',
//        url: 'getSchemaObjectMetaData',
//        cache: false,
//        data: data,
//
//        success: function (response) {
//            stopLoader();
//            if (response != null) {
//                // var responseObj = JSON.parse(response);
//                var dataArray = response['dataArray'];
//                var dataFieldsArray = response['dataFieldsArray'];
//                var columnsArray = response['columnsArray'];
//                var totalCount = response['totalCount'];
//                var gridId = ("divGrid-" + DBValue + "-" + tableName).replace(/\s/g, '');
//                gridId = gridId.replace(/\//g, '');
//                var selectedItemIndex = $('#dataViewDiv').jqxTabs('selectedItem');
//                if (selectedItemIndex == null) {
//
//                    //$("#designViewTab").jqxTabs('select', 1);
//                    switchETLDesignTabs("li_contentView", "dataViewDiv");
//                    $("#dataViewDiv").prepend("<ul></ul>");
//                    $("#dataViewDiv ul").prepend("<li title='" + DBValue + "." + tableName + "'>" + tableName + "</li>");
//                    $("#dataViewDiv").append("<div id='" + gridId + "'></div>");
//                    $('#dataViewDiv').jqxTabs({width: "100%", height: "100%", position: 'top', theme: 'ui-redmond', reorder: true});
//
//                    $('#dataViewDiv').jqxTabs('showAllCloseButtons');
//                    $("#dataViewDiv").find("li.jqx-tabs-title").find("div.jqx-tabs-close-button").css("background-image", "url(images/close.png)");
//                    $("#dataViewDiv").find("li.jqx-tabs-title-selected-top").find("div.jqx-tabs-close-button").css("background-image", "url(images/close_white.png)");
//
//
//
//                } else {
//                    switchETLDesignTabs("li_contentView", "dataViewDiv");
//                    $('#dataViewDiv').jqxTabs('addLast', DBValue + "." + tableName, '<div id="' + gridId + '"></div>');
//                }
//
//
//                var source =
//                        {
//                            type: 'POST',
////                                                async: false,
//                            datatype: "json",
//                            datafields: dataFieldsArray,
//                            data: data,
//                            url: 'getSchemaObjectData',
//                            cache: false,
//                            root: 'Rows',
//                            processdata: function (data) {
//                                data['getOnlyDataArray'] = 'Y';
//
//                            },
//                            beforeSend: function () {
//
//
//                            }, loadError: function (xhr, status, error) {
//                                $('#dataViewDiv').css("width", "102%");
//                            }, loadComplete: function (data)
//                            {
//                                $('#dataViewDiv').css("width", "102%");
//                                //$("#div_" + tableName).jqxGrid('hiderowdetails', 0);
//                                //$("#row0div_" + tableName).hide();
//
//                            },
//                            beforeprocessing: function (data) {
//
//                                source.totalrecords = data[data.length - 1];
//
//                            },
//                            sort: function ()
//                            {
////                                                $("#" + gridResultObj['gridId'] + "_sort_columns").remove();
//                                $("[id='" + gridId + "']").jqxGrid('updatebounddata', 'sort');
//                                try {
//                                    $("[id='" + gridId + "']").jqxGrid('clearselection');
//                                } catch (e) {
//                                }
//                                endTabLoader();
//                            },
//                            filter: function () {
//
//                                $("[id='" + gridId + "']").jqxGrid('updatebounddata', 'filter');
//                                try {
//                                    $("[id='" + gridId + "']").jqxGrid('clearselection');
//                                } catch (e) {
//                                }
//                                endTabLoader();
//                            }
//
//
//                        };
////                var source =
////                        {
////                            localdata: dataArray,
////                            datatype: "array",
////                            datafields: dataFieldsArray
////                        };
//                var dataAdapter = new $.jqx.dataAdapter(source);
//
//                $("[id='" + gridId + "']").jqxGrid(
//                        {
//                            width: "100%",
//                            height: "90%",
//                            theme: 'energyblue',
//                            autoshowloadelement: false,
//                            source: dataAdapter,
//                            pageable: true,
//                            pagesize: 50,
//                            showfilterrow: true,
//                            filterable: true,
//                            sortable: true,
//                            virtualmode: true,
//                            pagesizeoptions: ['50', '100', '500'],
//                            rendergridrows: function (params) {
//                                return params.data;
//                            },
//                            columnsresize: true,
//                            columns: columnsArray
//                        });
//
//
//
//
//
//                $('#dataViewDiv').unbind('selected').on('selected', function (event) {
//                    var $thid = this;
//
//                    $('#dataViewDiv').jqxTabs('getContentAt', i);
//
//                    $("#dataViewDiv").find("li.jqx-tabs-title").find("div.jqx-tabs-close-button").css("background-image", "url(images/close.png)");
//                    $("#dataViewDiv").find("li.jqx-tabs-title-selected-top").find("div.jqx-tabs-close-button").css("background-image", "url(images/close_white.png)");
//
//                });
//
//                $('#dataViewDiv').unbind('add').on('add', function (event) {
//
//                    $("#dataViewDiv").find("li.jqx-tabs-title").find("div.jqx-tabs-close-button").css("background-image", "url(images/close.png)");
////                    $("#dataViewDiv").find("li.jqx-tabs-title-selected-top").find("div.jqx-tabs-close-button").css("background-image", "url(images/close_white.png)");
//                    setTimeout(function () {
//                        $("#dataViewDiv").find("li.jqx-tabs-title-selected-top").find("div.jqx-tabs-close-button").css("background-image", "url(images/close_white.png)");
//
//                    }, 100);
//                    var selectedTabTitle = $("#dataViewDiv").jqxTabs("getTitleAt", event.args.item);
//                    $("#dataViewDiv").jqxTabs('setTitleAt', event.args.item, selectedTabTitle.split(".")[1]);
//                    var selectedTabLi = $("#dataViewDiv").find("li.jqx-tabs-title-selected-top");
//                    selectedTabLi.attr("title", selectedTabTitle);
//
//
//                });
//
//
//
//            }
//        },
//        error: function (e)
//        {
//            sessionTimeout(e);
//        }
//
//    });
}

function switchETLDesignTabs(liId, divId) {
    $("#" + liId).parent().find('li.visionETLDesignTabHighLight').removeClass('visionETLDesignTabHighLight');
    $("#" + liId).addClass('visionETLDesignTabHighLight');
    $("#contentSplitter").hide();
    $("#dataViewDiv").hide();
    $("#editorViewDiv").hide();
    $("#jobSchedulingViewDiv").hide();
    $("#" + divId).show();
    if (liId == 'li_SQLEditor') {
        $("#scriptsExecute").show();
    } else {
        $("#scriptsExecute").hide();
    }
    if (liId == 'li_JobScheduling') {
//        loadScheduledJobs();
        loadScheduledJobsInIAC();
    }

}
function searchTreeNode(treeId) {

    $('#' + treeId).jqxTree('collapseAll');
    var items = $('#' + treeId).jqxTree("getItems");
    var searchedValue = $("#treeSearchValue").val();
    if (searchedValue != null && searchedValue != '') {
        $("#searchTreeErrorMesg").hide();
        searchedValue = searchedValue.toUpperCase();
        var itemFound = false;
        for (var i = 0; i < items.length; i++) {
            //.toLowerCase().indexOf(searchedValue)
            if (items[i].level != 0) { // ravi colums search issue
//                var treeNodeValue = items[i].value;
                var treeNodeValue = items[i].label;// ravi colums search issue
                if (treeNodeValue != null && treeNodeValue != '') {
                    treeNodeValue = treeNodeValue.toUpperCase();

                    if (treeNodeValue.indexOf(searchedValue) > -1) {
                        if (!($(items[i].element).hasClass("searched"))) {
                            itemFound = true;
                            $(items[i].element).addClass("searched");
                            $('#' + treeId).jqxTree('expandItem', items[i].parentElement);
                            $('#' + treeId).jqxTree('selectItem', items[i]);
                            var elementTop = items[i].element['offsetTop'];
                            $("#" + treeId).animate({
                                scrollTop: parseInt(elementTop) - 100
                            });
                            break;
                        }
                    }
                }
            }
        }
        if (!itemFound) {
            $('#' + treeId + ' li').removeClass("searched");
        }
    } else {
        $("#searchTreeErrorMesg").html("Please provide value to search in tree");
        $("#searchTreeErrorMesg").show();
    }

}
function downloadExportedFile(fileName, orginalName) {
    $("#fileName").val(fileName);
    $("#orginalName").val(orginalName);
    $("#exportFinalData").submit();
}
function selectColumnFun($this, tableColType) {
    showLoader();
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    //dddw
    // prepare the data
    var data = [];
    var dataStr = $("#functionNameObj").val();
    if (dataStr != null && dataStr != '') {
        data = JSON.parse(dataStr);
        var source =
                {
                    dataType: "json",
                    dataFields: [
                        {name: "FUN_ID", type: "string"},
                        {name: "FUN_DISP_NAME", type: "string"},
                        {name: "FUN_DESCR", type: "string"},
                        {name: "FUN_NAME", type: "string"},
                        {name: "HL_FUN_ID", type: "string"},
                        {name: "FUN_FORM_ID", type: "string"},
                        {name: "FUN_LVL_TYPE", type: "string"},
                        {name: "DM_FUN_CUST_COL1", type: "string"},
                        {name: "DM_FUN_CUST_COL2", type: "string"},
                        {name: "ICON_PATH", type: "string"}
                    ],
                    hierarchy:
                            {
                                keyDataField: {name: 'FUN_ID'},
                                parentDataField: {name: 'HL_FUN_ID'}
                            },
                    id: 'FUN_ID',
                    localData: data
                };

        var dataAdapter = new $.jqx.dataAdapter(source);
        // create jqxTreeGrid.

        $("#columnMappingDialog").html("<div id='columnMappingTree' class='columnMappingTree'></div>");
        $("#columnMappingDialog").dialog({
            title: (labelObject['Functions'] != null ? labelObject['Functions'] : 'Functions'),
            modal: true,
            width: 400,
            maxWidth: 1000,
            height: 400,
            fluid: true,
            buttons: [{
                    text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                    click: function () {
                        var selection = $("#columnMappingTree").jqxTreeGrid('getSelection');
                        if (selection != null && selection.length != 0) {
                            var selectedRowData = selection[0];
                            if (selectedRowData != null
                                    && !jQuery.isEmptyObject(selectedRowData)
                                    && selectedRowData['HL_FUN_ID'] != null && selectedRowData['HL_FUN_ID'] != '') {
                                var source = $("#columnMappingTree").jqxTreeGrid('source');
                                $(this).html("");
                                $(this).dialog("close");
                                $(this).dialog("destroy");
                                openFunctionForm(selection[0], source, $this);
                            }
                        }

//                    var selectedItem = $("#columnMappingTree").jqxTree('getSelectedItem');
//                    if (selectedItem != null) {
//                        $($this).parents("td").find("input").val(selectedItem['value']);
//                        $($this).parents("td").find("input").attr("title", selectedItem['value']);
////                       $($this).parent.find("input").val(selectedItem['value']); 
//                    }


                    }

                }],
            open: function () {
                $("#columnMappingTree").jqxTreeGrid(
                        {
                            source: dataAdapter,
                            width: "100%",
                            height: 300,
                            sortable: true,
                            columnsResize: true,
                            columnsReorder: true,
                            enableHover: true,
                            enableBrowserSelection: true,
                            filterable: true,
//                            filterMode: 'simple',
//                        hierarchicalCheckboxes: true,
                            pageable: true,
                            pagerMode: 'advanced',
                            pagerPosition: 'bottom',
                            pageSize: 100,
                            pageSizeOptions: ['100', '200', '300'],
                            theme: 'energyblue',
                            selectionMode: 'singleRow',
                            autoShowLoadElement: false,
                            columns: [
                                {text: 'FUN_ID', dataField: 'FUN_ID', hidden: true, filterable: false},
                                {text: 'Function Name', dataField: 'FUN_DISP_NAME', width: '30%', filterable: true},
                                {text: 'Description', dataField: 'FUN_DESCR', width: '70%', filterable: true},
                                {text: 'value', dataField: 'FUN_NAME', hidden: true, filterable: false},
                                {text: 'HL_FUN_ID', dataField: 'HL_FUN_ID', hidden: true, filterable: false},
                                {text: 'FUN_FORM_ID', dataField: 'FUN_FORM_ID', hidden: true, filterable: false},
                                {text: 'FUN_LVL_TYPE', dataField: 'FUN_LVL_TYPE', hidden: true, filterable: false},
                                {text: 'DM_FUN_CUST_COL1', dataField: 'DM_FUN_CUST_COL1', hidden: true, filterable: false},
                                {text: 'DM_FUN_CUST_COL2', dataField: 'DM_FUN_CUST_COL2', hidden: true, filterable: false},
                                {text: 'ICON_PATH', dataField: 'ICON_PATH', hidden: true, filterable: false}
                            ]
                        });
                $('#columnMappingTree').on('rowDoubleClick', function (event) {
                    var args = event.args;
                    var selectedRowData = args.row;
                    if (selectedRowData != null
                            && !jQuery.isEmptyObject(selectedRowData)
                            && selectedRowData['HL_FUN_ID'] != null && selectedRowData['HL_FUN_ID'] != '') {
                        var source = $("#columnMappingTree").jqxTreeGrid('source');
                        $("#columnMappingDialog").html("");
                        $("#columnMappingDialog").dialog("close");
                        $("#columnMappingDialog").dialog("destroy");
                        openFunctionForm(selectedRowData, source, $this);
                    }


                });
//            $("#treeSearchValue").keyup(function (event) {
//                if (event.keyCode === 13) {
//                    // Cancel the default action, if needed
////            event.preventDefault();
//                    $("#treeNodeSearchIconId").click();
//                }
//            });
//            $('#columnMappingTree').jqxTree('expandItem', $("#columnMappingTree").find('li:first')[0]);
//            $("#columnMappingTree li").on('dblclick', function (event) {
//                var selectedItem = $("#columnMappingTree").jqxTree('getSelectedItem');
//                if (selectedItem != null && !(selectedItem['icon'] != null && selectedItem['icon'] != '')) {
//                    $($this).parents("td").find("input").val(selectedItem['value']);
//                    $($this).parents("td").find("input").attr("title", selectedItem['value']);
//                    $("#columnMappingDialog").dialog("close");
//                    $("#columnMappingDialog").dialog("destroy");
//                }
//
//            });
                $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");

                $(".visionHeaderMain").css("z-index", "999");
                $(".visionFooterMain").css("z-index", "999");
                $(".ui-dialog").addClass('visionDMTreePopup');

                stopLoader();
            },
            beforeClose: function (event, ui)
            {

                $(".visionHeaderMain").css("z-index", "99999");
                $(".visionFooterMain").css("z-index", "99999");

            }, close: function (event, ui)
            {

                $(this).html("");
                try {
                    $("#columnMappingTree").jqxTreeGrid('destroy');
                    $("#columnMappingTree").remove();
                    $("#treeSearchInputDiv").remove();

                } catch (e) {
                }
//            try {
//                $(this).dialog("destroy");
//            } catch (e) {
//            }
//                       
//                        
//                       
            }
//        ,resizeStop: function(event, ui) {
//          
//          var width = ui.size.width;
//          var height = ui.size.height;
//          var orginalWidth = ui.originalSize.width;
//          var orginalHeight = ui.originalSize.height;
//            var treeHeight = $('#columnMappingTree').jqxTree('height');
//            if (!(treeHeight != null && treeHeight != '')) {
//                treeHeight = $('.columnMappingTree').height();
//            }
//            if (parseInt(width) < 300) {
//                width = 300;
//            }else{
//               width = parseInt(width)-10;
//            }
//            if (parseInt(height) <= 350) {
//                height = 300;
//                treeHeight = 180;
//            }else{
//                height = parseInt(height) - 130;
//                treeHeight = height;
//            }
//            var inputDivHeight = $("#treeSearchInputDiv").height();
////            treeHeight = parseInt(treeHeight)-parseInt(inputDivHeight);
//            $('.columnMappingTree').height(treeHeight);
//            var treeWidth =   $('#columnMappingTree').jqxTree('width');
//            $('#columnMappingTree').jqxTree({width:parseInt(width)});
//            console.log("resizeStop");
//        }
//        , create: function (event, ui) {
//            $("#columnMappingTree li").on('dblclick', function (event) {
//                var selectedItem = $("#columnMappingTree").jqxTree('getSelectedItem');
//                if (selectedItem != null && !(selectedItem['icon'] != null && selectedItem['icon'] != '')) {
//                    $($this).parents("td").find("input").val(selectedItem['value']);
//                    $($this).parents("td").find("input").attr("title", selectedItem['value']);
//                }
//                $(this).dialog("close");
//                $(this).dialog("destroy");
//            });
//        }
        });

    } else {
        $.ajax({
            type: "post",
            traditional: true,
            url: 'getETLDBFunction',
            cache: false,
            data: {
                selectedRowData: JSON.stringify(selectedRowData)
            },
            success: function (response, status, xhr) {
                //columnMappingFormDialog
                if (response != null && response != '') {
                    stopLoader();
                    $("#functionNameObj").val(response);
                    data = JSON.parse(response);
                    var source =
                            {
                                dataType: "json",
                                dataFields: [
                                    {name: "FUN_ID", type: "string"},
                                    {name: "FUN_DISP_NAME", type: "string"},
                                    {name: "FUN_DESCR", type: "string"},
                                    {name: "FUN_NAME", type: "string"},
                                    {name: "HL_FUN_ID", type: "string"},
                                    {name: "FUN_FORM_ID", type: "string"},
                                    {name: "FUN_LVL_TYPE", type: "string"},
                                    {name: "DM_FUN_CUST_COL1", type: "string"},
                                    {name: "DM_FUN_CUST_COL2", type: "string"},
                                    {name: "ICON_PATH", type: "string"}
                                ],
                                hierarchy:
                                        {
                                            keyDataField: {name: 'FUN_ID'},
                                            parentDataField: {name: 'HL_FUN_ID'}
                                        },
                                id: 'FUN_ID',
                                localData: data
                            };

                    var dataAdapter = new $.jqx.dataAdapter(source);
                    // create jqxTreeGrid.

                    $("#columnMappingDialog").html("<div id='columnMappingTree' class='columnMappingTree'></div>");
                    $("#columnMappingDialog").dialog({
                        title: (labelObject['Functions'] != null ? labelObject['Functions'] : 'Functions'),
                        modal: true,
                        width: 400,
                        maxWidth: 1000,
                        height: 400,
                        fluid: true,
                        buttons: [{
                                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                                click: function () {
                                    var selection = $("#columnMappingTree").jqxTreeGrid('getSelection');
                                    if (selection != null && selection.length != 0) {
                                        var selectedRowData = selection[0];
                                        if (selectedRowData != null
                                                && !jQuery.isEmptyObject(selectedRowData)
                                                && selectedRowData['HL_FUN_ID'] != null && selectedRowData['HL_FUN_ID'] != '') {
                                            var source = $("#columnMappingTree").jqxTreeGrid('source');
                                            $(this).html("");
                                            $(this).dialog("close");
                                            $(this).dialog("destroy");
                                            openFunctionForm(selection[0], source, $this);
                                        }
                                    }

//                    var selectedItem = $("#columnMappingTree").jqxTree('getSelectedItem');
//                    if (selectedItem != null) {
//                        $($this).parents("td").find("input").val(selectedItem['value']);
//                        $($this).parents("td").find("input").attr("title", selectedItem['value']);
////                       $($this).parent.find("input").val(selectedItem['value']); 
//                    }


                                }

                            }],
                        open: function () {
                            $("#columnMappingTree").jqxTreeGrid(
                                    {
                                        source: dataAdapter,
                                        width: "100%",
                                        height: 300,
                                        sortable: true,
                                        columnsResize: true,
                                        columnsReorder: true,
                                        enableHover: true,
                                        enableBrowserSelection: true,
                                        filterable: true,
//                            filterMode: 'simple',
//                        hierarchicalCheckboxes: true,
                                        pageable: true,
                                        pagerMode: 'advanced',
                                        pagerPosition: 'bottom',
                                        pageSize: 100,
                                        pageSizeOptions: ['100', '200', '300'],
                                        theme: 'energyblue',
                                        selectionMode: 'singleRow',
                                        autoShowLoadElement: false,
                                        columns: [
                                            {text: 'FUN_ID', dataField: 'FUN_ID', hidden: true, filterable: false},
                                            {text: 'Function Name', dataField: 'FUN_DISP_NAME', width: '30%', filterable: true},
                                            {text: 'Description', dataField: 'FUN_DESCR', width: '70%', filterable: true},
                                            {text: 'value', dataField: 'FUN_NAME', hidden: true, filterable: false},
                                            {text: 'HL_FUN_ID', dataField: 'HL_FUN_ID', hidden: true, filterable: false},
                                            {text: 'FUN_FORM_ID', dataField: 'FUN_FORM_ID', hidden: true, filterable: false},
                                            {text: 'FUN_LVL_TYPE', dataField: 'FUN_LVL_TYPE', hidden: true, filterable: false},
                                            {text: 'DM_FUN_CUST_COL1', dataField: 'DM_FUN_CUST_COL1', hidden: true, filterable: false},
                                            {text: 'DM_FUN_CUST_COL2', dataField: 'DM_FUN_CUST_COL2', hidden: true, filterable: false},
                                            {text: 'ICON_PATH', dataField: 'ICON_PATH', hidden: true, filterable: false}
                                        ]
                                    });
                            $('#columnMappingTree').on('rowDoubleClick', function (event) {
                                var args = event.args;
                                var selectedRowData = args.row;
                                var source = $("#columnMappingTree").jqxTreeGrid('source');
                                if (selectedRowData != null
                                        && !jQuery.isEmptyObject(selectedRowData)
                                        && selectedRowData['HL_FUN_ID'] != null && selectedRowData['HL_FUN_ID'] != '') {
                                    $("#columnMappingDialog").html("");
                                    $("#columnMappingDialog").dialog("close");
                                    $("#columnMappingDialog").dialog("destroy");
                                    openFunctionForm(selectedRowData, source, $this);
                                }



                            });
//            $("#treeSearchValue").keyup(function (event) {
//                if (event.keyCode === 13) {
//                    // Cancel the default action, if needed
////            event.preventDefault();
//                    $("#treeNodeSearchIconId").click();
//                }
//            });
//            $('#columnMappingTree').jqxTree('expandItem', $("#columnMappingTree").find('li:first')[0]);
//            $("#columnMappingTree li").on('dblclick', function (event) {
//                var selectedItem = $("#columnMappingTree").jqxTree('getSelectedItem');
//                if (selectedItem != null && !(selectedItem['icon'] != null && selectedItem['icon'] != '')) {
//                    $($this).parents("td").find("input").val(selectedItem['value']);
//                    $($this).parents("td").find("input").attr("title", selectedItem['value']);
//                    $("#columnMappingDialog").dialog("close");
//                    $("#columnMappingDialog").dialog("destroy");
//                }
//
//            });
                            $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");

                            $(".visionHeaderMain").css("z-index", "999");
                            $(".visionFooterMain").css("z-index", "999");
                            $(".ui-dialog").addClass('visionDMTreePopup');

                            stopLoader();
                        },
                        beforeClose: function (event, ui)
                        {

                            $(".visionHeaderMain").css("z-index", "99999");
                            $(".visionFooterMain").css("z-index", "99999");

                        }, close: function (event, ui)
                        {

                            $(this).html("");
                            try {
                                $("#columnMappingTree").jqxTreeGrid('destroy');
                                $("#columnMappingTree").remove();
                                $("#treeSearchInputDiv").remove();

                            } catch (e) {
                            }
//            try {
//                $(this).dialog("destroy");
//            } catch (e) {
//            }
//                       
//                        
//                       
                        }
//        ,resizeStop: function(event, ui) {
//          
//          var width = ui.size.width;
//          var height = ui.size.height;
//          var orginalWidth = ui.originalSize.width;
//          var orginalHeight = ui.originalSize.height;
//            var treeHeight = $('#columnMappingTree').jqxTree('height');
//            if (!(treeHeight != null && treeHeight != '')) {
//                treeHeight = $('.columnMappingTree').height();
//            }
//            if (parseInt(width) < 300) {
//                width = 300;
//            }else{
//               width = parseInt(width)-10;
//            }
//            if (parseInt(height) <= 350) {
//                height = 300;
//                treeHeight = 180;
//            }else{
//                height = parseInt(height) - 130;
//                treeHeight = height;
//            }
//            var inputDivHeight = $("#treeSearchInputDiv").height();
////            treeHeight = parseInt(treeHeight)-parseInt(inputDivHeight);
//            $('.columnMappingTree').height(treeHeight);
//            var treeWidth =   $('#columnMappingTree').jqxTree('width');
//            $('#columnMappingTree').jqxTree({width:parseInt(width)});
//            console.log("resizeStop");
//        }
//        , create: function (event, ui) {
//            $("#columnMappingTree li").on('dblclick', function (event) {
//                var selectedItem = $("#columnMappingTree").jqxTree('getSelectedItem');
//                if (selectedItem != null && !(selectedItem['icon'] != null && selectedItem['icon'] != '')) {
//                    $($this).parents("td").find("input").val(selectedItem['value']);
//                    $($this).parents("td").find("input").attr("title", selectedItem['value']);
//                }
//                $(this).dialog("close");
//                $(this).dialog("destroy");
//            });
//        }
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



}
function openFunctionForm(selectedRowData, source, $this) {

    if (selectedRowData != null
            && !jQuery.isEmptyObject(selectedRowData)
            && selectedRowData['HL_FUN_ID'] != null && selectedRowData['HL_FUN_ID'] != '') {
        if (selectedRowData != null
                && !jQuery.isEmptyObject(selectedRowData)
                && selectedRowData['FUN_FORM_ID'] != null
                && selectedRowData['FUN_FORM_ID'] != ''
                ) {//FUN_FORM_ID
            var selectedRowDataMain = {};
            var columns = source._source.datafields;
            if (columns != null && columns.length != 0) {
                for (var i = 0; i < columns.length; i++) {
                    var columnObj = columns[i];
                    selectedRowDataMain[columnObj['name']] = selectedRowData[columnObj['name']];
                }
            }
            var dataFunobjstr = $($this).parents("td").find("input").attr("data-funobjstr");
            $.ajax({
                type: "post",
                traditional: true,
                url: 'getETLFunctionForm',
                cache: false,
                data: {
                    selectedRowData: JSON.stringify(selectedRowDataMain),
                    dataFunobjstr: dataFunobjstr
                },
                success: function (data, status, xhr) {
                    //columnMappingFormDialog
                    if (data != null && data != '') {
                        var response = JSON.parse(data);
                        if (response['messageFlag']) {
                            $("#columnMappingFormDialog").html(response['funFormStr'] + "<div id='funFormETLTableTr' style='display:none'></div>");

                            $("#columnMappingFormDialog").dialog({
                                title: (labelObject[selectedRowDataMain['FUN_DISP_NAME'] + ' Form'] != null ? labelObject[selectedRowDataMain['FUN_DISP_NAME'] + ' Form'] : selectedRowDataMain['FUN_DISP_NAME'] + ' Form'),
                                modal: true,
                                width: 650,
                                maxWidth: 1000,
                                height: 'auto',
                                maxHeight: 1000,
                                fluid: true,
                                buttons: [{
                                        text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                                        click: function () {
                                            var selectedFunctionName = selectedRowData['FUN_NAME'];
                                            var FUN_LVL_TYPE = selectedRowData['FUN_LVL_TYPE'];
                                            var HL_FUN_ID = selectedRowData['HL_FUN_ID'];
                                            var functionFormObj = {};
                                            var funStr = "";
                                            functionFormObj['functionName'] = selectedFunctionName;
                                            if (FUN_LVL_TYPE != 'MULTI_COLUMNS') {
                                                funStr += "" + selectedFunctionName + "(";
                                            } else {
                                                funStr += "" + selectedFunctionName + "";

                                            }
                                            var mainFunStr = "";
                                            if (FUN_LVL_TYPE != 'MULTI_COLUMNS') {
                                                mainFunStr += "" + selectedFunctionName + "(";
                                            } else {
                                                mainFunStr += "" + selectedFunctionName + "";

                                            }
                                            var rowCount = $('#funFormETLTable tr').length;
//                                        $('#myTable tr').length
                                            var loopCount = 0;
                                            var multiColumnsObj = {};
                                            $("#funFormETLTable tr").each(function () {
                                                var tdArray = this.cells;
                                                if (tdArray != null && tdArray.length != 0) {
                                                    if (FUN_LVL_TYPE == 'MULTI_COLUMNS') {
                                                        if (loopCount != 0) {
                                                            var multiColsObj = {};
                                                            for (var i = 1; i < tdArray.length; i++) {

                                                                var funValue = $(tdArray[i]).find("input").val();

                                                                if (!(funValue != null && funValue != '' && funValue != 'null')) {
                                                                    funValue = $(tdArray[i]).find("select").val();
                                                                }
                                                                if (funValue != null && funValue != '') {
                                                                    funValue = $.trim(funValue);
                                                                }
                                                                var funValueId = $(tdArray[i]).find("input").attr("id");
                                                                if (!(funValueId != null && funValueId != '' && funValueId != 'null')) {
                                                                    funValueId = $(tdArray[i]).find("select").attr("id");
                                                                }
                                                                if (funValueId != null && funValueId != '') {
                                                                    funValueId = $.trim(funValueId);
                                                                }
                                                                if (!(funValue != null
                                                                        && funValue != ''
                                                                        && funValue != undefined
                                                                        && funValue != 'undefined')) {
                                                                    funValue = "";
                                                                }
                                                                multiColsObj[funValueId] = funValue;

                                                                funStr += "'" + funValue + "'";
                                                                if ($(tdArray[i]).is(":visible")) {
                                                                    if (funValue.indexOf(".") > -1 ||
                                                                            funValueId == 'CASE_COND'
                                                                            || funValueId == 'OPERATOR') {
                                                                        mainFunStr += " " + funValue + " ";
                                                                    } else {

                                                                        mainFunStr += " '" + funValue + "' ";
                                                                    }

                                                                }
                                                                if (loopCount != parseInt(rowCount) - 1) {
                                                                    funStr += ",";


                                                                }
                                                                if (loopCount == 1 && i == 1) {
                                                                    functionFormObj['FUN_LVL_TYPE'] = FUN_LVL_TYPE;
                                                                }
                                                            }
                                                            multiColumnsObj[loopCount] = multiColsObj;
                                                        }

                                                    } else {
                                                        var funFormLabel = $(tdArray[0]).text();
                                                        var funValue = $(tdArray[1]).find("input").val();

                                                        if (!(funValue != null && funValue != '' && funValue != 'null')) {
                                                            funValue = $(tdArray[1]).find("select").val();
                                                        }
                                                        var funValueId = $(tdArray[1]).find("input").attr("id");
                                                        if (!(funValueId != null && funValueId != '' && funValueId != 'null')) {
                                                            funValueId = $(tdArray[1]).find("select").attr("id");
                                                        }
                                                        if (funValue != null && funValue != '') {
                                                            funValue = $.trim(funValue);
                                                        }
                                                        if (funValueId != null && funValueId != '') {
                                                            funValueId = $.trim(funValueId);
                                                        }
                                                        funStr += "'" + funValue + "'";
                                                        if ($(this).is(":visible")) {
                                                            if (funValue.indexOf(".") > -1) {
                                                                mainFunStr += " " + funValue + " ";
                                                            } else {
                                                                mainFunStr += "'" + funValue + "'";
                                                            }
                                                        }

                                                        functionFormObj[funValueId] = funValue;
                                                        if (loopCount != parseInt(rowCount) - 1) {
                                                            funStr += ",";
                                                            if ($(this).is(":visible") && loopCount != 0) {
                                                                mainFunStr += ",";

                                                            }

                                                        }


                                                    }

                                                    loopCount++;
                                                }
                                            });
                                            funStr += ")";
                                            if (FUN_LVL_TYPE != 'MULTI_COLUMNS') {
                                                mainFunStr += ")";
                                            }

                                            functionFormObj['funStr'] = funStr;
                                            functionFormObj['mainFunStr'] = mainFunStr;
                                            functionFormObj['HL_FUN_ID'] = HL_FUN_ID;
                                            functionFormObj['multiColumnsObj'] = multiColumnsObj;
                                            $($this).parents("td").find("input").attr("value", funStr);
//                                            $($this).parents("td").find("input").val(funStr);
                                            $($this).parents("td").find("input").attr("title", funStr);
                                            $($this).parents("td").find("input").attr("data-funobjstr", JSON.stringify(functionFormObj));
//                                            $(this).html("");
//                                            $(this).dialog("close");
//                                            $(this).dialog("destroy");
                                            $(this).html("");
                                            try {
                                                $(this).dialog("close");
                                            } catch (e) {
                                            }
                                            try {
                                                $(this).dialog("destroy");
                                            } catch (e) {
                                            }
                                        }

                                    }],
                                open: function () {
                                    $("#viewFunQuery").click(function () {
                                        console.log("iam in clickable ");
                                        viewFunQuery('funFormETLTable', selectedRowData);
                                    });
//                                    $("#viewFunQuery")
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
                                        $(this).dialog("close");
                                    } catch (e) {
                                    }
                                    try {
                                        $(this).dialog("destroy");
                                    } catch (e) {
                                    }

                                }
                            });
                            $("#funFormETLTableTr").html(response['addTrString']);
                        }

                    }


                },
                error: function (e) {
                    console.log(e);
                    sessionTimeout(e);
                    stopLoader();
                }
            });
        } else {
            var selectedFunctionName = selectedRowData['FUN_NAME'];
            var functionFormObj = {};
            functionFormObj['functionName'] = selectedFunctionName;
            functionFormObj['FUN_LVL_TYPE'] = selectedRowData['FUN_LVL_TYPE'];
            functionFormObj['DM_FUN_CUST_COL1'] = selectedRowData['DM_FUN_CUST_COL1'];
            functionFormObj['DM_FUN_CUST_COL2'] = selectedRowData['DM_FUN_CUST_COL2'];
            $($this).parents("td").find("input").val(selectedFunctionName);
            $($this).parents("td").find("input").attr("title", selectedFunctionName);
            $($this).parents("td").find("input").attr("data-funobjstr", JSON.stringify(functionFormObj));
        }
    } else {
        console.log("Please select function name");
    }


}
function selectFunColumnValue($this, columnType, titleName) {
    if (columnType == 'ALL_TABLE' || columnType == 'ALL_SCHEMA') {//ALL_TABLE 
        if (!(titleName != null && titleName != '')) {
            titleName = "Functions";
        }
        $("#columnMappingDialog").html("<div id='columnMappingTree' class='columnMappingTree'></div>");
        $("#columnMappingDialog").dialog({
            title: (labelObject[titleName] != null ? labelObject[titleName] : titleName),
            modal: true,
            width: 400,
            maxWidth: 1000,
            height: 430,
            fluid: true,
            buttons: [{
                    text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                    click: function () {
                        var selection = $("#columnMappingTree").jqxTreeGrid('getSelection');
                        if (selection != null && selection.length != 0) {
                            var selectedRowData = selection[0];
                            if (selectedRowData != null
                                    && !jQuery.isEmptyObject(selectedRowData)
                                    && selectedRowData['CONNECTION_NAME'] != null && selectedRowData['CONNECTION_NAME'] != '') {
                                $(this).html("");
                                $(this).dialog("close");
                                $(this).dialog("destroy");
                                $($this).parents("td").find("input").val(selectedRowData['CONNECTION_NAME'] + "." + selectedRowData['TABLE_NAME']);
                                $($this).parents("td").find("input").attr("title", selectedRowData['CONNECTION_NAME'] + "." + selectedRowData['TABLE_NAME']);
                                $($this).parents("td").find("input").attr("data-conobjstr", selectedRowData['CONNECTION_OBJ']);
                                $($this).parents("td").find("input").attr("data-tosystype", selectedRowData['CONNECTION_TYPE']);
                            } else if (columnType == 'ALL_SCHEMA' && selectedRowData != null
                                    && !jQuery.isEmptyObject(selectedRowData)) {
                                $(this).html("");
                                $(this).dialog("close");
                                $(this).dialog("destroy");
                                $($this).parents("td").find("input").val(selectedRowData['TABLE_NAME']);
                                $($this).parents("td").find("input").attr("title", selectedRowData['TABLE_NAME']);
                                $($this).parents("td").find("input").attr("data-conobjstr", selectedRowData['CONNECTION_OBJ']);
                                $($this).parents("td").find("input").attr("data-tosystype", selectedRowData['CONNECTION_TYPE']);
                            }
                        }

                    }

                }],
            open: function () {
                $("#columnMappingTree").jqxTreeGrid(
                        {
                            width: "100%",
                            virtualModeCreateRecords: function (expandedRecord, done) {
                                // expandedRecord is equal to null when the function is initially called, because there is still no record to be expanded.
                                // prepare the data
                                var source =
                                        {
                                            dataType: "json",
                                            dataFields: [
                                                {name: 'CONNECTION_OBJ', type: 'string'},
                                                {name: 'TABLE_NAME', type: 'string'},
                                                {name: 'CONNECTION_NAME', type: 'string'},
                                                {name: 'LEVEL_TYPE', type: 'string'},
                                                {name: 'icon', type: 'string'},
                                                {name: 'ID', type: 'string'},
                                                {name: 'CONNECTION_TYPE', type: 'string'}
                                            ],
                                            hierarchy:
                                                    {
                                                        keyDataField: {name: 'TABLE_NAME'},
                                                        parentDataField: {name: 'CONNECTION_NAME'}
                                                    },
                                            id: 'ID',
                                            url: "getLookupAllTables",
                                            method: "post",
                                            data: {
                                                expandedRecord: JSON.stringify(expandedRecord),
                                                columnType: columnType
                                            }
                                        };
                                var dataAdapter = new $.jqx.dataAdapter(source,
                                        {
                                            formatData: function (data) {
                                                if (expandedRecord == null) {
                                                    data.$filter = "(CONNECTION_NAME eq null)"
                                                } else {
                                                    data.$filter = "(CONNECTION_NAME eq " + expandedRecord.TABLE_NAME + ")"
                                                }
                                                return data;
                                            },
                                            loadComplete: function ()
                                            {
                                                done(dataAdapter.records);
                                                stopLoader();
                                            },
                                            loadStart: function ()
                                            {
                                                showLoader();
                                            },
                                            loadError: function (xhr, status, error) {
                                                stopLoader();
                                                done(false);
                                                throw new Error("Error: " + error.toString());

                                            }
                                        }
                                );
                                dataAdapter.dataBind();
                            },
                            virtualModeRecordCreating: function (record) {

                            },
                            height: 350,
                            sortable: true,
                            columnsResize: true,
                            columnsReorder: true,
                            enableHover: true,
                            enableBrowserSelection: true,
//                           filterable: true,
                            filterable: true,
//                            filterMode: 'simple',
                            icons: true,
                            hierarchicalCheckboxes: true,
                            pageable: true,
                            pagerMode: 'advanced',
                            pagerPosition: 'bottom',
                            pageSize: 100,
                            pageSizeOptions: ['100', '200', '300'],
                            theme: 'energyblue',
                            selectionMode: 'singleRow',
                            autoShowLoadElement: false,
                            columns: [
                                {text: 'Table/Schem Name', dataField: 'TABLE_NAME', width: '80%', filterable: true},
                                {text: 'Database Type', dataField: 'CONNECTION_TYPE', width: '30%', filterable: true},
                                {text: 'CONNECTION_NAME', dataField: 'CONNECTION_NAME', hidden: true, width: 160, filterable: false},
                                {text: 'LEVEL_TYPE', dataField: 'LEVEL_TYPE', hidden: true, width: 160, filterable: false},
                                {text: 'ID', dataField: 'ID', hidden: true, width: 160, filterable: false},
                                {text: 'CONNECTION_OBJ', dataField: 'CONNECTION_OBJ', hidden: true, width: 160, filterable: false}
                            ]
                        });
                $('#columnMappingTree').on('rowDoubleClick', function (event) {
                    var args = event.args;
                    var selectedRowData = args.row;
                    var source = $("#columnMappingTree").jqxTreeGrid('source');
                    if (selectedRowData != null
                            && !jQuery.isEmptyObject(selectedRowData)
                            && selectedRowData['CONNECTION_NAME'] != null && selectedRowData['CONNECTION_NAME'] != '') {
                        $("#columnMappingDialog").html("");
                        $("#columnMappingDialog").dialog("close");
                        $("#columnMappingDialog").dialog("destroy");
//                      
                        $($this).parents("td").find("input").val(selectedRowData['CONNECTION_NAME'] + "." + selectedRowData['TABLE_NAME']);
                        $($this).parents("td").find("input").attr("title", selectedRowData['CONNECTION_NAME'] + "." + selectedRowData['TABLE_NAME']);
                        $($this).parents("td").find("input").attr("data-conobjstr", selectedRowData['CONNECTION_OBJ']);
                    }



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
                    $("#columnMappingTree").jqxTreeGrid('destroy');
                    $("#columnMappingTree").remove();
                    $("#treeSearchInputDiv").remove();

                } catch (e) {
                }
            }
        });
    } else if (columnType == 'TO_COLUMN') {
        selectColumn($this, 'toColumn');
    } else if (columnType == 'LOOKUP_TABLE,COLUMN') {

        var lookupTableName = $("#" + columnType.split(",")[0]).val();
        if (lookupTableName != null && lookupTableName != '') {
            lookupTableName = lookupTableName.split(".")[1];
        }
        var connObjStr = $("#" + columnType.split(",")[0]).attr("data-conobjstr");
        $.ajax({
            datatype: "json",
            type: "post",
            traditional: true,
            url: 'getSelectedLookupTableColumns',
            cache: false,
            data: {
                lookupTableName: lookupTableName,
                connObj: connObjStr
            },
            success: function (response, status, xhr) {

                selectColumn($this, columnType, response);

            },
            error: function (e) {
                console.log(e);
                sessionTimeout(e);
                stopLoader();
            }
        });
    } else if (columnType == 'FROM_COLUMN') {
        selectColumn($this, 'fromColumn');
    }
}

function showSQLPopup($this) {
    showLoader();
    var selectedOperatorId = $('#flowchartworkSourcesspace').flowchart('getSelectedOperatorId');
    var selectedSQLOperatorData = $('#flowchartworkSourcesspace').flowchart('getOperatorData', selectedOperatorId);
    var getLinksTo = $('#flowchartworkSourcesspace').flowchart('getLinksTo', selectedOperatorId);


    if (getLinksTo != null && getLinksTo.length != 0) {
        var fromOperatorsArray = $('#flowchartworkSourcesspace').flowchart('getFromOperatorsByToOpId', selectedOperatorId);
        var createTableObj = selectedSQLOperatorData['createTableObj'];
        $.ajax({
            datatype: "json",
            type: "post",
            traditional: true,
            url: 'getFromTableColumns',
            cache: false,
            data: {
                fromOperators: JSON.stringify(fromOperatorsArray),
                createTableObj: (createTableObj != null ? JSON.stringify(createTableObj) : "")
            },
            success: function (response, status, xhr) {
                stopLoader();

                $("#columnSQLMappingDialog").html(response['sqlPopupDiv']);
                $("#columnSQLMappingDialog").dialog({
                    title: (labelObject['Create Table'] != null ? labelObject['Create Table'] : 'Create Table'),
                    modal: true,
                    width: 890,
                    maxWidth: 1000,
                    height: 350,
                    maxHeight: 1000,
                    fluid: true,
                    buttons: [{
                            text: (labelObject['Create'] != null ? labelObject['Create'] : 'Create'),
                            click: function () {
                                //sqlToTableName
                                //sqlToTableColumnsTable
                                var tableName = $("#sqlToTableName").val();
                                if (tableName != null && tableName != '') {
                                    tableName = tableName.toUpperCase();
                                    var dataSourceName = $("#sqlToDataSourceName").val();
                                    var dataSourceObj = $("#sqlToDataSourceName").attr("data-conobjstr");
                                    var rowCount = $('#sqlToTableColumnsTable >tbody >tr').length;
                                    if (rowCount != 0) {
                                        var createTableObj = {};
                                        createTableObj['tableName'] = tableName;
                                        createTableObj['dataSourceName'] = dataSourceName;
                                        createTableObj['dataSourceObj'] = dataSourceObj;
                                        var tableColsArray = [];
                                        $("#sqlToTableColumnsTable tbody tr").each(function () {
                                            var tdArray = this.cells;
                                            if (tdArray != null && tdArray.length != 0) {
                                                var columnName = $(tdArray[2]).find("input").val();
                                                var dataType = $(tdArray[3]).find("input").val();
                                                if (columnName != null && columnName != '' && dataType != null && dataType != '') {
                                                    var colsObj = {};
                                                    colsObj['COLUMN_NAME'] = $(tdArray[2]).find("input").val();

                                                    if ($(tdArray[1]).find("input[type=\"checkbox\"]").is(":checked")) {//input[type="checkbox"]:checked
                                                        colsObj['PK'] = "Y";
                                                    } else {
                                                        colsObj['PK'] = "N";
                                                    }
                                                    colsObj['DATA_TYPE'] = $(tdArray[3]).find("input").val();

                                                    tableColsArray.push(colsObj);
                                                }

                                            }
                                        });
                                        createTableObj['colsObj'] = tableColsArray;
                                        var tableName = createTableObj['tableName'];
                                        if (dataSourceObj != null) {
                                            var dataSourceObject = JSON.parse(dataSourceObj);
                                            if (dataSourceObject != null) {
                                                if (dataSourceObject['CONN_USER_NAME'] != null
                                                        && dataSourceObject['CONN_USER_NAME'] != ''
                                                        && tableName.indexOf(".") == -1
                                                        ) {
                                                    tableName = dataSourceObject['CONN_USER_NAME'] + "." + tableName;
                                                }
                                            }
                                        }
                                        selectedSQLOperatorData['tableName'] = tableName;
                                        selectedSQLOperatorData['createTableObj'] = createTableObj;
                                        selectedSQLOperatorData['statusLebel'] = createTableObj['tableName'];
                                        var dataSourceName = createTableObj['dataSourceName'];
                                        var connectionObj = {};
                                        if (!(dataSourceName != null && dataSourceName != '')) {//dataSourceName
                                            dataSourceName = 'Current V10';
                                        }
                                        if (dataSourceName == 'Current V10') {
                                            connectionObj = savedDBData['Current_V10'];
                                        } else {
                                            //dataSourceObj
                                            var dataSourceObjStr = createTableObj['dataSourceObj'];
                                            if (dataSourceObjStr != null && dataSourceObjStr != '') {
                                                connectionObj = JSON.parse(dataSourceObjStr);
                                            }
                                        }

                                        selectedSQLOperatorData['CONNECTION_NAME'] = connectionObj['CONNECTION_NAME'];
                                        selectedSQLOperatorData['CONN_DB_NAME'] = connectionObj['CONN_DB_NAME'];
                                        selectedSQLOperatorData['connObj'] = connectionObj;
                                        selectedSQLOperatorData['dragType'] = "Table";
                                        operatorDoublClick = true;
                                        $('#flowchartworkSourcesspace').flowchart('setOperatorData', selectedOperatorId, selectedSQLOperatorData);
                                        $(".flowchart-operator-connector-label").hide();
                                        $(".flowchart-operator-title").hide();
                                        operatorDoublClick = false;
                                        addPropsToSQLOp($this, createTableObj, 'columnSQLMappingDialog');
                                    } else {
                                        showMesg("You must specify some columns to create the table.");
                                    }
                                } else {
                                    showMesg("Table Name Should not empty.");
                                }
                            }

                        }],
                    open: function () {
                        var source =
                                {
                                    dataType: "json",
                                    dataFields: [
                                        {name: 'CONNECTION_OBJ', type: 'string'},
                                        {name: 'TABLE_NAME', type: 'string'},
                                        {name: 'CONNECTION_NAME', type: 'string'},
                                        {name: 'LEVEL_TYPE', type: 'string'},
                                        {name: 'icon', type: 'string'},
                                        {name: 'ID', type: 'string'},
                                        {name: 'DATA_TYPE', type: 'string'},
                                        {name: 'COLUMN_TYPE', type: 'string'},
                                        {name: 'DATA_LENGTH', type: 'string'},
                                        {name: 'TO_COLUMN_TYPE', type: 'string'},
                                        {name: 'CONNECTION_TYPE', type: 'string'}
                                    ],
                                    hierarchy:
                                            {
                                                keyDataField: {name: 'TABLE_NAME'},
                                                parentDataField: {name: 'CONNECTION_NAME'}
                                            },
                                    id: 'ID',
                                    localData: response['fromTableColsArray']
                                };
                        var dataAdapter = new $.jqx.dataAdapter(source);
                        $("#columnSQLMappingTree").jqxTreeGrid(
                                {
                                    width: "100%",
                                    source: dataAdapter,
                                    height: 250,
                                    sortable: true,
                                    columnsResize: true,
                                    columnsReorder: true,
                                    enableHover: true,
                                    enableBrowserSelection: true,
                                    filterable: true,
                                    icons: true,
                                    hierarchicalCheckboxes: true,
                                    pageable: true,
                                    pagerMode: 'advanced',
                                    pagerPosition: 'bottom',
                                    pageSize: 100,
                                    pageSizeOptions: ['100', '200', '300'],
                                    theme: 'energyblue',
                                    selectionMode: 'multipleRows',
                                    autoShowLoadElement: false,
                                    columns: [
                                        {text: 'Table/Column Name', dataField: 'TABLE_NAME', width: '80%', filterable: true},
                                        {text: 'Column Data Type', dataField: 'COLUMN_TYPE', width: '40%', filterable: false},
                                        {text: 'Data Type', dataField: 'DATA_TYPE', hidden: true, width: '30%', filterable: false},
                                        {text: 'Column Length', dataField: 'DATA_LENGTH', hidden: true, width: '10%', filterable: false},
                                        {text: 'Database Type', dataField: 'CONNECTION_TYPE', width: '15%', filterable: true},
                                        {text: 'CONNECTION_NAME', dataField: 'CONNECTION_NAME', hidden: true, width: 160, filterable: false},
                                        {text: 'LEVEL_TYPE', dataField: 'LEVEL_TYPE', hidden: true, width: 160, filterable: false},
                                        {text: 'ID', dataField: 'ID', hidden: true, width: 160, filterable: false},
                                        {text: 'TO_COLUMN_TYPE', dataField: 'TO_COLUMN_TYPE', hidden: true, width: 160, filterable: false},
                                        {text: 'CONNECTION_OBJ', dataField: 'CONNECTION_OBJ', hidden: true, width: 160, filterable: false}
                                    ]
                                });
                        $("#sqlToTableTrDiv").html(response['trString']);//trString
                        var allDataTypeObj = response['allDataTypeObj'];
                        if (allDataTypeObj != null) {
                            $("#sqlAllDataTypeObj").val(JSON.stringify(allDataTypeObj));
                        }
                        var editing = false;
                        $(".editable").attr("title", "Double click here to rename the columns");
                        $(".dataTypePopup").attr("title", "Double click here to change datatype");

                        $('.dataTypePopup').on('dblclick', function () {
                            var val = $(this).text();
                            $(this).find('input').val(val).show().focus();
                            $(this).find('.originalvalue').hide();
                            // need to show the popup
                            dataTypeDialog(this);

                        });
                        $('.editable').on('dblclick', function () {
                            var val = $(this).text();
                            $(this).find('input').val(val).show().focus();
                            $(this).find('.originalvalue').hide();

                        });
                        $('#sqlToTableColumnsTable input').bind('keyup keypress', function (e) {
                            var code = e.keyCode || e.which;
                            if (editing)
                                return editing = false;
                            if (code == 27) {
                                editing = false;
                                $(this).parent().find('.originalvalue').show().focus();
                                $(this).parent().find('input[type="text"],select,textarea').hide();
                            }

                            if (code == 13) {
                                editing = false;

                                $(this).parent().find('.originalvalue').show().focus();
                                $(this).parent().find('input[type="text"],select,textarea').hide();
                            }
                        });

                        $('#sqlToTableColumnsTable input').on('blur', function (e) {
                            var nEl = $(this).val();
                            $(this).parent().find('.originalvalue').text(nEl).show();
                            $(this).parent().find('input[type="text"],select,textarea').hide();
                        });
                        $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                        $(".visionHeaderMain").css("z-index", "999");
                        $(".visionFooterMain").css("z-index", "999");
                        $(".ui-dialog").addClass('visionDMTreePopup');


                    },
                    resizeStop: function (event, ui) {
                        //   $("#columnSQLMappingTree").jqxTreeGrid( -- 250/350 
                        //.visionEtlCreateSQLTable tbody 156/350
                        var sizes = ui.size;
                        var height = ui.size.height;
                        var width = ui.size.width;
                        var treeHeight = parseInt(height) - 100;
                        var tableHeight = parseInt(height) - 194;
                        $("#columnSQLMappingTree").jqxTreeGrid({height: treeHeight + "px"});
                        $(".visionEtlCreateSQLTable tbody").height(tableHeight + "px");
                    },
                    beforeClose: function (event, ui)
                    {

                        $(".visionHeaderMain").css("z-index", "99999");
                        $(".visionFooterMain").css("z-index", "99999");

                    }, close: function (event, ui)
                    {

                        $(this).html("");
                        try {
                            $("#columnSQLMappingTree").jqxTreeGrid('destroy');
                            $("#columnSQLMappingTree").remove();
                            $("#treeSearchInputDiv").remove();

                        } catch (e) {
                        }
                    }
                });

            },
            error: function (e) {
                console.log(e);
                sessionTimeout(e);
                stopLoader();
            }
        });
    } else {
        stopLoader();
        showMesg("No mappings found.");
    }
}


function ShowEtlConnectionPopup(response, type, typeName, dialogTitle)
{
    var labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    $("#dialog").html(response);
    $("#dialog").dialog({
        title: (labelObject[dialogTitle] != null ? labelObject[dialogTitle] : dialogTitle),
//        modal: true,
        width: 500,
        height: 'auto',
        maxHeight: 450,
        fluid: true,
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

function connectEtlDatabase(type, typeName) {
//    var selectedDbType = $('#selectedTypeName').val();
//    var ConnectionEtlType = $('#selectedType').val();
    var EtlconName = $('#DbEtlConnectionName').val();
    if (EtlconName != null && EtlconName != '') {
        EtlconName = $.trim(EtlconName);
    }
    var EtlhostName = $('#DbEtlHostName').val();
    if (EtlhostName != null && EtlhostName != '') {
        EtlhostName = $.trim(EtlhostName);
    }
    var Etlport = $('#DbEtlPort').val();
    if (Etlport != null && Etlport != '') {
        Etlport = $.trim(Etlport);
    }
    var EtluserName = $('#DbEtlUserName').val();
    if (EtluserName != null && EtluserName != '') {
        EtluserName = $.trim(EtluserName);
    }
    var Etlpassword = $('#DbEtlPassword').val();
    if (Etlpassword != null && Etlpassword != '') {
        Etlpassword = $.trim(Etlpassword);
    }
    var EtlserviceName = $('#DbEtlServiceName').val();
    if (EtlserviceName != null && EtlserviceName != '') {
        EtlserviceName = $.trim(EtlserviceName);
    }
    // var EtlcheckedVal = $('#EtlcheckBoxChecked').is(':checked');
    var EtlauditId = $('#EtlauditId').val();
    if (EtlauditId != null && EtlauditId != '') {
        EtlauditId = $.trim(EtlauditId);
    }
    if (!EtlconName) {
        $('#DbEtlConnectionNameError').html("Please enter Connection Name");
    }
    if (!EtlhostName) {
        $('#DbEtlHostNameError').html("Please enter Host Name");
    }
    if (!Etlport) {
        $('#DbEtlPortError').html("Please enter Port No");
    }
    if (!EtluserName) {
        $('#DbEtlUserNameError').html("Please enter Username");
    }
    if (!Etlpassword) {
        $('#DbEtlPasswordError').html("Please enter Password");
    }
    if (!EtlserviceName) {
        $('#DbEtlServiceNameError').html("Please enter Service Name");
    } else {

        $('.dataMigrationInputError').hide();
        $('.visionDataMigrationError').hide();

        connectEtlDatabaseProcess(EtlconName, EtlhostName, Etlport, EtluserName, Etlpassword, EtlserviceName, typeName, EtlauditId, type);
    }
}

function connectEtlDatabaseProcess(EtlconName, EtlhostName, Etlport, EtluserName, Etlpassword, EtlserviceName, typeName, EtlauditId, type) {
    $('#visionShowConnectionMsg').hide();
    showLoader();
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'html',
        cache: false,
        url: 'getConnectionDetails',
        async: true,
        data: {
            connectionName: EtlconName,
            hostName: EtlhostName,
            port: Etlport,
            userName: EtluserName,
            password: Etlpassword,
            serviceName: EtlserviceName,
            checkedVal: true,
            selectedItemLabel: typeName,
            auditId: EtlauditId,
            ConnectionType: type,
            EtlFlag: 'Y'

        },
        success: function (response) {
            stopLoader();
            if (response != null) {
                var result = JSON.parse(response);
                var messageFlag = result['messageFlag'];
                if (messageFlag) {
                    $('#visionShowConnectionMsg').show();
                    $('#visionShowConnectionMsg').html('<p style="color:green">' + result['connectionMessage'] + '</p>');
                    $("#connectEtlDbTd").html("<input type='button' value='Close' name='Close'  onclick = \"closeEtlConnectionDialog()\" class='visionEtlCloseButton'>");
                    // showMessagePopup(result['connectionMessage']);
                } else {
                    // showMessagePopup(result['connectionMessage']);
                    $('#visionShowConnectionMsg').show();
                    $('#visionShowConnectionMsg').html('<p style="color:red">' + result['connectionMessage'] + '</p>');
                }
                // files expand ravi start
                var treeItemElement = globalFileTreeItem[fileType];
                if (treeItemElement != null) {
                    var $element = $(treeItemElement);
                    var children = $element.find('ul:first').children();
                    if (children != null && children.length > 0) {
                        $.each(children, function (index) {
                            $('#savedConnections').jqxTree('removeItem', this)
                        })
                        $('#savedConnections').jqxTree('addTo', {label: 'dummy', value: "ajax"}, treeItemElement);
                        $('#savedConnections').jqxTree('collapseItem', treeItemElement);
                        $('#savedConnections').jqxTree('expandItem', treeItemElement);
                    }
                }
//                var treeItemElement = globalFileTreeItem[typeName.toUpperCase()];
//                var $element = $(treeItemElement);
//                var children = $element.find('ul:first').children();
//                $.each(children, function (index) {
//                    $('#savedConnections').jqxTree('removeItem', this)
//                })
//                $('#savedConnections').jqxTree('addTo', {label: 'dummy', value: "ajax"}, treeItemElement);
//                $('#savedConnections').jqxTree('collapseItem', treeItemElement);
//                $('#savedConnections').jqxTree('expandItem', treeItemElement);
                // files expand ravi end 


            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });
}

function closeEtlConnectionDialog() {
    $("#dialog").html("");
    $("#dialog").dialog("close");
    $("#dialog").dialog("destroy");
}

function moveTableColumns(treeId, selectionType) {
    var selection = $("#" + treeId).jqxTreeGrid('getSelection');
    var tableStr = "";
    if (selection != null && selection.length != 0) {
        var rowCount = $('#sqlToTableColumnsTable >tbody >tr').length;
        for (var i = 0; i < selection.length; i++) {
            // get a selected row.
            var selectedRowData = selection[i];
            if (selectedRowData != null && !jQuery.isEmptyObject(selectedRowData)) {
                var connObjStr = selectedRowData['CONNECTION_OBJ'];//CONNECTION_OBJ
                var connObj = {};
                if (connObjStr != null && connObjStr != '') {
                    connObj = JSON.parse(connObjStr);
                }
                var toColumnTypeStr = selectedRowData['TO_COLUMN_TYPE'];//CONNECTION_OBJ
                var toColumnTypeObj = {};
                if (toColumnTypeStr != null && toColumnTypeStr != '') {
                    toColumnTypeObj = JSON.parse(toColumnTypeStr);
                }
                //conntype
                var toSysType = $("#sqlToDataSourceName").attr("data-tosystype");
                if (toSysType != null && toSysType != '') {
                    toSysType = toSysType.toUpperCase();
                }
                var isExistColumn = false;
                if (rowCount != 0) {
                    $("#sqlToTableColumnsTable tbody tr").each(function () {
                        var tdArray = this.cells;
                        if (tdArray != null && tdArray.length != 0) {
                            var columnName = $(tdArray[2]).find("input").val();
                            if (columnName != null && columnName != '' && columnName == selectedRowData['TABLE_NAME']) {
                                isExistColumn = true;
                                return false;
                            }

                        }
                    });
                }
                if (!isExistColumn) {
                    var toColumnType = "";
                    if (toColumnTypeObj != null
                            && !jQuery.isEmptyObject(toColumnTypeObj)
                            && toSysType != null && toSysType != '') {
                        toColumnType = toColumnTypeObj[toSysType];
                        if (!(toColumnType != null && toColumnType != '')) {
                            toColumnType = selectedRowData['COLUMN_TYPE'];
                        }
                    }
                    if (!(toColumnType != null && toColumnType != '')) {
                        toColumnType = selectedRowData['COLUMN_TYPE'];
                    }
                    if (!(toColumnType != null && toColumnType != '')) {
                        toColumnType = "";
                    }
                    var columnName = selectedRowData['TABLE_NAME'];
                    if (columnName != null && columnName != '' && columnName.indexOf(" ") > -1) {
                        columnName = columnName.replace(/\s/g, '_');
                    }
                    tableStr += "<tr>"
                            + "<td width='5%'><img src=\"images/Detele Red Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionTdETLIcons\""
                            + " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>"
                            + "</td>"
                            + "<td width='5%' class=\"sourceJoinColsTd\"><input class='visionColJoinMappingInput' type='checkbox'/></td>"
                            + "<td width='35%' class=\"sourceJoinColsTd editable\"><input class='visionColJoinMappingInput inputvalue' type='text' value='" + columnName + "'/><span class=\"originalvalue\">" + columnName + "</span></td>"
                            + "<td width='35%' class=\"sourceJoinColsTd dataTypePopup\"><input class='visionColJoinMappingInput inputvalue' type='text' value='" + toColumnType + "' /><span class=\"originalvalue\">" + toColumnType + "</span></td>"
                            + "<td width='5%' class=\"sourceJoinColsTd\" style='display:none' >N</td>"
                            + "</tr>";
                }

            }
        }
    }
    $("#sqlToTableColumnsTable tbody").append(tableStr);
    $(".inputvalue").hide();
    var editing = false;
    $(".editable").attr("title", "Double click here to rename the columns");
    $('.editable').on('dblclick', function () {
        var val = $(this).text();
        $(this).find('input').val(val).show().focus();
        $(this).find('.originalvalue').hide();

    });
    $(".dataTypePopup").attr("title", "Double click here to change datatype");
    $('.dataTypePopup').on('dblclick', function () {
        var val = $(this).text();
        $(this).find('input').val(val).show().focus();
        $(this).find('.originalvalue').hide();
        // need to show the popup
        dataTypeDialog(this);

    });
    $('#sqlToTableColumnsTable input').bind('keyup keypress', function (e) {
        var code = e.keyCode || e.which;
        if (editing)
            return editing = false;
        if (code == 27) {
            editing = false;
            $(this).parent().find('.originalvalue').show().focus();
            $(this).parent().find('input[type="text"],select,textarea').hide();
        }

        if (code == 13) {
            editing = false;

            $(this).parent().find('.originalvalue').show().focus();
            $(this).parent().find('input[type="text"],select,textarea').hide();
        }
    });

    $('#sqlToTableColumnsTable input').on('blur', function (e) {
        var nEl = $(this).val();
        $(this).parent().find('.originalvalue').text(nEl).show();
        $(this).parent().find('input[type="text"],select,textarea').hide();
    });

}

function addNewTableRow(fromTrStrDivId, tableId, $this) {
    var trstring = $("#" + fromTrStrDivId).html();
    $("#" + tableId + " tbody").append(trstring);
    $(".inputvalue").hide();
    var editing = false;
    $(".editable").attr("title", "Double click here to rename the columns");
    $(".sqlToTableColumnsDivClass").animate({
        scrollTop: $(".sqlToTableColumnsDivClass").prop("scrollHeight")
    }, 1000);

    $('.editable').on('dblclick', function () {
        var val = $(this).text();
        $(this).find('input').val(val).show().focus();
        $(this).find('.originalvalue').hide();

    });
    $(".dataTypePopup").attr("title", "Double click here to change datatype");

    $('.dataTypePopup').on('dblclick', function () {
        var val = $(this).text();
        $(this).find('input').val(val).show().focus();
        $(this).find('.originalvalue').hide();
        // need to show the popup
        dataTypeDialog(this);

    });
    $('#' + tableId + ' input').bind('keyup keypress', function (e) {
        var code = e.keyCode || e.which;
        if (editing)
            return editing = false;
        if (code == 27) {
            editing = false;
            $(this).parent().find('.originalvalue').show().focus();
            $(this).parent().find('input[type="text"],select,textarea').hide();
        }

        if (code == 13) {
            editing = false;

            $(this).parent().find('.originalvalue').show().focus();
            $(this).parent().find('input[type="text"],select,textarea').hide();
        }
    });

    $('#' + tableId + ' input').on('blur', function (e) {
        var nEl = $(this).val();
        $(this).parent().find('.originalvalue').text(nEl).show();
        $(this).parent().find('input[type="text"],select,textarea').hide();
    });
}
function addNewTableDataRow(fromTrStrDivId, tableId, $this) {
    var trstring = $("#" + fromTrStrDivId).html();
    $("#" + tableId + " tbody").append(trstring);

}

function showMesg(message) {
    if (message != null) {
        $("#dialog").html(message);
        $("#dialog").dialog({
            title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
            modal: true,
            height: 'auto',
            minWidth: 300,
            maxWidth: 300,
            fluid: true,
            buttons: [{
                    text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                    click: function () {
                        $(this).html("");
                        $(this).dialog("close");
                        $(this).dialog("destroy");
                    }}],
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
}

function addPropsToSQLOp($this, createTableObj, dialogId) {
    showLoader();

    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'json',
        cache: false,
        url: 'createTableInETL',
        async: true,
        data: {
            createTableObj: JSON.stringify(createTableObj)
        },
        success: function (response) {
            stopLoader();
            if (response != null) {
                $("#dialog").html(response['message']);
                $("#dialog").dialog({
                    title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
                    modal: true,
                    height: 'auto',
                    minWidth: 300,
                    maxWidth: 'auto',
                    fluid: true,
                    buttons: [{
                            text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                            click: function () {
                                if (response['messageFlag']) {
                                    $("#" + dialogId).html("");
                                    $("#" + dialogId).dialog("close");
                                    $("#" + dialogId).dialog("destroy");
                                }
                                $(this).html("");
                                $(this).dialog("close");
                                $(this).dialog("destroy");
                            }}],
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
        error: function (e)
        {
            sessionTimeout(e);
        }

    });

}

function connectErpEtlDatabase(type, typeName) {

    var connectionName = $('#ErpEtlDbConnectionName').val();
    if (connectionName != null && connectionName != '') {
        connectionName = $.trim(connectionName);

    }
    var ClientId = $('#ERPEtlClientName').val();
    if (ClientId != null && ClientId != '') {
        ClientId = $.trim(ClientId);

    }
    var hostName = $('#ERPEtlHostName').val();
    if (hostName != null && hostName != '') {
        hostName = $.trim(hostName);

    }
    var userName = $('#ERPEtlUserName').val();
    if (userName != null && userName != '') {
        userName = $.trim(userName);

    }
    var password = $('#ERPEtlPassword').val();
    if (password != null && password != '') {
        password = $.trim(password);

    }
    var LanguageId = $('#ERPEtlLanguageId').val();
    if (LanguageId != null && LanguageId != '') {
        LanguageId = $.trim(LanguageId);

    }
    var ERPSystemId = $('#ERPEtlSystemId').val();
    if (ERPSystemId != null && ERPSystemId != '') {
        ERPSystemId = $.trim(ERPSystemId);

    }
    var checkedVal = $('#checkBoxChecked').is(':checked');
    var auditId = $('#auditId').val();
    if (!ClientId) {
        $('#ERPEtlClientNameError').html("Please enter ClientId");
    }
    if (!hostName) {
        $('#ERPEtlHostNameError').html("Please enter Host Name");
    }
    if (!userName) {
        $('#ERPEtlUserNameError').html("Please enter Username");
    }
    if (!password) {
        $('#ERPEtlPasswordError').html("Please enter Password");
    }
    if (!LanguageId) {
        $('#ERPEtlLanguageIdError').html("Please enter LanguageId");
    }
    if (!ERPSystemId) {
        $('#ERPEtlSystemIdError').html("Please enter ERPSystemId");
    } else {

        $('.dataMigrationInputError').hide();
        $('.visionDataMigrationError').hide();
        connectErpEtlDatabaseProcess(connectionName, hostName, ClientId, userName, password, ERPSystemId, typeName, LanguageId, type);
    }
}
function connectErpEtlDatabaseProcess(connectionName, hostName, ClientId, userName, password, ERPSystemId, selectedDbType, LanguageId, ConnectionType) {
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'html',
        cache: false,
        url: 'getErpConnectionDetails',
        async: true,
        data: {
            connectionName: connectionName,
            ClientId: ClientId,
            hostName: hostName,
            userName: userName,
            password: password,
            LanguageId: LanguageId,
            ERPSystemId: ERPSystemId,
            checkedVal: true,
            selectedItemLabel: selectedDbType,
            EtlERPFlag: 'Y',
            ConnectionType: ConnectionType

        },
        success: function (response) {
            stopLoader();
            if (response != null) {
                var responseObj = JSON.parse(response);
                if (responseObj != null && responseObj.connectionFlag == 'Y') {
                    $('#visionShowErpEtlMsg').show();
                    $('#visionShowErpEtlMsg').html('<p style="color:green">' + responseObj['connectionMessage'] + '</p>');
                    $("#connectEtlDbTd").html("<input type='button' value='Close' name='Close'  onclick = \"closeEtlConnectionDialog()\" class='visionEtlCloseButton'>");
                    // showMessagePopup(result['connectionMessage']);
                } else {
                    // showMessagePopup(result['connectionMessage']);
                    $('#visionShowErpEtlMsg').show();
                    $('#visionShowErpEtlMsg').html('<p style="color:red">' + responseObj['connectionMessage'] + '</p>');
                }
                var treeItemElement = globalFileTreeItem[fileType];
                if (treeItemElement != null) {
                    var $element = $(treeItemElement);
                    var children = $element.find('ul:first').children();
                    if (children != null && children.length > 0) {
                        $.each(children, function (index) {
                            $('#savedConnections').jqxTree('removeItem', this)
                        })
                        $('#savedConnections').jqxTree('addTo', {label: 'dummy', value: "ajax"}, treeItemElement);
                        $('#savedConnections').jqxTree('collapseItem', treeItemElement);
                        $('#savedConnections').jqxTree('expandItem', treeItemElement);
                    }
                }
//                var treeItemElement = globalFileTreeItem[selectedDbType.toUpperCase()];
//                var $element = $(treeItemElement);
//                var children = $element.find('ul:first').children();
//                $.each(children, function (index) {
//                    $('#savedConnections').jqxTree('removeItem', this)
//                })
//                $('#savedConnections').jqxTree('addTo', {label: 'dummy', value: "ajax"}, treeItemElement);
//                $('#savedConnections').jqxTree('collapseItem', treeItemElement);
//                $('#savedConnections').jqxTree('expandItem', treeItemElement);
                // files expand ravi end 
            }
        },
        error: function (e)
        {
            sessionTimeout(e);
            stopLoader();
        }

    });
}
function  refreshLogFile() {
    var currentProcesslogDate = $("#currentProcesslogDate").val();
    var currentProcesslogIndex = $("#currentProcesslogIndex").val();
    var jobId = $("#currentJobId").val();
    stopLoader();

    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'html',
        cache: false,
        url: 'refreshProcessLog',
        async: true,
        data: {
            currentProcesslogDate: ((currentProcesslogDate != null && currentProcesslogDate != '') ? currentProcesslogDate : ""),
            currentProcesslogIndex: ((currentProcesslogIndex != null && currentProcesslogIndex != '') ? currentProcesslogIndex : ""),
            jobId: jobId
        },
        success: function (response) {
            if (response != null && response != '') {
                var resultObj = JSON.parse(response);
                if (resultObj != null && !jQuery.isEmptyObject(resultObj)) {
                    $("#currentProcesslogIndex").val(resultObj['currentProcesslogIndex']);
                    var logTxt = resultObj['logTxt'];
                    if (logTxt != null && logTxt != '') {
                        $("#processlogTable tbody").append(logTxt);
                        $("#currentProcesslogDate").val(resultObj['currentProcesslogDate']);
                        if (resultObj['processFlag'] == 'N') {
                            clearInterval(processLogInterval);
                        }
                    } else {

                    }
                } else {

                }
            } else {

            }
        },
        error: function (e)
        {
            sessionTimeout(e);
            stopLoader();
            if (processLogInterval != null) {
                clearInterval(processLogInterval);
            }
        }

    });
}
function openLogFile() {
    var jobId = $("#currentJobId").val();
    var processLogTable = "<div id='processLogDiv' class='LogTableMain'>"
            + "<div id=''> "
            + " <div class='logIconDiv'>"
            + "<img src=\"images/StopPocessJob.svg\" class=\"visionETLIcons\" title=\"Cancel Job Execution\" "
            + " style=\"width:15px;height: 15px;cursor:pointer;\""
            + " onclick='cancellCurrentJob()'/>"
            + "<img src=\"images/Refresh Icon.svg\" class=\"visionETLIcons\" title=\"Refresh log\" "
            + " style=\"width:15px;height: 15px;cursor:pointer;margin-left:5px;\""
            + " onclick='refreshLogFile()'/>"
            + "</div>"
            + "</div"
            + "<input type='hidden' id='currentProcesslogDate'/><input type='hidden' id='currentProcesslogIndex'/>"//currentProcesslogIndex
            + "<table id='processlogTable' class='logtable' style='width:100%'>"
            + "<thead>"
            + "<tr>"
            + "<th width='5%'></th>"
            + "<th width='25%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;'>Time Stamp</th>"
            + "<th width='70%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;'>Message</th>"
            + "</tr>"
            + "</thead>"
            + "<tbody>"
            + "</tbody>"
            + "</table>"
            + "</div>";
    $("#dialogLogFile").html(processLogTable);
    $("#dialogLogFile").dialog({
        title: (labelObject['Log File'] != null ? labelObject['Log File'] : 'Log File'),
        modal: true,
        width: 'auto',
        maxWidth: 500,
        height: 'auto',
        maxHeight: 500,
        fluid: true,
        buttons: [{
                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                click: function () {
                    if (processLogInterval != null) {
                        clearInterval(processLogInterval);
                    }
                    $(this).html("");
                    $(this).dialog("close");
                    $(this).dialog("destroy");
                }

            }],
        open: function () {
            processLogInterval = setInterval(function () {
                refreshLogFile() // this will run after every 1 seconds
            }, 2000);
//       processLogInterval = setTimeout(refreshLogFile, 1000);
            $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
            $(".visionHeaderMain").css("z-index", "999");
            $(".visionFooterMain").css("z-index", "999");
            $(".ui-dialog").addClass('visionDMTreePopup');
//                            $("#dataMigrationTabs").jqxTabs({width: "100%", height: "130px", position: 'top', theme: 'ui-redmond', reorder: true});
//
//                            $('#dataMigrationTabs').unbind('selected').on('selected', function (event) {
//                                $('#iconsdiv').attr('style', 'margin-top:4px !important');
//                            });
//                            $("#dataMigrationTabs").jqxTabs({width: "100%", height: "100%", position: 'top', theme: 'ui-redmond', reorder: true});

        },
        beforeClose: function (event, ui)
        {

            $(".visionHeaderMain").css("z-index", "99999");
            $(".visionFooterMain").css("z-index", "99999");

        }, close: function (event, ui)
        {
            if (processLogInterval != null) {
                clearInterval(processLogInterval);
            }
//                        $(this).html("");
//                        $(this).dialog("close");
//                        $(this).dialog("destroy");
        }
    });

}

function recursiveReplaceFont(queryString, replceStr, fontStr) {
    if (queryString != null && queryString != '' && queryString.indexOf(replceStr) > -1) {
        var quotetedStr = "'" + queryString.substring(queryString.indexOf(replceStr) + 1,
                queryString.indexOf(replceStr, queryString.indexOf(replceStr) + 1)) + "'";
        queryString = queryString.replace(quotetedStr, fontStr + quotetedStr + "</font>");
        if (queryString != null && queryString != '' && queryString.indexOf(replceStr) > -1) {
            queryString = recursiveReplaceFont(queryString, replceStr, fontStr);
        }
    }
}

function dataTypeDialog($this) {
    var allDataTypeObjStr = $("#sqlAllDataTypeObj").val();
    if (allDataTypeObjStr != null && allDataTypeObjStr != '') {
        var allDataTypeObj = JSON.parse(allDataTypeObjStr);
        if (allDataTypeObj != null && !jQuery.isEmptyObject(allDataTypeObj)) {
            var toSysType = $("#sqlToDataSourceName").attr("data-tosystype");
            if (toSysType != null && toSysType != '') {
                toSysType = toSysType.toUpperCase();
            }
            var allDataTypeStr = allDataTypeObj[toSysType];
            //dataTypeDialog
            $("#dataTypeDialog").html(allDataTypeStr);
            $("#dataTypeDialog").dialog({
                title: ((labelObject['Data Type'] != null ? labelObject['Data Type'] : 'Data Type') + "(" + toSysType + ")"),
                modal: true,
                height: 'auto',
                minWidth: 700,
                maxWidth: 1000,
                fluid: true,
                buttons: [{
                        text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                        click: function () {
                            var dataTypeList = $("#dataTypeList").val();
                            var dataTypeLength = $("#dataTypeLength").val();
                            var dataTypeByteChar = $("#dataTypeByteChar").val();
                            var dataTypePrecesion = $("#dataTypePrecesion").val();
                            var dataTypeScale = $("#dataTypeScale").val();
                            if (dataTypeList != null && dataTypeList != '') {
                                var dataType = dataTypeList;

                                if (dataTypeLength != null && dataTypeLength != '' && !$("#dataTypeLength").prop('disabled')) {
                                    dataType += "(" + dataTypeLength;
                                    if (dataTypeByteChar != null && dataTypeByteChar != '' && !$("#dataTypeByteChar").prop('disabled')) {
                                        dataType += " " + dataTypeByteChar;
                                    }
                                    dataType += ")";
                                } else if (dataTypePrecesion != null && dataTypePrecesion != '' && !$("#dataTypePrecesion").prop('disabled')) {
                                    dataType += "(" + dataTypePrecesion;
                                    if (dataTypeScale != null && dataTypeScale != '' && !$("#dataTypeScale").prop('disabled')) {
                                        dataType += " ," + dataTypeScale;
                                    }
                                    dataType += ")";
                                }
                                $($this).find('input').val(dataType);
                                $($this).find('span').html(dataType);
                                $(this).html("");
                                $(this).dialog("close");
                                $(this).dialog("destroy");
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
    }
}
function dataTypeChange($this, id) {
    var dataType = $("#" + id).val();
    if (dataType != null && dataType != '') {
        var sysType = $("#" + id).find(':selected').data('sys-type');
        var maxLen = $("#" + id).find(':selected').data('max-len');
        var lengthFlag = $("#" + id).find(':selected').data('length-flag');//visionDataTypeLength
        var byteCharFlag = $("#" + id).find(':selected').data('byte-char-flag');//visionDataTypeByteChar
        var precesionFlag = $("#" + id).find(':selected').data('precesion-flag');//visionDataTypePrecesion
        var scaleFlag = $("#" + id).find(':selected').data('scale-flag');//visionDataTypeScale
        if (lengthFlag == 'Y') {
            $(".visionDataTypeLength").removeAttr("disabled");
        } else {
            $(".visionDataTypeLength").attr("disabled", "disabled");
        }
        if (byteCharFlag == 'Y') {
            $(".visionDataTypeByteChar").removeAttr("disabled");
        } else {
            $(".visionDataTypeByteChar").attr("disabled", "disabled");
        }
        if (precesionFlag == 'Y') {
            $(".visionDataTypePrecesion").removeAttr("disabled");
        } else {
            $(".visionDataTypePrecesion").attr("disabled", "disabled");
        }
        if (scaleFlag == 'Y') {
            $(".visionDataTypeScale").removeAttr("disabled");
        } else {
            $(".visionDataTypeScale").attr("disabled", "disabled");
        }

    } else {
        $(".visionDataTypeLength").removeAttr("disabled");
        $(".visionDataTypeByteChar").removeAttr("disabled");
        $(".visionDataTypePrecesion").removeAttr("disabled");
        $(".visionDataTypeScale").removeAttr("disabled");
    }
}
function showSavedJobs(newJobId) {
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'fetchSavedJobs',
        cache: false,
        data: {},

        success: function (response) {
            ajaxStop();
            var availableJobshtmlStr = response['availableJobshtmlStr'];
//            var jobIdList = response['jobIdList'];
//            var jobDescrList = response['jobDescrList'];
//            if (jobIdList.length > 0) {
//                var availableJobshtmlStr = '<div id="avaialableJobsTree">\n\
//                                                <ul>\n\
//                                                    <li>Saved Jobs\n\
//                                                        <ul>';
//
//                var imgIconStr = '<img src="images/Process Icon-01.svg" class="visionETLIcons"  style="width:15px;height: 15px;cursor:pointer;"/>';
//                $.each(jobIdList, function (index) {
//                    availableJobshtmlStr += '<li><div id="' + jobIdList[index] + '" class="visionETLAvailableJobs" ondblclick="openSavedJob(event,\'' + jobIdList[index] + '\',\'' + jobDescrList[index] + '\')">' + imgIconStr + '<span style="cursor:pointer;">  ' + (jobDescrList[index]).toUpperCase() + '</span></div></li>'
//
//                });
//                availableJobshtmlStr += '</ul></li></ul></div>';
            if (availableJobshtmlStr != null && availableJobshtmlStr != '') {
                $("#availableJobsId").html(availableJobshtmlStr);
                $('#avaialableJobsTree').jqxTree({height: '98%', width: '98%'});
                $('#avaialableJobsTree').jqxTree('expandAll');
                var currentJobItem = $("#" + newJobId).parents("li:first").find("div:first")[0];
                $("#avaialableJobsTree").find(".selectJqxTreeItem").removeClass("selectJqxTreeItem");
                $(currentJobItem).addClass("selectJqxTreeItem");
                $('#avaialableJobsTree').jqxTree('expandAll');
            }

            // $(".jqx-tree-item").addClass("selectJqxTreeItem");


//                try{
//                    $('#avaialableJobsTree').jqxTree('destroy');
//                }catch(e){}


//            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });


}

function openSavedJob(event, jobId, jobDescr) {
    $("#currentJobId").val(jobId);
    $("#currentJobName").val(jobDescr);
    showLoader();
    var targetDiv = $(event.target).parents('li:first').find("div:first")[0];
    $("#avaialableJobsTree").find(".selectJqxTreeItem").removeClass("selectJqxTreeItem");
    $(targetDiv).addClass("selectJqxTreeItem");
    var data = {
        jobDescr: jobDescr,
        jobId: jobId

    };

    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'getSavedJobData',
        cache: false,
        data: data,

        success: function (response) {
            stopLoader();
            switchETLDesignTabs("li_designView", "contentSplitter"); // -----code start
            var flowChartData = JSON.parse(response['flowChartData']);

            $("#flowchartworkSourcesspace").remove();
            $("#feedHeader").append('<div  id="flowchartworkSourcesspace" class="flowchart-example-container"></div>');

            $('#flowchartworkSourcesspace').flowchart({
                linkWidth: 2,
                defaultSelectedLinkColor: '#000055',
                grid: 10,
                distanceFromArrow: 0,
                multipleLinksOnInput: true,
                multipleLinksOnOutput: true,
                defaultSelectedLinkColor: 'red',
                onOperatorContextMenu: function (operatorId) {
                    console.log("operatorId:::" + operatorId);
                    openOpeartorContextMenu(operatorId);
                    return true;
                },
            });
            $('#flowchartworkSourcesspace').flowchart({
//                                onOperatorSelect: function(operatorId) {
//                                return true;
//                                },
                onAfterChange: function (changeType) {

                    if (!operatorDoublClick) {
                        trfmRulesChanged = true;
                        console.error("trfmRulesChanged : " + trfmRulesChanged);
                        return true;
                    }
                }, onOperatorSelect: function (changeType) {
                    $(document).find("input").blur();
                    return true;

                },
                onLinkSelect: function (changeType) {
                    $(document).find("input").blur();
                    return true;
                }
            });

            var count = 0;
            var OperatorsData = {};
            var linksData = {};
            try {
                for (var key in flowChartData.operators) {

                    OperatorsData[count] = flowChartData.operators[key];
                    for (var i in flowChartData.links) {
                        if (parseInt(key) == flowChartData.links[i].fromOperator) {
                            flowChartData.links[i].fromOperator = count;
                        }
                        if (parseInt(key) == flowChartData.links[i].toOperator) {
                            flowChartData.links[i].toOperator = count;
                        }

                    }

                    count = count + 1;
                }
                for (var key in OperatorsData) {
                    $('#flowchartworkSourcesspace').flowchart('addOperator', OperatorsData[key]);
                }
            } catch (e) {
            }



            var count = 0;
            //   try {

            for (var key in flowChartData.links) {

                linksData[count] = flowChartData.links[key];
                count = count + 1;

            }
            for (var key in linksData) {
                $('#flowchartworkSourcesspace').flowchart('addLink', linksData[key]);
            }

            $(".flowchart-operator-connector-label").hide();
            $(".flowchart-operator-title").hide();
            $(".visionMapOperator").removeClass("visionOpLabelDiv");
            // $('#flowchartworkSourcesspace').flowchart('setData', flowChartData);
            $("#dataMigrationTabs").remove();
            $("#normalizeOptionsTabs").remove(); // ravi normalise
            $("#deNormalizeOptionsTabs").remove();
            var mapIcons = $(".visionMapOperator");
            $.each(mapIcons, function (index) {
                $(this).attr('job-id', jobId);
                $(this).attr('job-name', jobDescr);
            });

//$("[title='Update']").attr("onclick","updateJob('"+jobId+"','"+jobDescr+"')");

            $('#dataMigrationTabs :input').keyup(function (event) {
                if (event.target.nodeName == 'INPUT') {
                    var $this = $(event.target);
                    $this.attr("value", event.target.value);
                    $this.attr("title", event.target.value);
                    $this.val(event.target.value);
                }

            });
            previousOperatorId = null;
            prevTargetOperatorId = null;
            trfmRulesChanged = false;
            $("#emptyJobName").val(""); // ------------------code start emptyJob
            $("#emptyJobId").val("");

            $("#currentTrnsOpId").val(""); // ravi updated code changes
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });
}
function rightClickProcessJob(jobId) {
    showLoader();
    $("#currentJobId").val(jobId);
    $("#currentJobName").val("");
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        cache: false,
        url: "getJobTransformationRules",
        data: {
            jobId: jobId,
        },
        success: function (response) {
            showLoader();
            try {
                response = JSON.parse(response);
            } catch (e) {
            }

            var processJobData = JSON.parse(response['processJobDataObj']);
            processJobData['mappedData'] = response['mappedObjectData'];
            processJobData['jobId'] = jobId;
            $.ajax({
                type: 'post',
                traditional: true,
                dataType: 'html',
                cache: false,
                url: 'processETLData',
                async: true,
                data: processJobData,
                success: function (response) {
                    stopLoader();
                    try {
                        $("#currentJobId").val(response['jobId']);
                        $("#currentJobName").val(response['jobName']);
                    } catch (e) {
                    }
                    openLogFile();

                },
                error: function (e)
                {
                    sessionTimeout(e);
                    stopLoader();
                    if (processLogInterval != null) {
                        clearInterval(processLogInterval);
                    }
                }

            });

        }

    });
}

function deleteJob(jobId) {

    $("#dialog").html("Are you sure you want to delete the Job ??");
    $("#dialog").dialog({
        title: (labelObject['Confirmation'] != null ? labelObject['Confirmation'] : 'Confirmation'),
        modal: true,
        height: 'auto',
        minWidth: 300,
        maxWidth: 'auto',
        fluid: true,
        buttons: [{
                text: (labelObject['Yes'] != null ? labelObject['Yes'] : 'Yes'),
                click: function () {

                    $(this).html("");
                    $(this).dialog("close");
                    $(this).dialog("destroy");
                    showLoader();
                    $.ajax({
                        type: "post",
                        traditional: true,
                        dataType: 'html',
                        cache: false,
                        url: "deleteJob",
                        data: {
                            jobId: jobId,
                        },
                        success: function (response) {
                            stopLoader();
                            try {
                                response = JSON.parse(response);
                            } catch (e) {
                            }
                            var message = response['message'];

                            showMesg(message);
                            showSavedJobs();

                        },
                        error: function (e)
                        {
                            sessionTimeout(e);
                        }

                    });


                }},
            {
                text: (labelObject['No'] != null ? labelObject['No'] : 'No'),
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

function showFileNamePopup($this, fileType) {
    showLoader();
    var selectedOperatorId = $('#flowchartworkSourcesspace').flowchart('getSelectedOperatorId');
    var selectedFileOperatorData = $('#flowchartworkSourcesspace').flowchart('getOperatorData', selectedOperatorId);
    var getLinksTo = $('#flowchartworkSourcesspace').flowchart('getLinksTo', selectedOperatorId);
    var fileName = selectedFileOperatorData['userFileName'];
    if (!(fileName != null && fileName != '')) {
        fileName = '';
    }
    var fileNameDiv = "<div><input type='text' placeholder='Enter File name' id='targetFileName' value='" + fileName + "'/></div>";
    $("#dialog").html(fileNameDiv);
    $("#dialog").dialog({
        title: (labelObject['File Name'] != null ? labelObject['File Name'] : 'File Name'),
        modal: true,
        height: 'auto',
        minWidth: 300,
        maxWidth: 'auto',
        fluid: true,
        buttons: [{
                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                click: function () {
                    var targetFileName = $("#targetFileName").val();
                    if (targetFileName != null && targetFileName != '') {
                        fileType = "." + fileType;
                        if (targetFileName.toLowerCase().indexOf(fileType) == -1) {
                            targetFileName += fileType;
                        }
                        selectedFileOperatorData['userFileName'] = targetFileName;
                        operatorDoublClick = true;
                        $('#flowchartworkSourcesspace').flowchart('setOperatorData', selectedOperatorId, selectedFileOperatorData);
                        $(".flowchart-operator-connector-label").hide();
                        $(".flowchart-operator-title").hide();
                        operatorDoublClick = false;
                    }
                    $(this).html("");
                    $(this).dialog("close");
                    $(this).dialog("destroy");

                }}],
        open: function () {
            stopLoader();
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
function showMesgContinue(message, processFlag) {
    if (processFlag) {
        message = message + "  Press Ok to process Job"
    }
    if (message != null) {
        $("#dialog").html(message);
        $("#dialog").dialog({
            title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
            modal: true,
            height: 'auto',
            minWidth: 300,
            maxWidth: 'auto',
            fluid: true,
            buttons: [{
                    text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                    click: function () {
                        $(this).html("");
                        $(this).dialog("close");
                        $(this).dialog("destroy");
                        if (processFlag) {
                            processJob();
                        }

                    }}],
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
}


function askSaveJobConfirmation() {
    if (trfmRulesChanged) {
        $("#dialog").html(labelObject['Do you want to save job before processing'] != null ? labelObject['Do you want to save job before processing'] : 'Do you want to save job before processing');
        $("#dialog").dialog({
            title: (labelObject['Confirm'] != null ? labelObject['Confirm'] : 'Confirm'),
            modal: true,
            height: 'auto',
            minWidth: 300,
            maxWidth: 'auto',
            fluid: true,
            buttons: [{
                    text: (labelObject['Yes'] != null ? labelObject['Yes'] : 'Yes'),
                    click: function () {
                        $(this).html("");
                        $(this).dialog("close");
                        $(this).dialog("destroy");
                        trfmRulesChanged = true;
                        getAllMappedData("Y");

                    }},
                {
                    text: (labelObject['No'] != null ? labelObject['No'] : 'No'),
                    click: function () {
                        $(this).html("");
                        $(this).dialog("close");
                        $(this).dialog("destroy");

                        processJob();


                    }}],

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
    } else {
        processJob();
    }

}
function viewConnection() {
    var selectedItem = $("#savedConnections").jqxTree('getSelectedItem');
    var connectonName = selectedItem.value;
    var parentEventItem = selectedItem.parentElement.parentElement.parentElement;

    var selectedParentItem = $('#savedConnections').jqxTree('getItem', parentEventItem);
    var dBType = selectedParentItem['value'];

//    if (connectonName == "Current_V10") {
//        var connectionObj = savedDBData['Current_V10'];
//
//    }


    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'html',
        cache: false,
        url: 'getConnDetails',
        async: true,
        data: {
            connectonName: connectonName,
//            connectionObj: JSON.stringify(connectionObj)
        },
        success: function (response) {
            stopLoader();
            if (response != null) {
                //var responseObj = JSON.parse(response);
                $("#dialog1").html(response);
                $("#dialog1").dialog({
                    title: (labelObject['View Connection'] != null ? labelObject['View Connection'] : 'View Connection'),
                    modal: true,
                    height: 'auto',
                    minWidth: 500,
                    maxWidth: 'auto',
                    fluid: true,
                    buttons: [],
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
        error: function (e)
        {
            sessionTimeout(e);
        }

    });
}

function updateConnection(auditId, dbType) {
    var data = {}
    if (dbType == "DB" || dbType == "Oracle_ERP") {
        data = {
            connectionName: $("#connectionNameEtl").val(),
            hostName: $("#hostNameEtl").val(),
            port: $("#portEtl").val(),
            userName: $("#userNameEtl").val(),
            password: $("#passwordEtl").val(),
            serviceName: $("#serviceNameEtl").val(),
            serviceName: $("#serviceNameEtl").val(),
            dbType: dbType,
            auditId: auditId,
        }
    } else if (dbType == "ERP") {
        data = {
            connectionName: $("#erpConnectionNameEtl").val(),
            ClientId: $("#erpClientEtl").val(),
            hostName: $("#erpHostNameEtl").val(),
            userName: $("#erpUserNameEtl").val(),
            password: $("#erpPasswordEtl").val(),
            LanguageId: $("#erpLanguageIdEtl").val(),
            ERPSystemId: $("#erpSystemIdEtl").val(),
            dbType: dbType,
            auditId: auditId,
        }
    }
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'html',
        cache: false,
        url: 'updateConnDetails',
        async: true,
        data: data,
        success: function (response) {
            stopLoader();
            if (response != null) {
                //var responseObj = JSON.parse(response);
                showMesg(response);
            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });

}

function deleteFile() {
    var selectedItem = $("#savedConnections").jqxTree('getSelectedItem');
    var fileName = selectedItem.label;
    var filePath = selectedItem.value;
    $("#dialog").html(labelObject['Are you sure you want to delete file?'] != null ? labelObject['Are you sure you want to delete file?'] : 'Are you sure you want to delete file?');
    $("#dialog").dialog({
        title: (labelObject['Confirmation'] != null ? labelObject['Confirmation'] : 'Confirmation'),
        modal: true,
        height: 'auto',
        minWidth: 300,
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
                        url: 'deleteFile',
                        async: true,
                        data: {
                            fileName: fileName,
                            filePath: filePath
                        },
                        success: function (response) {
                            stopLoader();
                            if (response.indexOf("Deleted Succesfully") > 0) {
                                $("#savedConnections").jqxTree('removeItem', selectedItem);
                            }

                            if (response != null) {
                                //var responseObj = JSON.parse(response);
                                showMesg(response);
                            }


                        },
                        error: function (e)
                        {
                            sessionTimeout(e);
                        }

                    });

                }},

            {
                text: (labelObject['No'] != null ? labelObject['No'] : 'No'),
                click: function () {
                    $(this).html("");
                    $(this).dialog("close");
                    $(this).dialog("destroy");
                }}],
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


function deleteConnection(auditId) { // ravi updated code changes
    if (auditId == null) {
        var selectedItem = $("#savedConnections").jqxTree('getSelectedItem');
        var connectionName = selectedItem.label;
    }
    $("#dialog").html(labelObject['Are you sure you want to delete connection?'] != null ? labelObject['Are you sure you want to delete connection?'] : 'Are you sure you want to delete connection?');
    $("#dialog").dialog({
        title: (labelObject['Confirmation'] != null ? labelObject['Confirmation'] : 'Confirmation'),
        modal: true,
        height: 'auto',
        minWidth: 300,
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
                        dataType: 'JSON',
                        cache: false,
                        url: 'deleteDatabaseDetails',
                        async: true,
                        data: {
                            auditId: auditId,
                            connectionName: connectionName
                        },
                        success: function (response) {
                            stopLoader();
                            if (response != null) {
//                                var responseObj = JSON.parse(response);
                                showMesg(response['message']);
                                if (response['deleteSucessful'] == "Y") {
                                    $('#savedConnections').jqxTree("removeItem", selectedItem);
                                    $("#savedConnections").jqxTree("refresh");
                                }

                            }
                        },
                        error: function (e)
                        {
                            sessionTimeout(e);
                        }

                    });

                }},

            {
                text: (labelObject['No'] != null ? labelObject['No'] : 'No'),
                click: function () {
                    $(this).html("");
                    $(this).dialog("close");
                    $(this).dialog("destroy");
                }}],
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

function validateQuery() {
    showLoader();
    var selectQuery = $("#generatedTotalQuery").val();
    if (selectQuery != null && selectQuery != '') {
        $.ajax({
            type: "post",
            traditional: true,
            dataType: 'json',
            url: "validateSQLQuery",
            cache: false,
            data: {
                query: selectQuery
            },
            success: function (data, status, xhr) {
                stopLoader();
                if (data != null && !jQuery.isEmptyObject(data)) {
                    showMesg(data['message']);
                }
            },
            error: function (e) {
                console.log(e);
                sessionTimeout(e);
                stopLoader();
            }
        });

    } else {
        showMesg("Query Empty");
    }
}

function viewFunQuery(tableId, selectedRowData) {
    var selectedFunctionName = selectedRowData['FUN_NAME'];
    var functionFormObj = {};
    var funStr = "";
    functionFormObj['functionName'] = selectedFunctionName;

    var rowCount = $('#' + tableId + ' tr').length;
    var funLvlType = selectedRowData['FUN_LVL_TYPE'];//
    if (funLvlType == 'MULTI_COLUMNS') {
        funStr += "" + selectedFunctionName + "";
    } else {

        funStr += "" + selectedFunctionName + "(";
    }
    var loopCount = 0;
    $("#" + tableId + " tr").each(function () {
        var tdArray = this.cells;
        if (tdArray != null && tdArray.length != 0) {
            var funFormLabel = $(tdArray[0]).text();
            var funValue = $(tdArray[1]).find("input").val();
            var funValueId = $(tdArray[1]).find("input").attr("id");
            if (funValue != null && funValue != '') {
                funValue = $.trim(funValue);
            }
            if (funValueId != null && funValueId != '') {
                funValueId = $.trim(funValueId);
            }
            funStr += "'" + funValue + "'";
            functionFormObj[funValueId] = funValue;
            if (loopCount != parseInt(rowCount) - 1) {
                funStr += ",";
            }
            loopCount++;
        }
    });
    if (funLvlType == 'MULTI_COLUMNS') {
        funStr += " END ";
    } else {

        funStr += ")";
    }
    console.log("funStr::" + funStr);
}
function viewSQLEditor(connectionName) {

    var timeOut;
    var currentIndex = $('#editorViewDiv').attr("display")
    if (!$('#editorViewDiv').is(":visible")) {
        switchETLDesignTabs('li_SQLEditor', 'editorViewDiv');
        timeOut = 1100;
        showLoader();
    } else {
        timeOut = 1;
    }
    setTimeout(function () {


        var length = $('#editorViewDiv').jqxTabs('length');
        var title = connectionName + ".Editor-";
        var lastSeq = 1;
        if (length != null && length != '' && parseInt(length) > 0) {
            for (var i = 0; i < parseInt(length); i++) {
                var titleText = $('#editorViewDiv').jqxTabs('getTitleAt', i);
                if (titleText != null && titleText != '' && titleText.indexOf(connectionName) > -1) {
                    var editorStr = titleText.split("-")[1];
                    if (editorStr != null && editorStr != '' && $.trim(editorStr)) {
                        lastSeq = (parseInt(editorStr) + 1);

                    }
                }
            }
        }
        var divId = connectionName.replace(" ", "_") + "_Editor_" + lastSeq;

        title = connectionName + ".Editor-" + lastSeq;
        var divCode = ''
                + '<div id="' + divId + '" class="' + divId + '"></div>'
                + '<div id="' + divId + '_GRID_DIV" class="' + divId + '_GRID_DIV">'
                + '<div id="' + divId + '_GRID" class="' + divId + '_GRID"></div>'
                + '</div>';



        $('#editorViewDiv').jqxTabs('addLast', title, divCode);
        $("#" + divId).parent().attr("id", divId + "_splitter");
// $("."+divId).parent().attr("data-connction-name", connectionName);
// $("."+divId).remove();
        $("#" + divId).attr("data-connction-name", connectionName);
        $('#editorViewDiv').jqxTabs('select', parseInt(length));
        $('#editorViewDiv').jqxTabs('ensureVisible', -1);
        $('#' + divId + '_splitter').jqxSplitter({width: '100%', height: '100%',
            orientation: 'horizontal',
            panels: [{size: "50%", min: 50}, {min: 50, size: "50%"}]});
        var sqlMainEditor = ace.edit(divId, {
            mode: "ace/mode/sql",
// enableBasicAutocompletion: true, // the editor completes the statement when you hit Ctrl + Space
// showPrintMargin: true, // hides the vertical limiting strip
            fontSize: "100%", // ensures that the editor fits in the environment
            minLines: 5,
            maxLines: 20,
            wrap: true,
            autoScrollEditorIntoView: true
        });
// var sqlMainEditor = ace.edit(divId);
// sqlMainEditor.setOptions({
// enableBasicAutocompletion: true, // the editor completes the statement when you hit Ctrl + Space
// showPrintMargin: false, // hides the vertical limiting strip
// fontSize: "100%" // ensures that the editor fits in the environment
// });
// sqlMainEditor.getSession().setMode("ace/mode/sql");

        stopLoader();
    }, timeOut)
}

function executeEditorScripts(tabId) {
    var script = "";
    var divId = "";
    var tabIndex = $("#" + tabId).jqxTabs("val");
    var content = $("#" + tabId).jqxTabs('getContentAt', parseInt(tabIndex));
    if (content != null) {
        var spliterIdDiv = content['0'];
        if (spliterIdDiv != null) {
            var spliterId = spliterIdDiv.id;
            console.log(spliterId);
            if (spliterId != null && spliterId != '') {
                divId = spliterId.replace("_splitter", "");
            }
        } else {
            var spliterId = content.id;
            console.log(spliterId);
            if (spliterId != null && spliterId != '') {
                divId = spliterId.replace("_splitter", "");
            }

        }

    }

    var sqlMainEditor = sqlMainEditor = ace.edit(divId);
    var script = sqlMainEditor.getSelectedText();
    if (script == "") {
        script = String(sqlMainEditor.getSession().getValue());
    }

    console.log("data:::" + script);
    if (script != null
            && $.trim(script) != null
            && $.trim(script) != ''
            && $.trim(script) != 'null'
            && $.trim(script.replace(/[\t\n]+/g, ' ')) != null
            && $.trim(script.replace(/[\t\n]+/g, ' ')) != ''
            && $.trim(script.replace(/[\t\n]+/g, ' ')) != 'null'
            ) {
        var connectionName = $("#" + divId).attr("data-connction-name");
        $.ajax({
            type: "post",
            traditional: true,
            dataType: 'json',
            url: "executeSQLQuery",
            cache: false,
            data: {
                script: script,
                connectionName: connectionName
            },
            success: function (response, status, xhr) {
                stopLoader();
                if (response != null && !jQuery.isEmptyObject(response)) {
                    if (response['selectFlag']) {
                        showExecutionResults(script, connectionName, response, divId)
                    } else {
                        $("#dialog").html(response['message']);
                        $("#dialog").dialog({
                            title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
                            modal: true,
                            height: 'auto',
                            minWidth: 300,
                            maxWidth: 'auto',
                            fluid: true,
                            buttons: [{
                                    text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                                    click: function () {
                                        $(this).html("");
                                        $(this).dialog("close");
                                        $(this).dialog("destroy");
                                    }}],
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

                }
            },
            error: function (e) {
                console.log(e);
                sessionTimeout(e);
                stopLoader();
            }
        });
    } else {
        showMesg("No scripts/query to be run");
    }
}

function showExecutionResults(script, connectionName, resultObj, divId) {
    showLoader();
    var gridId = divId + "_GRID";
    try {
        $("#" + gridId).jqxGrid("destroy");
    } catch (e) {
    }
    $("#" + gridId).remove();
    $("#" + gridId + "_DIV").html("<div id='" + gridId + "'></div>");
    var gridObj = resultObj['gridObject'];
    var gridPropObj = {};
    gridPropObj = gridObj.gridProperties;
    gridPropObj.columns = gridObj.columns;
    var columns = gridObj.columns;
    var source =
            {
                type: 'POST',
//                                                async: false,
                datatype: "json",
                datafields: gridObj['datafields'],
                data: {
                    columnList: JSON.stringify(gridObj['columnList']),
                    script: script,
                    connectionName: connectionName
                },
                url: 'showExecutionQueryResults',
                cache: false,
                root: 'Rows',
                processdata: function (data) {

                },
                beforeSend: function () {
                    showLoader();

                }, loadError: function (xhr, status, error) {
                    stopLoader();
                }, loadComplete: function (data)
                {
                    stopLoader();
                    //$("#div_" + tableName).jqxGrid('hiderowdetails', 0);
                    //$("#row0div_" + tableName).hide();

                },
                beforeprocessing: function (data) {

                    if (data[0] != null) {
                        source.totalrecords = data[0].TotalRows;
                    } else {
                        source.totalrecords = 0;
                    }
                    stopLoader();
                },
                sort: function ()
                {
//                                                $("#" + gridResultObj['gridId'] + "_sort_columns").remove();
                    $("#" + gridId).jqxGrid('updatebounddata', 'sort');
                    try {
                        $("#" + gridId).jqxGrid('clearselection');
                    } catch (e) {
                    }
                    stopLoader();
                },
                filter: function () {

                    $("#" + gridId).jqxGrid('updatebounddata', 'filter');
                    try {
                        $("#" + gridId).jqxGrid('clearselection');
                    } catch (e) {
                    }
                    stopLoader();
                }


            };
    var dataAdapter = new $.jqx.dataAdapter(source);
//    gridPropObj.source = dataAdapter;
//    for (var i = 0; i < gridPropObj.columns.length; i++) {
//        if (gridPropObj.columns [i].cellsrenderer != null) {
//            gridPropObj.columns [i].cellsrenderer = eval(gridPropObj.columns [i].cellsrenderer);
//        }
//        if (gridPropObj.columns[i].rendered != null) {
//            gridPropObj.columns[i].rendered = eval('(' + gridPropObj.columns[i].rendered + ')');
//        }
//        if (gridPropObj.columns[i].createeditor != null) {
//            gridPropObj.columns[i].createeditor = eval('(' + gridPropObj.columns[i].createeditor + ')');
//        }
//        if (gridPropObj.columns[i].initeditor != null) {
//            gridPropObj.columns[i].initeditor = eval('(' + gridPropObj.columns[i].initeditor + ')');
//        }
//        if (gridPropObj.columns[i].geteditorvalue != null) {
//            gridPropObj.columns[i].geteditorvalue = eval('(' + gridPropObj.columns[i].geteditorvalue + ')');
//        }
//        if (gridPropObj.columns[i].cellvaluechanging != null) {
//            gridPropObj.columns[i].cellvaluechanging = eval('(' + gridPropObj.columns[i].cellvaluechanging + ')');
//        }
//        if (gridPropObj.columns[i].cellbeginedit != null) {
//            gridPropObj.columns[i].cellbeginedit = eval('(' + gridPropObj.columns[i].cellbeginedit + ')');
//        }
//    }
//    if (gridPropObj.rendergridrows != null && gridPropObj.rendergridrows != "") {
//        gridPropObj.rendergridrows = eval('(' + gridPropObj.rendergridrows + ')');
//    }

    $('#' + gridId).jqxGrid(
            {
                width: "100%",
                height: "90%",
                theme: 'energyblue',
                autoshowloadelement: false,
                source: dataAdapter,
                pageable: true,
                pagesize: 50,
                showfilterrow: true,
                filterable: true,
                sortable: true,
                virtualmode: true,
                pagesizeoptions: ['50', '100', '500', '1000', '5000', '10000', '50000'],
                rendergridrows: function (params) {
                    return params.data;
                },
                columnsresize: true,
                columns: columns
            });
    $('#' + gridId).on('celldoubleclick', function (event) {
        var args = event.args;
        var dataField = args.datafield;
        var dataField1 = args.text;
        var rowIndex = args.rowindex;
        var cellValue = args.value;
        var column = $('#' + gridId).jqxGrid('getcolumn', event.args.datafield).text;
        popupedit(column, cellValue);
    });
//    $('#' + gridId).jqxGrid(gridPropObj);

}

function createNewJob() {
    var response = "<div id='textReason'>";
    response += "<textarea id='reasonId' class='visionDeleteReason'></textarea></div>";
    response += "<div id='dailog_error_id' style='display:none;color:red'>" + (labelObject['Please give any Job Name'] != null ? labelObject['Please give new Job Name'] : 'Please give new Job Name') + "</div>";

    $("#dialog").html(response);
    $("#dialog").dialog({
        title: (labelObject['Job Name'] != null ? labelObject['Job Name'] : 'Job Name'),
        modal: true,
        height: 'auto',
        minWidth: 300,
        maxWidth: 'auto',
        fluid: true,
        buttons: [{
                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                click: function () {
                    var jobName = $("#reasonId").val();
                    if (jobName != null && jobName != '') {
                        $("#dailog_error_id").hide();
                        $(this).html("");
                        $(this).dialog("close");
                        $(this).dialog("destroy");
                        //saveJob(jobName, data, processJobData);
                        $.ajax({
                            type: 'post',
                            traditional: true,
                            dataType: 'html',
                            cache: false,
                            url: 'saveMappings',
                            async: true,
                            data: {
                                jobName: jobName,
                                emptyJob: "Y",
                            },
                            success: function (response) {
                                stopLoader();
                                try {
                                    response = JSON.parse(response);
                                } catch (e) {
                                }
                                if (response != null && response['message'].indexOf("Job updated succesfully") > -1) {
                                    $("#emptyJobName").val(jobName);
                                    $("#emptyJobId").val(response['jobId']);
                                    $("#currentJobName").val(jobName);
                                    $("#currentJobId").val(response['jobId']);
                                    showSavedJobs(response['jobId']);
                                    //var responseObj = JSON.parse(response);
                                    $("#flowchartworkSourcesspace").remove();
                                    $("#feedHeader").append('<div  id="flowchartworkSourcesspace" class="flowchart-example-container"></div>');

                                    $('#flowchartworkSourcesspace').flowchart({
                                        linkWidth: 2,
                                        defaultSelectedLinkColor: '#000055',
                                        grid: 10,
                                        distanceFromArrow: 0,
                                        multipleLinksOnInput: true,
                                        multipleLinksOnOutput: true,
                                        defaultSelectedLinkColor: 'red',
                                        onOpeartorContextMenu: function (operatorId) {
                                            console.log("operatorId:::" + operatorId);
                                            openOpeartorContextMenu(operatorId);
                                            return true;
                                        },
                                    });
                                    $('#flowchartworkSourcesspace').flowchart({
                                        //                                onOperatorSelect: function(operatorId) {
                                        //                                return true;
                                        //                                },
                                        onAfterChange: function (changeType) {

                                            if (!operatorDoublClick) {
                                                trfmRulesChanged = true;
                                                console.error("trfmRulesChanged : " + trfmRulesChanged);
                                                return true;
                                            }
                                        }, onOperatorSelect: function (changeType) {
                                            $(document).find("input").blur();
                                            return true;

                                        },
                                        onLinkSelect: function (changeType) {
                                            $(document).find("input").blur();
                                            return true;
                                        }
                                    });

                                    $("#dataMigrationTabs").remove();
                                    $("#normalizeOptionsTabs").remove();
                                    $("#deNormalizeOptionsTabs").remove();
                                    previousOperatorId = null;
                                    prevTargetOperatorId = null;
                                    trfmRulesChanged = false;



                                }
                            },
                            error: function (e)
                            {
                                sessionTimeout(e);
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

function copyJob(jobId) {

    $("#dialog").html(labelObject['Do you want Copy this Job?'] != null ? labelObject['Do you want Copy this Job?'] : 'Do you want Copy this Job?');
    $("#dialog").dialog({
        title: (labelObject['Confirm'] != null ? labelObject['Confirm'] : 'Confirm'),
        modal: true,
        height: 'auto',
        minWidth: 300,
        maxWidth: 'auto',
        fluid: true,
        buttons: [{
                text: (labelObject['Yes'] != null ? labelObject['Yes'] : 'Yes'),
                click: function () {
                    $(this).html("");
                    $(this).dialog("close");
                    $(this).dialog("destroy");

                    var response = "<div id='textReason'>";
                    response += "<textarea id='reasonId' class='visionDeleteReason'></textarea></div>";
                    response += "<div id='dailog_error_id' style='display:none;color:red'>" + (labelObject['Please give any Job Name'] != null ? labelObject['Please give new Job Name'] : 'Please give new Job Name') + "</div>";

                    $("#dialog").html(response);
                    $("#dialog").dialog({
                        title: (labelObject['Job Name'] != null ? labelObject['Job Name'] : 'Job Name'),
                        modal: true,
                        height: 'auto',
                        minWidth: 300,
                        maxWidth: 'auto',
                        fluid: true,
                        buttons: [{
                                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                                click: function () {
                                    var jobName = $("#reasonId").val();
                                    if (jobName != null && jobName != '') {
                                        $("#dailog_error_id").hide();
                                        $(this).html("");
                                        $(this).dialog("close");
                                        $(this).dialog("destroy");
                                        //saveJob(jobName, data, processJobData);
                                        copyProcessJob(jobId, jobName);

                                    } else {
                                        $("#dailog_error_id").show();
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



                }},
            {
                text: (labelObject['No'] != null ? labelObject['No'] : 'No'),
                click: function () {
                    $(this).html("");
                    $(this).dialog("close");
                    $(this).dialog("destroy");


                }}],

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

function copyProcessJob(jobId, jobName) {
    showLoader();
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'html',
        cache: false,
        url: 'copyJob',
        async: true,
        data: {
            jobName: jobName,
            jobId: jobId
        },
        success: function (response) {
            stopLoader();
            if (response != null) {
                showSavedJobs();
                //var responseObj = JSON.parse(response);
                $("#dialog").html(response);
                $("#dialog").dialog({
                    title: (labelObject["Message"] != null ? labelObject["Message"] : "Message"),
                    modal: true,
                    height: 'auto',
                    minWidth: 300,
                    maxWidth: 1000,
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
        error: function (e)
        {
            sessionTimeout(e);
            stopLoader();
        }

    });
}
function mapAllColumns(event, $this, toIconType) {
    var data = [];
    var dataStr = $("#fromTableColsArray_hidden").val();
    if (dataStr != null && dataStr != '') {
        data = JSON.parse(dataStr);
    }
    var toTableColsArrayStr = $("#toTableColsArray_hidden").val();
    var currentTrnsOpId = $("#currentTrnsOpId").val();
    $.ajax({
        datatype: "json",
        type: "post",
        traditional: true,
        url: 'allColMappingForm',
        cache: false,
        data: {
            toTableColsArrayStr: toTableColsArrayStr,
            fromTableColsArrayStr: dataStr,
            toIconType: toIconType
        },
        success: function (response, status, xhr) {
            stopLoader();
            $("#columnSQLMappingDialog").html(response['sqlPopupDiv']);
            $("#columnSQLMappingDialog").dialog({
                title: (labelObject['Column Mapping'] != null ? labelObject['Column Mapping'] : 'Column Mapping'),
                modal: true,
                width: 890,
                maxWidth: 1000,
                height: 350,
                maxHeight: 1000,
                fluid: true,
                buttons: [{
                        text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                        click: function () {
                            var length = $('#sqlToTableColumnsTable >tbody >tr').length;
                            if (length != null && parseInt(length) != 0) {
                                $('#sqlToTableColumnsTable >tbody >tr').each(function () {
                                    var tdArray = this.cells;
                                    if (tdArray != null && tdArray.length != 0) {
                                        var sourceColumn = $(tdArray[0]).find("input").val();
                                        var columnStr = "";
                                        var destinationColumn = $(tdArray[1]).find("input").val();
                                        if (sourceColumn != null && sourceColumn != ''
                                                && destinationColumn != null && destinationColumn != '') {
                                            columnStr = '<tr><td width="1%" class="visionColMappingImgTd1">'
                                                    + '<img src="images/Detele Red Icon.svg" onclick="deleteSelectedRow(this)" class="visionColMappingImg" title="Delete"'
                                                    + 'style="width:15px;height: 15px;cursor:pointer;"></td>'
                                                    + '<td width="19%" style=""><input class="visionColMappingInput" '
                                                    + 'type="text" value="' + destinationColumn + '" title="' + destinationColumn + '" ' + ((toIconType != null && toIconType != "" && toIconType != 'SQL') ? '' : 'readonly="true"') + ' style="width:84% !important">'
                                                    + '<img title="Select Column" src="images/tree_icon.svg" '
                                                    + 'class="visionETLColMapImage " onclick="selectColumn(this,\'' + ((toTableColsArrayStr != null && toTableColsArrayStr != "" && toTableColsArrayStr != "[]") ? 'toColumn' : 'fromColumn') + '\')" style=""></td>'
                                                    + '<td width="20%"><input class="visionColMappingInput" type="text" value="' + sourceColumn + '" title="' + sourceColumn + '" readonly="true" style="width:84% !important">'
                                                    + '<img title="Select Column" '
                                                    + 'src="images/tree_icon.svg" class="visionETLColMapImage " onclick="selectColumn(this,\'fromColumn\')" style="">'
                                                    + '</td>'
                                                    + '<td width="20%"><input class="visionColMappingTextarea" type="text" value=""></td>'
                                                    + '<td width="20%" style="display:none;">'
                                                    + '<input class="visionColMappingTextarea" type="text" value=""></td>'
                                                    + '<td width="20%"><input class="visionColMappingInput" '
                                                    + 'type="text" value="" style="width:84% !important">'
                                                    + '<img title="Select Function" src="images/Fx icon-01.svg" '
                                                    + 'class="visionETLColMapImage " onclick="selectColumnFun(this,\"fromColumn\")" style=""></td>'
                                                    + '</tr>';
                                        }
//                                        if (toIconType != null
//                                                && toIconType != ''
//                                                && toIconType != 'null'
//                                                && toIconType != 'SQL') {
//                                            if (sourceColumn != null && sourceColumn != '') {
//                                                columnStr = '<tr><td width="1%" class="visionColMappingImgTd1">'
//                                                        + '<img src="images/Detele Red Icon.svg" onclick="deleteSelectedRow(this)" class="visionColMappingImg" title="Delete"'
//                                                        + 'style="width:15px;height: 15px;cursor:pointer;"></td>'
//                                                        + '<td width="19%" style=""><input class="visionColMappingInput" '
//                                                        + 'type="text" value="' + sourceColumn + '" title="' + sourceColumn + '" style="width:84% !important">'
//                                                        + '<img title="Select Column" src="images/tree_icon.svg" '
//                                                        + 'class="visionETLColMapImage " onclick="selectColumn(this,\'fromColumn\')" style=""></td>'
//                                                        + '<td width="20%"><input class="visionColMappingInput" type="text" value="' + sourceColumn + '" title="' + sourceColumn + '" readonly="true" style="width:84% !important">'
//                                                        + '<img title="Select Column" '
//                                                        + 'src="images/tree_icon.svg" class="visionETLColMapImage " onclick="selectColumn(this,\'fromColumn\')" style="">'
//                                                        + '</td>'
//                                                        + '<td width="20%"><input class="visionColMappingTextarea" type="text" value=""></td>'
//                                                        + '<td width="20%" style="display:none;">'
//                                                        + '<input class="visionColMappingTextarea" type="text" value=""></td>'
//                                                        + '<td width="20%"><input class="visionColMappingInput" '
//                                                        + 'type="text" value="" style="width:84% !important">'
//                                                        + '<img title="Select Function" src="images/Fx icon-01.svg" '
//                                                        + 'class="visionETLColMapImage " onclick="selectColumnFun(this,\"fromColumn\")" style=""></td>'
//                                                        + '</tr>';
////                                                columnStr = '<tr><td width="1%" class="visionColMappingImgTd1">'
////                                                        + '<img src="images/Detele Red Icon.svg" onclick="deleteSelectedRow(this)" class="visionColMappingImg" title="Delete"'
////                                                        + 'style="width:15px;height: 15px;cursor:pointer;"></td>'
////                                                        + '<td width="19%" style=""><input class="visionColMappingInput" '
////                                                        + 'type="text" value="" title="" readonly="true" style="width:84% !important">'
////                                                        + '<img title="Select Column" src="images/tree_icon.svg" '
////                                                        + 'class="visionETLColMapImage " onclick="selectColumn(this,\"toColumn\")" style=""></td>'
////                                                        + '<td width="20%"><input class="visionColMappingInput" type="text" value="' + sourceColumn + '" title="' + sourceColumn + '" readonly="true" style="width:84% !important">'
////                                                        + '<img title="Select Column" '
////                                                        + 'src="images/tree_icon.svg" class="visionETLColMapImage " onclick="selectColumn(this,\"fromColumn\")" style="">'
////                                                        + '</td>'
////                                                        + '<td width="20%"><input class="visionColMappingTextarea" type="text" value=""></td>'
////                                                        + '<td width="20%" style="display:none;">'
////                                                        + '<input class="visionColMappingTextarea" type="text" value=""></td>'
////                                                        + '<td width="20%"><input class="visionColMappingInput" '
////                                                        + 'type="text" value="" style="width:84% !important">'
////                                                        + '<img title="Select Function" src="images/Fx icon-01.svg" '
////                                                        + 'class="visionETLColMapImage " onclick="selectColumnFun(this,\"fromColumn\")" style=""></td>'
////                                                        + '</tr>';
//                                            }
//                                        } else {
//                                            var destinationColumn = $(tdArray[1]).find("input").val();
//                                            if (sourceColumn != null && sourceColumn != ''
//                                                    && destinationColumn != null && destinationColumn != '') {
//                                                columnStr = '<tr><td width="1%" class="visionColMappingImgTd1">'
//                                                        + '<img src="images/Detele Red Icon.svg" onclick="deleteSelectedRow(this)" class="visionColMappingImg" title="Delete"'
//                                                        + 'style="width:15px;height: 15px;cursor:pointer;"></td>'
//                                                        + '<td width="19%" style=""><input class="visionColMappingInput" '
//                                                        + 'type="text" value="' + destinationColumn + '" title="' + destinationColumn + '" readonly="true" style="width:84% !important">'
//                                                        + '<img title="Select Column" src="images/tree_icon.svg" '
//                                                        + 'class="visionETLColMapImage " onclick="selectColumn(this,\'toColumn\')" style=""></td>'
//                                                        + '<td width="20%"><input class="visionColMappingInput" type="text" value="' + sourceColumn + '" title="' + sourceColumn + '" readonly="true" style="width:84% !important">'
//                                                        + '<img title="Select Column" '
//                                                        + 'src="images/tree_icon.svg" class="visionETLColMapImage " onclick="selectColumn(this,\'fromColumn\')" style="">'
//                                                        + '</td>'
//                                                        + '<td width="20%"><input class="visionColMappingTextarea" type="text" value=""></td>'
//                                                        + '<td width="20%" style="display:none;">'
//                                                        + '<input class="visionColMappingTextarea" type="text" value=""></td>'
//                                                        + '<td width="20%"><input class="visionColMappingInput" '
//                                                        + 'type="text" value="" style="width:84% !important">'
//                                                        + '<img title="Select Function" src="images/Fx icon-01.svg" '
//                                                        + 'class="visionETLColMapImage " onclick="selectColumnFun(this,\"fromColumn\")" style=""></td>'
//                                                        + '</tr>';
//                                            }
//
//                                        }
//                                        
                                        if (columnStr != null && columnStr != '') {
                                            $("#sourceDestColsTableId tbody").append(columnStr);
                                        }

                                        //sourceDestColsTableId
//                                     var tdOutPutArray =  $("#selectedColumnStr").html().find("tr").cells;


                                    }
                                });
                                $("#tabs-1").animate({
                                    scrollTop: $("#tabs-1").prop("scrollHeight")
                                }, 1000);
                            }
                            $(this).html("");
                            $(this).dialog("close");
                            $(this).dialog("destroy");
//                            $("#sourceDestColsTableId tbody").append(selectedColumnStr);
                        }


                    }],
                open: function () {
                    var source =
                            {
                                dataType: "json",
                                dataFields: [
                                    {name: 'id', type: 'string'},
                                    {name: 'text', type: 'string'},
                                    {name: 'value', type: 'string'},
                                    {name: 'icon', type: 'string'},
                                    {name: 'parentid', type: 'string'}
                                ],
                                hierarchy:
                                        {
                                            keyDataField: {name: 'id'},
                                            parentDataField: {name: 'parentid'}
                                        },
                                id: 'id',
                                localData: data
                            };
                    var dataAdapter = new $.jqx.dataAdapter(source);
                    $("#columnSQLMappingTree").jqxTreeGrid(
                            {
                                width: "100%",
                                source: dataAdapter,
                                height: 250,
                                sortable: true,
                                columnsResize: true,
                                columnsReorder: true,
                                enableHover: true,
                                enableBrowserSelection: true,
                                filterable: true,
                                icons: true,
                                hierarchicalCheckboxes: true,
                                pageable: true,
                                pagerMode: 'advanced',
                                pagerPosition: 'bottom',
                                pageSize: 100,
                                pageSizeOptions: ['100', '200', '300'],
                                theme: 'energyblue',
                                selectionMode: 'multipleRows',
                                autoShowLoadElement: false,
                                columns: [
                                    {text: 'Table/Column Name', dataField: 'text', width: '100%', filterable: true},
                                    {text: 'value', dataField: 'value', hidden: true, width: '30%', filterable: false},
                                    {text: 'id', dataField: 'id', hidden: true, width: 160, filterable: false},
                                    {text: 'parentid', dataField: 'parentid', hidden: true, width: 160, filterable: false}
                                ]
                            });

                    $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                    $(".visionHeaderMain").css("z-index", "999");
                    $(".visionFooterMain").css("z-index", "999");
                    $(".ui-dialog").addClass('visionDMTreePopup');


                },
                resizeStop: function (event, ui) {
                    //   $("#columnSQLMappingTree").jqxTreeGrid( -- 250/350 
                    //.visionEtlCreateSQLTable tbody 156/350
                    var sizes = ui.size;
                    var height = ui.size.height;
                    var width = ui.size.width;
                    var treeHeight = parseInt(height) - 100;
                    var tableHeight = parseInt(height) - 194;
                    $("#columnSQLMappingTree").jqxTreeGrid({height: treeHeight + "px"});
                    $(".visionEtlCreateSQLTable tbody").height(tableHeight + "px");
                },
                beforeClose: function (event, ui)
                {

                    $(".visionHeaderMain").css("z-index", "99999");
                    $(".visionFooterMain").css("z-index", "99999");

                }, close: function (event, ui)
                {

                    $(this).html("");
                    try {
                        $("#columnSQLMappingTree").jqxTreeGrid('destroy');
                        $("#columnSQLMappingTree").remove();
                        $("#treeSearchInputDiv").remove();

                    } catch (e) {
                    }
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
function mapTableColumns(treeId, selectionType) {
    var selection = [];
//    if (selectionType == 'selected') {
//        selection = $("#" + treeId).jqxTreeGrid('getSelection');
//    } else {
//        //selection = $("#" + treeId).jqxTreeGrid('getRows');
//    }
    var selectionArray = $("#" + treeId).jqxTreeGrid('getSelection');
    if (selectionArray != null && !jQuery.isEmptyObject(selectionArray)) {
        if ($.isArray(selectionArray)) {
            for (var i = 0; i < selectionArray.length; i++) {
                var selectionObj = selectionArray[i];

                if (selectionObj['level'] == 0) {//level
                    var allChildRecords = selectionObj['records'];
                    if (allChildRecords != null && allChildRecords.length != 0) {
                        allChildRecords = allChildRecords.map(function (item) {
                            delete item.parent;
                            return item;
                        });
                        Array.prototype.push.apply(selection, allChildRecords);
                    }
                } else {
                    delete selectionObj.parent;
                    selection.push(selectionObj);
                }

            }
//            console.log("selectionObj:::" + JSON.stringify(selection));
            selectionType = "selected";
        }
    }
    if (true) {
//    if (selection != null && selection.length != 0) {
        var toTableColsArrayStr = $("#toTableColsArray_hidden").val();
        var dataStr = $("#fromTableColsArray_hidden").val();
        $.ajax({
            datatype: "json",
            type: "post",
            traditional: true,
            url: 'mapTableColumns',
            cache: false,
            data: {
                selection: JSON.stringify(selection),
                toTableColsArrayStr: toTableColsArrayStr,
                fromTableColsArrayStr: dataStr,
                selectionType: selectionType
            },
            success: function (response, status, xhr) {
                stopLoader();
                if (response != null && !jQuery.isEmptyObject(response)) {
                    $('#sqlToTableColumnsTable >tbody >tr').remove();
//                $("#table_of_items tbody tr").remove(); 
                    $("#sqlToTableColumnsTable tbody").append(response['columnsStr']);
                }


            },
            error: function (e) {
                console.log(e);
                sessionTimeout(e);
                stopLoader();
            }
        });
    }

}
function connectEtlWSSOAP(type, typeName) {
//    var selectedDbType = $('#selectedTypeName').val();
//    var ConnectionEtlType = $('#selectedType').val();
    var wsdlURL = $('#wsdlURL').val();
    if (wsdlURL != null && wsdlURL != '') {
        wsdlURL = $.trim(wsdlURL);
    }
    var endPointURL = $('#endPointURL').val();
    if (endPointURL != null && endPointURL != '') {
        endPointURL = $.trim(endPointURL);
    }

    if (!wsdlURL) {
        $('#wsdlURLError').html("Please enter WSDL URL");
    }
    if (!endPointURL) {
        $('#endPointURLError').html("Please enter End Point URL");
    }
    if (wsdlURL && endPointURL) {
        $('.dataMigrationInputError').hide();
        $('.visionDataMigrationError').hide();
        importSAOPService();
    }


}
function importSAOPService() {
    showLoader();
    var wsdlURL = $('#wsdlURL').val();
    var endPointURL = $('#endPointURL').val();
    $.ajax({
        datatype: "json",
        type: "post",
        traditional: true,
        url: 'importSAOPService',
        cache: false,
        data: {
            wsdlURL: wsdlURL,
            endPointURL: endPointURL
        },
        success: function (response, status, xhr) {
            stopLoader();
            if (response != null && !jQuery.isEmptyObject(response)) {
                if (response['messageFlag']) {
                    $("#dialog").html(response['message']);
                    $("#dialog").dialog({
                        title: (labelObject['SOAP Request'] != null ? labelObject['SOAP Request'] : 'SOAP Request'),
                        modal: true,
                        width: 800,
                        height: 500,
                        maxHeight: 1000,
                        maxWidth: 1000,
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
                            $(this).html("");
//                            $(this).dialog("close");
//                            $(this).dialog("destroy");
                            $(".visionHeaderMain").css("z-index", "99999");
                            $(".visionFooterMain").css("z-index", "99999");
                        }
                    });
                    $('#soapSplitterDiv').jqxSplitter({width: '100%', height: '100%',
                        orientation: 'vertical',
                        panels: [{size: "50%", min: 50}, {min: 50, size: "50%"}]}
                    );
                    $('#soapRequestSpliterDiv').jqxSplitter({width: '100%', height: '100%',
                        orientation: 'horizontal',
                        panels: [{size: "80%", min: 50}, {min: 50, size: "20%"}]});
//                    $('#soapRequestInput').jqxEditor({
//                        height: "100%",
//                                tools: '',
//                        width: '100%'
//                    });
//                    $("#soapRequestInput").val(response['request']);
//                    $('#soapResponseInput').jqxEditor({
//                        height: "100%",
//                                tools: '',
//                        width: '100%'
//                    });
                } else {
                    $('#visionShowConnectionMsg').show();
                    $('#visionShowConnectionMsg').html('<p style="color:red">' + response['message'] + '</p>');

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
function viewFileData(fileDataObjStr) {
    if (fileDataObjStr != null && fileDataObjStr != '') {
        var fileDataObj = JSON.parse(fileDataObjStr);
        if (fileDataObj != null) {
            var filePath = fileDataObj['filePath'];
            var fileName = fileDataObj['fileName'];
            var fileType = fileDataObj['fileType'];
            var sheetNo = fileDataObj['sheetNo'];// ravi multiple excelSheets code

            var gridId = ("divGrid-" + filePath.replace(fileType, "").replace(".csv", "")).replace(/\s/g, '');
            gridId = gridId.replace(/\//g, '');



            if (sheetNo == null) {


                var contentDivId = ("main_divGrid-" + filePath.replace(fileType, "")).replace(/\s/g, '');
                var selectedItemIndex = $('#dataViewDiv').jqxTabs('selectedItem');

                if (selectedItemIndex != null) {
                    var html = $('#dataViewDiv').html();
                    if (html.indexOf(contentDivId) > -1
                            || html.indexOf('"' + contentDivId.replace(/\//g, '') + '"') > -1) {
                        var length = $('#dataViewDiv').jqxTabs('length');
                        for (var i = 0; i < length; i++) {
                            var content = $('#dataViewDiv').jqxTabs('getContentAt', i);
                            var id = $(content).attr("id");
                            if (id == null) {
                                id = $(content).children('div:nth-child(2)').attr("id");
                            }
                            if (id == contentDivId || id == contentDivId.replace(/\//g, '')) {
                                switchETLDesignTabs("li_contentView", "dataViewDiv");
                                $('#dataViewDiv').jqxTabs('select', i);
                                break;
                            }
                        }
                        if (sheetNo == null) { // ravi multiple excelSheets code
                            return false;
                        }
                        //  return false;
                    }

                }
            }
            var data = {
                filePath: filePath,
                fileName: fileName,
                fileType: fileType,
                targetFile: fileDataObj['targetFile'], // ravi etl issues new
                fileHeaders: JSON.stringify(fileDataObj['fileHeaders']), // ravi file headers
                sheetNo: sheetNo, // ravi multiple excelSheets code
                gridId: gridId // ravi multiple excelSheets code
            };
//            var gridId = ("divGrid-" + filePath.replace(fileType, "").replace(".csv", "")).replace(/\s/g, '');
//            gridId = gridId.replace(/\//g, '');
            //gridPersonalizeStr

            //data['gridId'] = gridId;
            $.ajax({
                type: "post",
                traditional: true,
                dataType: 'json',
                url: 'getFileObjectMetaData',
                cache: false,
                data: data,
                success: function (response) {
                    stopLoader();
                    if (response != null) {
                        // var responseObj = JSON.parse(response);
                        var dataFieldsArray = response['dataFieldsArray'];
                        var columnsArray = response['columnsArray'];
                        data['columnsArray'] = JSON.stringify(response['columnList']);
                        var personalizeDivStr = response['gridPersonalizeStr'];
                        var selectedItemIndex = $('#dataViewDiv').jqxTabs('selectedItem');


                        if (sheetNo == null) {
                            var divStr = "<div>"
                                    + "<div id='personalize_" + gridId + "'>" + personalizeDivStr + "</div>"
                                    + "<div id='main_" + gridId + "'>"
                                    + "<div id='" + gridId + "'></div>"
                                    + "</div>"
                                    + "</div>";

                            if (selectedItemIndex == null) {
                                //$("#designViewTab").jqxTabs('select', 1);
                                switchETLDesignTabs("li_contentView", "dataViewDiv");
                                $("#dataViewDiv").prepend("<ul></ul>");
                                $("#dataViewDiv ul").prepend("<li title='" + fileName + "'>" + fileName + "</li>");
                                $("#dataViewDiv").append(divStr);
                                $('#dataViewDiv').jqxTabs({width: "100%", height: "100%", position: 'top', theme: 'ui-redmond', reorder: true});

                                $('#dataViewDiv').jqxTabs('showAllCloseButtons');
                                $("#dataViewDiv").find("li.jqx-tabs-title").find("div.jqx-tabs-close-button").css("background-image", "url(images/close.png)");
                                $("#dataViewDiv").find("li.jqx-tabs-title-selected-top").find("div.jqx-tabs-close-button").css("background-image", "url(images/close_white.png)");

                            } else {
                                switchETLDesignTabs("li_contentView", "dataViewDiv");
                                $('#dataViewDiv').jqxTabs('addLast', fileName, divStr);

                            }
                        }
                        // ravi multiple excelSheets code
                        $('#personalize_' + gridId).html(personalizeDivStr);
                        $("#navBar_" + gridId).remove();
                        var navigationDiv = response['navigationDiv'];
                        if (navigationDiv != null && navigationDiv != "") {
                            $("#main_" + gridId).append(navigationDiv);
                            $("#navBar_" + gridId).jqxNavBar({height: 20, selectedItem: ((sheetNo != null) ? sheetNo : 0), theme: 'fresh'});
                        }

                        var source =
                                {
                                    type: 'POST',
//                                                async: false,
                                    datatype: "json",
                                    datafields: dataFieldsArray,
                                    data: data,
                                    url: 'getFileObjectData',
                                    cache: false,
                                    root: 'Rows',
                                    processdata: function (data) {


                                    },
                                    beforeSend: function () {
                                        showLoader();

                                    }, loadError: function (xhr, status, error) {
                                        $('#dataViewDiv').css("width", "100%");
                                        stopLoader();
                                    }, loadComplete: function (data)
                                    {
                                        $('#dataViewDiv').css("width", "100%");
                                        $("#navBar_" + gridId).show();
                                        //$("#div_" + tableName).jqxGrid('hiderowdetails', 0);
                                        //$("#row0div_" + tableName).hide();
                                        stopLoader();
                                    },
                                    beforeprocessing: function (data) {
                                        if (data != null && data[0] != null) {
                                            source.totalrecords = data[0].totalrecords;
                                        } else {
                                            source.totalrecords = 0;
                                        }

                                        stopLoader();
                                    },
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
//                var source =
//                        {
//                            localdata: dataArray,
//                            datatype: "array",
//                            datafields: dataFieldsArray
//                        };
                        var dataAdapter = new $.jqx.dataAdapter(source);
                        var headerTooltipRenderer = function (element) {
                            $(element).parent().jqxTooltip({position: 'mouse', theme: 'energyblue',
                                position: 'bottom-right',
                                showArrow: false, content: $(element).text()});
                        }
                        window.allGridColumns[gridId] = columnsArray;
                        var tabHeight = $("#" + gridId).closest(".jqx-tabs-content-element").height();
                        $("#" + gridId).jqxGrid(
                                {
                                    width: "100%",
                                    height: parseInt(tabHeight) * 0.84,
                                    theme: 'energyblue',
                                    autoshowloadelement: false,
                                    source: dataAdapter,
                                    pageable: true,
                                    pagesize: 50,
                                    showfilterrow: false,
                                    filterable: false,
                                    sortable: false,
                                    virtualmode: true,
                                    pagesizeoptions: ['50', '100', '500', '1000', '5000', '10000', '50000'],
                                    enabletooltips: true,
                                    enablemousewheel: true,
                                    enablehover: true,
                                    enablebrowserselection: true,
                                    selectionmode: 'checkbox',
                                    rendergridrows: function (params) {
                                        return params.data;
                                    },
                                    columnsresize: true,
                                    columns: columnsArray
                                });


                        $('#' + gridId).on('celldoubleclick', function (event) {
                            var args = event.args;
                            var dataField = args.datafield;
                            var dataField1 = args.text;
                            var rowIndex = args.rowindex;
                            var cellValue = args.value;
                            var column = $('#' + gridId).jqxGrid('getcolumn', event.args.datafield).text;
                            popupedit(column, cellValue);
                        });


                        $('#dataViewDiv').unbind('selected').on('selected', function (event) {
                            var $thid = this;

                            $('#dataViewDiv').jqxTabs('getContentAt', i);

                            $("#dataViewDiv").find("li.jqx-tabs-title").find("div.jqx-tabs-close-button").css("background-image", "url(images/close.png)");
                            $("#dataViewDiv").find("li.jqx-tabs-title-selected-top").find("div.jqx-tabs-close-button").css("background-image", "url(images/close_white.png)");

                        });

                        $('#dataViewDiv').unbind('add').on('add', function (event) {

                            $("#dataViewDiv").find("li.jqx-tabs-title").find("div.jqx-tabs-close-button").css("background-image", "url(images/close.png)");
//                    $("#dataViewDiv").find("li.jqx-tabs-title-selected-top").find("div.jqx-tabs-close-button").css("background-image", "url(images/close_white.png)");
                            setTimeout(function () {
                                $("#dataViewDiv").find("li.jqx-tabs-title-selected-top").find("div.jqx-tabs-close-button").css("background-image", "url(images/close_white.png)");

                            }, 100);
                            var selectedTabTitle = $("#dataViewDiv").jqxTabs("getTitleAt", event.args.item);
                            $("#dataViewDiv").jqxTabs('setTitleAt', event.args.item, selectedTabTitle);
//                            $("#dataViewDiv").jqxTabs('setTitleAt', event.args.item, selectedTabTitle.split(".")[1]);
                            var selectedTabLi = $("#dataViewDiv").find("li.jqx-tabs-title-selected-top");
                            selectedTabLi.attr("title", selectedTabTitle);


                        });

                        $("#" + gridId + "_personalizeid").toggleClass("ui-icon-triangle-1-s");
                        $(document).mouseup(function (e)
                        {
                            var container = $("#" + gridId + "_settings_panel");
                            var container1 = $(".personaliseoption");
                            if ((!container.is(e.target)
                                    && container.has(e.target).length === 0)
                                    && (!container1.is(e.target)
                                            && container1.has(e.target).length === 0)
                                    )
                            {
                                $(container).hide();
                                $("#" + gridId + "_personalizeid").toggleClass("ui-icon-triangle-1-s");
                            }
                        });

                        // ravi excel sheet navigation

                        $("#navBar_" + gridId).on('change', function () {

                            var index = $("#navBar_" + gridId).jqxNavBar('getSelectedIndex');

                            if (fileDataObj['sheetNo'] != index) {
                                fileDataObj['sheetNo'] = index;
                                $('#' + gridId).remove();
                                $('#personalize_' + gridId).empty();
                                $('#main_' + gridId).append("<div id=" + gridId + "></div>");
                                var fileDataObjStr = JSON.stringify(fileDataObj);
                                viewFileData(fileDataObjStr)
                                $("#navBar_" + gridId).hide();
                            }

                        });

                    }
                },
                error: function (e)
                {
                    sessionTimeout(e);
                }

            });
        }
    }

}
function uploadColumnMap(event, $this, browseId) {


//     var csrfToken = $("input[name='_csrf']").val();
//        if (csrfToken != null && csrfToken != '') {
//            params['_csrf'] = $("input[name='_csrf']").val();
//        }
    $("#" + browseId).ajaxfileupload({
        'action': 'uploadColumnMap',
        valid_extensions: ['xls', 'xlsx', 'XLS', 'XLSX'],
//        headers: {"X-CSRF-TOKEN": $("input[name='_csrf']").val()},
//        'action': 'importFile?gridId=' + gridId + '&tableName=' + tableName,
//        params: params,
//        'action': 'importFile?gridId=' + gridId,
        async: true,
        onStart: function () {
            $("#Loader").css("opacity", "0.99");
            $("#Loader").css("display", "block");
            $("body").css("pointer-events", "none");
            showLoader();
        },
        'onComplete': function (result) {
            $("#importColMapFile").remove();
            $("#colMappinAddIconDiv").append('<input name="importColMapFile" id="importColMapFile" type="file" style="display:none" data-clicked="Y">');
//            ajaxStop();//30
            $("#Loader").css("display", "none");
            $("body").css("pointer-events", "auto");
            stopLoader();
            if (result != null) {
                var selectedColumnStr = result.message;
                getUploadedColMappings(result.message);
            }

        }
        , onCancel: function () {
            $("#importColMapFile").remove();
            $("#colMappinAddIconDiv").append('<input name="importColMapFile" id="importColMapFile" type="file" style="display:none" data-clicked="Y">');
            $("#Loader").css("display", "none");
            $("body").css("pointer-events", "auto");
            stopLoader();
//            ajaxStop();//29
        }
        //catch()}
    });

    $("#" + browseId).attr('data-clicked', 'Y');
    $("#" + browseId).click();

}
function getUploadedColMappings(filePath) {
    //getUploadedColMappings

    showLoader();
    $.ajax({
        datatype: "json",
        type: "post",
        traditional: true,
        url: 'getUploadedColMappings',
        cache: false,
        data: {
            filePath: filePath,
            toIconType: ""
        },
        success: function (response, status, xhr) {
            stopLoader();
            if (response != null && !jQuery.isEmptyObject(response)) {
                var selectedColumnStr = response['message'];
                $("#sourceDestColsTableId tbody").append(selectedColumnStr);
                $("#tabs-1").animate({
                    scrollTop: $("#tabs-1").prop("scrollHeight")
                }, 1000);

            }


        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
}

function selectSapTableColumns(dataObj) {

    showLoader();
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'selectSapTableColumns',
        cache: false,
        data: dataObj,

        success: function (response) {
            stopLoader();
            var columns = response.columnsArray;
            var tableStr = '<div class ="visionTablesComboBox">'
            tableStr += '<div class="visionTablesSearchDiv">';
            tableStr += '<input id="searchColumns" class="visionTablesSearchInput" type="text" placeholder="Search Columns.." onkeyup="searchColumns(\'' + dataObj.tableName + '\',\'' + dataObj.DBValue + '\')">'
            tableStr += '<img src="images/crossicon.png" title="Clear Data" onclick="clearColumnInput(\'' + dataObj.tableName + '\',\'' + dataObj.DBValue + '\')" class="visionClearFieldBtn">';
            tableStr += '</div>';


            tableStr += '<div class="visionTablesComboBoxInner" style="display: flex;">'; // ravi sap columns code start

            tableStr += '<div id="sourceFields" class="sourceFields" style="margin-top: 30px;height:272px; width:47.5%;">';
            tableStr += '<div  class="visionTableName" style="position: relative;"><input id="selectAllCheckBoxes"  type="checkbox" > Select All </div>'; // ---- ravi edit for checkboxes

            for (var i = 0; i < columns.length; i++) {
                tableStr += '<div id="' + columns[i] + '" class="visionTableName" title="' + columns[i] + '"  style="position: relative;"><input class="visionTableNameChbx" type="checkbox" > ' + columns[i] + '<img src="images/crossicon.png" title="Clear Data" onclick="moveColumnToSource(' + columns[i] + ')" class="visionCloseDestTableBtn"></div>';
            }

            tableStr += '</div>';

            tableStr += '<div class="visionNavigateButtonDiv" style="width:5%;margin-top: 162px;"><input onclick="moveSapTableColumns()" type="button" value=">" class="sqlMoveButtons"/></div>';


            tableStr += '<div id="destinationFields" class="destinationFields" style="width:47.5%; margin-left: 0px;"></div>';
            tableStr += '</div>';


            $("#logoutDailog").html(tableStr);
            $("#logoutDailog").dialog({
                title: (labelObject['Select Columns'] != null ? labelObject['Select Columns'] : 'Select Columns'),
                modal: true,
                height: 'auto',
                minWidth: '650',
                maxWidth: 'auto',
                fluid: true,
                buttons: [{
                        text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                        click: function () {

                            var selectedColumnsArray = [];
                            var checkedColumns = $("#destinationFields div");
                            if (checkedColumns.length == 0) {
                                showMesg("Destination Columns Empty!");
                                return false;
                            }
                            $.each(checkedColumns, function (index) {
                                var value = this.textContent.trim();// ravi sap table issue
                                selectedColumnsArray.push(value);
                            });
//                            if (selectedColumnsArray.length > 50) {
//                                showMesg("Max Select Columns limit exceeds.Please select less than 50 Columns");
//                                return false;
//                            }

                            dataObj['selectSapTableColumns'] = JSON.stringify(selectedColumnsArray);
                            viewTableDataGrid(dataObj);

                            $(this).html("");
                            $(this).dialog("close");
                            $(this).dialog("destroy");

                        }}],
                open: function () {
                    $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                    $(".visionHeaderMain").css("z-index", "999");
                    $(".visionFooterMain").css("z-index", "999");



//                        $(".visionTableNameChbx").change(function(){
//                            if ($(this).prop("checked")){
//                               $(this).parent('div').addClass("multipleSelect");
//                            }
//                        });

                    $("#selectAllCheckBoxes").click(function () {
//                      $(".visionTableNameChbx").prop('checked', $(this).prop('checked'));



                        if ($(this).prop("checked")) {
                            $(".visionTableNameChbx").each(function (i) {
                                if (true) {
//                                if (i < 50) {
                                    $(this).prop('checked', true)
                                    $(this).parent('div').addClass("multipleSelect");
                                } else {
                                    $(this).prop('checked', false)
                                    $(this).parent('div').removeClass("multipleSelect");
                                }

                            });

                        }
                        if (!$(this).prop("checked")) {
                            $(".visionTableNameChbx").prop('checked', $(this).prop('checked'));
                            $(".visionTableNameChbx").parent('div').removeClass("multipleSelect");
                        }

                    });

                    $(".visionTableNameChbx").change(function () {

                        var selectedLength = $(".sourceFields").find(".multipleSelect").length;
                        if (selectedLength < 50) {
                            $("#selectAllCheckBoxes").prop("checked", false);
                            if (!$(this).prop("checked")) {
                                $(this).parent('div').removeClass("multipleSelect");
                            }
                            if ($(this).prop("checked")) {
                                $(this).parent('div').addClass("multipleSelect");
                            }
                        } else {

                            if (!$(this).prop("checked")) {

                                $(this).prop("checked", false);
                                $(this).parent('div').removeClass("multipleSelect");
                                $("#selectAllCheckBoxes").prop("checked", false);
                            }
                            if ($(this).prop("checked")) {
                                showMesg("Cannot select more than 50");
                                $(this).prop("checked", false);
                                $(this).parent('div').removeClass("multipleSelect");
                            }

                        }

                    });

                    dragColumns();



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
            sessionTimeout(e);
        }
    })
}
function refreshTableGrid(gridId) {

    try {
        showLoader();
        $("#" + gridId).jqxGrid('clearfilters');
        $('#' + gridId).jqxGrid('updatebounddata');
//      ss$("#" + gridId).jqxGrid('updatebounddata', 'cells');
        $("#" + gridId).jqxGrid('clearselection');
        stopLoader();
    } catch (e) {
        stopLoader();
    }
}
function viewTableDataGrid(data) {
    showLoader();
    var DBValue = data.DBValue;
    var tableName = data.tableName;
    //    var gridId = ("divGrid" + DBValue + "-" + tableName).replace(/\s/g, '');
    var gridId = ("divGrid-" + DBValue + "-" + tableName).replace(/\s/g, ''); // ravi updated code changes
    gridId = gridId.replace(/\//g, '');
    data['gridId'] = gridId;
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'getSchemaObjectMetaData',
        cache: false,
        data: data,

        success: function (response) {

            if (response != null) {
                // var responseObj = JSON.parse(response);
                var dataArray = response['dataArray'];
                var dataFieldsArray = response['dataFieldsArray'];
                var columnsArray = response['columnsArray'];
                data['columnsArray'] = JSON.stringify(response['columnList']);
                var totalCount = response['totalCount'];
                var personalizeDivStr = response['gridPersonalizeStr'];

                var selectedItemIndex = $('#dataViewDiv').jqxTabs('selectedItem');

                if (selectedItemIndex == null) {
                    //$("#designViewTab").jqxTabs('select', 1);
                    switchETLDesignTabs("li_contentView", "dataViewDiv");
                    $("#dataViewDiv").prepend("<ul></ul>");
                    $("#dataViewDiv ul").prepend("<li title='" + DBValue + "." + tableName + "'>" + DBValue + "." + tableName + "</li>");
                    var divStr = "<div>";
                    divStr += "<div style='display: flex;float:left;margin-top:5px;'>";
                    divStr += "<img src='images/refresh_icon.png' style='width:18px;height: 18px;cursor:pointer;padding-left:3px;margin-right:5px;' onclick=refreshTableGrid('" + gridId + "') title='Refresh'>"
                    divStr += "<div id='exportDropdown" + gridId.trim() + "' class='exportDropdown visionSearchExport visionSearchExportDiv' style='vertical-align: bottom; display: flex; padding-top: 8px;margin-top:-8px;'><table style='/*vertical-align: bottom;*/ display: inline-block;width: 40%;' class='visionSearchExportTable'><tbody><tr><td><select id='export" + gridId.trim() + "' onchange=getImportType('" + gridId.trim() + "')><option value=''>Select</option><option value='Xlsx'>Xlsx</option><option value='CSV'>CSV</option><option value='XML'>XML</option></select></td></tr></tbody></table> ";
                    divStr += "<input title='Export'  id='excelExport" + gridId.trim() + "' onclick=finalExport('" + gridId.trim() + "') class='exportClass visionSearchExportButton visionGridExportButton' type='button' disabled=''>";
                    divStr += "<input title='Import'  id='excelImport" + gridId.trim() + "' onclick=finalImport('" + gridId.trim() + "') class='importClass visionSearchImportButton visionGridImportButton' type='button' disabled='' style='margin-top: 5px;margin-left:5px;'>";
                    divStr += "</div>";
                    divStr += "</div>";
                    divStr += personalizeDivStr;
                    divStr += "<div id='" + gridId + "'></div>";


                    $("#dataViewDiv").append(divStr);
//                    $("#dataViewDiv").append("<div><div><img src='images/refresh_icon.png' style='width:18px;height: 18px;cursor:pointer;padding-left:3px;' onclick=refreshTableGrid('" + gridId + "') title='Refresh'></div><div id='" + gridId + "'></div>"); // ravi edit for tabs navigation
                    $('#dataViewDiv').jqxTabs({width: "100%", height: "100%", position: 'top', theme: 'ui-redmond', reorder: true});

                    $('#dataViewDiv').jqxTabs('showAllCloseButtons');
                    $("#dataViewDiv").find("li.jqx-tabs-title").find("div.jqx-tabs-close-button").css("background-image", "url(images/close.png)");
                    $("#dataViewDiv").find("li.jqx-tabs-title-selected-top").find("div.jqx-tabs-close-button").css("background-image", "url(images/close_white.png)");



                } else {
                    switchETLDesignTabs("li_contentView", "dataViewDiv");
                    var divStr = "<div>";
                    divStr += "<div style='display: flex;float:left;margin-top:5px;'>";
                    divStr += "<img src='images/refresh_icon.png' style='width:18px;height: 18px;cursor:pointer;padding-left:3px;margin-right:5px;' onclick=refreshTableGrid('" + gridId + "') title='Refresh'>"
                    divStr += "<div id='exportDropdown" + gridId.trim() + "' class='exportDropdown visionSearchExport visionSearchExportDiv' style='vertical-align: bottom; display: flex; padding-top: 8px;margin-top:-8px;'><table style='/*vertical-align: bottom;*/ display: inline-block;width: 40%;' class='visionSearchExportTable'><tbody><tr><td><select id='export" + gridId.trim() + "' onchange=getImportType('" + gridId.trim() + "')><option value=''>Select</option><option value='Xlsx'>Xlsx</option><option value='CSV'>CSV</option><option value='XML'>XML</option></select></td></tr></tbody></table> ";
                    divStr += "<input title='Export'  id='excelExport" + gridId.trim() + "' onclick=finalExport('" + gridId.trim() + "') class='exportClass visionSearchExportButton visionGridExportButton' type='button' disabled=''>";
                    divStr += "<input title='Import'  id='excelImport" + gridId.trim() + "' onclick=finalImport('" + gridId.trim() + "') class='importClass visionSearchImportButton visionGridImportButton' type='button' disabled='' style='margin-top: 5px;margin-left:5px;'>";
                    divStr += "</div>";
                    divStr += "</div>";
                    divStr += personalizeDivStr;
                    divStr += "<div id='" + gridId + "'></div>";

                    $('#dataViewDiv').jqxTabs('addLast', DBValue + "." + tableName, divStr); // ravi edit for tabs navigation
//                    $('#dataViewDiv').jqxTabs('addLast', DBValue + "." + tableName, '<div><div><img src="images/refresh_icon.png" style="width:18px;height: 18px;cursor:pointer;padding-left:3px;" onclick=refreshTableGrid("' + gridId + '") title="Refresh"></div><div id="' + gridId + '"></div></div>'); // ravi edit for tabs navigation
                }

                var headerTooltipRenderer = function (element) {
                    $(element).parent().jqxTooltip({position: 'mouse', theme: 'energyblue',
                        position: 'bottom-right',
                        showArrow: false, content: $(element).text()});
                }
                var source =
                        {
                            type: 'POST',
//                                                async: false,
                            datatype: "json",
                            datafields: dataFieldsArray,
                            data: data,
                            url: 'getSchemaObjectData',
                            cache: false,
                            root: 'Rows',
                            processdata: function (data) {
                                showLoader();
                                data['getOnlyDataArray'] = 'Y';

                            },
                            beforeSend: function () {
                                //showLoader();

                            }, loadError: function (xhr, status, error) {
                                $('#dataViewDiv').css("width", "100%");
                            }, loadComplete: function (data)
                            {
                                $('#dataViewDiv').css("width", "100%");
                                //$("#div_" + tableName).jqxGrid('hiderowdetails', 0);
                                //$("#row0div_" + tableName).hide();
                                stopLoader();
                            },
                            beforeprocessing: function (data) {

                                source.totalrecords = data[data.length - 1];

                            },
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
//                var source =
//                        {
//                            localdata: dataArray,
//                            datatype: "array",
//                            datafields: dataFieldsArray
//                        };
                window.allGridColumns[gridId] = columnsArray;
                var dataAdapter = new $.jqx.dataAdapter(source);
                var tabHeight = $("#" + gridId).closest(".jqx-tabs-content-element").height();
                $("[id='" + gridId + "']").jqxGrid(
                        {
                            width: "100%",
                            height: parseInt(tabHeight) * 0.88,
                            theme: 'energyblue',
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

                $('#' + gridId).on('celldoubleclick', function (event) {
                    var args = event.args;
                    var dataField = args.datafield;
                    var dataField1 = args.text;
                    var rowIndex = args.rowindex;
                    var cellValue = args.value;
                    var column = $('#' + gridId).jqxGrid('getcolumn', event.args.datafield).text;
                    popupedit(column, cellValue);
                });



                $('#dataViewDiv').unbind('selected').on('selected', function (event) {
                    var $thid = this;

                    $('#dataViewDiv').jqxTabs('getContentAt', i);

                    $("#dataViewDiv").find("li.jqx-tabs-title").find("div.jqx-tabs-close-button").css("background-image", "url(images/close.png)");
                    $("#dataViewDiv").find("li.jqx-tabs-title-selected-top").find("div.jqx-tabs-close-button").css("background-image", "url(images/close_white.png)");

                });

                $('#dataViewDiv').unbind('add').on('add', function (event) {

                    $("#dataViewDiv").find("li.jqx-tabs-title").find("div.jqx-tabs-close-button").css("background-image", "url(images/close.png)");
//                    $("#dataViewDiv").find("li.jqx-tabs-title-selected-top").find("div.jqx-tabs-close-button").css("background-image", "url(images/close_white.png)");
                    setTimeout(function () {
                        $("#dataViewDiv").find("li.jqx-tabs-title-selected-top").find("div.jqx-tabs-close-button").css("background-image", "url(images/close_white.png)");

                    }, 100);
                    var selectedTabTitle = $("#dataViewDiv").jqxTabs("getTitleAt", event.args.item);
                    $("#dataViewDiv").jqxTabs('setTitleAt', event.args.item, selectedTabTitle);
//                    $("#dataViewDiv").jqxTabs('setTitleAt', event.args.item, selectedTabTitle.split(".")[1]);
                    var selectedTabLi = $("#dataViewDiv").find("li.jqx-tabs-title-selected-top");
                    selectedTabLi.attr("title", selectedTabTitle);

                    var selectedItem = $('#dataViewDiv').jqxTabs('selectedItem'); // ravi edit for tabs issue
                    $('#dataViewDiv').jqxTabs('ensureVisible', selectedItem); // ravi edit for tabs issue


                });


                $("#" + gridId + "_personalizeid").toggleClass("ui-icon-triangle-1-s");
                $(document).mouseup(function (e)
                {
                    var container = $("#" + gridId + "_settings_panel");
                    var container1 = $(".personaliseoption");
                    if ((!container.is(e.target)
                            && container.has(e.target).length === 0)
                            && (!container1.is(e.target)
                                    && container1.has(e.target).length === 0)
                            )
                    {
                        $(container).hide();
                        $("#" + gridId + "_personalizeid").toggleClass("ui-icon-triangle-1-s");
                    }
                });
            }
            stopLoader();
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });

}
function searchColumns(tableName, DBValue) {
    showLoader();
    var connectionObj;
    if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
    {
        var conObj = savedDBData[DBValue];
        if (conObj != null && !jQuery.isEmptyObject(conObj)) {
            connectionObj = conObj;
        }
    }
    var filterColumn = $('#searchColumns').val().toLowerCase();
    $.ajax({
        datatype: "json",
        type: "post",
        traditional: true,
        url: 'selectSapTableColumns',
        cache: false,
        data: {
            filterColumn: filterColumn,
            parentkey: tableName,
            connectionObj: JSON.stringify(connectionObj)

        },
        success: function (response) {
            stopLoader();
            var columns = response.columnsArray;
            var tableStr = "";
            tableStr += '<div  class="visionTableName" style="position: relative;"><input id="selectAllCheckBoxes"  type="checkbox" > Select All </div>'; // ---- ravi edit for checkboxes

            for (var i = 0; i < columns.length; i++) {
                var destination = $("#destinationFields").find("[id=" + columns[i] + "]");
                if (destination.length > 0) {

                } else {
                    //tableStr +=  '<div id="'+columns[i]+'" class="visionTableName" title="'+columns[i]+'"  style="position: relative;">'+columns[i]+'<img src="images/crossicon.png" title="Clear Data" onclick="moveToSource('+columns[i]+')" class="visionCloseDestTableBtn"></div>';
                    tableStr += '<div id="' + columns[i] + '" class="visionTableName" title="' + columns[i] + '"  style="position: relative;"><input class="visionTableNameChbx" type="checkbox" > ' + columns[i] + '<img src="images/crossicon.png" title="Clear Data" onclick="moveColumnToSource(' + columns[i] + ')" class="visionCloseDestTableBtn"></div>';

                }
            }
            $("#sourceFields").html(tableStr);


            $("#selectAllCheckBoxes").click(function () {
//                      $(".visionTableNameChbx").prop('checked', $(this).prop('checked'));



                if ($(this).prop("checked")) {
                    $(".visionTableNameChbx").each(function (i) {
                        if (i < 50) {
                            $(this).prop('checked', true)
                            $(this).parent('div').addClass("multipleSelect");
                        } else {
                            $(this).prop('checked', false)
                            $(this).parent('div').removeClass("multipleSelect");
                        }

                    });

                }
                if (!$(this).prop("checked")) {
                    $(".visionTableNameChbx").prop('checked', $(this).prop('checked'));
                    $(".visionTableNameChbx").parent('div').removeClass("multipleSelect");
                }

            });

            $(".visionTableNameChbx").change(function () {

                var selectedLength = $(".sourceFields").find(".multipleSelect").length;
                if (selectedLength < 50) {
                    $("#selectAllCheckBoxes").prop("checked", false);
                    if (!$(this).prop("checked")) {
                        $(this).parent('div').removeClass("multipleSelect");
                    }
                    if ($(this).prop("checked")) {
                        $(this).parent('div').addClass("multipleSelect");
                    }
                } else {

                    if (!$(this).prop("checked")) {

                        $(this).prop("checked", false);
                        $(this).parent('div').removeClass("multipleSelect");
                        $("#selectAllCheckBoxes").prop("checked", false);
                    }
                    if ($(this).prop("checked")) {
                        showMesg("Cannot select more than 50");
                        $(this).prop("checked", false);
                        $(this).parent('div').removeClass("multipleSelect");
                    }

                }

            });

            dragColumns()
        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
}


function clearColumnInput(tableName, DBValue) {
    $("#searchColumns").val('');
    searchColumns(tableName, DBValue);
}
function dragColumns() {


    $("#sourceFields div").draggable({
        revert: "invalid",
        refreshPositions: true,
        cursor: 'move',
        zindex: false,
        opacity: false,
        appendto: "parent",
        helper: function (event) {
            var helperList = $('<ul class="draggable-helper" />');
            if ($(this).is('.multipleSelect')) {
                helperList.append($(this).siblings('.multipleSelect').andSelf().clone());
            } else {
                helperList.append($(this).clone());
            }
            return helperList;
        },
        drag: function (event, ui) {
            ui.helper.addClass("draggableTable");
            var height = ui.helper[0].offsetHeight;
            var dialogHeight = $("#logoutDailog").css("height");
            $("#logoutDailog").css("height", dialogHeight);
            $("#logoutDailog").css("overflow", "inherit");
            $("#destinationFields").css("min-height", "300px");
            $("#destinationFields").css("height", height);

        },
        stop: function (event, ui) {
            // ui.helper.removeClass("draggableTable");
            $("#destinationFields").css("height", "300px");
            $("#logoutDailog").css("height", "auto");
            $("#logoutDailog").css("overflow", "auto");

        }
    });
    $("#destinationFields").droppable({
        revert: "invalid",
        refreshPositions: true,
        cursor: 'move',
        drop: function (event, ui) {
            $("#destinationFields").css("height", "300px");
            $("#logoutDailog").css("height", "auto");
            $("#logoutDailog").css("overflow", "auto");

            if ($("#destinationFields div").length == 0) {
                $("#destinationFields").html("");
            }

            var destinationFieldsLength = $(".destinationFields").find(".visionTableName").length;
            if (destinationFieldsLength < 50) {
                $("#selectAllCheckBoxes").prop("checked", false);
                var items = ui.helper.children();
                $.each(items, function (index) {
                    var destinationFieldsLength = $(".destinationFields").find(".visionTableName").length;
                    if (destinationFieldsLength < 50) {
                        $(this).removeClass("multipleSelect");
                        $(this).addClass("droppedTable");
                        var droppableId = this.id;

                        $("#destinationFields").append($(this));


                        $("#sourceFields").find("#" + droppableId).remove();
                        $("#" + droppableId).children().show();

                        $("#destinationFields").find(".visionTableNameChbx").hide();
                    } else {
                        return false;
                    }
                })
            } else {
                showMesg("Cannot select more than 50 columns");
            }

        }
    });
}

function moveColumnToSource(column) {
    var columnName = column.id;
    $("#sourceFields").append($("#" + columnName));
    $("#" + columnName).find(".visionCloseDestTableBtn").hide();
    $("#" + columnName).find(".visionTableNameChbx").show();
    $("#" + columnName).find(".visionTableNameChbx").prop("checked", false);
    $("#" + columnName).find(".visionTableNameChbx").prop("checked", false);

    $(".visionTableNameChbx").change(function () {
        if (!$(this).prop("checked")) {
            $("#selectAllCheckBoxes").prop("checked", false);
            $(this).parent('div').removeClass("multipleSelect");
        }
        if ($(this).prop("checked")) {
            $(this).parent('div').addClass("multipleSelect");
        }
    });
    dragColumns();
}

function scheduleJob(jobId) {
    var jobName = $("#" + jobId).find("span").text();
    console.log("jobName::" + $.trim(jobName));
    if (jobId != null && jobId != ''
            && jobName != null && jobName != '') {
        jobName = $.trim(jobName);
        var scheduleForm = "<div class='cronStartDateDiv'>"
                + "<table><tr>"
                + "<td>Start Date</td>"
                + "<td><input type='text' id='cronStartDate' readonly='true'  /></td>"
                + "<td>End Date</td>"
                + "<td><input type='text' id='cronEndDate' /></td>"
                + "</tr></table>"
                + "</div>"
                + "<div id='cronExpressionDiv' class='cron-builder'></div>"
                + "<div class='cron-expression'>"
                + " <p>Cron Expression:<input type='text' id='cronExpression' readonly='true' /></p>"
                + "</div>";
        $("#logoutDailog").html(scheduleForm);
        $("#logoutDailog").dialog({
            title: (labelObject[jobName] != null ? labelObject[jobName] : jobName),
            modal: true,
            height: 250,
            minWidth: '650',
            maxWidth: 'auto',
            fluid: true,
            buttons: [{
                    text: (labelObject['Schedule'] != null ? labelObject['Schedule'] : 'Schedule'),
                    click: function () {
                        var cronExp = $('#cronExpression').val();
                        if (cronExp != null && cronExp != '') {
                            cronExp = cronExp.substring(0, cronExp.lastIndexOf(" "));
                            var cronStartDate = $("#cronStartDate").val();
                            var cronEndDate = $("#cronEndDate").val();
                            console.log("cronExp:::" + cronExp);
                            $(this).html("");
                            $(this).dialog("close");
                            $(this).dialog("destroy");
                            scheduleProcessJob(cronExp, jobId, jobName, cronStartDate, cronEndDate);
                        }


                    }},
                {
                    text: (labelObject['Cancel'] != null ? labelObject['Cancel'] : 'Cancel'),
                    click: function () {
                        $(this).html("");
                        $(this).dialog("close");
                        $(this).dialog("destroy");

                    }}],
            open: function () {
                $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                $(this).closest(".ui-dialog").find(".ui-button").eq(2).addClass("dialogno");
                $(".visionHeaderMain").css("z-index", "999");
                $(".visionFooterMain").css("z-index", "999");
                $('#cronExpressionDiv').cronBuilder({
                    selectorLabel: "Select time period:  ",
                    onChange: function (expression) {
                        var getData = expression.join(" ");
                        $('#cronExpression').val(getData);
                    }
                });
                $("#cronStartDate").datepicker({
                    changeMonth: true,
                    changeYear: true,
                    dateFormat: "dd-mm-yy",
                    showOn: "button",
                    buttonImage: 'images/date_picker_icon.png',
                    buttonImageOnly: true,

//                    setDate:new Date()
                });
                $("#cronStartDate").datepicker("setDate", new Date());
                $("#cronStartDate").addClass("ui-datepickerJobSchedule");

                $("#cronEndDate").datepicker({
                    changeMonth: true,
                    changeYear: true,
                    dateFormat: "dd-mm-yy",
                    showOn: "button",
                    buttonImage: 'images/date_picker_icon.png',
                    buttonImageOnly: true
                });
                $("#ui-datepicker-div").addClass("ui-datepickerJobSchedule");
//                $('#cronExpression').html("");
                //cronExpressionDiv
            },
            beforeClose: function (event, ui)
            {
                $(".visionHeaderMain").css("z-index", "99999");
                $(".visionFooterMain").css("z-index", "99999");
            }
        });
    }
    // need to open dailog box for Scheduling time
}
function scheduleProcessJob(cronExp, jobId, jobName, cronStartDate, cronEndDate) {
    if (cronExp != null && cronExp != ''
            && jobId != null && jobId != ''
            && jobName != null && jobName != '') {

        $.ajax({
            datatype: "json",
            type: "post",
            traditional: true,
            url: 'scheduleProcessJob',
            cache: false,
            data: {
                cronExp: cronExp,
                jobId: jobId,
                jobName: jobName,
                cronStartDate: cronStartDate,
                cronEndDate: cronEndDate


            },
            success: function (response) {
                stopLoader();
                var dialogSplitMessage = dialogSplitIconText(response['message'], "Y");
                $("#logoutDailog").html(dialogSplitMessage);
                $("#logoutDailog").dialog({
                    title: (labelObject[jobName] != null ? labelObject[jobName] : jobName),
                    modal: true,
                    height: 'auto',
                    minHeight: 'auto',
                    minWidth: 370,
                    maxWidth: 'auto',
                    fluid: true,
                    buttons: [{
                            text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                            click: function () {
                                $(this).html("");
                                $(this).dialog("close");
                                $(this).dialog("destroy");


                            }}],
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
            },
            error: function (e) {
                sessionTimeout(e);
                stopLoader();
            }
        });
    }
}

function loadScheduledJobs() {
    showLoader();
    $.ajax({
        datatype: "json",
        type: "post",
        traditional: true,
        url: 'loadScheduledJobs',
        cache: false,
        data: {

        },
        success: function (response) {
            stopLoader();
            if (response != null) {
                var dataArray = response['scheduledJobsArray'];
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
                        localdata: dataArray
                    };

            // create data adapter.
            var dataAdapter = new $.jqx.dataAdapter(source);
            // perform Data Binding.
            dataAdapter.dataBind();
            // get the tree items. The first parameter is the item's id. The second parameter is the parent item's id. The 'items' parameter represents 
            // the sub items collection name. Each jqxTree item has a 'label' property, but in the JSON data, we have a 'text' field. The last parameter 
            // specifies the mapping between the 'text' and 'label' fields.  
            var records = dataAdapter.getRecordsHierarchy('id', 'parentid', 'items', [{name: 'text', map: 'label'}]);
            //scheduledJobs
            $('#scheduledJobs').jqxTree({source: records, width: 300,
                toggleMode: 'click', theme: 'energyblue',
                enableHover: true,
                incrementalSearch: true,
                keyboardNavigation: true
            });
            $("#scheduledJobs").on('mousedown', function (event) {


                var target = $(event.target).parents('li:first')[0];
                var treeItem = $(event.target).closest('.visionETLAvailableJobs');
                var rightClick = isRightClick(event);
                if (rightClick && target != null) {
                    $("#scheduledJobs").jqxTree('selectItem', target);
                    var selectedItem = $('#scheduledJobs').jqxTree('getSelectedItem');
                    var jobId = selectedItem['value'];
                    if (jobId != null
                            && jobId != ''
                            && jobId != 'RUNNING_JOBS'
                            && jobId != 'STOPED_JOBS'
                            ) {
                        var parentListItem = selectedItem.parentElement;
                        if (parentListItem != null) {
                            var selectedParentItem = $('#scheduledJobs').jqxTree('getItem', parentListItem);
                            if (selectedParentItem != null && selectedParentItem['value'] == 'RUNNING_JOBS') {
                                var menuItems = "<li onclick=\"processScheduledJobFlag('" + jobId + "','STOP')\">Stop</li>";
                                menuItems += "<li onclick=\"processScheduledJobFlag('" + jobId + "','DELETE')\">Remove</li>";
                                var menuHeight = 2;
                                $("#jqxMenu").remove();
                                $(".visionMainPage").append("<div id='jqxMenu'><ul></ul></div>");
                                $("#jqxMenu ul").html(menuItems);
                                var contextMenu = $("#jqxMenu").jqxMenu({width: '120px', height: menuHeight * 27.5 + 'px', autoOpenPopup: false, mode: 'popup', theme: 'energyblue'}); // ravi start
                                var scrollTop = $(window).scrollTop();
                                var scrollLeft = $(window).scrollLeft();
                                contextMenu.jqxMenu('open', parseInt(event.clientX) + 5 + scrollLeft, parseInt(event.clientY) + 5 + scrollTop);
                                return false;
                            } else if (selectedParentItem != null && selectedParentItem['value'] == 'STOPED_JOBS') {
                                var menuItems = "<li onclick=\"processScheduledJobFlag('" + jobId + "','RUN')\">Run</li>";
                                menuItems += "<li onclick=\"processScheduledJobFlag('" + jobId + "','DELETE')\">Remove</li>";
                                var menuHeight = 2;
                                $("#jqxMenu").remove();
                                $(".visionMainPage").append("<div id='jqxMenu'><ul></ul></div>");
                                $("#jqxMenu ul").html(menuItems);
                                var contextMenu = $("#jqxMenu").jqxMenu({width: '120px', height: menuHeight * 27.5 + 'px', autoOpenPopup: false, mode: 'popup', theme: 'energyblue'}); // ravi start
                                var scrollTop = $(window).scrollTop();
                                var scrollLeft = $(window).scrollLeft();

                                contextMenu.jqxMenu('open', parseInt(event.clientX) + 5 + scrollLeft, parseInt(event.clientY) + 5 + scrollTop);
                                return false;
                            }
                        }


                    }

                }

            });
            $("#scheduledJobs li").on('dblclick', function (event) {
                var selectedItem = $("#scheduledJobs").jqxTree('getSelectedItem');
                var jobId = selectedItem['value'];
                if (jobId != null
                        && jobId != ''
                        && jobId != 'RUNNING_JOBS'
                        && jobId != 'STOPED_JOBS'
                        ) {
                    openSelectedJobLog(jobId);
                }

            });
        },
        error: function (e) {
            sessionTimeout(e);
            stopLoader();
        }
    });
}

function processScheduledJobFlag(jobId, jobFlag) {
    showLoader();
    var jobName = $("#" + jobId).find("span").text();
    if (jobName != null && jobName != '') {
        jobName = $.trim(jobName);
    }
    $.ajax({
        datatype: "json",
        type: "post",
        traditional: true,
        url: 'processScheduledJobFlag',
        cache: false,
        data: {
            jobId: jobId,
            jobFlag: jobFlag
        },
        success: function (response) {
            stopLoader();
            var dialogSplitMessage = dialogSplitIconText(response['message'], "Y");
            $("#logoutDailog").html(dialogSplitMessage);
            $("#logoutDailog").dialog({
                title: (labelObject[jobName] != null ? labelObject[jobName] : jobName),
                modal: true,
                height: 'auto',
                minHeight: 'auto',
                minWidth: 370,
                maxWidth: 'auto',
                fluid: true,
                buttons: [{
                        text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                        click: function () {
                            $(this).html("");
                            $(this).dialog("close");
                            $(this).dialog("destroy");
                            loadScheduledJobs();
                        }}],
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
        },
        error: function (e) {
            sessionTimeout(e);
            stopLoader();
        }
    });
}
function isRightClick(event) {
    var rightclick;
    if (!event)
        var event = window.event;
    if (event.which)
        rightclick = (event.which == 3);
    else if (event.button)
        rightclick = (event.button == 2);
    return rightclick;
}

function openSelectedJobLog(jobId) {
    var jobName = $("#" + jobId).find("span").text();
    if (jobName != null && jobName != '') {
        jobName = $.trim(jobName);
    }
    if (jobId != null && jobId != '') {
        var hidden = "<input type='hidden' id='currentProcessJoblogDate'/><input type='hidden' id='currentProcessJoblogIndex'/>";
        hidden += "<table id='processlogJobScheduleTable' class='logtable' style='width:100%'>"
                + "<thead>"
                + "<tr>"
                + "<th width='5%'></th>"
                + "<th width='25%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;'>Time Stamp</th>"
                + "<th width='70%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;'>Message</th>"
                + "</tr>"
                + "</thead>"
                + "<tbody>"
                + "</tbody>"
                + "</table>";
        $("#scheduledJobsDetails").html(hidden);
        processLogInterval = setInterval(function () {
            refreshScheduledJobLogFile(jobId) // this will run after every 1 seconds
        }, 2000);
    }

}

function  refreshScheduledJobLogFile(jobId) {
    stopLoader();
    var currentProcesslogDate = $("#currentProcessJoblogDate").val();
    var currentProcesslogIndex = $("#currentProcessJoblogIndex").val();
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'html',
        cache: false,
        url: 'refreshProcessLog',
        async: true,
        data: {
            currentProcesslogDate: ((currentProcesslogDate != null && currentProcesslogDate != '') ? currentProcesslogDate : ""),
            currentProcesslogIndex: ((currentProcesslogIndex != null && currentProcesslogIndex != '') ? currentProcesslogIndex : ""),
            jobId: jobId
        },
        success: function (response) {
            if (response != null && response != '') {
                var resultObj = JSON.parse(response);
                if (resultObj != null && !jQuery.isEmptyObject(resultObj)) {
                    $("#currentProcessJoblogIndex").val(resultObj['currentProcesslogIndex']);
                    var logTxt = resultObj['logTxt'];
                    if (logTxt != null && logTxt != '') {
                        $("#processlogJobScheduleTable tbody").append(logTxt);
                        $("#currentProcessJoblogDate").val(resultObj['currentProcesslogDate']);
                        if (resultObj['processFlag'] == 'N') {
                            clearInterval(processScheduledJobLogInterval);
                        }
                    } else {

                    }
                } else {

                }
            } else {

            }
        },
        error: function (e)
        {
            sessionTimeout(e);
            stopLoader();
            if (processScheduledJobLogInterval != null) {
                clearInterval(processScheduledJobLogInterval);
            }
        }

    });
}

function normalizeData(mapOperatorId) {

    showLoader();
    var selectedOperatorId = $('#flowchartworkSourcesspace').flowchart('getSelectedOperatorId');
    var selectedOperatorData = $('#flowchartworkSourcesspace').flowchart('getOperatorData', selectedOperatorId);
//    previousOperatorId = selectedOperatorId;
    prevTargetOperatorId = selectedOperatorId;

    var mapOperatorData = $('#flowchartworkSourcesspace').flowchart('getOperatorData', mapOperatorId);

    // var jobId = selectedOperatorData['jobId'];

    var mapingObj = $('#flowchartworkSourcesspace').flowchart('getMapOperatorData', mapOperatorId);
    console.log("::::::::" + JSON.stringify(mapingObj));
    if (mapingObj != null && !jQuery.isEmptyObject(mapingObj)) {
        var toOpArray = mapingObj['toOpArray'];
        var fromOpArray = mapingObj['fromOpArray'];
    }

    var fromOperator = fromOpArray[0];
    var toOperator = toOpArray[0];


    $.ajax({
        datatype: "json",
        type: "post",
        traditional: true,
        url: 'getNormalizeTabs',
        cache: false,
        data: {

            fromOperator: JSON.stringify(fromOperator),
            toOperator: JSON.stringify(toOperator),
            operatorData: JSON.stringify(selectedOperatorData)
        },
        success: function (response, status, xhr) {
            stopLoader();

            if (response != null) {
                $("#feedContentArea").html(response);
                $("#normalizeOptionsTabs").jqxTabs({width: "100%", height: "100%", position: 'top', theme: 'ui-redmond', reorder: true});

                $("#normalizeOptionsTabs").change(function (event) {
                    trfmRulesChanged = true;
                });
            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });

}




function deNormalizeData(mapOperatorId) {


    showLoader();
    var selectedOperatorId = $('#flowchartworkSourcesspace').flowchart('getSelectedOperatorId');
    var selectedOperatorData = $('#flowchartworkSourcesspace').flowchart('getOperatorData', selectedOperatorId);
//    previousOperatorId = selectedOperatorId;
    prevTargetOperatorId = selectedOperatorId;


    var mapOperatorData = $('#flowchartworkSourcesspace').flowchart('getOperatorData', mapOperatorId);

    // var jobId = selectedOperatorData['jobId'];

    var mapingObj = $('#flowchartworkSourcesspace').flowchart('getMapOperatorData', mapOperatorId);
    console.log("::::::::" + JSON.stringify(mapingObj));
    if (mapingObj != null && !jQuery.isEmptyObject(mapingObj)) {
        var toOpArray = mapingObj['toOpArray'];
        var fromOpArray = mapingObj['fromOpArray'];
    }

    var fromOperator = fromOpArray[0];
    var toOperator = toOpArray[0];


    $.ajax({
        datatype: "json",
        type: "post",
        traditional: true,
        url: 'getDeNormalizeTabs',
        cache: false,
        data: {
            fromOperator: JSON.stringify(fromOperator),
            toOperator: JSON.stringify(toOperator),
            operatorData: JSON.stringify(selectedOperatorData)
        },
        success: function (response, status, xhr) {
            stopLoader();

            if (response != null) {
                $("#feedContentArea").html(response);
                $("#deNormalizeOptionsTabs").jqxTabs({width: "100%", height: "100%", position: 'top', theme: 'ui-redmond', reorder: true});

                $("#deNormalizeOptionsTabs").change(function (event) {
                    trfmRulesChanged = true;
                });
            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });

}

function changeSelectedTableDb(id, index) {
    var joinDBStr = $("#EtlMappingTable").attr("data-join-db");
    if (joinDBStr != null && joinDBStr != '') {
        var joinDB = JSON.parse(joinDBStr);
        if (joinDB != null && !jQuery.isEmptyObject(joinDB)) {
            var tableName = $("#" + id).val();
            var selectedTableDBObj = joinDB[tableName];
            //data-table-db
            $("#" + id).attr("data-table-db", JSON.stringify(selectedTableDBObj));


        }
    }
}
function finalImport(gridId) {
    var typeName = $("#export" + gridId).val();
    var divStr = '<div class="dataMigratorContent"> <div class="visionProgressSteps" style="display: none;"><ul class="visionStepsProgressbar"><li id="connectToDB" class="active"><p class="visionProcessStepsContent">Connect to Database</p></li><li id="fetchTables"><p class="visionProcessStepsContent">Fetch Tables</p> </li><li id="mapColumns"><p class="visionProcessStepsContent">Map Columns</p></li></ul></div><div class="visionProgressFilesSteps"><ul class="visionStepsProgressbar"><li id="uploadFile" class="active"><p class="visionProcessStepsContent">Upload File</p></li><li id="mapFileCols"><p class="visionProcessStepsContent">Map Columns</p> </li></ul></div><div class="visionUploadFileDiv" id="visionUploadDocs"><div id="showFileType" class="visionShowFileType"></div><input type="hidden" id="selectedType" value=""><input type="hidden" id="selectedTypeName" value=""><input type="hidden" id="savedConnectionList" value=""><input type="hidden" value="" id="dbDetails"><input type="hidden" value="" id="auditId"><div id="visionDMFileUploadDiv" class="visionDMFileUploadDiv"><input type="file" name="importDMFile" id="importDMFile" class="visionDMFilesInput"><div class="visionDMFileUploadclass" id="visionDmFileUpload"><p class="VisionDMUploadFileContent">Upload Files Here </p></div> <div id="visionDMFileList"></div><div class="allErrors visionDMFiles" splitcount="0" id="disvisionDMFiles" style="color: red;display:none"></div></div><div id="visionFileMapCols"></div></div><div class="visionDatabaseMain"><div id="showConnectionType" class="visionShowConnectionType"></div><div class="visionConnectToDbDiv" id="visionConnectToDb"><table class="visionDbTable"> <tbody><tr class="visionDbTr"><td class="visionDbTd"> <label class="visionDbLabels">Connection Name</label></td><td class="visionDbTd"> <input type="text" value="" name="ConnectionName" id="DbConnectionName" class="visionInputDbFields"><div class="dataMigrationInputError" id="DbConnectionNameError"></div></td> </tr><tr class="visionDbTr"><td class="visionDbTd"> <label class="visionDbLabels">Host Name</label></td><td class="visionDbTd"> <input type="text" value="" name="HostName" id="DbHostName" class="visionInputDbFields"><div class="dataMigrationInputError" id="DbHostNameError"></div></td></tr><tr class="visionDbTr"><td class="visionDbTd">  <label class="visionDbLabels">Port</label></td><td class="visionDbTd"><input type="text" value="" name="Port" id="DbPort" class="visionInputDbFields"><div class="dataMigrationInputError" id="DbPortError"></div></td></tr><tr class="visionDbTr"><td class="visionDbTd">  <label class="visionDbLabels">Username</label></td><td class="visionDbTd"> <input type="text" value="" name="Username" id="DbUserName" class="visionInputDbFields"><div class="dataMigrationInputError" id="DbUserNameError"></div></td></tr> <tr class="visionDbTr"><td class="visionDbTd">  <label class="visionDbLabels">Password</label></td><td class="visionDbTd">    <input type="password" value="" name="HostName" id="DbPassword" class="visionInputDbFields"><div class="dataMigrationInputError" id="DbPasswordError"></div></td></tr><tr class="visionDbTr"><td class="visionDbTd">  <label class="visionDbLabels">Database/Service Name</label></td><td class="visionDbTd">    <input type="text" value="" name="ServiceName" id="DbServiceName" class="visionInputDbFields"><div class="dataMigrationInputError" id="DbServiceNameError"></div></td></tr><tr class="visionDbTr" style="display:none"><td class="visionDbTd">  <label class="visionDbLabels">Audit Id</label></td><td class="visionDbTd">    <input type="hidden" value="" name="auditId" id="auditId" class="visionInputDbFields"></td></tr><tr class="visionDbTr"><td><input type="checkbox" name="checkBoxDetails" id="checkBoxChecked" value="checked" checked="">Save Details<div class="visionDataMigrationError" style="display:none">Please check the box</div></td></tr><tr class="visionDbTr"><td class="visionDbTd visionDbConnectBtn" id="connectDbTd" colspan="2"><input type="button" value="Connect" name="Connect" onclick="connectDatabase()" class="visionInputDbButton"></td><td class="visionDbTd visionDbSaveBtn" id="saveDbTd" colspan="2" style="display:none"></td></tr></tbody></table></div><div id="fieldChooser" class="visionTablesComboBox" style="display:none"></div><div id="mappingColumns" class="visionColumnsMapping" style="display:none"><div id="showSourceTablesList" class="visionSourceTablesMain"></div><div class="visionMappedTable" id="MappedTable"></div><div class="visionProcessCols"><input type="button" value="Process" name="Process" id="processCols" onclick="fetchSelectedColumns()" class="visionProcessColsBtn"></div></div></div></div>';
    $('#dataMigratorPopup').html(divStr);


    $("#dataMigratorPopup").dialog({
        title: (labelObject['Data Piping'] != null ? labelObject['Data Piping'] : 'Data Piping'),
        modal: true,
        minHeight: 450,
        maxHeight: 600,
        minWidth: 970,
        maxWidth: 1200,
        fluid: true,

        open: function () {
            $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
            $(".visionHeaderMain").css("z-index", "999");
            $(".visionFooterMain").css("z-index", "999");

            //18-02

            $('.visionProgressSteps').hide();


            getDataBase("FILE", typeName.toUpperCase());

            $(".dataMigratorContent").css("width", "87%");
            $(".dataMigratorContent").css("margin-left", "8%");


        },
        beforeClose: function (event, ui)
        {
            $(".visionHeaderMain").css("z-index", "99999");
            $(".visionFooterMain").css("z-index", "99999");
        },
        close: function (event, ui)
        {
            try {
                $("#sub-tabs-2").jqxTabs("destroy");
            } catch (e) {
            }
            $(this).html("");
            $(this).dialog("close");
            $(this).dialog("destroy");
        }

    });

    var fileslist = [];
    $("html").on("dragover", function (e) {
        e.preventDefault();
        e.stopPropagation();

    });
    $("html").on("drop", function (e) {
        e.preventDefault();
        e.stopPropagation();
    });
    $('.visionDMFileUploadclass').on('drop', function (e) {
        $("#wait").css("display", "block");
        console.log("iam in drop functionality");
        var filetype = $('#selectedTypeName').val();
        etlFileImport("Y", filetype);

//        console.log("i am in drop functionality1" + filename);
    });


    $("#visionDmFileUpload").click(function (e) {
        $("#wait").css("display", "block");
        var filetype = $('#selectedTypeName').val();
        console.log("iam in clickable ");
        etlFileImport("N", filetype);


    });

    $("#visionDMFiles").on('change', function (event) {
        console.log("iam in files change ");
        var filetype = $('#selectedTypeName').val();
        etlFileImport("N", filetype);


    });

}


function getImportType(selectedGridId) {
    getExportType(selectedGridId);
    var importType = $('#export' + selectedGridId).val();
    if (importType.toUpperCase() == 'XLSX') {
        $("#excelImport" + selectedGridId).prop("disabled", false);
        // $("#excelImport"+selectedGridId).css('background','url("images/export_as_xlsx_icon_blue.png") 5px center no-repeat rgb(255, 255, 255)');

        $("input.importClass").css('background', '#fff url("images/export_as_xlsx_icon_blue.png")  no-repeat 5px center', 'important');
        $("input.importClass").hover(
                function () {
                    $("input.importClass").css('background', '#0071c5 url("images/export_as_xls_white.png")  no-repeat 5px center', 'important');
                }, function () {
            $("input.importClass").css('background', '#fff url("images/export_as_xlsx_icon_blue.png")  no-repeat 5px center', 'important');
        });


    } else if (importType.toUpperCase() == 'CSV') {
        $("#excelImport" + selectedGridId).prop("disabled", false);
        //$("#excelImport"+selectedGridId).css('background','url("images/export_as_csv_icon_blue.png") 5px center no-repeat rgb(255, 255, 255)');
        $("input.importClass").css('background', '#fff url("images/export_as_csv_icon_blue.png") no-repeat 5px center');
        $("input.importClass").hover(
                function () {
                    $("input.importClass").css('background', '#0071c5 url("images/export_as_csv_icon_white.png") no-repeat 5px center', 'important');
                }, function () {
            $("input.importClass").css('background', '#fff url("images/export_as_csv_icon_blue.png") no-repeat 5px center', 'important');
        });

    } else if (importType.toUpperCase() == "XML")
    {
        $("#excelImport" + selectedGridId).prop("disabled", false);
        $("input.importClass").css('background', '#fff url("images/import_icon_blue.png") no-repeat 5px center');
        $("input.importClass").hover(
                function () {
                    $("input.importClass").css('background', '#0071c5 url("images/import_icon_white.png") no-repeat 5px center', 'important');
                }, function () {
            $("input.importClass").css('background', '#fff url("images/import_icon_blue.png") no-repeat 5px center', 'important');
        });

    } else {
        $("#excelImport" + selectedGridId).prop("disabled", true);
    }

}


function etlFileImport(draganddropInd, filetype) {

    var validExtension = [];
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }

    var secretKey = $('meta[name=keygeneration]').attr("content");
    var params = {};
    var gridId = $('#gridIdStr').val();

    var browseId = "#importDMFile";
    var sourceex = $("#" + gridId).jqxGrid('source');
    var dataFeilds = [];
    dataFeilds = sourceex._source.datafields;

    params['gridId'] = $('#gridIdStr').val();
    params['selectedGridId'] = $('#gridIdStr').val();
    params['tableName'] = $('#tableName').val();
    params['selectedFiletype'] = filetype;
    var dataFeildsStr = JSON.stringify(dataFeilds);
    if (dataFeildsStr != null && dataFeildsStr != '' && dataFeildsStr != '{}') {
        dataFeildsStr = CryptoJS.AES.encrypt(dataFeildsStr, secretKey);
        params['dataFeilds'] = dataFeildsStr;
    }


    $(browseId).ajaxfileupload({
        'action': 'importDMFile',
        params: params,

        'onComplete': function (response) {
            $("#Loader").css("display", "none");
            $("body").css({"pointer-events": "auto"});
            var status = response.status;
            var message = response.message;
            if (message != null && message.indexOf("{") > -1 && message.indexOf("}") > -1) {
                message = message.substring(message.indexOf("{"), message.indexOf("}") + 1);
            }
            message = JSON.parse(message);
            // var tableStr = message.columnStr;
            var tableStr = jQuery('<div />').html(message.columnStr).text();
            var filePath = message.filePath;
            $('#filePathValue').val(filePath);
            var result = message.result;
            if (status == true && result != null && result == "File imported successfully") {
                $("#importDMFile").remove();

                var fileImprtDiv = "<input type='file' name='importDMFile' id='importDMFile' style='display:none'>";
                $("#visionDmFileUpload").parent().append(fileImprtDiv);
                var processBtn = "<input type='button' value= 'process' id= 'processMappedFileCols' onclick = 'processMappedFileCols()' class='visionProcessMappedFileBtn'>";
                $('#visionFileMapCols').html(tableStr + processBtn);

                $('#visionDMFileUploadDiv').hide();
                $('#visionFileMapCols').show();
                $('.visionProgressFilesSteps').show();
                $('#mapFileCols').addClass("active");

                if ($("#dataMigratorTreeDiv").length == 0) { // ravi import
                    $(".visionProcessMappedFileBtn").css("margin-right", "47%");
                }

            } else {
                $("#dialog").html(result);
                $("#dialog").dialog({
                    modal: true,
                    width: 270,
                    height: 135,
                    fluid: true,
                    buttons: {
                        Ok: function () {
                            $(this).dialog("close");


                        }
                    },
                    open: function () {
                        $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                        $(".visionHeaderMain").css("z-index", "999");
                        $(".visionFooterMain").css("z-index", "999");
                    },
                    beforeClose: function (event, ui)
                    {
                        $("#importDMFile").remove();
                        var fileImprtDiv = "<input type='file' name='importDMFile' id='importDMFile' style='display:none'>";
                        $("#visionDmFileUpload").parent().append(fileImprtDiv);
                        $(".visionHeaderMain").css("z-index", "99999");
                        $(".visionFooterMain").css("z-index", "99999");
                    }
                });


            }
            console.log("On Complete::");

        },
        'onStart': function () {
            $('#Loader').show();
            $("body").css({"pointer-events": "none"});
            $("#Loader").css("display", "block");
            // $('#message').hide();
        }
    });

    if (draganddropInd == 'N') {
        $("#importDMFile").click();

        //var filename = 
    }
}

function openOpeartorContextMenu(selectedOperatorId) {
    var toOpLinkArray = $('#flowchartworkSourcesspace').flowchart('getLinksFrom', selectedOperatorId);
    var fromOpLinkArray = $('#flowchartworkSourcesspace').flowchart('getLinksTo', selectedOperatorId);
    console.log("toOpLinkArray:::" + toOpLinkArray);
    console.log("fromOpLinkArray:::" + fromOpLinkArray);
    var selectedOperatorData = $('#flowchartworkSourcesspace').flowchart('getOperatorData', selectedOperatorId);
    var iconType = selectedOperatorData["iconType"];
    var dragType = selectedOperatorData["dragType"];


    var menuItems = "";
    var menuHeight = 30;
    if (iconType == 'XLS'
            || iconType == 'XLSX'
            || iconType == 'CSV'
            || iconType == 'XML'
            || iconType == 'TXT'
            || iconType == 'JSON'
            ) {

        menuItems = "<li onclick=showFileNamePopup(this,'" + iconType + "')>Rename File</li>";

        menuItems += "<li onclick=addHeadersToFile()>Add Headers</li>";
        menuHeight = 56;
        var fileDataObj = {};
        fileDataObj['filePath'] = "V10ETLExport_" + selectedOperatorData['timeStamp'] + "." + iconType.toLowerCase();
        var fileName = ((selectedOperatorData['userFileName'] != null && selectedOperatorData['userFileName'] != 'undefined') ? selectedOperatorData['userFileName'] : fileDataObj['filePath']);
        for (var entitykey in HtmlEntities) {
            var entity = HtmlEntities[entitykey];
            var regex = new RegExp(entitykey, 'g');
            fileName = fileName.replace(regex, entity);
        }


        fileDataObj['fileName'] = fileName;
        fileDataObj['fileType'] = "." + iconType.toLowerCase();
        fileDataObj['targetFile'] = "Y";

        $.ajax({
            type: 'post',
            traditional: true,
            dataType: 'JSON',
            cache: false,
            url: 'checkFileExist',
            async: false,
            data: {
                fileDataObj: JSON.stringify(fileDataObj)
            },
            success: function (response) {
                stopLoader();
                if (response != null) {
                    var fileExist = response['fileExist'];
                    if (fileExist == "Y") {
                        if (selectedOperatorData['trfmRules-data'] != null) {
                            var t_rules = selectedOperatorData['trfmRules-data']
                            var fileHeaders = t_rules['fileHeaders'];
                            if (fileHeaders != null && !jQuery.isEmptyObject(fileHeaders)) {
                                fileDataObj['fileHeaders'] = fileHeaders;
                            }
                        }
                        menuItems += "<li onclick=viewFileData('" + JSON.stringify(fileDataObj) + "')>View File Data</li>";
                        menuHeight = 84;



                    }
                }
            },
            error: function (e)
            {
                stopLoader();
                sessionTimeout(e);
            }

        });

    } else if (iconType == 'SQL') {
        menuItems = "<li onclick=showSQLPopup(this)>Create Table</li>";
        if (dragType == "Table") {
            var connectionObj = selectedOperatorData['connObj'];
            menuItems += "<li onclick=viewTableData('" + connectionObj['CONNECTION_NAME'] + "','" + $.trim(selectedOperatorData.statusLebel) + "'," + JSON.stringify(connectionObj) + ")>View Data</li>";
            menuHeight = 56;
        }
    } else if (dragType == "Table") {
        var connectionObj = selectedOperatorData['connObj'];
        menuItems = "<li onclick=viewTableData('" + connectionObj['CONNECTION_NAME'] + "','" + $.trim(selectedOperatorData.statusLebel) + "'," + JSON.stringify(connectionObj) + ")>View Data</li>";
    } else if (dragType == "File") {

        var connectionObj = selectedOperatorData['connObj'];
        var fileObj = {};
        var filePath = connectionObj['filePath'];
        if (filePath != null && filePath.lastIndexOf("\\") > -1) {
            filePath = filePath.substring(filePath.lastIndexOf("\\") + 1);
        }
        fileObj['filePath'] = filePath;
        fileObj['fileType'] = connectionObj['fileType'];
        var filename = (connectionObj['fileName']);
        for (var entitykey in HtmlEntities) {
            var entity = HtmlEntities[entitykey];
            var regex = new RegExp(entitykey, 'g');
            filename = filename.replace(regex, entity);
        }
        fileObj['fileName'] = filename;


        menuItems = "<li onclick=viewFileData('" + JSON.stringify(fileObj) + "')>View File Data</li>";
    }

    if (menuItems != "") {
        $("#jqxMenu").remove();
        $(".visionMainPage").append("<div id='jqxMenu'><ul></ul></div>");
        $("#jqxMenu ul").html(menuItems);
        var contextMenu = $("#jqxMenu").jqxMenu({width: '140px', height: menuHeight + 'px', autoOpenPopup: false, mode: 'popup', theme: 'energyblue'}); // ravi start

        var scrollTop = $(window).scrollTop();
        var scrollLeft = $(window).scrollLeft();

        contextMenu.jqxMenu('open', parseInt(event.clientX) + 5 + scrollLeft, parseInt(event.clientY) + 5 + scrollTop);
        return false;
    }


}
function viewTrfmRules() {

    var selectedOperatorId = $('#flowchartworkSourcesspace').flowchart('getSelectedOperatorId');
    $("#currentTrnsOpId").val(selectedOperatorId);
    var selectedOperatorData = $('#flowchartworkSourcesspace').flowchart('getOperatorData', selectedOperatorId);
    if (selectedOperatorData != null && selectedOperatorData['iconType'] == 'SQL') {
        if (selectedOperatorData['dragType'] == null) {
            showMesg("Please select target table/Create target table by Right clicking on SQL icon.");
            return false;
        }
    }

//    var fromOpLinkArray = $('#flowchartworkSourcesspace').flowchart('getLinksTo', selectedOperatorId);
    try {
        if (prevTargetOperatorId != null) { // new issues changes
            prevTargetOperatorId = parseInt(prevTargetOperatorId);
        }

    } catch (e) {
    }
    if (prevTargetOperatorId != null && parseInt(selectedOperatorId) != prevTargetOperatorId) { // latest changes
        var t_rules = {};

        var previousTargetOperatorData = $('#flowchartworkSourcesspace').flowchart('getOperatorData', prevTargetOperatorId);

        var isEmpty = jQuery.isEmptyObject(previousTargetOperatorData);
        if (!jQuery.isEmptyObject(previousTargetOperatorData)) {

            if (previousTargetOperatorData['trfmRules-data'] != null) {
                t_rules = previousTargetOperatorData['trfmRules-data'];
            }

            var componentOperator = {};
            var fromOpLinkArray = $('#flowchartworkSourcesspace').flowchart('getAllFromOperatorsByToOpId', prevTargetOperatorId);
            if (fromOpLinkArray != null) {
                if (fromOpLinkArray != null && fromOpLinkArray.length != 0) {
                    for (var i = 0; i < fromOpLinkArray.length; i++) {
                        var operatorData = fromOpLinkArray[i];
                        if (operatorData != null
                                && operatorData['iconType'] != null
                                && operatorData['iconType'] != '') {
                            componentOperator = operatorData;
                            break;
                        }

                    }
                }

            }


            if (componentOperator['iconType'] == "MAP") {
                var destinationColumnEmpty = false;

                var colMappingsData = []

                var rowCount = $('#sourceDestColsTableId tbody tr').length;
                var columnCount = $('#sourceDestColsTableId th').length;
                for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                    var rowData = {};
                    var tr = $('#sourceDestColsTableId tbody tr')[rowIndex];
                    var destinationColumnStyle = $(tr.children[1]).attr("style");

                    var destinationColumnVal = $(tr.children[1]).find('input').val();

                    if (destinationColumnStyle == "display:none;" || destinationColumnStyle == "display:none") {
                        destinationColumnVal = "N/A:N/A";
                    }
                    if (destinationColumnVal == null || destinationColumnVal == "") {
                        destinationColumnEmpty = true;
                    }


                    if (destinationColumnEmpty) {
                        showMesg("Destination Column Cannot be empty");
                        throw new Error();
                    }

                    //        if (destinationColumnVal != null &&
                    //                destinationColumnVal != "" &&
                    //                destinationColumnVal != "undefined"
                    //                && destinationColumnVal.indexOf(":")==-1){
                    //            destinationColumnVal = "N/A:"+destinationColumnVal; // ravi process job issues 
                    //        }

                    var dataFunobjStr = $(tr.children[5]).find('input').attr("data-funobjstr");
                    //funobjstr
                    var funObj = {};
                    if (dataFunobjStr != null && dataFunobjStr != '' && dataFunobjStr != 'null') {
                        funObj['funobjstr'] = dataFunobjStr;
                    }
                    rowData['primaryKey'] = $(tr.children[1]).find('input').prop('checked') ? "Y" : "N";
                    rowData['destinationColumn'] = destinationColumnVal;
                    rowData['sourceColumn'] = $(tr.children[3]).find('input').val();
                    rowData['defaultValue'] = $(tr.children[4]).find('input').val();
                    rowData['appendValue'] = $(tr.children[5]).find('input').val();
                    rowData['columnClause'] = (dataFunobjStr != null && dataFunobjStr != '' && dataFunobjStr != 'null') ? funObj : $(tr.children[6]).find('input').val();
                    rowData['data-funobjstr'] = $(tr.children[6]).find('input').attr("data-funobjstr"); // ravi change new
                    rowData['data-columnClause'] = $(tr.children[6]).find('input').val(); // ravi change new
                    colMappingsData[rowIndex] = rowData;


                    if (rowData['primaryKey'] == 'Y') {
                        t_rules['primaryKey'] = "Y";
                    }
                }

                t_rules['colMappingsData'] = colMappingsData;

                // ---------------------------------

                var masterTable = "";
                var childTables = [];
                var fromTables = [];

                var rowCount = $('#EtlMappingTable thead tr').length;
                for (var rowIndex = 1; rowIndex < rowCount; rowIndex++) {
                    var rowData = {};
                    var tr = $('#EtlMappingTable thead tr')[rowIndex];
                    fromTables.push($(tr.children[0]).find('select').val());
                    if (rowIndex == 1) {
                        masterTable = $(tr.children[0]).find('select').val();
                    } else {
                        childTables.push($(tr.children[0]).find('select').val());
                    }
                }

                if (masterTable == "") {

                    var mapingObj = $('#flowchartworkSourcesspace').flowchart('getMapOperatorData', prevTargetOperatorId);
                    if (mapingObj != null && !jQuery.isEmptyObject(mapingObj)) {
                        var fromOpArray = mapingObj['fromOpArray'];
                    }
                    if (fromOpArray != null && fromOpArray.length != 0) {
                        masterTable = fromOpArray[0]['tableName'];
                    }
                    //            try{
                    //               masterTable = $($(".visionOpIcons")[0]).parent().next().text().trim();
                    //            }catch(e){}
                }

                t_rules['masterTableName'] = masterTable;
                t_rules['masterTableName'] = masterTable;
                t_rules['childTables'] = childTables;
                t_rules['fromTables'] = fromTables;
                //------------------------------------

                var joinClauseData = []
                var mapIcons = $("#EtlMappingTable").find(".visionEtlJoinClauseMapIcon");

                $.each(mapIcons, function (index) {
                    var mappedColumnData = $(this).attr('data-mappedcolumns');
                    if (mappedColumnData != null
                            && mappedColumnData != ''
                            && mappedColumnData != 'null'
                            && mappedColumnData != '{}') {
//                        if (mappedColumnData == "") {
//                            mappedColumnData = "{}";
//                        }
                        joinClauseData[index] = mappedColumnData;
                    }
                });
                if (joinClauseData.length > 0) {
                    t_rules['joinClauseData'] = joinClauseData;
                    var fromTablesList = $(".sourceJoinColsTd").find("select");
                    if (fromTablesList.lenght > 0) {
                        var reOrderFromTables = [];
                        $.each(fromTablesList, function (i) {
                            reOrderFromTables.push(this.value);
                        })
                        t_rules['fromTables'] = reOrderFromTables;
                    }
                }

                //------------------------------------
                var whereClauseData = []
                var mapIcons = $("#selectedTables").find(".visionEtlWhereClauseMapIcon");
                $.each(mapIcons, function (index) {
                    var mappedColumnData = $(this).attr('data-whereclause');
                    if (mappedColumnData != null) {
                        if (mappedColumnData == "") {
                            mappedColumnData = "{}";
                        }
                        whereClauseData[index] = mappedColumnData;
                    }

                });
                if (whereClauseData.length > 0) {
                    t_rules['whereClauseData'] = whereClauseData;
                }
                //---------------------------------------------------- advanced settings  
                var selectTabObj = {};
                var uniqueRowsFlag = "N";
                if ($("#distinctRowsInput").is(":checked")) {
                    uniqueRowsFlag = "Y";
                }
                selectTabObj['uniqueRowsFlag'] = uniqueRowsFlag;

                var minRows = $("#rowsCountFromInput").val();
                var maxRows = $("#rowsCountToInput").val();
                if (!(minRows != null && minRows != '') && (maxRows != null && maxRows != '')) {
                    minRows = 0;
                }
                selectTabObj['minRows'] = minRows;
                selectTabObj['maxRows'] = maxRows;

                var operatorType = $("#operatorType").val();
                selectTabObj['operatorType'] = operatorType;

                t_rules['selectTabObj'] = selectTabObj;

                //------------------------------------

                var orderByData = []

                var rowCount = $('#fromTablesOrderCauseTable tbody tr').length;
                for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                    var rowData = {};
                    var tr = $('#fromTablesOrderCauseTable tbody tr')[rowIndex];
                    rowData['columnName'] = $(tr.children[1]).find('input').val();
                    rowData['order'] = $(tr.children[2]).find('select').val();
                    orderByData[rowIndex] = rowData;
                }
                t_rules['orderByData'] = orderByData;
                //------------------------------------

                var groupByData = []

                var rowCount = $('#fromTablesGroupCauseTable tbody tr').length;
                for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                    var rowData = {};
                    var tr = $('#fromTablesGroupCauseTable tbody tr')[rowIndex];
                    rowData['columnName'] = $(tr.children[1]).find('input').val();
                    groupByData[rowIndex] = rowData;
                }
                t_rules['groupByData'] = groupByData;

            } else if (componentOperator['iconType'] == "UNGROUP") {

                var normalizeOptionsObj = {};
                normalizeOptionsObj['normalizeFlag'] = "normalize";
                normalizeOptionsObj['itemSeparator'] = $("#itemSeparator").val();
                normalizeOptionsObj['normalizeColumn'] = $("#selectNormalizeColHeader").val();
                t_rules['normalizeOptionsObj'] = normalizeOptionsObj;
                var colMappingsData = [];
                var fromColsArray = $("#selectNormalizeColHeader").find("option");
                if (fromColsArray != null && fromColsArray.length > 0) {
                    $.each(fromColsArray, function (rowIndex) {
                        if (rowIndex != 0) {
                            var rowData = {};
                            var destFileHeaders = t_rules['fileHeaders'];
                            if (destFileHeaders != null) {
                                var destinationColumn = destFileHeaders[rowIndex];
                            }
                            rowData['sourceColumn'] = this.value;
                            rowData['destinationColumn'] = (destinationColumn != null) ? destinationColumn : this.value;
                            colMappingsData[rowIndex - 1] = rowData;
                        }

                    })

                    t_rules['colMappingsData'] = colMappingsData;
                }
            } else if (componentOperator['iconType'] == "GROUP") {

                var deNormalizeOptionsObj = {};
                deNormalizeOptionsObj['normalizeFlag'] = "deNormalize";
                deNormalizeOptionsObj['delimiter'] = $("#delimiter").val();
                deNormalizeOptionsObj['deNormalizeColumn'] = $("#selectDenormalizeColHeader").val();
                deNormalizeOptionsObj['keyColumn'] = $("#selectDenormalizeKeyColumn").val(); // ravi updated code

                t_rules['normalizeOptionsObj'] = deNormalizeOptionsObj;
                var colMappingsData = [];
                var fromColsArray = $("#selectDenormalizeColHeader").find("option");
                if (fromColsArray != null && fromColsArray.length > 0) {
                    $.each(fromColsArray, function (rowIndex) {
                        if (rowIndex != 0) {
                            var rowData = {};
                            var destFileHeaders = t_rules['fileHeaders'];
                            if (destFileHeaders != null) {
                                var destinationColumn = destFileHeaders[rowIndex];
                            }
                            rowData['sourceColumn'] = this.value;
                            rowData['destinationColumn'] = (destinationColumn != null) ? destinationColumn : this.value;
                            colMappingsData[rowIndex - 1] = rowData;
                        }

                    })

                    t_rules['colMappingsData'] = colMappingsData;
                }
            }



            previousTargetOperatorData['trfmRules-data'] = t_rules;
            previousTargetOperatorData['targetOperator'] = "Y";
            $('#flowchartworkSourcesspace').flowchart('setOperatorData', prevTargetOperatorId, previousTargetOperatorData);
            var previousTargetOperatorData2 = $('#flowchartworkSourcesspace').flowchart('getOperatorData', prevTargetOperatorId);
            $(".flowchart-operator-connector-label").hide();
            $(".flowchart-operator-title").hide();
        }
    }
    var fromOpLinkArray = $('#flowchartworkSourcesspace').flowchart('getLinksTo', selectedOperatorId);
    if (fromOpLinkArray.length != 0) {
        var mapOperatorId = fromOpLinkArray[0]['operatorId'];// latest changes
        if (mapOperatorId == null) {
            mapOperatorId = fromOpLinkArray[0]['fromOperator'];// latest changes
        }

        var mapOperatorData = $('#flowchartworkSourcesspace').flowchart('getOperatorData', mapOperatorId);
        if (mapOperatorData['iconType'] == "GROUP") {
            deNormalizeData(mapOperatorId);
            return false;
        } else if (mapOperatorData['iconType'] == "UNGROUP") {
            normalizeData(mapOperatorId);
            return false;
        }

        prevTargetOperatorId = selectedOperatorId;

        var fromOpLinkArrayToMap = $('#flowchartworkSourcesspace').flowchart('getLinksTo', mapOperatorId);
        var fromTableName = "";
        var fromConnectionObj = {};
        var fromOpArray = [];
        var toOpArray = [];
        $.each(fromOpLinkArrayToMap, function (index) {
            var fromOperator = $('#flowchartworkSourcesspace').flowchart('getOperatorData', this.fromOperator);
            fromOpArray.push(fromOperator);
            fromTableName += fromOperator['tableName'];
            fromConnectionObj = fromOperator["connObj"];
            if (index != fromOpLinkArrayToMap.length - 1) {
                fromTableName += ",";
            }
        });


        var trfmRulesId = selectedOperatorData['trfmRulesId'];
        var trfmRulesData = selectedOperatorData['trfmRules-data'];

        var toTableName = selectedOperatorData['tableName'];
        var toIconType = selectedOperatorData['iconType'];
        var toConnectionObj = selectedOperatorData['connObj'];
        toOpArray.push(selectedOperatorData);

        // ravi end
        if ((fromConnectionObj != null && !jQuery.isEmptyObject(fromConnectionObj))) {
            showLoader();
            if (!(toConnectionObj != null && !jQuery.isEmptyObject(toConnectionObj))) {
                toConnectionObj = {};
            }
            $.ajax({
                type: 'post',
                traditional: true,
                dataType: 'html',
                cache: false,
                url: 'fetchTransformationRules',
                async: true,
                data: {
                    fromTable: fromTableName,
                    toTable: toTableName,
                    fromConnObj: JSON.stringify(fromConnectionObj),
                    toConnObj: JSON.stringify(toConnectionObj),
                    sourceTables: JSON.stringify(fromTableName.split(",")),
                    toIconType: toIconType,
                    createTableObj: null,
                    trfmRulesId: trfmRulesId,
                    trfmRulesData: JSON.stringify(trfmRulesData),
                    trfmRulesChanged: (trfmRulesChanged == true) ? "Y" : "N",
                    toOpArray: JSON.stringify(toOpArray),
                    fromOpArray: JSON.stringify(fromOpArray),
                },
                success: function (response) {
                    stopLoader();
                    if (response != null) {
                        $('.visionTablesComboBox').hide();
                        $('.visionUploadFileDiv').hide();
                        $('.visionConnectToDbDiv').hide();
                        $('#visionERPMain').hide();
                        var response = JSON.parse(response);
                        if (response != null && response.connectionFlag == 'Y') {
                            $("#feedContentArea").html(response['tabsString']);
                            $("#dataMigrationTabs").jqxTabs({width: "100%", height: "130px", position: 'top', theme: 'ui-redmond', reorder: true});

                            $('#dataMigrationTabs').unbind('selected').on('selected', function (event) {
                                $('#iconsdiv').attr('style', 'margin-top:4px !important');
                                // scroll bar issue fixed by SHI
                                var feedContentAreaHeight = $("#feedContentArea").height();
//                    $("#dataMigrationTabs").jqxTabs({height:feedContentAreaHeight})
                                var tbodyHeight = feedContentAreaHeight - 90;
                                $(".visionColMappScrollDiv1").css("max-height", tbodyHeight);
                                $(".visionEtlJoinClauseTablesDiv").css("max-height", tbodyHeight);
//                                $(".visionEtlMappingTablesDiv").css("max-height", parseInt(feedContentAreaHeight) - 130);
                                $(".visionEtlJoinClauseTablesDivScroll").css("max-height", parseInt(feedContentAreaHeight) - 105);
                                $(".visionSqlViewQuery1").css("max-height", parseInt(feedContentAreaHeight) - 67);
                                $(".visionEtlJoinrClauseTablesDiv").css("max-height", parseInt(feedContentAreaHeight) - 130);
                                $(".visionEtlwhereClauseTablesDiv").css("max-height", parseInt(feedContentAreaHeight) - 69);
                                var joinTablesHeight = $(".visionEtlJoinrClauseTablesDiv").height();
                                $(".viewJoinQueryOuterDivClass").css("max-height", parseInt(feedContentAreaHeight) - (parseInt(joinTablesHeight) + 82));
                                // scroll bar issue fixed by SHI
                                //end code
                            });
//                        $("#dataMigrationTabs").jqxTabs({width: "100%", height: "100%", position: 'top', theme: 'ui-redmond', reorder: true});
                            var columnStr = response['columnMapping'];
                            var selectedJoinTables = response['selectedJoinTables'];
                            $('#tabs-1').html(columnStr);
                            $('#tabs-1').append("<div id='selectedColumnStr' style='display:none'></div>");
                            $("#toTableColsArray_hidden").remove();
                            $("#fromTableColsArray_hidden").remove();
                            var hiddenData = "<input type='hidden' id='toTableColsArray_hidden'/><input type='hidden' id='fromTableColsArray_hidden'/>";
                            $('#tabs-1').append(hiddenData);
                            $("#toTableColsArray_hidden").val(JSON.stringify(response['toTableColsArray']));
                            $("#fromTableColsArray_hidden").val(JSON.stringify(response['fromTableColsArray']));
                            $('#tabs-2').html(selectedJoinTables);
                            $('#mapColumns').addClass("active");
                            $('.visionProgressFilesSteps').hide();
                            $("#selectedColumnStr").html(response['colMapTrString']);//colMapTrString
                            var selectedTableWhereClause = response['selectedTableWhereClause'];
                            if (selectedTableWhereClause != null) {
                                var hiddenDataWhere = "<input type='hidden' id='whereClauseTableColsArray_hidden'/><input type='hidden' id='currentClauseMapId'/>"
                                        + "<div id='wherClauseTrString' style='display:none;'></div><div id='wherClauseColsString' style='display:none;'></div>";
                                $('#tabs-3').html(selectedTableWhereClause['whereClauseCondition'] + hiddenDataWhere);//whereClauseCondition
                                $("#whereClauseTableColsArray_hidden").val(JSON.stringify(selectedTableWhereClause['fromTableColsArray']));
                            }
                            var selectedTableOrderGroupClause = response['selectedTableOrderGroupClause'];
                            if (selectedTableOrderGroupClause != null) {
                                var hiddenDataOrderBy = ""
                                        + "<div id='orderClauseTrString' style='display:none;'></div>";
                                $('#tabs-4').html(selectedTableOrderGroupClause['orderByCondition'] + hiddenDataOrderBy);//
                                var hiddenDataGroupBy = ""
                                        + "<div id='groupClauseTrString' style='display:none;'></div>";
                                $('#tabs-5').html(selectedTableOrderGroupClause['groupByCondition'] + hiddenDataGroupBy);//
                                $("#groupClauseTrString").html(selectedTableOrderGroupClause['groupByTrString']);
                                $("#orderClauseTrString").html(selectedTableOrderGroupClause['orderByTrString']);
                            }
                            // sql Editor
//                        var sqlEditor = ace.edit("tabs-6");
//                        sqlEditor.setOptions({
//                            enableBasicAutocompletion: true, // the editor completes the statement when you hit Ctrl + Space
//                            showPrintMargin: false, // hides the vertical limiting strip
//                            fontSize: "100%" // ensures that the editor fits in the environment
//                        });
//
//                        sqlEditor.getSession().setMode("ace/mode/sql");
// scroll bar issue fixed by SHI
                            var feedContentAreaHeight = $("#feedContentArea").height();
                            if ($('#dataMigrationTabs').length) {
                                $("#dataMigrationTabs").jqxTabs({height: feedContentAreaHeight});
                            }
//                            $("#dataMigrationTabs").jqxTabs({height: feedContentAreaHeight})
                            var tbodyHeight = feedContentAreaHeight - 90;
                            $(".visionColMappScrollDiv1").css("max-height", tbodyHeight);
                            $(".visionEtlJoinClauseTablesDiv").css("max-height", tbodyHeight);
//                            $(".visionEtlMappingTablesDiv").css("max-height", parseInt(feedContentAreaHeight) - 130);
                            $(".visionEtlJoinClauseTablesDivScroll").css("max-height", parseInt(feedContentAreaHeight) - 105);
                            $(".visionSqlViewQuery1").css("max-height", parseInt(feedContentAreaHeight) - 67);
                            $(".visionEtlJoinrClauseTablesDiv").css("max-height", parseInt(feedContentAreaHeight) - 130);
                            $(".visionEtlwhereClauseTablesDiv").css("max-height", parseInt(feedContentAreaHeight) - 69);
                            var joinTablesHeight = $(".visionEtlJoinrClauseTablesDiv").height();
                            $(".viewJoinQueryOuterDivClass").css("max-height", parseInt(feedContentAreaHeight) - (parseInt(joinTablesHeight) + 82));
                            // scroll bar issue fixed by SHI
                            $("#dataMigrationTabs").change(function (event) {
                                trfmRulesChanged = true;
                            });
                            if (response['uniqueRowsFlag'] == "Y") {
                                $("#distinctRowsInput").prop("checked", true);
                            }

                            $("#rowsCountFromInput").val(response['minRows']);
                            $("#rowsCountToInput").val(response['maxRows']);

                            if (response['operatorType'] != null && response['operatorType'] != '') {
                                $("#operatorType").val(response['operatorType']);
                            }

//                        //importColMapFile
//                        var fileslist = [];
//                        $("#importColMapFile").on('change', function (event) {
//                            console.log("iam in files change ");
//                           fileslist  = event.target.files;
//                            srsFileNames(fileslist);
//
//                        });
                            $(".visionETLColMapImage").mousedown(function (event) {
                                treeIconClickEvent = event;
                            });
                            try {
                                $("#sourceDestColsTableId").colResizable({
                                    disable: true
                                });
                                $("#sourceDestColsTableId").tableDnD({
                                    onDragStyle: null,
                                    onDropStyle: null,
                                    onDragClass: "tDnD_whileDrag",
                                });
                            } catch (e) {
                            }
                            $("#sourceDestColsTableId").colResizable();

                            $('#dataMigrationTabs').on('selected', function (event) {
                                var selectedTab = event.args.item;
                                if (selectedTab == '0' || selectedTab == 0) {// mapping
                                    try {
                                        $("#sourceDestColsTableId").colResizable({
                                            disable: true
                                        });
                                        $("#sourceDestColsTableId").tableDnD({
                                            onDragStyle: null,
                                            onDropStyle: null,
                                            onDragClass: "tDnD_whileDrag",
                                        });
                                    } catch (e) {
                                    }
                                    $("#sourceDestColsTableId").colResizable();
                                } else if (selectedTab == '1' || selectedTab == 1) {// join
                                    try {
                                        $("#EtlMappingTable").colResizable({
                                            disable: true
                                        });
                                    } catch (e) {
                                    }
                                    $("#EtlMappingTable").colResizable();
                                } else if (selectedTab == '2' || selectedTab == 2) {//where clause
                                    try {
                                        $("#selectedTables").colResizable({
                                            disable: true
                                        });
                                    } catch (e) {
                                    }
                                    $("#selectedTables").colResizable();
                                } else if (selectedTab == '3' || selectedTab == 3) {//order clause
                                    try {
                                        $("#fromTablesOrderCauseTable").colResizable({
                                            disable: true
                                        });
                                    } catch (e) {
                                    }
                                    $("#fromTablesOrderCauseTable").colResizable();
                                } else if (selectedTab == '4' || selectedTab == 4) {//group clause
                                    try {
                                        $("#fromTablesGroupCauseTable").colResizable({
                                            disable: true
                                        });
                                    } catch (e) {
                                    }
                                    $("#fromTablesGroupCauseTable").colResizable();
                                }
                            });








                        } else {
                            stopLoader();
                            showMessagePopup(response.connectionMessage);
                        }


                    }
                },
                error: function (e)
                {
                    stopLoader();
                    sessionTimeout(e);
                }

            });
        } else {
            showMesg("Please map from table.");
            stopLoader();
        }
    }
}


function getAllMappedData(processFlag) {

    var currentTrnsOpId = $("#currentTrnsOpId").val();
    var t_rules = {};
    try {
        currentTrnsOpId = parseInt(currentTrnsOpId);
    } catch (e) {
    }
    var previousTargetOperatorData = $('#flowchartworkSourcesspace').flowchart('getOperatorData', currentTrnsOpId);

    if (!jQuery.isEmptyObject(previousTargetOperatorData)) {

        if (previousTargetOperatorData['trfmRules-data'] != null) {
            t_rules = previousTargetOperatorData['trfmRules-data'];
        }

        var componentOperator = {};
        var fromOpLinkArray = $('#flowchartworkSourcesspace').flowchart('getAllFromOperatorsByToOpId', currentTrnsOpId);
        if (fromOpLinkArray != null) {
            if (fromOpLinkArray != null && fromOpLinkArray.length != 0) {
                for (var i = 0; i < fromOpLinkArray.length; i++) {
                    var operatorData = fromOpLinkArray[i];
                    if (operatorData != null
                            && operatorData['iconType'] != null
                            && operatorData['iconType'] != '') {
                        componentOperator = operatorData;
                        break;
                    }

                }
            }

        }


        if (componentOperator['iconType'] == "MAP") {
            var destinationColumnEmpty = false;

            var colMappingsData = []

            var rowCount = $('#sourceDestColsTableId tbody tr').length;
            var columnCount = $('#sourceDestColsTableId th').length;
            for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                var rowData = {};
                var tr = $('#sourceDestColsTableId tbody tr')[rowIndex];
                var destinationColumnStyle = $(tr.children[1]).attr("style");

                var destinationColumnVal = $(tr.children[1]).find('input').val();

                if (destinationColumnStyle == "display:none;" || destinationColumnStyle == "display:none") {
                    destinationColumnVal = "N/A:N/A";
                }
                if (destinationColumnVal == null || destinationColumnVal == "") {
                    destinationColumnEmpty = true;
                }


                if (destinationColumnEmpty) {
                    showMesg("Destination Column Cannot be empty");
                    throw new Error();
                }

                //        if (destinationColumnVal != null &&
                //                destinationColumnVal != "" &&
                //                destinationColumnVal != "undefined"
                //                && destinationColumnVal.indexOf(":")==-1){
                //            destinationColumnVal = "N/A:"+destinationColumnVal; // ravi process job issues 
                //        }
                var dataFunobjStr = $(tr.children[5]).find('input').attr("data-funobjstr");
                //funobjstr
                var funObj = {};
                if (dataFunobjStr != null && dataFunobjStr != '' && dataFunobjStr != 'null') {
                    funObj['funobjstr'] = dataFunobjStr;
                }
                rowData['primaryKey'] = $(tr.children[1]).find('input').prop('checked') ? "Y" : "N";
                rowData['destinationColumn'] = destinationColumnVal;
                rowData['sourceColumn'] = $(tr.children[3]).find('input').val();
                rowData['defaultValue'] = $(tr.children[4]).find('input').val();
                rowData['appendValue'] = $(tr.children[5]).find('input').val();
                rowData['columnClause'] = (dataFunobjStr != null && dataFunobjStr != '' && dataFunobjStr != 'null') ? funObj : $(tr.children[6]).find('input').val();
                rowData['data-funobjstr'] = $(tr.children[6]).find('input').attr("data-funobjstr"); // ravi change new
                rowData['data-columnClause'] = $(tr.children[6]).find('input').val(); // ravi change new
                colMappingsData[rowIndex] = rowData;


                if (rowData['primaryKey'] == 'Y') {
                    t_rules['primaryKey'] = "Y";
                }
            }

            t_rules['colMappingsData'] = colMappingsData;

            // ---------------------------------

            var masterTable = "";
            var childTables = [];
            var fromTables = [];

            var rowCount = $('#EtlMappingTable thead tr').length;
            for (var rowIndex = 1; rowIndex < rowCount; rowIndex++) {
                var rowData = {};
                var tr = $('#EtlMappingTable thead tr')[rowIndex];
                fromTables.push($(tr.children[0]).find('select').val());
                if (rowIndex == 1) {
                    masterTable = $(tr.children[0]).find('select').val();
                } else {
                    childTables.push($(tr.children[0]).find('select').val());
                }
            }

            if (masterTable == "") {

                var mapingObj = $('#flowchartworkSourcesspace').flowchart('getMapOperatorData', currentTrnsOpId);
                if (mapingObj != null && !jQuery.isEmptyObject(mapingObj)) {
                    var fromOpArray = mapingObj['fromOpArray'];
                }
                if (fromOpArray != null && fromOpArray.length != 0) {
                    masterTable = fromOpArray[0]['tableName'];
                }
                //            try{
                //               masterTable = $($(".visionOpIcons")[0]).parent().next().text().trim();
                //            }catch(e){}
            }

            t_rules['masterTableName'] = masterTable;
            t_rules['masterTableName'] = masterTable;
            t_rules['childTables'] = childTables;
            t_rules['fromTables'] = fromTables;
            //------------------------------------

            var joinClauseData = []
            var mapIcons = $("#EtlMappingTable").find(".visionEtlJoinClauseMapIcon");

            $.each(mapIcons, function (index) {
                var mappedColumnData = $(this).attr('data-mappedcolumns');
                if (mappedColumnData != null
                        && mappedColumnData != ''
                        && mappedColumnData != 'null'
                        && mappedColumnData != '{}') {
//                    if (mappedColumnData == "") {
//                        mappedColumnData = "{}";
//                    }
                    joinClauseData[index] = mappedColumnData;
                }
            });
            if (joinClauseData.length > 0) {
                t_rules['joinClauseData'] = joinClauseData;
                var fromTablesList = $(".sourceJoinColsTd").find("select");
                if (fromTablesList.lenght > 0) {
                    var reOrderFromTables = [];
                    $.each(fromTablesList, function (i) {
                        reOrderFromTables.push(this.value);
                    })
                    t_rules['fromTables'] = reOrderFromTables;
                }
            }

            //------------------------------------
            var whereClauseData = []
            var mapIcons = $("#selectedTables").find(".visionEtlWhereClauseMapIcon");
            $.each(mapIcons, function (index) {
                var mappedColumnData = $(this).attr('data-whereclause');
                if (mappedColumnData != null) {
                    if (mappedColumnData == "") {
                        mappedColumnData = "{}";
                    }
                    whereClauseData[index] = mappedColumnData;
                }

            });
            if (whereClauseData.length > 0) {
                t_rules['whereClauseData'] = whereClauseData;
            }
            //---------------------------------------------------- advanced settings  
            var selectTabObj = {};
            var uniqueRowsFlag = "N";
            if ($("#distinctRowsInput").is(":checked")) {
                uniqueRowsFlag = "Y";
            }
            selectTabObj['uniqueRowsFlag'] = uniqueRowsFlag;

            var minRows = $("#rowsCountFromInput").val();
            var maxRows = $("#rowsCountToInput").val();
            if (!(minRows != null && minRows != '') && (maxRows != null && maxRows != '')) {
                minRows = 0;
            }
            selectTabObj['minRows'] = minRows;
            selectTabObj['maxRows'] = maxRows;

            var operatorType = $("#operatorType").val();
            selectTabObj['operatorType'] = operatorType;

            t_rules['selectTabObj'] = selectTabObj;

            //------------------------------------

            var orderByData = []

            var rowCount = $('#fromTablesOrderCauseTable tbody tr').length;
            for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                var rowData = {};
                var tr = $('#fromTablesOrderCauseTable tbody tr')[rowIndex];
                rowData['columnName'] = $(tr.children[1]).find('input').val();
                rowData['order'] = $(tr.children[2]).find('select').val();
                orderByData[rowIndex] = rowData;
            }
            t_rules['orderByData'] = orderByData;
            //------------------------------------

            var groupByData = []

            var rowCount = $('#fromTablesGroupCauseTable tbody tr').length;
            for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                var rowData = {};
                var tr = $('#fromTablesGroupCauseTable tbody tr')[rowIndex];
                rowData['columnName'] = $(tr.children[1]).find('input').val();
                groupByData[rowIndex] = rowData;
            }
            t_rules['groupByData'] = groupByData;

        } else if (componentOperator['iconType'] == "UNGROUP") {

            var normalizeOptionsObj = {};
            normalizeOptionsObj['normalizeFlag'] = "normalize";
            normalizeOptionsObj['itemSeparator'] = $("#itemSeparator").val();
            normalizeOptionsObj['normalizeColumn'] = $("#selectNormalizeColHeader").val();
            t_rules['normalizeOptionsObj'] = normalizeOptionsObj;
            var colMappingsData = [];
            var fromColsArray = $("#selectNormalizeColHeader").find("option");
            if (fromColsArray != null && fromColsArray.length > 0) {
                $.each(fromColsArray, function (rowIndex) {
                    if (rowIndex != 0) {
                        var rowData = {};
                        var destFileHeaders = t_rules['fileHeaders'];
                        if (destFileHeaders != null) {
                            var destinationColumn = destFileHeaders[rowIndex];
                        }
                        rowData['sourceColumn'] = this.value;
                        rowData['destinationColumn'] = (destinationColumn != null) ? destinationColumn : this.value;
                        colMappingsData[rowIndex - 1] = rowData;
                    }

                })

                t_rules['colMappingsData'] = colMappingsData;
            }
        } else if (componentOperator['iconType'] == "GROUP") {

            var deNormalizeOptionsObj = {};
            deNormalizeOptionsObj['normalizeFlag'] = "deNormalize";
            deNormalizeOptionsObj['delimiter'] = $("#delimiter").val();
            deNormalizeOptionsObj['deNormalizeColumn'] = $("#selectDenormalizeColHeader").val();
            t_rules['normalizeOptionsObj'] = deNormalizeOptionsObj;
            var colMappingsData = [];
            var fromColsArray = $("#selectDenormalizeColHeader").find("option");
            if (fromColsArray != null && fromColsArray.length > 0) {
                $.each(fromColsArray, function (rowIndex) {
                    if (rowIndex != 0) {
                        var rowData = {};
                        var destFileHeaders = t_rules['fileHeaders'];
                        if (destFileHeaders != null) {
                            var destinationColumn = destFileHeaders[rowIndex];
                        }
                        rowData['sourceColumn'] = this.value;
                        rowData['destinationColumn'] = (destinationColumn != null) ? destinationColumn : this.value;
                        colMappingsData[rowIndex - 1] = rowData;
                    }

                })

                t_rules['colMappingsData'] = colMappingsData;
            }
        }



        previousTargetOperatorData['trfmRules-data'] = t_rules;
        previousTargetOperatorData['targetOperator'] = "Y";
        $('#flowchartworkSourcesspace').flowchart('setOperatorData', currentTrnsOpId, previousTargetOperatorData);
        var previousTargetOperatorData2 = $('#flowchartworkSourcesspace').flowchart('getOperatorData', currentTrnsOpId);
        $(".flowchart-operator-connector-label").hide();
        $(".flowchart-operator-title").hide();
    }
    var processJobData = {}

    var data = {};
    try {
        data = $('#flowchartworkSourcesspace').flowchart('getData');
    } catch (e) {
    }




    if (data != null && !jQuery.isEmptyObject(data)) {
        var operators = data['operators'];
        var links = data['links'];
        if (operators != null && !jQuery.isEmptyObject(operators)
                && links != null && !jQuery.isEmptyObject(links)) {

            var selectedMapOperatorData = $('#flowchartworkSourcesspace').flowchart('getOperatorData', currentTrnsOpId);
            var jobId = selectedMapOperatorData['jobId'];
            var jobName = selectedMapOperatorData['jobName'];

            // ravi save job without trfm rules start
            var targetOperatorExist = false;
            $.each(data['operators'], function (index) {
                var iconType = this['iconType'];
                if (iconType == "MAP" || iconType == "GROUP" || iconType == "UNGROUP" || iconType == "SPLITROW") {
                    var mapOperatorId = this.operatorId;
                    $.each(data['links'], function () {
                        if (this.fromOperator == mapOperatorId) {
                            var toOperatorData = $('#flowchartworkSourcesspace').flowchart('getOperatorData', this.toOperator);
                            toOperatorData['targetOperator'] = "Y";
                            $('#flowchartworkSourcesspace').flowchart('setOperatorData', this.toOperator, toOperatorData);
                            if (jobId == null) {
                                jobId = toOperatorData['jobId'];
                                jobName = toOperatorData['jobName'];
                            }
                            targetOperatorExist = true;
                        }
                    })

                }
            })
            if (!targetOperatorExist) {
                showMesg("Cannot save without a target");
                return false;
            }

            // ravi save job without trfm rules end

//    if (jobId == null) { // ravi normalise
//        $.each(data['operators'], function (index) {
//            if (this.targetOperator == "Y") {
//                jobId = this.jobId;
//                jobName = this.jobName;
//                return false;
//            }
//        })
//    }
//            if (jobName == null || jobName == ""   // ----------------------------------  code start emptyjob
//                    && jobId == null || jobId == "") {
//                jobName = $("#emptyJobName").val();
//                jobId = $("#emptyJobId").val();
//
//            }

            if (jobId == null) { // ravi save new job
                var currentJobId = $("#currentJobId").val();

                var selectedJobId = $(".selectJqxTreeItem :first-child").attr("id");
                if (currentJobId == selectedJobId) {
                    jobId = currentJobId;
                    jobName = $("#currentJobName").val();
                }

            }

            $("#currentJobId").val(jobId);
            if (jobId != null && jobId != "") {
                saveJob(jobName, jobId, data, processJobData, processFlag);
                return false;
            }
            // ravi end


            var response = "<div id='textReason'>";
            response += "<textarea id='reasonId' class='visionDeleteReason'></textarea></div>";
            response += "<div id='dailog_error_id' style='display:none;color:red'>" + (labelObject['Please give any Job Name'] != null ? labelObject['Please give new Job Name'] : 'Please give new Job Name') + "</div>";

            $("#dialog").html(response);
            $("#dialog").dialog({
                title: (labelObject['Job Name'] != null ? labelObject['Job Name'] : 'Job Name'),
                modal: true,
                height: 'auto',
                minWidth: 300,
                maxWidth: 'auto',
                fluid: true,
                buttons: [{
                        text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                        click: function () {
                            var jobName = $("#reasonId").val();
                            if (jobName != null && jobName != '') {
                                $("#dailog_error_id").hide();
                                $(this).html("");
                                $(this).dialog("close");
                                $(this).dialog("destroy");
                                saveJob(jobName, jobId, data, processJobData, processFlag);

                            } else {
                                $("#dailog_error_id").show();
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
        } else {
            $("#dialog").html(labelObject['No Mapped Objects to Save'] != null ? labelObject['No Mapped Objects to Save'] : 'No Mapped Objects to Save');
            $("#dialog").dialog({
                title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
                modal: true,
                height: 'auto',
                minWidth: 300,
                maxWidth: 'auto',
                fluid: true,
                buttons: [{
                        text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                        click: function () {
                            $(this).html("");
                            $(this).dialog("close");
                            $(this).dialog("destroy");

                        }}],
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

    } else {
        $("#dialog").html(labelObject['No Mapped Objects to Save'] != null ? labelObject['No Mapped Objects to Save'] : 'No Mapped Objects to Save');
        $("#dialog").dialog({
            title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
            modal: true,
            height: 'auto',
            minWidth: 300,
            maxWidth: 'auto',
            fluid: true,
            buttons: [{
                    text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                    click: function () {
                        $(this).html("");
                        $(this).dialog("close");
                        $(this).dialog("destroy");

                    }}],
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

}
function cancellCurrentJob() {
    var jobId = $("#currentJobId").val();
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'html',
        cache: false,
        url: 'cancellProcessJob',
        async: true,
        data: {
            jobId: jobId
        },
        success: function (response) {
            stopLoader();

        },
        error: function (e)
        {
            stopLoader();
            sessionTimeout(e);
        }

    });
}

function disableDdw($this, id) {
    var operator = $($this).val();
    var tr = $this.closest("tr");
    var tdArray = tr.cells;
    if (tdArray != null && tdArray.length != 0) {
        if (operator == 'IS' || operator == 'IS NOT') {
            $(tdArray[3]).find("input").val("NULL");
            $(tdArray[3]).find("input").attr('readonly', 'true');
        } else {
            $(tdArray[3]).find("input").val("");
//            $(tdArray[3]).find("input").attr('readonly', 'false');
            $(tdArray[3]).find("input").removeAttr('readonly');
        }
    }

}

function addHeadersToFile() {
    var selectedOperatorId = $('#flowchartworkSourcesspace').flowchart('getSelectedOperatorId');
    var selectedOperatorData = $('#flowchartworkSourcesspace').flowchart('getOperatorData', selectedOperatorId);
    var t_Rules = selectedOperatorData['trfmRules-data'];
    if (t_Rules != null) {
        var inputFields_Obj = t_Rules['fileHeaders'];
    }
    var headersTr = "";
    if (inputFields_Obj != null && !jQuery.isEmptyObject(inputFields_Obj)) {
        var inputFields_ObjLen = Object.keys(inputFields_Obj).length;
        for (var i = 1; i <= inputFields_ObjLen; i++) {
            headersTr += '<td width="6.5%"><img src="images/Detele Red Icon.svg" onclick="deleteSelectedRow(this)" class="visionColMappingImg" title="Delete" style="width:15px;height: 15px;cursor:pointer;"></td>\n\
                            <td width="6.5%">' + i + '</td>\n\
                            <td width="87%" class="visionFileHeaderNameTd"><input class="visionFileHeaderNameInput" type="text" value="' + inputFields_Obj[i] + '" style="width:97%" ></td></tr>';
        }
    } else {
        headersTr += '<td width="6.5%"><img src="images/Detele Red Icon.svg" onclick="deleteSelectedRow(this)" class="visionColMappingImg" title="Delete" style="width:15px;height: 15px;cursor:pointer;"></td>\n\
                     <td width="6.5%">1</td>\n\
                     <td width="87%" class="visionFileHeaderNameTd"><input class="visionFileHeaderNameInput" type="text" value="" style="width:97%" ></td></tr>';

    }


    var divStr = '<div class="visionETLaddheader">\n\
                  <div class="visionEtlAddIconDiv"><img data-trstring="" src="images/Add icon.svg" id="visionEtlAddRowIcon" class="visionHeaderEtlAddRowIcon" title="Add new Headers" onclick="addNewFileHeader(event,id,this)" style="width:15px;height: 15px;cursor:pointer;"></div><div class="visionEtlFileHeaderTablesDiv">\n\
                  <table id="fileColumnHeadersTable" class="visionEtlFileHeaderTable" style="width: 100%;" border="1"><thead><tr>\n\
                    <th width="6.5%" class="" style="background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center"></th>\n\
                    <th width="6.5%" class="" style="background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center"></th>\n\
                    <th width="87%" class="" style="background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center">Header Name</th></tr></thead>\n\
                  <tbody><tr>';

    divStr += headersTr;
    divStr += '</tbody></table></div></div>';

    $("#dialog").html(divStr);
    $("#dialog").dialog({
        title: (labelObject['Add File Headers'] != null ? labelObject['Add File Headers'] : 'Add File Headers'),
        modal: true,
        width: 300,
        fluid: true,
        buttons: [{
                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                click: function () {
                    if (selectedOperatorData['trfmRules-data'] == null) {
                        selectedOperatorData['trfmRules-data'] = {};
                    }
                    var tRules = selectedOperatorData['trfmRules-data'];

                    var inputFieldsObj = {};
                    var inputFieldsObjLength = Object.keys(inputFieldsObj).length;
                    $(".visionFileHeaderNameInput").each(function (index) {
                        if (this.value != null && this.value != '' && this.value != 'undefined') {

                            inputFieldsObj[inputFieldsObjLength + index + 1] = this.value;
                        }
                    });

                    tRules['fileHeaders'] = inputFieldsObj;
                    selectedOperatorData['trfmRules-data'] = tRules;

                    var fileType = "." + selectedOperatorData['iconType'];
                    var imageIcon = "";
                    if (fileType == '.xls'
                            || fileType == '.xlsx'
                            || fileType == '.XLS'
                            || fileType == '.XLSX'
                            ) {
                        imageIcon = "images/xlsx Icon-01.svg"
                    } else if (fileType == '.xml'
                            || fileType == '.XML') {
                        imageIcon = "images/XML Icon-01.svg";

                    } else if (fileType == '.CSV'
                            || fileType == '.csv') {
                        imageIcon = "images/CSV ICON-01.svg";
                    } else if (fileType == '.JSON'
                            || fileType == '.json') {
                        imageIcon = "images/JSON_Isons-02.svg";
                    }

                    selectedOperatorData['imageIcon'] = imageIcon;
                    $('#flowchartworkSourcesspace').flowchart('setOperatorData', selectedOperatorId, selectedOperatorData);
                    if ($("#dataMigrationTabs").length > 0) {
                        viewTrfmRules(); // ravi updated code changes
                    }

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
            $("#dataMigrationTabs").jqxTabs({theme: 'ui-redmond'});

        },
        beforeClose: function (event, ui)
        {
            $(".visionHeaderMain").css("z-index", "99999");
            $(".visionFooterMain").css("z-index", "99999");

        }, close: function (event, ui)
        {

            $(this).html("");
            $(this).dialog("close");
            $(this).dialog("destroy");
        }
    });
}

function addNewFileHeader(event, id, $this) {
    var rowIndex = $("#fileColumnHeadersTable tr:last")[0].rowIndex;
    rowIndex = rowIndex + 1;
    var newRow = '<tr><td width="6.5%"><img src="images/Detele Red Icon.svg" onclick="deleteSelectedRow(this)" class="visionColMappingImg" title="Delete" style="width:15px;height: 15px;cursor:pointer;"></td>\n\
                <td width="6.5%">' + rowIndex + '</td>\n\
                <td width="87%" class="visionFileHeaderNameTd"><input class="visionFileHeaderNameInput" type="text" value="" style="width:97%";></td></tr>'
    $('#fileColumnHeadersTable tr:last').after(newRow);
}

function deleteAllTableTrs(tableId) {
    $("#" + tableId + " > tbody").empty();
    try {
        $("#mainSplitter").resize();
    } catch (e) {
    }
}
function moveSapTableColumns() {
    //  $("#sourceFields").find(".multipleSelect").appendTo(".destinationFields ");
    var destinationFieldsLength = $(".destinationFields").find(".visionTableName").length;
    if (destinationFieldsLength < 50) {
        $("#selectAllCheckBoxes").prop("checked", false);
        $("#sourceFields").find(".multipleSelect").each(function () {
            var destinationFieldsLength = $(".destinationFields").find(".visionTableName").length;
            $(this).appendTo(".destinationFields");
            $(this).children().show();
            $(this).find(".visionTableNameChbx").hide();
//            if (destinationFieldsLength < 50) {
//                $(this).appendTo(".destinationFields");
//                $(this).children().show();
//                $(this).find(".visionTableNameChbx").hide();
//            } else {
//                return false;
//            }

        });
    } else {
        showMesg("Cannot select more than 50 columns");
    }

}

function slideSettingsETL(gridId) {
//  $("#settings_panel").toggle(100)
    $('#' + gridId + '_settings_panel').toggle('slide', {direction: 'right'}, 500);
    $("#" + gridId + "_personalizeid").toggleClass("ui-icon-triangle-1-s");
    //  var slideSettingsInd = $("#accordion").attr("data-slidesettingsind");
//    if (slideSettingsInd == 'N') {
//         $("#accordion").attr("data-slidesettingsind","Y");
//        getPersonalizationData();
//    }

    //$('#showcriteria').toggle();
}
function updateETLPersonalize(id) {
    var columnName = $("#" + id).attr("data-colname");
    var type = $("#" + id).attr("data-type");
    var gridId = $("#" + id).attr("data-gridid");
    if (type == 'display') {
        try {

            if ($("#" + id).is(':checked')) {
                $('#' + gridId).jqxGrid('showcolumn', columnName);
            } else {
                $('#' + gridId).jqxGrid('hidecolumn', columnName);
            }

        } catch (e) {

        }
    } else if (type == 'pinned') {
        try {

            if ($("#" + id).is(':checked')) {
                $('#' + gridId).jqxGrid('pincolumn', columnName);
            } else {
                $('#' + gridId).jqxGrid('unpincolumn', columnName);
            }

        } catch (e) {

        }
    }
}
//IAC
function loadScheduledJobsInIAC() {
    showLoader();
    $.ajax({
        datatype: "json",
        type: "post",
        traditional: true,
        url: 'loadScheduledJobs',
        cache: false,
        data: {

        },
        success: function (response) {
            stopLoader();
            if (response != null) {
                var dataArray = response['scheduledJobsArray'];
            }
            var source = [
                {
                    label: "Scheduling", items: [
                        {label: "Execution Plan"}
//                        {label: "Re_Scheduling Task"},
//                        {label: "Lock Scheduling Job"}
                    ]
                },
                {
                    label: "Monitoring", items: [
                        {label: "Execution History"},
                        {label: "Time Line"},
                        {label: "Command Line"},
                        {label: "Activity Monitoring Console"},
                    ]
                },
                {label: "Logging"},
                {
                    label: "Notifications", items: [
                    ]
                }
            ];

            $('#scheduledJobs').jqxTree({source: source, width: 300,
                toggleMode: 'click', theme: 'energyblue',
                enableHover: true,
                incrementalSearch: true,
                keyboardNavigation: true
            });

            $("#scheduledJobs").on('select', function (event) {
                var executionDiv = "<div id='iTransFromsIAS'> <div id='iTransFormIcons' style='float: left;' style='display:none;'>"
                        + "<span>"
                        + "<img src='images/Refresh Icon.svg' onclick='loadScheduledJobsInIAC()' class='visionETLIcons' title='Refresh' style='width:15px;height: 15px;cursor:pointer;' /></span>"
                        + "<span> <img src='images/Add icon.svg' onclick='showSavedJobInIAC()'  class='visionEtlColumnMapIcon' title='Add'  style='width:15px;height: 15px;cursor:pointer;'></span>"
                        + " <span>"
                        + " <img src='images/Detele Red Icon.svg' class='visionETLIcons' title='Delete' style='width:15px;height: 15px;cursor:pointer;'"
                        + " /></span>"
                        + " </div>";
                executionDiv += "<table id='iTransformTable'  style='width:100%'>"
                        + "<thead>"
                        + "<tr>"
                        + "<th  style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;'>Task Name</th>"
                        + "<th  style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;'>Start Date</th>"
                        + "<th  style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;'>Start Time</th>"
                        + "<th  style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;'>End Date</th>"
                        + "<th  style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;'>End Time</th>"
                        + "<th  style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;'>Status</th>"
                        + "<th  style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;'>log File</th>"
                        + "<th  style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;'>User Name</th>"
                        + "<th  style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;'>Description</th>"
                        + "</tr>"
                        + "</thead>"
                        + "<tbody>"
                        + "</tbody>"
                        + "</table></div>";
                var lockDiv = "<div id='lockDiv'> <div id='iTransFormIcons' style='float: left;' style='display:none;'>"
                        + "<span>"
                        + "<img src='images/Refresh Icon.svg' class='visionETLIcons' title='Refresh' style='width:15px;height: 15px;cursor:pointer;' /></span>"

                        + " <span>"
                        + " <img src='images/Detele Red Icon.svg' class='visionETLIcons' title='Remove Lock' style='width:15px;height: 15px;cursor:pointer;'"
                        + " /></span>"
                        + " </div>";
                lockDiv += "<table id='iTransformTable'  style='width:100%'>"
                        + "<thead>"
                        + "<tr>"
                        + "<th  style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;'>Locked Item</th>"
                        + "<th  style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;'>Locker</th>"
                        + "<th  style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;'>Locking Date</th>"
                        + "<th  style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;'>Status</th>"
                        + "<th  style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;'>Description</th>"
                        + "</tr>"
                        + "</thead>"
                        + "<tbody>"
                        + "</tbody>"
                        + "</table></div>";

                //pkh
                var scheduledJobsExPlanDiv = "<div id='iTransFromsIAS'> <div id='iTransFormIcons' style='float: left;' style='display:none;'>"
                        + "<span>"
                        + "<img src='images/Refresh Icon.svg' onclick='refreshIacData()' class='visionETLIcons' title='Refresh' style='width:15px;height: 15px;cursor:pointer;' /></span>"
                        + "<span> <img src='images/Add icon.svg' onclick='showSavedJobInIAC()'  class='visionEtlColumnMapIcon' title='Add'  style='width:15px;height: 15px;cursor:pointer;'></span>"
                        + " <span>"
                        + " <img src='images/Detele Red Icon.svg' onclick='deleteSelectedJob()' class='visionETLIcons' title='Delete' style='width:15px;height: 15px;cursor:pointer;'"
                        + " /></span>"
                        + " </div>";
                +" </div>";
                var lockScheduleDiv = "<div id='iTransFromsIAS'> <div id='iTransFormIcons' style='float: left;' style='display:none;'>"
                        + "<span>"
                        + "<img src='images/Refresh Icon.svg' onclick='refreshIacData()' class='visionETLIcons' title='Refresh' style='width:15px;height: 15px;cursor:pointer;' /></span>"
                        + "<span> <img src='images/Add icon.svg' onclick='showSavedJobInIAC()'  class='visionEtlColumnMapIcon' title='Add'  style='width:15px;height: 15px;cursor:pointer;'></span>"
                        + " <span>"
                        + " <img src='images/Detele Red Icon.svg' onclick='deleteSelectedJob()' class='visionETLIcons' title='Delete' style='width:15px;height: 15px;cursor:pointer;'"
                        + " /></span>"
                        + " </div>";
                +" </div>";
//                var getTreeItem = $('#scheduledJobs').jqxTree('getSelectedItem');
//                var item = $("#scheduledJobs").jqxTree('getItem');
//                var selectedTreeItem = $("#scheduledJobs").jqxTree('getItem',this);
//                var selectedTreeItem = getTreeItem.label;

//                var item = $("#scheduledJobs").jqxTree('getSelectedItem');
//                        var selectedTreeItem = $(item).attr('label');
//                        


                var htmlElement = event.args.element;
                var getTreeData = $('#scheduledJobs').jqxTree('getItem', htmlElement);
                var selectedTreeItem = getTreeData.label;




                if (selectedTreeItem == 'Execution Plan')
                {
                    $("#scheduledJobsNTFIcon").hide();
                    $("#scheduledJobsNTFGrid").hide();
                    $("#scheduledJobsExPlanGrid").show();
                    $("#scheduledJobsExPlan").html(scheduledJobsExPlanDiv);
                    $("#scheduledJobsExPlan").show();
                    showLoader();
                    $.ajax({
                        datatype: "json",
                        type: "post",
                        traditional: true,
                        url: 'getExecutionPlanData',
                        cache: false,
                        data: {

                        },
                        success: function (response, status, xhr) {
                            stopLoader();
                            if (response != null) {
                                var test = JSON.stringify(response);
                                var source =
                                        {
                                            localdata: test,
                                            datatype: "json",
                                            datafields: [
                                                {name: 'TASK_NAME', type: 'string'},
                                                {name: 'START_DATE', type: 'string'},
                                                {name: 'START_TIME', type: 'string'},
                                                {name: 'END_DATE', type: 'string'},
                                                {name: 'END_TIME', type: 'string'},
                                                {name: 'STATUS', type: 'string'},
                                                {name: 'LOG_FILE', type: 'string'},
                                                {name: 'USER_NAME', type: 'string'},
                                                {name: 'DESCRIPTION', type: 'string'},
                                                {name: 'JOB_ID', type: 'string'},
                                                {name: 'CRON_TRIGGER', type: 'string'}
                                            ],
                                        };


                                var imagerenderer = function (row, datafield, value) {
                                    return '<img src="images/logicon.png" style="width:30px;height: 20px;cursor:pointer;">';
                                }
                                var dataAdapter = new $.jqx.dataAdapter(source, {
                                    loadComplete: function (test) { },
                                    loadError: function (xhr, status, error) { }
                                });
                                var cellclass = function (row, columnfield, STATUS) {
                                    if (STATUS == 'Ready to Run' || STATUS == 'Ready to Run(IN 30Mins)') {
                                        return 'yellow';
                                    } else if (STATUS == 'Running...') {
                                        return 'green';
                                    } else if (STATUS == 'STOP') {
                                        return 'red';
                                    }
                                }
                                $("#scheduledJobsExPlanGrid").jqxGrid(
                                        {
                                            width: "100%",
                                            height: '360',
                                            source: dataAdapter,
                                            theme: 'energyblue',
                                            pagesize: 50,
                                            sortable: true,
                                            pageable: true,
                                            autoheight: true,
                                            autoloadstate: false,
                                            autosavestate: false,
                                            columnsresize: true,
                                            columnsreorder: true,
                                            showfilterrow: true,
                                            filterable: true,
                                            selectionmode: 'checkbox',
                                            columns: [
                                                {text: 'TASK NAME', datafield: 'TASK_NAME', width: 150, align: 'center'},
                                                {text: 'START_DATE', datafield: 'START_DATE', width: 150, align: 'center'},
                                                {text: 'START_TIME', datafield: 'START_TIME', width: 150, align: 'center'},
                                                {text: 'END_DATE', datafield: 'END_DATE', width: 150, align: 'center'},
                                                {text: 'END_TIME', datafield: 'END_TIME', width: 150, align: 'center'},
                                                {text: 'STATUS', datafield: 'STATUS', cellclassname: cellclass, align: 'center', width: 150, cellsrenderer: function (row, colum, value) {
                                                        if (value == "TEMPLOCK") {
                                                            var cell = '<div style="margin-top:5px;">';
                                                            cell += '<div style="margin-left: 5px; float: left;">Temp Locked<img src="images/lockedIcon.jpg" style="width:30px;height: 20px;cursor:pointer;"></div>';
                                                            cell += '</div>';
                                                            return cell;
                                                        }
                                                        if (value == "PERLOCKED") {
                                                            var cell = '<div style="margin-top:5px;">';
                                                            cell += '<div style="margin-left: 5px; float: left;">Per Locked<img src="images/lockedIcon.jpg" style="width:30px;height: 20px;cursor:pointer;"></div>';
                                                            cell += '</div>';
                                                            return cell;
                                                        }
                                                    }, },
                                                {text: 'LOG_FILE', datafield: 'LOG_FILE', cellsrenderer: imagerenderer, align: 'center', cellsalign: 'center', width: 50},
                                                {text: 'USER_NAME', datafield: 'USER_NAME', width: 150, align: 'center'},
                                                {text: 'DESCRIPTION', datafield: 'DESCRIPTION', width: 150, align: 'center'},
                                                {text: 'JOB_ID', datafield: 'JOB_ID', width: 150, hidden: true},
                                                {text: 'CRON_TRIGGER', datafield: 'CRON_TRIGGER', width: 150, hidden: true}
                                            ]
                                        });

                                if (test != null && test != "[]" && test != " ") {
                                    processLogInterval = setInterval(function () {
                                        refreshIacData() // this will run after every 1 seconds
                                    }, 2000);
                                }
                                $('#scheduledJobsExPlanGrid').on('mouseup', function (event) {
                                    var dfName = $("#getIacData").val();
                                    var rightClick = isRightClick(event);
                                    if (rightClick != null && rightClick == true) {
                                        if (dfName != 'undefined' && dfName != null && dfName == "TASK_NAME") {
                                            var target = $(event.target).parents('li:first')[0];
                                            var selectedrowindex = $("#scheduledJobsExPlanGrid").jqxGrid('getselectedrowindex');
                                            var datarow = $("#scheduledJobsExPlanGrid").jqxGrid('getrowdata', selectedrowindex);
                                            if (datarow != null && datarow != 'undefined') {
                                                var jobId = datarow.JOB_ID;

                                                var sDate = datarow.START_DATE;
                                                var eDate = datarow.END_DATE;
                                                var cronExp = datarow.CRON_TRIGGER;
                                                var sTime = datarow.START_TIME;
                                                var eTime = datarow.END_TIME;
                                                var status = datarow.STATUS;
                                                if (status == "PERLOCKED" && status != "" && status != "undefined") {
                                                    var menuItems = "<li onclick=\"unLockJob('" + jobId + "','" + sDate + "','" + eDate + "','" + cronExp + "','" + sTime + "','" + eTime + "')\">Unlock Job</li>";
                                                    var menuHeight = 1;
                                                } else if (status == "STOP" || status == "Ready to Run" || status == "Running...") {
                                                    var menuItems = "<li onclick=\"reScheduleJobInIAC('" + jobId + "','" + sDate + "','" + eDate + "','" + cronExp + "','" + sTime + "','" + eTime + "')\">Re-Schedule</li>";
                                                    menuItems += "<li onclick=\"lockScheduledJob('" + jobId + "')\">Lock Job</li>";
                                                    menuItems += "<li onclick=\"addNotificationToSendMail('" + jobId + "')\">Notification</li>";
                                                    menuItems += "<li onclick=\"viewJobDetails('" + jobId + "')\">View Details</li>";
                                                    var menuHeight = 4;
                                                } else if (status == "TEMPLOCK" && status != "" && status != "undefined") {
                                                    var menuItems = "<li onclick=\"unLockJob('" + jobId + "','" + sDate + "','" + eDate + "','" + cronExp + "','" + sTime + "','" + eTime + "')\">Unlock Job</li>";
                                                    var menuHeight = 1;
                                                }
                                                $("#jqxMenu").remove();
                                                $(".visionMainPage").append("<div id='jqxMenu'><ul></ul></div>");
                                                $("#jqxMenu ul").html(menuItems);
                                                var contextMenu = $("#jqxMenu").jqxMenu({width: '120px', height: menuHeight * 27.5 + 'px', autoOpenPopup: false, mode: 'popup', theme: 'energyblue'}); // ravi start
                                                var scrollTop = $(window).scrollTop();
                                                var scrollLeft = $(window).scrollLeft();

                                                contextMenu.jqxMenu('open', parseInt(event.clientX) + 5 + scrollLeft, parseInt(event.clientY) + 5 + scrollTop);
                                                return false;
                                            }
//                                            } else {
//                                                showMesg("Select any one Scheduled Job");
//                                            }
                                        }
                                    }
                                });

                                $('#scheduledJobsExPlanGrid').on('cellclick', function (event) {
                                    var dfName = event.args.datafield;
                                    $("#getIacData").val(dfName);
                                });


                                $('#scheduledJobsExPlanGrid').on('cellclick', function (event) {
                                    var dfName = event.args.datafield;

                                    if (dfName != 'undefined' && dfName != null && dfName == "LOG_FILE") {
                                        var target = $(event.target).parents('li:first')[0];
                                        var selectedrowindex = $("#scheduledJobsExPlanGrid").jqxGrid('getselectedrowindex');
                                        var datarow = $("#scheduledJobsExPlanGrid").jqxGrid('getrowdata', selectedrowindex);
                                        if (datarow != null && datarow != 'undefined') {
                                            var jobId = datarow.JOB_ID;
//                                            openSelectedJobLog(jobId);
                                            openLogFileIAC(jobId);
                                        } else {
                                            showMesg("Select one Schedule Job for open log file");
                                        }
                                    }
                                });

                            }
                        },
                        error: function (e)
                        {
                            sessionTimeout(e);
                        }
                    });

                } else if (selectedTreeItem == 'Lock Scheduling Job')
                {
                    $("#scheduledJobsNTFIcon").hide();
                    $("#scheduledJobsNTFGrid").hide();

                    if (processLogInterval != null) {
                        clearInterval(processLogInterval);
                    }
                    $("#lokedJobsGrid").show();
                    $("#lokedJobsIcon").html(lockScheduleDiv);
                    $("#lokedJobsIcon").show();
                    showLoader();
                    $.ajax({
                        datatype: "json",
                        type: "post",
                        traditional: true,
                        url: 'getExecutionPlanData',
                        cache: false,
                        data: {

                        },
                        success: function (response, status, xhr) {
                            stopLoader();
                            if (response != null) {
                                var test = JSON.stringify(response);
                                var source =
                                        {
                                            localdata: test,
                                            datatype: "json",
                                            datafields: [
                                                {name: 'LOCKED_ITEM', type: 'string'},
                                                {name: 'LOCKER', type: 'string'},
                                                {name: 'LOCKING', type: 'string'},
                                                {name: 'STATUS', type: 'string'},
                                                {name: 'DESCRIPTION', type: 'string'}
                                            ],
                                        };
                                var dataAdapter = new $.jqx.dataAdapter(source, {
                                    loadComplete: function (test) { },
                                    loadError: function (xhr, status, error) { }
                                });
                                $("#scheduledJobsExPlanGrid").jqxGrid(
                                        {
                                            width: "100%",
                                            height: '360',
                                            source: dataAdapter,
                                            theme: 'energyblue',
                                            pagesize: 50,
                                            sortable: true,
                                            pageable: true,
                                            autoheight: true,
                                            autoloadstate: false,
                                            autosavestate: false,
                                            columnsresize: true,
                                            columnsreorder: true,
                                            showfilterrow: true,
                                            filterable: true,
                                            selectionmode: 'checkbox',

                                            columns: [

                                                {text: 'LOCKED_ITEM', datafield: 'LOCKED_ITEM', width: 300},
                                                {text: 'LOCKER', datafield: 'LOCKER', width: 200},
                                                {text: 'LOCKING', datafield: 'LOCKING', width: 400},
                                                {text: 'STATUS', datafield: 'STATUS', width: 200},
                                                {text: 'DESCRIPTION', datafield: 'DESCRIPTION', width: 200}

                                            ]

                                        });

                                $('#scheduledJobsExPlanGrid').on('cellclick', function (event) {
                                    var dfName = event.args.datafield;
                                    if (event.args.rightclick) {
                                        if (dfName != 'undefined' && dfName != null && dfName == "TASK_NAME") {
                                            var target = $(event.target).parents('li:first')[0];

                                            var jobId = datarow.JOB_ID;
                                            var menuItems = "<li onclick=\"reScheduleJobInIAC('" + jobId + "')\">RE-Schedule</li>";
                                            var menuHeight = 1;
                                            $("#jqxMenu").remove();
                                            $(".visionMainPage").append("<div id='jqxMenu'><ul></ul></div>");
                                            $("#jqxMenu ul").html(menuItems);
                                            var contextMenu = $("#jqxMenu").jqxMenu({width: '120px', height: menuHeight * 27.5 + 'px', autoOpenPopup: false, mode: 'popup', theme: 'energyblue'}); // ravi start
                                            var scrollTop = $(window).scrollTop();
                                            var scrollLeft = $(window).scrollLeft();
                                            contextMenu.jqxMenu('open', parseInt(event.clientX) + 5 + scrollLeft, parseInt(event.clientY) + 5 + scrollTop);
                                            return false;
                                        }
                                    }
                                });
                            }
                        },
                        error: function (e)
                        {
                            sessionTimeout(e);
                        }
                    });




                } else if (selectedTreeItem == 'Re_Scheduling Task')
                {

                } else if (selectedTreeItem == 'Notifications') {
                    $("#scheduledJobsExPlanGrid").hide();
                    $("#scheduledJobsExPlan").hide();

                    IACNotification();


                } else {

                    $("#scheduledJobsExPlanGrid").hide();
                    $("#scheduledJobsExPlan").hide();
                    $("#iTransFromsIAS").hide();
                    $("#lockDiv").hide();

                }

            });
        },
        error: function (e) {
            sessionTimeout(e);
            stopLoader();
        }
    });
}
function showSavedJobInIAC(newJobId) {
//    ajaxStart();
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'fetchSavedJobsForIAC',
        cache: false,
        data: {},
        success: function (response) {
            ajaxStop();
            var availableJobshtmlStr = response['scheduledJobsArray'];
            var source =
                    {
                        datatype: "json",
                        datafields: [
                            {name: 'id'},
                            {name: 'parentid'},
                            {name: 'text'},
                            {name: 'icon'},
                            {name: 'value'},
                            {name: 'datatype'}
                        ],
                        id: 'id',
                        icon: 'icon',
                        localdata: availableJobshtmlStr
                    };
            var dataAdapter = new $.jqx.dataAdapter(source);
            dataAdapter.dataBind();
            var records = dataAdapter.getRecordsHierarchy('id', 'parentid', 'items', [{name: 'text', map: 'label'}]);

            $("#dialog").html("<div class='treeSearchInputDiv'>\n\
    <input id='treeSearchValue' type='text' class='treeSearchValueInput' placeholder='Search'/>\n\
    <img id='treeNodeSearchIconId' src='images/icon.png' style='height:12px;width:12px;cursor:pointer;'\n\
 onclick=searchTreeNode('jobTree')  /><div id='searchTreeErrorMesg' style='color:red;'>\n\
</div></div>\n\
<div id='jobTree' class='columnMappingTree'></div>");
            $("#dialog").dialog({
                title: (labelObject['iTransform Jobs'] != null ? labelObject['iTransform Jobs'] : 'iTransform Saved Jobs'),
                modal: true,
                width: 300,
                height: '300',
                fluid: true,
                open: function () {
                    if (processLogInterval != null) {
                        clearInterval(processLogInterval);
                    }
                    $('#jobTree').jqxTree({source: records, width: 300,
                        toggleMode: 'click', theme: 'energyblue',
                        enableHover: true,
                        incrementalSearch: true,
                        keyboardNavigation: true
                    });
                    $("#treeSearchValue").keyup(function (event) {
                        if (event.keyCode === 13) {
                            $("#treeNodeSearchIconId").click();
                        }
                    });
                    $('#jobTree').jqxTree('expandItem', $("#jobTree").find('li:first')[0]);
                    $("#jobTree").on('mousedown', function (event) {
                        var target = $(event.target).parents('li:first')[0];
                        var rightClick = isRightClick(event);
                        if (rightClick && target != null) {
                            $("#jobTree").jqxTree('selectItem', target);
                            var selectedItem = $('#jobTree').jqxTree('getSelectedItem');
                            var jobId = selectedItem.id;
                            var menuItems = "<li onclick=\"scheduleJobInIAC('" + jobId + "')\">Schedule</li>";
                            var menuHeight = 1;
                            $("#jqxMenu").remove();
                            $(".visionMainPage").append("<div id='jqxMenu'><ul></ul></div>");
                            $("#jqxMenu ul").html(menuItems);
                            var contextMenu = $("#jqxMenu").jqxMenu({width: '120px', height: menuHeight * 27.5 + 'px', autoOpenPopup: false, mode: 'popup', theme: 'energyblue'}); // ravi start
                            var scrollTop = $(window).scrollTop();
                            var scrollLeft = $(window).scrollLeft();
                            contextMenu.jqxMenu('open', parseInt(event.clientX) + 5 + scrollLeft, parseInt(event.clientY) + 5 + scrollTop);
                            return false;
                        }
                    });
                },
                beforeClose: function (event, ui)
                {
                    $(".visionHeaderMain").css("z-index", "99999");
                    $(".visionFooterMain").css("z-index", "99999");

                }, close: function (event, ui)
                {

                    $(this).html("");
                    $(this).dialog("close");
                    $(this).dialog("destroy");
                },
                buttons: {
                    'Submit': function () {
                        processLogInterval = setInterval(function () {
                            refreshIacData() // this will run after every 1 seconds
                        }, 2000);
                        var sHour = $("#iACstartTime").val();
                        var sMin = $("#iACendTime").val();
                        var sTime = sHour + ":" + sMin;
                        var cronExpBeforeSchedule = $("#iAC_CronExpBef").val();
                        if (cronExpBeforeSchedule == null) {
                            cronExpBeforeSchedule = " ";
                        }
                        $("#iACendTime").val();
                        var iStartDate = $("#iACstartDate").val();
                        var iEndDate = $("#iACendDate").val();
                        var iStartTime = $("#iACstartTime").val();
                        var cronExp = $("#iAC_CronExp").val();
                        var roleId = $("#ROLE_ID").val();
                        var items = $("#jobTree").jqxTree('getSelectedItem');
                        var value = items.value;
                        var id = items.id;
                        if (iStartDate != null && iStartDate != '' && iStartDate != 'undefined') {
                            $.ajax({
                                type: "post",
                                traditional: true,
                                dataType: 'html',
                                url: "insertSelectedTreeData",
                                cache: false,

                                data: {
                                    taskName: value,
                                    jobId: id,
                                    roleId: roleId,
                                    iStartDate: iStartDate,
                                    iEndDate: iEndDate,
                                    iStartTime: iStartTime,
                                    cronExp: cronExp,
                                    sTime: sTime,
                                    cronExpBeforeSchedule: cronExpBeforeSchedule
                                },
                                success: function (data) {
                                    stopLoader();
//                                    var dialogSplitMessage = dialogSplitIconText(response['message'], "Y");
                                    $("#logoutDailog").html(data);
                                    $("#logoutDailog").dialog({
                                        title: 'Message',
                                        modal: true,
                                        height: 'auto',
                                        minHeight: 'auto',
                                        minWidth: 370,
                                        maxWidth: 'auto',
                                        fluid: true,
                                        buttons: [{
                                                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                                                click: function () {
                                                    $(this).html("");
                                                    $(this).dialog("close");
                                                    $(this).dialog("destroy");
                                                    refreshIacData();
                                                }}],
                                        open: function () {
                                            $("#iACstartDate").val('');
                                            $("#iACendDate").val('');
                                            $("#iACstartTime").val('');
                                            $("#iACendTime").val('');
                                            $("#iAC_CronExp").val('');
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
                                },
                                error: function (e)
                                {
                                    sessionTimeout(e);
                                }

                            });

                        } else {
                            showMesg("Right Click on Job To Schedule");
                        }

//                        var checkedItems = new Array();
//                        $.each(items, function () {
//                            if (this.checked) {
//                                checkedItems[checkedItems.length] = this.value;
//                            }
//                        });
//                        console.log(checkedItems); // here is where I would submit the checkedItem codes
//                        $(this).dialog("close");
                        $(this).dialog("close");

                    },
                    'Cancel': function () {
                        $(this).dialog("close");
                    }

//                    },

                }



            });
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });
}
function scheduleJobInIAC(jobId) {
    var jobName = $("#" + jobId).find("span").text();
    console.log("jobName::" + $.trim(jobName));
    if (jobId != null && jobId != ''
            && jobName != null && jobName != '') {
        jobName = $.trim(jobName);
        var scheduleForm = "<div class='cronStartDateDiv'>"
                + "<table><tr>"
                + "<td>Start Date</td>"
                + "<td><input type='text' id='cronStartDate' readonly='true'  /></td>"
                + "<td>End Date</td>"
                + "<td><input type='text' id='cronEndDate' readonly='true'  /></td>"
                + "</tr>"
                + "<tr>"
//                + "<td>Start Time</td>"
//                + "<td><input type='text' id='cronStartTime' readonly='true'  /></td>"
//                + "<td>End Time</td>"
//                + "<td><input type='text' id='cronEndTime' /></td>"
                + "</tr></table>"
                + "</div>"
                + "<div id='cronExpressionDiv' class='cron-builder'></div>"
                + "<div class='cron-expression'>"
                + " <p>Cron Expression:<input type='text' id='cronExpression' readonly='true' /></p>"
                + "<input type='hidden' id='cronExpressionBef' readonly='true' />"
                + "</div>";
        $("#logoutDailog").html(scheduleForm);
        $("#logoutDailog").dialog({
            title: (labelObject[jobName] != null ? labelObject[jobName] : jobName),
            modal: true,
            height: 250,
            minWidth: '650',
            maxWidth: 'auto',
            fluid: true,
            buttons: [{
                    text: (labelObject['Schedule'] != null ? labelObject['Schedule'] : 'Schedule'),
                    click: function () {
                        var cronExp = $('#cronExpression').val();
                        if (cronExp != null && cronExp != '') {
                            cronExp = cronExp.substring(0, cronExp.lastIndexOf(" "));
                            var cronStartDate = $("#cronStartDate").val();
                            var cronExpressionBef = $("#cronExpressionBef").val();
                            if (cronExpressionBef != null && cronExpressionBef != '') {
                                cronExpressionBef = cronExpressionBef.substring(0, cronExpressionBef.lastIndexOf(" "));
                                $("#iAC_CronExpBef").val(cronExpressionBef);
                            }
                            var type = $('.cron-period-select').val();
                            $('#iacType').val(type);
                            var hours = $('.cron-hourly-hour').val();
                            var min = $('.cron-hourly-minute').val();
                            var selectedText = $(".cron-period-select option:selected").text();
                            if (hours && min == 0 && selectedText != "Minutes") {
                                hours = $('.cron-clock-hour').val();
                                min = $('.cron-clock-minute').val();
                                $("#iACstartTime").val(hours);
                                $("#iACendTime").val(min);
                            }
                            $("#iACstartTime").val(hours);
                            $("#iACendTime").val(min);
                            $("#iACstartDate").val(cronStartDate);
                            $("#iACstartDate").attr("sDate", cronStartDate);
                            var cronEndDate = $("#cronEndDate").val();
                            $("#iACendDate").val(cronEndDate);
                            $("#iACendDate").attr("eTime", cronEndDate);
                            $("#iAC_CronExp").val(cronExp);
                            $("#iACstart").val()
                            console.log("cronExp:::" + cronExp);
                            $(this).html("");
                            $(this).dialog("close");
                            $(this).dialog("destroy");
//                            scheduleProcessJobIAC(cronExp, jobId, jobName, cronStartDate, cronEndDate);
                        }
                    }},
                {
                    text: (labelObject['Cancel'] != null ? labelObject['Cancel'] : 'Cancel'),
                    click: function () {
                        $(this).html("");
                        $(this).dialog("close");
                        $(this).dialog("destroy");

                    }}],
            open: function () {
                if (processLogInterval != null) {
                    clearInterval(processLogInterval);
                }
                $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                $(this).closest(".ui-dialog").find(".ui-button").eq(2).addClass("dialogno");
                $(".visionHeaderMain").css("z-index", "999");
                $(".visionFooterMain").css("z-index", "999");
                $('#cronExpressionDiv').cronBuilder({
                    selectorLabel: "Select time period:  ",
                    onChange: function (expression) {
                        var getData = expression.join(" ");
                        $.each(expression, function () {
                            var sec = expression[0];
                            var min = expression[1];
                            var hour = expression[2];
                            var dom = expression[3];
                            var month = expression[4];
                            var dow = expression[5];
                            var year = expression[6];
//                            
                            if (hour != null && hour != "*") {
                                if (min != "*" && min > 30 || min == 30) {
                                    min = min - 30;
                                } else {
                                    hour = hour - 1;
                                    min = (min - 30) + 60;
                                }
                                $('#cronExpressionBef').val(sec + " " + min + " " + hour + " " + dom + " " + month + " " + dow + " " + year);
                            }

                        });

                        $('#cronExpression').val(getData);
                    }
                });
//                 $('#cronExpressionDiv').cronBuilderBeforeSchedule({
//                    onChange: function (expression) {
//                        $('#cronExpressionBef').val(expression);
//                    }
//                });
                $("#cronStartDate").datepicker({
                    changeMonth: true,
                    changeYear: true,
                    dateFormat: "dd-mm-yy",
                    showOn: "button",
                    buttonImage: 'images/date_picker_icon.png',
                    buttonImageOnly: true,
                });
                $("#cronStartDate").datepicker("setDate", new Date());
                $("#cronStartDate").addClass("ui-datepickerJobSchedule");

                $("#cronEndDate").datepicker({
                    changeMonth: true,
                    changeYear: true,
                    dateFormat: "dd-mm-yy",
                    showOn: "button",
                    buttonImage: 'images/date_picker_icon.png',
                    buttonImageOnly: true
                });
                $("#ui-datepicker-div").addClass("ui-datepickerJobSchedule");
                $('#cronStartTime').timepicker();
                $('#cronEndTime').timepicker();
            },
            beforeClose: function (event, ui)
            {
                $(".visionHeaderMain").css("z-index", "99999");
                $(".visionFooterMain").css("z-index", "99999");
            }
        });
    }
}
function refreshIacData() {
//    showLoader();
    $.ajax({
        datatype: "json",
        type: "post",
        traditional: true,
        url: 'getExecutionPlanData',
        cache: false,
        data: {
        },
        success: function (response, status, xhr) {
//            stopLoader();
            if (response != null) {
                var test = JSON.stringify(response);
                var source =
                        {
                            localdata: test,
                            datatype: "json",
                            datafields: [
                                {name: 'TASK_NAME', type: 'string'},
                                {name: 'START_DATE', type: 'string'},
                                {name: 'START_TIME', type: 'string'},
                                {name: 'END_DATE', type: 'string'},
                                {name: 'END_TIME', type: 'string'},
                                {name: 'STATUS', type: 'string'},
                                {name: 'LOG_FILE', type: 'string'},
                                {name: 'USER_NAME', type: 'string'},
                                {name: 'DESCRIPTION', type: 'string'},
                                {name: 'JOB_ID', type: 'string'},
                                {name: 'CRON_TRIGGER', type: 'string'}
                            ],
                        };
                var cellclass = function (row, columnfield, STATUS) {
                    if (STATUS == 'Ready to Run' || STATUS == 'Ready to Run(IN 30Mins)') {
                        return 'yellow';
                    } else if (STATUS == 'Running...') {
                        return 'green';
                    } else if (STATUS == 'STOP') {
                        return 'red';
                    } else if (STATUS == 'LOCKED') {
                        return '<img src="images/logicon.png" style="width:30px;height: 20px;cursor:pointer;">';
                    }
                }
                var imagerenderer = function (row, datafield, value) {
                    return '<img src="images/logicon.png" style="width:30px;height: 20px;cursor:pointer;">';
                }
                var dataAdapter = new $.jqx.dataAdapter(source, {
                    loadComplete: function (test) { },
                    loadError: function (xhr, status, error) { }
                });
                $("#scheduledJobsExPlanGrid").jqxGrid(
                        {
                            width: "100%",
                            height: '360',
                            source: dataAdapter,
                            theme: 'energyblue',
                            pagesize: 50,
                            sortable: true,
                            pageable: true,
                            autoheight: true,
                            autoloadstate: false,
                            autosavestate: false,
                            columnsresize: true,
                            columnsreorder: true,
                            showfilterrow: true,
                            filterable: true,
                            selectionmode: 'checkbox',
                            columns: [
                                {text: 'TASK NAME', datafield: 'TASK_NAME', width: 150, align: 'center'},
                                {text: 'START_DATE', datafield: 'START_DATE', width: 150, align: 'center'},
                                {text: 'START_TIME', datafield: 'START_TIME', width: 150, align: 'center'},
                                {text: 'END_DATE', datafield: 'END_DATE', width: 150, align: 'center'},
                                {text: 'END_TIME', datafield: 'END_TIME', width: 150, align: 'center'},
                                {text: 'STATUS', datafield: 'STATUS', width: 150, align: 'center', cellclassname: cellclass,
                                    cellsrenderer: function (row, colum, value) {
                                        if (value == "TEMPLOCK") {
                                            var cell = '<div style="margin-top:5px;">';
                                            cell += '<div style="margin-left: 5px; float: left;">Temp Locked<img src="images/lockedIcon.jpg" style="width:30px;height: 20px;cursor:pointer;"></div>';
                                            cell += '</div>';
                                            return cell;
                                        }
                                        if (value == "PERLOCKED") {
                                            var cell = '<div style="margin-top:5px;">';
                                            cell += '<div style="margin-left: 5px; float: left;">Per Locked<img src="images/lockedIcon.jpg" style="width:30px;height: 20px;cursor:pointer;"></div>';
                                            cell += '</div>';
                                            return cell;
                                        }

                                    }},
                                {text: 'LOG_FILE', datafield: 'LOG_FILE', cellsrenderer: imagerenderer, width: 50, align: 'center'},
                                {text: 'USER_NAME', datafield: 'USER_NAME', width: 150, align: 'center'},
                                {text: 'DESCRIPTION', datafield: 'DESCRIPTION', width: 150, align: 'center'},
                                {text: 'JOB_ID', datafield: 'JOB_ID', width: 150, hidden: true},
                                {text: 'CRON_TRIGGER', datafield: 'CRON_TRIGGER', width: 150, hidden: true}
                            ]

                        });
            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }
    });
}
function deleteSelectedJob() {
    showLoader();
    var selectedrowindex = $("#scheduledJobsExPlanGrid").jqxGrid('getselectedrowindex');
    var datarow = $("#scheduledJobsExPlanGrid").jqxGrid('getrowdata', selectedrowindex);
    if (datarow != null && datarow != 'undefined') {
        var jobId = datarow.JOB_ID;
        $.ajax({
            url: "deleteSelectedJob",
            traditional: true,
            type: "POST",
            dataType: 'html',
            cache: false,
            traditional: true,
            cache: false,
            data: {
                jobId: jobId

            },
            success: function (response) {
                stopLoader();
                if (response != null && response != '') {
                    if (processLogInterval != null) {
                        clearInterval(processLogInterval);
                    }
                    $("#dialog").html(response);
                    $("#dialog").dialog({
                        title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
                        modal: true,
                        height: 'auto',
                        minHeight: 'auto',
                        minWidth: 300,
                        maxWidth: 'auto',
                        fluid: true,
                        buttons: [{
                                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                                click: function () {
                                    if (response) {
                                        $(this).html("");
                                        $(this).dialog("close");
                                        $(this).dialog("destroy");
                                        refreshIacData();

                                    }
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
                stopLoader();
            }


        });
    } else {
        showMesg("Select only one Scheduled Job to delete");
        stopLoader();
    }
}
function isRightClickIAC(event) {
    if (event.args.rightclick) {
        return true;
    }
}

function reScheduleJobInIAC(jobId, sDate, eDate, cronExp, sTime) {
    var jobName = $("#" + jobId).find("span").text();
    console.log("jobName::" + $.trim(jobName));
    if (jobId != null && jobId != ''
            && jobName != null && jobName != '') {
        jobName = $.trim(jobName);
        var scheduleForm = "<div class='cronStartDateDiv'>"
                + "<table><tr>"
                + "<td>Start Date</td>"
                + "<td><input type='text' id='cronStartDate' readonly='true'  /></td>"
                + "<td>End Date</td>"
                + "<td><input type='text' id='cronEndDate' readonly='true'  /></td>"
                + "</tr>"
                + "<tr>"
//                + "<td>Start Time</td>"
//                + "<td><input type='text' id='cronStartTime' readonly='true'  /></td>"
//                + "<td>End Time</td>"
//                + "<td><input type='text' id='cronEndTime' /></td>"
                + "</tr></table>"
                + "</div>"
                + "<div id='cronExpressionDiv' class='cron-builder'></div>"
                + "<div class='cron-expression'>"
                + " <p>Cron Expression:<input type='text' id='cronExpression' readonly='true' /></p>"
                + "<input type='hidden' id='cronExpressionBef' readonly='true' />"
                + "</div>";
        $("#logoutDailog").html(scheduleForm);
        $("#logoutDailog").dialog({
            title: (labelObject[jobName] != null ? labelObject[jobName] : jobName),
            modal: true,
            height: 250,
            minWidth: '650',
            maxWidth: 'auto',
            fluid: true,
            buttons: [{
                    text: (labelObject['Schedule'] != null ? labelObject['Schedule'] : 'Schedule'),
                    click: function () {
                        var cronExp = $('#cronExpression').val();
                        if (cronExp != null && cronExp != '') {
                            var type = $('.cron-period-select').val();
                            $('#iacType').val(type);
                            var cronExpBef = $("#cronExpressionBef").val();
                            cronExpBef = cronExpBef.substring(0, cronExpBef.lastIndexOf(" "));
                            var hours = $('.cron-hourly-hour').val();
                            var min = $('.cron-hourly-minute').val();
                            var selectedText = $(".cron-period-select option:selected").text();
                            if (hours && min == 0 && selectedText != "Minutes") {
                                hours = $('.cron-clock-hour').val();
                                min = $('.cron-clock-minute').val();
                                $("#iACstartTime").val(hours);
                                $("#iACendTime").val(min);
                            }
                            $("#iACstartTime").val(hours);
                            $("#iACendTime").val(min);
                            var sTime = hours + ":" + min;
                            cronExp = cronExp.substring(0, cronExp.lastIndexOf(" "));
                            var cronStartDate = $("#cronStartDate").val();
                            var cronEndDate = $("#cronEndDate").val();
                            console.log("cronExp:::" + cronExp);
                            $(this).html("");
                            $(this).dialog("close");
                            $(this).dialog("destroy");
                            var selectedrowindex = $("#scheduledJobsExPlanGrid").jqxGrid('getselectedrowindex');
                            var datarow = $("#scheduledJobsExPlanGrid").jqxGrid('getrowdata', selectedrowindex);
                            if (datarow != null && datarow != 'undefined') {
                                var jobId = datarow.JOB_ID;
                                showLoader();
                                $.ajax({
                                    datatype: "html",
                                    type: "post",
                                    traditional: true,
                                    url: 'updateScheduleJob',
                                    cache: false,
                                    data: {
                                        jobId: jobId,
                                        sDate: cronStartDate,
                                        eDate: cronEndDate,
                                        cronExp: cronExp,
                                        sTime: sTime,
                                        cronExpBeforeSchedule: cronExpBef
                                    },
                                    success: function (response) {
                                        stopLoader();
                                        if (response != null && response != '') {
//                                            var dialogSplitMessage = dialogSplitIconText(response['message'], "Y");
                                            $("#dialog").html(response);
                                            $("#dialog").dialog({
                                                title: 'Message',
                                                modal: true,
                                                height: 'auto',
                                                minHeight: 'auto',
                                                minWidth: 370,
                                                maxWidth: 'auto',
                                                fluid: true,
                                                buttons: [{
                                                        text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                                                        click: function () {
                                                            $(this).html("");
                                                            $(this).dialog("close");
                                                            $(this).dialog("destroy");
                                                            $("#iACstartDate").val('');
                                                            $("#iACendDate").val('');
                                                            $("#iACstartTime").val('');
                                                            $("#iACendTime").val('');
                                                            $("#iAC_CronExp").val('');
                                                            var count = 0;
                                                            var interval = setInterval(function () {
                                                                // increasing the count by 1
                                                                count += 1;
                                                                // when count equals to 5, stop the function
                                                                if (count === 5) {
                                                                    clearInterval(interval);
                                                                }
                                                                refreshIacData();
                                                            }, 2000);

                                                        }}],
                                                open: function () {
                                                    if (processLogInterval != null) {
                                                        clearInterval(processLogInterval);
                                                    }
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
                                    },
                                    error: function (e)
                                    {
                                        sessionTimeout(e);
                                    }
                                });

                            }

//                            scheduleProcessJob(cronExp, jobId, jobName, cronStartDate, cronEndDate);
                        }
                    }},
                {
                    text: (labelObject['Cancel'] != null ? labelObject['Cancel'] : 'Cancel'),
                    click: function () {
                        $(this).html("");
                        $(this).dialog("close");
                        $(this).dialog("destroy");

                    }}],
            open: function () {
//                $('#cronExpression').val(cronExp);
//                $("#cronStartDate").val(sDate);
//                $("#cronEndDate").val(eDate);
                $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                $(this).closest(".ui-dialog").find(".ui-button").eq(2).addClass("dialogno");
                $(".visionHeaderMain").css("z-index", "999");
                $(".visionFooterMain").css("z-index", "999");
                $('#cronExpressionDiv').cronBuilder({
                    selectorLabel: "Select time period:  ",
                    onChange: function (expression) {
                        var getData = expression.join(" ");
                        $.each(expression, function () {
                            var sec = expression[0];
                            var min = expression[1];
                            var hour = expression[2];
                            var dom = expression[3];
                            var month = expression[4];
                            var dow = expression[5];
                            var year = expression[6];
//                            
                            if (hour != null && hour != "*") {
                                if (min != "*" && min > 30 || min == 30) {
                                    min = min - 30;
                                } else {
                                    hour = hour - 1;
                                    min = (min - 30) + 60;
                                }
                                $('#cronExpressionBef').val(sec + " " + min + " " + hour + " " + dom + " " + month + " " + dow + " " + year);
                            }

                        });

                        $('#cronExpression').val(getData);
                    }
                });
                $("#cronStartDate").datepicker({
                    changeMonth: true,
                    changeYear: true,
                    dateFormat: "dd-mm-yy",
                    showOn: "button",
                    buttonImage: 'images/date_picker_icon.png',
                    buttonImageOnly: true,
                });
                $("#cronStartDate").datepicker("setDate", new Date());
                $("#cronStartDate").addClass("ui-datepickerJobSchedule");

                $("#cronEndDate").datepicker({
                    changeMonth: true,
                    changeYear: true,
                    dateFormat: "dd-mm-yy",
                    showOn: "button",
                    buttonImage: 'images/date_picker_icon.png',
                    buttonImageOnly: true
                });
                $("#ui-datepicker-div").addClass("ui-datepickerJobSchedule");
                $('#cronStartTime').timepicker();
                $('#cronEndTime').timepicker();
            },
            beforeClose: function (event, ui)
            {
                $(".visionHeaderMain").css("z-index", "99999");
                $(".visionFooterMain").css("z-index", "99999");
            }
        });
    }
}
function openLogFileIAC(jobId) {
    var processLogTable = "<div id='processLogDiv' class='LogTableMain'>"
            + "<div id=''> "
            + " <div class='logIconDiv'>"
            + "<img src=\"images/StopPocessJob.svg\" class=\"visionETLIcons\" title=\"Cancel Job Execution\" "
            + " style=\"width:15px;height: 15px;cursor:pointer;\""
            + " onclick='cancellCurrentJob()'/>"
            + "<img src=\"images/Refresh Icon.svg\" class=\"visionETLIcons\" title=\"Refresh log\" "
            + " style=\"width:15px;height: 15px;cursor:pointer;margin-left:5px;\""
            + " onclick='refreshLogFile()'/>"
            + "</div>"
            + "</div"
            + "<input type='hidden' id='currentProcesslogDate'/><input type='hidden' id='currentProcesslogIndex'/>"//currentProcesslogIndex
            + "<table id='processlogTable' class='logtable' style='width:100%'>"
            + "<thead>"
            + "<tr>"
            + "<th width='5%'></th>"
            + "<th width='25%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;'>Time Stamp</th>"
            + "<th width='70%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;'>Message</th>"
            + "</tr>"
            + "</thead>"
            + "<tbody>"
            + "</tbody>"
            + "</table>"
            + "</div>";
    $("#dialogLogFile").html(processLogTable);
    $("#dialogLogFile").dialog({
        title: (labelObject['Log File'] != null ? labelObject['Log File'] : 'Log File'),
        modal: true,
        width: 'auto',
        maxWidth: 500,
        height: 'auto',
        maxHeight: 500,
        fluid: true,
        buttons: [{
                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                click: function () {
                    if (processLogInterval != null) {
                        clearInterval(processLogInterval);
                    }
                    $(this).html("");
                    $(this).dialog("close");
                    $(this).dialog("destroy");
                }

            }],
        open: function () {
            stopLoader();
            ajaxStop();
            processLogInterval = setInterval(function () {
                refreshLogFileInIAC(jobId) // this will run after every 1 seconds
            }, 2000);
            $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
            $(".visionHeaderMain").css("z-index", "999");
            $(".visionFooterMain").css("z-index", "999");
            $(".ui-dialog").addClass('visionDMTreePopup');
//                            $("#dataMigrationTabs").jqxTabs({width: "100%", height: "130px", position: 'top', theme: 'ui-redmond', reorder: true});
//
//                            $('#dataMigrationTabs').unbind('selected').on('selected', function (event) {
//                                $('#iconsdiv').attr('style', 'margin-top:4px !important');
//                            });
//                            $("#dataMigrationTabs").jqxTabs({width: "100%", height: "100%", position: 'top', theme: 'ui-redmond', reorder: true});

        },
        beforeClose: function (event, ui)
        {

            $(".visionHeaderMain").css("z-index", "99999");
            $(".visionFooterMain").css("z-index", "99999");

        }, close: function (event, ui)
        {
            if (processLogInterval != null) {
                clearInterval(processLogInterval);
            }
        }
    });

}
function  refreshLogFileInIAC(jobId) {
    stopLoader();
    var currentProcesslogDate = $("#currentProcesslogDate").val();
    var currentProcesslogIndex = $("#currentProcesslogIndex").val();
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'html',
        cache: false,
        url: 'refreshProcessLog',
        async: true,
        data: {
            currentProcesslogDate: ((currentProcesslogDate != null && currentProcesslogDate != '') ? currentProcesslogDate : ""),
            currentProcesslogIndex: ((currentProcesslogIndex != null && currentProcesslogIndex != '') ? currentProcesslogIndex : ""),
            jobId: jobId
        },
        success: function (response) {
            if (response != null && response != '') {
                var resultObj = JSON.parse(response);
                if (resultObj != null && !jQuery.isEmptyObject(resultObj)) {
                    $("#currentProcesslogIndex").val(resultObj['currentProcesslogIndex']);
                    var logTxt = resultObj['logTxt'];
                    if (logTxt != null && logTxt != '') {
                        $("#processlogTable tbody").append(logTxt);
                        $("#currentProcesslogDate").val(resultObj['currentProcesslogDate']);
                        if (resultObj['processFlag'] == 'N') {
                            clearInterval(processLogInterval);
                        }
                    } else {

                    }
                } else {

                }
            } else {

            }
        },
        error: function (e)
        {
            sessionTimeout(e);
            stopLoader();
            if (processLogInterval != null) {
                clearInterval(processLogInterval);
            }
        }

    });
}

function perLockScheduledJob(jobId) {
    showLoader();
    $.ajax({
        datatype: "html",
        type: "post",
        traditional: true,
        url: 'lockScheduleJob',
        cache: false,
        data: {
            jobId: jobId

        },
        success: function (response) {
            stopLoader();
            if (response != null && response != '') {
//                                            var dialogSplitMessage = dialogSplitIconText(response['message'], "Y");
                $("#dialog").html(response);
                $("#dialog").dialog({
                    title: 'Permenent Lock job',
                    modal: true,
                    height: 'auto',
                    minHeight: 'auto',
                    minWidth: 370,
                    maxWidth: 'auto',
                    fluid: true,
                    buttons: [{
                            text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                            click: function () {
                                $(this).html("");
                                $(this).dialog("close");
                                $(this).dialog("destroy");
                                processLogInterval = setInterval(function () {
                                    refreshIacData() // this will run after every 1 seconds
                                }, 2000);

                            }}],
                    open: function () {
                        if (processLogInterval != null) {
                            clearInterval(processLogInterval);
                        }
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
        },
        error: function (e)
        {
            sessionTimeout(e);
        }
    });
}
function unLockJob(jobId, sDate, eDate, cronExp, sTime) {
    showLoader();
    $.ajax({
        datatype: "html",
        type: "post",
        traditional: true,
        url: 'unlockScheduleJob',
        cache: false,
        data: {
            jobId: jobId

        },
        success: function (response) {
            stopLoader();
            if (response != null && response != '') {
//                                            var dialogSplitMessage = dialogSplitIconText(response['message'], "Y");
                $("#dialog").html(response);
                $("#dialog").dialog({
                    title: 'Message',
                    modal: true,
                    height: 'auto',
                    minHeight: 'auto',
                    minWidth: 370,
                    maxWidth: 'auto',
                    fluid: true,
                    buttons: [{
                            text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                            click: function () {
                                processLogInterval = setInterval(function () {
                                    refreshIacData() // this will run after every 1 seconds
                                }, 2000);
                                $(this).html("");
                                $(this).dialog("close");
                                $(this).dialog("destroy");


                            }}],
                    open: function () {
                        if (processLogInterval != null) {
                            clearInterval(processLogInterval);
                        }
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
        },
        error: function (e)
        {
            sessionTimeout(e);
        }
    });
}
function lockScheduledJob(jobId) {

    var selectRadioButton = "<input type='radio' id='TempLock' name='lock' value='TempLock'> <label for='TempLock'>Temporary Lock</label><br><input type='radio' id='perLock' name='lock' value='perLock'><label for='perLock'>Permanent Lock</label><br>";

    $("#dialog").html(selectRadioButton);

    $("#dialog").dialog({
        title: 'Select Lock Type',
        modal: true,
        height: 'auto',
        minHeight: 'auto',
        minWidth: 370,
        maxWidth: 'auto',
        fluid: true,

        buttons: [{

                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),

                click: function () {

                    var radioVal = $("input[name='lock']:checked").val();
                    if (radioVal == "TempLock") {
                        openDilogTempLockScheduledJob(jobId);

                    } else if (radioVal == "perLock") {
                        perLockScheduledJob(jobId);
                    }
                    $(this).html("");
                    $(this).dialog("close");
                    $(this).dialog("destroy");
                }
            },
            {
                text: (labelObject['Cancel'] != null ? labelObject['Cancel'] : 'Cancel'),
                click: function () {
                    $(this).html("");
                    $(this).dialog("close");
                    $(this).dialog("destroy");

                }}],
        open: function () {
            if (processLogInterval != null) {
                clearInterval(processLogInterval);
            }
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


function viewJobDetails(jobId) {
    var jobName = $("#" + jobId).find("span").text();
    var showData = " <label for='tname'>Task Name:</label>\n\
<input style='border: 0px none;' type='text' id='tname' readonly='true' name='tname' ><br><label for='SchType'>Schedule Type:</label>\n\
<input style='border: 0px none;' type='text' id='SchType' readonly='true' name='SchType'><br><br>";
    $("#dialog").html(showData);
    $("#dialog").dialog({
        title: 'View Job Details',
        modal: true,
        height: 'auto',
        minHeight: 'auto',
        minWidth: 370,
        maxWidth: 'auto',
        fluid: true,
        buttons: [{
                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                click: function () {
                    processLogInterval = setInterval(function () {
                        refreshIacData() // this will run after every 1 seconds
                    }, 2000);
                    $(this).html("");
                    $(this).dialog("close");
                    $(this).dialog("destroy");
                }
            }
        ],
        open: function () {
            if (processLogInterval != null) {
                clearInterval(processLogInterval);
            }
            var type = $('#iacType').val();
            $("#SchType").val(type);
            $("#tname").val(jobName);
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
function openDilogTempLockScheduledJob(jobId) {
    var jobName = $("#" + jobId).find("span").text();
    console.log("jobName::" + $.trim(jobName));
    if (jobId != null && jobId != ''
            && jobName != null && jobName != '') {
        jobName = $.trim(jobName);
        var scheduleForm = "<div class='cronStartDateDiv'>"
                + "<table><tr>"
                + "<td>Select Date to Unlock:</td>"
                + "<td><input type='text' id='cronStartDate' readonly='true'  /></td>"
//                + "<td>End Date</td>"
                + "<td><input type='hidden' id='cronEndDate' /></td>"
                + "<td></td>"
                + "</tr></table>"
                + "</div>"
                + "<div id='cronExpressionDiv' class='cron-builder'></div>"
                + "<div class='cron-expression'>"
                + " <p>Cron Expression:<input type='text' id='cronExpression' readonly='true' /></p>"
                + "</div>";
        $("#logoutDailog").html(scheduleForm);
        $("#logoutDailog").dialog({
            title: "Temperary lock job",
            modal: true,
            height: 250,
            minWidth: '650',
            maxWidth: 'auto',
            fluid: true,
            buttons: [{
                    text: (labelObject['Lock'] != null ? labelObject['Lock'] : 'Lock'),
                    click: function () {
                        processLogInterval = setInterval(function () {
                            refreshIacData() // this will run after every 1 seconds
                        }, 2000);
                        var cronExp = $('#cronExpression').val();
                        var lockDate = $('#cronStartDate').val();
                        if (cronExp != null && cronExp != '') {
                            cronExp = cronExp.substring(0, cronExp.lastIndexOf(" "));
                            console.log("cronExp:::" + cronExp);
                            $(this).html("");
                            $(this).dialog("close");
                            $(this).dialog("destroy");
                            temperaroryLockScheduledJob(jobId, cronExp, lockDate);
                        }
                    }},
                {
                    text: (labelObject['Cancel'] != null ? labelObject['Cancel'] : 'Cancel'),
                    click: function () {
                        $(this).html("");
                        $(this).dialog("close");
                        $(this).dialog("destroy");
                    }}],
            open: function () {
                if (processLogInterval != null) {
                    clearInterval(processLogInterval);
                }
                $("#cronStartDate").datepicker({
                    changeMonth: true,
                    changeYear: true,
                    dateFormat: "dd-mm-yy",
                    showOn: "button",
                    buttonImage: 'images/date_picker_icon.png',
                    buttonImageOnly: true,
                });
                $("#cronStartDate").datepicker("setDate", new Date());
                $("#cronStartDate").addClass("ui-datepickerJobSchedule");

//                        $("#cronEndDate").datepicker({
//                            changeMonth: true,
//                            changeYear: true,
//                            dateFormat: "dd-mm-yy",
//                            showOn: "button",
//                            buttonImage: 'images/date_picker_icon.png',
//                            buttonImageOnly: true
//                        });
                $("#ui-datepicker-div").addClass("ui-datepickerJobSchedule");

                $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                $(this).closest(".ui-dialog").find(".ui-button").eq(2).addClass("dialogno");
                $(".visionHeaderMain").css("z-index", "999");
                $(".visionFooterMain").css("z-index", "999");

                $('#cronExpressionDiv').LOCK({
                    selectorLabel: " ",
                    onChange: function (expression) {
                        var getData = expression.join(" ");
                        $('#cronExpression').val(getData);
                    }
                });
            },
            beforeClose: function (event, ui)
            {
                $(".visionHeaderMain").css("z-index", "99999");
                $(".visionFooterMain").css("z-index", "99999");
            }
        });
    }

}
function temperaroryLockScheduledJob(jobId, cronExp, lockDate) {
    showLoader();
    $.ajax({
        datatype: "html",
        type: "post",
        traditional: true,
        url: 'tempLockScheduleJob',
        cache: false,
        data: {
            jobId: jobId,
            cronExp: cronExp,
            lockDate: lockDate
        },
        success: function (response) {
            stopLoader();
            if (response != null && response != '') {
//                                            var dialogSplitMessage = dialogSplitIconText(response['message'], "Y");
                $("#dialog").html(response);
                $("#dialog").dialog({
                    title: 'Temp Lock job',
                    modal: true,
                    height: 'auto',
                    minHeight: 'auto',
                    minWidth: 370,
                    maxWidth: 'auto',
                    fluid: true,
                    buttons: [{
                            text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                            click: function () {
                                processLogInterval = setInterval(function () {
                                    refreshIacData() // this will run after every 1 seconds
                                }, 2000);
                                $(this).html("");
                                $(this).dialog("close");
                                $(this).dialog("destroy");


                            }}],
                    open: function ()
                    {
                        if (processLogInterval != null) {
                            clearInterval(processLogInterval);
                        }
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
        },
        error: function (e)
        {
            sessionTimeout(e);
        }
    });
}
function addNotificationToSendMail(jobId) {
    var jobName = $("#" + jobId).find("span").text();
//    var notificationDiv = "<div > "
//            + "<label for='jobName'>Task Name : </label>"
//            + "<label for='emails'>Enter email addresses : </label>"
//            + "<input type='email' id='emails' name='emails' placeholder='Separate each email address with a comma' multiple ></br>"
//            + "<label for='mobNum'>Enter Mobile number : </label>"
//            + "<input type='number' id='mobNum' name='mobNum'  placeholder='Separate each mobile number with a comma' multiple>"
//            + " </div>";
    var notificationDiv =
            "<table id='iTransformTable'  style='width:100%'>"
            + "<thead>"
            + "</thead>"
            + "<tbody>"
            + "<tr>"
            + "<td> <label>Task Name</label></td>"
            + "<td> <label  id='taskName'>" + jobName + "</label></td>"
            + "</tr>"
            + "<tr>"
            + "<td> <label>Email</label></td>"
            + "<td> <div class='tooltip'><input class='tooltip' type='text' id='nftEmail'  multiple><span class='tooltiptext'>Separate each email address with a comma</span></div>"
            + "</tr>"
            + "<tr>"
            + "<td> <label>CC Email</label></td>"
            + "<td><div class='tooltip'> <input  type='text' id='ccEmail'  multiple><span class='tooltiptext'>Separate each email address with a comma</span></div>"
            + "</tr>"
            + "<tr>"
            + "<td> <label>Mobile No</label></td>"
            + "<td> <input type='number' id='nftMoNo' placeholder='Separate each mobile number with a comma' multiple>"
            + "</tr>"
            + "<tr>"
            + "<td> <label>Notification Detail</label></td>"
            + "<td> <input type='text' id='nftDesc'>"
            + "</tr>"
            + "</tbody>"
            + "</table></div>";
    $("#dialog").html(notificationDiv);
    $("#dialog").dialog({
        title: 'Message',
        modal: true,
        height: '250',
        width: '350',
        fluid: true,
        buttons: [{
                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                click: function () {
                    var mail = $("#nftEmail").val();
                    var mobNum = $("#nftEmail").val();
                    var nftDesc = $("#nftDesc").val();
                    var ccEmail = $("#ccEmail").val();
                    addMailToTable(jobId, mail, ccEmail, mobNum, nftDesc);
                    $(this).html("");
                    $(this).dialog("close");
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
function addMailToTable(jobId, mail, ccEmail, mobNum, nftDesc) {
    $.ajax({
        datatype: "json",
        type: "post",
        traditional: true,
        url: 'updateMailToTable',
        cache: false,
        data: {
            jobId: jobId,
            mailIds: mail,
            ccMailIds: ccEmail,
            mobNum: mobNum,
            nftDesc: nftDesc
        },
        success: function (response, status, xhr) {
            stopLoader();

            if (response != null) {
                $("#dialog").html(response);
                $("#dialog").dialog({
                    title: 'Message',
                    modal: true,
                    height: '150',
                    width: '300',
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

            }//resp
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });

}
function scheduledJobExecutionPlan() {
    var scheduledJobsExPlanDiv = "<div id='iTransFromsIAS'> <div id='iTransFormIcons' style='float: left;' style='display:none;'>"
            + "<span>"
            + "<img src='images/Refresh Icon.svg' onclick='refreshIacData()' class='visionETLIcons' title='Refresh' style='width:15px;height: 15px;cursor:pointer;' /></span>"
            + "<span> <img src='images/Add icon.svg' onclick='showSavedJobInIAC()'  class='visionEtlColumnMapIcon' title='Add'  style='width:15px;height: 15px;cursor:pointer;'></span>"
            + " <span>"
            + " <img src='images/Detele Red Icon.svg' onclick='deleteSelectedJob()' class='visionETLIcons' title='Delete' style='width:15px;height: 15px;cursor:pointer;'"
            + " /></span>"
            + " </div>";
    +" </div>";


    $("#scheduledJobsExPlan").html(scheduledJobsExPlanDiv);
//                    $("#scheduledJobsExPlan").show();
    showLoader();
    $.ajax({
        datatype: "json",
        type: "post",
        traditional: true,
        url: 'getExecutionPlanData',
        cache: false,
        data: {

        },
        success: function (response, status, xhr) {
            stopLoader();
            if (response != null) {
                var test = JSON.stringify(response);
                var source =
                        {
                            localdata: test,
                            datatype: "json",
                            datafields: [
                                {name: 'TASK_NAME', type: 'string'},
                                {name: 'START_DATE', type: 'string'},
                                {name: 'START_TIME', type: 'string'},
                                {name: 'END_DATE', type: 'string'},
                                {name: 'END_TIME', type: 'string'},
                                {name: 'STATUS', type: 'string'},
                                {name: 'LOG_FILE', type: 'string'},
                                {name: 'USER_NAME', type: 'string'},
                                {name: 'DESCRIPTION', type: 'string'},
                                {name: 'JOB_ID', type: 'string'},
                                {name: 'CRON_TRIGGER', type: 'string'}
                            ],
                        };


                var imagerenderer = function (row, datafield, value) {
                    return '<img src="images/logicon.png" style="width:30px;height: 20px;cursor:pointer;">';
                }
                var dataAdapter = new $.jqx.dataAdapter(source, {
                    loadComplete: function (test) { },
                    loadError: function (xhr, status, error) { }
                });
                var cellclass = function (row, columnfield, STATUS) {
                    if (STATUS == 'Ready to Run' || STATUS == 'Ready to Run(IN 30Mins)') {
                        return 'yellow';
                    } else if (STATUS == 'Running...') {
                        return 'green';
                    } else if (STATUS == 'STOP') {
                        return 'red';
                    }
                }
                $("#scheduledJobsExPlanGrid").jqxGrid(
                        {
                            width: "100%",
                            height: '360',
                            source: dataAdapter,
                            theme: 'energyblue',
                            pagesize: 50,
                            sortable: true,
                            pageable: true,
                            autoheight: true,
                            autoloadstate: false,
                            autosavestate: false,
                            columnsresize: true,
                            columnsreorder: true,
                            showfilterrow: true,
                            filterable: true,
                            selectionmode: 'checkbox',
                            columns: [
                                {text: 'TASK NAME', datafield: 'TASK_NAME', width: 150, align: 'center'},
                                {text: 'START_DATE', datafield: 'START_DATE', width: 150, align: 'center'},
                                {text: 'START_TIME', datafield: 'START_TIME', width: 150, align: 'center'},
                                {text: 'END_DATE', datafield: 'END_DATE', width: 150, align: 'center'},
                                {text: 'END_TIME', datafield: 'END_TIME', width: 150, align: 'center'},
                                {text: 'STATUS', datafield: 'STATUS', cellclassname: cellclass, align: 'center', width: 150, cellsrenderer: function (row, colum, value) {
                                        if (value == "TEMPLOCK") {
                                            var cell = '<div style="margin-top:5px;">';
                                            cell += '<div style="margin-left: 5px; float: left;">Temp Locked<img src="images/lockedIcon.jpg" style="width:30px;height: 20px;cursor:pointer;"></div>';
                                            cell += '</div>';
                                            return cell;
                                        }
                                        if (value == "PERLOCKED") {
                                            var cell = '<div style="margin-top:5px;">';
                                            cell += '<div style="margin-left: 5px; float: left;">Per Locked<img src="images/lockedIcon.jpg" style="width:30px;height: 20px;cursor:pointer;"></div>';
                                            cell += '</div>';
                                            return cell;
                                        }
                                    }, },
                                {text: 'LOG_FILE', datafield: 'LOG_FILE', cellsrenderer: imagerenderer, align: 'center', cellsalign: 'center', width: 50},
                                {text: 'USER_NAME', datafield: 'USER_NAME', width: 150, align: 'center'},
                                {text: 'DESCRIPTION', datafield: 'DESCRIPTION', width: 150, align: 'center'},
                                {text: 'JOB_ID', datafield: 'JOB_ID', width: 150, hidden: true},
                                {text: 'CRON_TRIGGER', datafield: 'CRON_TRIGGER', width: 150, hidden: true}
                            ]
                        });

                if (test != null && test != "[]" && test != " ") {
                    processLogInterval = setInterval(function () {
                        refreshIacData() // this will run after every 1 seconds
                    }, 2000);
                }
                $('#scheduledJobsExPlanGrid').on('mouseup', function (event) {
                    var dfName = $("#getIacData").val();
                    var rightClick = isRightClick(event);
                    if (rightClick != null && rightClick == true) {
                        if (dfName != 'undefined' && dfName != null && dfName == "TASK_NAME") {
                            var target = $(event.target).parents('li:first')[0];
                            var selectedrowindex = $("#scheduledJobsExPlanGrid").jqxGrid('getselectedrowindex');
                            var datarow = $("#scheduledJobsExPlanGrid").jqxGrid('getrowdata', selectedrowindex);
                            if (datarow != null && datarow != 'undefined') {
                                var jobId = datarow.JOB_ID;

                                var sDate = datarow.START_DATE;
                                var eDate = datarow.END_DATE;
                                var cronExp = datarow.CRON_TRIGGER;
                                var sTime = datarow.START_TIME;
                                var eTime = datarow.END_TIME;
                                var status = datarow.STATUS;
                                if (status == "PERLOCKED" && status != "" && status != "undefined") {
                                    var menuItems = "<li onclick=\"unLockJob('" + jobId + "','" + sDate + "','" + eDate + "','" + cronExp + "','" + sTime + "','" + eTime + "')\">Unlock Job</li>";
                                    var menuHeight = 1;
                                } else if (status == "STOP" || status == "Ready to Run" || status == "Running...") {
                                    var menuItems = "<li onclick=\"reScheduleJobInIAC('" + jobId + "','" + sDate + "','" + eDate + "','" + cronExp + "','" + sTime + "','" + eTime + "')\">Re-Schedule</li>";
                                    menuItems += "<li onclick=\"lockScheduledJob('" + jobId + "')\">Lock Job</li>";
                                    menuItems += "<li onclick=\"addNotificationToSendMail('" + jobId + "')\">Notification</li>";
                                    menuItems += "<li onclick=\"viewJobDetails('" + jobId + "')\">View Details</li>";
                                    var menuHeight = 4;
                                } else if (status == "TEMPLOCK" && status != "" && status != "undefined") {
                                    var menuItems = "<li onclick=\"unLockJob('" + jobId + "','" + sDate + "','" + eDate + "','" + cronExp + "','" + sTime + "','" + eTime + "')\">Unlock Job</li>";
                                    var menuHeight = 1;
                                }
                                $("#jqxMenu").remove();
                                $(".visionMainPage").append("<div id='jqxMenu'><ul></ul></div>");
                                $("#jqxMenu ul").html(menuItems);
                                var contextMenu = $("#jqxMenu").jqxMenu({width: '120px', height: menuHeight * 27.5 + 'px', autoOpenPopup: false, mode: 'popup', theme: 'energyblue'}); // ravi start
                                var scrollTop = $(window).scrollTop();
                                var scrollLeft = $(window).scrollLeft();

                                contextMenu.jqxMenu('open', parseInt(event.clientX) + 5 + scrollLeft, parseInt(event.clientY) + 5 + scrollTop);
                                return false;
                            }
//                                            } else {
//                                                showMesg("Select any one Scheduled Job");
//                                            }
                        }
                    }
                });

                $('#scheduledJobsExPlanGrid').on('cellclick', function (event) {
                    var dfName = event.args.datafield;
                    $("#getIacData").val(dfName);
                });


                $('#scheduledJobsExPlanGrid').on('cellclick', function (event) {
                    var dfName = event.args.datafield;

                    if (dfName != 'undefined' && dfName != null && dfName == "LOG_FILE") {
                        var target = $(event.target).parents('li:first')[0];
                        var selectedrowindex = $("#scheduledJobsExPlanGrid").jqxGrid('getselectedrowindex');
                        var datarow = $("#scheduledJobsExPlanGrid").jqxGrid('getrowdata', selectedrowindex);
                        if (datarow != null && datarow != 'undefined') {
                            var jobId = datarow.JOB_ID;
//                                            openSelectedJobLog(jobId);
                            openLogFileIAC(jobId);
                        } else {
                            showMesg("Select one Schedule Job for open log file");
                        }
                    }
                });

            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }
    });


}

//IAC