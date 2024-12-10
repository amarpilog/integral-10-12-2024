/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


var selectedDataBase;
var selectConnObj;
var selectColumnsObj;
var savedDBData = {};
var cubeValueInner = 1;
var cubePossDimInner = 1;
var globalTreeObj;
var currentTabId;
var trfmRulesChanged = false; //joins
var tablesGroupObj = {};
var allGridColumns = {}; 
$(document).ready(function () {
    //visionAnalyticListIcon
    showSavedAnalyticsJobs();
$(".visionETLIcons").hide();
    hideBreadcrumb();
    var levelTabIdHeight = $("#levelTabId").height();
    var visionClusterSpliterMainHeight = $("#visionClusterSpliterMain").height();
    var levelTabIdHeight = $("#levelTabId").height();
    var visionClusterSpliterMainHeight = $("#visionClusterSpliterMain").height();
    var pageHeight = $(".visionHeader").height() + $(".visionFooterMain").height() + $(".visionBreadcrumMain").height();
    var pageHeight1 = $(".visionHeader").height() + $(".visionFooterMain").height();
    var splitterHeight = (parseInt($(window).height()) - parseInt(pageHeight1)) - 10;
    $("#dataAnalyticsSuiteMainSplitter").jqxSplitter({width: '99.1%', height: parseInt(splitterHeight),
        orientation: 'vertical',
        panels: [{size: "0%"}, {min: 50, size: "85%"}]});

    $("#dataAnalyticsMainSplitter").jqxSplitter({width: '100%', height: '100%',
        orientation: 'vertical',
        panels: [{size: "90%", min: 50}, {min: 50, size: "10%"}]});
    $("#designAreaMainSplitter").jqxSplitter({width: '100%', height: '100%',
        orientation: 'horizontal',
        panels: [{size: "50%", min: 50}, {min: 50, size: "50%"}]});
//    $("#anaylyticPropertiesSplitter").jqxSplitter({width: '100%', height: '100%',
//        orientation: 'vertical',
//        panels: [{size: "50%", min: 50}, {min: 50, size: "50%"}]});
    loadChartTypes();
    $('#dataAnalyticsSuiteMainSplitter').on('resize', function (event) {
        var height = $("#dataBaseConnectionDiv").height();
        var width = $("#dataBaseConnectionDiv").width();
        $("#schemaAnalyticsObjectsDiv").css("height", height);
        $("#schemaAnalyticsObjectsDiv").css("width", width);
    });
    $('#designAreaMainSplitter').on('resize', function (event) {
        var height = event.args.panels[0].size;
        height = parseInt(height) - 35;
        var currentHeight = $("#visionChartConfigOptionForm").height();
        $("#visionChartConfigOptionForm").css("height", (height) + "px");
        $(".anaylyticDataPropertiesDiv").css("height", (height) + "px");
    });
//    
//    $("#designAreaColumnFormTab").jqxTabs({width: "100%", height: "100%", position: 'top', theme: 'ui-redmond', reorder: true});
    $("#availableAnalyticsConnections").on('mousedown', function (event) {

        var target = $(event.target).parents('li:first')[0];
        var rightClick = isRightClick(event);
        if (rightClick && target != null) {
            $("#availableAnalyticsConnections").jqxTree('selectItem', target);

            var selectedItem = $('#availableAnalyticsConnections').jqxTree('getSelectedItem');
            var selectedParentItem = {};
            try {
                selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedItem.parentElement);
                if (selectedParentItem != null) {
                    selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedParentItem.parentElement);
                }
            } catch (e) {
            }
            if (selectedParentItem != null
                    && selectedParentItem['value'] != null
                    && selectedParentItem['value'] != 'null'
                    && selectedParentItem['value'] != 'null'
                    && (selectedParentItem['value'] == 'TABLES'
                            || selectedParentItem['value'] == 'VIEWS'
                            || selectedParentItem['value'] == 'SYNONYMS'
                            || selectedParentItem['value'] == 'FILES'
                            )
                    ) {
                var menuItems = "";
                var height = 2;
                menuItems += "<li onclick=addToDataProperty('analyticsColumnsInner')>Add to Columns(X-axis)</li>";
                menuItems += "<li onclick=addToDataProperty('analyticsRowsInner')>Add to Rows (Y-axis)</li>";
//                menuItems += "<li onclick=addToDataProperty('analyticsColumns')>Add to Columns(X-axis)</li>";
//                menuItems += "<li onclick=addToDataProperty('analyticsRows')>Add to Rows (Y-axis)</li>";
                $("#jqxMenu").remove();
                $(".visionMainPage").append("<div id='jqxMenu'><ul></ul></div>");
                $("#jqxMenu ul").html(menuItems);
                var contextMenu = $("#jqxMenu").jqxMenu({width: '170px', height: height * 40 + 'px', autoOpenPopup: false, mode: 'popup', theme: 'energyblue'}); // ravi start
                var scrollTop = $(window).scrollTop();
                var scrollLeft = $(window).scrollLeft();
                contextMenu.jqxMenu('open', parseInt(event.clientX) + 5 + scrollLeft, parseInt(event.clientY) + 5 + scrollTop);
                return true;
            } else if (selectedItem.level == 3 || selectedItem.level == 2) {
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
                var scrollTop = $(window).scrollTop();
                var scrollLeft = $(window).scrollLeft();
                contextMenu.jqxMenu('open', parseInt(event.clientX) + 5 + scrollLeft, parseInt(event.clientY) + 5 + scrollTop);
                return true;



            } else { // ravi end
                return false;
            }

        }
    });

    $("#availableAnalyticsJobsId").on('mousedown', function (event) {
        var target = $(event.target).parents('li:first')[0];
        var treeItem = $(event.target).closest('.visionETLAvailableJobs');
        var rightClick = isRightClick(event);
        if (rightClick && target != null && treeItem.length > 0) {
            $("#avaialableAnalyticsJobsTree").jqxTree('selectItem', target);
            var selectedItem = $('#avaialableAnalyticsJobsTree').jqxTree('getSelectedItem');
            var jobId = $(target).find("div.visionETLAvailableJobs").attr("id");
            var menuItems = "<li onclick=\"deleteAnalyticJob('" + jobId + "')\">Delete Job</li>";
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
    $("#schemaAnalyticsObjectsDiv").on('mousedown', function (event) {

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
            var columnObj = globalTreeObj['treeColumnObj'][3];
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
});

function currentConnectionTree(treeObj, treeId) {
    globalTreeObj = treeObj; // ravi 

    treeId = "availableAnalyticsConnections";
    var connectionObj = treeObj['connObj'];
    var treeConfigObj = treeObj['treeConfigObj'];
    var dragEndFunction = treeObj['dragEndFunction'];
//    treeConfigObj.allowDrag = true;
//    treeConfigObj.allowDrop = true;
    treeConfigObj.hasThreeStates = true;
    treeConfigObj.checkboxes = true;
//    treeConfigObj.dragEnd = function (item, dropItem, args, dropPosition, tree) {
//        dragNDropColumns(item, dropItem, args, dropPosition, tree, treeId);
//
//    };

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
            treePagingAnalytics(selectedValue, selectConnObj, $selectedElement[0], selectedLevel, selectColumnsObj, valueObj, selectedParentValue);
//            treePagingAnalytics(selectedValue, selectConnObj, $selectedElement[0], selectedLevel, selectColumnsObj, valueObj);
        }
    });


    $('#' + treeId).on('expand', function (event) {

        var parentItem = $('#' + treeId).jqxTree('getItem', event.args.element);
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
            if (parentItem.level > 1)
            {
                //var prevObj = parentItem['prevItem'];
//                value = parentItem['value'];
                var parentEventItem = event.args.element;
                for (var i = level; i > 1; i--)
                {
                    parentEventItem = parentEventItem.parentElement.parentElement;
                }
                var selectedItem = $('#' + treeId).jqxTree('getItem', parentEventItem);
                value = selectedItem['value'];
            }


            var data = {
                parentkey: parentItem.value,
                treeId: treeId,
                level: parentItem.level,
                extTreeParams: extTreeParams,
                columnsObj: JSON.stringify(columnsObj),
                connectionObj: JSON.stringify(connectionObj),
                startIndex: 0,
                endIndex: 20
            };
            var hiddenFieldId = "DATABASE_" + value + "_" + parentItem.value + "_hidden";
            var hiddenPagingId = "DATABASE_" + value + "_" + parentItem.value + "paging__hidden";
            hiddenFieldId = hiddenFieldId.replace(/ /g, '_');
            hiddenPagingId = hiddenPagingId.replace(/ /g, '_');
            hiddenFieldId = hiddenFieldId.replace(/\//g, '_');
            hiddenPagingId = hiddenPagingId.replace(/\//g, '_');
            var hiddenPagingField = "<input type='hidden' id='" + hiddenPagingId + "'value=''/>";
            $("#" + treeId).append(hiddenPagingField);
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

            $.ajax({
                type: "post",
                traditional: true,
                dataType: 'json',
                url: url,
                cache: false,
                data: data,
                success: function (data, status, xhr) {
                    stopLoader();

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
                    $("#availableAnalyticsConnections li").draggable({
                        revert: "invalid",
                        refreshPositions: true,
                        cursor: 'move',
                        zindex: false,
                        opacity: false,
                        appendto: "parent",
                        helper: function (event) {
                            var selectedItem = $('#availableAnalyticsConnections').jqxTree('getItem', event.currentTarget);
                            var helperList;
                            if (selectedItem.level == 4) {
                                helperList = '<ul></ul>';
                            } else {
                                helperList = "";
                            }
                            return helperList;
                        },
                        drag: function (event, ui) {
                            ui.helper.addClass("draggableTable");
                        },
                        stop: function (event, ui) {
                            ui.helper.removeClass("draggableTable");
                        }
                    });
                    $("#analyticsColumns").droppable({
                        revert: "invalid",
                        refreshPositions: true,
                        cursor: 'move',
                        drop: function (event, ui) {
                            var label = ui.draggable[0].innerText
                            var columnName = ui.draggable[0].innerText;
                            var tableName = "";
                            var connType = "";
                            var selectedItem = $('#availableAnalyticsConnections').jqxTree('getItem', ui.draggable[0]);
                            var selectedParentItem = {};
                            try {
                                selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedItem.parentElement);
                                if (selectedParentItem != null) {
                                    tableName = selectedParentItem['value'];
                                    selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedParentItem.parentElement);
                                    connType = selectedParentItem['value'];
                                }
                            } catch (e) {
                            }
                            
                            var columnData = '<div id="COLUMN_' + tableName + "_" + columnName + '" class="analyticsDivData"' //group
                                    + ' title="' + columnName + '" data-table-name="' + tableName + '" '
                                    + ' data-column-name="' + columnName + '" data-coon-type ="' + connType + '" ><span class="visionColsText" ondblclick="selectColumnFun(this,\'' + tableName + "." + columnName + '\',\'analyticsColumnsInner\')">' + columnName
                                    + '</span><img src="images/close_white.png" title="Remove Column"'
                                    + ' onclick="removeColumn(this,\'' + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;">'
                                    + '</div>';

                            $("#analyticsColumnsInner").append(columnData);
                            var columnLabelData = "<div id='COL_LBL_DIV_" + columnName + "' data-column-name='" + columnName + "' class='columnLabelDivs'>"
                                    + "<label>" + columnName + ":</label>"
                                    + "<input type='text' id='COL_" + columnName + "' data-column-name='" + columnName + "' value='" + columnName + "'/>  "
                                    + ""
                                    + "</div>";
                            $("#analyticsJoinClauseInner").empty();
                            var joinClauseImage = "<img src='images/mapping.svg' id='showjoinMapping' class='visionEtlMapTableIcon visionEtlJoinClauseMapIcon' title='Map Columns For Join'"
                                    + "                   onclick='showJoinsPopup()' style='width:15px;height: 15px;cursor:pointer;float:left;'/>";
                            $("#analyticsJoinClauseImg").html(joinClauseImage); //joins
                            $("#analyticsWhereClauseInner").empty();
                            var whereClauseImage = "<img src='images/mapping.svg' id='showWhereMapping' class='visionEtlMapTableIcon visionEtlWhereClauseMapIcon' title='Map Columns For Where'"
                                    + " onclick = 'showWhereClauseOnPopup()' style = 'width:15px;height: 15px;cursor:pointer;float:left;' / > ";
                            $("#analyticsWhereClauseImg").html(whereClauseImage); //joins
                            $("#selectedColLabelsDiv").append(columnLabelData);
                            $("#analyticsInnerJoinClause").empty();  //group
                            tablesGroupObj[tableName] = tableName; 

                        }

                    });
                    $("#analyticsRows").droppable({
                        revert: "invalid",
                        refreshPositions: true,
                        cursor: 'move',
                        drop: function (event, ui) {
                            var label = ui.draggable[0].innerText
                            var columnName = ui.draggable[0].innerText;
                            var tableName = "";
                            var connType = "";
                            var selectedItem = $('#availableAnalyticsConnections').jqxTree('getItem', ui.draggable[0]);
                            var selectedParentItem = {};
                            try {
                                selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedItem.parentElement);
                                if (selectedParentItem != null) {
                                    tableName = selectedParentItem['value'];
                                    selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedParentItem.parentElement);
                                    connType = selectedParentItem['value'];
                                }
                            } catch (e) {
                            }
                            var columnData = '<div id="COLUMN_' + tableName + "_" + columnName + '" class="analyticsDivData"' //group
                                    + ' title="' + columnName + '" data-table-name="' + tableName + '" '
                                    + ' data-column-name="' + columnName + '" data-coon-type ="' + connType + '" ><span class="visionColsText" ondblclick="selectColumnFun(this,\'' + tableName + "." + columnName + '\',\'analyticsRowsInner\')">' + columnName
                                    + '</span><img src="images/close_white.png" title="Remove Column"'
                                    + ' onclick="removeColumn(this,\'' + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;"></div>';

                            $("#analyticsRowsInner").append(columnData);
                            var columnLabelData = "<div id='COL_LBL_DIV_" + columnName + "' data-column-name='" + columnName + "' class='columnLabelDivs'>"
                                    + "<label>" + columnName + ":</label>"
                                    + "<input type='text' id='COL_" + columnName + "' data-column-name='" + columnName + "' value='" + columnName + "'/>  "
                                    + ""
                                    + "</div>";
                            $("#analyticsJoinClauseInner").empty();
                            var joinClauseImage = "<img src='images/mapping.svg' id='showjoinMapping' class='visionEtlMapTableIcon visionEtlJoinClauseMapIcon' title='Map Columns For Join'"
                                    + "                   onclick='showJoinsPopup()' style='width:15px;height: 15px;cursor:pointer;float:left;'/>";
                            $("#analyticsJoinClauseImg").html(joinClauseImage); //joins
                            $("#analyticsWhereClauseInner").empty();
                            var whereClauseImage = "<img src='images/mapping.svg' id='showWhereMapping' class='visionEtlMapTableIcon visionEtlWhereClauseMapIcon' title='Map Columns For Where'"
                                    + " onclick = 'showWhereClauseOnPopup()' style = 'width:15px;height: 15px;cursor:pointer;float:left;' / > ";
                            $("#analyticsWhereClauseImg").html(whereClauseImage); //joins
                            $("#selectedColLabelsDiv").append(columnLabelData);
                            $("#analyticsInnerJoinClause").empty();  //group
                            tablesGroupObj[tableName] = tableName;
                        }

                    });
                    $("#analyticsComboRows").droppable({
                        revert: "invalid",
                        refreshPositions: true,
                        cursor: 'move',
                        drop: function (event, ui) {
                            var label = ui.draggable[0].innerText
                            var columnName = ui.draggable[0].innerText;
                            var tableName = "";
                            var connType = "";
                            var selectedItem = $('#availableAnalyticsConnections').jqxTree('getItem', ui.draggable[0]);
                            var selectedParentItem = {};
                            try {
                                selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedItem.parentElement);
                                if (selectedParentItem != null) {
                                    tableName = selectedParentItem['value'];
                                    selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedParentItem.parentElement);
                                    connType = selectedParentItem['value'];
                                }
                            } catch (e) {
                            }
                            var columnData = '<div id="COLUMN_' + tableName + "_" + columnName + '" class="analyticsDivData"' //group
                                    + ' title="' + columnName + '" data-table-name="' + tableName + '" '
                                    + ' data-column-name="' + columnName + '" data-coon-type ="' + connType + '" ><span class="visionColsText" ondblclick="selectColumnFun(this,\'' + tableName + "." + columnName + '\',\'analyticsComboRowsInner\')">' + columnName
                                    + '</span><img src="images/close_white.png" title="Remove Column"'
                                    + ' onclick="removeColumn(this,\'' + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;"></div>';

                            $("#analyticsComboRowsInner").append(columnData);
                            var columnLabelData = "<div id='COL_LBL_DIV_" + columnName + "' data-column-name='" + columnName + "' class='columnLabelDivs'>"
                                    + "<label>" + columnName + ":</label>"
                                    + "<input type='text' id='COL_" + columnName + "' data-column-name='" + columnName + "' value='" + columnName + "'/>  "
                                    + ""
                                    + "</div>";
                             $("#analyticsJoinClauseInner").empty();
                            var joinClauseImage = "<img src='images/mapping.svg' id='showjoinMapping' class='visionEtlMapTableIcon visionEtlJoinClauseMapIcon' title='Map Columns For Join'"
                                    + "                   onclick='showJoinsPopup()' style='width:15px;height: 15px;cursor:pointer;float:left;'/>";
                            $("#analyticsJoinClauseImg").html(joinClauseImage); //joins
                            $("#analyticsWhereClauseInner").empty();
                            var whereClauseImage = "<img src='images/mapping.svg' id='showWhereMapping' class='visionEtlMapTableIcon visionEtlWhereClauseMapIcon' title='Map Columns For Where'"
                                    + " onclick = 'showWhereClauseOnPopup()' style = 'width:15px;height: 15px;cursor:pointer;float:left;' / > ";
                            $("#analyticsWhereClauseImg").html(whereClauseImage); //joins

                            $("#selectedColLabelsDiv").append(columnLabelData);
                            $("#analyticsInnerJoinClause").empty();  //group
                            tablesGroupObj[tableName] = tableName;

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

//                    $('#mainSplitter').resize();
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
function loadChartTypes() {
    showLoader();
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        url: "getAllCharts",
        cache: false,
        success: function (data, status, xhr) {
            stopLoader();
            $("#analyticsListMainDiv").html(data);

        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
}


function showAnalyticsList(treeId, listId) {
    $('#dataAnalyticsSuiteMainSplitter').jqxSplitter({width: '99.1%',
        panels: [{size: "15%", min: 10}, {min: "60%", size: "85%"}]});
    $("#schemaAnalyticsObjectsDiv").hide();
    $("#newAnalyticsConnections").hide();
    $("#availableAnalyticsConnectionsDiv").hide();
    $("#availableAnalyticsJobsId").hide();
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
    $('#dataAnalyticsSuiteMainSplitter').resize();
}

function switchAnalyticsDesignTabs(liId, divId) {
    $("#" + liId).parent().find('li.visionETLDesignTabHighLight').removeClass('visionETLDesignTabHighLight');
    $("#" + liId).addClass('visionETLDesignTabHighLight');
    $("#dataAnalyticsMainSplitter").hide();
    $("#analyticDataViewDiv").hide();
//    $("#editorViewDiv").hide();
    $("#" + divId).show();
//    if (liId == 'li_SQLEditor') {
//        $("#scriptsExecute").show();
//    } else {
//        $("#scriptsExecute").hide();
//    }
}
//function fetchChartConfigForm(chartId, chartFormId, jobId) {
//    showLoader();
//    $(".visionAnalyticListIcon").removeClass("visionAnalyticListIconSelected");
//    $("#" + chartId).addClass("visionAnalyticListIconSelected");
//    var chartType = $("#" + chartId).attr("data-chart-type");
//    var chartDescr = $("#" + chartId).attr("data-chart-descr");
//    $("#visionChartConfigOptionForm").attr("data-chartType", chartType);
//    $("#visionChartConfigOptionForm").attr("data-chartId", chartId);
//    $("#visionChartConfigOptionForm").attr("data-chartFormId", chartFormId);
//    $(".visionETLIcons").show();
//    $(".visionChartConfigForm").show();
//    if (chartType != null && chartType != '' && chartType != undefined && chartType.indexOf("Combo") > -1)
//    {
//        $(".comboChartColumn").show();
//    } else
//    {
//        $(".comboChartColumn").hide();
//        $("#analyticsComboRowsInner").empty();
//    }
//    $.ajax({
//        type: "post",
//        traditional: true,
//        dataType: 'html',
//        url: "fetchChartConfigForm",
//        data: {
//            chartId: chartId,
//            chartFormId: chartFormId,
//            jobId: jobId
//        },
//        cache: false,
//        success: function (data, status, xhr) {
////            introJs().setOption('showProgress', true).start();
//            stopLoader();
//            if (data != null && data != '') {
//                var responseObj = JSON.parse(data);
//                if (responseObj != null && !jQuery.isEmptyObject(responseObj)) {
//                    $("#visionChartConfigOptionForm").html("<input type='hidden' id='chartOptIdObjStr'/>" + responseObj['formStr']);
//                    $(".visionChildChartOptionDiv").hide();
//                    $("#chartOptIdObjStr").val(JSON.stringify(responseObj['chartOptIdObj']));
//
//                    $("#analyticsColumns").droppable({
//                        revert: "invalid",
//                        refreshPositions: true,
//                        cursor: 'move',
//                        drop: function (event, ui) {
//                            var label = ui.draggable[0].innerText
//                            var columnName = ui.draggable[0].innerText;
//                            var tableName = "";
//                            var connType = "";
//                            var selectedItem = $('#availableAnalyticsConnections').jqxTree('getItem', ui.draggable[0]);
//                            var selectedParentItem = {};
//                            try {
//                                selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedItem.parentElement);
//                                if (selectedParentItem != null) {
//                                    tableName = selectedParentItem['value'];
//                                    selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedParentItem.parentElement);
//                                    connType = selectedParentItem['value'];
//                                }
//                            } catch (e) {
//                            }
//                            var columnData = '<div id="COLUMN_' + tableName + "_" + columnName + '" class="analyticsDivData"' //group
//                                    + ' title="' + columnName + '" data-table-name="' + tableName + '" '
//                                    + ' data-column-name="' + columnName + '" data-coon-type ="' + connType + '" ><span class="visionColsText" ondblclick="selectColumnFun(this,\'' + tableName + "." + columnName + '\',\'analyticsColumnsInner\')" >' + columnName
//                                    + '</span><img src="images/close_white.png" title="Remove Column"'
//                                    + ' onclick="removeColumn(this,\'' + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;">'
//                                    + '</div>';
//                            $("#analyticsColumnsInner").append(columnData);
//                            var columnLabelData = "<div id='COL_LBL_DIV_" + columnName + "' data-column-name='" + columnName + "' class='columnLabelDivs'>"
//                                    + "<label>" + columnName + ":</label>"
//                                    + "<input type='text' id='COL_" + columnName + "' data-column-name='" + columnName + "' value='" + columnName + "'/>  "
//                                    + ""
//                                    + "</div>";
//                             $("#analyticsJoinClauseInner").empty();
//                            var joinClauseImage = "<img src='images/mapping.svg' id='showjoinMapping' class='visionEtlMapTableIcon visionEtlJoinClauseMapIcon' title='Map Columns For Join'"
//                                    + "                   onclick='showJoinsPopup()' style='width:15px;height: 15px;cursor:pointer;float:left;'/>";
//                            $("#analyticsJoinClauseImg").html(joinClauseImage); //joins
//                            $("#analyticsWhereClauseInner").empty();
//                            var whereClauseImage = "<img src='images/mapping.svg' id='showWhereMapping' class='visionEtlMapTableIcon visionEtlWhereClauseMapIcon' title='Map Columns For Where'"
//                                    + " onclick = 'showWhereClauseOnPopup()' style = 'width:15px;height: 15px;cursor:pointer;float:left;' / > ";
//                            $("#analyticsWhereClauseImg").html(whereClauseImage); //joins
//                            $("#selectedColLabelsDiv").append(columnLabelData);
//                            $("#analyticsInnerJoinClause").empty();  //group
//                            tablesGroupObj[tableName] = tableName;
//                        }
//
//                    });
//                    $("#analyticsRows").droppable({
//                        revert: "invalid",
//                        refreshPositions: true,
//                        cursor: 'move',
//                        drop: function (event, ui) {
//                            var label = ui.draggable[0].innerText
//                            var columnName = ui.draggable[0].innerText;
//                            var tableName = "";
//                            var connType = "";
//                            var selectedItem = $('#availableAnalyticsConnections').jqxTree('getItem', ui.draggable[0]);
//                            var selectedParentItem = {};
//                            try {
//                                selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedItem.parentElement);
//                                if (selectedParentItem != null) {
//                                    tableName = selectedParentItem['value'];
//                                    selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedParentItem.parentElement);
//                                    connType = selectedParentItem['value'];
//                                }
//                            } catch (e) {
//                            }
//
//                            var columnData = '<div id="COLUMN_' + tableName + "_" + columnName + '" class="analyticsDivData"' //group
//                                    + ' title="' + columnName + '" data-table-name="' + tableName + '" '
//                                    + ' data-column-name="' + columnName + '" data-coon-type ="' + connType + '" ><span class="visionColsText" ondblclick="selectColumnFun(this,\'' + tableName + "." + columnName + '\',\'analyticsRowsInner\')">' + columnName
//                                    + '</span><img src="images/close_white.png" title="Remove Column"'
//                                    + ' onclick="removeColumn(this,\'' + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;"></div>';
//
//                            $("#analyticsRowsInner").append(columnData);
//                            var columnLabelData = "<div id='COL_LBL_DIV_" + columnName + "' data-column-name='" + columnName + "' class='columnLabelDivs'>"
//                                    + "<label>" + columnName + ":</label>"
//                                    + "<input type='text' id='COL_" + columnName + "' data-column-name='" + columnName + "' value='" + columnName + "'/>  "
//                                    + ""
//                                    + "</div>";
//                             $("#analyticsJoinClauseInner").empty();
//                            var joinClauseImage = "<img src='images/mapping.svg' id='showjoinMapping' class='visionEtlMapTableIcon visionEtlJoinClauseMapIcon' title='Map Columns For Join'"
//                                    + "                   onclick='showJoinsPopup()' style='width:15px;height: 15px;cursor:pointer;float:left;'/>";
//                            $("#analyticsJoinClauseImg").html(joinClauseImage); //joins
//                            $("#analyticsWhereClauseInner").empty();
//                            var whereClauseImage = "<img src='images/mapping.svg' id='showWhereMapping' class='visionEtlMapTableIcon visionEtlWhereClauseMapIcon' title='Map Columns For Where'"
//                                    + " onclick = 'showWhereClauseOnPopup()' style = 'width:15px;height: 15px;cursor:pointer;float:left;' / > ";
//                            $("#analyticsWhereClauseImg").html(whereClauseImage); //joins
//                            $("#selectedColLabelsDiv").append(columnLabelData);
//                            $("#analyticsInnerJoinClause").empty();  //group
//                            tablesGroupObj[tableName] = tableName; 
//                        }
//
//                    });
//                    $("#analyticsComboRows").droppable({
//                        revert: "invalid",
//                        refreshPositions: true,
//                        cursor: 'move',
//                        drop: function (event, ui) {
//                            var label = ui.draggable[0].innerText
//                            var columnName = ui.draggable[0].innerText;
//                            var tableName = "";
//                            var connType = "";
//                            var selectedItem = $('#availableAnalyticsConnections').jqxTree('getItem', ui.draggable[0]);
//                            var selectedParentItem = {};
//                            try {
//                                selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedItem.parentElement);
//                                if (selectedParentItem != null) {
//                                    tableName = selectedParentItem['value'];
//                                    selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedParentItem.parentElement);
//                                    connType = selectedParentItem['value'];
//                                }
//                            } catch (e) {
//                            }
//                            var columnData = '<div id="COLUMN_' + tableName + "_" + columnName + '" class="analyticsDivData"'  //group
//                                    + ' title="' + columnName + '" data-table-name="' + tableName + '" '
//                                    + ' data-column-name="' + columnName + '" data-coon-type ="' + connType + '" ><span class="visionColsText" ondblclick="selectColumnFun(this,\'' + tableName + "." + columnName + '\',\'analyticsComboRowsInner\')">' + columnName
//                                    + '</span><img src="images/close_white.png" title="Remove Column"'
//                                    + ' onclick="removeColumn(this,\'' + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;"></div>';
//
//                            $("#analyticsComboRowsInner").append(columnData);
//                            var columnLabelData = "<div id='COL_LBL_DIV_" + columnName + "' data-column-name='" + columnName + "' class='columnLabelDivs'>"
//                                    + "<label>" + columnName + ":</label>"
//                                    + "<input type='text' id='COL_" + columnName + "' data-column-name='" + columnName + "' value='" + columnName + "'/>  "
//                                    + ""
//                                    + "</div>";
//                             $("#analyticsJoinClauseInner").empty();
//                            var joinClauseImage = "<img src='images/mapping.svg' id='showjoinMapping' class='visionEtlMapTableIcon visionEtlJoinClauseMapIcon' title='Map Columns For Join'"
//                                    + "                   onclick='showJoinsPopup()' style='width:15px;height: 15px;cursor:pointer;float:left;'/>";
//                            $("#analyticsJoinClauseImg").html(joinClauseImage); //joins
//                            $("#analyticsWhereClauseInner").empty();
//                            var whereClauseImage = "<img src='images/mapping.svg' id='showWhereMapping' class='visionEtlMapTableIcon visionEtlWhereClauseMapIcon' title='Map Columns For Where'"
//                                    + " onclick = 'showWhereClauseOnPopup()' style = 'width:15px;height: 15px;cursor:pointer;float:left;' / > ";
//                            $("#analyticsWhereClauseImg").html(whereClauseImage); //joins
//                            $("#selectedColLabelsDiv").append(columnLabelData);
//                            $("#analyticsInnerJoinClause").empty();  //group
//                            tablesGroupObj[tableName] = tableName;
//
//                        }
//
//                    });
//                }
//            }
//
//
////            $(".visionChartConfigFormLabel").html(chartDescr);
//
//        },
//        error: function (e) {
//            console.log(e);
//            sessionTimeout(e);
//            stopLoader();
//        }
//    });
//}
//function fetchChartConfigForm(chartId, chartFormId) {
//    $(".visionAnalyticListIcon").removeClass("visionAnalyticListIconSelected");
//    $("#" + chartId).addClass("visionAnalyticListIconSelected");
//    var chartType = $("#" + chartId).attr("data-chart-type");
//    var chartDescr = $("#" + chartId).attr("data-chart-descr");
//    $("#visionChartConfigOptionForm").attr("data-chartType", chartType);
//    $("#visionChartConfigOptionForm").attr("data-chartId", chartId);
//    $("#visionChartConfigOptionForm").attr("data-chartFormId", chartFormId);
//    $(".visionETLIcons").show();
//    $(".visionChartConfigForm").show();
//    $.ajax({
//        type: "post",
//        traditional: true,
//        dataType: 'html',
//        url: "fetchChartConfigForm",
//        data: {
//            chartId: chartId,
//            chartFormId: chartFormId
//        },
//        cache: false,
//        success: function (data, status, xhr) {
////            introJs().setOption('showProgress', true).start();
//            stopLoader();
//            if (data != null && data != '') {
//                var responseObj = JSON.parse(data);
//                if (responseObj != null && !jQuery.isEmptyObject(responseObj)) {
//                    $("#visionChartConfigOptionForm").html("<input type='hidden' id='chartOptIdObjStr'/>" + responseObj['formStr']);
//                    $(".visionChildChartOptionDiv").hide();
//                    $("#chartOptIdObjStr").val(JSON.stringify(responseObj['chartOptIdObj']));
//
//                    $("#analyticsColumns").droppable({
//                        revert: "invalid",
//                        refreshPositions: true,
//                        cursor: 'move',
//                        drop: function (event, ui) {
//                            var label = ui.draggable[0].innerText
//                            var columnName = ui.draggable[0].innerText;
//                            var tableName = "";
//                            var connType = "";
//                            var selectedItem = $('#availableAnalyticsConnections').jqxTree('getItem', ui.draggable[0]);
//                            var selectedParentItem = {};
//                            try {
//                                selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedItem.parentElement);
//                                if (selectedParentItem != null) {
//                                    tableName = selectedParentItem['value'];
//                                    selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedParentItem.parentElement);
//                                    connType = selectedParentItem['value'];
//                                }
//                            } catch (e) {
//                            }
//                            var columnData = '<div id="COLUMN_' + label + '" class="analyticsDivData"'
//                                    + ' title="' + columnName + '" data-table-name="' + tableName + '" '
//                                    + ' data-column-name="' + columnName + '" data-coon-type ="' + connType + '" ><span class="visionColsText" >' + columnName
//                                    + '</span><img src="images/close_white.png" title="Remove Column"'
//                                    + ' onclick="removeColumn(this,\'' + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;"></div>';
//
//                            $("#analyticsColumnsInner").append(columnData);
//                            var columnLabelData = "<div id='COL_LBL_DIV_" + columnName + "' data-column-name='" + columnName + "' class='columnLabelDivs'>"
//                                    + "<label>" + columnName + ":</label>"
//                                    + "<input type='text' id='COL_" + columnName + "' data-column-name='" + columnName + "' value='" + columnName + "'/>  "
//                                    + ""
//                                    + "</div>";
//
//                            $("#selectedColLabelsDiv").append(columnLabelData);
//                        }
//
//                    });
//                    $("#analyticsRows").droppable({
//                        revert: "invalid",
//                        refreshPositions: true,
//                        cursor: 'move',
//                        drop: function (event, ui) {
//                            var label = ui.draggable[0].innerText
//                            var columnName = ui.draggable[0].innerText;
//                            var tableName = "";
//                            var connType = "";
//                            var selectedItem = $('#availableAnalyticsConnections').jqxTree('getItem', ui.draggable[0]);
//                            var selectedParentItem = {};
//                            try {
//                                selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedItem.parentElement);
//                                if (selectedParentItem != null) {
//                                    tableName = selectedParentItem['value'];
//                                    selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedParentItem.parentElement);
//                                    connType = selectedParentItem['value'];
//                                }
//                            } catch (e) {
//                            }
//
//                            var columnData = '<div id="COLUMN_' + label + '" class="analyticsDivData"'
//                                    + ' title="' + columnName + '" data-table-name="' + tableName + '" '
//                                    + ' data-column-name="' + columnName + '" data-coon-type ="' + connType + '" ><span class="visionColsText" >' + columnName
//                                    + '</span><img src="images/close_white.png" title="Remove Column"'
//                                    + ' onclick="removeColumn(this,\'' + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;"></div>';
//
//                            $("#analyticsRowsInner").append(columnData);
//                            var columnLabelData = "<div id='COL_LBL_DIV_" + columnName + "' data-column-name='" + columnName + "' class='columnLabelDivs'>"
//                                    + "<label>" + columnName + ":</label>"
//                                    + "<input type='text' id='COL_" + columnName + "' data-column-name='" + columnName + "' value='" + columnName + "'/>  "
//                                    + ""
//                                    + "</div>";
//
//                            $("#selectedColLabelsDiv").append(columnLabelData);
//                        }
//
//                    });
//                }
//            }
//
//
////            $(".visionChartConfigFormLabel").html(chartDescr);
//
//        },
//        error: function (e) {
//            console.log(e);
//            sessionTimeout(e);
//            stopLoader();
//        }
//    });
//}

//new fetchChartConfigForm by ravi//
function fetchChartConfigForm(chartId, chartFormId, jobId) {
    showLoader();
    $(".visionAnalyticListIcon").removeClass("visionAnalyticListIconSelected");
    $("#" + chartId).addClass("visionAnalyticListIconSelected");
    var chartType = $("#" + chartId).attr("data-chart-type");
    var chartDescr = $("#" + chartId).attr("data-chart-descr");
    $("#visionChartConfigOptionForm").attr("data-chartType", chartType);
    $("#visionChartConfigOptionForm").attr("data-chartId", chartId);
    $("#visionChartConfigOptionForm").attr("data-chartFormId", chartFormId);
    $(".visionETLIcons").show();
    
    if (chartType != null && chartType != '' && chartType != undefined && chartType.indexOf("Cube") > -1){ // ravi cube
        $(".anaylyticDataPropertiesDiv").css("width","90%");
        $(".analyticsDataTh").css("width","0%");
        $(".visionChartConfigFormIcons").hide();
       $(".visionChartConfigForm").hide();
        $("#analyticsComboRowsInner").empty();
        $(".visionCubeConfigForm").show(); 
        var selectBoxStr = $("#cubeTitleSelectBox").val();
        $("#visionCubeTitleSelectBox").html(selectBoxStr);
    } else {
        $(".visionChartConfigFormIcons").show(); // ravi cube
         $(".anaylyticDataPropertiesDiv").css("width","40%");
        $(".analyticsDataTh").css("width","90%");
        
        
                    $(".visionChartConfigForm").show();
                if (chartType != null && chartType != '' && chartType != undefined && chartType.indexOf("Combo") > -1)
                {
                    $(".comboChartColumn").show();
                } else
                {
                    $(".comboChartColumn").hide();
                    $("#analyticsComboRowsInner").empty();
                }
    }
    
    
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        url: "fetchChartConfigForm",
        data: {
            chartId: chartId,
            chartFormId: chartFormId,
            jobId: jobId
        },
        cache: false,
        success: function (data, status, xhr) {
//            introJs().setOption('showProgress', true).start();
            stopLoader();
            if (data != null && data != '') {
                var responseObj = JSON.parse(data);
                if (responseObj != null && !jQuery.isEmptyObject(responseObj)) {
                    $("#visionChartConfigOptionForm").html("<input type='hidden' id='chartOptIdObjStr'/>" + responseObj['formStr']);
                    $(".visionChildChartOptionDiv").hide();
                    $("#chartOptIdObjStr").val(JSON.stringify(responseObj['chartOptIdObj']));

                    $("#analyticsColumns").droppable({
                        revert: "invalid",
                        refreshPositions: true,
                        cursor: 'move',
                        drop: function (event, ui) {
                            var label = ui.draggable[0].innerText
                            var columnName = ui.draggable[0].innerText;
                            var tableName = "";
                            var connType = "";
                            var selectedItem = $('#availableAnalyticsConnections').jqxTree('getItem', ui.draggable[0]);
                            var selectedParentItem = {};
                            try {
                                selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedItem.parentElement);
                                if (selectedParentItem != null) {
                                    tableName = selectedParentItem['value'];
                                    selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedParentItem.parentElement);
                                    connType = selectedParentItem['value'];
                                }
                            } catch (e) {
                            }
                            var columnData = '<div id="COLUMN_' + tableName + "_" + columnName + '" class="analyticsDivData"' //group
                                    + ' title="' + columnName + '" data-table-name="' + tableName + '" '
                                    + ' data-column-name="' + columnName + '" data-coon-type ="' + connType + '" ><span class="visionColsText" ondblclick="selectColumnFun(this,\'' + tableName + "." + columnName + '\',\'analyticsColumnsInner\')" >' + columnName
                                    + '</span><img src="images/close_white.png" title="Remove Column"'
                                    + ' onclick="removeColumn(this,\'' + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;">'
                                    + '</div>';
                            $("#analyticsColumnsInner").append(columnData);
                            var columnLabelData = "<div id='COL_LBL_DIV_" + columnName + "' data-column-name='" + columnName + "' class='columnLabelDivs'>"
                                    + "<label>" + columnName + ":</label>"
                                    + "<input type='text' id='COL_" + columnName + "' data-column-name='" + columnName + "' value='" + columnName + "'/>  "
                                    + ""
                                    + "</div>";
                             $("#analyticsJoinClauseInner").empty();
                            var joinClauseImage = "<img src='images/mapping.svg' id='showjoinMapping' class='visionEtlMapTableIcon visionEtlJoinClauseMapIcon' title='Map Columns For Join'"
                                    + "                   onclick='showJoinsPopup()' style='width:15px;height: 15px;cursor:pointer;float:left;'/>";
                            $("#analyticsJoinClauseImg").html(joinClauseImage); //joins
                            $("#analyticsWhereClauseInner").empty();
                            var whereClauseImage = "<img src='images/mapping.svg' id='showWhereMapping' class='visionEtlMapTableIcon visionEtlWhereClauseMapIcon' title='Map Columns For Where'"
                                    + " onclick = 'showWhereClauseOnPopup()' style = 'width:15px;height: 15px;cursor:pointer;float:left;' / > ";
                            $("#analyticsWhereClauseImg").html(whereClauseImage); //joins
                            $("#selectedColLabelsDiv").append(columnLabelData);
                            $("#analyticsInnerJoinClause").empty();  //group
                            tablesGroupObj[tableName] = tableName;
                        }

                    });
                    $("#analyticsRows").droppable({
                        revert: "invalid",
                        refreshPositions: true,
                        cursor: 'move',
                        drop: function (event, ui) {
                            var label = ui.draggable[0].innerText
                            var columnName = ui.draggable[0].innerText;
                            var tableName = "";
                            var connType = "";
                            var selectedItem = $('#availableAnalyticsConnections').jqxTree('getItem', ui.draggable[0]);
                            var selectedParentItem = {};
                            try {
                                selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedItem.parentElement);
                                if (selectedParentItem != null) {
                                    tableName = selectedParentItem['value'];
                                    selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedParentItem.parentElement);
                                    connType = selectedParentItem['value'];
                                }
                            } catch (e) {
                            }

                            var columnData = '<div id="COLUMN_' + tableName + "_" + columnName + '" class="analyticsDivData"' //group
                                    + ' title="' + columnName + '" data-table-name="' + tableName + '" '
                                    + ' data-column-name="' + columnName + '" data-coon-type ="' + connType + '" ><span class="visionColsText" ondblclick="selectColumnFun(this,\'' + tableName + "." + columnName + '\',\'analyticsRowsInner\')">' + columnName
                                    + '</span><img src="images/close_white.png" title="Remove Column"'
                                    + ' onclick="removeColumn(this,\'' + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;"></div>';

                            $("#analyticsRowsInner").append(columnData);
                            var columnLabelData = "<div id='COL_LBL_DIV_" + columnName + "' data-column-name='" + columnName + "' class='columnLabelDivs'>"
                                    + "<label>" + columnName + ":</label>"
                                    + "<input type='text' id='COL_" + columnName + "' data-column-name='" + columnName + "' value='" + columnName + "'/>  "
                                    + ""
                                    + "</div>";
                             $("#analyticsJoinClauseInner").empty();
                            var joinClauseImage = "<img src='images/mapping.svg' id='showjoinMapping' class='visionEtlMapTableIcon visionEtlJoinClauseMapIcon' title='Map Columns For Join'"
                                    + "                   onclick='showJoinsPopup()' style='width:15px;height: 15px;cursor:pointer;float:left;'/>";
                            $("#analyticsJoinClauseImg").html(joinClauseImage); //joins
                            $("#analyticsWhereClauseInner").empty();
                            var whereClauseImage = "<img src='images/mapping.svg' id='showWhereMapping' class='visionEtlMapTableIcon visionEtlWhereClauseMapIcon' title='Map Columns For Where'"
                                    + " onclick = 'showWhereClauseOnPopup()' style = 'width:15px;height: 15px;cursor:pointer;float:left;' / > ";
                            $("#analyticsWhereClauseImg").html(whereClauseImage); //joins
                            $("#selectedColLabelsDiv").append(columnLabelData);
                            $("#analyticsInnerJoinClause").empty();  //group
                            tablesGroupObj[tableName] = tableName; 
                        }

                    });
                    $("#analyticsComboRows").droppable({
                        revert: "invalid",
                        refreshPositions: true,
                        cursor: 'move',
                        drop: function (event, ui) {
                            var label = ui.draggable[0].innerText
                            var columnName = ui.draggable[0].innerText;
                            var tableName = "";
                            var connType = "";
                            var selectedItem = $('#availableAnalyticsConnections').jqxTree('getItem', ui.draggable[0]);
                            var selectedParentItem = {};
                            try {
                                selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedItem.parentElement);
                                if (selectedParentItem != null) {
                                    tableName = selectedParentItem['value'];
                                    selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedParentItem.parentElement);
                                    connType = selectedParentItem['value'];
                                }
                            } catch (e) {
                            }
                            var columnData = '<div id="COLUMN_' + tableName + "_" + columnName + '" class="analyticsDivData"'  //group
                                    + ' title="' + columnName + '" data-table-name="' + tableName + '" '
                                    + ' data-column-name="' + columnName + '" data-coon-type ="' + connType + '" ><span class="visionColsText" ondblclick="selectColumnFun(this,\'' + tableName + "." + columnName + '\',\'analyticsComboRowsInner\')">' + columnName
                                    + '</span><img src="images/close_white.png" title="Remove Column"'
                                    + ' onclick="removeColumn(this,\'' + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;"></div>';

                            $("#analyticsComboRowsInner").append(columnData);
                            var columnLabelData = "<div id='COL_LBL_DIV_" + columnName + "' data-column-name='" + columnName + "' class='columnLabelDivs'>"
                                    + "<label>" + columnName + ":</label>"
                                    + "<input type='text' id='COL_" + columnName + "' data-column-name='" + columnName + "' value='" + columnName + "'/>  "
                                    + ""
                                    + "</div>";
                             $("#analyticsJoinClauseInner").empty();
                            var joinClauseImage = "<img src='images/mapping.svg' id='showjoinMapping' class='visionEtlMapTableIcon visionEtlJoinClauseMapIcon' title='Map Columns For Join'"
                                    + "                   onclick='showJoinsPopup()' style='width:15px;height: 15px;cursor:pointer;float:left;'/>";
                            $("#analyticsJoinClauseImg").html(joinClauseImage); //joins
                            $("#analyticsWhereClauseInner").empty();
                            var whereClauseImage = "<img src='images/mapping.svg' id='showWhereMapping' class='visionEtlMapTableIcon visionEtlWhereClauseMapIcon' title='Map Columns For Where'"
                                    + " onclick = 'showWhereClauseOnPopup()' style = 'width:15px;height: 15px;cursor:pointer;float:left;' / > ";
                            $("#analyticsWhereClauseImg").html(whereClauseImage); //joins
                            $("#selectedColLabelsDiv").append(columnLabelData);
                            $("#analyticsInnerJoinClause").empty();  //group
                            tablesGroupObj[tableName] = tableName;

                        }

                    });
                  
                    //ravi cube
                    
                     $("#analyticsCubeXaxis, #analyticsCubeYaxis, #analyticsCubeZaxis, #analyticsCubeXLabel, #analyticsCubeYLabel, #analyticsCubeZLabel").droppable({ //cube changes
                        revert: "invalid",
                        refreshPositions: true,
                        cursor: 'move',
                        drop: function (event, ui) {
                            var label = ui.draggable[0].innerText
                            var columnName = ui.draggable[0].innerText;
                            var tableName = "";
                            var connType = "";
                            var selectedItem = $('#availableAnalyticsConnections').jqxTree('getItem', ui.draggable[0]);
                            var selectedParentItem = {};
                            try {
                                selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedItem.parentElement);
                                if (selectedParentItem != null) {
                                    tableName = selectedParentItem['value'];
                                    selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedParentItem.parentElement);
                                    connType = selectedParentItem['value'];
                                }
                            } catch (e) {
                            }
                            var columnData = '<div id="COLUMN_' + tableName + "_" + columnName + '" class="analyticsDivData"' //group
                                    + ' title="' + columnName + '" data-table-name="' + tableName + '" '
                                    + ' data-column-name="' + columnName + '" data-coon-type ="' + connType + '" ><span class="visionColsText" ondblclick="selectColumnFun(this,\'' + tableName + "." + columnName + '\',\'analyticsColumnsInner\')" >' + columnName
                                    + '</span><img src="images/close_white.png" title="Remove Column"'
                                    + ' onclick="removeColumn(this,\'' + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;">'
                                    + '</div>';
                            $("#" + this.id + "Inner").append(columnData);
                            var columnLabelData = "<div id='COL_LBL_DIV_" + columnName + "' data-column-name='" + columnName + "' class='columnLabelDivs'>"
                                    + "<label>" + columnName + ":</label>"
                                    + "<input type='text' id='COL_" + columnName + "' data-column-name='" + columnName + "' value='" + columnName + "'/>  "
                                    + ""
                                    + "</div>";

                            $("#selectedColLabelsDiv").append(columnLabelData);

                            tablesGroupObj[tableName] = tableName;
                        }

                    });
                    $('[id^="analyticsCubeValueInner"]').droppable({ //cube changes
                        revert: "invalid",
                        refreshPositions: true,
                        cursor: 'move',
                        drop: function (event, ui) {
                            var label = ui.draggable[0].innerText
                            var columnName = ui.draggable[0].innerText;
                            var tableName = "";
                            var connType = "";
                            var selectedItem = $('#availableAnalyticsConnections').jqxTree('getItem', ui.draggable[0]);
                            var selectedParentItem = {};
                            try {
                                selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedItem.parentElement);
                                if (selectedParentItem != null) {
                                    tableName = selectedParentItem['value'];
                                    selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedParentItem.parentElement);
                                    connType = selectedParentItem['value'];
                                }
                            } catch (e) {
                            }
                            var columnData = '<div id="COLUMN_' + tableName + "_" + columnName + '" class="analyticsDivData"' //group
                                    + ' title="' + columnName + '" data-table-name="' + tableName + '" '
                                    + ' data-column-name="' + columnName + '" data-coon-type ="' + connType + '" ><span class="visionColsText" ondblclick="selectColumnFun(this,\'' + tableName + "." + columnName + '\',\'analyticsColumnsInner\')" >' + columnName
                                    + '</span><img src="images/close_white.png" title="Remove Column"'
                                    + ' onclick="removeColumn(this,\'' + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;">'
                                    + '</div>';
                            $("#" + this.id).append(columnData);
                            var columnLabelData = "<div id='COL_LBL_DIV_" + columnName + "' data-column-name='" + columnName + "' class='columnLabelDivs'>"
                                    + "<label>" + columnName + ":</label>"
                                    + "<input type='text' id='COL_" + columnName + "' data-column-name='" + columnName + "' value='" + columnName + "'/>  "
                                    + ""
                                    + "</div>";

                            $("#selectedColLabelsDiv").append(columnLabelData);

                            tablesGroupObj[tableName] = tableName;
                        }

                    });
                    $('[id^="analyticsCubePossibleDimFactColValueInner"]').droppable({//cube changes
                        revert: "invalid",
                        refreshPositions: true,
                        cursor: 'move',
                        drop: function (event, ui) {
                            var label = ui.draggable[0].innerText
                            var columnName = ui.draggable[0].innerText;
                            var tableName = "";
                            var connType = "";
                            var selectedItem = $('#availableAnalyticsConnections').jqxTree('getItem', ui.draggable[0]);
                            var selectedParentItem = {};
                            try {
                                selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedItem.parentElement);
                                if (selectedParentItem != null) {
                                    tableName = selectedParentItem['value'];
                                    selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedParentItem.parentElement);
                                    connType = selectedParentItem['value'];
                                }
                            } catch (e) {
                            }
                            var columnData = '<div id="COLUMN_' + tableName + "_" + columnName + '" class="analyticsDivData"' //group
                                    + ' title="' + columnName + '" data-table-name="' + tableName + '" '
                                    + ' data-column-name="' + columnName + '" data-coon-type ="' + connType + '" ><span class="visionColsText" ondblclick="selectColumnFun(this,\'' + tableName + "." + columnName + '\',\'analyticsColumnsInner\')" >' + columnName
                                    + '</span><img src="images/close_white.png" title="Remove Column"'
                                    + ' onclick="removeColumn(this,\'' + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;">'
                                    + '</div>';
                            $("#" + this.id).append(columnData);
                            var columnLabelData = "<div id='COL_LBL_DIV_" + columnName + "' data-column-name='" + columnName + "' class='columnLabelDivs'>"
                                    + "<label>" + columnName + ":</label>"
                                    + "<input type='text' id='COL_" + columnName + "' data-column-name='" + columnName + "' value='" + columnName + "'/>  "
                                    + ""
                                    + "</div>";
                            $("#selectedColLabelsDiv").append(columnLabelData);
                            tablesGroupObj[tableName] = tableName;
                        }

                    });
                    $('[id^="analyticsCubePossibleDimColValueInner"]').droppable({//cube changes
                        revert: "invalid",
                        refreshPositions: true,
                        cursor: 'move',
                        drop: function (event, ui) {
                            var label = ui.draggable[0].innerText
                            var columnName = ui.draggable[0].innerText;
                            var tableName = "";
                            var connType = "";
                            var selectedItem = $('#availableAnalyticsConnections').jqxTree('getItem', ui.draggable[0]);
                            var selectedParentItem = {};
                            try {
                                selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedItem.parentElement);
                                if (selectedParentItem != null) {
                                    tableName = selectedParentItem['value'];
                                    selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedParentItem.parentElement);
                                    connType = selectedParentItem['value'];
                                }
                            } catch (e) {
                            }
                            var columnData = '<div id="COLUMN_' + tableName + "_" + columnName + '" class="analyticsDivData"' //group
                                    + ' title="' + columnName + '" data-table-name="' + tableName + '" '
                                    + ' data-column-name="' + columnName + '" data-coon-type ="' + connType + '" ><span class="visionColsText" ondblclick="selectColumnFun(this,\'' + tableName + "." + columnName + '\',\'analyticsColumnsInner\')" >' + columnName
                                    + '</span><img src="images/close_white.png" title="Remove Column"'
                                    + ' onclick="removeColumn(this,\'' + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;">'
                                    + '</div>';
                            $("#" + this.id).append(columnData);
                            var columnLabelData = "<div id='COL_LBL_DIV_" + columnName + "' data-column-name='" + columnName + "' class='columnLabelDivs'>"
                                    + "<label>" + columnName + ":</label>"
                                    + "<input type='text' id='COL_" + columnName + "' data-column-name='" + columnName + "' value='" + columnName + "'/>  "
                                    + ""
                                    + "</div>";
                            $("#selectedColLabelsDiv").append(columnLabelData);
                            tablesGroupObj[tableName] = tableName;
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
}
//new fetchChartConfigForm ravi//
function addToDataProperty(targetDivId) {
    var selectedItem = $("#availableAnalyticsConnections").jqxTree('getSelectedItem');
    if (selectedItem != null && !jQuery.isEmptyObject(selectedItem)) {
        var selectedItem = $('#availableAnalyticsConnections').jqxTree('getSelectedItem');
        var tableName = "";

        var selectedParentItem = {};
        try {
            selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedItem.parentElement);
            if (selectedParentItem != null) {
                tableName = selectedParentItem['value'];
                selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedParentItem.parentElement);
            }
        } catch (e) {
        }
        if (selectedParentItem != null
                && selectedParentItem['value'] != null
                && selectedParentItem['value'] != 'null'
                && selectedParentItem['value'] != 'null'
                && (selectedParentItem['value'] == 'TABLES'
                        || selectedParentItem['value'] == 'VIEWS'
                        || selectedParentItem['value'] == 'SYNONYMS'
                        || selectedParentItem['value'] == 'FILES'
                        )
                ) {
            var columnName = selectedItem['value'];
            var columnData = '<div id="COLUMN_' + tableName + "_" + columnName + '" class="analyticsDivData"'  //group
                    + ' title="' + columnName + '" data-table-name="' + tableName + '" '
                    + ' data-column-name="' + columnName + '" data-coon-type ="' + selectedParentItem['value'] + '" ><span class="visionColsText" ondblclick="selectColumnFun(this,\'' + tableName + "." + columnName + '\',\'' + targetDivId + '\')">' + columnName
                    + '</span><img src="images/close_white.png" title="Remove Column"'
                    + ' onclick="removeColumn(this,\'' + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;"></div>';
            $("#" + targetDivId).append(columnData);
            var columnLabelData = "<div id='COL_LBL_DIV_" + columnName + "' data-column-name='" + columnName + "' class='columnLabelDivs'>"
                    + "<label>" + columnName + ":</label>"
                    + "<input type='text' id='COL_" + columnName + "' data-column-name='" + columnName + "' value='" + columnName + "'/>  "
                    + ""
                    + "</div>";
            $("#analyticsJoinClauseInner").empty();
            var joinClauseImage = "<img src='images/mapping.svg' id='showjoinMapping' class='visionEtlMapTableIcon visionEtlJoinClauseMapIcon' title='Map Columns For Join'"
                    + "                   onclick='showJoinsPopup()' style='width:15px;height: 15px;cursor:pointer;float:left;'/>";
            $("#analyticsJoinClauseImg").html(joinClauseImage); //joins
            $("#analyticsWhereClauseInner").empty();
            var whereClauseImage = "<img src='images/mapping.svg' id='showWhereMapping' class='visionEtlMapTableIcon visionEtlWhereClauseMapIcon' title='Map Columns For Where'"
                    + " onclick = 'showWhereClauseOnPopup()' style = 'width:15px;height: 15px;cursor:pointer;float:left;' / > ";
            $("#analyticsWhereClauseImg").html(whereClauseImage); //joins
            $("#selectedColLabelsDiv").append(columnLabelData);
            $("#analyticsInnerJoinClause").empty();  //group
            tablesGroupObj[tableName] = tableName;
        }

    }
}
function removeColumn($this, columnName) {
    $($this).parent().remove();
    $("#COL_LBL_DIV_" + columnName).remove();
}
function applyChartProperties() {
    showLoader();
    var columns = {};
    var tablesObj = [];
    $("#filterParamArray").val("");
    var errorCount = 0;
    var chartType = $("#visionChartConfigOptionForm").attr("data-chartType");
    var chartTitle = $("#chartTitle").val();
  var chartId =   $("#visionChartConfigOptionForm").attr("data-chartId");
 var chartFormId =   $("#visionChartConfigOptionForm").attr("data-chartFormId");
    $('#analyticsColumns div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        var aggragateColumnName = $("#" + divId).attr("data-aggragate-name");
        if (tableName != null
                && tableName != ''
                && columnName != null && columnName != '') {
            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            columnObj['aggragateColumnName'] = aggragateColumnName;
            columns[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))
            {
                tablesObj.push(tableName);
            }
        }

    });

    console.log("columns:::" + JSON.stringify(columns));
    var rows = {};
    $('#analyticsRows div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        var aggragateColumnName = $("#" + divId).attr("data-aggragate-name");
        if (tableName != null
                && tableName != ''
                && columnName != null && columnName != '') {

            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            columnObj['aggragateColumnName'] = aggragateColumnName;
            rows[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))
            {
                tablesObj.push(tableName);
            }
        }

    });
    var comboRows = {};
    $('#analyticsComboRows div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        var aggragateColumnName = $("#" + divId).attr("data-aggragate-name");
        if (tableName != null
                && tableName != ''
                && columnName != null && columnName != '') {

            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            columnObj['aggragateColumnName'] = aggragateColumnName;
            comboRows[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))
            {
                tablesObj.push(tableName);
            }
            
        }
    });

    if (comboRows != null && comboRows != '' && comboRows != undefined &&
            Object.keys(comboRows).length > 1) {
        errorCount++;
        errorMessage("#disanalyticsComboRows", "Columns Should not be Greater than one");
    } else if (chartType != null && chartType != '' && chartType == 'ComboChart'
            && !(comboRows != null && comboRows != '' && comboRows != undefined &&
                    !jQuery.isEmptyObject(comboRows)))
    {
        errorCount++;
        errorMessage("#disanalyticsComboRows", "Should not be null");
    }
    var joinQuery = $("#analyticsInnerJoinClause").val();
    if (tablesObj != null && !jQuery.isEmptyObject(tablesObj) && Object.keys(tablesObj).length > 1
            && !(joinQuery != null && joinQuery != '' && joinQuery != undefined)) {
        errorCount++;
        errorMessage("#disanalyticsJoinClause", "Join columns should not be null");
    }
    //selectedColLabelsDiv
    var columnsLabel = {};
    $('#selectedColLabelsDiv div').each(function (i, ele) {
        var colLabelName = $(this).attr("data-column-name");
        var columnLabel = $("#COL_" + colLabelName).val();
        if (columnLabel != null && columnLabel != '') {
            columnsLabel[colLabelName] = columnLabel;
        } else {
            columnsLabel[colLabelName] = colLabelName;
        }
    });
    var sqlQuery = $("#analyticsSQL").val();
    if ((columns != null && !jQuery.isEmptyObject(columns))
            || (rows != null && !jQuery.isEmptyObject(rows))
            || (sqlQuery != null && sqlQuery != '' && $.trim(sqlQuery) != null)
            ) {
        errorCount == 0;
    } else {
        errorMessage("#disanalyticsColumns", "Should not be null");
        errorMessage("#disanalyticsRows", "Should not be null");
        errorCount == 1;
    }
    if (!(chartTitle != null && chartTitle != '')) {
        errorCount++;
        errorMessage("#dischartTitle", "Should not be null");
    }
    // get options
    //visionChartConfigOptionForm
    var chartOptionObj = {};
    var chartOptAllObj = {};
    var chartOptIdObjStr = $("#chartOptIdObjStr").val();
    var errorMessageStr = "";
    $('#chartOptionsOlList li').each(function (i, ele) {
        var optObj = {};
        var parent = $(this).parent().parent().get(0).children;
        var parentName = parent[0].innerText.trim();
        if (parentName != null && parentName != '' && parentName != undefined)
        {
            parentName = parentName.replace(":", ">");
        }
        var optColName = $(this).attr("data-column-name");
        var optName = $("#" + optColName).attr("data-opt-name");
        var optRegex = $("#" + optColName).attr("data-regex");
        var optRegexMesg = $("#" + optColName).attr("data-regex-msg");
        var optMan = $("#" + optColName).attr("data-man");
        var inputType = $("#" + optColName).attr("type");
        var optValue = $("#" + optColName).val();
        if (inputType == 'checkbox') {
            if ($("#" + optColName).is(':checked')) {
                optValue = true;
            } else {
                optValue = false;
            }
        }
        if (inputType == 'number') { //nested
            if (optValue != null && optValue != '' && optValue >= 1) {
                optValue = parseInt(optValue);
            }
        }
        
        if (optValue != null && optValue != '') {
            chartOptAllObj[optColName] = optValue;
        }else if (optMan == 'M') {
            errorCount++;
            if (parentName != null) {
                errorMessageStr += "<tr><td>  " + '<p class="visionGenericTabStatusDialog">' + " " + '<span style="color:blue;">' + "" + parentName + " " + optName + "</span><b>:</b> Should not be null.</tr></td>";
            } else {
                errorMessageStr += "<tr><td>  " + '<p class="visionGenericTabStatusDialog">' + " " + '<span style="color:blue;">' + " " + optName + "</span><b>:</b> Should not be null.</tr></td>";
            }
//            errorMessage("#dis"+optColName, "Should not be null");
        }

    });
    console.log("chartOptAllObj:::" + JSON.stringify(chartOptAllObj));


    console.log("rows:::" + JSON.stringify(rows));
    var whereClauseCond = $("#analyticsWhereClauseInner").text(); //joins
    var groupByCond = $("#analyticsInnerGroupClause").val();
    if (errorCount == 0) {
        hideErrors();
        $.ajax({
            type: "post",
            traditional: true,
            dataType: 'json',
            url: "generateChartByProps",
            data: {
                columns: JSON.stringify(columns),
                rows: JSON.stringify(rows),
                chartType: chartType,
                chartTitle: chartTitle,
                whereClauseCond: whereClauseCond,
                columnsLabel: JSON.stringify(columnsLabel),
                sqlQuery: sqlQuery,
                chartId: chartId,
                chartFormId: chartFormId,
                chartOptionObj: JSON.stringify(chartOptAllObj),
                comboRows: JSON.stringify(comboRows),
                joinCondition: joinQuery,
                groupByCond: groupByCond,
                chartDivId: 'designAreaAnalyticsDisplayDiv'
            },
            cache: false,
            success: function (data, status, xhr) {
                stopLoader();
                
                if (data != null && !jQuery.isEmptyObject(data)) {
                     var jqwidgetFlag = data['jqwidgetFlag'];
                     $("#designAreaAnalyticsRefreshButtonsDiv").empty(); //filter
                    $("#designAreaAnalyticsFilterButtonsDiv").empty();
                    $("#designAreaAnalyticsGridButtonsDiv").empty();
                    if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Y') {
                        var chartsObj = data['settingObj'];
                        var chartId = data['chartId'];
                        var height = chartsObj['height'];
                        var width = chartsObj['width'];
                        var finalChartObj = {};
                        finalChartObj = data['settingObj'];
                        finalChartObj.source = data['dataObjList'];
                        var settings = {};
                        settings = finalChartObj;
                        $("#" + chartId).css("width", "" + width + "", "important");
                        $("#" + chartId).css("height", "" + height + "", "important");
                        $("#" + chartId).jqxChart(settings);
                        $("#" + chartId).on('click', function (e) //nested
                        {
                            if (e.args != null) {
                                selectJqwidgetClickHandler(e.args.serie.dataField, e.args.elementValue, chartId);
                            }
                        });
                        var paramArray = []; //filter
                        $("#designAreaAnalyticsGridButtonsDiv").append('<img src="images/GridDB.png" class="visionAnalyticSuiteGridData" title="Grid Data" onclick="showNestedGrid(' + paramArray + ')">');
                        $("#designAreaAnalyticsFilterButtonsDiv").append('<img src="images/change_requests_icon_2.png" class="visionAnalyticSuiteFilterData" title="Filter Form" onclick="showChartFilterForm()">');
                        $("#designAreaAnalyticsRefreshButtonsDiv").append('<img src="images/refresh.png" class="visionAnalyticSuiteRefreshData" title="Refresh Chart" onclick="showRefreshChart()">');

                    } else {
                        var dataArray = [];
                        var options = {};
                        var input = "<input type='hidden' id='dataObj'/><input type='hidden' id='dataOption'/>"
                        $("#designAreaAnalyticsDisplayDiv").html(input);
                        $("#dataObj").val(JSON.stringify(data['dataObjList']));
                        $("#dataOption").val(JSON.stringify(data['optionObj']));
                        dataArray = JSON.parse($("#dataObj").val());
                        options = JSON.parse($("#dataOption").val());
                        var chartInit = data['chartInit'];
                        var chart = eval(chartInit);
    //                var chart = objInit1;
    //               var chart = eval(chartInit);
                        dataArray = google.visualization.arrayToDataTable(data['dataObjList']);
                        chart.draw(dataArray, options);
                        google.visualization.events.addListener(chart, 'select', function () { //nested
                            clickHandler(chart, dataArray, chartId);
                        });
                       var paramArray = []; //filter
                        $("#designAreaAnalyticsGridButtonsDiv").append('<img src="images/GridDB.png" class="visionAnalyticSuiteGridData" title="Grid Data" onclick="showNestedGrid(' + paramArray + ')">');
                        $("#designAreaAnalyticsFilterButtonsDiv").append('<img src="images/change_requests_icon_2.png" class="visionAnalyticSuiteFilterData" title="Filter Form" onclick="showChartFilterForm()">');
                        $("#designAreaAnalyticsRefreshButtonsDiv").append('<img src="images/refresh.png" class="visionAnalyticSuiteRefreshData" title="Refresh Chart" onclick="showRefreshChart()">');
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
        stopLoader();
        $("#dialog").html(errorMessageStr);
        $("#dialog").dialog({
            title: (labelObject['Error'] != null ? labelObject['Error'] : 'Error'),
            modal: true,
            height: 'auto',
            minHeight: 'auto',
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
    }

}



function showAnalyticsTablesListInTab() {
    showLoader();
    //var startIndex = parseInt(startIndx);
    var endIndex = $("#treePageSize").val();
    var connectionObj = globalTreeObj['connObj'];
    var tableListHtml = '';
    var selectedItem = $('#availableAnalyticsConnections').jqxTree('selectedItem');

    var selecteddbLiItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedItem.parentElement);
    var parentItemLabel = selecteddbLiItem.value;
    var selectedItemLabel = selectedItem.label;
    var liId = ('li-' + parentItemLabel + '-' + selectedItemLabel);
    var divId = ('div-' + parentItemLabel + '-' + selectedItemLabel);
    currentTabId = divId;
    // var splitterHeight = (parseInt($(window).height()) - parseInt(pageHeight));
    var value = parentItemLabel;
    var parentkey = selectedItem.value;
    var level = selectedItem.level;

    var filterId = divId.replace("div_", "filter_");
    var filterInput = '<div class ="visionFilterObjectsDiv"><input id="' + filterId + '" class="visionFilterObjectsInput" onkeyup="filterAnalyticsTables(event)" type="text" autocomplete="off" placeholder="Search Tables" value=""/></div>'

    var selectedItemIndex = $('#jqxtabs').jqxTabs('selectedItem');

    if (selectedItemIndex != null) {
        var html = $('#jqxtabs').html();
        if (html.indexOf(divId) > -1) {
            var length = $('#jqxtabs').jqxTabs('length');
            for (var i = 0; i < length; i++) {
                var content = $('#jqxtabs').jqxTabs('getContentAt', i);
                var id = $(content).attr("id");
                if ($(content).attr("id") == divId) {
                    showAnalyticsList("schemaAnalyticsObjectsDiv", "schemaAnalyticsObjects");
                    $('#jqxtabs').jqxTabs('select', i);
                    break;
                }
            }
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

    if (selecteddbLiItem != null) {
        var selectedParentValue = selecteddbLiItem['value'];
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
        data: data,
        success: function (data, status, xhr) {
//            $(".showEtlIcons").css("margin-top", "223px");
//            $(".showEtlIcons").css("margin-left", "-265");
            stopLoader();
            showAnalyticsList("schemaAnalyticsObjectsDiv", "schemaAnalyticsObjects");
            stopLoader();
            tableListHtml += '<div id ="' + divId + '_div">';
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
                tabContent.attr("id", currentTabId);
                tabContent.attr("class", "visionObjectsListDiv");

                var filterId = currentTabId.replace("div-", "filter-");
                var filterInput = '<div class ="visionFilterObjectsDiv"><input id="' + filterId + '" class="visionFilterObjectsInput" onkeyup="filterAnalyticsTables(event)" autocomplete="off"  type="text" placeholder="Search Tables" value=""/></div>'
                tabContent.prepend(filterInput);
                $("#jqxtabs").find("li.jqx-tabs-title").find("div.jqx-tabs-close-button").css("background-image", "url(images/close.png)");
                $("#jqxtabs").find("li.jqx-tabs-title-selected-top").find("div.jqx-tabs-close-button").css("background-image", "url(images/close_white.png)");

                //var title = $('#jqxtabs').jqxTabs('getTitleAt',event.args.item);
                var selectedTabLi = $("#jqxtabs").find("li.jqx-tabs-title-selected-top");
//                var cancelButton = selectedTabLi.find("div.jqx-tabs-close-button-selected");
//                cancelButton.css("background-image","url(images/close_white.png)");
                selectedTabLi.attr("title", currentTabId.replace("div-", "").replace("-", "."));


                // $('#jqxtabs').jqxTabs('select', event.args.item); 
            });



            var timeout;
            $(".visionObjectsListDiv").unbind("scroll").on("scroll", function (event) {
                console.log("scrolled ;;")
                if (($(this).scrollTop() + $(this).innerHeight() >= $(this)[0].scrollHeight) && ($(this)[0].scrollHeight > $(this).innerHeight())) {
                    console.log("iam in scroll functionality...........");
                    clearTimeout(timeout);
                    timeout = setTimeout(function () {
                        loadOnScrollDownAnaytics(event);
                    }, 50);

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


function viewAnalyticsTableData(DBValue, parentkey) {
//    var DBValue;
    var tableName;
    var connName;
//    var parentkey;
    var connectionObj = globalTreeObj['connObj'];

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
            var liItems = $("#availableAnalyticsConnections").find("li :contains('" + DBValue + "')");
            $.each(liItems, function (index) {
                var liItem = $('#availableAnalyticsConnections').jqxTree('getItem', this);
                if (liItem != null && liItem.level == 3 && liItem.value == DBValue) {

                    var connItem = this.parentElement.parentElement;
                    var selectedConnItem = $('#availableAnalyticsConnections').jqxTree('getItem', connItem);
                    if (selectedConnItem != null && selectedConnItem.level == 2) {
                        connName = selectedConnItem.value;
                    }

                    return false;
                }

            })


        } else {
            var parentItem = $('#availableAnalyticsConnections').jqxTree('getSelectedItem');
            tableName = parentItem.label;
            parentkey = parentItem.value
            var $element = $(parentItem.element);

            var dbItem = parentItem.element.parentElement.parentElement.parentElement.parentElement;

            //var dbItemvalue = parentItem.element.parentElement.parentElement.parentElement.parentElement.children[2].title;

            var selectedDBItem = $('#availableAnalyticsConnections').jqxTree('getItem', dbItem);
            DBValue = selectedDBItem['value'];

            var connItem = dbItem.parentElement.parentElement;
            var selectedConnItem = $('#availableAnalyticsConnections').jqxTree('getItem', connItem);
            if (selectedConnItem.level == 2) {
                connName = selectedConnItem.value;
            }


        }
    }

    if (!(tableName != null && tableName != '')) {
        tableName = parentkey;
    }



    var contentDivId = ("divGrid-" + DBValue + "-" + tableName).replace(/\s/g, '');
    var selectedItemIndex = $('#analyticDataViewDiv').jqxTabs('selectedItem');
    if (selectedItemIndex != null) {
        var html = $('#analyticDataViewDiv').html();
        if (html.indexOf(contentDivId) > -1
                || html.indexOf('"' + contentDivId.replace(/\//g, '') + '"') > -1) {
            var length = $('#analyticDataViewDiv').jqxTabs('length');
            for (var i = 0; i < length; i++) {
                var content = $('#analyticDataViewDiv').jqxTabs('getContentAt', i);
                var id = $(content).children('div:nth-child(2)').attr("id");
                if (id == null) {
                    id = $(content).children('div:first').children('div:nth-child(2)').attr("id");
                }
                if (id == contentDivId || id == contentDivId.replace(/\//g, '')) {
                    switchAnalyticsTabs("li_contentView", "analyticDataViewDiv");
                    $('#analyticDataViewDiv').jqxTabs('select', i);
                    break;
                }
            }
            return false;
        }

    }




    if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
    {
        var conObj = savedDBData[DBValue];
        if (conObj != null && !jQuery.isEmptyObject(conObj)) {
            connectionObj = conObj;
        }

    }

    var data = {
        parentkey: parentkey,
        treeId: globalTreeObj['treeId'],
        level: '2',
        extTreeParams: extTreeParams,
        columnsObj: JSON.stringify(columnsObj),
        connectionObj: JSON.stringify(connectionObj),
        startIndex: 0,
        endIndex: $("#treePageSize").val(),
        DBValue: DBValue,
        tableName: tableName,
        analytics: "Y"
    };

    viewAnalyticsTableDataGrid(data);

}

function viewAnalyticsTableDataGrid(data) {
    showLoader();
    var DBValue = data.DBValue;
    var tableName = data.tableName;
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
                var totalCount = response['totalCount'];
                var gridId = ("divGrid-" + DBValue + "-" + tableName).replace(/\s/g, '');
                gridId = gridId.replace(/\//g, '');
                var selectedItemIndex = $('#analyticDataViewDiv').jqxTabs('selectedItem');
                if (selectedItemIndex == null) {

                    //$("#designViewTab").jqxTabs('select', 1);
                    switchAnalyticsTabs("li_contentView", "analyticDataViewDiv");
                    $("#analyticDataViewDiv").prepend("<ul></ul>");
                    $("#analyticDataViewDiv ul").prepend("<li title='" + DBValue + "." + tableName + "'>" + tableName + "</li>");
                    $("#analyticDataViewDiv").append("<div><div><img src='images/refresh_icon.png' style='width:18px;height: 18px;cursor:pointer;padding-left:3px;' onclick=refreshTableGrid('" + gridId + "') title='Refresh'></div><div id='" + gridId + "'></div>"); // ravi edit for tabs navigation
                    $('#analyticDataViewDiv').jqxTabs({width: "100%", height: "100%", position: 'top', theme: 'ui-redmond', reorder: true});

                    $('#analyticDataViewDiv').jqxTabs('showAllCloseButtons');
                    $("#analyticDataViewDiv").find("li.jqx-tabs-title").find("div.jqx-tabs-close-button").css("background-image", "url(images/close.png)");
                    $("#analyticDataViewDiv").find("li.jqx-tabs-title-selected-top").find("div.jqx-tabs-close-button").css("background-image", "url(images/close_white.png)");



                } else {
                    switchAnalyticsTabs("li_contentView", "analyticDataViewDiv");
                    $('#analyticDataViewDiv').jqxTabs('addLast', DBValue + "." + tableName, '<div><div><img src="images/refresh_icon.png" style="width:18px;height: 18px;cursor:pointer;padding-left:3px;" onclick=refreshTableGrid("' + gridId + '") title="Refresh"></div><div id="' + gridId + '"></div></div>'); // ravi edit for tabs navigation
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
                                $('#analyticDataViewDiv').css("width", "100%");
                            }, loadComplete: function (data)
                            {
                                $('#analyticDataViewDiv').css("width", "100%");
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

                            },
                            filter: function () {

                                $("[id='" + gridId + "']").jqxGrid('updatebounddata', 'filter');
                                try {
                                    $("[id='" + gridId + "']").jqxGrid('clearselection');
                                } catch (e) {
                                }

                            }
                        };
//                var source =
//                        {
//                            localdata: dataArray,
//                            datatype: "array",
//                            datafields: dataFieldsArray
//                        };
                var dataAdapter = new $.jqx.dataAdapter(source);

                $("[id='" + gridId + "']").jqxGrid(
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
                            pagesizeoptions: ['50', '100', '500'],
                            rendergridrows: function (params) {
                                return params.data;
                            },
                            columnsresize: true,
                            columns: columnsArray
                        });





                $('#analyticDataViewDiv').unbind('selected').on('selected', function (event) {
                    var $thid = this;

                    $('#analyticDataViewDiv').jqxTabs('getContentAt', i);

                    $("#analyticDataViewDiv").find("li.jqx-tabs-title").find("div.jqx-tabs-close-button").css("background-image", "url(images/close.png)");
                    $("#analyticDataViewDiv").find("li.jqx-tabs-title-selected-top").find("div.jqx-tabs-close-button").css("background-image", "url(images/close_white.png)");

                });

                $('#analyticDataViewDiv').unbind('add').on('add', function (event) {

                    $("#analyticDataViewDiv").find("li.jqx-tabs-title").find("div.jqx-tabs-close-button").css("background-image", "url(images/close.png)");
//                    $("#analyticDataViewDiv").find("li.jqx-tabs-title-selected-top").find("div.jqx-tabs-close-button").css("background-image", "url(images/close_white.png)");
                    setTimeout(function () {
                        $("#analyticDataViewDiv").find("li.jqx-tabs-title-selected-top").find("div.jqx-tabs-close-button").css("background-image", "url(images/close_white.png)");

                    }, 100);
                    var selectedTabTitle = $("#analyticDataViewDiv").jqxTabs("getTitleAt", event.args.item);
                    $("#analyticDataViewDiv").jqxTabs('setTitleAt', event.args.item, selectedTabTitle.split(".")[1]);
                    var selectedTabLi = $("#analyticDataViewDiv").find("li.jqx-tabs-title-selected-top");
                    selectedTabLi.attr("title", selectedTabTitle);

                    var selectedItem = $('#analyticDataViewDiv').jqxTabs('selectedItem'); // ravi edit for tabs issue
                    $('#analyticDataViewDiv').jqxTabs('ensureVisible', selectedItem); // ravi edit for tabs issue


                });



            }

        },
        error: function (e)
        {
            stopLoader();
            sessionTimeout(e);
        }

    });

}
function switchAnalyticsTabs(liId, divId) {
    $("#" + liId).parent().find('li.visionETLDesignTabHighLight').removeClass('visionETLDesignTabHighLight');
    $("#" + liId).addClass('visionETLDesignTabHighLight');
    // $("#contentSplitter").hide();
    $("#dataAnalyticsMainSplitter").hide();
    $("#analyticDataViewDiv").hide();
    $("#" + divId).show();

}

function treePagingAnalytics(selectedLevelValue, selectConnObj, selectedItem, selectedLevel, selectColumnsObj, selectedDataBase, selectedParentValue)
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
        var pagesize = "50"; //$("#treePageSize").val();
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
                    $("#availableAnalyticsConnections").jqxTree('removeItem', children[i], false);
                }

                $('#availableAnalyticsConnections').jqxTree('addTo', data, selectedItem);
                var items = $('#availableAnalyticsConnections').jqxTree('getItems');
                $.each(items, function () {
                    $(this.titleElement).attr('title', this.label);
                });
                try {
                    var itemsLists = selectedItem.children[3];
                    var childrens = itemsLists['children'];
                    var childData = childrens[childrens['length'] - 1];
                    var childrenData = $('#availableAnalyticsConnections').jqxTree('getItem', childData);
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
                $("#availableAnalyticsConnections").append(hiddenPagingField);
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

function loadOnScrollDownAnaytics(event) {
    showLoader();
    var divId = $(event.target).attr("id");
    var value = divId.split("-")[1];
    var parentkey = divId.split("-")[2].toUpperCase();
    ;
    var level = '4';
    var arr = $("[id='" + divId + "']").find(".visionObjectNameDiv");

    var startIndex = arr.length + 1;
    var endIndex = arr.length + parseInt($("#treePageSize").val());
    var connectionObj = globalTreeObj['connObj'];
    var tableListHtml = '';

    if (savedDBData != null && !jQuery.isEmptyObject(savedDBData))
    {
        var conObj = savedDBData[value];
        if (conObj != null && !jQuery.isEmptyObject(conObj)) {
            connectionObj = conObj;
        }

    }
    var item = $("#availableAnalyticsConnections").jqxTree('getSelectedItem');
    var url = 'getTreePagingDataOpt';
    if (item != null)
    {
        var level = item['level'];
        var parentEventItem = item['parentElement'];
        for (var i = level; i > 0; i--)
        {
            parentEventItem = parentEventItem['parentElement'];
        }
        var selectedItem = $('#availableAnalyticsConnections').jqxTree('getItem', parentEventItem);
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
            stopLoader();
            $.each(data, function (index) {
                if (this.label == "Show More...") {

                } else {
                    tableListHtml += '<div title=' + connectionObj['CONNECTION_NAME'] + ' . ' + this.label + ' class = "visionObjectNameDiv">\n\
<img class="visionTableIcon" src="images/GridDB.png"/><span> ' + this.label + '</span></div>'
                }
            });
            $("#" + divId).append(tableListHtml);


        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });


}

function filterAnalyticsTables(event) {

    var inputValue = $(event.target).val();
//if (inputValue.length<3){
//    return false;
//}
    var value = '%' + $(event.target).val() + '%';
    var inputId = $(event.target).attr("id");
    var divId = inputId.replace("filter-", "div-");
    var dbLabel = inputId.split("-")[1];

    var selectedLevelValue = inputId.split("-")[2].toUpperCase();
    var selectConnObj = savedDBData[dbLabel];
    var selectedLevel = '2'
    var selectColumnsObj = globalTreeObj['treeColumnObj']
    var selectBoxValue = "LIKE"

    selectedLevelValue = selectedLevelValue.toUpperCase();
    var item = $("#availableAnalyticsConnections").jqxTree('getSelectedItem');
    var url = 'getTreePagingDataOpt';
    if (item != null)
    {
        var level = item['level'];
        var parentEventItem = item['parentElement'];
        for (var i = level; i > 0; i--)
        {
            parentEventItem = parentEventItem['parentElement'];
        }
        var selectedItem = $('#availableAnalyticsConnections').jqxTree('getItem', parentEventItem);
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
            var tableListHtml = "";
            stopLoader();

            $.each(data, function (index) {
                if (this.label == "Show More...") {

                } else {
                    tableListHtml += '<div class = "visionObjectNameDiv" style="cursor:pointer"><img class="visionTableIcon" src="images/GridDB.png"/><span> ' + this.label + '</span></div>'
                }
            });

//            var filterInput = '<div class ="visionFilterObjectsDiv"><input id="' + inputId + '" class="visionFilterObjectsInput" onkeyup="filterAnalyticsTables(event)" type="text" placeholder="Search Tables" /></div>'
//            $("[id='" + divId + "']").html(filterInput + tableListHtml);
//            $("[id='" + inputId + "']").focus();
//            $("[id='" + inputId + "']").val(inputValue);
            $("[id='" + divId + "_div']").html(tableListHtml);



        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });

}


function filterAnalyticsMappingTables()
{
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    var item = $("#availableAnalyticsConnections").jqxTree('getSelectedItem');
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
                        $("#availableAnalyticsConnections").append(hiddenField);
                        var jsFilterObj = {};
                        jsFilterObj.filterType = selectBoxValue;
                        jsFilterObj.filterValue = value;
                        $("#" + hiddenFieldId).val(JSON.stringify(jsFilterObj));
                        showLoader();
                        getAnalyticsFilteredValues(value, item['value'], selectConnObj, item['level'], selectColumnsObj, selectBoxValue);

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
                        $("#availableAnalyticsConnections").append(hiddenField);
                        $("#treeAnalyticFilterImage").attr('src', "images/Filter Icon-01.svg");
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

function getAnalyticsFilteredValues(value, selectedLevelValue, selectConnObj, selectedLevel, selectColumnsObj, selectBoxValue)
{
    selectedLevelValue = selectedLevelValue.toUpperCase();
    var item = $("#availableAnalyticsConnections").jqxTree('getSelectedItem');
    var url = 'getTreePagingDataOpt';
    if (item != null)
    {
        var level = item['level'];
        var parentEventItem = item['parentElement'];
        for (var i = level; i > 0; i--)
        {
            parentEventItem = parentEventItem['parentElement'];
        }
        var selectedItem = $('#availableAnalyticsConnections').jqxTree('getItem', parentEventItem);
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
            var selectedItem = $("#availableAnalyticsConnections").jqxTree('getSelectedItem');
            var count;
            var children;
            if (selectedItem != null) {
                children = $(selectedItem.element).find('li');
                count = children.length;
                for (var i = 0; i < count; i += 1) {
                    if (i < count - 1) {
                        $("#availableAnalyticsConnections").jqxTree('removeItem', children[i], false);
                    } else {
                        $("#availableAnalyticsConnections").jqxTree('removeItem', children[i], true);
                    }
                }
            }


            $("#availableAnalyticsConnections").jqxTree('addTo', data, selectedItem);
            $('#availableAnalyticsConnections').jqxTree('expandItem', selectedItem);
            var items = $("#availableAnalyticsConnections").jqxTree('getItems');
            $.each(items, function () {
                $(this.titleElement).attr('title', this.label);

            });
            var itemsList = $("#" + selectedItem.id);
            try {
                var itemsLists = itemsList[0].children[3];
                var childrens = itemsLists['children'];
                var childData = childrens[childrens['length'] - 1];
                var childrenData = $("#availableAnalyticsConnections").jqxTree('getItem', childData);
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
            $("#treeAnalyticFilterImage").attr('src', "images/Filter Icon2-01.svg");
//            $("#availableAnalyticsConnections").jqxTree('selectItem', null);
            var hiddenPagingId = "DATABASE_" + selectedDataBase + "_" + selectedLevelValue + "paging__hidden";
            hiddenPagingId = hiddenPagingId.replace(/ /g, '_');
            hiddenPagingId = hiddenPagingId.replace(/\//g, '_');
            $("#" + hiddenPagingId).remove();
            var hiddenPagingField = "<input type='hidden' id='" + hiddenPagingId + "'value=''/>";
            $("#availableAnalyticsConnections").append(hiddenPagingField);
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
function showChildOptions(masterColumnName, $this) {
    if ($($this).is(":checked")) {
        $("#" + masterColumnName + "_DIV").show();
    } else {
        $("#" + masterColumnName + "_DIV").hide();
    }

}
function populateSelectedColor(id, inputId, selectionFlag) {
    inputId = $.trim(inputId);
    if (selectionFlag == 'S') {
        $("#" + inputId).val($("#" + id).val());

    } else {
        var inputIdVal = $("#" + inputId).val();
        if (inputIdVal != null && inputIdVal != '') {
            $("#" + inputId).val(inputIdVal + "," + $("#" + id).val());
        } else {
            $("#" + inputId).val($("#" + id).val());
        }

       $("#" + inputId + "_CLR_DIV").css('display','block');
        $("#" + inputId + "_CLR_DIV").append("<div style='background-color:" + $("#" + id).val() + ";position: relative;float: left;margin-left: 10px;width:30px;height:30px'><img src='images/X_icon-04.svg' title='Remove Color' "
                + " onclick=removeColor(this,\'" + inputId + "\',\'" + $("#" + id).val() + "\') class='visionCloseColorBox' style=''></div>");
//        $("#" + inputId + "_CLR_DIV").append("<span><input class='visionMultipleColorBox' type='color' "
//                + " value='" + $("#" + id).val() + "'/><img src='images/X_icon-04.svg' title='Remove Color' "
//                + " onclick=removeColor(this,\'" + inputId + "\',\'" + $("#" + id).val() + "\') class='visionCloseColorBox' style='display: inline;'></span>");
    }
}
function resetChartProperties() {
    $("#analyticsColumnsInner").empty(); //nested
    $("#analyticsRowsInner").empty(); //nested
    $("#analyticsComboRowsInner").empty(); //nested
    $("#chartTitle").val("");
    var chartId = $("#visionChartConfigOptionForm").attr("data-chartId");
    var chartType = $("#visionChartConfigOptionForm").attr("data-chartType");
    var chartFormId = $("#visionChartConfigOptionForm").attr("data-chartFormId");
    $("#designAreaAnalyticsDisplayDiv").empty(); //nested
    $("#designAreaAnalyticsButtonsDiv").empty(); //nested
    $("#analyticsJoinClauseInner").empty(); //nested
    $("#analyticsInnerJoinClause").empty(); //nested
    $("#analyticsWhereClauseInner").empty(); //nested
    var joinClauseImage = "<img src='images/mapping.svg' id='showjoinMapping' class='visionEtlMapTableIcon visionEtlJoinClauseMapIcon' title='Map Columns For Join'"
            + "                   onclick='showJoinsPopup()' style='width:15px;height: 15px;cursor:pointer;float:left;'/>";
    $("#analyticsJoinClauseImg").html(joinClauseImage); //nested
    $("#analyticsWhereClauseInner").empty//nested
    var whereClauseImage = "<img src='images/mapping.svg' id='showWhereMapping' class='visionEtlMapTableIcon visionEtlWhereClauseMapIcon' title='Map Columns For Where'"
            + " onclick = 'showWhereClauseOnPopup()' style = 'width:15px;height: 15px;cursor:pointer;float:left;' / > ";
    $("#analyticsWhereClauseImg").html(whereClauseImage);//nested
    $("#analyticsWhereClauseImg").html(whereClauseImage);//nested
    fetchChartConfigForm(chartId, chartFormId);
}
function removeColor($this, inputId, colorCode) {
    $($this).parent().remove();
    var inputIdVal = $("#" + inputId).val();
    if (inputIdVal != null && inputIdVal != '' && inputIdVal.indexOf(colorCode) > -1) {
        inputIdVal = inputIdVal.replace("," + colorCode, "");
        inputIdVal = inputIdVal.replace(colorCode + ",", "");
        inputIdVal = inputIdVal.replace(colorCode, "");
        $("#" + inputId).val(inputIdVal);
        if ( !($("#" + inputId).val() != null &&  $("#" + inputId).val() != '')) {
             $("#" + inputId + "_CLR_DIV").css('display','');
             
        }
    }
}

function hideMainOptions(id) {
    var hiddenPropVal = $('#saveHiddenProp').val();
    if (hiddenPropVal != null && hiddenPropVal != '') {
        $('#saveHiddenProp').val(hiddenPropVal + "," + id);
    } else {
        $('#saveHiddenProp').val(id);
    }
    $("#LI_" + id).hide();

}

function showHiddenFields() {

    var hiddenPropVal = $('#saveHiddenProp').val();
    if (hiddenPropVal != null && hiddenPropVal != '') {
        var hiddenPropValArr = hiddenPropVal.split(',');
        console.log("hiddenPropVal", +hiddenPropValArr);
        for (var i = 0; i < hiddenPropValArr.length; i++) {

            var showPropVal = hiddenPropValArr[i];
            $("#LI_" + showPropVal).show();

        }
    }


}

function saveAnalytics() {
    showLoader();
    var columns = {};
     var tablesObj = [];
    var errorCount = 0;
    var chartType = $("#visionChartConfigOptionForm").attr("data-chartType");
    var chartTitle = $("#chartTitle").val();
    var chartId = $("#visionChartConfigOptionForm").attr("data-chartId");
    var chartFormId = $("#visionChartConfigOptionForm").attr("data-chartFormId");
    $('#analyticsColumns div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        var aggragateColumnName = $("#" + divId).attr("data-aggragate-name");
        if (tableName != null
                && tableName != ''
                && columnName != null && columnName != '') {
            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            columnObj['aggragateColumnName'] = aggragateColumnName; 
            columns[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))  //saved job
            {
                tablesObj.push(tableName);
            }
            
        }

    });

    console.log("columns:::" + JSON.stringify(columns));
    var rows = {};
    $('#analyticsRows div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        var aggragateColumnName = $("#" + divId).attr("data-aggragate-name");
        if (tableName != null
                && tableName != ''
                && columnName != null && columnName != '') {

            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            columnObj['aggragateColumnName'] = aggragateColumnName; 
            rows[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))  //saved job
            {
                tablesObj.push(tableName);
            }
        }

    });
    var comboRows = {};
    $('#analyticsComboRows div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        var aggragateColumnName = $("#" + divId).attr("data-aggragate-name"); //group
        if (tableName != null
                && tableName != ''
                && columnName != null && columnName != '') {

            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            columnObj['aggragateColumnName'] = aggragateColumnName; //group
            comboRows[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))
            {
                tablesObj.push(tableName);
            }

        }
    });
    if (comboRows != null && comboRows != '' && comboRows != undefined &&
            Object.keys(comboRows).length > 1) {
        errorCount++;
        errorMessage("#disanalyticsComboRows", "Columns Should not be Greater than one");
    } else if (chartType != null && chartType != '' && chartType == 'ComboChart'
            && !(comboRows != null && comboRows != '' && comboRows != undefined &&
                    !jQuery.isEmptyObject(comboRows)))
    {
        errorCount++;
        errorMessage("#disanalyticsComboRows", "Should not be null");
    }
    var joinQuery = $("#analyticsInnerJoinClause").val();
    if (tablesObj != null && !jQuery.isEmptyObject(tablesObj) && Object.keys(tablesObj).length > 1
            && !(joinQuery != null && joinQuery != '' && joinQuery != undefined)) {
        errorCount++;
        errorMessage("#disanalyticsJoinClause", "Join columns should not be null");
    }
    //selectedColLabelsDiv
    
    var columnsLabel = {};
    $('#selectedColLabelsDiv div').each(function (i, ele) {
        var colLabelName = $(this).attr("data-column-name");
        var columnLabel = $("#COL_" + colLabelName).val();
        
        if (columnLabel != null && columnLabel != '') {
            columnsLabel[colLabelName] = columnLabel;
        } else {
            columnsLabel[colLabelName] = colLabelName;
        }
    });
    var sqlQuery = $("#analyticsSQL").val();
    if ((columns != null && !jQuery.isEmptyObject(columns))
            || (rows != null && !jQuery.isEmptyObject(rows))
            || (sqlQuery != null && sqlQuery != '' && $.trim(sqlQuery) != null)
            ) {
        errorCount == 0;
    } else {
        errorMessage("#disanalyticsColumns", "Should not be null");
        errorMessage("#disanalyticsRows", "Should not be null");
        errorCount == 1;
    }
    if (!(chartTitle != null && chartTitle != '')) {
        errorCount++;
        errorMessage("#dischartTitle", "Should not be null");
    }
    // get options
    //visionChartConfigOptionForm
    var errorMessageStr = "";
    var chartOptionObj = {};
    var chartOptAllObj = {};
    var chartOptIdObjStr = $("#chartOptIdObjStr").val();
    $('#chartOptionsOlList li').each(function (i, ele) {
        var optObj = {};
        var parent = $(this).parent().parent().get(0).children;
        var parentName = parent[0].innerText.trim();
        if (parentName != null && parentName != '' && parentName != undefined)
        {
            parentName = parentName.replace(":", ">");
        }
        var optColName = $(this).attr("data-column-name");
        var optName = $("#" + optColName).attr("data-opt-name");
        var optRegex = $("#" + optColName).attr("data-regex");
        var optRegexMesg = $("#" + optColName).attr("data-regex-msg");
        var optMan = $("#" + optColName).attr("data-man");
        var inputType = $("#" + optColName).attr("type");
        var optValue = $("#" + optColName).val();
        if (inputType == 'checkbox') {
            if ($("#" + optColName).is(':checked')) {
                optValue = true;
            } else {
                optValue = false;
            }
        }
        if (inputType == 'number') { //nested
           if (optValue != null && optValue != '' && optValue >= 1) {
                optValue = parseInt(optValue);
            }
        }

        if (optValue != null && optValue != '') {
            chartOptAllObj[optColName] = optValue;
        } else if (optMan == 'M') {
            errorCount++;
            if (parentName != null) {
                errorMessageStr += "<tr><td>  " + '<p class="visionGenericTabStatusDialog">' + " " + '<span style="color:blue;">' + "" + parentName + " " + optName + "</span><b>:</b> Should not be null.</tr></td>";
            } else {
                errorMessageStr += "<tr><td>  " + '<p class="visionGenericTabStatusDialog">' + " " + '<span style="color:blue;">' + " " + optName + "</span><b>:</b> Should not be null.</tr></td>";
            }
//            errorMessage("#dis" + optColName, "Should not be null");
        }

    });
    console.log("chartOptAllObj:::" + JSON.stringify(chartOptAllObj));


    console.log("rows:::" + JSON.stringify(rows));
    var whereClauseCond = $("#analyticsWhereClauseInner").text(); //joins
    var joinColumnsObj = {};
    var whereClauseColumnsObj = {};
    var whereClauseQueryObj = {};
    if (tablesObj != null && !jQuery.isEmptyObject(tablesObj))
    {
        $.each(tablesObj, function (index) {
            if (index != 0) {
                var joinColumns = $("#joinConditionsMap_" + index + "_hidden").val();
                if (joinColumns != null && joinColumns != '' && joinColumns != undefined)
                {
                    joinColumns = JSON.parse(joinColumns);
                    joinColumnsObj['joinConditionsMap_' + index] = joinColumns;
                }
            }
            var whereClauseArea = $("#whereClauseConditionsArea_" + index + "_hidden").val();
            var whereClauseMap = $("#whereClauseConditionsMap_" + index + "_hidden").val();
            if (whereClauseArea != null && whereClauseArea != null && whereClauseArea != undefined)
            {
                if (whereClauseMap != null && whereClauseMap != null && whereClauseMap != undefined)
                {
                    whereClauseMap = JSON.parse(whereClauseMap);
                    whereClauseColumnsObj['whereClauseConditionsMap_' + index] = whereClauseMap;
                }
                whereClauseQueryObj['whereClauseConditionsArea_' + index] = whereClauseArea;
            }
        });
    }
    if (errorCount == 0) {
        hideErrors();
        stopLoader();
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
                            saveDataAnalytics(jobName, columns, rows, chartType, chartTitle, whereClauseCond, columnsLabel, sqlQuery, chartId, 
                                                           chartFormId, chartOptAllObj, joinQuery, comboRows, joinColumnsObj, whereClauseColumnsObj,whereClauseQueryObj);
                            $(this).html("");
                            $(this).dialog("close");
                            $(this).dialog("destroy");


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
        stopLoader();
        $("#dialog").html(errorMessageStr);
        $("#dialog").dialog({
            title: (labelObject['Error'] != null ? labelObject['Error'] : 'Error'),
            modal: true,
            height: 'auto',
            minHeight: 'auto',
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
    }

}

function saveDataAnalytics(jobName, columns, rows, chartType, chartTitle, whereClauseCond, columnsLabel, sqlQuery,
        chartId, chartFormId, chartOptAllObj, joinQuery, comboRows, joinColumnsObj, whereClauseColumnsObj,whereClauseQueryObj)
{
    showLoader();
    var groupByCond = $("#analyticsInnerGroupClause").val(); //saved job
    var groupByColumnsObj = $("#analyticsGroupClauseInner").val();
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: "saveAnylytics",
        data: {
            jobName: jobName,
            columns: JSON.stringify(columns),
            rows: JSON.stringify(rows),
            chartType: chartType,
            chartTitle: chartTitle,
            whereClauseCond: whereClauseCond,
            columnsLabel: JSON.stringify(columnsLabel),
            sqlQuery: sqlQuery,
            chartId: chartId,
            chartFormId: chartFormId,
            chartOptionObj: JSON.stringify(chartOptAllObj),
            joinQuery: joinQuery, //saved job
            groupByCond: groupByCond, //saved job
            comboRows: JSON.stringify(comboRows), //saved job
            joinColumnsObj: JSON.stringify(joinColumnsObj), //saved job  
            groupByColumnsObj: groupByColumnsObj, //saved job
            whereClauseColumnsObj: JSON.stringify(whereClauseColumnsObj), //saved job
            whereClauseQueryObj: JSON.stringify(whereClauseQueryObj)
        },
        cache: false,
        success: function (response, status, xhr) {
            stopLoader();
            if (response != null && !jQuery.isEmptyObject(response)) {
                $("#dialog").html(response['message']);
                $("#dialog").dialog({
                    title: (labelObject['Output'] != null ? labelObject['Output '] : 'Output'),
                    modal: true,
                    height: 'auto',
                    minWidth: 300,
                    maxWidth: 'auto',
                    fluid: true,
                    buttons: [{
                            text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                            click: function () {
                                showSavedAnalyticsJobs();
                                $(this).html("");
                                $(this).dialog("close");
                                $(this).dialog("destroy");
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
        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
}


function showSavedAnalyticsJobs(newJobId) {
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'fetchSavedAnalyticsJobs',
        cache: false,
        data: {},

        success: function (response) {
            ajaxStop();
            if (response != null) {
                var jobIdList = response['jobIdList'];
                var jobDescrList = response['jobDescrList'];
                var jobAnalyticIdList = response['jobAnalyticIdList'];
                var jobFormIdList = response['jobFormIdList'];
                var availableJobshtmlStr;
                if (jobIdList.length > 0) {
                    availableJobshtmlStr = '<div id="avaialableAnalyticsJobsTree">\n\
                                                <ul>\n\
                                                    <li>Saved Jobs\n\
                                                        <ul>';
                } else
                {
                    availableJobshtmlStr = '<div id="avaialableAnalyticsJobsTree">\n\
                                                <ul>\n\
                                                    <li>No available Jobs\n\
                                                        <ul>';
                }

                var imgIconStr = '<img src="images/Process Icon-01.svg" class="visionETLIcons"  style="width:15px;height: 15px;cursor:pointer;"/>';
                $.each(jobIdList, function (index) {
                    availableJobshtmlStr += '<li><div id="' + jobIdList[index] + '" class="visionETLAvailableJobs" ondblclick="openSavedJob(event,\'' + jobIdList[index] + '\',\'' + jobDescrList[index] + '\',\'' + jobAnalyticIdList[index] + '\',\'' + jobFormIdList[index] + '\')">' + imgIconStr + '<span style="cursor:pointer;">  ' + (jobDescrList[index]).toUpperCase() + '</span></div></li>'
                });
                availableJobshtmlStr += '</ul></li></ul></div>';

                $("#availableAnalyticsJobsId").html(availableJobshtmlStr);
                $('#avaialableAnalyticsJobsTree').jqxTree({height: '900px', width: '300px'});
                $('#avaialableAnalyticsJobsTree').jqxTree('expandAll');
                var currentJobItem = $("#" + newJobId).parents("li:first").find("div:first")[0];
                $("#avaialableAnalyticsJobsTree").find(".selectJqxTreeItem").removeClass("selectJqxTreeItem");
                $(currentJobItem).addClass("selectJqxTreeItem");
                $('#avaialableAnalyticsJobsTree').jqxTree('expandAll');


            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });


}


function openSavedJob(event, jobId, jobDescr, id, formType)//saved job
{
    ajaxStart();
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'openSavedAnalyticsJobs',
        cache: false,
        data: {
            jobId: jobId,
            chartId: id,
            chartFormId: formType
        },
        success: function (data) {
            ajaxStop();
            var response = data;
            if (response != null)
            {
                var chartTitle = response['chartTitle'];
                var whereClause = response['whereClause'];
                var joinQuery = response['joinQuery'];
                var columnsList = response['columnsList'];
                var rowsList = response['rowsList'];
                var comboRowsList = response['comboRowsList'];
                var joinColumnObj = response['joinColumnObj'];
                var groupByCond = response['groupByCond'];
                var groupByColumnsObj = response['groupByColumnsObj'];
                var whereClauseColumnObj = response['whereClauseColumnObj'];
                var whereClauseQueryObj = response['whereClauseQueryObj'];
                $("#selectedColLabelsDiv").empty();
                if (columnsList != null && columnsList != '' && columnsList != undefined)
                {
                    columnsList = JSON.parse(columnsList);
                    $.each(columnsList, function (index) {
                        var colList = columnsList[index];
                        var columnName = colList['columnName'];
                        var connType = colList['connType'];
                        var tableName = colList['tableName'];
                        var aggragateColumnName = colList['aggragateColumnName'];
                        var columnData = "";
                        var columnLabelData = "";
                        if (aggragateColumnName != null && aggragateColumnName != '' && aggragateColumnName != undefined) {
                            var subStr = aggragateColumnName.split("(");
                            subStr = subStr[0];
                            var colStr = subStr + "(" + columnName;
                            columnData = '<div id="COLUMN_' + tableName + "_" + columnName + '" class="analyticsDivData"'  //group
                                    + ' title="' + colStr + '" data-table-name="' + tableName + '" data-aggragate-name="' + aggragateColumnName + '"'
                                    + ' data-column-name="' + columnName + '" data-coon-type ="' + connType + '" ><span class="visionColsText">' + colStr
                                    + '</span><img src="images/close_white.png" title="Remove Column"'
                                    + ' onclick="removeColumn(this,\'' + tableName + "_" + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;"></div>';
                            columnLabelData = "<div id='COL_LBL_DIV_" + tableName + "_" + columnName + "' data-column-name='" + tableName + "_" + columnName + "' class='columnLabelDivs'>"
                                    + "<label>" + colStr + ":</label>"
                                    + "<input type='text' id='COL_" + tableName + "_" + columnName + "' data-column-name='" + colStr + "' value='" + colStr + "'/>  "
                                    + ""
                                    + "</div>";
                        } else
                        {
                            columnData = '<div id="COLUMN_' + tableName + "_" + columnName + '" class="analyticsDivData"'  //group
                                    + ' title="' + columnName + '" data-table-name="' + tableName + '" '
                                    + ' data-column-name="' + columnName + '" data-coon-type ="' + connType + '" ><span class="visionColsText" ondblclick="selectColumnFun(this,\'' + tableName + "." + columnName + '\',\'analyticsColumnsInner\')">' + columnName
                                    + '</span><img src="images/close_white.png" title="Remove Column"'
                                    + ' onclick="removeColumn(this,\'' + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;"></div>';
                            columnLabelData = "<div id='COL_LBL_DIV_" + columnName + "' data-column-name='" + columnName + "' class='columnLabelDivs'>"
                                    + "<label>" + columnName + ":</label>"
                                    + "<input type='text' id='COL_" + columnName + "' data-column-name='" + columnName + "' value='" + columnName + "'/>  "
                                    + ""
                                    + "</div>";
                        }
                        $("#analyticsColumnsInner").empty();
                        $("#analyticsColumnsInner").append(columnData);
                        $("#analyticsJoinClauseInner").empty();
                        $("#analyticsInnerJoinClause").empty(); //group
                        var joinClauseImage = "<img src='images/mapping.svg' id='showjoinMapping' class='visionEtlMapTableIcon visionEtlJoinClauseMapIcon' title='Map Columns For Join'"
                                + "                   onclick='showJoinsPopup()' style='width:15px;height: 15px;cursor:pointer;float:left;'/>";
                        $("#analyticsJoinClauseImg").html(joinClauseImage); //joins
                        $("#analyticsWhereClauseInner").empty();
                        var whereClauseImage = "<img src='images/mapping.svg' id='showWhereMapping' class='visionEtlMapTableIcon visionEtlWhereClauseMapIcon' title='Map Columns For Where'"
                                + " onclick = 'showWhereClauseOnPopup()' style = 'width:15px;height: 15px;cursor:pointer;float:left;' / > ";
                        $("#analyticsWhereClauseImg").html(whereClauseImage); //joins
                        $("#selectedColLabelsDiv").append(columnLabelData);
                        tablesGroupObj[tableName] = tableName; //group
                    });
                }
                if (rowsList != null && rowsList != '' && rowsList != undefined)
                {
                    rowsList = JSON.parse(rowsList);
                    $.each(rowsList, function (index) {
                        var rowList = rowsList[index];
                        var columnName = rowList['columnName'];
                        var connType = rowList['connType'];
                        var tableName = rowList['tableName'];
                        var aggragateColumnName = rowList['aggragateColumnName'];
                        var columnData = "";
                        var columnLabelData = "";
                        if (aggragateColumnName != null && aggragateColumnName != '' && aggragateColumnName != undefined)
                        {
                            var subStr = aggragateColumnName.split("(");
                            subStr = subStr[0];
                            var colStr = subStr + "(" + columnName;
                            columnData = '<div id="COLUMN_' + tableName + "_" + columnName + '" class="analyticsDivData"'  //group
                                    + ' title="' + colStr + '" data-table-name="' + tableName + '" data-aggragate-name="' + aggragateColumnName + '"'
                                    + ' data-column-name="' + columnName + '" data-coon-type ="' + connType + '" ><span class="visionColsText">' + colStr
                                    + '</span><img src="images/close_white.png" title="Remove Column"'
                                    + ' onclick="removeColumn(this,\'' + tableName + "_" + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;"></div>';
                            columnLabelData = "<div id='COL_LBL_DIV_" + tableName + "_" + columnName + "' data-column-name='" + tableName + "_" + columnName + "' class='columnLabelDivs'>"
                                    + "<label>" + colStr + ":</label>"
                                    + "<input type='text' id='COL_" + tableName + "_" + columnName + "' data-column-name='" + colStr + "' value='" + colStr + "'/>  "
                                    + ""
                                    + "</div>";
                        } else
                        {
                            columnData = '<div id="COLUMN_' + tableName + "_" + columnName + '" class="analyticsDivData"' //group
                                    + ' title="' + columnName + '" data-table-name="' + tableName + '" '
                                    + ' data-column-name="' + columnName + '" data-coon-type ="' + connType + '" ><span class="visionColsText" ondblclick="selectColumnFun(this,\'' + tableName + "." + columnName + '\',\'analyticsRowsInner\')">' + columnName
                                    + '</span><img src="images/close_white.png" title="Remove Column"'
                                    + ' onclick="removeColumn(this,\'' + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;"></div>';
                            columnLabelData = "<div id='COL_LBL_DIV_" + columnName + "' data-column-name='" + columnName + "' class='columnLabelDivs'>"
                                    + "<label>" + columnName + ":</label>"
                                    + "<input type='text' id='COL_" + columnName + "' data-column-name='" + columnName + "' value='" + columnName + "'/>  "
                                    + ""
                                    + "</div>";
                        }
                        $("#analyticsRowsInner").empty();
                        $("#analyticsRowsInner").append(columnData);
                        $("#analyticsJoinClauseInner").empty();
                        $("#analyticsInnerJoinClause").empty(); //group
                        var joinClauseImage = "<img src='images/mapping.svg' id='showjoinMapping' class='visionEtlMapTableIcon visionEtlJoinClauseMapIcon' title='Map Columns For Join'"
                                + "                   onclick='showJoinsPopup()' style='width:15px;height: 15px;cursor:pointer;float:left;'/>";
                        $("#analyticsJoinClauseImg").html(joinClauseImage); //joins
                        $("#analyticsWhereClauseInner").empty();
                        var whereClauseImage = "<img src='images/mapping.svg' id='showWhereMapping' class='visionEtlMapTableIcon visionEtlWhereClauseMapIcon' title='Map Columns For Where'"
                                + " onclick = 'showWhereClauseOnPopup()' style = 'width:15px;height: 15px;cursor:pointer;float:left;' / > ";
                        $("#analyticsWhereClauseImg").html(whereClauseImage); //joins
                        $("#selectedColLabelsDiv").append(columnLabelData);
                        tablesGroupObj[tableName] = tableName; //group
                    });
                }
                if (comboRowsList != null && comboRowsList != '' && comboRowsList != undefined)
                {
                    comboRowsList = JSON.parse(comboRowsList);
                    $.each(comboRowsList, function (index) {
                        var colList = comboRowsList[index];
                        var columnName = colList['columnName'];
                        var connType = colList['connType'];
                        var tableName = colList['tableName'];
                        var aggragateColumnName = colList['aggragateColumnName'];
                        var columnData = "";
                        var columnLabelData = "";
                        if (aggragateColumnName != null && aggragateColumnName != '' && aggragateColumnName != undefined) {
                            var subStr = aggragateColumnName.split("(");
                            subStr = subStr[0];
                            var colStr = subStr + "(" + columnName;
                            columnData = '<div id="COLUMN_' + tableName + "_" + columnName + '" class="analyticsDivData"'  //group
                                    + ' title="' + colStr + '" data-table-name="' + tableName + '" data-aggragate-name="' + aggragateColumnName + '"'
                                    + ' data-column-name="' + columnName + '" data-coon-type ="' + connType + '" ><span class="visionColsText">' + colStr
                                    + '</span><img src="images/close_white.png" title="Remove Column"'
                                    + ' onclick="removeColumn(this,\'' + tableName + "_" + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;"></div>';
                            columnLabelData = "<div id='COL_LBL_DIV_" + tableName + "_" + columnName + "' data-column-name='" + tableName + "_" + columnName + "' class='columnLabelDivs'>"
                                    + "<label>" + colStr + ":</label>"
                                    + "<input type='text' id='COL_" + tableName + "_" + columnName + "' data-column-name='" + colStr + "' value='" + colStr + "'/>  "
                                    + ""
                                    + "</div>";
                        } else {
                            columnData = '<div id="COLUMN_' + tableName + "_" + columnName + '" class="analyticsDivData"'  //group
                                    + ' title="' + columnName + '" data-table-name="' + tableName + '" '
                                    + ' data-column-name="' + columnName + '" data-coon-type ="' + connType + '" ><span class="visionColsText" ondblclick="selectColumnFun(this,\'' + tableName + "." + columnName + '\',\'analyticsColumnsInner\')">' + columnName
                                    + '</span><img src="images/close_white.png" title="Remove Column"'
                                    + ' onclick="removeColumn(this,\'' + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;"></div>';
                            columnLabelData = "<div id='COL_LBL_DIV_" + columnName + "' data-column-name='" + columnName + "' class='columnLabelDivs'>"
                                    + "<label>" + columnName + ":</label>"
                                    + "<input type='text' id='COL_" + columnName + "' data-column-name='" + columnName + "' value='" + columnName + "'/>  "
                                    + ""
                                    + "</div>";
                        }
                        $("#analyticsComboRowsInner").empty();
                        $("#analyticsComboRowsInner").append(columnData);
                        $("#analyticsJoinClauseInner").empty();
                        $("#analyticsInnerJoinClause").empty(); //group
                        var joinClauseImage = "<img src='images/mapping.svg' id='showjoinMapping' class='visionEtlMapTableIcon visionEtlJoinClauseMapIcon' title='Map Columns For Join'"
                                + "                   onclick='showJoinsPopup()' style='width:15px;height: 15px;cursor:pointer;float:left;'/>";
                        $("#analyticsJoinClauseImg").html(joinClauseImage); //joins
                        $("#analyticsWhereClauseInner").empty();
                        var whereClauseImage = "<img src='images/mapping.svg' id='showWhereMapping' class='visionEtlMapTableIcon visionEtlWhereClauseMapIcon' title='Map Columns For Where'"
                                + " onclick = 'showWhereClauseOnPopup()' style = 'width:15px;height: 15px;cursor:pointer;float:left;' / > ";
                        $("#analyticsWhereClauseImg").html(whereClauseImage); //joins
                        $("#selectedColLabelsDiv").append(columnLabelData);
                        tablesGroupObj[tableName] = tableName; //group
                    });
                }
                $("#analyticsWhereClauseInner").html(whereClause);
                $("#analyticsJoinClauseInner").html(joinQuery);
                $("#analyticsInnerJoinClause").val(joinQuery);
                $("#analyticsInnerGroupClause").val(groupByCond);
                $("#analyticsGroupClauseInner").val(groupByColumnsObj);
                $("#chartTitle").val(chartTitle);
                if (joinColumnObj != null && joinColumnObj != '' && joinColumnObj != undefined)
                {
                    joinColumnObj = JSON.parse(joinColumnObj);
                    $.each(joinColumnObj, function (key, value) {
                        $("#" + key + "_hidden").remove();
                        var columnJoinMapping = joinColumnObj[key];
                        $("#analyticsJoinClauseImg").append("<input type='hidden' id='" + key + "_hidden' value='" + JSON.stringify(columnJoinMapping) + "'/>");
                    });
                }
                if (whereClauseColumnObj != null && whereClauseColumnObj != '' && whereClauseColumnObj != undefined)
                {
                    whereClauseColumnObj = JSON.parse(whereClauseColumnObj);
                    $.each(whereClauseColumnObj, function (key, value) {
                        $("#" + key + "_hidden").remove();
                        var columnJoinMapping = whereClauseColumnObj[key];
                        $("#analyticsJoinClauseImg").append("<input type='hidden' id='" + key + "_hidden' value='" + JSON.stringify(columnJoinMapping) + "'/>");
                    });
                }
                if (whereClauseQueryObj != null && whereClauseQueryObj != '' && whereClauseQueryObj != undefined)
                {
                    whereClauseQueryObj = JSON.parse(whereClauseQueryObj);
                    $.each(whereClauseQueryObj, function (key, value) {
                        $("#" + key + "_hidden").remove();
                        var whereClauseQuery = whereClauseQueryObj[key];
                        $("#analyticsJoinClauseImg").append("<input type='hidden' id='" + key + "_hidden' value=\"" + whereClauseQuery + "\"/>");
                    });
                }
                $("#designAreaAnalyticsDisplayDiv").remove(); //filter
                var designDivs = "<div id ='designAreaAnalyticsButtonsDiv' class='designAreaAnalyticsButtonsClass'>"
                        + "  <div id='designAreaAnalyticsGridButtonsDiv' class ='designAreaAnalyticsGridButtonsClass'></div>"
                        + "  <div id='designAreaAnalyticsFilterButtonsDiv' class ='designAreaAnalyticsFilterButtonsClass'></div>"
                        + "  <div id='designAreaAnalyticsRefreshButtonsDiv' class ='designAreaAnalyticsRefreshButtonsClass'></div>"
                        + "  </div>"
                        + "  <div id ='designAreaAnalyticsDisplayDiv' class='designAreaAnalyticsDisplayClass'></div>";
                $("#designAreaDisplayDiv").html(designDivs); //filter
                fetchChartConfigForm(id, formType, jobId);
            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });
}
function deleteAnalyticJob(jobId)
{
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
                    $.ajax({
                        type: "post",
                        traditional: true,
                        dataType: 'json',
                        url: 'deleteAnalyticJob',
                        cache: false,
                        data: {
                            jobId: jobId,
                        },

                        success: function (data) {
                            ajaxStop();
                            if (data != null) {
                                var response = data;

                                var message = response['message'];
                                showMesg(message);
                                showSavedAnalyticsJobs();
                                $("#analyticsClause").val("");
                                $("#analyticsSQL").val("");
                                $("#analyticsRowsInner").empty();
                                $("#analyticsColumnsInner").empty();
                                $("#chartTitle").val("");
                                $("#visionChartConfigOptionForm").empty();
                                $("#selectedColLabelsDiv").empty();
                                $("#analyticsComboRowsInner").empty(); //nested
                                $("#designAreaAnalyticsDisplayDiv").empty(); //nested
                                $("#designAreaAnalyticsButtonsDiv").empty();
                                $("#analyticsJoinClauseInner").empty();
                                $("#analyticsInnerJoinClause").empty(); //group
                                var joinClauseImage = "<img src='images/mapping.svg' id='showjoinMapping' class='visionEtlMapTableIcon visionEtlJoinClauseMapIcon' title='Map Columns For Join'"
                                        + "                   onclick='showJoinsPopup()' style='width:15px;height: 15px;cursor:pointer;float:left;'/>";
                                $("#analyticsJoinClauseImg").html(joinClauseImage); //joins
                                $("#analyticsWhereClauseInner").empty();
                                var whereClauseImage = "<img src='images/mapping.svg' id='showWhereMapping' class='visionEtlMapTableIcon visionEtlWhereClauseMapIcon' title='Map Columns For Where'"
                                        + " onclick = 'showWhereClauseOnPopup()' style = 'width:15px;height: 15px;cursor:pointer;float:left;' / > ";
                                $("#analyticsWhereClauseImg").html(whereClauseImage);

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
function showMesg(message) {
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

function clearAnalytics()
{
    $("#analyticsClause").val("");
    $("#analyticsSQL").val("");
    $("#analyticsRowsInner").empty();
    $("#analyticsColumnsInner").empty();
    $("#chartTitle").val("");
    $("#visionChartConfigOptionForm").empty();
    $("#selectedColLabelsDiv").empty();
   $("#designAreaAnalyticsDisplayDiv").empty();
}

function showQueryAndOptionsData()
{
    showLoader();
    var errorCount = 0;
    var columns = {};
    var tablesObj = [];
    var chartId = $("#visionChartConfigOptionForm").attr("data-chartId");
    var chartFormId = $("#visionChartConfigOptionForm").attr("data-chartFormId");
    var chartType = $("#visionChartConfigOptionForm").attr("data-chartType");
    
    $('#analyticsColumns div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        var aggragateColumnName = $("#" + divId).attr("data-aggragate-name");
        if (tableName != null
                && tableName != ''
                && columnName != null && columnName != '') {
            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            columnObj['aggragateColumnName'] = aggragateColumnName;
            columns[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))
            {
                tablesObj.push(tableName);
            }
        }

    });
    var rows = {};
    $('#analyticsRows div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        var aggragateColumnName = $("#" + divId).attr("data-aggragate-name");
        if (tableName != null
                && tableName != ''
                && columnName != null && columnName != '') {

            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            columnObj['aggragateColumnName'] = aggragateColumnName;
            rows[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))
            {
                tablesObj.push(tableName);
            }
        }

    });

    var comboRows = {};
    $('#analyticsComboRows div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        var aggragateColumnName = $("#" + divId).attr("data-aggragate-name");
        if (tableName != null
                && tableName != ''
                && columnName != null && columnName != '') {

            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            columnObj['aggragateColumnName'] = aggragateColumnName;
            comboRows[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))
            {
                tablesObj.push(tableName);
            }

        }
    });
    if ((columns != null && !jQuery.isEmptyObject(columns))
            || (rows != null && !jQuery.isEmptyObject(rows))
            || (sqlQuery != null && sqlQuery != '' && $.trim(sqlQuery) != null)
            ) {
        errorCount == 0;
    } else {
        errorMessage("#disanalyticsColumns", "Should not be null");
        errorMessage("#disanalyticsRows", "Should not be null");
        errorCount == 1;
    }
    if (comboRows != null && comboRows != '' && comboRows != undefined &&
            Object.keys(comboRows).length > 1) {
        errorCount++;
        errorMessage("#disanalyticsComboRows", "Columns Should not be Greater than one");
    } else if (chartType != null && chartType != '' && chartType == 'ComboChart'
            && !(comboRows != null && comboRows != '' && comboRows != undefined &&
                    !jQuery.isEmptyObject(comboRows)))
    {
        errorCount++;
        errorMessage("#disanalyticsComboRows", "Should not be null");
    }
    var joinQuery = $("#analyticsInnerJoinClause").val();
    if (tablesObj != null && !jQuery.isEmptyObject(tablesObj) && Object.keys(tablesObj).length > 1
            && !(joinQuery != null && joinQuery != '' && joinQuery != undefined)) {
        errorCount++;
        errorMessage("#disanalyticsJoinClause", "Join columns should not be null");
    }
    var sqlQuery = $("#analyticsSQL").val();
    var chartOptAllObj = {};
    var errorMessageStr = "";
    $('#chartOptionsOlList li').each(function (i, ele) {
        var optObj = {};
        var parent = $(this).parent().parent().get(0).children;
        var parentName = parent[0].innerText.trim();
        if (parentName != null && parentName != '' && parentName != undefined)
        {
            parentName = parentName.replace(":", ">");
        }
        var optColName = $(this).attr("data-column-name");
        var optName = $("#" + optColName).attr("data-opt-name");
        var optRegex = $("#" + optColName).attr("data-regex");
        var optRegexMesg = $("#" + optColName).attr("data-regex-msg");
        var optMan = $("#" + optColName).attr("data-man");
        var inputType = $("#" + optColName).attr("type");
        var optValue = $("#" + optColName).val();
        if (inputType == 'checkbox') {
            if ($("#" + optColName).is(':checked')) {
                optValue = true;
            } else {
                optValue = false;
            }
        }
        if (inputType == 'number') { //nested
            if (optValue != null && optValue != '' && optValue >= 1) {
                optValue = parseInt(optValue);
            }
        }

        if (optValue != null && optValue != '') {
            chartOptAllObj[optColName] = optValue;
        } else if (optMan == 'M') {
            errorCount++;
            if (parentName != null) {
                errorMessageStr += "<tr><td>  " + '<p class="visionGenericTabStatusDialog">' + " " + '<span style="color:blue;">' + "" + parentName + " " + optName + "</span><b>:</b> Should not be null.</tr></td>";
            } else {
                errorMessageStr += "<tr><td>  " + '<p class="visionGenericTabStatusDialog">' + " " + '<span style="color:blue;">' + " " + optName + "</span><b>:</b> Should not be null.</tr></td>";
            }
//            errorMessage("#dis" + optColName, "Should not be null");
        }

    });
    var whereClauseCond = $("#analyticsWhereClauseInner").text(); //joins
    var groupByCond = $("#analyticsInnerGroupClause").val();
    if (errorCount == 0) {
        hideErrors();
        $.ajax({
            type: "post",
            traditional: true,
            dataType: 'json',
            url: "generateQueryAndOptions",
            data: {
                columns: JSON.stringify(columns),
                rows: JSON.stringify(rows),
                whereClauseCond: whereClauseCond,
                sqlQuery: sqlQuery,
                chartId: chartId,
                chartFormId: chartFormId,
                chartOptionObj: JSON.stringify(chartOptAllObj),
                comboRows: JSON.stringify(comboRows),
                joinQuery: joinQuery,
                groupByCond: groupByCond
            },
            cache: false,
            success: function (data, status, xhr) {
                stopLoader();
                if (data != null && !jQuery.isEmptyObject(data)) {
                    var dataStr = data['data'];
                    $("#dialog").html(dataStr);
                    $("#dialog").dialog({
                        title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
                        modal: true,
                        width: 600,
                        height: 250,
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
            },
            error: function (e) {
                console.log(e);
                stopLoader();
            }
        });
    } else {
        stopLoader();
        $("#dialog").html(errorMessageStr);
        $("#dialog").dialog({
            title: (labelObject['Error'] != null ? labelObject['Error'] : 'Error'),
            modal: true,
            height: 'auto',
            minHeight: 'auto',
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
    }

}
function publishChart()
{
    showLoader();
    var columns = {};
    var tablesObj = [];
    var errorCount = 0;
    var chartType = $("#visionChartConfigOptionForm").attr("data-chartType");
    var chartTitle = $("#chartTitle").val();
    var chartId = $("#visionChartConfigOptionForm").attr("data-chartId");
    var chartFormId = $("#visionChartConfigOptionForm").attr("data-chartFormId");
    $('#analyticsColumns div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        var aggragateColumnName = $("#" + divId).attr("data-aggragate-name");
        if (tableName != null
                && tableName != ''
                && columnName != null && columnName != '') {
            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            columnObj['aggragateColumnName'] = aggragateColumnName;
            columns[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))
            {
                tablesObj.push(tableName);
            }
        }

    });

    console.log("columns:::" + JSON.stringify(columns));
    var rows = {};
    $('#analyticsRows div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        var aggragateColumnName = $("#" + divId).attr("data-aggragate-name");
        if (tableName != null
                && tableName != ''
                && columnName != null && columnName != '') {

            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            columnObj['aggragateColumnName'] = aggragateColumnName;
            rows[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))
            {
                tablesObj.push(tableName);
            }
        }

    });
    var comboRows = {};
    $('#analyticsComboRows div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        var aggragateColumnName = $("#" + divId).attr("data-aggragate-name");
        if (tableName != null
                && tableName != ''
                && columnName != null && columnName != '') {

            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            columnObj['aggragateColumnName'] = aggragateColumnName;
            comboRows[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))
            {
                tablesObj.push(tableName);
            }
        }
    });

    if (comboRows != null && comboRows != '' && comboRows != undefined &&
            Object.keys(comboRows).length > 1) {
        errorCount++;
        errorMessage("#disanalyticsComboRows", "Columns Should not be Greater than one");
    } else if (chartType != null && chartType != '' && chartType == 'ComboChart'
            && !(comboRows != null && comboRows != '' && comboRows != undefined &&
                    !jQuery.isEmptyObject(comboRows)))
    {
        errorCount++;
        errorMessage("#disanalyticsComboRows", "Should not be null");
    }
    var joinQuery = $("#analyticsInnerJoinClause").val();
    if (tablesObj != null && !jQuery.isEmptyObject(tablesObj) && Object.keys(tablesObj).length > 1
            && !(joinQuery != null && joinQuery != '' && joinQuery != undefined)) {
        errorCount++;
        errorMessage("#disanalyticsJoinClause", "Join columns should not be null");
    }
    //selectedColLabelsDiv
    var columnsLabel = {};
    $('#selectedColLabelsDiv div').each(function (i, ele) {
        var colLabelName = $(this).attr("data-column-name");
        var columnLabel = $("#COL_" + colLabelName).val();
        if (columnLabel != null && columnLabel != '') {
            columnsLabel[colLabelName] = columnLabel;
        } else {
            columnsLabel[colLabelName] = colLabelName;
        }
    });
    var sqlQuery = $("#analyticsSQL").val();
    if ((columns != null && !jQuery.isEmptyObject(columns))
            || (rows != null && !jQuery.isEmptyObject(rows))
            || (sqlQuery != null && sqlQuery != '' && $.trim(sqlQuery) != null)
            ) {
        errorCount == 0;
    } else {
        errorMessage("#disanalyticsColumns", "Should not be null");
        errorMessage("#disanalyticsRows", "Should not be null");
        errorCount == 1;
    }
    if (!(chartTitle != null && chartTitle != '')) {
        errorCount++;
        errorMessage("#dischartTitle", "Should not be null");
    }
    // get options
    //visionChartConfigOptionForm
    var chartOptionObj = {};
    var chartOptAllObj = {};
    var chartOptIdObjStr = $("#chartOptIdObjStr").val();
    var errorMessageStr = "";
    $('#chartOptionsOlList li').each(function (i, ele) {
        var optObj = {};
        var parent = $(this).parent().parent().get(0).children;
        var parentName = parent[0].innerText.trim();
        if (parentName != null && parentName != '' && parentName != undefined)
        {
            parentName = parentName.replace(":", ">");
        }
        var optColName = $(this).attr("data-column-name");
        var optName = $("#" + optColName).attr("data-opt-name");
        var optRegex = $("#" + optColName).attr("data-regex");
        var optRegexMesg = $("#" + optColName).attr("data-regex-msg");
        var optMan = $("#" + optColName).attr("data-man");
        var inputType = $("#" + optColName).attr("type");
        var optValue = $("#" + optColName).val();
        if (inputType == 'checkbox') {
            if ($("#" + optColName).is(':checked')) {
                optValue = true;
            } else {
                optValue = false;
            }
        }
        if (inputType == 'number') { //nested
            if (optValue != null && optValue != '' && optValue >= 1) {
                optValue = parseInt(optValue);
            }
        }

        if (optValue != null && optValue != '') {
            chartOptAllObj[optColName] = optValue;
        } else if (optMan == 'M') {
            errorCount++;
            if (parentName != null) {
                errorMessageStr += "<tr><td>  " + '<p class="visionGenericTabStatusDialog">' + " " + '<span style="color:blue;">' + "" + parentName + " " + optName + "</span><b>:</b> Should not be null.</tr></td>";
            } else {
                errorMessageStr += "<tr><td>  " + '<p class="visionGenericTabStatusDialog">' + " " + '<span style="color:blue;">' + " " + optName + "</span><b>:</b> Should not be null.</tr></td>";
            }
//            errorMessage("#dis"+optColName, "Should not be null");
        }

    });
    console.log("chartOptAllObj:::" + JSON.stringify(chartOptAllObj));


    console.log("rows:::" + JSON.stringify(rows));
    var whereClauseCond = $("#analyticsWhereClauseInner").text(); //joins

    if (errorCount == 0) {
$.ajax({
            type: "post",
            url: "analyticsProp",
            cache: false,
            data: {
          
            },
            traditional: true,
            dataType: 'html',
            success: function (response) {
        hideErrors();
        stopLoader(); 
        response += "<div id='dailog_error_id' style='display:none;color:red'>" + (labelObject['Please give any Comp Id'] != null ? labelObject['Please give new Comp Id'] : 'Please give new Comp Id like MM_ABC_XYZ ') + "</div>";
        $("#dialog").html(response);
        $("#dialog").dialog({
        //title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
           title: (labelObject['Analytics Suit Properties'] != null ? labelObject['Analytics Suit Properties'] : 'Analytics Suit Properties'),
            modal: true,
            height: 'auto',
            minWidth: 450,
            maxWidth: 'auto',
            fluid: true,
            buttons: [{
                    text: (labelObject['YES'] != null ? labelObject['YES'] : 'YES'),
                    click: function () {
                        var compId = $("#reasonId").val();
                        var selectedcompId = $("#reportTittle").val();
                        var chkBoxVal = $("#chktype").val();
//                                if (chkBoxVal && compId != null && compId != '' && selectedcompId != null && selectedcompId != '') { //filter
                                if (chkBoxVal != null && chkBoxVal != '' && chkBoxVal == 'on' && compId != null && compId != '') {
                                    $("#dailog_error_id").hide();
                                    savePublishData(columns, rows, chartType, chartTitle, whereClauseCond, columnsLabel, sqlQuery, chartId, chartFormId, chartOptAllObj, comboRows, compId, selectedcompId, joinQuery)
                                    $(this).html("");
                                    $(this).dialog("close");
                                    $(this).dialog("destroy");
                                } else if (chkBoxVal != null && chkBoxVal != '' && chkBoxVal == 'off' &&compId != null && compId != '') {
                                    $("#dailog_error_id").hide();
                                    savePublishData(columns, rows, chartType, chartTitle, whereClauseCond, columnsLabel, sqlQuery, chartId, chartFormId, chartOptAllObj, comboRows, compId, '', joinQuery);
                                    $(this).html("");
                                    $(this).dialog("close");
                                    $(this).dialog("destroy");
                                } else {
                                    $("#dailog_error_id").fadeIn(1000).show();
                                }


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
     });
    } else {
        stopLoader();
        $("#dialog").html(errorMessageStr);
        $("#dialog").dialog({
            title: (labelObject['Error'] != null ? labelObject['Error'] : 'Error'),
            modal: true,
            height: 'auto',
            minHeight: 'auto',
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
    }


}
function savePublishData(columns, rows, chartType, chartTitle, whereClauseCond, columnsLabel, sqlQuery, chartId, chartFormId, chartOptAllObj, comboRows, compId,selectedcompId, joinQuery) {
    showLoader();
    var groupByCond = $("#analyticsInnerGroupClause").val();
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: "publishChart",
        data: {

            columns: JSON.stringify(columns),
            rows: JSON.stringify(rows),
            chartType: chartType,
            chartTitle: chartTitle,
            whereClauseCond: whereClauseCond,
            columnsLabel: JSON.stringify(columnsLabel),
            sqlQuery: sqlQuery,
            chartId: chartId,
            chartFormId: chartFormId,
            chartOptionObj: JSON.stringify(chartOptAllObj),
            comboRows: JSON.stringify(comboRows),
            compId: compId,
            selectedcompId: selectedcompId,
            joinQuery: joinQuery,
            groupByCond: groupByCond
        },
        cache: false,
        success: function (response) {
            if(response != null && response != '' && response != undefined){
            var errorMessage = response['messageStr'];
            stopLoader();
            if (errorMessage != null && errorMessage != '') {
                $("#dialog").html(errorMessage);
                $("#dialog").dialog({
                    title: (labelObject['Message'] != null ? labelObject['Message '] : 'Message'),
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
        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });

}
//Joins code
function showJoinsPopup()
{
    var tablesObj = [];
    var columns = {};
    $('#analyticsColumns div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        if (tableName != null
                && tableName != '' 
                && columnName != null && columnName != '') {
            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            columns[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))
            {
                tablesObj.push(tableName);
            }
        }

    });
    console.log("columns:::" + JSON.stringify(columns));
    var rows = {};
    $('#analyticsRows div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        
        if (tableName != null
                && tableName != ''
                && columnName != null && columnName != '') {

            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            rows[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))
            {
                tablesObj.push(tableName);
            }
        }

    });
    var comboRows = {};
    $('#analyticsComboRows div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        if (tableName != null
                && tableName != ''
                && columnName != null && columnName != '') {

            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            comboRows[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))
            {
                tablesObj.push(tableName);
            }
        }
    });
    if (tablesObj != null && tablesObj.length > 1)
    {
        $.ajax({
            type: "post",
            traditional: true,
            dataType: 'json',
            url: "chartJoinTables",
            data: {
                'tablesObj': JSON.stringify(tablesObj)
            },
            cache: false,
            success: function (response) {
                if (response != null) {
                    var tabsString = response['tabsString'];
                    $("#dialog").html(tabsString);
//                    $("#dataMigrationTabs").jqxTabs({width: "100%", height: "130px", position: 'top', theme: 'ui-redmond', reorder: true});
//
//                    $('#dataMigrationTabs').unbind('selected').on('selected', function (event) {
//                        $('#iconsdiv').attr('style', 'margin-top:4px !important');
//                    });
                    $("#dialog").dialog({
                        title: (labelObject['Join Clauses'] != null ? labelObject['Join Clauses '] : 'Join Clauses'),
                        modal: true,
                        width: 845,
                        maxWidth: 1015,
                        height: 350,
                        maxHeight: 1000,
                        fluid: true,
                        buttons: [{
                                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                                click: function () {
                                    viewJoinQuery();
                                    $(this).html("");
                                    $(this).dialog("close");
                                    $(this).dialog("destroy");
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
                    var selectedJoinTables = response['selectedJoinTables'];
                    $('#tabs-1').html(selectedJoinTables);
                    $(".visionETLColMapImage").mousedown(function (event) {
                        treeIconClickEvent = event;
                    })
                }
            },
            error: function (e) {
                console.log(e);
                sessionTimeout(e);
                stopLoader();
            }
        });
    } else {
        errorMessage("#disanalyticsJoinClause", "For single table there is no join");
    }
}


function showJoinsTables(event, tableName, tableMapId, iconIndex) {

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
    if (!(joinColumnMapping != null && joinColumnMapping != '' && joinColumnMapping != undefined))
    {
        joinColumnMapping = $("#" + tableMapId + "_hidden").val();
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
            $("#" + currentJoinTableId + "_hidden").remove(); //joins
            $("#analyticsJoinClauseImg").append("<input type='hidden' id='" + currentJoinTableId + "_hidden' value='" + JSON.stringify(columnJoinMapping) + "'/>");
        }
    }
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'html',
        cache: false,
        url: 'fetchChartJoinTablesData',
        async: true,
        data: {
            tableName: tableName,
            sourceTables: JSON.stringify(prevTables),
            iconIndex: iconIndex,
            joinColumnMapping: joinColumnMapping
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

            }

        },
        error: function (e)
        {
            sessionTimeout(e);
            stopLoader();
        }

    });
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
    }  else if (tableColType == 'fromFilterClauseColumn') {
        var dataStr = $("#filterClauseTableColsObj_hidden").val();
        if (dataStr != null && dataStr != '') {
            data = JSON.parse(dataStr);
        }
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
    console.log("$this");
    var prevMapIconId = $("#currentClauseMapId").val();
     $("#analyticsJoinClauseInner").empty();
    if (prevMapIconId != null && prevMapIconId != '' && prevMapIconId != undefined) {
    var currentClauseAreaId = prevMapIconId.replace("whereClauseConditionsMap_", "whereClauseConditionsArea_");
    $("#" + currentClauseAreaId).val(" ");
    $("#" + currentClauseAreaId + "_hidden").remove();
    var query = "";
    var columnClauseMapping = {};
    $("#analyticsWhereClauseImg").append("<input type='hidden' id='" + currentClauseAreaId + "_hidden' value=\"" + query + "\"/>");
    $("#" + prevMapIconId + "_hidden").remove();
    $("#analyticsWhereClauseImg").append("<input type='hidden' id='" + prevMapIconId + "_hidden' value='" + JSON.stringify(columnClauseMapping) + "'/>");
    trfmRulesChanged = true; // ravi edit new
  }
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
            $("#" + currentJoinTableId + "_hidden").remove();
            $("#analyticsJoinClauseImg").append("<input type='hidden' id='" + currentJoinTableId + "_hidden' value='" + JSON.stringify(columnJoinMapping) + "'/>");
        }
    }
    var dialogSplitMessage = dialogSplitIconText((labelObject['Saved Successfully'] != null ? labelObject['Saved Successfully'] : 'Saved Successfully'), "Y");
    $("#dialog1").html(dialogSplitMessage);
    $("#dialog1").dialog({
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
    var query = "<ul><li><font color='#0071c5'>FROM</font>";
    var joinQuery = "FROM";
    var i = 0;
    $("#EtlMappingTable thead tr").each(function () {
        if (i != 0) {
            var tdArray = this.cells;
            if (tdArray != null && tdArray.length != 0) {
                var tableName = $(tdArray[0]).find("option:selected").val();
                var joinTableName = $(tdArray[0]).find("option:selected").val();
                tableName = "<font color='#F911E9'>" + tableName + "</font>"
                if (i == 1) {
                    query += " " + tableName + "</li><li><ul>";
                    joinQuery += " " + joinTableName + " ";
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
                                    var joinChildTableColumn = mappedColObj['childTableColumn'];
                                    if (childTableColumn != null && childTableColumn != '') {
                                        var childTableColumnArray = childTableColumn.split(":");
                                        childTableColumn = "<font color='#F911E9'>" + childTableColumnArray[0] + "</font>." + childTableColumnArray[1];
                                        joinChildTableColumn = " " + childTableColumnArray[0] + "." + childTableColumnArray[1];
                                    }
                                    var masterTableColumn = mappedColObj['masterTableColumn'];
                                    var joinMasterTableColumn = mappedColObj['masterTableColumn'];
                                    if (masterTableColumn != null && masterTableColumn != '') {
                                        var masterTableColumnArray = masterTableColumn.split(":");
                                        masterTableColumn = "<font color='#F911E9'>" + masterTableColumnArray[0] + "</font>." + masterTableColumnArray[1];
                                        joinMasterTableColumn = " " + masterTableColumnArray[0] + "." + masterTableColumnArray[1];
                                    }
                                    if (j == 0) {
                                        query += "<li><font color='#0071c5'> " + mappedColObj['joinType'] + "</font> " + tableName + " <font color='#0071c5'>ON</font> </li><li><ul>"
                                        joinQuery += mappedColObj['joinType'] + " " + joinTableName + " ON ";
                                    }
                                    query += " <li> " + childTableColumn + " <font color='#0071c5'>" + mappedColObj['operator'] + "</font> "
                                            + ((mappedColObj['staticValue'] != null && mappedColObj['staticValue'] != '') ? (" <font color='#FF0000'>'" + mappedColObj['staticValue'] + "'</font> ") : masterTableColumn); //staticValue
                                    joinQuery += joinChildTableColumn + " " + mappedColObj['operator'] + " "
                                            + ((mappedColObj['staticValue'] != null && mappedColObj['staticValue'] != '') ? (mappedColObj['staticValue']) : joinMasterTableColumn) + " "; //staticValue

                                    if (j != parseInt(mapObjLength) - 1) {
                                        query += " <font color='#0071c5'>" + mappedColObj['andOrOperator'] + "</font> ";
                                        joinQuery += " " + mappedColObj['andOrOperator'] + " ";
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
    $("#analyticsJoinClauseInner").html(query);
    $("#analyticsInnerJoinClause").val(joinQuery);
}


// where clause code
function showWhereClauseOnPopup()
{
    var c = 0;
    var tablesObj = [];
    var columns = {};
    var hiddenColumnStr = {};
    $('#analyticsColumns div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        if (tableName != null
                && tableName != ''
                && columnName != null && columnName != '') {
            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            columns[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))
            {
                tablesObj.push(tableName);
                var query = $("#whereClauseConditionsArea_" + c + "_hidden").val();
                hiddenColumnStr[c] = query;
                c++;
            }
        }

    });
    console.log("columns:::" + JSON.stringify(columns));
    var rows = {};
    $('#analyticsRows div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        if (tableName != null
                && tableName != ''
                && columnName != null && columnName != '') {

            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            rows[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))
            {
                tablesObj.push(tableName);
                var query = $("#whereClauseConditionsArea_" + c + "_hidden").val();
                hiddenColumnStr[c] = query;
                c++;
            }
        }

    });
    var comboRows = {};
    $('#analyticsComboRows div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        if (tableName != null
                && tableName != ''
                && columnName != null && columnName != '') {

            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            comboRows[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))
            {
                tablesObj.push(tableName);
                var query = $("#whereClauseConditionsArea_" + c + "_hidden").val();
                hiddenColumnStr[c] = query;
                c++;
            }
        }

    });
    var query = "";
    var columnClauseMapping = {};
    var i = 1;
    var rowCount = $('#selectedTables >tbody >tr').length;
    $("#selectedTables tbody tr").each(function () {
        var tdArray = this.cells;
        if (tdArray != null && tdArray.length != 0) {
            var clauseObj = {};
            var columnName = $(tdArray[1]).find("textarea").val();
            if (columnName != null && columnName != '') {
                query += " " + columnName;
                if ((parseInt(i) - 1) != (parseInt(rowCount) - 1)) {
                    query += " " + "AND";
                }
                clauseObj['columnName'] = columnName;
                columnClauseMapping[i] = clauseObj;
                i++;
            }
        }

    });
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: "chartWhereClauseTables",
        data: {
            'tablesObj': JSON.stringify(tablesObj),
            'hiddenColumnStr': JSON.stringify(hiddenColumnStr)
        },
        cache: false,
        success: function (response) {
            if (response != null) {
                var tabsString = response['tabsString'];
                $("#dialog").html(tabsString);
                $("#dialog").dialog({
                    title: (labelObject['Where Clauses'] != null ? labelObject['Where Clauses '] : 'Where Clauses'),
                    modal: true,
                    width: 845,
                    maxWidth: 1015,
                    height: 350,
                    maxHeight: 1000,
                    fluid: true,
                    buttons: [{
                            text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                            click: function () {
                                viewWhereClause();
                                $(this).html("");
                                $(this).dialog("close");
                                $(this).dialog("destroy");
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
                var selectedWhereTables = response['selectedWhereTables'];
                $('#tabs-3').html(selectedWhereTables);
                $(".visionETLColMapImage").mousedown(function (event) {
                    treeIconClickEvent = event;
                })
            }
        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });
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
                if (columnName != null && columnName != '') {
                    if (operator != null && operator != '' && operator != undefined && operator.indexOf("IN") > -1) //funnel
                    {
                        var whereCon = "";
                        var staticValues = staticValue.split(",");
                        if (staticValues != null && !jQuery.isEmptyObject(staticValues))
                        {
                            whereCon = "(";
                            for (var j = 0; j < staticValues.length; j++) //nested
                            {
                                whereCon += "'" + staticValues[j] + "'";
                                if (j < staticValues.length - 1)
                                {
                                    whereCon += ",";
                                }
                            }
                            whereCon += ")";
                        }
                        query += " " + columnName.replace(":", ".") + " " + operator + " " + whereCon + " ";
                    } else {
                        query += " " + columnName.replace(":", ".") + " " + operator + " '" + staticValue + "' ";
                    }
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
        if (columnClauseMapping != null && !jQuery.isEmptyObject(columnClauseMapping)) {
            var currentClauseAreaId = prevMapIconId.replace("whereClauseConditionsMap_", "whereClauseConditionsArea_");
            $("#" + currentClauseAreaId).val(query);
            $("#" + prevMapIconId).attr("data-whereclause", JSON.stringify(columnClauseMapping));
//            $("#" + prevMapIconId).attr("data-whereclause", JSON.stringify(columnClauseMapping));
//            $.each(columnClauseMapping, function (i, obj) {
//                var tableName = obj['columnName'];
//                if (tableName != null && tableName != '' && tableName != undefined && tableName.indexOf(":") > -1)
//                {
//                    var tableNames = tableName.split(":");
//                    tableName = tableNames[0];
//                }
//                //query = query.replace(/'/g, "");
//                $("#" + tableName + "_where_query").remove();
//                $("#analyticsWhereClauseImg").append("<input type='hidden' id='" + tableName + "_where_query' value=\"" + query + "\"/>");
//            });
            $("#" + currentClauseAreaId + "_hidden").remove();
            $("#analyticsWhereClauseImg").append("<input type='hidden' id='" + currentClauseAreaId + "_hidden' value=\"" + query + "\"/>");
            $("#" + prevMapIconId + "_hidden").remove();
            $("#analyticsWhereClauseImg").append("<input type='hidden' id='" + prevMapIconId + "_hidden' value='" + JSON.stringify(columnClauseMapping) + "'/>");
        }
    }
    var whereClauseTableColsArrayStr = $("#whereClauseTableColsArray_hidden").val();
    var currentTableTreeArray = [];
    if (whereClauseTableColsArrayStr != null && whereClauseTableColsArrayStr != '') {
        var whereClauseTableColsArrayObj = JSON.parse(whsereClauseTableColsArrayStr);
        if (whereClauseTableColsArrayObj != null && !jQuery.isEmptyObject(whereClauseTableColsArrayObj)) {
            currentTableTreeArray = whereClauseTableColsArrayObj[tableName];
        }
    }
    var whereClauseStr = $("#" + id).attr("data-whereclause");
    if (!(whereClauseStr != null && whereClauseStr != '' && whereClauseStr != undefined && !jQuery.isEmptyObject(whereClauseStr)))
    {
        whereClauseStr = $("#" + id + "_hidden").val();
    }
// ravi start
    var trfmRulesData;
    var trfmRulesId;
    // ravi end
    $.ajax({
        type: 'post',
        traditional: true,
        dataType: 'html',
        cache: false,
        url: 'showingWhereClauseColumns',
        async: true,
        data: {
            whereClauseStr: whereClauseStr,
            tableName: tableName,
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
                var trString = response['trString'];
                $("#whereTableColumnTr").html(trString);
                $("#whereClauseMapColumnsDivId").append("<input type='hidden' id='whereClauseTableColsObj_hidden'/>");
//                var hiddenData ="<input type='hidden' id='whereClauseTableColsArray_hidden'/><input type='hidden' id='currentClauseMapId'/>\n\
//                                     <div id='wherClauseTrString' style='display:none;'></div><div id='wherClauseColsString' style='display:none;'></div>";
//                $('#whereClauseMapColumnsDivId').append(hiddenData);
                $("#whereClauseTableColsObj_hidden").val(JSON.stringify(response['tableColumnArray']));
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
                            if (columnName != null && columnName != '') {
                               if (operator != null && operator != '' && operator != undefined && operator.indexOf("IN") > -1) //funnel
                    {
                        var whereCon = "";
                        var staticValues = staticValue.split(",");
                        if (staticValues != null && !jQuery.isEmptyObject(staticValues))
                        {
                            whereCon = "(";
                            for (var j = 0; j < staticValues.length; j++) //nested
                                        {
                                            whereCon += "'" + staticValues[j] + "'";
                                            if (j < staticValues.length - 1)
                                            {
                                                whereCon += ",";
                                            }
                                        }
                            whereCon += ")";
                        }
                        query += " " + columnName.replace(":", ".") + " " + operator + " " + whereCon + " ";
                    } else {
                        query += " " + columnName.replace(":", ".") + " " + operator + " '" + staticValue + "' ";
                    }
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
                        if (columnClauseMapping != null && !jQuery.isEmptyObject(columnClauseMapping)) {
                            $("#" + prevMapIconId).attr("data-whereclause", JSON.stringify(columnClauseMapping));
                        }
//                        $("#" + prevMapIconId).attr("data-whereclause", JSON.stringify(columnClauseMapping));
                    }
                }
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
    var trstring = $("#whereTableColumnTr").html();
    $("#fromTablesWhereCauseTable tbody").append(trstring);
    $("#tabs-3").animate({
        scrollTop: $("#tabs-3").prop("scrollHeight")
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
                if (columnName != null && columnName != '') {
                    if (operator != null && operator != '' && operator != undefined && operator.indexOf("IN") > -1) //funnel
                    {
                        var whereCon = "";
                        var staticValues = staticValue.split(",");
                        if (staticValues != null && !jQuery.isEmptyObject(staticValues))
                        {
                            whereCon = "(";
                            for (var j = 0; j < staticValues.length; j++) //nested
                            {
                                whereCon += "'" + staticValues[j] + "'";
                                if (j < staticValues.length - 1)
                                {
                                    whereCon += ",";
                                }
                            }

                            whereCon += ")";
                        }
                        query += " " + columnName.replace(":", ".") + " " + operator + " " + whereCon + " ";
                    } else {
                        query += " " + columnName.replace(":", ".") + " " + operator + " '" + staticValue + "' ";
                    }
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
        if (columnClauseMapping != null && !jQuery.isEmptyObject(columnClauseMapping)) {
//            var whereClauseConditionsArea_
            var currentClauseAreaId = currentClauseMapId.replace("whereClauseConditionsMap_", "whereClauseConditionsArea_");
            $("#" + currentClauseAreaId).val(query);
//          var tdArray = this.cells;  $($this).parents("tr").find("textarea").val(query);
            $("#" + currentClauseMapId).attr("data-whereclause", JSON.stringify(columnClauseMapping));
//            $.each(columnClauseMapping, function (i, obj) {
//                var tableName = obj['columnName'];
//                if (tableName != null && tableName != '' && tableName != undefined && tableName.indexOf(":") > -1)
//                {
//                    var tableNames = tableName.split(":");
//                    tableName = tableNames[0];
//                }
//                $("#" + tableName + "_where_query").remove();
//                // query = query.replace(/'/g, "");
//                $("#analyticsWhereClauseImg").append("<input type='hidden' id='" + tableName + "_where_query' value=\"" + query + "\"/>");
//            });
            $("#" + currentClauseAreaId + "_hidden").remove();
            $("#analyticsWhereClauseImg").append("<input type='hidden' id='" + currentClauseAreaId + "_hidden' value=\"" + query + "\"/>");
            $("#" + currentClauseMapId + "_hidden").remove();
            $("#analyticsWhereClauseImg").append("<input type='hidden' id='" + currentClauseMapId + "_hidden' value='" + JSON.stringify(columnClauseMapping) + "'/>");
        }

    }
    var dialogSplitMessage = dialogSplitIconText((labelObject['Saved Successfully'] != null ? labelObject['Saved Successfully'] : 'Saved Successfully'), "Y");
    $("#dialog1").html(dialogSplitMessage);
    $("#dialog1").dialog({
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

function viewWhereClause() {
    var query = "";
    var columnClauseMapping = {};
    var i = 1;
    var j = 1;
    var rowCount = $('#selectedTables >tbody >tr').length;
    $("#selectedTables tbody tr").each(function () {
        var tdArray = this.cells;
        if (tdArray != null && tdArray.length != 0) {
            var clauseObj = {};
            var columnName = $(tdArray[1]).find("textarea").val();
            var operator = $(tdArray[2]).find("option:selected").val();
            var staticValue = $(tdArray[3]).find("input").val();
            var andOrOperator = "";
            if (columnName != null && columnName != '' && columnName != ' ' && columnName != undefined) {
                query += " " + columnName;
                if ((parseInt(i) - 1) != (parseInt(rowCount) - 1)) {
                    query += " " + "AND";
                }
                clauseObj['columnName'] = columnName;
                clauseObj['operator'] = operator;
                clauseObj['staticValue'] = staticValue;
                columnClauseMapping[i] = clauseObj;
                i++;
            }
            if (!(columnName != null && columnName != '' && columnName != ' ' && columnName != undefined)) {
                if ((parseInt(j) == 1)) {

                    var myString = query.substring(0, query.lastIndexOf(" ")); //split by space//split by space

                    query = myString;
                }
                j++;
            }


        }

    });
    var queryResultes = query.split(" ");
    var lastWord = queryResultes[queryResultes.length - 1];
    if (lastWord == "AND" && lastWord != null) {
        var myString = query.substring(0, query.lastIndexOf(" "));
        query = myString;
    }
    $("#analyticsWhereClauseInner").html(query);
    var currentClauseMapId = $("#currentClauseMapId").val();
    var currentClauseAreaId = currentClauseMapId.replace("whereClauseConditionsMap_", "whereClauseConditionsArea_");
    $("#" + currentClauseAreaId).val(columnClauseMapping);
}

function selectColumnFun($this, columnName, id)
{
    showLoader();
    labelObject = {};
    try {
        labelObject = JSON.parse($("#labelObjectHidden").val());
    } catch (e) {

    }
    // prepare the data
    var data = [];

    $.ajax({
        type: "post",
        traditional: true,
        url: 'getAnalyticDBFunction',
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
                             text: (labelObject['Apply On Same Column'] != null ? labelObject['Apply On Same Column'] : 'Apply On Same Column'),
                            click: function () {
                                var selection = $("#columnMappingTree").jqxTreeGrid('getSelection');
                                if (selection != null && selection.length != 0) {
                                    var selectedRowData = selection[0];
                                    if (selectedRowData != null
                                            && !jQuery.isEmptyObject(selectedRowData)
                                            && selectedRowData['HL_FUN_ID'] != null && selectedRowData['HL_FUN_ID'] != '') {
                                        $(this).html("");
                                        $(this).dialog("close");
                                        $(this).dialog("destroy");
                                        getSelectedColumnFun(selection[0], $this, columnName, id,"applyonSameColumn");
                                    }
                                }
                            }

                        },{
                            text: (labelObject['Create New Column'] != null ? labelObject['Create New Column'] : 'Create New Column'),
                            click: function () {
                                var selection = $("#columnMappingTree").jqxTreeGrid('getSelection');
                                if (selection != null && selection.length != 0) {
                                    var selectedRowData = selection[0];
                                    if (selectedRowData != null
                                            && !jQuery.isEmptyObject(selectedRowData)
                                            && selectedRowData['HL_FUN_ID'] != null && selectedRowData['HL_FUN_ID'] != '') {
                                        $(this).html("");
                                        $(this).dialog("close");
                                        $(this).dialog("destroy");
                                        getSelectedColumnFun(selection[0], $this, columnName, id,"CreateNewColumn");
                                    }
                                }
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
//                        $('#columnMappingTree').on('rowDoubleClick', function (event) {
//                            var args = event.args;
//                            var selectedRowData = args.row;
//                            var source = $("#columnMappingTree").jqxTreeGrid('source');
//                            if (selectedRowData != null
//                                    && !jQuery.isEmptyObject(selectedRowData)
//                                    && selectedRowData['HL_FUN_ID'] != null && selectedRowData['HL_FUN_ID'] != '') {
//                                $("#columnMappingDialog").html("");
//                                $("#columnMappingDialog").dialog("close");
//                                $("#columnMappingDialog").dialog("destroy");
//                                getSelectedColumnFun(selectedRowData, source, $this, columnName, id);
//                            }
//
//
//
//                        });

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

function getSelectedColumnFun(selectedRowData, $this, columnName, id, columnType)
{
    if (selectedRowData != null
            && !jQuery.isEmptyObject(selectedRowData)
            && selectedRowData['HL_FUN_ID'] != null && selectedRowData['HL_FUN_ID'] != '') {
        var selectedFunctionName = selectedRowData['FUN_NAME'];
        var functionFormObj = {};
        var funStr = "";
        functionFormObj['functionName'] = selectedFunctionName;
        funStr += "" + selectedFunctionName + "(";
        funStr += "" + columnName + ")";
        var labels = columnName.split(".");
        var tableName = labels[0];
        var label = labels[1];
        var colStr = "";
        colStr += "" + selectedFunctionName + "(";
        colStr += "" + label + ")";
        columnName = columnName.replace(".", "_"); //group
        var columnData = '<div id="COLUMN_' + columnName + '" class="analyticsDivData"'  //saved job
                + ' title="' + colStr + '" data-table-name="' + tableName + '" data-aggragate-name="' + funStr + '" '
                + ' data-column-name="' + label + '" ><span class="visionColsText">' + colStr
                + '</span><img src="images/close_white.png" title="Remove Column"'
                + ' onclick="removeColumn(this,\'' + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;">'
                + '</div>';
        var columnLabelData = "<div id='COL_LBL_DIV_" + columnName + "' data-column-name='" + columnName + "' class='columnLabelDivs'>"
                + "<label>" + colStr + ":</label>"
                + "<input type='text' id='COL_" + columnName + "' data-column-name='" + colStr + "' value='" + colStr + "'/>  "
                + ""
                + "</div>";
        if (columnType != null && columnType != '' && columnType == "applyonSameColumn")
        {
            $($this).parent().remove();
            $("#COL_LBL_DIV_" + label).remove();
        }
        $("#" + id).append(columnData);
        $("#selectedColLabelsDiv").append(columnLabelData);
        tablesGroupObj[tableName] = tableName; //group
        showGroupByPopup();
    }
}
function showGroupByPopup()
{
    startAjax();
    if (tablesGroupObj != null && !jQuery.isEmptyObject(tablesGroupObj))
    {
        var groupByCondColumns = $('#analyticsGroupClauseInner').val();
        $.ajax({
            type: "post",
            traditional: true,
            dataType: 'json',
            url: "getGroupByClause",
            data: {
                'tablesObj': JSON.stringify(tablesGroupObj),
                'groupByCond': groupByCondColumns
            },
            cache: false,
            success: function (response) {
                if (response != null) {
                    var tabsString = response['tabsString'];
                    $("#dialog").html(tabsString);
                    $("#dialog").dialog({
                        title: (labelObject['GroupBy Clauses'] != null ? labelObject['GroupBy Clauses '] : 'GroupBy Clauses'),
                        modal: true,
                        width: 1045,
                        maxWidth: 1215,
                        height: 350,
                        maxHeight: 1000,
                        fluid: true,
                        buttons: [{
                                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                                click: function () {
                                    groupByQuery(this);
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
                    var selectedTableOrderGroupClause = response['selectedTableOrderGroupClause'];
                    if (selectedTableOrderGroupClause != null) {
                        var hiddenDataGroupBy = ""
                                + "<div id='groupClauseTrString' style='display:none;'></div>";
                        $('#tabs-1').html(selectedTableOrderGroupClause['groupByCondition'] + hiddenDataGroupBy);//
                        $("#groupClauseTrString").html(selectedTableOrderGroupClause['groupByTrString']);
                        $('#tabs-1').append("<div id='selectedColumnStr' style='display:none'></div>");
                        $('#tabs-1').append(" <div id='disanalyticsGroupClauses' class='allErrors' style='color:red'></div>");
                        $("#fromTableColsArray_hidden").remove();
                        var hiddenData = "<input type='hidden' id='fromTableColsArray_hidden'/>";
                        $('#tabs-1').append(hiddenData);
                        $("#fromTableColsArray_hidden").val(JSON.stringify(response['fromTableColsArray']));
                    }
                    $(".visionETLColMapImage").mousedown(function (event) {
                        treeIconClickEvent = event;
                    })
                    stopLoader();
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
function addNewGroupClauseRow(event, id, $this) {
    var trstring = $("#groupClauseTrString").html();
    $("#fromTablesGroupCauseTable tbody").append(trstring);
    $("#tabs-5").animate({
        scrollTop: $("#tabs-5").prop("scrollHeight")
    }, 1000);
}
function groupByQuery($this)
{
    var selectQuery = "";
    var rowCount = $('#fromTablesGroupCauseTable >tbody >tr').length;
    var groupByCondColumns = $('#analyticsGroupClauseInner').val();
    var i = 0;
    var groupByColumns = "";
    $("#fromTablesGroupCauseTable tbody tr").each(function () {
        var tdArray = this.cells;
        if (tdArray != null && tdArray.length != 0) {
            var columnName = $(tdArray[1]).find("input").val();
            if (columnName != null && columnName != '') {
                var columnsArray = [];
                if (columnName.indexOf(":") > -1)
                {
                    columnsArray = columnName.split(":");
                } else if (columnName.indexOf(".") > -1) {
                    columnsArray = columnName.split(".");
                }
                var groupByCol = columnsArray[0] + "." + columnsArray[1];
                groupByColumns += groupByCol;
                var groupByArr = [];
                if (groupByCondColumns != null && groupByCondColumns != '' && groupByCondColumns != undefined)
                {
                    if (i == 0) {
                        groupByCondColumns = JSON.parse(groupByCondColumns);
                    }
                    if (groupByCondColumns[columnsArray[0]] != null && groupByCondColumns[columnsArray[0]] != ''
                            && groupByCondColumns[columnsArray[0]] != undefined)
                    {
                        groupByArr = groupByCondColumns[columnsArray[0]];
                    }
                } else
                {
                    groupByCondColumns = {};
                }
                if (!(groupByArr.indexOf(groupByCol) > -1))
                {
                    groupByArr.push(groupByCol);
                }
                groupByCondColumns[columnsArray[0]] = groupByArr;
                if (i != parseInt(rowCount) - 1) {
                    groupByColumns += ",";
                }
            }

            i++;
        }
    });
    if (groupByColumns != null && groupByColumns != '' && groupByColumns != undefined) {
        selectQuery += groupByColumns;
        $('#analyticsGroupClauseInner').val(JSON.stringify(groupByCondColumns));
        $("#analyticsInnerGroupClause").val(selectQuery);
        $($this).html("");
        $($this).dialog("close");
        $($this).dialog("destroy");
    } else
    {
        errorMessage("#disanalyticsGroupClauses", "Should not be null");
    }

}
//Joins code
//for delete chart data
function selecttab(id){
     var checked = $("#"+id).is(":checked")
//      var selectedtabid = $("#reportTittle").val();
       if(checked){
           getComponentid();
            $(".reportTabsClass").show(); 
            $(".selecttb").show();
            $(".analyticscompId").show();
         }else{
           $(".reportTabsClass").hide();  
           $(".selecttb").hide(); 
           $(".analyticscompId").hide();
           $(".reportTittle").hide();
           
        }  
      } 
      function getComponentid(){  
       var tabId = $("#analyticstab").val();
        $.ajax({
        type: "post",
        traditional: true,
        dataType: 'html',
        url: "getCompId",
        data: {
      tabId:tabId      
        },
        cache: false,
        success: function (response) {
            $("#reportTittle").html(response);
           $(".reportTabsClass").show();
        },
        error: function (e) {
            console.log(e);
            sessionTimeout(e);
            stopLoader();
        }
    });  
           
 }
//for delete chart data
//for nested charts and grid
function selectJqwidgetClickHandler(selectedValue, selectedCount, chartId) {
    try {
        var paramArray = [];
        var paramObj = {};
        var columnNames = "";
        $('#analyticsColumns div').each(function (i, ele) {
            var divId = $(this).attr("id");
            var connType = $("#" + divId).attr("data-coon-type");
            var tableName = $("#" + divId).attr("data-table-name");
            var columnName = $("#" + divId).attr("data-column-name");
            if (tableName != null
                    && tableName != ''
                    && columnName != null && columnName != '') {
                columnNames += tableName + "." + columnName + ",";
            }
        });
        if (columnNames != null && columnNames != '' && columnNames != undefined)
        {
            columnNames = columnNames.replace(/,\s*$/, "");
            var colsArr = columnNames.split(",");
            paramObj.column = $.trim(colsArr[0]);
        }
        paramObj.value = $.trim(selectedValue);
        paramObj.operator = "EQUALS";
        paramArray.push(paramObj);
        showNestedType(paramArray);
    } catch (e)
    {

    }

}
function clickHandler(chart, data, chartId) {
    try {
        var selection = chart.getSelection();
        var value;
        var count;
        var paramArray = [];
        var paramObj = {};
        var columnNames = "";
        $('#analyticsColumns div').each(function (i, ele) {
            var divId = $(this).attr("id");
            var connType = $("#" + divId).attr("data-coon-type");
            var tableName = $("#" + divId).attr("data-table-name");
            var columnName = $("#" + divId).attr("data-column-name");
            if (tableName != null
                    && tableName != ''
                    && columnName != null && columnName != '') {
                columnNames += tableName + "." + columnName + ",";
            }
        });
        for (var i = 0; i < selection.length; i++) {
            var item = selection[i];
            if (item.row != null && item.column != null) {
                if (columnNames != null && columnNames != '' && columnNames != undefined)
                {
                    columnNames = columnNames.replace(/,\s*$/, "");
                    var columnVal = item.column - 1;
                    var colsArr = columnNames.split(",");
                    paramObj.column = $.trim(colsArr[columnVal]);
                }
                count = data.getFormattedValue(item.row, item.column);
                value = data.getValue(chart.getSelection()[0].row, 0);
            } else if (item.row != null) {
                if (columnNames != null && columnNames != '' && columnNames != undefined)
                {
                    columnNames = columnNames.replace(/,\s*$/, "");
                    var colsArr = columnNames.split(",");
                    paramObj.column = $.trim(colsArr[0]);
                }
                count = data.getValue(chart.getSelection()[0].row, 1);
                value = data.getFormattedValue(item.row, 0);
            } else if (item.column != null) {
                value = data.getFormattedValue(0, item.column);
            }
            paramObj.value = $.trim(value);
            paramObj.operator = "EQUALS";
            paramArray.push(paramObj);
            showNestedType(paramArray);
        }

        chart.setSelection([]);
    } catch (e)
    {

    }

}
function showNestedType(paramArray)
{
    var nestedMsg = "<div id ='visionAnalyticsNestedType'>Please select the Filter type</div>";
    $("#dialog").html(nestedMsg);
    $("#dialog").dialog({
        title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
        modal: true,
        height: 'auto',
        minHeight: 'auto',
        minWidth: 700,
        maxWidth: 'auto',
        fluid: true,
        buttons: [{
                text: (labelObject['Grid'] != null ? labelObject['Grid'] : 'Grid'),
                click: function () {
                    showNestedGrid(paramArray);
                    $(this).html("");
                    $(this).dialog("close");
                    $(this).dialog("destroy");
                }
            },
            {
                text: (labelObject['Chart'] != null ? labelObject['Chart'] : 'Chart'),
                click: function () {
                    showSavedJobsNestedChart(paramArray);
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

function showNestedGrid(paramArray)
{
    showLoader();
    var columns = {};
    var tablesObj = [];
    $('#analyticsColumns div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        var aggragateColumnName = $("#" + divId).attr("data-aggragate-name");
        if (tableName != null
                && tableName != ''
                && columnName != null && columnName != '') {
            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            columnObj['aggragateColumnName'] = aggragateColumnName;
            columns[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))
            {
                tablesObj.push(tableName);
            }
        }

    });
    console.log("columns:::" + JSON.stringify(columns));
    var rows = {};
    $('#analyticsRows div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        var aggragateColumnName = $("#" + divId).attr("data-aggragate-name");
        if (tableName != null
                && tableName != ''
                && columnName != null && columnName != '') {

            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            columnObj['aggragateColumnName'] = aggragateColumnName;
            rows[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))
            {
                tablesObj.push(tableName);
            }
        }

    });
    var comboRows = {};
    $('#analyticsComboRows div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        var aggragateColumnName = $("#" + divId).attr("data-aggragate-name");
        if (tableName != null
                && tableName != ''
                && columnName != null && columnName != '') {

            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            columnObj['aggragateColumnName'] = aggragateColumnName;
            comboRows[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))
            {
                tablesObj.push(tableName);
            }

        }
    });
    var joinQuery = $("#analyticsInnerJoinClause").val();
    var whereClauseCond = $("#analyticsWhereClauseInner").text();
    var filterArray = $("#filterParamArray").val(); //filter
    if (filterArray != null && filterArray != '' && filterArray != undefined)
    {
        if (!(paramArray != null && paramArray != '' && paramArray != undefined))
        {
            paramArray = [];
        }
        filterArray = JSON.parse(filterArray);
        $.each(filterArray, function (index) {
            var paramObj = {};
            var filterObj = filterArray[index];
            paramObj.column = filterObj['column'];
            paramObj.operator = filterObj['operator'];
            paramObj.value = filterObj['value'];
            paramArray.push(paramObj);
        });

    }
    var data = {};
    data['joinQuery'] = joinQuery;
    data['tablesObj'] = JSON.stringify(tablesObj);
    data['whereClauseCond'] = whereClauseCond;
    data['paramArray'] = JSON.stringify(paramArray);
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'getAnalyticsMetaData',
        cache: false,
        data: data,
        success: function (response) {

            if (response != null) {
                // var responseObj = JSON.parse(response);
                var dataArray = response['dataArray'];
                var dataFieldsArray = response['dataFieldsArray'];
                var columnsArray = response['columnsArray'];
                var totalCount = response['totalCount'];
                var tableName = "";
                var tableNames = "";
                if (tablesObj != null && !jQuery.isEmptyObject(tablesObj))
                {
                    $.each(tablesObj, function (index, val) {
                        tableName += tablesObj[index] + ",";
                    });
                    tableName = tableName.replace(/,\s*$/, "");
                    tableNames = tablesObj[0];
                }

                var gridId = ("divGrid-" + tableName).replace(/\s/g, '');
                gridId = gridId.replace(/\//g, '');
                var selectedItemIndex = $('#analyticDataViewDiv').jqxTabs('selectedItem');
                if (selectedItemIndex == null) {
                    switchAnalyticsDesignTabs("li_contentView", "analyticDataViewDiv");
                    $("#analyticDataViewDiv").prepend("<ul></ul>");
                    var gridData = tableNames + "_GridData";
                    $("#analyticDataViewDiv ul").prepend("<li title='" + tableName + "'>" + gridData + "</li>");
                    var divStr = "<div>";
                    divStr += "<div style='display: flex;'>";
                    divStr += "<img src='images/refresh_icon.png' style='width:18px;height: 18px;cursor:pointer;padding-left:3px;' onclick=refreshTableGrid('" + gridId + "') title='Refresh'>"
                    // divStr += "<div id='exportDropdown" + gridId.trim() + "' class='exportDropdown visionSearchExport visionSearchExportDiv' style='vertical-align: bottom; display: flex; padding-top: 8px;margin-top:-8px;'><table style='/*vertical-align: bottom;*/ display: inline-block;width: 40%;' class='visionSearchExportTable'><tbody><tr><td><select id='export" + gridId.trim() + "' onchange=getImportType('" + gridId.trim() + "')><option value=''>Select</option><option value='Xlsx'>Xlsx</option><option value='CSV'>CSV</option><option value='XML'>XML</option></select></td></tr></tbody></table> ";
                    // divStr += "<input title='Export'  id='excelExport" + gridId.trim() + "' onclick=finalExport('" + gridId.trim() + "') class='exportClass visionSearchExportButton visionGridExportButton' type='button' disabled=''>";
                    // divStr += "<input title='Import'  id='excelImport" + gridId.trim() + "' onclick=finalImport('" + gridId.trim() + "') class='importClass visionSearchImportButton visionGridImportButton' type='button' disabled='' style='margin-top: 5px;'>";
                    // divStr += "</div>";
                    divStr += "</div>";
                    divStr += "<div id='" + gridId + "'></div>";
                    $("#analyticDataViewDiv").append(divStr);
//                    $("#dataViewDiv").append("<div><div><img src='images/refresh_icon.png' style='width:18px;height: 18px;cursor:pointer;padding-left:3px;' onclick=refreshTableGrid('" + gridId + "') title='Refresh'></div><div id='" + gridId + "'></div>"); // ravi edit for tabs navigation
                    $('#analyticDataViewDiv').jqxTabs({width: "100%", height: "100%", position: 'top', theme: 'ui-redmond', reorder: true});
                    $('#analyticDataViewDiv').jqxTabs('showAllCloseButtons');
                    $("#analyticDataViewDiv").find("li.jqx-tabs-title").find("div.jqx-tabs-close-button").css("background-image", "url(images/close.png)");
                    $("#analyticDataViewDiv").find("li.jqx-tabs-title-selected-top").find("div.jqx-tabs-close-button").css("background-image", "url(images/close_white.png)");
                } else {
                    switchAnalyticsDesignTabs("li_contentView", "analyticDataViewDiv");
                    var divStr = "<div>";
                    divStr += "<div style='display: flex;'>";
                    divStr += "<img src='images/refresh_icon.png' style='width:18px;height: 18px;cursor:pointer;padding-left:3px;' onclick=refreshTableGrid('" + gridId + "') title='Refresh'>"
                    // divStr += "<div id='exportDropdown" + gridId.trim() + "' class='exportDropdown visionSearchExport visionSearchExportDiv' style='vertical-align: bottom; display: flex; padding-top: 8px;margin-top:-8px;'><table style='/*vertical-align: bottom;*/ display: inline-block;width: 40%;' class='visionSearchExportTable'><tbody><tr><td><select id='export" + gridId.trim() + "' onchange=getImportType('" + gridId.trim() + "')><option value=''>Select</option><option value='Xlsx'>Xlsx</option><option value='CSV'>CSV</option><option value='XML'>XML</option></select></td></tr></tbody></table> ";
                    // divStr += "<input title='Export'  id='excelExport" + gridId.trim() + "' onclick=finalExport('" + gridId.trim() + "') class='exportClass visionSearchExportButton visionGridExportButton' type='button' disabled=''>";
                    // divStr += "<input title='Import'  id='excelImport" + gridId.trim() + "' onclick=finalImport('" + gridId.trim() + "') class='importClass visionSearchImportButton visionGridImportButton' type='button' disabled='' style='margin-top: 5px;'>";
                    // divStr += "</div>";
  
                    divStr += "</div>";
                    divStr += "<div id='" + gridId + "'></div>";
                    var html = $('#analyticDataViewDiv').html();
                    tableName = tableName + "_gridData";
                    if (!(html.indexOf(gridId) > -1)) {
                        $('#analyticDataViewDiv').jqxTabs('addLast', tableName, divStr);
//                    $('#dataViewDiv').jqxTabs('addLast', DBValue + "." + tableName, '<div><div><img src="images/refresh_icon.png" style="width:18px;height: 18px;cursor:pointer;padding-left:3px;" onclick=refreshTableGrid("' + gridId + '") title="Refresh"></div><div id="' + gridId + '"></div></div>'); // ravi edit for tabs navigation
                    }
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
                            url: 'getAnalyticsObjectData',
                            cache: false,
                            root: 'Rows',
                            processdata: function (data) {
                                showLoader();
                                data['getOnlyDataArray'] = 'Y';
                            },
                            beforeSend: function () {
                                showLoader();

                            }, loadError: function (xhr, status, error) {
                                $('#analyticDataViewDiv').css("width", "100%");
                            }, loadComplete: function (data)
                            {
                                $('#analyticDataViewDiv').css("width", "100%");
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
                stopLoader();
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
                $('#analyticDataViewDiv').unbind('selected').on('selected', function (event) {
                    var $thid = this;
                    $('#analyticDataViewDiv').jqxTabs('getContentAt', i);
                    $("#analyticDataViewDiv").find("li.jqx-tabs-title").find("div.jqx-tabs-close-button").css("background-image", "url(images/close.png)");
                    $("#analyticDataViewDiv").find("li.jqx-tabs-title-selected-top").find("div.jqx-tabs-close-button").css("background-image", "url(images/close_white.png)");
                });
                $('#analyticDataViewDiv').unbind('add').on('add', function (event) {

                    $("#analyticDataViewDiv").find("li.jqx-tabs-title").find("div.jqx-tabs-close-button").css("background-image", "url(images/close.png)");
//                    $("#analyticDataViewDiv").find("li.jqx-tabs-title-selected-top").find("div.jqx-tabs-close-button").css("background-image", "url(images/close_white.png)");
                    setTimeout(function () {
                        $("#analyticDataViewDiv").find("li.jqx-tabs-title-selected-top").find("div.jqx-tabs-close-button").css("background-image", "url(images/close_white.png)");
                    }, 100);
                    var selectedTabTitle = $("#analyticDataViewDiv").jqxTabs("getTitleAt", event.args.item);
                    $("#analyticDataViewDiv").jqxTabs('setTitleAt', event.args.item, selectedTabTitle);
//                    $("#analyticDataViewDiv").jqxTabs('setTitleAt', event.args.item, selectedTabTitle.split(".")[1]);
                    var selectedTabLi = $("#analyticDataViewDiv").find("li.jqx-tabs-title-selected-top");
                    selectedTabLi.attr("title", selectedTabTitle);
                    var selectedItem = $('#analyticDataViewDiv').jqxTabs('selectedItem'); // ravi edit for tabs issue
                    $('#analyticDataViewDiv').jqxTabs('ensureVisible', selectedItem); // ravi edit for tabs issue


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

function showSavedJobsNestedChart(filterArray)
{

    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'showSavedJobsNestedChart',
        cache: false,
        data: {
            filterArray: filterArray
        },
        success: function (response) {
            ajaxStop();
            if (response != null)
            {
                var nestedTables = response['nestedTables'];
                var selectBoxStr = response['selectBoxStr'];
                var nestedColumns = response['columnList'];
                $("#dialog").html(selectBoxStr);
                $("#dialog").dialog({
                    title: (labelObject['Chart'] != null ? labelObject['Chart'] : 'Chart'),
                    modal: true,
                    height: 'auto',
                    minHeight: 'auto',
                    minWidth: 500,
                    maxWidth: 'auto',
                    fluid: true,
                    buttons: [{
                            text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                            click: function () {
                                var selectedColumn = $("#nestedChartColumns").val();
                                var selectedJobId = $("#nestedChartJobId").val();
                                if (selectedColumn != null && selectedColumn != '' && selectedColumn != undefined && selectedColumn != 'select')
                                {
                                    var filterObj = filterArray[0];
                                    filterObj.column = selectedColumn;
                                    var paramArray = [];
                                    paramArray.push(filterObj);
                                    getNestedChart(paramArray, selectedJobId);
                                    $(this).html("");
                                    $(this).dialog("close");
                                    $(this).dialog("destroy");
                                } else
                                {
                                    errorMessage("#errorNestedSavedJobsColumns", "please select column");
                                }

                            }

                        }],
                    open: function () {
                        $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                        $(".ui-selectmenu-open").zIndex($("#dialog").zIndex() + 1);
                        $(".visionHeaderMain").css("z-index", "999");
                        $(".visionFooterMain").css("z-index", "999");
                    },
                    beforeClose: function (event, ui)
                    {
                        $(".visionHeaderMain").css("z-index", "99999");
                        $(".visionFooterMain").css("z-index", "99999");
                    }
                });
                $("#nestedSavedJobsDiv").html(nestedTables);
                $("#nestedSavedJobsColumnsDiv").html(nestedColumns);
            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });
}


function applyChartFilterProperties(columns, rows, chartType, chartTitle, whereClauseCond, columnsLabel
        , sqlQuery, chartId, chartFormId, chartOptAllObj, comboRows, joinQuery, groupByCond, paramArray) {
    showLoader();
    var filterArray = $("#filterParamArray").val(); //filter
    if (filterArray != null && filterArray != '' && filterArray != undefined)
    {
        filterArray = JSON.parse(filterArray);
        $.each(filterArray, function (index) {
            var paramObj = {};
            var filterObj = filterArray[index];
            paramObj.column = filterObj['column'];
            paramObj.operator = filterObj['operator'];
            paramObj.value = filterObj['value'];
            paramArray.push(paramObj);
        });
    }
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: "generateChartByProps",
        data: {
            columns: JSON.stringify(columns),
            rows: JSON.stringify(rows),
            chartType: chartType,
            chartTitle: chartTitle,
            whereClauseCond: whereClauseCond,
            columnsLabel: JSON.stringify(columnsLabel),
            sqlQuery: sqlQuery,
            chartId: chartId,
            chartFormId: chartFormId,
            chartOptionObj: JSON.stringify(chartOptAllObj),
            comboRows: JSON.stringify(comboRows),
            joinCondition: joinQuery,
            groupByCond: groupByCond,
            paramArray: JSON.stringify(paramArray),
            chartDivId: 'chartDialog'
        },
        cache: false,
        success: function (data, status, xhr) {
            stopLoader();
            if (data != null && !jQuery.isEmptyObject(data)) {
                var jqwidgetFlag = data['jqwidgetFlag'];
                if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Y') {
                    var chartsObj = data['settingObj'];
                    var chartId = data['chartId'];
                    var height = chartsObj['height'];
                    var width = chartsObj['width'];
                    var finalChartObj = {};
                    finalChartObj = data['settingObj'];
                    finalChartObj.source = data['dataObjList'];
                    showJqwidgetChartDialog(chartId, width, height, finalChartObj);
                } else {
                    var dataArray = [];
                    var options = {};
                    var input = "<input type='hidden' id='dataObj'/><input type='hidden' id='dataOption'/>"
                    $("#dataTypeDialog").html(input);
                    $("#dataObj").val(JSON.stringify(data['dataObjList']));
                    $("#dataOption").val(JSON.stringify(data['optionObj']));
                    dataArray = JSON.parse($("#dataObj").val());
                    options = JSON.parse($("#dataOption").val());
                    var chartDataStr = "<div id='chartDialog'></div> ";
                    var chartInit = data['chartInit'];
                    showChartDialog(chartDataStr, data, chartInit, dataArray, options);
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


function showJqwidgetChartDialog(chartId, height, width, finalChartObj)
{
    var chartData = "<div id='chartDialog'></div> ";
    $("#dialog").html(chartData);
    var settings = {};
    settings = finalChartObj;
    if (!(width != null && width != '' && width != undefined))
    {
        width = '600px';
    }
    if (!(height != null && height != '' && height != undefined))
    {
        height = '400px';
    }
    $("#" + chartId).css("width", "" + width + "", "important");
    $("#" + chartId).css("height", "" + height + "", "important");
    $("#dialog").dialog({
        title: (labelObject['Chart'] != null ? labelObject['Chart'] : 'Chart'),
        modal: true,
        height: 'auto',
        minHeight: 'auto',
        minWidth: 700,
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
    $("#" + chartId).jqxChart(settings);
}
function showChartDialog(chartData, data, chartInit, dataArr, options)
{
    $("#dialog").html(chartData);
    var chartObj = eval(chartInit);
    dataArr = google.visualization.arrayToDataTable(data['dataObjList']);
    $("#dialog").dialog({
        title: (labelObject['Chart'] != null ? labelObject['Chart'] : 'Chart'),
        modal: true,
        height: 'auto',
        minHeight: 'auto',
        minWidth: 700,
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
    chartObj.draw(dataArr, options);
}

function getColumnsBasedonJob(id)
{
    ajaxStart();
    var jobId = $("#" + id).val();
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'getColumnsByJobId',
        cache: false,
        data: {
            jobId: jobId
        },
        success: function (response) {
            ajaxStop();
            if (response != null)
            {
                var colStr = response['colStr'];
                $("#nestedSavedJobsColumnsDiv").html(colStr);
            }
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });
}

function getNestedChart(paramArray, nestedId)
{
    if (nestedId != null && nestedId != '' && nestedId != undefined)
    {
        var jobId = "";
        var chartId = "";
        var nestedIds = nestedId.split(",");
        if (nestedIds != null)
        {
            jobId = nestedIds[0];
            chartId = nestedIds[1];
        }
        $.ajax({
            type: "post",
            traditional: true,
            dataType: 'json',
            url: 'openSavedAnalyticsJobs',
            cache: false,
            data: {
                jobId: jobId,
                chartId: chartId
            },
            success: function (data) {
                ajaxStop();
                var response = data;
                if (response != null)
                {
                    var chartTitle = response['chartTitle'];
                    var whereClause = response['whereClause'];
                    var joinQuery = response['joinQuery'];
                    var columnsList = response['columnsList'];
                    var rowsList = response['rowsList'];
                    var comboRowsList = response['comboRowsList'];
                    var groupByCond = response['groupByCond'];
                    var chartType = response['chartType'];
                    var chartFormId = response['chartFormId'];
                    var optionsData = response['optionsData'];
                    var columnLabelData = {};
                    var tablesObj = [];
                    var columns = {};
                    var rows = {};
                    var comboRows = {};
                    var i = 0;
                    if (columnsList != null && columnsList != '' && columnsList != undefined)
                    {
                        columnsList = JSON.parse(columnsList);
                        $.each(columnsList, function (index) {
                            var colList = columnsList[index];
                            var columnName = colList['columnName'];
                            var connType = colList['connType'];
                            var tableName = colList['tableName'];
                            var aggragateColumnName = colList['aggragateColumnName'];
                            if (tableName != null
                                    && tableName != ''
                                    && columnName != null && columnName != '') {
                                var columnObj = {};
                                columnObj['connType'] = connType;
                                columnObj['tableName'] = tableName;
                                columnObj['columnName'] = columnName;
                                columnObj['aggragateColumnName'] = aggragateColumnName;
                                columns[i] = columnObj;
                                i++;
                                if (aggragateColumnName != null && aggragateColumnName != '' && aggragateColumnName != undefined) {
                                    var subStr = aggragateColumnName.split("(");
                                    subStr = subStr[0];
                                    var colStr = subStr + "(" + columnName;
                                    columnLabelData [tableName + "_" + columnName] = colStr;
                                } else
                                {
                                    columnLabelData [columnName] = columnName;
                                }
                            }
                            if (!(tablesObj.indexOf(tableName) > -1))
                            {
                                tablesObj.push(tableName);
                            }
                        });
                    }
                    if (rowsList != null && rowsList != '' && rowsList != undefined)
                    {
                        rowsList = JSON.parse(rowsList);
                        $.each(rowsList, function (index) {
                            var rowList = rowsList[index];
                            var columnName = rowList['columnName'];
                            var connType = rowList['connType'];
                            var tableName = rowList['tableName'];
                            var aggragateColumnName = rowList['aggragateColumnName'];
                            if (tableName != null
                                    && tableName != ''
                                    && columnName != null && columnName != '') {
                                var columnObj = {};
                                columnObj['connType'] = connType;
                                columnObj['tableName'] = tableName;
                                columnObj['columnName'] = columnName;
                                columnObj['aggragateColumnName'] = aggragateColumnName;
                                rows[i] = columnObj;
                                i++;
                                if (aggragateColumnName != null && aggragateColumnName != '' && aggragateColumnName != undefined) {
                                    var subStr = aggragateColumnName.split("(");
                                    subStr = subStr[0];
                                    var colStr = subStr + "(" + columnName;
                                    columnLabelData [tableName + "_" + columnName] = colStr;
                                } else
                                {
                                    columnLabelData [columnName] = columnName;
                                }
                            }
                            if (!(tablesObj.indexOf(tableName) > -1))
                            {
                                tablesObj.push(tableName);
                            }

                        });
                    }
                    if (comboRowsList != null && comboRowsList != '' && comboRowsList != undefined)
                    {
                        comboRowsList = JSON.parse(comboRowsList);
                        $.each(comboRowsList, function (index) {
                            var colList = comboRowsList[index];
                            var columnName = colList['columnName'];
                            var connType = colList['connType'];
                            var tableName = colList['tableName'];
                            var aggragateColumnName = colList['aggragateColumnName'];
                            if (tableName != null
                                    && tableName != ''
                                    && columnName != null && columnName != '') {
                                var columnObj = {};
                                columnObj['connType'] = connType;
                                columnObj['tableName'] = tableName;
                                columnObj['columnName'] = columnName;
                                columnObj['aggragateColumnName'] = aggragateColumnName;
                                comboRows[i] = columnObj;
                                i++;
                                if (aggragateColumnName != null && aggragateColumnName != '' && aggragateColumnName != undefined) {
                                    var subStr = aggragateColumnName.split("(");
                                    subStr = subStr[0];
                                    var colStr = subStr + "(" + columnName;
                                    columnLabelData [tableName + "_" + columnName] = colStr;
                                } else
                                {
                                    columnLabelData [columnName] = columnName;
                                }
                            }
                            if (!(tablesObj.indexOf(tableName) > -1))
                            {
                                tablesObj.push(tableName);
                            }
                        });
                    }
                    var chartOptAllObj = {};
                    if (optionsData != null && optionsData != null && optionsData != undefined)
                    {
                        chartOptAllObj = JSON.parse(optionsData);
                    }
                    applyChartFilterProperties(columns, rows, chartType, chartTitle, whereClause, columnLabelData
                            , "", chartId, chartFormId, chartOptAllObj, comboRows, joinQuery, groupByCond, paramArray);
                }
            },
            error: function (e)
            {
                sessionTimeout(e);
            }

        });
    }
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
function showChartFilterForm()
{
    showLoader();
    var columns = {};
    var tablesObj = [];
    $('#analyticsColumns div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        var aggragateColumnName = $("#" + divId).attr("data-aggragate-name");
        if (tableName != null
                && tableName != ''
                && columnName != null && columnName != '') {
            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            columnObj['aggragateColumnName'] = aggragateColumnName;
            columns[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))
            {
                tablesObj.push(tableName);
            }
        }

    });
    console.log("columns:::" + JSON.stringify(columns));
    var rows = {};
    $('#analyticsRows div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        var aggragateColumnName = $("#" + divId).attr("data-aggragate-name");
        if (tableName != null
                && tableName != ''
                && columnName != null && columnName != '') {

            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            columnObj['aggragateColumnName'] = aggragateColumnName;
            rows[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))
            {
                tablesObj.push(tableName);
            }
        }

    });
    var comboRows = {};
    $('#analyticsComboRows div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        var aggragateColumnName = $("#" + divId).attr("data-aggragate-name");
        if (tableName != null
                && tableName != ''
                && columnName != null && columnName != '') {

            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            columnObj['aggragateColumnName'] = aggragateColumnName;
            comboRows[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))
            {
                tablesObj.push(tableName);
            }

        }
    });
    var data = {};
    data['tablesObj'] = JSON.stringify(tablesObj);

    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'json',
        url: 'getAnalyticsFilterForm',
        cache: false,
        data: data,
        success: function (response) {

            if (response != null)
            {
                stopLoader();
                var htmlString = "<div id=\"filterClauseMapColumnsDivId\" class=\"filterMapColumnsDivClass\"></div>"
                        + "<div id='filterClauseTrString' style='display:none;'></div><div id='filterClauseColsString' style='display:none;'></div>";
                $("#dialog").html(htmlString);
                $("#filterClauseMapColumnsDivId").html(response['filterClauseColsCondition']);
                $("#filterClauseTrString").html(response['trString']);
                var trString = response['trString'];
                $("#filterTableColumnTr").html(trString);
                $("#filterClauseMapColumnsDivId").append("<input type='hidden' id='filterClauseTableColsObj_hidden'/>");
                $("#filterClauseTableColsObj_hidden").val(JSON.stringify(response['tableColumnArray']));
                $("#dialog").dialog({
                    title: (labelObject['Filter Form'] != null ? labelObject['Filter Form'] : 'Filter Form'),
                    modal: true,
                    height: 'auto',
                    minHeight: 'auto',
                    minWidth: 900,
                    maxWidth: 'auto',
                    fluid: true,
                    buttons: [{
                            text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                            click: function () {
                                getFilterChart();
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
            stopLoader();
        },
        error: function (e)
        {
            sessionTimeout(e);
        }

    });
}
function addFilterNewClauseRow(event, dbObject, id) {
    var trstring = $("#filterTableColumnTr").html();
    $("#fromTablesFilterCauseTable tbody").append(trstring);
}
function deleteFilterSelectedRow($this) {
    $($this).parents("tr").remove();
    console.log("$this");
}

function getFilterChart()
{
    var columnClauseMapping = [];
    var i = 1;
    $("#fromTablesFilterCauseTable tbody tr").each(function () {
        var tdArray = this.cells;
        if (tdArray != null && tdArray.length != 0) {
            var clauseObj = {};
            var columnName = $(tdArray[1]).find("input").val();
            var operator = $(tdArray[2]).find("option:selected").val();
            var staticValue = $(tdArray[3]).find("input").val();
            if (columnName != null && columnName != '') {
                if (operator != null && operator != '' && operator != undefined && operator.indexOf("IN") > -1)
                {
                    var whereCon = "";
                    var staticValues = staticValue.split(",");
                    if (staticValues != null && !jQuery.isEmptyObject(staticValues))
                    {
                        for (var j = 0; j < staticValues.length; j++)
                        {
                            whereCon += "" + staticValues[j] + "";
                            if (j < staticValues.length - 1)
                            {
                                whereCon += ",";
                            }
                        }
                        staticValue = whereCon;
                    }
                }
                columnName = columnName.replace(":", ".");
                clauseObj['column'] = columnName;
                clauseObj['operator'] = operator;
                clauseObj['value'] = staticValue;
                columnClauseMapping.push(clauseObj);
                i++;
            }
        }

    });
    $("#filterParamArray").val(JSON.stringify(columnClauseMapping));
    getFilterDataChart();
}
function showRefreshChart()
{
    $("#filterParamArray").val("");
    applyChartProperties();
}
function getFilterDataChart()
{
    showLoader();
    var columns = {};
    var tablesObj = [];
    var errorCount = 0;
    var chartType = $("#visionChartConfigOptionForm").attr("data-chartType");
    var chartTitle = $("#chartTitle").val();
    var chartId = $("#visionChartConfigOptionForm").attr("data-chartId");
    var chartFormId = $("#visionChartConfigOptionForm").attr("data-chartFormId");
    $('#analyticsColumns div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        var aggragateColumnName = $("#" + divId).attr("data-aggragate-name");
        if (tableName != null
                && tableName != ''
                && columnName != null && columnName != '') {
            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            columnObj['aggragateColumnName'] = aggragateColumnName;
            columns[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))
            {
                tablesObj.push(tableName);
            }
        }

    });
    console.log("columns:::" + JSON.stringify(columns));
    var rows = {};
    $('#analyticsRows div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        var aggragateColumnName = $("#" + divId).attr("data-aggragate-name");
        if (tableName != null
                && tableName != ''
                && columnName != null && columnName != '') {

            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            columnObj['aggragateColumnName'] = aggragateColumnName;
            rows[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))
            {
                tablesObj.push(tableName);
            }
        }

    });
    console.log("rows:::" + JSON.stringify(rows));
    var comboRows = {};
    $('#analyticsComboRows div').each(function (i, ele) {
        console.log(i + ': ' + ele);
        var divId = $(this).attr("id");
        console.log("divId:::" + divId);
        var connType = $("#" + divId).attr("data-coon-type");
        var tableName = $("#" + divId).attr("data-table-name");
        var columnName = $("#" + divId).attr("data-column-name");
        var aggragateColumnName = $("#" + divId).attr("data-aggragate-name");
        if (tableName != null
                && tableName != ''
                && columnName != null && columnName != '') {

            var columnObj = {};
            columnObj['connType'] = connType;
            columnObj['tableName'] = tableName;
            columnObj['columnName'] = columnName;
            columnObj['aggragateColumnName'] = aggragateColumnName;
            comboRows[i] = columnObj;
            if (!(tablesObj.indexOf(tableName) > -1))
            {
                tablesObj.push(tableName);
            }

        }
    });
    if (comboRows != null && comboRows != '' && comboRows != undefined &&
            Object.keys(comboRows).length > 1) {
        errorCount++;
        errorMessage("#disanalyticsComboRows", "Columns Should not be Greater than one");
    } else if (chartType != null && chartType != '' && chartType == 'ComboChart'
            && !(comboRows != null && comboRows != '' && comboRows != undefined &&
                    !jQuery.isEmptyObject(comboRows)))
    {
        errorCount++;
        errorMessage("#disanalyticsComboRows", "Should not be null");
    }
    var joinQuery = $("#analyticsInnerJoinClause").val();
    if (tablesObj != null && !jQuery.isEmptyObject(tablesObj) && Object.keys(tablesObj).length > 1
            && !(joinQuery != null && joinQuery != '' && joinQuery != undefined)) {
        errorCount++;
        errorMessage("#disanalyticsJoinClause", "Join columns should not be null");
    }
    var columnsLabel = {};
    $('#selectedColLabelsDiv div').each(function (i, ele) {
        var colLabelName = $(this).attr("data-column-name");
        var columnLabel = $("#COL_" + colLabelName).val();
        if (columnLabel != null && columnLabel != '') {
            columnsLabel[colLabelName] = columnLabel;
        } else {
            columnsLabel[colLabelName] = colLabelName;
        }
    });
    var sqlQuery = $("#analyticsSQL").val();
    if ((columns != null && !jQuery.isEmptyObject(columns))
            || (rows != null && !jQuery.isEmptyObject(rows))
            || (sqlQuery != null && sqlQuery != '' && $.trim(sqlQuery) != null)
            ) {
        errorCount == 0;
    } else {
        errorMessage("#disanalyticsColumns", "Should not be null");
        errorMessage("#disanalyticsRows", "Should not be null");
        errorCount == 1;
    }
    if (!(chartTitle != null && chartTitle != '')) {
        errorCount++;
        errorMessage("#dischartTitle", "Should not be null");
    }
    var chartOptAllObj = {};
    var errorMessageStr = "";
    $('#chartOptionsOlList li').each(function (i, ele) {
        var optObj = {};
        var parent = $(this).parent().parent().get(0).children;
        var parentName = parent[0].innerText.trim();
        if (parentName != null && parentName != '' && parentName != undefined)
        {
            parentName = parentName.replace(":", ">");
        }
        var optColName = $(this).attr("data-column-name");
        var optName = $("#" + optColName).attr("data-opt-name");
        var optRegex = $("#" + optColName).attr("data-regex");
        var optRegexMesg = $("#" + optColName).attr("data-regex-msg");
        var optMan = $("#" + optColName).attr("data-man");
        var inputType = $("#" + optColName).attr("type");
        var optValue = $("#" + optColName).val();
        if (inputType == 'checkbox') {
            if ($("#" + optColName).is(':checked')) {
                optValue = true;
            } else {
                optValue = false;
            }
        }
        if (inputType == 'number') { //filter
            if (optValue != null && optValue != '' && optValue >= 1) {
                optValue = parseInt(optValue);
            }
        }

        if (optValue != null && optValue != '') {
            chartOptAllObj[optColName] = optValue;
        } else if (optMan == 'M') {
            errorCount++;
            if (parentName != null) {
                errorMessageStr += "<tr><td>  " + '<p class="visionGenericTabStatusDialog">' + " " + '<span style="color:blue;">' + "" + parentName + " " + optName + "</span><b>:</b> Should not be null.</tr></td>";
            } else {
                errorMessageStr += "<tr><td>  " + '<p class="visionGenericTabStatusDialog">' + " " + '<span style="color:blue;">' + " " + optName + "</span><b>:</b> Should not be null.</tr></td>";
            }
        }

    });
    console.log("chartOptAllObj:::" + JSON.stringify(chartOptAllObj));
    var paramArray = $("#filterParamArray").val();
    var whereClauseCond = $("#analyticsWhereClauseInner").text();
    var groupByCond = $("#analyticsInnerGroupClause").val();
    if (errorCount == 0) {
        hideErrors();
        $.ajax({
            type: "post",
            traditional: true,
            dataType: 'json',
            url: "generateChartByProps",
            data: {
                columns: JSON.stringify(columns),
                rows: JSON.stringify(rows),
                chartType: chartType,
                chartTitle: chartTitle,
                whereClauseCond: whereClauseCond,
                columnsLabel: JSON.stringify(columnsLabel),
                sqlQuery: sqlQuery,
                chartId: chartId,
                chartFormId: chartFormId,
                chartOptionObj: JSON.stringify(chartOptAllObj),
                comboRows: JSON.stringify(comboRows),
                joinCondition: joinQuery,
                groupByCond: groupByCond,
                chartDivId: 'designAreaAnalyticsDisplayDiv',
                paramArray: paramArray
            },
            cache: false,
            success: function (data, status, xhr) {
                stopLoader();
                if (data != null && !jQuery.isEmptyObject(data)) {
                    $("#designAreaAnalyticsRefreshButtonsDiv").empty();
                    $("#designAreaAnalyticsFilterButtonsDiv").empty();
                    $("#designAreaAnalyticsGridButtonsDiv").empty();
                    var jqwidgetFlag = data['jqwidgetFlag'];
                    if (jqwidgetFlag != null && jqwidgetFlag != '' && jqwidgetFlag != undefined && jqwidgetFlag == 'Y') {
                        var chartsObj = data['settingObj'];
                        var chartId = data['chartId'];
                        var height = chartsObj['height'];
                        var width = chartsObj['width'];
                        var finalChartObj = {};
                        finalChartObj = data['settingObj'];
                        finalChartObj.source = data['dataObjList'];
                        var settings = {};
                        settings = finalChartObj;
                        $("#" + chartId).css("width", "" + width + "", "important");
                        $("#" + chartId).css("height", "" + height + "", "important");
                        $("#" + chartId).jqxChart(settings);
                        $("#" + chartId).on('click', function (e) //nested
                        {
                            if (e.args != null) {
                                selectJqwidgetClickHandler(e.args.serie.dataField, e.args.elementValue, chartId);
                            }
                        });
                        var paramArray = []; //filter
                        $("#designAreaAnalyticsGridButtonsDiv").append('<img src="images/GridDB.png" class="visionAnalyticSuiteGridData" title="Grid Data" onclick="showNestedGrid(' + paramArray + ')">');
                        $("#designAreaAnalyticsFilterButtonsDiv").append('<img src="images/change_requests_icon_2.png" class="visionAnalyticSuiteFilterData" title="Filter Form" onclick="showChartFilterForm()">');
                        $("#designAreaAnalyticsRefreshButtonsDiv").append('<img src="images/refresh.png" class="visionAnalyticSuiteRefreshData" title="Refresh Chart" onclick="showRefreshChart()">');
                    } else {
                        var dataArray = [];
                        var options = {};
                        var input = "<input type='hidden' id='dataObj'/><input type='hidden' id='dataOption'/>"
                        $("#designAreaAnalyticsDisplayDiv").html(input);
                        $("#dataObj").val(JSON.stringify(data['dataObjList']));
                        $("#dataOption").val(JSON.stringify(data['optionObj']));
                        dataArray = JSON.parse($("#dataObj").val());
                        options = JSON.parse($("#dataOption").val());
                        var chartInit = data['chartInit'];
                        var chart = eval(chartInit);
                        dataArray = google.visualization.arrayToDataTable(data['dataObjList']);
                        chart.draw(dataArray, options);
                        google.visualization.events.addListener(chart, 'select', function () { //nested
                            clickHandler(chart, dataArray, chartId);
                        });
                        var paramArray = []; //filter
                        $("#designAreaAnalyticsGridButtonsDiv").append('<img src="images/GridDB.png" class="visionAnalyticSuiteGridData" title="Grid Data" onclick="showNestedGrid(' + paramArray + ')">');
                        $("#designAreaAnalyticsFilterButtonsDiv").append('<img src="images/change_requests_icon_2.png" class="visionAnalyticSuiteFilterData" title="Filter Form" onclick="showChartFilterForm()">');
                        $("#designAreaAnalyticsRefreshButtonsDiv").append('<img src="images/refresh.png" class="visionAnalyticSuiteRefreshData" title="Refresh Chart" onclick="showRefreshChart()">');
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
        stopLoader();
        $("#dialog").html(errorMessageStr);
        $("#dialog").dialog({
            title: (labelObject['Error'] != null ? labelObject['Error'] : 'Error'),
            modal: true,
            height: 'auto',
            minHeight: 'auto',
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
    }

}
function addColumnCubeMapValue(event, $this)
{
    var selectedColumnStr = "<tr style='height: 1px'>"
            + "<td width='1%' class='visionCubeValueImgTd1'>"
            + "<img src='images/Detele Red Icon.svg' onclick='deleteSelectedRow(this)' class='visionCubeValueImg' title='Delete' style='width:15px;height: 15px;cursor:pointer;'></td>"
            + "<td width='19%'><div id='analyticsCubeValueInner" + cubeValueInner + "' class='analyticsCubeValueRowsInner'></div></td>"
            + "<td width='20%'><input class='visionCubeValueInput' type='text' value=''></td>"
            + "<td width='20%'><input class='visionCubeValueInput' type='text' value=''></td></tr>";

    $("#VisionCubeValueTableId tbody").append(selectedColumnStr);
    cubeValueInner++;
    $('[id^="analyticsCubeValueInner"]').droppable({
        revert: "invalid",
        refreshPositions: true,
        cursor: 'move',
        drop: function (event, ui) {
            var label = ui.draggable[0].innerText
            var columnName = ui.draggable[0].innerText;
            var tableName = "";
            var connType = "";
            var selectedItem = $('#availableAnalyticsConnections').jqxTree('getItem', ui.draggable[0]);
            var selectedParentItem = {};
            try {
                selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedItem.parentElement);
                if (selectedParentItem != null) {
                    tableName = selectedParentItem['value'];
                    selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedParentItem.parentElement);
                    connType = selectedParentItem['value'];
                }
            } catch (e) {
            }
            var columnData = '<div id="COLUMN_' + tableName + "_" + columnName + '" class="analyticsDivData"' //group
                    + ' title="' + columnName + '" data-table-name="' + tableName + '" '
                    + ' data-column-name="' + columnName + '" data-coon-type ="' + connType + '" ><span class="visionColsText" ondblclick="selectColumnFun(this,\'' + tableName + "." + columnName + '\',\'analyticsColumnsInner\')" >' + columnName
                    + '</span><img src="images/close_white.png" title="Remove Column"'
                    + ' onclick="removeColumn(this,\'' + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;">'
                    + '</div>';
            $("#" + this.id).append(columnData);
            var columnLabelData = "<div id='COL_LBL_DIV_" + columnName + "' data-column-name='" + columnName + "' class='columnLabelDivs'>"
                    + "<label>" + columnName + ":</label>"
                    + "<input type='text' id='COL_" + columnName + "' data-column-name='" + columnName + "' value='" + columnName + "'/>  "
                    + ""
                    + "</div>";

            $("#selectedColLabelsDiv").append(columnLabelData);

            
        }

    });
}
function deleteSelectedRow($this)
{
    $($this).closest("tr").remove();
}
//for nested charts and grid

function addColumnCubeMapValue(event, $this) //cube mdrm
{
    var selectedColumnStr = "<tr style='height: 1px'>"
            + "<td width='1%' class='visionCubeValueImgTd1'>"
            + "<img src='images/Detele Red Icon.svg' onclick='deleteCubeSelectedRow(this)' class='visionCubeValueImg' title='Delete' style='width:15px;height: 15px;cursor:pointer;'></td>"
            + "<td width='19%'><div id='analyticsCubeValueInner" + cubeValueInner + "' class='analyticsCubeValueRowsInner'></div></td>"
            + "<td width='20%'><input class='visionCubeValueInput' type='text' value=''></td>"
            + "<td width='20%'><input class='visionCubeValueInput' type='text' value=''></td></tr>";

    $("#VisionCubeValueTableId tbody").append(selectedColumnStr);
    cubeValueInner++;
    $('[id^="analyticsCubeValueInner"]').droppable({
        revert: "invalid",
        refreshPositions: true,
        cursor: 'move',
        drop: function (event, ui) {
            var label = ui.draggable[0].innerText
            var columnName = ui.draggable[0].innerText;
            var tableName = "";
            var connType = "";
            var selectedItem = $('#availableAnalyticsConnections').jqxTree('getItem', ui.draggable[0]);
            var selectedParentItem = {};
            try {
                selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedItem.parentElement);
                if (selectedParentItem != null) {
                    tableName = selectedParentItem['value'];
                    selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedParentItem.parentElement);
                    connType = selectedParentItem['value'];
                }
            } catch (e) {
            }
            var columnData = '<div id="COLUMN_' + tableName + "_" + columnName + '" class="analyticsDivData"' //group
                    + ' title="' + columnName + '" data-table-name="' + tableName + '" '
                    + ' data-column-name="' + columnName + '" data-coon-type ="' + connType + '" ><span class="visionColsText" ondblclick="selectColumnFun(this,\'' + tableName + "." + columnName + '\',\'analyticsColumnsInner\')" >' + columnName
                    + '</span><img src="images/close_white.png" title="Remove Column"'
                    + ' onclick="removeColumn(this,\'' + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;">'
                    + '</div>';
            $("#" + this.id).append(columnData);
            var columnLabelData = "<div id='COL_LBL_DIV_" + columnName + "' data-column-name='" + columnName + "' class='columnLabelDivs'>"
                    + "<label>" + columnName + ":</label>"
                    + "<input type='text' id='COL_" + columnName + "' data-column-name='" + columnName + "' value='" + columnName + "'/>  "
                    + ""
                    + "</div>";

            $("#selectedColLabelsDiv").append(columnLabelData);

            
        }

    });
}
//cube mdrm
function deleteCubeSelectedRow($this)
{
    $($this).parents("tr").remove();
}
function addColumnCubePossibleDimValue(event, $this)
{
    var selectedColumnStr = "<tr style='height: 1px'>"
            + "<td width='1%' class='visionCubePossibleDimImgTd1'>"
            + "<img src='images/Detele Red Icon.svg' onclick='deleteCubeSelectedRow(this)' class='visionCubePossibleDimImg' title='Delete' style='width:15px; height: 15px; cursor:pointer;'></td>"
            + "<td width='19%'><div id='analyticsCubePossibleDimFactColValueInner" + cubePossDimInner + "' class='analyticsCubePossibleDimRowsInner'></div></td>"
            + "<td width='20%'><div id='analyticsCubePossibleDimColValueInner" + cubePossDimInner + "' class='analyticsCubePossibleDimRowsInner'></div></td>"
            + "<td width='20%'><input class='visionCubeValueInput' type='text' value=''></td></tr>"
    $("#VisionCubePossibelDimTableId tbody").append(selectedColumnStr);
    cubePossDimInner++;
    $('[id^="analyticsCubePossibleDimFactColValueInner"]').droppable({
        revert: "invalid",
        refreshPositions: true,
        cursor: 'move',
        drop: function (event, ui) {
            var label = ui.draggable[0].innerText
            var columnName = ui.draggable[0].innerText;
            var tableName = "";
            var connType = "";
            var selectedItem = $('#availableAnalyticsConnections').jqxTree('getItem', ui.draggable[0]);
            var selectedParentItem = {};
            try {
                selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedItem.parentElement);
                if (selectedParentItem != null) {
                    tableName = selectedParentItem['value'];
                    selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedParentItem.parentElement);
                    connType = selectedParentItem['value'];
                }
            } catch (e) {
            }
            var columnData = '<div id="COLUMN_' + tableName + "_" + columnName + '" class="analyticsDivData"' //group
                    + ' title="' + columnName + '" data-table-name="' + tableName + '" '
                    + ' data-column-name="' + columnName + '" data-coon-type ="' + connType + '" ><span class="visionColsText" ondblclick="selectColumnFun(this,\'' + tableName + "." + columnName + '\',\'analyticsColumnsInner\')" >' + columnName
                    + '</span><img src="images/close_white.png" title="Remove Column"'
                    + ' onclick="removeColumn(this,\'' + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;">'
                    + '</div>';
            $("#" + this.id).append(columnData);
            var columnLabelData = "<div id='COL_LBL_DIV_" + columnName + "' data-column-name='" + columnName + "' class='columnLabelDivs'>"
                    + "<label>" + columnName + ":</label>"
                    + "<input type='text' id='COL_" + columnName + "' data-column-name='" + columnName + "' value='" + columnName + "'/>  "
                    + ""
                    + "</div>";
            $("#selectedColLabelsDiv").append(columnLabelData);
        }

    });
    $('[id^="analyticsCubePossibleDimColValueInner"]').droppable({
        revert: "invalid",
        refreshPositions: true,
        cursor: 'move',
        drop: function (event, ui) {
            var label = ui.draggable[0].innerText
            var columnName = ui.draggable[0].innerText;
            var tableName = "";
            var connType = "";
            var selectedItem = $('#availableAnalyticsConnections').jqxTree('getItem', ui.draggable[0]);
            var selectedParentItem = {};
            try {
                selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedItem.parentElement);
                if (selectedParentItem != null) {
                    tableName = selectedParentItem['value'];
                    selectedParentItem = $('#availableAnalyticsConnections').jqxTree('getItem', selectedParentItem.parentElement);
                    connType = selectedParentItem['value'];
                }
            } catch (e) {
            }
            var columnData = '<div id="COLUMN_' + tableName + "_" + columnName + '" class="analyticsDivData"' //group
                    + ' title="' + columnName + '" data-table-name="' + tableName + '" '
                    + ' data-column-name="' + columnName + '" data-coon-type ="' + connType + '" ><span class="visionColsText" ondblclick="selectColumnFun(this,\'' + tableName + "." + columnName + '\',\'analyticsColumnsInner\')" >' + columnName
                    + '</span><img src="images/close_white.png" title="Remove Column"'
                    + ' onclick="removeColumn(this,\'' + columnName + '\')" class="visionCloseColumnBtn" style="display: inline;">'
                    + '</div>';
            $("#" + this.id).append(columnData);
            var columnLabelData = "<div id='COL_LBL_DIV_" + columnName + "' data-column-name='" + columnName + "' class='columnLabelDivs'>"
                    + "<label>" + columnName + ":</label>"
                    + "<input type='text' id='COL_" + columnName + "' data-column-name='" + columnName + "' value='" + columnName + "'/>  "
                    + ""
                    + "</div>";
            $("#selectedColLabelsDiv").append(columnLabelData);
        }

    });
}
function deleteCubeSelectedRow($this)//cube mdrm
{
    $($this).closest("tr").remove();
}


