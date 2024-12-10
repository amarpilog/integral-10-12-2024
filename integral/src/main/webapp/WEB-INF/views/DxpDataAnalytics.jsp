<%-- 
    Document   : Data Analytics
    Created on : Sep 24, 2019, 11:25:31 AM
    Author     : Pilog
--%>

<%@page contentType="text/html" pageEncoding="UTF-8"%>
<%@ taglib prefix="c"   uri="http://java.sun.com/jsp/jstl/core" %>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt" %>
<%@ taglib prefix="fn"  uri="http://java.sun.com/jsp/jstl/functions" %>
<%@ taglib prefix="sec" uri="http://www.springframework.org/security/tags" %>
<%@ taglib prefix="tags" tagdir="/WEB-INF/tags" %>
<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <%--<tags:commonfiles/>--%>
        <%@include file="commonfiles.jsp" %>
        <title>Data Exchange Portal Analytics</title>

        <script>
            var basicData = {};
            var chartFilterItems = {};
            $(document).ready(function () {

                chartFilterItems = '${filterItems}';
                try {
                    $(window).resize(function ()
                    {
                        windowHeight = $(window).height();
                        pageHeader = $(".visionHeader").height();
                        pageFooter = $(".visionFooterMain").height();
                        pageBreadcrum = $(".visionBreadcrumMain").height();
                        $pageBodycontainer = $('.visionBodyContent');
                        pageBodycontinertop = parseInt($pageBodycontainer.css('padding-top'));
                        pageBodycontinerbottom = parseInt($pageBodycontainer.css('padding-bottom'));
                        gridHeight = windowHeight - pageHeader - pageFooter - pageBreadcrum - pageBodycontinertop - pageBodycontinerbottom - 5;
                    }).resize();
                    var theme = "ui-redmond";
                    $('#jqxTabs').jqxTabs({width: '100%', keyboardNavigation: true, position: 'top', theme: theme});
                    $(window).resize(function ()
                    {

                        $('#jqxTabs').jqxTabs({height: gridHeight + "px"});
                    }).resize();

                    $('#jqxTabs').on('selected', function (event) {

                        var selectedTab = event.args.item + 1;
                        var img = $("#jqxTabs ul li:nth-child(" + selectedTab + ")").find('img').attr('src');
                        var newimage = img.replace(".png", "");
                        $("#jqxTabs ul li:nth-child(" + selectedTab + ")").find('img').attr('src', newimage + '_white.png');

                        google.charts.setOnLoadCallback(getTabsData(event.args.item, event.args.item));
                    });
                    $('#jqxTabs').on('unselecting', function (event) {
                        var unselectedTab = event.args.item + 1;
                        var un_img = $("#jqxTabs ul li:nth-child(" + unselectedTab + ")").find('img').attr('src');
                        var newimage = un_img.replace("_white", "");
                        $("#jqxTabs ul li:nth-child(" + unselectedTab + ")").find('img').attr('src', newimage);
                    });
                } catch (e) {

                }
                google.charts.setOnLoadCallback(getTabsData(0, 0));
            });
            $(document).mouseup(function (e)
            {
                var container = $(".visionAnalyticCheckBoxDataClass");
                var container1 = $(".visionAnalyticChartPersonalize");
                var container2 = $(".visionarrow");
                if ((!container.is(e.target)
                        && container.has(e.target).length === 0) && (!container1.is(e.target)
                        && container1.has(e.target).length === 0) && (!container2.is(e.target)
                        && container2.has(e.target).length === 0))
                {
                    container.hide();
                    container2.hide();
                }

            });




            function startTabLoader() {
                console.log("in start startTabLoader ");
                setTimeout(function () {
                    $("#wait").css("opacity", "0.99");
                    $("#wait").css("display", "block");
                    $("body").css("pointer-events", "none");
                }, 50);
            }

            function endTabLoader() {
                console.log("in endTabLoaderregister");
                $("#wait").css("display", "none");
                $("body").css("pointer-events", "auto");
            }
            function ajaxTabLoader() {
                console.log("in start ajaxTabLoader ");
                setTimeout(function () {
                    $("#wait").css("opacity", "0.99");
                    $("#wait").css("display", "block");
                    $("body").css("pointer-events", "none");
                }, 600);
            }
            function ajaxEndTabLoader() {
                console.log("in start ajaxTabLoader ");
                delay(function () {
                    $("#wait").css("opacity", "0.99");
                    $("#wait").css("display", "block");
                    $("body").css("pointer-events", "none");
                }, 300);
            }
            var delay = (function () {
                var timer = 0;
                return function (callback, ms) {
                    clearTimeout(timer);
                    timer = setTimeout(callback, ms);
                };
            })();

            function analyticCheckboxToogle() {
                $("#visionAnalyticCheckBoxData").toggle();
                $(".visionarrow").toggle();

            }



        </script>


    </head>
    <body>
        <div class="visionMainPage">
            <div id='content'>
                <div id='jqxMenu'>

                </div>

            </div>
            
            <div class="visionBodyContent">
                <div id="dataIMG" class="chartDownloadDialog" style="display:none">

                    <form action='downloadChartImagePDF' id='pdfbodyForm'  method='POST' target='_blank' style='display: inline'>
                        <input type='hidden' id='imageData' name='imageData' value="">
                        <select id="selectedChart" >
                            <option  value="PNG">DownloadChartAsPNG</option>
                            <option  value="PDF">DownloadChartAsPDF</option>
                        </select>
                    </form>

                </div>
                 <%--<tags:sideMenu/>--%>
                 <%@include file="sideMenu.jsp" %>
                <div class="visionBodyContentInner">
                    <form action='downloadChartImageAllPDF' id='pdfChartForm'  method='POST' target='_blank' >
                        <div id ="chartPersonalizeId" class = "visionAnalyticChartPersonalize" >
                            <img src="images/attach_download.png"  alt="" class="analyticsSidebarIconMenu" id="analyticsIcons" onclick="chartObjImg();"> 
                            <input type="hidden" value="" id="chartImagesDashBoardName" name="chartImagesDashBoardName"/></div>
                            <input type="hidden" value="" id="chartImageObj" name="chartImageObj"/></div>
                    </form>
                    
                    <div id ="chartPersonalizeId" class = "visionAnalyticChartPersonalize" style="display:none">
                        <img src="<c:url value="/"/>images/menu.png"  alt="" class="analyticsSidebarIconMenu" id="analyticsIcons" onclick="analyticCheckboxToogle();"> 
                    </div>
                    <div class="visionarrow" style="display:none;"></div>
                    <div id="visionAnalyticCheckBoxData" class="visionAnalyticCheckBoxDataClass" style="display:none;">
                    </div>
                    <div class="visionGenericTabs">

                        <div id='jqxWidget'>
                            <div id='jqxTabs' class="visionReportandDashboard">
                                ${tabsList}
                            </div>
                        </div>     
                    </div>
                    <div id ="charts"></div>
                    <div id ="dialog"></div>
                    <div id ="logoutDailog"></div>
                    <div id ="dialog1"></div>
                    <div id ="expandDialog"></div>
                    <div id ="nestedChartDialog"></div>
                    <div id="dddw"></div> 
                    <div id='importsearchcriteria' style="display: none"></div>
                    <input type="hidden" value="${tabComponentId}" id="gridIdStr"/>
                    <input type="hidden" value="${compTypeStr}" id="compTypeStr"/>
                    <input type="hidden" value="${tabName}" id="tabNameStr"/>
                    <input type="hidden" value="${filterItems}" id="chartFilterItems"/>
                    <input type="hidden" value="" id="tabChartsId"/>
                    <input type="hidden" value="" id="tabComponentId"/>
                    <input type="hidden" value="" id="startIndex"/>
                    <input type="hidden" value="" id="endIndex"/>
                    <input type="hidden" value="" id="pageSize"/>
                    <input type="hidden" value="" id="totalCount"/>
                    <input type="hidden" value="" id="rowsCount"/>
                    <input type="hidden" value="" id="scrollFlag"/>
                    <input type="hidden" value="" id="analysisType"/>
                    <input type="hidden" value="" id="processId"/>

                    <div class="overlay visionLoginOverlay" id="overlay" style="display:none;"></div>
                    
                </div>
            </div>
            
        </div>
        <form id="submitForm" method="post" target="_blank">
            <c:if test="true">
                <input type="hidden" name="${_csrf.parameterName}" value="${_csrf.token}" /> 
            </c:if> 
            <input type="hidden" name="items" id="items" value=""/>
        </form>
        <form id="orgSubmitForm" method="post" target="_blank">
            <c:if test="true">
                <input type="hidden" name="${_csrf.parameterName}" value="${_csrf.token}" /> 
            </c:if> 
            <input type="hidden" name="items" id="items" value=""/>
        </form>
        <form id="nestedChartsubmitForm" method="post">
            <c:if test="true">
                <input type="hidden" name="${_csrf.parameterName}" value="${_csrf.token}" /> 
            </c:if> 
            <input type="hidden" name="nestedtabComponentId" id="nestedtabComponentId" value=""/>
            <input type="hidden" name="nestedPageChartParams" id="nestedPageChartParams" value=""/>
            <input type="hidden" name="nestedDhChartParams" id="nestedPageChartParams" value=""/>
            <input type="hidden" name="nestedclickedChartId" id="nestedclickedChartId" value=""/>
        </form>

        <form action='downloadJqxChartImagePDF' id='jqxPdfbodyForm'  method='POST' target='_blank' style='display: inline'>           
            <input type='hidden' id='jqxImageData1' name='jqxImageData1' value="">
        </form>

    </body>
</html>
