<%-- 
    Document   : commonfiles
    Created on : 19 Nov, 2021, 2:05:43 PM
    Author     : PiLog
--%>

<%@page contentType="text/html" pageEncoding="UTF-8"%>
<%@ taglib prefix="c"   uri="http://java.sun.com/jsp/jstl/core" %>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt" %>
<%@ taglib prefix="fn"  uri="http://java.sun.com/jsp/jstl/functions" %>
<%@ taglib prefix="sec" uri="http://www.springframework.org/security/tags" %>
<!DOCTYPE html>
<meta http-equiv="X-UA-Compatible" content="IE=edge, chrome=1" />
<meta charset="UTF-8">

<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="keygeneration" content="${secretKey}">  
<meta http-equiv='cache-control' content='no-cache'>
<meta http-equiv='pragma' content='no-cache'> 
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/select2/4.0.0/css/select2.min.css">
<link href="https://fonts.googleapis.com/css?family=Open+Sans:300,400,600,700,800" rel="stylesheet">
<link href="https://fonts.googleapis.com/css?family=Roboto%7CRoboto:medium" rel="stylesheet">
<link href="https://fonts.googleapis.com/css?family=Quicksand:500,700" rel="stylesheet">
<link rel="stylesheet" type="text/css" href="<c:url value="/"/>css/jquery-ui.css">
<link href="https://cdn.rawgit.com/harvesthq/chosen/gh-pages/chosen.min.css" rel="stylesheet"/>



<link rel="stylesheet" href="<c:url value="/"/>css/pivot.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>css/pivot.min.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>css\theme\etlDark.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>css\theme\headerDark.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>css\theme\jqxDark.base.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>css\theme\jqxDark.energyblue.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>css\theme\PiLogCloudDark.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>css\theme\sidebarDark.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>css\theme\dxpAnalyticsDark.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>css/etl.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>css/IntelleSense.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>css/header.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>css\sidebar.css" type="text/css" />  
<link rel="stylesheet" href="<c:url value="/"/>css/dxptheme.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>css/jquery.flowchart.css" type="text/css" /> 
<link rel="stylesheet" href="<c:url value="/"/>css/jquery.timepicker.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>css\bootstrap.min.css" type="text/css" />

<link rel="stylesheet" href="<c:url value="/"/>css\constituency.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>css\font-awesome.min.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>css\animate.min.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>css\customSchedulCalendar.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>css\theme\homeThemes.css" type="text/css" />
<link rel="stylesheet"href="<c:url value="/"/>css\fonts.css" type="text/css" />


<link rel="stylesheet" href="<c:url value="/"/>css\introjs.min.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>css\input.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>css\reply.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>css\says.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>css\setup.css" type="text/css" />


<script src="https://polyfill.io/v3/polyfill.min.js?features=default"></script>
<script src="https://maps.googleapis.com/maps/api/js?key=AIzaSyBktbrEU63oeqNZEbJr7P77CoqKXvj0T0w&callback=initMap&v=weekly"></script>

<!--<script src="https://cdnjs.cloudflare.com/ajax/libs/ace/1.1.3/ace.js" type="text/javascript" charset="utf-8"></script> -->

<link rel="stylesheet" href="<c:url value="/"/>css\theme\bootstrap-multiselect.css" type="text/css" />
<script src="https://cdnjs.cloudflare.com/ajax/libs/ace/1.35.0/ace.js" type="text/javascript" charset="utf-8"></script>

<link rel="stylesheet" href="<c:url value="/"/>css\daterangepicker.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>css\keyboard.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>css\aiPanel.css" type="text/css" />


<!-- conv ai dialog buttons --> 
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.3/css/all.min.css" />
<!-- conv ai dialog buttons --> 
<!--*/pivot*/-->


<!--pivot-->
<script src="https://accounts.google.com/gsi/client" async defer></script>
<script src="https://apis.google.com/js/api.js"></script>
<!--<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.17/d3.min.js"></script>-->

<script type="text/javascript" src="<c:url value="/"/>js/jquery-1.11.2.js"></script>
<script type="text/javascript" src="<c:url value="/"/>js/jquery-ui.js"></script>

<%-- <script type="text/javascript" src="<c:url value="/"/>js/jquery-3.3.1.js"></script> --%>
<script src="https://cdnjs.cloudflare.com/ajax/libs/select2/4.0.0/js/select2.min.js"></script>
<script type="text/javascript" src="<c:url value="/"/>js/jquery-ui.js"></script> 
<script type="text/javascript" src="<c:url value="/"/>js/pivot.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxpivot.js"></script> 
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxpivotgrid.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxpivotdesigner.js"></script>
<script type="text/javascript" src="<c:url value="/"/>js/d3.min.js"></script>

<script type="text/javascript" src="<c:url value="/"/>js/c3.min.js"></script>
<script type="text/javascript" src="<c:url value="/"/>js/papaparse.min.js"></script>
<script type="text/javascript" src="<c:url value="/"/>js/c3_renderers.js"></script>
<!--<script type="text/javascript" src="<c:url value="/"/>js/ace.js"></script>-->
<script src="https://checkout.razorpay.com/v1/checkout.js"></script>  
<script src="<c:url value="/"/>js/VisionDDW.js"></script>  

<script src="<c:url value="/"/>js/logout.js"></script>
<script src="<c:url value="/"/>js/aes.js"></script>
<script src="<c:url value="/"/>js/jquery.clearsearch-instance-dropdown.js"></script>
<script src="<c:url value="/"/>js/jquery.scannerdetection.js"></script>
<script src="<c:url value="/"/>js/jquery.easing.min.js"></script>
<script src="<c:url value="/"/>js/jquery.timepicker.js"></script>
<script src="<c:url value="/"/>js/jquery-cron-quartz.js"></script>
<script src="<c:url value="/"/>js/popper.min.js"></script>
<script src="<c:url value="/"/>js/bootstrap.min.js"></script>
<script src="<c:url value="/"/>js/waves.min.js"></script>
<script src="<c:url value="/"/>js/jquery.slimscroll.js"></script>
<script src="<c:url value="/"/>js/modernizr.js"></script>
<script src="<c:url value="/"/>js/script.js"></script>
<script src="<c:url value="/"/>js/loginOperations.js"></script>
<script src="<c:url value="/"/>js/jquery.validate.min.js"></script>
<script src="<c:url value="/"/>js/jquery-steps.js"></script>
<script src="<c:url value="/"/>js/invenMngmnt.js"></script>
<script src="<c:url value="/"/>js/slick.js"></script>
<script src="<c:url value="/"/>js/dxptheme.js"></script>
<script src="<c:url value="/"/>js/Bubbles.js"></script>
<script type="text/javascript" src="<c:url value="/"/>js/jquery.ba-throttle-debounce.min.js"></script>
<script src="<c:url value="/"/>js/chosen.jquery.js"></script>
<script src="<c:url value="/"/>js/DxpUnification.js"></script>
<script src="https://maps.googleapis.com/maps/api/js?sensor=false"></script> 

<link rel="stylesheet" href="<c:url value="/"/>css\dxpAnalytics.css" type="text/css" />
<script src="<c:url value="/"/>js/jquery.dragtable.js"></script>
<script src="<c:url value="/"/>js/jquery.flowchart.js"></script>
<script src="<c:url value="/"/>js/nimicPOC.js"></script>
<script type="text/javascript" src="<c:url value="/"/>js/bootstrap-multiselect.js"></script>
<link rel="stylesheet" href="<c:url value="/"/>jqwidgets/styles/jqx.base.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>jqwidgets/styles/jqx.bootstrap.css" type="text/css" />  
<link rel="stylesheet" href="<c:url value="/"/>jqwidgets/styles/jqx.energyblue.css" type="text/css" />       
<link rel="stylesheet" href="<c:url value="/"/>jqwidgets/styles/jqx.ui-redmond.css" type="text/css" />       
<link rel="stylesheet" href="<c:url value="/"/>jqwidgets/styles/jqx.metro.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>jqwidgets/styles/jqx.fresh.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>jqwidgets/styles/jqx.darkblue.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>css\calenderMain.css" type="text/css" />
<link rel="stylesheet" type="text/css" href="<c:url value="/"/>css/jquery-ui-visuallizeResize.css">
<link rel="stylesheet" href="<c:url value="/"/>css\voteAnalysis.css" type="text/css" />
<link rel="stylesheet" href="<c:url value="/"/>css\spectrum.css" type="text/css" />
<link rel="stylesheet" href="css\slick.css" type="text/css" />
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/bootstrap-select/1.13.1/css/bootstrap-select.css" />
<link rel="stylesheet" href="<c:url value="/"/>css\mobiscroll.jquery.min.css" type="text/css" />


<script src="<c:url value="/"/>js/calenderMain.js"></script>
<script src="<c:url value="/"/>jqwidgets/jqx-all.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxtabs.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxresponse.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxnotification.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxbuttons.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxcheckbox.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxscrollbar.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxmenu.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxgrid.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxgrid.selection.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxgrid.sort.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxgrid.columnsresize.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxgrid.columnsreorder.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxgrid.edit.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxlistbox.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxdropdownlist.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxgrid.pager.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxgrid.filter.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxprogressbar.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxinput.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxcalendar.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxdatetimeinput.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxdata.export.js"></script> 
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxgrid.export.js"></script> 
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxgrid.grouping.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxgrid.aggregates.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxpanel.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxtree.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxtree.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxpopover.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxtooltip.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxnotification.js"></script>
<script type="text/javascript" src="<c:url value="/"/>js/jquery.ajaxfileupload.js"></script>
<script src="<c:url value="/"/>js/dxpDataAnalytics.js"></script>
<script src="<c:url value="/"/>js/dxpDataAnalyticsJag.js"></script>
<script src="<c:url value="/"/>js/dxpWorkflow.js"></script>
<script src="<c:url value="/"/>js/jquery.dragtable.js"></script>
<script src="<c:url value="/"/>js/colResizable-1.6.js"></script> 
<script src="<c:url value="/"/>js/jquery.tablednd.js"></script>
<!-- <script src='https://cdn.plot.ly/plotly-2.6.3.min.js'></script> -->
<script src='<c:url value="/"/>js/plotly.2.24.2.min.js'></script>

<%-- <script src="<c:url value="/"/>js/loader.js"></script> --%>
<script src="<c:url value="/"/>js/echarts.min.js"></script>
<!--<script src='https://d3js.org/d3.v3.min.js'></script>-->

<script src="<c:url value="/"/>js/jspdf.min.js"></script>
<script src="<c:url value="/"/>js/jspdf.plugin.autotable.js"></script>
<script src="<c:url value="/"/>js/tableExport.min.js"></script>
<script src="<c:url value="/"/>js/xlsx.core.min.js"></script>
<script type="text/javascript" src="<c:url value="/"/>jqwidgets/jqxkanban.js"></script>
<script src="<c:url value="/"/>js/dxpKanbanView.js"></script>
<script src="https://polyfill.io/v3/polyfill.min.js?features=default"></script>
<script src="https://maps.googleapis.com/maps/api/js?key=AIzaSyBktbrEU63oeqNZEbJr7P77CoqKXvj0T0w&callback=initMap&v=weekly"></script>
<script src="https://fastly.jsdelivr.net/npm/echarts-simple-transform/dist/ecSimpleTransform.min.js"></script>
<script src="https://fastly.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
<script src="<c:url value="/"/>js/spectrum.js"></script>
<script src="<c:url value="/"/>js/ea/voteAnalysis.js"></script> 
<script src="<c:url value="/"/>js/ea/censusAnalysis.js"></script>
<script src="<c:url value="/"/>js/ea/arcgisMap.js"></script>
<script src="<c:url value="/"/>js/ea/eaMap.js"></script>

<script src="<c:url value="/"/>js/etl/etlDXP.js"></script>
<script src="<c:url value="/"/>js/etl/etlComponents.js"></script>
<script src="<c:url value="/"/>js/etl/etlConnections.js"></script>
<script src="<c:url value="/"/>js/etl/dataModeller.js"></script>
<script src="<c:url value="/"/>js/etl/Filesaver.js"></script>
<script src="<c:url value="/"/>js/etl/jszip.js"></script>
<script src="<c:url value="/"/>js/etl/xlsx.min.js"></script>
<script src="<c:url value="/"/>js/etl/xlsx.full.min.js"></script>
<script src="<c:url value="/"/>js/jquery.panzoom.min.js"></script>


<script src="https://cdnjs.cloudflare.com/ajax/libs/bootstrap-select/1.13.1/js/bootstrap-select.min.js"></script>

<script src="<c:url value="/"/>js/daterangepicker/moment.min.js"></script>
<script src="<c:url value="/"/>js/daterangepicker/daterangepicker.js"></script>

<script src="<c:url value="/"/>js/wavesurfer.js"></script>
<script src="<c:url value="/"/>js/wavesurfer.minimap.js"></script>
<script src="<c:url value="/"/>js/wavesurfer.regions.js"></script>

<script type="text/javascript" src="<c:url value="/"/>js/intro.min.js"></script><!-- introJs plugin JS -->
<script type="text/javascript" src="<c:url value="/"/>js/homeIntro.js"></script>

<script src="<c:url value="/"/>js/mobiscroll.jquery.min.js"></script>
<script type="text/javascript" src="https://www.gartner.com/reviews/public/Widget/js/widget.js"></script>


<script type="text/javascript" src="<c:url value="/"/>js/jquery.keyboard.js"></script>
<script type="text/javascript" src="<c:url value="/"/>js/jquery.mousewheel.js"></script>
<script type="text/javascript" src="<c:url value="/"/>js/jquery.keyboard.extension-typing.js"></script>
<%-- <script type="text/javascript" src="<c:url value="/"/>js/jquery-ui.js"></script> --%>
<script type="text/javascript" src="<c:url value="/"/>js/keyboard-layouts-microsoft.min.js"></script>
<script type="text/javascript" src="<c:url value="/"/>js/typed.umd.js"></script>
<script src="<c:url value="/"/>js/AIOperations.js"></script>
<script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
<script src="https://cdn.rawgit.com/harvesthq/chosen/gh-pages/chosen.jquery.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/annyang/2.6.1/annyang.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/html2pdf.js/0.9.2/html2pdf.bundle.min.js"></script>







<script>
    google.charts.load('current', {packages: ['corechart', 'annotationchart',
            'calendar', 'gantt', 'gauge', 'map', 'orgchart', 'sankey', 'timeline', 'treemap',
            'wordtree', 'bar', 'line', 'scatter', 'geochart']});

</script>