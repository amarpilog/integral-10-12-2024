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
<%-- <%@ taglib prefix="tags" tagdir="/WEB-INF/tags" %> --%>
<!DOCTYPE html>
<html>
    <head>
        <%@include file="commonfiles.jsp" %>  
        <link rel="stylesheet" href="css\leangovernance.css" type="text/css" />  
        <link rel="stylesheet" href="css\countrySelect.css" type="text/css" />  
        <script src="https://checkout.razorpay.com/v1/checkout.js"></script>
        <script src="<c:url value="/"/>js/leangovernance.js"></script>
        <script src="<c:url value="/"/>js/countrySelect.js"></script>

        <title>PiLog SaaS Solution Subscription</title> 
    </head>
    <body class="signupBody">      
        <div id='scbscriptionid' class='scbscriptionClass ${class}'>
            ${result}
        </div>
        <div id="Loader" class="loaderwait" style="display:none;">
            <div id="Layer1" >PiLog</div>
            <div class="loadermain" >
                <div class="loader-ring">
                    <img src="images/PiLog_Gif.gif" class="themeModeDark">
                </div>
            </div>
        </div>
        <!--        <div id='demoevent' class='demoeventclass'>
                  <button id ="addToCartBtnId" type="checkOut" class="btn btn-primary" data-toggle="modal" onclick="createIDXPCalendarEventForm('CRM_CALENDAR_EVENTS','add')">Demo Event</button>
                </div>-->
        <div id="dialog"></div> 
        <div class="showPlanInfoDescData" id="showPlanInfoDescData"></div>
        <input type='hidden' value='' name='formPaymentInfo' id='formPaymentInfo'>
        <input type='hidden' value='' name='requestId' id='requestId'>

        <script>
            $(document).ready(function () {

                $("#scbscriptionid").append("<div id='headertittleId' class='headertittleclass'>");
//                $("#headertittleId").append("<div id='companyid' class='companyclass'>");
//                $("#headertittleId").append("<div id='subscriptiontittleId' class='subscriptiontittleclass'>");
//               var  tittle = ${tittle};
                $("#subscriptiontittleId").append("<h3>Basic<h3>");
                $("#companyid").append("<img src='images/PiLog_Logo_New.png'  class='imageLogoClass visionVisualChartBoxSelected'>");            
               // var data = ["PRODUCT", "SERVICE", "CUSTOMER", "VENDOR", "ASSET"];
               // $("#jqxdomain").jqxDropDownList({source: data, placeHolder: "Choose Domain", width: '100%', height: '29px', dropDownHeight: 120, checkboxes: true});
                //var erpdata = ["SAP ECC", "SAP S4 HANA", "Oracle", "Oracle EBS", "D365", "Others"];
               // $("#jqxerp").jqxDropDownList({source: erpdata, placeHolder: "Choose ERP", width: '100%', height: '29px', dropDownHeight: 120, checkboxes: true});
                /* $("#jqxdomain").on('checkChange', function (event) {
                    var val = $("#jqxdomain").val();
                    var item = event.args.item;
                    var checked = item.checked;
                    if (checked == true) {
                        checkChangeBasicPlan(val, 'jqxdomain', event);
                    }
                });
                $("#jqxerp").on('checkChange', function (event) {
                    var val = $("#jqxdomain").val();
                    var item = event.args.item;
                    var checked = item.checked;
                    if (checked == true) {
                        checkChangeBasicPlan(val, 'jqxerp', event);
                    }
                }); */
                
                      $("#CUSTOMER_COUNTRY").countrySelect({
				// defaultCountry: "jp",
				// onlyCountries: ['us', 'gb', 'ch', 'ca', 'do'],
				// responsiveDropdown: true,
				preferredCountries: ['defult_flag', 'gb', 'us']
			});

            });
          
        </script>


    </body>

</html>