<%-- 
    Document   : Home
    Created on : Dec 17, 2020, 5:53:37 PM
    Author     : Devint01
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
        <%@include file="commonfiles.jsp" %>
        <link rel="stylesheet" href="https://js.arcgis.com/4.24/esri/themes/light/main.css"/>
        <script src="https://js.arcgis.com/4.24/"></script>
        <title>Political Science Insights | intelliSense</title>
        <link rel="icon" href="images/intellifavicon.svg">
        <meta name="description" content="PiLog Cloud Platform is a self-service solution for all Data & Analytics services, 
              It provides process-driven and methodology-based lean data harmonization, governance & Analytics for 
              multiple domains, a cloud-based application, providing Master data as a service, saas, business value, 
              learning, and consulting services.">
        <style>
            .carousel-inner img {
                width: 100%;
                height: 200px;
            }
        </style>
    </head>
    <body>
        <c:choose>
            <c:when test="${not empty sessionScope.ssUsername}"> 
                <%@include file="loginHeader.jsp" %>
            </c:when>
            <c:otherwise>
                <%@ include file="header.jsp" %>
            </c:otherwise>
        </c:choose>
        <div class="dxpPageWrapper dxpTheme d-flex align-items-stretch toggled">
            <%@include file="sideMenu.jsp" %>   
            <div class="dxpPageContent">  
                <div class="page-body" id="pageBody">
                    <!--${menuStr}-->  
                    <div class="page-body-content" id="pageBodyContent">
                    </div>
                    <div id="Loader" class="loaderwait" style="display:none;">
                        <div id="Layer1" >IS</div>
                        <div class="loadermain" >
                            <div class="loader-ring">
                                <img src="images/Loader.gif" class="themeModeDark">
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div><!-- DXP New Theme Body Layout -->
        <%@include file="footer.jsp" %>
        <div class="dataDxpSplitterValue" id="dataDxpSplitterValue"></div>
        <input type="hidden" name="helpMenuThemesURL" id="helpMenuThemesURL" value="${ssThemesURL}"/>
        <input type="hidden" name="rsUserName" id="rsUserName" value="${ssUserName}"/>  
        <input type="hidden" name="searchedValue" id="searchedValue" value=""/> 
        <input type="hidden" name="currentTypedValue" id="currentTypedValue" value=""/>  
        <input type="hidden" name="showCaseCardType" id="showCaseCardType" value=""/>  
        <input type="hidden" name="compareType" id="compareType" value=""/>  
        <input type="hidden" name="currentSocialMediaFlag" id="currentSocialMediaFlag" value=""/>  
        <input type="hidden" name="districtSearchListBox" id="districtSearchListBox" value=""/>  
        <input type="hidden" name="constituencySearchListBox" id="constituencySearchListBox" value=""/>  
        <input type="hidden" name="candidateSearchListBox" id="candidateSearchListBox" value=""/>  
        <input type="hidden" name="partySearchListBox" id="partySearchListBox" value=""/>  
        <script src="https://polyfill.io/v3/polyfill.min.js?features=default"></script>
        <script src="https://maps.googleapis.com/maps/api/js?key=AIzaSyBktbrEU63oeqNZEbJr7P77CoqKXvj0T0w&callback=initMap&v=weekly"></script>
        <script>
            $(document).ready(function () {
                var rsUserName = $("#rsUserName").val();
                if (rsUserName != null && rsUserName != '' && rsUserName != 'null' && rsUserName != undefined) {
                    socialMedialShowCaseCards();
                }
                var searchCandidate = '${searchCandidate}';
                if (searchCandidate != null && searchCandidate != '' && searchCandidate != 'null') {
                    getProfileUserNames(searchCandidate);
                } else {
                    $('#mainintelliSenseSelectBoxId').hide();
                }
            });
        </script>
    </body>
</html>

