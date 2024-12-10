<%-- 
    Document   : pageError
    Created on : 21 Dec, 2020, 5:08:28 PM
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
        <%--<tags:commonfiles/>--%>
        <%@include file="commonfiles.jsp" %>
        <title>PiLog Cloud Page Error</title>
        <link rel="icon" href="images/PiLog-Cloud-Logo-Large.png">
    </head>
    <body>
        <div id="pcoded" class="pcoded">
            <div class="pcoded-overlay-box"></div>
            <div class="pcoded-container navbar-wrapper">
                <!-- Header -->
                <c:choose>
                    <c:when test="${not empty sessionScope.ssUsername}">
                        <%--<tags:loginHeader/>--%> 
                        <%@include file="loginHeader.jsp" %>
                    </c:when>
                    <c:otherwise>
                        <%--<tags:header/>--%>
                         <%@ include file="header.jsp" %>
                    </c:otherwise>
                </c:choose>
               
                <!-- / Header -->
                <div class="pcoded-main-container">
                    <div class="pcoded-wrapper">
                        <!-- [ Side navigation menu ] start -->
                        <%--<tags:sideMenu/>--%>
                         <%@ include file="sideMenu.jsp" %>
                        <!-- [ side navigation menu ] end -->
                        <div class="pcoded-content">
                            <!-- Bread crumb -->
                          
                            <!-- Bread crumb -->
                            <div class="pcoded-inner-content">
                                <div class="main-body">
                                    <div class="page-wrapper">
                                        <div class="page-body">
                                            <div class="row align-items-center error-page">
                                                <div class="col-lg-12 col-xl-12">
                                                    <img src="images/404-Error-Icon-01.svg" alt="" width="250px">
                                                    <h1 class="error-font">${statusCode}</h1>
                                                    <h3>${errorMesg}</h3>
                                                    <p>${detailMessage}</p>
                                                </div>
                                            </div>
                                          

                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                <!-- footer -->
                <%--<tags:footer/>--%>
                <%@include file="footer.jsp" %>
            </div>
        </div>
    </body>
</html>
