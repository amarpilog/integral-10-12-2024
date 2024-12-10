<%-- 
    Document   : registration
    Created on : Dec 18, 2020, 11:51:08 AM
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

        <title>PiLog Cloud Reset Password</title>
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
                    <div class="pcoded-content">
                        <!-- Bread crumb -->
                        <%--<tags:breadCrumb/>--%>
                         <%@include file="breadCrumb.jsp" %>
        <div class="page-content">
           <section class="login-block">
        <!-- Container-fluid starts -->
        <div class="container-fluid">
            <div class="row">
                <div class="col-sm-12">
                    <!-- Authentication card start -->
                    <form class="md-float-material form-material" id="ValidateResetPassword">
                        <div class="text-center">
                            <img src="https://www.piloggroup.com/img/header/logo-header.png" alt="">
                        </div>
                        <div class="auth-box card">
                            <div class="card-block">
                                <div class="row m-b-20">
                                    <div class="col-md-12">
                                        <h3 class="text-center" style="color: #40875e;">Change Password</h3>
                                    </div>
                                </div>
                                <div class="form-group form-primary">
                                    <input type="password" name="old_password" id="old-password" class="form-control"
                                        required>
                                    
                                    <label class="float-label">Old Password<sup style="color: red;">*</sup></label>
                                </div>
                                <div class="form-group form-primary">
                                    <input type="password" name="new_password" id="new-password" class="form-control"
                                        required>
                                    
                                    <label class="float-label">New Password<sup style="color: red;">*</sup></label>
                                </div>
                                <div class="form-group form-primary">
                                    <input type="password" name="confirm_password" id="confirm-password"
                                        class="form-control" required>
                                    
                                    <label class="float-label">Confirm Password<sup style="color: red;">*</sup></label>
                                </div>
                                <div class="row">
                                    <div class="col-md-12">
                                        <button type="button"
                                            class="btn btn-primary btn-md btn-block waves-effect text-center m-b-20" value="Update Password" onclick="changepassword()">Update
                                            Password</button>
                                    </div>
                                </div>
                                <p class="f-w-600 text-right">Back to <a href="homePage"
                                        style="color: #0b4a99;">Login.</a></p>
                            </div>
                        </div>
                    </form>
                    <!-- Authentication card end -->
                </div>
                <!-- end of col-sm-12 -->
            </div>
            <!-- end of row -->
        </div>
        <!-- end of container-fluid -->
    </section>
        </div>
    </div>
                </div>
            </div>
            <!-- footer -->
            <%--<tags:footer/>--%>
            <%@include file="footer.jsp" %>
        </div>
        <input type="hidden" id="selectedGrid" value=""/>
        <input type="hidden" id="gridString" value="${gridString}"/>
        <input type="hidden" value="${gridIdStr}" id="gridIdStr"/>
        <input type="hidden" value="${compTypeStr}" id="compTypeStr"/>
        <input type="hidden" value="${gridIdStr}" id="totalGridIdStr"/>
        <input type="hidden" value="${tabid}" id="tabid"/>
        <input type="hidden" value="${highLevelMenu}" id="highLevelMenu"/>
        <input type="hidden" value="" id="tabChartsId"/>
        <input type="hidden" value="" id="tabComponentId"/>
        <input type="hidden" value="" id="startIndex"/>
        <input type="hidden" value="" id="endIndex"/>
        <input type="hidden" value="" id="pageSize"/>
        <input type="hidden" value="" id="totalCount"/>
        <input type="hidden" value="" id="scrollFlag"/>
    </div>
        <div id="passWordErrorShowModal" class="passWordErrorShowModal"></div>
    </body>

</html>
