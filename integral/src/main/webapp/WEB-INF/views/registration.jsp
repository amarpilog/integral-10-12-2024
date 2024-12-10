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
        <link rel="stylesheet" href="https://pro.fontawesome.com/releases/v5.10.0/css/all.css" />
        <link rel="stylesheet" type="text/css" href="<c:url value="/"/>css/dxpsignup.css"> 
        <title>Integral Data Analytics</title>
        <link rel="icon" href="images/PiLog-Cloud-Logo-Large.png">
        <meta name="description" content="PiLog Cloud Platform is a self-service solution for all Data & Analytics services, 
              It provides process-driven and methodology-based lean data harmonization, governance & Analytics for 
              multiple domains, a cloud-based application, providing Master data as a service, saas, business value, 
              learning, and consulting services.">
        <style>
            .carousel-inner img {
                width: 100%;
                height: 100%;
            }
        </style>
    </head>
    <body class="signupBody">
<div class="page-content container-fluid">
            <div class="row">
                <div class="col-12">
                    <div class="wizard-v1-content">
                        <div class="wizard-form">
                            <form class="form-register" id="form-register" action="#" method="post">
                                <div class="signupLogo">
                                    <img src="images/IntegralGifLogo.gif" class="pilogcloudLogo" style='width:90px;' alt="pilogcloud"> 
                                    <a class="dxpBackTohomePage" href="homePage"><img src="images/Home-Iocn.svg" class="home_img" title="Back to home" alt="BACK TO HOME"></a>
                                </div>
                                <div id="form-total">
                                    <!-- SECTION 1 -->
                                    <h2>
                                        <span class="step-icon"><i class="ti-lock"></i></span>
                                        <!--<span class="step-number">Step 1</span>-->
                                        <span class="step-text">Account Setup</span>
                                    </h2>
                                    <section>
                                        <div class="Maintitle">
                                            <h5 class="titleSignup">Create Your Digital Account</h5>
                                        </div>
                                        <div class="inner">
                                            <div class="form-row ">
                                                <div class="form-holder col-12 otp_textbox_div">
                                                    <!--/*21322*/-->
                                                    <input type="email" placeholder="Email" class="form-control" id="email_id" onkeyup="verifyAddress()" name="email"  autocomplete="off" required pattern="[^@]+@[^@]+.[a-zA-Z]{2,6}" readonly onfocus="this.removeAttribute('readonly');">
                                                    <a class="verify_mail verify_account_position" id="first_verify_acc" onclick="sendOtp()">Verify Account</a>
                                                    <p class="email_status"></p>    
                                                    <div class="cooldown_flex">
                                                        <input type="text" placeholder="OTP" class="otp_textfield" id="otp_textfield" name="OTP" >
                                                        <input type="text" id="timer" class="cooldown_textfiled" name="cd" readonly="true">
                                                        <a class="verify_mail" id="again_otp" onclick="sendOtp()">Send OTP Again</a>                                            
                                                        <p class="otp_status"></p>
                                                    </div>
                                                    <a class="btn btn-success" id="verifyOtpBtn" onclick="verify_email_otp()">Verify</a>
                                                    <!--/*21322*/-->

                                                </div>
                                            </div>
                                        </div>
                                    </section>
                                    <!-- SECTION 2 -->

                                    <!-- SECTION 3 -->
                                    <h2>
                                        <span class="step-icon"><i class="ti-user"></i></span>
                                        <!--<span class="step-number">Step 2</span>-->
                                        <span class="step-text">Create Password</span>
                                    </h2>
                                    <section>
                                        <div class="Maintitle">
                                            <h5 class="titleSignup">Create Your Digital Account</h5>
                                        </div>
                                        <div class="inner">
                                            <div class="form-row">
                                                <div class="form-holder col-12">
                                                    ${roleTypes}
                                                </div>
                                                <div class="form-holder col-12">
                                                    <input type="text" name="rsUsername" id="rsUsername" placeholder="Username" data-man="M" class="form-control" required="" aria-required="true" onkeyup="checkUser(id, event)">
                                                </div> 
                                            </div>
                                            <div class="form-row">

                                                <div class="form-holder col-12">
                                                    <input type="password" placeholder="Password" class="form-control" id="password"  name="password"  >
                                                    <small style="display: none;"></small>
                                                    <div class="passwordSuggestion">
                                                        <div class="gridd">
                                                            <span id="8char" class="fas fa-times"></span>
                                                            <span class="text-body">min length 8</span>
                                                            <span id="caps" class="fas fa-times"></span>
                                                            <span class="text-body">Uppercase</span>
                                                            <span id="spchar" class="fas fa-times"></span>
                                                            <span class="text-body">Special Character</span>
                                                            <span id="int" class="fas fa-times"></span>
                                                            <span class="text-body">number</span>
                                                        </div>
                                                    </div>
                                                </div>
                                            </div>

                                            <div class="form-row">
                                                <div class="form-holder col-12">
                                                    <input type="password" placeholder="Confirm Password" class="form-control" id="confirm_password" name="confirm_password" required>
                                                </div>
                                            </div>
                                        </div>
                                        <div class="row m-t-10 text-left">
                                            <div class="col-md-12">
                                                <div class="checkbox-fade fade-in-primary optionTC">
                                                    <label>
                                                        <input type="checkbox" value="" id ="Tnc">
                                                        <span class="cr"><i class="cr-icon icofont icofont-ui-check txt-primary"></i></span>
                                                        <span class="text-inverse">I read and accept <a href="#">Terms &amp; Conditions.</a></span>
                                                    </label>
                                                </div>
                                            </div>
                                        </div>
                                </div>
                                </section>
                        </div>
                        </form>
                        <div id="Loader" class="loaderWait" style="display: none">
                            <img src="images/Loader.gif" />
                        </div>
                    </div>
                </div>
            </div>


            <div id="myModal" class="modal fade" role="dialog">
                <div class="modal-dialog">

                    <!-- Modal content-->
                    <div class="modal-content">
                        <div class="modal-header" style="background-color: #40875e; color: #fff;">
                            <h6 class="modal-title">Message</h6>
                            <!--16322<button type="button" class="close" data-dismiss="modal">&times;</button>-->
                        </div>
                        <div class="modal-body">
                            <p class="registerMsg"></p>
                        </div>
                        <button type="button" class="close" data-dismiss="modal">&times;</button>
                    </div>
                    <div class="modal-body">
                        <p class="registerMsg"></p>
                    </div>
                </div>
            </div>
        </div>
</body>

</html>

