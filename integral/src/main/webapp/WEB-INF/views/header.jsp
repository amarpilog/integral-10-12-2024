<%-- 
    Document   : header
Created on : 19 Nov, 2021, 10:50:50 AM
    Author     : PiLog
--%>

<%@page contentType="text/html" pageEncoding="UTF-8"%>
<%@ taglib prefix="c"   uri="http://java.sun.com/jsp/jstl/core" %>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt" %>
<%@ taglib prefix="fn"  uri="http://java.sun.com/jsp/jstl/functions" %>
<%@ taglib prefix="sec" uri="http://www.springframework.org/security/tags" %>
<!DOCTYPE html>
<style>
    .se-pre-con {
        position: fixed;
        left: 0px;
        top: 0px;
        width: 100%;
        height: 100%;
        z-index: 9999;
        background: url(images/Loader.gif) center no-repeat #f8f9f9;
        background-size: 90px 82px;
    }

</style> 
${mainHeader}
<%-- <div class='container-fluid smartBILoginMain'> 
    <div class="row smartBIrow">
        <div class="col-12">
            <marquee  direction="left" >
              <img src="images/Knowledge.png" style='width:27px;margin-top:-13px;'/> Do you want to visualize your data for better decisions?        Do you want to know different types of visualizations?            
            Do you want Artificial Intelligence insights on your data?</marquee>
        </div>
        <div class="col-lg-8">
            <div id="carouselExampleIndicators" class="carousel slide" data-ride="carousel" style="margin-top:52px;">
                <ol class="carousel-indicators">
                    <li data-target="#carouselExampleIndicators" data-slide-to="0" class="active"></li>
                    <li data-target="#carouselExampleIndicators" data-slide-to="1"></li>
                    <li data-target="#carouselExampleIndicators" data-slide-to="2"></li>
                </ol>
                <div class="carousel-inner">
                    <div class="carousel-item active">
                        <img class="d-block w-100" src="images/slide1.jpg" alt="First slide">
                    </div>
                    <div class="carousel-item">
                        <img class="d-block w-100" src="images/slide2.jpg" alt="Second slide">
                    </div>
                    <div class="carousel-item">
                        <img class="d-block w-100" src="images/slide3.png" alt="Third slide">
                    </div>
                    <div class="carousel-item">
                        <img class="d-block w-100" src="images/slide4.png" alt="Third slide">
                    </div>
                </div>
                <a class="carousel-control-prev" href="#carouselExampleIndicators" role="button" data-slide="prev">
                    <span class="carousel-control-prev-icon" aria-hidden="true"></span>
                    <span class="sr-only">Previous</span>
                </a>
                <a class="carousel-control-next" href="#carouselExampleIndicators" role="button" data-slide="next">
                    <span class="carousel-control-next-icon" aria-hidden="true"></span>
                    <span class="sr-only">Next</span>
                </a>
            </div>
        </div>  
        <div class="col-lg-4">
            <div class="wrapper">
                <div class="logo">
                    <img src="images/logo_red.png" alt="" />   
                </div>
                <div class="visionLoginpageInner" id='visionLoginpageInner'> 
                    <span id='loginError' style='color:red;'></span>
                    <div class="form-field d-flex align-items-center">
                        <span class="far fa-user"></span>

                     <input type="text" name="rsUsername" id="rsUsername" placeholder="Username" autocomplete="off" onkeypress="showorHideError('username')">       
                    </div>
                    <span id='passwordError' style='color:red;'></span>
                    <div class="form-field d-flex align-items-center showorHidePassword" style='display:none !important'>
                        <span class="fas fa-key"></span>
                        <input type="password" name="rsPassword" id="rsPassword" placeholder= "Password" data-man="M" required="" onchange="showorHideError('password')">                    
                        <input type="hidden" name="rsPasswordHid" id="rsPasswordHid" >
                        <input type="hidden" name="language" id="language" value="en_US" >
                    </div>
                      <div id="verifyLoginSecurity" class="loginSecurity">   
                    </div>
                    <button class="btn btn-primary btn-md btn-block waves-effect text-center m-b-20" onclick="showNextPassword()" id="showPassword">Next</button>         
                    <button class="btn btn-primary btn-md btn-block waves-effect text-center m-b-20" onclick="showorHideError('password','P')" id="processLoginID2" style='display:none;'>Sign In</button>        

                </div>
                <div class="text-center fs-6">
                    <div class='signupClass' style='margin-top: 12px;'><a href="<c:url value="/"/>cloudRegistrationForm" class='signupRegisterClass'><img src="images/SignUp.png" style='width:12px;margin-top: -3px;' alt=""/>&nbsp;Register</a>&nbsp;&nbsp;&nbsp;<a href="#"><img src="images/forgotPass.png" style='width:12px;margin-top: -3px;' alt=""/>&nbsp;Forgot password?</a></div>
                </div>
            </div>
        </div>
    </div>
</div>  --%>
<!-- Modal -->

<div id="Loader" class="loaderWait" style="display: none">
    <img src="images/Loader.gif" />
</div>
<input type="hidden" name="afterUserFlag" id="afterUserFlag" value="N"/>

<!-- Modal -->


