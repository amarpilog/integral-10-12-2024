<%-- 
    Document   : footer
    Created on : 19 Nov, 2021, 1:58:23 PM
    Author     : Onkar
--%>

<%@page contentType="text/html" pageEncoding="UTF-8"%>
<%@ taglib prefix="c"   uri="http://java.sun.com/jsp/jstl/core" %>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt" %>
<%@ taglib prefix="fn"  uri="http://java.sun.com/jsp/jstl/functions" %>
<%@ taglib prefix="sec" uri="http://www.springframework.org/security/tags" %>
<!DOCTYPE html>
<div class="footer">
    <div class="container">
        <div class="row">
            <div class="col-xl-6 col-md-6 col-sm-6 col-12">  
                <ul class="footer-social-icons">
                    <li>
                        <a href="https://www.linkedin.com/company/89702458/admin/" title="Linked In" target="_blank" class="linkedin_Icon"><i class="fa fa-linkedin" aria-hidden="true"></i></a>
                    </li>
                    <li>
                        <a href="https://www.facebook.com/profile.php?id=100087005881195" class='facebook_Icon' title="Facebook" target="_blank"><i class="fa fa-facebook" aria-hidden="true"></i></a>
                    </li>
                    <li class="header-notification">
                        <a href="https://www.instagram.com/integralanalytics/" title="Instagram" target="_blank" class="insta_Icon"><i class="fa fa-instagram" aria-hidden="true"></i></a>
                    </li>
                    <li class="header-notification">
                        <a href="https://twitter.com/integral_ana" title="Twitter" target="_blank" class="twitter_icon"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><path d="M389.2 48h70.6L305.6 224.2 487 464H345L233.7 318.6 106.5 464H35.8L200.7 275.5 26.8 48H172.4L272.9 180.9 389.2 48zM364.4 421.8h39.1L151.1 88h-42L364.4 421.8z"></path></svg></a>
                    </li>
                    <li class="header-notification">
                        <a href="https://www.youtube.com/channel/UC0bkzl0wBcx1r9qtqgOzt6Q" title="You Tube" target="_blank" class="youtube_Icon"><i class="fa fa-youtube-play" aria-hidden="true"></i></a>
                    </li>
                </ul>
            </div>
            <div class="col-xl-6 col-md-6 col-sm-6 col-12 text-right">
                <fmt:formatDate var="currentYear" value="<%=new java.util.Date()%>" pattern="yyyy" />
                <p>Copyright &copy; ${currentYear} Smart Integraphics</p>
            </div>
        </div>
    </div>
</div>
            <script>
         $(document).ready(function () {
             var screenHeight = screen.height;
             var breadCrumbHeight = $("#breadCrumbDiv").height();
             var headerHeight = $(".pcoded-header").height();
             var footerHeight = $(".footer").height();
     //        console.log("screenHeight:::" + screenHeight);
     //        console.log("breadCrumbHeight:::" + breadCrumbHeight);
     //        console.log("headerHeight:::" + headerHeight);
     //        console.log("footerHeight:::" + footerHeight);
             var menuHeight = parseInt(screenHeight) - (parseInt(breadCrumbHeight) + parseInt(headerHeight) + parseInt(footerHeight));
     //        console.log("menuHeight:::" + menuHeight);
             menuHeight = parseInt(menuHeight)-150;
     //           console.log("menuHeight:::" + menuHeight);
             $(".pcoded-inner-content").css("height",menuHeight + "px");
         });
            </script>