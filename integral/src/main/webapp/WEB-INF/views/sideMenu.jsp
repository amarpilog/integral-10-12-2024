<%-- 
    Document   : sideMenu
    Created on : 19 Nov, 2021, 11:37:12 AM
    Author     : PiLog
--%>

<%@page contentType="text/html" pageEncoding="UTF-8"%>
<%@ taglib prefix="c"   uri="http://java.sun.com/jsp/jstl/core" %>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt" %>
<%@ taglib prefix="fn"  uri="http://java.sun.com/jsp/jstl/functions" %>
<%@ taglib prefix="sec" uri="http://www.springframework.org/security/tags" %>
<!DOCTYPE html>

        <div class="dxpSideMenuClass">
            ${sideMenuStr} 
        </div>
<!-- [ side navigation menu ] end -->