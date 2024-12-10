<%-- 
    Document   : loginHeader
    Created on : 19 Nov, 2021, 11:19:03 AM
    Author     : PiLog
--%>

<%@page contentType="text/html" pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions"%>
<%@ taglib prefix="sec"
	uri="http://www.springframework.org/security/tags"%>

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

input::-webkit-input-placeholder, textarea::-webkit-input-placeholder {
	color: #000;
}
</style>
<div class="se-pre-con"></div>


<div id="modalDailogDiv"></div>
<div id="modalDailogDiv1"></div>
<div id="modalInfoDailogDiv"></div>

<div class="modal fade signOutpopUp " id="signOut" role="dialog">
	<div class="backGroundOpacityModal"></div>
	<div class="modal-dialog opacity-animate3">

		<!-- Modal content-->
		<div class="modal-content">

			<div class="modal-header">
				<div class="signoutLogo">
					<!--<img src="images/logo_red.png" class="pilogcloudLogo themeModeDark" alt="pilogcloud">-->
				</div>
				<div class="signouttext">
					<h5 class="modal-title text-center">Sign Out</h5>
				</div>
			</div>
			<div class="modal-body">
				<p>
					<span class="signOutWarningMsg"><i
						class="fa fa-exclamation-triangle" aria-hidden="true"></i></span>Unsaved
					data will be lost, do you want to logout?
				</p>
			</div>
			<div class="modal-footer">
				<button type="button" class="btn btn-primary" onclick="logout()">Yes</button>
				<button type="button" class="btn btn-primary" data-dismiss="modal">No</button>
			</div>
		</div>
	</div>
</div>

<div class="dxpLoginHeader">

	<nav class="navbar navbar-expand-md  navbar-dark">
		<ul class="leftNavList">
			<li><img id="show-sidebar" class="show-sidebar themeModeDark"
				src="images/PiLogDots.png" width="16px" alt=""
				onclick="homeSideMenu()" /></li>
			<li><a class="navbar-brand" href="<c:url value="/"/>homePage">
					<img src="images/logo_red.png" class="pilogcloudLogo themeModeDark"
					alt="Integral Data Analytics" title='Integral Data Analytics'
					width='80px'>
			</a></li>

		</ul>
		<button class="navbar-toggler" type="button" data-toggle="collapse"
			data-target="#collapsibleNavbar">
			<span class="navbar-toggler-icon"></span>
		</button>
		<div class="collapse navbar-collapse" id="collapsibleNavbar">

			<div class="globalSearchBar">
				<div class="searchMainWrap">
					<div class='innerSearchDiv' style="display: none;">
						<div class="selectDropDown">
							<div class="form-group" id="selectFilter">${selectTypes}</div>
						</div>
						<div class="backbutton">
							<img src="images/back-icon_blue.png" width="20px"
								onclick="clearTextSearch()" class="themeModeDark" />
						</div>
						<div class="searchbutton" id="rightsearchicon">
							<img src="images/search_blue1.png" class="themeModeDark"
								width="20px"
								onclick="smartTextSearch('SearchResult', 'SelectedValue', 'N')" />
						</div>
						<div class="filterDownArrowIcon" id="filterDownArrowIconID"
							style="display: none">
							<img src="images/filterdownarrowblue.png" class="themeModeDark"
								width="20px" />
						</div>
						<div class="form-group has-search"
							style='color: #00186a !important;'>
							<input type="text" class="form-control" placeholder="Search"
								value="" id="SearchResult" onkeyup="keySearch(event)">
							<div class="tooltipdiv" id="tooltipdiv"></div>
							<div class="searchResultsDiv" id="intellisense"></div>
						</div>
					</div>
					<div class="searchFeildToggleIcon" id="toggleSearchFeildDiv">
						<div onclick="toggleSearchDiv();" class='searchFeildIconandText'>
							<span class="searchIcon"><img
								src="images/search_blue1.png" width="30px;"></span><span
								class="searchText"><div>Search</div></span>
						</div>
						<div class='closeSearchFeild' onclick="togglecloseDiv();"
							style='display: none;'>
							<i class="fa fa-times" aria-hidden="true"></i>
						</div>
					</div>
				</div>
				<div class="DxpVisualizationbutton" id="DxpVisualizationbutton"
					style="display: none"></div>
			</div>
			<div class="rightIcons">
				<ul class="navbar-nav">
					<li class="settingsIconList" style='display: none'><img
						src="images/Settings_Icon.svg" id='settingheaderImage'
						width="20px" class="settingheaderImage themeModeDark"
						title="Advanced Search" onclick="advancedSearches(event)"> <span
						class="mobileTitle">Settings</span></li>
					<li class="header-notification">
						<div class="DXPlLanguageSelectionClass"
							id="DXPlLanguageSelectionId">
							<select id="form-control" class="languageSelectionBox"
								name="language" onchange="languageLogin(id)">
								<option value="en_US" selected="selected">En (US)</option>
								<!-- <option value="fr_FR">Fr (FR)</option>
						<option value="pt_BR">Pt (BR)</option>
						<option value="ru_RU">Ru (RU)</option>
						<option value="ro_RO">Ro (RO)</option>
						<option value="es_ES">es (ES)</option>
						<option value="no_NO">No (NO)</option>
						<option value="pt_EU">Pt (EU)</option>
						<option value="cs_EU">Cs (EU)</option>  
						<option value="ko_KR">Ko (KR)</option>
						<option value="zh_CN">zh (CN)</option>
						<option value="ja_JP">Ja (JP)</option>
						<option value="nl_NL">Nl (NL)</option>
						<option value="de_DE">de (DE)</option> -->
							</select>
						</div>
					</li>
					<!-- <li class='voiceIcon' onclick="getVoiceRply()"title='voice assistance'><img src='images/Mike-OutLine-Icon-01.png' width='22px' class='themeModeDark' /></li> -->
					<li class='weatherIcon' onclick="getWeatherDetails()"
						title='Weather Report'><img src='images/Sunrise.png'
						width='22px' class='themeModeDark' /></li>
					<li class='AILensToggleIcon' style='display: none'><label
						id='AILensSwitchLabelId' class='switch switch-flat'> <input
							class='switch-input both' type='checkbox' id='AILensSwitchId'
							checked /> <span class='switch-label' data-on='AI ON'
							data-off='AI OFF'></span> <span class='switch-handle'></span>
					</label></li>
					<li id='limitIconDivID' class='limitsIcon'
						onhover="openLimitsPopOver()"
						onclick="openSettingPannel('limitsIcon')"
						title='Subscription Details'><img
						src='images/Subscription Status-Icon-01.png' width='22px'
						class='themeModeDark' /></li>
					<c:choose>
						<c:when
							test="${not empty sessionScope.ssSubscriptionType && (sessionScope.ssSubscriptionType == 'Basic' || sessionScope.ssSubscriptionType == 'BASIC')}">
							<li class="toggleIcon" id="toggleintroID" onclick="toggleCheck()"><input
								type="checkbox" name="toggle" id="toggle"> <label
								for="toggle"></label></li>
						</c:when>
					</c:choose>

					<li class='calendarIcon' onclick="openSettingPannel('calendardiv')"
						title='My Day'><img src='images/calendarBlue.png'
						width='22px' class='themeModeDark' /></li>
					<li class='settingIcon' onclick="openSettingPannel('settingdiv')"
						title='Settings'><img src='images/Settings_Icon.svg'
						width='22px' class='themeModeDark' /></li>
					<li class='helpIcon' onclick="openSettingPannel('helpdiv')" title='Help'>
					<img src='images/Help-Icon.svg' width='22px' class='themeModeDark' /></li>

					<li class="userProfileIcon newUser"
						onclick="openSettingPannel('useraccdiv')"
						title="${sessionScope.ssUsername}"><img
						id="userMainProfileImage" src="${sessionScope.imageurl}"
						width='22px' class="profile-img userMainProfile"
						alt="User-Profile-Image" style="border-radius: 50%;"
						onmouseover="templeteMouseOver(id)"
						onmouseout="templeteMouseOut(id)"></li>
				</ul>


			</div>


		</div>
	</nav>
</div>

<!-- Modal -->
<input type="hidden" id="ssUsername" value="${sessionScope.ssUsername}" />
<input type="hidden" id="ssCloudV10URL"
	value="${sessionScope.ssCloudV10URL}" />
<input type="hidden" id="addToCartCount" value="${cartCount}" />
<form action="" id="navigationUrlForm" method="POST">
	<c:if test="true">
		<input type="hidden" name="${_csrf.parameterName}"
			value="${_csrf.token}" />
	</c:if>
</form>
<div id="Loader" class="loaderWait" style="display: none">
	<img src="images/Loader.gif" class="themeModeDark" />
</div>
<script>
	$(document).ready(function() {
		$(".se-pre-con").fadeOut("slow");
	});
	$(function() {
		$('[data-toggle="tooltip"]').tooltip()
	})
	/* function openNav() {
	    document.getElementById("mySidepanel").style.width = "190px";
	}

	function closeNav() {
	    document.getElementById("mySidepanel").style.width = "0";
	} */
</script>