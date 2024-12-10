<%-- 
    Document   : Home
    Created on : Dec 17, 2020, 5:53:37 PM
    Author     : Devint01
--%>
<%@page contentType="text/html" pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions"%>
<%@ taglib prefix="sec"
	uri="http://www.springframework.org/security/tags"%>
<!DOCTYPE html>
<html>
<head>
<c:choose>
	<c:when test="${not empty sessionScope.ssUsername}">
		<%--<%@include file="loginHeader.jsp" %>--%>
		<%@include file="commonfiles.jsp"%>
	</c:when>
	<c:otherwise>
		<%@ include file="beforeLoginFiles.jsp"%>
	</c:otherwise>
</c:choose>
<link rel="stylesheet"
	href="https://js.arcgis.com/4.24/esri/themes/light/main.css" />
<link rel="stylesheet" href="./chatbot-ui.css">
<script src="https://js.arcgis.com/4.24/"></script>
<title>Integral Data Analytics | Integral</title>
<link rel="icon" href="images/intellifavicon.svg">
<meta name="description"
	content="PiLog Cloud Platform is a self-service solution for all Data & Analytics services, 
              It provides process-driven and methodology-based lean data harmonization, governance & Analytics for 
              multiple domains, a cloud-based application, providing Master data as a service, saas, business value, 
              learning, and consulting services.">
<style>
/* .carousel-inner img {
	width: 100%;
	height: 80vh;
} */
.head1 {
	font-size: 40px;
	color: #009900;
	font-weight: bold;
}

.head2 {
	font-size: 17px;
	margin-left: 10px;
	margin-bottom: 15px;
}

body {
	margin: 0 auto;
	background-position: center;
	background-size: contain;
}

.menu {
	position: sticky;
	top: 0;
	background-color: #009900;
	padding: 10px 0px 10px 0px;
	color: white;
	margin: 0 auto;
	overflow: hidden;
}

.menu a {
	float: left;
	color: white;
	text-align: center;
	padding: 14px 16px;
	text-decoration: none;
	font-size: 20px;
}

.menu-log {
	right: auto;
	float: right;
}

/* footer {
	width: 100%;
	bottom: 0px;
	background-color: #000;
	color: #fff;
	position: absolute;
	padding-top: 20px;
	padding-bottom: 50px;
	text-align: center;
	font-size: 30px;
	font-weight: bold;
} */
.body_sec {
	margin-left: 20px;
}
</style>
</head>
<body>

	<c:choose>
		<c:when test="${not empty sessionScope.ssUsername}">
			<%@include file="loginHeader.jsp"%>
		</c:when>
		<c:otherwise>
			<%@include file="header.jsp"%>
		</c:otherwise>
	</c:choose>
	<div class="dxpPageWrapper dxpTheme d-flex align-items-stretch toggled">
		<c:choose>
			<c:when test="${not empty sessionScope.ssUsername}">
				<%@include file="sideMenu.jsp"%>
			</c:when>
			<c:otherwise>
			</c:otherwise>
		</c:choose>

		<div class="dxpPageContent">
			<div class="page-body" id="pageBody">
				<div class="page-body-content" id="pageBodyContent"></div>
				<div id="Loader" style="display: none;">
					<div class="loadermain">
						<div class="loader-ring">
							<img src="images/Loader.gif" class="themeModeDark loaderWait">
						</div>
					</div>
				</div>
				<c:choose>
					<c:when
						test="${not empty sessionScope.ssSubscriptionType && (sessionScope.ssSubscriptionType == 'Basic' || sessionScope.ssSubscriptionType == 'BASIC')}">
						<div class="hintImage" id="hintImageID" style='display: none'></div>
					</c:when>
				</c:choose>
			</div>
			<div class='scrollToTop' onclick='scrollToTop();'>
				<img src="images/top_arrow_icon.svg" width='20px' id='top_arrow'
					style='display: none;' />
			</div>
			<div class='scrollToBottom' onclick=' scrollToBottom();'>
				<img src="images/bottom_arrow_icon.svg" class='scrollToBottom'
					width='20px' id='bottom_arrow' style='display: none;' />
			</div>

			<div id="settingPannel" class="settingPannel">
				<div class="pannelTitle">
					<span class='settingsIcon' id='settingsIcon'></span> <span
						class='spanTitle' id="clickedTitle">Settings</span> <span
						class='spanCloseIcon' onclick="closesettingPannel()">×</span>
				</div>
				<div class="settingPannelInnerWrapper">
					<div class='listofSettingDiv' id='settingContentDiv'
						style='display: none;'>
						<ul>
							<li id="fontChangeSettingId" class="fontChangeIcon dropdown">
								<a class="dropdown-toggle" href="#" data-toggle="dropdown">
									<span class="listImage"><img id="fontChangeSettingImgId"
										src="images/font.png" style="width: 20px;" title="Font Type"
										class="fontChangeButton themeModeDark"> </span> <span
									id="fontChangeSettingTitleId" class="mobileTitle">Font
										Type</span>
							</a>
								<ul class="dropdown-menu">
									<li onclick="fontUpperCase('UpperCase')"><a tabindex="-1"
										href="#" id="upperCaseMenu">UpperCase</a></li>
									<li onclick="fontUpperCase('LowerCase')"><a tabindex="-1"
										href="#" id="LowerCaseMenu">LowerCase</a></li>
									<li onclick="fontUpperCase('Default')"><a tabindex="-1"
										href="#" id="capitaliseFontMenu">Default</a></li>

								</ul>
							</li>
							<li id="fontSizeSettingId" class="fontSizeIcon dropdown"><a
								class="dropdown-toggle" href="#" data-toggle="dropdown"> <span
									class="listImage"> <img id="fontSizeSettingImgId"
										src="images/font-size.png" style="width: 20px;"
										title="Font Size" class="fontSizeChangeButton themeModeDark">
								</span> <span id="fontSizeSettingTitleId" class="mobileTitle">Font
										Size</span>
							</a>
								<ul class="dropdown-menu">
									<li onclick="changeFontSize('Smaller')"><a tabindex="-1"
										href="#" id="SmallerFontSize">Smaller</a></li>
									<li onclick="changeFontSize('Medium')"><a tabindex="-1"
										href="#" id="MediumFontSize">Medium</a></li>
									<li onclick="changeFontSize('Large')"><a tabindex="-1"
										href="#" id="LargeFontSize">Large</a></li>
									<li onclick="changeFontSize('Default')"><a tabindex="-1"
										href="#" id="DefaultFontSize">Default</a></li>

								</ul></li>
							<li class="themeChangeIcon" onclick="changeTheme()"><a
								class="" href="#"> <span class='listImage'> <img
										src="images/lightmode.png" title="Light Mode"
										class="themeModeClickButton" width="20px">
								</span> <span class="mobileTitle" id="themeChangeSettingTitleId">Dark
										Mode</span>
							</a></li>

							<li class='extendedViewIcon'
								onclick="javascript:toggleFullScreen()"><a href="#!"
								class="waves-effect waves-light"> <span class='listImage'>
										<img src="images/extendedview.png" class="themeModeDark"
										width="20px" id='IntelliSenseFs' title='View full screen'>
								</span> <span class="mobileTitle">View Full Screen</span>
							</a></li>

							<!-- <li class="languageChangeIcon" onclick="changeLanguage()"><a
								class="" href="#"> <span class='listImage'> <img
										src="images/languageSet.png" title="Light Mode"
										class="themeModeClickButton themeModeDark" width="20px">
								</span> <span class="mobileTitle">Language</span>
							</a></li> -->

							<!-- <li class="organizationIcon"><a class="" href="#"> <span
									class='listImage'> <img
										src="images/Organization_Vision_Icon.svg" title="DXP"
										class="profile-img themeModeDark" width="20px">
								</span> <span class="mobileTitle">Organization</span>
							</a></li>
							<li class="feedbackIcon"><a href="#"
								onclick="navigationMenuUrl"> <span class='listImage'>
										<img src="images/FeedBack_Icon.svg" title="Feedback"
										class="headerFeedback themeModeDark" width="20px">
								</span> <span class="mobileTitle">Feedback</span>
							</a></li> -->


							<li id="themesChangeSettingId" class="ThemesIcon"
								id="themesShowClass"><a class="" href="#"> <span
									class="listImage"> <img id="themesChangeSettingImgId"
										src="images/Themes.png" title="Themes"
										class="profile-img themeModeDark" width="20px">
								</span> <span id="themesChangeSettingTitleId" class="mobileTitle">Themes</span>
							</a>
								<div class="innerThemes">
									<ul>
										<li onclick="resetToDefault()">
											<div class="lightthemeColor defaultColor"></div>
										</li>
										<li onclick="applyTheme(this, 'colorAsBgTheme', '#69c2e6')">
											<div class="darkthemeColor primaryColor"></div>
										</li>
										<li onclick="applyTheme(this, 'colorAsBgTheme', '#707576')">
											<div class="lightthemeColor secondaryColor"></div>
										</li>
										<li onclick="applyTheme(this, 'colorAsBgTheme', '#4481b9')">
											<div class="darkthemeColor darkedColor"></div>
										</li>
										<li onclick="applyTheme(this, 'colorAsBgTheme', '#0c3c68')">
											<div class="darkthemeColor lightDarkColor"></div>
										</li>
										<li
											onclick="applyTheme(this, 'imageAsBgTheme', 'darkcoloredTheme')">
											<img src="images/home_theme2.jpg"
											class="lightthemeColors primarytheme">
										</li>

										<li
											onclick="applyTheme(this, 'imageAsBgTheme', 'darkcoloredTheme')">
											<img src="images/home_theme4.jpg"
											class="lightthemeColors basictheme">
										</li>
									</ul>
								</div></li>

							<!-- <li class="contactPreferencesIcon"><a class="" href="#">
									<span class='listImage'> <img src="images/SignUp.png"
										title="Contact Preferences" class="profile-img themeModeDark"
										width="20px">
								</span> <span class="mobileTitle">Contact Preferences</span>
							</a></li> -->
							<c:choose>
								<c:when
									test="${not empty sessionScope.orgnSubscriptionDefaultUser && sessionScope.orgnSubscriptionDefaultUser == 'Y'}">
									<li class="RegisterIcon"><a class="" href="#"
										onclick="showRegisterForm()"> <span class='listImage'>
												<img src="images/SignUp.png" title="Other"
												class="profile-img themeModeDark" width="20px">
										</span> <span class="mobileTitle">Register</span>
									</a></li>
								</c:when>
								<c:otherwise>
								</c:otherwise>
							</c:choose>

							<li class="passworIcon"><a class="" href="#"
								onclick="updatePassForm()"> <span class='listImage'>
										<img src="images/passwordSet.png" title="Password"
										class="profile-img themeModeDark" width="20px">
								</span> <span class="mobileTitle">Change Password</span>
							</a></li>
							<!-- <li class="aboutUsIcon"><a class="" href="#"> <span
									class='listImage'> <img src="images/AboutUsSet.png"
										title="About Us" class="profile-img themeModeDark"
										width="20px">
								</span> <span class="mobileTitle">About Us</span>
							</a></li>
							<li class="OtherIcon"><a class="" href="#"> <span
									class='listImage'> <img src="images/search_blue1.png"
										title="Other" class="profile-img themeModeDark" width="20px">
								</span> <span class="mobileTitle">Other</span>
							</a></li> -->
						</ul>
					</div>
					<div class='listofSettingDiv' id='helpContentDiv'
						style='display: none;'>
						<ul>
							<!-- 	<li><a class="" href="#"> <span class='listImage'>
										<img src="images/ChatIcon.png" title="Mycart"
										class="headerShoppingCart themeModeDark" width="20px">
								</span> <span class="mobileTitle">Chat</span>
							</a></li> -->
							<li><a class="" href="#"
								onclick="showAnalyticsHelp('document')"> <span
									class='listImage'> <img src="images/SearchHelp.png"
										title="Help Document" class="headerShoppingCart themeModeDark"
										width="20px">
								</span> <span class="mobileTitle">Help Document</span>
							</a></li>
							<li><a class="" href="#"
								onclick="showAnalyticsHelp('video')"> <span
									class='listImage'> <img src="images/SearchVideoPlay.png"
										title="Help Video" class="headerShoppingCart themeModeDark"
										width="20px">
								</span> <span class="mobileTitle">Help Video</span>
							</a></li>
							<!-- <li><a class="" href="#"> <span class='listImage'>
										<img src="images/SearchGif.png" title="Help Gif"
										class="headerShoppingCart themeModeDark" width="20px">
								</span> <span class="mobileTitle">Help Gif</span>
							</a></li>  -->
						</ul>
					</div>

					<div class='listofSettingDiv' id='calendarContentDiv'
						style='display: none;'>
						<div class="scheduledCalendarMainWrapper">
							<div class="content-wrapper grey lighten-3">
								<div class="container calendarMainClass">
									<div class="calendar-wrapper z-depth-2">
										<div class="calendar-header">
											<div class="row header-title header-text">
												<div class="col-md-6">
													<h3 id="month-name"></h3>
												</div>
												<div class="col-md-6">
													<div class="currentdate">
														<h5 id="todayDayName">Today</h5>
														<a class="prev-button" id="prev"> <i
															class="fa fa-chevron-left" aria-hidden="true"></i>
														</a> <a class="next-button" id="next"> <i
															class="fa fa-chevron-right" aria-hidden="true"></i>
														</a>
													</div>
												</div>
											</div>
										</div>


										<div class="calendar-content">
											<div id="calendar-table" class="calendar-cells">
												<div id="table-header">
													<div class="row">
														<div class="col-1 colDays">M</div>
														<div class="col-1 colDays">T</div>
														<div class="col-1 colDays">W</div>
														<div class="col-1 colDays">T</div>
														<div class="col-1 colDays">F</div>
														<div class="col-1 colDays">S</div>
														<div class="col-1 colDays">S</div>
													</div>
													<hr />
												</div>

												<div id="table-body"></div>

											</div>
										</div>
										<div class="sidebar-wrapper z-depth-2 side-nav fixed"
											id="sidebar">

											<div class="sidebar-title">
												<h5 id="eventDayName">Date</h5>
												<h5 class="newEventName" onclick="addNewDateWiseEvent()">
													Add new event <span class="newEventSpan"> <img
														src="images/calendarevent.png" class="newEventImage"
														width="20px">
													</span>
												</h5>
											</div>
											<div class="sidebar-events" id="sidebarEvents">
												<div class="empty-message">Currently, no events to
													selected date</div>
											</div>
										</div>

										<!-- 										<div class="calendar-footer"> -->
										<!-- 											<div class="emptyForm" id="emptyForm"> -->
										<!-- 												<h4 id="emptyFormTitle">No events now</h4> -->
										<!-- <!-- 												<a class="addEvent" id="changeFormButton">Add new</a> -->
										<!-- 											</div> -->

										<!-- 										</div> -->

									</div>

								</div>

							</div>
						</div>
					</div>
					<div class='listofSettingDiv' id='userContentDiv'
						style='display: none;'>
						<div class="media">
							<span class="spanCloseIcon" onclick="closesettingPannel()">×</span>
							<div id="userProfileImgDiv" class="userProfileImgDiv"
								objStr="${sessionScope.ssUsername}">
								<div class="avatar-upload">
									<div class="avatar-preview">
										<div id="imagePreview"
											style="background-image: url(${sessionScope.imageurl})"></div>
									</div>
								</div>
								<div class="userProfileImageBottomWrapper">
									<div class="deleteProfileImg" onclick="removeUserProfilePic()">
										<img src="images/delete_icon.svg" />
									</div>
									<div class="avatar-edit">
										<input type="file" id="imageUpload" accept=".png, .jpg, .jpeg" />
										<label for="imageUpload"></label>
									</div>
								</div>
							</div>
							<div class="media-body">
								<h4 class="loginaccName">${sessionScope.ssUsername}</h4>
							</div>
							<!--                                        <div class="p-image">
                                                                                           <i class="fa fa-camera upload-button"></i>
                                                                                       </div>-->
						</div>
						<ul>
							<%-- <li class="shoppingIcon"><a href="<c:url value="/"/>myCart">
									<span class='listImage'> <img
										src="images/shopping-Cart-Icon.svg" title="Mycart"
										class="headerShoppingCart themeModeDark" width="20px">
								</span> <span class="mobileTitle">My Cart</span>
							</a></li>
							<li><a href="#" onclick="showMySubscriptionsData()"> <span
									class='listImage'> <img src="images/subscription.png"
										alt="" style='width: 20px;'>
								</span> <span class="subscription mobileTitle">My Subscriptions</span>
							</a></li>
							<li><a href="#" onclick="showMyTransactionsData()"> <span
									class='listImage'> <img src="images/transaction.png"
										alt="" style='width: 20px;'>
								</span> <span class="transaction mobileTitle">My Transactions</span></a></li>
							<li><a href="#" onclick="showMyWalletsData()"> <span
									class='listImage'> <img src="images/wallet.png" alt=""
										style='width: 20px;'></span> <span class="wallet mobileTitle">My
										Wallet</span></a></li>
							<li><a href="#" onclick="showMyWorkSpaceData('FORM')"><span
									class='listImage'> <img src="images/workspace.png"
										alt="" style='width: 20px;'></span><span
									class="workSpace mobileTitle">My Workspace</span></a></li>
							<li><a href="#" onclick="showMyWorkSpaceData('CHART')"><span
									class='listImage'> <img
										src="images/analyticsShowCard.png" alt="" style='width: 20px;'></span><span
									class="analytics mobileTitle">Workspace Analytics</span></a></li> --%>
							<li class="logoutIcon openbtn" title="Logout" data-toggle="modal"
								data-target="#signOut"><a class="" href="#"> <span
									class='listImage'> <img src="images/LogOut_Icon.svg"
										class="profile-img themeModeDark" width="20px"></span> <span
									class="mobileTitle">Log Out</span>
							</a></li>
						</ul>
					</div>
				</div>
			</div>
		</div>
		<div class="backGroundOpacity" id="backgroundShadowDiv"
			style="display: none"></div>
	</div>
	<!-- DXP New Theme Body Layout -->
	<c:choose>
		<c:when test="${not empty sessionScope.ssUsername}">
			<%@include file="footer.jsp"%>
		</c:when>
		<c:otherwise>
		</c:otherwise>
	</c:choose>

	<div class="dataDxpSplitterValue" id="dataDxpSplitterValue"></div>
	<div class="dataDxpSplitterValueNew" id="dataDxpSplitterValueNew"></div>
	<div class="visionTempleteHoverImage" id="visionTempleteHoverImage"></div>
	<div class="" id="cardImageImportDiv">
		<input type="file" name="importCardImage" id="importCardImage"
			style="display: none;">
	</div>
	<input type="hidden" name="rsUserName" id="rsUserName"
		value="${ssUserName}" />
	<input type="hidden" id="analysisType" value="" />
	<input type="hidden" name="chartType" id="chartType" value="" />
	<input type="hidden" name="currentTypedValue" id="currentTypedValue"
		value="" />
	<input type="hidden" name="showCaseCardType" id="showCaseCardType"
		value="" />
	<input type="hidden" name="compareType" id="compareType" value="" />
	<input type="hidden" name="currentSocialMediaFlag"
		id="currentSocialMediaFlag" value="" />
	<input type="hidden" name="districtSearchListBox"
		id="districtSearchListBox" value="" />
	<input type="hidden" name="constituencySearchListBox"
		id="constituencySearchListBox" value="" />
	<input type="hidden" name="candidateSearchListBox"
		id="candidateSearchListBox" value="" />
	<input type="hidden" name="partySearchListBox" id="partySearchListBox"
		value="" />
	<input type="hidden" name="isCurrencyConversionEvent"
		id="isCurrencyConversionEvent" value="" />
	<input type="hidden" name="subscriptionType" id="subscriptionType"
		value="${sessionScope.ssSubscriptionType}" />
	<input type="hidden" name="roleId" id="roleId"
		value="${sessionScope.ssRole}" />
	<form action="" id="navigationUrlForm" method="POST">
		<c:if test="true">
			<input type="hidden" name="${_csrf.parameterName}"
				value="${_csrf.token}" />
		</c:if>
	</form>
	<div id="removeDup" class="removeDup"></div>
	<div id="deleteDup" class="deleteDup"></div>
	<div id="columnMappingDialog" class="columnMappingDialog"></div>
	<div id="dialog1"></div>
	<div id="importDataView"></div>
	<div id="dataDxpSplitterValue" class="dataDxpSplitterValue"></div>
	<div id="pivotGridDialog">
		<!-- 		<div id='output' style='margin: 30px;'></div> -->
	</div>
	<div id="jqxpivotGridDialog" style="display: none">
		<table>
			<tr>
				<td>
					<div id="divPivotGridDesigner" style="height: 400px; width: 250px;">
					</div>
				</td>
				<td>
					<div id="divPivotGrid" style="height: 400px; width: 550px;"></div>
				</td>
			</tr>
		</table>
	</div>
	<div class="mainchatcontainer" style="display: none">
		<div class="chatIcon">
			<img src="images/ChatIcon.png" data-flag="I" id="chatBotIcon"
				onclick="chatApplication();" style='width: 50px; cursor: pointer;'>
		</div>

		<div id="chat" class="chatBox chatBoxLargeSpaced">
			<div class="container mainHeader">
				<div class="row  chatBotHeaderTop">
					<div class="col-12 leftBotIcon">
						<span><img src="images/customer-service.png"> </span>
						<div class="chatbotMetaHeader">
							<span>
								<p>Hello</p>
							</span>
							<p>Ask me about how to use the system</p>
						</div>
					</div>

					<div class="rightIcons">
						<span class="minmaxIcon" onclick="minimizeChatBot();"><img
							src="images/minimize.png" id="maxminIcon" title="minimize"></span>
						<span class="chatbotClose" onclick="closeChatBot();"><img
							src="images/closeIcon.png" title="close"></span>
					</div>

				</div>
			</div>
		</div>
	</div>

	<div class="mainConversationalAIcontainer" style='display: none'>
		<div class="conversationalAIIcon">
			<img src="images/image_2023_03_27T10_09_03_182Z.png" data-flag="I"
				id="chatBotIcon"
				onclick="showIntelliSenseAutoSuggestions('visionChartsAutoSuggestionUserId');"
				style='width: 70px; cursor: pointer;'>
		</div>
	</div>
	<script src="<c:url value="/"/>js/customSchedulCalendar.js"></script>
	<script>
		$(document)
				.ready(
						function() {
							var rsUserName = $("#rsUserName").val();
							if (rsUserName != null && rsUserName != ''
									&& rsUserName != 'null'
									&& rsUserName != undefined) {
								showLoader();
								var subscriptionType = $("#subscriptionType")
										.val();
								function checkTime() {
									var now = new Date();
									var hours = now.getHours();
									var minutes = now.getMinutes();

									// Check if it's 12:00 PM
									if (hours === 12 && minutes === 0) {
										//scheduleDailyTask();
									}
								}

								// Check every minute
								// setInterval(checkTime, 60000);
								//getHomePageSelectBoxResults("CHARTS");
								//                     socialMedialShowCaseCards();
								getLocationDetails();
								getHomePageSelectBoxResults("CHARTS");
								$('.userProfileIcon .userMainProfile').attr(
										"src", localStorage['profile_imgStr']);
								$(".OpenAisection").show();
								$("#intellisenseHomeSelectBox").show();
								$('#chatBotIcon').attr('data-flag', 'A');
								if (subscriptionType != null
										&& subscriptionType != ''
										&& subscriptionType != undefined
										&& (subscriptionType == 'Basic' || subscriptionType == 'BASIC')) {
									$('#toggle').click();
									toggleCheck();
									//$('#toggle').prop('checked', true);
									//$("#hintImageID").show();
									$("#hintImageID")
											.html(
													'<img src="images/idea-icon-trans-bg.png" class="hintImageClass" id="hintImageID" width="20px"/><span class="textHint" id="textHintID">Help me to navigate</span>');
									$("#hintImageID").attr('onclick',
											'dxpAnalyticsGuideHome()');
								}
								stopLoader();
								$(".sidebar-dropdown").show();

							} else {
								$(".sidebar-dropdown").hide();
								$(".isPoliticalSceince").hide();
								//                    var homePageCarousel = $("#homePageCarousel").val();
								//                    $("#pageBodyContent").html(homePageCarousel);
							}
							var searchCandidate = '${searchCandidate}';
							if (searchCandidate != null
									&& searchCandidate != ''
									&& searchCandidate != 'null') {
								getProfileUserNames(searchCandidate);
							} else {
								$('#mainintelliSenseSelectBoxId').hide();
							}

							$('.dropdown-submenu a.test').on("click",
									function(e) {
										$(this).next('ul').toggle();
										e.stopPropagation();
										e.preventDefault();
									});
							sessionStorage.clear();

							$("#backgroundShadowDiv").click(function() {
								closesettingPannel();
							});
						});
	</script>
	<section class='OpenAisection' style='display: none'>
		<div class="openAiButton">
			<c:choose>
				<c:when
					test="${not empty sessionScope.ssSubscriptionType && (sessionScope.ssSubscriptionType == 'Basic' || sessionScope.ssSubscriptionType == 'BASIC')}">
				</c:when>
				<c:otherwise>
					<span style="font-size: 30px; cursor: pointer"
						onclick="toggleOpenCloseAINavigationCheckbox();openAINavigation();"><img
						src='images/IG_CO-Pilot.png'
						style="cursor: pointer; width: 45px; margin-top: 5px; margin-right: 5px;" /></span>
				</c:otherwise>
			</c:choose>

		</div>
		<div id="myNav" class="overlay">
			<!--<div class="dragArrowDiv"><img src="images/leftArrow.png" width="22px" id="dragArrowImgId" onclick="LRDragAIPanel()"></div>-->
			<div class="defultShowAIDiv">
				<div class='closeAibutton'>
					<div class="aiWelcometext">
						<ul>
							<li class="ailensicon"><img src="images/IG_CO-Pilot.png"
								class="aiLensImgSrcAppend" /></li>
							<li class="ailensTitle"><img src="images/mirai1.png"
								style='width: 40px'> <!-- IntelliSense Lens --></li>
						</ul>
					</div>
					<div class="aipanelRightIconsDiv">
						<!--<ul>
                             <li> <img src="images/aiSearchIcon.png" id="aiSearchId" onclick="showAISearch()" style="cursor:pointer;width:20px;"> </li>
                             <li> <img src="images/ai-history.png" id="aiPromptDataIcon" onclick="showRecentPromptData()" title="show prompts" style="cursor:pointer;width:20px;" /> </li>
                             <li> <img src="images/AINotification.gif" class="ainotification" onclick="showAINotification()" style="cursor:pointer;width:27px;"/> </li>
                             <li><a href="javascript:void(0)" class="closebtn" onclick="closeAINavigation()">&times;</a></li>
                           </ul> -->
					</div>
				</div>
				<div class="overlay-content">
					<div class="dragLeftArrowDiv">
						<img src="images/leftArrow.png" width="22px"
							id="dragLeftArrowImgId" onclick="leftDragAIPanel()">
					</div>
					<div class="dragRightArrowDiv" style="display: none">
						<img src="images/rightArrow.png" width="20px"
							id="dragRightArrowImgId" onclick="rightDragAIPanel()">
					</div>
					<!--  <div class="aiToggleBtn" id="aiLensToggleBtnId" onclick="disableEnableAiLens()">
                                                <span> <img src="images/aieyeLensclick.png"  width="24px" title="AI Lens Desable/Enable Buttton" /></span>
                                                <span class="aiTextHint">Click to disable <span class="aiBrand">AI Lens</span></span>
                                            </div> -->
					<div class='searchAiBarDiv' style="display: none">
						<div class="aiSearchIconMainDiv">
							<img src="images/aiSearchIcon.png" width="20px" />
						</div>
						<input type='text' class="form-control aiSearh-input"
							placeholder='Search..' /> <img src="images/aiCloseIcon.png"
							id="clearAISearch" class="aiSearchClearBtn" width="20px" />
					</div>
					<div class='aicontentArea'>
						<div class='aiHeaderContainerClass'>
							<div class="introGuiderAiCls" id="introGuiderAiId"
								style="display: none">
								<div>Hello ${sessionScope.ssUsername}!</div>
								<div>I am Your everyday IS Copilot</div>
							</div>
						</div>
						<div id="myBtnContainer" class="aiButtongroup"></div>
						<!-- Portfolio Gallery Grid -->
						<div class="aigridrow"></div>
						<div class='aiNotificationsResultClass' style="margin-top: 10px;"></div>
						<div class='aiTabsContainer'>
							<div id="aiPrevCopilotId" class="aiPrevCopilotCls">
								<button class="btn animation-anime active"
									id='aiCopilotHomeButtonId' clear-id='aiLensHomeDivId'
									onclick="startIntegralALLensHome('Home','aiLensHomeDivId')">
									<span class='aitabimage'></span><span class='aitabTitle'>Home</span>
								</button>
								<button class="btn animation-anime"
									id='aiCopilotMainDataAnalyticsButtonId'
									clear-id='aiLensMainDataAnalyticsDivId'
									onclick="startIntegralALLensHomeModules('DataAnalytics','aiLensMainDataAnalyticsDivId')">
									<span class='aitabimage'></span><span class='aitabTitle'>Data
										Analytics </span>
								</button>
								<c:choose>
									<c:when
										test="${not empty sessionScope.ssSubscriptionType && 
										(sessionScope.ssSubscriptionType == 'PROFESSIONAL'|| sessionScope.ssSubscriptionType == 'Professional' || sessionScope.ssSubscriptionType == 'ENTERPRISE' || sessionScope.ssSubscriptionType == 'Enterprise')}">
										<button class="btn animation-anime"
											id='aiCopilotDataIntegrationButtonId'
											clear-id='aiLensDataIntegrationDivId'
											onclick="startIntegralALLensHomeModules('DataIntegration','aiLensDataIntegrationDivId')">
											<span class='aitabimage'></span><span class='aitabTitle'>Data
												Integration</span>
										</button>
									</c:when>
								</c:choose>
								<c:choose>
									<c:when
										test="${not empty sessionScope.ssSubscriptionType && 
										(sessionScope.ssSubscriptionType == 'ENTERPRISE' || sessionScope.ssSubscriptionType == 'Enterprise')}">
										<button class="btn animation-anime"
											id='aiCopilotDataMigrationButtonId'
											clear-id='aiLensDataMigrationDivId'
											onclick="startIntegralALLensHomeModules('DataMigration','aiLensDataMigrationDivId')">
											<span class='aitabimage'></span><span class='aitabTitle'>Data
												Migration</span>
										</button>
									</c:when>
								</c:choose>
								<!-- <button class="btn animation-anime"
									id='aiCopilotQuickInsightsButtonId'
									clear-id='aiLensQuickInsightsDivId'
									onclick="filterSelection('Analytics','aiLensQuickInsightsDivId')">
									<span class='aitabimage'></span><span class='aitabTitle'>Quick
										Insights</span>
								</button> -->
							</div>
							<div class="aiChatContainer" id="aiChatContainerdivID">
								<div class="aiLensHomeDivClass" id='aiLensHomeDivId'
									tab-id="HOME"></div>
								<div class="aiLensMainDataAnalyticsDivClass"
									id='aiLensMainDataAnalyticsDivId' tab-id="DATA_ANALYTICS"
									style="display: none;"></div>
								<div class="aiLensDataIntegrationDivClass"
									id='aiLensDataIntegrationDivId' tab-id="DATA_INTEGRATION"
									style="display: none;"></div>
								<div class="aiLensDataMigrationDivClass"
									id='aiLensDataMigrationDivId' tab-id="DATA_MIGRATION"
									style="display: none;"></div>
								<div class='aiChatgptResponseContainer'
									id='aiLensQuickInsightsDivId' style="display: none;"
									tab-id="QUICK_INS"></div>

							</div>
						</div>
						<div id="threeDotsLoader" class='threeDotsLoader'
							style="display: none;">
							<img src="images/dots.gif" />
						</div>
					</div>
					<div id='aiLensQuickAnalyticsQuestionsDivId'></div>
				</div>
				<div class='aibottomMessageContainer'>
					<div class="stopresponding" id="stopResponsingID"
						style="display: none;">
						<button class="btn btn-primary">
							<span class="stopIcon"><i class="fa fa-stop"
								aria-hidden="true"></i></span> <span class="stopText">Stop
								Responding</span>
						</button>
					</div>
					<div class='userAIInputBottomWidget'>
						<div class="userAIInputText">
							<input type='text' class="form-control"
								placeholder='Please ask only system guidance questions'
								id='aiTypedValue' onkeyup="showAIKeyDownResults(event)"
								autocomplete="off" /> <img src='images/aisend.png'
								onclick="showAILensReply()" style="cursor: pointer;" />
						</div>
						<div class='moreaiOptions'>
							<ul>
								<li><img src="images/Mike-OutLine-Icon-01.png"
									id="muteMicId" class="" title="record a message"
									style="cursor: pointer;" /></li>
								<li>
									<div id="infoLens" class="infoClassLens">
										<div class="center">
											<div class="keyboardIconsMainDivLens">
												<span id="keyboardIdLens"><i class="fa fa-keyboard-o"
													style="font-size: 25px; color: #0b4a99"></i></span> <span
													class="minMaxKeys" id="minMaxKeysIdlens"
													style="display: none"> <span onclick="incKeySize()"><i
														class="fa fa-angle-up"
														style="font-size: 18px; color: blue"></i></span> <span
													onclick="decKeySize()"><i class="fa fa-angle-down"
														style="font-size: 18px; color: blue"></i></span>
												</span>
											</div>
										</div>
									</div>
								</li>
								<li>
									<div id="languageIdLens" class="integralMultiLanguageLensClass">
										<select id="langSelectLens" class="languageSelectionBoxLens"
											name="language" onchange="languageSelectLens()"></select>
									</div>
								</li>
								<li data-toggle="modal" data-target="#discovermoreAiPopup"><img
									src="images/threedots.png" width='20px' title="discover more"
									class="morethreeDots" style="cursor: pointer;" /></li>
							</ul>
						</div>
					</div>
				</div>
			</div>
		</div>
	</section>
	<script>
		$(document).ready(function() {
			$(".se-pre-con").fadeOut("slow");

		});
		$(function() {
			$('[data-toggle="tooltip"]').tooltip()
		})
		$(function() {
			/* Muni js for slick slider cards on Login */
			$('.service-statistics').slick({
				dots : false,
				arrows : true,
				speed : 300,
				autoplay : false,
				slidesToShow : 3,
				slidesToScroll : 1,
				draggable : false,
				responsive : [ {
					breakpoint : 1024,
					settings : {
						slidesToShow : 3,
						slidesToScroll : 1,
					}
				}, {
					breakpoint : 991,
					settings : {
						slidesToShow : 2,
						slidesToScroll : 1,
					}
				},

				{
					breakpoint : 600,
					settings : {
						slidesToShow : 2,
						slidesToScroll : 1
					}
				}, {
					breakpoint : 480,
					settings : {
						slidesToShow : 1,
						slidesToScroll : 1
					}
				} ]
			});
			$('.pilog-appointments').slick({
				dots : false,
				arrows : true,
				autoplay : true,
				slidesToShow : 2,
				slidesToScroll : 1,
				autoplaySpeed : 0,
				speed : 6000,
				pauseOnHover : true,
				cssEase : 'linear',
				vertical : true,
				verticalSwiping : true,
				responsive : [ {
					breakpoint : 480,
					settings : {
						slidesToShow : 1,
						slidesToScroll : 1
					}
				} ]
			});

			$('.piLog-awards').slick({
				dots : false,
				arrows : false,
				speed : 300,
				autoplay : true,
				slidesToShow : 1,
				slidesToScroll : 1,
			});
			$('.pilog-clients').slick({
				dots : false,
				arrows : false,
				autoplay : true,
				slidesToShow : 2,
				slidesToScroll : 1,
				autoplaySpeed : 0,
				speed : 6000,
				pauseOnHover : true,
				cssEase : 'linear'
			});
			$('#pilog-eventId').slick({
				dots : false,
				arrows : true,
				autoplay : true,
				slidesToShow : 1,
				slidesToScroll : 1,
				autoplaySpeed : 0,
				speed : 6000,
				pauseOnHover : true,
				cssEase : 'linear'
			});

			$('.facts_stats').slick({
				dots : false,
				arrows : false,
				speed : 300,
				autoplay : true,
				slidesToShow : 4,
				slidesToScroll : 1,
				draggable : false,
				responsive : [ {
					breakpoint : 1024,
					settings : {
						slidesToShow : 3,
						slidesToScroll : 1,
					}
				}, {
					breakpoint : 600,
					settings : {
						slidesToShow : 2,
						slidesToScroll : 1
					}
				}, {
					breakpoint : 480,
					settings : {
						slidesToShow : 1,
						slidesToScroll : 1
					}
				} ]
			});

			$('.key_cods').slick({
				dots : false,
				arrows : false,
				autoplay : true,
				slidesToShow : 5,
				vertical : true,
				draggable : false,
				autoplaySpeed : 0,
				speed : 5000,
				pauseOnHover : true,
				cssEase : 'linear'
			});

			setTimeout(function() {
				$("#niic_FeedBack_formId div").each(
						function(i) {
							var feedbackId = $(this).find("div").attr("id");
							if (feedbackId != null && feedbackId != ''
									&& feedbackId != undefined
									&& feedbackId != 'undefined') {
								var feedbackVal = $("#" + feedbackId).html();
								$("#" + feedbackId).jqxRating({
									width : 350,
									height : 35,
									value : feedbackVal,
									disabled : true,
									itemHeight : 15,
									itemWidth : 15
								});

							}
						});

			}, 500);
		});
		$(".about-benefits-carousel").slick({
			dots : true,
			arrows : false,
			infinite : true,
			autoplay : true,
			autoplaySpeed : 3500,
			speed : 1000,
			slidesToShow : 1,
			slidesToScroll : 1,
		});
	</script>
</body>
</html>

