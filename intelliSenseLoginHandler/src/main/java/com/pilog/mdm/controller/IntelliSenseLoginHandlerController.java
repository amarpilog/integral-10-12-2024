/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Kiran Raj
 */

package com.pilog.mdm.controller;

import com.pilog.mdm.DTO.LoginHandler;
import com.pilog.mdm.pojo.BApplProperties;
import com.pilog.mdm.pojo.BLanguage;
import com.pilog.mdm.service.IntelliSenseLoginHandlerService;
import com.pilog.mdm.utilities.PilogUtilities;
import javax.servlet.http.HttpServletRequest;

import com.pilog.mdm.service.IntelliSenseMenuService;
import com.pilog.mdm.utilities.PilogEncryption;

import java.lang.reflect.Method;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import javax.servlet.http.HttpSession;
import org.json.simple.JSONObject;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class IntelliSenseLoginHandlerController {

	@Autowired
	private IntelliSenseLoginHandlerService loginHandlerService;
//
	@Autowired
	private IntelliSenseMenuService menuService;

	@Autowired
	private ApplicationContext appContext;

	PilogUtilities utilities = new PilogUtilities();

	@RequestMapping(value = { "/", "/homePage" }, method = { RequestMethod.POST, RequestMethod.GET })
	public String piLogDXPHome(ModelMap model, HttpServletRequest request) {
		String sideMenuStr = "";
		String ssUsername = "";
		if (request.getSession(false) != null) {
			ssUsername = (String) request.getSession(false).getAttribute("ssUsername");
		}
		try {
			String username = (String) request.getSession(false).getAttribute("ssUsername");
			String terminate = request.getParameter("terminate");
			if (terminate != null && !terminate.equalsIgnoreCase("") && !terminate.equalsIgnoreCase("2"))
				try {
					this.loginHandlerService.updateLogOutUserNameOtherSessionIds(username.toUpperCase(), request);
				} catch (Exception e) {
					e.printStackTrace();
				}
		} catch (Exception e) {
		}
		
		if (ssUsername != null && !"".equalsIgnoreCase(ssUsername) && !"null".equalsIgnoreCase(ssUsername)) {
			sideMenuStr = menuService.getSideMenu(request);
			model.put("selectTypes", loginHandlerService.getDomainSelectionTypes(request));
		} else {
			sideMenuStr = "<nav id=\"sidebar\" class=\"sidebar-wrapper toggled\">"
					+ "<div class=\"sidebar-content\" onclick=\"sideMenuAccess('MM_MANAGER')\">"
					+ "<div class=\"sidebar-menu\"><ul>"
					+ "<li class=\"\"><a href=\"#\" class=\"waves-effect waves-dark\">"
					+ "<span class=\"dxpHomeMenuIconClass\">"
					+ "<img src=\"images/Home-Iocn.png\" class=\"themeModeDark\" title=\"Home\"></span>"
					+ "<span class=\"menuTitle\">Home</span></a></li>" + "<li class=\"\"><a href=\"#\">"
					+ "<span class=\"dxpMenuImageClass\"><img src=\"images/Data-Analytics-icon.svg\" "
					+ "class=\"themeModeDark\" title=\"Data Analytics\"></span>"
					+ "<span class=\"menuTitle\">Data Analytics</span></a</li>"
					+ "<li class=\"\"><a href=\"#\"><span class=\"dxpMenuImageClass\">"
					+ "<img src=\"images/iTransform ETL.svg\" class=\"themeModeDark\" title=\"iTransform ETL\"></span>"
					+ "<span class=\"menuTitle\">iTransform ETL</span></a></li>"
					// + "<li class=\"\"><a href=\"#\"><span class=\"dxpMenuImageClass\">"
					// + "<img src=\"images/TaxonomyAndRepositories.svg\" class=\"themeModeDark\">"
					// + "</span><span class=\"menuTitle\">Repositories</span></a></li>"
					// + "<li class=\"\"><a href=\"#\"><span class=\"dxpMenuImageClass\">"
					// + "<img src=\"images/Data-Governance.svg\" class=\"themeModeDark\"></span>"
					// + "<span class=\"menuTitle\">Data Governance</span></a></li>"
					+ "<li class=\"isPoliticalSceince\"><a href=\"#\"><span class=\"dxpMenuImageClass isPoliticalSceince\">"
					+ "<img src=\"images/Profile_Icon.svg\" class=\"themeModeDark\" title=\"Profile\"></span>"
					+ "<span class=\"menuTitle\">Political Science</span></a></li>" + "</ul></div></div></nav>";
//            String homePageCarousel = "<section class='homepageCarouselSection'>"
//                    + "<div id='demo' class='carousel slide' data-ride='carousel'>"
//                    + "<ul class='carousel-indicators'>"
//                    + "<li data-target='#demo' data-slide-to='0' class='active'></li>"
//                    + "<li data-target='#demo' data-slide-to='1'></li></ul>"
//                    + "<div class='carousel-inner'><div class='carousel-item active'>"
//                    + "<img src='images/homePageCardImg1.jpg' width='1100' height='500'></div>"
//                    + "<div class='carousel-item'>"
//                    + "<img src='images/homePageCardImg2.jpg' width='1100' height='500'></div></div>"
//                    + "<a class='carousel-control-prev' href='#demo' data-slide='prev'>"
//                    + "<span class='carousel-control-prev-icon'></span></a>"
//                    + "<a class='carousel-control-next' href='#demo' data-slide='next'>"
//                    + "<span class='carousel-control-next-icon'></span></a></div></section>";
//            model.put("homePageCarousel", homePageCarousel);
		}
		if (request.getSession(false) != null) {
			request.getSession(false).setAttribute("sideMenuStr", sideMenuStr);
		}
		String processVal = "";
		BApplProperties bapplPropertiesVal = (BApplProperties) loginHandlerService
				.getApplProperties("HOME_PAGE_TILES_SHOW_HIDE");
		if (bapplPropertiesVal != null) {
			processVal = bapplPropertiesVal.getId().getProcessValue();
		}

		String mainHeader = homePageDefaultString(request, processVal);
		model.put("mainHeader", mainHeader);
		model.put("sideMenuStr", sideMenuStr);
		model.put("secretKey", PilogEncryption.secretKey);
		model.put("ssUserName", ssUsername);
		String searchCandidate = request.getParameter("searchCandidate");
		if (searchCandidate != null && !"".equalsIgnoreCase(searchCandidate)
				&& !"null".equalsIgnoreCase(searchCandidate)) {
			model.put("searchCandidate", searchCandidate);
		} else {
			model.put("chartFlag", "Y");
		}

		return "Home";
	}

	@RequestMapping(value = { "/timeout" }, method = { RequestMethod.POST, RequestMethod.GET })
	public String piLogDXPTimeout(ModelMap model, HttpServletRequest request) {
//        String sideMenuStr = menuService.getSideMenu(request);
		String sideMenuStr = "<nav id=\"sidebar\" class=\"sidebar-wrapper toggled\">"
				+ "<div class=\"sidebar-content\" onclick=\"sideMenuAccess('MM_MANAGER')\">"
				+ "<div class=\"sidebar-menu\"><ul>" + "<li class=\"\"><a href=\"#\" class=\"waves-effect waves-dark\">"
				+ "<span class=\"dxpHomeMenuIconClass\">"
				+ "<img src=\"images/Home-Iocn.png\" class=\"themeModeDark\" title=\"Home\"></span>"
				+ "<span class=\"menuTitle\">Home</span></a></li>" + "<li class=\"\"><a href=\"#\">"
				+ "<span class=\"dxpMenuImageClass\"><img src=\"images/Data-Analytics-icon.svg\" "
				+ "class=\"themeModeDark\" title=\"Data Analytics\"></span>"
				+ "<span class=\"menuTitle\">Data Analytics</span></a</li>"
				+ "<li class=\"\"><a href=\"#\"><span class=\"dxpMenuImageClass\">"
				+ "<img src=\"images/iTransform ETL.svg\" class=\"themeModeDark\" title=\"iTransform ETL\"></span>"
				+ "<span class=\"menuTitle\">iTransform ETL</span></a></li>"
				// + "<li class=\"\"><a href=\"#\"><span class=\"dxpMenuImageClass\">"
				// + "<img src=\"images/TaxonomyAndRepositories.svg\" class=\"themeModeDark\">"
				// + "</span><span class=\"menuTitle\">Repositories</span></a></li>"
				// + "<li class=\"\"><a href=\"#\"><span class=\"dxpMenuImageClass\">"
				// + "<img src=\"images/Data-Governance.svg\" class=\"themeModeDark\"></span>"
				// + "<span class=\"menuTitle\">Data Governance</span></a></li>"
				+ "<li class=\"isPoliticalSceince\"><a href=\"#\"><span class=\"dxpMenuImageClass\">"
				+ "<img src=\"images/Profile_Icon.svg\" class=\"themeModeDark\" title=\"Profile\"></span>"
				+ "<span class=\"menuTitle\">Political Science</span></a></li>" + "</ul></div></div></nav>";
//        String homePageCarousel = "<section class='homepageCarouselSection'>"
//                + "<div id='demo' class='carousel slide' data-ride='carousel'>"
//                + "<ul class='carousel-indicators'>"
//                + "<li data-target='#demo' data-slide-to='0' class='active'></li>"
//                + "<li data-target='#demo' data-slide-to='1'></li></ul>"
//                + "<div class='carousel-inner'><div class='carousel-item active'>"
//                + "<img src='images/homePageCardImg1.jpg' width='1100' height='500'></div>"
//                + "<div class='carousel-item'>"
//                + "<img src='images/homePageCardImg2.jpg' width='1100' height='500'></div></div>"
//                + "<a class='carousel-control-prev' href='#demo' data-slide='prev'>"
//                + "<span class='carousel-control-prev-icon'></span></a>"
//                + "<a class='carousel-control-next' href='#demo' data-slide='next'>"
//                + "<span class='carousel-control-next-icon'></span></a></div></section>";
//        model.put("homePageCarousel", homePageCarousel);
		if (request.getSession(false) != null) {
			request.getSession(false).setAttribute("sideMenuStr", sideMenuStr);
		}
		String processVal = "";
		BApplProperties bapplPropertiesVal = (BApplProperties) loginHandlerService
				.getApplProperties("HOME_PAGE_TILES_SHOW_HIDE");
		if (bapplPropertiesVal != null) {
			processVal = bapplPropertiesVal.getId().getProcessValue();
		}

		String mainHeader = homePageDefaultString(request, processVal);
		model.put("mainHeader", mainHeader);
		model.put("sideMenuStr", sideMenuStr);
		model.put("secretKey", PilogEncryption.secretKey);
		return "Home";
	}

	@RequestMapping(value = "/cloudLogin", method = { RequestMethod.POST, RequestMethod.GET })
	public @ResponseBody JSONObject cloudLogin(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		String resultString = "";
		String redirectpage = "";
		String errmessage = "";
		try {
			String rsUsername = request.getParameter("rsUsername");
			request.setAttribute("ssUsername", rsUsername);
			System.out.println("rsUsername:::" + rsUsername);
			if (rsUsername != null && !"".equalsIgnoreCase(rsUsername) && !"null".equalsIgnoreCase(rsUsername)) {
				rsUsername = rsUsername.toUpperCase();
			}
			String rsPassword = request.getParameter("rsPasswordHid");
			String ssAutoStart = request.getParameter("ssAutoStart");
			String sessionId = request.getParameter("sessionId");
			if (rsPassword != null && !"".equalsIgnoreCase(rsPassword)) {
				rsPassword = PilogEncryption.decryptText(rsPassword, "");
			}
			String language = request.getParameter("language");
			LoginHandler loginHandler = new LoginHandler();
			loginHandler.setRsPassword(rsPassword);
			loginHandler.setRsUsername(rsUsername);
			loginHandler.setSsAutoStart(ssAutoStart);
			loginHandler.setLanguage(language);
			String otp = request.getParameter("otp");
			if (otp != null && !"".equalsIgnoreCase(otp) && !"null".equalsIgnoreCase(otp)) {
				loginHandler.setOtp(otp);
			}
			String message = request.getParameter("message");
			resultObj = loginHandlerService.loginHandler(loginHandler, request);
			// adding sessionObj value onkar
			HttpSession loginSession = request.getSession();
			System.out.println("loginSession:::" + loginSession.getId());
			resultObj.put("loginSession", loginSession.getId());
			if (resultObj.get("sessionObj") instanceof JSONObject) {
				JSONObject sessionObj = (JSONObject) resultObj.get("sessionObj");
				if (sessionObj != null && !sessionObj.isEmpty()) {
					for (Object sessionName : sessionObj.keySet()) {
						loginSession.setAttribute(String.valueOf(sessionName), sessionObj.get(sessionName));
					}
				}
			} else if (resultObj.get("sessionObj") instanceof HashMap) {
				HashMap sessionObj = (HashMap) resultObj.get("sessionObj");
//                    System.out.println("sessionObj:::"+sessionObj);
				if (sessionObj != null && !sessionObj.isEmpty()) {
					for (Object sessionName : sessionObj.keySet()) {
						loginSession.setAttribute(String.valueOf(sessionName), sessionObj.get(sessionName));
					}
				}
			}
			// adding sessionObj value onkar

			resultString = (String) resultObj.get("resultCode");
			JSONObject sessionObj = (JSONObject) resultObj.get("sessionObj");

			String role = (String) sessionObj.get("ssRole");
			String ssLocale = (String) sessionObj.get("ssLocale");
			resultObj.put("role", role);
			resultObj.put("ssLocale", ssLocale);
			resultObj.put("returnCde", resultString);
			resultObj.put("ssUsername", rsUsername);
			if ("success".equalsIgnoreCase(resultString)) {
				resultObj.put("redirectpage", "homePage");
			} else if (resultString.lastIndexOf("alreadyLoggedIn") > -1) {
				JSONObject labelobj = new JSONObject();
				resultObj.put("labelobj", labelobj);
				JSONObject obj = loginHandlerService.getLogonDetails(request, sessionId, rsUsername);
				resultObj.put("details", obj);
				String htmlStr = "<table class=\"visionLoginHandlerTable\">\n" + "    <tbody><tr>\n"
						+ "    <td class=\"visionLoginHandlerData visionLoginHandlerDataAlgin\" colspan=\"2\">"
						+ "<span>" + utilities.convertIntoMultilingualValue(labelobj, "User") + " "
						+ obj.get("userName") + " "
						+ utilities.convertIntoMultilingualValue(labelobj, "is already  logged in from")
						+ "</span></td></tr>\n"// userName
						+ "    <tr>   \n"
						+ "    <td class=\"visionLoginHandlerData visionLoginHandlerDataAlgin\" colspan=\"2\">"
						+ "<span>" + utilities.convertIntoMultilingualValue(labelobj, "IP") + ": "
						+ obj.get("ipAddress") + " " + utilities.convertIntoMultilingualValue(labelobj, "since") + " "
						+ obj.get("loginDate") + "</span></td>  \n"// ipAddress,loginDate
						+ "    \n" + "   </tr>\n" + "   <tr>\n"
						+ "	<td class=\"visionLoginHandlerData visionLoginHandlerDataAlgin visionLoginHandlermessage\" colspan=\"2\">"
						+ "<span>" + utilities.convertIntoMultilingualValue(labelobj, "Would you like to")
						+ ":</span></td>  \n" + "	</tr>\n" + "	 <tr>\n"
						+ "   <td class=\"visionLoginHandlerData\"><span>" + ""
						+ utilities.convertIntoMultilingualValue(labelobj,
								"Continue with this logon and terminate other session")
						+ "" + "</span></td>\n" + "  </tr>\n" + "\n" + "  \n" + "</tbody></table>";
				resultObj.put("htmlStr", htmlStr);
				BApplProperties errorMessageProps = (BApplProperties) loginHandlerService
						.getApplProperties(resultString);
				if (errorMessageProps != null) {
//                    errmessage = errorMessageProps.getId().getProcessValue();
					errmessage = (String) utilities.convertIntoMultilingualValue(labelobj, errmessage);
				}

			} else {
				BApplProperties errorMessageProps = (BApplProperties) loginHandlerService
						.getApplProperties(resultString);
				if (errorMessageProps != null) {
					errmessage = errorMessageProps.getId().getProcessValue();
				} else {
					errmessage = "failed to fetch error message.";
				}

			}

			if (errmessage != null && !"".equalsIgnoreCase(errmessage)) {
				redirectpage = "errorPage";
				JSONObject labelobj = new JSONObject();
				resultObj.put("errmessage", utilities.convertIntoMultilingualValue(labelobj, errmessage));
				resultObj.put("errorMesg", utilities.convertIntoMultilingualValue(labelobj, errmessage));
			}

			// }
		} catch (Exception e) {
			e.printStackTrace();
			redirectpage = "errorPage";
			resultObj.put("errorMesg", e.getLocalizedMessage());
		}
		return resultObj;

	}
//login handler

	@RequestMapping(value = "/cloudLogout", method = { RequestMethod.POST, RequestMethod.GET })
	public String cloudLogout(HttpServletRequest request, ModelMap model) {
		JSONObject resultObj = new JSONObject();
		try {
//            String sideMenuStr = menuService.getSideMenu(request);
			String sideMenuStr = "<nav id=\"sidebar\" class=\"sidebar-wrapper toggled\">"
					+ "<div class=\"sidebar-content\" onclick=\"sideMenuAccess('MM_MANAGER')\">"
					+ "<div class=\"sidebar-menu\"><ul>"
					+ "<li class=\"\"><a href=\"#\" class=\"waves-effect waves-dark\">"
					+ "<span class=\"dxpHomeMenuIconClass\">"
					+ "<img src=\"images/Home-Iocn.png\" class=\"themeModeDark\" title=\"Home\"></span>"
					+ "<span class=\"menuTitle\">Home</span></a></li>" + "<li class=\"\"><a href=\"#\">"
					+ "<span class=\"dxpMenuImageClass\"><img src=\"images/Data-Analytics-icon.svg\" "
					+ "class=\"themeModeDark\" title=\"Data Analytics\"></span>"
					+ "<span class=\"menuTitle\">Data Analytics</span></a</li>"
					+ "<li class=\"\"><a href=\"#\"><span class=\"dxpMenuImageClass\">"
					+ "<img src=\"images/iTransform ETL.svg\" class=\"themeModeDark\" title=\"iTransform ETL\"></span>"
					+ "<span class=\"menuTitle\">iTransform ETL</span></a></li>"
					// + "<li class=\"\"><a href=\"#\"><span class=\"dxpMenuImageClass\">"
					// + "<img src=\"images/TaxonomyAndRepositories.svg\" class=\"themeModeDark\">"
					// + "</span><span class=\"menuTitle\">Repositories</span></a></li>"
					// + "<li class=\"\"><a href=\"#\"><span class=\"dxpMenuImageClass\">"
					// + "<img src=\"images/Data-Governance.svg\" class=\"themeModeDark\"></span>"
					// + "<span class=\"menuTitle\">Data Governance</span></a></li>"
					+ "<li class=\"isPoliticalSceince\"><a href=\"#\"><span class=\"dxpMenuImageClass\">"
					+ "<img src=\"images/Profile_Icon.png\" class=\"themeModeDark\" title=\"Profile\"></span>"
					+ "<span class=\"menuTitle\">Political Science</span></a></li>" + "</ul></div></div></nav>";
//            String homePageCarousel = "<section class='homepageCarouselSection'>"
//                    + "<div id='demo' class='carousel slide' data-ride='carousel'>"
//                    + "<ul class='carousel-indicators'>"
//                    + "<li data-target='#demo' data-slide-to='0' class='active'></li>"
//                    + "<li data-target='#demo' data-slide-to='1'></li></ul>"
//                    + "<div class='carousel-inner'><div class='carousel-item active'>"
//                    + "<img src='images/homePageCardImg1.jpg' width='1100' height='500'></div>"
//                    + "<div class='carousel-item'>"
//                    + "<img src='images/homePageCardImg2.jpg' width='1100' height='500'></div></div>"
//                    + "<a class='carousel-control-prev' href='#demo' data-slide='prev'>"
//                    + "<span class='carousel-control-prev-icon'></span></a>"
//                    + "<a class='carousel-control-next' href='#demo' data-slide='next'>"
//                    + "<span class='carousel-control-next-icon'></span></a></div></section>";
//            model.put("homePageCarousel", homePageCarousel);
			if (request.getSession(false) != null) {
				request.getSession(false).setAttribute("sideMenuStr", sideMenuStr);
			}
			String processVal = "";
			BApplProperties bapplPropertiesVal = (BApplProperties) loginHandlerService
					.getApplProperties("HOME_PAGE_TILES_SHOW_HIDE");
			if (bapplPropertiesVal != null) {
				processVal = bapplPropertiesVal.getId().getProcessValue();
			}

			String mainHeader = homePageDefaultString(request, processVal);
			model.put("mainHeader", mainHeader);
			model.put("sideMenuStr", sideMenuStr);
			model.put("secretKey", PilogEncryption.secretKey);
			HttpSession session = request.getSession(false);

			if (session != null) {
				resultObj = loginHandlerService.cloudLogout(request);
				try {
					session.invalidate();
				} catch (Exception e) {
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			resultObj.put("message", e.getLocalizedMessage());
			resultObj.put("flag", false);
		}
		return "Home";
	}

	// add this controller
	@RequestMapping(value = "/generateNewSession", method = { RequestMethod.POST, RequestMethod.GET })
	public @ResponseBody JSONObject generateNewSession(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String flag = request.getParameter("flag");
			if ("Y".equalsIgnoreCase(flag)) {
				// Get the current session
				HttpSession session = request.getSession(false);

				if (session != null) {
					// Invalidate the current session
					// session.invalidate();
				}

				// Create a new session
				HttpSession newSession = request.getSession(true);

				System.out.println("newLoginSession:::" + newSession.getId());
				resultObj.put("newLoginSession", newSession.getId());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	/*
	 * public String homePageDefaultString(HttpServletRequest request, String
	 * processVal) { String mainHeader = ""; try {
	 * 
	 * mainHeader = "<div calss='beforeloginPagebody' id='beforeloginPagebody'>\n" +
	 * "    <div class='login-header'>\n" + "        <div class='visionlogo'>\n" //+
	 * "            <img src = 'images/KSA2030.png' alt='pilogvision2023' class=\"pilogvisionlogo themeModeDark\" />\n"
	 * + "        </div>\n" + "<div class='pilog-logo'>\n"
	 * 
	 * + "            <div class='languageSelect'>\n" +
	 * "                <span class='arabicLanguage english-text' onclick=\"changetoArabicapp('arabic')\" style='color:#00ab67;font-size: 24px; !important'>العربية</span>\n"
	 * // +
	 * "                <span class='arabicLanguage arabic-text' onclick=\"changetoArabicapp('arabic')\" >العربية</span>\n"
	 * +
	 * "                <span class='englishLanguage arabic-text' onclick=\"changetoArabicapp('english')\" style='color:#0b4a99;'>English</span>\n"
	 * + "            </div>\n"
	 * 
	 * +
	 * "            <div class=''><img src='images/logo_red.png' class='pilog-logo-gif themeModeDark'/></div>\n"
	 * + "        </div>" // + "        <div class='languageSelect'>\n" // +
	 * "            <span class='arabicLanguage'>Arabic</span>\n" // +
	 * "            <span class='englishLanguage'>English</span>\n" // +
	 * "        </div>\n" + "    </div>\n" +
	 * "    <div class='container-fluid login-body-content'>" +
	 * "        <div class='row'>" +
	 * "            <div class='col-md-3 col-lg-3 niic-center-wrapper'>\n" +
	 * "                <div class=\"wrapper mb-8\">\n" +
	 * "                    <div class=\"logo\" id='toggleloginDiv'>\n" +
	 * "                        <span class='login-icon' style='width:23px;'><img src=\"images/Signin-icon.svg\" class=\"pilogcloudLogo themeModeDark\" alt=\"pilogcloud\" /></span><span class='login-text english-text'>Login</span><span class='login-text arabic-text'>تسجيل الدخول</span>\n"
	 * + "                    </div>\n" +
	 * "                    <div class=\"visionLoginpageInner\" id='visionLoginpageInner' style='display: none;'> \n"
	 * +
	 * "                        <span id='loginError' style='color:red;'></span> \n"
	 * +
	 * "                        <div class=\"form-field d-flex align-items-center userNameInputMainDiv\">\n"
	 * + "                            <div class=\"input-group-prepend\">\n" +
	 * "                                <span class=\"input-group-text\" id=\"basic-addon1\">\n"
	 * +
	 * "                                    <img src=\"images/userlogin.png\" class=\"themeModeDark\" alt=\"userlogin\"/>\n"
	 * + "                                </span>\n" +
	 * "                            </div>" +
	 * "                            <input type=\"text\" class='form-control' name=\"rsUsername\" id=\"rsUsername\" placeholder=\"Username\" autocomplete=\"off\" onkeypress=\"showorHideError('username')\">       \n"
	 * + "                        </div>\n" +
	 * "                        <span id='passwordError' style='color:red;'></span>\n"
	 * +
	 * "                        <div class=\"form-field d-flex align-items-center showorHidePassword\" style='display:none !important'>\n"
	 * + "<div class=\"input-group-prepend\">\n" +
	 * "                                <span class=\"input-group-text\" id=\"basic-addon1\">\n"
	 * +
	 * "                                    <img src=\"images/passwordicon.png\" class=\"themeModeDark\" alt=\"passwordicon\"/>\n"
	 * + "                                </span>\n" +
	 * "                            </div>" + "\n" +
	 * "                            <input type=\"password\" class='form-control' name=\"rsPassword\" id=\"rsPassword\" placeholder= \"Password\" data-man=\"M\" required=\"\" onkeypress=\"showorHideError('password')\">                    \n"
	 * +
	 * "                            <input type=\"hidden\" name=\"rsPasswordHid\" id=\"rsPasswordHid\" >\n"
	 * // +
	 * "                            <input type=\"hidden\" name=\"language\" id=\"language\" value=\"en_US\" > \n"
	 * + "                        </div>\n"; List langList = new ArrayList(); try {
	 * String processClass = "com.pilog.mdm.cloud.DAO.UserRegistrationDAO"; String
	 * processMethod = "getAllLanguages"; Class clazz = Class.forName(processClass);
	 * Class<?>[] paramTypes = {}; Method method =
	 * clazz.getMethod(processMethod.trim(), paramTypes); Object targetObj =
	 * appContext.getBean(clazz); langList = (List) method.invoke(targetObj); }
	 * catch (ClassNotFoundException cnfe) { cnfe.printStackTrace(); } catch
	 * (NoSuchMethodError nsme) { nsme.printStackTrace(); } catch (Exception e) {
	 * e.printStackTrace(); } if (langList != null && !langList.isEmpty()) { String
	 * defaultLocale = "en_US"; String langStr =
	 * "<select id=\"id_locale\" name=\"language\" onchange=\"getDefaultLang()\">";
	 * for (int l = 0; l < langList.size(); l++) { BLanguage bLang = (BLanguage)
	 * langList.get(l); if (bLang != null) { String selected = ""; String langCode =
	 * null; String countryCode = null; // String langCode =
	 * bLang.getLanguageCode(); // String countryCode = bLang.getCountryCode(); //
	 * String langName = bLang.getId().getName(); String langCountryCode = langCode
	 * + "_" + countryCode; if (langCountryCode != null &&
	 * !"".equalsIgnoreCase(langCountryCode) &&
	 * defaultLocale.equalsIgnoreCase(langCountryCode)) { selected = "selected"; }
	 * String langName = null; langStr += "<option value='" + langCountryCode + "' "
	 * + selected + ">" + langName + "</option>"; } } langStr += "</select>";
	 * mainHeader +=
	 * "<div class=\"form-field d-flex align-items-center LanguageSelect\" style='display:none !important'>"
	 * + "<div class=\"input-group-prepend\">" +
	 * "                                <span class=\"input-group-text\" id=\"basic-addon1\">\n"
	 * + "                                    Language   " +
	 * "                                </span>\n" +
	 * "                            </div>" + langStr +
	 * "                            <input type=\"hidden\" name=\"language\" id=\"language\" value=\"en_US\" >"
	 * + "                        </div>"; }
	 * 
	 * mainHeader +=
	 * "                        <div id=\"verifyLoginSecurity\" class=\"loginSecurity\">   \n"
	 * + "                        </div>\n" +
	 * "                        <button class=\"btn btn-primary btn-md btn-block waves-effect text-center m-b-20\" onclick=\"showNextPassword()\" id=\"showPassword\"><span class=\"english-text\">Next</span><span class=\"arabic-text\">مقبل</span></button>         \n"
	 * +
	 * "                        <button class=\"btn btn-primary btn-md btn-block waves-effect text-center m-b-20\" onclick=\"showorHideError('password', 'P')\" id=\"processLoginID2\" style='display:none;'><span class=\"english-text\">Sign In</span><span class=\"arabic-text\">تسجيل الدخول</span></button>        \n"
	 * + "\n" + "                    </div>\n" + "\n" +
	 * "                    <div class='signupClass' ><a href=\"#\" onclick=\"registerForm()\" class='signupRegisterClass'><span><img src=\"images/SignUp.png\" class=\"themeModeDark\" alt=\"\" width=\"22px\" /></span><span class=\"login-text english-text\">Register</span><span class=\"login-text arabic-text\">يسجل</span></a></div>\n"
	 * +
	 * "                    <div class='signupClass'><a href=\"#\" onclick=\"updatePassForm()\"><span class=\"login-icon\"><img src=\"images/passwordSet.png\" alt=\"\" class=\"themeModeDark\" width=\"22px\"/> </span><span class=\"login-text english-text\">Forgot password?</span><span class=\"login-text arabic-text\">هل نسيت كلمة السر؟</span></a></div>\n"
	 * + "\n" + "\n" + "                </div>\n"; if (processVal != null &&
	 * !"".equalsIgnoreCase(processVal) && !"null".equalsIgnoreCase(processVal) &&
	 * processVal.contains("ANNOUNCE")) {
	 * 
	 * 
	 * mainHeader += "<div class=\"pilog-announce mt-2\">" +
	 * "<div class=\"service_header\">" +
	 * "<h5 class=\"english-text\"><img src=\"images/megaphone-red.png\" style='width:18px;'><span class=\"dxpTitleAnnoncements\" "
	 * + "style='margin-left:12px;'> Visualization</span></h5>" +
	 * "<h5 class=\"arabic-text\"><img src=\"images/megaphone-red.png\" style='width:18px;'><span class=\"dxpTitleAnnoncements\" "
	 * + ">إعلانات </span></h5>" + "</div>" +
	 * "<div id=\"carouselExampleIndicatorsLeft\" class=\"carousel slide\" data-ride=\"carousel\">"
	 * + "                    <ol class=\"carousel-indicators\">" +
	 * "                        <li data-target=\"#carouselExampleIndicatorsLeft\" data-slide-to=\"0\" class=\"active\"></li>"
	 * // +
	 * "                        <li data-target=\"#carouselExampleIndicators\" data-slide-to=\"1\"></li>"
	 * // +
	 * "                        <li data-target=\"#carouselExampleIndicators\" data-slide-to=\"2\"></li>"
	 * + "                    </ol>" +
	 * "                    <div class=\"carousel-inner\">" +
	 * "                        <div class=\"carousel-item active\">" +
	 * "                            <img class=\"d-block w-100\" src=\"images/left_analytics.png\" alt=\"First slide\" class='themeModeDark'>"
	 * + "                        </div>" // +
	 * "                        <div class=\"carousel-item\">" // +
	 * "                            <img class=\"d-block w-100\" src=\"images/BannerKSA2.png\" alt=\"Second slide\" class='themeModeDark'>"
	 * // + "                        </div>" +
	 * "                        <div class=\"carousel-item\">" +
	 * "                            <img class=\"d-block w-100\" src=\"images/analytics_left1.png\" alt=\"Third slide\" class='themeModeDark'>"
	 * + "                        </div>" // +
	 * "                        <div class=\"carousel-item\">" // +
	 * "                            <img class=\"d-block w-100\" src=\"images/BannerKSA4.png\" alt=\"Fourth slide\" class='themeModeDark'>"
	 * // + "                        </div>" // +
	 * "                        <div class=\"carousel-item\">" // +
	 * "                            <img class=\"d-block w-100\" src=\"images/AnnouncementsKsa2.png\" alt=\"Second slide\" class='themeModeDark'>"
	 * // + "                        </div>" // +
	 * "                        <div class=\"carousel-item\">" // +
	 * "                            <img class=\"d-block w-100\" src=\"images/AnnouncementsKsa.png\" alt=\"Third slide\" class='themeModeDark'>"
	 * // + "                        </div>" // +
	 * "                        <div class=\"carousel-item\">" // +
	 * "                            <img class=\"d-block w-100\" src=\"images/BannerKSA4.png\" alt=\"Fourth slide\" class='themeModeDark'>"
	 * // + "                        </div>" + "                    </div>" +
	 * "                    <a class=\"carousel-control-prev\" href=\"#carouselExampleIndicatorsLeft\" role=\"button\" data-slide=\"prev\">"
	 * +
	 * "                        <span class=\"carousel-control-prev-icon\" aria-hidden=\"true\"></span>"
	 * + "                        <span class=\"sr-only\">Previous</span>" +
	 * "                    </a>" +
	 * "                    <a class=\"carousel-control-next\" href=\"#carouselExampleIndicatorsLeft\" role=\"button\" data-slide=\"next\">"
	 * +
	 * "                        <span class=\"carousel-control-next-icon\" aria-hidden=\"true\"></span>"
	 * + "                        <span class=\"sr-only\">Next</span>" +
	 * "                    </a>" + "                </div>" // +
	 * "<div class=\"dxpNewBannerImage\">" // +
	 * "<img src=\"images/AnnouncementsKsa.png\" class='themeModeDark english-text' alt=\"\" style='width:100%' !important;>"
	 * // +
	 * "<img src=\"greenimages/Announcements.png\" class='themeModeDark arabic-text' alt=\"\" style='width:100%' !important;>"
	 * // + "</div>" + "</div>";
	 * 
	 * 
	 * 
	 * // StringBuilder twitterList = loginHandlerService.getTwitterData(request);
	 * // mainHeader +=
	 * "<div class=\"trendsCols showCaseCard mb-8\" id=\"showCaseCardT\">" // +
	 * "<div class=\"trendscolumn\">" // +
	 * " <div class='card socialMediaCard' id='SocialMediaCardT'><div class='card-header'>"
	 * // +
	 * " <span class='imageClass' id='mediaCardImageClassT'><img src=\"images/twitter_spaceX.png\" class=\"card-img-top twitimg\" style='width:25px;'></span>"
	 * // + " <span class='titleClass' id='mediaCardtitleT'>Twitter</span>" // +
	 * "</div>" // +
	 * " <h5 class='topTrendsTwitter' id='appendTypeToCardT' style='display:none'>"
	 * // + " <sup class='twitterLastUpdated' id='twitterLastUpdatedT'></sup></h5>"
	 * // + " <div class='card-body' id='mediaLastUpdatedT'><div class='list-body'>"
	 * // +
	 * " <div class='list-group main-list-group-card-div' id='listofGroupFiltersIdT' style='display:none;'></div>"
	 * // + " <div class='listofTrendsOuter' id='pilogListofTrendsOuterT'>" // +
	 * "<ul class=\"listofTrendsInner\" id=\"SocialMediaCardTGroupFilter\">" // + ""
	 * + twitterList + "" // + "</ul>" // + "</div>" // + " </div>" // + " </div>"
	 * // + " <div class='card-footer' id='cardFooterMainDivT'>" // + " </div>" // +
	 * " </div>" // + "</div></div>"; } // if (processVal != null &&
	 * !"".equalsIgnoreCase(processVal) && !"null".equalsIgnoreCase(processVal) &&
	 * processVal.contains("ANNOUNCE")) { // // StringBuilder twitterList =
	 * loginHandlerService.getTwitterData(request); // mainHeader +=
	 * "<div class=\"trendsCols showCaseCard mb-8\" id=\"showCaseCardT\">" // +
	 * "<div class=\"trendscolumn\">" // +
	 * " <div class='card socialMediaCard' id='SocialMediaCardT'><div class='card-header'>"
	 * // +
	 * " <span class='imageClass' id='mediaCardImageClassT'><img src=\"images/twitter_spaceX.png\" class=\"card-img-top twitimg\" style='width:25px;'></span>"
	 * // + " <span class='titleClass' id='mediaCardtitleT'>Twitter</span>" // +
	 * "</div>" // +
	 * " <h5 class='topTrendsTwitter' id='appendTypeToCardT' style='display:none'>"
	 * // + " <sup class='twitterLastUpdated' id='twitterLastUpdatedT'></sup></h5>"
	 * // + " <div class='card-body' id='mediaLastUpdatedT'><div class='list-body'>"
	 * // +
	 * " <div class='list-group main-list-group-card-div' id='listofGroupFiltersIdT' style='display:none;'></div>"
	 * // + " <div class='listofTrendsOuter' id='pilogListofTrendsOuterT'>" // +
	 * "<ul class=\"listofTrendsInner\" id=\"SocialMediaCardTGroupFilter\">" // + ""
	 * + twitterList + "" // + "</ul>" // + "</div>" // + " </div>" // + " </div>"
	 * // + " <div class='card-footer' id='cardFooterMainDivT'>" // + " </div>" // +
	 * " </div>" // + "</div></div>"; // } if (processVal != null &&
	 * !"".equalsIgnoreCase(processVal) && !"null".equalsIgnoreCase(processVal) &&
	 * processVal.contains("CALENDER")) { DateTimeFormatter dtf =
	 * DateTimeFormatter.ofPattern("dd"); LocalDateTime now = LocalDateTime.now();
	 * Date dt = new Date(); Date tomorrow = new Date(dt.getTime() + (1000 * 60 * 60
	 * * 24)); Date datomorrow = new Date(dt.getTime() + (1000 * 60 * 60 * 48));
	 * SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy"); String
	 * currentDate = format.format(dt); String currentDate1 =
	 * format.format(tomorrow); String currentDate2 = format.format(datomorrow);
	 * List dateList = new ArrayList(); dateList.add(currentDate);
	 * dateList.add(currentDate1); dateList.add(currentDate2); List eventsList =
	 * loginHandlerService.getEvents(request, dateList); mainHeader +=
	 * "<div class='login-custom-card statis-counts'>\n" +
	 * "                    <div class=\"service_header\">\n" +
	 * "                        <h5 class=\"english-text\"><img src=\"images/Events-Icon.png\" class=\"visionLoginPageNIICEventsClass\" width=\"22px\"></img> Events</h5>\n"
	 * +
	 * "                        <h5 class=\"arabic-text\"><img src=\"images/Events-Icon.png\" class=\"visionLoginPageNIICEventsClass\" width=\"22px\"></img> خبر</h5>\n"
	 * + "                    </div>\n" +
	 * "                    <div class=\"pilog-event-wrapper\">\n" +
	 * "                        <div class=\"\" id=\"pilog-eventId\">\n"; if
	 * (eventsList != null && !eventsList.isEmpty()) { for (int i = 0; i <
	 * eventsList.size(); i++) { Object[] objData = (Object[]) eventsList.get(i); if
	 * (objData != null) { String eventDate = (String) objData[2]; Date date =
	 * format.parse(eventDate); int eventDay = date.getDate(); int currentDay =
	 * dt.getDate(); int diff = eventDay - currentDay; String dateTime = ""; if
	 * (diff == 0) { dateTime = "Today"; } else if (diff == 1) { dateTime = diff +
	 * " day to go"; } else { dateTime = diff + " day' to go"; } mainHeader +=
	 * "                            <div class=\"row m-0\">\n" +
	 * "                               <div class=\"card eventCardMainDiv\" onclick=\"eventCalendarData1('')\"> "
	 * + "                                  <div class=\"evenTitleAnd_Date \">" +
	 * "                                  <div class=\"calIcon\"><i class=\"fa fa-calendar\" aria-hidden=\"true\"></i></div>"
	 * +
	 * "                                  <div class=\"dateShow\"><span id=\"eventDateId\"> "
	 * + objData[2] + "</span></div>" +
	 * "                                  <div class=\"titleClass fa fa-clock-o\"><span>"
	 * + objData[1] + "</span></div>" + "                                  </div>" +
	 * "                                  <div class=\"card-body eventDiscriptionDiv\"><h5>"
	 * + objData[0] + "</h5>" +
	 * "                                  <div class=\"bottomClass\"> <span>" +
	 * dateTime + "</span></div></div></div>\n" +
	 * "                            </div>\n"; } } } else {
	 * 
	 * mainHeader += "                            <div class=\"row m-0\">\n" +
	 * "                               <div class=\"card eventCardMainDiv\" onclick=\"eventCalendarData1('')\"> "
	 * + "                                  <div class=\"evenTitleAnd_Date \">" +
	 * "                                  <div class=\"calIcon\"><i class=\"fa fa-calendar\" aria-hidden=\"true\"></i></div>"
	 * +
	 * "                                  <div class=\"dateShow\"><span id=\"eventDateId\"> "
	 * + currentDate + "</span></div>" +
	 * "                                  <div class=\"titleClass fa fa-clock-o\"><span>00:00</span></div>"
	 * + "                                  </div>" +
	 * "                                  <div class=\"card-body eventDiscriptionDiv\"><h5>No Events</h5>"
	 * +
	 * "                                  <div class=\"bottomClass\"> <span>Today</span></div></div></div>\n"
	 * + "                            </div>\n"; } // +
	 * "                            <div class=\"\">\n" // +
	 * "<div class=\"card eventCardMainDiv\"  onclick=\"eventCalendarData('')\"> <div class=\"evenTitleAnd_Date \"><div class=\"calIcon\"><i class=\"fa fa-calendar\" aria-hidden=\"true\"></i></div><div class=\"dateShow\"><span id=\"eventDateId\">"
	 * + dtf.format(now) + "th," + currentDate +
	 * "</span></div><div class=\"titleClass fa fa-clock-o\"><span>00:00</span></div></div><div class=\"card-body eventDiscriptionDiv\"><h5>OPS meeting</h5><p></p><div class=\"bottomClass\"> <span>see more..</span></div></div></div>"
	 * // + "                            </div>\n" // +
	 * "                            <div class=\"\">\n" // +
	 * "<div class=\"card eventCardMainDiv\"  onclick=\"eventCalendarData('')\"> <div class=\"evenTitleAnd_Date \"><div class=\"calIcon\"><i class=\"fa fa-calendar\" aria-hidden=\"true\"></i></div><div class=\"dateShow\"><span id=\"eventDateId\">"
	 * + dtf.format(now) + "th," + currentDate +
	 * "</span></div><div class=\"titleClass fa fa-clock-o\"><span>00:00</span></div></div><div class=\"card-body eventDiscriptionDiv\"><h5>JAVA Meeting</h5><p>Java Standup</p><div class=\"bottomClass\"> <span>see more..</span></div></div></div>"
	 * // + "                            </div>\n" // +
	 * "                            <div class=\"\">\n" // +
	 * "<div class=\"card eventCardMainDiv\"  onclick=\"eventCalendarData('')\"> <div class=\"evenTitleAnd_Date \"><div class=\"calIcon\"><i class=\"fa fa-calendar\" aria-hidden=\"true\"></i></div><div class=\"dateShow\"><span id=\"eventDateId\">"
	 * + dtf.format(now) + "th," + currentDate +
	 * "</span></div><div class=\"titleClass fa fa-clock-o\"><span>00:00</span></div></div><div class=\"card-body eventDiscriptionDiv\"><h5>MDRM Changes</h5><p>Vulnerabulities</p><div class=\"bottomClass\"> <span>see more..</span></div></div></div>"
	 * // + "                            </div>\n" // +
	 * "                            <div class=\"\">\n" // +
	 * "<div class=\"card eventCardMainDiv\"  onclick=\"eventCalendarData('')\"> <div class=\"evenTitleAnd_Date \"><div class=\"calIcon\"><i class=\"fa fa-calendar\" aria-hidden=\"true\"></i></div><div class=\"dateShow\"><span id=\"eventDateId\">"
	 * + dtf.format(now) + "th," + currentDate +
	 * "</span></div><div class=\"titleClass fa fa-clock-o\"><span>00:00</span></div></div><div class=\"card-body eventDiscriptionDiv\"><h5>ETL Changes</h5><p>Toad Functionality</p><div class=\"bottomClass\"> <span>see more..</span></div></div></div>"
	 * // + "                            </div>\n" mainHeader +=
	 * "                        </div>\n" + "                    </div>   \n" +
	 * "                </div>" + "            </div>\n"; } if (processVal != null
	 * && !"".equalsIgnoreCase(processVal) && !"null".equalsIgnoreCase(processVal)
	 * && processVal.contains("CUSTOMERS")) { // mainHeader +=
	 * "<div class=\"pilog-announce mt-2\">" // + "<div class=\"service_header\">"
	 * // +
	 * "<h5 class='english-text'><img src=\"images/customerKSA.png\" style='width:22px;' class='themeModeDark'>"
	 * // +
	 * "<span class=\"dxpTitleAnnoncements\" style='margin-left:12px;'> Customers/Clients</span>"
	 * // + "</h5>" // +
	 * "<h5 class='arabic-text'><img src=\"greenimages/customerKSA.png\" style='width:22px;' class='themeModeDark'>"
	 * // +
	 * "<span class=\"dxpTitleAnnoncements\" style='margin-left:12px;'> العملاء / العملاء</span>"
	 * // + "</h5>" // + "</div>" // +
	 * "                    <div class=\"pilog-clients\">" // +
	 * "                        <div>" // +
	 * "                            <div class=\"card custom-card \">                         "
	 * // + "                                <div class=\"card-body\">" // +
	 * "                                    <div class=\"mainKsaimage\"><img src=\"images/JSW-logo.png\" class='themeModeDark' width='100px'></div>"
	 * // +
	 * "                                    <span class=\"card-title english-text\">JSW</span><span class=\"card-title arabic-text\">جي إس دبليو</span>"
	 * // + "                                </div>" // +
	 * "                            </div>" // + "                        </div>" //
	 * + "                        <div>" // +
	 * "                            <div class=\"card custom-card\">" // +
	 * "                                <div class=\"card-body\">" // +
	 * "                                    <div class=\"mainKsaimage\"><img src=\"images/adani.png\" class='themeModeDark' width='100px'></div>"
	 * // +
	 * "                                    <span class=\"card-title english-text\">Adani</span><span class=\"card-title arabic-text\">أداني</span>"
	 * // + "                                </div>" // +
	 * "                            </div>" // + "                        </div> "
	 * // + "                        <div>" // +
	 * "                            <div class=\"card custom-card \">" // +
	 * "                                <div class=\"card-body\">" // +
	 * "                                    <div class=\"mainKsaimage\"><img src=\"images/samsung.png\" class='themeModeDark' width='100px'></div>"
	 * // +
	 * "                                    <span class=\"card-title english-text\">Samsung</span><span class=\"card-title arabic-text\">سامسونج</span>"
	 * // + "                                </div>" // +
	 * "                            </div>" // + "                        </div>" //
	 * + "                        <div>" // +
	 * "                            <div class=\"card custom-card \">" // +
	 * "                                <div class=\"card-body\">" // +
	 * "                                    <div class=\"mainKsaimage\"><img src=\"images/sabic.png\" class='themeModeDark' width='100px'></div>"
	 * // +
	 * "                                    <span class=\"card-title english-text\">Sabic</span><span class=\"card-title arabic-text\">سابك</span>"
	 * // + "                                </div>" // +
	 * "                            </div>" // + "                        </div>" //
	 * + "                        <div>" // +
	 * "                            <div class=\"card custom-card \">" // +
	 * "                                <div class=\"card-body\">" // +
	 * "                                    <div class=\"mainKsaimage\"><img src=\"images/piramal.png\" class='themeModeDark' width='100px'></div>"
	 * // +
	 * "                                    <span class=\"card-title english-text\">Piramal</span><span class=\"card-title arabic-text\">بيرامال</span>"
	 * // + "                                </div>" // +
	 * "                            </div>" // + "                        </div>" //
	 * + "                        <div>" // +
	 * "                            <div class=\"card custom-card \">" // +
	 * "                                <div class=\"card-body\">" // +
	 * "                                    <div class=\"mainKsaimage\"><img src=\"images/orpic.png\" class='themeModeDark' width='100px'></div>"
	 * // +
	 * "                                    <span class=\"card-title english-text\">Orpic</span><span class=\"card-title arabic-text\">أوربك</span>"
	 * // + "                                </div>" // +
	 * "                            </div>" // + "                        </div>" //
	 * + "                        <div>" // +
	 * "                            <div class=\"card custom-card \">" // +
	 * "                                <div class=\"card-body\">" // +
	 * "                                    <div class=\"mainKsaimage\"><img src=\"images/coca-cola.png\" class='themeModeDark' width='100px'></div>"
	 * // +
	 * "                                    <span class=\"card-title english-text\">Coca Cola</span><span class=\"card-title arabic-text\">الكوك</span>"
	 * // + "                                </div>" // +
	 * "                            </div>" // + "                        </div>" //
	 * + "                        <div>" // +
	 * "                            <div class=\"card custom-card \">" // +
	 * "                                <div class=\"card-body\">" // +
	 * "                                    <div class=\"mainKsaimage\"><img src=\"images/anglo-american.png\" class='themeModeDark' width='100px'></div>"
	 * // +
	 * "                                    <span class=\"card-title english-text\">Anglo American</span><span class=\"card-title arabic-text\">الأنجلو أمريكي</span>"
	 * // + "                                </div>" // +
	 * "                            </div>" // + "                        </div>" //
	 * + "                        <div>" // +
	 * "                            <div class=\"card custom-card \">" // +
	 * "                                <div class=\"card-body\">" // +
	 * "                                    <div class=\"mainKsaimage\"><img src=\"images/abu-dhabi-airports.png\" class='themeModeDark' width='100px'></div>"
	 * // +
	 * "                                    <span class=\"card-title english-text\">Abu Dhabi Airports</span><span class=\"card-title arabic-text\">مطارات أبوظبي</span>"
	 * // + "                                </div>" // +
	 * "                            </div>" // + "                        </div>" //
	 * + "                        <div>" // +
	 * "                            <div class=\"card custom-card \">" // +
	 * "                                <div class=\"card-body\">" // +
	 * "                                    <div class=\"mainKsaimage\"><img src=\"images/aramco.png\" class='themeModeDark' width='100px'></div>"
	 * // +
	 * "                                    <span class=\"card-title english-text\">Aramco</span><span class=\"card-title arabic-text\">أرامكو</span>"
	 * // + "                                </div>" // +
	 * "                            </div>" // + "                        </div>" //
	 * + "                        <div>" // +
	 * "                            <div class=\"card custom-card \">" // +
	 * "                                <div class=\"card-body\">" // +
	 * "                                    <div class=\"mainKsaimage\"><img src=\"images/npcc.png\" class='themeModeDark' width='100px'></div>"
	 * // +
	 * "                                    <span class=\"card-title english-text\">NPCC</span><span class=\"card-title arabic-text\">إن بي سي سي</span>"
	 * // + "                                </div>" // +
	 * "                            </div>" // + "                        </div>" //
	 * + "                        <div>" // +
	 * "                            <div class=\"card custom-card \">" // +
	 * "                                <div class=\"card-body\">" // +
	 * "                                    <div class=\"mainKsaimage\"><img src=\"images/ultratech-cement.png\" class='themeModeDark' width='100px'></div>"
	 * // +
	 * "                                    <span class=\"card-title english-text\">Ultratech Cement</span><span class=\"card-title arabic-text\">أسمنت التراتك</span>"
	 * // + "                                </div>" // +
	 * "                            </div>" // + "                        </div>" //
	 * + "                        <div>" // +
	 * "                            <div class=\"card custom-card \">" // +
	 * "                                <div class=\"card-body\">" // +
	 * "                                    <div class=\"mainKsaimage\"><img src=\"images/wonder-cement.png\" class='themeModeDark' width='100px'></div>"
	 * // +
	 * "                                    <span class=\"card-title english-text\">Wonder Cement</span><span class=\"card-title arabic-text\">وندر للأسمنت</span>"
	 * // + "                                </div>" // +
	 * "                            </div>" // + "                        </div>" //
	 * // + "</div>" // + "</div>" // + "</div>"; } //mainHeader +=
	 * "            </div>" mainHeader +=
	 * "            <div class='col-md-6 col-lg-6 niic-center-wrapper'>"; if
	 * (processVal != null && !"".equalsIgnoreCase(processVal) &&
	 * !"null".equalsIgnoreCase(processVal) && processVal.contains("SERVICES")) {
	 * mainHeader += "                <div class='login-custom-card mb-8'>" +
	 * "                    <div class=\"service_header\">" +
	 * "                        <h5 class='english-text'><img src=\"images/MostUsedServices.png\" title=\"Frequently Used Services\" style='width:18px;'><span style='margin-left:5px;'>Frequently Used Services<span></h5>"
	 * +
	 * "                        <h5 class='arabic-text'><img src=\"images/MostUsedServices.png\" title=\"Frequently Used Services\" style='width:18px;'> خدمات  الأكثر استخدامًا</h5>"
	 * + "                    </div>" +
	 * "                    <div class=\"service-statistics\">" +
	 * "                        <div>" +
	 * "                            <div class=\"card custom-card\" onclick=\"showCardBasedVideo('VENDOR','How to Register a Vendor?')\">"
	 * +
	 * "                                <div class=\"card-body allServicesClass\">"
	 * +
	 * "                                    <div class=\"circle-image\"><img src=\"images/createChart.png\"></div>"
	 * +
	 * "                                    <h5 class=\"card-title english-text\">How to Create Chart?</h5>"
	 * +
	 * "                                    <h5 class=\"card-title arabic-text\">كيفية تسجيل بائع؟</h5>"
	 * +
	 * "<div class=\"card-icons-wrapper\"><a><img src=\"images/SearchHelp.png\" alt=\"SearchHelp\"></a><a><img src=\"images/SearchVideoPlay.png\" alt=\"SearchVideoPlay\"></a></div>"
	 * + "<div class=\"showMaterialpopUpBox\">" +
	 * "                                         <div class=\"media border p-2\">" +
	 * "                                         <img src=\"images/dashboard.png\" alt=\"\" class=\"popupInnerImgShowClass mr-2 mt-1 \" style=\"width:40px;\">"
	 * +
	 * "                                         <div class=\"media-body\"><h4>Vendor</h4><small>How to create Dashboard?</small></div>"
	 * + "                                         </div>" +
	 * "                                     </div>" +
	 * "                                </div>" +
	 * "                            </div>" + "                        </div>" +
	 * "                        <div>" +
	 * "                            <div class=\"card custom-card\" onclick=\"showCardBasedVideo('ONBOARD','OnBoard Vendor')\">"
	 * +
	 * "                                <div class=\"card-body allServicesClass\">"
	 * +
	 * "                                    <div class=\"circle-image\"><img src=\"images/dashboard1.png\"></div>"
	 * +
	 * "                                    <h5 class=\"card-title english-text\">How to Create Dashboard ?</h5>"
	 * +
	 * "                                    <h5 class=\"card-title arabic-text\">كيفية إعداد البائع؟</h5>"
	 * +
	 * "<div class=\"card-icons-wrapper\"><a><img src=\"images/SearchHelp.png\" alt=\"SearchHelp\"></a><a><img src=\"images/SearchVideoPlay.png\" alt=\"SearchVideoPlay\"></a></div>"
	 * + "		<div class=\"showMaterialpopUpBox\">" +
	 * "                                         <div class=\"media border p-2\">" +
	 * "                                         <img src=\"images/VendorOnBoard.png\" alt=\"\" class=\"popupInnerImgShowClass mr-2 mt-1 \" style=\"width:40px;\">"
	 * +
	 * "                                         <div class=\"media-body\"><h4>OnBoard</h4><small>How to create dashboard</small></div>"
	 * + "                                         </div>" +
	 * "                                     </div>" +
	 * "                                </div>" +
	 * "                            </div>" + "                        </div> " +
	 * "                        <div>" +
	 * "                            <div class=\"card custom-card\" onclick=\"showCardBasedVideo('PRODUCT','Register Product')\">"
	 * +
	 * "                                <div class=\"card-body allServicesClass\">"
	 * +
	 * "                                    <div class=\"circle-image\"><img src=\"images/import.png\"></div>"
	 * +
	 * "                                    <h5 class=\"card-title english-text\">How to Import Data?</h5> "
	 * +
	 * "                                    <h5 class=\"card-title arabic-text\">كيفية تسجيل المنتج؟</h5> "
	 * +
	 * "<div class=\"card-icons-wrapper\"><a><img src=\"images/SearchHelp.png\" alt=\"SearchHelp\"></a><a><img src=\"images/SearchVideoPlay.png\" alt=\"SearchVideoPlay\"></a></div>"
	 * + "<div class=\"showMaterialpopUpBox\">" +
	 * "                                         <div class=\"media border p-2\">" +
	 * "                                         <img src=\"images/MaterialMaster.png\" alt=\"\" class=\"popupInnerImgShowClass mr-2 mt-1 \" style=\"width:40px;\">"
	 * +
	 * "                                         <div class=\"media-body\"><h4>Product</h4><small>How to Import Data?</small></div>"
	 * + "                                         </div>" +
	 * "                                     </div>" +
	 * "                                </div>" +
	 * "                            </div>" + "                        </div>   " //
	 * + "                        <div>" // +
	 * "                            <div class=\"card custom-card\" onclick=\"showCardBasedVideo('SERVICE','How to Register a Service')\">"
	 * // +
	 * "                                <div class=\"card-body allServicesClass\">"
	 * // +
	 * "                                    <div class=\"circle-image\"><img src=\"images/ServiceMaster.png\"></div>"
	 * // +
	 * "                                    <h5 class=\"card-title english-text\">How to Register Service?</h5>"
	 * // +
	 * "                                    <h5 class=\"card-title arabic-text\">كيفية تسجيل خدمة؟</h5>"
	 * // +
	 * "<div class=\"card-icons-wrapper\"><a><img src=\"images/ChatIcon.png\" alt=\"ChatIcon\"></a><a><img src=\"images/SearchHelp.png\" alt=\"SearchHelp\"></a><a><img src=\"images/SearchVideoPlay.png\" alt=\"SearchVideoPlay\"></a><a><img src=\"images/SearchGif.png\" alt=\"SearchGif\"></a></div>"
	 * // + "<div class=\"showMaterialpopUpBox\">" // +
	 * "                                         <div class=\"media border p-2\">"
	 * // +
	 * "                                         <img src=\"images/ServiceMaster.png\" alt=\"\" class=\"popupInnerImgShowClass mr-2 mt-1 \" style=\"width:40px;\">"
	 * // +
	 * "                                         <div class=\"media-body\"><h4>Service</h4><small>How to Register Service?</small></div>"
	 * // + "                                         </div>" // +
	 * "                                     </div>" // +
	 * "                                </div>" // +
	 * "                            </div>" // + "                        </div>" //
	 * + "                        <div>" // +
	 * "                            <div class=\"card custom-card\" onclick=\"showCardBasedVideo('ASSET','How to Register a Asset')\">"
	 * // +
	 * "                                <div class=\"card-body allServicesClass\">"
	 * // +
	 * "                                    <div class=\"circle-image\"><img src=\"images/AssetMaster.png\"></div>"
	 * // +
	 * "                                    <h5 class=\"card-title english-text\">How to Register Asset?</h5>"
	 * // +
	 * "                                    <h5 class=\"card-title arabic-text\">كيفية تسجيل الأصول؟</h5>"
	 * // +
	 * "<div class=\"card-icons-wrapper\"><a><img src=\"images/ChatIcon.png\" alt=\"ChatIcon\"></a><a><img src=\"images/SearchHelp.png\" alt=\"SearchHelp\"></a><a><img src=\"images/SearchVideoPlay.png\" alt=\"SearchVideoPlay\"></a><a><img src=\"images/SearchGif.png\" alt=\"SearchGif\"></a></div>"
	 * // + "<div class=\"showMaterialpopUpBox\">" // +
	 * "                                         <div class=\"media border p-2\">"
	 * // +
	 * "                                         <img src=\"images/AssetMaster.png\" alt=\"\" class=\"popupInnerImgShowClass mr-2 mt-1 \" style=\"width:40px;\">"
	 * // +
	 * "                                         <div class=\"media-body\"><h4>Asset</h4><small>How to Register Asset?</small></div>"
	 * // + "                                         </div>" // +
	 * "                                     </div>" // +
	 * "                                </div>" // +
	 * "                            </div>" // + "                        </div>" +
	 * "                    </div>" + "                </div>"; } if (processVal !=
	 * null && !"".equalsIgnoreCase(processVal) &&
	 * !"null".equalsIgnoreCase(processVal) && processVal.contains("STATS")) { int
	 * productCount = loginHandlerService.getCountBasedOnDomainName(request,
	 * "PRODUCT_COUNT"); int vendorCount =
	 * loginHandlerService.getCountBasedOnDomainName(request, "VENDOR_COUNT"); int
	 * classesCount = loginHandlerService.getCountBasedOnDomainName(request,
	 * "CLASSES_COUNT"); int pprCount =
	 * loginHandlerService.getCountBasedOnDomainName(request, "PPR_COUNT"); // int
	 * unspscCount = loginHandlerService.getCountBasedOnDomainName(request,
	 * "UNSPSC_COUNT"); // int hsnCount =
	 * loginHandlerService.getCountBasedOnDomainName(request, "HSN_COUNT"); // int
	 * natoCount = loginHandlerService.getCountBasedOnDomainName(request,
	 * "NATO_COUNT"); mainHeader +=
	 * "                  <div class='login-custom-card statis-counts mb-8'>" +
	 * "                    <div class=\"service_header\">" +
	 * "                        <h5 class='english-text'><img src=\"images/statsAndFacts.png\" title=\"stats & Facts\" style='width:18px;'><span style='margin-left:5px;'>Facts & Stats</span></h5>"
	 * +
	 * "                        <h5 class='arabic-text'><img src=\"images/statsAndFacts.png\" title=\"stats & Facts\" style='width:18px;'>سجل المنتجات في المملكة العربية السعودية</h5>"
	 * + "                    </div>" +
	 * "                    <div class=\"facts_stats\">" +
	 * "                        <div class=\"card login-custom-card\" onclick=\"setFactsAndStatsChartsData('PRODUCT_COUNT', 'pie')\">                         "
	 * + "                            <div class=\"card-body\">" +
	 * "                                <div class=\"circle-image\"><img src=\"images/Product.png\" aria-hidden=\"true\"> "
	 * + "                                 <div>" +
	 * "                                     <h5 class=\"card-title\">0.7 Millions</h5>"
	 * +
	 * "<p class=\"card-title english-text\">Clients </p><p class=\"card-title arabic-text\">المنتجات المسجلة</p></div>"
	 * + "                                </div>" +
	 * "                            </div>" + "                        </div>" +
	 * "                        <div class=\"card login-custom-card\" onclick=\"setFactsAndStatsChartsData('VENDOR_COUNT', 'column')\">                       "
	 * + "                            <div class=\"card-body\">" +
	 * "                                <div class=\"circle-image\"><img src=\"images/profile.png\" aria-hidden=\"true\">"
	 * + "                                 <div>" +
	 * "                                      <h5 class=\"card-title\">" +
	 * convertNumToCharNameFormat(vendorCount) +
	 * "</h5><p class=\"card-title english-text\">Users</p><p class=\"card-title arabic-text\">البائعين</p></div>"
	 * + "                                </div>" +
	 * "                            </div>" + "                        </div>" +
	 * "                       " +
	 * "                        <div class=\"card login-custom-card\" onclick=\"setFactsAndStatsChartsData('CLASSES_COUNT', 'pie')\">"
	 * + "                            <div class=\"card-body\">" +
	 * "                                <div class=\"circle-image\"><img src=\"images/dashboard1.png\" aria-hidden=\"true\"> "
	 * + "                                 <div>" +
	 * "                                     <h5 class=\"card-title\">" +
	 * convertNumToCharNameFormat(classesCount) +
	 * "</h5><p class=\"card-title english-text\">Dashboards</p><p class=\"card-title arabic-text\">قوالب المنتجات</p></div>"
	 * + "                                 </div>" +
	 * "                            </div>" + "                        </div>" +
	 * "                        <div class=\"card login-custom-card\" onclick=\"setFactsAndStatsChartsData('PPR_COUNT', 'column')\">                        "
	 * + "                            <div class=\"card-body\">" +
	 * "                                <div class=\"circle-image\"><img src=\"images/createChart.png\" aria-hidden=\"true\">"
	 * + "                                 <div>" +
	 * "                                      <h5 class=\"card-title\">" +
	 * convertNumToCharNameFormat(pprCount) +
	 * "</h5><p class=\"card-title english-text\">Charts</p><p class=\"card-title arabic-text\">مستودع المنتج</p></div>"
	 * + "                                </div>" +
	 * "                            </div>" + "                        </div>" // +
	 * "                        <div class=\"card login-custom-card\" onclick=\"setFactsAndStatsChartsData('NATO_CODE', 'pie')\">                        "
	 * // + "                            <div class=\"card-body\">" // +
	 * "                                <div class=\"circle-image\"><img src=\"images/nato_icon.png\" aria-hidden=\"true\"> "
	 * // + "                                 <div>" // +
	 * "                                     <h5 class=\"card-title\">" +
	 * convertNumToCharNameFormat(natoCount) +
	 * "</h5><p class=\"card-title english-text\">NATO Codes</p><p class=\"card-title arabic-text\">رموز الناتو</p></div>"
	 * // + "                                </div>" // +
	 * "                            </div>" // + "                        </div>" +
	 * "                    </div>   " + "                </div>";
	 * 
	 * } if (processVal != null && !"".equalsIgnoreCase(processVal) &&
	 * !"null".equalsIgnoreCase(processVal) && processVal.contains("CALENDER")) {
	 * 
	 * mainHeader += "<div class='login-custom-card statis-counts'>\n" +
	 * "                    <div class=\"service_header\">\n" +
	 * "                        <h5 class=\"english-text\"><img src=\"images/News.png\" title=\"News Feeding\" style='width:18px;'><span style='margin-left:5px;'>News Feeding</span></h5>\n"
	 * +
	 * "                        <h5 class=\"arabic-text\"><img src=\"images/News.png\" title=\"News Feeding\" style='width:18px;'> خبر</h5>\n"
	 * + "                    </div>\n" + "                    <div class=\"\">\n" +
	 * "                        <div class=\"pilog-news-wrapper\">\n" +
	 * "                            <div class=\"pilog-date-news row m-0\">\n" +
	 * "                                <div class=\"pilog-date col-lg-3 col-md-3 col-sm-4 col-xs-12\">\n"
	 * + "                                    <div class=\"showNewsData\">\n" +
	 * "<a class=\"english-text\" href=\"https://www.my.gov.sa/wps/portal/snp/content/news\" target='_blank'><img class=\"showDataImageClass\" src=\"images/news1.png\" class='themeModeDark' style='width: 85px;'></a>"
	 * + "                                    </div>\n" +
	 * "                                \n" +
	 * "                                </div>\n" +
	 * "                                <div class=\"pilog-news col-lg-9 col-md-9 col-sm-8 col-xs-12\">\n"
	 * +
	 * "                                    <a class=\"english-text\" href=\"https://www.my.gov.sa/wps/portal/snp/content/news\" target='_blank'><h6 class=\"english-text\">SDAIA, NTDP Launch Groundbreaking AI Accelerator GAIA</h6></a>\n"
	 * +
	 * "                                    <h6 class=\"arabic-text\">منصة PiLog تحذر من عمليات الاحتيال والروابط المشبوهة</h6>\n"
	 * +
	 * "                                    <a class=\"english-text\" href=\"https://www.my.gov.sa/wps/portal/snp/content/news\" target='_blank'><p class=\"english-text\">Riyadh, SPA: Saudi Data and Artificial Intelligence Authority (SDAIA) and the National Technology Development Program (NTDP)</p></a>\n"
	 * +
	 * "                                    <p class=\"arabic-text\">حذرت البوابة الإلكترونية لوزارة الداخلية PiLog من ذلك</p>\n"
	 * +
	 * "                                    <a class=\"english-text\" href=\"https://www.my.gov.sa/wps/portal/snp/content/news\" target='_blank'>More...</a>\n"
	 * +
	 * "                                  <a class=\"arabic-text\" href=\"https://www.my.gov.sa/wps/portal/snp/content/news\" target='_blank'>أكثر...</a>\n"
	 * + "                                </div>\n" +
	 * "                            </div>\n" +
	 * "                            <div class=\"pilog-date-news\">\n" +
	 * "                                <div class=\"pilog-date col-lg-3 col-md-3 col-sm-4 col-xs-12\">\n"
	 * + "                                      <div class=\"showNewsData\">\n" +
	 * "<a class=\"english-text\" href=\"https://www.my.gov.sa/wps/portal/snp/content/news\" target='_blank'><img class=\"showDataImageClass\" src=\"images/news2.png\" class='themeModeDark' style='width: 85px;'></a>"
	 * + "                                    </div>\n" +
	 * "                                </div>\n" +
	 * "                                <div class=\"pilog-news col-lg-9 col-md-9 col-sm-8 col-xs-12\">\n"
	 * +
	 * "                                 <a class=\"english-text\" href=\"https://www.my.gov.sa/wps/portal/snp/content/news\" target='_blank'><h6 class=\"english-text\">Saudi Arabia Concludes its Participation in IDEF 2023 in Istanbul</h6></a>\n"
	 * +
	 * "                                 <h6 class=\"arabic-text\">أنظمة منصة PiLog المجدولة للتحديثات التالية</h6>\n"
	 * +
	 * "                                    <a class=\"english-text\" href=\"https://www.my.gov.sa/wps/portal/snp/content/news\" target='_blank'><p class=\"english-text\">Riyadh, SPA: The Kingdom of Saudi Arabia concluded its participation in the International Defense Industries Fair \"IDEF 2023\"</p></a>\n"
	 * +
	 * "                                    <p class=\"arabic-text\">ستقوم منصة أبشر الإلكترونية التابعة لوزارة الداخلية بإجراء اختبار مسبق ..</p>\n"
	 * +
	 * "                                    <a class=\"english-text\" href=\"https://www.my.gov.sa/wps/portal/snp/content/news\" target='_blank'>More...</a>\n"
	 * +
	 * "                                    <a class=\"arabic-text\" href=\"https://www.my.gov.sa/wps/portal/snp/content/news\" target='_blank'>أكثر...</a>\n"
	 * + "                                </div>\n" +
	 * "                            </div>\n" + "                        </div>\n" +
	 * "                    </div>   \n" // + "                </div>" +
	 * "            </div>\n"; } mainHeader += "</div>"; mainHeader +=
	 * "           <div class='col-md-3 col-lg-3 niic-center-wrapper'>"; if
	 * (processVal != null && !"".equalsIgnoreCase(processVal) &&
	 * !"null".equalsIgnoreCase(processVal) && processVal.contains("BANNER")) {
	 * mainHeader +=
	 * "                <div id=\"carouselExampleIndicators1\" class=\"hoverme\">"
	 * // + "                    <ol class=\"carousel-indicators\">" // +
	 * "                        <li data-target=\"#carouselExampleIndicators1\" data-slide-to=\"0\" class=\"active\"></li>"
	 * // // +
	 * "                        <li data-target=\"#carouselExampleIndicators1\" data-slide-to=\"1\"></li>"
	 * // // +
	 * "                        <li data-target=\"#carouselExampleIndicators1\" data-slide-to=\"2\"></li>"
	 * // + "                    </ol>" +
	 * "                    <div class=\"login-custom-card\" class=\"btn btn-default\" id=\"openBtn\">"
	 * + "                    <div class=\"service_header\">" +
	 * "                        <h5 class='english-text'><img src=\"images/key_benefits.png\" title=\"stats & Facts\" style='width:18px;'><span style='margin-left:5px;'>Benfits of Smart IG</span></h5>"
	 * +
	 * "                        <h5 class='arabic-text'><img src=\"images/key_benefits.png\" title=\"stats & Facts\" style='width:18px;'>فوائد PNR</h5>"
	 * + "                    </div>" +
	 * "                        <div class=\"carousel-item active popover__content\">"
	 * // +
	 * "                            <img class=\"d-block w-100\" src=\"images/Value Prosositions_Video_Animation.gif\" alt=\"First slide\" class='themeModeDark'>"
	 * + " <ul class=\"key_cods english-text\">" +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> AI driven solutions for Improving operational efficiency</li>"
	 * +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> Increased Agility</li>"
	 * +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> Data-driven decision-making</li>"
	 * +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> High-quality interactive data visualizations</li>"
	 * +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> Helps customers to make faster and accurate decisions by leveraging the existing data, at the right  time</li>"
	 * +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> Strong user experience (UX) for customers</li>"
	 * +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> Auto-Suggested charts gives users ready to use charts</li>"
	 * +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> Makes it easier for users to create subsets of data and build visualizations on that</li>"
	 * +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> Users can easily share their reports with other users of the organization</li>"
	 * +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> It supports Artificial Intelligence in Data Analytics that users can leverage to pre-process and keep the data ready for visualization</li>"
	 * +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> Allows users to divide and slice complex data models into a simpler form and separate charts are also built on it</li>"
	 * +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> Offers integrations with many connectors that allow users to connect to various data sources</li>"
	 * + " </ul>" + " <ul class=\"key_cods arabic-text\">" +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> حلول تعتمد على الذكاء الاصطناعي لتحسين الكفاءة التشغيلية</li>"
	 * +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> زيادة خفة الحركة</li>"
	 * +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> اتخاذ القرارات المبنية على البيانات</li>"
	 * +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> تصورات بيانات تفاعلية عالية الجودة</li>"
	 * +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> يساعد العملاء على اتخاذ قرارات أسرع وأكثر دقة من خلال الاستفادة من البيانات الموجودة، في الوقت المناسب</li>"
	 * +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> تجربة مستخدم قوية (UX) للعملاء</li>"
	 * +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> الرسوم البيانية المقترحة تلقائيًا تمنح المستخدمين الاستعداد لاستخدام الرسوم البيانية</li>"
	 * +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> يُسهل على المستخدمين إنشاء مجموعات فرعية من البيانات وإنشاء تصورات لها</li>"
	 * +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> يمكن للمستخدمين مشاركة تقاريرهم بسهولة مع المستخدمين الآخرين للمؤسسة</li>"
	 * +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> وهو يدعم الذكاء الاصطناعي في تحليلات البيانات التي يمكن للمستخدمين الاستفادة منها في المعالجة المسبقة والحفاظ على البيانات جاهزة للتصور</li>"
	 * +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> يسمح للمستخدمين بتقسيم نماذج البيانات المعقدة وتقطيعها إلى شكل أبسط ويتم أيضًا إنشاء مخططات منفصلة عليها</li>"
	 * +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> يقدم عمليات تكامل مع العديد من الموصلات التي تسمح للمستخدمين بالاتصال بمصادر البيانات المختلفة</li>"
	 * + " </ul>" + "                        </div>" // +
	 * "                        <div class=\"carousel-item\">" // +
	 * "                            <img class=\"d-block w-100\" src=\"images/BannerKSA2.png\" alt=\"Second slide\" class='themeModeDark'>"
	 * // + "                        </div>" // +
	 * "                        <div class=\"carousel-item\">" // +
	 * "                            <img class=\"d-block w-100\" src=\"images/BannerKSA3.png\" alt=\"Third slide\" class='themeModeDark'>"
	 * // + "                        </div>" // +
	 * "                        <div class=\"carousel-item\">" // +
	 * "                            <img class=\"d-block w-100\" src=\"images/BannerKSA4.png\" alt=\"Fourth slide\" class='themeModeDark'>"
	 * // + "                        </div>" + "                    </div>" // +
	 * "                    <a class=\"carousel-control-prev\" href=\"#carouselExampleIndicators1\" role=\"button\" data-slide=\"prev\">"
	 * // +
	 * "                        <span class=\"carousel-control-prev-icon\" aria-hidden=\"true\"></span>"
	 * // + "                        <span class=\"sr-only\">Previous</span>" // +
	 * "                    </a>" // +
	 * "                    <a class=\"carousel-control-next\" href=\"#carouselExampleIndicators1\" role=\"button\" data-slide=\"next\">"
	 * // +
	 * "                        <span class=\"carousel-control-next-icon\" aria-hidden=\"true\"></span>"
	 * // + "                        <span class=\"sr-only\">Next</span>" // +
	 * "                    </a>" + "                </div>"; // +
	 * "<div id=\"modal-content\" class=\"modal fade\" tabindex=\"-1\" role=\"dialog\">"
	 * // + "    <div class=\"modal-dialog\">" // +
	 * "        <div class=\"modal-content\">" // +
	 * "           <div class=\"modal-body\">" // +
	 * "             <img class=\"d-block w-100\" src=\"images/Value Prosositions_Video_Animation.gif\" alt=\"First slide\" class='themeModeDark'>"
	 * // + "            </div>" // + "        </div>" // + "    </div>" // +
	 * "</div>";
	 * 
	 * // + "                    <div class=\"pop\">" // +
	 * "                        <img class=\"d-block w-100\" src=\"images/Value Prosositions_Video_Animation.gif\" alt=\"First slide\" class='themeModeDark'>"
	 * // +"                     </div> "; }
	 * 
	 * // if (processVal != null && !"".equalsIgnoreCase(processVal) &&
	 * !"null".equalsIgnoreCase(processVal) && processVal.contains("BANNER")) { //
	 * mainHeader +=
	 * "                <div id=\"carouselExampleIndicators1\" class=\"hoverme\">"
	 * //// + "                    <ol class=\"carousel-indicators\">" //// +
	 * "                        <li data-target=\"#carouselExampleIndicators1\" data-slide-to=\"0\" class=\"active\"></li>"
	 * //// // +
	 * "                        <li data-target=\"#carouselExampleIndicators1\" data-slide-to=\"1\"></li>"
	 * //// // +
	 * "                        <li data-target=\"#carouselExampleIndicators1\" data-slide-to=\"2\"></li>"
	 * //// + "                    </ol>" // +
	 * "                    <div class=\"login-custom-card\" class=\"btn btn-default\" id=\"openBtn\">"
	 * // + "                    <div class=\"service_header\">" // +
	 * "                        <h5 class='english-text'><img src=\"images/key_benefits.png\" title=\"stats & Facts\" style='width:18px;' class='themeModeDark'><span style='margin-left:5px;'>Key benefits</span></h5>"
	 * // +
	 * "                        <h5 class='arabic-text'><i class=\"fa fa-bar-chart\" aria-hidden=\"true\"></i>الفوائد الرئيسية</h5>"
	 * // + "                    </div>" // +
	 * "                        <div class=\"carousel-item active popover__content\">"
	 * //// +
	 * "                            <img class=\"d-block w-100\" src=\"images/Value Prosositions_Video_Animation.gif\" alt=\"First slide\" class='themeModeDark'>"
	 * // + " <ul class=\"key_cods\">" // +
	 * "   <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> Unique National Product Number (NPN)</li>"
	 * // +
	 * "   <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> Equivalences / Alternative Materials</li>"
	 * // +
	 * "   <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> Demand-Supply Optimization & Sharing</li>"
	 * // +
	 * "   <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> Improved Consolidated Production & Supply</li>"
	 * // +
	 * "   <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> Inputs for Logistics Park (Virtual Warehouse)</li>"
	 * // +
	 * "   <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> Data Analytics & Dashboards</li>"
	 * // +
	 * "   <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> 100% Governed & Compliant processes</li>"
	 * // +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> Automation Opportunities</li>"
	 * // +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> Global Data Exchange Platform</li>"
	 * // +
	 * "  <li><i class=\"fa fa-hand-o-right\" aria-hidden=\"true\"></i> Adoption to KSA Vision 2030</li>"
	 * // + " </ul>" // + "                        </div>" // // +
	 * "                        <div class=\"carousel-item\">" // // +
	 * "                            <img class=\"d-block w-100\" src=\"images/BannerKSA2.png\" alt=\"Second slide\" class='themeModeDark'>"
	 * // // + "                        </div>" // // +
	 * "                        <div class=\"carousel-item\">" // // +
	 * "                            <img class=\"d-block w-100\" src=\"images/BannerKSA3.png\" alt=\"Third slide\" class='themeModeDark'>"
	 * // // + "                        </div>" // // +
	 * "                        <div class=\"carousel-item\">" // // +
	 * "                            <img class=\"d-block w-100\" src=\"images/BannerKSA4.png\" alt=\"Fourth slide\" class='themeModeDark'>"
	 * // // + "                        </div>" // + "                    </div>"
	 * //// +
	 * "                    <a class=\"carousel-control-prev\" href=\"#carouselExampleIndicators1\" role=\"button\" data-slide=\"prev\">"
	 * //// +
	 * "                        <span class=\"carousel-control-prev-icon\" aria-hidden=\"true\"></span>"
	 * //// + "                        <span class=\"sr-only\">Previous</span>" ////
	 * + "                    </a>" //// +
	 * "                    <a class=\"carousel-control-next\" href=\"#carouselExampleIndicators1\" role=\"button\" data-slide=\"next\">"
	 * //// +
	 * "                        <span class=\"carousel-control-next-icon\" aria-hidden=\"true\"></span>"
	 * //// + "                        <span class=\"sr-only\">Next</span>" //// +
	 * "                    </a>" // + "                </div>"; //// +
	 * "<div id=\"modal-content\" class=\"modal fade\" tabindex=\"-1\" role=\"dialog\">"
	 * //// + "    <div class=\"modal-dialog\">" //// +
	 * "        <div class=\"modal-content\">" //// +
	 * "           <div class=\"modal-body\">" //// +
	 * "             <img class=\"d-block w-100\" src=\"images/Value Prosositions_Video_Animation.gif\" alt=\"First slide\" class='themeModeDark'>"
	 * //// + "            </div>" //// + "        </div>" //// + "    </div>" ////
	 * + "</div>"; // //// + "                    <div class=\"pop\">" //// +
	 * "                        <img class=\"d-block w-100\" src=\"images/Value Prosositions_Video_Animation.gif\" alt=\"First slide\" class='themeModeDark'>"
	 * //// +"                     </div> "; // } if (processVal != null &&
	 * !"".equalsIgnoreCase(processVal) && !"null".equalsIgnoreCase(processVal) &&
	 * processVal.contains("NEWS")) { mainHeader +=
	 * "<div class=\"pilog-announce mt-2\">" + "<div class=\"service_header\">" +
	 * "<h5 class=\"english-text\"><img src=\"images/megaphone-red.png\" style='width:18px;'><span class=\"dxpTitleAnnoncements\" "
	 * + "style='margin-left:12px;'> Announcements</span></h5>" +
	 * "<h5 class=\"arabic-text\"><img src=\"images/megaphone-red.png\" style='width:18px;'><span class=\"dxpTitleAnnoncements\" "
	 * + ">إعلانات </span></h5>" + "</div>" +
	 * "<div id=\"carouselExampleIndicators\" class=\"carousel slide\" data-ride=\"carousel\">"
	 * + "                    <ol class=\"carousel-indicators\">" +
	 * "                        <li data-target=\"#carouselExampleIndicators\" data-slide-to=\"0\" class=\"active\"></li>"
	 * // +
	 * "                        <li data-target=\"#carouselExampleIndicators\" data-slide-to=\"1\"></li>"
	 * // +
	 * "                        <li data-target=\"#carouselExampleIndicators\" data-slide-to=\"2\"></li>"
	 * + "                    </ol>" +
	 * "                    <div class=\"carousel-inner\">" +
	 * "                        <div class=\"carousel-item active\">" +
	 * "                            <img class=\"d-block w-100\" src=\"images/Analytics-Image.jpg\" alt=\"First slide\" class='themeModeDark'>"
	 * + "                        </div>" // +
	 * "                        <div class=\"carousel-item\">" // +
	 * "                            <img class=\"d-block w-100\" src=\"images/BannerKSA2.png\" alt=\"Second slide\" class='themeModeDark'>"
	 * // + "                        </div>" +
	 * "                        <div class=\"carousel-item\">" +
	 * "                            <img class=\"d-block w-100\" src=\"images/BannerKSA3.png\" alt=\"Third slide\" class='themeModeDark'>"
	 * + "                        </div>" // +
	 * "                        <div class=\"carousel-item\">" // +
	 * "                            <img class=\"d-block w-100\" src=\"images/BannerKSA4.png\" alt=\"Fourth slide\" class='themeModeDark'>"
	 * // + "                        </div>" // +
	 * "                        <div class=\"carousel-item\">" // +
	 * "                            <img class=\"d-block w-100\" src=\"images/AnnouncementsKsa2.png\" alt=\"Second slide\" class='themeModeDark'>"
	 * // + "                        </div>" // +
	 * "                        <div class=\"carousel-item\">" // +
	 * "                            <img class=\"d-block w-100\" src=\"images/AnnouncementsKsa.png\" alt=\"Third slide\" class='themeModeDark'>"
	 * // + "                        </div>" // +
	 * "                        <div class=\"carousel-item\">" // +
	 * "                            <img class=\"d-block w-100\" src=\"images/BannerKSA4.png\" alt=\"Fourth slide\" class='themeModeDark'>"
	 * // + "                        </div>" + "                    </div>" +
	 * "                    <a class=\"carousel-control-prev\" href=\"#carouselExampleIndicators\" role=\"button\" data-slide=\"prev\">"
	 * +
	 * "                        <span class=\"carousel-control-prev-icon\" aria-hidden=\"true\"></span>"
	 * + "                        <span class=\"sr-only\">Previous</span>" +
	 * "                    </a>" +
	 * "                    <a class=\"carousel-control-next\" href=\"#carouselExampleIndicators\" role=\"button\" data-slide=\"next\">"
	 * +
	 * "                        <span class=\"carousel-control-next-icon\" aria-hidden=\"true\"></span>"
	 * + "                        <span class=\"sr-only\">Next</span>" +
	 * "                    </a>" + "                </div>" // +
	 * "<div class=\"dxpNewBannerImage\">" // +
	 * "<img src=\"images/AnnouncementsKsa.png\" class='themeModeDark english-text' alt=\"\" style='width:100%' !important;>"
	 * // +
	 * "<img src=\"greenimages/Announcements.png\" class='themeModeDark arabic-text' alt=\"\" style='width:100%' !important;>"
	 * // + "</div>" + "</div>"; } if (processVal != null &&
	 * !"".equalsIgnoreCase(processVal) && !"null".equalsIgnoreCase(processVal) &&
	 * processVal.contains("APPOINTMENTS")) { mainHeader +=
	 * "<div class=\"pilog-announce mt-2\">" + "<div class=\"service_header\">" +
	 * "<h5 class='english-text feed-back-flex'>" + "<div class=\"feed-back\">" +
	 * "<img src=\"images/FAQ.png\" style='width:22px;' class='themeModeDark'>" +
	 * "<span class=\"dxpTitleAnnoncements\" style='margin-left:12px;'> Feedback </span>"
	 * + "</div>" +
	 * " <span id=\"feedbackButton\" onclick=\"getFeedbackForm()\">Give Feedback</span>"
	 * + "</h5>" + "<h5 class='arabic-text'>" + "<div class=\"feed-back\">" +
	 * "<img src=\"images/calendarBlue.png\" style='width:22px;' class='themeModeDark'>"
	 * +
	 * "<span class=\"dxpTitleAnnoncements\" style='margin-left:12px;'>مواعيد </span>"
	 * + "</div>" +
	 * " <button id=\"feedbackButton\" onclick=\"getFeedbackForm()\">إعطاء ردود الفعل</button>"
	 * + "</h5>" + "</div>";
	 * 
	 * List listData = loginHandlerService.getAVGFeedbackRating(request); if
	 * (listData != null && !listData.isEmpty()) { mainHeader +=
	 * "<div id='niic_FeedBack_formId' class=\"feedback-item\">"; for (int i = 0; i
	 * < listData.size(); i++) { Object[] objData = (Object[]) listData.get(i); if
	 * (objData != null) { mainHeader += "<div class=\"review-item\">"; mainHeader
	 * += "<span id='" + objData[0] + "'>" + objData[0] + "</span>" +
	 * "<div id='jqxRating" + i + "'>" + objData[1] + "</div>" +
	 * "<span id='jqxRatingCount" + i + "'>(" + objData[2] + ")</span>"; mainHeader
	 * += "</div>"; } } mainHeader += "</div>"; }
	 * 
	 * // + "</div>" mainHeader += "</div>"; } // if (processVal != null &&
	 * !"".equalsIgnoreCase(processVal) && !"null".equalsIgnoreCase(processVal) &&
	 * processVal.contains("APPOINTMENTS")) { // mainHeader +=
	 * "<div class=\"pilog-announce mt-2\">" // + "<div class=\"service_header\">"
	 * // +
	 * "<h5 class='english-text'><img src=\"images/FAQ.png\" style='width:22px;' class='themeModeDark'>"
	 * // +
	 * "<span class=\"dxpTitleAnnoncements\" style='margin-left:12px;'> Feed Back</span>"
	 * // +
	 * " <button id=\"feedbackButton\" onclick=\"getFeedbackForm()\" style='margin-left:100px;'>Give Feedback</button>"
	 * // + "</h5>" // +
	 * "<h5 class='arabic-text'><img src=\"images/calendarBlue.png\" style='width:22px;' class='themeModeDark'>"
	 * // +
	 * "<span class=\"dxpTitleAnnoncements\" style='margin-left:12px;'>مواعيد </span>"
	 * // + "</h5>";// + "</div>" // // List listData =
	 * cloudLoginHandlerService.getAVGFeedbackRating(request); // if (listData !=
	 * null && !listData.isEmpty()) { // mainHeader +=
	 * "<div id='niic_FeedBack_formId' class=\"feedback-item\">"; // for (int i = 0;
	 * i < listData.size(); i++) { // Object[] objData = (Object[]) listData.get(i);
	 * // if (objData != null) { // mainHeader += "<div class=\"review-item\">"; //
	 * mainHeader += "<span id='" + objData[0] + "'>" + objData[0] + "</span>" // +
	 * "<div id='jqxRating" + i + "'>" + objData[1] + "</div>" // +
	 * "<span id='jqxRatingCount" + i + "'>" + objData[2] + "</span>"; // mainHeader
	 * += "</div>"; // } // } // mainHeader += "</div>"; // } // // // + "</div>" //
	 * mainHeader += "</div>" // + "</div>"; // }
	 * 
	 * mainHeader += "</div>" + "</div>" +
	 * "<footer class=\"vision-login-footer\">\n" +
	 * "        <div class=\"footer-wrapper container\">\n" +
	 * "            <div class=\"vision-footer-content row\">\n" +
	 * "                <div class=\"col-md-3\">\n" +
	 * "                    <div class=\"vision-social-content\">\n" +
	 * "                        <h6 class=\"english-text\">Social Media</h6>\n" +
	 * "                        <h6 class=\"arabic-text\">وسائل التواصل الاجتماعي</h6>\n"
	 * +
	 * "                        <div class=\"vision-social-icons\"><ul class=\"footer-social-icons\">\n"
	 * + "                    <li>\n" +
	 * "                        <a href=\"https://www.linkedin.com/company/piloggroup/\" title=\"Linked In\" target=\"_blank\"><i class=\"fa fa-linkedin\"></i></a>\n"
	 * + "                    </li>\n" + "                    <li>\n" +
	 * "                        <a href=\"https://www.facebook.com/piloggroup/\" title=\"Facebook\" class=\"facebookFontIcon\" target=\"_blank\"><i class=\"fa fa-facebook\"></i></a>\n"
	 * + "                    </li>\n" +
	 * "                    <li class=\"header-notification\">\n" +
	 * "                        <a href=\"https://www.instagram.com/piloggroup/\" title=\"Instagram\" target=\"_blank\"><i class=\"fa fa-instagram\"></i></a>\n"
	 * + "                    </li>\n" +
	 * "                    <li class=\"header-notification twitterxicon\">\n" +
	 * "                        <a href=\"https://twitter.com/PiLogGroup\" title=\"Twitter\" target=\"_blank\"><svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 512 512\"><path d=\"M389.2 48h70.6L305.6 224.2 487 464H345L233.7 318.6 106.5 464H35.8L200.7 275.5 26.8 48H172.4L272.9 180.9 389.2 48zM364.4 421.8h39.1L151.1 88h-42L364.4 421.8z\"></path></svg></a>\n"
	 * + "                    </li>\n" +
	 * "                    <li class=\"header-notification\">\n" +
	 * "                        <a href=\"https://www.youtube.com/channel/UCEOznx22QvZvTbkeJ-lj7GQ\" title=\"You Tube\" target=\"_blank\"><i class=\"fa fa-youtube-play\"></i></a>\n"
	 * + "                    </li>\n" + "                </ul></div>\n" // +
	 * "                        <h6>Accesibility Tools</h6>\n" // +
	 * "                        <div class=\"vision-footer-accessIcons\"></div>\n"
	 * // + "                        <div class=\"vision-footer-signLang\">\n" // +
	 * "                            <a href=\"javascript:void(0)\">\n" // +
	 * "                                <span class=\"icon-signLang\"></span>\n" //
	 * + "                                Sign Language Supported\n" // +
	 * "                            </a>\n" // + "                        </div>\n"
	 * + "                    </div>\n" + "                </div>\n" +
	 * "                <div class=\"col-md-3\">\n" +
	 * "                    <h6 class=\"english-text\">About Pilog</h6>\n" +
	 * "                    <h6 class=\"arabic-text\">حول بيلوج</h6>\n" +
	 * "                    <ul class=\"vision-footer-aboutUs english-text\">\n" +
	 * "                        <li>About Pilog</li>\n" +
	 * "                        <li>Privacy Policy</li>\n" +
	 * "                        <li>Terms of Use</li>\n" // +
	 * "                        <li>News</li>\n" + "                    </ul>\n" +
	 * "                    <ul class=\"vision-footer-aboutUs arabic-text\">\n" +
	 * "                        <li>حول بيلوج</li>\n" +
	 * "                        <li>سياسة الخصوصية</li>\n" +
	 * "                        <li>شروط الاستخدام</li>\n" // +
	 * "                        <li>News</li>\n" + "                    </ul>\n" +
	 * "                </div>\n" + "                <div class=\"col-md-3\">\n" +
	 * "                    <h6 class=\"english-text\">Contact and Support</h6>\n" +
	 * "                    <h6 class=\"arabic-text\">الاتصال والدعم</h6>\n" +
	 * "                    <ul class=\"vision-footer-contact english-text\">\n" +
	 * "                        <li>Contact Us</li>\n" +
	 * "                        <li>Report Corruption</li>\n" +
	 * "                        <li>FAQs</li>\n" // +
	 * "                        <li>Service channels</li>\n" +
	 * "                    </ul>\n" +
	 * "                    <ul class=\"vision-footer-contact arabic-text\">\n" +
	 * "                        <li>اتصل بنا</li>\n" +
	 * "                        <li>الإبلاغ عن الفساد</li>\n" +
	 * "                        <li>الأسئلة الشائعة</li>\n" // +
	 * "                        <li>Service channels</li>\n" +
	 * "                    </ul>\n" + "                </div>\n" +
	 * "                <div class=\"col-md-3\">\n" +
	 * "                    <h6 class=\"english-text\">Important Links</h6>\n" +
	 * "                    <h6 class=\"arabic-text\">روابط مهمة</h6>\n" +
	 * "                    <ul class=\"vision-footer-links\">\n" +
	 * "                        <li><img src=\"./images/android-logo.png\" alt=\"android-logo\"/></li>\n"
	 * +
	 * "                        <li><img src=\"./images/ios-logo.png\" alt=\"ios-logo\"/></li>\n"
	 * + "                    </ul>\n" + "                </div>\n" +
	 * "            </div>\n" + "        </div>\n" +
	 * "        <div class=\"vision-footer-secondWrapper\">\n" +
	 * "            <div class=\"vision-footer-css row\">\n" +
	 * "                <div class=\"vision-footer-copyRights col-md-12 text-center\">\n"
	 * +
	 * "                    <p class=\"english-text\">Copyright © 2023 PiLog Group</p>\n"
	 * +
	 * "                    <p class=\"arabic-text\">حقوق الطبع والنشر © 2023 مجموعة PiLog</p>\n"
	 * + "                </div>\n" + "            </div>\n" + "        </div>\n" +
	 * "    </footer>" + "</div>" + "</div>"; } catch (Exception e) {
	 * e.printStackTrace(); } return mainHeader; }
	 * 
	 */

	public String homePageDefaultString(HttpServletRequest request, String processVal) {
		String mainHeader = "";
		try {
			StringBuffer mainHeaderStrBuf = new StringBuffer();
			try {
				mainHeaderStrBuf.append("<div class=\"loginMainClass\">");
				mainHeaderStrBuf.append("    <header class=\"header\">");
				mainHeaderStrBuf.append("        <div class=\"container\">");
				mainHeaderStrBuf.append("            <div class=\"h-content\">");
				mainHeaderStrBuf.append("                <div class=\"h-left\">");
				mainHeaderStrBuf.append("                    <div class=\"h-logo\">");
				mainHeaderStrBuf.append("                        <a href=\"javascript:;\">");
				mainHeaderStrBuf.append("                            <figure>");
				mainHeaderStrBuf.append(
						"                                <img src=\"images/logo_red.png\" alt=\"logo\" class=\"img-fluid lazy\">");
				mainHeaderStrBuf.append("                            </figure>");
				mainHeaderStrBuf.append("                        </a>");
				mainHeaderStrBuf.append("                    </div>");
				mainHeaderStrBuf.append("                </div>");
				mainHeaderStrBuf.append("            </div>");
				mainHeaderStrBuf.append("        </div>");
				mainHeaderStrBuf.append("    </header>");
				mainHeaderStrBuf.append("    <main class=\"content\">");
				mainHeaderStrBuf.append("        <div class=\"main-content-wrapper-section\">");
				mainHeaderStrBuf.append("            <div class=\"container\">");
				mainHeaderStrBuf
						.append("                <div class=\"main-wrapper-description navbar navbar-expand-lg\">");
				mainHeaderStrBuf.append(
						"                    <div class=\"main-sidebar-part collapse navbar-collapse\" id=\"navbar-content\">");
				mainHeaderStrBuf.append("                        <div class=\"main-sidebar-section\">");
				mainHeaderStrBuf.append("                            <div class=\"login-sidebar-block\">");
				mainHeaderStrBuf.append("                            <div class=\"login-scroll-wrapper\">");
				mainHeaderStrBuf.append("                                <div class=\"wrapper\">");
				mainHeaderStrBuf.append("                                    <div class=\"logo\" id='toggleloginDiv'>");
				mainHeaderStrBuf.append(
						"                                        <span class='login-icon' style='width:23px;'>");
				mainHeaderStrBuf.append(
						"                                            <img src=\"images/Signin-icon.svg\" class=\"pilogcloudLogo themeModeDark\"");
				mainHeaderStrBuf.append("                                                alt=\"pilogcloud\" />");
				mainHeaderStrBuf.append("                                        </span>");
				mainHeaderStrBuf.append(
						"                                        <span class='login-text english-text'>Login</span>");
				mainHeaderStrBuf
						.append("                                        <span class='login-text arabic-text'>تسجيل");
				mainHeaderStrBuf.append("                                            الدخول</span>");
				mainHeaderStrBuf.append("                                    </div>");
				mainHeaderStrBuf.append(
						"                                    <div class=\"visionLoginpageInner\" id='visionLoginpageInner' style='display: none;'>");
				mainHeaderStrBuf.append(
						"                                        <span id='loginError' style='color:red;'></span>");
				mainHeaderStrBuf.append(
						"                                        <div class=\"form-field d-flex align-items-center userNameInputMainDiv\">");
				mainHeaderStrBuf
						.append("                                            <div class=\"input-group-prepend\">");
				mainHeaderStrBuf.append(
						"                                                <span class=\"input-group-text\" id=\"basic-addon1\">");
				mainHeaderStrBuf.append(
						"                                                    <img src=\"images/userlogin.png\" class=\"themeModeDark\"");
				mainHeaderStrBuf.append("                                                        alt=\"userlogin\" />");
				mainHeaderStrBuf.append("                                                </span>");
				mainHeaderStrBuf.append("                                            </div>");
				mainHeaderStrBuf.append(
						"                                            <input type=\"text\" class='form-control' name=\"rsUsername\" id=\"rsUsername\"");
				mainHeaderStrBuf.append(
						"                                                placeholder=\"Username\" autocomplete=\"off\"");
				mainHeaderStrBuf.append(
						"                                                onkeypress=\"showorHideError('username')\">");
				mainHeaderStrBuf.append("                                        </div>");
				mainHeaderStrBuf.append(
						"                                        <span id='passwordError' style='color:red;'></span>");
				mainHeaderStrBuf.append(
						"                                        <div class=\"form-field d-flex align-items-center showorHidePassword\"");
				mainHeaderStrBuf.append("                                            style='display:none !important'>");
				mainHeaderStrBuf
						.append("                                            <div class=\"input-group-prepend\">");
				mainHeaderStrBuf.append(
						"                                                <span class=\"input-group-text\" id=\"basic-addon1\">");
				mainHeaderStrBuf.append(
						"                                                    <img src=\"images/passwordicon.png\" class=\"themeModeDark\"");
				mainHeaderStrBuf
						.append("                                                        alt=\"passwordicon\" />");
				mainHeaderStrBuf.append("                                                </span>");
				mainHeaderStrBuf.append("                                            </div>");
				mainHeaderStrBuf.append(
						"                                            <input type=\"password\" class='form-control' name=\"rsPassword\"");
				mainHeaderStrBuf.append(
						"                                                id=\"rsPassword\" placeholder=\"Password\" data-man=\"M\" required=\"\"");
				mainHeaderStrBuf.append(
						"                                                onkeypress=\"showorHideError('password')\">");
				mainHeaderStrBuf.append(
						"                                            <input type=\"hidden\" name=\"rsPasswordHid\" id=\"rsPasswordHid\">");
				mainHeaderStrBuf.append("                                        </div>");
				mainHeaderStrBuf.append(
						"                                        <div class=\"form-field d-flex align-items-center LanguageSelect\"");
				mainHeaderStrBuf.append("                                            style='display:none !important'>");
				mainHeaderStrBuf
						.append("                                            <div class=\"input-group-prepend\">");
				mainHeaderStrBuf.append(
						"                                                <span class=\"input-group-text\" id=\"basic-addon1\">");
				mainHeaderStrBuf.append(
						"                                                    <img src=\"images/languageSet.png\" alt=\"languageSet\">");
				mainHeaderStrBuf.append("                                                </span>");
				mainHeaderStrBuf.append("                                            </div>");
				mainHeaderStrBuf.append(
						"                                            <select id=\"id_locale\" name=\"language\" onchange=\"getDefaultLang()\">");
				mainHeaderStrBuf.append(
						"                                                <option value='en_US' selected>English");
				mainHeaderStrBuf.append("                                                    US</option>");
				mainHeaderStrBuf.append(
						"                                                <option value='ar_SA'>Arabic SA</option>");
				mainHeaderStrBuf.append("                                            </select>");
				mainHeaderStrBuf.append(
						"                                            <input type=\"hidden\" name=\"language\" id=\"language\" value=\"en_US\">");
				mainHeaderStrBuf.append("                                        </div>");
				mainHeaderStrBuf.append(
						"                                        <div id=\"verifyLoginSecurity\" class=\"loginSecurity\"></div>");
				mainHeaderStrBuf.append(
						"                                        <input type=\"hidden\" id=\"englishSelectContent\" />");
				mainHeaderStrBuf.append(
						"                                        <button class=\"btn btn-primary btn-md btn-block waves-effect text-center m-b-20\"");
				mainHeaderStrBuf.append(
						"                                            onclick=\"showNextPassword()\" id=\"showPassword\">");
				mainHeaderStrBuf
						.append("                                            <span class=\"english-text\">Next</span>");
				mainHeaderStrBuf.append(
						"                                            <span class=\"arabic-text\">التالي</span>");
				mainHeaderStrBuf.append("                                        </button>");
				mainHeaderStrBuf.append(
						"                                        <button class=\"btn btn-primary btn-md btn-block waves-effect text-center m-b-20\"");
				mainHeaderStrBuf.append(
						"                                            onclick=\"showorHideError('password', 'P')\" id=\"processLoginID2\"");
				mainHeaderStrBuf.append("                                            style='display:none;'>");
				mainHeaderStrBuf.append(
						"                                            <span class=\"english-text\">Sign In</span>");
				mainHeaderStrBuf
						.append("                                            <span class=\"arabic-text\">تسجيل");
				mainHeaderStrBuf.append("                                                الدخول</span>");
				mainHeaderStrBuf.append("                                        </button>");
				mainHeaderStrBuf.append("                                    </div>");
				mainHeaderStrBuf.append(
						"                                    <div class='signupClass signupRegisterClass' onclick=\"registerForm()\">");
				mainHeaderStrBuf.append("                                        <span class=\"login-icon\">");
				mainHeaderStrBuf.append(
						"                                            <img src=\"images/SignUp.png\" class=\"themeModeDark\" alt=\"\"");
				mainHeaderStrBuf.append("                                                width=\"22px\" />");
				mainHeaderStrBuf.append("                                        </span>");
				mainHeaderStrBuf.append(
						"                                        <span class=\"login-text english-text\">Register</span>");
				mainHeaderStrBuf.append(
						"                                        <span class=\"login-text arabic-text\">تسجيل</span>");
				mainHeaderStrBuf.append("                                    </div>");
				mainHeaderStrBuf.append(
						"                                    <div class='signupClass' onclick=\"updatePassFormBeforeLogin()\">");
				mainHeaderStrBuf.append("                                        <span class=\"login-icon\">");
				mainHeaderStrBuf.append(
						"                                            <img src=\"images/passwordSet.png\" alt=\"\" class=\"themeModeDark\"");
				mainHeaderStrBuf.append("                                                width=\"22px\" />");
				mainHeaderStrBuf.append("                                        </span>");
				mainHeaderStrBuf.append(
						"                                        <span class=\"login-text english-text\">Forgot");
				mainHeaderStrBuf.append("                                            password?</span>");
				mainHeaderStrBuf.append(
						"                                        <span class=\"login-text arabic-text\">هل نسيت");
				mainHeaderStrBuf.append("                                            كلمة السر؟</span>");
				mainHeaderStrBuf.append("                                    </div>");
				mainHeaderStrBuf.append("                                </div>");
				mainHeaderStrBuf.append("                                </div>");
				mainHeaderStrBuf.append("                            </div>");
				mainHeaderStrBuf.append("                            <div class=\"register-vendor-section\">");
				mainHeaderStrBuf.append("                                <div class=\"register-vendor-description\">");
				mainHeaderStrBuf.append("                                    <div class=\"register-vendor-content\">");
				mainHeaderStrBuf.append("                                        <div class=\"register-vendor-list\">");
				mainHeaderStrBuf.append(
						"                                            <ul class=\"nav flex-column nav-pills\" id=\"v-pills-tab\" role=\"tablist\" aria-orientation=\"vertical\">");
				mainHeaderStrBuf.append("                                                <li class=\"nav-item\">");
				mainHeaderStrBuf.append(
						"                                                  <button class=\"nav-link active\" id=\"v-pills-dataAnalytics-tab\" data-toggle=\"pill\" href=\"#v-pills-dataAnalytics\" role=\"tab\" aria-controls=\"v-pills-dataAnalytics\" aria-selected=\"true\">");
				mainHeaderStrBuf.append(
						"                                                        <div class=\"register-vendor-block\">");
				mainHeaderStrBuf.append(
						"                                                            <div class=\"vendor-image\">");
				mainHeaderStrBuf.append("                                                                <figure>");
				mainHeaderStrBuf.append(
						"                                                                    <img src=\"images/DataAnalytics.png\"");
				mainHeaderStrBuf.append(
						"                                                                        alt=\"vendor-img\" class=\"img-fluid lazy light-mode-img\">");
				mainHeaderStrBuf.append(
						"                                                                    <img src=\"images/DataAnalytics.png\"");
				mainHeaderStrBuf.append(
						"                                                                        alt=\"vendor-img\" class=\"img-fluid lazy dark-mode-img\">");
				mainHeaderStrBuf.append("                                                                </figure>");
				mainHeaderStrBuf.append("                                                            </div>");
				mainHeaderStrBuf.append(
						"                                                            <div class=\"vendor-main-detail\">");
				/*
				 * mainHeaderStrBuf.
				 * append("                                                                <div class=\"more-info-icon\">"
				 * ); mainHeaderStrBuf.
				 * append("                                                                    <span>"
				 * ); mainHeaderStrBuf.
				 * append("                                                                        <figure>"
				 * ); mainHeaderStrBuf.
				 * append("                                                                            <img src=\"images/more-info-layout2-light-icon.svg\""
				 * ); mainHeaderStrBuf.
				 * append("                                                                                alt=\"more-info-icon\""
				 * ); mainHeaderStrBuf.
				 * append("                                                                                class=\"img-fluid lazy light-mode-img\">"
				 * ); mainHeaderStrBuf.
				 * append("                                                                            <img src=\"images/more-info-layout2-dark-icon.svg\""
				 * ); mainHeaderStrBuf.
				 * append("                                                                                alt=\"more-info-icon\""
				 * ); mainHeaderStrBuf.
				 * append("                                                                                class=\"img-fluid lazy dark-mode-img\">"
				 * ); mainHeaderStrBuf.
				 * append("                                                                        </figure>"
				 * ); mainHeaderStrBuf.
				 * append("                                                                    </span>"
				 * ); mainHeaderStrBuf.
				 * append("                                                                </div>"
				 * );
				 */
				mainHeaderStrBuf.append(
						"                                                                <div class=\"vendor-detail\">");
				mainHeaderStrBuf.append(
						"                                                                    <p class=\"english-text\"><span>Data Analytics</span></p>");
				mainHeaderStrBuf.append("                                                                </div>");
				mainHeaderStrBuf.append("                                                            </div>");
				mainHeaderStrBuf.append("                                                        </div>");
				mainHeaderStrBuf.append("                                                  </button>");
				mainHeaderStrBuf.append("                                                </li>");
				mainHeaderStrBuf.append("                                                <li class=\"nav-item\">");
				mainHeaderStrBuf.append(
						"                                                  <button class=\"nav-link\" id=\"v-pills-dataIntegration-tab\" data-toggle=\"pill\" href=\"#v-pills-dataIntegration\" role=\"tab\" aria-controls=\"v-pills-dataIntegration\" aria-selected=\"true\">");
				mainHeaderStrBuf.append(
						"                                                    <div class=\"register-vendor-block\">");
				mainHeaderStrBuf.append(
						"                                                            <div class=\"vendor-image\">");
				mainHeaderStrBuf.append("                                                                <figure>");
				mainHeaderStrBuf.append(
						"                                                                    <img src=\"images/iTransform ETL.png\"");
				mainHeaderStrBuf.append(
						"                                                                        alt=\"vendor-img\" class=\"img-fluid lazy light-mode-img\">");
				mainHeaderStrBuf.append(
						"                                                                    <img src=\"images/iTransform ETL.png\"");
				mainHeaderStrBuf.append(
						"                                                                        alt=\"vendor-img\" class=\"img-fluid lazy dark-mode-img\">");
				mainHeaderStrBuf.append("                                                                </figure>");
				mainHeaderStrBuf.append("                                                            </div>");
				mainHeaderStrBuf.append(
						"                                                            <div class=\"vendor-main-detail\">");
				/*
				 * mainHeaderStrBuf.
				 * append("                                                                <div class=\"more-info-icon\">"
				 * ); mainHeaderStrBuf.
				 * append("                                                                    <span>"
				 * ); mainHeaderStrBuf.
				 * append("                                                                        <figure>"
				 * ); mainHeaderStrBuf.
				 * append("                                                                            <img src=\"images/more-info-layout2-light-icon.svg\""
				 * ); mainHeaderStrBuf.
				 * append("                                                                                alt=\"more-info-icon\""
				 * ); mainHeaderStrBuf.
				 * append("                                                                                class=\"img-fluid lazy light-mode-img\">"
				 * ); mainHeaderStrBuf.
				 * append("                                                                            <img src=\"images/more-info-layout2-dark-icon.svg\""
				 * ); mainHeaderStrBuf.
				 * append("                                                                                alt=\"more-info-icon\""
				 * ); mainHeaderStrBuf.
				 * append("                                                                                class=\"img-fluid lazy dark-mode-img\">"
				 * ); mainHeaderStrBuf.
				 * append("                                                                        </figure>"
				 * ); mainHeaderStrBuf.
				 * append("                                                                    </span>"
				 * ); mainHeaderStrBuf.
				 * append("                                                                </div>"
				 * );
				 */
				mainHeaderStrBuf.append(
						"                                                                <div class=\"vendor-detail\">");
				mainHeaderStrBuf.append(
						"                                                                    <p class=\"english-text\"><span>Data Integration</span></p>");
				mainHeaderStrBuf.append("                                                                </div>");
				mainHeaderStrBuf.append("                                                            </div>");
				mainHeaderStrBuf.append("                                                        </div>");
				mainHeaderStrBuf.append("                                                  </button>");
				mainHeaderStrBuf.append("                                                </li>");
				mainHeaderStrBuf.append("                                                <li class=\"nav-item\">");
				mainHeaderStrBuf.append(
						"                                                  <button class=\"nav-link\" id=\"v-pills-dataMigration-tab\" data-toggle=\"pill\" href=\"#v-pills-dataMigration\" role=\"tab\" aria-controls=\"v-pills-dataMigration\" aria-selected=\"true\">");
				mainHeaderStrBuf.append(
						"                                                    <div class=\"register-vendor-block\">");
				mainHeaderStrBuf.append(
						"                                                            <div class=\"vendor-image\">");
				mainHeaderStrBuf.append("                                                                <figure>");
				mainHeaderStrBuf.append(
						"                                                                    <img src=\"images/DataMigration-icon.png\"");
				mainHeaderStrBuf.append(
						"                                                                        alt=\"vendor-img\" class=\"img-fluid lazy light-mode-img\">");
				mainHeaderStrBuf.append(
						"                                                                    <img src=\"images/vendor-layout2-dark-img2.svg\"");
				mainHeaderStrBuf.append(
						"                                                                        alt=\"vendor-img\" class=\"img-fluid lazy dark-mode-img\">");
				mainHeaderStrBuf.append("                                                                </figure>");
				mainHeaderStrBuf.append("                                                            </div>");
				mainHeaderStrBuf.append(
						"                                                            <div class=\"vendor-main-detail\">");
				/*
				 * mainHeaderStrBuf.
				 * append("                                                                <div class=\"more-info-icon\">"
				 * ); mainHeaderStrBuf.
				 * append("                                                                    <span>"
				 * ); mainHeaderStrBuf.
				 * append("                                                                        <figure>"
				 * ); mainHeaderStrBuf.
				 * append("                                                                            <img src=\"images/more-info-layout2-light-icon.svg\""
				 * ); mainHeaderStrBuf.
				 * append("                                                                                alt=\"more-info-icon\""
				 * ); mainHeaderStrBuf.
				 * append("                                                                                class=\"img-fluid lazy light-mode-img\">"
				 * ); mainHeaderStrBuf.
				 * append("                                                                            <img src=\"images/more-info-layout2-dark-icon.svg\""
				 * ); mainHeaderStrBuf.
				 * append("                                                                                alt=\"more-info-icon\""
				 * ); mainHeaderStrBuf.
				 * append("                                                                                class=\"img-fluid lazy dark-mode-img\">"
				 * ); mainHeaderStrBuf.
				 * append("                                                                        </figure>"
				 * ); mainHeaderStrBuf.
				 * append("                                                                    </span>"
				 * ); mainHeaderStrBuf.
				 * append("                                                                </div>"
				 * );
				 */
				mainHeaderStrBuf.append(
						"                                                                <div class=\"vendor-detail\">");
				mainHeaderStrBuf.append(
						"                                                                    <p class=\"english-text\"><span>Data Migration</span></p>");
				mainHeaderStrBuf.append("                                                                </div>");
				mainHeaderStrBuf.append("                                                            </div>");
				mainHeaderStrBuf.append("                                                    </div>");
				mainHeaderStrBuf.append("                                                  </button>");
				mainHeaderStrBuf.append("                                                </li>");
				mainHeaderStrBuf.append("                                              </ul>");
				mainHeaderStrBuf.append("                                        </div>");
				mainHeaderStrBuf.append("                                    </div>");
				mainHeaderStrBuf.append("                                </div>");
				mainHeaderStrBuf.append("                            </div>");
				mainHeaderStrBuf.append("                        </div>");
				mainHeaderStrBuf.append("                    </div>");
				mainHeaderStrBuf.append("                    <div class=\"main-wrapper-content-part\">");
				mainHeaderStrBuf.append("                        <div class=\"about-us-section\">");
				mainHeaderStrBuf.append("                            <div class=\"about-us-description\">");
				mainHeaderStrBuf.append("                                <div class=\"about-us-content\">");
				mainHeaderStrBuf.append("                                    <div class=\"about-us-left\">");
				mainHeaderStrBuf.append("                                        <div class=\"about-us-block\">");
				mainHeaderStrBuf
						.append("                                            <section class=\"section-title\">");
				/*
				 * mainHeaderStrBuf.
				 * append("                                                <div class=\"title-icon\">"
				 * ); mainHeaderStrBuf.
				 * append("                                                    <figure>");
				 * mainHeaderStrBuf.
				 * append("                                                        <img src=\"images/about-us-title-light-icon.svg\""
				 * ); mainHeaderStrBuf.
				 * append("                                                            alt=\"title-icon\" class=\"img-fluid lazy light-mode-img\">"
				 * ); mainHeaderStrBuf.
				 * append("                                                        <img src=\"images/about-us-title-dark-icon.svg\""
				 * ); mainHeaderStrBuf.
				 * append("                                                            alt=\"title-icon\" class=\"img-fluid lazy dark-mode-img\">"
				 * ); mainHeaderStrBuf.
				 * append("                                                    </figure>");
				 * mainHeaderStrBuf.
				 * append("                                                </div>");
				 */
				mainHeaderStrBuf.append(
						"                                                <h2 class=\"english-text\">About us</h2>");
				mainHeaderStrBuf.append(
						"                                                <h2 class=\"arabic-text\">نبذة عنا</h2>");
				mainHeaderStrBuf.append("                                            </section>");
				mainHeaderStrBuf.append("                                            <div class=\"about-us-detail\">");
				mainHeaderStrBuf.append(
						"                                                <p class=\"english-text\">Integral Analytics is a fast growing company that keeps technology and innovation at the heart of every thing we do. Our goal is to transform facts in to business insights. We go beyond traditional business intelligence by providing organizations with the best Business Intelligence and ETL solutions.");
				mainHeaderStrBuf.append("                                                </p>");
				mainHeaderStrBuf.append("                                            </div>");
				mainHeaderStrBuf.append("                                        </div>");
				mainHeaderStrBuf.append("                                    </div>");
				mainHeaderStrBuf.append("                                    <div class=\"about-us-right\">");
				mainHeaderStrBuf
						.append("                                        <div class=\"about-benefits-slider\">");
				mainHeaderStrBuf.append("                                            <div class=\"benefits-title\">");
				mainHeaderStrBuf.append("                                                <div class=\"title-icon\">");
				mainHeaderStrBuf.append("                                                    <figure>");
				mainHeaderStrBuf.append(
						"                                                        <img src=\"images/about-benefits-title-light-icon.svg\"");
				mainHeaderStrBuf.append(
						"                                                            alt=\"title-icon\" class=\"img-fluid lazy light-mode-img\">");
				mainHeaderStrBuf.append(
						"                                                        <img src=\"images/about-benefits-title-dark-icon.svg\"");
				mainHeaderStrBuf.append(
						"                                                            alt=\"title-icon\" class=\"img-fluid lazy dark-mode-img\">");
				mainHeaderStrBuf.append("                                                    </figure>");
				mainHeaderStrBuf.append("                                                </div>");
				mainHeaderStrBuf.append(
						"                                                <span class=\"english-text\">Benfits of Smart IG</span>");
				mainHeaderStrBuf.append("                                            </div>");
				mainHeaderStrBuf.append(
						"                                            <div class=\"about-benefits-carousel slick-slider\" id=\"about-benefits-carousel\">");
				mainHeaderStrBuf
						.append("                                                <div class=\"about-benefits-item\">");
				mainHeaderStrBuf.append(
						"                                                    <div class=\"about-benefits-block\">");
				mainHeaderStrBuf.append(
						"                                                        <div class=\"section-title\">");
				mainHeaderStrBuf.append(
						"                                                            <p class=\"english-text\">Makes it easier for users to create subsets of data and build visualizations on that</p>");
				mainHeaderStrBuf.append("                                                        </div>");
				mainHeaderStrBuf.append("                                                    </div>");
				mainHeaderStrBuf.append("                                                </div>");
				mainHeaderStrBuf
						.append("                                                <div class=\"about-benefits-item\">");
				mainHeaderStrBuf.append(
						"                                                    <div class=\"about-benefits-block\">");
				mainHeaderStrBuf.append(
						"                                                        <div class=\"section-title\">");
				mainHeaderStrBuf.append(
						"                                                            <p class=\"english-text\">Users can easily share their reports with other users of the organization</p>");
				mainHeaderStrBuf.append("                                                        </div>");
				mainHeaderStrBuf.append("                                                    </div>");
				mainHeaderStrBuf.append("                                                </div>");
				mainHeaderStrBuf
						.append("                                                <div class=\"about-benefits-item\">");
				mainHeaderStrBuf.append(
						"                                                    <div class=\"about-benefits-block\">");
				mainHeaderStrBuf.append(
						"                                                        <div class=\"section-title\">");
				mainHeaderStrBuf.append(
						"                                                            <p class=\"english-text\">It supports Artificial Intelligence in Data Analytics that users can leverage to pre-process and keep the data ready for visualization</p>");
				mainHeaderStrBuf.append("                                                        </div>");
				mainHeaderStrBuf.append("                                                    </div>");
				mainHeaderStrBuf.append("                                                </div>");
				mainHeaderStrBuf
						.append("                                                <div class=\"about-benefits-item\">");
				mainHeaderStrBuf.append(
						"                                                    <div class=\"about-benefits-block\">");
				mainHeaderStrBuf.append(
						"                                                        <div class=\"section-title\">");
				mainHeaderStrBuf.append(
						"                                                            <p class=\"english-text\">Allows users to divide and slice complex data models into a simpler form and separate charts are also built on it</p>");
				mainHeaderStrBuf.append("                                                        </div>");
				mainHeaderStrBuf.append("                                                    </div>");
				mainHeaderStrBuf.append("                                                </div>");
				mainHeaderStrBuf
						.append("                                                <div class=\"about-benefits-item\">");
				mainHeaderStrBuf.append(
						"                                                    <div class=\"about-benefits-block\">");
				mainHeaderStrBuf.append(
						"                                                        <div class=\"section-title\">");
				mainHeaderStrBuf.append(
						"                                                            <p class=\"english-text\">Offers integrations with many connectors that allow users to connect to various data sources</p>");
				mainHeaderStrBuf.append("                                                        </div>");
				mainHeaderStrBuf.append("                                                    </div>");
				mainHeaderStrBuf.append("                                                </div>");
				mainHeaderStrBuf
						.append("                                                <div class=\"about-benefits-item\">");
				mainHeaderStrBuf.append(
						"                                                    <div class=\"about-benefits-block\">");
				mainHeaderStrBuf.append(
						"                                                        <div class=\"section-title\">");
				mainHeaderStrBuf.append(
						"                                                            <p class=\"english-text\">AI driven solutions for Improving operational efficiency</p>");
				mainHeaderStrBuf.append("                                                        </div>");
				mainHeaderStrBuf.append("                                                    </div>");
				mainHeaderStrBuf.append("                                                </div>");
				mainHeaderStrBuf
						.append("                                                <div class=\"about-benefits-item\">");
				mainHeaderStrBuf.append(
						"                                                    <div class=\"about-benefits-block\">");
				mainHeaderStrBuf.append(
						"                                                        <div class=\"section-title\">");
				mainHeaderStrBuf.append(
						"                                                            <p class=\"english-text\"> Increased Agility</p>");
				mainHeaderStrBuf.append("                                                        </div>");
				mainHeaderStrBuf.append("                                                    </div>");
				mainHeaderStrBuf.append("                                                </div>");
				mainHeaderStrBuf
						.append("                                                <div class=\"about-benefits-item\">");
				mainHeaderStrBuf.append(
						"                                                    <div class=\"about-benefits-block\">");
				mainHeaderStrBuf.append(
						"                                                        <div class=\"section-title\">");
				mainHeaderStrBuf.append(
						"                                                            <p class=\"english-text\">Data-driven decision-making</p>");
				mainHeaderStrBuf.append("                                                        </div>");
				mainHeaderStrBuf.append("                                                    </div>");
				mainHeaderStrBuf.append("                                                </div>");
				mainHeaderStrBuf
						.append("                                                <div class=\"about-benefits-item\">");
				mainHeaderStrBuf.append(
						"                                                    <div class=\"about-benefits-block\">");
				mainHeaderStrBuf.append(
						"                                                        <div class=\"section-title\">");
				mainHeaderStrBuf.append(
						"                                                            <p class=\"english-text\">High-quality interactive data visualizations</p>");
				mainHeaderStrBuf.append("                                                        </div>");
				mainHeaderStrBuf.append("                                                    </div>");
				mainHeaderStrBuf.append("                                                </div>");
				mainHeaderStrBuf
						.append("                                                <div class=\"about-benefits-item\">");
				mainHeaderStrBuf.append(
						"                                                    <div class=\"about-benefits-block\">");
				mainHeaderStrBuf.append(
						"                                                        <div class=\"section-title\">");
				mainHeaderStrBuf.append(
						"                                                            <p class=\"english-text\">Helps customers to make faster and accurate decisions by leveraging the existing data, at the right  time</p>");
				mainHeaderStrBuf.append("                                                        </div>");
				mainHeaderStrBuf.append("                                                    </div>");
				mainHeaderStrBuf.append("                                                </div>");
				mainHeaderStrBuf
						.append("                                                <div class=\"about-benefits-item\">");
				mainHeaderStrBuf.append(
						"                                                    <div class=\"about-benefits-block\">");
				mainHeaderStrBuf.append(
						"                                                        <div class=\"section-title\">");
				mainHeaderStrBuf.append(
						"                                                            <p class=\"english-text\">Strong user experience (UX) for customers</p>");
				mainHeaderStrBuf.append("                                                        </div>");
				mainHeaderStrBuf.append("                                                    </div>");
				mainHeaderStrBuf.append("                                                </div>");
				mainHeaderStrBuf
						.append("                                                <div class=\"about-benefits-item\">");
				mainHeaderStrBuf.append(
						"                                                    <div class=\"about-benefits-block\">");
				mainHeaderStrBuf.append(
						"                                                        <div class=\"section-title\">");
				mainHeaderStrBuf.append(
						"                                                            <p class=\"english-text\">Auto-Suggested charts gives users ready to use charts</p>");
				mainHeaderStrBuf.append("                                                        </div>");
				mainHeaderStrBuf.append("                                                    </div>");
				mainHeaderStrBuf.append("                                                </div>");
				mainHeaderStrBuf.append("                                            </div>");
				mainHeaderStrBuf.append("                                        </div>");
				mainHeaderStrBuf.append("                                    </div>");
				mainHeaderStrBuf.append("                                </div>");
				mainHeaderStrBuf.append("                            </div>");
				mainHeaderStrBuf.append("                        </div>");
				mainHeaderStrBuf.append("                        <div class=\"fact-stats-section\">");
				mainHeaderStrBuf.append("                            <div class=\"fact-stats-description\">");
				mainHeaderStrBuf.append("");
				mainHeaderStrBuf.append(
						"                                <div class=\"tab-content\" id=\"v-pills-tabContent\">");
				mainHeaderStrBuf.append(
						"                                    <div class=\"tab-pane fade show active\" id=\"v-pills-dataAnalytics\" role=\"tabpanel\" aria-labelledby=\"v-pills-dataAnalytics-tab\">");
				mainHeaderStrBuf.append("                                        <ul>");
				mainHeaderStrBuf.append("                                            <li>");
				mainHeaderStrBuf.append(
						"                                                <p>Data analytics refers to the process of examining, transforming, and interpreting data to extract    meaningful insights that support informed business decisions</p>");
				mainHeaderStrBuf.append("                                            </li>");
				mainHeaderStrBuf.append("                                            <li>");
				mainHeaderStrBuf.append(
						"                                                <p>It encompasses a variety of techniques that allow organizations to analyze historical and current data, identify trends, patterns, and correlations, and make predictions about future outcomes.</p>");
				mainHeaderStrBuf.append("                                            </li>");
				mainHeaderStrBuf.append("                                            <li>");
				mainHeaderStrBuf.append(
						"                                                <p> Use interactive dashboards to monitor key performance indicators (KPIs) and metrics in real-time.</p>");
				mainHeaderStrBuf.append("                                            </li>");
				mainHeaderStrBuf.append("                                            <li>");
				mainHeaderStrBuf.append(
						"                                                <p>Employ charts, graphs, maps to make data easier to understand and interpret.</p>");
				mainHeaderStrBuf.append("                                            </li>");
				mainHeaderStrBuf.append("                                            <li>");
				mainHeaderStrBuf.append(
						"                                                <p>Dive deeper into data which also known as Drill-Down Analysis to explore specific details behind high-level trends or metrics.</p>");
				mainHeaderStrBuf.append("                                            </li>");
				mainHeaderStrBuf.append("                                            <li>");
				mainHeaderStrBuf.append(
						"                                                <p>Supports long-term strategic planning by providing insights into market trends, customer behaviour, and operational efficiency.</p>");
				mainHeaderStrBuf.append("                                            </li>");
				mainHeaderStrBuf.append("                                            <li>");
				mainHeaderStrBuf.append(
						"                                                <p>Enables businesses to optimize operations by identifying inefficiencies and areas for cost savings.</p>");
				mainHeaderStrBuf.append("                                            </li>");
				mainHeaderStrBuf.append("                                        </ul>");
				mainHeaderStrBuf.append("                                    </div>");
				mainHeaderStrBuf.append(
						"                                    <div class=\"tab-pane fade\" id=\"v-pills-dataIntegration\" role=\"tabpanel\" aria-labelledby=\"v-pills-dataIntegration-tab\">");
				mainHeaderStrBuf.append("                                        <ul>");
				mainHeaderStrBuf.append("                                            <li>");
				mainHeaderStrBuf.append(
						"                                                <p>Data integration involves combining data from different sources into a cohesive, unified view. </p>");
				mainHeaderStrBuf.append("                                            </li>");
				mainHeaderStrBuf.append("                                            <li>");
				mainHeaderStrBuf.append(
						"                                                <p>Smart IG deals best with various data inputs such as flat files, databases, and ERP systems. Flat files might contain large, unstructured data sets, databases provide structured and relational data, while ERP systems offer comprehensive, real-time data across business processes.</p>");
				mainHeaderStrBuf.append("                                            </li>");
				mainHeaderStrBuf.append("                                            <li>");
				mainHeaderStrBuf.append(
						"                                                <p>Integrating these diverse sources ensures consistent, accurate, and up-to-date information.</p>");
				mainHeaderStrBuf.append("                                            </li>");
				mainHeaderStrBuf.append("                                            <li>");
				mainHeaderStrBuf.append(
						"                                                <p>It also enables better decision-making and streamlined operations.</p>");
				mainHeaderStrBuf.append("                                            </li>");
				mainHeaderStrBuf.append("                            			 </ul>");
				mainHeaderStrBuf.append("                        			</div>");
				mainHeaderStrBuf.append(
						"                                    <div class=\"tab-pane fade\" id=\"v-pills-dataMigration\" role=\"tabpanel\" aria-labelledby=\"v-pills-dataMigration-tab\">");
				mainHeaderStrBuf.append("                                        <ul>");
				mainHeaderStrBuf.append(
						"                                        	 <h4 class=\"dataMigration-header\">Data migration from diverse sources, such as flat files, databases, and ERPs, requires a well-coordinated approach that works best with IG  ensuring accuracy and consistency.</h4>");
				mainHeaderStrBuf.append(
						"                                        	 <h5 class=\"dataMigration-discription\">The process involves several key functions:</h5>");
				mainHeaderStrBuf.append("                                            <li>");
				mainHeaderStrBuf.append(
						"                                                <p>Data Extraction: Extract data from various sources using and connectors for flat files, databases, and ERP systems.</p>");
				mainHeaderStrBuf.append("                                            </li>");
				mainHeaderStrBuf.append("                                            <li>");
				mainHeaderStrBuf.append(
						"                                                <p>Data Transformation: Transform the extracted data to fit the target schema, handling format conversions, data pre-processing, and standardization.</p>");
				mainHeaderStrBuf.append("                                            </li>");
				mainHeaderStrBuf.append("                                            <li>");
				mainHeaderStrBuf.append(
						"                                                <p>Data Loading: Load the transformed data into the target system, ensuring it aligns with the required structure and integrity constraints.</p>");
				mainHeaderStrBuf.append("                                            </li>");
				mainHeaderStrBuf.append("                                            <li>");
				mainHeaderStrBuf.append(
						"                                                <p>Coordination and Management: It would manage and monitor the data migration process, ensuring synchronization and error handling across all steps.</p>");
				mainHeaderStrBuf.append("                                            </li>");
				mainHeaderStrBuf.append("                            			 </ul>");
				mainHeaderStrBuf.append("                                  </div>");
				mainHeaderStrBuf.append("                                  </div>");
				mainHeaderStrBuf.append("                            </div>");
				mainHeaderStrBuf.append("                        </div>");
				mainHeaderStrBuf.append("                    </div>");
				mainHeaderStrBuf.append("                </div>");
				mainHeaderStrBuf.append("            </div>");
				mainHeaderStrBuf.append("        </div>");
				mainHeaderStrBuf.append("    </main>");

				mainHeaderStrBuf.append(" <div class=\"footer\">");
				mainHeaderStrBuf.append("    <div class=\"container\">");
				mainHeaderStrBuf.append("       <div class=\"row\">");
				mainHeaderStrBuf.append("           <div class=\"col-xl-6 col-md-6 col-sm-12\">  ");
				mainHeaderStrBuf.append("               <ul class=\"footer-social-icons\">");
				mainHeaderStrBuf.append("                   <li>");
				mainHeaderStrBuf.append(
						"                     <a href=\"https://www.linkedin.com/company/89702458/admin\" title=\"Linked In\" target=\"_blank\" class=\"linkedin_Icon\"><i class=\"fa fa-linkedin\" aria-hidden=\"true\"></i></a>");
				mainHeaderStrBuf.append("                    </li>");
				mainHeaderStrBuf.append("                   <li>");
				mainHeaderStrBuf.append(
						"                       <a href=\"https://www.facebook.com/profile.php?id=100087005881195\" class=\'facebook_Icon\' title=\"Facebook\" target=\"_blank\"><i class=\"fa fa-facebook\" aria-hidden=\"true\"></i></a>");
				mainHeaderStrBuf.append("                    </li>");
				mainHeaderStrBuf.append("                     <li class=\"header-notification\">");
				mainHeaderStrBuf.append(
						"                        <a href=\"https://www.instagram.com/integralanalytics/\" title=\"Instagram\" target=\"_blank\" class=\"insta_Icon\"><i class=\"fa fa-instagram\" aria-hidden=\"true\"></i></a>");
				mainHeaderStrBuf.append("                    </li>");
				mainHeaderStrBuf.append("                    <li class=\"header-notification\">");
				mainHeaderStrBuf.append(
						"                        <a href=\"https://twitter.com/integral_ana\" title=\"Twitter\" target=\"_blank\" class=\"twitter_icon\"><svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 512 512\"><path d=\"M389.2 48h70.6L305.6 224.2 487 464H345L233.7 318.6 106.5 464H35.8L200.7 275.5 26.8 48H172.4L272.9 180.9 389.2 48zM364.4 421.8h39.1L151.1 88h-42L364.4 421.8z\"></path></svg></a>");
				mainHeaderStrBuf.append("                    </li>");
				mainHeaderStrBuf.append("                    <li class=\"header-notification\">");
				mainHeaderStrBuf.append(
						"                         <a href=\"https://www.youtube.com/channel/UC0bkzl0wBcx1r9qtqgOzt6Q\" title=\"You Tube\" target=\"_blank\" class=\"youtube_Icon\"><i class=\"fa fa-youtube-play\" aria-hidden=\"true\"></i></a>");
				mainHeaderStrBuf.append("                    </li>");
				mainHeaderStrBuf.append("                </ul>");
				mainHeaderStrBuf.append("            </div>");
				mainHeaderStrBuf.append("            <div class=\"col-xl-6 col-md-6 col-sm-12 text-right\">");
				mainHeaderStrBuf.append(
						"                 <fmt:formatDate var=\"currentYear\" value=\"<%=new java.util.Date()%>\" pattern=\"yyyy\" />");
				mainHeaderStrBuf.append("                <p>Copyright &copy; 2024 Smart Integraphics</p>");
				mainHeaderStrBuf.append("            </div>");
				mainHeaderStrBuf.append("        </div>");
				mainHeaderStrBuf.append("    </div>");
				mainHeaderStrBuf.append(" </div>");

				mainHeaderStrBuf.append("  </div>");
			} catch (Exception e) {
				e.printStackTrace();
			}
			mainHeader = mainHeaderStrBuf.toString();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return mainHeader;
	}

	public String convertNumToCharNameFormat(int number) {
		String countVal = Integer.toString(number);
		try {
			if (number >= 1000000000) {
				countVal = String.format(
						"%.2f <span class=\"english-text\" style='font-size: 18px;'>Billions+</span><span class=\"arabic-text\">Billions</span>",
						number / 1000000000.0);
			} else if (number >= 1000000) {
				countVal = String.format(
						"%.2f <span class=\"english-text\" style='font-size: 18px;'>Millions+</span><span class=\"arabic-text\">الملايين</span>",
						number / 1000000.0);
			} else if (number >= 100000) {
				countVal = String.format(
						"%.2f <span class=\"english-text\" style='font-size: 18px;'>Lakhs+</span><span class=\"arabic-text\">لاكس</span>",
						number / 100000.0);
			} else if (number >= 1000) {
				NumberFormat nf = NumberFormat.getNumberInstance();
				double f = Double.parseDouble(countVal);
				nf.setGroupingUsed(true);
				countVal = nf.format(f);
			}
//            else if (number >= 1000) {
//                countVal= String.format("%.0f+ <span class=\"english-text\" style='font-size: 18px;'></span><span class=\"arabic-text\"></span>", number / 1000.0);
//            }
		} catch (Exception e) {
			e.printStackTrace();
		}
		return countVal;
	}

	@RequestMapping(value = "/getNIICFeedbackForm", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject getFeedbackForm(HttpServletRequest request) {
		JSONObject resultObject = null;
		try {
			resultObject = loginHandlerService.getFeedbackForm(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObject;
	}

	@RequestMapping(value = "/getNIICSaveFeedbackForm", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject getNIICSaveFeedbackForm(HttpServletRequest request) {
		JSONObject resultObject = null;
		try {

			resultObject = loginHandlerService.getNIICSaveFeedbackForm(request);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObject;
	}

	@RequestMapping(value = { "/setuserProfileUpdateIcon" }, method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject setuserProfileUpdateIcon(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = loginHandlerService.setuserProfileUpdateIcon(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	@RequestMapping(value = { "/removeUserProfilePic" }, method = { RequestMethod.GET,
			RequestMethod.POST }, produces = { "text/plain;charset=UTF-8" })
	public @ResponseBody String removeUserProfilePic(HttpServletRequest request) {
		String resultstr = "";
		try {
			resultstr = this.loginHandlerService.removeUserProfilePic(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultstr;
	}

}
