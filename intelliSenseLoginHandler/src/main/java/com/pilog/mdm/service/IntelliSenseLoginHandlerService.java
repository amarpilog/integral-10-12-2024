/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.service;

import com.pilog.mdm.DAO.IntelliSenseLoginHandlerDAO;
import com.pilog.mdm.DTO.LoginHandler;
import com.pilog.mdm.pojo.BApplProperties;
import com.pilog.mdm.utilities.PilogUtilities;

import java.sql.Clob;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

/**
 *
 * @author Devint01
 */
@Service
public class IntelliSenseLoginHandlerService {

	@Autowired
	private ApplicationContext appContext;
	@Autowired
	private IntelliSenseLoginHandlerDAO loginHandlerDAO;

	public JSONObject loginHandler(LoginHandler loginHandler, HttpServletRequest request) {

		JSONObject resultObj = new JSONObject();
		try {
			resultObj = loginHandlerDAO.loginHandler(loginHandler, request);
			String result = (String) resultObj.get("resultCode");
			if ("userPasswordInvalid2".equalsIgnoreCase(result) || "userPasswordInvalid".equalsIgnoreCase(result)) {
				loginHandlerDAO.updateLOginFailureAttempts(loginHandler.getRsUsername(), false);
			} else if ("success".equalsIgnoreCase(result) || "passwordReset".equalsIgnoreCase(result)
					|| "passwordExpiryReset".equalsIgnoreCase(result) || "passwordExpiryReset".equalsIgnoreCase(result)
					|| "passwordFirstLogin".equalsIgnoreCase(result) || "alreadyLoggedIn".equalsIgnoreCase(result)
					|| (result != null && !"".equalsIgnoreCase(result) && result.startsWith("1"))) {
				loginHandlerDAO.updateLOginFailureAttempts(loginHandler.getRsUsername(), true);

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public void getLabelsListByLocale(HttpServletRequest request) {

		try {

//            genericDAO.getLabelsListByLocale(request);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public JSONObject getLogonDetails(HttpServletRequest request, String sessionId, String ssUsername) {
		JSONObject jSONObject = new JSONObject();

		try {

			jSONObject = loginHandlerDAO.getLogonDetails(request, sessionId, ssUsername);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jSONObject;
	}

	public Object getApplProperties(String keyName) {
		return loginHandlerDAO.getApplProperties(keyName);
	}

	public JSONObject cloudLogout(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			/*
			 * String persId = request.getParameter("ssUserPersId"); String sessionId =
			 * request.getParameter("sessionId");
			 */
			// onkar
			HttpSession session = request.getSession(false);
			String persId = (String) session.getAttribute("ssUserPersId");
			String sessionId = session.getId();

			// onkar
			int updateCount = loginHandlerDAO.cloudLogout(persId, sessionId);
			if (updateCount != 0) {
				resultObj.put("message", "Success");
				resultObj.put("flag", true);
			} else {
				resultObj.put("message", "fail");
				resultObj.put("flag", true);
			}
		} catch (Exception e) {
			e.printStackTrace();
			resultObj.put("message", e.getMessage());
			resultObj.put("flag", false);
		}
		return resultObj;
	}

	public String getDomainSelectionTypes(HttpServletRequest request) {
		String resultStr = "";
		try {
			BApplProperties resultObj = (BApplProperties) loginHandlerDAO
					.getApplProperties("DXP_DOMAIN_SELECTION_TYPES");
			if (resultObj != null) {
				String selectionTypes = resultObj.getId().getProcessValue();
				if (selectionTypes != null && !"".equalsIgnoreCase(selectionTypes)
						&& !"null".equalsIgnoreCase(selectionTypes)) {
					if (selectionTypes.contains(",")) {
						String[] typesArr = selectionTypes.split(",");
						resultStr = "<select class='form-control fill' id='SelectedValue' name='sellist1' onchange='changeSelect()'>";
						for (int i = 0; i < typesArr.length; i++) {
							resultStr += "<option value='" + typesArr[i] + "'>" + typesArr[i] + "</option>";
						}
						resultStr += "</select>";

					}
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultStr;
	}

	public int getCountBasedOnDomainName(HttpServletRequest request, String domainType) {
		int count = 0;
		try {
			String query = "";
			if (domainType != null && !"".equalsIgnoreCase(domainType) && !"null".equalsIgnoreCase(domainType)
					&& "PRODUCT_COUNT".equalsIgnoreCase(domainType)) {
				query = "SELECT COUNT(*) FROM O_RECORD_DATA_UNIFICATION_STG ";
			} else if (domainType != null && !"".equalsIgnoreCase(domainType) && !"null".equalsIgnoreCase(domainType)
					&& "VENDOR_COUNT".equalsIgnoreCase(domainType)) {
				query = "SELECT COUNT (DISTINCT ROLE_ID) FROM O_RECORD_VISUALIZATION ";
			} else if (domainType != null && !"".equalsIgnoreCase(domainType) && !"null".equalsIgnoreCase(domainType)
					&& "CLASSES_COUNT".equalsIgnoreCase(domainType)) {
				query = "SELECT COUNT (DISTINCT DASHBORD_NAME) FROM O_RECORD_VISUALIZATION";
			} else if (domainType != null && !"".equalsIgnoreCase(domainType) && !"null".equalsIgnoreCase(domainType)
					&& "PPR_COUNT".equalsIgnoreCase(domainType)) {
				query = "SELECT COUNT(CHART_ID) FROM O_RECORD_VISUALIZATION ";
			}
			count = loginHandlerDAO.getCountBasedOnDomainName(request, query);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return count;
	}

	public List getAVGFeedbackRating(HttpServletRequest request) {
		// TODO Auto-generated method stub
		return loginHandlerDAO.getAVGFeedbackRating(request);
	}

	public JSONObject getFeedbackForm(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String orgnId = (String) request.getSession(false).getAttribute("ssOrgId");

			String feedBackData = "";
			BApplProperties feedbackObj = (BApplProperties) loginHandlerDAO.getApplProperties("FEEDBACK_PRODUCTS");
			if (feedbackObj != null) {
				feedBackData = feedbackObj.getId().getProcessValue();
				String feedbackStr = "";
				if (feedBackData != null && !"".equalsIgnoreCase(feedBackData)
						&& !"null".equalsIgnoreCase(feedBackData)) {
					String feedBackArr[] = feedBackData.split(",");
					if (feedBackArr != null && feedBackArr.length > 0) {
						feedbackStr = "<div class=\"reviewclass\">";

						for (int i = 0; i < feedBackArr.length; i++) {
							feedbackStr += "<div class=\"review-item\">";
							feedbackStr += "<span id='" + feedBackArr[i] + "_id'>" + feedBackArr[i] + "</span>"
									+ "<div id='jqxRating" + i + "'></div>";
							feedbackStr += "</div>";
						}
						feedbackStr += "<div class=\"customer-feedback-form\"><label>Name</label><div class=\"customer-feedback\"><input id='niicFeedbackUserId' type='text' >";
						feedbackStr += "<div id=\"nameError\" class=\"error-message\"></div></div></div>";
						feedbackStr += "<div class=\"customer-feedback-form\"><label>Email</label> <div class=\"customer-feedback\"><input id='niicFeedbackUserEmailId' type='text' >";
						feedbackStr += "<div id=\"emailError\" class=\"error-message\"></div></div></div>";
						feedbackStr += "</div>";
						resultObj.put("feedBackratingCount", feedBackArr.length);
					}
				}
				resultObj.put("feedBackResponse", feedbackStr);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getNIICSaveFeedbackForm(HttpServletRequest request) {
		return loginHandlerDAO.getNIICSaveFeedbackForm(request);
	}

	public StringBuilder getTwitterData(HttpServletRequest request) {
		StringBuilder result = new StringBuilder();
		try {
			String dataStr = "";
			String text1 = "";
			String text2 = "";
			String text3 = "";
			String image1 = "FollowersEmoji.png";
			String image2 = "LikesEmoji.png";
			String image3 = "RetweetEmoji.png";
			String strData = "";
			List resultList = loginHandlerDAO.getTwitterData(request);
			if (resultList != null && !resultList.isEmpty()) {
				String tweetStr = "";
				String createDate = "";
				String hrefLink = "";
				String[] createDateArr = null;
				for (Object dataObj : resultList) {
					Object[] resultArr = (Object[]) dataObj;

					if (resultArr[1] instanceof Clob) {
						tweetStr = new PilogUtilities().clobToString((Clob) resultArr[1]);
					} else {
						tweetStr = resultArr[1].toString();
					}

					if (resultArr[2] != null) {
						hrefLink = resultArr[2].toString();
					} else {
						hrefLink = "https://english.jagran.com/";
					}
					if (resultArr[6] != null) {
						Date date = new SimpleDateFormat("yyyy-MM-dd").parse(resultArr[6].toString());
						createDate = new SimpleDateFormat("dd-MM-yyyy").format(date);
					} else {
						createDate = "01/01/2022";
					}
//                    createDateArr = createDate.split(" ");
					strData += "<li>" + "<p class=\"postedBy\"><span class=\"postedName\">" + resultArr[0]
							+ "</span><sup class=\"postedDate\">" + createDate + "</sup></p>" + "<a href='" + hrefLink
							+ "' target='_blank' title=\"" + resultArr[0] + "\">" + tweetStr + "</a>"
							+ "<div class=\"Twit_Likes\"><div class=\"countsDiv\"><span class=\"countIcon\" "
							+ "title=\"" + text1 + "\"><img src=\"images/" + image1 + "\" title=\"" + text1 + "\" "
							+ "class=\"dxpSocialImageClass themeModeDark\"></span><span class=\"space\">" + resultArr[5]
							+ "</span></div>" + "<div class=\"countsDiv\"><span class=\"countIcon\" title=\"" + text2
							+ "\">" + "<img src=\"images/" + image2 + "\" title=\"" + text2 + "\" "
							+ "class=\"dxpSocialImageClass themeModeDark\"></span><span class=\"space\">" + resultArr[3]
							+ "</span></div>" + "<div class=\"countsDiv\"><span class=\"countIcon\" title=\"" + text3
							+ "\">" + "<img src=\"images/" + image3 + "\" title=\"followers\" "
							+ "class=\"dxpSocialImageClass themeModeDark\"></span><span class=\"space\">" + resultArr[4]
							+ "</span></div>" + "<div class=\"countsDiv\"><span class=\"countIcon\" title=\"" + text3
							+ "\">"
							+ "<img src=\"images/Sentiment-Icon.png\" onclick=\"getYoutubeHomeSentimentAnalysis('"
							+ resultArr[7] + "')\" title=\"Sentiment Analysis\" "
							+ "class=\"dxpSocialImageClass themeModeDark\"></span><span class=\"space\">Sentiment</span>&nbsp;"
							+ "</div>" + "</div>" + "</li>";

				}
				result.append(strData);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public List getEvents(HttpServletRequest request, List listData) {
		return loginHandlerDAO.getEvents(request, listData);
	}

	public JSONObject setuserProfileUpdateIcon(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String fileName = request.getParameter("fileName");
			if (isValidFileExtension(fileName)) {
				resultObj = loginHandlerDAO.setuserProfileUpdateIcon(request);
			} else {
				resultObj.put("message", "invalid file extension");
				resultObj.put("flag", Boolean.valueOf(false));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	private boolean isValidFileExtension(String fileName) {
		String[] allowedExtensions = { "jpg", "jpeg", "png" };
		String fileExtension = fileName.substring(fileName.lastIndexOf('.') + 1).toLowerCase();
		if (fileName.lastIndexOf('.') > 0) {
			String baseName = fileName.substring(0, fileName.lastIndexOf('.'));
			int lastDotIndexInBaseName = baseName.lastIndexOf('.');
			if (lastDotIndexInBaseName >= 0)
				return false;
		}
		for (String allowedExtension : allowedExtensions) {
			if (allowedExtension.equals(fileExtension))
				return true;
		}
		return false;
	}

	public String removeUserProfilePic(HttpServletRequest request) {
		return loginHandlerDAO.removeUserProfilePic(request);
	}

	public void updateLogOutUserNameOtherSessionIds(String username, HttpServletRequest request) {
		try {
			this.loginHandlerDAO.updateLogOutUserNameOtherSessionIds(username, request);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
