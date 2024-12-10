package com.pilog.mdm.Controller;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.pilog.mdm.Service.DashBoardsConversationService;
import com.pilog.mdm.Service.DashBoardsService;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import java.sql.Blob;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author Jagadish.R
 */

@Controller
public class DashBoardConversationController {
	@Autowired
	public DashBoardsConversationService dashBoardsService;

	@RequestMapping(value = "/getConversationalAIMessage", produces = { "application/json" })
	public @ResponseBody JSONObject getConversationalAIMessage(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsService.getConversationalAIMessage(request);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return resultObj;
	}

	@RequestMapping(value = "/getUserTableNamesData", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject getUserTableNames(HttpServletRequest request) {
		JSONObject resultObject = null;
		try {
			resultObject = dashBoardsService.getUserTableNames(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObject;
	}

	@RequestMapping(value = "/getAILensInsightsUserExistTableNamesData", method = { RequestMethod.GET,
			RequestMethod.POST })
	public @ResponseBody JSONObject getAILensInsightsUserExistTableNamesData(HttpServletRequest request) {
		JSONObject resultObject = null;
		try {
			resultObject = dashBoardsService.getAILensInsightsUserExistTableNamesData(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObject;
	}

	@RequestMapping(value = "/getAILensInsightsAnalyticsQuestions", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject getAILensInsightsAnalyticsQuestions(HttpServletRequest request) {
		JSONObject resultObject = null;
		try {
			resultObject = dashBoardsService.getAILensInsightsAnalyticsQuestions(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObject;
	}

	@RequestMapping(value = "/getAILensInsightsAnalyticsInsights", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject getAILensInsightsAnalyticsInsights(HttpServletRequest request) {
		JSONObject resultObject = null;
		try {
			resultObject = dashBoardsService.getAILensInsightsAnalyticsInsights(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObject;
	}

	@RequestMapping(value = "/getUserMergeTableNamesData", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject getUserMergeTableNames(HttpServletRequest request) {
		JSONObject resultObject = null;
		try {
			resultObject = dashBoardsService.getUserMergeTableNames(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObject;
	}

	@RequestMapping(value = "/getUserMergeTableNamesColumns", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject getUserMergeTableNamesColumns(HttpServletRequest request) {
		JSONObject resultObject = null;
		try {
			resultObject = dashBoardsService.getUserMergeTableNamesColumns(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObject;
	}

	@RequestMapping(value = "/getUserSearchData", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject getChatBotResponse(HttpServletRequest request) {
		JSONObject resultObject = null;
		try {
			resultObject = dashBoardsService.getUserSearchData(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObject;
	}

	@RequestMapping(value = { "/getVoiceSuggestedChartsBasedonColumns" }, method = { RequestMethod.POST,
			RequestMethod.GET }, produces = { "application/json" })
	public @ResponseBody JSONObject getVoiceSuggestedChartsBasedonColumns(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsService.getVoiceSuggestedChartsBasedonColumns(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	@RequestMapping(value = "/getInsightsView", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject getInsightsView(HttpServletRequest request) {
		JSONObject resultObject = new JSONObject();
		try {
			resultObject = dashBoardsService.getInsightsView(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObject;
	}

	@RequestMapping(value = "/executeInsightsSQLQuery", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject executeInsightsSQLQuery(HttpServletRequest request) {
		JSONObject resultObject = new JSONObject();
		try {
			resultObject = dashBoardsService.executeInsightsSQLQuery(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObject;
	}

	@RequestMapping(value = "/showAITypedValueResults", method = { RequestMethod.POST,
			RequestMethod.GET }, produces = "text/plain;charset=UTF-8")
	public @ResponseBody String showAITypedValueResults(HttpServletRequest request) {
		JSONObject rersultObj = new JSONObject();
		try {
			String finalResult = "";
			String result = "";
			JSONObject resultObjFromPython = new JSONObject();
			// String result ="HI" ;//dashBoardsService.showAITypedValueResults(request);
			resultObjFromPython = dashBoardsService.getResultFromPythonApi(request);
//			if(resultObjFromPython!= null && !resultObjFromPython.isEmpty()) {
			Boolean statusFromPython = (Boolean) resultObjFromPython.get("STATUS");
			if (statusFromPython) {
				//result = (String) resultObjFromPython.get("AI_LENS_QUES_ANS");
				result = (String) resultObjFromPython.get("ANSWER");
				resultObjFromPython.put("HAS_SUB_CHILD", "Y");
//					rersultObj.put("DOMAIN",resultObjFromPython.get("DOMAIN"));
//					rersultObj.put("QUESTION_ID",resultObjFromPython.get("QUESTION_ID"));
//					rersultObj.put("ANSWER",resultObjFromPython.get("ANSWER"));
				rersultObj.put("QUESTION_OBJ", resultObjFromPython.toJSONString());

			}
//			}
			else {
				result = dashBoardsService.showAITypedValueResults(request);
			}
			if (result != null && !"".equalsIgnoreCase(result) && !"null".equalsIgnoreCase(result)
					&& result.contains("\\n") || result.contains("\n\n")) {
				String newResult = result.replace("\\n", "__");
				String updatedResult = newResult.replace("____", "<br>").replace("___", "<br>").replace("__", "<br>");
				Pattern pattern = Pattern.compile("<br>([^>]*):");
				Matcher matcher = pattern.matcher(updatedResult);
				while (matcher.find()) {
					String replaceStr = matcher.group(1);
					finalResult = updatedResult.replace(replaceStr,
							"<span class='AILensUpdatedResultStr'>" + replaceStr + "</span>");
				}
				if (!(finalResult != null && !"".equalsIgnoreCase(finalResult)
						&& !"null".equalsIgnoreCase(finalResult))) {
					finalResult = updatedResult;
				}

				rersultObj.put("reply", finalResult);
				// rersultObj.put("reply", "Data visualization is the representation of complex
				// data in a visual format, such as charts, graphs, and maps, to help users
				// understand and make sense of the data more easily. It allows for patterns,
				// trends, and relationships within the data to be easily identified and
				// analyzed. Data visualization can be used to communicate information
				// effectively, make data-driven decisions, and tell a compelling story with
				// data.");
			} else {
				rersultObj.put("reply", result);
				// rersultObj.put("reply", "Data visualization is the representation of complex
				// data in a visual format, such as charts, graphs, and maps, to help users
				// understand and make sense of the data more easily. It allows for patterns,
				// trends, and relationships within the data to be easily identified and
				// analyzed. Data visualization can be used to communicate information
				// effectively, make data-driven decisions, and tell a compelling story with
				// data.");

			}
//			rersultObj.put("QUESTION_OBJ",rersultObj);

		} catch (Exception e) {
			e.printStackTrace();
		}

		return rersultObj.toJSONString();
	}

	@RequestMapping(value = "/loadIntialButtonsData", method = { RequestMethod.POST,
			RequestMethod.GET }, produces = "text/plain;charset=UTF-8")
	public @ResponseBody String loadIntialButtonsData(HttpServletRequest request) {
		JSONArray rersultObjArr = new JSONArray();
		try {
			rersultObjArr = dashBoardsService.loadIntialButtonsData(request);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return rersultObjArr.toJSONString();
	}

	@RequestMapping(value = "/showAILensNotificationsData", method = { RequestMethod.POST,
			RequestMethod.GET }, produces = "text/plain;charset=UTF-8")
	public @ResponseBody String getAILensNotifications(HttpServletRequest request) {
		JSONObject rersultObjArr = new JSONObject();
		try {
			rersultObjArr = dashBoardsService.getAILensNotifications(request);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return rersultObjArr.toString();
	}

//	@RequestMapping(value = "/fetchQuestionFromDb", method = {RequestMethod.POST, RequestMethod.GET}, produces = "text/plain;charset=UTF-8")
//	public @ResponseBody
//	String fetchQuestionFromDb(HttpServletRequest request) {
//		JSONObject rersultObjArr = new JSONObject();
//		try {
//			rersultObjArr = dashBoardsService.fetchQuestionFromDb(request);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//
//		return rersultObjArr.toString();
//	}
	@RequestMapping(value = "/fetchQuestionsFromDb", method = { RequestMethod.POST,
			RequestMethod.GET }, produces = "text/plain;charset=UTF-8")
	public @ResponseBody String fetchQuestionsFromDb(HttpServletRequest request) {
		JSONObject rersultObjArr = new JSONObject();
		try {
			rersultObjArr = dashBoardsService.fetchQuestionsFromDb(request);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return rersultObjArr.toString();
	}

	@RequestMapping(value = "/showAITypedValueAnalyticsResults", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody String showAITypedValueAnalyticsResults(HttpServletRequest request,
			HttpServletResponse response) {
		JSONObject TablesDataobj = new JSONObject();
		try {
			TablesDataobj = dashBoardsService.showAITypedValueAnalyticsResults(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return TablesDataobj.toJSONString();

	}

	@RequestMapping(value = "/showAILensInsightsResults", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody String showAILensInsightsResults(HttpServletRequest request, HttpServletResponse response) {
		JSONObject TablesDataobj = new JSONObject();
		try {
			TablesDataobj = dashBoardsService.showAILensInsightsResults(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return TablesDataobj.toJSONString();

	}

	@RequestMapping(value = "/showAILensChartsSuggestions", method = { RequestMethod.GET,
			RequestMethod.POST }, produces = { "application/json" })
	public @ResponseBody JSONObject showAILensChartsSuggestions(HttpServletRequest request) {
		JSONObject resultObject = null;
		try {
			resultObject = dashBoardsService.showAILensChartsSuggestions(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObject;
	}

	@RequestMapping(value = "/showDataLineageResults", method = { RequestMethod.GET, RequestMethod.POST }, produces = {
			"application/json" })
	public @ResponseBody JSONObject showDataLineageResults(HttpServletRequest request) {
		JSONObject resultObject = null;
		try {
			resultObject = dashBoardsService.showDataLineageResults(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObject;
	}

	@RequestMapping(value = "/getAILensInsightsAnalyticsQuestionsData", method = { RequestMethod.GET,
			RequestMethod.POST })
	public @ResponseBody JSONObject getAILensInsightsAnalyticsQuestionsData(HttpServletRequest request) {
		JSONObject resultObject = null;
		try {
			resultObject = dashBoardsService.getAILensInsightsAnalyticsQuestionsData(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObject;
	}

	@RequestMapping(value = "/getAILensAnalyticsUserExistTableNamesData", method = { RequestMethod.GET,
			RequestMethod.POST })
	public @ResponseBody JSONObject getAILensAnalyticsUserExistTableNamesData(HttpServletRequest request) {
		JSONObject resultObject = null;
		try {
			resultObject = dashBoardsService.getAILensAnalyticsUserExistTableNamesData(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObject;
	}

	@RequestMapping(value = "/getAILensFirstHeaders", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject getAILensFirstHeaders(HttpServletRequest request) {
		JSONObject resultObject = null;
		try {
			resultObject = dashBoardsService.getAILensFirstHeaders(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObject;
	}

	@RequestMapping(value = { "/getAILensAnalyticsInsightsAudio" })
	public void getAILensAnalyticsInsightsAudio(HttpServletRequest request, HttpServletResponse response) {
		dashBoardsService.getAILensAnalyticsInsightsAudio(request, response);
	}

	@RequestMapping(value = { "/saveSentDashBoardMailLastRun" })
	public JSONObject saveSentDashBoardMailLastRun(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsService.saveSentDashBoardMailLastRun(request);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return resultObj;
	}
	@RequestMapping(value = { "/getDashBoardsForMail" })
	public JSONObject getDashBoardsForMail(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsService.getDashBoardsForMail(request);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return resultObj;
	}

	@RequestMapping(value = "/importTreeDMPythonPDFFile", method = { RequestMethod.GET,
			RequestMethod.POST }, produces = { "application/json" })
	public @ResponseBody JSONObject importDMFile(HttpServletRequest request, HttpServletResponse response,
			@RequestParam("selectedFiletype") String selectedFiletype,
			@RequestParam("importTreeDMFile") MultipartFile file) {

		System.out.println("Entered Export Controller...");
		JSONObject resultObj = new JSONObject();
		try {

			resultObj = dashBoardsService.importTreeDMPythonPDFFile(request, response, file, selectedFiletype);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	@RequestMapping(value = { "/getPdfBasedonPath" })
	public @ResponseBody void getPdfBasedonPath(HttpServletRequest request,
			javax.servlet.http.HttpServletResponse response) {
		dashBoardsService.getPdfBasedonPath(request, response);
	}
	
	@RequestMapping(value = { "/downloadDashboardMailChartImageAllPDF" }, method = RequestMethod.POST)
	public void downloadDashboardMailChartImageAllPDF(HttpServletRequest request, HttpServletResponse response, ModelMap model) {
		dashBoardsService.downloadDashboardMailChartImageAllPDF(request, response);
	}
	
	@RequestMapping(value = { "/getShareDashBoardUsersList" }, method = { RequestMethod.POST, RequestMethod.GET }, produces = {
	"application/json" })
	public @ResponseBody JSONObject getShareDashBoardUsersList(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsService.getShareDashBoardUsersList(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}
	@RequestMapping(value = { "/getMailShareDashBoardUsersList" }, method = { RequestMethod.POST, RequestMethod.GET }, produces = {
	"application/json" })
	public @ResponseBody JSONObject getMailShareDashBoardUsersList(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsService.getMailShareDashBoardUsersList(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}
	@RequestMapping(value = { "/saveDashBoardUsersList" }, method = { RequestMethod.POST, RequestMethod.GET }, produces = {
	"application/json" })
	public @ResponseBody JSONObject saveDashBoardUsersList(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsService.saveDashBoardUsersList(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}
	
	@RequestMapping(value = {"/getDataBasedOnTemplateId"})
    public @ResponseBody void getDataBasedOnTemplateId(HttpServletRequest request, javax.servlet.http.HttpServletResponse response) {
		dashBoardsService.getDataBasedOnTemplateId(request, response);
    }


}
