package com.pilog.mdm.DAO;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.URLEncoder;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.rowset.serial.SerialBlob;
import javax.transaction.Transactional;

import com.pilog.mdm.Utils.DashBoardUtills;
import com.pilog.mdm.access.DataAccess;
import com.pilog.mdm.utilities.AuditIdGenerator;
import com.pilog.mdm.utilities.PilogUtilities;

//import jdk.internal.org.objectweb.asm.tree.TryCatchBlockNode;
import jxl.biff.EmptyCell;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.util.TablesNamesFinder;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

/**
 *
 * @author Jagadish.R
 */

@Repository
public class DashBoardsConversationDAO {

	@Value("${jdbc.username}")
	private String userName;
	@Value("${jdbc.password}")
	private String password;
	@Value("${jdbc.driver}")
	private String dataBaseDriver;
	@Value("${jdbc.url}")
	private String dbURL;

	@Autowired
	private DataAccess access;
	@Autowired
	private CloudGridFormationDAO cloudGridFormationDAO;
	@Autowired
	private DashBoardUtills dashBoardUtills;
	private PilogUtilities cloudUtills = new PilogUtilities();
	private String etlFilePath;
	{
		System.out.print("path :::" + System.getProperty("os.name"));
		if ((System.getProperty("os.name") != null
				&& !(System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")))) {
			etlFilePath = "/u01/";
		} else {
			etlFilePath = "C://";
		}
	}

	@Transactional
	public List getConversationalAIData(HttpServletRequest request) {
		List listData = new ArrayList();
		String messageId = request.getParameter("messageId");
		try {
			String selectQuery = "SELECT " + "MESSAGE_ID, "// 0
					+ "RIGHT_MESSAGE, "// 1
					+ "LEFT_MESSAGE, "// 2
					+ "RIGHT_BUTTON, "// 3
					+ "LEFT_BUTTON, "// 4
					+ "RIGHT_BUTTON_METHOD, "// 5
					+ "LEFT_BUTTON_METHOD, "// 6
					+ "RIGHT_NEXT_METHOD, "// 7
					+ "LEFT_NEXT_METHOD, "// 8
					+ "REPLIED_ID, "// 9
					+ "CUSTOM_COL1, "// 10
					+ "CUSTOM_COL2, "// 11
					+ "CUSTOM_COL3, "// 12
					+ "CUSTOM_COL4, "// 13
					+ "CUSTOM_COL5, "// 14
					+ "CUSTOM_COL6, "// 15
					+ "CUSTOM_COL7, "// 16
					+ "CUSTOM_COL8, "// 17
					+ "CUSTOM_COL9, "// 18
					+ "CUSTOM_COL10 "// 19
					+ "FROM CREATE_CHART_CONVERSATIONS WHERE MESSAGE_ID =:MESSAGE_ID";
			Map mapData = new HashMap();
			mapData.put("MESSAGE_ID", messageId);
			listData = access.sqlqueryWithParams(selectQuery, mapData);

		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return listData;

	}

	@Transactional
	public JSONObject getConversationalAIMessage(HttpServletRequest request) {
		JSONObject jsonData = new JSONObject();
		try {
			String mainDiv = "<div class='visionChartsAutoSuggestionsClass'>";
			String rightMainDIv = "";
			String leftMainDIv = "";
			List listData = getConversationalAIData(request);
			if (listData != null && !listData.isEmpty()) {
				mainDiv += "<div class='convai-message'>";
				rightMainDIv += "<div class='convai_right_main_message'>";
				leftMainDIv += "<div class='convai_left_main_message'>";
				for (int i = 0; i < listData.size(); i++) {
					Object[] objData = (Object[]) listData.get(i);
					if (objData != null) {
						int messageId = ((BigDecimal) objData[0]).intValue();
						String rightMsg = (String) objData[1];
						String leftMsg = (String) objData[2];
						String rightBtn = (String) objData[3];
						String leftBtn = (String) objData[4];
						String rightBtnMtd = (String) objData[5];
						String leftBtnMtd = (String) objData[6];
						String rightNxtMtd = (String) objData[7];
						String leftNxtMtd = (String) objData[8];
						int repliedId = 0;
						if (objData[9] != null) {
							repliedId = ((BigDecimal) objData[9]).intValue();
						}
						if (rightMsg != null && !"".equalsIgnoreCase(rightMsg)) {
							rightMainDIv += "<div class='visionConversationalAIClass convai-right-message nonLoadedBubble'>"
									+ rightMsg + "</div>";
						}
						if (leftMsg != null && !"".equalsIgnoreCase(leftMsg)) {
							leftMainDIv += "<div class='visionConversationalAIClass convai-left-message nonLoadedBubble'>"
									+ leftMsg + "</div>";
						}
						if (rightBtn != null && !"".equalsIgnoreCase(rightBtn)) {
							rightMainDIv += "<button class='visionConversationalAIClass convai-left-message-button nonLoadedBubble' onclick=\""
									+ rightBtnMtd + "\">" + rightBtn + "</button>";
						}
						if (leftBtn != null && !"".equalsIgnoreCase(leftBtn)) {
							leftMainDIv += "<button class='visionConversationalAIClass convai-left-message-button nonLoadedBubble' onclick=\""
									+ leftBtnMtd + "\">" + leftBtn + "</button>";
						}
						jsonData.put("rightNxtMtd", rightNxtMtd);
						jsonData.put("leftNxtMtd", leftNxtMtd);
						jsonData.put("replyId", repliedId);
						if (i == listData.size() - 1) {
							rightMainDIv += "</div>";
							leftMainDIv += "</div>";
						}
					}
				}
				mainDiv += leftMainDIv + rightMainDIv;
				mainDiv += "</div>";
			}
			mainDiv += "</div>";
			jsonData.put("mainDiv", mainDiv);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return jsonData;
	}

	@Transactional
	public JSONObject getUserTableNames(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String tableDiv = "";
			String userName = (String) request.getParameter("userName");
			String replyId = (String) request.getParameter("replyId");
			if (userName != null && !"".equalsIgnoreCase(userName)) {
				String fetchQuery = "SELECT TABLE_NAME  FROM C_ETL_DAL_AUTHORIZATION WHERE CREATE_BY =:CREATE_BY";
				Map mapData = new HashMap();
				mapData.put("CREATE_BY", userName);
				List listData = access.sqlqueryWithParams(fetchQuery, mapData);
				if (listData != null && !listData.isEmpty()) {
					tableDiv = "<div id='userTableNamesDivId' class='userTableNamesDivClass text-right replyIntelisenseView noBubble'>"
							// + "<p class='nonLoadedBubble'>Existing Files/Tables</p>"
							+ "<div class=\"search nonLoadedBubble\">"
							+ "<input type=\"text\" placeholder=\"search\" id='data-search'/>" + "</div>"
							+ "<div id='userIntellisenseViewTableNamesDivId' class='userIntellisenseViewTableNamesDivClass nonLoadedBubble'>";
					for (int i = 0; i < listData.size(); i++) {
						String tableName = (String) listData.get(i);
						tableDiv += "<div id='" + tableName
								+ "_table' class='userTableNameClass' onclick=getConversationalAISelectedDataTableName('"
								+ tableName + "','" + replyId
								+ "') data-intelliSenseViewTablefilter-item data-filter-name=\"" + tableName + "\">"
								+ tableName + "</div>";
					}
					tableDiv += "</div>" + "</div>";
				}
				resultObj.put("tableDiv", tableDiv);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	@Transactional
	public JSONObject getAILensInsightsUserExistTableNamesData(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String tableDiv = "";
			String userName = (String) request.getParameter("userName");
			if (userName != null && !"".equalsIgnoreCase(userName)) {
				String fetchQuery = "SELECT TABLE_NAME  FROM C_ETL_DAL_AUTHORIZATION WHERE CREATE_BY =:CREATE_BY";
				Map mapData = new HashMap();
				mapData.put("CREATE_BY", userName);
				List listData = access.sqlqueryWithParams(fetchQuery, mapData);
				if (listData != null && !listData.isEmpty()) {
					tableDiv = "<div id='aIlensInsightsExistUserTableNamesDivId' class='aIlensInsightsExistUserTableNamesDivClass text-right replyIntelisenseView noBubble'>"
							// + "<p class='nonLoadedBubble'>Existing Files/Tables</p>"
							+ "<div class=\"search nonLoadedBubble\">"
							+ "<input type=\"text\" placeholder=\"search\" id='data-insightsExistTablessearch'/>"
							+ "</div>"
							+ "<div id='userAIlensInsightsExistTableNamesDivId' class='userAIlensInsightsExistTableNamesDivClass nonLoadedBubble'>";
					for (int i = 0; i < listData.size(); i++) {
						String tableName = (String) listData.get(i);
						tableDiv += "<div id='" + tableName
								+ "_table' class='userTableNameClass' onclick=getAILensInsightsSelectedDataTableName('"
								+ tableName
								+ "') data-AIlensInsightsExistTablefilter-item data-AILensInsightExistfilter-name=\""
								+ tableName + "\">" + tableName + "</div>";
					}
					tableDiv += "</div>" + "</div>";
				}
				resultObj.put("tableDiv", tableDiv);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	@Transactional
	public JSONObject getUserMergeTableNames(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			JSONArray tablesArr = new JSONArray();
			String tableDiv = "";
			String userName = (String) request.getParameter("userName");
			String replyId = (String) request.getParameter("replyId");
			if (userName != null && !"".equalsIgnoreCase(userName)) {
				String fetchQuery = "SELECT TABLE_NAME  FROM C_ETL_DAL_AUTHORIZATION WHERE CREATE_BY =:CREATE_BY";
				Map mapData = new HashMap();
				mapData.put("CREATE_BY", userName);
				List listData = access.sqlqueryWithParams(fetchQuery, mapData);
				if (listData != null && !listData.isEmpty()) {
					tableDiv = "<div id='userMergeTableNamesDivId' class='userTableNamesDivClass text-right replyIntelisenseView noBubble'>"
							+ "<div id='userIntellisenseViewMergeTableNamesDivId' class='userIntellisenseViewTableNamesDivClass nonLoadedBubble'>";
					for (int i = 0; i < listData.size(); i++) {
						String tableName = (String) listData.get(i);
						tablesArr.add(tableName);
					}
					tableDiv += "</div>"
							+ "<div id='userIntellisenseViewMergeTableNamesErrorDivId' class='userIntellisenseViewMergeTableNamesErrorDivClass'></div>"
							+ "<button id='userConservationalMergeTableNamesButtonId' value='Confirm' onclick='showConversationalMergeTableNames("
							+ replyId + ")'>Ok</button>" + "</div>";
				}
				resultObj.put("tableDiv", tableDiv);
				resultObj.put("tablesArr", tablesArr);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	@Transactional
	public JSONObject getUserMergeTableNamesColumns(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			JSONArray tablesArr = new JSONArray();
			String tableDiv = "";
			Map tablesObj = new LinkedHashMap();
			String tableNames = (String) request.getParameter("tableNames");
			String replyId = (String) request.getParameter("replyId");
			if (!"1223".equalsIgnoreCase(replyId)) {
				if (tableNames != null && !"".equalsIgnoreCase(tableNames) && !"".equalsIgnoreCase(tableNames)) {
					tablesArr = (JSONArray) JSONValue.parse(tableNames);
					if (tablesArr != null && !tablesArr.isEmpty()) {
						tableDiv = "<div id='userMergeTableColumnsRemoveDeleteId' class='userMergeTableColumnsRemoveDeleteClass'>"
								+ "<img src='images/delete_icon_hover.png' class='visionConversationalAiIcon' onclick=\"deleteFlowChartSelectedOperators('#userMergeTableColumnsDivIdDXP')\"/>"
								+ "</div>"

								+ "<div id='userMergeTableColumnsDivId' class='userTableColumnsDivClass text-right replyIntelisenseView noBubble'>";
						for (int i = 0; i < tablesArr.size(); i++) {
							String tableName = (String) tablesArr.get(i);
							String fetchQuery = "SELECT COLUMN_NAME  FROM USER_TAB_COLUMNS WHERE TABLE_NAME=:TABLE_NAME";
							Map mapData = new HashMap();
							mapData.put("TABLE_NAME", tableName);
							List listData = access.sqlqueryWithParams(fetchQuery, mapData);
							JSONObject mainInputObj = new JSONObject();
							JSONObject mainOutputObj = new JSONObject();
							if (listData != null && !listData.isEmpty()) {
								for (int j = 0; j < listData.size(); j++) {
									String columnName = (String) listData.get(j);
									JSONObject objData = new JSONObject();
									objData.put("label", columnName);
									// objData.put("multiple", true);
									if (i == 0) {
										mainOutputObj.put("output_" + j, objData);
									} else if (i == (tablesArr.size() - 1)) {
										mainInputObj.put("input_" + j, objData);
									} else {
										mainInputObj.put("input_" + j, objData);
										mainOutputObj.put("output_" + j, objData);
									}

								}
							}
							JSONObject putsObjData = new JSONObject();
							putsObjData.put("inputs", mainInputObj);
							putsObjData.put("outputs", mainOutputObj);
							tablesObj.put(tableName, putsObjData);
						}
						tableDiv += "</div>" + "<div class='userMergeTablesJoinErrorClass'>"
								+ "<input type='hidden' id='linkDynamicId' value='0'/>"
								+ "<div id='visionConvAIDefaultMapLinkColumnsId' class='visionConvAIDefaultMapLinkColumnsClass'></div>"
								+ "<button onclick='getMergeJoinCondColumns(" + replyId
								+ ")' class='userMergeTablesJoinErrorButtonClass'>Next</button>" + "</div>";
						resultObj.put("tableDiv", tableDiv);
						resultObj.put("tablesObj", tablesObj);
					}
				}
			} else {
				if (tableNames != null && !"".equalsIgnoreCase(tableNames) && !"".equalsIgnoreCase(tableNames)) {
					tablesArr = (JSONArray) JSONValue.parse(tableNames);
					if (tablesArr != null && !tablesArr.isEmpty()) {
						tableDiv = "<div id='userMergeTableColumnsRemoveDeleteIdDXP' class='userMergeTableColumnsRemoveDeleteClassDXP'>"
								+ "<img src='images/delete_icon_hover.png' class='visionConversationalAiIconDXP' onclick=\"deleteFlowChartSelectedOperators('#userMergeTableColumnsDivIdDXP')\"/>"
								+ "</div>"
								+ "<div id='userMergeTableColumnsDivIdDXP' class='userTableColumnsDivClassDXP'>";
						for (int i = 0; i < tablesArr.size(); i++) {
							String tableName = (String) tablesArr.get(i);
							String fetchQuery = "SELECT COLUMN_NAME  FROM USER_TAB_COLUMNS WHERE TABLE_NAME=:TABLE_NAME";
							Map mapData = new HashMap();
							mapData.put("TABLE_NAME", tableName);
							List listData = access.sqlqueryWithParams(fetchQuery, mapData);
							JSONObject mainInputObj = new JSONObject();
							JSONObject mainOutputObj = new JSONObject();
							if (listData != null && !listData.isEmpty()) {
								for (int j = 0; j < listData.size(); j++) {
									String columnName = (String) listData.get(j);
									JSONObject objData = new JSONObject();
									objData.put("label", columnName);
									// objData.put("multiple", true);
									if (i == 0) {
										mainOutputObj.put("output_" + j, objData);
									} else if (i == (tablesArr.size() - 1)) {
										mainInputObj.put("input_" + j, objData);
									} else {
										mainInputObj.put("input_" + j, objData);
										mainOutputObj.put("output_" + j, objData);
									}

								}
							}

							JSONObject putsObjData = new JSONObject();
							putsObjData.put("inputs", mainInputObj);
							putsObjData.put("outputs", mainOutputObj);
							tablesObj.put(tableName, putsObjData);
						}

						tableDiv += "</div><input type='hidden' id='linkDynamicId' value='0'/>";
//						tableDiv +="<div id='nextProcessAfterColMapping' class='nextProcessAfterColMappingClass'>"
//								+ "<button onclick='getMergeJoinCondColumns()'>Next</button></div>"
//								+ "</div>";
						resultObj.put("tableDiv", tableDiv);
						resultObj.put("tablesObj", tablesObj);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getVoiceSuggestedChartsBasedonColumns(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			int colSize = 0;
			String colLength = request.getParameter("colLength");
			String colListStr = request.getParameter("columnsList");
			String axisColName = request.getParameter("axisColName");
			String title = request.getParameter("title");
			String dataTypeCountStr = request.getParameter("dataTypeCountObj");
			if (colLength != null && !"".equalsIgnoreCase(colLength)) {
				colSize = Integer.parseInt(colLength);
			}
			JSONObject dataTypesObj = new JSONObject();
			if (dataTypeCountStr != null && !"".equalsIgnoreCase(dataTypeCountStr)
					&& !"".equalsIgnoreCase(dataTypeCountStr)) {
				dataTypesObj = (JSONObject) JSONValue.parse(dataTypeCountStr);
			}
			long varCharCnt = 0;
			long numberCnt = 0;
			if (dataTypesObj != null && !dataTypesObj.isEmpty()) {
				varCharCnt = (long) dataTypesObj.get("VARCHAR2");
				numberCnt = (long) dataTypesObj.get("NUMBER");
			}
			if (colListStr != null && !"".equalsIgnoreCase(colListStr)) {
				colListStr = colListStr.replaceAll(" ", "#");
			}
			if (title != null && !"".equalsIgnoreCase(title)) {
				title = title.replaceAll(" ", "#");
			}
			String result = "<div id ='visionSuggestedChartTypes' class='visionSuggestedChartTypesClass'>"
					+ "<span class='visionSuggestionChartTypesSpan'>Please select the ChartType</span>"
					+ "<div id='visionSuggestionChartTypeId' class='visionSuggestionChartTypeClass row iconsRow'>";
			if (colSize == 1) {
				result += "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
						+ colListStr + "','indicator','" + axisColName + "','" + title
						+ "')  src='images/Guage.svg' class='visualDarkMode' title='Guage chart'></div>";
			} else if (colSize <= 2) {
				if (varCharCnt == 1 && numberCnt == 1) {
					result += "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','pie','" + axisColName + "','" + title
							+ "') src='images/Pie.svg' class='visualDarkMode' title='Pie chart'></div>"

							+ "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','bar','" + axisColName + "','" + title
							+ "')  src='images/Bar.svg' class='visualDarkMode' title='Bar chart'></div>"

							+ "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','donut','" + axisColName + "','" + title
							+ "')  src='images/Donut.svg' class='visualDarkMode' title='Donut chart'></div>"

							+ "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','column','" + axisColName + "','" + title
							+ "')  src='images/Column.svg' class='visualDarkMode' title='Column chart'></div>"

							+ "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','lines','" + axisColName + "','" + title
							+ "')  src='images/Line.svg' class='visualDarkMode' title='Line chart'></div>"

							+ "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','scatter','" + axisColName + "','" + title
							+ "')  src='images/Scatter.svg' class='visualDarkMode' title='Scatter chart'></div>"

							+ "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','histogram','" + axisColName + "','" + title
							+ "')  src='images/Histogram.svg' class='visualDarkMode' title='Histogram chart'></div>"

							+ "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','funnel','" + axisColName + "','" + title
							+ "')  src='images/Funnel.svg' class='visualDarkMode' title='Funnel chart'></div>"

							+ "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','waterfall','" + axisColName + "','" + title
							+ "')  src='images/Waterfall.svg' class='visualDarkMode' title='Waterfall chart'></div>"

							+ "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','scatterpolar','" + axisColName + "','" + title
							+ "')  src='images/Redar-Chart.svg' class='visualDarkMode' title='Radar chart'></div>"

							/*
							 * +
							 * "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							 * + colListStr + "','barRotation','"+axisColName+"','"+
							 * title+"') src='images/Bar.svg' class='visualDarkMode' title='Bar Label Rotation chart'></div>"
							 */

							+ "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','treemap','" + axisColName + "','" + title
							+ "')  src='images/Tree_Chart.svg' class='visualDarkMode' title='Tree chart'></div>"

							+ "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','BasicAreaChart','" + axisColName + "','" + title
							+ "') src='images/BasicAreaChart.png' class='visualDarkMode' title='Basic Area chart'></div>"

							+ "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','AreaPiecesChart','" + axisColName + "','" + title
							+ "') src='images/AreaPiecesChart.png' class='visualDarkMode' title='Basic Area chart'></div>";

				}

			} else if (2 < colSize) {
				if (varCharCnt == 1 && numberCnt >= 1) {
					result += "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','bar','" + axisColName + "','" + title
							+ "')  src='images/Bar.svg' class='visualDarkMode' title='Bar chart'></div>"
							+ "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','column','" + axisColName + "','" + title
							+ "')  src='images/Column.svg' class='visualDarkMode' title='Column chart'></div>"
							+ "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','lines','" + axisColName + "','" + title
							+ "')  src='images/Line.svg' class='visualDarkMode' title='Line chart'></div>"
							+ "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','scatter','" + axisColName + "','" + title
							+ "')  src='images/Scatter.svg' class='visualDarkMode' title='Scatter chart'></div>"
							+ "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','histogram','" + axisColName + "','" + title
							+ "')  src='images/Histogram.svg' class='visualDarkMode' title='Histogram chart'></div>"
							+ "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','funnel','" + axisColName + "','" + title
							+ "')  src='images/Funnel.svg' class='visualDarkMode' title='Funnel chart'></div>"
							+ "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','candlestick','" + axisColName + "','" + title
							+ "')  src='images/Candlestick.svg' class='visualDarkMode' title='Candlestick chart'></div>"
							+ "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','waterfall','" + axisColName + "','" + title
							+ "')  src='images/Waterfall.svg' class='visualDarkMode' title='Waterfall chart'></div>"
							+ "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','scatterpolar','" + axisColName + "','" + title
							+ "')  src='images/Redar-Chart.svg' class='visualDarkMode' title='Radar chart'></div>"

//							+ "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= getSuggestedChartBasedonCols('"
//							+ colListStr + "','barRotation','" + tableName + "','" + joinQueryFlag + "','" + script
//							+ "','" + prependFlag + "') src='images/Bar.svg' class='visualDarkMode' title='Bar Label Rotation chart'></div>"

							+ "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','StackedAreaChart','" + axisColName + "','" + title
							+ "') src='images/StackedAreaChart.png' class='visualDarkMode' title='Stacked Area Chart'></div>"

							+ "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','GradStackAreaChart','" + axisColName + "','" + title
							+ "') src='images/GradientStackedAreaChart.png' class='visualDarkMode' title='Gradient Stacked Area chart'></div>";

				}
				if (varCharCnt >= 1 && numberCnt == 1) {
					result += "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','treemap','" + axisColName + "','" + title
							+ "')  src='images/Tree_Chart.svg' class='visualDarkMode' title='Tree chart'></div>"

							+ "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','sunburst','" + axisColName + "','" + title
							+ "')  src='images/Sunburst_Inner_Icon.svg' class='visualDarkMode' title='SunBurst'></div>"

							+ "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','sankey','" + axisColName + "','" + title
							+ "')  src='images/sankey_chart.png' class='visualDarkMode' title='Sankey'></div>";
				}
				if (varCharCnt == 2 && numberCnt == 1) {
					result += "<div class=\"col-lg-4 my-2 px-1 col-md-4 visualChartsByQueryClass\"><img onclick= viewChartBasedOnType('"
							+ colListStr + "','heatMap','" + axisColName + "','" + title
							+ "')  src='images/HeatMap_Inner_Icon.svg' class='visualDarkMode' title='Heat Map'></div>";

				}
			}

			result += "</div>" + "</div>";
			resultObj.put("result", result);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;

	}

	public JSONObject getInsightsView(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String tableName = (String) request.getParameter("tableName");
			String flag = (String) request.getParameter("flag");
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
			MultiValueMap<String, String> inputMap = new LinkedMultiValueMap<>();
			JSONObject dbDetails = new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName, password,
					"DH101102");
			inputMap.add("table_name", tableName);
			inputMap.add("response_id", "26CC0708E0892BAA29");
			inputMap.add("USER_NAME", userName);
			inputMap.add("PASSWORD", password);
			inputMap.add("HOST", (String) dbDetails.get("HOST_NAME"));
			inputMap.add("PORT", (String) dbDetails.get("CONN_PORT"));
			inputMap.add("SERVICE_NAME", (String) dbDetails.get("CONN_DB_NAME"));
			inputMap.add("flag", flag);
			HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<MultiValueMap<String, String>>(inputMap,
					headers);
			RestTemplate template = new RestTemplate();
			ResponseEntity<Map> response = template.postForEntity("http://apihub.pilogcloud.com:6654/data_insights/",
					entity, Map.class);
			Map apiDataObj = response.getBody();
			if (apiDataObj != null && !apiDataObj.isEmpty()) {
				apiDataObj = new LinkedHashMap(apiDataObj);
				List insightsList = (List) apiDataObj.entrySet().stream().map(e -> ((Map.Entry) e).getKey())
						.collect(Collectors.toList());
				if (insightsList != null && !insightsList.isEmpty()) {
					if (insightsList.contains("error")) {
						resultObj.put("error", apiDataObj.get("error"));
						return resultObj;
					}
				}
				resultObj.put("insightList", insightsList);
				String insightsMainDiv = "<div class=\"accordion InsightsAccordionClassId\" id=\"accordionInsightsId\">";
				for (int i = 0; i < insightsList.size(); i++) {
					String insightType = (String) insightsList.get(i);
					if (insightType != null && !"".equalsIgnoreCase(insightType)
							&& "Compare".equalsIgnoreCase(insightType)) {
						insightsMainDiv += "<h3 onclick=showInsightsCompareData('" + insightType + "_divId_" + i + "','"
								+ insightType + "','" + tableName + "')>" + insightType + "</h3>";
						insightsMainDiv += "<div id='" + insightType + "_divId_" + i + "' class='insightsCompareClass'>"
								+ "<div id ='" + insightType + "_divId_" + i
								+ "_ddw' class='insightsCompareDropDownClass'></div>" + "<div id ='" + insightType
								+ "_divId_" + i + "_ddw_data' class='insightsCompareDropDownDataClass'></div>"
								+ "</div>";
						resultObj.put(insightType, getCompareInsightsKeysData(insightType,
								(Map) apiDataObj.get(insightType), insightType, tableName));

					} else {
						insightsMainDiv += "<h3 onclick=showInsightsSummaryData('summary_divId_" + i + "','"
								+ insightType + "','" + tableName + "')>" + insightType + "</h3>";
						insightsMainDiv += "<div id='summary_divId_" + i + "' class='insightsCompareClass'>"
								+ "<div id ='" + insightType + "_divId_" + i
								+ "_data' class='insightsCompareDropDownClass'></div>" + "</div>";
					}
				}
				insightsMainDiv += "</div>";

				resultObj.put("insightsMainDiv", insightsMainDiv);

			}
			resultObj.put("apiDataObj", apiDataObj);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	@Transactional
	public JSONObject executeInsightsSQLQuery(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		Connection connection = null;
		try {
			String tableHeader = "";
			String query = request.getParameter("script");
			Class.forName(dataBaseDriver);
			connection = DriverManager.getConnection(dbURL, userName, password);
			Statement statement = connection.createStatement();
			ResultSet results = statement.executeQuery(query);
			ResultSetMetaData metadata = results.getMetaData();
			boolean hasTemp = false;
			int columnCount = metadata.getColumnCount();
			if (columnCount > 0) {
				tableHeader = "<thead>";
				for (int i = 1; i <= columnCount; i++) {
					String columnName = metadata.getColumnName(i);
					if (!"temp".equalsIgnoreCase(columnName)) {
						tableHeader += "<th>" + columnName + "</th>";
					} else {
						hasTemp = true;
					}
				}
				tableHeader += "</thead>";
			}

			String tableStr = "";
			if (query != null && !"".equalsIgnoreCase(query) && !"null".equalsIgnoreCase(query)) {
				List listData = access.sqlqueryWithParams(query, new HashMap());
				if (listData != null && !listData.isEmpty()) {
					tableStr = "<table id='visionInsightsChartDataTableId'>";
					tableStr += tableHeader;
					tableStr += "<tbody>";
					for (int i = 0; i < listData.size(); i++) {

						if (listData.get(i) instanceof BigDecimal) {
							tableStr += "<tr>";
							tableStr += "<td>" + listData.get(i) + "</td>";
							tableStr += "</tr>";
						} else {

							Object[] objData = (Object[]) listData.get(i);
							if (objData != null) {
								tableStr += "<tr>";
								int objDataLen = objData.length;
								if (hasTemp)
									objDataLen--;
								for (int j = 0; j < objDataLen; j++) {
									tableStr += "<td>" + objData[j] + "</td>";
								}
								tableStr += "</tr>";
							}
						}
					}
					tableStr += "</tbody>";
					tableStr += "</table>";
				}
			}
			resultObj.put("tableStr", tableStr);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return resultObj;
	}

	@org.springframework.transaction.annotation.Transactional(propagation = Propagation.REQUIRES_NEW)
	public Object getApplProperties(String keyName) {
		Object resultObject = null;
		try {
			String query = " from BApplProperties WHERE id.keyname =:keyname ";
			Map<String, Object> applPropMap = new HashMap<>();
			applPropMap.put("keyname", keyName);
			List applPropList = access.queryWithParams(query, applPropMap);
			if (applPropList != null && !applPropList.isEmpty()) {
				resultObject = applPropList.get(0);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObject;
	}

	public JSONObject getAICHatNotification(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	@Transactional
	public JSONArray loadIntialButtonsData(HttpServletRequest request) {
		JSONArray resultArr = new JSONArray();
		try {
			String uxIconsType = request.getParameter("uxIconsType");
			String query = "SELECT UX_ICON, UX_NAVIGATION_PATH,UX_DESCRIPTION FROM LENS_UX WHERE "
					// + "ORGN_ID =:ORGN_ID AND "
					+ "UX_TYPE=:UX_TYPE " + "AND UX_ACTIVE_FLAG=:UX_ACTIVE_FLAG";
			Map map = new HashMap<>();
			// map.put("ORGN_ID", (String)
			// request.getSession(false).getAttribute("ssOrgId"));
			map.put("UX_TYPE", uxIconsType);
			map.put("UX_ACTIVE_FLAG", "Y");// SQL Injection change
			List countList = access.sqlqueryWithParams(query, map);
			if (countList != null && !countList.isEmpty()) {
				if (uxIconsType != null && !"".equalsIgnoreCase(uxIconsType) && !"null".equalsIgnoreCase(uxIconsType)
						&& "LENS_HEADER_ICONS".equalsIgnoreCase(uxIconsType)) {
					String resultStr = "<ul>";
					for (int i = 0; i < countList.size(); i++) {
						Object[] resultObjArr = (Object[]) countList.get(i);
						resultStr += "<li><img src='" + resultObjArr[1] + "' " + resultObjArr[0] + "></li>";
					}
					resultStr += "<li><a href=\"javascript:void(0)\" class=\"closebtn\" onclick=\"closeAINavigation()\">×</a></li>";
					resultStr += "</ul>";
					resultArr.add(resultStr);
				} else {
					for (int i = 0; i < countList.size(); i++) {
						JSONObject resultObj = new JSONObject();
						Object[] resultObjArr = (Object[]) countList.get(i);
						resultObj.put("text", resultObjArr[0]);
						resultObj.put("img", resultObjArr[1]);
						resultObj.put("reply", resultObjArr[2]);
						resultArr.add(resultObj);
					}
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultArr;
	}

	public JSONObject getAILensNotifications(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			Object chartCount = BigDecimal.ZERO, dashBoardCount = BigDecimal.ZERO;
			String fetchQuery = "SELECT COUNT(DISTINCT CHART_ID) ,COUNT(DISTINCT DASHBORD_NAME) FROM O_RECORD_VISUALIZATION WHERE CREATE_DATE >= SYSDATE - INTERVAL '24' HOUR AND ORGN_ID =:ORGN_ID AND DASHBORD_NAME NOT IN (SELECT DASHBORD_NAME FROM O_RECORD_VISUALIZATION WHERE CREATE_DATE < SYSDATE - INTERVAL '24' HOUR )";
			Map map = new HashMap<>();
			map.put("ORGN_ID", (String) request.getSession(false).getAttribute("ssOrgId"));
			List<Object[]> countList = access.sqlqueryWithParams(fetchQuery, map);
			if (countList != null && !countList.isEmpty()) {
				Object[] firstRow = countList.get(0);
				chartCount = firstRow[0];
				dashBoardCount = firstRow[1];
			}
			resultObj.put("CHART", String.format("LAST 24 HOURS %s NEW CHARTS HAVE BEEN CREATED", chartCount));
			resultObj.put("DASHBOARD",
					String.format("LAST 24 HOURS	%s NEW DASHBOARDS HAVE BEEN CREATED", dashBoardCount));

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	@Transactional
	public JSONObject fetchQuestionsFromDb(HttpServletRequest request) {

		JSONObject resultArrObj = new JSONObject();
		try {
			String domain = request.getParameter("domain");
			String questionId = request.getParameter("questionId");
			StringBuilder whereCondition = new StringBuilder(
					"WHERE DOMAIN =:DOMAIN AND ACTIVE_FLAG =:ACTIVE_FLAG AND ");
			Map map = new HashMap<>();
			if (!(questionId != null && !questionId.equalsIgnoreCase(""))) {
				// whereCondition.append("(QUESTION_TYPE =:QUESTION_TYPE OR QUESTION_TYPE = 'T')
				// ");
//				whereCondition.append("(QUESTION_TYPE =:QUESTION_TYPE OR QUESTION_TYPE = 'T' OR QUESTION_TYPE ='YN') ");
				whereCondition.append(" (PRIMARY_QUESTION_ID = QUESTION_ID OR QUESTION_TYPE =:QUESTION_TYPE) ");
				map.put("QUESTION_TYPE", "P");

			} else {
				whereCondition.append("(QUESTION_TYPE IS NULL) AND PRIMARY_QUESTION_ID =:QUESTION_ID ");
				map.put("QUESTION_ID", questionId);

			}
			String fetchQuery = "SELECT " + "QUESTION_ID," + // 1
					"DOMAIN," + // 2
					"QUESTION," + // 3
					"SHOW_SEQ," + // 4
					"QUESTION_TYPE," + // 5
					"PRIMARY_QUESTION_ID," + // 6
					"ANSWER," + // 7
					"ANSWER_TYPE," + // 8
					"VIDEO_URL," + // 9
					"API_URL," + // 10
					"API_TYPE," + // 11
					"AI_API_FLAG," + // 12
					"AI_API_URL, " + // 13
					"QUES_ONCLICK, " + // 14
					"QUES_ICON, " + // 15
					"BUTTON_POSITION, " + // 16
					"INTRO_STEPS " + // 17
					" FROM DAL_AI_LENS_QUESTIONS " + whereCondition + " ORDER BY SHOW_SEQ ";

			// map.put("ORGN_ID", (String)
			// request.getSession(false).getAttribute("ssOrgId"));
			map.put("DOMAIN", domain);
			map.put("ACTIVE_FLAG", 'Y');

			List<Object[]> countList = access.sqlqueryWithParams(fetchQuery, map);
			if (countList != null && !countList.isEmpty()) {
				for (int i = 0; i < countList.size(); i++) {
					JSONObject resultObj = new JSONObject();
					Object[] firstRow = countList.get(i);
					resultObj.put("QUESTION_ID", firstRow[0]);
					resultObj.put("DOMAIN", firstRow[1]);
					resultObj.put("QUESTION", firstRow[2]);
					resultObj.put("SHOW_SEQ", firstRow[3]);
					resultObj.put("QUESTION_TYPE", firstRow[4]);
					resultObj.put("PRIMARY_QUESTION_ID", firstRow[5]);
					resultObj.put("ANSWER", firstRow[6]);
					resultObj.put("ANSWER_TYPE", firstRow[7]);
					resultObj.put("VIDEO_URL", firstRow[8]);
					resultObj.put("API_URL", firstRow[9]);
					resultObj.put("API_TYPE", firstRow[10]);
					resultObj.put("AI_API_FLAG", firstRow[11]);
					resultObj.put("AI_API_URL", firstRow[12]);
					resultObj.put("QUES_ONCLICK", firstRow[13]);
					resultObj.put("QUES_ICON", firstRow[14]);
					resultObj.put("BUTTON_POSITION", firstRow[15]);
					resultObj.put("INTRO_STEPS", this.cloudUtills.clobToString((Clob) firstRow[16]));
					resultArrObj.put(i, resultObj);

				}
				resultArrObj.put("QUES_STATUS", true);

			} else {
				resultArrObj.put("QUES_STATUS", false);
			}

		} catch (Exception e) {
			resultArrObj.put("QUES_STATUS", false);
			e.printStackTrace();
		}
		return resultArrObj;
	}

	public JSONObject getResultFromPythonApi(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String msg = (String) request.getParameter("aiTypedValue");
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
			MultiValueMap<String, String> inputMap = new LinkedMultiValueMap<>();
			inputMap.add("input_str", msg);
			inputMap.add("name", (String) request.getSession(false).getAttribute("ssUsername"));
			inputMap.add("status", "");
			JSONObject dbDetails = new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName, password,
					"DH101102");
			// inputMap.add("userName", userName);
			inputMap.add("user_name", userName);
			inputMap.add("password", password);
			inputMap.add("host", (String) dbDetails.get("HOST_NAME"));
			inputMap.add("port", (String) dbDetails.get("CONN_PORT"));
			inputMap.add("access_name", (String) dbDetails.get("CONN_DB_NAME"));
			HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<MultiValueMap<String, String>>(inputMap,
					headers);
			RestTemplate template = new RestTemplate();
			ResponseEntity<JSONObject> response = template.postForEntity("http://idxp.pilogcloud.com:6661/db_search/",
					entity, JSONObject.class);
			resultObj = response.getBody();

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	@Transactional
	public JSONObject showAILensChartsSuggestions(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			JSONArray questionsArr = new JSONArray();
			JSONObject queryQuestionsObj = new JSONObject();
			String aiTypedValue = (String) request.getParameter("searchWord");
			if (aiTypedValue != null && !"".equalsIgnoreCase(aiTypedValue) && !"null".equalsIgnoreCase(aiTypedValue)) {
				String fetchQuery = "SELECT DISTINCT QUESTIONS,QUERY FROM SUGGESTIONS_TABLE WHERE QUESTIONS LIKE '%"
						+ aiTypedValue + "%'";
				Map mapData = new HashMap();
				List listData = access.sqlqueryWithParams(fetchQuery, mapData);
				if (listData != null && !listData.isEmpty()) {
					for (int i = 0; i < listData.size(); i++) {
						Object[] aiSuggestions = (Object[]) listData.get(i);
						if (aiSuggestions != null && aiSuggestions.length > 0) {
							questionsArr.add(aiSuggestions[0]);
							queryQuestionsObj.put(aiSuggestions[0], aiSuggestions[1]);
						}

					}

				}
				resultObj.put("questionsArr", questionsArr);
				resultObj.put("queryQuestionsObj", queryQuestionsObj);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public String getCompareInsightsKeysData(String keyName, Map apiData, String insightsType, String tableName) {
		String divId = "<div id='" + keyName + "_id' class='aiLensInsightsComapreClass'>";
		Set keys = apiData.keySet();
		Iterator ite = keys.iterator();
		while (ite.hasNext()) {
			String key = (String) ite.next();
			if (key != null && !"".equalsIgnoreCase(key) && !"column".equalsIgnoreCase(key)) {
				String keyId = key.replace(" ", "_");
				divId += "<div id='" + keyId + "_id' class='aiLensInsightsComapreMainHeaderClass'>";
				divId += "<h3 class='aiLensInsightsComapreHeaderClass'>" + key + "</h3>";
				Map dataObj = (Map) apiData.get(key);
				if (dataObj != null && !dataObj.isEmpty()) {
					List insightsList = (List) dataObj.entrySet().stream().map(e -> ((Map.Entry) e).getKey())
							.collect(Collectors.toList());
					if (insightsList != null && !insightsList.isEmpty()) {
						divId += "<div class='aiLensInsightsComapreDDwClass'>";
						divId += "<div id='" + keyId + "_colId' class='aiLensInsightsComapreBodyClass'>";
						divId += "<div id='" + keyId + "_colDropDownId' class='aiLensInsightsComapreBodyClass'>";
						String hiddenFieldDiv = "<div id='" + keyId
								+ "_hidden' class='aiLensInsightsComapreBodyHiddenClass'>";
						String colDivId = "<select id='" + keyId
								+ "_colDdwId' onchange=getAILensInsightsColumnNames(this,'" + keyId + "_colDdwId','"
								+ keyId + "_colDropDownBodyId','" + tableName + "')>";
						colDivId += "<option value='Select'>Select</option>";
						for (int i = 0; i < insightsList.size(); i++) {
							String selectColVal = (String) insightsList.get(i);
							colDivId += "<option value='" + selectColVal + "'>" + selectColVal + "</option>";
							String result = "";
							result = getAILensInsightsSubColumnValues(selectColVal, dataObj, keyId, result);
							hiddenFieldDiv += result;
						}
						colDivId += "</select>";
						divId += colDivId;
						divId += "</div>";
						divId += "<div id='" + keyId
								+ "_colDropDownBodyId' class='aiLensInsightsComapreBodyClass'></div>";
						divId += "</div>";
						hiddenFieldDiv += "</div>";
						divId += "<div id='" + keyId + "_subColId' class='aiLensInsightsComapreBodyClass'>"
								+ "<div id='" + keyId + "_subColDropDownId' class='aiLensInsightsComapreBodyClass'>"
								+ "<select id='" + keyId
								+ "_subColDdwId' multiple onchange=getAILensInsightsSubColumnNames(this,'" + keyId
								+ "_subColDdwId','" + keyId + "_subColDropDownBodyId','" + tableName
								+ "') style=\"height:20px\">" + "<option value='Select'>Select</option>" + "</select>"
								+ "</div>" + "<div id='" + keyId
								+ "_subColDropDownBodyId' class='aiLensInsightsComapreBodyClass'></div>" + "</div>";
						divId += "<div id='" + keyId + "_valId' class='aiLensInsightsComapreBodyClass'>" + "<div id='"
								+ keyId + "_valDropDownId' class='aiLensInsightsComapreBodyClass'>"
								/*
								 * + "<select id='"+ keyId +
								 * "_valDdwId' onchange=getAILensInsightsValColumnNames(this,'" + keyId +
								 * "_valDdwId','" + keyId + "_valDropDownBodyId','" + tableName + "')>" +
								 * "<option value='Select'>Select</option>" + "</select>"
								 */
								+ "</div>" + "<div id='" + keyId
								+ "_valDropDownBodyId' class='aiLensInsightsComapreBodyClass'></div>" + "</div>";
						divId += hiddenFieldDiv;
						divId += "</div>";
					}
				}
				divId += "</div>";
			}
		}
		divId += "</div>";

		return divId;
	}

	public String getAILensInsightsSubColumnValues(String value, Map dataObj, String id, String result) {
		try {
			if (dataObj != null && !dataObj.isEmpty()) {
				if (dataObj.get(value) instanceof Map) {
					Map subLevelDataObj = (Map) dataObj.get(value);
					if (subLevelDataObj != null && !subLevelDataObj.isEmpty()) {
						if (subLevelDataObj.containsKey("values")) {
							List insightsList = (List) subLevelDataObj.get("values");
							String query = (String) subLevelDataObj.get("query");
							id = id + "_" + value;
							result += "<input type='hidden' id='" + id + "' value='"
									+ JSONArray.toJSONString(insightsList) + "'/>";
							if (query != null && !"".equalsIgnoreCase(query) && !"null".equalsIgnoreCase(query)) {
								query = query.replaceAll("'", "&&");
								result += "<input type='hidden' id='" + id + "_query' value='" + query + "'/>";
							}
						} else {
							List insightsList = (List) subLevelDataObj.entrySet().stream()
									.map(e -> ((Map.Entry) e).getKey()).collect(Collectors.toList());
							id = id + "_" + value;
							result += "<input type='hidden' id='" + id + "' value='"
									+ JSONArray.toJSONString(insightsList) + "'/>";
							for (int i = 0; i < insightsList.size(); i++) {
								String subColVal = (String) insightsList.get(i);
								result = getAILensInsightsSubColumnValues(subColVal, subLevelDataObj, id, result);
							}
						}

					}
				}
				/*
				 * else if (dataObj.get(value) instanceof String) { String subLevelDataObj =
				 * (String) dataObj.get(value); id = id + "_" + value; subLevelDataObj =
				 * subLevelDataObj.replaceAll("'", "&&"); result += "<input type='hidden' id='"
				 * + id + "' value='" + subLevelDataObj + "'/>";
				 * 
				 * }
				 */
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return result;
	}

	@Transactional
	public JSONObject showDataLineageResults(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String resultDivStr = "";
			String tableName = (String) request.getParameter("tableName");
			if (tableName != null && !"".equalsIgnoreCase(tableName) && !"null".equalsIgnoreCase(tableName)) {
				String fetchQuery = "SELECT STATUS,TABLE_NAME, CUST_COL2,DATE_,TIME_ FROM DATA_LINEAGE WHERE TABLE_NAME = '"
						+ tableName + "' ";
				Map mapData = new HashMap();
				List listData = access.sqlqueryWithParams(fetchQuery, mapData);
				if (listData != null && !listData.isEmpty()) {
					JSONObject columnsObj = new JSONObject();
					resultDivStr = "<div class=\"container recoredHistoryTabsClassContainer\">"
							+ "<div class=\"wrapper\">" + "<div class=\"arrow-steps clearfix\">";
					for (int i = 0; i < listData.size(); i++) {
						Object[] lineageData = (Object[]) listData.get(i);
						if (lineageData != null && lineageData.length > 0) {
							String status = (String) lineageData[0];
							String tableNames = (String) lineageData[1];
							String query = (String) lineageData[2];
							String date = (String) lineageData[3];
							String time = (String) lineageData[4];
							String columns = "";
							if (query != null && !"".equalsIgnoreCase(query) && !"null".equalsIgnoreCase(query)) {
								columnsObj = decodeSqlQuery(query);
								if (columnsObj != null && !columnsObj.isEmpty()) {
									columns = (String) columnsObj.get("columns");
								}
							}
							resultDivStr += "<div class=\"step current\">"
									+ "                        <div class=\"recoredItemClassId\"><span>Type  </span>: "
									+ status + " </div>"
									+ "                        <div class=\"recoredItemClassId\"><span>Table Name  </span>: "
									+ tableNames + " </div>";
							if (columns != null && !"".equalsIgnoreCase(columns) && !"null".equalsIgnoreCase(columns)) {
								resultDivStr += "                        <div class=\"recoredItemClassId\"><span>Columns </span> :"
										+ "                            <ul class=\"recoredColumnsClassId\">";
								String columnsArr[] = columns.split(",");
								if (columnsArr != null && columnsArr.length > 0) {
									for (int s = 0; s < columnsArr.length; s++) {
										resultDivStr += "<li>" + columnsArr[s] + "</li>";
									}
								}

								resultDivStr += "                            </ul>" + "                        </div>";
							}
							resultDivStr += "                        <div class=\"recoredItemClassId\"><span>Time</span> : "
									+ date + " | " + time + "</div>" + "                    </div>";
						}

					}
					resultDivStr += "</div>" + "</div>" + "</div>";
					resultObj.put("resultDivStr", resultDivStr);
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getAILensInsightsAnalyticsQuestions(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String ssUserName = (String) request.getParameter("userName");
			String sessionId = (String) request.getParameter("sessionId");
			String tableName = (String) request.getParameter("tableName");
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
			MultiValueMap<String, String> inputMap = new LinkedMultiValueMap<>();
			JSONObject dbDetails = new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName, password,
					"DH101102");
			// inputMap.add("userName", userName);
			inputMap.add("USER_NAME", userName);
			inputMap.add("PASSWORD", password);
			inputMap.add("HOST", (String) dbDetails.get("HOST_NAME"));
			inputMap.add("PORT", (String) dbDetails.get("CONN_PORT"));
			inputMap.add("SERVICE_NAME", (String) dbDetails.get("CONN_DB_NAME"));
			inputMap.add("TABLE_NAME", tableName);
			inputMap.add("SESSION_ID", sessionId);
			HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<MultiValueMap<String, String>>(inputMap,
					headers);
			RestTemplate template = new RestTemplate();
			ResponseEntity<String> response = template.postForEntity("http://apihub.pilogcloud.com:6676/chat_with_data",
					entity, String.class);
			String apiDataStr = response.getBody();
			if (apiDataStr != null && !"".equalsIgnoreCase(apiDataStr) && !"null".equalsIgnoreCase(apiDataStr)) {
				JSONObject apiDataObj = (JSONObject) JSONValue.parse(apiDataStr);
				if (apiDataObj != null && !apiDataObj.isEmpty()) {
					List listData = (List) apiDataObj.get("QUESTIONS");
					if (listData != null && !listData.isEmpty()) {
						String questionsDivStr = "<div class='quickInsightsAnalyticsQuestionsToggleicon' onclick='quickInsightsAnalyticsQuestionsToggleicon()'><i class='fa fa-angle-double-up' aria-hidden='true'></i></div>"
								+ "<div id='data-quickInsightsAnalyticsQuestions' class='data-quickInsightsAnalyticsQuestionsClass'>";

						for (int i = 0; i < listData.size(); i++) {
							if (i == 0) {
								questionsDivStr += "<div class='quickInsightsAnalyticsQuestionsClass' id='showInsightsAnalyticsQuestionsID'>"
										+ listData.get(i) + "</div>";
							} else {
								questionsDivStr += "<div class='quickInsightsAnalyticsQuestionsClass' >"
										+ listData.get(i) + "</div>";
							}
						}
						questionsDivStr += "</div>";
						resultObj.put("questionsDivStr", questionsDivStr);
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getAILensInsightsAnalyticsInsights(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		String result = "";
		int numberOfThreads = 1;
		ExecutorService executor = Executors.newFixedThreadPool(1);
		List<Future<?>> futures = new ArrayList<>();
		try {
			String ssUserName = (String) request.getParameter("userName");
			String tableName = (String) request.getParameter("tableName");
			String responseId = AuditIdGenerator.genRandom32Hex();
			System.out.println("responseId :: " + responseId);
			executor.submit(() -> {
				try {
					System.out.println("responseId11111 :: " + responseId);
					HttpHeaders headers = new HttpHeaders();
					headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
					MultiValueMap<String, String> inputMap = new LinkedMultiValueMap<>();
					JSONObject dbDetails = new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName,
							password, "DH101102");
					inputMap.add("response_id", responseId);
					inputMap.add("USER_NAME", userName);
					inputMap.add("PASSWORD", password);
					inputMap.add("HOST", (String) dbDetails.get("HOST_NAME"));
					inputMap.add("PORT", (String) dbDetails.get("CONN_PORT"));
					inputMap.add("SERVICE_NAME", (String) dbDetails.get("CONN_DB_NAME"));
					inputMap.add("table_name", tableName);
					HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<MultiValueMap<String, String>>(
							inputMap, headers);
					SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
					requestFactory.setConnectTimeout(500000); // 5 seconds connection timeout
					requestFactory.setReadTimeout(1800000); // 30 minutes read timeout
					RestTemplate template = new RestTemplate(requestFactory);
					ResponseEntity<String> response = template
							.postForEntity("http://apihub.pilogcloud.com:6670/data_insights/", entity, String.class);
					String result1 = response.getBody();

				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					try {
						System.out.println("attempt to shutdown executor");
						executor.shutdown();
						executor.awaitTermination(5L, TimeUnit.SECONDS);
					} catch (InterruptedException e) {
						System.err.println("tasks interrupted");
					} finally {
						if (!executor.isTerminated()) {
							System.err.println("cancel non-finished tasks");
						}
						executor.shutdownNow();
						System.out.println("shutdown finished");
					}
				}
			});
			result = getApiResponse(request, responseId, executor, 0);
			if (result != null && !"".equalsIgnoreCase(result) && !"null".equalsIgnoreCase(result)) {
				List apiDataObj = Arrays.asList(result.split(";"));
				if (apiDataObj != null && !apiDataObj.isEmpty()) {
					String questionsDivStr = "<div class='quickInsightsAnalyticsDescriptionPointsClass' ><i class='fa fa-angle-double-up' aria-hidden='true'></i></div>"
							+ "<ul id='data-quickInsightsAnalyticsInsightsDescriptions' class='data-quickInsightsAnalyticsInsightsDescriptionsClass'>";

					for (int i = 0; i < apiDataObj.size(); i++) {

						questionsDivStr += "<li class='quickInsightsAnalyticsQuestionsClass' >" + apiDataObj.get(i)
								+ "</li>";
					}
					questionsDivStr += "</ul>";
					resultObj.put("questionsDivStr", questionsDivStr);
					resultObj.put("response_id", responseId);
					resultObj.put("textData", voicefileTextData(responseId));

				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				System.out.println("attempt to shutdown executor");
				executor.shutdown();
				executor.awaitTermination(5L, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				System.err.println("tasks interrupted");
			} finally {
				if (!executor.isTerminated()) {
					System.err.println("cancel non-finished tasks");
				}
				executor.shutdownNow();
				System.out.println("shutdown finished");
			}
		}
		return resultObj;
	}

	public JSONObject getAILensInsightsAnalyticsInsights1(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String ssUserName = (String) request.getParameter("userName");
			String tableName = (String) request.getParameter("tableName");
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
			MultiValueMap<String, String> inputMap = new LinkedMultiValueMap<>();
			JSONObject dbDetails = new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName, password,
					"DH101102");
			String responseId = AuditIdGenerator.genRandom32Hex();
			// String responseId = "26CC0708E0892BAA29";
			inputMap.add("response_id", responseId);
			inputMap.add("USER_NAME", userName);
			inputMap.add("PASSWORD", password);
			inputMap.add("HOST", (String) dbDetails.get("HOST_NAME"));
			inputMap.add("PORT", (String) dbDetails.get("CONN_PORT"));
			inputMap.add("SERVICE_NAME", (String) dbDetails.get("CONN_DB_NAME"));
			inputMap.add("table_name", tableName);
			HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<MultiValueMap<String, String>>(inputMap,
					headers);
			SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
			requestFactory.setConnectTimeout(500000); // 5 seconds connection timeout
			requestFactory.setReadTimeout(1800000); // 30 minutes read timeout
			RestTemplate template = new RestTemplate(requestFactory);
			// RestTemplate template = new RestTemplate();
			ResponseEntity<Object> response = template.postForEntity("http://apihub.pilogcloud.com:6670/data_insights/",
					entity, Object.class);
			String apiDataStr = (String) response.getBody();
			if (apiDataStr != null && !"".equalsIgnoreCase(apiDataStr) && !"null".equalsIgnoreCase(apiDataStr)) {
				JSONArray apiDataObj = (JSONArray) JSONValue.parse(apiDataStr);
				if (apiDataObj != null && !apiDataObj.isEmpty()) {
					String questionsDivStr = "<div class='quickInsightsAnalyticsDescriptionPointsClass' ><i class='fa fa-angle-double-up' aria-hidden='true'></i></div>"
							+ "<ul id='data-quickInsightsAnalyticsInsightsDescriptions' class='data-quickInsightsAnalyticsInsightsDescriptionsClass'>";

					for (int i = 0; i < apiDataObj.size(); i++) {

						questionsDivStr += "<li class='quickInsightsAnalyticsQuestionsClass' >" + apiDataObj.get(i)
								+ "</li>";
					}
					questionsDivStr += "</ul>";
					resultObj.put("questionsDivStr", questionsDivStr);
					resultObj.put("response_id", responseId);

				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getAILensInsightsAnalyticsQuestionsData(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		Connection connection = null;
		try {
			String sessionId = (String) request.getParameter("sessionId");
			String question = (String) request.getParameter("question");
			String voiceFlag = (String) request.getParameter("voiceFlag");
			String tableName = (String) request.getParameter("tableName");
			String dashboardName = (String) request.getParameter("dashboardName");
			String shareUserName = (String) request.getParameter("shareUserName");
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
			MultiValueMap<String, String> inputMap = new LinkedMultiValueMap<>();
			String responseId = AuditIdGenerator.genRandom32Hex();
			JSONObject dbDetails = new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName, password,
					"DH101102");
			inputMap.add("response_Id", responseId);
			inputMap.add("USER_NAME", userName);
			inputMap.add("PASSWORD", password);
			inputMap.add("HOST", (String) dbDetails.get("HOST_NAME"));
			inputMap.add("PORT", (String) dbDetails.get("CONN_PORT"));
			inputMap.add("SERVICE_NAME", (String) dbDetails.get("CONN_DB_NAME"));
			inputMap.add("SESSION_ID", sessionId);
			inputMap.add("QUESTION", question);
			if (tableName != null && !"".equalsIgnoreCase(tableName) && !"null".equalsIgnoreCase(tableName)) {
				inputMap.add("TABLE_NAME", tableName);
			} else {
				inputMap.add("dashboard_name", dashboardName);
				inputMap.add("login_user", ((shareUserName !=null && !"".equalsIgnoreCase(shareUserName) && !"null".equalsIgnoreCase(shareUserName))
						?shareUserName:(String)request.getSession(false).getAttribute("ssUsername")));
			}
			if (voiceFlag != null && !"".equalsIgnoreCase(voiceFlag) && !"null".equalsIgnoreCase(voiceFlag)) {
				inputMap.add("Voice_Flag", voiceFlag);
			}
			HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<MultiValueMap<String, String>>(inputMap,
					headers);
			RestTemplate template = new RestTemplate();
			ResponseEntity<String> response = template.postForEntity("http://apihub.pilogcloud.com:6676/chat_with_data",
					entity, String.class);
			String apiDataStr = response.getBody();
			if (apiDataStr != null && !"".equalsIgnoreCase(apiDataStr) && !"null".equalsIgnoreCase(apiDataStr)) {
				System.out.println(apiDataStr);
				resultObj.put("resultDataStr", apiDataStr);
				JSONObject apiObject = (JSONObject) JSONValue.parse(apiDataStr);
				if (apiObject.get("ANSWER") instanceof JSONObject) {
					resultObj.put("isJson", true);
				} else {
					if (apiObject.get("ANSWER") instanceof JSONArray) {
						resultObj.put("isJson", false);
					}
				}
				if (voiceFlag != null && !"".equalsIgnoreCase(voiceFlag) && !"null".equalsIgnoreCase(voiceFlag)) {
					resultObj.put("blobData", responseId);
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return resultObj;
	}

	@Transactional
	public JSONObject getAILensAnalyticsUserExistTableNamesData(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String tableDiv = "";
			String userName = (String) request.getParameter("userName");
			if (userName != null && !"".equalsIgnoreCase(userName)) {
				String fetchQuery = "SELECT DISTINCT TABLE_NAME  FROM C_ETL_DAL_AUTHORIZATION WHERE CREATE_BY =:CREATE_BY AND ORGN_ID=:ORGN_ID";
				Map mapData = new HashMap();
				mapData.put("CREATE_BY", userName);
				mapData.put("ORGN_ID", request.getSession(false).getAttribute("ssOrgId"));
				List listData = access.sqlqueryWithParams(fetchQuery, mapData);
				if (listData != null && !listData.isEmpty()) {
					tableDiv = "<div id='aIlensAnalyticsExistUserTableNamesDivId' class='aIlensAnalyticsExistUserTableNamesDivClass text-right replyIntelisenseView noBubble'>"
							// + "<p class='nonLoadedBubble'>Existing Files/Tables</p>"
							+ "<div class=\"userAIlensAnalyticsExistTableNamesSearch nonLoadedBubble\" >"
							+ "<input type=\"text\" placeholder=\"search\" id='data-AnalyticsExistTablessearch'/>"
							// + "<button class=\"userAIlensAnalyticsExistTableNamesButton\"
							// onclick=\"getAILensAnalyticsQuestions()\">Ok</button>"
							+ "</div>"
							+ "<div id='userAIlensAnalyticsExistTableNamesDivId' class='userAIlensAnalyticsExistTableNamesDivClass nonLoadedBubble'>";
					for (int i = 0; i < listData.size(); i++) {
						String tableName = (String) listData.get(i);
						tableDiv += "<div id='" + tableName
								+ "_table' class='userTableNameClass checkDivSingleCheckBoxClass' onclick=toggleAIlensTableSelectCheckbox('"
								+ tableName
								+ "') data-AIlensAnalyticsExistTablefilter-item data-AILensInsightExistfilter-name=\""
								+ tableName + "\">" + "<input onclick=toggleAIlensTableSelectCheckbox('" + tableName
								+ "') type='checkbox' class='checkBoxSingleCheckBoxClass' id='userAIlensAnalyticsCheckbox_"
								+ tableName + "' name='tableSelectCheckbox' value='" + tableName + "'>" + tableName
								+ "</div>";
					}
					tableDiv += "</div>" + "</div>";
				}
				resultObj.put("tableDiv", tableDiv);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	@Transactional
	public JSONObject getAILensFirstHeaders(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String tabId = request.getParameter("tabId");
			String domain = request.getParameter("domain");
			String fetchQuery = "SELECT " + "QUESTION_ID," + // 1
					"DOMAIN," + // 2
					"QUESTION," + // 3
					"SHOW_SEQ," + // 4
					"QUESTION_TYPE," + // 5
					"PRIMARY_QUESTION_ID," + // 6
					"ANSWER," + // 7
					"ANSWER_TYPE," + // 8
					"VIDEO_URL," + // 9
					"API_URL," + // 10
					"API_TYPE," + // 11
					"AI_API_FLAG," + // 12
					"AI_API_URL, " + // 13
					"QUES_ONCLICK, " + // 14
					"QUES_ICON, " + // 15
					"BUTTON_POSITION, " + // 16
					"INTRO_STEPS " + // 17
					" FROM DAL_AI_LENS_QUESTIONS WHERE QUESTION_ID =:QUESTION_TYPE AND ANSWER_TYPE =:ANSWER_TYPE AND DOMAIN =:DOMAIN AND ACTIVE_FLAG =:ACTIVE_FLAG";
			// String fetchQuery = "SELECT ANSWER FROM DAL_AI_LENS_QUESTIONS WHERE
			// QUESTION_TYPE =:QUESTION_TYPE AND ANSWER_TYPE =:ANSWER_TYPE AND DOMAIN
			// =:DOMAIN ";
			Map insertMap = new HashMap();
			insertMap.put("ANSWER_TYPE", "FM");
			insertMap.put("DOMAIN", domain);
			insertMap.put("QUESTION_TYPE", tabId);
			insertMap.put("ACTIVE_FLAG", 'Y');
			List<Object[]> listData = access.sqlqueryWithParams(fetchQuery, insertMap);
			if (listData != null && !listData.isEmpty()) {
				Object[] firstRow = listData.get(0);
				resultObj.put("QUESTION_ID", firstRow[0]);
				resultObj.put("DOMAIN", firstRow[1]);
				resultObj.put("QUESTION", firstRow[2]);
				resultObj.put("SHOW_SEQ", firstRow[3]);
				resultObj.put("QUESTION_TYPE", firstRow[4]);
				resultObj.put("PRIMARY_QUESTION_ID", firstRow[5]);
				resultObj.put("ANSWER", firstRow[6]);
				resultObj.put("ANSWER_TYPE", firstRow[7]);
				resultObj.put("VIDEO_URL", firstRow[8]);
				resultObj.put("API_URL", firstRow[9]);
				resultObj.put("API_TYPE", firstRow[10]);
				resultObj.put("AI_API_FLAG", firstRow[11]);
				resultObj.put("AI_API_URL", firstRow[12]);
				resultObj.put("QUES_ONCLICK", firstRow[13]);
				resultObj.put("QUES_ICON", firstRow[14]);
				resultObj.put("BUTTON_POSITION", firstRow[15]);
				resultObj.put("INTRO_STEPS", this.cloudUtills.clobToString((Clob) firstRow[16]));
				resultObj.put("STATUS", true);
			}

		} catch (Exception e) {
			e.printStackTrace();
			resultObj.put("STATUS", false);
		}
		return resultObj;
	}

	public JSONObject decodeSqlQuery(String query) {
		JSONObject resultObj = new JSONObject();
		try {
			Select select = (Select) CCJSqlParserUtil.parse(query);
			net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(query);
			TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
			net.sf.jsqlparser.statement.select.Select selectStatement = (Select) statement;
			List<String> tableList = tablesNamesFinder.getTableList(statement);
			if (tableList != null && !tableList.isEmpty()) {
				resultObj.put("tableNames", String.join(",", tableList));
			}
			String whereCondition, columnsStr;
			PlainSelect ps = (PlainSelect) select.getSelectBody();
			if (ps.getWhere() != null) {
				System.out.println(ps.getWhere().toString());
				whereCondition = ps.getWhere().toString();
				if (!whereCondition.contains("temp")) {
					resultObj.put("whereCondition", ps.getWhere().toString());
				}
			} else {
//					if (ps.getFromItem() instanceof ParenthesedSelect) {
//						ParenthesedSelect subSelect = (ParenthesedSelect) ps.getFromItem();
//						PlainSelect subPlainSelect = (PlainSelect) subSelect.getSelectBody();
//						if (subPlainSelect.getWhere() != null) {
//							Expression subWhere = subPlainSelect.getWhere();
//							System.out.println("Subquery WHERE condition: " + subWhere.toString());
//						}
//					}
			}
			if (ps.getSelectItems() != null) {
				resultObj.put("columns", ps.getSelectItems().toString());
			}
			JSONArray columnsArr = new JSONArray();
			List<SelectItem<?>> joins = ((PlainSelect) ((Select) select).getSelectBody()).getSelectItems();
			for (SelectItem join : joins) {
				columnsArr.add(join.toString());

			}
			if (select instanceof PlainSelect) {
				PlainSelect plainSelect = (PlainSelect) select;
				if (plainSelect.getJoins() != null) {
					List<Join> joins1 = plainSelect.getJoins();
					List joinItems = new ArrayList();
					JSONObject tableNamesAndAliasNamesObj = new JSONObject();

					Table fromTable = (Table) plainSelect.getFromItem();
					String fromTableName = fromTable.getName();
					tableNamesAndAliasNamesObj.put(fromTableName, fromTable.getAlias());
					for (Join join : joins1) {
						String joinItem = join.toString();
						System.out.println("fromTableName:::" + fromTableName);
						System.out.println("joinItem:::" + joinItem);
						joinItems.add(joinItem);
						if (join.getFromItem() instanceof Table)
							tableNamesAndAliasNamesObj.put(((Table) join.getFromItem()).getName(),
									join.getFromItem().getAlias());
					}
					resultObj.put("joinItems", joinItems.get(joinItems.size() - 1).toString());
					resultObj.put("tableNamesAndAliasNamesObj", tableNamesAndAliasNamesObj);
				} else {
//						Table fromTable = (Table) plainSelect.getFromItem();
//						String fromTableName = fromTable.getName();
//						System.out.println("fromTableName:::" + fromTableName);
				}
			}

			resultObj.put("columns", String.join(",", columnsArr));

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	@Transactional()
	public void getAILensAnalyticsInsightsAudio(HttpServletRequest request, HttpServletResponse response) {
		Connection connection = null;
		try {
			String query = "SELECT RESPONSE_FILE FROM PYTHON_API_RESPONSE " + "WHERE RESPONSE_ID='"
					+ (String) request.getParameter("responseId") + "'";
			System.out.println("query::::" + query);

			String fileType = "audio/mp3";
			List videoList = access.sqlqueryWithParams(query, Collections.EMPTY_MAP);
			if (videoList != null && !videoList.isEmpty()) {
				Blob blob = (Blob) videoList.get(0);
				if (fileType.trim().equalsIgnoreCase("audio/mp3")) {
					response.setContentType("audio/mp3");
				} else {
					response.setContentType("application/octet-stream");
				}
				response.setHeader("Content-Disposition", "inline; filename=audioFile");
				byte[] data = blob.getBytes(1, (int) ((java.sql.Blob) blob).length());

				if (data == null) {
					data = "THIS IS AN EMPTY FILE!!".getBytes();
				}
				System.out.println("audio data :::" + data);
				OutputStream o = response.getOutputStream();
				o.write(data);
				o.flush();
				o.close();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Transactional()
	public JSONObject saveSentDashBoardMailLastRun(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String lastRunTime = cloudGridFormationDAO.getOrgPropertyValue(request, "DASHBOARD_LAST_RUN");
			;
			System.out.println("lastRunTime::::" + lastRunTime);
			if (lastRunTime != null && !"".equalsIgnoreCase(lastRunTime) && !"null".equalsIgnoreCase(lastRunTime)) {
				resultObj.put("lastRun", lastRunTime);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	@Transactional()
	public JSONObject getDashBoardsForMail(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String dashBoardNames = cloudGridFormationDAO.getOrgPropertyValue(request, "DASHBOARD_NAMES");
			;
			System.out.println("dashBoardNames::::" + dashBoardNames);
			if (dashBoardNames != null && !"".equalsIgnoreCase(dashBoardNames)
					&& !"null".equalsIgnoreCase(dashBoardNames)) {
				resultObj.put("dashBoardNames", dashBoardNames);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public String getApiResponse(HttpServletRequest request, String responseId, ExecutorService executor,
			int currentTimeInMilliSec) {
		String result = "";
		int maxWaitingTime = 3600000;
		String apiResponseWaitTimeInMilliSec = request.getParameter("apiResponseWaitTimeInMilliSec");
		if (apiResponseWaitTimeInMilliSec != null && !"".equalsIgnoreCase(apiResponseWaitTimeInMilliSec)
				&& !"null".equalsIgnoreCase(apiResponseWaitTimeInMilliSec)) {
			maxWaitingTime = Integer.parseInt(apiResponseWaitTimeInMilliSec);
		}
		try {
			if (currentTimeInMilliSec >= maxWaitingTime) {
				executor.shutdown();
			}
			String query = "SELECT RESPONSE_DATA FROM PYTHON_API_RESPONSE WHERE RESPONSE_ID='" + responseId + "'";
			List<Clob> list = this.access.sqlqueryWithParams(query, Collections.EMPTY_MAP);
			if (list != null && !list.isEmpty()) {
				result = cloudUtills.clobToString(list.get(0));
				if (result != null && !"".equalsIgnoreCase(result) && !"null".equalsIgnoreCase(result)) {
					// deleteApiResponse(responseId);
				}
			} else {
				currentTimeInMilliSec += 10000;
				Thread.sleep(10000L);
				if (executor.isShutdown()) {
					query = "SELECT RESPONSE_DATA FROM PYTHON_API_RESPONSE WHERE RESPONSE_ID='" + responseId + "'";
					list = this.access.sqlqueryWithParams(query, Collections.EMPTY_MAP);
					if (list != null && !list.isEmpty()) {
						result = cloudUtills.clobToString(list.get(0));
						if (result != null && !"".equalsIgnoreCase(result) && !"null".equalsIgnoreCase(result)) {
							// deleteApiResponse(responseId);
						} else {
							result = getApiResponse(request, responseId, executor, currentTimeInMilliSec);
						}
					} else {
						result = getApiResponse(request, responseId, executor, currentTimeInMilliSec);
					}
				} else {
					System.out.println("Response Waiting Time :::: " + currentTimeInMilliSec + "ms");
					System.out.println("Thread waiting");
					result = getApiResponse(request, responseId, executor, currentTimeInMilliSec);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	@Transactional
	public int deleteApiResponse(String responseId) {
		int count = 0;
		try {
			String deleteQuery = "DELETE FROM PYTHON_API_RESPONSE WHERE RESPONSE_ID='" + responseId + "'";
			count = access.executeUpdateSQL(deleteQuery);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return count;
	}

	@Transactional
	public String voicefileTextData(String responseId) {
		String result = "";
		try {
			String fileQuery = "SELECT FILE_SCRIPT FROM PYTHON_API_RESPONSE WHERE RESPONSE_ID='" + responseId + "'";
			List<Clob> list = this.access.sqlqueryWithParams(fileQuery, Collections.EMPTY_MAP);
			result = cloudUtills.clobToString(list.get(0));
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return result;
	}

	@Transactional
	public void getPdfBasedonPath(HttpServletRequest request, HttpServletResponse response) {
		try {
			String fileName = request.getParameter("fileName");
			String excelFilePath = etlFilePath + "Files/TreeDMImport/"
					+ request.getSession(false).getAttribute("ssUsername");
			File file = new File(excelFilePath);
			if (file.exists()) {
				file.delete();
			}
			if (!file.exists()) {
				file.mkdirs();
			}
			if (fileName.lastIndexOf(File.separator) >= 0) {

				file = new File(fileName);
			} else {
				file = new File(excelFilePath + File.separator + fileName);
			}
			if (file != null) {
				String fileType = fileName.substring(fileName.lastIndexOf(".") + 1, fileName.length());
				System.err.println("filename after:::" + fileName);
				if (fileType.trim().equalsIgnoreCase("txt") || fileType.trim().equalsIgnoreCase("text/plain")) {
					response.setContentType("text/plain");
				} else if (fileType.trim().equalsIgnoreCase("text/html")) {
					response.setContentType("text/html");
				} else if (fileType.trim().equalsIgnoreCase("text/xml")) {
					response.setContentType("text/html");
				} else if (fileType.trim().equalsIgnoreCase("tif")) {
					response.setContentType("image/tiff");
				} else if (fileType.trim().equalsIgnoreCase("tiff")) {
					response.setContentType("image/tiff");
				} else if (fileType.trim().equalsIgnoreCase("doc") || fileType.trim()
						.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.wordprocessingml.document")) {
					response.setContentType("application/msword");
				} else if (fileType.trim().equalsIgnoreCase("xls") || fileType.trim()
						.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")) {
					response.setContentType("application/vnd.ms-excel");
				} else if (fileType.trim().equalsIgnoreCase("pdf")
						|| fileType.trim().equalsIgnoreCase("application/pdf")) {
					response.setContentType("application/pdf");
				} else if (fileType.trim().equalsIgnoreCase("json")
						|| fileType.trim().equalsIgnoreCase("application/json")) {
					response.setContentType("application/json");
				} else if (fileType.trim().equalsIgnoreCase("ppt") || fileType.trim().equalsIgnoreCase(
						"application/vnd.openxmlformats-officedocument.presentationml.presentation")) {
					response.setContentType("application/ppt");
				} else if (fileType.trim().equalsIgnoreCase("image/png")) {
					response.setContentType("image/png");
				} else if (fileType.trim().equalsIgnoreCase("image/jpeg")) {
					response.setContentType("image/jpeg");
				} else if (fileType.trim().equalsIgnoreCase("image/svg")) {
					response.setContentType("image/svg");
				} else if (fileType.trim().equalsIgnoreCase("image/svg+xml")) {
					response.setContentType("image/svg+xml");
				} else if (fileType.trim().equalsIgnoreCase("image/gif")) {
					response.setContentType("image/gif");
				} else if (fileType.trim().equalsIgnoreCase("image/eps")) {
					response.setContentType("image/eps");
				} else if (fileType.trim().equalsIgnoreCase("video/mp4")) {
					response.setContentType("video/mp4");
				} else if (fileType.trim().equalsIgnoreCase("video/x-ms-wmv")) {
					response.setContentType("video/x-ms-wmv");
				} else {
					response.setContentType("application/octet-stream");
				}
				response.setHeader("Content-Disposition", "inline; filename=" + fileName);
				byte[] data = new byte[(int) file.length()]; // Create byte array of file size
				try (FileInputStream fileInputStream = new FileInputStream(file)) {
					// Read file into byte array
					int bytesRead;
					OutputStream os = response.getOutputStream();
					byte[] buffer = new byte[4096];
					while ((bytesRead = fileInputStream.read(buffer)) != -1) {
						os.write(buffer, 0, bytesRead);
					}
					os.flush();

				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Transactional
	public JSONObject getShareDashBoardUsersList(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			JSONArray userNamesArr = new JSONArray();
			String roleId = (String) request.getSession(false).getAttribute("ssRole");
			String ssUserName = (String) request.getSession(false).getAttribute("ssUsername");
			Map userNamesMap = new HashMap();
			String userNamesQuery = "SELECT DISTINCT USER_NAME FROM S_PERS_DETAIL SPD,S_PERSONNEL SP,S_PERS_PROFILE SPP WHERE SP.ORGN_ID=:ORGN_ID AND SPD.USER_NAME <> '"
					+ ssUserName
					+ "' AND SP.STATUS=:STATUS AND SPP.ROLE_ID <> 'MM_MANAGER' AND SPP.DEFAULT_IND='Y' AND SP.EXPIRY_DATE > SYSDATE";
			JSONArray dashBordArr = new JSONArray();
			userNamesMap.put("STATUS", "ACTIVE");
			userNamesMap.put("ORGN_ID", request.getSession(false).getAttribute("ssOrgId"));
			List userNamesList = access.sqlqueryWithParams(userNamesQuery, userNamesMap);
			if (userNamesList != null && !userNamesList.isEmpty()) {
				for (int i = 0; i < userNamesList.size(); i++) {
					String userName = (String) userNamesList.get(i);
					if (userName != null && !"".equalsIgnoreCase(userName) && !"null".equalsIgnoreCase(userName)) {
						userNamesArr.add(userName);
					}
				}

			}
			resultObj.put("userNamesArr", userNamesArr);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	@Transactional
	public JSONObject getMailShareDashBoardUsersList(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			JSONArray userNamesArr = new JSONArray();
			String roleId = (String) request.getSession(false).getAttribute("ssRole");
			String ssUserName = (String) request.getSession(false).getAttribute("ssUsername");
			Map userNamesMap = new HashMap();
			String userNamesQuery = "SELECT DISTINCT USER_NAME FROM S_PERS_DETAIL SPD,S_PERSONNEL SP,S_PERS_PROFILE SPP WHERE SP.ORGN_ID=:ORGN_ID AND SPD.USER_NAME <> '"
					+ ssUserName + "' AND SP.STATUS=:STATUS AND  SP.EXPIRY_DATE > SYSDATE";
			JSONArray dashBordArr = new JSONArray();
			userNamesMap.put("STATUS", "ACTIVE");
			userNamesMap.put("ORGN_ID", request.getSession(false).getAttribute("ssOrgId"));
			List userNamesList = access.sqlqueryWithParams(userNamesQuery, userNamesMap);
			String managerName = "";
			if (roleId != null && !"".equalsIgnoreCase(roleId) && !"null".equalsIgnoreCase(roleId)
					&& !roleId.contains("MANAGER")) {
				String managerQuery = "SELECT DISTINCT MANAGER_NAME FROM V_SUB_ORDINATES_BY_MANAGER WHERE ORGN_ID =:ORGN_ID AND  USER_LIST LIKE '%"
						+ ssUserName + "%'";
				Map managerMap = new HashMap();
				managerMap.put("ORGN_ID", request.getSession(false).getAttribute("ssOrgId"));
				List managerNameList = access.sqlqueryWithParams(managerQuery, managerMap);
				if (managerNameList != null && !managerNameList.isEmpty()) {
					managerName = (String) managerNameList.get(0);
				}
			}
			/*
			 * else if (roleId != null && !"".equalsIgnoreCase(roleId) &&
			 * !"null".equalsIgnoreCase(roleId) && roleId.contains("MANAGER")) { Map
			 * mgruserNamesMap = new HashMap(); String mgruserNamesQuery =
			 * "SELECT DISTINCT USER_NAME FROM S_PERS_DETAIL SPD,S_PERSONNEL SP,S_PERS_PROFILE SPP WHERE SP.ORGN_ID=:ORGN_ID AND SPD.USER_NAME <> '"
			 * + ssUserName +
			 * "' AND SP.STATUS=:STATUS AND SP.EXPIRY_DATE > SYSDATE AND SPP.ROLE_ID=:ROLE_ID"
			 * ; userNamesMap.put("STATUS", "ACTIVE"); userNamesMap.put("ORGN_ID",
			 * request.getSession(false).getAttribute("ssOrgId"));
			 * userNamesMap.put("ROLE_ID",
			 * request.getSession(false).getAttribute("ssRole")); List managerNameList =
			 * access.sqlqueryWithParams(mgruserNamesQuery, mgruserNamesMap); if
			 * (managerNameList != null && !managerNameList.isEmpty()) { for (int i = 0; i <
			 * managerNameList.size(); i++) { String userName = (String)
			 * managerNameList.get(i); if (userName != null &&
			 * !"".equalsIgnoreCase(userName) && !"null".equalsIgnoreCase(userName)) {
			 * userNamesList.remove(userName); } } } }
			 */

			if (userNamesList != null && !userNamesList.isEmpty()) {
				if (userNamesList.contains(managerName)) {
					userNamesList.remove(managerName);
				}
				for (int i = 0; i < userNamesList.size(); i++) {
					String userName = (String) userNamesList.get(i);
					if (userName != null && !"".equalsIgnoreCase(userName) && !"null".equalsIgnoreCase(userName)) {
						userNamesArr.add(userName);
					}
				}

			}
			resultObj.put("userNamesArr", userNamesArr);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	@Transactional
	public JSONObject saveDashBoardUsersList(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String message = "";
			JSONArray userNamesArr = new JSONArray();
			String roleId = (String) request.getSession(false).getAttribute("ssRole");
			String orgnId = (String) request.getSession(false).getAttribute("ssOrgId");
			String dashBoardName = (String) request.getParameter("dashBoardName");
			String userNamesArrStr = (String) request.getParameter("userNamesArr");
			if (userNamesArrStr != null && !"".equalsIgnoreCase(userNamesArrStr)
					&& !"null".equalsIgnoreCase(userNamesArrStr)) {
				userNamesArr = (JSONArray) JSONValue.parse(userNamesArrStr);
				if (userNamesArr != null && !userNamesArr.isEmpty()) {
					userNamesArrStr = String.join(",", userNamesArr);
				}
			}
			Map userNamesMap = new HashMap();
			String userNamesQuery = "UPDATE O_RECORD_VISUALIZATION SET VISUALIZE_CUST_COL29 =  CASE  WHEN VISUALIZE_CUST_COL29 IS NULL THEN '"
					+ userNamesArrStr + "'   ELSE VISUALIZE_CUST_COL29 || ',' || '" + userNamesArrStr + "' END "
					+ " WHERE ORGN_ID=:ORGN_ID AND DASHBORD_NAME=:DASHBORD_NAME AND ROLE_ID=:ROLE_ID";
			JSONArray dashBordArr = new JSONArray();
			userNamesMap.put("ORGN_ID", orgnId);
			userNamesMap.put("ROLE_ID", roleId);
			userNamesMap.put("DASHBORD_NAME", dashBoardName);
			System.out.println("userNames Update Query is:::" + userNamesQuery);
			System.out.println("userNames Update Map is:::" + userNamesMap);
			int count = access.executeUpdateSQL(userNamesQuery, userNamesMap);
			if (count > 0) {
				message = "Dashboard is shared successfully";
			}
			resultObj.put("message", message);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	@Transactional
	public void getDataBasedOnTemplateId(HttpServletRequest request, HttpServletResponse response) {
		try {
			String query = "SELECT TEMPLATE_CONTENT,FILE_NAME,FILE_EXT FROM B_ATTACH_TEMPLATE " + "WHERE TEMPLATE_ID='"
					+ (String) request.getParameter("templateId") + "'";
			System.out.println("query::::" + query);
			List videoList = access.sqlqueryWithParams(query, Collections.EMPTY_MAP);
			if (videoList != null && !videoList.isEmpty()) {
				Object[] videoObj = (Object[]) videoList.get(0);
				String filename = (String) videoObj[1];
				String fileType = (String) videoObj[2];

				filename = URLEncoder.encode(filename, "UTF-8");
				System.err.println("filename::::" + filename);
				if (filename != null && filename.indexOf("/") > -1) {
					filename = filename.substring(filename.lastIndexOf("/") + 1);
				}
				if (filename != null && filename.indexOf("\\") > -1) {
					filename = filename.substring(filename.lastIndexOf("\\") + 1);
				}
				System.err.println("filename after:::" + filename);
				if (fileType.trim().equalsIgnoreCase("txt") || fileType.trim().equalsIgnoreCase("text/plain")) {
					response.setContentType("text/plain");
					// filename = filename + ".txt";
				} else if (fileType.trim().equalsIgnoreCase("text/html")) {
					response.setContentType("text/html");
					// filename = filename + ".tif";
				} else if (fileType.trim().equalsIgnoreCase("text/xml")) {
					response.setContentType("text/html");
					// filename = filename + ".tif";
				} else if (fileType.trim().equalsIgnoreCase("tif")) {
					response.setContentType("image/tiff");
					// filename = filename + ".tif";
				} else if (fileType.trim().equalsIgnoreCase("tiff")) {
					response.setContentType("image/tiff");
					// filename = filename + ".tiff";
				} else if (fileType.trim().equalsIgnoreCase("doc") || fileType.trim()
						.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.wordprocessingml.document")) {
					response.setContentType("application/vnd.openxmlformats-officedocument.wordprocessingml.document");
					// filename = filename + ".doc";
				} else if (fileType.trim().equalsIgnoreCase("xls") || fileType.trim()
						.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")) {
					response.setContentType("application/vnd.ms-excel");
					// filename = filename + ".xls";
				} else if (fileType.trim().equalsIgnoreCase("pdf")
						|| fileType.trim().equalsIgnoreCase("application/pdf")) {
					response.setContentType("application/pdf");
					// filename = filename + ".pdf";
				} else if (fileType.trim().equalsIgnoreCase("ppt") || fileType.trim().equalsIgnoreCase(
						"application/vnd.openxmlformats-officedocument.presentationml.presentation")) {
					response.setContentType("application/ppt");
					// filename = filename + ".ppt";
				} else if (fileType.trim().equalsIgnoreCase("image/png")) {
					response.setContentType("image/png");
					// filename = filename + ".ppt";
				} else if (fileType.trim().equalsIgnoreCase("image/jpeg")) {
					response.setContentType("image/jpeg");
					// filename = filename + ".ppt";
				} else if (fileType.trim().equalsIgnoreCase("image/svg")) {
					response.setContentType("image/svg");
					// filename = filename + ".ppt";
				} else if (fileType.trim().equalsIgnoreCase("image/gif")) {
					response.setContentType("image/gif");
					// filename = filename + ".ppt";
				} else if (fileType.trim().equalsIgnoreCase("image/eps")) {
					response.setContentType("image/eps");
					// filename = filename + ".ppt";
				} else if (fileType.trim().equalsIgnoreCase("video/mp4")) {
					response.setContentType("video/mp4");
				} else if (fileType.trim().equalsIgnoreCase("video/x-ms-wmv")) {
					response.setContentType("video/x-ms-wmv");
				} else {
					response.setContentType("application/octet-stream");
					// filename = filename + ".txt";
				}
				response.setHeader("Content-Disposition", "inline; filename=" + filename);
				Blob blob = (Blob) videoObj[0];
				byte[] data = blob.getBytes(1, (int) ((java.sql.Blob) blob).length());
				ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
				ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

				// Convert DOCX to PDF

				if (data == null) {
					data = "THIS IS AN EMPTY FILE!!".getBytes();
				}
				OutputStream o = response.getOutputStream();
				o.write(data);
				o.flush();
				o.close();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
