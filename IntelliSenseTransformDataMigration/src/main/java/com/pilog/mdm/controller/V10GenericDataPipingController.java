/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.controller;

import com.pilog.mdm.DAO.iVisionTransformConnectionTabsDAO;
import com.pilog.mdm.service.V10GenericDataPipingService;
import com.pilog.mdm.service.V10GenericProcessETLDataService;
import com.pilog.mdm.service.iVisionTransformConnectionTabsService;
import com.pilog.mdm.service.iVisionTransformSaveJobsService;
import com.pilog.mdm.utilities.PilogUtilities;
import com.sap.mw.jco.JCO;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.persistence.criteria.From;
import javax.servlet.ServletContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

/**
 *
 * @author Naidu
 */
@Controller
public class V10GenericDataPipingController {

	@Autowired
	private V10GenericDataPipingService dataPipingService;

	private PilogUtilities visionUtills = new PilogUtilities();
//    @Autowired
//    private VisionSearchService visionSearchService;
	@Autowired
	private V10GenericProcessETLDataService genericProcessETLDataService;

	@Autowired
	private iVisionTransformSaveJobsService saveJobsService;

	@Autowired
	private iVisionTransformConnectionTabsService connectionTabsService;

	@Autowired
	private iVisionTransformConnectionTabsDAO visionTransformConnectionTabsDAO;

	@Value("${jdbc.driver}")
	private String dataBaseDriver;
	@Value("${jdbc.username}")
	private String userName;
	@Value("${jdbc.password}")
	private String password;
	@Value("${jdbc.url}")
	private String dbURL;

	@Value("${MultipartResolver.fileUploadSize}")
	private long maxFileSize;
	@Value("${MultipartResolver.fileinMemorySize}")
	private int maxMemSize;

	// @Value("${etl.file.path}")
	private String etlFilePath;
	{
		if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")) {
			etlFilePath = "C://";
		} else {
			etlFilePath = "/u01/";
		}
	}

	@RequestMapping(value = "/getDataPiping", method = { RequestMethod.POST, RequestMethod.GET })
	public JSONObject dataPiping(HttpServletRequest request) {
		JSONObject dataPipingObj = new JSONObject();

		try {
			HttpSession session = request.getSession(false);
			String subscriptionType = (String) request.getSession(false).getAttribute("ssSubscriptionType");
			if (subscriptionType != null && !"".equalsIgnoreCase(subscriptionType)
					&& !"null".equalsIgnoreCase(subscriptionType)) {
				subscriptionType = subscriptionType.toUpperCase();
			}
			Object role = session.getAttribute("ssOrgId");
			Object username = session.getAttribute("ssUsername");
			Object ssRole = session.getAttribute("ssRole");
			String treeId = request.getParameter("treeId");
			String headerFlag = request.getParameter("headerFlag");

			String formJobFlag = request.getParameter("formJobHidden");
			String formJobId = request.getParameter("formJobId");
			String formJobName = request.getParameter("formJobName");

			JSONObject treeObj = dataPipingService.getGenericTreeOpt(request, treeId);

			dataPipingObj.put("treeId", treeId);
			dataPipingObj.put("treeDesc", treeObj.get("treeDesc"));
			dataPipingObj.put("treeLov", treeObj.get("selectBoxStr"));
			dataPipingObj.put("labelobj", visionUtills.getMultilingualObject(request));
			dataPipingObj.put("headerFlag", headerFlag);
			dataPipingObj.put("treePageSize", "50");
			String extTreeParamsStr = request.getParameter("extTreeParams");
			JSONObject extTreeParams = new JSONObject();
			String dragEndFunction = "function (item) {\n"
					+ "                                      createImageSourcesFlowchart(item,item.label,'Table');\n"
					+ "//                                        $('#feedHeader').append(item.label);\n"
					+ "                                    }";
			if (extTreeParamsStr != null && !"".equalsIgnoreCase(extTreeParamsStr)
					&& !"null".equalsIgnoreCase(extTreeParamsStr)) {
				extTreeParams = (JSONObject) JSONValue.parse(extTreeParamsStr);
			}
			treeObj.put("dragEndFunction", dragEndFunction);
			dataPipingObj.put("treeObj", treeObj);
			dataPipingObj.put("extTreeParams", extTreeParams);
//            dataPipingObj.put("breadCrumb", visionSearchService.getBreadCrumbs(request, treeId));
			if (formJobFlag != null && !"".equalsIgnoreCase(formJobFlag) && "Y".equalsIgnoreCase(formJobFlag)
					&& !"null".equalsIgnoreCase(formJobFlag)) {
				dataPipingObj.put("formJobFlag", 'Y');
				dataPipingObj.put("currentGroupJobId", formJobId);
				dataPipingObj.put("currentGroupJobName", formJobName);
			}

			String getCompnoentsStr = dataPipingService.fetchEtlComponents(request);
			dataPipingObj.put("componentsDiv", getCompnoentsStr);

			// JSONObject getAvailableConnobj =
			// connectionTabsService.getAvaliableConnections(request, "parentDiv");

			// if (getAvailableConnobj != null && !getAvailableConnobj.isEmpty()) {
			// dataPipingObj.put("connMainDiv", getAvailableConnobj.get("htmlDiv"));
			// }

			dataPipingObj.put("imagePath", "images");
			dataPipingObj.put("currentV10ConnObj",
					new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName, password, "Current_V10"));
			dataPipingObj.put("availableConnections", visionTransformConnectionTabsDAO.getSavedConnections(request));

			String connectionDivStr = "<div id='jqxWidget' class=\"visionJqxWidgetsClass\">\n"
					+ "<div id=\"mainSplitter\" class=\"mainSplitterDiv\">\n" + "<div id=\"connectionsDiv\">\n"
					+ "<div class=\"showEtlIcons\">\n" + "<ul class=\"showEtlLists\">\n"
					+ "<li title=\"New Connections\" id=\"newConnections\" onclick=\"showEtlList('savedSources', 'newConnections')\">\n"
					+ "<img src=\"images/New Connection Icon-01.svg\" class=\"visionEtlTabIcons\"style=\"cursor:pointer;\"/>\n"
					+ "</li>\n"
					+ "<li title=\"Available Connections\" id=\"availableConnections\" onclick=\"showEtlList('savedConnectionsIconsDiv', 'availableConnections')\" >\n"
					+ "<img src=\"images/Saved Connection Icon-01.svg\" class=\"visionEtlTabIcons\"style=\"cursor:pointer;\"/>\n"
					+ "</li>\n"
					+ "<li title=\"Available Jobs\" id=\"availableJobs\" onclick=\"showEtlList('availableJobsDiv', 'availableJobs')\"> \n"
					+ "<img src=\"images/Datebase jobs Icon2-01.svg\" class=\"visionEtlTabIcons\"style=\"cursor:pointer;\"/>\n"
					+ "</li>\n"
					+ "<li title=\"Schema Objects\" id=\"schemaObjects\" onclick=\"showEtlList('schemaObjectsDiv', 'schemaObjects')\" >\n"
					+ "<img src=\"images/GridDB.svg\" class=\"visionEtlTabIcons visionETLIcons\"\n"
					+ "style=\"cursor:pointer;\"/>\n" + "</li>\n" + "</ul>\n" + "</div>\n"
					+ "<div id='connectionsTabs'>\n" // START
					+ "<div id=\"savedSources\" class =\"savedSourcesClass\" style=\"display:none\"></div>\n"
					+ "<div id =\"savedConnectionsIconsDiv\" class=\"savedConnectionsClass\" style=\"display:none\">\n"
					+ "<div id=\"conTabs\"class=\"conTabsClass\">\n" + "<ul class=\"conTabsClassList\">\n"
					+ "<li style=\"margin-left: 1px;\"><span style=\"display:none\">Icon</span><img src=\"images/availableConn.svg\" class=\"visionETLIcons\" title=\"Icon Format\" style=\"width:15px;height: 15px;cursor:pointer;\" /></li> \n"
					+ "<li style=\"margin-left: 1px;\"><span style=\"display:none\">Tree</span><img src=\"images/tree.svg\" class=\"visionETLIcons\" title=\"Tree Format\" style=\"width:15px;height: 15px;cursor:pointer;\" /></li>\n"
					+ "<li style=\"margin-left: 1px;\"><span style=\"display:none\">FAV</span><img src=\"images/starIcon.svg\" class=\"visionETLIcons\" title=\"favorites\" style=\"width:15px;height: 15px;cursor:pointer;\" /></li>\n"
					+ "</ul>\n" + "<div id=\"savedConnectionsModified\">\n"
					+ "<div id=\"ConnInnerPreviousButtonDiv\"></div>\n"
					+ "<div id=\"ConnInnerDiv\" class=\"ConnInnerDivClass\">\n" + "</div>\n" + "</div>\n" + "<div>\n"
					+ "<div id=\"savedConnIcons\" class=\"savedConnIconsClass\">\n"
					+ "<img src=\"images/Refresh Icon.svg\" class=\"visionETLIcons\" title=\"Refresh\" style=\"width:15px;height: 15px;cursor:pointer;\" \n"
					+ "onclick='refreshMappingTables()'/>"
					+ "<img src=\"images/Filter Icon-01.svg\" class=\"visionETLIcons\" id=\"treeETLFilterImage\" title=\"Filter\" style=\"width:15px;height: 15px;cursor:pointer;\"\n"
					+ "onclick='filterMappingTables()'/>\n"
//                    + "<img src=\"images/Filter Icon2-01.svg\" class=\"visionETLIcons\" title=\"Filter\" style=\"width:18px;height: 18px;cursor:pointer;\"\n"
//                    + "onclick='filterMappingTables()'/>\n"
					+ "</div>\n" + "<div id=\"savedConnections\" >\n" + "</div>\n" + "</div>\n"
					+ "<div id=\"favSchemasDiv\" class=\"favSchemasDivClass\">\n"
					+ "<div id=\"favSchemasDivInner\" class=\"favSchemasDivInnerClass\"></div>\n" + "</div> \n"
					+ "</div> \n" + "</div>\n"
					+ "<div id=\"availableJobsDiv\" class=\"visionAvailableJobsDiv\" style=\"display:none\">\n"
					+ "<div id=\"availableJobsId\"></div>\n" + "<div id=\"preDefinedJobs\"></div>\n" + "</div>\n"
					+ "<div id=\"schemaObjectsDiv\" class=\"visionSchemaObjects\"style=\"display:none\">\n"
					+ "<div id='jqxtabs'><ul></ul></div>\n" + "</div>\n"
					+ "<div id=\"dataModellerJobsDiv\" class=\"visionDataModellerJobs\"style=\"display:none\"> \n"
					+ "</div>\n" + "</div>\n" + "</div>\n" // END
					+ "<div id=\"designViewTab\">\n"
					+ "<div id=\"contentSplitter\" class=\"visionETLDesignTabContent\" >\n"
					+ "<div class=\"jqx-hideborder\" id=\"feedListExpander\">\n"
					+ "<div class=\"jqx-hideborder jqx-hidescrollbars\">\n" + "<div id=\"feedListContainer\">\n"
					+ "<div>\n" + "<div id=\"iconsdiv\" class='topIconsBarClass' style=\"float: left;\">\n"
					+ "<button class='btn btn-default' onclick='refreshMappingArea()'><span class='ImageIcon'><img src=\"images/Refresh Icon.svg\" class=\"visionETLIcons\" title=\"Refresh\" style=\"width:15px;height: 15px;cursor:pointer;\"  /></span></button>"
					+ "<button class='btn btn-default' onclick='getAllMappedData()'><span class='ImageIcon'><img src=\"images/Save Icon.svg\" class=\"visionETLIcons\" title=\"Save\" style=\"width:15px;height: 15px;cursor:pointer; \" /></span></button>"
					+ "<button class='btn btn-default' onclick='getAllMappedData()'><span class='ImageIcon'><img src=\"images/Delete_Red_Icon.svg\" class=\"visionETLIcons\" title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\" /></span></button>"
					+ "<button class='btn btn-default' onclick='askSaveJobConfirmation()'><span class='ImageIcon'><img src=\"images/Execute Job Icon-01.svg\" class=\"visionETLIcons\" title=\"Execute\" style=\"width:15px;height: 15px;cursor:pointer;\" /></span></button>"
					+ "<button class='btn btn-default' onclick='openLogFile()'><span class='ImageIcon'><img src=\"images/logicon.svg\" class=\"visionETLIcons\" title=\"View Last Job log\" style=\"width:15px;height: 15px;cursor:pointer;\" /></span></button>"
					+ "<button class='btn btn-default' onclick='stopAllJobs()'><span class='ImageIcon'><img src=\"images/cancelAllJobs.svg\" class=\"visionETLIcons\" title=\"Cancel All Jobs\" style=\"width:15px;height: 15px;cursor:pointer;\" /></span></button>"
					+ "<button class='btn btn-default' onclick='alignOperators('left')'><span class='ImageIcon'><img src=\"images/etl/left-align.png\" class=\"visionETLIcons\" title=\"Align Left\" style=\"width:15px;height: 15px;cursor:pointer;\" /></span></button>"
					+ "<button class='btn btn-default' onclick='alignOperators('center')'><span class='ImageIcon'><img src=\"images/etl/center-align.png\" class=\"visionETLIcons\" title=\"Align Center\" style=\"width:15px;height: 15px;cursor:pointer;\" /></span></button>"
					+ "<button class='btn btn-default' onclick='alignOperators('right')'><span class='ImageIcon'><img src=\"images/etl/right-align.png\" class=\"visionETLIcons\" title=\"Align Right\" style=\"width:15px;height: 15px;cursor:pointer;\" /></span></button>"
					+ "<button class='btn btn-default' onclick='enableDesignViewZoom()'><span class='ImageIcon'><img src=\"images/etl/magnifier.png\" class=\"visionETLIcons\" title=\"Enable Magnify\" style=\"width:15px;height: 15px;cursor:pointer;\" /></span></button>"
					+ "<button class='btn btn-default' onclick='disableDesignViewZoom ()'><span class='ImageIcon'><img src=\"images/etl/resetZoom.png\" class=\"visionETLIcons\" title=\"Disable Magnify\" style=\"width:15px;height: 15px;cursor:pointer;\" /></span></button>"
					+ "<button class='btn btn-default' onclick='showJobExecutionPreview()'><span class='ImageIcon'><img src=\"images/etl/preview.png\" class=\"visionETLIcons\" title=\"Job Execution Preview\" style=\"width:15px;height: 15px;cursor:pointer;\" /></span></button>"
					+ "<button class='btn btn-default' onclick='checkDataTypeValidations()'><span class='ImageIcon'><img src=\"images/etl/Validate.png\" class=\"visionETLIcons\" title=\"Validate Data Load\" style=\"width:15px;height: 15px;cursor:pointer;\" /></span></button>"
					+ "<button class='btn btn-default' onclick='openServerConsoleLog()'><span class='ImageIcon'><img  src=\"images/etl/console.png\" class=\"visionETLIcons\" title=\"Console\" style=\"width:15px;height: 15px;cursor:pointer;\" /></span></button>"
					+ "<button class='btn btn-default' onclick='debugProcessJob(0)'><span class='ImageIcon'><img  src=\"images/etl/debug.png\" class=\"visionETLIcons\" title=\"Console\" style=\"width:15px;height: 15px;cursor:pointer;\" /></span></button>"
					+ "</div>\n" + "<div id=\"availableMapTools\" class=\"availableETLToolsDiv\">\n"
					+ "<div id=\"showComponents\" class=\"showComponents\" onclick=\"showEtlComponents();\" title=\"Palette\">\n"
					+ "<div class='etlComponentsPallete etlComponentsLeftArraow'><img src=\"images/palette.png\" class=\"visionETLIcons etlComponentsPalleteIcon\"style=\"width:18px;height: 18px;cursor:pointer;\"/></div>\n"
					+ "</div>\n"
					+ "<div id=\"scriptsExecuteIcon\" class=\"showComponents\" style=\"display: none\" data-type=\"SCRIPTS\" title=\"Execute Scripts\">\n"
					+ "<img id=\"scriptsExecute\" src=\"images/Script_execution_icon-06.svg\" class=\"visionETLIcons\"style=\"width:18px;height: 18px;cursor:pointer;\"/>\n"
					+ "</div>\n" + "<div id=\"etlIconGroup\" class=\"etlIconGroup\" >\n" + "</div>\n" + "</div>\n"
					+ "</div>\n" + "</div>\n" + "</div>\n" + "<div class=\"jqx-hideborder\" id=\"feedHeader\">\n"
					+ "</div>\n" + "</div>\n" + "<div id=\"feedContentArea\">\n" + "<div id=\"transForamationDiv\"> \n"
					+ "</div>\n" + "</div>\n" + "</div>\n"
					+ "<div id=\"dataViewDiv\" class=\"visionETLDesignTabContent\" style=\"display:none;\">\n"
					+ "</div>\n"
					+ "<div id=\"editorViewDiv\" class=\"visionETLDesignTabContent\" style=\"display:none;\">\n"
					+ "<ul>\n" + "<li title=\"Current_V10.Editor-1\">Current_V10.Editor-1</li>\n" + "</ul>\n"
					+ "<div id=\"Current_V10_editor_1_splitter\">\n"

					+ "<div id=\"Current_V10_editor_Main_1\" class=\"sqlEditorMainClass\" >\n"

					+ "<div id=\"sqlIconsdiv\" class=\"sqlIconsdivClass\">\n"
					+ "<img src=\"images/Refresh Icon.svg\" id=\"refreshSQLEditor\" class=\"visionETLIcons\" title=\"Refresh\" style=\"width:15px;height: 15px;cursor:pointer;\"\n"
					+ "'/>\n"
					+ " <img id=\"scriptsExecute\"  onclick=executeEditorScripts(\"editorViewDiv\");  src=\"images/Script_execution_icon-06.svg\" class=\"visionETLIcons\"  style=\"width:18px;height: 18px;cursor:pointer;\"/>"
					+ "\n"
					+ " <img id='scriptsCommit' title='Commit' onclick=executeCommitOrRollback('editorViewDiv','COMMIT')  src='images/update_icon.svg' class='visionETLIcons'  style='width:18px;height: 18px;cursor:pointer;'/>"
					+ " <img id='scriptsRollback' title='Rollback' onclick=executeCommitOrRollback('editorViewDiv','ROLLBACK')  src='images/etl/back.png' class='visionETLIcons'  style='width:18px;height: 18px;cursor:pointer;'/>"

					+ "</div>\n"

					+ "<div id=\"Current_V10_editor_1\" class='etlSQLEditior' >\n"

					+ "</div>\n" + "</div>\n" + "<div id=\"Current_V10_editor_1_GRID_DIV\">\n"
					+ "<div id=\"Current_V10_editor_1_GRID\">\n" + "</div>\n" + "</div>\n" + "</div>\n" + "</div>\n";
			connectionDivStr += "<div id=\"jobSchedulingViewDiv\" class=\"visionETLDesignTabContent\" style=\"display:none;\">\n"
					+ "<div id=\"jobSchedulingViewSplitter\">\n" + "<div id=\"scheduledJobs\" ></div>\n"
					+ "<div id=\"scheduledJobsDetails\">\n" + "</div>\n" + "</div>\n" + "</div>\n"
					+ "<div id=\"designViewTabHeading\" class=\"visionETLDesignTabHeadingsDiv\">\n"
					+ "<ul class=\"visionETLDesignTabHeadings\">\n"
					+ "<li title=\"Design View\" id=\"li_designView\" class=\"visionETLDesignTab visionETLDesignTabHighLight\" onclick=\"switchETLDesignTabs('li_designView', 'contentSplitter')\" ><span>Design View</span></li>\n"
					+ "<li title=\"View Content\" id=\"li_contentView\" class=\"visionETLDesignTab\"onclick=\"switchETLDesignTabs('li_contentView', 'dataViewDiv')\"><span>Content View</span></li> \n";
			if (subscriptionType != null && ("PROFESSIONAL".equalsIgnoreCase(subscriptionType)
					|| "ENTERPRISE".equalsIgnoreCase(subscriptionType))) {
				connectionDivStr += "<li title=\"SQL Editor\" id=\"li_SQLEditor\" class=\"visionETLDesignTab\"onclick=\"switchETLDesignTabs('li_SQLEditor', 'editorViewDiv')\"><span>SQL Editor</span></li> \n"
						+ "<li title=\"Scheduled Jobs\" id=\"li_JobScheduling\" class=\"visionETLDesignTab\"onclick=\"switchETLDesignTabs('li_JobScheduling', 'jobSchedulingViewDiv')\"><span>Scheduled Jobs</span></li> \n";
			}
			connectionDivStr += "</ul>\n" + "</div>\n" + "</div>\n" + "</div>\n" + "</div>\n"
					+ "<div id=\"connectionHiddenFields\">\n" + "</div>\n" + "<div id=\"dddw\"></div> \n"
					+ "<div id=\"logoutDailog\"></div>\n" + "<div id=\"columnMappingDialog\"></div>\n"
					+ "<div id=\"columnSQLMappingDialog\"></div>\n" + "<div id=\"columnMappingFormDialog\"></div>\n"
					+ "<div id=\"dialogLogFile\"></div>\n" + "<div id=\"dataTypeDialog\"></div>\n"
					+ "<div id=\"columnMappingFuncDialog\"></div>\n"
					+ "<div id=\"columnMappingFuncFormDialog\"></div>\n" + "<div id=\"parallelJobsDiv\"></div>\n"
					+ "<div id=\"joinTableColumnTr\" style=\"display: none\"></div> \n"
					+ "<div id=\"dialogLapsTimeProcess\" style=\"display: none\" >\n"
					+ "<div class= 'visionLapsFromTo'><span class='visionLapsFrom'>From Date:</span><inputid='form-date-picker' readonly=\"true\">\n"
					+ "<span class='visionLapsTo'>To Date:</span><input readonly=\"true\" id='to-date-picker'>\n"
					+ "<div id=\"errorLapsTimeProcess\"style=\"color: red\"></div>\n" + "</div>\n" + "</div>\n"
					+ "<div id=\"reqDescrDialog\"></div> \n"
					+ "<div id=\"dialogsucess\" class=\"visionGenericTabSuccess\"></div>\n"
					+ "<div id=\"dialog1\" ></div>\n" + "<div id=\"etldialog\" ></div>\n"
					+ "<div id=\"etldialog1\" ></div>\n" + "<div id=\"etldialog2\" ></div>\n"
					+ "<div id=\"dialog\" ></div>\n" + "<div id=\"imgdialog\" ></div>\n"
					+ "<div id=\"dialog2\" class=\"Rejection_dialog visionGenericTabReject\"></div>\n" + // "<div
																											// id=\"result\"></div>
																											// \n" +
					"<div id=\"jqxMenu\" style=\"display:none;\">\n" + "<ul><li>Show in Tab</li></ul>\n" + "</div>\n"
					+ "<input type=\"hidden\" id=\"fromConnObj\" value=\"\"/>\n"
					+ "<input type=\"hidden\" id=\"toConnObj\" value=\"\"/>\n"
					+ "<input type=\"hidden\" id=\"fromTable\" value=\"\"/>\n"
					+ "<input type=\"hidden\" id=\"toTable\" value=\"\"/>\n"
					+ "<input type=\"hidden\" id=\"treePageSize\" value=\"\"/>\n"
					+ "<input type=\"hidden\" id=\"functionNameObj\" value=\"\"/>\n"
					+ "<input type=\"hidden\" id=\"currentTrnsOpId\" value=\"\"/>\n"
					+ "<input type=\"hidden\" id=\"emptyJobName\" value=\"\"/> \n"
					+ "<input type=\"hidden\" id=\"emptyJobId\" value=\"\"/>	\n"
					+ "<input type=\"hidden\" id=\"currentJobId\" value=\"\"/> \n"
					+ "<input type=\"hidden\" id=\"tableName\" value=\"\"/> \n"
					+ "<input type=\"hidden\" id=\"gridIdStr\" value=\"\"/>\n"
					+ "<input type=\"hidden\" id=\"filePathValue\" value=\"\"/>\n"
					+ "<input type=\"hidden\" id=\"currentJobName\" value=\"\"/> \n"
					+ "<input type=\"hidden\" id=\"folderNameHidden\" value=\"\"/> \n"
					+ "<input type=\"hidden\" id=\"folderIdHidden\" value=\"\"/> \n"
					+ "<input type=\"hidden\" id=\"currentGroupJobName\" value=\"\"/>\n"
					+ "<input type=\"hidden\" id=\"toConnObjPK\" value=\"\"/>\n"
					+ "<input type=\"hidden\" id=\"formJobFlag\" name=\"formJobFlag\" value=\"\"/>\n"
					+ "<input type=\"hidden\" id=\"currentGroupJobId\" name=\"currentGroupJobId\" value=\"\" />\n"
					+ "<input type=\"hidden\" id=\"currentSelectedGroupJobName\" name=\"currentSelectedGroupJobName\" value=\"\" />\n"
					+ "<input type=\"hidden\" id=\"alterTablePKList\"value=\"\" />";

			dataPipingObj.put("connectionDivsStr", connectionDivStr);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return dataPipingObj;
	}

	@RequestMapping(value = "/getTreeDataPiping", method = { RequestMethod.POST,
			RequestMethod.GET }, produces = "text/plain;charset=UTF-8")
	public @ResponseBody String getTreeDataOpt(HttpServletRequest request, ModelMap map) {
		JSONObject treeObj = new JSONObject();
		try {
			String treeId = request.getParameter("treeId");
			treeObj = dataPipingService.getGenericTreeOpt(request, treeId);
			String dragEndFunction = "function (item) {\n"
					+ "                                        createImageDestinationFlowchart(item,item.label,'Table');\n"
					+ "//                                        $('#feedHeader').append(item.label);\n"
					+ "                                    }";
			treeObj.put("dragEndFunction", dragEndFunction);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return treeObj.toJSONString();
	}

	@RequestMapping(value = "/getTreeDataMigrationMenus", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody String getDataMigrationMenus(HttpServletRequest request) {

		JSONObject resultObj = new JSONObject();
		try {
			String menuResult = "";
			String menuId = request.getParameter("menuId");
			JSONArray treeMenuArray = dataPipingService.getTreeMenu(request, menuId);
			if (treeMenuArray != null && !treeMenuArray.isEmpty()) {
				menuResult = treeMenuArray.toJSONString();
			}
			resultObj.put("menuResult", menuResult);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj.toJSONString();
	}

	@RequestMapping(value = "/getTreeUploadedFiles", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody String getTreeUploadedFiles(HttpServletRequest request) {

		String result = "";
		try {
			String treeId = request.getParameter("treeId");
			result = dataPipingService.getTreeUploadedFiles(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	@RequestMapping(value = "/getTreeParentFile", method = { RequestMethod.POST,
			RequestMethod.GET }, produces = "text/plain;charset=UTF-8")
	public @ResponseBody String getTreeParentFile(HttpServletRequest request, ModelMap map) {
		JSONObject treeObj = new JSONObject();
		try {
			String dragEndFunc = "";
			String treeId = request.getParameter("treeId");
			String divId = request.getParameter("divId");
			treeObj = dataPipingService.getGenericTreeOpt(request, treeId);
			if (divId != null && !"".equalsIgnoreCase(divId) && "uploadDestFiles".equalsIgnoreCase(divId)) {
				dragEndFunc = "function (item) {\n"
						+ "                                        createImageDestinationFlowchart(item,item.label,'FileData');\n"
						+ "//                                        $('#feedHeader').append(item.label);\n"
						+ "                                    }";
			} else if (divId != null && !"".equalsIgnoreCase(divId) && "uploadSourceFiles".equalsIgnoreCase(divId)) {
				dragEndFunc = "function (item) {\n"
						+ "                                      createImageSourcesFlowchart(item,item.label,'FileData');\n"
						+ "//                                        $('#feedHeader').append(item.label);\n"
						+ "                                    }";
			}
			treeObj.put("dragEnd", dragEndFunc);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return treeObj.toJSONString();
	}

	@RequestMapping(value = "/importTreeDMFileETL", produces = "text/plain;charset=UTF-8")
	public @ResponseBody String importDMFile(HttpServletRequest request, HttpServletResponse response,
			@RequestParam("selectedFiletype") String selectedFiletype,
			@RequestParam("fileLocalPath") String fileLocalPath, @RequestParam("importTreeDMFile") MultipartFile file) {

		System.out.println("Entered Export Controller...");
		String result = "";
		try {

			result = dataPipingService.importTreeDMFile(request, response, file, selectedFiletype);

		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("result::::" + result);
		return result;
	}

	@RequestMapping(value = "/importFileDataDirectly", produces = "text/plain;charset=UTF-8")
	public @ResponseBody String importFileDataDirectly(HttpServletRequest request, HttpServletResponse response,
			@RequestParam("selectedFiletype") String selectedFiletype,
			@RequestParam("fileLocalPath") String fileLocalPath, @RequestParam("importTreeDMFile") MultipartFile file,
			@RequestParam("connObj") String connObj, @RequestParam("tableName") String tableName,
			@RequestParam("selectedSheet") String selectedSheet) {

		System.out.println("Entered Export Controller...");
		String result = "";
		try {
			JSONObject connectionObj = (JSONObject) JSONValue.parse(connObj);

			result = dataPipingService.importFileDataDirectly(request, response, file, selectedFiletype, connectionObj,
					tableName, selectedSheet);

		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("result::::" + result);
		return result;
	}

	@RequestMapping(value = "/importFileDataToTableDirectly", method = { RequestMethod.POST, RequestMethod.GET })
	public @ResponseBody String importFileDataToTableDirectly(HttpServletRequest request,
			HttpServletResponse response) {
		String result = "";
		try {

			String tableName = request.getParameter("tableName");
			String filePath = request.getParameter("filePath");
			result = dataPipingService.importFileDataToTableDirectly(request, response, filePath, tableName);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	@RequestMapping(value = "/importTreeDMFileXlsx", produces = "text/plain;charset=UTF-8")
	public @ResponseBody String importDMFileXlsx(HttpServletRequest request, HttpServletResponse response) {

		System.out.println("Entered Export Controller...");
		String result = "";
		try {
			String selectedFiletype = request.getParameter("selectedFiletype");
			String jsonDataStr = request.getParameter("jsonData");
			JSONObject jsonData = (JSONObject) JSONValue.parse(jsonDataStr);
			result = dataPipingService.importTreeDMFileXlsx(request, response, jsonData, selectedFiletype);

		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("result::::" + result);
		return result;
	}

	@RequestMapping(value = "/fetchTreeDMTableColumns", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject fetchTreeDMTableColumns(HttpServletRequest request, HttpServletResponse response) {

		JSONObject columnsObject = new JSONObject();
		try {
			columnsObject = dataPipingService.fetchTransformationRules(request, response);
//            columnsObject = dataPipingService.fetchTreeDMTableColumns(request);
		} catch (Exception e) {
		}
		return columnsObject;
	}

	@RequestMapping(value = "/fetchTransformationRules", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject fetchTransformationRules(HttpServletRequest request, HttpServletResponse response) {

		JSONObject columnsObject = new JSONObject();
		try {
			columnsObject = dataPipingService.fetchTransformationRules(request, response);
//            columnsObject = dataPipingService.fetchTreeDMTableColumns(request);
		} catch (Exception e) {
		}
		return columnsObject;
	}

	@RequestMapping(value = "/fetchTreeDMTableFileColumns", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject fetchTreeDMTableFileColumns(HttpServletRequest request,
			HttpServletResponse response) {

		JSONObject columnsObject = new JSONObject();
		try {
			columnsObject = dataPipingService.fetchTreeDMTableFileColumns(request, response);
		} catch (Exception e) {
		}
		return columnsObject;
	}

	@RequestMapping(value = "/mappingTreeDMSourceColsWithDestCols", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody String mappingSourceColsWithDestCols(HttpServletRequest request) {

		JSONObject columnsObject = new JSONObject();
		try {
			columnsObject = dataPipingService.mappingSourceColsWithDestCols(request);
		} catch (Exception e) {
		}
		return columnsObject.toJSONString();
	}

	@RequestMapping(value = "/exportDMXlsxData")
	public void exportXlsData(HttpServletRequest request, HttpServletResponse response) {
		String result = "";
		try {

			dataPipingService.exportXlsx(request, response);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@RequestMapping(value = "/exportDMCSVData")
	public void exportCSVData(HttpServletRequest request, HttpServletResponse response) {
		String result = "";
		try {

			dataPipingService.exportCSVData(request, response);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@RequestMapping(value = "/exportDMXMLData")
	public void exportXMLData(HttpServletRequest request, HttpServletResponse response) {
		String result = "";
		try {

			dataPipingService.exportXMLData(request, response);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@RequestMapping(value = "/mappingTreeDMFileColsWithDestCols", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody String mappingTreeDMFileColsWithDestCols(HttpServletRequest request,
			HttpServletResponse response) {

		String result = "";
		try {
			result = dataPipingService.mappingSourceFileColsWithDestCols(request, response);
		} catch (Exception e) {
		}
		return result;
	}

	@RequestMapping(value = "/getJoinTableRows", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody String getJoinTableRows(HttpServletRequest request, HttpServletResponse response) {

		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dataPipingService.getJoinTableRows(request);
		} catch (Exception e) {
		}
		return resultObj.toJSONString();
	}

	@RequestMapping(value = "/fetchJoinTablesData1", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject fetchJoinTablesData(HttpServletRequest request) {

		JSONObject joinTablesData = new JSONObject();
		try {
			joinTablesData = dataPipingService.fetchJoinTableColumnTrfnRules(request);
//            joinTablesData = dataPipingService.fetchJoinTablesData(request);
		} catch (Exception e) {
		}
		return joinTablesData;
	}

//    @RequestMapping(value = "/processETLData", method = {RequestMethod.GET, RequestMethod.POST})
//    public @ResponseBody
//    JSONObject processETLData(HttpServletRequest request) {
//
//        JSONObject joinTablesData = new JSONObject();
//        try {
//            joinTablesData = dataTransformationService.processJobData(request);
////            joinTablesData = dataPipingService.processETLData(request);
//        } catch (Exception e) {
//        }
//        return joinTablesData;
//    }
	@RequestMapping(value = "/showingClauseColumns1", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody String showingClauseColumns(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {

			String clauseCondStr = request.getParameter("whereClauseStr");
			JSONObject whereClauseObj = new JSONObject();
			if (clauseCondStr != null && !"".equalsIgnoreCase(clauseCondStr)) {
				whereClauseObj = (JSONObject) JSONValue.parse(clauseCondStr);
			}
			String whereClauseColsCondition = "<div class=\"visionEtlJoinClauseMain\">"
					+ "<div class=\"visionEtlAddIconDiv\">"
					+ "<img data-trstring='' src=\"images/add.png\" id=\"visionEtlAddRowIcon\" "
					+ "class=\"visionEtlAddRowIcon\" title=\"Add new where clause\""
					+ " onclick=addNewClauseRow(event,id,this) "
					+ "style=\"width:18px;height: 18px;cursor:pointer; float: left;\"/>"
					+ "<img data-trstring='' src=\"images/save_icon2.png\" id=\"visionEtlSaveIcon\" "
					+ "class=\"visionEtlAddRowIcon\" title=\"Save Mapping\""
					+ " onclick=saveClauseMapping(event,id,this) "
					+ "style=\"width:18px;height: 18px;cursor:pointer; float: left;\"/>" + "</div>"
					+ "<div class=\"visionEtlJoinClauseTablesDiv visionEtlJoinClauseTablesDivScroll\">"
					+ "<table id=\"fromTablesWhereCauseTable\""
					+ " class=\"visionEtlJoinClauseTable\" style=\"width: 100%;\" border=\"1\">" + "<thead>" + "<tr>"
					+ "<th width='5%' class=\"\" "
					+ "style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\"></th>"
					+ "<th width='35%' class=\"\" "
					+ "style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">"
					+ "Column Name" + "</th>" + "<th width='20%' class=\"\""
					+ " style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">"
					+ "Opeartor" + "</th>" + "<th width='30%' class=\"\" "
					+ "style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">" + "Value"
					+ "</th>" + "<th width='10%' class=\"\" "
					+ "style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">" + "AND/OR"
					+ "</th>" + "</tr>" + "</thead>" + "<tbody>";

			// ravi start
			String trString = "";
			String singletrString = "<tr>" + "<td width='5%'>" + "<img src=\"images/Delete_Red_Icon.svg\" "
					+ "onclick=\"deleteSelectedRow(this)\" class=\"visionColMappingImg\" "
					+ "title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"></td>"
					+ "<td width='35%'  class=\"sourceJoinColsTd\">"
					+ "<input class=\"visionColJoinMappingInput visionColFuncInput\" type=\"text\" value=\"\" readonly=\"true\">"
					+ "<img src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \""
					+ " onclick=\"selectColumn(this,'fromWhereClauseColumn')\" style=\"margin-left: 2px;\">"
					+ "<img title='Select Function' src=\"images/Fx icon-01.svg\" class=\"visionETLColMapImage \" onclick=\"selectColumnFun(this,'fromWhereClauseColumn')\" style=\"margin-left: 2px;\">"
					+ "</td>" + "<td width='20%' class=\"sourceJoinColsTd\">"
					+ "<select id=\"OPERATOR_TYPE\"  class=\"sourceColsJoinSelectBox\" onchange=\"disableDdw(this,id)\">"
					+ "<option  value='='>=</option>" + "<option  value='!='>!=</option>"
					+ "<option  value='LIKE'>LIKE</option>" + "<option  value='NOT LIKE'>NOT LIKE</option>"
					+ "<option  value='IN'>IN</option>" + "<option  value='NOT IN'>NOT IN</option>"
					+ "<option  value='<'><</option>" + "<option  value='>'>></option>"
					+ "<option  value='<='><=</option>" + "<option  value='>='>>=</option>"
					+ "<option  value='IS'>IS</option>" + "<option  value='IS NOT'>IS NOT</option>" + "</select>"
					+ "</td>" + "<td width='30%' class=\"sourceJoinColsTd\">"
					+ "<input class='visionColMappingTextarea' type=\"text\" value=\"\">" + "</td>"
					+ "<td width='10%' class=\"sourceJoinColsTd\">" + "<select id='andOrOpt'>"
					+ "<option value='AND'>AND</option>" + "<option value='OR'>OR</option>" + "</select>" + "</td>"
					+ "</tr>";
			String trfmRulesId = request.getParameter("trfmRulesId");
			String trfmRulesChanged = request.getParameter("trfmRulesChanged");

			trString += singletrString;
			// ravi end
			String whereClauseTrString = "";
			if (whereClauseObj != null && !whereClauseObj.isEmpty()) {
				int row = 1;
				for (Object keyName : whereClauseObj.keySet()) {
					JSONObject dataClauseObj = (JSONObject) whereClauseObj.get(keyName);
					if (dataClauseObj != null && !dataClauseObj.isEmpty()) {
						String operator = (String) dataClauseObj.get("operator");
						whereClauseTrString += "<tr>" + "<td width='5%'>" + "<img src=\"images/Delete_Red_Icon.svg\" "
								+ "onclick=\"deleteSelectedRow(this)\" class=\"visionColMappingImg\" "
								+ "title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"></td>"
								+ "<td width='35%'  class=\"sourceJoinColsTd\">"
								+ "<input class=\"visionColJoinMappingInput visionColFuncInput\" type=\"text\""
								+ " value=\"" + dataClauseObj.get("columnName") + "\"  " + "data-funobjstr='"
								+ String.valueOf(dataClauseObj.get("datafunobjstr")).replaceAll("'", "&#39;") + "'"
								+ " readonly=\"true\">"
								+ "<img src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \""
								+ " onclick=\"selectColumn(this,'fromWhereClauseColumn')\" style=\"\">"
								// + "</td>"

								+ "<img title='Select Function' src=\"images/Fx icon-01.svg\" class=\"visionETLColMapImage \" onclick=\"selectColumnFun(this,'fromWhereClauseColumn')\" style=\"margin-left: 2px;\">"
								+ "</td>" + "<td width='20%' class=\"sourceJoinColsTd\">"
								+ "<select id=\"OPERATOR_TYPE\"  class=\"sourceColsJoinSelectBox\" onchange=\"disableDdw(this,id)\">"
								+ "<option  value='=' " + ("=".equalsIgnoreCase(operator) ? "selected" : "")
								+ ">=</option>" + "<option  value='!=' "
								+ ("!=".equalsIgnoreCase(operator) ? "selected" : "") + ">!=</option>"
								+ "<option  value='LIKE' " + ("LIKE".equalsIgnoreCase(operator) ? "selected" : "")
								+ ">LIKE</option>" + "<option  value='NOT LIKE' "
								+ ("NOT LIKE".equalsIgnoreCase(operator) ? "selected" : "") + ">NOT LIKE</option>"
								+ "<option  value='IN' " + ("IN".equalsIgnoreCase(operator) ? "selected" : "")
								+ ">IN</option>" + "<option  value='NOT IN' "
								+ ("NOT IN".equalsIgnoreCase(operator) ? "selected" : "") + ">NOT IN</option>"
								+ "<option  value='<' " + ("<".equalsIgnoreCase(operator) ? "selected" : "")
								+ "><</option>" + "<option  value='>' "
								+ (">".equalsIgnoreCase(operator) ? "selected" : "") + ">></option>"
								+ "<option  value='<=' " + ("<=".equalsIgnoreCase(operator) ? "selected" : "")
								+ "><=</option>" + "<option  value='>=' "
								+ (">=".equalsIgnoreCase(operator) ? "selected" : "") + ">>=</option>"
								+ "<option  value='IS' " + ("IS".equalsIgnoreCase(operator) ? "selected" : "")
								+ ">IS</option>" + "<option  value='IS NOT' "
								+ ("IS NOT".equalsIgnoreCase(operator) ? "selected" : "") + ">IS NOT</option>"
								+ "</select>" + "</td>" + "<td width='30%' class=\"sourceJoinColsTd\">"
								+ "<input class='visionColMappingTextarea' type=\"text\" value=\""
								+ String.valueOf(dataClauseObj.get("staticValue")).replaceAll("'", "&#39;") + "\">"
								+ "</td>" + "<td width='10%' class=\"sourceJoinColsTd\">" + "<select id='andOrOpt'>"
								+ "<option value='AND' " + ("AND".equalsIgnoreCase(operator) ? "selected" : "")
								+ ">AND</option>" + "<option value='OR' "
								+ ("OR".equalsIgnoreCase(operator) ? "selected" : "") + ">OR</option>" + "</select>"
								+ "</td>" + "</tr>";
					}
					row++;
				}
			}
			if (!(whereClauseTrString != null && !"".equalsIgnoreCase(whereClauseTrString))) {
				whereClauseTrString = trString;
			}
			whereClauseColsCondition = whereClauseColsCondition + whereClauseTrString + "</tbody></table></div>";
			resultObj.put("whereClauseColsCondition", whereClauseColsCondition);
			resultObj.put("trString", singletrString); // ravi start
		} catch (Exception e) {
		}
		return resultObj.toJSONString();
	}

	@RequestMapping(value = "/downloadExportedFile", method = { RequestMethod.GET, RequestMethod.POST })
	public void downloadExportedFile(HttpServletRequest request, HttpServletResponse response) {
		response.reset();

		try {
			String fileName = request.getParameter("fileName");
			String orginalName = request.getParameter("orginalName");
			String filePath = etlFilePath + "ETL_EXPORT_" + File.separator
					+ request.getSession(false).getAttribute("ssUsername") + File.separator + fileName;
			File outputFile = new File(filePath);
			ServletContext ctx = request.getSession().getServletContext();
			InputStream fis = new FileInputStream(outputFile);
			String mimeType = ctx.getMimeType(outputFile.getAbsolutePath());
			response.setContentType(mimeType != null ? mimeType : "application/octet-stream");
			if (!(orginalName != null && !"".equalsIgnoreCase(orginalName) && !"null".equalsIgnoreCase(orginalName))) {
				orginalName = fileName;
			}
			response.setHeader("Content-Disposition", "attachment; filename=\"" + orginalName.trim() + "\"");

			ServletOutputStream os = response.getOutputStream();
			byte[] bufferData = new byte[4096];
			int read = 0;
			while ((read = fis.read(bufferData)) != -1) {
				os.write(bufferData, 0, read);
			}
			fis.close();
			os.flush();
			os.close();
		} catch (Exception e) {
		}
	}

	@RequestMapping(value = "/downloadExportedFileAjax", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject downloadExportedFileAjax(HttpServletRequest request, HttpServletResponse response) {
		JSONObject resultObj = new JSONObject();
		try {
			String fileName = request.getParameter("fileName");
			String orginalName = request.getParameter("orginalName");
			String filePath = etlFilePath + "ETL_EXPORT_" + File.separator
					+ request.getSession(false).getAttribute("ssUsername") + File.separator + fileName;
			File outputFile = new File(filePath);
			ServletContext ctx = request.getSession().getServletContext();
			InputStream fis = new FileInputStream(outputFile);
			String mimeType = ctx.getMimeType(outputFile.getAbsolutePath());
			response.setContentType(mimeType != null ? mimeType : "application/octet-stream");
			if (!(orginalName != null && !"".equalsIgnoreCase(orginalName) && !"null".equalsIgnoreCase(orginalName))) {
				orginalName = fileName;
			}
			response.setHeader("Content-Disposition", "attachment; filename=\"" + orginalName.trim() + "\"");

			ServletOutputStream os = response.getOutputStream();
			byte[] bufferData = new byte[4096];
			int read = 0;
			while ((read = fis.read(bufferData)) != -1) {
				os.write(bufferData, 0, read);
			}
			fis.close();
			os.flush();
			os.close();
		} catch (Exception e) {
		}
		return resultObj;
	}

	@RequestMapping(value = "/getETLDBFunction", method = { RequestMethod.GET,
			RequestMethod.POST }, produces = "text/plain;charset=UTF-8")
	public @ResponseBody String getETLDBFunction(HttpServletRequest request) {
		JSONArray allDBFunctionsArray = new JSONArray();
		try {
			allDBFunctionsArray = dataPipingService.getDBFunctions(request);
			// functionNameObj
		} catch (Exception e) {
		}
		return allDBFunctionsArray.toJSONString();
	}

	@RequestMapping(value = "/getETLFunctionForm", method = { RequestMethod.GET,
			RequestMethod.POST }, produces = "text/plain;charset=UTF-8")
	public @ResponseBody String getETLFunctionForm(HttpServletRequest request) {
		String result = "";
		try {
			result = dataPipingService.getETLFunctionForm(request);
		} catch (Exception e) {
		}
		return result;
	}

	@RequestMapping(value = "/getLookupAllTables", method = { RequestMethod.GET,
			RequestMethod.POST }, produces = "application/json;charset=UTF-8")
	public @ResponseBody JSONArray getLookupAllTables(HttpServletRequest request) {
		JSONArray dataArray = new JSONArray();
		try {
			dataArray = dataPipingService.getLookupTables(request);
		} catch (Exception e) {
		}
		return dataArray;
	}

	@RequestMapping(value = "/getSelectedLookupTableColumns", method = { RequestMethod.GET,
			RequestMethod.POST }, produces = "application/json;charset=UTF-8")
	public @ResponseBody JSONArray getSelectedLookupTableColumns(HttpServletRequest request) {
		JSONArray dataArray = new JSONArray();
		try {
			dataArray = dataPipingService.getSelectedLookupTableColumns(request);
		} catch (Exception e) {
		}
		return dataArray;
	}

	@RequestMapping(value = "/getTreeErpConnectionDetails", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONArray getTreeErpConnectionDetails(HttpServletRequest request) {

		JSONArray treeDataObjArray = new JSONArray();

		try {

			treeDataObjArray = dataPipingService.getTreeErpConnectionDetails(request);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return treeDataObjArray;
	}

	@RequestMapping(value = "/getFromTableColumns", method = { RequestMethod.GET,
			RequestMethod.POST }, produces = "application/json;charset=UTF-8")
	public @ResponseBody JSONObject getFromTableColumns(HttpServletRequest request, HttpServletResponse response) {

		JSONObject fromTableColsObj = new JSONObject();
		try {
			JSONArray fromTableColsArray = dataPipingService.getFromTableColumns(request, response);
			String createTableObjStr = request.getParameter("createTableObj");
			JSONObject createTableObj = new JSONObject();
			JSONObject connObj = new JSONObject();
			JSONObject selectedSQLOperator = new JSONObject();
			if (createTableObjStr != null && !"".equalsIgnoreCase(createTableObjStr)
					&& !"null".equalsIgnoreCase(createTableObjStr)) {
				createTableObj = (JSONObject) JSONValue.parse(createTableObjStr);
			}
			if (createTableObj != null && !createTableObj.isEmpty()) {
				String connObjStr = (String) createTableObj.get("dataSourceObj");
				if (connObjStr != null && !"".equalsIgnoreCase(connObjStr) && !"null".equalsIgnoreCase(connObjStr)) {
					connObj = (JSONObject) JSONValue.parse(connObjStr);
				}
			}
			if (!(connObj != null && !connObj.isEmpty())) {
				connObj = new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName, password,
						"Current_V10");
			}
			String sqlPopupDiv = "<div id='sqlPopupDivId' class='sqlPopupDivClass'>"
					+ "<div id='sqlFromTableTreeDiv' class='sqlFromTableTreeDivClass'>"
					+ "<div id='columnSQLMappingTree' class='columnMappingTree'></div>" + "" + "</div>"
					+ "<div id='sqlMoveButtonsDiv' class='sqlMoveButtonsDivClass'>"
					+ "<div class='sqlMoveButtonsClass'><b><input onclick=moveTableColumns('columnSQLMappingTree','selected') type=\"button\" value=\">\" class=\"sqlMoveButtons\"></b>"
					// + "<b><input type=\"button\"
					// onclick=moveTableColumns('columnSQLMappingTree','all') value=\">>\"
					// class=\"sqlMoveButtons\"></b>"
					+ "</div>" + "</div>" + "<div id='sqlToTableDiv' class='sqlToTableDivClass'>"
					+ "<div id='sqlToTableIconsDiv'>"
					+ "<img data-trstring='' src=\"images/Add icon.svg\" id=\"visionEtlAddRowIcon\" "
					+ "class=\"visionEtlAddRowIcon\" title=\"Add new Column\""
					+ " onclick=addNewTableRow('sqlToTableTrDiv','sqlToTableColumnsTable',this) "
					+ "style=\"width:15px;height: 15px;cursor:pointer; float: left;\"/>"
					+ "<img data-trstring='' src=\"images/Delete_Red_Icon.svg\" id=\"visionEtlAddRowIcon\" "
					+ "class=\"visionEtlAddRowIcon\" title=\"Delete\"" + " onclick=deleteAllColumns() "
					+ "style=\"width:15px;height: 15px;cursor:pointer; float: left;\"/>" + "</div>"
					+ "<div id='sqlToTableNameDiv'>"
					+ "<table class=\"visionEtlCreateSQLDS\" id='sqlToTableColumnsTableName' style='width: 100%;' border='1'>"
					+ "<tr>" + "<td>Data Source:</td>" + "<td>"
					+ "<input id=\"sqlToDataSourceName\" class=\"visionDSMappingInput\" type=\"text\"" + " value=\""
					+ ((createTableObj != null && !createTableObj.isEmpty()
							&& createTableObj.get("dataSourceName") != null
							&& !"".equalsIgnoreCase(String.valueOf(createTableObj.get("dataSourceName")))
							&& !"null".equalsIgnoreCase(String.valueOf(createTableObj.get("dataSourceName"))))
									? String.valueOf(createTableObj.get("dataSourceName"))
									: "Current V10")
					+ "\""
					// + " data-conobjstr='" + ((createTableObj != null &&
					// !createTableObj.isEmpty()) ? createTableObj.get("dataSourceObj") : "") + "'"
					+ " data-conobjstr='"
					+ ((createTableObj != null && !createTableObj.isEmpty()) ? createTableObj.get("dataSourceObj")
							: String.valueOf(connObj))
					+ "'" + " data-tosystype='"
					+ ((connObj != null && !connObj.isEmpty())
							? String.valueOf(connObj.get("CONN_CUST_COL1")).toUpperCase()
							: "")
					+ "' " + " readonly=\"true\">"// dataSourceObj
					+ "<img src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \""
					+ " onclick=\"selectFunColumnValue(this,'ALL_SCHEMA','Data Source')\" style=\"\">" + "</td>"
					+ "<td>Table Name:</td>" + "<td>" + "<input class='' type='text' id='sqlToTableName' value='"
					+ ((createTableObj != null && !createTableObj.isEmpty() && createTableObj.get("tableName") != null
							&& !"".equalsIgnoreCase(String.valueOf(createTableObj.get("tableName")))
							&& !"null".equalsIgnoreCase(String.valueOf(createTableObj.get("tableName"))))
									? String.valueOf(createTableObj.get("tableName")).toUpperCase()
									: "")
					+ "'/>" + "</td>" + "</table>" + "</div>"
					+ "<div id='sqlToTableColumnsDiv' class='sqlToTableColumnsDivClass'>"
					+ "<table class=\"visionEtlCreateSQLTable\" id='sqlToTableColumnsTable' style='width: 100%;' border='1'>"
					+ "<thead>" + "<tr>"
					+ "<th width='5%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'></th>"
					+ "<th width='5%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>"
					+ "<img src=\"images/lock.png\" title=\"Primary Key\" style=\"width:14px;height: 14px;margin-top: 3px;\"/></th>"
					+ "<th width='20%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Column Name</th>"
					+ "<th width='20%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Data Type</th>"
					// PKH default value
					+ "<th width='5%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>NOT NULL</th>"
					+ "<th width='35%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Default Value</th>"
					// PKH default value

					+ "</tr>" + "</thead>" + "<tbody>";
			if (createTableObj != null && !createTableObj.isEmpty()) {
				JSONArray columnsArray = (JSONArray) createTableObj.get("colsObj");
				if (columnsArray != null && !columnsArray.isEmpty()) {
					for (int i = 0; i < columnsArray.size(); i++) {
						JSONObject columnsObj = (JSONObject) columnsArray.get(i);
						if (columnsObj != null && !columnsObj.isEmpty()) {
							sqlPopupDiv += "<tr>"
									+ "<td width='5%'><img src=\"images/Delete_Red_Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionTdETLIcons\""
									+ " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>" + "</td>"
									+ "<td width='5%' class=\"sourceJoinColsTd\">"
									+ "<input class='visionColJoinMappingInput' type='checkbox' "
									+ ("Y".equalsIgnoreCase(String.valueOf(columnsObj.get("PK"))) ? "checked" : "")
									+ " />" + "</td>" + "<td width='35%' class=\"sourceJoinColsTd editable\">"
									+ "<input class='visionColJoinMappingInput inputvalue' type='text'" + " value='"
									+ ((columnsObj.get("COLUMN_NAME") != null
											&& !"".equalsIgnoreCase(String.valueOf(columnsObj.get("COLUMN_NAME")))
											&& !"".equalsIgnoreCase(String.valueOf(columnsObj.get("COLUMN_NAME"))))
													? columnsObj.get("COLUMN_NAME")
													: "")
									+ "' style='display:none;'/>" + "<span class=\"originalvalue\">"
									+ ((columnsObj.get("COLUMN_NAME") != null
											&& !"".equalsIgnoreCase(String.valueOf(columnsObj.get("COLUMN_NAME")))
											&& !"".equalsIgnoreCase(String.valueOf(columnsObj.get("COLUMN_NAME"))))
													? columnsObj.get("COLUMN_NAME")
													: "")
									+ "</span>" + "</td>" + "<td width='35%' class=\"sourceJoinColsTd dataTypePopup\">"
									+ "<input class='visionColJoinMappingInput inputvalue' type='text'" + " value='"
									+ ((columnsObj.get("DATA_TYPE") != null
											&& !"".equalsIgnoreCase(String.valueOf(columnsObj.get("DATA_TYPE")))
											&& !"".equalsIgnoreCase(String.valueOf(columnsObj.get("DATA_TYPE"))))
													? columnsObj.get("DATA_TYPE")
													: "")
									+ "' style='display:none;'/>" + "<span class=\"originalvalue\">"
									+ ((columnsObj.get("DATA_TYPE") != null
											&& !"".equalsIgnoreCase(String.valueOf(columnsObj.get("DATA_TYPE")))
											&& !"".equalsIgnoreCase(String.valueOf(columnsObj.get("DATA_TYPE"))))
													? columnsObj.get("DATA_TYPE")
													: "")
									+ "</span></td>"
									// PKH default value
									+ "<td width='5%' class=\"sourceJoinColsTd\">"
									+ "<input class='visionColJoinMappingInput' type='checkbox' "
									+ ("Y".equalsIgnoreCase(String.valueOf(columnsObj.get("NOTNULL"))) ? "checked" : "")
									+ " />" + "</td>" + "<td width='35%' class=\"sourceJoinColsTd editable\">"
									+ "<input class='visionColJoinMappingInput inputvalue' type='text'" + " value='"
									+ ((columnsObj.get("DEFAULT_VALUE") != null
											&& !"".equalsIgnoreCase(String.valueOf(columnsObj.get("DEFAULT_VALUE")))
											&& !"".equalsIgnoreCase(String.valueOf(columnsObj.get("DEFAULT_VALUE"))))
													? columnsObj.get("DEFAULT_VALUE")
													: "")
									+ "' style='display:none;'/>" + "<span class=\"originalvalue\">"
									+ ((columnsObj.get("DEFAULT_VALUE") != null
											&& !"".equalsIgnoreCase(String.valueOf(columnsObj.get("DEFAULT_VALUE")))
											&& !"".equalsIgnoreCase(String.valueOf(columnsObj.get("DEFAULT_VALUE"))))
													? columnsObj.get("DEFAULT_VALUE")
													: "")
									+ "</span>" + "</td>"
									// PKH default value

									+ "<td width='5%' class=\"sourceJoinColsTd\" style='display:none' >E</td>"
									+ "</tr>";
						}

					}

				}
			}
			sqlPopupDiv += "</tbody>" + "</table>" + "</div>" + "<div id='sqlToTableTrDiv' style='display:none'>"
					+ "</div>" + "<input type='hidden' id='sqlAllDataTypeObj' value=''/>" + "" + "</div>" + "</div>";
			String trString = "<tr>"
					+ "<td width='5%'><img src=\"images/Delete_Red_Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionTdETLIcons\""
					+ " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>" + "</td>"
					+ "<td width='5%' class=\"sourceJoinColsTd\"><input class='visionColJoinMappingInput' type='checkbox'/></td>"
					+ "<td width='20%' class=\"sourceJoinColsTd editable\"><input class='visionColJoinMappingInput inputvalue' type='text' value='' style='display:none;'/><span class=\"originalvalue\"></span></td>"
					+ "<td width='20%' class=\"sourceJoinColsTd dataTypePopup\"><input class='visionColJoinMappingInput inputvalue' type='text' value='' style='display:none;'/><span class=\"originalvalue\"></span></td>"
					// PKH default value
					+ "<td width='5%' class=\"sourceJoinColsTd\"><input class='visionColJoinMappingInput' type='checkbox'/></td>"
					+ "<td width='35%' class=\"editable\"><input type='text' /><span class=\"originalvalue\"></span></td>"
					// PKH default value

					+ "<td width='5%' class=\"sourceJoinColsTd\" style='display:none' >N</td>" + "</tr>";
			fromTableColsObj.put("sqlPopupDiv", sqlPopupDiv);
			fromTableColsObj.put("fromTableColsArray", fromTableColsArray);
			fromTableColsObj.put("trString", trString);
			fromTableColsObj.put("allDataTypeObj", dataPipingService.getAllDataTypesList(request));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return fromTableColsObj;
	}

	@RequestMapping(value = "/createTableInETL", method = { RequestMethod.GET,
			RequestMethod.POST }, produces = "application/json;charset=UTF-8")
	public @ResponseBody JSONObject createTableInETL(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dataPipingService.createTableInETL(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	@RequestMapping(value = "/getSchemaObjectMetaData", method = { RequestMethod.GET,
			RequestMethod.POST }, produces = "application/json")
	public @ResponseBody JSONObject getSchemaObjectMetaData(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String parentkeyData = request.getParameter("parentkey");
			System.out.println("parentkey:::" + parentkeyData);
			resultObj = dataPipingService.getSchemaObjectData(request, parentkeyData);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	@RequestMapping(value = "/getSchemaObjectData", method = { RequestMethod.GET,
			RequestMethod.POST }, produces = "application/json")
	public @ResponseBody JSONArray getSchemaObjectData(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		JSONArray dataArray = new JSONArray();
		try {
			String parentkeyData = request.getParameter("parentkey");
			System.out.println("parentkey:::" + parentkeyData);
			resultObj = dataPipingService.getSchemaObjectData(request, parentkeyData);
			dataArray = (JSONArray) resultObj.get("dataArray");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return dataArray;
	}

	@RequestMapping(value = "/refreshProcessLog", method = { RequestMethod.GET,
			RequestMethod.POST }, produces = "text/plain;charset=UTF-8")
	public @ResponseBody String refreshProcessLog(HttpServletRequest request) {
		String result = "";
		try {
			result = genericProcessETLDataService.getProcesslog(request);
		} catch (Exception e) {
		}
		return result;
	}

	@RequestMapping(value = "/refreshOperatorProcessStatus", method = { RequestMethod.GET,
			RequestMethod.POST }, produces = "text/plain;charset=UTF-8")
	public @ResponseBody String refreshOperatorProcessStatus(HttpServletRequest request) {
		String result = "";
		try {
			result = genericProcessETLDataService.refreshOperatorProcessStatus(request);
		} catch (Exception e) {
		}
		return result;
	}

	@RequestMapping(value = "/validateSQLQuery", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject validateSQLQuery(HttpServletRequest request) {

		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dataPipingService.validateSQLQuery(request);
		} catch (Exception e) {
		}
		return resultObj;
	}

	@RequestMapping(value = "/executeSQLQuery", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject executeSQLQuery(HttpServletRequest request) {

		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dataPipingService.executeSQLQuery(request);
		} catch (Exception e) {
		}
		return resultObj;
	}

	// showExecutionQueryResults
	@RequestMapping(value = "/showExecutionQueryResults", method = RequestMethod.POST)
	public @ResponseBody List showExecutionQueryResults(HttpServletRequest request) {
		List resultList = new ArrayList();

		try {
//           
			resultList = dataPipingService.showExecutionQueryResults(request);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return resultList;
	}

	@RequestMapping(value = "/copyJob", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody String copyJob(HttpServletRequest request) {
		String message = "";
		JSONObject resultObj = new JSONObject();
		try {
			message = saveJobsService.copyJob(request);
		} catch (Exception e) {
		}
		return message;
	}

	@RequestMapping(value = "/renameJob", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody String renameJob(HttpServletRequest request) {
		String message = "";
		try {
			message = saveJobsService.renameJob(request);
		} catch (Exception e) {
		}
		return message;
	}

	@RequestMapping(value = "/allColMappingForm", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject allColMappingForm(HttpServletRequest request) {

		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dataPipingService.allColMappingForm(request);
		} catch (Exception e) {
		}
		return resultObj;
	}

	@RequestMapping(value = "/mapTableColumns", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject mapTableColumns(HttpServletRequest request) {

		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dataPipingService.mapTableColumns(request);
		} catch (Exception e) {
		}
		return resultObj;
	}

//    @RequestMapping(value = "/importSAOPService", method = {RequestMethod.GET, RequestMethod.POST})
//    public @ResponseBody
//    JSONObject importSAOPService(HttpServletRequest request) {
//
//        JSONObject resultObj = new JSONObject();
//        try {
//            resultObj = dataPipingService.importSAOPService(request);
//        } catch (Exception e) {
//        }
//        return resultObj;
//    }

	@RequestMapping(value = "/getFileObjectMetaData", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject getFileObjectMetaData(HttpServletRequest request, HttpServletResponse response) {

		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dataPipingService.getFileObjectMetaData(request, response);
		} catch (Exception e) {
		}
		return resultObj;
	}

	@RequestMapping(value = "/getFileObjectData", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody List getFileObjectData(HttpServletRequest request, HttpServletResponse response) {

		List dataList = new ArrayList();
		try {
			dataList = dataPipingService.getFileObjectData(request, response);
		} catch (Exception e) {
		}
		return dataList;
	}

	@RequestMapping(value = "/uploadColumnMap", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody String importFile(HttpServletRequest request, HttpServletResponse response,
			@RequestParam("importColMapFile") MultipartFile multipartFile) {
//        System.out.println("Entered Export Controller...");
		String result = "";
		try {
			try {
				String excelFilePath = etlFilePath + "Files/Excelimport/"
						+ request.getSession(false).getAttribute("ssUsername");
				boolean isMultipart = ServletFileUpload.isMultipartContent(request);

				// FileInputStream inputStream = new FileInputStream(new File(excelFilePath));
				long sizeInBytes = 0;
				String resultstring = "";
				File file = new File(excelFilePath);
				if (file.exists()) {
					file.delete();
				}
				if (!file.exists()) {
					file.mkdirs();
				}

				DiskFileItemFactory factory = new DiskFileItemFactory();
				// maximum size that will be stored in memory
				factory.setSizeThreshold(maxMemSize);
				ServletFileUpload upload = new ServletFileUpload(factory);
				upload.setSizeMax(maxFileSize);
				List fileItems = upload.parseRequest(request);
//            System.out.println("fileItems::::::::" + fileItems);
				byte[] bytes = multipartFile.getBytes();
				String filename = multipartFile.getOriginalFilename();
				System.out.println("filenAME:::" + filename);
				String fileType1 = filename.substring(filename.lastIndexOf(".") + 1, filename.length());
				String mainFileName = "ETLColMapping" + System.currentTimeMillis() + "." + fileType1;
				if (filename != null) {
					if (filename.lastIndexOf(File.separator) >= 0) {

						file = new File(filename);
						// filename = filename.substring(filename.lastIndexOf(File.separator)+1,
						// filename.length());
						// System.out.println(file.getAbsolutePath()+"=====IF=======fileName======="+filename);
					} else {
						file = new File(excelFilePath + File.separator + mainFileName);
						// System.out.println(file.getAbsolutePath()+"=====ELSE=======fileName======="+filename);
					}

					FileOutputStream osf = new FileOutputStream(file);
					osf.write(bytes);
					osf.flush();
					osf.close();
					result = mainFileName;
					// fi.write(file);
//                   result = dataPipingService.generateColumnMapping(request,response,file);
//                    file.delete();
//                        fi.delete();
				} else {
					result = "[]";
				}

				// }
				// }
			} catch (Exception e) {
				e.printStackTrace();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("result::::" + result);
		return result;
	}

	@RequestMapping(value = "/getUploadedColMappings", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject getUploadedColMappings(HttpServletRequest request) {
		JSONObject mappingObj = new JSONObject();
		try {
			String filePath = request.getParameter("filePath");
			if (filePath != null && !"".equalsIgnoreCase(filePath) && !"null".equalsIgnoreCase(filePath)) {
				filePath = etlFilePath + "Files/Excelimport/" + request.getSession(false).getAttribute("ssUsername")
						+ File.separator + filePath;
				File file = new File(filePath);
				mappingObj = dataPipingService.generateColumnMapping(request, file);
				file.delete();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return mappingObj;
	}

	@RequestMapping(value = "/selectSapTableColumns", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject selectSapTableColumns(HttpServletRequest request) {
		JSONObject columnsObj = new JSONObject();
		String result = "";
		try {

			columnsObj = dataPipingService.selectSapTableColumns(request);

		} catch (Exception e) {
		}
		return columnsObj;
	}

	@RequestMapping(value = "/getNormalizeTabs", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody String getNormalizeTabs(HttpServletRequest request) {
//        JSONObject resultObj = new JSONObject();
		List headersList = new ArrayList<>();
		Connection connection = null;
		JCO.Client fromJCOConnection = null;

		String fromOperatorStr = request.getParameter("fromOperator");
		JSONObject fromOperator = (JSONObject) JSONValue.parse(fromOperatorStr);
		JSONObject fromConnectObj = (JSONObject) fromOperator.get("connObj");
		String fromDragType = (String) fromOperator.get("dragType");
		String fromFilePath = (String) fromOperator.get("filePath");
		String fromTableName = (String) fromOperator.get("tableName");
		String fileName = (String) fromOperator.get("statusLabel");

		String toOperatorStr = request.getParameter("toOperator");
		JSONObject toOperator = (JSONObject) JSONValue.parse(toOperatorStr);

		String divStr = "";
		try {
			if (fromDragType != null && "File".equalsIgnoreCase(fromDragType)) {
//                String fileExt = FileUtils.get
				String fileExtension = fromFilePath.substring(fromFilePath.lastIndexOf(".") + 1, fromFilePath.length());
				headersList = genericProcessETLDataService.getHeadersOfImportedFile(fromFilePath, "." + fileExtension);
				headersList = (List) headersList.stream().map(element -> (fileName + ":" + element))
						.collect(Collectors.toList());

			} else if (fromDragType != null && "Table".equalsIgnoreCase(fromDragType)) {

				Object fromReturendObj = dataPipingService.getConnection(fromConnectObj);

				String matchedSelectStr = "";
				if (fromReturendObj instanceof Connection) {
					connection = (Connection) fromReturendObj;

				} else if (fromReturendObj instanceof JCO.Client) {
					fromJCOConnection = (JCO.Client) fromReturendObj;
				}
				List headersObjList = dataPipingService.getTreeDMTableColumnsOpt(connection, request, fromConnectObj,
						fromTableName);
				headersList = (List) headersObjList.stream()
						.map(element -> ((Object[]) element)[0] + ":" + ((Object[]) element)[1])
						.collect(Collectors.toList());
			}

			divStr = dataPipingService.getNormalizeTabs(request, headersList);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return divStr;
	}

	@RequestMapping(value = "/getDeNormalizeTabs", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody String getDeNormalizeTabs(HttpServletRequest request) {

		List headersList = new ArrayList<>();
		Connection connection = null;
		JCO.Client fromJCOConnection = null;

		String fromOperatorStr = request.getParameter("fromOperator");
		JSONObject fromOperator = (JSONObject) JSONValue.parse(fromOperatorStr);
		JSONObject fromConnectObj = (JSONObject) fromOperator.get("connObj");
		String fromDragType = (String) fromOperator.get("dragType");
		String fromFilePath = (String) fromOperator.get("filePath");
		String fromTableName = (String) fromOperator.get("tableName");
		String fileName = (String) fromOperator.get("statusLabel");

		String toOperatorStr = request.getParameter("toOperator");
		JSONObject toOperator = (JSONObject) JSONValue.parse(toOperatorStr);
		String divStr = "";
		try {
			if (fromDragType != null && "File".equalsIgnoreCase(fromDragType)) {
				String fileExtension = fromFilePath.substring(fromFilePath.lastIndexOf(".") + 1, fromFilePath.length());
				headersList = genericProcessETLDataService.getHeadersOfImportedFile(fromFilePath, "." + fileExtension);
				headersList = (List) headersList.stream().map(element -> (fileName + ":" + element))
						.collect(Collectors.toList());

			} else if (fromDragType != null && "Table".equalsIgnoreCase(fromDragType)) {

				Object fromReturendObj = dataPipingService.getConnection(fromConnectObj);

				String matchedSelectStr = "";
				if (fromReturendObj instanceof Connection) {
					connection = (Connection) fromReturendObj;

				} else if (fromReturendObj instanceof JCO.Client) {
					fromJCOConnection = (JCO.Client) fromReturendObj;
				}
				List headersObjList = dataPipingService.getTreeDMTableColumnsOpt(connection, request, fromConnectObj,
						fromTableName);
				headersList = (List) headersObjList.stream()
						.map(element -> ((Object[]) element)[0] + ":" + ((Object[]) element)[1])
						.collect(Collectors.toList());
			}
			divStr = dataPipingService.getDeNormalizeTabs(request, headersList);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return divStr;
	}

	@RequestMapping(value = "/checkFileExist", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject checkFileExist(HttpServletRequest request) { // ravi etl integration
		JSONObject resultObj = new JSONObject();
		try {

			resultObj = dataPipingService.checkFileExist(request);

		} catch (Exception e) {
		}
		return resultObj;
	}

	@RequestMapping(value = "/processPredefinedJob", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject processPredefinedJob(HttpServletRequest request) { // ravi etl integration
		JSONObject resultObj = new JSONObject();
		try {

//           resultObj = dataPipingService.processPredefinedJob(request);
		} catch (Exception e) {
		}
		return resultObj;
	}

	// uttezzz
	@RequestMapping(value = "/createCubeTable", method = { RequestMethod.GET,
			RequestMethod.POST }, produces = "application/json;charset=UTF-8")
	public @ResponseBody JSONArray createCubeTableInETL(HttpServletRequest request) {
		JSONArray resultObjArray = new JSONArray();
		try {
			resultObjArray = dataPipingService.createCubeTable(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObjArray;
	}

	@RequestMapping(value = "/getETLOpenFunctionForm", method = { RequestMethod.GET,
			RequestMethod.POST }, produces = "text/plain;charset=UTF-8")
	public @ResponseBody String getETLOpenFunctionForm(HttpServletRequest request) {
		String result = "";
		try {
			result = dataPipingService.getETLOpenFunctionForm(request);
		} catch (Exception e) {
		}
		return result;
	}

	@RequestMapping(value = "/importTreeDMDifferentFiles", method = { RequestMethod.GET,
			RequestMethod.POST }, produces = "text/plain;charset=UTF-8")
	public @ResponseBody String importDMDifferentFiles(HttpServletRequest request, HttpServletResponse response) {
		System.out.println("Entered Export Controller...");
		String result = "";
		try {

			String selectedFiletype = request.getParameter("selectedFiletype");
			String jsonDataStr = request.getParameter("jsonData");
			JSONObject jsonData = (JSONObject) JSONValue.parse(jsonDataStr);
			String sheetsStr = request.getParameter("sheets");
			JSONArray sheetsArray = (JSONArray) JSONValue.parse(sheetsStr);
			for (int i = 0; i < sheetsArray.size(); i++) {
				String sheetName = (String) sheetsArray.get(i);
				result = dataPipingService.importTreeDMDifferentFiles(request, response, jsonData, selectedFiletype,
						sheetName);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("result::::" + result);
		return result;
	}

	@RequestMapping(value = "/setFileDataType", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject setFileDataType(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dataPipingService.setFileDataType(request);
		} catch (Exception e) {
		}
		return resultObj;
	}

	@RequestMapping(value = "/importDataToStgTable", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject importDataToStgTable(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
//           resultObj = dataPipingService.importDataToStgTable(request);
		} catch (Exception e) {
		}
		return resultObj;
	}

	@RequestMapping(value = "/provideDALAuthorisation", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject provideDALAuthorisation(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String tableName = request.getParameter("tableName");
			String ssUsername = (String) request.getSession(false).getAttribute("ssUsername");
			resultObj = dataPipingService.provideDALAuthorisation(request, tableName, ssUsername);
		} catch (Exception e) {
		}
		return resultObj;
	}

	@RequestMapping(value = "/fetchSubJobProcessLog", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject fetchSubJobProcessLog(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {

			resultObj = dataPipingService.fetchSubJobProcessLog(request);
		} catch (Exception e) {
		}
		return resultObj;
	}

	@RequestMapping(value = "/sequenceMappingColumns", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject sequenceMappingColumns(HttpServletRequest request) {

		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dataPipingService.sequenceMappingColumns(request);
		} catch (Exception e) {
		}
		return resultObj;
	}

	@RequestMapping(value = "/getDriveApiDetails", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody JSONObject getDriveApiDetails(HttpServletRequest request) {

		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dataPipingService.getDriveApiDetails(request);
		} catch (Exception e) {
		}
		return resultObj;
	}

	@RequestMapping(value = "/shareSavedJob", method = { RequestMethod.GET, RequestMethod.POST })
	public @ResponseBody String shareSavedJob(HttpServletRequest request) {
		String message = "";
		try {
			message = saveJobsService.shareSavedJob(request);
		} catch (Exception e) {
		}
		return message;
	}

} // end of the class
