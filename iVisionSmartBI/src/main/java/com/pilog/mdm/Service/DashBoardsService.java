/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.Service;

import static java.lang.Integer.parseInt;
import static jxl.biff.BaseCellFeatures.logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.itextpdf.text.pdf.PdfPCell;
import com.itextpdf.text.pdf.PdfPTable;
import com.itextpdf.text.pdf.PdfWriter;
import com.itextpdf.text.*;
import com.ccavenue.security.AesCryptUtil;
import com.itextpdf.text.pdf.BaseFont;
import com.opencsv.CSVReader;
import com.pilog.mdm.DAO.CloudGridFormationDAO;
import com.pilog.mdm.DAO.CloudGridResultsDAO;
import com.pilog.mdm.DAO.DashBoardsDAO;
import com.pilog.mdm.DTO.RegistrationDTO;
import com.pilog.mdm.Utils.DashBoardUtills;
import com.pilog.mdm.service.IntelliSenseRegistrationService;
import com.pilog.mdm.service.V10GenericDxpTreeService;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import java.io.*;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.net.URI;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.itextpdf.xmp.impl.Base64;
import com.pilog.mdm.utilities.PilogUtilities;
import java.sql.Clob;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import jxl.format.Colour;
import jxl.write.WritableCellFormat;
import jxl.write.WritableFont;

import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.NumberToTextConverter;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.ui.ModelMap;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.util.MultiValueMap;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * @author Jagadish.K
 */
@Service
public class DashBoardsService {

	@Autowired
	public DashBoardsDAO dashBoardsDAO;

	@Autowired
	public CloudGridFormationDAO cloudGridFormationDAO;
	@Autowired
	public V10GenericDxpTreeService v10GenericDxpTreeService;
	@Autowired
	public IntelliSenseRegistrationService registrationService;

	private final ResourceLoader resourceLoader;

	public DashBoardsService(ResourceLoader resourceLoader) {
		this.resourceLoader = resourceLoader;
	}

	@Value("${file.store.homedirectory}")
	private String fileStoreHomedirectory;

	@Value("${MultipartResolver.fileUploadSize}")
	private long maxFileSize;
	private int maxMemSize;

	@Value("${jdbc.driver}")
	private String dataBaseDriver;
	@Value("${jdbc.username}")
	private String jdbcUsername;
	@Value("${jdbc.password}")
	private String jdbcPassword;
	@Value("${jdbc.url}")
	private String jdbcUrl;

	@Value("${jdbc.accessName}")
	public String accessName;

	/*
	 * private final static String CCAVENUE_WORKING_KEY_INR =
	 * "108F97CE68DEF1311DD7EA982E217D21"; private final static String
	 * CCAVENUE_WORKING_KEY_NON_INR = "4072806B891169F4AF1DDB1D8964912C";
	 */
	private final static String CCAVENUE_WORKING_KEY_INR = "1A35E6DF1D5D052227CBC56C6CB0B0B2";
	private final static String CCAVENUE_WORKING_KEY_NON_INR = "1A35E6DF1D5D052227CBC56C6CB0B0B2";

//    @Value("${jdbc.driver}")
//	private String dataBaseDriver;
	@Value("${jdbc.username}")
	private String userName;
	@Value("${jdbc.password}")
	private String password;
	@Value("${jdbc.url}")
	private String dbURL;
//	@Value("${jdbc.accessName}")
//	private String accessName;

	@Autowired
	public CloudGridResultsDAO cloudGridResultsDAO;

	@Autowired
	public DashBoardUtills dashBoardUtills;

	private PilogUtilities cloudUtills = new PilogUtilities();

	private static final String[] numNames = { "", " one", " two", " three", " four", " five", " six", " seven",
			" eight", " nine", " ten", " eleven", " twelve", " thirteen", " fourteen", " fifteen", " sixteen",
			" seventeen", " eighteen", " nineteen" };

	private static final String[] tensNames = { "", " ten", " twenty", " thirty", " forty", " fifty", " sixty",
			" seventy", " eighty", " ninety" };

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

//    public String getVisualizationLayout(HttpServletRequest request) {
//        String result = "";
//        try {
//            result = "<div>"
//                    + " <div class=\"container-fluid\">"
//                    + "<div class=\"row\">"
//                    + "<div class=\"col-md-8 chartView\">"
//                    + "<div class=\"row\">"
//                    + " <div class=\"col-md-2 lefticonView\">"
//                    + "<div class=\"visionVisualizationDataSourcesCLass\" id=\"visionVisualizationDataSourcesId\">"
//                    + "<div id=\"VisualizationSources\" class =\"VisualizationSourcesCLass\" ></div> "
//                    + "</div>"
//                    + "</div>"
//                    + "<div class=\"col-md-10\" id=\"visualizeArea\">"
//                    + "<div class=\"visionVisualizationDataChartcount\" id=\"visionVisualizationDataChartcount\">"
//                    + "<div class=\"visionVisualizationDataChartViewCLass\" id=\"visionVisualizationDataChartViewId\">"
//                    + "</div>"
//                    + "</div>"
//                    + "</div>"
//                    + "</div>"
//                    + "</div>"
//                    + "<div class=\"col-md-4 formMainDIv\">"
//                    + "<div id=\"VisualizationFormAreaId\" class=\"VisualizationFormAreaClass\">"
//                    //                    + "<div class=\"VisionImageVisualizationFilterOpen\" style=\"display:none\"><span class=\"textSpanFilterCLass\">Filters</span><span class=\"imageSpanFiltersCLass\"><img src=\"images/nextrightarrow.png\" width=\"16px\" id=\"VisionImageVisualizationFilterId\" class=\"VisionImageVisualizationFilterClass\" title=\"Show/Hide pane\"/></span></div>"
//                    //                    + "<div id =\"Filters\" class=\"VisionAnalyticsBIFilters\"><span>Filters</span><span class=\"imageSpanCLass\"><img src=\"images/nextrightarrow.png\" width=\"16px\" id=\"VisionImageVisualizationFilter\" class=\"VisionImageVisualization\" title=\"Show/Hide pane\"/></span></div> "
//                    + "<div class=\"VisionImageVisualizationChartsOpen\" style=\"display:none\"><span  class=\"textSpanChartsCLass\">Visualizations</span><span class=\"imageSpanChartsCLass\"><img src=\"images/nextrightarrow.png\" width=\"16px\" id=\"VisionImageVisualizationChartsId\" class=\"VisionImageVisualizationChartsClass\" title=\"Show/Hide pane\"/></span></div>"
//                    + "<div id =\"Visualization\" class=\"VisionAnalyticsBICharts\"><div><span>Visualizations</span><span class=\"imageSpanCLass\"><img src=\"images/nextrightarrow.png\" width=\"16px\" id=\"VisionImageVisualizationCharts\" class=\"VisionImageVisualization\" title=\"Show/Hide pane\"/></span></div>"
//                    + "<div id=\"VisionBIVisualization\">"
//                    + "<div id='jqxTabCharts' class=\"jqxTabChartsClass\">"
//                    + "<div id='visionVisualizeBasicTabs' class='visionVisualizeChartsTabsClass'>"
//                    + "<img onclick=\"getChartDiv('Pie_Chart_Inner_Icon.svg', 'pie')\" src='images/Pie.svg' title='Pie chart'>"
//                    + "<img onclick=\"getChartDiv('Bar_Chart_Inner_Icon.svg', 'bar')\" src='images/Bar.svg' title='Bar chart'>"
//                    + "<img onclick=\"getChartDiv('Donut_Chart_Inner_Icon.svg', 'donut')\"  src='images/Donut.svg' title='Donut chart'>"
//                    + "<img onclick=\"getChartDiv('Column_Chart_Inner_Icon.svg', 'column')\"  src='images/Column.svg' title='Column chart'>"
//                    + "<img onclick=\"getChartDiv('Line_Chart_Inner_Icon.svg', 'lines')\"  src='images/Line.svg' title='Line chart'>"
//                    + "<img onclick=\"getChartDiv('Scatter_Chart_Inner_Icon.svg', 'scatter')\"  src='images/Scatter.svg' title='Scatter chart'>"
//                    + "<img onclick=\"getChartDiv('Tree_Chart_Inner_Icon.svg', 'treemap')\"  src='images/Tree_Chart.svg' title='Tree chart'>"
//                    + "<img onclick=\"getChartDiv('Histogram_Chart_Inner_Icon.svg', 'column')\"  src='images/Histogram.svg' title='Histogram chart'>"
//                    + "<img onclick=\"getChartDiv('Guage_Chart_Inner_Icon.svg', 'indicator')\"  src='images/Guage.svg' title='Guage chart'>"
//                    + "<img onclick=\"getChartDiv('Funnel_Chart_Inner_Icon.svg', 'funnel')\"  src='images/Funnel.svg' title='Funnel chart'>"
//                    + "<img onclick=\"getChartDiv('Candlestick_Chart_Inner_Icon.svg', 'candlestick')\"  src='images/Candlestick.svg' title='Candlestick chart'>"
//                    + "<img onclick=\"getChartDiv('Waterfall_Chart_Inner_Icon.svg', 'waterfall')\"  src='images/Waterfall.svg' title='Waterfall chart'>"
//                    + "<img onclick=\"getChartDiv('Redar-Chart-Thin.svg', 'scatterpolar')\"  src='images/Redar-Chart.svg' title='Radar chart'>"
//                    + "<img onclick=\"getChartDiv('vendorsCount.svg', 'Card')\"  src='images/Redar-Chart.svg' title='DashBordCard chart'>"
//                    + "</div>"
//                    + "</div>"
//                    + "<div id = 'visionVisualizeSlicerId' class='visionVisualizeSlicerClass'>"
//                    + "<div class='visionVisualizeSlicerImageDivClass'><img src=\"images/Chart_Slicer.svg\" onclick=\"showSlicerField('visionVisualizeSlicerFieldId')\" width=\"20px\" id=\"VisionVisualizationSlicerImageId\" class=\"VisionVisualizationSlicerImageClass\" title=\"Click for Slicer\"/></div>"
//                    + "<div id ='visionVisualizeSlicerFieldId' class='visionVisualizeSlicerFieldClass' style='display:none'><span>Drop Fields Here</span></div>"
//                    + "</div>"
//                    + "<div id='visualizeConfigTabs' class='visualizeConfigTabsClass'>"
//                    + "<ul id='visionVisualizeConfig'>"
//                    + "<li id='visionVisualizeFields' class='visionVisualizeFieldsClass'><img src='images/Fields_Selection.svg' style='cursor:pointer;' onclick=\"showChartConfigurationDIv('visualizeChartConfigColumns','visionVisualizeFields')\"/></li>"
//                    + "<li id='visionVisualizeConfiguration' class='visionVisualizeConfigurationClass'><img src='images/Chart_Config.svg' style='cursor:pointer;' onclick=\"showChartConfigurationDIv('visualizeChartConfigProperties','visionVisualizeConfiguration')\"/></li>"
//                    + "<li id='visionVisualizeFilters' class='visionVisualizeFiltersClass'><img src='images/Filter.svg' style='cursor:pointer;' onclick=\"showChartConfigurationDIv('visualizeChartConfigFilters','visionVisualizeFilters')\"/></li>"
//                    + "<li id='visionVisualizeJoins' class='visionVisualizeJoinsClass'><img src='images/mapping.svg' style='cursor:pointer;' onclick=\"showChartConfigurationDIv('visualizeChartConfigJoins','visionVisualizeJoins')\"/></li>"
//                    + "</ul>"
//                    + "</div>"
//                    + "<div id=\"visualizeChartConfigColumns\" class=\"visualizeChartConfigColumnsClass\"></div>"
//                    + "<div id=\"visualizeChartConfigProperties\" class=\"visualizeChartConfigPropertiesClass\" style='display:none'></div>"
//                    + "<div id=\"visualizeChartConfigFilters\" class=\"visualizeChartConfigFiltersClass\" style='display:none'></div>"
//                    + "<div id=\"visualizeChartConfigJoins\" class=\"visualizeChartConfigJoinsClass\" style='display:none'></div>"
//                    //                    + "</div>"
//                    + "</div>"
//                    + "</div>"
//                    + "<div class=\"VisionImageVisualizationFieldsOpen\" style=\"display:none\"><span  class=\"textSpanFieldsCLass\">Columns</span><span class=\"imageSpanFieldsCLass\"><img src=\"images/nextrightarrow.png\" width=\"16px\" id=\"VisionImageVisualizationFieldsId\" class=\"VisionImageVisualizationFieldsClass\" title=\"Show/Hide pane\"/></span></div>"
//                    + "<div id =\"Fields\" class=\"VisionAnalyticsBIFields\"><div><span>Columns</span><span class=\"imageSpanCLass\"><img src=\"images/nextrightarrow.png\" width=\"16px\" id=\"VisionImageVisualizationFields\" class=\"VisionImageVisualization\" title=\"Show/Hide pane\"/></span></div><div id=\"VisualizeBIColumns\"></div></div> "
//                    + "</div>"
//                    + "</div>"
//                    + "</div>"
//                    + "</div>"
//                    + "</div>";
//        } catch (Exception ex) {
//            ex.printStackTrace();
//        }
//        return result;
//    }
	public JSONObject getGenericDxpTreeOpt(HttpServletRequest request, String treeId) {
		JSONObject treeObj = new JSONObject();
		try {
			JSONObject labelsObj = new JSONObject();
			JSONObject treeConfigObj = new JSONObject();
			JSONObject treeInitParamObj = new JSONObject();
			JSONObject treeDefaultSource = new JSONObject();
			JSONObject treeColumnObj = new JSONObject();
			List<Object[]> treeList = dashBoardsDAO.getTreeListOpt(request, treeId);
			String treeGlobalSearchColsStr = "";

			if (treeList != null && !treeList.isEmpty()) {
				for (int i = 0; i < treeList.size(); i++) {
					Object[] treeObjArray = treeList.get(i);
					if (treeObjArray != null && treeObjArray.length != 0) {
						if (i == 0) {
							if (treeObjArray[2] != null && !"".equalsIgnoreCase(String.valueOf(treeObjArray[2]))
									&& !"null".equalsIgnoreCase(String.valueOf(treeObjArray[2]))) {
								treeObj.put("treeDesc", replaceSessionValues(String.valueOf(treeObjArray[2]), request));
							} else {
								treeObj.put("treeDesc", treeObjArray[2]);
							}

							treeConfigObj.put("width", treeObjArray[4]);
							treeConfigObj.put("height", treeObjArray[5]);
							treeConfigObj.put("enableHover", true);
							treeConfigObj.put("keyboardNavigation", true);
							treeConfigObj.put("incrementalSearch", true);
							treeConfigObj.put("theme", treeObjArray[3]);
							if (treeObjArray[6] != null && "CHBX".equalsIgnoreCase(String.valueOf(treeObjArray[6]))) {
								treeConfigObj.put("checkboxes", true);
								treeConfigObj.put("hasThreeStates", true);
							} else {
								treeConfigObj.put("checkboxes", false);
							}
							treeDefaultSource.put("label",
									((treeObjArray[8] != null && !"".equalsIgnoreCase(String.valueOf(treeObjArray[8]))
											&& !"null".equalsIgnoreCase(String.valueOf(treeObjArray[8])))
													? replaceSessionValues(String.valueOf(treeObjArray[8]), request)
													: treeObjArray[2]));
							treeDefaultSource.put("description",
									((treeObjArray[8] != null && !"".equalsIgnoreCase(String.valueOf(treeObjArray[8]))
											&& !"null".equalsIgnoreCase(String.valueOf(treeObjArray[8])))
													? replaceSessionValues(String.valueOf(treeObjArray[8]), request)
													: treeObjArray[2]));

							JSONArray sourceItems = new JSONArray();
							JSONObject itemObj = new JSONObject();
							itemObj.put("label",
									((treeObjArray[8] != null && !"".equalsIgnoreCase(String.valueOf(treeObjArray[8]))
											&& !"null".equalsIgnoreCase(String.valueOf(treeObjArray[8])))
													? replaceSessionValues(String.valueOf(treeObjArray[8]), request)
													: treeObjArray[2]));
							itemObj.put("value", "ajax");
							sourceItems.add(itemObj);
							treeDefaultSource.put("items", sourceItems);
							JSONObject treeInitParams = dashBoardsDAO
									.getInitParamObject(cloudUtills.clobToString((Clob) treeObjArray[19]));
							if (treeInitParams != null && !treeInitParams.isEmpty()
									&& treeInitParams.get("uuu_MultiTreeDlovId") != null
									&& !"".equalsIgnoreCase(String.valueOf(treeInitParams.get("uuu_MultiTreeDlovId")))
									&& !"null".equalsIgnoreCase(
											String.valueOf(treeInitParams.get("uuu_MultiTreeDlovId")))) {
								String buttonStr = "";
								String selectBoxStr = dashBoardsDAO.getLOV(request,
										(String) treeInitParams.get("uuu_MultiTreeDlovId"));
								String mainResultSearch = "<div class='mainsearch_input_div smartsearchtb'>";

								mainResultSearch += "<input type='text' id='mainTreeSearchResult' autocomplete='off' title='"
										+ cloudUtills.convertIntoMultilingualValue(
												labelsObj, "Enter atleast 3 characters to Search")
										+ "' " + "placeholder='  "
										+ cloudUtills.convertIntoMultilingualValue(labelsObj,
												"Type keyword(s) to search")
										+ "' "
										+ "data-no='NA' aria-haspopup='true' aria-multiline='false' aria-readonly='false' aria-disabled='false' aria-autocomplete='both' "
										+ "role='textbox' class='visionSearchClearResize clearable clearable2 ac jqx-widget-content jqx-widget-content-arctic jqx-input jqx-input-arctic jqx-widget jqx-widget-arctic jqx-rc-all jqx-rc-all-arctic smartserachclass' "
										+ "data-selected='NO'>"
										+ "  <a class='clear_searchField' style='position: absolute; font-size: 18px; cursor: pointer; display: none; top:2.5px; right: 5px;' onclick='clearTextSearch();'>×</a>";

								mainResultSearch += "<div class='TreesearchButton'>"
										// + " <input type='submit' id='getsmartsearch' data-source='" + treeObjArray[1]
										// + "' class='searchbutton' value='' onclick=searchResultsHandler('s','lucene')
										// id='result' title='Click here to Search'>"
										+ "                                <input type='submit' id='getsmartsearch' data-source='"
										+ treeObjArray[1]
										+ "' class='dxpTreesearchbutton' value='' onclick=treeSearchResultsHandler('"
										+ treeObjArray[1] + "') id='result' title='Click here to Search'>"
										+ "            </div>"
										+ " <div data-selection-type='containing' data-text='NA' data-space='no' "
										+ "				class='smartsearchresultsviews' id='cloudTreeIntellisensebox' "
										+ "			        style='background: transparent none repeat scroll 0% 0%;'>"
										+ " <div class='cloudTreeSearchinnerclass' id='cloudTreeIntellisense'></div>"
										+ " <div id='cloudTreeIntellisense1'></div>" + "     </div>" + "</div>";
								treeObj.put("mainSearchBox", mainResultSearch);
								if (selectBoxStr != null && !"".equalsIgnoreCase(selectBoxStr)
										&& !"null".equalsIgnoreCase(selectBoxStr)) {
									selectBoxStr = "<span class='selectDxpBoxstr'>Selection Type: </span>"
											+ "<span class=\"treeDxpSelectBoxMain\"><select id=\"treeSelectBox\" onchange=getSelectedTree()>"
											+ selectBoxStr + "</select></span>";
									treeObj.put("selectBoxStr", selectBoxStr);
								}

							}
							String result = "";
							result += "<div class='treeSearchtextcount' id='treeSearchtextcount'></div></h3>";
							result += "<div class='search_input_div smartsearchtb' >";

							result += "<input type='text' id='dxptreeSearchResult' autocomplete='off' title='"
									+ cloudUtills.convertIntoMultilingualValue(labelsObj,
											"Enter atleast 3 characters to Search")
									+ "' " + "placeholder='  "
									+ cloudUtills.convertIntoMultilingualValue(labelsObj, "Type keyword(s) to search")
									+ "' "
									+ "data-no='NA' aria-haspopup='true' aria-multiline='false' aria-readonly='false' aria-disabled='false' aria-autocomplete='both' "
									+ "role='textbox' class='visionSearchClearResize clearable clearable2 ac jqx-widget-content jqx-widget-content-arctic jqx-input jqx-input-arctic jqx-widget jqx-widget-arctic jqx-rc-all jqx-rc-all-arctic smartserachclass' "
									+ "data-selected='NO'>"
									+ "  <a class='clear_searchField' style='position: absolute; font-size: 18px; cursor: pointer; display: none; top:2.5px; right: 5px;' onclick='clearTextSearch();'>×</a>";

							result += "</div><div class='TreesearchButton'>"
									+ "                                <input type='submit' id='getsmartsearch' data-source='"
									+ treeObjArray[1]
									+ "' class='dxpTreesearchbutton' value='' onclick=treeSearchResultsHandler('"
									+ treeObjArray[1] + "') id='result' title='Click here to Search'>"
									+ "            </div>";
							result += "<div data-selection-type='containing' data-text='NA' data-space='no' "
									+ "				class='dxpTreesmartsearchresults' id='intellisenseTreebox' "
									+ "			        style='background: transparent none repeat scroll 0% 0%;'>"
									+ " <div class='dxpTreesearchinnerclass' id='intellisenseTree'></div>"
									+ " <div id='dxpTreeintellisense'></div>" + "     </div>";

							treeObj.put("searchField", result);
						}
						JSONObject columnObj = new JSONObject();
						columnObj.put("TREE_REF_TABLE", treeObjArray[1]);// TREE_REF_TABLE
						columnObj.put("HL_FLD_NAME", treeObjArray[13]);// HL_FLD_NAME
						columnObj.put("FLD_NAME", treeObjArray[14]);// FLD_NAME
						columnObj.put("DISP_FLD_NAME", treeObjArray[15]);// DISP_FLD_NAME
						columnObj.put("FOLLOWUP_COMP_ID", treeObjArray[16]);// FOLLOWUP_COMP_ID
						columnObj.put("FOLLOWUP_COMP_TYPE", treeObjArray[17]);// FOLLOWUP_COMP_TYPE
						columnObj.put("FOLLOWOP_COMP_DESCR", treeObjArray[18]);// FOLLOWOP_COMP_DESCR

						if (treeObjArray[10] != null && !"".equalsIgnoreCase(String.valueOf(treeObjArray[10]))
								&& !"null".equalsIgnoreCase(String.valueOf(treeObjArray[10]))) {
							columnObj.put("TREE_PARAMS_ID", treeObjArray[10]);// TREE_PARAMS_ID
						}
						if (treeObjArray[19] != null && !"".equalsIgnoreCase(String.valueOf(treeObjArray[19]))
								&& !"null".equalsIgnoreCase(String.valueOf(treeObjArray[19]))) {
							columnObj.put("TREE_INIT_PARAMS", dashBoardsDAO
									.getInitParamObject(cloudUtills.clobToString((Clob) treeObjArray[19])));// TREE_INIT_PARAMS
						}
						treeColumnObj.put(i, columnObj);
					}
					JSONObject searchTreeInitParams = dashBoardsDAO
							.getInitParamObject(cloudUtills.clobToString((Clob) treeObjArray[19]));
					if (searchTreeInitParams != null && !searchTreeInitParams.isEmpty()
							&& searchTreeInitParams.get("uuu_GlobalTreeSearchColEnable") != null
							&& !"".equalsIgnoreCase(
									String.valueOf(searchTreeInitParams.get("uuu_GlobalTreeSearchColEnable")))
							&& "Y".equalsIgnoreCase(
									String.valueOf(searchTreeInitParams.get("uuu_GlobalTreeSearchColEnable")))) {
						treeGlobalSearchColsStr = "" + treeObjArray[34] + "";
						treeObj.put("treeGlobalSearchColumns", treeGlobalSearchColsStr);
						treeObj.put("FOLLOWUP_COMP_ID", treeObjArray[16]);

					}
				}
			}
			String divId = " <div class=\"mainDxpSplitter\" id=\"mainDxpSplitter\">"
					+ "<div class=\"firstDxpSplitterTree\" id=\"firstDxpSplitterTree\" style=\"overflow-y: auto;\">"
					+ " <div class=\"firstDxpSplitterData\" id=\"firstDxpSplitterData\">" + "<div id='jqxExpander'>"
					+ " <div id=\"expanderDesc\" class=\"visionTreeDescription\">" + treeObj.get("treeDesc") + "</div>"
					+ "<div style=\"border: none;\" id='jqxTreeDropdown' class=\"visionDxpTreeDropDown\">"
					+ treeObj.get("selectBoxStr") + "</div>"
					+ " <div style=\"border: none;\" id='jqxTreeDropdown' class=\"visionTreeSearchResults\" >"
					+ treeObj.get("searchField") + "</div>"
					+ "<div style=\"overflow: hidden;\" id=\"jqxTreeDiv\" class=\"visionjqxTreeDiv\">"
					+ "<div style=\"border: none;\" id='jqxTree'></div></div>" + "</div>" + "</div>"
					+ "<div id=\"treeGridDiv\" class=\"visionTreeCompDiv\"></div>" + "</div>" + "</div>";
			JSONArray treeDefaultSourceArray = new JSONArray();
			treeDefaultSourceArray.add(treeDefaultSource);
			treeConfigObj.put("source", treeDefaultSourceArray);
			treeObj.put("treeConfigObj", treeConfigObj);
			treeObj.put("treeColumnObj", treeColumnObj);
			treeObj.put("divid", divId);
			treeObj.put("chartTypesConfig", dashBoardsDAO.getChartTypesConfigDetails(request));
		} catch (Exception e) {
		}
		return treeObj;
	}

	public String replaceSessionValues(String query, HttpServletRequest request) {
		try {

			if (query.contains("'<<--") && query.contains("-->>'")) {
				String sessionAtt = query.substring((query.indexOf("'<<--")) + 5, query.indexOf("-->>'"));
				String sessionval = ((String) request.getSession(false).getAttribute(sessionAtt)) != null
						? (String) request.getSession(false).getAttribute(sessionAtt)
						: "";
				String replaceval = "'<<--" + sessionAtt + "-->>'";
				String query1 = query.substring(0, (query.indexOf("'<<--")));
				String query2 = query.substring((query.indexOf("-->>'")) + 5);
				query = query1 + "'" + sessionval + "'" + query2;
			}
			if (query.contains("<<--") && query.contains("-->>")
					&& !(query.contains("'<<--") && query.contains("-->>'"))) {
				String sessionAtt = query.substring((query.indexOf("<<--")) + 4, query.indexOf("-->>"));
				String sessionval = ((String) request.getSession(false).getAttribute(sessionAtt)) != null
						? (String) request.getSession(false).getAttribute(sessionAtt)
						: "";
				String replaceval = "<<--" + sessionAtt + "-->>";
				String query1 = query.substring(0, (query.indexOf("<<--")));
				String query2 = query.substring((query.indexOf("-->>")) + 4);
				query = query1 + "'" + sessionval + "'" + query2;
			}
			if (query.contains("'<<-") && query.contains("->>'")) {
				String sessionAtt = query.substring((query.indexOf("'<<-")) + 4, query.indexOf("->>'"));
				String sessionval = ((String) request.getSession(false).getAttribute(sessionAtt)) != null
						? (String) request.getSession(false).getAttribute(sessionAtt)
						: "";
				String replaceval = "'<<-" + sessionAtt + "->>'";
				String query1 = query.substring(0, (query.indexOf("'<<-")));
				String query2 = query.substring((query.indexOf("->>'")) + 4);
				query = query1 + "'" + sessionval + "'" + query2;
			}
			if (query.contains("<<-") && query.contains("->>") && !(query.contains("'<<-") && query.contains("->>'"))
					&& !(query.contains("<<--") && query.contains("-->>"))
					&& !(query.contains("'<<--") && query.contains("-->>'"))) {
				String sessionAtt = query.substring((query.indexOf("<<-")) + 3, query.indexOf("->>"));
				String sessionval = ((String) request.getSession(false).getAttribute(sessionAtt)) != null
						? (String) request.getSession(false).getAttribute(sessionAtt)
						: "";
				String replaceval = "<<-" + sessionAtt + "->>";
				String query1 = query.substring(0, (query.indexOf("<<-")));
				String query2 = query.substring((query.indexOf("->>")) + 3);
				query = query1 + "'" + sessionval + "'" + query2;
			}

			if ((query.contains("<<--") && query.contains("-->>"))
					|| (query.contains("<<-") && query.contains("->>"))) {
				query = replaceSessionValues(query, request);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return query;
	}

	public String getVisualizationLayout(HttpServletRequest request) {
		String result = "";
		String Connection = "Test";
		try {
			String subscriptionType = (String) request.getSession(false).getAttribute("ssSubscriptionType");
			String treeId = request.getParameter("treeId");
			JSONObject treeObj = getGenericDxpTreeOpt(request, treeId);
			String defaultDataViewTableName = dashBoardsDAO.getDefaultDataViewTableName(request);
			result = "<div class='dxpDataAnalyticswrapper'>"
					+ "<div class='leftFileUploads width60' id=\"leftFileUploadMainDivwrapperID\">"
					+ "<div class=\"leftUploadHeaderDiv\" onclick=\"leftFileUploadsDivToggle()\">"
					+ "<span class=\"uploadstitle\">" + "<h4>Data&nbsp;Integration</h4>" + "</span>"
					+ "<span class=\"toggleImg\" id=\"columnsToggleIcon\" onclick=\\\"dataIntegrationGuide()\\\"><img src=\"images/toggle_plusicon.png\" width=\"16px;\">"
					+ "</span>" + "</div>"
					// + "<div id=\"savedConnections\" style='display:none' >\n"
//					+ "<div id='dbVisualizeConeectionsParentId' class ='dbVisualizeSavedConeectionsParentClass'>"
					+ "<div id='dBConnection' class='dBConnectionClass'>" + "<ul id='dxpConnection'>"
					+ "<li title='New Connections' id='visualConnectionLi' >"
					+ "<img src='images/New Connection Icon-01.svg' onclick=showVisualizationConnection() class='visionEtlTabIcons visualDarkMode' style='cursor:pointer;'>"
					+ "</li>" + "<li title='Available Connections' id='treeDxpConnectionLi' >"
					+ "<img src='images/tree.svg' onclick=treeDxpConnections() class='visionEtlTabIcons visualDarkMode' style='cursor:pointer;'>"
//					+ "<img src='images/tree.svg' onclick=getTreeConnectionDropDown() class='visionEtlTabIcons visualDarkMode' style='cursor:pointer;'>"
					+ "</li>" + "</ul>" + "</div>"
					+ "<div id=\"ivisualizationConnectionsMain\" class='ivisualizationConnectionsMainClass' style='display:none' >"
					+ "<!--<div id=\"savedConnIcons\" class=\"savedConnIconsClass\">"
					+ "<img src=\"images/Refresh Icon.svg\" class=\"visionETLIcons visualDarkMode\" title=\"Refresh\" style=\"width:15px;height: 15px;cursor:pointer;\" "
					+ "onclick='refreshMappingTablesAnalytics()'/>"
					+ "<img src=\"images/Filter Icon-01.svg\" class=\"visionETLIcons visualDarkMode\" id=\"treeETLFilterImage\" title=\"Filter\" style=\"width:15px;height: 15px;cursor:pointer;\""
					+ "onclick='filterMappingTablesAnalytics()'/>" + "</div>-->"
//					+ "<div id=\"connectionSearchId\" class=\"connectionSearchId\"><input type=\"search\" id=\"valueId\" name=\"search\" placeholder=\"Search here...\"></div>"
					+ "<div id=\"connectionSearchId\" class=\"connectionSearchId\"><input type=\"search\" id=\"valueId\" class=\"connectionSearchInputCls\" onkeypress=\"showDataBaseTablesData(event)\"  name=\"search\" placeholder=\"Search here...\">"
					+ "<button onclick=\"getFilteredSchemaObject()\" id=\"searchSchemaObjBtnId\" class=\"searchSchemaObjBtnCls\">"
					+ "<i class=\"fas fa-search\"></i> " + "</button>" + "</div>"
					+ "<div id=\"ivisualizationConnections\" class='ivisualizationConnectionsClass'>" + "</div>"
//					+ "<div id =\"ivisualizationDataView\" class='ivisualizationDataViewCls'></div>"

					+ "</div>"
					// + "<div id=\"ivisualizationConnections\"
					// class='ivisualizationConnectionsClass' style='display:none' >"
					// +"</div>"
					+ "<div class='visionVisualizationDataSourcesCLass fileUploadsDA' id='visionVisualizationDataSourcesId'>"
					+ "<div id=\"VisualizationSources\" class ='VisualizationSourcesCLass'></div> "
					// + "<div id=\"savedConnections\" style='display:none' ></div>"
					+ "</div>" + "</div>"
//					+"</div>"
					+ "<div id=\"visualizationMainDivwrapperID\" class='visualizationMainDivwrapper width60'>"
					+ "<div class=\"visualizationHeaderDiv\" onclick=\"visualizationDivToggle()\">"
					+ "<span class=\"visualizationtitle\">" + "<h4>Data&nbsp;Analytics</h4>" + "</span>"
					+ "<span class=\"toggleImg\" id=\"visualToggleIcon\" onclick=\\\"dataAnalyticGuide()\\\"><img src=\"images/toggle_plusicon.png\" width=\"16px;\" class='visualDarkMode'></span>"
					+ "</div>" + "<div id =\"Visualization\" class='VisionAnalyticsBICharts visualBIChart'>"
					+ "<div id=\"VisionBIVisualization\">" + "<div id='jqxTabCharts' class=\"jqxTabChartsClass\">"
					+ "<div id='visionVisualizeBasicTabs' class='visionVisualizeChartsTabsClass'>"
					+ "<div class='row iconsRow'>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('Pie_Chart_Inner_Icon.svg', 'pie')\" src='images/Pie.svg' class='visualDarkMode' title='Pie chart looks like circle it is divided into sectors that each represent a proportion of the whole.'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('Bar_Chart_Inner_Icon.svg', 'bar')\" src='images/Bar.svg' class='visualDarkMode' title='A bar chart is a chart that presents categorical data with rectangular bars with lengths proportional to the values that they represent. The bars can be plotted horizontally'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('Donut_Chart_Inner_Icon.svg', 'donut')\"  src='images/Donut.svg' class='visualDarkMode' title='Doughnut chart looks like circle with hole it is divided into sectors that each represent a proportion of the whole'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('Column_Chart_Inner_Icon.svg', 'column')\"  src='images/Column.svg' class='visualDarkMode' title='A column chart is a chart that presents categorical data with rectangular bars with heights proportional to the values that they represent. The bars can be plotted vertically'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('Line_Chart_Inner_Icon.svg', 'lines')\"  src='images/Line.svg' class='visualDarkMode' title='A line chart is a type of chart which displays information as a series of data points called 'markers' connected by straight line segments'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('Scatter_Chart_Inner_Icon.svg', 'scatter')\"  src='images/Scatter.svg' class='visualDarkMode' title='A scatter chart visualizes the relationship between two continuous variables by plotting data points on a two-dimensional axis. It helps identify correlations, patterns, and trends in the data'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('Tree_Chart_Inner_Icon.svg', 'treemap')\"  src='images/Tree_Chart.svg' class='visualDarkMode' title='Tree maps display hierarchical data as a set of nested rectangles. Each branch of the tree is given a rectangle, which is then tiled with smaller rectangles representing sub-branches'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('Histogram_Chart_Inner_Icon.svg', 'histogram')\"  src='images/Histogram.svg' class='visualDarkMode' title='A histogram chart is a type of graph that displays the distribution of a dataset by grouping data points into bins or intervals. It helps visualize the frequency or count of values within each interval, providing insights into the shape, spread, and central tendency of the data'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('Guage.svg', 'indicator')\"  src='images/Guage.svg' class='visualDarkMode' title='A gauge chart is a circular visualization used to display a single data point's value relative to a defined range, typically represented as a dial with markers for minimum, maximum, and intermediate values. It is often used to show progress, performance, or levels'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('Funnel_Chart_Inner_Icon.svg', 'funnel')\"  src='images/Funnel.svg' class='visualDarkMode' title='Funnel charts can be used to illustrate stages in a process, they could be used to show anything that’s decreasing in size'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('Candlestick.svg', 'candlestick')\"  src='images/Candlestick.svg' class='visualDarkMode' title='A candlestick chart is a type of financial chart used to represent price movements of an asset over time, typically in stock trading. Each \"candlestick\" shows the opening, closing, highest, and lowest prices within a specific time period, with a rectangular body and \"wicks\" or \"shadows\" extending from the top and bottom'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('Waterfall.svg', 'waterfall')\"  src='images/Waterfall.svg' class='visualDarkMode' title='A waterfall chart is a form of data visualization that helps in understanding the cumulative effect of sequentially introduced positive or negative values. These intermediate values can either be time based or category based'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('Redar-Chart-Thin.svg', 'scatterpolar')\"  src='images/Redar-Chart.svg' class='visualDarkMode' title='A radar chart is a graphical method of displaying multivariate data in the form of a two-dimensional chart of three or more quantitative variables represented on axes starting from the same point'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('HeatMap_Inner_Icon.svg', 'heatMap')\"  src='images/HeatMap.svg' class='visualDarkMode' title='A heat map is a data visualization technique that shows magnitude of a phenomenon as color in two dimensions. The variation in color may be by hue or intensity, giving obvious visual cues to the reader about how the phenomenon is clustered or varies over space'></div>"
					// + "<div class='col-lg-4 col-md-4 visualIconDivImg'><img
					// onclick=\"getChartDiv('Bar_Chart_Inner_Icon.svg', 'barRotation')\"
					// src='images/Bar.svg' class='visualDarkMode' title='Bar Label Rotation
					// chart'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('Sunburst_Inner_Icon.svg', 'sunburst')\" src='images/Sunburst.svg' class='visualDarkMode' title='The sunburst chart is ideal for displaying hierarchical data. Each level of the hierarchy is represented by one ring or circle with the innermost circle as the top of the hierarchy'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('geoWorldMapInnerIcon.png', 'geochart')\" src='images/geoWorldMap.png' class='visualDarkMode' title='A geo chart is a type of map-based visualization that displays data geographically, with regions such as countries, states, or cities colored or marked based on specific values or metrics. It helps to visually compare data across different geographic locations, making it easier to spot regional trends and patterns'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('GeoChart_Inner_Icon.svg', 'geoLatLangchart')\" src='images/GeoChart.svg' class='visualDarkMode' title='A geo LatLong chart is a type of map-based visualization that displays data geographically, with regions such as countries, states, or cities colored or marked based on specific values or metrics. It helps to visually compare data across different geographic locations, making it easier to spot regional trends and patterns'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('Bar_Chart_Inner_Icon.svg', 'BarAndLine')\" src='images/Bar_Chart_Inner_Icon.svg' class='visualDarkMode' title='A bar and line chart combines a bar chart and a line chart in a single visualization. The bar chart typically represents categorical data with rectangular bars, while the line chart shows trends over time or continuous data, with a line connecting data points. This combination is useful for comparing discrete data alongside trends or progress over time'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('Integral-Analytics-Icon.png', 'boxplot')\" src='images/Integral-Analytics-Icon.png' class='visualDarkMode' title='A box plot chart visualizes the distribution of a dataset by displaying its minimum, first quartile, median, third quartile, and maximum values. It helps identify the spread, central tendency, and potential outliers in the data'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('sankey_chart.png', 'sankey')\" src='images/sankey_chart.png' class='visualDarkMode' title='The sankey chart is ideal for displaying hierarchical data. Each level of the hierarchy is represented by one ring or circle with the innermost circle as the top of the hierarchy'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('BasicAreaChart.png', 'BasicAreaChart')\" src='images/BasicAreaChart.png' class='visualDarkMode' title='A basic area chart is a type of graph that displays quantitative data over time or categories, with the area below the line filled in to represent volume. It is similar to a line chart, but the filled area emphasizes the magnitude of values, making it easier to visualize the cumulative total or changes over time'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('StackedAreaChart.png', 'StackedAreaChart')\" src='images/StackedAreaChart.png' class='visualDarkMode' title='A stacked area chart is a variation of the area chart where multiple data series are stacked on top of each other, showing the cumulative total as well as the contribution of each series over time or categories. It helps visualize both the overall trend and the individual parts that make up the total, making it useful for comparing proportions across multiple categories'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('GradientStackedAreaChart.png', 'GradStackAreaChart')\" src='images/GradientStackedAreaChart.png' class='visualDarkMode' title='A gradient stacked area chart is a variation of the stacked area chart where the areas are filled with gradient colors instead of solid colors. This technique enhances visual appeal and makes it easier to distinguish between different data series, especially when there are many stacked layers. The gradient effect also helps highlight changes in values or trends more effectively over time or categories'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('AreaPiecesChart.png', 'AreaPiecesChart')\" src='images/AreaPiecesChart.png' class='visualDarkMode' title='An area pieces chart is a data visualization that combines elements of both area and pie charts. It represents categorical data as a set of stacked areas where each section's size corresponds to a proportion of the total, similar to a pie chart, but displayed along a continuous axis, often with areas stacked over time or categories'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('ganttChart.png', 'ganttChart')\" src='images/ganttChart.png' class='visualDarkMode' title='A Gantt chart is a type of bar chart used for project management, displaying the schedule of tasks or activities over time. Each task is represented by a horizontal bar, with the length and position of the bar indicating the start date, duration, and end date of the task, making it easier to track project progress and dependencies.'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('stackedBarChart.png', 'stackedBarChart')\" src='images/stackedBarChart.png' class='visualDarkMode' title='A stacked bar chart is a type of bar chart where each bar is divided into multiple segments, with each segment representing a different category or sub-group. The length of each segment shows the proportion of each category within the total, making it easy to compare both the overall size and the individual components across different categories or time periods'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('decompositionTree.jpg', 'decompositionTree')\" src='images/decompositionTree.jpg' class='visualDarkMode' title='A decomposition tree chart is an interactive data visualization used to break down a main value or metric into its contributing factors. It allows users to explore hierarchical data by drilling down into different levels, showing how each component contributes to the overall value. This chart is especially useful for identifying patterns, causes, or areas that impact key metrics, often used in business analysis and decision-making'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('kpi_chart_image.png', 'kpiChart')\" src='images/kpi_chart_image.png' class='visualDarkMode' title='A KPI (Key Performance Indicator) chart is a visual representation of critical business metrics or performance indicators that are used to track the success of an organization or project. KPI charts are commonly used by managers and teams to monitor progress toward goals, identify areas of improvement, and make data-driven decisions'></div>"
					+ "<div class='col-lg-4  col-md-4 visualIconDivImg'><img onclick=\"getChartDiv('kpi_chart_image.png', 'kpiBarChart')\" src='images/kpi_chart_image.png' class='visualDarkMode' title='A KPI (Key Performance Indicator) chart is a visual representation of critical business metrics or performance indicators that are used to track the success of an organization or project. KPI charts are commonly used by managers and teams to monitor progress toward goals, identify areas of improvement, and make data-driven decisions'></div>"

					+ "</div>" + "</div>" + "</div>"
					+ "<div id='visionVisualizeCardTypesId' class='visionVisualizeCardTypesClass'>"
					+ "<span class='visionVisualizeCardTypesSpanClass'>Cards :</span>"
					+ "<img src='images/DashBoardCard.svg' class='visualDarkMode' style='cursor:pointer;' width=\"20px\" title='Card' onclick=\"getChartDiv('DashBoardCard.svg','Card','','','Normal')\"/>"
					+ "<img src='images/DashBoardCard.svg' class='visualCardDarkMode' style='cursor:pointer;' width=\"20px\" onclick=\"getChartDiv('DashBoardCard.svg','Card','','','Rectangle')\"/>"
					+ "</div>" + "<div id = 'visionVisualizeSlicerId' class='visionVisualizeSlicerClass'>"
					+ "<div class='visionVisualizeSlicerImageDivClass'><img src=\"images/Chart_Slicer.svg\" onclick=\"showSlicerField('visionVisualizeSlicerFieldId')\" width=\"20px\" id=\"VisionVisualizationSlicerImageId\" class=\"VisionVisualizationSlicerImageClass visualDarkMode\" title=\"Click for Slicer\"/></div>"
					+ "<div id ='visionVisualizeSlicerFieldId' class='visionVisualizeSlicerFieldClass' style='display:none'><span>Drop Fields Here</span></div>"
					+ "</div>" + "<div id='visualizeConfigTabs' class='visualizeConfigTabsClass'>"
					+ "<ul id='visionVisualizeConfig'>"
					+ "<li id='visionVisualizeFields' class='visionVisualizeFieldsClass'><img src='images/Fields_Selection.svg' class='visualDarkMode' style='cursor:pointer;' onclick=\"showChartConfigurationDIv('visualizeChartConfigColumns','visionVisualizeFields')\"/></li>"
					+ "<li id='visionVisualizeConfiguration' class='visionVisualizeConfigurationClass'><img src='images/Chart_Config.svg' class='visualDarkMode' style='cursor:pointer;' onclick=\"showChartConfigurationDIv('visualizeChartConfigProperties','visionVisualizeConfiguration')\"/></li>"
					+ "<li id='visionVisualizeFilters' class='visionVisualizeFiltersClass'><img src='images/Filter.svg' class='visualDarkMode' style='cursor:pointer;' onclick=\"showChartConfigurationDIv('visualizeChartConfigFilters','visionVisualizeFilters')\"/></li>"
					+ "<li id='visionVisualizeJoins' class='visionVisualizeJoinsClass'><img src='images/mapping.svg' class='visualDarkMode' style='cursor:pointer;' onclick=\"showChartConfigurationDIv('visualizeChartConfigJoins','visionVisualizeJoins')\"/></li>"
					// + "<li id='visionVisualizeButton' class='visionVisualizeJoinsClass'><button
					// type=\"button\" id=\"nameButton\" class=\"btn btn-primary\"
					// onclick=\"createChartWithoutDrag()\">Create</button>\r\n"
					+ "</ul>" + "</div>"
					+ "<div id=\"visualizeChartConfigColumns\" class=\"visualizeChartConfigColumnsClass\"></div>"
					+ "<div id=\"visualizeChartConfigProperties\" class=\"visualizeChartConfigPropertiesClass\" style='display:none'></div>"
					+ "<div id=\"visualizeChartConfigFilters\" class=\"visualizeChartConfigFiltersClass\" style='display:none'></div>"
					+ "<div id=\"visualizeChartConfigJoins\" class=\"visualizeChartConfigJoinsClass\" style='display:none'></div>"

					// + "</div>"
					+ "</div>" + "</div>" + "</div>"
					+ "<div class=\"chartViewAreaClass\" id=\"visualizeChartAndDataArea\">"
					+ "<div class=\"chartView\" id=\"visualizeArea\">"
					+ "<div class=\"visionVisualizationDataChartcount\" id=\"visionVisualizationDataChartcount\">"
					+ "<div class=\"visionVisualizationDataChartViewFilterClass\" id=\"visionVisualizationDataChartViewFilterId\" style='display:none'>"
					+ "<div class='visionHomeAutoSuggestionChartCount'><span class='visionAutoSuggestionChartCountSpanClass'>Charts Count :</span><span class='visionAutoSuggestionChartCountSpan'></span></div>"
					+ "<div id=\"visionHomeChartSuggestionsSaveId\" class=\"visionHomeChartSuggestionsFilterClass\"><span class=\"FilterImage\"><img onclick=\"saveHomePageAutoSuggestionsCharts()\" src=\"images/Save Icon.svg\" title=\"Save Charts\" style=\"width:20px;margin-left: 7px;\"></span></div>"
					+ "<div id=\"visionHomeChartSuggestionsFilterId\" class=\"visionHomeChartSuggestionsFilterClass\"><span class=\"FilterImage\"><img onclick=\"filterHomePageAutoSuggestionsCharts()\" src=\"images/filter.png\" title=\"Filter Charts\" style=\"width:20px;margin-left: 7px;\"></span></div>"
					+ "<div id=\"visionHomeChartSuggestionsDeleteId\" class=\"visionHomeChartSuggestionsFilterClass\"><span class=\"FilterImage\"><img onclick=\"deleteHomePageAutoSuggestionsCharts()\" src=\"images/delete_icon.svg\" title=\"Delete Charts\" style=\"width:20px;margin-left: 7px;\"></span></div>"
					+ "<div id='visionVisualizeChartsInRowId' class='visionVisualizeChartsInRowClass'>"
					+ "<span class='visionVisualizeChartsInRowSpanClass'>Charts in Row :</span>"
					+ "<select id='visionVisualizeChartsInRowSelectId' onchange='showChartsInRow()'>"
					+ "<option value='2'>2</option>" + "<option value='3' selected>3</option>"
					+ "<option value='4'>4</option>" + "</select>" + "</div>"
					+ "<div id='visionVisualizeChartsBasedOnQuestionsMainId' class='visionVisualizeChartsBasedOnQuestionsMainClass'>"
					+ "<div id='visionVisualizeChartsBasedOnQuestionsImageId' class='visionVisualizeChartsBasedOnQuestionsImageClass'><img src='./images/questions-img.png'></div>"
					+ "<div id='visionVisualizeChartsBasedOnQuestionsId' class='visionVisualizeChartsBasedOnQuestionsClass'></div>"
					+ "</div>" + "</div>"
					+ "<div class=\"visionVisualizationDataChartViewCLass\" id=\"visionVisualizationDataChartViewId\"></div>"
					+ "<div class=\"visionVisualizationDataModalChartViewCLass container-fluid\" id=\"visionVisualizationDataModalChartViewId\"></div></div>"
					+ "</div>" + "<div class=\"dataView\" id=\"visionGridDataView\" style=\"display:none\">"
					+ "<div class=\"VisionImageVisualizationFieldsOpen\" style=\"display:none\"><span  class=\"textSpanFieldsCLass\">Columns</span><span class=\"imageSpanFieldsCLass\"><img src=\"images/nextrightarrow.png\" width=\"16px\" id=\"VisionImageVisualizationFieldsId\" class=\"VisionImageVisualizationFieldsClass\" title=\"Show/Hide pane\"/></span></div>"
					+ "<div id =\"Fields\" class=\"VisionAnalyticsBIFields\" style=\"display:none\"><div><span>Columns</span><span class=\"imageSpanCLass\"><img src=\"images/nextrightarrow.png\" width=\"16px\" id=\"VisionImageVisualizationFields\" class=\"VisionImageVisualization\" title=\"Show/Hide pane\"/></span></div><div id=\"VisualizeBIColumns\"></div></div> "
					+ "<div class=\"visionSmartBiGridDataClass\" id=\"visionSmartBiGridDataId\">"
					+ "<div class=\"buttoonClass\" id=\"btnGroup\" style=\"display:none\"></div>"
					+ "<div class=\"gridDataView\" id=\"visualizeAreaGirdData1\"></div>"
					+ "<div class=\"visualizeAreaGirdDataView\" id=\"visualizeAreaGirdData\" style='display:none'></div>"
					+ "</div>"
					+ "<div class=\"visualizeTablesGridDataView\" id=\"visualizeTablesGridData\" style='display:none'></div>"
					+ "</div>"
					+ "<div id='visionChartAutoSuggestionsViewId' class='visionChartAutoSuggestionsViewClass' style=\"display:none\">"
					+ "<div class='visionChartsUserAutoSuggetionsClass' id='visionChartsUserAutoSuggetionsClassId'>"
					+ "<div class='visionChartsAutoSuggestionUserClass' id='visionChartsAutoSuggestionUserId1'></div>"
					+ "<div class='visionChartsAutoSuggestionUserExamplesClass' id='visionChartsAutoSuggestionUserExamplesId'>"
					+ "<div id='visionInsightsVisualizationChartId' class='visionInsightsVisualizationChartClass'></div>"
					+ "<div id='visionInsightsVisualizationChartDataId' class='visionInsightsVisualizationChartDataClass'></div>"
					+ "</div>" + "</div>" + "</div>"
					+ "<div id='visionVisualizeQueryGridId' class='visionVisualizeQueryGridClass' style=\"display:none\">"
					+ "<div id='visionVisualizeQueryId' class='visionVisualizeQueryClass'>"
					+ "<div id='visionVisualizeQueryHeaderId' class='visionVisualizeQueryHeaderClass'>"
					+ "<img id=\"scriptsExecute\" onclick=\"executeBIEditorScripts('visionVisualizeQueryBodyId')\" src=\"images/oracle_db.png\" class=\"visionETLIcons visionIntegralEditorIcons\" style=\"width:18px;height: 18px;cursor:pointer;\">"
					+ "<img id=\"pythonScriptsExecute\" onclick=\"executePythonBIEditorScripts('visionVisualizeQueryBodyId')\" src=\"images/python.png\" class=\"visionETLIcons visionIntegralEditorIcons\" style=\"width:18px;height: 18px;cursor:pointer;\">"
					+ "<img id=\"mergeTablesId\" onclick=\"mergeTables('visionVisualizeQueryBodyId')\" src=\"images/merge.png\" class=\"visionETLIcons visionIntegralEditorIcons\" style=\"width:18px;height: 18px;cursor:pointer;\">";
			if (subscriptionType != null && !"".equalsIgnoreCase(subscriptionType)
					&& !"null".equalsIgnoreCase(subscriptionType) && !"BASIC".equalsIgnoreCase(subscriptionType)
					&& !"PROFESSIONAL".equalsIgnoreCase(subscriptionType)) {
				result += "<img id=\"tablesSearchId\" onclick=\"searchTablesAi()\"  class=\"visionETLSearchIcons visionIntegralEditorIcons\" src=\"images/image_2023_03_27T10_09_03_182R.png\" style=\"width:18px;height: 18px;cursor:pointer;\">";
			}
			result += "</div>"
					+ "<div id='visionVisualizeQueryTablesBodyParentId' class='visionVisualizeQueryTablesBodyParentClass'>"
					+ "<div id='visionVisualizeQueryBodyParentId' class='visionVisualizeQueryBodyParentClass'>"
					+ "<div id='visionVisualizeQueryBodyId' class='visionVisualizeQueryBodyClass'>"
					+ "<div id='Current_V10_editor_1' class='Current_V10_editor_1Class'></div>" + "</div>"
					+ "<div id=\"searchDataContent\" class=\"searchDataContentClass\" style=\"display:none\"></div>"
					+ "</div>" + "<div id='visionVisualizeShowTablesDataId' class='visionVisualizeShowTablesDataClass'>"
					+ "</div>" + "</div>" + "</div>"
					+ "<div id='visionVisualizeQueryGridDataId' class='visionVisualizeQueryGridDataClass'>"
					+ "<div id='visionVisualizeQueryGridButtonsId' class='visionVisualizeQueryGridButtonsClass'></div>"
					+ "<div id='visionVisualizeQueryGridDataBodyId' class='visionVisualizeQueryGridDataBodyClass'></div>"
					+ "</div>" + "</div>"
					+ "<div id=\"designViewTabHeading\" class=\"visionSmartBIDesignTabHeadingsDiv\">"
					+ "<ul class=\"visionSmartBIDesignTabHeadings\">"
					+ "<li title=\"Design View\" id=\"li_designView\" class=\"visionSmartBiDesignTab visionSmartBiDesignTabHighLight\" onclick=\"switchSmartBiDesignTabs('li_designView', 'visualizeArea')\" ><span>Design View</span></li>"
					+ "<li title=\"Data View\" id=\"li_contentView\" class=\"visionSmartBiDesignTab\"onclick=\"switchSmartBiDesignTabs('li_contentView', 'visionGridDataView')\"><span>Data View</span></li> ";
			if (subscriptionType != null && !"".equalsIgnoreCase(subscriptionType)
					&& !"null".equalsIgnoreCase(subscriptionType) && !"BASIC".equalsIgnoreCase(subscriptionType)) {
				result += "<li title=\"IntelliSense View\" id=\"li_autoSuggestionsView\" class=\"visionSmartBiDesignTab\"onclick=\"switchSmartBiDesignTabs('li_autoSuggestionsView', 'visionChartAutoSuggestionsViewId')\"><span>Insights View</span></li>"
						+ "<li title=\"Editor View\" id=\"li_queryGridView\" class=\"visionSmartBiDesignTab\"onclick=\"switchSmartBiDesignTabs('li_queryGridView', 'visionVisualizeQueryGridId')\"><span>Editor View</span></li>";
			}
			result += "</ul>" + "</div>" + "</div>" + "<div id='dialog'></div>" + "<div id='dxpCreatePopOver'></div>"
					+ "<div id ='drillDownChartDataDialog'></div>"
					+ "<input type='hidden' id='defaultDataViewTableId' value='" + defaultDataViewTableName + "'/>"
					+ "<div id ='visionVisualizeChartEditAISuggest'></div>";
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return result;
	}

	public JSONArray getDataMigrationConnectionsTreeMenu(HttpServletRequest request, String parentMenuId) {

		JSONArray menuArray = new JSONArray();
		try {
			String subscriptionType = (String) request.getSession(false).getAttribute("ssSubscriptionType");
			if (parentMenuId != null) {
				if (subscriptionType != null && !"".equalsIgnoreCase(subscriptionType)
						&& !"null".equalsIgnoreCase(subscriptionType)) {
					if ("BASIC".equalsIgnoreCase(subscriptionType) || "PROFESSIONAL".equalsIgnoreCase(subscriptionType)
							|| "ENTERPRISE".equalsIgnoreCase(subscriptionType)) {

						JSONObject menuObj = new JSONObject();
						menuObj.put("id", "FILES");
						menuObj.put("PARENT_ID", parentMenuId);
						menuObj.put("PARENT_MENU_ID", parentMenuId);
						menuObj.put("MENU_ID", "FILES");
						menuObj.put("MENU_DESCRIPTION", "<span class='visionMenuTreeLabel'>Files</span>");
						menuObj.put("MAIN_DESCRIPTION", "Files");
						menuObj.put("icon", "images/File-Icon.svg");
						menuObj.put("value", "javascript:getTreeDataBase()");
						menuObj.put("TOOL_TIP", "Files");
						menuArray.add(menuObj);

						JSONObject menuObj5 = new JSONObject();
						menuObj5.put("id", "CSV");
						menuObj5.put("PARENT_ID", "FILES");
						menuObj5.put("PARENT_MENU_ID", "FILES");
						menuObj5.put("MENU_ID", "CSV");
						menuObj5.put("MENU_DESCRIPTION", "<span class='visionMenuTreeLabel'>CSV</span>");
						menuObj5.put("MAIN_DESCRIPTION", "CSV");
						menuObj5.put("icon", "images/CSV-Icon.svg");
						menuObj5.put("value", "javascript:anlyticsgetTreeDataBase('FILE','CSV')");
						menuObj5.put("TOOL_TIP", "CSV");
						// menuArray.add(menuObj5);
						JSONObject menuObj6 = new JSONObject();
						menuObj6.put("id", "XLS");
						menuObj6.put("PARENT_ID", "FILES");
						menuObj6.put("PARENT_MENU_ID", "FILES");
						menuObj6.put("MENU_ID", "XLS");
						menuObj6.put("MENU_DESCRIPTION", "<span class='visionMenuTreeLabel'>XLS</span>");
						menuObj6.put("MAIN_DESCRIPTION", "XLS");
						menuObj6.put("icon", "images/xls-Icon.svg");
						menuObj6.put("value", "javascript:anlyticsgetTreeDataBase('FILE','XLS')");
						menuObj6.put("TOOL_TIP", "XLS");
						menuArray.add(menuObj6);
						JSONObject menuObj7 = new JSONObject();
						menuObj7.put("id", "XLSX");
						menuObj7.put("PARENT_ID", "FILES");
						menuObj7.put("PARENT_MENU_ID", "FILES");
						menuObj7.put("MENU_ID", "XLSX");
						menuObj7.put("MENU_DESCRIPTION", "<span class='visionMenuTreeLabel'>XLSX</span>");
						menuObj7.put("MAIN_DESCRIPTION", "XLSX");
						menuObj7.put("icon", "images/XLSX-Icon.svg");
						menuObj7.put("value", "javascript:anlyticsgetTreeDataBase('FILE','XLSX')");
						menuObj7.put("TOOL_TIP", "XLSX");
						menuArray.add(menuObj7);
						JSONObject menuObj8 = new JSONObject();
						menuObj8.put("id", "JSON");
						menuObj8.put("PARENT_ID", "FILES");
						menuObj8.put("PARENT_MENU_ID", "FILES");
						menuObj8.put("MENU_ID", "JSON");
						menuObj8.put("MENU_DESCRIPTION", "<span class='visionMenuTreeLabel'>JSON</span>");
						menuObj8.put("MAIN_DESCRIPTION", "JSON");
						menuObj8.put("icon", "images/JSON_Icon.svg");
						menuObj8.put("value", "javascript:anlyticsgetTreeDataBase('FILE','JSON')");
						menuObj8.put("TOOL_TIP", "JSON");
						menuArray.add(menuObj8);
						JSONObject menuObj9 = new JSONObject();
						menuObj9.put("id", "XML");
						menuObj9.put("PARENT_ID", "FILES");
						menuObj9.put("PARENT_MENU_ID", "FILES");
						menuObj9.put("MENU_ID", "XML");
						menuObj9.put("MENU_DESCRIPTION", "<span class='visionMenuTreeLabel'>XML</span>");
						menuObj9.put("MAIN_DESCRIPTION", "XML");
						menuObj9.put("icon", "images/XML-Icon.svg");
						menuObj9.put("value", "javascript:anlyticsgetTreeDataBase('FILE','XML')");
						menuObj9.put("TOOL_TIP", "XML");
						menuArray.add(menuObj9);
						JSONObject menuObj10 = new JSONObject();
						menuObj10.put("id", "TEXT");
						menuObj10.put("PARENT_ID", "FILES");
						menuObj10.put("PARENT_MENU_ID", "FILES");
						menuObj10.put("MENU_ID", "TEXT");
						menuObj10.put("MENU_DESCRIPTION", "<span class='visionMenuTreeLabel'>TEXT</span>");
						menuObj10.put("MAIN_DESCRIPTION", "TEXT");
						menuObj10.put("icon", "images/TEXT_Icon.svg");
						menuObj10.put("value", "javascript:anlyticsgetTreeDataBase('FILE','TEXT')");
						menuObj10.put("TOOL_TIP", "TEXT");
						menuArray.add(menuObj10);

					}
					/*
					 * if ("PROFESSIONAL".equalsIgnoreCase(subscriptionType) ||
					 * "ENTERPRISE".equalsIgnoreCase(subscriptionType)) { JSONObject menuObj1 = new
					 * JSONObject(); menuObj1.put("id", "DATABASE"); menuObj1.put("PARENT_ID",
					 * parentMenuId); menuObj1.put("PARENT_MENU_ID", parentMenuId);
					 * menuObj1.put("MENU_ID", "DATABASE"); menuObj1.put("MENU_DESCRIPTION",
					 * "<span class='visionMenuTreeLabel'>Database</span>");
					 * menuObj1.put("MAIN_DESCRIPTION", "Database"); menuObj1.put("icon",
					 * "images/DB-Icon.svg"); menuObj1.put("value", "javascript:getTreeDataBase()");
					 * menuObj1.put("TOOL_TIP", "Database"); menuArray.add(menuObj1);
					 * 
					 * JSONObject menuObj11 = new JSONObject(); menuObj11.put("id", "ORACLE");
					 * menuObj11.put("PARENT_ID", "DATABASE"); menuObj11.put("PARENT_MENU_ID",
					 * "DATABASE"); menuObj11.put("MENU_ID", "ORACLE");
					 * menuObj11.put("MENU_DESCRIPTION",
					 * "<span class='visionMenuTreeLabel'>Oracle</span>");
					 * menuObj11.put("MAIN_DESCRIPTION", "Oracle"); menuObj11.put("icon",
					 * "images/DM_ORACLE-Icon.svg"); menuObj11.put("value",
					 * "javascript:getTreeDataBase('DB','Oracle')"); menuObj11.put("TOOL_TIP",
					 * "Oracle"); menuArray.add(menuObj11); JSONObject menuObj12 = new JSONObject();
					 * menuObj12.put("id", "MYSQL"); menuObj12.put("PARENT_ID", "DATABASE");
					 * menuObj12.put("PARENT_MENU_ID", "DATABASE"); menuObj12.put("MENU_ID",
					 * "MYSQL"); menuObj12.put("MENU_DESCRIPTION",
					 * "<span class='visionMenuTreeLabel'>MYSQL</span>");
					 * menuObj12.put("MAIN_DESCRIPTION", "MYSQL"); menuObj12.put("icon",
					 * "images/MYSQL_Icon.png"); menuObj12.put("value",
					 * "javascript:getTreeDataBase('DB','MYSQL')"); menuObj12.put("TOOL_TIP",
					 * "MYSQL"); menuArray.add(menuObj12); JSONObject menuObj13 = new JSONObject();
					 * menuObj13.put("id", "MSSQL"); menuObj13.put("PARENT_ID", "DATABASE");
					 * menuObj13.put("PARENT_MENU_ID", "DATABASE"); menuObj13.put("MENU_ID",
					 * "MSSQL"); menuObj13.put("MENU_DESCRIPTION",
					 * "<span class='visionMenuTreeLabel'>MSSQL</span>");
					 * menuObj13.put("MAIN_DESCRIPTION", "MSSQL"); menuObj13.put("icon",
					 * "images/MSSQL-Icon.png"); menuObj13.put("value",
					 * "javascript:getTreeDataBase('DB','MSSQL')"); menuObj13.put("TOOL_TIP",
					 * "MSSQL"); menuArray.add(menuObj13); JSONObject menuObj14 = new JSONObject();
					 * menuObj14.put("id", "MSAccess"); menuObj14.put("PARENT_ID", "DATABASE");
					 * menuObj14.put("PARENT_MENU_ID", "DATABASE"); menuObj14.put("MENU_ID",
					 * "MSAccess"); menuObj14.put("MENU_DESCRIPTION",
					 * "<span class='visionMenuTreeLabel'>MSAccess</span>");
					 * menuObj14.put("MAIN_DESCRIPTION", "MSAccess"); menuObj14.put("icon",
					 * "images/MSACCESS-Icon.svg"); menuObj14.put("value",
					 * "javascript:getTreeDataBase('DB','MSAccess')"); menuObj14.put("TOOL_TIP",
					 * "MSAccess"); menuArray.add(menuObj14); JSONObject menuObj15 = new
					 * JSONObject(); menuObj15.put("id", "SQLite"); menuObj15.put("PARENT_ID",
					 * "DATABASE"); menuObj15.put("PARENT_MENU_ID", "DATABASE");
					 * menuObj15.put("MENU_ID", "SQLite"); menuObj15.put("MENU_DESCRIPTION",
					 * "<span class='visionMenuTreeLabel'>SQLite</span>");
					 * menuObj15.put("MAIN_DESCRIPTION", "SQLite"); menuObj15.put("icon",
					 * "images/SQLLITE-Icon.png"); menuObj15.put("value",
					 * "javascript:getTreeDataBase('DB','SQLite')"); menuObj15.put("TOOL_TIP",
					 * "SQLite"); menuArray.add(menuObj15); JSONObject menuObj16 = new JSONObject();
					 * menuObj16.put("id", "PostgreSQL"); menuObj16.put("PARENT_ID", "DATABASE");
					 * menuObj16.put("PARENT_MENU_ID", "DATABASE"); menuObj16.put("MENU_ID",
					 * "PostgreSQL"); menuObj16.put("MENU_DESCRIPTION",
					 * "<span class='visionMenuTreeLabel'>PostgreSQL</span>");
					 * menuObj16.put("MAIN_DESCRIPTION", "PostgreSQL"); menuObj16.put("icon",
					 * "images/DM_POSTGRESQL-Icon.png"); menuObj16.put("value",
					 * "javascript:getTreeDataBase('DB','PostgreSQL')"); menuObj16.put("TOOL_TIP",
					 * "PostgreSQL"); menuArray.add(menuObj16); } if
					 * ("ENTERPRISE".equalsIgnoreCase(subscriptionType)) {
					 * 
					 * JSONObject menuObj2 = new JSONObject(); menuObj2.put("id", "ONL_SERVICES");
					 * menuObj2.put("PARENT_ID", parentMenuId); menuObj2.put("PARENT_MENU_ID",
					 * parentMenuId); menuObj2.put("MENU_ID", "ONL_SERVICES");
					 * menuObj2.put("MENU_DESCRIPTION",
					 * "<span class='visionMenuTreeLabel'>Online Services</span>");
					 * menuObj2.put("MAIN_DESCRIPTION", "Online Services"); menuObj2.put("icon",
					 * "images/ONLINE_SERVICES_Icon.svg"); menuObj2.put("value",
					 * "javascript:getTreeDataBase()"); menuObj2.put("TOOL_TIP", "Online Services");
					 * menuArray.add(menuObj2); JSONObject menuObj3 = new JSONObject();
					 * menuObj3.put("id", "ERP"); menuObj3.put("PARENT_ID", parentMenuId);
					 * menuObj3.put("PARENT_MENU_ID", parentMenuId); menuObj3.put("MENU_ID", "ERP");
					 * menuObj3.put("MENU_DESCRIPTION",
					 * "<span class='visionMenuTreeLabel'>ERP</span>");
					 * menuObj3.put("MAIN_DESCRIPTION", "ERP"); menuObj3.put("icon",
					 * "images/ERP-Icon.svg"); menuObj3.put("value",
					 * "javascript:getTreeDataBase()"); menuObj3.put("TOOL_TIP", "ERP");
					 * menuArray.add(menuObj3);
					 * 
					 * JSONObject menuObj17 = new JSONObject(); menuObj17.put("id", "FACEBOOK");
					 * menuObj17.put("PARENT_ID", "ONL_SERVICES"); menuObj17.put("PARENT_MENU_ID",
					 * "ONL_SERVICES"); menuObj17.put("MENU_ID", "FACEBOOK");
					 * menuObj17.put("MENU_DESCRIPTION",
					 * "<span class='visionMenuTreeLabel'>Facebook</span>");
					 * menuObj17.put("MAIN_DESCRIPTION", "Facebook"); menuObj17.put("icon",
					 * "images/FB-Icon-01.png"); menuObj17.put("value",
					 * "javascript:getTreeDataBase('Online_Services','Facebook')");
					 * menuObj17.put("TOOL_TIP", "Facebook"); menuArray.add(menuObj17); JSONObject
					 * menuObj18 = new JSONObject(); menuObj18.put("id", "Twitter");
					 * menuObj18.put("PARENT_ID", "ONL_SERVICES"); menuObj18.put("PARENT_MENU_ID",
					 * "ONL_SERVICES"); menuObj18.put("MENU_ID", "Twitter");
					 * menuObj18.put("MENU_DESCRIPTION",
					 * "<span class='visionMenuTreeLabel'>Twitter</span>");
					 * menuObj18.put("MAIN_DESCRIPTION", "Twitter"); // menuObj18.put("icon",
					 * "images/TWITTER-Icon.svg"); menuObj18.put("icon",
					 * "images/twitter_spaceX.png"); menuObj18.put("value",
					 * "javascript:getTreeDataBase('Online_Services','Twitter')");
					 * menuObj18.put("TOOL_TIP", "Twitter"); menuArray.add(menuObj18); JSONObject
					 * menuObj19 = new JSONObject(); menuObj19.put("id", "LinkedIn");
					 * menuObj19.put("PARENT_ID", "ONL_SERVICES"); menuObj19.put("PARENT_MENU_ID",
					 * "ONL_SERVICES"); menuObj19.put("MENU_ID", "LinkedIn");
					 * menuObj19.put("MENU_DESCRIPTION",
					 * "<span class='visionMenuTreeLabel'>LinkedIn</span>");
					 * menuObj19.put("MAIN_DESCRIPTION", "LinkedIn"); menuObj19.put("icon",
					 * "images/Linkedin-Icon-01.png"); menuObj19.put("value",
					 * "javascript:getTreeDataBase('Online_Services','LinkedIn')");
					 * menuObj19.put("TOOL_TIP", "LinkedIn"); menuArray.add(menuObj19);
					 * 
					 * JSONObject menuObj21 = new JSONObject(); menuObj21.put("id", "SAP");
					 * menuObj21.put("PARENT_ID", "ERP"); menuObj21.put("PARENT_MENU_ID", "ERP");
					 * menuObj21.put("MENU_ID", "SAP"); menuObj21.put("MENU_DESCRIPTION",
					 * "<span class='visionMenuTreeLabel'>SAP</span>");
					 * menuObj21.put("MAIN_DESCRIPTION", "SAP"); menuObj21.put("icon",
					 * "images/SAP_Ison-01.png"); menuObj21.put("value",
					 * "javascript:getTreeDataBase('ERP','SAP')"); menuObj21.put("TOOL_TIP", "SAP");
					 * menuArray.add(menuObj21); JSONObject menuObj22 = new JSONObject();
					 * menuObj22.put("id", "ORACLE_ERP"); menuObj22.put("PARENT_ID", "ERP");
					 * menuObj22.put("PARENT_MENU_ID", "ERP"); menuObj22.put("MENU_ID",
					 * "ORACLE_ERP"); menuObj22.put("MENU_DESCRIPTION",
					 * "<span class='visionMenuTreeLabel'>ORACLE ERP</span>");
					 * menuObj22.put("MAIN_DESCRIPTION", "ORACLE ERP"); menuObj22.put("icon",
					 * "images/DM_ORA_ERP-Icon-01.png"); menuObj22.put("value",
					 * "javascript:getTreeDataBase('ERP','Oracle_ERP')"); menuObj22.put("TOOL_TIP",
					 * "ORACLE ERP"); menuArray.add(menuObj22);
					 * 
					 * JSONObject menuObj21 = new JSONObject(); menuObj21.put("id", "SAP_ECC");
					 * menuObj21.put("PARENT_ID", "ERP"); menuObj21.put("PARENT_MENU_ID", "ERP");
					 * menuObj21.put("MENU_ID", "SAP_ECC"); menuObj21.put("MENU_DESCRIPTION",
					 * "<span class='visionMenuTreeLabel'>SAP ECC</span>");
					 * menuObj21.put("MAIN_DESCRIPTION", "SAP ECC"); menuObj21.put("icon",
					 * "images/SAP_Ison-01.png"); menuObj21.put("value",
					 * "javascript:getTreeDataBase('ERP','SAP_ECC')"); menuObj21.put("TOOL_TIP",
					 * "SAP ECC"); menuArray.add(menuObj21); JSONObject menuObj22 = new
					 * JSONObject(); menuObj22.put("id", "SAP_HANA"); menuObj22.put("PARENT_ID",
					 * "ERP"); menuObj22.put("PARENT_MENU_ID", "ERP"); menuObj22.put("MENU_ID",
					 * "SAP_HANA"); menuObj22.put("MENU_DESCRIPTION",
					 * "<span class='visionMenuTreeLabel'>SAP_HANA</span>");
					 * menuObj22.put("MAIN_DESCRIPTION", "SAP HANA"); menuObj22.put("icon",
					 * "images/etl/SAP_HANA.jpg"); menuObj22.put("value",
					 * "javascript:getTreeDataBase('ERP','SAP_HANA')"); menuObj22.put("TOOL_TIP",
					 * "SAP HANA"); menuArray.add(menuObj22); JSONObject menuObj23 = new
					 * JSONObject(); menuObj23.put("id", "ORACLE_ERP"); menuObj23.put("PARENT_ID",
					 * "ERP"); menuObj23.put("PARENT_MENU_ID", "ERP"); menuObj23.put("MENU_ID",
					 * "ORACLE_ERP"); menuObj23.put("MENU_DESCRIPTION",
					 * "<span class='visionMenuTreeLabel'>ORACLE ERP</span>");
					 * menuObj23.put("MAIN_DESCRIPTION", "ORACLE ERP"); menuObj23.put("icon",
					 * "images/DM_ORA_ERP-Icon-01.png"); menuObj23.put("value",
					 * "javascript:getTreeDataBase('ERP','Oracle_ERP')"); menuObj23.put("TOOL_TIP",
					 * "ORACLE ERP"); menuArray.add(menuObj23); }
					 */
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return menuArray;

	}

	public JSONObject importTreeDMFileXlsx(HttpServletRequest request, HttpServletResponse response,
			JSONObject jsonData, String selectedFiletype) {

		FileInputStream inputStream = null;
		FileOutputStream outs = null;
		JSONObject importResult = new JSONObject();
		try {
			if (true) {
				// fis = new FileInputStream(new File(filepath));
				String originalFileName = request.getParameter("fileName");
				String userName = (String) request.getSession(false).getAttribute("ssUsername");
				String filePath = fileStoreHomedirectory + "TreeDMImport/" + userName;
//				String filePath = "C:/Files/TreeDMImport" + File.separator + userName;

				String mainFileName = "SPIRUploadSheet" + System.currentTimeMillis() + "." + selectedFiletype;
				String fileName = filePath + File.separator + mainFileName;

				String headersObjStr = request.getParameter("headersObj");
				JSONObject headersObj = (JSONObject) JSONValue.parse(headersObjStr);

				String sheetsStr = request.getParameter("sheets");
				JSONArray sheetsArray = (JSONArray) JSONValue.parse(sheetsStr);

				File outputFile = new File(filePath);
				if (outputFile.exists()) {
					System.out.println("folder is deleted");
					outputFile.delete();
				}
				if (!outputFile.exists()) {
					System.out.println("folder is created");
					outputFile.mkdirs();
				} else {
					System.out.println("folder is not created");
				}

				XSSFWorkbook outputWb = new XSSFWorkbook();
//                Workbook outputWb = (XSSFWorkbook) WorkbookFactory.create(new File(fileName));
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
				CellStyle dateCellStyle = outputWb.createCellStyle();
				CellStyle timeCellStyle = outputWb.createCellStyle();
				CreationHelper createHelper = outputWb.getCreationHelper();
				dateCellStyle.setDataFormat(createHelper.createDataFormat().getFormat("dd-MM-yyyy"));
				timeCellStyle.setDataFormat(createHelper.createDataFormat().getFormat("h:mm:ss"));

				for (Object sheet : sheetsArray) {
					XSSFSheet outputSheet = outputWb.createSheet((String) sheet);

					JSONArray sheetData = (JSONArray) jsonData.get(sheet);
					JSONArray sheetHeaders = (JSONArray) headersObj.get(sheet);
					XSSFRow outPutHeader = outputSheet.createRow(0);

					for (int cellIndex = 0; cellIndex < sheetHeaders.size(); cellIndex++) {

						WritableFont cellFont = new WritableFont(WritableFont.TIMES, 16);

						WritableCellFormat cellFormat = new WritableCellFormat(cellFont);
						cellFormat.setBackground(Colour.ORANGE);
						XSSFCellStyle cellStyle = outputWb.createCellStyle();
						cellStyle.setAlignment(HSSFCellStyle.ALIGN_CENTER);
						cellStyle.setWrapText(true);

						String cellValue = (String) sheetHeaders.get(cellIndex);

						XSSFCell hssfCell = outPutHeader.createCell(cellIndex);
						hssfCell.setCellStyle(cellStyle);

						hssfCell.setCellValue(cellValue);

					}

					for (int i = 0; i < sheetData.size(); i++) {

						XSSFRow outPutRow = outputSheet.createRow(i + 1);

						JSONObject rowData = (JSONObject) sheetData.get(i);
						if (rowData != null) {

							for (int cellIndex = 0; cellIndex < sheetHeaders.size(); cellIndex++) {
								String header = (String) sheetHeaders.get(cellIndex);
								Object cellValue = rowData.get(header);
								XSSFCell outputCell = null;
								try {
//                            System.out.println(i+ " cellIndex::::" + cellIndex);
									outputCell = outPutRow.createCell(cellIndex);
									if (cellValue != null) {

										if (cellValue instanceof String) {
											if (isValidDate((String) cellValue)) {
												Date date = sdf.parse((String) cellValue);
												outputCell.setCellValue(date);
												if (((String) cellValue).contains("1899-12-31T")) {
													String timeStr = ((String) cellValue).substring(11, 19);
													Double timeDouble = DateUtil.convertTime(timeStr);
													outputCell.setCellValue(timeDouble);
													outputCell.setCellStyle(timeCellStyle);
												} else {
													outputCell.setCellStyle(dateCellStyle);
												}

											} else {
												outputCell.setCellValue((String) cellValue);
											}

//                                            outputCell.setCellType(CellType._NONE);
										} else if (cellValue instanceof Number) {
											outputCell.setCellValue(Double.valueOf(String.valueOf(cellValue)));
										} else if (cellValue instanceof Boolean) {
											outputCell.setCellValue((Boolean) cellValue);
										} else {
											outputCell.setCellValue(String.valueOf(cellValue));
										}

									} else {
										outputCell.setCellValue("");
									}

								} catch (Exception e) {
									outputCell.setCellValue("");
									continue;
								}

							}
						}

					}
				}
				outs = new FileOutputStream(fileName);
				outputWb.write(outs);
				outs.close();
				try {
//                    dashBoardsDAO.saveUserFiles(request, originalFileName, mainFileName, filePath, selectedFiletype);
				} catch (Exception e) {
				}
				String gridId = "divGrid-" + mainFileName.replace("." + selectedFiletype, "");
				gridId = gridId.replace(".csv", "");

				importResult = getFileObjectMetaData(request, response, fileName, gridId, selectedFiletype,
						mainFileName);
				importResult.put("fileExist", dashBoardsDAO.checkExistMergeTableName(request));

			}
			// return result1;
			if (inputStream != null) {
				inputStream.close();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return importResult;
	}

	public boolean isNumeric(String strNum) {
		if (strNum == null) {
			return false;
		}
		try {
			strNum = strNum.replace(",", "");
			double d = Double.parseDouble(strNum);
		} catch (NumberFormatException nfe) {
			return false;
		}
		return true;
	}

	public boolean isValidDate(String dateStr) {
		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
//        sdf.setLenient(false);
		try {
			sdf.parse(dateStr);
		} catch (ParseException e) {
			return false;
		}
		return true;
	}

	public boolean isBooleanValue(String str) {
		if (str == null) {
			return false;
		}
		if ("TRUE".equalsIgnoreCase(str) || "FALSE".equalsIgnoreCase(str)) {
			return true;
		} else {
			return false;
		}

	}

	public JSONObject getFileObjectMetaData(HttpServletRequest request, HttpServletResponse response, String filePath,
			String gridId, String fileType, String fileFolderPath) {
		JSONObject fileMetaObj = new JSONObject();
		try {
			JSONArray dataFieldsArray = new JSONArray();
			JSONArray columnsArray = new JSONArray();

			List<String> headers = getHeadersOfImportedFile(request, filePath);
			headers = dashBoardUtills.fileHeaderValidations(headers);
			Map<String, String> headerDataTypes = dashBoardsDAO.getDataTypesOFHeader(request);
			if (!(headers != null && !headers.isEmpty())) {
				String fileHeadersStr = request.getParameter("fileHeaders");
				if (fileHeadersStr != null && !"".equalsIgnoreCase(fileHeadersStr)
						&& !"[]".equalsIgnoreCase(fileHeadersStr)) {
					JSONObject fileHeaders = (JSONObject) JSONValue.parse(fileHeadersStr);
					headers = new ArrayList(fileHeaders.values());
				}

			}
			String gridPersonalizeStr = "";
			if (headers != null && !headers.isEmpty()) {
				List<String> columnList = new ArrayList();
				if (!(headers.contains("AUDIT_ID") || headers.contains("Audit_Id") || headers.contains("audit_id"))) {
					headers.add("AUDIT_ID");
				}
				for (int i = 0; i < headers.size(); i++) {
					String header = headers.get(i);
					if (header != null && !"".equalsIgnoreCase(header) && !"".equalsIgnoreCase(header)) {
						header = header.toUpperCase();
						String colLabel = header.toLowerCase().replace("_", " ");
						String headerText = Stream.of(colLabel.trim().split("\\s")).filter(word -> word.length() > 0)
								.map(word -> word.substring(0, 1).toUpperCase() + word.substring(1))
								.collect(Collectors.joining(" "));
						JSONObject dataFieldsObj = new JSONObject();
						columnList.add(header.replaceAll("\\s", "_"));
						gridPersonalizeStr += "<tr>" + "<td>" + header + "</td>" + "<td>"
								+ "<input type='checkbox' data-gridid='" + gridId + "' checked id='" + gridId + "_"
								+ header.replaceAll("\\s", "_") + "_DISPLAY' data-type='display' " + " data-colname='"
								+ header.replaceAll("\\s", "_") + "' onchange=\"updateETLPersonalize(id)\"" + "</td>"
								+ "<td>" + "<input type='checkbox' id='" + gridId + "_" + header.replaceAll("\\s", "_")
								+ "_FREEZE' data-gridid='" + gridId + "' data-type='pinned' " + " data-colname='"
								+ header.replaceAll("\\s", "_") + "' onchange=\"updateETLPersonalize(id)\"" + "</td>"
								+ "</tr>";
						dataFieldsObj.put("name", header.replaceAll("\\s", "_").replaceAll("[^a-zA-Z0-9_]", "_"));
						dataFieldsObj.put("type", headerDataTypes.get(header.replaceAll("[^a-zA-Z0-9_]", "_")));

						dataFieldsArray.add(dataFieldsObj);

						JSONObject columnsObject = new JSONObject();

						columnsObject.put("text", headerText);
						columnsObject.put("datafield", header.replaceAll("\\s", "_").replaceAll("[^a-zA-Z0-9_]", "_"));
						columnsObject.put("width", 120);
						columnsArray.add(columnsObject);

					}
				}
				gridPersonalizeStr = "<div class=\"personaliseoption visionSearchPersonaliseoption\" style=\"margin-top:5px;\">"
						+ "<div onclick=slideSettingsETL('" + gridId + "') class=\"layoutoptions ui-accordion\">"
						+ "<h3 class=\"ui-accordion-header1\"><span class=\"ui-accordion-header-icon ui-icon1 "
						+ " ui-icon-triangle-1-e ui-icon-triangle-1-s\" id=\"" + gridId + "_personalizeid\"></span>"
						+ "<img alt=\"\" class=\"navIcon gear\" src=\"images/f_spacer.gif\">Personalize</h3>"
						+ "</div><div id=\"" + gridId
						+ "_settings_panel\" class=\"VisionETLSettings_panel\" style=\"display: none;\">"
						+ "<div class=\"personalize\" id=\"" + gridId
						+ "_personalize_fields\"> <div class=\"pers_content\">"
						+ " <div id=\"tg-wrap4\" class=\"VisionETL-tg-wrap visionSearchPersonalise\"> "
						+ "<div class=\"visionPersonaliseSticky\"> <div class=\"sticky-wrap\"> "
						+ " <div class=\"sticky-wrap\">"
						+ "<table class=\"personalize_tbl sticky-enabled\" id=\"pers_criteria\" style=\"margin: 0px; width: 100%;\"> "
						+ "  <thead> <tr style=\"\"><th>Parameter</th><th>Display</th><th>Freeze</th>	   </tr>   </thead>  "
						+ " <tbody>" + gridPersonalizeStr + "</tbody>"
						+ "</table></div></div></div></div></div></div></div></div>";

				// ravi multiple excelsheets sheets
				if (fileType != null && ("XLS".equalsIgnoreCase(fileType.toUpperCase())
						|| "XLSX".equalsIgnoreCase(fileType.toUpperCase()))) {
					Workbook workBook = WorkbookFactory.create(new File(filePath));
					int sheetCount = workBook.getNumberOfSheets();
					if (sheetCount > 1) {
						String navDiv = "<div id='navBar_" + gridId + "'><ul style='width: fit-content;'>";

						for (int i = 0; i < sheetCount; i++) {
							navDiv += "<li width='70px' >" + workBook.getSheetName(i) + "</li>";
						}
						navDiv += "</ul></div>";
						fileMetaObj.put("navigationDiv", navDiv);
					}
				}
				fileMetaObj.put("gridPersonalizeStr", gridPersonalizeStr);
				fileMetaObj.put("dataFieldsArray", dataFieldsArray);
				fileMetaObj.put("columnsArray", columnsArray);
				fileMetaObj.put("columnList", columnList);
				fileMetaObj.put("gridId", gridId);
				fileMetaObj.put("filePath", fileFolderPath);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return fileMetaObj;
	}

	public List getHeadersOfImportedFile(HttpServletRequest request, String filePath) {
		List<String> headers = new ArrayList<String>();
		Map<String, Integer> headerCount = new HashMap();
		try {
			if (filePath != null && !"".equalsIgnoreCase(filePath) && !"null".equalsIgnoreCase(filePath)) {
				String fileExt = filePath.substring(filePath.lastIndexOf(".") + 1, filePath.length());

				if (fileExt != null && !"".equalsIgnoreCase(fileExt)) {
					if ("txt".equalsIgnoreCase(fileExt) || "csv".equalsIgnoreCase(fileExt)) {

						CsvParserSettings settings = new CsvParserSettings();
						settings.detectFormatAutomatically();

						CsvParser parser = new CsvParser(settings);
						List<String[]> rows = parser.parseAll(new File(filePath));

						// if you want to see what it detected
//                        CsvFormatDetector formatdetect =  new CsvFormatDetector();
						CsvFormat format = parser.getDetectedFormat();
						char columnSeparator = format.getDelimiter();

						String fileType = request.getParameter("fileType");
//                        char columnSeparator = '\t';
//                        char columnSeparator = ',';
						if (!(fileType != null && !"".equalsIgnoreCase(fileType)
								&& !"null".equalsIgnoreCase(fileType))) {
							fileType = (String) request.getAttribute("fileType");
						}
						if ("json".equalsIgnoreCase(fileType)) {
							columnSeparator = ',';
						}

						CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(filePath), "UTF8"),
								columnSeparator);

						String[] nextLine;
//						while ((nextLine = reader.readNext()) != null) {
//							if (nextLine.length != 0 && nextLine[0].contains("" + columnSeparator)) {
//								headers = new ArrayList<>(Arrays.asList(nextLine[0].split("" + columnSeparator)));
//							} else {
//								headers = new ArrayList<>(Arrays.asList(nextLine));
//							}
//
//							break;
//						}
//						boolean containsAuditId = headers.stream()
//								.anyMatch(header -> header.equalsIgnoreCase("audit_id"));
//
//						if (!containsAuditId) {
//							headers.add("AUDIT_ID");
//						}
						while ((nextLine = reader.readNext()) != null) {
							for (String header : nextLine) {
								if (headerCount.containsKey(header.toLowerCase())) {
									int count = headerCount.get(header.toLowerCase());
									count++;
									headerCount.put(header.toLowerCase(), count);
									if (count == 2) {
										header = header + "_1";
									} else {
										header = header + "_" + (count - 1);
									}
								} else {
									headerCount.put(header.toLowerCase(), 1);
								}
								headers.add(header);
							}
							break;
						}
					} else if ("xls".equalsIgnoreCase(fileExt) || "xlsx".equalsIgnoreCase(fileExt)) {
						headers = new ArrayList<>();
						Workbook workBook = null;
						Sheet sheet = null;

						// PKH sheet Header
						String sheetNum = request.getParameter("sheetNo");
						int sheetNo = (sheetNum != null && !"".equalsIgnoreCase(sheetNum))
								? (Integer.parseInt(sheetNum))
								: 0;
						// PKH sheet Header

//                        if (fileExt != null && "xls".equalsIgnoreCase(fileExt)) { // commented by PKH
//                            workBook = WorkbookFactory.create(new File(filePath));
//                            sheet = (HSSFSheet) workBook.getSheetAt(sheetNo);
//                        } else {
						System.out.println("Before::::" + new Date());
//                fis = new FileInputStream(new File(filepath));              
//                XSSFWorkbook xssfWb = (XSSFWorkbook) new XSSFWorkbook(fis);
						workBook = WorkbookFactory.create(new File(filePath));
						System.out.println("After::fileInputStream::" + new Date());
						sheet = (XSSFSheet) workBook.getSheetAt(sheetNo);
//                sheet = (XSSFSheet) xssfWb.getSheetAt(0);
//                        }
						if (sheet != null) {
							Row row = sheet.getRow(0);
							if (row != null) {
								for (int j = 0; j < row.getLastCellNum(); j++) {
									// System.out.println("Cell Num:::" + j + ":::Start Date And Time :::" + new
									// Date());

									try {
										Cell cell = row.getCell(j);

										if (cell != null) {
											switch (cell.getCellType()) {
											case Cell.CELL_TYPE_STRING:
												headers.add(cell.getStringCellValue());
												break;
											case Cell.CELL_TYPE_BOOLEAN:
//                                rowObj.put(header, hSSFCell.getBooleanCellValue());
												break;
											case Cell.CELL_TYPE_NUMERIC:

												if (HSSFDateUtil.isCellDateFormatted(cell)) {
													String cellDateString = "";
													Date cellDate = cell.getDateCellValue();
													if ((cellDate.getYear() + 1900) == 1899
															&& (cellDate.getMonth() + 1) == 12
															&& (cellDate.getDate()) == 31) {
														cellDateString = (cellDate.getHours()) + ":"
																+ (cellDate.getMinutes()) + ":"
																+ (cellDate.getSeconds());
//                                                    System.out.println("cellDateString :: "+cellDateString);
													} else {
														cellDateString = (cellDate.getYear() + 1900) + "-"
																+ (cellDate.getMonth() + 1) + "-"
																+ (cellDate.getDate());
													}

//                                                        String cellDateString = (cellDate.getYear() + 1900) + "-" + (cellDate.getMonth() + 1) + "-" + (cellDate.getDate());
													headers.add(cellDateString);
//                                            
												} else {
													String cellvalStr = NumberToTextConverter
															.toText(cell.getNumericCellValue());
													headers.add(cellvalStr);
												}
												break;
											case Cell.CELL_TYPE_BLANK:
												headers.add("");
												break;
											}

										} else {
											headers.add("");
//                            testMap.put(stmt, "");
										}
									} catch (Exception e) {
										e.printStackTrace();
										headers.add("");
										continue;
									}

								} // end of row cell loop
							}
						}

					} else if ("xml".equalsIgnoreCase(fileExt)) {
						headers = new ArrayList<>();
						DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
						DocumentBuilder builder = factory.newDocumentBuilder();
						Document document = builder.parse(new File(filePath));
						document.getDocumentElement().normalize();
						Element root = document.getDocumentElement();
						if (root.hasChildNodes() && root.getChildNodes().getLength() > 1) {
							String evaluateTagName = "/" + root.getTagName();
							NodeList rootList = root.getChildNodes();
							if (!"#Text".equalsIgnoreCase(rootList.item(0).getNodeName())) {
								evaluateTagName += "/" + rootList.item(0).getNodeName();
							} else {
								evaluateTagName += "/" + rootList.item(1).getNodeName();
							}

							System.out.println("evaluateTagName:::" + evaluateTagName);
							XPath xpath = XPathFactory.newInstance().newXPath();
							NodeList dataNodeList = (NodeList) xpath.evaluate(evaluateTagName,
									// NodeList nList = (NodeList) xpath.evaluate("/PiLog_Data_Export/Item",
									document, XPathConstants.NODESET);
							if (dataNodeList != null && dataNodeList.getLength() != 0) {
								int rowCount = dataNodeList.getLength();
								Node node = dataNodeList.item(0);
								if (node.getNodeType() == Node.ELEMENT_NODE) {
									NodeList childNodeList = node.getChildNodes();
									for (int i = 0; i < childNodeList.getLength(); i++) {// Columns

										Node childNode = childNodeList.item(i);
										if (childNode != null && !"#Text".equalsIgnoreCase(childNode.getNodeName())) {
											headers.add(childNode.getNodeName());
											System.err.println(
													childNode.getNodeName() + "---> " + childNode.getTextContent());
										}

									} // end of columns loop

								}

							}

						}
					} else {
						if ("json".equalsIgnoreCase(fileExt)) {
							ObjectMapper objectMapper = new ObjectMapper();
							JsonNode rootNode = objectMapper.readTree(new File(filePath));
							headers = extractJSONColumnNames(rootNode);
						}
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return headers;
	}

	private static List extractJSONColumnNames(JsonNode jsonNode) {
		List<String> columnNames = new ArrayList<>();
		if (jsonNode != null && jsonNode.isArray()) {
			for (JsonNode node : jsonNode) {
				Iterator<Map.Entry<String, JsonNode>> fields1 = node.fields();
				while (fields1.hasNext()) {
					Map.Entry<String, JsonNode> field2 = fields1.next();
					columnNames.add(field2.getKey());
				}
				break;
			}
		} else {
			if (jsonNode != null && jsonNode.isObject()) {
				Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
				while (fields.hasNext()) {
					Map.Entry<String, JsonNode> field = fields.next();
					columnNames.add(field.getKey());
				}
			}
		}

		return columnNames;
	}

	public List getFileObjectData(HttpServletRequest request, HttpServletResponse response) {
		List dataList = new ArrayList();
		try {
			String filePath = request.getParameter("filePath");
			if (filePath != null && !"".equalsIgnoreCase(filePath)) {
				String targetFile = request.getParameter("targetFile"); // ravi etl new issues
				if ("Y".equalsIgnoreCase(targetFile)) {
					filePath = "C:/ETL_EXPORT_" + File.separator + request.getSession(false).getAttribute("ssUsername")
							+ File.separator + filePath;
				} else {
					filePath = fileStoreHomedirectory + "TreeDMImport/"
							+ request.getSession(false).getAttribute("ssUsername") + File.separator + filePath;

				}
				// C:/Files/TreeDMImport/SAN_MGR_MM
//                filePath = "C:/Files/TreeDMImport" + File.separator + request.getSession(false).getAttribute("ssUsername") + File.separator + filePath;
			}
			String fileName = request.getParameter("fileName");
			String fileType = request.getParameter("fileType");
			if (fileType != null && !"".equalsIgnoreCase(fileType) && !fileType.startsWith(".")) {
				fileType = "." + fileType;
			}
			String columnsArray = request.getParameter("columnsArray");
			List<String> columnList = new ArrayList<>();
			if (columnsArray != null && !"".equalsIgnoreCase(columnsArray) && !"null".equalsIgnoreCase(columnsArray)) {
				columnList = (List<String>) JSONValue.parse(columnsArray);
			}
			if (".xls".equalsIgnoreCase(fileType) || ".xlsx".equalsIgnoreCase(fileType)) {
				dataList = readExcel(request, response, filePath, columnList);
			} else if (".CSV".equalsIgnoreCase(fileType) || ".TXT".equalsIgnoreCase(fileType)
					|| ".JSON".equalsIgnoreCase(fileType)) {
				dataList = readCSV(request, response, filePath, columnList);
			} else if (".xml".equalsIgnoreCase(fileType)) {
				dataList = readXML(request, response, filePath, columnList);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return dataList;
	}

	public List readExcel(HttpServletRequest request, HttpServletResponse response, String filepath,
			List<String> columnList) {

		FileInputStream fis = null;

		System.out.println("Start Date And Time :::" + new Date());
		List dataList = new ArrayList();
		int rowVal = 1;
		try {
			if (true) {
				// fis = new FileInputStream(new File(filepath));

				Workbook workBook = null;
				Sheet sheet = null;
				String sheetNum = request.getParameter("sheetNo");// ravi multiple excel sheet

				int sheetNo = (sheetNum != null && !"".equalsIgnoreCase(sheetNum)) ? (Integer.parseInt(sheetNum)) : 0;// ravi
																														// multiple
																														// excel
																														// sheet

				String fileExtension = filepath.substring(filepath.lastIndexOf(".") + 1, filepath.length());
				System.out.println("fileExtension:::" + fileExtension);
//                if (fileExtension != null && "xls".equalsIgnoreCase(fileExtension)) { //commented by PKH
//                    workBook = WorkbookFactory.create(new File(filepath));
//                    sheet = (HSSFSheet) workBook.getSheetAt(sheetNo);
//                } else {
				System.out.println("Before::::" + new Date());
				workBook = WorkbookFactory.create(new File(filepath));
				System.out.println("After::fileInputStream::" + new Date());
				sheet = (XSSFSheet) workBook.getSheetAt(sheetNo);
//                }
				int lastRowNo = sheet.getLastRowNum();
				System.out.println("lastRowNo::::" + lastRowNo);
				int firstRowNo = sheet.getFirstRowNum();
				System.out.println("firstRowNo::::" + firstRowNo);
				int rowCount = lastRowNo - firstRowNo;
				System.out.println("rowCount:::::" + rowCount);

				int stmt = 1;
				String strToDateCol = "";
				String pagenum = request.getParameter("pagenum");
				String pagesize = request.getParameter("pagesize") != null ? request.getParameter("pagesize") : "10";
				String recordendindex = request.getParameter("recordendindex");
				String recordstartindex = (request.getParameter("recordstartindex"));
				Integer filterscount = 0;
				if (request.getParameter("filterscount") != null) {
					filterscount = new Integer(request.getParameter("filterscount"));
				}
				String sortdatafield = request.getParameter("sortdatafield");
				System.out.println("sortdatafield::::" + sortdatafield);
				String sortorder = request.getParameter("sortorder");
				if (!(sortdatafield != null && !"".equalsIgnoreCase(sortdatafield))) {
					sortdatafield = (String) request.getAttribute("sortdatafield");
				}
				if (!(sortorder != null && !"".equalsIgnoreCase(sortorder))) {
					sortorder = (String) request.getAttribute("sortorder");
				}

				rowVal = 1;
				if (recordstartindex != null && !"".equalsIgnoreCase(recordstartindex)
						&& !"null".equalsIgnoreCase(recordstartindex) && !"0".equalsIgnoreCase(recordstartindex)) {
					rowVal = Integer.parseInt(recordstartindex);
				}
				int endIndex = rowCount + 1;
				if (recordendindex != null && !"".equalsIgnoreCase(recordendindex)
						&& !"null".equalsIgnoreCase(recordendindex) && Integer.parseInt(recordendindex) <= rowCount) {
					endIndex = Integer.parseInt(recordendindex) + 1;
				}
				System.out.println("endIndex:::::" + endIndex);
				for (int i = rowVal; i < endIndex; i++) {
					Row row = sheet.getRow(i);
					stmt = 1;
					JSONObject dataObject = new JSONObject();
					dataObject.put("totalrecords", rowCount);
					for (int cellIndex = 0; cellIndex < row.getLastCellNum(); cellIndex++) {

						try {
//                            System.out.println("cellIndex::::" + cellIndex);
							Cell cell = row.getCell(cellIndex);
							if (cell != null) {
								switch (cell.getCellType()) {
								case Cell.CELL_TYPE_STRING:
									String cellValue = cell.getStringCellValue();
									if (cellValue != null && !"".equalsIgnoreCase(cellValue)
											&& !"null".equalsIgnoreCase(cellValue)) {
										dataObject.put(columnList.get(cellIndex), cellValue);
									} else {
										dataObject.put(columnList.get(cellIndex), "");
									}

									break;
								case Cell.CELL_TYPE_BOOLEAN:
//                                rowObj.put(header, hSSFCell.getBooleanCellValue());
									break;
								case Cell.CELL_TYPE_NUMERIC:

									if (HSSFDateUtil.isCellDateFormatted(cell)) {
										if (strToDateCol != null && !"".equalsIgnoreCase(strToDateCol)
												&& !"null".equalsIgnoreCase(strToDateCol)
												&& strToDateCol.contains(String.valueOf(stmt))) {
											DateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
											Date convertedDate = (Date) formatter.parse(cell.toString());
											dataObject.put(columnList.get(cellIndex), cell.toString());

//                                            testMap.put(stmt, sqlDat);
										} else {
											String cellDateString = "";
											Date cellDate = cell.getDateCellValue();
											if ((cellDate.getYear() + 1900) == 1899 && (cellDate.getMonth() + 1) == 12
													&& (cellDate.getDate()) == 31) {
												cellDateString = (cellDate.getHours()) + ":" + (cellDate.getMinutes())
														+ ":" + (cellDate.getSeconds());
//                                                    System.out.println("cellDateString :: "+cellDateString);
											} else {
												cellDateString = (cellDate.getYear() + 1900) + "-"
														+ (cellDate.getMonth() + 1) + "-" + (cellDate.getDate());
											}

//                                                String cellDateString = (cellDate.getYear() + 1900) + "-" + (cellDate.getMonth() + 1) + "-" + (cellDate.getDate());
											dataObject.put(columnList.get(cellIndex), cellDateString);
										}

									} else {
										String cellvalStr = NumberToTextConverter.toText(cell.getNumericCellValue());
										dataObject.put(columnList.get(cellIndex), cellvalStr);
									}
									break;
								case Cell.CELL_TYPE_BLANK:
									dataObject.put(columnList.get(cellIndex), "");
									break;
								}

							} else {
								dataObject.put(columnList.get(cellIndex), "");
							}
						} catch (Exception e) {
							dataObject.put(columnList.get(cellIndex), "");
							continue;
						}

					} // end of row cell loop
					dataList.add(dataObject);
				} // row end

				// return result1;
				if (fis != null) {
					fis.close();
				}
			}

		} catch (Exception e) {
			e.printStackTrace();

		}

		return dataList;
	}

	public List readCSV(HttpServletRequest request, HttpServletResponse response, String filepath,
			List<String> columnList) {
		FileInputStream fis = null;
		System.out.println("Start Date And Time :::" + new Date());
		List dataList = new ArrayList();
		int rowVal = 1;
		try {
			int rowCount = 0;
			// fis = new FileInputStream(new File(filepath));
			String fileType = request.getParameter("fileType");
			String fileExtension = filepath.substring(filepath.lastIndexOf(".") + 1, filepath.length());
			System.out.println("fileExtension:::" + fileExtension);

			int stmt = 1;
			String strToDateCol = "";
//            char colSepartor = '\t';

			CsvParserSettings settings = new CsvParserSettings();
			settings.detectFormatAutomatically();

			CsvParser parser = new CsvParser(settings);
			List<String[]> rows = parser.parseAll(new File(filepath));

			// if you want to see what it detected
//                        CsvFormatDetector formatdetect =  new CsvFormatDetector();
			CsvFormat format = parser.getDetectedFormat();
			char colSepartor = format.getDelimiter();

//            char colSepartor = ',';
			if (".JSON".equalsIgnoreCase(fileType) || "json".equalsIgnoreCase(fileType)) {
				colSepartor = ',';
			}
			// need to write logic for extraction from File
			CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(filepath), "UTF8"), colSepartor);
			LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(filepath));
			lineNumberReader.skip(Long.MAX_VALUE);
			long totalRecords = lineNumberReader.getLineNumber();
			if (totalRecords != 0) {
				totalRecords = totalRecords - 1;
			}
			System.out.println("totalRecords:::" + totalRecords);
//             CSVReader  reader = new CSVReader(new FileReader(filepath),'\t');
			String pagenum = request.getParameter("pagenum");
			String pagesize = request.getParameter("pagesize") != null ? request.getParameter("pagesize") : "10";
			String recordendindex = request.getParameter("recordendindex");
			String recordstartindex = (request.getParameter("recordstartindex"));
			rowVal = 1;

			if (recordstartindex != null && !"".equalsIgnoreCase(recordstartindex)
					&& !"null".equalsIgnoreCase(recordstartindex) && !"0".equalsIgnoreCase(recordstartindex)) {
				rowVal = Integer.parseInt(recordstartindex);
			}
//            int endIndex = (int)totalRecords + 1;
//            if (recordendindex != null
//                    && !"".equalsIgnoreCase(recordendindex)
//                    && !"null".equalsIgnoreCase(recordendindex)
//                    && Integer.parseInt(recordendindex) <= rowCount) {
//                endIndex = Integer.parseInt(recordendindex);
//            }
			int skipLines = 0;
			if (pagenum != null && !"".equalsIgnoreCase(pagenum) && !"null".equalsIgnoreCase(pagenum)
					&& !"0".equalsIgnoreCase(pagenum) && pagesize != null && !"".equalsIgnoreCase(pagesize)
					&& !"null".equalsIgnoreCase(pagesize)) {
				skipLines = Integer.parseInt(pagenum) * Integer.parseInt(pagesize);
			}
			if (skipLines == 0) {
				String[] headers = reader.readNext();
			}
			reader.skip(skipLines);

			String[] nextLine;
			int rowsCount = 1;
			while ((nextLine = reader.readNext()) != null) {// no of rows
				if (Integer.parseInt(pagesize) >= rowsCount) {
					rowsCount++;

					JSONObject dataObject = new JSONObject();
					dataObject.put("totalrecords", totalRecords);
					for (int j = 0; j < columnList.size(); j++) {
						try {
							int cellIndex = j;
							if (cellIndex <= (nextLine.length - 1)) {
								String token = nextLine[cellIndex];
								if (token != null && !"".equalsIgnoreCase(token)) {
									try {
										dataObject.put(columnList.get(j), token);
									} catch (Exception e) {
										dataObject.put(columnList.get(j), "");
										continue;
									}
								} else {
									dataObject.put(columnList.get(j), "");
								}
							} else {
								dataObject.put(columnList.get(j), "");
							}
						} catch (Exception e) {
							dataObject.put(columnList.get(j), "");
							continue;
						}

					}

					dataList.add(dataObject);
				} else {
					break;
				}

			}

			if (fis != null) {
				fis.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return dataList;
	}
//

	public List readXML(HttpServletRequest request, HttpServletResponse response, String filepath,
			List<String> columnList) {
		FileInputStream fis = null;
		List dataList = new ArrayList();
		try {
			int rowCount = 0;
			String fileExtension = filepath.substring(filepath.lastIndexOf(".") + 1, filepath.length());
			System.out.println("fileExtension:::" + fileExtension);

			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document document = builder.parse(new FileInputStream(filepath), "UTF-8");
			document.getDocumentElement().normalize();
			Element root = document.getDocumentElement();

			if (root.hasChildNodes() && root.getChildNodes().getLength() > 1) {
				// nested childs
				String evaluateTagName = "/" + root.getTagName();
				NodeList rootList = root.getChildNodes();
				if (!"#Text".equalsIgnoreCase(rootList.item(0).getNodeName())) {
					evaluateTagName += "/" + rootList.item(0).getNodeName();
				} else {
					evaluateTagName += "/" + rootList.item(1).getNodeName();
				}

				System.out.println("evaluateTagName:::" + evaluateTagName);
				XPath xpath = XPathFactory.newInstance().newXPath();
				NodeList dataNodeList = (NodeList) xpath.evaluate(evaluateTagName,
						// NodeList nList = (NodeList) xpath.evaluate("/PiLog_Data_Export/Item",
						document, XPathConstants.NODESET);

				if (dataNodeList != null && dataNodeList.getLength() != 0) {
					rowCount = dataNodeList.getLength();
					String pagenum = request.getParameter("pagenum");
					String pagesize = request.getParameter("pagesize") != null ? request.getParameter("pagesize")
							: "10";
					String recordendindex = request.getParameter("recordendindex");
					String recordstartindex = (request.getParameter("recordstartindex"));
					// need to write logic for extraction from File
					int startIndex = 0;
					if (recordstartindex != null && !"".equalsIgnoreCase(recordstartindex)
							&& !"null".equalsIgnoreCase(recordstartindex) && !"0".equalsIgnoreCase(recordstartindex)) {
						startIndex = Integer.parseInt(recordstartindex);
					}
					int endIndex = rowCount;
					if (recordendindex != null && !"".equalsIgnoreCase(recordendindex)
							&& !"null".equalsIgnoreCase(recordendindex)
							&& Integer.parseInt(recordendindex) <= rowCount) {
						endIndex = Integer.parseInt(recordendindex);
					}
					int skipLines = 0;
					if (pagenum != null && !"".equalsIgnoreCase(pagenum) && !"null".equalsIgnoreCase(pagenum)
							&& !"1".equalsIgnoreCase(pagenum) && pagesize != null && !"".equalsIgnoreCase(pagesize)
							&& !"null".equalsIgnoreCase(pagesize)) {
						skipLines = Integer.parseInt(pagenum) * Integer.parseInt(pagesize);
					}

					for (int temp = startIndex; temp < endIndex; temp++) {// Rows
						Node node = dataNodeList.item(temp);
						JSONObject dataObject = new JSONObject();
						dataObject.put("totalrecords", rowCount);
						if (node.getNodeType() == Node.ELEMENT_NODE) {
							NodeList childNodeList = node.getChildNodes();
							for (int j = 0; j < columnList.size(); j++) {
								try {
									int childNodeIndex = j;
									int nodeListLength = childNodeList.getLength();
									if (childNodeIndex <= (childNodeList.getLength() - 1)) {
										Node childNode = childNodeList.item(childNodeIndex);
										if (childNode != null) {
											if (childNode != null && childNode.getNodeType() == Node.ELEMENT_NODE) {
												try {
													if (childNode.getTextContent() != null
															&& !"".equalsIgnoreCase(childNode.getTextContent())
															&& !"null".equalsIgnoreCase(childNode.getTextContent())) {
														dataObject.put(columnList.get(j), childNode.getTextContent());

													} else {
														dataObject.put(columnList.get(j), "");
													}

												} catch (Exception e) {
													dataObject.put(columnList.get(j), "");
													continue;
												}
												// Need to set the Data

											}
										} else {
											dataObject.put(columnList.get(j), "");
										}
									} else {
										dataObject.put(columnList.get(j), "");
									}
								} catch (Exception e) {
									dataObject.put(columnList.get(j), "");
									continue;
								}

							} // column list loop

						}
						dataList.add(dataObject);
					} // end of rows loop

				}
			} else {
				System.err.println("*** Root Element Not Found ****");
			}

			if (fis != null) {
				fis.close();
			}
		} catch (Exception e) {
			e.printStackTrace();

		}

		return dataList;
	}

	public String getLoadTableColumns(HttpServletRequest request) {
		return dashBoardsDAO.getLoadTableColumns(request);
	}

	public JSONObject fetchChartData(HttpServletRequest request) {
		JSONObject chartObj = new JSONObject();
		try {
			String chartType = request.getParameter("chartType");
			if (chartType != null && !"".equalsIgnoreCase(chartType) && "heatMap".equalsIgnoreCase(chartType)) {
				chartObj = dashBoardsDAO.fetchHeatMapEChartData(request);
			} else if (chartType != null && !"".equalsIgnoreCase(chartType) && "sunburst".equalsIgnoreCase(chartType)) {
				chartObj = dashBoardsDAO.fetchSunbrstEChartData(request);
			} else if (chartType != null && !"".equalsIgnoreCase(chartType) && "geochart".equalsIgnoreCase(chartType)) {
				chartObj = dashBoardsDAO.fetchGeoChartData(request);
			} else if (chartType != null && !"".equalsIgnoreCase(chartType)
					&& "geoLatLangchart".equalsIgnoreCase(chartType)) {
				chartObj = dashBoardsDAO.fetchGeoLatLangchartData(request);
			} else if (chartType != null && !"".equalsIgnoreCase(chartType)
					&& "BarAndLine".equalsIgnoreCase(chartType)) {
				chartObj = dashBoardsDAO.fetchBarwithLineEChartData(request);
			} else if (chartType != null && !"".equalsIgnoreCase(chartType) && "treemap".equalsIgnoreCase(chartType)) {
				chartObj = dashBoardsDAO.fetchTreeMapEChartData(request);
			} else if (chartType != null && !"".equalsIgnoreCase(chartType) && "boxplot".equalsIgnoreCase(chartType)) {
				chartObj = dashBoardsDAO.fetchBoxPlotChartData(request);
			} else if (chartType != null && !"".equalsIgnoreCase(chartType) && "sankey".equalsIgnoreCase(chartType)) {
				chartObj = dashBoardsDAO.fetchSankeyChartData(request);
			} else if (chartType != null && !"".equalsIgnoreCase(chartType)
					&& "horizontal_bar".equalsIgnoreCase(chartType)) {
				chartObj = dashBoardsDAO.fetchHorizontalBarChartData(request);
			} else if (chartType != null && !"".equalsIgnoreCase(chartType)
					&& "ganttChart".equalsIgnoreCase(chartType)) {
				chartObj = dashBoardsDAO.fetchGanttChartData(request);
			} else if (chartType != null && !"".equalsIgnoreCase(chartType)
					&& "stackedBarChart".equalsIgnoreCase(chartType)) {
				chartObj = dashBoardsDAO.fetchStackedBarChartData(request);
			} else if (chartType != null && !"".equalsIgnoreCase(chartType)
					&& "decompositionTree".equalsIgnoreCase(chartType)) {
				//chartObj = v10GenericDxpTreeService.getEchartsDecompositionTreeData(request);
			} else if (chartType != null && !"".equalsIgnoreCase(chartType)
					&& "candlestick".equalsIgnoreCase(chartType)) {
				chartObj = dashBoardsDAO.getEchartsCandlestickData(request);
			} else if (chartType != null && !"".equalsIgnoreCase(chartType) && "kpiChart".equalsIgnoreCase(chartType)) {
				chartObj = dashBoardsDAO.fetchKpiChartData(request);
			} else {
				chartObj = dashBoardsDAO.fetchChartData(request);
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return chartObj;
	}

	public JSONObject fetchFiltersValues(HttpServletRequest request) {
		return dashBoardsDAO.fetchFiltersValues(request);
	}

	public JSONObject fetchSlicerValues(HttpServletRequest request) {
		return dashBoardsDAO.fetchSlicerValues(request);
	}

	public JSONObject fetchSlicerButtonValues(HttpServletRequest request) {
		return dashBoardsDAO.fetchSlicerButtonValues(request);
	}

	public JSONObject fetchSlicerListValues(HttpServletRequest request) {
		return dashBoardsDAO.fetchSlicerListValues(request);
	}

	public JSONObject fetchSlicerDropdownValues(HttpServletRequest request) {
		return dashBoardsDAO.fetchSlicerDropdownValues(request);
	}

	public JSONObject getChartFilters(HttpServletRequest request) {
		JSONObject jsonChartFilter = new JSONObject();
		String startUltag = "<ul class='conigProperties'>";
		String endUltag = "</ul>";
		String pieChart = startUltag;
		pieChart += getGeneralFilters("PIE");
		pieChart += pieDonutGeneralFilters("PIE");
		// pieChart += getLegendFilters("PIE");
		pieChart += getChartColors("PIE");
		pieChart += getChartHover("PIE", "layout");
		pieChart += endUltag;

		String gaugeChart = startUltag;
		gaugeChart += getIndicatorFilters("INDICATOR");
		gaugeChart += endUltag;

		String heatMap = startUltag;
		heatMap += getTitleFilters("HEATMAP");
		heatMap += getLabelAndHoverDataFiltersWithOutPercentage("HEATMAP");
		heatMap += endUltag;

		String donutChart = startUltag;
		donutChart += getGeneralFilters("DONUT");
		donutChart += pieDonutGeneralFilters("DONUT");
		// donutChart += getLegendFilters("DONUT");
		donutChart += getChartColors("DONUT");
		donutChart += getChartHover("DONUT", "layout");
		// need to fix
//		donutChart += "<li id=\"hole-filter\" data-column-name=\"DONUTHOLE\">" + "<div class=\"main-container\">"
//				+ "<div class=\"filter-container\">"
//				+ "<img src=\"images/down-chevron.png\" alt=\"Down Chevron\" class=\"icons visualDarkMode\"/>"
//				+ "<p>Hole Radius</p>" + "</div>" + getToggleButton("DONUT", "") + "</div>"
//				+ "<ul class=\"sub-filters\" id=\"DONUTHOLE\" style=\"display: none;\">"
//				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"DONUTHOLERADIUS\">"
//				+ "<p>Inner Radius</p>" + "<input type=\"text\"id=\"DONUTHOLERADIUS\" data-opt-name=\"hole\"/>"
//				+ "<input type=\"range\" data-opt-name=\"hole\"/>" + "</li>" + "</ul>" + "</li>";
		donutChart += endUltag;

		String barChart = startUltag;
		barChart += getGeneralFilters("BAR");
		barChart += getLabelAndHoverDataFilters("BAR");
		barChart += "<li class=\"general-filters active-filter\" data-column-name=\"BARMODE\" data-key-type=\"layout\">"
				+ "<div class=\"sub-filterItems\">" + "<p>Bar Mode</p>"
				+ "<select name=\"text-info\" id=\"BARMODE\" data-opt-name=\"barmode\">"
				+ "<option value=\"group\">Group</option>" + "<option value=\"stack\">Stack</option>"
				// + "<option value=\"overlay\">Overlay</option>" + "<option
				// value=\"relative\">Relative</option>"
				+ "</select>" + "</div>" + "</li>";
//        barChart += "<li class=\"general-filters active-filter\" data-column-name=\"BARGAP\" data-key-type=\"layout\">"
//                + "<div class=\"sub-filterItems\">"
//                + "<p>Bar Gap</p>"
//                + "<input type=\"number\" id=\"BARGAP\" data-opt-name=\"bargap\" data-man=\"O\" title=\"Title\"/>"
//                + "</div>"
//                + "</li>";
		barChart += getChartHover("BAR", "layout");
		barChart += getLegendFilters("BAR");
		barChart += getChartColors("BAR");
		barChart += getaxis("BAR", "X");
		barChart += getaxis("BAR", "Y");
		barChart += endUltag;

		String columnChart = startUltag;
		columnChart += getGeneralFilters("COLUMN");
		columnChart += getLabelAndHoverDataFilters("COLUMN");
		columnChart += "<li class=\"general-filters active-filter\" data-column-name=\"COLUMNMODE\" data-key-type=\"layout\">"
				+ "<div class=\"sub-filterItems\">" + "<p>Bar Mode</p>"
				+ "<select name=\"text-info\" id=\"COLUMNMODE\" data-opt-name=\"barmode\">"
				+ "<option value=\"stack\">Stack</option>" + "<option value=\"group\">Group</option>"
				// + "<option value=\"overlay\">Overlay</option>" + "<option
				// value=\"relative\">Relative</option>"
				+ "</select>" + "</div>" + "</li>";

		// need to fix
//		columnChart += "<li class=\"general-filters active-filter\" data-column-name=\"COLUMNGAP\" data-key-type=\"layout\">"
//				+ "<div class=\"sub-filterItems\">" + "<p>Bar Gap</p>"
//				+ "<input type=\"number\" id=\"COLUMNGAP\" data-opt-name=\"bargap\" data-man=\"O\" title=\"Title\"/>"
//				+ "</div>" + "</li>";
		columnChart += getChartHover("COLUMN", "layout");
		columnChart += getLegendFilters("COLUMN");
		columnChart += getChartColors("COLUMN");
		columnChart += getaxis("COLUMN", "X");
		columnChart += getaxis("COLUMN", "Y");
		columnChart += endUltag;

		String lineChart = startUltag;
		lineChart += getGeneralFilters("LINES");
		lineChart += "<li class=\"general-filters active-filter\" data-column-name=\"LINESMODE\" data-key-type=\"data\">"
				+ "<div class=\"sub-filterItems\">" + "<label>Mode</label>"
				+ "<select name=\"text-info\" id=\"LINESMODE\" data-opt-name=\"mode\">"
				+ "<option value=\"markers\">Markers</option>" + "<option value=\"lines\" selected>Lines</option>"
				+ "<option value=\"lines+markers\">Lines and Markers</option>"

				// need to fix
				// + "<option value=\"lines+text\">Lines and Text</option>"
				// + "<option value=\"lines+markers+text\">Lines, Markers and Text</option>"
				+ "</select>" + "</div>" + "</li>";
		lineChart += getLabelAndHoverDataFilters("LINES");
		lineChart += getChartHover("LINES", "data");
		lineChart += getLegendFilters("LINES");
		lineChart += getaxis("LINES", "X");
		lineChart += getaxis("LINES", "Y");
		lineChart += "<li id=\"marker-filter\" data-column-name=\"LINESMARKER\" data-key-type=\"data\">"
				+ "<div class=\"main-container\">" + "<div class=\"filter-container\">"
				+ "<img src=\"images/down-chevron.png\" alt=\"Down Chevron\" class=\"icons visualDarkMode\"/>"
				+ "<p>Chart Markers</p>" + "</div>" + getToggleButton("LINES", "") + "</div>"
				+ "<ul class=\"sub-filters\" id=\"LINESMARKER\" data-opt-name=\"marker\" style=\"display: none;\">"
				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"LINESCOLORSMARKER\" data-key-type=\"data\">"
				+ "<label>Marker Color</label>" + "<input type=\"hidden\" id=\"LINESCOLORSMARKER\" value=\"\">"
				+ "<input type=\"color\" id=\"LINESCOLORSMARKER_CLR\" data-opt-name=\"color\" onchange=\"populateSelectedColor(id,'LINESMARKERCOLOR','M')\" value=\"#1864ab\">"
				+ "<div id=\"LINESCOLORSMARKER_CLR_DIV\" class=\"colorsSelectDiv\"></div>"
				// + "<input type=\"color\" id=\"MARKERCOLOR\" data-opt-name=\"color\"
				// data-man=\"O\" title=\"\"/>"
				+ "</li>"
				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"LINESMARKERSIZE\" data-key-type=\"data\">"
				+ "<label>Marker Size</label>"
				+ "<input type=\"number\" id=\"LINESMARKERSIZE\" data-opt-name=\"size\" data-man=\"O\" title=\"\"/>"
				+ "</li>" + "</ul>" + "</li>";
		lineChart += "<li id=\"line-filter\" data-column-name=\"LINES\" data-key-type=\"data\">"
				+ "<div class=\"main-container\">" + "<div class=\"filter-container\">"
				+ "<img src=\"images/down-chevron.png\" alt=\"Down Chevron\" class=\"icons visualDarkMode\"/>"
				+ "<p>Line Markers</p>" + "</div>" + getToggleButton("LINES", "") + "</div>"
				+ "<ul class=\"sub-filters\" id=\"LINES\" data-opt-name=\"line\" style=\"display: none;\">"
				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"LINESCOLORS\" data-key-type=\"data\">"
				+ "<label>Line Color</label>" + "<input type=\"hidden\" id=\"LINESCOLORS\" value=\"\">"
				+ "<input type=\"color\" id=\"LINESCOLORS_CLR\" data-opt-name=\"color\" onchange=\"populateSelectedColor(id,'LINECOLOR','M')\" value=\"#1864ab\">"
				+ "<div id=\"LINESCOLORS_CLR_DIV\" class=\"colorsSelectDiv\"></div>" + "</li>"
				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"LINESWIDTH\" data-key-type=\"data\">"
				+ "<label>Line Width</label>"
				+ "<input type=\"number\" id=\"LINESWIDTH\" data-opt-name=\"width\" data-man=\"O\" title=\"\"/>"
				+ "</li>"
				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"LINESDASH\" data-key-type=\"data\">"
				+ "<label>Line Dash</label>"
				+ "<select name=\"text-position\" id=\"LINESDASH\" data-opt-name=\"dash\" data-man=\"O\" title=\"\">"
				+ "<option value=\"solid\">Solid</option>" + "<option value=\"dot\">Dot</option>"
				+ "<option value=\"dashdot\">Dashdot</option>" + "<option value=\"longdash\">Longdash</option>"
				+ "</select>" + "</li>"
				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"LINESSHAPE\" data-key-type=\"data\">"
				+ "<label>Line Shape</label>" + "<select name=\"text-info\" id=\"LINESSHAPE\" data-opt-name=\"shape\">"
				+ "<option value=\"linear\">Linear Shape</option>" + "<option value=\"spline\">Spline Shape</option>"
				+ "<option value=\"vh\">VH Shape</option>" + "<option value=\"hvh\">HVH Shape</option>" + "</select>"
				+ "</li>" + "</ul>" + "</li>";
		lineChart += endUltag;

		String bubbleChart = startUltag;
		bubbleChart += getGeneralFilters("SCATTER");
		// bubbleChart += getLabelAndHoverDataFilters("SCATTER");
		bubbleChart += getChartHover("SCATTER", "data");
		bubbleChart += getLegendFilters("SCATTER");
		bubbleChart += getaxis("SCATTER", "X");
		bubbleChart += getaxis("SCATTER", "Y");
		bubbleChart += "<li id=\"line-filter\" data-column-name=\"SCATTERMARKER\" data-key-type=\"data\">"
				+ "<div class=\"main-container\">" + "<div class=\"filter-container\">"
				+ "<img src=\"images/down-chevron.png\" alt=\"Down Chevron\" class=\"icons visualDarkMode\"/>"
				+ "<p>Bubble Markers</p>" + "</div>" + getToggleButton("SCATTER", "") + "</div>"

				+ "<ul class=\"sub-filters\" id=\"SCATTERMARKER\" data-opt-name=\"marker\" style=\"display: none;\">"
				// need to fix
				/*
				 * +
				 * "<li class=\"sub-filterItems active-filter\" data-column-name=\"SCATTERCOLORSMARKER\" data-key-type=\"data\">"
				 * + "<label>Bubble Color</label>" +
				 * "<input type=\"hidden\" id=\"SCATTERCOLORSMARKER\" value=\"\">" +
				 * "<input type=\"color\" id=\"SCATTERCOLORSMARKER_CLR\" data-opt-name=\"color\" onchange=\"populateSelectedColor(id,'SCATTERCOLORSMARKER','M')\" value=\"#dce2e8\">"
				 * + "<div id=\"SCATTERCOLORSMARKER_CLR_DIV\" class=\"colorsSelectDiv\"></div>"
				 * + "</li>" +
				 * "<li class=\"sub-filterItems active-filter\" data-column-name=\"SCATTEROPACITY\" data-key-type=\"data\">"
				 * + "<label>Bubble Opacity</label>" +
				 * "<input type=\"number\" id=\"SCATTEROPACITY\" data-opt-name=\"opacity\" data-man=\"O\" title=\"\"/>"
				 * + "</li>"
				 */

				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"SCATTERMARKERSIZE\" data-key-type=\"data\">"
				+ "<label>Bubble Size</label>"
				+ "<input type=\"number\" id=\"SCATTERMARKERSIZE\" data-opt-name=\"size\" data-man=\"O\" title=\"\"/>"
				+ "</li>" + "</ul>" + "</li>";
		bubbleChart += endUltag;

		String histogramChart = startUltag;
		histogramChart += getGeneralFilters("HISTOGRAM");
		histogramChart += getHoverDataFormat("HISTOGRAM");
		histogramChart += getChartHover("HISTOGRAM", "layout");
		histogramChart += getLegendFilters("HISTOGRAM");
		histogramChart += getChartColors("HISTOGRAM");
		histogramChart += getaxis("HISTOGRAM", "X");
		histogramChart += getaxis("HISTOGRAM", "Y");
		histogramChart += endUltag;

		String funnel = startUltag;
		funnel += getGeneralFilters("FUNNEL");
		funnel += getLabelAndHoverDataFilters("FUNNEL");
		funnel += getChartHover("FUNNEL", "layout");
		funnel += getLegendFilters("FUNNEL");
		funnel += getChartColors("FUNNEL");
		// funnel += getaxis("FUNNEL", "X");
		// funnel += getaxis("FUNNEL", "Y");
		funnel += endUltag;

		String waterfall = startUltag;
		waterfall += getGeneralFilters("WATERFALL");
		waterfall += getHoverDataFormat("WATERFALL");
		waterfall += getChartHover("WATERFALL", "layout");
		waterfall += getLegendFilters("WATERFALL");
		waterfall += getChartColors("WATERFALL");
		waterfall += getaxis("WATERFALL", "X");
		waterfall += getaxis("WATERFALL", "Y");
		waterfall += endUltag;

		String radar = startUltag;
		radar += getGeneralFilters("SCATTERPOLAR");
		// radar += getLabelDataRadar("SCATTERPOLAR");
		// radar += getHoverDataRadar("SCATTERPOLAR");
		radar += getChartHover("SCATTERPOLAR", "layout");
		radar += getLegendFilters("SCATTERPOLAR");
		radar += getChartColors("SCATTERPOLAR");
		// radar += getaxis("SCATTERPOLAR", "X");
		// radar += getaxis("SCATTERPOLAR", "Y");
		radar += endUltag;

		StringBuilder sunBurst = new StringBuilder();
		sunBurst.append(startUltag);
		sunBurst.append(getTitleFilterECharts("SUNBURST"));
		sunBurst.append(getSliceLabelsECharts("SUNBURST"));
		sunBurst.append(getTooltipDataECharts("SUNBURST"));
		sunBurst.append(endUltag);

		StringBuilder treeMapEcharts = new StringBuilder();
		treeMapEcharts.append(startUltag);
		treeMapEcharts.append(getTitleFilterECharts("TREEMAP"));
		treeMapEcharts.append(getSliceLabelsECharts("TREEMAP"));
		treeMapEcharts.append(getTooltipDataECharts("TREEMAP"));
		treeMapEcharts.append(endUltag);

		JSONObject filtercolumn = dashBoardsDAO.getcharttableattr(request);
		StringBuilder basicAreaChart = new StringBuilder(startUltag);
		basicAreaChart.append(getGeneralFilters("BASICAREACHART"));
		basicAreaChart.append(getEchartProperties("BASICAREACHART"));
		// basicAreaChart.append(//("BASICAREACHART","S"));
		basicAreaChart.append(getLineColorProperties("BASICAREACHART", "S"));
		basicAreaChart.append(endUltag);

		StringBuilder stackedAreaChart = new StringBuilder(startUltag);
		stackedAreaChart.append(getGeneralFilters("STACKEDAREACHART"));
		stackedAreaChart.append(getEchartProperties("STACKEDAREACHART"));
		// stackedAreaChart.append(getChartAreaProperties("STACKEDAREACHART","M"));
		stackedAreaChart.append(getLineColorProperties("STACKEDAREACHART", "M"));
		stackedAreaChart.append(endUltag);

		StringBuilder gradStackAreaChart = new StringBuilder(startUltag);
		gradStackAreaChart.append(getGeneralFilters("GRADSTACKAREACHART"));
		gradStackAreaChart.append(getEchartProperties("GRADSTACKAREACHART"));
		// gradStackAreaChart.append(getChartAreaProperties("GRADSTACKAREACHART","M"));
		gradStackAreaChart.append(getLineColorProperties("GRADSTACKAREACHART", "M"));
		gradStackAreaChart.append(endUltag);

		StringBuilder areaPiecesChart = new StringBuilder(startUltag);
		areaPiecesChart.append(getAreaPieacesDiv("AREAPIECESCHART"));

		areaPiecesChart.append(getGeneralFilters("AREAPIECESCHART"));

		areaPiecesChart.append(getEchartProperties("AREAPIECESCHART"));

		// areaPiecesChart.append(getChartAreaProperties("AREAPIECESCHART","S"));
		areaPiecesChart.append(getLineColorProperties("AREAPIECESCHART", "S"));
		areaPiecesChart.append(endUltag);

		StringBuilder ganttChart = new StringBuilder(startUltag);
		ganttChart.append(getGeneralFilters("GANTTCHART"));

		ganttChart.append("<li class=\"sub-filterItems active-filter\" data-column-name=\"" + "GANTTCHARTBARHEIGHT"
				+ "\">" + "<label>Bar Height</label>" + "<input type=\"number\" id='" + "GANTTCHARTBARHEIGHT"
				+ "' data-opt-name=\"barheight\" data-man=\"O\" title=\"barheight\"/>" + "</li>");

		ganttChart
				.append("<li id=\"marker-filter\" data-column-name=\"" + "GANTTCHART"
						+ "AREA\" data-key-type=\"data\">")
				.append("<div class=\"main-container\"><div class=\"filter-container\">")
				.append("<img src=\"images/down-chevron.png\" alt=\"Down Chevron\" class=\"icons visualDarkMode\"/>")
				.append("<p>Grid Area</p>").append("</div>").append(getToggleButton("GANTTCHART", "")).append("</div>")
				.append("<ul class=\"sub-filters\" id=\"" + "GANTTCHART"
						+ "\" data-opt-name=\"marker\" style=\"display: none;\">")
				.append("<li class=\"sub-filterItems active-filter\" data-column-name=\"" + "GANTTCHART"
						+ "COLORSAREA\" data-key-type=\"data\">")
				.append("<label>Grid Area Color</label>")
				.append("<input type=\"hidden\" id=\"" + "GANTTCHART" + "COLORSAREA\" value=\"pink\">")
				.append("<input type=\"color\" id=\"" + "GANTTCHART"
						+ "COLORSAREA_CLR\" data-opt-name=\"color\" onchange=\"populateSelectedColor(id,"
						+ "'GANTTCHART" + "AREACOLOR','" + "S" + "')\" value=\"#1864ab\">")
				.append("<div id=\"" + "GANTTCHART" + "COLORSAREA_CLR_DIV\" class=\"colorsSelectDiv\"></div>")
				.append("</li>").append("</ul>").append("</li>");
		ganttChart.append(endUltag);

		StringBuilder candlestick = new StringBuilder(startUltag);
		candlestick.append(getGeneralFilters("CANDLESTICK"));
		candlestick.append(getCandleSticktProperties("CANDLESTICK"));
		candlestick.append(endUltag);
		StringBuilder geochart = new StringBuilder(startUltag);
		geochart.append(getGeneralFilters("GEOCHART"));
		geochart.append(getCandleSticktProperties("GEOCHART"));
		geochart.append(endUltag);
		StringBuilder geoLantLangchart = new StringBuilder(startUltag);
		geoLantLangchart.append(getGeneralFilters("GEOLATLANGCHART"));
		geoLantLangchart.append(getCandleSticktProperties("GEOLATLANGCHART"));
		geoLantLangchart.append(endUltag);

		StringBuilder barAndLineChart = new StringBuilder(startUltag);
		barAndLineChart.append(getGeneralFilters("BARANDLINE"));
		barAndLineChart.append(getLabelAndHoverDataFiltersWithOutPercentage("BARANDLINE"));
		barAndLineChart.append(
				"<li class=\"general-filters active-filter\" data-column-name=\"BARMODE\" data-key-type=\"layout\">"
						+ "<div class=\"sub-filterItems\">" + "<p>Bar Mode</p>"
						+ "<select name=\"text-info\" id=\"BARMODE\" data-opt-name=\"barmode\">"
						+ "<option value=\"group\">Group</option>" + "<option value=\"stack\">Stack</option>"
						+ "</select>" + "</div>" + "</li>");
		barAndLineChart.append(
				"<li class=\"general-filters active-filter\" data-column-name=\"BARGAP\" data-key-type=\"layout\">"
						+ "<div class=\"sub-filterItems\">" + "<p>Bar Gap</p>"
						+ "<input id=\"BARGAP\" data-opt-name=\"bargap\" data-man=\"O\" title=\"Gap\" type=\"range\" min=\"10\" max=\"100\" step=\"10\" value=\"20\" />"
						+ "</div>" + "</li>");
		barAndLineChart.append(getChartAreaProperties("BARANDLINE", "M"));
		barAndLineChart.append(getLineColorProperties("BARANDLINE", "S"));
		barAndLineChart.append(endUltag);

		StringBuilder sankey = new StringBuilder(startUltag);
		sankey.append(getGeneralFilters("SANKEY"));
		sankey.append(getLabelAndHoverDataFiltersWithOutPercentage("SANKEY"));
		sankey.append(
				"<li class=\"general-filters active-filter\" data-column-name=\"LINEOPACITY\" data-key-type=\"layout\">"
						+ "<div class=\"sub-filterItems\">" + "<p>Opacity</p>"
						+ "<input id=\"SANKEYLINEOPACITY\" data-opt-name=\"sankeylineopacity\" data-man=\"O\" title=\"opacity\" type=\"range\" min=\"0\" max=\"1\" step=\"0.1\" value=\"0.5\" />"
						+ "</div>" + "</li>");
		sankey.append(
				"<li class=\"general-filters active-filter\" data-column-name=\"SANKEYCURVENESS\" data-key-type=\"layout\">"
						+ "<div class=\"sub-filterItems\">" + "<p>Curveness</p>"
						+ "<input id=\"SANKEYCURVENESS\" data-opt-name=\"sankeycurverness\" data-man=\"O\" title=\"Curveness\" type=\"range\" min=\"0\" max=\"1\" step=\"0.1\" value=\"0.5\" />"
						+ "</div>" + "</li>");
		// sankey.append("<li class=\"general-filters active-filter\"
		// data-column-name=\"" + "SANKEY"
		// + "LABELPOSITION\" data-key-type=\"data\">" + "<div
		// class=\"sub-filterItems\">" + "<p>Text Position</p>"
		// + "<select name=\"text-position\" id=\"" + "SANKEY" + "LABELPOSITION\"
		// data-opt-name=\"textposition\">"
		// + "<option value=\"inside\">Inside</option>" + "<option
		// value=\"top\">Top</option>"
		// + "<option value=\"right\">Right</option>" + "<option
		// value=\"bottom\">Bottom</option>"
		// +"<option value=\"left\">Left</option>"
		// + "</select>"
		// + "</div>" + "</li>");
		sankey.append(endUltag);

		StringBuilder boxPlot = new StringBuilder(startUltag);
		boxPlot.append(getGeneralFilters("BOXPLOT"));
		boxPlot.append("<li class=\"general-filters active-filter\" data-column-name=\"" + "BOXPLOT"
				+ "HOVERLABELDATA\" data-key-type=\"data\">" + "<div class=\"sub-filterItems\">"
				+ "<p>Hover Data Visible</p>" + "<select name=\"text-info\" id=\"" + "BOXPLOT"
				+ "HOVERLABELDATA\" data-opt-name=\"hoverinfo\" >" + "<option value=\"x\">Label</option>"
				+ "<option value=\"y\">Value</option>" + "<option value=\"x+y\" selected>Label and value</option>"
				+ "</select>" + "</div>" + "</li>");
		boxPlot.append(getChartAreaProperties("BOXPLOT", "S"));
		boxPlot.append(endUltag);
		StringBuilder stackedBarChart = new StringBuilder(startUltag);
		stackedBarChart.append(getGeneralFilters("STACKEDBARCHART"));
		stackedBarChart.append(getLabelAndHoverDataFiltersWithOutPercentage("STACKEDBARCHART"));
		// stackedBarChart.append(getChartHover("STACKEDBARCHART","DATA"));
		stackedBarChart.append(endUltag);
		StringBuilder decompositionTree = new StringBuilder(startUltag);
		decompositionTree.append(getGeneralFilters("DECOMPOSITIONTREE"));
		decompositionTree.append("<li class=\"general-filters active-filter\" data-column-name=\"DECOMPOSITIONTREE"
				+ "ORIENT\" data-key-type=\"data\"><div class=\"sub-filterItems\"><p>Orientation</p>"
				+ "<select name=\"text-info\" id=\"DECOMPOSITIONTREEORIENT\" data-opt-name=\"ORIENT\">"
				+ "<option value=\"LR\" selected>Left To Right</option><option value=\"RL\" >Right To Left</option>"
				+ "<option value=\"TB\">Top To Bottom</option>" + "<option value=\"BT\">Bottom To Top</option>"
				+ "</select>" + "</div></li>"
				+ "<li class=\"general-filters active-filter\" data-column-name=\"DECOMPOSITIONTREE"
				+ "SYMBOL\" data-key-type=\"data\">" + "<div class=\"sub-filterItems\">"
				+ "<p>Marker Shape</p><select name=\"text-info\" id=\"DECOMPOSITIONTREE"
				+ "SYMBOL\" data-opt-name=\"SYMBOL\" ><option value=\"none\">none</option>"
				+ "<option value=\"circle\" selected>circle</option>" + "<option value=\"rect\">Rectangle</option>"
				+ "<option value=\"triangle\" >Triangle</option>" + "<option value=\"diamond\" >Diamond</option>"
				+ "<option value=\"pin\" >Pin</option>" + "<option value=\"arrow\">Arrow</option>"
				+ "</select></div></li>"
				+ "</li><li class=\"general-filters active-filter\" data-column-name=\"DECOMPOSITIONTREE"
				+ "LAYOUT\" data-key-type=\"data\"><div class=\"sub-filterItems\"><p>Layout</p>"
				+ "<select name=\"text-position\" id=\"DECOMPOSITIONTREELAYOUT\" data-opt-name=\"LAYOUT\">"
				+ "<option value=\"orthogonal\">Orthogonal</option><option value=\"radial\">Radial</option>"
				+ "</select>" + "</div></li>");
		decompositionTree.append(getLineColorProperties("DECOMPOSITIONTREE", "S"));
		decompositionTree
				.append("<li class=\"general-filters active-filter\" id=\"line-filter\" data-column-name=\""
						+ "DECOMPOSITIONTREE" + "\" data-key-type=\"data\">")
				.append("<div class=\"main-container\"><div class=\"filter-container\">")
				.append("<img src=\"images/down-chevron.png\" alt=\"Down Chevron\" class=\"icons visualDarkMode\"/>")
				.append("<p>Chart Marker</p>").append("</div>").append(getToggleButton("DECOMPOSITIONTREE", ""))
				.append("</div>")
				.append("<ul class=\"sub-filters\" id=\"" + "DECOMPOSITIONTREEMARKER"
						+ "\" data-opt-name=\"line\" style=\"display: none;\">")
				.append("<li class=\"sub-filterItems active-filter\" data-column-name=\"" + "DECOMPOSITIONTREEMARKER"
						+ "COLORS\" data-key-type=\"data\">")
				.append("<label>Maker Color</label>")
				.append("<input type=\"hidden\" id=\"" + "DECOMPOSITIONTREE" + "MARKERCOLORS\" value=\"#1864ab\">")
				.append("<input type=\"color\" id=\"" + "DECOMPOSITIONTREE"
						+ "MARKERCOLORS_CLR\" data-opt-name=\"color\" onchange=\"populateSelectedColor(id,'DECOMPOSITIONTREEMARKERCOLORS','S')\" value=\"#1864ab\">")
				.append("<div id=\"" + "DECOMPOSITIONTREE" + "MARKERCOLORS_CLR_DIV\" class=\"colorsSelectDiv\"></div>")
				.append("</li>").append("</ul>").append("</li>");
		decompositionTree.append(endUltag);
		String kpiChart = startUltag;
		kpiChart += getTitleFilters("KPI");
		kpiChart += endUltag;

		String kpiBarChart = startUltag;
		kpiBarChart += getTitleFilters("KPIBAR");
		kpiBarChart += generateKPIInputs("KPIBARCHART");
		kpiBarChart += endUltag;

		jsonChartFilter.put("kpiBarChart", kpiBarChart);
		jsonChartFilter.put("kpiChart", kpiChart);
		jsonChartFilter.put("pie", pieChart);
		jsonChartFilter.put("donut", donutChart);
		jsonChartFilter.put("bar", barChart);
		jsonChartFilter.put("column", columnChart);
		jsonChartFilter.put("lines", lineChart);
		jsonChartFilter.put("scatter", bubbleChart);
		jsonChartFilter.put("histogram", histogramChart);
		jsonChartFilter.put("funnel", funnel);
		jsonChartFilter.put("waterfall", waterfall);
		jsonChartFilter.put("scatterpolar", radar);
		jsonChartFilter.put("indicator", gaugeChart);
		jsonChartFilter.put("heatMap", heatMap);
		jsonChartFilter.put("sunburst", sunBurst.toString());
		jsonChartFilter.put("filtercolumn", filtercolumn);
		jsonChartFilter.put("treemap", treeMapEcharts.toString());
		jsonChartFilter.put("BasicAreaChart", basicAreaChart.toString());
		jsonChartFilter.put("StackedAreaChart", stackedAreaChart.toString());
		jsonChartFilter.put("GradStackAreaChart", gradStackAreaChart.toString());
		jsonChartFilter.put("AreaPiecesChart", areaPiecesChart.toString());
		jsonChartFilter.put("ganttChart", ganttChart.toString());
		jsonChartFilter.put("candlestick", candlestick);
		jsonChartFilter.put("geochart", geochart);
		jsonChartFilter.put("BarAndLine", barAndLineChart.toString());
		jsonChartFilter.put("sankey", sankey.toString());
		jsonChartFilter.put("boxplot", boxPlot.toString());
		jsonChartFilter.put("stackedBarChart", stackedBarChart.toString());
		jsonChartFilter.put("decompositionTree", decompositionTree.toString());
		jsonChartFilter.put("geoLatLangchart", geoLantLangchart.toString());

		return jsonChartFilter;
	}

	public String getDataVisibleProperty(String chartType) {
		StringBuilder str = new StringBuilder();
		return str.toString();
	}

	public String getLabelAndHoverDataFiltersWithOutPercentage(String chartType) {
		String labelAndHoverDataFilters = "<li class=\"general-filters active-filter\" data-column-name=\"" + chartType
				+ "LABELDATA\" data-key-type=\"data\">" + "<div class=\"sub-filterItems\">" + "<p>Data Visible</p>"
				+ "<select name=\"text-info\" id=\"" + chartType + "LABELDATA\" data-opt-name=\"textinfo\">"
				+ "<option value=\"''\" selected>None</option>" + "<option value=\"x\">Label</option>"
				+ "<option value=\"y\" >Value</option>" + "<option value=\"x+y\">Label and value</option>" + "</select>"
				+ "</div>" + "</li>" + "<li class=\"general-filters active-filter\" data-column-name=\"" + chartType
				+ "HOVERLABELDATA\" data-key-type=\"data\">" + "<div class=\"sub-filterItems\">"
				+ "<p>Hover Data Visible</p>" + "<select name=\"text-info\" id=\"" + chartType
				+ "HOVERLABELDATA\" data-opt-name=\"hoverinfo\" >" + "<option value=\"x\">Label</option>"
				+ "<option value=\"y\">Value</option>" + "<option value=\"x+y\" selected>Label and value</option>"
				+ "</select>" + "</div>" + "</li>" + "</li>"
				+ "<li class=\"general-filters active-filter\" data-column-name=\"" + chartType
				+ "LABELPOSITION\" data-key-type=\"data\">" + "<div class=\"sub-filterItems\">" + "<p>Text Position</p>"
				+ "<select name=\"text-position\" id=\"" + chartType + "LABELPOSITION\" data-opt-name=\"textposition\">"
				+ "<option value=\"inside\">Inside</option>" + "<option value=\"outside\">Outside</option>"
				+ "<option value=\"auto\">Auto</option></select>" + "</div>" + "</li>";
		return labelAndHoverDataFilters;
	}

	public String getTitleFilters(String chartType) {
		String generalFilters = "<li class=\"general-filters active-filter\" data-column-name=\"" + chartType
				+ "CHARTTITLE\" data-key-type=\"layout\">" + "<div class=\"sub-filterItems\">" + "<p>Title</p>"
				+ "<input type=\"text\" id=\"" + chartType
				+ "CHARTTITLE\" data-opt-name=\"title\" data-man=\"O\" title=\"Title\"/>" + "</div>" + "</li>";
		return generalFilters;
	}

	public String getGeneralFilters(String chartType) {
		String generalFilters = "<li class=\"general-filters active-filter\" data-column-name=\"" + chartType
				+ "CHARTTITLE\" data-key-type=\"layout\">" + "<div class=\"sub-filterItems\">" + "<p>Title</p>"
				+ "<input type=\"text\" id=\"" + chartType
				+ "CHARTTITLE\" data-opt-name=\"title\" data-man=\"O\" title=\"Title\"/>" + "</div>" + "</li>";
		return generalFilters;
	}

	public String getIndicatorFilters(String chartType) {
		String gaugeChart = "<li class=\"general-filters active-filter\" data-column-name=\"" + chartType
				+ "CHARTTITLE\" data-key-type=\"layout\">" + "<div class=\"sub-filterItems\">" + "<p>Title</p>"
				+ "<input type=\"text\" id=\"" + chartType
				+ "CHARTTITLE\" data-opt-name=\"title\" data-man=\"O\" title=\"Title\"/>" + "</div>" + "</li>"
				+ "<li class=\"general-filters\" data-column-name=\"" + chartType
				+ "PAPER_BGCOLOR\" data-key-type=\"layout\">" + "<div class=\"sub-filterItems\">"
				+ "<p>PAPER BGCOLOR</p>" + "<input type=\"color\" id=\"" + chartType
				// + "PAPER_BGCOLOR\" data-opt-name=\"paper_bgcolor\" data-man=\"O\"
				// title=\"Paper Bgcolor\" value=\"#14b1e6\"/>"
				+ "PAPER_BGCOLOR\" data-opt-name=\"paper_bgcolor\" data-man=\"O\" title=\"Paper Bgcolor\" value=\"#ffffff\"/>"
				+ "</div>" + "</li>";
		gaugeChart += "<li class=\"legendFontClass\" data-column-name=\"" + chartType
				+ "LEGENDFONT\" data-key-type=\"layout\">" + getFontObject(chartType + "LEGEND", "font") + "</li>";
		return gaugeChart;
	}

	public String getLegendFilters(String chartType) {
		String legendFilters = "<li id=\"legend-filter\" data-column-name=\"" + chartType
				+ "LEGEND\" data-key-type=\"layout\">" + "<div class=\"main-container\">"
				+ "<div class=\"filter-container\">"
				+ "<img src=\"images/down-chevron.png\" alt=\"Down Chevron\" class=\"icons\"/>" + "<p>Legend</p>"
				+ "</div>" + getToggleButton(chartType, "legend") + "</div>" + "<ul class=\"sub-filters\" id=\""
				+ chartType + "LEGEND\" style=\"display: none;\">"
				+ "<li class=\"sub-filterItems active-filter\"  data-column-name=\"ORIENTATION\" data-key-type=\"layout\">"
				+ "<label>Orientation</label>"
				+ "<select name=\"legend\" id=\"ORIENTATION\" data-opt-name=\"orientation\" data-man=\"O\" title=\"Orientation\">"
				+ "<option v  alue=\"h\">Horizontal</option>" + "<option value=\"v\">Vertical</option>" + "</select>"
				+ "</li>" + "<li class=\"sub-filterItems active-filter\" data-column-name=\"" + chartType
				+ "SHOWLEGEND\" data-key-type=\"layout\" style=\"display: none;\">" + "<span id=\"" + chartType
				+ "SHOWLEGEND\" data-opt-name=\"showlegend\" value=\"true\"></span>" + "</li>"
//				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"" + chartType
//				+ "LEGENDPOSITION\" data-key-type=\"layout\">" + "<label>Position</label>"
//				+ "<select name=\"legend\" id=\"" + chartType + "LEGENDPOSITION\" data-opt-name=\"position\">"
//				+ "<option value=\"Top\">Top</option>" + "<option value=\"Bottom\">Bottom</option>"
//				+ "<option value=\"Left\">Left</option>" + "<option value=\"Right\" selected>Right</option>"
//				+ "</select>" + "</li>"  
				+ "<li class=\"legendFontClass active-filter\" data-column-name=\"" + chartType + "LEGENDFONT\">"
				+ getFontObject(chartType + "LEGEND", "font") + "</li>" + "</ul>" + "</li>";
		return legendFilters;
	}

	public String getTreeMapLabelFilters(String chartType) {

		String getTreeMapLabelFilters = "<li class=\"general-filters active-filter\" data-column-name=\"" + chartType
				+ "LABELDATA\" data-key-type=\"data\">" + "<div class=\"sub-filterItems\">" + "<p>Data Visible</p>"
				+ "<select name=\"text-info\" id=\"" + chartType + "LABELDATA\" data-opt-name=\"textinfo\">"
				+ "<option value=\"label\">Label</option>" + "<option value=\"value\">Value</option>"
				+ "<option value=\"label+value\">Label and value</option>"
				+ "<option value=\"percent parent\">Parent Percentage</option>"
				+ "<option value=\"label+percent parent\">Label and Parent Percentage</option>"
				+ "<option value=\"label+value+percent parent\">Label,Value and Parent Percentage</option>"
				+ "</select>" + "</div>" + "</li>";
		return getTreeMapLabelFilters;
	}

	public String pieDonutGeneralFilters(String chartType) {

		String pieDonutGeneralFilters = "<li class=\"general-filters active-filter\" data-column-name=\"" + chartType
				+ "LABELDATA\" data-key-type=\"data\">" + "<div class=\"sub-filterItems\">" + "<p>Data Visible</p>"
				+ "<select name=\"text-info\" id=\"" + chartType + "LABELDATA\" data-opt-name=\"textinfo\">"
				+ "<option value=\"none\">None</option>" + "<option value=\"label\">Label</option>"
				+ "<option value=\"value\">Value</option>"
				+ "<option value=\"label+value\" selected>Label and value</option>"
				+ "<option value=\"percent\">Percentage</option>"
				+ "<option value=\"label+percent\">Label and Percentage</option>"
				+ "<option value=\"value+percent\">Value and Percentage</option>" + "</select>" + "</div>" + "</li>"
				+ "<li class=\"general-filters active-filter\" data-column-name=\"" + chartType
				+ "LABELPOSITION\" data-key-type=\"data\">" + "<div class=\"sub-filterItems\">" + "<p>Text Position</p>"
				+ "<select name=\"text-position\" id=\"" + chartType + "LABELPOSITION\" data-opt-name=\"textposition\">"
				+ "<option value=\"inside\">Inside</option>" + "<option value=\"outside\">Outside</option>"
				+ "<option value=\"auto\">Auto</option>" + "<option value=\"none\">None</option>" + "</select>"
				+ "</div>" + "</li>" + "<li class=\"general-filters active-filter\" data-column-name=\"" + chartType
				+ "HOVERLABELDATA\" data-key-type=\"data\">" + "<div class=\"sub-filterItems\">"
				+ "<p>Hover Data Visible</p>" + "<select name=\"text-info\" id=\"" + chartType
				+ "HOVERLABELDATA\" data-opt-name=\"hoverinfo\" >" + "<option value=\"none\">None</option>"
				+ "<option value=\"label\">Label</option>" + "<option value=\"value\">Value</option>"
				+ "<option value=\"percent\">Percentage</option>"
				+ "<option value=\"label+value\">Label and value</option>"
				+ "<option value=\"label+percent\">Label and Percentage</option>"
				+ "<option value=\"value+percent\">Value and Percentage</option>" + "</select>" + "</div>" + "</li>"
				+ "</li>";
		return pieDonutGeneralFilters;
	}

	public String getLabelAndHoverDataFilters(String chartType) {
		String labelAndHoverDataFilters = "<li class=\"general-filters active-filter\" data-column-name=\"" + chartType
				+ "LABELDATA\" data-key-type=\"data\">" + "<div class=\"sub-filterItems\">" + "<p>Data Visible</p>"
				+ "<select name=\"text-info\" id=\"" + chartType + "LABELDATA\" data-opt-name=\"textinfo\">"
				+ "<option value=\"none\">None</option>" + "<option value=\"x\">Label</option>"
				+ "<option value=\"y\">Value</option>" + "<option value=\"%\">Percentage</option>"
				+ "<option value=\"x+y\">Label and value</option>"
				+ "<option value=\"x+%\">Label and Percentage</option>"
				+ "<option value=\"y+%\">Value and Percentage</option>" + "</select>" + "</div>" + "</li>"
				+ "<li class=\"general-filters active-filter\" data-column-name=\"" + chartType
				+ "HOVERLABELDATA\" data-key-type=\"data\">" + "<div class=\"sub-filterItems\">"
				+ "<p>Hover Data Visible</p>" + "<select name=\"text-info\" id=\"" + chartType
				+ "HOVERLABELDATA\" data-opt-name=\"hoverinfo\" >" + "<option value=\"x\">Label</option>"
				+ "<option value=\"y\">Value</option>" + "<option value=\"%\">Percentage</option>"
				+ "<option value=\"x+y\" selected>Label and value</option>"
				+ "<option value=\"x+%\">Label and Percentage</option>"
				+ "<option value=\"value+percent\">Value and Percentage</option>" + "</select>" + "</div>" + "</li>"
				+ "</li>" + "<li class=\"general-filters active-filter\" data-column-name=\"" + chartType
				+ "LABELPOSITION\" data-key-type=\"data\">" + "<div class=\"sub-filterItems\">" + "<p>Text Position</p>"
				+ "<select name=\"text-position\" id=\"" + chartType + "LABELPOSITION\" data-opt-name=\"textposition\">"
				+ "<option value=\"inside\">Inside</option>" + "<option value=\"outside\">Outside</option>"
				+ "<option value=\"auto\">Auto</option>" + "<option value=\"none\">None</option>" + "</select>"
				+ "</div>" + "</li>";
		return labelAndHoverDataFilters;
	}

	public String getChartColors(String chartType) {
		String chartColors = "<li id=\"slice-color-filter\" data-column-name=\"" + chartType
				+ "MARKER\" data-key-type=\"data\" style=\"display:none\">" + "<div class=\"main-container\">"
				+ "<div class=\"filter-container\">"
				+ "<img src=\"images/down-chevron.png\" alt=\"Down Chevron\" class=\"icons\"/>" + "<p>Chart Color</p>"
				+ "</div>" + getToggleButton(chartType, "") + "</div>" + "<ul class=\"sub-filters\" id=\"" + chartType
				+ "MARKER\" style=\"display: none;\">" + "<li class=\"sub-filterItems\" data-column-name=\"" + chartType
				+ "COLORS\">" + "<label>Chart Colors</label>" + "<input type=\"hidden\" id=\"" + chartType
				+ "COLORS\" value=\"\">" + "<input type=\"color\" id=\"" + chartType
				+ "COLORS_CLR\" data-opt-name=\"colors\" onchange=\"populateSelectedColor(id,'" + chartType
				+ "COLORS','M')\" value=\"#dce2e8\">" + "<div id=\"" + chartType
				+ "COLORS_CLR_DIV\" class=\"colorsSelectDiv\"></div>" + "</li>"
				+ "<li id=\"slice-color-filter\" data-column-name=\"" + chartType + "LINES\">"
				+ "<div class=\"main-container\">" + "<div class=\"filter-container\">"
				+ "<img src=\"images/down-chevron.png\" alt=\"Down Chevron\" class=\"icons\"/>" + "<p>Line</p>"
				+ "</div>" + getToggleButton(chartType, "") + "</div>" + "<ul class=\"sub-filters\" id=\"" + chartType
				+ "LINES\" style=\"display: none;\">" + "<li class=\"sub-filterItems\" data-column-name=\"" + chartType
				+ "LINECOLOR\">" + "<label>Line Color</label>" + "<input type=\"color\" id=\"" + chartType
				+ "LINECOLOR\" data-opt-name=\"color\">" + "</li>" + "<li class=\"sub-filterItems\" data-column-name=\""
				+ chartType + "LINEWIDTH\">" + "<label>Line Width</label>" + "<input type=\"number\" id=\"" + chartType
				+ "LINEWIDTH\" data-opt-name=\"width\">" + "</li>" + "</ul>" + "</li>" + "</ul>" + "</li>";
		return chartColors;
	}

	public String getTreeMapChartMarkers(String chartType) {
		String chartColors = "<li id=\"slice-color-filter\" data-column-name=\"" + chartType
				+ "MARKER\" data-key-type=\"data\">" + "<div class=\"main-container\">"
				+ "<div class=\"filter-container\">"
				+ "<img src=\"images/down-chevron.png\" alt=\"Down Chevron\" class=\"icons\"/>" + "<p>Marker</p>"
				+ "</div>" + getToggleButton(chartType, "") + "</div>" + "<ul class=\"sub-filters\" id=\"" + chartType
				+ "MARKER\" style=\"display: none;\">" + "<li id=\"slice-color-filter\" data-column-name=\"" + chartType
				+ "LINES\">" + "<div class=\"main-container\">" + "<div class=\"filter-container\">"
				+ "<img src=\"images/down-chevron.png\" alt=\"Down Chevron\" class=\"icons\"/>" + "<p>Line</p>"
				+ "</div>" + getToggleButton(chartType, "") + "</div>" + "<ul class=\"sub-filters\" id=\"" + chartType
				+ "LINES\" style=\"display: none;\">" + "<li class=\"sub-filterItems\" data-column-name=\"" + chartType
				+ "LINECOLOR\">" + "<label>Line Color</label>" + "<input type=\"color\" id=\"" + chartType
				+ "LINECOLOR\" data-opt-name=\"color\">" + "</li>" + "<li class=\"sub-filterItems\" data-column-name=\""
				+ chartType + "LINEWIDTH\">" + "<label>Line Width</label>" + "<input type=\"number\" id=\"" + chartType
				+ "LINEWIDTH\" data-opt-name=\"width\">" + "</li>" + "</ul>" + "</li>" + "</ul>" + "</li>";
		return chartColors;
	}

	public String getHoverDataFormat(String chartType) {
		String hoverDataFormat = "<li class=\"general-filters active-filter\" data-column-name=\"" + chartType
				+ "HOVERLABELDATA\" data-key-type=\"data\">" + "<div class=\"sub-filterItems\">"
				+ "<p>Hover Data Visible</p>" + "<select name=\"text-info\" id=\"" + chartType
				+ "HOVERLABELDATA\" data-opt-name=\" \" >" + "<option value=\"none\">None</option>"
				+ "<option value=\"x\">Label</option>" + "<option value=\"y\">Values</option>"
				+ "<option value=\"x+y\" selected>Label and value</option>" + "</select>" + "</div>" + "</li>";

		return hoverDataFormat;
	}

	public String getHoverTreeDataDataFormat(String chartType) {
		String selected = "selected";
		String hoverDataFormat = "<li class=\"general-filters active-filter\" data-column-name=\"" + chartType
				+ "HOVERLABELDATA\" data-key-type=\"data\">" + "<div class=\"sub-filterItems\">"
				+ "<p>Hover Data Visible</p>" + "<select name=\"text-info\" id=\"" + chartType
				+ "HOVERLABELDATA\" data-opt-name=\"hoverinfo\" >" + "<option value=\"label\" " + selected
				+ ">Label</option>" + "<option value=\"label+value\">Label and value</option>"
				+ "<option value=\"label+value+percent parent\">Label,Value And Parent</option>"
				+ "<option value=\"none\">None</option>" + "</select>" + "</div>" + "</li>";

		return hoverDataFormat;
	}

	public String getLabelDataRadar(String chartType) {
		String labelDataFormat = "<li class=\"general-filters active-filter\" data-column-name=\"" + chartType
				+ "LABELDATA\" data-key-type=\"data\">" + "<div class=\"sub-filterItems\">"
				+ "<p>Label Data Visible</p>" + "<select name=\"text-info\" id=\"" + chartType
				+ "LABELDATA\" data-opt-name=\" \" >" + "<option value=\"theta\">Label</option>"
				+ "<option value=\"r\">Values</option>" + "<option value=\"theta+r\" selected>Label and value</option>"
				+ "</select>" + "</div>" + "</li>";

		return labelDataFormat;
	}

	public String getHoverDataRadar(String chartType) {
		String hoverDataFormat = "<li class=\"general-filters active-filter\" data-column-name=\"" + chartType
				+ "HOVERLABELDATA\" data-key-type=\"data\">" + "<div class=\"sub-filterItems\">"
				+ "<p>Hover Data Visible</p>" + "<select name=\"text-info\" id=\"" + chartType
				+ "HOVERLABELDATA\" data-opt-name=\" \" >" + "<option value=\"theta\">Label</option>"
				+ "<option value=\"r\">Values</option>" + "<option value=\"theta+r\" selected>Label and value</option>"
				+ "</select>" + "</div>" + "</li>";

		return hoverDataFormat;
	}

	public String getChartHover(String chartType, String layoutType) {
		String hoverDetails = "<li id=\"slice-hover-filter\" data-column-name=\"" + chartType
				+ "HOVERLABEL\" data-key-type=\"" + layoutType + "\">" + "<div class=\"main-container\">"
				+ "<div class=\"filter-container\">"
				+ "<img src=\"images/down-chevron.png\" alt=\"Down Chevron\" class=\"icons\"/>" + "<p>Chart Hover</p>"
				+ "</div>" + getToggleButton(chartType, "chartHover") + "</div>" + "<ul class=\"sub-filters\" id=\""
				+ chartType + "HOVERLABEL\" style=\"display: none;\">"
				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"" + chartType + "HOVERBG\">"
				+ "<label>Background Color</label>" + "<input type=\"color\" id=\"" + chartType
				+ "HOVERBG\" data-opt-name=\"bgcolor\" value=\"#74c0fc\">" + "</li>"
				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"" + chartType + "HOVERBORDERCOLOR\">"
				+ "<label>Border Color</label>" + "<input type=\"color\" id=\"" + chartType
				+ "HOVERBORDERCOLOR\" data-opt-name=\"bordercolor\" value=\"#ffffff\">" + "</li>"
				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"" + chartType + "HOVERFONT\">"
				+ getFontObject(chartType + "HOVER", "font") + "</li>" + "</ul>" + "</li>";
		return hoverDetails;
	}

	public String getaxis(String chartType, String axisMode) {

		String axisFilter = "<li id=\"" + axisMode + "-axis-filter\" data-column-name=\"" + chartType + axisMode
				+ "AXIS\" data-key-type=\"layout\">" + "<div class=\"main-container\">"
				+ "<div class=\"filter-container\">"
				+ "<img src=\"images/down-chevron.png\" alt=\"Down Chevron\" class=\"icons\"/>";
		if (axisMode != null && "X".equalsIgnoreCase(axisMode)) {
			axisFilter += "<p>X-Axis</p>";
		} else if (axisMode != null && "Y".equalsIgnoreCase(axisMode)) {
			axisFilter += "<p>Y-Axis</p>";
		}
		axisFilter += "</div>" + getToggleButton(chartType, axisMode + "axis") + "</div>"
				+ "<ul class=\"sub-filters\" id=\"" + chartType + axisMode + "AXIS\" style=\"display: none;\">"
				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"" + chartType + axisMode
				+ "AXISTITLE\">" + "<label>Title</label>" + "<input type=\"text\" id='" + chartType + axisMode
				+ "AXISTITLE' data-opt-name=\"title\" data-man=\"O\" title=\"title\"/>" + "</li>"
				// need to fix
//				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"" + chartType + axisMode
//				+ "RANGEMODE\">" + "<label>Range Mode</label>" + "<select id='" + chartType + axisMode
//				+ "RANGEMODE' data-opt-name=\"rangemode\" data-man=\"O\" title=\"RangeMode\">"
//				+ "<option value=\"normal\">Normal</option>" + "<option value=\"tozero\">To Zero</option>"
//				+ "<option value=\"nonnegative\">Non Negative</option>" + "</select>" + "</li>"

				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"" + chartType + axisMode
				+ "AXISTICKANGEL\">" + "<label>Tick Angle</label>" + "<input type=\"number\" id='" + chartType
				+ axisMode + "AXISTICKANGEL' data-opt-name=\"tickangle\" data-man=\"O\" title=\"tickangle\"/>" + "</li>"
				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"" + chartType + axisMode
				+ "TITLEFONT\">" + getFontObject(chartType + axisMode + "TITLE", "titlefont") + "</li>" + "</ul>"
				+ "</li>";

		return axisFilter;
	}

	public String getFontObject(String layoutType, String fontKey) {

		String fontObject = "<div class=\"main-container inner-container\">" + "<div class=\"filter-container\">"
				+ "<img src=\"images/down-chevron.png\" alt=\"Down Chevron\" class=\"icons\"/>" + "<p>Font</p>"
				+ "</div>" + "</div>" + "<ul id=\"" + layoutType + "FONT\" data-opt-name=\"" + fontKey
				+ "\" data-man=\"O\" style=\"display: none\">"
				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"" + layoutType + "FONTCOLOR\">"
				+ "<label>Font Color</label>" + "<input type=\"color\" id=\"" + layoutType
				+ "FONTCOLOR\" data-opt-name=\"color\" data-man=\"O\" value=\"#343a40\"/>" + "</li>"
				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"" + layoutType + "FONTFAMILY\">"
				+ "<label>Font Family</label>" + "<select id=\"" + layoutType
				+ "FONTFAMILY\" data-opt-name=\"family\" data-man=\"O\">" + "<option value=\"Arial\">Arial</option>"
				+ "<option value=\"Verdana\">Verdana</option>" + "<option value=\"Tahoma\">Tahoma</option>"
				+ "<option value=\"Georgia\">Georgia</option>"
				+ "<option value=\"Times New Roman\">Times New Roman</option>"
				+ "<option value=\"Courier New, monospace\">Courier New</option>"
				+ "<option value=\"'Apple System'\">Apple System</option>"
				+ "<option value=\"'Segoe UI'\">Segoe UI</option>" + "</select>" + "</li>"
				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"" + layoutType + "FONTSIZE\">"
				+ "<label>Font Size</label>" + "<input type=\"number\" id=\"" + layoutType
				+ "FONTSIZE\" data-opt-name=\"size\" data-man=\"O\"/>" + "</li>" + "</ul>";
		return fontObject;
	}

	public String getToggleButton(String chartName, String filterType) {

		String toggleButton = "<div class=\"toggle-container\">" + "<div id=\"toggleButtonFor" + filterType + chartName
				+ "\" class=\"toggle-btn active\">" + "<span class=\"on-off-text\">on</span>"
				+ "<div class=\"straight-line\">&nbsp;</div>" + "<div class=\"circle-bg\">&nbsp;</div>" + "</div>"
				+ "</div>";
		return toggleButton;
	}

	public String getTitleFilterECharts(String chartType) {
		String generalFilters = "<li class=\"general-filters active-filter\" data-column-name=\"" + chartType
				+ "TITLEECHARTS\" data-key-type=\"layout\">" + "<div class=\"sub-filterItems\">" + "<p>Title</p>"
				+ "<input type=\"text\" id=\"" + chartType
				+ "TITLEECHARTS\" data-opt-name=\"text\" data-man=\"O\" title=\"Title\"/>" + "</div>" + "</li>";
		return generalFilters;
	}

	public String getSliceLabelsECharts(String chartType) {
		String sliceLabels = "<li id=\"label-filter\" data-column-name=\"" + chartType
				+ "SLICELABELECHARTS\" data-key-type=\"data\">" + "<div class=\"main-container\">"
				+ "<div class=\"filter-container\">"
				+ "<img src=\"images/down-chevron.png\" alt=\"Down Chevron\" class=\"icons visualDarkMode\"/>"
				+ "<p>Slice Label</p>" + "</div>" + getToggleButton("SUNBURST", "") + "</div>"
				+ "<ul class=\"sub-filters\" id=\"" + chartType
				+ "SLICELABELECHARTS\" data-opt-name=\"label\" style=\"display: none;\">"
				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"" + chartType
				+ "SLICELABELDATAECHARTS\" data-key-type=\"data\">" + "<label>Label Data</label>" + "<select id=\""
				+ chartType
				+ "SLICELABELDATAECHARTS\" data-opt-name=\"formatter\" data-man=\"O\" title=\"Chart Hover Data\">"
				+ "<option value=\"None\" selected>None</option>" + "<option value=\"getLabelFormatter\">Label</option>"
				+ "<option value=\"getValueFormatter\">Value</option>"
				+ "<option value=\"getLabelAndValueLabelFormatter\" >Label and Value</option>" + "</select>" + "</li>"
				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"" + chartType
				+ "LABELROTATEECHARTS\" data-key-type=\"data\">" + "<label>Rotate</label>"
				+ "<input type=\"number\" id=\"" + chartType
				+ "LABELROTATEECHARTS\" data-opt-name=\"rotate\" value=\"0\" title= \"Rotation from -90 degrees to 90 degrees, Positive values stand for counterclockwise\" data-man=\"O\"/>"
				+ "</li>" + "<li class=\"sub-filterItems active-filter\" data-column-name=\"" + chartType
				+ "LABELPOSITIONECHARTS\" data-key-type=\"data\">" + "<label>Position</label>"
				+ "<select name=\"legend\" id=\"" + chartType
				+ "LABELPOSITIONECHARTS\" data-opt-name=\"position\" data-man=\"O\" title=\"Position\">"
				+ "<option value=\"inside\">Inside</option>" + "<option value=\"outside\">Outside</option>"
				+ "</select>" + "</li>"
//				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"" + chartType
//				+ "LABELFONTWIDTHECHARTS\" data-key-type=\"data\">" + "<label>Label Width</label>"
//				+ "<input type=\"number\" id=\"" + chartType
//				+ "LABELFONTWIDTHECHARTS\" data-opt-name=\"width\" value=\"40\" data-man=\"O\"/>" + "</li>"

				// need to fix
//				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"" + chartType
//				+ "LABELOVERFLOWECHARTS\" data-key-type=\"data\">" + "<label>Overflow</label>"
//				+ "<select name=\"legend\" id=\"" + chartType
//				+ "LABELOVERFLOWECHARTS\" data-opt-name=\"overflow\" data-man=\"O\" title=\"Position\">"
//				+ "<option value=\"truncate\">Truncate</option>" + "<option value=\"break\">Break</option>"
//				+ "<option value=\"breakAll\">Break All</option>" + "<option value=\"none\">None</option>" + "</select>"
//				+ "</li>" 

				+ getFontListForECharts(chartType) + "</ul>" + "</li>";
		return sliceLabels;

	}

	public String getFontListForECharts(String chartType) {
		String fontList = "<li class=\"sub-filterItems active-filter\" data-column-name=\"" + chartType
				+ "LABELFONTCOLORECHARTS\" data-key-type=\"data\">" + "<label>Color</label>"
				+ "<input type=\"color\" id=\"" + chartType
				+ "LABELFONTCOLORECHARTS\" data-opt-name=\"color\" value=\"#333\">" + "</li>"
				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"" + chartType
				+ "LABELFONTSIZEECHARTS\" data-key-type=\"data\">" + "<label>Font Size</label>"
				+ "<input type=\"number\" id=\"" + chartType
				+ "LABELFONTSIZEECHARTS\" data-opt-name=\"size\" value=\"12\" data-man=\"O\"/>" + "</li>"
				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"" + chartType
				+ "LABELFONTFAMILYECHARTS\" data-key-type=\"data\">" + "<label>Font Family</label>"
				+ "<select name=\"legend\" id=\"" + chartType
				+ "LABELFONTFAMILYECHARTS\" data-opt-name=\"fontFamily\" data-man=\"O\" title=\"Font Family\">"
				+ "<option value=\"sans-serif\">Sans Serif</option>" + "<option value=\"serif\">Serif</option>"
				+ "<option value=\"monospace\">Monospace</option>" + "<option value=\"Arial\">Arial</option>"
				+ "<option value=\"Courier New\">Courier New</option>"
				+ "<option value=\"'Apple System', sans-serif\">Apple System</option>" + // Added Apple System
				"<option value=\"'Segoe UI', sans-serif\">Segoe UI</option>" + // Added Segoe UI
				"</select>" + "</li>";
		return fontList;
	}

	public String getTooltipDataECharts(String chartType) {

		String tooltip = "<li id=\"label-filter\" data-column-name=\"" + chartType
				+ "TOOLTIPECHARTS\" data-key-type=\"layout\">" + "<div class=\"main-container\">"
				+ "<div class=\"filter-container\">"
				+ "<img src=\"images/down-chevron.png\" alt=\"Down Chevron\" class=\"icons visualDarkMode\"/>"
				+ "<p>Chart Hover</p>" + "</div>" + getToggleButton("SUNBURST", "chartHover") + "</div>"
				+ "<ul class=\"sub-filters\" id=\"" + chartType
				+ "TOOLTIPECHARTS\" data-opt-name=\"tooltip\" style=\"display: none;\">"
				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"" + chartType
				+ "HOVERDATAECHARTS\" data-key-type=\"data\">" + "<label>Hover Data</label>" + "<select id=\""
				+ chartType
				+ "HOVERDATAECHARTS\" data-opt-name=\"formatter\" data-man=\"O\" title=\"Chart Hover Data\">"
				+ "<option value=\"getLabelFormatter\">Label</option>"
				+ "<option value=\"getValueFormatter\">Value</option>"
				+ "<option value=\"getLabelAndValueTooltipFormatter\" selected>Label and Value</option>" + "</select>"
				+ "</li>" + "<li class=\"sub-filterItems active-filter\" data-column-name=\"" + chartType
				+ "BACKGROUNDCOLORECHARTS\" data-key-type=\"layout\">" + "<label>Background Color</label>"
				+ "<input type=\"color\" id=\"" + chartType
				+ "BACKGROUNDCOLORECHARTS\" data-opt-name=\"backgroundColor\" value=\"#333\">" + "</li>"
				+ "<li class=\"sub-filters\" data-column-name=\"" + chartType
				+ "TEXTSTYLEECHARTS\" data-key-type=\"layout\">" + getFontObjectEcharts(chartType) + "</li>" + "</ul>";
		return tooltip;
	}

	public String getFontObjectEcharts(String chartType) {
		String fontObject = "<div class=\"main-container inner-container\">" + "<div class=\"filter-container\">"
				+ "<img src=\"images/down-chevron.png\" alt=\"Down Chevron\" class=\"icons\"/>" + "<p>Font</p>"
				+ "</div>" + "</div>" + "<ul id=\"" + chartType
				+ "TEXTSTYLEECHARTS\" data-opt-name=\"textStyle\" data-man=\"O\" style=\"display: none;\">"
				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"" + chartType
				+ "FONTCOLORECHARTS\" data-key-type=\"layout\">" + "<label>Color</label>"
				+ "<input type=\"color\" id=\"" + chartType
				+ "FONTCOLORECHARTS\" data-opt-name=\"color\" value=\"#74c0fc\">" + "</li>"
				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"" + chartType
				+ "FONTSIZEECHARTS\" data-key-type=\"layout\">" + "<label>Font Size</label>"
				+ "<input type=\"number\" id=\"" + chartType
				+ "FONTSIZEECHARTS\" data-opt-name=\"size\" data-man=\"O\"/>" + "</li>"
				+ "<li class=\"sub-filterItems active-filter\" data-column-name=\"" + chartType
				+ "FONTFAMILYECHARTS\" data-key-type=\"layout\">" + "<label>Font Family</label>"
				+ "<select name=\"legend\" id=\"" + chartType
				+ "FONTFAMILYECHARTS\" data-opt-name=\"fontFamily\" data-man=\"O\" title=\"Font Family\">"
				+ "<option value=\"sans-serif\">Sans Serif</option>" + "<option value=\"serif\">Serif</option>"
				+ "<option value=\"monospace\">Monospace</option>" + "<option value=\"Arial\">Arial</option>"
				+ "<option value=\"Courier New\">Courier New</option>"
				+ "<option value=\"'Apple System', sans-serif\">Apple System</option>" + // Added Apple System
				"<option value=\"'Segoe UI', sans-serif\">Segoe UI</option>" + // Added Segoe UI
				"</select>" + "</li>" + "</ul>";
		return fontObject;
	}

	public JSONObject chartJoinTables(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		JSONArray tablesObj = new JSONArray();
		// JSONObject labelObj = new VisionUtills().getMultilingualObject(request);
		String fromTable = request.getParameter("tablesObj");
		if (fromTable != null && !"".equalsIgnoreCase(fromTable) && !"null".equalsIgnoreCase(fromTable)) {
			tablesObj = (JSONArray) JSONValue.parse(fromTable);
		}
		try {
			String tabsString = "<div id='dataMigrationTabs' class='dataMigrationTabs'>"
					+ "<div id='tabs-1' class='dataMigrationsTabsInner'>" + "</div>" + "</div>"
					+ "<div id='viewMergeJoinQueryDivId' class='viewMergeJoinQueryDivClass'></div>"
					+ "<div id='viewMergeJoinQueryErrorDivId' class='viewMergeJoinQueryErrorDivClass'></div>"
					+ "<input type='hidden' id='userEditorMergeJoinSaveId' value='false'/>";
			resultObj.put("tabsString", tabsString);
			resultObj.put("selectedJoinTables", joinDxpTransformationRules(request, tablesObj));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public String joinDxpTransformationRules(HttpServletRequest request, JSONArray tablesObj) {
		String joinTableString = "";
		try {
			if (tablesObj != null && !tablesObj.isEmpty()) {
				JSONArray sourceTablesArr = new JSONArray();
				sourceTablesArr.addAll(tablesObj);
				//
				if (sourceTablesArr != null && sourceTablesArr.size() > 1) {
					joinTableString += "<div class=\"visionEtlMappingMain\">" + ""
							+ "<div class=\"visionEtlMappingTablesDiv VisionAnalyticMappingTables\">"
							+ "<table class=\"visionEtlMappingTables\" id='EtlMappingTable'" + ">" + "<thead>"
							+ "<tr><th style='background: #f1f1f1 none repeat scroll 0 0;text-align: center' colspan=\"2\">Tables</th>";

					for (int i = 0; i < sourceTablesArr.size(); i++) {
						joinTableString += "<tr><td class=\"sourceJoinColsTd\">"
								+ "<select id=\"SOURCE_SELECT_JOIN_TABLES_" + i
								+ "\" onchange=changeSelectedTableDb(id," + i + ")  class=\"sourceColsJoinSelectBox\""
								+ ">" + "" + generateTableSelectBoxStr(sourceTablesArr, (String) sourceTablesArr.get(i),
										"SOURCE_SELECT_JOIN_TABLES_" + i + "")
								+ "" + "</select>" + "</td>" + "<td>";
						if (i != 0) {
							joinTableString += "<img src=\"images/mapping.svg\" " + " id=\"joinConditionsMap_" + i
									+ "\" "
									+ "class=\"visionEtlMapTableIcon visionEtlJoinClauseMapIcon\" title=\"Map Columns For Join\""
									+ " onclick=showDxpJoinsTables(event,'" + sourceTablesArr.get(i) + "',id," + i + ")"
									+ " style=\"width:15px;height: 15px;cursor:pointer;\"/>";
						}
						joinTableString += "</td>" + "</tr>";

					}

					joinTableString += "</tbody>" + "" + "</table>" + "</div>"
							+ "<div id=\"joinMapColumnsDivId\" class=\"joinMapColumnsDivClass\"></div>" + "</div>";

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return joinTableString;
	}

	public String generateTableSelectBoxStr(List<String> columnList, String selectedTable, String selectBoxId) {
		String selectBoxStr = "<option>Select</option>";
		try {
			for (int i = 0; i < columnList.size(); i++) {
				String table = columnList.get(i);
				String selectedStr = "";
				if (selectedTable != null && !"".equalsIgnoreCase(selectedTable)
						&& selectedTable.equalsIgnoreCase(String.valueOf(table))) {
					selectedStr = "selected";
				}
				selectBoxStr += "<option  value='" + table + "'" + " id ='" + selectBoxId + "_" + table
						+ "' data-tablename='" + table + "' " + "" + selectedStr + ">" + table + "</option>";
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return selectBoxStr;
	}

	public JSONObject fetchJoinTableColumnTrfnRules(HttpServletRequest request) {
		String joinsDataStr = "";
		JSONObject resultObj = new JSONObject();
		Connection connection = null;
		try {
			// ravi start
			String joinType = "";
			// ravi end

			JSONArray masterTablesArray = new JSONArray();
			String dbObjStr = request.getParameter("dbObj");
			String childTableName = request.getParameter("tableName");
			if (childTableName != null && !"".equalsIgnoreCase(childTableName)
					&& !"null".equalsIgnoreCase(childTableName) && childTableName.contains(".")) {
				childTableName = childTableName.substring(childTableName.lastIndexOf(".") + 1);
			}
			String masterTables = request.getParameter("sourceTables");
			if (masterTables != null && !"".equalsIgnoreCase(masterTables) && !"".equalsIgnoreCase(masterTables)) {
				masterTablesArray = (JSONArray) JSONValue.parse(masterTables);
			}
			String joinColumnMapping = request.getParameter("joinColumnMapping");
			JSONObject joinColumnMappingObj = new JSONObject();
			if (joinColumnMapping != null && !"".equalsIgnoreCase(joinColumnMapping)
					&& !"null".equalsIgnoreCase(joinColumnMapping)) {
				joinColumnMappingObj = (JSONObject) JSONValue.parse(joinColumnMapping);
			}
			// String trString = "<tr>";
			List<Object[]> childTableColumnList = new ArrayList<>();
			childTableColumnList = dashBoardsDAO.getTreeOracleTableColumns(request, childTableName);

			JSONArray childTableColsTreeArray = new JSONArray();
			if (childTableColumnList != null && !childTableColumnList.isEmpty()) {
				JSONObject tableObj = new JSONObject();
				tableObj.put("id", childTableName);// CONNECTION_NAME
				tableObj.put("text", childTableName);
				tableObj.put("value", childTableName);
				tableObj.put("icon", "images/GridDB.png");
				childTableColsTreeArray.add(tableObj);
				for (int i = 0; i < childTableColumnList.size(); i++) {
					Object[] childColsArray = childTableColumnList.get(i);
					if (childColsArray != null && childColsArray.length != 0) {
						JSONObject columnObj = new JSONObject();
						columnObj.put("id", childColsArray[0] + ":" + childColsArray[1]);
						columnObj.put("text", childColsArray[1]);
						columnObj.put("value", childColsArray[0] + ":" + childColsArray[1]);
						columnObj.put("parentid", childColsArray[0]);
						childTableColsTreeArray.add(columnObj);
					}

				}
			}
			resultObj.put("childTableColsArray", childTableColsTreeArray);

			// ravi start
			String trString = "<tr>";
			String singleTrString = "<tr>";
			singleTrString += "<td width='5%'><img src=\"images/Detele Red Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionTdETLIcons\""
					+ " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>" + "</td>";
			singleTrString += "<td width='35%' class=\"sourceJoinColsTd\"><input class='visionColJoinMappingInput' type='text' value='' readonly='true'/>"
					+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
					+ " onclick=\"selectDxpColumn(this,'childColumn')\" style=\"\"></td>";

			singleTrString += "<td width='10%' class=\"sourceJoinColsTd\">"
					+ "<select id=\"OPERATOR_TYPE\"  class=\"sourceColsJoinSelectBox\">"
					+ "<option  value='=' selected>=</option>" + "<option  value='!='>!=</option>" + "</select>"
					+ "</td>";

			singleTrString += "<td width='35%' class=\"sourceJoinColsTd\"><input class='visionColJoinMappingInput' type='text' value='' readonly='true'/>"
					+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
					+ " onclick=\"selectDxpColumn(this,'masterColumn')\" style=\"\"></td>";
			singleTrString += "<td width='10%'><input type=\"text\" class=\"defaultValues\" id=\"static_value_0\"></td>"
					+ "<td width='5%'>" + "<select id='andOrOpt'>" + "<option value='AND'>AND</option>"
					+ "<option value='OR'>OR</option>" + "</select>" + "</td>";
			singleTrString += "</tr>";

			// ravi end
			// ravi start
			JSONArray masterTableColsTreeArray = new JSONArray();
			for (int i = 0; i < masterTablesArray.size(); i++) {
				String masterTableName = (String) masterTablesArray.get(i);
//                if (request.getParameter("tableName") != null && !"".equalsIgnoreCase(request.getParameter("tableName"))
//                        && !childTableName.equalsIgnoreCase(request.getParameter("tableName"))) {
				if (masterTableName != null && !"".equalsIgnoreCase(masterTableName)
						&& !"null".equalsIgnoreCase(masterTableName) && masterTableName.contains(".")) {
					masterTableName = masterTableName.substring(masterTableName.lastIndexOf(".") + 1);
				}
				List<Object[]> columnList = new ArrayList<>();
				columnList = dashBoardsDAO.getTreeOracleTableColumns(request, masterTableName);

				if (columnList != null && !columnList.isEmpty()) {
					JSONObject tableObj = new JSONObject();
					tableObj.put("id", masterTableName);
					tableObj.put("text", masterTableName);
					tableObj.put("value", masterTableName);
					tableObj.put("icon", "images/GridDB.png");
					masterTableColsTreeArray.add(tableObj);
					for (int j = 0; j < columnList.size(); j++) {
						Object[] masterColsArray = columnList.get(j);
						if (masterColsArray != null && masterColsArray.length != 0) {
							JSONObject columnObj = new JSONObject();
							columnObj.put("id", masterColsArray[0] + ":" + masterColsArray[1]);
							columnObj.put("text", masterColsArray[1]);
							columnObj.put("value", masterColsArray[0] + ":" + masterColsArray[1]);
							columnObj.put("parentid", masterColsArray[0]);
							masterTableColsTreeArray.add(columnObj);
						}

					}
				}
//                }
			}
			resultObj.put("masterTableColsArray", masterTableColsTreeArray);
			trString = singleTrString;

// ravi end 
			// String joinType = "";
			String mappedColTrString = "";
			if (joinColumnMappingObj != null && !joinColumnMappingObj.isEmpty()) {
				Set keySet = joinColumnMappingObj.keySet();
				List keysList = new ArrayList();
				keysList.addAll(keySet);
				Collections.sort(keysList);

				for (int i = 0; i < keysList.size(); i++) {
					Object keyName = keysList.get(i);
					JSONObject joinColMapObj = (JSONObject) joinColumnMappingObj.get(keysList.get(i));
					if (joinColMapObj != null && !joinColMapObj.isEmpty()) {
						joinType = (String) joinColMapObj.get("joinType");
						mappedColTrString += "<td width='5%' ><img src=\"images/Detele Red Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionTdETLIcons\""
								+ " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>" + "</td>";
						mappedColTrString += "<td width='35%' class=\"sourceJoinColsTd\">"
								+ "<input class='visionColJoinMappingInput' type='text' value='"
								+ (String) joinColMapObj.get("childTableColumn") + "' readonly='true'/>"
								+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
								+ " onclick=\"selectDxpColumn(this,'childColumn')\" style=\"\"></td>";

						String operator = (String) joinColMapObj.get("operator");

						mappedColTrString += "<td width='10%' class=\"sourceJoinColsTd\">"
								+ "<select id=\"OPERATOR_TYPE\"  class=\"sourceColsJoinSelectBox\">";
						mappedColTrString += "<option  value='=' " + ("=".equalsIgnoreCase(operator) ? "selected" : "")
								+ ">=</option>";
						mappedColTrString += "<option  value='!=' "
								+ ("!=".equalsIgnoreCase(operator) ? "selected" : "") + ">!=</option>";
						mappedColTrString += "</select>" + "</td>";
						mappedColTrString += "<td width='35%' class=\"sourceJoinColsTd\">"
								+ "<input class='visionColJoinMappingInput' type='text' value='"
								+ (String) joinColMapObj.get("masterTableColumn") + "' readonly='true'/>"
								+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
								+ " onclick=\"selectDxpColumn(this,'masterColumn')\" style=\"\"></td>";
						mappedColTrString += "" + "<td width='10%'><input type=\"text\" " + "value='"
								+ ((joinColMapObj.get("staticValue") != null
										&& !"".equalsIgnoreCase(String.valueOf(joinColMapObj.get("staticValue")))
										&& !"null".equalsIgnoreCase(String.valueOf(joinColMapObj.get("staticValue"))))
												? String.valueOf(joinColMapObj.get("staticValue"))
												: "")
								+ "' " + " class=\"defaultValues\" id=\"static_value_" + i + "\"></td>"
								+ "<td width='5%'>" + "<select id='andOrOpt'>";
						String andOrOperator = (String) joinColMapObj.get("andOrOperator");
						mappedColTrString += "<option value='AND' "
								+ ("AND".equalsIgnoreCase(andOrOperator) ? "selected" : "") + ">AND</option>";
						mappedColTrString += "<option value='OR' "
								+ ("OR".equalsIgnoreCase(andOrOperator) ? "selected" : "") + ">OR</option>";
						mappedColTrString += "</select>" + "</td>";
						mappedColTrString += "</tr>";
					}

				}
			}
			if (!(mappedColTrString != null && !"".equalsIgnoreCase(mappedColTrString)
					&& !"null".equalsIgnoreCase(mappedColTrString))) {
				mappedColTrString = trString;
			}
			joinsDataStr += "<div class=\"visionEtlJoinClauseMain visionAnalyticsJoinClauseMain\">"
					+ "<div class=\"visionEtlAddIconDiv\">"
					+ "<img data-trstring='' src=\"images/Add icon.svg\" id=\"visionDxpAddRowIcon\" "
					+ "class=\"visionDxpAddRowIcon\" title=\"Add column for mapping\""
					+ " onclick=addNewDxpJoinsRow(event,'" + dbObjStr + "',id) "
					+ "style=\"width:15px;height: 15px;cursor:pointer; float: left;\"/>"
					+ "<img data-trstring='' src=\"images/Save Icon.svg\" id=\"visionEtlSaveIcon\" "
					+ "class=\"visionDxpAddRowIcon\" title=\"Save Mapping\"" + " onclick=saveDxpJoinMapping(event,id) "
					+ "style=\"width:15px;height: 15px;cursor:pointer; float: left;\"/>"
					+ "<span class='visionDxpColumnJoinType'>Join Type : </span>"
					+ "<select class='visionDxpColumnJoinType' id='joinType'>" + "<option value='INNER JOIN' "
					+ ("INNER JOIN".equalsIgnoreCase(joinType) ? "selected" : "") + " >Inner Join</option>"
					+ "<option value='JOIN' " + ("JOIN".equalsIgnoreCase(joinType) ? "selected" : "") + ">Join</option>"
					+ "<option value='LEFT OUTER JOIN' "
					+ ("LEFT OUTER JOIN".equalsIgnoreCase(joinType) ? "selected" : "") + ">Left Outer Join</option>"
					+ "<option value='RIGHT OUTER JOIN' "
					+ ("RIGHT OUTER JOIN".equalsIgnoreCase(joinType) ? "selected" : "") + ">Right Outer Join</option>"
					+ "<option value='FULL OUTER JOIN' "
					+ ("FULL OUTER JOIN".equalsIgnoreCase(joinType) ? "selected" : "") + ">Full Outer Join</option>"
					+ "</select>" + "</div>" + "<div class=\"visionDxpJoinClauseTablesDiv\">"
					+ "<table class=\"visionEtlJoinClauseTable\" id='etlJoinClauseTable' style='width: 100%;' border='1'>"
					+ "<thead>" + "<tr>"
					+ "<th width='5%' style='background: #f1f1f1 none repeat scroll 0 0;text-align: center'></th>"
					+ "<th width='35%' style='background: #f1f1f1 none repeat scroll 0 0;text-align: center'>Child Column</th>"
					+ "<th width='10%' style='background: #f1f1f1 none repeat scroll 0 0;text-align: center'>Operator</th>"
					+ "<th width='35%' style='background: #f1f1f1 none repeat scroll 0 0;text-align: center'>Master Column</th>"
					+ "<th width='10%' style='background: #f1f1f1 none repeat scroll 0 0;text-align: center'>Static Value</th>"
					+ "<th width='5%' style='background: #f1f1f1 none repeat scroll 0 0;text-align: center'>AND/OR</th>"
					+ "" + "</tr>" + "</thead>" + "<tbody>" + "";
			joinsDataStr += mappedColTrString + "</tbody>" + "" + "</table>" + "" + "</div>" + "</div>";
			resultObj.put("joinsDataStr", joinsDataStr);
			resultObj.put("trString", singleTrString); // ravi edit

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
			}
		}
		return resultObj;

	}

	public JSONObject fetchCardDetails(HttpServletRequest request) {
		return dashBoardsDAO.fetchCardDetails(request);
	}

	public JSONObject fetchHomeCardDetails(HttpServletRequest request) {
		return dashBoardsDAO.fetchHomeCardDetails(request);
	}

	public JSONObject fetchpredictiveChartData(HttpServletRequest request) {
		return dashBoardsDAO.fetchpredictiveChartData(request);
	}

	public String saveVisualizeData(HttpServletRequest request) {
		return dashBoardsDAO.saveVisualizeData(request);
	}

	public String getchartElement(HttpServletRequest request) {

		String result = "<div class='searchedDxpSearchResults' id='searchedDxpSearchResults'>";
		int Count = 0;
		String className = "";
		try {

			String typedValue = request.getParameter("typedValue");
			String domainValue = request.getParameter("domainValue");
			String chartid = request.getParameter("chartid");
			// String count = request.getParameter("count");
			List resultList = new ArrayList();
			resultList.add("Axex");
			resultList.add("Axex Titles");
			resultList.add("Chart Titles");
			resultList.add("Data Lebel");
			resultList.add("Data Table");
			resultList.add("Trendline");

			if (resultList != null && !resultList.isEmpty()) {
				for (int i = 0; i < resultList.size(); i++) {
					String name = (String) resultList.get(i);
					result += "<div class='searchFilterResultsList'>"
							// + "<input type='checkbox' name='dxpFilterSearchCheckBox'
							// class='dxpFilterSearchCheckClass' id='dxpFilterSearchCheckId' value='" + name
							// + "'/>"
							+ "<span class=\"chartElementPopContentTitle\">" + name + "</span>"
							+ "<div id='chartelementId" + Count
							+ "' class='chartElementImgClass'> <img onclick=\"getChartContent('chartelementId" + Count
							+ "','" + chartid + "')\"  src='images/nextrightarrow.png' title='next row' />" + "</div>"
							+ "</div>";
					Count++;

				}
			}

			result += "</div>";
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public String getchartchildElement(HttpServletRequest request) {
		String result = "<div class='searchedDxpSearchResults' id='searchedDxpSearchResults'>";
		int Count = 0;
		String className = "";
		try {
			String typedValue = request.getParameter("typedValue");
			String domainValue = request.getParameter("domainValue");
			String chartid = request.getParameter("chartid");
			String chartType = request.getParameter("chartType");
			String count = request.getParameter("count");
			JSONObject movingAvgObj = new JSONObject();
			// movingAvgObj.put("Linear", "L");
			movingAvgObj.put("Exponential", "E");
			movingAvgObj.put("Moving avgerage", "M");
			result += "<div class='selectapredictionmethodClass'>Select a prediction method</div>";
			for (Object key : movingAvgObj.keySet()) {
				String keyStr = (String) key;
				Object keyvalue = movingAvgObj.get(keyStr);
				result += "<div class='searchFilterResultsList' onclick=\"getpredictivechart(event,'" + chartid + "','"
						+ chartType + "','" + count + "','" + keyvalue + "')\">"
						+ "<div class='chartElementPopContentTitle1'>" + keyStr + "</div>" + "</div>";
			}
			result += "</div>";
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public String getButtons() {
		JSONObject buttonobj = new JSONObject();
		String result = "<span>"
				/*
				 * +
				 * "<button type='button' value='Emport' class='emportclasss btn ' onclick=\"showVisuvalization()\">Import</button>"
				 * +
				 * "<button type='button' value='Save' class='Saveclasss btn ' onclick=\"saveVisualizationData()\">Save</button>"
				 * +
				 * "<button type='button' value='Analysis' class='Analysisclasss btn ' onclick=\"showSlicerField1()\">Analysis</button>"
				 * +
				 * "<button type='button' value='Format' class='Formateclasss btn ' onclick=\"showSlicerField1()\">Format</button>"
				 */
				// + "<button type='button' value='IntelliSense' class='Analysisclasss btn '
				// onclick=\"showIntelliSenseSuggestions()\">IntelliSense</button>"
				+ "</span>";
		return result;
	}

	public JSONObject getChartData(HttpServletRequest request) {
		return dashBoardsDAO.getChartData(request);
	}

	public JSONObject getfilterColumnData(HttpServletRequest request) {
		String Resultstr = "";
		JSONObject resultobj = new JSONObject();
//        String tablename = request.getParameter("table");
		try {
			// JSONObject filtercolumn = dashBoardsDAO.getcharttableattr(request);
			String result = dashBoardsDAO.getLoadTableColumns(request);
			JSONObject savedFilterObj = dashBoardsDAO.getSaveFilterColumns(request);
			Resultstr = "<div id='FilterColumndataId' class = 'FilterColumndataClass'>"
					+ "<div id=\"VisualizeBIFilterColumns\"></div>"
					+ "<div id=\"visualizeChartConfigFiltersData\" class=\"visualizeChartConfigFiltersClass\"></div>"
					+ "</div>";

			resultobj.put("Resultstr", Resultstr);
			resultobj.put("result", result);
			resultobj.put("savedFilterObj", savedFilterObj);
			// resultobj.put("filtercolumn", filtercolumn);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultobj;

	}

	public JSONObject getchartGrid(HttpServletRequest request) {
		JSONObject gridObj = new JSONObject();
		try {
			String gridId = request.getParameter("gridId");
			gridObj = dashBoardsDAO.getGrid(gridId, request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return gridObj;
	}

	public List getSerachResults(List gridColArray, String tableName, String gridId, HttpServletRequest request) {
		List dataList = new ArrayList();
		try {
			JSONArray paramArray = new JSONArray();
			String paramArrayStr = request.getParameter("paramArray");
			if (paramArrayStr != null && !"".equalsIgnoreCase(paramArrayStr)) {
				paramArray = (JSONArray) JSONValue.parse(paramArrayStr);
			}
			dataList = cloudGridResultsDAO.getSerachResults(gridColArray, tableName, gridId, request, paramArray);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return dataList;
	}

	public String deleteVisualizeChart(HttpServletRequest request) {
		return dashBoardsDAO.deleteVisualizeChart(request);
	}

	public String create(HttpServletRequest request) {
		String result = "<div class='searchedDxpSearchResults' id='searchedDxpSearchResults'>";
		int Count = 0;
		String className = "";
		try {
			String chartid = request.getParameter("chartid");
			// String count = request.getParameter("count");
			List resultList = new ArrayList();
			resultList.add("Single");
			resultList.add("Multiple");
			if (resultList != null && !resultList.isEmpty()) {
				for (int i = 0; i < resultList.size(); i++) {
					String name = (String) resultList.get(i);
					result += "<div class='createpopupResultsList' onclick=\"callElements(event)\">"
							// + "<input type='checkbox' name='dxpFilterSearchCheckBox'
							// class='dxpFilterSearchCheckClass' id='dxpFilterSearchCheckId' value='" + name
							// + "'/>"
							+ "<span class=\"chartElementPopContentTitle\">" + name + "</span>" + "</div>";
					Count++;

				}
			}

			result += "</div>";
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public String updatechartdata(HttpServletRequest request) {
		return dashBoardsDAO.updatechartdata(request);
	}

	public String dashboardSetting(HttpServletRequest request) {
		String result = "";
		try {

			result = "<div class='dxpDataAnalyticswrapper'>"
					+ "<div id=\"columnsMainDivwrapperID\" class='columnsMainDivwrapper width60'>"
					+ "<div id=\"visualizationMainDivwrapperID\" class='visualizationMainDivwrapper width60'>"
					+ "<div class=\"visualizationHeaderDiv\" onclick=\"visualizationDivToggle()\">"
					+ "<span class=\"visualizationtitle\">" + "<h4>Visualizations</h4>" + "</span>"
					+ "<span class=\"toggleImg\" id=\"visualToggleIcon\"><img src=\"images/toggle_blueIcon.png\" width=\"16px;\"></span>"
					+ "</div>" + "<div id =\"Visualization\" class='VisionAnalyticsBICharts visualBIChart'>"
					+ "<div id=\"VisionBIVisualization\">"
					+ "<div id = 'visionVisualizeSlicerId' class='visionVisualizeSlicerClass'>"
					+ "<div class='visionVisualizeSlicerImageDivClass'><img src=\"images/Chart_Slicer.svg\" onclick=\"showSlicerField('visionVisualizeSlicerFieldId')\" width=\"20px\" id=\"VisionVisualizationSlicerImageId\" class=\"VisionVisualizationSlicerImageClass\" title=\"Click for Slicer\"/></div>"
					+ "<div id ='visionVisualizeSlicerFieldId' class='visionVisualizeSlicerFieldClass' style='display:none'><span>Drop Fields Here</span></div>"
					+ "</div>" + "<div id='visualizeConfigTabs' class='visualizeConfigTabsClass'>"
					+ "<ul id='visionVisualizeConfig'>"
					+ "<li id='visionVisualizeFields' class='visionVisualizeFieldsClass'><img src='images/Fields_Selection.svg' style='cursor:pointer;' onclick=\"showChartConfigurationDIv('visualizeChartConfigColumns','visionVisualizeFields')\"/></li>"
					+ "<li id='visionVisualizeConfiguration' class='visionVisualizeConfigurationClass'><img src='images/Chart_Config.svg' style='cursor:pointer;' onclick=\"showChartConfigurationDIv('visualizeChartConfigProperties','visionVisualizeConfiguration')\"/></li>"
					+ "<li id='visionVisualizeFilters' class='visionVisualizeFiltersClass'><img src='images/Filter.svg' style='cursor:pointer;' onclick=\"showChartConfigurationDIv('visualizeChartConfigFilters','visionVisualizeFilters')\"/></li>"
					+ "<li id='visionVisualizeJoins' class='visionVisualizeJoinsClass'><img src='images/mapping.svg' style='cursor:pointer;' onclick=\"showChartConfigurationDIv('visualizeChartConfigJoins','visionVisualizeJoins')\"/></li>"
					+ "</ul>" + "</div>"
					+ "<div id=\"visualizeChartConfigColumns\" class=\"visualizeChartConfigColumnsClass\"></div>"
					+ "<div id=\"visualizeChartConfigProperties\" class=\"visualizeChartConfigPropertiesClass\" style='display:none'></div>"
					+ "<div id=\"visualizeChartConfigFilters\" class=\"visualizeChartConfigFiltersClass\" style='display:none'></div>"
					+ "<div id=\"visualizeChartConfigJoins\" class=\"visualizeChartConfigJoinsClass\" style='display:none'></div>"
					// + "</div>"
					+ "</div>" + "</div>" + "</div>" + "<div class=\"chartView\" id=\"visualizeArea\">"
					+ "<div class=\"visionVisualizationDataChartcount\" id=\"visionVisualizationDataChartcount\">"
					+ "<div class=\"visionVisualizationDataChartViewCLass\" id=\"visionVisualizationDataChartViewId\">"
					+ "</div>" + "</div>" + "</div>";
		} catch (Exception e) {
			e.printStackTrace();
		}

		return result;
	}

	public JSONObject getconfigobject(HttpServletRequest request) {
		return dashBoardsDAO.getconfigobject(request);
	}

	public JSONObject getCurrentDBTables(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();

		try {
			resultObj = dashBoardsDAO.getCurrentDBTables(request);

		} catch (Exception e) {
		}
		return resultObj;
	}

	public JSONObject getChartFilterData(HttpServletRequest request) {
		return dashBoardsDAO.getChartFilterData(request);
	}

	public JSONObject getHomeChartSlicerData(HttpServletRequest request) {
		return dashBoardsDAO.getHomeChartSlicerData(request);
	}

	public JSONObject fetchHomeSlicerValues(HttpServletRequest request) {
		return dashBoardsDAO.fetchHomeSlicerValues(request);
	}

	public JSONObject getSlicerHomeCharts(HttpServletRequest request) {
		return dashBoardsDAO.getSlicerHomeCharts(request);
	}

	public JSONObject movingAvgData(HttpServletRequest request) {
		List selectData = null;
		JSONObject chartObj = new JSONObject();
		List<String> columnKeys = new ArrayList<>();
		String chartConfigObjStr = "";
		JSONObject filteredChartConfigObj = new JSONObject();
		JSONObject chartConfigObj = new JSONObject();
		try {
			String chartType = request.getParameter("chartType");
			String chartId = request.getParameter("chartId");
			String chartConfigPositionKeyStr = request.getParameter("chartConfigPositionKeyStr");
			JSONObject chartListObj = dashBoardsDAO.movingAvgData(request);
			if (chartListObj != null && !chartListObj.isEmpty()) {
				selectData = (List) chartListObj.get("chartList");
				columnKeys = (List<String>) chartListObj.get("columnKeys");

			}
			JSONObject dataObject = dashBoardsDAO.getVisualizationData(request, chartId);
			chartConfigObjStr = (String) dataObject.get("chartPropObj");
			chartConfigPositionKeyStr = (String) dataObject.get("chartConfigObj");
			if (chartConfigObjStr != null && !"".equalsIgnoreCase(chartConfigObjStr)
					&& !"null".equalsIgnoreCase(chartConfigObjStr)) {
				chartConfigObj = (JSONObject) JSONValue.parse(chartConfigObjStr);
			}

			for (Object chartKey : chartConfigObj.keySet()) {
				String key = String.valueOf(chartKey);
				String filteredKey = key.replaceAll("\\d", "");
				filteredChartConfigObj.put(filteredKey, chartConfigObj.get(key));
			}
			JSONObject configObj = dashBoardsDAO.buildOptionsObj(request, filteredChartConfigObj,
					chartConfigPositionKeyStr, chartId, chartType);
			JSONObject layoutObj = (JSONObject) configObj.get("layoutObj");
			JSONObject dataPropObj = (JSONObject) configObj.get("dataObj");
			JSONObject framedChartDataObj = dashBoardsDAO.getFramedMovingAvgDataObject(selectData, columnKeys,
					layoutObj, dataPropObj);
			if (framedChartDataObj != null && !framedChartDataObj.isEmpty()) {
				chartObj.put("layout", (JSONObject) framedChartDataObj.get("layoutObj"));
				if (chartType != null && !"".equalsIgnoreCase(chartType) && "treemap".equalsIgnoreCase(chartType)) {
					JSONObject treeMapDataObj = dashBoardsDAO.getTreeMapDataObject(framedChartDataObj, columnKeys);
					if (treeMapDataObj != null && !treeMapDataObj.isEmpty()) {
						chartObj.put("treeMapCol", treeMapDataObj.get("treeMapColObj"));
						chartObj.put("data", treeMapDataObj.get("data"));
					}
				} else {
					chartObj.put("data", (JSONObject) framedChartDataObj.get("dataObj"));
				}
			}
			chartObj.put("dataPropObject", dataPropObj);
			chartObj.put("columnObj", chartListObj.get("columnObj"));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return chartObj;

	}

	public String insertdata(HttpServletRequest request) {
		return dashBoardsDAO.insertdata(request);
	}

	public JSONObject getlandingGraphData(HttpServletRequest request) {
		return dashBoardsDAO.getlandingGraphData(request);
	}

	public String getdashbordname(HttpServletRequest request) {
		return dashBoardsDAO.getdashbordname(request);
	}

	public JSONObject getJqxPivotGridData(String gridId, HttpServletRequest request) {
		return this.dashBoardsDAO.getJqxPivotGridData(gridId, request);
	}

	public JSONObject getPivotGridData(String gridId, HttpServletRequest request) {
		return this.dashBoardsDAO.getPivotGridData(gridId, request);
	}

	public String updatechartSettingdata(HttpServletRequest request) {
		return dashBoardsDAO.updatechartSettingdata(request);
	}

	public JSONObject getSchemaObjectData(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.getObjectdata(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject showDuplicateData(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.showDuplicateData(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject removeDuplicateData(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.removeDuplicateData(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public String gridUpdateRecords(HttpServletRequest request, JSONObject newGridData, String baseTableName,
			String gridId) {
		String resultobj = "";
		try {
			resultobj = dashBoardsDAO.gridUpdateRecords(request, newGridData, baseTableName, gridId);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultobj;

	}

	public JSONObject saveHomeChartsColorsData(HttpServletRequest request) {
		return dashBoardsDAO.saveHomeChartsColorsData(request);
	}

	public JSONObject getSurveyHomeCharts(HttpServletRequest request) {
		return dashBoardsDAO.getSurveyHomeCharts(request);
	}

	public String updteFilterColumn(HttpServletRequest request) {
		String result = "";
		try {
			result = dashBoardsDAO.updteFilterColumn(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public String updteDrillDownColumns(HttpServletRequest request) {
		String result = "";
		try {
			result = dashBoardsDAO.updteDrillDownColumns(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public String saveChartRadioButtonColumns(HttpServletRequest request) {
		String result = "";
		try {
			result = dashBoardsDAO.saveChartRadioButtonColumns(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public String updteCompareFilterColumn(HttpServletRequest request) {
		return dashBoardsDAO.updteCompareFilterColumn(request);

	}

	public JSONObject getHomeChartHeaderFilterForm(HttpServletRequest request) {
		return dashBoardsDAO.getHomeChartHeaderFilterForm(request);
	}

	public JSONObject getHomeChartDrillDownColumnForm(HttpServletRequest request) {
		return dashBoardsDAO.getHomeChartDrillDownColumnForm(request);
	}

	public JSONObject updteCompareFilterColumnsData(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		String result = "";
		try {
			JSONObject compareFilterDataObj = new JSONObject();
			String compareFilterData = request.getParameter("compareFilterData");
			String dashBoardName = request.getParameter("dashbordName");
			if (compareFilterData != null && !"".equalsIgnoreCase(compareFilterData)
					&& !"".equalsIgnoreCase(compareFilterData)) {
				compareFilterDataObj = (JSONObject) JSONValue.parse(compareFilterData);
			}
			JSONArray dataColArr = new JSONArray();
			JSONObject dataColObj = new JSONObject();
			result = "<div id='dashBoardHomeCompareFiltersId' class=\"dashBoardHomeCompareFiltersClass row\">";
			if (compareFilterDataObj != null && !compareFilterDataObj.isEmpty()) {
				String chartCount = "";
				result += "<div id='dashBoardHomeChartOneCompareFiltersId' class='dashBoardHomeChartOneCompareFiltersClass col-md-6 col-sm-6 col-lg-6'>";
				result += "<p class='dashBoardHomeChartOneCompareFiltersTableSpanClass'>Chart 1:</p>";
				JSONArray compareOneFiltersArr = (JSONArray) compareFilterDataObj.get("chart1");
				result += "<table id='dashBoardHomeChartOneCompareFiltersTableId' class= 'dashBoardHomeChartOneCompareFiltersTableClass'>";
				for (int i = 0; i < compareOneFiltersArr.size(); i++) {
					String columnVal = (String) compareOneFiltersArr.get(i);
					chartCount = "one";
					result += "<tr class='visionDashBoardCompareChartFiltersTrClass'>";
					if (columnVal != null && !"".equalsIgnoreCase(columnVal)) {
						String colName = columnVal.split("\\.")[1];
						if (colName != null && !"".equalsIgnoreCase(colName) && colName.contains("DATE")) {
							JSONObject dateNormal = new JSONObject();
							dateNormal.put("tbid", "tbone" + i);
							dateNormal.put("type", "normal");
							dataColArr.add(dateNormal);
							JSONObject dateMin = new JSONObject();
							dateMin.put("tbid", "tbminone" + i);
							dateMin.put("type", "min");
							dataColArr.add(dateMin);
							JSONObject dateMax = new JSONObject();
							dateMax.put("tbid", "tbmaxone" + i);
							dateMax.put("type", "max");
							dataColArr.add(dateMax);
							result += "<td id='td" + i + "' data-columnName='" + columnVal
									+ "' data-Range='Y' class='visionDashBoardCompareChartFiltersTdClass'>";
						} else {
							result += "<td id='td" + i + "' data-columnName='" + columnVal
									+ "' data-Range='N' class='visionDashBoardCompareChartFiltersTdClass'>";
						}

						String values = dashBoardsDAO.getSurveyAnalyticPartyWiseFilters(columnVal.split("\\.")[0],
								columnVal.split("\\.")[1], i, chartCount);
						if (values != null && !"".equalsIgnoreCase(values)) {
							JSONObject valueObj = (JSONObject) JSONValue.parse(values);
							if (valueObj != null && !valueObj.isEmpty()) {
								result += (String) valueObj.get("result");
								if (colName != null && !"".equalsIgnoreCase(colName) && !colName.contains("DATE")) {
									dataColObj.put("tbValues" + chartCount + i, valueObj.get("checkBoxDataArr"));
								}
							}
						}
						result += "</td>";
						result += "</tr>";
					}
				}
				result += "</table>";
				result += "</div>";
				result += "<div id='dashBoardHomeChartTwoCompareFiltersId' class='dashBoardHomeChartTwoCompareFiltersClass col-md-6 col-sm-6 col-lg-6'>";
				JSONArray compareTwoFiltersArr = (JSONArray) compareFilterDataObj.get("chart2");
				result += "<p class='dashBoardHomeChartTwoCompareFiltersTableSpanClass'>Chart 2:</p>";
				result += "<table id='dashBoardHomeChartTwoCompareFiltersTableId' class= 'dashBoardHomeChartTwoCompareFiltersTableClass'>";
				for (int i = 0; i < compareTwoFiltersArr.size(); i++) {
					chartCount = "two";
					result += "<tr class='visionDashBoardCompareChartFiltersTrClass'>";
					String columnVal = (String) compareTwoFiltersArr.get(i);
					if (columnVal != null && !"".equalsIgnoreCase(columnVal)) {
						String colName = columnVal.split("\\.")[1];
						if (colName != null && !"".equalsIgnoreCase(colName) && colName.contains("DATE")) {
							JSONObject dateNormal = new JSONObject();
							dateNormal.put("tbid", "tbtwo" + i);
							dateNormal.put("type", "normal");
							dataColArr.add(dateNormal);
							JSONObject dateMin = new JSONObject();
							dateMin.put("tbid", "tbmintwo" + i);
							dateMin.put("type", "min");
							dataColArr.add(dateMin);
							JSONObject dateMax = new JSONObject();
							dateMax.put("tbid", "tbmaxtwo" + i);
							dateMax.put("type", "max");
							dataColArr.add(dateMax);
							result += "<td id='td" + i + "' data-columnName='" + columnVal
									+ "' data-Range='Y' class='visionDashBoardCompareChartFiltersTdClass'>";
						} else {
							result += "<td id='td" + i + "' data-columnName='" + columnVal
									+ "' data-Range='N' class='visionDashBoardCompareChartFiltersTdClass'>";
						}

						String values = dashBoardsDAO.getSurveyAnalyticPartyWiseFilters(columnVal.split("\\.")[0],
								columnVal.split("\\.")[1], i, chartCount);
						if (values != null && !"".equalsIgnoreCase(values)) {
							JSONObject valueObj = (JSONObject) JSONValue.parse(values);
							if (valueObj != null && !valueObj.isEmpty()) {
								result += (String) valueObj.get("result");
								if (colName != null && !"".equalsIgnoreCase(colName) && !colName.contains("DATE")) {
									dataColObj.put("tbValues" + chartCount + i, valueObj.get("checkBoxDataArr"));
								}
							}
						}
						result += "</td>";
						result += "</tr>";
					}
				}

				result += "</table>";
				result += "</div>";
			}
			result += "</div>";
			result += "<div class ='visionDbCompareChartsFilterButtonDivClass'><button type='button' class='visionDbCompareChartsFilterButton btn btn-primary' value='Apply' onclick=\"applyCompareChartFilters('"
					+ dashBoardName + "')\">Apply</button></div>";
			resultObj.put("result", result);
			resultObj.put("jsDateItems", dataColArr);
			resultObj.put("dataColObj", dataColObj);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject createFilterHeader(HttpServletRequest request) {
		return dashBoardsDAO.createFilterHeader(request);
	}

	public JSONObject getDrillDownFilterColumns(HttpServletRequest request) {
		String Resultstr = "";
		JSONObject resultobj = new JSONObject();
//        String tablename = request.getParameter("table");
		try {
			String tableName = dashBoardsDAO.getChartTable(request);
			String result = dashBoardsDAO.getLoadTableColumns(request, tableName);
			JSONObject chartConfigObj = getChartFilters(request);
			resultobj.put("chartConfigObj", chartConfigObj);
			JSONObject dropDownObj = new JSONObject();
			dropDownObj.put("chartConfigObj", chartConfigObj.toString());
			Resultstr = "<div id='FilterColumndataId' class = 'FilterColumndataClass'>"
					+ "<div id=\"VisualizeBIFilterColumns\"></div>"
					+ "<div id=\"visualizeChartConfigFiltersData\" class=\"visualizeChartConfigFiltersClass\"></div>"
					+ "</div>";
			String chartTypes = "<select id='drillDownChartTypeId' class='drillDownChartTypeClass' onchange=getDrillDownConfigByChartType()>"
					+ "<option value='pie'>Pie</option>" + "<option value='donut'>Donut</option>"
					+ "<option value='bar'>Bar</option>" + "<option value='column'>Column</option>"
					+ "<option value='lines'>Line</option>" + "</select>";
			resultobj.put("Resultstr", Resultstr);
			resultobj.put("result", result);
			resultobj.put("tableName", tableName);
			resultobj.put("chartTypes", chartTypes);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultobj;

	}

	public void downloadALLChartsInPDF(HttpServletRequest request, HttpServletResponse response) {
		OutputStream os = null;
		com.itextpdf.text.Document document = null;
		try {
			String dashBoardName = request.getParameter("chartImagesDashBoardName");
			String filename = "ALLCharts_" + dashBoardName + ".pdf";
//			String filename = "ALLCharts.pdf";
			File filelocation = new File(fileStoreHomedirectory);
			if (!filelocation.exists()) {
				filelocation.mkdir();
			}
			String filepath = filelocation.getAbsolutePath() + File.separator + filename;

			response.reset();
			response.setContentType("application/pdf");
			response.setCharacterEncoding("UTF-8");
			response.setHeader("Content-disposition", "attachment; filename=\"" + filename + "\"");

			document = new com.itextpdf.text.Document(PageSize.A4, 50, 50, 50, 50);
			PdfWriter.getInstance(document, new FileOutputStream(filepath));
			document.open();

			String imgObjStr = request.getParameter("chartImageObj");

			if (imgObjStr != null && !imgObjStr.isEmpty()) {
				JSONObject imgObj = (JSONObject) JSONValue.parse(imgObjStr);

				if (imgObj != null && !imgObj.isEmpty()) {
					int x = 30;
					int y = 550;
					int key = 1;
					int maxHeight = 250;

					for (int i = 1; i <= imgObj.size(); i++) {
						String image = (String) imgObj.get(String.valueOf(i));
						if (image == null)
							continue;

						Image img = getImage(image);
						float width = img.getWidth();
						boolean largeImage = width > 1000;
						img.scaleAbsoluteWidth(largeImage ? 600f : 250f);
						img.scaleAbsoluteHeight(220f);
						img.setWidthPercentage(100);

						if (y - 30 < 0) {
							document.newPage();
							x = 30;
							y = 550;
						}

						img.setAbsolutePosition(x, y);
						document.add(img);

						x += largeImage ? 0 : 270;
						if (largeImage || key % 2 == 0) {
							y -= maxHeight + 10;
							x = 30;
						}

						key++;
					}

					document.close();

					os = response.getOutputStream();
					try (InputStream fis = new FileInputStream(filepath)) {
						byte[] bufferData = new byte[1024];
						int read;
						while ((read = fis.read(bufferData)) != -1) {
							os.write(bufferData, 0, read);
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (os != null)
					os.close();
				if (document != null && document.isOpen())
					document.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public Image getImage(String src) throws DocumentException {
		int pos = src.indexOf("base64,");
		Image image = null;
		try {
			if (src.startsWith("data") && pos > 0) {
				byte[] img = Base64.decode(src.substring(pos + 7).getBytes());
				image = Image.getInstance(img);

			} else {
				image = Image.getInstance(src);
			}
		} catch (IOException ex) {
			System.out.println("BadElementException :: " + ex.getMessage());
		}
		return image;
	}

	public JSONObject getChartHomePageDiv(HttpServletRequest request) {
		JSONObject homePageDivObj = new JSONObject();
		try {

			String divStr = "<div id=\"mainintelliSenseSelectBoxId\" class='intelliSenseSelectBoxClass' onclick=\"scrollerWheel()\">"
					+ "<div class=\"iconMenuNavPrev\"><i class=\"fa fa fa-angle-double-left\"></i></div>"
					+ "<div id='mainintelliSenseInnerSelectBoxId'class='intelliSenseSelectBoxClass'></div>"
					+ "<div class=\"chartSelectionsDropDown\" style=\"display:none\">"
					+ "<div id=\"visionHomePageSlicer\" class=\"visionHomePageSlicerClass\"></div>"
					+ "<div id=\"visionFilterData\" class=\"visionFilterData\"></div>"
					+ "<div id=\"visionHomeKanbanView\" class=\"visionHomeKanbanViewClass\"></div>"
					+ "<div id=\"visionChartColorPalleteId\" class=\"visionChartColorPalleteClass\"></div>" + "</div>"
					+ "<div id=\"sharedUserNamesListId\" class='visionVisualizeHomePageDropdown' style='display:none'>"
					+ "</div>" + "<div id=\"OptionListData\" class='visionVisualizeHomePageDropdown'>" + "</div>"
					+ "<div class=\"expendInOutDivClass\" id=\"expendInOutDivID\" onclick=\"shrinkExpandCard()\"><img src=\"images/up-arrow-caret.png\" alt=\"caret_down_icon\"></div>"
					+ "<div id=\'visionDashBoardHomeFilterId\' class=\'visionDashBoardHomeFilterClass\' style=\"display:none\">"
					+ "</div>"
					+ "<div id=\'visionDashBoardResetDivId\' class=\'visionDashBoardResetDivClass\' style=\"display:none\">"
					+ "<button class=\"btn btn-primary\" onclick=\"resetHeaderFilters();\"><img src=\"images/reset_d.png\" alt=\"reset\">Reset</button>"
					+ "</div>" + "<div class=\"iconMenuNavNext\"><i class=\"fa fa fa-angle-double-right\"></i></div>"
					+ "</div>"

					+ "<section class=\"visualizationDashboardView\" style=\"display:none\">"
					+ "<div class=\"container-fluid\">" + "<div class=\"row\">"
					+ " <div id='upperCpmpaireMainDIvID' class=\"col-12\">"

					+ "<div id='visionDashBoardHomeCompareFilterId' class='visionDashBoardHomeFilterClass row' style=\"display:none\"></div>"
					+ "<div id=\"visionCardView\" class=\"visionCardViewClass\">" + "</div>" + "</div>" + "</div>"
					+ "<div class=\"container-fluid\" id ='visualizecharts'>"
					+ "<div id=\"visualizechartId\" class='visionVisualizeHomePageCharts row'></div>" + "</div> "
					+ "</div>" + "</section>"
					+ "<form action='downloadChartImageAllPDF' id='pdfChartForm'  method='POST' target='_blank' >\n"
					+ "<c:if test=\"true\">\n"
					+ "<input type=\"hidden\" name=\"${_csrf.parameterName}\" value=\"${_csrf.token}\" /> \n"
					+ "</c:if> \n" + "<input type=\"hidden\" value=\"\" id=\"chartImageObj\" name=\"chartImageObj\"/>\n"
					+ "</form>"
					+ "<form action='downloadDashboardMailChartImageAllPDF' id='pdfDashBoardMailChartForm'  method='POST'>\n"
					+ "<c:if test=\"true\">\n"
					+ "<input type=\"hidden\" name=\"${_csrf.parameterName}\" value=\"${_csrf.token}\" /> \n"
					+ "</c:if> \n"
					+ "<input type=\"hidden\" value=\"\" id=\"chartDashBoardMailImageObj\" name=\"chartDashBoardMailImageObj\"/>\n"
					+ "</form>" + "<div id=\"dialog\"></div>" + "<div id=\"dialog1\"></div>"
					+ "<div id=\"gridDialog\"></div>" + "<div id=\"homepageChartDialog\"></div>"
					+ "<div id ='drillDownChartDataDialog'></div>" + "<div id ='exchangeTreeDialog'></div>"
					+ "<div id ='dxpCreatePopOver'></div>" + "<div id ='smartBiTreeDateCalendarPopup'>"

					+ "</div>";

			/*
			 * String divStr =
			 * "<div id=\"mainintelliSenseSelectBoxId\" class='intelliSenseSelectBoxClass'>"
			 * +
			 * "<div id='mainintelliSenseInnerSelectBoxId'class='intelliSenseSelectBoxClass'></div>"
			 * + "<div class=\"chartSelectionsDropDown\" style=\"display:none\">" +
			 * "<div id=\"OptionListData\" class='visionVisualizeHomePageDropdown'>" +
			 * "</div>" +
			 * "<div id=\"visionHomePageSlicer\" class=\"visionHomePageSlicerClass\"></div>"
			 * + "<div id=\"visionFilterData\" class=\"visionFilterData\"></div>" +
			 * "<div id=\"visionHomeKanbanView\" class=\"visionHomeKanbanViewClass\"></div>"
			 * +
			 * "<div id=\"visionChartColorPalleteId\" class=\"visionChartColorPalleteClass\"></div>"
			 * + "</div>" + "</div>" +
			 * "<section class=\"visualizationDashboardView\" style=\"display:none\">" +
			 * "<div class=\"container-fluid\">" + "<div class=\"row\">" +
			 * " <div class=\"col-12\">" +
			 * "<div id='visionDashBoardHomeFilterId' class='visionDashBoardHomeFilterClass row' style=\"display:none\"></div>"
			 * +
			 * "<div id='visionDashBoardHomeCompareFilterId' class='visionDashBoardHomeFilterClass row' style=\"display:none\"></div>"
			 * + "<div id=\"visionCardView\" class=\"visionCardViewClass\">" + "</div>" +
			 * "</div>" + "</div>" + "<div class=\"container-fluid\" id ='visualizecharts'>"
			 * +
			 * "<div id=\"visualizechartId\" class='visionVisualizeHomePageCharts row'></div>"
			 * + "</div> " + "</div>" + "</section>" +
			 * "<form action='downloadChartImageAllPDF' id='pdfChartForm'  method='POST' target='_blank' >\n"
			 * + "<c:if test=\"true\">\n" +
			 * "<input type=\"hidden\" name=\"${_csrf.parameterName}\" value=\"${_csrf.token}\" /> \n"
			 * + "</c:if> \n" +
			 * "<input type=\"hidden\" value=\"\" id=\"chartImageObj\" name=\"chartImageObj\"/>\n"
			 * + "</form>" + "<div id=\"dialog\"></div>" + "<div id=\"dialog1\"></div>" +
			 * "<div id=\"gridDialog\"></div>" + "<div id=\"homepageChartDialog\"></div>" +
			 * "<div id ='drillDownChartDataDialog'></div>" +
			 * "<div id ='exchangeTreeDialog'></div>" + "<div id ='dxpCreatePopOver'></div>"
			 * + "<div id ='smartBiTreeDateCalendarPopup'></div>";
			 */
			homePageDivObj.put("chartDiv", divStr);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return homePageDivObj;
	}

	public JSONObject showDrillDownChart(HttpServletRequest request) {
		return dashBoardsDAO.showDrillDownChart(request);
	}

	public JSONObject getcolorpalleteform(HttpServletRequest request) {
		JSONObject resultobj = new JSONObject();
		String result = "";
		JSONObject markerobj = new JSONObject();
		try {
			String data = request.getParameter("coloobjdata");
			String chartid = request.getParameter("chartid");
			String defaultColors = request.getParameter("defaultColors");
			JSONArray defaultValuesArr = new JSONArray();
			if (defaultColors != null && !"".equalsIgnoreCase(defaultColors)) {
				defaultValuesArr = (JSONArray) JSONValue.parse(defaultColors);
			}
			String defaultClrStr = "";
			if (defaultValuesArr != null && !defaultValuesArr.isEmpty()) {
				for (int c = 0; c < defaultValuesArr.size(); c++) {
					defaultClrStr += "<span class='themeBtns' data-color='" + defaultValuesArr.get(c)
							+ "' style='background-color: " + defaultValuesArr.get(c) + ";'></span>";
					if (c == 5) {
						break;
					}
				}
			}
			JSONArray dataarray = new JSONArray();
			if (data != null && !"".equalsIgnoreCase(data) && !"null".equalsIgnoreCase(data)) {
				dataarray = (JSONArray) JSONValue.parse(data);
				markerobj = (JSONObject) dataarray.get(0);
			}
			result += " <div class='colorPallatteMainDiv'>";
			if (chartid != null && !"".equalsIgnoreCase(chartid)) {
				result += "<div class='themeBtnsContainer'>" + " <div class='colorblockTitle'></div>"
						+ "<div class='colorPalletteSection'>"
						+ "<div class='themeBtnsGroup' onclick=saveGrpahColors(event,'" + chartid
						+ "') tittle=''Click to Save Color onmouseover=\"updatecolorOnGraph(event,'" + chartid + "')\">"
						+ defaultClrStr + "</div>" + "  </div>" + " </div>";
			}

			result += "<div class='themeBtnsContainer'>" // + " <div class='colorblockTitle'><h5>Palette</h5></div>"
					+ "<div class='colorPalletteSection'>"
					+ "<div class='themeBtnsGroup' onclick=saveGrpahColors(event,'" + chartid
					+ "') tittle=''Click to Save Color onmouseover=\"updatecolorOnGraph(event,'" + chartid + "')\">"
					+ "<div>"
					+ "<span class='themeBtns' data-color='#696969' style='background-color: #696969;'></span>"
					+ "  <span class='themeBtns' data-color='#888888' style='background-color: #888888;'></span>"
					+ " <span class='themeBtns' data-color='#A0A0A0' style='background-color: #A0A0A0;'></span>"
					+ "</div>" + "<div>"
					+ "  <span class='themeBtns' data-color='#A8A8A8' style='background-color: #A8A8A8;'></span>"
					+ "  <span class='themeBtns' data-color='#B8B8B8' style='background-color: #B8B8B8;'></span>"
					+ "  <span class='themeBtns' data-color='#C0C0C0' style='background-color: #C0C0C0;'></span>"
					+ "</div>" + "  </div>" + "<div class='themeBtnsGroup' onclick=saveGrpahColors(event,'" + chartid
					+ "') tittle=''Click to Save Color onmouseover=\"updatecolorOnGraph(event,'" + chartid + "')\">"
					+ "<div>"
					+ "<span class='themeBtns' data-color='#00acee' style='background-color: #00acee;'></span>"
					+ "  <span class='themeBtns' data-color='#00b9ff' style='background-color: #00b9ff;'></span>"
					+ " <span class='themeBtns' data-color='#2bc4ff' style='background-color: #2bc4ff;'></span>"
					+ "</div>" + "<div>"
					+ "  <span class='themeBtns' data-color='#00aaee' style='background-color: #00aaee;'></span>"
					+ "  <span class='themeBtns' data-color='#26a7de' style='background-color: #26a7de;'></span>"
					+ "  <span class='themeBtns' data-color='#45b1e8' style='background-color: #45b1e8;'></span>"
					+ "</div>" + "  </div>" + "<div class='themeBtnsGroup' onclick=saveGrpahColors(event,'" + chartid
					+ "') tittle=''Click to Save Color onmouseover=\"updatecolorOnGraph(event, '" + chartid + "')\">"
					+ "<div>"
					+ "<span class='themeBtns' data-color='#006400' style='background-color: #006400;'></span>"
					+ "  <span class='themeBtns' data-color='#008000' style='background-color: #008000;'></span>"
					+ " <span class='themeBtns' data-color='#228B22' style='background-color: #228B22;'></span>"
					+ "</div>" + "<div>"
					+ "  <span class='themeBtns' data-color='#347C2C' style='background-color: #347C2C;'></span>"
					+ "  <span class='themeBtns' data-color='#437C17' style='background-color: #437C17;'></span>"
					+ "  <span class='themeBtns' data-color='#4AA02C' style='background-color: #4AA02C;'></span>"
					+ "</div>" + "  </div>" + "<div class='themeBtnsGroup' onclick=saveGrpahColors(event,'" + chartid
					+ "') tittle=''Click to Save Color onmouseover=\"updatecolorOnGraph(event,'" + chartid + "')\">"
					+ "<div>"
					+ "<span class='themeBtns' data-color='#EAC117' style='background-color: #EAC117;'></span>"
					+ "  <span class='themeBtns' data-color='#806517' style='background-color: #806517;'></span>"
					+ " <span class='themeBtns' data-color='#5C3317' style='background-color: #5C3317;'></span>"
					+ "</div>" + "<div>"
					+ "  <span class='themeBtns' data-color='#347C2C' style='background-color: #347C2C;'></span>"
					+ "  <span class='themeBtns' data-color='#E66C2C' style='background-color: #E66C2C;'></span>"
					+ "  <span class='themeBtns' data-color='#C11B17' style='background-color: #C11B17;'></span>"
					+ "</div>" + "  </div>" + "<div class='themeBtnsGroup' onclick=saveGrpahColors(event,'" + chartid
					+ "') tittle=''Click to Save Color onmouseover=\"updatecolorOnGraph(event,'" + chartid + "')\">"
					+ "<div>"
					+ "<span class='themeBtns' data-color='#00008B' style='background-color: #00008B;'></span>"
					+ "  <span class='themeBtns' data-color='#191970' style='background-color: #191970;'></span>"
					+ " <span class='themeBtns' data-color='#000080' style='background-color:#000080;'></span>"
					+ "</div>" + "<div>"
					+ "  <span class='themeBtns' data-color='#0000A0' style='background-color: #0000A0;'></span>"
					+ "  <span class='themeBtns' data-color='#0020C2' style='background-color: #0020C2;'></span>"
					+ "  <span class='themeBtns' data-color='#0909FF' style='background-color: #0909FF;'></span>"
					+ "  </div>" + "</div>" + "<div class='themeBtnsGroup' onclick=saveGrpahColors(event,'" + chartid
					+ "') tittle=''Click to Save Color onmouseover=\"updatecolorOnGraph(event,'" + chartid + "')\">"
					+ "<div>"
					+ "<span class='themeBtns' data-color='#00acee' style='background-color: #00acee;'></span>"
					+ "  <span class='themeBtns' data-color='#5cb9f1' style='background-color: #5cb9f1;'></span>"
					+ " <span class='themeBtns' data-color='#86c7f4' style='background-color: #86c7f4;'></span>"
					+ "</div>" + "<div>"
					+ "  <span class='themeBtns' data-color='#a8d5f7' style='background-color: #a8d5f7;'></span>"
					+ "  <span class='themeBtns' data-color='#c6e3fa' style='background-color: #c6e3fa;'></span>"
					+ "  <span class='themeBtns' data-color='#e3f1fc' style='background-color: #e3f1fc;'></span>"
					+ "</div>" + "  </div>" + "<div class='themeBtnsGroup' onclick=saveGrpahColors(event,'" + chartid
					+ "') tittle=''Click to Save Color onmouseover=\"updatecolorOnGraph(event,'" + chartid + "')\">"
					+ "<div>"
					+ "<span class='themeBtns' data-color='#FF0000' style='background-color: #FF0000;'></span>"
					+ "  <span class='themeBtns' data-color='#FF7F00' style='background-color: #FF7F00;'></span>"
					+ " <span class='themeBtns' data-color='#FFFF00' style='background-color: #FFFF00;'></span>"
					+ "  <span class='themeBtns' data-color='#9400D3' style='background-color: #9400D3;'></span>"
					+ "</div>" + "<div>"
					+ " <span class='themeBtns' data-color='#00FF00' style='background-color: #00FF00;'></span>"
					+ "  <span class='themeBtns' data-color='#0000FF' style='background-color: #0000FF;'></span>"
					+ "  <span class='themeBtns' data-color='#4B0082' style='background-color: #4B0082;'></span>"

					// + " <span class='themeBtns' data-color='#2bc4ff' style='background-color:
					// #2bc4ff;'></span>"ḍ
					+ "</div>" + "  </div>" + "<div class='themeBtnsGroup' onclick=saveGrpahColors(event,'" + chartid
					+ "') tittle=''Click to Save Color onmouseover=\"updatecolorOnGraph(event,'" + chartid + "')\">"
					+ "<div>" + "<span class='themeBtns' data-color='#4b6043' style='background-color:#4b6043;'></span>"
					+ "<span class='themeBtns' data-color='#658354'  style='background-color:#658354;'></span>"
					+ "<span class='themeBtns' data-color='#75975e'  style='background-color:#75975e;'></span>"
					+ "<span class='themeBtns' data-color='#87ab69'  style='background-color:#87ab69;'></span>"

					+ "</div>" + "<div>"
					+ "<span class='themeBtns' data-color='#95bb72'  style='background-color:#95bb72;'></span>"
					+ "<span class='themeBtns' data-color='#a3c585'  style='background-color:#a3c585;'></span>"
					+ "<span class='themeBtns' data-color='#b3cf99'  style='background-color:#b3cf99;'></span>"
					+ "<span class='themeBtns' data-color='#c7ddb5'  style='background-color:#c7ddb5;'></span>"
					+ "<span class='themeBtns' data-color='#ddead1'  style='background-color:#ddeadl;'></span>"

					// + " <span class='themeBtns' data-color='#2bc4ff' style='background-color:
					// #2bc4ff;'></span>"ḍ
					+ "</div>" + "  </div>" + "<div class='themeBtnsGroup' onclick=saveGrpahColors(event,'" + chartid
					+ "') tittle=''Click to Save Color onmouseover=\"updatecolorOnGraph(event,'" + chartid + "')\">"
					+ "<div>" + "<span class='themeBtns' data-color='#19362d' style='background-color:#19362d;'></span>"
					+ "<span class='themeBtns' data-color='#255045'  style='background-color:#255045;'></span>"
					+ "<span class='themeBtns' data-color='#2b5f53'  style='background-color:#2b5f53;'></span>"
					+ "<span class='themeBtns' data-color='#306e61'  style='background-color:#306e61;'></span>"
					+ "<span class='themeBtns' data-color='#347a6c'  style='background-color:#347a6c;'></span>"

					+ "</div>" + "<div>"
					+ "<span class='themeBtns' data-color='#478c80'  style='background-color:#478c80;'></span>"
					+ "<span class='themeBtns' data-color='#649e94'  style='background-color:#649e94;'></span>"
					+ "<span class='themeBtns' data-color='#8db9b2'  style='background-color:#8db9b2;'></span>"
					+ "<span class='themeBtns' data-color='#b7d4d0'  style='background-color:#b7d4d0;'></span>"

					// + " <span class='themeBtns' data-color='#2bc4ff' style='background-color:
					// #2bc4ff;'></span>"ḍ
					+ "</div>" + "  </div>" + "<div class='themeBtnsGroup' onclick=saveGrpahColors(event,'" + chartid
					+ "') tittle=''Click to Save Color onmouseover=\"updatecolorOnGraph(event,'" + chartid
					+ "')\"><div>"
					+ "<span class='themeBtns' data-color='#c61a09' style='background-color:#c61a09;'></span>"
					+ "<span class='themeBtns' data-color='#df2c14'  style='background-color:#df2c14;'></span>"
					+ "<span class='themeBtns' data-color='#ed3419'  style='background-color:#ed3419;'></span>"
					+ "<span class='themeBtns' data-color='#fb3b1e'  style='background-color:#fb3b1e;'></span>"
					+ "<span class='themeBtns' data-color='#ff4122'  style='background-color:#ff4122;'></span>"
					+ "</div>" + "<div>"
					+ "<span class='themeBtns' data-color='#ff6242'  style='background-color:#ff6242;'></span>"
					+ "<span class='themeBtns' data-color='#ff8164'  style='background-color:#ff8164;'></span>"
					+ "<span class='themeBtns' data-color='#ffa590'  style='background-color:#ffa590;'></span>"
					+ "<span class='themeBtns' data-color='#ffc9bb'  style='background-color:#ffc9bb;'></span>"
					// + " <span class='themeBtns' data-color='#2bc4ff' style='background-color:
					// #2bc4ff;'></span>"ḍ
					+ "</div>" + "  </div>" + "<div class='themeBtnsGroup' onclick=saveGrpahColors(event,'" + chartid
					+ "') tittle=''Click to Save Color onmouseover=\"updatecolorOnGraph(event,'" + chartid
					+ "')\"><div>"
					+ "<span class='themeBtns' data-color='#235284' style='background-color:#235284;'></span>"
					+ "<span class='themeBtns' data-color='#3271a5'  style='background-color:#3271a5;'></span>"
					+ "<span class='themeBtns' data-color='#3982b8'  style='background-color:#3982b8;'></span>"
					+ "<span class='themeBtns' data-color='#4194cb'  style='background-color:#4194cb;'></span>"
					+ "<span class='themeBtns' data-color='#46a2da'  style='background-color:#46a2da;'></span>"
					+ "</div>" + "<div>"
					+ "<span class='themeBtns' data-color='#58afdd'  style='background-color:#58afdd;'></span>"
					+ "<span class='themeBtns' data-color='#6abce2'  style='background-color:#6abce2;'></span>"
					+ "<span class='themeBtns' data-color='#8dcfec'  style='background-color:#8dcfec;'></span>"
					+ "<span class='themeBtns' data-color='#b8e2f4'  style='background-color:#b8e2f4;'></span>"
					+ "</div>" + "  </div>" +

					"<div class='themeBtnsGroup' onclick=saveGrpahColors(event,'" + chartid
					+ "') tittle=''Click to Save Color onmouseover=\"updatecolorOnGraph(event,'" + chartid
					+ "')\"><div>"
					+ "<span class='themeBtns' data-color='#7f3667' style='background-color:#7f3667;'></span>"
					+ "<span class='themeBtns' data-color='#a53e76'  style='background-color:#a53e76;'></span>"
					+ "<span class='themeBtns' data-color='#bb437e'  style='background-color:#bb437e;'></span>"
					+ "<span class='themeBtns' data-color='#d24787'  style='background-color:#d24787;'></span>"
					+ "<span class='themeBtns' data-color='#e44b8d'  style='background-color:#e44b8d;'></span>"
					+ "</div>" + "<div>"
					+ "<span class='themeBtns' data-color='#e2619f'  style='background-color:#e2619f;'></span>"
					+ "<span class='themeBtns' data-color='#e27bb1'  style='background-color:#e27bb1;'></span>"
					+ "<span class='themeBtns' data-color='#e5a0c6'  style='background-color:#e5a0c6;'></span>"
					+ "<span class='themeBtns' data-color='#eec4dc'  style='background-color:#eec4dc;'></span>"
					+ "</div>" + "  </div>" + "<div class='themeBtnsGroup' onclick=saveGrpahColors(event,'" + chartid
					+ "') tittle=''Click to Save Color onmouseover=\"updatecolorOnGraph(event,'" + chartid
					+ "')\"><div>"
					+ "<span class='themeBtns' data-color='#2B9348' style='background-color:#2B9348;'></span>"
					+ "<span class='themeBtns' data-color='#55A630'  style='background-color:#55A630;'></span>"
					+ "<span class='themeBtns' data-color='#808918'  style='background-color:#808918;'></span>"
					+ "<span class='themeBtns' data-color='#AACC00'  style='background-color:#AACC00;'></span>"
					+ "<span class='themeBtns' data-color='#BFD200'  style='background-color:#BFD200;'></span>"
					+ "</div>" + "<div>"
					+ "<span class='themeBtns' data-color='#D4D700'  style='background-color:#D4D700;'></span>"
					+ "<span class='themeBtns' data-color='#DDDF00'  style='background-color:#DDDF00;'></span>"
					+ "<span class='themeBtns' data-color='#EEEF20'  style='background-color:#EEEF20;'></span>"
					+ "<span class='themeBtns' data-color='#FFFF3F'  style='background-color:#FFFF3F;'></span>"
					+ "</div>" + "  </div>" +

					"<div class='themeBtnsGroup' onclick=saveGrpahColors(event,'" + chartid
					+ "') tittle=''Click to Save Color onmouseover=\"updatecolorOnGraph(event,'" + chartid
					+ "')\"><div>"
					+ "<span class='themeBtns' data-color='#C71585' style='background-color:#C71585;'></span>"
					+ "<span class='themeBtns' data-color='#8A2BE2'  style='background-color:#8A2BE2;'></span>"
					+ "<span class='themeBtns' data-color='#0000FF'  style='background-color:#0000FF;'></span>"
					+ "<span class='themeBtns' data-color='#0D98BA'  style='background-color:#0D98BA;'></span>"
					+ "<span class='themeBtns' data-color='#008000'  style='background-color:#008000;'></span>"
					+ "<span class='themeBtns' data-color='#9ACD32'  style='background-color:#9ACD32;'></span>"
					+ "</div>" + "<div>"

					+ "<span class='themeBtns' data-color='#FFFF00'  style='background-color:#FFFF00;'></span>"
					+ "<span class='themeBtns' data-color='#FFAE42'  style='background-color:#FFAE42;'></span>"
					+ "<span class='themeBtns' data-color='#FFA500'  style='background-color:#FFA500;'></span>"
					+ "<span class='themeBtns' data-color='#FF4500'  style='background-color:#FF4500;'></span>"
					+ "<span class='themeBtns' data-color='#FF0000'  style='background-color:#FF0000;'></span>"
					+ "</div>" + "  </div>" +

					"<div class='themeBtnsGroup' onclick=saveGrpahColors(event,'" + chartid
					+ "') tittle=''Click to Save Color onmouseover=\"updatecolorOnGraph(event,'" + chartid
					+ "')\"><div>"
					+ "<span class='themeBtns' data-color='#4c00b0' style='background-color:#4c00b0;'></span>"
					+ "<span class='themeBtns' data-color='#7600bc'  style='background-color:#7600bc;'></span>"
					+ "<span class='themeBtns' data-color='#8a00c2'  style='background-color:#8a00c2;'></span>"
					+ "<span class='themeBtns' data-color='#a000c8'  style='background-color:#a000c8;'></span>"
					+ "<span class='themeBtns' data-color='#b100cd'  style='background-color:#b100cd;'></span>"
					+ "</div>" + "<div>"
					+ "<span class='themeBtns' data-color='#be2ed6'  style='background-color:#be2ed6;'></span>"
					+ "<span class='themeBtns' data-color='#ca5cdd'  style='background-color:#ca5cdd;'></span>"
					+ "<span class='themeBtns' data-color='#da8ee7'  style='background-color:#da8ee7;'></span>"
					+ "<span class='themeBtns' data-color='#e8bcf0'  style='background-color:#e8bcf0;'></span>"
					+ "</div>" + "  </div>" + "<div class='themeBtnsGroup' onclick=saveGrpahColors(event,'" + chartid
					+ "') tittle=''Click to Save Color onmouseover=\"updatecolorOnGraph(event,'" + chartid
					+ "')\"><div>"
					+ "<span class='themeBtns' data-color='#E65100' style='background-color:#E65100;'></span>"
					+ "<span class='themeBtns' data-color='#EF6C00'  style='background-color:#EF6C00;'></span>"
					+ "<span class='themeBtns' data-color='#F57C00'  style='background-color:#F57C00;'></span>"
					+ "<span class='themeBtns' data-color='#FB8C00'  style='background-color:#FB8C00;'></span>"
					+ "<span class='themeBtns' data-color='#FF9800'  style='background-color:#FF9800;'></span>"
					+ "</div>" + "<div>"
					+ "<span class='themeBtns' data-color='#FFA726'  style='background-color:#FFA726;'></span>"
					+ "<span class='themeBtns' data-color='#FFB74D'  style='background-color:#FFB74D;'></span>"
					+ "<span class='themeBtns' data-color='#FFCC80'  style='background-color:#FFCC80;'></span>"
					+ "<span class='themeBtns' data-color='#FFE0B2'  style='background-color:#FFE0B2;'></span>"
					+ "<span class='themeBtns' data-color='#FFF3E0'  style='background-color:#FFF3E0;'></span>"
					+ "</div>" + "  </div>" +

					" </div>" + " </div>" + "</div>";

			resultobj.put("colorpalateobj", result);
			resultobj.put("data", dataarray);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultobj;
	}

	public JSONObject getChartColumnsForm(HttpServletRequest request) {
		return dashBoardsDAO.getChartColumnsForm(request);
	}

	public JSONObject getchartconfigobjdata(HttpServletRequest request) {
		JSONObject dataobj = new JSONObject();
		try {
			dataobj = dashBoardsDAO.getchartPropertiesobj(request);
			if (dataobj != null && !dataobj.isEmpty()) {

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return dataobj;
	}

	public JSONObject updateGraphProperties(HttpServletRequest request) {
		JSONObject dataobj = new JSONObject();
		try {
			dataobj = dashBoardsDAO.updateGraphProperties(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return dataobj;
	}

	public JSONObject getTreeMapExchangeLevels(HttpServletRequest request) {
		JSONObject dataobj = new JSONObject();
		try {
			dataobj = dashBoardsDAO.getTreeMapExchangeLevels(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return dataobj;
	}

	public JSONObject getExchaneLevelsData(HttpServletRequest request) {
		return dashBoardsDAO.getExchaneLevelsData(request);
	}

	public JSONObject createTableasFile(HttpServletRequest request, HttpServletResponse response) {
		JSONObject resultObj = new JSONObject();
		String buttonDiv = "";
		String ListBoxId = "";
		JSONArray checkBoxDataArr = new JSONArray();
		try {
			resultObj = dashBoardsDAO.createTableasFile(request, response);
			String tableName = request.getParameter("tableName");
			String filePath = request.getParameter("filePath");
			if (tableName != null && !tableName.isEmpty()) {
				String ClolumnListStr = "";
				try {
					List ColumnList = dashBoardsDAO.tableColumnList(request, tableName);
//                ListBoxId = "<div id = 'tablecolumnId' class = 'tablecolumnId'>";
					if (ColumnList != null && !ColumnList.isEmpty()) {
						for (int i = 0; i < ColumnList.size(); i++) {
							String ColumnName = (String) ColumnList.get(i);
							checkBoxDataArr.add(ColumnName);
						}
					}

				} catch (Exception e) {
					// e.printStackTrace();
				}
			}
			JSONObject gridButtonsObj = dashBoardsDAO.getGridDataViewButtonsObject(request);
			if (resultObj != null) {
				if (gridButtonsObj != null && !gridButtonsObj.isEmpty()) {
					buttonDiv = "<span>";
					if (gridButtonsObj.get("isDeleteFlag") != null
							&& !"".equalsIgnoreCase(String.valueOf(gridButtonsObj.get("isDeleteFlag")))
							&& "Y".equalsIgnoreCase(String.valueOf(gridButtonsObj.get("isDeleteFlag")))) {
						buttonDiv += "<img src='images/Delete-Icon-03-01.png' title='Delete' onclick=deleteColumn("
								+ checkBoxDataArr + ",'" + tableName + "')>";
					}
					if (gridButtonsObj.get("isMergeFlag") != null
							&& !"".equalsIgnoreCase(String.valueOf(gridButtonsObj.get("isMergeFlag")))
							&& "Y".equalsIgnoreCase(String.valueOf(gridButtonsObj.get("isMergeFlag")))) {
						buttonDiv += "<img src='images/Data Merge-Icon-01.png' title='Merge Data' onclick= mergeColumntwthData('"
								+ tableName + "')>";
					}
					if (gridButtonsObj.get("isTransposeFlag") != null
							&& !"".equalsIgnoreCase(String.valueOf(gridButtonsObj.get("isTransposeFlag")))
							&& "Y".equalsIgnoreCase(String.valueOf(gridButtonsObj.get("isTransposeFlag")))) {
						buttonDiv += "<img src='images/Data Transpose-Icon-01-01.png' title='Transpose Data' onclick= composeData(event,'"
								+ tableName + "')>";
					}
					if (gridButtonsObj.get("isDimensionTransposeFlag") != null
							&& !"".equalsIgnoreCase(String.valueOf(gridButtonsObj.get("isDimensionTransposeFlag")))
							&& "Y".equalsIgnoreCase(String.valueOf(gridButtonsObj.get("isDimensionTransposeFlag")))) {
						buttonDiv += "<img src='images/Dimention-Transpsose-Icon-02-01.png' title='Dimensional Transpose' onclick= DimensionTranspose(event,'"
								+ tableName + "')>";
					}
					if (gridButtonsObj.get("isTableEditFlag") != null
							&& !"".equalsIgnoreCase(String.valueOf(gridButtonsObj.get("isTableEditFlag")))
							&& "Y".equalsIgnoreCase(String.valueOf(gridButtonsObj.get("isTableEditFlag")))) {
						buttonDiv += "<img src='images/Change Datatype-Icon-03-01.png' title='Table Edit' onclick= ChooseOptions(event,'"
								+ tableName + "')>";
					}
					if (gridButtonsObj.get("isExportFlag") != null
							&& !"".equalsIgnoreCase(String.valueOf(gridButtonsObj.get("isExportFlag")))
							&& "Y".equalsIgnoreCase(String.valueOf(gridButtonsObj.get("isExportFlag")))) {
						// buttonDiv += "<img src='images/Export-Icon-03-01.png' title='Export'id
						// ='ExportgridId' onclick= generateexcel>";
					}
					if (gridButtonsObj.get("isAIChartSUggestionsFlag") != null
							&& !"".equalsIgnoreCase(String.valueOf(gridButtonsObj.get("isAIChartSUggestionsFlag")))
							&& "Y".equalsIgnoreCase(String.valueOf(gridButtonsObj.get("isAIChartSUggestionsFlag")))) {
						buttonDiv += "<img src='images/Chart Auto-Suggetion-Icon-03-01.png' title='AI Chart Suggestions' onclick= getModalFileColumns(event,'"
								+ tableName + "')>";
					}
					if (gridButtonsObj.get("isPivotTableFlag") != null
							&& !"".equalsIgnoreCase(String.valueOf(gridButtonsObj.get("isPivotTableFlag")))
							&& "Y".equalsIgnoreCase(String.valueOf(gridButtonsObj.get("isPivotTableFlag")))) {
						buttonDiv += "<img src='images/Pivot Descriptor-Icon-03-01.png' title='Pivot Table' onclick= getCrossTabData('"
								+ tableName + "')>";
					}
					if (gridButtonsObj.get("isInsightsViewFlag") != null
							&& !"".equalsIgnoreCase(String.valueOf(gridButtonsObj.get("isInsightsViewFlag")))
							&& "Y".equalsIgnoreCase(String.valueOf(gridButtonsObj.get("isInsightsViewFlag")))) {
						buttonDiv += "<img src=\"images/Insights_View.svg\" title=\"Insights\" onclick=\"getInsightsDataView('"
								+ tableName + "')\">";
					}
					if (gridButtonsObj.get("isDataLineageFlag") != null
							&& !"".equalsIgnoreCase(String.valueOf(gridButtonsObj.get("isDataLineageFlag")))
							&& "Y".equalsIgnoreCase(String.valueOf(gridButtonsObj.get("isDataLineageFlag")))) {
						buttonDiv += "<img src='images/Data_Lineage_icon.svg' title=\"Data Lineage\" onclick=\"getInsightsDataView('"
								+ tableName + "')\">";
					}
					if (gridButtonsObj.get("isDataProfilingFlag") != null
							&& !"".equalsIgnoreCase(String.valueOf(gridButtonsObj.get("isDataProfilingFlag")))
							&& "Y".equalsIgnoreCase(String.valueOf(gridButtonsObj.get("isDataProfilingFlag")))) {
						buttonDiv += "<img src='images/Data_Profiling_icon.svg' title='Data Profiling' onclick= getDataProfiling('"
								+ tableName + "') >";
					}
					if (gridButtonsObj.get("isPivotGridFlag") != null
							&& !"".equalsIgnoreCase(String.valueOf(gridButtonsObj.get("isPivotGridFlag")))
							&& "Y".equalsIgnoreCase(String.valueOf(gridButtonsObj.get("isPivotGridFlag")))) {

						buttonDiv += "<img src='images/Pivot table-Icon-03-01.png' title='Pivot Grid' onclick= getPivotGridData('"
								+ tableName + "')>";
					}
					if (gridButtonsObj.get("isRemoveDuplicatesFlag") != null
							&& !"".equalsIgnoreCase(String.valueOf(gridButtonsObj.get("isRemoveDuplicatesFlag")))
							&& "Y".equalsIgnoreCase(String.valueOf(gridButtonsObj.get("isRemoveDuplicatesFlag")))) {
						buttonDiv += "<img src='images/Remove duplicates-Icon.png' title='Remove Duplicates' onclick=removeDuplicates("
								+ checkBoxDataArr + ",'" + tableName + "')>";
					}
					/*
					 * buttonDiv +=
					 * "<img src='images/Pivot table-Icon-03-01.png' title='Data Correlation' onclick= getDataCorrelation('"
					 * + filePath + "')>";
					 */
					buttonDiv += "</span>";

				}
			}
//            buttonDiv +="<div id = 'tablecolumnId' class = 'tablecolumnId'>";

			resultObj.put("buttons", buttonDiv);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject deleteTableColumn(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.deleteTableColumn(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject mergeformdata(HttpServletRequest request) {
		JSONObject resultobj = new JSONObject();
		try {
			resultobj = dashBoardsDAO.mergeformdata(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultobj;
	}

	public String transformdata(HttpServletRequest request) {
		String result = "";
		try {
			result = dashBoardsDAO.transformdata(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public String gettransposedata(HttpServletRequest request) {
		String result = "";
		try {
			result = dashBoardsDAO.gettransposedata(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public String DimensionTransposeColumn(HttpServletRequest request) {
		JSONObject resultobj = new JSONObject();
		String Result = "";
		try {
			Result = dashBoardsDAO.DimensionTransposeColumn(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return Result;
	}

	public boolean setImportData(HttpServletRequest request) {
		boolean result = false;
		try {
			result = dashBoardsDAO.setImportData(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public JSONObject showtableData(HttpServletRequest request) {
		JSONObject resultobj = new JSONObject();
		try {
			resultobj = dashBoardsDAO.showtableData(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultobj;
	}

	public JSONObject gettableObjectData(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.showtableData(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public String gettableattribute(HttpServletRequest request) {
		JSONObject resultobj = new JSONObject();
		String Result = "";
		try {
			Result = dashBoardsDAO.gettableattribute(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return Result;
	}

	public String caseSensitive(HttpServletRequest request) {
		String result = "";
		try {
			result = dashBoardsDAO.caseSensitive(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public JSONObject DimensionTransposedata(HttpServletRequest request) {
		JSONObject resultobj = new JSONObject();
		try {
			resultobj = dashBoardsDAO.DimensionTransposedata(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultobj;
	}

	public JSONObject generateQueryStr(HttpServletRequest request) {
		JSONObject resultobj = new JSONObject();
		String tableStr = "";
		String ColumnStr = "";
		try {
			List tablelist = dashBoardsDAO.gettableList(request);
			List columnlist = dashBoardsDAO.gettablecolumn(request);
//            String result = "<div id ='graphQueryGeneratorid' class = 'graphQueryGeneratorClass'>";
			String result = "<span id='inputstrId' class='inputstrClass'>";
			result += "<input type=\"text\"id=\"inpittext\" data-opt-name=\"hole\"/>";
			result += "</span>";
			if (tablelist != null && !tablelist.isEmpty()) {
				tableStr = "<span id='tableId' class='tableClass'>";
				tableStr += "File Name:<select id ='DxpdashbordoptionListId' class='DxpdashbordoptionListClass' onChange=\"gettable(event,id)\">";
				for (int i = 0; i < tablelist.size(); i++) {
					String selected = "";
					tableStr += "<option value= '" + tablelist.get(i) + "' " + selected + ">" + tablelist.get(i)
							+ "</option>";
				}
				tableStr += "</select>";
				tableStr += "</span>";
			}
			if (columnlist != null && !columnlist.isEmpty()) {
				ColumnStr = "<select id ='columnlistoptionListId' class='columnlistoptionListClass' onChange=\"getDashBoardCharts(event,id)\">";
				for (int i = 0; i < columnlist.size(); i++) {
					String selected = "";
					ColumnStr += "<option value= '" + columnlist.get(i) + "' " + selected + ">" + columnlist.get(i)
							+ "</option>";
				}
				ColumnStr += "</select>";
			}
//            result += "</div>"; 

			resultobj.put("result", result);
			resultobj.put("tableStr", tableStr);
			resultobj.put("ColumnStr", ColumnStr);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultobj;
	}

	public JSONObject getModalFileColumns(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.getModalFileColumns(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject fetchModalChartData(HttpServletRequest request) {
		JSONObject chartObj = new JSONObject();
		try {
			String chartType = request.getParameter("chartType");
			String chartId = request.getParameter("chartId");
			if ("BarAndLine".equalsIgnoreCase(chartType)) {
				chartObj = dashBoardsDAO.fetchBarwithLineEChartData(request);
				chartObj.put("flag", "Y");
			} else if ("treemap".equalsIgnoreCase(chartType) || "sunburst".equalsIgnoreCase(chartType)) {
				chartObj = dashBoardsDAO.fetchTreeMapEChartData(request);
				chartObj.put("flag", "Y");
			} else if ("heatmap".equalsIgnoreCase(chartType)) {
				chartObj = dashBoardsDAO.fetchHeatMapEChartData(request);
				chartObj.put("flag", "Y");
			} else if (chartType != null && !"".equalsIgnoreCase(chartType) && "sankey".equalsIgnoreCase(chartType)) {
				chartObj = dashBoardsDAO.fetchSankeyChartData(request);
				chartObj.put("flag", "Y");
			} else {
				chartObj = dashBoardsDAO.fetchModalChartData(request);
			}

			chartObj.put("chartId", chartId);

		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return chartObj;
	}

	public String renameSQLColumn(HttpServletRequest request) {
		String reuslt = "";
		try {
			reuslt = dashBoardsDAO.renameSQLColumn(request);

		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return reuslt;
	}

	public String getColumnformStr(HttpServletRequest request) {
		String result = "";
		try {
			result = dashBoardsDAO.getColumnDataType(request);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return result;
	}

	public String getAggregateResult(HttpServletRequest request) {
		String result = "";
		try {
			result = dashBoardsDAO.getAggregateResult(request);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return result;
	}

	public String createSuffixAndPriffix(HttpServletRequest request) {
		String result = "";
		try {
			result = dashBoardsDAO.createSuffixAndPriffix(request);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return result;
	}

	public String updatePalatteColor(HttpServletRequest request) {
		String result = "";
		try {
			result = dashBoardsDAO.updatePalatteColor(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public JSONObject getDataCorrelation(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		JSONArray dataTableArray = new JSONArray();
		try {
			JSONObject responeObject = dashBoardsDAO.getDataCorrelation(request);
			if (responeObject != null && !responeObject.isEmpty()) {
				String correlationDataStr = String.valueOf(responeObject.get("dataCorrelation"));
				List<String> headersArray = (List<String>) responeObject.get("headers");
				JSONObject correlationDataObj = (JSONObject) JSONValue.parse(correlationDataStr);
				List<JSONObject> correlatedDataList = (List<JSONObject>) correlationDataObj.keySet().stream()
						.map(e -> correlationDataObj.get(e)).collect(Collectors.toList());
				JSONArray dataFieldsArray = getDataFieldsArray(headersArray);
				JSONArray columnsArray = getColumnsArray(headersArray);
				resultObj.put("dataObject", correlatedDataList);
				resultObj.put("dataFields", dataFieldsArray);
				resultObj.put("columns", columnsArray);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONArray getDataFieldsArray(List<String> tableHeaders) {
		JSONArray dataFieldsArray = new JSONArray();
		try {
			for (String field : tableHeaders) {
				JSONObject dataFieldsObj = new JSONObject();
				dataFieldsObj.put("name", field);
				dataFieldsObj.put("type", "float");
				dataFieldsArray.add(dataFieldsObj);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return dataFieldsArray;
	}

	public JSONArray getColumnsArray(List<String> tableHeaders) {
		JSONArray columnsArray = new JSONArray();
		try {
			for (String field : tableHeaders) {
				JSONObject columnsObj = new JSONObject();
				String replacedField = field.replace("_", " ");
				String titleCaseField = dashBoardUtills.convertTextToTitleCase(replacedField);
				columnsObj.put("type", titleCaseField);
				columnsObj.put("dataField", field);
				columnsObj.put("width", 300);
				columnsArray.add(columnsObj);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return columnsArray;
	}

	public JSONObject getAutoSuggestedChartTypes(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.getAutoSuggestedChartTypes(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getDateColumns(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.getDateColumns(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getQueryGridData(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.getQueryGridData(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getChartObjectData(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.getQueryGridData(request);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject viewAnalyticsTableDataGrid(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.viewAnalyticsTableDataGrid(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject executeSQLQuery(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.executeSQLQuery(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getSuggestedChartTypesBasedonColumns(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.getSuggestedChartTypesBasedonColumns(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getAILensSuggestedChartTypesBasedonColumns(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.getAILensSuggestedChartTypesBasedonColumns(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public String getCurrencyAndCodesData(HttpServletRequest request) {
		String currencyAndCodeHtmlData = "";
		String currencyTableHeadTrTag = "<table><tr><td>From Currency</td><td></td><td>To Currency</td></tr>";
		String fromCurrencytrTableBodyTag = "<tr><td><select id=\"fromCurrencyDropDown\">";
		String toCurrencytrTableBodyTag = "<td><select id=\"toCurrencyDropDown\">";
		StringBuilder currencyCodeList = new StringBuilder(currencyTableHeadTrTag);
		StringBuilder optionTagsForCurrencyList = new StringBuilder();
		currencyCodeList.append(fromCurrencytrTableBodyTag);
		try {
			List<Object[]> codeCurrencyList = dashBoardsDAO.getCodeAndCurrencyList(request);
			if (codeCurrencyList != null && !codeCurrencyList.isEmpty()) {
				for (Object[] object : codeCurrencyList) {
					String code = String.valueOf(object[0]);
					String currency = String.valueOf(object[1]);
					String symbol = String.valueOf(object[2]);
					String optionTag = "<option value=" + code + " data-currencySymbol=" + symbol + ">" + code
							+ "&nbsp&nbsp" + currency + "</option>";
					optionTagsForCurrencyList.append(optionTag);
				}
				currencyCodeList.append(optionTagsForCurrencyList);
				currencyCodeList.append("</td>");
				currencyCodeList.append("<td><img src='images/currency-conversion.png' width='18px'/></td>");
				currencyCodeList.append(toCurrencytrTableBodyTag);
				currencyCodeList.append(optionTagsForCurrencyList);
				currencyCodeList.append("</td></tr>");
				currencyCodeList.append("</table>");
				currencyAndCodeHtmlData = currencyCodeList.toString();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return currencyAndCodeHtmlData;
	}

	public JSONObject getAutoSuggestedFilterTables(HttpServletRequest request) {
		return dashBoardsDAO.getAutoSuggestedFilterTables(request);
	}

	public JSONObject getArtificialIntellisenseApiDetails(HttpServletRequest request) {
		return dashBoardsDAO.getArtificialIntellisenseApiDetails(request);
	}

	public JSONObject alterBiTable(HttpServletRequest request) {
		return dashBoardsDAO.alterBiTable(request);
	}

	public String gettableformStr(HttpServletRequest request) {
		return dashBoardsDAO.gettableformStr(request);
	}

	public String getSelectType(HttpServletRequest request) {
		return dashBoardsDAO.getSelectType(request);
	}

	public String getSuffixValue(HttpServletRequest request) {
		return dashBoardsDAO.getSuffixValue(request);
	}

	public String getPrefixValue(HttpServletRequest request) {
		return dashBoardsDAO.getPrefixValue(request);
	}

	public String getCreateFind(HttpServletRequest request) {
		return dashBoardsDAO.getCreateFind(request);
	}

	public String getRenameValue(HttpServletRequest request) {
		return dashBoardsDAO.getRenameValue(request);
	}

	public JSONObject executeAlterTable(HttpServletRequest request) {

		return dashBoardsDAO.executeAlterTable(request);
	}

	public String createPrefixValue(HttpServletRequest request) {
		return dashBoardsDAO.createPrefixValue(request);
	}

	public String deleterowdata(HttpServletRequest request) {
		return dashBoardsDAO.deleterowdata(request);
	}

	public JSONObject removeDuplicateValue(HttpServletRequest request) {
		return dashBoardsDAO.removeDuplicateValue(request);
	}

	public JSONObject removeDuplicateEachColumn(HttpServletRequest request) {
		return dashBoardsDAO.removeDuplicateEachColumn(request);

	}

	public JSONObject deleteDuplicateValues(HttpServletRequest request) {
		return dashBoardsDAO.deleteDuplicateValues(request);
	}

	public JSONObject executePythonQuery(HttpServletRequest request) {
		return dashBoardsDAO.executePythonQuery(request);
	}

	public JSONObject getPythonChartObjectData(HttpServletRequest request) {
		return dashBoardsDAO.getPythonChartObjectData(request);
	}

	public JSONObject getCardDateValues(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.getCardDateValues(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject saveImageOnServer(HttpServletRequest request, MultipartFile multipartFileData) {
		JSONObject resultObject = new JSONObject();
		try {
			boolean isImageUploaded = false;
			String imageUploadResponse = "";
			String imageEncodedString = "";
			String userName = (String) request.getSession(false).getAttribute("ssUsername");
			String fileName = StringUtils.cleanPath(multipartFileData.getOriginalFilename());
			String fileExtension = FilenameUtils.getExtension(fileName);
			String updatedImageName = "CardUploadedImage" + System.currentTimeMillis() + "." + fileExtension;
			String fileDirectoryOnserver = fileStoreHomedirectory + "images/" + userName;
			if (fileExtension != null && !"".equalsIgnoreCase(fileExtension) && !"null".equalsIgnoreCase(fileExtension)
					&& "JPEG".equalsIgnoreCase(fileExtension) || "PNG".equalsIgnoreCase(fileExtension)
					|| "SVG".equalsIgnoreCase(fileExtension) || "JPG".equalsIgnoreCase(fileExtension)) {
				isImageUploaded = dashBoardUtills.saveFileOnServer(fileDirectoryOnserver, updatedImageName,
						multipartFileData);
				if (isImageUploaded) {
//					imageUploadResponse = "Image uploaded successfully.";
					String fileContentType = multipartFileData.getContentType();
					imageEncodedString = dashBoardUtills.getImageBase64EncodedString(fileDirectoryOnserver,
							updatedImageName);
					String imageHeader = "data:" + fileContentType + ";base64,";
					resultObject.put("imageEncodedString", imageHeader + imageEncodedString);
					String homepageCardImgChngEvt = request.getParameter("homepageCardImgChngEvt");
					if (!dashBoardUtills.isNullOrEmpty(homepageCardImgChngEvt)) {
						JSONObject homepageCardImgChngEvtObj = (JSONObject) JSONValue.parse(homepageCardImgChngEvt);
						String isCardImgChngEvt = String.valueOf(homepageCardImgChngEvtObj.get("isCardImgChngEvt"));
						if (!dashBoardUtills.isNullOrEmpty(homepageCardImgChngEvt)
								&& "true".equalsIgnoreCase(isCardImgChngEvt)) {
							homepageCardImgChngEvtObj.put("encodedCardImg", imageHeader + imageEncodedString);
							int updatedCount = dashBoardsDAO.updateHomepageCardImg(request, homepageCardImgChngEvtObj);
							if (updatedCount <= 0) {
								imageUploadResponse = "Failed to upload the image.";
								resultObject.put("isImageUploaded", false);
								return resultObject;
							}
						}
					}
				} else {
					imageUploadResponse = "Failed to upload the image.";
				}
//				resultObject.put("imageUploadResponse", imageUploadResponse);
				resultObject.put("isImageUploaded", isImageUploaded);
				resultObject.put("imageName", updatedImageName);

			} else {
				imageUploadResponse = "Upload Failed, Please upload only Images.";
				resultObject.put("imageUploadResponse", imageUploadResponse);
				resultObject.put("isImageUploaded", isImageUploaded);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObject;
	}

	public JSONObject getCardImageData(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.getCardImageData(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getCardImgData(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.getCardImageData(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject chartDxpJoinTables(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		JSONArray tablesObj = new JSONArray();
		// JSONObject labelObj = new VisionUtills().getMultilingualObject(request);
		String fromTable = request.getParameter("tablesObj");
		if (fromTable != null && !"".equalsIgnoreCase(fromTable) && !"null".equalsIgnoreCase(fromTable)) {
			tablesObj = (JSONArray) JSONValue.parse(fromTable);
		}
		try {
			String tabsString = "<div id='dataMigrationTabs' class='dataMigrationTabs'>"
					// + "<ul class='dataMigrationTabsHeader'>"
					// + "<li class='dataMigrationTabsli'><a href='#tabs-1'>" + new
					// VisionUtills().convertIntoMultilingualValue(labelObj, "Join Clauses") +
					// "</a></li>"
					// + "</ul>"
					+ "<div id='tabs-1' class='dataMigrationsTabsInner'>" + " </div>" + " </div>" + "";
			resultObj.put("tabsString", tabsString);
			resultObj.put("selectedJoinTables", joinDxpTransformationRules(request, tablesObj));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject fetchChartJoinDxpTablesData(HttpServletRequest request) {
		String joinsDataStr = "";
		JSONObject resultObj = new JSONObject();
		Connection connection = null;
		try {
			// ravi start
			String joinType = "";
			// ravi end

			JSONArray masterTablesArray = new JSONArray();
			String dbObjStr = request.getParameter("dbObj");
			String childTableName = request.getParameter("tableName");
			if (childTableName != null && !"".equalsIgnoreCase(childTableName)
					&& !"null".equalsIgnoreCase(childTableName) && childTableName.contains(".")) {
				childTableName = childTableName.substring(childTableName.lastIndexOf(".") + 1);
			}
			String masterTables = request.getParameter("sourceTables");
			if (masterTables != null && !"".equalsIgnoreCase(masterTables) && !"".equalsIgnoreCase(masterTables)) {
				masterTablesArray = (JSONArray) JSONValue.parse(masterTables);
			}
			String joinColumnMapping = request.getParameter("joinColumnMapping");
			JSONObject joinColumnMappingObj = new JSONObject();
			if (joinColumnMapping != null && !"".equalsIgnoreCase(joinColumnMapping)
					&& !"null".equalsIgnoreCase(joinColumnMapping)) {
				joinColumnMappingObj = (JSONObject) JSONValue.parse(joinColumnMapping);
			}
			// String trString = "<tr>";
			List<Object[]> childTableColumnList = new ArrayList<>();
			childTableColumnList = dashBoardsDAO.getTreeOracleTableColumns(request, childTableName);

			JSONArray childTableColsTreeArray = new JSONArray();
			if (childTableColumnList != null && !childTableColumnList.isEmpty()) {
				JSONObject tableObj = new JSONObject();
				tableObj.put("id", childTableName);// CONNECTION_NAME
				tableObj.put("text", childTableName);
				tableObj.put("value", childTableName);
				tableObj.put("icon", "images/GridDB.png");
				childTableColsTreeArray.add(tableObj);
				for (int i = 0; i < childTableColumnList.size(); i++) {
					Object[] childColsArray = childTableColumnList.get(i);
					if (childColsArray != null && childColsArray.length != 0) {
						JSONObject columnObj = new JSONObject();
						columnObj.put("id", childColsArray[0] + ":" + childColsArray[1]);
						columnObj.put("text", childColsArray[1]);
						columnObj.put("value", childColsArray[0] + ":" + childColsArray[1]);
						columnObj.put("parentid", childColsArray[0]);
						childTableColsTreeArray.add(columnObj);
					}

				}
			}
			resultObj.put("childTableColsArray", childTableColsTreeArray);

			// ravi start
			String trString = "<tr>";
			String singleTrString = "<tr>";
			singleTrString += "<td width='5%'><img src=\"images/Detele Red Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionTdETLIcons\""
					+ " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>" + "</td>";
			singleTrString += "<td width='35%' class=\"sourceJoinColsTd\"><input class='visionColJoinMappingInput' type='text' value='' readonly='true'/>"
					+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
					+ " onclick=\"selectDxpColumn(this,'childColumn')\" style=\"\"></td>";

			singleTrString += "<td width='10%' class=\"sourceJoinColsTd\">"
					+ "<select id=\"OPERATOR_TYPE\"  class=\"sourceColsJoinSelectBox\">"
					+ "<option  value='=' selected>=</option>" + "<option  value='!='>!=</option>" + "</select>"
					+ "</td>";

			singleTrString += "<td width='35%' class=\"sourceJoinColsTd\"><input class='visionColJoinMappingInput' type='text' value='' readonly='true'/>"
					+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
					+ " onclick=\"selectDxpColumn(this,'masterColumn')\" style=\"\"></td>";
			singleTrString += "<td width='10%'><input type=\"text\" class=\"defaultValues\" id=\"static_value_0\"></td>"
					+ "<td width='5%'>" + "<select id='andOrOpt'>" + "<option value='AND'>AND</option>"
					+ "<option value='OR'>OR</option>" + "</select>" + "</td>";
			singleTrString += "</tr>";

			// ravi end
			// ravi start
			JSONArray masterTableColsTreeArray = new JSONArray();
			for (int i = 0; i < masterTablesArray.size(); i++) {
				String masterTableName = (String) masterTablesArray.get(i);
//                if (request.getParameter("tableName") != null && !"".equalsIgnoreCase(request.getParameter("tableName"))
//                        && !childTableName.equalsIgnoreCase(request.getParameter("tableName"))) {
				if (masterTableName != null && !"".equalsIgnoreCase(masterTableName)
						&& !"null".equalsIgnoreCase(masterTableName) && masterTableName.contains(".")) {
					masterTableName = masterTableName.substring(masterTableName.lastIndexOf(".") + 1);
				}
				List<Object[]> columnList = new ArrayList<>();
				columnList = dashBoardsDAO.getTreeOracleTableColumns(request, masterTableName);

				if (columnList != null && !columnList.isEmpty()) {
					JSONObject tableObj = new JSONObject();
					tableObj.put("id", masterTableName);
					tableObj.put("text", masterTableName);
					tableObj.put("value", masterTableName);
					tableObj.put("icon", "images/GridDB.png");
					masterTableColsTreeArray.add(tableObj);
					for (int j = 0; j < columnList.size(); j++) {
						Object[] masterColsArray = columnList.get(j);
						if (masterColsArray != null && masterColsArray.length != 0) {
							JSONObject columnObj = new JSONObject();
							columnObj.put("id", masterColsArray[0] + ":" + masterColsArray[1]);
							columnObj.put("text", masterColsArray[1]);
							columnObj.put("value", masterColsArray[0] + ":" + masterColsArray[1]);
							columnObj.put("parentid", masterColsArray[0]);
							masterTableColsTreeArray.add(columnObj);
						}

					}
				}
//                }
			}
			resultObj.put("masterTableColsArray", masterTableColsTreeArray);
			trString = singleTrString;

// ravi end 
			// String joinType = "";
			String mappedColTrString = "";
			if (joinColumnMappingObj != null && !joinColumnMappingObj.isEmpty()) {
				Set keySet = joinColumnMappingObj.keySet();
				List keysList = new ArrayList();
				keysList.addAll(keySet);
				Collections.sort(keysList);

				for (int i = 0; i < keysList.size(); i++) {
					Object keyName = keysList.get(i);
					JSONObject joinColMapObj = (JSONObject) joinColumnMappingObj.get(keysList.get(i));
					if (joinColMapObj != null && !joinColMapObj.isEmpty()) {
						joinType = (String) joinColMapObj.get("joinType");
						mappedColTrString += "<td width='5%' ><img src=\"images/Detele Red Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionTdETLIcons\""
								+ " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>" + "</td>";
						mappedColTrString += "<td width='35%' class=\"sourceJoinColsTd\">"
								+ "<input class='visionColJoinMappingInput' type='text' value='"
								+ (String) joinColMapObj.get("childTableColumn") + "' readonly='true'/>"
								+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
								+ " onclick=\"selectDxpColumn(this,'childColumn')\" style=\"\"></td>";

						String operator = (String) joinColMapObj.get("operator");

						mappedColTrString += "<td width='10%' class=\"sourceJoinColsTd\">"
								+ "<select id=\"OPERATOR_TYPE\"  class=\"sourceColsJoinSelectBox\">";
						mappedColTrString += "<option  value='=' " + ("=".equalsIgnoreCase(operator) ? "selected" : "")
								+ ">=</option>";
						mappedColTrString += "<option  value='!=' "
								+ ("!=".equalsIgnoreCase(operator) ? "selected" : "") + ">!=</option>";
						mappedColTrString += "</select>" + "</td>";
						mappedColTrString += "<td width='35%' class=\"sourceJoinColsTd\">"
								+ "<input class='visionColJoinMappingInput' type='text' value='"
								+ (String) joinColMapObj.get("masterTableColumn") + "' readonly='true'/>"
								+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
								+ " onclick=\"selectDxpColumn(this,'masterColumn')\" style=\"\"></td>";
						mappedColTrString += "" + "<td width='10%'><input type=\"text\" " + "value='"
								+ ((joinColMapObj.get("staticValue") != null
										&& !"".equalsIgnoreCase(String.valueOf(joinColMapObj.get("staticValue")))
										&& !"null".equalsIgnoreCase(String.valueOf(joinColMapObj.get("staticValue"))))
												? String.valueOf(joinColMapObj.get("staticValue"))
												: "")
								+ "' " + " class=\"defaultValues\" id=\"static_value_" + i + "\"></td>"
								+ "<td width='5%'>" + "<select id='andOrOpt'>";
						String andOrOperator = (String) joinColMapObj.get("andOrOperator");
						mappedColTrString += "<option value='AND' "
								+ ("AND".equalsIgnoreCase(andOrOperator) ? "selected" : "") + ">AND</option>";
						mappedColTrString += "<option value='OR' "
								+ ("OR".equalsIgnoreCase(andOrOperator) ? "selected" : "") + ">OR</option>";
						mappedColTrString += "</select>" + "</td>";
						mappedColTrString += "</tr>";
					}

				}
			}
			if (!(mappedColTrString != null && !"".equalsIgnoreCase(mappedColTrString)
					&& !"null".equalsIgnoreCase(mappedColTrString))) {
				mappedColTrString = trString;
			}
			joinsDataStr += "<div class=\"visionEtlJoinClauseMain visionAnalyticsJoinClauseMain\">"
					+ "<div class=\"visionEtlAddIconDiv\">"
					+ "<img data-trstring='' src=\"images/Add icon.svg\" id=\"visionDxpAddRowIcon\" "
					+ "class=\"visionDxpAddRowIcon\" title=\"Add column for mapping\""
					+ " onclick=addNewDxpJoinsRow(event,'" + dbObjStr + "',id) "
					+ "style=\"width:15px;height: 15px;cursor:pointer; float: left;\"/>"
					+ "<img data-trstring='' src=\"images/Save Icon.svg\" id=\"visionEtlSaveIcon\" "
					+ "class=\"visionDxpAddRowIcon\" title=\"Save Mapping\"" + " onclick=saveDxpJoinMapping(event,id) "
					+ "style=\"width:15px;height: 15px;cursor:pointer; float: left;\"/>"
					+ "<span class='visionDxpColumnJoinType'>Join Type : </span>"
					+ "<select class='visionDxpColumnJoinType' id='joinType'>" + "<option value='INNER JOIN' "
					+ ("INNER JOIN".equalsIgnoreCase(joinType) ? "selected" : "") + " >Inner Join</option>"
					+ "<option value='JOIN' " + ("JOIN".equalsIgnoreCase(joinType) ? "selected" : "") + ">Join</option>"
					+ "<option value='LEFT OUTER JOIN' "
					+ ("LEFT OUTER JOIN".equalsIgnoreCase(joinType) ? "selected" : "") + ">Left Outer Join</option>"
					+ "<option value='RIGHT OUTER JOIN' "
					+ ("RIGHT OUTER JOIN".equalsIgnoreCase(joinType) ? "selected" : "") + ">Right Outer Join</option>"
					+ "<option value='FULL OUTER JOIN' "
					+ ("FULL OUTER JOIN".equalsIgnoreCase(joinType) ? "selected" : "") + ">Full Outer Join</option>"
					+ "</select>" + "</div>" + "<div class=\"visionDxpJoinClauseTablesDiv\">"
					+ "<table class=\"visionEtlJoinClauseTable\" id='etlJoinClauseTable' style='width: 100%;' border='1'>"
					+ "<thead>" + "<tr>"
					+ "<th width='5%' style='background: #f1f1f1 none repeat scroll 0 0;text-align: center'></th>"
					+ "<th width='35%' style='background: #f1f1f1 none repeat scroll 0 0;text-align: center'>Child Column</th>"
					+ "<th width='10%' style='background: #f1f1f1 none repeat scroll 0 0;text-align: center'>Operator</th>"
					+ "<th width='35%' style='background: #f1f1f1 none repeat scroll 0 0;text-align: center'>Master Column</th>"
					+ "<th width='10%' style='background: #f1f1f1 none repeat scroll 0 0;text-align: center'>Static Value</th>"
					+ "<th width='5%' style='background: #f1f1f1 none repeat scroll 0 0;text-align: center'>AND/OR</th>"
					+ "" + "</tr>" + "</thead>" + "<tbody>" + "";
			joinsDataStr += mappedColTrString + "</tbody>" + "" + "</table>" + "" + "</div>" + "</div>";
			resultObj.put("joinsDataStr", joinsDataStr);
			resultObj.put("trString", singleTrString); // ravi edit

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
			}
		}
		return resultObj;

	}

	public String importTreeDMFile(HttpServletRequest request, HttpServletResponse response, MultipartFile file1,
			String selectedFiletype) {

		String result = "";
		String filename = "";
		JSONObject importResult = new JSONObject();
		try {
			String excelFilePath = etlFilePath + "Files/TreeDMImport/"
					+ request.getSession(false).getAttribute("ssUsername");
			boolean isMultipart = ServletFileUpload.isMultipartContent(request);
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
			byte[] bytes = file1.getBytes();
			filename = file1.getOriginalFilename();
			System.out.println("filenAME:::" + filename);
			String fileType1 = filename.substring(filename.lastIndexOf(".") + 1, filename.length());
			String mainFileName = "SPIRUploadSheet" + System.currentTimeMillis() + "." + fileType1;
			selectedFiletype = selectedFiletype.toLowerCase();
			fileType1 = fileType1.toLowerCase();

			if (selectedFiletype != null && !"".equalsIgnoreCase(selectedFiletype) && fileType1 != null
					&& !"".equalsIgnoreCase(fileType1) && !selectedFiletype.equalsIgnoreCase(fileType1)) {
				result = "Please upload " + selectedFiletype + " files only";
				importResult.put("result", result);
				importResult.put("flag", "Fail");
			} else {
				if (filename != null) {
					if (filename.lastIndexOf(File.separator) >= 0) {

						file = new File(filename);
					} else {
						file = new File(excelFilePath + File.separator + mainFileName);
					}

					FileOutputStream osf = new FileOutputStream(file);

					osf.write(bytes);
					osf.flush();
					osf.close();

					HttpHeaders headers = new HttpHeaders();
					// headers.setContentType(MediaType.parseMediaType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"));
					headers.setContentType(MediaType.MULTIPART_FORM_DATA);
					headers.setAccept(Collections.singletonList(MediaType.MULTIPART_FORM_DATA));
					FileSystemResource fileData = new FileSystemResource(file);
					MultiValueMap inputMap = new LinkedMultiValueMap();
					inputMap.add("fileName", fileData);
					inputMap.add("flag", "Y");
					// inputMap.add("Content-disposition", "form-data; name=file;
					// filename="+mainFileName+"");
					// inputMap.add("Content-type",
					// "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
					String dataCorrelaltionApiUrl = "http://idxp.pilogcloud.com:6649/file/";
					HttpEntity<MultiValueMap<String, Object>> entity = new HttpEntity<MultiValueMap<String, Object>>(
							inputMap, headers);
					RestTemplate template = new RestTemplate();
					ResponseEntity<byte[]> apiResponse = template.postForEntity(dataCorrelaltionApiUrl, entity,
							byte[].class);
					byte[] apiDataObj = apiResponse.getBody();
					if (apiDataObj != null) {
						file.delete();
						mainFileName = "SPIRUploadSheet" + System.currentTimeMillis() + "." + fileType1;
						FileOutputStream output = new FileOutputStream(
								new File(excelFilePath + File.separator + mainFileName));
						filename = excelFilePath + File.separator + mainFileName;
						IOUtils.write(apiDataObj, output);
					}

					try {
						// dashBoardsDAO.saveUserFiles(request, originalFileName, mainFileName,
						// filePath, selectedFiletype);
					} catch (Exception e) {
					}
					String gridId = "divGrid-" + mainFileName.replace("." + selectedFiletype, "");
					gridId = gridId.replace(".csv", "");

					importResult = getFileObjectMetaData(request, response, filename, gridId, selectedFiletype,
							mainFileName);
				} else {
					result = "[]";
					importResult.put("result", result);
					importResult.put("flag", "Fail");
				}
			}

			importResult.put("fileName", mainFileName);
			importResult.put("fileType", fileType1);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return importResult.toJSONString();
	}

	public JSONObject getChatBotResponse(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
//            String gridId = request.getParameter("gridId");
			String message = (String) request.getParameter("message");
			String username = (String) request.getParameter("username");
			String sessionId = (String) request.getParameter("sessionId");
			String lang = (String) request.getParameter("lang");
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
			MultiValueMap<String, String> inputMap = new LinkedMultiValueMap<>();
			inputMap.add("msg", message);
			inputMap.add("user_name", sessionId);
			inputMap.add("lang", lang);
			HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<MultiValueMap<String, String>>(inputMap,
					headers);
			RestTemplate template = new RestTemplate();
			ResponseEntity<JSONObject> response = template.postForEntity("http://idxp.pilogcloud.com:6653/chatbot/",
					entity, JSONObject.class);
			JSONObject apiDataObj = response.getBody();
//            if (apiDataObj != null && !apiDataObj.isEmpty()) {
//            	if(!(apiDataObj.get("says") !=null && !((ArrayList)apiDataObj.get("says")).isEmpty()))
//            	{
//            		JSONArray saysArr = new JSONArray();
//            		saysArr.add("Please select the below options");
//            		apiDataObj.put("says",saysArr );
//            	}
//            	resultObj.put("ice", apiDataObj); 
//            }
			if (apiDataObj != null && !apiDataObj.isEmpty()) {
				if ((apiDataObj.get("says") != null && !((ArrayList) apiDataObj.get("says")).isEmpty())) {
					ArrayList<String> saysArr = new ArrayList();
					saysArr.add("Please select the below options");
					saysArr = (ArrayList) apiDataObj.get("says");
					// String colLabel = (String) saysArr.get(0);
					for (int i = 0; i < saysArr.size(); i++) {
						String collect = Stream.of(saysArr.get(i).trim().split("\\s")).filter(word -> word.length() > 0)
								.map(word -> word.substring(0, 1).toUpperCase() + word.substring(1))
								.collect(Collectors.joining(" "));
						saysArr.set(i, collect);
					}
					apiDataObj.put("says", saysArr);
				}
				resultObj.put("ice", apiDataObj);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getUserTableNames(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String editorFlag = request.getParameter("editorFlag");
			if (editorFlag != null && !"".equalsIgnoreCase(editorFlag) && "Y".equalsIgnoreCase(editorFlag)) {
				resultObj = dashBoardsDAO.getEditorViewUserTableNames(request);
			} else {
				resultObj = dashBoardsDAO.getUserTableNames(request);
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return resultObj;
	}

	/*
	 * public JSONObject importIntelliSenseTreeDMFileXlsx(HttpServletRequest
	 * request, HttpServletResponse response, JSONObject jsonData, String
	 * selectedFiletype) {
	 * 
	 * FileInputStream inputStream = null; FileOutputStream outs = null; JSONObject
	 * importResult = new JSONObject(); try { if (true) { // fis = new
	 * FileInputStream(new File(filepath)); String originalFileName =
	 * request.getParameter("fileName"); String userName = (String)
	 * request.getSession(false).getAttribute("ssUsername"); String filePath =
	 * fileStoreHomedirectory + "TreeDMImport/" + userName; // String filePath =
	 * "C:/Files/TreeDMImport" + File.separator + userName;
	 * 
	 * String mainFileName = "SPIRUploadSheet" + System.currentTimeMillis() + "." +
	 * selectedFiletype; String fileName = filePath + File.separator + mainFileName;
	 * 
	 * String headersObjStr = request.getParameter("headersObj"); JSONObject
	 * headersObj = (JSONObject) JSONValue.parse(headersObjStr);
	 * 
	 * String sheetsStr = request.getParameter("sheets"); JSONArray sheetsArray =
	 * (JSONArray) JSONValue.parse(sheetsStr);
	 * 
	 * File outputFile = new File(filePath); if (outputFile.exists()) {
	 * outputFile.delete(); } if (!outputFile.exists()) { outputFile.mkdirs(); }
	 * 
	 * XSSFWorkbook outputWb = new XSSFWorkbook(); // Workbook outputWb =
	 * (XSSFWorkbook) WorkbookFactory.create(new File(fileName)); SimpleDateFormat
	 * sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"); CellStyle dateCellStyle
	 * = outputWb.createCellStyle(); CellStyle timeCellStyle =
	 * outputWb.createCellStyle(); CreationHelper createHelper =
	 * outputWb.getCreationHelper();
	 * dateCellStyle.setDataFormat(createHelper.createDataFormat().getFormat(
	 * "dd-MM-yyyy"));
	 * timeCellStyle.setDataFormat(createHelper.createDataFormat().getFormat(
	 * "h:mm:ss"));
	 * 
	 * for (Object sheet : sheetsArray) { XSSFSheet outputSheet =
	 * outputWb.createSheet((String) sheet);
	 * 
	 * JSONArray sheetData = (JSONArray) jsonData.get(sheet); JSONArray sheetHeaders
	 * = (JSONArray) headersObj.get(sheet); XSSFRow outPutHeader =
	 * outputSheet.createRow(0);
	 * 
	 * for (int cellIndex = 0; cellIndex < sheetHeaders.size(); cellIndex++) {
	 * 
	 * WritableFont cellFont = new WritableFont(WritableFont.TIMES, 16);
	 * 
	 * WritableCellFormat cellFormat = new WritableCellFormat(cellFont);
	 * cellFormat.setBackground(Colour.ORANGE); XSSFCellStyle cellStyle =
	 * outputWb.createCellStyle();
	 * cellStyle.setAlignment(HSSFCellStyle.ALIGN_CENTER);
	 * cellStyle.setWrapText(true);
	 * 
	 * String cellValue = (String) sheetHeaders.get(cellIndex);
	 * 
	 * XSSFCell hssfCell = outPutHeader.createCell(cellIndex);
	 * hssfCell.setCellStyle(cellStyle);
	 * 
	 * hssfCell.setCellValue(cellValue);
	 * 
	 * }
	 * 
	 * for (int i = 0; i < sheetData.size(); i++) {
	 * 
	 * XSSFRow outPutRow = outputSheet.createRow(i + 1);
	 * 
	 * JSONObject rowData = (JSONObject) sheetData.get(i); if (rowData != null) {
	 * 
	 * for (int cellIndex = 0; cellIndex < sheetHeaders.size(); cellIndex++) {
	 * String header = (String) sheetHeaders.get(cellIndex); Object cellValue =
	 * rowData.get(header); XSSFCell outputCell = null; try { //
	 * System.out.println(i+ " cellIndex::::" + cellIndex); outputCell =
	 * outPutRow.createCell(cellIndex); if (cellValue != null) {
	 * 
	 * if (cellValue instanceof String) { if (isValidDate((String) cellValue)) {
	 * Date date = sdf.parse((String) cellValue); outputCell.setCellValue(date); if
	 * (((String) cellValue).contains("1899-12-31T")) { String timeStr = ((String)
	 * cellValue).substring(11, 19); Double timeDouble =
	 * DateUtil.convertTime(timeStr); outputCell.setCellValue(timeDouble);
	 * outputCell.setCellStyle(timeCellStyle); } else {
	 * outputCell.setCellStyle(dateCellStyle); }
	 * 
	 * } else { outputCell.setCellValue((String) cellValue); }
	 * 
	 * // outputCell.setCellType(CellType._NONE); } else if (cellValue instanceof
	 * Number) { outputCell.setCellValue(Double.valueOf(String.valueOf(cellValue)));
	 * } else if (cellValue instanceof Boolean) { outputCell.setCellValue((Boolean)
	 * cellValue); } else { outputCell.setCellValue(String.valueOf(cellValue)); }
	 * 
	 * } else { outputCell.setCellValue(""); }
	 * 
	 * } catch (Exception e) { outputCell.setCellValue(""); continue; }
	 * 
	 * } }
	 * 
	 * } } outs = new FileOutputStream(fileName); outputWb.write(outs);
	 * outs.close();
	 * 
	 * importResult = createIntelliSenseTableasFile(request, response,
	 * mainFileName); } // return result1; if (inputStream != null) {
	 * inputStream.close(); }
	 * 
	 * } catch (Exception e) {
	 * 
	 * e.printStackTrace(); }
	 * 
	 * return importResult; }
	 */

	public JSONObject importIntelliSenseTreeDMFileXlsx(HttpServletRequest request, HttpServletResponse response,
			JSONObject jsonData, String selectedFiletype) {

		FileInputStream inputStream = null;
		FileOutputStream outs = null;
		JSONObject importResult = new JSONObject();
		try {
			if (true) {
				// fis = new FileInputStream(new File(filepath));
				String originalFileName = request.getParameter("fileName");
				String userName = (String) request.getSession(false).getAttribute("ssUsername");
				String filePath = etlFilePath + "Files/TreeDMImport/" + userName;
//				String filePath = "C:/Files/TreeDMImport" + File.separator + userName;

				String mainFileName = "SPIRUploadSheet" + System.currentTimeMillis() + "." + selectedFiletype;
				String fileName = filePath + File.separator + mainFileName;

				String headersObjStr = request.getParameter("headersObj");
				JSONObject headersObj = (JSONObject) JSONValue.parse(headersObjStr);

				String sheetsStr = request.getParameter("sheets");
				JSONArray sheetsArray = (JSONArray) JSONValue.parse(sheetsStr);

				File outputFile = new File(filePath);
				if (outputFile.exists()) {
					outputFile.delete();
				}
				if (!outputFile.exists()) {
					outputFile.mkdirs();
				}

				XSSFWorkbook outputWb = new XSSFWorkbook();
//                Workbook outputWb = (XSSFWorkbook) WorkbookFactory.create(new File(fileName));
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
				CellStyle dateCellStyle = outputWb.createCellStyle();
				CellStyle timeCellStyle = outputWb.createCellStyle();
				CreationHelper createHelper = outputWb.getCreationHelper();
				dateCellStyle.setDataFormat(createHelper.createDataFormat().getFormat("dd-MM-yyyy"));
				timeCellStyle.setDataFormat(createHelper.createDataFormat().getFormat("h:mm:ss"));

				for (Object sheet : sheetsArray) {
					XSSFSheet outputSheet = outputWb.createSheet((String) sheet);

					JSONArray sheetData = (JSONArray) jsonData.get(sheet);
					JSONArray sheetHeaders = (JSONArray) headersObj.get(sheet);
					XSSFRow outPutHeader = outputSheet.createRow(0);

					for (int cellIndex = 0; cellIndex < sheetHeaders.size(); cellIndex++) {

						WritableFont cellFont = new WritableFont(WritableFont.TIMES, 16);

						WritableCellFormat cellFormat = new WritableCellFormat(cellFont);
						cellFormat.setBackground(Colour.ORANGE);
						XSSFCellStyle cellStyle = outputWb.createCellStyle();
						cellStyle.setAlignment(HSSFCellStyle.ALIGN_CENTER);
						cellStyle.setWrapText(true);

						String cellValue = (String) sheetHeaders.get(cellIndex);

						XSSFCell hssfCell = outPutHeader.createCell(cellIndex);
						hssfCell.setCellStyle(cellStyle);

						hssfCell.setCellValue(cellValue);

					}

					for (int i = 0; i < sheetData.size(); i++) {

						XSSFRow outPutRow = outputSheet.createRow(i + 1);

						JSONObject rowData = (JSONObject) sheetData.get(i);
						if (rowData != null) {

							for (int cellIndex = 0; cellIndex < sheetHeaders.size(); cellIndex++) {
								String header = (String) sheetHeaders.get(cellIndex);
								Object cellValue = rowData.get(header);
								XSSFCell outputCell = null;
								try {
//                            System.out.println(i+ " cellIndex::::" + cellIndex);
									outputCell = outPutRow.createCell(cellIndex);
									if (cellValue != null) {

										if (cellValue instanceof String) {
											if (isValidDate((String) cellValue)) {
												Date date = sdf.parse((String) cellValue);
												outputCell.setCellValue(date);
												if (((String) cellValue).contains("1899-12-31T")) {
													String timeStr = ((String) cellValue).substring(11, 19);
													Double timeDouble = DateUtil.convertTime(timeStr);
													outputCell.setCellValue(timeDouble);
													outputCell.setCellStyle(timeCellStyle);
												} else {
													outputCell.setCellStyle(dateCellStyle);
												}

											} else {
												outputCell.setCellValue((String) cellValue);
											}

//                                            outputCell.setCellType(CellType._NONE);
										} else if (cellValue instanceof Number) {
											outputCell.setCellValue(Double.valueOf(String.valueOf(cellValue)));
										} else if (cellValue instanceof Boolean) {
											outputCell.setCellValue((Boolean) cellValue);
										} else {
											outputCell.setCellValue(String.valueOf(cellValue));
										}

									} else {
										outputCell.setCellValue("");
									}

								} catch (Exception e) {
									outputCell.setCellValue("");
									continue;
								}

							}
						}

					}
				}
				outs = new FileOutputStream(fileName);
				outputWb.write(outs);
				outs.close();

				importResult = createIntelliSenseTableasFile(request, response, mainFileName);
			}
			// return result1;
			if (inputStream != null) {
				inputStream.close();
			}

		} catch (Exception e) {

			e.printStackTrace();
		}

		return importResult;
	}

	public JSONObject createIntelliSenseTableasFile(HttpServletRequest request, HttpServletResponse response,
			String mainFileName) {
		JSONObject resultObj = new JSONObject();
		JSONArray checkBoxDataArr = new JSONArray();
		try {
			resultObj = dashBoardsDAO.createIntelliSenseTableasFile(request, response, mainFileName);

		} catch (Exception e) {
			e.printStackTrace();
		}

		return resultObj;
	}

	public JSONObject getIntelliSenseTableColumns(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		JSONArray checkBoxDataArr = new JSONArray();
		try {
			String editorFlag = request.getParameter("editorFlag");
			if (editorFlag != null && !"".equalsIgnoreCase(editorFlag) && "Y".equalsIgnoreCase(editorFlag)) {
				resultObj = dashBoardsDAO.getEditorViewTableColumns(request);
			} else {
				resultObj = dashBoardsDAO.getIntelliSenseTableColumns(request);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getIntelliSenseChartTypes(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		JSONArray checkBoxDataArr = new JSONArray();
		try {
			resultObj = dashBoardsDAO.getIntelliSenseChartTypes(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getIntelliSenseChartColumns(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.getIntelliSenseChartColumns(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getIntelliSenseChartConfig(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj.put("configOptions", getChartFilters(request));

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getIntelliSenseExampleChartDesign(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.getIntelliSenseExampleChartDesign(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getIntelliSenseChartSubColumns(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.getIntelliSenseChartSubColumns(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getIntelliSenseViewFilters(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.getIntelliSenseViewFilters(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getIntelliSenseViewFiltersValues(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.getIntelliSenseViewFiltersValues(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getEditorMergeTableNames(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.getEditorMergeTableNames(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getEditorMergeTableColumns(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.getEditorMergeTableColumns(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject checkExistMergeTableName(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.checkExistMergeTableName(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject createTableANdJoinTables(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = chartJoinTables(request);
			resultObj.put("tableDiv", dashBoardsDAO.createTableANdJoinTables(request));

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject insertMergeTablesData(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.insertMergeTablesData(request);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getChatRplyResponse(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String message = (String) request.getParameter("message");
			String sessionId = (String) request.getParameter("sessionId");
			String lang = (String) request.getParameter("lang");
			String ssUsername = (String) request.getSession(false).getAttribute("ssUsername");
			String mainDiv = "";
			JSONObject dbDetailsFromDB = dashBoardsDAO.getDBDetailsFromDB(request);
			String tempdataBaseDriver = dataBaseDriver, tempuserName = userName, tempdbURL = dbURL,
					temppassword = password;
			if (dbDetailsFromDB != null && !dbDetailsFromDB.isEmpty()) {
				tempdataBaseDriver = (String) dbDetailsFromDB.getOrDefault("dataBaseDriver", dataBaseDriver);
				tempuserName = (String) dbDetailsFromDB.getOrDefault("userName", userName);
				tempdbURL = (String) dbDetailsFromDB.getOrDefault("url", dbURL);
				temppassword = (String) dbDetailsFromDB.getOrDefault("password", password);

			}
			JSONObject dbDetails = new PilogUtilities().getDatabaseDetails(tempdataBaseDriver, tempdbURL, tempuserName,
					temppassword, "DH11024");

			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
			MultiValueMap<String, String> inputMap = new LinkedMultiValueMap();
			inputMap.add("msg", message);
			inputMap.add("sessionId", sessionId);
			inputMap.add("name", ssUsername);
			inputMap.add("user_name", tempuserName);
			inputMap.add("password", temppassword);
			inputMap.add("host", (String) dbDetails.get("HOST_NAME"));
			inputMap.add("port", (String) dbDetails.get("CONN_PORT"));
			inputMap.add("access_name", (String) dbDetails.get("CONN_DB_NAME"));
			/* inputMap.add("lang", lang); */
			HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<MultiValueMap<String, String>>(inputMap,
					headers);
			RestTemplate template = new RestTemplate();
			ResponseEntity<JSONObject> response = template.postForEntity("http://idxp.pilogcloud.com:6658/txtsql/",
					entity, JSONObject.class);
			JSONObject apiDataObj = response.getBody();

			resultObj.put("result", apiDataObj);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getConvAIMergeTableColumns(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.getConvAIMergeTableColumns(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public String updateCardData(HttpServletRequest request) {
		return dashBoardsDAO.updateCardData(request);
	}

	public String deleteDashboard(HttpServletRequest request) {
		return dashBoardsDAO.deleteDashboard(request);
	}

	public JSONObject saveFileOnServer(HttpServletRequest request, MultipartFile multipartFileData) {
		JSONObject fileObj = new JSONObject();
		try {
			String userName = (String) request.getSession(false).getAttribute("ssUsername");
			String fileName = StringUtils.cleanPath(multipartFileData.getOriginalFilename());
			String fileExtension = FilenameUtils.getExtension(fileName);
			String updatedFileName = "SPIRUploadSheet" + System.currentTimeMillis() + "." + fileExtension;
			String fileDirectoryOnserver = etlFilePath + "Files/TreeDMImport/" + userName;
			boolean isFileUploaded = false;
			if ("json".equalsIgnoreCase(fileExtension)) {
				File file = new File(fileName);
				String excelFilePath = etlFilePath + "Files/TreeDMImport/"
						+ request.getSession(false).getAttribute("ssUsername");
				if (fileName.lastIndexOf(File.separator) >= 0) {

					file = new File(fileName);
				} else {

					file = new File(excelFilePath + File.separator + updatedFileName);
				}

				FileOutputStream osf = new FileOutputStream(file);

				osf.write(multipartFileData.getBytes());
				osf.flush();
				osf.close();

				// need to save
				if ("json".equalsIgnoreCase(fileExtension)) {

					try {
						updatedFileName = convertJSONtoCSV(file, excelFilePath);
					} catch (Exception e) {
					}

				}
				isFileUploaded = true;
			} else {
				isFileUploaded = dashBoardUtills.saveFileOnServer(fileDirectoryOnserver, updatedFileName,
						multipartFileData);
			}

			if (!isFileUploaded) {
				fileObj.put("uploadStatus", "false");
				return fileObj;
			}
			fileObj.put("isFileExits", dashBoardsDAO.checkExistMergeTableName(request));
			fileObj.put("uploadStatus", "true");
			fileObj.put("originalFileName", fileName);
			fileObj.put("updatedFileName", updatedFileName);
			fileObj.put("fileExtension", fileExtension);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return fileObj;
	}

	public JSONObject getEditDashBoardNames(HttpServletRequest request) {
		return dashBoardsDAO.getEditDashBoardNames(request);
	}

	public JSONObject getDeleteDashBoardNames(HttpServletRequest request) {
		return dashBoardsDAO.getDeleteDashBoardNames(request);
	}

	public JSONObject getShareDashBoardList(HttpServletRequest request) {
		return dashBoardsDAO.getShareDashBoardList(request);
	}

	public JSONObject getShareMailDashBoardList(HttpServletRequest request) {
		return dashBoardsDAO.getShareMailDashBoardList(request);
	}

	public JSONObject getSaveDashBoardNames(HttpServletRequest request) {
		return dashBoardsDAO.getSaveDashBoardNames(request);
	}

	public JSONObject getWeatherDetailsByCity(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();

		RestTemplate restTemplate = new RestTemplate();
		String city = request.getParameter("city");
		String flag = request.getParameter("flag");
		MultiValueMap<String, String> formData = new LinkedMultiValueMap<>();
		formData.add("city", city);
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.MULTIPART_FORM_DATA);
		headers.setAccept(Collections.singletonList(MediaType.MULTIPART_FORM_DATA));
		HttpEntity<MultiValueMap<String, String>> requestEntity = new HttpEntity<MultiValueMap<String, String>>(
				formData, headers);

		String apiUrl = "http://idxp.pilogcloud.com:6671/weather_report/"; // Replace with the API endpoint URL
		ResponseEntity<JSONObject> response = restTemplate.postForEntity(apiUrl, requestEntity, JSONObject.class);
		JSONObject responseBody = response.getBody();
		String htmlDiv = "";
		LocalDate currentDate = LocalDate.now();

		// Define a custom pattern for the month format
		DateTimeFormatter monthFormatter = DateTimeFormatter.ofPattern("MMMM", Locale.ENGLISH);

		// Format the month of the current date
		String formattedMonth = monthFormatter.format(currentDate);

		// Create a custom format pattern for the entire date
		DateTimeFormatter customFormatter = DateTimeFormatter.ofPattern("d MMMM, yyyy", Locale.ENGLISH);

		// Format the entire current date using the custom pattern
		String formattedDate = customFormatter.format(currentDate);

		// Combine the formatted date and location
		String location = "Hyderabad, India";
		String result = formattedDate + " " + location;
		LocalDate currentDatee = LocalDate.now();
		DayOfWeek dayOfWeek = currentDatee.getDayOfWeek();
		if (responseBody != null && !responseBody.isEmpty()) {
			if (flag == null || flag.isEmpty() || !flag.equalsIgnoreCase("HD")) {
				Date todayDate = new Date();

				htmlDiv = "<div class=\"weatherContent page-container\" id=\"weatherContent\">\r\n"
						+ "        <div class=\"mainWrapperDiv\">\r\n" + "            <div class=\"subWrapper\">\r\n"
						+ "    <div class=\"grid-margin stretch-card\">\r\n"
						+ "                  <!--weather card-->\r\n"
						+ "                  <div class=\"card card-weather\">\r\n"
						+ "                    <div class=\"card-body cardInMainDiv weatherMiddleInnerContent\">\r\n"
						+ "                      <div class=\"temp_Location\">\r\n"
						+ "                        <div class=\"weather-date-location\">\r\n"
						+ "                          <h3>" + dayOfWeek + "</h3>\r\n"
						+ "                          <p class=\"text-gray\">\r\n"
						+ "                            <span class=\"weather-date\">" + result + "</span>\r\n"
						+ "                          </p>\r\n" + "                        </div>\r\n"
						+ "                        <div class=\"weather-data\">\r\n"
						+ "                          <div class=\"mr-auto\">\r\n"
						+ "                            <h4 class=\"display-3\">" + responseBody.get("temp_celsius")
						+ "\r\n" + "                              <sup class=\"symbol\">&deg;</sup>C</h4>\r\n"
						+ "                            <p>\r\n" + "                              "
						+ responseBody.get("description") + "\r\n" + "                            </p>"
						+ "                          </div>"
						+ "                          <div class='weatherReport'><div>Precipitation:"
						+ responseBody.get("feels_like_celsius") + "%</div><div>Humidity:"
						+ responseBody.get("humidity") + "%</div><div>Wind:" + responseBody.get("wind")
						+ "km/h</div></div>" + "                        </div>\r\n" + "                      </div>\r\n"
						+ "                      <div class=\"changerContent\"> \r\n"
						+ "                        <div class=\"changeButtonMainDiv\">\r\n"
						+ "                          <div class=\"buttonClass active-link\" onclick=\"opentab('temp')\">Temperature</div>\r\n"
						+ "                          <div class=\"buttonClass\" onclick=\"opentab('prec')\">Precipitation</div>\r\n"
						+ "                          <div class=\"buttonClass\" onclick=\"opentab('wind')\">Wind</div>\r\n"
						+ "                        </div>\r\n"
						+ "                          <div class=\"passDataInfo active-tab\" id=\"temp\">\r\n"
						+ "                            <span><img src='images/temp_img.png'></span>\r\n"
						+ "                          </div>\r\n"
						+ "                          <div class=\"passDataInfo\" id=\"prec\">\r\n"
						+ "                           <span><img src='images/perception.png'></span>\r\n"
						+ "                          </div>\r\n"
						+ "                          <div class=\"passDataInfo\" id=\"wind\">\r\n"
						+ "                            <span><img src='images/wind.png'></span>\r\n"
						+ "                          </div>\r\n" + "                      </div>\r\n"
						+ "                    </div>\r\n"
						+ "                      <div class=\"bottomWeatherIcons weakly-weather\">\r\n"
						+ "                        <div class=\"weakly-weather-item\">\r\n"
						+ "                          <p class=\"mb-0\">\r\n" + "                            Sun\r\n"
						+ "                          </p>\r\n"
						+ "                          <i class=\"mdi mdi-weather-cloudy\"></i>\r\n"
						+ "                          <p class=\"mb-0\">\r\n" + "                            30&deg;\r\n"
						+ "                          </p>\r\n" + "                        </div>\r\n"
						+ "                        <div class=\"weakly-weather-item\">\r\n"
						+ "                          <p class=\"mb-1\">\r\n" + "                            Mon\r\n"
						+ "                          </p>\r\n"
						+ "                          <i class=\"mdi mdi-weather-hail\"></i>\r\n"
						+ "                          <p class=\"mb-0\">\r\n" + "                            31&deg;\r\n"
						+ "                          </p>\r\n" + "                        </div>\r\n"
						+ "                        <div class=\"weakly-weather-item\">\r\n"
						+ "                          <p class=\"mb-1\">\r\n" + "                            Tue\r\n"
						+ "                          </p>\r\n"
						+ "                          <i class=\"mdi mdi-weather-partlycloudy\"></i>\r\n"
						+ "                          <p class=\"mb-0\">\r\n" + "                            28&deg;\r\n"
						+ "                          </p>\r\n" + "                        </div>\r\n"
						+ "                        <div class=\"weakly-weather-item\">\r\n"
						+ "                          <p class=\"mb-1\">\r\n" + "                            Wed\r\n"
						+ "                          </p>\r\n"
						+ "                          <i class=\"mdi mdi-weather-pouring\"></i>\r\n"
						+ "                          <p class=\"mb-0\">\r\n" + "                            30&deg;\r\n"
						+ "                          </p>\r\n" + "                        </div>\r\n"
						+ "                        <div class=\"weakly-weather-item\">\r\n"
						+ "                          <p class=\"mb-1\">\r\n" + "                            Thu\r\n"
						+ "                          </p>\r\n"
						+ "                          <i class=\"mdi mdi-weather-pouring\"></i>\r\n"
						+ "                          <p class=\"mb-0\">\r\n" + "                            29&deg;\r\n"
						+ "                          </p>\r\n" + "                        </div>\r\n"
						+ "                        <div class=\"weakly-weather-item\">\r\n"
						+ "                          <p class=\"mb-1\">\r\n" + "                            Fri\r\n"
						+ "                          </p>\r\n"
						+ "                          <i class=\"mdi mdi-weather-snowy-rainy\"></i>\r\n"
						+ "                          <p class=\"mb-0\">\r\n" + "                            31&deg;\r\n"
						+ "                          </p>\r\n" + "                        </div>\r\n"
						+ "                        <div class=\"weakly-weather-item\">\r\n"
						+ "                          <p class=\"mb-1\">\r\n" + "                            Sat\r\n"
						+ "                          </p>\r\n"
						+ "                          <i class=\"mdi mdi-weather-snowy\"></i>\r\n"
						+ "                          <p class=\"mb-0\">\r\n" + "                            32&deg;\r\n"
						+ "                          </p>\r\n" + "                        </div>\r\n"
						+ "                      </div>\r\n" + "                    </div>\r\n"
						+ "                  <!--weather card ends-->\r\n" + "                </div>\r\n"
						+ "                </div>\r\n" + "                </div>\r\n" + "                </div>";
				resultObj.put("response", htmlDiv);

			} else {
				resultObj.put("temperature", responseBody.get("temp_celsius"));
				resultObj.put("description", responseBody.get("description"));
				resultObj.put("dayOfWeek", dayOfWeek);
				resultObj.put("sunrise", responseBody.get("sunrise_time"));
				resultObj.put("sunset", responseBody.get("sunset_time"));
			}
		}

		return resultObj;
	}

	public JSONObject getChartNotes(HttpServletRequest request) {
		return dashBoardsDAO.getChartNotes(request);
	}

	public JSONObject saveChartNotes(HttpServletRequest request) {
		return dashBoardsDAO.saveChartNotes(request);
	}

	public String getAreaPieacesDiv(String chartType) {
		return ("<li id=\"addingLowerAndUpperBound\" class=\"general-filters active-filter\" data-column-name=\"" + ""
				+ chartType + "AREAPIECES\" data-key-type=\"data\">"
				+ "<p style=\"display: block;\" >Area Pieces</p><br>" + "<div class=\"sub-filterItems\">"
				+ "      <input placeholder=\" Lower Bound\" type=\"number\" id=\"" + chartType
				+ "LOWERBOUND_0\" data-opt-name=\"width\" data-man=\"O\" title=\"\" style=\"width:50px\"> \r\n"
				+ "      <input placeholder=\" Upper Bound\" type=\"number\" id=\"" + chartType + "UPPERBOUND_0\"\r\n"
				+ "       data-opt-name=\"width\" data-man=\"O\" title=\"\" style=\"width:50px\"> \r\n"
				+ "<span style=\"width: 20px height: 20px\" id=\"plusbuttonInAreaPieces_0\" onclick=\"handlePlusInAreaPieces(event)\">+</span>"
				+ "<span style=\"display:none;width: 20px height: 20px\"  id=\"minusbuttonInAreaPieces_0\" onclick=\"handleMinusInAreaPieces(event)\">-</span>"
				+ "\n</div></li>");
	}

	public StringBuilder getLineColorProperties(String ChartType, String mode) {
		StringBuilder eChartProperties = new StringBuilder();
		eChartProperties
				.append("<li class=\"general-filters active-filter\" id=\"line-filter\" data-column-name=\"" + ChartType
						+ "\" data-key-type=\"data\">")
				.append("<div class=\"main-container\"><div class=\"filter-container\">")
				.append("<img src=\"images/down-chevron.png\" alt=\"Down Chevron\" class=\"icons visualDarkMode\"/>")
				.append("<p>Chart Line</p>").append("</div>").append(getToggleButton(ChartType, "")).append("</div>")
				.append("<ul class=\"sub-filters\" id=\"" + ChartType
						+ "\" data-opt-name=\"line\" style=\"display: none;\">")
				.append("<li class=\"sub-filterItems active-filter\" data-column-name=\"" + ChartType
						+ "LINECOLORS\" data-key-type=\"data\">")
				.append("<label>Line Color</label>")
				.append("<input type=\"hidden\" id=\"" + ChartType + "LINECOLORS\" value=\"#1864ab\">")
				.append("<input type=\"color\" id=\"" + ChartType
						+ "LINECOLORS_CLR\" data-opt-name=\"color\" onchange=\"populateSelectedColor(id,'" + ChartType
						+ "LINECOLORS','" + mode + "')\" value=\"#1864ab\">")
				.append("<div id=\"" + ChartType + "LINECOLORS_CLR_DIV\" class=\"colorsSelectDiv\"></div>")
				.append("</li>")
				.append("<li class=\"sub-filterItems active-filter\" data-column-name=\"" + ChartType
						+ "LINEWIDTH\" data-key-type=\"data\">")
				.append("<label>Line Width</label>")
				.append("<input type=\"number\" value =\"1\" id=\"" + ChartType
						+ "LINEWIDTH\" data-opt-name=\"width\" data-man=\"O\" title=\"\"/>")
				.append("</li>")
				.append("<li class=\"sub-filterItems active-filter\" data-column-name=\"" + ChartType
						+ "LINEDASH\" data-key-type=\"data\">")
				.append("<label>Line Dash</label>")
				.append("<select name=\"text-position\" id=\"" + ChartType
						+ "LINEDASH\" data-opt-name=\"dash\" data-man=\"O\" title=\"\">")
				.append("<option value=\"solid\">Solid</option>").append("<option value=\"dotted\">Dotted</option>")
				.append("<option value=\"smooth\">Curve</option>").append("<option value=\"dashed\">Dashed</option>")
				.append("</select>").append("</li>").append("</ul>").append("</li>");
		return eChartProperties;
	}

	public StringBuilder getChartAreaProperties(String ChartType, String mode) {
		StringBuilder eChartProperties = new StringBuilder();
		eChartProperties
				.append("<li class=\"general-filters active-filter\" id=\"marker-filter\" data-column-name=\""
						+ ChartType + "AREA\" data-key-type=\"data\">")
				.append("<div class=\"main-container active-filter\"><div class=\"filter-container\">")
				.append("<img src=\"images/down-chevron.png\" alt=\"Down Chevron\" class=\"icons visualDarkMode\"/>")
				.append("<p>Chart Area</p>").append("</div>").append(getToggleButton(ChartType, "")).append("</div>")
				.append("<ul class=\"sub-filters\" id=\"" + ChartType
						+ "\" data-opt-name=\"marker\" style=\"display: none;\">")
				.append("<li class=\"sub-filterItems active-filter\" data-column-name=\"" + ChartType
						+ "COLORSAREA\" data-key-type=\"data\">")
				.append("<label>Area Color</label>")
				.append("<input type=\"hidden\" id=\"" + ChartType + "COLORSAREA\" value=\"#1864ab\">")
				.append("<input type=\"color\" id=\"" + ChartType
						+ "COLORSAREA_CLR\" data-opt-name=\"color\" onchange=\"populateSelectedColor(id,'" + ChartType
						+ "AREACOLOR','" + mode + "')\" value=\"#1864ab\">")
				.append("<div id=\"" + ChartType + "COLORSAREA_CLR_DIV\" class=\"colorsSelectDiv\"></div>")
				.append("</li>")
				.append("<li class=\"sub-filterItems active-filter\" data-column-name=\"" + ChartType
						+ "OPACITY\" data-key-type=\"data\">")
				.append("<label>Opacity</label>")
				.append("<input type=\"number\" placeholder=\"Value Between 0 to 1\"id=\"" + ChartType
						+ "OPACITY\" data-opt-name=\"size\" data-man=\"O\" title=\"\"/>")
				.append("</li>").append("</ul>").append("</li>");
		return eChartProperties;
	}

	public StringBuilder getEchartProperties(String ChartType) {

		StringBuilder eChartProperties = new StringBuilder();
		eChartProperties
				.append("<li class=\"general-filters active-filter\" data-column-name=\"" + ChartType
						+ "MODE\" data-key-type=\"data\">")
				.append("<div class=\"sub-filterItems\"><label>Mode</label>")
				.append("<select name=\"text-info\" id=\"" + ChartType + "MODE\" data-opt-name=\"mode\">")
				.append("<option value=\"lines\" selected>Lines</option>")
				.append("<option value=\"lines+markers\">Lines and Markers</option>").append("</select>")
				.append("</div>").append("</li>");

		eChartProperties.append("<li class=\"general-filters active-filter\" data-column-name=\"" + ChartType
				+ "LABELDATA\" data-key-type=\"data\">" + "<div class=\"sub-filterItems\">" + "<p>Data Visible</p>"
				+ "<select name=\"text-info\" id=\"" + ChartType + "LABELDATA\" data-opt-name=\"textinfo\">"
				+ "<option value=\"''\">None</option>" + "<option value=\"x\">Label</option>"
				+ "<option value=\"y\">Value</option>" + "<option value=\"%\">Percentage</option>"
				+ "<option value=\"x+y\">Label and value</option>"
				+ "<option value=\"x+%\">Label and Percentage</option>"
				+ "<option value=\"y+%\">Value and Percentage</option>" + "</select>" + "</div>" + "</li>"
				+ "<li class=\"general-filters active-filter\" data-column-name=\"" + ChartType
				+ "HOVERLABELDATA\" data-key-type=\"data\">" + "<div class=\"sub-filterItems\">"
				+ "<p>Hover Data Visible</p>" + "<select name=\"text-info\" id=\"" + ChartType
				+ "HOVERLABELDATA\" data-opt-name=\"hoverinfo\" >" + "<option value=\"x\">Label</option>"
				+ "<option value=\"y\">Value</option>" + "<option value=\"%\">Percentage</option>"
				+ "<option value=\"x+y\" selected>Label and value</option>"
				+ "<option value=\"x+%\">Label and Percentage</option>"
				+ "<option value=\"y+%\">Value and Percentage</option>" + "</select>" + "</div>" + "</li>" + "</li>"
				+ "<li class=\"general-filters active-filter\" data-column-name=\"" + ChartType
				+ "LABELPOSITION\" data-key-type=\"data\">" + "<div class=\"sub-filterItems\">" + "<p>Text Position</p>"
				+ "<select name=\"text-position\" id=\"" + ChartType + "LABELPOSITION\" data-opt-name=\"textposition\">"
				+ "<option value=\"inside\">Inside</option>" + "<option value=\"top\">Top</option>"
				+ "<option value=\"right\">Right</option>" + "<option value=\"bottom\">Bottom</option>"
				+ "<option value=\"left\">Left</option>" + "</select>" + "</div>" + "</li>"
				+ "<li class=\"general-filters active-filter\" data-column-name=\"" + ChartType
				+ "MARKERSHAPE\" data-key-type=\"data\">" + "<div class=\"sub-filterItems\">" + "<p>Marker Shape</p>"
				+ "<select name=\"text-position\" id=\"" + ChartType + "MARKERSHAPE\" data-opt-name=\"textposition\">"
				+ "<option value=\"triangle\">Triangle</option>" + "<option value=\"square\">Square</option>"
				+ "<option value=\"circle\">Circle</option>" + "</select>" + "</div>" + "</li>");
		eChartProperties.append(getChartHover(ChartType, "data"));

		eChartProperties
				.append("<li id=\"marker-filter\" data-column-name=\"" + ChartType + "MARKER\" data-key-type=\"data\">")
				.append("<div class=\"main-container\"><div class=\"filter-container\">")
				.append("<img src=\"images/down-chevron.png\" alt=\"Down Chevron\" class=\"icons visualDarkMode\"/>")
				.append("<p>Chart Markers</p>").append("</div>").append(getToggleButton(ChartType, "")).append("</div>")
				.append("<ul class=\"sub-filters\" id=\"" + ChartType
						+ "MARKER\" data-opt-name=\"marker\" style=\"display: none;\">")
				.append("<li class=\"sub-filterItems active-filter\" data-column-name=\"" + ChartType
						+ "COLORSMARKER\" data-key-type=\"data\">")
				.append("<label>Marker Color</label>")
				.append("<input type=\"hidden\" id=\"" + ChartType + "COLORSMARKER\" value=\"\">")
				.append("<input type=\"color\" id=\"" + ChartType
						+ "COLORSMARKER_CLR\" data-opt-name=\"color\" onchange=\"populateSelectedColor(id,'" + ChartType
						+ "'MARKERCOLOR','M')\" value=\"#1864ab\">")
				.append("<div id=\"" + ChartType + "COLORSMARKER_CLR_DIV\" class=\"colorsSelectDiv\"></div>")
				.append("</li>")
				.append("<li class=\"sub-filterItems active-filter\" data-column-name=\"" + ChartType
						+ "MARKERSIZE\" data-key-type=\"data\">")
				.append("<label>Marker Size</label>")
				.append("<input type=\"number\" id=\"" + ChartType
						+ "MARKERSIZE\" data-opt-name=\"size\" data-man=\"O\" title=\"\"/>")
				.append("</li>").append("</ul>").append("</li>");

		return eChartProperties;
	}

	public JSONObject getVoiceResponse(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String inputText = (String) request.getParameter("inputText");
			String mainDiv = "";
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
			MultiValueMap<String, String> inputMap = new LinkedMultiValueMap();
			inputMap.add("query", inputText);
			HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<MultiValueMap<String, String>>(inputMap,
					headers);
			RestTemplate template = new RestTemplate();
			ResponseEntity<JSONObject> response = template
					.postForEntity("http://apihub.pilogcloud.com:6652/voice_command_data/", entity, JSONObject.class);
			JSONObject apiDataObj = response.getBody();

			resultObj.put("result", apiDataObj);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getLGFeaturesInfo(HttpServletRequest request) {
		String result = "";
		JSONArray featureArr = new JSONArray();
		JSONObject resultdata = new JSONObject();
		String priceStr = "";
		try {
			priceStr = "<div class='pricingCustomeTable'><table class='table table-hover table-bordered'><thead><tr class='active'> "
					+ "<th style=\"background:#fff\" class=\"firstChildClass\"><div id ='expandId' class='expandClass'>"
					+ "<img src='images/plus-solid.svg' width='16px' id='expandImageId' class='VisionImageVisualization' title='Show/Hide pane'/>"
					+ "<span class='expandClass'>Expand all</span></div></th>\r\n <th class='subscriptionClass'>"

					+ "<center><h3 class=\"PlanTitle\">Basic</h3><p\r\n class=\"text-muted text-sm\">Ideal\r\n for\r\n small operations.</p>\r\n "
					+ "<h3 class=\"panel-title price\"><del style=\"color:red\">$299</del>"
					+ " <b style=\"font-weight:1000\">$149 </b> User/Month</h3>\r\n <a href='https://smart.integraldataanalytics.com/getIGinfo?Lgtype=Basic' target='_blank'>"
					+ "<button class=\"btn btn-primary\" id='btnclass'>Buy now</button></a>\r\n </center></th>\r\n <th class='subscriptionClass'>"
					+ "<center><h3 class=\"PlanTitle\">Professional</h3><p\r\n class=\"text-muted text-sm\">Perfect\r\n for\r\n larger operations.</p>\r\n "
					+ "<h3 class=\"panel-title price\"><del style=\"color:red\">$399</del> "
					+ "<b style=\"font-weight:1000\">$199 </b> User/Month</h3>\r\n <a href='https://smart.integraldataanalytics.com/getIGinfo?Lgtype=Professional' target='_blank'>"
					+ "<button class=\"btn btn-primary\">Buy now</button></a></center></th>\r\n <th class='subscriptionClass'>"
					+ "<center><h3 class=\"PlanTitle\">Enterprise</h3><p\r\n class=\"text-muted text-sm\">Enterprise\r\n<h3 class=\"panel-title price\">$$$</h3>"
					+ "<a href='https://smart.integraldataanalytics.com/getIGinfo?Lgtype=Enterprise' target='_blank'>"
					+ "<button class=\"btn btn-primary\" >Buy now</button></a></center></th>\r\n</tr>\r\n</thead>\r\n";
			List processGstList = dashBoardsDAO.getFeaturesPrice();
			if (!processGstList.isEmpty()) {
				Clob clob = (Clob) processGstList.get(0);
//                    priceStr = piLogCloudUtills.(processGstList.get(0));
				priceStr = cloudUtills.clobToString(clob);
			} else {
				priceStr = "<div class='pricingCustomeTable'><table class='table table-hover table-bordered'><thead><tr class='active'> <th style=\"background:#fff\" class=\"firstChildClass\"><div id ='expandId' class='expandClass'><img src='images/plus-solid.svg' width='16px' id='expandImageId' class='VisionImageVisualization' title='Show/Hide pane'/><span class='expandClass'>Expand all</span></div></th>\r\n <th class='subscriptionClass'><center><h3 class=\"PlanTitle\">Basic</h3><p\r\n class=\"text-muted text-sm\">Ideal\r\n for\r\n small operations.</p>\r\n <h3 class=\"panel-title price\"><del style=\"color:red\">$299</del> <b style=\"font-weight:1000\">$149 </b> User/Month</h3>\r\n <a href='https://smart.integraldataanalytics.com/getIGinfo?Lgtype=Basic' target='_blank'><button class=\"btn btn-primary\" id='btnclass'>Buy now</button></a>\r\n </center></th>\r\n <th class='subscriptionClass'><center><h3 class=\"PlanTitle\">Professional</h3><p\r\n class=\"text-muted text-sm\">Perfect\r\n for\r\n larger operations.</p>\r\n <h3 class=\"panel-title price\"><del style=\"color:red\">$399</del> <b style=\"font-weight:1000\">$199 </b> User/Month</h3>\r\n <a href='https://smart.integraldataanalytics.com/getIGinfo?Lgtype=Professional' target='_blank'><button class=\"btn btn-primary\">Buy now</button></a></center></th>\r\n <th class='subscriptionClass'><center><h3 class=\"PlanTitle\">Enterprise</h3><p\r\n class=\"text-muted text-sm\">Enterprise\r\n<h3 class=\"panel-title price\">$$$</h3>\r\n<a href='https://smart.integraldataanalytics.com/getIGinfo?Lgtype=Enterprise' target='_blank'><button class=\"btn btn-primary\" >Buy now</button></a></center></th>\r\n</tr>\r\n</thead>\r\n";
			}
			List resultlist = dashBoardsDAO.getLgFeature(request);
//      result = result + "<div class='pricingCustomeTable'><table class='table table-hover table-bordered'><thead><tr class='active'> <th style=\"background:#fff\" class=\"firstChildClass\"><div id ='expandId' class='expandClass'><img src='images/plus-solid.svg' width='16px' id='expandImageId' class='VisionImageVisualization' title='Show/Hide pane'/><span class='expandClass'>Expand all</span></div></th>\r\n <th class='subscriptionClass'><center><h3 class=\"PlanTitle\">Basic</h3><p\r\n class=\"text-muted text-sm\">Ideal\r\n for\r\n small operations.</p>\r\n <h3 class=\"panel-title price\">99 $ User/Month</h3>\r\n <button class=\"btn btn-primary\" id='btnclass' onclick=getform('Basic_Model')>Buy now</button>\r\n </center></th>\r\n <th class='subscriptionClass'><center><h3 class=\"PlanTitle\">Professional</h3><p\r\n class=\"text-muted text-sm\">Perfect\r\n for\r\n larger operations.</p>\r\n <h3 class=\"panel-title price\">199 $ User/Month</h3>\r\n <button class=\"btn btn-primary\"onclick=getform('Standard_Model')>Buy now</button></center></th>\r\n <th class='subscriptionClass'><center><h3 class=\"PlanTitle\">Enterprise Model</h3><p\r\n class=\"text-muted text-sm\">Enterprise\r\n<h3 class=\"panel-title price\">$$$</h3>\r\n<button class=\"btn btn-primary\" onclick=getform('Enterprise_Model')>Buy now</button></center></th>\r\n</tr>\r\n</thead>\r\n";
			result = result + priceStr;
			if ((resultlist != null) && (!resultlist.isEmpty())) {
				String featureName = "";

				result = result + "<tbody>";
				for (int i = 0; i < resultlist.size(); i++) {
					Object[] rowData = (Object[]) resultlist.get(i);
					featureName = (String) rowData[0];
					String basicModel = (String) rowData[1];
					String standeredModel = (String) rowData[2];
					String EnterprisedModel = (String) rowData[3];
					String icon = (String) rowData[6];
					List featurelist = dashBoardsDAO.getSubFeature(request, featureName);
					if (featurelist.size() > 0) {
						if ((featureName.equalsIgnoreCase((String) rowData[0]))
								&& (!featureArr.contains(featureName))) {
							result = result + "<tr data-positionNumber='1' data-parentFeature = '"
									+ featureName.replaceAll(" ", "")
									+ "' data-toggle='collapse' data-target='#accordion"
									+ featureName.replaceAll(" ", "")
									+ "' class='clickable collapse-row collapsed'>\r\n<td colspan=\"1\" align=\"left\"  text-align: left !important;\"\r\nclass=\"active\"><span class='rowtitle'>"
									+ featureName + "</span><span id='imageid' class='imageClass'><img src='" + icon
									+ "' width='50px' id='expandImageId' class='VisionImageVisualization'></span></td>";
							if (basicModel.equalsIgnoreCase("Y")) {
								result = result
										+ "<td><i style=\"color:limegreen\" class=\"fa fa-check\nfa-lg\"></i></td>";
							} else if (basicModel.equalsIgnoreCase("N")) {
								result = result
										+ "<td><i style=\"color:red\" class=\"fa fa-times fa-lg\" aria-hidden=\"true\"></i></td>";
							} else {
								result = result + "<td>" + basicModel + "</td>\r\n";
							}
							if (standeredModel.equalsIgnoreCase("Y")) {
								result = result
										+ "<td><i style=\"color:limegreen\" class=\"fa fa-check\nfa-lg\"></i></td>";
							} else if (standeredModel.equalsIgnoreCase("N")) {
								result = result
										+ "<td><i style=\"color:red\" class=\"fa fa-times fa-lg\" aria-hidden=\"true\"></i></td>";
							} else {
								result = result + "<td>" + standeredModel + "</td>\r\n";
							}
							if (EnterprisedModel.equalsIgnoreCase("Y")) {
								result = result
										+ "<td><i style=\"color:limegreen\" class=\"fa fa-check\nfa-lg\"></i></td>";
							} else if (EnterprisedModel.equalsIgnoreCase("N")) {
								result = result
										+ "<td><i style=\"color:red\" class=\"fa fa-times fa-lg\" aria-hidden=\"true\"></i></td>";
							} else {
								result = result + "<td>" + EnterprisedModel + "</td>\r\n";
							}
							result = result + "</tr>\r\n";
							featureArr.add(featureName);
						}
					} else if ((featureName.equalsIgnoreCase((String) rowData[0]))
							&& (!featureArr.contains(featureName))) {
						result = result + "<tr  data-positionNumber='1' data-parentFeature = '"
								+ featureName.replaceAll(" ", "")
								+ "' class='clickable'>\r\n<td colspan=\"1\" align=\"left\"  text-align: left !important;\"\r\nclass=\"active\"><span class='rowtitle'>"
								+ featureName + "</span><span id='imageid' class='imageClass'><img src='" + icon
								+ "' width='50px' id='expandImageId' class='VisionImageVisualization'></span></td>";
						if (basicModel.equalsIgnoreCase("Y")) {
							result = result + "<td><i style=\"color:limegreen\" class=\"fa fa-check\nfa-lg\"></i></td>";
						} else if (basicModel.equalsIgnoreCase("N")) {
							result = result
									+ "<td><i style=\"color:red\" class=\"fa fa-times fa-lg\" aria-hidden=\"true\"></i></td>";
						} else {
							result = result + "<td>" + basicModel + "</td>\r\n";
						}
						if (standeredModel.equalsIgnoreCase("Y")) {
							result = result + "<td><i style=\"color:limegreen\" class=\"fa fa-check\nfa-lg\"></i></td>";
						} else if (standeredModel.equalsIgnoreCase("N")) {
							result = result
									+ "<td><i style=\"color:red\" class=\"fa fa-times fa-lg\" aria-hidden=\"true\"></i></td>";
						} else {
							result = result + "<td>" + standeredModel + "</td>\r\n";
						}
						if (EnterprisedModel.equalsIgnoreCase("Y")) {
							result = result + "<td><i style=\"color:limegreen\" class=\"fa fa-check\nfa-lg\"></i></td>";
						} else if (EnterprisedModel.equalsIgnoreCase("N")) {
							result = result
									+ "<td><i style=\"color:red\" class=\"fa fa-times fa-lg\" aria-hidden=\"true\"></i></td>";
						} else {
							result = result + "<td>" + EnterprisedModel + "</td>\r\n";
						}
						result = result + "</tr>\r\n";
						featureArr.add(featureName);
					}
					if ((featurelist != null) && (!featurelist.isEmpty())) {
						for (int j = 0; j < featurelist.size(); j++) {
							Object[] featurerowData = (Object[]) featurelist.get(j);
							String subfeturedata = (String) featurerowData[0];
							String subbasicModel = (String) featurerowData[1];
							String substanderedModel = (String) featurerowData[2];
							String subEntereprice = (String) featurerowData[3];
							String subicon = (String) featurerowData[4];
							List subfeaturelist = dashBoardsDAO.getSubFeature(request, subfeturedata);

							if (subfeaturelist.size() > 0) {
								if (subicon != null && !"null".equalsIgnoreCase(subicon)
										&& !"".equalsIgnoreCase(subicon)) {
									result = result + "<tr data-parentFeature = '" + featureName.replaceAll(" ", "")
											+ "' id='accordion" + featureName.replaceAll(" ", "")
											+ "' data-toggle='collapse' data-target='#accordion"
											+ subfeturedata.replaceAll(" ", "")
											+ "' class='clickable collapse-row collapsed'>\r\n<td colspan=\"1\" align=\"left\"  text-align: left !important;\"\r\nclass=\"active\"><span class='rowtitle'>"
											+ subfeturedata + "</span><span id='imageid' class='imageClass'><img src='"
											+ subicon
											+ "' width='50px' id='expandImageId' class='VisionImageVisualization'></span></td>";
								} else {
									result = result + "<tr data-parentFeature = '" + featureName.replaceAll(" ", "")
											+ "' id='accordion" + featureName.replaceAll(" ", "")
											+ "' data-toggle='collapse' data-target='#accordion"
											+ subfeturedata.replaceAll(" ", "")
											+ "' class='clickable collapse-row collapsed'>\r\n<td colspan=\"1\" align=\"left\"  text-align: left !important;\"\r\nclass=\"active\"><span class='rowtitle'>"
											+ subfeturedata + "</span></td>";
								}
								if (basicModel.equalsIgnoreCase("Y")) {
									result = result
											+ "<td><i style=\"color:limegreen\" class=\"fa fa-check\nfa-lg\"></i></td>";
								} else if (basicModel.equalsIgnoreCase("N")) {
									result = result
											+ "<td><i style=\"color:red\" class=\"fa fa-times fa-lg\" aria-hidden=\"true\"></i></td>";
								} else {
									result = result + "<td>" + basicModel + "</td>\r\n";
								}
								if (standeredModel.equalsIgnoreCase("Y")) {
									result = result
											+ "<td><i style=\"color:limegreen\" class=\"fa fa-check\nfa-lg\"></i></td>";
								} else if (standeredModel.equalsIgnoreCase("N")) {
									result = result
											+ "<td><i style=\"color:red\" class=\"fa fa-times fa-lg\" aria-hidden=\"true\"></i></td>";
								} else {
									result = result + "<td>" + standeredModel + "</td>\r\n";
								}
								if (EnterprisedModel.equalsIgnoreCase("Y")) {
									result = result
											+ "<td><i style=\"color:limegreen\" class=\"fa fa-check\nfa-lg\"></i></td>";
								} else if (EnterprisedModel.equalsIgnoreCase("N")) {
									result = result
											+ "<td><i style=\"color:red\" class=\"fa fa-times fa-lg\" aria-hidden=\"true\"></i></td>";
								} else {
									result = result + "<td>" + EnterprisedModel + "</td>\r\n";
								}
								result = result + "</tr>\r\n";
							} else {
								if (subicon != null && !"null".equalsIgnoreCase(subicon)
										&& !"".equalsIgnoreCase(subicon)) {
//               result = result + "<tr id='accordion" + featureName.replaceAll(" ", "") + "' class=\"collapse\"><td style='padding-left: 40px'><span class='rowtitle'>" + subfeturedata + "</span><span id='imageid' class='imageClass'><img src='" + subicon + "' width='50px' id='expandImageId' class='VisionImageVisualization'></span></td>";
									result = result + "<tr data-parentFeature = '" + featureName.replaceAll(" ", "")
											+ "' id='accordion" + featureName.replaceAll(" ", "")
											+ "' class=\"collapse\"><td style='padding-left: 40px'><span>"
											+ subfeturedata + "</span><span id='imageid' class='imageClass'><img src='"
											+ subicon
											+ "' width='50px' id='expandImageId' class='VisionImageVisualization'></span></td>";
								} else {
									result = result + "<tr data-parentFeature = '" + featureName.replaceAll(" ", "")
											+ "' id='accordion" + featureName.replaceAll(" ", "")
											+ "' class=\"collapse\"><td style='padding-left: 40px'>" + subfeturedata
											+ "</td>";
								}

//               result = result + "<tr id='accordion" + featureName.replaceAll(" ", "") + "' class=\"collapse\"><td style='padding-left: 40px'>" + subfeturedata + "</td>";
								if (subbasicModel.equalsIgnoreCase("Y")) {
									result = result
											+ "<td><i style=\"color:limegreen\" class=\"fa fa-check\nfa-lg\"></i></td>";
								} else if (subbasicModel.equalsIgnoreCase("N")) {
									result = result
											+ "<td><i style=\"color:red\" class=\"fa fa-times fa-lg\" aria-hidden=\"true\"></i></td>";
								} else {
									result = result + "<td>" + subbasicModel + "</td>\r\n";
								}
								if (substanderedModel.equalsIgnoreCase("Y")) {
									result = result
											+ "<td><i style=\"color:limegreen\" class=\"fa fa-check\nfa-lg\"></i></td>";
								} else if (substanderedModel.equalsIgnoreCase("N")) {
									result = result
											+ "<td><i style=\"color:red\" class=\"fa fa-times fa-lg\" aria-hidden=\"true\"></i></td>";
								} else {
									result = result + "<td>" + substanderedModel + "</td>\r\n";
								}
								if (subEntereprice.equalsIgnoreCase("Y")) {
									result = result
											+ "<td><i style=\"color:limegreen\" class=\"fa fa-check\nfa-lg\"></i></td>";
								} else if (subEntereprice.equalsIgnoreCase("N")) {
									result = result
											+ "<td><i style=\"color:red\" class=\"fa fa-times fa-lg\" aria-hidden=\"true\"></i></td>";
								} else {
									result = result + "<td>" + subEntereprice + "</td>";
								}
								result = result + "</tr>";
							}

							for (int y = 0; y < subfeaturelist.size(); y++) {
								Object[] subfeaturerowData = (Object[]) subfeaturelist.get(y);
								String subsubfeturedata = (String) subfeaturerowData[0];
								String subsubbasicModel = (String) subfeaturerowData[1];
								String subsubstanderedModel = (String) subfeaturerowData[2];
								String subsubEntereprice = (String) subfeaturerowData[3];
								String subsubicon = (String) subfeaturerowData[4];

								if (subicon != null && !"null".equalsIgnoreCase(subicon)
										&& !"".equalsIgnoreCase(subicon)) {
//               result = result + "<tr id='accordion" + featureName.replaceAll(" ", "") + "' class=\"collapse\"><td style='padding-left: 40px'><span class='rowtitle'>" + subfeturedata + "</span><span id='imageid' class='imageClass'><img src='" + subicon + "' width='50px' id='expandImageId' class='VisionImageVisualization'></span></td>";
									result = result + "<tr data-parentFeature = '" + featureName.replaceAll(" ", "")
											+ "' id='accordion" + subfeturedata.replaceAll(" ", "")
											+ "' class=\"collapse\"><td style='padding-left: 40px'><span>"
											+ subsubfeturedata
											+ "</span><span id='imageid' class='imageClass'><img src='" + subsubicon
											+ "' width='50px' id='expandImageId' class='VisionImageVisualization'></span></td>";
								} else {
									result = result + "<tr data-parentFeature = '" + featureName.replaceAll(" ", "")
											+ "' id='accordion" + subfeturedata.replaceAll(" ", "")
											+ "' class=\"collapse\"><td style='padding-left: 40px'>" + subsubfeturedata
											+ "</td>";
								}

//               result = result + "<tr id='accordion" + featureName.replaceAll(" ", "") + "' class=\"collapse\"><td style='padding-left: 40px'>" + subfeturedata + "</td>";
								if (subsubbasicModel.equalsIgnoreCase("Y")) {
									result = result
											+ "<td><i style=\"color:limegreen\" class=\"fa fa-check\nfa-lg\"></i></td>";
								} else if (subsubbasicModel.equalsIgnoreCase("N")) {
									result = result
											+ "<td><i style=\"color:red\" class=\"fa fa-times fa-lg\" aria-hidden=\"true\"></i></td>";
								} else {
									result = result + "<td>" + subsubbasicModel + "</td>\r\n";
								}
								if (subsubstanderedModel.equalsIgnoreCase("Y")) {
									result = result
											+ "<td><i style=\"color:limegreen\" class=\"fa fa-check\nfa-lg\"></i></td>";
								} else if (subsubstanderedModel.equalsIgnoreCase("N")) {
									result = result
											+ "<td><i style=\"color:red\" class=\"fa fa-times fa-lg\" aria-hidden=\"true\"></i></td>";
								} else {
									result = result + "<td>" + subsubstanderedModel + "</td>\r\n";
								}
								if (subsubEntereprice.equalsIgnoreCase("Y")) {
									result = result
											+ "<td><i style=\"color:limegreen\" class=\"fa fa-check\nfa-lg\"></i></td>";
								} else if (subsubEntereprice.equalsIgnoreCase("N")) {
									result = result
											+ "<td><i style=\"color:red\" class=\"fa fa-times fa-lg\" aria-hidden=\"true\"></i></td>";
								} else {
									result = result + "<td>" + subsubEntereprice + "</td>";
								}
								result = result + "</tr>";

							}

						}
					}
				}
				result = result + "</tbody>";
			}
			result = result + "</table>\r\n</div>";

			System.out.println("resultstr::::::::::::::::::" + result);
			resultdata.put("result", result);
			resultdata.put("featureArr", featureArr);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultdata;
	}

	public StringBuilder getinfo(HttpServletRequest request) {
		StringBuilder result = new StringBuilder();
		StringBuilder sideLgfeatures = new StringBuilder();
		try {
			JSONObject resultobj = new JSONObject();
			String tittle = request.getParameter("Lgtype");
			HttpSession currentSession = request.getSession(false);
			if (currentSession != null) {
				currentSession.invalidate();
			}
			request.getSession(true).setAttribute("Lgtype", tittle);
			sideLgfeatures = dashBoardsDAO.getInfoBasicPlan(request, tittle);
			result.append(
					"<div class=\"pageContainer\"><div class=\"containerWrapper\"><div class=\"leftSideContainer\">") // done
					.append("<div class=\"logoHeader\"><img src=\"images/logo_red.png\" alt=\"logo_red\"></div>");
			result.append(sideLgfeatures);
			result.append("</div>");
			result.append("<div class=\"RightContainer\">").append(
					"<div class=\"progress-container\" id=\"progressContainer\"><div class=\"subscriptionHeader\"><h3>"
							+ tittle
							+ " Subscription</h3></div><div class=\"step-progress\"><div class=\"step-wrapper\" id=\"first-wrapper-progress\"><div class=\"step-icon step-contactDetailsImg \"></div><div class=\"step-name\">Contact Info</div></div><div id=\"second-wrapper-progress\" class=\"step-wrapper\"><div class=\"step-icon billingInfo-progress\"></div><div class=\"step-name\">Billing Info</div></div><div id=\"third-wrapper-progress\" class=\"step-wrapper \"><div class=\"step-icon checkout-progress\"></div><div class=\"step-name\">Checkout</div></div></div></div>")
					.append("<div class=\"formWrapper\" id=\"paymentDirectBodyFormPage\">");
			List resultList = dashBoardsDAO.getInfoDynamicHTML(request);
			// som
			int n = 0;
			int d = 0;
			StringBuilder firstDiv = new StringBuilder();
			StringBuilder secondDiv = new StringBuilder();
			StringBuilder thirdDiv = new StringBuilder();
			firstDiv.append(
					"<div class=\"rightContainerFirstMainDiv\"><div class=\"rightContainerMainspan\">Personal Details:</div><div class=\"rightContainerFirstDiv firstRow\" id=\"rightContainerFirstDiv\">");
			secondDiv.append(
					"<div class=\"rightContainerSecondMainDiv\"><div class=\"rightContainerMainspan\">Company Details:</div><div class=\"rightContainerSecondDiv\" id=\"rightContainerSecondDiv\">");
			// thirdDiv.append("<div class=\"rightContainerthirdMainDiv\"><div
			// class=\"rightContainerMainspan\">Doman Details:</div><div
			// class=\"rightContainerthirdDiv\" id=\"rightContainerthirdDiv\">");
			if (resultList != null && !resultList.isEmpty()) {
				for (int i = 0; i < resultList.size(); i++) {
					Object[] rowData = (Object[]) resultList.get(i);
					if (rowData[5].toString().equalsIgnoreCase("F")) {
						StringBuilder resultStr = textFildSetRow(request, rowData);
						firstDiv.append(resultStr);
					} else if (rowData[5].toString().equalsIgnoreCase("S")) {
						n = n + 1;
						d = n % 2;
						StringBuilder resultStr = new StringBuilder();
						String fieldType = (String) rowData[2];
						if (!fieldType.equalsIgnoreCase("H")) {
							if (n == 1) {
								secondDiv.append("<div class=\"firstRow\">");
								resultStr = textFildSetRow(request, rowData);
								secondDiv.append(resultStr);
							} else if (d == 0) {
								resultStr = textFildSetRow(request, rowData);
								secondDiv.append(resultStr);
								secondDiv.append("</div>");
							} else {
								secondDiv.append("<div class=\"firstRow\">");
								resultStr = textFildSetRow(request, rowData);
								secondDiv.append(resultStr);
							}
						} else {
							resultStr = textFildSetRow(request, rowData);
							secondDiv.append(resultStr);
						}
					} else if (rowData[5].toString().equalsIgnoreCase("T")) {
						StringBuilder resultStr = textFildSetRow(request, rowData);
						// thirdDiv.append(resultStr);
					}

				}
				firstDiv.append(
						"</div><div class=\"rightContainerFirstErrorDiv\" id=\"rightContainerFirstErrorDiv\" style=\"display:none\"></div></div>");
				result.append(firstDiv);
				secondDiv.append(
						"</div><div class=\"rightContainerSecondErrorDiv\" id=\"rightContainerSecondErrorDiv\" style=\"display:none\"></div></div>");
				result.append(secondDiv);
				// thirdDiv.append("</div><div class=\"rightContainerthirdErrorDiv\"
				// id=\"rightContainerthirdErrorDiv\" style=\"display:none\"></div></div>");
				// result.append(thirdDiv);
				result.append(
						"</div><div class=\"firstRow downBtnField\"><div class=\"checkboxLevelCLS\"><input type=\"checkbox\" "
								// + "onclick=\"checkFormValidation()\" "
								+ "name=\"paymentFormCheck\" id=\"paymentFormCheck\" required><span class=\"iAgreeCls\">I agree the </span>"
								+ "<a href=\"javascript:void(0)\" onclick='showTermsAndConditions()' style=\"color:blue;margin-top:3px;\"> Terms & Conditions </a>"
								+ "</div>");
				result.append("<div class=\"checkoutBtns\" onclick=\"getNextFeatureInfo('" + tittle
						+ "')\" class=\"nextbtn\" id=\"paymentFormNextPage\"><div>Next</div>");
				result.append("</div></div>");

			}
			result.append("</div></div></div>");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	private StringBuilder textFildSetRow(HttpServletRequest request, Object[] rowData) {
		StringBuilder result = new StringBuilder();
		try {
			String fieldType = (String) rowData[2];
			String colName = (String) rowData[0];
			String colLevel = (String) rowData[1];
			String condFlag = (String) rowData[3];
			String attrValue = (String) rowData[4];
			String colMan = (String) rowData[6];
			// String colMan = "";
			String title = request.getParameter("Lgtype");
			JSONObject resultobj = dashBoardsDAO.getCountryList(request);
			JSONObject subscriptionObj = getSubscriptionType(request);
			List countryList = (List) resultobj.get("dataList");
			StringBuilder cuntryOpation = new StringBuilder();
			StringBuilder cityOpation = new StringBuilder();
//som
			for (int i = 0; i < countryList.size(); i++) {
				Object[] cunData = (Object[]) countryList.get(i);
				String CountryCode = (String) cunData[0];
				String countryname = (String) cunData[1];
				cuntryOpation.append("<option value=\'" + countryname + "\' data-dialcode=\'" + CountryCode + "\' >"
						+ countryname + "</option>");
			}

			StringBuilder currencyLStr = new StringBuilder();
			// currencyLStr.append("<option>INR
			// </option><option>USD</option><option>EURO</option>");
			currencyLStr.append("<option value='USD' selected>USD</option>");
//son
			String btnAttr = "";
			if (rowData[4] != null) {
				btnAttr = attrValue.replace("&&", "('" + title + "')");
			}
			String colManAttr = "";
			String labelMandClass = "";
			if (colMan != null && !"".equalsIgnoreCase(colMan) && !"null".equalsIgnoreCase(colMan)
					&& "M".equalsIgnoreCase(colMan)) {
				colManAttr = "data-req=\"" + colMan + "\"";
				labelMandClass = "class='required_Mand_label'";
			}
			if (fieldType.equalsIgnoreCase("T")) {
				if ("PHONE_NUMBER".equalsIgnoreCase(colName)) {
					result.append(
							"<div class=\"textLevelCLS\"><label " + labelMandClass + " for=\"\">Phone Number</label>")
							.append("<div class=\"PhoneNumberInputs\">")
							.append("<input type=\"text\" class=\"CountryCode\" id=\"dialCode\" >")
							.append("<input type=\"tel\"  pattern=\"^\\d{10}$\" class=\"inputPayment CountryNumber\" name=\"billing_tel\" id=\"CONTACT_PHONE_NO\" required "
									+ colManAttr + "></div></div>");
				} else if ("EMAIL".equalsIgnoreCase(colName)) {
					result.append(
							"<div class=\"textLevelCLS customDiv\" id=\"paymentMailFeild\"><div class=\"EmailFeildForm\" id=\"EmailFeildForm\"><label "
									+ labelMandClass
									+ " for=\"\">Email</label><input data-attr=false name=\"billing_email\" pattern=\"^[_a-zA-Z0-9-]+(\\\\.[_a-zA-Z0-9-]+)*@[a-z0-9-]+(\\\\.[a-z0-9-]+)*(\\\\.[a-z]{2,4})$\\\" class=\"inputPayment\" "
									+ colManAttr + " id=\"CONTACT_MAIL_ID\" type=\"text\"></div>")
							.append("<button id=\"EmailFeildBTN\" onclick=\"getPaymentEmailOtp()\">OTP</button><div style=\"display:none\" id=\"otpVerificationContaniner\" class=\"otpVerificationContaniner\"></div></div>");
				} else {
					result.append("<div class=\"textLevelCLS\"><label " + labelMandClass + " for=\"\">" + colLevel
							+ "</label><input name=\"billing_" + colLevel.toLowerCase() + "\" " + attrValue + " "
							+ colManAttr + "></div>");
				}
			} else if (fieldType.equalsIgnoreCase("L")) {
				if (colLevel.toUpperCase().equalsIgnoreCase("CURRENCY")) {

					result.append("<div class=\"listLevelCLS\"><label " + labelMandClass + " for=\"\">" + colLevel
							+ "</label>")
							.append("<select id=\"CUSTOMER_" + colLevel.toUpperCase() + "\" name=\""
									+ colLevel.toLowerCase() + "\" " + attrValue + " disabled " + colManAttr + ">")
							.append("<option value=\"\" >Select " + colLevel + " </option>");
					result.append(currencyLStr);
					result.append("</select></div>");

				} else if (colName.toUpperCase().equalsIgnoreCase("SUBSCRIPTION_TYPE")) {
					result.append("<div class=\"listLevelCLS\"><label " + labelMandClass + " for=\"\">" + colLevel
							+ "</label>")
							.append("<select id=\"CUSTOMER_" + colName.toUpperCase() + "\" name=\""
									+ colName.toLowerCase() + "\" " + attrValue + " " + colManAttr + ">")
							.append("<option value=\"\" > " + colLevel + " </option>");
					result.append(subscriptionObj.get("SubscriptionType"));
					result.append("</select></div>");
				}
				else if (colName.toUpperCase().equalsIgnoreCase("DEPARTMENT")) {
					result.append("<div class=\"listLevelCLS\"><label " + labelMandClass + " for=\"\">" + colLevel
							+ "</label>")
							.append("<select id=\"CUSTOMER_" + colName.toUpperCase() + "\" name=\""
									+ colName.toLowerCase() + "\" " + attrValue + " " + colManAttr + ">")
							.append("<option value=\"\" > " + colLevel + " </option>");
					result.append(subscriptionObj.get("SubscriptionType"));
					result.append("</select></div>");
				}
				else {
					result.append("<div class=\"listLevelCLS\"><label " + labelMandClass + " for=\"\">" + colLevel
							+ "</label>")
							.append("<select id=\"CUSTOMER_" + colLevel.toUpperCase() + "\" name=\"billing_"
									+ colLevel.toLowerCase() + "\" " + attrValue + " " + colManAttr + ">")
							.append("<option value=\"\" >Select " + colLevel + " </option>");
					if (colLevel.toUpperCase().equalsIgnoreCase("COUNTRY")) {
						result.append(cuntryOpation);
					}
					result.append("</select></div>");
				}
			} else if (fieldType.equalsIgnoreCase("CL")) {
				result.append("<div class=\"checkListLevelCLS\"><label " + labelMandClass
						+ " for=\"\" class=\"LablesStyle\">" + colLevel + "</label><div id=\"jqx"
						+ colLevel.toLowerCase() + "\" type=\"text\"></div></div>");
			} else if (fieldType.equalsIgnoreCase("B")) {
				result.append("<div class=\"checkoutBtns\"><div " + btnAttr + ">Next</div>");
			} else if (fieldType.equalsIgnoreCase("C")) {
				result.append("<div class=\"checkboxLevelCLS\"><input type=\"checkbox\" "
						// + "onclick=\"checkFormValidation()\" "
						+ "name=\"\" id=\"paymentFormCheck\" required>I agree to the "
						+ "<a href=\"javascript:void(0)\" onclick='showTermsAndConditions()' style=\"color:blue;margin-top:3px;\"> Terms & Conditions </a>"
						+ "</div>");
			} else if (fieldType.equalsIgnoreCase("H")) {
				int randomNumber = ThreadLocalRandom.current().nextInt();
				String hiddAttr = "";
				if (rowData[4] != null) {
					HttpSession session = request.getSession(false);
					hiddAttr = attrValue.replace("&&&", "P-" + randomNumber);
					if (session != null && session.getAttribute("order_id") == null) {
						session.setAttribute("order_id", "P-" + randomNumber);
					}
				}
				result.append("<input " + hiddAttr + "/>");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public String getstate(HttpServletRequest request) {
		return dashBoardsDAO.getstate(request);
	}

	public JSONObject getCity(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			StringBuilder result = new StringBuilder();
			result.append("<option value=\"\">Select City</option>");
			List resultList = dashBoardsDAO.getCity(request);
			if (resultList != null && !resultList.isEmpty()) {
				for (int i = 0; i < resultList.size(); i++) {
					String CityList = (String) resultList.get(i);
					result.append("<option >" + CityList + "</option>");
				}
			}
			result.append("<option>Other</option>");
			resultObj.put("City", result);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject addOnpackage(HttpServletRequest request) {
		return dashBoardsDAO.addOnpackage(request);
	}

	public JSONObject getverificationcode(HttpServletRequest request) {
		JSONObject resultobj = new JSONObject();
		try {
			String otpnum = dashBoardsDAO.getverificationcode(request);
			if (otpnum.equalsIgnoreCase("Success")) {
				StringBuilder str = new StringBuilder();
				str.append(" <div class='otpVerifyContainer' id='otpVerfication'>").append(
						"<div class='otpEnterDiv'><input name='OTP' id=\"paymentOTPFeild\" placeholder='Enter OTP'></div>")
						.append("<div id='invalidOtpClearIcon'></div>")
						.append("<div class='otpVerify' onclick=\"getVerifyOTP()\">Verify</div>")
						.append("<div class='RevertbacktEmailDiv' onclick='hideEmailShowOtp()'>Email</div></div>");
				resultobj.put("str", str);
				resultobj.put("status", true);
			} else {
				StringBuilder str = new StringBuilder();
				str.append(
						"<i class=\"fa fa-times\" id=\"dataVerfiedMatch\" data-match=false aria-hidden=\"true\"></i>");
				resultobj.put("str", str);
				resultobj.put("status", false);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultobj;
	}

	public JSONObject getOTPVerificationcode(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String encoderOtp = (String) request.getSession(false).getAttribute("PaymentVerifyOTP");
			byte[] decodedBytes = java.util.Base64.getDecoder().decode(encoderOtp);
			String otp = new String(decodedBytes);
			String val = (String) request.getParameter("val");
			Boolean match;
			if (val.matches(otp)) {
				match = true;
				resultObj.put("str", "<i class=\"fa fa-check\" id=\"dataVerfiedMatch\" data-match=" + match
						+ " aria-hidden=\"true\"></i>");
			} else {
				match = false;
				resultObj.put("str", "<i class=\"fa fa-times\" id=\"dataVerfiedMatch\" data-match=" + match
						+ " aria-hidden=\"true\"></i>");
			}

			resultObj.put("match", match);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getApplyDiscountCode(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = dashBoardsDAO.getApplyDiscountCode(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public String getCcAvenueResponsePageUrl(HttpServletRequest request) {

//       StringBuilder responsePageUrl = new StringBuilder("https://secure.ccavenue.com/transaction.do?");
		StringBuilder responsePageUrl = new StringBuilder();
		StringBuilder ccaRequest = new StringBuilder();
		JSONObject textParam = new JSONObject();
		String orderId = (String) request.getSession(false).getAttribute("order_id");
		Enumeration enumeration = request.getParameterNames();
		while (enumeration.hasMoreElements()) {
			String name = enumeration.nextElement().toString();
			String val = (String) request.getParameter(name);
			textParam.put(name, val);
		}

		try {
			String currency = (String) textParam.get("currency");
			currency = (!(currency != null && !"".equalsIgnoreCase(currency) && !"null".equalsIgnoreCase(currency)))
					? "USD"
					: currency;
			AesCryptUtil aesUtil = new AesCryptUtil();

			if (currency != null && !"".equalsIgnoreCase(currency) && !"null".equalsIgnoreCase(currency)
					&& "INR".equalsIgnoreCase(currency)) {
				responsePageUrl = new StringBuilder("https://test.ccavenue.com/transaction.do?");
				// responsePageUrl = new
				// StringBuilder("https://secure.ccavenue.ae/transaction.do?");
				// responsePageUrl.append("command=initiateTransaction").append("&merchant_id=2165579").append("&access_code=AVQQ29KC84BH82QQHB").append("&encRequest=");
				responsePageUrl.append("command=initiateTransaction").append("&merchant_id=45990")
						.append("&access_code=AVCR05LK19AY26RCYA").append("&encRequest=");
				textParam.remove("merchant_id");
				textParam.remove("subscription type");
				textParam.put("merchant_id", "45990");
				aesUtil = new AesCryptUtil(CCAVENUE_WORKING_KEY_INR);
			} else {
				responsePageUrl = new StringBuilder("https://secure.ccavenue.ae/transaction/transaction.do?");
				/*
				 * responsePageUrl.append("command=initiateTransaction").append(
				 * "&merchant_id=0045990")
				 * .append("&access_code=AVAU04KJ78AL98UALA").append("&encRequest=");
				 */
				responsePageUrl.append("command=initiateTransaction").append("&merchant_id=45990")
						.append("&access_code=AVCR05LK19AY26RCYA").append("&encRequest=");
				textParam.remove("merchant_id");
				textParam.remove("subscription type");
				textParam.put("merchant_id", "45990");
				aesUtil = new AesCryptUtil(CCAVENUE_WORKING_KEY_NON_INR);
			}

			if (aesUtil == null) {
				return "";
			}
			StringBuilder pname = new StringBuilder();
			String pvalue = "";
			/*
			 * while (enumeration.hasMoreElements()) { String enxt =
			 * enumeration.nextElement().toString(); pname.setLength(0); pname.append(enxt);
			 * pvalue = request.getParameter(pname.toString()); if
			 * ("tid".equalsIgnoreCase(pname.toString()) && "".equalsIgnoreCase(pvalue)) {
			 * pvalue = String.valueOf(System.currentTimeMillis() / 1000);// for generating
			 * unique tranaction IDs } if (!pname.toString().equalsIgnoreCase("addON")) {
			 * ccaRequest.append(pname) .append("=") .append(pvalue) .append("&"); } }
			 */
			dashBoardsDAO.compareAndUpdateTextParam(textParam, orderId);
			textParam.keySet().forEach(keyStr -> {
				String keyvalue = (String) textParam.get(keyStr);

				System.out.println("key: " + keyStr + " value: " + keyvalue);
				if ("tid".equalsIgnoreCase(keyStr.toString()) && "".equalsIgnoreCase(keyvalue)) {
					keyvalue = String.valueOf(System.currentTimeMillis() / 1000);
				}

				if (!keyStr.toString().equalsIgnoreCase("addON")) {
					ccaRequest.append(keyStr).append("=").append(keyvalue).append("&");
				}

			});
			String url = retUrl(request);
			System.out.println("redirect_url:" + url);
			ccaRequest.append("redirect_url").append("=").append(url).append("&");
			ccaRequest.append("cancel_url").append("=")
					// .append("http://localhost:8080/integral/getIGinfo?Lgtype=Basic").append("&");
					.append("https://smart.integraldataanalytics.com/getIGinfo?Lgtype=Basic").append("&");
//           StringBuilder ccaRequest1 = new StringBuilder();   
//           ccaRequest1.append("ERP=SmartBI&totalOrginalDisAmount=750000&billing_address=india&language=EN&merchant_id=2165579&billing_lastname=singh&integration_type=iframe_normal&disCouponPercentage=0&billing_tel=7007089689&disCouponAmmount=0&billing_name=som&billing_state=Assam&requestId=MDc3MUY3QzUxNEZBQkZBMUUwNjMwNDAwMDMwQUY3Nzc=&billing_email=jagadish.kumar@piloggroup.com&billing_zip=7845562&currency=INR&tittle=Basic&amount=750000&billing_country=India&billing_city=Chapar&billing_company=pilog&discountCode=&Domain=SmartBI&order_id=P--1324392233&totalOrginalAmount=750000&redirect_url=http://localhost:8080/integral/setInfo?status=MDc3MUY3QzUxNEZBQkZBMUUwNjMwNDAwMDMwQUY3Nzc=&cancel_url=http://localhost:8080/integral/getIGinfo?Lgtype=Basic&");
			StringBuilder encRequest = new StringBuilder(aesUtil.encrypt(ccaRequest.toString()));
			if (encRequest == null || "".equalsIgnoreCase(encRequest.toString())) {
				return "";
			}

			responsePageUrl.append(encRequest);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return responsePageUrl.toString();
	}

	public String retUrl(HttpServletRequest request) {
		String result = "";
		try {
			JSONObject textParam = new JSONObject();
			Enumeration enumeration = request.getParameterNames();
			while (enumeration.hasMoreElements()) {
				String name = enumeration.nextElement().toString();
				String val = (String) request.getParameter(name);
				textParam.put(name, val);
			}
			String text = "Pending";
			request.getSession(false).setAttribute("flag", "I");
			String countryStr = (String) textParam.get("billing_country");
			String titleStr = (String) textParam.get("tittle");
			String country = countryStr.substring(0, 2);
			DateTimeFormatter dateFormate = DateTimeFormatter.ofPattern("ddMMyyyyhh");
			LocalDateTime toDate = LocalDateTime.now();
			String todayDate = dateFormate.format(toDate);
			textParam.put("invoice", " P" + country.toUpperCase() + titleStr.toUpperCase() + todayDate);
			String orgn_id = (String) dashBoardsDAO.saveTransactionDetailsDB(request, textParam, text);
			String encodedString = java.util.Base64.getEncoder().encodeToString(orgn_id.getBytes());
			result = "https://smart.integraldataanalytics.com/setIGInfo?status=" + encodedString + "";
			// result = "https://idxp1.pilogcloud.com/iVisionDXP/setInfo?status=" +
			// encodedString + "";
			// result = "http://localhost:8080/integral/setIGInfo?status=" + encodedString
			// +"";
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public String setInfo(HttpServletRequest request) {
		String result = "";
		try {
			JSONObject textParam = dashBoardsDAO.getDBAllTranstionDetails(request);
			String subject = "";
			String column = "";
			String textPdf = getPdfStr(request, textParam);
			String email = (String) textParam.get("billing_email");
			String status = (String) request.getParameter("status");
			String subscriptionType = (String) textParam.get("subscription_type");
			request.getSession().setAttribute("subscription_type", "subscriptionType");
			byte[] decodedBytes = java.util.Base64.getDecoder().decode(status);
			String decodedString = new String(decodedBytes);

			String orgn_id = textParam.get("requestId").toString();
			String encodedString = java.util.Base64.getEncoder().encodeToString(orgn_id.getBytes());
			textParam.put("requestId", encodedString);
			textParam.put("subscription_type", subscriptionType);

			// String successEmail = dashBoardsDAO.sendEmailText(request, textPdf, subject,
			// email, column, textParam);
//	            String status = (String) request.getParameter("status");
			StringBuilder fieldresult = new StringBuilder();
			fieldresult.append("<div class=\"paymentSuccessfulMaiWrapper\">")
					.append("<div class=\"paymentSuccessfulPageInnerMainDiv\">")
					.append("<div class=\"showStatusImgDiv\">")
					.append("<span class=\"paymentImgClass\"><img src=\"images/successFull.gif\" alt=\"\"></span>")
					.append("<span class=\"paymentTextClass\"><h2>Transaction Successful</h2></span></div> ")
					.append("<div class=\"paymentSucc_Discription_Div\">")
					.append("<div class=\"paymentStatusDis_Div\">")
					.append("<span class=\"greetClass\"><h4>Congratulations! Your subscription process has been successfully completed. </h4></span>")
					.append("<span><h6>Your Order Number is: " + textParam.get("order_id")
							+ ". Please check your email for the payment receipt.</h6></span>")
					.append("<span><h6>Additionally, you will soon receive an email containing your Manager Login Credentials for the SMART INTEGRAPHICS Application for your organization.</h6></span>")
					.append("<span class=\"paymentButtonClass\"></span>")
					.append("</div></div></div></div></div></div>");
			String successDataBaseAdd = "";
			String text = "";
			request.getSession().setAttribute("flag", "U");
			textParam.put("PAYMENT_STATUS", "SUCCESS");
			textParam.put("ORGN_ASSIGNED_STATUS", "PENDING");
			text = "PayementCompleted";
			successDataBaseAdd = dashBoardsDAO.saveTransactionDetailsDB(request, textParam, text);
			if (successDataBaseAdd.contentEquals(decodedString)) {
				String title = textParam.get("tittle").toString();
				StringBuilder lastStr = getSideLastPageData(request, fieldresult, title);
				result = lastStr.toString();
				String orgnId = dashBoardsDAO.createOrgn(request, textParam);
				dashBoardsDAO.createBRole(request, textParam, orgnId);
				RegistrationDTO registrationDTO = setAllToRegistrationDTO(request, textParam);
				textParam.put("orgnId", orgnId);
				registrationDTO.setOrgnId(orgnId);
				dashBoardsDAO.updateNoofUsersForNewSubscription(request, textParam);
				dashBoardsDAO.createNumGeneration(request, textParam, orgnId);
				dashBoardsDAO.updateOrgnSubscriptionDetails(request, textParam);
				JSONObject resultObj = registrationService.registerUser(registrationDTO, request);
				if (resultObj != null && !resultObj.isEmpty() && resultObj.get("Message") != null
						&& !"".equalsIgnoreCase(String.valueOf(resultObj.get("Message")))
						&& String.valueOf(resultObj.get("Message")).contains("Registration Successfully Completed.")) {
					String userName = (String) resultObj.get("userName");
					textParam.put("userName", userName);
					String successEmail = dashBoardsDAO.sendEmailText(request, textPdf, subject, email, column,
							textParam);
					dashBoardsDAO.updatePasswordParamFlagForNewSubscriptedUsers(request, textParam);
				}
			} else {
				result = "<div class=\"errorMsg\">Payment Recipt failed<div>" + fieldresult + "";
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public StringBuilder getSideLastPageData(HttpServletRequest request, StringBuilder text, String tittle) {
		StringBuilder result = new StringBuilder();
		try {
			StringBuilder sideLgfeatures = new StringBuilder();
			JSONObject resultobj = new JSONObject();
			sideLgfeatures = dashBoardsDAO.getInfoBasicPlan(request, tittle);
			result.append(
					"<div class=\"pageContainer\"><div class=\"containerWrapper\"><div class=\"leftSideContainer\">") // done
					.append("<div class=\"logoHeader\"><img src=\"https://www.piloggroup.com/img/header/logo-header.png\" alt=\"&^^&\"></div>");
			result.append(sideLgfeatures);
			result.append("</div>");
			result.append("<div class=\"RightContainer\">").append(
					"<div class=\"progress-container\" id=\"progressContainer\"><div class=\"subscriptionHeader\"><h3>"
							+ tittle
							+ " Subscription</h3></div><div class=\"step-progress\"><div class=\"step-wrapper step-active\"\" id=\"first-wrapper-progress\"><div class=\"step-icon step-contactDetailsImg step-active\"></div><div class=\"step-name\">Contact Info</div></div><div id=\"second-wrapper-progress\" class=\"step-wrapper step-active\"\"><div class=\"step-icon billingInfo-progress step-active\"></div><div class=\"step-name\">Billing Info</div></div><div id=\"third-wrapper-progress\" class=\"step-wrapper step-active\"\"><div class=\"step-icon checkout-progress step-active\"></div><div class=\"step-name\">Checkout</div></div></div></div>")
					.append("<div class=\"formWrapper\" id=\"paymentDirectBodyFormPage\">").append(text);
			result.append("</div></div></div>");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public String getPdfStr(HttpServletRequest request, JSONObject textParam) {
		String result = "";
		try {
			DateTimeFormatter dateFormate = DateTimeFormatter.ofPattern("dd-MM-yyyy");
			SimpleDateFormat formatter = new SimpleDateFormat("dd MMM yyyy");
			Date date = new Date();
			SimpleDateFormat localDateFormate = new SimpleDateFormat("dd MMM yyyy hh:mm:ss");
			LocalDateTime toDate = LocalDateTime.now();
			String amountNum = (String) textParam.get("amount");
			Float amountNumFloat = Float.parseFloat(amountNum);
			DecimalFormat df = new DecimalFormat("0.00");
			df.setMaximumFractionDigits(2);
			String amountStr = df.format(amountNumFloat);
			// String amountStr = convert(parseInt(amountNum));
			String strDate = formatter.format(date);
			result = "<div style=\"border: 1px solid #000; width: 100%;\">" + "<table>" + "<tr>"
					+ "<td  style=\" width: 50%;\">"
					+ "<img width=\"100\" src=\"https://pilogcloud.com/iVisionDXP/images/PiLog_Logo_New.png\" alt=\"...\"/></td>"
					+ "<td style=\"float: right; margin-left: 925px;\"><b>Tax Invoice/Bill of Supply/Cash Memo</b><br/>"
					+ "(Original for Recipient)" + "</td>" + "</tr>" + "</table>"
					+ "<table style=\"margin-top:10px; margin-bottom:10px; padding: 10px; border-bottom: 2px solid #ddd; width: 100%;\">"
					+ "<tr>" + "<td style=\" width: 69%;\">PiLog Cloud Services<br/>" + "Date: "
					+ localDateFormate.format(date) + "" + "</td></tr>" + "</table>"
					+ "<table style=\"margin-top:10px; margin-bottom:10px; padding: 10px; border-bottom: 2px solid #ddd; width: 100%;\">"
					+ "<tbody>" + "<tr>"
					// + "<td style=\"width: 56%;\"><b>Sold By:<br/> </b>PiLog India Private
					// Limited<br/>"
					// + "MJR Magnifique, Rai Durg,<br/>"
					// + "X roads, Nanakaramguda,/1<br/>"
					// + "Hyderabad,Telangana-500008.<br/>"
					// + "IN</td>"
					+ "<td style=\"width: 56%; display:block;\"><b>Billing Address:<br/></b>" + ""
					+ textParam.get("billing_name") + "<br/>" + textParam.get("ORGN_NAME") + "<br/>" + ""
					+ textParam.get("billing_address") + ",<br/>" + "" + textParam.get("billing_city") + ", " + ""
					+ textParam.get("billing_state") + ",<br/> " + textParam.get("billing_zip") + ", " + ""
					+ textParam.get("billing_country") + "</td>"
					// sold by remove
					+ "<td style=\"display:block;\"></td>"
					// sold by remove
					+ "</tr>" + "</tbody>" + "</table>" + "<table style=\"margin-top:10px; margin-bottom:10px;\">"
					+ "<tbody>" + "<tr>" + "<td style=\" width: 49%;\">" + "<b>Order Number: </b>"
					+ textParam.get("order_id") + "<br/>" + "<b>Order Date: </b>" + localDateFormate.format(date)
					+ "<br/>" + "</td>" + "<td>" + "<b>Invoice Number: </b>" + textParam.get("invoice") + "<br/>"
					+ "<b>Invoice Details: </b>IG(" + textParam.get("tittle") + ") <br/>" + "<b>Invoice Date: </b>"
					+ strDate + "<br/>" + "</td>" + "</tr>" + "</tbody>" + "</table>"
					+ "<table border=\"1\" style=\"border-collapse: collapse; width: 100%; margin-top:15px; margin-bottom:10px;\">"
					+ "<thead>" + "<tr>" + "<th style=\"background-color: #f1f1f1; width: 10%;\">#</th>"
					+ "<th colspan=\"2\" style=\"background-color: #f1f1f1; width: 15%;\">Description</th>"
					+ "<th style=\"background-color: #f1f1f1;\">Unit Price</th>"
					+ "<th style=\"background-color: #f1f1f1; width: 10%;\">Qty</th>"
					+ "<th style=\"background-color: #f1f1f1;\">Net Amount</th>"
					// + "<th style=\"background-color: #f1f1f1;\">Tax Rate</th>"
					+ "<th style=\"background-color: #f1f1f1; width: 10%;\">Tax Type</th>"
					+ "<th style=\"background-color: #f1f1f1;\">Tax Amount</th>"
					+ "<th style=\"background-color: #f1f1f1;\">Total Amount</th>" + "</tr>" + "</thead>" + "<tr>"
					+ "<td >1</td>" + "<td colspan=\"2\">" + textParam.get("tittle") + "</td>" + "<td>"
					+ textParam.get("amount") + "</td>" + "<td>1</td>" + "<td>" + textParam.get("amount") + "</td>"
					// + "<td>18%</td>"
					+ "<td>Tax</td>" + "<td>" + textParam.get("amount") + "</td>" + "<td>" + textParam.get("amount")
					+ "</td>" + "</tr>" + "<tr>" + "<td colspan=\"7\"><b>TOTAL:</b></td>"
					+ "<td style=\"background-color: #f1f1f1;\">" + textParam.get("amount") + "</td>"
					+ "<td style=\"background-color: #f1f1f1;\">" + textParam.get("amount") + "</td>" + "</tr>" + "<tr>"
					+ "<td colspan=\"9\" style=\"text-align: inherit; padding: 10px 1px;\">" + "<b>Amount in Words:</b>"
					+ "" + amountStr.toUpperCase() + " Rupees" + "</td>" + "</tr>" + "<tr>"
					+ "<td colspan=\"9\" style=\"text-align: right; padding: 10px 25px;\">"
					+ "<b>PiLog Cloud Services</b>" + "</td>" + "</tr>" + "</table>" + "<table>"
					+ "<tr><td style=\"width:65%\"></td>" + "<td><small>PiLog Cloud Services<sup>TM</sup></small></td>"
					+ "</tr>" + "</table>" + "</div>";
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public static String convert(long number) {
		// 0 to 999 999 999 999
		if (number == 0) {
			return "zero";
		}

		String snumber = Long.toString(number);

		// pad with "0"
		String mask = "000000000000";
		DecimalFormat df = new DecimalFormat(mask);
		snumber = df.format(number);

		// XXXnnnnnnnnn
		int billions = Integer.parseInt(snumber.substring(0, 3));
		// nnnXXXnnnnnn
		int millions = Integer.parseInt(snumber.substring(3, 6));
		// nnnnnnXXXnnn
		int hundredThousands = Integer.parseInt(snumber.substring(6, 9));
		// nnnnnnnnnXXX
		int thousands = Integer.parseInt(snumber.substring(9, 12));

		String tradBillions;
		switch (billions) {
		case 0:
			tradBillions = "";
			break;
		case 1:
			tradBillions = convertLessThanOneThousand(billions) + " billion ";
			break;
		default:
			tradBillions = convertLessThanOneThousand(billions) + " billion ";
		}
		String result = tradBillions;

		String tradMillions;
		switch (millions) {
		case 0:
			tradMillions = "";
			break;
		case 1:
			tradMillions = convertLessThanOneThousand(millions) + " million ";
			break;
		default:
			tradMillions = convertLessThanOneThousand(millions) + " million ";
		}
		result = result + tradMillions;

		String tradHundredThousands;
		switch (hundredThousands) {
		case 0:
			tradHundredThousands = "";
			break;
		case 1:
			tradHundredThousands = "one thousand ";
			break;
		default:
			tradHundredThousands = convertLessThanOneThousand(hundredThousands) + " thousand ";
		}
		result = result + tradHundredThousands;

		String tradThousand;
		tradThousand = convertLessThanOneThousand(thousands);
		result = result + tradThousand;

		// remove extra spaces!
		return result.replaceAll("^\\s+", "").replaceAll("\\b\\s{2,}\\b", " ");
	}

	private static String convertLessThanOneThousand(int number) {
		String soFar;

		if (number % 100 < 20) {
			soFar = numNames[number % 100];
			number /= 100;
		} else {
			soFar = numNames[number % 10];
			number /= 10;

			soFar = tensNames[number % 10] + soFar;
			number /= 10;
		}
		if (number == 0) {
			return soFar;
		}
		return numNames[number] + " hundred" + soFar;
	}

	public StringBuilder getCandleSticktProperties(String ChartType) {
		StringBuilder candlestickProperties = new StringBuilder();

		candlestickProperties.append("<li class=\"general-filters active-filter\" data-column-name=\"" + ChartType
				+ "HOVERLABELDATA\" data-key-type=\"data\">" + "<div class=\"sub-filterItems\">"
				+ "<p>Hover Data Visible</p>" + "<select name=\"text-info\" id=\"" + ChartType
				+ "HOVERLABELDATA\" data-opt-name=\"hoverinfo\" >" + "<option value=\"x\">Label</option>"
				+ "<option value=\"y\">Value</option>" + "<option value=\"%\">Percentage</option>"
				+ "<option value=\"x+y\" selected>Label and value</option>"
				+ "<option value=\"x+%\">Label and Percentage</option>"
				+ "<option value=\"y+%\">Value and Percentage</option>" + "</select>" + "</div>" + "</li>");
		candlestickProperties.append(getChartHover(ChartType, "data"));

		return candlestickProperties;
	}

	public RegistrationDTO setAllToRegistrationDTO(HttpServletRequest request, JSONObject basicData) {
		RegistrationDTO registrationDTO = new RegistrationDTO();
		String userFirstName = "";
		try {
			String role = "ADMIN";
			basicData.put("add_role", role);

			String userNameReq = (String) basicData.get("billing_name") + "_"
					+ (String) basicData.get("billing_lastname");
			if (userNameReq != null && !"".equalsIgnoreCase(userNameReq) && !"null".equalsIgnoreCase(userNameReq)
					&& userNameReq.contains("_")) {
				String[] detalisStringArr = userNameReq.split("_");
				userFirstName = detalisStringArr[0];
				userNameReq = userNameReq.toUpperCase();
			}
			String password = "P@ssw0rd";
			basicData.put("confirm_password", password);
			basicData.put("rsUsername", userNameReq);
			registrationDTO.setAdditional_role(
					(String) basicData.get("add_role") != null ? (String) basicData.get("add_role") : "");
			registrationDTO.setAddress1(
					(String) basicData.get("billing_city") != null ? (String) basicData.get("billing_city") : "");
			registrationDTO.setAddress2(
					(String) basicData.get("billing_state") != null ? (String) basicData.get("billing_state") : "");
			registrationDTO.setConfirm_password(
					(String) basicData.get("confirm_password") != null ? (String) basicData.get("confirm_password")
							: "");
			registrationDTO.setCountry(
					(String) basicData.get("billing_country") != null ? (String) basicData.get("billing_country") : "");
			registrationDTO.setEmail_id(
					(String) basicData.get("billing_email") != null ? (String) basicData.get("billing_email") : "");
			registrationDTO.setExperience_summary(
					(String) basicData.get("billing_company") != null ? (String) basicData.get("billing_company") : "");
			registrationDTO.setFirst_name(userFirstName);

			registrationDTO.setLast_name(
					(String) basicData.get("billing_lastname") != null ? (String) basicData.get("billing_lastname")
							: "");
			registrationDTO
					.setLocale((String) basicData.get("locale") != null ? (String) basicData.get("locale") : "en_US  ");
			registrationDTO.setMobile_number(
					(String) basicData.get("billing_tel") != null ? (String) basicData.get("billing_tel") : "");
			registrationDTO.setMiddle_name((String) basicData.get("age") != null ? (String) basicData.get("age") : "");
			registrationDTO.setMonth((String) basicData.get("month") != null ? (String) basicData.get("month") : "");
			registrationDTO
					.setNick_name((String) basicData.get("jobtitle") != null ? (String) basicData.get("jobtitle") : "");
			registrationDTO.setPassword(password);
			registrationDTO
					.setDate((String) basicData.get("dob") != null ? (String) basicData.get("dob") : "01-01-1958");
			registrationDTO.setDate_of_birth(
					(String) basicData.get("dob") != null ? (String) basicData.get("dob") : "01-01-2000");

			registrationDTO.setPhone_number(
					(String) basicData.get("billing_zip") != null ? (String) basicData.get("billing_zip") : "");
			registrationDTO
					.setPlant((String) basicData.get("plant") != null ? (String) basicData.get("plant") : "1000");
			registrationDTO.setInstance(
					(String) basicData.get("instance") != null ? (String) basicData.get("instance") : "100");
			registrationDTO.setReport_to(
					(String) basicData.get("report_to") != null ? (String) basicData.get("report_to") : "MM_MANAGER");
			registrationDTO.setRole(role);
			// registrationDTO.setUser_name((String) basicData.get("user_name") != null ?
			// (String) basicData.get("user_name") : "");
			registrationDTO.setUser_name(
					(String) basicData.get("rsUsername") != null ? (String) basicData.get("rsUsername") : "");
			registrationDTO.setYear((String) basicData.get("year") != null ? (String) basicData.get("year") : "");
			registrationDTO.setFilepath(request.getParameter("filepath"));
			registrationDTO.setOrgName(
					(String) basicData.get("ORGN_NAME") != null ? (String) basicData.get("ORGN_NAME") : "SmartBI");
			registrationDTO.setPurposeofReg(
					(String) basicData.get("gender") != null ? (String) basicData.get("gender") : "MALE");
			registrationDTO.setDefaultUserFlag("Y");
			registrationDTO.setUserDepartment(
					(String) basicData.get("billing_department") != null ? (String) basicData.get("billing_department")
							: "");
			// usr_orgid
		} catch (Exception e) {
			logger.error(e.getLocalizedMessage());
		}

		return registrationDTO;
	}

	public JSONObject checkSubscriptedMailExists(HttpServletRequest request) {
		return dashBoardsDAO.checkSubscriptedMailExists(request);
	}

	public JSONObject checkForCompanyAlreadyExist(HttpServletRequest request) {
		return dashBoardsDAO.checkForCompanyAlreadyExist(request);
	}

	public JSONObject fetchCardFromQuestion(HttpServletRequest request) {
		return dashBoardsDAO.fetchCardFromQuestion(request);
	}

	public String classAllocationAPI(HttpServletRequest request) {
		return dashBoardsDAO.classAllocationAPI(request);
	}

	public String dataProfilingResponse(HttpServletRequest request) {
		String resultStr = "";
		try {
			String tableName = request.getParameter("tableName");
			String colsArrayStr = request.getParameter("colsArray");
			String url = "http://apihub.pilogcloud.com:6650/profiling";

			// Extracting host, port, and service name from JDBC URL
			URI uri = new URI(dbURL.substring(dbURL.indexOf("@") + 2));
			String[] dbUrl = uri.getPath().split("/");
			String[] hostname = dbUrl[1].split(":");
			String host = hostname[0];
			String portNumber = hostname[1];
			String serviceName = dbUrl[2];

			JSONObject dbDetailsFromDB = dashBoardsDAO.getDBDetailsFromDB(request);
			String tempdataBaseDriver = dataBaseDriver, tempuserName = userName, tempdbURL = dbURL,
					temppassword = password;
			if (dbDetailsFromDB != null && !dbDetailsFromDB.isEmpty()) {
				tempdataBaseDriver = (String) dbDetailsFromDB.getOrDefault("dataBaseDriver", dataBaseDriver);
				tempuserName = (String) dbDetailsFromDB.getOrDefault("userName", userName);
				tempdbURL = (String) dbDetailsFromDB.getOrDefault("url", dbURL);
				temppassword = (String) dbDetailsFromDB.getOrDefault("password", password);

			}
			JSONObject dbDetails = new PilogUtilities().getDatabaseDetails(tempdataBaseDriver, tempdbURL, tempuserName,
					temppassword, "DH11024");

			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

			MultiValueMap<String, String> dataMap = new LinkedMultiValueMap();
			JSONArray colsArray = new JSONArray();
			if (colsArrayStr != null && !"".equals(colsArrayStr)) {
				// Parsing JSONArray from colsArrayStr
				colsArray = (JSONArray) JSONValue.parse(colsArrayStr);
				// Manipulating colsArray or performing any necessary operations
				colsArrayStr = colsArray.stream().filter(e -> e != null).map(String::valueOf)
						.collect(Collectors.joining(",")).toString();
			}

			dataMap.add("tableName", tableName);
			dataMap.add("colsArray", colsArrayStr);

			dataMap.add("accessName", (String) dbDetails.get("CONN_DB_NAME"));
			dataMap.add("USER_NAME", tempuserName);
			dataMap.add("PASSWORD", temppassword);
			dataMap.add("HOST", (String) dbDetails.get("HOST_NAME"));
			dataMap.add("PORT", (String) dbDetails.get("CONN_PORT"));
			dataMap.add("SERVICE_NAME", (String) dbDetails.get("CONN_DB_NAME"));

			HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<MultiValueMap<String, String>>(dataMap,
					headers);

			RestTemplate template = new RestTemplate();
			ResponseEntity<JSONObject> response = template.postForEntity(url, entity, JSONObject.class);
			JSONObject apiDataObj = response.getBody();
			for (Object key : apiDataObj.keySet()) {
				resultStr = apiDataObj.get(key).toString();

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultStr;
	}

	public JSONObject getDHAResponse(HttpServletRequest request) {
		String resultStr = "";
		JSONObject resultObj = new JSONObject();
		try {
			String tableName = request.getParameter("tableName");
			String batchId = request.getParameter("batchId");
			String colsArrayStr = request.getParameter("colsArray");
			String url = "http://apihub.pilogcloud.com:6651/DHA";
			URI uri = new URI(dbURL.substring(dbURL.indexOf("@") + 2));
			String[] dbUrl = uri.getPath().split("/");
			String[] hostname = dbUrl[1].split(":");
			String host = hostname[0];
			String portNumber = hostname[1];
			String serviceName = dbUrl[2];
			String base64PdfContent = "";

			JSONObject dbDetailsFromDB = dashBoardsDAO.getDBDetailsFromDB(request);
			String tempdataBaseDriver = dataBaseDriver, tempuserName = userName, tempdbURL = dbURL,
					temppassword = password;
			if (dbDetailsFromDB != null && !dbDetailsFromDB.isEmpty()) {
				tempdataBaseDriver = (String) dbDetailsFromDB.getOrDefault("dataBaseDriver", dataBaseDriver);
				tempuserName = (String) dbDetailsFromDB.getOrDefault("userName", userName);
				tempdbURL = (String) dbDetailsFromDB.getOrDefault("url", dbURL);
				temppassword = (String) dbDetailsFromDB.getOrDefault("password", password);

			}
			JSONObject dbDetails = new PilogUtilities().getDatabaseDetails(tempdataBaseDriver, tempdbURL, tempuserName,
					temppassword, "DH11024");

			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

			MultiValueMap<String, String> dataMap = new LinkedMultiValueMap<>();
			JSONArray colsArray = new JSONArray();
			if (colsArrayStr != null && !"".equals(colsArrayStr)) {
				// Parsing JSONArray from colsArrayStr
				colsArray = (JSONArray) JSONValue.parse(colsArrayStr);
				// Manipulating colsArray or performing any necessary operations
				colsArrayStr = colsArray.stream().filter(e -> e != null).map(String::valueOf)
						.collect(Collectors.joining(",")).toString();
			}

			// Adding data to dataMap
			dataMap.add("colsArray", colsArrayStr);
			dataMap.add("tableName", tableName);
			dataMap.add("BATCH_ID", batchId);
			dataMap.add("analysisType", "DHA");

			dataMap.add("accessName", (String) dbDetails.get("CONN_DB_NAME"));
			dataMap.add("USER_NAME", tempuserName);
			dataMap.add("PASSWORD", temppassword);
			dataMap.add("HOST", (String) dbDetails.get("HOST_NAME"));
			dataMap.add("PORT", (String) dbDetails.get("CONN_PORT"));
			dataMap.add("SERVICE_NAME", (String) dbDetails.get("CONN_DB_NAME"));

			HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<MultiValueMap<String, String>>(dataMap,
					headers);

			RestTemplate template = new RestTemplate();
			ResponseEntity<byte[]> response = template.postForEntity(url, entity, byte[].class);
			byte[] pdfContent = response.getBody();
			// Assuming pdfContent is a byte array containing PDF content
			base64PdfContent = java.util.Base64.getEncoder().encodeToString(pdfContent);

			resultStr = "<html><head>"
					+ "<script type='text/javascript' src='https://cdnjs.cloudflare.com/ajax/libs/pdf.js/2.11.338/pdf.js'></script>"
					+ "</head><body>" + "<div id='viewer'></div>" + "<script>" + "var pdfData = atob('"
					+ base64PdfContent + "');" + "var pdfjsLib = window['pdfjs-dist/build/pdf'];"
					+ "pdfjsLib.GlobalWorkerOptions.workerSrc = 'https://cdnjs.cloudflare.com/ajax/libs/pdf.js/2.11.338/pdf.worker.js';"
					+ "var loadingTask = pdfjsLib.getDocument({ data: pdfData });"
					+ "loadingTask.promise.then(function(pdf) {" + "  var numPages = pdf.numPages;"
					+ "  var viewer = document.getElementById('viewer');"
					+ "  for (var pageNum = 1; pageNum <= numPages; pageNum++) {"
					+ "    pdf.getPage(pageNum).then(function(page) {"
					+ "      var canvas = document.createElement('canvas');" + "      viewer.appendChild(canvas);"
					+ "      var scale = 1.5;" + "      var viewport = page.getViewport({ scale: scale });"
					+ "      var context = canvas.getContext('2d');" + "      canvas.height = viewport.height;"
					+ "      canvas.width = viewport.width;" + "      var renderContext = {"
					+ "        canvasContext: context," + "        viewport: viewport" + "      };"
					+ "      page.render(renderContext);" + "    });" + "  }" + "});" + "</script></body></html>";
			resultObj.put("base64PdfContent", base64PdfContent);
			resultObj.put("resultStr", resultStr);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public String dataEnrichmentAPI(HttpServletRequest request) {
		return dashBoardsDAO.dataEnrichmentAPI(request);
	}

	public JSONObject getStraticsData(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String type = request.getParameter("type");
			if (!"null".equalsIgnoreCase(type) && !"".equalsIgnoreCase(type) && type.equalsIgnoreCase("Class")) {
				resultObj = dashBoardsDAO.getStraticsData(request);
			} else if (!"null".equalsIgnoreCase(type) && !"".equalsIgnoreCase(type) && type.equalsIgnoreCase("Char")) {
				resultObj = dashBoardsDAO.getStraticsCharData(request);
			}
			if (!"null".equalsIgnoreCase(type) && !"".equalsIgnoreCase(type) && type.equalsIgnoreCase("Reference")) {
				resultObj = dashBoardsDAO.getStraticsRefDocData(request);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONArray getBatchId(HttpServletRequest request) throws Exception {
		return dashBoardsDAO.getBatchId(request);

	}

	public void chartInPDF(String imageStr, HttpServletResponse response, ModelMap model, HttpServletRequest request,
			String filename) throws IOException {
		OutputStream os = null;
		com.itextpdf.text.Document document = null;

		Resource textFontResource = resourceLoader.getResource("classpath:/static/fonts/NotoSansCJKjp-Regular.otf");
		File textFontFile = textFontResource.getFile();

		Resource pilogLogoResource = resourceLoader.getResource("classpath:/static/images/PiLog_Logo_New.png");
		File pilogLogoFile = pilogLogoResource.getFile();

		String textFontFilePath = textFontFile.getAbsolutePath();
		String pilogLogoImage = pilogLogoFile.getAbsolutePath();

//		String textFontFilePath = request.getServletContext().getRealPath("/fonts/NotoSansCJKjp-Regular.otf");
//		String pilogLogoImage = request.getServletContext().getRealPath("/images/PiLog_Logo_New.png");
		BaseFont customBaseFont = null;

		try {
			customBaseFont = BaseFont.createFont(textFontFilePath, BaseFont.IDENTITY_H, BaseFont.NOT_EMBEDDED);
			String analysisType = request.getParameter("analysisType");
			String gridId = request.getParameter("gridId");

			response.reset();
			File filelocation = new File("C:/tempfiles");
			if (!filelocation.exists()) {
				filelocation.mkdir();
			}
			String filepath = filelocation.getAbsolutePath() + File.separator + filename;
			response.setContentType("application/pdf");
			response.setCharacterEncoding("UTF-8");
			os = response.getOutputStream();
			if (!(analysisType != null && !"".equalsIgnoreCase(analysisType)
					&& !"null".equalsIgnoreCase(analysisType))) {
				document = new com.itextpdf.text.Document(PageSize.A4, 0, 0, 0, 0);
			} else {
				document = new com.itextpdf.text.Document(PageSize.A4, 36, 36, 50, 40);
			}
			Rectangle rectangle = new Rectangle(30, 30, 550, 800);
			PdfWriter writer = PdfWriter.getInstance(document, new FileOutputStream(filepath));
			writer.setBoxSize("rectangle", rectangle);
			Rectangle rect = writer.getBoxSize("rectangle");
//            System.out.println("rect::" + rect);

			document.open();
			/*
			 * Rectangle borderRectangle = new Rectangle(30, 30, 559, 795);
			 * borderRectangle.enableBorderSide(1);
			 * borderRectangle.setBorder(Rectangle.BOX); borderRectangle.setBorderWidth(2);
			 * borderRectangle.setBorderColor(BaseColor.BLACK);
			 * document.add(borderRectangle);
			 */ // for border
			com.pilog.mdm.Service.HeaderAndFooterPdfPageEventHelper headerAndFooter = new com.pilog.mdm.Service.HeaderAndFooterPdfPageEventHelper(
					request, writer, document, resourceLoader);
			writer.setPageEvent(headerAndFooter);
			String logoImageString = request.getParameter("pilogLogoString");
//            Image image = getImage(logoImageString);

			Image image = Image.getInstance(pilogLogoImage);
			image.scalePercent(80);
			image.setAlignment(com.itextpdf.text.Element.ALIGN_CENTER);
			// image.setAligement(com.itextpdf.text.Element.)
			image.setAbsolutePosition(170f, 420f);
//             document.add(new Paragraph(Chunk.NEWLINE));
//            document.add(new Paragraph(Chunk.NEWLINE));
//              document.add(new Paragraph(Chunk.NEWLINE));
//            document.add(new Paragraph(Chunk.NEWLINE));
//              document.add(new Paragraph(Chunk.NEWLINE));
//            document.add(new Paragraph(Chunk.NEWLINE));
//              document.add(new Paragraph(Chunk.NEWLINE));
//            document.add(new Paragraph(Chunk.NEWLINE));
//               document.add(new Paragraph(Chunk.NEWLINE));
//              document.add(new Paragraph(Chunk.NEWLINE));
//            document.add(new Paragraph(Chunk.NEWLINE));
			document.add(image);
			document.add(new Paragraph(Chunk.NEWLINE));
			document.add(new Paragraph(Chunk.NEWLINE));
			document.add(new Paragraph(Chunk.NEWLINE));

//            document.add(new Paragraph(Chunk.NEXTPAGE));
			if (analysisType != null && !"".equalsIgnoreCase(analysisType) && !"null".equalsIgnoreCase(analysisType)
					&& !"ASSET CRITICALITY ASSESSMENT".equalsIgnoreCase(analysisType)) {
				Paragraph firstPagetext1 = new Paragraph("\n\n\n\n\n\n\n\n\n\n\n " + analysisType + " Analysis\n",
						new Font(customBaseFont, 20, Font.BOLD, BaseColor.BLACK));
				Paragraph firstPagetext2 = new Paragraph("for\n",
						new Font(customBaseFont, 12, Font.BOLD, BaseColor.BLACK));
				Paragraph firstPagetext3 = new Paragraph("Material Criticality Assessment\n(Inventory Management)",
						new Font(customBaseFont, 20, Font.BOLD, BaseColor.BLACK));
				firstPagetext1.setAlignment(com.itextpdf.text.Element.ALIGN_CENTER);
				document.add(firstPagetext1);
				firstPagetext2.setAlignment(com.itextpdf.text.Element.ALIGN_CENTER);
				document.add(firstPagetext2);
				firstPagetext3.setAlignment(com.itextpdf.text.Element.ALIGN_CENTER);
				document.add(firstPagetext3);
				document.add(new Paragraph(Chunk.NEXTPAGE));
			} else {
				Paragraph firstPagetext3 = new Paragraph("Equipment Criticality Assessment\n(Inventory Management)",
						new Font(customBaseFont, 20, Font.BOLD, BaseColor.BLACK));
				firstPagetext3.setAlignment(com.itextpdf.text.Element.ALIGN_CENTER);
				document.add(firstPagetext3);
				document.add(new Paragraph(Chunk.NEXTPAGE));
			}

			if (imageStr != null) {
				if (analysisType != null && !"".equalsIgnoreCase(analysisType) && !"null".equalsIgnoreCase(analysisType)
						&& "ABC".equalsIgnoreCase(analysisType)) {
//                    abcPdfData(imageStr, request, document, analysisType);
					abcPdfData(imageStr, request, document, analysisType, customBaseFont);
				} else if (analysisType != null && !"".equalsIgnoreCase(analysisType)
						&& !"null".equalsIgnoreCase(analysisType) && "XYZ".equalsIgnoreCase(analysisType)) {
					xyzPdfData(imageStr, request, document, analysisType, customBaseFont);
				} else if (analysisType != null && !"".equalsIgnoreCase(analysisType)
						&& !"null".equalsIgnoreCase(analysisType) && "FMSN".equalsIgnoreCase(analysisType)) {
					fmsnPdfData(imageStr, request, document, analysisType, customBaseFont);
				} else if (analysisType != null && !"".equalsIgnoreCase(analysisType)
						&& !"null".equalsIgnoreCase(analysisType) && "HML".equalsIgnoreCase(analysisType)) {
					hmlPdfData(imageStr, request, document, analysisType, customBaseFont);
				} else if (analysisType != null && !"".equalsIgnoreCase(analysisType)
						&& !"null".equalsIgnoreCase(analysisType) && "VED".equalsIgnoreCase(analysisType)) {
					vedPdfData(imageStr, request, document, analysisType, customBaseFont);
				} else if (analysisType != null && !"".equalsIgnoreCase(analysisType)
						&& !"null".equalsIgnoreCase(analysisType) && "SDE".equalsIgnoreCase(analysisType)) {
					sdePdfData(imageStr, request, document, analysisType, customBaseFont);
				} else if (analysisType != null && !"".equalsIgnoreCase(analysisType)
						&& !"null".equalsIgnoreCase(analysisType)
						&& "ASSET CRITICALITY ASSESSMENT".equalsIgnoreCase(analysisType)) {
					acaPdfData(imageStr, request, document, analysisType, customBaseFont);
				}

				else {
					defaultPdfData(imageStr, request, document);
				}
				writer.close();
				InputStream fis = new FileInputStream(new File(filepath));
				String mimeType = request.getServletContext().getMimeType(filename);
				response.setContentType(mimeType != null ? mimeType : "application/octet-stream");
				response.setHeader("Content-disposition", "attachment; filename=\"" + filename + "\"");
				byte[] bufferData = new byte[1024];
				int read = 0;

				while ((read = fis.read(bufferData)) != -1) {
					os.write(bufferData, 0, read);
				}
				os.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (os != null) {
					os.close();
				}
				if (document != null && document.isOpen()) {
					document.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

	public void abcPdfData(String imageStr, HttpServletRequest request, com.itextpdf.text.Document document,
			String analysisType, BaseFont customBaseFont) {
		try {
			BaseColor backgroundBaseColor = new BaseColor(64, 135, 94);// green
//            BaseColor backgroundBaseColor=new BaseColor(0, 113, 197);//blue
			PdfPCell cell = new PdfPCell();
			Paragraph paragraph = new Paragraph("Hello World");
			cell.addElement(paragraph);
			cell.setBackgroundColor(BaseColor.RED);
//            Font f = new Font(customBaseFont, 16.0f, Font.BOLD, BaseColor.WHITE);

			Chunk firstMainHeaddingChunk = new Chunk("1.Introduction :",
					new Font(customBaseFont, 16.0f, Font.BOLD, BaseColor.WHITE));
			firstMainHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 403, 3);
			Paragraph firstMainHeadding = new Paragraph(firstMainHeaddingChunk);
			firstMainHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(firstMainHeadding);// onkar 168, 162, 64
			document.add(cell);
			document.add(new Paragraph(Chunk.NEWLINE));
			// onkar color
			Chunk firstSubHeaddingChunk = new Chunk("Material Criticality Analysis",
					new Font(customBaseFont, 14.0f, Font.BOLD, BaseColor.WHITE));
			firstSubHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 343, 3);
			// color
			Paragraph firstSubHeadding = new Paragraph(firstSubHeaddingChunk);
//            Paragraph firstSubHeadding = new Paragraph("Material Criticality Analysis\n", new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLACK));
			firstSubHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(firstSubHeadding);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph firstMainHeaddingData = new Paragraph();
			firstMainHeaddingData.add(new Paragraph(
					"   Material Criticality and Inventory Analysis is performed in order to determine the criticality, consumption value, consumption rate, stock value, "
							+ "lead time, price of single unit & frequency Inventory management is the ongoing process of moving parts and products into and out of a company’s location(s). Companies manage "
							+ "their inventory on a daily basis as they place new orders for products and ship orders out to customers. It’s important that business leaders gain a firm grasp of everything "
							+ "involved in the inventory management process. That way, they can figure out creative ways.\n"
							+ "This module allows the users to easily view inventory movement, usage and trends. Users can analyze inventory sales over a specified time frame and make decisions on how to best "
							+ "adjust item resource planning values based on sales averages and months availability. The process of understanding the moving parts & products combined with the knowledge of the "
							+ "demand for stock/product. It is the technique to determine the optimum level of inventory for a firm\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(firstMainHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph firstSmallSubHeadingData = new Paragraph(
					"The following are the various methods to control the inventory process:",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
//            firstSmallSubHeadingData.setAlignment(Element.ALIGN_LEFT);
			document.add(firstSmallSubHeadingData);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings1 = new Paragraph();
			Font zapfdingbats = new Font();
			Chunk bullet = new Chunk("\u2022", zapfdingbats);
			bulletMainHeaddings1.add(bullet);
			bulletMainHeaddings1.add(new Phrase(" ABC Analysis - Based on consumption value per year\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings1);
//            document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletSubHeadding1 = new Paragraph(
					"        It classifies the materials based on their consumption during a particular   time period (usually one year)",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding1);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings2 = new Paragraph();
			bulletMainHeaddings2.add(bullet);
			bulletMainHeaddings2.add(new Phrase(" FMSN Analysis - Based on consumption rate\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings2);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding2 = new Paragraph(
					"        It is based on the rate of issue or rate of usage of spare parts. This classification system categorizes the items based on how frequently"
							+ " the parts are issued and how frequently they are used",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding2);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings3 = new Paragraph();
			bulletMainHeaddings3.add(bullet);
			bulletMainHeaddings3.add(new Phrase(" XYZ Analysis - Based on stock value accumulation\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings3);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding3 = new Paragraph(
					"        It classifies the materials based on stock value accumulation. It is calculated by dividing an item's current stock value by the "
							+ "total stock value of the stores.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding3);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings4 = new Paragraph();
			bulletMainHeaddings4.add(bullet);
			bulletMainHeaddings4.add(new Phrase(" VED Analysis - Based on criticality & impact\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings4);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding4 = new Paragraph(
					"        It classifies the materials according to their criticality and impact to the production process or other services i.e. how and to what extent the material "
							+ "M1 is going to effect the production if the material M1 is not available.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding4);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings5 = new Paragraph();
			bulletMainHeaddings5.add(bullet);
			bulletMainHeaddings5.add(new Phrase(" HML Analysis - Based on unit cost of material\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings5);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding5 = new Paragraph(
					"        It classifies the materials based on their unit prices. The main objective of this analysis is to minimize the inventory cost such as "
							+ "labor & material cost etc.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding5);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings6 = new Paragraph();
			bulletMainHeaddings6.add(bullet);
			bulletMainHeaddings6.add(new Phrase(" SDE Analysis: Based on Lead Time & Availability of Items\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings6);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding6 = new Paragraph(
					"        It classifies inventory based on how freely available an item or scarce an item is, or the length of its lead time.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding6);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk secondMainHeaddingChunk = new Chunk("2. ABC Analysis\n",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.WHITE));
//            c.setBackground(backgroundBaseColor);
			secondMainHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 407, 3);
			Paragraph secondMainHeadding = new Paragraph(secondMainHeaddingChunk);
			secondMainHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondMainHeadding);
			document.add(new Paragraph(Chunk.NEWLINE));
			// bg color
			Chunk secondSubHeaddingChunk = new Chunk("Overview of ABC Analysis:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			secondSubHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 350, 3);
			Paragraph secondSubHeadding = new Paragraph(secondSubHeaddingChunk);
			secondSubHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondSubHeadding);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph secondMainHeaddingData = new Paragraph();
			secondMainHeaddingData.add(new Paragraph(
					"    Always Better Control (ABC) approach is a popular and effective method used to classify inventory items into specific categories that can "
							+ "be managed and controlled separately. This method aims to draw attention on the critical (A-items) not on the trivial many (C-items).\n"
							+ "ABC analysis is one of the most important approaches of stock-control. Wilfredo Pareto discovered ABC analysis and is recognized today as Pareto's concept. When ABC analysis is applied "
							+ "to an inventory it determines the Importance of items and the level of controls placed on the same item with its characteristic value.\n"
							+ "By dividing a company’s inventory into different classifications such as A, B & C these can focus on the items that the majority of the inventory.\n"
							+ "ABC analysis is an inventory categorization technique which determines the relative value of a group of inventory items based on a user-specified valuation criterion.\n"
							+ "Using ABC analysis you can identify items that will have a significant impact on overall inventory cost, while also identifying different categories of stock that will require "
							+ "different management and controls.\n"
							+ "ABC refers to the ranking you assign to items in order of their estimated importance suggesting that inventories of an organization are not of equal value such as items classified as:\n"
							+ "A - items are very important for an organization and are very tightly controlled and accurate records are maintained.\n"
							+ "B - items are important but less important than A items and more important than C items and are less tightly controlled and good records are maintained.\n"
							+ "C - items are marginally important with the simplest controls possible and minimal records are maintained.\n"
							+ "Typically  using ABC analysis as the basis for defining the frequency in which items are cycle counted. The frequency with which you count your items depends upon criticality "
							+ "of the item, cost of the item, lead time of the item and past stock movements of the item as well as other criteria.\n"
							+ "This concepts demonstrates how to set up ABC classes create ABC classification sets and perform ABC assignments to perform ABC analysis. Setting up of ABC information in the Setup "
							+ "and Maintenance work area.\n"
							+ "This table summarizes the key decisions for ABC analysis:\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(secondMainHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			PdfPTable rowsDataTable = new PdfPTable(2);
			rowsDataTable.setTotalWidth(100f);
			// onkar added
			Paragraph rowsDataTablevalue = new Paragraph();
			rowsDataTablevalue = new Paragraph("Decisions to be consider",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			rowsDataTable.addCell(rowsDataTablevalue);
			rowsDataTablevalue = new Paragraph("In This Concept",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			rowsDataTable.addCell(rowsDataTablevalue);
			rowsDataTablevalue = new Paragraph("How to determine relative ranking of each item?",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			rowsDataTable.addCell(rowsDataTablevalue);
			rowsDataTablevalue = new Paragraph("Create an ABC class",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			rowsDataTable.addCell(rowsDataTablevalue);
			rowsDataTablevalue = new Paragraph(
					"How to arrange the items in the decreasing order based on the defined criteria?",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			rowsDataTable.addCell(rowsDataTablevalue);
			rowsDataTablevalue = new Paragraph("Create an ABC classification set",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			rowsDataTable.addCell(rowsDataTablevalue);
			rowsDataTablevalue = new Paragraph("How to associate ABC classes with ABC classification sets?",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			rowsDataTable.addCell(rowsDataTablevalue);
			rowsDataTablevalue = new Paragraph("Create ABC assignment groups",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			rowsDataTable.addCell(rowsDataTablevalue);
			rowsDataTablevalue = new Paragraph("How to associate items to ABC classes within an ABC assignment group?",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			rowsDataTable.addCell(rowsDataTablevalue);
			rowsDataTablevalue = new Paragraph("Perform ABC assignments",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			rowsDataTable.addCell(rowsDataTablevalue);
			rowsDataTablevalue = new Paragraph("Can we update ABC assignments?",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			rowsDataTable.addCell(rowsDataTablevalue);
			rowsDataTablevalue = new Paragraph("                    Yes",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			rowsDataTable.addCell(rowsDataTablevalue);
			rowsDataTablevalue = new Paragraph("Can we purge an ABC assignment group?",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			rowsDataTable.addCell(rowsDataTablevalue);
			rowsDataTablevalue = new Paragraph("                    Yes",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			rowsDataTable.addCell(rowsDataTablevalue);
			document.add(rowsDataTable);
			document.add(new Paragraph(Chunk.NEWLINE));
			// bg color
			Chunk thirdSubHeaddingChunk = new Chunk("Proposed methodology\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			thirdSubHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 365, 3);
			Paragraph thirdSubHeadding = new Paragraph(thirdSubHeaddingChunk);
			thirdSubHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(thirdSubHeadding);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph thirdSubHeaddingData = new Paragraph();
			thirdSubHeaddingData.add(new Paragraph(
					"   In ABC Analysis, the items are classified into A, B, C category in which \n"
							+ "A - items are which annual consumption value is the highest and the top 70-80% of the annual consumption of the organization typically accounts for only 10-20% of the total inventory items.\n"
							+ "B - items are the inter-class items with medium consumption value that 15-25% of annual consumption value typically accounts for 30% of the total inventory items.\n"
							+ "C - items are the items with the lowest consumption value the lower 5% of the annual consumption value typically accounts for 50% of total inventory items.",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(thirdSubHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));

			// bg color
			Chunk fourthSubHeadingChunk = new Chunk("Steps involved in ABC Analysis:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			fourthSubHeadingChunk.setBackground(backgroundBaseColor, 0, 3, 315, 3);
			Paragraph fourthSubHeading = new Paragraph(fourthSubHeadingChunk);
			fourthSubHeading.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(fourthSubHeading);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph fourthSubHeadingData = new Paragraph();
			fourthSubHeadingData.add(new Paragraph(" To conduct ABC analysis following six Steps are necessary:\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			fourthSubHeadingData.add(
					new Paragraph("    1) Prepare the list of items and estimate their annual consumption (units).\n",
							new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			fourthSubHeadingData.add(new Paragraph("    2) Determine unit price (or cost) of each item.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			fourthSubHeadingData.add(new Paragraph(
					"    3) Multiply each annual consumption by its unit price (or cost) to obtain its annual consumption in rupees (annual usage).\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			fourthSubHeadingData.add(new Paragraph(
					"    4) Arrange items in the descending order of their annual usage starting with the highest annual usage down to the smallest usage.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			fourthSubHeadingData.add(new Paragraph(
					"    5) Calculate cumulative annual usages and express the same as cumulative usage percentages. Also express the number of items into cumulative item percentages.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			fourthSubHeadingData.add(new Paragraph(
					"    6) Graph cumulative usage percentages against cumulative item percentages and segregate the items into A, B and C categories.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(fourthSubHeadingData);
			document.add(new Paragraph(Chunk.NEWLINE));

			// bg color
			Chunk fifthMainHeaddingChunk = new Chunk(
					"" + analysisType + " Analysis - Based on consumption value per year\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			fifthMainHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 180, 3);
			Paragraph fifthMainHeadding = new Paragraph(fifthMainHeaddingChunk);
			fifthMainHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(fifthMainHeadding);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph fifstMainHeaddingData = new Paragraph();
			fifstMainHeaddingData.add(new Paragraph("  " + analysisType
					+ "  analysis classifies the materials based on their consumption during a particular time period (usually one year)\n"
					+ "   In day-to-day warehouse operations, materials are some time under issued, over issued, issued and not accounted into the system, misplaced, etc. This results into inaccuracy in the inventory. "
					+ "Cycle counting is the process to count and reconcile the materials. Ideally, every material in the warehouse should be counted during a fixed interval (every year) for maintaining 100% accuracy,"
					+ " but counting & reconciling every material is not cost effective and very expensive. To count the accuracy of the inventory in a cost-effective manner, it is recommended to count the materials "
					+ "based on inventory classification\n"
					+ "An inventory controller shall be concentrating more on the A class items for reducing the inventory as he/she shall be concentrating only 5% to 10% of the total items and shall be getting the "
					+ "opportunity to reduce inventory on 60% to 80% of the value\n"
					+ "Any reduction in lead time of A class items shall result in reduction in inventory, so procurement manager will work out with suppliers to reduce the lead time\n"
					+ "On issue of materials, Tight control on A class, Moderate control on B class, Loose Control on C class. So, A class items may be issued after getting the approvals from Senior Executives of the "
					+ "company. B may be moderately controlled. Very little control can be exercised while issuing C class item\n"
					+ "Note: An A class item need not necessarily be a fast-moving item. Alternatively, C class may or may not be a fast-moving item. ABC analysis is purely based on the dollar value of consumptionn",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(fifstMainHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			PdfPTable dataTable = new PdfPTable(3);
			dataTable.setTotalWidth(100f);
			float[] columnWidths = new float[] { 8f, 65f, 27f };
			dataTable.setWidths(columnWidths);
			// onkar added
			Paragraph cellvalue = new Paragraph();
			cellvalue = new Paragraph("    A", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));
			dataTable.addCell(cellvalue);
			cellvalue = new Paragraph(
					"It represents the most valuable products. These are the products that contribute heavily to your overall profit without "
							+ "eating up too much of your resources. This category will be the smallest category reserved exclusively for biggest money makers. "
							+ "It requires tight control",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));
			dataTable.addCell(cellvalue);
			cellvalue = new Paragraph("Cumulative percent: 0-75%\n" + "It represents the most valuable products",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			dataTable.addCell(cellvalue);
			cellvalue = new Paragraph("    B", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar
																											// added
			dataTable.addCell(cellvalue);
			cellvalue = new Paragraph(
					"It represents the middle products. These are the products that contribute to the bottom line but aren’t significant enough to "
							+ "receive a lot of attention. It requires moderate control",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			dataTable.addCell(cellvalue);
			cellvalue = new Paragraph("Cumulative percent: >75-90%\n" + "It represents the middle products",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			dataTable.addCell(cellvalue);
			cellvalue = new Paragraph("    C", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar
																											// added
			dataTable.addCell(cellvalue);
			cellvalue = new Paragraph(
					"It represents the hundreds of tiny transactions that are essential for profit but don’t individually contribute much value "
							+ "to the company. This is the category where most of the products will live. It is also the category where we must try to automate sales as much as possible to drive down overhead costs. It requires least control",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			dataTable.addCell(cellvalue);
			cellvalue = new Paragraph(
					"Cumulative percent: >90-100%\n"
							+ "It represents the hundreds of tiny transactions that are essential",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			dataTable.addCell(cellvalue);
			document.add(dataTable);
			document.add(new Paragraph(Chunk.NEWLINE));
			// bg color
			Chunk sixthMainHeaddingChunk = new Chunk("Steps Performed on ABC Analysis:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			sixthMainHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 297, 3);
			Paragraph sixthMainHeadding = new Paragraph(sixthMainHeaddingChunk);
			Paragraph sixthMainHeaddingData = new Paragraph();
			sixthMainHeaddingData.add(new Paragraph("    1) Identify the objective and the analysis criterion\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			sixthMainHeaddingData.add(new Paragraph(
					"    2) Collect all the data about the inventory and calculate the consumption or sale value\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			sixthMainHeaddingData
					.add(new Paragraph("    3) Arrange all the consumption values in descending order of values\n",
							new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			sixthMainHeaddingData.add(new Paragraph(
					"    4) Create next column and start adding the cumulative total of consumption value\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			sixthMainHeaddingData.add(new Paragraph(
					"    5) Consumption is considered for 5 years’ data and results are calculated as average for one year\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(sixthMainHeadding);
			document.add(sixthMainHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk sixthMainHeaddingChunk1 = new Chunk("ABC Analysis Cut Off Percentages as per PiLog recommendation:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			sixthMainHeaddingChunk1.setBackground(backgroundBaseColor, 0, 3, 100, 3);
			Paragraph sixthMainHeadding1 = new Paragraph(sixthMainHeaddingChunk1);
			document.add(sixthMainHeadding1);
			document.add(new Paragraph(Chunk.NEWLINE));
			PdfPTable countTable = new PdfPTable(2);
			countTable.setTotalWidth(100f);
			float[] columnWidths2 = new float[] { 20f, 80f };
			countTable.setWidths(columnWidths2);
			cellvalue = new Paragraph("           A", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar
																													// added
			countTable.addCell(cellvalue);
			cellvalue = new Paragraph("                            Cumulative percent: 0-75%",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			countTable.addCell(cellvalue);
			cellvalue = new Paragraph("           B", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar
																													// added
			countTable.addCell(cellvalue);
			cellvalue = new Paragraph("                            Cumulative percent: >75-90%",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			countTable.addCell(cellvalue);
			cellvalue = new Paragraph("           C", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar
																													// added
			countTable.addCell(cellvalue);
			cellvalue = new Paragraph("                            Cumulative percent: >90-100%",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			countTable.addCell(cellvalue);
//            countTable.setSpacingBefore(30f);
			document.add(countTable);
			document.add(new Paragraph(Chunk.NEWLINE));
			document.add(new Paragraph(Chunk.NEXTPAGE));
			document.add(new Paragraph(Chunk.NEWLINE));
			// bg color
			Chunk eightMainHeaddingChunk = new Chunk(
					"Sample Graphical Representation Of " + analysisType + " Analysis:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			eightMainHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 190, 3);
			Paragraph eightMainHeadding = new Paragraph(eightMainHeaddingChunk);
			document.add(eightMainHeadding);
			document.add(new Paragraph(Chunk.NEWLINE));
			Image img = getImage(imageStr);
			img.scalePercent(70);
			img.setAlignment(com.itextpdf.text.Element.ALIGN_CENTER);
			document.add(img);
			document.add(new Paragraph(Chunk.NEWLINE));
			// bg color
			Chunk ninethMainHeaddingChunk = new Chunk("Pareto Concept:\n",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.WHITE));
			ninethMainHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 396, 3);
			Paragraph ninethMainHeadding = new Paragraph(ninethMainHeaddingChunk);
			document.add(ninethMainHeadding);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph ninethMainHeaddingValue = new Paragraph(
					"The adaptation of Pareto's Law of the vital few and trivial many follows pattern:",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(ninethMainHeaddingValue);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph ninethMainHeaddingValue1 = new Paragraph(
					"Category ‘A’ consists of items of high value but small in numbers.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(ninethMainHeaddingValue1);
			Paragraph ninethMainHeaddingValue2 = new Paragraph(
					"Category ‘B’ kept in between A and C items. ‘B’ items have similar controls to ‘A’ items but are less frequent.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(ninethMainHeaddingValue2);
			Paragraph ninethMainHeaddingValue3 = new Paragraph(
					"Category ‘C’ items have the simplest controls. They are only important if there is a shortage of one of them.  Thus ‘C’ category  "
							+ "items can be ordered in large quantities and have higher safety stocks.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(ninethMainHeaddingValue3);
			document.add(new Paragraph(Chunk.NEWLINE));
			// bg color
			Chunk ninethMainHeadding1Chunk = new Chunk("Classification of Material as per Pareto concept:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			ninethMainHeadding1Chunk.setBackground(backgroundBaseColor, 0, 3, 210, 3);
			Paragraph ninethMainHeadding1 = new Paragraph(ninethMainHeadding1Chunk);
			document.add(ninethMainHeadding1);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings9 = new Paragraph();
			bulletMainHeaddings9.add(bullet);
			bulletMainHeaddings9.add(new Phrase(
					"  A category inventory items where annual consumption value is the highest. Applying the Pareto principle (also referred to as the 80/20 rule where 80 "
							+ "percent of the output is determined by 20 percent of the input) they comprise a relatively small number of items but have a relatively high consumption value. So it’s logical that "
							+ "analysis and control of this class is relatively intense since there is the greatest potential to reduce costs or losses.",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings9);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings10 = new Paragraph();
			bulletMainHeaddings10.add(bullet);
			bulletMainHeaddings10.add(new Phrase(
					" B category inventory items are inter class items. Their consumption values are lower than A items but higher than C items. A key point of having this inter-class group is to "
							+ "watch items close to A item and C item classes that would alter their stock management policies.  So there needs to be a balance between controls to protect the asset class and "
							+ "the value at risk of loss or the cost of analysis and the potential value returned by reducing class costs.",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings10);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings11 = new Paragraph();
			bulletMainHeaddings11.add(bullet);
			bulletMainHeaddings11.add(new Phrase(
					" C category inventory items have the lowest consumption value. This class has a relatively high proportion of the total number of lines but with relatively low consumption values."
							+ " Logically, it’s not usually cost-effective to deploy tight inventory controls as the value at risk of significant loss is relatively low and the cost of analysis would typically yield "
							+ "relatively low returns.",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings11);
			document.add(new Paragraph(Chunk.NEWLINE));
//            document.add(new Paragraph(Chunk.NEXTPAGE));

			// bg color
			Chunk thirdMainHeaddingChunk = new Chunk("3. Benefits of ABC Analysis\n",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.WHITE));
			thirdMainHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 325, 3);
			Paragraph thirdMainHeadding = new Paragraph(thirdMainHeaddingChunk);
			thirdMainHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(thirdMainHeadding);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph thirdMainHeaddingData = new Paragraph(
					"ABC analysis of inventory helps you keep working capital costs low because it identifies which items you should reorder more frequently "
							+ "and which items don't need to be stocked to reducing obsolete inventory and optimizing the rate of inventory turnover.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(thirdMainHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph thirdMainHeaddingData1 = new Paragraph();
			thirdMainHeaddingData1.add(bullet);
			thirdMainHeaddingData1.add(new Phrase(
					" Better control over high-value inventory improves availability and reduces losses and costs.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(thirdMainHeaddingData1);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph thirdMainHeaddingData2 = new Paragraph();
			thirdMainHeaddingData2.add(bullet);
			thirdMainHeaddingData2.add(new Phrase(
					" More efficient use of stock management resources. During stock count more resources are dedicated to A class than B or C class holdings or "
							+ "fewer counts are made of B or C class holdings which saves time and money.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(thirdMainHeaddingData2);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph thirdMainHeaddingData3 = new Paragraph();
			thirdMainHeaddingData3.add(bullet);
			thirdMainHeaddingData3.add(new Phrase(
					" Relatively low value of B or C class holdings can allow a business to hold bigger buffer stocks to reduce stock outs.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(thirdMainHeaddingData3);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph thirdMainHeaddingData4 = new Paragraph();
			thirdMainHeaddingData4.add(bullet);
			thirdMainHeaddingData4.add(new Phrase(" Fewer stock outs resulting in improved production efficiency.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(thirdMainHeaddingData4);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph thirdMainHeaddingData5 = new Paragraph();
			thirdMainHeaddingData5.add(bullet);
			thirdMainHeaddingData5.add(new Phrase(
					" Fewer stock outs and improved production efficiency resulting in more reliable cycle time and improved customer satisfaction.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(thirdMainHeaddingData5);
			document.add(new Paragraph(Chunk.NEWLINE));

			// bg color
			Chunk fourthSubMainHeaddingChunk = new Chunk("Advantages of ABC Analysis in Inventory Management:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			fourthSubMainHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 165, 3);
			Paragraph fourthSubMainHeadding = new Paragraph(fourthSubMainHeaddingChunk);
			document.add(fourthSubMainHeadding);
			document.add(new Paragraph(Chunk.NEWLINE));

			// bg color
			Chunk fourthSubMainHeadding1Chunk = new Chunk("Better Forecasting:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			fourthSubMainHeadding1Chunk.setBackground(backgroundBaseColor, 0, 3, 392, 3);
			Paragraph fourthSubMainHeadding1 = new Paragraph(fourthSubMainHeadding1Chunk);
			document.add(fourthSubMainHeadding1);
			Paragraph fourthSubMainHeaddingData1 = new Paragraph(
					"ABC analysis of inventory helps you effectively forecast demand by splitting companies inventory into categories "
							+ "( A,B & C ) that are based on consumption and customer demand.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(fourthSubMainHeaddingData1);
			document.add(new Paragraph(Chunk.NEWLINE));

			// bg color
			Chunk fourthSubMainHeadding2Chunk = new Chunk("Ease of Negotiations with Suppliers:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			fourthSubMainHeadding2Chunk.setBackground(backgroundBaseColor, 0, 3, 282, 3);
			Paragraph fourthSubMainHeadding2 = new Paragraph(fourthSubMainHeadding2Chunk);

			document.add(fourthSubMainHeadding2);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph fourthSubMainHeaddingData2 = new Paragraph(
					"After analyzing your inventory and splitting inventory into A, B or C categories  the professionals need to know which "
							+ "products should focus on buying at the lowest price and which suppliers of those products you should negotiate with to bring their prices down so you can maximize "
							+ "companies profit margins.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(fourthSubMainHeaddingData2);
			document.add(new Paragraph(Chunk.NEWLINE));

			// bg color
			Chunk fourthSubMainHeadding3Chunk = new Chunk("Strategic Pricing (Deciding factor):\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			fourthSubMainHeadding3Chunk.setBackground(backgroundBaseColor, 0, 3, 295, 3);
			Paragraph fourthSubMainHeadding3 = new Paragraph(fourthSubMainHeadding3Chunk);
			document.add(fourthSubMainHeadding3);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph fourthSubMainHeaddingData3 = new Paragraph(
					"Since the items are category A are best-sellers  and company may be able to raise the prices of those products for increased revenue.\n"
							+ "Where as with products in category B or C professional may have to use creative sale techniques like product bundling or social media sales to move these products out of warehouse.",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(fourthSubMainHeaddingData3);
			document.add(new Paragraph(Chunk.NEWLINE));

			// bg color
			Chunk fifththSubMainHeaddingChunk = new Chunk("Best Practices of ABC Analysis Inventory Control:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			fifththSubMainHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 202, 3);
			Paragraph fifththSubMainHeadding = new Paragraph(fifththSubMainHeaddingChunk);
			document.add(fifththSubMainHeadding);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph fourthSubMainHeaddingData = new Paragraph(
					"ABC analysis is an important technique in materials management. It includes of supply chain functions including sourcing, procuring, "
							+ "receiving, and inventory managing.\n"
							+ "Simply an ABC analysis definition is the categorization of inventory material into three categories (A, B, and C) to determine levels of importance."
							+ "Category A items are regularly counted and tightly controlled. Category B items are counted somewhat regularly and are somewhat controlled. Category C items are "
							+ "counted less frequently and more leniently controlled.",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(fourthSubMainHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph tenthMainHeadding = new Paragraph(
					"ABC classification is important in inventory management for several reasons. It allows supply chain managers to:\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			Paragraph tenthMainHeaddingData = new Paragraph();
			tenthMainHeaddingData.add(new Paragraph(
					"    1) Identify the inventory items that pose the biggest business risks due to theft or damage, and pose the largest opportunities from to sales\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			tenthMainHeaddingData.add(new Paragraph(
					"    2) Help warehouse managers and other supply chain professionals to properly prioritize their time\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			tenthMainHeaddingData.add(
					new Paragraph("    3) Empower warehouse managers to achieve close to 100% inventory accuracy\n",
							new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(tenthMainHeadding);
			document.add(tenthMainHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph SubMainHeadding1 = new Paragraph("Generally, there are two popular methods of ABC analysis.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(SubMainHeadding1);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph SubMainHeadding2 = new Paragraph(
					"Some of the supply chain professionals categorize items based on how frequently they movement. In some instance frequently ordered items would sit in Category A,"
							+ " items ordered somewhat often would sit in Category B and items ordered less frequently would sit in Category C..\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(SubMainHeadding2);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph SubMainHeadding3 = new Paragraph(
					"Finally, the justification for this method is that fast moving items are more likely to experience stockouts. This means they are more susceptible to loss, "
							+ "theft, spoilage, or damage. Consequently, warehouse managers keep a closer eye on items with more frequent inventory counts.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(SubMainHeadding3);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph SubMainHeadding4 = new Paragraph(
					"Some of the supply chain managers prefer to categorize items based on their value. The most expensive items fall into Category A. items with an average price fall"
							+ " into Category B and the cheapest items fall into Category C.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(SubMainHeadding4);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph SubMainHeadding5 = new Paragraph(
					"Final justification for this method is that these items represent the highest individual sales for a company and the biggest potential loss. "
							+ "This would also allow managers to do the right stock replenishment decisions.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(SubMainHeadding5);
			document.add(new Paragraph(Chunk.NEWLINE));
//            document.add(new Paragraph(Chunk.NEXTPAGE));
			// bg color
			Chunk fourthMainHeaddingChunk = new Chunk("4.Perform ABC Analysis For Inventory Control Management:\n",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.WHITE));
			fourthMainHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 77, 3);
			Paragraph fourthMainHeadding = new Paragraph(fourthMainHeaddingChunk);
			fourthMainHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(fourthMainHeadding);
			document.add(new Paragraph(Chunk.NEWLINE));
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph fourthMainHeaddingdata = new Paragraph();
			fourthMainHeaddingdata.add(new Phrase(
					" The method we will use to conduct an ABC analysis takes both factors into consideration, the demand and the cost of the inventory material.\n"
							+ "When the demand levels between different items is equal or close to equal companies will focus on the cost.\n"
							+ "When the cost of different items is equal or close to equal companies focus on the demand.\n"
							+ "The following are the steps to perform ABC analysis\n"
							+ "Step 1: Gather All Inventory Data\n" + "Step 2: Find The Total Value of Each Item\n"
							+ "Step 3: Calculate the Total Value of Your Inventory\n"
							+ "Step 4: Calculate the Percentage of Value Each Inventory Item Offers\n"
							+ "Step 5: Classify Your Inventory\n" + "Step 6: Schedule Follow-Up Activities\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(fourthMainHeaddingdata);
			document.add(new Paragraph(Chunk.NEWLINE));
			document.add(new Paragraph(Chunk.NEWLINE));
			document.add(new Paragraph(Chunk.NEWLINE));

			// bg color
			Chunk ninethsubMainHeaddingChunk = new Chunk("Process flow of ABC analysis:\n",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.WHITE));
			ninethsubMainHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 305, 3);
			Paragraph ninethsubMainHeadding = new Paragraph(ninethsubMainHeaddingChunk);
			document.add(ninethsubMainHeadding);
			document.add(new Paragraph(Chunk.NEWLINE));

			// bg color
			Chunk ninethsubMainHeaddingDataChunk = new Chunk("ABC Classification process:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			ninethsubMainHeaddingDataChunk.setBackground(backgroundBaseColor, 0, 3, 345, 3);
			Paragraph ninethsubMainHeaddingData = new Paragraph(ninethsubMainHeaddingDataChunk);
			ninethsubMainHeaddingData.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(ninethsubMainHeaddingData);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph ninethsubMainHeaddingData1 = new Paragraph();
			ninethsubMainHeaddingData1.add(new Phrase(
					" The concept of ABC classification is a simple process of classifying material in to large, complex inventories. While classifying your inventory the first time a lot "
							+ "of work with little return and payoff. When you consider that it forms the foundation of proper procedure and material flow otherwise not achievable the result.\n"
							+ "Once started conducting an ABC analysis of inventory the following steps has to follow.",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(ninethsubMainHeaddingData1);
			document.add(new Paragraph(Chunk.NEWLINE));

			// bg color
			Chunk ninethsubMainHeaddingDataStep1Chunk = new Chunk("Step 1. Separate Purchased Items\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			ninethsubMainHeaddingDataStep1Chunk.setBackground(backgroundBaseColor, 0, 3, 300, 3);
			Paragraph ninethsubMainHeaddingDataStep1 = new Paragraph(ninethsubMainHeaddingDataStep1Chunk);
			ninethsubMainHeaddingDataStep1.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(ninethsubMainHeaddingDataStep1);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph ninethsubMainHeaddingDataValue1 = new Paragraph();
			ninethsubMainHeaddingDataValue1.add(new Phrase(
					" Initially create two item lists, One list is for items that are purchased and the other is for items that are manufactured. In this way we have a "
							+ "complete and accurate look at the highest, middle-range and low costs in each category. Because the annual cost of manufactured items tends to be much higher than that of purchased "
							+ "parts and manufactured items will dominate the limited class A. Pushing  purchased items that require Classes A with respect to B and C categories.\n"
							+ "Also not to exclude maintenance, repair and operating (MRO) items from list of inventory classification. The expense of MRO supplies can add in to it. "
							+ "Have to focus on inventory value. We can focus mainly on all purchased items even if they aren’t directly related to production.",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(ninethsubMainHeaddingDataValue1);
			document.add(new Paragraph(Chunk.NEWLINE));

			// bg color
			Chunk ninethsubMainHeaddingDataStep2Chunk = new Chunk(
					"Step 2. Collect the Standard Cost and Annual Usage Data\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			ninethsubMainHeaddingDataStep2Chunk.setBackground(backgroundBaseColor, 0, 3, 150, 3);
			Paragraph ninethsubMainHeaddingDataStep2 = new Paragraph(ninethsubMainHeaddingDataStep2Chunk);
			ninethsubMainHeaddingDataStep2.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(ninethsubMainHeaddingDataStep2);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph ninethsubMainHeaddingDataValue2 = new Paragraph();
			ninethsubMainHeaddingDataValue2.add(new Phrase(
					" In a spreadsheet( excel ), we can segregate the material in three columns. The first column should be a list of the part numbers for"
							+ " every item. The second column should contain the unit cost of each listed inventory item as it is consumed and The third column is where you input the annual demand for "
							+ "each item. Here demand should also be in consumption quantity..",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(ninethsubMainHeaddingDataValue2);
			document.add(new Paragraph(Chunk.NEWLINE));

			// bg color
			Chunk ninethsubMainHeaddingDataStep3Chunk = new Chunk("Step 3. Calculate the Annual Spend\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			ninethsubMainHeaddingDataStep3Chunk.setBackground(backgroundBaseColor, 0, 3, 290, 3);
			Paragraph ninethsubMainHeaddingDataStep3 = new Paragraph(ninethsubMainHeaddingDataStep3Chunk);
			ninethsubMainHeaddingDataStep3.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(ninethsubMainHeaddingDataStep3);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph ninethsubMainHeaddingDataValue3 = new Paragraph();
			ninethsubMainHeaddingDataValue3.add(new Phrase(
					" Once collected the consumption and cost for the materials calculation of annual spend. Simply multiply the standard unit cost of each part by its "
							+ "annual demand( consumption ). This will give you the annual spend for each item.\n"
							+ "Then sort out the materials by their annual spend. Arrange in descending order so that those with the highest annual spend values are on top. Once items are sorted by annual spend then "
							+ "calculate the total cumulative annual spend for materials.",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(ninethsubMainHeaddingDataValue3);
			document.add(new Paragraph(Chunk.NEWLINE));

			// bg color
			Chunk ninethsubMainHeaddingDataStep4Chunk = new Chunk("Step 4. Calculation of Cumulative Total\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			ninethsubMainHeaddingDataStep4Chunk.setBackground(backgroundBaseColor, 0, 3, 262, 3);
			Paragraph ninethsubMainHeaddingDataStep4 = new Paragraph(ninethsubMainHeaddingDataStep4Chunk);
			ninethsubMainHeaddingDataStep4.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(ninethsubMainHeaddingDataStep4);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph ninethsubMainHeaddingDataValue4 = new Paragraph();
			ninethsubMainHeaddingDataValue4.add(new Phrase(
					" The Cumulative annual spend is simply a running total of each item’s annual spend that accumulates to equal the total annual spend of all items."
							+ " The cumulative running total should be equal to the sum of the annual spend of itself plus the annual spend of all items..",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(ninethsubMainHeaddingDataValue4);
			document.add(new Paragraph(Chunk.NEWLINE));

			// bg color
			Chunk ninethsubMainHeaddingDataStep5Chunk = new Chunk("Step 5. Identify Class A Items\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			ninethsubMainHeaddingDataStep5Chunk.setBackground(backgroundBaseColor, 0, 3, 330, 3);
			Paragraph ninethsubMainHeaddingDataStep5 = new Paragraph(ninethsubMainHeaddingDataStep5Chunk);
			ninethsubMainHeaddingDataStep5.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(ninethsubMainHeaddingDataStep5);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph ninethsubMainHeaddingDataValue5 = new Paragraph();
			ninethsubMainHeaddingDataValue5.add(new Phrase(
					" To identify which parts should be classified as class A items you must first find the point that can spend annual of 75% of the total cost.",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(new Paragraph(Chunk.NEWLINE));
			ninethsubMainHeaddingDataValue5
					.add(new Phrase(" Target Total Annual Spend = Total annual spend for all purchased items x 75%",
							new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(ninethsubMainHeaddingDataValue5);
			document.add(new Paragraph(Chunk.NEWLINE));

			// bg color
			Chunk ninethsubMainHeaddingDataStep6Chunk = new Chunk("Step 6. Identify Class B Items\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			ninethsubMainHeaddingDataStep6Chunk.setBackground(backgroundBaseColor, 0, 3, 330, 3);
			Paragraph ninethsubMainHeaddingDataStep6 = new Paragraph(ninethsubMainHeaddingDataStep6Chunk);
			ninethsubMainHeaddingDataStep6.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(ninethsubMainHeaddingDataStep6);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph ninethsubMainHeaddingDataValue6 = new Paragraph();
			ninethsubMainHeaddingDataValue6.add(new Phrase(
					" Class B items will represent the next 15% of your total annual spend. To find the separating class B items from class C items determine the value "
							+ "of 90% of the total annual spend.\n"
							+ "Both the class A (75%) and class B (15%) contribute to the total of 90% spend we will simply use this as a cut-off estimate between B and C inventory items.\n"
							+ "Make manual adjustments so that your cut-off point is meaningful as you did with the A items. Designate the appropriate non-A items below your identified as B items.",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(ninethsubMainHeaddingDataValue6);
			document.add(new Paragraph(Chunk.NEWLINE));

			// bg color
			Chunk ninethsubMainHeaddingDataStep7Chunk = new Chunk("Step 7. Identify class C Items\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			ninethsubMainHeaddingDataStep7Chunk.setBackground(backgroundBaseColor, 0, 3, 330, 3);
			Paragraph ninethsubMainHeaddingDataStep7 = new Paragraph(ninethsubMainHeaddingDataStep7Chunk);
			ninethsubMainHeaddingDataStep7.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(ninethsubMainHeaddingDataStep7);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph ninethsubMainHeaddingDataValue7 = new Paragraph();
			ninethsubMainHeaddingDataValue7.add(new Phrase(
					" All remaining items that do not meet the requirements to be classified as Class A or Class B are simply categorize with  “C” in the ABC "
							+ "Classification. Which is approximately 14% of the contribution.\n"
							+ "After classifying the materials in to A,B,C categories you can find the total number of items and its cumulative and percentage of total spend with the appropriate material. "
							+ "You can choose all these and plot a graph for the same by using cumulative percentages…",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(ninethsubMainHeaddingDataValue7);
			Chunk thirdMainHeaddingvalueChunk = new Chunk("Report on " + analysisType + " Analysis:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			thirdMainHeaddingvalueChunk.setBackground(backgroundBaseColor, 0, 3, 362, 3);
			Paragraph thirdMainHeaddingvalue = new Paragraph(thirdMainHeaddingvalueChunk);
			document.add(thirdMainHeaddingvalue);
			document.add(new Paragraph(Chunk.NEWLINE));
			document.add(new Paragraph(Chunk.NEWLINE));
			PdfPTable reportTable = new PdfPTable(5);
			Paragraph reportTableValue = new Paragraph();
//            cellvalue=new Paragraph(,new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));//onkar added
			reportTable.addCell("Category");
			reportTable.addCell("Count");
			reportTable.addCell("Count %");
			reportTable.addCell("Consumption Value %");
			reportTable.addCell("Value In Millions");
			reportTable.addCell("A-High");
			reportTable.addCell(request.getParameter("ACount"));
			reportTable.addCell(request.getParameter("A_Value_TP"));
			reportTable.addCell(request.getParameter("A_Pct"));
			reportTable.addCell(request.getParameter("A_Value"));
			reportTable.addCell("B-Medium");
			reportTable.addCell(request.getParameter("BCount"));
			reportTable.addCell(request.getParameter("B_Value_TP"));
			reportTable.addCell(request.getParameter("B_Pct"));
			reportTable.addCell(request.getParameter("B_Value"));
			reportTable.addCell("C-Low");
			reportTable.addCell(request.getParameter("CCount"));
			reportTable.addCell(request.getParameter("C_Value_TP"));
			reportTable.addCell(request.getParameter("C_Pct"));
			reportTable.addCell(request.getParameter("C_Value"));
			reportTable.addCell("Total");
			reportTable.addCell(String.valueOf(
					Integer.parseInt(request.getParameter("ACount")) + Integer.parseInt(request.getParameter("BCount"))
							+ Integer.parseInt(request.getParameter("CCount"))));
			reportTable.addCell("");
			reportTable.addCell("");
			reportTable.addCell(request.getParameter("Total_CPP_Pct"));
			document.add(reportTable);
			document.add(new Paragraph(Chunk.NEWLINE));
			document.add(new Paragraph(Chunk.NEWLINE));
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph endingLine = new Paragraph(
					"              Properly managing inventory can make or break a business, and inventory planning is essential to this process. Having insight into your stock at any given"
							+ " moment is critical to success. Decision-makers know they need the right analytical tools in place so they can manage their inventory effectively.",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(endingLine);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph endingquote = new Paragraph("                           \"A DOLLAR SAVED IS  DOLLAR EARNED\"\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLACK));
			document.add(endingquote);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph endingquote2 = new Paragraph(
					"\"As renowned partner of choice, we are here to assist in journey to excel\"",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLACK));
			document.add(endingquote2);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph endingquote3 = new Paragraph(
					"Learn more about how you can explore the Digital Analytical Tools in our \"Data Quality Hub\" to transform your Business",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(endingquote3);
			// link started
			document.add(new Paragraph(Chunk.NEXTPAGE));
			Paragraph recommendedAnalysis = new Paragraph("Recommended Other PiLog Products :",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.BLACK));
			document.add(recommendedAnalysis);
			Anchor anchor = new Anchor("VED Analysis (Based on Criticality & Impact)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=VED_ANALYSIS_TAB&highLevelMenu=VED_ANALYSIS");
			document.add(anchor);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor1 = new Anchor("SDE Analysis (Based on Lead Time & Availability of Items)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor1.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=SDE_ANALYSIS_TAB&highLevelMenu=SDE_ANALYSIS");
			document.add(anchor1);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor2 = new Anchor("HML Analysis (Based on Unit Price of the Material)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor2.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=HML_ANALYSIS_TAB&highLevelMenu=HML_ANALYSIS");
			document.add(anchor2);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor3 = new Anchor("FMSN Analysis (Based on Consumption Rate)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor3.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=FMSN_ANALYSIS_TAB&highLevelMenu=FMSN_ANALYSIS");
			document.add(anchor3);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor10 = new Anchor("XYZ Analysis (Based on Stock Value Accumulation)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor10.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=XYZ_ANALYSIS_TAB&highLevelMenu=XYZ_ANALYSIS");
			document.add(anchor10);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor4 = new Anchor("Overview of Vendor Data Profiling.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor4.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=VENDOR_DATA_PROFILING_TABS&highLevelMenu=VENDOR_DATA_PROFILING");
			document.add(anchor4);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor5 = new Anchor("Overview of Master Data Profiling.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor5.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=MATERIAL_DATA_PROFILING_TABS&highLevelMenu=MATERIAL_DATA_PROFILING");
			document.add(anchor5);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor6 = new Anchor("Overview of Customer Data Profiling.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor6.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=CUSTOMER_DATA_PROFILING_TABS&highLevelMenu=CUSTOMER_DATA_PROFILING");
			document.add(anchor6);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor7 = new Anchor("ISIC Classification Allocation\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor7.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=ISIC_ALLOCATION_TABS&highLevelMenu=ISIC_ALLOCATION");
			document.add(anchor7);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor8 = new Anchor("HSN Code Allocation\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor8.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=HSN_ALLOCATION_TABS&highLevelMenu=HSN_ALLOCATION");
			document.add(anchor8);
//            document.add(new Paragraph(Chunk.NEWLINE));

			// SDE Analysis (SDE analysis classifies inventory based on how freely available
			// an item or scarce an item is, or the length of its lead time.)
			// HML Analysis (HML analysis classifies the materials based on their unit
			// prices.)
			document.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void xyzPdfData(String imageStr, HttpServletRequest request, com.itextpdf.text.Document document,
			String analysisType, BaseFont customBaseFont) {
		try {
			BaseColor backgroundBaseColor = new BaseColor(64, 135, 94);// green
//            BaseColor backgroundBaseColor=new BaseColor(0, 113, 197);//blue
			PdfPCell cell = new PdfPCell();
			Paragraph paragraph = new Paragraph("Hello World");
			cell.addElement(paragraph);
			cell.setBackgroundColor(BaseColor.RED);
			Chunk firstMainHeaddingChunk = new Chunk("1.Introduction :",
					new Font(customBaseFont, 16.0f, Font.BOLD, BaseColor.WHITE));
			firstMainHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 403, 3);
			Paragraph firstMainHeadding = new Paragraph(firstMainHeaddingChunk);
			firstMainHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(firstMainHeadding);// onkar 168, 162, 64
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk firstSubHeaddingChunk = new Chunk("Material Criticality Analysis",
					new Font(customBaseFont, 14.0f, Font.BOLD, BaseColor.WHITE));
			firstSubHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 343, 3);
			Paragraph firstSubHeadding = new Paragraph(firstSubHeaddingChunk);
			firstSubHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(firstSubHeadding);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph firstMainHeaddingData = new Paragraph();
			firstMainHeaddingData.add(new Paragraph(
					"Material Criticality and Inventory Analysis is performed in order to determine the criticality, consumption value, consumption rate, stock value, "
							+ "lead time, price of single unit & frequency Inventory management is the ongoing process of moving parts and products into and out of a company’s location(s). Companies manage "
							+ "their inventory on a daily basis as they place new orders for products and ship orders out to customers. It’s important that business leaders gain a firm grasp of everything "
							+ "involved in the inventory management process. That way, they can figure out creative ways.\n"
							+ "This module allows the users to easily view inventory movement, usage and trends. Users can analyze inventory sales over a specified time frame and make decisions on how to best "
							+ "adjust item resource planning values based on sales averages and months availability. The process of understanding the moving parts & products combined with the knowledge of the "
							+ "demand for stock/product. It is the technique to determine the optimum level of inventory for a firm\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(firstMainHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph firstSmallSubHeadingData = new Paragraph(
					"The following are the various methods to control the inventory process:\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			firstSmallSubHeadingData.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(firstSmallSubHeadingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings1 = new Paragraph();
			Font zapfdingbats = new Font();
			Chunk bullet = new Chunk("\u2022", zapfdingbats);
			bulletMainHeaddings1.add(bullet);
			bulletMainHeaddings1.add(new Phrase(" ABC Analysis - Based on consumption value per year\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings1);
			document.add(cell);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletSubHeadding1 = new Paragraph(
					"        It classifies the materials based on their consumption during a particular   time period (usually one year)",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding1);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings2 = new Paragraph();
			bulletMainHeaddings2.add(bullet);
			bulletMainHeaddings2.add(new Phrase(" FMSN Analysis - Based on consumption rate\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings2);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding2 = new Paragraph(
					"        It is based on the rate of issue or rate of usage of spare parts. This classification system categorizes the items based on how frequently"
							+ " the parts are issued and how frequently they are used",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding2);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings3 = new Paragraph();
			bulletMainHeaddings3.add(bullet);
			bulletMainHeaddings3.add(new Phrase(" XYZ Analysis - Based on stock value accumulation\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings3);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding3 = new Paragraph(
					"        It classifies the materials based on stock value accumulation. It is calculated by dividing an item's current stock value by the "
							+ "total stock value of the stores.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding3);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings4 = new Paragraph();
			bulletMainHeaddings4.add(bullet);
			bulletMainHeaddings4.add(new Phrase(" VED Analysis - Based on criticality & impact\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings4);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding4 = new Paragraph(
					"        It classifies the materials according to their criticality and impact to the production process or other services i.e. how and to what extent the material "
							+ "M1 is going to effect the production if the material M1 is not available.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding4);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings5 = new Paragraph();
			bulletMainHeaddings5.add(bullet);
			bulletMainHeaddings5.add(new Phrase(" HML Analysis - Based on unit cost of material\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings5);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding5 = new Paragraph(
					"        It classifies the materials based on their unit prices. The main objective of this analysis is to minimize the inventory cost such as "
							+ "labor & material cost etc.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding5);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings6 = new Paragraph();
			bulletMainHeaddings6.add(bullet);
			bulletMainHeaddings6.add(new Phrase(" SDE Analysis: Based on Lead Time & Availability of Items\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings6);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding6 = new Paragraph(
					"        It classifies inventory based on how freely available an item or scarce an item is, or the length of its lead time.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding6);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk secondMainHeadding = new Chunk("2. XYZ Analysis \n",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.BLACK));
			secondMainHeadding.setBackground(backgroundBaseColor, 0, 3, 407, 3);
			Paragraph secondMainHeaddingp = new Paragraph(secondMainHeadding);
			secondMainHeaddingp.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondMainHeaddingp);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk secondSubHeadding = new Chunk("Overview Of XYZ Analysis:\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			secondSubHeadding.setBackground(backgroundBaseColor, 0, 3, 350, 3);
			Paragraph secondMainHeaddingsub = new Paragraph(secondSubHeadding);
			secondMainHeaddingsub.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondMainHeaddingsub);

			Paragraph secondMainHeaddingData = new Paragraph();
			secondMainHeaddingData.add(new Paragraph(
					"XYZ analysis classifies the materials based on stock value accumulation. It is calculated by dividing an item's current stock value by the total "
							+ "stock value of the stores. XYZ analysis is one of the basic supply chain techniques and it can be used to determine the inventory valuation inside stores activities.\n"
							+ "It's also strategic as it intends to enable the Inventory manager in exercising maximum control over the highest stocked item in terms of stock value.\n"
							+ "The items are first sorted on descending order of their current stock value. The values are then accumulated till values reach say 75% of the total stock value. These items are "
							+ "grouped as 'X'. Similarly, other items are grouped as 'Y' and 'Z' items based on their accumulated value reaching another 15% & 10% respectively\n"
							+ "The XYZ analysis is a method to classify products according to their variance of demand.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(secondMainHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings1 = new Paragraph();
			Font fonts = new Font();
			Chunk bullets = new Chunk("\u2022", fonts);
			bulletHeaddings1.add(bullets);
			bulletHeaddings1.add(new Phrase(
					" X – very little variance: X materials are characterized by a constant, non-changing usage over time. The requirements fluctuate only slightly around a constant"
							+ " level so that the future demand can basically be forecast quite well.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings1);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings2 = new Paragraph();
			bulletHeaddings2.add(bullets);
			bulletHeaddings2.add(new Phrase(
					" Y – some variance: The usage of Y materials is neither constant nor sporadic. With Y materials, we can often observe trends, for example, that the usage increases "
							+ "or decreases for a while, or that it is characterized by seasonal fluctuations. For these materials, it’s harder to obtain an accurate forecast.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings2);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings3 = new Paragraph();
			bulletHeaddings3.add(bullets);
			bulletHeaddings3.add(new Phrase(
					" Z – the most variation: Z materials are not used regularly. The usage can strongly fluctuate or occur sporadically. In these cases, we can often observe periods "
							+ "with no consumption at all.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings3);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings4 = new Paragraph();
			bulletHeaddings4.add(new Phrase(
					" To arrange products according to their consumption (turnover is constant, fluctuating, irregular) to derive an optimal inventory strategy. The XYZ analysis gives,"
							+ " you an immediate view of which items are expensive to hold. Through this analysis, you can reduce your money locked up by keeping as little as possible of these expensive items.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings4);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings5 = new Paragraph();
			bulletHeaddings5.add(new Phrase(
					"The XYZ analysis gives, you an immediate view of which items are expensive to hold. Through this analysis, you can reduce your money locked up by keeping as little "
							+ "as possible of these expensive items\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings5);
			document.add(new Paragraph(Chunk.NEWLINE));

			PdfPTable dataTable = new PdfPTable(3);
			dataTable.addCell("X");
			dataTable.addCell("Items which are critically important and require close monitoring and tight control");
			dataTable.addCell("Cumulative percent: 0-75%\n" + "It represents the most valuable products");
			dataTable.addCell("Y");
			dataTable.addCell(
					"Items which are of lower criticality requiring standard controls and periodic reviews of usage");
			dataTable.addCell("Cumulative percent: >75-90%\n" + "It represents the middle products");
			dataTable.addCell("Z");
			dataTable.addCell(
					"Items which require least controls, are sometimes issues as \"free stock\" or forward holding");
			dataTable.addCell("Cumulative percent: >90-100%\n");
			document.add(dataTable);
			document.add(new Paragraph(Chunk.NEWLINE));

			Chunk thirdMainHeadding = new Chunk("3.Procedure for XYZ Analysis:\n",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.BLACK));
			thirdMainHeadding.setBackground(backgroundBaseColor, 0, 3, 303, 3);
			Paragraph thirdMainHeaddingGraph = new Paragraph(thirdMainHeadding);
			thirdMainHeaddingGraph.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(thirdMainHeaddingGraph);
			document.add(new Paragraph(Chunk.NEWLINE));

			Chunk secondMainHeaddings = new Chunk("3.1 Steps Performed on " + analysisType + " Analysis:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLACK));
			secondMainHeaddings.setBackground(backgroundBaseColor, 0, 3, 279, 3);
			Paragraph secondMainHeaddingsGraph = new Paragraph(secondMainHeaddings);
			secondMainHeaddingsGraph.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondMainHeaddingsGraph);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph secondMainHeaddingDatas = new Paragraph();
			secondMainHeaddingDatas.add(new Paragraph(
					"    1) Identify the objective and the analysis criterion and determine the relevant items.\n"));
			secondMainHeaddingDatas.add(new Paragraph(
					"    2) Collect all the data about the inventory and calculate the variation coefficients of each item.\n"));
			secondMainHeaddingDatas.add(new Paragraph(
					"    3) Divide Item's current stock value (Cost per piece) with the total stock value (Total cost).\n"));
			secondMainHeaddingDatas
					.add(new Paragraph("    4) Arrange all the consumption values in descending order of values.\n"));
			secondMainHeaddingDatas
					.add(new Paragraph("    5) Create next column and start adding the cumulative total.\n"));
			secondMainHeaddingDatas.add(new Paragraph(
					"    6) Assign the Category based on the accumulated value as X, Y & Z and plot a Graphical representation divided in to categories.\n"));
			secondMainHeaddingDatas.add(new Paragraph("    7) Sort the items by increasing variation coefficient.\n"));
			document.add(secondMainHeaddingDatas);
			document.add(new Paragraph(Chunk.NEWLINE));

			Chunk secondMainHeaddings1 = new Chunk("3.2.XYZ Articles for Analysis:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLACK));
			secondMainHeaddings1.setBackground(backgroundBaseColor, 0, 3, 343, 3);
			Paragraph secondMainHeaddings1Graph = new Paragraph(secondMainHeaddings1);
			secondMainHeaddings1Graph.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondMainHeaddings1Graph);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings12 = new Paragraph();
			Font fonts1 = new Font();
			Chunk bullets1 = new Chunk("\u2022", fonts1);
			bulletHeaddings12.add(bullets1);
			bulletHeaddings12.add(new Phrase(
					" X – very little variance: X materials are characterized by a constant, non-changing usage over time. The requirements fluctuate only slightly around a constant"
							+ " level so that the future demand can basically be forecast quite well.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings12);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings22 = new Paragraph();
			bulletHeaddings22.add(bullets);
			bulletHeaddings22.add(new Phrase(
					" Y – some variance: The usage of Y materials is neither constant nor sporadic. With Y materials, we can often observe trends, for example, that the usage increases "
							+ "or decreases for a while, or that it is characterized by seasonal fluctuations. For these materials, it’s harder to obtain an accurate forecast.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings22);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings23 = new Paragraph();
			bulletHeaddings23.add(bullets);
			bulletHeaddings23.add(new Phrase(
					" Z – the most variation: Z materials are not used regularly. The usage can strongly fluctuate or occur sporadically. In these cases, we can often observe periods "
							+ "with no consumption at all.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings23);
			document.add(new Paragraph(Chunk.NEWLINE));

			Chunk thirdMainHeaddings1 = new Chunk("4. Report on " + analysisType + " Analysis:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLACK));
			thirdMainHeaddings1.setBackground(backgroundBaseColor, 0, 3, 350, 3);
			Paragraph thirdMainHeaddings1Graph = new Paragraph(thirdMainHeaddings1);
			thirdMainHeaddings1Graph.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(thirdMainHeaddings1Graph);
			document.add(new Paragraph(Chunk.NEWLINE));

			PdfPTable reportTable = new PdfPTable(5);
			reportTable.addCell("Category");
			reportTable.addCell("Count");
			reportTable.addCell("Count %");
			reportTable.addCell("Consumption Value %");
			reportTable.addCell("Value In Millions");
			reportTable.addCell("X");
			reportTable.addCell(request.getParameter("X"));
			reportTable.addCell(request.getParameter("X_COUNT"));
			reportTable.addCell(request.getParameter("X_Total_Stock"));
			reportTable.addCell(request.getParameter("X_Value"));
			reportTable.addCell("Y");
			reportTable.addCell(request.getParameter("Y"));
			reportTable.addCell(request.getParameter("Y_COUNT"));
			reportTable.addCell(request.getParameter("Y_Total_Stock"));
			reportTable.addCell(request.getParameter("Y_Value"));
			reportTable.addCell("Z");
			reportTable.addCell(request.getParameter("Z"));
			reportTable.addCell(request.getParameter("Z_COUNT"));
			reportTable.addCell(request.getParameter("Z_Total_Stock"));
			reportTable.addCell(request.getParameter("Z_Value"));
			reportTable.addCell("Total");
			reportTable.addCell(String.valueOf(Integer.parseInt(request.getParameter("X"))
					+ Integer.parseInt(request.getParameter("Y")) + Integer.parseInt(request.getParameter("Z"))));
			reportTable.addCell("");
			reportTable.addCell("");
			reportTable.addCell(request.getParameter("TOTAL_STOCK"));
			document.add(reportTable);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk fourthMainHeadding = new Chunk(
					"4.1 Sample graphical representation of " + analysisType + " Analysis:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLACK));
			fourthMainHeadding.setBackground(backgroundBaseColor, 0, 3, 178, 3);
			Paragraph fourthMainHeaddingGraph = new Paragraph(fourthMainHeadding);
			fourthMainHeaddingGraph.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(fourthMainHeaddingGraph);
			document.add(new Paragraph(Chunk.NEWLINE));
			Image img = getImage(imageStr);
			img.scalePercent(70);
			img.setAlignment(com.itextpdf.text.Element.ALIGN_CENTER);
			document.add(img);
			Chunk fourthMainHeadding1 = new Chunk("4.2 PiLog Conclusion on " + analysisType + " Analysis:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLACK));
			fourthMainHeadding1.setBackground(backgroundBaseColor, 0, 3, 279, 3);
			Paragraph fourthMainHeadding1Graph = new Paragraph(fourthMainHeadding1);
			fourthMainHeadding1Graph.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(fourthMainHeadding1Graph);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph firstSmallSubHeadingDatas = new Paragraph(
					"After analyzing the data it is observed that the recommendation factors are like\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			firstSmallSubHeadingDatas.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(firstSmallSubHeadingDatas);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph fifthMainHeaddingData = new Paragraph();
			fifthMainHeaddingData.add(new Paragraph("    After analyzing the data, it is observed that\n"));
			fifthMainHeaddingData.add(new Paragraph("    X: Approx. " + request.getParameter("X_Total_Stock") + " ("
					+ request.getParameter("X") + ") materials accounting for " + request.getParameter("X_COUNT")
					+ "% i.e., " + request.getParameter("X_VALUE") + " Millions of the total consumption value\n"));
			fifthMainHeaddingData.add(new Paragraph("    Y: Approx. " + request.getParameter("Y_Total_Stock") + " ("
					+ request.getParameter("Y") + ") materials accounting for " + request.getParameter("Y_COUNT")
					+ "% i.e., " + request.getParameter("Y_VALUE") + " Millions of the total consumption value\n"));
			fifthMainHeaddingData.add(new Paragraph("    Z: Approx. " + request.getParameter("Z_Total_Stock") + " ("
					+ request.getParameter("Z") + ") materials accounting for " + request.getParameter("Z_COUNT")
					+ "% i.e., " + request.getParameter("Z_VALUE") + " Millions of the total consumption value\n"));
			document.add(fifthMainHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk secondMainHeaddings12 = new Chunk("5.Approach to the ABC - XYZ relationship: \n",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.BLACK));
			secondMainHeaddings12.setBackground(backgroundBaseColor, 0, 3, 206, 3);
			Paragraph secondMainHeaddings12Graph = new Paragraph(secondMainHeaddings12);
			secondMainHeaddings12Graph.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondMainHeaddings12Graph);

			document.add(new Paragraph(Chunk.NEWLINE));
			PdfPTable reportTable2 = new PdfPTable(3);
			reportTable2.addCell("AX Class");
			reportTable2.addCell("BX Class");
			reportTable2.addCell("CX Class");
			reportTable2.addCell("High consumption value\n Even demand\n Reliable forecasts\n");
			reportTable2.addCell("Medium consumption value\n Even demand\n Reliable forecasts\n");
			reportTable2.addCell("Low consumption value\n Even demand\n Reliable forecasts\n");
			reportTable2.addCell("AY Class");
			reportTable2.addCell("BY Class");
			reportTable2.addCell("CY Class");
			reportTable2.addCell("High consumption value\n Predictably variable demand\n Less reliable forecasts\n");
			reportTable2.addCell("Medium consumption value\n Predictably variable demand\n Less reliable forecasts\n");
			reportTable2.addCell("Low consumption value\n Predictably variable demand\n Less reliable forecasts\n");
			reportTable2.addCell("AZ Class");
			reportTable2.addCell("BZ Class");
			reportTable2.addCell("CZ Class");
			reportTable2.addCell(
					"High consumption value\n Sporadic, variable demand\n Forecasting unreliable or impossible\n");
			reportTable2.addCell(
					"Medium consumption value\n Sporadic, variable demand\n Forecasting unreliable or impossible\n");
			reportTable2.addCell(
					"Low consumption value\n Sporadic, variable demand\n Forecasting unreliable or impossible\n");
			document.add(reportTable2);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph firstSmallSubHeadingDatas1 = new Paragraph(
					"Organizations should never be out of stock on Class AX items. By using above comparison class AZ items likely would not be inventoried because "
							+ "they could result from a large one-time purchase. Class CZ items are inventory liabilities.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			firstSmallSubHeadingDatas1.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(firstSmallSubHeadingDatas1);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph fourthMainHeadding12 = new Paragraph("A blended effort approach\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLACK));
			document.add(fourthMainHeadding12);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph firstSmallSubHeadingData2 = new Paragraph(
					"With the combination of ABC analysis to consider item value and XYZ analysis to factor in demand variation, inventory managers can then collaborate"
							+ " with other key functional managers in production, accounting, information technology, logistics and procurement.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			firstSmallSubHeadingData2.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(firstSmallSubHeadingData2);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph firstSmallSubHeadingData21 = new Paragraph(
					"The departments should all work together to both establish inventory management policies and develop systems and processes to implement these policies. "
							+ "AICPA suggests that these inventory policies include\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			firstSmallSubHeadingData21.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(firstSmallSubHeadingData21);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings33 = new Paragraph();
			bulletHeaddings33.add(bullets);
			bulletHeaddings33.add(new Phrase(" Degree of automation and timing of replenishment processes.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings33);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletHeaddings34 = new Paragraph();
			bulletHeaddings34.add(bullets);
			bulletHeaddings34.add(
					new Phrase(" Mutually agreed upon inventory parameters for ABC items including buffer stocks.\n",
							new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings34);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletHeaddings35 = new Paragraph();
			bulletHeaddings35.add(bullets);
			bulletHeaddings35.add(new Phrase(" Inventory control rules such as cycle counting frequencies.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings35);
			document.add(new Paragraph(Chunk.NEWLINE));
			PdfPTable reportTable3 = new PdfPTable(3);
			reportTable3.addCell("AX Class");
			reportTable3.addCell("BX Class");
			reportTable3.addCell("CX Class");
			reportTable3.addCell(
					"Automated replenishment\n Low buffer - JIT or consignment transfers the responsibility for security of supply\n Perpetual inventory\n");
			reportTable3
					.addCell("Automated replenishment\n Low buffer - safety first\n Periodic count; medium security\n");
			reportTable3.addCell(
					"Automated replenishment\n Low buffer - safety first\n Free stock or periodic estimation by inspection or weighing; low security\n");
			reportTable3.addCell("AY Class");
			reportTable3.addCell("BY Class");
			reportTable3.addCell("CY Class");
			reportTable3.addCell(
					"Automated with manual intervention\n Low buffer - accept stock out risk\n Perpetual inventory\n");
			reportTable3.addCell(
					"Automated with manual intervention\n Manually adjust buffer for seasonality\n Periodic count; medium security\n");
			reportTable3.addCell(
					"Automated replenishment\n High buffer - safety first\n Free stock or periodic estimation by inspection or weighing; low security\n");
			reportTable3.addCell("AZ Class");
			reportTable3.addCell("BZ Class");
			reportTable3.addCell("CZ Class");
			reportTable3.addCell("Buy to order\n No buffer - customer understands lead times\n Not stocked\n");
			reportTable3.addCell("Buy to order\n No buffer - customer understands lead times\n Not stocked\n");
			reportTable3.addCell("Automated replenishment\n High buffer\n Free stock\n");
			document.add(reportTable3);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk secondMainHeaddings41 = new Chunk("6. Calculations and Use case of XYZ Classification: \n",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.BLACK));
			secondMainHeaddings41.setBackground(backgroundBaseColor, 0, 3, 129, 3);
			Paragraph secondMainHeaddings12Graphs = new Paragraph(secondMainHeaddings41);
			secondMainHeaddings12Graphs.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondMainHeaddings12Graphs);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph secondMainHeaddings42 = new Paragraph("FORMULAE AND STEPS: \n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLACK));
			secondMainHeaddings42.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondMainHeaddings42);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph secondMainHeaddings43 = new Paragraph(
					"XYZ analysis can be calculated by using the following procedure have to be done. \n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			secondMainHeaddings43.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondMainHeaddings43);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletHeaddings44 = new Paragraph();
			bulletHeaddings44.add(bullets);
			bulletHeaddings44
					.add(new Phrase(" Sum of Squares.\n", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings44);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings45 = new Paragraph();
			bulletHeaddings45.add(bullets);
			bulletHeaddings45
					.add(new Phrase(" Variances and.\n", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings45);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings46 = new Paragraph();
			bulletHeaddings46.add(bullets);
			bulletHeaddings46.add(new Phrase(" Standard Deviation (SD).\n",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings46);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings47 = new Paragraph();
			bulletHeaddings47.add(bullets);
			bulletHeaddings47.add(new Phrase(" Co-efficient of Variation (CV)\n",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings47);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph secondMainHeaddings48 = new Paragraph(
					"Compilation of all these data can be identified and undergone in to troubles. \n",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));
			document.add(secondMainHeaddings48);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings48 = new Paragraph();
			bulletHeaddings48.add(bullets);
			bulletHeaddings48.add(new Phrase("σ is Standard Deviation\n",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings48);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings49 = new Paragraph();
			bulletHeaddings49.add(bullets);
			bulletHeaddings49.add(new Phrase(" Xi are individual values,\n",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings49);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings50 = new Paragraph();
			bulletHeaddings50.add(bullets);
			bulletHeaddings50.add(new Phrase(" X̅ is the average Value\n",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings50);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings51 = new Paragraph();
			bulletHeaddings51.add(bullets);
			bulletHeaddings51.add(new Phrase(" N is the total number of observations\n",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings51);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph secondMainHeaddings52 = new Paragraph(
					"The above formula may look simple for calculating SD. But if consider data of large organization with 3000-15000 SKU’s spread over a "
							+ "large time frame work.  \n" + "Calculating SD can be a very tough task. \n"
							+ "Once SD is CV is easily calculated. \n" + "CV= σ / X̅ (σ is S.D. and X̅is Mean)"
							+ "The first big challenge for calculating SD. As a statistician first need to define what is average SD needs to be calculated for weekly data or monthly data or any other set "
							+ "of data collection.\n"
							+ "Technically it is possible to estimate SD for the following periods.\n",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));
			document.add(secondMainHeaddings52);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings53 = new Paragraph();
			bulletHeaddings53.add(bullets);
			bulletHeaddings53.add(new Phrase(" Yearly \n", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings53);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings54 = new Paragraph();
			bulletHeaddings54.add(bullets);
			bulletHeaddings54
					.add(new Phrase(" Quarterly\n", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings54);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings55 = new Paragraph();
			bulletHeaddings55.add(bullets);
			bulletHeaddings55.add(new Phrase(" Monthly\n", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings55);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings56 = new Paragraph();
			bulletHeaddings56.add(bullets);
			bulletHeaddings56
					.add(new Phrase(" Fortnightly\n", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings56);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings57 = new Paragraph();
			bulletHeaddings57.add(bullets);
			bulletHeaddings57.add(new Phrase(" Weekly\n", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings57);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings58 = new Paragraph();
			bulletHeaddings58.add(bullets);
			bulletHeaddings58.add(new Phrase(" Daily\n", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings58);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings59 = new Paragraph();
			bulletHeaddings59.add(bullets);
			bulletHeaddings59
					.add(new Phrase(" Hourly etc \n", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings59);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph secondMainHeaddings60 = new Paragraph(
					"Let’s starts to calculate Variance, SD and CV for monthly data i.e. data consolidated monthly and their monthly patterns as per the data "
							+ "study.\n",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));
			document.add(secondMainHeaddings60);
			document.add(new Paragraph(Chunk.NEWLINE));
			document.add(new Paragraph(Chunk.NEWLINE));
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk recommendedAnalysis = new Chunk("Recommended Other PiLog Products :",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.BLACK));
			recommendedAnalysis.setBackground(backgroundBaseColor, 0, 3, 206, 3);
			Paragraph recommendedAnalysisGraph = new Paragraph(recommendedAnalysis);
			recommendedAnalysisGraph.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(recommendedAnalysisGraph);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor = new Anchor("VED Analysis (Based on Criticality & Impact)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=VED_ANALYSIS_TAB&highLevelMenu=VED_ANALYSIS");
			document.add(anchor);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor1 = new Anchor("SDE Analysis (Based on Lead Time & Availability of Items)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor1.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=SDE_ANALYSIS_TAB&highLevelMenu=SDE_ANALYSIS");
			document.add(anchor1);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor2 = new Anchor("HML Analysis (Based on Unit Price of the Material)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor2.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=HML_ANALYSIS_TAB&highLevelMenu=HML_ANALYSIS");
			document.add(anchor2);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor3 = new Anchor("FMSN Analysis (Based on Consumption Rate)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor3.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=FMSN_ANALYSIS_TAB&highLevelMenu=FMSN_ANALYSIS");
			document.add(anchor3);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor10 = new Anchor("XYZ Analysis (Based on Stock Value Accumulation)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor10.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=XYZ_ANALYSIS_TAB&highLevelMenu=XYZ_ANALYSIS");
			document.add(anchor10);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor4 = new Anchor("Overview of Vendor Data Profiling.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor4.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=VENDOR_DATA_PROFILING_TABS&highLevelMenu=VENDOR_DATA_PROFILING");
			document.add(anchor4);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor5 = new Anchor("Overview of Master Data Profiling.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor5.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=MATERIAL_DATA_PROFILING_TABS&highLevelMenu=MATERIAL_DATA_PROFILING");
			document.add(anchor5);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor6 = new Anchor("Overview of Customer Data Profiling.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor6.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=CUSTOMER_DATA_PROFILING_TABS&highLevelMenu=CUSTOMER_DATA_PROFILING");
			document.add(anchor6);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor7 = new Anchor("ISIC Classification Allocation\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor7.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=ISIC_ALLOCATION_TABS&highLevelMenu=ISIC_ALLOCATION");
			document.add(anchor7);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor8 = new Anchor("HSN Code Allocation\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor8.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=HSN_ALLOCATION_TABS&highLevelMenu=HSN_ALLOCATION");
			document.add(anchor8);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph endingLine = new Paragraph(
					"              Properly managing inventory can make or break a business, and inventory planning is essential to this process. Having insight into your stock at any given"
							+ " moment is critical to success. Decision-makers know they need the right analytical tools in place so they can manage their inventory effectively.",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(endingLine);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph endingquote = new Paragraph("                           \"A DOLLAR SAVED IS  DOLLAR EARNED\"\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLACK));
			document.add(endingquote);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph endingquote2 = new Paragraph(
					"\"As renowned partner of choice, we are here to assist in journey to excel\"",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLACK));
			document.add(endingquote2);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph endingquote3 = new Paragraph(
					"Learn more about how you can explore the Digital Analytical Tools in our \"Data Quality Hub\" to transform your Business",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(endingquote3);
			document.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void fmsnPdfData(String imageStr, HttpServletRequest request, com.itextpdf.text.Document document,
			String analysisType, BaseFont customBaseFont) {
		try {
			Image img = getImage(imageStr);
			img.scalePercent(70);
			img.setAlignment(com.itextpdf.text.Element.ALIGN_CENTER);
			PdfPTable dataTable = new PdfPTable(3);
			dataTable.addCell("F");
			dataTable.addCell("Fast Moving\n");
			dataTable.addCell("Items which are issued in 5 years every year in continuous consumption patterns\n");
			dataTable.addCell("M");
			dataTable.addCell("Medium Moving\n");
			dataTable.addCell(
					"Items which are issued times in 5 years with exception of 1 or 2 years not used and / or intermittently used for remaining years and / or used 3-4 times in every 5 year\n");
			dataTable.addCell("S");
			dataTable.addCell("Slow Moving\n");
			dataTable.addCell(
					"Items which are issued once or twice in every 5 years with extremely disjointed consumption\n");
			dataTable.addCell("N");
			dataTable.addCell("Non-Moving\n");
			dataTable.addCell("Items which are not issued in 5 years\n");

			Paragraph firstMainHeaddingData = new Paragraph();
			firstMainHeaddingData.add(new Paragraph("" + analysisType
					+ "  Analysis is based on the rate of issue or rate of usage of spare parts. "
					+ "This classification system categorizes the items based on how frequently the parts are issued and how frequently used\n"
					+ "\n"
					+ "It helps in the arrangement of stocks in the stores and in determining the distribution and handling patterns. "
					+ "It also helps to avoid investments in non-moving or slow items. It is also useful in facilitating timely control\n"
					+ "Usual classification of Items at Inventory can be classified based on the following criteria."));
			Paragraph firstMainHeadding = new Paragraph("" + analysisType + " Analysis - Based on consumption rate\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLUE));
			firstMainHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			Paragraph secondMainHeadding = new Paragraph("Steps Performed on " + analysisType + " Analysis:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLUE));
			Paragraph secondMainHeaddingData = new Paragraph();
			secondMainHeaddingData.add(new Paragraph(
					"    1) Classification is based on the pattern of issues from stores and is useful in controlling obsolescence\n"));
			secondMainHeaddingData.add(new Paragraph(
					"    2) Date of receipt or last date of issue, whichever is later, is taken to determine the no. of years which have lapsed since the last transaction\n"));
			secondMainHeaddingData.add(new Paragraph("    3) Based on Number of Years categorize as F, M, S & N\n"));
			Paragraph thirdMainHeadding = new Paragraph("Report on " + analysisType + " Analysis:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLUE));
//            Paragraph thirdMainHeaddingData = new Paragraph();
//            thirdMainHeaddingData.add(new Paragraph("    Based on the data provided for the analysis period, PiLog performed the analysis\n"));
			Paragraph fifthMainHeadding = new Paragraph("PiLog Conclusion on " + analysisType + " Analysis:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLUE));
			Paragraph fifthMainHeaddingData = new Paragraph();
			fifthMainHeaddingData.add(new Paragraph("    After analyzing the data, it is observed that\n"));
			fifthMainHeaddingData.add(
					new Paragraph("    F: Approx. " + request.getParameter("F_COUNT") + " (" + request.getParameter("F")
							+ ") of the materials are active items which are ordered every year\n"));
			fifthMainHeaddingData.add(new Paragraph("    S: Approx. " + request.getParameter("S_COUNT") + " ("
					+ request.getParameter("S") + ") of the materials are used once or twice in every 5 Years\n"));
			fifthMainHeaddingData.add(new Paragraph("    N: Approx. " + request.getParameter("N_COUNT") + " ("
					+ request.getParameter("N")
					+ ") of the materials whose consumption is nil or insignificant i.e.,    those are non-moving items\n"));
			Paragraph fourthMainHeadding = new Paragraph("Graphical Representation Of " + analysisType + " Analysis:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLUE));
			document.add(firstMainHeadding);
			document.add(firstMainHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			PdfPTable reportTable = new PdfPTable(3);
			reportTable.addCell("Category");
			reportTable.addCell("Count");
			reportTable.addCell("Count %");
			reportTable.addCell("F");
			reportTable.addCell(request.getParameter("F"));
			reportTable.addCell(request.getParameter("F_COUNT"));
			reportTable.addCell("S");
			reportTable.addCell(request.getParameter("S"));
			reportTable.addCell(request.getParameter("S_COUNT"));
			reportTable.addCell("N");
			reportTable.addCell(request.getParameter("N"));
			reportTable.addCell(request.getParameter("N_COUNT"));
			reportTable.addCell("Total");
			reportTable.addCell(String.valueOf(Integer.parseInt(request.getParameter("F"))
					+ Integer.parseInt(request.getParameter("S")) + Integer.parseInt(request.getParameter("N"))));
			reportTable.addCell("");
			reportTable.addCell("");
//            reportTable.addCell(request.getParameter("Total_CPP_Pct"));
			document.add(dataTable);
			document.add(secondMainHeadding);
			document.add(secondMainHeaddingData);
			document.add(thirdMainHeadding);
//            document.add(thirdMainHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			document.add(reportTable);
			document.add(new Paragraph(Chunk.NEWLINE));
			document.add(fourthMainHeadding);
			document.add(img);
			document.add(new Paragraph(Chunk.NEWLINE));
			document.add(new Paragraph(Chunk.NEWLINE));
			document.add(fifthMainHeadding);
			document.add(fifthMainHeaddingData);
			document.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void hmlPdfData(String imageStr, HttpServletRequest request, com.itextpdf.text.Document document,
			String analysisType, BaseFont customBaseFont) {
		try {
			BaseColor backgroundBaseColor = new BaseColor(64, 135, 94);// green
//            BaseColor backgroundBaseColor=new BaseColor(0, 113, 197);//blue
			PdfPCell cell = new PdfPCell();
			Paragraph paragraph = new Paragraph("Hello World");
			cell.addElement(paragraph);
			cell.setBackgroundColor(BaseColor.RED);
//            Font f = new Font(customBaseFont, 16.0f, Font.BOLD, BaseColor.WHITE);

			Chunk firstMainHeaddingChunk = new Chunk("1.Introduction :",
					new Font(customBaseFont, 16.0f, Font.BOLD, BaseColor.WHITE));
			firstMainHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 403, 3);
			Paragraph firstMainHeadding = new Paragraph(firstMainHeaddingChunk);
			firstMainHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(firstMainHeadding);// onkar 168, 162, 64
			document.add(cell);
			document.add(new Paragraph(Chunk.NEWLINE));
			// onkar color
			Chunk firstSubHeaddingChunk = new Chunk("Material Criticality Analysis",
					new Font(customBaseFont, 14.0f, Font.BOLD, BaseColor.WHITE));
			firstSubHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 343, 3);
			// color
			Paragraph firstSubHeadding = new Paragraph(firstSubHeaddingChunk);
//            Paragraph firstSubHeadding = new Paragraph("Material Criticality Analysis\n", new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLACK));
			firstSubHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(firstSubHeadding);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph firstMainHeaddingData = new Paragraph();
			firstMainHeaddingData.add(new Paragraph(
					"   Material Criticality and Inventory Analysis is performed in order to determine the criticality, consumption value, consumption rate, stock value, "
							+ "lead time, price of single unit & frequency Inventory management is the ongoing process of moving parts and products into and out of a company’s location(s). Companies manage "
							+ "their inventory on a daily basis as they place new orders for products and ship orders out to customers. It’s important that business leaders gain a firm grasp of everything "
							+ "involved in the inventory management process. That way, they can figure out creative ways.\n"
							+ "This module allows the users to easily view inventory movement, usage and trends. Users can analyze inventory sales over a specified time frame and make decisions on how to best "
							+ "adjust item resource planning values based on sales averages and months availability. The process of understanding the moving parts & products combined with the knowledge of the "
							+ "demand for stock/product. It is the technique to determine the optimum level of inventory for a firm\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(firstMainHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph firstSmallSubHeadingData = new Paragraph(
					"The following are the various methods to control the inventory process:",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
//            firstSmallSubHeadingData.setAlignment(Element.ALIGN_LEFT);
			document.add(firstSmallSubHeadingData);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings1 = new Paragraph();
			Font zapfdingbats = new Font();
			Chunk bullet = new Chunk("\u2022", zapfdingbats);
			bulletMainHeaddings1.add(bullet);
			bulletMainHeaddings1.add(new Phrase(" ABC Analysis - Based on consumption value per year\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings1);
//            document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletSubHeadding1 = new Paragraph(
					"        It classifies the materials based on their consumption during a particular   time period (usually one year)",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding1);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings2 = new Paragraph();
			bulletMainHeaddings2.add(bullet);
			bulletMainHeaddings2.add(new Phrase(" FMSN Analysis - Based on consumption rate\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings2);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding2 = new Paragraph(
					"        It is based on the rate of issue or rate of usage of spare parts. This classification system categorizes the items based on how frequently"
							+ " the parts are issued and how frequently they are used",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding2);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings3 = new Paragraph();
			bulletMainHeaddings3.add(bullet);
			bulletMainHeaddings3.add(new Phrase(" XYZ Analysis - Based on stock value accumulation\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings3);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding3 = new Paragraph(
					"        It classifies the materials based on stock value accumulation. It is calculated by dividing an item's current stock value by the "
							+ "total stock value of the stores.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding3);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings4 = new Paragraph();
			bulletMainHeaddings4.add(bullet);
			bulletMainHeaddings4.add(new Phrase(" VED Analysis - Based on criticality & impact\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings4);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding4 = new Paragraph(
					"        It classifies the materials according to their criticality and impact to the production process or other services i.e. how and to what extent the material "
							+ "M1 is going to effect the production if the material M1 is not available.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding4);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings5 = new Paragraph();
			bulletMainHeaddings5.add(bullet);
			bulletMainHeaddings5.add(new Phrase(" HML Analysis - Based on unit cost of material\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings5);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding5 = new Paragraph(
					"        It classifies the materials based on their unit prices. The main objective of this analysis is to minimize the inventory cost such as "
							+ "labor & material cost etc.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding5);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings6 = new Paragraph();
			bulletMainHeaddings6.add(bullet);
			bulletMainHeaddings6.add(new Phrase(" SDE Analysis: Based on Lead Time & Availability of Items\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings6);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding6 = new Paragraph(
					"        It classifies inventory based on how freely available an item or scarce an item is, or the length of its lead time.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding6);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk secondMainHeadding = new Chunk("2. HML Analysis of Inventory: \n",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.WHITE));
			secondMainHeadding.setBackground(backgroundBaseColor, 0, 3, 296, 3);
			Paragraph secondMainHeaddings = new Paragraph(secondMainHeadding);
			secondMainHeaddings.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondMainHeaddings);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk secondSubHeadding = new Chunk("2.1 Overview - Based on unit cost of material\n",
					new Font(customBaseFont, 16, Font.NORMAL, BaseColor.WHITE));
			secondSubHeadding.setBackground(backgroundBaseColor, 0, 3, 180, 3);
			Paragraph secondSubHeaddings = new Paragraph(secondSubHeadding);
			secondSubHeaddings.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondSubHeaddings);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph secondMainHeaddingData = new Paragraph();
			secondMainHeaddingData.add(new Paragraph(
					"HML is another inventory analysis method. This method of analysis classifies inventory according to the materials unit price. HML method of inventory "
							+ "describes the products or items under the following sections of inventory items.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(secondMainHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));

			Font fonts = new Font();
			Chunk bullets = new Chunk("\u2022", fonts);
			Paragraph bulletHeaddings55 = new Paragraph();
			bulletHeaddings55.add(bullets);
			bulletHeaddings55.add(new Phrase(" High Cost (H) – items with high unit value.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings55);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings56 = new Paragraph();
			bulletHeaddings56.add(bullets);
			bulletHeaddings56.add(new Phrase(" Medium Cost (M) – items with medium unit value.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings56);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings57 = new Paragraph();
			bulletHeaddings57.add(bullets);
			bulletHeaddings57.add(new Phrase(" Low Cost (L) – items with low unit value.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings57);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph secondMainHeaddingv1 = new Paragraph();
			secondMainHeaddingv1.add(new Paragraph(
					"It is based on Pareto principle or the 80/20 rule. It is used to keep control over consumption at departmental level for deciding the frequency of "
							+ "physical verification.\n"
							+ "In this analysis cut-off-lines are then fixed by the management of the organization to classify the inventory items.",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(secondMainHeaddingv1);
			document.add(new Paragraph(Chunk.NEWLINE));
			PdfPTable dataTable = new PdfPTable(3);
			dataTable.addCell("H");
			dataTable.addCell("High Cost");
			dataTable.addCell("These are the costly item and are generally 10-15% of total items");
			dataTable.addCell("M");
			dataTable.addCell("Medium Cost");
			dataTable.addCell(
					"These items are low cost item as compared to H class items, this are generally 20-25% of total items");
			dataTable.addCell("L");
			dataTable.addCell("Low Cost");
			dataTable.addCell("These items are low class item and generally 60-70% of total items");
			document.add(dataTable);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk secondSubHeadding1 = new Chunk("2.2 Objectives of HML analysis:\n",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.WHITE));
			secondSubHeadding1.setBackground(backgroundBaseColor, 0, 3, 255, 3);
			Paragraph secondSubHeaddings1 = new Paragraph(secondSubHeadding1);
			secondSubHeaddings1.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondSubHeaddings1);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings58 = new Paragraph();
			bulletHeaddings58.add(bullets);
			bulletHeaddings58.add(new Phrase(
					" Primary Objectives of HML is essential goal of this analysis is to limit the expense of Inventory.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings58);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings59 = new Paragraph();
			bulletHeaddings59.add(bullets);
			bulletHeaddings59.add(new Phrase(
					" The main objective of this analysis is to minimize the inventory cost such as labor cost, material cost etc. This analysis is similar to ABC analysis the difference "
							+ "that instant “usage value, price” criteria is used.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings59);
			document.add(new Paragraph(Chunk.NEWLINE));

			Chunk secondSubHeaddings2 = new Chunk("3. Steps Performed on HML Analysis:\n",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.WHITE));
			secondSubHeaddings2.setBackground(backgroundBaseColor, 0, 3, 222, 3);
			Paragraph secondSubHeaddings12 = new Paragraph(secondSubHeaddings2);
			secondSubHeaddings12.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondSubHeaddings12);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings60 = new Paragraph();
			bulletHeaddings60.add(bullets);
			bulletHeaddings60.add(new Phrase(
					" Prepare the list of items and calculate their unit cost, annual demand and annual usage\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings60);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings61 = new Paragraph();
			bulletHeaddings61.add(bullets);
			bulletHeaddings61.add(new Phrase(" Arrange items in the decreasing order of their unit cost\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings61);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings62 = new Paragraph();
			bulletHeaddings62.add(bullets);
			bulletHeaddings62.add(new Phrase(
					" Calculate percentage of unit cost, cumulative of unit cost and then categories the inventory item. The cut off lines are then fixed by the organization for deciding"
							+ " three categories\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings62);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings63 = new Paragraph();
			bulletHeaddings63.add(bullets);
			bulletHeaddings63.add(new Phrase(
					" Plot the graph on the basis of cumulative of unit cost and then categories the inventory items",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings63);
			document.add(new Paragraph(Chunk.NEWLINE));

			Chunk secondSubHeaddings22 = new Chunk("4. Benefits of HML analysis:\n",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.WHITE));
			secondSubHeaddings22.setBackground(backgroundBaseColor, 0, 3, 303, 3);
			Paragraph thirdMainHeadding = new Paragraph(secondSubHeaddings22);
			thirdMainHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(thirdMainHeadding);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings64 = new Paragraph();
			bulletHeaddings64.add(bullets);
			bulletHeaddings64.add(new Phrase(
					" The HML analysis is useful for keeping control over consumption at departmental levels",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings64);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings65 = new Paragraph();
			bulletHeaddings65.add(bullets);
			bulletHeaddings65.add(new Phrase(
					" HML method is deciding the frequency of physical verification and for controlling purchases.",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings65);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings66 = new Paragraph();
			bulletHeaddings66.add(bullets);
			bulletHeaddings66.add(new Phrase(" HML analysis can determine the frequency of stock verification.",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings66);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings67 = new Paragraph();
			bulletHeaddings67.add(bullets);
			bulletHeaddings67.add(new Phrase(
					" The objective of HML analysis is to keep control over the consumption at the department level. ",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings67);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletHeaddings68 = new Paragraph();
			bulletHeaddings68.add(bullets);
			bulletHeaddings68.add(new Phrase(
					" This method can evolve buying policy to control purchase of material in the organization level and delegate the authority to another buyer.",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletHeaddings68);
			document.add(new Paragraph(Chunk.NEWLINE));

			Chunk secondSubHeaddings5 = new Chunk("5.Procedure for HML analysis:\n",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.WHITE));
			secondSubHeaddings5.setBackground(backgroundBaseColor, 0, 3, 285, 3);
			Paragraph thirdMainHeaddings = new Paragraph(secondSubHeaddings5);
			thirdMainHeaddings.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(thirdMainHeaddings);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph secondMainHeadding22 = new Paragraph();
			secondMainHeadding22.add(new Paragraph(
					"The high, medium and low (HML) analysis is same as ABC. Actually, the things are matters is that rather than the yearly utilization of more utilized in the ABC. "
							+ "The purchase costs or expense per unit basis is considering in HML analysis. .\n"
							+ "HML analysis procedure depends on the Pareto standard or the 80/20 principle. The remembered for this principle conspires are grouped in slipping request "
							+ "of their unit cost.\n"
							+ "It assists the management of personnel with settling on buying strategic choices in which implies that H&M things are not to be requested more than the necessary amount by departmental"
							+ " stores.\n"
							+ "The recurrence of Inventory checks is set off by this technique. The most significant things require continuous Inventory checking. To perform HML analysis the following steps are crucial"
							+ " in defining the results:\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(secondMainHeadding22);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph secondMainHeadding12 = new Paragraph();
			secondMainHeadding12.add(new Paragraph(
					"    1) Prepare the list of items and calculate their unit cost, annual demand and annual usage\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			secondMainHeadding12.add(new Paragraph("    2) Arrange items in the decreasing order of their unit cost\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			secondMainHeadding12.add(new Paragraph(
					"    3) Calculate percentage of unit cost, cumulative of unit cost and then categories the inventory item\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			secondMainHeadding12.add(new Paragraph(
					"    4) The cut off lines are then fixed by the organization for deciding three categories\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			secondMainHeadding12.add(new Paragraph(
					"    5) Plot the graph on the basis of cumulative of unit cost and then categories the inventory items\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(secondMainHeadding12);
			document.add(new Paragraph(Chunk.NEWLINE));

			Chunk secondSubHeaddings55 = new Chunk("6.Sample Data Report on HML Analysis:\n",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.WHITE));
			secondSubHeaddings55.setBackground(backgroundBaseColor, 0, 3, 212, 3);
			Paragraph thirdMainHeaddings1 = new Paragraph(secondSubHeaddings55);
			thirdMainHeaddings1.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(thirdMainHeaddings1);
			document.add(new Paragraph(Chunk.NEWLINE));
			PdfPTable reportTable = new PdfPTable(5);
			reportTable.addCell("Category");
			reportTable.addCell("Count");
			reportTable.addCell("Count %");
			reportTable.addCell("Range ");
			reportTable.addCell("Value In Millions");
			reportTable.addCell("H");
			reportTable.addCell(request.getParameter("H"));
			reportTable.addCell(request.getParameter("H_COUNT"));
			reportTable.addCell(request.getParameter("H_CUM_PCT"));
			reportTable.addCell(request.getParameter("H_Value"));
			reportTable.addCell("M");
			reportTable.addCell(request.getParameter("M"));
			reportTable.addCell(request.getParameter("M_COUNT"));
			reportTable.addCell(request.getParameter("M_CUM_PCT"));
			reportTable.addCell(request.getParameter("M_Value"));
			reportTable.addCell("L");
			reportTable.addCell(request.getParameter("L"));
			reportTable.addCell(request.getParameter("L_COUNT"));
			reportTable.addCell(request.getParameter("L_CUM_PCT"));
			reportTable.addCell(request.getParameter("L_Value"));
			reportTable.addCell("Total");
//            reportTable.addCell(String.valueOf(Integer.parseInt(request.getParameter("H")) + Integer.parseInt(request.getParameter("M")) + Integer.parseInt(request.getParameter("L"))));
			reportTable.addCell("");
			reportTable.addCell("");
			reportTable.addCell(request.getParameter("TOTAL_STOCK"));
			document.add(reportTable);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk secondSubHeaddings56 = new Chunk("6.1 Sample Graphical representation on data for ABC-HML data\n",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.WHITE));
			secondSubHeaddings56.setBackground(backgroundBaseColor, 0, 3, 33, 3);
			Paragraph secondSubHeaddings23 = new Paragraph(secondSubHeaddings56);
			secondSubHeaddings23.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondSubHeaddings23);
			document.add(new Paragraph(Chunk.NEWLINE));
			Image img = getImage(imageStr);
			img.scalePercent(70);
			img.setAlignment(com.itextpdf.text.Element.ALIGN_CENTER);
			document.add(img);
			Chunk fifthMainHeadding = new Chunk("6.2 PiLog Conclusion on " + analysisType + " Analysis:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			fifthMainHeadding.setBackground(backgroundBaseColor, 0, 3, 255, 3);
			Paragraph fifthMainHeadding1 = new Paragraph(fifthMainHeadding);
			fifthMainHeadding1.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(fifthMainHeadding1);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph fifthMainHeaddingData = new Paragraph();
			fifthMainHeaddingData.add(new Paragraph("    After analyzing the data, it is observed that\n"));
			fifthMainHeaddingData.add(new Paragraph("    H: Approx. " + request.getParameter("H_COUNT") + " ("
					+ request.getParameter("H") + ") materials accounting for " + request.getParameter("H_CUM_PCT")
					+ "% i.e., " + request.getParameter("H_VALUE") + " Millions of the total consumption value\n"));
			fifthMainHeaddingData.add(new Paragraph("    M: Approx. " + request.getParameter("M_COUNT") + " ("
					+ request.getParameter("M") + ") materials accounting for " + request.getParameter("M_CUM_PCT")
					+ "% i.e., " + request.getParameter("M_VALUE") + " Millions of the total consumption value\n"));
			fifthMainHeaddingData.add(new Paragraph("    L: Approx. " + request.getParameter("L_COUNT") + " ("
					+ request.getParameter("L") + ") materials accounting for " + request.getParameter("L_CUM_PCT")
					+ "% i.e., " + request.getParameter("L_VALUE") + " Millions of the total consumption value\n"));
			document.add(fifthMainHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			PdfPTable reportTablel = new PdfPTable(5);
			reportTablel.addCell("Category");
			reportTablel.addCell("H");
			reportTablel.addCell("High Cost");
			reportTablel.addCell("These are the costly item and are generally 10-15% of total items");
			reportTablel.addCell("More than 50,000 SAR");
			reportTablel.addCell("M");
			reportTablel.addCell("Medium Cost");
			reportTablel.addCell(
					"These items are low cost item as compared to H class items, this are generally 20-25% of total items");
			reportTablel.addCell("Between 1000-50,000 SAR");
			reportTablel.addCell("L");
			reportTablel.addCell("Low Cost");
			reportTablel.addCell("These items are low class item and generally 60-70% of total items");
			reportTablel.addCell("Less than 1000 SAR");
			document.add(reportTablel);
			document.add(new Paragraph(Chunk.NEWLINE));
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk recommendedAnalysis = new Chunk("Recommended Other PiLog Products :",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.WHITE));
			recommendedAnalysis.setBackground(backgroundBaseColor, 0, 3, 200, 3);
			Paragraph fifthMainHeadding12 = new Paragraph(recommendedAnalysis);
			fifthMainHeadding12.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(fifthMainHeadding12);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor = new Anchor("VED Analysis (Based on Criticality & Impact)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=VED_ANALYSIS_TAB&highLevelMenu=VED_ANALYSIS");
			document.add(anchor);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor1 = new Anchor("SDE Analysis (Based on Lead Time & Availability of Items)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor1.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=SDE_ANALYSIS_TAB&highLevelMenu=SDE_ANALYSIS");
			document.add(anchor1);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor2 = new Anchor("HML Analysis (Based on Unit Price of the Material)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor2.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=HML_ANALYSIS_TAB&highLevelMenu=HML_ANALYSIS");
			document.add(anchor2);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor3 = new Anchor("FMSN Analysis (Based on Consumption Rate)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor3.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=FMSN_ANALYSIS_TAB&highLevelMenu=FMSN_ANALYSIS");
			document.add(anchor3);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor10 = new Anchor("XYZ Analysis (Based on Stock Value Accumulation)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor10.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=XYZ_ANALYSIS_TAB&highLevelMenu=XYZ_ANALYSIS");
			document.add(anchor10);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor4 = new Anchor("Overview of Vendor Data Profiling.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor4.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=VENDOR_DATA_PROFILING_TABS&highLevelMenu=VENDOR_DATA_PROFILING");
			document.add(anchor4);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor5 = new Anchor("Overview of Master Data Profiling.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor5.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=MATERIAL_DATA_PROFILING_TABS&highLevelMenu=MATERIAL_DATA_PROFILING");
			document.add(anchor5);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor6 = new Anchor("Overview of Customer Data Profiling.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor6.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=CUSTOMER_DATA_PROFILING_TABS&highLevelMenu=CUSTOMER_DATA_PROFILING");
			document.add(anchor6);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor7 = new Anchor("ISIC Classification Allocation\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor7.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=ISIC_ALLOCATION_TABS&highLevelMenu=ISIC_ALLOCATION");
			document.add(anchor7);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor8 = new Anchor("HSN Code Allocation\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor8.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=HSN_ALLOCATION_TABS&highLevelMenu=HSN_ALLOCATION");
			document.add(anchor8);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph endingLine = new Paragraph(
					"              Properly managing inventory can make or break a business, and inventory planning is essential to this process. Having insight into your stock at any given"
							+ " moment is critical to success. Decision-makers know they need the right analytical tools in place so they can manage their inventory effectively.",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(endingLine);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph endingquote = new Paragraph("                           \"A DOLLAR SAVED IS  DOLLAR EARNED\"\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLACK));
			document.add(endingquote);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph endingquote2 = new Paragraph(
					"\"As renowned partner of choice, we are here to assist in journey to excel\"",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLACK));
			document.add(endingquote2);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph endingquote3 = new Paragraph(
					"Learn more about how you can explore the Digital Analytical Tools in our \"Data Quality Hub\" to transform your Business",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(endingquote3);
			document.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void defaultPdfData(String imageStr, HttpServletRequest request, com.itextpdf.text.Document document) {
		try {
			Image img = getImage(imageStr);
			img.scalePercent(80);
			img.setAlignment(com.itextpdf.text.Element.ALIGN_CENTER);
			document.add(img);
			document.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void sdePdfData(String imageStr, HttpServletRequest request, com.itextpdf.text.Document document,
			String analysisType, BaseFont customBaseFont) {
		try {
			BaseColor backgroundBaseColor = new BaseColor(64, 135, 94);// green
//            BaseColor backgroundBaseColor=new BaseColor(0, 113, 197);//blue
			PdfPCell cell = new PdfPCell();
			Paragraph paragraph = new Paragraph("Hello World");
			cell.addElement(paragraph);
			cell.setBackgroundColor(BaseColor.RED);
//            Font f = new Font(customBaseFont, 16.0f, Font.BOLD, BaseColor.WHITE);

			Chunk firstMainHeaddingChunk = new Chunk("1.Introduction :",
					new Font(customBaseFont, 16.0f, Font.BOLD, BaseColor.WHITE));
			firstMainHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 403, 3);
			Paragraph firstMainHeadding = new Paragraph(firstMainHeaddingChunk);
			firstMainHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(firstMainHeadding);// onkar 168, 162, 64
			document.add(cell);
			document.add(new Paragraph(Chunk.NEWLINE));
			// onkar color
			Chunk firstSubHeaddingChunk = new Chunk("Material Criticality Analysis",
					new Font(customBaseFont, 14.0f, Font.BOLD, BaseColor.WHITE));
			firstSubHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 343, 3);
			// color
			Paragraph firstSubHeadding = new Paragraph(firstSubHeaddingChunk);
//            Paragraph firstSubHeadding = new Paragraph("Material Criticality Analysis\n", new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLACK));
			firstSubHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(firstSubHeadding);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph firstMainHeaddingData = new Paragraph();
			firstMainHeaddingData.add(new Paragraph(
					"   Material Criticality and Inventory Analysis is performed in order to determine the criticality, consumption value, consumption rate, stock value, "
							+ "lead time, price of single unit & frequency Inventory management is the ongoing process of moving parts and products into and out of a company’s location(s). Companies manage "
							+ "their inventory on a daily basis as they place new orders for products and ship orders out to customers. It’s important that business leaders gain a firm grasp of everything "
							+ "involved in the inventory management process. That way, they can figure out creative ways.\n"
							+ "This module allows the users to easily view inventory movement, usage and trends. Users can analyze inventory sales over a specified time frame and make decisions on how to best "
							+ "adjust item resource planning values based on sales averages and months availability. The process of understanding the moving parts & products combined with the knowledge of the "
							+ "demand for stock/product. It is the technique to determine the optimum level of inventory for a firm\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(firstMainHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph firstSmallSubHeadingData = new Paragraph(
					"The following are the various methods to control the inventory process:",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
//            firstSmallSubHeadingData.setAlignment(Element.ALIGN_LEFT);
			document.add(firstSmallSubHeadingData);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings1 = new Paragraph();
			Font zapfdingbats = new Font();
			Chunk bullet = new Chunk("\u2022", zapfdingbats);
			bulletMainHeaddings1.add(bullet);
			bulletMainHeaddings1.add(new Phrase(" ABC Analysis - Based on consumption value per year\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings1);
//            document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletSubHeadding1 = new Paragraph(
					"        It classifies the materials based on their consumption during a particular   time period (usually one year)",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding1);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings2 = new Paragraph();
			bulletMainHeaddings2.add(bullet);
			bulletMainHeaddings2.add(new Phrase(" FMSN Analysis - Based on consumption rate\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings2);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding2 = new Paragraph(
					"        It is based on the rate of issue or rate of usage of spare parts. This classification system categorizes the items based on how frequently"
							+ " the parts are issued and how frequently they are used",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding2);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings3 = new Paragraph();
			bulletMainHeaddings3.add(bullet);
			bulletMainHeaddings3.add(new Phrase(" XYZ Analysis - Based on stock value accumulation\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings3);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding3 = new Paragraph(
					"        It classifies the materials based on stock value accumulation. It is calculated by dividing an item's current stock value by the "
							+ "total stock value of the stores.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding3);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings4 = new Paragraph();
			bulletMainHeaddings4.add(bullet);
			bulletMainHeaddings4.add(new Phrase(" VED Analysis - Based on criticality & impact\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings4);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding4 = new Paragraph(
					"        It classifies the materials according to their criticality and impact to the production process or other services i.e. how and to what extent the material "
							+ "M1 is going to effect the production if the material M1 is not available.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding4);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings5 = new Paragraph();
			bulletMainHeaddings5.add(bullet);
			bulletMainHeaddings5.add(new Phrase(" HML Analysis - Based on unit cost of material\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings5);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding5 = new Paragraph(
					"        It classifies the materials based on their unit prices. The main objective of this analysis is to minimize the inventory cost such as "
							+ "labor & material cost etc.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding5);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings6 = new Paragraph();
			bulletMainHeaddings6.add(bullet);
			bulletMainHeaddings6.add(new Phrase(" SDE Analysis: Based on Lead Time & Availability of Items\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings6);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding6 = new Paragraph(
					"        It classifies inventory based on how freely available an item or scarce an item is, or the length of its lead time.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding6);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk secondMainHeadding = new Chunk("2. SDE Analysis of Inventory\n",
					new Font(customBaseFont, 20, Font.BOLD, BaseColor.WHITE));
			secondMainHeadding.setBackground(backgroundBaseColor, 0, 3, 407, 3);
			Paragraph secondMainHeaddingq = new Paragraph(secondMainHeadding);
			secondMainHeaddingq.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondMainHeaddingq);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph secondSubHeadding = new Paragraph("Overview:\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.WHITE));
			secondSubHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondSubHeadding);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph secondMainHeaddingData = new Paragraph();
			secondMainHeaddingData.add(new Paragraph(
					"The SDE analysis looks at inventory is available and classifies it according to the scarcity of supply. SDE Analysis - Based on Lead Time & Availability of Items.\n"
							+ "SDE analysis classifies inventory based on how freely available an item or scarce an item is, or the length of its lead time.\n"
							+ "Here 'S' refers to scarce items, items which are in short supply. Usually these are the raw materials, spare parts and imported material items.\n"
							+ "'D' stands for difficult items, items which are not readily available in local markets and have to be procured from faraway places or items for "
							+ "which there are a limited number of suppliers. Or items for which quality suppliers are difficult to get..\n"
							+ "'E' refers to items which are easily available in the local markets.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(secondMainHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph fourthSubHeading = new Paragraph("Steps Performed on SDE Analysis:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLACK));
			fourthSubHeading.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(fourthSubHeading);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph fourthSubHeadingData = new Paragraph();
			fourthSubHeadingData.add(new Paragraph("    1) Identify the objective and the analysis criterion.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			fourthSubHeadingData.add(
					new Paragraph("    2) Collect all the data about the scarcity and availability of the items.\n",
							new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			fourthSubHeadingData.add(new Paragraph("    3) Identify the lead time in weeks/days\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			fourthSubHeadingData.add(new Paragraph("    4) Arrange the values in the descending order.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			fourthSubHeadingData.add(new Paragraph("    5) Assign the category based on the lead time as S, D & E.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(fourthSubHeadingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk fifthMainHeadding = new Chunk("Importance of SDE analysis:\n",
					new Font(customBaseFont, 20, Font.BOLD, BaseColor.WHITE));
			fifthMainHeadding.setBackground(backgroundBaseColor, 0, 3, 407, 3);
			Paragraph secondMainHeadding1 = new Paragraph(fifthMainHeadding);
			secondMainHeadding1.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondMainHeadding1);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph fifstMainHeaddingData = new Paragraph();
			fifstMainHeaddingData.add(new Paragraph(
					"By using the SDE classification analysis the following importance can takes place\n"
							+ "1.SDE can help in order to reduce its force or neutralize any difficulties faced in the procurement process.\n"
							+ "2.By classifying items as scarce, difficult or easy can help you clearly plan material year with the procurement team.\n"
							+ "3.It helps in identify the items need to get in advance and which products can be sourced with ease. ",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(fifstMainHeaddingData);
			Chunk thirdMainHeadding = new Chunk("4. Benefits of SDE analysis\n",
					new Font(customBaseFont, 20, Font.BOLD, BaseColor.WHITE));
			thirdMainHeadding.setBackground(backgroundBaseColor, 0, 3, 407, 3);
			Paragraph secondMainHeadding12 = new Paragraph(thirdMainHeadding);
			secondMainHeadding12.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondMainHeadding12);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph thirdMainHeaddingData = new Paragraph(
					"One of the main benefits of conducting an SDE analysis is future planning. SDE analysis highlights the products might be requiring care and skill "
							+ "because difficult to acquire than others.\n"
							+ "SDE analysis can kick-start your procurement process. This allows a business owner to prepare the warehouse and organize plans with other suppliers when they can get this process "
							+ "underway.\n"
							+ "It is a simple system that can provide guidelines and a quick snapshot of what’s going on and what needs to be ordered in "
							+ "inventory items.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(thirdMainHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk thirdMainHeaddingnew = new Chunk("5. Disadvantages of SDE analysis\n",
					new Font(customBaseFont, 20, Font.BOLD, BaseColor.WHITE));
			thirdMainHeaddingnew.setBackground(backgroundBaseColor, 0, 3, 407, 3);
			Paragraph secondMainHeadding22 = new Paragraph(thirdMainHeaddingnew);
			secondMainHeadding22.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondMainHeadding22);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph ninethMainHeaddingValue1 = new Paragraph(
					"SDE can make a confuse in inventory data won’t be clearer it isn’t always very accurate.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(ninethMainHeaddingValue1);
			Paragraph ninethMainHeaddingValue2 = new Paragraph(
					"This analysis can help loosely with planning but one difficult product might vary drastically from another difficult product.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(ninethMainHeaddingValue2);
			Paragraph ninethMainHeaddingValue3 = new Paragraph(
					"For instance, one could be acquired in three weeks while another one could take fourth months to procure. However, they are both marked D for difficult"
							+ ".\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(ninethMainHeaddingValue3);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph ninethMainHeadding1 = new Paragraph(
					"It can provide some overarching guidance but this classification shouldn’t be used to plan exact dates.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(ninethMainHeadding1);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph ninethMainHeadding = new Paragraph(
					"Consulting with individual suppliers is necessary and each product needs to be managed on an individual basis.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(ninethMainHeadding);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk thirdMainHeaddingnew5 = new Chunk("6. Report on SDE Analysis\n",
					new Font(customBaseFont, 20, Font.BOLD, BaseColor.WHITE));
			thirdMainHeaddingnew5.setBackground(backgroundBaseColor, 0, 3, 362, 3);
			Paragraph thirdMainHeaddingvalue = new Paragraph(thirdMainHeaddingnew5);
			thirdMainHeaddingvalue.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(thirdMainHeaddingvalue);
			document.add(new Paragraph(Chunk.NEWLINE));
			PdfPTable reportTable = new PdfPTable(4);
			reportTable.addCell("Criticality");
			reportTable.addCell("Count");
			reportTable.addCell("Count %");
			reportTable.addCell("Input Count");
			reportTable.addCell("S-Scarce");
			reportTable.addCell(request.getParameter("S"));
			reportTable.addCell(request.getParameter("S_Count"));
			reportTable.addCell(request.getParameter("S_INPUT_PCT"));
			reportTable.addCell("D-Difficult");
			reportTable.addCell(request.getParameter("D"));
			reportTable.addCell(request.getParameter("D_Count"));
			reportTable.addCell(request.getParameter("D_INPUT_PCT"));
			reportTable.addCell("E-Easy to acquire");
			reportTable.addCell(request.getParameter("E"));
			reportTable.addCell(request.getParameter("E_Count"));
			reportTable.addCell(request.getParameter("E_INPUT_PCT"));
			reportTable.addCell("Total");
			reportTable.addCell(String.valueOf(Integer.parseInt(request.getParameter("S"))
					+ Integer.parseInt(request.getParameter("D")) + Integer.parseInt(request.getParameter("E"))));
			reportTable.addCell("");
			reportTable.addCell("");
			document.add(reportTable);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk thirdMainHeaddingnew1 = new Chunk("6.1 Graphical Representation of SDE Analysis:\n",
					new Font(customBaseFont, 18, Font.BOLD, BaseColor.WHITE));
			thirdMainHeaddingnew1.setBackground(backgroundBaseColor, 0, 3, 362, 3);
			Paragraph thirdMainHeaddingvalues = new Paragraph(thirdMainHeaddingnew1);
			thirdMainHeaddingvalues.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(thirdMainHeaddingvalues);
			document.add(new Paragraph(Chunk.NEWLINE));
			Image img = getImage(imageStr);
			img.scalePercent(40);
			img.setAlignment(com.itextpdf.text.Element.ALIGN_CENTER);
			document.add(img);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk fifthMainHeaddings = new Chunk("PiLog Conclusion on " + analysisType + " Analysis:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			fifthMainHeaddings.setBackground(backgroundBaseColor, 0, 3, 362, 3);
			Paragraph thirdMainHeaddingvalues1 = new Paragraph(fifthMainHeaddings);
			thirdMainHeaddingvalues1.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(thirdMainHeaddingvalues1);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph fifthMainHeaddingData = new Paragraph();
			fifthMainHeaddingData.add(new Paragraph("    After analyzing the data, it is observed that\n"));
			fifthMainHeaddingData.add(new Paragraph("    S: Approx. " + request.getParameter("S_INPUT_PCT") + " ("
					+ request.getParameter("S") + ") materials accounting for " + request.getParameter("S_COUNT")
					+ "% i.e., " + request.getParameter("S_INPUT_PCT") + " Millions of the total consumption value\n"));
			fifthMainHeaddingData.add(new Paragraph("    D: Approx. " + request.getParameter("D_INPUT_PCT") + " ("
					+ request.getParameter("D") + ") materials accounting for " + request.getParameter("D_COUNT")
					+ "% i.e., " + request.getParameter("D_INPUT_PCT") + " Millions of the total consumption value\n"));
			fifthMainHeaddingData.add(new Paragraph("    E: Approx. " + request.getParameter("E_INPUT_PCT") + " ("
					+ request.getParameter("E") + ") materials accounting for " + request.getParameter("E_COUNT")
					+ "% i.e., " + request.getParameter("E_INPUT_PCT") + " Millions of the total consumption value\n"));
			document.add(fifthMainHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk thirdMainHeaddingnew2 = new Chunk("7. GProcess of SDE Analysis:\n",
					new Font(customBaseFont, 20, Font.BOLD, BaseColor.WHITE));
			thirdMainHeaddingnew2.setBackground(backgroundBaseColor, 0, 3, 362, 3);
			Paragraph thirdMainHeaddingvalues12 = new Paragraph(thirdMainHeaddingnew2);
			thirdMainHeaddingvalues12.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(thirdMainHeaddingvalues12);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph thirdMainHeaddingnew3 = new Paragraph("Classification of SDE analysis:\n",
					new Font(customBaseFont, 18, Font.BOLD, BaseColor.BLACK));
			thirdMainHeaddingnew3.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(thirdMainHeaddingnew3);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph thirdMainHeaddingnew4 = new Paragraph(
					"SDE analysis classification represents three levels are Scarce, difficult and easy.\n"
							+ "SDE analysis scarce products are usually imported (from other places) take longer to arrive and the supply in harder to come. This slows down the process whether it be in customs "
							+ "or through regulatory bodies.\n",
					new Font(customBaseFont, 16, Font.NORMAL, BaseColor.BLACK));
			thirdMainHeaddingnew4.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(thirdMainHeaddingnew4);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph thirdMainHeaddingnew6 = new Paragraph("Scarce (S):\n",
					new Font(customBaseFont, 18, Font.BOLD, BaseColor.BLACK));
			thirdMainHeaddingnew6.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(thirdMainHeaddingnew6);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph ninethMainHeaddingValue4 = new Paragraph(
					"Items which are imported and those items which require more than 6 months lead time.\n"
							+ "Next is the difficult classification. This refers to items that could be available domestically but they are still harder to get hold of "
							+ "in the market. The lead time to acquire these goods might be two weeks but is generally less than six months.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(ninethMainHeaddingValue4);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph thirdMainHeaddingnew7 = new Paragraph("Difficult (D):\n",
					new Font(customBaseFont, 18, Font.BOLD, BaseColor.BLACK));
			thirdMainHeaddingnew7.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(thirdMainHeaddingnew7);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph ninethMainHeaddingValue5 = new Paragraph(
					"Items which require more than a within a day but less than 6 months lead time.\n"
							+ "Lastly easily identifies inventory that is readily available and easy to access. These items are available locally "
							+ "and can be procured quickly.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(ninethMainHeaddingValue5);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph thirdMainHeaddingnew8 = new Paragraph("Easily Available (E):\n",
					new Font(customBaseFont, 18, Font.BOLD, BaseColor.BLACK));
			thirdMainHeaddingnew8.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(thirdMainHeaddingnew8);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph ninethMainHeaddingValue6 = new Paragraph(
					"Items which require more than a within a day but less than 6 months lead time.\n"
							+ "Lastly easily identifies inventory that is readily available and easy to access. These items are available locally "
							+ "and can be procured quickly.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(ninethMainHeaddingValue6);
			document.add(new Paragraph(Chunk.NEWLINE));
			PdfPTable reportTable1 = new PdfPTable(3);
			reportTable1.addCell("S-Scarce");
			reportTable1.addCell(
					"Items which are imported and require longer lead time ie., more than 6 months (180+ Days)");
			reportTable1.addCell("Longer Lead Time");
			reportTable1.addCell("D - Difficult");
			reportTable1.addCell(
					"Items which require more than a fortnight to be available, but less than 6 months’ lead time (between 1 to 180 Days)");
			reportTable1.addCell("Long Lead Time");
			reportTable1.addCell("E -Easy");
			reportTable1.addCell("Items which are easily available in less than a fortnight");
			reportTable1.addCell("Reasonable Lead Time");
			document.add(reportTable1);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk recommendedAnalysis = new Chunk("Recommended Other PiLog Products :",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.WHITE));
			recommendedAnalysis.setBackground(backgroundBaseColor, 0, 3, 362, 3);
			Paragraph thirdMainHeaddingvalues22 = new Paragraph(recommendedAnalysis);
			thirdMainHeaddingvalues22.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(thirdMainHeaddingvalues22);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor = new Anchor("VED Analysis (Based on Criticality & Impact)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=VED_ANALYSIS_TAB&highLevelMenu=VED_ANALYSIS");
			document.add(anchor);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor1 = new Anchor("SDE Analysis (Based on Lead Time & Availability of Items)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor1.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=SDE_ANALYSIS_TAB&highLevelMenu=SDE_ANALYSIS");
			document.add(anchor1);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor2 = new Anchor("HML Analysis (Based on Unit Price of the Material)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor2.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=HML_ANALYSIS_TAB&highLevelMenu=HML_ANALYSIS");
			document.add(anchor2);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor3 = new Anchor("FMSN Analysis (Based on Consumption Rate)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor3.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=FMSN_ANALYSIS_TAB&highLevelMenu=FMSN_ANALYSIS");
			document.add(anchor3);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor10 = new Anchor("XYZ Analysis (Based on Stock Value Accumulation)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor10.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=XYZ_ANALYSIS_TAB&highLevelMenu=XYZ_ANALYSIS");
			document.add(anchor10);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor4 = new Anchor("Overview of Vendor Data Profiling.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor4.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=VENDOR_DATA_PROFILING_TABS&highLevelMenu=VENDOR_DATA_PROFILING");
			document.add(anchor4);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor5 = new Anchor("Overview of Master Data Profiling.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor5.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=MATERIAL_DATA_PROFILING_TABS&highLevelMenu=MATERIAL_DATA_PROFILING");
			document.add(anchor5);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor6 = new Anchor("Overview of Customer Data Profiling.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor6.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=CUSTOMER_DATA_PROFILING_TABS&highLevelMenu=CUSTOMER_DATA_PROFILING");
			document.add(anchor6);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor7 = new Anchor("ISIC Classification Allocation\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor7.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=ISIC_ALLOCATION_TABS&highLevelMenu=ISIC_ALLOCATION");
			document.add(anchor7);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor8 = new Anchor("HSN Code Allocation\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor8.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=HSN_ALLOCATION_TABS&highLevelMenu=HSN_ALLOCATION");
			document.add(anchor8);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph endingLine = new Paragraph(
					"              Properly managing inventory can make or break a business, and inventory planning is essential to this process. Having insight into your stock at any given"
							+ " moment is critical to success. Decision-makers know they need the right analytical tools in place so they can manage their inventory effectively.",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(endingLine);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph endingquote = new Paragraph("                           \"A DOLLAR SAVED IS  DOLLAR EARNED\"\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLACK));
			document.add(endingquote);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph endingquote2 = new Paragraph(
					"\"As renowned partner of choice, we are here to assist in journey to excel\"",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLACK));
			document.add(endingquote2);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph endingquote3 = new Paragraph(
					"Learn more about how you can explore the Digital Analytical Tools in our \"Data Quality Hub\" to transform your Business",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(endingquote3);
			document.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void vedPdfData(String imageStr, HttpServletRequest request, com.itextpdf.text.Document document,
			String analysisType, BaseFont customBaseFont) {
		try {
			BaseColor backgroundBaseColor = new BaseColor(64, 135, 94);// green
//            BaseColor backgroundBaseColor=new BaseColor(0, 113, 197);//blue
			PdfPCell cell = new PdfPCell();
			Paragraph paragraph = new Paragraph("Hello World");
			cell.addElement(paragraph);
			cell.setBackgroundColor(BaseColor.RED);
//            Font f = new Font(customBaseFont, 16.0f, Font.BOLD, BaseColor.WHITE);

			Chunk firstMainHeaddingChunk = new Chunk("1.Introduction :",
					new Font(customBaseFont, 16.0f, Font.BOLD, BaseColor.WHITE));
			firstMainHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 403, 3);
			Paragraph firstMainHeadding = new Paragraph(firstMainHeaddingChunk);
			firstMainHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(firstMainHeadding);// onkar 168, 162, 64
			document.add(cell);
			document.add(new Paragraph(Chunk.NEWLINE));
			// onkar color
			Chunk firstSubHeaddingChunk = new Chunk("Material Criticality Analysis",
					new Font(customBaseFont, 14.0f, Font.BOLD, BaseColor.WHITE));
			firstSubHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 343, 3);
			// color
			Paragraph firstSubHeadding = new Paragraph(firstSubHeaddingChunk);
//            Paragraph firstSubHeadding = new Paragraph("Material Criticality Analysis\n", new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLACK));
			firstSubHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(firstSubHeadding);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph firstMainHeaddingData = new Paragraph();
			firstMainHeaddingData.add(new Paragraph(
					"   Material Criticality and Inventory Analysis is performed in order to determine the criticality, consumption value, consumption rate, stock value, "
							+ "lead time, price of single unit & frequency Inventory management is the ongoing process of moving parts and products into and out of a company’s location(s). Companies manage "
							+ "their inventory on a daily basis as they place new orders for products and ship orders out to customers. It’s important that business leaders gain a firm grasp of everything "
							+ "involved in the inventory management process. That way, they can figure out creative ways.\n"
							+ "This module allows the users to easily view inventory movement, usage and trends. Users can analyze inventory sales over a specified time frame and make decisions on how to best "
							+ "adjust item resource planning values based on sales averages and months availability. The process of understanding the moving parts & products combined with the knowledge of the "
							+ "demand for stock/product. It is the technique to determine the optimum level of inventory for a firm\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(firstMainHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph firstSmallSubHeadingData = new Paragraph(
					"The following are the various methods to control the inventory process:",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
//            firstSmallSubHeadingData.setAlignment(Element.ALIGN_LEFT);
			document.add(firstSmallSubHeadingData);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings1 = new Paragraph();
			Font zapfdingbats = new Font();
			Chunk bullet = new Chunk("\u2022", zapfdingbats);
			bulletMainHeaddings1.add(bullet);
			bulletMainHeaddings1.add(new Phrase(" ABC Analysis - Based on consumption value per year\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings1);
//            document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletSubHeadding1 = new Paragraph(
					"        It classifies the materials based on their consumption during a particular   time period (usually one year)",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding1);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings2 = new Paragraph();
			bulletMainHeaddings2.add(bullet);
			bulletMainHeaddings2.add(new Phrase(" FMSN Analysis - Based on consumption rate\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings2);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding2 = new Paragraph(
					"        It is based on the rate of issue or rate of usage of spare parts. This classification system categorizes the items based on how frequently"
							+ " the parts are issued and how frequently they are used",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding2);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings3 = new Paragraph();
			bulletMainHeaddings3.add(bullet);
			bulletMainHeaddings3.add(new Phrase(" XYZ Analysis - Based on stock value accumulation\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings3);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding3 = new Paragraph(
					"        It classifies the materials based on stock value accumulation. It is calculated by dividing an item's current stock value by the "
							+ "total stock value of the stores.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding3);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings4 = new Paragraph();
			bulletMainHeaddings4.add(bullet);
			bulletMainHeaddings4.add(new Phrase(" VED Analysis - Based on criticality & impact\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings4);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding4 = new Paragraph(
					"        It classifies the materials according to their criticality and impact to the production process or other services i.e. how and to what extent the material "
							+ "M1 is going to effect the production if the material M1 is not available.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding4);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings5 = new Paragraph();
			bulletMainHeaddings5.add(bullet);
			bulletMainHeaddings5.add(new Phrase(" HML Analysis - Based on unit cost of material\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings5);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding5 = new Paragraph(
					"        It classifies the materials based on their unit prices. The main objective of this analysis is to minimize the inventory cost such as "
							+ "labor & material cost etc.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding5);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings6 = new Paragraph();
			bulletMainHeaddings6.add(bullet);
			bulletMainHeaddings6.add(new Phrase(" SDE Analysis: Based on Lead Time & Availability of Items\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings6);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding6 = new Paragraph(
					"        It classifies inventory based on how freely available an item or scarce an item is, or the length of its lead time.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding6);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk secondMainHeaddingChunk = new Chunk("2. VED Analysis - Based on criticality & impact\n",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.WHITE));
			secondMainHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 145, 3);
			Paragraph secondMainHeadding = new Paragraph(secondMainHeaddingChunk);
			secondMainHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondMainHeadding);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk secondSubHeaddingChunk = new Chunk("Overview Of VED Analysis:\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.WHITE));
			secondSubHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 350, 3);
			Paragraph secondSubHeadding = new Paragraph(secondSubHeaddingChunk);
			secondSubHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondSubHeadding);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph secondMainHeaddingData = new Paragraph();
			secondMainHeaddingData.add(new Paragraph(
					"VED analysis classifies the materials according to their criticality and impact to the production "
							+ "process or other services i.e. how and to what extent the material M1 is going to effect the production if the material M1 is not "
							+ "available.\n"
							+ "It finds out which materials and parts are valuable, which are essential and which are desirable. It is best suited for spares "
							+ "inventory & determine the stocking levels of spare parts.\n"
							+ "VED analysis is a concept used in the management of industries and companies dealing with production activities to classify the "
							+ "items based on their requirement.\n"
							+ "VITAL items are those without which the activities of an industry or organization would come to get seriously affected.\n"
							+ "ESSENTIAL items are those without which the organization may have to face risks. However essential items will have to be "
							+ "provided or replaced by the industry within a timeline.\n"
							+ "DESIRABLE items category includes those materials absence or shortage may cause minor disturbances for a shorter period within "
							+ "the production lane.\n"
							+ "In addition to the ABC analysis VED also plays an important role in materials management. VED ranking may be done on the basis "
							+ "of the shortages costs of materials which can be either qualified or qualitatively expressed.\n"
							+ "VED analysis is used to understand the functional utility of a material item and the effect of non-availability of an item to "
							+ "identify that are considered as not very important and materials which are available for replacement can be postponed.\n"
							+ "VED analysis is very helpful in commercial mediation related to contracts dealing with construction, supply of goods, "
							+ "completion of project etc.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(secondMainHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			PdfPTable dataTable = new PdfPTable(2);
			dataTable.addCell("V-Vital");
			dataTable.addCell("Average Working Hours: >= 4 Hours");
			dataTable.addCell("E-Essential");
			dataTable.addCell("Average Working Hours: >=1, < 4 Hours");
			dataTable.addCell("D-Desirable");
			dataTable.addCell("Average Working Hours: < 1 Hour");
			document.add(dataTable);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk fourthSubHeading = new Chunk("2.2 Steps Performed on VED Analysis:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			fourthSubHeading.setBackground(backgroundBaseColor, 0, 3, 245, 3);
			Paragraph secondSubHeaddinga = new Paragraph(fourthSubHeading);
			secondSubHeaddinga.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondSubHeaddinga);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph fourthSubHeadingData = new Paragraph();
			fourthSubHeadingData.add(new Paragraph("    1) Identification of factors essential for VED Analysis.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			fourthSubHeadingData.add(new Paragraph("    2) Calculate the sum of Actual working hours.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			fourthSubHeadingData.add(new Paragraph("    3) Finding out how many times the order was placed.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			fourthSubHeadingData.add(new Paragraph("    4) Calculate the average working hours.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			fourthSubHeadingData.add(new Paragraph("    5) Place the items into V, E & D Categories.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(fourthSubHeadingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk fifthMainHeadding = new Chunk("3. Categorization of VED Analysis:\n",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.WHITE));
			fifthMainHeadding.setBackground(backgroundBaseColor, 0, 3, 245, 3);
			Paragraph secondSubHeadding1 = new Paragraph(fifthMainHeadding);
			secondSubHeadding1.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondSubHeadding1);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph ninethMainHeaddingValues = new Paragraph(
					"VED Analysis can categorize the inventory materials under three heads which are VITAL, "
							+ "ESSENTIAL AND DESIRABLE.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(ninethMainHeaddingValues);
			Paragraph thirdMainHeaddingnew6 = new Paragraph("V - Vital Category:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLACK));
			thirdMainHeaddingnew6.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(thirdMainHeaddingnew6);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph ninethMainHeaddingValue4 = new Paragraph(
					"VITAL category suggests that “Vital” includes the inventory which is necessary for production "
							+ "or any other process in an organization. The shortage of items under this category can severely impacts or disturb in the proper "
							+ "operational functions.\n"
							+ "Stores or Production employees continuous keeps on checking, evaluation and replenishment happen in such stock materials. If any of "
							+ "such inventories are unavailable the entire production chain may stop and also a missing essential component may be of need at the "
							+ "time of a breakdown.\n"
							+ "VITAL items are always keeps in stock and ordering such inventory should be before hand. Effective cross checking should be put in "
							+ "place by the management to ensure the continuous availability of items under the “vital” category.",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(ninethMainHeaddingValue4);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph thirdMainHeaddingnew7 = new Paragraph("E - Essential category:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLACK));
			thirdMainHeaddingnew7.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(thirdMainHeaddingnew7);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph ninethMainHeaddingValue5 = new Paragraph(
					"IEssential category inventory items includes which is next to be place vital. These category "
							+ "materials are also very important for organization because it may lead to a stoppage of production or effects on some other process "
							+ "in the organization.\n"
							+ "Due to unavailability of essential inventory may get loss on temporary basis or it might be possible to repair the stock item or part "
							+ "of such materials.\n"
							+ "Management of organizations should ensure optimum availability and maintenance of inventory under the “Essential” category. "
							+ "The unavailability of essential category inventory should not cause any stoppage or delays.",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(ninethMainHeaddingValue5);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph thirdMainHeaddingnew8 = new Paragraph("D- Desirable category:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLACK));
			thirdMainHeaddingnew8.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(thirdMainHeaddingnew8);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph ninethMainHeaddingValue6 = new Paragraph(
					"Desirable category of inventory items is the least important among VED and the unavailability "
							+ "of desirable material may result in minor stoppages in production or other processes. However, the  easy replenishment of such "
							+ "shortages is possible in a short duration of time.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(ninethMainHeaddingValue6);
			document.add(new Paragraph(Chunk.NEWLINE));
			PdfPTable dataTable1 = new PdfPTable(2);
			dataTable1.addCell("V-Vital");
			dataTable1.addCell(
					"The spares stock out of which even for a short time will stop production for quite some time & future the cost of stock out is very high");
			dataTable1.addCell("Average Working Hours: >= 4 Hours");
			dataTable1.addCell("E-Essential");
			dataTable1.addCell(
					"The spares stock out of which even for a few hours of days & cost of lost production is high");
			dataTable1.addCell("Average Working Hours: >=1, < 4 Hours");
			dataTable1.addCell("D-Desirable");
			dataTable1.addCell(
					"The spares which are needed but their absence for even for even a week or so will not lead to stoppage of production");
			dataTable1.addCell("Average Working Hours: < 1 Hour");
			document.add(dataTable1);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk thirdMainHeadding = new Chunk("4. Benefits of VED analysis\n",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.WHITE));
			thirdMainHeadding.setBackground(backgroundBaseColor, 0, 3, 303, 3);
			Paragraph secondSubHeadding12 = new Paragraph(thirdMainHeadding);
			secondSubHeadding12.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondSubHeadding12);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph thirdMainHeaddingData = new Paragraph(
					"VED stands for vital, essential and desirable. this analysis is used for classifying each "
							+ "stock (mainly spare parts) into above three category based on their requirement and stocked accordingly.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(thirdMainHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph thirdMainHeaddingDataS = new Paragraph(
					"It helps in classifying items into three category and stocked accordingly. Vital items are "
							+ "stocked more, medium amount of essentials and least amount of desirable item are stocked.\n"
							+ "it brings valuation in stock maintenance as each item need not to be stocked in a very large quantity of material items.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(thirdMainHeaddingDataS);
			document.add(new Paragraph(Chunk.NEWLINE));
			PdfPTable reportTable = new PdfPTable(3);
			reportTable.addCell("Criticality");
			reportTable.addCell("Count");
			reportTable.addCell("Count %");
			reportTable.addCell("V-Vital");
			reportTable.addCell(request.getParameter("V"));
			reportTable.addCell(request.getParameter("V_Count"));
			reportTable.addCell("E-Essential");
			reportTable.addCell(request.getParameter("E"));
			reportTable.addCell(request.getParameter("E_Count"));
			reportTable.addCell("D-Desirable");
			reportTable.addCell(request.getParameter("D"));
			reportTable.addCell(request.getParameter("D_Count"));
			reportTable.addCell("Total");
			reportTable.addCell(String.valueOf(Integer.parseInt(request.getParameter("V"))
					+ Integer.parseInt(request.getParameter("E")) + Integer.parseInt(request.getParameter("D"))));
			reportTable.addCell("");
			reportTable.addCell("");
			document.add(reportTable);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk thirdMainHeaddingnew1 = new Chunk("5. Graphical Representation of SDE Analysis:\n",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.WHITE));
			thirdMainHeaddingnew1.setBackground(backgroundBaseColor, 0, 3, 175, 3);
			Paragraph secondSubHeadding22 = new Paragraph(thirdMainHeaddingnew1);
			secondSubHeadding22.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondSubHeadding22);
			document.add(new Paragraph(Chunk.NEWLINE));
			Image img = getImage(imageStr);
			img.scalePercent(40);
			img.setAlignment(com.itextpdf.text.Element.ALIGN_CENTER);
			document.add(img);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk fifthMainHeaddings = new Chunk("PiLog Conclusion on " + analysisType + " Analysis:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			fifthMainHeaddings.setBackground(backgroundBaseColor, 0, 3, 275, 3);
			Paragraph secondSubHeadding234 = new Paragraph(fifthMainHeaddings);
			secondSubHeadding234.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondSubHeadding234);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph fifthMainHeaddingData = new Paragraph();
			fifthMainHeaddingData.add(new Paragraph("    After analyzing the data, it is observed that\n"));
			fifthMainHeaddingData.add(new Paragraph("    V: Approx. " + request.getParameter("V_COUNT")
					+ " of the spares are very critical, failure of which leads to stoppage of production for more than 4 hours"));
			fifthMainHeaddingData.add(new Paragraph("    E: Approx. " + request.getParameter("E_COUNT")
					+ " of the spares are critical for which lost production is high which leads to stoppage of production for 1-4 hours"));
			fifthMainHeaddingData.add(new Paragraph("    D: Approx. " + request.getParameter("D_COUNT")
					+ " of the spares are non-critical which lead to short stop or production stop for less than an hour."));
			document.add(fifthMainHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk thirdMainHeaddingnew2 = new Chunk("6. Importance of VED Analysis:\n",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.WHITE));
			thirdMainHeaddingnew2.setBackground(backgroundBaseColor, 0, 3, 265, 3);
			Paragraph secondSubHeadding23 = new Paragraph(thirdMainHeaddingnew2);
			secondSubHeadding23.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondSubHeadding23);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeadding1 = new Paragraph();
			bulletMainHeadding1.add(bullet);
			bulletMainHeadding1.add(new Phrase(
					" It is of utmost importance to any organization to maintain an optimum level of inventory.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeadding1);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeadding2 = new Paragraph();
			bulletMainHeadding2.add(bullet);
			bulletMainHeadding2.add(new Phrase(
					" VED analysis is a crucial tool to understand and categorize inventory according to its importance..\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeadding2);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeadding3 = new Paragraph();
			bulletMainHeadding3.add(bullet);
			bulletMainHeadding3.add(new Phrase(
					" Management can optimize costs by investing more in the vital and essential category materials..\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeadding3);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeadding4 = new Paragraph();
			bulletMainHeadding4.add(bullet);
			bulletMainHeadding4.add(new Phrase(
					" Maintaining inventory has its costs, and hence, this analysis bifurcates inventory in three parts to help "
							+ "in managerial decisions on inventory maintenance..\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeadding4);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeadding5 = new Paragraph();
			bulletMainHeadding5.add(new Phrase(" There are three types of costs to maintain stock which are:\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeadding5);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeadding6 = new Paragraph();
			bulletMainHeadding6
					.add(new Phrase("Item Cost \n", new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeadding6);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeadding7 = new Paragraph();
			bulletMainHeadding7.add(bullet);
			bulletMainHeadding7.add(new Phrase(
					" It is the actual purchase value of holding stock. This is the cost or price of the inventory items. "
							+ "It will be high with more inventory.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeadding7);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeadding8 = new Paragraph();
			bulletMainHeadding8.add(new Phrase("Ordering / Set-up Cost \n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeadding8);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeadding9 = new Paragraph();
			bulletMainHeadding9.add(bullet);
			bulletMainHeadding9.add(new Phrase(
					" These may include transportation charges, packing charges, etc. "
							+ "The purchase of inventory involves certain costs.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeadding9);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeadding10 = new Paragraph();
			bulletMainHeadding10
					.add(new Phrase("Holding Costs \n", new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeadding10);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeadding11 = new Paragraph();
			bulletMainHeadding11.add(bullet);
			bulletMainHeadding11.add(new Phrase(
					" After the purchase of inventory items, there are classified into few more costs. These may be related "
							+ "to storage, insurance charges of stock or inventory, labor costs associated with the handling of stock, etc. It includes any damage,"
							+ " leakage of the stock in hand..\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeadding11);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk thirdMainHeaddingnew3 = new Chunk("Classification of VED analysis:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			thirdMainHeaddingnew3.setBackground(backgroundBaseColor, 0, 3, 305, 3);
			Paragraph secondSubHeadding245 = new Paragraph(thirdMainHeaddingnew3);
			secondSubHeadding245.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondSubHeadding245);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings = new Paragraph();
			bulletMainHeaddings
					.add(new Phrase("Vital: \n", new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddingv1 = new Paragraph();
			bulletMainHeaddingv1.add(bullet);
			bulletMainHeaddingv1.add(new Phrase(
					" Vital category includes the inventory items which is necessary for production or any other process in "
							+ "an organization.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeadding11);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddingv2 = new Paragraph();
			bulletMainHeaddingv2.add(bullet);
			bulletMainHeaddingv2
					.add(new Phrase(
							" If the inventory of Vital item is unavailable in the market entire production may "
									+ "effects or stopped.\n",
							new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddingv2);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddingv3 = new Paragraph();
			bulletMainHeaddingv3.add(bullet);
			bulletMainHeaddingv3.add(new Phrase(
					" Vital items are very most important in the production process ordering such items should be "
							+ "beforehand while its finishes in the warehouse.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddingv3);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddingse1 = new Paragraph();
			bulletMainHeaddingse1
					.add(new Phrase("Essential : \n", new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddingse1);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddingse2 = new Paragraph();
			bulletMainHeaddingse2.add(bullet);
			bulletMainHeaddingse2.add(new Phrase(
					" Essential category of inventory items can be classified as the next to Vital and these materials "
							+ "are also important for any of the companies which are handling the maintenance tasks.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddingse2);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddingse3 = new Paragraph();
			bulletMainHeaddingse3.add(bullet);
			bulletMainHeaddingse3.add(new Phrase(
					" Essential materials are also keeping in stock always loss due to their unavailability may be "
							+ "temporary.it might be possible to repair the stock item or part of the machinery.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddingse3);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddingse4 = new Paragraph();
			bulletMainHeaddingse4.add(bullet);
			bulletMainHeaddingse4.add(new Phrase(
					" Ensure the essential material items are always in optimum availability and maintenance.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddingse4);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddingsd = new Paragraph();
			bulletMainHeaddingsd
					.add(new Phrase("Desirable : \n", new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddingsd);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddingsd1 = new Paragraph();
			bulletMainHeaddingsd1.add(bullet);
			bulletMainHeaddingsd1.add(
					new Phrase(" Desirable category of inventory materials is the least important among the VED.\n",
							new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddingsd1);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddingsd2 = new Paragraph();
			bulletMainHeaddingsd2.add(bullet);
			bulletMainHeaddingsd2
					.add(new Phrase(" Unavailability of Desirable material may cause minor stoppages in production.\n",
							new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddingsd2);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddingsd3 = new Paragraph();
			bulletMainHeaddingsd3.add(bullet);
			bulletMainHeaddingsd3.add(new Phrase(
					" The easiest way of maintaining these material shortages is possible in a short duration of time.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddingsd3);
			document.add(new Paragraph(Chunk.NEWLINE));
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk recommendedAnalysis = new Chunk("Recommended Other PiLog Products :",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.WHITE));
			recommendedAnalysis.setBackground(backgroundBaseColor, 0, 3, 215, 3);
			Paragraph secondSubHeadding24 = new Paragraph(recommendedAnalysis);
			secondSubHeadding24.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondSubHeadding24);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor = new Anchor("VED Analysis (Based on Criticality & Impact)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=VED_ANALYSIS_TAB&highLevelMenu=VED_ANALYSIS");
			document.add(anchor);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor1 = new Anchor("SDE Analysis (Based on Lead Time & Availability of Items)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor1.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=SDE_ANALYSIS_TAB&highLevelMenu=SDE_ANALYSIS");
			document.add(anchor1);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor2 = new Anchor("HML Analysis (Based on Unit Price of the Material)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor2.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=HML_ANALYSIS_TAB&highLevelMenu=HML_ANALYSIS");
			document.add(anchor2);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor3 = new Anchor("FMSN Analysis (Based on Consumption Rate)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor3.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=FMSN_ANALYSIS_TAB&highLevelMenu=FMSN_ANALYSIS");
			document.add(anchor3);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor10 = new Anchor("XYZ Analysis (Based on Stock Value Accumulation)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor10.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=XYZ_ANALYSIS_TAB&highLevelMenu=XYZ_ANALYSIS");
			document.add(anchor10);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor4 = new Anchor("Overview of Vendor Data Profiling.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor4.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=VENDOR_DATA_PROFILING_TABS&highLevelMenu=VENDOR_DATA_PROFILING");
			document.add(anchor4);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor5 = new Anchor("Overview of Master Data Profiling.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor5.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=MATERIAL_DATA_PROFILING_TABS&highLevelMenu=MATERIAL_DATA_PROFILING");
			document.add(anchor5);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor6 = new Anchor("Overview of Customer Data Profiling.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor6.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=CUSTOMER_DATA_PROFILING_TABS&highLevelMenu=CUSTOMER_DATA_PROFILING");
			document.add(anchor6);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor7 = new Anchor("ISIC Classification Allocation\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor7.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=ISIC_ALLOCATION_TABS&highLevelMenu=ISIC_ALLOCATION");
			document.add(anchor7);
			document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor8 = new Anchor("HSN Code Allocation\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor8.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=HSN_ALLOCATION_TABS&highLevelMenu=HSN_ALLOCATION");
			document.add(anchor8);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph endingLine = new Paragraph(
					"              Properly managing inventory can make or break a business, and inventory planning is essential to this process. Having insight into your stock at any given"
							+ " moment is critical to success. Decision-makers know they need the right analytical tools in place so they can manage their inventory effectively.",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(endingLine);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph endingquote = new Paragraph("                           \"A DOLLAR SAVED IS  DOLLAR EARNED\"\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLACK));
			document.add(endingquote);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph endingquote2 = new Paragraph(
					"\"As renowned partner of choice, we are here to assist in journey to excel\"",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLACK));
			document.add(endingquote2);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph endingquote3 = new Paragraph(
					"Learn more about how you can explore the Digital Analytical Tools in our \"Data Quality Hub\" to transform your Business",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(endingquote3);

			document.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void acaPdfData(String imageStr, HttpServletRequest request, com.itextpdf.text.Document document,
			String analysisType, BaseFont customBaseFont) {
		try {
			BaseColor backgroundBaseColor = new BaseColor(64, 135, 94);// green
//            BaseColor backgroundBaseColor=new BaseColor(0, 113, 197);//blue
			PdfPCell cell = new PdfPCell();
			Paragraph paragraph = new Paragraph("Hello World");
			cell.addElement(paragraph);
			cell.setBackgroundColor(BaseColor.RED);
//            Font f = new Font(customBaseFont, 16.0f, Font.BOLD, BaseColor.WHITE);

			Chunk firstMainHeaddingChunk = new Chunk("1.Introduction :",
					new Font(customBaseFont, 16.0f, Font.BOLD, BaseColor.WHITE));
			firstMainHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 403, 3);
			Paragraph firstMainHeadding = new Paragraph(firstMainHeaddingChunk);
			firstMainHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(firstMainHeadding);// onkar 168, 162, 64
			document.add(cell);
			document.add(new Paragraph(Chunk.NEWLINE));
			// onkar color
			Chunk firstSubHeaddingChunk = new Chunk("Equipment Criticality Analysis",
					new Font(customBaseFont, 14.0f, Font.BOLD, BaseColor.WHITE));
			firstSubHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 343, 3);
			// color
			Paragraph firstSubHeadding = new Paragraph(firstSubHeaddingChunk);
//            Paragraph firstSubHeadding = new Paragraph("Material Criticality Analysis\n", new Font(customBaseFont, 14, Font.BOLD, BaseColor.BLACK));
			firstSubHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(firstSubHeadding);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph firstMainHeaddingData = new Paragraph();
			firstMainHeaddingData.add(new Paragraph(
					"   Equipment critical analysis is a quantitative analysis of equipment faults, and ranking them in order of serious consequences on safety, environment, production loss and maintenance cost. "
							+ "The key benefit of this analysis is to provide the means to recognize high-criticality vs. low-criticality equipment, reduce the level of uncertainty and focus on high-priority maintenance tasks."
							+ "The analysis also helps select the best and most economic maintenance strategy, prioritize work orders and decide on insurance and the demand on spare parts\n "
							+ "Cross-functional analysis and input from Operations and Maintenance, and as required, input from Engineering, Material Management and Health and Safety representatives\n"
							+ "Purpose of Equipment Criticality Analysis",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(firstMainHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph firstSmallSubHeadingData = new Paragraph(
					"The following are the various methods to control the inventory process:",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
//            firstSmallSubHeadingData.setAlignment(Element.ALIGN_LEFT);
			document.add(firstSmallSubHeadingData);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings1 = new Paragraph();
			Font zapfdingbats = new Font();
			Chunk bullet = new Chunk("\u2022", zapfdingbats);
			bulletMainHeaddings1.add(bullet);
			bulletMainHeaddings1.add(new Phrase(" ABC Analysis - Based on consumption value per year\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings1);
//            document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph bulletSubHeadding1 = new Paragraph(
					"        It classifies the materials based on their consumption during a particular   time period (usually one year)",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding1);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings2 = new Paragraph();
			bulletMainHeaddings2.add(bullet);
			bulletMainHeaddings2.add(new Phrase(" FMSN Analysis - Based on consumption rate\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings2);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding2 = new Paragraph(
					"        It is based on the rate of issue or rate of usage of spare parts. This classification system categorizes the items based on how frequently"
							+ " the parts are issued and how frequently they are used",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding2);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings3 = new Paragraph();
			bulletMainHeaddings3.add(bullet);
			bulletMainHeaddings3.add(new Phrase(" XYZ Analysis - Based on stock value accumulation\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings3);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding3 = new Paragraph(
					"        It classifies the materials based on stock value accumulation. It is calculated by dividing an item's current stock value by the "
							+ "total stock value of the stores.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding3);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings4 = new Paragraph();
			bulletMainHeaddings4.add(bullet);
			bulletMainHeaddings4.add(new Phrase(" VED Analysis - Based on criticality & impact\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings4);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding4 = new Paragraph(
					"        It classifies the materials according to their criticality and impact to the production process or other services i.e. how and to what extent the material "
							+ "M1 is going to effect the production if the material M1 is not available.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding4);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings5 = new Paragraph();
			bulletMainHeaddings5.add(bullet);
			bulletMainHeaddings5.add(new Phrase(" HML Analysis - Based on unit cost of material\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings5);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding5 = new Paragraph(
					"        It classifies the materials based on their unit prices. The main objective of this analysis is to minimize the inventory cost such as "
							+ "labor & material cost etc.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding5);
			document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletMainHeaddings6 = new Paragraph();
			bulletMainHeaddings6.add(bullet);
			bulletMainHeaddings6.add(new Phrase(" SDE Analysis: Based on Lead Time & Availability of Items\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(bulletMainHeaddings6);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph bulletSubHeadding6 = new Paragraph(
					"        It classifies inventory based on how freely available an item or scarce an item is, or the length of its lead time.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(bulletSubHeadding6);
			document.add(new Paragraph(Chunk.NEWLINE));
			Chunk secondMainHeaddingChunk = new Chunk("2. Equipment Criticality Analysis\n",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.WHITE));
//            c.setBackground(backgroundBaseColor);
			secondMainHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 407, 3);
			Paragraph secondMainHeadding = new Paragraph(secondMainHeaddingChunk);
			secondMainHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondMainHeadding);
			document.add(new Paragraph(Chunk.NEWLINE));
			// bg color
			Chunk secondSubHeaddingChunk = new Chunk("The Equipment Criticality Analysis is used to identify:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			secondSubHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 350, 3);
			Paragraph secondSubHeadding = new Paragraph(secondSubHeaddingChunk);
			secondSubHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(secondSubHeadding);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph secondMainHeaddingData = new Paragraph();
			secondMainHeaddingData.add(new Paragraph(
					"    Which equipment has the most serious potential consequences on business performance, 'if it fails'? The resulting Equipment Criticality Number is used to prioritize resources performing maintenance work.\n"
							+ "Identify what equipment is most likely to negatively impact business performance because it both matters a lot when it fails and it fails too often. The resulting Relative Risk Number is used to identify candidate assets for reliability improvement.\n"
							+ "This table summarizes the key decisions for ABC analysis:\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(secondMainHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			PdfPTable rowsDataTable = new PdfPTable(2);
			rowsDataTable.setTotalWidth(100f);

//

			String equiCriticalityRankingImage = request.getServletContext()
					.getRealPath("/images/Equipment_Criticality_Ranking.png");
			Image ecaimage = Image.getInstance(equiCriticalityRankingImage);
			// ecaimage.setAbsolutePosition(200f, 200f);
			ecaimage.scalePercent(40);
			ecaimage.setAlignment(com.itextpdf.text.Element.ALIGN_CENTER);
			document.add(new Paragraph("Some text before the image"));
			document.add(ecaimage);
			// bg color

			document.add(new Paragraph(Chunk.NEXTPAGE));

			Chunk thirdSubHeaddingChunk = new Chunk("ACA Scoring Matrix \n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			thirdSubHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 365, 3);
			Paragraph thirdSubHeadding = new Paragraph(thirdSubHeaddingChunk);
			thirdSubHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(thirdSubHeadding);
//            document.add(new Paragraph(Chunk.NEWLINE));

			String assCriticalityScoring = request.getServletContext()
					.getRealPath("/images/ASSET_CRITICALITY_SCORING.png");
			Image ecaScoringimage = Image.getInstance(assCriticalityScoring);
			ecaScoringimage.scalePercent(50);
			ecaScoringimage.setAlignment(com.itextpdf.text.Element.ALIGN_CENTER);
			// ecaScoringimage.setAbsolutePosition(400f, 400f);
			document.add(ecaScoringimage);

			document.add(new Paragraph(Chunk.NEWLINE));

			// bg color
			Chunk fourthSubHeadingChunk = new Chunk("Asset Criticality Index Identification\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			fourthSubHeadingChunk.setBackground(backgroundBaseColor, 0, 3, 315, 3);
			Paragraph fourthSubHeading = new Paragraph(fourthSubHeadingChunk);
			fourthSubHeading.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(fourthSubHeading);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph fourthSubHeadingData = new Paragraph();
			fourthSubHeadingData.add(new Paragraph(
					" Weighted criticality criteria scores are multiplied to determine the asset\n"
							+ "Criticality Index ‘CI’ that will provide the total weighted score of the asset or system under analysis. The CI is calculated as follows:\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			fourthSubHeadingData.add(new Paragraph(
					"             Criticality Index\n" + "                       = Assert Failure × Asset Importance\n"
							+ "                                       × Asset Reliability × Asset Utilization ",
					new Font(Font.FontFamily.TIMES_ROMAN, 12, Font.ITALIC, BaseColor.RED)));
			fourthSubHeadingData.add(new Paragraph(
					" ACA scoring criteria ranges from 0, indicating high criticality against the criterion, to 4, indicating low criticality against the criterion\n"
							+ "",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));

			document.add(fourthSubHeadingData);
			document.add(new Paragraph(Chunk.NEWLINE));

			// bg color
			Chunk fifthMainHeaddingChunk = new Chunk("Asset Criticality Class Identification \n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			fifthMainHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 180, 3);
			Paragraph fifthMainHeadding = new Paragraph(fifthMainHeaddingChunk);
			fifthMainHeadding.setAlignment(com.itextpdf.text.Element.ALIGN_LEFT);
			document.add(fifthMainHeadding);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph fifstMainHeaddingData = new Paragraph();
			fifstMainHeaddingData.add(new Paragraph(
					" SAP- Plant Maintenance (SAP-PM) Module, where Class A represents ‘Critical’ asset, Class B represents ‘Important’ asset, and Class C represents ‘Ordinary’ Asset",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK)));
			document.add(fifstMainHeaddingData);
			document.add(new Paragraph(Chunk.NEWLINE));
			PdfPTable dataTable = new PdfPTable(3);
			dataTable.setTotalWidth(100f);
			float[] columnWidths = new float[] { 30f, 30f, 30f };
			dataTable.setWidths(columnWidths);
			// onkar added
			Paragraph cellvalue = new Paragraph();
			cellvalue = new Paragraph("SAP-PM Criticality Class",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));
			dataTable.addCell(cellvalue);
			cellvalue = new Paragraph(" Criticality Index (CI) Range",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));
			dataTable.addCell(cellvalue);
			cellvalue = new Paragraph(" Equipment\n" + "Criticality",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			dataTable.addCell(cellvalue);
			cellvalue = new Paragraph("Class A", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar
																												// added
			dataTable.addCell(cellvalue);
			cellvalue = new Paragraph((String) request.getParameter("C_RANGE"),
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			dataTable.addCell(cellvalue);
			cellvalue = new Paragraph("Critical Equipment", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar
																														// added
			dataTable.addCell(cellvalue);
			cellvalue = new Paragraph("Class B", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar
																												// added
			dataTable.addCell(cellvalue);
			cellvalue = new Paragraph((String) request.getParameter("I_RANGE"),
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			dataTable.addCell(cellvalue);
			cellvalue = new Paragraph("Important Equipment",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			dataTable.addCell(cellvalue);
			cellvalue = new Paragraph("Class C", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar
																												// added
			dataTable.addCell(cellvalue);
			cellvalue = new Paragraph((String) request.getParameter("O_RANGE"),
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			dataTable.addCell(cellvalue);
			cellvalue = new Paragraph("Ordinary Equipment", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar
																														// added
			dataTable.addCell(cellvalue);

			document.add(dataTable);
			document.add(new Paragraph(Chunk.NEXTPAGE));

			Chunk eightMainHeaddingChunk = new Chunk(
					"Sample Graphical Representation Of " + analysisType + " Analysis:\n",
					new Font(customBaseFont, 14, Font.BOLD, BaseColor.WHITE));
			eightMainHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 190, 3);
			Paragraph eightMainHeadding = new Paragraph(eightMainHeaddingChunk);
			document.add(eightMainHeadding);
			document.add(new Paragraph(Chunk.NEWLINE));
			Image img = getImage(imageStr);
			img.scalePercent(70);
			img.setAlignment(com.itextpdf.text.Element.ALIGN_CENTER);
			document.add(img);
			document.add(new Paragraph(Chunk.NEWLINE));

			Paragraph cellData = new Paragraph();
			PdfPTable dataTable1 = new PdfPTable(3);
			cellData = new Paragraph("Equipment Criticality",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));
			dataTable1.addCell(cellData);
			cellData = new Paragraph(" Equipment Count ", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));
			dataTable1.addCell(cellData);
			cellData = new Paragraph(" Equipment Percentage(%)",
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			dataTable1.addCell(cellData);
			cellData = new Paragraph("Critical Equipment", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar
																														// added
			dataTable1.addCell(cellData);
			cellData = new Paragraph((String) request.getParameter("C_Count"),
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			dataTable1.addCell(cellData);
			cellData = new Paragraph((String) request.getParameter("C_Percentage"),
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			dataTable1.addCell(cellData);
			cellData = new Paragraph("Important Equipment", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar
																														// added
			dataTable1.addCell(cellData);
			cellData = new Paragraph((String) request.getParameter("I_Count"),
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			dataTable1.addCell(cellData);
			cellData = new Paragraph((String) request.getParameter("I_Percentage"),
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			dataTable1.addCell(cellData);
			cellData = new Paragraph("Ordinary Equipment", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar
																														// added
			dataTable1.addCell(cellData);
			cellData = new Paragraph((String) request.getParameter("O_Count"),
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			dataTable1.addCell(cellData);
			cellData = new Paragraph((String) request.getParameter("O_Percentage"),
					new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));// onkar added
			dataTable1.addCell(cellData);

			document.add(dataTable1);
			document.add(new Paragraph(Chunk.NEWLINE));

			document.add(new Paragraph(Chunk.NEWLINE));

			// bg color
			Chunk ninethMainHeaddingChunk = new Chunk("Assets Criticality Index Classification:\n",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.WHITE));
			ninethMainHeaddingChunk.setBackground(backgroundBaseColor, 0, 3, 396, 3);
			Paragraph ninethMainHeadding = new Paragraph(ninethMainHeaddingChunk);
			document.add(ninethMainHeadding);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Paragraph ninethMainHeaddingValue = new Paragraph(
					"A score of zero ‘0’ in any of the ACA criteria, shall immediately rank the asset as a ‘Critical’ asset. Therefore, any HSE or SCE shall automatically be ranked as Critical Asset thru the ACA methodology. Assets identified by risk assessment studies as HSE critical or AIMS SCE shall has major impact on HSE and consequently, by default, will receive a score of 0 for its Failure Consequences in ACA Matrix.",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLACK));
			document.add(ninethMainHeaddingValue);
			document.add(new Paragraph(Chunk.NEWLINE));

			// link started
			document.add(new Paragraph(Chunk.NEXTPAGE));
			Paragraph recommendedAnalysis = new Paragraph("Recommended Other PiLog Products :",
					new Font(customBaseFont, 16, Font.BOLD, BaseColor.BLACK));
			document.add(recommendedAnalysis);
			Anchor anchor = new Anchor("VED Analysis (Based on Criticality & Impact)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=VED_ANALYSIS_TAB&highLevelMenu=VED_ANALYSIS");
			document.add(anchor);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor1 = new Anchor("SDE Analysis (Based on Lead Time & Availability of Items)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor1.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=SDE_ANALYSIS_TAB&highLevelMenu=SDE_ANALYSIS");
			document.add(anchor1);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor2 = new Anchor("HML Analysis (Based on Unit Price of the Material)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor2.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=HML_ANALYSIS_TAB&highLevelMenu=HML_ANALYSIS");
			document.add(anchor2);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor3 = new Anchor("FMSN Analysis (Based on Consumption Rate)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor3.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=FMSN_ANALYSIS_TAB&highLevelMenu=FMSN_ANALYSIS");
			document.add(anchor3);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor10 = new Anchor("XYZ Analysis (Based on Stock Value Accumulation)\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor10.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=XYZ_ANALYSIS_TAB&highLevelMenu=XYZ_ANALYSIS");
			document.add(anchor10);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor4 = new Anchor("Overview of Vendor Data Profiling.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor4.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=VENDOR_DATA_PROFILING_TABS&highLevelMenu=VENDOR_DATA_PROFILING");
			document.add(anchor4);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor5 = new Anchor("Overview of Master Data Profiling.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor5.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=MATERIAL_DATA_PROFILING_TABS&highLevelMenu=MATERIAL_DATA_PROFILING");
			document.add(anchor5);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor6 = new Anchor("Overview of Customer Data Profiling.\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor6.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=CUSTOMER_DATA_PROFILING_TABS&highLevelMenu=CUSTOMER_DATA_PROFILING");
			document.add(anchor6);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor7 = new Anchor("ISIC Classification Allocation\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor7.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=ISIC_ALLOCATION_TABS&highLevelMenu=ISIC_ALLOCATION");
			document.add(anchor7);
//            document.add(new Paragraph(Chunk.NEWLINE));
			Anchor anchor8 = new Anchor("HSN Code Allocation\n",
					new Font(customBaseFont, 14, Font.NORMAL, BaseColor.BLUE));
			anchor8.setReference(
					"https://imdrm.pilog.in/PiLogCloud/cloudTabs?tabId=HSN_ALLOCATION_TABS&highLevelMenu=HSN_ALLOCATION");
			document.add(anchor8);
//            document.add(new Paragraph(Chunk.NEWLINE));

			// SDE Analysis (SDE analysis classifies inventory based on how freely available
			// an item or scarce an item is, or the length of its lead time.)
			// HML Analysis (HML analysis classifies the materials based on their unit
			// prices.)
			document.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String movingToWorkArea(HttpServletRequest request) {
		return dashBoardsDAO.movingToWorkArea(request);
	}

	public String characteristicsUpdate(HttpServletRequest request) {
		return dashBoardsDAO.characteristicsUpdate(request);
	}

	public String RefAndDocUpdate(HttpServletRequest request) {
		return dashBoardsDAO.RefAndDocUpdate(request);
	}

	public String confirmCharacteristicsUpdate(HttpServletRequest request) {
		return dashBoardsDAO.confirmCharacteristicsUpdate(request);
	}

	public String charAutomationAPI(HttpServletRequest request) {
		return dashBoardsDAO.charAutomationAPI(request);
	}

	public String getDataHarmonisationView(HttpServletRequest request) {
		String result = "";
		String tabData = "";
		String tabId = request.getParameter("tabId");
		String roleId = (String) request.getSession(false).getAttribute("ssRole");
		String orgnId = (String) request.getSession(false).getAttribute("ssOrgId");

		try {
			List<Object[]> tabList = dashBoardsDAO.genericTabsOpt(request, tabId, roleId, orgnId);
			StringBuilder resultBuilder = new StringBuilder();
			resultBuilder.append("<section class=\"dhDataParentView \">"
					+ "<div class=\" topFilterRow\"><div class=\"leftWrapperDiv\">"
					+ "<div id='listDataView' class='listViewData'>" + "<ul>");

			StringBuilder liGenerator = new StringBuilder();
			if (tabList != null && !tabList.isEmpty()) {
				for (int i = 0; i < tabList.size(); i++) {
					Object[] tabListArr = tabList.get(i);
					String tabTitle = (String) tabListArr[1];
					String componentType = (String) tabListArr[17];
					String componentId = (String) tabListArr[2];
					String image = (String) tabListArr[16];
					String functionName = (String) tabListArr[15];
					String type = (String) tabListArr[14];
					liGenerator.append("<li id='li_" + type + "' onclick='" + functionName + "(\"" + componentId
							+ "\",\"" + type + "\")'><span class='listDataIcon'><img src='" + image
							+ "' /></span><span class='listDataText' title='" + tabTitle + "'>" + tabTitle
							+ "</span></li>");

				}

				liGenerator.append("</ul></div></div>" + "<div id='importDataView'class=\"rightWrapperDiv\"></div>"
						+ "</div></section>");
				resultBuilder.append(liGenerator);
				result = resultBuilder.toString();
			}

		} catch (Exception e) {
		}
		return result;
	}

	public JSONObject deleteCompleteTable(HttpServletRequest request) {
		return dashBoardsDAO.deleteCompleteTable(request);
	}

	public JSONObject saveHomePageCardResizeDetails(HttpServletRequest request) {
		return dashBoardsDAO.saveHomePageCardResizeDetails(request);
	}

	public String updateChartSeqNos(HttpServletRequest request) {
		return dashBoardsDAO.updateChartSeqNos(request);
	}

	public JSONObject getFileObjectMetaData(HttpServletRequest request, HttpServletResponse response) {
		JSONObject fileMetaObj = new JSONObject();
		try {
			JSONArray dataFieldsArray = new JSONArray();
			JSONArray columnsArray = new JSONArray();
			String filePath = request.getParameter("filePath");
			fileMetaObj.put("filePath", filePath);
			String gridId = request.getParameter("gridId");
			if (filePath != null && !"".equalsIgnoreCase(filePath)) {
				// C:/Files/TreeDMImport/SAN_MGR_MM
//                filePath = etlFilePath+"Files/TreeDMImport" + File.separator + request.getSession(false).getAttribute("ssUsername") + File.separator + filePath;
				String targetFile = request.getParameter("targetFile"); // ravi etl new issues

				if ("Y".equalsIgnoreCase(targetFile)) {
					filePath = etlFilePath + "ETL_EXPORT_" + File.separator
							+ request.getSession(false).getAttribute("ssUsername") + File.separator + filePath;
				} else {
					if (filePath.contains("\\") || filePath.contains("/")) {

					} else {
						filePath = etlFilePath + "Files/TreeDMImport" + File.separator
								+ request.getSession(false).getAttribute("ssUsername") + File.separator + filePath;

					}
				}

			}
			filePath = filePath.trim();
			String fileName = request.getParameter("fileName");
			String fileType = filePath.split("\\.")[1];
//			String fileType = request.getParameter("fileType");
			List<String> headers = getHeadersOfImportedFile(request, filePath);
			Map<String, String> headerDataTypes = new HashMap<>();
			if (fileType != null && fileType != "undefined" && !fileType.equalsIgnoreCase("")
					&& fileType.equalsIgnoreCase("json")) {
				ObjectMapper objectMapper = new ObjectMapper();
				JsonNode rootNode = objectMapper.readTree(new File(filePath));
				headerDataTypes = dashBoardsDAO.extractJSONColumnTypesForJqxGrid(rootNode);
			} else {
				if (fileType != null && fileType != "undefined" && !fileType.equalsIgnoreCase("")
						&& fileType.equalsIgnoreCase("xml")) {
					for (String head : headers) {
						headerDataTypes.put(head, "string");
					}
				} else {
					headerDataTypes = dashBoardsDAO.getDataTypesOFHeader(request);
				}

			}

			if (!(headers != null && !headers.isEmpty())) {
				String fileHeadersStr = request.getParameter("fileHeaders");
				if (fileHeadersStr != null && !"".equalsIgnoreCase(fileHeadersStr)
						&& !"[]".equalsIgnoreCase(fileHeadersStr)) {
					JSONObject fileHeaders = (JSONObject) JSONValue.parse(fileHeadersStr);
					headers = new ArrayList(fileHeaders.values());
				}

			}
			String gridPersonalizeStr = "";
			if (headers != null && !headers.isEmpty()) {
				List<String> columnList = new ArrayList();

				for (int i = 0; i < headers.size(); i++) {
					String header = headers.get(i);
					if (header != null && !"".equalsIgnoreCase(header) && !"".equalsIgnoreCase(header)) {
						JSONObject dataFieldsObj = new JSONObject();
						columnList.add(header.replaceAll("\\s", "_"));
						gridPersonalizeStr += "<tr>" + "<td>" + header + "</td>" + "<td>"
								+ "<input type='checkbox' data-gridid='" + gridId + "' checked id='" + gridId + "_"
								+ header.replaceAll("\\s", "_") + "_DISPLAY' data-type='display' " + " data-colname='"
								+ header.replaceAll("\\s", "_") + "' onchange=\"updateETLPersonalize(id)\"" + "</td>"
								+ "<td>" + "<input type='checkbox' id='" + gridId + "_" + header.replaceAll("\\s", "_")
								+ "_FREEZE' data-gridid='" + gridId + "' data-type='pinned' " + " data-colname='"
								+ header.replaceAll("\\s", "_") + "' onchange=\"updateETLPersonalize(id)\"" + "</td>"
								+ "</tr>";
						dataFieldsObj.put("name", header.replaceAll("\\s", "_"));
						dataFieldsObj.put("type", headerDataTypes.get(header.replaceAll("[^a-zA-Z0-9_]", "_")));

						dataFieldsArray.add(dataFieldsObj);

						JSONObject columnsObject = new JSONObject();

						columnsObject.put("text", header);
						columnsObject.put("datafield", header.replaceAll("\\s", "_"));
						columnsObject.put("width", 120);
						columnsObject.put("sortable", true);
						columnsArray.add(columnsObject);

					}
				}
				gridPersonalizeStr = "<div class=\"personaliseoption visionSearchPersonaliseoption\" style=\"margin-top:5px;\">"
						+ "<div onclick=slideSettingsETL('" + gridId + "') class=\"layoutoptions ui-accordion\">"
						+ "<h3 class=\"ui-accordion-header1\"><span class=\"ui-accordion-header-icon ui-icon1 "
						+ " ui-icon-triangle-1-e ui-icon-triangle-1-s\" id=\"" + gridId + "_personalizeid\"></span>"
						+ "<img alt=\"\" class=\"navIcon gear\" src=\"images/f_spacer.gif\">Personalize</h3>"
						+ "</div><div id=\"" + gridId
						+ "_settings_panel\" class=\"VisionETLSettings_panel\" style=\"display: none;\">"
						+ "<div class=\"personalize\" id=\"" + gridId
						+ "_personalize_fields\"> <div class=\"pers_content\">"
						+ " <div id=\"tg-wrap4\" class=\"VisionETL-tg-wrap visionSearchPersonalise\"> "
						+ "<div class=\"visionPersonaliseSticky\"> <div class=\"sticky-wrap\"> "
						+ " <div class=\"sticky-wrap\">"
						+ "<table class=\"personalize_tbl sticky-enabled\" id=\"pers_criteria\" style=\"margin: 0px; width: 100%;\"> "
						+ "  <thead> <tr style=\"\"><th>Parameter</th><th>Display</th><th>Freeze</th>	   </tr>   </thead>  "
						+ " <tbody>" + gridPersonalizeStr + "</tbody>"
						+ "</table></div></div></div></div></div></div></div></div>";

				// ravi multiple excelsheets sheets
//                if (fileType != null && (".XLS".equalsIgnoreCase(fileType.toUpperCase())
//                        || ".XLSX".equalsIgnoreCase(fileType.toUpperCase()))) {
//                    Workbook workBook = WorkbookFactory.create(new File(filePath));
//                    int sheetCount = workBook.getNumberOfSheets();
//                    if (sheetCount > 1) {
//                        String navDiv = "<div id='navBar_" + gridId + "'><ul style='width: fit-content;'>";
//
//                        for (int i = 0; i < sheetCount; i++) {
//                            navDiv += "<li width='70px' >" + workBook.getSheetName(i) + "</li>";
//                        }
//                        navDiv += "</ul></div>";
//                        fileMetaObj.put("navigationDiv", navDiv);
//                    }
//                }
//                fileMetaObj.put("gridPersonalizeStr", gridPersonalizeStr);
//                fileMetaObj.put("dataFieldsArray", dataFieldsArray);
//                fileMetaObj.put("columnsArray", columnsArray);
//                fileMetaObj.put("columnList", columnList);
				if (fileType != null && (".XLS".equalsIgnoreCase(fileType.toUpperCase())
						|| ".XLSX".equalsIgnoreCase(fileType.toUpperCase()))) {
					try (Workbook workBook = WorkbookFactory.create(new File(filePath))) {

						int sheetCount = workBook.getNumberOfSheets();

						if (sheetCount > 1) {
							StringBuilder navDivBuilder = new StringBuilder(
									"<div id='navBar_" + gridId + "'><ul style='width: fit-content;'>");

							for (int i = 0; i < sheetCount; i++) {
								navDivBuilder.append("<li width='70px' >").append(workBook.getSheetName(i))
										.append("</li>");
							}

							navDivBuilder.append("</ul></div>");
							fileMetaObj.put("navigationDiv", navDivBuilder.toString());
						}
					} catch (IOException ex) {
						ex.printStackTrace();
					}
				}

				fileMetaObj.put("gridPersonalizeStr", gridPersonalizeStr);
				fileMetaObj.put("dataFieldsArray", dataFieldsArray);
				fileMetaObj.put("columnsArray", columnsArray);
				fileMetaObj.put("columnList", columnList);

			} else {
				fileMetaObj.put("Message", "Failed to read");
			}
		} catch (Exception e) {
			e.printStackTrace();
			fileMetaObj.put("Message", "Failed to read");
		}
		return fileMetaObj;
	}

	public String convertJSONtoCSV(File jsonFile, String filePath) {
		String csvFileName = "SPIRUploadSheet" + System.currentTimeMillis() + ".csv";
		try {
			File csvFile = new File(filePath + File.separator + csvFileName);
			JsonNode jsonTree = new ObjectMapper().readTree(jsonFile);
			CsvSchema.Builder csvSchemaBuilder = CsvSchema.builder().disableEscapeChar();
//            JsonNode firstObject = jsonTree.elements().next();
			JsonNode firstObject = jsonTree.elements().next();
			firstObject.fieldNames().forEachRemaining(fieldName -> {
				csvSchemaBuilder.addColumn(fieldName);
			});
			CsvSchema csvSchema = csvSchemaBuilder.build().withHeader();
//                    .withoutHeader();
			CsvMapper csvMapper = new CsvMapper();

			csvMapper.writerFor(JsonNode.class).with(csvSchema).writeValue(new FileOutputStream(csvFile), jsonTree);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return csvFileName;
	}

	public JSONObject gethomePageVisualizeHeaderFilters(HttpServletRequest request) {
		return dashBoardsDAO.gethomePageVisualizeHeaderFilters(request);
	}

	public JSONObject refreshVisualizationchart(HttpServletRequest request) {
		return dashBoardsDAO.refreshVisualizationchart(request);
	}

	public JSONObject sendMailChartImageAllPDF(HttpServletRequest request, HttpServletResponse response) {
		JSONObject resultObj = new JSONObject();
		com.itextpdf.text.Document document = null;
		try {
			String dashBoardName = request.getParameter("chartImagesDashBoardName");
			String userNamesArr = request.getParameter("userNamesArr");
			String alterDashBoardName = "";
			if (dashBoardName != null && !"".equalsIgnoreCase(dashBoardName)
					&& !"null".equalsIgnoreCase(dashBoardName)) {
				alterDashBoardName = dashBoardName.replace(" ", "_");
			}
			String filename = alterDashBoardName + System.currentTimeMillis() + ".pdf";
			File filelocation = new File(
					fileStoreHomedirectory + "/" + request.getSession(false).getAttribute("ssUsername"));
			if (!filelocation.exists()) {
				filelocation.mkdir();
			}
			String filepath = filelocation.getAbsolutePath() + File.separator + filename;
			response.reset();
			/*
			 * response.setContentType("application/pdf");
			 * response.setCharacterEncoding("UTF-8");
			 * response.setHeader("Content-disposition", "attachment; filename=\"" +
			 * filename + "\"");
			 */

			document = new com.itextpdf.text.Document(PageSize.A4, 50, 50, 50, 50);
			PdfWriter.getInstance(document, new FileOutputStream(filepath));
			document.open();

			String imgObjStr = request.getParameter("allImageContent");

			if (imgObjStr != null && !imgObjStr.isEmpty()) {
				JSONObject imgObj = (JSONObject) JSONValue.parse(imgObjStr);

				if (imgObj != null && !imgObj.isEmpty()) {
					int x = 30;
					int y = 550;
					int key = 1;
					int maxHeight = 250;

					for (int i = 1; i <= imgObj.size(); i++) {
						String image = (String) imgObj.get(String.valueOf(i));
						if (image == null)
							continue;

						Image img = getImage(image);
						float width = img.getWidth();
						boolean largeImage = width > 1000;
						img.scaleAbsoluteWidth(largeImage ? 600f : 250f);
						img.scaleAbsoluteHeight(220f);
						img.setWidthPercentage(100);

						if (y - 30 < 0) {
							document.newPage();
							x = 30;
							y = 550;
						}

						img.setAbsolutePosition(x, y);
						document.add(img);

						x += largeImage ? 0 : 270;
						if (largeImage || key % 2 == 0) {
							y -= maxHeight + 10;
							x = 30;
						}

						key++;
					}

					document.close();

					/*
					 * os = response.getOutputStream(); try (InputStream fis = new
					 * FileInputStream(filepath)) { byte[] bufferData = new byte[1024]; int read;
					 * while ((read = fis.read(bufferData)) != -1) { os.write(bufferData, 0, read);
					 * } if (fis != null) { fis.close(); }
					 * 
					 * }
					 */

				}
				resultObj = sendDashBoardMail(request, filepath, userNamesArr, dashBoardName, filename);

			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (document != null) {
					document.close();
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		return resultObj;
	}

	public JSONObject sendDashBoardMail(HttpServletRequest request, String filePath, String users, String dashBoardName,
			String originalFileName) {
		JSONObject resultObj = new JSONObject();
		try {
			String loginUserName = (String) request.getSession(false).getAttribute("ssUsername");
			List userMailIds = dashBoardsDAO.getMailIdsByuserNames(request, users);
			if (userMailIds != null && !userMailIds.isEmpty()) {
				InternetAddress[] recipientAddress = new InternetAddress[userMailIds.size()];
				int counter = 0;
				for (Object recipient : userMailIds) {
					String recipientMailId = (String) recipient;
					recipientAddress[counter] = new InternetAddress(recipientMailId.trim());
					counter++;
				}

				// Sender's email ID needs to be mentioned
				String from = "pilogvision1@piloggroup.org";

				final String username = "pilogvision1@piloggroup.org";// change accordingly
				final String password = "waxvwnxwgaoaaikf";// change accordingly

				// Assuming you are sending email through relay.jangosmtp.net
				String host = "smtp.gmail.com";

				Properties props = new Properties();
				props.put("mail.smtp.auth", "true");
				props.put("mail.smtp.starttls.enable", "true");
				props.put("mail.smtp.host", host);
				props.put("mail.smtp.port", "587");

				// Get the Session object.
				Session session = Session.getInstance(props, new javax.mail.Authenticator() {
					protected PasswordAuthentication getPasswordAuthentication() {
						return new PasswordAuthentication(username, password);
					}
				});

				try {
					// Create a default MimeMessage object.
					Message message = new MimeMessage(session);

					// Set From: header field of the header.
					message.setFrom(new InternetAddress(from));

					// Set To: header field of the header.
					message.setRecipients(Message.RecipientType.TO, recipientAddress);

					// Set Subject: header field
					message.setSubject("Dashboard Shared");
					// message.setSubject("You received this " + dashBoardName + " from " +
					// loginUserName + "");

					// Create the message part
					BodyPart messageBodyPart = new MimeBodyPart();

					String fieldresult = "";
					fieldresult += "<div>" + "<span style='display:block;padding:8px;'>Dear User, </span>"
							+ "<span style='display:block;padding:8px;'>You have received a dashboard from "
							+ loginUserName + " for your review and reference.</span>"
							+ "<span style='display:block;padding:8px;'>Dashboard Name:  " + dashBoardName + "</span>"
							+ "<span style='display:block;padding-top:8px;'>Best regards,</span>" + "<span>"
							+ loginUserName + "</span>" + "</div>";

					// Now set the actual message
					messageBodyPart.setContent(fieldresult, "text/html");

					// Create a multipar message
					Multipart multipart = new MimeMultipart();

					// Set text message part
					multipart.addBodyPart(messageBodyPart);

					// Part two is attachment
					messageBodyPart = new MimeBodyPart();
					String filename = filePath;
					DataSource source = new FileDataSource(filename);
					messageBodyPart.setDataHandler(new DataHandler(source));
					messageBodyPart.setFileName(filename);
					multipart.addBodyPart(messageBodyPart);

					// Send the complete message parts
					message.setContent(multipart);

					// Send message
					Transport.send(message);

					System.out.println("Sent message successfully....");
					resultObj.put("Message", "Dashboard has been successfully mailed");
				} catch (MessagingException e) {
					throw new RuntimeException(e);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getSubscriptionType(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			StringBuilder result = new StringBuilder();
			List resultList = dashBoardsDAO.getSubscriptionType(request);
			if (resultList != null && !resultList.isEmpty()) {
				for (int i = 0; i < resultList.size(); i++) {
					String subscriptionList = (String) resultList.get(i);
					result.append("<option >" + subscriptionList + "</option>");
				}
			}

			resultObj.put("SubscriptionType", result);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public String generateKPIInputs(String chartType) {
		
		String html =
				"<li>"
				+"<div class=\"sub-filterItems\">"
				+"<p>Green Threshold(More than 80%)</p>"
				+"<ul class=\"filtersColorwrapper\">"
				+"<li class=\"general-filters active-filter\" data-column-name=\"" + chartType+ "GREENTRESHOLDVALUE\" data-key-type=\"data\">"
				+"<input type=\"number\" id=\""+ chartType + "GREENTRESHOLDVALUE\"  style=\"flex: 1;\" value=\"90\"/>"
				+"</li>"
				+"<li class=\"general-filters active-filter colorPalateBoxClassId\" data-column-name=\"" + chartType+ "GREENTRESHOLDCOLOR\" data-key-type=\"data\">"
				+"<input type=\"color\" id=\"" + chartType+ "GREENTRESHOLDCOLOR\" value=\"#00FF00\"/>" 
				+"</li>"
				+"</ul>"
				+"</div>"
				+"</li>"
				
				+"<li>"
				+"<div class=\"sub-filterItems\">"
				+"<p>Yellow Threshold(60-80%)</p>"
				+"<ul class=\"filtersColorwrapper\">"
				+"<li class=\"general-filters active-filter\" data-column-name=\"" + chartType+ "YELLOWTRESHOLDVALUE\" data-key-type=\"data\">" 
				+"<input type=\"number\" id=\"" + chartType + "YELLOWTRESHOLDVALUE\"  style=\"flex: 1;\" value=\"70\"/>"
				+"</li>"
				+"<li class=\"general-filters active-filter colorPalateBoxClassId\" data-column-name=\"" + chartType+ "YELLOWTRESHOLDCOLOR\" data-key-type=\"data\">"
				+"<input type=\"color\" id=\""+ chartType + "YELLOWTRESHOLDCOLOR\" value=\"#FFFF00\"/>"
				+"</li>"
				+"</ul>"
				+"</div>"
				+"</li>"
				
				+"<li>"
				+"<div class=\"sub-filterItems\">"
				+"<p>Red Threshold(Below 60%)</p>"
				+"<ul class=\"filtersColorwrapper\">"
				+"<li class=\"general-filters active-filter\" data-column-name=\"" + chartType+ "REDTRESHOLDVALUE\" data-key-type=\"data\">"
				+"<input type=\"number\" id=\"" + chartType + "REDTRESHOLDVALUE\"  style=\"flex: 1;\" value=\"50\"/>"
				+"</li>"
				+"<li class=\"general-filters active-filter colorPalateBoxClassId\" data-column-name=\"" + chartType+ "REDTRESHOLDCOLOR\" data-key-type=\"data\">"
				+"<input type=\"color\" id=\"" + chartType + "REDTRESHOLDCOLOR\" value=\"#FF0000\"/>"
				+"</li>"
				+"</ul>"
				+"</div>"
				+"</li>";
		/*
		 * String html =
		 * "<li class=\"general-filters active-filter\" data-column-name=\"" +
		 * chartType+ "GREENTRESHOLDCOLOR\" data-key-type=\"data\">" +
		 * "<div class=\"sub-filterItems\"><p>Green Threshold(More than 80%)</p>" +
		 * "<input type=\"color\" id=\"" + chartType+
		 * "GREENTRESHOLDCOLOR\" value=\"#00FF00\" style=\"width: 70px;\"/>" +"</div>" +
		 * "</li>"
		 * 
		 * +"<li class=\"general-filters active-filter\" data-column-name=\"" +
		 * chartType+ "GREENTRESHOLDVALUE\" data-key-type=\"data\">" +
		 * "<input type=\"number\" id=\""+ chartType +
		 * "GREENTRESHOLDVALUE\"  style=\"flex: 1;\" value=\"90\"/>" + "</li>"
		 * 
		 * + "<li class=\"general-filters active-filter\" data-column-name=\"" +
		 * chartType+ "YELLOWTRESHOLDCOLOR\" data-key-type=\"data\">" +
		 * "<div class=\"sub-filterItems\"><p>Yellow Threshold(60-80%)</p>" +
		 * "<input type=\"color\" id=\""+ chartType +
		 * "YELLOWTRESHOLDCOLOR\" value=\"#FFFF00\" style=\"width: 70px;\"/>" +"</div>"
		 * + "</li>"
		 * 
		 * + "<li class=\"general-filters active-filter\" data-column-name=\"" +
		 * chartType+ "YELLOWTRESHOLDVALUE\" data-key-type=\"data\">" +
		 * "<input type=\"number\" id=\"" + chartType +
		 * "YELLOWTRESHOLDVALUE\"  style=\"flex: 1;\" value=\"70\"/>" + "</li>"
		 * 
		 * + "<li class=\"general-filters active-filter\" data-column-name=\"" +
		 * chartType+ "REDTRESHOLDCOLOR\" data-key-type=\"data\">" +
		 * "<div class=\"sub-filterItems\"><p>Red Threshold(Below 60%)</p>" +
		 * "<input type=\"color\" id=\"" + chartType +
		 * "REDTRESHOLDCOLOR\" value=\"#FF0000\"style=\"width: 70px;\"/>" +"</div>" +
		 * "</li>"
		 * 
		 * + "<li class=\"general-filters active-filter\" data-column-name=\"" +
		 * chartType+ "REDTRESHOLDVALUE\" data-key-type=\"data\">" +
		 * "<input type=\"number\" id=\"" + chartType +
		 * "REDTRESHOLDVALUE\"  style=\"flex: 1;\" value=\"50\"/>" + "</li>";
		 */

		return html;
	}

}
