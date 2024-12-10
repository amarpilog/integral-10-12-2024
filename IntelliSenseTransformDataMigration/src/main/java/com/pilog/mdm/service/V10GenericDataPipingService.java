package com.pilog.mdm.service;

import com.opencsv.CSVWriter;

import com.sap.mw.jco.JCO;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.servlet.ServletContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import jxl.format.Colour;
import jxl.write.WritableCellFormat;
import jxl.write.WritableFont;
import oracle.sql.RAW;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.NumberToTextConverter;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.SharedStringsTable;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.bson.conversions.Bson;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.openxmlformats.schemas.spreadsheetml.x2006.main.CTRst;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.mongodb.client.MongoClient;
import com.opencsv.CSVReader;

import com.pilog.mdm.DAO.V10DataMigrationAccessDAO;
import com.pilog.mdm.DAO.V10GenericDataMigrationDAO;
import com.pilog.mdm.DAO.V10GenericDataPipingDAO;
import com.pilog.mdm.DAO.iVisionTransformConnectionTabsDAO;
import com.pilog.mdm.DAO.iVisionTransformSaveJobsDAO;
import com.pilog.mdm.DAO.iVisionTransformTableOperationsDAO;
import com.pilog.mdm.utilities.AuditIdGenerator;
import com.pilog.mdm.utilities.PilogUtilities;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.util.LinkedHashMap;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

import java.sql.DatabaseMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.NumberUtils;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.DateUtil;
import org.springframework.transaction.annotation.Transactional;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;

/**
 *
 * @author Ravindar
 */
@Service
public class V10GenericDataPipingService {

	@Value("${file.storeFilePath}")
	private String storeFilePath;
	@Value("${MultipartResolver.fileUploadSize}")
	private long maxFileSize;
	private int maxMemSize;
	@Value("${jdbc.batchSize}")
	private int batchSize;
	@Autowired
	private V10GenericDataMigrationService dataMigrationService;
	@Autowired
	private V10GenericProcessETLDataService genericProcessETLDataService;
	@Autowired
	private V10GenericDataPipingDAO genericDataPipingDAO;
	@Autowired
	private V10GenericDataMigrationDAO dataMigrationDAO;

	private PilogUtilities cloudUtills = new PilogUtilities();
	@Autowired
	private iVisionTransformSaveJobsDAO saveJobsDAO;
	@Autowired
	private iVisionTransformConnectionTabsDAO visionTransformConnectionTabsDAO;

	@Autowired
	private iVisionTransformTableOperationsDAO tableOperationsDAO;

	@Value("${jdbc.driver}")
	private String dataBaseDriver;
	@Value("${jdbc.username}")
	private String userName;
	@Value("${jdbc.password}")
	private String password;
	@Value("${jdbc.url}")
	private String dbURL;

	private String etlFilePath;
	private HashMap globalConnectionMap = new HashMap();
	{
		if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")) {
			etlFilePath = "C://";
		} else {
			etlFilePath = "/u01/";
		}
	}

	private V10DataMigrationAccessDAO dataMigrationAccessDAO = new V10DataMigrationAccessDAO();

	public JSONObject mappingSourceColsWithDestCols(HttpServletRequest request) {

		JSONObject resultObj = new JSONObject();
		Connection fromconnection = null;
		Connection toconnection = null;
		PreparedStatement importStmt = null;
		Connection sourceConn = null;
		try {

			JSONObject dbFromObj = new JSONObject();
			JSONObject dbTOObj = new JSONObject();
			JSONObject fromConnectObj = new JSONObject();
			JSONObject toConnectObj = new JSONObject();
			String fromConnObj = request.getParameter("fromConnObj");
			String toConnObj = request.getParameter("toConnObj");
			String fromTable = request.getParameter("fromTable");
			if (fromConnObj != null && !"".equalsIgnoreCase(fromConnObj) && !"null".equalsIgnoreCase(fromConnObj)) {
				fromConnectObj = (JSONObject) JSONValue.parse(fromConnObj);
			}
			if (toConnObj != null && !"".equalsIgnoreCase(toConnObj) && !"null".equalsIgnoreCase(toConnObj)) {
				toConnectObj = (JSONObject) JSONValue.parse(toConnObj);
			}
			if (fromConnectObj != null && !fromConnectObj.isEmpty() && toConnectObj != null
					&& !toConnectObj.isEmpty()) {
				dbFromObj.put("selectedItemLabel", fromConnectObj.get("CONN_CUST_COL1"));
				dbFromObj.put("hostName", fromConnectObj.get("HOST_NAME"));
				dbFromObj.put("port", fromConnectObj.get("CONN_PORT"));
				dbFromObj.put("userName", fromConnectObj.get("CONN_USER_NAME"));
				dbFromObj.put("password", fromConnectObj.get("CONN_PASSWORD"));
				dbFromObj.put("serviceName", fromConnectObj.get("CONN_DB_NAME"));

				dbTOObj.put("selectedItemLabel", toConnectObj.get("CONN_CUST_COL1"));
				dbTOObj.put("hostName", toConnectObj.get("HOST_NAME"));
				dbTOObj.put("port", toConnectObj.get("CONN_PORT"));
				dbTOObj.put("userName", toConnectObj.get("CONN_USER_NAME"));
				dbTOObj.put("password", toConnectObj.get("CONN_PASSWORD"));
				dbTOObj.put("serviceName", toConnectObj.get("CONN_DB_NAME"));
			}

			Object fromReturendObj = dataMigrationService.getConnection(dbFromObj);
			Object toReturendObj = dataMigrationService.getConnection(dbTOObj);
			String destTableName = request.getParameter("toTable");
			String sourceTableName = request.getParameter("tablesObj");
			String joinTables = request.getParameter("joinTablesObj");
			String sourceTables = request.getParameter("sourceTables");
			JSONObject destColumnsObj = new JSONObject();
			JSONObject columnsObj = new JSONObject();
			JSONObject tablesObj = new JSONObject();
			JSONArray joinTablesArr = new JSONArray();
			JSONArray sourceTablesArr = new JSONArray();
			JSONObject defaultValuesObj = new JSONObject();
			String defaultValue = request.getParameter("defaultValObj");
			if (fromReturendObj instanceof Connection && toReturendObj instanceof Connection) {
				fromconnection = (Connection) fromReturendObj;
				toconnection = (Connection) toReturendObj;
				if (fromconnection != null && toconnection != null) {
					if (sourceTableName != null && !"".equalsIgnoreCase(sourceTableName)
							&& !"null".equalsIgnoreCase(sourceTableName)) {
						tablesObj = (JSONObject) JSONValue.parse(sourceTableName);
						System.out.println("tablesObj:::" + tablesObj);
					}
					if (defaultValue != null && !"".equalsIgnoreCase(defaultValue)
							&& !"null".equalsIgnoreCase(defaultValue)) {
						defaultValuesObj = (JSONObject) JSONValue.parse(defaultValue);
						System.out.println("defaultValuesObj:::" + defaultValuesObj);
					}
					if (joinTables != null && !"".equalsIgnoreCase(joinTables)
							&& !"null".equalsIgnoreCase(joinTables)) {
						joinTablesArr = (JSONArray) JSONValue.parse(joinTables);
					}
					if (sourceTables != null && !"".equalsIgnoreCase(sourceTables)
							&& !"null".equalsIgnoreCase(sourceTables)) {
						sourceTablesArr = (JSONArray) JSONValue.parse(sourceTables);
					}

					String columnsObjStr = request.getParameter("columnsObj");
					if (columnsObjStr != null && !"".equalsIgnoreCase(columnsObjStr)
							&& !"null".equalsIgnoreCase(columnsObjStr)) {
						columnsObj = (JSONObject) JSONValue.parse(columnsObjStr);
						System.out.println("columnsObj:::" + columnsObj);

						if (columnsObj != null && !columnsObj.isEmpty()) {

							for (Object sourceColumn : columnsObj.keySet()) {
								if (sourceColumn != null && columnsObj.get(sourceColumn) != null) {
									String destColumnStr = (String) columnsObj.get(sourceColumn);
									if (destColumnStr != null && !"".equalsIgnoreCase(destColumnStr)
											&& !"null".equalsIgnoreCase(destColumnStr)) {
										String[] destColumnArray = destColumnStr.split(":");
										if (destColumnArray != null && destColumnArray.length != 0) {
											if (destColumnsObj != null && !destColumnsObj.isEmpty()) {
												if (destColumnsObj.containsKey(destColumnArray[0])) {
													destColumnsObj.put(destColumnArray[0],
															(destColumnsObj.get(destColumnArray[0]) + ","
																	+ destColumnArray[1]));
												} else {
													destColumnsObj.put(destColumnArray[0], (destColumnArray[1]));
												}
											} else {
												destColumnsObj.put(destColumnArray[0], (destColumnArray[1]));
											}
										}
									}
//                                    JSONObject destObj = (JSONObject) columnsObj.get(sourceColumn);
//                                    if (destObj != null && !destObj.isEmpty()) {
//                                        
//                                    }
								}
							}
							Set<String> destColumns = new HashSet<>(columnsObj.keySet());

							if (defaultValuesObj != null && !defaultValuesObj.isEmpty()) {
								Set<String> defaultColumns = defaultValuesObj.keySet();
								if (defaultColumns != null && !defaultColumns.isEmpty() && destColumns != null
										&& !destColumns.isEmpty()) {
									destColumns.addAll(defaultColumns);
								}
							}

							List<String> columnsList = new ArrayList<>();
							columnsList.addAll(destColumns);

							String insertQuery = generateInsertQuery(destTableName, columnsList);
							System.out.println("insertQuery::::" + insertQuery);
							importStmt = toconnection.prepareStatement(insertQuery);
							int totalDataCount = 0;
							totalDataCount = bulkMappingSourceColsWithDestCols(dbFromObj, fromconnection,
									destColumnsObj, 1, 500, columnsObj, destTableName, importStmt, totalDataCount,
									columnsList, tablesObj, defaultValuesObj, sourceTablesArr, joinTablesArr);
							System.out.println("totalDataCount::::" + totalDataCount);
							if (totalDataCount != 0 && totalDataCount > 0) {
								resultObj.put("Message", totalDataCount + " Row(s) migrated successfully");
								resultObj.put("connectionFlag", "Y");

							}

						}
					}
				}
			} else {
				resultObj.put("connectionFlag", "N");
				resultObj.put("connectionMessage", fromReturendObj);
			}
			System.out.println("destColumnsObj:::" + destColumnsObj);
		} catch (Exception e) {
			resultObj.put("connectionFlag", "N");
			resultObj.put("connectionMessage", e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				if (importStmt != null) {
					importStmt.close();
				}
				if (sourceConn != null) {
					sourceConn.close();
				}
				if (fromconnection != null) {
					fromconnection.close();
				}
				if (toconnection != null) {
					toconnection.close();
				}
			} catch (Exception e) {
			}
		}
		return resultObj;
	}

	public int bulkMappingSourceColsWithDestCols(JSONObject dbObj, Connection connection, JSONObject destColumnsObj,
			int start, int limit, JSONObject columnsObj, String destTableName, PreparedStatement importStmt,
			int totalDataCount, List<String> sourceColumnsList, JSONObject tablesObj, JSONObject defaultValuesObj,
			JSONArray sourceTables, JSONArray joinArr) {
		JSONObject resultObj = new JSONObject();
		try {
			List totalData = getSelectedColumnsData(dbObj, connection, destColumnsObj, start, limit, tablesObj,
					sourceTables, joinArr);
			if (totalData != null && !totalData.isEmpty()) {
				int insertCount = dataMigrationService.importingData(destTableName, destColumnsObj, columnsObj,
						totalData, importStmt, sourceColumnsList, defaultValuesObj);
				if (insertCount != 0) {
					totalDataCount += insertCount;
					totalDataCount = bulkMappingSourceColsWithDestCols(dbObj, connection, destColumnsObj,
							(start + limit), limit, columnsObj, destTableName, importStmt, totalDataCount,
							sourceColumnsList, tablesObj, defaultValuesObj, sourceTables, joinArr);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return totalDataCount;
	}

	public List getSelectedColumnsData(JSONObject dbObj, Connection connection,
			// JSONObject destColumnsObj,
			Map destColumnsObj, int start, int limit, JSONObject tablesObj, JSONArray sourceTables, JSONArray joinArr) {
		List totalData = new ArrayList();
		try {

			String initParamClassName = "com.pilog.mdm.DAO.V10DataMigrationAccessDAO";
			String initParamMethodName = "getTree" + dbObj.get("selectedItemLabel") + "SelectedColumnsData";
			System.out.println(
					" initParamClassName:" + initParamClassName + "initParamMethodName:" + initParamMethodName);
			Class clazz = Class.forName(initParamClassName);
			Class<?>[] paramTypes = { Connection.class, JSONObject.class, Integer.class, Integer.class,
					JSONObject.class, String.class, JSONArray.class, JSONArray.class };
			Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
			Object targetObj = new PilogUtilities().createObjectByClass(clazz);
			totalData = (List) method.invoke(targetObj, connection, destColumnsObj, start, limit, tablesObj,
					String.valueOf(dbObj.get("serviceName")), sourceTables, joinArr);

		} catch (Exception e) {

			e.printStackTrace();
		}
		return totalData;
	}

	public String generateInsertQuery(String tableName, List<String> columnsList) {
		String query = "";
		try {
			String columnsStr = StringUtils.collectionToCommaDelimitedString(columnsList);
			columnsStr = ":" + columnsStr.trim().replaceAll(" ", "").replaceAll(",", ",:");
			query = " INSERT INTO " + tableName + " (" + StringUtils.collectionToCommaDelimitedString(columnsList) + ")"
					+ " VALUES(";
			for (int i = 0; i < columnsList.size(); i++) {
				query += "?";
				if (i != columnsList.size() - 1) {
					query += ",";
				}

			}
			query += ")";
		} catch (Exception e) {
			e.printStackTrace();
		}
		return query;
	}

	public void exportCSVData(HttpServletRequest request, HttpServletResponse response)
			throws UnsupportedEncodingException {
		System.out.println("Start Date:::" + new Date());
		response.reset();
		Connection fromConnection = null;
		try {
			List csvDataList = new ArrayList();
			JSONObject destColsObj = new JSONObject();
			JSONObject tablesObj = new JSONObject();
			JSONArray headerData = new JSONArray();
			String jsonString = request.getParameter("jsonExpData");
			List<Object[]> sourceColumnList = new ArrayList();
			System.out.println("Start Date:::" + new Date());
			if (jsonString != null && !"".equalsIgnoreCase(jsonString) && jsonString.contains("???")) {
				byte ptext[] = jsonString.getBytes("ISO-8859-1");
				jsonString = new String(ptext, "UTF-8");
			}

			JSONObject dbFromObj = new JSONObject();
			JSONObject fromConnectObj = new JSONObject();
			String fromConnObj = request.getParameter("fromConnObj");
			String fromTable = request.getParameter("fromTable");
			if (fromConnObj != null && !"".equalsIgnoreCase(fromConnObj) && !"null".equalsIgnoreCase(fromConnObj)) {
				fromConnectObj = (JSONObject) JSONValue.parse(fromConnObj);
			}
			if (fromConnectObj != null && !fromConnectObj.isEmpty()) {
				dbFromObj.put("selectedItemLabel", fromConnectObj.get("CONN_CUST_COL1"));
				dbFromObj.put("hostName", fromConnectObj.get("HOST_NAME"));
				dbFromObj.put("port", fromConnectObj.get("CONN_PORT"));
				dbFromObj.put("userName", fromConnectObj.get("CONN_USER_NAME"));
				dbFromObj.put("password", fromConnectObj.get("CONN_PASSWORD"));
				dbFromObj.put("serviceName", fromConnectObj.get("CONN_DB_NAME"));

			}
			Object fromReturendObj = dataMigrationService.getConnection(dbFromObj);
			if (fromReturendObj instanceof Connection) {
				fromConnection = (Connection) fromReturendObj;
				if (fromConnection != null) {
					if (fromTable != null && !"".equalsIgnoreCase(fromTable) && !"null".equalsIgnoreCase(fromTable)) {
						sourceColumnList = getTreeDMTableColumns(fromConnection, request, dbFromObj, fromTable);
					}
				}
			}

			String columnsObj = "";
			for (int i = 0; i < sourceColumnList.size(); i++) {
				Object[] columnsList = sourceColumnList.get(i);
				columnsObj += columnsList[1];
				if (i != sourceColumnList.size() - 1) {
					columnsObj += ",";
				}
				headerData.add(columnsList[1]);
			}
			if (columnsObj != null && !"".equalsIgnoreCase(columnsObj) && !"null".equalsIgnoreCase(columnsObj)) {
				csvDataList.add(columnsObj.split(","));
			}
			destColsObj.put(fromTable, columnsObj);
			tablesObj.put(fromTable, "");
			System.out.println("Start Date:::" + new Date());
			SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy:HH:mm:ss:SSS");
			Date date = new Date();
			String dateStr = sdf.format(date);
			String filename = "V10Download_" + dateStr + ".csv";
			filename = filename.replaceAll(":", "-");
			File file12 = new File(storeFilePath);
			if (!file12.exists()) {
				file12.mkdirs();
			}
			File outputFile = new File(file12.getAbsolutePath() + File.separator + filename);

			int start = 0;
			int limit = 500;
			csvDataList = setDMBulkExportCSVData(dbFromObj, fromConnection, destColsObj, start, limit, tablesObj,
					headerData, fromTable, csvDataList);
			if (csvDataList != null && !csvDataList.isEmpty()) {

				FileOutputStream fos = new FileOutputStream(outputFile);
				fos.write(0xef);
				fos.write(0xbb);
				fos.write(0xbf);
				CSVWriter writer = new CSVWriter(new OutputStreamWriter(fos, "UTF-8"), '\t');
				writer.writeAll(csvDataList, false);
				writer.close();
				ServletContext ctx = request.getSession().getServletContext();
				InputStream fis = new FileInputStream(outputFile);
				String mimeType = ctx.getMimeType(outputFile.getAbsolutePath());
				response.setContentType(mimeType != null ? mimeType : "application/octet-stream");
				response.setHeader("Content-Disposition", "attachment; filename=\"" + filename.trim() + "\"");
				ServletOutputStream os = response.getOutputStream();
				byte[] bufferData = new byte[4096];
				int read = 0;
				while ((read = fis.read(bufferData)) != -1) {
					os.write(bufferData, 0, read);
				}
				fis.close();
				os.flush();
				os.close();
				System.out.println("End date::::" + new Date());
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (fromConnection != null) {
					fromConnection.close();
				}
			} catch (Exception e) {
			}
		}

	}

	public void exportXlsx(HttpServletRequest request, HttpServletResponse response)
			throws UnsupportedEncodingException {
		System.out.println("Start Date:::" + new Date());
		response.reset();
		Connection fromConnection = null;
		try {
			JSONObject destColsObj = new JSONObject();
			JSONObject tablesObj = new JSONObject();
			JSONArray headerData = new JSONArray();
			String jsonString = request.getParameter("jsonExpData");
			List<Object[]> sourceColumnList = new ArrayList();
			System.out.println("Start Date:::" + new Date());
			if (jsonString != null && !"".equalsIgnoreCase(jsonString) && jsonString.contains("???")) {
				byte ptext[] = jsonString.getBytes("ISO-8859-1");
				jsonString = new String(ptext, "UTF-8");
			}

			JSONObject dbFromObj = new JSONObject();
			JSONObject fromConnectObj = new JSONObject();
			String fromConnObj = request.getParameter("fromConnObj");
			String fromTable = request.getParameter("fromTable");
			if (fromConnObj != null && !"".equalsIgnoreCase(fromConnObj) && !"null".equalsIgnoreCase(fromConnObj)) {
				fromConnectObj = (JSONObject) JSONValue.parse(fromConnObj);
			}
			if (fromConnectObj != null && !fromConnectObj.isEmpty()) {
				dbFromObj.put("selectedItemLabel", fromConnectObj.get("CONN_CUST_COL1"));
				dbFromObj.put("hostName", fromConnectObj.get("HOST_NAME"));
				dbFromObj.put("port", fromConnectObj.get("CONN_PORT"));
				dbFromObj.put("userName", fromConnectObj.get("CONN_USER_NAME"));
				dbFromObj.put("password", fromConnectObj.get("CONN_PASSWORD"));
				dbFromObj.put("serviceName", fromConnectObj.get("CONN_DB_NAME"));

			}
			Object fromReturendObj = dataMigrationService.getConnection(dbFromObj);
			if (fromReturendObj instanceof Connection) {
				fromConnection = (Connection) fromReturendObj;
				if (fromConnection != null) {
					if (fromTable != null && !"".equalsIgnoreCase(fromTable) && !"null".equalsIgnoreCase(fromTable)) {
						sourceColumnList = getTreeDMTableColumns(fromConnection, request, dbFromObj, fromTable);
					}
				}
			}

			String columnsObj = "";
			for (int i = 0; i < sourceColumnList.size(); i++) {
				Object[] columnsList = sourceColumnList.get(i);
				columnsObj += columnsList[1];
				if (i != sourceColumnList.size() - 1) {
					columnsObj += ",";
				}
				headerData.add(columnsList[1]);
			}
			destColsObj.put(fromTable, columnsObj);
			tablesObj.put(fromTable, "");
			System.out.println("Start Date:::" + new Date());
			SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy:HH:mm:ss:SSS");
			Date date = new Date();
			String dateStr = sdf.format(date);
			String sheetName = "Results";
			XSSFWorkbook wb = new XSSFWorkbook();
			XSSFSheet sheet = wb.createSheet(sheetName);

			int rowIdx = 0;
			short cellIdx = 0;

			BufferedInputStream input = null;
			BufferedOutputStream output = null;
			BufferedReader bufferedReader = null;
			String selectType = request.getParameter("selectType");
			String filename = "";
			if (selectType != null && !"".equalsIgnoreCase(selectType) && "XLSX".equalsIgnoreCase(selectType)) {
				filename = "V10Download_" + dateStr + ".xlsx";
			} else if (selectType != null && !"".equalsIgnoreCase(selectType) && "XLS".equalsIgnoreCase(selectType)) {
				filename = "V10Download_" + dateStr + ".xls";
			}

			filename = filename.replaceAll(":", "-");
			File file12 = new File(storeFilePath);
			if (!file12.exists()) {
				file12.mkdirs();
			}
			File outputFile = new File(file12.getAbsolutePath() + File.separator + filename);

			XSSFRow hssfHeader = sheet.createRow(rowIdx);
			WritableFont cellFont = new WritableFont(WritableFont.TIMES, 16);

			WritableCellFormat cellFormat = new WritableCellFormat(cellFont);
			cellFormat.setBackground(Colour.ORANGE);
			XSSFCellStyle cellStyle = wb.createCellStyle();
			cellStyle.setAlignment(HSSFCellStyle.ALIGN_CENTER);
			cellStyle.setWrapText(true);

			for (int i = 0; i < headerData.size(); i++) {
				XSSFCell hssfCell = hssfHeader.createCell(cellIdx++);
				hssfCell.setCellStyle(cellStyle);
				hssfCell.setCellValue((String) headerData.get(i));
			}

			rowIdx = 1;
			int start = 0;
			int limit = 500;
			int count = setDMBulkExportData(dbFromObj, fromConnection, destColsObj, start, limit, tablesObj, rowIdx,
					headerData, sheet, fromTable);
			if (count > 0) {

				wb.setSheetName(0, sheetName);
				FileOutputStream outs = new FileOutputStream(outputFile);

				wb.write(outs);
				outs.close();

				ServletContext ctx = request.getSession().getServletContext();
				InputStream fis = new FileInputStream(outputFile);
				String mimeType = ctx.getMimeType(outputFile.getAbsolutePath());
				response.setContentType(mimeType != null ? mimeType : "application/octet-stream");

				response.setHeader("Content-Disposition", "attachment; filename=\"" + filename.trim() + "\"");

				ServletOutputStream os = response.getOutputStream();
				byte[] bufferData = new byte[4096];
				int read = 0;
				while ((read = fis.read(bufferData)) != -1) {
					os.write(bufferData, 0, read);
				}
				fis.close();
				os.flush();
				os.close();
				System.out.println("End date::::" + new Date());
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (fromConnection != null) {
					fromConnection.close();
				}
			} catch (Exception e) {
			}
		}

	}

	public void exportXMLData(HttpServletRequest request, HttpServletResponse response)
			throws UnsupportedEncodingException {
		System.out.println("Start Date:::" + new Date());
		response.reset();
		Connection fromConnection = null;
		try {
			JSONArray columnsArray = new JSONArray();
			JSONObject destColsObj = new JSONObject();
			JSONObject tablesObj = new JSONObject();
			JSONArray headerData = new JSONArray();
			String jsonString = request.getParameter("jsonExpData");
			List<Object[]> sourceColumnList = new ArrayList();
			System.out.println("Start Date:::" + new Date());
			if (jsonString != null && !"".equalsIgnoreCase(jsonString) && jsonString.contains("???")) {
				byte ptext[] = jsonString.getBytes("ISO-8859-1");
				jsonString = new String(ptext, "UTF-8");
			}

			JSONObject dbFromObj = new JSONObject();
			JSONObject fromConnectObj = new JSONObject();
			String fromConnObj = request.getParameter("fromConnObj");
			String fromTable = request.getParameter("fromTable");
			if (fromConnObj != null && !"".equalsIgnoreCase(fromConnObj) && !"null".equalsIgnoreCase(fromConnObj)) {
				fromConnectObj = (JSONObject) JSONValue.parse(fromConnObj);
			}
			if (fromConnectObj != null && !fromConnectObj.isEmpty()) {
				dbFromObj.put("selectedItemLabel", fromConnectObj.get("CONN_CUST_COL1"));
				dbFromObj.put("hostName", fromConnectObj.get("HOST_NAME"));
				dbFromObj.put("port", fromConnectObj.get("CONN_PORT"));
				dbFromObj.put("userName", fromConnectObj.get("CONN_USER_NAME"));
				dbFromObj.put("password", fromConnectObj.get("CONN_PASSWORD"));
				dbFromObj.put("serviceName", fromConnectObj.get("CONN_DB_NAME"));

			}
			Object fromReturendObj = dataMigrationService.getConnection(dbFromObj);
			if (fromReturendObj instanceof Connection) {
				fromConnection = (Connection) fromReturendObj;
				if (fromConnection != null) {
					if (fromTable != null && !"".equalsIgnoreCase(fromTable) && !"null".equalsIgnoreCase(fromTable)) {
						sourceColumnList = getTreeDMTableColumns(fromConnection, request, dbFromObj, fromTable);
					}
				}
			}
			int index = 0;
			String columnsObj = "";
			for (int i = 0; i < sourceColumnList.size(); i++) {
				Object[] columnsList = sourceColumnList.get(i);
				columnsObj += columnsList[1];
				if (i != sourceColumnList.size() - 1) {
					columnsObj += ",";
				}
				headerData.add(columnsList[1]);
				columnsArray.add(index, columnsList[1]);
				index++;
			}

			destColsObj.put(fromTable, columnsObj);
			tablesObj.put(fromTable, "");
			System.out.println("Start Date:::" + new Date());
			SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy:HH:mm:ss:SSS");
			Date date = new Date();
			String dateStr = sdf.format(date);
			String filename = "V10Download_" + dateStr + ".xml";
			filename = filename.replaceAll(":", "-");
			File file12 = new File(storeFilePath);
//            File file12 = new File("C:\\samplexmls");

			if (!file12.exists()) {
				file12.mkdirs();
			}
			String xmlString = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<PiLog_Data_Export>\n";
			File outputFile = new File(file12.getAbsolutePath() + File.separator + filename);

			int start = 0;
			int limit = 500;
			xmlString = setDMBulkExportXMLData(dbFromObj, fromConnection, destColsObj, start, limit, tablesObj,
					headerData, fromTable, xmlString, columnsArray);
			if (xmlString != null && !"".equalsIgnoreCase(xmlString) && !"null".equalsIgnoreCase(xmlString)) {
				xmlString += "</PiLog_Data_Export>\n";
				FileOutputStream outs = new FileOutputStream(outputFile);
				outs.write(xmlString.getBytes("UTF-8"));
				outs.close();

				ServletContext ctx = request.getSession().getServletContext();
				InputStream fis = new FileInputStream(outputFile);

				String mimeType = ctx.getMimeType(outputFile.getAbsolutePath());
				response.setContentType(mimeType != null ? mimeType : "application/octet-stream");

				response.setHeader("Content-Disposition", "attachment; filename=\"" + filename.trim() + "\"");

				ServletOutputStream os = response.getOutputStream();
				byte[] bufferData = new byte[4096];
				int read = 0;
				while ((read = fis.read(bufferData)) != -1) {
					os.write(bufferData, 0, read);
				}
				fis.close();
				os.flush();
				os.close();
				System.out.println("End date::::" + new Date());
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (fromConnection != null) {
					fromConnection.close();
				}
			} catch (Exception e) {
			}
		}

	}

	public int setDMBulkExportData(JSONObject dbFromObj, Connection fromConnection, JSONObject destColsObj, int start,
			int limit, JSONObject dummyObj, int rowIdx, JSONArray headerData, XSSFSheet sheet, String fromTable) {
		Connection con = null;
		try {
			con = (Connection) dataMigrationService.getConnection(dbFromObj);
			if (con != null && con instanceof Connection) {
				List data = dataMigrationService.getSelectedColumnsData(dbFromObj, con, destColsObj, start, limit,
						dummyObj);
				if (data != null && !data.isEmpty()) {
					rowIdx = setDMBulkExportData(data, rowIdx, headerData, sheet, fromTable);
					setDMBulkExportData(dbFromObj, con, destColsObj, (start + limit), limit, dummyObj, rowIdx,
							headerData, sheet, fromTable);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (con != null) {
					con.close();
				}
			} catch (Exception e) {

			}
		}

		return rowIdx;
	}

	public List setDMBulkExportCSVData(JSONObject dbFromObj, Connection fromConnection, JSONObject destColsObj,
			int start, int limit, JSONObject dummyObj, JSONArray headerData, String fromTable, List csvDataList) {
		Connection con = null;
		try {
			con = (Connection) dataMigrationService.getConnection(dbFromObj);
			if (con != null && con instanceof Connection) {
				List data = dataMigrationService.getSelectedColumnsData(dbFromObj, con, destColsObj, start, limit,
						dummyObj);
				if (data != null && !data.isEmpty()) {
					csvDataList = setDMBulkExportCSVData(data, headerData, fromTable, csvDataList);
					setDMBulkExportCSVData(dbFromObj, con, destColsObj, (start + limit), limit, dummyObj, headerData,
							fromTable, csvDataList);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (con != null) {
					con.close();
				}
			} catch (Exception e) {

			}
		}

		return csvDataList;
	}

	public String setDMBulkExportXMLData(JSONObject dbFromObj, Connection fromConnection, JSONObject destColsObj,
			int start, int limit, JSONObject dummyObj, JSONArray headerData, String fromTable, String xmlString,
			JSONArray columnsArray) {
		Connection con = null;
		try {
			con = (Connection) dataMigrationService.getConnection(dbFromObj);
			if (con != null && con instanceof Connection) {
				List data = dataMigrationService.getSelectedColumnsData(dbFromObj, con, destColsObj, start, limit,
						dummyObj);
				if (data != null && !data.isEmpty()) {
					xmlString = setDMBulkExportXMLData(data, headerData, fromTable, xmlString, columnsArray);
					setDMBulkExportXMLData(dbFromObj, con, destColsObj, (start + limit), limit, dummyObj, headerData,
							fromTable, xmlString, columnsArray);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (con != null) {
					con.close();
				}
			} catch (Exception e) {

			}
		}

		return xmlString;
	}

	public int setDMBulkExportData(List data, int rowIdx, JSONArray headerData, XSSFSheet sheet, String tableName) {
		XSSFRow hssfRow = null;
		try {
			for (int i = 0; i < data.size(); i++) {
				Map items = (HashMap) data.get(i);
				if (items != null && !items.isEmpty()) {
					hssfRow = sheet.createRow(rowIdx);
					int cellIdx = 0;
					for (int j = 0; j < headerData.size(); j++) {
						if (items != null && !items.isEmpty()) {
							XSSFCell hSSFCell = hssfRow.createCell(cellIdx++);
							String headerDatas = tableName + ":" + headerData.get(j);
							String cellvalue = String.valueOf(items.get(headerDatas));
							if (cellvalue != null && !"".equalsIgnoreCase(cellvalue)) {
								cellvalue = cellvalue.replaceAll("Â", "");
							}
							hSSFCell.setCellValue(cellvalue);

						}
					}
					rowIdx++;
				}

			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return rowIdx;
	}

	public List setDMBulkExportCSVData(List data, JSONArray headerData, String tableName, List csvDataList) {
		try {
			String dataString = "";
			for (int i = 0; i < data.size(); i++) {
				Map items = (HashMap) data.get(i);
				if (items != null && !items.isEmpty()) {
					for (int idx = 0; idx < headerData.size(); idx++) {
						if (items != null && !items.isEmpty()) {
							try {
								String header_key = tableName + ":" + headerData.get(idx);
								String cellvalue = (String) items.get(header_key);
								cellvalue = cellvalue.replaceAll("Â", "");
								dataString += "" + cellvalue;
								if (idx != headerData.size() - 1) {
									dataString += ":::";
								}
							} catch (Exception exe) {
							}
						}
					}
				}
				if (dataString != null && !"".equalsIgnoreCase(dataString)) {
					csvDataList.add(dataString.split(":::"));
					dataString = "";
				}

			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return csvDataList;
	}

	public String setDMBulkExportXMLData(List data, JSONArray headerData, String tableName, String xmlString,
			JSONArray columnsArray) {
		try {
			if (data != null && !data.isEmpty()) {
				for (int i = 0; i < data.size(); i++) {
					Map items = (HashMap) data.get(i);
					if (items != null && !items.isEmpty()) {
						xmlString += "<Item>";
						for (Object columnName : columnsArray) {
							if (columnName != null) {
								String columnKey = tableName + ":" + columnName;
								String cellvalue = String.valueOf(items.get(columnKey));
								if (cellvalue != null && !"".equalsIgnoreCase(cellvalue)) {
									cellvalue = cellvalue.replaceAll("Â", "");
									cellvalue = cellvalue.replaceAll("[^\\x00-\\x7F]", " ");
									cellvalue = cellvalue.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", " ");
									cellvalue = cellvalue.replaceAll("\\p{C}", " ");
									// cellvalue = cellvalue.replaceAll("\\s+", " ");
								}
								xmlString += "<" + String.valueOf(columnName).replaceAll(" ", "_") + ">"
										+ escape(cellvalue) + "</" + String.valueOf(columnName).replaceAll(" ", "_")
										+ ">\n";
							}
						}
						xmlString += "</Item>\n";
					}

				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return xmlString;
	}

	public String escape(String string) {
		StringBuffer sb = new StringBuffer();
		int i = 0;
		for (int length = string.length(); i < length; i++) {
			char c = string.charAt(i);
			switch (c) {
			case '&':
				sb.append("&amp;");
				break;
			case '<':
				sb.append("&lt;");
				break;
			case '>':
				sb.append("&gt;");
				break;
			case '"':
				sb.append("&quot;");
				break;
			case '\'':
				sb.append("&apos;");
				break;
			default:
				sb.append(c);
			}
		}
		return sb.toString();
	}

	public JSONObject fetchTreeDMTableColumns(HttpServletRequest request) {

		JSONObject resultObj = new JSONObject();
		Connection fromConnection = null;
		Connection toConnection = null;

		try {
			JSONObject dbFromObj = new JSONObject();
			JSONObject dbTOObj = new JSONObject();
			JSONObject fromConnectObj = new JSONObject();
			JSONObject toConnectObj = new JSONObject();
			JSONArray sourceTablesArr = new JSONArray();
			String fromConnObj = request.getParameter("fromConnObj");
			String toConnObj = request.getParameter("toConnObj");
			String fromTable = request.getParameter("fromTable");
			String toTable = request.getParameter("toTable");
			String sourceTables = request.getParameter("sourceTables");
			String selectedJoinTables = "";
			if (sourceTables != null && !"".equalsIgnoreCase(sourceTables) && !"".equalsIgnoreCase(sourceTables)) {
				sourceTablesArr = (JSONArray) JSONValue.parse(sourceTables);
			}
			if (fromConnObj != null && !"".equalsIgnoreCase(fromConnObj) && !"null".equalsIgnoreCase(fromConnObj)) {
				fromConnectObj = (JSONObject) JSONValue.parse(fromConnObj);
			}
			if (toConnObj != null && !"".equalsIgnoreCase(toConnObj) && !"null".equalsIgnoreCase(toConnObj)) {
				toConnectObj = (JSONObject) JSONValue.parse(toConnObj);
			}
			if (fromConnectObj != null && !fromConnectObj.isEmpty() && toConnectObj != null
					&& !toConnectObj.isEmpty()) {
				dbFromObj.put("selectedItemLabel", fromConnectObj.get("CONN_CUST_COL1"));
				dbFromObj.put("hostName", fromConnectObj.get("HOST_NAME"));
				dbFromObj.put("port", fromConnectObj.get("CONN_PORT"));
				dbFromObj.put("userName", fromConnectObj.get("CONN_USER_NAME"));
				dbFromObj.put("password", fromConnectObj.get("CONN_PASSWORD"));
				dbFromObj.put("serviceName", fromConnectObj.get("CONN_DB_NAME"));

				dbTOObj.put("selectedItemLabel", toConnectObj.get("CONN_CUST_COL1"));
				dbTOObj.put("hostName", toConnectObj.get("HOST_NAME"));
				dbTOObj.put("port", toConnectObj.get("CONN_PORT"));
				dbTOObj.put("userName", toConnectObj.get("CONN_USER_NAME"));
				dbTOObj.put("password", toConnectObj.get("CONN_PASSWORD"));
				dbTOObj.put("serviceName", toConnectObj.get("CONN_DB_NAME"));
			}

			Object fromReturendObj = dataMigrationService.getConnection(dbFromObj);
			Object toReturendObj = dataMigrationService.getConnection(dbTOObj);
			String matchedSelectStr = "";
			if (toReturendObj instanceof Connection && fromReturendObj instanceof Connection) {
				fromConnection = (Connection) fromReturendObj;
				toConnection = (Connection) toReturendObj;
				if (fromConnection != null && toConnection != null) {
					String selectedJoinColumns = "";
					String selectedJoinTypes = "";
					String selectedTables = "<table class=\"visionSelectedTables\" id='selectedTables' style='width: 100%;' border='1'><thead>"
							+ "<tr><th style='width:225px;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Source Table Name</th>"
							+ "<th style='width:475px;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Table Where Clause Cond.</th>"
							+ "</tr></thead><tbody>";
					if (sourceTablesArr != null && !sourceTablesArr.isEmpty() && sourceTablesArr.size() > 1) {
						selectedJoinTypes = "<div id='visionSelectedRadioJoin' class='visionSelectedRadioJoinTypes'>"
								+ " <label><input type=\"radio\" name=\"dmJoin\" value=\"LEFT OUTER JOIN\">Left Outer Join</label>"
								+ " <label><input type=\"radio\" name=\"dmJoin\" value=\"RIGHT OUTER JOIN\">Right Outer Join</label>"
								+ " <label><input type=\"radio\" name=\"dmJoin\" value=\"INNER JOIN\">Inner Join</label>"
								+ " <label><input type=\"radio\" name=\"dmJoin\" value=\"JOIN\">Join</label>";
						selectedJoinTables = "<div>"
								+ "<div id='visionSelectedDMJoinTablesId' class='visionSelectedDMJoinTables'><div id ='addJoinTableRowId' style='cursor: pointer;'><img src='images/Add icon.svg' onclick=addJoinTableRow('"
								+ dbFromObj.toJSONString() + "',selectedJoinTables,'0')/></div>"
								+ "<table class=\"visionSelectedJoinTables\" id='selectedJoinTables_0' style='width: 100%;' border='1'><thead>"
								+ "<tr><th style='width:300px;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Join Table Names</th>"
								+ "</tr></thead><tbody>";

						selectedJoinColumns = "<div>"
								+ "<div id ='visionSelectedDMJoinColumnsId' class='visionSelectedDMJoinColumns'><div id ='addJoinColumnRowId' style='cursor: pointer;'><img src='images/Add icon.svg' onclick=addJoinColumnRow('"
								+ dbFromObj.toJSONString() + "','selectedJoinColumns','0')/></div>"
								+ "<table class=\"visionSelectedJoinColumns\" id='selectedJoinColumns_0' style='width: 100%;' border='1'><thead>"
								+ "<tr><th style='width:300px;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Join Column Names</th>"
								+ "</tr></thead><tbody>";
					}
					String sourceTr = "<thead><tr><th class=\"mappedColsTh\" style='width:252px;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'> Destination Columns</th>"
							+ "<th class=\"mappedColsTh\" style='width:252px;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Source Columns</th>"
							+ "<th class=\"mappedColsTh\" style='width:252px;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Default Values</th>"
							+ "</tr></thead>";
					if ((fromTable != null && !"".equalsIgnoreCase(fromTable) && !"null".equalsIgnoreCase(fromTable))
							&& (toTable != null && !"".equalsIgnoreCase(toTable)
									&& !"null".equalsIgnoreCase(toTable))) {
						if (sourceTablesArr != null && !sourceTablesArr.isEmpty() && sourceTablesArr.size() > 1) {
							for (int s = 0; s < sourceTablesArr.size(); s++) {
								selectedTables += "<tr><td  id=SELECT_TABLE_" + s
										+ " style='width: 225px;' data-table = " + sourceTablesArr.get(s) + ">"
										+ sourceTablesArr.get(s) + "</td>"
										+ "<td colspan=\"3\" style='width:460px'><input type=\"text\" style='width:100%' id=TABLE_INPUT"
										+ s + " ></td></tr>";

							}
						} else {
							selectedTables += "<tr><td  id=SELECT_TABLE_" + 0 + " style='width: 225px;' data-table = "
									+ fromTable + ">" + fromTable + "</td>"
									+ "<td colspan=\"3\" style='width:460px'><input type=\"text\" style='width:100%' id=TABLE_INPUT"
									+ 0 + " ></td></tr>";
						}
					}
					List<Object[]> sourceColumnList = getTreeDMTableColumns(toConnection, request, dbTOObj, toTable);

					List<Object[]> destinationColumnList = new ArrayList();
					if (sourceTablesArr != null && !sourceTablesArr.isEmpty() && sourceTablesArr.size() > 1) {
						for (int s = 0; s < sourceTablesArr.size(); s++) {
							List<Object[]> destinationsColumnList = getTreeDMTableColumns(fromConnection, request,
									dbFromObj, (String) sourceTablesArr.get(s));
							destinationColumnList.addAll(destinationsColumnList);
							if (sourceTablesArr != null && !sourceTablesArr.isEmpty() && sourceTablesArr.size() > 1) {
								if (s < 2) {
									selectedJoinColumns += "<tr><td class=\"sourceJoinColsTd\">"
											+ "<select id=\"SOURCE_SELECT_JOIN_LEFT_COLUMNS_" + s
											+ "\"  class=\"sourceColsJoinSelectBox\">" + ""
											+ dataMigrationService.generateSelectBoxStr(destinationsColumnList, "",
													"SOURCE_SELECT_LEFT_JOIN_COLUMNS_" + s + "")
											+ "" + "</select>" + "</td></tr>";
								}
							}
						}
					} else {
						destinationColumnList = getTreeDMTableColumns(fromConnection, request, dbFromObj, fromTable);
					}

					sourceTr += "<tbody>";

					if (sourceTablesArr != null && !sourceTablesArr.isEmpty() && sourceTablesArr.size() > 1) {
						selectedJoinTables += "<tr><td class=\"sourceJoinColsTd\">"
								+ "<input type='text' id='SOURCE_SELECT_JOIN_TABLES_0' value='"
								+ (String) sourceTablesArr.get(0) + "'/>" + "</td></tr>"
								+ "<tr><td class=\"sourceJoinColsTd\">"
								+ "<input type='text' id='SOURCE_SELECT_JOIN_TABLES_1' value='"
								+ (String) sourceTablesArr.get(1) + "'/>" + "</td></tr></table></div></div>";

						selectedJoinColumns += "</table></div></div>";
					}

					if (sourceColumnList != null && !sourceColumnList.isEmpty() && destinationColumnList != null
							&& !destinationColumnList.isEmpty()) {
						int minSize = (sourceColumnList.size() > destinationColumnList.size())
								? destinationColumnList.size()
								: sourceColumnList.size();
						for (int i = 0; i < sourceColumnList.size(); i++) {
							Object[] sourceColumnsArray = sourceColumnList.get(i);
							if (sourceColumnsArray != null && sourceColumnsArray.length != 0) {
								sourceTr += "<tr><td id=" + sourceColumnsArray[1] + " class=\"sourceColsTd\">"
										+ "<select id=\"SOURCE_SELECT_" + i + "\"  class=\"sourceColsSelectBox\">" + ""
										+ dataMigrationService.generateSelectBoxStr(sourceColumnList,
												(sourceColumnsArray[0] + ":" + sourceColumnsArray[1]),
												"SOURCE_SELECT_" + i)
										+ "" + "</select>" + "</td>";
							}
							if (i < destinationColumnList.size()) {
								Object[] destColumnsArray = destinationColumnList.get(i);
								if (destColumnsArray != null && destColumnsArray.length != 0) {
									sourceTr += "" + "<td><select id=\"DEST_SELECT_" + i
											+ "\" class=\"destColsSelectBox\" >" + ""
											+ dataMigrationService.generateSelectBoxStr(destinationColumnList,
													(destColumnsArray[0] + ":" + destColumnsArray[1]),
													"DEST_SELECT_" + i)
											+ "" + "</select>"
											+ "</td><td><input type=\"text\" class=\"defaultValues\" id=\"DEFAULTVALUES_"
											+ i + "\"></td></tr>";
								}
							} else {
								sourceTr += "" + "<td><select id=\"DEST_SELECT_" + i
										+ "\" class=\"destColsSelectBox\" >" + ""
										+ dataMigrationService.generateSelectBoxStr(destinationColumnList, "",
												"DEST_SELECT_" + i)
										+ "" + "</select>"
										+ "</td><td><input type=\"text\" class=\"defaultValues\" id=\"DEFAULTVALUES_"
										+ i + "\"></td></tr>";

							}

						}
//                        for (int i = 0; i < minSize; i++) {
//                            Object[] sourceColumnsArray = sourceColumnList.get(i);
//                            if (sourceColumnsArray != null && sourceColumnsArray.length != 0) {
//                                sourceTr += "<tr><td id=" + sourceColumnsArray[1] + " class=\"sourceColsTd\">"
//                                        + "<select id=\"SOURCE_SELECT_" + i + "\"  class=\"sourceColsSelectBox\">"
//                                        + "" + generateSelectBoxStr(sourceColumnList, (sourceColumnsArray[0] + ":" + sourceColumnsArray[1]), "SOURCE_SELECT_" + i) + ""
//                                        + "</select>"
//                                        + "</td>";
//                            }
//                            
//                            Object[] destColumnsArray = destinationColumnList.get(i);
//                            if (destColumnsArray != null && destColumnsArray.length != 0) {
//                                sourceTr += ""
//                                        + "<td><select id=\"DEST_SELECT_" + i + "\" class=\"destColsSelectBox\" >"
//                                        + "" + generateSelectBoxStr(destinationColumnList, (destColumnsArray[0] + ":" + destColumnsArray[1]), "DEST_SELECT_" + i) + ""
//                                        + "</select>"
//                                        + "</td><td><input type=\"text\" class=\"defaultValues\" id=\"DEFAULTVALUES_" + i + "\"></td></tr>";
//                            }
//
//                        }

					}

					sourceTr += "</tbody>";
					selectedTables += "</tbody></table>";

					String columnStr = "<table id=\"sourceDestColsTableId\" class=\"visionEtlSourceDestColsTable\" style='width: auto;' border='1'>"
							+ sourceTr + "</table>";
					resultObj.put("columnStr", columnStr);
					resultObj.put("matchedSelectStr", matchedSelectStr);
					resultObj.put("selectedTables", selectedTables);
					resultObj.put("selectedJoinTables", fetchJoinsData(request));
//                    resultObj.put("selectedJoinTables", selectedJoinTypes + selectedJoinTables + selectedJoinColumns);
					resultObj.put("connectionFlag", "Y");
				}
			} else {
				resultObj.put("connectionFlag", "N");
				resultObj.put("connectionMessage", toReturendObj);
			}

		} catch (Exception e) {
			e.printStackTrace();
//            resultObj.put("connectionFlag", "N");
//            resultObj.put("connectionMessage", e.getMessage());
		} finally {
			try {
//                Connection fromConnection = null;
//                Connection toConnection = null;
				if (fromConnection != null) {
					fromConnection.close();
				}
				if (toConnection != null) {
					toConnection.close();
				}
			} catch (Exception e) {
			}
		}
		return resultObj;
	}

	public JSONObject fetchTreeDMTableFileColumns(HttpServletRequest request, HttpServletResponse response) {

		JSONObject resultObj = new JSONObject();
		Connection fromConnection = null;
		try {
			JSONObject dbFromObj = new JSONObject();
			JSONObject fromConnectObj = new JSONObject();
			String fromConnObj = request.getParameter("fromConnObj");
			String fromTable = request.getParameter("fromTable");
			String toTable = request.getParameter("toTable");
			if (fromConnObj != null && !"".equalsIgnoreCase(fromConnObj) && !"null".equalsIgnoreCase(fromConnObj)) {
				fromConnectObj = (JSONObject) JSONValue.parse(fromConnObj);
			}

			if (fromConnectObj != null && !fromConnectObj.isEmpty()) {
				dbFromObj.put("selectedItemLabel", fromConnectObj.get("CONN_CUST_COL1"));
				dbFromObj.put("hostName", fromConnectObj.get("HOST_NAME"));
				dbFromObj.put("port", fromConnectObj.get("CONN_PORT"));
				dbFromObj.put("userName", fromConnectObj.get("CONN_USER_NAME"));
				dbFromObj.put("password", fromConnectObj.get("CONN_PASSWORD"));
				dbFromObj.put("serviceName", fromConnectObj.get("CONN_DB_NAME"));

			}

			Object fromReturendObj = dataMigrationService.getConnection(dbFromObj);
			String matchedSelectStr = "";
			if (fromReturendObj instanceof Connection) {
				fromConnection = (Connection) fromReturendObj;
				if (fromConnection != null) {
					String sourceTr = "<thead><tr><th class=\"mappedColsTh\" style='width:260px;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'> Source Columns</th>"
							+ "<th class=\"mappedColsTh\" style='width:260px;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Destination Columns</th>"
							+ "<th class=\"mappedColsTh\" style='width:190px;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Default Values</th>"
							+ "</tr></thead>";

					List<Object[]> destColumnList = getTreeDMTableColumns(fromConnection, request, dbFromObj,
							fromTable);
					String excelFilePath = etlFilePath + "Files/DMImport/"
							+ request.getSession(false).getAttribute("ssUsername");
					File file = new File(excelFilePath + File.separator + toTable);
					List<String> sourceColumnList = dataMigrationService.getHeadersOfImportedFile(request, response,
							file.getAbsolutePath());
					sourceTr += "<tbody>";

					if (sourceColumnList != null && !sourceColumnList.isEmpty() && destColumnList != null
							&& !destColumnList.isEmpty()) {
						int minSize = (destColumnList.size() > sourceColumnList.size()) ? sourceColumnList.size()
								: destColumnList.size();
						for (int i = 0; i < minSize; i++) {
							// Object[] sourceColumnsArray = sourceColumnList.get(i);
							if (destColumnList != null && destColumnList.size() != 0) {
								Object[] destColumnsArray = (Object[]) destColumnList.get(i);
								sourceTr += "<tr><td id = " + destColumnList.get(i) + " class=\"destColsTd\">"
										+ "<select id=\"DEST_FILE_SELECT_" + i + "\"  class=\"destColsSelectBox\">" + ""
										+ dataMigrationService.generateSelectBoxStr(destColumnList,
												(destColumnsArray[0] + ":" + destColumnsArray[1]), "DEST_SELECT_" + i)
										+ "" + "</select>" + "</td>";
							}

							if (sourceColumnList != null && sourceColumnList.size() != 0) {
								sourceTr += "" + "<td id = " + sourceColumnList.get(i)
										+ " class=\"sourceColsTd\"><select id=\"SOURCE_FILE_SELECT_" + i
										+ "\" class=\"destColsSelectBox\" >" + ""
										+ dataMigrationService.generateFileColSelectBoxStr(sourceColumnList,
												(String) sourceColumnList.get(i), "SOURCE_FILE_SELECT_" + i)
										+ "" + "</select>" + "</td>"
										+ "<td><input type=\"text\" style=\"width:194px\" class=\"visionDMDefaultValues\" id=\"DEFAULTVALUESFILES_"
										+ i + "\"></td>" + "</tr>";
							}

						}

					}

					sourceTr += "</tbody>";
					String columnStr = "<table id=\"sourceDestColsFilesTableId\" class=\"sourceDestColsFilesTable\" style='width: auto;' border='1'>"
							+ sourceTr + "</table>";
					resultObj.put("columnStr", columnStr);
					resultObj.put("matchedSelectStr", matchedSelectStr);
					resultObj.put("connectionFlag", "Y");
				}
			} else {
				resultObj.put("connectionFlag", "N");
				resultObj.put("connectionMessage", fromReturendObj);
			}

		} catch (Exception e) {
			e.printStackTrace();
//            resultObj.put("connectionFlag", "N");
//            resultObj.put("connectionMessage", e.getMessage());
		} finally {
			try {
//                Connection fromConnection = null;
//                Connection toConnection = null;
				if (fromConnection != null) {
					fromConnection.close();
				}

			} catch (Exception e) {
			}
		}
		return resultObj;
	}

	public String mappingSourceFileColsWithDestCols(HttpServletRequest request, HttpServletResponse response) {
		String result = "";
		try {
			String mappedFileCols = request.getParameter("mappedFileColsObj");
			String defaultValues = request.getParameter("defaultValObj");
			String excelFilePath = etlFilePath + "Files/TreeDMImport/"
					+ request.getSession(false).getAttribute("ssUsername");
			String filePath = excelFilePath + File.separator + request.getParameter("toTable");
			String fileExt = filePath.substring(filePath.lastIndexOf(".") + 1, filePath.length());
			JSONObject mappedFileColsObj = new JSONObject();
			JSONObject defaultValuesObj = new JSONObject();
			if (mappedFileCols != null && !"".equalsIgnoreCase(mappedFileCols)
					&& !"null".equalsIgnoreCase(mappedFileCols)) {
				mappedFileColsObj = (JSONObject) JSONValue.parse(mappedFileCols);
				System.out.println("mappedFileColsObj:::" + mappedFileColsObj);
			}
			if (defaultValues != null && !"".equalsIgnoreCase(defaultValues)
					&& !"null".equalsIgnoreCase(defaultValues)) {
				defaultValuesObj = (JSONObject) JSONValue.parse(defaultValues);
				System.out.println("defaultValuesObj:::" + defaultValuesObj);
			}
			System.out.println("mappedFileCols:::" + mappedFileCols);
			System.out.println("defaultValues:::" + defaultValues);
			System.out.println("filePath:::" + filePath);
			if (fileExt != null && !"".equalsIgnoreCase(fileExt) && !"null".equalsIgnoreCase(fileExt)) {
				if ("xls".equalsIgnoreCase(fileExt) || "xlsx".equalsIgnoreCase(fileExt)) {
					result = importExcel(request, response, filePath, request.getParameter("fromTable"),
							mappedFileColsObj, defaultValuesObj);
				} else if ("txt".equalsIgnoreCase(fileExt) || "csv".equalsIgnoreCase(fileExt)) {
					// result = visionGenericImportService.importCSV(request, response, filePath,
					// request.getParameter("gridId"), request.getParameter("tableName"),
					// mappedFileColsObj, defaultValuesObj);
				} else if ("xml".equalsIgnoreCase(fileExt)) {
					// result = visionGenericImportService.importXML(request, response, filePath,
					// request.getParameter("gridId"), request.getParameter("tableName"),
					// mappedFileColsObj, defaultValuesObj);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public String importExcel(HttpServletRequest request, HttpServletResponse response, String filepath,
			String tableName, JSONObject mappingObj, JSONObject defaultValuesObj) {

		FileInputStream fis = null;
		String resultString = "";
		Integer insertSuccessCount = 0;
		Integer insertFailureCount = 0;
		String result = "";
		String result1 = "";
		Integer uploadLimit = 0;
		System.out.println("Start Date And Time :::" + new Date());
		JSONObject labelObj = new PilogUtilities().getMultilingualObject(request);
		Connection fromConnection = null;
		PreparedStatement importStmt = null;
		int errorCount = 0;
		int rowVal = 1;
		String excluderow = "";
		try {
			if (mappingObj != null && !mappingObj.isEmpty()) {

				JSONObject dbFromObj = new JSONObject();
				JSONObject fromConnectObj = new JSONObject();
				String fromConnObj = request.getParameter("fromConnObj");
				if (fromConnObj != null && !"".equalsIgnoreCase(fromConnObj) && !"null".equalsIgnoreCase(fromConnObj)) {
					fromConnectObj = (JSONObject) JSONValue.parse(fromConnObj);
				}

				if (fromConnectObj != null && !fromConnectObj.isEmpty()) {
					dbFromObj.put("selectedItemLabel", fromConnectObj.get("CONN_CUST_COL1"));
					dbFromObj.put("hostName", fromConnectObj.get("HOST_NAME"));
					dbFromObj.put("port", fromConnectObj.get("CONN_PORT"));
					dbFromObj.put("userName", fromConnectObj.get("CONN_USER_NAME"));
					dbFromObj.put("password", fromConnectObj.get("CONN_PASSWORD"));
					dbFromObj.put("serviceName", fromConnectObj.get("CONN_DB_NAME"));

				}

				Object fromReturendObj = dataMigrationService.getConnection(dbFromObj);
				if (fromReturendObj instanceof Connection) {
					fromConnection = (Connection) fromReturendObj;
					if (fromConnection != null) {
						// fis = new FileInputStream(new File(filepath));
						Workbook workBook = null;
						Sheet sheet = null;

						String fileExtension = filepath.substring(filepath.lastIndexOf(".") + 1, filepath.length());
						System.out.println("fileExtension:::" + fileExtension);
//                        if (fileExtension != null && "xls".equalsIgnoreCase(fileExtension)) { // commented by PKH
//                            workBook = WorkbookFactory.create(new File(filepath));
//                            sheet = (HSSFSheet) workBook.getSheetAt(0);
//                        } else {
						System.out.println("Before::::" + new Date());
						workBook = WorkbookFactory.create(new File(filepath));
						System.out.println("After::fileInputStream::" + new Date());
						if (workBook.getSheetAt(0) instanceof XSSFSheet) {
							sheet = (XSSFSheet) workBook.getSheetAt(0);
						} else if (workBook.getSheetAt(0) instanceof HSSFSheet) {
							sheet = (HSSFSheet) workBook.getSheetAt(0);
						}
//                        }
						int lastRowNo = sheet.getLastRowNum();
						System.out.println("lastRowNo::::" + lastRowNo);
						int firstRowNo = sheet.getFirstRowNum();
						System.out.println("firstRowNo::::" + firstRowNo);
						int rowCount = lastRowNo - firstRowNo;
						System.out.println("rowCount:::::" + rowCount);
						Set columnsSet = new HashSet(mappingObj.keySet());
						if (defaultValuesObj != null && !defaultValuesObj.isEmpty()) {
							columnsSet.addAll(defaultValuesObj.keySet());
						}

						List<String> columnList = new ArrayList<>(columnsSet);

						String insertQuery = generateInsertQuery(tableName, columnList);
						System.out.println(insertQuery + ":::Start Date And Time After Query:::" + new Date());
						importStmt = fromConnection.prepareStatement(insertQuery);
						int stmt = 1;
						String strToDateCol = "";
						Row headerRow = sheet.getRow(0);
						Map<Object, Integer> headerData = new HashMap<>();
						for (int j = 0; j < headerRow.getLastCellNum(); j++) {
							try {
								Cell cell = headerRow.getCell(j);
								if (cell != null) {
									switch (cell.getCellType()) {
									case Cell.CELL_TYPE_STRING:
										headerData.put(cell.getStringCellValue(), j);
										break;
									case Cell.CELL_TYPE_BOOLEAN:
										headerData.put(cell.getBooleanCellValue(), j);
										break;
									case Cell.CELL_TYPE_NUMERIC:

										if (HSSFDateUtil.isCellDateFormatted(cell)) {
											if (strToDateCol != null && !"".equalsIgnoreCase(strToDateCol)
													&& !"null".equalsIgnoreCase(strToDateCol)
													&& strToDateCol.contains(String.valueOf(stmt))) {
												DateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
												Date convertedDate = (Date) formatter.parse(cell.toString());
												java.sql.Date sqlDat = new java.sql.Date(convertedDate.getTime());
												headerData.put(sqlDat, j);
//                                            testMap.put(stmt, sqlDat);
											} else {
												String cellDateString = "";
												Date cellDate = cell.getDateCellValue();
												if ((cellDate.getYear() + 1900) == 1899
														&& (cellDate.getMonth() + 1) == 12
														&& (cellDate.getDate()) == 31) {
													cellDateString = (cellDate.getHours()) + ":"
															+ (cellDate.getMinutes()) + ":" + (cellDate.getSeconds());
//                                                    System.out.println("cellDateString :: "+cellDateString);
												} else {
													cellDateString = (cellDate.getYear() + 1900) + "-"
															+ (cellDate.getMonth() + 1) + "-" + (cellDate.getDate());
												}

//                                                    String cellDateString = (cellDate.getYear() + 1900) + "-" + (cellDate.getMonth() + 1) + "-" + (cellDate.getDate());
												headerData.put(cellDateString, j);
											}

										} else {
											String cellvalStr = NumberToTextConverter
													.toText(cell.getNumericCellValue());
											headerData.put(cellvalStr, j);
										}
										break;
									case Cell.CELL_TYPE_BLANK:
										headerData.put("", j);
										break;
									}

								}
							} catch (Exception e) {
								e.printStackTrace();

								continue;
							}

						} // end of row cell loop
						for (int i = rowVal; i < rowCount + 1; i++) {
							Row row = sheet.getRow(i);
							stmt = 1;
							Map settingMap = new HashMap();
							for (int j = 0; j < columnList.size(); j++) {

//                for (int j = 0; j < row.getLastCellNum(); j++) {
								if (mappingObj.get(columnList.get(j)) != null) {
									int cellIndex = headerData.get(mappingObj.get(columnList.get(j)));
									try {
										System.out.println("cellIndex::::" + cellIndex);
										Cell cell = row.getCell(cellIndex);
										if (cell != null) {
											switch (cell.getCellType()) {
											case Cell.CELL_TYPE_STRING:
												String cellValue = cell.getStringCellValue();
												if (cellValue != null && !"".equalsIgnoreCase(cellValue)
														&& !"null".equalsIgnoreCase(cellValue)) {
													importStmt.setObject(stmt, cell.getStringCellValue());
													settingMap.put(stmt, cell.getStringCellValue());
												} else {
													if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
															&& defaultValuesObj.containsKey(columnList.get(j))) {
														importStmt.setObject(stmt,
																defaultValuesObj.get(columnList.get(j)));
														settingMap.put(stmt, defaultValuesObj.get(columnList.get(j)));
													} else {
														importStmt.setObject(stmt, "");
														settingMap.put(stmt, "");
													}
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
														java.sql.Date sqlDat = new java.sql.Date(
																convertedDate.getTime());
														importStmt.setObject(stmt, sqlDat);
														settingMap.put(stmt, sqlDat);
//                                            testMap.put(stmt, sqlDat);
													} else {

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

//                                                            String cellDateString = (cellDate.getYear() + 1900) + "-" + (cellDate.getMonth() + 1) + "-" + (cellDate.getDate());
														importStmt.setObject(stmt, cellDateString);
														settingMap.put(stmt, cellDateString);
													}

												} else {
													String cellvalStr = NumberToTextConverter
															.toText(cell.getNumericCellValue());
													importStmt.setObject(stmt, cellvalStr);
													settingMap.put(stmt, cellvalStr);
												}
												break;
											case Cell.CELL_TYPE_BLANK:
												if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
														&& defaultValuesObj.containsKey(columnList.get(j))) {
													importStmt.setObject(stmt, defaultValuesObj.get(columnList.get(j)));
													settingMap.put(stmt, defaultValuesObj.get(columnList.get(j)));
												} else {
													importStmt.setObject(stmt, "");
													settingMap.put(stmt, "");
												}
												break;
											}

										} else {
											if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
													&& defaultValuesObj.containsKey(columnList.get(j))) {
												importStmt.setObject(stmt, defaultValuesObj.get(columnList.get(j)));
												settingMap.put(stmt, defaultValuesObj.get(columnList.get(j)));
											} else {
												importStmt.setObject(stmt, "");
												settingMap.put(stmt, "");
											}

										}
									} catch (Exception e) {
										e.printStackTrace();
										importStmt.setObject(stmt, "");
										settingMap.put(stmt, "");
										continue;
									}
								} else {
									if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
											&& defaultValuesObj.containsKey(columnList.get(j))) {
										importStmt.setObject(stmt, defaultValuesObj.get(columnList.get(j)));
										settingMap.put(stmt, defaultValuesObj.get(columnList.get(j)));
									} else {
										importStmt.setObject(stmt, "");
										settingMap.put(stmt, "");
									}
								}

								stmt++;
							} // end of row cell loop

							importStmt.addBatch();
							if (i != 0 && i % batchSize == 0) {

								importStmt.executeBatch();
								importStmt.clearBatch();
								System.out.println("Batch Excuted:::" + i);
							}
							System.out.println("Row Num:::" + i + "::settingMap::::" + settingMap);
						} // row end
						int[] insertBulkCount = importStmt.executeBatch();
						System.out.println("insertBulkCount::::" + insertBulkCount);
						Integer count = Arrays.stream(insertBulkCount).sum();
						System.out.println(count + ":::End Insert Date And Time :::" + new Date());

						result1 = rowCount + " record(s) successfully imported";
						System.out.println(
								new Date() + "End Date And Time :::" + (rowCount) + ":::" + insertFailureCount);
						// return result1;
						if (fis != null) {
							fis.close();
						}

					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			errorCount = errorCount++;
			result = errorCount + " Record(s) failed to import";
		} finally {
			try {
				if (importStmt != null) {
					importStmt.close();
				}
				if (fromConnection != null) {
					fromConnection.close();
				}
			} catch (Exception e) {
				errorCount = errorCount++;
				result = errorCount + " Record(s) failed to import";
			}
		}
		String finalresult = result1 + " " + result;
		System.out.println("final result:::" + finalresult);
		return finalresult;
	}

	// import Tree DM file
	public String importFileDataDirectly(HttpServletRequest request, HttpServletResponse response, MultipartFile file1,
			String selectedFiletype, JSONObject connectionObj, String tableName, String selectedSheet) {

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
			// selectedFiletype = selectedFiletype.toLowerCase();
			// fileType1 = fileType1.toLowerCase();
			if (("jpeg".equalsIgnoreCase(fileType1) || "jpg".equalsIgnoreCase(fileType1)
					|| "png".equalsIgnoreCase(fileType1)) && selectedFiletype != null
					&& "Image".equalsIgnoreCase(selectedFiletype)) {
				selectedFiletype = fileType1;
			}
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

					// need to save
					if ("json".equalsIgnoreCase(fileType1)) {
						// need to convert from json to CSV
						try {
							mainFileName = convertJSONtoCSV(file, excelFilePath);
						} catch (Exception e) {
						}

					}

					result = directFileImportColumMappingTable(request, file, connectionObj, tableName,
							selectedFiletype, excelFilePath, selectedSheet);

					importResult.put("result", result);
					importResult.put("flag", "Success");
				} else {
					result = "[]";
					importResult.put("result", result);
					importResult.put("flag", "Fail");
				}
			}

			importResult.put("fileName", mainFileName);
			importResult.put("excelFilePath", excelFilePath + File.separator + mainFileName);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return importResult.toJSONString();
	}

	public String directFileImportColumMappingTable(HttpServletRequest request, File file, JSONObject connectionObj,
			String tableName, String fileType, String filePath, String selectedSheet) {
		String tableStr = "";
		Connection connection = null;
		try {
			connection = (Connection) getConnection(connectionObj);
			List<Object[]> columnsListObj = getTableColumnsOpt(connection, request, connectionObj, tableName);
			List<String> columnsList = columnsListObj.stream().map(rowData -> String.valueOf(rowData[2]).toUpperCase())
					.collect(Collectors.toList());
			List<Object[]> datalist = new ArrayList<>();
			String filePath1 = file.getPath();
			if (fileType.equalsIgnoreCase("xls") || fileType.equalsIgnoreCase("xlsx")) {
				datalist = readExcelFile(request, file, 10, selectedSheet);
			} else if (fileType.equalsIgnoreCase("csv") || fileType.equalsIgnoreCase("txt")) {
				datalist = readCsvOrTextFile(request, filePath1, 10);
			}
			tableStr += "<table id='fileTableColMappingTable' class='fileTableEtlColMappingTable' border='1'>";
			tableStr += "<thead><tr>";

			int fileColumnsSize = datalist.get(0).length;
			Object[] fileHeadersList = datalist.get(0);
			for (int i = 0; i < fileColumnsSize; i++) {
				String header = (String) fileHeadersList[i];

				String selectStr = "<select ><option value='SELECT' >SELECT</option>";
				selectStr += (String) columnsList.stream().map(col -> {
					if (header.equalsIgnoreCase(col.toUpperCase())) {
						return "<option value='" + col + "' selected >" + col + "</option>";
					} else {
						return "<option value='" + col + "' >" + col + "</option>";
					}

				}).collect(Collectors.joining());
				selectStr += "</select>";
				tableStr += "<th>" + selectStr + "</th>";
			}
			tableStr += "<tr></thead><tbody>";
			for (int i = 0; i < datalist.size(); i++) {
				tableStr += "<tr>";

				Object[] rowData = datalist.get(i);
				for (int j = 0; j < rowData.length; j++) {
					tableStr += "<td>" + rowData[j] + "</td>";
				}

				tableStr += "</tr>";

			}
			tableStr += "</tbody>";
			tableStr += "</table>";

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				connection.close();
			} catch (Exception e) {
			}

		}
		return tableStr;
	}

	public List readExcelFile(HttpServletRequest request, File file, int limit, String selectedSheet) {

		FileInputStream fis = null;
		List dataList = new ArrayList();
		int rowVal = 0;
		try {
			Workbook workBook = WorkbookFactory.create(file);
			Sheet sheet = null;

			for (int i = 0; i < workBook.getNumberOfSheets(); i++) {
				if (workBook.getSheetAt(i).getSheetName().equalsIgnoreCase(selectedSheet)) {
					sheet = workBook.getSheetAt(i);
					break;
				}
			}

			if (sheet == null) {
				// Sheet not found, handle the error or return a message
				System.out.println("Sheet not found: " + selectedSheet);
				return dataList; // or handle the error as per your requirement
			}
//            int noOfSheets = workBook.getNumberOfSheets();
//
//            if (workBook.getSheetAt(0) instanceof XSSFSheet) {
//                sheet = (XSSFSheet) workBook.getSheetAt(0);
//            } else if (workBook.getSheetAt(0) instanceof HSSFSheet) {
//                sheet = (HSSFSheet) workBook.getSheetAt(0);
//            }
			int lastRowNo = sheet.getLastRowNum();
			int firstRowNo = sheet.getFirstRowNum();
//                System.out.println("firstRowNo::::" + firstRowNo);
			int rowCount = lastRowNo - firstRowNo;
//                System.out.println("rowCount:::::" + rowCount);
			Row headerRow = sheet.getRow(0);
			for (int i = rowVal; i <= limit; i++) {
				Row row = sheet.getRow(i);
				if (row != null) {
//                        JSONObject dataObject = new JSONObject();
//                    Object[] dataObject = new Object[row.getLastCellNum()];
					Object[] dataObject = new Object[headerRow.getLastCellNum()];
					// JSONObject dataObject = new JSONObject();
					// dataObject.put("totalrecords", rowCount);
					for (int cellIndex = 0; cellIndex < headerRow.getLastCellNum(); cellIndex++) {

						try {
//                            System.out.println("cellIndex::::" + cellIndex);
							Cell cell = row.getCell(cellIndex);
							if (cell != null) {
								switch (cell.getCellType()) {
								case Cell.CELL_TYPE_STRING:
									String cellValue = cell.getStringCellValue();
									if (cellValue != null && !"".equalsIgnoreCase(cellValue)
											&& !"null".equalsIgnoreCase(cellValue)) {
//                                                dataObject.put(fileName + ":" + columnList.get(cellIndex), cellValue);
										dataObject[cellIndex] = cellValue;
									} else {
										dataObject[cellIndex] = "";
									}

									break;
								case Cell.CELL_TYPE_BOOLEAN:
//                                rowObj.put(header, hSSFCell.getBooleanCellValue());
									break;
								case Cell.CELL_TYPE_NUMERIC:

									if (HSSFDateUtil.isCellDateFormatted(cell)) {
										CellStyle cellStyle = cell.getCellStyle();
										Date cellDate = cell.getDateCellValue();
										String cellDateString = "";
										if ((cellDate.getYear() + 1900) == 1899 && (cellDate.getMonth() + 1) == 12
												&& (cellDate.getDate()) == 31) {
											cellDateString = (cellDate.getHours()) + ":" + (cellDate.getMinutes()) + ":"
													+ (cellDate.getSeconds());
//                                                    System.out.println("cellDateString :: "+cellDateString);
										} else {
											cellDateString = (cellDate.getYear() + 1900) + "-"
													+ (cellDate.getMonth() + 1) + "-" + (cellDate.getDate());
										}

//                                                dataObject.put(fileName + ":" + columnList.get(cellIndex), cellDateString);
										dataObject[cellIndex] = cellDateString;

									} else {
										String cellvalStr = NumberToTextConverter.toText(cell.getNumericCellValue());
										dataObject[cellIndex] = cellvalStr;
									}
									break;
								case Cell.CELL_TYPE_BLANK:
									dataObject[cellIndex] = "";
									break;
								}

							} else {
								dataObject[cellIndex] = "";
							}
						} catch (Exception e) {
							dataObject[cellIndex] = "";
							continue;
						}

					} // end of row cell loop
					dataList.add(dataObject);
				}

			} // row end

			// return result1;
			if (fis != null) {
				fis.close();
			}

		} catch (Exception e) {
			e.printStackTrace();

		}

		return dataList;
	}

	public String importFileDataToTableDirectly(HttpServletRequest request, HttpServletResponse response,
			String filepath, String tableName) {
		JSONObject resultObj = new JSONObject();
		FileInputStream fis = null;
		String resultString = "";
		Integer insertSuccessCount = 0;
		Integer insertFailureCount = 0;
		String result = "";
		String result1 = "";
//    String query = "select value from b_appl_properties where KEYNAME='UPLOAD_LIMIT'";
		Integer uploadLimit = 0;
		System.out.println("Start Date And Time :::" + new Date());

		Connection conn = null;
		PreparedStatement importStmt = null;
		int errorCount = 0;
		int rowVal = 1;
		String excluderow = "";
		try {
			// fis = new FileInputStream(new File(filepath));
			String connObj = request.getParameter("connObj");
			JSONObject connectionObj = (JSONObject) JSONValue.parse(connObj);
			conn = (Connection) getConnection(connectionObj);
			Workbook workBook = null;
			Sheet sheet = null;

			String fileExtension = filepath.substring(filepath.lastIndexOf(".") + 1, filepath.length());
			System.out.println("fileExtension:::" + fileExtension);
			if (fileExtension != null && "xls".equalsIgnoreCase(fileExtension)) {
				workBook = WorkbookFactory.create(new File(filepath));
				sheet = (HSSFSheet) workBook.getSheetAt(0);
			} else if ("csv".equalsIgnoreCase(fileExtension) || "txt".equalsIgnoreCase(fileExtension)) {
				// Handle CSV or text file
				workBook = new XSSFWorkbook(); // Dummy workbook for CSV or text processing
				sheet = workBook.createSheet();
				try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
					String line;
					int rowIndex = 0;
					while ((line = reader.readLine()) != null) {
						String[] values = line.split(","); // Adjust the delimiter as needed
						Row row = sheet.createRow(rowIndex++);
						for (int columnIndex = 0; columnIndex < values.length; columnIndex++) {
							Cell cell = row.createCell(columnIndex);
							cell.setCellValue(values[columnIndex]);
						}
					}
				}
			} else {

				System.out.println("Before::::" + new Date());
//            fis = new FileInputStream(new File(filepath));              
//            XSSFWorkbook xssfWb = (XSSFWorkbook) new XSSFWorkbook(fis);
				workBook = WorkbookFactory.create(new File(filepath));
				System.out.println("After::fileInputStream::" + new Date());
				sheet = (XSSFSheet) workBook.getSheetAt(0);
//            sheet = (XSSFSheet) xssfWb.getSheetAt(0);
			}
//       
			int lastRowNo = sheet.getLastRowNum();
			System.out.println("lastRowNo::::" + lastRowNo);
			int firstRowNo = sheet.getFirstRowNum();
			System.out.println("firstRowNo::::" + firstRowNo);
			int rowCount = lastRowNo - firstRowNo;
			System.out.println("rowCount:::::" + rowCount);

			String mappedGridColumnsArrayStr = request.getParameter("mappedGridColumnsArray");
			JSONArray mappedGridColumnsArray = (JSONArray) JSONValue.parse(mappedGridColumnsArrayStr);
			List mappedColumnsList = new ArrayList(mappedGridColumnsArray);

			String mappedFileHeadersArrayStr = request.getParameter("mappedFileHeadersArray");
			JSONArray mappedFileHeadersArray = (JSONArray) JSONValue.parse(mappedFileHeadersArrayStr);
			List mappedFileHeaders = new ArrayList(mappedFileHeadersArray);

			String fileHeadersStr = request.getParameter("fileHeaders");
			JSONArray fileHeadersArray = (JSONArray) JSONValue.parse(fileHeadersStr);
			List fileHeaders = new ArrayList(fileHeadersArray);

			// tableName = (String) (columnListArray.get(0))[1];
			System.out.println("tableName:::" + tableName);

			String insertQuery = generateInsertQuery(tableName, mappedColumnsList);
			List objectList = new ArrayList();
			System.out.println(insertQuery + ":::Start Date And Time After Query:::" + new Date());
			importStmt = conn.prepareStatement(insertQuery);
			int stmt = 1;
			String strToDateCol = "";
			JSONObject autoGenColObj = new JSONObject();

			List headersList = new ArrayList();
			Row headerROw = sheet.getRow(0);
			for (int j = 0; j < headerROw.getLastCellNum(); j++) {
				Cell cell = headerROw.getCell(j);
				String headerName = cell.getStringCellValue();
				headersList.add(headerName);
			}
			for (int i = rowVal; i < rowCount + 1; i++) {

				Row row = sheet.getRow(i);
				Map testMap = new HashMap();
//            System.out.println("row.getLastCellNum():::" + row.getLastCellNum());
				stmt = 1;
				for (int j = 0; j < mappedFileHeaders.size(); j++) {
					// System.out.println("Cell Num:::" + j + ":::Start Date And Time :::" + new
					// Date());
					String header = (String) mappedFileHeaders.get(j);
					int cellIndex = fileHeaders.indexOf(header);
					if (true) {
						try {
							Cell cell = row.getCell(cellIndex);
							if (autoGenColObj != null && !autoGenColObj.isEmpty() && autoGenColObj.containsKey(stmt)) {
//                            JSONObject paramResObj = setParamObj(request, autoGenColObj, stmt, row, importStmt, testMap, batchNumber);
								JSONObject paramResObj = new JSONObject();
								if (paramResObj != null && !paramResObj.isEmpty()) {
									stmt = (int) paramResObj.get("stmt");
									importStmt = (PreparedStatement) paramResObj.get("importStmt");
//                            testMap = (Map) paramResObj.get("testMap");
								}
							}

							if (cell != null) {
								switch (cell.getCellType()) {
								case Cell.CELL_TYPE_STRING:
//                            rowObj.put(header, hSSFCell.getStringCellValue());
									importStmt.setString(stmt, cell.getStringCellValue());
//                                testMap.put(stmt, cell.getStringCellValue());
//                                importStmt.setString(j + 1, cell.getStringCellValue());
//                                dataList.add(cell.getStringCellValue());
									break;
								case Cell.CELL_TYPE_BOOLEAN:
//                            rowObj.put(header, hSSFCell.getBooleanCellValue());
									break;
								case Cell.CELL_TYPE_NUMERIC:

									if (HSSFDateUtil.isCellDateFormatted(cell)) {
										if (strToDateCol != null && !"".equalsIgnoreCase(strToDateCol)
												&& !"null".equalsIgnoreCase(strToDateCol)
												&& strToDateCol.contains(String.valueOf(stmt))) {
											DateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
											Date convertedDate = (Date) formatter.parse(cell.toString());
											java.sql.Date sqlDat = new java.sql.Date(convertedDate.getTime());
											importStmt.setDate(stmt, sqlDat);
//                                        testMap.put(stmt, sqlDat);
										} else {
											Date cellDate = cell.getDateCellValue();
											String cellDateString = (cellDate.getYear() + 1900) + "-"
													+ (cellDate.getMonth() + 1) + "-" + (cellDate.getDate());
											importStmt.setString(stmt, cellDateString);
//                                        testMap.put(stmt, cellDateString);
										}

//                                    importStmt.setString(j + 1, cellDateString);
									} else {
										String cellvalStr = NumberToTextConverter.toText(cell.getNumericCellValue());
//                                    dataList.add(cellvalStr);
										importStmt.setString(stmt, cellvalStr);
//                                    testMap.put(stmt, cellvalStr);
//                                    importStmt.setString(j + 1, cellvalStr);
//                                rowObj.put(header, cellvalStr);
									}
									break;
								case Cell.CELL_TYPE_BLANK:
//                            rowObj.put(header, "");
//                                dataList.add("");
									importStmt.setString(stmt, "");
//                                testMap.put(stmt, "");
//                                importStmt.setString(j + 1, "");
									break;
								}

							} else {
								importStmt.setString(stmt, "");
//                        testMap.put(stmt, "");
							}
						} catch (Exception e) {
							e.printStackTrace();
//                    dataList.add("");
							importStmt.setString(stmt, "");
//                    testMap.put(stmt, "");
							continue;
//                    importStmt.setString(j + 1, "");
						}
						stmt++;
					}

				} // end of row cell loop

				importStmt.addBatch();
				if (i != 0 && i % batchSize == 0) {

					importStmt.executeBatch();
					importStmt.clearBatch(); // not sure if this is necessary
					System.out.println("Batch Excuted:::" + i);
				}

			} // row end
			int[] insertBulkCount = importStmt.executeBatch();
			System.out.println("insertBulkCount::::" + insertBulkCount);
			Integer count = Arrays.stream(insertBulkCount).sum();
			System.out.println(count + ":::End Insert Date And Time :::" + new Date());

			result1 = rowCount + " record(s) successfully imported";
			resultObj.put("flag", true);

			System.out.println(new Date() + "End Date And Time :::" + (rowCount) + ":::" + insertFailureCount);
			// return result1;
			if (fis != null) {
				fis.close();
			}
		} catch (Exception e) {
			e.printStackTrace();

			errorCount = errorCount++;
			result = errorCount + " Record(s) failed to import";
			resultObj.put("flag", false);
			resultObj.put("errorCount", errorCount);

		} finally {
			try {
				if (importStmt != null) {
					importStmt.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (Exception e) {
				errorCount = errorCount++;
				result = errorCount + " Record(s) failed to import";
				resultObj.put("errorCount", errorCount);
				resultObj.put("flag", false);
			}
		}
		String finalresult = result1 + " " + result;
		System.out.println("final result:::" + finalresult);
		resultObj.put("finalresult", finalresult);

		return resultObj.toJSONString();
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
			if (("jpeg".equalsIgnoreCase(fileType1) || "jpg".equalsIgnoreCase(fileType1)
					|| "png".equalsIgnoreCase(fileType1)) && selectedFiletype != null
					&& "Image".equalsIgnoreCase(selectedFiletype)) {
				selectedFiletype = fileType1;
			}
			if (selectedFiletype != null && !"".equalsIgnoreCase(selectedFiletype) && fileType1 != null
					&& !"".equalsIgnoreCase(fileType1) && !selectedFiletype.equalsIgnoreCase(fileType1)
					&& !"TXT".equalsIgnoreCase(fileType1) && !"TEST".equalsIgnoreCase(selectedFiletype)) {
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

					// need to save
					if ("json".equalsIgnoreCase(fileType1)) {
						// need to convert from json to CSV
						try {
							mainFileName = convertJSONtoCSV(file, excelFilePath);
						} catch (Exception e) {
						}

					}
					try {
						genericDataPipingDAO.saveUserFiles(request, filename, mainFileName, excelFilePath, fileType1);
					} catch (Exception e) {
					}
					result = "File imported successfully,Please check in Available Connections";
					importResult.put("result", result);
					importResult.put("flag", "Success");
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

	public String importTreeDMDifferentFiles(HttpServletRequest request, HttpServletResponse response,
			JSONObject jsonData, String selectedFiletype, String sheet) {

		String result = "";
		FileInputStream inputStream = null;
		FileOutputStream outs = null;
		JSONObject importResult = new JSONObject();
		try {
			if (true) {
				// fis = new FileInputStream(new File(filepath));
				String originalFileName = request.getParameter("fileName");
				String userName = (String) request.getSession(false).getAttribute("ssUsername");
				String filePath = etlFilePath + "Files/TreeDMImport" + File.separator + userName;
				String mainFileName = "SPIRUploadSheet" + System.currentTimeMillis() + "_" + sheet + "."
						+ selectedFiletype;
				String fileName = filePath + File.separator + mainFileName;

				String headersObjStr = request.getParameter("headersObj");
				JSONObject headersObj = (JSONObject) JSONValue.parse(headersObjStr);

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
				CreationHelper createHelper = outputWb.getCreationHelper();
				dateCellStyle.setDataFormat(createHelper.createDataFormat().getFormat("dd-MM-yyyy"));

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
											outputCell.setCellStyle(dateCellStyle);

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

				outs = new FileOutputStream(fileName);
				outputWb.write(outs);
				outs.close();
				try {
					genericDataPipingDAO.saveUserFiles(request, originalFileName, mainFileName, filePath,
							selectedFiletype);
				} catch (Exception e) {
				}
				result = "File imported successfully,Please check in Available Connections";
				importResult.put("result", result);
				importResult.put("fileName", mainFileName);
			}
			// return result1;
			if (inputStream != null) {
				inputStream.close();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return importResult.toJSONString();
	}

	public String importTreeDMFileXlsx(HttpServletRequest request, HttpServletResponse response, JSONObject jsonData,
			String selectedFiletype) {

		String result = "";
		FileInputStream inputStream = null;
		FileOutputStream outs = null;
		JSONObject importResult = new JSONObject();
		try {
			if (true) {
				// fis = new FileInputStream(new File(filepath));
				String originalFileName = request.getParameter("fileName");
				String userName = (String) request.getSession(false).getAttribute("ssUsername");
				String filePath = etlFilePath + "Files/TreeDMImport" + File.separator + userName;
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
				try {
					genericDataPipingDAO.saveUserFiles(request, originalFileName, mainFileName, filePath,
							selectedFiletype);
				} catch (Exception e) {
				}
				result = "File imported successfully,Please check in Available Connections";
				importResult.put("result", result);
				importResult.put("fileName", mainFileName);
			}
			// return result1;
			if (inputStream != null) {
				inputStream.close();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return importResult.toJSONString();
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

	public boolean isCharacter(String str) {
		if (str == null) {
			return false;
		}
		if (str.length() == 1) {
			return true;
		} else {
			return false;
		}

	}

	public String getTreeUploadedFiles(HttpServletRequest request) {
		JSONArray treeDataObjArray = new JSONArray();
		try {
			String level = request.getParameter("level");
			System.out.println("level:::" + level);
			if ("1".equalsIgnoreCase(level)) {
				List<String> fileTypesList = genericDataPipingDAO.getAllFilesType(request);
				if (fileTypesList != null && !fileTypesList.isEmpty()) {
					for (int i = 0; i < fileTypesList.size(); i++) {
						String fileTypeName = fileTypesList.get(i);
						JSONObject treeObj = new JSONObject();
						treeObj.put("label", fileTypeName);
						// treeObj.put("id", treeDataArray[0]);
						treeObj.put("description", fileTypeName);
						treeObj.put("parentKeyValue", "FILES");
						JSONArray childArray = new JSONArray();
						JSONObject dummyObj = new JSONObject();
						dummyObj.put("value", "ajax");
						dummyObj.put("label", fileTypeName);
						childArray.add(dummyObj);
						treeObj.put("items", childArray);
						treeObj.put("value", fileTypeName);
						treeDataObjArray.add(treeObj);
					}
				}
			} else if ("2".equalsIgnoreCase(level)) {
				String parentkey = request.getParameter("parentkey");
				List<Object[]> getFileNamesList = genericDataPipingDAO.allFileNamesListByType(request, parentkey);
				if (getFileNamesList != null && !getFileNamesList.isEmpty()) {
					for (int i = 0; i < getFileNamesList.size(); i++) {
						Object[] fileNamesArray = getFileNamesList.get(i);
						try {

							File folder = new File((String) fileNamesArray[2]);
							if (!folder.exists()) {
								folder.mkdirs();
							}
							String filePath = (String) fileNamesArray[2] + File.separator + (String) fileNamesArray[1];
							File file = new File(filePath);
							if (file.exists()) {
								file.delete();
							}
							Blob blob = (Blob) fileNamesArray[4];
//                    InputStream in = blob.getBinaryStream();
							OutputStream out = new FileOutputStream(file);
							byte[] buff = blob.getBytes(1, (int) blob.length());
							out.write(buff);
							out.close();

						} catch (Exception e) {
							e.printStackTrace();
						}
						try {
							if (fileNamesArray != null && fileNamesArray.length != 0) {
								String pathLocation = "" + fileNamesArray[2];
								String fileName = "" + fileNamesArray[1];
								if (pathLocation != null && !"".equalsIgnoreCase(pathLocation)
										&& !"null".equalsIgnoreCase(pathLocation)) {
									JSONObject treeObj = new JSONObject();
									treeObj.put("label", fileNamesArray[0]);
									// treeObj.put("id", treeDataArray[0]);
									treeObj.put("description", fileNamesArray[0]);
									JSONArray childArray = new JSONArray();
									JSONObject dummyObj = new JSONObject();
									dummyObj.put("value", "ajax");
									dummyObj.put("label", fileNamesArray[0]);
//                                    childArray.add(dummyObj);
									treeObj.put("items", childArray);
									treeObj.put("value", pathLocation + File.separator + fileName);
									treeDataObjArray.add(treeObj);
								}
							}
						} catch (Exception e) {
							continue;
						}

					}
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return treeDataObjArray.toJSONString();
	}
//    public String getTreeUploadedFiles(HttpServletRequest request) {
//        JSONArray treeDataObjArray = new JSONArray();
//        try {
//            String pathLocation = etlFilePath+"Files/TreeDMImport/" + request.getSession(false).getAttribute("ssUsername");
//            File file = new File(pathLocation);
//            String[] fileList = file.list();
//
//            for (int i = 0; i < fileList.length; i++) {
//                String fileName = fileList[i];
//                System.out.println(fileName);
//                JSONObject treeObj = new JSONObject();
//                treeObj.put("label", fileName);
//                //treeObj.put("id", treeDataArray[0]);
//                treeObj.put("description", fileName);
//                JSONArray childArray = new JSONArray();
//                JSONObject dummyObj = new JSONObject();
//                dummyObj.put("value", "ajax");
//                dummyObj.put("label", fileName);
//                childArray.add(dummyObj);
//                treeObj.put("items", childArray);
//                treeObj.put("value", fileName);
//                treeDataObjArray.add(treeObj);
//            }
//        } catch (Exception ex) {
//            ex.printStackTrace();
//        }
//        return treeDataObjArray.toJSONString();
//    }

	public JSONObject getJoinTableRows(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		Connection fromconnection = null;
		try {
			String tableResult = "";
			String columnsResult = "";
			JSONArray sourceTablesArr = new JSONArray();
			JSONObject fromConnectObj = new JSONObject();
			JSONObject dbFromObj = new JSONObject();
			List destinationColumnList = new ArrayList();
			String sourceTables = request.getParameter("sourceTables");
			String rowId = request.getParameter("rowId");
			String fromConnObj = request.getParameter("connObj");
			if (fromConnObj != null && !"".equalsIgnoreCase(fromConnObj) && !"null".equalsIgnoreCase(fromConnObj)) {
				fromConnectObj = (JSONObject) JSONValue.parse(fromConnObj);
			}
			if (fromConnectObj != null && !fromConnectObj.isEmpty()) {
				dbFromObj.put("selectedItemLabel", fromConnectObj.get("CONN_CUST_COL1"));
				dbFromObj.put("hostName", fromConnectObj.get("HOST_NAME"));
				dbFromObj.put("port", fromConnectObj.get("CONN_PORT"));
				dbFromObj.put("userName", fromConnectObj.get("CONN_USER_NAME"));
				dbFromObj.put("password", fromConnectObj.get("CONN_PASSWORD"));
				dbFromObj.put("serviceName", fromConnectObj.get("CONN_DB_NAME"));
			}
			Object fromReturendObj = dataMigrationService.getConnection(dbFromObj);
			if (fromReturendObj instanceof Connection) {
				fromconnection = (Connection) fromReturendObj;
			}
			int i = 0;
			if (rowId != null && !"".equalsIgnoreCase(rowId)) {
				i = Integer.parseInt(rowId);
				if (i == 0) {
					i = i + 2;
				} else {
					++i;
				}
			}
			if (sourceTables != null && !"".equalsIgnoreCase(sourceTables) && !"null".equalsIgnoreCase(sourceTables)) {
				sourceTablesArr = (JSONArray) JSONValue.parse(sourceTables);
			}
			int s = Integer.parseInt(rowId);
			++s;
			if (sourceTablesArr != null && !sourceTablesArr.isEmpty() && sourceTablesArr.size() > 1) {
				tableResult += "<div><table id='selectedJoinTables_" + s
						+ "'><tr><td id=\"SOURCE_LEFT_JOIN_SELECT_TABLES_ 0\" class=\"sourceJoinColsTd\">"
						+ "<input type='text' id='SOURCE_SELECT_JOIN_TABLES_0' value='"
						+ (String) sourceTablesArr.get(i) + "'/>" + "</td>" + "</tr></table></div>";
				columnsResult += "<div>"
						+ "<div id ='addJoinTableRowId' style='cursor: pointer;'><img src='images/Add icon.svg' onclick=addJoinTableRow('"
						+ dbFromObj.toJSONString() + "',selectedJoinColumns,'" + s + "')/></div>"
						+ "<table id='selectedJoinColumns_" + s + "'>";
				if (fromconnection != null) {
					for (int j = 0; j < sourceTablesArr.size(); j++) {
						List<Object[]> destinationsColumnList = getTreeDMTableColumns(fromconnection, request,
								dbFromObj, (String) sourceTablesArr.get(j));
						destinationColumnList.addAll(destinationsColumnList);
						columnsResult += "<tr><td class=\"sourceJoinColsTd\">"
								+ "<select id=\"SOURCE_SELECT_JOIN_LEFT_COLUMNS_0\"  class=\"sourceColsJoinSelectBox\">"
								+ "" + dataMigrationService.generateSelectBoxStr(destinationColumnList, "",
										"SOURCE_SELECT_LEFT_JOIN_COLUMNS_0")
								+ "" + "</select>" + "</td></tr>";
					}
					List<Object[]> destinationsColumnList = getTreeDMTableColumns(fromconnection, request, dbFromObj,
							(String) sourceTablesArr.get(i));
					columnsResult += "<tr><td class=\"sourceJoinColsTd\">"
							+ "<select id=\"SOURCE_SELECT_JOIN_LEFT_COLUMNS_1\"  class=\"sourceColsJoinSelectBox\">"
							+ "" + dataMigrationService.generateSelectBoxStr(destinationsColumnList, "",
									"SOURCE_SELECT_LEFT_JOIN_COLUMNS_1")
							+ "" + "</select>" + "</td></tr>";
					columnsResult += "</table></div>";
				}
			}
			resultObj.put("tableRowData", tableResult);
			resultObj.put("columnRowData", columnsResult);
			resultObj.put("tableRowId", "<img src='images/Add icon.svg' onclick=addJoinTableRow('"
					+ dbFromObj.toJSONString() + "',selectedJoinTables,'" + s + "')/>");
			// resultObj.put("columnRowId", "<img src='images/Add icon.svg'
			// onclick=addJoinColumnRow('" + dbFromObj.toJSONString() +
			// "',selectedJoinColumns,'" + s + "')/>");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
//                Connection fromConnection = null;
//                Connection toConnection = null;
				if (fromconnection != null) {
					fromconnection.close();
				}

			} catch (Exception e) {
			}
		}
		return resultObj;
	}

	public String generateSelectBoxStr(String tableName, String selectBoxId) {
		String selectBoxStr = "<option>Select</option>";
		try {
			if (tableName != null && !"".equalsIgnoreCase(tableName) && !"null".equalsIgnoreCase(tableName)) {
				selectBoxStr += "<option  value='" + tableName + "'" + " id ='" + tableName + "' data-tablename='"
						+ tableName + "' " + ">" + tableName + "</option>";
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return selectBoxStr;
	}

	public List<Object[]> getTreeDMTableColumns(Connection connection, HttpServletRequest request, JSONObject dbObj,
			String tableName) {
		List<Object[]> sourceColumnsList = new ArrayList<>();
		Connection con = null;
		try {
			con = (Connection) dataMigrationService.getConnection(dbObj);
			String initParamClassName = "com.pilog.mdm.DAO.V10DataMigrationAccessDAO";
			String initParamMethodName = "getTree" + dbObj.get("selectedItemLabel") + "TableColumns";
			System.out.println(
					" initParamClassName:" + initParamClassName + "initParamMethodName:" + initParamMethodName);
			Class clazz = Class.forName(initParamClassName);
			Class<?>[] paramTypes = { Connection.class, HttpServletRequest.class, String.class, String.class };
			Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
			Object targetObj = new PilogUtilities().createObjectByClass(clazz);
			sourceColumnsList = (List<Object[]>) method.invoke(targetObj, con, request,
					String.valueOf(dbObj.get("serviceName")), tableName);

		} catch (Exception e) {

			e.printStackTrace();
		} finally {
			try {
				if (con != null) {
					con.close();
				}
			} catch (Exception ex) {

			}
		}
		return sourceColumnsList;
	}

	public String getTreeParentUploadedFiles(HttpServletRequest request, String treeId) {
		JSONObject treeFileObj = new JSONObject();
		try {
			String dragEndFunc = "";
			JSONArray treeDataObjArray = new JSONArray();
			JSONObject treeObj = new JSONObject();
			treeObj.put("label", "Uploaded Files");
			treeObj.put("description", "Uploaded Files");
			JSONArray childArray = new JSONArray();
			JSONObject dummyObj = new JSONObject();
			dummyObj.put("value", "ajax");
			dummyObj.put("label", "Uploaded Files");
			childArray.add(dummyObj);
			treeObj.put("items", childArray);
			treeObj.put("value", "Uploaded Files");
			treeDataObjArray.add(treeObj);
			treeFileObj.put("source", treeDataObjArray);
			treeFileObj.put("width", "100%");
			treeFileObj.put("height", "50%");
			treeFileObj.put("allowDrag", "true");
			treeFileObj.put("allowDrop", "true");
			if (treeId != null && !"".equalsIgnoreCase(treeId) && "uploadDestFiles".equalsIgnoreCase(treeId)) {
				dragEndFunc = "function (item) {\n"
						+ "                                        createImageDestinationFlowchart(item,item.label,'FileData');\n"
						+ "//                                        $('#feedHeader').append(item.label);\n"
						+ "                                    }";
			} else if (treeId != null && !"".equalsIgnoreCase(treeId) && "uploadSourceFiles".equalsIgnoreCase(treeId)) {
				dragEndFunc = "function (item) {\n"
						+ "                                      createImageSourcesFlowchart(item,item.label,'FileData');\n"
						+ "//                                        $('#feedHeader').append(item.label);\n"
						+ "                                    }";
			}
			treeFileObj.put("dragEnd", dragEndFunc);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return treeFileObj.toJSONString();
	}

	public String fetchJoinsData(HttpServletRequest request) {
		String joinsDataStr = "";
		JSONObject resultObj = new JSONObject();
		Connection fromConnection = null;
		Connection toConnection = null;

		try {
			JSONObject dbFromObj = new JSONObject();
			JSONObject dbTOObj = new JSONObject();
			JSONObject fromConnectObj = new JSONObject();
			JSONObject toConnectObj = new JSONObject();
			JSONArray sourceTablesArr = new JSONArray();
			String fromConnObj = request.getParameter("fromConnObj");
			String toConnObj = request.getParameter("toConnObj");
			String fromTable = request.getParameter("fromTable");
			String toTable = request.getParameter("toTable");
			String sourceTables = request.getParameter("sourceTables");
			String selectedJoinTables = "";
			if (sourceTables != null && !"".equalsIgnoreCase(sourceTables) && !"".equalsIgnoreCase(sourceTables)) {
				sourceTablesArr = (JSONArray) JSONValue.parse(sourceTables);
			}
			if (fromConnObj != null && !"".equalsIgnoreCase(fromConnObj) && !"null".equalsIgnoreCase(fromConnObj)) {
				fromConnectObj = (JSONObject) JSONValue.parse(fromConnObj);
			}
			if (toConnObj != null && !"".equalsIgnoreCase(toConnObj) && !"null".equalsIgnoreCase(toConnObj)) {
				toConnectObj = (JSONObject) JSONValue.parse(toConnObj);
			}
			if (fromConnectObj != null && !fromConnectObj.isEmpty() && toConnectObj != null
					&& !toConnectObj.isEmpty()) {
				dbFromObj.put("selectedItemLabel", fromConnectObj.get("CONN_CUST_COL1"));
				dbFromObj.put("hostName", fromConnectObj.get("HOST_NAME"));
				dbFromObj.put("port", fromConnectObj.get("CONN_PORT"));
				dbFromObj.put("userName", fromConnectObj.get("CONN_USER_NAME"));
				dbFromObj.put("password", fromConnectObj.get("CONN_PASSWORD"));
				dbFromObj.put("serviceName", fromConnectObj.get("CONN_DB_NAME"));

				dbTOObj.put("selectedItemLabel", toConnectObj.get("CONN_CUST_COL1"));
				dbTOObj.put("hostName", toConnectObj.get("HOST_NAME"));
				dbTOObj.put("port", toConnectObj.get("CONN_PORT"));
				dbTOObj.put("userName", toConnectObj.get("CONN_USER_NAME"));
				dbTOObj.put("password", toConnectObj.get("CONN_PASSWORD"));
				dbTOObj.put("serviceName", toConnectObj.get("CONN_DB_NAME"));
			}
			String abc = dbTOObj.toString();
			Object fromReturendObj = dataMigrationService.getConnection(dbFromObj);
			Object toReturendObj = dataMigrationService.getConnection(dbTOObj);
			String matchedSelectStr = "";
			if (toReturendObj instanceof Connection && fromReturendObj instanceof Connection) {
				fromConnection = (Connection) fromReturendObj;
				toConnection = (Connection) toReturendObj;
			}

			joinsDataStr += "<div class=\"visionEtlMappingMain\">" + ""
					+ "<div class=\"visionEtlMappingTablesDiv visionEtlJoinrClauseTablesDiv\">"
					+ "<table class=\\\"visionEtlMappingTables\\\" id='EtlMappingTable' style='width: 50%;' border='1'>"
					+ "<thead>"
					+ "<tr><th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center' colspan=\"2\">Tables</th>";

			if (sourceTablesArr != null && sourceTablesArr.size() > 1) {
				for (int i = 0; i < sourceTablesArr.size(); i++) {
					joinsDataStr += "<tr><td class=\"sourceJoinColsTd\">" + "<select id=\"SOURCE_SELECT_JOIN_TABLES_"
							+ i + "\"  class=\"sourceColsJoinSelectBox\">" + ""
							+ generateTableSelectBoxStr(sourceTablesArr, (String) sourceTablesArr.get(i),
									"SOURCE_SELECT_JOIN_TABLES_" + i + "")
							+ "" + "</select>" + "</td>" + "<td>";
					if (i != 0) {
						joinsDataStr += "<img src=\"images/mapping.svg\" data-mappedcolumns='' id=\"joinConditionsMap_"
								+ i + "\" "
								+ "class=\"visionEtlMapTableIcon visionEtlJoinClauseMapIcon\" title=\"Map Columns For Join\""
								+ " onclick=showJoinsPopup(event,'" + sourceTablesArr.get(i) + "',id," + i + ")"
								+ " style=\"width:15px;height: 15px;cursor:pointer;\"/>";
					}
					joinsDataStr += "</td>" + "</tr>";

				}
			}

			joinsDataStr += "</tbody>" + "" + "</table>" + "" + "</div>" + "</div>";

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (fromConnection != null) {
					fromConnection.close();
				}
				if (toConnection != null) {
					toConnection.close();
				}
			} catch (Exception e) {
			}
		}
		return joinsDataStr;

	}

	public JSONObject fetchJoinTablesData(HttpServletRequest request) {
		String joinsDataStr = "";
		JSONObject resultObj = new JSONObject();
		Connection connection = null;

		try {
			JSONObject dbObj = new JSONObject();
			JSONObject connectObj = new JSONObject();
			List<Object[]> allColumnList = new ArrayList();

			JSONArray sourceTablesArr = new JSONArray();
			String dbObjStr = request.getParameter("dbObj");
			String tableName = request.getParameter("tableName");
			String sourceTables = request.getParameter("sourceTables");
			String iconIndex = request.getParameter("iconIndex");
			String joinColumnMapping = request.getParameter("joinColumnMapping");
			JSONObject joinColumnMappingObj = new JSONObject();
			if (joinColumnMapping != null && !"".equalsIgnoreCase(joinColumnMapping)) {
				joinColumnMappingObj = (JSONObject) JSONValue.parse(joinColumnMapping);
			}

			if (sourceTables != null && !"".equalsIgnoreCase(sourceTables) && !"".equalsIgnoreCase(sourceTables)) {
				sourceTablesArr = (JSONArray) JSONValue.parse(sourceTables);
			}
			if (dbObjStr != null && !"".equalsIgnoreCase(dbObjStr) && !"null".equalsIgnoreCase(dbObjStr)) {
				dbObj = (JSONObject) JSONValue.parse(dbObjStr);
			}

			Object fromReturendObj = dataMigrationService.getConnection(dbObj);

			String matchedSelectStr = "";
			if (fromReturendObj instanceof Connection) {
				connection = (Connection) fromReturendObj;

			}

			String trString = "<tr>";
			List<Object[]> destinationsColumnList = getTreeDMTableColumns(connection, request, dbObj, tableName);
			trString += "<td width='5%'><img src=\"images/delete.gif\" onclick='deleteSelectedRow(this)'  class=\"visionTdETLIcons\""
					+ " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>" + "</td>";
			trString += "<td width='35%' class=\"sourceJoinColsTd\">"
					+ "<select id=\"CHILD_TABLE_COLUMNS_0\"  class=\"sourceColsJoinSelectBox\">" + ""
					+ dataMigrationService.generateSelectBoxStr(destinationsColumnList,
							destinationsColumnList.get(0)[0] + ":" + destinationsColumnList.get(0)[1],
							"CHILD_TABLE_COLUMNS")
					+ "" + "</select>" + "</td>";

			trString += "<td width='15%' class=\"sourceJoinColsTd\">"
					+ "<select id=\"OPERATOR_TYPE\"  class=\"sourceColsJoinSelectBox\">"
					+ "<option  value='=' selected>=</option>" + "<option  value='!='>!=</option>"
					// +"<option value='like'>like</option>"
					+ "</select>" + "</td>";

			for (int i = 0; i < sourceTablesArr.size(); i++) {
				if (!tableName.equalsIgnoreCase((String) sourceTablesArr.get(i))) {
					List<Object[]> columnList = getTreeDMTableColumns(connection, request, dbObj,
							(String) sourceTablesArr.get(i));
					allColumnList.addAll(columnList);
				}
			}

			trString += "<td width='35%' class=\"sourceJoinColsTd\">"
					+ "<select id=\"MASTER_TABLE_COLUMNS_0\"  class=\"sourceColsJoinSelectBox\">" + ""
					+ dataMigrationService.generateSelectBoxStr(allColumnList,
							allColumnList.get(0)[0] + ":" + allColumnList.get(0)[1], "MASTER_TABLE_COLUMNS")
					+ "" + "</select>" + "</td>" + "<td width='10%'>" + "<select id='andOrOpt'>"
					+ "<option value='AND'>AND</option>" + "<option value='OR'>OR</option>" + "</select>" + "</td>";
			trString += "</tr>";
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
						mappedColTrString += "<td width='5%' ><img src=\"images/delete.gif\" onclick='deleteSelectedRow(this)'  class=\"visionTdETLIcons\""
								+ " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>" + "</td>";
						mappedColTrString += "<td width='35%' class=\"sourceJoinColsTd\">"
								+ "<select id=\"CHILD_TABLE_COLUMNS_" + i + "\"  class=\"sourceColsJoinSelectBox\">"
								+ ""
								+ dataMigrationService.generateSelectBoxStr(destinationsColumnList,
										(String) joinColMapObj.get("childTableColumn"), "CHILD_TABLE_COLUMNS_" + i + "")
								+ "" + "</select>" + "</td>";
						String operator = (String) joinColMapObj.get("operator");

						mappedColTrString += "<td width='15%' class=\"sourceJoinColsTd\">"
								+ "<select id=\"OPERATOR_TYPE\"  class=\"sourceColsJoinSelectBox\">";
						mappedColTrString += "<option  value='=' " + ("=".equalsIgnoreCase(operator) ? "selected" : "")
								+ ">=</option>";
						mappedColTrString += "<option  value='!=' "
								+ ("!=".equalsIgnoreCase(operator) ? "selected" : "") + ">!=</option>";
						// +"<option value='like'>like</option>"
						mappedColTrString += "</select>" + "</td>";

						mappedColTrString += "<td width='35%' class=\"sourceJoinColsTd\">"
								+ "<select id=\"MASTER_TABLE_COLUMNS_" + i + "\"  class=\"sourceColsJoinSelectBox\">"
								+ ""
								+ dataMigrationService.generateSelectBoxStr(allColumnList,
										(String) joinColMapObj.get("masterTableColumn"),
										"MASTER_TABLE_COLUMNS_" + i + "")
								+ "" + "</select>" + "</td>" + "<td width='10%'>" + "<select id='andOrOpt'>";
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
			joinsDataStr += "<div class=\"visionEtlJoinClauseMain\">" + "<div class=\"visionEtlAddIconDiv\">"
					+ "<img data-trstring='' src=\"images/Add icon.svg\" id=\"visionEtlAddRowIcon\" "
					+ "class=\"visionEtlAddRowIcon\" title=\"Add column for mapping\""
					+ " onclick=addNewJoinsRow(event,'',id) "
					+ "style=\"width:15px;height: 15px;cursor:pointer; float: left;\"/>"
					// + "</div>"
					// + "<div class=\"visionEtlJoinTypes\">"
					+ "<span class='visionColumnJoinType'>Join Type : </span>"
					+ "<select class='visionColumnJoinTypeSelect' id='joinType'>"
					+ "<option value='INNER JOIN'>Inner Join</option>" + "<option value='JOIN'>Join</option>"
					+ "<option value='LEFT OUTER JOIN'>Left Outer Join</option>"
					+ "<option value='RIGHT OUTER JOIN'>Right Outer Join</option>"
					+ "<option value='OUTER JOIN'>Outer Join</option>" + "</select>" + "</div>"
					+ "<div class=\"visionEtlJoinClauseTablesDiv\">"
					+ "<table class=\"visionEtlJoinClauseTable\" id='etlJoinClauseTable' style='width: 100%;' border='1'>"
					+ "<thead>" + "<tr>"
					+ "<th width='5%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'></th>"
					+ "<th width='35%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Child Column</th>"
					+ "<th width='15%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Operator</th>"
					+ "<th width='35%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Master Column</th>"
					+ "<th width='10%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>AND/OR</th>"
					+ "" + "</tr>" + "</thead>" + "<tbody>" + "";
			joinsDataStr += mappedColTrString + "</tbody>" + "" + "</table>" + "" + "</div>" + "</div>";
			resultObj.put("joinsDataStr", joinsDataStr);
			resultObj.put("trString", trString);

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

	public JSONObject processETLData(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		Connection fromConnection = null;
		Connection toConnection = null;
		JCO.Client fromJCOConnection = null;
		JCO.Client toJCOConnection = null;
		String jobId = request.getParameter("jobId");
		if (!(jobId != null && !"".equalsIgnoreCase(jobId) && !"null".equalsIgnoreCase(jobId))) {
			jobId = AuditIdGenerator.genRandom32Hex();
		}
		try {

			try {
				genericDataPipingDAO.deleteProcesslog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), jobId);
			} catch (Exception e) {
			}
			try {
				genericProcessETLDataService.processETLLog(
						(String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), "ETL Process is started", "INFO",
						20, "Y", jobId);
			} catch (Exception e) {
			}
			String mappedDataStr = request.getParameter("mappedData");

			String currentTrnsOpId = request.getParameter("currentTrnsOpId");
			String columnsObjStr = request.getParameter("columnsObj");
			String whereClauseObjStr = request.getParameter("whereClauseObj");
			String defaultValObjStr = request.getParameter("defaultValObj");
			String appendValObjStr = request.getParameter("appendValObj");
			String selectTabObjStr = request.getParameter("selectTabObj");
			String columnClauseObjStr = request.getParameter("columnClauseObj");
			String joinQuery = request.getParameter("joinQuery");
			String joinQueryMapObjStr = request.getParameter("joinQueryMapObj");
			String orderByStr = request.getParameter("orderBy");
			String groupByStr = request.getParameter("groupBy");
			String nativeSQL = request.getParameter("nativeSQL");
			// JSONObject columnsObj = new JSONObject();
			Map columnsObj = new LinkedHashMap(); // ravi etl integration
			JSONObject tablesWhereClauseObj = new JSONObject();
			JSONObject defaultValuesObj = new JSONObject();
			JSONObject appendValObj = new JSONObject();
			JSONObject selectTabObj = new JSONObject();
			JSONObject columnClauseObj = new JSONObject();
			JSONObject joinQueryMapObj = new JSONObject();
			JSONObject orderByObj = new JSONObject();
			if (columnsObjStr != null && !"".equalsIgnoreCase(columnsObjStr)
					&& !"null".equalsIgnoreCase(columnsObjStr)) {
				columnsObj = (JSONObject) JSONValue.parse(columnsObjStr);
			}
			if (whereClauseObjStr != null && !"".equalsIgnoreCase(whereClauseObjStr)
					&& !"null".equalsIgnoreCase(whereClauseObjStr)) {
				tablesWhereClauseObj = (JSONObject) JSONValue.parse(whereClauseObjStr);
			}
			if (defaultValObjStr != null && !"".equalsIgnoreCase(defaultValObjStr)
					&& !"null".equalsIgnoreCase(defaultValObjStr)) {
				defaultValuesObj = (JSONObject) JSONValue.parse(defaultValObjStr);
			}
			if (appendValObjStr != null && !"".equalsIgnoreCase(appendValObjStr)
					&& !"null".equalsIgnoreCase(appendValObjStr)) {
				appendValObj = (JSONObject) JSONValue.parse(appendValObjStr);
			}
			if (columnClauseObjStr != null && !"".equalsIgnoreCase(columnClauseObjStr)
					&& !"null".equalsIgnoreCase(columnClauseObjStr)) {
				columnClauseObj = (JSONObject) JSONValue.parse(columnClauseObjStr);
			}
			if (selectTabObjStr != null && !"".equalsIgnoreCase(selectTabObjStr)
					&& !"null".equalsIgnoreCase(selectTabObjStr)) {
				selectTabObj = (JSONObject) JSONValue.parse(selectTabObjStr);
			}
			if (!(selectTabObj != null && !selectTabObj.isEmpty())) {
				selectTabObj = new JSONObject();
			}
			selectTabObj.put("jobId", jobId);
			if (joinQueryMapObjStr != null && !"".equalsIgnoreCase(joinQueryMapObjStr)
					&& !"null".equalsIgnoreCase(joinQueryMapObjStr)) {
				joinQueryMapObj = (JSONObject) JSONValue.parse(joinQueryMapObjStr);
			}
			if (orderByStr != null && !"".equalsIgnoreCase(orderByStr) && !"null".equalsIgnoreCase(orderByStr)) {
				orderByObj = (JSONObject) JSONValue.parse(orderByStr);
			}
			if (mappedDataStr != null && !"".equalsIgnoreCase(mappedDataStr)) {
				Map mappedData = (Map) JSONValue.parse(mappedDataStr);
				Map mappingOperatorObj = new HashMap();
				Map nonMapOperatorObj = new HashMap();
				if (mappedData != null && !mappedData.isEmpty()) {
					Map operatorsMap = (Map) mappedData.get("operators");
					Map linksMap = (Map) mappedData.get("links");
//                    System.out.println("operatorsMap::::" + operatorsMap);
//                    System.out.println("linksMap::::" + linksMap);
					if (operatorsMap != null && !operatorsMap.isEmpty()) {
						mappingOperatorObj = (Map) operatorsMap.keySet().stream().filter(keyName -> (keyName != null
								&& (Map) operatorsMap.get(keyName) != null
								&& ("MAP".equalsIgnoreCase(
										String.valueOf(((Map) operatorsMap.get(keyName)).get("iconType")))
										|| ("UNGROUP".equalsIgnoreCase(
												String.valueOf(((Map) operatorsMap.get(keyName)).get("iconType"))))
										|| ("GROUP".equalsIgnoreCase(
												String.valueOf(((Map) operatorsMap.get(keyName)).get("iconType")))))))
								// ((Map) operatorsMap.get(keyName))
								// .containsKey("mapType")))
								.collect(Collectors.toMap(keyName -> keyName,
										keyName -> (Map) operatorsMap.get(keyName)));

//                        System.out.println("mappingOperatorObj:::" + mappingOperatorObj);
//                        System.out.println("nonMapOperatorObj:::" + nonMapOperatorObj);
						if (mappingOperatorObj != null && !mappingOperatorObj.isEmpty()) {

							for (Object mappedKey : mappingOperatorObj.keySet()) {
								List<Map> fromOperatorList = new ArrayList<>();
								List<Map> toOperatorList = new ArrayList<>();
								if (linksMap != null && !linksMap.isEmpty()) {
									Map fromOperator = new HashMap();
									Map toOperator = new HashMap();
									for (Object linkKey : linksMap.keySet()) {
										Map linkMap = (Map) linksMap.get(linkKey);
										if (linkMap != null && !linkMap.isEmpty() && String.valueOf(mappedKey)
												.equalsIgnoreCase(String.valueOf(linkMap.get("toOperator")))) {
											System.out.println(
													"linkMap.get(\"fromOperator\")::" + linkMap.get("fromOperator"));
											String fromOperatorId = String.valueOf(linkMap.get("fromOperator"));
											fromOperatorList.add((Map) operatorsMap
													.get(String.valueOf(linkMap.get("fromOperator"))));

										}
										if (linkMap != null && !linkMap.isEmpty() && String.valueOf(mappedKey)
												.equalsIgnoreCase(String.valueOf(linkMap.get("fromOperator")))) {
											toOperator = (Map) operatorsMap
													.get(String.valueOf(linkMap.get("toOperator")));
											toOperatorList.add(toOperator);

										}
									}

									if (fromOperatorList != null && !fromOperatorList.isEmpty()) {
										boolean isSameDataBase = fromOperatorList.stream()
												.filter(fromOpMap -> (fromOpMap.containsKey("CONNECTION_NAME")
														&& fromOpMap.containsKey("CONN_DB_NAME")))
												.map(fromOpMap -> (fromOpMap.get("CONNECTION_NAME") + ":::"
														+ fromOpMap.get("CONN_DB_NAME")))
												.distinct().count() == 1;
										boolean iscontainsFile = fromOperatorList.stream().anyMatch(fromOpMap -> "file"
												.equalsIgnoreCase(String.valueOf(fromOpMap.get("dragType"))));
										if (isSameDataBase && !iscontainsFile) {// all operators having same DB
											// need to call retrive method for getting same db data
											fromOperator = fromOperatorList.get(0);
//                                            System.out.println("fromOpeartor:::" + fromOperator);
//                                            System.out.println("toOpeartor:::" + toOperator);
											JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");
											String fromTableName = (String) fromOperator.get("tableName");
											JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
											String toTableName = (String) toOperator.get("tableName");
											String toIconType = (String) toOperator.get("iconType");
											Object fromConnectionObj = getConnection(fromConnObj);
											Object toConnectionObj = getConnection(toConnObj);
											if (fromConnectionObj instanceof Connection) {
												try {
													genericProcessETLDataService.processETLLog(
															(String) request.getSession(false)
																	.getAttribute("ssUsername"),
															(String) request.getSession(false).getAttribute("ssOrgId"),
															"Source system successfully connected.", "INFO", 20, "Y",
															jobId);
												} catch (Exception e) {
												}
												if ((toConnectionObj instanceof Connection)
														|| (toConnectionObj instanceof JCO.Client)
														|| (toIconType != null && !"".equalsIgnoreCase(toIconType)
																&& !"null".equalsIgnoreCase(toIconType)
																&& !"SQL".equalsIgnoreCase(toIconType))) {
													try {
														genericProcessETLDataService.processETLLog(
																(String) request.getSession(false)
																		.getAttribute("ssUsername"),
																(String) request.getSession(false)
																		.getAttribute("ssOrgId"),
																"Target system successfully connected.", "INFO", 20,
																"Y", jobId);
													} catch (Exception e) {
													}
													fromConnection = (Connection) fromConnectionObj;
													if (!(toIconType != null && !"".equalsIgnoreCase(toIconType)
															&& !"null".equalsIgnoreCase(toIconType)
															&& !"SQL".equalsIgnoreCase(toIconType))) {
														toConnection = (Connection) toConnectionObj;
													}
													Map mapOpeartorData = (Map) mappingOperatorObj.get(mappedKey);
													Map trfmRulesDataMap = (Map) mapOpeartorData.get("trfmRules-data");

													if (trfmRulesDataMap != null && !trfmRulesDataMap.isEmpty()
															&& !String.valueOf(mappedKey)
																	.equalsIgnoreCase(currentTrnsOpId)) {//
//                                                        System.out.println("trfmRulesDataMap::" + trfmRulesDataMap);
														Map paramsMap = convertTransFrmRulsMapToParam(trfmRulesDataMap);

														if (paramsMap != null && !paramsMap.isEmpty()) {
															JSONObject selectTabMapObj = (JSONObject) paramsMap
																	.get("selectTabObj");
															JSONObject normalizeOptionsParamObj = (JSONObject) paramsMap
																	.get("normalizeOptionsObj");
															if (!(selectTabMapObj != null
																	&& !selectTabMapObj.isEmpty())) {
																selectTabMapObj = new JSONObject();

															}
															selectTabMapObj.put("jobId", jobId);
															selectTabMapObj.put("toOperatorList", toOperatorList);
															resultObj = genericProcessETLDataService.processETLData(
																	request.getSession(false), fromConnection,
																	toConnection, fromOperatorList, toOperator,
																	((normalizeOptionsParamObj != null
																			&& !normalizeOptionsParamObj.isEmpty())
																					? (JSONObject) normalizeOptionsParamObj
																							.get("colsObj")
																					: (JSONObject) paramsMap
																							.get("columnsObj")),
																	(JSONObject) paramsMap.get("whereClauseObj"),
																	(JSONObject) paramsMap.get("defaultValObj"),
																	(JSONObject) paramsMap.get("joinQueryMapObj"),
																	(String) paramsMap.get("joinQuery"),
																	(JSONObject) paramsMap.get("orderBy"),
																	(String) paramsMap.get("groupBy"), nativeSQL,
																	(JSONObject) paramsMap.get("appendValObj"),
																	(JSONObject) paramsMap.get("columnClauseObj"),
																	selectTabMapObj, normalizeOptionsParamObj);
														}

													} else {
														JSONObject normalizeOptionsObj = (JSONObject) mapOpeartorData
																.get("normalizeOptionsObj");// RAVI NORMALIZE
														if (normalizeOptionsObj != null
																&& !normalizeOptionsObj.isEmpty()) {
															columnsObj = (JSONObject) normalizeOptionsObj
																	.get("colsObj");
														}
														if (!(selectTabObj != null && !selectTabObj.isEmpty())) {
															selectTabObj = new JSONObject();
														}
														selectTabObj.put("toOperatorList", toOperatorList);
														resultObj = genericProcessETLDataService.processETLData(
																request.getSession(false), fromConnection, toConnection,
																fromOperatorList, toOperator, columnsObj,
																tablesWhereClauseObj, defaultValuesObj, joinQueryMapObj,
																joinQuery, orderByObj, groupByStr, nativeSQL,
																appendValObj, columnClauseObj, selectTabObj,
																normalizeOptionsObj);
													}

//                                                    resultObj = processETLData(request,
//                                                            fromConnection,
//                                                            toConnection,
//                                                            fromOperatorList,
//                                                            toOperator,
//                                                            columnsObj,
//                                                            tablesWhereClauseObj,
//                                                            defaultValuesObj,
//                                                            joinQueryMapObj,
//                                                            joinQuery,
//                                                            orderByObj,
//                                                            groupByStr,
//                                                            nativeSQL
//                                                    );
												} else {
													try {
														genericProcessETLDataService.processETLLog(
																(String) request.getSession(false)
																		.getAttribute("ssUsername"),
																(String) request.getSession(false)
																		.getAttribute("ssOrgId"),
																"Unable to connect target system due to "
																		+ toConnectionObj,
																"ERROR", 20, "N", jobId);
													} catch (Exception e) {
													}
													resultObj.put("Message", toConnectionObj);
													resultObj.put("connectionFlag", "N");
												}

											} else if (fromConnectionObj instanceof JCO.Client) {
												// for SAP
												try {
													genericProcessETLDataService.processETLLog(
															(String) request.getSession(false)
																	.getAttribute("ssUsername"),
															(String) request.getSession(false).getAttribute("ssOrgId"),
															"Source system successfully connected.", "INFO", 20, "Y",
															jobId);
												} catch (Exception e) {
												}
												if ((toConnectionObj instanceof Connection)
														|| (toConnectionObj instanceof JCO.Client)
														|| (toIconType != null && !"".equalsIgnoreCase(toIconType)
																&& !"null".equalsIgnoreCase(toIconType)
																&& !"SQL".equalsIgnoreCase(toIconType))) {
													try {
														genericProcessETLDataService.processETLLog(
																(String) request.getSession(false)
																		.getAttribute("ssUsername"),
																(String) request.getSession(false)
																		.getAttribute("ssOrgId"),
																"Target system successfully connected.", "INFO", 20,
																"Y", jobId);
													} catch (Exception e) {
													}
													fromJCOConnection = (JCO.Client) fromConnectionObj;
													if (!(toIconType != null && !"".equalsIgnoreCase(toIconType)
															&& !"null".equalsIgnoreCase(toIconType)
															&& !"SQL".equalsIgnoreCase(toIconType))) {
														if (toConnectionObj instanceof Connection) {
															toConnection = (Connection) toConnectionObj;
														} else if (toConnectionObj instanceof JCO.Client) {
															toJCOConnection = (JCO.Client) toConnectionObj;
														}

													}
													Map mapOpeartorData = (Map) mappingOperatorObj.get(mappedKey);
													Map trfmRulesDataMap = (Map) mapOpeartorData.get("trfmRules-data");
													if (trfmRulesDataMap != null && !trfmRulesDataMap.isEmpty()
															&& !String.valueOf(mappedKey)
																	.equalsIgnoreCase(currentTrnsOpId)) {
//                                                        System.out.println("trfmRulesDataMap::" + trfmRulesDataMap);
														Map paramsMap = convertTransFrmRulsMapToParam(trfmRulesDataMap);

														if (paramsMap != null && !paramsMap.isEmpty()) {
															JSONObject selectTabMapObj = (JSONObject) paramsMap
																	.get("selectTabObj");
															if (!(selectTabMapObj != null
																	&& !selectTabMapObj.isEmpty())) {
																selectTabMapObj = new JSONObject();

															}
															JSONObject normalizeOptionsParamObj = (JSONObject) paramsMap
																	.get("normalizeOptionsObj");
															selectTabMapObj.put("jobId", jobId);
															selectTabMapObj.put("toOperatorList", toOperatorList);
															resultObj = genericProcessETLDataService.processETLData(
																	request.getSession(false), fromJCOConnection,
																	toConnection, toJCOConnection, fromOperatorList,
																	toOperator,
																	((normalizeOptionsParamObj != null
																			&& !normalizeOptionsParamObj.isEmpty())
																					? (JSONObject) normalizeOptionsParamObj
																							.get("colsObj")
																					: (JSONObject) paramsMap
																							.get("columnsObj")),
																	(JSONObject) paramsMap.get("whereClauseObj"),
																	(JSONObject) paramsMap.get("defaultValObj"),
																	(JSONObject) paramsMap.get("joinQueryMapObj"),
																	(String) paramsMap.get("joinQuery"),
																	(JSONObject) paramsMap.get("orderBy"),
																	(String) paramsMap.get("groupBy"), nativeSQL,
																	(JSONObject) paramsMap.get("appendValObj"),
																	(JSONObject) paramsMap.get("columnClauseObj"),
																	selectTabMapObj, normalizeOptionsParamObj);
														}

													} else {
														JSONObject normalizeOptionsObj = (JSONObject) mapOpeartorData
																.get("normalizeOptionsObj");// RAVI NORMALIZE
														if (normalizeOptionsObj != null
																&& !normalizeOptionsObj.isEmpty()) {
															columnsObj = (JSONObject) normalizeOptionsObj
																	.get("colsObj");
														}
														if (!(selectTabObj != null && !selectTabObj.isEmpty())) {
															selectTabObj = new JSONObject();
														}
														selectTabObj.put("toOperatorList", toOperatorList);
														resultObj = genericProcessETLDataService.processETLData(
																request.getSession(false), fromJCOConnection,
																toConnection, toJCOConnection, fromOperatorList,
																toOperator, columnsObj, tablesWhereClauseObj,
																defaultValuesObj, joinQueryMapObj, joinQuery,
																orderByObj, groupByStr, nativeSQL, appendValObj,
																columnClauseObj, selectTabObj, normalizeOptionsObj);
													}
												} else {
													try {
														genericProcessETLDataService.processETLLog(
																(String) request.getSession(false)
																		.getAttribute("ssUsername"),
																(String) request.getSession(false)
																		.getAttribute("ssOrgId"),
																"Unable to connect target system due to "
																		+ toConnectionObj,
																"ERROR", 20, "N", jobId);
													} catch (Exception e) {
													}
													resultObj.put("Message", toConnectionObj);
													resultObj.put("connectionFlag", "N");
												}
											} else {
												try {
													genericProcessETLDataService.processETLLog(
															(String) request.getSession(false)
																	.getAttribute("ssUsername"),
															(String) request.getSession(false).getAttribute("ssOrgId"),
															"Unable to connect source system due to "
																	+ fromConnectionObj,
															"ERROR", 20, "N", jobId);
												} catch (Exception e) {
												}
												resultObj.put("Message", fromConnectionObj);
												resultObj.put("connectionFlag", "N");
											}
										} else {

											boolean isDataBase = fromOperatorList.stream()
													.anyMatch(fromOpMap -> (fromOpMap.containsKey("CONNECTION_NAME")
															&& fromOpMap.containsKey("CONN_DB_NAME")));
											if (!isDataBase) {// files
												fromOperator = fromOperatorList.get(0);
//                                                System.out.println("fromOpeartor:::" + fromOperator);
//                                                System.out.println("toOpeartor:::" + toOperator);
												JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");
												String fromTableName = (String) fromOperator.get("tableName");
												JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
												String toTableName = (String) toOperator.get("tableName");
												String toIconType = (String) toOperator.get("iconType");
												Object toConnectionObj = getConnection(toConnObj);
												try {
													genericProcessETLDataService.processETLLog(
															(String) request.getSession(false)
																	.getAttribute("ssUsername"),
															(String) request.getSession(false).getAttribute("ssOrgId"),
															"Source File successfully connected.", "INFO", 20, "Y",
															jobId);
												} catch (Exception e) {
												}
												if ((toConnectionObj instanceof Connection)
														|| (toConnectionObj instanceof JCO.Client)
														|| (toIconType != null && !"".equalsIgnoreCase(toIconType)
																&& !"null".equalsIgnoreCase(toIconType)
																&& !"SQL".equalsIgnoreCase(toIconType))) {
													try {
														genericProcessETLDataService.processETLLog(
																(String) request.getSession(false)
																		.getAttribute("ssUsername"),
																(String) request.getSession(false)
																		.getAttribute("ssOrgId"),
																"Target system successfully connected.", "INFO", 20,
																"Y", jobId);
													} catch (Exception e) {
													}

													if (!(toIconType != null && !"".equalsIgnoreCase(toIconType)
															&& !"null".equalsIgnoreCase(toIconType)
															&& !"SQL".equalsIgnoreCase(toIconType))) {
														toConnection = (Connection) toConnectionObj;
													} else {
														toConnection = null; // ----------- ravi normalize edit
													}
													Map mapOpeartorData = (Map) mappingOperatorObj.get(mappedKey);
													Map trfmRulesDataMap = (Map) mapOpeartorData.get("trfmRules-data");
													if (trfmRulesDataMap != null && !trfmRulesDataMap.isEmpty()
															&& !String.valueOf(mappedKey)
																	.equalsIgnoreCase(currentTrnsOpId)) {//
//                                                        System.out.println("trfmRulesDataMap::" + trfmRulesDataMap);
														Map paramsMap = convertTransFrmRulsMapToParam(trfmRulesDataMap);
														if (paramsMap != null && !paramsMap.isEmpty()) {
															JSONObject selectTabMapObj = (JSONObject) paramsMap
																	.get("selectTabObj");
															if (!(selectTabMapObj != null
																	&& !selectTabMapObj.isEmpty())) {
																selectTabMapObj = new JSONObject();

															}
															JSONObject normalizeOptionsParamObj = (JSONObject) paramsMap
																	.get("normalizeOptionsObj");
															selectTabMapObj.put("jobId", jobId);
															selectTabMapObj.put("toOperatorList", toOperatorList);
															resultObj = genericProcessETLDataService.processETLData(
																	request.getSession(false), toConnection,
																	fromOperatorList, toOperator,
																	((normalizeOptionsParamObj != null
																			&& !normalizeOptionsParamObj.isEmpty())
																					? (JSONObject) normalizeOptionsParamObj
																							.get("colsObj")
																					: (JSONObject) paramsMap
																							.get("columnsObj")),
																	(JSONObject) paramsMap.get("whereClauseObj"),
																	(JSONObject) paramsMap.get("defaultValObj"),
																	(JSONObject) paramsMap.get("joinQueryMapObj"),
																	(String) paramsMap.get("joinQuery"),
																	(JSONObject) paramsMap.get("orderBy"),
																	(String) paramsMap.get("groupBy"), nativeSQL,
																	(JSONObject) paramsMap.get("appendValObj"),
																	(JSONObject) paramsMap.get("columnClauseObj"),
																	selectTabMapObj, normalizeOptionsParamObj);
														}
													} else {
														JSONObject normalizeOptionsObj = (JSONObject) mapOpeartorData
																.get("normalizeOptionsObj");// RAVI NORMALIZE
														if (normalizeOptionsObj != null
																&& !normalizeOptionsObj.isEmpty()) {
															columnsObj = (JSONObject) normalizeOptionsObj
																	.get("colsObj");
														}
														if (!(selectTabObj != null && !selectTabObj.isEmpty())) {
															selectTabObj = new JSONObject();
														}
														selectTabObj.put("toOperatorList", toOperatorList);
														resultObj = genericProcessETLDataService.processETLData(
																request.getSession(false), toConnection,
																fromOperatorList, toOperator, columnsObj,
																tablesWhereClauseObj, defaultValuesObj, joinQueryMapObj,
																joinQuery, orderByObj, groupByStr, nativeSQL,
																appendValObj, columnClauseObj, selectTabObj,
																normalizeOptionsObj);
													}

												} else {
													try {
														genericProcessETLDataService.processETLLog(
																(String) request.getSession(false)
																		.getAttribute("ssUsername"),
																(String) request.getSession(false)
																		.getAttribute("ssOrgId"),
																"Unable to connect target system due to "
																		+ toConnectionObj,
																"ERROR", 20, "N", jobId);
													} catch (Exception e) {
													}
													resultObj.put("Message", toConnectionObj);
													resultObj.put("connectionFlag", "N");
												}
											} else {
												// need to call retrive method for getting different db data
											}
										}
									} else {
										System.out.println("***** Form Opeartors are Empty ******");
									}

								}
							}
						}

					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			try {
				genericProcessETLDataService.processETLLog(
						(String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"),
						"Getting error while Process :" + e.getMessage(), "ERROR", 20, "N", jobId);
			} catch (Exception ex) {
			}
			resultObj.put("Message", e.getMessage());
			resultObj.put("connectionFlag", "N");
		} finally {
			if (!(resultObj != null)) {
				resultObj = new JSONObject();
			}
			resultObj.put("jobId", jobId);
		}

		return resultObj;
	}
//    public JSONObject processSameDBETLData

	public JSONObject processETLDataOld(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		Connection fromConnection = null;
		Connection toConnection = null;
		try {
			String mappedDataStr = request.getParameter("mappedData");
			String columnsObjStr = request.getParameter("columnsObj");
			String whereClauseObjStr = request.getParameter("whereClauseObj");
			String defaultValObjStr = request.getParameter("defaultValObj");
			String joinQuery = request.getParameter("joinQuery");
			String joinQueryMapObjStr = request.getParameter("joinQueryMapObj");
			String orderByStr = request.getParameter("orderBy");
			String groupByStr = request.getParameter("groupBy");
			String nativeSQL = request.getParameter("nativeSQL");
			// JSONObject columnsObj = new JSONObject();
			Map columnsObj = new LinkedHashMap(); // ravi etl integration
			JSONObject tablesWhereClauseObj = new JSONObject();
			JSONObject defaultValuesObj = new JSONObject();
			JSONObject joinQueryMapObj = new JSONObject();
			JSONObject orderByObj = new JSONObject();
			if (columnsObjStr != null && !"".equalsIgnoreCase(columnsObjStr)
					&& !"null".equalsIgnoreCase(columnsObjStr)) {
				columnsObj = (JSONObject) JSONValue.parse(columnsObjStr);
			}
			if (whereClauseObjStr != null && !"".equalsIgnoreCase(whereClauseObjStr)
					&& !"null".equalsIgnoreCase(whereClauseObjStr)) {
				tablesWhereClauseObj = (JSONObject) JSONValue.parse(whereClauseObjStr);
			}
			if (defaultValObjStr != null && !"".equalsIgnoreCase(defaultValObjStr)
					&& !"null".equalsIgnoreCase(defaultValObjStr)) {
				defaultValuesObj = (JSONObject) JSONValue.parse(defaultValObjStr);
			}
			if (joinQueryMapObjStr != null && !"".equalsIgnoreCase(joinQueryMapObjStr)
					&& !"null".equalsIgnoreCase(joinQueryMapObjStr)) {
				joinQueryMapObj = (JSONObject) JSONValue.parse(joinQueryMapObjStr);
			}
			if (orderByStr != null && !"".equalsIgnoreCase(orderByStr) && !"null".equalsIgnoreCase(orderByStr)) {
				orderByObj = (JSONObject) JSONValue.parse(orderByStr);
			}
			if (mappedDataStr != null && !"".equalsIgnoreCase(mappedDataStr)) {
				Map mappedData = (Map) JSONValue.parse(mappedDataStr);
				Map mappingOperatorObj = new HashMap();
				Map nonMapOperatorObj = new HashMap();
				if (mappedData != null && !mappedData.isEmpty()) {
					Map operatorsMap = (Map) mappedData.get("operators");
					Map linksMap = (Map) mappedData.get("links");
//                    System.out.println("operatorsMap::::" + operatorsMap);
//                    System.out.println("linksMap::::" + linksMap);
					if (operatorsMap != null && !operatorsMap.isEmpty()) {
						mappingOperatorObj = (Map) operatorsMap.keySet().stream()
								.filter(keyName -> (keyName != null && (Map) operatorsMap.get(keyName) != null
										&& ("MAP".equalsIgnoreCase(
												String.valueOf(((Map) operatorsMap.get(keyName)).get("mapType"))))))
								// ((Map) operatorsMap.get(keyName))
								// .containsKey("mapType")))
								.collect(Collectors.toMap(keyName -> keyName,
										keyName -> (Map) operatorsMap.get(keyName)));

//                        System.out.println("mappingOperatorObj:::" + mappingOperatorObj);
//                        System.out.println("nonMapOperatorObj:::" + nonMapOperatorObj);
						if (mappingOperatorObj != null && !mappingOperatorObj.isEmpty()) {
							List<Map> fromOperatorList = new ArrayList<>();
							for (Object mappedKey : mappingOperatorObj.keySet()) {
								if (linksMap != null && !linksMap.isEmpty()) {
									Map fromOperator = new HashMap();
									Map toOperator = new HashMap();
									for (Object linkKey : linksMap.keySet()) {
										Map linkMap = (Map) linksMap.get(linkKey);
										if (linkMap != null && !linkMap.isEmpty() && String.valueOf(mappedKey)
												.equalsIgnoreCase(String.valueOf(linkMap.get("toOperator")))) {
											System.out.println(
													"linkMap.get(\"fromOperator\")::" + linkMap.get("fromOperator"));
											String fromOperatorId = String.valueOf(linkMap.get("fromOperator"));
											fromOperatorList.add((Map) operatorsMap
													.get(String.valueOf(linkMap.get("fromOperator"))));

										}
										if (linkMap != null && !linkMap.isEmpty() && String.valueOf(mappedKey)
												.equalsIgnoreCase(String.valueOf(linkMap.get("fromOperator")))) {
											toOperator = (Map) operatorsMap
													.get(String.valueOf(linkMap.get("toOperator")));

										}
									}
//                                    System.out.println("fromOpeartor:::" + fromOperator);
//                                    System.out.println("toOpeartor:::" + toOperator);
									JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");
									String fromTableName = (String) fromOperator.get("tableName");
									JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
									String toTableName = (String) toOperator.get("tableName");
									Object fromConnectionObj = getConnection(fromConnObj);
									Object toConnectionObj = getConnection(toConnObj);
									if (fromConnectionObj instanceof Connection) {
										if (toConnectionObj instanceof Connection) {
											fromConnection = (Connection) fromConnectionObj;
											toConnection = (Connection) toConnectionObj;
											resultObj = processETLData(request, fromConnection, toConnection,
													fromOperator, toOperator, columnsObj, tablesWhereClauseObj,
													defaultValuesObj);
										} else {
											resultObj.put("Message", toConnectionObj);
											resultObj.put("connectionFlag", "N");
										}

									} else {
										resultObj.put("Message", fromConnectionObj);
										resultObj.put("connectionFlag", "N");
									}
								}
							}
						}

					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			resultObj.put("Message", e.getMessage());
			resultObj.put("connectionFlag", "N");
		} finally {
			try {
				if (fromConnection != null) {
					fromConnection.close();
				}
				if (toConnection != null) {
					toConnection.close();
				}
			} catch (Exception e) {
			}
		}
		return resultObj;
	}

	public Object getConnection(JSONObject dbObj) {

		Connection connection = null;
		Object returnedObj = null;
		try {

			if (dbObj != null && !dbObj.isEmpty()) {
				String initParamClassName = "com.pilog.mdm.transformaccess.V10MigrationDataAccess";
				String initParamMethodName = "get" + dbObj.get("CONN_CUST_COL1") + "Connection";
				System.out.println(
						" initParamClassName:" + initParamClassName + "initParamMethodName:" + initParamMethodName);
				Class clazz = Class.forName(initParamClassName);
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1")))
						|| "SAP_HANA".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1")))) {
//                    dbObj.put("selectedItemLabel", connectionObj.get("CONN_CUST_COL1"));
//                    dbObj.put("ClientId", connectionObj.get("CONN_PORT"));
//                    dbObj.put("hostName", connectionObj.get("HOST_NAME"));
//                    dbObj.put("userName", connectionObj.get("CONN_USER_NAME"));
//                    dbObj.put("password", connectionObj.get("CONN_PASSWORD"));
//                    dbObj.put("LanguageId", "EN");
//                    dbObj.put("ERPSystemId", connectionObj.get("CONN_DB_NAME"));
//String ClientId,String userName,String password, String LanguageId,String hostName,String ERPSystemId
					Class<?>[] paramTypes = { String.class, String.class, String.class, String.class, String.class,
							String.class, String.class };
					Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
					Object targetObj = new PilogUtilities().createObjectByClass(clazz);
					returnedObj = method.invoke(targetObj, String.valueOf(dbObj.get("CONN_PORT")),
							String.valueOf(dbObj.get("CONN_USER_NAME")), String.valueOf(dbObj.get("CONN_PASSWORD")),
							"EN", String.valueOf(dbObj.get("HOST_NAME")), String.valueOf(dbObj.get("CONN_DB_NAME")),
							String.valueOf(dbObj.get("GROUP")));

				} else {
					// dbFromObj.put("selectedItemLabel", fromConnectObj.get("CONN_CUST_COL1"));
//            dbFromObj.put("hostName", fromConnectObj.get("HOST_NAME"));
//            dbFromObj.put("port", fromConnectObj.get("CONN_PORT"));
//            dbFromObj.put("userName", fromConnectObj.get("CONN_USER_NAME"));
//            dbFromObj.put("password", fromConnectObj.get("CONN_PASSWORD"));
//            dbFromObj.put("serviceName", fromConnectObj.get("CONN_DB_NAME"));
					Class<?>[] paramTypes = { String.class, String.class, String.class, String.class, String.class };
					Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
					Object targetObj = new PilogUtilities().createObjectByClass(clazz);
					returnedObj = method.invoke(targetObj, String.valueOf(dbObj.get("HOST_NAME")),
							String.valueOf(dbObj.get("CONN_PORT")), String.valueOf(dbObj.get("CONN_USER_NAME")),
							String.valueOf(dbObj.get("CONN_PASSWORD")), String.valueOf(dbObj.get("CONN_DB_NAME")));

				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return returnedObj;
	}

	public Object getGlobalConnection(JSONObject dbObj) {

		Object returnedObj = null;
		try {

			if (dbObj != null && !dbObj.isEmpty()) {

				String hostName = String.valueOf(dbObj.get("HOST_NAME"));
				String dbName = String.valueOf(dbObj.get("CONN_DB_NAME"));
				String port = String.valueOf(dbObj.get("CONN_PORT"));
				String key = hostName + "::" + port + "::" + dbName;

				if (globalConnectionMap.get(key) != null) {
					returnedObj = globalConnectionMap.get(key);
					if (returnedObj instanceof Connection) {
						if (((Connection) returnedObj).isClosed()) {
							String initParamClassName = "com.pilog.mdm.transformaccess.V10MigrationDataAccess";
							String initParamMethodName = "get" + dbObj.get("CONN_CUST_COL1") + "Connection";
							System.out.println(" initParamClassName:" + initParamClassName + "initParamMethodName:"
									+ initParamMethodName);
							Class clazz = Class.forName(initParamClassName);

							Class<?>[] paramTypes = { String.class, String.class, String.class, String.class,
									String.class };
							Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
							Object targetObj = new PilogUtilities().createObjectByClass(clazz);
							returnedObj = method.invoke(targetObj, String.valueOf(dbObj.get("HOST_NAME")),
									String.valueOf(dbObj.get("CONN_PORT")), String.valueOf(dbObj.get("CONN_USER_NAME")),
									String.valueOf(dbObj.get("CONN_PASSWORD")),
									String.valueOf(dbObj.get("CONN_DB_NAME")));
							((Connection) returnedObj).setAutoCommit(false);
						}
					}
				} else {

					String initParamClassName = "com.pilog.mdm.transformaccess.V10MigrationDataAccess";
					String initParamMethodName = "get" + dbObj.get("CONN_CUST_COL1") + "Connection";
					System.out.println(
							" initParamClassName:" + initParamClassName + "initParamMethodName:" + initParamMethodName);
					Class clazz = Class.forName(initParamClassName);
					if ("SAP_ECC".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1")))
							|| "SAP_HANA".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1")))) {

						Class<?>[] paramTypes = { String.class, String.class, String.class, String.class, String.class,
								String.class, String.class };
						Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
						Object targetObj = new PilogUtilities().createObjectByClass(clazz);
						returnedObj = method.invoke(targetObj, String.valueOf(dbObj.get("CONN_PORT")),
								String.valueOf(dbObj.get("CONN_USER_NAME")), String.valueOf(dbObj.get("CONN_PASSWORD")),
								"EN", String.valueOf(dbObj.get("HOST_NAME")), String.valueOf(dbObj.get("CONN_DB_NAME")),
								String.valueOf(dbObj.get("GROUP")));

					} else if ("MongoDb".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1")))) {
						Class<?>[] paramTypes = { String.class, String.class, String.class, String.class,
								String.class };
						Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
						Object targetObj = new PilogUtilities().createObjectByClass(clazz);
						returnedObj = method.invoke(targetObj, String.valueOf(dbObj.get("HOST_NAME")),
								String.valueOf(dbObj.get("CONN_PORT")), String.valueOf(dbObj.get("CONN_USER_NAME")),
								String.valueOf(dbObj.get("CONN_PASSWORD")), String.valueOf(dbObj.get("CONN_DB_NAME")));
					} else {

						Class<?>[] paramTypes = { String.class, String.class, String.class, String.class,
								String.class };
						Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
						Object targetObj = new PilogUtilities().createObjectByClass(clazz);
						returnedObj = method.invoke(targetObj, String.valueOf(dbObj.get("HOST_NAME")),
								String.valueOf(dbObj.get("CONN_PORT")), String.valueOf(dbObj.get("CONN_USER_NAME")),
								String.valueOf(dbObj.get("CONN_PASSWORD")), String.valueOf(dbObj.get("CONN_DB_NAME")));
						((Connection) returnedObj).setAutoCommit(false);
					}
					if (returnedObj != null && returnedObj instanceof Connection) {
						globalConnectionMap.put(key, returnedObj);
					}

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return returnedObj;
	}

	public JSONObject processETLData(HttpServletRequest request, Connection fromConnection, Connection toConnection,
			List<Map> fromOperatorList, Map toOperator,
			// JSONObject columnsObj,
			Map columnsObj, // ravi etl integration
			JSONObject tablesWhereClauseObj, JSONObject defaultValuesObj, JSONObject joinQueryMapObj, String joinQuery,
			JSONObject orderByObj, String groupByQuery, String nativeSQL) {
		JSONObject resultObj = new JSONObject();
		PreparedStatement toPreparedStatement = null;
		try {
			// JSONObject fromColumnsObj = new JSONObject();
			Map fromColumnsObj = new LinkedHashMap(); // ravi etl integration
			if (columnsObj != null && !columnsObj.isEmpty()) {
				for (Object toColumnName : columnsObj.keySet()) {
					if (toColumnName != null && columnsObj.get(toColumnName) != null) {
						String fromColumnStr = (String) columnsObj.get(toColumnName);
						if (fromColumnStr != null && !"".equalsIgnoreCase(fromColumnStr)
								&& !"null".equalsIgnoreCase(fromColumnStr)) {
							String[] fromColumnArray = fromColumnStr.split(":");
							if (fromColumnArray != null && fromColumnArray.length != 0) {
								if (fromColumnsObj != null && !fromColumnsObj.isEmpty()) {
									if (fromColumnsObj.containsKey(fromColumnArray[0])) {
										fromColumnsObj.put(fromColumnArray[0],
												(fromColumnsObj.get(fromColumnArray[0]) + "," + fromColumnArray[1]));
									} else {
										fromColumnsObj.put(fromColumnArray[0], (fromColumnArray[1]));
									}
								} else {
									fromColumnsObj.put(fromColumnArray[0], (fromColumnArray[1]));
								}
							}
						}

					}

				}
				Set<String> toColumns = new HashSet<>();
				toColumns.addAll(columnsObj.keySet());
				if (defaultValuesObj != null && !defaultValuesObj.isEmpty()) {
					Set<String> defaultColumns = defaultValuesObj.keySet();
					if (defaultColumns != null && !defaultColumns.isEmpty() && toColumns != null
							&& !toColumns.isEmpty()) {
						toColumns.addAll(defaultColumns);
					}
				}
				List<String> toColumnsList = new ArrayList<>();
				toColumnsList.addAll(toColumns);
				if (toConnection != null) {
					String toTableInsertQuery = generateInsertQuery((String) toOperator.get("tableName"),
							toColumnsList);
					System.out.println("insertQuery::::" + toTableInsertQuery);
					toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
				}

				int totalDataCount = 0;
				int fileDataLastIndex = 0;
				List<Map> nonJoinOpList = fromOperatorList;
				String fileName = "";
				String iconType = (String) toOperator.get("iconType");
				if ("XLSX".equalsIgnoreCase(iconType)) {
					fileName = "V10ETLExport_" + System.currentTimeMillis() + ".xlsx";
				} else if ("XLS".equalsIgnoreCase(iconType)) {
					fileName = "V10ETLExport_" + System.currentTimeMillis() + ".xls";
				} else if ("XML".equalsIgnoreCase(iconType)) {
					fileName = "V10ETLExport_" + System.currentTimeMillis() + ".xml";
				} else if ("CSV".equalsIgnoreCase(iconType)) {
					fileName = "V10ETLExport_" + System.currentTimeMillis() + ".csv";
				} else if ("TXT".equalsIgnoreCase(iconType)) {
					fileName = "V10ETLExport_" + System.currentTimeMillis() + ".txt";
				}
				if (joinQueryMapObj != null && !joinQueryMapObj.isEmpty()) {
					nonJoinOpList = fromOperatorList.stream()
							.filter(opMap -> (opMap != null && !opMap.isEmpty() && opMap.get("tableName") != null
									&& !"".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
									&& !"null".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
									&& !joinQueryMapObj.containsKey(String.valueOf(opMap.get("tableName")))))
							.collect(Collectors.toList());
					totalDataCount += processETLData(request, fromConnection, toConnection, fromOperatorList,
							toOperator, columnsObj, fromColumnsObj, tablesWhereClauseObj, defaultValuesObj,
							toPreparedStatement, 1, 1000, totalDataCount, toColumnsList, joinQueryMapObj, joinQuery,
							fileDataLastIndex, fileName);

				} else {
					if (nonJoinOpList != null && !nonJoinOpList.isEmpty()) {
						for (int i = 0; i < nonJoinOpList.size(); i++) {
							Map fromOperator = nonJoinOpList.get(i);
							if (fromOperator != null && !fromOperator.isEmpty()) {
								totalDataCount += processETLData(request, fromConnection, toConnection, fromOperator,
										toOperator, columnsObj, fromColumnsObj, tablesWhereClauseObj, defaultValuesObj,
										toPreparedStatement, 1, 1000, totalDataCount, toColumnsList, fileDataLastIndex,
										fileName);
							}

						}
					}

				}

				System.out.println("totalDataCount::::" + totalDataCount);
				if (totalDataCount != 0 && totalDataCount > 0) {
					String message = totalDataCount + " Row(s) extracted successfully";
					if (fileName != null && !"".equalsIgnoreCase(fileName) && !"null".equalsIgnoreCase(fileName)) {
						message += " <br> <a href='#' style='color:#0071c5;' onclick=downloadExportedFile('" + fileName
								+ "') >Click here to download the " + iconType + " file</a>.";//
					}
					resultObj.put("Message", message);
					resultObj.put("connectionFlag", "Y");

				} else {
					resultObj.put("Message", totalDataCount + " Row(s) extracted successfully");
					resultObj.put("connectionFlag", "Y");
				}
			} // end of columnObj if

		} catch (Exception e) {
			resultObj.put("Message", e.getMessage());
			resultObj.put("connectionFlag", "N");
			e.printStackTrace();
		} finally {
			try {
				if (toPreparedStatement != null) {
					toPreparedStatement.close();
				}
			} catch (Exception e) {
			}
		}
		return resultObj;
	}

	public JSONObject processETLData(HttpServletRequest request, Connection fromConnection, Connection toConnection,
			Map fromOperator, Map toOperator,
			// JSONObject columnsObj,
			Map columnsObj, // ravi etl integration
			JSONObject tablesWhereClauseObj, JSONObject defaultValuesObj) {
		JSONObject resultObj = new JSONObject();
		PreparedStatement toPreparedStatement = null;
		try {
			// JSONObject fromColumnsObj = new JSONObject();
			Map fromColumnsObj = new LinkedHashMap(); // ravi updated code changes
			if (columnsObj != null && !columnsObj.isEmpty()) {
				for (Object toColumnName : columnsObj.keySet()) {
					if (toColumnName != null && columnsObj.get(toColumnName) != null) {
						String fromColumnStr = (String) columnsObj.get(toColumnName);
						if (fromColumnStr != null && !"".equalsIgnoreCase(fromColumnStr)
								&& !"null".equalsIgnoreCase(fromColumnStr)) {
							String[] fromColumnArray = fromColumnStr.split(":");
							if (fromColumnArray != null && fromColumnArray.length != 0) {
								if (fromColumnsObj != null && !fromColumnsObj.isEmpty()) {
									if (fromColumnsObj.containsKey(fromColumnArray[0])) {
										fromColumnsObj.put(fromColumnArray[0],
												(fromColumnsObj.get(fromColumnArray[0]) + "," + fromColumnArray[1]));
									} else {
										fromColumnsObj.put(fromColumnArray[0], (fromColumnArray[1]));
									}
								} else {
									fromColumnsObj.put(fromColumnArray[0], (fromColumnArray[1]));
								}
							}
						}

					}

				}
				Set<String> toColumns = columnsObj.keySet();
				if (defaultValuesObj != null && !defaultValuesObj.isEmpty()) {
					Set<String> defaultColumns = defaultValuesObj.keySet();
					if (defaultColumns != null && !defaultColumns.isEmpty() && toColumns != null
							&& !toColumns.isEmpty()) {
						toColumns.addAll(defaultColumns);
					}
				}
				List<String> toColumnsList = new ArrayList<>();
				toColumnsList.addAll(toColumns);

				String toTableInsertQuery = generateInsertQuery((String) toOperator.get("tableName"), toColumnsList);
				System.out.println("insertQuery::::" + toTableInsertQuery);
				toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
				int totalDataCount = 0;
				totalDataCount = processETLData(request, fromConnection, toConnection, fromOperator, toOperator,
						columnsObj, fromColumnsObj, tablesWhereClauseObj, defaultValuesObj, toPreparedStatement, 0, 500,
						totalDataCount, toColumnsList, 0, "");
				System.out.println("totalDataCount::::" + totalDataCount);
				if (totalDataCount != 0 && totalDataCount > 0) {
					resultObj.put("Message", totalDataCount + " Row(s) extracted successfully");
					resultObj.put("connectionFlag", "Y");

				} else {
					resultObj.put("Message",
							"No data available in Source Tables/Problem in inserting the data in target table,please check log for more information.");
					resultObj.put("connectionFlag", "N");
				}
			} // end of columnObj if

		} catch (Exception e) {
			resultObj.put("Message", e.getMessage());
			resultObj.put("connectionFlag", "N");
		} finally {
			try {
				if (toPreparedStatement != null) {
					toPreparedStatement.close();
				}
			} catch (Exception e) {
			}
		}
		return resultObj;
	}

	public int processETLData(HttpServletRequest request, Connection fromConnection, Connection toConnection,
			List<Map> fromOperatorList, Map toOperator,
			// JSONObject columnsObj,
			Map columnsObj, // ravi etl integration
			// JSONObject fromColumnsObj,
			Map fromColumnsObj, // ravi etl integration
			JSONObject tablesWhereClauseObj, JSONObject defaultValuesObj, PreparedStatement toPreparedStatement,
			int start, int limit, int totalDataCount, List<String> toColumnsList, JSONObject joinQueryMapObj,
			String joinQuery, int fileDataLastIndex, String fileName) {
		try {
			Map fromOperator = fromOperatorList.get(0);
			List totalDataList = getSelectedJoinColumnsData((JSONObject) fromOperator.get("connObj"), fromConnection,
					fromColumnsObj, start, limit, tablesWhereClauseObj, joinQueryMapObj, joinQuery);
			if (totalDataList != null && !totalDataList.isEmpty()) {
				String iconType = (String) toOperator.get("iconType");
				int insertCount = 0;
				JSONObject fileDataObj = new JSONObject();
				if ("XLSX".equalsIgnoreCase(iconType)) {
					FileInputStream inputStream = null;
					XSSFWorkbook wb = null;
					XSSFSheet sheet = null;
					String filePath = etlFilePath + "ETL_EXPORT_" + File.separator
							+ request.getSession(false).getAttribute("ssUsername");
					File file12 = new File(filePath);

					if (!file12.exists()) {
						file12.mkdirs();
					}

					File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
					if (outputFile.exists()) {
						inputStream = new FileInputStream(outputFile);
						wb = (XSSFWorkbook) WorkbookFactory.create(inputStream);
						sheet = wb.getSheetAt(0);
						fileDataLastIndex = sheet.getLastRowNum();
						if ((fileDataLastIndex + totalDataList.size()) >= 1000000) {
							sheet = wb.createSheet();
						}
					} else {
						wb = new XSSFWorkbook();
						sheet = wb.createSheet();
					}

					insertCount = exportingXLSXFileData(toOperator, fromColumnsObj, columnsObj, totalDataList,
							toPreparedStatement, toColumnsList, defaultValuesObj, filePath, fileName, fileDataLastIndex,
							sheet, wb);

					if (inputStream != null) {
						inputStream.close();
					}
					if (wb != null) {
//                        wb.close();
					}
				} else if ("XLS".equalsIgnoreCase(iconType)) {
					FileInputStream inputStream = null;
					HSSFWorkbook wb = null;
					HSSFSheet sheet = null;
					String filePath = etlFilePath + "ETL_EXPORT_" + File.separator
							+ request.getSession(false).getAttribute("ssUsername");
					File file12 = new File(filePath);

					if (!file12.exists()) {
						file12.mkdirs();
					}

					File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
					if (outputFile.exists()) {
						inputStream = new FileInputStream(outputFile);
						wb = (HSSFWorkbook) WorkbookFactory.create(inputStream);
						sheet = wb.getSheetAt(0);
						fileDataLastIndex = sheet.getLastRowNum();
						if ((fileDataLastIndex + totalDataList.size()) >= 60000) {
							sheet = wb.createSheet();
						}
					} else {
						wb = new HSSFWorkbook();
						sheet = wb.createSheet();
					}

					insertCount = exportingXLSFileData(toOperator, fromColumnsObj, columnsObj, totalDataList,
							toPreparedStatement, toColumnsList, defaultValuesObj, filePath, fileName, fileDataLastIndex,
							sheet, wb);

					if (inputStream != null) {
						inputStream.close();
					}
					if (wb != null) {
						wb.close();
					}
				} else if ("XML".equalsIgnoreCase(iconType)) {
					String filePath = etlFilePath + "ETL_EXPORT_" + File.separator
							+ request.getSession(false).getAttribute("ssUsername");
					File file12 = new File(filePath);
					if (!file12.exists()) {
						file12.mkdirs();
					}
					File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
					insertCount = exportingXMLFileData(toOperator, fromColumnsObj, columnsObj, totalDataList,
							toPreparedStatement, toColumnsList, defaultValuesObj, filePath, fileName,
							fileDataLastIndex);
					fileDataLastIndex += insertCount;
				} else if ("CSV".equalsIgnoreCase(iconType)) {
					String filePath = etlFilePath + "ETL_EXPORT_" + File.separator
							+ request.getSession(false).getAttribute("ssUsername");
					File file12 = new File(filePath);
					if (!file12.exists()) {
						file12.mkdirs();
					}
					File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
					insertCount = exportingCsvAndTxtFileData(toOperator, fromColumnsObj, columnsObj, totalDataList,
							toPreparedStatement, toColumnsList, defaultValuesObj, filePath, fileName,
							fileDataLastIndex);
					fileDataLastIndex += insertCount;
//                    exportingCsvAndTxtFileData
				} else if ("TXT".equalsIgnoreCase(iconType)) {
					String filePath = etlFilePath + "ETL_EXPORT_" + File.separator
							+ request.getSession(false).getAttribute("ssUsername");
					File file12 = new File(filePath);
					if (!file12.exists()) {
						file12.mkdirs();
					}

					File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
					insertCount = exportingCsvAndTxtFileData(toOperator, fromColumnsObj, columnsObj, totalDataList,
							toPreparedStatement, toColumnsList, defaultValuesObj, filePath, fileName,
							fileDataLastIndex);
					fileDataLastIndex += insertCount;
				} else {
					insertCount = dataMigrationService.importingData((String) toOperator.get("tableName"),
							fromColumnsObj, columnsObj, totalDataList, toPreparedStatement, toColumnsList,
							defaultValuesObj);
				}

				if (insertCount != 0) {
					totalDataCount += insertCount;
					totalDataCount = processETLData(request, fromConnection, toConnection, fromOperatorList, toOperator,
							columnsObj, fromColumnsObj, tablesWhereClauseObj, defaultValuesObj, toPreparedStatement,
							(start + limit), limit, totalDataCount, toColumnsList, joinQueryMapObj, joinQuery,
							fileDataLastIndex, fileName);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return totalDataCount;
	}

	public int processETLData(HttpServletRequest request, Connection fromConnection, Connection toConnection,
			Map fromOperator, Map toOperator,
			// JSONObject columnsObj,
			Map columnsObj, // ravi etl integration
			// JSONObject fromColumnsObj,
			Map fromColumnsObj, // ravi etl integration
			JSONObject tablesWhereClauseObj, JSONObject defaultValuesObj, PreparedStatement toPreparedStatement,
			int start, int limit, int totalDataCount, List<String> toColumnsList, int fileDataLastIndex,
			String fileName) {
		Object resultObj = null;
		try {
			List totalDataList = getSelectedColumnsData((JSONObject) fromOperator.get("connObj"), fromConnection,
					fromColumnsObj, start, limit, tablesWhereClauseObj);
			if (totalDataList != null && !totalDataList.isEmpty()) {
				int insertCount = 0;
				String iconType = (String) toOperator.get("iconType");
				if ("XLSX".equalsIgnoreCase(iconType)) {
					FileInputStream inputStream = null;
					XSSFWorkbook wb = null;
					XSSFSheet sheet = null;
					String filePath = etlFilePath + "ETL_EXPORT_" + File.separator
							+ request.getSession(false).getAttribute("ssUsername");
					File file12 = new File(filePath);

					if (!file12.exists()) {
						file12.mkdirs();
					}

					File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
					if (outputFile.exists()) {
						inputStream = new FileInputStream(outputFile);
						wb = (XSSFWorkbook) WorkbookFactory.create(inputStream);
						sheet = wb.getSheetAt(0);
						fileDataLastIndex = sheet.getLastRowNum();
						if ((fileDataLastIndex + totalDataList.size()) >= 1000000) {
							sheet = wb.createSheet();
						}
					} else {
						wb = new XSSFWorkbook();
						sheet = wb.createSheet();
					}

					insertCount = exportingXLSXFileData(toOperator, fromColumnsObj, columnsObj, totalDataList,
							toPreparedStatement, toColumnsList, defaultValuesObj, filePath, fileName, fileDataLastIndex,
							sheet, wb);

					if (inputStream != null) {
						inputStream.close();
					}
					if (wb != null) {
//                        wb.close();
					}
				} else if ("XLS".equalsIgnoreCase(iconType)) {
					FileInputStream inputStream = null;
					HSSFWorkbook wb = null;
					HSSFSheet sheet = null;
					String filePath = etlFilePath + "ETL_EXPORT_" + File.separator
							+ request.getSession(false).getAttribute("ssUsername");
					File file12 = new File(filePath);

					if (!file12.exists()) {
						file12.mkdirs();
					}

					File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
					if (outputFile.exists()) {
						inputStream = new FileInputStream(outputFile);
						wb = (HSSFWorkbook) WorkbookFactory.create(inputStream);
						sheet = wb.getSheetAt(0);
						fileDataLastIndex = sheet.getLastRowNum();
						if ((fileDataLastIndex + totalDataList.size()) >= 60000) {
							sheet = wb.createSheet();
						}
					} else {
						wb = new HSSFWorkbook();
						sheet = wb.createSheet();
					}

					insertCount = exportingXLSFileData(toOperator, fromColumnsObj, columnsObj, totalDataList,
							toPreparedStatement, toColumnsList, defaultValuesObj, filePath, fileName, fileDataLastIndex,
							sheet, wb);

					if (inputStream != null) {
						inputStream.close();
					}
					if (wb != null) {
						wb.close();
					}
				} else if ("XML".equalsIgnoreCase(iconType)) {
					String filePath = etlFilePath + "ETL_EXPORT_" + File.separator
							+ request.getSession(false).getAttribute("ssUsername");
					File file12 = new File(filePath);
					if (!file12.exists()) {
						file12.mkdirs();
					}
					File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
					insertCount = exportingXMLFileData(toOperator, fromColumnsObj, columnsObj, totalDataList,
							toPreparedStatement, toColumnsList, defaultValuesObj, filePath, fileName,
							fileDataLastIndex);
					fileDataLastIndex += insertCount;
				} else if ("CSV".equalsIgnoreCase(iconType)) {
					String filePath = etlFilePath + "ETL_EXPORT_" + File.separator
							+ request.getSession(false).getAttribute("ssUsername");
					File file12 = new File(filePath);
					if (!file12.exists()) {
						file12.mkdirs();
					}
					File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
					insertCount = exportingCsvAndTxtFileData(toOperator, fromColumnsObj, columnsObj, totalDataList,
							toPreparedStatement, toColumnsList, defaultValuesObj, filePath, fileName,
							fileDataLastIndex);
					fileDataLastIndex += insertCount;
//                    exportingCsvAndTxtFileData
				} else if ("TXT".equalsIgnoreCase(iconType)) {
					String filePath = etlFilePath + "ETL_EXPORT_" + File.separator
							+ request.getSession(false).getAttribute("ssUsername");
					File file12 = new File(filePath);
					if (!file12.exists()) {
						file12.mkdirs();
					}

					File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
					insertCount = exportingCsvAndTxtFileData(toOperator, fromColumnsObj, columnsObj, totalDataList,
							toPreparedStatement, toColumnsList, defaultValuesObj, filePath, fileName,
							fileDataLastIndex);
					fileDataLastIndex += insertCount;
				} else {
					insertCount = dataMigrationService.importingData((String) toOperator.get("tableName"),
							fromColumnsObj, columnsObj, totalDataList, toPreparedStatement, toColumnsList,
							defaultValuesObj);
				}
				if (insertCount != 0) {
					totalDataCount += insertCount;
					totalDataCount = processETLData(request, fromConnection, toConnection, fromOperator, toOperator,
							columnsObj, fromColumnsObj, tablesWhereClauseObj, defaultValuesObj, toPreparedStatement,
							(start + limit), limit, totalDataCount, toColumnsList, fileDataLastIndex, fileName);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return totalDataCount;
	}

	public List getSelectedColumnsData(JSONObject dbObj, Connection fromConnection,
			// JSONObject columnsObj,
			Map columnsObj, // ravi etl integration
			int start, int limit, JSONObject tablesConditionObj) {
		List totalData = new ArrayList();
		try {
//            dbFromObj.put("selectedItemLabel", fromConnectObj.get("CONN_CUST_COL1"));
//            dbFromObj.put("hostName", fromConnectObj.get("HOST_NAME"));
//            dbFromObj.put("port", fromConnectObj.get("CONN_PORT"));
//            dbFromObj.put("userName", fromConnectObj.get("CONN_USER_NAME"));
//            dbFromObj.put("password", fromConnectObj.get("CONN_PASSWORD"));
//            dbFromObj.put("serviceName", fromConnectObj.get("CONN_DB_NAME"));
			String initParamClassName = "com.pilog.mdm.DAO.V10DataMigrationAccessDAO";
			String initParamMethodName = "get" + dbObj.get("CONN_CUST_COL1") + "SelectedColumnsData";
			System.out.println(
					" initParamClassName:" + initParamClassName + "initParamMethodName:" + initParamMethodName);
			Class clazz = Class.forName(initParamClassName);
			Class<?>[] paramTypes = { Connection.class, JSONObject.class, Integer.class, Integer.class,
					JSONObject.class, String.class };
			Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
			Object targetObj = new PilogUtilities().createObjectByClass(clazz);
			totalData = (List) method.invoke(targetObj, fromConnection, columnsObj, start, limit, tablesConditionObj,
					String.valueOf(dbObj.get("CONN_DB_NAME")));

		} catch (Exception e) {

			e.printStackTrace();
		}
		return totalData;
	}

	public List getSelectedJoinColumnsData(JSONObject dbObj, Connection fromConnection,
			// JSONObject columnsObj,
			Map columnsObj, // ravi etl integration
			int start, int limit, JSONObject tablesConditionObj, JSONObject joinColsObj, String joinQuery) {
		List totalData = new ArrayList();
		try {
//            dbFromObj.put("selectedItemLabel", fromConnectObj.get("CONN_CUST_COL1"));
//            dbFromObj.put("hostName", fromConnectObj.get("HOST_NAME"));
//            dbFromObj.put("port", fromConnectObj.get("CONN_PORT"));
//            dbFromObj.put("userName", fromConnectObj.get("CONN_USER_NAME"));
//            dbFromObj.put("password", fromConnectObj.get("CONN_PASSWORD"));
//            dbFromObj.put("serviceName", fromConnectObj.get("CONN_DB_NAME"));
			String initParamClassName = "com.pilog.mdm.DAO.V10DataMigrationAccessDAO";
			String initParamMethodName = "get" + dbObj.get("CONN_CUST_COL1") + "SelectedJoinColumnsData";
			System.out.println(
					" initParamClassName:" + initParamClassName + "initParamMethodName:" + initParamMethodName);
			Class clazz = Class.forName(initParamClassName);
			Class<?>[] paramTypes = { Connection.class, JSONObject.class, Integer.class, Integer.class,
					JSONObject.class, String.class, JSONObject.class, String.class };
			Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
			Object targetObj = new PilogUtilities().createObjectByClass(clazz);
			totalData = (List) method.invoke(targetObj, fromConnection, columnsObj, start, limit, tablesConditionObj,
					String.valueOf(dbObj.get("CONN_DB_NAME")), joinColsObj, joinQuery);

		} catch (Exception e) {

			e.printStackTrace();
		}
		return totalData;
	}

	public JSONObject fetchTransformationRules(HttpServletRequest request, HttpServletResponse response) {

		JSONObject resultObj = new JSONObject();
		Connection fromConnection = null;
		Connection toConnection = null;
		JCO.Client fromJCOConnection = null;
		JCO.Client toJCOConnection = null;
		try {
			JSONObject labelObj = new PilogUtilities().getMultilingualObject(request);
			JSONArray fromTableColsArray = new JSONArray();
			JSONArray toTableColsArray = new JSONArray();

			JSONArray sourceTablesArray = new JSONArray();
			JSONArray destinationTablesArray = new JSONArray();

			String tabsString = "<div id='dataMigrationTabs' class='dataMigrationTabs'><ul class='dataMigrationTabsHeader'>"
					+ "<li class='dataMigrationTabsli'><a href='#tabs-1'><img src='images/mapping.svg' style='width:18px;height:18px;'/><span style='vertical-align: 3px;padding: 5px;'>"
					+ new PilogUtilities().convertIntoMultilingualValue(labelObj, "Mapping") + "</span></a></li>"
					+ "<li class='dataMigrationTabsli'><a href='#tabs-2'><img src='images/ETL_Joins.png' style='width:18px;height:18px;'/><span style='vertical-align: 3px;padding: 5px;'>"
					+ new PilogUtilities().convertIntoMultilingualValue(labelObj, "Joins") + "</span></a></li>"
					+ "<li class='dataMigrationTabsli'><a href='#tabs-3'><img src='images/Filter Icon-01.svg' style='width:18px;height:18px;'/><span style='vertical-align: 3px;padding: 5px;'>"
					+ new PilogUtilities().convertIntoMultilingualValue(labelObj, "Filters") + "</span></a></li>"
					+ "<li class='dataMigrationTabsli'><a href='#tabs-4'><img src='images/ETL_sort.png' style='width:18px;height:18px;'/><span style='vertical-align: 3px;padding: 5px;'>"
					+ new PilogUtilities().convertIntoMultilingualValue(labelObj, "Sort") + "</span></a></li>"
					+ "<li class='dataMigrationTabsli'><a href='#tabs-5'><img src='images/ETL_groupBy.png' style='width:18px;height:18px;'/><span style='vertical-align: 3px;padding: 5px;'>"
					+ new PilogUtilities().convertIntoMultilingualValue(labelObj, "Group") + "</span></a></li>"
					// + "<li class='dataMigrationTabsli'><a href='#trnsSQLEditor'>" + new
					// PilogUtilities().convertIntoMultilingualValue(labelObj, "SQL Editor") +
					// "</a></li>"
					+ "<li class='dataMigrationTabsli' onclick=viewTotalSQLQuery(this,'tabs-7')><a href='#tabs-7'><img src='images/ETL_SQLQuery.png' style='width:18px;height:18px;'/><span style='vertical-align: 3px;padding: 5px;'>"
					+ new PilogUtilities().convertIntoMultilingualValue(labelObj, "Script") + "</span></a></li>"
					+ "<li class='dataMigrationTabsli' ><a href='#tabs-8'><img src='images/ETL_advanced.png' style='width:18px;height:18px;'/><span style='vertical-align: 3px;padding: 5px;'>"
					+ new PilogUtilities().convertIntoMultilingualValue(labelObj, "Advanced") + "</span></a></li>"
					+ "</ul>" + "<div id='tabs-1' class='dataMigrationsTabsInner'>" + " </div>"
					+ "<div id='tabs-2' class='dataMigrationsTabsInner'>" + "</div>"
					+ "<div id='tabs-3' class='dataMigrationsTabsInner'>" + "</div>"
					+ "<div id='tabs-4' class='dataMigrationsTabsInner'>" + "</div>"
					+ "<div id='tabs-5' class='dataMigrationsTabsInner'>" + "</div>"
					// + "<div id='trnsSQLEditor'><div id='tabs-6'
					// class='dataMigrationsTabsInner'></div>"
					// + "</div>"
					+ "<div id='tabs-7' class='dataMigrationsTabsInner'>" + "</div>"
					+ "<div id='tabs-8' class='dataMigrationsTabsInner'>"
					+ "<table class='selectTabTable' width='100%'>" + "<tr><th style='text-align:left'>Unique Rows</th>"
					+ "<th style='text-align:left'><input type='checkbox' id='distinctRowsInput'/></th></tr>"
					+ "<tr><th style='text-align:left'>Fetch Rows Range</th>"
					+ "<th style='text-align:left'><input type='number'"
					+ " id='rowsCountFromInput' min='0' oninput=\"validity.valid||(value='');\"/> To "
					+ "<input type='number' id='rowsCountToInput' min='0' oninput=\"validity.valid||(value='');\"/>"
					+ "</th>" + "</tr>" + "<tr><th style='text-align:left'>Operator Type</th>"
					+ "<th style='text-align:left'>" + "<select id='operatorType'>" + "<option selected>Select</option>"
					+ "<option>Insert</option>" + "<option>Update</option>" + "<option>Delete</option>"
					+ "<option>Insert Or Update</option>" + "<option>Delete or Insert</option>"
					+ "<option>Truncate and Insert</option>" + "</select>" + "</th>" + "</tr>"
					+ "<tr><th style='text-align:left'>Show Rejected Records</th>"
					+ "<th style='text-align:left'><input type='checkbox' id='showRejectedRecords'/></th></tr>"
					// ravi SCD TYPES START

					+ "<tr><th style='text-align:left'>SCD Type</th>" + "<th style='text-align:left'>"
					+ "<input type='radio' id='typeNone' name='scdType' value='typeNone'>"
					+ "<label for='typeNone'>None   </label>"
					+ "<input type='radio' id='type1' name='scdType' value='type1'>"
					+ "<label for='type1'>Type 1   </label>"
					+ "<input type='radio' id='type2' name='scdType' value='type2'>"
					+ "<label for='type2'>Type 2   </label>"
					+ "<input type='radio' id='type3' name='scdType' value='type3'>"
					+ "<label for='type3'>Type 3   </label>"
					+ "<input type='radio' id='hybridType' name='scdType' value='hybridType'>"
					+ "<label for='hybridType'>Hybrid Type</label>" + "</th>" + "</tr>"
					// RAVI SCD TYPES END
					+ "</table>" + "" + "</div>" + "";
			String fromConnObjStr = request.getParameter("fromConnObj");
			String toConnObjStr = request.getParameter("toConnObj");
			String fromTable = request.getParameter("fromTable");
			String toTable = request.getParameter("toTable");
			String toIconType = request.getParameter("toIconType");
			String fromOpeartorArrayStr = request.getParameter("fromOpArray");
			String toOpeartorArrayStr = request.getParameter("toOpArray");
			String trfmRulesDataStr = request.getParameter("trfmRulesData");
			JSONObject trfmRulesData = new JSONObject();
			List<Map> fromOperatorsList = new ArrayList<>();
			List<Map> toOperatorsList = new ArrayList<>();
			if (fromOpeartorArrayStr != null && !"".equalsIgnoreCase(fromOpeartorArrayStr)
					&& !"null".equalsIgnoreCase(fromOpeartorArrayStr)) {
				fromOperatorsList = (List<Map>) JSONValue.parse(fromOpeartorArrayStr);
			}
			if (toOpeartorArrayStr != null && !"".equalsIgnoreCase(toOpeartorArrayStr)
					&& !"null".equalsIgnoreCase(toOpeartorArrayStr)) {
				toOperatorsList = (List<Map>) JSONValue.parse(toOpeartorArrayStr);
			}
			if (trfmRulesDataStr != null && !"".equalsIgnoreCase(trfmRulesDataStr)) {
				trfmRulesData = (JSONObject) new JSONValue().parse(trfmRulesDataStr);
			}
			JSONObject fromConnectObj = new JSONObject();
			JSONObject toConnectObj = new JSONObject();
			if (fromConnObjStr != null && !"".equalsIgnoreCase(fromConnObjStr)
					&& !"null".equalsIgnoreCase(fromConnObjStr)) {
				fromConnectObj = (JSONObject) JSONValue.parse(fromConnObjStr);
			}
			if (toConnObjStr != null && !"".equalsIgnoreCase(toConnObjStr) && !"null".equalsIgnoreCase(toConnObjStr)) {
				toConnectObj = (JSONObject) JSONValue.parse(toConnObjStr);
			}
			if (fromOperatorsList != null && !fromOperatorsList.isEmpty()) {
				JSONObject fromOpConnObj = new JSONObject();
				JSONObject fromTableWhereColsObj = new JSONObject();
				JSONObject selectedTableWhereClause = new JSONObject();
				JSONObject selectedTableOrderGroupClause = new JSONObject();
				String whereClauseCondition = "";
				whereClauseCondition = "<div class='visionEtlMappingMain'>"
						+ "<div class=\"visionEtlMappingTablesMainDiv\">"
						+ "<div id='selectedTablesDivId' class='visionEtlMappingTablesDiv visionEtlwhereClauseTablesDiv'>"
						+ "<table id=\"selectedTables\""
						+ " class=\"visionEtlJoinClauseTable\" style=\"width: 100%;\" border=\"1\">" + "<thead>"
						+ "<tr>" + "<th width='20%' class=\"mappedColsTh\" "
						+ "style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">"
						+ "Table Name" + "</th>" + "<th  width='75%' class=\"mappedColsTh\""
						+ " style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">"
						+ "Where Clause" + "</th>" + "<th  width='5%' class=\"mappedColsTh\""
						+ " style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">"
						+ "</th>" + "</tr>" + "</thead>" + "<tbody>";
				String groupByCols = "<div class=\"visionEtlJoinClauseMain\">" + "<div class=\"visionEtlAddIconDiv\">"
						+ "<img data-trstring='' src=\"images/Add icon.svg\" id=\"visionEtlAddRowIcon\" "
						+ "class=\"visionEtlAddRowIcon\" title=\"Add new where clause\""
						+ " onclick=addNewGroupClauseRow(event,id,this) "
						+ "style=\"width:15px;height: 15px;cursor:pointer; float: left;\"/>" + "</div>"
						+ "<div class=\"visionEtlJoinClauseTablesDiv\">" + "<table id=\"fromTablesGroupCauseTable\""
						+ " class=\"visionEtlJoinClauseTable\" style=\"width: 100%;\" border=\"1\">" + "<thead>"
						+ "<tr>" + "<th width='2%' class=\"\" "
						+ "style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\"></th>"
						+ "<th width='98%' class=\"\" "
						+ "style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">"
						+ "Column Name" + "</th>" + "</tr>" + "</thead>" + "<tbody>";
				String orderByColsCondition = "<div class=\"visionEtlJoinClauseMain\">"
						+ "<div class=\"visionEtlAddIconDiv\">"
						+ "<img data-trstring='' src=\"images/Add icon.svg\" id=\"visionEtlAddRowIcon\" "
						+ "class=\"visionEtlAddRowIcon\" title=\"Add new where clause\""
						+ " onclick=addNewOrderClauseRow(event,id,this) "
						+ "style=\"width:15px;height: 15px;cursor:pointer; float: left;\"/>" + "</div>"
						+ "<div class=\"visionEtlJoinClauseTablesDiv\">" + "<table id=\"fromTablesOrderCauseTable\""
						+ " class=\"visionEtlJoinClauseTable\" style=\"width: 100%;\" border=\"1\">" + "<thead>"
						+ "<tr>" + "<th width='2%' class=\"\" "
						+ "style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\"></th>"
						+ "<th width='49%' class=\"\" "
						+ "style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">"
						+ "Column Name" + "</th>" + "<th width='49%' class=\"\""
						+ " style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">"
						+ "Order" + "</th>" + "</tr>" + "</thead>" + "<tbody>";
				String groupByTrString = "";
				String orderByTrString = "";

				int index = 0;
				for (Map fromOperatorMap : fromOperatorsList) {
					if (fromOperatorMap != null && !fromOperatorMap.isEmpty()) {
						fromConnectObj = (JSONObject) fromOperatorMap.get("connObj");
						if (fromConnectObj != null && fromConnectObj.containsKey("fileType")) {

							String fileName = (String) fromConnectObj.get("fileName");
							fileName = fileName.replaceAll("[^a-zA-Z0-9]", "_");
							fromConnectObj.put("fileName", fileName);

							sourceTablesArray.add(fileName);

							JSONArray fromTableColsArrayForClause = new JSONArray();
							// get Headers
							List<Object[]> fromTableColumnList = new ArrayList<>();
							request.setAttribute("fileType", fromConnectObj.get("fileType"));
							List<String> headers = dataMigrationService.getHeadersOfImportedFile(request, response,
									(String) fromConnectObj.get("filePath"));
//                            // ravi headers trim start
							headers = fileHeaderValidations(headers);

							// ravi headers trim end
							if (headers != null && !headers.isEmpty()) {
								JSONObject tableObj = new JSONObject();
								if (fromConnectObj.get("fileName") != null
										&& !"".equalsIgnoreCase(String.valueOf(fromConnectObj.get("fileName")))
										&& !"null".equalsIgnoreCase(String.valueOf(fromConnectObj.get("fileName")))) {
									tableObj.put("id", fromConnectObj.get("fileName"));
									tableObj.put("text", fromConnectObj.get("fileName"));
									tableObj.put("value", fromConnectObj.get("filePath"));
									tableObj.put("icon", fromConnectObj.get("imageIcon"));// imageIcon
									fromTableColsArray.add(tableObj);
									fromTableColsArrayForClause.add(tableObj);
									for (int i = 0; i < headers.size(); i++) {
										String headerName = headers.get(i);
										// fileName
										JSONObject columnObj = new JSONObject();
										columnObj.put("id", headerName);
										columnObj.put("text", headerName);
										columnObj.put("value", fromConnectObj.get("fileName") + ":" + headerName);
										columnObj.put("parentid", fromConnectObj.get("fileName"));
										fromTableColsArray.add(columnObj);
										fromTableColsArrayForClause.add(tableObj);
										Object[] objArray = new Object[2];
										try {
											objArray[0] = fromConnectObj.get("fileName");
											objArray[1] = headerName;
										} catch (Exception e) {
										}
										fromTableColumnList.add(objArray);

									}
								}
							}
							fromOpConnObj.put(fromConnectObj.get("fileName"), fromConnectObj);
							fromTableWhereColsObj.put((String) fromConnectObj.get("fileName"),
									fromTableColsArrayForClause);
							whereClauseCondition += fetchTableWhereClauseTrfnRules(request, fromTableColumnList,
									trfmRulesData, (String) fromConnectObj.get("fileName"), index);
						} else {
							fromOpConnObj.put(fromOperatorMap.get("tableName"), fromConnectObj);
							JSONArray fromTableColsArrayForClause = new JSONArray();
							Object fromConnObj = getConnection(fromConnectObj);
							if (fromConnObj instanceof Connection || fromConnObj instanceof JCO.Client) {
								if (fromConnObj instanceof Connection) {
									fromConnection = (Connection) fromConnObj;
								} else if (fromConnObj instanceof JCO.Client) {
									fromJCOConnection = (JCO.Client) fromConnObj;
								}
								List<Object[]> fromTableColumnList = new ArrayList<>();
								if (fromConnObj instanceof Connection) {
									fromTableColumnList = getTreeDMTableColumnsOpt(fromConnection, request,
											fromConnectObj, (String) fromOperatorMap.get("statusLabel"));
								} else if (fromConnObj instanceof JCO.Client) {
									fromTableColumnList = dataMigrationAccessDAO.getSAPTableColumns(request,
											fromJCOConnection, (String) fromOperatorMap.get("statusLabel"));
								}
								String tableName = (String) fromOperatorMap.get("statusLabel");

								sourceTablesArray.add(tableName);

								if (tableName != null && !"".equalsIgnoreCase(tableName)) {
									JSONObject tableObj = new JSONObject();
									tableObj.put("id",
											("SAP_ECC".equalsIgnoreCase(
													String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))
													|| "SAP_HANA".equalsIgnoreCase(
															String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))
																	? tableName
																	: fromConnectObj.get("CONN_USER_NAME") + "."
																			+ tableName));
									tableObj.put("text", fromConnectObj.get("CONNECTION_NAME") + "." + tableName);
									tableObj.put("value",
											("SAP_ECC".equalsIgnoreCase(
													String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))
													|| "SAP_HANA".equalsIgnoreCase(
															String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))
																	? tableName
																	: fromConnectObj.get("CONN_USER_NAME") + "."
																			+ tableName));
									tableObj.put("icon", "images/GridDB.png");
									fromTableColsArray.add(tableObj);
									fromTableColsArrayForClause.add(tableObj);
									List<String> columnsList = fromTableColumnList.stream()
											.filter(tableColsArray -> (tableName
													.equalsIgnoreCase(String.valueOf(tableColsArray[0]))))
											.map(tableColsArray -> String.valueOf(tableColsArray[1]))
											.collect(Collectors.toList());

									// RAVI DM
									List<String> dataTypeList = new ArrayList();
									try {
										dataTypeList = fromTableColumnList.stream()
												.filter(tableColsArray -> (tableName
														.equalsIgnoreCase(String.valueOf(tableColsArray[0]))))
												.map(tableColsArray -> String.valueOf(tableColsArray[2])
														+ (String.valueOf(tableColsArray[3]) != null
																? " (" + String.valueOf(tableColsArray[3]) + ")"
																: ""))
												.collect(Collectors.toList());
									} catch (Exception ex) {
										ex.printStackTrace();
									}

									for (int j = 0; j < columnsList.size(); j++) {
										if (columnsList.get(j) != null && !"".equalsIgnoreCase(columnsList.get(j))) {
											JSONObject columnObj = new JSONObject();
											columnObj.put("id", (("SAP_ECC".equalsIgnoreCase(
													String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))
													|| "SAP_HANA".equalsIgnoreCase(
															String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))
																	? tableName
																	: fromConnectObj.get("CONN_USER_NAME") + "."
																			+ tableName)
													+ ":" + columnsList.get(j)));
											columnObj.put("text", columnsList.get(j));
											columnObj.put("value", (("SAP_ECC".equalsIgnoreCase(
													String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))
													|| "SAP_HANA".equalsIgnoreCase(
															String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))
																	? tableName
																	: fromConnectObj.get("CONN_USER_NAME") + "."
																			+ tableName)
													+ ":" + columnsList.get(j)));
											columnObj.put("parentid", ("SAP_ECC".equalsIgnoreCase(
													String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))
													|| "SAP_HANA".equalsIgnoreCase(
															String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))
																	? tableName
																	: fromConnectObj.get("CONN_USER_NAME") + "."
																			+ tableName));
											if (dataTypeList != null && !dataTypeList.isEmpty()) {
												columnObj.put("dataType",
														dataTypeList.get(j) != null ? dataTypeList.get(j) : ""); // RAVI
																													// DM

											}

											fromTableColsArray.add(columnObj);
											fromTableColsArrayForClause.add(columnObj);
										}

									}
								}
								fromTableWhereColsObj.put((String) fromOperatorMap.get("tableName"),
										fromTableColsArrayForClause);
								whereClauseCondition += fetchTableWhereClauseTrfnRules(request, fromTableColumnList,
										trfmRulesData, (String) fromOperatorMap.get("tableName"), index);

							} else {
								resultObj.put("connectionFlag", "N");
								resultObj.put("connectionMessage", fromConnObj);
							}

						} // end else for file type
					}
					index++;
				} // end loop
				whereClauseCondition += "</tbody></table></div>" + "</div>" + "<div class=\"joinMapColumnsDivIdmain1\">"
						+ "<div id=\"whereClauseMapColumnsDivId\" class=\"joinMapColumnsDivClass\">" + "" + "</div>"
						+ "</div>" + "</div>";
				JSONObject orderByGroupByObj = fetchOrderByGroupByClauses(request, trfmRulesData);
				if (orderByGroupByObj != null && !orderByGroupByObj.isEmpty()) {
					groupByCols += orderByGroupByObj.get("groupByCondition");
					orderByColsCondition += orderByGroupByObj.get("orderByCondition");
					groupByTrString = "" + orderByGroupByObj.get("groupByTrString");
					orderByTrString = "" + orderByGroupByObj.get("orderByTrString");
				}
				groupByCols += "</tbody></table></div>";
				orderByColsCondition += "</tbody></table></div>";
				selectedTableOrderGroupClause.put("groupByCondition", groupByCols);
				selectedTableOrderGroupClause.put("orderByCondition", orderByColsCondition);
				selectedTableOrderGroupClause.put("groupByTrString", groupByTrString);
				selectedTableOrderGroupClause.put("orderByTrString", orderByTrString);

				selectedTableWhereClause.put("whereClauseCondition", whereClauseCondition);
				selectedTableWhereClause.put("fromTableColsArray", fromTableWhereColsObj);
				selectedTableWhereClause.put("fromTableColsArray", fromTableWhereColsObj);
				resultObj.put("selectedJoinTables",
						joinTransformationRules(request, fromTable, fromOpConnObj, trfmRulesData, fromOperatorsList));
				resultObj.put("selectedTableWhereClause", selectedTableWhereClause);
				resultObj.put("selectedTableOrderGroupClause", selectedTableOrderGroupClause);

			}
			if (toOperatorsList != null && !toOperatorsList.isEmpty()) {
				for (Map toOperatorMap : toOperatorsList) {
					if (toOperatorMap != null && !toOperatorMap.isEmpty()) {
						toConnectObj = (JSONObject) toOperatorMap.get("connObj");
						Object toConnObj = getConnection(toConnectObj);
						if (toConnObj != null && toConnObj instanceof JCO.Client) {
							toJCOConnection = (JCO.Client) toConnObj;
						} else if (toConnObj != null && toConnObj instanceof Connection) {
							toConnection = (Connection) toConnObj;
						}

						if (!(toIconType != null && !"".equalsIgnoreCase(toIconType)
								&& !"null".equalsIgnoreCase(toIconType) && !"SQL".equalsIgnoreCase(toIconType))) {
							List<Object[]> toTableColumnList = new ArrayList<>();
							if (toConnObj != null && toConnObj instanceof JCO.Client) {
								toTableColumnList = dataMigrationAccessDAO.getSAPTableColumns(request, toJCOConnection,
										(String) toOperatorMap.get("statusLabel"));
							} else if (toConnObj != null && toConnObj instanceof Connection) {
								toTableColumnList = getTreeDMTableColumnsOpt(toConnection, request, toConnectObj,
										(String) toOperatorMap.get("statusLabel"));
							}

							String tableName = (String) toOperatorMap.get("statusLabel");

							destinationTablesArray.add(tableName);
							if (tableName != null && !"".equalsIgnoreCase(tableName)) {
								JSONObject tableObj = new JSONObject();
								tableObj.put("id",
										("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1")))
												|| "SAP_HANA".equalsIgnoreCase(
														String.valueOf(toConnectObj.get("CONN_CUST_COL1"))) ? tableName
																: toConnectObj.get("CONN_USER_NAME") + "."
																		+ tableName));
								tableObj.put("text", toConnectObj.get("CONNECTION_NAME") + "." + tableName);
								tableObj.put("value",
										("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1")))
												|| "SAP_HANA".equalsIgnoreCase(
														String.valueOf(toConnectObj.get("CONN_CUST_COL1"))) ? tableName
																: toConnectObj.get("CONN_USER_NAME") + "."
																		+ tableName));
								tableObj.put("icon", "images/GridDB.png");
								toTableColsArray.add(tableObj);
								List<String> columnsList = toTableColumnList.stream()
										.filter(tableColsArray -> (tableName
												.equalsIgnoreCase(String.valueOf(tableColsArray[0]))))
										.map(tableColsArray -> String.valueOf(tableColsArray[1]))
										.collect(Collectors.toList());
								for (int j = 0; j < columnsList.size(); j++) {
									if (columnsList.get(j) != null && !"".equalsIgnoreCase(columnsList.get(j))) {
										JSONObject columnObj = new JSONObject();
										columnObj.put("id",
												(("SAP_ECC".equalsIgnoreCase(
														String.valueOf(toConnectObj.get("CONN_CUST_COL1")))
														|| "SAP_HANA".equalsIgnoreCase(
																String.valueOf(toConnectObj.get("CONN_CUST_COL1")))
																		? tableName
																		: toConnectObj.get("CONN_USER_NAME") + "."
																				+ tableName)
														+ ":" + columnsList.get(j)));
										columnObj.put("text", columnsList.get(j));
										columnObj.put("value",
												(("SAP_ECC".equalsIgnoreCase(
														String.valueOf(toConnectObj.get("CONN_CUST_COL1")))
														|| "SAP_HANA".equalsIgnoreCase(
																String.valueOf(toConnectObj.get("CONN_CUST_COL1")))
																		? tableName
																		: toConnectObj.get("CONN_USER_NAME") + "."
																				+ tableName)
														+ ":" + columnsList.get(j)));
										columnObj.put("parentid",
												("SAP_ECC".equalsIgnoreCase(
														String.valueOf(toConnectObj.get("CONN_CUST_COL1")))
														|| "SAP_HANA".equalsIgnoreCase(
																String.valueOf(toConnectObj.get("CONN_CUST_COL1")))
																		? tableName
																		: toConnectObj.get("CONN_USER_NAME") + "."
																				+ tableName));
										toTableColsArray.add(columnObj);
									}

								}
							}
						} else if (toIconType != null && !"".equalsIgnoreCase(toIconType)
								&& ("XLSX".equalsIgnoreCase(toIconType) || "XLS".equalsIgnoreCase(toIconType)
										|| "XML".equalsIgnoreCase(toIconType) || "JSON".equalsIgnoreCase(toIconType)
										|| "CSV".equalsIgnoreCase(toIconType))) {
							if (toOperatorMap != null && toOperatorMap.containsKey("trfmRules-data")) {
								JSONObject trfmRules_Data = (JSONObject) toOperatorMap.get("trfmRules-data");
								if (trfmRules_Data != null && !trfmRules_Data.isEmpty()
										&& trfmRules_Data.containsKey("fileHeaders")) {
									JSONObject fileHeaders = (JSONObject) trfmRules_Data.get("fileHeaders");
									if (fileHeaders != null && !fileHeaders.isEmpty()) {

										JSONObject tableObj = new JSONObject();

										String fileName = (toOperatorMap.get("userFileName") != null)
												? ((String) toOperatorMap.get("userFileName"))
												: ("V10ETLExport_" + toOperatorMap.get("timeStamp") + "."
														+ toIconType.toLowerCase());
										destinationTablesArray.add(fileName);
										String filePath = etlFilePath + "ETL_EXPORT_" + File.separator
												+ request.getSession(false).getAttribute("ssUsername") + File.separator
												+ "V10ETLExport_" + toOperatorMap.get("timeStamp") + "."
												+ toIconType.toLowerCase();
										tableObj.put("id", fileName);
										tableObj.put("text", fileName);
										tableObj.put("value", filePath);
										tableObj.put("icon", toOperatorMap.get("imageIcon"));// imageIcon
										toTableColsArray.add(tableObj);
										for (int i = 1; i <= fileHeaders.size(); i++) {
											String headerName = (String) fileHeaders.get(String.valueOf(i));
											// fileName
											JSONObject columnObj = new JSONObject();
											columnObj.put("id", headerName);
											columnObj.put("text", headerName);
											columnObj.put("value", headerName);
											columnObj.put("parentid", fileName);
											toTableColsArray.add(columnObj);

										}

									}

								}
							} // ravi file headers end
						}
					}
				}
			}

			String columnMappingStr = "<div id='colMappinAddIconDiv'>"
					+ "<img src=\"images/Add icon.svg\" data-mappedcolumns='' "
					+ "class=\"visionEtlColumnMapIcon\" title=\"Add New Column Map\""
					+ " onclick=addColumnMapping(event,this)" + " style=\"width:15px;height: 15px;cursor:pointer;\"/>"
					+ "<img src=\"images/mapping.svg\" data-mappedcolumns='' "
					+ "class=\"visionEtlColumnMapIcon\" title=\"Map Columns\"" + " onclick=mapAllColumns(event,this,'"
					+ toIconType + "')" + " style=\"width:15px;height: 15px;cursor:pointer;margin-left: 5px;\"/>"
					+ "<img src=\"images/Delete_Red_Icon.svg\" data-mappedcolumns='' "
					+ "class=\"visionEtlColumnMapIcon\" title=\"Delete All column mappings\""
					+ " onclick=deleteAllTableTrs('sourceDestColsTableId')"
					+ " style=\"width:15px;height: 15px;cursor:pointer;margin-left: 5px;\"/>"
					+ "<img src=\"images/attach_download.png\" data-mappedcolumns='' "
					+ "class=\"visionEtlColumnMapIcon\" title=\"Download Template for Bulk Column Mapping\""
					+ " onclick=downloadTemplate('ETL_BULK_COLUMN_MAP','ETL_BULK_COLUMN_MAP')"
					+ " style=\"width:15px;height: 15px;cursor:pointer;margin-left: 5px;\"/>"
					+ "<img src=\"images/attach_upload.png\" data-mappedcolumns='' "
					+ "class=\"visionEtlColumnMapIcon\" title=\"Upload the Excel file for bulk column mapping\""
					+ " onclick=uploadColumnMap(event,this,'importColMapFile')"
					+ " style=\"width:15px;height: 15px;cursor:pointer;margin-left: 5px;\"/>"
					+ "<input name=\"importColMapFile\" id=\"importColMapFile\" type=\"file\" style=\"display:none\">"
					+ "</div>" + "" + "<div id='visionColMappScrollDiv' class='visionColMappScrollDiv1'>"
					+ "<table id=\"sourceDestColsTableId\" class=\"visionEtlJoinClauseTable visionEtlSourceDestColsTable1\" style='width: 100%;' border='1'>"
					+ "<thead>" + "<tr>"
					+ "<th width='1.5%' class=\"visionColMappingImgTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'></th>";

			// PKH PKIcon
//            if (toIconType != null && !"".equalsIgnoreCase(toIconType) && !"null".equalsIgnoreCase(toIconType)
//                    && "XLSX".equalsIgnoreCase(toIconType)) {
//                columnMappingStr += "<th width='2%' class=\"visionColMappingImgTh1\" style=' display:none ;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center '>PK</th>"; //  
//
//            } else {
//                if (!"".equalsIgnoreCase(toConnObjStr) && !"null".equalsIgnoreCase(toConnObjStr) && !"{}".equalsIgnoreCase(toConnObjStr)) {
//                    columnMappingStr += "<th width='2%' class=\"visionColMappingImgTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>PK</th>"; //  
//                }
//            }
			// PKH PKIcon
			columnMappingStr += "<th width='2%' class=\"visionColMappingImgTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>PK</th>"; // RAVI
																																												// PK
			columnMappingStr += "<th width='19%' class=\"mappedColsTh1\" "
					+ "style='background: #0071c5 none repeat scroll 0 0;"
					+ "color: #FFF;text-align: center;'> Destination Columns</th>";
			columnMappingStr += "<th width='20%' class=\"mappedColsTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Source Columns</th>"
					+ "<th width='20%' class=\"mappedColsTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Default Values</th>"
					+ "<th width='20%' class=\"mappedColsTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;display:none;'>Append Values</th>"
					+ "<th width='20%' class=\"mappedColsTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Column Clause</th>"
					+ "</tr>" + "</thead>" + "<tbody>";

			// ravi start ----
			String colMapTrString = "";
			String colMapSingleTrString = "";

			colMapSingleTrString += "<tr style = 'height: 1px'>"
					+ "<td width='1.5%' class=\"visionColMappingImgTd1\" ><img src=\"images/Delete_Red_Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionColMappingImg\""
					+ " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>" + "</td>";

			// PKH PK Icon
//            if (!"".equalsIgnoreCase(toConnObjStr) && !"null".equalsIgnoreCase(toConnObjStr) && !"{}".equalsIgnoreCase(toConnObjStr)) {
//                colMapSingleTrString += "<td width='2%' class=\"visionColMappingImgTd1\" ><input type=\"checkbox\" class=\"visionPKSelectCbx\" /> </td>";
//            }
			// PKH PK Icon
			colMapSingleTrString += "<td width='2%' class=\"visionColMappingImgTd1\" ><input type=\"checkbox\" class=\"visionPKSelectCbx\" />"
					+ "</td>"; // ravi pk
			if (toIconType != null && !"".equalsIgnoreCase(toIconType) && !"null".equalsIgnoreCase(toIconType)
					&& !"SQL".equalsIgnoreCase(toIconType)) {
				colMapSingleTrString += "<td width='19%' ><input class='visionColMappingInput' type='text' value='' />"
						+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
						+ " onclick=\"selectColumn(this,'"
						+ ((toTableColsArray != null && !toTableColsArray.isEmpty()) ? "toColumn" : "fromColumn")
						+ "')\"" + " style=\"\"></td>";
			} else {
				colMapSingleTrString += "<td width='19%' ><input class='visionColMappingInput' type='text' value='' readonly='true'/>"
						+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
						+ " onclick=\"selectColumn(this,'toColumn')\" style=\"\"></td>";
			}
//            colMapSingleTrString += "<td width='19%' style='" + ((toIconType != null && !"".equalsIgnoreCase(toIconType) && !"null".equalsIgnoreCase(toIconType) && !"SQL".equalsIgnoreCase(toIconType)) ? "display:none;" : "") + "'><input class='visionColMappingInput' type='text' value='' readonly='true'/>"
//                    + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
//                    + " onclick=\"selectColumn(this,'toColumn')\" style=\"\"></td>";

			colMapSingleTrString += "<td width='20%'><input class='visionColMappingInput' type='text' value='' readonly='true'/>"
					+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
					+ " onclick=\"selectColumn(this,'fromColumn')\" style=\"\"></td>"
					+ "<td width='20%'><input class='visionColMappingTextarea' type='text' value='' ></td>"
					+ "<td width='20%' style='display:none;'><input class='visionColMappingTextarea' type='text' value=''></td>"
					+ "<td width='20%'><input id='visionETLFuncFromColId' class='visionColMappingInput' type='text' value=''>"
					+ "<img title='Select Function' src=\"images/Fx icon-01.svg\" class=\"visionETLColMapImage \" "
					+ " onclick=\"selectColumnFun(this,'fromColumn')\" style=\"\">" + "</td>" + "</tr>";

			JSONArray colMappingsData = new JSONArray();
			if (trfmRulesData != null && !trfmRulesData.isEmpty() && trfmRulesData.size() != 0) {
				colMappingsData = (JSONArray) trfmRulesData.get("colMappingsData");
			}
			String trfmRulesChanged = request.getParameter("trfmRulesChanged");

			// PKH PKIcon
			List toTablePKColumnList = new ArrayList<>();

			if (toConnection != null && toConnection instanceof Connection) {
				Object toConnPKObj = getConnection((JSONObject) JSONValue.parse(toConnObjStr));
				toConnection = (Connection) toConnPKObj;
				String[] tableName = toTable.split("\\.");
				toTablePKColumnList = getPrimaryKeyColumns(toConnection, tableName[1]);
			}

			// PKH PKIcon
			if (colMappingsData != null && colMappingsData.size() > 0 // && "Y".equalsIgnoreCase(trfmRulesChanged)
			) {
				for (int i = 0; i < colMappingsData.size(); i++) {
					colMapTrString += "<tr>"
							+ "<td width='1.5%' class=\"visionColMappingImgTd1\" ><img src=\"images/Delete_Red_Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionColMappingImg\""
							+ " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>" + "</td>";

					if ("Y".equalsIgnoreCase(String.valueOf(trfmRulesData.get("primaryKey")))) {
						String pkChecked = String.valueOf(((JSONObject) colMappingsData.get(i)).get("primaryKey"));

						colMapTrString += "<td width='2%' class=\"visionColMappingImgTd1\" ><input type=\"checkbox\" "
								+ (("Y".equalsIgnoreCase(pkChecked)) ? "checked" : "")
								+ " class=\"visionPKSelectCbx\" />" + "</td>"; // ravi pk
					} else {

//                        if (toIconType != null && !"".equalsIgnoreCase(toIconType) && !"null".equalsIgnoreCase(toIconType)
//                                && "XLSX".equalsIgnoreCase(toIconType)) {
//                            colMapTrString += "<td width='2%' class=\"visionColMappingImgTd1\" style='display:none'><input type=\"checkbox\"  class=\"visionPKSelectCbx\" />"
//                                    + "</td>";
//                        } else {
//                            if (!"".equalsIgnoreCase(toConnObjStr) && !"null".equalsIgnoreCase(toConnObjStr) && !"{}".equalsIgnoreCase(toConnObjStr)) {
						JSONObject destinationColumnStr = (JSONObject) colMappingsData.get(i);
						String destinationColumn = (String) destinationColumnStr.get("destinationColumn");
						String[] parts = destinationColumn.split(":");
						boolean flag = toTablePKColumnList.contains(parts[1]);
						if (flag) {
							colMapTrString += "<td width='2%' class=\"visionColMappingImgTd1\" ><input type=\"checkbox\" checked class=\"visionPKSelectCbx\" />"
									+ "</td>";
						} else {
							colMapTrString += "<td width='2%' class=\"visionColMappingImgTd1\" ><input type=\"checkbox\"  class=\"visionPKSelectCbx\" />"
									+ "</td>";
						}
//                            }
//                        }
						// PKH PKIcon

					}

					if (toIconType != null && !"".equalsIgnoreCase(toIconType) && !"null".equalsIgnoreCase(toIconType)
							&& !"SQL".equalsIgnoreCase(toIconType)) {
						colMapTrString += "<td width='19%' >"
								+ "<input class='visionColMappingInput' type='text' value='"
								+ ((JSONObject) colMappingsData.get(i)).get("destinationColumn") + "'" + " title='"
								+ ((JSONObject) colMappingsData.get(i)).get("destinationColumn") + "' />"
								+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
								+ " onclick=\"selectColumn(this,'"
								+ ((toTableColsArray != null && !toTableColsArray.isEmpty()) ? "toColumn"
										: "fromColumn")
								+ "')\" ></td>";
					} else {
						colMapTrString += "<td width='19%' >"
								+ "<input class='visionColMappingInput' type='text' value='"
								+ ((JSONObject) colMappingsData.get(i)).get("destinationColumn") + "'" + " title='"
								+ ((JSONObject) colMappingsData.get(i)).get("destinationColumn") + "' readonly='true'/>"
								+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
								+ " onclick=\"selectColumn(this,'toColumn')\" style=\"\"></td>";
					}
//                    colMapTrString += "<td width='19%' style='" + ((toIconType != null && !"".equalsIgnoreCase(toIconType) && !"null".equalsIgnoreCase(toIconType) && !"SQL".equalsIgnoreCase(toIconType)) ? "display:none;" : "") + "'>"
//                            + "<input class='visionColMappingInput' type='text' value='" + ((JSONObject) colMappingsData.get(i)).get("destinationColumn") + "' title='" + ((JSONObject) colMappingsData.get(i)).get("destinationColumn") + "' readonly='true'/>"
//                            + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
//                            + " onclick=\"selectColumn(this,'toColumn')\" style=\"\"></td>";

					colMapTrString += "<td width='20%'><input class='visionColMappingInput' type='text'  value=\""
							+ ((JSONObject) colMappingsData.get(i)).get("sourceColumn") + "\" title=\""
							+ ((JSONObject) colMappingsData.get(i)).get("sourceColumn") + "\" readonly='true'/>"
							+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
							+ " onclick=\"selectColumn(this,'fromColumn')\" style=\"\"></td>"
							+ "<td width='20%'><input class='visionColMappingTextarea' type='text' value=\""
							+ ((JSONObject) colMappingsData.get(i)).get("defaultValue") + "\" title=\""
							+ ((JSONObject) colMappingsData.get(i)).get("defaultValue") + "\" ></td>"
							+ "<td width='20%' style='display:none;'><input class='visionColMappingTextarea' type='text' value=\""
							+ ((JSONObject) colMappingsData.get(i)).get("appendValue") + "\" title=\""
							+ ((JSONObject) colMappingsData.get(i)).get("appendValue") + "\"></td>"
							+ "<td width='20%'><input class='visionColMappingInput' id='visionETLFuncChildColId' type='text'"
							+ " value=\""
							+ ((((JSONObject) colMappingsData.get(i)).get("data-columnClause")) != null
									? (((JSONObject) colMappingsData.get(i)).get("data-columnClause"))
									: (((JSONObject) colMappingsData.get(i)).get("columnClause")))
							+ "\"" + " title=\""
							+ ((((JSONObject) colMappingsData.get(i)).get("data-columnClause")) != null
									? (((JSONObject) colMappingsData.get(i)).get("data-columnClause"))
									: (((JSONObject) colMappingsData.get(i)).get("columnClause")))
							+ "\" "
							// + " value=\"" + ((JSONObject) colMappingsData.get(i)).get("columnClause") +
							// "\""
							// + " title=\"" + ((JSONObject) colMappingsData.get(i)).get("columnClause") +
							// "\" "
							+ " data-funobjstr = '" + ((JSONObject) colMappingsData.get(i)).get("data-funobjstr") + "' "
							+ ">"
							+ "<img title='Select Function' src=\"images/Fx icon-01.svg\" class=\"visionETLColMapImage \" "
							+ " onclick=\"selectColumnFun(this,'fromColumn')\" style=\"\">" + "</td>" + "</tr>";
				}
			} else {
				colMapTrString = colMapSingleTrString;
			}

			JSONObject selectTabObj = (JSONObject) trfmRulesData.get("selectTabObj");
			if (selectTabObj != null && !selectTabObj.isEmpty()) {
				resultObj.put("uniqueRowsFlag", selectTabObj.get("uniqueRowsFlag"));
				resultObj.put("minRows", selectTabObj.get("minRows"));
				resultObj.put("maxRows", selectTabObj.get("maxRows"));
				resultObj.put("operatorType", selectTabObj.get("operatorType"));// ---
				resultObj.put("showRejectedRecords", selectTabObj.get("showRejectedRecords"));// ---
			}
			columnMappingStr += colMapTrString + "</tbody>" + "</table>" + "</div>";
			// for joining

			resultObj.put("toTablePKColumnList", toTablePKColumnList);// PKH PKIcon

			resultObj.put("tabsString", tabsString);
			resultObj.put("columnMapping", columnMappingStr);
			resultObj.put("colMapTrString", colMapSingleTrString);
			resultObj.put("toTableColsArray", toTableColsArray);
			resultObj.put("fromTableColsArray", fromTableColsArray);
			resultObj.put("sourceTablesArray", sourceTablesArray);
			resultObj.put("destinationTablesArray", destinationTablesArray);
			resultObj.put("connectionFlag", "Y");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (fromConnection != null) {
					fromConnection.close();
				}
				if (toConnection != null) {
					toConnection.close();
				}
			} catch (Exception e) {
			}
		}
		return resultObj;
	}

	public List fileHeaderValidations(List<String> headers) {
		try {
			String[] reservedKeyWords = { "ACCESS", "ELSE", "MODIFY", "START", "ADD", "EXCLUSIVE", "NOAUDIT", "SELECT",
					"ALL", "EXISTS", "NOCOMPRESS", "SESSION", "ALTER", "FILE", "NOT", "SET", "AND", "FLOAT", "NOTFOUND",
					"SHARE", "ANY", "FOR", "NOWAIT", "SIZE", "ARRAYLEN", "FROM", "NULL", "SMALLINT", "AS", "GRANT",
					"NUMBER", "SQLBUF", "ASC", "GROUP", "OF", "SUCCESSFUL", "AUDIT", "HAVING", "OFFLINE", "SYNONYM",
					"BETWEEN", "IDENTIFIED", "ON", "SYSDATE", "BY", "IMMEDIATE", "ONLINE", "TABLE", "CHAR", "IN",
					"OPTION", "THEN", "CHECK", "INCREMENT", "OR", "TO", "CLUSTER", "INDEX", "ORDER", "TRIGGER",
					"COLUMN", "INITIAL", "PCTFREE", "UID", "COMMENT", "INSERT", "PRIOR", "UNION", "COMPRESS", "INTEGER",
					"PRIVILEGES", "UNIQUE", "CONNECT", "INTERSECT", "PUBLIC", "UPDATE", "CREATE", "INTO", "RAW", "USER",
					"CURRENT", "IS", "RENAME", "VALIDATE", "DATE", "LEVEL", "RESOURCE", "VALUES", "DECIMAL", "LIKE",
					"REVOKE", "VARCHAR", "DEFAULT", "LOCK", "ROW", "VARCHAR2", "DELETE", "LONG", "ROWID", "VIEW",
					"DESC", "MAXEXTENTS", "ROWLABEL", "WHENEVER", "DISTINCT", "MINUS", "ROWNUM", "WHERE", "DROP",
					"MODE", "ROWS", "WITH" };
			List reservedKeyWordsList = Arrays.asList(reservedKeyWords);
			headers = headers.stream().map(col -> ((String) col).replaceAll("[^a-zA-Z0-9]", "_"))
					.collect(Collectors.toList());

			List tempHeadersList = new ArrayList();
			Map duplicateHeadersList = new HashMap();

			for (int i = 0; i < headers.size(); i++) {
				String col = headers.get(i);
				if (tempHeadersList.contains(col)) {
					duplicateHeadersList.put(col,
							(duplicateHeadersList.get(col) != null) ? ((int) duplicateHeadersList.get(col) + 1) : 1);
					col = col + duplicateHeadersList.get(col);
				}
				if (reservedKeyWordsList.contains(col.toUpperCase())) {
					duplicateHeadersList.put(col,
							(duplicateHeadersList.get(col) != null) ? ((int) duplicateHeadersList.get(col) + 1) : 1);
					col = col + duplicateHeadersList.get(col);
				}
				tempHeadersList.add(col);
			}
			headers = tempHeadersList;
			headers = headers.stream().map(col -> {

				if (((String) col).length() > 32) {
					col = (String) col;
					col = col.substring(col.length() - 31);
				}
				return col;
			}).collect(Collectors.toList());

			headers = headers.stream().map(col -> {
				if (Character.isDigit(((String) col).charAt(0))) {
					col = (String) col;
					col = col.replace(String.valueOf(col.charAt(0)), "A" + col.charAt(0));
				}
				if (col.startsWith("_")) {
					col = (String) col;
					col = col.replace(String.valueOf(col.charAt(0)), "");
				}
				return col;
			}).collect(Collectors.toList());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return headers;
	}

	public String joinTransformationRules(HttpServletRequest request, String fromTables, JSONObject dbFromObj,
			JSONObject trfmRulesData, List<Map> fromOperatorsList) {
		String joinTableString = "";
		try {
			if (fromOperatorsList != null && fromOperatorsList.size() > 1) {

				// ravi start
				String trfmRulesId = request.getParameter("trfmRulesId");
				List<String> fromTablesList = fromOperatorsList.stream().map(fromOp -> {
					String tableName = "";
					try {
						if (fromOp != null && !fromOp.isEmpty()) {
							if (fromOp.get("tableName") != null
									&& !"".equalsIgnoreCase(String.valueOf(fromOp.get("tableName")))
									&& !"null".equalsIgnoreCase(String.valueOf(fromOp.get("tableName")))) {
								tableName = String.valueOf(fromOp.get("tableName"));
							}
							if ("file".equalsIgnoreCase(String.valueOf(fromOp.get("dragType")))) {
								JSONObject fileObj = (JSONObject) fromOp.get("connObj");
								tableName = String.valueOf(fileObj.get("fileName"));
							}
						}
					} catch (Exception e) {

					}
					return tableName;
				}).collect(Collectors.toList());

				JSONArray joinClauseData = new JSONArray();
				// ravi end
				if (trfmRulesData != null && !trfmRulesData.isEmpty()) {
					joinClauseData = (JSONArray) trfmRulesData.get("joinClauseData");
				}

				joinTableString += "<div class=\"visionEtlMappingMain\">" + ""
						+ "<div class=\"visionEtlMappingTablesMainDiv \">"
						+ "<div class=\"visionEtlMappingTablesDiv visionEtlJoinrClauseTablesDiv\">"
						+ "<table class=\"visionEtlMappingTables\" id='EtlMappingTable'"
						+ " style='width: 100%;' border='1'" + " data-join-db='" + (dbFromObj).toJSONString() + "'>"
						+ "<thead>"
						+ "<tr><th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;width: 410px !important' colspan=\"2\">Tables</th>";
				for (int i = 0; i < fromOperatorsList.size(); i++) {
					Map fromOperator = fromOperatorsList.get(i);
					if (fromOperator != null && !fromOperator.isEmpty()) {
						String tableName = (String) fromOperator.get("tableName");
						JSONObject connObj = (JSONObject) fromOperator.get("connObj");
						if (!(tableName != null && !"".equalsIgnoreCase(tableName)
								&& !"null".equalsIgnoreCase(tableName))) {
							tableName = String.valueOf(connObj.get("fileName"));
						}
						joinTableString += "<tr><td class=\"sourceJoinColsTd\">"
								+ "<select id=\"SOURCE_SELECT_JOIN_TABLES_" + i
								+ "\" onchange=changeSelectedTableDb(id," + i + ")  class=\"sourceColsJoinSelectBox\""
								+ " data-table-db='" + (connObj.toJSONString()) + "'>" + ""
								+ generateTableSelectBoxStr(fromTablesList, tableName,
										"SOURCE_SELECT_JOIN_TABLES_" + i + "")
								+ "" + "</select>" + "</td>" + "<td>";
						if (i != 0) {
							joinTableString += "<img src=\"images/mapping.svg\" " + " data-mappedcolumns='"
									+ ((joinClauseData != null && joinClauseData.size() > (i - 1))
											? String.valueOf(joinClauseData.get(i - 1)).replace("'", "&#39;")
											: "")
									+ "'" + " id=\"joinConditionsMap_" + i + "\" "
									+ "class=\"visionEtlMapTableIcon visionEtlJoinClauseMapIcon\" title=\"Map Columns For Join\""
									+ " onclick=showJoinsPopup(event,'" + tableName + "',id," + i + ")"
									+ " style=\"width:15px;height: 15px;cursor:pointer;\"/>";
						}
						joinTableString += "</td>" + "</tr>";

					}
				}
				joinTableString += "</tbody>" + "" + "</table>" + "</div>"
						+ "<div id='viewJoinQueryDivId' class='viewJoinQueryOuterDivClass'>"
						+ "<img src='images/SQL ICON-01.svg' id='viewJoinQuery'"
						+ " onclick='viewJoinQuery()' title='Click here to view the join query'"
						+ " style=\"width:15px;height: 15px;cursor:pointer;\" />"
						+ "<div id='viewJoinQueryId' class='viewJoinQueryDivClass'></div>" + "</div>" + "</div>"
						+ "<div class=\"joinMapColumnsDivIdmain1\">"
						+ "<div id=\"joinMapColumnsDivId\" class=\"joinMapColumnsDivClass\"></div>" + "</div>"
						+ "</div>";

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return joinTableString;
	}

	public JSONObject fetchJoinTableColumnTrfnRules(HttpServletRequest request) {
		String joinsDataStr = "";
		JSONObject resultObj = new JSONObject();
		Connection connection = null;
		JCO.Client fromJCOConnection = null;

		try {
			JSONObject dbObj = new JSONObject();
			JSONObject connectObj = new JSONObject();
			List<Object[]> allColumnList = new ArrayList();

			// ravi start
			String savedJoinType = "";
			String joinType = "";
			// ravi end

			String flowchartDataStr = request.getParameter("flowchartData");
			JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
			JSONObject operators = (JSONObject) flowchartData.get("operators");

			JSONArray masterTablesArray = new JSONArray();
			String dbObjStr = request.getParameter("dbObj");
			if (dbObjStr != null && !"".equalsIgnoreCase(dbObjStr) && !"null".equalsIgnoreCase(dbObjStr)) {
				dbObj = (JSONObject) JSONValue.parse(dbObjStr);
			}
			String childTableName = request.getParameter("tableName");
			if (childTableName != null && !"".equalsIgnoreCase(childTableName)
					&& !"null".equalsIgnoreCase(childTableName) && childTableName.contains(".") && dbObj != null
					&& !dbObj.isEmpty() && !dbObj.containsKey("fileName")) {
				childTableName = childTableName.substring(childTableName.lastIndexOf(".") + 1);
			}
			String masterTables = request.getParameter("sourceTables");
			String iconIndex = request.getParameter("iconIndex");
			String joinDBStr = request.getParameter("joinDBStr");
			String joinColumnMapping = request.getParameter("joinColumnMapping");
			JSONObject joinColumnMappingObj = new JSONObject();
			JSONObject joinDBObject = new JSONObject();
			if (joinDBStr != null && !"".equalsIgnoreCase(joinDBStr) && !"null".equalsIgnoreCase(joinDBStr)) {
				joinDBObject = (JSONObject) JSONValue.parse(joinDBStr);
			}
			if (joinColumnMapping != null && !"".equalsIgnoreCase(joinColumnMapping)) {
				joinColumnMappingObj = (JSONObject) JSONValue.parse(joinColumnMapping);
			}

			if (masterTables != null && !"".equalsIgnoreCase(masterTables) && !"".equalsIgnoreCase(masterTables)) {
				masterTablesArray = (JSONArray) JSONValue.parse(masterTables);
			}
			JSONArray childTableColsTreeArray = new JSONArray();
			Object fromReturendObj = null;
			if (dbObj != null && !dbObj.isEmpty() && !dbObj.containsKey("fileName")) {
				fromReturendObj = getConnection(dbObj);

				String matchedSelectStr = "";
				boolean tableExist = true;
				List<Object[]> childTableColumnList = new ArrayList<>();
				if (fromReturendObj instanceof Connection) {
					connection = (Connection) fromReturendObj;
					childTableColumnList = getTreeDMTableColumnsOpt(connection, request, dbObj, childTableName);
				} else if (fromReturendObj instanceof JCO.Client) {
					fromJCOConnection = (JCO.Client) fromReturendObj;
					childTableColumnList = dataMigrationAccessDAO.getSAPTableColumns(request, fromJCOConnection,
							childTableName);
				}
				if (childTableColumnList != null && childTableColumnList.isEmpty()) {
					tableExist = false;
					String selectedOperatorId = childTableName.substring(childTableName.lastIndexOf("_") + 1);
					JSONObject operator = (JSONObject) operators.get(selectedOperatorId);

					List simpleColumnsList = (List) operator.get("simpleColumnsList");
					for (int i = 0; i < simpleColumnsList.size(); i++) {
						Object[] rowData = new Object[2];
						rowData[0] = childTableName;
						rowData[1] = simpleColumnsList.get(i);
						childTableColumnList.add(rowData);
					}

				}
				if (childTableColumnList != null && !childTableColumnList.isEmpty()) {
					JSONObject tableObj = new JSONObject();
					String tableId = "";
					String tabletext = "";
					String tablevalue = "";
					if (tableExist) {
						tableId = ("SAP_ECC".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1")))
								|| "SAP_HANA".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1")))
										? childTableName
										: dbObj.get("CONN_USER_NAME") + "." + childTableName);
						tabletext = dbObj.get("CONNECTION_NAME") + "." + childTableName;
						tablevalue = ("SAP_ECC".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1")))
								|| "SAP_HANA".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1")))
										? childTableName
										: dbObj.get("CONN_USER_NAME") + "." + childTableName);
					} else {
						tableId = childTableName;
						tabletext = childTableName;
						tablevalue = childTableName;
					}

					tableObj.put("id", tableId);// CONNECTION_NAME
					tableObj.put("text", tabletext);
					tableObj.put("value", tablevalue);
					tableObj.put("icon", "images/GridDB.png");
					childTableColsTreeArray.add(tableObj);
					for (int i = 0; i < childTableColumnList.size(); i++) {
						Object[] childColsArray = childTableColumnList.get(i);
						if (childColsArray != null && childColsArray.length != 0) {
							JSONObject columnObj = new JSONObject();

							String columnId = "";
							String columntext = "";
							String columnValue = "";
							String columnParentId = "";
							if (tableExist) {
								columnId = (String) (("SAP_ECC"
										.equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1")))
										|| "SAP_HANA".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1")))
												? childColsArray[0]
												: dbObj.get("CONN_USER_NAME") + "." + childColsArray[0])
										+ ":" + childColsArray[1]);
								columntext = (String) childColsArray[1];
								columnValue = (String) ("SAP_ECC"
										.equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1")))
										|| "SAP_HANA".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1")))
												? childColsArray[0]
												: dbObj.get("CONN_USER_NAME") + "." + childColsArray[0]);
								columnParentId = (String) ("SAP_ECC"
										.equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1")))
										|| "SAP_HANA".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1")))
												? childColsArray[0]
												: dbObj.get("CONN_USER_NAME") + "." + childColsArray[0]);
							} else {
								columnId = (String) childColsArray[1];
								columntext = (String) childColsArray[1];
								columnValue = (String) childColsArray[1];
								columnParentId = (String) childColsArray[0];
							}

							columnObj.put("id", columnId);
							columnObj.put("text", columntext);
							columnObj.put("value", columnValue);
							columnObj.put("parentid", columnParentId);
							childTableColsTreeArray.add(columnObj);
						}

					}
				}

			} else {
				String fileType = (String) dbObj.get("fileType");
				String fromFileName = (String) dbObj.get("fileName");
				String filePath = (String) dbObj.get("filePath");
				List<String> headers = genericProcessETLDataService.getHeadersOfImportedFile(filePath, fileType);
				if (headers != null && !headers.isEmpty()) {
					JSONObject tableObj = new JSONObject();
					if (dbObj.get("fileName") != null && !"".equalsIgnoreCase(String.valueOf(dbObj.get("fileName")))
							&& !"null".equalsIgnoreCase(String.valueOf(dbObj.get("fileName")))) {
						tableObj.put("id", dbObj.get("fileName"));
						tableObj.put("text", dbObj.get("fileName"));
						tableObj.put("value", dbObj.get("filePath"));
						tableObj.put("icon", dbObj.get("imageIcon"));// imageIcon
						childTableColsTreeArray.add(tableObj);
						for (int i = 0; i < headers.size(); i++) {
							String headerName = headers.get(i);
							JSONObject columnObj = new JSONObject();
							columnObj.put("id", dbObj.get("fileName") + ":" + headerName);
							columnObj.put("text", headerName);
							columnObj.put("value", dbObj.get("fileName") + ":" + headerName);
							columnObj.put("parentid", dbObj.get("fileName"));
							childTableColsTreeArray.add(columnObj);

						}
					}
				}
			}

			resultObj.put("childTableColsArray", childTableColsTreeArray);

			// ravi start
			String trString = "<tr>";
			String singleTrString = "<tr>";
			singleTrString += "<td width='5%'><img src=\"images/Delete_Red_Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionTdETLIcons\""
					+ " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>" + "</td>";
//            singleTrString += "<td width='35%' class=\"sourceJoinColsTd\"><input class='visionColJoinMappingInput' type='text' value='' />"
//                    + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
//                    + " onclick=\"selectColumn(this,'childColumn')\" style=\"\"></td>";

//            jagadish functions
			singleTrString += "<td width='35%' class=\"sourceJoinColsTd\"><input class='visionColJoinMappingInput visionColFuncInput' id='visionETLFuncChildColId' type='text' value='' />"
					+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
					+ " onclick=\"selectColumn(this,'childColumn')\" style=\"\"><img title='Select Function' src=\"images/Fx icon-01.svg\" class=\"visionETLColMapImage \" "
					+ " onclick=\"selectColumnFun(this,'childColumn')\" style=\"margin-left: 2px;\"></td>";

//            jagadish functions
			singleTrString += "<td width='10%' class=\"sourceJoinColsTd\">"
					+ "<select id=\"OPERATOR_TYPE\"  class=\"sourceColsJoinSelectBox\">"
					+ "<option  value='=' selected>=</option>" + "<option  value='!='>!=</option>" + "</select>"
					+ "</td>";

//            singleTrString += "<td width='35%' class=\"sourceJoinColsTd\"><input class='visionColJoinMappingInput' type='text' value='' />"
//                    + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
//                    + " onclick=\"selectColumn(this,'masterColumn')\" style=\"\"></td>";
//            jagadish functions
			singleTrString += "<td width='35%' class=\"sourceJoinColsTd\"><input class='visionColJoinMappingInput visionColFuncInput' id='visionETLFuncMasterColId' type='text' value='' />"
					+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
					+ " onclick=\"selectColumn(this,'masterColumn')\" style=\"\"><img title='Select Function' src=\"images/Fx icon-01.svg\" class=\"visionETLColMapImage \" "
					+ " onclick=\"selectColumnFun(this,'masterColumn')\" style=\"margin-left: 2px;\"></td>";
//            jagadish functions

			singleTrString += "<td width='10%'><input type=\"text\" class=\"defaultValues\" id=\"static_value_0\"></td>"
					+ "<td width='5%'>" + "<select id='andOrOpt'>" + "<option value='AND'>AND</option>"
					+ "<option value='OR'>OR</option>" + "</select>" + "</td>";
			singleTrString += "</tr>";

			JSONArray masterTableColsTreeArray = new JSONArray();
			for (int i = 0; i < masterTablesArray.size(); i++) {
				String masterTableName = (String) masterTablesArray.get(i);
				JSONObject masterDBObject = (JSONObject) joinDBObject.get(masterTableName);
				if (masterDBObject != null && !masterDBObject.isEmpty() && !masterDBObject.containsKey("fileName")) {
					Object masterConection = null;
					if (masterDBObject != null && !masterDBObject.isEmpty()
							&& !((String) masterDBObject.get("CONNECTION_NAME"))
									.equalsIgnoreCase((String) dbObj.get("CONNECTION_NAME"))) {
						masterConection = getConnection(masterDBObject);
						if (masterConection instanceof Connection) {
							connection = (Connection) masterConection;

						} else if (masterConection instanceof JCO.Client) {
							fromJCOConnection = (JCO.Client) masterConection;
						}

					} else {
						masterConection = fromReturendObj;
					}

//                    if (request.getParameter("tableName") != null && !"".equalsIgnoreCase(request.getParameter("tableName"))
//                            && !childTableName.equalsIgnoreCase(request.getParameter("tableName"))) {
					if (masterTableName != null && !"".equalsIgnoreCase(masterTableName)
							&& !"null".equalsIgnoreCase(masterTableName) && masterTableName.contains(".")) {
						masterTableName = masterTableName.substring(masterTableName.lastIndexOf(".") + 1);
					}

					List<Object[]> columnList = new ArrayList<>();
					boolean masterTableExist = true;
					if (masterConection instanceof Connection) {
						columnList = getTreeDMTableColumnsOpt(connection, request, masterDBObject, masterTableName);
					} else if (masterConection instanceof JCO.Client) {
						columnList = dataMigrationAccessDAO.getSAPTableColumns(request, fromJCOConnection,
								masterTableName);
					}

					if (columnList != null && columnList.isEmpty()) {
						masterTableExist = false;
						String selectedOperatorId = masterTableName.substring(masterTableName.lastIndexOf("_") + 1);
						JSONObject operator = (JSONObject) operators.get(selectedOperatorId);

						List simpleMasterColumnsList = (List) operator.get("simpleColumnsList");
						for (int j = 0; j < simpleMasterColumnsList.size(); j++) {
							Object[] rowData = new Object[2];
							rowData[0] = masterTableName;
							rowData[1] = simpleMasterColumnsList.get(j);
							columnList.add(rowData);
						}

					}

					if (columnList != null && !columnList.isEmpty()) {
						JSONObject tableObj = new JSONObject();
						String tableId = "";
						String tabletext = "";
						String tablevalue = "";
						if (masterTableExist) {
							tableId = ("SAP_ECC".equalsIgnoreCase(String.valueOf(masterDBObject.get("CONN_CUST_COL1")))
									|| "SAP_HANA".equalsIgnoreCase(String.valueOf(masterDBObject.get("CONN_CUST_COL1")))
											? masterTableName
											: masterDBObject.get("CONN_USER_NAME") + "." + masterTableName);
							tabletext = masterDBObject.get("CONNECTION_NAME") + "." + masterTableName;
							tablevalue = ("SAP_ECC"
									.equalsIgnoreCase(String.valueOf(masterDBObject.get("CONN_CUST_COL1")))
									|| "SAP_HANA".equalsIgnoreCase(String.valueOf(masterDBObject.get("CONN_CUST_COL1")))
											? masterTableName
											: masterDBObject.get("CONN_USER_NAME") + "." + masterTableName);
						} else {
							tableId = masterTableName;
							tabletext = masterTableName;
							tablevalue = masterTableName;
						}
						tableObj.put("id", tableId);
						tableObj.put("text", tabletext);
						tableObj.put("value", tablevalue);
						tableObj.put("icon", "images/GridDB.png");
						masterTableColsTreeArray.add(tableObj);
						for (int j = 0; j < columnList.size(); j++) {
							Object[] masterColsArray = columnList.get(j);
							if (masterColsArray != null && masterColsArray.length != 0) {
								JSONObject columnObj = new JSONObject();
								String columnId = "";
								String columntext = "";
								String columnValue = "";
								String columnParentId = "";
								if (masterTableExist) {
									columnId = (String) (("SAP_ECC"
											.equalsIgnoreCase(String.valueOf(masterDBObject.get("CONN_CUST_COL1")))
											|| "SAP_HANA".equalsIgnoreCase(
													String.valueOf(masterDBObject.get("CONN_CUST_COL1")))
															? masterColsArray[0]
															: masterDBObject.get("CONN_USER_NAME") + "."
																	+ masterColsArray[0])
											+ ":" + masterColsArray[1]);
									columntext = (String) masterColsArray[1];
									columnValue = (String) (("SAP_ECC"
											.equalsIgnoreCase(String.valueOf(masterDBObject.get("CONN_CUST_COL1")))
											|| "SAP_HANA".equalsIgnoreCase(
													String.valueOf(masterDBObject.get("CONN_CUST_COL1")))
															? masterColsArray[0]
															: masterDBObject.get("CONN_USER_NAME") + "."
																	+ masterColsArray[0])
											+ ":" + masterColsArray[1]);
									columnParentId = (String) ("SAP_ECC"
											.equalsIgnoreCase(String.valueOf(masterDBObject.get("CONN_CUST_COL1")))
											|| "SAP_HANA".equalsIgnoreCase(
													String.valueOf(masterDBObject.get("CONN_CUST_COL1")))
															? masterColsArray[0]
															: masterDBObject.get("CONN_USER_NAME") + "."
																	+ masterColsArray[0]);
								} else {
									columnId = (String) masterColsArray[1];
									columntext = (String) masterColsArray[1];
									columnValue = (String) masterColsArray[1];
									columnParentId = (String) masterColsArray[0];
								}

								columnObj.put("id", columnId);
								columnObj.put("text", columntext);
								columnObj.put("value", columnValue);
								columnObj.put("parentid", columnParentId);
								masterTableColsTreeArray.add(columnObj);
							}

						}
					}
//                    }
				} else {
//                    if (request.getParameter("tableName") != null && !"".equalsIgnoreCase(request.getParameter("tableName"))
//                            && !childTableName.equalsIgnoreCase(request.getParameter("tableName"))) {
					String fileType = (String) masterDBObject.get("fileType");
					String fromFileName = (String) masterDBObject.get("fileName");
					String filePath = (String) masterDBObject.get("filePath");
					List<String> headers = genericProcessETLDataService.getHeadersOfImportedFile(filePath, fileType);
					if (headers != null && !headers.isEmpty()) {
						JSONObject tableObj = new JSONObject();
						if (masterDBObject.get("fileName") != null
								&& !"".equalsIgnoreCase(String.valueOf(masterDBObject.get("fileName")))
								&& !"null".equalsIgnoreCase(String.valueOf(masterDBObject.get("fileName")))) {
							tableObj.put("id", masterDBObject.get("fileName"));
							tableObj.put("text", masterDBObject.get("fileName"));
							tableObj.put("value", masterDBObject.get("filePath"));
							tableObj.put("icon", masterDBObject.get("imageIcon"));// imageIcon
							masterTableColsTreeArray.add(tableObj);
							for (int j = 0; j < headers.size(); j++) {
								String headerName = headers.get(j);
								JSONObject columnObj = new JSONObject();
								columnObj.put("id", masterDBObject.get("fileName") + ":" + headerName);
								columnObj.put("text", headerName);
								columnObj.put("value", masterDBObject.get("fileName") + ":" + headerName);
								columnObj.put("parentid", masterDBObject.get("fileName"));
								masterTableColsTreeArray.add(columnObj);

							}
						}
					}
//                    }

				}

			}
			resultObj.put("masterTableColsArray", masterTableColsTreeArray);
			trString = singleTrString;
//            }
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
						mappedColTrString += "<td width='5%' ><img src=\"images/Delete_Red_Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionTdETLIcons\""
								+ " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>" + "</td>";

//                              String colVal=(String) joinColMapObj.get("childTableColumn");
////                        String replaceString=colVal.replace("'","\"");
//                        mappedColTrString += "<td width='35%' class=\"sourceJoinColsTd\">"
//                                + "<input class='visionColJoinMappingInput' type='text' value='"+colVal.replace("'","\"") +"' />"
//                                + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
//                                + " onclick=\"selectColumn(this,'childColumn')\" style=\"\"></td>";
						mappedColTrString += "<td width='35%' class=\"sourceJoinColsTd\">"
								+ "<input class='visionColJoinMappingInput visionColFuncInput' type='text' value='"
								+ String.valueOf(joinColMapObj.get("childTableColumn")).replace("'", "&#39;")
								+ "' data-funobjstr='"
								+ String.valueOf(joinColMapObj.get("childTableDataFunStr")).replace("'", "&#39;")
								+ "'/>"
								+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
								+ " onclick=\"selectColumn(this,'childColumn')\" style=\"\">"
								+ "<img title='Select Function' src=\"images/Fx icon-01.svg\" class=\"visionETLColMapImage \" "
								+ " onclick=\"selectColumnFun(this,'childColumn')\" style=\"margin-left: 2px;\">"
								+ "</td>";

						String operator = (String) joinColMapObj.get("operator");

						mappedColTrString += "<td width='10%' class=\"sourceJoinColsTd\">"
								+ "<select id=\"OPERATOR_TYPE\"  class=\"sourceColsJoinSelectBox\">";
						mappedColTrString += "<option  value='=' " + ("=".equalsIgnoreCase(operator) ? "selected" : "")
								+ ">=</option>";
						mappedColTrString += "<option  value='!=' "
								+ ("!=".equalsIgnoreCase(operator) ? "selected" : "") + ">!=</option>";
						mappedColTrString += "</select>" + "</td>";

//                            String masterColVal=(String) joinColMapObj.get("masterTableColumn");
//                             mappedColTrString += "<td width='35%' class=\"sourceJoinColsTd\">"
//                                    + "<input class='visionColJoinMappingInput' type='text' value='" + masterColVal.replace("'","\"") + "' />"
//                                    + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
//                                    + " onclick=\"selectColumn(this,'masterColumn')\" style=\"\"></td>";
						mappedColTrString += "<td width='35%' class=\"sourceJoinColsTd\">"
								+ "<input class='visionColJoinMappingInput visionColFuncInput' type='text' value='"
								+ String.valueOf(joinColMapObj.get("masterTableColumn")).replace("'", "&#39;")
								+ "' data-funobjstr='"
								+ String.valueOf(joinColMapObj.get("masterTableDataFunStr")).replace("'", "&#39;")
								+ "' />"
								+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
								+ " onclick=\"selectColumn(this,'masterColumn')\" style=\"\">"
								+ "<img title='Select Function' src=\"images/Fx icon-01.svg\" class=\"visionETLColMapImage \" "
								+ " onclick=\"selectColumnFun(this,'masterColumn')\" style=\"margin-left: 2px;\">"
								+ "</td>";

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
			joinsDataStr += "<div class=\"visionEtlJoinClauseMain\">" + "<div class=\"visionEtlAddIconDiv\">"
					+ "<img data-trstring='' src=\"images/Add icon.svg\" id=\"visionEtlAddRowIcon\" "
					+ "class=\"visionEtlAddRowIcon\" title=\"Add column for mapping\""
					+ " onclick=addNewJoinsRow(event,'',id) "
					+ "style=\"width:15px;height: 15px;cursor:pointer; float: left;\"/>"
					+ "<img data-trstring='' src=\"images/Save Icon.svg\" id=\"visionEtlSaveIcon\" "
					+ "class=\"visionEtlAddRowIcon\" title=\"Save Mapping\"" + " onclick=saveJoinMapping(event,id) "
					+ "style=\"width:15px;height: 15px;cursor:pointer; float: left;\"/>"
					+ "<span class='visionColumnJoinType'>Join Type : </span>"
					+ "<select class='visionColumnJoinTypeSelect' id='joinType'>" + "<option value='INNER JOIN' "
					+ ("INNER JOIN".equalsIgnoreCase(joinType) ? "selected" : "") + " >Inner Join</option>"
					+ "<option value='JOIN' " + ("JOIN".equalsIgnoreCase(joinType) ? "selected" : "") + ">Join</option>"
					+ "<option value='LEFT OUTER JOIN' "
					+ ("LEFT OUTER JOIN".equalsIgnoreCase(joinType) ? "selected" : "") + ">Left Outer Join</option>"
					+ "<option value='RIGHT OUTER JOIN' "
					+ ("RIGHT OUTER JOIN".equalsIgnoreCase(joinType) ? "selected" : "") + ">Right Outer Join</option>"
					+ "<option value='OUTER JOIN' " + ("OUTER JOIN".equalsIgnoreCase(joinType) ? "selected" : "")
					+ ">Outer Join</option>" + "</select>" + "</div>"
					+ "<div class=\"visionEtlJoinClauseTablesDiv visionEtlJoinClauseTablesDivScroll\">"
					+ "<table class=\"visionEtlJoinClauseTable\" id='etlJoinClauseTable' style='width: 100%;' border='1'>"
					+ "<thead>" + "<tr>"
					+ "<th width='5%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'></th>"
					+ "<th width='35%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Child Column</th>"
					+ "<th width='10%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Operator</th>"
					+ "<th width='35%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Master Column</th>"
					+ "<th width='10%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Static Value</th>"
					+ "<th width='5%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>AND/OR</th>"
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

	public String fetchTableWhereClauseTrfnRules(HttpServletRequest request, List<Object[]> fromTableColumnList,
			JSONObject trfmRulesData, String tableName, int index) {
		String whereClauseCondition = "";

		try {
			String trfmRulesId = request.getParameter("trfmRulesId");
			JSONArray whereClauseData = new JSONArray();
			if (trfmRulesData != null && !trfmRulesData.isEmpty()) {
				whereClauseData = (JSONArray) trfmRulesData.get("whereClauseData");
			}
			List<String> whereClauseDataList = new ArrayList<>();
			if (whereClauseData != null && !whereClauseData.isEmpty()) {
				whereClauseDataList.addAll(whereClauseData);
			}
			// ravi end
			if (tableName != null && !"".equalsIgnoreCase(tableName)) {
				String whereClauseObjStr = "";
				String whereClauseStr = "";
				if (whereClauseDataList != null && !whereClauseDataList.isEmpty()) {
					List<String> matchedClauseList = whereClauseDataList.stream()
							.filter(clauseObjStr -> (clauseObjStr != null && !"".equalsIgnoreCase(clauseObjStr)
									&& !"null".equalsIgnoreCase(clauseObjStr) && clauseObjStr.contains(tableName)))
							.collect(Collectors.toList());
					if (matchedClauseList != null && !matchedClauseList.isEmpty()) {
						whereClauseObjStr = matchedClauseList.get(0);
					}
					if (whereClauseObjStr != null && !"".equalsIgnoreCase(whereClauseObjStr)
							&& !"null".equalsIgnoreCase(whereClauseObjStr)) {
						JSONObject whereClauseObjList = (JSONObject) JSONValue.parse(whereClauseObjStr);
						if (whereClauseObjList != null && !whereClauseObjList.isEmpty()) {
							int j = 0;
							for (Object key : whereClauseObjList.keySet()) {
								JSONObject whereClauseColObj = (JSONObject) whereClauseObjList.get(key);
								if (whereClauseColObj != null && !whereClauseColObj.isEmpty()
										&& whereClauseColObj.get("columnName") != null
										&& !"".equalsIgnoreCase(String.valueOf(whereClauseColObj.get("columnName")))
										&& !"null".equalsIgnoreCase(String.valueOf(whereClauseColObj.get("columnName")))
										&& whereClauseColObj.get("staticValue") != null
										&& !"".equalsIgnoreCase(String.valueOf(whereClauseColObj.get("staticValue")))
										&& !"null".equalsIgnoreCase(
												String.valueOf(whereClauseColObj.get("staticValue")))) {
									whereClauseStr += " "
											+ String.valueOf(whereClauseColObj.get("columnName")).replace(":", ".")
											+ " " + "" + whereClauseColObj.get("operator") + " " + " '"
											+ whereClauseColObj.get("staticValue") + "' ";// operator
									if (j != whereClauseObjList.size() - 1) {
										whereClauseStr += " " + whereClauseColObj.get("andOrOperator") + " "; // andOrOperator
									}
									j++;
								}

							}
						}
					}
				}
				whereClauseCondition += "<tr>" + "<td width='20%'>" + tableName + "</td>" + "<td width='75%'>"
						+ "<textarea readonly='true' id=\"whereClauseConditionsArea_" + index
						+ "\" class=\"visionColMappingTextarea\"" + " rows=\"2\" cols=\"50\">" + whereClauseStr
						+ "</textarea>" + "</td>" + "<td width='5%'>" + "<img src=\"images/mapping.svg\""
						+ " data-whereclause='" + (whereClauseObjStr.replaceAll("'", "&#39;"))
						+ "' id=\"whereClauseConditionsMap_" + index + "\" "
						+ "class=\"visionEtlMapTableIcon visionEtlWhereClauseMapIcon\" title=\"Build where clause\""
						+ " onclick=showWhereClausePopup(event,'" + tableName + "',id,this," + index + ")"
						+ " style=\"width:15px;height: 15px;cursor:pointer;\"/>" + "</td>" + "</tr>";
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return whereClauseCondition;
	}

	public JSONObject fetchOrderByGroupByClauses(HttpServletRequest request, JSONObject trfmRulesData) {
		JSONObject resultObj = new JSONObject();
		try {

			// ravi start ---------------------
			List<Object[]> orderByDataList = new ArrayList<>();
			List<String> groupByDataList = new ArrayList<>();

			String trfmRulesId = request.getParameter("trfmRulesId");
			String trfmRulesChanged = request.getParameter("trfmRulesChanged");

			// ravi end -----------------
			String groupByCols = "";

			// ravi start
			String groupByTrString = "";
			String groupBySingleTrString = "<tr>";
			groupBySingleTrString += "<td width='1%'>" + "<img src=\"images/Delete_Red_Icon.svg\" "
					+ "onclick=\"deleteSelectedRow(this)\" class=\"visionColMappingImg\" "
					+ "title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"></td>"
					+ "<td width='99%'  class=\"sourceJoinColsTd\">"
					+ "<input class=\"visionColMappingInput\" type=\"text\" value=\"\" readonly=\"true\">"
					+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \""
					+ " onclick=\"selectColumn(this,'fromColumn')\" style=\"\"></td>" + "</td>" + "</tr>";

			JSONArray groupByData = new JSONArray();
			JSONArray orderByData = new JSONArray();

			if (trfmRulesData != null && !trfmRulesData.isEmpty() && trfmRulesData.size() != 0) {
				groupByData = (JSONArray) trfmRulesData.get("groupByData");
				orderByData = (JSONArray) trfmRulesData.get("orderByData");
			}

			if (groupByData != null && groupByData.size() != 0 // && "Y".equalsIgnoreCase(trfmRulesChanged)
			) {
				for (int i = 0; i < groupByData.size(); i++) {
					groupByTrString += "<tr>" + "<td width='1%'>" + "<img src=\"images/Delete_Red_Icon.svg\" "
							+ "onclick=\"deleteSelectedRow(this)\" class=\"visionColMappingImg\" "
							+ "title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"></td>"
							+ "<td width='99%'  class=\"sourceJoinColsTd\">"
							+ "<input class=\"visionColMappingInput\" type=\"text\" value=\""
							+ ((JSONObject) groupByData.get(i)).get("columnName") + "\" readonly=\"true\">"
							+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \""
							+ " onclick=\"selectColumn(this,'fromColumn')\" style=\"\"></td>" + "</td>" + "</tr>";
				}
			} else if (groupByDataList != null && !groupByDataList.isEmpty()) {
				for (int i = 0; i < groupByDataList.size(); i++) {
					groupByTrString += "<tr>" + "<td width='1%'>" + "<img src=\"images/Delete_Red_Icon.svg\" "
							+ "onclick=\"deleteSelectedRow(this)\" class=\"visionColMappingImg\" "
							+ "title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"></td>"
							+ "<td width='99%'  class=\"sourceJoinColsTd\">"
							+ "<input class=\"visionColMappingInput\" type=\"text\" value=\"" + groupByDataList.get(i)
							+ "\" readonly=\"true\">"
							+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \""
							+ " onclick=\"selectColumn(this,'fromColumn')\" style=\"\"></td>" + "</td>" + "</tr>";
				}
			} else {
				groupByTrString = groupBySingleTrString;
			}
			// ravi end

			groupByCols = groupByCols + groupByTrString + "";
			resultObj.put("groupByCondition", groupByCols);
			resultObj.put("groupByTrString", groupBySingleTrString);

			// For Order By
			String orderByColsCondition = "";

			// ravi start
			String orderByTrString = "";
			String orderBySingleTrString = "<tr>";
			orderBySingleTrString += "<td width='2%'>" + "<img src=\"images/Delete_Red_Icon.svg\" "
					+ "onclick=\"deleteSelectedRow(this)\" class=\"visionColMappingImg\" "
					+ "title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"></td>"
					+ "<td width='49%'  class=\"sourceJoinColsTd\">"
					+ "<input class=\"visionColMappingInput\" type=\"text\" value=\"\" readonly=\"true\">"
					+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \""
					+ " onclick=\"selectColumn(this,'fromColumn')\" style=\"\"></td>"
					+ "<td width='49%' class=\"sourceJoinColsTd\">"
					+ "<select id=\"orderType\"  class=\"sourceColsJoinSelectBox\">"
					+ "<option  value='ASC'>Ascending Order</option>"
					+ "<option  value='DESC'>Descending Order</option>" + "</select>" + "</td>" + "</tr>";

			if (orderByData != null && !orderByData.isEmpty() // && "Y".equalsIgnoreCase(trfmRulesChanged)
			) {
				for (int i = 0; i < orderByData.size(); i++) {
					orderByTrString += "<tr>" + "<td width='2%'>" + "<img src=\"images/Delete_Red_Icon.svg\" "
							+ "onclick=\"deleteSelectedRow(this)\" class=\"visionColMappingImg\" "
							+ "title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"></td>"
							+ "<td width='49%'  class=\"sourceJoinColsTd\">"
							+ "<input class=\"visionColMappingInput\" type=\"text\" value=\""
							+ ((JSONObject) orderByData.get(i)).get("columnName") + "\" readonly=\"true\">"
							+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \""
							+ " onclick=\"selectColumn(this,'fromColumn')\" style=\"\"></td>"
							+ "<td width='49%' class=\"sourceJoinColsTd\">"
							+ "<select id=\"orderType\"  class=\"sourceColsJoinSelectBox\">" + "<option  value='ASC' "
							+ (((String) ((JSONObject) orderByData.get(i)).get("order")).equalsIgnoreCase("ASC")
									? "selected"
									: "")
							+ ">Ascending Order</option>" + "<option  value='DESC' "
							+ (((String) ((JSONObject) orderByData.get(i)).get("order")).equalsIgnoreCase("DESC")
									? "selected"
									: "")
							+ ">Descending Order</option>" + "</select>" + "</td>" + "</tr>";
				}
			} else if (orderByDataList != null && !orderByDataList.isEmpty()) {
				for (int i = 0; i < orderByDataList.size(); i++) {
					orderByTrString += "<tr>" + "<td width='2%'>" + "<img src=\"images/Delete_Red_Icon.svg\" "
							+ "onclick=\"deleteSelectedRow(this)\" class=\"visionColMappingImg\" "
							+ "title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"></td>"
							+ "<td width='49%'  class=\"sourceJoinColsTd\">"
							+ "<input class=\"visionColMappingInput\" type=\"text\" value=\""
							+ (String) orderByDataList.get(i)[0] + "\" readonly=\"true\">"
							+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \""
							+ " onclick=\"selectColumn(this,'fromColumn')\" style=\"\"></td>"
							+ "<td width='49%' class=\"sourceJoinColsTd\">"
							+ "<select id=\"orderType\"  class=\"sourceColsJoinSelectBox\">" + "<option  value='ASC' "
							+ (((String) orderByDataList.get(i)[1]).equalsIgnoreCase("ASC") ? "selected" : "")
							+ ">Ascending Order</option>" + "<option  value='DESC' "
							+ (((String) orderByDataList.get(i)[1]).equalsIgnoreCase("DESC") ? "selected" : "")
							+ ">Descending Order</option>" + "</select>" + "</td>" + "</tr>";
				}
			} else {
				orderByTrString = orderBySingleTrString;
			}

			// ravi end
			orderByColsCondition = orderByColsCondition + orderByTrString + "";
			resultObj.put("orderByCondition", orderByColsCondition);
			resultObj.put("orderByTrString", orderBySingleTrString);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject fetchOrderByGroupByClauses(HttpServletRequest request, String fromTable,
			List<Object[]> fromTableColumnList, JSONObject trfmRulesData) {
		JSONObject resultObj = new JSONObject();
		try {

			// ravi start ---------------------
//            List<Object[]> orderByDataList = new ArrayList<>();
//            List<String> groupByDataList = new ArrayList<>();
//            String trfmRulesId = request.getParameter("trfmRulesId");
//            String trfmRulesChanged = request.getParameter("trfmRulesChanged");
//
//            if (trfmRulesId != null
//                    && !"".equalsIgnoreCase(trfmRulesId)
//                    && trfmRulesChanged != null
//                   // && !"Y".equalsIgnoreCase(trfmRulesChanged)
//                    ) {
//                orderByDataList = genericDataPipingDAO.getSavedOrderByData(request, trfmRulesId);
//                groupByDataList = genericDataPipingDAO.getSavedGroupByData(request, trfmRulesId);
//            }
			// ravi end -----------------
			String groupByCols = "<div class=\"visionEtlJoinClauseMain\">" + "<div class=\"visionEtlAddIconDiv\">"
					+ "<img data-trstring='' src=\"images/Add icon.svg\" id=\"visionEtlAddRowIcon\" "
					+ "class=\"visionEtlAddRowIcon\" title=\"Add new where clause\""
					+ " onclick=addNewGroupClauseRow(event,id,this) "
					+ "style=\"width:15px;height: 15px;cursor:pointer; float: left;\"/>" + "</div>"
					+ "<div class=\"visionEtlJoinClauseTablesDiv\">" + "<table id=\"fromTablesGroupCauseTable\""
					+ " class=\"visionEtlJoinClauseTable\" style=\"width: 100%;\" border=\"1\">" + "<thead>" + "<tr>"
					+ "<th width='1%' class=\"\" "
					+ "style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\"></th>"
					+ "<th width='99%' class=\"\" "
					+ "style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">"
					+ "Column Name" + "</th>" + "</tr>" + "</thead>" + "<tbody>";

			// ravi start
			String groupByTrString = "";
			String groupBySingleTrString = "<tr>";
			groupBySingleTrString += "<td width='1%'>" + "<img src=\"images/Delete_Red_Icon.svg\" "
					+ "onclick=\"deleteSelectedRow(this)\" class=\"visionColMappingImg\" "
					+ "title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"></td>"
					+ "<td width='99%'  class=\"sourceJoinColsTd\">"
					+ "<input class=\"visionColMappingInput\" type=\"text\" value=\"\" readonly=\"true\">"
					+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \""
					+ " onclick=\"selectColumn(this,'fromColumn')\" style=\"\"></td>" + "</td>" + "</tr>";

			JSONArray groupByData = new JSONArray();
			JSONArray orderByData = new JSONArray();

			if (trfmRulesData != null && !trfmRulesData.isEmpty() && trfmRulesData.size() != 0) {
				groupByData = (JSONArray) trfmRulesData.get("groupByData");
				orderByData = (JSONArray) trfmRulesData.get("orderByData");
			}

			if (groupByData != null && groupByData.size() != 0 // && "Y".equalsIgnoreCase(trfmRulesChanged)
			) {
				for (int i = 0; i < groupByData.size(); i++) {
					groupByTrString += "<tr>" + "<td width='1%'>" + "<img src=\"images/Delete_Red_Icon.svg\" "
							+ "onclick=\"deleteSelectedRow(this)\" class=\"visionColMappingImg\" "
							+ "title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"></td>"
							+ "<td width='99%'  class=\"sourceJoinColsTd\">"
							+ "<input class=\"visionColMappingInput\" type=\"text\" value=\""
							+ ((JSONObject) groupByData.get(i)).get("columnName") + "\" readonly=\"true\">"
							+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \""
							+ " onclick=\"selectColumn(this,'fromColumn')\" style=\"\"></td>" + "</td>" + "</tr>";
				}
			} // else if (groupByDataList != null && !groupByDataList.isEmpty()) {
				// for (int i = 0; i < groupByDataList.size(); i++) {
				// groupByTrString += "<tr>"
				// + "<td width='1%'>"
				// + "<img src=\"images/Delete_Red_Icon.svg\" "
				// + "onclick=\"deleteSelectedRow(this)\" class=\"visionColMappingImg\" "
				// + "title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"></td>"
				// + "<td width='99%' class=\"sourceJoinColsTd\">"
				// + "<input class=\"visionColMappingInput\" type=\"text\" value=\"" +
				// groupByDataList.get(i) + "\" readonly=\"true\">"
				// + "<img title='Select Column' src=\"images/tree_icon.svg\"
				// class=\"visionETLColMapImage \""
				// + " onclick=\"selectColumn(this,'fromColumn')\" style=\"\"></td>"
				// + "</td>"
				// + "</tr>";
				// }
				// }
			else {
				groupByTrString = groupBySingleTrString;
			}
			// ravi end

			groupByCols = groupByCols + groupByTrString + "</tbody></table></div>";
			resultObj.put("groupByCondition", groupByCols);
			resultObj.put("groupByTrString", groupBySingleTrString);

			// For Order By
			String orderByColsCondition = "<div class=\"visionEtlJoinClauseMain\">"
					+ "<div class=\"visionEtlAddIconDiv\">"
					+ "<img data-trstring='' src=\"images/Add icon.svg\" id=\"visionEtlAddRowIcon\" "
					+ "class=\"visionEtlAddRowIcon\" title=\"Add new where clause\""
					+ " onclick=addNewOrderClauseRow(event,id,this) "
					+ "style=\"width:15px;height: 15px;cursor:pointer; float: left;\"/>" + "</div>"
					+ "<div class=\"visionEtlJoinClauseTablesDiv\">" + "<table id=\"fromTablesOrderCauseTable\""
					+ " class=\"visionEtlJoinClauseTable\" style=\"width: 100%;\" border=\"1\">" + "<thead>" + "<tr>"
					+ "<th width='2%' class=\"\" "
					+ "style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\"></th>"
					+ "<th width='49%' class=\"\" "
					+ "style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">"
					+ "Column Name" + "</th>" + "<th width='49%' class=\"\""
					+ " style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">" + "Order"
					+ "</th>" + "</tr>" + "</thead>" + "<tbody>";

			// ravi start
			String orderByTrString = "";
			String orderBySingleTrString = "<tr>";
			orderBySingleTrString += "<td width='2%'>" + "<img src=\"images/Delete_Red_Icon.svg\" "
					+ "onclick=\"deleteSelectedRow(this)\" class=\"visionColMappingImg\" "
					+ "title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"></td>"
					+ "<td width='49%'  class=\"sourceJoinColsTd\">"
					+ "<input class=\"visionColMappingInput\" type=\"text\" value=\"\" readonly=\"true\">"
					+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \""
					+ " onclick=\"selectColumn(this,'fromColumn')\" style=\"\"></td>"
					+ "<td width='49%' class=\"sourceJoinColsTd\">"
					+ "<select id=\"orderType\"  class=\"sourceColsJoinSelectBox\">"
					+ "<option  value='ASC'>Ascending Order</option>"
					+ "<option  value='DESC'>Descending Order</option>" + "</select>" + "</td>" + "</tr>";

			if (orderByData != null && !orderByData.isEmpty() // && "Y".equalsIgnoreCase(trfmRulesChanged)
			) {
				for (int i = 0; i < orderByData.size(); i++) {
					orderByTrString += "<tr>" + "<td width='2%'>" + "<img src=\"images/Delete_Red_Icon.svg\" "
							+ "onclick=\"deleteSelectedRow(this)\" class=\"visionColMappingImg\" "
							+ "title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"></td>"
							+ "<td width='49%'  class=\"sourceJoinColsTd\">"
							+ "<input class=\"visionColMappingInput\" type=\"text\" value=\""
							+ ((JSONObject) orderByData.get(i)).get("columnName") + "\" readonly=\"true\">"
							+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \""
							+ " onclick=\"selectColumn(this,'fromColumn')\" style=\"\"></td>"
							+ "<td width='49%' class=\"sourceJoinColsTd\">"
							+ "<select id=\"orderType\"  class=\"sourceColsJoinSelectBox\">" + "<option  value='ASC' "
							+ (((String) ((JSONObject) orderByData.get(i)).get("order")).equalsIgnoreCase("ASC")
									? "selected"
									: "")
							+ ">Ascending Order</option>" + "<option  value='DESC' "
							+ (((String) ((JSONObject) orderByData.get(i)).get("order")).equalsIgnoreCase("DESC")
									? "selected"
									: "")
							+ ">Descending Order</option>" + "</select>" + "</td>" + "</tr>";
				}
			} // else if (orderByDataList != null && !orderByDataList.isEmpty()) {
				// for (int i = 0; i < orderByDataList.size(); i++) {
				// orderByTrString += "<tr>"
				// + "<td width='2%'>"
				// + "<img src=\"images/Delete_Red_Icon.svg\" "
				// + "onclick=\"deleteSelectedRow(this)\" class=\"visionColMappingImg\" "
				// + "title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"></td>"
				// + "<td width='49%' class=\"sourceJoinColsTd\">"
				// + "<input class=\"visionColMappingInput\" type=\"text\" value=\"" + (String)
				// orderByDataList.get(i)[0] + "\" readonly=\"true\">"
				// + "<img title='Select Column' src=\"images/tree_icon.svg\"
				// class=\"visionETLColMapImage \""
				// + " onclick=\"selectColumn(this,'fromColumn')\" style=\"\"></td>"
				// + "<td width='49%' class=\"sourceJoinColsTd\">"
				// + "<select id=\"orderType\" class=\"sourceColsJoinSelectBox\">"
				// + "<option value='ASC' " + (((String)
				// orderByDataList.get(i)[1]).equalsIgnoreCase("ASC") ? "selected" : "") +
				// ">Ascending Order</option>"
				// + "<option value='DESC' " + (((String)
				// orderByDataList.get(i)[1]).equalsIgnoreCase("DESC") ? "selected" : "") +
				// ">Descending Order</option>"
				// + "</select>"
				// + "</td>"
				// + "</tr>";
				// }
				// }
			else {
				orderByTrString = orderBySingleTrString;
			}

			// ravi end
			orderByColsCondition = orderByColsCondition + orderByTrString + "</tbody></table></div>";
			resultObj.put("orderByCondition", orderByColsCondition);
			resultObj.put("orderByTrString", orderBySingleTrString);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public List<Object[]> getTreeDMTableColumnsOpt(Connection connection, HttpServletRequest request, JSONObject dbObj,
			String tableName) {
		List<Object[]> columnsList = new ArrayList<>();
		try {
			if (connection != null) {
				if (tableName != null && tableName.contains(",")) {
//                    tableName = tableName.replaceAll(",", "','");
					List<String> fullTableNamesList = Arrays.asList(tableName.split(",")); // ravi process job issues
					tableName = fullTableNamesList.stream().map(table -> table.split("\\.")[1])
							.collect(Collectors.joining(","));
					// ---------------------------------------- process job issues end
					tableName = tableName.replaceAll(",", "','");
				} else {
					if (tableName != null && !"".equalsIgnoreCase(tableName) && !"null".equalsIgnoreCase(tableName)
							&& tableName.contains(".")) {
						tableName = (tableName.split("[.]"))[1];
					}
				}
				String initParamClassName = "com.pilog.mdm.DAO.V10DataMigrationAccessDAO";
				String initParamMethodName = "getTree" + dbObj.get("CONN_CUST_COL1") + "TableColumns";
				System.out.println(
						" initParamClassName:" + initParamClassName + "initParamMethodName:" + initParamMethodName);
				Class clazz = Class.forName(initParamClassName);
				Class<?>[] paramTypes = { Connection.class, HttpServletRequest.class, String.class, String.class };
				Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
				Object targetObj = new PilogUtilities().createObjectByClass(clazz);
				columnsList = (List<Object[]>) method.invoke(targetObj, connection, request,
						String.valueOf(dbObj.get("CONN_DB_NAME")), tableName);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return columnsList;
	}

	public JSONObject fetchTableWhereClauseTrfnRules(HttpServletRequest request, String fromTable,
			List<Object[]> fromTableColumnList, JSONObject trfmRulesData) {
		JSONObject resultObj = new JSONObject();
		try {

			// ravi start
			String trfmRulesId = request.getParameter("trfmRulesId");
//            if (trfmRulesId != null && !"".equalsIgnoreCase(trfmRulesId)) {
//                String fromTableStr = genericDataPipingDAO.getFromTablesString(request, trfmRulesId);
//                if (fromTableStr != null && !"".equalsIgnoreCase(fromTableStr)) {
//                    fromTable = fromTableStr;
//                }
//            }
			JSONArray whereClauseData = new JSONArray();
			if (trfmRulesData != null && !trfmRulesData.isEmpty()) {
				whereClauseData = (JSONArray) trfmRulesData.get("whereClauseData");
			}
			// ravi end

			String whereClauseCondition = "";
			whereClauseCondition = "<div class='visionEtlMappingMain'>"
					+ "<div id='selectedTablesDivId' class='visionEtlMappingTablesDiv'>"
					+ "<table id=\"selectedTables\""
					+ " class=\"visionEtlJoinClauseTable\" style=\"width: 100%;\" border=\"1\">" + "<thead>" + "<tr>"
					+ "<th width='20%' class=\"mappedColsTh\" "
					+ "style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">"
					+ "Table Name" + "</th>" + "<th  width='75%' class=\"mappedColsTh\""
					+ " style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">"
					+ "Where Clause" + "</th>" + "<th  width='5%' class=\"mappedColsTh\""
					+ " style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">" + "</th>"
					+ "</tr>" + "</thead>" + "<tbody>";

			if (fromTable != null && !"".equalsIgnoreCase(fromTable)) {
				JSONObject fromTableColsObj = new JSONObject();
				String[] fromTableArray = fromTable.split(",");
				if (fromTableArray != null && fromTableArray.length != 0) {
					List<String> whereClauseDataList = new ArrayList<>();
					whereClauseDataList.addAll(whereClauseData);
					for (int i = 0; i < fromTableArray.length; i++) {
						JSONArray fromTableColsArray = new JSONArray();
						String tableName = fromTableArray[i];
						if (tableName != null && !"".equalsIgnoreCase(tableName)) {
							String whereClauseObjStr = "";
							String whereClauseStr = "";
							if (whereClauseDataList != null && !whereClauseDataList.isEmpty()) {
								List<String> matchedClauseList = whereClauseDataList.stream()
										.filter(clauseObjStr -> (clauseObjStr != null
												&& !"".equalsIgnoreCase(clauseObjStr)
												&& !"null".equalsIgnoreCase(clauseObjStr)
												&& clauseObjStr.contains(tableName)))
										.collect(Collectors.toList());
								if (matchedClauseList != null && !matchedClauseList.isEmpty()) {
									whereClauseObjStr = matchedClauseList.get(0);
								}
								if (whereClauseObjStr != null && !"".equalsIgnoreCase(whereClauseObjStr)
										&& !"null".equalsIgnoreCase(whereClauseObjStr)) {
									JSONObject whereClauseObjList = (JSONObject) JSONValue.parse(whereClauseObjStr);
									if (whereClauseObjList != null && !whereClauseObjList.isEmpty()) {
										int j = 0;
										for (Object key : whereClauseObjList.keySet()) {
											JSONObject whereClauseColObj = (JSONObject) whereClauseObjList.get(key);
											if (whereClauseColObj != null && !whereClauseColObj.isEmpty()
													&& whereClauseColObj.get("columnName") != null
													&& !"".equalsIgnoreCase(
															String.valueOf(whereClauseColObj.get("columnName")))
													&& !"null".equalsIgnoreCase(
															String.valueOf(whereClauseColObj.get("columnName")))
													&& whereClauseColObj.get("staticValue") != null
													&& !"".equalsIgnoreCase(
															String.valueOf(whereClauseColObj.get("staticValue")))
													&& !"null".equalsIgnoreCase(
															String.valueOf(whereClauseColObj.get("staticValue")))) {
												whereClauseStr += " "
														+ String.valueOf(whereClauseColObj.get("columnName")).replace(
																":", ".")
														+ " " + "" + whereClauseColObj.get("operator") + " " + " '"
														+ whereClauseColObj.get("staticValue") + "' ";// operator
												if (j != whereClauseObjList.size() - 1) {
													whereClauseStr += " " + whereClauseColObj.get("andOrOperator")
															+ " "; // andOrOperator
												}
												j++;
											}

										}
									}
								}
							}
							whereClauseCondition += "<tr>" + "<td width='20%'>" + tableName + "</td>"
									+ "<td width='75%'>" + "<textarea readonly='true' id=\"whereClauseConditionsArea_"
									+ i + "\" class=\"visionColMappingTextarea\"" + " rows=\"2\" cols=\"50\">"
									+ whereClauseStr + "</textarea>" + "</td>" + "<td width='5%'>"
									+ "<img src=\"images/mapping.svg\"" + " data-whereclause='"
									+ (whereClauseObjStr.replaceAll("'", "&#39;")) + "' id=\"whereClauseConditionsMap_"
									+ i + "\" "
									+ "class=\"visionEtlMapTableIcon visionEtlWhereClauseMapIcon\" title=\"Build where clause\""
									+ " onclick=showWhereClausePopup(event,'" + tableName + "',id,this," + i + ")"
									+ " style=\"width:15px;height: 15px;cursor:pointer;\"/>" + "</td>" + "</tr>";
							JSONObject tableObj = new JSONObject();
							tableObj.put("id", tableName);
							tableObj.put("text", tableName);
							tableObj.put("value", tableName);
							tableObj.put("icon", "images/GridDB.png");
							fromTableColsArray.add(tableObj);
							List<String> columnsList = fromTableColumnList.stream().filter(
									tableColsArray -> (tableName.equalsIgnoreCase(String.valueOf(tableColsArray[0]))))
									.map(tableColsArray -> String.valueOf(tableColsArray[1]))
									.collect(Collectors.toList());
							for (int j = 0; j < columnsList.size(); j++) {
								if (columnsList.get(j) != null && !"".equalsIgnoreCase(columnsList.get(j))) {
									JSONObject columnObj = new JSONObject();
									columnObj.put("id", tableName + ":" + columnsList.get(j));
									columnObj.put("text", columnsList.get(j));
									columnObj.put("value", tableName + ":" + columnsList.get(j));
									columnObj.put("parentid", tableName);
									fromTableColsArray.add(columnObj);
								}

							}
							fromTableColsObj.put(tableName, fromTableColsArray);
						}
					}
					resultObj.put("fromTableColsArray", fromTableColsObj);
				}
				whereClauseCondition += "</tbody></table></div>"
						+ "<div id=\"whereClauseMapColumnsDivId\" class=\"joinMapColumnsDivClass\">" + "" + "</div>"
						+ "</div>";

				resultObj.put("whereClauseCondition", whereClauseCondition);

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public int exportingXLSXFileData(Map toOperator,
			// JSONObject destColumnsObj,
			Map destColumnsObj,
			// JSONObject columnsObj,
			Map columnsObj, // ravi etl integration
			List totalData, PreparedStatement importStmt, List<String> columnsList, JSONObject defaultValuesObj,
			String filePath, String fileName, int lastRowIndex, XSSFSheet sheet, XSSFWorkbook wb) {
		int insertCount = 0;
		try {
			if (totalData != null && !totalData.isEmpty() && columnsObj != null && !columnsObj.isEmpty()) {
				File file12 = new File(filePath);

				if (!file12.exists()) {
					file12.mkdirs();
				}
				int cellIdx = 0;
				File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
				if (lastRowIndex == 0) {
					XSSFRow hssfHeader = sheet.createRow(lastRowIndex);
					WritableFont cellFont = new WritableFont(WritableFont.TIMES, 16);

					WritableCellFormat cellFormat = new WritableCellFormat(cellFont);
					cellFormat.setBackground(Colour.ORANGE);
					XSSFCellStyle cellStyle = wb.createCellStyle();
					cellStyle.setAlignment(HSSFCellStyle.ALIGN_CENTER);
					cellStyle.setWrapText(true);
					for (int i = 0; i < columnsList.size(); i++) {
						String get = columnsList.get(i);
						XSSFCell hssfCell = hssfHeader.createCell(cellIdx++);
						hssfCell.setCellStyle(cellStyle);

						hssfCell.setCellValue(String.valueOf(columnsList.get(i)).split(":")[1]);
					}
					lastRowIndex++;
				}

				for (int i = 0; i < totalData.size(); i++) {
					Map dataObj = (Map) totalData.get(i);
					XSSFRow hssfRow = null;
					if (dataObj != null && !dataObj.isEmpty()) {
						hssfRow = sheet.createRow(lastRowIndex);
						cellIdx = 0;
						for (int j = 0; j < columnsList.size(); j++) {// columns data
							String sourceColumnName = columnsList.get(j);
							if (sourceColumnName != null && !"".equalsIgnoreCase(sourceColumnName)
									&& !"null".equalsIgnoreCase(sourceColumnName)) {
								XSSFCell hSSFCell = hssfRow.createCell(cellIdx++);
//                                    JSONObject mappedColumnObj = (JSONObject) columnsObj.get(sourceColumnName);
								// JSONObject defaultValObj = (JSONObject)
								// defaultValuesObj.get(sourceColumnName);
								if (columnsObj.get(sourceColumnName) != null) {
									try {
										// SET default values
										if (!(dataObj.get(columnsObj.get(sourceColumnName)) != null
												&& !"".equalsIgnoreCase(
														String.valueOf(dataObj.get(columnsObj.get(sourceColumnName))))
												&& !"null".equalsIgnoreCase(String
														.valueOf(dataObj.get(columnsObj.get(sourceColumnName)))))) {
											if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
													&& defaultValuesObj.get(sourceColumnName) != null
													&& !"".equalsIgnoreCase(
															(String) defaultValuesObj.get(sourceColumnName))
													&& !"null".equalsIgnoreCase(
															(String) defaultValuesObj.get(sourceColumnName))) {
												hSSFCell.setCellValue(
														String.valueOf(defaultValuesObj.get(sourceColumnName)));

											} else {
												hSSFCell.setCellValue("");
											}

										} else {
											hSSFCell.setCellValue(String.valueOf(dataObj.get(sourceColumnName)));

										}

									} catch (Exception e) {
										hSSFCell.setCellValue("");
										continue;
									}

								} else {
									if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
											&& defaultValuesObj.get(sourceColumnName) != null
											&& !"".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))
											&& !"null".equalsIgnoreCase(
													(String) defaultValuesObj.get(sourceColumnName))) {
										hSSFCell.setCellValue(String.valueOf(defaultValuesObj.get(sourceColumnName)));
									} else {
										hSSFCell.setCellValue("");
									}

								}
							}

						}
						lastRowIndex++;

					}
					insertCount++;
				}

//                wb.setSheetName(0, sheetName);
				FileOutputStream outs = new FileOutputStream(outputFile);
				wb.write(outs);
				outs.close();

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return insertCount;
	}

	public int exportingXLSFileData(Map toOperator,
			// JSONObject destColumnsObj,
			Map destColumnsObj,
			// JSONObject columnsObj,
			Map columnsObj, // ravi etl integration
			List totalData, PreparedStatement importStmt, List<String> columnsList, JSONObject defaultValuesObj,
			String filePath, String fileName, int lastRowIndex, HSSFSheet sheet, HSSFWorkbook wb) {
		int insertCount = 0;
		try {
			if (totalData != null && !totalData.isEmpty() && columnsObj != null && !columnsObj.isEmpty()) {
				File file12 = new File(filePath);

				if (!file12.exists()) {
					file12.mkdirs();
				}
				int cellIdx = 0;
				File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
				if (lastRowIndex == 0) {
					HSSFRow hssfHeader = sheet.createRow(lastRowIndex);
					WritableFont cellFont = new WritableFont(WritableFont.TIMES, 16);

					WritableCellFormat cellFormat = new WritableCellFormat(cellFont);
					cellFormat.setBackground(Colour.ORANGE);
					HSSFCellStyle cellStyle = wb.createCellStyle();
					cellStyle.setAlignment(HSSFCellStyle.ALIGN_CENTER);
					cellStyle.setWrapText(true);
					for (int i = 0; i < columnsList.size(); i++) {
						String get = columnsList.get(i);
						HSSFCell hssfCell = hssfHeader.createCell(cellIdx++);
						hssfCell.setCellStyle(cellStyle);

						hssfCell.setCellValue(String.valueOf(columnsList.get(i)).split(":")[1]);
					}
					lastRowIndex++;
				}

				for (int i = 0; i < totalData.size(); i++) {
					Map dataObj = (Map) totalData.get(i);
					HSSFRow hssfRow = null;
					if (dataObj != null && !dataObj.isEmpty()) {
						hssfRow = sheet.createRow(lastRowIndex);
						cellIdx = 0;
						for (int j = 0; j < columnsList.size(); j++) {// columns data
							String sourceColumnName = columnsList.get(j);
							if (sourceColumnName != null && !"".equalsIgnoreCase(sourceColumnName)
									&& !"null".equalsIgnoreCase(sourceColumnName)) {
								HSSFCell hSSFCell = hssfRow.createCell(cellIdx++);
//                                    JSONObject mappedColumnObj = (JSONObject) columnsObj.get(sourceColumnName);
								// JSONObject defaultValObj = (JSONObject)
								// defaultValuesObj.get(sourceColumnName);
								if (columnsObj.get(sourceColumnName) != null) {
									try {
										// SET default values
										if (!(dataObj.get(columnsObj.get(sourceColumnName)) != null
												&& !"".equalsIgnoreCase(
														String.valueOf(dataObj.get(columnsObj.get(sourceColumnName))))
												&& !"null".equalsIgnoreCase(String
														.valueOf(dataObj.get(columnsObj.get(sourceColumnName)))))) {
											if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
													&& defaultValuesObj.get(sourceColumnName) != null
													&& !"".equalsIgnoreCase(
															(String) defaultValuesObj.get(sourceColumnName))
													&& !"null".equalsIgnoreCase(
															(String) defaultValuesObj.get(sourceColumnName))) {
												hSSFCell.setCellValue(
														String.valueOf(defaultValuesObj.get(sourceColumnName)));

											} else {
												hSSFCell.setCellValue("");
											}

										} else {
											hSSFCell.setCellValue(String.valueOf(dataObj.get(sourceColumnName)));

										}

									} catch (Exception e) {
										hSSFCell.setCellValue("");
										continue;
									}

								} else {
									if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
											&& defaultValuesObj.get(sourceColumnName) != null
											&& !"".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))
											&& !"null".equalsIgnoreCase(
													(String) defaultValuesObj.get(sourceColumnName))) {
										hSSFCell.setCellValue(String.valueOf(defaultValuesObj.get(sourceColumnName)));
									} else {
										hSSFCell.setCellValue("");
									}

								}
							}

						}
						lastRowIndex++;

					}
					insertCount++;
				}

//                wb.setSheetName(0, sheetName);
				FileOutputStream outs = new FileOutputStream(outputFile);
				wb.write(outs);
				outs.close();

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return insertCount;
	}

	public int exportingCsvAndTxtFileData(Map toOperator,
			// JSONObject destColumnsObj,
			Map destColumnsObj,
			// JSONObject columnsObj,
			Map columnsObj, // ravi etl integration
			List totalData, PreparedStatement importStmt, List<String> columnsList, JSONObject defaultValuesObj,
			String filePath, String fileName, int lastRowIndex) {
		int insertCount = 0;
		try {
			if (totalData != null && !totalData.isEmpty() && columnsObj != null && !columnsObj.isEmpty()) {
				List<String[]> writeFileDataList = new ArrayList<>();
				File file12 = new File(filePath);

				if (!file12.exists()) {
					file12.mkdirs();
				}
				int cellIdx = 0;
				File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
				if (lastRowIndex == 0) {
					String columnsString = columnsList.stream()
							.map(columnName -> (String.valueOf(columnName).split(":")[1]))
							.collect(Collectors.joining(":::"));
					writeFileDataList.add(columnsString.split(":::"));
					lastRowIndex++;
				}

				for (int i = 0; i < totalData.size(); i++) {
					Map dataObj = (Map) totalData.get(i);
					String dataString = "";
					if (dataObj != null && !dataObj.isEmpty()) {
						for (int j = 0; j < columnsList.size(); j++) {// columns data
							String cellvalue = "";
							String sourceColumnName = columnsList.get(j);
							if (sourceColumnName != null && !"".equalsIgnoreCase(sourceColumnName)
									&& !"null".equalsIgnoreCase(sourceColumnName)) {
								if (columnsObj.get(sourceColumnName) != null) {
									try {
										// SET default values
										if (!(dataObj.get(columnsObj.get(sourceColumnName)) != null
												&& !"".equalsIgnoreCase(
														String.valueOf(dataObj.get(columnsObj.get(sourceColumnName))))
												&& !"null".equalsIgnoreCase(String
														.valueOf(dataObj.get(columnsObj.get(sourceColumnName)))))) {
											if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
													&& defaultValuesObj.get(sourceColumnName) != null
													&& !"".equalsIgnoreCase(
															(String) defaultValuesObj.get(sourceColumnName))
													&& !"null".equalsIgnoreCase(
															(String) defaultValuesObj.get(sourceColumnName))) {
												cellvalue = String.valueOf(defaultValuesObj.get(sourceColumnName));

											} else {
												cellvalue = "";
											}

										} else {
											cellvalue = String.valueOf(dataObj.get(sourceColumnName));

										}

									} catch (Exception e) {
										cellvalue = "";
										continue;
									}

								} else {
									if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
											&& defaultValuesObj.get(sourceColumnName) != null
											&& !"".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))
											&& !"null".equalsIgnoreCase(
													(String) defaultValuesObj.get(sourceColumnName))) {
										cellvalue = String.valueOf(defaultValuesObj.get(sourceColumnName));

									} else {
										cellvalue = "";
									}

								}
							}
							if (cellvalue != null && !"".equalsIgnoreCase(cellvalue)
									&& !"null".equalsIgnoreCase(cellvalue)) {
								cellvalue = cellvalue.replaceAll("Â", "");
							}

							dataString += cellvalue;
							if (j != columnsList.size() - 1) {
								dataString += ":::";
							}
						} // end of columns loop
						lastRowIndex++;
						if (dataString != null && !"".equalsIgnoreCase(dataString)) {
							writeFileDataList.add(dataString.split(":::"));
							dataString = "";
						}

					}
					insertCount++;
				} // end of data loop

				FileOutputStream fos = new FileOutputStream(outputFile, true);
				fos.write(0xef);
				fos.write(0xbb);
				fos.write(0xbf);
				CSVWriter writer = new CSVWriter(new OutputStreamWriter(fos, "UTF-8"), '\t');
				writer.writeAll(writeFileDataList, false);
				writer.close();

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return insertCount;
	}

	public int exportingXMLFileData(Map toOperator,
			// JSONObject destColumnsObj,
			Map destColumnsObj,
			// JSONObject columnsObj,
			Map columnsObj, // ravi etl integration
			List totalData, PreparedStatement importStmt, List<String> columnsList, JSONObject defaultValuesObj,
			String filePath, String fileName, int lastRowIndex) {
		int insertCount = 0;
		try {
			if (totalData != null && !totalData.isEmpty() && columnsObj != null && !columnsObj.isEmpty()) {
				File file12 = new File(filePath);
				if (!file12.exists()) {
					file12.mkdirs();
				}
				File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
				if (outputFile.exists()) {
					// file exist
					DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
					DocumentBuilder builder = factory.newDocumentBuilder();
					Document document = builder.parse(new FileInputStream(outputFile), "UTF-8");
					document.getDocumentElement().normalize();
					Element root = document.getDocumentElement();
					Element rootElement = document.getDocumentElement();
					for (int i = 0; i < totalData.size(); i++) {
						Map dataObj = (Map) totalData.get(i);
						if (dataObj != null && !dataObj.isEmpty()) {
							Element itemElement = document.createElement("Item");
							rootElement.appendChild(itemElement);
							for (int j = 0; j < columnsList.size(); j++) {// columns data
								String sourceColumnName = columnsList.get(j);
								Element sourceColumnNameElement = document
										.createElement(sourceColumnName.replaceAll(":", "_").replaceAll(" ", "_"));
								String cellvalue = "";
								if (sourceColumnName != null && !"".equalsIgnoreCase(sourceColumnName)
										&& !"null".equalsIgnoreCase(sourceColumnName)) {
									if (columnsObj.get(sourceColumnName) != null) {
										try {
											// SET default values
											if (!(dataObj.get(columnsObj.get(sourceColumnName)) != null
													&& !"".equalsIgnoreCase(String
															.valueOf(dataObj.get(columnsObj.get(sourceColumnName))))
													&& !"null".equalsIgnoreCase(String
															.valueOf(dataObj.get(columnsObj.get(sourceColumnName)))))) {
												if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
														&& defaultValuesObj.get(sourceColumnName) != null
														&& !"".equalsIgnoreCase(
																(String) defaultValuesObj.get(sourceColumnName))
														&& !"null".equalsIgnoreCase(
																(String) defaultValuesObj.get(sourceColumnName))) {
													cellvalue = String.valueOf(defaultValuesObj.get(sourceColumnName));

												} else {
													cellvalue = "";
												}

											} else {
												cellvalue = String.valueOf(dataObj.get(sourceColumnName));

											}

										} catch (Exception e) {
											cellvalue = "";
											continue;
										}

									} else {
										if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
												&& defaultValuesObj.get(sourceColumnName) != null
												&& !"".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))
												&& !"null".equalsIgnoreCase(
														(String) defaultValuesObj.get(sourceColumnName))) {
											cellvalue = String.valueOf(defaultValuesObj.get(sourceColumnName));

										} else {
											cellvalue = "";
										}

									}
								}
								if (cellvalue != null && !"".equalsIgnoreCase(cellvalue)) {
									cellvalue = cellvalue.replaceAll("Â", "");
									cellvalue = cellvalue.replaceAll("[^\\x00-\\x7F]", " ");
									cellvalue = cellvalue.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", " ");
									cellvalue = cellvalue.replaceAll("\\p{C}", " ");
									// cellvalue = cellvalue.replaceAll("\\s+", " ");

								}
								sourceColumnNameElement.appendChild(document.createTextNode(escape(cellvalue)));
								itemElement.appendChild(sourceColumnNameElement);

							} // end of columns loop
							lastRowIndex++;
							root.appendChild(itemElement);
							insertCount++;
						}
					}
					DOMSource source = new DOMSource(document);
					TransformerFactory transformerFactory = TransformerFactory.newInstance();
					Transformer transformer = transformerFactory.newTransformer();
					StreamResult result = new StreamResult(outputFile);
					transformer.transform(source, result);
				} else {
					String xmlString = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<PiLog_ETL_Data_Export>\n";
					for (int i = 0; i < totalData.size(); i++) {
						Map dataObj = (Map) totalData.get(i);
						if (dataObj != null && !dataObj.isEmpty()) {
							xmlString += "<Item>";
							for (int j = 0; j < columnsList.size(); j++) {// columns data
								String sourceColumnName = columnsList.get(j);
								String cellvalue = "";
								if (sourceColumnName != null && !"".equalsIgnoreCase(sourceColumnName)
										&& !"null".equalsIgnoreCase(sourceColumnName)) {
									if (columnsObj.get(sourceColumnName) != null) {
										try {
											// SET default values
											if (!(dataObj.get(columnsObj.get(sourceColumnName)) != null
													&& !"".equalsIgnoreCase(String
															.valueOf(dataObj.get(columnsObj.get(sourceColumnName))))
													&& !"null".equalsIgnoreCase(String
															.valueOf(dataObj.get(columnsObj.get(sourceColumnName)))))) {
												if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
														&& defaultValuesObj.get(sourceColumnName) != null
														&& !"".equalsIgnoreCase(
																(String) defaultValuesObj.get(sourceColumnName))
														&& !"null".equalsIgnoreCase(
																(String) defaultValuesObj.get(sourceColumnName))) {
													cellvalue = String.valueOf(defaultValuesObj.get(sourceColumnName));
												} else {
													cellvalue = "";
												}
											} else {
												cellvalue = String.valueOf(dataObj.get(sourceColumnName));
											}
										} catch (Exception e) {
											cellvalue = "";
											continue;
										}
									} else {
										if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
												&& defaultValuesObj.get(sourceColumnName) != null
												&& !"".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))
												&& !"null".equalsIgnoreCase(
														(String) defaultValuesObj.get(sourceColumnName))) {
											cellvalue = String.valueOf(defaultValuesObj.get(sourceColumnName));
										} else {
											cellvalue = "";
										}
									}
								}
								if (cellvalue != null && !"".equalsIgnoreCase(cellvalue)) {
									cellvalue = cellvalue.replaceAll("Â", "");
									cellvalue = cellvalue.replaceAll("[^\\x00-\\x7F]", " ");
									cellvalue = cellvalue.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", " ");
									cellvalue = cellvalue.replaceAll("\\p{C}", " ");
									// cellvalue = cellvalue.replaceAll("\\s+", " ");

								}
								xmlString += "<" + sourceColumnName.replaceAll(":", "_").replaceAll(" ", "_") + ">" + ""
										+ escape(cellvalue) + "" + "</"
										+ sourceColumnName.replaceAll(":", "_").replaceAll(" ", "_") + ">\n";

							} // end of columns loop
							lastRowIndex++;
							xmlString += "</Item>\n";

						}
						insertCount++;
					} // end of data loop
					xmlString += "</PiLog_ETL_Data_Export>\n";
					FileOutputStream outs = new FileOutputStream(outputFile);
					outs.write(xmlString.getBytes("UTF-8"));
					outs.close();

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return insertCount;
	}

	public JSONArray getDBFunctions(HttpServletRequest request) {
		JSONArray dbFunctionsArray = new JSONArray();
		try {
			List<Object[]> dbFunList = genericDataPipingDAO.getDBFunctions(request, "");
			if (dbFunList != null && !dbFunList.isEmpty()) {
				for (int i = 0; i < dbFunList.size(); i++) {
					Object[] funDataArray = dbFunList.get(i);
					if (funDataArray != null && funDataArray.length != 0) {
						JSONObject functionObj = new JSONObject();
						functionObj.put("FUN_ID", funDataArray[1]);
						functionObj.put("FUN_DISP_NAME", funDataArray[3]);
						functionObj.put("FUN_DESCR", funDataArray[4]);
						functionObj.put("FUN_NAME", funDataArray[2]);
						functionObj.put("FUN_FORM_ID", funDataArray[22]);
						functionObj.put("FUN_LVL_TYPE", funDataArray[9]);
						functionObj.put("DM_FUN_CUST_COL1", funDataArray[12]);
						functionObj.put("DM_FUN_CUST_COL2", funDataArray[13]);
						if (funDataArray[11] != null && !"".equalsIgnoreCase(String.valueOf(funDataArray[11]))
								&& !"null".equalsIgnoreCase(String.valueOf(funDataArray[11]))) {
							functionObj.put("ICON_PATH", funDataArray[11]);
						}
						dbFunctionsArray.add(functionObj);
						dbFunctionsArray = getDBFunctions(request, (String) funDataArray[1], dbFunctionsArray);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return dbFunctionsArray;
	}

	public JSONArray getDBFunctions(HttpServletRequest request, String hlFunId, JSONArray dbFunctionsArray) {
		try {
			List<Object[]> dbFunList = genericDataPipingDAO.getDBFunctions(request, hlFunId);
			if (dbFunList != null && !dbFunList.isEmpty()) {
				for (int i = 0; i < dbFunList.size(); i++) {
					Object[] funDataArray = dbFunList.get(i);
					if (funDataArray != null && funDataArray.length != 0) {
						JSONObject functionObj = new JSONObject();
						functionObj.put("FUN_ID", funDataArray[1]);
						functionObj.put("FUN_DISP_NAME", funDataArray[3]);
						functionObj.put("FUN_DESCR", funDataArray[4]);
						functionObj.put("FUN_NAME", funDataArray[2]);
						functionObj.put("HL_FUN_ID", funDataArray[5]);
						functionObj.put("FUN_FORM_ID", funDataArray[22]);
						functionObj.put("FUN_LVL_TYPE", funDataArray[9]);
						functionObj.put("DM_FUN_CUST_COL1", funDataArray[12]);
						functionObj.put("DM_FUN_CUST_COL2", funDataArray[13]);
						if (funDataArray[11] != null && !"".equalsIgnoreCase(String.valueOf(funDataArray[11]))
								&& !"null".equalsIgnoreCase(String.valueOf(funDataArray[11]))) {
							functionObj.put("ICON_PATH", funDataArray[11]);
						}
						dbFunctionsArray.add(functionObj);
						dbFunctionsArray = getDBFunctions(request, (String) funDataArray[1], dbFunctionsArray);
					}
				}
			}
		} catch (Exception e) {
		}
		return dbFunctionsArray;
	}

	public String getETLFunctionForm(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String selectedFunRowDataStr = request.getParameter("selectedRowData");
			String columnType = request.getParameter("columnType");
			List columnTypes = new ArrayList();
			columnTypes.add("fromWhereClauseColumn");
			columnTypes.add("childColumn");
			columnTypes.add("masterColumn");
			Map columnMapTypes = new HashMap();
			columnMapTypes.put("fromWhereClauseColumn", "FROM_WHERE_CLAUSE_COLUMN");
			columnMapTypes.put("childColumn", "CHILD_COLUMN");
			columnMapTypes.put("masterColumn", "MASTER_COLUMN");
			if (selectedFunRowDataStr != null && !"".equalsIgnoreCase(selectedFunRowDataStr)
					&& !"null".equalsIgnoreCase(selectedFunRowDataStr)) {
				String dataFunobjstr = request.getParameter("dataFunobjstr");
				JSONObject dataFunObj = new JSONObject();
				if (dataFunobjstr != null && !"".equalsIgnoreCase(dataFunobjstr)
						&& !"null".equalsIgnoreCase(dataFunobjstr)) {
					dataFunObj = (JSONObject) JSONValue.parse(dataFunobjstr);
				}
				JSONObject selectedFunRowData = (JSONObject) JSONValue.parse(selectedFunRowDataStr);
				if (selectedFunRowData != null && !selectedFunRowData.isEmpty()) {
					String columnIconStr = "<div id=\"funFormETLDivIcon\"><table id=\"funFormETLTableIcon\"><tr>";
					String columnMappingStr = "<div id=\"funFormETLDiv\"><table id=\"funFormETLTable\""
							+ " class=\"visionEtlFunFormTable\" style='width: 100%;' border='1'>" + "";
					List<Object[]> funFormList = genericDataPipingDAO.getFunFormList(request, selectedFunRowData);
					if (funFormList != null && !funFormList.isEmpty()) {
						String columnMappingDataStr = "<tr>";
						String columnMappingHeaderStr = "<tr>";
						String addTrString = "<tr>";
						String hiddenAttar = "";
						for (int i = 0; i < funFormList.size(); i++) {

							Object[] funFormColsArray = funFormList.get(i);
							if (funFormColsArray != null && funFormColsArray.length != 0) {
								String displayStyle = "";
								String inputAttar = "";
								if (columnType != null && !"".equalsIgnoreCase(columnType)
										&& !"null".equalsIgnoreCase(columnType) && columnTypes.contains(columnType)) {
									columnType = (String) columnMapTypes.get(columnType);
								} else {
									columnType = (String) funFormColsArray[8];
								}
								if ("T".equalsIgnoreCase(String.valueOf(funFormColsArray[5]))) {
									inputAttar = "<input id='" + funFormColsArray[4] + "' class='visionColMappingInput'"
											+ " type='text'" + " value='"
											+ ((funFormColsArray[7] != null
													&& !"".equalsIgnoreCase(String.valueOf(funFormColsArray[7]))
													&& !"null".equalsIgnoreCase(String.valueOf(funFormColsArray[7])))
															? funFormColsArray[7]
															: "")
											+ "' />";
								} else if ("TP".equalsIgnoreCase(String.valueOf(funFormColsArray[5]))) {
									inputAttar = "<input id='" + funFormColsArray[4] + "' class='visionColMappingInput'"
											+ " type='text'" + " value='"
											+ ((funFormColsArray[7] != null
													&& !"".equalsIgnoreCase(String.valueOf(funFormColsArray[7]))
													&& !"null".equalsIgnoreCase(String.valueOf(funFormColsArray[7])))
															? funFormColsArray[7]
															: "")
											+ "' />"
											+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
											+ " onclick=\"selectFunColumnValue(this,'" + columnType
											+ "')\" style=\"\" />"
											+ "<img title='Select Function' src=\"images/Fx icon-01.svg\" class=\"visionETLColMapImage \" "
											+ " onclick=\"selectColumnFunForm(this,'" + columnType + "')\" style=\"\">";
								} else if ("P".equalsIgnoreCase(String.valueOf(funFormColsArray[5]))) {
									inputAttar = "<input id='" + funFormColsArray[4]
											+ "' readonly='readonly' class='visionColMappingInput'" + " type='text'"
											+ " value='"
											+ ((funFormColsArray[7] != null
													&& !"".equalsIgnoreCase(String.valueOf(funFormColsArray[7]))
													&& !"null".equalsIgnoreCase(String.valueOf(funFormColsArray[7])))
															? funFormColsArray[7]
															: "")
											+ "' />"
											+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
											+ " onclick=\"selectFunColumnValue(this,'" + columnType
											+ "')\" style=\"\" />"
											+ "<img title='Select Function' src=\"images/Fx icon-01.svg\" class=\"visionETLColMapImage \" "
											+ " onclick=\"selectColumnFunForm(this,'" + columnType + "')\" style=\"\">";
								} else if ("TA".equalsIgnoreCase(String.valueOf(funFormColsArray[5]))) {
									inputAttar = "<input id='" + funFormColsArray[4] + "' class='visionColMappingInput'"
											+ " type='text'" + " value='"
											+ ((funFormColsArray[7] != null
													&& !"".equalsIgnoreCase(String.valueOf(funFormColsArray[7]))
													&& !"null".equalsIgnoreCase(String.valueOf(funFormColsArray[7])))
															? funFormColsArray[7]
															: "")
											+ "' />";
								} else if ("L".equalsIgnoreCase(String.valueOf(funFormColsArray[5]))) {
									inputAttar = "<select id='" + funFormColsArray[4]
											+ "' class='visionColMappingInput' >" + ""
											+ genericDataPipingDAO.getGetListOfValues(
													String.valueOf(funFormColsArray[8]),
													String.valueOf(funFormColsArray[7]),
													String.valueOf(funFormColsArray[4]), request)
											+ "</select>";
								} else if ("D".equalsIgnoreCase(String.valueOf(funFormColsArray[5]))) {

								} else if ("C".equalsIgnoreCase(String.valueOf(funFormColsArray[5]))) {
									inputAttar = "<input id='" + funFormColsArray[4] + "' class='visionColMappingInput'"
											+ " type='checkbox'" + " />";
								} else if ("H".equalsIgnoreCase(String.valueOf(funFormColsArray[5]))) {
									displayStyle = "display:none;";
									inputAttar = "<input id='" + funFormColsArray[4] + "' class='visionColMappingInput'"
											+ " type='hidden'" + " value='"
											+ ((funFormColsArray[7] != null
													&& !"".equalsIgnoreCase(String.valueOf(funFormColsArray[7]))
													&& !"null".equalsIgnoreCase(String.valueOf(funFormColsArray[7])))
															? funFormColsArray[7]
															: "")
											+ "' />";
								}
								if (!"MULTI_COLUMNS"
										.equalsIgnoreCase(String.valueOf(selectedFunRowData.get("FUN_LVL_TYPE")))) {
									columnMappingStr += "<tr>" + "<th style='" + displayStyle
											+ "' width='15%' class=\"mappedColsTh1\" " + "style='text-align: center'>"
											+ funFormColsArray[3] + "</th>" + "<td style='" + displayStyle + "'> "
											+ inputAttar + "</td>";
									if (i == funFormList.size() - 1) {
										columnMappingStr += "<tr style='display:none'>"
												+ "<th width='15%' class=\"mappedColsTh1\" "
												+ "style='text-align: center'>Level Type</th>"
												+ "<td width='85%'  ><input id='FUN_LVL_TYPE' class='visionColMappingInput'"
												+ " type='text'" + " value='" + selectedFunRowData.get("FUN_LVL_TYPE")
												+ "' />";
										columnMappingStr += "</td>" + "</tr>";
										columnMappingStr += "<tr style='display:none'>"
												+ "<th width='15%' class=\"mappedColsTh1\" "
												+ "style='text-align: center'>Level Type</th>"
												+ "<td width='85%'  ><input id='DM_FUN_CUST_COL1' class='visionColMappingInput'"
												+ " type='text'" + " value='"
												+ selectedFunRowData.get("DM_FUN_CUST_COL1") + "' />";
										columnMappingStr += "</td>" + "</tr>";
										columnMappingStr += "<tr style='display:none'>"
												+ "<th width='15%' class=\"mappedColsTh1\" "
												+ "style='text-align: center'>Level Type</th>"
												+ "<td width='85%'  ><input id='DM_FUN_CUST_COL2' class='visionColMappingInput'"
												+ " type='text'" + " value='"
												+ selectedFunRowData.get("DM_FUN_CUST_COL2") + "' />";
										columnMappingStr += "</td>" + "</tr>";
									}
								} else {
									if (i == 0) {
										addTrString = "<tr>";
										columnIconStr += "<th><img src=\"images/Add icon.svg\""
												+ " id=\"addMultiPleFunForm\" onclick=\"addNewTableDataRow('funFormETLTableTr','funFormETLTable',this)\""
												+ " title=\"Add new condition\""
												+ " style=\"width:15px;height: 15px;cursor:pointer;\">" + "</th>";
										columnMappingHeaderStr += "<th "
												+ "style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;"
												+ displayStyle + "' >" + "</th>";
										columnMappingDataStr += "<td width='2%'>"
												+ "<img src=\"images/Delete_Red_Icon.svg\" "
												+ "onclick=\"deleteSelectedRow(this)\" class=\"visionColMappingImg\" "
												+ "title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"></td>";
										addTrString += "<td width='2%'>" + "<img src=\"images/Delete_Red_Icon.svg\" "
												+ "onclick=\"deleteSelectedRow(this)\" class=\"visionColMappingImg\" "
												+ "title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"></td>";
									}
									columnMappingHeaderStr += "<th "
											+ "style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;"
											+ displayStyle + "' >" + "" + funFormColsArray[3] + "</th>";
									columnMappingDataStr += "<td style='" + displayStyle + "'>" + inputAttar + "</td>";
									addTrString += "<td style='" + displayStyle + "'>" + inputAttar + "</td>";
									if (i == funFormList.size() - 1) {
										columnMappingHeaderStr += "<th style='display:none' class=\"mappedColsTh1\" "
												+ "style='text-align: center'>Level Type</th>";
										columnMappingDataStr += "<td style='display:none'  >"
												+ "<input id='FUN_LVL_TYPE' class='visionColMappingInput'"
												+ " type='text'" + " value='" + selectedFunRowData.get("FUN_LVL_TYPE")
												+ "' /></td>";

										addTrString += "<td style='display:none'  >"
												+ "<input id='FUN_LVL_TYPE' class='visionColMappingInput'"
												+ " type='text'" + " value='" + selectedFunRowData.get("FUN_LVL_TYPE")
												+ "' /></td>";

										columnMappingHeaderStr += "<th style='display:none' class=\"mappedColsTh1\" "
												+ "style='text-align: center'>Level Type</th>";
										columnMappingDataStr += ""
												+ "<td style='display:none' ><input id='DM_FUN_CUST_COL1' class='visionColMappingInput'"
												+ " type='text'" + " value='"
												+ selectedFunRowData.get("DM_FUN_CUST_COL1") + "' /></td>";
										addTrString += ""
												+ "<td style='display:none' ><input id='DM_FUN_CUST_COL1' class='visionColMappingInput'"
												+ " type='text'" + " value='"
												+ selectedFunRowData.get("DM_FUN_CUST_COL1") + "' /></td>";

										columnMappingHeaderStr += "<th style='display:none' class=\"mappedColsTh1\" "
												+ "style='text-align: center'>Level Type</th>";
										columnMappingDataStr += ""
												+ "<td style='display:none'  ><input id='DM_FUN_CUST_COL2' class='visionColMappingInput'"
												+ " type='text'" + " value='"
												+ selectedFunRowData.get("DM_FUN_CUST_COL2") + "' /></td>";
										addTrString += ""
												+ "<td style='display:none'  ><input id='DM_FUN_CUST_COL2' class='visionColMappingInput'"
												+ " type='text'" + " value='"
												+ selectedFunRowData.get("DM_FUN_CUST_COL2") + "' /></td>";
										addTrString += "</tr>";
										columnMappingHeaderStr += "</tr>";
										columnMappingDataStr += "</tr>";
										resultObj.put("addTrString", addTrString);
										columnMappingStr += "<thead>" + columnMappingHeaderStr + "</tr></thead><tbody>"
												+ columnMappingDataStr + "</tbody>";
									}
								}

							}

						}

					}
					columnIconStr += "" + "<th><img src=\"images/SQL ICON-01.svg\"" + " id=\"viewFunQuery\" "
							+ " title=\"Click here to view the query\""
							+ " style=\"width:15px;height: 15px;cursor:pointer;\">" + "</th>" + "</tr></table></div>";
					columnMappingStr += "</table>" + "</div>";
					resultObj.put("funFormStr", columnIconStr + columnMappingStr);
					resultObj.put("messageFlag", true);//
				} else {
					resultObj.put("message", "Selected Row Data Empty");
					resultObj.put("messageFlag", false);//
				}
			} else {
				resultObj.put("message", "Selected Row Data Empty");
				resultObj.put("messageFlag", false);//
			}
		} catch (Exception e) {
			resultObj.put("message", e.getMessage());
			resultObj.put("messageFlag", false);
		}
		return resultObj.toJSONString();
	}

//    public String getETLFunctionForm(HttpServletRequest request) {
//        JSONObject resultObj = new JSONObject();
//        try {
//            String selectedFunRowDataStr = request.getParameter("selectedRowData");
//            if (selectedFunRowDataStr != null
//                    && !"".equalsIgnoreCase(selectedFunRowDataStr)
//                    && !"null".equalsIgnoreCase(selectedFunRowDataStr)) {
//                String dataFunobjstr = request.getParameter("dataFunobjstr");
//                JSONObject dataFunObj = new JSONObject();
//                if (dataFunobjstr != null
//                        && !"".equalsIgnoreCase(dataFunobjstr)
//                        && !"null".equalsIgnoreCase(dataFunobjstr)) {
//                    dataFunObj = (JSONObject) JSONValue.parse(dataFunobjstr);
//                }
//                JSONObject selectedFunRowData = (JSONObject) JSONValue.parse(selectedFunRowDataStr);
//                if (selectedFunRowData != null && !selectedFunRowData.isEmpty()) {
//                    String columnIconStr = "<div id=\"funFormETLDivIcon\"><table id=\"funFormETLTableIcon\"><tr>";
//                    String columnMappingStr = "<div id=\"funFormETLDiv\"><table id=\"funFormETLTable\""
//                            + " class=\"visionEtlFunFormTable\" style='width: 100%;' border='1'>"
//                            + "";
//                    List<Object[]> funFormList = genericDataPipingDAO.getFunFormList(request, selectedFunRowData);
//                    if (funFormList != null && !funFormList.isEmpty()) {
//                        String columnMappingDataStr = "<tr>";
//                        String columnMappingHeaderStr = "<tr>";
//                        String addTrString = "<tr>";
//                        String hiddenAttar = "";
//                        for (int i = 0; i < funFormList.size(); i++) {
//
//                            Object[] funFormColsArray = funFormList.get(i);
//                            if (funFormColsArray != null && funFormColsArray.length != 0) {
//                                String displayStyle = "";
//                                String inputAttar = "";
//                                if ("T".equalsIgnoreCase(String.valueOf(funFormColsArray[5]))) {
//                                    inputAttar = "<input id='" + funFormColsArray[4] + "' class='visionColMappingInput'"
//                                            + " type='text'"
//                                            + " value='" + ((funFormColsArray[7] != null
//                                            && !"".equalsIgnoreCase(String.valueOf(funFormColsArray[7]))
//                                            && !"null".equalsIgnoreCase(String.valueOf(funFormColsArray[7]))) ? funFormColsArray[7] : "") + "' />";
//                                } else if ("TP".equalsIgnoreCase(String.valueOf(funFormColsArray[5]))) {
//                                    inputAttar = "<input id='" + funFormColsArray[4] + "' class='visionColMappingInput'"
//                                            + " type='text'"
//                                            + " value='" + ((funFormColsArray[7] != null
//                                            && !"".equalsIgnoreCase(String.valueOf(funFormColsArray[7]))
//                                            && !"null".equalsIgnoreCase(String.valueOf(funFormColsArray[7]))) ? funFormColsArray[7] : "") + "' />"
//                                            + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
//                                            + " onclick=\"selectFunColumnValue(this,'" + funFormColsArray[8] + "')\" style=\"\" />";
//                                } else if ("P".equalsIgnoreCase(String.valueOf(funFormColsArray[5]))) {
//                                    inputAttar = "<input id='" + funFormColsArray[4] + "' readonly='readonly' class='visionColMappingInput'"
//                                            + " type='text'"
//                                            + " value='" + ((funFormColsArray[7] != null
//                                            && !"".equalsIgnoreCase(String.valueOf(funFormColsArray[7]))
//                                            && !"null".equalsIgnoreCase(String.valueOf(funFormColsArray[7]))) ? funFormColsArray[7] : "") + "' />"
//                                            + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
//                                            + " onclick=\"selectFunColumnValue(this,'" + funFormColsArray[8] + "')\" style=\"\" />";
//                                } else if ("TA".equalsIgnoreCase(String.valueOf(funFormColsArray[5]))) {
//                                    inputAttar = "<input id='" + funFormColsArray[4] + "' class='visionColMappingInput'"
//                                            + " type='text'"
//                                            + " value='" + ((funFormColsArray[7] != null
//                                            && !"".equalsIgnoreCase(String.valueOf(funFormColsArray[7]))
//                                            && !"null".equalsIgnoreCase(String.valueOf(funFormColsArray[7]))) ? funFormColsArray[7] : "") + "' />";
//                                } else if ("L".equalsIgnoreCase(String.valueOf(funFormColsArray[5]))) {
//                                    inputAttar = "<select id='" + funFormColsArray[4] + "' class='visionColMappingInput' >"
//                                            + "" + genericDataPipingDAO.getGetListOfValues(String.valueOf(funFormColsArray[8]),
//                                                    String.valueOf(funFormColsArray[7]),
//                                                    String.valueOf(funFormColsArray[4]), request)
//                                            + "</select>";
//                                } else if ("D".equalsIgnoreCase(String.valueOf(funFormColsArray[5]))) {
//
//                                } else if ("C".equalsIgnoreCase(String.valueOf(funFormColsArray[5]))) {
//                                    inputAttar = "<input id='" + funFormColsArray[4] + "' class='visionColMappingInput'"
//                                            + " type='checkbox'"
//                                            + " />";
//                                } else if ("H".equalsIgnoreCase(String.valueOf(funFormColsArray[5]))) {
//                                    displayStyle = "display:none;";
//                                    inputAttar = "<input id='" + funFormColsArray[4] + "' class='visionColMappingInput'"
//                                            + " type='hidden'"
//                                            + " value='" + ((funFormColsArray[7] != null
//                                            && !"".equalsIgnoreCase(String.valueOf(funFormColsArray[7]))
//                                            && !"null".equalsIgnoreCase(String.valueOf(funFormColsArray[7]))) ? funFormColsArray[7] : "") + "' />";
//                                }
//                                if (!"MULTI_COLUMNS".equalsIgnoreCase(String.valueOf(selectedFunRowData.get("FUN_LVL_TYPE")))) {
//                                    columnMappingStr += "<tr>"
//                                            + "<th style='" + displayStyle + "' width='15%' class=\"mappedColsTh1\" "
//                                            + "style='text-align: center'>" + funFormColsArray[3] + "</th>"
//                                            + "<td style='" + displayStyle + "'> " + inputAttar
//                                            + "</td>";
//                                    if (i == funFormList.size() - 1) {
//                                        columnMappingStr += "<tr style='display:none'>"
//                                                + "<th width='15%' class=\"mappedColsTh1\" "
//                                                + "style='text-align: center'>Level Type</th>"
//                                                + "<td width='85%'  ><input id='FUN_LVL_TYPE' class='visionColMappingInput'"
//                                                + " type='text'"
//                                                + " value='" + selectedFunRowData.get("FUN_LVL_TYPE") + "' />";
//                                        columnMappingStr += "</td>"
//                                                + "</tr>";
//                                        columnMappingStr += "<tr style='display:none'>"
//                                                + "<th width='15%' class=\"mappedColsTh1\" "
//                                                + "style='text-align: center'>Level Type</th>"
//                                                + "<td width='85%'  ><input id='DM_FUN_CUST_COL1' class='visionColMappingInput'"
//                                                + " type='text'"
//                                                + " value='" + selectedFunRowData.get("DM_FUN_CUST_COL1") + "' />";
//                                        columnMappingStr += "</td>"
//                                                + "</tr>";
//                                        columnMappingStr += "<tr style='display:none'>"
//                                                + "<th width='15%' class=\"mappedColsTh1\" "
//                                                + "style='text-align: center'>Level Type</th>"
//                                                + "<td width='85%'  ><input id='DM_FUN_CUST_COL2' class='visionColMappingInput'"
//                                                + " type='text'"
//                                                + " value='" + selectedFunRowData.get("DM_FUN_CUST_COL2") + "' />";
//                                        columnMappingStr += "</td>"
//                                                + "</tr>";
//                                    }
//                                } else {
//                                    if (i == 0) {
//                                        addTrString = "<tr>";
//                                        columnIconStr += "<th><img src=\"images/Add icon.svg\""
//                                                + " id=\"addMultiPleFunForm\" onclick=\"addNewTableDataRow('funFormETLTableTr','funFormETLTable',this)\""
//                                                + " title=\"Add new condition\""
//                                                + " style=\"width:15px;height: 15px;cursor:pointer;\">"
//                                                + "</th>";
//                                        columnMappingHeaderStr += "<th "
//                                                + "style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;" + displayStyle + "' >"
//                                                + "</th>";
//                                        columnMappingDataStr += "<td width='2%'>"
//                                                + "<img src=\"images/Delete_Red_Icon.svg\" "
//                                                + "onclick=\"deleteSelectedRow(this)\" class=\"visionColMappingImg\" "
//                                                + "title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"></td>";
//                                        addTrString += "<td width='2%'>"
//                                                + "<img src=\"images/Delete_Red_Icon.svg\" "
//                                                + "onclick=\"deleteSelectedRow(this)\" class=\"visionColMappingImg\" "
//                                                + "title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"></td>";
//                                    }
//                                    columnMappingHeaderStr += "<th "
//                                            + "style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;" + displayStyle + "' >"
//                                            + "" + funFormColsArray[3] + "</th>";
//                                    columnMappingDataStr += "<td style='" + displayStyle + "'>" + inputAttar
//                                            + "</td>";
//                                    addTrString += "<td style='" + displayStyle + "'>" + inputAttar
//                                            + "</td>";
//                                    if (i == funFormList.size() - 1) {
//                                        columnMappingHeaderStr += "<th style='display:none' class=\"mappedColsTh1\" "
//                                                + "style='text-align: center'>Level Type</th>";
//                                        columnMappingDataStr += "<td style='display:none'  >"
//                                                + "<input id='FUN_LVL_TYPE' class='visionColMappingInput'"
//                                                + " type='text'"
//                                                + " value='" + selectedFunRowData.get("FUN_LVL_TYPE") + "' /></td>";
//
//                                        addTrString += "<td style='display:none'  >"
//                                                + "<input id='FUN_LVL_TYPE' class='visionColMappingInput'"
//                                                + " type='text'"
//                                                + " value='" + selectedFunRowData.get("FUN_LVL_TYPE") + "' /></td>";
//
//                                        columnMappingHeaderStr += "<th style='display:none' class=\"mappedColsTh1\" "
//                                                + "style='text-align: center'>Level Type</th>";
//                                        columnMappingDataStr += ""
//                                                + "<td style='display:none' ><input id='DM_FUN_CUST_COL1' class='visionColMappingInput'"
//                                                + " type='text'"
//                                                + " value='" + selectedFunRowData.get("DM_FUN_CUST_COL1") + "' /></td>";
//                                        addTrString += ""
//                                                + "<td style='display:none' ><input id='DM_FUN_CUST_COL1' class='visionColMappingInput'"
//                                                + " type='text'"
//                                                + " value='" + selectedFunRowData.get("DM_FUN_CUST_COL1") + "' /></td>";
//
//                                        columnMappingHeaderStr += "<th style='display:none' class=\"mappedColsTh1\" "
//                                                + "style='text-align: center'>Level Type</th>";
//                                        columnMappingDataStr += ""
//                                                + "<td style='display:none'  ><input id='DM_FUN_CUST_COL2' class='visionColMappingInput'"
//                                                + " type='text'"
//                                                + " value='" + selectedFunRowData.get("DM_FUN_CUST_COL2") + "' /></td>";
//                                        addTrString += ""
//                                                + "<td style='display:none'  ><input id='DM_FUN_CUST_COL2' class='visionColMappingInput'"
//                                                + " type='text'"
//                                                + " value='" + selectedFunRowData.get("DM_FUN_CUST_COL2") + "' /></td>";
//                                        addTrString += "</tr>";
//                                        columnMappingHeaderStr += "</tr>";
//                                        columnMappingDataStr += "</tr>";
//                                        resultObj.put("addTrString", addTrString);
//                                        columnMappingStr += "<thead>" + columnMappingHeaderStr + "</tr></thead><tbody>" + columnMappingDataStr + "</tbody>";
//                                    }
//                                }
//
//                            }
//
//                        }
//
//                    }
//                    columnIconStr += ""
//                            + "<th><img src=\"images/SQL ICON-01.svg\""
//                            + " id=\"viewFunQuery\" "
//                            + " title=\"Click here to view the query\""
//                            + " style=\"width:15px;height: 15px;cursor:pointer;\">"
//                            + "</th>"
//                            + "</tr></table></div>";
//                    columnMappingStr
//                            += "</table>"
//                            + "</div>";
//                    resultObj.put("funFormStr", columnIconStr + columnMappingStr);
//                    resultObj.put("messageFlag", true);//
//                } else {
//                    resultObj.put("message", "Selected Row Data Empty");
//                    resultObj.put("messageFlag", false);//
//                }
//            } else {
//                resultObj.put("message", "Selected Row Data Empty");
//                resultObj.put("messageFlag", false);//
//            }
//        } catch (Exception e) {
//            resultObj.put("message", e.getMessage());
//            resultObj.put("messageFlag", false);
//        }
//        return resultObj.toJSONString();
//    }
	public JSONArray getLookupTables(HttpServletRequest request) {
		JSONArray dataArray = new JSONArray();
		Connection connection = null;
		try {
			String expandedRecordStr = request.getParameter("expandedRecord");
			String columnType = request.getParameter("columnType");

			String connCustCol2 = request.getParameter("connCustCol2"); // ravi predefined
			if (!(expandedRecordStr != null && !"".equalsIgnoreCase(expandedRecordStr)
					&& !"null".equalsIgnoreCase(expandedRecordStr))) {
				List savedConnections = new ArrayList();
				if (connCustCol2 != null && "ERP".equalsIgnoreCase(connCustCol2)) { // ravi predefined
					savedConnections = dataMigrationDAO.getSavedErpConnections(request);
				} else {
					savedConnections = dataMigrationDAO.getSavedConnections(request);
				}

				if (savedConnections != null && !savedConnections.isEmpty()) {
					for (int i = 0; i < savedConnections.size(); i++) {
						Object[] dataObjArray = (Object[]) savedConnections.get(i);
						if (dataObjArray != null && dataObjArray.length != 0) {
							JSONObject dataObj = new JSONObject();
							dataObj.put("CONNECTION_TYPE", dataObjArray[6]);
							dataObj.put("LEVEL_TYPE", "SCHEMA");
							dataObj.put("TABLE_NAME", dataObjArray[0]);
							dataObj.put("icon", "images/DM_Database.png");
							dataObj.put("ID", AuditIdGenerator.genRandom32Hex());
							JSONObject connObj = new JSONObject();
							connObj.put("CONNECTION_NAME", dataObjArray[0]);
							connObj.put("HOST_NAME", dataObjArray[1]);
							connObj.put("CONN_USER_NAME", dataObjArray[3]);
							connObj.put("CONN_PASSWORD", dataObjArray[4]);
							connObj.put("CONN_PORT", dataObjArray[2]);
							connObj.put("CONN_DB_NAME", dataObjArray[5]);
							connObj.put("CONN_CUST_COL1", dataObjArray[6]);
							connObj.put("CONN_CUST_COL2", (connCustCol2 != null) ? connCustCol2 : "DB"); // ravi
																											// predefined
							connObj.put("CONN_CUST_COL3", "");
							dataObj.put("CONNECTION_OBJ", connObj.toJSONString());
							dataArray.add(dataObj);
						}

					}
				}
			} else {
				JSONObject expandedRecord = (JSONObject) JSONValue.parse(expandedRecordStr);
				if (expandedRecord != null && !expandedRecord.isEmpty()) {
					String connObjStr = (String) expandedRecord.get("CONNECTION_OBJ");
					if (connObjStr != null && !"".equalsIgnoreCase(connObjStr)
							&& !"null".equalsIgnoreCase(connObjStr)) {
						JSONObject connObj = (JSONObject) JSONValue.parse(connObjStr);
						if (connObj != null && !connObj.isEmpty()) {
							Object connectionObj = getConnection(connObj);
							if (connectionObj instanceof Connection) {
								connection = (Connection) connectionObj;
								if (!"ALL_TABLE".equalsIgnoreCase(columnType)
										&& "TABLE".equalsIgnoreCase(String.valueOf(expandedRecord.get("LEVEL_TYPE")))) {
									List<Object[]> columnsArray = getTreeDMTableColumnsOpt(connection, request, connObj,
											String.valueOf(expandedRecord.get("TABLE_NAME")));
									if (columnsArray != null && !columnsArray.isEmpty()) {
										for (int i = 0; i < columnsArray.size(); i++) {
											Object[] columnsArrayObj = columnsArray.get(i);
											JSONObject dataObj = new JSONObject();
											dataObj.put("LEVEL_TYPE", "COLUMN");
											dataObj.put("CONNECTION_NAME", columnsArrayObj[0]);
											dataObj.put("CONNECTION_TYPE", connObj.get("CONN_CUST_COL1"));
											dataObj.put("TABLE_NAME", columnsArrayObj[1]);
											dataObj.put("CONNECTION_OBJ", connObj.toJSONString());
											dataObj.put("ID", AuditIdGenerator.genRandom32Hex());
											dataArray.add(dataObj);
										}
									}
								} else if ("SCHEMA"
										.equalsIgnoreCase(String.valueOf(expandedRecord.get("LEVEL_TYPE")))) {
									JSONArray tablesArray = getTablesListBySchema(connObj, connection);
									if (tablesArray != null && !tablesArray.isEmpty()) {
										for (int i = 0; i < tablesArray.size(); i++) {
											JSONObject dataObj = new JSONObject();
											dataObj.put("LEVEL_TYPE", "TABLE");
											dataObj.put("CONNECTION_NAME", connObj.get("CONNECTION_NAME"));
											dataObj.put("CONNECTION_TYPE", connObj.get("CONN_CUST_COL1"));
											dataObj.put("TABLE_NAME", tablesArray.get(i));
											dataObj.put("CONNECTION_OBJ", connObj.toJSONString());
											dataObj.put("icon", "images/GridDB.png");
											dataObj.put("ID", AuditIdGenerator.genRandom32Hex());
											dataArray.add(dataObj);
										}
									}
								}

							}
						}
					}
				}
			}
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
		return dataArray;
	}

	public JSONArray getTablesListBySchema(JSONObject dbObj, Connection connection) {
		JSONArray totalTables = new JSONArray();
		try {
//            dbFromObj.put("selectedItemLabel", fromConnectObj.get("CONN_CUST_COL1"));
//            dbFromObj.put("hostName", fromConnectObj.get("HOST_NAME"));
//            dbFromObj.put("port", fromConnectObj.get("CONN_PORT"));
//            dbFromObj.put("userName", fromConnectObj.get("CONN_USER_NAME"));
//            dbFromObj.put("password", fromConnectObj.get("CONN_PASSWORD"));
//            dbFromObj.put("serviceName", fromConnectObj.get("CONN_DB_NAME"));
			String initParamClassName = "com.pilog.mdm.DAO.V10DataMigrationAccessDAO";// getOracleDatabaseTables
			String initParamMethodName = "get" + dbObj.get("CONN_CUST_COL1") + "DatabaseTables";
			System.out.println(
					" initParamClassName:" + initParamClassName + "initParamMethodName:" + initParamMethodName);
			Class clazz = Class.forName(initParamClassName);
			Class<?>[] paramTypes = { Connection.class, String.class };
			Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
			Object targetObj = new PilogUtilities().createObjectByClass(clazz);
			totalTables = (JSONArray) method.invoke(targetObj, connection, String.valueOf(dbObj.get("CONN_DB_NAME")));

		} catch (Exception e) {

			e.printStackTrace();
		}
		return totalTables;
	}

	public JSONArray getSelectedLookupTableColumns(HttpServletRequest request) {
		JSONArray totalColumns = new JSONArray();
		Connection connection = null;
		try {
			String connObjStr = request.getParameter("connObj");
			String lookupTableName = request.getParameter("lookupTableName");
			if (connObjStr != null && !"".equalsIgnoreCase(connObjStr) && !"null".equalsIgnoreCase(connObjStr)) {
				JSONObject connObj = (JSONObject) JSONValue.parse(connObjStr);
				if (connObj != null && !connObj.isEmpty()) {
					Object connectionObj = getConnection(connObj);
					if (connectionObj instanceof Connection) {
						connection = (Connection) connectionObj;
						List<Object[]> columnList = getTreeDMTableColumnsOpt(connection, request, connObj,
								lookupTableName);
						if (columnList != null && !columnList.isEmpty()) {
							JSONObject tableObj = new JSONObject();
							tableObj.put("id", lookupTableName);
							tableObj.put("text", lookupTableName);
							tableObj.put("value", lookupTableName);
							tableObj.put("icon", "images/GridDB.png");
							totalColumns.add(tableObj);
							for (int j = 0; j < columnList.size(); j++) {
								Object[] masterColsArray = columnList.get(j);
								if (masterColsArray != null && masterColsArray.length != 0) {
									JSONObject columnObj = new JSONObject();
									columnObj.put("id", masterColsArray[0] + ":" + masterColsArray[1]);
									columnObj.put("text", masterColsArray[1]);
									columnObj.put("value", masterColsArray[0] + ":" + masterColsArray[1]);
									columnObj.put("parentid", masterColsArray[0]);
									totalColumns.add(columnObj);
								}

							}
						}
					}
				}
			}
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
		return totalColumns;
	}

	public JSONArray getTreeErpConnectionDetails(HttpServletRequest request) {
		JCO.Client connection = null;
		JSONArray treeDataObjArray = new JSONArray();
		try {
			JSONObject dbObj = new JSONObject();
			JSONObject connectionObj = new JSONObject();
			String connObj = request.getParameter("connectionObj");
			if (connObj != null && !"".equalsIgnoreCase(connObj) && !"null".equalsIgnoreCase(connObj)) {
				connectionObj = (JSONObject) JSONValue.parse(connObj);
			}
			String levelStr = request.getParameter("level");
			String tableName = request.getParameter("parentkey");
			dbObj.put("selectedItemLabel", connectionObj.get("CONN_CUST_COL1"));
			dbObj.put("ClientId", connectionObj.get("CONN_PORT"));
			dbObj.put("hostName", connectionObj.get("HOST_NAME"));
			dbObj.put("userName", connectionObj.get("CONN_USER_NAME"));
			dbObj.put("password", connectionObj.get("CONN_PASSWORD"));
			dbObj.put("LanguageId", "EN");
			dbObj.put("ERPSystemId", connectionObj.get("CONN_DB_NAME"));
			dbObj.put("group", connectionObj.get("GROUP"));
			if (dbObj != null && !dbObj.isEmpty()) {
				System.out.println("selectedItemLabel:::::::::::::::" + connectionObj.get("CONN_CUST_COL1"));
				String filterTableVal = request.getParameter("filterValue");
				Object returnObj = dataMigrationService.getErpConnection(dbObj);
				if (returnObj != null && returnObj instanceof JCO.Client) {
					connection = (JCO.Client) returnObj;
					if (levelStr != null && !"".equalsIgnoreCase(levelStr) && "5".equalsIgnoreCase(levelStr)) {
						treeDataObjArray = genericDataPipingDAO.getTreeSapListOfTableColumns(request, connection,
								tableName);
					} else {
						treeDataObjArray = genericDataPipingDAO.getTreeSapListOfTable(request, connection,
								filterTableVal);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return treeDataObjArray;
	}

	public JSONArray getFromTableColumns(HttpServletRequest request, HttpServletResponse response) {
		JSONArray fromTableColsArray = new JSONArray();
		Connection fromConnection = null;
		try {
			String fromOpArrayStr = request.getParameter("fromOperators");
			if (fromOpArrayStr != null && !"".equalsIgnoreCase(fromOpArrayStr)
					&& !"null".equalsIgnoreCase(fromOpArrayStr)) {

				JSONArray fromOpArray = (JSONArray) JSONValue.parse(fromOpArrayStr);
				if (fromOpArray != null && !fromOpArray.isEmpty()) {
					List<Map> fromOperatorList = new ArrayList<>();
					fromOperatorList.addAll(fromOpArray);
					boolean isSameDataBase = fromOperatorList.stream()
							.filter(fromOpMap -> (fromOpMap.containsKey("CONNECTION_NAME")
									&& fromOpMap.containsKey("CONN_DB_NAME")
									&& !"file".equalsIgnoreCase(String.valueOf(fromOpMap.get("dragType")))))
							.map(fromOpMap -> (fromOpMap.get("CONNECTION_NAME") + ":::"
									+ fromOpMap.get("CONN_DB_NAME")))
							.distinct().count() == 1;
					boolean iscontainsFile = fromOperatorList.stream()
							.anyMatch(fromOpMap -> "file".equalsIgnoreCase(String.valueOf(fromOpMap.get("dragType"))));
//                    boolean isSameDataBase = fromOperatorList.stream()
//                            .filter(fromOpMap -> (fromOpMap.containsKey("CONNECTION_NAME") && fromOpMap.containsKey("CONN_DB_NAME")))
//                            .map(fromOpMap -> (fromOpMap.get("CONNECTION_NAME") + ":::" + fromOpMap.get("CONN_DB_NAME")))
//                            .distinct().count() == 1;
					if (isSameDataBase && !iscontainsFile) {
						List<Object[]> columnsList = new ArrayList<>();
						List<Object[]> dataTypeList = new ArrayList<>();
						JSONObject fromOperatorMap = (JSONObject) fromOperatorList.get(0);
						String iconType = (String) fromOperatorList.get(0).get("iconType");
						if (iconType != null && "QUERY".equalsIgnoreCase(iconType)) {

							JSONObject fromConnectObj = (JSONObject) fromOperatorMap.get("connObj");

							List fromColumnsList = new ArrayList();
							List fromAliasColumnsList = new ArrayList();
							List toColumnsList = new ArrayList();

							JSONObject connObj = (JSONObject) fromOperatorMap.get("queryConnObj");

							JSONObject trfmRules = (JSONObject) fromOperatorMap.get("trfmRules");
							String query = (String) trfmRules.get("queryData");
							query = query.toUpperCase();

							int toIndex = query.indexOf("FROM");
							String columnsStr = query.substring(6, toIndex);
							String restOfQuery = query.substring(toIndex);
							String columnsListStr = "";

							Pattern pattern = Pattern.compile("[^ _.a-zA-Z0-9]", Pattern.CASE_INSENSITIVE);
							String[] colsArray = columnsStr.split(",");
							for (int i = 0; i < colsArray.length; i++) {
								String column = colsArray[i];
								String fromColumn = column;
								if (column != null && column.contains(" AS ")) {
									String fromColumn0 = column.split(" AS ")[0];
									String fromColumn1 = column.split(" AS ")[1];
									fromColumn0 = fromColumn0.trim();
									if ("''".equalsIgnoreCase(fromColumn0)) {
										fromColumn0 = "'                                        '";
									}
									fromColumnsList.add(fromColumn0 + " AS " + fromColumn1);
								} else if (pattern.matcher(column).find()) {
									if ("''".equalsIgnoreCase(column)) {
										column = "'                                        ' AS " + " COLUMN_" + i;
									} else {
										column = column + " AS " + " COLUMN_" + i;
									}

									fromColumnsList.add(column);
								} else {
									fromColumnsList.add(column);
								}

								toColumnsList.add(column);
							}

							for (int i = 0; i < fromColumnsList.size(); i++) {
								if (i < (fromColumnsList.size() - 1)) {
									columnsListStr += fromColumnsList.get(i) + ", ";
								} else {
									columnsListStr += fromColumnsList.get(i);
								}

							}
							fromAliasColumnsList.addAll(toColumnsList);

							query = "SELECT " + columnsListStr + " " + restOfQuery;
							System.out.println("query :: " + query);
							String selectQuery = "SELECT * FROM (" + query + ") WHERE 1=2";
							fromConnection = (Connection) getConnection(connObj);
							columnsList = getColumnsListObjWithQuery(request, selectQuery, fromConnection, connObj);

							if (columnsList != null && !columnsList.isEmpty()) {

								try {
									dataTypeList = genericDataPipingDAO.getTargetDataType(
											String.valueOf(connObj.get("CONN_CUST_COL1")).toUpperCase());
								} catch (Exception e) {
								}
								String[] tableNamesArray = "Query Columns".split(",");
								for (int i = 0; i < tableNamesArray.length; i++) {
									JSONObject dataObj = new JSONObject();
									dataObj.put("LEVEL_TYPE", "TABLE");
									dataObj.put("CONNECTION_NAME", connObj.get("CONNECTION_NAME"));
									dataObj.put("CONNECTION_TYPE", connObj.get("CONN_CUST_COL1"));
									dataObj.put("TABLE_NAME", tableNamesArray[i]);
									dataObj.put("CONNECTION_OBJ", connObj.toJSONString());
									dataObj.put("icon", "images/GridDB.png");
									dataObj.put("ID", AuditIdGenerator.genRandom32Hex());
									fromTableColsArray.add(dataObj);
								}
								for (Object[] columnsObjArray : columnsList) {
									JSONObject dataObj = new JSONObject();
									dataObj.put("LEVEL_TYPE", "COLUMN");
									dataObj.put("CONNECTION_NAME", columnsObjArray[1]);
									dataObj.put("CONNECTION_TYPE", connObj.get("CONN_CUST_COL1"));
									dataObj.put("TABLE_NAME", columnsObjArray[2]);
									dataObj.put("DATA_TYPE", columnsObjArray[3]);
									dataObj.put("COLUMN_TYPE", columnsObjArray[8]);
									dataObj.put("DATA_LENGTH", columnsObjArray[4]);
									dataObj.put("CONNECTION_OBJ", connObj.toJSONString());
									if (("P".equalsIgnoreCase(String.valueOf(columnsObjArray[7]))
											|| "PRI".equalsIgnoreCase(String.valueOf(columnsObjArray[7]))
											|| "PRIMARY KEY".equalsIgnoreCase(String.valueOf(columnsObjArray[7])))
											&& "ENABLED".equalsIgnoreCase(String.valueOf(columnsObjArray[6]))) {
										dataObj.put("icon", "images/lock.png");
									}
									if (dataTypeList != null && !dataTypeList.isEmpty()) {
										JSONObject dataTypeObj = new JSONObject();
										List<Object[]> matchedDataTypeList = dataTypeList.stream()
												.filter(dataTypeArray -> (dataTypeArray[1] != null
														&& !"".equalsIgnoreCase(String.valueOf(dataTypeArray[1]))
														&& !"null".equalsIgnoreCase(String.valueOf(dataTypeArray[1]))
														&& dataTypeArray[6] != null
														&& !"".equalsIgnoreCase(String.valueOf(dataTypeArray[6]))
														&& !"null".equalsIgnoreCase(String.valueOf(dataTypeArray[6]))
														&& String.valueOf(dataTypeArray[6])
																.equalsIgnoreCase(String.valueOf(columnsObjArray[3]))))
												.collect(Collectors.toList());
										if (matchedDataTypeList != null && !matchedDataTypeList.isEmpty()) {
											for (int i = 0; i < matchedDataTypeList.size(); i++) {
												Object[] matchedDataTypeArray = matchedDataTypeList.get(i);
												if (matchedDataTypeArray != null && matchedDataTypeArray.length != 0) {
													String dataType = "" + matchedDataTypeArray[1];
													String length = "";
//                                                    dataTypeObj.put(connectionObj, connectionObj)
													if ("Y".equalsIgnoreCase(String.valueOf(matchedDataTypeArray[3]))
															&& columnsObjArray[4] != null
															&& !"".equalsIgnoreCase(String.valueOf(columnsObjArray[4]))
															&& !"null".equalsIgnoreCase(
																	String.valueOf(columnsObjArray[4]))) {
														length += "" + new PilogUtilities().convertIntoInteger(
																new BigInteger("" + columnsObjArray[4])) + " ";
														if (matchedDataTypeArray[4] != null
																&& !"".equalsIgnoreCase(
																		String.valueOf(matchedDataTypeArray[4]))
																&& !"null".equalsIgnoreCase(
																		String.valueOf(matchedDataTypeArray[4]))) {
															length += "" + matchedDataTypeArray[4] + " ";
														}
													}

													if (length != null && !"".equalsIgnoreCase(length)
															&& !"null".equalsIgnoreCase(length)) {
														dataType += "(" + length + ") ";
													}
													if (dataTypeObj != null && !dataTypeObj.isEmpty()) {
														if (!dataTypeObj.containsKey(matchedDataTypeArray[0])) {
															dataTypeObj.put(matchedDataTypeArray[0], dataType);
														}

													} else {
														dataTypeObj.put(matchedDataTypeArray[0], dataType);
													}
												}

											}

										}
										dataObj.put("TO_COLUMN_TYPE", dataTypeObj.toJSONString());
									}
									dataObj.put("ID", AuditIdGenerator.genRandom32Hex());
									fromTableColsArray.add(dataObj);
								}
							}

						} else {
							JSONObject connObj = (JSONObject) fromOperatorList.get(0).get("connObj");
							if (connObj != null && !connObj.isEmpty()) {

								Object connectionObj = getConnection(connObj);
								String tableNames = fromOperatorList.stream()
										.map(fromOpMap -> (((String) fromOpMap.get("tableName"))
												.substring(((String) fromOpMap.get("tableName")).lastIndexOf(".") + 1)))
										.collect(Collectors.joining(","));
								if (connectionObj instanceof Connection) {
									fromConnection = (Connection) connectionObj;
									columnsList = getTableColumnsOpt(fromConnection, request, connObj, tableNames);
								} else if (connectionObj instanceof JCO.Client) {
									columnsList = dataMigrationAccessDAO.getSAPTableColumnsWithType(request,
											(JCO.Client) connectionObj, tableNames);
								} // CONN_CUST_COL1

								if (columnsList != null && !columnsList.isEmpty()) {

									try {
										dataTypeList = genericDataPipingDAO.getTargetDataType(
												String.valueOf(connObj.get("CONN_CUST_COL1")).toUpperCase());
									} catch (Exception e) {
									}
									String[] tableNamesArray = tableNames.split(",");
									for (int i = 0; i < tableNamesArray.length; i++) {
										JSONObject dataObj = new JSONObject();
										dataObj.put("LEVEL_TYPE", "TABLE");
										dataObj.put("CONNECTION_NAME", connObj.get("CONNECTION_NAME"));
										dataObj.put("CONNECTION_TYPE", connObj.get("CONN_CUST_COL1"));
										dataObj.put("TABLE_NAME", tableNamesArray[i]);
										dataObj.put("CONNECTION_OBJ", connObj.toJSONString());
										dataObj.put("icon", "images/GridDB.png");
										dataObj.put("ID", AuditIdGenerator.genRandom32Hex());
										fromTableColsArray.add(dataObj);
									}
									for (Object[] columnsObjArray : columnsList) {
										JSONObject dataObj = new JSONObject();
										dataObj.put("LEVEL_TYPE", "COLUMN");
										dataObj.put("CONNECTION_NAME", columnsObjArray[1]);
										dataObj.put("CONNECTION_TYPE", connObj.get("CONN_CUST_COL1"));
										dataObj.put("TABLE_NAME", columnsObjArray[2]);
										dataObj.put("DATA_TYPE", columnsObjArray[3]);
										dataObj.put("COLUMN_TYPE", columnsObjArray[8]);
										dataObj.put("DATA_LENGTH", columnsObjArray[4]);
										dataObj.put("CONNECTION_OBJ", connObj.toJSONString());
										if (("P".equalsIgnoreCase(String.valueOf(columnsObjArray[7]))
												|| "PRI".equalsIgnoreCase(String.valueOf(columnsObjArray[7]))
												|| "PRIMARY KEY".equalsIgnoreCase(String.valueOf(columnsObjArray[7])))
												&& "ENABLED".equalsIgnoreCase(String.valueOf(columnsObjArray[6]))) {
											dataObj.put("icon", "images/lock.png");
										}
										if (dataTypeList != null && !dataTypeList.isEmpty()) {
											JSONObject dataTypeObj = new JSONObject();
											List<Object[]> matchedDataTypeList = dataTypeList.stream()
													.filter(dataTypeArray -> (dataTypeArray[1] != null
															&& !"".equalsIgnoreCase(String.valueOf(dataTypeArray[1]))
															&& !"null"
																	.equalsIgnoreCase(String.valueOf(dataTypeArray[1]))
															&& dataTypeArray[6] != null
															&& !"".equalsIgnoreCase(String.valueOf(dataTypeArray[6]))
															&& !"null"
																	.equalsIgnoreCase(String.valueOf(dataTypeArray[6]))
															&& String.valueOf(dataTypeArray[6]).equalsIgnoreCase(
																	String.valueOf(columnsObjArray[3]))))
													.collect(Collectors.toList());
											if (matchedDataTypeList != null && !matchedDataTypeList.isEmpty()) {
												for (int i = 0; i < matchedDataTypeList.size(); i++) {
													Object[] matchedDataTypeArray = matchedDataTypeList.get(i);
													if (matchedDataTypeArray != null
															&& matchedDataTypeArray.length != 0) {
														String dataType = "" + matchedDataTypeArray[1];
														String length = "";
//                                                    dataTypeObj.put(connectionObj, connectionObj)
														if ("Y".equalsIgnoreCase(
																String.valueOf(matchedDataTypeArray[3]))
																&& columnsObjArray[4] != null
																&& !"".equalsIgnoreCase(
																		String.valueOf(columnsObjArray[4]))
																&& !"null".equalsIgnoreCase(
																		String.valueOf(columnsObjArray[4]))) {
															length += ""
																	+ new PilogUtilities().convertIntoInteger(
																			new BigInteger("" + columnsObjArray[4]))
																	+ " ";
															if (matchedDataTypeArray[4] != null
																	&& !"".equalsIgnoreCase(
																			String.valueOf(matchedDataTypeArray[4]))
																	&& !"null".equalsIgnoreCase(
																			String.valueOf(matchedDataTypeArray[4]))) {
																length += "" + matchedDataTypeArray[4] + " ";
															}
														}

														if (length != null && !"".equalsIgnoreCase(length)
																&& !"null".equalsIgnoreCase(length)) {
															dataType += "(" + length + ") ";
														}
														if (dataTypeObj != null && !dataTypeObj.isEmpty()) {
															if (!dataTypeObj.containsKey(matchedDataTypeArray[0])) {
																dataTypeObj.put(matchedDataTypeArray[0], dataType);
															}

														} else {
															dataTypeObj.put(matchedDataTypeArray[0], dataType);
														}
													}

												}

											}
											dataObj.put("TO_COLUMN_TYPE", dataTypeObj.toJSONString());
										}
										dataObj.put("ID", AuditIdGenerator.genRandom32Hex());
										fromTableColsArray.add(dataObj);
									}
								}
							}
						}

					} else {
						for (int k = 0; k < fromOperatorList.size(); k++) {
							Map fromOperator = fromOperatorList.get(k);
							if (fromOperator != null && !fromOperator.isEmpty()) {
								if ("file".equalsIgnoreCase(String.valueOf(fromOperator.get("dragType")))) {
									JSONObject fromConnectObj = (JSONObject) fromOperator.get("connObj");
									if (fromConnectObj != null && !fromConnectObj.isEmpty()) {
										List<String> headers = dataMigrationService.getHeadersOfImportedFile(request,
												response, (String) fromConnectObj.get("filePath"));
										// ravi headers trim start
										headers = fileHeaderValidations(headers);
//                                        List dataTypeList = getHeaderDataTypesOfImportedFile(request, (String) fromConnectObj.get("filePath"));
//                                        List dataTypeList = (List) headers.stream().map(e -> "VARCHAR2(4000)").collect(Collectors.toList());
										JSONObject dataTypesObj = (JSONObject) fromOperator.get("dataTypesObj");
										List dataTypeList = new ArrayList();
										if (dataTypesObj != null) {
											for (Object col : headers) {
												JSONObject typeObj = (JSONObject) dataTypesObj.get(col);
												String datatype = (String) typeObj.get("datatype");
												String columnsize = String.valueOf(typeObj.get("columnsize"));
												if (columnsize != null && !"".equalsIgnoreCase(columnsize)) {
													datatype = datatype + "(" + columnsize + ")";
												}
												dataTypeList.add(datatype);
											}
										} else {
//                                            dataTypeList = (List) headers.stream().map(e -> "VARCHAR2(4000)").collect(Collectors.toList());
											dataTypeList = getHeaderDataTypesOfImportedFile(request,
													(String) fromConnectObj.get("filePath"));
										}

										// ravi headers trim end
										if (headers != null && !headers.isEmpty()) {
											if (fromConnectObj.get("fileName") != null
													&& !"".equalsIgnoreCase(
															String.valueOf(fromConnectObj.get("fileName")))
													&& !"null".equalsIgnoreCase(
															String.valueOf(fromConnectObj.get("fileName")))) {
												JSONObject dataObj = new JSONObject();
												dataObj.put("LEVEL_TYPE", "TABLE");
												dataObj.put("CONNECTION_NAME", fromConnectObj.get("filePath"));
												dataObj.put("CONNECTION_TYPE", fromConnectObj.get("filePath"));
												dataObj.put("TABLE_NAME", fromConnectObj.get("fileName"));
												dataObj.put("CONNECTION_OBJ", fromConnectObj.toJSONString());
												dataObj.put("icon", fromConnectObj.get("imageIcon"));
												dataObj.put("ID", AuditIdGenerator.genRandom32Hex());
												fromTableColsArray.add(dataObj);
												for (int j = 0; j < headers.size(); j++) {
													dataObj = new JSONObject();
													String headerName = headers.get(j);
													dataObj.put("LEVEL_TYPE", "COLUMN");
													dataObj.put("CONNECTION_NAME", fromConnectObj.get("fileName"));
													dataObj.put("CONNECTION_TYPE", fromConnectObj.get("filePath"));
													dataObj.put("TABLE_NAME", headerName);
													dataObj.put("CONNECTION_OBJ", fromConnectObj.toJSONString());
//                                                    dataObj.put("COLUMN_TYPE", "VARCHAR2(4000 CHAR)"); // PKH DEFAULT DATA TYPE
													dataObj.put("COLUMN_TYPE", dataTypeList.get(j)); // PKH DEFAULT DATA
																										// TYPE
													dataObj.put("TO_COLUMN_TYPE",
															"{\"SAP\":\"CUKY\",\"MYSQL\":\"TEXT(64 )\",\"MSSQL\":\"VARCHAR(MAX)\"}"); // PKH
																																		// DEFAULT
																																		// DATA
																																		// TYPE
																																		// dataObj.put("COLUMN_TYPE",
																																		// "VARCHAR2(4000
																																		// CHAR)");
																																		// //
																																		// PKH
																																		// DEFAULT
																																		// DATA
																																		// TYPE

//                                                    dataObj.put("icon", fromConnectObj.get("imageIcon"));
													dataObj.put("ID", AuditIdGenerator.genRandom32Hex());
													fromTableColsArray.add(dataObj);

												}
											}
										}
									}
								} else if ("API".equalsIgnoreCase(String.valueOf(fromOperator.get("iconType")))) {
									JSONObject fromConnectObj = (JSONObject) fromOperator.get("connObj");
									List<String> headers = (List<String>) fromOperator.get("simpleColumnsList");
									if (headers != null && !headers.isEmpty()) {
										JSONObject dataTypesObj = (JSONObject) fromOperator.get("dataTypesObj");
										List dataTypeList = new ArrayList();
										if (dataTypesObj != null) {
											for (Object col : headers) {
												JSONObject typeObj = (JSONObject) dataTypesObj.get(col);
												String datatype = (String) typeObj.get("datatype");
												String columnsize = String.valueOf(typeObj.get("columnsize"));
												if (columnsize != null && !"".equalsIgnoreCase(columnsize)) {
													datatype = datatype + "(" + columnsize + ")";
												}
												dataTypeList.add(datatype);
											}
										} else {
											dataTypeList = (List) headers.stream().map(e -> "VARCHAR2(4000)")
													.collect(Collectors.toList());
										}

										// ravi headers trim end
										JSONObject dataObj = new JSONObject();
										dataObj.put("LEVEL_TYPE", "TABLE");
										dataObj.put("CONNECTION_NAME", fromConnectObj.get("CONN_NAME"));
										dataObj.put("CONNECTION_TYPE", fromConnectObj.get("CONN_CUST_COL1"));
										dataObj.put("TABLE_NAME", "API_OUTPUT_" + fromOperator.get("operatorId"));
										dataObj.put("CONNECTION_OBJ", fromConnectObj.toJSONString());
										dataObj.put("icon", fromConnectObj.get("imageIcon"));
										dataObj.put("ID", AuditIdGenerator.genRandom32Hex());
										fromTableColsArray.add(dataObj);
										for (int j = 0; j < headers.size(); j++) {
											dataObj = new JSONObject();
											String headerName = headers.get(j);
											dataObj.put("LEVEL_TYPE", "COLUMN");
											dataObj.put("CONNECTION_NAME", fromConnectObj.get("CONN_NAME"));
											dataObj.put("CONNECTION_TYPE", fromConnectObj.get("CONN_CUST_COL1"));
											dataObj.put("TABLE_NAME", headerName);
											dataObj.put("CONNECTION_OBJ", fromConnectObj.toJSONString());
//                                                    dataObj.put("COLUMN_TYPE", "VARCHAR2(4000 CHAR)"); // PKH DEFAULT DATA TYPE
											dataObj.put("COLUMN_TYPE", dataTypeList.get(j)); // PKH DEFAULT DATA TYPE
											dataObj.put("TO_COLUMN_TYPE",
													"{\"SAP\":\"CUKY\",\"MYSQL\":\"TEXT(64 )\",\"MSSQL\":\"VARCHAR(MAX)\"}"); // PKH
																																// DEFAULT
																																// DATA
																																// TYPE
																																// dataObj.put("COLUMN_TYPE",
																																// "VARCHAR2(4000
																																// CHAR)");
																																// //
																																// PKH
																																// DEFAULT
																																// DATA
																																// TYPE

//                                                    dataObj.put("icon", fromConnectObj.get("imageIcon"));
											dataObj.put("ID", AuditIdGenerator.genRandom32Hex());
											fromTableColsArray.add(dataObj);

										}

									}
								} else {
									JSONObject fromConnectObj = (JSONObject) fromOperator.get("connObj");
									String tableNames = (String) fromOperator.get("tableName");
									if (tableNames != null && !"".equalsIgnoreCase(tableNames)
											&& !"null".equalsIgnoreCase(tableNames) && tableNames.contains(".")) {
										tableNames = tableNames.split("[.]")[1];
									}
									Object connectionObj = getConnection(fromConnectObj);
									List<Object[]> columnsList = new ArrayList<>();
									if (connectionObj instanceof Connection) {
										fromConnection = (Connection) connectionObj;
										columnsList = getTableColumnsOpt(fromConnection, request, fromConnectObj,
												tableNames);
									} else if (connectionObj instanceof JCO.Client) {
										columnsList = dataMigrationAccessDAO.getSAPTableColumnsWithType(request,
												(JCO.Client) connectionObj, tableNames);
									}
									if (columnsList != null && !columnsList.isEmpty()) {

										List<Object[]> dataTypeList = new ArrayList<>();
										try {
											dataTypeList = genericDataPipingDAO.getTargetDataType(
													String.valueOf(fromConnectObj.get("CONN_CUST_COL1")).toUpperCase());
										} catch (Exception e) {
										}

										String[] tableNamesArray = tableNames.split(",");
										for (int i = 0; i < tableNamesArray.length; i++) {
											JSONObject dataObj = new JSONObject();
											dataObj.put("LEVEL_TYPE", "TABLE");
											dataObj.put("CONNECTION_NAME", fromConnectObj.get("CONNECTION_NAME"));
											dataObj.put("CONNECTION_TYPE", fromConnectObj.get("CONN_CUST_COL1"));
											dataObj.put("TABLE_NAME", tableNamesArray[i]);
											dataObj.put("CONNECTION_OBJ", fromConnectObj.toJSONString());
											dataObj.put("icon", "images/GridDB.png");
											dataObj.put("ID", AuditIdGenerator.genRandom32Hex());
											fromTableColsArray.add(dataObj);
										}
										for (Object[] columnsObjArray : columnsList) {
											JSONObject dataObj = new JSONObject();
											dataObj.put("LEVEL_TYPE", "COLUMN");
											dataObj.put("CONNECTION_NAME", columnsObjArray[1]);
											dataObj.put("CONNECTION_TYPE", fromConnectObj.get("CONN_CUST_COL1"));
											dataObj.put("TABLE_NAME", columnsObjArray[2]);
											dataObj.put("DATA_TYPE", columnsObjArray[3]);
											dataObj.put("COLUMN_TYPE", columnsObjArray[8]);
											dataObj.put("DATA_LENGTH", columnsObjArray[4]);
											dataObj.put("CONNECTION_OBJ", fromConnectObj.toJSONString());
											if (("P".equalsIgnoreCase(String.valueOf(columnsObjArray[7]))
													|| "PRI".equalsIgnoreCase(String.valueOf(columnsObjArray[7]))
													|| "PRIMARY KEY"
															.equalsIgnoreCase(String.valueOf(columnsObjArray[7])))
													&& "ENABLED".equalsIgnoreCase(String.valueOf(columnsObjArray[6]))) {
												dataObj.put("icon", "images/lock.png");
											}
											if (dataTypeList != null && !dataTypeList.isEmpty()) {
												JSONObject dataTypeObj = new JSONObject();
												List<Object[]> matchedDataTypeList = dataTypeList.stream()
														.filter(dataTypeArray -> (dataTypeArray[1] != null
																&& !"".equalsIgnoreCase(
																		String.valueOf(dataTypeArray[1]))
																&& !"null".equalsIgnoreCase(
																		String.valueOf(dataTypeArray[1]))
																&& dataTypeArray[6] != null
																&& !"".equalsIgnoreCase(
																		String.valueOf(dataTypeArray[6]))
																&& !"null".equalsIgnoreCase(
																		String.valueOf(dataTypeArray[6]))
																&& String.valueOf(dataTypeArray[6]).equalsIgnoreCase(
																		String.valueOf(columnsObjArray[3]))))
														.collect(Collectors.toList());
												if (matchedDataTypeList != null && !matchedDataTypeList.isEmpty()) {
													for (int i = 0; i < matchedDataTypeList.size(); i++) {
														Object[] matchedDataTypeArray = matchedDataTypeList.get(i);
														if (matchedDataTypeArray != null
																&& matchedDataTypeArray.length != 0) {
															String dataType = "" + matchedDataTypeArray[1];
															String length = "";
//                                                    dataTypeObj.put(connectionObj, connectionObj)
															if ("Y".equalsIgnoreCase(
																	String.valueOf(matchedDataTypeArray[3]))
																	&& columnsObjArray[4] != null
																	&& !"".equalsIgnoreCase(
																			String.valueOf(columnsObjArray[4]))
																	&& !"null".equalsIgnoreCase(
																			String.valueOf(columnsObjArray[4]))) {
																length += ""
																		+ new PilogUtilities().convertIntoInteger(
																				new BigInteger("" + columnsObjArray[4]))
																		+ " ";
																if (matchedDataTypeArray[4] != null
																		&& !"".equalsIgnoreCase(
																				String.valueOf(matchedDataTypeArray[4]))
																		&& !"null".equalsIgnoreCase(String
																				.valueOf(matchedDataTypeArray[4]))) {
																	length += "" + matchedDataTypeArray[4] + " ";
																}
															}

															if (length != null && !"".equalsIgnoreCase(length)
																	&& !"null".equalsIgnoreCase(length)) {
																dataType += "(" + length + ") ";
															}
															if (dataTypeObj != null && !dataTypeObj.isEmpty()) {
																if (!dataTypeObj.containsKey(matchedDataTypeArray[0])) {
																	dataTypeObj.put(matchedDataTypeArray[0], dataType);
																}

															} else {
																dataTypeObj.put(matchedDataTypeArray[0], dataType);
															}
														}

													}

												}
												dataObj.put("TO_COLUMN_TYPE", dataTypeObj.toJSONString());
											}
											dataObj.put("ID", AuditIdGenerator.genRandom32Hex());
											fromTableColsArray.add(dataObj);
										}
									}

								}

							}

						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (fromConnection != null) {
					fromConnection.close();
				}
			} catch (Exception e) {
			}
		}
		return fromTableColsArray;
	}

	public List<Object[]> getTableColumnsOpt(Connection connection, HttpServletRequest request, JSONObject dbObj,
			String tableName) {
		List<Object[]> columnsList = new ArrayList<>();
		try {
			if (connection != null) {
				if (tableName != null && tableName.contains(",")) {
					tableName = tableName.replaceAll(",", "','");
				}
				String initParamClassName = "com.pilog.mdm.DAO.V10DataMigrationAccessDAO";
				String initParamMethodName = "get" + dbObj.get("CONN_CUST_COL1") + "TableColumns";
				System.out.println(
						" initParamClassName:" + initParamClassName + "initParamMethodName:" + initParamMethodName);
				Class clazz = Class.forName(initParamClassName);
				Class<?>[] paramTypes = { Connection.class, String.class, String.class };
				Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
				Object targetObj = new PilogUtilities().createObjectByClass(clazz);
				columnsList = (List<Object[]>) method.invoke(targetObj, connection,
						String.valueOf(dbObj.get("CONN_DB_NAME")), tableName);
			}

		} catch (Exception e) {

			e.printStackTrace();
		}
		return columnsList;
	}

	public JSONObject createTableInETL(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		try {
			String user = (String) request.getSession(false).getAttribute("ssUsername");
			String orgnId = (String) request.getSession(false).getAttribute("ssOrgId");
			String createTableObjStr = request.getParameter("createTableObj");
			if (createTableObjStr != null && !"".equalsIgnoreCase(createTableObjStr)
					&& !"null".equalsIgnoreCase(createTableObjStr)) {
				JSONObject createTableObj = (JSONObject) JSONValue.parse(createTableObjStr);
				if (createTableObj != null && !createTableObj.isEmpty()) {
					String dataSourceName = (String) createTableObj.get("dataSourceName");
					if (!(dataSourceName != null && !"".equalsIgnoreCase(dataSourceName)
							&& !"null".equalsIgnoreCase(dataSourceName))) {
						dataSourceName = "Current V10";
					}
					if (!"Current V10".equalsIgnoreCase(dataSourceName)) {
						String connObjStr = (String) createTableObj.get("dataSourceObj");
						if (connObjStr != null && !"".equalsIgnoreCase(connObjStr)
								&& !"null".equalsIgnoreCase(connObjStr)) {
							JSONObject connObj = (JSONObject) JSONValue.parse(connObjStr);
							Object connectionObj = getConnection(connObj);
							if (connectionObj instanceof Connection) {
								conn = (Connection) connectionObj;
							} else {
								resultObj.put("message", connectionObj);
								resultObj.put("messageFlag", false);
							}
						}
					} else {
						Class.forName(dataBaseDriver);
						conn = DriverManager.getConnection(dbURL, userName, password);
					}
					if (conn != null) {
						if (createTableObj != null && !createTableObj.isEmpty()) {
							String tableName = (String) createTableObj.get("tableName");
							JSONArray columnsObjArray = (JSONArray) createTableObj.get("colsObj");
							if (tableName != null && !"".equalsIgnoreCase(tableName)
									&& !"null".equalsIgnoreCase(tableName)) {
								if (columnsObjArray != null && !columnsObjArray.isEmpty()) {
									String query = "";
									List<String> pkColumns = new ArrayList<>();
									for (int i = 0; i < columnsObjArray.size(); i++) {
										JSONObject columnObj = (JSONObject) columnsObjArray.get(i);
										if (columnObj != null && !columnObj.isEmpty()
												&& columnObj.get("COLUMN_NAME") != null
												&& !"".equalsIgnoreCase(String.valueOf(columnObj.get("COLUMN_NAME")))
												&& !"null"
														.equalsIgnoreCase(String.valueOf(columnObj.get("COLUMN_NAME")))
												&& columnObj.get("DATA_TYPE") != null
												&& !"".equalsIgnoreCase(String.valueOf(columnObj.get("DATA_TYPE")))
												&& !"null"
														.equalsIgnoreCase(String.valueOf(columnObj.get("DATA_TYPE")))) {
											String columnName = String.valueOf(columnObj.get("COLUMN_NAME"));
											if (columnName.contains("/")) {
												columnName = "\"" + columnName + "\"";
											}
											query += " " + columnName + " " + columnObj.get("DATA_TYPE");

											// PKH default value
											if (columnObj.get("DEFAULT_VALUE") != null
													&& !"".equalsIgnoreCase(
															String.valueOf(columnObj.get("DEFAULT_VALUE")))
													&& !"null".equalsIgnoreCase(
															String.valueOf(columnObj.get("DEFAULT_VALUE")))) {
												query += " DEFAULT " + columnObj.get("DEFAULT_VALUE") + "";
											}
											if ("Y".equalsIgnoreCase(String.valueOf(columnObj.get("NOTNULL")))) {
												query += " NOT NULL";
											}
											// PKH default value

											if ("Y".equalsIgnoreCase(String.valueOf(columnObj.get("PK")))) {
//                                                pkColumns.add(String.valueOf(columnObj.get("COLUMN_NAME")));
												pkColumns.add(String.valueOf(columnName));
											}
											if (i != columnsObjArray.size() - 1) {
												query += " , ";
											}
										}
									}
									if (query != null && !"".equalsIgnoreCase(query)
											&& !"null".equalsIgnoreCase(query)) {
//										try {
//											String dropQuery = "ALTER TABLE " + tableName.toUpperCase() + " "
//													+ " DROP PRIMARY KEY CASCADE";
//											preparedStatement = conn.prepareStatement(dropQuery);
//											preparedStatement.executeUpdate();
//										} catch (Exception e) {
//										}
//										try {
//											String dropQuery = "DROP TABLE " + tableName.toUpperCase()
//													+ " CASCADE CONSTRAINTS";
//											preparedStatement = conn.prepareStatement(dropQuery);
//											preparedStatement.executeUpdate();
//										} catch (Exception e) {
//										}
										query = "CREATE TABLE " + tableName.toUpperCase() + " (" + query + ")\n" + "";
										System.out.println("Create table Query:::" + query);
										preparedStatement = conn.prepareStatement(query);
										int createCount = preparedStatement.executeUpdate();
										System.out.println("createTableCount:::" + createCount);
										if (createCount == 0) {
											try {
												String dalEntryQuery = "INSERT INTO C_ETL_DAL_AUTHORIZATION(TABLE_NAME,CREATE_BY,ORGN_ID,TYPE) VALUES('"
														+ tableName.toUpperCase() + "','" + user + "','" + orgnId
														+ "','TABLES')";
												preparedStatement = conn.prepareStatement(dalEntryQuery);
												preparedStatement.executeUpdate();
											} catch (Exception e) {

											}

										}
										try {
											if (pkColumns != null && !pkColumns.isEmpty()) {
												String pkQuery = "ALTER TABLE " + tableName.toUpperCase() + " ADD (\n"
														+ "  CONSTRAINT " + tableName.toUpperCase() + "_PK\n"
														+ " PRIMARY KEY\n" + " ("
														+ StringUtils.collectionToCommaDelimitedString(pkColumns)
														+ "))";
												System.out.println("pkQuery:::" + pkQuery);
												preparedStatement = conn.prepareStatement(pkQuery);
												int createPKCount = preparedStatement.executeUpdate();
												System.out.println("createPKCount:::" + createPKCount);
											}
										} catch (Exception e) {
										}

										resultObj.put("message", "Table(" + tableName.toUpperCase()
												+ ") created/modified successfully.");
										resultObj.put("messageFlag", true);
									}
								} else {
									resultObj.put("message", "You must specify some columns to create the table.");
									resultObj.put("messageFlag", false);
								}
							} else {
								resultObj.put("message", "Table name should no be empty.");
								resultObj.put("messageFlag", false);
							}
						} else {
							resultObj.put("message", "No table and columns for creation.");
							resultObj.put("messageFlag", false);
						}
					}

				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			resultObj.put("message", e.getMessage());
			resultObj.put("messageFlag", false);
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (Exception e) {

			}
		}
		return resultObj;
	}

	public JSONObject getSchemaObjectData(HttpServletRequest request, String parentkeyData) {
		JSONObject resultObj = new JSONObject();
		try {
			int count = 0;
			int endTreeIndex = 0;
			JSONArray savedDbList = new JSONArray();
			String columnsObjStr = request.getParameter("columnsObj");
			String startIndex = request.getParameter("startIndex");
			String endIndex = request.getParameter("endIndex");
			String levelStr = request.getParameter("level");
			JSONObject columnsObj = new JSONObject();
			// if (columnsObjStr != null && !"".equalsIgnoreCase(columnsObjStr)) {
			// JSONObject treeColumnsObj = (JSONObject) JSONValue.parse(columnsObjStr);
//                if (treeColumnsObj != null && !treeColumnsObj.isEmpty()) {
//                    String levelStr = request.getParameter("level");
//                    String extTreeParams = request.getParameter("extTreeParams");
//                    JSONObject columnsObj = (JSONObject) treeColumnsObj.get(levelStr);
			// if (columnsObj != null && !columnsObj.isEmpty()) {
//                        JSONObject treeInitParams = (JSONObject) columnsObj.get("TREE_INIT_PARAMS");
//                        String showMoreIcon = "";
//                        if (treeInitParams != null && !treeInitParams.isEmpty()) {
//                            showMoreIcon = (String) treeInitParams.get("uuu_treeEtlShowMoreIcon");
//                            String dataBaseConnectFlag = (String) treeInitParams.get("uuu_DataBaseConnectivityFlag");
//                            if (dataBaseConnectFlag != null && !"".equalsIgnoreCase(dataBaseConnectFlag) && "Y".equalsIgnoreCase(dataBaseConnectFlag)) {
//                                savedDbList = visionTransformConnectionTabsDAO.getSavedConnections(request);
//                            }
//                        }

			savedDbList = visionTransformConnectionTabsDAO.getSavedConnections(request);

			String connObj = request.getParameter("connectionObj");
			if (connObj != null && !"".equalsIgnoreCase(connObj) && !"null".equalsIgnoreCase(connObj)) {
				JSONObject connectionObj = (JSONObject) JSONValue.parse(connObj);
				Object connectionObject = getConnection(connectionObj);
				if (connectionObject instanceof Connection) {
					resultObj = genericDataPipingDAO.getObjectdata(request, parentkeyData, columnsObj, connObj,
							levelStr, (Connection) connectionObject);
				} else if (connectionObject instanceof JCO.Client) {
					resultObj = genericDataPipingDAO.getSapTableData(request, (JCO.Client) connectionObject,
							parentkeyData);
				} else if (connectionObject instanceof MongoClient) {
					resultObj = genericDataPipingDAO.getDocumentData(request, parentkeyData, connObj,
							(MongoClient) connectionObject);
				} else {
					resultObj.put("dataFieldsArray", new JSONArray());
					resultObj.put("columnsArray", new JSONArray());
					resultObj.put("message", connectionObject);
					resultObj.put("messageFlag", false);
				}

			}

			// }
			// }
			// }
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getAllDataTypesList(HttpServletRequest request) {
		JSONObject allDataTypeObj = new JSONObject();
		try {
			List<Object[]> allDataTypeList = genericDataPipingDAO.getAllDataTypes();
			if (allDataTypeList != null && !allDataTypeList.isEmpty()) {
				List<String> allSysTypes = allDataTypeList.stream().map(objArray -> (String.valueOf(objArray[0])))
						.distinct().collect(Collectors.toList());
				if (allSysTypes != null && !allSysTypes.isEmpty()) {
					for (int i = 0; i < allSysTypes.size(); i++) {
						String sysType = allSysTypes.get(i);
						if (sysType != null && !"".equalsIgnoreCase(sysType) && !"null".equalsIgnoreCase(sysType)) {
							List<Object[]> selectedSysDataTypeList = allDataTypeList.stream()
									.filter(objectArray -> (sysType.equalsIgnoreCase(String.valueOf(objectArray[0]))))
									.collect(Collectors.toList());
							if (selectedSysDataTypeList != null && !selectedSysDataTypeList.isEmpty()) {
								allDataTypeObj.put(sysType, generateSelectStr(selectedSysDataTypeList));
							}
						}

					}
				}
			}
		} catch (Exception e) {
		}
		return allDataTypeObj;
	}

	public String generateSelectStr(List<Object[]> selectedSysDataTypeList) {
		String selectedStr = "<table border=\"0\" id=\"DATA_TYPE_FORM_VIEW\" "
				+ " class='tabsGridFormViewFieldsTable tabsFieldsTable erpdatatable responsiveTable'"
				+ " cellspacing='5' cellpadding='5' width='100%'>" + "<tr><th>Data Type</th>"
				+ "<td><select id='dataTypeList' onchange='dataTypeChange(this,id)' class='visionDataTypeList'> ";
		try {
			selectedStr += "<option value=''>Select</option>";
			if (selectedSysDataTypeList != null && !selectedSysDataTypeList.isEmpty()) {
				for (int i = 0; i < selectedSysDataTypeList.size(); i++) {
					Object[] dataTypeArray = selectedSysDataTypeList.get(i);
					if (dataTypeArray != null && dataTypeArray.length != 0) {
						selectedStr += "<option " + " data-sys-type='" + dataTypeArray[0] + "' " + " data-max-len='"
								+ dataTypeArray[2] + "' " + " data-length-flag='" + dataTypeArray[3] + "' "
								+ " data-byte-char-flag='" + dataTypeArray[4] + "' " + " data-precesion-flag='"
								+ dataTypeArray[5] + "' " + " data-scale-flag='" + dataTypeArray[6] + "' " + " value='"
								+ dataTypeArray[1] + "'>" + dataTypeArray[1] + "</option> ";
					}
				}
			}
			selectedStr += "</select></td>" + "<th>Length</th>"
					+ "<td><input type='number' id='dataTypeLength' class='visionDataTypeLength'/></td>"
					+ "<th>Byte/Char</th>"
					+ "<td width='10%'><select id='dataTypeByteChar' class='visionDataTypeByteChar'>"
					+ "<option value='CHAR'>Char</option>" + "<option value='BYTE'>Byte</option>" + " </select></td>"
					+ "</tr> " + "<tr>" + "<th>Precesion</th>"
					+ "<td width='20%'><input type='number' id='dataTypePrecesion' class='visionDataTypePrecesion'/></td>"
					+ "<th>Scale</th>"
					+ "<td width='20%'><input type='number' id='dataTypeScale' class='visionDataTypeScale'/></td>"
					+ "</tr>" + "</table>";
		} catch (Exception e) {
			e.printStackTrace();
		}
		return selectedStr;
	}

	public Map convertTransFrmRulsMapToParam(Map trfmRulesDataMap) {
		Map paramMap = new HashMap<>();
		try {
			JSONObject orderByList = new JSONObject();
			String groupByColumns = "";
			if (trfmRulesDataMap != null && !trfmRulesDataMap.isEmpty()) {
				List<Map> orderByDataMapList = (List) trfmRulesDataMap.get("orderByData");
				if (orderByDataMapList != null && !orderByDataMapList.isEmpty()) {
					int index = 1;
					for (Map orderByDataMap : orderByDataMapList) {
						if (orderByDataMap != null && !orderByDataMap.isEmpty()) {
							JSONObject orderByObj = new JSONObject();
							if (orderByDataMap.get("columnName") != null
									&& !"".equalsIgnoreCase(String.valueOf(orderByDataMap.get("columnName")))
									&& !"null".equalsIgnoreCase(String.valueOf(orderByDataMap.get("columnName")))
									&& orderByDataMap.get("order") != null
									&& !"".equalsIgnoreCase(String.valueOf(orderByDataMap.get("order")))
									&& !"null".equalsIgnoreCase(String.valueOf(orderByDataMap.get("order")))) {
								orderByObj.put("columnName", orderByDataMap.get("columnName"));
								orderByObj.put("direction", orderByDataMap.get("order"));
								orderByList.put(index, orderByObj);
								index++;
							}

						}
					}
				} // orderByDataMapList end
				paramMap.put("orderBy", orderByList);
				List<Map> groupByDataMapList = (List) trfmRulesDataMap.get("groupByData");
				if (groupByDataMapList != null && !groupByDataMapList.isEmpty()) {
					for (Map groupByDataMap : groupByDataMapList) {
						if (groupByDataMap != null && !groupByDataMap.isEmpty()) {
							if (groupByDataMap.get("columnName") != null
									&& !"".equalsIgnoreCase(String.valueOf(groupByDataMap.get("columnName")))
									&& !"null".equalsIgnoreCase(String.valueOf(groupByDataMap.get("columnName")))) {
								groupByColumns += "" + groupByDataMap.get("columnName") + ",";
							}
						}
					}
					if (groupByColumns != null && !"".equalsIgnoreCase(groupByColumns)
							&& !"null".equalsIgnoreCase(groupByColumns)) {
						groupByColumns = new PilogUtilities().trimChar(groupByColumns, ',');
					}
					paramMap.put("groupBy", groupByColumns);
				} // groupByDataMapList end
				List<String> joinClauseDataMapList = (List) trfmRulesDataMap.get("joinClauseData");
				List<String> childTables = (List) trfmRulesDataMap.get("childTables");
				if (childTables != null && !childTables.isEmpty() && joinClauseDataMapList != null
						&& !joinClauseDataMapList.isEmpty()) {
					JSONObject joinQueryMapObj = new JSONObject();
					String joinQuery = "";
					String masterTableName = (String) trfmRulesDataMap.get("masterTableName");
					joinQuery += " " + masterTableName;
					joinQueryMapObj.put(masterTableName, masterTableName);
					for (int i = 0; i < childTables.size(); i++) {
						String childTableName = childTables.get(i);
						String childJoinStr = joinClauseDataMapList.get(i);
						if (childJoinStr != null && !"".equalsIgnoreCase(childJoinStr)
								&& !"null".equalsIgnoreCase(childJoinStr)) {
							JSONObject joinObj = (JSONObject) JSONValue.parse(childJoinStr);
							if (joinObj != null && !joinObj.isEmpty()) {
								joinQueryMapObj.put(childTableName, joinObj);
								int j = 0;
								for (Object joinObjKey : joinObj.keySet()) {
									JSONObject joinMappedColumnObj = (JSONObject) joinObj.get(joinObjKey);
									if (joinMappedColumnObj != null && !joinMappedColumnObj.isEmpty()) {
										String childTableColumn = "";
										if (joinMappedColumnObj.get("childTableColumn") != null
												&& !"".equalsIgnoreCase(
														String.valueOf(joinMappedColumnObj.get("childTableColumn")))
												&& !"null".equalsIgnoreCase(
														String.valueOf(joinMappedColumnObj.get("childTableColumn")))) {// childTableColumn
											childTableColumn = String
													.valueOf(joinMappedColumnObj.get("childTableColumn"))
													.replace(":", ".");
										}
										String masterTableColumn = "";
										if (joinMappedColumnObj.get("masterTableColumn") != null
												&& !"".equalsIgnoreCase(
														String.valueOf(joinMappedColumnObj.get("masterTableColumn")))
												&& !"null".equalsIgnoreCase(
														String.valueOf(joinMappedColumnObj.get("masterTableColumn")))) {// childTableColumn
											masterTableColumn = String
													.valueOf(joinMappedColumnObj.get("masterTableColumn"))
													.replace(":", ".");
										}
										if (j == 0) {
											joinQuery += " " + joinMappedColumnObj.get("joinType") + " "
													+ childTableName + " ON ";
										}
										joinQuery += " " + childTableColumn + " " + joinMappedColumnObj.get("operator")
												+ " " + " "
												+ ((joinMappedColumnObj.get("staticValue") != null
														&& !"".equalsIgnoreCase(
																String.valueOf(joinMappedColumnObj.get("staticValue")))
														&& !"null".equalsIgnoreCase(
																String.valueOf(joinMappedColumnObj.get("staticValue"))))
																		? "'" + joinMappedColumnObj.get("staticValue")
																				+ "'"
																		: masterTableColumn);
										if (j != joinObj.size() - 1) {
											joinQuery += " AND ";
										}
									}
									j++;
								}
							}
						}

					}
					paramMap.put("joinQueryMapObj", joinQueryMapObj);
					paramMap.put("joinQuery", joinQuery);
				} // join Query Map Obj End
				JSONObject appendValObj = new JSONObject();
				// JSONObject colsObj = new JSONObject(); // ravi etl integration
				Map colsObj = new LinkedHashMap();
				JSONObject defaultValObj = new JSONObject();
				JSONObject columnClauseObj = new JSONObject();
				List<Map> colMappingsDataMapList = (List) trfmRulesDataMap.get("colMappingsData");
				if (colMappingsDataMapList != null && !colMappingsDataMapList.isEmpty()) {
					for (Map colMappingsDataMap : colMappingsDataMapList) {
						if (colMappingsDataMap != null && !colMappingsDataMap.isEmpty()) {
							String toTableColumnName = (String) colMappingsDataMap.get("destinationColumn");
							String fromTableColumn = (String) colMappingsDataMap.get("sourceColumn");
							String columnClause = (String) colMappingsDataMap.get("columnClause");
							String defaultValue = (String) colMappingsDataMap.get("defaultValue");
							String appendValue = (String) colMappingsDataMap.get("appendValue");
							if (toTableColumnName != null && !"".equalsIgnoreCase(toTableColumnName)
									&& !"null".equalsIgnoreCase(toTableColumnName)
									&& !toTableColumnName.contains("N/A")) {
								if (fromTableColumn != null && !"".equalsIgnoreCase(fromTableColumn)
										&& !"null".equalsIgnoreCase(fromTableColumn)
										&& !fromTableColumn.contains("N/A")) {

									colsObj.put(toTableColumnName.split(":")[1], fromTableColumn);
									if (appendValue != null && !"".equalsIgnoreCase(appendValue)
											&& !"null".equalsIgnoreCase(appendValue)) {
										appendValObj.put(fromTableColumn, appendValue);
									}
								} else if (columnClause != null && !"".equalsIgnoreCase(columnClause)
										&& !"null".equalsIgnoreCase(columnClause)) {
									columnClauseObj.put(toTableColumnName.split(":")[1], columnClause);
								}
							} else if (toTableColumnName != null && !"".equalsIgnoreCase(toTableColumnName)
									&& !"null".equalsIgnoreCase(toTableColumnName)
									&& toTableColumnName.contains("N/A")) {

								colsObj.put(fromTableColumn, fromTableColumn);
							}
							if (defaultValue != null && !"".equalsIgnoreCase(defaultValue)
									&& !"null".equalsIgnoreCase(defaultValue)) {
								if (toTableColumnName != null && !"".equalsIgnoreCase(toTableColumnName)
										&& !"null".equalsIgnoreCase(toTableColumnName)
										&& !toTableColumnName.contains("N/A")) {
									defaultValObj.put(toTableColumnName.split(":")[1], defaultValue);
								} else if (fromTableColumn != null && !"".equalsIgnoreCase(fromTableColumn)
										&& !"null".equalsIgnoreCase(fromTableColumn)
										&& !fromTableColumn.contains("N/A")) {
									defaultValObj.put(fromTableColumn, defaultValue);
								}

							}
						}
					}
					paramMap.put("columnsObj", colsObj);
					paramMap.put("defaultValObj", defaultValObj);
					paramMap.put("appendValObj", appendValObj);
					paramMap.put("columnClauseObj", columnClauseObj);
				} //
				List<String> whereClauseDataList = (List) trfmRulesDataMap.get("whereClauseData");
				JSONObject whereClauseObject = new JSONObject();
				if (whereClauseDataList != null && !whereClauseDataList.isEmpty()) {
					for (int i = 0; i < whereClauseDataList.size(); i++) {
						String whereClauseDataStr = whereClauseDataList.get(i);
						if (whereClauseDataStr != null && !"".equalsIgnoreCase(whereClauseDataStr)
								&& !"null".equalsIgnoreCase(whereClauseDataStr)) {
							JSONObject whereClauseData = (JSONObject) JSONValue.parse(whereClauseDataStr);
							if (whereClauseData != null && !whereClauseData.isEmpty()) {
								int j = 0;
								String tableName = "";
								String whereCluaseQuery = "";
								for (Object whereClauseObjKey : whereClauseData.keySet()) {
									JSONObject whereClauseObj = (JSONObject) whereClauseData.get(whereClauseObjKey);
									if (whereClauseObj != null && !whereClauseObj.isEmpty()) {
										String columnName = (String) whereClauseObj.get("columnName");
										String operator = (String) whereClauseObj.get("operator");
										String andOrOperator = (String) whereClauseObj.get("andOrOperator");
										String staticValue = (String) whereClauseObj.get("staticValue");
										if (columnName != null && !"".equalsIgnoreCase(columnName)
												&& !"null".equalsIgnoreCase(columnName)) {
											tableName = columnName.split(":")[0];
											columnName = columnName.replaceAll(":", ".");
											whereCluaseQuery += " " + columnName + " " + operator + " '" + staticValue
													+ "' ";
											if (j != whereClauseData.size() - 1) {
												whereCluaseQuery += " " + andOrOperator + " ";
											}
										}
									}
									j++;
								}
								if (tableName != null && !"".equalsIgnoreCase(tableName)
										&& !"null".equalsIgnoreCase(tableName) && whereCluaseQuery != null
										&& !"".equalsIgnoreCase(whereCluaseQuery)
										&& !"null".equalsIgnoreCase(whereCluaseQuery)) {
									whereClauseObject.put(tableName, whereCluaseQuery);
								}

							}
						}

					}
					paramMap.put("whereClauseObj", whereClauseObject);
				} // whereClauseDataList End

				JSONObject selectTabObj = (JSONObject) trfmRulesDataMap.get("selectTabObj");
				if (selectTabObj != null && !selectTabObj.isEmpty()) {
					paramMap.put("selectTabObj", selectTabObj);
				} // end selectTabObj
				JSONObject normalizeOptionsObj = (JSONObject) trfmRulesDataMap.get("normalizeOptionsObj");
				if (normalizeOptionsObj != null && !normalizeOptionsObj.isEmpty()) {
					paramMap.put("normalizeOptionsObj", normalizeOptionsObj);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return paramMap;
	}

	public JSONObject validateSQLQuery(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet resultSet = null;
		try {
			Class.forName(dataBaseDriver);
			conn = DriverManager.getConnection(dbURL, userName, password);
			String sqlQuery = request.getParameter("query");
			if (sqlQuery != null && !"".equalsIgnoreCase(sqlQuery) && !"null".equalsIgnoreCase(sqlQuery)) {
				stmt = conn.prepareStatement(sqlQuery);
				resultSet = stmt.executeQuery();
				resultObj.put("message", "Query validated with out errors.");
				resultObj.put("messageFlag", true);
			}
		} catch (Exception e) {
			String message = e.getMessage();
			if (message != null
					&& (message.contains("invalid identifier") || message.contains("table or view does not exist"))) {
				resultObj.put("message", "Query validated with out errors.");
				resultObj.put("messageFlag", true);
			} else {
				resultObj.put("message", message);
				resultObj.put("messageFlag", false);
			}

//            e.printStackTrace();
		} finally {
			try {
				if (resultSet != null) {
					resultSet.close();
				}
				if (stmt != null) {
					stmt.close();
				}
				if (stmt != null) {
					stmt.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (Exception e) {
			}
		}
		return resultObj;
	}

	public JSONObject executeSQLQuery(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		Connection conn = null;
		PreparedStatement sqlStmt = null;
		Statement stmt = null;
		ResultSet sqlResultSet = null;
		CallableStatement callableStmt = null;
		MongoClient client = null;
		String connectionDbName = "";
		try {
			String user = (String) request.getSession(false).getAttribute("ssUsername");
			String orgnId = (String) request.getSession(false).getAttribute("ssOrgId");
			String script = request.getParameter("script");
			String connName = request.getParameter("connectionName");
			if (script != null && !"".equalsIgnoreCase(script) && !"null".equalsIgnoreCase(script) && connName != null
					&& !"".equalsIgnoreCase(connName) && !"null".equalsIgnoreCase(connName)) {

				if ("Current_V10".equalsIgnoreCase(connName)) {
					JSONObject dbObj = new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName,
							password, "Current_V10");
					conn = (Connection) getGlobalConnection(dbObj);
//                    Class.forName(dataBaseDriver);
//                    conn = DriverManager.getConnection(dbURL, userName, password);
				} else {
					List<Object[]> selectedConnDetailsList = dataMigrationDAO.getConDetails(request, connName);
					if (selectedConnDetailsList != null && !selectedConnDetailsList.isEmpty()) {
						Object[] selectedConnDetails = selectedConnDetailsList.get(0);
						if (selectedConnDetails != null && selectedConnDetails.length != 0) {
							JSONObject dbObj = new JSONObject();
							dbObj.put("CONNECTION_NAME", selectedConnDetails[0]);
							dbObj.put("HOST_NAME", selectedConnDetails[1]);
							dbObj.put("CONN_USER_NAME", selectedConnDetails[2]);
							dbObj.put("CONN_PASSWORD", selectedConnDetails[3]);
							dbObj.put("CONN_PORT", selectedConnDetails[4]);
							dbObj.put("CONN_DB_NAME", selectedConnDetails[5]);
							dbObj.put("CONN_CUST_COL1", selectedConnDetails[6]);
							dbObj.put("CONN_CUST_COL2", selectedConnDetails[7]);
							dbObj.put("CONN_CUST_COL3", selectedConnDetails[8]);
//                            Object connObj = getConnection(dbObj);
							if ("MongoDb".equalsIgnoreCase((String) selectedConnDetails[6])) {
								client = (MongoClient) getGlobalConnection(dbObj);
								connectionDbName = (String) dbObj.get("CONN_DB_NAME");
							} else {
								Object connObj = conn = (Connection) getGlobalConnection(dbObj);
								if (connObj instanceof Connection) {
									conn = (Connection) connObj;
								}
							}

						}
					}
				}
				if (conn != null) {

					script = script.trim();
					script = script.replaceAll("\t", " ");
					script = script.replaceAll("\n", " ");
					String[] scriptArray = script.split(";");
					for (String subScript : scriptArray) {
						String opType = subScript.indexOf(" ") > 0 ? subScript.substring(0, subScript.indexOf(" "))
								: subScript;
						if (opType != null && !"".equalsIgnoreCase(opType)) {
							opType = opType.toUpperCase();
						}
						if (!"SELECT".equalsIgnoreCase(opType)
								|| (opType != null && !"".equalsIgnoreCase(opType) && !opType.startsWith("SELECT"))) {

							if ("UPDATE".equalsIgnoreCase(opType) || "DELETE".equalsIgnoreCase(opType)
									|| "INSERT".equalsIgnoreCase(opType) || "TRUNCATE".equalsIgnoreCase(opType)
									|| "ALTER".equalsIgnoreCase(opType)) {

								int noSelectCount = 0;

								sqlStmt = conn.prepareStatement(subScript);

								noSelectCount += sqlStmt.executeUpdate();
								conn.commit();

								String message = StringUtils.capitalize(opType.toLowerCase()) + "ed Successfully.";
								message = noSelectCount + " Row(s) has been " + message;
								resultObj.put("message", message);
								resultObj.put("messageFlag", true);
							} else if ("CREATE".equalsIgnoreCase(opType)) {
								sqlStmt = conn.prepareStatement(subScript);
								int createCount = sqlStmt.executeUpdate();
								if (createCount == 0 || createCount > 0) {
									String tableName = subScript.split("\\s+")[2];
									try {

										String dalEntryQuery = "INSERT INTO C_ETL_DAL_AUTHORIZATION(TABLE_NAME,CREATE_BY,ORGN_ID,TYPE) VALUES('"
												+ tableName.toUpperCase() + "','" + user + "','" + orgnId
												+ "','TABLES')";
										sqlStmt = conn.prepareStatement(dalEntryQuery);
										sqlStmt.executeUpdate();
									} catch (Exception e) {

									}
									String message = "Created " + tableName + " Successfully";
									resultObj.put("message", message);

								}
							} else if ("ROLLBACK;".equalsIgnoreCase(opType) || "ROLLBACK".equalsIgnoreCase(opType)) {
								conn.rollback();
								String message = "Rollback successfully";
								resultObj.put("message", message);
							} else if ("COMMIT;".equalsIgnoreCase(opType) || "COMMIT".equalsIgnoreCase(opType)) {
								conn.commit();
								String message = "Commit successfully";
								resultObj.put("message", message);
							} else if (subScript != null && subScript.contains("BEGIN")) {
								String procedureName = "";
								if (subScript.contains("PROCEDURE")) {
									int procedureIndex = subScript.indexOf("PROCEDURE");
									if (procedureIndex != -1) {
										// Move to the first character after "PROCEDURE"
										String afterProcedure = subScript
												.substring(procedureIndex + "PROCEDURE".length());

										// Trim spaces and get the procedure name
										String[] parts = afterProcedure.trim().split("\\.");

										if (parts.length > 1) {
											procedureName = parts[1].toString().split(" ")[0]; // Remove quotes if
																								// present
											procedureName = procedureName.replaceAll("\"", "");
											System.out.println("Found procedure name: " + procedureName);
										} else {
											System.out.println("No procedure name found after PROCEDURE.");
										}
									} else {
										System.out.println("No PROCEDURE found in the script.");
									}
									Statement enableStmt = conn.createStatement();
									enableStmt.executeUpdate("BEGIN DBMS_OUTPUT.ENABLE(NULL); END;");
									enableStmt.close();

									callableStmt = conn.prepareCall("CALL " + procedureName + "()");
									callableStmt.execute();
									System.out.println("proc is executing");

									CallableStatement outputStatement = conn
											.prepareCall("BEGIN DBMS_OUTPUT.GET_LINES(?, ?); END;");
									outputStatement.registerOutParameter(1, Types.ARRAY, "DBMSOUTPUT_LINESARRAY");
									outputStatement.registerOutParameter(2, Types.INTEGER);
									outputStatement.execute();

									Array array = outputStatement.getArray(1);
									String[] lines = (String[]) array.getArray();
									// String outputlines = String.join(" ", lines);

									String outputlines = "";
									for (String line : lines) {
										if (line != null) {
											System.out.println(line);
											outputlines += line + "$";
										}
									}
									resultObj.put("dbmsOutput", outputlines);
									System.out.println("Script Executed......");
								} else {
									Statement enableStmt = conn.createStatement();
									enableStmt.executeUpdate("BEGIN DBMS_OUTPUT.ENABLE(NULL); END;");
									enableStmt.close();
									callableStmt = conn.prepareCall(script);
									callableStmt.execute();

									CallableStatement outputStatement = conn
											.prepareCall("BEGIN DBMS_OUTPUT.GET_LINES(?, ?); END;");
									outputStatement.registerOutParameter(1, Types.ARRAY, "DBMSOUTPUT_LINESARRAY");
									outputStatement.registerOutParameter(2, Types.INTEGER);
									outputStatement.execute();

									Array array = outputStatement.getArray(1);
									String[] lines = (String[]) array.getArray();

									String outputlines = "";
									for (String line : lines) {
										if (line != null) {
											System.out.println(line);
											outputlines += line;
										}
									}
									resultObj.put("dbmsOutput", outputlines);
									resultObj.put("message", "Script Executed Succesfully.");
									resultObj.put("messageFlag", true);
									System.out.println("Script Executed......");
								}
							} else {
								if (subScript.trim().endsWith(";")) {
									subScript = subScript.substring(0, script.lastIndexOf(";"));
								}
								sqlStmt = conn.prepareStatement(subScript);
								sqlResultSet = sqlStmt.executeQuery();
								ResultSetMetaData resultSetMetaData = sqlResultSet.getMetaData();
								int columnCount = resultSetMetaData.getColumnCount();
								if (sqlResultSet.next()) {
									List<String> columnList = new ArrayList<>();
									if (true) {
										JSONObject gridProperties = new JSONObject();
										JSONObject gridObject = new JSONObject();
										List gridDataFieldsList = new ArrayList();
										List gridColumnsList = new ArrayList();
										HashMap<String, Integer> columnCounts = new HashMap<>();
										for (int i = 1; i <= columnCount; i++) {
											String columnName = resultSetMetaData.getColumnName(i);
											if (columnCounts.containsKey(columnName)) {
												int count = columnCounts.get(columnName);
												count++;
												columnCounts.put(columnName, count);
												if (count == 2) {
													columnName = columnName + "_1";
												} else {
													columnName = columnName + "_" + (count - 1);
												}
											} else {
												columnCounts.put(columnName, 1);
											}

											columnList.add(columnName);
											JSONObject dataFieldObj = new JSONObject();

											JSONObject columnsObj = new JSONObject();
											dataFieldObj.put("name", columnName);
											String columnType = resultSetMetaData.getColumnTypeName(i);
//                                            if ("DATE".equalsIgnoreCase(columnType)
//                                                    || "DATETIME".equalsIgnoreCase(columnType)
//                                                    || "TIMESTAMP".equalsIgnoreCase(columnType)) {
//                                                dataFieldObj.put("type", "date");//15
//                                            } else {
//                                                dataFieldObj.put("type", "string");//15
//                                            }
											dataFieldObj.put("type", "string");// 15
											columnsObj.put("text", columnName);// 3
											columnsObj.put("editable", false);
											columnsObj.put("datafield", columnName);
											columnsObj.put("width", ("20" + "%"));// 7
											columnsObj.put("showfilterrow", true);// 7
											columnsObj.put("cellsalign", "left");// 15
											columnsObj.put("align", "center");// 15
//                                        columnsObj.put("enabletooltips", true);
											columnsObj.put("filterable", true);
											columnsObj.put("sortable", true);
											columnsObj.put("filtercondition", "contains");
											columnsObj.put("enabletooltips", true);
											gridDataFieldsList.add(dataFieldObj);
											gridColumnsList.add(columnsObj);
										}
										gridObject.put("datafields", gridDataFieldsList);
										gridObject.put("columns", gridColumnsList);
										gridObject.put("gridProperties", gridProperties);
										gridObject.put("columnList", columnList);
										resultObj.put("gridObject", gridObject);
									}

									resultObj.put("message", "Data Selected Succesfully.");
									resultObj.put("messageFlag", true);
									resultObj.put("selectFlag", true);
								} else {
									resultObj.put("message", "No Row(s) selected.");
									resultObj.put("messageFlag", true);
								}
//                                resultObj.put("message", "Script Executed......");
//                                System.out.println("Script Executed......");
							}

						} else {
							if (subScript.trim().endsWith(";")) {
								subScript = subScript.substring(0, subScript.lastIndexOf(";"));
							}
							sqlStmt = conn.prepareStatement(subScript);
							sqlResultSet = sqlStmt.executeQuery();
							ResultSetMetaData resultSetMetaData = sqlResultSet.getMetaData();
							int columnCount = resultSetMetaData.getColumnCount();
//                            if (sqlResultSet.next()) {
							List<String> columnList = new ArrayList<>();
							if (true) {
								JSONObject gridProperties = new JSONObject();
								JSONObject gridObject = new JSONObject();
								List gridDataFieldsList = new ArrayList();
								List gridColumnsList = new ArrayList();
								HashMap<String, Integer> columnCounts = new HashMap<>();
								for (int i = 1; i <= columnCount; i++) {
									String columnName = resultSetMetaData.getColumnName(i);
									if (columnCounts.containsKey(columnName)) {
										int count = columnCounts.get(columnName);
										count++;
										columnCounts.put(columnName, count);
										if (count == 2) {
											columnName = columnName + "_1";
										} else {
											columnName = columnName + "_" + (count - 1);
										}
									} else {
										columnCounts.put(columnName, 1);
									}

									columnList.add(columnName);
									JSONObject dataFieldObj = new JSONObject();

									JSONObject columnsObj = new JSONObject();
									dataFieldObj.put("name", columnName);
									String columnType = resultSetMetaData.getColumnTypeName(i);
//                                        if ("DATE".equalsIgnoreCase(columnType)
//                                                || "DATETIME".equalsIgnoreCase(columnType)
//                                                || "TIMESTAMP".equalsIgnoreCase(columnType)) {
//                                            dataFieldObj.put("type", "date");//15
//                                        } else {
//                                            dataFieldObj.put("type", "string");//15
//                                        }
									dataFieldObj.put("type", "string");// 15
									columnsObj.put("text", columnName);// 3
									columnsObj.put("editable", false);
									columnsObj.put("datafield", columnName);
									columnsObj.put("width", ("20" + "%"));// 7
									columnsObj.put("showfilterrow", true);// 7
									columnsObj.put("cellsalign", "left");// 15
									columnsObj.put("align", "center");// 15
//                                    columnsObj.put("enabletooltips", true);
									columnsObj.put("filterable", true);
									columnsObj.put("sortable", true);
									columnsObj.put("filtercondition", "contains");
									columnsObj.put("enabletooltips", true);
									gridDataFieldsList.add(dataFieldObj);
									gridColumnsList.add(columnsObj);
								}
								gridObject.put("datafields", gridDataFieldsList);
								gridObject.put("columns", gridColumnsList);
								gridObject.put("gridProperties", gridProperties);
								gridObject.put("columnList", columnList);
								resultObj.put("gridObject", gridObject);
							}

							resultObj.put("message", "Data Selected Succesfully.");
							resultObj.put("messageFlag", true);
							resultObj.put("selectFlag", true);
//                            } else {
//                                resultObj.put("message", "No Row(s) selected.");
//                                resultObj.put("messageFlag", true);
//                            }

						}
					}

				} else if (client != null) {
					if (script.trim().endsWith(";")) {
						script = script.substring(0, script.lastIndexOf(";"));
					}
					MongoDatabase database = client.getDatabase(connectionDbName);
					script.toUpperCase();
					String collectionName = extractCollectionName(script);
					MongoCollection<org.bson.Document> collection = database.getCollection(collectionName);
					List<org.bson.Document> pipeline = convertSQLToMongoDB(script);
					List<org.bson.Document> results = new ArrayList<>();
					AggregateIterable<org.bson.Document> result = collection.aggregate((List<? extends Bson>) pipeline);
					for (org.bson.Document document : result) {
						// System.out.println(document.toJson());
						results.add((org.bson.Document) document);
					}
					org.bson.Document firstDocument = (org.bson.Document) results.get(0);
					if (firstDocument != null) {
						JSONObject gridProperties = new JSONObject();
						JSONObject gridObject = new JSONObject();
						List gridDataFieldsList = new ArrayList();
						List gridColumnsList = new ArrayList();
						HashMap<String, Integer> columnCounts = new HashMap<>();

						for (String columnName : firstDocument.keySet()) {
							if (!(columnName.equalsIgnoreCase("_id"))) {
								if (columnCounts.containsKey(columnName)) {
									int count = columnCounts.get(columnName);
									count++;
									columnCounts.put(columnName, count);
									if (count == 2) {
										columnName = columnName + "_1";
									} else {
										columnName = columnName + "_" + (count - 1);
									}
								} else {
									columnCounts.put(columnName, 1);
								}

								JSONObject dataFieldObj = new JSONObject();
								JSONObject columnsObj = new JSONObject();

								dataFieldObj.put("name", columnName);
								dataFieldObj.put("type", "string");// Adjust based on the actual data type
								columnsObj.put("text", columnName);
								columnsObj.put("editable", false);
								columnsObj.put("datafield", columnName);
								columnsObj.put("width", "20%");
								columnsObj.put("showfilterrow", true);
								columnsObj.put("cellsalign", "left");
								columnsObj.put("align", "center");
								columnsObj.put("filterable", true);
								columnsObj.put("sortable", true);
								columnsObj.put("filtercondition", "contains");
								columnsObj.put("enabletooltips", true);

								gridDataFieldsList.add(dataFieldObj);
								gridColumnsList.add(columnsObj);
							}
						}

						gridObject.put("datafields", gridDataFieldsList);
						gridObject.put("columns", gridColumnsList);
						gridObject.put("gridProperties", gridProperties);
						gridObject.put("columnList", new ArrayList<>(columnCounts.keySet()));

						resultObj.put("gridObject", gridObject);
						resultObj.put("message", "Data Selected Successfully.");
						resultObj.put("messageFlag", true);
						resultObj.put("selectFlag", true);
					} else {
						resultObj.put("message", "No Documents selected.");
					}
				} else {
					resultObj.put("message", "Unable to Connection Obj");
					resultObj.put("messageFlag", false);
				}
			}
		} catch (StringIndexOutOfBoundsException e) {
			resultObj.put("message", "Query/Script not valid");
			resultObj.put("messageFlag", false);
		} catch (Exception e) {
			resultObj.put("message", e.getMessage());
			resultObj.put("messageFlag", false);
			e.printStackTrace();
		} finally {
			try {
				if (sqlResultSet != null) {
					sqlResultSet.close();
				}
				if (sqlStmt != null) {
					sqlStmt.close();
				}
				if (stmt != null) {
					stmt.close();
				}
				if (conn != null) {
					conn.close();
				}
				if (client != null) {
					client.close();
				}
			} catch (Exception e) {
			}
		}
		return resultObj;
	}

	public List showExecutionQueryResults(HttpServletRequest request) {
		Connection conn = null;
		PreparedStatement sqlStmt = null;
		ResultSet sqlResultSet = null;
		PreparedStatement sqlCountStmt = null;
		ResultSet sqlCountResultSet = null;
		List resultList = new ArrayList();
		MongoClient client = null;
		try {
			List<String> columnList = new ArrayList<>();
			String colsArray = request.getParameter("columnList");
			if (colsArray != null && !"".equalsIgnoreCase(colsArray) && !"null".equalsIgnoreCase(colsArray)) {
				columnList = (List<String>) JSONValue.parse(colsArray);
			}
			String script = request.getParameter("script");
			String connName = request.getParameter("connectionName");
			if (script != null && !"".equalsIgnoreCase(script) && !"null".equalsIgnoreCase(script) && connName != null
					&& !"".equalsIgnoreCase(connName) && !"null".equalsIgnoreCase(connName)) {

				if (script.trim().endsWith(";")) {
					script = script.substring(0, script.lastIndexOf(";"));
				}
				String dbType = "";
				String connDbName = "";
				if ("Current_V10".equalsIgnoreCase(connName)) {
					Class.forName(dataBaseDriver);
					conn = DriverManager.getConnection(dbURL, userName, password);
					JSONObject dbObj = new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName,
							password, "Current_V10");

					dbType = (String) dbObj.get("CONN_CUST_COL1");
				} else {
					List<Object[]> selectedConnDetailsList = dataMigrationDAO.getConDetails(request, connName);
					if (selectedConnDetailsList != null && !selectedConnDetailsList.isEmpty()) {
						Object[] selectedConnDetails = selectedConnDetailsList.get(0);
						if (selectedConnDetails != null && selectedConnDetails.length != 0) {
							JSONObject dbObj = new JSONObject();
							dbObj.put("CONNECTION_NAME", selectedConnDetails[0]);
							dbObj.put("HOST_NAME", selectedConnDetails[1]);
							dbObj.put("CONN_USER_NAME", selectedConnDetails[2]);
							dbObj.put("CONN_PASSWORD", selectedConnDetails[3]);
							dbObj.put("CONN_PORT", selectedConnDetails[4]);
							dbObj.put("CONN_DB_NAME", selectedConnDetails[5]);
							dbObj.put("CONN_CUST_COL1", selectedConnDetails[6]);
							dbObj.put("CONN_CUST_COL2", selectedConnDetails[7]);
							dbObj.put("CONN_CUST_COL3", selectedConnDetails[8]);
							dbType = String.valueOf(selectedConnDetails[6]);
							connDbName = String.valueOf(selectedConnDetails[5]);
							//
							if (dbType.equalsIgnoreCase("MongoDb")) {
								Object connObj = client = (MongoClient) getGlobalConnection(dbObj);
							}
							Object connObj = getConnection(dbObj);
							if (connObj instanceof Connection) {
								conn = (Connection) connObj;
							}

						}
					}
				}
				if (conn != null) {
					script = script.trim();
					script = script.replaceAll("\t", " ");
					script = script.replaceAll("\n", " ");
					String countQuery = "";
//                    if (script.contains(" from ")) {
//                        countQuery = script.substring(script.indexOf(" from "));
//                    } else if (script.contains(" FROM ")) {
//                        countQuery = script.substring(script.indexOf(" FROM "));
//                    }

					String pagenum = request.getParameter("pagenum");
					String pagesize = request.getParameter("pagesize");
					String recordendindex = request.getParameter("recordendindex");
					String recordstartindex = (request.getParameter("recordstartindex") != null
							? request.getParameter("recordstartindex")
							: "0");
//                    if (!script.contains(" OFFSET ") || !script.contains(" offset ")) {
//                        if (dataBaseDriver.toUpperCase().contains("ORACLE") || "Oracle".equalsIgnoreCase(dbType)) {
//                            script += " OFFSET " + recordstartindex + " ROWS FETCH NEXT " + pagesize + " ROWS ONLY ";
//                        } else if (dataBaseDriver.toUpperCase().contains("MYSQL") || "MYSQL".equalsIgnoreCase(dbType)) {
//                            script += " LIMIT " + recordstartindex + " , " + pagesize + " ";
//                        } else if (dataBaseDriver.toUpperCase().contains("SQLSERVER") || "SQLSERVER".equalsIgnoreCase(dbType)) {
//                            if (!script.contains(" ORDER BY ") || !script.contains(" order by ")) {//ORDER BY (SELECT NULL)
//                                script += " ORDER BY (SELECT NULL) ";
//                            }
//                            script += " OFFSET " + recordstartindex + " ROWS FETCH NEXT " + pagesize + " ROWS ONLY ";
//                        } else if (dataBaseDriver.toUpperCase().contains("DB2") || "DB2".equalsIgnoreCase(dbType)) {
//
//                        }
//                }

					String conditionQuery = "";
					Integer filterscount = 0;
					String filterCondition = "";
					if (request.getParameter("filterscount") != null
							&& !"0".equalsIgnoreCase(request.getParameter("filterscount"))) {
						filterscount = new Integer(request.getParameter("filterscount"));
						filterCondition = genericDataPipingDAO.buildFilterCondition(filterscount, request, dbType);
						if (!"".equalsIgnoreCase(filterCondition)) {
							conditionQuery += " WHERE " + filterCondition;
						}
					} else if (request.getParameter("whereClause") != null
							&& !"".equalsIgnoreCase(request.getParameter("whereClause"))) {
						conditionQuery += " " + request.getParameter("whereClause");
					}
					countQuery = "SELECT COUNT(*) AS COUNT FROM (" + script + ") " + conditionQuery;

					String orderby = "";
					String sortdatafield = request.getParameter("sortdatafield");
					String sortorder = request.getParameter("sortorder");
					if (!(sortdatafield != null && !"".equalsIgnoreCase(sortdatafield))) {
						sortdatafield = (String) request.getAttribute("sortdatafield");
					}
					if (!(sortorder != null && !"".equalsIgnoreCase(sortorder))) {
						sortorder = (String) request.getAttribute("sortorder");
					}
					if (sortdatafield != null && sortorder != null
							&& (sortorder.equals("asc") || sortorder.equals("desc"))) {
						orderby = " ORDER BY " + sortdatafield + " " + sortorder;
					}
					script += conditionQuery;
					script += orderby;

					if (!(script.contains(" OFFSET ") || script.contains(" offset "))
							&& !(script.contains(" FETCH ") || script.contains(" fetch "))) {
						if ("Oracle".equalsIgnoreCase(dbType)) {
							script += " OFFSET " + recordstartindex + " ROWS FETCH NEXT " + pagesize + " ROWS ONLY ";
						} else if ("MYSQL".equalsIgnoreCase(dbType)) {
							script += " LIMIT " + recordstartindex + " , " + pagesize + " ";
						} else if ("SQLSERVER".equalsIgnoreCase(dbType)) {
							if (!script.contains(" ORDER BY ") || !script.contains(" order by ")) {// ORDER BY (SELECT
																									// NULL)
								script += " ORDER BY (SELECT NULL) ";
							}
							script += " OFFSET " + recordstartindex + " ROWS FETCH NEXT " + pagesize + " ROWS ONLY ";
						} else if (dataBaseDriver.toUpperCase().contains("DB2") || "DB2".equalsIgnoreCase(dbType)) {

						}
					}

					System.out.println("countQuery:::" + countQuery);
					sqlCountStmt = conn.prepareStatement(countQuery);
					sqlCountResultSet = sqlCountStmt.executeQuery();
					String totalCount = "0";
					if (sqlCountResultSet.next()) {
						totalCount = sqlCountResultSet.getString("COUNT");
					}
					sqlStmt = conn.prepareStatement(script);

					sqlResultSet = sqlStmt.executeQuery();
					int j = 0;
					ResultSetMetaData resultSetMetaData = sqlResultSet.getMetaData();
					int columnCount = resultSetMetaData.getColumnCount();
					while (sqlResultSet.next()) {
						JSONObject dataObject = new JSONObject();

						for (int i = 1; i <= columnCount; i++) {
							String columnName = resultSetMetaData.getColumnName(i);
							String columnType = resultSetMetaData.getColumnTypeName(i);
							Object dataObj = null;
							if ("DATE".equalsIgnoreCase(columnType) || "DATETIME".equalsIgnoreCase(columnType)
									|| "TIMESTAMP".equalsIgnoreCase(columnType)) {
								dataObj = sqlResultSet.getString(columnName);
							} else if ("CLOB".equalsIgnoreCase(columnType)) {
								dataObj = sqlResultSet.getString(columnName);
							} else {
								dataObj = sqlResultSet.getObject(columnName);
							}

							if (dataObj instanceof byte[]) {
								byte[] dataArray = (byte[]) dataObj;
								dataObj = new RAW(dataArray).stringValue();
							}
							if (dataObj instanceof oracle.sql.BLOB) {

								dataObj = "";
							}
							dataObject.put(columnName, dataObj);
						}
						if (j == 0) {
							dataObject.put("TotalRows", totalCount);
						}
						resultList.add(dataObject);
						j++;
					}
				} else if (client != null) {
					MongoDatabase database = client.getDatabase(connDbName);
					script.toUpperCase();
					String collectionName = extractCollectionName(script);
					MongoCollection<org.bson.Document> collection = database.getCollection(collectionName);
					int pageSize = request.getParameter("pagesize") != null
							? Integer.parseInt(request.getParameter("pagesize"))
							: 10; // Set your desired page size

					// Get the requested page number or default to 1 if not provided
					int pageNum = request.getParameter("pagenum") != null
							? Integer.parseInt(request.getParameter("pagenum"))
							: 1;

					// Skip documents based on pagination
					int skipCount = (pageNum - 1) * pageSize;
					int recordsCount = 0;

					Integer filterscount = 0;
					org.bson.Document filterCriteria = null;
					if (request.getParameter("filterscount") != null
							&& !"0".equalsIgnoreCase(request.getParameter("filterscount"))) {
						filterscount = new Integer(request.getParameter("filterscount"));
						filterCriteria = genericDataPipingDAO.constructFilterCriteria(request, filterscount);
					}

					FindIterable<org.bson.Document> findIterable;

					if (filterCriteria != null && !filterCriteria.isEmpty()) {
						// If filterCriteria is present, apply filtering
						findIterable = collection.find(filterCriteria).skip(skipCount).limit(pageSize);
						recordsCount = (int) collection.countDocuments(filterCriteria);
					} else {
						// If filterCriteria is null or empty, fetch all documents without filtering
						findIterable = collection.find().skip(skipCount).limit(pageSize);
						recordsCount = (int) collection.countDocuments();
					}

					String sortdatafield = request.getParameter("sortdatafield");
					String sortorder = request.getParameter("sortorder");

					// Default sorting field and order if not provided
					if (sortdatafield == null || sortdatafield.isEmpty()) {
						sortdatafield = "defaultSortField"; // Replace with your default sorting field
					}

					// Add sorting conditions to the query
					if (sortorder != null && !sortorder.isEmpty()) {
						Bson sort = Sorts.orderBy(Sorts.ascending(sortdatafield)); // Ascending order by default
						if ("desc".equalsIgnoreCase(sortorder)) {
							sort = Sorts.orderBy(Sorts.descending(sortdatafield));
						}

						findIterable = findIterable.sort(sort);
					}
					int j = 0;
					for (org.bson.Document document : findIterable) {
						JSONObject dataObject = new JSONObject();
						for (String columnName : document.keySet()) {
							if (!columnName.equalsIgnoreCase("_id")) {
								Object data = document.get(columnName);
								dataObject.put(columnName, data);
							}
						}
						resultList.add(dataObject);
						if (j == 0) {
							dataObject.put("TotalRows", recordsCount);
						}
						j++;
					}

				}
			}
		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			try {
				if (sqlResultSet != null) {
					sqlResultSet.close();
				}
				if (sqlCountResultSet != null) {
					sqlCountResultSet.close();
				}
				if (sqlStmt != null) {
					sqlStmt.close();
				}
				if (sqlCountStmt != null) {
					sqlCountStmt.close();
				}
				if (conn != null) {
					conn.close();
				}
				if (client != null) {
					client.close();
				}
			} catch (Exception e) {
			}
		}
		return resultList;
	}

	public JSONObject allColMappingForm(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String toTableColsArrayStr = request.getParameter("toTableColsArrayStr");
			String fromTableColsArrayStr = request.getParameter("fromTableColsArrayStr");
			String toIconType = request.getParameter("toIconType");
			if (!(toTableColsArrayStr != null && !"".equalsIgnoreCase(toTableColsArrayStr)
					&& !"null".equalsIgnoreCase(toTableColsArrayStr) && !"[]".equalsIgnoreCase(toTableColsArrayStr))) {
				toTableColsArrayStr = fromTableColsArrayStr;
			}
			String sqlPopupDiv = "<div id='sqlPopupDivId' class='sqlPopupDivClass'>"
					+ "<div id='sqlFromTableTreeDiv' class='sqlFromTableTreeDivClass'>"
					+ "<div id='columnSQLMappingTree' class='columnMappingTree'></div>" + "" + "</div>"
					+ "<div id='sqlMoveButtonsDiv' class='sqlMoveButtonsDivClass'>"
					+ "<div class='sqlMoveButtonsClass'>"
					// + "<b>"
					// + "<input onclick=mapTableColumns('columnSQLMappingTree','selected')
					// type=\"button\" value=\">\" class=\"sqlMoveButtons\">"
					// + "</b>"

					+ "<b>"
					+ "<input title=\"Click here for Sequential Mapping\" onclick=sequenceMappingColumns('columnSQLMappingTree','ALL') type=\"button\" value=\">>\" class=\"sqlMoveButtons\">"
					+ "</b>" + "<br></br>"

					+ "<b>"
					+ "<input title=\"Click here to Map All Columns\" onclick=mapTableColumns('columnSQLMappingTree','ALL') type=\"button\" value=\">>\" class=\"sqlMoveButtons\">"
					+ "</b>"
					// + "<b><input type=\"button\"
					// onclick=moveTableColumns('columnSQLMappingTree','all') value=\">>\"
					// class=\"sqlMoveButtons\"></b>"
					+ "</div>" + "</div>" + "<div id='sqlToTableDiv' class='sqlToTableDivClass'>"
					// + "<div id='sqlToTableIconsDiv'>"
					//// + "<img data-trstring='' src=\"images/Add icon.svg\"
					// id=\"visionEtlAddRowIcon\" "
					//// + "class=\"visionEtlAddRowIcon\" title=\"Add new Column\""
					//// + " onclick=addNewTableRow('sqlToTableTrDiv','sqlToTableColumnsTable',this)
					// "
					//// + "style=\"width:15px;height: 15px;cursor:pointer; float: left;\"/>"
					// + "</div>"
					+ "<div id='sqlToTableColumnsDiv' class='sqlMapColToTableColumnsDivClass'>"
					+ "<table class=\"visionEtlCreateSQLTable\" id='sqlToTableColumnsTable' style='width: 100%;' border='1'>"
					+ "<thead>";
			String columnsStr = "";
//            if (toIconType != null
//                    && !"".equalsIgnoreCase(toIconType)
//                    && !"null".equalsIgnoreCase(toIconType)
//                    && !"SQL".equalsIgnoreCase(toIconType)) {
//                sqlPopupDiv += "<tr>"
//                        //                    + "<th width='5%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'></th>"//delete
//                        + "<th width='35%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Source Column</th>"//Destination
//                        //                        + "<th width='35%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Destination Column</th>"//Source
//                        //                    + "<th width='35%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Default Values</th>"
//                        + "</tr>";
////                JSONArray fromTableColsArray = (JSONArray) JSONValue.parse(fromTableColsArrayStr);
////                if (fromTableColsArray != null && !fromTableColsArray.isEmpty()) {
////                    for (int i = 0; i < fromTableColsArray.size(); i++) {
////                        JSONObject columnObj = (JSONObject) fromTableColsArray.get(i);
////                        if (columnObj != null
////                                && !columnObj.isEmpty()
////                                && columnObj.containsKey("parentid")) {
////                            columnsStr += "<tr>"
////                                    //                    + "<th width='5%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'></th>"//delete
////                                    + "<td width='35%'>"
////                                    + "<input class='visionColMappingInput' type='text' value='' title='" + columnObj.get("value") + "' readonly='true'/>"
////                                    + "</td>"//Destination
////                                    //                                    + "<th width='35%'></th>"//Source
////                                    //                    + "<th width='35%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Default Values</th>"
////                                    + "</tr>";
////                        }
////
////                    }
////                }
//            } else
			if (toTableColsArrayStr != null && !"".equalsIgnoreCase(toTableColsArrayStr)
					&& !"null".equalsIgnoreCase(toTableColsArrayStr)) {
				sqlPopupDiv += "<tr>"

						// + "<th width='5%' style='background: #0071c5 none repeat scroll 0 0;color:
						// #FFF;text-align: center'></th>"//delete
						+ "<th width='35%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>"
						+ "Source Column"
						+ "<span style=\"position: absolute;  right: 6px\" onclick=\"deleteSourceColumns(0)\">"
						+ "<img src=\"images/Delete_Red_Icon.svg\" class=\"visionETLIcons\" title=\"Delete Columns\" style=\"width:15px;height: 15px;cursor:pointer;\""
						+ "</span>" + "</th>"// Destination
						+ "<th width='35%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Destination Column</th>"// Source
						// + "<th width='35%' style='background: #0071c5 none repeat scroll 0 0;color:
						// #FFF;text-align: center'>Default Values</th>"
						+ "</tr>" + "<tr>"
						+ "<td width='35%' style='background-color: #f0f2f5;position: sticky; top: 0'><input class='visionColMappingSearch' id='bulkColMappingSearchSource' type='text' onkeyup= bulkColumnMappingSearch(0) placeholder= 'Search' ></td>"// Destination
																																																															// search
						+ "<td width='35%' style='background-color: #f0f2f5;position: sticky; top: 0'><input class='visionColMappingSearch' id='bulkColMappingSearchDestination' type='text' onkeyup= bulkColumnMappingSearch(1) placeholder= 'Search' ></td>"// Source
																																																																// search
						+ "</tr>";

				JSONArray toTableColsArray = (JSONArray) JSONValue.parse(toTableColsArrayStr);
				if (toTableColsArray != null && !toTableColsArray.isEmpty()) {
					for (int i = 0; i < toTableColsArray.size(); i++) {
						JSONObject columnObj = (JSONObject) toTableColsArray.get(i);
						if (columnObj != null && !columnObj.isEmpty() && columnObj.containsKey("parentid")) {
							columnsStr += "<tr>"
									// + "<th width='5%' style='background: #0071c5 none repeat scroll 0 0;color:
									// #FFF;text-align: center'></th>"//delete
									+ "<td width='35%'>"
									// + "<input class='visionColMappingInput' type='text' value=''
									// readonly='true'/>"
									+ "<input class='visionColMappingInput' type='text' value='' />" + "</td>"// Destination
									+ "<td width='35%'>" + "<input class='visionColMappingInput' type='text' value='"
									+ columnObj.get("value") + "' title='" + columnObj.get("value")
									+ "' readonly='true'/>" + "</td>"// Source
									// + "<th width='35%' style='background: #0071c5 none repeat scroll 0 0;color:
									// #FFF;text-align: center'>Default Values</th>"
									+ "</tr>";
						}

					}
				}
			}
			sqlPopupDiv += "" + "</thead>" + "<tbody>" + columnsStr;

			sqlPopupDiv += "</tbody>" + "</table>" + "</div>" + "<div id='sqlToTableTrDiv' style='display:none'>"
					+ "</div>" + "<input type='hidden' id='sqlAllDataTypeObj' value=''/>" + "" + "</div>" + "</div>";
			resultObj.put("sqlPopupDiv", sqlPopupDiv);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject mapTableColumns(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String columnsStr = "";
			String toTableColsArrayStr = request.getParameter("toTableColsArrayStr");
			String fromTableColsArrayStr = request.getParameter("fromTableColsArrayStr");
			String selectionArrayStr = request.getParameter("selection");
			String selectionType = request.getParameter("selectionType");
			if (!(toTableColsArrayStr != null && !"".equalsIgnoreCase(toTableColsArrayStr)
					&& !"null".equalsIgnoreCase(toTableColsArrayStr) && !"[]".equalsIgnoreCase(toTableColsArrayStr))) {
				toTableColsArrayStr = fromTableColsArrayStr;
			}
			List<Map> selectionList = new ArrayList<>();
			if ("ALL".equalsIgnoreCase(selectionType)) {
				if (fromTableColsArrayStr != null && !"".equalsIgnoreCase(fromTableColsArrayStr)
						&& !"null".equalsIgnoreCase(fromTableColsArrayStr)) {
					selectionList = (List<Map>) JSONValue.parse(fromTableColsArrayStr);
				}
			} else {
				if (selectionArrayStr != null && !"".equalsIgnoreCase(selectionArrayStr)
						&& !"null".equalsIgnoreCase(selectionArrayStr)) {
					selectionList = (List<Map>) JSONValue.parse(selectionArrayStr);
				}
			}

			if (toTableColsArrayStr != null && !"".equalsIgnoreCase(toTableColsArrayStr)
					&& !"null".equalsIgnoreCase(toTableColsArrayStr) && !"[]".equalsIgnoreCase(toTableColsArrayStr)) {
				JSONArray toTableColsArray = (JSONArray) JSONValue.parse(toTableColsArrayStr);
				if (toTableColsArray != null && !toTableColsArray.isEmpty()) {
					for (int i = 0; i < toTableColsArray.size(); i++) {
						JSONObject columnObj = (JSONObject) toTableColsArray.get(i);
						if (columnObj != null && !columnObj.isEmpty() && columnObj.containsKey("parentid")) {
							List<Map> sourceMapList = selectionList.stream()
									.filter(sourceColsMap -> (sourceColsMap != null
											&& sourceColsMap.containsKey("parentid")
											&& String.valueOf(columnObj.get("text"))
													.equalsIgnoreCase(String.valueOf(sourceColsMap.get("text")))))
									.collect(Collectors.toList());
							Map sourceMap = new HashMap();
							if (sourceMapList != null && !sourceMapList.isEmpty()) {
								sourceMap = sourceMapList.get(0);
							}
							if (sourceMap != null && !sourceMap.isEmpty()) {
								columnsStr += "<tr>"
										// + "<th width='5%' style='background: #0071c5 none repeat scroll 0 0;color:
										// #FFF;text-align: center'></th>"//delete
										+ "<td width='35%'>"
										+ "<input class='visionColMappingInput' type='text' title='"
										+ ((sourceMap != null && !sourceMap.isEmpty()) ? sourceMap.get("value") : "")
										+ "'" + " value='"
										+ ((sourceMap != null && !sourceMap.isEmpty()) ? sourceMap.get("value") : "")
										+ "' actual-value='"
										+ ((sourceMap != null && !sourceMap.isEmpty()) ? sourceMap.get("id") : "")
										+ "' />" + "</td>"// Destination
										+ "<td width='35%'>"
										+ "<input class='visionColMappingInput' type='text' value='"
										+ columnObj.get("value") + "' actual-value='" + columnObj.get("id")
										+ "' title='" + columnObj.get("value") + "' readonly='true'/>" + "</td>"// Source
										// + "<th width='35%' style='background: #0071c5 none repeat scroll 0 0;color:
										// #FFF;text-align: center'>Default Values</th>"
										+ "</tr>";
							} else {
								columnsStr += "<tr>"
										// + "<th width='5%' style='background: #0071c5 none repeat scroll 0 0;color:
										// #FFF;text-align: center'></th>"//delete
										+ "<td width='35%'>"
										+ "<input class='visionColMappingInput' type='text' title='' value=''/>"
										+ "</td>"// Destination
										+ "<td width='35%'>"
										+ "<input class='visionColMappingInput' type='text' value='"
										+ columnObj.get("value") + "' actual-value='" + columnObj.get("id")
										+ "' title='" + columnObj.get("value") + "' readonly='true'/>" + "</td>"// Source
										// + "<th width='35%' style='background: #0071c5 none repeat scroll 0 0;color:
										// #FFF;text-align: center'>Default Values</th>"
										+ "</tr>";
							}

						}

					}
				}
			} else if (fromTableColsArrayStr != null && !"".equalsIgnoreCase(fromTableColsArrayStr)
					&& !"null".equalsIgnoreCase(fromTableColsArrayStr)) {
				JSONArray fromTableColsArray = (JSONArray) JSONValue.parse(fromTableColsArrayStr);
				if (fromTableColsArray != null && !fromTableColsArray.isEmpty()) {
					for (int i = 0; i < fromTableColsArray.size(); i++) {
						JSONObject columnObj = (JSONObject) fromTableColsArray.get(i);
						if (columnObj != null && !columnObj.isEmpty() && columnObj.containsKey("parentid")) {
							columnsStr += "<tr>"
									// + "<th width='5%' style='background: #0071c5 none repeat scroll 0 0;color:
									// #FFF;text-align: center'></th>"//delete
									+ "<td width='35%'>" + "<input class='visionColMappingInput' type='text' value='"
									+ columnObj.get("value") + "' actual-value='" + columnObj.get("id") + "' title='"
									+ columnObj.get("value") + "' readonly='true'/>" + "</td>"// Destination
									// + "<th width='35%'></th>"//Source
									// + "<th width='35%' style='background: #0071c5 none repeat scroll 0 0;color:
									// #FFF;text-align: center'>Default Values</th>"
									+ "</tr>";

						}

					}
				}
			}
			resultObj.put("columnsStr", columnsStr);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

//    public JSONObject importSAOPService(HttpServletRequest request) {
//        JSONObject resultObj = new JSONObject();
//        try {
//            String wsdlURL = request.getParameter("wsdlURL");
//            String endPointURL = request.getParameter("endPointURL");
//            if (wsdlURL != null
//                    && !"".equalsIgnoreCase(wsdlURL)
//                    && !"null".equalsIgnoreCase(wsdlURL)
//                    && endPointURL != null
//                    && !"".equalsIgnoreCase(endPointURL)
//                    && !"null".equalsIgnoreCase(endPointURL)) {
//                int responseCode = new PilogUtilities().webServiceUp(endPointURL);
//                if (responseCode == 200
//                        || responseCode == 405
//                        || responseCode == 307) {
//                    WsdlProject project = new WsdlProject();
//                    WsdlInterface[] wsdls = WsdlImporter.importWsdl(project, wsdlURL);
//                    WsdlInterface wsdl = wsdls[0];
//                    Operation operation = wsdl.getOperationAt(0);
//                    WsdlOperation wsdlOperation = (WsdlOperation) operation;
//                    String requestXMLStr = wsdlOperation.createRequest(true);
//                    System.out.println("Request:\n" + requestXMLStr);
//                    resultObj.put("request", requestXMLStr);
//                    resultObj.put("response", wsdlOperation.createResponse(true));
//                    resultObj.put("messageFlag", true);
//                    String message = ""
//                            + "<div id=\"soapSplitterDiv\">"
//                            + "<div>"
//                            + "  <div id=\"soapRequestSpliterDiv\">"
//                            + "     <div id=\"soapRequestDiv\">"
//                            + ""
//                            + "         <textarea rows=\"10\" id=\"soapRequestInput\" style=\"width:100%;height: 100%;text-transform: none;\">" + requestXMLStr + "</textarea>"
//                            + ""
//                            + "     </div>"//soapRequestDiv
//                            + "      <div id='soapRequestPropDiv'>"
//                            + "         <table id='soapRequestPropTable' width='100%'>"
//                            + "             <tr>"
//                            + "                 <th style='width:25%;'>End Point URL</th>"
//                            + "                 <th style='width:75%;'><input type='text' value='" + endPointURL + "' style='width:90%;'/></th>"
//                            + "             </tr>"
//                            + "             <tr>"
//                            + "                  <th style='width:25%;'>User Name</th><th style='width:75%;'><input type='text' value='' style='width:90%;'/></th>"
//                            + "             </tr>"
//                            + "             <tr>"
//                            + "                 <th style='width:25%;'>Password</th><th style='width:75%;'><input type='password' value='' style='width:90%;'/></th>"
//                            + "             </tr>"
//                            + "         </table>"
//                            + "        </div>"//soapRequestPropDiv
//                            + "     </div>"//soapRequestSpliterDiv
//                            + " </div>"//
//                            + " <div id=\"soapResponseDiv\">"
//                            + "     <textarea rows=\"10\" id=\"soapResponseInput\" style=\"width:100%;height: 100%;text-transform: none;\"></textarea>"
//                            + " </div>"//soapResponseDiv
//                            + "</div>";//soapSplitterDiv
//                    resultObj.put("message", message);
////                    System.out.println("\nResponse:\n" + wsdlOperation.createResponse(true));
//                } else {
//                    String message = "Web Service Down";
//                    System.err.println("******Web Service Down********Response Code::::" + responseCode);
//                    BApplProperties applProperties = (BApplProperties) genericDataPipingDAO.getApplProperties(String.valueOf(responseCode));
//                    //  System.out.println("applProperties:::"+applProperties);
//                    if (applProperties != null) {
//                        message = applProperties.getId().getProcessValue() + ":" + applProperties.getHeader();
//                    } else {
//                        message = "Web Service Down";
//                    }
//                    resultObj.put("message", message);
//                    resultObj.put("messageFlag", false);
//                }
//            } else {
//                resultObj.put("message", "WSDL URL/End Point URL not found");
//                resultObj.put("messageFlag", false);
//            }
//        } catch (Exception e) {
//            resultObj.put("message", e.getMessage());
//            resultObj.put("messageFlag", false);
//            e.printStackTrace();
//        }
//        return resultObj;
//    }
	public JSONObject getFileObjectMetaData(HttpServletRequest request, HttpServletResponse response) {
		JSONObject fileMetaObj = new JSONObject();
		try {
			JSONArray dataFieldsArray = new JSONArray();
			JSONArray columnsArray = new JSONArray();
			String filePath = request.getParameter("filePath");

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
			String fileType = request.getParameter("fileType");
			List<String> headers = dataMigrationService.getHeadersOfImportedFile(request, response, filePath);
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
						dataFieldsObj.put("type", "string");

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

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return fileMetaObj;
	}

	public List getFileObjectData(HttpServletRequest request, HttpServletResponse response) {
		List dataList = new ArrayList();
		try {
			String filePath = request.getParameter("filePath");
			if (filePath != null && !"".equalsIgnoreCase(filePath)) {

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
				// C:/Files/TreeDMImport/SAN_MGR_MM
//                filePath = etlFilePath+"Files/TreeDMImport" + File.separator + request.getSession(false).getAttribute("ssUsername") + File.separator + filePath;
			}
			String fileName = request.getParameter("fileName");
			String fileType = request.getParameter("fileType");
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
			} else if (".pdf".equalsIgnoreCase(fileType)) {
				dataList = readPDF(request, response, filePath, columnList);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return dataList;
	}

	public List readPDF(HttpServletRequest request, HttpServletResponse response, String filePath,
			List<String> columnList) {

		FileInputStream fis = null;

		System.out.println("Start Date And Time :::" + new Date());
		List dataList = new ArrayList();
		List pdfdataList = new ArrayList();
		int rowVal = 1;
		try {
			// dataList = dataMigrationService.readPDFRestApi(request, filePath);
			String result = dataMigrationService.readPDFRestApi(request, filePath);
			JSONObject apiPdfJsonData = (JSONObject) JSONValue.parse(result);
			List<String> headerArray = (List<String>) apiPdfJsonData.get("columns");
			dataList = (List) apiPdfJsonData.get("data");
			if (dataList != null && dataList.get(0) instanceof LinkedHashMap) {
				((LinkedHashMap) dataList.get(0)).put("totalrecords", dataList.size());
				pdfdataList = dataList;
			} else if (dataList != null && dataList.get(0) instanceof JSONArray) {
				for (int i = 0; i < dataList.size(); i++) {
					JSONArray rowArray = (JSONArray) dataList.get(i);
					JSONObject rowData = new JSONObject();
					for (int j = 0; j < headerArray.size(); j++) {
						if (headerArray.get(j) != null) {
							rowData.put(headerArray.get(j), rowArray.get(j));
						}

					}
					if (i == 0) {
						rowData.put("totalrecords", dataList.size());
					}
					pdfdataList.add(rowData);
				}
				// ((JSONArray) dataList.get(0)).add("totalrecords", dataList.size());
			}

//            for (int i = 0; i < resultArrayList.size(); i++) {
//                LinkedHashMap resultObj = (LinkedHashMap) resultArrayList.get(i);
//                Object[] rowData = resultObj.values().toArray();
//                dataList.add(rowData);
//            }
		} catch (Exception e) {
			e.printStackTrace();

		}

		return pdfdataList;
	}

	public List readExcel(HttpServletRequest request, HttpServletResponse response, String filepath,
			List<String> columnList) {

		// FileInputStream fis = null;
		System.out.println("Start Date And Time :::" + new Date());
		List dataList = new ArrayList();
		int rowVal = 1;
		try {
			if (true) {
				// fis = new FileInputStream(new File(filepath));

				// Workbook workBook = null;
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
				// workBook = WorkbookFactory.create(new File(filepath));
				try (Workbook workBook = WorkbookFactory.create(new File(filepath))) {
					System.out.println("After::fileInputStream::" + new Date());
					if (workBook.getSheetAt(sheetNo) instanceof XSSFSheet) {
						sheet = (XSSFSheet) workBook.getSheetAt(sheetNo);
					} else if (workBook.getSheetAt(sheetNo) instanceof HSSFSheet) {
						sheet = (HSSFSheet) workBook.getSheetAt(sheetNo);
					}
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
					String pagesize = request.getParameter("pagesize") != null ? request.getParameter("pagesize")
							: "10";
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
							&& !"null".equalsIgnoreCase(recordendindex)
							&& Integer.parseInt(recordendindex) <= rowCount) {
						endIndex = Integer.parseInt(recordendindex) + 1;
					}
					for (int i = rowVal; i < endIndex; i++) {
						Row row = sheet.getRow(i);
						stmt = 1;
						JSONObject dataObject = new JSONObject();
						dataObject.put("totalrecords", rowCount);
						for (int cellIndex = 0; cellIndex < row.getLastCellNum(); cellIndex++) {

							try {
								System.out.println("cellIndex::::" + cellIndex);
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
										boolean booleanCellValue = cell.getBooleanCellValue();
										dataObject.put(columnList.get(cellIndex), booleanCellValue);
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
												if ((cellDate.getYear() + 1900) == 1899
														&& (cellDate.getMonth() + 1) == 12
														&& (cellDate.getDate()) == 31) {
													cellDateString = (cellDate.getHours()) + ":"
															+ (cellDate.getMinutes()) + ":" + (cellDate.getSeconds());
//                                                    System.out.println("cellDateString :: "+cellDateString);
												} else {
													cellDateString = (cellDate.getYear() + 1900) + "-"
															+ (cellDate.getMonth() + 1) + "-" + (cellDate.getDate())
															+ " " + (cellDate.getHours()) + ":"
															+ (cellDate.getMinutes()) + ":" + (cellDate.getSeconds());
												}

//                                                String cellDateString = (cellDate.getYear() + 1900) + "-" + (cellDate.getMonth() + 1) + "-" + (cellDate.getDate());
												dataObject.put(columnList.get(cellIndex), cellDateString);
											}

										} else {
											String cellvalStr = NumberToTextConverter
													.toText(cell.getNumericCellValue());
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
				} catch (Exception e) {
					e.printStackTrace();

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

	/*
	 * public List readXML(HttpServletRequest request, HttpServletResponse response,
	 * String filepath, List<String> columnList) { FileInputStream fis = null; List
	 * dataList = new ArrayList(); try { int rowCount = 0; String fileExtension =
	 * filepath.substring(filepath.lastIndexOf(".") + 1, filepath.length());
	 * System.out.println("fileExtension:::" + fileExtension);
	 * 
	 * DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	 * DocumentBuilder builder = factory.newDocumentBuilder(); Document document =
	 * builder.parse(new FileInputStream(filepath), "UTF-8");
	 * document.getDocumentElement().normalize(); Element root =
	 * document.getDocumentElement();
	 * 
	 * if (root.hasChildNodes() && root.getChildNodes().getLength() > 1) { // nested
	 * childs String evaluateTagName = "/" + root.getTagName(); NodeList rootList =
	 * root.getChildNodes(); if
	 * (!"#Text".equalsIgnoreCase(rootList.item(0).getNodeName())) { evaluateTagName
	 * += "/" + rootList.item(0).getNodeName(); } else { evaluateTagName += "/" +
	 * rootList.item(1).getNodeName(); }
	 * 
	 * System.out.println("evaluateTagName:::" + evaluateTagName); XPath xpath =
	 * XPathFactory.newInstance().newXPath(); NodeList dataNodeList = (NodeList)
	 * xpath.evaluate(evaluateTagName, // NodeList nList = (NodeList)
	 * xpath.evaluate("/PiLog_Data_Export/Item", document, XPathConstants.NODESET);
	 * 
	 * if (dataNodeList != null && dataNodeList.getLength() != 0) { rowCount =
	 * dataNodeList.getLength(); String pagenum = request.getParameter("pagenum");
	 * String pagesize = request.getParameter("pagesize") != null ?
	 * request.getParameter("pagesize") : "10"; String recordendindex =
	 * request.getParameter("recordendindex"); String recordstartindex =
	 * (request.getParameter("recordstartindex")); // need to write logic for
	 * extraction from File int startIndex = 0; if (recordstartindex != null &&
	 * !"".equalsIgnoreCase(recordstartindex) &&
	 * !"null".equalsIgnoreCase(recordstartindex) &&
	 * !"0".equalsIgnoreCase(recordstartindex)) { startIndex =
	 * Integer.parseInt(recordstartindex); } int endIndex = rowCount; if
	 * (recordendindex != null && !"".equalsIgnoreCase(recordendindex) &&
	 * !"null".equalsIgnoreCase(recordendindex) && Integer.parseInt(recordendindex)
	 * <= rowCount) { endIndex = Integer.parseInt(recordendindex); } int skipLines =
	 * 0; if (pagenum != null && !"".equalsIgnoreCase(pagenum) &&
	 * !"null".equalsIgnoreCase(pagenum) && !"1".equalsIgnoreCase(pagenum) &&
	 * pagesize != null && !"".equalsIgnoreCase(pagesize) &&
	 * !"null".equalsIgnoreCase(pagesize)) { skipLines = Integer.parseInt(pagenum) *
	 * Integer.parseInt(pagesize); } // Node headerNode = dataNodeList.item(0); //
	 * if (headerNode.getNodeType() == Node.ELEMENT_NODE) { // NodeList
	 * headerChildNodeList = headerNode.getChildNodes(); // int index = 0; // for
	 * (int i = 0; i < headerChildNodeList.getLength(); i++) {// Columns // Node
	 * childNode = headerChildNodeList.item(i); // if (childNode != null // &&
	 * childNode.getNodeType() == Node.ELEMENT_NODE) { //
	 * headerData.put(childNode.getNodeName(), i); // // } // }// end of columns
	 * loop // // } for (int temp = startIndex; temp < endIndex; temp++) {// Rows
	 * Node node = dataNodeList.item(temp); JSONObject dataObject = new
	 * JSONObject(); dataObject.put("totalrecords", rowCount); if
	 * (node.getNodeType() == Node.ELEMENT_NODE) { NodeList childNodeList =
	 * node.getChildNodes(); for (int j = 0; j < columnList.size(); j++) { try { int
	 * childNodeIndex = j; int nodeListLength = childNodeList.getLength(); if
	 * (childNodeIndex <= (childNodeList.getLength() - 1)) { Node childNode =
	 * childNodeList.item(childNodeIndex); if (childNode != null) { if (childNode !=
	 * null && childNode.getNodeType() == Node.ELEMENT_NODE) { try { if
	 * (childNode.getTextContent() != null &&
	 * !"".equalsIgnoreCase(childNode.getTextContent()) &&
	 * !"null".equalsIgnoreCase(childNode.getTextContent())) {
	 * dataObject.put(columnList.get(j), childNode.getTextContent());
	 * 
	 * } else { dataObject.put(columnList.get(j), ""); }
	 * 
	 * } catch (Exception e) { dataObject.put(columnList.get(j), ""); continue; } //
	 * Need to set the Data
	 * 
	 * } } else { dataObject.put(columnList.get(j), ""); } } else {
	 * dataObject.put(columnList.get(j), ""); } } catch (Exception e) {
	 * dataObject.put(columnList.get(j), ""); continue; }
	 * 
	 * }// column list loop
	 * 
	 * } dataList.add(dataObject); }// end of rows loop
	 * 
	 * } } else { System.err.println("*** Root Element Not Found ****"); }
	 * 
	 * if (fis != null) { fis.close(); } } catch (Exception e) {
	 * e.printStackTrace();
	 * 
	 * }
	 * 
	 * return dataList; }
	 */

	public List readXML(HttpServletRequest request, HttpServletResponse response, String filepath,
			List<String> columnList) {
		FileInputStream fis = null;
		List dataList = new ArrayList();
		try {
			int rowCount = 0;
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document document = builder.parse(new FileInputStream(filepath), "UTF-8");
			document.getDocumentElement().normalize();
			Element root = document.getDocumentElement();

			if (root.hasChildNodes() && root.getChildNodes().getLength() > 1) {
				// Build XPath expression
				String evaluateTagName = "/" + root.getTagName();
				NodeList rootList = root.getChildNodes();
				if (!"#Text".equalsIgnoreCase(rootList.item(0).getNodeName())) {
					evaluateTagName += "/" + rootList.item(0).getNodeName();
				} else {
					evaluateTagName += "/" + rootList.item(1).getNodeName();
				}

				XPath xpath = XPathFactory.newInstance().newXPath();
				NodeList dataNodeList = (NodeList) xpath.evaluate(evaluateTagName, document, XPathConstants.NODESET);

				if (dataNodeList != null && dataNodeList.getLength() != 0) {
					rowCount = dataNodeList.getLength();

					// Get pagination parameters
					String recordstartindex = request.getParameter("recordstartindex");
					String recordendindex = request.getParameter("recordendindex");

					// Calculate start index
					int startIndex = 0;
					if (recordstartindex != null && !recordstartindex.trim().isEmpty()
							&& !"null".equalsIgnoreCase(recordstartindex) && !"0".equalsIgnoreCase(recordstartindex)) {
						startIndex = Integer.parseInt(recordstartindex);
					}

					// Calculate end index
					int endIndex = rowCount;
					if (recordendindex != null && !recordendindex.trim().isEmpty()
							&& !"null".equalsIgnoreCase(recordendindex)
							&& Integer.parseInt(recordendindex) <= rowCount) {
						endIndex = Integer.parseInt(recordendindex);
					}

					// Process each row
					for (int temp = startIndex; temp < endIndex; temp++) {
						Node node = dataNodeList.item(temp);
						if (node.getNodeType() == Node.ELEMENT_NODE) {
							JSONObject dataObject = new JSONObject();
							dataObject.put("totalrecords", rowCount);

							// Create a map of child nodes by name for efficient lookup
							Map<String, Node> nodeMap = new HashMap<>();
							NodeList childNodeList = node.getChildNodes();
							for (int i = 0; i < childNodeList.getLength(); i++) {
								Node childNode = childNodeList.item(i);
								if (childNode.getNodeType() == Node.ELEMENT_NODE) {
									nodeMap.put(childNode.getNodeName(), childNode);
								}
							}

							// Process each column using the node map
							for (String columnName : columnList) {
								Node matchingNode = nodeMap.get(columnName);
								if (matchingNode != null && matchingNode.getTextContent() != null
										&& !matchingNode.getTextContent().trim().isEmpty()
										&& !"null".equalsIgnoreCase(matchingNode.getTextContent())) {
									dataObject.put(columnName, matchingNode.getTextContent().trim());
								} else {
									dataObject.put(columnName, "");
								}
							}

							dataList.add(dataObject);
						}
					}
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

	public String buildExcelFilterCondition(int filterscount, HttpServletRequest request) {
		String conditionQuery = "";
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");

			for (int i = 0; i < filterscount; i++) {
				String columnName = request.getParameter("filterdatafield" + i);
				if (columnName != null && !"".equalsIgnoreCase(columnName) && !"null".equalsIgnoreCase(columnName)
						&& columnName.endsWith("_DLOV")) {
					columnName = columnName.replace("_DLOV", "");
				}
				String condition = request.getParameter("filtercondition" + i);
				String value = request.getParameter("filtervalue" + i);
				String filteroperator = request.getParameter("filteroperator" + i);
//                //System.out.println("columnName::::" + columnName);
//                //System.out.println("filtercondition::::" + condition);
//                //System.out.println("filtervalue::::" + value);
//                //System.out.println("filteroperator::::" + filteroperator);
				String condtionQuery = "";

				value = value.toUpperCase();
				System.out.println("value::::Before:::" + value);
//                if (columnName.contains("DATE")) {
//                    // value = "TO_DATE('" + value + "', 'MM/DD/YYYY')";
//                    if (value.contains("GMT")) {
//                        value = value.substring(0, value.indexOf("GMT") - 9).trim();
//                        if (dataBaseDriver != null && !"".equalsIgnoreCase(dataBaseDriver)) {
//                            if (dataBaseDriver.toUpperCase().contains("ORACLE")) {
//                                columnName = "TO_DATE(TO_CHAR(" + columnName + ",'DY MON DD YYYY'), 'DY MON DD YYYY')";
//                                value = "TO_DATE('" + value + "','DY MON DD YYYY')";
//                            } else if (dataBaseDriver.toUpperCase().contains("MYSQL")) {
//                                columnName = "STR_TO_DATE(DATE_FORMAT(" + columnName + ",'DY MON DD YYYY'), 'DY MON DD YYYY')";
//                                value = "STR_TO_DATE('" + value + "','DY MON DD YYYY')";
//                            } else if (dataBaseDriver.toUpperCase().contains("SQLSERVER")) {
//                                columnName = "CONVERT(CONVERT(VARCHAR(10)," + columnName + ",'DY MON DD YYYY'), 'DY MON DD YYYY')";
//                                value = "CONVERT('" + value + "','DY MON DD YYYY')";
//                            } else if (dataBaseDriver.toUpperCase().contains("DB2")) {
//
//                            }
//
//                        }
//                    } else {
//                        value = value.substring(0, value.indexOf(" ")).trim();
//                        System.out.println("value:::" + value);
//                        if (dataBaseDriver != null && !"".equalsIgnoreCase(dataBaseDriver)) {
//                            if (dataBaseDriver.toUpperCase().contains("ORACLE")) {
//                                columnName = "TO_DATE(TO_CHAR(" + columnName + ",'DD-MM-YY'), 'DD-MM-YY')";
//                                value = "TO_DATE('" + value + "','DD-MM-YY')";
//                            } else if (dataBaseDriver.toUpperCase().contains("MYSQL")) {
//                                columnName = "STR_TO_DATE(DATE_FORMAT(" + columnName + ",'DD-MM-YY'), 'DD-MM-YY')";
//                                value = "STR_TO_DATE('" + value + "','DD-MM-YY')";
//                            } else if (dataBaseDriver.toUpperCase().contains("SQLSERVER")) {
//                                columnName = "CONVERT(CONVERT(VARCHAR(10)," + columnName + ",'DD-MM-YY'), 'DD-MM-YY')";
//                                value = "CONVERT('" + value + "','DD-MM-YY')";
//                            } else if (dataBaseDriver.toUpperCase().contains("DB2")) {
//
//                            }
//
//                        }
//                    }
//
//                }

//                //System.out.println("value::::After:::" + value);
				if (condition != null && !"".equalsIgnoreCase(condition)) {

					String query = "";
					switch (condition) {
					case "CONTAINS":
//                            query = " " + columnName + " LIKE '%" + splitValue(value) + "%'";
						query = "UPPER(" + columnName + ") LIKE '%" + splitValue(value) + "%'";
						break;

					case "DOES_NOT_CONTAIN":
						query = " " + columnName + " NOT LIKE '%" + splitValue(value) + "%'";
						break;

					case "EQUAL":
						query = " " + columnName + " = '" + value + "'";
						break;
					case "NOT_EQUAL":
						query = " " + columnName + " != '" + value + "'";
						break;

					case "GREATER_THAN":
						query = " " + columnName + " > '" + value + "'";
						break;
					case "LESS_THAN":
						query = " " + columnName + " < '" + value + "'";
						break;

					case "STARTS_WITH":
						query = " " + columnName + " LIKE '" + value + "%'";

						break;
					case "ENDS_WITH":
						query = " " + columnName + " LIKE '%" + value + "'";
						break;

					case "NULL":
						query = " " + columnName + " IS  NULL";
						break;
					case "NOT_NULL":
						query = " " + columnName + " IS NOT NULL";
						break;
					case "GREATER_THAN_OR_EQUAL":
						query = " " + columnName + " >= " + value + "";
						break;
					case "LESS_THAN_OR_EQUAL":
						query = " " + columnName + " <= " + value + "";
						break;

					}
					if (query != null && !"".equalsIgnoreCase(query)) {
						conditionQuery += query;
						if (i != filterscount - 1) {
							conditionQuery += " AND ";
						}
					}
				}
				// query = query + " AND " + getCondition(filterdatafield, filtercondition,
				// filtervalue);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return conditionQuery;
	}

	public String splitValue(String value) {

		try {
			System.err.println("value:::Before:::" + value);
			if (value != null && !"".equalsIgnoreCase(value)) {
				value = value.replaceAll(" ", "%");
			}
			System.err.println("value:::After:::" + value);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return value;
	}

	public JSONObject generateColumnMapping(HttpServletRequest request, File file) {
		JSONObject columnMappingObj = new JSONObject();
		try {
			String toIconType = request.getParameter("toIconType");
			String colMapStr = "";
			Workbook workBook = null;
			Sheet sheet = null;
			String filepath = file.getAbsolutePath();
			String fileExtension = filepath.substring(filepath.lastIndexOf(".") + 1, filepath.length());
			System.out.println("fileExtension:::" + fileExtension);
//            if (fileExtension != null && "xls".equalsIgnoreCase(fileExtension)) { // commented by PKH
//                workBook = WorkbookFactory.create(file);
//                sheet = (HSSFSheet) workBook.getSheetAt(0);
//            } else {
			System.out.println("Before::::" + new Date());
			workBook = WorkbookFactory.create(file);
			System.out.println("After::fileInputStream::" + new Date());
			if (workBook.getSheetAt(0) instanceof XSSFSheet) {
				sheet = (XSSFSheet) workBook.getSheetAt(0);
			} else if (workBook.getSheetAt(0) instanceof HSSFSheet) {
				sheet = (HSSFSheet) workBook.getSheetAt(0);
			}
//            }
			int lastRowNo = sheet.getLastRowNum();
			System.out.println("lastRowNo::::" + lastRowNo);
			int firstRowNo = sheet.getFirstRowNum();
			System.out.println("firstRowNo::::" + firstRowNo);
			int rowCount = lastRowNo - firstRowNo;
			System.out.println("rowCount:::::" + rowCount);
			for (int i = 1; i < rowCount + 1; i++) {
				Row row = sheet.getRow(i);
				colMapStr += "<tr>"
						+ "<td width='1%' class=\"visionColMappingImgTd1\" ><img src=\"images/Delete_Red_Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionColMappingImg\""
						+ " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>" + "</td>";
				Cell destTableCell = row.getCell(2);
				Cell destTableColumnCell = row.getCell(3);
				String destTable = getCellValue(destTableCell);
				String destinationCol = "";
				if (destTable != null && !"".equalsIgnoreCase(destTable) && !"null".equalsIgnoreCase(destTable)) {
					String destTableColumn = getCellValue(destTableColumnCell);
					if (destTableColumn != null && !"".equalsIgnoreCase(destTableColumn)
							&& !"null".equalsIgnoreCase(destTableColumn)) {
						destinationCol = destTable + ":" + destTableColumn;
					}
				}
				colMapStr += "<td width='19%' style='"
						+ ((toIconType != null && !"".equalsIgnoreCase(toIconType)
								&& !"null".equalsIgnoreCase(toIconType) && !"SQL".equalsIgnoreCase(toIconType))
										? "display:none;"
										: "")
						+ "'>" + "<input class='visionColMappingInput' type='text' value='" + destinationCol
						+ "' title='" + destinationCol + "' readonly='true'/>"
						+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
						+ " onclick=\"selectColumn(this,'toColumn')\" style=\"\"></td>";

				Cell sourceTableCell = row.getCell(0);
				Cell sourceTableColumnCell = row.getCell(1);
				String sourceTable = getCellValue(sourceTableCell);
				String sourceColStr = "";
				if (sourceTable != null && !"".equalsIgnoreCase(sourceTable) && !"null".equalsIgnoreCase(sourceTable)) {
					String sourceTableColumn = getCellValue(sourceTableColumnCell);
					if (sourceTableColumn != null && !"".equalsIgnoreCase(sourceTableColumn)
							&& !"null".equalsIgnoreCase(sourceTableColumn)) {
						sourceColStr = sourceTable + ":" + sourceTableColumn;
					}
				}
				Cell defaultValueCell = row.getCell(4);
				String defaultValue = getCellValue(defaultValueCell);
				colMapStr += "<td width='20%'>" + "<input class='visionColMappingInput' type='text' value='"
						+ sourceColStr + "' title='" + sourceColStr + "' readonly='true'/>"
						+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
						+ " onclick=\"selectColumn(this,'fromColumn')\" style=\"\"></td>"
						+ "<td width='20%'><input class='visionColMappingTextarea' type='text' value='" + defaultValue
						+ "' ></td>"
						+ "<td width='20%' style='display:none;'><input class='visionColMappingTextarea' type='text' value=''></td>"
						+ "<td width='20%'><input class='visionColMappingInput' id='visionETLFuncChildColId' type='text' value=''>"
						+ "<img title='Select Function' src=\"images/Fx icon-01.svg\" class=\"visionETLColMapImage \" "
						+ " onclick=\"selectColumnFun(this,'fromColumn')\" style=\"\">" + "</td>" + "</tr>";

			} // row end
			columnMappingObj.put("message", colMapStr);
			columnMappingObj.put("flag", true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return columnMappingObj;
	}

	public String getCellValue(Cell cell) {
		String cellValue = "";
		try {
			if (cell != null) {
				switch (cell.getCellType()) {
				case Cell.CELL_TYPE_STRING:
					cellValue = cell.getStringCellValue();

					break;
				case Cell.CELL_TYPE_BOOLEAN:
					cellValue = String.valueOf(cell.getBooleanCellValue());

					break;

				case Cell.CELL_TYPE_NUMERIC:
					cellValue = NumberToTextConverter.toText(cell.getNumericCellValue());
					if (cellValue != null && !"".equalsIgnoreCase(cellValue) && !"".equalsIgnoreCase(cellValue)
							&& !"".equalsIgnoreCase(cellValue)) {
						cellValue = String.valueOf(Integer.parseInt(cellValue));
					}
					break;
				case Cell.CELL_TYPE_BLANK:
					cellValue = "";
					break;
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return cellValue;
	}

	public JSONObject selectSapTableColumns(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();

		try {
			String connObj = request.getParameter("connectionObj");
			String tableName = request.getParameter("parentkey");
			if (connObj != null && !"".equalsIgnoreCase(connObj) && !"null".equalsIgnoreCase(connObj)) {
				JSONObject connectionObj = (JSONObject) JSONValue.parse(connObj);
				Object connectionObject = getConnection(connectionObj);
				if (connectionObject instanceof JCO.Client) {
					resultObj = genericDataPipingDAO.selectSapTableColumns(request, (JCO.Client) connectionObject,
							tableName);
				}
			}
		} catch (Exception e) {
		}
		return resultObj;
	}

	public JSONObject processETLScheduleJob(String jobId, String orgnId, String sessionUserName,
			String processJobDataStr, String mappedDataStr) {
		JSONObject resultObj = new JSONObject();
		Connection fromConnection = null;
		Connection toConnection = null;
		JCO.Client fromJCOConnection = null;
		JCO.Client toJCOConnection = null;

		try {

			try {
				genericDataPipingDAO.deleteProcesslog(sessionUserName, orgnId, jobId);
			} catch (Exception e) {
			}
			try {
				genericProcessETLDataService.processETLLog(sessionUserName, orgnId, "ETL Process is started", "INFO",
						20, "Y", jobId);
			} catch (Exception e) {
			}

			JSONObject processJobData = (JSONObject) JSONValue.parse(processJobDataStr);

			// JSONObject columnsObj = new JSONObject();
			Map columnsObj = new LinkedHashMap(); // ravi etl integration
			JSONObject tablesWhereClauseObj = new JSONObject();
			JSONObject defaultValuesObj = new JSONObject();
			JSONObject appendValObj = new JSONObject();
			JSONObject selectTabObj = new JSONObject();
			JSONObject columnClauseObj = new JSONObject();
			JSONObject joinQueryMapObj = new JSONObject();
			JSONObject orderByObj = new JSONObject();

			String currentTrnsOpId = (String) processJobData.get("currentTrnsOpId");
			String groupByStr = (String) processJobData.get("groupBy");
			String nativeSQL = (String) processJobData.get("nativeSQL");
			String joinQuery = (String) processJobData.get("joinQuery");

			String columnsObjStr = (String) processJobData.get("columnsObj");
			if (columnsObjStr != null && !"".equalsIgnoreCase(columnsObjStr)) {
				columnsObj = (JSONObject) JSONValue.parse(columnsObjStr);
			}

			String tablesWhereClauseObjStr = (String) processJobData.get("whereClauseObj");
			if (tablesWhereClauseObjStr != null && !"".equalsIgnoreCase(tablesWhereClauseObjStr)) {
				tablesWhereClauseObj = (JSONObject) JSONValue.parse(tablesWhereClauseObjStr);
			}

			String defaultValuesObjStr = (String) processJobData.get("defaultValObj");
			if (defaultValuesObjStr != null && !"".equalsIgnoreCase(defaultValuesObjStr)) {
				defaultValuesObj = (JSONObject) JSONValue.parse(defaultValuesObjStr);
			}

			String appendValObjStr = (String) processJobData.get("appendValObj");
			if (appendValObjStr != null && !"".equalsIgnoreCase(appendValObjStr)) {
				appendValObj = (JSONObject) JSONValue.parse(appendValObjStr);
			}

			String columnClauseObjStr = (String) processJobData.get("columnClauseObj");
			if (columnClauseObjStr != null && !"".equalsIgnoreCase(columnClauseObjStr)) {
				columnClauseObj = (JSONObject) JSONValue.parse(columnClauseObjStr);
			}

			String selectTabObjStr = (String) processJobData.get("selectTabObj");
			if (selectTabObjStr != null && !"".equalsIgnoreCase(selectTabObjStr)) {
				selectTabObj = (JSONObject) JSONValue.parse(selectTabObjStr);
			}

			if (!(selectTabObj != null && !selectTabObj.isEmpty())) {
				selectTabObj = new JSONObject();
			}
			selectTabObj.put("jobId", jobId);

			String joinQueryMapObjStr = (String) processJobData.get("joinQueryMapObj");
			if (joinQueryMapObjStr != null && !"".equalsIgnoreCase(joinQueryMapObjStr)) {
				joinQueryMapObj = (JSONObject) JSONValue.parse(joinQueryMapObjStr);
			}

			String orderByStr = (String) processJobData.get("orderBy");
			if (orderByStr != null && !"".equalsIgnoreCase(orderByStr)) {
				orderByObj = (JSONObject) JSONValue.parse(orderByStr);
			}

			if (mappedDataStr != null && !"".equalsIgnoreCase(mappedDataStr)) {
				Map mappedData = (Map) JSONValue.parse(mappedDataStr);
				Map mappingOperatorObj = new HashMap();
				Map nonMapOperatorObj = new HashMap();
				if (mappedData != null && !mappedData.isEmpty()) {
					Map operatorsMap = (Map) mappedData.get("operators");
					Map linksMap = (Map) mappedData.get("links");
//                    System.out.println("operatorsMap::::" + operatorsMap);
//                    System.out.println("linksMap::::" + linksMap);
					if (operatorsMap != null && !operatorsMap.isEmpty()) {
						mappingOperatorObj = (Map) operatorsMap.keySet().stream()
								.filter(keyName -> (keyName != null && (Map) operatorsMap.get(keyName) != null
										&& ("MAP".equalsIgnoreCase(
												String.valueOf(((Map) operatorsMap.get(keyName)).get("iconType"))))
										|| ("UNGROUP".equalsIgnoreCase(
												String.valueOf(((Map) operatorsMap.get(keyName)).get("iconType")))) // RAVI
																													// NORMALIZE
										|| ("GROUP".equalsIgnoreCase(
												String.valueOf(((Map) operatorsMap.get(keyName)).get("iconType"))))))
								// ((Map) operatorsMap.get(keyName))
								// .containsKey("mapType")))
								.collect(Collectors.toMap(keyName -> keyName,
										keyName -> (Map) operatorsMap.get(keyName)));

//                        System.out.println("mappingOperatorObj:::" + mappingOperatorObj);
//                        System.out.println("nonMamappingOperatorObj:::pOperatorObj:::" + nonMapOperatorObj);
						if (mappingOperatorObj != null && !mappingOperatorObj.isEmpty()) {

							for (Object mappedKey : mappingOperatorObj.keySet()) {
								List<Map> fromOperatorList = new ArrayList<>();
								if (linksMap != null && !linksMap.isEmpty()) {
									Map fromOperator = new HashMap();
									Map toOperator = new HashMap();

									for (Object linkKey : linksMap.keySet()) {
										Map linkMap = (Map) linksMap.get(linkKey);
										if (linkMap != null && !linkMap.isEmpty() && String.valueOf(mappedKey)
												.equalsIgnoreCase(String.valueOf(linkMap.get("toOperator")))) {
											System.out.println(
													"linkMap.get(\"fromOperator\")::" + linkMap.get("fromOperator"));
											String fromOperatorId = String.valueOf(linkMap.get("fromOperator"));
											fromOperatorList.add((Map) operatorsMap
													.get(String.valueOf(linkMap.get("fromOperator"))));

										}
										if (linkMap != null && !linkMap.isEmpty() && String.valueOf(mappedKey)
												.equalsIgnoreCase(String.valueOf(linkMap.get("fromOperator")))) {
											toOperator = (Map) operatorsMap
													.get(String.valueOf(linkMap.get("toOperator")));

										}
									}
									if (fromOperatorList != null && !fromOperatorList.isEmpty()) {
										boolean isSameDataBase = fromOperatorList.stream()
												.filter(fromOpMap -> (fromOpMap.containsKey("CONNECTION_NAME")
														&& fromOpMap.containsKey("CONN_DB_NAME")))
												.map(fromOpMap -> (fromOpMap.get("CONNECTION_NAME") + ":::"
														+ fromOpMap.get("CONN_DB_NAME")))
												.distinct().count() == 1;
										boolean iscontainsFile = fromOperatorList.stream().anyMatch(fromOpMap -> "file"
												.equalsIgnoreCase(String.valueOf(fromOpMap.get("dragType"))));
										if (isSameDataBase && !iscontainsFile) {// all operators having same DB
											// need to call retrive method for getting same db data
											fromOperator = fromOperatorList.get(0);
//                                            System.out.println("fromOpeartor:::" + fromOperator);
//                                            System.out.println("toOpeartor:::" + toOperator);
											JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");
											String fromTableName = (String) fromOperator.get("tableName");
											JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
											String toTableName = (String) toOperator.get("tableName");
											String toIconType = (String) toOperator.get("iconType");
											Object fromConnectionObj = getConnection(fromConnObj);
											Object toConnectionObj = getConnection(toConnObj);
											if (fromConnectionObj instanceof Connection) {
												try {
													genericProcessETLDataService.processETLLog(sessionUserName, orgnId,
															"Source system successfully connected.", "INFO", 20, "Y",
															jobId);
												} catch (Exception e) {
												}
												if ((toConnectionObj instanceof Connection)
														|| (toConnectionObj instanceof JCO.Client)
														|| (toIconType != null && !"".equalsIgnoreCase(toIconType)
																&& !"null".equalsIgnoreCase(toIconType)
																&& !"SQL".equalsIgnoreCase(toIconType))) {
													try {
														genericProcessETLDataService.processETLLog(sessionUserName,
																orgnId, "Target system successfully connected.", "INFO",
																20, "Y", jobId);
													} catch (Exception e) {
													}
													fromConnection = (Connection) fromConnectionObj;
													if (!(toIconType != null && !"".equalsIgnoreCase(toIconType)
															&& !"null".equalsIgnoreCase(toIconType)
															&& !"SQL".equalsIgnoreCase(toIconType))) {
														toConnection = (Connection) toConnectionObj;
													}
													Map mapOpeartorData = (Map) mappingOperatorObj.get(mappedKey);
													Map trfmRulesDataMap = (Map) mapOpeartorData.get("trfmRules-data");

													JSONObject normalizeOptionsObj = (JSONObject) mapOpeartorData
															.get("normalizeOptionsObj");// RAVI NORMALIZE
													if (normalizeOptionsObj != null && !normalizeOptionsObj.isEmpty()) {
														columnsObj = (JSONObject) normalizeOptionsObj.get("colsObj");
													}

													if (trfmRulesDataMap != null && !trfmRulesDataMap.isEmpty()
															&& !String.valueOf(mappedKey)
																	.equalsIgnoreCase(currentTrnsOpId)) {//
//                                                        System.out.println("trfmRulesDataMap::" + trfmRulesDataMap);
														Map paramsMap = convertTransFrmRulsMapToParam(trfmRulesDataMap);

														if (paramsMap != null && !paramsMap.isEmpty()) {
															JSONObject selectTabMapObj = (JSONObject) paramsMap
																	.get("selectTabObj");
															if (!(selectTabMapObj != null
																	&& !selectTabMapObj.isEmpty())) {
																selectTabMapObj = new JSONObject();

															}
															selectTabMapObj.put("jobId", jobId);
															resultObj = genericProcessETLDataService.processETLData(
																	sessionUserName, orgnId, fromConnection,
																	toConnection, fromOperatorList, toOperator,
																	(JSONObject) paramsMap.get("columnsObj"),
																	(JSONObject) paramsMap.get("whereClauseObj"),
																	(JSONObject) paramsMap.get("defaultValObj"),
																	(JSONObject) paramsMap.get("joinQueryMapObj"),
																	(String) paramsMap.get("joinQuery"),
																	(JSONObject) paramsMap.get("orderBy"),
																	(String) paramsMap.get("groupBy"), nativeSQL,
																	(JSONObject) paramsMap.get("appendValObj"),
																	(JSONObject) paramsMap.get("columnClauseObj"),
																	selectTabMapObj, normalizeOptionsObj);
														}

													} else {
														resultObj = genericProcessETLDataService.processETLData(
																sessionUserName, orgnId, fromConnection, toConnection,
																fromOperatorList, toOperator, columnsObj,
																tablesWhereClauseObj, defaultValuesObj, joinQueryMapObj,
																joinQuery, orderByObj, groupByStr, nativeSQL,
																appendValObj, columnClauseObj, selectTabObj,
																normalizeOptionsObj);
													}

												} else {
													try {
														genericProcessETLDataService
																.processETLLog(sessionUserName, orgnId,
																		"Unable to connect target system due to "
																				+ toConnectionObj,
																		"ERROR", 20, "N", jobId);
													} catch (Exception e) {
													}
													resultObj.put("Message", toConnectionObj);
													resultObj.put("connectionFlag", "N");
												}

											} else if (fromConnectionObj instanceof JCO.Client) {
												// for SAP
												try {
													genericProcessETLDataService.processETLLog(sessionUserName, orgnId,
															"Source system successfully connected.", "INFO", 20, "Y",
															jobId);
												} catch (Exception e) {
												}
												if ((toConnectionObj instanceof Connection)
														|| (toConnectionObj instanceof JCO.Client)
														|| (toIconType != null && !"".equalsIgnoreCase(toIconType)
																&& !"null".equalsIgnoreCase(toIconType)
																&& !"SQL".equalsIgnoreCase(toIconType))) {
													try {
														genericProcessETLDataService.processETLLog(sessionUserName,
																orgnId, "Target system successfully connected.", "INFO",
																20, "Y", jobId);
													} catch (Exception e) {
													}
													fromJCOConnection = (JCO.Client) fromConnectionObj;
													if (!(toIconType != null && !"".equalsIgnoreCase(toIconType)
															&& !"null".equalsIgnoreCase(toIconType)
															&& !"SQL".equalsIgnoreCase(toIconType))) {
														if (toConnectionObj instanceof Connection) {
															toConnection = (Connection) toConnectionObj;
														} else if (toConnectionObj instanceof JCO.Client) {
															toJCOConnection = (JCO.Client) toConnectionObj;
														}

													}
													Map mapOpeartorData = (Map) mappingOperatorObj.get(mappedKey);
													Map trfmRulesDataMap = (Map) mapOpeartorData.get("trfmRules-data");

													JSONObject normalizeOptionsObj = (JSONObject) mapOpeartorData
															.get("normalizeOptionsObj");// RAVI NORMALIZE
													if (normalizeOptionsObj != null && !normalizeOptionsObj.isEmpty()) {
														columnsObj = (JSONObject) normalizeOptionsObj.get("colsObj");
													}

													if (trfmRulesDataMap != null && !trfmRulesDataMap.isEmpty()
															&& !String.valueOf(mappedKey)
																	.equalsIgnoreCase(currentTrnsOpId)) {
//                                                        System.out.println("trfmRulesDataMap::" + trfmRulesDataMap);
														Map paramsMap = convertTransFrmRulsMapToParam(trfmRulesDataMap);

														if (paramsMap != null && !paramsMap.isEmpty()) {
															JSONObject selectTabMapObj = (JSONObject) paramsMap
																	.get("selectTabObj");
															if (!(selectTabMapObj != null
																	&& !selectTabMapObj.isEmpty())) {
																selectTabMapObj = new JSONObject();

															}
															selectTabMapObj.put("jobId", jobId);
															resultObj = genericProcessETLDataService.processETLData(
																	sessionUserName, orgnId, fromJCOConnection,
																	toConnection, toJCOConnection, fromOperatorList,
																	toOperator,
																	(JSONObject) paramsMap.get("columnsObj"),
																	(JSONObject) paramsMap.get("whereClauseObj"),
																	(JSONObject) paramsMap.get("defaultValObj"),
																	(JSONObject) paramsMap.get("joinQueryMapObj"),
																	(String) paramsMap.get("joinQuery"),
																	(JSONObject) paramsMap.get("orderBy"),
																	(String) paramsMap.get("groupBy"), nativeSQL,
																	(JSONObject) paramsMap.get("appendValObj"),
																	(JSONObject) paramsMap.get("columnClauseObj"),
																	selectTabMapObj, normalizeOptionsObj);
														}

													} else {

														resultObj = genericProcessETLDataService.processETLData(
																sessionUserName, orgnId, fromJCOConnection,
																toConnection, toJCOConnection, fromOperatorList,
																toOperator, columnsObj, tablesWhereClauseObj,
																defaultValuesObj, joinQueryMapObj, joinQuery,
																orderByObj, groupByStr, nativeSQL, appendValObj,
																columnClauseObj, selectTabObj, normalizeOptionsObj);
													}
												} else {
													try {
														genericProcessETLDataService
																.processETLLog(sessionUserName, orgnId,
																		"Unable to connect target system due to "
																				+ toConnectionObj,
																		"ERROR", 20, "N", jobId);
													} catch (Exception e) {
													}
													resultObj.put("Message", toConnectionObj);
													resultObj.put("connectionFlag", "N");
												}
											} else {
												try {
													genericProcessETLDataService
															.processETLLog(sessionUserName, orgnId,
																	"Unable to connect source system due to "
																			+ fromConnectionObj,
																	"ERROR", 20, "N", jobId);
												} catch (Exception e) {
												}
												resultObj.put("Message", fromConnectionObj);
												resultObj.put("connectionFlag", "N");
											}
										} else {

											boolean isDataBase = fromOperatorList.stream()
													.anyMatch(fromOpMap -> (fromOpMap.containsKey("CONNECTION_NAME")
															&& fromOpMap.containsKey("CONN_DB_NAME")));
											if (!isDataBase) {// files
												fromOperator = fromOperatorList.get(0);
//                                                System.out.println("fromOpeartor:::" + fromOperator);
//                                                System.out.println("toOpeartor:::" + toOperator);
												JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");
												String fromTableName = (String) fromOperator.get("tableName");
												JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
												String toTableName = (String) toOperator.get("tableName");
												String toIconType = (String) toOperator.get("iconType");
												Object toConnectionObj = getConnection(toConnObj);
												try {
													genericProcessETLDataService.processETLLog(sessionUserName, orgnId,
															"Source File successfully connected.", "INFO", 20, "Y",
															jobId);
												} catch (Exception e) {
												}
												if ((toConnectionObj instanceof Connection)
														|| (toConnectionObj instanceof JCO.Client)
														|| (toIconType != null && !"".equalsIgnoreCase(toIconType)
																&& !"null".equalsIgnoreCase(toIconType)
																&& !"SQL".equalsIgnoreCase(toIconType))) {
													try {
														genericProcessETLDataService.processETLLog(sessionUserName,
																orgnId, "Target system successfully connected.", "INFO",
																20, "Y", jobId);
													} catch (Exception e) {
													}

													if (!(toIconType != null && !"".equalsIgnoreCase(toIconType)
															&& !"null".equalsIgnoreCase(toIconType)
															&& !"SQL".equalsIgnoreCase(toIconType))) {
														toConnection = (Connection) toConnectionObj;
													} else {
														toConnection = null; // ----------- ravi normalize edit
													}
													Map mapOpeartorData = (Map) mappingOperatorObj.get(mappedKey);
													Map trfmRulesDataMap = (Map) mapOpeartorData.get("trfmRules-data");

													JSONObject normalizeOptionsObj = (JSONObject) mapOpeartorData
															.get("normalizeOptionsObj");// RAVI NORMALIZE
													if (normalizeOptionsObj != null && !normalizeOptionsObj.isEmpty()) {
														columnsObj = (JSONObject) normalizeOptionsObj.get("colsObj");
													}
													if (trfmRulesDataMap != null && !trfmRulesDataMap.isEmpty()
															&& !String.valueOf(mappedKey)
																	.equalsIgnoreCase(currentTrnsOpId)) {//
//                                                        System.out.println("trfmRulesDataMap::" + trfmRulesDataMap);
														Map paramsMap = convertTransFrmRulsMapToParam(trfmRulesDataMap);
														if (paramsMap != null && !paramsMap.isEmpty()) {
															JSONObject selectTabMapObj = (JSONObject) paramsMap
																	.get("selectTabObj");
															if (!(selectTabMapObj != null
																	&& !selectTabMapObj.isEmpty())) {
																selectTabMapObj = new JSONObject();

															}
															selectTabMapObj.put("jobId", jobId);
															resultObj = genericProcessETLDataService.processETLData(
																	sessionUserName, orgnId, toConnection,
																	fromOperatorList, toOperator,
																	(JSONObject) paramsMap.get("columnsObj"),
																	(JSONObject) paramsMap.get("whereClauseObj"),
																	(JSONObject) paramsMap.get("defaultValObj"),
																	(JSONObject) paramsMap.get("joinQueryMapObj"),
																	(String) paramsMap.get("joinQuery"),
																	(JSONObject) paramsMap.get("orderBy"),
																	(String) paramsMap.get("groupBy"), nativeSQL,
																	(JSONObject) paramsMap.get("appendValObj"),
																	(JSONObject) paramsMap.get("columnClauseObj"),
																	selectTabMapObj, normalizeOptionsObj);
														}
													} else {
														resultObj = genericProcessETLDataService.processETLData(
																sessionUserName, orgnId, toConnection, fromOperatorList,
																toOperator, columnsObj, tablesWhereClauseObj,
																defaultValuesObj, joinQueryMapObj, joinQuery,
																orderByObj, groupByStr, nativeSQL, appendValObj,
																columnClauseObj, selectTabObj, normalizeOptionsObj);
													}

												} else {
													try {
														genericProcessETLDataService
																.processETLLog(sessionUserName, orgnId,
																		"Unable to connect target system due to "
																				+ toConnectionObj,
																		"ERROR", 20, "N", jobId);
													} catch (Exception e) {
													}
													resultObj.put("Message", toConnectionObj);
													resultObj.put("connectionFlag", "N");
												}
											} else {
												// need to call retrive method for getting different db data
											}
										}
									} else {
										System.out.println("***** Form Opeartors are Empty ******");
									}

								}
							}
						}

					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			try {
				genericProcessETLDataService.processETLLog(sessionUserName, orgnId,
						"Getting error while Process :" + e.getMessage(), "ERROR", 20, "N", jobId);
			} catch (Exception ex) {
			}
			resultObj.put("Message", e.getMessage());
			resultObj.put("connectionFlag", "N");
		} finally {
			if (!(resultObj != null)) {
				resultObj = new JSONObject();
			}
			resultObj.put("jobId", jobId);
		}

		return resultObj;
	}

	// RAVI NORMALIZE
	public String getNormalizeTabs(HttpServletRequest request, List headersList) {
		String normalizeTabsStr = "";
		String normalizeColumn = "";
		String itemSeparator = "";
		try {

			String jobId = request.getParameter("jobId");
			String operatorDataStr = request.getParameter("operatorData");
			JSONObject operatorData = (JSONObject) JSONValue.parse(operatorDataStr);
			JSONObject trfmRulesData = (JSONObject) operatorData.get("trfmRules-data");
			JSONObject normalizeOptionsObj = new JSONObject();
			if (trfmRulesData != null && !trfmRulesData.isEmpty()) {
				normalizeOptionsObj = (JSONObject) trfmRulesData.get("normalizeOptionsObj");
			}

			if (normalizeOptionsObj != null && normalizeOptionsObj.size() > 0) {
				normalizeColumn = (String) normalizeOptionsObj.get("normalizeColumn");
				itemSeparator = (String) normalizeOptionsObj.get("itemSeparator");

				normalizeTabsStr += "<div id='normalizeOptionsTabs'>" + "<ul>" + "<li>Normalize Options</li>" + "</ul>";

				normalizeTabsStr += "<div class = 'visionNormalizeOptions'>"
						+ "<div class = 'visionSelectColumnsHeaders' style = 'margin-left: 10px; margin-top: 10px;'>"
						+ "<span>Select Column to Normalize </span>" + "<select id='selectNormalizeColHeader'>"
						+ "<option selected>Select</option>";
				for (int i = 0; i < headersList.size(); i++) {
					if (normalizeColumn != null
							&& normalizeColumn.equalsIgnoreCase(String.valueOf(headersList.get(i)))) {
						normalizeTabsStr += "<option selected>" + headersList.get(i) + "</option>";
					} else {
						normalizeTabsStr += "<option>" + headersList.get(i) + "</option>";
					}
				}

				normalizeTabsStr += "</select>" + "</div>"
						+ "<div class='visionItemSeparatorDiv' style = 'margin-left: 10px; margin-top: 10px;'>"
						+ "<span class='visionItemSeparatorSpan' >Item Separator </span>"
						+ "<input id='itemSeparator' type='text' class='visionItemSeparatorInput' value='"
						+ itemSeparator + "'/>" + "</div>" + "</div>" + "</div>";

			} else if (jobId != null) {
				List jobData = saveJobsDAO.getcDmJobsList(request, jobId);
				if (jobData != null && !jobData.isEmpty()) {
					// String normalisationRulesStr = (((Object
					// [])jobData.get(0))[6])!=null?((String)((Object [])jobData.get(0))[6]):"";
					String flowChartDataStr = new PilogUtilities().clobToString((Clob) ((Object[]) jobData.get(0))[6]);
					JSONObject flowChartData = (JSONObject) JSONValue.parse(flowChartDataStr);
					JSONObject operators = (JSONObject) flowChartData.get("operators");

					for (Object key : operators.keySet()) {
						JSONObject operator = (JSONObject) operators.get(key);
						String iconType = (String) operator.get("iconType");
						if (iconType != null && "UNGROUP".equalsIgnoreCase(iconType)) {
							trfmRulesData = (JSONObject) operator.get("trfmRules-data");
							normalizeOptionsObj = (JSONObject) trfmRulesData.get("normalizeOptionsObj");

							normalizeColumn = (String) normalizeOptionsObj.get("normalizeColumn");
							itemSeparator = (String) normalizeOptionsObj.get("itemSeparator");
						}
					}

					normalizeTabsStr += "<div id='normalizeOptionsTabs'>" + "<ul>" + "<li>Normalize Options</li>"
							+ "</ul>";

					normalizeTabsStr += "<div class = 'visionNormalizeOptions'>"
							+ "<div class = 'visionSelectColumnsHeaders' style = 'margin-left: 10px; margin-top: 10px;'>"
							+ "<span>Select Column to Normalize </span>" + "<select id='selectNormalizeColHeader'>"
							+ "<option selected>Select</option>";
					for (int i = 0; i < headersList.size(); i++) {
						if (normalizeColumn != null
								&& normalizeColumn.equalsIgnoreCase(String.valueOf(headersList.get(i)))) {
							normalizeTabsStr += "<option selected>" + headersList.get(i) + "</option>";
						} else {
							normalizeTabsStr += "<option>" + headersList.get(i) + "</option>";
						}
					}

					normalizeTabsStr += "</select>" + "</div>"
							+ "<div class='visionItemSeparatorDiv' style = 'margin-left: 10px; margin-top: 10px;'>"
							+ "<span class='visionItemSeparatorSpan' >Item Separator </span>"
							+ "<input id='itemSeparator' type='text' class='visionItemSeparatorInput' value='"
							+ itemSeparator + "'/>" + "</div>" + "</div>" + "</div>";

				}
			} else {

				normalizeTabsStr += "<div id='normalizeOptionsTabs'>" + "<ul>" + "<li>Normalize Options</li>" + "</ul>";

				normalizeTabsStr += "<div class = 'visionNormalizeOptions'>"
						+ "<div class = 'visionSelectColumnsHeaders' style = 'margin-left: 10px; margin-top: 10px;'>"
						+ "<span>Select Column to Normalize </span>" + "<select id='selectNormalizeColHeader'>"
						+ "<option selected>Select</option>";
				for (int i = 0; i < headersList.size(); i++) {

					normalizeTabsStr += "<option>" + headersList.get(i) + "</option>";
				}

				normalizeTabsStr += "</select>" + "</div>"
						+ "<div class='visionItemSeparatorDiv' style = 'margin-left: 10px; margin-top: 10px;'>"
						+ "<span class='visionItemSeparatorSpan' >Item Separator </span>"
						+ "<input id='itemSeparator' type='text' class='visionItemSeparatorInput' value=''/>" + "</div>"
						+ "</div>" + "</div>";
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return normalizeTabsStr;
	}

	public String getDeNormalizeTabs(HttpServletRequest request, List headersList) { // ravi etl integration
		String deNormalizeTabsStr = "";
		String deNormalizeColumn = "";
		String delimiter = "";
		String keyColumn = "";
		try {
			String jobId = request.getParameter("jobId");

			String operatorDataStr = request.getParameter("operatorData");
			JSONObject operatorData = (JSONObject) JSONValue.parse(operatorDataStr);
			JSONObject trfmRulesData = (JSONObject) operatorData.get("trfmRules-data");

			JSONObject normalizeOptionsObj = new JSONObject();
			if (trfmRulesData != null && !trfmRulesData.isEmpty()) {
				normalizeOptionsObj = (JSONObject) trfmRulesData.get("normalizeOptionsObj");
			}

			if (normalizeOptionsObj != null && normalizeOptionsObj.size() > 0) {
				deNormalizeColumn = (String) normalizeOptionsObj.get("deNormalizeColumn");
				delimiter = (String) normalizeOptionsObj.get("delimiter");
				keyColumn = (String) normalizeOptionsObj.get("keyColumn");

				deNormalizeTabsStr += "<div id='deNormalizeOptionsTabs'>" + "<ul>" + "<li>DeNormalize Options</li>"
						+ "</ul>";

				deNormalizeTabsStr += "<div class = 'visionDeNormalizeOptions'>"
						+ "<div class = 'visionSelectColumnsHeaders' style = 'margin-left: 10px; margin-top: 10px;'>"
						+ "<span>Select Column to De-Normalize </span>" + "<select id='selectDenormalizeColHeader'>"
						+ "<option selected>Select</option>";
				for (int i = 0; i < headersList.size(); i++) {
					if (deNormalizeColumn != null
							&& deNormalizeColumn.equalsIgnoreCase(String.valueOf(headersList.get(i)))) {
						deNormalizeTabsStr += "<option selected>" + headersList.get(i) + "</option>";
					} else {
						deNormalizeTabsStr += "<option>" + headersList.get(i) + "</option>";
					}
				}

				deNormalizeTabsStr += "</select>" + "</div>"
						+ "<div class = 'visionSelectKeyColumn' style = 'margin-left: 10px; margin-top: 10px;'>"
						+ "<span> Key Column </span>" + "<select id='selectDenormalizeKeyColumn'>"
						+ "<option selected>Select</option>";
				for (int i = 0; i < headersList.size(); i++) {
					if (keyColumn != null && keyColumn.equalsIgnoreCase(String.valueOf(headersList.get(i)))) {
						deNormalizeTabsStr += "<option selected>" + headersList.get(i) + "</option>";
					} else {
						deNormalizeTabsStr += "<option>" + headersList.get(i) + "</option>";
					}
				}
				deNormalizeTabsStr += "</select>" + "</div>"
						+ "<div class='visionItemSeparatorDiv' style = 'margin-left: 10px; margin-top: 10px;'>"
						+ "<span class='visionItemSeparatorSpan' >Delimiter </span>"
						+ "<input id='delimiter' type='text' class='visionItemSeparatorInput' value='" + delimiter
						+ "'/>" + "</div>" + "</div>" + "</div>";

			} else if (jobId != null) {
				List jobData = saveJobsDAO.getcDmJobsList(request, jobId);
				if (jobData != null && !jobData.isEmpty()) {
					String flowChartDataStr = new PilogUtilities().clobToString((Clob) ((Object[]) jobData.get(0))[6]);
					JSONObject flowChartData = (JSONObject) JSONValue.parse(flowChartDataStr);
					JSONObject operators = (JSONObject) flowChartData.get("operators");
					for (Object key : operators.keySet()) {
						JSONObject operator = (JSONObject) operators.get(key);
						String iconType = (String) operator.get("iconType");
						if (iconType != null && "UNGROUP".equalsIgnoreCase(iconType)) {
							trfmRulesData = (JSONObject) operator.get("trfmRules-data");
							normalizeOptionsObj = (JSONObject) trfmRulesData.get("normalizeOptionsObj");

							deNormalizeColumn = (String) normalizeOptionsObj.get("deNormalizeColumn");
							delimiter = (String) normalizeOptionsObj.get("delimiter");
						}
					}

					deNormalizeTabsStr += "<div id='deNormalizeOptionsTabs'>" + "<ul>" + "<li>DeNormalize Options</li>"
							+ "</ul>";

					deNormalizeTabsStr += "<div class = 'visionDeNormalizeOptions'>"
							+ "<div class = 'visionSelectColumnsHeaders' style = 'margin-left: 10px; margin-top: 10px;'>"
							+ "<span>Select Column to De-Normalize </span>" + "<select id='selectDenormalizeColHeader'>"
							+ "<option selected>Select</option>";
					for (int i = 0; i < headersList.size(); i++) {
						if (deNormalizeColumn != null
								&& deNormalizeColumn.equalsIgnoreCase(String.valueOf(headersList.get(i)))) {
							deNormalizeTabsStr += "<option selected>" + headersList.get(i) + "</option>";
						} else {
							deNormalizeTabsStr += "<option>" + headersList.get(i) + "</option>";
						}
					}
					deNormalizeTabsStr += "</select>" + "</div>"
							+ "<div class='visionItemSeparatorDiv' style = 'margin-left: 10px; margin-top: 10px;'>"
							+ "<span class='visionItemSeparatorSpan' >Delimiter </span>"
							+ "<input id='delimiter' type='text' class='visionItemSeparatorInput' value='" + delimiter
							+ "'/>" + "</div>" + "</div>" + "</div>";

				}
			} else {
				deNormalizeTabsStr += "<div id='deNormalizeOptionsTabs'>" + "<ul>" + "<li>DeNormalize Options</li>"
						+ "</ul>";

				deNormalizeTabsStr += "<div class = 'visionDeNormalizeOptions'>"
						+ "<div class = 'visionSelectColumnsHeaders' style = 'margin-left: 10px; margin-top: 10px;'>"
						+ "<span>Select Column to De-Normalize </span>" + "<select id='selectDenormalizeColHeader'>"
						+ "<option selected>Select</option>";
				for (int i = 0; i < headersList.size(); i++) {
					deNormalizeTabsStr += "<option>" + headersList.get(i) + "</option>";
				}
				deNormalizeTabsStr += "</select>" + "</div>"
						+ "<div class='visionItemSeparatorDiv' style = 'margin-left: 10px; margin-top: 10px;'>"
						+ "<span class='visionItemSeparatorSpan' >Delimiter </span>"
						+ "<input id='delimiter' type='text' class='visionItemSeparatorInput' value=''/>" + "</div>"
						+ "</div>" + "</div>";
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return deNormalizeTabsStr;
	}

//    public String getDeNormalizeTabs(HttpServletRequest request, List headersList) {
//        String deNormalizeTabsStr = "";
//        String deNormalizeColumn = "";
//        String delimiter = "";
//        try {
//            String jobId = request.getParameter("jobId");
//
//            String operatorDataStr = request.getParameter("operatorData");
//            JSONObject operatorData = (JSONObject) JSONValue.parse(operatorDataStr);
//            JSONObject trfmRulesData = (JSONObject) operatorData.get("trfmRules-data");
//
//            JSONObject normalizeOptionsObj = new JSONObject();
//            if (trfmRulesData != null && !trfmRulesData.isEmpty()) {
//                normalizeOptionsObj = (JSONObject) trfmRulesData.get("normalizeOptionsObj");
//            }
//
//            if (normalizeOptionsObj != null && normalizeOptionsObj.size() > 0) {
//                deNormalizeColumn = (String) normalizeOptionsObj.get("deNormalizeColumn");
//                delimiter = (String) normalizeOptionsObj.get("delimiter");
//
//                deNormalizeTabsStr += "<div id='deNormalizeOptionsTabs'>"
//                        + "<ul>"
//                        + "<li>DeNormalize Options</li>"
//                        + "</ul>";
//
//                deNormalizeTabsStr += "<div class = 'visionDeNormalizeOptions'>"
//                        + "<div class = 'visionSelectColumnsHeaders' style = 'margin-left: 10px; margin-top: 10px;'>"
//                        + "<span>Select Column to De-Normalize </span>"
//                        + "<select id='selectDenormalizeColHeader'>"
//                        + "<option selected>Select</option>";
//                for (int i = 0; i < headersList.size(); i++) {
//                    if (deNormalizeColumn != null && deNormalizeColumn.equalsIgnoreCase(String.valueOf(headersList.get(i)))) {
//                        deNormalizeTabsStr += "<option selected>" + headersList.get(i) + "</option>";
//                    } else {
//                        deNormalizeTabsStr += "<option>" + headersList.get(i) + "</option>";
//                    }
//                }
//                deNormalizeTabsStr += "</select>"
//                        + "</div>"
//                        + "<div class='visionItemSeparatorDiv' style = 'margin-left: 10px; margin-top: 10px;'>"
//                        + "<span class='visionItemSeparatorSpan' >Delimiter </span>"
//                        + "<input id='delimiter' type='text' class='visionItemSeparatorInput' value='" + delimiter + "'/>"
//                        + "</div>"
//                        + "</div>"
//                        + "</div>";
//
//            } else if (jobId != null) {
//                List jobData = genericDataPipingDAO.getcDmJobsList(request, jobId);
//                if (jobData != null && !jobData.isEmpty()) {
//                    String flowChartDataStr = new PilogUtilities().clobToString((Clob) ((Object[]) jobData.get(0))[6]);
//                    JSONObject flowChartData = (JSONObject) JSONValue.parse(flowChartDataStr);
//                    JSONObject operators = (JSONObject) flowChartData.get("operators");
//                    for (Object key : operators.keySet()) {
//                        JSONObject operator = (JSONObject) operators.get(key);
//                        String iconType = (String) operator.get("iconType");
//                        if (iconType != null && "UNGROUP".equalsIgnoreCase(iconType)) {
//                            trfmRulesData = (JSONObject) operator.get("trfmRules-data");
//                            normalizeOptionsObj = (JSONObject) trfmRulesData.get("normalizeOptionsObj");
//
//                            deNormalizeColumn = (String) normalizeOptionsObj.get("deNormalizeColumn");
//                            delimiter = (String) normalizeOptionsObj.get("delimiter");
//                        }
//                    }
//
//                    deNormalizeTabsStr += "<div id='deNormalizeOptionsTabs'>"
//                            + "<ul>"
//                            + "<li>DeNormalize Options</li>"
//                            + "</ul>";
//
//                    deNormalizeTabsStr += "<div class = 'visionDeNormalizeOptions'>"
//                            + "<div class = 'visionSelectColumnsHeaders' style = 'margin-left: 10px; margin-top: 10px;'>"
//                            + "<span>Select Column to De-Normalize </span>"
//                            + "<select id='selectDenormalizeColHeader'>"
//                            + "<option selected>Select</option>";
//                    for (int i = 0; i < headersList.size(); i++) {
//                        if (deNormalizeColumn != null && deNormalizeColumn.equalsIgnoreCase(String.valueOf(headersList.get(i)))) {
//                            deNormalizeTabsStr += "<option selected>" + headersList.get(i) + "</option>";
//                        } else {
//                            deNormalizeTabsStr += "<option>" + headersList.get(i) + "</option>";
//                        }
//                    }
//                    deNormalizeTabsStr += "</select>"
//                            + "</div>"
//                            + "<div class='visionItemSeparatorDiv' style = 'margin-left: 10px; margin-top: 10px;'>"
//                            + "<span class='visionItemSeparatorSpan' >Delimiter </span>"
//                            + "<input id='delimiter' type='text' class='visionItemSeparatorInput' value='" + delimiter + "'/>"
//                            + "</div>"
//                            + "</div>"
//                            + "</div>";
//
//                }
//            } else {
//                deNormalizeTabsStr += "<div id='deNormalizeOptionsTabs'>"
//                        + "<ul>"
//                        + "<li>DeNormalize Options</li>"
//                        + "</ul>";
//
//                deNormalizeTabsStr += "<div class = 'visionDeNormalizeOptions'>"
//                        + "<div class = 'visionSelectColumnsHeaders' style = 'margin-left: 10px; margin-top: 10px;'>"
//                        + "<span>Select Column to De-Normalize </span>"
//                        + "<select id='selectDenormalizeColHeader'>"
//                        + "<option selected>Select</option>";
//                for (int i = 0; i < headersList.size(); i++) {
//                    deNormalizeTabsStr += "<option>" + headersList.get(i) + "</option>";
//                }
//                deNormalizeTabsStr += "</select>"
//                        + "</div>"
//                        + "<div class='visionItemSeparatorDiv' style = 'margin-left: 10px; margin-top: 10px;'>"
//                        + "<span class='visionItemSeparatorSpan' >Delimiter </span>"
//                        + "<input id='delimiter' type='text' class='visionItemSeparatorInput' value=''/>"
//                        + "</div>"
//                        + "</div>"
//                        + "</div>";
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return deNormalizeTabsStr;
//    }
	public String convertJSONtoCSV(File jsonFile, String filePath) {
		String csvFileName = "V10ETLImport_" + System.currentTimeMillis() + ".csv";
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

	public JSONObject checkFileExist(HttpServletRequest request) { // ravi etl integration
		JSONObject resultObj = new JSONObject();
		try {
			String fileDataObjStr = request.getParameter("fileDataObj");
			JSONObject fileDataObj = (JSONObject) JSONValue.parse(fileDataObjStr);
			String fileName = (String) fileDataObj.get("filePath");
			String filePath = etlFilePath + "ETL_EXPORT_" + File.separator
					+ request.getSession(false).getAttribute("ssUsername") + File.separator + fileName;
			File file = new File(filePath);
			if (file.exists()) {
				resultObj.put("fileExist", "Y");
			} else {
				resultObj.put("fileExist", "N");
			}

		} catch (Exception e) {
		}
		return resultObj;
	}

//    public JSONObject processPredefinedJob(HttpServletRequest request) { // ravi etl integration
//        JSONObject resultObj = new JSONObject();
//        String tableStr = "<table style=\"width: 100%;\" border=\"1\"><tbody><tr><th style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">Request No</th><th style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">Message</th></tr>";
//        try {
//            String columnsListStr = "RECORD_NO,"
//                    + "ERP_NO,"
//                    + "CLASS_TERM,"
//                    + "ERP_COMMENT,"
//                    + "SHORT_DESR,"
//                    + "LONG_DESR,"
//                    + "INSTANCE,"
//                    + "BUSINESS_UNIT,"
//                    + "PLANT,"
//                    + "RECORD_TYPE,"
//                    + "RECORD_GROUP,"
//                    + "UOM,"
//                    + "HSN_CODE,"
//                    + "STATUS,"
//                    + "LOCALE,"
//                    + "ABBREVIATION,"
//                    + "SOURCE,"
//                    + "DATA_SOURCE,"
//                    + "CREATE_BY,"
//                    + "CREATE_DATE,"
//                    + "EDIT_BY,"
//                    + "EDIT_DATE,"
//                    + "CONCEPT_ID,"
//                    + "CUSTOM_COLUMN15,"
//                    + "REGION,"
//                    + "DR_ID1";
//
//            List columnsList = Arrays.asList(columnsListStr.split(","));
//            List<Object[]> sourceData = genericDataPipingDAO.getPredefinedSourceData(request);
//            if (sourceData != null && !sourceData.isEmpty()) {
//                for (int i = 0; i < sourceData.size(); i++) {
//                    JSONObject dataMap = new JSONObject();
//                    Object[] sourceRow = sourceData.get(i);
//                    for (int j = 0; j < sourceRow.length; j++) {
//                        dataMap.put(columnsList.get(j), sourceRow[j]);
//                    }
//
//                    String hiddenObjStr = "{\"HIDDEN_INSTANCE\":\"MANDT\",\"HIDDEN_REGION\":\"SPRAS\",\"HIDDEN_UOM\":\"MEINH,MEINS\",\"HIDDEN_RECORD_NO\":\"MATN,REQ_NUMBER\",\"HIDDEN_RECORD_TYPE\":\"MTART\",\"HIDDEN_BUSINESS_UNIT\":\"PLANT,BWKEY,WERKS\"}";
//                    JSONObject hiddenObj = (JSONObject) JSONValue.parse(hiddenObjStr);
//                    for (Object key : hiddenObj.keySet()) {
//                        String value = String.valueOf(hiddenObj.get(key));
//                        if (value != null) {
//                            if (key != null && String.valueOf(key).contains("HIDDEN")) {
//                                String[] columnsArray = value.split(",");
//                                String[] hiddenIds = String.valueOf(key).split("HIDDEN_");
//                                String hiddenVal = (String) dataMap.get(hiddenIds[1]);
//                                for (int k = 0; k < columnsArray.length; k++) {
//                                    dataMap.put(columnsArray[k], hiddenVal);
//                                    //                                                    data[columnsArray[j]] = encodeURIComponent(hiddenVal);
//                                }
//                            }
//                        }
//                    }
//
//                    String response = visionSOASAPInterface.transferToERP(request, dataMap);
//                    resultObj = (JSONObject) JSONValue.parse(response);
//                    tableStr += "<tr><td>" + resultObj.get("RecordNo") + "</td><td>" + resultObj.get("Message") + "</td></tr>";
//                }
//
//            }
//            tableStr += "</tbody></table>";
//            resultObj.put("tableStr", tableStr);
//        } catch (Exception e) {
//        }
//        return resultObj;
//    }
	// uttezz
	public JSONArray createCubeTable(HttpServletRequest request) {

		JSONArray resultObjArray = new JSONArray();
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		try {
			String flowChartDataStr = request.getParameter("flowChartData");
			if (flowChartDataStr != null && !"".equalsIgnoreCase(flowChartDataStr)
					&& !"null".equalsIgnoreCase(flowChartDataStr)) {
				JSONObject flowChartData = (JSONObject) JSONValue.parse(flowChartDataStr);
				if (flowChartData != null && !flowChartData.isEmpty()) {
					JSONObject operators = (JSONObject) flowChartData.get("operators");
					if (operators != null && !operators.isEmpty()) {

						for (Object key : operators.keySet()) {

							JSONObject operatorData = (JSONObject) operators.get(key);

							String targetOperator = (String) operatorData.get("targetOperator");
							String iconType = (String) operatorData.get("iconType");
							if (iconType != null && "CUBE".equalsIgnoreCase(iconType) && targetOperator != null
									&& "Y".equalsIgnoreCase(targetOperator)) {
								String tableName = (String) operatorData.get("tableName");
								JSONObject connObj = (JSONObject) operatorData.get("connObj");
								JSONArray columnsObjArray = new JSONArray();
								JSONObject trfmRulesdata = (JSONObject) operatorData.get("trfmRules-data");
								JSONArray colMappingsData = (JSONArray) trfmRulesdata.get("colMappingsData");
								List<String> columnsList = new ArrayList();
								for (Object columnMapping : colMappingsData) {

									String destinationColumn = (String) ((JSONObject) columnMapping)
											.get("destinationColumn");
									if (destinationColumn != null && destinationColumn.contains(":")) {
										destinationColumn = destinationColumn.split(":")[1];
									}
									columnsList.add(destinationColumn);

								}
								String columnsStr = String.join(",", columnsList);

								String dataSourceName = (String) operatorData.get("dataSourceName");
								if (connObj != null && !connObj.isEmpty()) {

									Object connectionObj = getConnection(connObj);
									if (connectionObj instanceof Connection) {
										conn = (Connection) connectionObj;
									} else {
										JSONObject resultObj = new JSONObject();
										resultObj.put("message", connectionObj);
										resultObj.put("messageFlag", false);
										resultObjArray.add(resultObj);
									}
								} else {
									Class.forName(dataBaseDriver);
									conn = DriverManager.getConnection(dbURL, userName, password);
								}

								Statement statement = conn.createStatement();
								String selectQuery = "SELECT " + columnsStr + " FROM " + tableName + " WHERE 1=2";
								ResultSet rs = statement.executeQuery(selectQuery);
								ResultSetMetaData resultSetMetaData = rs.getMetaData();
								for (int i = 0; i < columnsList.size(); i++) {
									String dataTypeName = resultSetMetaData.getColumnTypeName(i + 1);
									String dataTypeLength = String.valueOf(resultSetMetaData.getPrecision(i + 1));
									dataTypeLength = (dataTypeLength != null) ? ("(" + dataTypeLength + ")") : "";
									String dataType = dataTypeName + dataTypeLength;
									JSONObject colsObj = new JSONObject();
									colsObj.put("COLUMN_NAME", columnsList.get(i));
									colsObj.put("DATA_TYPE", dataType);
									colsObj.put("PK", "");
									columnsObjArray.add(colsObj);
								}

								if (tableName != null && !"".equalsIgnoreCase(tableName)
										&& !"null".equalsIgnoreCase(tableName)) {
									if (columnsObjArray != null && !columnsObjArray.isEmpty()) {
										String query = "";
										List<String> pkColumns = new ArrayList<>();
										for (int j = 0; j < columnsObjArray.size(); j++) {
											JSONObject columnObj = (JSONObject) columnsObjArray.get(j);
											if (columnObj != null && !columnObj.isEmpty()
													&& columnObj.get("COLUMN_NAME") != null
													&& !"".equalsIgnoreCase(
															String.valueOf(columnObj.get("COLUMN_NAME")))
													&& !"null".equalsIgnoreCase(
															String.valueOf(columnObj.get("COLUMN_NAME")))
													&& columnObj.get("DATA_TYPE") != null
													&& !"".equalsIgnoreCase(String.valueOf(columnObj.get("DATA_TYPE")))
													&& !"null".equalsIgnoreCase(
															String.valueOf(columnObj.get("DATA_TYPE")))) {
												query += " " + columnObj.get("COLUMN_NAME") + " "
														+ columnObj.get("DATA_TYPE");
												if ("Y".equalsIgnoreCase(String.valueOf(columnObj.get("PK")))) {
													pkColumns.add(String.valueOf(columnObj.get("COLUMN_NAME")));
												}
												if (j != columnsObjArray.size() - 1) {
													query += " , ";
												}
											}
										}
										if (query != null && !"".equalsIgnoreCase(query)
												&& !"null".equalsIgnoreCase(query)) {
											try {
												String dropQuery = "ALTER TABLE " + tableName.toUpperCase() + " "
														+ " DROP PRIMARY KEY CASCADE";
												preparedStatement = conn.prepareStatement(dropQuery);
												preparedStatement.executeUpdate();
											} catch (Exception e) {
											}
											try {
												String dropQuery = "DROP TABLE " + tableName.toUpperCase()
														+ " CASCADE CONSTRAINTS";
												preparedStatement = conn.prepareStatement(dropQuery);
												preparedStatement.executeUpdate();
											} catch (Exception e) {
											}
											query = "CREATE TABLE " + tableName.toUpperCase() + " (" + query + ")\n"
													+ "";
											System.out.println("Create table Query:::" + query);
											preparedStatement = conn.prepareStatement(query);
											int createCount = preparedStatement.executeUpdate();
											System.out.println("createTableCount:::" + createCount);
											try {
												if (pkColumns != null && !pkColumns.isEmpty()) {
													String pkQuery = "ALTER TABLE " + tableName.toUpperCase()
															+ " ADD (\n" + "  CONSTRAINT " + tableName.toUpperCase()
															+ "_PK\n" + " PRIMARY KEY\n" + " ("
															+ StringUtils.collectionToCommaDelimitedString(pkColumns)
															+ "))";
													System.out.println("pkQuery:::" + pkQuery);
													preparedStatement = conn.prepareStatement(pkQuery);
													int createPKCount = preparedStatement.executeUpdate();
													System.out.println("createPKCount:::" + createPKCount);
												}
											} catch (Exception e) {
											}

										}
										JSONObject resultObj = new JSONObject();
										resultObj.put("message",
												"Table " + tableName.toUpperCase() + " created/modified successfully.");
										resultObj.put("messageFlag", true);
										resultObjArray.add(resultObj);
									} else {
										JSONObject resultObj = new JSONObject();
										resultObj.put("message", "You must specify some columns to create the table.");
										resultObj.put("messageFlag", false);
										resultObjArray.add(resultObj);
									}

								}

							}
						}

					} else {
						JSONObject resultObj = new JSONObject();
						resultObj.put("message", "You must specify Tables");
						resultObj.put("messageFlag", false);
						resultObjArray.add(resultObj);
					}
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
			JSONObject resultObj = new JSONObject();
			resultObj.put("message", e.getMessage());
			resultObj.put("messageFlag", false);
			resultObjArray.add(resultObj);
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (Exception e) {

			}
		}
		return resultObjArray;
	}

	public JSONArray getPrimaryKeyColumns(Connection connection, String tableName) {

		ResultSet resultSet = null;
		DatabaseMetaData dmd = null;
		JSONArray tablePKColumnArr = new JSONArray();

		try {
			dmd = connection.getMetaData();
			resultSet = dmd.getPrimaryKeys(null, null, tableName);
			while (resultSet.next()) {
				String name = resultSet.getString("COLUMN_NAME");
				tablePKColumnArr.add(name);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (resultSet != null) {
					resultSet.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
			}
		}
		return tablePKColumnArr;
	}

	public String getETLOpenFunctionForm(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String selectedFunRowDataStr = request.getParameter("selectedRowData");
			String columnType = request.getParameter("columnType");
			List columnTypes = new ArrayList();
			columnTypes.add("fromWhereClauseColumn");
			columnTypes.add("childColumn");
			columnTypes.add("masterColumn");
			Map columnMapTypes = new HashMap();
			columnMapTypes.put("fromWhereClauseColumn", "FROM_WHERE_CLAUSE_COLUMN");
			columnMapTypes.put("childColumn", "CHILD_COLUMN");
			columnMapTypes.put("masterColumn", "MASTER_COLUMN");
			if (selectedFunRowDataStr != null && !"".equalsIgnoreCase(selectedFunRowDataStr)
					&& !"null".equalsIgnoreCase(selectedFunRowDataStr)) {
				String dataFunobjstr = request.getParameter("dataFunobjstr");
				JSONObject dataFunObj = new JSONObject();
				if (dataFunobjstr != null && !"".equalsIgnoreCase(dataFunobjstr)
						&& !"null".equalsIgnoreCase(dataFunobjstr)) {
					dataFunObj = (JSONObject) JSONValue.parse(dataFunobjstr);
				}
				JSONObject selectedFunRowData = (JSONObject) JSONValue.parse(selectedFunRowDataStr);
				if (selectedFunRowData != null && !selectedFunRowData.isEmpty()) {
					String columnIconStr = "<div id=\"funFormETLDivIcon\"><table id=\"funOpenFormETLTableIcon\"><tr>";
					String columnMappingStr = "<div id=\"funFormETLDiv\"><table id=\"funOpenFormETLTable\""
							+ " class=\"visionEtlFunFormTable\" style='width: 100%;' border='1'>" + "";
					List<Object[]> funFormList = genericDataPipingDAO.getFunFormList(request, selectedFunRowData);
					if (funFormList != null && !funFormList.isEmpty()) {
						String columnMappingDataStr = "<tr>";
						String columnMappingHeaderStr = "<tr>";
						String addTrString = "<tr>";
						String hiddenAttar = "";
						for (int i = 0; i < funFormList.size(); i++) {

							Object[] funFormColsArray = funFormList.get(i);
							if (funFormColsArray != null && funFormColsArray.length != 0) {
								String displayStyle = "";
								String inputAttar = "";
								if (columnType != null && !"".equalsIgnoreCase(columnType)
										&& !"null".equalsIgnoreCase(columnType) && columnTypes.contains(columnType)) {
									columnType = (String) columnMapTypes.get(columnType);
								} else {
									columnType = (String) funFormColsArray[8];
								}
								if ("T".equalsIgnoreCase(String.valueOf(funFormColsArray[5]))) {
									inputAttar = "<input id='" + funFormColsArray[4] + "' class='visionColMappingInput'"
											+ " type='text'" + " value='"
											+ ((funFormColsArray[7] != null
													&& !"".equalsIgnoreCase(String.valueOf(funFormColsArray[7]))
													&& !"null".equalsIgnoreCase(String.valueOf(funFormColsArray[7])))
															? funFormColsArray[7]
															: "")
											+ "' />";
								} else if ("TP".equalsIgnoreCase(String.valueOf(funFormColsArray[5]))) {
									inputAttar = "<input id='" + funFormColsArray[4] + "' class='visionColMappingInput'"
											+ " type='text'" + " value='"
											+ ((funFormColsArray[7] != null
													&& !"".equalsIgnoreCase(String.valueOf(funFormColsArray[7]))
													&& !"null".equalsIgnoreCase(String.valueOf(funFormColsArray[7])))
															? funFormColsArray[7]
															: "")
											+ "' />"
											+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
											+ " onclick=\"selectFunColumnValue(this,'" + columnType
											+ "')\" style=\"\" />"
											+ "<img title='Select Function' src=\"images/Fx icon-01.svg\" class=\"visionETLColMapImage \" "
											+ " onclick=\"selectColumnFunForm(this,'" + columnType + "')\" style=\"\">";
								} else if ("P".equalsIgnoreCase(String.valueOf(funFormColsArray[5]))) {
									inputAttar = "<input id='" + funFormColsArray[4]
											+ "' readonly='readonly' class='visionColMappingInput'" + " type='text'"
											+ " value='"
											+ ((funFormColsArray[7] != null
													&& !"".equalsIgnoreCase(String.valueOf(funFormColsArray[7]))
													&& !"null".equalsIgnoreCase(String.valueOf(funFormColsArray[7])))
															? funFormColsArray[7]
															: "")
											+ "' />"
											+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
											+ " onclick=\"selectFunColumnValue(this,'" + columnType
											+ "')\" style=\"\" />"
											+ "<img title='Select Function' src=\"images/Fx icon-01.svg\" class=\"visionETLColMapImage \" "
											+ " onclick=\"selectColumnFunForm(this,'" + columnType + "')\" style=\"\">";
								} else if ("TA".equalsIgnoreCase(String.valueOf(funFormColsArray[5]))) {
									inputAttar = "<input id='" + funFormColsArray[4] + "' class='visionColMappingInput'"
											+ " type='text'" + " value='"
											+ ((funFormColsArray[7] != null
													&& !"".equalsIgnoreCase(String.valueOf(funFormColsArray[7]))
													&& !"null".equalsIgnoreCase(String.valueOf(funFormColsArray[7])))
															? funFormColsArray[7]
															: "")
											+ "' />";
								} else if ("L".equalsIgnoreCase(String.valueOf(funFormColsArray[5]))) {
									inputAttar = "<select id='" + funFormColsArray[4]
											+ "' class='visionColMappingInput' >" + ""
											+ genericDataPipingDAO.getGetListOfValues(
													String.valueOf(funFormColsArray[8]),
													String.valueOf(funFormColsArray[7]),
													String.valueOf(funFormColsArray[4]), request)
											+ "</select>";
								} else if ("D".equalsIgnoreCase(String.valueOf(funFormColsArray[5]))) {

								} else if ("C".equalsIgnoreCase(String.valueOf(funFormColsArray[5]))) {
									inputAttar = "<input id='" + funFormColsArray[4] + "' class='visionColMappingInput'"
											+ " type='checkbox'" + " />";
								} else if ("H".equalsIgnoreCase(String.valueOf(funFormColsArray[5]))) {
									displayStyle = "display:none;";
									inputAttar = "<input id='" + funFormColsArray[4] + "' class='visionColMappingInput'"
											+ " type='hidden'" + " value='"
											+ ((funFormColsArray[7] != null
													&& !"".equalsIgnoreCase(String.valueOf(funFormColsArray[7]))
													&& !"null".equalsIgnoreCase(String.valueOf(funFormColsArray[7])))
															? funFormColsArray[7]
															: "")
											+ "' />";
								}
								if (!"MULTI_COLUMNS"
										.equalsIgnoreCase(String.valueOf(selectedFunRowData.get("FUN_LVL_TYPE")))) {
									columnMappingStr += "<tr>" + "<th style='" + displayStyle
											+ "' width='15%' class=\"mappedColsTh1\" " + "style='text-align: center'>"
											+ funFormColsArray[3] + "</th>" + "<td style='" + displayStyle + "'> "
											+ inputAttar + "</td>";
									if (i == funFormList.size() - 1) {
										columnMappingStr += "<tr style='display:none'>"
												+ "<th width='15%' class=\"mappedColsTh1\" "
												+ "style='text-align: center'>Level Type</th>"
												+ "<td width='85%'  ><input id='FUN_LVL_TYPE' class='visionColMappingInput'"
												+ " type='text'" + " value='" + selectedFunRowData.get("FUN_LVL_TYPE")
												+ "' />";
										columnMappingStr += "</td>" + "</tr>";
										columnMappingStr += "<tr style='display:none'>"
												+ "<th width='15%' class=\"mappedColsTh1\" "
												+ "style='text-align: center'>Level Type</th>"
												+ "<td width='85%'  ><input id='DM_FUN_CUST_COL1' class='visionColMappingInput'"
												+ " type='text'" + " value='"
												+ selectedFunRowData.get("DM_FUN_CUST_COL1") + "' />";
										columnMappingStr += "</td>" + "</tr>";
										columnMappingStr += "<tr style='display:none'>"
												+ "<th width='15%' class=\"mappedColsTh1\" "
												+ "style='text-align: center'>Level Type</th>"
												+ "<td width='85%'  ><input id='DM_FUN_CUST_COL2' class='visionColMappingInput'"
												+ " type='text'" + " value='"
												+ selectedFunRowData.get("DM_FUN_CUST_COL2") + "' />";
										columnMappingStr += "</td>" + "</tr>";
									}
								} else {
									if (i == 0) {
										addTrString = "<tr>";
										columnIconStr += "<th><img src=\"images/Add icon.svg\""
												+ " id=\"addMultiPleFunForm\" onclick=\"addNewTableDataRow('funOpenFormETLTableTr','funFormETLTable',this)\""
												+ " title=\"Add new condition\""
												+ " style=\"width:15px;height: 15px;cursor:pointer;\">" + "</th>";
										columnMappingHeaderStr += "<th "
												+ "style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;"
												+ displayStyle + "' >" + "</th>";
										columnMappingDataStr += "<td width='2%'>"
												+ "<img src=\"images/Delete_Red_Icon.svg\" "
												+ "onclick=\"deleteSelectedRow(this)\" class=\"visionColMappingImg\" "
												+ "title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"></td>";
										addTrString += "<td width='2%'>" + "<img src=\"images/Delete_Red_Icon.svg\" "
												+ "onclick=\"deleteSelectedRow(this)\" class=\"visionColMappingImg\" "
												+ "title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"></td>";
									}
									columnMappingHeaderStr += "<th "
											+ "style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;"
											+ displayStyle + "' >" + "" + funFormColsArray[3] + "</th>";
									columnMappingDataStr += "<td style='" + displayStyle + "'>" + inputAttar + "</td>";
									addTrString += "<td style='" + displayStyle + "'>" + inputAttar + "</td>";
									if (i == funFormList.size() - 1) {
										columnMappingHeaderStr += "<th style='display:none' class=\"mappedColsTh1\" "
												+ "style='text-align: center'>Level Type</th>";
										columnMappingDataStr += "<td style='display:none'  >"
												+ "<input id='FUN_LVL_TYPE' class='visionColMappingInput'"
												+ " type='text'" + " value='" + selectedFunRowData.get("FUN_LVL_TYPE")
												+ "' /></td>";

										addTrString += "<td style='display:none'  >"
												+ "<input id='FUN_LVL_TYPE' class='visionColMappingInput'"
												+ " type='text'" + " value='" + selectedFunRowData.get("FUN_LVL_TYPE")
												+ "' /></td>";

										columnMappingHeaderStr += "<th style='display:none' class=\"mappedColsTh1\" "
												+ "style='text-align: center'>Level Type</th>";
										columnMappingDataStr += ""
												+ "<td style='display:none' ><input id='DM_FUN_CUST_COL1' class='visionColMappingInput'"
												+ " type='text'" + " value='"
												+ selectedFunRowData.get("DM_FUN_CUST_COL1") + "' /></td>";
										addTrString += ""
												+ "<td style='display:none' ><input id='DM_FUN_CUST_COL1' class='visionColMappingInput'"
												+ " type='text'" + " value='"
												+ selectedFunRowData.get("DM_FUN_CUST_COL1") + "' /></td>";

										columnMappingHeaderStr += "<th style='display:none' class=\"mappedColsTh1\" "
												+ "style='text-align: center'>Level Type</th>";
										columnMappingDataStr += ""
												+ "<td style='display:none'  ><input id='DM_FUN_CUST_COL2' class='visionColMappingInput'"
												+ " type='text'" + " value='"
												+ selectedFunRowData.get("DM_FUN_CUST_COL2") + "' /></td>";
										addTrString += ""
												+ "<td style='display:none'  ><input id='DM_FUN_CUST_COL2' class='visionColMappingInput'"
												+ " type='text'" + " value='"
												+ selectedFunRowData.get("DM_FUN_CUST_COL2") + "' /></td>";
										addTrString += "</tr>";
										columnMappingHeaderStr += "</tr>";
										columnMappingDataStr += "</tr>";
										resultObj.put("addTrString", addTrString);
										columnMappingStr += "<thead>" + columnMappingHeaderStr + "</tr></thead><tbody>"
												+ columnMappingDataStr + "</tbody>";
									}
								}

							}

						}

					}
					columnIconStr += "" + "<th><img src=\"images/SQL ICON-01.svg\"" + " id=\"viewFunQuery\" "
							+ " title=\"Click here to view the query\""
							+ " style=\"width:15px;height: 15px;cursor:pointer;\">" + "</th>" + "</tr></table></div>";
					columnMappingStr += "</table>" + "</div>";
					resultObj.put("funFormStr", columnIconStr + columnMappingStr);
					resultObj.put("messageFlag", true);//
				} else {
					resultObj.put("message", "Selected Row Data Empty");
					resultObj.put("messageFlag", false);//
				}
			} else {
				resultObj.put("message", "Selected Row Data Empty");
				resultObj.put("messageFlag", false);//
			}
		} catch (Exception e) {
			resultObj.put("message", e.getMessage());
			resultObj.put("messageFlag", false);
		}
		return resultObj.toJSONString();
	}

	public String fetchEtlComponents(HttpServletRequest request) {
		String htmlDiv = "";
		try {
			int j = 1;
			int cols = 3;
			List<Object[]> getEtlComponents = genericDataPipingDAO.getEtlComponents(request);
			for (int i = 0; i < getEtlComponents.size(); i++) {
				if (i % cols == 0) {
					htmlDiv += " <div class=innerMain> ";
				}
				Object[] etlComponentsArray = getEtlComponents.get(i);
				if (etlComponentsArray != null) {
					String divId = (String) etlComponentsArray[7];

					htmlDiv += " <div id= " + etlComponentsArray[7]
							+ " class=\"mappingIconClassVertical\" data-imgsrc='" + (String) etlComponentsArray[5] + "'"
							+ " data-optitle='" + etlComponentsArray[3] + "' " + "data-functionname='"
							+ etlComponentsArray[4] + "'  " + "component='" + etlComponentsArray[8] + "'  "
							+ "data-type='" + etlComponentsArray[2] + "' " + "title='" + etlComponentsArray[2] + "' > "
							+ " <img src='" + (String) etlComponentsArray[5] + "' class=\"visionETLIcons\"  /> "
							+ " <p>" + etlComponentsArray[6] + "</p> "
							// + "<div id='etlOperatorInfo' class='etlGroupComponentsInfo'
							// onclick='showOperatoriiii()' ><img src='images/etl/info.png'></div>"
							+ " </div> ";

				}
				if (i % cols == (cols - 1)) {
					htmlDiv += " </div>";
					j++;
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return htmlDiv;
	}

	public JSONArray getTreeMenu(HttpServletRequest request, String parentMenuId) {
		JSONArray menuArray = new JSONArray();
		try {
			List<Object[]> sourceConnectionsList = genericDataPipingDAO.getSourceConnectionsList(request, parentMenuId);
			if (sourceConnectionsList != null && !sourceConnectionsList.isEmpty()) {
				for (int i = 0; i < sourceConnectionsList.size(); i++) {
					Object[] rowData = sourceConnectionsList.get(i);
					JSONObject menuObj = new JSONObject();
					menuObj.put("id", rowData[2]);
					menuObj.put("PARENT_ID", rowData[6]);
					menuObj.put("PARENT_MENU_ID", rowData[6]);
					menuObj.put("MENU_ID", rowData[2]);
					menuObj.put("MENU_DESCRIPTION", "<span class='visionMenuTreeLabel'>" + rowData[4] + "</span>");
					menuObj.put("MAIN_DESCRIPTION", rowData[4]);
					menuObj.put("icon", rowData[5]);
//                    menuObj.put("iconsize", rowData[11]);
					menuObj.put("value", rowData[7]);
					menuObj.put("TOOL_TIP", rowData[8]);
					menuArray.add(menuObj);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return menuArray;
	}

	public JSONObject getGenericTreeOpt(HttpServletRequest request, String treeId) {
		JSONObject treeObj = new JSONObject();
		try {
			JSONObject labelsObj = new JSONObject();
			JSONObject treeConfigObj = new JSONObject();
			JSONObject treeInitParamObj = new JSONObject();
			JSONObject treeDefaultSource = new JSONObject();
			JSONObject treeColumnObj = new JSONObject();
			List<Object[]> treeList = genericDataPipingDAO.getTreeListOpt(request, treeId);

			if (treeList != null && !treeList.isEmpty()) {
				for (int i = 0; i < treeList.size(); i++) {
					Object[] treeObjArray = treeList.get(i);
					if (treeObjArray != null && treeObjArray.length != 0) {
						if (i == 0) {
							if (treeObjArray[2] != null && !"".equalsIgnoreCase(String.valueOf(treeObjArray[2]))
									&& !"null".equalsIgnoreCase(String.valueOf(treeObjArray[2]))) {
//                                treeObj.put("treeDesc", visionGenericDAO.replaceSessionValues(String.valueOf(treeObjArray[2]), request));
								treeObj.put("treeDesc", String.valueOf(treeObjArray[2]));
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
													? String.valueOf(treeObjArray[2])
													: treeObjArray[2]));
							treeDefaultSource.put("description",
									((treeObjArray[8] != null && !"".equalsIgnoreCase(String.valueOf(treeObjArray[8]))
											&& !"null".equalsIgnoreCase(String.valueOf(treeObjArray[8])))
													? String.valueOf(treeObjArray[2])
													: treeObjArray[2]));

							JSONArray sourceItems = new JSONArray();
							JSONObject itemObj = new JSONObject();
							itemObj.put("label",
									((treeObjArray[8] != null && !"".equalsIgnoreCase(String.valueOf(treeObjArray[8]))
											&& !"null".equalsIgnoreCase(String.valueOf(treeObjArray[8])))
													? String.valueOf(treeObjArray[2])
													: treeObjArray[2]));
							itemObj.put("value", "ajax");
							sourceItems.add(itemObj);
							treeDefaultSource.put("items", sourceItems);
							JSONObject treeInitParams = getInitParamObject(
									cloudUtills.clobToString((Clob) treeObjArray[19]));
//                            if (treeInitParams != null && !treeInitParams.isEmpty()
//                                    && treeInitParams.get("uuu_MultiTreeDlovId") != null
//                                    && !"".equalsIgnoreCase(String.valueOf(treeInitParams.get("uuu_MultiTreeDlovId")))
//                                    && !"null".equalsIgnoreCase(String.valueOf(treeInitParams.get("uuu_MultiTreeDlovId")))) {
//                                String selectBoxStr = genericDataPipingDAO.getLOV(request, (String) treeInitParams.get("uuu_MultiTreeDlovId"));
//                                if (selectBoxStr != null && !"".equalsIgnoreCase(selectBoxStr) && !"null".equalsIgnoreCase(selectBoxStr)) {
////                                    selectBoxStr = "Select Tree :<select id=\"treeSelectBox\" onchange=getSelectedTree()>" + selectBoxStr + "</select>";
//                                    selectBoxStr = "<span class=\"treesearchIconMain\"><img class=\"treesearchIcon\" src=\"images/Tree.png\" height=\"25px\" width=\"25px\"></span>"
//                                            + "<span class=\"treeSelectBoxMain\"><select id=\"treeSelectBox\" onchange=getSelectedTree()>" + selectBoxStr + "</select></span>";
//                                    treeObj.put("selectBoxStr", selectBoxStr);
//                                }
//                            }
							String result = "";
							// String searchField=genericTreeDAO.getCRmSearchString(request);
							result += "<div class='treeSearchtextcount' id='treeSearchtextcount'></div></h3>";
							result += "<div class='search_input_div smartsearchtb' >";

							result += "<input type='text' id='treeSearchResult' autocomplete='off' title='"
									+ cloudUtills.convertIntoMultilingualValue(
											labelsObj, "Enter atleast 3 characters to Search")
									+ "' " + "placeholder='  "
									+ new PilogUtilities().convertIntoMultilingualValue(labelsObj,
											"Type keyword(s) to search")
									+ "' "
									+ "data-no='NA' aria-haspopup='true' aria-multiline='false' aria-readonly='false' aria-disabled='false' aria-autocomplete='both' "
									+ "role='textbox' class='visionSearchClearResize clearable clearable2 ac jqx-widget-content jqx-widget-content-arctic jqx-input jqx-input-arctic jqx-widget jqx-widget-arctic jqx-rc-all jqx-rc-all-arctic smartserachclass' "
									+ "data-selected='NO'>"
									+ "  <a class='clear_searchField' style='position: absolute; font-size: 18px; cursor: pointer; display: none; top:2.5px; right: 5px;' onclick='clearTextSearch();'>×</a>";

							result += "</div><div class='TreesearchButton'>"
									// + " <input type='submit' id='getsmartsearch' data-source='" + treeObjArray[1]
									// + "' class='searchbutton' value='' onclick=searchResultsHandler('s','lucene')
									// id='result' title='Click here to Search'>"
									+ "                                <input type='submit' id='getsmartsearch' data-source='"
									+ treeObjArray[1]
									+ "' class='searchbutton' value='' onclick=treeSearchResultsHandler('"
									+ treeObjArray[1] + "') id='result' title='Click here to Search'>"
									+ "                            </div>";
							result += "<div data-selection-type='containing' data-text='NA' data-space='no' "
									+ "				class='smartsearchresultsviews' id='intellisensebox' "
									+ "			        style='background: transparent none repeat scroll 0% 0%;'>"
									+ "                         <div class='searchinnerclass' id='intellisense'></div>"
									+ "                         <div id='intellisense1'></div>"
									+ "                     </div>";

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
							columnObj.put("TREE_INIT_PARAMS",
									getInitParamObject(cloudUtills.clobToString((Clob) treeObjArray[19])));
//                                    visionGenericDAO.getInitParamObject(cloudUtills.clobToString((Clob) treeObjArray[19])));//TREE_INIT_PARAMS
						}
						treeColumnObj.put(i, columnObj);
					}

				}
			}
			JSONArray treeDefaultSourceArray = new JSONArray();
			treeDefaultSourceArray.add(treeDefaultSource);
			treeConfigObj.put("source", treeDefaultSourceArray);
			treeObj.put("treeConfigObj", treeConfigObj);
			treeObj.put("treeColumnObj", treeColumnObj);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return treeObj;
	}

	public JSONObject getColumnsObjFromQuery(HttpServletRequest request, String selectQuery, Connection connection,
			JSONObject connObj) {
		JSONObject resultObj = new JSONObject();
		List dataTypesList = new ArrayList();
		List colsList = new ArrayList();
		PreparedStatement preparedStatement = null;
		try {

			String statingTable = "ZZ_" + System.currentTimeMillis();
			String query = "CREATE TABLE " + statingTable + " AS " + selectQuery;
			preparedStatement = connection.prepareStatement(query);
			preparedStatement.execute();
			List<Object[]> fromColumnsObjList = getTableColumnsOpt(connection, request, (JSONObject) connObj,
					statingTable);
			colsList = fromColumnsObjList.stream().map(e -> (String) ((Object[]) e)[2]).collect(Collectors.toList());
			dataTypesList = fromColumnsObjList.stream().map(e -> (String) ((Object[]) e)[8])
					.collect(Collectors.toList());
			dropStagingTable(statingTable, connection);
			resultObj.put("dataTypesList", dataTypesList);
			resultObj.put("columnsList", colsList);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public List<Object[]> getColumnsListObjWithQuery(HttpServletRequest request, String selectQuery,
			Connection connection, JSONObject connObj) {

		List<Object[]> fromColumnsObjList = new ArrayList();
		List colsList = new ArrayList();
		PreparedStatement preparedStatement = null;
		try {

			String statingTable = "ZZ_" + System.currentTimeMillis();
			String query = "CREATE TABLE " + statingTable + " AS " + selectQuery;
			System.out.println("query ::: " + query);
			preparedStatement = connection.prepareStatement(query);
			preparedStatement.execute();
			fromColumnsObjList = getTableColumnsOpt(connection, request, (JSONObject) connObj, statingTable);
			dropStagingTable(statingTable, connection);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return fromColumnsObjList;
	}

	@Transactional
	public void dropStagingTable(String tableName, Connection connection) {
		PreparedStatement preparedStatement = null;
		Boolean tableDroped = false;
		try {
			String dropQuery = "DROP TABLE " + tableName;

			preparedStatement = connection.prepareStatement(dropQuery);
			tableDroped = preparedStatement.execute();
			System.out.println(tableName + " droped ");
		} catch (Exception e) {
			System.out.println(tableName + " drop failed ");
			e.printStackTrace();
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}

			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

	}

	public List getHeaderDataTypesOfImportedFile(HttpServletRequest request, String filePath) {
		List<String> headerDataTypes = new ArrayList();
		try {
			if (filePath != null && !"".equalsIgnoreCase(filePath) && !"null".equalsIgnoreCase(filePath)) {
				String fileExt = filePath.substring(filePath.lastIndexOf(".") + 1, filePath.length());

				if (fileExt != null && !"".equalsIgnoreCase(fileExt)) {
					if ("txt".equalsIgnoreCase(fileExt) || "csv".equalsIgnoreCase(fileExt)
							|| "json".equalsIgnoreCase(fileExt)) {

						CsvParserSettings settings = new CsvParserSettings();
						settings.detectFormatAutomatically();

						CsvParser parser = new CsvParser(settings);
						List<String[]> rows = parser.parseAll(new File(filePath));

						// if you want to see what it detected
//                        CsvFormatDetector formatdetect =  new CsvFormatDetector();
						char columnSeparator = ',';
						String fileType = request.getParameter("fileType");
//                        char columnSeparator = '\t';
//                        char columnSeparator = ',';
						if (!(fileType != null && !"".equalsIgnoreCase(fileType)
								&& !"null".equalsIgnoreCase(fileType))) {
							fileType = (String) request.getAttribute("fileType");
						}
						if (".json".equalsIgnoreCase(fileType)) {
							columnSeparator = ',';
						} else {
							CsvFormat format = parser.getDetectedFormat();
							columnSeparator = format.getDelimiter();
						}

						CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(filePath), "UTF8"),
								columnSeparator);

						String[] nextLine;
						reader.readNext();
						List<String> values = null;
						while ((nextLine = reader.readNext()) != null) {
							if (nextLine.length != 0 && nextLine[0].contains("" + columnSeparator)) {
								values = new ArrayList<>(Arrays.asList(nextLine[0].split("" + columnSeparator)));
							} else {
								values = new ArrayList<>(Arrays.asList(nextLine));
							}

							break;
						}
						for (int i = 0; i < values.size(); i++) {
							String value = values.get(i);

							int dataTypeLength = value.length();
							String dataType = getOracleDataTypeOfValue(value, dataTypeLength);
							headerDataTypes.add(dataType);
						}

					} else if ("xls".equalsIgnoreCase(fileExt) || "xlsx".equalsIgnoreCase(fileExt)) {
						headerDataTypes = new ArrayList<>();
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
						if (workBook.getSheetAt(sheetNo) instanceof XSSFSheet) {
							sheet = (XSSFSheet) workBook.getSheetAt(sheetNo);
						} else if (workBook.getSheetAt(sheetNo) instanceof HSSFSheet) {
							sheet = (HSSFSheet) workBook.getSheetAt(sheetNo);
						}
//                sheet = (XSSFSheet) xssfWb.getSheetAt(0);
//                        }
						if (sheet != null) {
							Row columnCountrow = sheet.getRow(0);
							int columnCount = columnCountrow.getLastCellNum();
							Row row = sheet.getRow(1);
							if (row != null) {
								for (int j = 0; j < columnCount; j++) {
									// System.out.println("Cell Num:::" + j + ":::Start Date And Time :::" + new
									// Date());

									try {
										Cell cell = row.getCell(j);
										// String value = cell.getStringCellValue();
										// int dataTypeLength = value.length();
										// String dataType = getOracleDataTypeOfValue(value, dataTypeLength);
										// headerDataTypes.add(dataType);
										if (cell != null) {

											switch (cell.getCellType()) {
											case Cell.CELL_TYPE_STRING:
												headerDataTypes.add("VARCHAR2(4000)");
												break;
											case Cell.CELL_TYPE_BOOLEAN:
												headerDataTypes.add("BOOLEAN");
//                                rowObj.put(header, hSSFCell.getBooleanCellValue());
												break;
											case Cell.CELL_TYPE_NUMERIC:

												if (HSSFDateUtil.isCellDateFormatted(cell)) {
													headerDataTypes.add("DATE");
//                                            
												} else {
													double numericValue = cell.getNumericCellValue();
													if (numericValue == Math.floor(numericValue)) {
														headerDataTypes.add("NUMBER"); // or "INTEGER" if you prefer
													} else {
														headerDataTypes.add("FLOAT"); // or "NUMBER(p, s)" with
																						// precision and scale if needed
													}
												}
												break;
											case Cell.CELL_TYPE_BLANK:
												headerDataTypes.add("VARCHAR2(4000)");
												break;
											}
										} else {
											headerDataTypes.add("VARCHAR2(4000)");
//                            testMap.put(stmt, "");
										}
									} catch (Exception e) {
										e.printStackTrace();
										headerDataTypes.add("VARCHAR2(100)");
										continue;
									}

								} // end of row cell loop
							}
						}

					} else if ("xml".equalsIgnoreCase(fileExt)) {
						headerDataTypes = new ArrayList<>();
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
//                                Node node = dataNodeList.item(0);
//                                if (node.getNodeType() == Node.ELEMENT_NODE) {
//                                    NodeList childNodeList = node.getChildNodes();
//                                    for (int i = 0; i < childNodeList.getLength(); i++) {// Columns
//
//                                        Node childNode = childNodeList.item(i);
//                                        if (childNode != null
//                                                && !"#Text".equalsIgnoreCase(childNode.getNodeName())) {
//                                            headerDataTypes.add(childNode.getNodeName());
//                                            System.err.println(childNode.getNodeName() + "---> " + childNode.getTextContent());
//                                        }
//
//                                    }// end of columns loop
//
//                                }

								Node node = dataNodeList.item(1);

								if (node != null && node.getNodeType() == Node.ELEMENT_NODE) {
//                            JSONObject dataObject = new JSONObject();

									// dataObject.put("totalrecords", rowCount);
									NodeList childNodeList = node.getChildNodes();
									Object[] dataObject = new Object[childNodeList.getLength()];
									for (int j = 0; j < childNodeList.getLength(); j++) {
										try {
											Node childNode = childNodeList.item(j);
//                                    int childNodeIndex = j;
//                                    int nodeListLength = childNodeList.getLength();
											if (childNode != null) {
												if (childNode != null && childNode.getNodeType() == Node.ELEMENT_NODE) {
													try {
														if (childNode.getTextContent() != null
																&& !"".equalsIgnoreCase(childNode.getTextContent())
																&& !"null"
																		.equalsIgnoreCase(childNode.getTextContent())) {
//                                                    dataObject.put(fileName + ":" + childNode.getNodeName(), childNode.getTextContent());
															String value = childNode.getTextContent();
															int dataTypeLength = value.length();
															String dataType = getOracleDataTypeOfValue(value,
																	dataTypeLength);
															headerDataTypes.add(dataType);

														} else {
//                                                    dataObject.put(fileName + ":" + childNode.getNodeName(), "");
															headerDataTypes.add("VARCHAR2(100)");
														}

													} catch (Exception e) {
														headerDataTypes.add("VARCHAR2(100)");
														continue;
													}
													// Need to set the Data

												}
											}
										} catch (Exception e) {

											continue;
										}

									} // column list loop

								}

							}

						}
					} else if ("pdf".equalsIgnoreCase(fileExt)) {
						headerDataTypes = new ArrayList();
						// List resultArray = dataMigrationService.readPDFRestApi(request, filePath);
						String result = dataMigrationService.readPDFRestApi(request, filePath);
						JSONObject apiPdfJsonData = (JSONObject) JSONValue.parse(result);
						List<String> headerArray = (List<String>) apiPdfJsonData.get("columns");
						List<List<String>> resultArray = (List<List<String>>) apiPdfJsonData.get("data");
						if (resultArray != null && !resultArray.isEmpty()) {
							LinkedHashMap rowObj = (LinkedHashMap) resultArray.get(0);
//                            Iterator itr = rowObj.values().iterator();
							for (int i = 0; i < rowObj.size(); i++) {
								headerDataTypes.add("VARCHAR2(4000)");
							}

						}

					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return headerDataTypes;
	}

	public String getOracleDataTypeOfValue(String value, int length) {
		String dataType = "";
		try {
			if (value != null) {
				if (isNumeric(value)) {
					dataType = "NUMBER";
				} else if (isValidDate(value)) {
					dataType = "DATE";
				} else if (isBooleanValue(value)) {
					dataType = "BOOLEAN";
				} else if (isCharacter(value)) {
					dataType = "VARCHAR2(4)";
				} else {
					dataType = "VARCHAR2(" + (length + 100) + ")";
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return dataType;
	}

	public JSONObject setFileDataType(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		List<String> columnList = new ArrayList<>();
		List oracleDataTypesList = new ArrayList();
		JSONArray data = new JSONArray();
		try {
			String fileObjStr = request.getParameter("fileObj");
			JSONObject fileObj = (JSONObject) JSONValue.parse(fileObjStr);

			if (fileObj != null && !fileObj.isEmpty()) {
				String fileType = (String) fileObj.get("fileType");
				String fileName = (String) fileObj.get("fileName");
				String filePath = (String) fileObj.get("filePath");
				columnList = genericProcessETLDataService.getHeadersOfImportedFile(filePath, fileType);
				String savedDataTypesObjStr = request.getParameter("dataTypesObj");
				JSONObject savedDataTypesObj = new JSONObject();
				List fileDataTypeList = new ArrayList();
				if (savedDataTypesObjStr != null && !"".equalsIgnoreCase(savedDataTypesObjStr)) {
					savedDataTypesObj = (JSONObject) JSONValue.parse(savedDataTypesObjStr);
				} else {
					fileDataTypeList = getHeaderDataTypesOfImportedFile(request, filePath);
				}
				oracleDataTypesList = tableOperationsDAO.getListOfDataTypes(request, "ORACLE");
				resultObj.put("dataTypesList", oracleDataTypesList);
				for (int i = 0; i < columnList.size(); i++) {

					JSONObject row = new JSONObject();
					String columnName = columnList.get(i);

					String datatypeName = "VARCHAR2";
					String columnsize = "4000";
					if (savedDataTypesObj != null && !savedDataTypesObj.isEmpty()) {
						JSONObject dataTypeObj = (JSONObject) savedDataTypesObj.get(columnName);
						datatypeName = (String) dataTypeObj.get("datatype");
						columnsize = String.valueOf(dataTypeObj.get("columnsize"));
					} else {
						String dataTypeWithLength = (String) fileDataTypeList.get(i);
						if (dataTypeWithLength.contains("(")) {
							datatypeName = dataTypeWithLength.split("\\(")[0];
							columnsize = dataTypeWithLength.split("\\(")[1];
							columnsize = columnsize.substring(0, columnsize.indexOf(")") - 1);
							if (columnsize.contains(",")) {
								columnsize = columnsize.split(",")[0];
							}
							columnsize = columnsize.trim();
						} else {
							datatypeName = dataTypeWithLength;
							columnsize = "";
						}
					}

					row.put("columnName", columnName);
					row.put("datatypeName", datatypeName);
					row.put("columnsize", columnsize);
					data.add(row);
				}
				resultObj.put("data", data);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject provideDALAuthorisation(HttpServletRequest request, String tableName, String username) {
		JSONObject resultObj = new JSONObject();
		List<String> columnList = new ArrayList<>();
		List oracleDataTypesList = new ArrayList();
		JSONArray data = new JSONArray();
		try {

			genericDataPipingDAO.provideDALAuthorisation(request, tableName, username);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject fetchSubJobProcessLog(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		String logText = "";
		try {

			String jobId = request.getParameter("jobId");
			String subJobId = request.getParameter("subJobId");

			List<Object[]> processlogDataList = genericDataPipingDAO.fetchSubJobProcessLog(
					(String) request.getSession(false).getAttribute("ssUsername"),
					(String) request.getSession(false).getAttribute("ssOrgId"), jobId, subJobId);
			if (processlogDataList != null && !processlogDataList.isEmpty()) {
				for (int i = 0; i < processlogDataList.size(); i++) {
					Object[] processlogDataArray = processlogDataList.get(i);
					if (processlogDataArray != null && processlogDataArray.length != 0) {
						String imagePath = "images/information.gif";// erroricon.png
						String contentClass = "visionETLLogInfo";
						if ("ERROR".equalsIgnoreCase(String.valueOf(processlogDataArray[0]))) {
							imagePath = "images/erroricon.png";
							contentClass = "visionETLLogError";
						} else if ("WARNING".equalsIgnoreCase(String.valueOf(processlogDataArray[0]))) {
							imagePath = "images/44warning.png";
							contentClass = "visionETLLogWarning";
						}
						logText += "<tr>"// 44warning.png
								+ "<td width='5%'><img src='" + imagePath
								+ "' style='width:16px;height:16px;padding:2px'></td>" + "<td width='25%'>"
								+ processlogDataArray[2] + "</td>" + "<td width='70%' class='" + contentClass + "'>"
								+ processlogDataArray[1] + "</td>" + "</tr>";

					}

				}
//                      System.out.println("currentProcesslogIndex:::" + currentProcesslogIndex);
				resultObj.put("logTxt", logText);

			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return resultObj;
	}

	public JSONObject getInitParamObject(String gridInitParams) {
		JSONObject gridOperation = new JSONObject();
		try {
			if (gridInitParams != null && !"".equalsIgnoreCase(gridInitParams)
					&& !"null".equalsIgnoreCase(gridInitParams)) {
				String[] operationIconsArray = gridInitParams.split("&");
				for (int j = 0; j < operationIconsArray.length; j++) {
					if (operationIconsArray[j] != null && !"null".equalsIgnoreCase(operationIconsArray[j])
							&& !"".equalsIgnoreCase(operationIconsArray[j])) {
						String[] paramArray = operationIconsArray[j].split("=");
						if (paramArray != null && paramArray.length != 0) {
							if ("uuu_TableView".equalsIgnoreCase(paramArray[0])) {
								gridOperation.put("tableview",
										((paramArray[1] != null && paramArray[1].equalsIgnoreCase("Y")) ? "Y" : "N"));
							} else if (paramArray[0] != null && !"".equalsIgnoreCase(paramArray[0])
									&& paramArray[1] != null && !"".equalsIgnoreCase(paramArray[1])) {
								if ("persInd".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("persInd",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
									// gridOperation.put("addIcon", "images/add_icon.png");
								} else if ("uuu_HideGraph".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("umgraph",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
									// gridOperation.put("addIcon", "images/add_icon.png");
								} else if ("uuu_HideEditExport".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("editExportFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
									// gridOperation.put("addIcon", "images/add_icon.png");
								} else if ("uuu_HideUMUpdate".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("umupdate",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
									// gridOperation.put("addIcon", "images/add_icon.png");
								} else if ("uuu_RunAnalysis".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("runAnalysis",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
									// gridOperation.put("addIcon", "images/add_icon.png");
								} else if ("uuu_IsUM".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("IsUM",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
									// gridOperation.put("addIcon", "images/add_icon.png");
								} else if ("uuu_HideFormInsert".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("gridformInsert",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
									// gridOperation.put("addIcon", "images/add_icon.png");
								} else if ("uuu_Hide_UnlockUser".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("unlockUsrFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
									// gridOperation.put("addIcon", "images/add_icon.png");
								} else if ("uuu_Hide_ResetUser".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("resetUsrFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
									// gridOperation.put("addIcon", "images/add_icon.png");
								} else if ("uuu_HideInsert".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("addFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
									// gridOperation.put("addIcon", "images/add_icon.png");
								} else if ("uuu_HideUpdate".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("editFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
									// gridOperation.put("editIcon", "images/update_icon.png");
								} else if ("uuu_SrsNewRegister".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("srsRegisterFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
									// gridOperation.put("addIcon", "images/add_icon.png");
								} else if ("uuu_HideDelete".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("deleteFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
									// gridOperation.put("deleteIcon", "images/delete_icon.png");
								} else if ("uuu_HideRefresh".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("refreshFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_HidePaging".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("pagingFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
									// gridOperation.put("refreshIcon", "images/refresh_icon.png");
								} else if ("uuu_HideImport".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("importFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
//                                    gridOperation.put("importIcon", gridObjectArray[33]);
								} else if ("uuu_importSPIRButton".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("importSPIRButton",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_verifySPIRButton".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("verifySPIRButton",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_registerSPIRButton".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("registerSPIRButton",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_deleteSPIRButton".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("deleteSPIRButton",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_HideExport".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("exportFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
//                                    gridOperation.put("importIcon", gridObjectArray[33]);
								} else if ("uuu_HideExportExcel".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("exportExcelFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
									// gridOperation.put("exportIcon", "");
								} else if ("uuu_HideExportCSV".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("exportCSVFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
									// gridOperation.put("exportIcon", "");
								} else if ("uuu_HideExportPDF".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("exportPDFFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_EncEditable".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("encEditable",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("Y")) ? "Y"
													: "N"));
//                                    gridOperation.put("exportIcon", gridObjectArray[33]);
								} else if ("uuu_FormView".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("formEditable",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("Y")) ? "Y"
													: "N"));
//                                    gridOperation.put("exportIcon", gridObjectArray[33]);
								} else if ("uuu_OrderBy".equalsIgnoreCase(paramArray[0])
										&& (paramArray[1] != null && !"".equalsIgnoreCase(paramArray[1]))) {
									gridOperation.put("orderBy", paramArray[1]);
//                                    gridOperation.put("exportIcon", gridObjectArray[33]);
								} else if ("uuu_GroupBy".equalsIgnoreCase(paramArray[0])
										&& (paramArray[1] != null && !"".equalsIgnoreCase(paramArray[1]))) {
									gridOperation.put("groupBy", paramArray[1]);
								} else if ("uuu_OpFunName".equalsIgnoreCase(paramArray[0])
										&& (paramArray[1] != null && !"".equalsIgnoreCase(paramArray[1]))) {
									gridOperation.put("opFunctionName", paramArray[1]);
								} else if ("uuu_GridRowHeight".equalsIgnoreCase(paramArray[0])
										&& (paramArray[1] != null && !"".equalsIgnoreCase(paramArray[1]))) {
									gridOperation.put("uuu_GridRowHeight", paramArray[1]);
								} else if ("uuu_nonInstance".equalsIgnoreCase(paramArray[0])
										&& (paramArray[1] != null && !"".equalsIgnoreCase(paramArray[1]))) {
									gridOperation.put("NON-INSTANCE", paramArray[1]);
								} else if ("uuu_instance".equalsIgnoreCase(paramArray[0])
										&& (paramArray[1] != null && !"".equalsIgnoreCase(paramArray[1]))) {
									gridOperation.put("INSTANCE", paramArray[1]);
								} else if ("uuu_poExt".equalsIgnoreCase(paramArray[0])
										&& (paramArray[1] != null && !"".equalsIgnoreCase(paramArray[1]))) {
									gridOperation.put("PO_EXT", paramArray[1]);
								} else if ("uuu_ccExt".equalsIgnoreCase(paramArray[0])
										&& (paramArray[1] != null && !"".equalsIgnoreCase(paramArray[1]))) {
									gridOperation.put("CC_EXT", paramArray[1]);
								} else if ("uuu_poAndCcExt".equalsIgnoreCase(paramArray[0])
										&& (paramArray[1] != null && !"".equalsIgnoreCase(paramArray[1]))) {
									gridOperation.put("PO_AND_CC_EXT", paramArray[1]);
								} else if ("uuu_soExt".equalsIgnoreCase(paramArray[0])
										&& (paramArray[1] != null && !"".equalsIgnoreCase(paramArray[1]))) {
									gridOperation.put("SO_EXT", paramArray[1]);
								} else if ("uuu_soAndCcExt".equalsIgnoreCase(paramArray[0])
										&& (paramArray[1] != null && !"".equalsIgnoreCase(paramArray[1]))) {
									gridOperation.put("SO_AND_CC_EXT", paramArray[1]);
								} else if ("uuu_panelId".equalsIgnoreCase(paramArray[0])
										&& (paramArray[1] != null && !"".equalsIgnoreCase(paramArray[1]))) {
									gridOperation.put("PANEL_ID", paramArray[1]);
								} else if ("uuu_gridId".equalsIgnoreCase(paramArray[0])
										&& (paramArray[1] != null && !"".equalsIgnoreCase(paramArray[1]))) {
									gridOperation.put("GRID_ID", paramArray[1]);
								} else if ("uuu_ShowExtPlantGrid".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("ShowExtPlantGrid",
											((paramArray[1] != null && !"".equalsIgnoreCase(paramArray[1]))
													? paramArray[1]
													: "N"));
								} else if ("uuu_HideAttachForm".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("gridformAttach",
											((paramArray[1] != null && "N".equalsIgnoreCase(paramArray[1])) ? "Y"
													: "N"));
								} else if ("uuu_AutoGenerateColumns".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("autoGenerateColumns",
											((paramArray[1] != null && !"".equalsIgnoreCase(paramArray[1]))
													? paramArray[1]
													: ""));
								} else if ("uuu_importDomain".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("importDomain",
											((paramArray[1] != null && !"".equalsIgnoreCase(paramArray[1]))
													? paramArray[1]
													: ""));
								} else if ("uuu_HideAuditViewFlag".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("auditViewFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_HideAuditGridId".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("auditGridId", paramArray[1]);
								} else if ("uuu_HideclauseColumns".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("clauseColumns", paramArray[1]);
								} else if ("uuu_HideSpirAttachForm".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("uuu_HideSpirAttachForm", paramArray[1]);
								} else if ("uuu_fillDownButton".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("fillDownButton",
											((paramArray[1] != null && "Y".equalsIgnoreCase(paramArray[1])) ? "Y"
													: "N"));
								} else if ("uuu_fillDownColumns".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("fillDownColumns", paramArray[1]);
								} else if ("uuu_HideEditableFlag".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("uuu_HideEditableFlag", paramArray[1]);
								} else if ("uuu_HideTableName".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("uuu_HideTableName", paramArray[1]);
								} else if ("uuu_HideVariableName".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("uuu_HideVariableName", paramArray[1]);
								} else if ("uuu_HideAttachEditableFlag".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("uuu_HideAttachEditableFlag", paramArray[1]);
								} else if ("uuu_massParams".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("massParams", paramArray[1]);
								} else if ("uuu_massValidationId".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("massValidationId", paramArray[1]);
								} else if ("uuu_massValidate".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("massValidateButton", paramArray[1]);
								} else if ("uuu_massDHProcess".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("massDHProcess", paramArray[1]);
								} else if ("uuu_massProcessData".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("bulkCreate", paramArray[1]);
								} else if ("uuu_massDHProcName".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("dhProcName", paramArray[1]);
								} else if ("uuu_massViewName".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("massViewName", paramArray[1]);
								} else if ("uuu_massPPRSearchButton".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("massPPRSearch", paramArray[1]);
								} else if ("uuu_massCallCopyQuery".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("copyQueryFlag", paramArray[1]);
								} else if ("uuu_runQCTool".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("runQCToolFlag", paramArray[1]);
								} else if ("uuu_massDuplCheckFlag".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("duplCheckFlag", paramArray[1]);
								} else if ("uuu_ClearStagingTable".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("clearStagingTable", paramArray[1]);
								} else if ("uuu_massCopyId".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("massCopyId", paramArray[1]);
								} else if ("uuu_masterGridInd".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("massMasterGridInd", paramArray[1]);
								} else if ("uuu_masterChngInd".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("massMasterChngInd", paramArray[1]);
								} else if ("uuu_massColumnToHide".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("massColumnHide", paramArray[1]);
								} else if ("uuu_massTableUpdate".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("massTableUpdate", paramArray[1]);
								} else if ("uuu_clusterRefresh".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("clusterRefreshFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_HideOpenDoc".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("gridOpenDocument",
											((paramArray[1] != null && "N".equalsIgnoreCase(paramArray[1])) ? "Y"
													: "N"));
								} else if ("uuu_HideOpenDocClassName".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("className", paramArray[1]);
								} else if ("uuu_HideOpenDocMethodName".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("methodName", paramArray[1]);
								} else if ("uuu_populateAdminFileBrowser".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("populateAdminFileBrowser",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_gridDownloadTemplate".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("downloadTemplate",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_gridCalculateStock".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("calculateStock",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_importColumnToExclude".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("uuu_excludeColumns", paramArray[1]);
								} else if ("uuu_processMrpPlanData".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("processMrpPlanData",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_gridNoRefresh".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("gridNoRefresh",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_gridDataDHURL".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("DHUrl", paramArray[1]);
								} else if ("uuu_genericRegisterButton".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("registerButtonFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_genericRegisterGridId".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("registerButtonId", paramArray[1]);
								} else if ("uuu_callBapiFlag".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("callBapiFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_calculateBapiName".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("calculateBapiName", paramArray[1]);
								} else if ("uuu_tableToUpdate".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("calculateTableUpd", paramArray[1]);
								} else if ("uuu_calculateBapiMethodName".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("calculateBapiMethodName", paramArray[1]);
								} else if ("uuu_calculateColumnsToUpdate".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("calculateColumnsToUpdate", paramArray[1]);
								} else if ("uuu_calculateWhereCondColumns".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("calculateWhereColumns", paramArray[1]);
								} else if ("uuu_sapFileProcessButton".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("bulkUploadFileProcessFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_HideSapImagesImport".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("bulkUploadImagesImportFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
//                                    gridOperation.put("importIcon", gridObjectArray[33]);
								} else if ("uuu_HideSapDataImport".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("bulkUploadDataImportFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
//                                    gridOperation.put("importIcon", gridObjectArray[33]);
								} else if ("uuu_massexcluderow".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("massexcluderow", paramArray[1]);
								} else if ("uuu_ValidColumns".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("uuu_ValidColumns", paramArray[1]);
								} else if ("uuu_TaxonomyNewTemplate".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("taxonomyNewFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_TaxonomyDrTemplate".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("taxonomyDrFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_TaxonomyUpdateTemplate".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("taxonomyUpdateFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_TaxonomyPropTemplate".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("taxonomyPropertyFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_TaxonomyModifyTemplate".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("taxonomyModifierFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_TaxonomyHome".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("taxonomyHomeFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_TaxonomyCloud".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("taxonomyCloudFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_taxonomyClassDel".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("taxonomyClsDelFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_TxmnyAppProcess".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("txmnyAppFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_TxmnyDridProcess".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("txmnyDridFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_TxmnyDridAppProcess".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("txmnyDridAppFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_TxmnyDridStagingProcess".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("txmnyDridStagingFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_DataHarmImport".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("dataHarmFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_Downloadtemplet".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("downloadTempletFlag",
											((paramArray[1] != null && paramArray[1].equalsIgnoreCase("N")) ? "Y"
													: "N"));
								} else if ("uuu_imageUploadLimit".equalsIgnoreCase(paramArray[0])
										&& (paramArray[1] != null && !"".equalsIgnoreCase(paramArray[1]))) {
									gridOperation.put("uuu_imageUploadLimit", paramArray[1]);
								} else if ("uuu_filesUploadSizeInMB".equalsIgnoreCase(paramArray[0])
										&& (paramArray[1] != null && !"".equalsIgnoreCase(paramArray[1]))) {
									gridOperation.put("uuu_filesUploadSizeInMB", paramArray[1]);
								} else if ("uuu_ModelSpecDuplicateCheckMergeFlag".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("ModelSpecDuplicateChecFlag", paramArray[1]);
								} else if ("uuu_ModelSpecDuplicateChecButtonFlag".equalsIgnoreCase(paramArray[0])) {
									gridOperation.put("ModelSpecDuplicateChecButtonFlag", paramArray[1]);
								} else {
									gridOperation.put(paramArray[0], paramArray[1]);
								}
							}
						}
					}

				} // for
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return gridOperation;
	}

	public JSONObject sequenceMappingColumns(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String columnsStr = "";
			String toTableColsArrayStr = request.getParameter("toTableColsArrayStr");
			String fromTableColsArrayStr = request.getParameter("fromTableColsArrayStr");
			String selectionArrayStr = request.getParameter("selection");
			String selectionType = request.getParameter("selectionType");
			if (!(toTableColsArrayStr != null && !"".equalsIgnoreCase(toTableColsArrayStr)
					&& !"null".equalsIgnoreCase(toTableColsArrayStr) && !"[]".equalsIgnoreCase(toTableColsArrayStr))) {
				toTableColsArrayStr = fromTableColsArrayStr;
			}
			List<Map> selectionList = new ArrayList<>();
			if ("ALL".equalsIgnoreCase(selectionType)) {
				if (fromTableColsArrayStr != null && !"".equalsIgnoreCase(fromTableColsArrayStr)
						&& !"null".equalsIgnoreCase(fromTableColsArrayStr)) {
					selectionList = (List<Map>) JSONValue.parse(fromTableColsArrayStr);
				}
			} else {
				if (selectionArrayStr != null && !"".equalsIgnoreCase(selectionArrayStr)
						&& !"null".equalsIgnoreCase(selectionArrayStr)) {
					selectionList = (List<Map>) JSONValue.parse(selectionArrayStr);
				}
			}

			if (toTableColsArrayStr != null && !"".equalsIgnoreCase(toTableColsArrayStr)
					&& !"null".equalsIgnoreCase(toTableColsArrayStr) && !"[]".equalsIgnoreCase(toTableColsArrayStr)) {
				JSONArray toTableColsArray = (JSONArray) JSONValue.parse(toTableColsArrayStr);
				if (toTableColsArray != null && !toTableColsArray.isEmpty()
						&& selectionList.size() <= toTableColsArray.size()) {
					for (int i = 0; i < toTableColsArray.size(); i++) {
						JSONObject columnObj = (JSONObject) toTableColsArray.get(i);
						if (columnObj != null && !columnObj.isEmpty() && columnObj.containsKey("parentid")) {

							Map sourceMap = new HashMap();
							if (selectionList != null && !selectionList.isEmpty()) {
								try {
									sourceMap = selectionList.get(i);
									if (sourceMap != null && !sourceMap.isEmpty()) {
										columnsStr += "<tr>"
												// + "<th width='5%' style='background: #0071c5 none repeat scroll 0
												// 0;color: #FFF;text-align: center'></th>"//delete
												+ "<td width='35%'>"
												+ "<input class='visionColMappingInput' type='text' title='"
												+ ((sourceMap != null && !sourceMap.isEmpty()) ? sourceMap.get("value")
														: "")
												+ "'" + " value='"
												+ ((sourceMap != null && !sourceMap.isEmpty()) ? sourceMap.get("value")
														: "")
												+ "' actual-value='"
												+ ((sourceMap != null && !sourceMap.isEmpty()) ? sourceMap.get("id")
														: "")
												+ "' />" + "</td>"// Destination
												+ "<td width='35%'>"
												+ "<input class='visionColMappingInput' type='text' value='"
												+ columnObj.get("value") + "' actual-value='" + columnObj.get("id")
												+ "' title='" + columnObj.get("value") + "' readonly='true'/>" + "</td>"// Source
												// + "<th width='35%' style='background: #0071c5 none repeat scroll 0
												// 0;color: #FFF;text-align: center'>Default Values</th>"
												+ "</tr>";
									}
								} catch (Exception e) {
									columnsStr += "<tr>"
											// + "<th width='5%' style='background: #0071c5 none repeat scroll 0
											// 0;color: #FFF;text-align: center'></th>"//delete
											+ "<td width='35%'>"
											+ "<input class='visionColMappingInput' type='text' title='' value=''/>"
											+ "</td>"// Destination
											+ "<td width='35%'>"
											+ "<input class='visionColMappingInput' type='text' value='"
											+ columnObj.get("value") + "' actual-value='" + columnObj.get("id")
											+ "' title='" + columnObj.get("value") + "' readonly='true'/>" + "</td>"// Source
											// + "<th width='35%' style='background: #0071c5 none repeat scroll 0
											// 0;color: #FFF;text-align: center'>Default Values</th>"
											+ "</tr>";
								}
							}

						}

					}
				} else if (toTableColsArray != null && !toTableColsArray.isEmpty()
						&& selectionList.size() > toTableColsArray.size()) {
					for (int i = 0; i < selectionList.size(); i++) {

						Map sourceMap = new HashMap();
						if (selectionList != null && !selectionList.isEmpty()) {
							try {
								sourceMap = selectionList.get(i);
								JSONObject columnObj = (JSONObject) toTableColsArray.get(i);
								if (columnObj != null && !columnObj.isEmpty() && columnObj.containsKey("parentid")) {
									if (sourceMap != null && !sourceMap.isEmpty()) {
										columnsStr += "<tr>"
												// + "<th width='5%' style='background: #0071c5 none repeat scroll 0
												// 0;color: #FFF;text-align: center'></th>"//delete
												+ "<td width='35%'>"
												+ "<input class='visionColMappingInput' type='text' title='"
												+ ((sourceMap != null && !sourceMap.isEmpty()) ? sourceMap.get("value")
														: "")
												+ "'" + " value='"
												+ ((sourceMap != null && !sourceMap.isEmpty()) ? sourceMap.get("value")
														: "")
												+ "' actual-value='"
												+ ((sourceMap != null && !sourceMap.isEmpty()) ? sourceMap.get("id")
														: "")
												+ "' />" + "</td>"// Destination
												+ "<td width='35%'>"
												+ "<input class='visionColMappingInput' type='text' value='"
												+ columnObj.get("value") + "' actual-value='" + columnObj.get("id")
												+ "' title='" + columnObj.get("value") + "' readonly='true'/>" + "</td>"// Source
												// + "<th width='35%' style='background: #0071c5 none repeat scroll 0
												// 0;color: #FFF;text-align: center'>Default Values</th>"
												+ "</tr>";
									}
								}
							} catch (Exception e) {
//                                columnsStr += "<tr>"
//                                        //                    + "<th width='5%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'></th>"//delete
//                                        + "<td width='35%'>"
//                                        + "<input class='visionColMappingInput' type='text' title='" + ((sourceMap != null && !sourceMap.isEmpty()) ? sourceMap.get("value") : "") + "'"
//                                        + " value='" + ((sourceMap != null && !sourceMap.isEmpty()) ? sourceMap.get("value") : "") + "' actual-value='" + ((sourceMap != null && !sourceMap.isEmpty()) ? sourceMap.get("id") : "") + "' />"
//                                        + "</td>"//Destination
//                                        + "<td width='35%'>"
//                                        + "<input class='visionColMappingInput' type='text' value=''/>"
//                                        + "</td>"//Source
//                                        //                    + "<th width='35%' style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Default Values</th>"
//                                        + "</tr>";
							}
						}

					}
				}
			} else if (fromTableColsArrayStr != null && !"".equalsIgnoreCase(fromTableColsArrayStr)
					&& !"null".equalsIgnoreCase(fromTableColsArrayStr)) {
				JSONArray fromTableColsArray = (JSONArray) JSONValue.parse(fromTableColsArrayStr);
				if (fromTableColsArray != null && !fromTableColsArray.isEmpty()) {
					for (int i = 0; i < fromTableColsArray.size(); i++) {
						JSONObject columnObj = (JSONObject) fromTableColsArray.get(i);
						if (columnObj != null && !columnObj.isEmpty() && columnObj.containsKey("parentid")) {
							columnsStr += "<tr>"
									// + "<th width='5%' style='background: #0071c5 none repeat scroll 0 0;color:
									// #FFF;text-align: center'></th>"//delete
									+ "<td width='35%'>" + "<input class='visionColMappingInput' type='text' value='"
									+ columnObj.get("value") + "' actual-value='" + columnObj.get("id") + "' title='"
									+ columnObj.get("value") + "' readonly='true'/>" + "</td>"// Destination
									// + "<th width='35%'></th>"//Source
									// + "<th width='35%' style='background: #0071c5 none repeat scroll 0 0;color:
									// #FFF;text-align: center'>Default Values</th>"
									+ "</tr>";

						}

					}
				}
			}
			resultObj.put("columnsStr", columnsStr);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public List readCsvOrTextFile(HttpServletRequest request, String filePath, int limit) {
		List<Object[]> headers = new ArrayList<>();
		try {
			CsvParserSettings settings = new CsvParserSettings();
			settings.detectFormatAutomatically();

			CsvParser parser = new CsvParser(settings);
			List<String[]> rows = parser.parseAll(new File(filePath));

			// if you want to see what it detected
//                    CsvFormatDetector formatdetect =  new CsvFormatDetector();
			CsvFormat format = parser.getDetectedFormat();
			char columnSeparator = format.getDelimiter();

			String fileType = request.getParameter("fileType");
//                    char columnSeparator = '\t';
//                    char columnSeparator = ',';
			if (!(fileType != null && !"".equalsIgnoreCase(fileType) && !"null".equalsIgnoreCase(fileType))) {
				fileType = (String) request.getAttribute("fileType");
			}
			if (".json".equalsIgnoreCase(fileType)) {
				columnSeparator = ',';
			}
			try (CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(filePath), "UTF8"),
					columnSeparator)) {
				String[] nextLine;
				int rowCounter = 0;
				while ((nextLine = reader.readNext()) != null && rowCounter < limit) {
					Object[] dataObject = new Object[nextLine.length];

					for (int cellIndex = 0; cellIndex < nextLine.length; cellIndex++) {
						String cellValue = nextLine[cellIndex];
						// You can perform additional processing or validation here if needed
						dataObject[cellIndex] = cellValue != null ? cellValue : "";
					}

					headers.add(dataObject);
					rowCounter++;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return headers;
	}

	public List<org.bson.Document> convertSQLToMongoDB(String sqlQuery) {
		List<org.bson.Document> pipeline = new ArrayList<>();

		// Extract SELECT, FROM, WHERE, and GROUP BY clauses
		String selectClause = getClause(sqlQuery, "SELECT(.*?)FROM");
		String fromClause = getClause(sqlQuery, "FROM(.*?)WHERE|GROUP BY|;");
		String whereClause = getClause(sqlQuery, "WHERE(.*?)GROUP BY|;");
		String groupByClause = getClause(sqlQuery, "GROUP BY(.*?);");

		// Parse the clauses and construct the MongoDB aggregation pipeline
		// For simplicity, we assume the field names are directly mentioned in the query
		// In a real-world scenario, you'd need a more sophisticated parser
		if (fromClause != null) {
			pipeline.add((org.bson.Document) new org.bson.Document("$match", parseWhereClause(whereClause)));
			pipeline.add((org.bson.Document) new org.bson.Document("$group",
					parseGroupByClause(selectClause, groupByClause)));
		}

		return pipeline;
	}

	public String getClause(String sqlQuery, String regex) {
		Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(sqlQuery);
		if (matcher.find()) {
			return matcher.group(1).trim();
		}
		return null;
	}

	public Document parseWhereClause(String whereClause) {
		if (whereClause == null) {
			return (Document) new org.bson.BsonDocument();
		}
		// For simplicity, we assume simple comparison operations
		// You'll need to extend this for more complex conditions
		String[] parts = whereClause.split("\\s+");
		return (Document) new org.bson.Document(parts[0], new org.bson.Document(parts[1], Integer.parseInt(parts[2])));
	}

	public Document parseGroupByClause(String selectClause, String groupByClause) {
		String[] selectFields = selectClause.split(",");
		Document groupFields = (Document) new org.bson.Document();
		for (String field : selectFields) {
			((HashMap) groupFields).put(field.trim(), "$" + field.trim());
		}
		return (Document) new org.bson.Document("_id", groupFields);
	}

	public String extractCollectionName(String sqlQuery) {
		// Regular expression to match the "FROM" clause and extract the table name
		String regex = "\\bFROM\\s+([a-zA-Z_][a-zA-Z0-9_]*)\\b";
		Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(sqlQuery);

		if (matcher.find()) {
			return matcher.group(1);
		} else {
			// Handle the case when the "FROM" clause is not found
			return null;
		}
	}

	public JSONObject getDriveApiDetails(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = genericDataPipingDAO.getDriveApiDetails(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	/*
	 * public List<JSONObject> readExcel(HttpServletRequest request,
	 * HttpServletResponse response, String filepath, List<String> columnList) {
	 * System.out.println("Start Date And Time: " + new Date()); List<JSONObject>
	 * dataList = new ArrayList<>();
	 * 
	 * try (Workbook workbook = WorkbookFactory.create(new File(filepath));
	 * InputStream is = new FileInputStream(new File(filepath))) {
	 * 
	 * String sheetNum = request.getParameter("sheetNo"); int sheetNo = (sheetNum !=
	 * null && !sheetNum.isEmpty()) ? Integer.parseInt(sheetNum) : 0;
	 * 
	 * Sheet sheet = workbook.getSheetAt(sheetNo); int lastRowNum =
	 * sheet.getLastRowNum(); System.out.println("Total rows: " + lastRowNum);
	 * 
	 * // Pagination parameters int pageSize =
	 * Integer.parseInt(request.getParameter("pagesize") != null ?
	 * request.getParameter("pagesize") : "1000"); int startIndex = Math.max(1,
	 * Integer.parseInt(request.getParameter("recordstartindex") != null ?
	 * request.getParameter("recordstartindex") : "1")); int endIndex =
	 * Math.min(startIndex + pageSize, lastRowNum + 1);
	 * 
	 * // Use streaming API for XLSX files if (workbook instanceof XSSFWorkbook) {
	 * XSSFWorkbook xssfWorkbook = (XSSFWorkbook) workbook; XSSFReader xssfReader =
	 * new XSSFReader(xssfWorkbook.getPackage()); SharedStringsTable sst =
	 * xssfReader.getSharedStringsTable();
	 * 
	 * XMLReader parser = fetchSheetParser(sst, columnList, dataList, startIndex,
	 * endIndex, lastRowNum - 1);
	 * 
	 * InputStream sheetStream = xssfReader.getSheet("rId" + (sheetNo + 1));
	 * InputSource sheetSource = new InputSource(sheetStream);
	 * parser.parse(sheetSource); sheetStream.close(); } else { // For XLS files,
	 * use the existing row-by-row approach for (int i = startIndex; i < endIndex;
	 * i++) { Row row = sheet.getRow(i); if (row != null) { JSONObject dataObject =
	 * processRow(row, columnList); dataObject.put("totalrecords", lastRowNum - 1);
	 * // Subtract 1 to exclude header row dataList.add(dataObject); } } } } catch
	 * (Exception e) { e.printStackTrace(); }
	 * 
	 * System.out.println("End Date And Time: " + new Date()); return dataList; }
	 * 
	 * private XMLReader fetchSheetParser(SharedStringsTable sst, List<String>
	 * columnList, List<JSONObject> dataList, int startIndex, int endIndex, int
	 * totalRecords) throws SAXException { XMLReader parser =
	 * XMLReaderFactory.createXMLReader(); ContentHandler handler = new
	 * SheetHandler(sst, columnList, dataList, startIndex, endIndex, totalRecords);
	 * parser.setContentHandler(handler); return parser; }
	 * 
	 * private static class SheetHandler extends DefaultHandler { private
	 * SharedStringsTable sst; private List<String> columnList; private
	 * List<JSONObject> dataList; private int startIndex; private int endIndex;
	 * private int currentRow = 0; private JSONObject currentRowData; private
	 * StringBuilder cellValue; private boolean nextIsString; private int
	 * currentColumn; private int totalRecords;
	 * 
	 * public SheetHandler(SharedStringsTable sst, List<String> columnList,
	 * List<JSONObject> dataList, int startIndex, int endIndex, int totalRecords) {
	 * this.sst = sst; this.columnList = columnList; this.dataList = dataList;
	 * this.startIndex = startIndex; this.endIndex = endIndex; this.totalRecords =
	 * totalRecords; }
	 * 
	 * @Override public void startElement(String uri, String localName, String name,
	 * Attributes attributes) throws SAXException { if (name.equals("row")) {
	 * currentRow++; if (currentRow > 1 && currentRow >= startIndex && currentRow <
	 * endIndex) { currentRowData = new JSONObject(); currentColumn = 0; } } else if
	 * (name.equals("c")) { String cellType = attributes.getValue("t"); nextIsString
	 * = cellType != null && cellType.equals("s"); cellValue = new StringBuilder();
	 * } }
	 * 
	 * @Override public void endElement(String uri, String localName, String name)
	 * throws SAXException { if (name.equals("row") && currentRow > 1 && currentRow
	 * >= startIndex && currentRow < endIndex) { currentRowData.put("totalrecords",
	 * totalRecords); // Subtract 1 to exclude header row
	 * dataList.add(currentRowData); } else if (name.equals("v") && currentRow > 1
	 * && currentRow >= startIndex && currentRow < endIndex) { String value =
	 * cellValue.toString(); if (nextIsString) { int idx = Integer.parseInt(value);
	 * CTRst ctRst = sst.getEntryAt(idx); value = ctRst.getT(); // Get the text
	 * content if (value == null && ctRst.sizeOfRArray() > 0) { value =
	 * ctRst.getRArray(0).getT(); // If text is rich text } } if (currentColumn <
	 * columnList.size()) { currentRowData.put(columnList.get(currentColumn),
	 * value); } currentColumn++; } }
	 * 
	 * @Override public void characters(char[] ch, int start, int length) throws
	 * SAXException { cellValue.append(ch, start, length); } }
	 * 
	 * private JSONObject processRow(Row row, List<String> columnList) { JSONObject
	 * dataObject = new JSONObject(); for (int cellIndex = 0; cellIndex <
	 * columnList.size(); cellIndex++) { Cell cell = row.getCell(cellIndex); String
	 * columnName = columnList.get(cellIndex); if (cell != null) { switch
	 * (cell.getCellType()) { case Cell.CELL_TYPE_STRING: dataObject.put(columnName,
	 * cell.getStringCellValue()); break; case Cell.CELL_TYPE_BOOLEAN:
	 * dataObject.put(columnName, cell.getBooleanCellValue()); break; case
	 * Cell.CELL_TYPE_NUMERIC: if (DateUtil.isCellDateFormatted(cell)) { Date
	 * cellDate = cell.getDateCellValue(); SimpleDateFormat sdf = new
	 * SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); dataObject.put(columnName,
	 * sdf.format(cellDate)); } else { dataObject.put(columnName,
	 * NumberToTextConverter.toText(cell.getNumericCellValue())); } break; default:
	 * dataObject.put(columnName, ""); } } else { dataObject.put(columnName, ""); }
	 * } return dataObject; }
	 */
}
