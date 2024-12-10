/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.pilog.mdm.DAO.V10DataMigrationAccessDAO;
import com.pilog.mdm.DAO.V10GenericDataMigrationDAO;
import com.pilog.mdm.DAO.V10GenericDataPipingDAO;
import com.pilog.mdm.transformaccess.V10iTransformAccessOld;
import com.pilog.mdm.transformutills.V10DataPipingUtills;
import com.pilog.mdm.utilities.PilogUtilities;

import com.sap.mw.jco.JCO;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import jxl.format.Colour;
import jxl.write.WritableCellFormat;
import jxl.write.WritableFont;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
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
import org.hibernate.SessionFactory;
import org.hibernate.Session;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * @author Naidu
 */
@Service
public class V10GenericProcessETLDataService {

    @Value("${file.storeFilePath}")
    private String storeFilePath;
    @Value("${MultipartResolver.fileUploadSize}")
    private long maxFileSize;
    @Value("${MultipartResolver.fileinMemorySize}")
    private int maxMemSize;
    @Value("${jdbc.batchSize}")
    private int batchSize;
    @Autowired
    private V10GenericDataMigrationService dataMigrationService;
    @Autowired
    private V10GenericDataPipingDAO genericDataPipingDAO;
    @Autowired
    private V10GenericDataMigrationDAO dataMigrationDAO;

    @Autowired
    private V10iTransformAccessOld iTransformAccess;
    

    @Value("${jdbc.driver}")
    private String dataBaseDriver;
    @Value("${jdbc.username}")
    private String userName;
    @Value("${jdbc.password}")
    private String password;
    @Value("${jdbc.url}")
    private String dbURL;
    private V10DataMigrationAccessDAO dataMigrationAccessDAO = new V10DataMigrationAccessDAO();
    private V10DataPipingUtills dataPipingUtills = new V10DataPipingUtills();
    
    private String etlFilePath;
	{
		if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")) {
			etlFilePath = "C://";
		} else {
			etlFilePath = "/u01/";
		}
	}
      @Async
    public JSONObject processETLData(HttpSession httpSession,
            Connection fromConnection,
            Connection toConnection,
            List<Map> fromOperatorList,
            Map toOperator,
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            JSONObject tablesWhereClauseObj,
            JSONObject defaultValuesObj,
            JSONObject joinQueryMapObj,
            String joinQuery,
            JSONObject orderByObj,
            String groupByQuery,
            String nativeSQL,
            JSONObject appendValObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj,
            JSONObject normalizeOptionsObj
    ) {
        JSONObject resultObj = new JSONObject();
        PreparedStatement toPreparedStatement = null;
        try {
            JSONObject orderAndGroupByObj = new JSONObject();
            orderAndGroupByObj.put("orderByObj", orderByObj);
            orderAndGroupByObj.put("groupByQuery", groupByQuery);
            orderAndGroupByObj.put("nativeSQL", nativeSQL);
            int logSequenceNo = 10;
            //JSONObject fromColumnsObj = new JSONObject();			
            Map fromColumnsObj = new LinkedHashMap(); // ravi etl integration
            //JSONObject toColumnsObj = new JSONObject();			
            Map toColumnsObj = new LinkedHashMap(); // ravi etl integration
            if (columnsObj != null && !columnsObj.isEmpty()) {
                logSequenceNo += 10;
                try {
                    processETLLog((String) httpSession.getAttribute("ssUsername"),
                            (String) httpSession.getAttribute("ssOrgId"), "Reading the transformations rules.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                } catch (Exception e) {
                }
                for (Object toColumnName : columnsObj.keySet()) {
                    if (toColumnName != null && columnsObj.get(toColumnName) != null) {
                        String fromColumnStr = (String) columnsObj.get(toColumnName);
                        if (fromColumnStr != null
                                && !"".equalsIgnoreCase(fromColumnStr)
                                && !"null".equalsIgnoreCase(fromColumnStr)) {
                            String[] fromColumnArray = fromColumnStr.split(":");
                            if (fromColumnArray != null && fromColumnArray.length != 0) {
                                if (fromColumnsObj != null && !fromColumnsObj.isEmpty()) {
                                    if (fromColumnsObj.containsKey(fromColumnArray[0])) {
                                        fromColumnsObj.put(fromColumnArray[0],
                                                (fromColumnsObj.get(fromColumnArray[0]) + "," + fromColumnArray[1]));
                                    } else {
                                        fromColumnsObj.put(fromColumnArray[0],
                                                (fromColumnArray[1]));
                                    }
                                } else {
                                    fromColumnsObj.put(fromColumnArray[0], (fromColumnArray[1]));
                                }
                            }
                        }
                        if (toColumnName != null
                                && !"".equalsIgnoreCase(String.valueOf(toColumnName))
                                && !"null".equalsIgnoreCase(String.valueOf(toColumnName))) {
                            String[] toColumnArray = String.valueOf(toColumnName).split(":");
                            if (toColumnArray != null && toColumnArray.length != 0) {
                                if (toColumnsObj != null && !toColumnsObj.isEmpty()) {
                                    if (toColumnsObj.containsKey(toColumnArray[0])) {
                                        toColumnsObj.put(toColumnArray[0],
                                                (toColumnsObj.get(toColumnArray[0]) + "," + toColumnArray[1]));
                                    } else {
                                        toColumnsObj.put(toColumnArray[0],
                                                (toColumnArray[1]));
                                    }
                                } else {
                                    toColumnsObj.put(toColumnArray[0], (toColumnArray[1]));
                                }
                            }
                        }

                    }

                }
                //System.out.println("toColumnsObj:::" + toColumnsObj);
                Set<String> toColumns = new HashSet<>();
                toColumns.addAll(columnsObj.keySet());
                if (defaultValuesObj != null && !defaultValuesObj.isEmpty()) {
                    Set<String> defaultColumns = defaultValuesObj.keySet();
                    if (defaultColumns != null
                            && !defaultColumns.isEmpty() //  && toColumns != null && !toColumns.isEmpty()
                            ) {
                        Iterator defaultColumnsItr = defaultColumns.iterator();
                        while (defaultColumnsItr.hasNext()) {
                            String defaultColName = (String) defaultColumnsItr.next();
                            if (defaultColName != null
                                    && !"".equalsIgnoreCase(String.valueOf(defaultColName))
                                    && !"null".equalsIgnoreCase(String.valueOf(defaultColName))) {
                                String[] toColumnArray = String.valueOf(defaultColName).split(":");
                                if (toColumnArray != null && toColumnArray.length != 0) {
                                    if (toColumnsObj != null && !toColumnsObj.isEmpty()) {
                                        if (toColumnsObj.containsKey(toColumnArray[0])) {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnsObj.get(toColumnArray[0]) + "," + toColumnArray[1]));
                                        } else {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnArray[1]));
                                        }
                                    } else {
                                        toColumnsObj.put(toColumnArray[0], (toColumnArray[1]));
                                    }
                                }
                            }
                        }
//                        toColumns.addAll(defaultColumns);
                    }
                }
                if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
                    Set<String> columnClauseCols = columnClauseObj.keySet();
                    if (columnClauseCols != null
                            && !columnClauseCols.isEmpty() //  && toColumns != null && !toColumns.isEmpty()
                            ) {
                        // toColumns.addAll(columnClauseCols);
                        Iterator columnClauseColsItr = columnClauseCols.iterator();
                        while (columnClauseColsItr.hasNext()) {
                            String columnClauseColName = (String) columnClauseColsItr.next();
                            if (columnClauseColName != null
                                    && !"".equalsIgnoreCase(String.valueOf(columnClauseColName))
                                    && !"null".equalsIgnoreCase(String.valueOf(columnClauseColName))) {
                                String[] toColumnArray = String.valueOf(columnClauseColName).split(":");
                                if (toColumnArray != null && toColumnArray.length != 0) {
                                    if (toColumnsObj != null && !toColumnsObj.isEmpty()) {
                                        if (toColumnsObj.containsKey(toColumnArray[0])) {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnsObj.get(toColumnArray[0]) + "," + toColumnArray[1]));
                                        } else {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnArray[1]));
                                        }
                                    } else {
                                        toColumnsObj.put(toColumnArray[0], (toColumnArray[1]));
                                    }
                                }
                            }
                        }
                    }
                }
                selectTabObj.put("toColumnsObj", toColumnsObj);
//                for (Object toColumnName : columnsObj.keySet()) {
//                    if (toColumnName != null && columnsObj.get(toColumnName) != null) {
//                        String fromColumnStr = (String) columnsObj.get(toColumnName);
//                        if (fromColumnStr != null
//                                && !"".equalsIgnoreCase(fromColumnStr)
//                                && !"null".equalsIgnoreCase(fromColumnStr)) {
//                            String[] fromColumnArray = fromColumnStr.split(":");
//                            if (fromColumnArray != null && fromColumnArray.length != 0) {
//                                if (fromColumnsObj != null && !fromColumnsObj.isEmpty()) {
//                                    if (fromColumnsObj.containsKey(fromColumnArray[0])) {
//                                        fromColumnsObj.put(fromColumnArray[0],
//                                                (fromColumnsObj.get(fromColumnArray[0]) + "," + fromColumnArray[1]));
//                                    } else {
//                                        fromColumnsObj.put(fromColumnArray[0],
//                                                (fromColumnArray[1]));
//                                    }
//                                } else {
//                                    fromColumnsObj.put(fromColumnArray[0], (fromColumnArray[1]));
//                                }
//                            }
//                        }
//
//                    }
//
//                }
//                Set<String> toColumns = new HashSet<>();
//                toColumns.addAll(columnsObj.keySet());
//                if (defaultValuesObj != null && !defaultValuesObj.isEmpty()) {
//                    Set<String> defaultColumns = defaultValuesObj.keySet();
//                    if (defaultColumns != null
//                            && !defaultColumns.isEmpty()
//                            && toColumns != null && !toColumns.isEmpty()) {
//                        toColumns.addAll(defaultColumns);
//                    }
//                }
//                if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
//                    Set<String> columnClauseCols = columnClauseObj.keySet();
//                    if (columnClauseCols != null
//                            && !columnClauseCols.isEmpty()
//                            && toColumns != null && !toColumns.isEmpty()) {
//                        toColumns.addAll(columnClauseCols);
//                    }
//                }
                List<Map> toOperatorsList = (List<Map>) selectTabObj.get("toOperatorList");
                if (toOperatorsList != null && !toOperatorsList.isEmpty()) {
                    for (Map toOperatorMap : toOperatorsList) {
                        if (toOperatorMap != null && !toOperatorMap.isEmpty()) {
                            List<String> toColumnsList = new ArrayList<>();
                            toOperator = toOperatorMap;
                            String toTableName = (String) toOperator.get("tableName");
                            int totalDataCount = 0;
                            int fileDataLastIndex = 0;
                            List<Map> nonJoinOpList = fromOperatorList;
                            String fileName = "";
                            String orginalName = (String) toOperator.get("userFileName");
                            String iconType = (String) toOperator.get("iconType");
                            JSONObject toConnObj = (JSONObject) toOperator.get("connObj");

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
                            } else if ("JSON".equalsIgnoreCase(iconType)) {
                                fileName = "V10ETLExport_" + System.currentTimeMillis() + ".json";
                            } else {
                                if (toTableName != null
                                        && !"".equalsIgnoreCase(String.valueOf(toTableName))
                                        && !"null".equalsIgnoreCase(String.valueOf(toTableName))) {
                                    String toColumnsStr = (String) toColumnsObj.get(toTableName);
                                    if (toColumnsStr != null
                                            && !"".equalsIgnoreCase(toColumnsStr)) {

                                        toColumnsList = Arrays.asList(toColumnsStr.split(","));
                                        Object toConnectionObj = getConnection(toConnObj);
                                        if (toConnectionObj instanceof Connection) {
                                            toConnection = (Connection) toConnectionObj;
                                        }
                                        if (toConnection != null) {
                                            String toTableInsertQuery = generateInsertQuery((String) toOperator.get("tableName"), toColumnsList);
                                            System.out.println("insertQuery::::" + toTableInsertQuery);
                                            toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
                                            Map columnsTypeObj = getColumnsType((String) toOperator.get("tableName"), toColumnsList, toConnection);
                                            selectTabObj.put("columnsTypeObj", columnsTypeObj);
                                        }
                                    }
                                }
                            }
                            if (!(toColumnsList != null && !toColumnsList.isEmpty())) {
                                toColumnsList.addAll(columnsObj.keySet());
                            }
                            if (joinQueryMapObj != null
                                    && !joinQueryMapObj.isEmpty()) {

                                nonJoinOpList = fromOperatorList.stream()
                                        .filter(opMap -> (opMap != null && !opMap.isEmpty()
                                        && opMap.get("tableName") != null
                                        && !"".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
                                        && !"null".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
                                        && !joinQueryMapObj.containsKey(String.valueOf(opMap.get("tableName"))))).collect(Collectors.toList());
                                try {
                                    processETLLog((String) httpSession.getAttribute("ssUsername"),
                                            (String) httpSession.getAttribute("ssOrgId"), "Starting extract join tables data.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                                } catch (Exception e) {
                                }
                                totalDataCount += processETLData(httpSession,
                                        fromConnection,
                                        toConnection,
                                        fromOperatorList,
                                        toOperator,
                                        columnsObj,
                                        fromColumnsObj,
                                        tablesWhereClauseObj,
                                        defaultValuesObj,
                                        toPreparedStatement,
                                        1,
                                        1000,
                                        totalDataCount,
                                        toColumnsList,
                                        joinQueryMapObj,
                                        joinQuery,
                                        fileDataLastIndex,
                                        fileName,
                                        1,
                                        orderAndGroupByObj,
                                        columnClauseObj,
                                        selectTabObj,
                                        normalizeOptionsObj
                                );

                            } else {
                                if (nonJoinOpList != null && !nonJoinOpList.isEmpty()) {
                                    try {
                                        processETLLog((String) httpSession.getAttribute("ssUsername"),
                                                (String) httpSession.getAttribute("ssOrgId"), "Starting extract non-join tables data.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                                    } catch (Exception e) {
                                    }
                                    for (int i = 0; i < nonJoinOpList.size(); i++) {
                                        Map fromOperator = nonJoinOpList.get(i);
                                        if (fromOperator != null && !fromOperator.isEmpty()) {
                                            totalDataCount += processETLData(httpSession,
                                                    fromConnection,
                                                    toConnection,
                                                    fromOperator,
                                                    toOperator,
                                                    columnsObj,
                                                    fromColumnsObj,
                                                    tablesWhereClauseObj,
                                                    defaultValuesObj,
                                                    toPreparedStatement,
                                                    1,
                                                    1000,
                                                    totalDataCount,
                                                    toColumnsList,
                                                    fileDataLastIndex,
                                                    fileName,
                                                    1,
                                                    orderAndGroupByObj,
                                                    columnClauseObj,
                                                    selectTabObj,
                                                    normalizeOptionsObj
                                            );
                                        }

                                    }
                                }

                            }

                            System.out.println("totalDataCount::::" + totalDataCount);
                            if (totalDataCount != 0 && totalDataCount > 0) {
                                String message = totalDataCount + " Row(s) successfully extracted and loaded into target system.";
                                if (fileName != null && !"".equalsIgnoreCase(fileName) && !"null".equalsIgnoreCase(fileName)) {//orginalName
                                    if (orginalName != null
                                            && !"".equalsIgnoreCase(orginalName)
                                            && !"null".equalsIgnoreCase(orginalName)) {
                                        orginalName = orginalName.replaceAll("[^.a-zA-Z0-9]", "_");
                                    }

                                    message += " <br> <a href='#' style='color:#0071c5;' onclick=downloadExportedFile('" + fileName + "',\"" + orginalName + "\") >Click here to download the "
                                            + "" + iconType + ((orginalName != null
                                            && !"".equalsIgnoreCase(orginalName)
                                            && !"null".equalsIgnoreCase(orginalName)) ? "(" + orginalName + ")" : "") + " file</a>.";//
                                }
                                logSequenceNo += 10;
                                try {
                                    processETLLog((String) httpSession.getAttribute("ssUsername"),
                                            (String) httpSession.getAttribute("ssOrgId"), message, "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
                                } catch (Exception e) {
                                }
                                resultObj.put("Message", message);
                                resultObj.put("connectionFlag", "Y");

                            } else {
                                logSequenceNo += 10;
                                try {
                                    processETLLog((String) httpSession.getAttribute("ssUsername"),
                                            (String) httpSession.getAttribute("ssOrgId"), totalDataCount + " Row(s) successfully extracted and loaded into target system.", "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
                                } catch (Exception e) {
                                }
//                    processETLLog(httpSession,totalDataCount + " Row(s) extracted successfully", 10);
                                resultObj.put("Message", totalDataCount + " Row(s) successfully extracted and loaded into target system.");
                                resultObj.put("connectionFlag", "Y");
                            }
                        }
                    }
                }

            }// end of columnObj if
            try {
                processETLLog((String) httpSession.getAttribute("ssUsername"),
                        (String) httpSession.getAttribute("ssOrgId"), "ETL Process is completed", "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception e) {
            }
        } catch (Exception e) {
            e.printStackTrace();
            resultObj.put("Message", e.getMessage());
            resultObj.put("connectionFlag", "N");
            try {
                processETLLog((String) httpSession.getAttribute("ssUsername"),
                        (String) httpSession.getAttribute("ssOrgId"), e.getMessage(), "ERROR", 20, "N", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }

        } finally {
            try {
                if (toPreparedStatement != null) {
                    toPreparedStatement.close();
                }
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
// for files

    @Async
    public JSONObject processETLData(HttpSession httpSession,
            Connection toConnection,
            List<Map> fromOperatorList,
            Map toOperator,
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            JSONObject tablesWhereClauseObj,
            JSONObject defaultValuesObj,
            JSONObject joinQueryMapObj,
            String joinQuery,
            JSONObject orderByObj,
            String groupByQuery,
            String nativeSQL,
            JSONObject appendValObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj,
            JSONObject normalizeOptionsObj
    ) {
        JSONObject resultObj = new JSONObject();
        PreparedStatement toPreparedStatement = null;
        try {
            JSONObject orderAndGroupByObj = new JSONObject();
            orderAndGroupByObj.put("orderByObj", orderByObj);
            orderAndGroupByObj.put("groupByQuery", groupByQuery);
            orderAndGroupByObj.put("nativeSQL", nativeSQL);
            int logSequenceNo = 10;
            //JSONObject fromColumnsObj = new JSONObject();			
            Map fromColumnsObj = new LinkedHashMap(); // ravi etl integration
            //JSONObject toColumnsObj = new JSONObject();			
            Map toColumnsObj = new LinkedHashMap(); // ravi etl integration
            if (columnsObj != null && !columnsObj.isEmpty()) {
                logSequenceNo += 10;
                try {
                    processETLLog((String) httpSession.getAttribute("ssUsername"),
                            (String) httpSession.getAttribute("ssOrgId"), "Reading the transformations rules.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                } catch (Exception e) {
                }
                for (Object toColumnName : columnsObj.keySet()) {
                    if (toColumnName != null && columnsObj.get(toColumnName) != null) {
                        String fromColumnStr = (String) columnsObj.get(toColumnName);
                        if (fromColumnStr != null
                                && !"".equalsIgnoreCase(fromColumnStr)
                                && !"null".equalsIgnoreCase(fromColumnStr)) {
                            String[] fromColumnArray = fromColumnStr.split(":");
                            if (fromColumnArray != null && fromColumnArray.length != 0) {
                                if (fromColumnsObj != null && !fromColumnsObj.isEmpty()) {
                                    if (fromColumnsObj.containsKey(fromColumnArray[0])) {
                                        fromColumnsObj.put(fromColumnArray[0],
                                                (fromColumnsObj.get(fromColumnArray[0]) + "," + fromColumnArray[1]));
                                    } else {
                                        fromColumnsObj.put(fromColumnArray[0],
                                                (fromColumnArray[1]));
                                    }
                                } else {
                                    fromColumnsObj.put(fromColumnArray[0], (fromColumnArray[1]));
                                }
                            }
                        }
                        if (toColumnName != null
                                && !"".equalsIgnoreCase(String.valueOf(toColumnName))
                                && !"null".equalsIgnoreCase(String.valueOf(toColumnName))) {
                            String[] toColumnArray = String.valueOf(toColumnName).split(":");
                            if (toColumnArray != null && toColumnArray.length != 0) {
                                if (toColumnsObj != null && !toColumnsObj.isEmpty()) {
                                    if (toColumnsObj.containsKey(toColumnArray[0])) {
                                        toColumnsObj.put(toColumnArray[0],
                                                (toColumnsObj.get(toColumnArray[0]) + "," + toColumnArray[1]));
                                    } else {
                                        toColumnsObj.put(toColumnArray[0],
                                                (toColumnArray[1]));
                                    }
                                } else {
                                    toColumnsObj.put(toColumnArray[0], (toColumnArray[1]));
                                }
                            }
                        }

                    }

                }
                System.out.println("toColumnsObj:::" + toColumnsObj);
                Set<String> toColumns = new HashSet<>();
                toColumns.addAll(columnsObj.keySet());
                if (defaultValuesObj != null && !defaultValuesObj.isEmpty()) {
                    Set<String> defaultColumns = defaultValuesObj.keySet();
                    if (defaultColumns != null
                            && !defaultColumns.isEmpty() //  && toColumns != null && !toColumns.isEmpty()
                            ) {
                        Iterator defaultColumnsItr = defaultColumns.iterator();
                        while (defaultColumnsItr.hasNext()) {
                            String defaultColName = (String) defaultColumnsItr.next();
                            if (defaultColName != null
                                    && !"".equalsIgnoreCase(String.valueOf(defaultColName))
                                    && !"null".equalsIgnoreCase(String.valueOf(defaultColName))) {
                                String[] toColumnArray = String.valueOf(defaultColName).split(":");
                                if (toColumnArray != null && toColumnArray.length != 0) {
                                    if (toColumnsObj != null && !toColumnsObj.isEmpty()) {
                                        if (toColumnsObj.containsKey(toColumnArray[0])) {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnsObj.get(toColumnArray[0]) + "," + toColumnArray[1]));
                                        } else {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnArray[1]));
                                        }
                                    } else {
                                        toColumnsObj.put(toColumnArray[0], (toColumnArray[1]));
                                    }
                                }
                            }
                        }
//                        toColumns.addAll(defaultColumns);
                    }
                }
                if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
                    Set<String> columnClauseCols = columnClauseObj.keySet();
                    if (columnClauseCols != null
                            && !columnClauseCols.isEmpty() //  && toColumns != null && !toColumns.isEmpty()
                            ) {
                        // toColumns.addAll(columnClauseCols);
                        Iterator columnClauseColsItr = columnClauseCols.iterator();
                        while (columnClauseColsItr.hasNext()) {
                            String columnClauseColName = (String) columnClauseColsItr.next();
                            if (columnClauseColName != null
                                    && !"".equalsIgnoreCase(String.valueOf(columnClauseColName))
                                    && !"null".equalsIgnoreCase(String.valueOf(columnClauseColName))) {
                                String[] toColumnArray = String.valueOf(columnClauseColName).split(":");
                                if (toColumnArray != null && toColumnArray.length != 0) {
                                    if (toColumnsObj != null && !toColumnsObj.isEmpty()) {
                                        if (toColumnsObj.containsKey(toColumnArray[0])) {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnsObj.get(toColumnArray[0]) + "," + toColumnArray[1]));
                                        } else {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnArray[1]));
                                        }
                                    } else {
                                        toColumnsObj.put(toColumnArray[0], (toColumnArray[1]));
                                    }
                                }
                            }
                        }
                    }
                }
                selectTabObj.put("toColumnsObj", toColumnsObj);
//                for (Object toColumnName : columnsObj.keySet()) {
//                    if (toColumnName != null && columnsObj.get(toColumnName) != null) {
//                        String fromColumnStr = (String) columnsObj.get(toColumnName);
//                        if (fromColumnStr != null
//                                && !"".equalsIgnoreCase(fromColumnStr)
//                                && !"null".equalsIgnoreCase(fromColumnStr)) {
//                            String[] fromColumnArray = fromColumnStr.split(":");
//                            if (fromColumnArray != null && fromColumnArray.length != 0) {
//                                if (fromColumnsObj != null && !fromColumnsObj.isEmpty()) {
//                                    if (fromColumnsObj.containsKey(fromColumnArray[0])) {
//                                        fromColumnsObj.put(fromColumnArray[0],
//                                                (fromColumnsObj.get(fromColumnArray[0]) + "," + fromColumnArray[1]));
//                                    } else {
//                                        fromColumnsObj.put(fromColumnArray[0],
//                                                (fromColumnArray[1]));
//                                    }
//                                } else {
//                                    fromColumnsObj.put(fromColumnArray[0], (fromColumnArray[1]));
//                                }
//                            }
//                        }
//
//                    }
//
//                }
//                Set<String> toColumns = new HashSet<>();
//                toColumns.addAll(columnsObj.keySet());
//                if (defaultValuesObj != null && !defaultValuesObj.isEmpty()) {
//                    Set<String> defaultColumns = defaultValuesObj.keySet();
//                    if (defaultColumns != null
//                            && !defaultColumns.isEmpty()
//                            && toColumns != null && !toColumns.isEmpty()) {
//                        toColumns.addAll(defaultColumns);
//                    }
//                }
//                if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
//                    Set<String> columnClauseCols = columnClauseObj.keySet();
//                    if (columnClauseCols != null
//                            && !columnClauseCols.isEmpty()
//                            && toColumns != null && !toColumns.isEmpty()) {
//                        toColumns.addAll(columnClauseCols);
//                    }
//                }

//                toColumnsList.addAll(toColumns);
//                if (toConnection != null) {
//                    String toTableInsertQuery = generateInsertQuery((String) toOperator.get("tableName"), toColumnsList);
//                    System.out.println("insertQuery::::" + toTableInsertQuery);
//                    toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
//                }
                List<Map> toOperatorsList = (List<Map>) selectTabObj.get("toOperatorList");
                if (toOperatorsList != null && !toOperatorsList.isEmpty()) {
                    for (Map toOperatorMap : toOperatorsList) {
                        if (toOperatorMap != null && !toOperatorMap.isEmpty()) {
                            List<String> toColumnsList = new ArrayList<>();
                            toOperator = toOperatorMap;
                            String toTableName = (String) toOperator.get("tableName");
                            int totalDataCount = 0;
                            int fileDataLastIndex = 0;
                            List<Map> nonJoinOpList = fromOperatorList;
                            String fileName = "";
                            String orginalName = (String) toOperator.get("userFileName");
                            String iconType = (String) toOperator.get("iconType");
                            JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
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
                            } else if ("JSON".equalsIgnoreCase(iconType)) {
                                fileName = "V10ETLExport_" + System.currentTimeMillis() + ".json";
                            } else {
                                if (toTableName != null
                                        && !"".equalsIgnoreCase(String.valueOf(toTableName))
                                        && !"null".equalsIgnoreCase(String.valueOf(toTableName))) {
                                    String toColumnsStr = (String) toColumnsObj.get(toTableName);
                                    if (toColumnsStr != null
                                            && !"".equalsIgnoreCase(toColumnsStr)) {

                                        toColumnsList = Arrays.asList(toColumnsStr.split(","));
                                        Object toConnectionObj = getConnection(toConnObj);
                                        if (toConnectionObj instanceof Connection) {
                                            toConnection = (Connection) toConnectionObj;
                                        }
                                        if (toConnection != null) {
                                            String toTableInsertQuery = generateInsertQuery((String) toOperator.get("tableName"), toColumnsList);
                                            System.out.println("insertQuery::::" + toTableInsertQuery);
                                            toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
                                            Map columnsTypeObj = getColumnsType((String) toOperator.get("tableName"), toColumnsList, toConnection);
                                            selectTabObj.put("columnsTypeObj", columnsTypeObj);
                                        }
                                    }
                                }
                            }
                            if (!(toColumnsList != null && !toColumnsList.isEmpty())) {
                                toColumnsList.addAll(columnsObj.keySet());
                            }
                            if (joinQueryMapObj != null
                                    && !joinQueryMapObj.isEmpty()) {

                                nonJoinOpList = fromOperatorList.stream()
                                        .filter(opMap -> (opMap != null && !opMap.isEmpty()
                                        && opMap.get("tableName") != null
                                        && !"".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
                                        && !"null".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
                                        && !joinQueryMapObj.containsKey(String.valueOf(opMap.get("tableName"))))).collect(Collectors.toList());
                                try {
                                    processETLLog((String) httpSession.getAttribute("ssUsername"),
                                            (String) httpSession.getAttribute("ssOrgId"), "Starting extract join tables data.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                                } catch (Exception e) {
                                }
                                totalDataCount += processETLData(httpSession,
                                        toConnection,
                                        fromOperatorList,
                                        toOperator,
                                        columnsObj,
                                        fromColumnsObj,
                                        tablesWhereClauseObj,
                                        defaultValuesObj,
                                        toPreparedStatement,
                                        1,
                                        1000,
                                        totalDataCount,
                                        toColumnsList,
                                        joinQueryMapObj,
                                        joinQuery,
                                        fileDataLastIndex,
                                        fileName,
                                        1,
                                        orderAndGroupByObj,
                                        columnClauseObj,
                                        selectTabObj,
                                        normalizeOptionsObj
                                );

                            } else {
                                if (nonJoinOpList != null && !nonJoinOpList.isEmpty()) {
                                    try {
                                        processETLLog((String) httpSession.getAttribute("ssUsername"),
                                                (String) httpSession.getAttribute("ssOrgId"), "Starting extract non-join tables data.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                                    } catch (Exception e) {
                                    }
                                    for (int i = 0; i < nonJoinOpList.size(); i++) {
                                        Map fromOperator = nonJoinOpList.get(i);
                                        if (fromOperator != null && !fromOperator.isEmpty()) {
                                            totalDataCount += processETLData(httpSession,
                                                    toConnection,
                                                    fromOperator,
                                                    toOperator,
                                                    columnsObj,
                                                    fromColumnsObj,
                                                    tablesWhereClauseObj,
                                                    defaultValuesObj,
                                                    toPreparedStatement,
                                                    1,
                                                    1000,
                                                    totalDataCount,
                                                    toColumnsList,
                                                    fileDataLastIndex,
                                                    fileName,
                                                    1,
                                                    orderAndGroupByObj,
                                                    columnClauseObj,
                                                    selectTabObj,
                                                    normalizeOptionsObj
                                            );
                                        }

                                    }
                                }

                            }

                            System.out.println("totalDataCount::::" + totalDataCount);
                            if (totalDataCount != 0 && totalDataCount > 0) {
                                String message = totalDataCount + " Row(s) successfully extracted and loaded into target system.";
                                if (fileName != null && !"".equalsIgnoreCase(fileName) && !"null".equalsIgnoreCase(fileName)) {//orginalName
                                    if (orginalName != null
                                            && !"".equalsIgnoreCase(orginalName)
                                            && !"null".equalsIgnoreCase(orginalName)) {
                                        orginalName = orginalName.replaceAll("[^.a-zA-Z0-9]", "_");
                                    }

                                    message += " <br> <a href='#' style='color:#0071c5;' onclick=downloadExportedFile('" + fileName + "',\"" + orginalName + "\") >Click here to download the "
                                            + "" + iconType + ((orginalName != null
                                            && !"".equalsIgnoreCase(orginalName)
                                            && !"null".equalsIgnoreCase(orginalName)) ? "(" + orginalName + ")" : "") + " file</a>.";//
                                }
                                logSequenceNo += 10;
                                try {
                                    processETLLog((String) httpSession.getAttribute("ssUsername"),
                                            (String) httpSession.getAttribute("ssOrgId"), message, "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
                                } catch (Exception e) {
                                }
                                resultObj.put("Message", message);
                                resultObj.put("connectionFlag", "Y");

                            } else {
                                logSequenceNo += 10;
                                try {
                                    processETLLog((String) httpSession.getAttribute("ssUsername"),
                                            (String) httpSession.getAttribute("ssOrgId"), totalDataCount + " Row(s) successfully extracted and loaded into target system.", "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
                                } catch (Exception e) {
                                }
//                    processETLLog(httpSession,totalDataCount + " Row(s) extracted successfully", 10);
                                resultObj.put("Message", totalDataCount + " Row(s) successfully extracted and loaded into target system.");
                                resultObj.put("connectionFlag", "Y");
                            }
                        }
                    }
                }
//                if (toColumnsObj != null && !toColumnsObj.isEmpty()) {
//                    List<Map> toOperatorsList = (List<Map>) selectTabObj.get("toOperatorList");
//                    for (Object toTableName : toColumnsObj.keySet()) {
//                        if (toTableName != null
//                                && !"".equalsIgnoreCase(String.valueOf(toTableName))
//                                && !"null".equalsIgnoreCase(String.valueOf(toTableName))) {
//                            String toColumnsStr = (String) toColumnsObj.get(toTableName);
//                            if (toColumnsStr != null
//                                    && !"".equalsIgnoreCase(toColumnsStr)) {
//                                List<String> toColumnsList = new ArrayList<>();
//                                toColumnsList = Arrays.asList(toColumnsStr.split(","));
//                                List<Map> matchedToOperatorsList = toOperatorsList.stream().filter(toOp
//                                        -> (toOp != null
//                                        && !toOp.isEmpty()
//                                        && toOp.get("tableName") != null
//                                        && !"".equalsIgnoreCase((String) toOp.get("tableName"))
//                                        && !"null".equalsIgnoreCase((String) toOp.get("tableName"))
//                                        && String.valueOf(toOp.get("tableName")).equalsIgnoreCase(String.valueOf(toTableName)))).collect(Collectors.toList());
//                                if (matchedToOperatorsList != null && !matchedToOperatorsList.isEmpty()) {
//                                    toOperator = matchedToOperatorsList.get(0);
//                                }
//                                if (toConnection != null) {
//                                    String toTableInsertQuery = generateInsertQuery((String) toOperator.get("tableName"), toColumnsList);
//                                    System.out.println("insertQuery::::" + toTableInsertQuery);
//                                    toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
//                                }
//
//                                int totalDataCount = 0;
//                                int fileDataLastIndex = 0;
//                                List<Map> nonJoinOpList = fromOperatorList;
//                                String fileName = "";
//                                String orginalName = (String) toOperator.get("userFileName");
//                                String iconType = (String) toOperator.get("iconType");
//
//                                if ("XLSX".equalsIgnoreCase(iconType)) {
//                                    fileName = "V10ETLExport_" + System.currentTimeMillis() + ".xlsx";
//
//                                } else if ("XLS".equalsIgnoreCase(iconType)) {
//                                    fileName = "V10ETLExport_" + System.currentTimeMillis() + ".xls";
//                                } else if ("XML".equalsIgnoreCase(iconType)) {
//                                    fileName = "V10ETLExport_" + System.currentTimeMillis() + ".xml";
//                                } else if ("CSV".equalsIgnoreCase(iconType)) {
//                                    fileName = "V10ETLExport_" + System.currentTimeMillis() + ".csv";
//                                } else if ("TXT".equalsIgnoreCase(iconType)) {
//                                    fileName = "V10ETLExport_" + System.currentTimeMillis() + ".txt";
//                                }
//
//                                if (joinQueryMapObj != null
//                                        && !joinQueryMapObj.isEmpty()) {
//
//                                    nonJoinOpList = fromOperatorList.stream()
//                                            .filter(opMap -> (opMap != null && !opMap.isEmpty()
//                                            && opMap.get("tableName") != null
//                                            && !"".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
//                                            && !"null".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
//                                            && !joinQueryMapObj.containsKey(String.valueOf(opMap.get("tableName"))))).collect(Collectors.toList());
//                                    try {
//                                        processETLLog((String) httpSession.getAttribute("ssUsername"),
//                                                (String) httpSession.getAttribute("ssOrgId"), "Starting extract join tables data.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
//                                    } catch (Exception e) {
//                                    }
//                                    totalDataCount += processETLData(httpSession,
//                                            toConnection,
//                                            fromOperatorList,
//                                            toOperator,
//                                            columnsObj,
//                                            fromColumnsObj,
//                                            tablesWhereClauseObj,
//                                            defaultValuesObj,
//                                            toPreparedStatement,
//                                            0,
//                                            1000,
//                                            totalDataCount,
//                                            toColumnsList,
//                                            joinQueryMapObj,
//                                            joinQuery,
//                                            fileDataLastIndex,
//                                            fileName,
//                                            1,
//                                            orderAndGroupByObj,
//                                            columnClauseObj,
//                                            selectTabObj,
//                                            normalizeOptionsObj
//                                    );
//
//                                } else {
//                                    if (nonJoinOpList != null && !nonJoinOpList.isEmpty()) {
//                                        try {
//                                            processETLLog((String) httpSession.getAttribute("ssUsername"),
//                                                    (String) httpSession.getAttribute("ssOrgId"), "Starting extract non-join tables data.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
//                                        } catch (Exception e) {
//                                        }
//                                        for (int i = 0; i < nonJoinOpList.size(); i++) {
//                                            Map fromOperator = nonJoinOpList.get(i);
//                                            if (fromOperator != null && !fromOperator.isEmpty()) {
//                                                totalDataCount += processETLData(httpSession,
//                                                        toConnection,
//                                                        fromOperator,
//                                                        toOperator,
//                                                        columnsObj,
//                                                        fromColumnsObj,
//                                                        tablesWhereClauseObj,
//                                                        defaultValuesObj,
//                                                        toPreparedStatement,
//                                                        0,
//                                                        1000,
//                                                        totalDataCount,
//                                                        toColumnsList,
//                                                        fileDataLastIndex,
//                                                        fileName,
//                                                        1,
//                                                        orderAndGroupByObj,
//                                                        columnClauseObj,
//                                                        selectTabObj,
//                                                        normalizeOptionsObj
//                                                );
//                                            }
//
//                                        }
//                                    }
//
//                                }
//
//                                System.out.println("totalDataCount::::" + totalDataCount);
//                                if (totalDataCount != 0 && totalDataCount > 0) {
//                                    String message = totalDataCount + " Row(s) successfully extracted and loaded into target system.";
//                                    if (fileName != null && !"".equalsIgnoreCase(fileName) && !"null".equalsIgnoreCase(fileName)) {//orginalName
//                                        if (orginalName != null
//                                                && !"".equalsIgnoreCase(orginalName)
//                                                && !"null".equalsIgnoreCase(orginalName)) {
//                                            orginalName = orginalName.replaceAll("[^.a-zA-Z0-9]", "_");
//                                        }
//
//                                        message += " <br> <a href='#' style='color:#0071c5;' onclick=downloadExportedFile('" + fileName + "',\"" + orginalName + "\") >Click here to download the " + iconType + " file</a>.";//
//                                    }
//                                    logSequenceNo += 10;
//                                    try {
//                                        processETLLog((String) httpSession.getAttribute("ssUsername"),
//                                                (String) httpSession.getAttribute("ssOrgId"), message, "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
//                                    } catch (Exception e) {
//                                    }
//                                    resultObj.put("Message", message);
//                                    resultObj.put("connectionFlag", "Y");
//
//                                } else {
//                                    logSequenceNo += 10;
//                                    try {
//                                        processETLLog((String) httpSession.getAttribute("ssUsername"),
//                                                (String) httpSession.getAttribute("ssOrgId"), totalDataCount + " Row(s) successfully extracted and loaded into target system.", "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
//                                    } catch (Exception e) {
//                                    }
////                    processETLLog(httpSession,totalDataCount + " Row(s) extracted successfully", 10);
//                                    resultObj.put("Message", totalDataCount + " Row(s) successfully extracted and loaded into target system.");
//                                    resultObj.put("connectionFlag", "Y");
//                                }
//
//                            }
//                        }
//                    }
//                }

            }// end of columnObj if
            try {
                processETLLog((String) httpSession.getAttribute("ssUsername"),
                        (String) httpSession.getAttribute("ssOrgId"), "ETL Process is completed", "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception e) {
            }
        } catch (Exception e) {
            e.printStackTrace();
            resultObj.put("Message", e.getMessage());
            resultObj.put("connectionFlag", "N");
            try {
                processETLLog((String) httpSession.getAttribute("ssUsername"),
                        (String) httpSession.getAttribute("ssOrgId"), e.getMessage(), "ERROR", 20, "N", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }

        } finally {
            try {
                if (toPreparedStatement != null) {
                    toPreparedStatement.close();
                }

                if (toConnection != null) {
                    toConnection.close();
                }

            } catch (Exception e) {
            }
        }
        return resultObj;
    }
// for SAP

    @Async
    public JSONObject processETLData(HttpSession httpSession,
            JCO.Client fromJCOConnection,
            Connection toConnection,
            JCO.Client toJCOConnection,
            List<Map> fromOperatorList,
            Map toOperator,
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            JSONObject tablesWhereClauseObj,
            JSONObject defaultValuesObj,
            JSONObject joinQueryMapObj,
            String joinQuery,
            JSONObject orderByObj,
            String groupByQuery,
            String nativeSQL,
            JSONObject appendValObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj,
            JSONObject normalizeOptionsObj
    ) {
        JSONObject resultObj = new JSONObject();
        Connection fromConnection = null;
        PreparedStatement toPreparedStatement = null;
        try {
            JSONObject orderAndGroupByObj = new JSONObject();
            orderAndGroupByObj.put("orderByObj", orderByObj);
            orderAndGroupByObj.put("groupByQuery", groupByQuery);
            orderAndGroupByObj.put("nativeSQL", nativeSQL);
            int logSequenceNo = 10;
            //JSONObject fromColumnsObj = new JSONObject();			
            Map fromColumnsObj = new LinkedHashMap(); // ravi etl integration
            //JSONObject toColumnsObj = new JSONObject();			
            Map toColumnsObj = new LinkedHashMap(); // ravi etl integration
            if (columnsObj != null && !columnsObj.isEmpty()) {
                logSequenceNo += 10;
                try {
                    processETLLog((String) httpSession.getAttribute("ssUsername"),
                            (String) httpSession.getAttribute("ssOrgId"), "Reading the transformations rules.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                } catch (Exception e) {
                }
                for (Object toColumnName : columnsObj.keySet()) {
                    if (toColumnName != null && columnsObj.get(toColumnName) != null) {
                        String fromColumnStr = (String) columnsObj.get(toColumnName);
                        if (fromColumnStr != null
                                && !"".equalsIgnoreCase(fromColumnStr)
                                && !"null".equalsIgnoreCase(fromColumnStr)) {
                            String[] fromColumnArray = fromColumnStr.split(":");
                            if (fromColumnArray != null && fromColumnArray.length != 0) {
                                if (fromColumnsObj != null && !fromColumnsObj.isEmpty()) {
                                    if (fromColumnsObj.containsKey(fromColumnArray[0])) {
                                        fromColumnsObj.put(fromColumnArray[0],
                                                (fromColumnsObj.get(fromColumnArray[0]) + "," + fromColumnArray[1]));
                                    } else {
                                        fromColumnsObj.put(fromColumnArray[0],
                                                (fromColumnArray[1]));
                                    }
                                } else {
                                    fromColumnsObj.put(fromColumnArray[0], (fromColumnArray[1]));
                                }
                            }
                        }
                        if (toColumnName != null
                                && !"".equalsIgnoreCase(String.valueOf(toColumnName))
                                && !"null".equalsIgnoreCase(String.valueOf(toColumnName))) {
                            String[] toColumnArray = String.valueOf(toColumnName).split(":");
                            if (toColumnArray != null && toColumnArray.length != 0) {
                                if (toColumnsObj != null && !toColumnsObj.isEmpty()) {
                                    if (toColumnsObj.containsKey(toColumnArray[0])) {
                                        toColumnsObj.put(toColumnArray[0],
                                                (toColumnsObj.get(toColumnArray[0]) + "," + toColumnArray[1]));
                                    } else {
                                        toColumnsObj.put(toColumnArray[0],
                                                (toColumnArray[1]));
                                    }
                                } else {
                                    toColumnsObj.put(toColumnArray[0], (toColumnArray[1]));
                                }
                            }
                        }

                    }

                }
                System.out.println("toColumnsObj:::" + toColumnsObj);
                Set<String> toColumns = new HashSet<>();
                toColumns.addAll(columnsObj.keySet());
                if (defaultValuesObj != null && !defaultValuesObj.isEmpty()) {
                    Set<String> defaultColumns = defaultValuesObj.keySet();
                    if (defaultColumns != null
                            && !defaultColumns.isEmpty() //  && toColumns != null && !toColumns.isEmpty()
                            ) {
                        Iterator defaultColumnsItr = defaultColumns.iterator();
                        while (defaultColumnsItr.hasNext()) {
                            String defaultColName = (String) defaultColumnsItr.next();
                            if (defaultColName != null
                                    && !"".equalsIgnoreCase(String.valueOf(defaultColName))
                                    && !"null".equalsIgnoreCase(String.valueOf(defaultColName))) {
                                String[] toColumnArray = String.valueOf(defaultColName).split(":");
                                if (toColumnArray != null && toColumnArray.length != 0) {
                                    if (toColumnsObj != null && !toColumnsObj.isEmpty()) {
                                        if (toColumnsObj.containsKey(toColumnArray[0])) {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnsObj.get(toColumnArray[0]) + "," + toColumnArray[1]));
                                        } else {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnArray[1]));
                                        }
                                    } else {
                                        toColumnsObj.put(toColumnArray[0], (toColumnArray[1]));
                                    }
                                }
                            }
                        }
//                        toColumns.addAll(defaultColumns);
                    }
                }
                if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
                    Set<String> columnClauseCols = columnClauseObj.keySet();
                    if (columnClauseCols != null
                            && !columnClauseCols.isEmpty() //  && toColumns != null && !toColumns.isEmpty()
                            ) {
                        // toColumns.addAll(columnClauseCols);
                        Iterator columnClauseColsItr = columnClauseCols.iterator();
                        while (columnClauseColsItr.hasNext()) {
                            String columnClauseColName = (String) columnClauseColsItr.next();
                            if (columnClauseColName != null
                                    && !"".equalsIgnoreCase(String.valueOf(columnClauseColName))
                                    && !"null".equalsIgnoreCase(String.valueOf(columnClauseColName))) {
                                String[] toColumnArray = String.valueOf(columnClauseColName).split(":");
                                if (toColumnArray != null && toColumnArray.length != 0) {
                                    if (toColumnsObj != null && !toColumnsObj.isEmpty()) {
                                        if (toColumnsObj.containsKey(toColumnArray[0])) {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnsObj.get(toColumnArray[0]) + "," + toColumnArray[1]));
                                        } else {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnArray[1]));
                                        }
                                    } else {
                                        toColumnsObj.put(toColumnArray[0], (toColumnArray[1]));
                                    }
                                }
                            }
                        }
                    }
                }
                selectTabObj.put("toColumnsObj", toColumnsObj);
//                for (Object toColumnName : columnsObj.keySet()) {
//                    if (toColumnName != null && columnsObj.get(toColumnName) != null) {
//                        String fromColumnStr = (String) columnsObj.get(toColumnName);
//                        if (fromColumnStr != null
//                                && !"".equalsIgnoreCase(fromColumnStr)
//                                && !"null".equalsIgnoreCase(fromColumnStr)) {
//                            String[] fromColumnArray = fromColumnStr.split(":");
//                            if (fromColumnArray != null && fromColumnArray.length != 0) {
//                                if (fromColumnsObj != null && !fromColumnsObj.isEmpty()) {
//                                    if (fromColumnsObj.containsKey(fromColumnArray[0])) {
//                                        fromColumnsObj.put(fromColumnArray[0],
//                                                (fromColumnsObj.get(fromColumnArray[0]) + "," + fromColumnArray[1]));
//                                    } else {
//                                        fromColumnsObj.put(fromColumnArray[0],
//                                                (fromColumnArray[1]));
//                                    }
//                                } else {
//                                    fromColumnsObj.put(fromColumnArray[0], (fromColumnArray[1]));
//                                }
//                            }
//                        }
//
//                    }
//
//                }
//                Set<String> toColumns = new HashSet<>();
//                toColumns.addAll(columnsObj.keySet());
//                if (defaultValuesObj != null && !defaultValuesObj.isEmpty()) {
//                    Set<String> defaultColumns = defaultValuesObj.keySet();
//                    if (defaultColumns != null
//                            && !defaultColumns.isEmpty()
//                            && toColumns != null && !toColumns.isEmpty()) {
//                        toColumns.addAll(defaultColumns);
//                    }
//                }
//                if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
//                    Set<String> columnClauseCols = columnClauseObj.keySet();
//                    if (columnClauseCols != null
//                            && !columnClauseCols.isEmpty()
//                            && toColumns != null && !toColumns.isEmpty()) {
//                        toColumns.addAll(columnClauseCols);
//                    }
//                }
//                List<String> toColumnsList = new ArrayList<>();
//                toColumnsList.addAll(toColumns);
//                if (toConnection != null) {
//                    String toTableInsertQuery = generateInsertQuery((String) toOperator.get("tableName"), toColumnsList);
//                    System.out.println("insertQuery::::" + toTableInsertQuery);
//                    toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
//                }
                List<Map> toOperatorsList = (List<Map>) selectTabObj.get("toOperatorList");
                if (toOperatorsList != null && !toOperatorsList.isEmpty()) {
                    for (Map toOperatorMap : toOperatorsList) {
                        if (toOperatorMap != null && !toOperatorMap.isEmpty()) {
                            List<String> toColumnsList = new ArrayList<>();
                            toOperator = toOperatorMap;
                            String toTableName = (String) toOperator.get("tableName");
                            int totalDataCount = 0;
                            int fileDataLastIndex = 0;
                            List<Map> nonJoinOpList = fromOperatorList;
                            String fileName = "";
                            String orginalName = (String) toOperator.get("userFileName");
                            String iconType = (String) toOperator.get("iconType");
                            JSONObject toConnObj = (JSONObject) toOperator.get("connObj");

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
                            } else if ("JSON".equalsIgnoreCase(iconType)) {
                                fileName = "V10ETLExport_" + System.currentTimeMillis() + ".json";
                            } else {
                                if (toTableName != null
                                        && !"".equalsIgnoreCase(String.valueOf(toTableName))
                                        && !"null".equalsIgnoreCase(String.valueOf(toTableName))) {
                                    String toColumnsStr = (String) toColumnsObj.get(toTableName);
                                    if (toColumnsStr != null
                                            && !"".equalsIgnoreCase(toColumnsStr)) {

                                        toColumnsList = Arrays.asList(toColumnsStr.split(","));
                                        Object toConnectionObj = getConnection(toConnObj);
                                        if (toConnectionObj instanceof Connection) {
                                            toConnection = (Connection) toConnectionObj;
                                        }
                                        if (toConnection != null) {
                                            String toTableInsertQuery = generateInsertQuery((String) toOperator.get("tableName"), toColumnsList);
                                            System.out.println("insertQuery::::" + toTableInsertQuery);
                                            toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
                                            Map columnsTypeObj = getColumnsType((String) toOperator.get("tableName"), toColumnsList, toConnection);
                                            selectTabObj.put("columnsTypeObj", columnsTypeObj);
                                        }
                                    }
                                }
                            }
                            if (!(toColumnsList != null && !toColumnsList.isEmpty())) {
                                toColumnsList.addAll(columnsObj.keySet());
                            }
                            // need to get SAP data and create the tables in Current V10 DB
                            JSONObject sapColumnsObj = getSelectedSAPTablesData(httpSession,
                                    fromJCOConnection,
                                    fromOperatorList,
                                    columnsObj,
                                    fromColumnsObj,
                                    tablesWhereClauseObj,
                                    defaultValuesObj,
                                    orderByObj,
                                    groupByQuery,
                                    joinQueryMapObj
                            );

                            if (sapColumnsObj != null && !sapColumnsObj.isEmpty()) {
                                JSONObject dbFromColumnsObj = (JSONObject) sapColumnsObj.get("fromColumnsObj");
                                JSONObject tableNameObj = (JSONObject) sapColumnsObj.get("tableNameObj");
                                fromConnection = DriverManager.getConnection(dbURL, userName, password);
//                    String dbDriver, String dbURL,String userName,String password,String connName
                                JSONObject currentV10DBObj = new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName, password, "Current_V10");
                                fromOperatorList = fromOperatorList.stream().map(formOp -> {
                                    try {
                                        formOp.put("CONNECTION_NAME", currentV10DBObj.get("CONNECTION_NAME"));
                                        formOp.put("CONN_DB_NAME", currentV10DBObj.get("CONN_DB_NAME"));
                                        formOp.put("CONN_CUST_COL1", currentV10DBObj.get("CONN_CUST_COL1"));
                                        formOp.put("connObj", currentV10DBObj);
                                        if (tableNameObj != null && !tableNameObj.isEmpty()
                                                && tableNameObj.containsKey(String.valueOf(formOp.get("tableName")))) {
                                            formOp.put("tableName", tableNameObj.get(String.valueOf(formOp.get("tableName"))));
                                        }

                                    } catch (Exception e) {
                                    }
                                    return formOp;
                                }).collect(Collectors.toList());
                                JSONObject tempJoinQueryMapObj = new JSONObject();
                                JSONObject tempTablesWhereClauseObj = new JSONObject();
                                JSONObject tempColumnsObj = new JSONObject();
                                tempColumnsObj.putAll(columnsObj);
                                for (Object sapTableName : tableNameObj.keySet()) {
                                    if (joinQueryMapObj != null
                                            && !joinQueryMapObj.isEmpty()
                                            && joinQueryMapObj.containsKey(sapTableName)) {
                                        tempJoinQueryMapObj.put(String.valueOf(tableNameObj.get(sapTableName)), joinQueryMapObj.get(sapTableName));
                                        joinQueryMapObj.remove(sapTableName);

                                    }
                                    if (tablesWhereClauseObj != null
                                            && !tablesWhereClauseObj.isEmpty()
                                            && tablesWhereClauseObj.containsKey(sapTableName)) {
                                        tempTablesWhereClauseObj.put(String.valueOf(tableNameObj.get(sapTableName)), joinQueryMapObj.get(sapTableName));
                                        tablesWhereClauseObj.remove(sapTableName);
                                    }
                                    if (columnsObj != null && !columnsObj.isEmpty()) {

                                        for (Object sourceCol : columnsObj.keySet()) {
                                            String keyValueContains = sapTableName + ":";

                                            if (columnsObj.get(sourceCol) != null
                                                    && !"".equalsIgnoreCase(String.valueOf(columnsObj.get(sourceCol)))
                                                    && !"null".equalsIgnoreCase(String.valueOf(columnsObj.get(sourceCol)))
                                                    && String.valueOf(columnsObj.get(sourceCol)).contains(keyValueContains)) {
                                                String modifiedKey = (String) sourceCol;
                                                if (String.valueOf(sourceCol).contains(keyValueContains)) {
                                                    tempColumnsObj.remove(sourceCol);
                                                    modifiedKey = String.valueOf(sourceCol).replace(keyValueContains, String.valueOf(tableNameObj.get(sapTableName)) + ":");
                                                }
                                                tempColumnsObj.put(modifiedKey,
                                                        String.valueOf(columnsObj.get(sourceCol))
                                                                .replace(keyValueContains, String.valueOf(tableNameObj.get(sapTableName)) + ":"));
                                            }
                                        }
                                    }

                                    if (joinQuery != null
                                            && !"".equalsIgnoreCase(joinQuery)
                                            && !"null".equalsIgnoreCase(joinQuery)) {
                                        String keyValueContains = " " + sapTableName + " ";
                                        joinQuery = joinQuery.replaceAll(keyValueContains, " " + String.valueOf(tableNameObj.get(sapTableName)) + " ");
                                        keyValueContains = sapTableName + "[.]";
                                        joinQuery = joinQuery.replaceAll(keyValueContains, String.valueOf(tableNameObj.get(sapTableName)) + ".");
                                    }
                                    if (fileName != null
                                            && !"".equalsIgnoreCase(fileName)
                                            && !"null".equalsIgnoreCase(fileName)
                                            && toColumnsList != null && !toColumnsList.isEmpty()) {
                                        String keyValueContains = sapTableName + ":";
                                        toColumnsList = toColumnsList.stream().map(toColName -> (String.valueOf(toColName).replace(keyValueContains, String.valueOf(tableNameObj.get(sapTableName)) + ":")))
                                                .collect(Collectors.toList());
                                    }
                                }// end loop
                                if (tempColumnsObj != null && !tempColumnsObj.isEmpty()) {
                                    columnsObj = tempColumnsObj;
                                }
                                if (tempJoinQueryMapObj != null && !tempJoinQueryMapObj.isEmpty()) {
                                    joinQueryMapObj.putAll(tempJoinQueryMapObj);
                                }
                                if (tempTablesWhereClauseObj != null && !tempTablesWhereClauseObj.isEmpty()) {
                                    tablesWhereClauseObj.putAll(tempTablesWhereClauseObj);
                                }

                                if (joinQueryMapObj != null
                                        && !joinQueryMapObj.isEmpty()) {

                                    nonJoinOpList = fromOperatorList.stream()
                                            .filter(opMap -> (opMap != null && !opMap.isEmpty()
                                            && opMap.get("tableName") != null
                                            && !"".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
                                            && !"null".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
                                            && !joinQueryMapObj.containsKey(String.valueOf(opMap.get("tableName"))))).collect(Collectors.toList());
                                    try {
                                        processETLLog((String) httpSession.getAttribute("ssUsername"),
                                                (String) httpSession.getAttribute("ssOrgId"), "Starting extract join tables data.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                                    } catch (Exception e) {
                                    }
                                    totalDataCount += processETLData(httpSession,
                                            fromConnection,
                                            toConnection,
                                            fromOperatorList,
                                            toOperator,
                                            columnsObj,
                                            dbFromColumnsObj,
                                            tablesWhereClauseObj,
                                            defaultValuesObj,
                                            toPreparedStatement,
                                            1,
                                            1000,
                                            totalDataCount,
                                            toColumnsList,
                                            joinQueryMapObj,
                                            joinQuery,
                                            fileDataLastIndex,
                                            fileName,
                                            1,
                                            orderAndGroupByObj,
                                            columnClauseObj,
                                            selectTabObj,
                                            normalizeOptionsObj
                                    );

                                } else {
                                    if (nonJoinOpList != null && !nonJoinOpList.isEmpty()) {
                                        try {
                                            processETLLog((String) httpSession.getAttribute("ssUsername"),
                                                    (String) httpSession.getAttribute("ssOrgId"), "Starting extract non-join tables data.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                                        } catch (Exception e) {
                                        }
                                        for (int i = 0; i < nonJoinOpList.size(); i++) {
                                            Map fromOperator = nonJoinOpList.get(i);
                                            if (fromOperator != null && !fromOperator.isEmpty()) {
                                                totalDataCount += processETLData(httpSession,
                                                        fromConnection,
                                                        toConnection,
                                                        fromOperator,
                                                        toOperator,
                                                        columnsObj,
                                                        dbFromColumnsObj,
                                                        tablesWhereClauseObj,
                                                        defaultValuesObj,
                                                        toPreparedStatement,
                                                        1,
                                                        1000,
                                                        totalDataCount,
                                                        toColumnsList,
                                                        fileDataLastIndex,
                                                        fileName,
                                                        1,
                                                        orderAndGroupByObj,
                                                        columnClauseObj,
                                                        selectTabObj,
                                                        normalizeOptionsObj
                                                );
                                            }

                                        }
                                    }

                                }
                                System.out.println("totalDataCount::::" + totalDataCount);
                                if (totalDataCount != 0 && totalDataCount > 0) {
                                    String message = totalDataCount + " Row(s) successfully extracted and loaded into target system.";
                                    if (fileName != null && !"".equalsIgnoreCase(fileName) && !"null".equalsIgnoreCase(fileName)) {
                                        message += " <br> <a href='#' style='color:#0071c5;' onclick=downloadExportedFile('" + fileName + "') >Click here to download the " + iconType + " file</a>.";//
                                    }
                                    logSequenceNo += 10;
                                    try {
                                        processETLLog((String) httpSession.getAttribute("ssUsername"),
                                                (String) httpSession.getAttribute("ssOrgId"), message, "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
                                    } catch (Exception e) {
                                    }
                                    resultObj.put("Message", message);
                                    resultObj.put("connectionFlag", "Y");

                                } else {
                                    logSequenceNo += 10;
                                    try {
                                        processETLLog((String) httpSession.getAttribute("ssUsername"),
                                                (String) httpSession.getAttribute("ssOrgId"), totalDataCount + " Row(s) successfully extracted and loaded into target system.", "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
                                    } catch (Exception e) {
                                    }
//                    processETLLog(httpSession,totalDataCount + " Row(s) extracted successfully", 10);
                                    resultObj.put("Message", totalDataCount + " Row(s) successfully extracted and loaded into target system.");
                                    resultObj.put("connectionFlag", "Y");
                                }
                                try {
                                    dropTemptables(tableNameObj);
                                } catch (Exception e) {
                                }
                            } else {
                                try {
                                    processETLLog((String) httpSession.getAttribute("ssUsername"),
                                            (String) httpSession.getAttribute("ssOrgId"), totalDataCount + " Row(s) successfully extracted and loaded into target system.", "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
                                } catch (Exception e) {
                                }
//                    processETLLog(httpSession,totalDataCount + " Row(s) extracted successfully", 10);
                                resultObj.put("Message", totalDataCount + " Row(s) successfully extracted and loaded into target system.");
                                resultObj.put("connectionFlag", "Y");
                            }
                        }
                    }
                }

            }// end of columnObj if
            try {
                processETLLog((String) httpSession.getAttribute("ssUsername"),
                        (String) httpSession.getAttribute("ssOrgId"), "ETL Process is completed", "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception e) {
            }
        } catch (Exception e) {
            resultObj.put("Message", e.getMessage());
            resultObj.put("connectionFlag", "N");
            try {
                processETLLog((String) httpSession.getAttribute("ssUsername"),
                        (String) httpSession.getAttribute("ssOrgId"), "Problem in process ETL Data", "ERROR", 20, "N", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }
            e.printStackTrace();
        } finally {
            try {
                if (toPreparedStatement != null) {
                    toPreparedStatement.close();
                }
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

    public JSONObject processETLData(HttpSession httpSession,
            Connection fromConnection,
            Connection toConnection,
            Map fromOperator,
            Map toOperator,
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            JSONObject tablesWhereClauseObj,
            JSONObject defaultValuesObj,
            JSONObject appendValObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj,
            JSONObject normalizeOptionsObj
    ) {
        JSONObject resultObj = new JSONObject();
        PreparedStatement toPreparedStatement = null;
        try {
            //JSONObject fromColumnsObj = new JSONObject();			
            Map fromColumnsObj = new LinkedHashMap(); // ravi etl integration
            //JSONObject toColumnsObj = new JSONObject();			
            Map toColumnsObj = new LinkedHashMap(); // ravi etl integration
            if (columnsObj != null && !columnsObj.isEmpty()) {
                for (Object toColumnName : columnsObj.keySet()) {
                    if (toColumnName != null && columnsObj.get(toColumnName) != null) {
                        String fromColumnStr = (String) columnsObj.get(toColumnName);
                        if (fromColumnStr != null
                                && !"".equalsIgnoreCase(fromColumnStr)
                                && !"null".equalsIgnoreCase(fromColumnStr)) {
                            String[] fromColumnArray = fromColumnStr.split(":");
                            if (fromColumnArray != null && fromColumnArray.length != 0) {
                                if (fromColumnsObj != null && !fromColumnsObj.isEmpty()) {
                                    if (fromColumnsObj.containsKey(fromColumnArray[0])) {
                                        fromColumnsObj.put(fromColumnArray[0],
                                                (fromColumnsObj.get(fromColumnArray[0]) + "," + fromColumnArray[1]));
                                    } else {
                                        fromColumnsObj.put(fromColumnArray[0],
                                                (fromColumnArray[1]));
                                    }
                                } else {
                                    fromColumnsObj.put(fromColumnArray[0], (fromColumnArray[1]));
                                }
                            }
                        }
                        if (toColumnName != null
                                && !"".equalsIgnoreCase(String.valueOf(toColumnName))
                                && !"null".equalsIgnoreCase(String.valueOf(toColumnName))) {
                            String[] toColumnArray = String.valueOf(toColumnName).split(":");
                            if (toColumnArray != null && toColumnArray.length != 0) {
                                if (toColumnsObj != null && !toColumnsObj.isEmpty()) {
                                    if (toColumnsObj.containsKey(toColumnArray[0])) {
                                        toColumnsObj.put(toColumnArray[0],
                                                (toColumnsObj.get(toColumnArray[0]) + "," + toColumnArray[1]));
                                    } else {
                                        toColumnsObj.put(toColumnArray[0],
                                                (toColumnArray[1]));
                                    }
                                } else {
                                    toColumnsObj.put(toColumnArray[0], (toColumnArray[1]));
                                }
                            }
                        }

                    }

                }
                //System.out.println("toColumnsObj:::" + toColumnsObj);
                Set<String> toColumns = new HashSet<>();
                toColumns.addAll(columnsObj.keySet());
                if (defaultValuesObj != null && !defaultValuesObj.isEmpty()) {
                    Set<String> defaultColumns = defaultValuesObj.keySet();
                    if (defaultColumns != null
                            && !defaultColumns.isEmpty() //  && toColumns != null && !toColumns.isEmpty()
                            ) {
                        Iterator defaultColumnsItr = defaultColumns.iterator();
                        while (defaultColumnsItr.hasNext()) {
                            String defaultColName = (String) defaultColumnsItr.next();
                            if (defaultColName != null
                                    && !"".equalsIgnoreCase(String.valueOf(defaultColName))
                                    && !"null".equalsIgnoreCase(String.valueOf(defaultColName))) {
                                String[] toColumnArray = String.valueOf(defaultColName).split(":");
                                if (toColumnArray != null && toColumnArray.length != 0) {
                                    if (toColumnsObj != null && !toColumnsObj.isEmpty()) {
                                        if (toColumnsObj.containsKey(toColumnArray[0])) {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnsObj.get(toColumnArray[0]) + "," + toColumnArray[1]));
                                        } else {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnArray[1]));
                                        }
                                    } else {
                                        toColumnsObj.put(toColumnArray[0], (toColumnArray[1]));
                                    }
                                }
                            }
                        }
//                        toColumns.addAll(defaultColumns);
                    }
                }
                if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
                    Set<String> columnClauseCols = columnClauseObj.keySet();
                    if (columnClauseCols != null
                            && !columnClauseCols.isEmpty() //  && toColumns != null && !toColumns.isEmpty()
                            ) {
                        // toColumns.addAll(columnClauseCols);
                        Iterator columnClauseColsItr = columnClauseCols.iterator();
                        while (columnClauseColsItr.hasNext()) {
                            String columnClauseColName = (String) columnClauseColsItr.next();
                            if (columnClauseColName != null
                                    && !"".equalsIgnoreCase(String.valueOf(columnClauseColName))
                                    && !"null".equalsIgnoreCase(String.valueOf(columnClauseColName))) {
                                String[] toColumnArray = String.valueOf(columnClauseColName).split(":");
                                if (toColumnArray != null && toColumnArray.length != 0) {
                                    if (toColumnsObj != null && !toColumnsObj.isEmpty()) {
                                        if (toColumnsObj.containsKey(toColumnArray[0])) {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnsObj.get(toColumnArray[0]) + "," + toColumnArray[1]));
                                        } else {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnArray[1]));
                                        }
                                    } else {
                                        toColumnsObj.put(toColumnArray[0], (toColumnArray[1]));
                                    }
                                }
                            }
                        }
                    }
                }
                selectTabObj.put("toColumnsObj", toColumnsObj);
                List<Map> toOperatorsList = (List<Map>) selectTabObj.get("toOperatorList");
                if (toOperatorsList != null && !toOperatorsList.isEmpty()) {
                    for (Map toOperatorMap : toOperatorsList) {
                        if (toOperatorMap != null && !toOperatorMap.isEmpty()) {
                            toOperator = toOperatorMap;
                            String toTableName = (String) toOperator.get("tableName");
                            if (toTableName != null
                                    && !"".equalsIgnoreCase(String.valueOf(toTableName))
                                    && !"null".equalsIgnoreCase(String.valueOf(toTableName))) {
                                String toColumnsStr = (String) toColumnsObj.get(toTableName);
                                if (toColumnsStr != null
                                        && !"".equalsIgnoreCase(toColumnsStr)) {
                                    List<String> toColumnsList = new ArrayList<>();
                                    toColumnsList = Arrays.asList(toColumnsStr.split(","));
                                    if (toConnection != null) {
                                        String toTableInsertQuery = generateInsertQuery((String) toOperator.get("tableName"), toColumnsList);
                                        System.out.println("insertQuery::::" + toTableInsertQuery);
                                        toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
                                        Map columnsTypeObj = getColumnsType((String) toOperator.get("tableName"), toColumnsList, toConnection);
                                        selectTabObj.put("columnsTypeObj", columnsTypeObj);
                                    }

                                    int totalDataCount = 0;
                                    totalDataCount = processETLData(httpSession,
                                            fromConnection,
                                            toConnection,
                                            fromOperator,
                                            toOperator,
                                            columnsObj,
                                            fromColumnsObj,
                                            tablesWhereClauseObj,
                                            defaultValuesObj,
                                            toPreparedStatement,
                                            0,
                                            500,
                                            totalDataCount,
                                            toColumnsList, 0, "",
                                            10,
                                            appendValObj,
                                            columnClauseObj,
                                            selectTabObj,
                                            normalizeOptionsObj
                                    );
                                    System.out.println("totalDataCount::::" + totalDataCount);
                                    if (totalDataCount != 0 && totalDataCount > 0) {
                                        resultObj.put("Message", totalDataCount + " Row(s) extracted successfully");
                                        resultObj.put("connectionFlag", "Y");

                                    } else {
                                        resultObj.put("Message", "No data available in Source Tables/Problem in inserting the data in target table,please check log for more information.");
                                        resultObj.put("connectionFlag", "N");
                                    }
                                }
                            }
                        }
                    }
                }
                if (toColumnsObj != null && !toColumnsObj.isEmpty()) {

                    for (Object toTableName : toColumnsObj.keySet()) {
                        if (toTableName != null
                                && !"".equalsIgnoreCase(String.valueOf(toTableName))
                                && !"null".equalsIgnoreCase(String.valueOf(toTableName))) {
                            String toColumnsStr = (String) toColumnsObj.get(toTableName);
                            if (toColumnsStr != null
                                    && !"".equalsIgnoreCase(toColumnsStr)) {
                                List<String> toColumnsList = new ArrayList<>();
                                toColumnsList = Arrays.asList(toColumnsStr.split(","));
                                List<Map> matchedToOperatorsList = toOperatorsList.stream().filter(toOp
                                        -> (toOp != null
                                        && !toOp.isEmpty()
                                        && toOp.get("tableName") != null
                                        && !"".equalsIgnoreCase((String) toOp.get("tableName"))
                                        && !"null".equalsIgnoreCase((String) toOp.get("tableName"))
                                        && String.valueOf(toOp.get("tableName")).equalsIgnoreCase(String.valueOf(toTableName)))).collect(Collectors.toList());
                                if (matchedToOperatorsList != null && !matchedToOperatorsList.isEmpty()) {
                                    toOperator = matchedToOperatorsList.get(0);
                                }
                                if (toConnection != null) {
                                    String toTableInsertQuery = generateInsertQuery((String) toOperator.get("tableName"), toColumnsList);
                                    System.out.println("insertQuery::::" + toTableInsertQuery);
                                    toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
                                    Map columnsTypeObj = getColumnsType((String) toOperator.get("tableName"), toColumnsList, toConnection);
                                    selectTabObj.put("columnsTypeObj", columnsTypeObj);
                                }

//                                String toTableInsertQuery = generateInsertQuery((String) toOperator.get("tableName"), toColumnsList);
//                                System.out.println("insertQuery::::" + toTableInsertQuery);
//                                toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
                                int totalDataCount = 0;
                                totalDataCount = processETLData(httpSession,
                                        fromConnection,
                                        toConnection,
                                        fromOperator,
                                        toOperator,
                                        columnsObj,
                                        fromColumnsObj,
                                        tablesWhereClauseObj,
                                        defaultValuesObj,
                                        toPreparedStatement,
                                        0,
                                        500,
                                        totalDataCount,
                                        toColumnsList, 0, "",
                                        10,
                                        appendValObj,
                                        columnClauseObj,
                                        selectTabObj,
                                        normalizeOptionsObj
                                );
                                System.out.println("totalDataCount::::" + totalDataCount);
                                if (totalDataCount != 0 && totalDataCount > 0) {
                                    resultObj.put("Message", totalDataCount + " Row(s) extracted successfully");
                                    resultObj.put("connectionFlag", "Y");

                                } else {
                                    resultObj.put("Message", "No data available in Source Tables/Problem in inserting the data in target table,please check log for more information.");
                                    resultObj.put("connectionFlag", "N");
                                }
                            }
                        }
                    }
                }

            }// end of columnObj if

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

    public int processETLData(HttpSession httpSession,
            Connection fromConnection,
            Connection toConnection,
            List<Map> fromOperatorList,
            Map toOperator,
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            //JSONObject fromColumnsObj,
            Map fromColumnsObj, // ravi etl integration
            JSONObject tablesWhereClauseObj,
            JSONObject defaultValuesObj,
            PreparedStatement toPreparedStatement,
            int start,
            int limit,
            int totalDataCount,
            List<String> toColumnsList,
            JSONObject joinQueryMapObj,
            String joinQuery,
            int fileDataLastIndex,
            String fileName,
            int logSequenceNo,
            JSONObject appendValObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj,
            JSONObject normalizeOptionsObj
    ) {
        try {

            if (selectTabObj != null
                    && !selectTabObj.isEmpty()) {
                selectTabObj.put("columnClauseObj", columnClauseObj);
                String minRows = String.valueOf(selectTabObj.get("minRows"));
                if (minRows != null
                        && !"".equalsIgnoreCase(minRows)
                        && !"null".equalsIgnoreCase(minRows)
                        && NumberUtils.isNumber(minRows)) {
                    if (logSequenceNo == 1) {
                        start = Integer.parseInt(minRows);
                    }
                }
                String maxRows = String.valueOf(selectTabObj.get("maxRows"));
                if (maxRows != null
                        && !"".equalsIgnoreCase(maxRows)
                        && !"null".equalsIgnoreCase(maxRows)
                        && !"0".equalsIgnoreCase(maxRows)
                        && NumberUtils.isNumber(maxRows)) {
                    int maxRowsCount = Integer.parseInt(maxRows);
                    if (logSequenceNo == 1) {
                        if (((maxRowsCount - start)) <= limit) {
                            limit = (maxRowsCount - start);
                        }
                    } else {
                        if (((maxRowsCount - start) + 1) <= limit) {
                            limit = (maxRowsCount - start) + 1;
                        }
                    }

                }

            }
            Map fromOperator = fromOperatorList.get(0);
            int end = start + limit - 1;
            if (start == 0) {
                end = limit;
            }
            try {
                processETLLog((String) httpSession.getAttribute("ssUsername"),
                        (String) httpSession.getAttribute("ssOrgId"), "Fetching from " + start + " to " + end + " record(s).", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
//                        (String) httpSession.getAttribute("ssOrgId"), "Fetching next 1000 record(s).", "INFO", logSequenceNo, "Y");
            } catch (Exception e) {
            }
            List totalDataList = new ArrayList();
            if (limit > 0) {
                totalDataList = getSelectedJoinColumnsData((JSONObject) fromOperator.get("connObj"),
                        fromConnection,
                        fromColumnsObj,
                        start,
                        limit,
                        tablesWhereClauseObj, joinQueryMapObj, joinQuery,
                        (String) httpSession.getAttribute("ssUsername"),
                        (String) httpSession.getAttribute("ssOrgId"),
                        appendValObj,
                        columnClauseObj,
                        selectTabObj,
                        toConnection
                );
            }
            String iconType = (String) toOperator.get("iconType");
            if (totalDataList != null && !totalDataList.isEmpty()) {

                try {
                    processETLLog((String) httpSession.getAttribute("ssUsername"),
                            (String) httpSession.getAttribute("ssOrgId"), "Fetched " + totalDataList.size() + " record(s).", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                } catch (Exception e) {
                }
// String normalizeFlag = request.getParameter("normalizeFlag");
                if (normalizeOptionsObj != null && normalizeOptionsObj.size() > 0) {
                    String normalizeFlag = (String) normalizeOptionsObj.get("normalizeFlag");
//                    columnsObj = (JSONObject) normalizeOptionsObj.get("colsObj");
//                    Set<String> toColumnsSet = columnsObj.keySet();
//                    toColumnsList.clear();
//                    if (toColumnsSet != null && !toColumnsSet.isEmpty()) {
//                        toColumnsList.addAll(toColumnsSet);
//                    }
                    try {
                        processETLLog((String) httpSession.getAttribute("ssUsername"),
                                (String) httpSession.getAttribute("ssOrgId"), "Started " + normalizeFlag + " records.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                    } catch (Exception e) {
                    }
                    if (normalizeFlag != null && "normalize".equalsIgnoreCase(normalizeFlag)) {

                        totalDataList = getNoramlisedData(normalizeOptionsObj, totalDataList);
                    } else if (normalizeFlag != null && "deNormalize".equalsIgnoreCase(normalizeFlag)) {

                        totalDataList = getDeNoramlisedData(normalizeOptionsObj, totalDataList);
                    }
                }
                // ravi normalising   end	
                try {
                    processETLLog((String) httpSession.getAttribute("ssUsername"),
                            (String) httpSession.getAttribute("ssOrgId"), "Pushing"
                            + " " + totalDataList.size() + " record(s) into target system(" + ((fileName != null
                            && !"".equalsIgnoreCase(fileName)
                            && !"null".equalsIgnoreCase(fileName)) ? fileName : "" + toOperator.get("tableName")) + ").", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                } catch (Exception e) {
                }

                int insertCount = 0;
                JSONObject fileDataObj = new JSONObject();
                if ("XLSX".equalsIgnoreCase(iconType)) {
                    FileInputStream inputStream = null;
                    XSSFWorkbook wb = null;
                    XSSFSheet sheet = null;
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
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

                    insertCount = exportingXLSXFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            sheet,
                            wb,
                            selectTabObj
                    );

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
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
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

                    insertCount = exportingXLSFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            sheet,
                            wb,
                            selectTabObj
                    );

                    if (inputStream != null) {
                        inputStream.close();
                    }
                    if (wb != null) {
                        wb.close();
                    }
                } else if ("XML".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingXMLFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else if ("CSV".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingCsvAndTxtFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
//                    exportingCsvAndTxtFileData
                } else if ("TXT".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }

                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingCsvAndTxtFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else if ("JSON".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }

                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingJSONFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else {
                    String dragType = (String) toOperator.get("dragType");
                    JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
                    if ("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1")))) {
                        Object toConnectionObj = selectTabObj.get("toConnectionObj");
//                        Object toConnectionObj = getConnection(toConnObj);
                        if (toConnectionObj instanceof JCO.Client) {
                            if ("Table".equalsIgnoreCase(dragType)) {
                                // need to call BAPI for pushing into SAP
                                insertCount = dataMigrationService.importSAPData((String) toOperator.get("tableName"),
                                        fromColumnsObj,
                                        columnsObj,
                                        totalDataList,
                                        (JCO.Client) toConnectionObj,
                                        toColumnsList,
                                        defaultValuesObj,
                                        (String) httpSession.getAttribute("ssUsername"),
                                        (String) httpSession.getAttribute("ssOrgId"),
                                        String.valueOf(selectTabObj.get("jobId")),
                                        selectTabObj);
                            }
                        }
                    } else {
                        insertCount = dataMigrationService.importingData((String) toOperator.get("tableName"),
                                fromColumnsObj,
                                columnsObj,
                                totalDataList,
                                toPreparedStatement,
                                toColumnsList,
                                defaultValuesObj,
                                (String) httpSession.getAttribute("ssUsername"),
                                (String) httpSession.getAttribute("ssOrgId"),
                                String.valueOf(selectTabObj.get("jobId")),
                                selectTabObj
                        );
                    }
                }
                if (insertCount != 0) {
                    try {
                        processETLLog((String) httpSession.getAttribute("ssUsername"),
                                (String) httpSession.getAttribute("ssOrgId"), "Pushed " + insertCount + " record(s) into target system(" + ((fileName != null
                                && !"".equalsIgnoreCase(fileName)
                                && !"null".equalsIgnoreCase(fileName)) ? fileName : "" + toOperator.get("tableName")) + ").", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                    } catch (Exception e) {
                    }
                }
                totalDataCount += insertCount;
                if (insertCount != 0 && insertCount >= 1000) {
                    //int startIndex = (logSequenceNo * limit + 1);
                    int startIndex = (logSequenceNo * limit); // ravi etl integration
                    logSequenceNo++;
                    totalDataCount = processETLData(httpSession,
                            fromConnection,
                            toConnection,
                            fromOperatorList,
                            toOperator,
                            columnsObj,
                            fromColumnsObj,
                            tablesWhereClauseObj,
                            defaultValuesObj,
                            toPreparedStatement,
                            startIndex,
                            limit,
                            totalDataCount,
                            toColumnsList,
                            joinQueryMapObj,
                            joinQuery,
                            fileDataLastIndex,
                            fileName,
                            logSequenceNo,
                            appendValObj,
                            columnClauseObj,
                            selectTabObj,
                            normalizeOptionsObj
                    );

                } else if ("JSON".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
                    File file12 = new File(filePath);
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    if (outputFile.exists()) {
                        FileOutputStream fos = new FileOutputStream(outputFile, true);
                        OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
                        BufferedWriter writer = new BufferedWriter(osw);
                        writer.append("]");
                        writer.close();
                        osw.close();
                        fos.close();
                    }
                }
            } else if ("JSON".equalsIgnoreCase(iconType)) {
                String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
                File file12 = new File(filePath);
                File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                if (outputFile.exists()) {
                    FileOutputStream fos = new FileOutputStream(outputFile, true);
                    OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
                    BufferedWriter writer = new BufferedWriter(osw);
                    writer.append("]");
                    writer.close();
                    osw.close();
                    fos.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();

            try {
                processETLLog((String) httpSession.getAttribute("ssUsername"),
                        (String) httpSession.getAttribute("ssOrgId"), "Failed process records in ETL due to " + e.getLocalizedMessage(), "ERROR", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }
        }
        return totalDataCount;
    }

    public int processETLData(HttpSession httpSession,
            Connection toConnection,
            List<Map> fromOperatorList,
            Map toOperator,
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            //JSONObject fromColumnsObj,
            Map fromColumnsObj, // ravi etl integration
            JSONObject tablesWhereClauseObj,
            JSONObject defaultValuesObj,
            PreparedStatement toPreparedStatement,
            int start,
            int limit,
            int totalDataCount,
            List<String> toColumnsList,
            JSONObject joinQueryMapObj,
            String joinQuery,
            int fileDataLastIndex,
            String fileName,
            int logSequenceNo,
            JSONObject appendValObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj,
            JSONObject normalizeOptionsObj
    ) {
        try {
            String iconType = (String) toOperator.get("iconType");
            if (selectTabObj != null
                    && !selectTabObj.isEmpty()) {
                selectTabObj.put("columnClauseObj", columnClauseObj);
                String minRows = String.valueOf(selectTabObj.get("minRows"));
                if (minRows != null
                        && !"".equalsIgnoreCase(minRows)
                        && !"null".equalsIgnoreCase(minRows)
                        && NumberUtils.isNumber(minRows)) {
                    if (logSequenceNo == 1) {
                        start = Integer.parseInt(minRows);
                    }
                }
                String maxRows = String.valueOf(selectTabObj.get("maxRows"));
                if (maxRows != null
                        && !"".equalsIgnoreCase(maxRows)
                        && !"null".equalsIgnoreCase(maxRows)
                        && !"0".equalsIgnoreCase(maxRows)
                        && NumberUtils.isNumber(maxRows)) {
                    int maxRowsCount = Integer.parseInt(maxRows);
                    if (logSequenceNo == 1) {
                        if (((maxRowsCount - start)) <= limit) {
                            limit = (maxRowsCount - start);
                        }
                    } else {
                        if (((maxRowsCount - start) + 1) <= limit) {
                            limit = (maxRowsCount - start) + 1;
                        }
                    }

                }

            }
            Map fromOperator = fromOperatorList.get(0);
            int end = start + limit - 1;
            if (start == 0) {
                end = limit;
            }
            try {
                processETLLog((String) httpSession.getAttribute("ssUsername"),
                        (String) httpSession.getAttribute("ssOrgId"), "Fetching from " + start + " to " + end + " record(s).", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
//                        (String) httpSession.getAttribute("ssOrgId"), "Fetching next 1000 record(s).", "INFO", logSequenceNo, "Y");
            } catch (Exception e) {
            }
            List totalDataList = new ArrayList();
            if (limit > 0) {
//                totalDataList = getSelectedJoinColumnsData((JSONObject) fromOperator.get("connObj"),
//                        fromConnection,
//                        fromColumnsObj,
//                        start,
//                        limit,
//                        tablesWhereClauseObj, joinQueryMapObj, joinQuery,
//                        (String) httpSession.getAttribute("ssUsername"),
//                        (String) httpSession.getAttribute("ssOrgId"),
//                        appendValObj,
//                        columnClauseObj,
//                        selectTabObj,
//                        toConnection
//                );
            }

            if (totalDataList != null && !totalDataList.isEmpty()) {

                try {
                    processETLLog((String) httpSession.getAttribute("ssUsername"),
                            (String) httpSession.getAttribute("ssOrgId"), "Fetched " + totalDataList.size() + " record(s).", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                } catch (Exception e) {
                }
                if (normalizeOptionsObj != null && normalizeOptionsObj.size() > 0) {
                    String normalizeFlag = (String) normalizeOptionsObj.get("normalizeFlag");
//                    columnsObj = (JSONObject) normalizeOptionsObj.get("colsObj");
//                    Set<String> toColumnsSet = columnsObj.keySet();

                    try {
                        processETLLog((String) httpSession.getAttribute("ssUsername"),
                                (String) httpSession.getAttribute("ssOrgId"), "Started " + normalizeFlag + " records.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                    } catch (Exception e) {
                    }
                    if (normalizeFlag != null && "normalize".equalsIgnoreCase(normalizeFlag)) {

                        totalDataList = getNoramlisedData(normalizeOptionsObj, totalDataList);
                    } else if (normalizeFlag != null && "deNormalize".equalsIgnoreCase(normalizeFlag)) {

                        totalDataList = getDeNoramlisedData(normalizeOptionsObj, totalDataList);
                    }
                }
                try {
                    processETLLog((String) httpSession.getAttribute("ssUsername"),
                            (String) httpSession.getAttribute("ssOrgId"), "Pushing " + totalDataList.size() + " record(s) into target system(" + ((fileName != null
                            && !"".equalsIgnoreCase(fileName)
                            && !"null".equalsIgnoreCase(fileName)) ? fileName : "" + toOperator.get("tableName")) + ").", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                } catch (Exception e) {
                }
//                String iconType = (String) toOperator.get("iconType");
                int insertCount = 0;
                JSONObject fileDataObj = new JSONObject();
                if ("XLSX".equalsIgnoreCase(iconType)) {
                    FileInputStream inputStream = null;
                    XSSFWorkbook wb = null;
                    XSSFSheet sheet = null;
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
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

                    insertCount = exportingXLSXFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            sheet,
                            wb,
                            selectTabObj
                    );

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
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
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

                    insertCount = exportingXLSFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            sheet,
                            wb,
                            selectTabObj
                    );

                    if (inputStream != null) {
                        inputStream.close();
                    }
                    if (wb != null) {
                        wb.close();
                    }
                } else if ("XML".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingXMLFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else if ("CSV".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingCsvAndTxtFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
//                    exportingCsvAndTxtFileData
                } else if ("TXT".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }

                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingCsvAndTxtFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else if ("JSON".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }

                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingJSONFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else {
                    selectTabObj.put("oldTableName", null); // ravi etl integration
                    String dragType = (String) toOperator.get("dragType");
                    JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
                    if ("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1")))) {
                        Object toConnectionObj = selectTabObj.get("toConnectionObj");
//                        Object toConnectionObj = getConnection(toConnObj);
                        if (toConnectionObj instanceof JCO.Client) {
                            if ("Table".equalsIgnoreCase(dragType)) {
                                // need to call BAPI for pushing into SAP
                                insertCount = dataMigrationService.importSAPData((String) toOperator.get("tableName"),
                                        fromColumnsObj,
                                        columnsObj,
                                        totalDataList,
                                        (JCO.Client) toConnectionObj,
                                        toColumnsList,
                                        defaultValuesObj,
                                        (String) httpSession.getAttribute("ssUsername"),
                                        (String) httpSession.getAttribute("ssOrgId"),
                                        String.valueOf(selectTabObj.get("jobId")),
                                        selectTabObj);
                            }
                        }
                    } else {
                        insertCount = dataMigrationService.importingData((String) toOperator.get("tableName"),
                                fromColumnsObj,
                                columnsObj,
                                totalDataList,
                                toPreparedStatement,
                                toColumnsList,
                                defaultValuesObj, (String) httpSession.getAttribute("ssUsername"),
                                (String) httpSession.getAttribute("ssOrgId"),
                                String.valueOf(selectTabObj.get("jobId")),
                                selectTabObj);
                    }
                }
                if (insertCount != 0) {
                    try {
                        processETLLog((String) httpSession.getAttribute("ssUsername"),
                                (String) httpSession.getAttribute("ssOrgId"), "Pushed " + insertCount + " record(s) into target system(" + ((fileName != null
                                && !"".equalsIgnoreCase(fileName)
                                && !"null".equalsIgnoreCase(fileName)) ? fileName : "" + toOperator.get("tableName")) + ").", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                    } catch (Exception e) {
                    }
                }
                totalDataCount += insertCount;
                if (insertCount != 0 && insertCount >= 1000) {
                    //int startIndex = (logSequenceNo * limit + 1);
                    int startIndex = (logSequenceNo * limit); // ravi etl integration
                    logSequenceNo++;
                    totalDataCount = processETLData(httpSession,
                            //                            fromConnection,
                            toConnection,
                            fromOperatorList,
                            toOperator,
                            columnsObj,
                            fromColumnsObj,
                            tablesWhereClauseObj,
                            defaultValuesObj,
                            toPreparedStatement,
                            startIndex,
                            limit,
                            totalDataCount,
                            toColumnsList,
                            joinQueryMapObj,
                            joinQuery,
                            fileDataLastIndex,
                            fileName,
                            logSequenceNo,
                            appendValObj,
                            columnClauseObj,
                            selectTabObj,
                            normalizeOptionsObj
                    );

                } else if ("JSON".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
                    File file12 = new File(filePath);
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    if (outputFile.exists()) {
                        FileOutputStream fos = new FileOutputStream(outputFile, true);
                        OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
                        BufferedWriter writer = new BufferedWriter(osw);
                        writer.append("]");
                        writer.close();
                        osw.close();
                        fos.close();
                    }
                }
            } else if ("JSON".equalsIgnoreCase(iconType)) {
                String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
                File file12 = new File(filePath);
                File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                if (outputFile.exists()) {
                    FileOutputStream fos = new FileOutputStream(outputFile, true);
                    OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
                    BufferedWriter writer = new BufferedWriter(osw);
                    writer.append("]");
                    writer.close();
                    osw.close();
                    fos.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();

            try {
                processETLLog((String) httpSession.getAttribute("ssUsername"),
                        (String) httpSession.getAttribute("ssOrgId"), "Failed process records in ETL due to " + e.getLocalizedMessage(), "ERROR", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }
        }
        return totalDataCount;
    }

    public int processETLData(HttpSession httpSession,
            Connection fromConnection,
            Connection toConnection,
            Map fromOperator,
            Map toOperator,
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            //JSONObject fromColumnsObj,
            Map fromColumnsObj, // ravi etl integration
            JSONObject tablesWhereClauseObj,
            JSONObject defaultValuesObj,
            PreparedStatement toPreparedStatement,
            int start,
            int limit,
            int totalDataCount,
            List<String> toColumnsList,
            int fileDataLastIndex,
            String fileName,
            int logSequenceNo,
            JSONObject appendValObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj,
            JSONObject normalizeOptionsObj
    ) {
        Object resultObj = null;
        try {
            if (selectTabObj != null
                    && !selectTabObj.isEmpty()) {
                selectTabObj.put("columnClauseObj", columnClauseObj);
                String minRows = String.valueOf(selectTabObj.get("minRows"));
                if (minRows != null
                        && !"".equalsIgnoreCase(minRows)
                        && !"null".equalsIgnoreCase(minRows)
                        && NumberUtils.isNumber(minRows)) {
                    if (logSequenceNo == 1) {
                        start = Integer.parseInt(minRows);
                    }
                }
                String maxRows = String.valueOf(selectTabObj.get("maxRows"));
                if (maxRows != null
                        && !"".equalsIgnoreCase(maxRows)
                        && !"null".equalsIgnoreCase(maxRows)
                        && NumberUtils.isNumber(maxRows)) {
                    int maxRowsCount = Integer.parseInt(maxRows);
                    if (logSequenceNo == 1) {
                        if (((maxRowsCount - start)) <= limit) {
                            limit = (maxRowsCount - start);
                        }
                    } else {
                        if (((maxRowsCount - start) + 1) <= limit) {
                            limit = (maxRowsCount - start) + 1;
                        }
                    }
                }

            }
            int end = start + limit - 1;
            if (start == 0) {
                end = limit;
            }
            try {
                processETLLog((String) httpSession.getAttribute("ssUsername"),
                        (String) httpSession.getAttribute("ssOrgId"), "Fetching from " + start + " to " + end + " record(s).", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception e) {
            }
            List totalDataList = new ArrayList();
            if (limit > 0) {
                totalDataList = getSelectedColumnsData((JSONObject) fromOperator.get("connObj"),
                        fromConnection,
                        fromColumnsObj,
                        start,
                        limit,
                        tablesWhereClauseObj, (String) httpSession.getAttribute("ssUsername"),
                        (String) httpSession.getAttribute("ssOrgId"), appendValObj, columnClauseObj, selectTabObj, toConnection);
            }
            String iconType = (String) toOperator.get("iconType");
            if (totalDataList != null && !totalDataList.isEmpty()) {
                try {
                    processETLLog((String) httpSession.getAttribute("ssUsername"),
                            (String) httpSession.getAttribute("ssOrgId"), "Fetched " + totalDataList.size() + " record(s).", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                } catch (Exception e) {
                }
                if (normalizeOptionsObj != null && normalizeOptionsObj.size() > 0) {
                    String normalizeFlag = (String) normalizeOptionsObj.get("normalizeFlag");
//                    columnsObj = (JSONObject) normalizeOptionsObj.get("colsObj");
//                    Set<String> toColumnsSet = columnsObj.keySet();

                    try {
                        processETLLog((String) httpSession.getAttribute("ssUsername"),
                                (String) httpSession.getAttribute("ssOrgId"), "Started " + normalizeFlag + " records.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                    } catch (Exception e) {
                    }
                    if (normalizeFlag != null && "normalize".equalsIgnoreCase(normalizeFlag)) {

                        totalDataList = getNoramlisedData(normalizeOptionsObj, totalDataList);
                    } else if (normalizeFlag != null && "deNormalize".equalsIgnoreCase(normalizeFlag)) {

                        totalDataList = getDeNoramlisedData(normalizeOptionsObj, totalDataList);
                    }
                }
                try {
                    processETLLog((String) httpSession.getAttribute("ssUsername"),
                            (String) httpSession.getAttribute("ssOrgId"), "Pushing " + totalDataList.size() + " record(s) into target system(" + ((fileName != null
                            && !"".equalsIgnoreCase(fileName)
                            && !"null".equalsIgnoreCase(fileName)) ? fileName : "" + toOperator.get("tableName")) + ").", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                } catch (Exception e) {
                }
                int insertCount = 0;
//                String iconType = (String) toOperator.get("iconType");
                if ("XLSX".equalsIgnoreCase(iconType)) {
                    FileInputStream inputStream = null;
                    XSSFWorkbook wb = null;
                    XSSFSheet sheet = null;
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
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

                    insertCount = exportingXLSXFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            sheet,
                            wb,
                            selectTabObj
                    );

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
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
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

                    insertCount = exportingXLSFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            sheet,
                            wb,
                            selectTabObj
                    );

                    if (inputStream != null) {
                        inputStream.close();
                    }
                    if (wb != null) {
                        wb.close();
                    }
                } else if ("XML".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingXMLFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else if ("CSV".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingCsvAndTxtFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
//                    exportingCsvAndTxtFileData
                } else if ("TXT".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }

                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingCsvAndTxtFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else if ("JSON".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }

                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingJSONFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else {
                    selectTabObj.put("oldTableName", null); // ravi etl integration
                    String dragType = (String) toOperator.get("dragType");
                    JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
                    if ("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1")))) {
                        Object toConnectionObj = selectTabObj.get("toConnectionObj");
//                        Object toConnectionObj = getConnection(toConnObj);
                        if (toConnectionObj instanceof JCO.Client) {
                            if ("Table".equalsIgnoreCase(dragType)) {
                                // need to call BAPI for pushing into SAP
                                insertCount = dataMigrationService.importSAPData((String) toOperator.get("tableName"),
                                        fromColumnsObj,
                                        columnsObj,
                                        totalDataList,
                                        (JCO.Client) toConnectionObj,
                                        toColumnsList,
                                        defaultValuesObj,
                                        (String) httpSession.getAttribute("ssUsername"),
                                        (String) httpSession.getAttribute("ssOrgId"),
                                        String.valueOf(selectTabObj.get("jobId")),
                                        selectTabObj);
                            }
                        }
                    } else {
                        insertCount = dataMigrationService.importingData((String) toOperator.get("tableName"),
                                fromColumnsObj,
                                columnsObj,
                                totalDataList,
                                toPreparedStatement,
                                toColumnsList,
                                defaultValuesObj,
                                (String) httpSession.getAttribute("ssUsername"),
                                (String) httpSession.getAttribute("ssOrgId"),
                                String.valueOf(selectTabObj.get("jobId")),
                                selectTabObj);
                    }
                }
                if (insertCount != 0) {
                    try {
                        processETLLog((String) httpSession.getAttribute("ssUsername"),
                                (String) httpSession.getAttribute("ssOrgId"), "Pushed " + insertCount + " record(s) into target system(" + ((fileName != null
                                && !"".equalsIgnoreCase(fileName)
                                && !"null".equalsIgnoreCase(fileName)) ? fileName : "" + toOperator.get("tableName")) + ").", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                    } catch (Exception e) {
                    }
                }

                totalDataCount += insertCount;
                if (insertCount != 0 && insertCount >= 1000) {
                    //int startIndex = (logSequenceNo * limit + 1);
                    int startIndex = (logSequenceNo * limit); // ravi etl integration
                    logSequenceNo++;
                    totalDataCount = processETLData(httpSession,
                            fromConnection,
                            toConnection,
                            fromOperator,
                            toOperator,
                            columnsObj,
                            fromColumnsObj,
                            tablesWhereClauseObj,
                            defaultValuesObj,
                            toPreparedStatement,
                            startIndex,
                            //(logSequenceNo * limit + 1),
                            limit,
                            totalDataCount,
                            toColumnsList,
                            fileDataLastIndex,
                            fileName,
                            logSequenceNo,
                            appendValObj,
                            columnClauseObj,
                            selectTabObj,
                            normalizeOptionsObj
                    );

                } else if ("JSON".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
                    File file12 = new File(filePath);
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    if (outputFile.exists()) {
                        FileOutputStream fos = new FileOutputStream(outputFile, true);
                        OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
                        BufferedWriter writer = new BufferedWriter(osw);
                        writer.append("]");
                        writer.close();
                        osw.close();
                        fos.close();
                    }
                }
            } else if ("JSON".equalsIgnoreCase(iconType)) {
                String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
                File file12 = new File(filePath);
                File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                if (outputFile.exists()) {
                    FileOutputStream fos = new FileOutputStream(outputFile, true);
                    OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
                    BufferedWriter writer = new BufferedWriter(osw);
                    writer.append("]");
                    writer.close();
                    osw.close();
                    fos.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            try {
                processETLLog((String) httpSession.getAttribute("ssUsername"),
                        (String) httpSession.getAttribute("ssOrgId"), e.getMessage(), "ERROR", 20, "Y", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }
        }
        return totalDataCount;
    }

    public int processETLData(HttpSession httpSession,
            Connection toConnection,
            Map fromOperator,
            Map toOperator,
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            //JSONObject fromColumnsObj,
            Map fromColumnsObj, // ravi etl integration
            JSONObject tablesWhereClauseObj,
            JSONObject defaultValuesObj,
            PreparedStatement toPreparedStatement,
            int start,
            int limit,
            int totalDataCount,
            List<String> toColumnsList,
            int fileDataLastIndex,
            String fileName,
            int logSequenceNo,
            JSONObject appendValObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj,
            JSONObject normalizeOptionsObj
    ) {
        Object resultObj = null;
        try {
            if (selectTabObj != null
                    && !selectTabObj.isEmpty()) {
                selectTabObj.put("columnClauseObj", columnClauseObj);
                String minRows = String.valueOf(selectTabObj.get("minRows"));
                if (minRows != null
                        && !"".equalsIgnoreCase(minRows)
                        && !"null".equalsIgnoreCase(minRows)
                        && NumberUtils.isNumber(minRows)) {
                    if (logSequenceNo == 1) {
                        start = Integer.parseInt(minRows);
                    }
                }
                String maxRows = String.valueOf(selectTabObj.get("maxRows"));
                if (maxRows != null
                        && !"".equalsIgnoreCase(maxRows)
                        && !"null".equalsIgnoreCase(maxRows)
                        && NumberUtils.isNumber(maxRows)) {
                    int maxRowsCount = Integer.parseInt(maxRows);
                    if (logSequenceNo == 1) {
                        if (((maxRowsCount - start)) <= limit) {
                            limit = (maxRowsCount - start);
                        }
                    } else {
                        if (((maxRowsCount - start) + 1) <= limit) {
                            limit = (maxRowsCount - start) + 1;
                        }
                    }
                }

            }
            int end = start + limit - 1;
            if (start == 0) {
                end = limit;
            }
            try {
                processETLLog((String) httpSession.getAttribute("ssUsername"),
                        (String) httpSession.getAttribute("ssOrgId"), "Fetching from " + start + " to " + end + " record(s).", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception e) {
            }
            List totalDataList = new ArrayList();
            if (limit > 0) {
                JSONObject fileObj = (JSONObject) fromOperator.get("connObj");
                if (fileObj != null && !fileObj.isEmpty()) {
                    String fileType = (String) fileObj.get("fileType");
                    String fromFileName = (String) fileObj.get("fileName");
                    String filePath = (String) fileObj.get("filePath");
                    List<String> columnList = new ArrayList<>();

//                    if (columnList != null && !columnList.isEmpty()) {
//                        columnList = columnList.stream().map(header ->(header.replaceAll("\\s","_")))
//                                .collect(Collectors.toList());
//                    }
//                    HttpSession httpSession,
//                            String filepath
//                    , 
//            List<String> columnList,
//                    int startIndex,
//                    int limit,
//                    int endIndex
                    if (".xls".equalsIgnoreCase(fileType) || ".xlsx".equalsIgnoreCase(fileType)) {
                        columnList = getHeadersOfImportedFile(filePath, fileType);
                        totalDataList = readExcel(httpSession, filePath, columnList, start, limit, end, fromFileName);
                    } else if (".CSV".equalsIgnoreCase(fileType)
                            || ".TXT".equalsIgnoreCase(fileType)
                            || ".JSON".equalsIgnoreCase(fileType)) {
                        columnList = getHeadersOfImportedFile(filePath, fileType);
                        totalDataList = readCSV(httpSession, filePath, columnList, start, limit, end, fromFileName, fileType);
                    } else if (".xml".equalsIgnoreCase(fileType)) {
                        totalDataList = readXML(httpSession, filePath, columnList, start, limit, end, fromFileName);
                    }
                }

//                totalDataList = getSelectedColumnsData((JSONObject) fromOperator.get("connObj"),
//                        fromConnection,
//                        fromColumnsObj,
//                        start,
//                        limit,
//                        tablesWhereClauseObj, (String) httpSession.getAttribute("ssUsername"),
//                        (String) httpSession.getAttribute("ssOrgId"), appendValObj, columnClauseObj, selectTabObj, toConnection);
            }
            String iconType = (String) toOperator.get("iconType");
            if (totalDataList != null && !totalDataList.isEmpty()) {
                try {
                    processETLLog((String) httpSession.getAttribute("ssUsername"),
                            (String) httpSession.getAttribute("ssOrgId"), "Fetched " + totalDataList.size() + " record(s).", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                } catch (Exception e) {
                }
                if (normalizeOptionsObj != null && normalizeOptionsObj.size() > 0) {
                    String normalizeFlag = (String) normalizeOptionsObj.get("normalizeFlag");
//                    columnsObj = (JSONObject) normalizeOptionsObj.get("colsObj");
//                    Set<String> toColumnsSet = columnsObj.keySet();

                    try {
                        processETLLog((String) httpSession.getAttribute("ssUsername"),
                                (String) httpSession.getAttribute("ssOrgId"), "Started " + normalizeFlag + " records.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                    } catch (Exception e) {
                    }
                    if (normalizeFlag != null && "normalize".equalsIgnoreCase(normalizeFlag)) {

                        totalDataList = getNoramlisedData(normalizeOptionsObj, totalDataList);
                    } else if (normalizeFlag != null && "deNormalize".equalsIgnoreCase(normalizeFlag)) {

                        totalDataList = getDeNoramlisedData(normalizeOptionsObj, totalDataList);
                    }
                }
                try {
                    processETLLog((String) httpSession.getAttribute("ssUsername"),
                            (String) httpSession.getAttribute("ssOrgId"), "Pushing " + totalDataList.size() + " record(s) into target system(" + ((fileName != null
                            && !"".equalsIgnoreCase(fileName)
                            && !"null".equalsIgnoreCase(fileName)) ? fileName : "" + toOperator.get("tableName")) + ").", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                } catch (Exception e) {
                }
                int insertCount = 0;
//                String iconType = (String) toOperator.get("iconType");
                if ("XLSX".equalsIgnoreCase(iconType)) {
                    FileInputStream inputStream = null;
                    XSSFWorkbook wb = null;
                    XSSFSheet sheet = null;
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
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

                    insertCount = exportingXLSXFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            sheet,
                            wb,
                            selectTabObj
                    );

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
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
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

                    insertCount = exportingXLSFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            sheet,
                            wb,
                            selectTabObj
                    );

                    if (inputStream != null) {
                        inputStream.close();
                    }
                    if (wb != null) {
                        wb.close();
                    }
                } else if ("XML".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingXMLFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else if ("CSV".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingCsvAndTxtFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
//                    exportingCsvAndTxtFileData
                } else if ("TXT".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }

                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingCsvAndTxtFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else if ("JSON".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }

                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingJSONFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else {
                    selectTabObj.put("oldTableName", null); // ravi etl integration
                    String dragType = (String) toOperator.get("dragType");
                    JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
                    if ("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1")))) {
                        Object toConnectionObj = selectTabObj.get("toConnectionObj");
//                        Object toConnectionObj = getConnection(toConnObj);
                        if (toConnectionObj instanceof JCO.Client) {
                            if ("Table".equalsIgnoreCase(dragType)) {
                                // need to call BAPI for pushing into SAP
                                insertCount = dataMigrationService.importSAPData((String) toOperator.get("tableName"),
                                        fromColumnsObj,
                                        columnsObj,
                                        totalDataList,
                                        (JCO.Client) toConnectionObj,
                                        toColumnsList,
                                        defaultValuesObj,
                                        (String) httpSession.getAttribute("ssUsername"),
                                        (String) httpSession.getAttribute("ssOrgId"),
                                        String.valueOf(selectTabObj.get("jobId")),
                                        selectTabObj);
                            }
                        }
                    } else {
                        insertCount = dataMigrationService.importingData((String) toOperator.get("tableName"),
                                fromColumnsObj,
                                columnsObj,
                                totalDataList,
                                toPreparedStatement,
                                toColumnsList,
                                defaultValuesObj,
                                (String) httpSession.getAttribute("ssUsername"),
                                (String) httpSession.getAttribute("ssOrgId"),
                                String.valueOf(selectTabObj.get("jobId")),
                                selectTabObj);
                    }
                }
                if (insertCount != 0) {
                    try {
                        processETLLog((String) httpSession.getAttribute("ssUsername"),
                                (String) httpSession.getAttribute("ssOrgId"), "Pushed " + insertCount + " record(s) into target system(" + ((fileName != null
                                && !"".equalsIgnoreCase(fileName)
                                && !"null".equalsIgnoreCase(fileName)) ? fileName : "" + toOperator.get("tableName")) + ").", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                    } catch (Exception e) {
                    }
                }

                totalDataCount += insertCount;
                if (insertCount != 0 && insertCount >= 1000) {
                    //int startIndex = (logSequenceNo * limit + 1);
                    int startIndex = (logSequenceNo * limit); // ravi etl integration

                    logSequenceNo++;
                    totalDataCount = processETLData(httpSession,
                            toConnection,
                            fromOperator,
                            toOperator,
                            columnsObj,
                            fromColumnsObj,
                            tablesWhereClauseObj,
                            defaultValuesObj,
                            toPreparedStatement,
                            startIndex,
                            //(logSequenceNo * limit + 1),
                            limit,
                            totalDataCount,
                            toColumnsList,
                            fileDataLastIndex,
                            fileName,
                            logSequenceNo,
                            appendValObj,
                            columnClauseObj,
                            selectTabObj,
                            normalizeOptionsObj
                    );

                } else if ("JSON".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
                    File file12 = new File(filePath);
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    if (outputFile.exists()) {
                        FileOutputStream fos = new FileOutputStream(outputFile, true);
                        OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
                        BufferedWriter writer = new BufferedWriter(osw);
                        writer.append("]");
                        writer.close();
                        osw.close();
                        fos.close();
                    }
                }
            } else if ("JSON".equalsIgnoreCase(iconType)) {
                String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + httpSession.getAttribute("ssUsername");
                File file12 = new File(filePath);
                File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                if (outputFile.exists()) {
                    FileOutputStream fos = new FileOutputStream(outputFile, true);
                    OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
                    BufferedWriter writer = new BufferedWriter(osw);
                    writer.append("]");
                    writer.close();
                    osw.close();
                    fos.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            try {
                processETLLog((String) httpSession.getAttribute("ssUsername"),
                        (String) httpSession.getAttribute("ssOrgId"), e.getMessage(), "ERROR", 20, "Y", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }
        }
        return totalDataCount;
    }

    public List getSelectedColumnsData(JSONObject dbObj,
            Connection fromConnection,
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            int start,
            int limit,
            JSONObject tablesConditionObj,
            String loginUserName,
            String loginOrgnId,
            JSONObject appendValObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj, Connection toConnection) {
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
            System.out.println(" initParamClassName:" + initParamClassName + "initParamMethodName:" + initParamMethodName);
            Class clazz = Class.forName(initParamClassName);
            Class<?>[] paramTypes = {Connection.class,
                //JSONObject.class,
                Map.class, // ravi etl integration
                Integer.class,
                Integer.class, JSONObject.class, String.class, String.class, String.class,
                String.class, String.class, String.class, String.class, JSONObject.class, JSONObject.class, JSONObject.class, Connection.class};
            Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
            Object targetObj = new PilogUtilities().createObjectByClass(clazz);
            totalData = (List) method.invoke(targetObj, fromConnection, columnsObj, start, limit, tablesConditionObj,
                    String.valueOf(dbObj.get("CONN_DB_NAME")), loginUserName, loginOrgnId,
                    dataBaseDriver, dbURL, userName, password, appendValObj,
                    columnClauseObj,
                    selectTabObj, toConnection);

        } catch (Exception e) {

            e.printStackTrace();
        }
        return totalData;
    }

    public List getSelectedJoinColumnsData(JSONObject dbObj,
            Connection fromConnection,
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            int start,
            int limit,
            JSONObject tablesConditionObj, JSONObject joinColsObj,
            String joinQuery, String loginUserName,
            String loginOrgnId,
            JSONObject appendValObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj, Connection toConnection) {
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
            System.out.println(" initParamClassName:" + initParamClassName + "initParamMethodName:" + initParamMethodName);
            Class clazz = Class.forName(initParamClassName);
            Class<?>[] paramTypes = {Connection.class,
                //JSONObject.class,
                Map.class, // ravi etl integration
                Integer.class, Integer.class,
                JSONObject.class, String.class, JSONObject.class,
                String.class, String.class, String.class,
                String.class, String.class, String.class,
                String.class, JSONObject.class, JSONObject.class, JSONObject.class, Connection.class};
            Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
            Object targetObj = new PilogUtilities().createObjectByClass(clazz);
            totalData = (List) method.invoke(targetObj, fromConnection, columnsObj, start, limit,
                    tablesConditionObj, String.valueOf(dbObj.get("CONN_DB_NAME")), joinColsObj,
                    joinQuery, loginUserName, loginOrgnId, dataBaseDriver, dbURL, userName, password, appendValObj,
                    columnClauseObj,
                    selectTabObj, toConnection);
//            dataBaseDriver
//            );
//            logConnection = DriverManager.getConnection(dbURL, userName, password);

        } catch (Exception e) {

            e.printStackTrace();
        }
        return totalData;
    }

    public int exportingXLSXFileData(Map toOperator,
            //JSONObject destColumnsObj,
            Map destColumnsObj,// ravi etl integration
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            List totalData,
            PreparedStatement importStmt,
            List<String> columnsList,
            JSONObject defaultValuesObj,
            String filePath,
            String fileName,
            int lastRowIndex,
            XSSFSheet sheet,
            XSSFWorkbook wb,
            JSONObject selectTabObj
    ) {
        int insertCount = 0;
        try {
            if (totalData != null && !totalData.isEmpty() && columnsObj != null && !columnsObj.isEmpty()) {
                JSONObject columnClauseObj = (JSONObject) selectTabObj.get("columnClauseObj");
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

                        hssfCell.setCellValue(String.valueOf(columnsList.get(i)));
                    }
                    lastRowIndex++;
                } else {
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
                            if (sourceColumnName != null
                                    && !"".equalsIgnoreCase(sourceColumnName)
                                    && !"null".equalsIgnoreCase(sourceColumnName)) {
                                String dateFormat = "";
                                if (columnClauseObj != null
                                        && !columnClauseObj.isEmpty()
                                        && columnClauseObj.containsKey(sourceColumnName)) {
                                    dateFormat = String.valueOf(columnClauseObj.get(sourceColumnName));
                                }
                                XSSFCell hSSFCell = hssfRow.createCell(cellIdx++);
//                                    JSONObject mappedColumnObj = (JSONObject) columnsObj.get(sourceColumnName);
                                // JSONObject defaultValObj = (JSONObject) defaultValuesObj.get(sourceColumnName);
                                if (columnsObj.get(sourceColumnName) != null) {
                                    try {
                                        //SET default values
                                        if (!(dataObj.get(columnsObj.get(sourceColumnName)) != null
                                                && !"".equalsIgnoreCase(String.valueOf(dataObj.get(columnsObj.get(sourceColumnName))))
                                                && !"null".equalsIgnoreCase(String.valueOf(dataObj.get(columnsObj.get(sourceColumnName)))))) {

                                            if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
                                                    && defaultValuesObj.get(sourceColumnName) != null
                                                    && !"".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))
                                                    && !"null".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))) {
                                                hSSFCell.setCellValue(String.valueOf(defaultValuesObj.get(sourceColumnName)));

                                            } else {
                                                hSSFCell.setCellValue("");
                                            }

                                        } else if ((dataObj.get(columnsObj.get(sourceColumnName)) != null
                                                && !"".equalsIgnoreCase(String.valueOf(dataObj.get(columnsObj.get(sourceColumnName))))
                                                && !"null".equalsIgnoreCase(String.valueOf(dataObj.get(columnsObj.get(sourceColumnName)))))) {
                                            hSSFCell.setCellValue(String.valueOf(dataObj.get(columnsObj.get(sourceColumnName))));
                                        } else {
                                            hSSFCell.setCellValue(String.valueOf(dataObj.get(sourceColumnName)));

                                        }

                                    } catch (Exception e) {
                                        hSSFCell.setCellValue("");
                                        continue;
                                    }

                                } else {
                                    if (dataObj.get(sourceColumnName) != null
                                            && !"".equalsIgnoreCase(String.valueOf(dataObj.get(sourceColumnName)))
                                            && !"null".equalsIgnoreCase(String.valueOf(dataObj.get(sourceColumnName)))) {
                                        hSSFCell.setCellValue(String.valueOf(dataObj.get(sourceColumnName)));
                                    } else if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
                                            && defaultValuesObj.get(sourceColumnName) != null
                                            && !"".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))
                                            && !"null".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))) {
                                        hSSFCell.setCellValue(String.valueOf(defaultValuesObj.get(sourceColumnName)));
                                    } else if (columnClauseObj != null
                                            && !columnClauseObj.isEmpty()
                                            && columnClauseObj.containsKey(sourceColumnName)) {
                                        hSSFCell.setCellValue(String.valueOf(dataPipingUtills.convertIntoDBValue(dateFormat, sourceColumnName, dateFormat)));
                                        //importStmt.setObject(stmtIndex, dataPipingUtills.convertIntoDBValue(columnType, dateFormat, sourceColumnName, dateFormat));
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
            //JSONObject destColumnsObj,
            Map destColumnsObj,// ravi etl integration
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            List totalData,
            PreparedStatement importStmt,
            List<String> columnsList,
            JSONObject defaultValuesObj,
            String filePath,
            String fileName,
            int lastRowIndex,
            HSSFSheet sheet,
            HSSFWorkbook wb,
            JSONObject selectTabObj
    ) {
        int insertCount = 0;
        try {
            if (totalData != null && !totalData.isEmpty() && columnsObj != null && !columnsObj.isEmpty()) {
                JSONObject columnClauseObj = (JSONObject) selectTabObj.get("columnClauseObj");
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

                        hssfCell.setCellValue(String.valueOf(columnsList.get(i)));
                    }
                    lastRowIndex++;
                } else {
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
                            if (sourceColumnName != null
                                    && !"".equalsIgnoreCase(sourceColumnName)
                                    && !"null".equalsIgnoreCase(sourceColumnName)) {
                                String dateFormat = "";
                                if (columnClauseObj != null
                                        && !columnClauseObj.isEmpty()
                                        && columnClauseObj.containsKey(sourceColumnName)) {
                                    dateFormat = String.valueOf(columnClauseObj.get(sourceColumnName));
                                }
                                HSSFCell hSSFCell = hssfRow.createCell(cellIdx++);
//                                    JSONObject mappedColumnObj = (JSONObject) columnsObj.get(sourceColumnName);
                                // JSONObject defaultValObj = (JSONObject) defaultValuesObj.get(sourceColumnName);
                                if (columnsObj.get(sourceColumnName) != null) {
                                    try {
                                        //SET default values
                                        if (!(dataObj.get(columnsObj.get(sourceColumnName)) != null
                                                && !"".equalsIgnoreCase(String.valueOf(dataObj.get(columnsObj.get(sourceColumnName))))
                                                && !"null".equalsIgnoreCase(String.valueOf(dataObj.get(columnsObj.get(sourceColumnName)))))) {
                                            if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
                                                    && defaultValuesObj.get(sourceColumnName) != null
                                                    && !"".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))
                                                    && !"null".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))) {
                                                hSSFCell.setCellValue(String.valueOf(defaultValuesObj.get(sourceColumnName)));

                                            } else {
                                                hSSFCell.setCellValue("");
                                            }

                                        } else if ((dataObj.get(columnsObj.get(sourceColumnName)) != null
                                                && !"".equalsIgnoreCase(String.valueOf(dataObj.get(columnsObj.get(sourceColumnName))))
                                                && !"null".equalsIgnoreCase(String.valueOf(dataObj.get(columnsObj.get(sourceColumnName)))))) {
                                            hSSFCell.setCellValue(String.valueOf(dataObj.get(columnsObj.get(sourceColumnName))));
                                        } else {
                                            hSSFCell.setCellValue(String.valueOf(dataObj.get(sourceColumnName)));

                                        }

                                    } catch (Exception e) {
                                        hSSFCell.setCellValue("");
                                        continue;
                                    }

                                } else {
                                    if (dataObj.get(sourceColumnName) != null
                                            && !"".equalsIgnoreCase(String.valueOf(dataObj.get(sourceColumnName)))
                                            && !"null".equalsIgnoreCase(String.valueOf(dataObj.get(sourceColumnName)))) {
                                        hSSFCell.setCellValue(String.valueOf(dataObj.get(sourceColumnName)));
                                    } else if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
                                            && defaultValuesObj.get(sourceColumnName) != null
                                            && !"".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))
                                            && !"null".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))) {
                                        hSSFCell.setCellValue(String.valueOf(defaultValuesObj.get(sourceColumnName)));
                                    } else if (columnClauseObj != null
                                            && !columnClauseObj.isEmpty()
                                            && columnClauseObj.containsKey(sourceColumnName)) {
                                        hSSFCell.setCellValue(String.valueOf(dataPipingUtills.convertIntoDBValue(dateFormat, sourceColumnName, dateFormat)));
                                        //importStmt.setObject(stmtIndex, dataPipingUtills.convertIntoDBValue(columnType, dateFormat, sourceColumnName, dateFormat));
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
            //JSONObject destColumnsObj,
            Map destColumnsObj,// ravi etl integration
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            List totalData,
            PreparedStatement importStmt,
            List<String> columnsList,
            JSONObject defaultValuesObj,
            String filePath,
            String fileName,
            int lastRowIndex,
            JSONObject selectTabObj
    ) {
        int insertCount = 0;
        try {
            if (totalData != null && !totalData.isEmpty() && columnsObj != null && !columnsObj.isEmpty()) {
                JSONObject columnClauseObj = (JSONObject) selectTabObj.get("columnClauseObj");
                List<String[]> writeFileDataList = new ArrayList<>();
                File file12 = new File(filePath);

                if (!file12.exists()) {
                    file12.mkdirs();
                }
                int cellIdx = 0;
                File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                if (lastRowIndex == 0) {
                    String columnsString = StringUtils.collectionToDelimitedString(columnsList, ":::");
//                    String columnsString = columnsList.stream()
//                            .map(columnName -> (String.valueOf(columnName).split(":")[1]))
//                            .collect(Collectors.joining(":::"));
                    writeFileDataList.add(columnsString.split(":::"));
                    lastRowIndex++;
                } else {
                    lastRowIndex++;
                }

                for (int i = 0; i < totalData.size(); i++) {
                    Map dataObj = (Map) totalData.get(i);
                    String dataString = "";
                    if (dataObj != null && !dataObj.isEmpty()) {
                        for (int j = 0; j < columnsList.size(); j++) {// columns data
                            String cellvalue = "";
                            String sourceColumnName = columnsList.get(j);
                            if (sourceColumnName != null
                                    && !"".equalsIgnoreCase(sourceColumnName)
                                    && !"null".equalsIgnoreCase(sourceColumnName)) {
                                String dateFormat = "";
                                if (columnClauseObj != null
                                        && !columnClauseObj.isEmpty()
                                        && columnClauseObj.containsKey(sourceColumnName)) {
                                    dateFormat = String.valueOf(columnClauseObj.get(sourceColumnName));
                                }
                                if (columnsObj.get(sourceColumnName) != null) {

                                    try {
                                        //SET default values
                                        if (!(dataObj.get(columnsObj.get(sourceColumnName)) != null
                                                && !"".equalsIgnoreCase(String.valueOf(dataObj.get(columnsObj.get(sourceColumnName))))
                                                && !"null".equalsIgnoreCase(String.valueOf(dataObj.get(columnsObj.get(sourceColumnName)))))) {
                                            if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
                                                    && defaultValuesObj.get(sourceColumnName) != null
                                                    && !"".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))
                                                    && !"null".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))) {
                                                cellvalue = String.valueOf(defaultValuesObj.get(sourceColumnName));

                                            } else {
                                                cellvalue = "";
                                            }

                                        } else if ((dataObj.get(columnsObj.get(sourceColumnName)) != null
                                                && !"".equalsIgnoreCase(String.valueOf(dataObj.get(columnsObj.get(sourceColumnName))))
                                                && !"null".equalsIgnoreCase(String.valueOf(dataObj.get(columnsObj.get(sourceColumnName)))))) {
                                            cellvalue = String.valueOf(dataObj.get(columnsObj.get(sourceColumnName)));
                                        } else {
                                            cellvalue = String.valueOf(dataObj.get(sourceColumnName));

                                        }

                                    } catch (Exception e) {
                                        cellvalue = "";
                                        continue;
                                    }

                                } else {
                                    if (dataObj.get(sourceColumnName) != null
                                            && !"".equalsIgnoreCase(String.valueOf(dataObj.get(sourceColumnName)))
                                            && !"null".equalsIgnoreCase(String.valueOf(dataObj.get(sourceColumnName)))) {
                                        cellvalue = String.valueOf(dataObj.get(sourceColumnName));
                                    } else if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
                                            && defaultValuesObj.get(sourceColumnName) != null
                                            && !"".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))
                                            && !"null".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))) {
                                        cellvalue = String.valueOf(defaultValuesObj.get(sourceColumnName));

                                    } else if (columnClauseObj != null
                                            && !columnClauseObj.isEmpty()
                                            && columnClauseObj.containsKey(sourceColumnName)) {
                                        cellvalue = String.valueOf(dataPipingUtills.convertIntoDBValue(dateFormat, sourceColumnName, dateFormat));
                                        //importStmt.setObject(stmtIndex, dataPipingUtills.convertIntoDBValue(columnType, dateFormat, sourceColumnName, dateFormat));
                                    } else {
                                        cellvalue = "";
                                    }

                                }
                            }
                            if (cellvalue != null
                                    && !"".equalsIgnoreCase(cellvalue)
                                    && !"null".equalsIgnoreCase(cellvalue)) {
                                cellvalue = cellvalue.replaceAll("Â", "");
                            }

                            dataString += cellvalue;
                            if (j != columnsList.size() - 1) {
                                dataString += ":::";
                            }
                        }// end of columns loop
                        lastRowIndex++;
                        if (dataString != null && !"".equalsIgnoreCase(dataString)) {
                            writeFileDataList.add(dataString.split(":::"));
                            dataString = "";
                        }

                    }
                    insertCount++;
                }// end of data loop

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

    public int exportingJSONFileData(Map toOperator,
            //JSONObject destColumnsObj,
            Map destColumnsObj,// ravi etl integration
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            List totalData,
            PreparedStatement importStmt,
            List<String> columnsList,
            JSONObject defaultValuesObj,
            String filePath,
            String fileName,
            int lastRowIndex,
            JSONObject selectTabObj
    ) {
        int insertCount = 0;
        try {
            if (totalData != null && !totalData.isEmpty() && columnsObj != null && !columnsObj.isEmpty()) {
                JSONObject columnClauseObj = (JSONObject) selectTabObj.get("columnClauseObj");
                List<String[]> writeFileDataList = new ArrayList<>();
                File file12 = new File(filePath);

                if (!file12.exists()) {
                    file12.mkdirs();
                }
                int cellIdx = 0;
                File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
//                if (lastRowIndex == 0) {
//                    String columnsString = columnsList.stream()
//                            .map(columnName -> (String.valueOf(columnName).split(":")[1]))
//                            .collect(Collectors.joining(":::"));
//                    writeFileDataList.add(columnsString.split(":::"));
//                    lastRowIndex++;
//                } else {
//                    lastRowIndex++;
//                }
                String jsonDataStr = "";
                JSONArray totalDataArray = new JSONArray();
                if (lastRowIndex == 0) {
                    jsonDataStr += "[";
//                    Object existingData = JSONValue.parse(new FileReader(outputFile));
//                    if (existingData != null) {
//                        totalDataArray = (JSONArray) existingData;
//                    }

                } else {
                    jsonDataStr += ",";
                }
                for (int i = 0; i < totalData.size(); i++) {
                    Map dataObj = (Map) totalData.get(i);
                    String dataString = "";
                    if (dataObj != null && !dataObj.isEmpty()) {
                        JSONObject jsonDataObj = new JSONObject();
                        for (int j = 0; j < columnsList.size(); j++) {// columns data
                            String cellvalue = "";
                            String sourceColumnName = columnsList.get(j);
                            if (sourceColumnName != null
                                    && !"".equalsIgnoreCase(sourceColumnName)
                                    && !"null".equalsIgnoreCase(sourceColumnName)) {
                                String dateFormat = "";
                                if (columnClauseObj != null
                                        && !columnClauseObj.isEmpty()
                                        && columnClauseObj.containsKey(sourceColumnName)) {
                                    dateFormat = String.valueOf(columnClauseObj.get(sourceColumnName));
                                }
                                if (columnsObj.get(sourceColumnName) != null) {
                                    try {
                                        //SET default values
                                        if (!(dataObj.get(columnsObj.get(sourceColumnName)) != null
                                                && !"".equalsIgnoreCase(String.valueOf(dataObj.get(columnsObj.get(sourceColumnName))))
                                                && !"null".equalsIgnoreCase(String.valueOf(dataObj.get(columnsObj.get(sourceColumnName)))))) {
                                            if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
                                                    && defaultValuesObj.get(sourceColumnName) != null
                                                    && !"".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))
                                                    && !"null".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))) {
                                                cellvalue = String.valueOf(defaultValuesObj.get(sourceColumnName));

                                            } else {
                                                cellvalue = "";
                                            }

                                        } else if ((dataObj.get(columnsObj.get(sourceColumnName)) != null
                                                && !"".equalsIgnoreCase(String.valueOf(dataObj.get(columnsObj.get(sourceColumnName))))
                                                && !"null".equalsIgnoreCase(String.valueOf(dataObj.get(columnsObj.get(sourceColumnName)))))) {
                                            cellvalue = String.valueOf(dataObj.get(columnsObj.get(sourceColumnName)));
                                        } else {
                                            cellvalue = String.valueOf(dataObj.get(sourceColumnName));

                                        }

                                    } catch (Exception e) {
                                        cellvalue = "";
                                        continue;
                                    }

                                } else {
                                    if (dataObj.get(sourceColumnName) != null
                                            && !"".equalsIgnoreCase(String.valueOf(dataObj.get(sourceColumnName)))
                                            && !"null".equalsIgnoreCase(String.valueOf(dataObj.get(sourceColumnName)))) {
                                        cellvalue = String.valueOf(dataObj.get(sourceColumnName));
                                    } else if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
                                            && defaultValuesObj.get(sourceColumnName) != null
                                            && !"".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))
                                            && !"null".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))) {
                                        cellvalue = String.valueOf(defaultValuesObj.get(sourceColumnName));

                                    } else if (columnClauseObj != null
                                            && !columnClauseObj.isEmpty()
                                            && columnClauseObj.containsKey(sourceColumnName)) {
                                        cellvalue = String.valueOf(dataPipingUtills.convertIntoDBValue(dateFormat, sourceColumnName, dateFormat));
                                        //importStmt.setObject(stmtIndex, dataPipingUtills.convertIntoDBValue(columnType, dateFormat, sourceColumnName, dateFormat));
                                    } else {
                                        cellvalue = "";
                                    }

                                }
                            }
                            if (cellvalue != null
                                    && !"".equalsIgnoreCase(cellvalue)
                                    && !"null".equalsIgnoreCase(cellvalue)) {
                                cellvalue = cellvalue.replaceAll("Â", "");
                            }
                            jsonDataObj.put(sourceColumnName, cellvalue);
                            //
                        }// end of columns loop
                        lastRowIndex++;
                        jsonDataStr += jsonDataObj.toJSONString();
                        if (i != totalData.size() - 1) {
                            jsonDataStr += ",";
                        }

                    }
                    insertCount++;
                }// end of data loop

                FileOutputStream fos = new FileOutputStream(outputFile, true);
                OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
                BufferedWriter writer = new BufferedWriter(osw);
                writer.append(jsonDataStr);
                writer.close();
                osw.close();
                fos.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return insertCount;
    }

    public int exportingXMLFileData(Map toOperator,
            //JSONObject destColumnsObj,
            Map destColumnsObj,// ravi etl integration
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            List totalData,
            PreparedStatement importStmt,
            List<String> columnsList,
            JSONObject defaultValuesObj,
            String filePath,
            String fileName,
            int lastRowIndex,
            JSONObject selectTabObj
    ) {
        int insertCount = 0;
        try {
            if (totalData != null && !totalData.isEmpty() && columnsObj != null && !columnsObj.isEmpty()) {
                JSONObject columnClauseObj = (JSONObject) selectTabObj.get("columnClauseObj");
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
                                Element sourceColumnNameElement = document.createElement(sourceColumnName.replaceAll(":", "_").replaceAll(" ", "_"));
                                String cellvalue = "";
                                if (sourceColumnName != null
                                        && !"".equalsIgnoreCase(sourceColumnName)
                                        && !"null".equalsIgnoreCase(sourceColumnName)) {
                                    String dateFormat = "";
                                    if (columnClauseObj != null
                                            && !columnClauseObj.isEmpty()
                                            && columnClauseObj.containsKey(sourceColumnName)) {
                                        dateFormat = String.valueOf(columnClauseObj.get(sourceColumnName));
                                    }
                                    if (columnsObj.get(sourceColumnName) != null) {
                                        try {
                                            //SET default values
                                            if (!(dataObj.get(columnsObj.get(sourceColumnName)) != null
                                                    && !"".equalsIgnoreCase(String.valueOf(dataObj.get(columnsObj.get(sourceColumnName))))
                                                    && !"null".equalsIgnoreCase(String.valueOf(dataObj.get(columnsObj.get(sourceColumnName)))))) {
                                                if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
                                                        && defaultValuesObj.get(sourceColumnName) != null
                                                        && !"".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))
                                                        && !"null".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))) {
                                                    cellvalue = String.valueOf(defaultValuesObj.get(sourceColumnName));

                                                } else {
                                                    cellvalue = "";
                                                }

                                            } else if ((dataObj.get(columnsObj.get(sourceColumnName)) != null
                                                    && !"".equalsIgnoreCase(String.valueOf(dataObj.get(columnsObj.get(sourceColumnName))))
                                                    && !"null".equalsIgnoreCase(String.valueOf(dataObj.get(columnsObj.get(sourceColumnName)))))) {
                                                cellvalue = String.valueOf(dataObj.get(columnsObj.get(sourceColumnName)));
                                            } else {
                                                cellvalue = String.valueOf(dataObj.get(sourceColumnName));

                                            }

                                        } catch (Exception e) {
                                            cellvalue = "";
                                            continue;
                                        }

                                    } else {
                                        if (dataObj.get(sourceColumnName) != null
                                                && !"".equalsIgnoreCase(String.valueOf(dataObj.get(sourceColumnName)))
                                                && !"null".equalsIgnoreCase(String.valueOf(dataObj.get(sourceColumnName)))) {
                                            cellvalue = String.valueOf(dataObj.get(sourceColumnName));
                                        } else if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
                                                && defaultValuesObj.get(sourceColumnName) != null
                                                && !"".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))
                                                && !"null".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))) {
                                            cellvalue = String.valueOf(defaultValuesObj.get(sourceColumnName));

                                        } else if (columnClauseObj != null
                                                && !columnClauseObj.isEmpty()
                                                && columnClauseObj.containsKey(sourceColumnName)) {
                                            cellvalue = String.valueOf(dataPipingUtills.convertIntoDBValue(dateFormat, sourceColumnName, dateFormat));
                                            //importStmt.setObject(stmtIndex, dataPipingUtills.convertIntoDBValue(columnType, dateFormat, sourceColumnName, dateFormat));
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
                                    //  cellvalue = cellvalue.replaceAll("\\s+", " ");

                                }
                                sourceColumnNameElement.appendChild(document.createTextNode(escape(cellvalue)));
                                itemElement.appendChild(sourceColumnNameElement);

                            }// end of columns loop
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
                                if (sourceColumnName != null
                                        && !"".equalsIgnoreCase(sourceColumnName)
                                        && !"null".equalsIgnoreCase(sourceColumnName)) {
                                    String dateFormat = "";
                                    if (columnClauseObj != null
                                            && !columnClauseObj.isEmpty()
                                            && columnClauseObj.containsKey(sourceColumnName)) {
                                        dateFormat = String.valueOf(columnClauseObj.get(sourceColumnName));
                                    }
                                    if (columnsObj.get(sourceColumnName) != null) {
                                        try {
                                            //SET default values
                                            if (!(dataObj.get(columnsObj.get(sourceColumnName)) != null
                                                    && !"".equalsIgnoreCase(String.valueOf(dataObj.get(columnsObj.get(sourceColumnName))))
                                                    && !"null".equalsIgnoreCase(String.valueOf(dataObj.get(columnsObj.get(sourceColumnName)))))) {
                                                if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
                                                        && defaultValuesObj.get(sourceColumnName) != null
                                                        && !"".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))
                                                        && !"null".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))) {
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
                                        if (dataObj.get(sourceColumnName) != null
                                                && !"".equalsIgnoreCase(String.valueOf(dataObj.get(sourceColumnName)))
                                                && !"null".equalsIgnoreCase(String.valueOf(dataObj.get(sourceColumnName)))) {
                                            cellvalue = String.valueOf(dataObj.get(sourceColumnName));
                                        } else if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
                                                && defaultValuesObj.get(sourceColumnName) != null
                                                && !"".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))
                                                && !"null".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))) {
                                            cellvalue = String.valueOf(defaultValuesObj.get(sourceColumnName));
                                        } else if (columnClauseObj != null
                                                && !columnClauseObj.isEmpty()
                                                && columnClauseObj.containsKey(sourceColumnName)) {
                                            cellvalue = String.valueOf(dataPipingUtills.convertIntoDBValue(dateFormat, sourceColumnName, dateFormat));
                                            //importStmt.setObject(stmtIndex, dataPipingUtills.convertIntoDBValue(columnType, dateFormat, sourceColumnName, dateFormat));
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
                                    //  cellvalue = cellvalue.replaceAll("\\s+", " ");

                                }
                                xmlString += "<" + sourceColumnName.replaceAll(":", "_").replaceAll(" ", "_") + ">"
                                        + "" + escape(cellvalue) + ""
                                        + "</" + sourceColumnName.replaceAll(":", "_").replaceAll(" ", "_") + ">\n";

                            }// end of columns loop
                            lastRowIndex++;
                            xmlString += "</Item>\n";

                        }
                        insertCount++;
                    }// end of data loop
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

    public Map getColumnsType(String tableName, List<String> columnsList, Connection connection) {
        Map columnsTypeObj = new HashMap();
        PreparedStatement colTypStmt = null;
        ResultSet resultSet = null;
        try {
            String selectQuery = "SELECT " + StringUtils.collectionToCommaDelimitedString(columnsList) + " FROM " + tableName + " WHERE 1=2";
            colTypStmt = connection.prepareStatement(selectQuery);
            resultSet = colTypStmt.executeQuery();
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columncount = resultSetMetaData.getColumnCount();
            for (int i = 0; i < columncount; i++) {
                String columnName = resultSetMetaData.getColumnName(i + 1);
                String columnType = resultSetMetaData.getColumnTypeName(i + 1);
                columnsTypeObj.put(columnName, columnType);
//                System.out.println("columnName:::" + columnName + "::::columnType::::" + columnType);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (colTypStmt != null) {
                    colTypStmt.close();
                }
            } catch (Exception e) {
            }
        }
        return columnsTypeObj;
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

//    public void processETLLog(String sessionUserName, 
//            String orgnId, 
//            String logTxt, 
//            String logType, 
//            int sequenceNo, 
//            String processFlag
//    ) {
//        processETLLog(sessionUserName, orgnId, logTxt, logType, sequenceNo, processFlag, AuditIdGenerator.genRandom32Hex());
//        try {
//           
////            System.out.println("insert Log Count " + insertCount);
//        } catch (Exception e) {
//            e.printStackTrace();
//        } 
//    }
    public void processETLLog(String sessionUserName,
            String orgnId,
            String logTxt,
            String logType,
            int sequenceNo,
            String processFlag,
            String jobId
    ) {
        Connection logConnection = null;
        PreparedStatement logStmt = null;
        try {
            if (logTxt != null
                    && !"".equalsIgnoreCase(logTxt)
                    && !"null".equalsIgnoreCase(logTxt)
                    && logTxt.length() > 3000) {
                logTxt = logTxt.substring(0, 3000);
            }
            int lastSequenceNo = genericDataPipingDAO.getLastLogSequenceNo(sessionUserName, orgnId);
            lastSequenceNo++;
            String insertQuery = "INSERT INTO RECORD_DM_PROCESS_LOG (SEQUENCE_NO, USER_NAME, ORGN_ID, LOG_TXT,LOG_TYPE,PROCESS_FLAG,JOB_ID)"
                    + " VALUES (?,?,?,?,?,?,?)";
            Class.forName(dataBaseDriver);
            logConnection = DriverManager.getConnection(dbURL, userName, password);
            logStmt = logConnection.prepareStatement(insertQuery);
            logStmt.setObject(1, lastSequenceNo);//SEQUENCE_NO
            logStmt.setObject(2, sessionUserName);//USER_NAME
            logStmt.setObject(3, orgnId);//ORGN_ID
            logStmt.setObject(4, logTxt);//LOG_TXT
            logStmt.setObject(5, logType);//LOG_TYPE
            logStmt.setObject(6, processFlag);//PROCESS_FLAG
            logStmt.setObject(7, jobId);//JOB_ID
            int insertCount = logStmt.executeUpdate();
//            System.out.println("insert Log Count " + insertCount);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (logStmt != null) {
                    logStmt.close();
                }
                if (logConnection != null) {
                    logConnection.close();
                }
            } catch (Exception e) {

            }
        }
    }
    
    
    public String refreshOperatorProcessStatus(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        JSONArray insertCountObjectsArray = new JSONArray();
        JSONArray updateCountObjectsArray = new JSONArray();
        JSONArray fromTableCountObjectsArray = new JSONArray();

       List statusOpArray =  new ArrayList();
        try {

            String jobId = request.getParameter("jobId");

            List<Object[]> processlogDataList = genericDataPipingDAO.getOperatorProcessStatus((String) request.getSession(false).getAttribute("ssUsername"),
                    (String) request.getSession(false).getAttribute("ssOrgId"), null, jobId);
            List processingOperators =  new ArrayList();
            List processedOperators =  new ArrayList();
            List failedOperators =  new ArrayList();
            
             for (int i=0; i<processlogDataList.size();i++) {
            	 Object [] rowData = (Object [])processlogDataList.get(i);
            	 String processFlag = String.valueOf(rowData[1]);
        		 if ("N".equalsIgnoreCase(processFlag)) {
        			 resultObj.put("processFlag", processFlag);
        			// processingOperators = null;
        		 }
         
            	 if ("Y".equalsIgnoreCase(processFlag)) {
            		 String stepType =  (String)rowData[2];
            		 if (stepType.equalsIgnoreCase("START")) {
            			 processingOperators.add(rowData[0]);
            		 } else if (stepType.equalsIgnoreCase("END")) {
            			 String opStatus = String.valueOf(rowData[3]);
            			 if (opStatus.equalsIgnoreCase("FAIL")) {
            				 failedOperators.add(rowData[0]);
            			 } else {
            				 processedOperators.add(rowData[0]);
            			 }
            			 
            		 }
            		
            	 }
             }
            resultObj.put("processingOperators", processingOperators);
            resultObj.put("processedOperators", processedOperators);
            resultObj.put("failedOperators", failedOperators);
           

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj.toJSONString();
    }
    
    
    public String getProcesslog(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        JSONArray insertCountObjectsArray = new JSONArray();
        JSONArray updateCountObjectsArray = new JSONArray();
        JSONArray fromTableCountObjectsArray = new JSONArray();

        String logText = "";
        try {
            String currentProcesslogDate = request.getParameter("currentProcesslogDate");
            String currentProcesslogIndex = request.getParameter("currentProcesslogIndex");
            String jobId = request.getParameter("jobId");

            List<Object[]> processlogDataList = genericDataPipingDAO.getProcesslog((String) request.getSession(false).getAttribute("ssUsername"),
                    (String) request.getSession(false).getAttribute("ssOrgId"), currentProcesslogIndex, jobId);
            if (processlogDataList != null && !processlogDataList.isEmpty()) {
                for (int i = 0; i < processlogDataList.size(); i++) {
                    Object[] processlogDataArray = processlogDataList.get(i);
                    if (processlogDataArray != null && processlogDataArray.length != 0) {
                        String imagePath = "images/information.gif";//erroricon.png
                        String contentClass = "visionETLLogInfo";
                        if ("ERROR".equalsIgnoreCase(String.valueOf(processlogDataArray[0]))) {
                            imagePath = "images/erroricon.png";
                            contentClass = "visionETLLogError";
                        } else if ("WARNING".equalsIgnoreCase(String.valueOf(processlogDataArray[0]))) {
                            imagePath = "images/44warning.png";
                            contentClass = "visionETLLogWarning";
                        }
                        logText += "<tr>"//44warning.png
                                + "<td width='5%'><img src='" + imagePath + "' style='width:16px;height:16px;padding:2px'></td>"
                                + "<td width='25%'>" + processlogDataArray[2] + "</td>"
                                + "<td width='70%' class='" + contentClass + "'>" + processlogDataArray[1] + "</td>"
                                + "</tr>";

                        if (processlogDataArray[5] != null
                                && !"".equalsIgnoreCase(String.valueOf(processlogDataArray[5]))) {
                            insertCountObjectsArray.add(processlogDataArray[5]);
                        }
                        if (processlogDataArray[6] != null
                                && !"".equalsIgnoreCase(String.valueOf(processlogDataArray[6]))) {
                            updateCountObjectsArray.add(processlogDataArray[6]);
                        }
                        if (processlogDataArray[7] != null
                                && !"".equalsIgnoreCase(String.valueOf(processlogDataArray[7]))) {
                            fromTableCountObjectsArray.add(processlogDataArray[7]);
                        }

                        if (i == processlogDataList.size() - 1) {
                            resultObj.put("processFlag", processlogDataArray[3]);
//                            if (processlogDataArray[2] != null 
//                                    && !"".equalsIgnoreCase(String.valueOf(processlogDataArray[2])) 
//                                    && !"null".equalsIgnoreCase(String.valueOf(processlogDataArray[2]))
//                                    && String.valueOf(processlogDataArray[2]).contains(".")) {//2020-05-26 18:55:39.0                               
//                                currentProcesslogDate = String.valueOf(processlogDataArray[2]).split("[.]")[0];
//                                 resultObj.put("processFlag", processlogDataArray[3]);  
//                            }
                            if (processlogDataArray[4] != null
                                    && !"".equalsIgnoreCase(String.valueOf(processlogDataArray[4]))
                                    && !"null".equalsIgnoreCase(String.valueOf(processlogDataArray[4]))) {//2020-05-26 18:55:39.0                               
                                currentProcesslogIndex = String.valueOf(new PilogUtilities().convertIntoInteger(processlogDataArray[4]));

                            }

                        }
                        resultObj.put("currentDate", processlogDataArray[2].toString());
                    }

                }
//                System.out.println("currentProcesslogIndex:::" + currentProcesslogIndex);
                resultObj.put("logTxt", logText);

                resultObj.put("insertCountObjectsArray", insertCountObjectsArray);
                resultObj.put("updateCountObjectsArray", updateCountObjectsArray);
                resultObj.put("fromTableCountObjectsArray", fromTableCountObjectsArray);
            } else {
                resultObj.put("currentDate", genericDataPipingDAO.getCurrentDBDate().toString());
            }
            resultObj.put("currentProcesslogDate", currentProcesslogDate);
            resultObj.put("currentProcesslogIndex", currentProcesslogIndex);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj.toJSONString();
    }

    public String getnthNumber(int loopCount) {
        String result = "";
        try {
            String number = "" + loopCount;
            if (number != null && number.length() > 1) {
                number = "" + number.charAt(number.length() - 1);
            }
            switch (number) {
                case "1":
                    result = "" + loopCount + "<sup>st</sup>";
                    break;
                case "2":
                    result = "" + loopCount + "<sup>nd</sup>";
                    break;
                case "3":
                    result = "" + loopCount + "<sup>rd</sup>";
                    break;
                default:
                    result = "" + loopCount + "<sup>th</sup>";
            }
        } catch (Exception e) {
        }
        return result;
    }

    public JSONObject getSelectedSAPTablesData(HttpSession httpSession,
            JCO.Client fromJCOConnection,
            List<Map> fromOperatorList,
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            //JSONObject fromColumnsObj,
            Map fromColumnsObj, // ravi etl integration
            JSONObject tablesWhereClauseObj,
            JSONObject defaultValuesObj,
            JSONObject orderByObj,
            String groupByQuery,
            JSONObject joinQueryMapObj
    ) {
        int totalInsertCount = 0;
        Connection currentV10Conn = null;
        PreparedStatement currentV10Stmt = null;
        JSONObject modifiedFromColumnsObj = new JSONObject();
        JSONObject resultMapObj = new JSONObject();
        try {
            JSONObject tableNameObj = new JSONObject();
            if (fromColumnsObj != null && !fromColumnsObj.isEmpty()) {
                if (joinQueryMapObj != null && !joinQueryMapObj.isEmpty()) {
                    for (Object joinTableName : joinQueryMapObj.keySet()) {
                        Object joinTableData = joinQueryMapObj.get(joinTableName);
                        if (joinTableData instanceof JSONObject) {
                            JSONObject joinTableDataObj = (JSONObject) joinTableData;
                            if (joinTableDataObj != null && !joinTableDataObj.isEmpty()) {
                                for (Object joinTableDataObjKey : joinTableDataObj.keySet()) {
                                    JSONObject joinChildTableData = (JSONObject) joinTableDataObj.get(joinTableDataObjKey);
                                    if (joinChildTableData != null && !joinChildTableData.isEmpty()) {
                                        String childTableColumn = (String) joinChildTableData.get("childTableColumn");
                                        String masterTableColumn = (String) joinChildTableData.get("masterTableColumn");
                                        if (childTableColumn != null
                                                && !"".equalsIgnoreCase(childTableColumn)
                                                && !"null".equalsIgnoreCase(childTableColumn)) {
                                            String[] childTableColumnArray = childTableColumn.split(":");
                                            if (childTableColumnArray != null
                                                    && childTableColumnArray.length != 0) {
                                                List<String> existingColumns = Arrays.asList(((String) fromColumnsObj.get(childTableColumnArray[0])).split(","));
                                                if (existingColumns != null
                                                        && !existingColumns.isEmpty()
                                                        && !existingColumns.contains(childTableColumnArray[1])) {
                                                    fromColumnsObj.put(childTableColumnArray[0], (fromColumnsObj.get(childTableColumnArray[0]) + "," + childTableColumnArray[1]));
                                                }

                                            }
                                        }
                                        if (masterTableColumn != null
                                                && !"".equalsIgnoreCase(masterTableColumn)
                                                && !"null".equalsIgnoreCase(masterTableColumn)) {
                                            String[] masterTableColumnArray = masterTableColumn.split(":");
                                            if (masterTableColumnArray != null
                                                    && masterTableColumnArray.length != 0) {
                                                List<String> existingColumns = Arrays.asList(((String) fromColumnsObj.get(masterTableColumnArray[0])).split(","));
                                                if (existingColumns != null
                                                        && !existingColumns.isEmpty()
                                                        && !existingColumns.contains(masterTableColumnArray[1])) {
                                                    fromColumnsObj.put(masterTableColumnArray[0], (fromColumnsObj.get(masterTableColumnArray[0]) + "," + masterTableColumnArray[1]));
                                                }

                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Class.forName(dataBaseDriver);
                currentV10Conn = DriverManager.getConnection(dbURL, userName, password);
                for (Object fromSAPTableName : fromColumnsObj.keySet()) {
                    String dbTableName = fromSAPTableName + "_" + System.currentTimeMillis();
                    List<String> columnsList = new ArrayList<>();
                    if (dbTableName != null
                            && !"".equalsIgnoreCase(dbTableName)
                            && !"null".equalsIgnoreCase(dbTableName)) {
                        if (dbTableName.length() > 30) {
                            dbTableName = dbTableName.substring(0, 30);
                        }
                        tableNameObj.put(fromSAPTableName, dbTableName);
                        System.out.println("dbTableName:::" + dbTableName);

                        String columns = (String) fromColumnsObj.get(fromSAPTableName);
                        if (columns != null && !"".equalsIgnoreCase(columns) && !"null".equalsIgnoreCase(columns)) {
                            columnsList = Arrays.asList(columns.split(","));
                            columns = columns.replaceAll(",", " VARCHAR2(4000 CHAR), ");
                            columns += "  VARCHAR2(4000 CHAR)";
                            System.out.println("columns:::" + columns);
                            String createTableQuery = " CREATE TABLE " + dbTableName + " (" + columns + " )";
                            System.out.println("createTableQuery:::" + createTableQuery);
                            currentV10Stmt = currentV10Conn.prepareStatement(createTableQuery);
                            boolean isTableCreated = currentV10Stmt.execute();
                            System.out.println("isTableCreated:::" + isTableCreated);
                            // need to fetch sap data and insert into 
                            modifiedFromColumnsObj.put(dbTableName, fromColumnsObj.get(fromSAPTableName));
                            JSONObject tempFromColumnsObj = new JSONObject();
                            tempFromColumnsObj.put(fromSAPTableName, fromColumnsObj.get(fromSAPTableName));
                            String insertQuery = generateInsertQuery(dbTableName, columnsList);
                            System.out.println("insertQuery::::" + insertQuery);
                            currentV10Stmt = currentV10Conn.prepareStatement(insertQuery);
                            columnsList = columnsList.stream().map(columnName -> (fromSAPTableName + ":" + columnName))
                                    .collect(Collectors.toList());
                            totalInsertCount = importingSAPDataIntoV10(httpSession,
                                    fromJCOConnection,
                                    fromOperatorList,
                                    columnsObj,
                                    tempFromColumnsObj,
                                    tablesWhereClauseObj,
                                    defaultValuesObj,
                                    orderByObj,
                                    groupByQuery,
                                    1,
                                    1000,
                                    1,
                                    totalInsertCount,
                                    currentV10Conn,
                                    currentV10Stmt,
                                    columnsList, (String) fromSAPTableName);

                        }

                    }
                }
            }
            resultMapObj.put("fromColumnsObj", modifiedFromColumnsObj);
            resultMapObj.put("tableNameObj", tableNameObj);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
        return resultMapObj;
    }

    public int importingSAPDataIntoV10(HttpSession httpSession,
            JCO.Client fromJCOConnection,
            List<Map> fromOperatorList,
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            //JSONObject fromColumnsObj,
            Map fromColumnsObj, // ravi etl integration
            JSONObject tablesWhereClauseObj,
            JSONObject defaultValuesObj,
            JSONObject orderByObj,
            String groupByQuery,
            int start,
            int limit,
            int pageNo,
            int totalDataCount,
            Connection currentV10Conn,
            PreparedStatement currentV10Stmt,
            List<String> columnsList,
            String fromSAPTableName
    ) {
        int insertCount = 0;
        try {
            List dataList = dataMigrationAccessDAO.getErpSelectedColumnsData(fromColumnsObj,
                    tablesWhereClauseObj,
                    fromJCOConnection,
                    start,
                    limit);
            if (dataList != null && !dataList.isEmpty()) {
                insertCount = importingData(dataList,
                        currentV10Stmt,
                        columnsList,
                        defaultValuesObj,
                        (String) httpSession.getAttribute("ssUsername"),
                        (String) httpSession.getAttribute("ssOrgId"));
                totalDataCount += insertCount;
                if (insertCount != 0 && insertCount >= 1000) {
                    start = (pageNo * limit + 1);
                    pageNo++;
                    totalDataCount = importingSAPDataIntoV10(httpSession,
                            fromJCOConnection,
                            fromOperatorList,
                            columnsObj,
                            fromColumnsObj,
                            tablesWhereClauseObj,
                            defaultValuesObj,
                            orderByObj,
                            groupByQuery,
                            start,
                            limit,
                            pageNo,
                            totalDataCount,
                            currentV10Conn,
                            currentV10Stmt,
                            columnsList, fromSAPTableName);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return totalDataCount;
    }

    public int importingData(List totalData,
            PreparedStatement importStmt,
            List<String> columnsList,
            JSONObject defaultValuesObj,
            String loginUserName,
            String loginOrgnId) {
        int insertCount = 0;

        try {
            if (totalData != null && !totalData.isEmpty()) {
                insertCount = totalData.size();
                for (int i = 0; i < totalData.size(); i++) {// rows data
                    Map dataObj = (Map) totalData.get(i);
                    int stmtIndex = 1;
                    if (dataObj != null && !dataObj.isEmpty()) {

                        for (int j = 0; j < columnsList.size(); j++) {// columns data
                            String sourceColumnName = columnsList.get(j);
                            if (sourceColumnName != null
                                    && !"".equalsIgnoreCase(sourceColumnName)
                                    && !"null".equalsIgnoreCase(sourceColumnName)) {
//                                    JSONObject mappedColumnObj = (JSONObject) columnsObj.get(sourceColumnName);
                                // JSONObject defaultValObj = (JSONObject) defaultValuesObj.get(sourceColumnName);
                                if (dataObj.get(sourceColumnName) != null) {
                                    try {
                                        //SET default values
                                        if (!(dataObj.get(sourceColumnName) != null
                                                && !"".equalsIgnoreCase(String.valueOf(dataObj.get(sourceColumnName)))
                                                && !"null".equalsIgnoreCase(String.valueOf(dataObj.get(sourceColumnName))))) {
                                            if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
                                                    && defaultValuesObj.get(sourceColumnName) != null
                                                    && !"".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))
                                                    && !"null".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))) {
                                                importStmt.setObject(stmtIndex, defaultValuesObj.get(sourceColumnName));
                                            } else {
                                                importStmt.setObject(stmtIndex, "");
                                            }

                                        } else {
                                            importStmt.setObject(stmtIndex,
                                                    dataObj.get(sourceColumnName));
                                        }

                                    } catch (Exception e) {
                                        importStmt.setObject(stmtIndex, "");
                                        continue;
                                    }
                                    stmtIndex++;
                                } else {
                                    if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
                                            && defaultValuesObj.get(sourceColumnName) != null
                                            && !"".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))
                                            && !"null".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))) {
                                        importStmt.setObject(stmtIndex, defaultValuesObj.get(sourceColumnName));
                                    } else {
                                        importStmt.setObject(stmtIndex, "");
                                    }
                                    stmtIndex++;
                                }
                            }

                        }
                        importStmt.addBatch();
                        if (i != 0 && i % batchSize == 0) {

                            importStmt.executeBatch();
                            importStmt.clearBatch(); //not sure if this is necessary
                            System.out.println("Batch Excuted:::" + i);
                        }
                    }

                }
                importStmt.executeBatch();

            }

        } catch (Exception e) {
            e.printStackTrace();
//            try {
//                processETLLog(loginUserName,
//                        loginOrgnId,
//                        "Faild to load the data into target table due to " + e.getMessage(), "ERROR", 20, "Y");
//            } catch (Exception ex) {
//            }
            insertCount = 0;

        }
        return insertCount;
    }

    public void dropTemptables(JSONObject tableNameObj) {
        Connection dropConnection = null;
        PreparedStatement dropStmt = null;
        try {
            if (tableNameObj != null && !tableNameObj.isEmpty()) {
                Class.forName(dataBaseDriver);
                dropConnection = DriverManager.getConnection(dbURL, userName, password);
                for (Object tableName : tableNameObj.keySet()) {
                    String dropQuery = "DROP TABLE " + tableNameObj.get(tableName) + "  CASCADE CONSTRAINTS ";

                    try {
                        dropStmt = dropConnection.prepareStatement(dropQuery);
                        dropStmt.execute();
                    } catch (Exception e) {
                        continue;
                    }
                }
            }
        } catch (Exception e) {
        } finally {
            try {
                if (dropStmt != null) {
                    dropStmt.close();
                }
                if (dropConnection != null) {
                    dropConnection.close();
                }
            } catch (Exception e) {
            }
        }
    }

    public List readExcel(HttpSession httpSession,
            String filepath,
            List<String> columnList,
            int startIndex,
            int limit,
            int endIndex, String fileName) {

        FileInputStream fis = null;

//        System.out.println("Start Date And Time :::" + new Date());
        List dataList = new ArrayList();
        int rowVal = 1;
        try {
            if (true) {
                //  fis = new FileInputStream(new File(filepath));

                Workbook workBook = null;
                Sheet sheet = null;

                String fileExtension = filepath.substring(filepath.lastIndexOf(".") + 1, filepath.length());
//                System.out.println("fileExtension:::" + fileExtension);
                if (fileExtension != null && "xls".equalsIgnoreCase(fileExtension)) {
                    workBook = WorkbookFactory.create(new File(filepath));
                    sheet = (HSSFSheet) workBook.getSheetAt(0);
                } else {
//                    System.out.println("Before::::" + new Date());
                    workBook = WorkbookFactory.create(new File(filepath));
//                    System.out.println("After::fileInputStream::" + new Date());
                    sheet = (XSSFSheet) workBook.getSheetAt(0);
                }
                int lastRowNo = sheet.getLastRowNum();
//                System.out.println("lastRowNo::::" + lastRowNo);
                int firstRowNo = sheet.getFirstRowNum();
//                System.out.println("firstRowNo::::" + firstRowNo);
                int rowCount = lastRowNo - firstRowNo;
//                System.out.println("rowCount:::::" + rowCount);

                String strToDateCol = "";

                rowVal = startIndex;
                if (rowVal == 0) {
                    rowVal = 1;

                }
                for (int i = rowVal; i <= endIndex; i++) {
                    Row row = sheet.getRow(i);

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
                                        if (cellValue != null && !"".equalsIgnoreCase(cellValue) && !"null".equalsIgnoreCase(cellValue)) {
                                            dataObject.put(fileName + ":" + columnList.get(cellIndex), cellValue);
                                        } else {
                                            dataObject.put(fileName + ":" + columnList.get(cellIndex), "");
                                        }

                                        break;
                                    case Cell.CELL_TYPE_BOOLEAN:
//                                rowObj.put(header, hSSFCell.getBooleanCellValue());
                                        break;
                                    case Cell.CELL_TYPE_NUMERIC:

                                        if (HSSFDateUtil.isCellDateFormatted(cell)) {
                                            String cellDateString = "";
                                            Date cellDate = cell.getDateCellValue();
                                            if ((cellDate.getYear() + 1900) == 1899 && (cellDate.getMonth() + 1) == 12 && (cellDate.getDate()) == 31) {
                                                cellDateString = (cellDate.getHours()) + ":" + (cellDate.getMinutes()) + ":" + (cellDate.getSeconds());
//                                                    System.out.println("cellDateString :: "+cellDateString);
                                            } else {
                                                cellDateString = (cellDate.getYear() + 1900) + "-" + (cellDate.getMonth() + 1) + "-" + (cellDate.getDate());
                                            }

//                                            String cellDateString = (cellDate.getYear() + 1900) + "-" + (cellDate.getMonth() + 1) + "-" + (cellDate.getDate());
                                            dataObject.put(fileName + ":" + columnList.get(cellIndex), cellDateString);

                                        } else {
                                            String cellvalStr = NumberToTextConverter.toText(cell.getNumericCellValue());
                                            dataObject.put(fileName + ":" + columnList.get(cellIndex), cellvalStr);
                                        }
                                        break;
                                    case Cell.CELL_TYPE_BLANK:
                                        dataObject.put(fileName + ":" + columnList.get(cellIndex), "");
                                        break;
                                }

                            } else {
                                dataObject.put(fileName + ":" + columnList.get(cellIndex), "");
                            }
                        } catch (Exception e) {
                            dataObject.put(fileName + ":" + columnList.get(cellIndex), "");
                            continue;
                        }

                    }// end of row cell loop
                    dataList.add(dataObject);
                }// row end

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

    public List readCSV(HttpSession httpSession,
            String filepath,
            List<String> columnList,
            int startIndex,
            int limit,
            int endIndex, String fileName, String fileType) {
        FileInputStream fis = null;
        System.out.println("Start Date And Time :::" + new Date());
        List dataList = new ArrayList();
        int rowVal = 1;
        try {
            int rowCount = 0;
            //  fis = new FileInputStream(new File(filepath));

            String fileExtension = filepath.substring(filepath.lastIndexOf(".") + 1, filepath.length());
            System.out.println("fileExtension:::" + fileExtension);

//            char columnSeparator = '\t';
            CsvParserSettings settings = new CsvParserSettings();
            settings.detectFormatAutomatically();

            CsvParser parser = new CsvParser(settings);
            List<String[]> rows = parser.parseAll(new File(filepath));

            // if you want to see what it detected
            CsvFormat format = parser.getDetectedFormat();
            char columnSeparator = format.getDelimiter();

            if (".json".equalsIgnoreCase(fileType)) {
                columnSeparator = ',';
            }
            int stmt = 1;
            String strToDateCol = "";
            // need to write logic for extraction from File
            CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(filepath), "UTF8"), columnSeparator);
            LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(filepath));
            lineNumberReader.skip(Long.MAX_VALUE);
            long totalRecords = lineNumberReader.getLineNumber();
            if (totalRecords != 0) {
                totalRecords = totalRecords - 1;
            }
            System.out.println("totalRecords:::" + totalRecords);
//             CSVReader  reader = new CSVReader(new FileReader(filepath),'\t');

            rowVal = 1;

//            if (recordstartindex != null
//                    && !"".equalsIgnoreCase(recordstartindex)
//                    && !"null".equalsIgnoreCase(recordstartindex)
//                    && !"0".equalsIgnoreCase(recordstartindex)) {
//                rowVal = Integer.parseInt(recordstartindex);
//            }
////            int endIndex = (int)totalRecords + 1;
////            if (recordendindex != null
////                    && !"".equalsIgnoreCase(recordendindex)
////                    && !"null".equalsIgnoreCase(recordendindex)
////                    && Integer.parseInt(recordendindex) <= rowCount) {
////                endIndex = Integer.parseInt(recordendindex);
////            }
            int skipLines = 0;
            if (limit != 0) {
                skipLines = startIndex;
            }
            if (skipLines == 0) {
                String[] headers = reader.readNext();
            }
            reader.skip(skipLines);

            String[] nextLine;
            int rowsCount = 1;
            while ((nextLine = reader.readNext()) != null) {// no of rows
                if (limit >= rowsCount) {
                    rowsCount++;

                    JSONObject dataObject = new JSONObject();
                    dataObject.put("totalrecords", totalRecords);
                    for (int j = 0; j < columnList.size(); j++) {
                        try {
                            int cellIndex = j;
                            if (cellIndex <= (nextLine.length - 1)) {
                                String token = nextLine[cellIndex];
                                if (token != null
                                        && !"".equalsIgnoreCase(token)) {
                                    try {
                                        dataObject.put(fileName + ":" + columnList.get(j), token);
                                    } catch (Exception e) {
                                        dataObject.put(fileName + ":" + columnList.get(j), "");
                                        continue;
                                    }
                                } else {
                                    dataObject.put(fileName + ":" + columnList.get(j), "");
                                }
                            } else {
                                dataObject.put(fileName + ":" + columnList.get(j), "");
                            }
                        } catch (Exception e) {
                            dataObject.put(fileName + ":" + columnList.get(j), "");
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

    public List readXML(HttpSession httpSession,
            String filepath,
            List<String> columnList,
            int startIndex,
            int limit,
            int endIndex, String fileName) {
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
                        //            NodeList nList = (NodeList) xpath.evaluate("/PiLog_Data_Export/Item",
                        document,
                        XPathConstants.NODESET);

                if (dataNodeList != null && dataNodeList.getLength() != 0) {
                    rowCount = dataNodeList.getLength();

//                    Node headerNode = dataNodeList.item(0);
//                    if (headerNode.getNodeType() == Node.ELEMENT_NODE) {
//                        NodeList headerChildNodeList = headerNode.getChildNodes();
//                        int index = 0;
//                        for (int i = 0; i < headerChildNodeList.getLength(); i++) {// Columns
//                            Node childNode = headerChildNodeList.item(i);
//                            if (childNode != null
//                                    && childNode.getNodeType() == Node.ELEMENT_NODE) {
//                                headerData.put(childNode.getNodeName(), i);
//
//                            }
//                        }// end of columns loop
//
//                    }
                    if (startIndex != 0) {
                        endIndex++;
                    }
                    if (endIndex > rowCount) {
                        endIndex = rowCount;
                    }
                    for (int temp = startIndex; temp < endIndex; temp++) {// Rows
                        Node node = dataNodeList.item(temp);

                        if (node != null && node.getNodeType() == Node.ELEMENT_NODE) {
                            JSONObject dataObject = new JSONObject();
                            dataObject.put("totalrecords", rowCount);
                            NodeList childNodeList = node.getChildNodes();
                            for (int j = 0; j < childNodeList.getLength(); j++) {
                                try {
                                    Node childNode = childNodeList.item(j);
//                                    int childNodeIndex = j;
//                                    int nodeListLength = childNodeList.getLength();
                                    if (childNode != null) {
                                        if (childNode != null
                                                && childNode.getNodeType() == Node.ELEMENT_NODE) {
                                            try {
                                                if (childNode.getTextContent() != null
                                                        && !"".equalsIgnoreCase(childNode.getTextContent())
                                                        && !"null".equalsIgnoreCase(childNode.getTextContent())) {
                                                    dataObject.put(fileName + ":" + childNode.getNodeName(), childNode.getTextContent());

                                                } else {
                                                    dataObject.put(fileName + ":" + childNode.getNodeName(), "");
                                                }

                                            } catch (Exception e) {
                                                dataObject.put(fileName + ":" + childNode.getNodeName(), "");
                                                continue;
                                            }
                                            // Need to set the Data

                                        }
                                    }
                                } catch (Exception e) {

                                    continue;
                                }

                            }// column list loop
                            dataList.add(dataObject);
                        }

                    }// end of rows loop

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

    public List readJSON(String sessionUsername,
            String filepath,
            List<String> columnList,
            int startIndex,
            int limit,
            int endIndex, String fileName) {
        List DataList = new ArrayList();
        try {

        } catch (Exception e) {
            e.printStackTrace();
        }
        return DataList;
    }

    public List getHeadersOfImportedFile(String filePath, String fileType) {
        List<String> headers = null;
        try {
            if (filePath != null && !"".equalsIgnoreCase(filePath) && !"null".equalsIgnoreCase(filePath)) {
                String fileExt = filePath.substring(filePath.lastIndexOf(".") + 1, filePath.length());

                if (fileExt != null && !"".equalsIgnoreCase(fileExt)) {
                    if ("txt".equalsIgnoreCase(fileExt)
                            || "csv".equalsIgnoreCase(fileExt)) {
//                        char columnSeparator = '\t';
                        CsvParserSettings settings = new CsvParserSettings();
                        settings.detectFormatAutomatically();

                        CsvParser parser = new CsvParser(settings);
                        List<String[]> rows = parser.parseAll(new File(filePath));

                        // if you want to see what it detected
                        CsvFormat format = parser.getDetectedFormat();
                        char columnSeparator = format.getDelimiter();

                        if (".json".equalsIgnoreCase(fileType)) {
                            columnSeparator = ',';
                        }
                        CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(filePath), "UTF8"), columnSeparator);
                        String[] nextLine;
                        while ((nextLine = reader.readNext()) != null) {
                            if (nextLine.length != 0 && nextLine[0].contains("\t")) {
                                headers = new ArrayList<>(Arrays.asList(nextLine[0].split("\t")));
                            } else {
                                headers = new ArrayList<>(Arrays.asList(nextLine));
                            }

                            break;
                        }
                    } else if ("xls".equalsIgnoreCase(fileExt) || "xlsx".equalsIgnoreCase(fileExt)) {
                        headers = new ArrayList<>();
                        Workbook workBook = null;
                        Sheet sheet = null;
//                        if (fileExt != null && "xls".equalsIgnoreCase(fileExt)) {
//                            workBook = WorkbookFactory.create(new File(filePath));
//                            sheet = (HSSFSheet) workBook.getSheetAt(0);
//                        } else {

//                            System.out.println("Before::::" + new Date());
//                fis = new FileInputStream(new File(filepath));              
//                XSSFWorkbook xssfWb = (XSSFWorkbook) new XSSFWorkbook(fis);
                        workBook = WorkbookFactory.create(new File(filePath));
//                            System.out.println("After::fileInputStream::" + new Date());
                        sheet = (XSSFSheet) workBook.getSheetAt(0);
//                sheet = (XSSFSheet) xssfWb.getSheetAt(0);
//                        }
                        if (sheet != null) {
                            Row row = sheet.getRow(0);
                            if (row != null) {
                                for (int j = 0; j < row.getLastCellNum(); j++) {
                                    //  System.out.println("Cell Num:::" + j + ":::Start Date And Time :::" + new Date());

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
                                                        if ((cellDate.getYear() + 1900) == 1899 && (cellDate.getMonth() + 1) == 12 && (cellDate.getDate()) == 31) {
                                                            cellDateString = (cellDate.getHours()) + ":" + (cellDate.getMinutes()) + ":" + (cellDate.getSeconds());
//                                                    System.out.println("cellDateString :: "+cellDateString);
                                                        } else {
                                                            cellDateString = (cellDate.getYear() + 1900) + "-" + (cellDate.getMonth() + 1) + "-" + (cellDate.getDate());
                                                        }

//                                                        String cellDateString = (cellDate.getYear() + 1900) + "-" + (cellDate.getMonth() + 1) + "-" + (cellDate.getDate());
                                                        headers.add(cellDateString);
//                                            
                                                    } else {
                                                        String cellvalStr = NumberToTextConverter.toText(cell.getNumericCellValue());
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

                                }// end of row cell loop
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
                                    //            NodeList nList = (NodeList) xpath.evaluate("/PiLog_Data_Export/Item",
                                    document,
                                    XPathConstants.NODESET);
                            if (dataNodeList != null && dataNodeList.getLength() != 0) {
                                int rowCount = dataNodeList.getLength();
                                Node node = dataNodeList.item(0);
                                if (node.getNodeType() == Node.ELEMENT_NODE) {
                                    NodeList childNodeList = node.getChildNodes();
                                    for (int i = 0; i < childNodeList.getLength(); i++) {// Columns

                                        Node childNode = childNodeList.item(i);
                                        if (childNode != null
                                                && !"#Text".equalsIgnoreCase(childNode.getNodeName())) {
                                            headers.add(childNode.getNodeName());
                                            System.err.println(childNode.getNodeName() + "---> " + childNode.getTextContent());
                                        }

                                    }// end of columns loop

                                }

                            }

                        }
                    } else if ("json".equalsIgnoreCase(fileExt)) {
                        headers = new ArrayList<>();
                        JsonReader jsonReader = new JsonReader(new InputStreamReader(
                                new FileInputStream(filePath), StandardCharsets.UTF_8));
                        Gson gson = new GsonBuilder().create();
                        jsonReader.beginArray(); //start of json array
                        int totalRecords = 0;
                        if (jsonReader.hasNext()) {
                            JSONObject dataDocument = gson.fromJson(jsonReader, JSONObject.class);
                            if (dataDocument != null && !dataDocument.isEmpty()) {
                                headers.addAll(dataDocument.keySet());
                            }
                        }
                        jsonReader.endArray();
                        jsonReader.close();

                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return headers;
    }
    //scheduling code start
    // for DB

    public JSONObject processETLData(String sessionUserName,
            String orgnId,
            Connection fromConnection,
            Connection toConnection,
            List<Map> fromOperatorList,
            Map toOperator,
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            JSONObject tablesWhereClauseObj,
            JSONObject defaultValuesObj,
            JSONObject joinQueryMapObj,
            String joinQuery,
            JSONObject orderByObj,
            String groupByQuery,
            String nativeSQL,
            JSONObject appendValObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj,
            JSONObject normalizeOptionsObj
    ) {
        JSONObject resultObj = new JSONObject();
        PreparedStatement toPreparedStatement = null;
        try {
            JSONObject orderAndGroupByObj = new JSONObject();
            orderAndGroupByObj.put("orderByObj", orderByObj);
            orderAndGroupByObj.put("groupByQuery", groupByQuery);
            orderAndGroupByObj.put("nativeSQL", nativeSQL);
            int logSequenceNo = 10;
            //JSONObject fromColumnsObj = new JSONObject();			
            Map fromColumnsObj = new LinkedHashMap(); // ravi etl integration
            //JSONObject toColumnsObj = new JSONObject();			
            Map toColumnsObj = new LinkedHashMap(); // ravi etl integration
            if (columnsObj != null && !columnsObj.isEmpty()) {
                logSequenceNo += 10;
                try {
                    processETLLog(sessionUserName,
                            orgnId, "Reading the transformations rules.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                } catch (Exception e) {
                }
                for (Object toColumnName : columnsObj.keySet()) {
                    if (toColumnName != null && columnsObj.get(toColumnName) != null) {
                        String fromColumnStr = (String) columnsObj.get(toColumnName);
                        if (fromColumnStr != null
                                && !"".equalsIgnoreCase(fromColumnStr)
                                && !"null".equalsIgnoreCase(fromColumnStr)) {
                            String[] fromColumnArray = fromColumnStr.split(":");
                            if (fromColumnArray != null && fromColumnArray.length != 0) {
                                if (fromColumnsObj != null && !fromColumnsObj.isEmpty()) {
                                    if (fromColumnsObj.containsKey(fromColumnArray[0])) {
                                        fromColumnsObj.put(fromColumnArray[0],
                                                (fromColumnsObj.get(fromColumnArray[0]) + "," + fromColumnArray[1]));
                                    } else {
                                        fromColumnsObj.put(fromColumnArray[0],
                                                (fromColumnArray[1]));
                                    }
                                } else {
                                    fromColumnsObj.put(fromColumnArray[0], (fromColumnArray[1]));
                                }
                            }
                        }
                        if (toColumnName != null
                                && !"".equalsIgnoreCase(String.valueOf(toColumnName))
                                && !"null".equalsIgnoreCase(String.valueOf(toColumnName))) {
                            String[] toColumnArray = String.valueOf(toColumnName).split(":");
                            if (toColumnArray != null && toColumnArray.length != 0) {
                                if (toColumnsObj != null && !toColumnsObj.isEmpty()) {
                                    if (toColumnsObj.containsKey(toColumnArray[0])) {
                                        toColumnsObj.put(toColumnArray[0],
                                                (toColumnsObj.get(toColumnArray[0]) + "," + toColumnArray[1]));
                                    } else {
                                        toColumnsObj.put(toColumnArray[0],
                                                (toColumnArray[1]));
                                    }
                                } else {
                                    toColumnsObj.put(toColumnArray[0], (toColumnArray[1]));
                                }
                            }
                        }

                    }

                }
                //System.out.println("toColumnsObj:::" + toColumnsObj);
                Set<String> toColumns = new HashSet<>();
                toColumns.addAll(columnsObj.keySet());
                if (defaultValuesObj != null && !defaultValuesObj.isEmpty()) {
                    Set<String> defaultColumns = defaultValuesObj.keySet();
                    if (defaultColumns != null
                            && !defaultColumns.isEmpty() //  && toColumns != null && !toColumns.isEmpty()
                            ) {
                        Iterator defaultColumnsItr = defaultColumns.iterator();
                        while (defaultColumnsItr.hasNext()) {
                            String defaultColName = (String) defaultColumnsItr.next();
                            if (defaultColName != null
                                    && !"".equalsIgnoreCase(String.valueOf(defaultColName))
                                    && !"null".equalsIgnoreCase(String.valueOf(defaultColName))) {
                                String[] toColumnArray = String.valueOf(defaultColName).split(":");
                                if (toColumnArray != null && toColumnArray.length != 0) {
                                    if (toColumnsObj != null && !toColumnsObj.isEmpty()) {
                                        if (toColumnsObj.containsKey(toColumnArray[0])) {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnsObj.get(toColumnArray[0]) + "," + toColumnArray[1]));
                                        } else {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnArray[1]));
                                        }
                                    } else {
                                        toColumnsObj.put(toColumnArray[0], (toColumnArray[1]));
                                    }
                                }
                            }
                        }
//                        toColumns.addAll(defaultColumns);
                    }
                }
                if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
                    Set<String> columnClauseCols = columnClauseObj.keySet();
                    if (columnClauseCols != null
                            && !columnClauseCols.isEmpty() //  && toColumns != null && !toColumns.isEmpty()
                            ) {
                        // toColumns.addAll(columnClauseCols);
                        Iterator columnClauseColsItr = columnClauseCols.iterator();
                        while (columnClauseColsItr.hasNext()) {
                            String columnClauseColName = (String) columnClauseColsItr.next();
                            if (columnClauseColName != null
                                    && !"".equalsIgnoreCase(String.valueOf(columnClauseColName))
                                    && !"null".equalsIgnoreCase(String.valueOf(columnClauseColName))) {
                                String[] toColumnArray = String.valueOf(columnClauseColName).split(":");
                                if (toColumnArray != null && toColumnArray.length != 0) {
                                    if (toColumnsObj != null && !toColumnsObj.isEmpty()) {
                                        if (toColumnsObj.containsKey(toColumnArray[0])) {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnsObj.get(toColumnArray[0]) + "," + toColumnArray[1]));
                                        } else {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnArray[1]));
                                        }
                                    } else {
                                        toColumnsObj.put(toColumnArray[0], (toColumnArray[1]));
                                    }
                                }
                            }
                        }
                    }
                }
                selectTabObj.put("toColumnsObj", toColumnsObj);
//                if (toConnection != null) {
//                    String toTableInsertQuery = generateInsertQuery((String) toOperator.get("tableName"), toColumnsList);
//                    System.out.println("insertQuery::::" + toTableInsertQuery);
//                    toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
//                }
                List<Map> toOperatorsList = (List<Map>) selectTabObj.get("toOperatorList");
                if (toOperatorsList != null && !toOperatorsList.isEmpty()) {
                    for (Map toOperatorMap : toOperatorsList) {
                        if (toOperatorMap != null && !toOperatorMap.isEmpty()) {
                            toOperator = toOperatorMap;
                            List<String> toColumnsList = new ArrayList<>();
                            toOperator = toOperatorMap;
                            String toTableName = (String) toOperator.get("tableName");
                            int totalDataCount = 0;
                            int fileDataLastIndex = 0;
                            List<Map> nonJoinOpList = fromOperatorList;
                            String fileName = "";
                            String orginalName = (String) toOperator.get("userFileName");
                            String iconType = (String) toOperator.get("iconType");
                            JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
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
                            } else if ("JSON".equalsIgnoreCase(iconType)) {
                                fileName = "V10ETLExport_" + System.currentTimeMillis() + ".json";
                            } else {
                                if (toTableName != null
                                        && !"".equalsIgnoreCase(String.valueOf(toTableName))
                                        && !"null".equalsIgnoreCase(String.valueOf(toTableName))) {
                                    String toColumnsStr = (String) toColumnsObj.get(toTableName);
                                    if (toColumnsStr != null
                                            && !"".equalsIgnoreCase(toColumnsStr)) {

                                        toColumnsList = Arrays.asList(toColumnsStr.split(","));
                                        Object toConnectionObj = getConnection(toConnObj);
                                        if (toConnectionObj instanceof Connection) {
                                            toConnection = (Connection) toConnectionObj;
                                        }
                                        if (toConnection != null) {
                                            String toTableInsertQuery = generateInsertQuery((String) toOperator.get("tableName"), toColumnsList);
                                            System.out.println("insertQuery::::" + toTableInsertQuery);
                                            toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
                                            Map columnsTypeObj = getColumnsType((String) toOperator.get("tableName"), toColumnsList, toConnection);
                                            selectTabObj.put("columnsTypeObj", columnsTypeObj);
                                        }
                                    }
                                }
                            }
                            if (!(toColumnsList != null && !toColumnsList.isEmpty())) {
                                toColumnsList.addAll(columnsObj.keySet());
                            }
                            if (joinQueryMapObj != null
                                    && !joinQueryMapObj.isEmpty()) {

                                nonJoinOpList = fromOperatorList.stream()
                                        .filter(opMap -> (opMap != null && !opMap.isEmpty()
                                        && opMap.get("tableName") != null
                                        && !"".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
                                        && !"null".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
                                        && !joinQueryMapObj.containsKey(String.valueOf(opMap.get("tableName"))))).collect(Collectors.toList());
                                try {
                                    processETLLog(sessionUserName,
                                            orgnId, "Starting extract join tables data.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                                } catch (Exception e) {
                                }
                                totalDataCount += processETLData(sessionUserName,
                                        orgnId,
                                        fromConnection,
                                        toConnection,
                                        fromOperatorList,
                                        toOperator,
                                        columnsObj,
                                        fromColumnsObj,
                                        tablesWhereClauseObj,
                                        defaultValuesObj,
                                        toPreparedStatement,
                                        1,
                                        1000,
                                        totalDataCount,
                                        toColumnsList,
                                        joinQueryMapObj,
                                        joinQuery,
                                        fileDataLastIndex,
                                        fileName,
                                        1,
                                        orderAndGroupByObj,
                                        columnClauseObj,
                                        selectTabObj,
                                        normalizeOptionsObj
                                );

                            } else {
                                if (nonJoinOpList != null && !nonJoinOpList.isEmpty()) {
                                    try {
                                        processETLLog(sessionUserName,
                                                orgnId, "Starting extract non-join tables data.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                                    } catch (Exception e) {
                                    }
                                    for (int i = 0; i < nonJoinOpList.size(); i++) {
                                        Map fromOperator = nonJoinOpList.get(i);
                                        if (fromOperator != null && !fromOperator.isEmpty()) {
                                            totalDataCount += processETLData(sessionUserName,
                                                    orgnId,
                                                    fromConnection,
                                                    toConnection,
                                                    fromOperator,
                                                    toOperator,
                                                    columnsObj,
                                                    fromColumnsObj,
                                                    tablesWhereClauseObj,
                                                    defaultValuesObj,
                                                    toPreparedStatement,
                                                    1,
                                                    1000,
                                                    totalDataCount,
                                                    toColumnsList,
                                                    fileDataLastIndex,
                                                    fileName,
                                                    1,
                                                    orderAndGroupByObj,
                                                    columnClauseObj,
                                                    selectTabObj,
                                                    normalizeOptionsObj
                                            );
                                        }

                                    }
                                }

                            }

                            System.out.println("totalDataCount::::" + totalDataCount);
                            if (totalDataCount != 0 && totalDataCount > 0) {
                                String message = totalDataCount + " Row(s) successfully extracted and loaded into target system.";
                                if (fileName != null && !"".equalsIgnoreCase(fileName) && !"null".equalsIgnoreCase(fileName)) {//orginalName
                                    if (orginalName != null
                                            && !"".equalsIgnoreCase(orginalName)
                                            && !"null".equalsIgnoreCase(orginalName)) {
                                        orginalName = orginalName.replaceAll("[^.a-zA-Z0-9]", "_");
                                    }

                                    message += " <br> <a href='#' style='color:#0071c5;' onclick=downloadExportedFile('" + fileName + "',\"" + orginalName + "\") >Click here to download the " + iconType + " file</a>.";//
                                }
                                logSequenceNo += 10;
                                try {
                                    processETLLog(sessionUserName,
                                            orgnId, message, "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
                                } catch (Exception e) {
                                }
                                resultObj.put("Message", message);
                                resultObj.put("connectionFlag", "Y");

                            } else {
                                logSequenceNo += 10;
                                try {
                                    processETLLog(sessionUserName,
                                            orgnId, totalDataCount + " Row(s) successfully extracted and loaded into target system.", "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
                                } catch (Exception e) {
                                }
//                    processETLLog(httpSession,totalDataCount + " Row(s) extracted successfully", 10);
                                resultObj.put("Message", totalDataCount + " Row(s) successfully extracted and loaded into target system.");
                                resultObj.put("connectionFlag", "Y");
                            }

                        }
                    }
                }
//                if (toColumnsObj != null && !toColumnsObj.isEmpty()) {
//                    List<Map> toOperatorsList = (List<Map>) selectTabObj.get("toOperatorList");
//                    for (Object toTableName : toColumnsObj.keySet()) {
//                        if (toTableName != null
//                                && !"".equalsIgnoreCase(String.valueOf(toTableName))
//                                && !"null".equalsIgnoreCase(String.valueOf(toTableName))) {
//                            String toColumnsStr = (String) toColumnsObj.get(toTableName);
//                            if (toColumnsStr != null
//                                    && !"".equalsIgnoreCase(toColumnsStr)) {
//                                List<String> toColumnsList = new ArrayList<>();
//                                toColumnsList = Arrays.asList(toColumnsStr.split(","));
//                                List<Map> matchedToOperatorsList = toOperatorsList.stream().filter(toOp
//                                        -> (toOp != null
//                                        && !toOp.isEmpty()
//                                        && toOp.get("tableName") != null
//                                        && !"".equalsIgnoreCase((String) toOp.get("tableName"))
//                                        && !"null".equalsIgnoreCase((String) toOp.get("tableName"))
//                                        && String.valueOf(toOp.get("tableName")).equalsIgnoreCase(String.valueOf(toTableName)))).collect(Collectors.toList());
//                                if (matchedToOperatorsList != null && !matchedToOperatorsList.isEmpty()) {
//                                    toOperator = matchedToOperatorsList.get(0);
//                                }
//                                if (toConnection != null) {
//                                    String toTableInsertQuery = generateInsertQuery((String) toOperator.get("tableName"), toColumnsList);
//                                    System.out.println("insertQuery::::" + toTableInsertQuery);
//                                    toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
//                                }
//
//                                int totalDataCount = 0;
//                                int fileDataLastIndex = 0;
//                                List<Map> nonJoinOpList = fromOperatorList;
//                                String fileName = "";
//                                String orginalName = (String) toOperator.get("userFileName");
//                                String iconType = (String) toOperator.get("iconType");
//
//                                if ("XLSX".equalsIgnoreCase(iconType)) {
//                                    fileName = "V10ETLExport_" + System.currentTimeMillis() + ".xlsx";
//
//                                } else if ("XLS".equalsIgnoreCase(iconType)) {
//                                    fileName = "V10ETLExport_" + System.currentTimeMillis() + ".xls";
//                                } else if ("XML".equalsIgnoreCase(iconType)) {
//                                    fileName = "V10ETLExport_" + System.currentTimeMillis() + ".xml";
//                                } else if ("CSV".equalsIgnoreCase(iconType)) {
//                                    fileName = "V10ETLExport_" + System.currentTimeMillis() + ".csv";
//                                } else if ("TXT".equalsIgnoreCase(iconType)) {
//                                    fileName = "V10ETLExport_" + System.currentTimeMillis() + ".txt";
//                                }
//
//                                if (joinQueryMapObj != null
//                                        && !joinQueryMapObj.isEmpty()) {
//
//                                    nonJoinOpList = fromOperatorList.stream()
//                                            .filter(opMap -> (opMap != null && !opMap.isEmpty()
//                                            && opMap.get("tableName") != null
//                                            && !"".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
//                                            && !"null".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
//                                            && !joinQueryMapObj.containsKey(String.valueOf(opMap.get("tableName"))))).collect(Collectors.toList());
//                                    try {
//                                        processETLLog(sessionUserName,
//                                                orgnId, "Starting extract join tables data.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
//                                    } catch (Exception e) {
//                                    }
//                                    totalDataCount += processETLData(sessionUserName,
//                                            orgnId,
//                                            fromConnection,
//                                            toConnection,
//                                            fromOperatorList,
//                                            toOperator,
//                                            columnsObj,
//                                            fromColumnsObj,
//                                            tablesWhereClauseObj,
//                                            defaultValuesObj,
//                                            toPreparedStatement,
//                                            0,
//                                            1000,
//                                            totalDataCount,
//                                            toColumnsList,
//                                            joinQueryMapObj,
//                                            joinQuery,
//                                            fileDataLastIndex,
//                                            fileName,
//                                            1,
//                                            orderAndGroupByObj,
//                                            columnClauseObj,
//                                            selectTabObj,
//                                            normalizeOptionsObj
//                                    );
//
//                                } else {
//                                    if (nonJoinOpList != null && !nonJoinOpList.isEmpty()) {
//                                        try {
//                                            processETLLog(sessionUserName,
//                                                    orgnId, "Starting extract non-join tables data.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
//                                        } catch (Exception e) {
//                                        }
//                                        for (int i = 0; i < nonJoinOpList.size(); i++) {
//                                            Map fromOperator = nonJoinOpList.get(i);
//                                            if (fromOperator != null && !fromOperator.isEmpty()) {
//                                                totalDataCount += processETLData(sessionUserName,
//                                                        orgnId,
//                                                        fromConnection,
//                                                        toConnection,
//                                                        fromOperator,
//                                                        toOperator,
//                                                        columnsObj,
//                                                        fromColumnsObj,
//                                                        tablesWhereClauseObj,
//                                                        defaultValuesObj,
//                                                        toPreparedStatement,
//                                                        0,
//                                                        1000,
//                                                        totalDataCount,
//                                                        toColumnsList,
//                                                        fileDataLastIndex,
//                                                        fileName,
//                                                        1,
//                                                        orderAndGroupByObj,
//                                                        columnClauseObj,
//                                                        selectTabObj,
//                                                        normalizeOptionsObj
//                                                );
//                                            }
//
//                                        }
//                                    }
//
//                                }
//
//                                System.out.println("totalDataCount::::" + totalDataCount);
//                                if (totalDataCount != 0 && totalDataCount > 0) {
//                                    String message = totalDataCount + " Row(s) successfully extracted and loaded into target system.";
//                                    if (fileName != null && !"".equalsIgnoreCase(fileName) && !"null".equalsIgnoreCase(fileName)) {//orginalName
//                                        if (orginalName != null
//                                                && !"".equalsIgnoreCase(orginalName)
//                                                && !"null".equalsIgnoreCase(orginalName)) {
//                                            orginalName = orginalName.replaceAll("[^.a-zA-Z0-9]", "_");
//                                        }
//
//                                        message += " <br> <a href='#' style='color:#0071c5;' onclick=downloadExportedFile('" + fileName + "',\"" + orginalName + "\") >Click here to download the " + iconType + " file</a>.";//
//                                    }
//                                    logSequenceNo += 10;
//                                    try {
//                                        processETLLog(sessionUserName,
//                                                orgnId, message, "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
//                                    } catch (Exception e) {
//                                    }
//                                    resultObj.put("Message", message);
//                                    resultObj.put("connectionFlag", "Y");
//
//                                } else {
//                                    logSequenceNo += 10;
//                                    try {
//                                        processETLLog(sessionUserName,
//                                                orgnId, totalDataCount + " Row(s) successfully extracted and loaded into target system.", "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
//                                    } catch (Exception e) {
//                                    }
////                    processETLLog(httpSession,totalDataCount + " Row(s) extracted successfully", 10);
//                                    resultObj.put("Message", totalDataCount + " Row(s) successfully extracted and loaded into target system.");
//                                    resultObj.put("connectionFlag", "Y");
//                                }
//                            }
//                        }
//                    }
//                }
            }// end of columnObj if
            try {
                processETLLog(sessionUserName,
                        orgnId, "ETL Process is completed", "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception e) {
            }
        } catch (Exception e) {
            e.printStackTrace();
            resultObj.put("Message", e.getMessage());
            resultObj.put("connectionFlag", "N");
            try {
                processETLLog(sessionUserName,
                        orgnId, e.getMessage(), "ERROR", 20, "N", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }

        } finally {
            try {
                if (toPreparedStatement != null) {
                    toPreparedStatement.close();
                }
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

    // JOINS 
    public int processETLData(String sessionUserName,
            String orgnId,
            Connection fromConnection,
            Connection toConnection,
            List<Map> fromOperatorList,
            Map toOperator,
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            //JSONObject fromColumnsObj,
            Map fromColumnsObj, // ravi etl integration
            JSONObject tablesWhereClauseObj,
            JSONObject defaultValuesObj,
            PreparedStatement toPreparedStatement,
            int start,
            int limit,
            int totalDataCount,
            List<String> toColumnsList,
            JSONObject joinQueryMapObj,
            String joinQuery,
            int fileDataLastIndex,
            String fileName,
            int logSequenceNo,
            JSONObject appendValObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj,
            JSONObject normalizeOptionsObj
    ) {
        try {
            if (selectTabObj != null
                    && !selectTabObj.isEmpty()) {
                selectTabObj.put("columnClauseObj", columnClauseObj);
                String minRows = String.valueOf(selectTabObj.get("minRows"));
                if (minRows != null
                        && !"".equalsIgnoreCase(minRows)
                        && !"null".equalsIgnoreCase(minRows)
                        && NumberUtils.isNumber(minRows)) {
                    if (logSequenceNo == 1) {
                        start = Integer.parseInt(minRows);
                    }
                }
                String maxRows = String.valueOf(selectTabObj.get("maxRows"));
                if (maxRows != null
                        && !"".equalsIgnoreCase(maxRows)
                        && !"null".equalsIgnoreCase(maxRows)
                        && !"0".equalsIgnoreCase(maxRows)
                        && NumberUtils.isNumber(maxRows)) {
                    int maxRowsCount = Integer.parseInt(maxRows);
                    if (logSequenceNo == 1) {
                        if (((maxRowsCount - start)) <= limit) {
                            limit = (maxRowsCount - start);
                        }
                    } else {
                        if (((maxRowsCount - start) + 1) <= limit) {
                            limit = (maxRowsCount - start) + 1;
                        }
                    }

                }

            }
            Map fromOperator = fromOperatorList.get(0);
            int end = start + limit - 1;
            if (start == 0) {
                end = limit;
            }
            try {
                processETLLog(sessionUserName,
                        orgnId, "Fetching from " + start + " to " + end + " record(s).", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
//                        (String) httpSession.getAttribute("ssOrgId"), "Fetching next 1000 record(s).", "INFO", logSequenceNo, "Y");
            } catch (Exception e) {
            }
            List totalDataList = new ArrayList();
            if (limit > 0) {
                totalDataList = getSelectedJoinColumnsData((JSONObject) fromOperator.get("connObj"),
                        fromConnection,
                        fromColumnsObj,
                        start,
                        limit,
                        tablesWhereClauseObj, joinQueryMapObj, joinQuery,
                        sessionUserName,
                        orgnId,
                        appendValObj,
                        columnClauseObj,
                        selectTabObj,
                        toConnection
                );
            }
            String iconType = (String) toOperator.get("iconType");
            if (totalDataList != null && !totalDataList.isEmpty()) {

                try {
                    processETLLog(sessionUserName,
                            orgnId, "Fetched " + totalDataList.size() + " record(s).", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                } catch (Exception e) {
                }

                // ravi normalising   start
                // String normalizeFlag = request.getParameter("normalizeFlag");
                if (normalizeOptionsObj != null && normalizeOptionsObj.size() > 0) {
                    String normalizeFlag = (String) normalizeOptionsObj.get("normalizeFlag");
//                    columnsObj = (JSONObject) normalizeOptionsObj.get("colsObj");
//                    Set<String> toColumnsSet = columnsObj.keySet();
//                    toColumnsList.clear();
//                    if (toColumnsSet != null && !toColumnsSet.isEmpty()) {
//                        toColumnsList.addAll(toColumnsSet);
//                    }
                    try {
                        processETLLog(sessionUserName,
                                orgnId, "Started " + normalizeFlag + " records.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                    } catch (Exception e) {
                    }
                    if (normalizeFlag != null && "normalize".equalsIgnoreCase(normalizeFlag)) {

                        totalDataList = getNoramlisedData(normalizeOptionsObj, totalDataList);
                    } else if (normalizeFlag != null && "deNormalize".equalsIgnoreCase(normalizeFlag)) {

                        totalDataList = getDeNoramlisedData(normalizeOptionsObj, totalDataList);
                    }
                }
                // ravi normalising   end

                try {
                    processETLLog(sessionUserName,
                            orgnId, "Pushing " + totalDataList.size() + " record(s) into target system(" + ((fileName != null
                            && !"".equalsIgnoreCase(fileName)
                            && !"null".equalsIgnoreCase(fileName)) ? fileName : "" + toOperator.get("tableName")) + ").", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                } catch (Exception e) {
                }
//                String iconType = (String) toOperator.get("iconType");
                int insertCount = 0;
                JSONObject fileDataObj = new JSONObject();
                if ("XLSX".equalsIgnoreCase(iconType)) {
                    FileInputStream inputStream = null;
                    XSSFWorkbook wb = null;
                    XSSFSheet sheet = null;
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
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

                    insertCount = exportingXLSXFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            sheet,
                            wb,
                            selectTabObj
                    );

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
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
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

                    insertCount = exportingXLSFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            sheet,
                            wb,
                            selectTabObj
                    );

                    if (inputStream != null) {
                        inputStream.close();
                    }
                    if (wb != null) {
                        wb.close();
                    }
                } else if ("XML".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingXMLFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else if ("CSV".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingCsvAndTxtFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
//                    exportingCsvAndTxtFileData
                } else if ("TXT".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }

                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingCsvAndTxtFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else if ("JSON".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }

                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingJSONFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else {
                    String dragType = (String) toOperator.get("dragType");
                    JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
                    if ("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1")))) {
                        Object toConnectionObj = selectTabObj.get("toConnectionObj");
//                        Object toConnectionObj = getConnection(toConnObj);
                        if (toConnectionObj instanceof JCO.Client) {
                            if ("Table".equalsIgnoreCase(dragType)) {
                                // need to call BAPI for pushing into SAP
                                insertCount = dataMigrationService.importSAPData((String) toOperator.get("tableName"),
                                        fromColumnsObj,
                                        columnsObj,
                                        totalDataList,
                                        (JCO.Client) toConnectionObj,
                                        toColumnsList,
                                        defaultValuesObj,
                                        sessionUserName,
                                        orgnId,
                                        String.valueOf(selectTabObj.get("jobId")),
                                        selectTabObj);
                            }
                        }
                    } else {
                        insertCount = dataMigrationService.importingData((String) toOperator.get("tableName"),
                                fromColumnsObj,
                                columnsObj,
                                totalDataList,
                                toPreparedStatement,
                                toColumnsList,
                                defaultValuesObj, sessionUserName,
                                orgnId,
                                String.valueOf(selectTabObj.get("jobId")),
                                selectTabObj);
                    }

                }
                if (insertCount != 0) {
                    try {
                        processETLLog(sessionUserName,
                                orgnId, "Pushed " + insertCount + " record(s) into target system(" + ((fileName != null
                                && !"".equalsIgnoreCase(fileName)
                                && !"null".equalsIgnoreCase(fileName)) ? fileName : "" + toOperator.get("tableName")) + ").", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                    } catch (Exception e) {
                    }
                }
                totalDataCount += insertCount;
                if (insertCount != 0 && insertCount >= 1000) {
                    int startIndex = (logSequenceNo * limit + 1);
                    logSequenceNo++;
                    totalDataCount = processETLData(sessionUserName,
                            orgnId,
                            fromConnection,
                            toConnection,
                            fromOperatorList,
                            toOperator,
                            columnsObj,
                            fromColumnsObj,
                            tablesWhereClauseObj,
                            defaultValuesObj,
                            toPreparedStatement,
                            startIndex,
                            limit,
                            totalDataCount,
                            toColumnsList,
                            joinQueryMapObj,
                            joinQuery,
                            fileDataLastIndex,
                            fileName,
                            logSequenceNo,
                            appendValObj,
                            columnClauseObj,
                            selectTabObj,
                            normalizeOptionsObj
                    );

                } else if ("JSON".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                    File file12 = new File(filePath);
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    if (outputFile.exists()) {
                        FileOutputStream fos = new FileOutputStream(outputFile, true);
                        OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
                        BufferedWriter writer = new BufferedWriter(osw);
                        writer.append("]");
                        writer.close();
                        osw.close();
                        fos.close();
                    }
                }
            } else if ("JSON".equalsIgnoreCase(iconType)) {
                String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                File file12 = new File(filePath);
                File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                if (outputFile.exists()) {
                    FileOutputStream fos = new FileOutputStream(outputFile, true);
                    OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
                    BufferedWriter writer = new BufferedWriter(osw);
                    writer.append("]");
                    writer.close();
                    osw.close();
                    fos.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();

            try {
                processETLLog(sessionUserName,
                        orgnId, "Failed process records in ETL due to " + e.getLocalizedMessage(), "ERROR", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }
        }
        return totalDataCount;
    }

    public int processETLData(String sessionUserName,
            String orgnId,
            Connection fromConnection,
            Connection toConnection,
            Map fromOperator,
            Map toOperator,
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            //JSONObject fromColumnsObj,
            Map fromColumnsObj, // ravi etl integration
            JSONObject tablesWhereClauseObj,
            JSONObject defaultValuesObj,
            PreparedStatement toPreparedStatement,
            int start,
            int limit,
            int totalDataCount,
            List<String> toColumnsList,
            int fileDataLastIndex,
            String fileName,
            int logSequenceNo,
            JSONObject appendValObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj,
            JSONObject normalizeOptionsObj
    ) {
        Object resultObj = null;
        try {

            if (selectTabObj != null
                    && !selectTabObj.isEmpty()) {
                selectTabObj.put("columnClauseObj", columnClauseObj);
                String minRows = String.valueOf(selectTabObj.get("minRows"));
                if (minRows != null
                        && !"".equalsIgnoreCase(minRows)
                        && !"null".equalsIgnoreCase(minRows)
                        && NumberUtils.isNumber(minRows)) {
                    if (logSequenceNo == 1) {
                        start = Integer.parseInt(minRows);
                    }
                }
                String maxRows = String.valueOf(selectTabObj.get("maxRows"));
                if (maxRows != null
                        && !"".equalsIgnoreCase(maxRows)
                        && !"null".equalsIgnoreCase(maxRows)
                        && NumberUtils.isNumber(maxRows)) {
                    int maxRowsCount = Integer.parseInt(maxRows);
                    if (logSequenceNo == 1) {
                        if (((maxRowsCount - start)) <= limit) {
                            limit = (maxRowsCount - start);
                        }
                    } else {
                        if (((maxRowsCount - start) + 1) <= limit) {
                            limit = (maxRowsCount - start) + 1;
                        }
                    }
                }

            }
            int end = start + limit - 1;
            if (start == 0) {
                end = limit;
            }
            try {
                processETLLog(sessionUserName,
                        orgnId, "Fetching from " + start + " to " + end + " record(s).", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception e) {
            }
            List totalDataList = new ArrayList();
            if (limit > 0) {
                totalDataList = getSelectedColumnsData((JSONObject) fromOperator.get("connObj"),
                        fromConnection,
                        fromColumnsObj,
                        start,
                        limit,
                        tablesWhereClauseObj, sessionUserName,
                        orgnId, appendValObj, columnClauseObj, selectTabObj, toConnection);
            }
            String iconType = (String) toOperator.get("iconType");
            if (totalDataList != null && !totalDataList.isEmpty()) {
                try {
                    processETLLog(sessionUserName,
                            orgnId, "Fetched " + totalDataList.size() + " record(s).", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                } catch (Exception e) {
                }

                // ravi normalising   start
                // String normalizeFlag = request.getParameter("normalizeFlag");
                if (normalizeOptionsObj != null && normalizeOptionsObj.size() > 0) {
                    String normalizeFlag = (String) normalizeOptionsObj.get("normalizeFlag");
//                    columnsObj = (JSONObject) normalizeOptionsObj.get("colsObj");
//                    Set<String> toColumnsSet = columnsObj.keySet();
//                    toColumnsList.clear();
//                    if (toColumnsSet != null && !toColumnsSet.isEmpty()) {
//                        toColumnsList.addAll(toColumnsSet);
//                    }
                    try {
                        processETLLog(sessionUserName,
                                orgnId, "Started " + normalizeFlag + " records.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                    } catch (Exception e) {
                    }
                    if (normalizeFlag != null && "normalize".equalsIgnoreCase(normalizeFlag)) {

                        totalDataList = getNoramlisedData(normalizeOptionsObj, totalDataList);
                    } else if (normalizeFlag != null && "deNormalize".equalsIgnoreCase(normalizeFlag)) {

                        totalDataList = getDeNoramlisedData(normalizeOptionsObj, totalDataList);
                    }
                }
                // ravi normalising   end

                try {
                    processETLLog(sessionUserName,
                            orgnId, "Pushing " + totalDataList.size() + " record(s) into target system(" + ((fileName != null
                            && !"".equalsIgnoreCase(fileName)
                            && !"null".equalsIgnoreCase(fileName)) ? fileName : "" + toOperator.get("tableName")) + ").", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                } catch (Exception e) {
                }
                int insertCount = 0;
//                String iconType = (String) toOperator.get("iconType");
                if ("XLSX".equalsIgnoreCase(iconType)) {
                    FileInputStream inputStream = null;
                    XSSFWorkbook wb = null;
                    XSSFSheet sheet = null;
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
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

                    insertCount = exportingXLSXFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            sheet,
                            wb,
                            selectTabObj
                    );

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
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
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

                    insertCount = exportingXLSFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            sheet,
                            wb,
                            selectTabObj
                    );

                    if (inputStream != null) {
                        inputStream.close();
                    }
                    if (wb != null) {
                        wb.close();
                    }
                } else if ("XML".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingXMLFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else if ("CSV".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingCsvAndTxtFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
//                    exportingCsvAndTxtFileData
                } else if ("TXT".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }

                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingCsvAndTxtFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else if ("JSON".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }

                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingJSONFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else {
                    String dragType = (String) toOperator.get("dragType");
                    JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
                    if ("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1")))) {
                        Object toConnectionObj = selectTabObj.get("toConnectionObj");
//                        Object toConnectionObj = getConnection(toConnObj);
                        if (toConnectionObj instanceof JCO.Client) {
                            if ("Table".equalsIgnoreCase(dragType)) {
                                // need to call BAPI for pushing into SAP
                                insertCount = dataMigrationService.importSAPData((String) toOperator.get("tableName"),
                                        fromColumnsObj,
                                        columnsObj,
                                        totalDataList,
                                        (JCO.Client) toConnectionObj,
                                        toColumnsList,
                                        defaultValuesObj,
                                        sessionUserName,
                                        orgnId,
                                        String.valueOf(selectTabObj.get("jobId")),
                                        selectTabObj);
                            } // ravi material creation bapi
                            else if ("Bapi".equalsIgnoreCase(dragType)) {

                            }

                        }
                    } else {
                        insertCount = dataMigrationService.importingData((String) toOperator.get("tableName"),
                                fromColumnsObj,
                                columnsObj,
                                totalDataList,
                                toPreparedStatement,
                                toColumnsList,
                                defaultValuesObj, sessionUserName,
                                orgnId,
                                String.valueOf(selectTabObj.get("jobId")),
                                selectTabObj);
                    }
                }
                if (insertCount != 0) {
                    try {
                        processETLLog(sessionUserName,
                                orgnId, "Pushed " + insertCount + " record(s) into target system.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                    } catch (Exception e) {
                    }
                }

                totalDataCount += insertCount;
                if (insertCount != 0 && insertCount >= 1000) {
                    int startIndex = (logSequenceNo * limit + 1);

                    logSequenceNo++;
                    totalDataCount = processETLData(sessionUserName,
                            orgnId,
                            fromConnection,
                            toConnection,
                            fromOperator,
                            toOperator,
                            columnsObj,
                            fromColumnsObj,
                            tablesWhereClauseObj,
                            defaultValuesObj,
                            toPreparedStatement,
                            startIndex,
                            //(logSequenceNo * limit + 1),
                            limit,
                            totalDataCount,
                            toColumnsList,
                            fileDataLastIndex,
                            fileName,
                            logSequenceNo,
                            appendValObj,
                            columnClauseObj,
                            selectTabObj,
                            normalizeOptionsObj
                    );

                } else if ("JSON".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                    File file12 = new File(filePath);
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    if (outputFile.exists()) {
                        FileOutputStream fos = new FileOutputStream(outputFile, true);
                        OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
                        BufferedWriter writer = new BufferedWriter(osw);
                        writer.append("]");
                        writer.close();
                        osw.close();
                        fos.close();
                    }
                }
            } else if ("JSON".equalsIgnoreCase(iconType)) {
                String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                File file12 = new File(filePath);
                File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                if (outputFile.exists()) {
                    FileOutputStream fos = new FileOutputStream(outputFile, true);
                    OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
                    BufferedWriter writer = new BufferedWriter(osw);
                    writer.append("]");
                    writer.close();
                    osw.close();
                    fos.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            try {
                processETLLog(sessionUserName,
                        orgnId, e.getMessage(), "ERROR", 20, "Y", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }
        }
        return totalDataCount;
    }

    //for files
    public JSONObject processETLData(String sessionUserName,
            String orgnId,
            Connection toConnection,
            List<Map> fromOperatorList,
            Map toOperator,
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            JSONObject tablesWhereClauseObj,
            JSONObject defaultValuesObj,
            JSONObject joinQueryMapObj,
            String joinQuery,
            JSONObject orderByObj,
            String groupByQuery,
            String nativeSQL,
            JSONObject appendValObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj,
            JSONObject normalizeOptionsObj
    ) {
        JSONObject resultObj = new JSONObject();
        PreparedStatement toPreparedStatement = null;
        try {
            JSONObject orderAndGroupByObj = new JSONObject();
            orderAndGroupByObj.put("orderByObj", orderByObj);
            orderAndGroupByObj.put("groupByQuery", groupByQuery);
            orderAndGroupByObj.put("nativeSQL", nativeSQL);
            int logSequenceNo = 10;
            //JSONObject fromColumnsObj = new JSONObject();			
            Map fromColumnsObj = new LinkedHashMap(); // ravi etl integration
            //JSONObject toColumnsObj = new JSONObject();			
            Map toColumnsObj = new LinkedHashMap(); // ravi etl integration
            if (columnsObj != null && !columnsObj.isEmpty()) {
                logSequenceNo += 10;
                try {
                    processETLLog(sessionUserName,
                            orgnId, "Reading the transformations rules.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                } catch (Exception e) {
                }
                for (Object toColumnName : columnsObj.keySet()) {
                    if (toColumnName != null && columnsObj.get(toColumnName) != null) {
                        String fromColumnStr = (String) columnsObj.get(toColumnName);
                        if (fromColumnStr != null
                                && !"".equalsIgnoreCase(fromColumnStr)
                                && !"null".equalsIgnoreCase(fromColumnStr)) {
                            String[] fromColumnArray = fromColumnStr.split(":");
                            if (fromColumnArray != null && fromColumnArray.length != 0) {
                                if (fromColumnsObj != null && !fromColumnsObj.isEmpty()) {
                                    if (fromColumnsObj.containsKey(fromColumnArray[0])) {
                                        fromColumnsObj.put(fromColumnArray[0],
                                                (fromColumnsObj.get(fromColumnArray[0]) + "," + fromColumnArray[1]));
                                    } else {
                                        fromColumnsObj.put(fromColumnArray[0],
                                                (fromColumnArray[1]));
                                    }
                                } else {
                                    fromColumnsObj.put(fromColumnArray[0], (fromColumnArray[1]));
                                }
                            }
                        }
                        if (toColumnName != null
                                && !"".equalsIgnoreCase(String.valueOf(toColumnName))
                                && !"null".equalsIgnoreCase(String.valueOf(toColumnName))) {
                            String[] toColumnArray = String.valueOf(toColumnName).split(":");
                            if (toColumnArray != null && toColumnArray.length != 0) {
                                if (toColumnsObj != null && !toColumnsObj.isEmpty()) {
                                    if (toColumnsObj.containsKey(toColumnArray[0])) {
                                        toColumnsObj.put(toColumnArray[0],
                                                (toColumnsObj.get(toColumnArray[0]) + "," + toColumnArray[1]));
                                    } else {
                                        toColumnsObj.put(toColumnArray[0],
                                                (toColumnArray[1]));
                                    }
                                } else {
                                    toColumnsObj.put(toColumnArray[0], (toColumnArray[1]));
                                }
                            }
                        }

                    }

                }
                //System.out.println("toColumnsObj:::" + toColumnsObj);
                Set<String> toColumns = new HashSet<>();
                toColumns.addAll(columnsObj.keySet());
                if (defaultValuesObj != null && !defaultValuesObj.isEmpty()) {
                    Set<String> defaultColumns = defaultValuesObj.keySet();
                    if (defaultColumns != null
                            && !defaultColumns.isEmpty() //  && toColumns != null && !toColumns.isEmpty()
                            ) {
                        Iterator defaultColumnsItr = defaultColumns.iterator();
                        while (defaultColumnsItr.hasNext()) {
                            String defaultColName = (String) defaultColumnsItr.next();
                            if (defaultColName != null
                                    && !"".equalsIgnoreCase(String.valueOf(defaultColName))
                                    && !"null".equalsIgnoreCase(String.valueOf(defaultColName))) {
                                String[] toColumnArray = String.valueOf(defaultColName).split(":");
                                if (toColumnArray != null && toColumnArray.length != 0) {
                                    if (toColumnsObj != null && !toColumnsObj.isEmpty()) {
                                        if (toColumnsObj.containsKey(toColumnArray[0])) {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnsObj.get(toColumnArray[0]) + "," + toColumnArray[1]));
                                        } else {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnArray[1]));
                                        }
                                    } else {
                                        toColumnsObj.put(toColumnArray[0], (toColumnArray[1]));
                                    }
                                }
                            }
                        }
//                        toColumns.addAll(defaultColumns);
                    }
                }
                if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
                    Set<String> columnClauseCols = columnClauseObj.keySet();
                    if (columnClauseCols != null
                            && !columnClauseCols.isEmpty() //  && toColumns != null && !toColumns.isEmpty()
                            ) {
                        // toColumns.addAll(columnClauseCols);
                        Iterator columnClauseColsItr = columnClauseCols.iterator();
                        while (columnClauseColsItr.hasNext()) {
                            String columnClauseColName = (String) columnClauseColsItr.next();
                            if (columnClauseColName != null
                                    && !"".equalsIgnoreCase(String.valueOf(columnClauseColName))
                                    && !"null".equalsIgnoreCase(String.valueOf(columnClauseColName))) {
                                String[] toColumnArray = String.valueOf(columnClauseColName).split(":");
                                if (toColumnArray != null && toColumnArray.length != 0) {
                                    if (toColumnsObj != null && !toColumnsObj.isEmpty()) {
                                        if (toColumnsObj.containsKey(toColumnArray[0])) {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnsObj.get(toColumnArray[0]) + "," + toColumnArray[1]));
                                        } else {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnArray[1]));
                                        }
                                    } else {
                                        toColumnsObj.put(toColumnArray[0], (toColumnArray[1]));
                                    }
                                }
                            }
                        }
                    }
                }
                selectTabObj.put("toColumnsObj", toColumnsObj);
//                if (toConnection != null) {
//                    String toTableInsertQuery = generateInsertQuery((String) toOperator.get("tableName"), toColumnsList);
//                    System.out.println("insertQuery::::" + toTableInsertQuery);
//                    toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
//                }
                List<Map> toOperatorsList = (List<Map>) selectTabObj.get("toOperatorList");
                if (toOperatorsList != null && !toOperatorsList.isEmpty()) {
                    for (Map toOperatorMap : toOperatorsList) {
                        if (toOperatorMap != null && !toOperatorMap.isEmpty()) {
                            toOperator = toOperatorMap;
                            List<String> toColumnsList = new ArrayList<>();
                            toOperator = toOperatorMap;
                            String toTableName = (String) toOperator.get("tableName");
                            int totalDataCount = 0;
                            int fileDataLastIndex = 0;
                            List<Map> nonJoinOpList = fromOperatorList;
                            String fileName = "";
                            String orginalName = (String) toOperator.get("userFileName");
                            String iconType = (String) toOperator.get("iconType");
                            JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
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
                            } else if ("JSON".equalsIgnoreCase(iconType)) {
                                fileName = "V10ETLExport_" + System.currentTimeMillis() + ".json";
                            } else {
                                if (toTableName != null
                                        && !"".equalsIgnoreCase(String.valueOf(toTableName))
                                        && !"null".equalsIgnoreCase(String.valueOf(toTableName))) {
                                    String toColumnsStr = (String) toColumnsObj.get(toTableName);
                                    if (toColumnsStr != null
                                            && !"".equalsIgnoreCase(toColumnsStr)) {

                                        toColumnsList = Arrays.asList(toColumnsStr.split(","));
                                        Object toConnectionObj = getConnection(toConnObj);
                                        if (toConnectionObj instanceof Connection) {
                                            toConnection = (Connection) toConnectionObj;
                                        }
                                        if (toConnection != null) {
                                            String toTableInsertQuery = generateInsertQuery((String) toOperator.get("tableName"), toColumnsList);
                                            System.out.println("insertQuery::::" + toTableInsertQuery);
                                            toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
                                            Map columnsTypeObj = getColumnsType((String) toOperator.get("tableName"), toColumnsList, toConnection);
                                            selectTabObj.put("columnsTypeObj", columnsTypeObj);
                                        }
                                    }
                                }
                            }
                            if (!(toColumnsList != null && !toColumnsList.isEmpty())) {
                                toColumnsList.addAll(columnsObj.keySet());
                            }
                            if (joinQueryMapObj != null
                                    && !joinQueryMapObj.isEmpty()) {

                                nonJoinOpList = fromOperatorList.stream()
                                        .filter(opMap -> (opMap != null && !opMap.isEmpty()
                                        && opMap.get("tableName") != null
                                        && !"".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
                                        && !"null".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
                                        && !joinQueryMapObj.containsKey(String.valueOf(opMap.get("tableName"))))).collect(Collectors.toList());
                                try {
                                    processETLLog(sessionUserName,
                                            orgnId, "Starting extract join tables data.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                                } catch (Exception e) {
                                }
                                totalDataCount += processETLData(sessionUserName,
                                        orgnId,
                                        toConnection,
                                        fromOperatorList,
                                        toOperator,
                                        columnsObj,
                                        fromColumnsObj,
                                        tablesWhereClauseObj,
                                        defaultValuesObj,
                                        toPreparedStatement,
                                        1,
                                        1000,
                                        totalDataCount,
                                        toColumnsList,
                                        joinQueryMapObj,
                                        joinQuery,
                                        fileDataLastIndex,
                                        fileName,
                                        1,
                                        orderAndGroupByObj,
                                        columnClauseObj,
                                        selectTabObj,
                                        normalizeOptionsObj
                                );

                            } else {
                                if (nonJoinOpList != null && !nonJoinOpList.isEmpty()) {
                                    try {
                                        processETLLog(sessionUserName,
                                                orgnId, "Starting extract non-join tables data.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                                    } catch (Exception e) {
                                    }
                                    for (int i = 0; i < nonJoinOpList.size(); i++) {
                                        Map fromOperator = nonJoinOpList.get(i);
                                        if (fromOperator != null && !fromOperator.isEmpty()) {
                                            totalDataCount += processETLData(sessionUserName,
                                                    orgnId,
                                                    toConnection,
                                                    fromOperator,
                                                    toOperator,
                                                    columnsObj,
                                                    fromColumnsObj,
                                                    tablesWhereClauseObj,
                                                    defaultValuesObj,
                                                    toPreparedStatement,
                                                    1,
                                                    1000,
                                                    totalDataCount,
                                                    toColumnsList,
                                                    fileDataLastIndex,
                                                    fileName,
                                                    1,
                                                    orderAndGroupByObj,
                                                    columnClauseObj,
                                                    selectTabObj,
                                                    normalizeOptionsObj
                                            );
                                        }

                                    }
                                }

                            }

                            System.out.println("totalDataCount::::" + totalDataCount);
                            if (totalDataCount != 0 && totalDataCount > 0) {
                                String message = totalDataCount + " Row(s) successfully extracted and loaded into target system.";
                                if (fileName != null && !"".equalsIgnoreCase(fileName) && !"null".equalsIgnoreCase(fileName)) {//orginalName
                                    if (orginalName != null
                                            && !"".equalsIgnoreCase(orginalName)
                                            && !"null".equalsIgnoreCase(orginalName)) {
                                        orginalName = orginalName.replaceAll("[^.a-zA-Z0-9]", "_");
                                    }

                                    message += " <br> <a href='#' style='color:#0071c5;' onclick=downloadExportedFile('" + fileName + "',\"" + orginalName + "\") >Click here to download the " + iconType + " file</a>.";//
                                }
                                logSequenceNo += 10;
                                try {
                                    processETLLog(sessionUserName,
                                            orgnId, message, "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
                                } catch (Exception e) {
                                }
                                resultObj.put("Message", message);
                                resultObj.put("connectionFlag", "Y");

                            } else {
                                logSequenceNo += 10;
                                try {
                                    processETLLog(sessionUserName,
                                            orgnId, totalDataCount + " Row(s) successfully extracted and loaded into target system.", "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
                                } catch (Exception e) {
                                }
//                    processETLLog(httpSession,totalDataCount + " Row(s) extracted successfully", 10);
                                resultObj.put("Message", totalDataCount + " Row(s) successfully extracted and loaded into target system.");
                                resultObj.put("connectionFlag", "Y");
                            }// end 

                        }
                    }
                }
//                if (toColumnsObj != null && !toColumnsObj.isEmpty()) {
//                    List<Map> toOperatorsList = (List<Map>) selectTabObj.get("toOperatorList");
//                    for (Object toTableName : toColumnsObj.keySet()) {
//                        if (toTableName != null
//                                && !"".equalsIgnoreCase(String.valueOf(toTableName))
//                                && !"null".equalsIgnoreCase(String.valueOf(toTableName))) {
//                            String toColumnsStr = (String) toColumnsObj.get(toTableName);
//                            if (toColumnsStr != null
//                                    && !"".equalsIgnoreCase(toColumnsStr)) {
//                                List<String> toColumnsList = new ArrayList<>();
//                                toColumnsList = Arrays.asList(toColumnsStr.split(","));
//                                List<Map> matchedToOperatorsList = toOperatorsList.stream().filter(toOp
//                                        -> (toOp != null
//                                        && !toOp.isEmpty()
//                                        && toOp.get("tableName") != null
//                                        && !"".equalsIgnoreCase((String) toOp.get("tableName"))
//                                        && !"null".equalsIgnoreCase((String) toOp.get("tableName"))
//                                        && String.valueOf(toOp.get("tableName")).equalsIgnoreCase(String.valueOf(toTableName)))).collect(Collectors.toList());
//                                if (matchedToOperatorsList != null && !matchedToOperatorsList.isEmpty()) {
//                                    toOperator = matchedToOperatorsList.get(0);
//                                }
//                                if (toConnection != null) {
//                                    String toTableInsertQuery = generateInsertQuery((String) toOperator.get("tableName"), toColumnsList);
//                                    System.out.println("insertQuery::::" + toTableInsertQuery);
//                                    toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
//                                }
//
//                                int totalDataCount = 0;
//                                int fileDataLastIndex = 0;
//                                List<Map> nonJoinOpList = fromOperatorList;
//                                String fileName = "";
//                                String orginalName = (String) toOperator.get("userFileName");
//                                String iconType = (String) toOperator.get("iconType");
//
//                                if ("XLSX".equalsIgnoreCase(iconType)) {
//                                    fileName = "V10ETLExport_" + System.currentTimeMillis() + ".xlsx";
//
//                                } else if ("XLS".equalsIgnoreCase(iconType)) {
//                                    fileName = "V10ETLExport_" + System.currentTimeMillis() + ".xls";
//                                } else if ("XML".equalsIgnoreCase(iconType)) {
//                                    fileName = "V10ETLExport_" + System.currentTimeMillis() + ".xml";
//                                } else if ("CSV".equalsIgnoreCase(iconType)) {
//                                    fileName = "V10ETLExport_" + System.currentTimeMillis() + ".csv";
//                                } else if ("TXT".equalsIgnoreCase(iconType)) {
//                                    fileName = "V10ETLExport_" + System.currentTimeMillis() + ".txt";
//                                }
//
//                                if (joinQueryMapObj != null
//                                        && !joinQueryMapObj.isEmpty()) {
//
//                                    nonJoinOpList = fromOperatorList.stream()
//                                            .filter(opMap -> (opMap != null && !opMap.isEmpty()
//                                            && opMap.get("tableName") != null
//                                            && !"".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
//                                            && !"null".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
//                                            && !joinQueryMapObj.containsKey(String.valueOf(opMap.get("tableName"))))).collect(Collectors.toList());
//                                    try {
//                                        processETLLog(sessionUserName,
//                                                orgnId, "Starting extract join tables data.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
//                                    } catch (Exception e) {
//                                    }
//                                    totalDataCount += processETLData(sessionUserName,
//                                            orgnId,
//                                            toConnection,
//                                            fromOperatorList,
//                                            toOperator,
//                                            columnsObj,
//                                            fromColumnsObj,
//                                            tablesWhereClauseObj,
//                                            defaultValuesObj,
//                                            toPreparedStatement,
//                                            0,
//                                            1000,
//                                            totalDataCount,
//                                            toColumnsList,
//                                            joinQueryMapObj,
//                                            joinQuery,
//                                            fileDataLastIndex,
//                                            fileName,
//                                            1,
//                                            orderAndGroupByObj,
//                                            columnClauseObj,
//                                            selectTabObj,
//                                            normalizeOptionsObj
//                                    );
//
//                                } else {
//                                    if (nonJoinOpList != null && !nonJoinOpList.isEmpty()) {
//                                        try {
//                                            processETLLog(sessionUserName,
//                                                    orgnId, "Starting extract non-join tables data.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
//                                        } catch (Exception e) {
//                                        }
//                                        for (int i = 0; i < nonJoinOpList.size(); i++) {
//                                            Map fromOperator = nonJoinOpList.get(i);
//                                            if (fromOperator != null && !fromOperator.isEmpty()) {
//                                                totalDataCount += processETLData(sessionUserName,
//                                                        orgnId,
//                                                        toConnection,
//                                                        fromOperator,
//                                                        toOperator,
//                                                        columnsObj,
//                                                        fromColumnsObj,
//                                                        tablesWhereClauseObj,
//                                                        defaultValuesObj,
//                                                        toPreparedStatement,
//                                                        0,
//                                                        1000,
//                                                        totalDataCount,
//                                                        toColumnsList,
//                                                        fileDataLastIndex,
//                                                        fileName,
//                                                        1,
//                                                        orderAndGroupByObj,
//                                                        columnClauseObj,
//                                                        selectTabObj,
//                                                        normalizeOptionsObj
//                                                );
//                                            }
//
//                                        }
//                                    }
//
//                                }
//
//                                System.out.println("totalDataCount::::" + totalDataCount);
//                                if (totalDataCount != 0 && totalDataCount > 0) {
//                                    String message = totalDataCount + " Row(s) successfully extracted and loaded into target system.";
//                                    if (fileName != null && !"".equalsIgnoreCase(fileName) && !"null".equalsIgnoreCase(fileName)) {//orginalName
//                                        if (orginalName != null
//                                                && !"".equalsIgnoreCase(orginalName)
//                                                && !"null".equalsIgnoreCase(orginalName)) {
//                                            orginalName = orginalName.replaceAll("[^.a-zA-Z0-9]", "_");
//                                        }
//
//                                        message += " <br> <a href='#' style='color:#0071c5;' onclick=downloadExportedFile('" + fileName + "',\"" + orginalName + "\") >Click here to download the " + iconType + " file</a>.";//
//                                    }
//                                    logSequenceNo += 10;
//                                    try {
//                                        processETLLog(sessionUserName,
//                                                orgnId, message, "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
//                                    } catch (Exception e) {
//                                    }
//                                    resultObj.put("Message", message);
//                                    resultObj.put("connectionFlag", "Y");
//
//                                } else {
//                                    logSequenceNo += 10;
//                                    try {
//                                        processETLLog(sessionUserName,
//                                                orgnId, totalDataCount + " Row(s) successfully extracted and loaded into target system.", "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
//                                    } catch (Exception e) {
//                                    }
////                    processETLLog(httpSession,totalDataCount + " Row(s) extracted successfully", 10);
//                                    resultObj.put("Message", totalDataCount + " Row(s) successfully extracted and loaded into target system.");
//                                    resultObj.put("connectionFlag", "Y");
//                                }// end
//                            }
//                        }
//                    }
//                }

            }// end of columnObj if
            try {
                processETLLog(sessionUserName,
                        orgnId, "ETL Process is completed", "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception e) {
            }
        } catch (Exception e) {
            e.printStackTrace();
            resultObj.put("Message", e.getMessage());
            resultObj.put("connectionFlag", "N");
            try {
                processETLLog(sessionUserName,
                        orgnId, e.getMessage(), "ERROR", 20, "N", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }

        } finally {
            try {
                if (toPreparedStatement != null) {
                    toPreparedStatement.close();
                }

                if (toConnection != null) {
                    toConnection.close();
                }

            } catch (Exception e) {
            }
        }
        return resultObj;
    }

    public int processETLData(String sessionUserName,
            String orgnId,
            Connection toConnection,
            List<Map> fromOperatorList,
            Map toOperator,
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            //JSONObject fromColumnsObj,
            Map fromColumnsObj, // ravi etl integration
            JSONObject tablesWhereClauseObj,
            JSONObject defaultValuesObj,
            PreparedStatement toPreparedStatement,
            int start,
            int limit,
            int totalDataCount,
            List<String> toColumnsList,
            JSONObject joinQueryMapObj,
            String joinQuery,
            int fileDataLastIndex,
            String fileName,
            int logSequenceNo,
            JSONObject appendValObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj,
            JSONObject normalizeOptionsObj
    ) {
        try {
            if (selectTabObj != null
                    && !selectTabObj.isEmpty()) {
                selectTabObj.put("columnClauseObj", columnClauseObj);
                String minRows = String.valueOf(selectTabObj.get("minRows"));
                if (minRows != null
                        && !"".equalsIgnoreCase(minRows)
                        && !"null".equalsIgnoreCase(minRows)
                        && NumberUtils.isNumber(minRows)) {
                    if (logSequenceNo == 1) {
                        start = Integer.parseInt(minRows);
                    }
                }
                String maxRows = String.valueOf(selectTabObj.get("maxRows"));
                if (maxRows != null
                        && !"".equalsIgnoreCase(maxRows)
                        && !"null".equalsIgnoreCase(maxRows)
                        && !"0".equalsIgnoreCase(maxRows)
                        && NumberUtils.isNumber(maxRows)) {
                    int maxRowsCount = Integer.parseInt(maxRows);
                    if (logSequenceNo == 1) {
                        if (((maxRowsCount - start)) <= limit) {
                            limit = (maxRowsCount - start);
                        }
                    } else {
                        if (((maxRowsCount - start) + 1) <= limit) {
                            limit = (maxRowsCount - start) + 1;
                        }
                    }

                }

            }
            Map fromOperator = fromOperatorList.get(0);
            int end = start + limit - 1;
            if (start == 0) {
                end = limit;
            }
            try {
                processETLLog(sessionUserName,
                        orgnId, "Fetching from " + start + " to " + end + " record(s).", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
//                        (String) httpSession.getAttribute("ssOrgId"), "Fetching next 1000 record(s).", "INFO", logSequenceNo, "Y");
            } catch (Exception e) {
            }
            String iconType = (String) toOperator.get("iconType");
            List totalDataList = new ArrayList();
            if (limit > 0) {
//                totalDataList = getSelectedJoinColumnsData((JSONObject) fromOperator.get("connObj"),
//                        fromConnection,
//                        fromColumnsObj,
//                        start,
//                        limit,
//                        tablesWhereClauseObj, joinQueryMapObj, joinQuery,
//                        (String) httpSession.getAttribute("ssUsername"),
//                        (String) httpSession.getAttribute("ssOrgId"),
//                        appendValObj,
//                        columnClauseObj,
//                        selectTabObj,
//                        toConnection
//                );
            }

            if (totalDataList != null && !totalDataList.isEmpty()) {

                try {
                    processETLLog(sessionUserName,
                            orgnId, "Fetched " + totalDataList.size() + " record(s).", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                } catch (Exception e) {
                }

                // ravi normalising   start
                // String normalizeFlag = request.getParameter("normalizeFlag");
                if (normalizeOptionsObj != null && normalizeOptionsObj.size() > 0) {
                    String normalizeFlag = (String) normalizeOptionsObj.get("normalizeFlag");
//                    columnsObj = (JSONObject) normalizeOptionsObj.get("colsObj");
//                    Set<String> toColumnsSet = columnsObj.keySet();
//                    toColumnsList.clear();
//                    if (toColumnsSet != null && !toColumnsSet.isEmpty()) {
//                        toColumnsList.addAll(toColumnsSet);
//                    }
                    try {
                        processETLLog(sessionUserName,
                                orgnId, "Started " + normalizeFlag + " records.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                    } catch (Exception e) {
                    }
                    if (normalizeFlag != null && "normalize".equalsIgnoreCase(normalizeFlag)) {

                        totalDataList = getNoramlisedData(normalizeOptionsObj, totalDataList);
                    } else if (normalizeFlag != null && "deNormalize".equalsIgnoreCase(normalizeFlag)) {

                        totalDataList = getDeNoramlisedData(normalizeOptionsObj, totalDataList);
                    }
                }
                // ravi normalising   end

                try {
                    processETLLog(sessionUserName,
                            orgnId, "Pushing " + totalDataList.size() + " record(s) into target system(" + ((fileName != null
                            && !"".equalsIgnoreCase(fileName)
                            && !"null".equalsIgnoreCase(fileName)) ? fileName : "" + toOperator.get("tableName")) + ").", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                } catch (Exception e) {
                }
//                String iconType = (String) toOperator.get("iconType");
                int insertCount = 0;
                JSONObject fileDataObj = new JSONObject();
                if ("XLSX".equalsIgnoreCase(iconType)) {
                    FileInputStream inputStream = null;
                    XSSFWorkbook wb = null;
                    XSSFSheet sheet = null;
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
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

                    insertCount = exportingXLSXFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            sheet,
                            wb,
                            selectTabObj
                    );

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
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
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

                    insertCount = exportingXLSFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            sheet,
                            wb,
                            selectTabObj
                    );

                    if (inputStream != null) {
                        inputStream.close();
                    }
                    if (wb != null) {
                        wb.close();
                    }
                } else if ("XML".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingXMLFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else if ("CSV".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingCsvAndTxtFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
//                    exportingCsvAndTxtFileData
                } else if ("TXT".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }

                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingCsvAndTxtFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else if ("JSON".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }

                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingJSONFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else {
                    String dragType = (String) toOperator.get("dragType");
                    JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
                    if ("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1")))) {
                        Object toConnectionObj = selectTabObj.get("toConnectionObj");
//                        Object toConnectionObj = getConnection(toConnObj);
                        if (toConnectionObj instanceof JCO.Client) {
                            if ("Table".equalsIgnoreCase(dragType)) {
                                // need to call BAPI for pushing into SAP
                                insertCount = dataMigrationService.importSAPData((String) toOperator.get("tableName"),
                                        fromColumnsObj,
                                        columnsObj,
                                        totalDataList,
                                        (JCO.Client) toConnectionObj,
                                        toColumnsList,
                                        defaultValuesObj,
                                        sessionUserName,
                                        orgnId,
                                        String.valueOf(selectTabObj.get("jobId")),
                                        selectTabObj);
                            }
                        }
                    } else {
                        insertCount = dataMigrationService.importingData((String) toOperator.get("tableName"),
                                fromColumnsObj,
                                columnsObj,
                                totalDataList,
                                toPreparedStatement,
                                toColumnsList,
                                defaultValuesObj, sessionUserName,
                                orgnId,
                                String.valueOf(selectTabObj.get("jobId")),
                                selectTabObj);
                    }
                }
                if (insertCount != 0) {
                    try {
                        processETLLog(sessionUserName,
                                orgnId, "Pushed " + insertCount + " record(s) into target system.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                    } catch (Exception e) {
                    }
                }
                totalDataCount += insertCount;
                if (insertCount != 0 && insertCount >= 1000) {
                    int startIndex = (logSequenceNo * limit + 1);
                    logSequenceNo++;
                    totalDataCount = processETLData(sessionUserName,
                            orgnId,
                            //                            fromConnection,
                            toConnection,
                            fromOperatorList,
                            toOperator,
                            columnsObj,
                            fromColumnsObj,
                            tablesWhereClauseObj,
                            defaultValuesObj,
                            toPreparedStatement,
                            startIndex,
                            limit,
                            totalDataCount,
                            toColumnsList,
                            joinQueryMapObj,
                            joinQuery,
                            fileDataLastIndex,
                            fileName,
                            logSequenceNo,
                            appendValObj,
                            columnClauseObj,
                            selectTabObj,
                            normalizeOptionsObj
                    );

                } else if ("JSON".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                    File file12 = new File(filePath);
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    if (outputFile.exists()) {
                        FileOutputStream fos = new FileOutputStream(outputFile, true);
                        OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
                        BufferedWriter writer = new BufferedWriter(osw);
                        writer.append("]");
                        writer.close();
                        osw.close();
                        fos.close();
                    }
                }
            } else if ("JSON".equalsIgnoreCase(iconType)) {
                String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                File file12 = new File(filePath);
                File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                if (outputFile.exists()) {
                    FileOutputStream fos = new FileOutputStream(outputFile, true);
                    OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
                    BufferedWriter writer = new BufferedWriter(osw);
                    writer.append("]");
                    writer.close();
                    osw.close();
                    fos.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();

            try {
                processETLLog(sessionUserName,
                        orgnId, "Failed process records in ETL due to " + e.getLocalizedMessage(), "ERROR", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }
        }
        return totalDataCount;
    }

    public int processETLData(String sessionUserName,
            String orgnId,
            Connection toConnection,
            Map fromOperator,
            Map toOperator,
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            //JSONObject fromColumnsObj,
            Map fromColumnsObj, // ravi etl integration
            JSONObject tablesWhereClauseObj,
            JSONObject defaultValuesObj,
            PreparedStatement toPreparedStatement,
            int start,
            int limit,
            int totalDataCount,
            List<String> toColumnsList,
            int fileDataLastIndex,
            String fileName,
            int logSequenceNo,
            JSONObject appendValObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj,
            JSONObject normalizeOptionsObj
    ) {
        Object resultObj = null;
        try {
            String iconType = (String) toOperator.get("iconType");
            if (selectTabObj != null
                    && !selectTabObj.isEmpty()) {
                selectTabObj.put("columnClauseObj", columnClauseObj);
                String minRows = String.valueOf(selectTabObj.get("minRows"));
                if (minRows != null
                        && !"".equalsIgnoreCase(minRows)
                        && !"null".equalsIgnoreCase(minRows)
                        && NumberUtils.isNumber(minRows)) {
                    if (logSequenceNo == 1) {
                        start = Integer.parseInt(minRows);
                    }
                }
                String maxRows = String.valueOf(selectTabObj.get("maxRows"));
                if (maxRows != null
                        && !"".equalsIgnoreCase(maxRows)
                        && !"null".equalsIgnoreCase(maxRows)
                        && NumberUtils.isNumber(maxRows)) {
                    int maxRowsCount = Integer.parseInt(maxRows);
                    if (logSequenceNo == 1) {
                        if (((maxRowsCount - start)) <= limit) {
                            limit = (maxRowsCount - start);
                        }
                    } else {
                        if (((maxRowsCount - start) + 1) <= limit) {
                            limit = (maxRowsCount - start) + 1;
                        }
                    }
                }

            }
            int end = start + limit - 1;
            if (start == 0) {
                end = limit;
            }
            try {
                processETLLog(sessionUserName,
                        orgnId, "Fetching from " + start + " to " + end + " record(s).", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception e) {
            }
            List totalDataList = new ArrayList();
            if (limit > 0) {
                JSONObject fileObj = (JSONObject) fromOperator.get("connObj");
                if (fileObj != null && !fileObj.isEmpty()) {
                    String fileType = (String) fileObj.get("fileType");
                    String fromFileName = (String) fileObj.get("fileName");
                    String filePath = (String) fileObj.get("filePath");
                    List<String> columnList = new ArrayList<>();

//                    if (columnList != null && !columnList.isEmpty()) {
//                        columnList = columnList.stream().map(header ->(header.replaceAll("\\s","_")))
//                                .collect(Collectors.toList());
//                    }
//                    HttpSession httpSession,
//                            String filepath
//                    , 
//            List<String> columnList,
//                    int startIndex,
//                    int limit,
//                    int endIndex
                    if (".xls".equalsIgnoreCase(fileType) || ".xlsx".equalsIgnoreCase(fileType)) {
                        columnList = getHeadersOfImportedFile(filePath, fileType);
                        totalDataList = readExcel(filePath, columnList, start, limit, end, fromFileName);
                    } else if (".CSV".equalsIgnoreCase(fileType)
                            || ".TXT".equalsIgnoreCase(fileType)
                            || ".json".equalsIgnoreCase(fileType)) {
                        columnList = getHeadersOfImportedFile(filePath, fileType);
                        totalDataList = readCSV(filePath, columnList, start, limit, end, fromFileName, fileType);
                    } else if (".xml".equalsIgnoreCase(fileType)) {
                        totalDataList = readXML(filePath, columnList, start, limit, end, fromFileName);
                    }
                }

//                totalDataList = getSelectedColumnsData((JSONObject) fromOperator.get("connObj"),
//                        fromConnection,
//                        fromColumnsObj,
//                        start,
//                        limit,
//                        tablesWhereClauseObj, (String) httpSession.getAttribute("ssUsername"),
//                        (String) httpSession.getAttribute("ssOrgId"), appendValObj, columnClauseObj, selectTabObj, toConnection);
            }

            if (totalDataList != null && !totalDataList.isEmpty()) {
                try {
                    processETLLog(sessionUserName,
                            orgnId, "Fetched " + totalDataList.size() + " record(s).", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                } catch (Exception e) {
                }

                // ravi normalising   start
                // String normalizeFlag = request.getParameter("normalizeFlag");
                if (normalizeOptionsObj != null && normalizeOptionsObj.size() > 0) {
                    String normalizeFlag = (String) normalizeOptionsObj.get("normalizeFlag");
//                    columnsObj = (JSONObject) normalizeOptionsObj.get("colsObj");
//                    Set<String> toColumnsSet = columnsObj.keySet();
//                    toColumnsList.clear();
//                    if (toColumnsSet != null && !toColumnsSet.isEmpty()) {
//                        toColumnsList.addAll(toColumnsSet);
//                    }
                    try {
                        processETLLog(sessionUserName,
                                orgnId, "Started " + normalizeFlag + " records.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                    } catch (Exception e) {
                    }
                    if (normalizeFlag != null && "normalize".equalsIgnoreCase(normalizeFlag)) {

                        totalDataList = getNoramlisedData(normalizeOptionsObj, totalDataList);
                    } else if (normalizeFlag != null && "deNormalize".equalsIgnoreCase(normalizeFlag)) {

                        totalDataList = getDeNoramlisedData(normalizeOptionsObj, totalDataList);
                    }
                }
                // ravi normalising   end

                try {
                    processETLLog(sessionUserName,
                            orgnId, "Pushing " + totalDataList.size() + " record(s) into target system(" + ((fileName != null
                            && !"".equalsIgnoreCase(fileName)
                            && !"null".equalsIgnoreCase(fileName)) ? fileName : "" + toOperator.get("tableName")) + ").", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                } catch (Exception e) {
                }
                int insertCount = 0;

                if ("XLSX".equalsIgnoreCase(iconType)) {
                    FileInputStream inputStream = null;
                    XSSFWorkbook wb = null;
                    XSSFSheet sheet = null;
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
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

                    insertCount = exportingXLSXFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            sheet,
                            wb,
                            selectTabObj
                    );

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
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
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

                    insertCount = exportingXLSFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            sheet,
                            wb,
                            selectTabObj
                    );

                    if (inputStream != null) {
                        inputStream.close();
                    }
                    if (wb != null) {
                        wb.close();
                    }
                } else if ("XML".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingXMLFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else if ("CSV".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingCsvAndTxtFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
//                    exportingCsvAndTxtFileData
                } else if ("TXT".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }

                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingCsvAndTxtFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else if ("JSON".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }

                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingJSONFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else {
                    String dragType = (String) toOperator.get("dragType");
                    JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
                    if ("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1")))) {
                        Object toConnectionObj = selectTabObj.get("toConnectionObj");
//                        Object toConnectionObj = getConnection(toConnObj);
                        if (toConnectionObj instanceof JCO.Client) {
                            if ("Table".equalsIgnoreCase(dragType)) {
                                // need to call BAPI for pushing into SAP
                                insertCount = dataMigrationService.importSAPData((String) toOperator.get("tableName"),
                                        fromColumnsObj,
                                        columnsObj,
                                        totalDataList,
                                        (JCO.Client) toConnectionObj,
                                        toColumnsList,
                                        defaultValuesObj,
                                        sessionUserName,
                                        orgnId,
                                        String.valueOf(selectTabObj.get("jobId")),
                                        selectTabObj);
                            }
                        }
                    } else {
                        insertCount = dataMigrationService.importingData((String) toOperator.get("tableName"),
                                fromColumnsObj,
                                columnsObj,
                                totalDataList,
                                toPreparedStatement,
                                toColumnsList,
                                defaultValuesObj,
                                sessionUserName,
                                orgnId,
                                String.valueOf(selectTabObj.get("jobId")),
                                selectTabObj);
                    }
                }
                if (insertCount != 0) {
                    try {
                        processETLLog(sessionUserName,
                                orgnId, "Pushed " + insertCount + " record(s) into target system(" + ((fileName != null
                                && !"".equalsIgnoreCase(fileName)
                                && !"null".equalsIgnoreCase(fileName)) ? fileName : "" + toOperator.get("tableName")) + ").", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                    } catch (Exception e) {
                    }
                }

                totalDataCount += insertCount;
                if (insertCount != 0 && insertCount >= 1000) {
                    int startIndex = (logSequenceNo * limit + 1);

                    logSequenceNo++;
                    totalDataCount = processETLData(sessionUserName,
                            orgnId,
                            toConnection,
                            fromOperator,
                            toOperator,
                            columnsObj,
                            fromColumnsObj,
                            tablesWhereClauseObj,
                            defaultValuesObj,
                            toPreparedStatement,
                            startIndex,
                            //(logSequenceNo * limit + 1),
                            limit,
                            totalDataCount,
                            toColumnsList,
                            fileDataLastIndex,
                            fileName,
                            logSequenceNo,
                            appendValObj,
                            columnClauseObj,
                            selectTabObj,
                            normalizeOptionsObj
                    );

                } else if ("JSON".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                    File file12 = new File(filePath);
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    if (outputFile.exists()) {
                        FileOutputStream fos = new FileOutputStream(outputFile, true);
                        OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
                        BufferedWriter writer = new BufferedWriter(osw);
                        writer.append("]");
                        writer.close();
                        osw.close();
                        fos.close();
                    }
                }
            } else if ("JSON".equalsIgnoreCase(iconType)) {
                String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                File file12 = new File(filePath);
                File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                if (outputFile.exists()) {
                    FileOutputStream fos = new FileOutputStream(outputFile, true);
                    OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
                    BufferedWriter writer = new BufferedWriter(osw);
                    writer.append("]");
                    writer.close();
                    osw.close();
                    fos.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            try {
                processETLLog(sessionUserName,
                        orgnId, e.getMessage(), "ERROR", 20, "Y", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }
        }
        return totalDataCount;
    }

    public int processETLData(String sessionUserName,
            String orgnId,
            Connection toConnection,
            Map fromOperator,
            Map toOperator,
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            //JSONObject fromColumnsObj,
            Map fromColumnsObj, // ravi etl integration
            JSONObject tablesWhereClauseObj,
            JSONObject defaultValuesObj,
            PreparedStatement toPreparedStatement,
            int start,
            int limit,
            int totalDataCount,
            List<String> toColumnsList,
            int fileDataLastIndex,
            String fileName,
            int logSequenceNo,
            JSONObject appendValObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj,
            JSONObject normalizeOptionsObj,
            Object fileLoadObj,
            List<String> columnList,
            LineNumberReader lineNumberReader
    ) {
        Object resultObj = null;
        try {
            String iconType = (String) toOperator.get("iconType");
            if (selectTabObj != null
                    && !selectTabObj.isEmpty()) {
                selectTabObj.put("columnClauseObj", columnClauseObj);
                String minRows = String.valueOf(selectTabObj.get("minRows"));
                if (minRows != null
                        && !"".equalsIgnoreCase(minRows)
                        && !"null".equalsIgnoreCase(minRows)
                        && NumberUtils.isNumber(minRows)) {
                    if (logSequenceNo == 1) {
                        start = Integer.parseInt(minRows);
                    }
                }
                String maxRows = String.valueOf(selectTabObj.get("maxRows"));
                if (maxRows != null
                        && !"".equalsIgnoreCase(maxRows)
                        && !"null".equalsIgnoreCase(maxRows)
                        && NumberUtils.isNumber(maxRows)) {
                    int maxRowsCount = Integer.parseInt(maxRows);
                    if (logSequenceNo == 1) {
                        if (((maxRowsCount - start)) <= limit) {
                            limit = (maxRowsCount - start);
                        }
                    } else {
                        if (((maxRowsCount - start) + 1) <= limit) {
                            limit = (maxRowsCount - start) + 1;
                        }
                    }
                }

            }
            int end = start + limit - 1;
            if (start == 0) {
                end = limit;
            }
            try {
                processETLLog(sessionUserName,
                        orgnId, "Fetching from " + start + " to " + end + " record(s).", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception e) {
            }
            List totalDataList = new ArrayList();
            if (limit > 0) {
                JSONObject fileObj = (JSONObject) fromOperator.get("connObj");
                if (fileObj != null && !fileObj.isEmpty()) {
                    String fileType = (String) fileObj.get("fileType");
                    String fromFileName = (String) fileObj.get("fileName");
                    String filePath = (String) fileObj.get("filePath");

                    if (".xls".equalsIgnoreCase(fileType) || ".xlsx".equalsIgnoreCase(fileType)) {
                        //totalDataList = readExcel(filePath, columnList, start, limit, end, fromFileName, (Workbook) fileLoadObj);
                        // ravi etl integration start
                        JSONObject excelInfoObj = readExcel(filePath, columnList, start, limit, end, fromFileName, (Workbook) fileLoadObj, selectTabObj);
                        totalDataList = (List) excelInfoObj.get("dataList");
                        selectTabObj.put("noOfSheets", excelInfoObj.get("noOfSheets"));
                        selectTabObj.put("sheetNo", excelInfoObj.get("sheetNo"));
                        // ravi ravi etl integration end 
                    } else if (".CSV".equalsIgnoreCase(fileType)
                            || ".TXT".equalsIgnoreCase(fileType)
                            || ".json".equalsIgnoreCase(fileType)) {
                        totalDataList = readCSV(filePath, columnList, start, limit, end, fromFileName, fileType, (CSVReader) fileLoadObj, lineNumberReader);
                    } else if (".xml".equalsIgnoreCase(fileType)) {
                        totalDataList = readXML(filePath, columnList, start, limit, end, fromFileName, (Document) fileLoadObj);
                    }
                }

//                totalDataList = getSelectedColumnsData((JSONObject) fromOperator.get("connObj"),
//                        fromConnection,
//                        fromColumnsObj,
//                        start,
//                        limit,
//                        tablesWhereClauseObj, (String) httpSession.getAttribute("ssUsername"),
//                        (String) httpSession.getAttribute("ssOrgId"), appendValObj, columnClauseObj, selectTabObj, toConnection);
            }

            if (totalDataList != null && !totalDataList.isEmpty()) {
                try {
                    processETLLog(sessionUserName,
                            orgnId, "Fetched " + totalDataList.size() + " record(s).", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                } catch (Exception e) {
                }

                // ravi normalising   start
                // String normalizeFlag = request.getParameter("normalizeFlag");
                if (normalizeOptionsObj != null && normalizeOptionsObj.size() > 0) {
                    String normalizeFlag = (String) normalizeOptionsObj.get("normalizeFlag");
//                    columnsObj = (JSONObject) normalizeOptionsObj.get("colsObj");
//                    Set<String> toColumnsSet = columnsObj.keySet();
//                    toColumnsList.clear();
//                    if (toColumnsSet != null && !toColumnsSet.isEmpty()) {
//                        toColumnsList.addAll(toColumnsSet);
//                    }
                    try {
                        processETLLog(sessionUserName,
                                orgnId, "Started " + normalizeFlag + " records.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                    } catch (Exception e) {
                    }
                    if (normalizeFlag != null && "normalize".equalsIgnoreCase(normalizeFlag)) {

                        totalDataList = getNoramlisedData(normalizeOptionsObj, totalDataList);
                    } else if (normalizeFlag != null && "deNormalize".equalsIgnoreCase(normalizeFlag)) {

                        totalDataList = getDeNoramlisedData(normalizeOptionsObj, totalDataList);
                    }
                }
                // ravi normalising   end

                try {
                    processETLLog(sessionUserName,
                            orgnId, "Pushing " + totalDataList.size() + " record(s) into target system(" + ((fileName != null
                            && !"".equalsIgnoreCase(fileName)
                            && !"null".equalsIgnoreCase(fileName)) ? fileName : "" + toOperator.get("tableName")) + ").", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                } catch (Exception e) {
                }
                int insertCount = 0;

                if ("XLSX".equalsIgnoreCase(iconType)) {
                    FileInputStream inputStream = null;
                    XSSFWorkbook wb = null;
                    XSSFSheet sheet = null;
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
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

                    insertCount = exportingXLSXFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            sheet,
                            wb,
                            selectTabObj
                    );

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
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
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

                    insertCount = exportingXLSFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            sheet,
                            wb,
                            selectTabObj
                    );

                    if (inputStream != null) {
                        inputStream.close();
                    }
                    if (wb != null) {
                        wb.close();
                    }
                } else if ("XML".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingXMLFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else if ("CSV".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingCsvAndTxtFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
//                    exportingCsvAndTxtFileData
                } else if ("TXT".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }

                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingCsvAndTxtFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else if ("JSON".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                    File file12 = new File(filePath);
                    if (!file12.exists()) {
                        file12.mkdirs();
                    }

                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    insertCount = exportingJSONFileData(toOperator,
                            fromColumnsObj,
                            columnsObj,
                            totalDataList,
                            toPreparedStatement,
                            toColumnsList,
                            defaultValuesObj,
                            filePath,
                            fileName,
                            fileDataLastIndex,
                            selectTabObj
                    );
                    fileDataLastIndex += insertCount;
                } else {
                    String dragType = (String) toOperator.get("dragType");
                    JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
                    if ("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1")))) {
                        Object toConnectionObj = selectTabObj.get("toConnectionObj");
//                        Object toConnectionObj = getConnection(toConnObj);
                        if (toConnectionObj instanceof JCO.Client) {
                            if ("Table".equalsIgnoreCase(dragType)) {
                                // need to call BAPI for pushing into SAP
                                insertCount = dataMigrationService.importSAPData((String) toOperator.get("tableName"),
                                        fromColumnsObj,
                                        columnsObj,
                                        totalDataList,
                                        (JCO.Client) toConnectionObj,
                                        toColumnsList,
                                        defaultValuesObj,
                                        sessionUserName,
                                        orgnId,
                                        String.valueOf(selectTabObj.get("jobId")),
                                        selectTabObj);
                            }
                        }
                    } else {
                        insertCount = dataMigrationService.importingData((String) toOperator.get("tableName"),
                                fromColumnsObj,
                                columnsObj,
                                totalDataList,
                                toPreparedStatement,
                                toColumnsList,
                                defaultValuesObj,
                                sessionUserName,
                                orgnId,
                                String.valueOf(selectTabObj.get("jobId")),
                                selectTabObj);
                    }
                }
                if (insertCount != 0) {
                    try {
                        processETLLog(sessionUserName,
                                orgnId, "Pushed " + insertCount + " record(s) into target system(" + ((fileName != null
                                && !"".equalsIgnoreCase(fileName)
                                && !"null".equalsIgnoreCase(fileName)) ? fileName : "" + toOperator.get("tableName")) + ").", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                    } catch (Exception e) {
                    }
                }

                totalDataCount += insertCount;
                if (insertCount != 0 && insertCount >= 1000) {
                    int startIndex = (logSequenceNo * limit + 1);

                    logSequenceNo++;
                    totalDataCount = processETLData(sessionUserName,
                            orgnId,
                            toConnection,
                            fromOperator,
                            toOperator,
                            columnsObj,
                            fromColumnsObj,
                            tablesWhereClauseObj,
                            defaultValuesObj,
                            toPreparedStatement,
                            startIndex,
                            //(logSequenceNo * limit + 1),
                            limit,
                            totalDataCount,
                            toColumnsList,
                            fileDataLastIndex,
                            fileName,
                            logSequenceNo,
                            appendValObj,
                            columnClauseObj,
                            selectTabObj,
                            normalizeOptionsObj,
                            fileLoadObj,
                            columnList,
                            lineNumberReader
                    );

                } else if ("JSON".equalsIgnoreCase(iconType)) {
                    String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                    File file12 = new File(filePath);
                    File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                    if (outputFile.exists()) {
                        FileOutputStream fos = new FileOutputStream(outputFile, true);
                        OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
                        BufferedWriter writer = new BufferedWriter(osw);
                        writer.append("]");
                        writer.close();
                        osw.close();
                        fos.close();
                    }
                }
            } else if ("JSON".equalsIgnoreCase(iconType)) {
                String filePath = etlFilePath+"ETL_EXPORT_" + File.separator + sessionUserName;
                File file12 = new File(filePath);
                File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                if (outputFile.exists()) {
                    FileOutputStream fos = new FileOutputStream(outputFile, true);
                    OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
                    BufferedWriter writer = new BufferedWriter(osw);
                    writer.append("]");
                    writer.close();
                    osw.close();
                    fos.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            try {
                processETLLog(sessionUserName,
                        orgnId, e.getMessage(), "ERROR", 20, "Y", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }
        }
        return totalDataCount;
    }

    public List readExcel(
            String filepath,
            List<String> columnList,
            int startIndex,
            int limit,
            int endIndex, String fileName) {

        FileInputStream fis = null;

//        System.out.println("Start Date And Time :::" + new Date());
        List dataList = new ArrayList();
        int rowVal = 1;
        try {
            if (true) {
                //  fis = new FileInputStream(new File(filepath));

                Workbook workBook = null;
                Sheet sheet = null;

                String fileExtension = filepath.substring(filepath.lastIndexOf(".") + 1, filepath.length());
//                System.out.println("fileExtension:::" + fileExtension);
                if (fileExtension != null && "xls".equalsIgnoreCase(fileExtension)) {
                    workBook = WorkbookFactory.create(new File(filepath));
                    sheet = (HSSFSheet) workBook.getSheetAt(0);
                } else {
//                    System.out.println("Before::::" + new Date());
                    workBook = WorkbookFactory.create(new File(filepath));
//                    System.out.println("After::fileInputStream::" + new Date());
                    sheet = (XSSFSheet) workBook.getSheetAt(0);
                }
                int lastRowNo = sheet.getLastRowNum();
//                System.out.println("lastRowNo::::" + lastRowNo);
                int firstRowNo = sheet.getFirstRowNum();
//                System.out.println("firstRowNo::::" + firstRowNo);
                int rowCount = lastRowNo - firstRowNo;
//                System.out.println("rowCount:::::" + rowCount);

                String strToDateCol = "";

                rowVal = startIndex;
                if (rowVal == 0) {
                    rowVal = 1;

                }
                if (endIndex > lastRowNo) {
                    endIndex = lastRowNo;
                }
                for (int i = rowVal; i <= endIndex; i++) {
                    Row row = sheet.getRow(i);
                    if (row != null) {
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
                                            if (cellValue != null && !"".equalsIgnoreCase(cellValue) && !"null".equalsIgnoreCase(cellValue)) {
                                                dataObject.put(fileName + ":" + columnList.get(cellIndex), cellValue);
                                            } else {
                                                dataObject.put(fileName + ":" + columnList.get(cellIndex), "");
                                            }

                                            break;
                                        case Cell.CELL_TYPE_BOOLEAN:
//                                rowObj.put(header, hSSFCell.getBooleanCellValue());
                                            break;
                                        case Cell.CELL_TYPE_NUMERIC:

                                            if (HSSFDateUtil.isCellDateFormatted(cell)) {
                                                String cellDateString = "";
                                                Date cellDate = cell.getDateCellValue();
                                                if ((cellDate.getYear() + 1900) == 1899 && (cellDate.getMonth() + 1) == 12 && (cellDate.getDate()) == 31) {
                                                    cellDateString = (cellDate.getHours()) + ":" + (cellDate.getMinutes()) + ":" + (cellDate.getSeconds());
//                                                    System.out.println("cellDateString :: "+cellDateString);
                                                } else {
                                                    cellDateString = (cellDate.getYear() + 1900) + "-" + (cellDate.getMonth() + 1) + "-" + (cellDate.getDate());
                                                }

//                                                String cellDateString = (cellDate.getYear() + 1900) + "-" + (cellDate.getMonth() + 1) + "-" + (cellDate.getDate());
                                                dataObject.put(fileName + ":" + columnList.get(cellIndex), cellDateString);

                                            } else {
                                                String cellvalStr = NumberToTextConverter.toText(cell.getNumericCellValue());
                                                dataObject.put(fileName + ":" + columnList.get(cellIndex), cellvalStr);
                                            }
                                            break;
                                        case Cell.CELL_TYPE_BLANK:
                                            dataObject.put(fileName + ":" + columnList.get(cellIndex), "");
                                            break;
                                    }

                                } else {
                                    dataObject.put(fileName + ":" + columnList.get(cellIndex), "");
                                }
                            } catch (Exception e) {
                                dataObject.put(fileName + ":" + columnList.get(cellIndex), "");
                                continue;
                            }

                        }// end of row cell loop
                        dataList.add(dataObject);
                    }

                }// row end

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

    public JSONObject readExcel(
            String filepath,
            List<String> columnList,
            int startIndex,
            int limit,
            int endIndex,
            String fileName,
            Workbook workBook,
            JSONObject excelInfoObj // ravi updated code
    ) {

        FileInputStream fis = null;
        int sheetNo = (excelInfoObj.get("sheetNo") != null) ? (int) excelInfoObj.get("sheetNo") : 0;

//        System.out.println("Start Date And Time :::" + new Date());
        List dataList = new ArrayList();
        int rowVal = 1;
        try {
            if (true) {
                //  fis = new FileInputStream(new File(filepath));

                int noOfSheets = workBook.getNumberOfSheets();
                excelInfoObj.put("noOfSheets", noOfSheets - 1);
                Sheet sheet = null;

                String fileExtension = filepath.substring(filepath.lastIndexOf(".") + 1, filepath.length());
//                System.out.println("fileExtension:::" + fileExtension);

                if (fileExtension != null && "xls".equalsIgnoreCase(fileExtension)) {
                    sheet = (HSSFSheet) workBook.getSheetAt(sheetNo);
                } else {
                    sheet = (XSSFSheet) workBook.getSheetAt(sheetNo);
                }
                int lastRowNo = sheet.getLastRowNum();
//                System.out.println("lastRowNo::::" + lastRowNo);
                int firstRowNo = sheet.getFirstRowNum();
//                System.out.println("firstRowNo::::" + firstRowNo);
                int rowCount = lastRowNo - firstRowNo;
//                System.out.println("rowCount:::::" + rowCount);

                String strToDateCol = "";

                rowVal = startIndex;
                if (rowVal == 0) {
                    rowVal = 1;

                }
                if (endIndex >= lastRowNo) {
                    endIndex = lastRowNo;

                    sheetNo = (excelInfoObj.get("sheetNo") != null) ? (int) excelInfoObj.get("sheetNo") : sheetNo;
                    sheetNo = sheetNo + 1;
                }
                excelInfoObj.put("sheetNo", sheetNo);
                excelInfoObj.put("lastRowNo", lastRowNo);

                for (int i = rowVal; i <= endIndex; i++) {
                    Row row = sheet.getRow(i);
                    if (row != null) {
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
                                            if (cellValue != null && !"".equalsIgnoreCase(cellValue) && !"null".equalsIgnoreCase(cellValue)) {
                                                dataObject.put(fileName + ":" + columnList.get(cellIndex), cellValue);
                                            } else {
                                                dataObject.put(fileName + ":" + columnList.get(cellIndex), "");
                                            }

                                            break;
                                        case Cell.CELL_TYPE_BOOLEAN:
//                                rowObj.put(header, hSSFCell.getBooleanCellValue());
                                            break;
                                        case Cell.CELL_TYPE_NUMERIC:

                                            if (HSSFDateUtil.isCellDateFormatted(cell)) {
                                                String cellDateString = "";
                                                Date cellDate = cell.getDateCellValue();
                                                if ((cellDate.getYear() + 1900) == 1899 && (cellDate.getMonth() + 1) == 12 && (cellDate.getDate()) == 31) {
                                                    cellDateString = (cellDate.getHours()) + ":" + (cellDate.getMinutes()) + ":" + (cellDate.getSeconds());
//                                                    System.out.println("cellDateString :: "+cellDateString);
                                                } else {
                                                    cellDateString = (cellDate.getYear() + 1900) + "-" + (cellDate.getMonth() + 1) + "-" + (cellDate.getDate());
                                                }

//                                                String cellDateString = (cellDate.getYear() + 1900) + "-" + (cellDate.getMonth() + 1) + "-" + (cellDate.getDate());
                                                dataObject.put(fileName + ":" + columnList.get(cellIndex), cellDateString);

                                            } else {
                                                String cellvalStr = NumberToTextConverter.toText(cell.getNumericCellValue());
                                                dataObject.put(fileName + ":" + columnList.get(cellIndex), cellvalStr);
                                            }
                                            break;
                                        case Cell.CELL_TYPE_BLANK:
                                            dataObject.put(fileName + ":" + columnList.get(cellIndex), "");
                                            break;
                                    }

                                } else {
                                    dataObject.put(fileName + ":" + columnList.get(cellIndex), "");
                                }
                            } catch (Exception e) {
                                dataObject.put(fileName + ":" + columnList.get(cellIndex), "");
                                continue;
                            }

                        }// end of row cell loop
                        dataList.add(dataObject);
                    }

                }// row end

                // return result1;
                if (fis != null) {
                    fis.close();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();

        }
        excelInfoObj.put("dataList", dataList);

        return excelInfoObj;
    }

//    public List readExcel(
//            String filepath,
//            List<String> columnList,
//            int startIndex,
//            int limit,
//            int endIndex,
//            String fileName,
//            Workbook workBook
//    ) {
//
//        FileInputStream fis = null;
//
////        System.out.println("Start Date And Time :::" + new Date());
//        List dataList = new ArrayList();
//        int rowVal = 1;
//        try {
//            if (true) {
//                //  fis = new FileInputStream(new File(filepath));
//
//                Sheet sheet = null;
//
//                String fileExtension = filepath.substring(filepath.lastIndexOf(".") + 1, filepath.length());
////                System.out.println("fileExtension:::" + fileExtension);
//                if (fileExtension != null && "xls".equalsIgnoreCase(fileExtension)) {
//                    sheet = (HSSFSheet) workBook.getSheetAt(0);
//                } else {
//                    sheet = (XSSFSheet) workBook.getSheetAt(0);
//                }
//                int lastRowNo = sheet.getLastRowNum();
////                System.out.println("lastRowNo::::" + lastRowNo);
//                int firstRowNo = sheet.getFirstRowNum();
////                System.out.println("firstRowNo::::" + firstRowNo);
//                int rowCount = lastRowNo - firstRowNo;
////                System.out.println("rowCount:::::" + rowCount);
//
//                String strToDateCol = "";
//
//                rowVal = startIndex;
//                if (rowVal == 0) {
//                    rowVal = 1;
//
//                }
//                if (endIndex > lastRowNo) {
//                    endIndex = lastRowNo;
//                }
//                for (int i = rowVal; i <= endIndex; i++) {
//                    Row row = sheet.getRow(i);
//                    if (row != null) {
//                        JSONObject dataObject = new JSONObject();
//                        dataObject.put("totalrecords", rowCount);
//                        for (int cellIndex = 0; cellIndex < row.getLastCellNum(); cellIndex++) {
//
//                            try {
////                            System.out.println("cellIndex::::" + cellIndex);
//                                Cell cell = row.getCell(cellIndex);
//                                if (cell != null) {
//                                    switch (cell.getCellType()) {
//                                        case Cell.CELL_TYPE_STRING:
//                                            String cellValue = cell.getStringCellValue();
//                                            if (cellValue != null && !"".equalsIgnoreCase(cellValue) && !"null".equalsIgnoreCase(cellValue)) {
//                                                dataObject.put(fileName + ":" + columnList.get(cellIndex), cellValue);
//                                            } else {
//                                                dataObject.put(fileName + ":" + columnList.get(cellIndex), "");
//                                            }
//
//                                            break;
//                                        case Cell.CELL_TYPE_BOOLEAN:
////                                rowObj.put(header, hSSFCell.getBooleanCellValue());
//                                            break;
//                                        case Cell.CELL_TYPE_NUMERIC:
//
//                                            if (HSSFDateUtil.isCellDateFormatted(cell)) {
//                                                Date cellDate = cell.getDateCellValue();
//                                                String cellDateString = (cellDate.getYear() + 1900) + "-" + (cellDate.getMonth() + 1) + "-" + (cellDate.getDate());
//                                                dataObject.put(fileName + ":" + columnList.get(cellIndex), cellDateString);
//
//                                            } else {
//                                                String cellvalStr = NumberToTextConverter.toText(cell.getNumericCellValue());
//                                                dataObject.put(fileName + ":" + columnList.get(cellIndex), cellvalStr);
//                                            }
//                                            break;
//                                        case Cell.CELL_TYPE_BLANK:
//                                            dataObject.put(fileName + ":" + columnList.get(cellIndex), "");
//                                            break;
//                                    }
//
//                                } else {
//                                    dataObject.put(fileName + ":" + columnList.get(cellIndex), "");
//                                }
//                            } catch (Exception e) {
//                                dataObject.put(fileName + ":" + columnList.get(cellIndex), "");
//                                continue;
//                            }
//
//                        }// end of row cell loop
//                        dataList.add(dataObject);
//                    }
//
//                }// row end
//
//                // return result1;
//                if (fis != null) {
//                    fis.close();
//                }
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//
//        }
//
//        return dataList;
//    }
    public List readCSV(
            String filepath,
            List<String> columnList,
            int startIndex,
            int limit,
            int endIndex, String fileName, String fileType) {
        FileInputStream fis = null;
//        System.out.println("Start Date And Time :::" + new Date());
        List dataList = new ArrayList();
        int rowVal = 1;
        try {
            int rowCount = 0;
            //  fis = new FileInputStream(new File(filepath));
//            char columnSeparator = '\t';
            CsvParserSettings settings = new CsvParserSettings();
            settings.detectFormatAutomatically();

            CsvParser parser = new CsvParser(settings);
            List<String[]> rows = parser.parseAll(new File(filepath));

            // if you want to see what it detected
            CsvFormat format = parser.getDetectedFormat();
            char columnSeparator = format.getDelimiter();

            if (".json".equalsIgnoreCase(fileType)) {
                columnSeparator = ',';
            }
            String fileExtension = filepath.substring(filepath.lastIndexOf(".") + 1, filepath.length());
//            System.out.println("fileExtension:::" + fileExtension);

            int stmt = 1;
            String strToDateCol = "";
            // need to write logic for extraction from File
            CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(filepath), "UTF8"), columnSeparator);
            LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(filepath));
            lineNumberReader.skip(Long.MAX_VALUE);
            long totalRecords = lineNumberReader.getLineNumber();
            if (totalRecords != 0) {
                totalRecords = totalRecords - 1;
            }
//            System.out.println("totalRecords:::" + totalRecords);
//             CSVReader  reader = new CSVReader(new FileReader(filepath),'\t');

            rowVal = 1;

//            if (recordstartindex != null
//                    && !"".equalsIgnoreCase(recordstartindex)
//                    && !"null".equalsIgnoreCase(recordstartindex)
//                    && !"0".equalsIgnoreCase(recordstartindex)) {
//                rowVal = Integer.parseInt(recordstartindex);
//            }
////            int endIndex = (int)totalRecords + 1;
////            if (recordendindex != null
////                    && !"".equalsIgnoreCase(recordendindex)
////                    && !"null".equalsIgnoreCase(recordendindex)
////                    && Integer.parseInt(recordendindex) <= rowCount) {
////                endIndex = Integer.parseInt(recordendindex);
////            }
            int skipLines = 0;
            if (limit != 0) {
                skipLines = startIndex;
            }
            if (skipLines == 0) {
                String[] headers = reader.readNext();
            }
            reader.skip(skipLines);

            String[] nextLine;
            int rowsCount = 1;
            while ((nextLine = reader.readNext()) != null) {// no of rows
                if (limit >= rowsCount) {
                    rowsCount++;

                    JSONObject dataObject = new JSONObject();
                    dataObject.put("totalrecords", totalRecords);
                    for (int j = 0; j < columnList.size(); j++) {
                        try {
                            int cellIndex = j;
                            if (cellIndex <= (nextLine.length - 1)) {
                                String token = nextLine[cellIndex];
                                if (token != null
                                        && !"".equalsIgnoreCase(token)) {
                                    try {
                                        dataObject.put(fileName + ":" + columnList.get(j), token);
                                    } catch (Exception e) {
                                        dataObject.put(fileName + ":" + columnList.get(j), "");
                                        continue;
                                    }
                                } else {
                                    dataObject.put(fileName + ":" + columnList.get(j), "");
                                }
                            } else {
                                dataObject.put(fileName + ":" + columnList.get(j), "");
                            }
                        } catch (Exception e) {
                            dataObject.put(fileName + ":" + columnList.get(j), "");
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

    public List readCSV(
            String filepath,
            List<String> columnList,
            int startIndex,
            int limit,
            int endIndex, String fileName, String fileType,
            CSVReader reader,
            LineNumberReader lineNumberReader) {
        FileInputStream fis = null;
//        System.out.println("Start Date And Time :::" + new Date());
        List dataList = new ArrayList();
        int rowVal = 1;
        try {
            int rowCount = 0;
            //  fis = new FileInputStream(new File(filepath));
//            char columnSeparator = '\t';
            CsvParserSettings settings = new CsvParserSettings();
            settings.detectFormatAutomatically();

            CsvParser parser = new CsvParser(settings);
            List<String[]> rows = parser.parseAll(new File(filepath));

            // if you want to see what it detected
            CsvFormat format = parser.getDetectedFormat();
            char columnSeparator = format.getDelimiter();

            if (".json".equalsIgnoreCase(fileType)) {
                columnSeparator = ',';
            }
            String fileExtension = filepath.substring(filepath.lastIndexOf(".") + 1, filepath.length());
//            System.out.println("fileExtension:::" + fileExtension);

            int stmt = 1;
            String strToDateCol = "";
            // need to write logic for extraction from File
//            CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(filepath), "UTF8"), columnSeparator);
//            LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(filepath));
            lineNumberReader.skip(Long.MAX_VALUE);
            long totalRecords = lineNumberReader.getLineNumber();
            if (totalRecords != 0) {
                totalRecords = totalRecords - 1;
            }
//            System.out.println("totalRecords:::" + totalRecords);
//             CSVReader  reader = new CSVReader(new FileReader(filepath),'\t');

            rowVal = 1;

//            if (recordstartindex != null
//                    && !"".equalsIgnoreCase(recordstartindex)
//                    && !"null".equalsIgnoreCase(recordstartindex)
//                    && !"0".equalsIgnoreCase(recordstartindex)) {
//                rowVal = Integer.parseInt(recordstartindex);
//            }
////            int endIndex = (int)totalRecords + 1;
////            if (recordendindex != null
////                    && !"".equalsIgnoreCase(recordendindex)
////                    && !"null".equalsIgnoreCase(recordendindex)
////                    && Integer.parseInt(recordendindex) <= rowCount) {
////                endIndex = Integer.parseInt(recordendindex);
////            }
            int skipLines = 0;
            if (limit != 0) {
                skipLines = startIndex;
            }
            if (skipLines == 0) {
                String[] headers = reader.readNext();
            }
            reader.skip(skipLines);

            String[] nextLine;
            int rowsCount = 1;
            while ((nextLine = reader.readNext()) != null) {// no of rows
                if (limit >= rowsCount) {
                    rowsCount++;

                    JSONObject dataObject = new JSONObject();
                    dataObject.put("totalrecords", totalRecords);
                    for (int j = 0; j < columnList.size(); j++) {
                        try {
                            int cellIndex = j;
                            if (cellIndex <= (nextLine.length - 1)) {
                                String token = nextLine[cellIndex];
                                if (token != null
                                        && !"".equalsIgnoreCase(token)) {
                                    try {
                                        dataObject.put(fileName + ":" + columnList.get(j), token);
                                    } catch (Exception e) {
                                        dataObject.put(fileName + ":" + columnList.get(j), "");
                                        continue;
                                    }
                                } else {
                                    dataObject.put(fileName + ":" + columnList.get(j), "");
                                }
                            } else {
                                dataObject.put(fileName + ":" + columnList.get(j), "");
                            }
                        } catch (Exception e) {
                            dataObject.put(fileName + ":" + columnList.get(j), "");
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

    public List readXML(
            String filepath,
            List<String> columnList,
            int startIndex,
            int limit,
            int endIndex, String fileName) {
        FileInputStream fis = null;
        List dataList = new ArrayList();
        try {
            int rowCount = 0;
            String fileExtension = filepath.substring(filepath.lastIndexOf(".") + 1, filepath.length());
//            System.out.println("fileExtension:::" + fileExtension);

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
                        //            NodeList nList = (NodeList) xpath.evaluate("/PiLog_Data_Export/Item",
                        document,
                        XPathConstants.NODESET);

                if (dataNodeList != null && dataNodeList.getLength() != 0) {
                    rowCount = dataNodeList.getLength();

//                    Node headerNode = dataNodeList.item(0);
//                    if (headerNode.getNodeType() == Node.ELEMENT_NODE) {
//                        NodeList headerChildNodeList = headerNode.getChildNodes();
//                        int index = 0;
//                        for (int i = 0; i < headerChildNodeList.getLength(); i++) {// Columns
//                            Node childNode = headerChildNodeList.item(i);
//                            if (childNode != null
//                                    && childNode.getNodeType() == Node.ELEMENT_NODE) {
//                                headerData.put(childNode.getNodeName(), i);
//
//                            }
//                        }// end of columns loop
//
//                    }
                    if (startIndex != 0) {
                        endIndex++;
                    }
                    if (endIndex > rowCount) {
                        endIndex = rowCount;
                    }
                    for (int temp = startIndex; temp < endIndex; temp++) {// Rows
                        Node node = dataNodeList.item(temp);

                        if (node != null && node.getNodeType() == Node.ELEMENT_NODE) {
                            JSONObject dataObject = new JSONObject();
                            dataObject.put("totalrecords", rowCount);
                            NodeList childNodeList = node.getChildNodes();
                            for (int j = 0; j < childNodeList.getLength(); j++) {
                                try {
                                    Node childNode = childNodeList.item(j);
//                                    int childNodeIndex = j;
//                                    int nodeListLength = childNodeList.getLength();
                                    if (childNode != null) {
                                        if (childNode != null
                                                && childNode.getNodeType() == Node.ELEMENT_NODE) {
                                            try {
                                                if (childNode.getTextContent() != null
                                                        && !"".equalsIgnoreCase(childNode.getTextContent())
                                                        && !"null".equalsIgnoreCase(childNode.getTextContent())) {
                                                    dataObject.put(fileName + ":" + childNode.getNodeName(), childNode.getTextContent());

                                                } else {
                                                    dataObject.put(fileName + ":" + childNode.getNodeName(), "");
                                                }

                                            } catch (Exception e) {
                                                dataObject.put(fileName + ":" + childNode.getNodeName(), "");
                                                continue;
                                            }
                                            // Need to set the Data

                                        }
                                    }
                                } catch (Exception e) {

                                    continue;
                                }

                            }// column list loop
                            dataList.add(dataObject);
                        }

                    }// end of rows loop

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

    public List readXML(
            String filepath,
            List<String> columnList,
            int startIndex,
            int limit,
            int endIndex, String fileName, Document document) {
        FileInputStream fis = null;
        List dataList = new ArrayList();
        try {
            int rowCount = 0;
            String fileExtension = filepath.substring(filepath.lastIndexOf(".") + 1, filepath.length());
//            System.out.println("fileExtension:::" + fileExtension);

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
                        //            NodeList nList = (NodeList) xpath.evaluate("/PiLog_Data_Export/Item",
                        document,
                        XPathConstants.NODESET);

                if (dataNodeList != null && dataNodeList.getLength() != 0) {
                    rowCount = dataNodeList.getLength();

//                    Node headerNode = dataNodeList.item(0);
//                    if (headerNode.getNodeType() == Node.ELEMENT_NODE) {
//                        NodeList headerChildNodeList = headerNode.getChildNodes();
//                        int index = 0;
//                        for (int i = 0; i < headerChildNodeList.getLength(); i++) {// Columns
//                            Node childNode = headerChildNodeList.item(i);
//                            if (childNode != null
//                                    && childNode.getNodeType() == Node.ELEMENT_NODE) {
//                                headerData.put(childNode.getNodeName(), i);
//
//                            }
//                        }// end of columns loop
//
//                    }
                    if (startIndex != 0) {
                        endIndex++;
                    }
                    if (endIndex > rowCount) {
                        endIndex = rowCount;
                    }
                    for (int temp = startIndex; temp < endIndex; temp++) {// Rows
                        Node node = dataNodeList.item(temp);

                        if (node != null && node.getNodeType() == Node.ELEMENT_NODE) {
                            JSONObject dataObject = new JSONObject();
                            dataObject.put("totalrecords", rowCount);
                            NodeList childNodeList = node.getChildNodes();
                            for (int j = 0; j < childNodeList.getLength(); j++) {
                                try {
                                    Node childNode = childNodeList.item(j);
//                                    int childNodeIndex = j;
//                                    int nodeListLength = childNodeList.getLength();
                                    if (childNode != null) {
                                        if (childNode != null
                                                && childNode.getNodeType() == Node.ELEMENT_NODE) {
                                            try {
                                                if (childNode.getTextContent() != null
                                                        && !"".equalsIgnoreCase(childNode.getTextContent())
                                                        && !"null".equalsIgnoreCase(childNode.getTextContent())) {
                                                    dataObject.put(fileName + ":" + childNode.getNodeName(), childNode.getTextContent());

                                                } else {
                                                    dataObject.put(fileName + ":" + childNode.getNodeName(), "");
                                                }

                                            } catch (Exception e) {
                                                dataObject.put(fileName + ":" + childNode.getNodeName(), "");
                                                continue;
                                            }
                                            // Need to set the Data

                                        }
                                    }
                                } catch (Exception e) {

                                    continue;
                                }

                            }// column list loop
                            dataList.add(dataObject);
                        }

                    }// end of rows loop

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

    // for scheduling SAP 
    public JSONObject processETLData(String sessionUserName,
            String orgnId,
            JCO.Client fromJCOConnection,
            Connection toConnection,
            JCO.Client toJCOConnection,
            List<Map> fromOperatorList,
            Map toOperator,
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            JSONObject tablesWhereClauseObj,
            JSONObject defaultValuesObj,
            JSONObject joinQueryMapObj,
            String joinQuery,
            JSONObject orderByObj,
            String groupByQuery,
            String nativeSQL,
            JSONObject appendValObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj,
            JSONObject normalizeOptionsObj
    ) {
        JSONObject resultObj = new JSONObject();
        Connection fromConnection = null;
        PreparedStatement toPreparedStatement = null;
        try {
            JSONObject orderAndGroupByObj = new JSONObject();
            orderAndGroupByObj.put("orderByObj", orderByObj);
            orderAndGroupByObj.put("groupByQuery", groupByQuery);
            orderAndGroupByObj.put("nativeSQL", nativeSQL);
            int logSequenceNo = 10;
            //JSONObject fromColumnsObj = new JSONObject();			
            Map fromColumnsObj = new LinkedHashMap(); // ravi etl integration
            //JSONObject toColumnsObj = new JSONObject();			
            Map toColumnsObj = new LinkedHashMap(); // ravi etl integration
            if (columnsObj != null && !columnsObj.isEmpty()) {
                logSequenceNo += 10;
                try {
                    processETLLog(sessionUserName,
                            orgnId, "Reading the transformations rules.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                } catch (Exception e) {
                }
                for (Object toColumnName : columnsObj.keySet()) {
                    if (toColumnName != null && columnsObj.get(toColumnName) != null) {
                        String fromColumnStr = (String) columnsObj.get(toColumnName);
                        if (fromColumnStr != null
                                && !"".equalsIgnoreCase(fromColumnStr)
                                && !"null".equalsIgnoreCase(fromColumnStr)) {
                            String[] fromColumnArray = fromColumnStr.split(":");
                            if (fromColumnArray != null && fromColumnArray.length != 0) {
                                if (fromColumnsObj != null && !fromColumnsObj.isEmpty()) {
                                    if (fromColumnsObj.containsKey(fromColumnArray[0])) {
                                        fromColumnsObj.put(fromColumnArray[0],
                                                (fromColumnsObj.get(fromColumnArray[0]) + "," + fromColumnArray[1]));
                                    } else {
                                        fromColumnsObj.put(fromColumnArray[0],
                                                (fromColumnArray[1]));
                                    }
                                } else {
                                    fromColumnsObj.put(fromColumnArray[0], (fromColumnArray[1]));
                                }
                            }
                        }
                        if (toColumnName != null
                                && !"".equalsIgnoreCase(String.valueOf(toColumnName))
                                && !"null".equalsIgnoreCase(String.valueOf(toColumnName))) {
                            String[] toColumnArray = String.valueOf(toColumnName).split(":");
                            if (toColumnArray != null && toColumnArray.length != 0) {
                                if (toColumnsObj != null && !toColumnsObj.isEmpty()) {
                                    if (toColumnsObj.containsKey(toColumnArray[0])) {
                                        toColumnsObj.put(toColumnArray[0],
                                                (toColumnsObj.get(toColumnArray[0]) + "," + toColumnArray[1]));
                                    } else {
                                        toColumnsObj.put(toColumnArray[0],
                                                (toColumnArray[1]));
                                    }
                                } else {
                                    toColumnsObj.put(toColumnArray[0], (toColumnArray[1]));
                                }
                            }
                        }

                    }

                }
                //System.out.println("toColumnsObj:::" + toColumnsObj);
                Set<String> toColumns = new HashSet<>();
                toColumns.addAll(columnsObj.keySet());
                if (defaultValuesObj != null && !defaultValuesObj.isEmpty()) {
                    Set<String> defaultColumns = defaultValuesObj.keySet();
                    if (defaultColumns != null
                            && !defaultColumns.isEmpty() //  && toColumns != null && !toColumns.isEmpty()
                            ) {
                        Iterator defaultColumnsItr = defaultColumns.iterator();
                        while (defaultColumnsItr.hasNext()) {
                            String defaultColName = (String) defaultColumnsItr.next();
                            if (defaultColName != null
                                    && !"".equalsIgnoreCase(String.valueOf(defaultColName))
                                    && !"null".equalsIgnoreCase(String.valueOf(defaultColName))) {
                                String[] toColumnArray = String.valueOf(defaultColName).split(":");
                                if (toColumnArray != null && toColumnArray.length != 0) {
                                    if (toColumnsObj != null && !toColumnsObj.isEmpty()) {
                                        if (toColumnsObj.containsKey(toColumnArray[0])) {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnsObj.get(toColumnArray[0]) + "," + toColumnArray[1]));
                                        } else {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnArray[1]));
                                        }
                                    } else {
                                        toColumnsObj.put(toColumnArray[0], (toColumnArray[1]));
                                    }
                                }
                            }
                        }
//                        toColumns.addAll(defaultColumns);
                    }
                }
                if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
                    Set<String> columnClauseCols = columnClauseObj.keySet();
                    if (columnClauseCols != null
                            && !columnClauseCols.isEmpty() //  && toColumns != null && !toColumns.isEmpty()
                            ) {
                        // toColumns.addAll(columnClauseCols);
                        Iterator columnClauseColsItr = columnClauseCols.iterator();
                        while (columnClauseColsItr.hasNext()) {
                            String columnClauseColName = (String) columnClauseColsItr.next();
                            if (columnClauseColName != null
                                    && !"".equalsIgnoreCase(String.valueOf(columnClauseColName))
                                    && !"null".equalsIgnoreCase(String.valueOf(columnClauseColName))) {
                                String[] toColumnArray = String.valueOf(columnClauseColName).split(":");
                                if (toColumnArray != null && toColumnArray.length != 0) {
                                    if (toColumnsObj != null && !toColumnsObj.isEmpty()) {
                                        if (toColumnsObj.containsKey(toColumnArray[0])) {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnsObj.get(toColumnArray[0]) + "," + toColumnArray[1]));
                                        } else {
                                            toColumnsObj.put(toColumnArray[0],
                                                    (toColumnArray[1]));
                                        }
                                    } else {
                                        toColumnsObj.put(toColumnArray[0], (toColumnArray[1]));
                                    }
                                }
                            }
                        }
                    }
                }
                selectTabObj.put("toColumnsObj", toColumnsObj);
//                if (toConnection != null) {
//                    String toTableInsertQuery = generateInsertQuery((String) toOperator.get("tableName"), toColumnsList);
//                    System.out.println("insertQuery::::" + toTableInsertQuery);
//                    toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
//                }

                List<Map> toOperatorsList = (List<Map>) selectTabObj.get("toOperatorList");
                if (toOperatorsList != null && !toOperatorsList.isEmpty()) {
                    for (Map toOperatorMap : toOperatorsList) {
                        if (toOperatorMap != null && !toOperatorMap.isEmpty()) {
                            List<String> toColumnsList = new ArrayList<>();
                            toOperator = toOperatorMap;
                            String toTableName = (String) toOperator.get("tableName");
                            int totalDataCount = 0;
                            int fileDataLastIndex = 0;
                            List<Map> nonJoinOpList = fromOperatorList;
                            String fileName = "";
                            String orginalName = (String) toOperator.get("userFileName");
                            String iconType = (String) toOperator.get("iconType");
                            JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
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
                            } else if ("JSON".equalsIgnoreCase(iconType)) {
                                fileName = "V10ETLExport_" + System.currentTimeMillis() + ".json";
                            } else {
                                if (toTableName != null
                                        && !"".equalsIgnoreCase(String.valueOf(toTableName))
                                        && !"null".equalsIgnoreCase(String.valueOf(toTableName))) {
                                    String toColumnsStr = (String) toColumnsObj.get(toTableName);
                                    if (toColumnsStr != null
                                            && !"".equalsIgnoreCase(toColumnsStr)) {

                                        toColumnsList = Arrays.asList(toColumnsStr.split(","));
                                        Object toConnectionObj = getConnection(toConnObj);
                                        if (toConnectionObj instanceof Connection) {
                                            toConnection = (Connection) toConnectionObj;
                                        }
                                        if (toConnection != null) {
                                            String toTableInsertQuery = generateInsertQuery((String) toOperator.get("tableName"), toColumnsList);
                                            System.out.println("insertQuery::::" + toTableInsertQuery);
                                            toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
                                            Map columnsTypeObj = getColumnsType((String) toOperator.get("tableName"), toColumnsList, toConnection);
                                            selectTabObj.put("columnsTypeObj", columnsTypeObj);
                                        }
                                    }
                                }
                            }
                            if (!(toColumnsList != null && !toColumnsList.isEmpty())) {
                                toColumnsList.addAll(columnsObj.keySet());
                            }
                            // need to get SAP data and create the tables in Current V10 DB
                            JSONObject sapColumnsObj = getSelectedSAPTablesData(sessionUserName,
                                    orgnId,
                                    fromJCOConnection,
                                    fromOperatorList,
                                    columnsObj,
                                    fromColumnsObj,
                                    tablesWhereClauseObj,
                                    defaultValuesObj,
                                    orderByObj,
                                    groupByQuery,
                                    joinQueryMapObj
                            );
                            if (sapColumnsObj != null && !sapColumnsObj.isEmpty()) {
                                JSONObject dbFromColumnsObj = (JSONObject) sapColumnsObj.get("fromColumnsObj");
                                JSONObject tableNameObj = (JSONObject) sapColumnsObj.get("tableNameObj");
                                fromConnection = DriverManager.getConnection(dbURL, userName, password);
//                    String dbDriver, String dbURL,String userName,String password,String connName
                                JSONObject currentV10DBObj = new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName, password, "Current_V10");
                                fromOperatorList = fromOperatorList.stream().map(formOp -> {
                                    try {
                                        formOp.put("CONNECTION_NAME", currentV10DBObj.get("CONNECTION_NAME"));
                                        formOp.put("CONN_DB_NAME", currentV10DBObj.get("CONN_DB_NAME"));
                                        formOp.put("CONN_CUST_COL1", currentV10DBObj.get("CONN_CUST_COL1"));
                                        formOp.put("connObj", currentV10DBObj);
                                        if (tableNameObj != null && !tableNameObj.isEmpty()
                                                && tableNameObj.containsKey(String.valueOf(formOp.get("tableName")))) {
                                            formOp.put("tableName", tableNameObj.get(String.valueOf(formOp.get("tableName"))));
                                        }

                                    } catch (Exception e) {
                                    }
                                    return formOp;
                                }).collect(Collectors.toList());
                                JSONObject tempJoinQueryMapObj = new JSONObject();
                                JSONObject tempTablesWhereClauseObj = new JSONObject();
                                JSONObject tempColumnsObj = new JSONObject();
                                tempColumnsObj.putAll(columnsObj);
                                for (Object sapTableName : tableNameObj.keySet()) {
                                    if (joinQueryMapObj != null
                                            && !joinQueryMapObj.isEmpty()
                                            && joinQueryMapObj.containsKey(sapTableName)) {
                                        tempJoinQueryMapObj.put(String.valueOf(tableNameObj.get(sapTableName)), joinQueryMapObj.get(sapTableName));
                                        joinQueryMapObj.remove(sapTableName);

                                    }
                                    if (tablesWhereClauseObj != null
                                            && !tablesWhereClauseObj.isEmpty()
                                            && tablesWhereClauseObj.containsKey(sapTableName)) {
                                        tempTablesWhereClauseObj.put(String.valueOf(tableNameObj.get(sapTableName)), joinQueryMapObj.get(sapTableName));
                                        tablesWhereClauseObj.remove(sapTableName);
                                    }
                                    if (columnsObj != null && !columnsObj.isEmpty()) {

                                        for (Object sourceCol : columnsObj.keySet()) {
                                            String keyValueContains = sapTableName + ":";

                                            if (columnsObj.get(sourceCol) != null
                                                    && !"".equalsIgnoreCase(String.valueOf(columnsObj.get(sourceCol)))
                                                    && !"null".equalsIgnoreCase(String.valueOf(columnsObj.get(sourceCol)))
                                                    && String.valueOf(columnsObj.get(sourceCol)).contains(keyValueContains)) {
                                                String modifiedKey = (String) sourceCol;
                                                if (String.valueOf(sourceCol).contains(keyValueContains)) {
                                                    tempColumnsObj.remove(sourceCol);
                                                    modifiedKey = String.valueOf(sourceCol).replace(keyValueContains, String.valueOf(tableNameObj.get(sapTableName)) + ":");
                                                }
                                                tempColumnsObj.put(modifiedKey,
                                                        String.valueOf(columnsObj.get(sourceCol))
                                                                .replace(keyValueContains, String.valueOf(tableNameObj.get(sapTableName)) + ":"));
                                            }
                                        }
                                    }

                                    if (joinQuery != null
                                            && !"".equalsIgnoreCase(joinQuery)
                                            && !"null".equalsIgnoreCase(joinQuery)) {
                                        String keyValueContains = " " + sapTableName + " ";
                                        joinQuery = joinQuery.replaceAll(keyValueContains, " " + String.valueOf(tableNameObj.get(sapTableName)) + " ");
                                        keyValueContains = sapTableName + "[.]";
                                        joinQuery = joinQuery.replaceAll(keyValueContains, String.valueOf(tableNameObj.get(sapTableName)) + ".");
                                    }
                                    if (fileName != null
                                            && !"".equalsIgnoreCase(fileName)
                                            && !"null".equalsIgnoreCase(fileName)
                                            && toColumnsList != null && !toColumnsList.isEmpty()) {
                                        String keyValueContains = sapTableName + ":";
                                        toColumnsList = toColumnsList.stream().map(toColName -> (String.valueOf(toColName).replace(keyValueContains, String.valueOf(tableNameObj.get(sapTableName)) + ":")))
                                                .collect(Collectors.toList());
                                    }
                                }// end loop
                                if (tempColumnsObj != null && !tempColumnsObj.isEmpty()) {
                                    columnsObj = tempColumnsObj;
                                }
                                if (tempJoinQueryMapObj != null && !tempJoinQueryMapObj.isEmpty()) {
                                    joinQueryMapObj.putAll(tempJoinQueryMapObj);
                                }
                                if (tempTablesWhereClauseObj != null && !tempTablesWhereClauseObj.isEmpty()) {
                                    tablesWhereClauseObj.putAll(tempTablesWhereClauseObj);
                                }

                                if (joinQueryMapObj != null
                                        && !joinQueryMapObj.isEmpty()) {

                                    nonJoinOpList = fromOperatorList.stream()
                                            .filter(opMap -> (opMap != null && !opMap.isEmpty()
                                            && opMap.get("tableName") != null
                                            && !"".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
                                            && !"null".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
                                            && !joinQueryMapObj.containsKey(String.valueOf(opMap.get("tableName"))))).collect(Collectors.toList());
                                    try {
                                        processETLLog(sessionUserName,
                                                orgnId, "Starting extract join tables data.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                                    } catch (Exception e) {
                                    }
                                    totalDataCount += processETLData(sessionUserName,
                                            orgnId,
                                            fromConnection,
                                            toConnection,
                                            fromOperatorList,
                                            toOperator,
                                            columnsObj,
                                            dbFromColumnsObj,
                                            tablesWhereClauseObj,
                                            defaultValuesObj,
                                            toPreparedStatement,
                                            1,
                                            1000,
                                            totalDataCount,
                                            toColumnsList,
                                            joinQueryMapObj,
                                            joinQuery,
                                            fileDataLastIndex,
                                            fileName,
                                            1,
                                            orderAndGroupByObj,
                                            columnClauseObj,
                                            selectTabObj,
                                            normalizeOptionsObj
                                    );

                                } else {
                                    if (nonJoinOpList != null && !nonJoinOpList.isEmpty()) {
                                        try {
                                            processETLLog(sessionUserName,
                                                    orgnId, "Starting extract non-join tables data.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                                        } catch (Exception e) {
                                        }
                                        for (int i = 0; i < nonJoinOpList.size(); i++) {
                                            Map fromOperator = nonJoinOpList.get(i);
                                            if (fromOperator != null && !fromOperator.isEmpty()) {
                                                totalDataCount += processETLData(sessionUserName,
                                                        orgnId,
                                                        fromConnection,
                                                        toConnection,
                                                        fromOperator,
                                                        toOperator,
                                                        columnsObj,
                                                        dbFromColumnsObj,
                                                        tablesWhereClauseObj,
                                                        defaultValuesObj,
                                                        toPreparedStatement,
                                                        1,
                                                        1000,
                                                        totalDataCount,
                                                        toColumnsList,
                                                        fileDataLastIndex,
                                                        fileName,
                                                        1,
                                                        orderAndGroupByObj,
                                                        columnClauseObj,
                                                        selectTabObj,
                                                        normalizeOptionsObj
                                                );
                                            }

                                        }
                                    }

                                }
                                System.out.println("totalDataCount::::" + totalDataCount);
                                if (totalDataCount != 0 && totalDataCount > 0) {
                                    String message = totalDataCount + " Row(s) successfully extracted and loaded into target system.";
                                    if (fileName != null && !"".equalsIgnoreCase(fileName) && !"null".equalsIgnoreCase(fileName)) {
                                        message += " <br> <a href='#' style='color:#0071c5;' onclick=downloadExportedFile('" + fileName + "') >Click here to download the " + iconType + " file</a>.";//
                                    }
                                    logSequenceNo += 10;
                                    try {
                                        processETLLog(sessionUserName,
                                                orgnId, message, "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
                                    } catch (Exception e) {
                                    }
                                    resultObj.put("Message", message);
                                    resultObj.put("connectionFlag", "Y");

                                } else {
                                    logSequenceNo += 10;
                                    try {
                                        processETLLog(sessionUserName,
                                                orgnId, totalDataCount + " Row(s) successfully extracted and loaded into target system.", "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
                                    } catch (Exception e) {
                                    }
//                    processETLLog(httpSession,totalDataCount + " Row(s) extracted successfully", 10);
                                    resultObj.put("Message", totalDataCount + " Row(s) successfully extracted and loaded into target system.");
                                    resultObj.put("connectionFlag", "Y");
                                }
                                try {
                                    dropTemptables(tableNameObj);
                                } catch (Exception e) {
                                }
                            } else {
                                try {
                                    processETLLog(sessionUserName,
                                            orgnId, totalDataCount + " Row(s) successfully extracted and loaded into target system.", "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
                                } catch (Exception e) {
                                }
//                    processETLLog(httpSession,totalDataCount + " Row(s) extracted successfully", 10);
                                resultObj.put("Message", totalDataCount + " Row(s) successfully extracted and loaded into target system.");
                                resultObj.put("connectionFlag", "Y");
                            }//end

                        }
                    }
                }

//                if (toColumnsObj != null && !toColumnsObj.isEmpty()) {
//                    List<Map> toOperatorsList = (List<Map>) selectTabObj.get("toOperatorList");
//                    for (Object toTableName : toColumnsObj.keySet()) {
//                        if (toTableName != null
//                                && !"".equalsIgnoreCase(String.valueOf(toTableName))
//                                && !"null".equalsIgnoreCase(String.valueOf(toTableName))) {
//                            String toColumnsStr = (String) toColumnsObj.get(toTableName);
//                            if (toColumnsStr != null
//                                    && !"".equalsIgnoreCase(toColumnsStr)) {
//                                List<String> toColumnsList = new ArrayList<>();
//                                toColumnsList = Arrays.asList(toColumnsStr.split(","));
//                                List<Map> matchedToOperatorsList = toOperatorsList.stream().filter(toOp
//                                        -> (toOp != null
//                                        && !toOp.isEmpty()
//                                        && toOp.get("tableName") != null
//                                        && !"".equalsIgnoreCase((String) toOp.get("tableName"))
//                                        && !"null".equalsIgnoreCase((String) toOp.get("tableName"))
//                                        && String.valueOf(toOp.get("tableName")).equalsIgnoreCase(String.valueOf(toTableName)))).collect(Collectors.toList());
//                                if (matchedToOperatorsList != null && !matchedToOperatorsList.isEmpty()) {
//                                    toOperator = matchedToOperatorsList.get(0);
//                                }
//                                if (toConnection != null) {
//                                    String toTableInsertQuery = generateInsertQuery((String) toOperator.get("tableName"), toColumnsList);
//                                    System.out.println("insertQuery::::" + toTableInsertQuery);
//                                    toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
//                                }
//
//                                int totalDataCount = 0;
//                                int fileDataLastIndex = 0;
//                                List<Map> nonJoinOpList = fromOperatorList;
//                                String fileName = "";
//                                String iconType = (String) toOperator.get("iconType");
//                                if ("XLSX".equalsIgnoreCase(iconType)) {
//                                    fileName = "V10ETLExport_" + System.currentTimeMillis() + ".xlsx";
//                                } else if ("XLS".equalsIgnoreCase(iconType)) {
//                                    fileName = "V10ETLExport_" + System.currentTimeMillis() + ".xls";
//                                } else if ("XML".equalsIgnoreCase(iconType)) {
//                                    fileName = "V10ETLExport_" + System.currentTimeMillis() + ".xml";
//                                } else if ("CSV".equalsIgnoreCase(iconType)) {
//                                    fileName = "V10ETLExport_" + System.currentTimeMillis() + ".csv";
//                                } else if ("TXT".equalsIgnoreCase(iconType)) {
//                                    fileName = "V10ETLExport_" + System.currentTimeMillis() + ".txt";
//                                }
//                                // need to get SAP data and create the tables in Current V10 DB
//                                JSONObject sapColumnsObj = getSelectedSAPTablesData(sessionUserName,
//                                        orgnId,
//                                        fromJCOConnection,
//                                        fromOperatorList,
//                                        columnsObj,
//                                        fromColumnsObj,
//                                        tablesWhereClauseObj,
//                                        defaultValuesObj,
//                                        orderByObj,
//                                        groupByQuery,
//                                        joinQueryMapObj
//                                );
//                                if (sapColumnsObj != null && !sapColumnsObj.isEmpty()) {
//                                    JSONObject dbFromColumnsObj = (JSONObject) sapColumnsObj.get("fromColumnsObj");
//                                    JSONObject tableNameObj = (JSONObject) sapColumnsObj.get("tableNameObj");
//                                    fromConnection = DriverManager.getConnection(dbURL, userName, password);
////                    String dbDriver, String dbURL,String userName,String password,String connName
//                                    JSONObject currentV10DBObj = new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName, password, "Current_V10");
//                                    fromOperatorList = fromOperatorList.stream().map(formOp -> {
//                                        try {
//                                            formOp.put("CONNECTION_NAME", currentV10DBObj.get("CONNECTION_NAME"));
//                                            formOp.put("CONN_DB_NAME", currentV10DBObj.get("CONN_DB_NAME"));
//                                            formOp.put("CONN_CUST_COL1", currentV10DBObj.get("CONN_CUST_COL1"));
//                                            formOp.put("connObj", currentV10DBObj);
//                                            if (tableNameObj != null && !tableNameObj.isEmpty()
//                                                    && tableNameObj.containsKey(String.valueOf(formOp.get("tableName")))) {
//                                                formOp.put("tableName", tableNameObj.get(String.valueOf(formOp.get("tableName"))));
//                                            }
//
//                                        } catch (Exception e) {
//                                        }
//                                        return formOp;
//                                    }).collect(Collectors.toList());
//                                    JSONObject tempJoinQueryMapObj = new JSONObject();
//                                    JSONObject tempTablesWhereClauseObj = new JSONObject();
//                                    JSONObject tempColumnsObj = new JSONObject();
//                                    tempColumnsObj.putAll(columnsObj);
//                                    for (Object sapTableName : tableNameObj.keySet()) {
//                                        if (joinQueryMapObj != null
//                                                && !joinQueryMapObj.isEmpty()
//                                                && joinQueryMapObj.containsKey(sapTableName)) {
//                                            tempJoinQueryMapObj.put(String.valueOf(tableNameObj.get(sapTableName)), joinQueryMapObj.get(sapTableName));
//                                            joinQueryMapObj.remove(sapTableName);
//
//                                        }
//                                        if (tablesWhereClauseObj != null
//                                                && !tablesWhereClauseObj.isEmpty()
//                                                && tablesWhereClauseObj.containsKey(sapTableName)) {
//                                            tempTablesWhereClauseObj.put(String.valueOf(tableNameObj.get(sapTableName)), joinQueryMapObj.get(sapTableName));
//                                            tablesWhereClauseObj.remove(sapTableName);
//                                        }
//                                        if (columnsObj != null && !columnsObj.isEmpty()) {
//
//                                            for (Object sourceCol : columnsObj.keySet()) {
//                                                String keyValueContains = sapTableName + ":";
//
//                                                if (columnsObj.get(sourceCol) != null
//                                                        && !"".equalsIgnoreCase(String.valueOf(columnsObj.get(sourceCol)))
//                                                        && !"null".equalsIgnoreCase(String.valueOf(columnsObj.get(sourceCol)))
//                                                        && String.valueOf(columnsObj.get(sourceCol)).contains(keyValueContains)) {
//                                                    String modifiedKey = (String) sourceCol;
//                                                    if (String.valueOf(sourceCol).contains(keyValueContains)) {
//                                                        tempColumnsObj.remove(sourceCol);
//                                                        modifiedKey = String.valueOf(sourceCol).replace(keyValueContains, String.valueOf(tableNameObj.get(sapTableName)) + ":");
//                                                    }
//                                                    tempColumnsObj.put(modifiedKey,
//                                                            String.valueOf(columnsObj.get(sourceCol))
//                                                                    .replace(keyValueContains, String.valueOf(tableNameObj.get(sapTableName)) + ":"));
//                                                }
//                                            }
//                                        }
//
//                                        if (joinQuery != null
//                                                && !"".equalsIgnoreCase(joinQuery)
//                                                && !"null".equalsIgnoreCase(joinQuery)) {
//                                            String keyValueContains = " " + sapTableName + " ";
//                                            joinQuery = joinQuery.replaceAll(keyValueContains, " " + String.valueOf(tableNameObj.get(sapTableName)) + " ");
//                                            keyValueContains = sapTableName + "[.]";
//                                            joinQuery = joinQuery.replaceAll(keyValueContains, String.valueOf(tableNameObj.get(sapTableName)) + ".");
//                                        }
//                                        if (fileName != null
//                                                && !"".equalsIgnoreCase(fileName)
//                                                && !"null".equalsIgnoreCase(fileName)
//                                                && toColumnsList != null && !toColumnsList.isEmpty()) {
//                                            String keyValueContains = sapTableName + ":";
//                                            toColumnsList = toColumnsList.stream().map(toColName -> (String.valueOf(toColName).replace(keyValueContains, String.valueOf(tableNameObj.get(sapTableName)) + ":")))
//                                                    .collect(Collectors.toList());
//                                        }
//                                    }// end loop
//                                    if (tempColumnsObj != null && !tempColumnsObj.isEmpty()) {
//                                        columnsObj = tempColumnsObj;
//                                    }
//                                    if (tempJoinQueryMapObj != null && !tempJoinQueryMapObj.isEmpty()) {
//                                        joinQueryMapObj.putAll(tempJoinQueryMapObj);
//                                    }
//                                    if (tempTablesWhereClauseObj != null && !tempTablesWhereClauseObj.isEmpty()) {
//                                        tablesWhereClauseObj.putAll(tempTablesWhereClauseObj);
//                                    }
//
//                                    if (joinQueryMapObj != null
//                                            && !joinQueryMapObj.isEmpty()) {
//
//                                        nonJoinOpList = fromOperatorList.stream()
//                                                .filter(opMap -> (opMap != null && !opMap.isEmpty()
//                                                && opMap.get("tableName") != null
//                                                && !"".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
//                                                && !"null".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
//                                                && !joinQueryMapObj.containsKey(String.valueOf(opMap.get("tableName"))))).collect(Collectors.toList());
//                                        try {
//                                            processETLLog(sessionUserName,
//                                                    orgnId, "Starting extract join tables data.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
//                                        } catch (Exception e) {
//                                        }
//                                        totalDataCount += processETLData(sessionUserName,
//                                                orgnId,
//                                                fromConnection,
//                                                toConnection,
//                                                fromOperatorList,
//                                                toOperator,
//                                                columnsObj,
//                                                dbFromColumnsObj,
//                                                tablesWhereClauseObj,
//                                                defaultValuesObj,
//                                                toPreparedStatement,
//                                                0,
//                                                1000,
//                                                totalDataCount,
//                                                toColumnsList,
//                                                joinQueryMapObj,
//                                                joinQuery,
//                                                fileDataLastIndex,
//                                                fileName,
//                                                1,
//                                                orderAndGroupByObj,
//                                                columnClauseObj,
//                                                selectTabObj,
//                                                normalizeOptionsObj
//                                        );
//
//                                    } else {
//                                        if (nonJoinOpList != null && !nonJoinOpList.isEmpty()) {
//                                            try {
//                                                processETLLog(sessionUserName,
//                                                        orgnId, "Starting extract non-join tables data.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
//                                            } catch (Exception e) {
//                                            }
//                                            for (int i = 0; i < nonJoinOpList.size(); i++) {
//                                                Map fromOperator = nonJoinOpList.get(i);
//                                                if (fromOperator != null && !fromOperator.isEmpty()) {
//                                                    totalDataCount += processETLData(sessionUserName,
//                                                            orgnId,
//                                                            fromConnection,
//                                                            toConnection,
//                                                            fromOperator,
//                                                            toOperator,
//                                                            columnsObj,
//                                                            dbFromColumnsObj,
//                                                            tablesWhereClauseObj,
//                                                            defaultValuesObj,
//                                                            toPreparedStatement,
//                                                            0,
//                                                            1000,
//                                                            totalDataCount,
//                                                            toColumnsList,
//                                                            fileDataLastIndex,
//                                                            fileName,
//                                                            1,
//                                                            orderAndGroupByObj,
//                                                            columnClauseObj,
//                                                            selectTabObj,
//                                                            normalizeOptionsObj
//                                                    );
//                                                }
//
//                                            }
//                                        }
//
//                                    }
//                                    System.out.println("totalDataCount::::" + totalDataCount);
//                                    if (totalDataCount != 0 && totalDataCount > 0) {
//                                        String message = totalDataCount + " Row(s) successfully extracted and loaded into target system.";
//                                        if (fileName != null && !"".equalsIgnoreCase(fileName) && !"null".equalsIgnoreCase(fileName)) {
//                                            message += " <br> <a href='#' style='color:#0071c5;' onclick=downloadExportedFile('" + fileName + "') >Click here to download the " + iconType + " file</a>.";//
//                                        }
//                                        logSequenceNo += 10;
//                                        try {
//                                            processETLLog(sessionUserName,
//                                                    orgnId, message, "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
//                                        } catch (Exception e) {
//                                        }
//                                        resultObj.put("Message", message);
//                                        resultObj.put("connectionFlag", "Y");
//
//                                    } else {
//                                        logSequenceNo += 10;
//                                        try {
//                                            processETLLog(sessionUserName,
//                                                    orgnId, totalDataCount + " Row(s) successfully extracted and loaded into target system.", "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
//                                        } catch (Exception e) {
//                                        }
////                    processETLLog(httpSession,totalDataCount + " Row(s) extracted successfully", 10);
//                                        resultObj.put("Message", totalDataCount + " Row(s) successfully extracted and loaded into target system.");
//                                        resultObj.put("connectionFlag", "Y");
//                                    }
//                                    try {
//                                        dropTemptables(tableNameObj);
//                                    } catch (Exception e) {
//                                    }
//                                } else {
//                                    try {
//                                        processETLLog(sessionUserName,
//                                                orgnId, totalDataCount + " Row(s) successfully extracted and loaded into target system.", "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
//                                    } catch (Exception e) {
//                                    }
////                    processETLLog(httpSession,totalDataCount + " Row(s) extracted successfully", 10);
//                                    resultObj.put("Message", totalDataCount + " Row(s) successfully extracted and loaded into target system.");
//                                    resultObj.put("connectionFlag", "Y");
//                                }//end
//                            }
//                        }
//                    }
//                }
            }// end of columnObj if
            try {
                processETLLog(sessionUserName,
                        orgnId, "ETL Process is completed", "INFO", logSequenceNo, "N", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception e) {
            }
        } catch (Exception e) {
            resultObj.put("Message", e.getMessage());
            resultObj.put("connectionFlag", "N");
            try {
                processETLLog(sessionUserName,
                        orgnId, "Problem in process ETL Data", "ERROR", 20, "N", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }
            e.printStackTrace();
        } finally {
            try {
                if (toPreparedStatement != null) {
                    toPreparedStatement.close();
                }
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

    public JSONObject getSelectedSAPTablesData(
            String sessionUserName,
            String orgnId,
            JCO.Client fromJCOConnection,
            List<Map> fromOperatorList,
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            //JSONObject fromColumnsObj,
            Map fromColumnsObj, // ravi etl integration
            JSONObject tablesWhereClauseObj,
            JSONObject defaultValuesObj,
            JSONObject orderByObj,
            String groupByQuery,
            JSONObject joinQueryMapObj
    ) {
        int totalInsertCount = 0;
        Connection currentV10Conn = null;
        PreparedStatement currentV10Stmt = null;
        JSONObject modifiedFromColumnsObj = new JSONObject();
        JSONObject resultMapObj = new JSONObject();
        try {
            JSONObject tableNameObj = new JSONObject();
            if (fromColumnsObj != null && !fromColumnsObj.isEmpty()) {
                if (joinQueryMapObj != null && !joinQueryMapObj.isEmpty()) {
                    for (Object joinTableName : joinQueryMapObj.keySet()) {
                        Object joinTableData = joinQueryMapObj.get(joinTableName);
                        if (joinTableData instanceof JSONObject) {
                            JSONObject joinTableDataObj = (JSONObject) joinTableData;
                            if (joinTableDataObj != null && !joinTableDataObj.isEmpty()) {
                                for (Object joinTableDataObjKey : joinTableDataObj.keySet()) {
                                    JSONObject joinChildTableData = (JSONObject) joinTableDataObj.get(joinTableDataObjKey);
                                    if (joinChildTableData != null && !joinChildTableData.isEmpty()) {
                                        String childTableColumn = (String) joinChildTableData.get("childTableColumn");
                                        String masterTableColumn = (String) joinChildTableData.get("masterTableColumn");
                                        if (childTableColumn != null
                                                && !"".equalsIgnoreCase(childTableColumn)
                                                && !"null".equalsIgnoreCase(childTableColumn)) {
                                            String[] childTableColumnArray = childTableColumn.split(":");
                                            if (childTableColumnArray != null
                                                    && childTableColumnArray.length != 0
                                                    && fromColumnsObj.get(childTableColumnArray[0]) != null
                                                    && !"".equalsIgnoreCase(String.valueOf(fromColumnsObj.get(childTableColumnArray[0])))
                                                    && !"null".equalsIgnoreCase(String.valueOf(fromColumnsObj.get(childTableColumnArray[0])))) {
                                                List<String> existingColumns = Arrays.asList(((String) fromColumnsObj.get(childTableColumnArray[0])).split(","));
                                                if (existingColumns != null
                                                        && !existingColumns.isEmpty()
                                                        && !existingColumns.contains(childTableColumnArray[1])) {
                                                    fromColumnsObj.put(childTableColumnArray[0], (fromColumnsObj.get(childTableColumnArray[0]) + "," + childTableColumnArray[1]));
                                                }
                                            } else {
                                                fromColumnsObj.put(childTableColumnArray[0], childTableColumnArray[1]);
                                            }
                                        }
                                        if (masterTableColumn != null
                                                && !"".equalsIgnoreCase(masterTableColumn)
                                                && !"null".equalsIgnoreCase(masterTableColumn)) {
                                            String[] masterTableColumnArray = masterTableColumn.split(":");
                                            if (masterTableColumnArray != null
                                                    && masterTableColumnArray.length != 0
                                                    && fromColumnsObj.get(masterTableColumnArray[0]) != null
                                                    && !"".equalsIgnoreCase(String.valueOf(fromColumnsObj.get(masterTableColumnArray[0])))
                                                    && !"null".equalsIgnoreCase(String.valueOf(fromColumnsObj.get(masterTableColumnArray[0])))) {
                                                List<String> existingColumns = Arrays.asList(((String) fromColumnsObj.get(masterTableColumnArray[0])).split(","));
                                                if (existingColumns != null
                                                        && !existingColumns.isEmpty()
                                                        && !existingColumns.contains(masterTableColumnArray[1])) {
                                                    fromColumnsObj.put(masterTableColumnArray[0], (fromColumnsObj.get(masterTableColumnArray[0]) + "," + masterTableColumnArray[1]));
                                                }
                                            } else {
                                                fromColumnsObj.put(masterTableColumnArray[0], masterTableColumnArray[1]);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
//                if (joinQueryMapObj != null && !joinQueryMapObj.isEmpty()) {
//                    for (Object joinTableName : joinQueryMapObj.keySet()) {
//                        Object joinTableData = joinQueryMapObj.get(joinTableName);
//                        if (joinTableData instanceof JSONObject) {
//                            JSONObject joinTableDataObj = (JSONObject) joinTableData;
//                            if (joinTableDataObj != null && !joinTableDataObj.isEmpty()) {
//                                for (Object joinTableDataObjKey : joinTableDataObj.keySet()) {
//                                    JSONObject joinChildTableData = (JSONObject) joinTableDataObj.get(joinTableDataObjKey);
//                                    if (joinChildTableData != null && !joinChildTableData.isEmpty()) {
//                                        String childTableColumn = (String) joinChildTableData.get("childTableColumn");
//                                        String masterTableColumn = (String) joinChildTableData.get("masterTableColumn");
//                                        if (childTableColumn != null
//                                                && !"".equalsIgnoreCase(childTableColumn)
//                                                && !"null".equalsIgnoreCase(childTableColumn)) {
//                                            String[] childTableColumnArray = childTableColumn.split(":");
//                                            if (childTableColumnArray != null
//                                                    && childTableColumnArray.length != 0
//                                                    && fromColumnsObj.get(childTableColumnArray[0]) != null
//                                                    && !"".equalsIgnoreCase(String.valueOf(fromColumnsObj.get(childTableColumnArray[0])))
//                                                    && !"null".equalsIgnoreCase(String.valueOf(fromColumnsObj.get(childTableColumnArray[0])))) {
//                                                List<String> existingColumns = Arrays.asList(((String) fromColumnsObj.get(childTableColumnArray[0])).split(","));
//                                                if (existingColumns != null
//                                                        && !existingColumns.isEmpty()
//                                                        && !existingColumns.contains(childTableColumnArray[1])) {
//                                                    fromColumnsObj.put(childTableColumnArray[0], (fromColumnsObj.get(childTableColumnArray[0]) + "," + childTableColumnArray[1]));
//                                                }
//                                            } else {
//                                                fromColumnsObj.put(childTableColumnArray[0], childTableColumnArray[1]);
//                                            }
//                                        }
//                                        if (masterTableColumn != null
//                                                && !"".equalsIgnoreCase(masterTableColumn)
//                                                && !"null".equalsIgnoreCase(masterTableColumn)) {
//                                            String[] masterTableColumnArray = masterTableColumn.split(":");
//                                            if (masterTableColumnArray != null
//                                                    && masterTableColumnArray.length != 0
//                                                    && fromColumnsObj.get(masterTableColumnArray[0]) != null
//                                                    && !"".equalsIgnoreCase(String.valueOf(fromColumnsObj.get(masterTableColumnArray[0])))
//                                                    && !"null".equalsIgnoreCase(String.valueOf(fromColumnsObj.get(masterTableColumnArray[0])))) {
//                                                List<String> existingColumns = Arrays.asList(((String) fromColumnsObj.get(masterTableColumnArray[0])).split(","));
//                                                if (existingColumns != null
//                                                        && !existingColumns.isEmpty()
//                                                        && !existingColumns.contains(masterTableColumnArray[1])) {
//                                                    fromColumnsObj.put(masterTableColumnArray[0], (fromColumnsObj.get(masterTableColumnArray[0]) + "," + masterTableColumnArray[1]));
//                                                }
//                                            } else {
//                                                fromColumnsObj.put(masterTableColumnArray[0], masterTableColumnArray[1]);
//                                            }
//                                        }
//                                    }
//                                }
//                            }
//                        }
//                    }
//                }
                Class.forName(dataBaseDriver);
                currentV10Conn = DriverManager.getConnection(dbURL, userName, password);
                for (Object fromSAPTableName : fromColumnsObj.keySet()) {
                    String dbTableName = fromSAPTableName + "_" + System.currentTimeMillis();
                    List<String> columnsList = new ArrayList<>();
                    if (dbTableName != null
                            && !"".equalsIgnoreCase(dbTableName)
                            && !"null".equalsIgnoreCase(dbTableName)) {
                        if (dbTableName.length() > 30) {
                            dbTableName = dbTableName.substring(0, 30);
                        }
                        tableNameObj.put(fromSAPTableName, dbTableName);
                        System.out.println("dbTableName:::" + dbTableName);

                        String columns = (String) fromColumnsObj.get(fromSAPTableName);
                        if (columns != null && !"".equalsIgnoreCase(columns) && !"null".equalsIgnoreCase(columns)) {
                            columnsList = Arrays.asList(columns.split(","));
                            columns = columns.replaceAll(",", " VARCHAR2(4000 CHAR), ");
                            columns += "  VARCHAR2(4000 CHAR)";
                            System.out.println("columns:::" + columns);
                            String createTableQuery = " CREATE TABLE " + dbTableName + " (" + columns + " )";
                            System.out.println("createTableQuery:::" + createTableQuery);
                            currentV10Stmt = currentV10Conn.prepareStatement(createTableQuery);
                            boolean isTableCreated = currentV10Stmt.execute();
                            System.out.println("isTableCreated:::" + isTableCreated);
                            // need to fetch sap data and insert into 
                            modifiedFromColumnsObj.put(dbTableName, fromColumnsObj.get(fromSAPTableName));
                            JSONObject tempFromColumnsObj = new JSONObject();
                            tempFromColumnsObj.put(fromSAPTableName, fromColumnsObj.get(fromSAPTableName));
                            String insertQuery = generateInsertQuery(dbTableName, columnsList);
                            System.out.println("insertQuery::::" + insertQuery);
                            currentV10Stmt = currentV10Conn.prepareStatement(insertQuery);
                            columnsList = columnsList.stream().map(columnName -> (fromSAPTableName + ":" + columnName))
                                    .collect(Collectors.toList());
                            totalInsertCount = importingSAPDataIntoV10(sessionUserName,
                                    orgnId,
                                    fromJCOConnection,
                                    fromOperatorList,
                                    columnsObj,
                                    tempFromColumnsObj,
                                    tablesWhereClauseObj,
                                    defaultValuesObj,
                                    orderByObj,
                                    groupByQuery,
                                    1,
                                    1000,
                                    1,
                                    totalInsertCount,
                                    currentV10Conn,
                                    currentV10Stmt,
                                    columnsList, (String) fromSAPTableName);

                        }

                    }
                }
            }
            resultMapObj.put("fromColumnsObj", modifiedFromColumnsObj);
            resultMapObj.put("tableNameObj", tableNameObj);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
        return resultMapObj;
    }

    public int importingSAPDataIntoV10(String sessionUserName,
            String orgnId,
            JCO.Client fromJCOConnection,
            List<Map> fromOperatorList,
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            //JSONObject fromColumnsObj,
            Map fromColumnsObj, // ravi etl integration
            JSONObject tablesWhereClauseObj,
            JSONObject defaultValuesObj,
            JSONObject orderByObj,
            String groupByQuery,
            int start,
            int limit,
            int pageNo,
            int totalDataCount,
            Connection currentV10Conn,
            PreparedStatement currentV10Stmt,
            List<String> columnsList,
            String fromSAPTableName
    ) {
        int insertCount = 0;
        try {
            List dataList = dataMigrationAccessDAO.getErpSelectedColumnsData(fromColumnsObj,
                    tablesWhereClauseObj,
                    fromJCOConnection,
                    start,
                    limit);
            if (dataList != null && !dataList.isEmpty()) {
                insertCount = importingData(dataList,
                        currentV10Stmt,
                        columnsList,
                        defaultValuesObj,
                        sessionUserName,
                        orgnId);
                totalDataCount += insertCount;
                if (insertCount != 0 && insertCount >= 1000) {
                    start = (pageNo * limit + 1);
                    pageNo++;
                    totalDataCount = importingSAPDataIntoV10(sessionUserName,
                            orgnId,
                            fromJCOConnection,
                            fromOperatorList,
                            columnsObj,
                            fromColumnsObj,
                            tablesWhereClauseObj,
                            defaultValuesObj,
                            orderByObj,
                            groupByQuery,
                            start,
                            limit,
                            pageNo,
                            totalDataCount,
                            currentV10Conn,
                            currentV10Stmt,
                            columnsList, fromSAPTableName);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return totalDataCount;
    }

    public List getDeNoramlisedData(JSONObject normalizeOptionsObj, List fileData) { // updated getDeNoramlisedData Ravi
        List deNormalisedData = new ArrayList();
        String deNormalizeColumn = (String) normalizeOptionsObj.get("deNormalizeColumn");
        String delimiter = (String) normalizeOptionsObj.get("delimiter");

        String wrtColumn = "";
        String wrtColumnVal = "";
        String prevWrtColumn = "";
        String prevWrtColumnVal = "";
        JSONArray groupingVals = new JSONArray();
        Map prevRowDataObj = new HashMap();

        List sortedFileData = new ArrayList();

        if (fileData != null && !fileData.isEmpty()) {
            Map firstRowDataObj = (Map) fileData.get(0);
            int index = 0;
            for (Object key : firstRowDataObj.keySet()) {
                if (index == 0 && !((String) key).equalsIgnoreCase(deNormalizeColumn)) {
                    wrtColumn = (String) key;
                }
                if (index == 1 && "".equalsIgnoreCase(wrtColumn)) {
                    wrtColumn = (String) key;
                }
                index++;
            }

            // for (int i = 0; i < fileData.size(); i++) {
            int count = 0;
            while (count < fileData.size()) {

                Map rowDataObj = (Map) fileData.get(count);
                count++;
                wrtColumnVal = (String) rowDataObj.get(wrtColumn);

                for (int j = 0; j < fileData.size(); j++) {
                    Map newRowDataObj = (Map) fileData.get(j);
                    String newWrtColumnVal = (String) newRowDataObj.get(wrtColumn);
                    if (newWrtColumnVal != null && newWrtColumnVal.equalsIgnoreCase(wrtColumnVal)) {
                        sortedFileData.add(newRowDataObj);
                        fileData.remove(j);
                        j--;
                        count = 0;
                    }
                }

            }

            for (int i = 0; i < sortedFileData.size(); i++) {
                Map rowDataObj = (Map) sortedFileData.get(i);

                if (i == 0) {

                    wrtColumnVal = (String) rowDataObj.get(wrtColumn);
                    groupingVals.add(rowDataObj.get(deNormalizeColumn));
                    prevWrtColumnVal = wrtColumnVal;
                    prevRowDataObj.putAll(rowDataObj);

                } else {
                    wrtColumnVal = (String) rowDataObj.get(wrtColumn);
                    if (!prevWrtColumnVal.equalsIgnoreCase(wrtColumnVal)) {
                        Map newRowData = new HashMap(prevRowDataObj);
                        String groupingValsStr = String.join(delimiter, groupingVals);
                        groupingVals.clear();
                        groupingVals.add(rowDataObj.get(deNormalizeColumn));
                        newRowData.put(deNormalizeColumn, groupingValsStr);
                        deNormalisedData.add(newRowData);
                        prevWrtColumnVal = wrtColumnVal;
                        prevRowDataObj.putAll(rowDataObj);
                    } else {
                        groupingVals.add(rowDataObj.get(deNormalizeColumn));
                    }

                    if (i == sortedFileData.size() - 1) {
                        String groupingValsStr = String.join(delimiter, groupingVals);
                        rowDataObj.put(deNormalizeColumn, groupingValsStr);
                        deNormalisedData.add(rowDataObj);
                        groupingVals.clear();
                    }
                }

                //  prevWrtColumnVal = wrtColumnVal;
            }
        }

        return deNormalisedData;
    }

    public List getNoramlisedData(JSONObject normalizeOptionsObj, List fileData) {
        List normalisedData = new ArrayList();
        String normalizeColumn = (String) normalizeOptionsObj.get("normalizeColumn");
        String itemSeparator = (String) normalizeOptionsObj.get("itemSeparator");
        try {
            for (int i = 0; i < fileData.size(); i++) {
                Map rowDataObj = (Map) fileData.get(i);
                List colValList = (List) rowDataObj.keySet().stream().filter((key) -> (((String) key).equalsIgnoreCase(normalizeColumn))).map((key) -> (String) rowDataObj.get(key)).collect(Collectors.toList());
                String colVal = (String) colValList.get(0);
                String[] colVals = colVal.split(itemSeparator);
                for (int j = 0; j < colVals.length; j++) {
                    Map map = new HashMap();
                    for (Object key : rowDataObj.keySet()) {

                        if (((String) key).equalsIgnoreCase(normalizeColumn)) {
                            map.put(key, colVals[j]);

                        } else {
                            map.put(key, rowDataObj.get(key));

                        }

                    }
                    normalisedData.add(map);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return normalisedData;
    }

    public Object getConnection(JSONObject dbObj) {

        Connection connection = null;
        Object returnedObj = null;
        try {

            if (dbObj != null && !dbObj.isEmpty()) {
                String initParamClassName = "com.pilog.mdm.transformaccess.V10MigrationDataAccess";
                String initParamMethodName = "get" + dbObj.get("CONN_CUST_COL1") + "Connection";
                System.out.println(" initParamClassName:" + initParamClassName + "initParamMethodName:" + initParamMethodName);
                Class clazz = Class.forName(initParamClassName);
                if ("SAP_ECC".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1")))) {
//                    dbObj.put("selectedItemLabel", connectionObj.get("CONN_CUST_COL1"));
//                    dbObj.put("ClientId", connectionObj.get("CONN_PORT"));
//                    dbObj.put("hostName", connectionObj.get("HOST_NAME"));
//                    dbObj.put("userName", connectionObj.get("CONN_USER_NAME"));
//                    dbObj.put("password", connectionObj.get("CONN_PASSWORD"));
//                    dbObj.put("LanguageId", "EN");
//                    dbObj.put("ERPSystemId", connectionObj.get("CONN_DB_NAME"));
//String ClientId,String userName,String password, String LanguageId,String hostName,String ERPSystemId
                    Class<?>[] paramTypes = {String.class, String.class, String.class, String.class, String.class, String.class};
                    Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
                    Object targetObj = new PilogUtilities().createObjectByClass(clazz);
                    returnedObj = method.invoke(targetObj, String.valueOf(dbObj.get("CONN_PORT")),
                            String.valueOf(dbObj.get("CONN_USER_NAME")),
                            String.valueOf(dbObj.get("CONN_PASSWORD")),
                            "EN",
                            String.valueOf(dbObj.get("HOST_NAME")),
                            String.valueOf(dbObj.get("CONN_DB_NAME")));

                } else {
                    //            dbFromObj.put("selectedItemLabel", fromConnectObj.get("CONN_CUST_COL1"));
//            dbFromObj.put("hostName", fromConnectObj.get("HOST_NAME"));
//            dbFromObj.put("port", fromConnectObj.get("CONN_PORT"));
//            dbFromObj.put("userName", fromConnectObj.get("CONN_USER_NAME"));
//            dbFromObj.put("password", fromConnectObj.get("CONN_PASSWORD"));
//            dbFromObj.put("serviceName", fromConnectObj.get("CONN_DB_NAME"));
                    Class<?>[] paramTypes = {String.class, String.class, String.class, String.class, String.class};
                    Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
                    Object targetObj = new PilogUtilities().createObjectByClass(clazz);
                    returnedObj = method.invoke(targetObj, String.valueOf(dbObj.get("HOST_NAME")), String.valueOf(dbObj.get("CONN_PORT")),
                            String.valueOf(dbObj.get("CONN_USER_NAME")), String.valueOf(dbObj.get("CONN_PASSWORD")), String.valueOf(dbObj.get("CONN_DB_NAME")));

                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return returnedObj;
    }

    public Map getSelectedDBTablesData(Map fromOperator,
            Map toOperator,
            //JSONObject fromColumnsObj,
            Map fromColumnsObj, // ravi etl integration
            Map transformationMap) {
        int totalInsertCount = 0;
        Connection currentV10Conn = null;
        PreparedStatement currentV10Stmt = null;
        Connection fromConnection = null;
        PreparedStatement fromStmt = null;
        ResultSet fromSet = null;
        try {
            if (fromOperator != null && !fromOperator.isEmpty()) {
                //JSONObject columnsObj = (JSONObject) transformationMap.get("columnsObj");
                Map columnsObj = (LinkedHashMap) transformationMap.get("columnsObj"); // ravi etl integration
                JSONObject tablesWhereClauseObj = (JSONObject) transformationMap.get("whereClauseObj");//
                JSONObject joinQueryMapObj = (JSONObject) transformationMap.get("joinQueryMapObj");//
                JSONObject orderByObj = (JSONObject) transformationMap.get("orderBy");//
                JSONObject appendValObj = (JSONObject) transformationMap.get("appendValObj");//
                JSONObject columnClauseObj = (JSONObject) transformationMap.get("columnClauseObj");//
                JSONObject selectTabObj = (JSONObject) transformationMap.get("selectTabObj");
                JSONObject defaultValuesObj = (JSONObject) transformationMap.get("defaultValObj");
                JSONObject normalizeOptionsObj = (JSONObject) transformationMap.get("normalizeOptionsObj");
                String joinQuery = (String) transformationMap.get("joinQuery");//
                String groupByQuery = (String) transformationMap.get("groupBy");
                if (joinQueryMapObj != null && !joinQueryMapObj.isEmpty()) {
                    for (Object joinTableName : joinQueryMapObj.keySet()) {
                        Object joinTableData = joinQueryMapObj.get(joinTableName);
                        if (joinTableData instanceof JSONObject) {
                            JSONObject joinTableDataObj = (JSONObject) joinTableData;
                            if (joinTableDataObj != null && !joinTableDataObj.isEmpty()) {
                                for (Object joinTableDataObjKey : joinTableDataObj.keySet()) {
                                    JSONObject joinChildTableData = (JSONObject) joinTableDataObj.get(joinTableDataObjKey);
                                    if (joinChildTableData != null && !joinChildTableData.isEmpty()) {
                                        String childTableColumn = (String) joinChildTableData.get("childTableColumn");
                                        String masterTableColumn = (String) joinChildTableData.get("masterTableColumn");
                                        if (childTableColumn != null
                                                && !"".equalsIgnoreCase(childTableColumn)
                                                && !"null".equalsIgnoreCase(childTableColumn)) {
                                            String[] childTableColumnArray = childTableColumn.split(":");
                                            if (childTableColumnArray != null
                                                    && childTableColumnArray.length != 0
                                                    && fromColumnsObj.get(childTableColumnArray[0]) != null
                                                    && !"".equalsIgnoreCase(String.valueOf(fromColumnsObj.get(childTableColumnArray[0])))
                                                    && !"null".equalsIgnoreCase(String.valueOf(fromColumnsObj.get(childTableColumnArray[0])))) {
                                                List<String> existingColumns = Arrays.asList(((String) fromColumnsObj.get(childTableColumnArray[0])).split(","));
                                                if (existingColumns != null
                                                        && !existingColumns.isEmpty()
                                                        && !existingColumns.contains(childTableColumnArray[1])) {
                                                    fromColumnsObj.put(childTableColumnArray[0], (fromColumnsObj.get(childTableColumnArray[0]) + "," + childTableColumnArray[1]));
                                                }
                                            } else {
                                                fromColumnsObj.put(childTableColumnArray[0], childTableColumnArray[1]);
                                            }
                                        }
                                        if (masterTableColumn != null
                                                && !"".equalsIgnoreCase(masterTableColumn)
                                                && !"null".equalsIgnoreCase(masterTableColumn)) {
                                            String[] masterTableColumnArray = masterTableColumn.split(":");
                                            if (masterTableColumnArray != null
                                                    && masterTableColumnArray.length != 0
                                                    && fromColumnsObj.get(masterTableColumnArray[0]) != null
                                                    && !"".equalsIgnoreCase(String.valueOf(fromColumnsObj.get(masterTableColumnArray[0])))
                                                    && !"null".equalsIgnoreCase(String.valueOf(fromColumnsObj.get(masterTableColumnArray[0])))) {
                                                List<String> existingColumns = Arrays.asList(((String) fromColumnsObj.get(masterTableColumnArray[0])).split(","));
                                                if (existingColumns != null
                                                        && !existingColumns.isEmpty()
                                                        && !existingColumns.contains(masterTableColumnArray[1])) {
                                                    fromColumnsObj.put(masterTableColumnArray[0], (fromColumnsObj.get(masterTableColumnArray[0]) + "," + masterTableColumnArray[1]));
                                                }
                                            } else {
                                                fromColumnsObj.put(masterTableColumnArray[0], masterTableColumnArray[1]);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                String fromTableName = (String) fromOperator.get("tableName");
                String fromColumns = (String) fromColumnsObj.get(fromTableName);
                if (fromColumns != null
                        && !"".equalsIgnoreCase(fromColumns)
                        && !"null".equalsIgnoreCase(fromColumns)) {

                    JSONObject connObj = (JSONObject) fromOperator.get("connObj");
                    currentV10Conn = DriverManager.getConnection(dbURL, userName, password);

                    if (currentV10Conn != null) {
                        JSONObject currentV10DBObj = new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName, password, "Current_V10");
                        String dbTableName = "ZZ_" + System.currentTimeMillis();

                        Object fromConnObj = getConnection(connObj);
                        if (dbTableName != null
                                && !"".equalsIgnoreCase(dbTableName)
                                && !"null".equalsIgnoreCase(dbTableName)) {
                            if (dbTableName.length() > 30) {
                                dbTableName = dbTableName.substring(0, 30);
                            }
                            List<Object[]> columnsList = new ArrayList<>();
                            if (fromConnObj instanceof Connection) {
                                fromConnection = (Connection) fromConnObj;
                                String fronDBTableName = fromTableName;
                                if (fromTableName.contains(".")) {
                                    fronDBTableName = (fromTableName.split("[.]"))[1];
                                }
                                columnsList = getTableColumnsOpt(fromConnection, connObj, fronDBTableName);
                            } else if (fromConnObj instanceof JCO.Client) {
                                String fronDBTableName = fromTableName;
                                if (fromTableName.contains(".")) {
                                    fronDBTableName = (fromTableName.split("[.]"))[1];
                                }
                                columnsList = dataMigrationAccessDAO.getSAPTableColumnsWithType((JCO.Client) fromConnObj, fronDBTableName);
                            }

                            if (columnsList != null && !columnsList.isEmpty()) {
                                List<Object[]> dataTypeList = new ArrayList<>();
                                try {
                                    dataTypeList = genericDataPipingDAO.getTargetDataType(String.valueOf(connObj.get("CONN_CUST_COL1")).toUpperCase(),
                                            String.valueOf(currentV10DBObj.get("CONN_CUST_COL1")).toUpperCase());
                                } catch (Exception e) {
                                }

                                String createTableQuery = "CREATE TABLE " + dbTableName + "("
                                        + "";

                                for (int k = 0; k < columnsList.size(); k++) {
                                    Object[] columnsObjArray = columnsList.get(k);
                                    if (columnsObjArray != null && columnsObjArray.length != 0) {

                                        createTableQuery += "" + columnsObjArray[2];
                                        if (dataTypeList != null && !dataTypeList.isEmpty()) {
                                            JSONObject dataTypeObj = new JSONObject();
                                            List<Object[]> matchedDataTypeList = dataTypeList.stream()
                                                    .filter(dataTypeArray
                                                            -> (dataTypeArray[1] != null
                                                    && !"".equalsIgnoreCase(String.valueOf(dataTypeArray[1]))
                                                    && !"null".equalsIgnoreCase(String.valueOf(dataTypeArray[1]))
                                                    && dataTypeArray[6] != null
                                                    && !"".equalsIgnoreCase(String.valueOf(dataTypeArray[6]))
                                                    && !"null".equalsIgnoreCase(String.valueOf(dataTypeArray[6]))
                                                    && String.valueOf(dataTypeArray[6]).equalsIgnoreCase(String.valueOf(columnsObjArray[3]))))
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
                                                                && !"null".equalsIgnoreCase(String.valueOf(columnsObjArray[4]))) {
                                                            int dataTypeLen = new PilogUtilities().convertIntoInteger(new BigInteger("" + columnsObjArray[4]));
                                                            if (dataTypeLen == 0) {
                                                                dataTypeLen = 1;
                                                            }
                                                            //length += "" + dataTypeLen+ " ";
                                                            length += "" + (dataTypeLen + 100) + " "; // ravi etl integration
                                                            if (matchedDataTypeArray[4] != null
                                                                    && !"".equalsIgnoreCase(String.valueOf(matchedDataTypeArray[4]))
                                                                    && !"null".equalsIgnoreCase(String.valueOf(matchedDataTypeArray[4]))) {
                                                                length += "" + matchedDataTypeArray[4] + " ";
                                                            }
                                                        }

                                                        if (length != null
                                                                && !"".equalsIgnoreCase(length)
                                                                && !"null".equalsIgnoreCase(length)) {

                                                            dataType += "(" + length + ") ";
                                                        }
                                                        createTableQuery += " " + dataType;
                                                    }

                                                }

                                            }

                                        } else {
                                            createTableQuery += " " + columnsObjArray[8];
                                        }
                                        if (k != columnsList.size() - 1) {
                                            createTableQuery += " ,";
                                        }
                                    }

                                }// columns loop end
                                createTableQuery += ")";
                                System.out.println("createTableQuery:::" + createTableQuery);
                                currentV10Stmt = currentV10Conn.prepareStatement(createTableQuery);
                                boolean isTableCreated = currentV10Stmt.execute();
                                System.out.println("isTableCreated:::" + isTableCreated);

                                List<String> fromColumnsList = Arrays.asList(fromColumns.split(","));
                                String insertQuery = generateInsertQuery(dbTableName, fromColumnsList);
                                System.out.println("insertQuery:::" + insertQuery);
                                Map columnsTypeObj = getColumnsType(dbTableName, fromColumnsList, currentV10Conn);
                                if (!(selectTabObj != null && !selectTabObj.isEmpty())) {
                                    selectTabObj = new JSONObject();
                                }
                                selectTabObj.put("columnsTypeObj", columnsTypeObj);
                                currentV10Stmt = currentV10Conn.prepareStatement(insertQuery);
                                //JSONObject fromColumnsObjUpdated = new JSONObject();
                                Map fromColumnsObjUpdated = new LinkedHashMap(); // ravi etl integration
                                fromColumnsObjUpdated.put(fromTableName, fromColumnsObj.get(fromTableName));
                                //JSONObject columnsObjUpdated = columnsObj;
                                Map columnsObjUpdated = columnsObj; // ravi etl integration
                                if (!(columnsObjUpdated != null && !columnsObjUpdated.isEmpty())) {
                                    for (int i = 0; i < fromColumnsList.size(); i++) {
                                        columnsObjUpdated.put(fromTableName + ":" + fromColumnsList.get(i), fromTableName + ":" + fromColumnsList.get(i));
                                    }
                                }
                                int insertCount = importDBData(columnsObjUpdated,
                                        fromColumnsObjUpdated,
                                        tablesWhereClauseObj,
                                        defaultValuesObj,
                                        orderByObj,
                                        groupByQuery,
                                        1,
                                        1000,
                                        1,
                                        totalInsertCount,
                                        currentV10Conn,
                                        currentV10Stmt,
                                        fromColumnsList,
                                        fromTableName,
                                        selectTabObj,
                                        fromOperator,
                                        fromConnObj,
                                        appendValObj,
                                        columnClauseObj,
                                        "",
                                        "",
                                        dbTableName);

                                fromOperator.put("oldTableName", fromOperator.get("tableName"));
                                fromOperator.put("tableName", currentV10DBObj.get("CONN_USER_NAME") + "." + dbTableName);
                                fromOperator.put("oldstatusLabel", fromOperator.get("statusLabel"));
                                fromOperator.put("statusLabel", dbTableName);
                                fromOperator.put("CONNECTION_NAME", currentV10DBObj.get("CONNECTION_NAME"));
                                fromOperator.put("CONN_DB_NAME", currentV10DBObj.get("CONN_DB_NAME"));
                                fromOperator.put("CONN_CUST_COL1", currentV10DBObj.get("CONN_CUST_COL1"));
                                fromOperator.put("connObj", currentV10DBObj);

                            }
                        }
                    }
                } else {

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (fromSet != null) {
                    fromSet.close();
                }
                if (fromStmt != null) {
                    fromStmt.close();
                }
                if (currentV10Stmt != null) {
                    currentV10Stmt.close();
                }
                if (fromConnection != null) {
                    fromConnection.close();
                }
                if (currentV10Conn != null) {
                    currentV10Conn.close();
                }
            } catch (Exception e) {
            }
        }
        return fromOperator;
    }

    public List<Object[]> getTableColumnsOpt(Connection connection, JSONObject dbObj, String tableName) {
        List<Object[]> columnsList = new ArrayList<>();
        try {
            if (connection != null) {
                if (tableName != null && tableName.contains(",")) {
                    tableName = tableName.replaceAll(",", "','");
                }
                String initParamClassName = "com.pilog.mdm.DAO.V10DataMigrationAccessDAO";
                String initParamMethodName = "get" + dbObj.get("CONN_CUST_COL1") + "TableColumns";
                System.out.println(" initParamClassName:" + initParamClassName + "initParamMethodName:" + initParamMethodName);
                Class clazz = Class.forName(initParamClassName);
                Class<?>[] paramTypes = {Connection.class, String.class, String.class};
                Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
                Object targetObj = new PilogUtilities().createObjectByClass(clazz);
                columnsList = (List<Object[]>) method.invoke(targetObj, connection, String.valueOf(dbObj.get("CONN_DB_NAME")), tableName);
            }

        } catch (Exception e) {

            e.printStackTrace();
        }
        return columnsList;
    }

    public int importDBData(
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            //JSONObject fromColumnsObj,
            Map fromColumnsObj, // ravi etl integration
            JSONObject tablesWhereClauseObj,
            JSONObject defaultValuesObj,
            JSONObject orderByObj,
            String groupByQuery,
            int start,
            int limit,
            int pageNo,
            int totalDataCount,
            Connection toConnection,
            PreparedStatement toPreparedStatement,
            List<String> columnsList,
            String fromTableName,
            JSONObject selectTabObj,
            Map fromOperator,
            Object fromConnObj,
            JSONObject appendValObj,
            JSONObject columnClauseObj,
            String sessionUserName,
            String orgnId,
            String toTableName
    ) {
        int insertCount = 0;
        try {
            if (selectTabObj != null
                    && !selectTabObj.isEmpty()) {
                selectTabObj.put("columnClauseObj", columnClauseObj);
                String minRows = String.valueOf(selectTabObj.get("minRows"));
                if (minRows != null
                        && !"".equalsIgnoreCase(minRows)
                        && !"null".equalsIgnoreCase(minRows)
                        && NumberUtils.isNumber(minRows)) {
                    if (pageNo == 1) {
                        start = Integer.parseInt(minRows);
                    }
                }
                String maxRows = String.valueOf(selectTabObj.get("maxRows"));
                if (maxRows != null
                        && !"".equalsIgnoreCase(maxRows)
                        && !"null".equalsIgnoreCase(maxRows)
                        && NumberUtils.isNumber(maxRows)) {
                    int maxRowsCount = Integer.parseInt(maxRows);
                    if (pageNo == 1) {
                        if (((maxRowsCount - start)) <= limit) {
                            limit = (maxRowsCount - start);
                        }
                    } else {
                        if (((maxRowsCount - start) + 1) <= limit) {
                            limit = (maxRowsCount - start) + 1;
                        }
                    }
                }

            }
            int end = start + limit - 1;
            if (start == 0) {
                end = limit;
            }
            List totalDataList = new ArrayList();
            if (limit > 0) {

                if (fromConnObj instanceof Connection) {
                    totalDataList = getSelectedColumnsData((JSONObject) fromOperator.get("connObj"),
                            (Connection) fromConnObj,
                            fromColumnsObj,
                            start,
                            limit,
                            tablesWhereClauseObj,
                            sessionUserName,
                            orgnId,
                            appendValObj,
                            columnClauseObj,
                            selectTabObj,
                            toConnection);
                } else if (fromConnObj instanceof JCO.Client) {
                    totalDataList = dataMigrationAccessDAO.getErpSelectedColumnsData(fromColumnsObj,
                            tablesWhereClauseObj,
                            (JCO.Client) fromConnObj,
                            start,
                            limit);
                }
            }
            if (totalDataList != null && !totalDataList.isEmpty()) {
                if (!(selectTabObj != null && !selectTabObj.isEmpty())) {
                    selectTabObj = new JSONObject();
                }
                selectTabObj.put("oldTableName", fromTableName);
                insertCount = dataMigrationService.importingData(toTableName,
                        fromColumnsObj,
                        columnsObj,
                        totalDataList,
                        toPreparedStatement,
                        columnsList,
                        defaultValuesObj,
                        sessionUserName,
                        orgnId,
                        String.valueOf(selectTabObj.get("jobId")),
                        selectTabObj);
                totalDataCount += insertCount;
                if (insertCount != 0 && insertCount >= 1000) {
                    int startIndex = (pageNo * limit + 1);

                    pageNo++;
                    totalDataCount = importDBData(columnsObj,
                            fromColumnsObj,
                            tablesWhereClauseObj,
                            defaultValuesObj,
                            orderByObj,
                            groupByQuery,
                            startIndex,
                            limit,
                            pageNo,
                            totalDataCount,
                            toConnection,
                            toPreparedStatement,
                            columnsList,
                            fromTableName,
                            selectTabObj,
                            fromOperator,
                            fromConnObj,
                            appendValObj,
                            columnClauseObj,
                            sessionUserName,
                            orgnId,
                            toTableName);

                }
            }

        } catch (Exception e) {
        }
        return totalDataCount;
    }

    public int importFileData(
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            //JSONObject fromColumnsObj,
            Map fromColumnsObj, // ravi etl integration
            JSONObject tablesWhereClauseObj,
            JSONObject defaultValuesObj,
            JSONObject orderByObj,
            String groupByQuery,
            int start,
            int limit,
            int pageNo,
            int totalDataCount,
            Connection toConnection,
            PreparedStatement toPreparedStatement,
            List<String> columnsList,
            String fromTableName,
            JSONObject selectTabObj,
            Map fromOperator,
            Object fromConnObj,
            JSONObject appendValObj,
            JSONObject columnClauseObj,
            String sessionUserName,
            String orgnId,
            String toTableName,
            Object fileLoadObj,
            List<String> columnList,
            LineNumberReader lineNumberReader
    ) {
        int insertCount = 0;
        try {
            if (selectTabObj != null
                    && !selectTabObj.isEmpty()) {
                selectTabObj.put("columnClauseObj", columnClauseObj);
                String minRows = String.valueOf(selectTabObj.get("minRows"));
                if (minRows != null
                        && !"".equalsIgnoreCase(minRows)
                        && !"null".equalsIgnoreCase(minRows)
                        && NumberUtils.isNumber(minRows)) {
                    if (pageNo == 1) {
                        start = Integer.parseInt(minRows);
                    }
                }
                String maxRows = String.valueOf(selectTabObj.get("maxRows"));
                if (maxRows != null
                        && !"".equalsIgnoreCase(maxRows)
                        && !"null".equalsIgnoreCase(maxRows)
                        && NumberUtils.isNumber(maxRows)) {
                    int maxRowsCount = Integer.parseInt(maxRows);
                    if (pageNo == 1) {
                        if (((maxRowsCount - start)) <= limit) {
                            limit = (maxRowsCount - start);
                        }
                    } else {
                        if (((maxRowsCount - start) + 1) <= limit) {
                            limit = (maxRowsCount - start) + 1;
                        }
                    }
                }

            }
            int end = start + limit - 1;
            if (start == 0) {
                end = limit;
            }
            List totalDataList = new ArrayList();
            JSONObject fileObj = (JSONObject) fromOperator.get("connObj");
            String fromFileName = (String) fileObj.get("fileName");
            if (limit > 0) {

                if (fileObj != null && !fileObj.isEmpty()) {
                    String fileType = (String) fileObj.get("fileType");

                    String filePath = (String) fileObj.get("filePath");

                    if (".xls".equalsIgnoreCase(fileType) || ".xlsx".equalsIgnoreCase(fileType)) {
                        //totalDataList = readExcel(filePath, columnList, start, limit, end, fromFileName, (Workbook) fileLoadObj);
                        // ravi etl integration code start
                        JSONObject excelInfoObj = readExcel(filePath, columnList, start, limit, end, fromFileName, (Workbook) fileLoadObj, selectTabObj);
                        totalDataList = (List) excelInfoObj.get("dataList");
                        selectTabObj.put("noOfSheets", excelInfoObj.get("noOfSheets"));
                        selectTabObj.put("sheetNo", excelInfoObj.get("sheetNo"));
                        // ravi etl integration code start end

                    } else if (".CSV".equalsIgnoreCase(fileType)
                            || ".TXT".equalsIgnoreCase(fileType)
                            || ".json".equalsIgnoreCase(fileType)) {
                        totalDataList = readCSV(filePath, columnList, start, limit, end, fromFileName, fileType, (CSVReader) fileLoadObj, lineNumberReader);
                    } else if (".xml".equalsIgnoreCase(fileType)) {
                        totalDataList = readXML(filePath, columnList, start, limit, end, fromFileName, (Document) fileLoadObj);
                    }
                }
            }
            if (totalDataList != null && !totalDataList.isEmpty()) {
                selectTabObj.put("oldTableName", fromFileName);
                insertCount = dataMigrationService.importingData(toTableName,
                        fromColumnsObj,
                        columnsObj,
                        totalDataList,
                        toPreparedStatement,
                        columnList,
                        defaultValuesObj,
                        sessionUserName,
                        orgnId,
                        String.valueOf(selectTabObj.get("jobId")),
                        selectTabObj);
                totalDataCount += insertCount;
                if (insertCount != 0 && insertCount >= 1000) {
                    int startIndex = (pageNo * limit + 1);

                    pageNo++;
                    totalDataCount = importFileData(columnsObj,
                            fromColumnsObj,
                            tablesWhereClauseObj,
                            defaultValuesObj,
                            orderByObj,
                            groupByQuery,
                            startIndex,
                            limit,
                            pageNo,
                            totalDataCount,
                            toConnection,
                            toPreparedStatement,
                            columnsList,
                            fromTableName,
                            selectTabObj,
                            fromOperator,
                            fromConnObj,
                            appendValObj,
                            columnClauseObj,
                            sessionUserName,
                            orgnId,
                            toTableName, fileLoadObj,
                            columnList,
                            lineNumberReader);

                }
            }

        } catch (Exception e) {
        }
        return totalDataCount;
    }

    public Map getSelectedFileTablesData(Map fromOperator,
            Map toOperator,
            //JSONObject fromColumnsObj,
            Map fromColumnsObj, // ravi etl integration
            Map transformationMap,
            int fileDataLastIndex,
            String fileName) {
        Connection currentV10Conn = null;
        PreparedStatement toPreparedStatement = null;
        try {
            if (fromOperator != null && !fromOperator.isEmpty()) {
                //JSONObject columnsObj = (JSONObject) transformationMap.get("columnsObj");
                Map columnsObj = (LinkedHashMap) transformationMap.get("columnsObj"); // ravi etl integration
                JSONObject tablesWhereClauseObj = (JSONObject) transformationMap.get("whereClauseObj");//
                JSONObject joinQueryMapObj = (JSONObject) transformationMap.get("joinQueryMapObj");//
                JSONObject orderByObj = (JSONObject) transformationMap.get("orderBy");//
                JSONObject appendValObj = (JSONObject) transformationMap.get("appendValObj");//
                JSONObject columnClauseObj = (JSONObject) transformationMap.get("columnClauseObj");//
                JSONObject selectTabObj = (JSONObject) transformationMap.get("selectTabObj");
                JSONObject defaultValuesObj = (JSONObject) transformationMap.get("defaultValObj");
                JSONObject normalizeOptionsObj = (JSONObject) transformationMap.get("normalizeOptionsObj");
                String joinQuery = (String) transformationMap.get("joinQuery");//
                String groupByQuery = (String) transformationMap.get("groupBy");
                JSONObject orderAndGroupByObj = new JSONObject();
                orderAndGroupByObj.put("orderByObj", orderByObj);
                orderAndGroupByObj.put("groupByQuery", groupByQuery);
                orderAndGroupByObj.put("nativeSQL", "");
                if (!(selectTabObj != null && !selectTabObj.isEmpty())) {
                    selectTabObj = new JSONObject();
                }
//                selectTabObj.put("jobId", jobId);
                if (joinQueryMapObj != null && !joinQueryMapObj.isEmpty()) {
                    for (Object joinTableName : joinQueryMapObj.keySet()) {
                        Object joinTableData = joinQueryMapObj.get(joinTableName);
                        if (joinTableData instanceof JSONObject) {
                            JSONObject joinTableDataObj = (JSONObject) joinTableData;
                            if (joinTableDataObj != null && !joinTableDataObj.isEmpty()) {
                                for (Object joinTableDataObjKey : joinTableDataObj.keySet()) {
                                    JSONObject joinChildTableData = (JSONObject) joinTableDataObj.get(joinTableDataObjKey);
                                    if (joinChildTableData != null && !joinChildTableData.isEmpty()) {
                                        String childTableColumn = (String) joinChildTableData.get("childTableColumn");
                                        String masterTableColumn = (String) joinChildTableData.get("masterTableColumn");
                                        if (childTableColumn != null
                                                && !"".equalsIgnoreCase(childTableColumn)
                                                && !"null".equalsIgnoreCase(childTableColumn)) {
                                            String[] childTableColumnArray = childTableColumn.split(":");
                                            if (childTableColumnArray != null
                                                    && childTableColumnArray.length != 0
                                                    && fromColumnsObj.get(childTableColumnArray[0]) != null
                                                    && !"".equalsIgnoreCase(String.valueOf(fromColumnsObj.get(childTableColumnArray[0])))
                                                    && !"null".equalsIgnoreCase(String.valueOf(fromColumnsObj.get(childTableColumnArray[0])))) {
                                                List<String> existingColumns = Arrays.asList(((String) fromColumnsObj.get(childTableColumnArray[0])).split(","));
                                                if (existingColumns != null
                                                        && !existingColumns.isEmpty()
                                                        && !existingColumns.contains(childTableColumnArray[1])) {
                                                    fromColumnsObj.put(childTableColumnArray[0], (fromColumnsObj.get(childTableColumnArray[0]) + "," + childTableColumnArray[1]));
                                                }
                                            } else {
                                                fromColumnsObj.put(childTableColumnArray[0], childTableColumnArray[1]);
                                            }
                                        }
                                        if (masterTableColumn != null
                                                && !"".equalsIgnoreCase(masterTableColumn)
                                                && !"null".equalsIgnoreCase(masterTableColumn)) {
                                            String[] masterTableColumnArray = masterTableColumn.split(":");
                                            if (masterTableColumnArray != null
                                                    && masterTableColumnArray.length != 0
                                                    && fromColumnsObj.get(masterTableColumnArray[0]) != null
                                                    && !"".equalsIgnoreCase(String.valueOf(fromColumnsObj.get(masterTableColumnArray[0])))
                                                    && !"null".equalsIgnoreCase(String.valueOf(fromColumnsObj.get(masterTableColumnArray[0])))) {
                                                List<String> existingColumns = Arrays.asList(((String) fromColumnsObj.get(masterTableColumnArray[0])).split(","));
                                                if (existingColumns != null
                                                        && !existingColumns.isEmpty()
                                                        && !existingColumns.contains(masterTableColumnArray[1])) {
                                                    fromColumnsObj.put(masterTableColumnArray[0], (fromColumnsObj.get(masterTableColumnArray[0]) + "," + masterTableColumnArray[1]));
                                                }
                                            } else {
                                                fromColumnsObj.put(masterTableColumnArray[0], masterTableColumnArray[1]);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                currentV10Conn = DriverManager.getConnection(dbURL, userName, password);
                JSONObject currentV10DBObj = new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName, password, "Current_V10");
                String dbTableName = "ZZ_" + System.currentTimeMillis();

                JSONObject fileObj = (JSONObject) fromOperator.get("connObj");
                List<String> columnList = new ArrayList<>();
                Object fileLoadObj = null;
                LineNumberReader lineNumberReader = null;
                if (fileObj != null && !fileObj.isEmpty()) {
                    String fileType = "";
                    String fromFileName = "";
                    String filePath = "";
                    if (fromOperator.containsKey("filePath")
                            && !fileObj.containsKey("filePath")) {
                        filePath = (String) fromOperator.get("filePath");
                        fromFileName = (String) fromOperator.get("oldTableName");
                        fileType = "." + filePath.substring(filePath.lastIndexOf(".") + 1, filePath.length());
                    } else {
                        fileType = (String) fileObj.get("fileType");
                        fromFileName = (String) fileObj.get("fileName");
                        filePath = (String) fileObj.get("filePath");
                    }

                    if (".xls".equalsIgnoreCase(fileType) || ".xlsx".equalsIgnoreCase(fileType)) {
                        columnList = getHeadersOfImportedFile(filePath, fileType);
                        Workbook workBook = WorkbookFactory.create(new File(filePath));
                        fileLoadObj = workBook;
                    } else if (".CSV".equalsIgnoreCase(fileType)
                            || ".TXT".equalsIgnoreCase(fileType)
                            || ".json".equalsIgnoreCase(fileType)) {
                        columnList = getHeadersOfImportedFile(filePath, fileType);
//                        char columnSeparator = '\t';
                        CsvParserSettings settings = new CsvParserSettings();
                        settings.detectFormatAutomatically();

                        CsvParser parser = new CsvParser(settings);
                        List<String[]> rows = parser.parseAll(new File(filePath));

                        // if you want to see what it detected
                        CsvFormat format = parser.getDetectedFormat();
                        char columnSeparator = format.getDelimiter();

                        if (".json".equalsIgnoreCase(fileType)) {
                            columnSeparator = ',';
                        }
                        CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(filePath), "UTF8"), columnSeparator);
                        lineNumberReader = new LineNumberReader(new FileReader(filePath));
                        fileLoadObj = reader;
                    } else if (".xml".equalsIgnoreCase(fileType)) {
                        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                        DocumentBuilder builder = factory.newDocumentBuilder();
                        Document document = builder.parse(new FileInputStream(filePath), "UTF-8");
                        fileLoadObj = document;
                        columnList = getHeadersOfImportedFile(filePath, fileType);
                    }
                }

                List<String> toColumnsList = new ArrayList<>();
                if (columnList != null) {
                    String createTableQuery = "CREATE TABLE " + dbTableName + "(";
                    for (int i = 0; i < columnList.size(); i++) {
                        String headerName = columnList.get(i);
                        if (headerName != null
                                && !"".equalsIgnoreCase(headerName)
                                && !"null".equalsIgnoreCase(headerName)) {
                            headerName = headerName.replaceAll(" ", "_").toUpperCase();
                        }
                        toColumnsList.add(headerName);
                        createTableQuery += headerName + " VARCHAR2(4000 CHAR)";
                        if (i != columnList.size() - 1) {
                            createTableQuery += ",";
                        }

                    }
                    createTableQuery += ")";
                    System.out.println("createTableQuery:::" + createTableQuery);
                    toPreparedStatement = currentV10Conn.prepareStatement(createTableQuery);
                    toPreparedStatement.execute();
                }

                String insertQuery = generateInsertQuery(dbTableName, toColumnsList);
                System.out.println("insertQuery::" + insertQuery);
                toPreparedStatement = currentV10Conn.prepareStatement(insertQuery);
//                Map columnsTypeObj = getColumnsType(dbTableName, toColumnsList, currentV10Conn);
//                if (!(selectTabObj != null && !selectTabObj.isEmpty())) {
//                    selectTabObj = new JSONObject();
//                }
//                selectTabObj.put("columnsTypeObj", columnsTypeObj);
                JSONObject fromColumnsObjUpdated = new JSONObject();
                String fromFileName = (String) fileObj.get("fileName");
                fromColumnsObjUpdated.put(fromFileName, fromColumnsObj.get(fromFileName));
                //JSONObject columnsObjUpdated = columnsObj;
                Map columnsObjUpdated = columnsObj; // ravi etl integration
                if ((columnsObjUpdated != null)) {
                    for (int i = 0; i < columnList.size(); i++) {
                        columnsObjUpdated.put(fromFileName + ":" + columnList.get(i), fromFileName + ":" + columnList.get(i));
                    }
                }
                int insertCount = importFileData(columnsObjUpdated,
                        fromColumnsObjUpdated,
                        tablesWhereClauseObj,
                        defaultValuesObj,
                        orderByObj,
                        groupByQuery,
                        1,
                        1000,
                        1,
                        0,
                        currentV10Conn,
                        toPreparedStatement,
                        toColumnsList,
                        dbTableName,
                        selectTabObj,
                        fromOperator,
                        fileLoadObj,
                        appendValObj,
                        columnClauseObj,
                        "",
                        "",
                        dbTableName,
                        fileLoadObj,
                        columnList,
                        lineNumberReader);

                fromOperator.put("oldTableName", fileObj.get("fileName"));
                fromOperator.put("tableName", currentV10DBObj.get("CONN_USER_NAME") + "." + dbTableName);
                fromOperator.put("oldstatusLabel", fileObj.get("fileName"));
                fromOperator.put("statusLabel", dbTableName);
                fromOperator.put("CONNECTION_NAME", currentV10DBObj.get("CONNECTION_NAME"));
                fromOperator.put("CONN_DB_NAME", currentV10DBObj.get("CONN_DB_NAME"));
                fromOperator.put("CONN_CUST_COL1", currentV10DBObj.get("CONN_CUST_COL1"));
                fromOperator.put("connObj", currentV10DBObj);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fromOperator;
    }

//    public JSONObject updateFromColsObj(Map fromOpearatorMap,
    public Map updateFromColsObj(Map fromOpearatorMap, // ravi etl integration
            //JSONObject fromColumnsObj
            Map fromColumnsObj // ravi etl integration
    ) {
        try {
            if (fromOpearatorMap != null && !fromOpearatorMap.isEmpty()) {
                if (fromColumnsObj.containsKey(fromOpearatorMap.get("oldTableName"))) {
//                    JSONObject fromColumnsObjUpdated = fromColumnsObj;
                    fromColumnsObj.put(fromOpearatorMap.get("tableName"), fromColumnsObj.get(fromOpearatorMap.get("oldTableName")));
                    fromColumnsObj.remove(fromOpearatorMap.get("oldTableName"));
//                    fromColumnsObj = new JSONObject();
//                    for (Object tblName : fromColumnsObjUpdated.keySet()) {
//                        if (!String.valueOf(tblName).equalsIgnoreCase(String.valueOf(fromOpearatorMap.get("oldTableName")))) {
//                            fromColumnsObj.put(tblName, fromColumnsObjUpdated.get(tblName));
//                        }
//                    }

                }
            }
        } catch (Exception e) {
        }
        return fromColumnsObj;
    }

    public JSONObject processJobWithFilesAndTables(String loginUserName,
            String loginOrgnId,
            Map toOperator,
            Map transformationMap,
            List<Map> fromOperatorList,
            String jobId) {
        JSONObject resultObj = new JSONObject();
        Connection toConnection = null;
        Connection fromConnection = null;
        JCO.Client toJCOConnection = null;
        PreparedStatement toPreparedStatement = null;
        PreparedStatement fromPreparedStatement = null;
        try {
            if (transformationMap != null) {
                //JSONObject columnsObj = (JSONObject) transformationMap.get("columnsObj");
                Map columnsObj = (LinkedHashMap) transformationMap.get("columnsObj"); // ravi etl integration
                JSONObject tablesWhereClauseObj = (JSONObject) transformationMap.get("whereClauseObj");//               
                JSONObject orderByObj = (JSONObject) transformationMap.get("orderBy");//
                JSONObject appendValObj = (JSONObject) transformationMap.get("appendValObj");//
                JSONObject columnClauseObj = (JSONObject) transformationMap.get("columnClauseObj");//
                JSONObject selectTabObj = (JSONObject) transformationMap.get("selectTabObj");
                JSONObject defaultValuesObj = (JSONObject) transformationMap.get("defaultValObj");
                JSONObject normalizeOptionsObj = (JSONObject) transformationMap.get("normalizeOptionsObj");
                String joinQuery = (String) transformationMap.get("joinQuery");//
                String groupByQuery = (String) transformationMap.get("groupBy");
                String nativeSQL = "";
                JSONObject orderAndGroupByObj = new JSONObject();
                orderAndGroupByObj.put("orderByObj", orderByObj);
                orderAndGroupByObj.put("groupByQuery", groupByQuery);
                orderAndGroupByObj.put("nativeSQL", nativeSQL);
                if (!(selectTabObj != null && !selectTabObj.isEmpty())) {
                    selectTabObj = new JSONObject();
                }
                selectTabObj.put("jobId", jobId);
                if (columnsObj != null && !columnsObj.isEmpty()) {
                    try {
                        processETLLog(loginUserName,
                                loginOrgnId,
                                "Reading the transformations rules.",
                                "INFO", 10, "Y", jobId);
                    } catch (Exception e) {
                    }
                    //JSONObject fromColumnsObj = new JSONObject();			
                    Map fromColumnsObj = new LinkedHashMap(); // ravi etl integration
                    //JSONObject toColumnsObj = new JSONObject();			
                    Map toColumnsObj = new LinkedHashMap(); // ravi etl integration
                    for (Object toColumnName : columnsObj.keySet()) {
                        if (toColumnName != null && columnsObj.get(toColumnName) != null) {
                            String fromColumnStr = (String) columnsObj.get(toColumnName);
                            if (fromColumnStr != null
                                    && !"".equalsIgnoreCase(fromColumnStr)
                                    && !"null".equalsIgnoreCase(fromColumnStr)) {
                                String[] fromColumnArray = fromColumnStr.split(":");
                                if (fromColumnArray != null && fromColumnArray.length != 0) {
                                    if (fromColumnsObj != null && !fromColumnsObj.isEmpty()) {
                                        if (fromColumnsObj.containsKey(fromColumnArray[0])) {
                                            fromColumnsObj.put(fromColumnArray[0],
                                                    (fromColumnsObj.get(fromColumnArray[0]) + "," + fromColumnArray[1]));
                                        } else {
                                            fromColumnsObj.put(fromColumnArray[0],
                                                    (fromColumnArray[1]));
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
                        if (defaultColumns != null
                                && !defaultColumns.isEmpty()
                                && toColumns != null && !toColumns.isEmpty()) {
                            toColumns.addAll(defaultColumns);
                        }
                    }
                    if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
                        Set<String> columnClauseCols = columnClauseObj.keySet();
                        if (columnClauseCols != null
                                && !columnClauseCols.isEmpty()
                                && toColumns != null && !toColumns.isEmpty()) {
                            toColumns.addAll(columnClauseCols);
                        }
                    }

                    List<String> toColumnsList = new ArrayList<>();
                    toColumnsList.addAll(toColumns);
                    String toTableName = (String) toOperator.get("tableName");
                    int totalDataCount = 0;
                    int fileDataLastIndex = 0;
                    List<Map> nonJoinOpList = fromOperatorList;
                    String fileName = "";
                    String orginalName = (String) toOperator.get("userFileName");
                    String iconType = (String) toOperator.get("iconType");
                    JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
                    String timeStamp = String.valueOf(toOperator.get("timeStamp"));// ravi etl issues new
                    if (!(timeStamp != null
                            && !"".equalsIgnoreCase(timeStamp)
                            && !"null".equalsIgnoreCase(timeStamp))) {
                        timeStamp = "" + System.currentTimeMillis();
                        toOperator.put("timeStamp", timeStamp);
                    }
                    if ("XLSX".equalsIgnoreCase(iconType)) {
                        fileName = "V10ETLExport_" + timeStamp + ".xlsx";

                    } else if ("XLS".equalsIgnoreCase(iconType)) {
                        fileName = "V10ETLExport_" + timeStamp + ".xls";
                    } else if ("XML".equalsIgnoreCase(iconType)) {
                        fileName = "V10ETLExport_" + timeStamp + ".xml";
                    } else if ("CSV".equalsIgnoreCase(iconType)) {
                        fileName = "V10ETLExport_" + timeStamp + ".csv";
                    } else if ("TXT".equalsIgnoreCase(iconType)) {
                        fileName = "V10ETLExport_" + timeStamp + ".txt";
                    } else if ("JSON".equalsIgnoreCase(iconType)) {
                        fileName = "V10ETLExport_" + timeStamp + ".json";
//                    if ("XLSX".equalsIgnoreCase(iconType)) {
//                        fileName = "V10ETLExport_" + System.currentTimeMillis() + ".xlsx";
//
//                    } else if ("XLS".equalsIgnoreCase(iconType)) {
//                        fileName = "V10ETLExport_" + System.currentTimeMillis() + ".xls";
//                    } else if ("XML".equalsIgnoreCase(iconType)) {
//                        fileName = "V10ETLExport_" + System.currentTimeMillis() + ".xml";
//                    } else if ("CSV".equalsIgnoreCase(iconType)) {
//                        fileName = "V10ETLExport_" + System.currentTimeMillis() + ".csv";
//                    } else if ("TXT".equalsIgnoreCase(iconType)) {
//                        fileName = "V10ETLExport_" + System.currentTimeMillis() + ".txt";
//                    } else if ("JSON".equalsIgnoreCase(iconType)) {
//                        fileName = "V10ETLExport_" + System.currentTimeMillis() + ".json";
                    } else {
                        if (toTableName != null
                                && !"".equalsIgnoreCase(String.valueOf(toTableName))
                                && !"null".equalsIgnoreCase(String.valueOf(toTableName))) {
                            Object toConnectionObj = getConnection(toConnObj);
                            if (toConnectionObj instanceof Connection) {
                                toConnection = (Connection) toConnectionObj;
                            }
                            if (toConnection != null) {
                                String toTableInsertQuery = generateInsertQuery((String) toOperator.get("tableName"),
                                        toColumnsList);
                                System.out.println("insertQuery::::" + toTableInsertQuery);
                                toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
                                Map columnsTypeObj = getColumnsType((String) toOperator.get("tableName"),
                                        toColumnsList, toConnection);
                                selectTabObj.put("columnsTypeObj", columnsTypeObj);
                            }

                        }
                    }
                    if (!(toColumnsList != null && !toColumnsList.isEmpty())) {
                        toColumnsList.addAll(columnsObj.keySet());
                    }
                    List<Map> fromOperatorMapList = new ArrayList<>();
                    if (fromOperatorList != null && !fromOperatorList.isEmpty()) {
                        List fromFilesList = new ArrayList<>();
//                        Map trfmRulesDataMap = (Map) toOperator.get("trfmRules-data");
                        for (int i = 0; i < fromOperatorList.size(); i++) {
                            Map fromOpearator = fromOperatorList.get(i);
                            if (fromOpearator != null && !fromOpearator.isEmpty()) {
                                if ("file".equalsIgnoreCase(String.valueOf(fromOpearator.get("dragType")))) {
                                    Map fromOpearatorMap = getSelectedFileTablesData(fromOpearator,
                                            toOperator,
                                            fromColumnsObj,
                                            transformationMap,
                                            fileDataLastIndex,
                                            fileName);
                                    if (fromOpearatorMap != null && !fromOpearatorMap.isEmpty()) {
                                        fromOperatorMapList.add(fromOpearatorMap);
                                        fromFilesList.add(fromOpearatorMap.get("oldTableName"));
                                        if (fromColumnsObj.containsKey(fromOpearatorMap.get("oldTableName"))) {
                                            transformationMap = convertTransFrmRulsMapToParam(transformationMap, fromOpearatorMap);
                                            fromColumnsObj = updateFromColsObj(fromOpearatorMap, fromColumnsObj);

                                        }
                                    }

                                } else {
                                    Map fromOpearatorMap = getSelectedDBTablesData(fromOpearator,
                                            toOperator,
                                            fromColumnsObj,
                                            transformationMap);
                                    if (fromOpearatorMap != null && !fromOpearatorMap.isEmpty()) {
                                        fromOperatorMapList.add(fromOpearatorMap);
                                        if (fromColumnsObj.containsKey(fromOpearatorMap.get("oldTableName"))) {
                                            transformationMap = convertTransFrmRulsMapToParam(transformationMap, fromOpearatorMap);
                                            fromColumnsObj.put(fromOpearatorMap.get("tableName"), fromColumnsObj.get(fromOpearatorMap.get("oldTableName")));
                                            fromColumnsObj.remove(fromOpearatorMap.get("oldTableName"));
                                        }
                                    }
                                }

                            }

                        }
                        if (fromFilesList != null && !fromFilesList.isEmpty()) {
                            for (int i = 0; i < fromFilesList.size(); i++) {
                                Object oldTableName = fromFilesList.get(i);
                                if (fromColumnsObj != null
                                        && !fromColumnsObj.isEmpty()
                                        && fromColumnsObj.containsKey(oldTableName)) {
                                    fromColumnsObj.remove(oldTableName);
                                }

                            }
                        }

                    }
                    if (fromOperatorMapList != null && !fromOperatorMapList.isEmpty()) {
                        fromConnection = DriverManager.getConnection(dbURL, userName, password);
                        fromOperatorList = fromOperatorMapList;
                        // columnsObj = (JSONObject) transformationMap.get("columnsObj");
                        columnsObj = (LinkedHashMap) transformationMap.get("columnsObj"); // ravi etl integration
                        tablesWhereClauseObj = (JSONObject) transformationMap.get("whereClauseObj");
                        orderByObj = (JSONObject) transformationMap.get("orderBy");//
                        appendValObj = (JSONObject) transformationMap.get("appendValObj");//
                        columnClauseObj = (JSONObject) transformationMap.get("columnClauseObj");//
                        selectTabObj = (JSONObject) transformationMap.get("selectTabObj");
                        defaultValuesObj = (JSONObject) transformationMap.get("defaultValObj");
                        normalizeOptionsObj = (JSONObject) transformationMap.get("normalizeOptionsObj");
                        joinQuery = (String) transformationMap.get("joinQuery");//
                        groupByQuery = (String) transformationMap.get("groupBy");
                        nativeSQL = "";
                        orderAndGroupByObj = new JSONObject();
                        orderAndGroupByObj.put("orderByObj", orderByObj);
                        orderAndGroupByObj.put("groupByQuery", groupByQuery);
                        orderAndGroupByObj.put("nativeSQL", nativeSQL);
                        if (!(selectTabObj != null && !selectTabObj.isEmpty())) {
                            selectTabObj = new JSONObject();
                        }
                        selectTabObj.put("jobId", jobId);

                    }
                    if (toConnection != null) {
                        Map columnsTypeObj = getColumnsType((String) toOperator.get("tableName"), toColumnsList, toConnection);
                        selectTabObj.put("columnsTypeObj", columnsTypeObj);
                    }
                    LinkedHashMap joinQueryHashMapObj = (LinkedHashMap) transformationMap.get("joinQueryHashMapObj");
                    JSONObject joinQueryMapObj = new JSONObject();//
                    if (joinQueryHashMapObj != null && !joinQueryHashMapObj.isEmpty()) {
                        joinQueryMapObj.putAll(joinQueryHashMapObj);
                    }
                    if (selectTabObj != null && !selectTabObj.isEmpty()) {
                        //oldTableName
                        selectTabObj.remove("oldTableName");
                    }
                    if (joinQueryMapObj != null
                            && !joinQueryMapObj.isEmpty()) {

                        nonJoinOpList = fromOperatorList.stream()
                                .filter(opMap -> (opMap != null && !opMap.isEmpty()
                                && opMap.get("tableName") != null
                                && !"".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
                                && !"null".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
                                && !joinQueryMapObj.containsKey(String.valueOf(opMap.get("tableName"))))).collect(Collectors.toList());
                        try {
                            processETLLog(loginUserName,
                                    loginOrgnId, "Starting extract join tables data.", "INFO", 10, "Y", jobId);
                        } catch (Exception e) {
                        }
                        totalDataCount += processETLData(loginUserName,
                                loginOrgnId,
                                fromConnection,
                                toConnection,
                                fromOperatorList,
                                toOperator,
                                columnsObj,
                                fromColumnsObj,
                                tablesWhereClauseObj,
                                defaultValuesObj,
                                toPreparedStatement,
                                1,
                                1000,
                                totalDataCount,
                                toColumnsList,
                                joinQueryMapObj,
                                joinQuery,
                                fileDataLastIndex,
                                fileName,
                                1,
                                orderAndGroupByObj,
                                columnClauseObj,
                                selectTabObj,
                                normalizeOptionsObj
                        );

                    } else {
                        if (nonJoinOpList != null && !nonJoinOpList.isEmpty()) {
                            try {
                                processETLLog(loginUserName,
                                        loginOrgnId, "Starting extract non-join tables data.", "INFO", 10, "Y", jobId);
                            } catch (Exception e) {
                            }
                            for (int i = 0; i < nonJoinOpList.size(); i++) {
                                Map fromOperator = nonJoinOpList.get(i);
                                if (fromOperator != null && !fromOperator.isEmpty()) {
                                    totalDataCount += processETLData(loginUserName,
                                            loginOrgnId,
                                            fromConnection,
                                            toConnection,
                                            fromOperator,
                                            toOperator,
                                            columnsObj,
                                            fromColumnsObj,
                                            tablesWhereClauseObj,
                                            defaultValuesObj,
                                            toPreparedStatement,
                                            1,
                                            1000,
                                            totalDataCount,
                                            toColumnsList,
                                            fileDataLastIndex,
                                            fileName,
                                            1,
                                            orderAndGroupByObj,
                                            columnClauseObj,
                                            selectTabObj,
                                            normalizeOptionsObj
                                    );
                                }

                            }
                        }

                    }

//                    // need to drop the temp tables
                    for (int i = 0; i < fromOperatorMapList.size(); i++) {
                        Map fromOperator = fromOperatorMapList.get(i);
                        if (fromOperator != null
                                && fromOperator.containsKey("oldTableName")) {
                            try {
                                String dropTable = "DROP TABLE " + fromOperator.get("tableName");
                                System.out.println("dropTable:::" + dropTable);
                                fromPreparedStatement = fromConnection.prepareStatement(dropTable);
                                fromPreparedStatement.execute();
                            } catch (Exception e) {
                                continue;
                            }

                        }

                    }
                    System.out.println("totalDataCount::::" + totalDataCount);
                    if (totalDataCount != 0 && totalDataCount > 0) {
                        String message = totalDataCount + " Row(s) successfully extracted and loaded into target system.";
                        if (fileName != null && !"".equalsIgnoreCase(fileName) && !"null".equalsIgnoreCase(fileName)) {//orginalName
                            if (orginalName != null
                                    && !"".equalsIgnoreCase(orginalName)
                                    && !"null".equalsIgnoreCase(orginalName)) {
                                orginalName = orginalName.replaceAll("[^.a-zA-Z0-9]", "_");
                            }

                            message += " <br> <a href='#' style='color:#0071c5;' onclick=downloadExportedFile('" + fileName + "',\"" + orginalName + "\") >Click here to download the "
                                    + "" + iconType + ((orginalName != null
                                    && !"".equalsIgnoreCase(orginalName)
                                    && !"null".equalsIgnoreCase(orginalName)) ? "(" + orginalName + ")" : "") + " file</a>.";//
                        }

                        try {
                            processETLLog(loginUserName,
                                    loginOrgnId, message, "INFO", 10, "N", jobId);
                        } catch (Exception e) {
                        }
                        resultObj.put("Message", message);
                        resultObj.put("connectionFlag", "Y");

                    } else {
                        try {
                            processETLLog(loginUserName,
                                    loginOrgnId, totalDataCount + " Row(s) successfully extracted and loaded into target system.", "INFO", 10, "N", jobId);
                        } catch (Exception e) {
                        }

                        resultObj.put("Message", totalDataCount + " Row(s) successfully extracted and loaded into target system.");
                        resultObj.put("connectionFlag", "Y");
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (toPreparedStatement != null) {
                    toPreparedStatement.close();
                }
                if (toConnection != null) {
                    toConnection.close();
                }
                if (fromConnection != null) {
                    fromConnection.close();
                }
                if (toJCOConnection != null) {
                    toJCOConnection.disconnect();
                }
            } catch (Exception e) {
            }
        }
        return resultObj;
    }

    public Map convertTransFrmRulsMapToParam(Map trfmRulesDataMap, Map fromOperator) {
        Map paramMap = new HashMap<>();
        try {

            JSONObject orderByList = new JSONObject();
            String groupByColumns = "";
            String oldTableName = (String) fromOperator.get("oldTableName");
            String newTableName = (String) fromOperator.get("tableName");
            String dragType = (String) fromOperator.get("dragType");
            if (trfmRulesDataMap != null && !trfmRulesDataMap.isEmpty()) {
                JSONObject orderByDataMapList = (JSONObject) trfmRulesDataMap.get("orderBy");
                if (orderByDataMapList != null && !orderByDataMapList.isEmpty()) {
                    for (Object orderBykeyIndex : orderByDataMapList.keySet()) {
                        JSONObject orderByDataMap = (JSONObject) orderByDataMapList.get(orderBykeyIndex);
                        JSONObject orderByObj = new JSONObject();
                        if (orderByDataMap != null && !orderByDataMap.isEmpty()) {
                            String orderByColumn = String.valueOf(orderByDataMap.get("columnName"));
                            if (orderByColumn.contains(oldTableName)) {
                                if (orderByColumn.contains(":")) {
                                    String[] orderByColumnArray = orderByColumn.split(":");
                                    if (orderByColumnArray[0].equalsIgnoreCase(oldTableName)) {
                                        orderByColumn = orderByColumn.replaceAll(oldTableName, newTableName);
                                    }
//                                    if (!"file".equalsIgnoreCase(dragType)) {
//                                        String[] orderByColumnDb = orderByColumnArray[0].split("[.]");
//                                        if (orderByColumnDb[0].equalsIgnoreCase(oldTableName)) {
//                                            orderByColumn = orderByColumn.replaceAll(oldTableName, newTableName);
//                                        }
//                                    } else {
//                                        if (orderByColumnArray[0].equalsIgnoreCase(oldTableName)) {
//                                            orderByColumn = orderByColumn.replaceAll(oldTableName, newTableName);
//                                        }
//                                    }
                                } else {
                                    orderByColumn = orderByColumn.replaceAll(oldTableName, newTableName);
                                }
//                                orderByColumn = orderByColumn.replaceAll(oldTableName, newTableName);
                                if ("file".equalsIgnoreCase(dragType)) {
                                    orderByColumn = orderByColumn.trim().replaceAll(" ", "_");
                                }
                            }
                            orderByObj.put("columnName", orderByColumn);
                            orderByObj.put("direction", orderByDataMap.get("order"));
                            orderByList.put(orderBykeyIndex, orderByObj);
                        }
                    }
                }// orderByDataMapList end
                paramMap.put("orderBy", orderByList);
                String groupByCols = (String) trfmRulesDataMap.get("groupBy");
                if (groupByCols != null
                        && !"".equalsIgnoreCase(groupByCols)
                        && !"null".equalsIgnoreCase(groupByCols)) {
                    String[] groupByColsArray = groupByCols.split(",");
                    if (groupByColsArray != null && groupByColsArray.length != 0) {
                        for (int i = 0; i < groupByColsArray.length; i++) {
                            String groupByColumn = groupByColsArray[i];
                            if (groupByColumn != null
                                    && !"".equalsIgnoreCase(groupByColumn)
                                    && !"null".equalsIgnoreCase(groupByColumn)
                                    && groupByColumn.contains(oldTableName)) {
                                if (groupByColumn.contains(":")) {
                                    String[] groupByColumnArray = groupByColumn.split(":");
                                    if (groupByColumnArray[0].equalsIgnoreCase(oldTableName)) {
                                        groupByColumn = groupByColumn.replaceAll(oldTableName, newTableName);
                                    }
//                                    if (!"file".equalsIgnoreCase(dragType)) {
//                                        String[] groupByColumnDb = groupByColumnArray[0].split("[.]");
//                                        if (groupByColumnDb[0].equalsIgnoreCase(oldTableName)) {
//                                            groupByColumn = groupByColumn.replaceAll(oldTableName, newTableName);
//                                        }
//                                    } else {
//                                        if (groupByColumnArray[0].equalsIgnoreCase(oldTableName)) {
//                                            groupByColumn = groupByColumn.replaceAll(oldTableName, newTableName);
//                                        }
//                                    }
                                } else {
                                    groupByColumn = groupByColumn.replaceAll(oldTableName, newTableName);
                                }

                                if ("file".equalsIgnoreCase(dragType)) {
                                    groupByColumn = groupByColumn.trim().replaceAll(" ", "_");
                                }
                            }
                            groupByColumns += "" + groupByColumn + ",";
                        }
                    }
                }
                if (groupByColumns != null
                        && !"".equalsIgnoreCase(groupByColumns)
                        && !"null".equalsIgnoreCase(groupByColumns)) {
                    groupByColumns = new PilogUtilities().trimChar(groupByColumns, ',');
                }
                paramMap.put("groupBy", groupByColumns);

                LinkedHashMap joinQueryObj = (LinkedHashMap) trfmRulesDataMap.get("joinQueryHashMapObj");
                String joinQuery = "";
                LinkedHashMap joinQueryMapObj = new LinkedHashMap();
                if (joinQueryObj != null && !joinQueryObj.isEmpty()) {
                    for (Object joinKey : joinQueryObj.keySet()) {
                        Object joinObj = joinQueryObj.get(joinKey);
                        if (joinObj instanceof String) {
                            String masterTableName = String.valueOf(joinObj);
                            if (masterTableName != null
                                    && !"".equalsIgnoreCase(masterTableName)
                                    && masterTableName.equalsIgnoreCase(oldTableName)) {
                                masterTableName = newTableName;
                            }
                            joinQueryMapObj.put(masterTableName, masterTableName);
                            joinQuery += " " + masterTableName;
                        } else if (joinObj instanceof JSONObject) {
                            JSONObject childJoinObjUpdated = new JSONObject();
                            JSONObject childJoinObj = (JSONObject) joinObj;
//                            String childJoinStr = childJoinObj.toJSONString();
////                            childJoinStr = childJoinStr.replaceAll(oldTableName, newTableName);
//                            
//                            childJoinObj = (JSONObject) JSONValue.parse(childJoinStr);
                            String childTableName = (String) joinKey;
                            if (childTableName != null
                                    && !"".equalsIgnoreCase(childTableName)
                                    && childTableName.equalsIgnoreCase(oldTableName)) {
                                childTableName = newTableName;
                            }

                            int j = 0;
                            for (Object joinObjKey : childJoinObj.keySet()) {
                                JSONObject joinMappedColumnObj = (JSONObject) childJoinObj.get(joinObjKey);

                                if (joinMappedColumnObj != null
                                        && !joinMappedColumnObj.isEmpty()) {
                                    String childTableColumn = "";
                                    if (joinMappedColumnObj.get("childTableColumn") != null
                                            && !"".equalsIgnoreCase(String.valueOf(joinMappedColumnObj.get("childTableColumn")))
                                            && !"null".equalsIgnoreCase(String.valueOf(joinMappedColumnObj.get("childTableColumn")))) {//childTableColumn
                                        String[] childTableColumnArray = String.valueOf(joinMappedColumnObj.get("childTableColumn")).split(":");
//                                        if (!"file".equalsIgnoreCase(dragType)) {
//                                            String[] childTableColumnArrayDb = childTableColumnArray[0].split("[.]");
//                                            if (childTableColumnArrayDb[0].equalsIgnoreCase(oldTableName)) {
//                                               joinMappedColumnObj.put("childTableColumn",
//                                                        String.valueOf(joinMappedColumnObj.get("childTableColumn")).replaceAll(oldTableName, newTableName));
//                                            }
//                                        }else{
//                                            if (childTableColumnArray[0].equalsIgnoreCase(oldTableName)) {
//                                                joinMappedColumnObj.put("childTableColumn",
//                                                        String.valueOf(joinMappedColumnObj.get("childTableColumn")).replaceAll(oldTableName, newTableName));
//                                            }
//                                        }
                                        if (childTableColumnArray[0].equalsIgnoreCase(oldTableName)) {
                                            joinMappedColumnObj.put("childTableColumn",
                                                    String.valueOf(joinMappedColumnObj.get("childTableColumn")).replaceAll(oldTableName, newTableName));
                                        }
                                        childTableColumn = String.valueOf(joinMappedColumnObj.get("childTableColumn"))
                                                .replace(":", ".");
                                        if ("file".equalsIgnoreCase(dragType)) {
                                            joinMappedColumnObj.put("childTableColumn",
                                                    String.valueOf(joinMappedColumnObj.get("childTableColumn")).replaceAll(" ", "_"));
                                            childTableColumn = childTableColumn.trim().replaceAll(" ", "_");
                                        }

                                    }
                                    String masterTableColumn = "";
                                    if (joinMappedColumnObj.get("masterTableColumn") != null
                                            && !"".equalsIgnoreCase(String.valueOf(joinMappedColumnObj.get("masterTableColumn")))
                                            && !"null".equalsIgnoreCase(String.valueOf(joinMappedColumnObj.get("masterTableColumn")))) {//childTableColumn

                                        String[] masterTableColumnArray = String.valueOf(joinMappedColumnObj.get("masterTableColumn")).split(":");
//                                        if (!"file".equalsIgnoreCase(dragType)) {
//                                            String[] masterTableColumnArrayDb = masterTableColumnArray[0].split("[.]");
//                                            if (masterTableColumnArrayDb[0].equalsIgnoreCase(oldTableName)) {
//                                                joinMappedColumnObj.put("masterTableColumn",
//                                                        String.valueOf(joinMappedColumnObj.get("masterTableColumn")).replaceAll(oldTableName, newTableName));
//                                            }
//                                        } else {
//                                            if (masterTableColumnArray[0].equalsIgnoreCase(oldTableName)) {
//                                                joinMappedColumnObj.put("masterTableColumn",
//                                                        String.valueOf(joinMappedColumnObj.get("masterTableColumn")).replaceAll(oldTableName, newTableName));
//                                            }
//                                        }
                                        if (masterTableColumnArray[0].equalsIgnoreCase(oldTableName)) {
                                            joinMappedColumnObj.put("masterTableColumn",
                                                    String.valueOf(joinMappedColumnObj.get("masterTableColumn")).replaceAll(oldTableName, newTableName));
                                        }
                                        masterTableColumn = String.valueOf(joinMappedColumnObj.get("masterTableColumn")).replace(":", ".");
                                        if ("file".equalsIgnoreCase(dragType)) {
                                            joinMappedColumnObj.put("masterTableColumn",
                                                    String.valueOf(joinMappedColumnObj.get("masterTableColumn")).replaceAll(" ", "_"));
                                            masterTableColumn = masterTableColumn.trim().replaceAll(" ", "_");
                                        }
                                    }
                                    if (j == 0) {
                                        joinQuery += " " + joinMappedColumnObj.get("joinType") + " " + childTableName + " ON ";
                                    }
                                    String colValue = String.valueOf(joinMappedColumnObj.get("staticValue"));
                                    if (colValue != null
                                            && !"".equalsIgnoreCase(colValue)
                                            && !"null".equalsIgnoreCase(colValue)) {
                                        if (joinMappedColumnObj.get("operator") != null
                                                && !"".equalsIgnoreCase(String.valueOf(joinMappedColumnObj.get("operator")))
                                                && !"null".equalsIgnoreCase(String.valueOf(joinMappedColumnObj.get("operator")))
                                                && ("IN".equalsIgnoreCase(String.valueOf(joinMappedColumnObj.get("operator")))
                                                || "NOT IN".equalsIgnoreCase(String.valueOf(joinMappedColumnObj.get("operator"))))) {
                                            colValue = "('" + colValue.replaceAll("##", "','") + "')";
                                        } else {
                                            colValue = "'" + colValue + "'";
                                        }

                                    }

                                    joinQuery += " " + childTableColumn + " " + joinMappedColumnObj.get("operator") + " "
                                            + " " + ((joinMappedColumnObj.get("staticValue") != null
                                            && !"".equalsIgnoreCase(String.valueOf(joinMappedColumnObj.get("staticValue")))
                                            && !"null".equalsIgnoreCase(String.valueOf(joinMappedColumnObj.get("staticValue"))))
                                            ? "" + colValue + "" : masterTableColumn);
                                    if (j != childJoinObj.size() - 1) {
                                        joinQuery += " AND ";
                                    }

                                    childJoinObjUpdated.put(joinObjKey, joinMappedColumnObj);
                                }
                                j++;
                            }
                            joinQueryMapObj.put(childTableName, childJoinObjUpdated);
                        }
                    }

                }
                paramMap.put("joinQueryMapObj", trfmRulesDataMap.get("joinQueryMapObj"));
                paramMap.put("joinQueryHashMapObj", joinQueryMapObj);
                paramMap.put("joinQuery", joinQuery);

                //JSONObject columnsObj = (JSONObject) trfmRulesDataMap.get("columnsObj");
                Map columnsObj = (LinkedHashMap) trfmRulesDataMap.get("columnsObj");
                JSONObject defaultValObjMap = (JSONObject) trfmRulesDataMap.get("defaultValObj");
                JSONObject appendValObjMap = (JSONObject) trfmRulesDataMap.get("appendValObj");
                JSONObject columnClauseObjMap = (JSONObject) trfmRulesDataMap.get("columnClauseObj");

                JSONObject appendValObj = new JSONObject();
                //JSONObject colsObj = new JSONObject(); // ravi etl integration
                Map colsObj = new LinkedHashMap();
                JSONObject defaultValObj = new JSONObject();
                JSONObject columnClauseObj = new JSONObject();

                if (columnsObj != null && !columnsObj.isEmpty()) {
                    for (Object fromTableColumnObj : columnsObj.keySet()) {
                        String fromTableColumn = String.valueOf(fromTableColumnObj);
                        String toTableColumnName = String.valueOf(columnsObj.get(fromTableColumnObj));
                        if (fromTableColumn != null
                                && !"".equalsIgnoreCase(fromTableColumn)
                                && !"null".equalsIgnoreCase(fromTableColumn) //  && !toTableColumnName.contains("N/A")
                                ) {
                            if (fromTableColumn.contains(":")) {
                                String[] fromTableColumnArray = fromTableColumn.split(":");
//                                if (!"file".equalsIgnoreCase(dragType)) {
//                                    String[] fromTableColumnDb = fromTableColumnArray[0].split("[.]");
//                                    if (fromTableColumnDb[0].equalsIgnoreCase(oldTableName)) {
//                                        fromTableColumn = fromTableColumn.replaceAll(oldTableName, newTableName);
//                                    }
//                                } else {
//                                    if (fromTableColumnArray[0].equalsIgnoreCase(oldTableName)) {
//                                        fromTableColumn = fromTableColumn.replaceAll(oldTableName, newTableName);
//                                    }
//                                }

                                if (fromTableColumnArray[0].equalsIgnoreCase(oldTableName)) {
//                                    fromTableColumn = fromTableColumn.replaceAll(oldTableName, newTableName);
                                    fromTableColumn = fromTableColumn.replaceAll(oldTableName + ":", newTableName + ":"); // ravi etl integration
                                }
                            } else if (fromTableColumn.contains(".")) {  // // ravi etl integration // added else if
                                fromTableColumn = fromTableColumn.replaceAll(oldTableName + ".", newTableName + ".");
                            }
//                            else {  // ravi etl integration // commented else 
//                                fromTableColumn = fromTableColumn.replaceAll(oldTableName, newTableName);
//                            }

                            if ("file".equalsIgnoreCase(dragType)) {
                                fromTableColumn = fromTableColumn.trim().replaceAll(" ", "_");
                            }
                        }
                        if (toTableColumnName != null
                                && !"".equalsIgnoreCase(toTableColumnName)
                                && !"null".equalsIgnoreCase(toTableColumnName) //  && !toTableColumnName.contains("N/A")
                                ) {
                            if (toTableColumnName.contains(":")) {
                                String[] toTableColumnNameArray = toTableColumnName.split(":");
//                                if (!"file".equalsIgnoreCase(dragType)) {
//                                    String[] toTableColumnNameDb = toTableColumnNameArray[0].split("[.]");
//                                    if (toTableColumnNameDb[0].equalsIgnoreCase(oldTableName)) {
//                                        toTableColumnName = toTableColumnName.replaceAll(oldTableName, newTableName);
//                                    }
//                                } else {
//                                    if (toTableColumnNameArray[0].equalsIgnoreCase(oldTableName)) {
//                                        toTableColumnName = toTableColumnName.replaceAll(oldTableName, newTableName);
//                                    }
//                                }
                                if (toTableColumnNameArray[0].equalsIgnoreCase(oldTableName)) {
                                    toTableColumnName = toTableColumnName.replaceAll(oldTableName, newTableName);
                                }
                            } else {
                                toTableColumnName = toTableColumnName.replaceAll(oldTableName, newTableName);
                            }

//                            toTableColumnName = toTableColumnName.replaceAll(oldTableName, newTableName);
                            if ("file".equalsIgnoreCase(dragType)) {
                                toTableColumnName = toTableColumnName.trim().replaceAll(" ", "_");
                            }
                        }
                        colsObj.put(fromTableColumn, toTableColumnName);
                        if (defaultValObjMap.get(String.valueOf(columnsObj.get(fromTableColumnObj))) != null
                                && !"".equalsIgnoreCase(String.valueOf(defaultValObjMap.get(String.valueOf(columnsObj.get(fromTableColumnObj)))))
                                && !"null".equalsIgnoreCase(String.valueOf(defaultValObjMap.get(String.valueOf(columnsObj.get(fromTableColumnObj)))))) {
                            defaultValObj.put(toTableColumnName, defaultValObjMap.get(String.valueOf(columnsObj.get(fromTableColumnObj))));
                        }
                        if (appendValObjMap.get(fromTableColumnObj) != null
                                && !"".equalsIgnoreCase(String.valueOf(appendValObjMap.get(fromTableColumnObj)))
                                && !"null".equalsIgnoreCase(String.valueOf(appendValObjMap.get(fromTableColumnObj)))) {
                            appendValObj.put(toTableColumnName, appendValObjMap.get(fromTableColumnObj));
                        }
                        if (columnClauseObjMap.get(String.valueOf(columnsObj.get(fromTableColumnObj))) != null
                                && !"".equalsIgnoreCase(String.valueOf(columnClauseObjMap.get(String.valueOf(columnsObj.get(fromTableColumnObj)))))
                                && !"null".equalsIgnoreCase(String.valueOf(columnClauseObjMap.get(String.valueOf(columnsObj.get(fromTableColumnObj)))))) {
                            columnClauseObj.put(toTableColumnName, columnClauseObjMap.get(String.valueOf(columnsObj.get(fromTableColumnObj))));
                        }

                    }
                }
                paramMap.put("columnsObj", colsObj);
                paramMap.put("defaultValObj", defaultValObj);
                paramMap.put("appendValObj", appendValObj);
                paramMap.put("columnClauseObj", columnClauseObj);

                JSONObject whereClauseDataObject = (JSONObject) trfmRulesDataMap.get("whereClauseDataObject");
                JSONObject whereClauseDataObjectUpdated = new JSONObject();
                JSONObject whereClauseObject = new JSONObject();
                if (whereClauseDataObject != null && !whereClauseDataObject.isEmpty()) {
                    for (Object whereTableName : whereClauseDataObject.keySet()) {
                        //JSONObject whereClauseData = (JSONObject) whereClauseDataObject.get(whereTableName);
                        Map whereClauseData = (HashMap) whereClauseDataObject.get(whereTableName); // ravi updated code changes
                        JSONObject whereClauseDataUpdated = new JSONObject();
                        String tableName = "";
                        String whereCluaseQuery = "";
                        if (whereClauseData != null && !whereClauseData.isEmpty()) {
                            int j = 0;
                            for (Object index : whereClauseData.keySet()) {
                                Map whereClauseDataMap = (Map) whereClauseData.get(index);
                                if (whereClauseDataMap != null && !whereClauseDataMap.isEmpty()) {
                                    Map whereClauseDataMapUpdated = new HashMap();
//                                    String tableName = String.valueOf(whereTableName);
                                    String columnName = (String) whereClauseDataMap.get("columnName");
                                    String operator = (String) whereClauseDataMap.get("operator");
                                    String andOrOperator = (String) whereClauseDataMap.get("andOrOperator");
                                    String staticValue = (String) whereClauseDataMap.get("staticValue");
                                    if (columnName != null
                                            && !"".equalsIgnoreCase(columnName)
                                            && !"null".equalsIgnoreCase(columnName)) {
//                                        columnName = columnName.replaceAll(oldTableName, newTableName);
                                        tableName = columnName.split(":")[0];
                                        if (tableName != null
                                                && !"".equalsIgnoreCase(tableName)
                                                && tableName.equalsIgnoreCase(oldTableName)) {
                                            tableName = newTableName;
                                        }
                                        columnName = columnName.replaceAll(":", ".");

                                        if (staticValue != null
                                                && !"".equalsIgnoreCase(staticValue)
                                                && !"null".equalsIgnoreCase(staticValue)) {
                                            if ("IN".equalsIgnoreCase(operator)
                                                    || "NOT IN".equalsIgnoreCase(operator)) {
                                                staticValue = "('" + staticValue.replaceAll("#", "','") + "')";
                                            } else {
                                                staticValue = "'" + staticValue + "'";
                                            }
                                        }
                                        whereCluaseQuery += " " + columnName + " " + operator + " " + staticValue + " ";
                                        if (j != whereClauseData.size() - 1) {
                                            whereCluaseQuery += " " + andOrOperator + " ";
                                        }
                                        whereClauseDataMapUpdated.put("columnName", columnName);
                                        whereClauseDataMapUpdated.put("operator", operator);
                                        whereClauseDataMapUpdated.put("andOrOperator", andOrOperator);
                                        whereClauseDataMapUpdated.put("staticValue", staticValue);
                                        whereClauseDataUpdated.put(j, whereClauseDataMapUpdated);
                                    }
                                }
                                j++;
                            }

                            if (tableName != null
                                    && !"".equalsIgnoreCase(tableName)
                                    && !"null".equalsIgnoreCase(tableName)
                                    && whereCluaseQuery != null
                                    && !"".equalsIgnoreCase(whereCluaseQuery)
                                    && !"null".equalsIgnoreCase(whereCluaseQuery)) {
                                whereClauseObject.put(tableName, whereCluaseQuery);
                                whereClauseDataObjectUpdated.put(tableName, whereClauseDataUpdated);
                            }
                        }
                    }
                }
                paramMap.put("whereClauseObj", whereClauseObject);
                paramMap.put("whereClauseDataObject", whereClauseDataObjectUpdated);

                JSONObject selectTabObj = (JSONObject) trfmRulesDataMap.get("selectTabObj");

                if (!(selectTabObj != null && !selectTabObj.isEmpty())) {
                    selectTabObj = new JSONObject();
                }
                selectTabObj.put("columnClauseObj", columnClauseObj);
                selectTabObj.put("appendValObj", appendValObj);
                paramMap.put("selectTabObj", selectTabObj);
                String nativeSQL = (String) trfmRulesDataMap.get("nativeSQL");
                if (nativeSQL != null
                        && !"".equalsIgnoreCase(nativeSQL)
                        && !"null".equalsIgnoreCase(nativeSQL)) {
                    paramMap.put("nativeSQL", nativeSQL);
                }// end Native SQL Query                
                JSONObject normalizeOptionsObj = (JSONObject) trfmRulesDataMap.get("normalizeOptionsObj");
                if (normalizeOptionsObj != null && !normalizeOptionsObj.isEmpty()) {
                    String normalizeOptionsObjStr = normalizeOptionsObj.toJSONString();
                    normalizeOptionsObjStr = normalizeOptionsObjStr.replaceAll(oldTableName, newTableName);
                    normalizeOptionsObj = (JSONObject) JSONValue.parse(normalizeOptionsObjStr);
                    paramMap.put("normalizeOptionsObj", normalizeOptionsObj);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return paramMap;
    }

    public List processETLBapiJoinData(String sessionUserName, //  join
            String orgnId,
            Connection fromConnection,
            Connection toConnection,
            List<Map> fromOperatorList,
            Map toOperator,
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            //JSONObject fromColumnsObj,
            Map fromColumnsObj, // ravi etl integration
            JSONObject tablesWhereClauseObj,
            JSONObject defaultValuesObj,
            PreparedStatement toPreparedStatement,
            int start,
            int limit,
            int totalDataCount,
            List<String> toColumnsList,
            JSONObject joinQueryMapObj,
            String joinQuery,
            int fileDataLastIndex,
            String fileName,
            int logSequenceNo,
            JSONObject appendValObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj,
            JSONObject normalizeOptionsObj
    ) {
        List totalDataList = new ArrayList();
        try {
            if (selectTabObj != null
                    && !selectTabObj.isEmpty()) {
                selectTabObj.put("columnClauseObj", columnClauseObj);
                String minRows = String.valueOf(selectTabObj.get("minRows"));
                if (minRows != null
                        && !"".equalsIgnoreCase(minRows)
                        && !"null".equalsIgnoreCase(minRows)
                        && NumberUtils.isNumber(minRows)) {
                    if (logSequenceNo == 1) {
                        start = Integer.parseInt(minRows);
                    }
                }
                String maxRows = String.valueOf(selectTabObj.get("maxRows"));
                if (maxRows != null
                        && !"".equalsIgnoreCase(maxRows)
                        && !"null".equalsIgnoreCase(maxRows)
                        && !"0".equalsIgnoreCase(maxRows)
                        && NumberUtils.isNumber(maxRows)) {
                    int maxRowsCount = Integer.parseInt(maxRows);
                    if (logSequenceNo == 1) {
                        if (((maxRowsCount - start)) <= limit) {
                            limit = (maxRowsCount - start);
                        }
                    } else {
                        if (((maxRowsCount - start) + 1) <= limit) {
                            limit = (maxRowsCount - start) + 1;
                        }
                    }

                }

            }
            Map fromOperator = fromOperatorList.get(0);
            int end = start + limit - 1;
            if (start == 0) {
                end = limit;
            }
            try {
                processETLLog(sessionUserName,
                        orgnId, "Fetching from " + start + " to " + end + " record(s).", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
//                        (String) httpSession.getAttribute("ssOrgId"), "Fetching next 1000 record(s).", "INFO", logSequenceNo, "Y");
            } catch (Exception e) {
            }

            if (limit > 0) {
                List dataList = getSelectedJoinColumnsData((JSONObject) fromOperator.get("connObj"),
                        fromConnection,
                        fromColumnsObj,
                        start,
                        limit,
                        tablesWhereClauseObj, joinQueryMapObj, joinQuery,
                        sessionUserName,
                        orgnId,
                        appendValObj,
                        columnClauseObj,
                        selectTabObj,
                        toConnection
                );

                totalDataList = (List) selectTabObj.get("totalDataList");
                if (!(totalDataList != null && !totalDataList.isEmpty())) {
                    totalDataList = new ArrayList();
                }
                totalDataList.addAll(dataList);
                selectTabObj.put("totalDataList", totalDataList);

                if (dataList.size() >= 1000) {
                    start = start + limit;
                    processETLBapiJoinData(sessionUserName, //  join
                            orgnId,
                            fromConnection,
                            toConnection,
                            fromOperatorList,
                            toOperator,
                            // columnsObj,
                            columnsObj, // ravi etl integration
                            // fromColumnsObj,
                            fromColumnsObj, // ravi etl integration
                            tablesWhereClauseObj,
                            defaultValuesObj,
                            toPreparedStatement,
                            start,
                            limit,
                            totalDataCount,
                            toColumnsList,
                            joinQueryMapObj,
                            joinQuery,
                            fileDataLastIndex,
                            fileName,
                            logSequenceNo,
                            appendValObj,
                            columnClauseObj,
                            selectTabObj,
                            normalizeOptionsObj);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();

        }
        return totalDataList;
    }

    public List processETLBapiData(String sessionUserName, // non join
            String orgnId,
            Connection fromConnection,
            Connection toConnection,
            Map fromOperator,
            Map toOperator,
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            //JSONObject fromColumnsObj,
            Map fromColumnsObj, // ravi etl integration
            JSONObject tablesWhereClauseObj,
            JSONObject defaultValuesObj,
            PreparedStatement toPreparedStatement,
            int start,
            int limit,
            int totalDataCount,
            List<String> toColumnsList,
            int fileDataLastIndex,
            String fileName,
            int logSequenceNo,
            JSONObject appendValObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj,
            JSONObject normalizeOptionsObj
    ) {
        Object resultObj = null;
        List totalDataList = new ArrayList();
        try {

            if (selectTabObj != null
                    && !selectTabObj.isEmpty()) {
                selectTabObj.put("columnClauseObj", columnClauseObj);
                String minRows = String.valueOf(selectTabObj.get("minRows"));
                if (minRows != null
                        && !"".equalsIgnoreCase(minRows)
                        && !"null".equalsIgnoreCase(minRows)
                        && NumberUtils.isNumber(minRows)) {
                    if (logSequenceNo == 1) {
                        start = Integer.parseInt(minRows);
                    }
                }
                String maxRows = String.valueOf(selectTabObj.get("maxRows"));
                if (maxRows != null
                        && !"".equalsIgnoreCase(maxRows)
                        && !"null".equalsIgnoreCase(maxRows)
                        && NumberUtils.isNumber(maxRows)) {
                    int maxRowsCount = Integer.parseInt(maxRows);
                    if (logSequenceNo == 1) {
                        if (((maxRowsCount - start)) <= limit) {
                            limit = (maxRowsCount - start);
                        }
                    } else {
                        if (((maxRowsCount - start) + 1) <= limit) {
                            limit = (maxRowsCount - start) + 1;
                        }
                    }
                }

            }
            int end = start + limit - 1;
            if (start == 0) {
                end = limit;
            }
            try {
                processETLLog(sessionUserName,
                        orgnId, "Fetching from " + start + " to " + end + " record(s).", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception e) {
            }

            if (limit > 0) {
                List dataList = getSelectedColumnsData((JSONObject) fromOperator.get("connObj"),
                        fromConnection,
                        fromColumnsObj,
                        start,
                        limit,
                        tablesWhereClauseObj, sessionUserName,
                        orgnId, appendValObj, columnClauseObj, selectTabObj, toConnection);

                totalDataList = (List) selectTabObj.get("totalDataList");
                if (!(totalDataList != null && !totalDataList.isEmpty())) {
                    totalDataList = new ArrayList();
                }
                totalDataList.addAll(dataList);
                selectTabObj.put("totalDataList", totalDataList);

                if (dataList.size() >= 1000) {
                    start = start + limit;
                    processETLBapiData(sessionUserName, // non join
                            orgnId,
                            fromConnection,
                            toConnection,
                            fromOperator,
                            toOperator,
                            //JSONObject columnsObj,
                            columnsObj, // ravi etl integration
                            //JSONObject fromColumnsObj,
                            fromColumnsObj, // ravi etl integration
                            tablesWhereClauseObj,
                            defaultValuesObj,
                            toPreparedStatement,
                            start,
                            limit,
                            totalDataCount,
                            toColumnsList,
                            fileDataLastIndex,
                            fileName,
                            logSequenceNo,
                            appendValObj,
                            columnClauseObj,
                            selectTabObj,
                            normalizeOptionsObj
                    );
                }
            }

        } catch (Exception e) {
            e.printStackTrace();

        }
        return totalDataList;
    }

    public String getSelectedColumnsDataQuery(
            Map columnsObj,// ravi etl integration
            Map tableColsObj,// ravi etl integration
            JSONObject tablesObj,
            JSONObject orderAndGroupByObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj,
            Map totalColumnsObj,
            Map defaultValIndxObj,
            Map colClauseIndxObj
    ) {
        String query = "";
        if (totalColumnsObj != null && !totalColumnsObj.isEmpty()) {

//                            String rowNumStr = "ROWNUM AS RNO,";
            List<String> fromTablesList = new ArrayList(tableColsObj.keySet());
            String tableName = fromTablesList.get(0);

            query = "SELECT ";
            if ((selectTabObj != null
                    && !selectTabObj.isEmpty()
                    && "Y".equalsIgnoreCase(String.valueOf(selectTabObj.get("uniqueRowsFlag"))))) {//uniqueRowsFlag
                query += " DISTINCT ";
            }
            String fetchColumns = "";
            List<String> columnsList = new ArrayList(totalColumnsObj.values());
            // List<String> columnsList = new ArrayList(Arrays.asList(colsObj.split(",")));
            Set<String> items = new HashSet<>();
            Set matchedColsSet = columnsList.stream()
                    .filter(n -> !items.add(n)) // Set.add() returns false if the element was already in the set.
                    .collect(Collectors.toSet());

            for (int j = 0; j < columnsList.size(); j++) {

                String columnName = String.valueOf(columnsList.get(j)).replace(":", ".");
                if (columnName.contains(tableName)) {

                    if (columnName.contains("FROMCOL_")) {
                        if (defaultValIndxObj.get(columnName) != null) {
                            fetchColumns += "'" + defaultValIndxObj.get(columnName) + "' ";
                        } else if (colClauseIndxObj.get(columnName) != null) {
                            fetchColumns += colClauseIndxObj.get(columnName);
                        }

                    }

                    if (matchedColsSet != null && !matchedColsSet.isEmpty() && matchedColsSet.contains(columnsList.get(j))) {
                        fetchColumns += columnName + " AS " + columnName + "_0" + j;
                    } else {
                        fetchColumns += columnName;
                    }
                    if (j != columnsList.size() - 1) {
                        fetchColumns += ",";
                    }
                }
            }

            columnsList = new ArrayList(Arrays.asList(fetchColumns.split(",")));
            query += " " + fetchColumns;
//                            if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
//                                for (Object toColumnName : columnClauseObj.keySet()) {
//                                    if (columnClauseObj.get(toColumnName) instanceof JSONObject) {
//                                        JSONObject funObj = (JSONObject) columnClauseObj.get(toColumnName);
//                                        if (funObj != null
//                                                && !funObj.isEmpty()) {
//                                            String functionMainObjStr = (String) funObj.get("funobjstr");
//                                            if (functionMainObjStr != null
//                                                    && !"".equalsIgnoreCase(functionMainObjStr)
//                                                    && !"null".equalsIgnoreCase(functionMainObjStr)) {
//                                                JSONObject functionMainObj = (JSONObject) JSONValue.parse(functionMainObjStr);
//                                                if (functionMainObj != null && !functionMainObj.isEmpty()
//                                                        && ("QUERY".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
//                                                        || "COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE")))) {//mainFunStr
//                                                    if ("DM_COL_AGGREGATE".equalsIgnoreCase(String.valueOf(functionMainObj.get("HL_FUN_ID")))) {
//                                                        String aggTableName = String.valueOf(functionMainObj.get("COLUMN_NAME"));
//                                                        if (aggTableName != null
//                                                                && !"".equalsIgnoreCase(aggTableName)
//                                                                && !"null".equalsIgnoreCase(aggTableName)
//                                                                && aggTableName.contains(":")) {
//                                                            aggTableName = aggTableName.split(":")[0];
//                                                        }
//
//                                                        query += ",(SELECT " + String.valueOf(functionMainObj.get("mainFunStr")).replace(":", ".") + " FROM " + aggTableName + " ) AS  " + toColumnName;
//                                                    } else {
//                                                        query += ",(" + String.valueOf(functionMainObj.get("mainFunStr")).replace(":", ".") + ") AS  " + toColumnName;
//                                                    }
//
//                                                } else if (functionMainObj != null && !functionMainObj.isEmpty()
//                                                        && "MULTI_COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))) {
//                                                    JSONObject multiColumnsObj = (JSONObject) functionMainObj.get("multiColumnsObj");
//                                                    if (multiColumnsObj != null && !multiColumnsObj.isEmpty()) {
//                                                        String multColStr = new iVisioniTransformDataPipingUtills().generateMultiColumnsObj(multiColumnsObj, (String) functionMainObj.get("functionName"));
//                                                        if (multColStr != null
//                                                                && !"".equalsIgnoreCase(multColStr)
//                                                                && !"null".equalsIgnoreCase(multColStr)) {
//                                                            query += ",(" + multColStr + ") AS  " + toColumnName;
//                                                        }
//                                                    }
//
//                                                }
//                                            }
//
//                                        }
//                                    }
//
//                                }
//                            }
//                            if (String.valueOf(tableName).contains(".")){
//                            tableName = String.valueOf(tableName).split("\\.")[1];
//                            }
            query += " FROM " + tableName + " ";
            if (tablesObj != null && !tablesObj.isEmpty()) {
                String tableInputVal = (String) tablesObj.get(tableName);
                if (tableInputVal != null && !"".equalsIgnoreCase(tableInputVal) && !"null".equalsIgnoreCase(tableInputVal)) {
                    query += " WHERE " + tableInputVal + "";
                }
            }
            int end = 0;

//                            if ((selectTabObj != null
//                                    && !selectTabObj.isEmpty()
//                                    && "Y".equalsIgnoreCase(String.valueOf(selectTabObj.get("uniqueRowsFlag"))))) {
//                                query = "SELECT " + rowNumStr + "A.* FROM (" + query + ") A";
//                            }
            if (orderAndGroupByObj != null && !orderAndGroupByObj.isEmpty()) {

                String groupByQuery = (String) orderAndGroupByObj.get("groupByQuery");
                if (groupByQuery != null
                        && !"".equalsIgnoreCase(groupByQuery)
                        && !"null".equalsIgnoreCase(groupByQuery)) {
                    query += " GROUP BY  " + groupByQuery + " ";
                }

                String orderByClause = "";
                JSONObject orderByObj = (JSONObject) orderAndGroupByObj.get("orderByObj");
                if (orderByObj != null
                        && !orderByObj.isEmpty()) {
                    int orderIndex = 0;
                    for (Object orderByKey : orderByObj.keySet()) {
                        JSONObject orderObj = (JSONObject) orderByObj.get(orderByKey);
                        if (orderObj != null
                                && !orderObj.isEmpty()
                                && orderObj.get("columnName") != null
                                && !"".equalsIgnoreCase(String.valueOf(orderObj.get("columnName")))
                                && !"null".equalsIgnoreCase(String.valueOf(orderObj.get("columnName")))) {
                            orderByClause += "" + String.valueOf(orderObj.get("columnName")).replaceAll(":", ".") + " "
                                    + " " + (("DESC".equalsIgnoreCase(String.valueOf(orderObj.get("direction")))) ? "DESC" : "ASC");
                            if (orderIndex != orderByObj.size() - 1) {
                                orderByClause += ",";
                            }
                            orderIndex++;
                        }
                    }
                    //columnName,direction
                    if (orderByClause != null
                            && !"".equalsIgnoreCase(orderByClause)
                            && !"null".equalsIgnoreCase(orderByClause)) {
                        query += " ORDER BY  " + orderByClause + " ";
                    }
                }

            }
            // query += " OFFSET " + (start - 1) + " ROWS FETCH NEXT " + limit + " ROWS ONLY ";
//                            query = "SELECT * FROM (" + query + ") WHERE RNO BETWEEN " + start + " AND " + (end) + "";
            System.out.println("query::::" + query);

        }

        return query;
    }

    public List getSelectedColumnsData(
            String query,
            Session hibernateSession,
            SessionFactory sessionFactory,
            Integer start,
            Integer limit
    ) {

        List totalData = new ArrayList();

        try {

//                            SessionFactory sessionFactory = iTransformAccess.getSessionFactoryObject(hostName,
//                                    port,
//                                    userName,
//                                    password,
//                                    dataBaseName,
//                                    dbType);
            totalData = iTransformAccess.queryWithParamsWithLimit(
                    query,
                    hibernateSession,
                    sessionFactory,
                    limit,
                    start);

//            try {
//                dataMigrationAccessDAO.processETLLog(loginUserName,
//                        loginOrgnId, "   ", "INFO", 1, "Y",
//                        dataBaseDriver, dbURL, dbUserName, dbPassword, lastSeqObj.toJSONString(), String.valueOf(selectTabObj.get("jobId")));
//            } catch (Exception ex) {
//            }
        } catch (Exception e) {
            e.printStackTrace();
            try {
//                dataMigrationAccessDAO.processETLLog(loginUserName,
//                        loginOrgnId, "Failed to fetch the data due to " + e.getMessage(), "ERROR", 1, "Y",
//                        dataBaseDriver, dbURL, dbUserName, dbPassword, String.valueOf(selectTabObj.get("jobId")));
//            } catch (Exception ex) {
//            }
            } finally {
                try {

//                    hibernateSession.flush();
//                    System.gc();
                } catch (Exception ex) {
                }
            }

        } finally {
            try {
                //System.gc();
                // hibernateSession.flush();

            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return totalData;
    }

    public String getSelectedJoinColumnsDataQuery(
            Map columnsObj, // ravi etl integration
            Map tableColsObj, // ravi etl integration
            JSONObject tablesWhereClauseObj,
            JSONObject joinColsObj,
            String joinQuery,
            String loginUserName,
            String loginOrgnId,
            JSONObject orderAndGroupByObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj,
            Map totalColumnsObj,
            Map defaultValIndxObj,
            Map colClauseIndxObj
    ) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        String result = "";
        String selectQuery = "";
        List totalData = new ArrayList();
        JSONObject lastSeqObj = new JSONObject();
        try {

            if (totalColumnsObj != null && !totalColumnsObj.isEmpty()) {
//                            String rowNumStr = "ROWNUM AS RNO,";
                //  List<String> fromTablesList =  new ArrayList(tableColsObj.keySet());
                String tablesNames = String.join(", ", tableColsObj.keySet());

                String fetchColumns = "";
                List<String> columnsList = new ArrayList(totalColumnsObj.values());
                // List<String> columnsList = new ArrayList(Arrays.asList(colsObj.split(",")));
                Set<String> items = new HashSet<>();
                Set matchedColsSet = columnsList.stream()
                        .filter(n -> !items.add(n)) // Set.add() returns false if the element was already in the set.
                        .collect(Collectors.toSet());

                for (int j = 0; j < columnsList.size(); j++) {
                    String columnName = String.valueOf(columnsList.get(j)).replace(":", ".");

                    if (columnName.contains("FROMCOL_NA_")) {
                        if (defaultValIndxObj.get(columnName) != null) {
                            fetchColumns += "'" + defaultValIndxObj.get(columnName) + "' ";
                        } else if (colClauseIndxObj.get(columnName) != null) {
                            fetchColumns += colClauseIndxObj.get(columnName);
                        }

                    }
                    if (matchedColsSet != null && !matchedColsSet.isEmpty() && matchedColsSet.contains(columnsList.get(j))) {
                        fetchColumns += columnName + " AS " + columnName + "_0" + j;
                    } else {
                        fetchColumns += columnName;
                    }
                    if (j != columnsList.size() - 1) {
                        fetchColumns += ",";
                    }
                }

                columnsList = new ArrayList(Arrays.asList(fetchColumns.split(",")));
                selectQuery += " " + fetchColumns;
//                if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
//                    for (Object toColumnName : columnClauseObj.keySet()) {
//                        if (columnClauseObj.get(toColumnName) instanceof JSONObject) {
//                            JSONObject funObj = (JSONObject) columnClauseObj.get(toColumnName);
//                            if (funObj != null
//                                    && !funObj.isEmpty()) {
//                                String functionMainObjStr = (String) funObj.get("funobjstr");
//                                if (functionMainObjStr != null
//                                        && !"".equalsIgnoreCase(functionMainObjStr)
//                                        && !"null".equalsIgnoreCase(functionMainObjStr)) {
//                                    JSONObject functionMainObj = (JSONObject) JSONValue.parse(functionMainObjStr);
//                                    if (functionMainObj != null && !functionMainObj.isEmpty()
//                                            && ("QUERY".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
//                                            || "COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE")))) {//mainFunStr
//                                        if ("DM_COL_AGGREGATE".equalsIgnoreCase(String.valueOf(functionMainObj.get("HL_FUN_ID")))) {
//                                            String aggTableName = String.valueOf(functionMainObj.get("COLUMN_NAME"));
//                                            if (aggTableName != null
//                                                    && !"".equalsIgnoreCase(aggTableName)
//                                                    && !"null".equalsIgnoreCase(aggTableName)
//                                                    && aggTableName.contains(":")) {
//                                                aggTableName = aggTableName.split(":")[0];
//                                            }
//
//                                            selectQuery += ",(SELECT " + String.valueOf(functionMainObj.get("mainFunStr")).replace(":", ".") + " FROM " + aggTableName + " ) AS  " + toColumnName;
//                                        } else {
//                                            selectQuery += ",(" + String.valueOf(functionMainObj.get("mainFunStr")).replace(":", ".") + ") AS  " + toColumnName;
//                                        }
//
//                                    } else if (functionMainObj != null && !functionMainObj.isEmpty()
//                                            && "MULTI_COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))) {
//                                        JSONObject multiColumnsObj = (JSONObject) functionMainObj.get("multiColumnsObj");
//                                        if (multiColumnsObj != null && !multiColumnsObj.isEmpty()) {
//                                            String multColStr = new iVisioniTransformDataPipingUtills().generateMultiColumnsObj(multiColumnsObj, (String) functionMainObj.get("functionName"));
//                                            if (multColStr != null
//                                                    && !"".equalsIgnoreCase(multColStr)
//                                                    && !"null".equalsIgnoreCase(multColStr)) {
//                                                selectQuery += ",(" + multColStr + ") AS  " + toColumnName;
//                                            }
//                                        }
//
//                                    }
//                                }
//
//                            }
//                        }
//
//                    }
//                }

                selectQuery = "SELECT ";
                if ((selectTabObj != null
                        && !selectTabObj.isEmpty()
                        && "Y".equalsIgnoreCase(String.valueOf(selectTabObj.get("uniqueRowsFlag"))))) {//uniqueRowsFlag
                    selectQuery += " DISTINCT ";
                }
                selectQuery += " " + fetchColumns + " FROM " + joinQuery;
                System.out.println("selectQuery:::" + selectQuery);
                String whereClause = "";
                if (tablesWhereClauseObj != null && !tablesWhereClauseObj.isEmpty()) {
                    for (Object tableName : tablesWhereClauseObj.keySet()) {
                        if (joinColsObj.containsKey(tableName)) {
                            String condition = (String) tablesWhereClauseObj.get(tableName);
                            if (condition != null
                                    && !"".equalsIgnoreCase(condition)
                                    && !"null".equalsIgnoreCase(condition)) {
                                whereClause += " " + condition + " AND ";
                            }
                        }

                    }
                }
                if (whereClause != null
                        && !"".equalsIgnoreCase(whereClause)
                        && !"null".equalsIgnoreCase(whereClause)) {
                    whereClause = new PilogUtilities().trimAND(whereClause);
                    if (selectQuery != null && !"".equalsIgnoreCase(selectQuery)) {
                        if (selectQuery.contains(" WHERE ")) {
                            selectQuery += " AND " + whereClause;
                        } else {
                            selectQuery += " WHERE " + whereClause;
                        }
                    }
                }

//                    if ((selectTabObj != null
//                            && !selectTabObj.isEmpty()
//                            && "Y".equalsIgnoreCase(String.valueOf(selectTabObj.get("uniqueRowsFlag"))))) {
//                        selectQuery = "SELECT " + rowNumStr + "A.* FROM (" + selectQuery + ") A";
//                    }
                if (orderAndGroupByObj != null && !orderAndGroupByObj.isEmpty()) {

                    String groupByQuery = (String) orderAndGroupByObj.get("groupByQuery");
                    if (groupByQuery != null
                            && !"".equalsIgnoreCase(groupByQuery)
                            && !"null".equalsIgnoreCase(groupByQuery)) {
                        selectQuery += " GROUP BY  " + groupByQuery + " ";
                    }

                    String orderByClause = "";
                    JSONObject orderByObj = (JSONObject) orderAndGroupByObj.get("orderByObj");
                    if (orderByObj != null
                            && !orderByObj.isEmpty()) {
                        int orderIndex = 0;
                        for (Object orderByKey : orderByObj.keySet()) {
                            JSONObject orderObj = (JSONObject) orderByObj.get(orderByKey);
                            if (orderObj != null
                                    && !orderObj.isEmpty()
                                    && orderObj.get("columnName") != null
                                    && !"".equalsIgnoreCase(String.valueOf(orderObj.get("columnName")))
                                    && !"null".equalsIgnoreCase(String.valueOf(orderObj.get("columnName")))) {
                                orderByClause += "" + String.valueOf(orderObj.get("columnName")).replaceAll(":", ".") + " "
                                        + " " + (("DESC".equalsIgnoreCase(String.valueOf(orderObj.get("direction")))) ? "DESC" : "ASC");
                                if (orderIndex != orderByObj.size() - 1) {
                                    orderByClause += ",";
                                }
                                orderIndex++;
                            }
                        }
                        //columnName,direction
                        if (orderByClause != null
                                && !"".equalsIgnoreCase(orderByClause)
                                && !"null".equalsIgnoreCase(orderByClause)) {
                            selectQuery += " ORDER BY " + orderByClause + " ";
                        }
                    }

                }
                System.out.println("query::::" + selectQuery);

            }

//            try {
//                processETLLog(loginUserName,
//                        loginOrgnId, "   ", "INFO", 1, "Y",
//                        dataBaseDriver, dbURL, dbUserName, dbPassword, lastSeqObj.toJSONString(), String.valueOf(selectTabObj.get("jobId")));
//            } catch (Exception ex) {
//            }
        } catch (Exception e) {
            e.printStackTrace();
//            try {
//                processETLLog(loginUserName,
//                        loginOrgnId, "Failed to fetch the data due to " + e.getMessage(), "ERROR", 1, "Y",
//                        dataBaseDriver, dbURL, dbUserName, dbPassword, String.valueOf(selectTabObj.get("jobId")));
//            } catch (Exception ex) {
//            }
        } finally {
            try {

            } catch (Exception e) {
            }
        }
        return selectQuery;
    }

    public int exportingXLSXFileDataOpt(Map toOperator,
            //JSONObject destColumnsObj,
            Map destColumnsObj,// ravi etl integration
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            List totalData,
            PreparedStatement importStmt,
            List<String> columnsList,
            JSONObject defaultValuesObj,
            String filePath,
            String fileName,
            int lastRowIndex,
            XSSFSheet sheet,
            XSSFWorkbook wb,
            JSONObject selectTabObj
    ) {
        int insertCount = 0;
        try {
            if (totalData != null && !totalData.isEmpty()) {
                JSONObject columnClauseObj = (JSONObject) selectTabObj.get("columnClauseObj");
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

                        hssfCell.setCellValue(String.valueOf(columnsList.get(i)));
                    }
                    lastRowIndex++;
                } else {
                    lastRowIndex++;
                }

                for (int i = 0; i < totalData.size(); i++) {
                    Object[] dataObj = (Object[]) totalData.get(i);
                    XSSFRow hssfRow = sheet.createRow(lastRowIndex);

                    if (dataObj != null && dataObj.length > 0) {
                        cellIdx = 0;
                        for (int j = 0; j < dataObj.length - 1; j++) {
                            XSSFCell hSSFCell = hssfRow.createCell(cellIdx++);
                            if (dataObj[j] != null && !"".equalsIgnoreCase(String.valueOf(dataObj[j]))) {
                                hSSFCell.setCellValue(String.valueOf(dataObj[j]));
                            } else {
                                hSSFCell.setCellValue("");
                            }

                        }

                    }
                    lastRowIndex++;
                    insertCount++;
                }

                FileOutputStream outs = new FileOutputStream(outputFile);
                wb.write(outs);
                outs.close();
            }

//                wb.setSheetName(0, sheetName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return insertCount;
    }

    public int exportingXLSFileDataOpt(Map toOperator,
            //JSONObject destColumnsObj,
            Map destColumnsObj,// ravi etl integration
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            List totalData,
            PreparedStatement importStmt,
            List<String> columnsList,
            JSONObject defaultValuesObj,
            String filePath,
            String fileName,
            int lastRowIndex,
            HSSFSheet sheet,
            HSSFWorkbook wb,
            JSONObject selectTabObj
    ) {
        int insertCount = 0;
        try {

            if (totalData != null && !totalData.isEmpty()) {
                JSONObject columnClauseObj = (JSONObject) selectTabObj.get("columnClauseObj");
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

                        hssfCell.setCellValue(String.valueOf(columnsList.get(i)));
                    }
                    lastRowIndex++;
                } else {
                    lastRowIndex++;
                }

                for (int i = 0; i < totalData.size(); i++) {
                    Object[] dataObj = (Object[]) totalData.get(i);
                    HSSFRow hssfRow = sheet.createRow(lastRowIndex);

                    if (dataObj != null && dataObj.length > 0) {
                        cellIdx = 0;
                        for (int j = 0; j < dataObj.length - 1; j++) {
                            HSSFCell hSSFCell = hssfRow.createCell(cellIdx++);
                            if (dataObj[j] != null && !"".equalsIgnoreCase(String.valueOf(dataObj[j]))) {
                                hSSFCell.setCellValue(String.valueOf(dataObj[j]));
                            } else {
                                hSSFCell.setCellValue("");
                            }

                        }

                    }
                    lastRowIndex++;
                    insertCount++;
                }

                FileOutputStream outs = new FileOutputStream(outputFile);
                wb.write(outs);
                outs.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return insertCount;
    }

    public int exportingXMLFileDataOpt(Map toOperator,
            //JSONObject destColumnsObj,
            Map destColumnsObj,// ravi etl integration
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            List totalData,
            PreparedStatement importStmt,
            List<String> columnsList,
            JSONObject defaultValuesObj,
            String filePath,
            String fileName,
            int lastRowIndex,
            JSONObject selectTabObj
    ) {
        int insertCount = 0;
        try {
            if (totalData != null && !totalData.isEmpty() && columnsObj != null && !columnsObj.isEmpty()) {
                JSONObject columnClauseObj = (JSONObject) selectTabObj.get("columnClauseObj");
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
                        Object[] dataObj = (Object[]) totalData.get(i);
                        if (dataObj != null && dataObj.length > 0) {
                            Element itemElement = document.createElement("Item");
                            rootElement.appendChild(itemElement);
                            for (int j = 0; j < columnsList.size(); j++) {// columns data
                                String sourceColumnName = columnsList.get(j);
                                Element sourceColumnNameElement = document.createElement(sourceColumnName.replaceAll(":", "_").replaceAll(" ", "_"));

                                String cellvalue = "";
                                if (dataObj[j] != null && !"".equalsIgnoreCase(String.valueOf(dataObj[j]))) {
                                    cellvalue = String.valueOf(dataObj[j]);
                                }

                                if (cellvalue != null && !"".equalsIgnoreCase(cellvalue)) {
                                    cellvalue = cellvalue.replaceAll("Â", "");
                                    cellvalue = cellvalue.replaceAll("[^\\x00-\\x7F]", " ");
                                    cellvalue = cellvalue.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", " ");
                                    cellvalue = cellvalue.replaceAll("\\p{C}", " ");
                                    //  cellvalue = cellvalue.replaceAll("\\s+", " ");

                                }
                                sourceColumnNameElement.appendChild(document.createTextNode(escape(cellvalue)));
                                itemElement.appendChild(sourceColumnNameElement);

                            }// end of columns loop
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
                        Object[] dataObj = (Object[]) totalData.get(i);
                        if (dataObj != null && dataObj.length > 0) {
                            xmlString += "<Item>";
                            for (int j = 0; j < columnsList.size(); j++) {// columns data
                                String sourceColumnName = columnsList.get(j);
                                String cellvalue = "";

                                if (dataObj[j] != null && !"".equalsIgnoreCase(String.valueOf(dataObj[j]))) {
                                    cellvalue = String.valueOf(dataObj[j]);
                                }

                                if (cellvalue != null && !"".equalsIgnoreCase(cellvalue)) {
                                    cellvalue = cellvalue.replaceAll("Â", "");
                                    cellvalue = cellvalue.replaceAll("[^\\x00-\\x7F]", " ");
                                    cellvalue = cellvalue.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", " ");
                                    cellvalue = cellvalue.replaceAll("\\p{C}", " ");
                                    //  cellvalue = cellvalue.replaceAll("\\s+", " ");

                                }
                                xmlString += "<" + sourceColumnName.replaceAll(":", "_").replaceAll(" ", "_") + ">"
                                        + "" + escape(cellvalue) + ""
                                        + "</" + sourceColumnName.replaceAll(":", "_").replaceAll(" ", "_") + ">\n";

                            }// end of columns loop
                            lastRowIndex++;
                            xmlString += "</Item>\n";

                        }
                        insertCount++;
                    }// end of data loop
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

    public int exportingCsvAndTxtFileDataOpt(Map toOperator,
            //JSONObject destColumnsObj,
            Map destColumnsObj,// ravi etl integration
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            List totalData,
            PreparedStatement importStmt,
            List<String> columnsList,
            JSONObject defaultValuesObj,
            String filePath,
            String fileName,
            int lastRowIndex,
            JSONObject selectTabObj
    ) {
        int insertCount = 0;
        try {
            if (totalData != null && !totalData.isEmpty() && columnsObj != null && !columnsObj.isEmpty()) {
                JSONObject columnClauseObj = (JSONObject) selectTabObj.get("columnClauseObj");
                List<String[]> writeFileDataList = new ArrayList<>();
                File file12 = new File(filePath);

                if (!file12.exists()) {
                    file12.mkdirs();
                }
                int cellIdx = 0;
                File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                if (lastRowIndex == 0) {
                    String columnsString = StringUtils.collectionToDelimitedString(columnsList, ":::");
//                    String columnsString = columnsList.stream()
//                            .map(columnName -> (String.valueOf(columnName).split(":")[1]))
//                            .collect(Collectors.joining(":::"));
                    writeFileDataList.add(columnsString.split(":::"));
                    lastRowIndex++;
                } else {
                    lastRowIndex++;
                }

                for (int i = 0; i < totalData.size(); i++) {
                    Object[] dataObj = (Object[]) totalData.get(i);
                    String dataString = "";
                    if (dataObj != null && dataObj.length > 0) {
                        for (int j = 0; j < columnsList.size(); j++) {// columns data
                            String cellvalue = "";
                            if (dataObj[j] != null && !"".equalsIgnoreCase(String.valueOf(dataObj[j]))) {
                                cellvalue = String.valueOf(dataObj[j]);
                            }

                            if (cellvalue != null
                                    && !"".equalsIgnoreCase(cellvalue)
                                    && !"null".equalsIgnoreCase(cellvalue)) {
                                cellvalue = cellvalue.replaceAll("Â", "");
                            }

                            dataString += cellvalue;
                            if (j != columnsList.size() - 1) {
                                dataString += ":::";
                            }
                        }// end of columns loop
                        lastRowIndex++;
                        if (dataString != null && !"".equalsIgnoreCase(dataString)) {
                            writeFileDataList.add(dataString.split(":::"));
                            dataString = "";
                        }

                    }
                    insertCount++;
                }// end of data loop

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

    public int exportingJSONFileDataOpt(Map toOperator,
            //JSONObject destColumnsObj,
            Map destColumnsObj,// ravi etl integration
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            List totalData,
            PreparedStatement importStmt,
            List<String> columnsList,
            JSONObject defaultValuesObj,
            String filePath,
            String fileName,
            int lastRowIndex,
            JSONObject selectTabObj
    ) {
        int insertCount = 0;
        try {
            if (totalData != null && !totalData.isEmpty() && columnsObj != null && !columnsObj.isEmpty()) {
                JSONObject columnClauseObj = (JSONObject) selectTabObj.get("columnClauseObj");
                List<String[]> writeFileDataList = new ArrayList<>();
                File file12 = new File(filePath);

                if (!file12.exists()) {
                    file12.mkdirs();
                }
                int cellIdx = 0;
                File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
//                if (lastRowIndex == 0) {
//                    String columnsString = columnsList.stream()
//                            .map(columnName -> (String.valueOf(columnName).split(":")[1]))
//                            .collect(Collectors.joining(":::"));
//                    writeFileDataList.add(columnsString.split(":::"));
//                    lastRowIndex++;
//                } else {
//                    lastRowIndex++;
//                }
                String jsonDataStr = "";
                JSONArray totalDataArray = new JSONArray();
                if (lastRowIndex == 0) {
                    jsonDataStr += "[";
//                    Object existingData = JSONValue.parse(new FileReader(outputFile));
//                    if (existingData != null) {
//                        totalDataArray = (JSONArray) existingData;
//                    }

                } else {
                    jsonDataStr += ",";
                }
                for (int i = 0; i < totalData.size(); i++) {
                    Object[] dataObj = (Object[]) totalData.get(i);
                    String dataString = "";
                    if (dataObj != null && dataObj.length > 0) {
                        JSONObject jsonDataObj = new JSONObject();
                        for (int j = 0; j < columnsList.size(); j++) {// columns data
                            String cellvalue = "";
                            if (dataObj[j] != null && !"".equalsIgnoreCase(String.valueOf(dataObj[j]))) {
                                cellvalue = String.valueOf(dataObj[j]);
                            }

                            if (cellvalue != null
                                    && !"".equalsIgnoreCase(cellvalue)
                                    && !"null".equalsIgnoreCase(cellvalue)) {
                                cellvalue = cellvalue.replaceAll("Â", "");
                            }
                            jsonDataObj.put(columnsList.get(j), cellvalue);
                            //
                        }// end of columns loop
                        lastRowIndex++;
                        jsonDataStr += jsonDataObj.toJSONString();
                        if (i != totalData.size() - 1) {
                            jsonDataStr += ",";
                        }

                    }
                    insertCount++;
                }// end of data loop

                FileOutputStream fos = new FileOutputStream(outputFile, true);
                OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
                BufferedWriter writer = new BufferedWriter(osw);
                writer.append(jsonDataStr);
                writer.close();
                osw.close();
                fos.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return insertCount;
    }

    public int importDBDataHib(
            String query,
            Session hibernateSession,
            SessionFactory sessionFactory,
            //JSONObject columnsObj,
            Map columnsObj, // ravi etl integration
            //JSONObject fromColumnsObj,
            Map fromColumnsObj, // ravi etl integration
            JSONObject tablesWhereClauseObj,
            JSONObject defaultValuesObj,
            JSONObject orderByObj,
            String groupByQuery,
            int start,
            int limit,
            int pageNo,
            int totalDataCount,
            Connection toConnection,
            PreparedStatement toPreparedStatement,
            List<String> columnsList,
            String fromTableName,
            JSONObject selectTabObj,
            Map fromOperator,
            Object fromConnObj,
            JSONObject appendValObj,
            JSONObject columnClauseObj,
            String sessionUserName,
            String orgnId,
            String toTableName,
            Map totalColumnsObj
    ) {
        int insertCount = 0;
        try {
            if (selectTabObj != null
                    && !selectTabObj.isEmpty()) {
                selectTabObj.put("columnClauseObj", columnClauseObj);
                String minRows = String.valueOf(selectTabObj.get("minRows"));
                if (minRows != null
                        && !"".equalsIgnoreCase(minRows)
                        && !"null".equalsIgnoreCase(minRows)
                        && NumberUtils.isNumber(minRows)) {
                    if (pageNo == 1) {
                        start = Integer.parseInt(minRows);
                    }
                }
                String maxRows = String.valueOf(selectTabObj.get("maxRows"));
                if (maxRows != null
                        && !"".equalsIgnoreCase(maxRows)
                        && !"null".equalsIgnoreCase(maxRows)
                        && NumberUtils.isNumber(maxRows)) {
                    int maxRowsCount = Integer.parseInt(maxRows);
                    if (pageNo == 1) {
                        if (((maxRowsCount - start)) <= limit) {
                            limit = (maxRowsCount - start);
                        }
                    } else {
                        if (((maxRowsCount - start) + 1) <= limit) {
                            limit = (maxRowsCount - start) + 1;
                        }
                    }
                }

            }
            int end = start + limit - 1;
            if (start == 0) {
                end = limit;
            }
            List totalDataList = new ArrayList();
            if (limit > 0) {

                if (fromConnObj instanceof Connection) {
                    totalDataList = getSelectedColumnsData(
                            query,
                            hibernateSession,
                            sessionFactory,
                            start,
                            limit
                    );
                } else if (fromConnObj instanceof JCO.Client) {
                    totalDataList = dataMigrationAccessDAO.getErpSelectedColumnsData(fromColumnsObj,
                            tablesWhereClauseObj,
                            (JCO.Client) fromConnObj,
                            start,
                            limit);
                }
            }
            if (totalDataList != null && !totalDataList.isEmpty()) {
                if (!(selectTabObj != null && !selectTabObj.isEmpty())) {
                    selectTabObj = new JSONObject();
                }
                selectTabObj.put("oldTableName", fromTableName);
                insertCount = dataMigrationService.importingDataHib(toTableName,
                        fromColumnsObj,
                        columnsObj,
                        totalDataList,
                        toPreparedStatement,
                        columnsList,
                        defaultValuesObj,
                        sessionUserName,
                        orgnId,
                        String.valueOf(selectTabObj.get("jobId")),
                        selectTabObj);
                totalDataCount += insertCount;
                if (insertCount != 0 && insertCount >= 1000) {
                    int startIndex = (pageNo * limit + 1);

                    pageNo++;
                    totalDataCount = importDBDataHib(
                            query,
                            hibernateSession,
                            sessionFactory,
                            columnsObj,
                            fromColumnsObj,
                            tablesWhereClauseObj,
                            defaultValuesObj,
                            orderByObj,
                            groupByQuery,
                            startIndex,
                            limit,
                            pageNo,
                            totalDataCount,
                            toConnection,
                            toPreparedStatement,
                            columnsList,
                            fromTableName,
                            selectTabObj,
                            fromOperator,
                            fromConnObj,
                            appendValObj,
                            columnClauseObj,
                            sessionUserName,
                            orgnId,
                            toTableName,
                            totalColumnsObj);

                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return totalDataCount;
    }

    public Map getSelectedDBTablesDataHib(Map fromOperator,
            Map toOperator,
            //JSONObject fromColumnsObj,
            Map fromColumnsObj, // ravi etl integration
            Map transformationMap
    ) {
        int totalInsertCount = 0;
        Connection currentV10Conn = null;
        PreparedStatement currentV10Stmt = null;
        Connection fromConnection = null;
        PreparedStatement fromStmt = null;
        ResultSet fromSet = null;
        try {
            if (fromOperator != null && !fromOperator.isEmpty()) {
                //JSONObject columnsObj = (JSONObject) transformationMap.get("columnsObj");
                Map columnsObj = (LinkedHashMap) transformationMap.get("columnsObj"); // ravi etl integration
                JSONObject tablesWhereClauseObj = (JSONObject) transformationMap.get("whereClauseObj");//
                JSONObject joinQueryMapObj = (JSONObject) transformationMap.get("joinQueryMapObj");//
                JSONObject orderByObj = (JSONObject) transformationMap.get("orderBy");//
                JSONObject appendValObj = (JSONObject) transformationMap.get("appendValObj");//
                JSONObject columnClauseObj = (JSONObject) transformationMap.get("columnClauseObj");//
                JSONObject selectTabObj = (JSONObject) transformationMap.get("selectTabObj");
                JSONObject defaultValuesObj = (JSONObject) transformationMap.get("defaultValObj");
                JSONObject normalizeOptionsObj = (JSONObject) transformationMap.get("normalizeOptionsObj");
                String joinQuery = (String) transformationMap.get("joinQuery");//

                String groupByQuery = (String) transformationMap.get("groupBy");
                String nativeSQL = "";
                JSONObject orderAndGroupByObj = new JSONObject();
                orderAndGroupByObj.put("orderByObj", orderByObj);
                orderAndGroupByObj.put("groupByQuery", groupByQuery);
                orderAndGroupByObj.put("nativeSQL", nativeSQL);

                Map totalColumnsObj = (LinkedHashMap) transformationMap.get("totalColumnsObj");
                Map defaultValIndxObj = (LinkedHashMap) transformationMap.get("defaultValIndxObj");
                Map colClauseIndxObj = (LinkedHashMap) transformationMap.get("colClauseIndxObj");

                if (joinQueryMapObj != null && !joinQueryMapObj.isEmpty()) {
                    for (Object joinTableName : joinQueryMapObj.keySet()) {
                        Object joinTableData = joinQueryMapObj.get(joinTableName);
                        if (joinTableData instanceof JSONObject) {
                            JSONObject joinTableDataObj = (JSONObject) joinTableData;
                            if (joinTableDataObj != null && !joinTableDataObj.isEmpty()) {
                                for (Object joinTableDataObjKey : joinTableDataObj.keySet()) {
                                    JSONObject joinChildTableData = (JSONObject) joinTableDataObj.get(joinTableDataObjKey);
                                    if (joinChildTableData != null && !joinChildTableData.isEmpty()) {
                                        String childTableColumn = (String) joinChildTableData.get("childTableColumn");
                                        String masterTableColumn = (String) joinChildTableData.get("masterTableColumn");
                                        if (childTableColumn != null
                                                && !"".equalsIgnoreCase(childTableColumn)
                                                && !"null".equalsIgnoreCase(childTableColumn)) {
                                            String[] childTableColumnArray = childTableColumn.split(":");
                                            if (childTableColumnArray != null
                                                    && childTableColumnArray.length != 0
                                                    && fromColumnsObj.get(childTableColumnArray[0]) != null
                                                    && !"".equalsIgnoreCase(String.valueOf(fromColumnsObj.get(childTableColumnArray[0])))
                                                    && !"null".equalsIgnoreCase(String.valueOf(fromColumnsObj.get(childTableColumnArray[0])))) {
                                                List<String> existingColumns = Arrays.asList(((String) fromColumnsObj.get(childTableColumnArray[0])).split(","));
                                                if (existingColumns != null
                                                        && !existingColumns.isEmpty()
                                                        && !existingColumns.contains(childTableColumnArray[1])) {
                                                    fromColumnsObj.put(childTableColumnArray[0], (fromColumnsObj.get(childTableColumnArray[0]) + "," + childTableColumnArray[1]));
                                                }
                                            } else {
                                                fromColumnsObj.put(childTableColumnArray[0], childTableColumnArray[1]);
                                            }
                                        }
                                        if (masterTableColumn != null
                                                && !"".equalsIgnoreCase(masterTableColumn)
                                                && !"null".equalsIgnoreCase(masterTableColumn)) {
                                            String[] masterTableColumnArray = masterTableColumn.split(":");
                                            if (masterTableColumnArray != null
                                                    && masterTableColumnArray.length != 0
                                                    && fromColumnsObj.get(masterTableColumnArray[0]) != null
                                                    && !"".equalsIgnoreCase(String.valueOf(fromColumnsObj.get(masterTableColumnArray[0])))
                                                    && !"null".equalsIgnoreCase(String.valueOf(fromColumnsObj.get(masterTableColumnArray[0])))) {
                                                List<String> existingColumns = Arrays.asList(((String) fromColumnsObj.get(masterTableColumnArray[0])).split(","));
                                                if (existingColumns != null
                                                        && !existingColumns.isEmpty()
                                                        && !existingColumns.contains(masterTableColumnArray[1])) {
                                                    fromColumnsObj.put(masterTableColumnArray[0], (fromColumnsObj.get(masterTableColumnArray[0]) + "," + masterTableColumnArray[1]));
                                                }
                                            } else {
                                                fromColumnsObj.put(masterTableColumnArray[0], masterTableColumnArray[1]);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                String fromTableName = (String) fromOperator.get("tableName");
                String fromColumns = (String) fromColumnsObj.get(fromTableName);
                if (fromColumns != null
                        && !"".equalsIgnoreCase(fromColumns)
                        && !"null".equalsIgnoreCase(fromColumns)) {

                    JSONObject connObj = (JSONObject) fromOperator.get("connObj");
                    currentV10Conn = DriverManager.getConnection(dbURL, userName, password);

                    if (currentV10Conn != null) {
                        JSONObject currentV10DBObj = new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName, password, "Current_V10");
                        String dbTableName = "ZZ_" + System.currentTimeMillis();

                        Object fromConnObj = getConnection(connObj);
                        if (dbTableName != null
                                && !"".equalsIgnoreCase(dbTableName)
                                && !"null".equalsIgnoreCase(dbTableName)) {
                            if (dbTableName.length() > 30) {
                                dbTableName = dbTableName.substring(0, 30);
                            }
                            List<Object[]> columnsList = new ArrayList<>();
                            if (fromConnObj instanceof Connection) {
                                fromConnection = (Connection) fromConnObj;
                                String fronDBTableName = fromTableName;
                                if (fromTableName.contains(".")) {
                                    fronDBTableName = (fromTableName.split("[.]"))[1];
                                }
                                columnsList = getTableColumnsOpt(fromConnection, connObj, fronDBTableName);
                            } else if (fromConnObj instanceof JCO.Client) {
                                String fronDBTableName = fromTableName;
                                if (fromTableName.contains(".")) {
                                    fronDBTableName = (fromTableName.split("[.]"))[1];
                                }
                                columnsList = dataMigrationAccessDAO.getSAPTableColumnsWithType((JCO.Client) fromConnObj, fronDBTableName);
                            }

                            if (columnsList != null && !columnsList.isEmpty()) {
                                List<Object[]> dataTypeList = new ArrayList<>();
                                try {
                                    dataTypeList = genericDataPipingDAO.getTargetDataType(String.valueOf(connObj.get("CONN_CUST_COL1")).toUpperCase(),
                                            String.valueOf(currentV10DBObj.get("CONN_CUST_COL1")).toUpperCase());
                                } catch (Exception e) {
                                }

                                String createTableQuery = "CREATE TABLE " + dbTableName + "("
                                        + "";

                                for (int k = 0; k < columnsList.size(); k++) {
                                    Object[] columnsObjArray = columnsList.get(k);
                                    if (columnsObjArray != null && columnsObjArray.length != 0) {

                                        createTableQuery += "" + columnsObjArray[2];
                                        if (dataTypeList != null && !dataTypeList.isEmpty()) {
                                            JSONObject dataTypeObj = new JSONObject();
                                            List<Object[]> matchedDataTypeList = dataTypeList.stream()
                                                    .filter(dataTypeArray
                                                            -> (dataTypeArray[1] != null
                                                    && !"".equalsIgnoreCase(String.valueOf(dataTypeArray[1]))
                                                    && !"null".equalsIgnoreCase(String.valueOf(dataTypeArray[1]))
                                                    && dataTypeArray[6] != null
                                                    && !"".equalsIgnoreCase(String.valueOf(dataTypeArray[6]))
                                                    && !"null".equalsIgnoreCase(String.valueOf(dataTypeArray[6]))
                                                    && String.valueOf(dataTypeArray[6]).equalsIgnoreCase(String.valueOf(columnsObjArray[3]))))
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
                                                                && !"null".equalsIgnoreCase(String.valueOf(columnsObjArray[4]))) {
                                                            int dataTypeLen = new PilogUtilities().convertIntoInteger(new BigInteger("" + columnsObjArray[4]));
                                                            if (dataTypeLen == 0) {
                                                                dataTypeLen = 1;
                                                            }
                                                            //length += "" + dataTypeLen+ " ";
                                                            length += "" + (dataTypeLen + 100) + " "; // ravi etl integration
                                                            if (matchedDataTypeArray[4] != null
                                                                    && !"".equalsIgnoreCase(String.valueOf(matchedDataTypeArray[4]))
                                                                    && !"null".equalsIgnoreCase(String.valueOf(matchedDataTypeArray[4]))) {
                                                                length += "" + matchedDataTypeArray[4] + " ";
                                                            }
                                                        }

                                                        if (length != null
                                                                && !"".equalsIgnoreCase(length)
                                                                && !"null".equalsIgnoreCase(length)) {

                                                            dataType += "(" + length + ") ";
                                                        }
                                                        createTableQuery += " " + dataType;
                                                    }

                                                }

                                            }

                                        } else {
                                            createTableQuery += " " + columnsObjArray[8];
                                        }
                                        if (k != columnsList.size() - 1) {
                                            createTableQuery += " ,";
                                        }
                                    }

                                }// columns loop end
                                createTableQuery += ")";
                                System.out.println("createTableQuery:::" + createTableQuery);
                                currentV10Stmt = currentV10Conn.prepareStatement(createTableQuery);
                                boolean isTableCreated = currentV10Stmt.execute();
                                System.out.println("isTableCreated:::" + isTableCreated);

                                List<String> fromColumnsList = Arrays.asList(fromColumns.split(","));
                                String insertQuery = generateInsertQuery(dbTableName, fromColumnsList);
                                System.out.println("insertQuery:::" + insertQuery);
                                Map columnsTypeObj = getColumnsType(dbTableName, fromColumnsList, currentV10Conn);
                                if (!(selectTabObj != null && !selectTabObj.isEmpty())) {
                                    selectTabObj = new JSONObject();
                                }
                                selectTabObj.put("columnsTypeObj", columnsTypeObj);
                                currentV10Stmt = currentV10Conn.prepareStatement(insertQuery);
                                //JSONObject fromColumnsObjUpdated = new JSONObject();
                                Map fromColumnsObjUpdated = new LinkedHashMap(); // ravi etl integration
                                fromColumnsObjUpdated.put(fromTableName, fromColumnsObj.get(fromTableName));
                                //JSONObject columnsObjUpdated = columnsObj;
                                Map columnsObjUpdated = new LinkedHashMap(); // ravi etl integration
                                // Map columnsObjUpdated = columnsObj; // ravi etl integration
                                if (!(columnsObjUpdated != null && !columnsObjUpdated.isEmpty())) {
                                    for (int i = 0; i < fromColumnsList.size(); i++) {
                                        columnsObjUpdated.put(dbTableName + ":" + fromColumnsList.get(i), fromTableName + ":" + fromColumnsList.get(i));
                                    }
                                }

                                JSONObject dbObj = (JSONObject) fromOperator.get("connObj");
                                String hostName = String.valueOf(dbObj.get("HOST_NAME"));
                                String port = String.valueOf(dbObj.get("CONN_PORT"));
                                String userName = String.valueOf(dbObj.get("CONN_USER_NAME"));
                                String password = String.valueOf(dbObj.get("CONN_PASSWORD"));
                                String dataBaseName = String.valueOf(dbObj.get("CONN_DB_NAME"));
                                String dbType = String.valueOf(dbObj.get("CONN_CUST_COL1"));

                                SessionFactory sessionFactory = iTransformAccess.getSessionFactoryObject(hostName,
                                        port,
                                        userName,
                                        password,
                                        dataBaseName,
                                        dbType);
                                // Session hibernateSession = sessionFactory.openSession();
                                Session hibernateSession = null;

                                String query = getSelectedColumnsDataQuery(
                                        columnsObj,
                                        fromColumnsObjUpdated,
                                        tablesWhereClauseObj,
                                        orderAndGroupByObj,
                                        columnClauseObj,
                                        selectTabObj,
                                        columnsObjUpdated,
                                        defaultValIndxObj,
                                        colClauseIndxObj
                                );
                                int insertCount = importDBDataHib(
                                        query,
                                        hibernateSession,
                                        sessionFactory,
                                        columnsObjUpdated,
                                        fromColumnsObjUpdated,
                                        tablesWhereClauseObj,
                                        defaultValuesObj,
                                        orderByObj,
                                        groupByQuery,
                                        1,
                                        1000,
                                        1,
                                        totalInsertCount,
                                        currentV10Conn,
                                        currentV10Stmt,
                                        fromColumnsList,
                                        fromTableName,
                                        selectTabObj,
                                        fromOperator,
                                        fromConnObj,
                                        appendValObj,
                                        columnClauseObj,
                                        "",
                                        "",
                                        dbTableName,
                                        totalColumnsObj);

                                fromOperator.put("oldTableName", fromOperator.get("tableName"));
                                fromOperator.put("tableName", currentV10DBObj.get("CONN_USER_NAME") + "." + dbTableName);
                                fromOperator.put("oldstatusLabel", fromOperator.get("statusLabel"));
                                fromOperator.put("statusLabel", dbTableName);
                                fromOperator.put("CONNECTION_NAME", currentV10DBObj.get("CONNECTION_NAME"));
                                fromOperator.put("CONN_DB_NAME", currentV10DBObj.get("CONN_DB_NAME"));
                                fromOperator.put("CONN_CUST_COL1", currentV10DBObj.get("CONN_CUST_COL1"));
                                fromOperator.put("connObj", currentV10DBObj);

                            }
                        }
                    }
                } else {

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (fromSet != null) {
                    fromSet.close();
                }
                if (fromStmt != null) {
                    fromStmt.close();
                }
                if (currentV10Stmt != null) {
                    currentV10Stmt.close();
                }
                if (fromConnection != null) {
                    fromConnection.close();
                }
                if (currentV10Conn != null) {
                    currentV10Conn.close();
                }
            } catch (Exception e) {
            }
        }
        return fromOperator;
    }

}// end of the class
