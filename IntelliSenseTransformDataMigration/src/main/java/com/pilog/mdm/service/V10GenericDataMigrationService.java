/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.service;

import com.opencsv.CSVReader;

import com.pilog.mdm.DAO.V10GenericDataMigrationDAO;
import com.pilog.mdm.DAO.iVisionTransformConnectionTabsDAO;
import com.pilog.mdm.DAO.V10DataMigrationAccessDAO;
import com.pilog.mdm.access.DataAccess;
import com.pilog.mdm.utilities.AuditIdGenerator;
import com.pilog.mdm.utilities.PilogEncryption;
import com.pilog.mdm.transformaccess.V10MigrationDataAccess;
import com.pilog.mdm.transformutills.JCOUtills;
import com.pilog.mdm.transformutills.V10DataPipingUtills;
import com.pilog.mdm.utilities.PilogUtilities;
import com.pilog.mdm.pojo.DalRoleMenu;

import com.sap.mw.jco.IRepository;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.NumberToTextConverter;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import com.sap.mw.jco.JCO;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import java.io.IOException;
import java.sql.Clob;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import com.pilog.mdm.utilities.PilogEncryption;
import com.mongodb.client.MongoClient;
/**
 *
 * @author PiLog
 */
@Service
public class V10GenericDataMigrationService {

    @Value("${MultipartResolver.fileUploadSize}")
    private long maxFileSize;
    @Value("${MultipartResolver.fileinMemorySize}")
    private int maxMemSize;
    @Value("${jdbc.batchSize}")
    private int batchSize;
    @Value("${jdbc.driver}")
    private String dataBaseDriver;
    @Value("${jdbc.username}")
    private String userName;
    @Value("${jdbc.password}")
    private String password;
    @Value("${jdbc.url}")
    private String dbURL;
    
    @Value("${jdbc.accessName}")
    private String accessName;
    
    private String etlFilePath;
	{
		if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")) {
			etlFilePath = "C://";
		} else {
			etlFilePath = "/u01/";
		}
	}

    @Autowired
    private V10GenericDataMigrationDAO v10GenericDataMigrationDAO;
    @Autowired
    private V10GenericProcessETLDataService genericProcessETLDataService;
//    @Autowired
//    private VisionGenericImportService visionGenericImportService;
    private PilogUtilities pLlogCloudUtills = new PilogUtilities();

    @Autowired
    private V10MigrationDataAccess v10MigrationDataAccess;
    @Autowired
    private iVisionTransformConnectionTabsDAO visionTransformConnectionTabsDAO;
    
    @Autowired
    private DataAccess access;

    private V10DataMigrationAccessDAO v10DataMigrationAccessTables = new V10DataMigrationAccessDAO();
    private V10DataPipingUtills dataPipingUtills = new V10DataPipingUtills();

       public Object getConnection(JSONObject dbObj) {

        Connection connection = null;
        Object returnedObj = null;
        try {

            String initParamClassName = "com.pilog.mdm.transformaccess.V10MigrationDataAccess";
            String initParamMethodName = "get" + dbObj.get("selectedItemLabel") + "Connection";
            System.out.println(" initParamClassName:" + initParamClassName + "initParamMethodName:" + initParamMethodName);
            Class clazz = Class.forName(initParamClassName);
            Class<?>[] paramTypes = {String.class, String.class, String.class, String.class, String.class};
            Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
            Object targetObj = new PilogUtilities().createObjectByClass(clazz);
            returnedObj = method.invoke(targetObj, String.valueOf(dbObj.get("hostName")), String.valueOf(dbObj.get("port")),
                    String.valueOf(dbObj.get("userName")), String.valueOf(dbObj.get("password")), String.valueOf(dbObj.get("serviceName")));

        } catch (Exception e) {
            e.printStackTrace();
        }
        return returnedObj;
    }

    // method to view connection details in ETL under connections tree
       public JSONObject getConnectionDetails(HttpServletRequest request) {
           Connection connection = null;
           String saveDetails = "";
           JSONObject resultObj = new JSONObject();
           String updateConnectionDetails = "";
           //uttej
           JSONArray savedDbList = new JSONArray();
           MongoClient client = null;
           try {
               JSONObject dbObj = new JSONObject();
               dbObj.put("selectedItemLabel", request.getParameter("selectedItemLabel"));
               dbObj.put("hostName", request.getParameter("hostName"));
               dbObj.put("port", request.getParameter("port"));
               dbObj.put("userName", request.getParameter("userName"));
               dbObj.put("password", request.getParameter("password"));
               dbObj.put("serviceName", request.getParameter("serviceName"));
               String filterTableVal = request.getParameter("filerTableValue");

               Object returendObj = getConnection(dbObj);
               if (returendObj instanceof Connection) {
                   connection = (Connection) returendObj;
                   if (connection != null) {
                	   String testFlag = (String)request.getParameter("testFlag");
                	   if (testFlag != null && "Y".equalsIgnoreCase(testFlag)){
                		   resultObj.put("messageFlag", "true");
                		   resultObj.put("connectionMessage", "Success");
                		   return resultObj;
                	   }
                       String checkBoxVal = request.getParameter("checkedVal");
                       String auditIdVal = request.getParameter("auditId");
                       if (checkBoxVal != null && !"".equalsIgnoreCase(checkBoxVal)
                               && !"null".equalsIgnoreCase(checkBoxVal)
                               && "true".equalsIgnoreCase(checkBoxVal)) {
                           if (auditIdVal != null && !"".equalsIgnoreCase(auditIdVal) && !"null".equalsIgnoreCase(auditIdVal)) {
                               updateConnectionDetails = v10GenericDataMigrationDAO.updateConnectionDetails(request);
                               // updateConnectionDetails ;
                           } else {
                               saveDetails = v10GenericDataMigrationDAO.saveConnectionDetails(request);
                           }

                       }
                       String etlFlag = request.getParameter("EtlFlag");
                       if (etlFlag != null && !"".equalsIgnoreCase(etlFlag) && "Y".equalsIgnoreCase(etlFlag)) {

                           resultObj.put("connectionMessage", "Database connection established successfully,Please check in Available Connections!");

                       } else {
                           String tablesList = getDatabaseTables(dbObj, connection, filterTableVal);
                           resultObj.put("tablesList", tablesList);
                       }
//                       String tablesList = getDatabaseTables(dbObj, connection, filterTableVal);
//                       resultObj.put("tablesList", tablesList);
                       resultObj.put("updateMessage", updateConnectionDetails);
                       resultObj.put("dbObj", dbObj);
                       resultObj.put("messageFlag", saveDetails);
                       resultObj.put("connectionFlag", "Y");

                   }
               } else if(returendObj instanceof MongoClient) {
               	client = (MongoClient) returendObj;

                   if (client != null) {
                       String checkBoxVal = request.getParameter("checkedVal");
                       String auditIdVal = request.getParameter("auditId");

                       if (checkBoxVal != null && !"".equalsIgnoreCase(checkBoxVal)
                               && !"null".equalsIgnoreCase(checkBoxVal)
                               && "true".equalsIgnoreCase(checkBoxVal)) {
                           if (auditIdVal != null && !"".equalsIgnoreCase(auditIdVal) && !"null".equalsIgnoreCase(auditIdVal)) {
                               updateConnectionDetails = v10GenericDataMigrationDAO.updateConnectionDetails(request);
                           } else {
                               saveDetails = v10GenericDataMigrationDAO.saveConnectionDetails(request);
                           }
                       }

                       String etlFlag = request.getParameter("EtlFlag");
                       if (etlFlag != null && !"".equalsIgnoreCase(etlFlag) && "Y".equalsIgnoreCase(etlFlag)) {
                           resultObj.put("connectionMessage", "Database connection established successfully, Please check in Available Connections!");
                       } else {
                           //String tablesList = getDatabaseTables(dbObj, database, filterTableVal);
                           //resultObj.put("tablesList", tablesList);
                       }

                       resultObj.put("updateMessage", updateConnectionDetails);
                       resultObj.put("dbObj", dbObj);
                       resultObj.put("messageFlag", saveDetails);
                       resultObj.put("connectionFlag", "Y");
                   }
               } else {
                   resultObj.put("connectionFlag", "N");
                   resultObj.put("connectionMessage", returendObj);
               }
               savedDbList = visionTransformConnectionTabsDAO.getSavedConnections(request);
               if (savedDbList != null && !savedDbList.isEmpty()) {
                   JSONObject jsConnectionObj = new JSONObject();
                   for (int k = 0; k < savedDbList.size(); k++) {
                       JSONObject jsConnObj = (JSONObject) savedDbList.get(k);
                       if (jsConnObj != null && !jsConnObj.isEmpty()) {
                           jsConnectionObj.put(jsConnObj.get("CONNECTION_NAME"), savedDbList.get(k));
                       }
                   }
                   resultObj.put("connectionObj", jsConnectionObj);
               }

           } catch (Exception e) {
               e.printStackTrace();
               resultObj.put("connectionFlag", "N");
               resultObj.put("connectionMessage", e.getMessage());
               return resultObj;
           }
           return resultObj;
       }

    public List getSavedConnections(HttpServletRequest request) {
        List connectionsList = new ArrayList();
        try {
            connectionsList = v10GenericDataMigrationDAO.getSavedConnections(request);
        } catch (Exception e) {
        }
        return connectionsList;
    }

    public JSONObject fetchTableColumns(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        Connection connection = null;

        try {
            JSONObject dbObj = new JSONObject();
            dbObj.put("selectedItemLabel", request.getParameter("selectedItemLabel"));
            dbObj.put("hostName", request.getParameter("hostName"));
            dbObj.put("port", request.getParameter("port"));
            dbObj.put("userName", request.getParameter("userName"));
            dbObj.put("password", request.getParameter("password"));
            dbObj.put("serviceName", request.getParameter("serviceName"));
            String destTableName = request.getParameter("tableName");
            String sourceTableName = request.getParameter("tablesArray");
            Object returendObj = getConnection(dbObj);
            String matchedSelectStr = "";
            if (returendObj instanceof Connection) {
                connection = (Connection) returendObj;
                if (connection != null) {
                    String selectedTables = "<table class=\"visionSelectedTables\" id='selectedTables' style='width: 100%;' border='1'><thead>"
                            + "<tr><th style='width:225px;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Table Name</th>"
                            + "<th style='width:475px;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Table Where Clause Cond.</th>"
                            + "</tr></thead><tbody>";
                    String sourceTr = "<thead><tr><th class=\"mappedColsTh\" style='width:260px;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'> Source Columns</th>"
                            + "<th class=\"mappedColsTh\" style='width:260px;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Destination Columns</th>"
                            + "<th class=\"mappedColsTh\" style='width:190px;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Default Values</th>"
                            + "</tr></thead>";
                    if (sourceTableName != null && !"".equalsIgnoreCase(sourceTableName) && !"null".equalsIgnoreCase(sourceTableName)) {
                        JSONArray tablesValuesArr = (JSONArray) JSONValue.parse(sourceTableName);
                        List tablesList = new ArrayList();
                        tablesList.addAll(tablesValuesArr);
                        if (tablesList != null && !tablesList.isEmpty()) {
                            for (int k = 0; k < tablesList.size(); k++) {
                                selectedTables += "<tr><td  id=SELECT_TABLE_" + k + " style='width: 225px;' data-table = " + tablesList.get(k) + ">" + tablesList.get(k) + "</td>"
                                        + "<td colspan=\"3\" style='width:460px'><input type=\"text\" style='width:100%' id=TABLE_INPUT" + k + " ></td></tr>";
                            }
                        }

                    }
                    List<Object[]> destinationColumnList = getTableColumns(connection, request, dbObj);
                    List<Object[]> sourceColumnList = new ArrayList<>();

                    String gridColumns = request.getParameter("dataFeilds");
                    if (gridColumns != null && !"".equalsIgnoreCase(gridColumns)) {
                        JSONArray gridColumnsArray = (JSONArray) JSONValue.parse(gridColumns);
                        if (gridColumnsArray != null && !gridColumnsArray.isEmpty()) {
                            for (int i = 0; i < gridColumnsArray.size(); i++) {
                                JSONObject dataFieldObj = (JSONObject) gridColumnsArray.get(i);
                                if (dataFieldObj != null && !dataFieldObj.isEmpty()) {
                                    String columnName = (String) dataFieldObj.get("name");
                                    if (columnName != null
                                            && !"".equalsIgnoreCase(columnName)
                                            && !(columnName.startsWith("HIDDEN_")
                                            || columnName.endsWith("_HIDDEN")
                                            || "CREATE_BY".equalsIgnoreCase(columnName)
                                            || "EDIT_BY".equalsIgnoreCase(columnName)
                                            || "CREATE_DATE".equalsIgnoreCase(columnName)
                                            || "AUDIT_ID".equalsIgnoreCase(columnName)
                                            || "EDIT_DATE".equalsIgnoreCase(columnName))) {
                                        Object[] sourceArray = new Object[2];
                                        sourceArray[0] = destTableName;
                                        sourceArray[1] = columnName;
                                        sourceColumnList.add(sourceArray);
                                    }

                                }

                            }
                        }
                    }
                    sourceTr += "<tbody>";

                    if (sourceColumnList != null
                            && !sourceColumnList.isEmpty()
                            && destinationColumnList != null && !destinationColumnList.isEmpty()) {
                        int minSize = (sourceColumnList.size() > destinationColumnList.size()) ? destinationColumnList.size() : sourceColumnList.size();
                        for (int i = 0; i < sourceColumnList.size(); i++) {
                            Object[] sourceColumnsArray = sourceColumnList.get(i);
                            if (sourceColumnsArray != null && sourceColumnsArray.length != 0) {
                                sourceTr += "<tr><td id=" + sourceColumnsArray[1] + " class=\"sourceColsTd\">"
                                        + "<select id=\"SOURCE_SELECT_" + i + "\"  class=\"sourceColsSelectBox\">"
                                        + "" + generateSelectBoxStr(sourceColumnList, (sourceColumnsArray[0] + ":" + sourceColumnsArray[1]), "SOURCE_SELECT_" + i) + ""
                                        + "</select>"
                                        + "</td>";
                            }
                            if (i < destinationColumnList.size()) {
                                Object[] destColumnsArray = destinationColumnList.get(i);
                                if (destColumnsArray != null && destColumnsArray.length != 0) {
                                    sourceTr += ""
                                            + "<td><select id=\"DEST_SELECT_" + i + "\" class=\"destColsSelectBox\" >"
                                            + "" + generateSelectBoxStr(destinationColumnList, (destColumnsArray[0] + ":" + destColumnsArray[1]), "DEST_SELECT_" + i) + ""
                                            + "</select>"
                                            + "</td><td><input type=\"text\" class=\"defaultValues\" id=\"DEFAULTVALUES_" + i + "\"></td></tr>";
                                }
                            } else {
                                sourceTr += ""
                                        + "<td><select id=\"DEST_SELECT_" + i + "\" class=\"destColsSelectBox\" >"
                                        + "" + generateSelectBoxStr(destinationColumnList, "", "DEST_SELECT_" + i) + ""
                                        + "</select>"
                                        + "</td><td><input type=\"text\" class=\"defaultValues\" id=\"DEFAULTVALUES_" + i + "\"></td></tr>";

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

                    String columnStr = "<table id=\"sourceDestColsTableId\" class=\"sourceDestColsTable\" style='width: auto;' border='1'>" + sourceTr + "</table>";
                    resultObj.put("columnStr", columnStr);
                    resultObj.put("matchedSelectStr", matchedSelectStr);
                    resultObj.put("dbObj", dbObj);
                    resultObj.put("selectedTables", selectedTables);
                    resultObj.put("connectionFlag", "Y");
                }
            } else {
                resultObj.put("connectionFlag", "N");
                resultObj.put("connectionMessage", returendObj);
            }

        } catch (Exception e) {
            e.printStackTrace();
//            resultObj.put("connectionFlag", "N");
//            resultObj.put("connectionMessage", e.getMessage());
        }
        return resultObj;
    }

    public String generateSelectBoxStr(List<Object[]> columnList, String selectedColumn, String selectBoxId) {
        String selectBoxStr = "<option>Select</option>";
        try {
            for (int i = 0; i < columnList.size(); i++) {
                Object[] columnsArray = columnList.get(i);
                if (columnsArray != null && columnsArray.length != 0) {
                    String selectedStr = "";
                    if (selectedColumn != null
                            && !"".equalsIgnoreCase(selectedColumn) && selectedColumn.equalsIgnoreCase(String.valueOf(columnsArray[0] + ":" + columnsArray[1]))) {
                        selectedStr = "selected";
                    }
                    selectBoxStr += "<option  value='" + columnsArray[0] + ":" + columnsArray[1] + "'"
                            + " id ='" + selectBoxId + "_" + columnsArray[1] + "' data-tablename='" + columnsArray[0] + "' "
                            + "" + selectedStr + ">" + (columnsArray[0] + "." + columnsArray[1]) + "</option>";
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return selectBoxStr;
    }

    public JSONObject mappingSourceColsWithDestCols(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        Connection connection = null;
        PreparedStatement importStmt = null;
        Connection sourceConn = null;
        try {

            JSONObject dbObj = new JSONObject();
            dbObj.put("selectedItemLabel", request.getParameter("selectedItemLabel"));
            dbObj.put("hostName", request.getParameter("hostName"));
            dbObj.put("port", request.getParameter("port"));
            dbObj.put("userName", request.getParameter("userName"));
            dbObj.put("password", request.getParameter("password"));
            dbObj.put("serviceName", request.getParameter("serviceName"));
            String destTableName = request.getParameter("tableName");
            String sourceTableName = request.getParameter("tablesObj");
            JSONObject destColumnsObj = new JSONObject();
            JSONObject columnsObj = new JSONObject();
            JSONObject tablesObj = new JSONObject();
            Object returendObj = getConnection(dbObj);
            JSONObject defaultValuesObj = new JSONObject();
            String defaultValue = request.getParameter("defaultValObj");
            if (returendObj instanceof Connection) {
                connection = (Connection) returendObj;
                if (connection != null) {
                    if (sourceTableName != null && !"".equalsIgnoreCase(sourceTableName) && !"null".equalsIgnoreCase(sourceTableName)) {
                        tablesObj = (JSONObject) JSONValue.parse(sourceTableName);
                        System.out.println("tablesObj:::" + tablesObj);
                    }
                    if (defaultValue != null && !"".equalsIgnoreCase(defaultValue) && !"null".equalsIgnoreCase(defaultValue)) {
                        defaultValuesObj = (JSONObject) JSONValue.parse(defaultValue);
                        System.out.println("defaultValuesObj:::" + defaultValuesObj);
                    }

                    String columnsObjStr = request.getParameter("columnsObj");
                    if (columnsObjStr != null && !"".equalsIgnoreCase(columnsObjStr) && !"null".equalsIgnoreCase(columnsObjStr)) {
                        columnsObj = (JSONObject) JSONValue.parse(columnsObjStr);
                        System.out.println("columnsObj:::" + columnsObj);

                        if (columnsObj != null && !columnsObj.isEmpty()) {

                            for (Object sourceColumn : columnsObj.keySet()) {
                                if (sourceColumn != null && columnsObj.get(sourceColumn) != null) {
                                    String destColumnStr = (String) columnsObj.get(sourceColumn);
                                    if (destColumnStr != null
                                            && !"".equalsIgnoreCase(destColumnStr)
                                            && !"null".equalsIgnoreCase(destColumnStr)) {
                                        String[] destColumnArray = destColumnStr.split(":");
                                        if (destColumnArray != null && destColumnArray.length != 0) {
                                            if (destColumnsObj != null && !destColumnsObj.isEmpty()) {
                                                if (destColumnsObj.containsKey(destColumnArray[0])) {
                                                    destColumnsObj.put(destColumnArray[0],
                                                            (destColumnsObj.get(destColumnArray[0]) + "," + destColumnArray[1]));
                                                } else {
                                                    destColumnsObj.put(destColumnArray[0],
                                                            (destColumnArray[1]));
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
                            Class.forName(dataBaseDriver);
                            sourceConn = DriverManager.getConnection(dbURL, userName, password);
                            Set<String> destColumns = new HashSet<>(columnsObj.keySet());
                            if (!(defaultValuesObj != null && !defaultValuesObj.isEmpty())) {
                                defaultValuesObj = new JSONObject();
                            }
                            defaultValuesObj.put("CREATE_BY", request.getSession(false).getAttribute("ssUsername"));
                            defaultValuesObj.put("EDIT_BY", request.getSession(false).getAttribute("ssUsername"));
                            if (defaultValuesObj != null && !defaultValuesObj.isEmpty()) {
                                Set<String> defaultColumns = defaultValuesObj.keySet();
                                if (defaultColumns != null
                                        && !defaultColumns.isEmpty()
                                        && destColumns != null && !destColumns.isEmpty()) {
                                    destColumns.addAll(defaultColumns);
                                }
                            }

                            List<String> columnsList = new ArrayList<>();
                            columnsList.addAll(destColumns);

                            String insertQuery = generateInsertQuery(destTableName, columnsList);
                            System.out.println("insertQuery::::" + insertQuery);
                            importStmt = sourceConn.prepareStatement(insertQuery);
                            int totalDataCount = 0;
                            totalDataCount = bulkMappingSourceColsWithDestCols(dbObj, connection, destColumnsObj,
                                    1, 500, columnsObj, destTableName, importStmt, totalDataCount, columnsList, tablesObj, defaultValuesObj);
                            System.out.println("totalDataCount::::" + totalDataCount);
                            if (totalDataCount != 0 && totalDataCount > 0) {
                                resultObj.put("Message", totalDataCount + " Row(s) extracted successfully");
                                resultObj.put("connectionFlag", "Y");

                            }

                        }
                    }
                }
            } else {
                resultObj.put("connectionFlag", "N");
                resultObj.put("connectionMessage", returendObj);
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
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
            }
        }
        return resultObj;
    }

    public int bulkMappingSourceColsWithDestCols(JSONObject dbObj, Connection connection, JSONObject destColumnsObj, int start, int limit,
            JSONObject columnsObj, String destTableName, PreparedStatement importStmt, int totalDataCount, List<String> sourceColumnsList, JSONObject tablesObj, JSONObject defaultValuesObj) {
        JSONObject resultObj = new JSONObject();
        try {
            List totalData = getSelectedColumnsData(dbObj, connection, destColumnsObj, start, limit, tablesObj);
            if (totalData != null && !totalData.isEmpty()) {
                int insertCount = importingData(destTableName, destColumnsObj, columnsObj, totalData, importStmt, sourceColumnsList, defaultValuesObj);
                if (insertCount != 0) {
                    totalDataCount += insertCount;
                    totalDataCount = bulkMappingSourceColsWithDestCols(dbObj, connection, destColumnsObj,
                            (start + limit), limit, columnsObj, destTableName, importStmt,
                            totalDataCount, sourceColumnsList, tablesObj, defaultValuesObj);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return totalDataCount;
    }

    public int importingData(String destTableName,
            //JSONObject destColumnsObj,
            Map destColumnsObj, // ravi etl integration
            //JSONObject columnsObj,
            Map columnsObj,// ravi etl integration
            List totalData,
            PreparedStatement importStmt, List<String> columnsList, JSONObject defaultValuesObj) {
        int insertCount = 0;

        try {
            if (totalData != null && !totalData.isEmpty() && columnsObj != null && !columnsObj.isEmpty()) {
                if (!(destTableName != null
                        && !"".equalsIgnoreCase(destTableName)
                        && !"null".equalsIgnoreCase(destTableName))) {
                    destTableName = "STG_BULK_UPLOAD";
                }
                if (destTableName != null
                        && !"".equalsIgnoreCase(destTableName)
                        && !"null".equalsIgnoreCase(destTableName)) {
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
                                                    importStmt.setObject(stmtIndex, defaultValuesObj.get(sourceColumnName));
                                                } else {
                                                    importStmt.setObject(stmtIndex, "");
                                                }

                                            } else {
                                                importStmt.setObject(stmtIndex,
                                                        dataObj.get(columnsObj.get(sourceColumnName)));
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
            }

        } catch (Exception e) {
            e.printStackTrace();

            insertCount = 0;

        }
        return insertCount;
    }

    public int importingData(String destTableName,
            //JSONObject destColumnsObj,
            Map destColumnsObj, // ravi etl integration
            //JSONObject columnsObj,
            Map columnsObj,// ravi etl integration
            List totalData,
            PreparedStatement importStmt, List<String> columnsList,
            JSONObject defaultValuesObj,
            String loginUserName,
            String loginOrgnId,
            String jobId,
            JSONObject selectTabObj) {
        int insertCount = 0;

        try {
            if (totalData != null && !totalData.isEmpty()
                    && columnsObj != null && !columnsObj.isEmpty()) {
                Map columnsTypeObj = (Map) selectTabObj.get("columnsTypeObj");
                if (!(destTableName != null
                        && !"".equalsIgnoreCase(destTableName)
                        && !"null".equalsIgnoreCase(destTableName))) {
                    destTableName = "STG_BULK_UPLOAD";
                }
                JSONObject columnClauseObj = (JSONObject) selectTabObj.get("columnClauseObj");

                if (destTableName != null
                        && !"".equalsIgnoreCase(destTableName)
                        && !"null".equalsIgnoreCase(destTableName)) {
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
                                    String columnType = "";
                                    if (columnsTypeObj != null && !columnsTypeObj.isEmpty()) {
                                        columnType = String.valueOf(columnsTypeObj.get(sourceColumnName));
                                    }
                                    if (selectTabObj != null
                                            && !selectTabObj.isEmpty()
                                            && selectTabObj.get("oldTableName") != null
                                            && !"".equalsIgnoreCase(String.valueOf(selectTabObj.get("oldTableName")))
                                            && !"null".equalsIgnoreCase(String.valueOf(selectTabObj.get("oldTableName")))) {
                                        //oldTableName
                                        sourceColumnName = selectTabObj.get("oldTableName") + ":" + sourceColumnName;
                                    }
                                    String dateFormat = "";
                                    if (columnClauseObj != null
                                            && !columnClauseObj.isEmpty()
                                            && columnClauseObj.containsKey(sourceColumnName)) {
                                        dateFormat = String.valueOf(columnClauseObj.get(sourceColumnName));
                                    }
//                                    sourceColumnName = destTableName+":"+sourceColumnName;
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
                                                    importStmt.setObject(stmtIndex, dataPipingUtills.convertIntoDBValue(columnType, defaultValuesObj.get(sourceColumnName), sourceColumnName, dateFormat));
                                                } else {
                                                    importStmt.setObject(stmtIndex, "");
                                                }

                                            } else {
                                                importStmt.setObject(stmtIndex,
                                                        dataPipingUtills.convertIntoDBValue(columnType, dataObj.get(columnsObj.get(sourceColumnName)), sourceColumnName, dateFormat));
                                            }

                                        } catch (Exception e) {
                                            importStmt.setObject(stmtIndex, "");
                                            continue;
                                        }
                                        stmtIndex++;
                                    } else {
                                        if (dataObj.get(sourceColumnName) != null
                                                && !"".equalsIgnoreCase(String.valueOf(dataObj.get(sourceColumnName)))
                                                && !"null".equalsIgnoreCase(String.valueOf(dataObj.get(sourceColumnName)))) {
                                            importStmt.setObject(stmtIndex,
                                                    dataPipingUtills.convertIntoDBValue(columnType, dataObj.get(sourceColumnName), sourceColumnName, dateFormat));
                                        } else if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
                                                && defaultValuesObj.get(sourceColumnName) != null
                                                && !"".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))
                                                && !"null".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))) {
                                            importStmt.setObject(stmtIndex, dataPipingUtills.convertIntoDBValue(columnType, defaultValuesObj.get(sourceColumnName), sourceColumnName, dateFormat));
                                        } else if (columnClauseObj != null
                                                && !columnClauseObj.isEmpty()
                                                && columnClauseObj.containsKey(sourceColumnName)) {
                                            importStmt.setObject(stmtIndex, dataPipingUtills.convertIntoDBValue(columnType, dateFormat, sourceColumnName, dateFormat));
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
            }

        } catch (Exception e) {
            e.printStackTrace();
            try {
                genericProcessETLDataService.processETLLog(loginUserName,
                        loginOrgnId,
                        "Faild to load the data into target table due to " + e.getMessage(), "ERROR", 20, "Y", jobId);
            } catch (Exception ex) {
            }
            insertCount = 0;

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

    public String getDatabaseTables(JSONObject dbObj, Connection connection, String filterTableVal) {
        String result = "";
        try {

            String initParamClassName = "com.pilog.mdm.DAO.V10DataMigrationAccessDAO";
            String initParamMethodName = "get" + dbObj.get("selectedItemLabel") + "DatabaseTables";
            System.out.println(" initParamClassName:" + initParamClassName + "initParamMethodName:" + initParamMethodName);
            Class clazz = Class.forName(initParamClassName);
            Class<?>[] paramTypes = {Connection.class, String.class, String.class};
            Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
            Object targetObj = new PilogUtilities().createObjectByClass(clazz);
            result = (String) method.invoke(targetObj, connection, String.valueOf(dbObj.get("serviceName")), filterTableVal);

        } catch (Exception e) {

            e.printStackTrace();
        }
        return result;
    }

    public List<Object[]> getTableColumns(Connection connection, HttpServletRequest request, JSONObject dbObj) {
        List<Object[]> sourceColumnsList = new ArrayList<>();
        try {

            String initParamClassName = "com.pilog.mdm.DAO.V10DataMigrationAccessDAO";
            String initParamMethodName = "get" + dbObj.get("selectedItemLabel") + "TableColumns";
            System.out.println(" initParamClassName:" + initParamClassName + "initParamMethodName:" + initParamMethodName);
            Class clazz = Class.forName(initParamClassName);
            Class<?>[] paramTypes = {Connection.class, HttpServletRequest.class, String.class};
            Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
            Object targetObj = new PilogUtilities().createObjectByClass(clazz);
            sourceColumnsList = (List<Object[]>) method.invoke(targetObj, connection, request, String.valueOf(dbObj.get("serviceName")));

        } catch (Exception e) {

            e.printStackTrace();
        }
        return sourceColumnsList;
    }

    public List getSelectedColumnsData(JSONObject dbObj,
            Connection connection,
            //JSONObject destColumnsObj,// ravi etl integration
            Map destColumnsObj,
            int start, int limit, JSONObject tablesObj) {
        List totalData = new ArrayList();
        try {

            String initParamClassName = "com.pilog.mdm.DAO.V10DataMigrationAccessDAO";
            String initParamMethodName = "get" + dbObj.get("selectedItemLabel") + "SelectedColumnsData";
            System.out.println(" initParamClassName:" + initParamClassName + "initParamMethodName:" + initParamMethodName);
            Class clazz = Class.forName(initParamClassName);
            Class<?>[] paramTypes = {Connection.class, JSONObject.class, Integer.class, Integer.class, JSONObject.class, String.class};
            Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
            Object targetObj = new PilogUtilities().createObjectByClass(clazz);
            totalData = (List) method.invoke(targetObj, connection, destColumnsObj, start, limit, tablesObj, String.valueOf(dbObj.get("serviceName")));

        } catch (Exception e) {

            e.printStackTrace();
        }
        return totalData;
    }

//    public String deleteConnectionDetails(HttpServletRequest request) {
//        String result = "";
//        try {
//            result = v10GenericDataMigrationDAO.deleteConnectionDetails(request);
//        } catch (Exception e) {
//        }
//        return result;
//    }
    public JSONObject deleteConnectionDetails(HttpServletRequest request) { // ravi etl integration
        JSONObject resultObj = new JSONObject();
        try {
            resultObj = v10GenericDataMigrationDAO.deleteConnectionDetails(request);
        } catch (Exception e) {
        }
        return resultObj;
    }

    //import DM file
    public String importDMFile(HttpServletRequest request, HttpServletResponse response, String gridId, MultipartFile file1, String tableName, String selectedFiletype, String dataFeilds) {

        String result = "";
        String filename = "";
        JSONObject importResult = new JSONObject();
        try {
            String excelFilePath = etlFilePath+"Files/DMImport/" + request.getSession(false).getAttribute("ssUsername");
            boolean isMultipart = ServletFileUpload.isMultipartContent(request);
            String gridColumns = PilogEncryption.decryptText(request.getParameter("dataFeilds"), "");
            // String gridColumns = request.getParameter("dataFeilds");
            //FileInputStream inputStream = new FileInputStream(new File(excelFilePath));
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
            } else {
                if (filename != null) {
                    if (filename.lastIndexOf(File.separator) >= 0) {

                        file = new File(filename);
                        // filename = filename.substring(filename.lastIndexOf(File.separator)+1, filename.length());
                        //  System.out.println(file.getAbsolutePath()+"=====IF=======fileName======="+filename);
                    } else {
                        file = new File(excelFilePath + File.separator + mainFileName);
                        // System.out.println(file.getAbsolutePath()+"=====ELSE=======fileName======="+filename);
                    }

                    FileOutputStream osf = new FileOutputStream(file);
                    osf.write(bytes);
                    osf.flush();
                    osf.close();
                    List<String> headers = getHeadersOfImportedFile(request, response, file.getAbsolutePath());
                    if (headers != null && !headers.isEmpty()) {
                        String columnStr = fetchTableFileColumns(request, response, gridColumns, headers);
                        importResult.put("columnStr", columnStr);
                    }
                    System.out.println("headers:::" + headers);
                    result = "File imported successfully";

                } else {
                    result = "[]";
                }
            }
            importResult.put("result", result);
            importResult.put("filePath", file.getAbsolutePath());

            //  }
            //  }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return importResult.toJSONString();
    }

    public List getHeadersOfImportedFile(HttpServletRequest request, HttpServletResponse response, String filePath) {
        List<String> headers = new ArrayList<String>();
        Map<String, Integer> headerCount = new HashMap();
        try {
            if (filePath != null && !"".equalsIgnoreCase(filePath) && !"null".equalsIgnoreCase(filePath)) {
                String fileExt = filePath.substring(filePath.lastIndexOf(".") + 1, filePath.length());

                if (fileExt != null && !"".equalsIgnoreCase(fileExt)) {
                    if ("txt".equalsIgnoreCase(fileExt)
                            || "csv".equalsIgnoreCase(fileExt)) {

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
                        if (!(fileType != null
                                && !"".equalsIgnoreCase(fileType)
                                && !"null".equalsIgnoreCase(fileType))) {
                            fileType = (String) request.getAttribute("fileType");
                        }
                        if (".json".equalsIgnoreCase(fileType)) {
                            columnSeparator = ',';
                        }
//                        FileInputStream fis = new FileInputStream("c:/temp/utf8.txt");
//                        BufferedReader r = new BufferedReader(new InputStreamReader(fis,
//                                "UTF8"));
//                        CSVParser parser = new CSVParserBuilder().withSeparator('\t').build();
//
//                        // create csvReader object with parameter
//                        // filereader and parser
//                        CSVReader reader = new CSVReaderBuilder(new FileReader(filePath))
//                                .withCSVParser(parser)
//                                .build();
                        CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(filePath), "UTF8"), columnSeparator);

                        String[] nextLine;
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
                        //Workbook workBook = null;
                        Sheet sheet = null;

                        //PKH sheet Header
                        String sheetNum = request.getParameter("sheetNo");
                        int sheetNo = (sheetNum != null && !"".equalsIgnoreCase(sheetNum)) ? (Integer.parseInt(sheetNum)) : 0;
                        //PKH sheet Header

//                        if (fileExt != null && "xls".equalsIgnoreCase(fileExt)) { // commented by PKH
//                            workBook = WorkbookFactory.create(new File(filePath));
//                            sheet = (HSSFSheet) workBook.getSheetAt(sheetNo);
//                        } else {
                        System.out.println("Before::::" + new Date());
//                fis = new FileInputStream(new File(filepath));
//                XSSFWorkbook xssfWb = (XSSFWorkbook) new XSSFWorkbook(fis);
                        File file = new File(filePath);
                        String fileNamee = file.getName();
                        boolean fileexist = file.exists();

                        //workBook = WorkbookFactory.create(file);
                        try (Workbook workBook = WorkbookFactory.create(file)) {
                            System.out.println("After::fileInputStream::" + new Date());
                            if (workBook.getSheetAt(sheetNo) instanceof XSSFSheet) {
                                sheet = (XSSFSheet) workBook.getSheetAt(sheetNo);
                            } else if (workBook.getSheetAt(sheetNo) instanceof HSSFSheet) {
                                sheet = (HSSFSheet) workBook.getSheetAt(sheetNo);
                            }

//                sheet = (XSSFSheet) xssfWb.getSheetAt(0);
//                        }
                            if (sheet != null) {
                                Row row = sheet.getRow(0);
                                if (row != null) {
                                    for (int j = 0; j < row.getLastCellNum(); j++) {
                                        //  System.out.println("Cell Num:::" + j + ":::Start Date And Time :::" + new Date());

                                        try {
                                            Cell cell = row.getCell(j);
                                            String header = "";
                                            if (cell != null) {
                                                switch (cell.getCellType()) {
                                                    case Cell.CELL_TYPE_STRING:
                                                        header = cell.getStringCellValue();
                                                        break;
                                                    case Cell.CELL_TYPE_BOOLEAN:
//                                rowObj.put(header, hSSFCell.getBooleanCellValue());
                                                        break;
                                                    case Cell.CELL_TYPE_NUMERIC:

                                                        if (HSSFDateUtil.isCellDateFormatted(cell)) {
                                                            Date cellDate = cell.getDateCellValue();
                                                            if ((cellDate.getYear() + 1900) == 1899 && (cellDate.getMonth() + 1) == 12 && (cellDate.getDate()) == 31) {
                                                                header = (cellDate.getHours()) + ":" + (cellDate.getMinutes()) + ":" + (cellDate.getSeconds());
//                                                    System.out.println("cellDateString :: "+cellDateString);
                                                            } else {
                                                                header = (cellDate.getYear() + 1900) + "-" + (cellDate.getMonth() + 1) + "-" + (cellDate.getDate());
                                                            }

//                                                        String cellDateString = (cellDate.getYear() + 1900) + "-" + (cellDate.getMonth() + 1) + "-" + (cellDate.getDate());
//
                                                        } else {
                                                            header = NumberToTextConverter.toText(cell.getNumericCellValue());
                                                        }
                                                        break;
                                                    case Cell.CELL_TYPE_BLANK:
                                                        header = "";
                                                        break;
                                                }
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
                        } catch (IOException e) {
                            e.printStackTrace(); // Handle or log the exception appropriately
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
                                            String header = childNode.getNodeName();
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
                                            System.err.println(childNode.getNodeName() + "---> " + childNode.getTextContent());
                                        }

                                    }// end of columns loop

                                }

                            }

                        }
                    } else if ("pdf".equalsIgnoreCase(fileExt)) {
                        headers = new ArrayList<>();
                        String result = readPDFRestApi(request, filePath);
                        JSONObject apiPdfJsonData = (JSONObject) JSONValue.parse(result);
                        List<String> headerArray = (List<String>) apiPdfJsonData.get("columns");
                        List<List<String>> resultArray = (List<List<String>>) apiPdfJsonData.get("data");
                        if (resultArray != null && !resultArray.isEmpty()) {
                            // LinkedHashMap rowObj = (LinkedHashMap) resultArray.get(0);
                            headers.addAll(headerArray);
                        }
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return headers;
    }

    public String fetchTableFileColumns(HttpServletRequest request, HttpServletResponse response, String gridColumns, List sourceColumnList) {
        String result = "";
        //JSONObject resultObj = new JSONObject();

        try {
            String sourceTr = "<thead><tr><th class=\"mappedColsTh\" style='width:260px;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'> Source Columns</th>"
                    + "<th class=\"mappedColsTh\" style='width:260px;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Destination Columns</th>"
                    + "<th class=\"mappedColsTh\" style='width:260px;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Default Value</th>"
                    + "</tr></thead>";
            List destColumnList = new ArrayList<>();
            if (gridColumns != null && !"".equalsIgnoreCase(gridColumns)) {
                JSONArray gridColumnsArray = (JSONArray) JSONValue.parse(gridColumns);
                if (gridColumnsArray != null && !gridColumnsArray.isEmpty()) {
                    for (int i = 0; i < gridColumnsArray.size(); i++) {
                        JSONObject dataFieldObj = (JSONObject) gridColumnsArray.get(i);
                        if (dataFieldObj != null && !dataFieldObj.isEmpty()) {
                            String columnName = (String) dataFieldObj.get("name");
                            if (columnName != null
                                    && !"".equalsIgnoreCase(columnName)
                                    && !(columnName.startsWith("HIDDEN_")
                                    || columnName.endsWith("_HIDDEN")
                                    || "CREATE_BY".equalsIgnoreCase(columnName)
                                    || "EDIT_BY".equalsIgnoreCase(columnName)
                                    || "CREATE_DATE".equalsIgnoreCase(columnName)
                                    || "AUDIT_ID".equalsIgnoreCase(columnName)
                                    || "EDIT_DATE".equalsIgnoreCase(columnName))) {
                                destColumnList.add(columnName);
                            }

                        }

                    }
                }
            }

            if (sourceColumnList != null
                    && !sourceColumnList.isEmpty()
                    && destColumnList != null && !destColumnList.isEmpty()) {
                int minSize = (destColumnList.size() > sourceColumnList.size()) ? sourceColumnList.size() : destColumnList.size();
                for (int i = 0; i < minSize; i++) {
                    // Object[] sourceColumnsArray = sourceColumnList.get(i);
                    if (destColumnList != null && destColumnList.size() != 0) {
                        sourceTr += "<tr><td id = " + destColumnList.get(i) + " class=\"destColsTd\">"
                                + "<select id=\"DEST_FILE_SELECT_" + i + "\"  class=\"destColsSelectBox\">"
                                + "" + generateFileColSelectBoxStr(destColumnList, (String) destColumnList.get(i), "DEST_FILE_SELECT_" + i) + ""
                                + "</select>"
                                + "</td>";
                    }

                    if (sourceColumnList != null && sourceColumnList.size() != 0) {
                        sourceTr += ""
                                + "<td id = " + sourceColumnList.get(i) + " class=\"sourceColsTd\"><select id=\"SOURCE_FILE_SELECT_" + i + "\" class=\"destColsSelectBox\" >"
                                + "" + generateFileColSelectBoxStr(sourceColumnList, (String) sourceColumnList.get(i), "SOURCE_FILE_SELECT_" + i) + ""
                                + "</select>"
                                + "</td>"
                                + "<td><input type=\"text\" style=\"width:194px\" class=\"visionDMDefaultValues\" id=\"DEFAULTVALUESFILES_" + i + "\"></td>"
                                + "</tr>";
                    }

                }

            }
            sourceTr += "</tbody>";

            result += "<table id=\"sourceDestColsFilesTableId\" class=\"sourceDestColsFilesTable\" style='width: auto;' border='1'>" + sourceTr + "</table>";
//            System.out.println("columnStr::: " + result);
            // resultObj.put("columnStr",columnStr);
        } catch (Exception e) {
            e.printStackTrace();

        }
        return result;
    }

    public String generateFileColSelectBoxStr(List columnList, String selectedColumn, String selectBoxId) {
        String selectBoxStr = "<option>Select</option>";
        try {
            for (int i = 0; i < columnList.size(); i++) {

                String selectedStr = "";
                if (selectedColumn != null
                        && !"".equalsIgnoreCase(selectedColumn) && selectedColumn.equalsIgnoreCase(String.valueOf(columnList.get(i)))) {
                    selectedStr = "selected";
                }
                selectBoxStr += "<option  value='" + columnList.get(i) + "'"
                        + " id ='" + selectBoxId + "_" + columnList.get(i) + "' data-tablename='" + columnList.get(i) + "' "
                        + "" + selectedStr + ">" + columnList.get(i) + "</option>";

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return selectBoxStr;
    }

//    public String mappingSourceFileColsWithDestCols(HttpServletRequest request, HttpServletResponse response) {
//        String result = "";
//        try {
//            String mappedFileCols = request.getParameter("mappedFileColsObj");
//            String defaultValues = request.getParameter("defaultValObj");
//            String filePath = request.getParameter("filePath");
//            String fileExt = filePath.substring(filePath.lastIndexOf(".") + 1, filePath.length());
//            JSONObject mappedFileColsObj = new JSONObject();
//            JSONObject defaultValuesObj = new JSONObject();
//            if (mappedFileCols != null && !"".equalsIgnoreCase(mappedFileCols) && !"null".equalsIgnoreCase(mappedFileCols)) {
//                mappedFileColsObj = (JSONObject) JSONValue.parse(mappedFileCols);
//                System.out.println("mappedFileColsObj:::" + mappedFileColsObj);
//            }
//            if (defaultValues != null && !"".equalsIgnoreCase(defaultValues) && !"null".equalsIgnoreCase(defaultValues)) {
//                defaultValuesObj = (JSONObject) JSONValue.parse(defaultValues);
//                System.out.println("defaultValuesObj:::" + defaultValuesObj);
//            }
//            if (!(defaultValuesObj != null && !defaultValuesObj.isEmpty())) {
//                defaultValuesObj = new JSONObject();
//            }
//            defaultValuesObj.put("CREATE_BY", request.getSession(false).getAttribute("ssUsername"));
//            defaultValuesObj.put("EDIT_BY", request.getSession(false).getAttribute("ssUsername"));
//            System.out.println("mappedFileCols:::" + mappedFileCols);
//            System.out.println("defaultValues:::" + defaultValues);
//            System.out.println("filePath:::" + filePath);
//            if (fileExt != null && !"".equalsIgnoreCase(fileExt) && !"null".equalsIgnoreCase(fileExt)) {
//                if ("xls".equalsIgnoreCase(fileExt) || "xlsx".equalsIgnoreCase(fileExt)) {
//                    result = visionGenericImportService.importExcel(request, response, filePath, request.getParameter("gridId"), request.getParameter("tableName"), mappedFileColsObj, defaultValuesObj);
//                } else if ("txt".equalsIgnoreCase(fileExt) || "csv".equalsIgnoreCase(fileExt)) {
//                    result = visionGenericImportService.importCSV(request, response, filePath, request.getParameter("gridId"), request.getParameter("tableName"), mappedFileColsObj, defaultValuesObj);
//                } else if ("xml".equalsIgnoreCase(fileExt)) {
//                    result = visionGenericImportService.importXML(request, response, filePath, request.getParameter("gridId"), request.getParameter("tableName"), mappedFileColsObj, defaultValuesObj);
//                }
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return result;
//    }
    public JSONObject getErpConnectionDetails(HttpServletRequest request) {
        JCO.Client connection = null;
        String tableDataStr = "";

        String saveDetails = "";
        JSONObject resultObj = new JSONObject();
        String updateConnectionDetails = "";

        try {
            JSONObject dbObj = new JSONObject();
            System.out.println("selectedItemLabel:::::::::::::::" + request.getParameter("selectedItemLabel"));
            dbObj.put("selectedItemLabel", request.getParameter("selectedItemLabel"));
            dbObj.put("ClientId", request.getParameter("ClientId"));
            dbObj.put("hostName", request.getParameter("hostName"));
            dbObj.put("userName", request.getParameter("userName"));
            dbObj.put("password", request.getParameter("password"));
            dbObj.put("LanguageId", request.getParameter("LanguageId"));
            dbObj.put("ERPSystemId", request.getParameter("ERPSystemId"));
            dbObj.put("group", request.getParameter("group"));
            String filterTableVal = request.getParameter("filerTableValue");

            Object returnObj = getErpConnection(dbObj);
//            connection = (JCO.Client) v10MigrationDataAccess.getSAPConnection(request.getParameter("ClientId"),
//                    request.getParameter("userName"), request.getParameter("password"),
//                     request.getParameter("LanguageId"), request.getParameter("hostName"),
//                     request.getParameter("ERPSystemId"));
            if (returnObj != null && returnObj instanceof JCO.Client) {
                connection = (JCO.Client) returnObj;
                String checkBoxVal = request.getParameter("checkedVal");
                String auditIdVal = request.getParameter("auditId");
                if (checkBoxVal != null && !"".equalsIgnoreCase(checkBoxVal)
                        && !"null".equalsIgnoreCase(checkBoxVal)
                        && "true".equalsIgnoreCase(checkBoxVal)) {
                    if (auditIdVal != null && !"".equalsIgnoreCase(auditIdVal) && !"null".equalsIgnoreCase(auditIdVal)) {
                        updateConnectionDetails = v10GenericDataMigrationDAO.updateErpConnectionDetails(request);
                        // updateConnectionDetails ;
                    } else {
                        saveDetails = v10GenericDataMigrationDAO.saveErpConnectionDetails(request);
                    }

                }
                String erpEtlFlag = request.getParameter("EtlERPFlag");
                if (erpEtlFlag != null && !"".equalsIgnoreCase(erpEtlFlag) && "Y".equalsIgnoreCase(erpEtlFlag)) {
                    resultObj.put("connectionMessage", "SAP connection established successfully,Please check in Available Connections!");
                } else {
                    String tablesList = v10DataMigrationAccessTables.GetSapListOfTable(connection, filterTableVal);
                    resultObj.put("tablesList", tablesList);
                }
                //String tablesList = getDatabaseTables(dbObj, connection,filterTableVal);
//                String tablesList = v10DataMigrationAccessTables.GetSapListOfTable(connection, filterTableVal);
//
//                resultObj.put("tablesList", tablesList);
                resultObj.put("updateMessage", updateConnectionDetails);
                resultObj.put("dbObj", dbObj);
                resultObj.put("messageFlag", saveDetails);
                resultObj.put("connectionFlag", "Y");

            } else {
                resultObj.put("connectionFlag", "N");
                resultObj.put("connectionMessage", returnObj);
            }

        } catch (Exception e) {
            e.printStackTrace();
            resultObj.put("connectionFlag", "N");
            resultObj.put("connectionMessage", e.getMessage());
            return resultObj;
        }
        return resultObj;
    }

    public Object getErpConnection(JSONObject dbObj) {

        Connection connection = null;
        Object returnObj = null;
        try {
            String initParamClassName = "com.pilog.mdm.transformaccess.V10MigrationDataAccess";
            String initParamMethodName = "get" + dbObj.get("selectedItemLabel") + "Connection";
            System.out.println(" initParamClassName:" + initParamClassName + "initParamMethodName:" + initParamMethodName);
            Class clazz = Class.forName(initParamClassName);
            Class<?>[] paramTypes = {String.class, String.class, String.class, String.class, String.class, String.class, String.class};
            Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
            Object targetObj = new PilogUtilities().createObjectByClass(clazz);
            returnObj = method.invoke(targetObj, String.valueOf(dbObj.get("ClientId")), String.valueOf(dbObj.get("userName")),
                    String.valueOf(dbObj.get("password")), String.valueOf(dbObj.get("LanguageId")), String.valueOf(dbObj.get("hostName")),
                    String.valueOf(dbObj.get("ERPSystemId")), String.valueOf(dbObj.get("group")));

        } catch (Exception e) {
            e.printStackTrace();
        }
        return returnObj;
    }

    public JSONObject mappingErpSourceColsWithDestCols(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        PreparedStatement importStmt = null;
        Connection sourceConn = null;
        JCO.Client connection = null;
        try {
            String connectionname = request.getParameter("selectedItemLabel");
            // if(connectionname!=null && connectionname.equalsIgnoreCase("SAP")){
            JSONObject dbObj = new JSONObject();
            dbObj.put("selectedItemLabel", request.getParameter("selectedItemLabel"));
            dbObj.put("hostName", request.getParameter("hostName"));
            dbObj.put("ClientId", request.getParameter("ClientId"));
            dbObj.put("userName", request.getParameter("userName"));
            dbObj.put("password", request.getParameter("password"));
            dbObj.put("LanguageId", request.getParameter("LanguageId"));
            dbObj.put("ERPSystemId", request.getParameter("ERPSystemId"));
            dbObj.put("group", request.getParameter("group"));
            Object returendObj = getErpConnection(dbObj);
            connection = (JCO.Client) v10MigrationDataAccess.getSAPConnection(request.getParameter("ClientId"),
                    request.getParameter("userName"), request.getParameter("password"),
                    request.getParameter("LanguageId"), request.getParameter("hostName"),
                    request.getParameter("ERPSystemId"),
                    request.getParameter("group"));
            String destTableName = request.getParameter("tableName");
            String sourceTableName = request.getParameter("tablesObj");
            JSONObject destColumnsObj = new JSONObject();
            JSONObject columnsObj = new JSONObject();
            JSONObject tablesObj = new JSONObject();
            JSONObject defaultValuesObj = new JSONObject();
            String defaultValue = request.getParameter("defaultValObj");
            if (connection != null) {
                if (sourceTableName != null && !"".equalsIgnoreCase(sourceTableName) && !"null".equalsIgnoreCase(sourceTableName)) {
                    tablesObj = (JSONObject) JSONValue.parse(sourceTableName);
                    System.out.println("tablesObj:::" + tablesObj);
                }
                if (defaultValue != null && !"".equalsIgnoreCase(defaultValue) && !"null".equalsIgnoreCase(defaultValue)) {
                    defaultValuesObj = (JSONObject) JSONValue.parse(defaultValue);
                    System.out.println("defaultValuesObj:::" + defaultValuesObj);
                }
                if (!(defaultValuesObj != null && !defaultValuesObj.isEmpty())) {
                    defaultValuesObj = new JSONObject();
                }
                String columnsObjStr = request.getParameter("columnsObj");
                if (columnsObjStr != null && !"".equalsIgnoreCase(columnsObjStr) && !"null".equalsIgnoreCase(columnsObjStr)) {
                    columnsObj = (JSONObject) JSONValue.parse(columnsObjStr);
                    System.out.println("columnsObj:::" + columnsObj);

                    if (columnsObj != null && !columnsObj.isEmpty()) {

                        for (Object sourceColumn : columnsObj.keySet()) {
                            if (sourceColumn != null && columnsObj.get(sourceColumn) != null) {
                                String destColumnStr = (String) columnsObj.get(sourceColumn);
                                if (destColumnStr != null
                                        && !"".equalsIgnoreCase(destColumnStr)
                                        && !"null".equalsIgnoreCase(destColumnStr)) {
                                    String[] destColumnArray = destColumnStr.split(":");
                                    if (destColumnArray != null && destColumnArray.length != 0) {
                                        if (destColumnsObj != null && !destColumnsObj.isEmpty()) {
                                            if (destColumnsObj.containsKey(destColumnArray[0])) {
                                                destColumnsObj.put(destColumnArray[0],
                                                        (destColumnsObj.get(destColumnArray[0]) + "," + destColumnArray[1]));
                                            } else {
                                                destColumnsObj.put(destColumnArray[0],
                                                        (destColumnArray[1]));
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
                        defaultValuesObj.put("CREATE_BY", request.getSession(false).getAttribute("ssUsername"));
                        defaultValuesObj.put("EDIT_BY", request.getSession(false).getAttribute("ssUsername"));
                        Class.forName(dataBaseDriver);
                        sourceConn = DriverManager.getConnection(dbURL, userName, password);
                        Set<String> destColumns = new HashSet<>(columnsObj.keySet());
                        if (defaultValuesObj != null && !defaultValuesObj.isEmpty()) {
                            Set<String> defaultColumns = defaultValuesObj.keySet();
                            if (defaultColumns != null
                                    && !defaultColumns.isEmpty()
                                    && destColumns != null && !destColumns.isEmpty()) {
                                destColumns.addAll(defaultColumns);
                            }
                        }

                        List<String> columnsList = new ArrayList<>();
                        columnsList.addAll(destColumns);

                        String insertQuery = generateInsertQuery(destTableName, columnsList);

                        int totalDataCount = 0;
                        importStmt = sourceConn.prepareStatement(insertQuery);
                        // v10GenericDataMigrationDAO.getErpSelectedColumnsData(request,destColumnsObj, tablesObj,connection1);
                        totalDataCount = bulkErpMappingSourceColsWithDestCols(dbObj, connection, destColumnsObj,
                                1, 500, columnsObj, destTableName, importStmt, totalDataCount, columnsList, tablesObj, defaultValuesObj);
                        System.out.println("totalDataCount::::" + totalDataCount);
                        if (totalDataCount != 0 && totalDataCount > 0) {
                            resultObj.put("Message", totalDataCount + " Row(s) migrated successfully");
                            resultObj.put("connectionFlag", "Y");

                        } else {
                            resultObj.put("connectionMessage", "TableData Empty");
                        }

                    }
                }
                //}
            } else {
                resultObj.put("connectionFlag", "N");
                resultObj.put("connectionMessage", returendObj);
            }
            System.out.println("destColumnsObj:::" + destColumnsObj);
        } catch (Exception e) {
            resultObj.put("connectionFlag", "N");
            resultObj.put("connectionMessage", e.getMessage());
            e.printStackTrace();
        }
        return resultObj;
    }

    public int bulkErpMappingSourceColsWithDestCols(JSONObject dbObj, JCO.Client connection, JSONObject destColumnsObj, int start, int limit,
            JSONObject columnsObj, String destTableName, PreparedStatement importStmt, int totalDataCount, List<String> sourceColumnsList, JSONObject tablesObj, JSONObject defaultValuesObj) {
        JSONObject resultObj = new JSONObject();
        try {
            List totalData = v10DataMigrationAccessTables.getErpSelectedColumnsData(destColumnsObj, tablesObj, connection);
            if (totalData != null && !totalData.isEmpty()) {
                int insertCount = importingData(destTableName, destColumnsObj, columnsObj, totalData, importStmt, sourceColumnsList, defaultValuesObj);
                if (insertCount != 0) {
                    totalDataCount += insertCount;
//                    totalDataCount = bulkErpMappingSourceColsWithDestCols(dbObj, JCO.Client connection, destColumnsObj,
//                            (start + limit), limit, columnsObj, destTableName, importStmt,
//                            totalDataCount, sourceColumnsList, tablesObj, defaultValuesObj);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return totalDataCount;
    }

    public List getOracleErpSavedConnections(HttpServletRequest request) {
        List connectionsList = new ArrayList();
        try {
            connectionsList = v10GenericDataMigrationDAO.getOracleErpSavedConnections(request);
        } catch (Exception e) {
        }
        return connectionsList;
    }

    public List getSavedErpConnections(HttpServletRequest request) {
        List connectionsList = new ArrayList();
        try {
            connectionsList = v10GenericDataMigrationDAO.getSavedErpConnections(request);
        } catch (Exception e) {
        }
        return connectionsList;
    }

    public String getErpDatabaseTables(JSONObject dbObj, Connection connection, String filterTableVal) {
        String result = "";
        try {

            String initParamClassName = "com.pilog.mdm.DAO.V10DataMigrationAccessDAO";
            String initParamMethodName = "get" + dbObj.get("selectedItemLabel") + "DatabaseTables";
            System.out.println(" initParamClassName:" + initParamClassName + "initParamMethodName:" + initParamMethodName);
            Class clazz = Class.forName(initParamClassName);
            Class<?>[] paramTypes = {Connection.class, String.class, String.class};
            Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
            Object targetObj = new PilogUtilities().createObjectByClass(clazz);
            result = (String) method.invoke(targetObj, connection, String.valueOf(dbObj.get("serviceName")), filterTableVal);

        } catch (Exception e) {

            e.printStackTrace();
        }
        return result;
    }

    public String deleteErpConnectionDetails(HttpServletRequest request) {
        String result = "";
        try {
            result = v10GenericDataMigrationDAO.deleteErpConnectionDetails(request);
        } catch (Exception e) {
        }
        return result;
    }

    public JSONObject fetchErpTableColumns(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        //Connection connection = null;
        JCO.Client connection = null;
        try {
            JSONObject dbObj = new JSONObject();
            dbObj.put("selectedItemLabel", request.getParameter("selectedItemLabel"));
            dbObj.put("hostName", request.getParameter("hostName"));
            dbObj.put("ClientId", request.getParameter("ClientId"));
            dbObj.put("userName", request.getParameter("userName"));
            dbObj.put("password", request.getParameter("password"));
            dbObj.put("LanguageId", request.getParameter("LanguageId"));
            dbObj.put("ERPSystemId", request.getParameter("ERPSystemId"));
            dbObj.put("group", request.getParameter("group"));
            String destTableName = request.getParameter("tableName");
            String sourceTableName = request.getParameter("tablesArray");
            Object returendObj = getErpConnection(dbObj);
            connection = (JCO.Client) v10MigrationDataAccess.getSAPConnection(request.getParameter("ClientId"),
                    request.getParameter("userName"), request.getParameter("password"),
                    request.getParameter("LanguageId"), request.getParameter("hostName"),
                    request.getParameter("ERPSystemId"),
                    request.getParameter("group"));
            String matchedSelectStr = "";
            if (returendObj != null) {
                String selectedTables = "<table class=\"visionSelectedTables\" id='selectedTables' style='width: 100%;' border='1'><thead>"
                        + "<tr><th style='width:225px;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Table Name</th>"
                        + "<th style='width:475px;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Table Where Clause Cond.</th>"
                        + "</tr></thead><tbody>";
                String sourceTr = "<thead><tr><th class=\"mappedColsTh\" style='width:260px;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'> Source Columns</th>"
                        + "<th class=\"mappedColsTh\" style='width:260px;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Destination Columns</th>"
                        + "<th class=\"mappedColsTh\" style='width:190px;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Default Values</th>"
                        + "</tr></thead>";
                if (sourceTableName != null && !"".equalsIgnoreCase(sourceTableName) && !"null".equalsIgnoreCase(sourceTableName)) {
                    JSONArray tablesValuesArr = (JSONArray) JSONValue.parse(sourceTableName);
                    List tablesList = new ArrayList();
                    tablesList.addAll(tablesValuesArr);
                    if (tablesList != null && !tablesList.isEmpty()) {
                        for (int k = 0; k < tablesList.size(); k++) {
                            selectedTables += "<tr><td  id=SELECT_TABLE_" + k + " style='width: 225px;' data-table = " + tablesList.get(k) + ">" + tablesList.get(k) + "</td>"
                                    + "<td colspan=\"3\" style='width:460px'><input type=\"text\" style='width:100%' id=TABLE_INPUT" + k + " ></td></tr>";
                        }
                    }
                    List<Object[]> destinationColumnList = v10DataMigrationAccessTables.getErpTablecolumn(request, connection);
                    List<Object[]> sourceColumnList = new ArrayList<>();

                    String gridColumns = request.getParameter("dataFeilds");
                    if (gridColumns != null && !"".equalsIgnoreCase(gridColumns)) {
                        JSONArray gridColumnsArray = (JSONArray) JSONValue.parse(gridColumns);
                        if (gridColumnsArray != null && !gridColumnsArray.isEmpty()) {
                            for (int i = 0; i < gridColumnsArray.size(); i++) {
                                JSONObject dataFieldObj = (JSONObject) gridColumnsArray.get(i);
                                if (dataFieldObj != null && !dataFieldObj.isEmpty()) {
                                    String columnName = (String) dataFieldObj.get("name");
                                    if (columnName != null
                                            && !"".equalsIgnoreCase(columnName)
                                            && !(columnName.startsWith("HIDDEN_")
                                            || columnName.endsWith("_HIDDEN")
                                            || "CREATE_BY".equalsIgnoreCase(columnName)
                                            || "EDIT_BY".equalsIgnoreCase(columnName)
                                            || "CREATE_DATE".equalsIgnoreCase(columnName)
                                            || "AUDIT_ID".equalsIgnoreCase(columnName)
                                            || "EDIT_DATE".equalsIgnoreCase(columnName))) {
                                        Object[] sourceArray = new Object[2];
                                        sourceArray[0] = destTableName;
                                        sourceArray[1] = columnName;
                                        sourceColumnList.add(sourceArray);
                                    }

                                }

                            }
                        }
                    }
                    sourceTr += "<tbody>";

                    if (sourceColumnList != null
                            && !sourceColumnList.isEmpty()
                            && destinationColumnList != null && !destinationColumnList.isEmpty()) {
                        int minSize = (sourceColumnList.size() > destinationColumnList.size()) ? destinationColumnList.size() : sourceColumnList.size();
                        for (int i = 0; i < sourceColumnList.size(); i++) {
                            Object[] sourceColumnsArray = sourceColumnList.get(i);
                            if (sourceColumnsArray != null && sourceColumnsArray.length != 0) {
                                sourceTr += "<tr><td id=" + sourceColumnsArray[1] + " class=\"sourceColsTd\">"
                                        + "<select id=\"SOURCE_SELECT_" + i + "\"  class=\"sourceColsSelectBox\">"
                                        + "" + generateSelectBoxStr(sourceColumnList, (sourceColumnsArray[0] + ":" + sourceColumnsArray[1]), "SOURCE_SELECT_" + i) + ""
                                        + "</select>"
                                        + "</td>";
                            }
                            if (i < destinationColumnList.size()) {
                                Object[] destColumnsArray = destinationColumnList.get(i);
                                if (destColumnsArray != null && destColumnsArray.length != 0) {
                                    sourceTr += ""
                                            + "<td><select id=\"DEST_SELECT_" + i + "\" class=\"destColsSelectBox\" >"
                                            + "" + generateSelectBoxStr(destinationColumnList, (destColumnsArray[0] + ":" + destColumnsArray[1]), "DEST_SELECT_" + i) + ""
                                            + "</select>"
                                            + "</td><td><input type=\"text\" class=\"defaultValues\" id=\"DEFAULTVALUES_" + i + "\"></td></tr>";
                                }
                            } else {
                                sourceTr += ""
                                        + "<td><select id=\"DEST_SELECT_" + i + "\" class=\"destColsSelectBox\" >"
                                        + "" + generateSelectBoxStr(destinationColumnList, "", "DEST_SELECT_" + i) + ""
                                        + "</select>"
                                        + "</td><td><input type=\"text\" class=\"defaultValues\" id=\"DEFAULTVALUES_" + i + "\"></td></tr>";

                            }

                        }
                    }

                    sourceTr += "</tbody>";
                    selectedTables += "</tbody></table>";

                    String columnStr = "<table id=\"sourceDestColsTableId\" class=\"sourceDestColsTable\" style='width: auto;' border='1'>" + sourceTr + "</table>";
                    resultObj.put("columnStr", columnStr);
                    resultObj.put("matchedSelectStr", matchedSelectStr);
                    resultObj.put("dbObj", dbObj);
                    resultObj.put("selectedTables", selectedTables);
                    resultObj.put("connectionFlag", "Y");
                }
            } else {
                resultObj.put("connectionFlag", "N");
                resultObj.put("connectionMessage", returendObj);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    public String getConnDetails(HttpServletRequest request) {
        String result = "";
        String dbType = "";
        try {
            String connectonName = request.getParameter("connectonName");
            List<Object[]> connectionDetailsList = v10GenericDataMigrationDAO.getConDetails(request, connectonName);
            if (connectionDetailsList != null && !connectionDetailsList.isEmpty()) {
                dbType = (String) connectionDetailsList.get(0)[7];
            }

            if (dbType != null && !"".equalsIgnoreCase(dbType)
                    && "DB".equalsIgnoreCase(dbType) || "Oracle_ERP".equalsIgnoreCase(dbType)) {

                if (connectionDetailsList != null && !connectionDetailsList.isEmpty()) {
                    String connName = (String) connectionDetailsList.get(0)[0];
                    String hostName = (String) connectionDetailsList.get(0)[1];
                    String connUserName = (String) connectionDetailsList.get(0)[2];
                    String connPassword = (String) connectionDetailsList.get(0)[3];
                    String connPort = (String) connectionDetailsList.get(0)[4];
                    String connDBName = (String) connectionDetailsList.get(0)[5];
                    String connType = (String) connectionDetailsList.get(0)[6];

                    String auditId = (String) connectionDetailsList.get(0)[10];

                    result += "<div class='visionEtlConnectDbMain'>"
                            + "<div id='visionShowConnectionMsg'></div>"
                            + "<table class='visionEtlDbTable table table-bordered' autocomplete='false'>"
                            + "<tr><td><p style='font-weight:bold'>Connection Type:</p></td>"
                            + "<td>" + connType + "</td></tr>"
                            + "<tr>"
                            + "<td> <label class='visionDbLabels'>Connection Name</label></td>"
                            + "<td> <input type='text'  name='ConnectionName' id='connectionNameEtl' class='visionInputDbFields' autocomplete='false' value='" + connName + "'>"
                            + "<div class='dataMigrationInputError' id='connectionNameEtlError'></div></td>"
                            + " </tr>"
                            + "<tr>";

                    result += "<td> <label class='visionDbLabels'>Host Name</label></td>"
                            + "<td> <input type='text'  name='HostName' id='hostNameEtl' class='visionInputDbFields' autocomplete='false' value='" + hostName + "'>"
                            + "<div class='dataMigrationInputError' id='hostNameEtlError'></div></td>"
                            + "</tr>"
                            + "<tr>"
                            + "<td>  <label class='visionDbLabels'>Port</label></td>"
                            + "<td><input type='text'  name='Port' id='portEtl' class='visionInputDbFields' autocomplete='false' value='" + connPort + "'>"
                            + "<div class='dataMigrationInputError' id='portEtlError'></div></td>"
                            + "</tr>"
                            + "<tr>"
                            + "<td>  <label class='visionDbLabels'>Username</label></td>"
                            + "<td> <input type='text'  name='EtlUsername' id='userNameEtl' class='visionInputDbFields' autocomplete='false' value='" + connUserName + "'>"
                            + "<div class='dataMigrationInputError' id='userNameEtlError'></div></td>"
                            + "</tr>"
                            + " <tr>"
                            + "<td>  <label class='visionDbLabels'>Password</label></td>"
                            + "<td>    <input type='password'   name='EtlPassword' id='passwordEtl' class='visionInputDbFields' autocomplete='false' value='" + connPassword + "'>"
                            + "<div class='dataMigrationInputError' id='passwordEtlError'></div></td>"
                            + "</tr>"
                            + "<tr>"
                            + "<td>  <label class='visionDbLabels'>Database/Service Name</label></td>"
                            + "<td>    <input type='text'  name='ServiceName' id='serviceNameEtl' class='visionInputDbFields' autocomplete='false' value='" + connDBName + "'>"
                            + "<div class='dataMigrationInputError' id='serviceNameEtlError'></div></td>"
                            + "</tr>"
                            + "<tr style='display:none'>"
                            + "<td>  <label class='visionDbLabels'>Audit Id</label></td>"
                            + "<td>    <input type='hidden'  name='auditId' id='auditIdEtl' class='visionInputDbFields' value='" + auditId + "'></td>"
                            + "</tr>"
                            + "<tr>"
                            + "<td colspan = '2'>"
                            + "<div class='connectionPopUpButtons'>"
                            + "<input type=\"button\" value=\"Update\" name=\"Connect\" title=\\\"Update Connection\\\" onclick = \"updateConnection('" + auditId + "','" + dbType + "')\" class=\"visionInputDbUpdateButton\">"
                            //                           + "<img src=\"images/update_icon.png\" style=\"width:16px;height:16px;padding:2px\" id=\"connectionUpdateEtl\" title=\"Update Connection\" onclick = \"updateConnection('" + auditId + "','" + dbType + "')\" class=\"visionDBConnectIcon\">"
                            //                            + "<img src=\"images/delete_icon1.png\" id=\"connectionDeleteEtl\" style=\"padding:2px\" title=\"Delete Connection\" onclick = \"deleteConnection('" + auditId + "')\" class=\"visionDBConnectIcon\">"

                            + "<input type=\"button\" value=\"Delete\" title=\"Delete Connection\" name=\"Connect\" onclick = \"deleteConnection('" + auditId + "')\" class=\"visionInputDbDeleteButton\">"
                            //                           + "<img src=\"images/update_icon.png\" style=\"width:16px;height:16px;padding:2px\" id=\"connectionUpdateEtl\" title=\"Update Connection\" onclick = \"updateConnection('" + auditId + "','" + dbType + "')\" class=\"visionDBConnectIcon\">"
                            //                            + "<img src=\"images/delete_icon1.png\" id=\"connectionDeleteEtl\" style=\"padding:2px\" title=\"Delete Connection\" onclick = \"deleteConnection('" + auditId + "')\" class=\"visionDBConnectIcon\">"

                            + "</div>"
                            + "</td>"
                            + "</tr>"
                            + "</table></div>";

                }
            } else if (dbType != null && "ERP".equalsIgnoreCase(dbType)) {
                String connName = (String) connectionDetailsList.get(0)[0];
                String hostName = (String) connectionDetailsList.get(0)[1];
                String connUserName = (String) connectionDetailsList.get(0)[2];
                String connPassword = (String) connectionDetailsList.get(0)[3];
                String connClient = (String) connectionDetailsList.get(0)[4];
                String systemId = (String) connectionDetailsList.get(0)[5];
                String languageId = (String) connectionDetailsList.get(0)[8];
                String group = (String) connectionDetailsList.get(0)[9];

                String auditId = (String) connectionDetailsList.get(0)[10];

                result += "<div class='visionEtlConnectDbMain'>"
                        + "<div id='visionShowConnectionMsg'></div>"
                        + "<table class='visionEtlDbTable' autocomplete='false'>"
                        + "<td> <label class='visionDbLabels'>Connection Name</label></td>"
                        + "<td> <input type='text'  name='EtlConnectionName' id='erpConnectionNameEtl' class='visionInputDbFields' autocomplete='false' value='" + connName + "'>"
                        + "<div class='dataMigrationInputError' id='erpConnectionNameEtlError'></div></td>"
                        + " </tr>";
                if (group != null && !"".equalsIgnoreCase(group) && !"null".equalsIgnoreCase(group)) {
                    result += "<td> <label class='visionDbLabels'>Group</label></td>"
                            + "<td> <input type='text'  name='GroupName' id='GroupNameEtl' class='visionInputDbFields' autocomplete='false' value='" + group + "'>"
                            + "<div class='dataMigrationInputError' id='groupNameEtlError'></div></td>"
                            + "</tr>";
                }

                result += "<tr>"
                        + "<td> <label class='visionDbLabels'>Client</label></td>"
                        + "<td> <input type='text'  name='Etlclient' id='erpClientEtl' class='visionInputDbFields' autocomplete='false' value='" + connClient + "'>"
                        + "<div class='dataMigrationInputError' id='erpClientEtlError'></div></td>"
                        + "</tr>"
                        + "<tr>"
                        + "<td> <label class='visionDbLabels'>Host Name</label></td>"
                        + "<td> <input type='text'  name='EtlHostName' id='erpHostNameEtl' class='visionInputDbFields' autocomplete='false' value='" + hostName + "'>"
                        + "<div class='dataMigrationInputError' id='erpHostNameEtlError'></div></td>"
                        + "</tr>"
                        + "<tr>"
                        + "<td>  <label class='visionDbLabels'>Username</label></td>"
                        + "<td> <input type='text'  name='EtlUsername' id='erpUserNameEtl' class='visionInputDbFields' autocomplete='false' value='" + connUserName + "'>"
                        + "<div class='dataMigrationInputError' id='erpUserNameEtlError'></div></td>"
                        + "</tr>"
                        + " <tr>"
                        + "<td>  <label class='visionDbLabels'>Password</label></td>"
                        + "<td>    <input type='password'   name='EtlPassword' id='erpPasswordEtl' class='visionInputDbFields' autocomplete='false' value='" + connPassword + "'>"
                        + "<div class='dataMigrationInputError' id='erpPasswordEtlError'></div></td>"
                        + "</tr>"
                        + "<tr>"
                        + "<td>  <label class='visionDbLabels'>Language Id</label></td>"
                        + "<td>    <input type='text'  name='EtlLanguageId' id='erpLanguageIdEtl' class='visionInputDbFields' autocomplete='false' value='" + languageId + "'>"
                        + "<div class='dataMigrationInputError' id='erpLnguageIdEtlError'></div></td>"
                        + "</tr>"
                        + "<tr>"
                        + "<td>  <label class='visionDbLabels'>System Id</label></td>"
                        + "<td>    <input type='text'  name='SystemId' id='erpSystemIdEtl' class='visionInputDbFields' autocomplete='false' value='" + systemId + "'>"
                        + "<div class='dataMigrationInputError' id='erpSystemIdEtlrror'></div></td>"
                        + "</tr>"
                        + "<tr style='display:none'>"
                        + "<td>  <label class='visionDbLabels'>Audit Id</label></td>"
                        + "<td>    <input type='hidden'  name='auditId' id='erpAuditIdEtl' class='visionInputDbFields' value='" + auditId + "'></td>"
                        + "</tr>"
                        + "<tr>"
                        + "<td colspan = '2'>"
                        + "<div class='connectionPopUpButtons'>"
                        + "<input type=\"button\" value=\"Update\" name=\"Connect\" title=\\\"Update Connection\\\" onclick = \"updateConnection('" + auditId + "','" + dbType + "')\" class=\"visionInputDbUpdateButton\">"
                        //                           + "<img src=\"images/update_icon.png\" style=\"width:16px;height:16px;padding:2px\" id=\"connectionUpdateEtl\" title=\"Update Connection\" onclick = \"updateConnection('" + auditId + "','" + dbType + "')\" class=\"visionDBConnectIcon\">"
                        //                            + "<img src=\"images/delete_icon1.png\" id=\"connectionDeleteEtl\" style=\"padding:2px\" title=\"Delete Connection\" onclick = \"deleteConnection('" + auditId + "')\" class=\"visionDBConnectIcon\">"

                        + "<input type=\"button\" value=\"Delete\" title=\"Delete Connection\" name=\"Connect\" onclick = \"deleteConnection('" + auditId + "')\" class=\"visionInputDbDeleteButton\">"
                        //                           + "<img src=\"images/update_icon.png\" style=\"width:16px;height:16px;padding:2px\" id=\"connectionUpdateEtl\" title=\"Update Connection\" onclick = \"updateConnection('" + auditId + "','" + dbType + "')\" class=\"visionDBConnectIcon\">"
                        //                           
                        //                        + "<div>"
                        //                        + "<img src=\"images/update_icon.png\" style=\"width:16px;height:16px;padding:2px\" id=\"connectionUpdateEtl\" title=\"Update Connection\" onclick = \"updateConnection('" + auditId + "','" + dbType + "')\" class=\"visionDBConnectIcon\">"
                        //                        + "<img src=\"images/delete_icon1.png\" id=\"connectionDeleteEtl\" style=\"padding:2px\" title=\"Delete Connection\" onclick = \"deleteConnection('" + auditId + "')\" class=\"visionDBConnectIcon\">"
                        //                        + "</div>"
                        + "</div>"
                        + "</td>"
                        + "</tr>"
                        + "</table></div>";
            }

        } catch (Exception e) {
        }
        return result;
    }

    public String updateConnDetails(HttpServletRequest request) {
        String result = "";
        try {
            String dbType = request.getParameter("dbType");
            if (dbType != null && "DB".equalsIgnoreCase(dbType) || "Oracle_ERP".equalsIgnoreCase(dbType)) {
                result = v10GenericDataMigrationDAO.updateConnectionDetails(request);
            } else if (dbType != null && "ERP".equalsIgnoreCase(dbType)) {
                result = v10GenericDataMigrationDAO.updateErpConnectionDetails(request);
            }

        } catch (Exception e) {
        }
        return result;
    }

    public int deleteDalDmSavedFile(HttpServletRequest request, String fileName) {
        int deleteCount = 0;
        try {
            deleteCount = v10GenericDataMigrationDAO.deleteDalDmSavedFile(request, fileName);
        } catch (Exception e) {
        }
        return deleteCount;
    }

    public int importSAPData(String destTableName,
            Map destColumnsObj, // ravi etl integration
            Map columnsObj,// ravi etl integration
            List totalData,
            JCO.Client toJCOConnection,
            List<String> columnsList,
            JSONObject defaultValuesObj,
            String loginUserName,
            String loginOrgnId,
            String jobId,
            JSONObject selectTabObj) {
        int insertCount = 0;

        try {

            JCOUtills jCOUtills = new JCOUtills();
            if (totalData != null && !totalData.isEmpty()
                    && columnsObj != null && !columnsObj.isEmpty()) {
                Map columnsTypeObj = (Map) selectTabObj.get("columnsTypeObj");

                JSONObject columnClauseObj = (JSONObject) selectTabObj.get("columnClauseObj");
                String functionName = "/PILOG/BAPI_DATA_INSERT";
                if (destTableName != null
                        && !"".equalsIgnoreCase(destTableName)
                        && !"null".equalsIgnoreCase(destTableName)) {
                    insertCount = totalData.size();
                    IRepository iRepository = jCOUtills.retrieveRepository(toJCOConnection);
                    if (iRepository != null) {
//                        String functionName = "/PILOG/BAPI_EQUI_EQBOM_CH";                        
                        JCO.Function function = jCOUtills.getFunction(functionName, iRepository);
                        if (function != null) {
                            String feildsStr = StringUtils.collectionToDelimitedString(columnsList, "|");
                            String tableName = destTableName;
                            if (tableName != null && tableName.contains(".")) {
                                tableName = tableName.split("[.]")[1];
                            }
                            function.getImportParameterList().setValue(tableName, "TAB_NAME");
                            JCO.Table tableData = function.getTableParameterList().getTable("INPTABVAL");
                            JCO.Table tableCols = function.getTableParameterList().getTable("INPTABFLDS");
                            tableCols.appendRow();
                            tableCols.setValue(feildsStr, "FIELD_NAME");
                            function.getTableParameterList().setValue(tableCols, "INPTABFLDS");

                            for (int i = 0; i < totalData.size(); i++) {// rows data
                                Map dataObj = (Map) totalData.get(i);
                                int stmtIndex = 1;
                                if (dataObj != null && !dataObj.isEmpty()) {
                                    String dataStr = "";
                                    for (int j = 0; j < columnsList.size(); j++) {// columns data
                                        String sourceColumnName = columnsList.get(j);
                                        if (sourceColumnName != null
                                                && !"".equalsIgnoreCase(sourceColumnName)
                                                && !"null".equalsIgnoreCase(sourceColumnName)) {
                                            String columnType = "";
                                            if (columnsTypeObj != null && !columnsTypeObj.isEmpty()) {
                                                columnType = String.valueOf(columnsTypeObj.get(sourceColumnName));
                                            }
                                            if (selectTabObj != null
                                                    && !selectTabObj.isEmpty()
                                                    && selectTabObj.get("oldTableName") != null
                                                    && !"".equalsIgnoreCase(String.valueOf(selectTabObj.get("oldTableName")))
                                                    && !"null".equalsIgnoreCase(String.valueOf(selectTabObj.get("oldTableName")))) {
                                                //oldTableName
                                                sourceColumnName = selectTabObj.get("oldTableName") + ":" + sourceColumnName;
                                            }
                                            String dateFormat = "";
                                            if (columnClauseObj != null
                                                    && !columnClauseObj.isEmpty()
                                                    && columnClauseObj.containsKey(sourceColumnName)) {
                                                dateFormat = String.valueOf(columnClauseObj.get(sourceColumnName));
                                            }
//                                    sourceColumnName = destTableName+":"+sourceColumnName;
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
                                                            dataStr += "" + defaultValuesObj.get(sourceColumnName);
                                                            // importStmt.setObject(stmtIndex, dataPipingUtills.convertIntoDBValue(columnType, defaultValuesObj.get(sourceColumnName), sourceColumnName, dateFormat));
                                                        } else {
                                                            dataStr += "";
                                                        }

                                                    } else {
                                                        dataStr += "" + dataObj.get(columnsObj.get(sourceColumnName));
//                                                            importStmt.setObject(stmtIndex,
//                                                                    dataPipingUtills.convertIntoDBValue(columnType, dataObj.get(columnsObj.get(sourceColumnName)), sourceColumnName, dateFormat));
                                                    }

                                                } catch (Exception e) {
                                                    dataStr += "";
//                                                        importStmt.setObject(stmtIndex, "");
                                                    continue;
                                                }
                                                stmtIndex++;
                                            } else {
                                                if (dataObj.get(sourceColumnName) != null
                                                        && !"".equalsIgnoreCase(String.valueOf(dataObj.get(sourceColumnName)))
                                                        && !"null".equalsIgnoreCase(String.valueOf(dataObj.get(sourceColumnName)))) {
                                                    dataStr += "" + dataObj.get(sourceColumnName);
//                                                        importStmt.setObject(stmtIndex,
//                                                                dataPipingUtills.convertIntoDBValue(columnType, dataObj.get(sourceColumnName), sourceColumnName, dateFormat));
                                                } else if (defaultValuesObj != null && !defaultValuesObj.isEmpty()
                                                        && defaultValuesObj.get(sourceColumnName) != null
                                                        && !"".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))
                                                        && !"null".equalsIgnoreCase((String) defaultValuesObj.get(sourceColumnName))) {
                                                    dataStr += "" + defaultValuesObj.get(sourceColumnName);
//                                                        importStmt.setObject(stmtIndex, dataPipingUtills.convertIntoDBValue(columnType, defaultValuesObj.get(sourceColumnName), sourceColumnName, dateFormat));
                                                } else if (columnClauseObj != null
                                                        && !columnClauseObj.isEmpty()
                                                        && columnClauseObj.containsKey(sourceColumnName)) {
                                                    dataStr += "" + dataPipingUtills.convertIntoDBValue(columnType, dateFormat, sourceColumnName, dateFormat);
//                                                        importStmt.setObject(stmtIndex, dataPipingUtills.convertIntoDBValue(columnType, dateFormat, sourceColumnName, dateFormat));
                                                } else {
                                                    dataStr += "";
//                                                        importStmt.setObject(stmtIndex, "");
                                                }
                                                stmtIndex++;
                                            }
                                        }
                                        if (j != columnsList.size() - 1) {
                                            dataStr += "|";
                                        }
                                    }// end columns loop;
                                    if (dataStr != null
                                            && !"".equalsIgnoreCase(dataStr)) {
                                        tableData.appendRow();
                                        tableData.setValue(dataStr, "FIELD_VAL");
                                    }

                                }

                            }// end loop
                            function.getTableParameterList().setValue(tableData, "INPTABVAL");
                            toJCOConnection.execute(function);

                        } else {
                            genericProcessETLDataService.processETLLog(loginUserName,
                                    loginOrgnId,
                                    "Failed to Connect BAPI (" + functionName + ") ", "ERROR", 20, "Y", jobId);
                            System.err.println("Function(" + functionName + ") Not exist.");
                        }

                    } else {
                        genericProcessETLDataService.processETLLog(loginUserName,
                                loginOrgnId,
                                "Failed to Connect BAPI (" + functionName + ") ", "ERROR", 20, "Y", jobId);
                        System.err.println("Function(" + functionName + ") Not exist.");
                    }

                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            try {
                genericProcessETLDataService.processETLLog(loginUserName,
                        loginOrgnId,
                        "Faild to load the data into target table due to " + e.getMessage(), "ERROR", 20, "Y", jobId);
            } catch (Exception ex) {
            }
            insertCount = 0;

        }
        return insertCount;
    }

    // ravi cube
//    public JSONObject getCubeLabels(HttpServletRequest request) {
//        JSONObject resultObj = new JSONObject();
//
//        try {
//
//            resultObj = v10GenericDataMigrationDAO.getCubeLabels(request);
//
//        } catch (Exception e) {
//        }
//        return resultObj;
//    }
//
//    public JSONObject getCubeValue(HttpServletRequest request) {
//        JSONObject resultObj = new JSONObject();
//
//        try {
//
//            resultObj = v10GenericDataMigrationDAO.getCubeValue(request);
//
//        } catch (Exception e) {
//        }
//        return resultObj;
//    }
//
//    public JSONObject getCubeData(HttpServletRequest request) {
//        JSONObject resultObj = new JSONObject();
//
//        try {
//
//            resultObj = v10GenericDataMigrationDAO.getCubeData(request);
//
//        } catch (Exception e) {
//        }
//        return resultObj;
//    }
//
//    public JSONObject getMultiCubeData(HttpServletRequest request) {
//        JSONObject resultObj = new JSONObject();
//
//        try {
//
//            resultObj = v10GenericDataMigrationDAO.getMultiCubeData(request);
//
//        } catch (Exception e) {
//        }
//        return resultObj;
//    }
//    
//        public JSONObject getAllCubesData(HttpServletRequest request) {
//        JSONObject resultObj = new JSONObject();
//
//        try {
//
//            resultObj = v10GenericDataMigrationDAO.getAllCubesData(request);
//
//        } catch (Exception e) {
//        }
//        return resultObj;
//    }
    public int importingDataHib(String destTableName,
            //JSONObject destColumnsObj,
            Map destColumnsObj, // ravi etl integration
            //JSONObject columnsObj,
            Map columnsObj,// ravi etl integration
            List totalData,
            PreparedStatement importStmt, List<String> columnsList,
            JSONObject defaultValuesObj,
            String loginUserName,
            String loginOrgnId,
            String jobId,
            JSONObject selectTabObj) {
        int insertCount = 0;

        try {
            if (totalData != null && !totalData.isEmpty()) {

                for (int i = 0; i < totalData.size(); i++) {
                    Object[] rowData = (Object[]) totalData.get(i);

                    for (int j = 0; j < columnsList.size(); j++) {
                        int stmtIndex = j + 1;
                        Object value = rowData[j];
                        if (value != null) {
                            importStmt.setObject(stmtIndex, value);
                        } else {
                            importStmt.setObject(stmtIndex, "");
                        }
                    }
                    importStmt.addBatch();

                }

                importStmt.executeBatch();
                insertCount = totalData.size();

            }
        } catch (Exception e) {
            e.printStackTrace();
            try {
                genericProcessETLDataService.processETLLog(loginUserName,
                        loginOrgnId,
                        "Faild to load the data into target table due to " + e.getMessage(), "ERROR", 20, "Y", jobId);
            } catch (Exception ex) {
            }
            insertCount = 0;

        }
        return insertCount;
    }

    public JSONObject getSavedDBData(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();

        try {

            resultObj = v10GenericDataMigrationDAO.getSavedDBData(request);

        } catch (Exception e) {
        }
        return resultObj;
    }

    public JSONArray getTreeMenu(HttpServletRequest request, String parentMenuId) {
        JSONArray menuArray = new JSONArray();
        try {
            JSONObject labelObj = pLlogCloudUtills.getMultilingualObject(request);
            if (parentMenuId != null) {
//                List<DalRoleMenu> menuDataList = cloudGenericMenuDAO.getMenuByHighLevelMenu(parentMenuId,
//                        (String) request.getSession(false).getAttribute("ssRole"),
//                        (String) request.getSession(false).getAttribute("ssOrgId"));
                List<DalRoleMenu> menuDataList  = new ArrayList<>();
                if (menuDataList != null && !menuDataList.isEmpty()) {
                    for (int i = 0; i < menuDataList.size(); i++) {
                        DalRoleMenu dalRoleMenu = menuDataList.get(i);
                        if (dalRoleMenu != null) {
                            JSONObject menuObj = new JSONObject();
                            menuObj.put("id", dalRoleMenu.getId().getMenuId());
                            menuObj.put("PARENT_ID", parentMenuId);
                            menuObj.put("PARENT_MENU_ID", parentMenuId);
                            menuObj.put("MENU_ID", dalRoleMenu.getId().getMenuId());
                            menuObj.put("MENU_DESCRIPTION", "<span class='visionMenuTreeLabel'>" + new PilogUtilities().convertIntoMultilingualValue(labelObj, dalRoleMenu.getMenuDescription()) + "</span>");
                            menuObj.put("MAIN_DESCRIPTION", new PilogUtilities().convertIntoMultilingualValue(labelObj, dalRoleMenu.getMainDescription()));
                            menuObj.put("icon", dalRoleMenu.getIcon());
                            menuObj.put("iconsize", "25px");
                            menuObj.put("value", dalRoleMenu.getHref());
                            menuObj.put("TOOL_TIP", new PilogUtilities().convertIntoMultilingualValue(labelObj, dalRoleMenu.getToolTip()));
                            menuArray.add(menuObj);
                            JSONArray subMenuArray = getTreeMenu(request, dalRoleMenu.getId().getMenuId());
                            if (subMenuArray != null && !subMenuArray.isEmpty()) {
                                menuArray.addAll(subMenuArray);
                            }
                        }

                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return menuArray;
    }

//    public List readPDFRestApi(HttpServletRequest request, String filePath) {
//        ArrayList responseBody = new ArrayList();
//        try {
//            if (filePath.contains("\\")) {
//                filePath = filePath.substring(filePath.lastIndexOf("\\") + 1);
//            }
//            if (filePath.contains("/")) {
//              //  filePath = filePath.substring(filePath.lastIndexOf("/") + 1);
//            }
//            HttpHeaders headers = new HttpHeaders();
//            headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
//            MultiValueMap<String, String> inputMap = new LinkedMultiValueMap<>();
////            inputMap.add("tableName", "DAL_DM_SAVED_FILES");
////            inputMap.add("userName", (String) request.getSession(false).getAttribute("ssUsername"));
////            inputMap.add("orgnId", (String) request.getSession(false).getAttribute("ssOrgId"));
////            inputMap.add("FILE_NAME", "SPIRUploadSheet1642749028412.pdf");
//
//            inputMap.add("fileName", filePath);
//            inputMap.add("accessName", "NIICPDB");
//            inputMap.add("responseId ", AuditIdGenerator.genRandom32Hex());
//            
//
//            HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<MultiValueMap<String, String>>(inputMap,
//                    headers);
//            RestTemplate template = new RestTemplate();
//            //ResponseEntity<ArrayList> response = template.postForEntity("http://apihub.piloggroup.com:6654/pdfreader", entity, ArrayList.class);
//            ResponseEntity<ArrayList> response = template.postForEntity("http://idxp.pilogcloud.com:6651/pdfreader", entity, ArrayList.class);
//            responseBody = response.getBody();
//
//            System.out.println("");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return responseBody;
//
//    }

    
      public String readPDFRestApi(HttpServletRequest request, String filePath) {
        String result = "";
        final int numberOfThreads = 1;
        final ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        final List<Future<?>> futures = new ArrayList<>();
        try {
            String responseId = AuditIdGenerator.genRandom32Hex();
            executor.submit(() -> {
                try {
                    String threadName = Thread.currentThread().getName();
                    System.out.println("Hello " + threadName);
                    String userName = (String) request.getSession(false).getAttribute("ssUsername");
                    File file = new File(filePath);
                    FileSystemResource fileData = new FileSystemResource(file);
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.MULTIPART_FORM_DATA);
                    MultiValueMap<String, Object> inputMap = new LinkedMultiValueMap<>();
                    inputMap.add("fileName", fileData);
                    inputMap.add("responseId", responseId);
                    inputMap.add("accessName", "NIICPDB");
                    System.out.println("ACCESS NAME:::::::::::" + accessName);
                    HttpEntity<MultiValueMap<String, Object>> entity = new HttpEntity<>(inputMap, headers);
                    RestTemplate template = new RestTemplate();
                    System.out.println("Request sent ::::: ");
                    long startTime = System.currentTimeMillis();
                    System.out.println("API called :: " + System.currentTimeMillis());
                    ResponseEntity<String> responseObject = template
                            .postForEntity("http://idxp.pilogcloud.com:6651/pdfreader", entity, String.class);
                    String response = responseObject.getBody();
                    System.out.println("Response time :: " + (System.currentTimeMillis() - startTime) / 1000 + " Sec");
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
//                      Thread.sleep(10000);
                        System.out.println("attempt to shutdown executor");
                        executor.shutdown();
                        executor.awaitTermination(5, TimeUnit.SECONDS);
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
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                System.out.println("attempt to shutdown executor");
                executor.shutdown();
                executor.awaitTermination(5, TimeUnit.SECONDS);
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
        return result;
    }

    public String getApiResponse(HttpServletRequest request, String responseId, ExecutorService executor, int currentTimeInMilliSec) {
        String result = "";
        
        int maxWaitingTime = 1800000;
        String apiResponseWaitTimeInMilliSec = (String) request.getParameter("apiResponseWaitTimeInMilliSec");
        if (apiResponseWaitTimeInMilliSec != null && !"".equalsIgnoreCase(apiResponseWaitTimeInMilliSec)
                && !"null".equalsIgnoreCase(apiResponseWaitTimeInMilliSec)) {
            maxWaitingTime = Integer.parseInt(apiResponseWaitTimeInMilliSec); // 5 Minutes
        }
        try {
            if (currentTimeInMilliSec >= maxWaitingTime) {
                executor.shutdown();
            }
            String query = "SELECT RESPONSE_DATA FROM PYTHON_API_RESPONSE WHERE RESPONSE_ID='" + responseId + "'";
            List list = access.sqlqueryWithParams(query, Collections.EMPTY_MAP);
            if (list != null && !list.isEmpty()) {
                result = new PilogUtilities().clobToString((Clob) list.get(0));
            } else {
                // ,each time the for loop runs
                currentTimeInMilliSec += 5000;
                Thread.sleep(5000);
                if (executor.isShutdown()) {
//                    Thread.sleep(10000);
                    query = "SELECT RESPONSE_DATA FROM PYTHON_API_RESPONSE WHERE RESPONSE_ID='" + responseId + "'";
                    list = access.sqlqueryWithParams(query, Collections.EMPTY_MAP);
                    if (list != null && !list.isEmpty()) {
                        result = new PilogUtilities().clobToString((Clob) list.get(0));
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
    
//     public JSONObject getCurrentDBTables(HttpServletRequest request) {
//        JSONObject resultObj = new JSONObject();
//
//        try {
//
//            resultObj = v10GenericDataMigrationDAO.getCurrentDBTables(request);
//
//        } catch (Exception e) {
//        }
//        return resultObj;
//    }
    public JSONObject getTableLabels(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();

        try {

            resultObj = v10GenericDataMigrationDAO.getTableLabels(request);

        } catch (Exception e) {
        }
        return resultObj;
    }
    
     public JSONObject fetchYoutubeApiDataDirectCall(HttpServletRequest request) {
        String result = "";
       JSONObject resultObj = new JSONObject();
        try {
            
                   String tableName = request.getParameter("tableName");
                   String url = request.getParameter("url");
                   String start_date = request.getParameter("start_date");
                   String end_date = request.getParameter("end_date");
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
                    MultiValueMap<String, Object> inputMap = new LinkedMultiValueMap<>();
                   
                    String connObjStr = (String)request.getParameter("connObj");
                     JSONObject connObj  =  (JSONObject)JSONValue.parse(connObjStr);
                     String USER_NAME = (String)connObj.get("CONN_USER_NAME");
                     String PASSWORD = (String)connObj.get("CONN_PASSWORD");
                     String HOST = (String)connObj.get("HOST_NAME");
                     String PORT = (String)connObj.get("CONN_PORT");
                     String SERVICE_NAME = (String)connObj.get("CONN_DB_NAME");
                     
	            
	             inputMap.add("USER_NAME", USER_NAME);
	             inputMap.add("PASSWORD", PASSWORD);
	             inputMap.add("HOST", HOST);
	             inputMap.add("PORT", PORT);
	             inputMap.add("SERVICE_NAME", SERVICE_NAME);
                    
                    inputMap.add("url", url);
                    inputMap.add("start_date", start_date);
                    inputMap.add("end_date", end_date);
                    inputMap.add("TableName", tableName);
                    
                    HttpEntity<MultiValueMap<String, Object>> entity = new HttpEntity<>(inputMap, headers);
                    RestTemplate template = new RestTemplate();
                    System.out.println("Request sent ::::: ");
                    long startTime = System.currentTimeMillis();
                    System.out.println("API called :: " + System.currentTimeMillis());
                    ResponseEntity<String> responseObject = template.postForEntity("http://apihub.pilogcloud.com:6671/youtube/", entity, String.class);
                    String response = responseObject.getBody();
                    System.out.println("Response time :: " + (System.currentTimeMillis() - startTime) / 1000 + " Sec");
                    resultObj.put("result",response);
          
          
        } catch (Exception e) {
            e.printStackTrace();
            resultObj.put("result",e.getMessage());
        } 
        return resultObj;
    }
     
      public JSONObject fetchYoutubeApiData(HttpServletRequest request) {
          JSONObject resultObj = new JSONObject();
        String result = "";
        final int numberOfThreads = 1;
        final ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        final List<Future<?>> futures = new ArrayList<>();
        try {
            String responseId = AuditIdGenerator.genRandom32Hex();
            executor.submit(() -> {
                try {
                    String threadName = Thread.currentThread().getName();
                    System.out.println("Hello " + threadName);
                    String tableName = request.getParameter("tableName");
                   String url = request.getParameter("url");
                   String start_date = request.getParameter("start_date");
                   String end_date = request.getParameter("end_date");
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
                    MultiValueMap<String, Object> inputMap = new LinkedMultiValueMap<>();
                   
                    String connObjStr = (String)request.getParameter("connObj");
                     JSONObject connObj  =  (JSONObject)JSONValue.parse(connObjStr);
                     String USER_NAME = (String)connObj.get("CONN_USER_NAME");
                     String PASSWORD = (String)connObj.get("CONN_PASSWORD");
                     String HOST = (String)connObj.get("HOST_NAME");
                     String PORT = (String)connObj.get("CONN_PORT");
                     String SERVICE_NAME = (String)connObj.get("CONN_DB_NAME");
                     
	            
	             inputMap.add("response_id", responseId);
                     
	             inputMap.add("USER_NAME", USER_NAME);
	             inputMap.add("PASSWORD", PASSWORD);
	             inputMap.add("HOST", HOST);
	             inputMap.add("PORT", PORT);
	             inputMap.add("SERVICE_NAME", SERVICE_NAME);
                    
                    inputMap.add("url", url);
                    inputMap.add("start_date", start_date);
                    inputMap.add("end_date", end_date);
                    inputMap.add("TableName", tableName);
                    
                    HttpEntity<MultiValueMap<String, Object>> entity = new HttpEntity<>(inputMap, headers);
                    RestTemplate template = new RestTemplate();
                    System.out.println("Request sent ::::: ");
                    System.out.println("response_id  ::::: "+responseId);
                    long startTime = System.currentTimeMillis();
                    System.out.println("API called :: " + System.currentTimeMillis());
                    ResponseEntity<String> responseObject = template.postForEntity("http://apihub.pilogcloud.com:6671/youtube/", entity, String.class);
                    String response = responseObject.getBody();
                    System.out.println("Response time :: " + (System.currentTimeMillis() - startTime) / 1000 + " Sec");
                    
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
//                      Thread.sleep(10000);
                        System.out.println("attempt to shutdown executor");
                        executor.shutdown();
                        executor.awaitTermination(5, TimeUnit.SECONDS);
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
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                System.out.println("attempt to shutdown executor");
                executor.shutdown();
                executor.awaitTermination(5, TimeUnit.SECONDS);
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
        resultObj.put("result",result);
        return resultObj;
      }
    public JSONObject fetchApiDataFromETLOS(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        String result = "";
        final int numberOfThreads = 1;
        final ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        final List<Future<?>> futures = new ArrayList<>();
        try {
            String responseId = AuditIdGenerator.genRandom32Hex();
            executor.submit(() -> {
            try { 
            	
            	String threadName = Thread.currentThread().getName();
                String apiUrlKey=request.getParameter("apiUrlKey");
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
                MultiValueMap<String, Object> inputMap = new LinkedMultiValueMap<>();

                String data =request.getParameter("formData");
                JSONObject formData=(JSONObject) JSONValue.parse(data);
                Set<String> keys = formData.keySet();

                for (String key : keys) {
                    Object value = formData.get(key);
                    inputMap.add(key, value);
                }

                String url=(String) request.getSession(false).getAttribute(apiUrlKey);
                inputMap.add("response_id",responseId);
                HttpEntity<MultiValueMap<String, Object>> entity = new HttpEntity<>(inputMap, headers);
                RestTemplate template = new RestTemplate();
                System.out.println("Request sent ::::: ");
                System.out.println("response_id  ::::: "+responseId);
                long startTime = System.currentTimeMillis();
                System.out.println("API called :: " + System.currentTimeMillis());
                ResponseEntity<String> responseObject = template.postForEntity(url, entity, String.class);
                String response = responseObject.getBody();
                System.out.println("Response time :: " + (System.currentTimeMillis() - startTime) / 1000 + " Sec");

            } catch (Exception e) {
                e.printStackTrace();
                } finally {
                    try {
//                      Thread.sleep(10000);
                        System.out.println("attempt to shutdown executor");
                        executor.shutdown();
                        executor.awaitTermination(5, TimeUnit.SECONDS);
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
            resultObj.put("result",result);
            resultObj.put("responseId",responseId);
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            try {
                System.out.println("attempt to shutdown executor");
                executor.shutdown();
                executor.awaitTermination(5, TimeUnit.SECONDS);
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


    public JSONObject getOnlineServicesHtml(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();

        try {

            resultObj = v10GenericDataMigrationDAO.getOnlineServicesHtml(request);

        } catch (Exception e) {
        }
        return resultObj;
    }
}
