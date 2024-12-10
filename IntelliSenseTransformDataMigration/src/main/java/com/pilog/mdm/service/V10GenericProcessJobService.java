/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.service;

import com.opencsv.CSVReader;
//import com.pilog.mdm.DAO.iVisioniTransformAccessDAO;
import com.pilog.mdm.access.DataAccess;
import com.pilog.mdm.transformaccess.V10MigrationDataAccess;
import com.pilog.mdm.transformutills.V10DataPipingUtills;
import com.pilog.mdm.utilities.PilogUtilities;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.json.simple.JSONValue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;


import com.sap.mw.jco.JCO;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.hibernate.SessionFactory;
import org.hibernate.Session;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author sanya
 */
@Service
public class V10GenericProcessJobService {

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
    private V10GenericProcessETLDataService genericProcessETLDataService;
    
    
    @Value("${jdbc.driver}")
    private String dataBaseDriver;
    @Value("${jdbc.username}")
    private String userName;
    @Value("${jdbc.password}")
    private String password;
    @Value("${jdbc.url}")
    private String dbURL;
    
    private String etlFilePath;
	{
		if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")) {
			etlFilePath = "C://";
		} else {
			etlFilePath = "/u01/";
		}
	}
	
//    private iVisioniTransformAccessDAO dataMigrationAccessDAO = new iVisioniTransformAccessDAO();
    private Map<String, Thread> asyncJobMap = new HashMap<>();

    @Autowired
    private DataAccess access;
    PilogUtilities visionUtills = new PilogUtilities();

    @Autowired
    private V10MigrationDataAccess iTransformAccess;

    @Async
    public JSONObject processJobData(String loginUserName,
            String loginOrgnId,
            Map mappedData,
            Map mappingOperatorObj,
            String jobId
    ) {
        JSONObject resultObj = new JSONObject();
        try {
            asyncJobMap.put(jobId + loginUserName + loginOrgnId, Thread.currentThread());
//             resultObj = processJobSchedularData(loginUserName,
//                    loginOrgnId,
//                    mappedData,
//                    mappingOperatorObj,
//                    jobId);
        } catch (Exception e) {
        } finally {
            try {
                if (asyncJobMap != null && !asyncJobMap.isEmpty()) {
                    asyncJobMap.remove(jobId + loginUserName + loginOrgnId);
                }
            } catch (Exception e) {
            }
        }
        return resultObj;
    }

    @Async
    public JSONObject processJobData(String loginUserName,
            String loginOrgnId,
            String jobId,
            String mappedDataStr,
            String currentTrnsOpId
    ) {
        JSONObject resultObj = new JSONObject();
        try {
            asyncJobMap.put(jobId + loginUserName + loginOrgnId, Thread.currentThread());

            if (mappedDataStr != null && !"".equalsIgnoreCase(mappedDataStr)) {

                Map mappedData = (Map) JSONValue.parse(mappedDataStr);
                Map mappingOperatorObj = new HashMap();
                Map nonMapOperatorObj = new HashMap();
                if (mappedData != null && !mappedData.isEmpty()) {
                    Map operatorsMap = (Map) mappedData.get("operators");
                    if (operatorsMap != null && !operatorsMap.isEmpty()) {
                        mappingOperatorObj = (Map) operatorsMap.keySet().stream()
                                .filter(keyName -> (keyName != null && (Map) operatorsMap.get(keyName) != null
                                && ("MAP".equalsIgnoreCase(String.valueOf(((Map) operatorsMap.get(keyName)).get("iconType")))
                                || "GROUP".equalsIgnoreCase(String.valueOf(((Map) operatorsMap.get(keyName)).get("iconType")))
                                || "UNGROUP".equalsIgnoreCase(String.valueOf(((Map) operatorsMap.get(keyName)).get("iconType"))))))
                                .collect(Collectors.toMap(keyName -> keyName, keyName -> (Map) operatorsMap.get(keyName)));
                        if (mappingOperatorObj != null && !mappingOperatorObj.isEmpty()) {
//                            resultObj = processJobSchedularData(loginUserName,
//                                    loginOrgnId,
//                                    mappedData,
//                                    mappingOperatorObj,
//                                    jobId, mappedDataStr);
                            // ravi hibernate code
                            resultObj = processJobSchedularDataHib(loginUserName,
                                    loginOrgnId,
                                    mappedData,
                                    mappingOperatorObj,
                                    jobId, mappedDataStr);

                        }
                    }
                }
            }
        } catch (Exception e) {
        } finally {
            try {
                if (asyncJobMap != null && !asyncJobMap.isEmpty()) {
                    asyncJobMap.remove(jobId + loginUserName + loginOrgnId);
                }
            } catch (Exception e) {
            }
        }
        return resultObj;
    }

    public JSONObject processJobSchedularData(String loginUserName,
            String loginOrgnId,
            Map mappedData,
            Map mappingOperatorObj,
            String jobId,
            String mappedDataStr
    ) {
        JSONObject resultObj = new JSONObject();
        Connection fromConnection = null;
        Connection toConnection = null;
        JCO.Client fromJCOConnection = null;
        JCO.Client toJCOConnection = null;
        try {
            if (mappingOperatorObj != null && !mappingOperatorObj.isEmpty()) {
                Map operatorsMap = (Map) mappedData.get("operators");
                Map linksMap = (Map) mappedData.get("links");
                for (Object mappedKey : mappingOperatorObj.keySet()) {
                    if (linksMap != null && !linksMap.isEmpty()) {
                        List<Map> fromOperatorList = new ArrayList<>();
                        List<Map> toOperatorList = new ArrayList<>();
                        for (Object linkKey : linksMap.keySet()) {
                            Map linkMap = (Map) linksMap.get(linkKey);
                            if (linkMap != null
                                    && !linkMap.isEmpty()
                                    && String.valueOf(mappedKey)
                                            .equalsIgnoreCase(String.valueOf(linkMap.get("toOperator")))) {
//                                System.out.println("linkMap.get(\"fromOperator\")::" + linkMap.get("fromOperator"));
                                String fromOperatorId = String.valueOf(linkMap.get("fromOperator"));
                                Map fromOperator = (Map) operatorsMap.get(String.valueOf(linkMap.get("fromOperator")));

//                              fromOperator.put("operatorId", fromOperatorId);
                                fromOperatorList.add(fromOperator);

                            }
                            if (linkMap != null
                                    && !linkMap.isEmpty()
                                    && String.valueOf(mappedKey)
                                            .equalsIgnoreCase(String.valueOf(linkMap.get("fromOperator")))) {
                                String toOperatorId = String.valueOf(linkMap.get("toOperator"));
                                Map toOperator = (Map) operatorsMap.get(String.valueOf(linkMap.get("toOperator")));
//                                toOperator.put("operatorId", toOperatorId);
                                toOperatorList.add(toOperator);

                            }
                        }// end loop linksMap

                        if (fromOperatorList != null
                                && !fromOperatorList.isEmpty()
                                && toOperatorList != null
                                && !toOperatorList.isEmpty()) {

                            // ravi bapi code start
                            String jobType = (String) ((JSONObject) mappingOperatorObj.get(mappedKey)).get("jobType");
                            String bapiName = (String) ((JSONObject) mappingOperatorObj.get(mappedKey)).get("bapiName");
                            if (jobType != null && !"".equalsIgnoreCase(jobType) && "Bapi".equalsIgnoreCase(jobType)) {
                            
                            } // ravi bapi code end
                            else {
                                for (int i = 0; i < toOperatorList.size(); i++) {
                                    Map toOperator = toOperatorList.get(i);
                                    if (toOperator != null && !toOperator.isEmpty()) {
                                        Map trfmRulesDataMap = (Map) toOperator.get("trfmRules-data");
                                        if (trfmRulesDataMap != null && !trfmRulesDataMap.isEmpty()) {
                                            if (i != 0) {
                                                fromOperatorList = new V10DataPipingUtills().getFromOperators((Map) JSONValue.parse(mappedDataStr), mappedKey);
                                            }

                                            try {
                                                Map transformationRulesMap = convertTransFrmRulsMapToParam(trfmRulesDataMap);
                                                if (transformationRulesMap != null
                                                        && !transformationRulesMap.isEmpty()) {
                                                    boolean isSameDataBase = fromOperatorList.stream()
                                                            .filter(fromOpMap -> (fromOpMap.containsKey("CONNECTION_NAME")
                                                            && fromOpMap.containsKey("CONN_DB_NAME")
                                                            && !"file".equalsIgnoreCase(String.valueOf(fromOpMap.get("dragType")))))
                                                            .map(fromOpMap -> (fromOpMap.get("CONNECTION_NAME") + ":::" + fromOpMap.get("CONN_DB_NAME")))
                                                            .distinct().count() == 1;
                                                    boolean iscontainsFile = fromOperatorList.stream()
                                                            .anyMatch(fromOpMap -> "file".equalsIgnoreCase(String.valueOf(fromOpMap.get("dragType"))));
                                                    if (isSameDataBase && !iscontainsFile) {// CASE 1 --> if all from tables having same data base
                                                        Map fromOperator = fromOperatorList.get(0);
                                                        if (fromOperator != null && !fromOperator.isEmpty()) {
                                                            JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");
                                                            String fromTableName = (String) fromOperator.get("tableName");
                                                            JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
                                                            String toTableName = (String) toOperator.get("tableName");
                                                            String toIconType = (String) toOperator.get("iconType");
                                                            Object fromConnectionObj = getConnection(fromConnObj);
                                                            if (fromConnectionObj instanceof Connection) {
                                                                fromConnection = (Connection) fromConnectionObj;
                                                                try {
                                                                    genericProcessETLDataService.processETLLog(loginUserName,
                                                                            loginOrgnId,
                                                                            "Source system successfully connected.", "INFO", 20, "Y", jobId);
                                                                } catch (Exception e) {
                                                                }
                                                                resultObj = processJob(loginUserName,
                                                                        loginOrgnId,
                                                                        toOperator,
                                                                        transformationRulesMap,
                                                                        fromConnection,
                                                                        fromOperatorList,
                                                                        jobId);
                                                            } else if (fromConnectionObj instanceof JCO.Client) {
                                                                fromJCOConnection = (JCO.Client) fromConnectionObj;
                                                                // for SAP
                                                                try {
                                                                    genericProcessETLDataService.processETLLog(loginUserName,
                                                                            loginOrgnId,
                                                                            "Source system successfully connected.", "INFO", 20, "Y", jobId);
                                                                } catch (Exception e) {
                                                                }
                                                                resultObj = processJob(loginUserName,
                                                                        loginOrgnId,
                                                                        toOperator,
                                                                        transformationRulesMap,
                                                                        fromJCOConnection,
                                                                        fromOperatorList,
                                                                        jobId);
                                                            } else {
                                                                try {
                                                                    genericProcessETLDataService.processETLLog(loginUserName,
                                                                            loginOrgnId,
                                                                            "Unable to connect source system due to " + fromConnectionObj, "ERROR", 20, "N", jobId);
                                                                } catch (Exception e) {
                                                                }
                                                                resultObj.put("Message", fromConnectionObj);
                                                                resultObj.put("connectionFlag", "N");
                                                            }
                                                        }
                                                    } else {
                                                        boolean isDataBase = fromOperatorList.stream().allMatch(fromOpMap
                                                                -> (fromOpMap.containsKey("CONNECTION_NAME")
                                                                && !"file".equalsIgnoreCase(String.valueOf(fromOpMap.get("dragType")))
                                                                && fromOpMap.containsKey("CONN_DB_NAME")));
                                                        if (isDataBase) {// CASE 2 -->  if all from tables having different DB/ERP.
                                                            resultObj = processJobWithDiffSchema(loginUserName,
                                                                    loginOrgnId,
                                                                    toOperator,
                                                                    transformationRulesMap,
                                                                    fromOperatorList,
                                                                    jobId);

                                                        } else {
                                                            //fileName
                                                            boolean isFile = fromOperatorList.stream().allMatch(fromOpMap
                                                                    -> (fromOpMap.containsKey("dragType")
                                                                    && "file".equalsIgnoreCase(String.valueOf(fromOpMap.get("dragType")))));
                                                            if (isFile) {// CASE 3 -->if all from objects having files.
                                                                resultObj = processJob(loginUserName,
                                                                        loginOrgnId,
                                                                        toOperator,
                                                                        transformationRulesMap,
                                                                        fromOperatorList,
                                                                        jobId);
                                                            } else {// CASE 4 -->if all from objects having files and DB/ERP Objects
                                                                //processJobWithFilesAndTables
                                                                resultObj = genericProcessETLDataService.processJobWithFilesAndTables(loginUserName,
                                                                        loginOrgnId,
                                                                        toOperator,
                                                                        transformationRulesMap,
                                                                        fromOperatorList,
                                                                        jobId);
                                                            }

                                                        }
                                                    }
                                                }
                                            } catch (Exception e) {
                                                e.printStackTrace();
                                                continue;
                                            }

                                        }
                                    }

                                    // if any operators connected with To Operator
                                    Map<String, List<Map>> toOpeartorMapObj = getFromAndToOperatorList(linksMap,
                                            operatorsMap,
                                            toOperator.get("operatorId"));
                                    if (toOpeartorMapObj != null && !toOpeartorMapObj.isEmpty()) {
                                        processNestedJobData(toOpeartorMapObj,
                                                mappedData,
                                                jobId,
                                                loginUserName,
                                                loginOrgnId);

                                    }

                                }
                            }
                        }
                    }// end if for linksMap
                }// end loop for mappingOperatorObj
            }
            try {
                genericProcessETLDataService.processETLLog(loginUserName,
                        loginOrgnId, "ETL Process is completed", "INFO", 10, "N", jobId);
            } catch (Exception e) {
            }
        } catch (Exception e) {
            e.printStackTrace();
            resultObj.put("Message", e.getMessage());
            resultObj.put("connectionFlag", "N");
            try {
                genericProcessETLDataService.processETLLog(loginUserName,
                        loginOrgnId, e.getMessage(), "ERROR", 20, "N", jobId);
            } catch (Exception ex) {
            }
        } finally {

            try {
                if (fromConnection != null) {
                    fromConnection.close();
                }
                if (toConnection != null) {
                    toConnection.close();
                }
                if (toJCOConnection != null) {
                    toJCOConnection.disconnect();
                }
                if (fromJCOConnection != null) {
                    fromJCOConnection.disconnect();
                }
            } catch (Exception e) {
            }
        }
        return resultObj;
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
                }// orderByDataMapList end
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
                    if (groupByColumns != null
                            && !"".equalsIgnoreCase(groupByColumns)
                            && !"null".equalsIgnoreCase(groupByColumns)) {
                        groupByColumns = new PilogUtilities().trimChar(groupByColumns, ',');
                    }
                    paramMap.put("groupBy", groupByColumns);
                }//groupByDataMapList end
                List<String> joinClauseDataMapList = (List) trfmRulesDataMap.get("joinClauseData");
                List<String> childTables = (List) trfmRulesDataMap.get("childTables");
                if (childTables != null && !childTables.isEmpty()
                        && joinClauseDataMapList != null && !joinClauseDataMapList.isEmpty()) {
                    JSONObject joinQueryMapObj = new JSONObject();
                    LinkedHashMap joinQueryHashMapObj = new LinkedHashMap();
                    String joinQuery = "";
                    String masterTableName = (String) trfmRulesDataMap.get("masterTableName");
                    joinQuery += " " + masterTableName;
                    joinQueryMapObj.put(masterTableName, masterTableName);
                    joinQueryHashMapObj.put(masterTableName, masterTableName);
                    for (int i = 0; i < childTables.size(); i++) {
                        String childTableName = childTables.get(i);
                        String childJoinStr = joinClauseDataMapList.get(i);
                        if (childJoinStr != null
                                && !"".equalsIgnoreCase(childJoinStr)
                                && !"null".equalsIgnoreCase(childJoinStr)) {
                            JSONObject joinObj = (JSONObject) JSONValue.parse(childJoinStr);
                            if (joinObj != null && !joinObj.isEmpty()) {
                                joinQueryMapObj.put(childTableName, joinObj);
                                joinQueryHashMapObj.put(childTableName, joinObj);
                                int j = 0;
                                for (Object joinObjKey : joinObj.keySet()) {
                                    JSONObject joinMappedColumnObj = (JSONObject) joinObj.get(joinObjKey);
                                    if (joinMappedColumnObj != null
                                            && !joinMappedColumnObj.isEmpty()) {
                                        String childTableColumn = "";
                                        if (joinMappedColumnObj.get("childTableColumn") != null
                                                && !"".equalsIgnoreCase(String.valueOf(joinMappedColumnObj.get("childTableColumn")))
                                                && !"null".equalsIgnoreCase(String.valueOf(joinMappedColumnObj.get("childTableColumn")))) {//childTableColumn
                                            childTableColumn = String.valueOf(joinMappedColumnObj.get("childTableColumn")).replace(":", ".");
                                        }
                                        String masterTableColumn = "";
                                        if (joinMappedColumnObj.get("masterTableColumn") != null
                                                && !"".equalsIgnoreCase(String.valueOf(joinMappedColumnObj.get("masterTableColumn")))
                                                && !"null".equalsIgnoreCase(String.valueOf(joinMappedColumnObj.get("masterTableColumn")))) {//childTableColumn
                                            masterTableColumn = String.valueOf(joinMappedColumnObj.get("masterTableColumn")).replace(":", ".");
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
                    paramMap.put("joinQueryHashMapObj", joinQueryHashMapObj);
                    paramMap.put("joinQuery", joinQuery);
                }// join Query Map Obj End
                JSONObject appendValObj = new JSONObject();
//                JSONObject colsObj = new JSONObject(); 
                Map colsObj = new LinkedHashMap(); // ravi etl integration

                JSONObject defaultValObj = new JSONObject();
                JSONObject columnClauseObj = new JSONObject();

                Map totalColumnsObj = new LinkedHashMap(); // ravi etl integration
                Map defaultValIndxObj = new LinkedHashMap(); // ravi etl integration
                Map colClauseIndxObj = new LinkedHashMap(); // ravi etl integration

                List<Map> colMappingsDataMapList = (List) trfmRulesDataMap.get("colMappingsData");

                if (colMappingsDataMapList != null && !colMappingsDataMapList.isEmpty()) {

                    //  for (Map colMappingsDataMap : colMappingsDataMapList) {
                    for (int i = 0; i < colMappingsDataMapList.size(); i++) {
                        Map colMappingsDataMap = colMappingsDataMapList.get(i);
                        if (colMappingsDataMap != null && !colMappingsDataMap.isEmpty()) {
                            String toTableColumnName = (String) colMappingsDataMap.get("destinationColumn");
                            String fromTableColumn = (String) colMappingsDataMap.get("sourceColumn");
                            Object columnClause = colMappingsDataMap.get("columnClause");
                            String defaultValue = (String) colMappingsDataMap.get("defaultValue");
                            String appendValue = (String) colMappingsDataMap.get("appendValue");
                            if (toTableColumnName != null
                                    && !"".equalsIgnoreCase(toTableColumnName)
                                    && !"null".equalsIgnoreCase(toTableColumnName) //  && !toTableColumnName.contains("N/A")
                                    ) {
                                if (fromTableColumn != null
                                        && !"".equalsIgnoreCase(fromTableColumn)
                                        && !"null".equalsIgnoreCase(fromTableColumn)
                                        && !fromTableColumn.contains("N/A")) {

                                    colsObj.put((toTableColumnName.contains(":") ? toTableColumnName.split(":")[1] : toTableColumnName), fromTableColumn);
                                    totalColumnsObj.put((toTableColumnName.contains(":") ? toTableColumnName.split(":")[1] : toTableColumnName), fromTableColumn);

                                    if (appendValue != null
                                            && !"".equalsIgnoreCase(appendValue)
                                            && !"null".equalsIgnoreCase(appendValue)) {
                                        appendValObj.put(fromTableColumn, appendValue);
                                    }
                                } else { // ravi HIBERNATE MODIFICATION

                                    totalColumnsObj.put((toTableColumnName.contains(":") ? toTableColumnName.split(":")[1] : toTableColumnName), "FROMCOL_NA_" + i);

                                }
                                if (columnClause != null
                                        && !"".equalsIgnoreCase(String.valueOf(columnClause))
                                        && !"null".equalsIgnoreCase(String.valueOf(columnClause))) {
                                    columnClauseObj.put((toTableColumnName.contains(":") ? toTableColumnName.split(":")[1] : toTableColumnName), columnClause);
                                    colClauseIndxObj.put("FROMCOL_NA_" + i, columnClause);
                                }

                            } else if (toTableColumnName != null
                                    && !"".equalsIgnoreCase(toTableColumnName)
                                    && !"null".equalsIgnoreCase(toTableColumnName)
                                    && toTableColumnName.contains("N/A")) {

                                colsObj.put(fromTableColumn, fromTableColumn);
                            }
                            if (defaultValue != null
                                    && !"".equalsIgnoreCase(defaultValue)
                                    && !"null".equalsIgnoreCase(defaultValue)) {
                                if (toTableColumnName != null
                                        && !"".equalsIgnoreCase(toTableColumnName)
                                        && !"null".equalsIgnoreCase(toTableColumnName) //  && !toTableColumnName.contains("N/A")
                                        ) {
                                    defaultValObj.put((toTableColumnName.contains(":") ? toTableColumnName.split(":")[1] : toTableColumnName), defaultValue);

                                    defaultValIndxObj.put("FROMCOL_NA_" + i, defaultValue);

                                } else if (fromTableColumn != null
                                        && !"".equalsIgnoreCase(fromTableColumn)
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

                    paramMap.put("totalColumnsObj", totalColumnsObj);
                    paramMap.put("defaultValIndxObj", defaultValIndxObj);
                    paramMap.put("colClauseIndxObj", colClauseIndxObj);
                }//
                List<String> whereClauseDataList = (List) trfmRulesDataMap.get("whereClauseData");
                JSONObject whereClauseObject = new JSONObject();
                JSONObject whereClauseDataObject = new JSONObject();
                if (whereClauseDataList != null && !whereClauseDataList.isEmpty()) {
                    for (int i = 0; i < whereClauseDataList.size(); i++) {
                        String whereClauseDataStr = whereClauseDataList.get(i);
                        if (whereClauseDataStr != null
                                && !"".equalsIgnoreCase(whereClauseDataStr)
                                && !"null".equalsIgnoreCase(whereClauseDataStr)) {
                            JSONObject whereClauseData = (JSONObject) JSONValue.parse(whereClauseDataStr);
                            if (whereClauseData != null && !whereClauseData.isEmpty()) {
                                int j = 0;
                                String tableName = "";
                                String whereCluaseQuery = "";
                                Map whereClauseDataMap = new HashMap();
                                for (Object whereClauseObjKey : whereClauseData.keySet()) {
                                    JSONObject whereClauseObj = (JSONObject) whereClauseData.get(whereClauseObjKey);
                                    if (whereClauseObj != null && !whereClauseObj.isEmpty()) {
                                        String columnName = (String) whereClauseObj.get("columnName");
                                        String operator = (String) whereClauseObj.get("operator");
                                        String andOrOperator = (String) whereClauseObj.get("andOrOperator");
                                        String staticValue = (String) whereClauseObj.get("staticValue");
                                        if (columnName != null
                                                && !"".equalsIgnoreCase(columnName)
                                                && !"null".equalsIgnoreCase(columnName)) {
                                            tableName = columnName.split(":")[0];
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
                                        }
                                        whereClauseDataMap.put(j, whereClauseObj);

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
                                    whereClauseDataObject.put(tableName, whereClauseDataMap);
                                }

                            }
                        }

                    }
                    paramMap.put("whereClauseObj", whereClauseObject);
                    paramMap.put("whereClauseDataObject", whereClauseDataObject);
                }//whereClauseDataList End

                JSONObject selectTabObj = (JSONObject) trfmRulesDataMap.get("selectTabObj");
                if (!(selectTabObj != null && !selectTabObj.isEmpty())) {
                    selectTabObj = new JSONObject();
                }
                selectTabObj.put("columnClauseObj", columnClauseObj);
                selectTabObj.put("appendValObj", appendValObj);

                String nativeSQL = (String) trfmRulesDataMap.get("nativeSQL");
                if (nativeSQL != null
                        && !"".equalsIgnoreCase(nativeSQL)
                        && !"null".equalsIgnoreCase(nativeSQL)) {
                    paramMap.put("nativeSQL", nativeSQL);
                }// end Native SQL Query                
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
                    Class<?>[] paramTypes = {String.class, String.class, String.class, String.class, String.class, String.class, String.class};
                    Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
                    Object targetObj = new PilogUtilities().createObjectByClass(clazz);
                    returnedObj = method.invoke(targetObj, String.valueOf(dbObj.get("CONN_PORT")),
                            String.valueOf(dbObj.get("CONN_USER_NAME")),
                            String.valueOf(dbObj.get("CONN_PASSWORD")),
                            "EN",
                            String.valueOf(dbObj.get("HOST_NAME")),
                            String.valueOf(dbObj.get("CONN_DB_NAME")),
                            String.valueOf(dbObj.get("GROUP")));

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

    // for executing job if the sources(from) Data Base tables
    public JSONObject processJob(String loginUserName,
            String loginOrgnId,
            Map toOperator,
            Map transformationMap,
            Connection fromConnection,
            List<Map> fromOperatorList,
            String jobId
    ) {
        JSONObject resultObj = new JSONObject();
        Connection toConnection = null;
        JCO.Client toJCOConnection = null;
        PreparedStatement toPreparedStatement = null;
        try {
            if (transformationMap != null) {
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
                if (!(selectTabObj != null && !selectTabObj.isEmpty())) {
                    selectTabObj = new JSONObject();
                }
                selectTabObj.put("jobId", jobId);
                if (columnsObj != null && !columnsObj.isEmpty()) {
                    try {
                        genericProcessETLDataService.processETLLog(loginUserName,
                                loginOrgnId,
                                "Reading the transformations rules.",
                                "INFO", 10, "Y", jobId);
                    } catch (Exception e) {
                    }
                    //JSONObject fromColumnsObj = new JSONObject();
                    Map fromColumnsObj = new LinkedHashMap();// ravi etl integration
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
                            selectTabObj.put("toConnectionObj", toConnectionObj);
                            if (toConnectionObj instanceof Connection) {
                                toConnection = (Connection) toConnectionObj;
                            }
                            if (toConnection != null) {
                                String toTableInsertQuery = genericProcessETLDataService.generateInsertQuery((String) toOperator.get("tableName"),
                                        toColumnsList);
                                System.out.println("insertQuery::::" + toTableInsertQuery);
                                toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
                                Map columnsTypeObj = genericProcessETLDataService.getColumnsType((String) toOperator.get("tableName"),
                                        toColumnsList, toConnection);
                                selectTabObj.put("columnsTypeObj", columnsTypeObj);
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
                            genericProcessETLDataService.processETLLog(loginUserName,
                                    loginOrgnId, "Starting extract join tables data.", "INFO", 10, "Y", jobId);
                        } catch (Exception e) {
                        }
                        totalDataCount += genericProcessETLDataService.processETLData(loginUserName,
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
                                genericProcessETLDataService.processETLLog(loginUserName,
                                        loginOrgnId, "Starting extract non-join tables data.", "INFO", 10, "Y", jobId);
                            } catch (Exception e) {
                            }
                            for (int i = 0; i < nonJoinOpList.size(); i++) {
                                Map fromOperator = nonJoinOpList.get(i);
                                if (fromOperator != null && !fromOperator.isEmpty()) {
                                    totalDataCount += genericProcessETLDataService.processETLData(loginUserName,
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
                                            10000,
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

                        try {
                            genericProcessETLDataService.processETLLog(loginUserName,
                                    loginOrgnId, message, "INFO", 10, "Y", jobId);
                        } catch (Exception e) {
                        }
                        resultObj.put("Message", message);
                        resultObj.put("connectionFlag", "Y");

                    } else {
                        try {
                            genericProcessETLDataService.processETLLog(loginUserName,
                                    loginOrgnId, totalDataCount + " Row(s) successfully extracted and loaded into target system.", "INFO", 10, "Y", jobId);
                        } catch (Exception e) {
                        }

                        resultObj.put("Message", totalDataCount + " Row(s) successfully extracted and loaded into target system.");
                        resultObj.put("connectionFlag", "Y");
                    }
                }
            }//end if transformationMap
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
                if (toJCOConnection != null) {
                    toJCOConnection.disconnect();
                }
            } catch (Exception e) {
            }
        }
        return resultObj;
    }

    // for executing job if the sources(from) are ERP Tables 
    public JSONObject processJob(String loginUserName,
            String loginOrgnId,
            Map toOperator,
            Map transformationMap,
            JCO.Client fromJCOConnection,
            List<Map> fromOperatorList,
            String jobId
    ) {
        JSONObject resultObj = new JSONObject();
        Connection fromConnection = null;
        JCO.Client toJCOConnection = null;
        Connection toConnection = null;
        PreparedStatement toPreparedStatement = null;
        PreparedStatement fromPreparedStatement = null;
        try {
            if (transformationMap != null) {
                //JSONObject columnsObj = (JSONObject) transformationMap.get("columnsObj");
                Map columnsObj = (LinkedHashMap) transformationMap.get("columnsObj"); // ravi etl integration
                JSONObject tablesWhereClauseObj = (JSONObject) transformationMap.get("whereClauseObj");//
//                JSONObject joinQueryMapObj = (JSONObject) transformationMap.get("joinQueryMapObj");//
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
                        genericProcessETLDataService.processETLLog(loginUserName,
                                loginOrgnId,
                                "Reading the transformations rules.",
                                "INFO", 10, "Y", jobId);
                    } catch (Exception e) {
                    }
                    //JSONObject fromColumnsObj = new JSONObject();
                    Map fromColumnsObj = new LinkedHashMap();// ravi etl integration
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
                            selectTabObj.put("toConnectionObj", toConnectionObj);
                            if (toConnectionObj instanceof Connection) {
                                toConnection = (Connection) toConnectionObj;
                            }
                            if (toConnection != null) {
                                String toTableInsertQuery = genericProcessETLDataService.generateInsertQuery((String) toOperator.get("tableName"), toColumnsList);
                                System.out.println("insertQuery::::" + toTableInsertQuery);
                                toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
                                Map columnsTypeObj = genericProcessETLDataService.getColumnsType((String) toOperator.get("tableName"), toColumnsList, toConnection);
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
                                    Map fromOpearatorMap = genericProcessETLDataService.getSelectedFileTablesData(fromOpearator,
                                            toOperator,
                                            fromColumnsObj,
                                            transformationMap,
                                            fileDataLastIndex,
                                            fileName);
                                    if (fromOpearatorMap != null && !fromOpearatorMap.isEmpty()) {
                                        fromOperatorMapList.add(fromOpearatorMap);
                                        fromFilesList.add(fromOpearatorMap.get("oldTableName"));
                                        if (fromColumnsObj.containsKey(fromOpearatorMap.get("oldTableName"))) {
                                            transformationMap = genericProcessETLDataService.convertTransFrmRulsMapToParam(transformationMap, fromOpearatorMap);
                                            fromColumnsObj = genericProcessETLDataService.updateFromColsObj(fromOpearatorMap, fromColumnsObj);

                                        }
                                    }

                                } else {
                                    Map fromOpearatorMap = genericProcessETLDataService.getSelectedDBTablesData(fromOpearator,
                                            toOperator,
                                            fromColumnsObj,
                                            transformationMap);
                                    if (fromOpearatorMap != null && !fromOpearatorMap.isEmpty()) {
                                        fromOperatorMapList.add(fromOpearatorMap);
                                        if (fromColumnsObj.containsKey(fromOpearatorMap.get("oldTableName"))) {
                                            transformationMap = genericProcessETLDataService.convertTransFrmRulsMapToParam(transformationMap, fromOpearatorMap);
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
                        //columnsObj = (JSONObject) transformationMap.get("columnsObj");
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
                        Map columnsTypeObj = genericProcessETLDataService.getColumnsType((String) toOperator.get("tableName"), toColumnsList, toConnection);
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

                    if (true) {

                        if (joinQueryMapObj != null
                                && !joinQueryMapObj.isEmpty()) {

                            nonJoinOpList = fromOperatorList.stream()
                                    .filter(opMap -> (opMap != null && !opMap.isEmpty()
                                    && opMap.get("tableName") != null
                                    && !"".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
                                    && !"null".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
                                    && !joinQueryMapObj.containsKey(String.valueOf(opMap.get("tableName"))))).collect(Collectors.toList());
                            try {
                                genericProcessETLDataService.processETLLog(loginUserName,
                                        loginOrgnId, "Starting extract join tables data.", "INFO", 10, "Y", jobId);
                            } catch (Exception e) {
                            }
                            totalDataCount += genericProcessETLDataService.processETLData(loginUserName,
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
//                            totalDataCount += genericProcessETLDataService.processETLData(loginUserName,
//                                    loginOrgnId,
//                                    fromConnection,
//                                    toConnection,
//                                    fromOperatorList,
//                                    toOperator,
//                                    columnsObj,
//                                    dbFromColumnsObj,
//                                    tablesWhereClauseObj,
//                                    defaultValuesObj,
//                                    toPreparedStatement,
//                                    0,
//                                    1000,
//                                    totalDataCount,
//                                    toColumnsList,
//                                    joinQueryMapObj,
//                                    joinQuery,
//                                    fileDataLastIndex,
//                                    fileName,
//                                    1,
//                                    orderAndGroupByObj,
//                                    columnClauseObj,
//                                    selectTabObj,
//                                    normalizeOptionsObj
//                            );

                        } else {
                            if (nonJoinOpList != null && !nonJoinOpList.isEmpty()) {
                                try {
                                    genericProcessETLDataService.processETLLog(loginUserName,
                                            loginOrgnId, "Starting extract non-join tables data.", "INFO", 10, "Y", jobId);
                                } catch (Exception e) {
                                }
                                for (int i = 0; i < nonJoinOpList.size(); i++) {
                                    Map fromOperator = nonJoinOpList.get(i);
                                    if (fromOperator != null && !fromOperator.isEmpty()) {
                                        totalDataCount += genericProcessETLDataService.processETLData(loginUserName,
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
                                                10000,
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
//                                        totalDataCount += genericProcessETLDataService.processETLData(loginUserName,
//                                                loginOrgnId,
//                                                fromConnection,
//                                                toConnection,
//                                                fromOperator,
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
//                                                fileDataLastIndex,
//                                                fileName,
//                                                1,
//                                                orderAndGroupByObj,
//                                                columnClauseObj,
//                                                selectTabObj,
//                                                normalizeOptionsObj
//                                        );
                                    }

                                }
                            }

                        }
                        System.out.println("totalDataCount::::" + totalDataCount);
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
                        if (totalDataCount != 0 && totalDataCount > 0) {
                            String message = totalDataCount + " Row(s) successfully extracted and loaded into target system.";
                            if (fileName != null && !"".equalsIgnoreCase(fileName) && !"null".equalsIgnoreCase(fileName)) {
                                message += " <br> <a href='#' style='color:#0071c5;' onclick=downloadExportedFile('" + fileName + "') >Click here to download the " + iconType + " file</a>.";//
                            }

                            try {
                                genericProcessETLDataService.processETLLog(loginUserName,
                                        loginOrgnId, message, "INFO", 10, "Y", jobId);
                            } catch (Exception e) {
                            }
                            resultObj.put("Message", message);
                            resultObj.put("connectionFlag", "Y");

                        } else {
                            try {
                                genericProcessETLDataService.processETLLog(loginUserName,
                                        loginOrgnId, totalDataCount + " Row(s) successfully extracted and loaded into target system.", "INFO", 10, "Y", jobId);
                            } catch (Exception e) {
                            }
//                    processETLLog(httpSession,totalDataCount + " Row(s) extracted successfully", 10);
                            resultObj.put("Message", totalDataCount + " Row(s) successfully extracted and loaded into target system.");
                            resultObj.put("connectionFlag", "Y");
                        }

                    } else {
                        try {
                            genericProcessETLDataService.processETLLog(loginUserName,
                                    loginOrgnId, totalDataCount + " Row(s) successfully extracted and loaded into target system.", "INFO", 10, "Y", jobId);
                        } catch (Exception e) {
                        }
//                    processETLLog(httpSession,totalDataCount + " Row(s) extracted successfully", 10);
                        resultObj.put("Message", totalDataCount + " Row(s) successfully extracted and loaded into target system.");
                        resultObj.put("connectionFlag", "Y");
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
                            genericProcessETLDataService.processETLLog(loginUserName,
                                    loginOrgnId, message, "INFO", 10, "Y", jobId);
                        } catch (Exception e) {
                        }
                        resultObj.put("Message", message);
                        resultObj.put("connectionFlag", "Y");

                    } else {
                        try {
                            genericProcessETLDataService.processETLLog(loginUserName,
                                    loginOrgnId, totalDataCount + " Row(s) successfully extracted and loaded into target system.", "INFO", 10, "Y", jobId);
                        } catch (Exception e) {
                        }

                        resultObj.put("Message", totalDataCount + " Row(s) successfully extracted and loaded into target system.");
                        resultObj.put("connectionFlag", "Y");
                    }
                }
            }//end if transformationMap
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (fromPreparedStatement != null) {
                    fromPreparedStatement.close();
                }
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
//    public JSONObject processJob(String loginUserName,
//            String loginOrgnId,
//            Map toOperator,
//            Map transformationMap,
//            JCO.Client fromJCOConnection,
//            List<Map> fromOperatorList,
//            String jobId
//    ) {
//        JSONObject resultObj = new JSONObject();
//        Connection fromConnection = null;
//        JCO.Client toJCOConnection = null;
//        Connection toConnection = null;
//        PreparedStatement toPreparedStatement = null;
//        try {
//            if (transformationMap != null) {
//                JSONObject columnsObj = (JSONObject) transformationMap.get("columnsObj");
//                JSONObject tablesWhereClauseObj = (JSONObject) transformationMap.get("whereClauseObj");//
//                JSONObject joinQueryMapObj = (JSONObject) transformationMap.get("joinQueryMapObj");//
//                JSONObject orderByObj = (JSONObject) transformationMap.get("orderBy");//
//                JSONObject appendValObj = (JSONObject) transformationMap.get("appendValObj");//
//                JSONObject columnClauseObj = (JSONObject) transformationMap.get("columnClauseObj");//
//                JSONObject selectTabObj = (JSONObject) transformationMap.get("selectTabObj");
//                JSONObject defaultValuesObj = (JSONObject) transformationMap.get("defaultValObj");
//                JSONObject normalizeOptionsObj = (JSONObject) transformationMap.get("normalizeOptionsObj");
//                String joinQuery = (String) transformationMap.get("joinQuery");//
//                String groupByQuery = (String) transformationMap.get("groupBy");
//                String nativeSQL = "";
//                JSONObject orderAndGroupByObj = new JSONObject();
//                orderAndGroupByObj.put("orderByObj", orderByObj);
//                orderAndGroupByObj.put("groupByQuery", groupByQuery);
//                orderAndGroupByObj.put("nativeSQL", nativeSQL);
//                if (!(selectTabObj != null && !selectTabObj.isEmpty())) {
//                    selectTabObj = new JSONObject();
//                }
//                selectTabObj.put("jobId", jobId);
//                if (columnsObj != null && !columnsObj.isEmpty()) {
//                    try {
//                        genericProcessETLDataService.processETLLog(loginUserName,
//                                loginOrgnId,
//                                "Reading the transformations rules.",
//                                "INFO", 10, "Y", jobId);
//                    } catch (Exception e) {
//                    }
//                    JSONObject fromColumnsObj = new JSONObject();
//                    JSONObject toColumnsObj = new JSONObject();
//                    for (Object toColumnName : columnsObj.keySet()) {
//                        if (toColumnName != null && columnsObj.get(toColumnName) != null) {
//                            String fromColumnStr = (String) columnsObj.get(toColumnName);
//                            if (fromColumnStr != null
//                                    && !"".equalsIgnoreCase(fromColumnStr)
//                                    && !"null".equalsIgnoreCase(fromColumnStr)) {
//                                String[] fromColumnArray = fromColumnStr.split(":");
//                                if (fromColumnArray != null && fromColumnArray.length != 0) {
//                                    if (fromColumnsObj != null && !fromColumnsObj.isEmpty()) {
//                                        if (fromColumnsObj.containsKey(fromColumnArray[0])) {
//                                            fromColumnsObj.put(fromColumnArray[0],
//                                                    (fromColumnsObj.get(fromColumnArray[0]) + "," + fromColumnArray[1]));
//                                        } else {
//                                            fromColumnsObj.put(fromColumnArray[0],
//                                                    (fromColumnArray[1]));
//                                        }
//                                    } else {
//                                        fromColumnsObj.put(fromColumnArray[0], (fromColumnArray[1]));
//                                    }
//                                }
//                            }
//
//                        }
//
//                    }
//                    Set<String> toColumns = new HashSet<>();
//                    toColumns.addAll(columnsObj.keySet());
//                    if (defaultValuesObj != null && !defaultValuesObj.isEmpty()) {
//                        Set<String> defaultColumns = defaultValuesObj.keySet();
//                        if (defaultColumns != null
//                                && !defaultColumns.isEmpty()
//                                && toColumns != null && !toColumns.isEmpty()) {
//                            toColumns.addAll(defaultColumns);
//                        }
//                    }
//                    if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
//                        Set<String> columnClauseCols = columnClauseObj.keySet();
//                        if (columnClauseCols != null
//                                && !columnClauseCols.isEmpty()
//                                && toColumns != null && !toColumns.isEmpty()) {
//                            toColumns.addAll(columnClauseCols);
//                        }
//                    }
//
//                    List<String> toColumnsList = new ArrayList<>();
//                    toColumnsList.addAll(toColumns);
//                    String toTableName = (String) toOperator.get("tableName");
//                    int totalDataCount = 0;
//                    int fileDataLastIndex = 0;
//                    List<Map> nonJoinOpList = fromOperatorList;
//                    String fileName = "";
//                    String orginalName = (String) toOperator.get("userFileName");
//                    String iconType = (String) toOperator.get("iconType");
//                    JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
//                    String timeStamp = String.valueOf(toOperator.get("timeStamp"));// ravi etl issues new
//                    if (!(timeStamp != null
//                            && !"".equalsIgnoreCase(timeStamp)
//                            && !"null".equalsIgnoreCase(timeStamp))) {
//                        timeStamp = "" + System.currentTimeMillis();
//                        toOperator.put("timeStamp", timeStamp);
//                    }
//                    if ("XLSX".equalsIgnoreCase(iconType)) {
//                        fileName = "V10ETLExport_" + timeStamp + ".xlsx";
//
//                    } else if ("XLS".equalsIgnoreCase(iconType)) {
//                        fileName = "V10ETLExport_" + timeStamp + ".xls";
//                    } else if ("XML".equalsIgnoreCase(iconType)) {
//                        fileName = "V10ETLExport_" + timeStamp + ".xml";
//                    } else if ("CSV".equalsIgnoreCase(iconType)) {
//                        fileName = "V10ETLExport_" + timeStamp + ".csv";
//                    } else if ("TXT".equalsIgnoreCase(iconType)) {
//                        fileName = "V10ETLExport_" + timeStamp + ".txt";
//                    } else if ("JSON".equalsIgnoreCase(iconType)) {
//                        fileName = "V10ETLExport_" + timeStamp + ".json";
////                    if ("XLSX".equalsIgnoreCase(iconType)) {
////                        fileName = "V10ETLExport_" + System.currentTimeMillis() + ".xlsx";
////
////                    } else if ("XLS".equalsIgnoreCase(iconType)) {
////                        fileName = "V10ETLExport_" + System.currentTimeMillis() + ".xls";
////                    } else if ("XML".equalsIgnoreCase(iconType)) {
////                        fileName = "V10ETLExport_" + System.currentTimeMillis() + ".xml";
////                    } else if ("CSV".equalsIgnoreCase(iconType)) {
////                        fileName = "V10ETLExport_" + System.currentTimeMillis() + ".csv";
////                    } else if ("TXT".equalsIgnoreCase(iconType)) {
////                        fileName = "V10ETLExport_" + System.currentTimeMillis() + ".txt";
////                    } else if ("JSON".equalsIgnoreCase(iconType)) {
////                        fileName = "V10ETLExport_" + System.currentTimeMillis() + ".json";
//                    } else {
//                        if (toTableName != null
//                                && !"".equalsIgnoreCase(String.valueOf(toTableName))
//                                && !"null".equalsIgnoreCase(String.valueOf(toTableName))) {
//                            Object toConnectionObj = getConnection(toConnObj);
//                            if (toConnectionObj instanceof Connection) {
//                                toConnection = (Connection) toConnectionObj;
//                            }
//                            if (toConnection != null) {
//                                String toTableInsertQuery = genericProcessETLDataService.generateInsertQuery((String) toOperator.get("tableName"), toColumnsList);
//                                System.out.println("insertQuery::::" + toTableInsertQuery);
//                                toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
//                                Map columnsTypeObj = genericProcessETLDataService.getColumnsType((String) toOperator.get("tableName"), toColumnsList, toConnection);
//                                selectTabObj.put("columnsTypeObj", columnsTypeObj);
//                            }
//
//                        }
//                    }
//                    if (!(toColumnsList != null && !toColumnsList.isEmpty())) {
//                        toColumnsList.addAll(columnsObj.keySet());
//                    }
//                    // need to get SAP data and create the tables in Current V10 DB
//                    JSONObject sapColumnsObj = genericProcessETLDataService.getSelectedSAPTablesData(loginUserName,
//                            loginOrgnId,
//                            fromJCOConnection,
//                            fromOperatorList,
//                            columnsObj,
//                            fromColumnsObj,
//                            tablesWhereClauseObj,
//                            defaultValuesObj,
//                            orderByObj,
//                            groupByQuery,
//                            joinQueryMapObj
//                    );
//
//                    if (sapColumnsObj != null && !sapColumnsObj.isEmpty()) {
//                        JSONObject dbFromColumnsObj = (JSONObject) sapColumnsObj.get("fromColumnsObj");
//                        JSONObject tableNameObj = (JSONObject) sapColumnsObj.get("tableNameObj");
//                        fromConnection = DriverManager.getConnection(dbURL, userName, password);
////                    String dbDriver, String dbURL,String userName,String password,String connName
//                        JSONObject currentV10DBObj = new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName, password, "Current_V10");
//                        fromOperatorList = fromOperatorList.stream().map(formOp -> {
//                            try {
//                                formOp.put("CONNECTION_NAME", currentV10DBObj.get("CONNECTION_NAME"));
//                                formOp.put("CONN_DB_NAME", currentV10DBObj.get("CONN_DB_NAME"));
//                                formOp.put("CONN_CUST_COL1", currentV10DBObj.get("CONN_CUST_COL1"));
//                                formOp.put("connObj", currentV10DBObj);
//                                if (tableNameObj != null && !tableNameObj.isEmpty()
//                                        && tableNameObj.containsKey(String.valueOf(formOp.get("tableName")))) {
//                                    formOp.put("tableName", tableNameObj.get(String.valueOf(formOp.get("tableName"))));
//                                }
//
//                            } catch (Exception e) {
//                            }
//                            return formOp;
//                        }).collect(Collectors.toList());
//                        JSONObject tempJoinQueryMapObj = new JSONObject();
//                        JSONObject tempTablesWhereClauseObj = new JSONObject();
//                        JSONObject tempColumnsObj = new JSONObject();
//                        tempColumnsObj.putAll(columnsObj);
//                        for (Object sapTableName : tableNameObj.keySet()) {
//                            if (joinQueryMapObj != null
//                                    && !joinQueryMapObj.isEmpty()
//                                    && joinQueryMapObj.containsKey(sapTableName)) {
//                                tempJoinQueryMapObj.put(String.valueOf(tableNameObj.get(sapTableName)), joinQueryMapObj.get(sapTableName));
//                                joinQueryMapObj.remove(sapTableName);
//
//                            }
//                            if (tablesWhereClauseObj != null
//                                    && !tablesWhereClauseObj.isEmpty()
//                                    && tablesWhereClauseObj.containsKey(sapTableName)) {
//                                tempTablesWhereClauseObj.put(String.valueOf(tableNameObj.get(sapTableName)), joinQueryMapObj.get(sapTableName));
//                                tablesWhereClauseObj.remove(sapTableName);
//                            }
//                            if (columnsObj != null && !columnsObj.isEmpty()) {
//
//                                for (Object sourceCol : columnsObj.keySet()) {
//                                    String keyValueContains = sapTableName + ":";
//
//                                    if (columnsObj.get(sourceCol) != null
//                                            && !"".equalsIgnoreCase(String.valueOf(columnsObj.get(sourceCol)))
//                                            && !"null".equalsIgnoreCase(String.valueOf(columnsObj.get(sourceCol)))
//                                            && String.valueOf(columnsObj.get(sourceCol)).contains(keyValueContains)) {
//                                        String modifiedKey = (String) sourceCol;
//                                        if (String.valueOf(sourceCol).contains(keyValueContains)) {
//                                            tempColumnsObj.remove(sourceCol);
//                                            modifiedKey = String.valueOf(sourceCol).replace(keyValueContains, String.valueOf(tableNameObj.get(sapTableName)) + ":");
//                                        }
//                                        tempColumnsObj.put(modifiedKey,
//                                                String.valueOf(columnsObj.get(sourceCol))
//                                                        .replace(keyValueContains, String.valueOf(tableNameObj.get(sapTableName)) + ":"));
//                                    }
//                                }
//                            }
//
//                            if (joinQuery != null
//                                    && !"".equalsIgnoreCase(joinQuery)
//                                    && !"null".equalsIgnoreCase(joinQuery)) {
//                                String keyValueContains = " " + sapTableName + " ";
//                                joinQuery = joinQuery.replaceAll(keyValueContains, " " + String.valueOf(tableNameObj.get(sapTableName)) + " ");
//                                keyValueContains = sapTableName + "[.]";
//                                joinQuery = joinQuery.replaceAll(keyValueContains, String.valueOf(tableNameObj.get(sapTableName)) + ".");
//                            }
//                            if (fileName != null
//                                    && !"".equalsIgnoreCase(fileName)
//                                    && !"null".equalsIgnoreCase(fileName)
//                                    && toColumnsList != null && !toColumnsList.isEmpty()) {
//                                String keyValueContains = sapTableName + ":";
//                                toColumnsList = toColumnsList.stream().map(toColName -> (String.valueOf(toColName).replace(keyValueContains, String.valueOf(tableNameObj.get(sapTableName)) + ":")))
//                                        .collect(Collectors.toList());
//                            }
//                        }// end loop
//                        if (tempColumnsObj != null && !tempColumnsObj.isEmpty()) {
//                            columnsObj = tempColumnsObj;
//                        }
//                        if (tempJoinQueryMapObj != null && !tempJoinQueryMapObj.isEmpty()) {
//                            joinQueryMapObj.putAll(tempJoinQueryMapObj);
//                        }
//                        if (tempTablesWhereClauseObj != null && !tempTablesWhereClauseObj.isEmpty()) {
//                            tablesWhereClauseObj.putAll(tempTablesWhereClauseObj);
//                        }
//
//                        if (joinQueryMapObj != null
//                                && !joinQueryMapObj.isEmpty()) {
//
//                            nonJoinOpList = fromOperatorList.stream()
//                                    .filter(opMap -> (opMap != null && !opMap.isEmpty()
//                                    && opMap.get("tableName") != null
//                                    && !"".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
//                                    && !"null".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
//                                    && !joinQueryMapObj.containsKey(String.valueOf(opMap.get("tableName"))))).collect(Collectors.toList());
//                            try {
//                                genericProcessETLDataService.processETLLog(loginUserName,
//                                        loginOrgnId, "Starting extract join tables data.", "INFO", 10, "Y", jobId);
//                            } catch (Exception e) {
//                            }
//                            totalDataCount += genericProcessETLDataService.processETLData(loginUserName,
//                                    loginOrgnId,
//                                    fromConnection,
//                                    toConnection,
//                                    fromOperatorList,
//                                    toOperator,
//                                    columnsObj,
//                                    dbFromColumnsObj,
//                                    tablesWhereClauseObj,
//                                    defaultValuesObj,
//                                    toPreparedStatement,
//                                    0,
//                                    1000,
//                                    totalDataCount,
//                                    toColumnsList,
//                                    joinQueryMapObj,
//                                    joinQuery,
//                                    fileDataLastIndex,
//                                    fileName,
//                                    1,
//                                    orderAndGroupByObj,
//                                    columnClauseObj,
//                                    selectTabObj,
//                                    normalizeOptionsObj
//                            );
//
//                        } else {
//                            if (nonJoinOpList != null && !nonJoinOpList.isEmpty()) {
//                                try {
//                                    genericProcessETLDataService.processETLLog(loginUserName,
//                                            loginOrgnId, "Starting extract non-join tables data.", "INFO", 10, "Y", jobId);
//                                } catch (Exception e) {
//                                }
//                                for (int i = 0; i < nonJoinOpList.size(); i++) {
//                                    Map fromOperator = nonJoinOpList.get(i);
//                                    if (fromOperator != null && !fromOperator.isEmpty()) {
//                                        totalDataCount += genericProcessETLDataService.processETLData(loginUserName,
//                                                loginOrgnId,
//                                                fromConnection,
//                                                toConnection,
//                                                fromOperator,
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
//                                                fileDataLastIndex,
//                                                fileName,
//                                                1,
//                                                orderAndGroupByObj,
//                                                columnClauseObj,
//                                                selectTabObj,
//                                                normalizeOptionsObj
//                                        );
//                                    }
//
//                                }
//                            }
//
//                        }
//                        System.out.println("totalDataCount::::" + totalDataCount);
//                        if (totalDataCount != 0 && totalDataCount > 0) {
//                            String message = totalDataCount + " Row(s) successfully extracted and loaded into target system.";
//                            if (fileName != null && !"".equalsIgnoreCase(fileName) && !"null".equalsIgnoreCase(fileName)) {
//                                message += " <br> <a href='#' style='color:#0071c5;' onclick=downloadExportedFile('" + fileName + "') >Click here to download the " + iconType + " file</a>.";//
//                            }
//
//                            try {
//                                genericProcessETLDataService.processETLLog(loginUserName,
//                                        loginOrgnId, message, "INFO", 10, "Y", jobId);
//                            } catch (Exception e) {
//                            }
//                            resultObj.put("Message", message);
//                            resultObj.put("connectionFlag", "Y");
//
//                        } else {
//                            try {
//                                genericProcessETLDataService.processETLLog(loginUserName,
//                                        loginOrgnId, totalDataCount + " Row(s) successfully extracted and loaded into target system.", "INFO", 10, "Y", jobId);
//                            } catch (Exception e) {
//                            }
////                    processETLLog(httpSession,totalDataCount + " Row(s) extracted successfully", 10);
//                            resultObj.put("Message", totalDataCount + " Row(s) successfully extracted and loaded into target system.");
//                            resultObj.put("connectionFlag", "Y");
//                        }
//                        try {
//                            genericProcessETLDataService.dropTemptables(tableNameObj);
//                        } catch (Exception e) {
//                        }
//                    } else {
//                        try {
//                            genericProcessETLDataService.processETLLog(loginUserName,
//                                    loginOrgnId, totalDataCount + " Row(s) successfully extracted and loaded into target system.", "INFO", 10, "Y", jobId);
//                        } catch (Exception e) {
//                        }
////                    processETLLog(httpSession,totalDataCount + " Row(s) extracted successfully", 10);
//                        resultObj.put("Message", totalDataCount + " Row(s) successfully extracted and loaded into target system.");
//                        resultObj.put("connectionFlag", "Y");
//                    }
//
//                    System.out.println("totalDataCount::::" + totalDataCount);
//                    if (totalDataCount != 0 && totalDataCount > 0) {
//                        String message = totalDataCount + " Row(s) successfully extracted and loaded into target system.";
//                        if (fileName != null && !"".equalsIgnoreCase(fileName) && !"null".equalsIgnoreCase(fileName)) {//orginalName
//                            if (orginalName != null
//                                    && !"".equalsIgnoreCase(orginalName)
//                                    && !"null".equalsIgnoreCase(orginalName)) {
//                                orginalName = orginalName.replaceAll("[^.a-zA-Z0-9]", "_");
//                            }
//
//                            message += " <br> <a href='#' style='color:#0071c5;' onclick=downloadExportedFile('" + fileName + "',\"" + orginalName + "\") >Click here to download the "
//                                    + "" + iconType + ((orginalName != null
//                                    && !"".equalsIgnoreCase(orginalName)
//                                    && !"null".equalsIgnoreCase(orginalName)) ? "(" + orginalName + ")" : "") + " file</a>.";//
//                        }
//
//                        try {
//                            genericProcessETLDataService.processETLLog(loginUserName,
//                                    loginOrgnId, message, "INFO", 10, "Y", jobId);
//                        } catch (Exception e) {
//                        }
//                        resultObj.put("Message", message);
//                        resultObj.put("connectionFlag", "Y");
//
//                    } else {
//                        try {
//                            genericProcessETLDataService.processETLLog(loginUserName,
//                                    loginOrgnId, totalDataCount + " Row(s) successfully extracted and loaded into target system.", "INFO", 10, "Y", jobId);
//                        } catch (Exception e) {
//                        }
//
//                        resultObj.put("Message", totalDataCount + " Row(s) successfully extracted and loaded into target system.");
//                        resultObj.put("connectionFlag", "Y");
//                    }
//                }
//            }//end if transformationMap
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            try {
//                if (toPreparedStatement != null) {
//                    toPreparedStatement.close();
//                }
//                if (toConnection != null) {
//                    toConnection.close();
//                }
//                if (toJCOConnection != null) {
//                    toJCOConnection.disconnect();
//                }
//            } catch (Exception e) {
//            }
//        }
//        return resultObj;
//    }

    // for executing job if the sources(from) are Files
    public JSONObject processJob(String loginUserName,
            String loginOrgnId,
            Map toOperator,
            Map transformationMap,
            List<Map> fromOperatorList,
            String jobId) {
        JSONObject resultObj = new JSONObject();
        Connection toConnection = null;
        Connection fromConnection = null;
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
                        genericProcessETLDataService.processETLLog(loginUserName,
                                loginOrgnId,
                                "Reading the transformations rules.",
                                "INFO", 10, "Y", jobId);
                    } catch (Exception e) {
                    }
                    //JSONObject fromColumnsObj = new JSONObject();
                    Map fromColumnsObj = new LinkedHashMap();// ravi etl integration
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
                            selectTabObj.put("toConnectionObj", toConnectionObj);
                            if (toConnectionObj instanceof Connection) {
                                toConnection = (Connection) toConnectionObj;
                            }
                            if (toConnection != null) {
                                String toTableInsertQuery = genericProcessETLDataService.generateInsertQuery((String) toOperator.get("tableName"), toColumnsList);
                                System.out.println("insertQuery::::" + toTableInsertQuery);
                                toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
                                Map columnsTypeObj = genericProcessETLDataService.getColumnsType((String) toOperator.get("tableName"), toColumnsList, toConnection);
                                selectTabObj.put("columnsTypeObj", columnsTypeObj);
                            }

                        }
                    }
                    if (!(toColumnsList != null && !toColumnsList.isEmpty())) {
                        toColumnsList.addAll(columnsObj.keySet());
                    }
                    JSONObject joinQueryMapObj = (JSONObject) transformationMap.get("joinQueryMapObj");//
                    if (joinQueryMapObj != null
                            && !joinQueryMapObj.isEmpty()) {

                        nonJoinOpList = fromOperatorList.stream()
                                .filter(opMap -> (opMap != null && !opMap.isEmpty()
                                && opMap.get("connObj") != null
                                && !((JSONObject) opMap.get("connObj")).isEmpty()
                                && !"".equalsIgnoreCase(String.valueOf(((JSONObject) opMap.get("connObj")).get("fileName")))
                                && !"null".equalsIgnoreCase(String.valueOf(((JSONObject) opMap.get("connObj")).get("fileName")))
                                && !joinQueryMapObj.containsKey(String.valueOf(((JSONObject) opMap.get("connObj")).get("fileName")))))
                                .collect(Collectors.toList());
                        List<Map> joinOpList = fromOperatorList.stream()
                                .filter(opMap -> (opMap != null && !opMap.isEmpty()
                                && opMap.get("connObj") != null
                                && !((JSONObject) opMap.get("connObj")).isEmpty()
                                && !"".equalsIgnoreCase(String.valueOf(((JSONObject) opMap.get("connObj")).get("fileName")))
                                && !"null".equalsIgnoreCase(String.valueOf(((JSONObject) opMap.get("connObj")).get("fileName")))
                                && joinQueryMapObj.containsKey(String.valueOf(((JSONObject) opMap.get("connObj")).get("fileName")))))
                                .collect(Collectors.toList());
                        try {
                            genericProcessETLDataService.processETLLog(loginUserName,
                                    loginOrgnId, "Starting extract join files data.", "INFO", 10, "Y", jobId);
                        } catch (Exception e) {
                        }
                        // need to get files data into table
                        List<Map> fromOperatorMapList = new ArrayList<>();
                        if (joinOpList != null && !joinOpList.isEmpty()) {
//                            Map trfmRulesDataMap = (Map) toOperator.get("trfmRules-data");
                            List fromFilesList = new ArrayList<>();
                            for (int i = 0; i < joinOpList.size(); i++) {
                                Map fromOpearator = joinOpList.get(i);
                                if (fromOpearator != null && !fromOpearator.isEmpty()) {
                                    Map fromOpearatorMap = genericProcessETLDataService.getSelectedFileTablesData(fromOpearator,
                                            toOperator,
                                            fromColumnsObj,
                                            transformationMap,
                                            fileDataLastIndex,
                                            fileName);
                                    if (fromOpearatorMap != null && !fromOpearatorMap.isEmpty()) {
                                        fromOperatorMapList.add(fromOpearatorMap);
                                        fromFilesList.add(fromOpearatorMap.get("oldTableName"));
                                        if (fromColumnsObj.containsKey(fromOpearatorMap.get("oldTableName"))) {
                                            transformationMap = genericProcessETLDataService.convertTransFrmRulsMapToParam(transformationMap, fromOpearatorMap);
                                            fromColumnsObj = genericProcessETLDataService.updateFromColsObj(fromOpearatorMap, fromColumnsObj);

                                        }
                                    }

                                }

                            }
                            for (int i = 0; i < fromFilesList.size(); i++) {
                                Object oldTableName = fromFilesList.get(i);
                                if (fromColumnsObj != null
                                        && !fromColumnsObj.isEmpty()
                                        && fromColumnsObj.containsKey(oldTableName)) {
                                    fromColumnsObj.remove(oldTableName);
                                }

                            }
                            System.out.println("fromColumnsObj::" + fromColumnsObj);
                        }
                        LinkedHashMap joinQueryHashMapObj = (LinkedHashMap) transformationMap.get("joinQueryHashMapObj");//
                        JSONObject joinQueryMapObjUpdated = new JSONObject(joinQueryHashMapObj);
                        if (fromOperatorMapList != null && !fromOperatorMapList.isEmpty()) {
                            fromConnection = DriverManager.getConnection(dbURL, userName, password);
                            fromOperatorList = fromOperatorMapList;
                            fromOperatorList.addAll(nonJoinOpList);
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
                        selectTabObj.remove("oldTableName");
                        if (toConnection != null) {
                            Map columnsTypeObj = genericProcessETLDataService.getColumnsType((String) toOperator.get("tableName"), toColumnsList, toConnection);
                            selectTabObj.put("columnsTypeObj", columnsTypeObj);
                        }

                        totalDataCount += genericProcessETLDataService.processETLData(loginUserName,
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
                                joinQueryMapObjUpdated,
                                joinQuery,
                                fileDataLastIndex,
                                fileName,
                                1,
                                orderAndGroupByObj,
                                columnClauseObj,
                                selectTabObj,
                                normalizeOptionsObj
                        );
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
                    } else {
                        if (nonJoinOpList != null && !nonJoinOpList.isEmpty()) {
                            try {
                                genericProcessETLDataService.processETLLog(loginUserName,
                                        loginOrgnId, "Starting extract non-join Files data.", "INFO", 10, "Y", jobId);
                            } catch (Exception e) {
                            }
                            for (int i = 0; i < nonJoinOpList.size(); i++) {
                                Map fromOperator = nonJoinOpList.get(i);
                                if (fromOperator != null && !fromOperator.isEmpty()) {
                                    JSONObject fileObj = (JSONObject) fromOperator.get("connObj");
                                    List<String> columnList = new ArrayList<>();
                                    Object fileLoadObj = null;
                                    LineNumberReader lineNumberReader = null;
                                    if (fileObj != null && !fileObj.isEmpty()) {
                                        String fileType = (String) fileObj.get("fileType");
                                        String fromFileName = (String) fileObj.get("fileName");
                                        String filePath = (String) fileObj.get("filePath");

                                        if (".xls".equalsIgnoreCase(fileType) || ".xlsx".equalsIgnoreCase(fileType)) {
                                            columnList = genericProcessETLDataService.getHeadersOfImportedFile(filePath, fileType);
                                            Workbook workBook = WorkbookFactory.create(new File(filePath));
                                            fileLoadObj = workBook;
                                        } else if (".CSV".equalsIgnoreCase(fileType)
                                                || ".TXT".equalsIgnoreCase(fileType)
                                                || ".json".equalsIgnoreCase(fileType)) {
                                            columnList = genericProcessETLDataService.getHeadersOfImportedFile(filePath, fileType);
//                                            char columnSeparator = '\t';
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
                                        }
                                    }
                                    totalDataCount += genericProcessETLDataService.processETLData(loginUserName,
                                            loginOrgnId,
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
                                            normalizeOptionsObj,
                                            fileLoadObj,
                                            columnList,
                                            lineNumberReader
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

                        try {
                            genericProcessETLDataService.processETLLog(loginUserName,
                                    loginOrgnId, message, "INFO", 10, "Y", jobId);
                        } catch (Exception e) {
                        }
                        resultObj.put("Message", message);
                        resultObj.put("connectionFlag", "Y");

                    } else {
                        try {
                            genericProcessETLDataService.processETLLog(loginUserName,
                                    loginOrgnId, totalDataCount + " Row(s) successfully extracted and loaded into target system.", "INFO", 10, "Y", jobId);
                        } catch (Exception e) {
                        }

                        resultObj.put("Message", totalDataCount + " Row(s) successfully extracted and loaded into target system.");
                        resultObj.put("connectionFlag", "Y");
                    }
                }
            }// end if transformationMap
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    public Map getFromAndToOperatorList(Map linksMap, Map operatorsMap, Object mappedKey) {
        Map<String, List<Map>> linkedFronAndToOpMap = new HashMap<>();
        try {
            if (linksMap != null && !linksMap.isEmpty()) {
                List<Map> fromOperatorList = new ArrayList<>();
                List<Map> toOperatorList = new ArrayList<>();
                for (Object linkKey : linksMap.keySet()) {
                    Map linkMap = (Map) linksMap.get(linkKey);
                    if (linkMap != null
                            && !linkMap.isEmpty()
                            && String.valueOf(mappedKey)
                                    .equalsIgnoreCase(String.valueOf(linkMap.get("toOperator")))) {
//                                System.out.println("linkMap.get(\"fromOperator\")::" + linkMap.get("fromOperator"));
                        String fromOperatorId = String.valueOf(linkMap.get("fromOperator"));
                        fromOperatorList.add((Map) operatorsMap.get(String.valueOf(linkMap.get("fromOperator"))));

                    }
                    if (linkMap != null
                            && !linkMap.isEmpty()
                            && String.valueOf(mappedKey)
                                    .equalsIgnoreCase(String.valueOf(linkMap.get("fromOperator")))) {
                        toOperatorList.add((Map) operatorsMap.get(String.valueOf(linkMap.get("toOperator"))));

                    }
                }// end loop linksMap
                linkedFronAndToOpMap.put("fromOperatorList", fromOperatorList);
                linkedFronAndToOpMap.put("toOperatorList", toOperatorList);
            }
        } catch (Exception e) {
        }
        return linkedFronAndToOpMap;
    }

    public void processNestedJobData(Map<String, List<Map>> toOpeartorMapObj,
            Map mappedData,
            String jobId,
            String loginUserName,
            String loginOrgnId) {
        Connection fromConnection = null;
        Connection toConnection = null;
        JCO.Client fromJCOConnection = null;
        JCO.Client toJCOConnection = null;
        try {
            List<Map> toOpertaotrsListOfToOp = toOpeartorMapObj.get("toOperatorList");
            if (toOpertaotrsListOfToOp != null && !toOpertaotrsListOfToOp.isEmpty()) {
                // need to get connectros map like MAP,Normalize,De-Normalize etc..
                List<Map> mappingOperatorObjList = toOpertaotrsListOfToOp.stream()
                        .filter(opMap -> (opMap != null && (String) opMap.get("iconType") != null
                        && ("MAP".equalsIgnoreCase(String.valueOf(opMap.get("iconType")))
                        || "GROUP".equalsIgnoreCase(String.valueOf(opMap.get("iconType")))
                        || "UNGROUP".equalsIgnoreCase(String.valueOf(opMap.get("iconType"))))))
                        .collect(Collectors.toList());
                Map operatorsMap = (Map) mappedData.get("operators");
                Map linksMap = (Map) mappedData.get("links");
                if (linksMap != null && !linksMap.isEmpty()) {
                    for (Map connectorsMapObj : mappingOperatorObjList) {
                        if (connectorsMapObj != null && !connectorsMapObj.isEmpty()) {
                            Object operatorMapId = connectorsMapObj.get("operatorId");

                            Map<String, List<Map>> mapOpeartorLevelObj = getFromAndToOperatorList(linksMap,
                                    operatorsMap,
                                    connectorsMapObj.get("operatorId"));
                            if (mapOpeartorLevelObj != null && !mapOpeartorLevelObj.isEmpty()) {
                                List<Map> fromOperatorList = mapOpeartorLevelObj.get("fromOperatorList");
                                List<Map> toOperatorList = mapOpeartorLevelObj.get("toOperatorList");
                                if (fromOperatorList != null
                                        && !fromOperatorList.isEmpty()
                                        && toOperatorList != null
                                        && !toOperatorList.isEmpty()) {
                                    for (int i = 0; i < toOperatorList.size(); i++) {
                                        Map toOperator = toOperatorList.get(i);
                                        if (toOperator != null && !toOperator.isEmpty()) {
                                            Map trfmRulesDataMap = (Map) toOperator.get("trfmRules-data");
                                            if (trfmRulesDataMap != null && !trfmRulesDataMap.isEmpty()) {
                                                try {
                                                    Map transformationRulesMap = convertTransFrmRulsMapToParam(trfmRulesDataMap);
                                                    if (transformationRulesMap != null
                                                            && !transformationRulesMap.isEmpty()) {
                                                        boolean isSameDataBase = fromOperatorList.stream()
                                                                .filter(fromOpMap -> (fromOpMap.containsKey("CONNECTION_NAME") && fromOpMap.containsKey("CONN_DB_NAME")))
                                                                .map(fromOpMap -> (fromOpMap.get("CONNECTION_NAME") + ":::" + fromOpMap.get("CONN_DB_NAME")))
                                                                .distinct().count() == 1;
                                                        boolean iscontainsFile = fromOperatorList.stream()
                                                                .anyMatch(fromOpMap -> "file".equalsIgnoreCase(String.valueOf(fromOpMap.get("dragType"))));
                                                        if (isSameDataBase && !iscontainsFile) {// CASE 1 --> if all from tables having same data base
                                                            Map fromOperator = fromOperatorList.get(0);
                                                            if (fromOperator != null && !fromOperator.isEmpty()) {
                                                                JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");
                                                                String fromTableName = (String) fromOperator.get("tableName");
                                                                JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
                                                                String toTableName = (String) toOperator.get("tableName");
                                                                String toIconType = (String) toOperator.get("iconType");
                                                                Object fromConnectionObj = getConnection(fromConnObj);
                                                                if (fromConnectionObj instanceof Connection) {
                                                                    fromConnection = (Connection) fromConnectionObj;
                                                                    try {
                                                                        genericProcessETLDataService.processETLLog(loginUserName,
                                                                                loginOrgnId,
                                                                                "Source system successfully connected.", "INFO", 20, "Y", jobId);
                                                                    } catch (Exception e) {
                                                                    }
                                                                    processJob(loginUserName,
                                                                            loginOrgnId,
                                                                            toOperator,
                                                                            transformationRulesMap,
                                                                            fromConnection,
                                                                            fromOperatorList,
                                                                            jobId);
                                                                } else if (fromConnectionObj instanceof JCO.Client) {
                                                                    fromJCOConnection = (JCO.Client) fromConnectionObj;
                                                                    // for SAP
                                                                    try {
                                                                        genericProcessETLDataService.processETLLog(loginUserName,
                                                                                loginOrgnId,
                                                                                "Source system successfully connected.", "INFO", 20, "Y", jobId);
                                                                    } catch (Exception e) {
                                                                    }
                                                                    processJob(loginUserName,
                                                                            loginOrgnId,
                                                                            toOperator,
                                                                            transformationRulesMap,
                                                                            fromJCOConnection,
                                                                            fromOperatorList,
                                                                            jobId);
                                                                } else {
                                                                    try {
                                                                        genericProcessETLDataService.processETLLog(loginUserName,
                                                                                loginOrgnId,
                                                                                "Unable to connect source system due to " + fromConnectionObj, "ERROR", 20, "N", jobId);
                                                                    } catch (Exception e) {
                                                                    }

                                                                }
                                                            }
                                                        } else {
                                                            boolean isDataBase = fromOperatorList.stream().anyMatch(fromOpMap
                                                                    -> (fromOpMap.containsKey("CONNECTION_NAME")
                                                                    && fromOpMap.containsKey("CONN_DB_NAME")));
                                                            if (!isDataBase) {// CASE 2 --> if all from objects having files.                                                        
                                                                processJob(loginUserName,
                                                                        loginOrgnId,
                                                                        toOperator,
                                                                        transformationRulesMap,
                                                                        fromOperatorList,
                                                                        jobId);
                                                            } else { // CASE 3 --> if all from tables having different DB.

                                                            }
                                                        }
                                                    }
                                                } catch (Exception e) {
                                                    e.printStackTrace();
                                                    continue;
                                                }

                                            }
                                        }

                                        // if any operators connected with To Operator
                                        Map<String, List<Map>> nestedToOpeartorMapObj = getFromAndToOperatorList(linksMap, operatorsMap, toOperator.get("operatorId"));
                                        if (nestedToOpeartorMapObj != null && !nestedToOpeartorMapObj.isEmpty()) {
                                            processNestedJobData(nestedToOpeartorMapObj, mappedData, jobId, loginUserName, loginOrgnId);

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
                if (toConnection != null) {
                    toConnection.close();
                }
                if (toJCOConnection != null) {
                    toJCOConnection.disconnect();
                }
                if (fromJCOConnection != null) {
                    fromJCOConnection.disconnect();
                }
            } catch (Exception e) {
            }
        }

    }



    // for executing job if the sources(from) Data Base tables with different schema
    public JSONObject processJobWithDiffSchema(String loginUserName,
            String loginOrgnId,
            Map toOperator,
            Map transformationMap,
            List<Map> fromOperatorList,
            String jobId
    ) {
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
                        genericProcessETLDataService.processETLLog(loginUserName,
                                loginOrgnId,
                                "Reading the transformations rules.",
                                "INFO", 10, "Y", jobId);
                    } catch (Exception e) {
                    }
                    //JSONObject fromColumnsObj = new JSONObject();
                    Map fromColumnsObj = new LinkedHashMap();// ravi etl integration
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
                            selectTabObj.put("toConnectionObj", toConnectionObj);
                            if (toConnectionObj instanceof Connection) {
                                toConnection = (Connection) toConnectionObj;
                            }
                            if (toConnection != null) {
                                String toTableInsertQuery = genericProcessETLDataService.generateInsertQuery((String) toOperator.get("tableName"),
                                        toColumnsList);
                                System.out.println("insertQuery::::" + toTableInsertQuery);
                                toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
                                Map columnsTypeObj = genericProcessETLDataService.getColumnsType((String) toOperator.get("tableName"),
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
//                        Map trfmRulesDataMap = (Map) toOperator.get("trfmRules-data");
                        for (int i = 0; i < fromOperatorList.size(); i++) {
                            Map fromOpearator = fromOperatorList.get(i);
                            if (fromOpearator != null && !fromOpearator.isEmpty()) {
                                Map fromOpearatorMap = genericProcessETLDataService.getSelectedDBTablesData(fromOpearator,
                                        toOperator,
                                        fromColumnsObj,
                                        transformationMap);
                                if (fromOpearatorMap != null && !fromOpearatorMap.isEmpty()) {
                                    fromOperatorMapList.add(fromOpearatorMap);
                                    if (fromColumnsObj.containsKey(fromOpearatorMap.get("oldTableName"))) {
                                        transformationMap = genericProcessETLDataService.convertTransFrmRulsMapToParam(transformationMap, fromOpearatorMap);
                                        fromColumnsObj.put(fromOpearatorMap.get("tableName"), fromColumnsObj.get(fromOpearatorMap.get("oldTableName")));
                                        fromColumnsObj.remove(fromOpearatorMap.get("oldTableName"));
                                    }
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
                        Map columnsTypeObj = genericProcessETLDataService.getColumnsType((String) toOperator.get("tableName"), toColumnsList, toConnection);
                        selectTabObj.put("columnsTypeObj", columnsTypeObj);
                    }
//                    JSONObject joinQueryMapObj = (JSONObject) transformationMap.get("joinQueryHashMapObj");//
                    LinkedHashMap joinQueryHashMapObj = (LinkedHashMap) transformationMap.get("joinQueryHashMapObj");
                    JSONObject joinQueryMapObj = new JSONObject();//
                    if (joinQueryHashMapObj != null && !joinQueryHashMapObj.isEmpty()) {
                        joinQueryMapObj.putAll(joinQueryHashMapObj);
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
                            genericProcessETLDataService.processETLLog(loginUserName,
                                    loginOrgnId, "Starting extract join tables data.", "INFO", 10, "Y", jobId);
                        } catch (Exception e) {
                        }
                        totalDataCount += genericProcessETLDataService.processETLData(loginUserName,
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
                                genericProcessETLDataService.processETLLog(loginUserName,
                                        loginOrgnId, "Starting extract non-join tables data.", "INFO", 10, "Y", jobId);
                            } catch (Exception e) {
                            }
                            for (int i = 0; i < nonJoinOpList.size(); i++) {
                                Map fromOperator = nonJoinOpList.get(i);
                                if (fromOperator != null && !fromOperator.isEmpty()) {
                                    totalDataCount += genericProcessETLDataService.processETLData(loginUserName,
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
                            genericProcessETLDataService.processETLLog(loginUserName,
                                    loginOrgnId, message, "INFO", 10, "Y", jobId);
                        } catch (Exception e) {
                        }
                        resultObj.put("Message", message);
                        resultObj.put("connectionFlag", "Y");

                    } else {
                        try {
                            genericProcessETLDataService.processETLLog(loginUserName,
                                    loginOrgnId, totalDataCount + " Row(s) successfully extracted and loaded into target system.", "INFO", 10, "Y", jobId);
                        } catch (Exception e) {
                        }

                        resultObj.put("Message", totalDataCount + " Row(s) successfully extracted and loaded into target system.");
                        resultObj.put("connectionFlag", "Y");
                    }
                }
            }//end if transformationMap
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

    public Map convertTransFrmRulsMapToParamOld(Map trfmRulesDataMap, Map fromOperator) {
        Map paramMap = new HashMap<>();
        try {

            JSONObject orderByList = new JSONObject();
            String groupByColumns = "";
            String oldTableName = (String) fromOperator.get("oldTableName");
            String newTableName = (String) fromOperator.get("tableName");
            String dragType = (String) fromOperator.get("dragType");
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
                                String orderByColumn = String.valueOf(orderByDataMap.get("columnName"));
                                if (orderByColumn.contains(oldTableName)) {
                                    orderByColumn = orderByColumn.replaceAll(oldTableName, newTableName);
                                    if ("file".equalsIgnoreCase(dragType)) {
                                        orderByColumn = orderByColumn.trim().replaceAll(" ", "_");
                                    }
                                }
                                orderByObj.put("columnName", orderByColumn);
                                orderByObj.put("direction", orderByDataMap.get("order"));
                                orderByList.put(index, orderByObj);
                                index++;
                            }

                        }
                    }
                }// orderByDataMapList end
                paramMap.put("orderBy", orderByList);
                List<Map> groupByDataMapList = (List) trfmRulesDataMap.get("groupByData");
                if (groupByDataMapList != null && !groupByDataMapList.isEmpty()) {
                    for (Map groupByDataMap : groupByDataMapList) {
                        if (groupByDataMap != null && !groupByDataMap.isEmpty()) {
                            if (groupByDataMap.get("columnName") != null
                                    && !"".equalsIgnoreCase(String.valueOf(groupByDataMap.get("columnName")))
                                    && !"null".equalsIgnoreCase(String.valueOf(groupByDataMap.get("columnName")))) {
                                String groupByColumn = String.valueOf(groupByDataMap.get("columnName"));
                                if (groupByColumn.contains(oldTableName)) {
                                    groupByColumn = groupByColumn.replaceAll(oldTableName, newTableName);
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
                }//groupByDataMapList end
                List<String> joinClauseDataMapList = (List) trfmRulesDataMap.get("joinClauseData");
                List<String> childTables = (List) trfmRulesDataMap.get("childTables");
                if (childTables != null && !childTables.isEmpty()
                        && joinClauseDataMapList != null && !joinClauseDataMapList.isEmpty()) {
                    JSONObject joinQueryMapObj = new JSONObject();
                    String joinQuery = "";
                    String masterTableName = (String) trfmRulesDataMap.get("masterTableName");
                    if (masterTableName != null
                            && !"".equalsIgnoreCase(masterTableName)
                            && masterTableName.equalsIgnoreCase(oldTableName)) {
                        masterTableName = newTableName;
                    }
                    joinQuery += " " + masterTableName;
                    joinQueryMapObj.put(masterTableName, masterTableName);
                    for (int i = 0; i < childTables.size(); i++) {
                        String childTableName = childTables.get(i);
                        if (childTableName != null
                                && !"".equalsIgnoreCase(childTableName)
                                && childTableName.equalsIgnoreCase(oldTableName)) {
                            childTableName = newTableName;
                        }
                        String childJoinStr = joinClauseDataMapList.get(i);
                        if (childJoinStr != null
                                && !"".equalsIgnoreCase(childJoinStr)
                                && !"null".equalsIgnoreCase(childJoinStr)) {
                            childJoinStr = childJoinStr.replaceAll(oldTableName, newTableName);
                            JSONObject joinObj = (JSONObject) JSONValue.parse(childJoinStr);
                            if (joinObj != null && !joinObj.isEmpty()) {
                                joinQueryMapObj.put(childTableName, joinObj);
                                int j = 0;
                                for (Object joinObjKey : joinObj.keySet()) {
                                    JSONObject joinMappedColumnObj = (JSONObject) joinObj.get(joinObjKey);
                                    if (joinMappedColumnObj != null
                                            && !joinMappedColumnObj.isEmpty()) {
                                        String childTableColumn = "";
                                        if (joinMappedColumnObj.get("childTableColumn") != null
                                                && !"".equalsIgnoreCase(String.valueOf(joinMappedColumnObj.get("childTableColumn")))
                                                && !"null".equalsIgnoreCase(String.valueOf(joinMappedColumnObj.get("childTableColumn")))) {//childTableColumn
                                            childTableColumn = String.valueOf(joinMappedColumnObj.get("childTableColumn")).replace(":", ".");
                                            if ("file".equalsIgnoreCase(dragType)) {
                                                childTableColumn = childTableColumn.trim().replaceAll(" ", "_");
                                            }
                                        }
                                        String masterTableColumn = "";
                                        if (joinMappedColumnObj.get("masterTableColumn") != null
                                                && !"".equalsIgnoreCase(String.valueOf(joinMappedColumnObj.get("masterTableColumn")))
                                                && !"null".equalsIgnoreCase(String.valueOf(joinMappedColumnObj.get("masterTableColumn")))) {//childTableColumn
                                            masterTableColumn = String.valueOf(joinMappedColumnObj.get("masterTableColumn")).replace(":", ".");
                                            if ("file".equalsIgnoreCase(dragType)) {
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
                }// join Query Map Obj End
                JSONObject appendValObj = new JSONObject();
                //                JSONObject colsObj = new JSONObject(); 
                Map colsObj = new LinkedHashMap(); // ravi etl integration
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
                            if (fromTableColumn != null
                                    && !"".equalsIgnoreCase(fromTableColumn)
                                    && !"null".equalsIgnoreCase(fromTableColumn) //  && !toTableColumnName.contains("N/A")
                                    ) {
                                fromTableColumn = fromTableColumn.replaceAll(oldTableName, newTableName);
                                if ("file".equalsIgnoreCase(dragType)) {
                                    fromTableColumn = fromTableColumn.trim().replaceAll(" ", "_");
                                }
                            }
                            if (toTableColumnName != null
                                    && !"".equalsIgnoreCase(toTableColumnName)
                                    && !"null".equalsIgnoreCase(toTableColumnName) //  && !toTableColumnName.contains("N/A")
                                    ) {
                                toTableColumnName = toTableColumnName.replaceAll(oldTableName, newTableName);
                                if ("file".equalsIgnoreCase(dragType)) {
                                    toTableColumnName = toTableColumnName.trim().replaceAll(" ", "_");
                                }
                            }
                            if (toTableColumnName != null
                                    && !"".equalsIgnoreCase(toTableColumnName)
                                    && !"null".equalsIgnoreCase(toTableColumnName) //  && !toTableColumnName.contains("N/A")
                                    ) {

                                if (fromTableColumn != null
                                        && !"".equalsIgnoreCase(fromTableColumn)
                                        && !"null".equalsIgnoreCase(fromTableColumn)
                                        && !fromTableColumn.contains("N/A")) {

                                    colsObj.put((toTableColumnName.contains(":") ? toTableColumnName.split(":")[1] : toTableColumnName), fromTableColumn);
                                    if (appendValue != null
                                            && !"".equalsIgnoreCase(appendValue)
                                            && !"null".equalsIgnoreCase(appendValue)) {
                                        appendValObj.put(fromTableColumn, appendValue);
                                    }
                                } else if (columnClause != null
                                        && !"".equalsIgnoreCase(columnClause)
                                        && !"null".equalsIgnoreCase(columnClause)) {
                                    columnClauseObj.put((toTableColumnName.contains(":") ? toTableColumnName.split(":")[1] : toTableColumnName), columnClause);
                                }
                            } else if (toTableColumnName != null
                                    && !"".equalsIgnoreCase(toTableColumnName)
                                    && !"null".equalsIgnoreCase(toTableColumnName)
                                    && toTableColumnName.contains("N/A")) {

                                colsObj.put(fromTableColumn, fromTableColumn);
                            }
                            if (defaultValue != null
                                    && !"".equalsIgnoreCase(defaultValue)
                                    && !"null".equalsIgnoreCase(defaultValue)) {
                                if (toTableColumnName != null
                                        && !"".equalsIgnoreCase(toTableColumnName)
                                        && !"null".equalsIgnoreCase(toTableColumnName) //  && !toTableColumnName.contains("N/A")
                                        ) {
                                    defaultValObj.put((toTableColumnName.contains(":") ? toTableColumnName.split(":")[1] : toTableColumnName), defaultValue);
                                } else if (fromTableColumn != null
                                        && !"".equalsIgnoreCase(fromTableColumn)
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
                }//
                List<String> whereClauseDataList = (List) trfmRulesDataMap.get("whereClauseData");
                JSONObject whereClauseObject = new JSONObject();
                if (whereClauseDataList != null && !whereClauseDataList.isEmpty()) {
                    for (int i = 0; i < whereClauseDataList.size(); i++) {
                        String whereClauseDataStr = whereClauseDataList.get(i);
                        if (whereClauseDataStr != null
                                && !"".equalsIgnoreCase(whereClauseDataStr)
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
                                        if (columnName != null
                                                && !"".equalsIgnoreCase(columnName)
                                                && !"null".equalsIgnoreCase(columnName)) {
                                            columnName = columnName.replaceAll(oldTableName, newTableName);
                                            tableName = columnName.split(":")[0];
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
                                }

                            }
                        }

                    }
                    paramMap.put("whereClauseObj", whereClauseObject);
                }//whereClauseDataList End

                JSONObject selectTabObj = (JSONObject) trfmRulesDataMap.get("selectTabObj");
                if (selectTabObj != null && !selectTabObj.isEmpty()) {
                    paramMap.put("selectTabObj", selectTabObj);
                } else {
                    paramMap.put("selectTabObj", new JSONObject());
                }// end selectTabObj 

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

    // for executing job if the sources(from) Data Base tables
//    public JSONObject processBapiJob(JCO.Function function, JCO.Client sapClient, HashMap sessionMap,
//            String loginUserName,
//            String loginOrgnId,
//            Map toOperator,
//            Map transformationMap,
//            Connection fromConnection,
//            List<Map> fromOperatorList,
//            String jobId
//    ) {
//        JSONObject resultObj = new JSONObject();
//        Connection toConnection = null;
//        JCO.Client toJCOConnection = null;
//        PreparedStatement toPreparedStatement = null;
//        List totalDataList = new ArrayList();
//        String responseObj = "";
//        try {
//            if (transformationMap != null) {
//                //JSONObject columnsObj = (JSONObject) transformationMap.get("columnsObj");
//                Map columnsObj = (LinkedHashMap) transformationMap.get("columnsObj"); // ravi etl integration
//                JSONObject tablesWhereClauseObj = (JSONObject) transformationMap.get("whereClauseObj");//
//                JSONObject joinQueryMapObj = (JSONObject) transformationMap.get("joinQueryMapObj");//
//                JSONObject orderByObj = (JSONObject) transformationMap.get("orderBy");//
//                JSONObject appendValObj = (JSONObject) transformationMap.get("appendValObj");//
//                JSONObject columnClauseObj = (JSONObject) transformationMap.get("columnClauseObj");//
//                JSONObject selectTabObj = (JSONObject) transformationMap.get("selectTabObj");
//                JSONObject defaultValuesObj = (JSONObject) transformationMap.get("defaultValObj");
//                JSONObject normalizeOptionsObj = (JSONObject) transformationMap.get("normalizeOptionsObj");
//                String joinQuery = (String) transformationMap.get("joinQuery");//
//                String groupByQuery = (String) transformationMap.get("groupBy");
//                String nativeSQL = "";
//                JSONObject orderAndGroupByObj = new JSONObject();
//                orderAndGroupByObj.put("orderByObj", orderByObj);
//                orderAndGroupByObj.put("groupByQuery", groupByQuery);
//                orderAndGroupByObj.put("nativeSQL", nativeSQL);
//                if (!(selectTabObj != null && !selectTabObj.isEmpty())) {
//                    selectTabObj = new JSONObject();
//                }
//                selectTabObj.put("jobId", jobId);
//                if (columnsObj != null && !columnsObj.isEmpty()) {
//                    try {
//                        genericProcessETLDataService.processETLLog(loginUserName,
//                                loginOrgnId,
//                                "Reading the transformations rules.",
//                                "INFO", 10, "Y", jobId);
//                    } catch (Exception e) {
//                    }
//                    //JSONObject fromColumnsObj = new JSONObject();
//                    Map fromColumnsObj = new LinkedHashMap();// ravi etl integration
//                    //JSONObject toColumnsObj = new JSONObject();
//                    Map toColumnsObj = new LinkedHashMap(); // ravi etl integration
//                    for (Object toColumnName : columnsObj.keySet()) {
//                        if (toColumnName != null && columnsObj.get(toColumnName) != null) {
//                            String fromColumnStr = (String) columnsObj.get(toColumnName);
//                            if (fromColumnStr != null
//                                    && !"".equalsIgnoreCase(fromColumnStr)
//                                    && !"null".equalsIgnoreCase(fromColumnStr)) {
//                                String[] fromColumnArray = fromColumnStr.split(":");
//                                if (fromColumnArray != null && fromColumnArray.length != 0) {
//                                    if (fromColumnsObj != null && !fromColumnsObj.isEmpty()) {
//                                        if (fromColumnsObj.containsKey(fromColumnArray[0])) {
//                                            fromColumnsObj.put(fromColumnArray[0],
//                                                    (fromColumnsObj.get(fromColumnArray[0]) + "," + fromColumnArray[1]));
//                                        } else {
//                                            fromColumnsObj.put(fromColumnArray[0],
//                                                    (fromColumnArray[1]));
//                                        }
//                                    } else {
//                                        fromColumnsObj.put(fromColumnArray[0], (fromColumnArray[1]));
//                                    }
//                                }
//                            }
//
//                        }
//
//                    }
//                    Set<String> toColumns = new HashSet<>();
//                    toColumns.addAll(columnsObj.keySet());
//                    if (defaultValuesObj != null && !defaultValuesObj.isEmpty()) {
//                        Set<String> defaultColumns = defaultValuesObj.keySet();
//                        if (defaultColumns != null
//                                && !defaultColumns.isEmpty()
//                                && toColumns != null && !toColumns.isEmpty()) {
//                            toColumns.addAll(defaultColumns);
//                        }
//                    }
//                    if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
//                        Set<String> columnClauseCols = columnClauseObj.keySet();
//                        if (columnClauseCols != null
//                                && !columnClauseCols.isEmpty()
//                                && toColumns != null && !toColumns.isEmpty()) {
//                            toColumns.addAll(columnClauseCols);
//                        }
//                    }
//
//                    List<String> toColumnsList = new ArrayList<>();
//                    toColumnsList.addAll(toColumns);
//                    String toTableName = (String) toOperator.get("tableName");
//                    int totalDataCount = 0;
//                    int fileDataLastIndex = 0;
//
//                    List<Map> nonJoinOpList = fromOperatorList;
//                    String fileName = "";
//                    String orginalName = (String) toOperator.get("userFileName");
//                    String iconType = (String) toOperator.get("iconType");
//                    JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
//                    String timeStamp = String.valueOf(toOperator.get("timeStamp"));// ravi etl issues new
//                    if (!(timeStamp != null
//                            && !"".equalsIgnoreCase(timeStamp)
//                            && !"null".equalsIgnoreCase(timeStamp))) {
//                        timeStamp = "" + System.currentTimeMillis();
//                        toOperator.put("timeStamp", timeStamp);
//                    }
//                    if ("XLSX".equalsIgnoreCase(iconType)) {
//                        fileName = "V10ETLExport_" + timeStamp + ".xlsx";
//
//                    } else if ("XLS".equalsIgnoreCase(iconType)) {
//                        fileName = "V10ETLExport_" + timeStamp + ".xls";
//                    } else if ("XML".equalsIgnoreCase(iconType)) {
//                        fileName = "V10ETLExport_" + timeStamp + ".xml";
//                    } else if ("CSV".equalsIgnoreCase(iconType)) {
//                        fileName = "V10ETLExport_" + timeStamp + ".csv";
//                    } else if ("TXT".equalsIgnoreCase(iconType)) {
//                        fileName = "V10ETLExport_" + timeStamp + ".txt";
//                    } else if ("JSON".equalsIgnoreCase(iconType)) {
//                        fileName = "V10ETLExport_" + timeStamp + ".json";
//                    } else {
//                        if (toTableName != null
//                                && !"".equalsIgnoreCase(String.valueOf(toTableName))
//                                && !"null".equalsIgnoreCase(String.valueOf(toTableName))) {
//                            Object toConnectionObj = getConnection(toConnObj);
//                            selectTabObj.put("toConnectionObj", toConnectionObj);
//                            if (toConnectionObj instanceof Connection) {
//                                toConnection = (Connection) toConnectionObj;
//                            }
//
//                        }
//                    }
//                    if (!(toColumnsList != null && !toColumnsList.isEmpty())) {
//                        toColumnsList.addAll(columnsObj.keySet());
//                    }
//                    if (joinQueryMapObj != null
//                            && !joinQueryMapObj.isEmpty()) { //  joins
//
//                        nonJoinOpList = fromOperatorList.stream()
//                                .filter(opMap -> (opMap != null && !opMap.isEmpty()
//                                && opMap.get("tableName") != null
//                                && !"".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
//                                && !"null".equalsIgnoreCase(String.valueOf(opMap.get("tableName")))
//                                && !joinQueryMapObj.containsKey(String.valueOf(opMap.get("tableName"))))).collect(Collectors.toList());
//                        try {
//                            genericProcessETLDataService.processETLLog(loginUserName,
//                                    loginOrgnId, "Starting extract join tables data.", "INFO", 10, "Y", jobId);
//                        } catch (Exception e) {
//                        }
//                        totalDataList = genericProcessETLDataService.processETLBapiJoinData(loginUserName,
//                                loginOrgnId,
//                                fromConnection,
//                                toConnection,
//                                fromOperatorList,
//                                toOperator,
//                                columnsObj,
//                                fromColumnsObj,
//                                tablesWhereClauseObj,
//                                defaultValuesObj,
//                                toPreparedStatement,
//                                1,
//                                1000,
//                                totalDataCount,
//                                toColumnsList,
//                                joinQueryMapObj,
//                                joinQuery,
//                                fileDataLastIndex,
//                                fileName,
//                                1,
//                                orderAndGroupByObj,
//                                columnClauseObj,
//                                selectTabObj,
//                                normalizeOptionsObj
//                        );
//
//                    } else { // non joins
//                        if (nonJoinOpList != null && !nonJoinOpList.isEmpty()) {
//                            try {
//                                genericProcessETLDataService.processETLLog(loginUserName,
//                                        loginOrgnId, "Starting extract non-join tables data.", "INFO", 10, "Y", jobId);
//                            } catch (Exception e) {
//                            }
//                            for (int i = 0; i < nonJoinOpList.size(); i++) {
//                                Map fromOperator = nonJoinOpList.get(i);
//                                if (fromOperator != null && !fromOperator.isEmpty()) {
//                                    totalDataList = genericProcessETLDataService.processETLBapiData(loginUserName,
//                                            loginOrgnId,
//                                            fromConnection,
//                                            toConnection,
//                                            fromOperator,
//                                            toOperator,
//                                            columnsObj,
//                                            fromColumnsObj,
//                                            tablesWhereClauseObj,
//                                            defaultValuesObj,
//                                            toPreparedStatement,
//                                            1,
//                                            1000,
//                                            totalDataCount,
//                                            toColumnsList,
//                                            fileDataLastIndex,
//                                            fileName,
//                                            1,
//                                            orderAndGroupByObj,
//                                            columnClauseObj,
//                                            selectTabObj,
//                                            normalizeOptionsObj
//                                    );
//                                }
//
//                            }
//                        }
//
//                    }
//                    List<String> columnsList = new ArrayList();
//                    if (fromColumnsObj != null && !fromColumnsObj.isEmpty()) {
//                        for (Object tableName : fromColumnsObj.keySet()) {
//                            if (tableName != null && fromColumnsObj.get(tableName) != null) {
//                                String colsObj = (String) fromColumnsObj.get(tableName);
//                                if (colsObj != null && !colsObj.isEmpty()) {
////                            String query = "SELECT " + colsObj;
//                                    columnsList = new ArrayList(Arrays.asList(colsObj.split(",")));
//
//                                }
//                            }
//                        }
//                    }
//
//                    for (int j = 0; j < totalDataList.size(); j++) {
//
//                        HashMap rowData = (HashMap) totalDataList.get(j);
//                        JSONObject dataMap = new JSONObject(rowData);
////                        for (int k = 0; k < rowData.size(); k++) {
////                            dataMap.put(columnsList.get(k), rowData[k]);
////                        }
//                        dataMap.put("ORGN_ID", sessionMap.get("ssOrgId"));
//                        dataMap.put("DOMAIN", sessionMap.get("domain")); //RAVI PREDEFINED
//
//                        if (toOperator != null && !toOperator.isEmpty()) {
//                            function = visionSOASAPInterface.transferToERP(function, sapClient, dataMap, sessionMap, toOperator);
//                            resultObj.put("function", function);
//                            resultObj.put("dataMap", dataMap);
//                        }
//
//                    }
//
//                }
//
//            }//end if transformationMap
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//
//            try {
//                if (toPreparedStatement != null) {
//                    toPreparedStatement.close();
//                }
//                if (toConnection != null) {
//                    toConnection.close();
//                }
//                if (toJCOConnection != null) {
//                    toJCOConnection.disconnect();
//                }
//            } catch (Exception e) {
//            }
//        }
//        return resultObj;
//    }

//    public JCO.Function setDataToBapiStructures(JCO.Function function, Map toOperator, JSONObject dataMap) {
//
//        List sourceList = new ArrayList(); //jagadish
//        String query = "";
//        try {
//            String trfmRulesId = (String) toOperator.get("trfmRulesId");
//            if (trfmRulesId != null) {
//// column mapping
//
//                String fromColumns = "";
//                String fromTable = "";
//                String defaultval = "";
//                String destinationTable = "";
//                List<Object[]> columnMappingList = genericDataPipingDAO.getcolumnMappingData(trfmRulesId);
//                if (columnMappingList != null && !columnMappingList.isEmpty()) {
//                    JSONArray columnMappingsDataArray = new JSONArray();
//
//                    for (int j = 0; j < columnMappingList.size(); j++) {
//                        JSONObject rowData = new JSONObject();
//                        String sourceColumn = (columnMappingList.get(j)[1] != null ? (String) columnMappingList.get(j)[1] : "");
//                        String sourceTable = (columnMappingList.get(j)[0] != null ? (String) columnMappingList.get(j)[0] : "");
//                        destinationTable = (columnMappingList.get(j)[2] != null ? (String) columnMappingList.get(j)[2] : "");
//                        String destinationColumn = (columnMappingList.get(j)[2] != null ? (String) columnMappingList.get(j)[2] + ":" : "") + (columnMappingList.get(j)[3] != null ? (String) columnMappingList.get(j)[3] : "");
//                        String defaultValue = (columnMappingList.get(j)[4] != null ? (String) columnMappingList.get(j)[4] : "");
//                        String appendValue = (columnMappingList.get(j)[5] != null ? (String) columnMappingList.get(j)[5] : "");
//                        String columnClause = (columnMappingList.get(j)[6] != null ? (String) columnMappingList.get(j)[6] : "");
//                        if (!(defaultValue != null && !"".equalsIgnoreCase(defaultValue))) {
//                            if (j != (columnMappingList.size() - 1)) {
//                                fromColumns += sourceColumn + ",";
//                            } else {
//                                fromColumns += sourceColumn;
//                            }
//
//                            fromTable = sourceTable;
//                        } else {
//                            defaultval = defaultValue;
//                        }
//
//                    }
//
//                    List<Object[]> masterTableList = new ArrayList();
//                    String whereClauseCondition = "";
//                    if (!(defaultval != null && !"".equalsIgnoreCase(defaultval))) {
//                        query = "SELECT " + fromColumns + " FROM " + fromTable;
//
//                        masterTableList = genericDataPipingDAO.getcDmMasterTablesList(trfmRulesId);
//                    } else {
//                        if (defaultval != null && !"".equalsIgnoreCase(defaultval)
//                                && (defaultval.contains("<--") && defaultval.contains("-->")) || (defaultval.contains("<<-") && defaultval.contains("->>"))) {
//                            defaultval = jslProcessDAO.replaceValues(defaultval, dataMap, new HashMap());
//                            Map map = new HashMap();
//                            List defaultVlaueList = access.sqlqueryWithParams(defaultval, map);
//                            if (defaultVlaueList != null && defaultVlaueList.size() > 0) {
//                                defaultval = (String) defaultVlaueList.get(0);
//                            }
//                        }
//                    }
//
//                    if (masterTableList != null && !masterTableList.isEmpty()) {
//                        for (int i = 0; i < masterTableList.size(); i++) {
//
//// join clauses
//                            String joinTableId = (String) masterTableList.get(i)[1];
//                            JSONArray childTables = new JSONArray();
//                            if (joinTableId != null) {
//                                List<Object[]> joinTableList = genericDataPipingDAO.getcDmJoinTablesList(joinTableId, null);
//                                JSONArray joinClauseData = new JSONArray();
//
//                                if (joinTableList != null && !joinTableList.isEmpty()) {
//                                    for (int k = 0; k < joinTableList.size(); k++) {
//                                        String joinColId = (String) joinTableList.get(k)[1];
//                                        if (k == 0) {
//                                            String childTableName = (String) joinTableList.get(k)[3];
//                                            childTables.add(childTableName);
//                                        }
//
//                                        List<Object[]> joinTableColumnList = genericDataPipingDAO.getcDmJoinColumnsList(joinColId);
//                                        for (int m = 0; m < joinTableColumnList.size(); m++) {
//
//                                            String childTableColumn = (joinTableColumnList.get(m)[3] != null ? (String) joinTableColumnList.get(m)[3] : "");
//                                            if (childTableColumn != null) {
//                                                String dataMapCol = childTableColumn.split("\\.")[1];
//                                                String dataMapVal = (String) dataMap.get(dataMapCol);
//                                                if (dataMapVal != null) {
//                                                    if (m != (joinTableColumnList.size() - 1)) {
//                                                        whereClauseCondition += childTableColumn + " = '" + dataMapVal + "' AND ";
//                                                    } else {
//                                                        whereClauseCondition += childTableColumn + " = '" + dataMapVal + "'";
//                                                    }
//                                                }
//                                            }
//
//                                        }
//
//                                    }
//
//                                }
//
//                            }
//
//                        }
//
//                    }
//                    List sourceDataList = new ArrayList();
//                    if (!"".equalsIgnoreCase(defaultval)) {
//                        Object[] rowData = new Object[1];
//                        rowData[0] = defaultval;
//// String[] fromColumsArray = fromColumns.split(","); jagadish
//                        JSONObject dataObj = new JSONObject();
//
////dataObj.put(fromColumsArray[0], rowData[0]); jagadish
//                        dataObj.put(destinationTable, rowData[0]); //jagadish
//
//                        sourceDataList.add(dataObj);
//// sourceList.add(defaultval);
//                    } else {
//                        if (whereClauseCondition != null && !"".equalsIgnoreCase(whereClauseCondition)) {
//                            whereClauseCondition = " WHERE " + whereClauseCondition;
//                        }
//                        query = query + whereClauseCondition;
//                        Map map = new HashMap();
//                        sourceList = access.sqlqueryWithParams(query, map);
//                        for (int i = 0; i < sourceList.size(); i++) { //jagadish
//                            String[] fromColumsArray = fromColumns.split(",");
//                            JSONObject dataObj = new JSONObject();
//                            if (fromColumsArray != null && fromColumsArray.length > 0) {
//                                if (sourceList.get(i) != null && sourceList.get(i) instanceof String) {
//                                    String strData = (String) sourceList.get(i);
//                                    for (int j = 0; j < fromColumsArray.length; j++) {
//                                        dataObj.put(fromColumsArray[j], strData);
//                                    }
//                                } else {
//                                    Object[] rowData = (Object[]) sourceList.get(i);
//                                    if (rowData != null && rowData.length > 0) {
//                                        for (int j = 0; j < fromColumsArray.length; j++) {
//                                            dataObj.put(fromColumsArray[j], rowData[j]);
//                                        }
//                                    }
//                                }
//                            }
//                            sourceDataList.add(dataObj);
//                        }
//                    }
//
//                    for (int i = 0; i < sourceDataList.size(); i++) {
//                        if (sourceDataList != null && !sourceDataList.isEmpty()) {
//
//                            JSONObject dataMapObj = (JSONObject) sourceDataList.get(i);
//                            if (dataMapObj.size() > 1) {
//                                JCO.Table table = function.getTableParameterList().getTable(destinationTable);
//                                if ("ALLOCVALUESCHAR".equalsIgnoreCase(destinationTable)) {
//                                    table.appendRow();
//                                    for (Object key : dataMapObj.keySet()) {
//                                        Object value = dataMapObj.get(key);
//                                        if (key != null && "PROPERTY_CONCEPT_ID".equalsIgnoreCase(String.valueOf(key))) {
//                                            table.setValue(value, "CHARACT");
//                                        } else if (key != null && "PROPERTY_VALUE1".equalsIgnoreCase(String.valueOf(key))) {
//                                            if ("N/A".equalsIgnoreCase((String) value)) {
//                                                table.setValue("", "VALUE_CHAR");
//                                            } else {
//                                                table.setValue(value, "VALUE_CHAR");
//                                            }
//                                        }
//                                    }
//                                } else if ("CHARACTERISTICS".equalsIgnoreCase(destinationTable)) {
//                                    table.appendRow();
//                                    for (Object key : dataMapObj.keySet()) {
//                                        Object value = dataMapObj.get(key);
//                                        if (key != null && "PROPERTY_CONCEPT_ID".equalsIgnoreCase(String.valueOf(key))) {
//                                            table.setValue(value, "CHARACT_NAME");
//                                        } else if (key != null && "PROPERTY_NAME".equalsIgnoreCase(String.valueOf(key))) {
//                                            table.setValue(value, "DESCRIPTION");
//                                        }
//                                    }
//                                } else {
//                                    table.appendRow();
//                                    for (Object key : dataMapObj.keySet()) {
//                                        Object value = dataMapObj.get(key);
//                                        if (value != null) {
//                                            table.setValue(value, String.valueOf(key));
//                                        }
//                                    }
//                                }
//                                function.getTableParameterList().setValue(table, destinationTable);
//                            } else if (dataMapObj.size() == 1) {
//                                for (Object key : dataMapObj.keySet()) {
//                                    Object value = dataMapObj.get(key);
//                                    if (value != null) {
//
//                                        if ("MATERIALDESCRIPTION".equalsIgnoreCase(destinationTable)
//                                                || "SERVICE_DESCRIPTION".equalsIgnoreCase(destinationTable)
//                                                || "MATERIALLONGTEXT".equalsIgnoreCase(destinationTable)
//                                                || "SERVICE_LONG_TEXTS".equalsIgnoreCase(destinationTable)) {
//                                            JCO.Table table = function.getTableParameterList().getTable(destinationTable);
//                                            int splitValue = 0;
//                                            if ("MATERIALLONGTEXT".equalsIgnoreCase(destinationTable)) {
//                                                splitValue = 40;
//                                            }
//                                            if (splitValue > 0) {
//// long Description
//                                                String[] longDescr = splitDescrUtils.splitLongDescr((String) value, splitValue);
//                                                if (longDescr != null && longDescr.length != 0) {
//                                                    for (int j = 0; j < longDescr.length; j++) {
//                                                        table.appendRow();
//                                                        table.setValue("EN", "LANGU_ISO");
//                                                        table.setValue(longDescr[j], "TEXT_LINE");
//                                                    }
//                                                }
//
//                                            } else {
//                                                table.appendRow();
//                                                table.setValue("EN", "LANGU_ISO");
//                                                table.setValue(value, "MATL_DESC");
//
//                                            }
//                                            function.getTableParameterList().setValue(table, destinationTable);
//                                        } else {
//                                            function.getImportParameterList().setValue(value, destinationTable);
//                                        }
//                                    }
//
//                                }
//
//                            }
//
//                        }
//                    }
//
//                }
//
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return function;
//    }

//    public JCO.Function setDataToBapiStructures(JCO.Function function, Map toOperator, JSONObject dataMap) {
//
//        List<Object[]> sourceList = new ArrayList();
//        String query = "";
//        try {
//            String trfmRulesId = (String) toOperator.get("trfmRulesId");
//            if (trfmRulesId != null) {
//                // column mapping
//
//                String fromColumns = "";
//                String fromTable = "";
//                String defaultval = "";
//                String destinationTable = "";
//                List<Object[]> columnMappingList = genericDataPipingDAO.getcolumnMappingData(trfmRulesId);
//                if (columnMappingList != null && !columnMappingList.isEmpty()) {
//                    JSONArray columnMappingsDataArray = new JSONArray();
//
//                    for (int j = 0; j < columnMappingList.size(); j++) {
//                        JSONObject rowData = new JSONObject();
//                        String sourceColumn = (columnMappingList.get(j)[1] != null ? (String) columnMappingList.get(j)[1] : "");
//                        String sourceTable = (columnMappingList.get(j)[0] != null ? (String) columnMappingList.get(j)[0] : "");
//                        destinationTable = (columnMappingList.get(j)[2] != null ? (String) columnMappingList.get(j)[2] : "");
//                        String destinationColumn = (columnMappingList.get(j)[2] != null ? (String) columnMappingList.get(j)[2] + ":" : "") + (columnMappingList.get(j)[3] != null ? (String) columnMappingList.get(j)[3] : "");
//                        String defaultValue = (columnMappingList.get(j)[4] != null ? (String) columnMappingList.get(j)[4] : "");
//                        String appendValue = (columnMappingList.get(j)[5] != null ? (String) columnMappingList.get(j)[5] : "");
//                        String columnClause = (columnMappingList.get(j)[6] != null ? (String) columnMappingList.get(j)[6] : "");
//                        if (!(defaultValue != null && !"".equalsIgnoreCase(defaultValue))) {
//                            if (j != (columnMappingList.size() - 1)) {
//                                fromColumns += sourceColumn + ",";
//                            } else {
//                                fromColumns += sourceColumn;
//                            }
//
//                            fromTable = sourceTable;
//                        } else {
//                            defaultval = defaultValue;
//                        }
//
//                    }
//                    
//                    List<Object[]> masterTableList = new ArrayList();
//                    String whereClauseCondition = "";
//                   if (!(defaultval != null && !"".equalsIgnoreCase(defaultval))){
//                   query = "SELECT " + fromColumns + " FROM " + fromTable;
//                    
//                    masterTableList = genericDataPipingDAO.getcDmMasterTablesList(trfmRulesId);
//                   }
//                    
//
//                    if (masterTableList != null && !masterTableList.isEmpty()) {
//                        for (int i = 0; i < masterTableList.size(); i++) {
//
//                            // join clauses
//                            String joinTableId = (String) masterTableList.get(i)[1];
//                            JSONArray childTables = new JSONArray();
//                            if (joinTableId != null) {
//                                List<Object[]> joinTableList = genericDataPipingDAO.getcDmJoinTablesList(joinTableId, null);
//                                JSONArray joinClauseData = new JSONArray();
//
//                                if (joinTableList != null && !joinTableList.isEmpty()) {
//
//                                    for (int k = 0; k < joinTableList.size(); k++) {
//                                        String joinColId = (String) joinTableList.get(k)[1];
//                                        if (k == 0) {
//                                            String childTableName = (String) joinTableList.get(k)[3];
//                                            childTables.add(childTableName);
//                                        }
//
//                                        List<Object[]> joinTableColumnList = genericDataPipingDAO.getcDmJoinColumnsList(joinColId);
//                                        for (int m = 0; m < joinTableColumnList.size(); m++) {
//
//                                            String childTableColumn = (joinTableColumnList.get(m)[3] != null ? (String) joinTableColumnList.get(m)[3] : "");
//                                            if (childTableColumn != null) {
//                                                String dataMapCol = childTableColumn.split("\\.")[1];
//                                                String dataMapVal = (String) dataMap.get(dataMapCol);
//                                                if (dataMapVal != null) {
//                                                    if (m != (joinTableColumnList.size() - 1)) {
//                                                        whereClauseCondition += childTableColumn + " = '" + dataMapVal + "' AND ";
//                                                    } else {
//                                                        whereClauseCondition += childTableColumn + " = '" + dataMapVal + "'";
//                                                    }
//                                                }
//                                            }
//
//                                        }
//
//                                    }
//
//                                }
//
//                            }
//
//                        }
//
//                    }
//                    List sourceDataList = new ArrayList();
//                    if (!"".equalsIgnoreCase(defaultval)) {
//                        Object[] rowData = new Object[1];
//                        rowData[0] = defaultval;
//                        String[] fromColumsArray = fromColumns.split(",");
//                        JSONObject dataObj = new JSONObject();
//
//                        dataObj.put(fromColumsArray[0], rowData[0]);
//
//                        sourceDataList.add(dataObj);
//                        //    sourceList.add(defaultval);
//                    } else {
//                        if (whereClauseCondition != null && !"".equalsIgnoreCase(whereClauseCondition)) {
//                            whereClauseCondition = " WHERE " + whereClauseCondition;
//                        }
//                        query = query + whereClauseCondition;
//                        Map map = new HashMap();
//                        sourceList = access.sqlqueryWithParams(query, map);
//                        for (int i = 0; i < sourceList.size(); i++) {
//                            Object[] rowData = sourceList.get(i);
//                            String[] fromColumsArray = fromColumns.split(",");
//                            JSONObject dataObj = new JSONObject();
//                            for (int j = 0; j < fromColumsArray.length; j++) {
//                                dataObj.put(fromColumsArray[j], rowData[j]);
//                            }
//                            sourceDataList.add(dataObj);
//                        }
//                    }
//
//                    for (int i = 0; i < sourceDataList.size(); i++) {
//                        if (sourceDataList != null && !sourceDataList.isEmpty()) {
//
//                            JSONObject dataMapObj = (JSONObject) sourceDataList.get(i);
//                            if (dataMapObj.size() > 1) {
//                                JCO.Table table = function.getTableParameterList().getTable(destinationTable);
//                                for (Object key : dataMapObj.keySet()) {
//                                    Object value = dataMapObj.get(key);
//                                    if (value != null) {
//                                        table.appendRow();
//                                        table.setValue(value, String.valueOf(key));
//                                    }
//                                   
//                                }
//                                 function.getTableParameterList().setValue(table, destinationTable);
//                            } else if (dataMapObj.size() == 1) {
//                                for (Object key : dataMapObj.keySet()) {
//                                    Object value = dataMapObj.get(key);
//                                    if (value != null) {
//                                        function.getImportParameterList().setValue(value, destinationTable);
//                                    }
//
//                                }
//
//                            }
//
//                        }
//                    }
//
//                }
//
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return function;
//    }
//    public Map getSavedTransformationRules(Map toOperator) {
//        Map trfmRulesDataMap = new HashMap();
//
//        try {
//            String trfmRulesId = (String) toOperator.get("trfmRulesId");
//            if (trfmRulesId != null) {
//                // column mapping
//                List<Object[]> columnMappingList = genericDataPipingDAO.getcolumnMappingData(trfmRulesId);
//                if (columnMappingList != null && !columnMappingList.isEmpty()) {
//                    JSONArray columnMappingsDataArray = new JSONArray();
//                    for (int j = 0; j < columnMappingList.size(); j++) {
//                        JSONObject rowData = new JSONObject();
//                        String sourceColumn = (columnMappingList.get(j)[0] != null ? (String) columnMappingList.get(j)[0] + ":" : "") + (columnMappingList.get(j)[1] != null ? (String) columnMappingList.get(j)[1] : "");
//                        String destinationColumn = (columnMappingList.get(j)[2] != null ? (String) columnMappingList.get(j)[2] + ":" : "") + (columnMappingList.get(j)[3] != null ? (String) columnMappingList.get(j)[3] : "");
//                        String defaultValue = (columnMappingList.get(j)[4] != null ? (String) columnMappingList.get(j)[4] : "");
//                        String appendValue = (columnMappingList.get(j)[5] != null ? (String) columnMappingList.get(j)[5] : "");
//                        String columnClause = (columnMappingList.get(j)[6] != null ? (String) columnMappingList.get(j)[6] : "");
//
//                        rowData.put("sourceColumn", sourceColumn);
//                        rowData.put("destinationColumn", destinationColumn);
//                        rowData.put("defaultValue", defaultValue);
//                        rowData.put("appendValue", appendValue);
//                        rowData.put("columnClause", columnClause);
//
//                        columnMappingsDataArray.add(rowData);
//                    }
//                    trfmRulesDataMap.put("colMappingsData", columnMappingsDataArray);
//                }
//
//                List<Object[]> masterTableList = genericDataPipingDAO.getcDmMasterTablesList(trfmRulesId);
//
//                if (masterTableList != null && !masterTableList.isEmpty()) {
//                    for (int i = 0; i < masterTableList.size(); i++) {
//                        String masterTableName = (String) masterTableList.get(i)[3];
//                        trfmRulesDataMap.put("masterTableName", masterTableName);
//
//                        // join clauses
//                        String joinTableId = (String) masterTableList.get(i)[1];
//                        JSONArray childTables = new JSONArray();
//                        if (joinTableId != null) {
//                            List<Object[]> joinTableList = genericDataPipingDAO.getcDmJoinTablesList(joinTableId, null);
//                            JSONArray joinClauseData = new JSONArray();
//
//                            if (joinTableList != null && !joinTableList.isEmpty()) {
//
//                                for (int k = 0; k < joinTableList.size(); k++) {
//                                    String joinColId = (String) joinTableList.get(k)[1];
//                                    String joinType = joinType = (String) joinTableList.get(k)[2];
//                                    if (k == 0) {
//                                        String childTableName = (String) joinTableList.get(k)[3];
//                                        childTables.add(childTableName);
//                                    }
//
//                                    List<Object[]> joinTableColumnList = genericDataPipingDAO.getcDmJoinColumnsList(joinColId);
//                                    JSONObject joinColsObj = new JSONObject();
//                                    for (int m = 0; m < joinTableColumnList.size(); m++) {
//
//                                        JSONObject joinColsDetailsObj = new JSONObject();
//                                        String masterTableColumn = (joinTableColumnList.get(m)[1] != null ? (String) joinTableColumnList.get(m)[1] : "");
//                                        String operator = (joinTableColumnList.get(m)[2] != null ? (String) joinTableColumnList.get(m)[2] : "");
//                                        String childTableColumn = (joinTableColumnList.get(m)[3] != null ? (String) joinTableColumnList.get(m)[3] : "");
//                                        String andOrOperator = (joinTableColumnList.get(m)[4] != null ? (String) joinTableColumnList.get(m)[4] : "");
//                                        String staticValue = (joinTableColumnList.get(m)[5] != null ? (String) joinTableColumnList.get(m)[5] : "");
//                                        joinColsDetailsObj.put("masterTableColumn", masterTableColumn);
//                                        joinColsDetailsObj.put("operator", operator);
//                                        joinColsDetailsObj.put("childTableColumn", childTableColumn);
//                                        joinColsDetailsObj.put("andOrOperator", andOrOperator);
//                                        joinColsDetailsObj.put("staticValue", staticValue);
//                                        joinColsDetailsObj.put("joinType", joinType);
//                                        joinColsObj.put(m + 1, joinColsDetailsObj);
//                                    }
//                                    joinClauseData.add(joinColsObj.toJSONString());
//                                }
//
//                            }
//                            trfmRulesDataMap.put("joinClauseData", joinClauseData);
//
//                        }
//                        trfmRulesDataMap.put("childTables", childTables);
//
//                        // where clauses
//                        String whereClauseId = (String) masterTableList.get(i)[5];
//                        if (whereClauseId != null) {
//                            JSONArray whereClauseData = new JSONArray();
//                            List<Object[]> whereClausesList = genericDataPipingDAO.getCDmTableClausesList(whereClauseId);
//                            if (whereClausesList != null && !whereClausesList.isEmpty()) {
//                                JSONObject whereClauseObj = new JSONObject();
//                                for (int j = 0; j < whereClausesList.size(); j++) {
//                                    JSONObject whereClauseDetailsObj = new JSONObject();
//                                    String columnName = (whereClausesList.get(i)[2] != null ? (String) whereClausesList.get(i)[2] : "");
//                                    String operator = (whereClausesList.get(i)[3] != null ? (String) whereClausesList.get(i)[3] : "");
//                                    String andOrOperator = (whereClausesList.get(i)[5] != null ? (String) whereClausesList.get(i)[5] : "");
//                                    String staticValue = (whereClausesList.get(i)[4] != null ? (String) whereClausesList.get(i)[4] : "");
//                                    String tableName = (whereClausesList.get(i)[6] != null ? (String) whereClausesList.get(i)[6] : "");
//                                    whereClauseDetailsObj.put("columnName", columnName);
//                                    whereClauseDetailsObj.put("operator", operator);
//                                    whereClauseDetailsObj.put("andOrOperator", andOrOperator);
//                                    whereClauseDetailsObj.put("staticValue", staticValue);
//                                    whereClauseObj.put(j + 1, whereClauseDetailsObj);
//                                }
//                                whereClauseData.add(whereClauseObj.toJSONString());
//                            }
//                            trfmRulesDataMap.put("whereClauseData", whereClauseData);
//                        }
//
//                        // order by
//                        String orderClauseId = (String) masterTableList.get(i)[6];
//                        if (orderClauseId != null) {
//                            JSONArray orderByData = new JSONArray();
//                            List<Object[]> orderByClausesList = genericDataPipingDAO.getCDmTableClausesList(orderClauseId);
//                            if (orderByClausesList != null && !orderByClausesList.isEmpty()) {
//                                JSONObject orderByObj = new JSONObject();
//                                for (int j = 0; j < orderByClausesList.size(); j++) {
//                                    JSONObject orderByDetailsObj = new JSONObject();
//                                    String columnName = (orderByClausesList.get(i)[2] != null ? (String) orderByClausesList.get(i)[2] : "");
//                                    String order = (orderByClausesList.get(i)[3] != null ? (String) orderByClausesList.get(i)[3] : "");
//                                    orderByDetailsObj.put("columnName", columnName);
//                                    orderByDetailsObj.put("order", order);
//
//                                    orderByObj.put(j + 1, orderByDetailsObj);
//                                }
//                                orderByData.add(orderByObj);
//                            }
//                            trfmRulesDataMap.put("orderByData", orderByData);
//                        }
//
//                        // group by
//                        String groupClauseId = (String) masterTableList.get(i)[7];
//                        if (groupClauseId != null) {
//                            JSONArray groupByData = new JSONArray();
//                            List<Object[]> groupByClausesList = genericDataPipingDAO.getCDmTableClausesList(groupClauseId);
//                            if (groupByClausesList != null && !groupByClausesList.isEmpty()) {
//                                JSONObject groupByObj = new JSONObject();
//                                for (int j = 0; j < groupByClausesList.size(); j++) {
//                                    JSONObject groupByDetailsObj = new JSONObject();
//                                    String columnName = (groupByClausesList.get(i)[2] != null ? (String) groupByClausesList.get(i)[2] : "");
//                                    groupByDetailsObj.put("columnName", columnName);
//
//                                    groupByObj.put(j + 1, groupByDetailsObj);
//                                }
//                                groupByData.add(groupByObj);
//                            }
//                            trfmRulesDataMap.put("groupByData", groupByData);
//                        }
//                    }
//
//                }
//
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return trfmRulesDataMap;
//    }

    public JSONObject processJobSchedularDataHib(String loginUserName,
            String loginOrgnId,
            Map mappedData,
            Map mappingOperatorObj,
            String jobId,
            String mappedDataStr
    ) {
        JSONObject resultObj = new JSONObject();
        Connection fromConnection = null;
        Connection toConnection = null;
        JCO.Client fromJCOConnection = null;
        JCO.Client toJCOConnection = null;
        try {
            if (mappingOperatorObj != null && !mappingOperatorObj.isEmpty()) {
                Map operatorsMap = (Map) mappedData.get("operators");
                Map linksMap = (Map) mappedData.get("links");
                for (Object mappedKey : mappingOperatorObj.keySet()) {
                    if (linksMap != null && !linksMap.isEmpty()) {
                        List<Map> fromOperatorList = new ArrayList<>();
                        List<Map> toOperatorList = new ArrayList<>();
                        for (Object linkKey : linksMap.keySet()) {
                            Map linkMap = (Map) linksMap.get(linkKey);
                            if (linkMap != null
                                    && !linkMap.isEmpty()
                                    && String.valueOf(mappedKey)
                                            .equalsIgnoreCase(String.valueOf(linkMap.get("toOperator")))) {
//                                System.out.println("linkMap.get(\"fromOperator\")::" + linkMap.get("fromOperator"));
                                String fromOperatorId = String.valueOf(linkMap.get("fromOperator"));
                                Map fromOperator = (Map) operatorsMap.get(String.valueOf(linkMap.get("fromOperator")));

//                              fromOperator.put("operatorId", fromOperatorId);
                                fromOperatorList.add(fromOperator);

                            }
                            if (linkMap != null
                                    && !linkMap.isEmpty()
                                    && String.valueOf(mappedKey)
                                            .equalsIgnoreCase(String.valueOf(linkMap.get("fromOperator")))) {
                                String toOperatorId = String.valueOf(linkMap.get("toOperator"));
                                Map toOperator = (Map) operatorsMap.get(String.valueOf(linkMap.get("toOperator")));
//                                toOperator.put("operatorId", toOperatorId);
                                toOperatorList.add(toOperator);

                            }
                        }// end loop linksMap

                        if (fromOperatorList != null
                                && !fromOperatorList.isEmpty()
                                && toOperatorList != null
                                && !toOperatorList.isEmpty()) {

                            // ravi bapi code start
                            String jobType = (String) ((JSONObject) mappingOperatorObj.get(mappedKey)).get("jobType");
                            String bapiName = (String) ((JSONObject) mappingOperatorObj.get(mappedKey)).get("bapiName");
                            if (jobType != null && !"".equalsIgnoreCase(jobType) && "Bapi".equalsIgnoreCase(jobType)) {

                                // ravi new bapi code
                            } // ravi bapi code end
                            else {
                                for (int i = 0; i < toOperatorList.size(); i++) {
                                    Map toOperator = toOperatorList.get(i);
                                    if (toOperator != null && !toOperator.isEmpty()) {
                                        Map trfmRulesDataMap = (Map) toOperator.get("trfmRules-data");
                                        if (trfmRulesDataMap != null && !trfmRulesDataMap.isEmpty()) {
                                            if (i != 0) {
                                                fromOperatorList = new V10DataPipingUtills().getFromOperators((Map) JSONValue.parse(mappedDataStr), mappedKey);
                                            }

                                            try {
                                                Map transformationRulesMap = convertTransFrmRulsMapToParam(trfmRulesDataMap);
                                                if (transformationRulesMap != null
                                                        && !transformationRulesMap.isEmpty()) {
                                                    boolean isSameDataBase = fromOperatorList.stream()
                                                            .filter(fromOpMap -> (fromOpMap.containsKey("CONNECTION_NAME")
                                                            && fromOpMap.containsKey("CONN_DB_NAME")
                                                            && !"file".equalsIgnoreCase(String.valueOf(fromOpMap.get("dragType")))))
                                                            .map(fromOpMap -> (fromOpMap.get("CONNECTION_NAME") + ":::" + fromOpMap.get("CONN_DB_NAME")))
                                                            .distinct().count() == 1;
                                                    boolean iscontainsFile = fromOperatorList.stream()
                                                            .anyMatch(fromOpMap -> "file".equalsIgnoreCase(String.valueOf(fromOpMap.get("dragType"))));
                                                    if (isSameDataBase && !iscontainsFile) {// CASE 1 --> if all from tables having same data base
                                                        Map fromOperator = fromOperatorList.get(0);
                                                        if (fromOperator != null && !fromOperator.isEmpty()) {
                                                            JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");
                                                            String fromTableName = (String) fromOperator.get("tableName");
                                                            JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
                                                            String toTableName = (String) toOperator.get("tableName");
                                                            String toIconType = (String) toOperator.get("iconType");
                                                            Object fromConnectionObj = getConnection(fromConnObj);
                                                            if (fromConnectionObj instanceof Connection) {
                                                                fromConnection = (Connection) fromConnectionObj;
                                                                try {
                                                                    genericProcessETLDataService.processETLLog(loginUserName,
                                                                            loginOrgnId,
                                                                            "Source system successfully connected.", "INFO", 20, "Y", jobId);
                                                                } catch (Exception e) {
                                                                }
                                                                resultObj = processJobHib(loginUserName,
                                                                        loginOrgnId,
                                                                        toOperator,
                                                                        transformationRulesMap,
                                                                        fromConnection,
                                                                        fromOperatorList,
                                                                        jobId);
                                                            } else if (fromConnectionObj instanceof JCO.Client) {
                                                                fromJCOConnection = (JCO.Client) fromConnectionObj;
                                                                // for SAP
                                                                try {
                                                                    genericProcessETLDataService.processETLLog(loginUserName,
                                                                            loginOrgnId,
                                                                            "Source system successfully connected.", "INFO", 20, "Y", jobId);
                                                                } catch (Exception e) {
                                                                }
                                                                resultObj = processJob(loginUserName,
                                                                        loginOrgnId,
                                                                        toOperator,
                                                                        transformationRulesMap,
                                                                        fromJCOConnection,
                                                                        fromOperatorList,
                                                                        jobId);
                                                            } else {
                                                                try {
                                                                    genericProcessETLDataService.processETLLog(loginUserName,
                                                                            loginOrgnId,
                                                                            "Unable to connect source system due to " + fromConnectionObj, "ERROR", 20, "N", jobId);
                                                                } catch (Exception e) {
                                                                }
                                                                resultObj.put("Message", fromConnectionObj);
                                                                resultObj.put("connectionFlag", "N");
                                                            }
                                                        }
                                                    } else {
                                                        boolean isDataBase = fromOperatorList.stream().allMatch(fromOpMap
                                                                -> (fromOpMap.containsKey("CONNECTION_NAME")
                                                                && !"file".equalsIgnoreCase(String.valueOf(fromOpMap.get("dragType")))
                                                                && fromOpMap.containsKey("CONN_DB_NAME")));
                                                        if (isDataBase) {// CASE 2 -->  if all from tables having different DB/ERP.
                                                            resultObj = processJobWithDiffSchemaHib(loginUserName,
                                                                    loginOrgnId,
                                                                    toOperator,
                                                                    transformationRulesMap,
                                                                    fromOperatorList,
                                                                    jobId);

                                                        } else {
                                                            //fileName
                                                            boolean isFile = fromOperatorList.stream().allMatch(fromOpMap
                                                                    -> (fromOpMap.containsKey("dragType")
                                                                    && "file".equalsIgnoreCase(String.valueOf(fromOpMap.get("dragType")))));
                                                            if (isFile) {// CASE 3 -->if all from objects having files.
                                                                resultObj = processJob(loginUserName,
                                                                        loginOrgnId,
                                                                        toOperator,
                                                                        transformationRulesMap,
                                                                        fromOperatorList,
                                                                        jobId);
                                                            } else {// CASE 4 -->if all from objects having files and DB/ERP Objects
                                                                //processJobWithFilesAndTables
                                                                resultObj = genericProcessETLDataService.processJobWithFilesAndTables(loginUserName,
                                                                        loginOrgnId,
                                                                        toOperator,
                                                                        transformationRulesMap,
                                                                        fromOperatorList,
                                                                        jobId);
                                                            }

                                                        }
                                                    }
                                                }
                                            } catch (Exception e) {
                                                e.printStackTrace();
                                                continue;
                                            }

                                        }
                                    }

                                    // if any operators connected with To Operator
                                    Map<String, List<Map>> toOpeartorMapObj = getFromAndToOperatorList(linksMap,
                                            operatorsMap,
                                            toOperator.get("operatorId"));
                                    if (toOpeartorMapObj != null && !toOpeartorMapObj.isEmpty()) {
                                        processNestedJobData(toOpeartorMapObj,
                                                mappedData,
                                                jobId,
                                                loginUserName,
                                                loginOrgnId);

                                    }

                                }
                            }
                        }
                    }// end if for linksMap
                }// end loop for mappingOperatorObj
            }
            try {
                genericProcessETLDataService.processETLLog(loginUserName,
                        loginOrgnId, "ETL Process is completed", "INFO", 10, "N", jobId);
            } catch (Exception e) {
            }
        } catch (Exception e) {
            e.printStackTrace();
            resultObj.put("Message", e.getMessage());
            resultObj.put("connectionFlag", "N");
            try {
                genericProcessETLDataService.processETLLog(loginUserName,
                        loginOrgnId, e.getMessage(), "ERROR", 20, "N", jobId);
            } catch (Exception ex) {
            }
        } finally {

            try {
                if (fromConnection != null) {
                    fromConnection.close();
                }
                if (toConnection != null) {
                    toConnection.close();
                }
                if (toJCOConnection != null) {
                    toJCOConnection.disconnect();
                }
                if (fromJCOConnection != null) {
                    fromJCOConnection.disconnect();
                }
            } catch (Exception e) {
            }
        }
        return resultObj;
    }

    public JSONObject processJobHib(String loginUserName,
            String loginOrgnId,
            Map toOperator,
            Map transformationMap,
            Connection fromConnection,
            List<Map> fromOperatorList,
            String jobId
    ) {
        JSONObject resultObj = new JSONObject();
        Connection toConnection = null;
        JCO.Client toJCOConnection = null;
        PreparedStatement toPreparedStatement = null;
        try {
            if (transformationMap != null) {
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
//                ravi hibernate
                Map totalColumnsObj = (LinkedHashMap) transformationMap.get("totalColumnsObj");
                Map defaultValIndxObj = (LinkedHashMap) transformationMap.get("defaultValIndxObj");
                Map colClauseIndxObj = (LinkedHashMap) transformationMap.get("colClauseIndxObj");

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
                        genericProcessETLDataService.processETLLog(loginUserName,
                                loginOrgnId,
                                "Reading the transformations rules.",
                                "INFO", 10, "Y", jobId);
                    } catch (Exception e) {
                    }
                    //JSONObject fromColumnsObj = new JSONObject();
                    Map fromColumnsObj = new LinkedHashMap();// ravi etl integration
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
                    //Set<String> toColumns = new HashSet<>();
//                    List<String> toColumns = new ArrayList<>(columnsObj.keySet());
                    List<String> toColumns = new ArrayList<>(totalColumnsObj.keySet()); // RAVI HIBERNATE
                    //toColumns.addAll(columnsObj.keySet());
//                    if (defaultValuesObj != null && !defaultValuesObj.isEmpty()) {
//                        Set<String> defaultColumns = defaultValuesObj.keySet();
//                        if (defaultColumns != null
//                                && !defaultColumns.isEmpty()
//                                && toColumns != null && !toColumns.isEmpty()) {
//                            toColumns.addAll(defaultColumns);
//                        }
//                    }
//                    if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
//                        Set<String> columnClauseCols = columnClauseObj.keySet();
//                        if (columnClauseCols != null
//                                && !columnClauseCols.isEmpty()
//                                && toColumns != null && !toColumns.isEmpty()) {
//                            toColumns.addAll(columnClauseCols);
//                        }
//                    }

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
                            selectTabObj.put("toConnectionObj", toConnectionObj);
                            if (toConnectionObj instanceof Connection) {
                                toConnection = (Connection) toConnectionObj;
                            }
                            if (toConnection != null) {
                                String toTableInsertQuery = genericProcessETLDataService.generateInsertQuery((String) toOperator.get("tableName"),
                                        toColumnsList);
                                System.out.println("insertQuery::::" + toTableInsertQuery);
                                toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
                                Map columnsTypeObj = genericProcessETLDataService.getColumnsType((String) toOperator.get("tableName"),
                                        toColumnsList, toConnection);
                                selectTabObj.put("columnsTypeObj", columnsTypeObj);
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
                            genericProcessETLDataService.processETLLog(loginUserName,
                                    loginOrgnId, "Starting extract join tables data.", "INFO", 10, "Y", jobId);
                        } catch (Exception e) {
                        } // JOINS

                        Map fromOperator = fromOperatorList.get(0);
                        JSONObject dbObj = (JSONObject) fromOperator.get("connObj");
                        String hostName = String.valueOf(dbObj.get("HOST_NAME"));
                        String port = String.valueOf(dbObj.get("CONN_PORT"));
                        String userName = String.valueOf(dbObj.get("CONN_USER_NAME"));
                        String password = String.valueOf(dbObj.get("CONN_PASSWORD"));
                        String dataBaseName = String.valueOf(dbObj.get("CONN_DB_NAME"));
                        String dbType = String.valueOf(dbObj.get("CONN_CUST_COL1"));

//                        SessionFactory sessionFactory = iTransformAccess.getSessionFactoryObject(hostName,
//                                port,
//                                userName,
//                                password,
//                                dataBaseName,
//                                dbType);
                        SessionFactory sessionFactory = null;
//                        Session hibernateSession = sessionFactory.openSession();
                        Session hibernateSession = null;
                        String query = genericProcessETLDataService.getSelectedJoinColumnsDataQuery(
                                columnsObj, // ravi etl integration
                                fromColumnsObj, // ravi etl integration
                                tablesWhereClauseObj,
                                joinQueryMapObj,
                                joinQuery,
                                loginUserName,
                                loginOrgnId,
                                appendValObj,
                                columnClauseObj,
                                selectTabObj,
                                totalColumnsObj,
                                defaultValIndxObj,
                                colClauseIndxObj
                        );

                        totalDataCount += processETLDataHibJoins(
                                query,
                                hibernateSession,
                                sessionFactory,
                                loginUserName,
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
                                25000,
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
                                normalizeOptionsObj,
                                totalColumnsObj
                        );

                    } else {
                        if (nonJoinOpList != null && !nonJoinOpList.isEmpty()) {
                            try {
                                genericProcessETLDataService.processETLLog(loginUserName,
                                        loginOrgnId, "Starting extract non-join tables data.", "INFO", 10, "Y", jobId);
                            } catch (Exception e) {
                            }  // non joins
                            for (int i = 0; i < nonJoinOpList.size(); i++) {
                                Map fromOperator = nonJoinOpList.get(i);
                                if (fromOperator != null && !fromOperator.isEmpty()) {

                                    JSONObject dbObj = (JSONObject) fromOperator.get("connObj");
                                    String hostName = String.valueOf(dbObj.get("HOST_NAME"));
                                    String port = String.valueOf(dbObj.get("CONN_PORT"));
                                    String userName = String.valueOf(dbObj.get("CONN_USER_NAME"));
                                    String password = String.valueOf(dbObj.get("CONN_PASSWORD"));
                                    String dataBaseName = String.valueOf(dbObj.get("CONN_DB_NAME"));
                                    String dbType = String.valueOf(dbObj.get("CONN_CUST_COL1"));

                                    SessionFactory sessionFactory =  null;
//                                            iTransformAccess.getSessionFactoryObject(hostName,
//                                            port,
//                                            userName,
//                                            password,
//                                            dataBaseName,
//                                            dbType);
                                    //Session hibernateSession = sessionFactory.openSession();
                                    Session hibernateSession = null;
                                    String query = genericProcessETLDataService.getSelectedColumnsDataQuery(
                                            columnsObj,// ravi etl integration
                                            fromColumnsObj,// ravi etl integration
                                            tablesWhereClauseObj,
                                            orderAndGroupByObj,
                                            columnClauseObj,
                                            selectTabObj,
                                            totalColumnsObj,
                                            defaultValIndxObj,
                                            colClauseIndxObj
                                    );

                                    totalDataCount += processETLDataHibNonJoin(
                                            query,
                                            hibernateSession,
                                            sessionFactory,
                                            loginUserName,
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
                                            25000,
                                            totalDataCount,
                                            toColumnsList,
                                            fileDataLastIndex,
                                            fileName,
                                            1,
                                            orderAndGroupByObj,
                                            columnClauseObj,
                                            selectTabObj,
                                            normalizeOptionsObj,
                                            totalColumnsObj
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

                        try {
                            genericProcessETLDataService.processETLLog(loginUserName,
                                    loginOrgnId, message, "INFO", 10, "Y", jobId);
                        } catch (Exception e) {
                        }
                        resultObj.put("Message", message);
                        resultObj.put("connectionFlag", "Y");

                    } else {
                        try {
                            genericProcessETLDataService.processETLLog(loginUserName,
                                    loginOrgnId, totalDataCount + " Row(s) successfully extracted and loaded into target system.", "INFO", 10, "Y", jobId);
                        } catch (Exception e) {
                        }

                        resultObj.put("Message", totalDataCount + " Row(s) successfully extracted and loaded into target system.");
                        resultObj.put("connectionFlag", "Y");
                    }
                }
            }//end if transformationMap
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
                if (toJCOConnection != null) {
                    toJCOConnection.disconnect();
                }
            } catch (Exception e) {
            }
        }
        return resultObj;
    }

    // JOINS
    public int processETLDataHibJoins(
            String query,
            Session hibernateSession,
            SessionFactory sessionFactory,
            String sessionUserName,
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
            JSONObject normalizeOptionsObj,
            Map totalColumnsObj
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
                genericProcessETLDataService.processETLLog(sessionUserName,
                        orgnId, "Fetching from " + start + " to " + end + " record(s).", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
//                        (String) httpSession.getAttribute("ssOrgId"), "Fetching next 1000 record(s).", "INFO", logSequenceNo, "Y");
            } catch (Exception e) {
            }
            List totalDataList = new ArrayList();
            if (limit > 0) {

                totalDataList = genericProcessETLDataService.getSelectedColumnsData(
                        query,
                        hibernateSession,
                        sessionFactory,
                        start,
                        limit
                );

            }
            String iconType = (String) toOperator.get("iconType");
            if (totalDataList != null && !totalDataList.isEmpty()) {

                try {
                    genericProcessETLDataService.processETLLog(sessionUserName,
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
                        genericProcessETLDataService.processETLLog(sessionUserName,
                                orgnId, "Started " + normalizeFlag + " records.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                    } catch (Exception e) {
                    }
                    if (normalizeFlag != null && "normalize".equalsIgnoreCase(normalizeFlag)) {

                        // totalDataList = getNoramlisedData(normalizeOptionsObj, totalDataList);
                    } else if (normalizeFlag != null && "deNormalize".equalsIgnoreCase(normalizeFlag)) {

                        //  totalDataList = getDeNoramlisedData(normalizeOptionsObj, totalDataList);
                    }
                }
                // ravi normalising   end

                try {
                    genericProcessETLDataService.processETLLog(sessionUserName,
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

                    insertCount = genericProcessETLDataService.exportingXLSXFileDataOpt(toOperator,
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

                    insertCount = genericProcessETLDataService.exportingXLSFileData(toOperator,
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
                    insertCount = genericProcessETLDataService.exportingXMLFileData(toOperator,
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
                    insertCount = genericProcessETLDataService.exportingCsvAndTxtFileDataOpt(toOperator,
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
                    insertCount = genericProcessETLDataService.exportingCsvAndTxtFileDataOpt(toOperator,
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
                    insertCount = genericProcessETLDataService.exportingJSONFileDataOpt(toOperator,
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
                        insertCount = dataMigrationService.importingDataHib((String) toOperator.get("tableName"),
                                fromColumnsObj,
                                columnsObj,
                                totalDataList,
                                toPreparedStatement,
                                toColumnsList,
                                defaultValuesObj, sessionUserName,
                                orgnId,
                                String.valueOf(selectTabObj.get("jobId")),
                                selectTabObj);
                        System.out.println("After Insert ::: " + System.currentTimeMillis());

//                        insertCount = dataMigrationService.importingData((String) toOperator.get("tableName"),
//                                fromColumnsObj,
//                                columnsObj,
//                                totalDataList,
//                                toPreparedStatement,
//                                toColumnsList,
//                                defaultValuesObj, sessionUserName,
//                                orgnId,
//                                String.valueOf(selectTabObj.get("jobId")),
//                                selectTabObj);
                    }

                }
                if (insertCount != 0) {
                    try {
                        genericProcessETLDataService.processETLLog(sessionUserName,
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
                    totalDataCount = processETLDataHibJoins(
                            query,
                            hibernateSession,
                            sessionFactory,
                            sessionUserName,
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
                            normalizeOptionsObj,
                            totalColumnsObj
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
                genericProcessETLDataService.processETLLog(sessionUserName,
                        orgnId, "Failed process records in ETL due to " + e.getLocalizedMessage(), "ERROR", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }
        }
        return totalDataCount;
    }

    // NON JOINS METHOD
    public int processETLDataHibNonJoin(
            String query,
            Session hibernateSession,
            SessionFactory sessionFactory,
            String sessionUserName,
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
            JSONObject normalizeOptionsObj,
            Map totalColumnsObj
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
                genericProcessETLDataService.processETLLog(sessionUserName,
                        orgnId, "Fetching from " + start + " to " + end + " record(s).", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception e) {
            }

            List totalDataList = new ArrayList();
            if (limit > 0) {
                System.out.println("Before Fetch ::: " + System.currentTimeMillis());
                totalDataList = genericProcessETLDataService.getSelectedColumnsData(
                        query,
                        hibernateSession,
                        sessionFactory,
                        start,
                        limit
                );
                System.out.println("After Fetch ::: " + System.currentTimeMillis());
            }
            String iconType = (String) toOperator.get("iconType");
            if (totalDataList != null && !totalDataList.isEmpty()) {
                try {
                    genericProcessETLDataService.processETLLog(sessionUserName,
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
                        genericProcessETLDataService.processETLLog(sessionUserName,
                                orgnId, "Started " + normalizeFlag + " records.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                    } catch (Exception e) {
                    }
                    if (normalizeFlag != null && "normalize".equalsIgnoreCase(normalizeFlag)) {

                        //   totalDataList = getNoramlisedData(normalizeOptionsObj, totalDataList);
                    } else if (normalizeFlag != null && "deNormalize".equalsIgnoreCase(normalizeFlag)) {

                        //  totalDataList = getDeNoramlisedData(normalizeOptionsObj, totalDataList);
                    }
                }
                // ravi normalising   end

                try {
                    genericProcessETLDataService.processETLLog(sessionUserName,
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

                    insertCount = genericProcessETLDataService.exportingXLSXFileDataOpt(toOperator,
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

                    insertCount = genericProcessETLDataService.exportingXLSFileDataOpt(toOperator,
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
                    insertCount = genericProcessETLDataService.exportingXMLFileDataOpt(toOperator,
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
                    insertCount = genericProcessETLDataService.exportingCsvAndTxtFileData(toOperator,
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
                    insertCount = genericProcessETLDataService.exportingCsvAndTxtFileData(toOperator,
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
                    insertCount = genericProcessETLDataService.exportingJSONFileData(toOperator,
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
                        System.out.println("Before Insert ::: " + System.currentTimeMillis());
                        insertCount = dataMigrationService.importingDataHib((String) toOperator.get("tableName"),
                                fromColumnsObj,
                                columnsObj,
                                totalDataList,
                                toPreparedStatement,
                                toColumnsList,
                                defaultValuesObj, sessionUserName,
                                orgnId,
                                String.valueOf(selectTabObj.get("jobId")),
                                selectTabObj);
                        System.out.println("After Insert ::: " + System.currentTimeMillis());
                    }
                }
                if (insertCount != 0) {
                    try {
                        genericProcessETLDataService.processETLLog(sessionUserName,
                                orgnId, "Pushed " + insertCount + " record(s) into target system.", "INFO", logSequenceNo, "Y", String.valueOf(selectTabObj.get("jobId")));
                    } catch (Exception e) {
                    }
                }

                totalDataCount += insertCount;
                if (insertCount != 0 && insertCount >= 1000) {
                    int startIndex = (logSequenceNo * limit + 1);

                    logSequenceNo++;
                    // resursion
                    totalDataCount = processETLDataHibNonJoin(
                            query,
                            hibernateSession,
                            sessionFactory,
                            sessionUserName,
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
                            normalizeOptionsObj,
                            totalColumnsObj
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
                genericProcessETLDataService.processETLLog(sessionUserName,
                        orgnId, e.getMessage(), "ERROR", 20, "Y", String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }
        }
        return totalDataCount;
    }

    // for executing job if the sources(from) Data Base tables with different schema
    public JSONObject processJobWithDiffSchemaHib(String loginUserName,
            String loginOrgnId,
            Map toOperator,
            Map transformationMap,
            List<Map> fromOperatorList,
            String jobId
    ) {
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

                Map totalColumnsObj = (LinkedHashMap) transformationMap.get("totalColumnsObj"); // ravi Hibernate
                Map defaultValIndxObj = (LinkedHashMap) transformationMap.get("defaultValIndxObj"); // ravi Hibernate
                Map colClauseIndxObj = (LinkedHashMap) transformationMap.get("colClauseIndxObj"); // ravi Hibernate

                if (!(selectTabObj != null && !selectTabObj.isEmpty())) {
                    selectTabObj = new JSONObject();
                }
                selectTabObj.put("jobId", jobId);
                if (columnsObj != null && !columnsObj.isEmpty()) {
                    try {
                        genericProcessETLDataService.processETLLog(loginUserName,
                                loginOrgnId,
                                "Reading the transformations rules.",
                                "INFO", 10, "Y", jobId);
                    } catch (Exception e) {
                    }
                    //JSONObject fromColumnsObj = new JSONObject();
                    Map fromColumnsObj = new LinkedHashMap();// ravi etl integration
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
//                    toColumns.addAll(columnsObj.keySet());
                    toColumns.addAll(totalColumnsObj.keySet()); // ravi hibernate
                    
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
                            selectTabObj.put("toConnectionObj", toConnectionObj);
                            if (toConnectionObj instanceof Connection) {
                                toConnection = (Connection) toConnectionObj;
                            }
                            if (toConnection != null) {
                                String toTableInsertQuery = genericProcessETLDataService.generateInsertQuery((String) toOperator.get("tableName"),
                                        toColumnsList);
                                System.out.println("insertQuery::::" + toTableInsertQuery);
                                toPreparedStatement = toConnection.prepareStatement(toTableInsertQuery);
                                Map columnsTypeObj = genericProcessETLDataService.getColumnsType((String) toOperator.get("tableName"),
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
//                        Map trfmRulesDataMap = (Map) toOperator.get("trfmRules-data");
                        for (int i = 0; i < fromOperatorList.size(); i++) {
                            Map fromOpearator = fromOperatorList.get(i);
                            if (fromOpearator != null && !fromOpearator.isEmpty()) {
                                Map fromOpearatorMap = genericProcessETLDataService.getSelectedDBTablesDataHib(fromOpearator,
                                        toOperator,
                                        fromColumnsObj,
                                        transformationMap);
                                if (fromOpearatorMap != null && !fromOpearatorMap.isEmpty()) {
                                    fromOperatorMapList.add(fromOpearatorMap);
                                    if (fromColumnsObj.containsKey(fromOpearatorMap.get("oldTableName"))) {
                                        transformationMap = genericProcessETLDataService.convertTransFrmRulsMapToParam(transformationMap, fromOpearatorMap);
                                        fromColumnsObj.put(fromOpearatorMap.get("tableName"), fromColumnsObj.get(fromOpearatorMap.get("oldTableName")));
                                        fromColumnsObj.remove(fromOpearatorMap.get("oldTableName"));
                                    }
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
                        Map columnsTypeObj = genericProcessETLDataService.getColumnsType((String) toOperator.get("tableName"), toColumnsList, toConnection);
                        selectTabObj.put("columnsTypeObj", columnsTypeObj);
                    }
//                    JSONObject joinQueryMapObj = (JSONObject) transformationMap.get("joinQueryHashMapObj");//
                    LinkedHashMap joinQueryHashMapObj = (LinkedHashMap) transformationMap.get("joinQueryHashMapObj");
                    JSONObject joinQueryMapObj = new JSONObject();//
                    if (joinQueryHashMapObj != null && !joinQueryHashMapObj.isEmpty()) {
                        joinQueryMapObj.putAll(joinQueryHashMapObj);
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
                            genericProcessETLDataService.processETLLog(loginUserName,
                                    loginOrgnId, "Starting extract join tables data.", "INFO", 10, "Y", jobId);
                        } catch (Exception e) {
                        }
                        Map fromOperator = fromOperatorList.get(0);
                        JSONObject dbObj = (JSONObject) fromOperator.get("connObj");
                        String hostName = String.valueOf(dbObj.get("HOST_NAME"));
                        String port = String.valueOf(dbObj.get("CONN_PORT"));
                        String userName = String.valueOf(dbObj.get("CONN_USER_NAME"));
                        String password = String.valueOf(dbObj.get("CONN_PASSWORD"));
                        String dataBaseName = String.valueOf(dbObj.get("CONN_DB_NAME"));
                        String dbType = String.valueOf(dbObj.get("CONN_CUST_COL1"));

                        SessionFactory sessionFactory =  null;
                        
//                                iTransformAccess.getSessionFactoryObject(hostName,
//                                port,
//                                userName,
//                                password,
//                                dataBaseName,
//                                dbType);
                        //Session hibernateSession = sessionFactory.openSession();
                        Session hibernateSession = null;
                        String query = genericProcessETLDataService.getSelectedJoinColumnsDataQuery(
                                columnsObj, // ravi etl integration
                                fromColumnsObj, // ravi etl integration
                                tablesWhereClauseObj,
                                joinQueryMapObj,
                                joinQuery,
                                loginUserName,
                                loginOrgnId,
                                appendValObj,
                                columnClauseObj,
                                selectTabObj,
                                columnsObj,
                                defaultValIndxObj,
                                colClauseIndxObj
                        );

                        totalDataCount += processETLDataHibJoins(
                                query,
                                hibernateSession,
                                sessionFactory,
                                loginUserName,
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
                                10000,
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
                                normalizeOptionsObj,
                                totalColumnsObj
                        );

                    } else {
                        if (nonJoinOpList != null && !nonJoinOpList.isEmpty()) {
                            try {
                                genericProcessETLDataService.processETLLog(loginUserName,
                                        loginOrgnId, "Starting extract non-join tables data.", "INFO", 10, "Y", jobId);
                            } catch (Exception e) {
                            }
                            for (int i = 0; i < nonJoinOpList.size(); i++) {
                                Map fromOperator = nonJoinOpList.get(i);
                                if (fromOperator != null && !fromOperator.isEmpty()) {

                                    
                                    JSONObject dbObj = (JSONObject) fromOperator.get("connObj");
                                    String hostName = String.valueOf(dbObj.get("HOST_NAME"));
                                    String port = String.valueOf(dbObj.get("CONN_PORT"));
                                    String userName = String.valueOf(dbObj.get("CONN_USER_NAME"));
                                    String password = String.valueOf(dbObj.get("CONN_PASSWORD"));
                                    String dataBaseName = String.valueOf(dbObj.get("CONN_DB_NAME"));
                                    String dbType = String.valueOf(dbObj.get("CONN_CUST_COL1"));

                                    SessionFactory sessionFactory = null;
//                                            iTransformAccess.getSessionFactoryObject(hostName,
//                                            port,
//                                            userName,
//                                            password,
//                                            dataBaseName,
//                                            dbType);
//                                    Session hibernateSession = sessionFactory.openSession();
                                    Session hibernateSession = null;
                                    String query = genericProcessETLDataService.getSelectedColumnsDataQuery(
                                            columnsObj,// ravi etl integration
                                            fromColumnsObj,// ravi etl integration
                                            tablesWhereClauseObj,
                                            orderAndGroupByObj,
                                            columnClauseObj,
                                            selectTabObj,
                                            columnsObj,
                                            defaultValIndxObj,
                                            colClauseIndxObj
                                    );

                                    totalDataCount += processETLDataHibNonJoin(
                                            query,
                                            hibernateSession,
                                            sessionFactory,
                                            loginUserName,
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
                                            10000,
                                            totalDataCount,
                                            toColumnsList,
                                            fileDataLastIndex,
                                            fileName,
                                            1,
                                            orderAndGroupByObj,
                                            columnClauseObj,
                                            selectTabObj,
                                            normalizeOptionsObj,
                                            totalColumnsObj
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
                            genericProcessETLDataService.processETLLog(loginUserName,
                                    loginOrgnId, message, "INFO", 10, "Y", jobId);
                        } catch (Exception e) {
                        }
                        resultObj.put("Message", message);
                        resultObj.put("connectionFlag", "Y");

                    } else {
                        try {
                            genericProcessETLDataService.processETLLog(loginUserName,
                                    loginOrgnId, totalDataCount + " Row(s) successfully extracted and loaded into target system.", "INFO", 10, "Y", jobId);
                        } catch (Exception e) {
                        }

                        resultObj.put("Message", totalDataCount + " Row(s) successfully extracted and loaded into target system.");
                        resultObj.put("connectionFlag", "Y");
                    }
                }
            }//end if transformationMap
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

}
