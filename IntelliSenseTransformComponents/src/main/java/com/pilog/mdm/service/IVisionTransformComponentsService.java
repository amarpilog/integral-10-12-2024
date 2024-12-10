/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Ravindar.P
 */
package com.pilog.mdm.service;

import com.pilog.mdm.DAO.IVisionTransformComponentsDAO;
import com.pilog.mdm.DAO.IVisionTransformProcessJobComponentsDAO;
//import com.pilog.mdm.DAO.iVisionTransformTableOperationsDAO;
import com.pilog.mdm.access.DataAccess;
import com.pilog.mdm.utilities.AuditIdGenerator;
import com.pilog.mdm.utilities.PilogUtilities;
import com.sap.mw.jco.JCO;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
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

import javax.servlet.ServletContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.map.HashedMap;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

@Service
public class IVisionTransformComponentsService {

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

    private String etlFilePath;

    {
        if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")) {
            etlFilePath = "C://";
        } else {
            etlFilePath = "/u01/";
        }
    }

    @Autowired
    private IVisionTransformComponentsDAO iVisionTransformComponentsDAO;

    @Autowired
    private IVisionTransformProcessJobComponentsDAO processJobComponentsDAO;

    @Autowired
    private IVisionTransformComponentsService iVisionTransformComponentsService;

    @Autowired
    private IVisionTransformProcessJobComponentsService processJobComponentsService;
//    @Autowired
//    private V10GenericDataPipingService dataPipingService;
    @Autowired
    private IVisionTransformComponentUtilities componentUtilities;

    @Autowired
    private IVisionTransformFileProcessJobService fileProcessJobService;

    @Autowired
    private IVisionTransformERPProcessJobService erpProcessJobService;

    @Autowired
    private IVisionTransformComponentsDAO componentsDAO;

    @Autowired
    private DataAccess access;
    
//    @Autowired
//    private iVisionTransformTableOperationsDAO tableOperationsDAO;

    public JSONObject normalizeTrfmRules(HttpServletRequest request) {
//        JSONObject resultObj = new JSONObject();
        JSONObject resultObj = new JSONObject();
        Connection fromConnection = null;
        JCO.Client fromJCOConnection = null;
        List headersList = new ArrayList();
        String flowchartDataStr = request.getParameter("flowchartData");
        JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
        JSONObject operators = (JSONObject) flowchartData.get("operators");
        String sourceOperatorsStr = request.getParameter("sourceOperators");
        List<Map> fromOperatorsList = (List<Map>) JSONValue.parse(sourceOperatorsStr);
        JSONObject fromOperator = (JSONObject) fromOperatorsList.get(0);
        JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

        String selectedOperatorId = request.getParameter("selectedOperatorId");
        JSONObject operator = (JSONObject) operators.get(selectedOperatorId);
        JSONObject trfmRulesData = (JSONObject) operator.get("trfmRules");
        List simpleColumnsList = fromOperator.get("simpleColumnsList") != null
                ? (List) fromOperator.get("simpleColumnsList")
                : new ArrayList();

        String divStr = "";
        boolean tableExist = false;
        try {
            if (fromConnObj != null && fromConnObj.containsKey("fileName")) {
                String filePath = (String) fromConnObj.get("filePath");
                String fileType = (String) fromConnObj.get("fileType");
                String fileName = (String) fromConnObj.get("fileName");
                headersList = componentUtilities.getHeadersOfImportedFile(request, filePath);
                headersList = componentUtilities.fileHeaderValidations(headersList);

            } else if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
                String fromTableName = (String) fromOperator.get("statusLabel");

                Object fromReturendObj = componentUtilities.getConnection(fromConnObj);

                String matchedSelectStr = "";
                if (fromReturendObj instanceof Connection) {
                    fromConnection = (Connection) fromReturendObj;
                    tableExist = checkTableExsist((Connection) fromReturendObj, fromTableName);
                    if (tableExist) {
                        List headersObjList = componentUtilities.getTreeDMTableColumnsOpt(fromConnection, request,
                                fromConnObj, fromTableName);
                        headersList = (List) headersObjList.stream().map(element -> (((Object[]) element)[1]))
                                .collect(Collectors.toList());

                    } else {
                        headersList = simpleColumnsList;
                    }
                } else if (fromReturendObj instanceof JCO.Client) {
                    tableExist = true;
                    fromJCOConnection = (JCO.Client) fromReturendObj;
                    List headersObjList = componentUtilities.getSAPTableColumns(request, fromJCOConnection,
                            fromTableName);
                    headersList = (List) headersObjList.stream().map(element -> (((Object[]) element)[1]))
                            .collect(Collectors.toList());

                }
            }
            simpleColumnsList.addAll(headersList);
            divStr = normalizeTrfmRulesTabString(request, headersList, trfmRulesData);
            resultObj.put("divStr", divStr);
            resultObj.put("simpleColumnsList", simpleColumnsList);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return resultObj;
    }

    public JSONObject deNormalizeTrfmRules(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        Connection fromConnection = null;
        JCO.Client fromJCOConnection = null;
        List headersList = new ArrayList();
        String flowchartDataStr = request.getParameter("flowchartData");
        JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
        JSONObject operators = (JSONObject) flowchartData.get("operators");
        String sourceOperatorsStr = request.getParameter("sourceOperators");
        List<Map> fromOperatorsList = (List<Map>) JSONValue.parse(sourceOperatorsStr);
        JSONObject fromOperator = (JSONObject) fromOperatorsList.get(0);
        JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

        String selectedOperatorId = request.getParameter("selectedOperatorId");
        JSONObject operator = (JSONObject) operators.get(selectedOperatorId);
        JSONObject trfmRulesData = (JSONObject) operator.get("trfmRules");
        List simpleColumnsList = fromOperator.get("simpleColumnsList") != null
                ? (List) fromOperator.get("simpleColumnsList")
                : new ArrayList();

        String divStr = "";
        boolean tableExist = false;
        try {
            if (fromConnObj != null && fromConnObj.containsKey("fileName")) {
                String filePath = (String) fromConnObj.get("filePath");
                String fileType = (String) fromConnObj.get("fileType");
                String fileName = (String) fromConnObj.get("fileName");
                headersList = componentUtilities.getHeadersOfImportedFile(request, filePath);
                headersList = componentUtilities.fileHeaderValidations(headersList);

            } else if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
                String fromTableName = (String) fromOperator.get("statusLabel");

                Object fromReturendObj = componentUtilities.getConnection(fromConnObj);

                String matchedSelectStr = "";
                if (fromReturendObj instanceof Connection) {
                    fromConnection = (Connection) fromReturendObj;
                    tableExist = checkTableExsist((Connection) fromReturendObj, fromTableName);
                    if (tableExist) {
                        List headersObjList = componentUtilities.getTreeDMTableColumnsOpt(fromConnection, request,
                                fromConnObj, fromTableName);
                        headersList = (List) headersObjList.stream().map(element -> (((Object[]) element)[1]))
                                .collect(Collectors.toList());

                    } else {
                        headersList = simpleColumnsList;
                    }

                } else if (fromReturendObj instanceof JCO.Client) {
                    tableExist = true;
                    fromJCOConnection = (JCO.Client) fromReturendObj;
                    List headersObjList = componentUtilities.getSAPTableColumns(request, fromJCOConnection,
                            fromTableName);
                    headersList = (List) headersObjList.stream().map(element -> (((Object[]) element)[1]))
                            .collect(Collectors.toList());

                }
            }

            simpleColumnsList.addAll(headersList);
            divStr = deNormalizeTrfmRulesTabString(request, headersList, trfmRulesData);
            resultObj.put("divStr", divStr);
            resultObj.put("simpleColumnsList", simpleColumnsList);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return resultObj;
    }

    public String normalizeTrfmRulesTabString(HttpServletRequest request, List headersList, JSONObject trfmRulesData) {
        String tabsStr = "";
        String splitColumn = "";
        String itemSeparator = "";
        try {

            JSONObject normalizeOptionsObj = new JSONObject();
            if (trfmRulesData != null && !trfmRulesData.isEmpty()) {
                normalizeOptionsObj = (JSONObject) trfmRulesData.get("normalizeOptionsObj");
            }

            if (normalizeOptionsObj != null && normalizeOptionsObj.size() > 0) {
                splitColumn = (String) normalizeOptionsObj.get("normalizeColumn");
                itemSeparator = (String) normalizeOptionsObj.get("itemSeparator");

                tabsStr += "<div id='dataMigrationTabs'>" + "<ul>" + "<li>Normalize</li>" + "</ul>";

                tabsStr += "<div class = 'visionNormalizeOptions'>"
                        + "<div class = 'visionSelectColumnsHeaders' style = 'margin-left: 10px; margin-top: 10px;'>"
                        + "<span>Select Column to Normalize </span>" + "<select id='selectNormalizeColHeader'>"
                        + "<option selected>Select</option>";
                for (int i = 0; i < headersList.size(); i++) {
                    if (splitColumn != null && splitColumn.equalsIgnoreCase(String.valueOf(headersList.get(i)))) {
                        tabsStr += "<option selected>" + headersList.get(i) + "</option>";
                    } else {
                        tabsStr += "<option>" + headersList.get(i) + "</option>";
                    }
                }

                tabsStr += "</select>" + "</div>"
                        + "<div class='visionItemSeparatorDiv' style = 'margin-left: 10px; margin-top: 10px;'>"
                        + "<span class='visionItemSeparatorSpan' >Item Separator </span>"
                        + "<input id='itemSeparator' type='text' class='visionItemSeparatorInput' value='"
                        + itemSeparator + "'/>" + "</div>" + "</div>" + "</div>";

            } else {

                tabsStr += "<div id='dataMigrationTabs'>" + "<ul>" + "<li>Normalize</li>" + "</ul>";

                tabsStr += "<div class = 'visionNormalizeOptions'>"
                        + "<div class = 'visionSelectColumnsHeaders' style = 'margin-left: 10px; margin-top: 10px;'>"
                        + "<span>Select Column to Normalize </span>" + "<select id='selectNormalizeColHeader'>"
                        + "<option selected>Select</option>";
                for (int i = 0; i < headersList.size(); i++) {

                    tabsStr += "<option>" + headersList.get(i) + "</option>";
                }

                tabsStr += "</select>" + "</div>"
                        + "<div class='visionItemSeparatorDiv' style = 'margin-left: 10px; margin-top: 10px;'>"
                        + "<span class='visionItemSeparatorSpan' >Item Separator </span>"
                        + "<input id='itemSeparator' type='text' class='visionItemSeparatorInput' value=''/>" + "</div>"
                        + "</div>" + "</div>";
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return tabsStr;
    }

    public String deNormalizeTrfmRulesTabString(HttpServletRequest request, List headersList,
            JSONObject trfmRulesData) { // ravi etl integration
        String tabsStr = "";
        String denormalizeColumn = "";
        String delimiter = "";
        String keyColumn = "";
        try {

            JSONObject normalizeOptionsObj = new JSONObject();
            if (trfmRulesData != null && !trfmRulesData.isEmpty()) {
                normalizeOptionsObj = (JSONObject) trfmRulesData.get("normalizeOptionsObj");
            }

            if (normalizeOptionsObj != null && normalizeOptionsObj.size() > 0) {
                denormalizeColumn = (String) normalizeOptionsObj.get("denormalizeColumn");
                delimiter = (String) normalizeOptionsObj.get("delimiter");
                keyColumn = (String) normalizeOptionsObj.get("keyColumn");

                tabsStr += "<div id='dataMigrationTabs'>" + "<ul>" + "<li>Denormalize</li>" + "</ul>";

                tabsStr += "<div class = 'visionDeNormalizeOptions'>"
                        + "<div class = 'visionSelectColumnsHeaders' style = 'margin-left: 10px; margin-top: 10px;'>"
                        + "<span>Select Columns Denormalize </span>" + "<select id='selectDenormalizeColHeader'>"
                        + "<option selected>Select</option>";
                for (int i = 0; i < headersList.size(); i++) {
                    if (denormalizeColumn != null
                            && denormalizeColumn.equalsIgnoreCase(String.valueOf(headersList.get(i)))) {
                        tabsStr += "<option selected>" + headersList.get(i) + "</option>";
                    } else {
                        tabsStr += "<option>" + headersList.get(i) + "</option>";
                    }
                }

                tabsStr += "</select>" + "</div>"
                        + "<div class = 'visionSelectKeyColumn' style = 'margin-left: 10px; margin-top: 10px;'>"
                        + "<span> Key Column </span>" + "<select id='selectDenormalizeKeyColumn'>"
                        + "<option selected>Select</option>";
                for (int i = 0; i < headersList.size(); i++) {
                    if (keyColumn != null && keyColumn.equalsIgnoreCase(String.valueOf(headersList.get(i)))) {
                        tabsStr += "<option selected>" + headersList.get(i) + "</option>";
                    } else {
                        tabsStr += "<option>" + headersList.get(i) + "</option>";
                    }
                }
                tabsStr += "</select>" + "</div>"
                        + "<div class='visionItemSeparatorDiv' style = 'margin-left: 10px; margin-top: 10px;'>"
                        + "<span class='visionItemSeparatorSpan' >Delimiter </span>"
                        + "<input id='delimiter' type='text' class='visionItemSeparatorInput' value='" + delimiter
                        + "'/>" + "</div>" + "</div>" + "</div>";

            } else {
                tabsStr += "<div id='dataMigrationTabs'>" + "<ul>" + "<li>Denormalize</li>" + "</ul>";

                tabsStr += "<div class = 'visionDeNormalizeOptions'>"
                        + "<div class = 'visionSelectColumnsHeaders' style = 'margin-left: 10px; margin-top: 10px;'>"
                        + "<span>Select Column to Denormalize </span>" + "<select id='selectDenormalizeColHeader'>"
                        + "<option selected>Select</option>";
                for (int i = 0; i < headersList.size(); i++) {
                    tabsStr += "<option>" + headersList.get(i) + "</option>";
                }
                tabsStr += "</select>" + "</div>"
                        + "<div class = 'visionSelectKeyColumn' style = 'margin-left: 10px; margin-top: 10px;'>"
                        + "<span> Key Column </span>" + "<select id='selectDenormalizeKeyColumn'>"
                        + "<option selected>Select</option>";
                for (int i = 0; i < headersList.size(); i++) {
                    tabsStr += "<option>" + headersList.get(i) + "</option>";
                }
                tabsStr += "</select>" + "</div>"
                        + "<div class='visionItemSeparatorDiv' style = 'margin-left: 10px; margin-top: 10px;'>"
                        + "<span class='visionItemSeparatorSpan' >Delimiter </span>"
                        + "<input id='delimiter' type='text' class='visionItemSeparatorInput' value=''/>" + "</div>"
                        + "</div>" + "</div>";
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return tabsStr;
    }

    public JSONObject getuniqueRecords(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        try {

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    public JSONObject scdType3TrfmRules(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        Object connection = null;
        List<Object[]> columnsObjList = new ArrayList();
        List<String> columnsList = new ArrayList();
        try {
            List simpleColumnsList = new ArrayList();
            String tableName = request.getParameter("tableName");
            String connObjStr = request.getParameter("connObj");
            JSONObject connObj = (JSONObject) JSONValue.parse(connObjStr);

            String selectedOperatorId = request.getParameter("selectedOperatorId");

            String flowchartDataStr = request.getParameter("flowchartData");
            JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
            JSONObject operators = (JSONObject) flowchartData.get("operators");
            JSONObject selectedOperator = (JSONObject) operators.get(selectedOperatorId);
            JSONArray historyCols = new JSONArray();
            if (selectedOperator != null && !selectedOperator.isEmpty()) {
                JSONObject trfmRules = (JSONObject) selectedOperator.get("trfmRules");
                if (trfmRules != null && !trfmRules.isEmpty()) {
                    historyCols = (JSONArray) trfmRules.get("historyCols");
                }
            }

            String divStr = "";
            boolean tableExist = false;
            if (connObj.containsKey("HOST_NAME")) {

                if ("SAP_ECC".equalsIgnoreCase(String.valueOf(connObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(connObj.get("CONN_CUST_COL1")))) {
                    connection = (JCO.Client) componentUtilities.getConnection(connObj);
                    columnsObjList = componentUtilities.getSAPTableColumnsWithType(request, (JCO.Client) connection,
                            tableName);
                    columnsList = (List) columnsObjList.stream().map(rowdata -> rowdata[2])
                            .collect(Collectors.toList());
                } else {

                    connection = (Connection) componentUtilities.getConnection(connObj);
                    tableExist = checkTableExsist((Connection) connection, tableName);
                    if (tableExist) {
                        columnsObjList = componentUtilities.getTableColumnsOpt((Connection) connection, connObj,
                                tableName);
                        columnsList = (List) columnsObjList.stream().map(rowdata -> rowdata[2])
                                .collect(Collectors.toList());
//                        data = (List) columnsObjList.stream().map(rowdata -> rowdata[2]).collect(Collectors.toList());
                    } else {
                        JSONArray connectedFromOpIdArray = processJobComponentsService
                                .getConnectedFromOperatorIds(request, selectedOperatorId, flowchartData);
                        String connectedFromOpId = String.valueOf(connectedFromOpIdArray.get(0));
//                        String connectedFromOpId = String.valueOf(((JSONArray) selectedOperator.get("connectedFrom")).get(0));
                        JSONObject fromOperator = (JSONObject) operators.get(connectedFromOpId);
                        columnsList = (List) fromOperator.get("simpleColumnsList");

                    }

                }

                if (columnsList != null && !columnsList.isEmpty()) {

                    divStr += "<div class=\"visionSCDTypeColsDiv\" ><table id=\"scd3HistoryColsTable\" class=\"visionSCD3HistoryCols\" style=\"width: 20%;\" border=\"1\" >"
                            + "<thead>" + "<tr>"
                            + "<th style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">Column Name</th>"
                            + "<th style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">History Col<input class='visionSCD3CompSelectAll' id='scd3CompSelectAll' type='checkbox'  /></th>"
                            + "</tr>" + "</thead>" + "<tbody>";

                    for (int i = 0; i < columnsList.size(); i++) {
                        String columnName = columnsList.get(i);
                        String checked = (historyCols.contains(columnName)) ? "checked" : "";
                        divStr += "<tr>" + "<td>" + columnName
                                + "</td><td><input class='visionscd3HistoryColSelectBox' type='checkbox'   value = '"
                                + columnName + "' " + checked + " /></td>" + "</tr>";

                        simpleColumnsList.add(columnName);
                    }

                    divStr += "</tbody>" + "</table></div>";
                }
            } else {
                String filePath = (String) connObj.get("filePath");
                List headers = componentUtilities.getHeadersOfImportedFile(request, filePath);
                List validHeaders = componentUtilities.fileHeaderValidations(headers);
                simpleColumnsList.addAll(validHeaders);
                divStr += "<div class=\"visionSCDTypeColsDiv\" ><table id=\"scd3HistoryColsTable\" class=\"visionSCD3HistoryCols\"  style=\"width: 20%;\" border=\"1\" >"
                        + "<thead>" + "<tr>"
                        + "<th style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">Column Name</th>"
                        + "<th style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">History Col<input class='visionSCD3CompSelectAll' id='scd3CompSelectAll' type='checkbox'  /></th>"
                        + "</tr>" + "</thead>" + "<tbody>";

                for (int i = 0; i < headers.size(); i++) {
                    String columnName = (String) validHeaders.get(i);
                    String checked = (historyCols.contains(columnName)) ? "checked" : "";
//                  String colValue = columnName.replaceAll(" ", "_");

                    divStr += "<tr>" + "<td>" + headers.get(i)
                            + "</td><td><input class='visionscd3HistoryColSelectBox' type='checkbox' datatype='VARCHAR2(4000)'  value = '"
                            + columnName + "' " + checked + " /></td>" + "</tr>";
                }

                divStr += "</tbody>" + "</table></div>";
            }

            resultObj.put("result", divStr);
            resultObj.put("simpleColumnsList", simpleColumnsList);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    public JSONObject scdType6TrfmRules(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        Object connection = null;
        List<Object[]> columnsObjList = new ArrayList();
        List<String> columnsList = new ArrayList();
        try {
            List simpleColumnsList = new ArrayList();
            String tableName = request.getParameter("tableName");
            String connObjStr = request.getParameter("connObj");
            JSONObject connObj = (JSONObject) JSONValue.parse(connObjStr);

            String selectedOperatorId = request.getParameter("selectedOperatorId");

            String flowchartDataStr = request.getParameter("flowchartData");
            JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
            JSONObject operators = (JSONObject) flowchartData.get("operators");
            JSONObject selectedOperator = (JSONObject) operators.get(selectedOperatorId);
            JSONArray historyCols = new JSONArray();
            if (selectedOperator != null && !selectedOperator.isEmpty()) {
                JSONObject trfmRules = (JSONObject) selectedOperator.get("trfmRules");
                if (trfmRules != null && !trfmRules.isEmpty()) {
                    historyCols = (JSONArray) trfmRules.get("historyCols");
                }
            }

            String divStr = "";
            boolean tableExist = false;
            if (connObj.containsKey("HOST_NAME")) {

                if ("SAP_ECC".equalsIgnoreCase(String.valueOf(connObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(connObj.get("CONN_CUST_COL1")))) {
                    connection = (JCO.Client) componentUtilities.getConnection(connObj);
                    columnsObjList = componentUtilities.getSAPTableColumnsWithType(request, (JCO.Client) connection,
                            tableName);
                    columnsList = (List) columnsObjList.stream().map(rowdata -> rowdata[2])
                            .collect(Collectors.toList());
                } else {

                    connection = (Connection) componentUtilities.getConnection(connObj);
                    tableExist = checkTableExsist((Connection) connection, tableName);
                    if (tableExist) {
                        columnsObjList = componentUtilities.getTableColumnsOpt((Connection) connection, connObj,
                                tableName);
                        columnsList = (List) columnsObjList.stream().map(rowdata -> rowdata[2])
                                .collect(Collectors.toList());
//                        data = (List) columnsObjList.stream().map(rowdata -> rowdata[2]).collect(Collectors.toList());
                    } else {
                        JSONArray connectedFromOpIdArray = processJobComponentsService
                                .getConnectedFromOperatorIds(request, selectedOperatorId, flowchartData);
                        String connectedFromOpId = String.valueOf(connectedFromOpIdArray.get(0));

//                        String connectedFromOpId = String.valueOf(((JSONArray) selectedOperator.get("connectedFrom")).get(0));
                        JSONObject fromOperator = (JSONObject) operators.get(connectedFromOpId);
                        columnsList = (List) fromOperator.get("simpleColumnsList");

                    }

                }

                if (columnsList != null && !columnsList.isEmpty()) {

                    divStr += "<div class=\"visionSCDTypeColsDiv\" ><table id=\"scd6HistoryColsTable\" class=\"visionSCD6HistoryCols\" style=\"width: 20%;\" border=\"1\" >"
                            + "<thead>" + "<tr>"
                            + "<th style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">Column Name</th>"
                            + "<th style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">History Col<input class='visionSCD6CompSelectAll' id='scd6CompSelectAll' type='checkbox'  /></th>"
                            + "</tr>" + "</thead>" + "<tbody>";

                    for (int i = 0; i < columnsList.size(); i++) {
                        String columnName = columnsList.get(i);
                        String checked = (historyCols.contains(columnName)) ? "checked" : "";
                        divStr += "<tr>" + "<td>" + columnName
                                + "</td><td><input class='visionscd6HistoryColSelectBox' type='checkbox'   value = '"
                                + columnName + "' " + checked + " /></td>" + "</tr>";

                        simpleColumnsList.add(columnName);
                    }

                    divStr += "</tbody>" + "</table></div>";
                }
            } else {
                String filePath = (String) connObj.get("filePath");
                List headers = componentUtilities.getHeadersOfImportedFile(request, filePath);
                List validHeaders = componentUtilities.fileHeaderValidations(headers);
                simpleColumnsList.addAll(validHeaders);
                divStr += "<div class=\"visionSCDTypeColsDiv\" ><table id=\"scd6HistoryColsTable\" class=\"visionSCD6HistoryCols\"  style=\"width: 20%;\" border=\"1\" >"
                        + "<thead>" + "<tr>"
                        + "<th style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">Column Name</th>"
                        + "<th style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">History Col<input class='visionSCD6CompSelectAll' id='scd6CompSelectAll' type='checkbox'  /></th>"
                        + "</tr>" + "</thead>" + "<tbody>";

                for (int i = 0; i < headers.size(); i++) {
                    String columnName = (String) validHeaders.get(i);
                    String checked = (historyCols.contains(columnName)) ? "checked" : "";
//                  String colValue = columnName.replaceAll(" ", "_");

                    divStr += "<tr>" + "<td>" + headers.get(i)
                            + "</td><td><input class='visionscd6HistoryColSelectBox' type='checkbox' datatype='VARCHAR2(4000)'  value = '"
                            + columnName + "' " + checked + " /></td>" + "</tr>";
                }

                divStr += "</tbody>" + "</table></div>";
            }

            resultObj.put("result", divStr);
            resultObj.put("simpleColumnsList", simpleColumnsList);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    public JSONObject uniqueComponentTrfmRules(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        Object connection = null;
        List<Object[]> columnsObjList = new ArrayList();
        List<String> columnsList = new ArrayList();
        try {
            List simpleColumnsList = new ArrayList();
            String tableName = request.getParameter("tableName");
            String connObjStr = request.getParameter("connObj");
            JSONObject connObj = (JSONObject) JSONValue.parse(connObjStr);

            String selectedOperatorId = request.getParameter("selectedOperatorId");

            String flowchartDataStr = request.getParameter("flowchartData");
            JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
            JSONObject operators = (JSONObject) flowchartData.get("operators");
            JSONObject selectedOperator = (JSONObject) operators.get(selectedOperatorId);
            JSONArray uniqueKeys = new JSONArray();
            if (selectedOperator != null && !selectedOperator.isEmpty()) {
                JSONObject trfmRules = (JSONObject) selectedOperator.get("trfmRules");
                if (trfmRules != null && !trfmRules.isEmpty()) {
                    uniqueKeys = (JSONArray) trfmRules.get("uniqueKeys");
                }
            }

            String divStr = "";
            boolean tableExist = false;
            if (connObj.containsKey("HOST_NAME")) {

                if ("SAP_ECC".equalsIgnoreCase(String.valueOf(connObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(connObj.get("CONN_CUST_COL1")))) {
                    connection = (JCO.Client) componentUtilities.getConnection(connObj);
                    columnsObjList = componentUtilities.getSAPTableColumnsWithType(request, (JCO.Client) connection,
                            tableName);
                    columnsList = (List) columnsObjList.stream().map(rowdata -> rowdata[2])
                            .collect(Collectors.toList());
                } else {

                    connection = (Connection) componentUtilities.getConnection(connObj);
                    tableExist = checkTableExsist((Connection) connection, tableName);
                    if (tableExist) {
                        columnsObjList = componentUtilities.getTableColumnsOpt((Connection) connection, connObj,
                                tableName);
                        columnsList = (List) columnsObjList.stream().map(rowdata -> rowdata[2])
                                .collect(Collectors.toList());
                    } else {
                        JSONArray connectedFromOpIdArray = processJobComponentsService
                                .getConnectedFromOperatorIds(request, selectedOperatorId, flowchartData);
                        String connectedFromOpId = String.valueOf(connectedFromOpIdArray.get(0));

//                        String connectedFromOpId = String.valueOf(((JSONArray) selectedOperator.get("connectedFrom")).get(0));
                        JSONObject fromOperator = (JSONObject) operators.get(connectedFromOpId);
                        columnsList = (List) fromOperator.get("simpleColumnsList");

                    }

                }

                if (columnsList != null && !columnsList.isEmpty()) {

                    divStr += "<div class=\"visionUniqueColsDiv\" ><table class=\"visionUniqueCols\" style=\"width: 20%;\" border=\"1\" >"
                            + "<thead>" + "<tr>"
                            + "<th style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">Column</th>"
                            + "<th style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">Unique Key<input class='visionUniqueCompSelectAll' id='uniqueCompSelectAll' type='checkbox'  /></th>"
                            + "</tr>" + "</thead>" + "<tbody>";

                    for (int i = 0; i < columnsList.size(); i++) {
                        String columnName = columnsList.get(i);
                        String checked = (uniqueKeys.contains(columnName)) ? "checked" : "";
                        divStr += "<tr>" + "<td>" + columnName
                                + "</td><td><input class='visionUniqueKeyColSelectBox' type='checkbox'   value = '"
                                + columnName + "' " + checked + " /></td>" + "</tr>";

                        simpleColumnsList.add(columnName);
                    }

                    divStr += "</tbody>" + "</table></div>";
                }
            } else {
                String filePath = (String) connObj.get("filePath");
                List headers = componentUtilities.getHeadersOfImportedFile(request, filePath);
                List validHeaders = componentUtilities.fileHeaderValidations(headers);
                simpleColumnsList.addAll(validHeaders);
                divStr += "<div class=\"visionUniqueColsDiv\" ><table class=\"visionUniqueCols\"  style=\"width: 20%;\" border=\"1\" >"
                        + "<thead>" + "<tr>"
                        + "<th style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">Column</th>"
                        + "<th style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">Unique Key<input class='visionUniqueCompSelectAll' id='uniqueCompSelectAll' type='checkbox'  /></th>"
                        + "</tr>" + "</thead>" + "<tbody>";

                for (int i = 0; i < headers.size(); i++) {
                    String columnName = (String) validHeaders.get(i);
                    String checked = (uniqueKeys.contains(columnName)) ? "checked" : "";
//                    String colValue = columnName.replaceAll(" ", "_");

                    divStr += "<tr>" + "<td>" + headers.get(i)
                            + "</td><td><input class='visionUniqueKeyColSelectBox' type='checkbox' datatype='VARCHAR2(4000)'  value = '"
                            + columnName + "' " + checked + " /></td>" + "</tr>";
                }

                divStr += "</tbody>" + "</table></div>";
            }

            resultObj.put("result", divStr);
            resultObj.put("simpleColumnsList", simpleColumnsList);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    public JSONObject mergeComponentTrfmRules(HttpServletRequest request, HttpServletResponse response) {

        JSONObject resultObj = new JSONObject();
        Object connection = null;
        List<Object[]> columnsObjList = new ArrayList();
        List<String> columnsList = new ArrayList();
        try {
            List simpleColumnsList = new ArrayList();
            String tableName = request.getParameter("tableName");
            String connObjStr = request.getParameter("connObj");
            JSONObject connObj = (JSONObject) JSONValue.parse(connObjStr);

            String selectedOperatorId = request.getParameter("selectedOperatorId");

            String flowchartDataStr = request.getParameter("flowchartData");
            JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
            JSONObject operators = (JSONObject) flowchartData.get("operators");
            JSONObject selectedOperator = (JSONObject) operators.get(selectedOperatorId);

            //JSONArray primaryKeys = new JSONArray();
            //JSONArray updateCols = new JSONArray();
            //String operatorType = "Insert Or Update";
            //String selected = "";
            //JSONObject trfmRules = new JSONObject();
            //if (selectedOperator != null && !selectedOperator.isEmpty()) {
            //	trfmRules = (JSONObject) selectedOperator.get("trfmRules");
            //if (trfmRules != null && !trfmRules.isEmpty()) {
            //primaryKeys = (JSONArray) trfmRules.get("uniqueKeys");
            //updateCols = (JSONArray) trfmRules.get("updateColsList");
            //operatorType = (trfmRules.get("operatorType") != null) ? (String) trfmRules.get("operatorType")
            //		: "Insert Or Update";
            //}
            //}
//			String divStr = "";
//			divStr += "<div class='visionOperatorType'>"
//					+ "<div class='visionMergeOperatorType'><span>Operator Type</span></div>"
//					+ "<div class='visionSelectMergeOperatorType'>" + "<select id='operatorType'>" + "<option "
//					+ ((operatorType.equalsIgnoreCase("Insert")) ? " selected " : "") + " >Insert</option>" + "<option "
//					+ ((operatorType.equalsIgnoreCase("Update")) ? " selected " : "") + " >Update</option>"
//					// + "<option " + ((operatorType.equalsIgnoreCase("Delete")) ? " selected " :
//					// "") + " >Delete</option>"
//					+ "<option " + ((operatorType.equalsIgnoreCase("Insert Or Update")) ? " selected " : "")
//					+ " >Insert Or Update</option>"
//					// + "<option " + ((operatorType.equalsIgnoreCase("Delete or Insert")) ? "
//					// selected " : "") + " >Delete or Insert</option>"
//					+ "</select>" + "</div>" + "</div>";
//			divStr += "<div class=\"visionMergeColsTableDiv\"   ><table class=\"visionMergeColsTable\" style=\"width: 40%;\" border=\"1\" >"
//					+ "<thead>" + "<tr>"
//					+ "<th style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">Column</th>"
//					+ "<th style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">PK <input class='visionUniqueCompSelectAll' id='uniqueCompSelectAll' type='checkbox'  /></th>"
//					+ "<th style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">Update <input class='visionUpdateCompSelectAll' id='updateCompSelectAll' type='checkbox'  /></th>"
//					+ "</tr>" + "</thead>" + "<tbody></div>";
//
//			if (connObj.containsKey("HOST_NAME")) {
//				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(connObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(connObj.get("CONN_CUST_COL1")))) {
//					connection = (JCO.Client) componentUtilities.getConnection(connObj);
//					columnsObjList = componentUtilities.getSAPTableColumnsWithType(request, (JCO.Client) connection,
//							tableName);
//				} else {
//					connection = (Connection) componentUtilities.getConnection(connObj);
//					columnsObjList = componentUtilities.getTableColumnsOpt((Connection) connection, connObj, tableName);
//				}
//				if (!(columnsObjList != null && !columnsObjList.isEmpty())) {
//					JSONArray connectedFromOpIdArray = processJobComponentsService.getConnectedFromOperatorIds(request,
//							selectedOperatorId, flowchartData);
//					String connectedFromOpId = String.valueOf(connectedFromOpIdArray.get(0));
//
//					JSONArray connectedFrom = processJobComponentsService.getConnectedFromOperatorIds(request,
//							selectedOperatorId, flowchartData);
//					String fromOperatorId = String.valueOf(connectedFrom.get(0));
//					JSONObject fromOperator = (JSONObject) operators.get(fromOperatorId);
//					columnsList = (List) fromOperator.get("simpleColumnsList");
//				} else {
//					columnsList = (List) columnsObjList.stream().map(rowData -> rowData[2])
//							.collect(Collectors.toList());
//				}
//
//				if (columnsList != null && !columnsList.isEmpty()) {
//
//					for (int i = 0; i < columnsList.size(); i++) {
//						String columnName = (String) columnsList.get(i);
//						String pkchecked = (primaryKeys.contains(columnName)) ? "checked" : "";
//						String updatechecked = (updateCols.contains(columnName)) ? "checked" : "";
//						divStr += "<tr>" + "<td>" + columnName + "</td>"
//								+ "<td><input class='visionUniqueKeyColSelectBox' type='checkbox'   value = '"
//								+ columnName + "' " + pkchecked + " /></td>"
//								+ "<td><input class='visionUpdateColSelectBox' type='checkbox'   value = '" + columnName
//								+ "' " + updatechecked + "  /></td>" + "</tr>";
//
//						simpleColumnsList.add(columnName);
//					}
//
//					divStr += "</tbody>" + "</table>";
//				}
//			} else {
//				String filePath = (String) connObj.get("filePath");
//				List headers = componentUtilities.getHeadersOfImportedFile(request, filePath);
//				List validHeaders = componentUtilities.fileHeaderValidations(headers);
//				simpleColumnsList.addAll(validHeaders);
//
//				for (int i = 0; i < headers.size(); i++) {
//					String columnName = (String) validHeaders.get(i);
//					String pkchecked = (primaryKeys.contains(columnName)) ? "checked" : "";
//					String updatechecked = (updateCols.contains(columnName)) ? "checked" : "";
////                    String colValue = columnName.replaceAll(" ", "_");
//
//					divStr += "<tr>" + "<td>" + headers.get(i) + "</td>"
//							+ "<td><input class='visionUniqueKeyColSelectBox' type='checkbox' datatype='VARCHAR2(4000)'  value = '"
//							+ columnName + "' " + pkchecked + " /></td>"
//							+ "<td><input class='visionUpdateColSelectBox' type='checkbox'   value = '" + columnName
//							+ "' " + updatechecked + "  /></td>" + "</tr>";
//					simpleColumnsList.add(columnName);
//				}
//
//				divStr += "</tbody>" + "</table>";
//			}
            JSONObject colMappingObj = columnMappingTrfmRulesForMergeComponent(request,
                    response);
            JSONObject fromOpConnObj = (JSONObject) colMappingObj.get("fromOpConnObj");
            resultObj.putAll(colMappingObj);

//			List simpleFromColumnsList = (List)colMappingObj.get("simpleColumnsList");
//			List simpleTargetColumnsList = (List)colMappingObj.get("simpleTargetColumnsList");
//			
//			String divStr = "";
//			divStr += "<div class='visionOperatorType'>"
//					+ "<div class='visionMergeOperatorType'><span>Operator Type</span></div>"
//					+ "<div class='visionSelectMergeOperatorType'>" + "<select id='operatorType'>" + "<option "
//					+ ((operatorType.equalsIgnoreCase("Insert")) ? " selected " : "") + " >Insert</option>" + "<option "
//					+ ((operatorType.equalsIgnoreCase("Update")) ? " selected " : "") + " >Update</option>"
//					// + "<option " + ((operatorType.equalsIgnoreCase("Delete")) ? " selected " :
//					// "") + " >Delete</option>"
//					+ "<option " + ((operatorType.equalsIgnoreCase("Insert Or Update")) ? " selected " : "")
//					+ " >Insert Or Update</option>"
//					// + "<option " + ((operatorType.equalsIgnoreCase("Delete or Insert")) ? "
//					// selected " : "") + " >Delete or Insert</option>"
//					+ "</select>" + "</div>" + "</div>";
//			divStr += "<div class=\"visionMergeColsTableDiv\"   ><table class=\"visionMergeColsTable\" style=\"width: 40%;\" border=\"1\" >"
//					+ "<thead>" + "<tr>"
//					+ "<th style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">Source Column</th>"
//					+ "<th style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">Target Column</th>"
//					+ "<th style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">PK <input class='visionUniqueCompSelectAll' id='uniqueCompSelectAll' type='checkbox'  /></th>"
//					+ "<th style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">Update <input class='visionUpdateCompSelectAll' id='updateCompSelectAll' type='checkbox'  /></th>"
//					+ "</tr>" + "</thead>" + "<tbody></div>";
//			for (int i = 0; i < simpleFromColumnsList.size(); i++) {
//				String fromColumnName = (String) simpleFromColumnsList.get(i);
//				String targetColumnName = (String) simpleTargetColumnsList.get(i);
//				String pkchecked = (primaryKeys.contains(fromColumnName)) ? "checked" : "";
//				String updatechecked = (updateCols.contains(fromColumnName)) ? "checked" : "";
////                String colValue = columnName.replaceAll(" ", "_");
//				
//				divStr += "<tr>" 
//						+ "<td>" + fromColumnName + "</td>"
//						+ "<td>" + targetColumnName + "</td>"
//						+ "<td><input class='visionUniqueKeyColSelectBox' type='checkbox' datatype='VARCHAR2(4000)' fromColumnName='"+fromColumnName+"' targetColumnName='"+targetColumnName+"' value = '"
//						+ fromColumnName + "' " + pkchecked + " /></td>"
//						+ "<td><input class='visionUpdateColSelectBox' type='checkbox' fromColumnName='\"+fromColumnName+\"' targetColumnName='\"+targetColumnName+\"'  value = '" + fromColumnName
//						+ "' " + updatechecked + "  /></td>" + "</tr>";
//				simpleColumnsList.add(fromColumnName);
//			}
//			divStr += "</tbody>" + "</table>";
//			
//			resultObj.put("result", divStr);
            //resultObj.put("simpleColumnsList", simpleColumnsList);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    public JSONObject columnMappingTrfmRules(HttpServletRequest request, HttpServletResponse response) {

        JSONObject columnsObject = new JSONObject();
        JSONArray sourceTablesArray = new JSONArray();
        JSONArray fromTableColsArray = new JSONArray();
        JSONArray toTableColsArray = new JSONArray();
        Connection fromConnection = null;
        Connection toConnection = null;
        JCO.Client fromJCOConnection = null;
        JCO.Client toJCOConnection = null;

        JSONObject fromConnectObj = new JSONObject();
        JSONObject toConnectObj = new JSONObject();

        try {
            String flowchartDataStr = request.getParameter("flowchartData");
            JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
            JSONObject operators = (JSONObject) flowchartData.get("operators");

            String sourceOperatorsStr = request.getParameter("sourceOperators");
//            JSONObject sourceOperatorsObj = (JSONObject) JSONValue.parse(sourceOperatorsStr);
            List<Map> fromOperatorsList = (List<Map>) JSONValue.parse(sourceOperatorsStr);

            String selectedOperatorId = request.getParameter("selectedOperatorId");
            JSONObject fromOpConnObj = new JSONObject();
            List simpleColumnsList = new ArrayList();
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
                        List<String> headers = componentUtilities.getHeadersOfImportedFile(request,
                                (String) fromConnectObj.get("filePath"));

                        headers = componentUtilities.fileHeaderValidations(headers);
                        simpleColumnsList.addAll(headers);
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
                                    columnObj.put("id", fromConnectObj.get("fileName") + ":" + headerName);
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

                    } else {

                        fromOpConnObj.put(fromOperatorMap.get("tableName"), fromConnectObj);
                        JSONArray fromTableColsArrayForClause = new JSONArray();
                        Object fromConnObj = componentUtilities.getConnection(fromConnectObj);
                        if (fromConnObj instanceof Connection || fromConnObj instanceof JCO.Client) {
                            if (fromConnObj instanceof Connection) {
                                fromConnection = (Connection) fromConnObj;
                            } else if (fromConnObj instanceof JCO.Client) {
                                fromJCOConnection = (JCO.Client) fromConnObj;
                            }
                            List<Object[]> fromTableColumnList = new ArrayList<>();
                            String tableName = (String) fromOperatorMap.get("statusLabel");
                            String tableNameLabel = (String) fromOperatorMap.get("tableNameLabel");
                            boolean tableExist = false;
                            if (fromConnObj instanceof Connection) {
                                tableExist = checkTableExsist(fromConnection,
                                        (String) fromOperatorMap.get("statusLabel"));
                                if (tableExist) {
                                    fromTableColumnList = componentUtilities.getTreeDMTableColumnsOpt(fromConnection,
                                            request, fromConnectObj, (String) fromOperatorMap.get("statusLabel"));

                                } else {
                                }
                            } else if (fromConnObj instanceof JCO.Client) {
                                tableExist = true;
                                fromTableColumnList = componentUtilities.getSAPTableColumns(request, fromJCOConnection,
                                        (String) fromOperatorMap.get("statusLabel"));
                            }

                            sourceTablesArray.add(tableName);
                            fromOperatorMap.put("tableName", tableName);

                            if (tableName != null && !"".equalsIgnoreCase(tableName)) {
                                String treeObjTableName = "";
                                String tableId = "";

                                if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))) {
                                    treeObjTableName = tableName;
                                    tableId = tableName;
                                } else if (!tableExist) {
                                    treeObjTableName = tableNameLabel;
                                    tableId = tableName;
                                } else {
                                    treeObjTableName = fromConnectObj.get("CONN_USER_NAME") + "." + tableName;
                                    tableId = fromConnectObj.get("CONN_USER_NAME") + "." + tableName;;
                                }
                                JSONObject tableObj = new JSONObject();
                                tableObj.put("id", tableId);
                                tableObj.put("text", treeObjTableName);
                                tableObj.put("value", treeObjTableName);
                                tableObj.put("icon", "images/GridDB.svg");
                                fromTableColsArray.add(tableObj);
                                fromTableColsArrayForClause.add(tableObj);
                                List<String> columnsList = new ArrayList();
                                if (tableExist) {
                                    columnsList = fromTableColumnList.stream()
                                            .filter(tableColsArray -> (tableName
                                            .equalsIgnoreCase(String.valueOf(tableColsArray[0]))))
                                            .map(tableColsArray -> String.valueOf(tableColsArray[1]))
                                            .collect(Collectors.toList());
                                } else {
                                    columnsList = (List) fromOperatorMap.get("simpleColumnsList");
                                    if(sourceOperatorsStr.contains("VALIDATE")) {
                                    	columnsList.add("VALIDATIONS");
                                    }
                                }

                                List<String> fromOperatorsColumnsList = getSourceColumnsList(fromOperatorMap);
                                if (fromOperatorsColumnsList != null && !fromOperatorsColumnsList.isEmpty()) {
                                    simpleColumnsList.addAll(fromOperatorsColumnsList);
                                } else {
                                    simpleColumnsList.addAll(columnsList);
                                }

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
                                        String treeObjColumnId = "";
                                        String treeObjColumnText = "";
                                        String treeObjColumnValue = "";
                                        String treeObjColumnParentId = "";
                                        if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))) {
                                            treeObjColumnId = tableName + ":" + columnsList.get(j);
                                            treeObjColumnText = columnsList.get(j);
                                            treeObjColumnValue = tableName + ":" + columnsList.get(j);
                                            treeObjColumnParentId = tableId;
                                        } else if (!tableExist) {
                                            treeObjColumnId = tableName + ":" + columnsList.get(j);
                                            treeObjColumnText = columnsList.get(j);
                                            treeObjColumnValue = tableNameLabel + ":" + columnsList.get(j);
                                            treeObjColumnParentId = tableId;
                                        } else {
                                            treeObjColumnId = fromConnectObj.get("CONN_USER_NAME") + "." + tableName
                                                    + ":" + columnsList.get(j);
                                            treeObjColumnText = columnsList.get(j);
                                            treeObjColumnValue = fromConnectObj.get("CONN_USER_NAME") + "." + tableName
                                                    + ":" + columnsList.get(j);
                                            treeObjColumnParentId = tableId;
                                        }
                                        JSONObject columnObj = new JSONObject();
                                        columnObj.put("id", treeObjColumnId);
                                        columnObj.put("text", treeObjColumnText);
                                        columnObj.put("value", treeObjColumnValue);
                                        columnObj.put("parentid", treeObjColumnParentId);
                                        if (dataTypeList != null && !dataTypeList.isEmpty()) {
                                            columnObj.put("dataType",
                                                    dataTypeList.get(j) != null ? dataTypeList.get(j) : ""); // RAVI DM

                                        }

                                        fromTableColsArray.add(columnObj);
                                        fromTableColsArrayForClause.add(columnObj);

                                    }
                                }
                            }

                        } else {
                            columnsObject.put("connectionFlag", "N");
                            columnsObject.put("connectionMessage", fromConnObj);
                        }

                    } // end else for file type
                }

            } // end loop

            // destination table obj start
            String targetOperatorId = request.getParameter("selectedOperatorId");
            JSONObject targetOperator = (JSONObject) operators.get(targetOperatorId);
            String toIconType = (String) targetOperator.get("iconType");

            if (targetOperator.get("statusLabel") == null) {
                targetOperator.put("statusLabel", targetOperator.get("tableName"));
            }

            toConnectObj = (JSONObject) targetOperator.get("connObj");

//            if (toIconType != null && toIconType.equalsIgnoreCase("OUTPUT")) {
////                toIconType = "XLSX";
////                toConnectObj = null;
////                targetOperator.put("statusLabel", null);
//            }
            String destinationTableName = (String) targetOperator.get("statusLabel");
            List<Object[]> toTableColumnList = new ArrayList<>();
            Object toConnObj = componentUtilities.getConnection(toConnectObj);
            if (toConnObj instanceof Connection) {
                toConnection = (Connection) toConnObj;
            } else if (toConnObj instanceof JCO.Client) {
                toJCOConnection = (JCO.Client) toConnObj;
            }
            if (toConnObj instanceof Connection) {
                toTableColumnList = componentUtilities.getTreeDMTableColumnsOpt(toConnection, request, toConnectObj,
                        destinationTableName);
            } else if (toConnObj instanceof JCO.Client) {
                toTableColumnList = componentUtilities.getSAPTableColumns(request, toJCOConnection,
                        (String) destinationTableName);
            }
            if (destinationTableName != null && !"".equalsIgnoreCase(destinationTableName)) {
                JSONObject tableObj = new JSONObject();
                tableObj.put("id",
                        ("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1")))
                        ? destinationTableName
                        : toConnectObj.get("CONN_USER_NAME") + "." + destinationTableName));
                tableObj.put("text", toConnectObj.get("CONNECTION_NAME") + "." + destinationTableName);
                tableObj.put("value",
                        ("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1")))
                        ? destinationTableName
                        : toConnectObj.get("CONN_USER_NAME") + "." + destinationTableName));
                tableObj.put("icon", "images/GridDB.svg");
                toTableColsArray.add(tableObj);

                List<String> columnsList = toTableColumnList.stream().filter(
                        tableColsArray -> (destinationTableName.equalsIgnoreCase(String.valueOf(tableColsArray[0]))))
                        .map(tableColsArray -> String.valueOf(tableColsArray[1])).collect(Collectors.toList());

                // RAVI DM
                List<String> dataTypeList = new ArrayList();
                try {
                    dataTypeList = toTableColumnList.stream()
                            .filter(tableColsArray -> (destinationTableName
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
                        columnObj.put("id",
                                (("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1")))
                                ? destinationTableName
                                : toConnectObj.get("CONN_USER_NAME") + "." + destinationTableName) + ":"
                                + columnsList.get(j)));
                        columnObj.put("text", columnsList.get(j));
                        columnObj.put("value",
                                (("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1")))
                                ? destinationTableName
                                : toConnectObj.get("CONN_USER_NAME") + "." + destinationTableName) + ":"
                                + columnsList.get(j)));
                        columnObj.put("parentid",
                                ("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1")))
                                ? destinationTableName
                                : toConnectObj.get("CONN_USER_NAME") + "." + destinationTableName));
                        if (dataTypeList != null && !dataTypeList.isEmpty()) {
                            columnObj.put("dataType", dataTypeList.get(j) != null ? dataTypeList.get(j) : ""); // RAVI
                            // DM

                        }

                        toTableColsArray.add(columnObj);

                    }

                }
            }

            // dest table obj end
            JSONObject trfmRules = targetOperator.get("trfmRules") != null ? (JSONObject) targetOperator.get("trfmRules") : new JSONObject();
            String checkBoxVal = (trfmRules.get("skipRejectedRecords") != null && "Y".equalsIgnoreCase(String.valueOf(trfmRules.get("skipRejectedRecords")))) ? "checked " : "";
            String truncateCheckBoxVal = (trfmRules.get("truncateDestination") != null && "Y".equalsIgnoreCase(String.valueOf(trfmRules.get("truncateDestination")))) ? "checked " : "";
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
                    + "<div id='skipRejectedRecordsDiv' ><input id='skipRejectedRecords' type='checkbox' " + checkBoxVal + " /><span> Skip on error</span>"
                    + "<input type='hidden' id='selectedOperatorIdValue' value='" + selectedOperatorId + "'></div>"
                    + "<div id='truncateDestinationDiv' ><input id='truncateDestination' type='checkbox' " + truncateCheckBoxVal + " /><span> Truncate Destination</span></div>"
                    + "</div>" + "" + "<div id='visionColMappScrollDiv' class='visionColMappScrollDiv1'>"
                    + "<table id=\"sourceDestColsTableId\" class=\"visionEtlJoinClauseTable visionEtlSourceDestColsTable1\" style='width: 100%;' border='1'>"
                    + "<thead>" + "<tr>"
                    + "<th width='1.5%' class=\"visionColMappingImgTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'></th>";

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

//            columnMappingStr += "<tr style = 'height: 1px'>"
//                    + "<td width='1.5%' class=\"visionColMappingImgTd1\" ><img src=\"images/Delete_Red_Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionColMappingImg\""
//                    + " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>"
//                    + "</td>";
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
                    + "<td width='20%'><input class='visionColMappingTextarea' type='text' value='' />"
                    + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                    + " onclick=\"selectNumberGeneration(this)\" style=\"\"></td>"
                    + "<td width='20%' style='display:none;'><input class='visionColMappingTextarea' type='text' value=''></td>"
                    + "<td width='20%'><input id='visionETLFuncFromColId' class='visionColMappingInput' type='text' value=''>"
                    + "<img title='Select Function' src=\"images/Fx icon-01.svg\" class=\"visionETLColMapImage \" "
                    + " onclick=\"selectColumnFun(this,'fromColumn')\" style=\"\">" + "</td>" + "</tr>";

            if (trfmRules != null && !trfmRules.isEmpty()) {
                JSONArray columnMappingData = (JSONArray) trfmRules.get("colMappingsData");

                if (columnMappingData != null && !columnMappingData.isEmpty()) {

                    for (int i = 0; i < columnMappingData.size(); i++) {

                        JSONObject rowData = (JSONObject) columnMappingData.get(i);
                        String primaryKey = (String) rowData.get("primaryKey");
                        String pkChecked = (primaryKey != null && "Y".equalsIgnoreCase(primaryKey) ? " checked " : "");
                        String destinationColumn = (String) rowData.get("destinationColumn");
                        String destinationColumnActualValue = (String) rowData.get("destinationColumnActualValue");
                        String destTable = (rowData.get("destTable") != null) ? rowData.get("destTable").toString()
                                : "";

                        String sourceColumn = (String) rowData.get("sourceColumn");
                        String sourceColumnActualValue = (String) rowData.get("sourceColumnActualValue");

                        String sourceTable = (rowData.get("sourceTable") != null)
                                ? rowData.get("sourceTable").toString()
                                : "";

                        String defaultValue = (String) rowData.get("defaultValue");
                        String appendValue = (String) rowData.get("appendValue");
                        String columnClause = (String) rowData.get("columnClause");
                        String columnClauseActualValue = (String) rowData.get("columnClauseActualValue");
                        String dataFunTables = (rowData.get("dataFunTables") != null)
                                ? rowData.get("dataFunTables").toString()
                                : "";
                        String funcolumnslist = (rowData.get("funcolumnslist") != null)
                                ? rowData.get("funcolumnslist").toString()
                                : "";
                        String dataFunobjstr = (String) rowData.get("data-funobjstr");
                        String dataColumnClause = (String) rowData.get("data-columnClause");

                        columnMappingStr += "<tr style = 'height: 1px'>"
                                + "<td width='1.5%' class=\"visionColMappingImgTd1\" ><img src=\"images/Delete_Red_Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionColMappingImg\""
                                + " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>" + "</td>";

                        columnMappingStr += "<td width='2%' class=\"visionColMappingImgTd1\" ><input type=\"checkbox\" class=\"visionPKSelectCbx\" "
                                + pkChecked + " />" + "</td>"; // ravi pk
                        if (toIconType != null && !"".equalsIgnoreCase(toIconType)
                                && !"null".equalsIgnoreCase(toIconType) && !"SQL".equalsIgnoreCase(toIconType)) {
                            columnMappingStr += "<td width='19%' ><input class='visionColMappingInput' type='text' tableName='"
                                    + destTable + "' value='" + destinationColumn + "'  actual-value='"
                                    + destinationColumnActualValue + "' />"
                                    + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                                    + " onclick=\"selectColumn(this,'"
                                    + ((toTableColsArray != null && !toTableColsArray.isEmpty()) ? "toColumn"
                                    : "fromColumn")
                                    + "')\"" + " style=\"\"></td>";
                        } else {
                            columnMappingStr += "<td width='19%' ><input class='visionColMappingInput' type='text' tableName='"
                                    + destTable + "' value='" + destinationColumn + "' actual-value='"
                                    + destinationColumnActualValue + "' />"
                                    + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                                    + " onclick=\"selectColumn(this,'toColumn')\" style=\"\"></td>";
                        }

                        columnMappingStr += "<td width='20%'><input class='visionColMappingInput' type='text' tableName='"
                                + sourceTable + "' value='" + sourceColumn + "' actual-value='"
                                + sourceColumnActualValue + "'  readonly='true'/>"
                                + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                                + " onclick=\"selectColumn(this,'fromColumn')\" style=\"\"></td>"
                                + "<td width='20%'><input class='visionColMappingTextarea' type='text' value='"
                                + defaultValue + "' >"
                                + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                                + " onclick=\"selectNumberGeneration(this)\" style=\"\"></td>"
                                + "<td width='20%' style='display:none;'><input class='visionColMappingTextarea' type='text' value='"
                                + appendValue + "'></td>"
                                + "<td width='20%'><input id='visionETLFuncFromColId' class='visionColMappingInput' type='text' tableName='"
                                + dataFunTables + "'  value='" + columnClause + "' value='" + columnClauseActualValue
                                + "' actual-value='" + columnClauseActualValue + "'   funcolumnslist='" + funcolumnslist + "'   >"
                                + "<img title='Select Function' src=\"images/Fx icon-01.svg\" class=\"visionETLColMapImage \" "
                                + " onclick=\"selectColumnFun(this,'fromColumn')\" style=\"\">" + "</td>" + "</tr>";

                    }
                }
            } else {
                columnMappingStr += colMapSingleTrString;
            }

            columnsObject.put("selectedColumnStr", colMapSingleTrString);
            columnsObject.put("colMappingStr", columnMappingStr);
            columnsObject.put("toTableColsArray", toTableColsArray);
            columnsObject.put("fromTableColsArray", fromTableColsArray);
            columnsObject.put("sourceTablesArray", sourceTablesArray);
            columnsObject.put("simpleColumnsList", simpleColumnsList);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return columnsObject;
    }

    public JSONObject columnMappingTrfmRulesQueryComp(HttpServletRequest request, HttpServletResponse response) {

        JSONObject columnsObject = new JSONObject();
        JSONArray sourceTablesArray = new JSONArray();
        JSONArray fromTableColsArray = new JSONArray();
        JSONArray toTableColsArray = new JSONArray();
        Connection fromConnection = null;
        Connection queryConnection = null;
        Connection toConnection = null;
        JCO.Client fromJCOConnection = null;
        JCO.Client toJCOConnection = null;

        JSONObject fromConnectObj = new JSONObject();
        JSONObject toConnectObj = new JSONObject();

        try {
            String flowchartDataStr = request.getParameter("flowchartData");
            JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
            JSONObject operators = (JSONObject) flowchartData.get("operators");

            String sourceOperatorsStr = request.getParameter("sourceOperators");
//            JSONObject sourceOperatorsObj = (JSONObject) JSONValue.parse(sourceOperatorsStr);
            List<Map> fromOperatorsList = (List<Map>) JSONValue.parse(sourceOperatorsStr);

            JSONObject fromOpConnObj = new JSONObject();
            List simpleColumnsList = new ArrayList();
            for (Map fromOperatorMap : fromOperatorsList) {
                if (fromOperatorMap != null && !fromOperatorMap.isEmpty()) {
                    fromConnectObj = (JSONObject) fromOperatorMap.get("connObj");

                    List fromColumnsList = new ArrayList();
                    List fromAliasColumnsList = new ArrayList();
                    List toColumnsList = new ArrayList();

                    JSONObject connObj = (JSONObject) fromOperatorMap.get("connObj");

                    JSONObject trfmRules = (JSONObject) fromOperatorMap.get("trfmRules");
                    String query = (String) trfmRules.get("queryData");
                    query = query.toUpperCase();

                    JSONObject queryConnObj = (JSONObject) fromOperatorMap.get("queryConnObj");
                    queryConnection = (Connection) componentUtilities.getConnection(queryConnObj);

//                    int toIndex = query.indexOf("FROM");
//                    String columnsStr = query.substring(6, toIndex);
//                    String restOfQuery = query.substring(toIndex);
//                    String columnsListStr = "";
//
//                    Pattern pattern = Pattern.compile("[^ _.a-zA-Z0-9]", Pattern.CASE_INSENSITIVE);
//                    String[] colsArray = columnsStr.split(",");
//                    for (int i = 0; i < colsArray.length; i++) {
//                        String column = colsArray[i];
//                        String fromColumn = column;
//                        if (column != null && column.contains(" AS ")) {
//                            String fromColumn0 = column.split(" AS ")[0];
//                            String fromColumn1 = column.split(" AS ")[1];
//                            fromColumn0 = fromColumn0.trim();
//                            if ("''".equalsIgnoreCase(fromColumn0)) {
//                                fromColumn0 = "'                                        '";
//                            }
//                            fromColumnsList.add(fromColumn0 + " AS " + fromColumn1);
//                        } else if (pattern.matcher(column).find()) {
//                            if ("''".equalsIgnoreCase(column)) {
//                                column = "'                                        ' AS " + " COLUMN_" + i;
//                            } else {
//                                column = column + " AS " + " COLUMN_" + i;
//                            }
//
//                            fromColumnsList.add(column);
//                        } else {
//                            fromColumnsList.add(column);
//                        }
//
//                        toColumnsList.add(column);
//                    }
//
//                    for (int i = 0; i < fromColumnsList.size(); i++) {
//                        if (i < (fromColumnsList.size() - 1)) {
//                            columnsListStr += fromColumnsList.get(i) + ", ";
//                        } else {
//                            columnsListStr += fromColumnsList.get(i);
//                        }
//
//                    }
//                    fromAliasColumnsList.addAll(toColumnsList);
//
//                    query = "SELECT " + columnsListStr + " " + restOfQuery;
                    String selectQuery = "SELECT * FROM (" + query + ") WHERE 1=2";
                    fromConnection = (Connection) componentUtilities.getConnection(connObj);
                    JSONObject columnsObj = componentUtilities.getColumnsObjFromQuery(selectQuery, queryConnection,
                            queryConnObj);
                    List dataTypeList = (List) columnsObj.get("dataTypesList");

                    List<String> columnsList = (List) columnsObj.get("columnsList");

                    simpleColumnsList.addAll(columnsList);

                    JSONArray fromTableColsArrayForClause = new JSONArray();
                    Object fromConnObj = componentUtilities.getConnection(fromConnectObj);

//                    String tableName = (String) "ZZ_TEMP_" + fromOperatorMap.get("timeStamp");
                    String tableName = (String) fromOperatorMap.get("iconType") + "_OUTPUT_"
                            + fromOperatorMap.get("operatorId");

                    sourceTablesArray.add(tableName);
                    fromOperatorMap.put("tableName", tableName);
//                    fromOpConnObj.put(tableName, fromConnectObj);
                    if (tableName != null && !"".equalsIgnoreCase(tableName)) {
                        JSONObject tableObj = new JSONObject();
                        tableObj.put("id", tableName);
                        tableObj.put("text", tableName);
                        tableObj.put("value", tableName);
                        tableObj.put("icon", "images/GridDB.svg");
                        fromTableColsArray.add(tableObj);
                        fromTableColsArrayForClause.add(tableObj);

                        for (int j = 0; j < columnsList.size(); j++) {
                            if (columnsList.get(j) != null && !"".equalsIgnoreCase(columnsList.get(j))) {
                                JSONObject columnObj = new JSONObject();
                                columnObj.put("id", columnsList.get(j));
                                columnObj.put("text", columnsList.get(j));
                                columnObj.put("value", columnsList.get(j));
                                columnObj.put("parentid", tableName);
                                if (dataTypeList != null && !dataTypeList.isEmpty()) {
                                    columnObj.put("dataType", dataTypeList.get(j) != null ? dataTypeList.get(j) : ""); // RAVI
                                    // DM

                                }

                                fromTableColsArray.add(columnObj);
                                fromTableColsArrayForClause.add(columnObj);
                            }

                        }
                    }

                }

            } // end loop

            // destination table obj start
            String targetOperatorId = request.getParameter("selectedOperatorId");
            JSONObject targetOperator = (JSONObject) operators.get(targetOperatorId);
            String toIconType = (String) targetOperator.get("iconType");

            if (targetOperator.get("statusLabel") == null) {
                targetOperator.put("statusLabel", targetOperator.get("tableName"));
            }
            String destinationTableName = (String) targetOperator.get("statusLabel");
            toConnectObj = (JSONObject) targetOperator.get("connObj");

            List<Object[]> toTableColumnList = new ArrayList<>();
            Object toConnObj = componentUtilities.getConnection(toConnectObj);
            if (toConnObj instanceof Connection) {
                toConnection = (Connection) toConnObj;
            } else if (toConnObj instanceof JCO.Client) {
                toJCOConnection = (JCO.Client) toConnObj;
            }
            if (toConnObj instanceof Connection) {
                toTableColumnList = componentUtilities.getTreeDMTableColumnsOpt(toConnection, request, toConnectObj,
                        destinationTableName);
            } else if (toConnObj instanceof JCO.Client) {
                toTableColumnList = componentUtilities.getSAPTableColumns(request, toJCOConnection,
                        (String) destinationTableName);
            }
            if (destinationTableName != null && !"".equalsIgnoreCase(destinationTableName)) {
                JSONObject tableObj = new JSONObject();
                tableObj.put("id",
                        ("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1")))
                        ? destinationTableName
                        : toConnectObj.get("CONN_USER_NAME") + "." + destinationTableName));
                tableObj.put("text", fromConnectObj.get("CONNECTION_NAME") + "." + destinationTableName);
                tableObj.put("value",
                        ("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1")))
                        ? destinationTableName
                        : toConnectObj.get("CONN_USER_NAME") + "." + destinationTableName));
                tableObj.put("icon", "images/GridDB.svg");
                toTableColsArray.add(tableObj);

                List<String> columnsList = toTableColumnList.stream().filter(
                        tableColsArray -> (destinationTableName.equalsIgnoreCase(String.valueOf(tableColsArray[0]))))
                        .map(tableColsArray -> String.valueOf(tableColsArray[1])).collect(Collectors.toList());

                // RAVI DM
                List<String> dataTypeList = new ArrayList();
                try {
                    dataTypeList = toTableColumnList.stream()
                            .filter(tableColsArray -> (destinationTableName
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
                        columnObj.put("id",
                                (("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1")))
                                ? destinationTableName
                                : toConnectObj.get("CONN_USER_NAME") + "." + destinationTableName) + ":"
                                + columnsList.get(j)));
                        columnObj.put("text", columnsList.get(j));
                        columnObj.put("value",
                                (("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1")))
                                ? destinationTableName
                                : toConnectObj.get("CONN_USER_NAME") + "." + destinationTableName) + ":"
                                + columnsList.get(j)));
                        columnObj.put("parentid",
                                ("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1")))
                                ? destinationTableName
                                : toConnectObj.get("CONN_USER_NAME") + "." + destinationTableName));
                        if (dataTypeList != null && !dataTypeList.isEmpty()) {
                            columnObj.put("dataType", dataTypeList.get(j) != null ? dataTypeList.get(j) : ""); // RAVI
                            // DM

                        }

                        toTableColsArray.add(columnObj);

                    }

                }
            }

            // dest table obj end
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

//            columnMappingStr += "<tr style = 'height: 1px'>"
//                    + "<td width='1.5%' class=\"visionColMappingImgTd1\" ><img src=\"images/Delete_Red_Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionColMappingImg\""
//                    + " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>"
//                    + "</td>";
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
                    + "<td width='20%'><input class='visionColMappingTextarea' type='text' value='' />"
                    + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                    + " onclick=\"selectNumberGeneration(this)\" style=\"\"></td>"
                    + "<td width='20%' style='display:none;'><input class='visionColMappingTextarea' type='text' value=''></td>"
                    + "<td width='20%'><input id='visionETLFuncFromColId' class='visionColMappingInput' type='text' value=''>"
                    + "<img title='Select Function' src=\"images/Fx icon-01.svg\" class=\"visionETLColMapImage \" "
                    + " onclick=\"selectColumnFun(this,'fromColumn')\" style=\"\">" + "</td>" + "</tr>";

            JSONObject trfmRules = (JSONObject) targetOperator.get("trfmRules");
            if (trfmRules != null && !trfmRules.isEmpty()) {
                JSONArray columnMappingData = (JSONArray) trfmRules.get("colMappingsData");

                if (columnMappingData != null && !columnMappingData.isEmpty()) {

                    for (int i = 0; i < columnMappingData.size(); i++) {

                        JSONObject rowData = (JSONObject) columnMappingData.get(i);
                        String primaryKey = (String) rowData.get("primaryKey");
                        String pkChecked = (primaryKey != null && "Y".equalsIgnoreCase(primaryKey) ? " checked " : "");
                        String destinationColumn = (String) rowData.get("destinationColumn");
                        String destinationColumnActualValue = (String) rowData.get("destinationColumnActualValue");
                        String destTable = (rowData.get("destTable") != null) ? rowData.get("destTable").toString()
                                : "";

                        String sourceColumn = (String) rowData.get("sourceColumn");
                        String sourceColumnActualValue = (String) rowData.get("sourceColumnActualValue");

                        String sourceTable = (rowData.get("sourceTable") != null)
                                ? rowData.get("sourceTable").toString()
                                : "";

                        String defaultValue = (String) rowData.get("defaultValue");
                        String appendValue = (String) rowData.get("appendValue");
                        String columnClause = (String) rowData.get("columnClause");
                        String columnClauseActualValue = (String) rowData.get("columnClauseActualValue");
                        String dataFunTables = (rowData.get("dataFunTables") != null)
                                ? rowData.get("dataFunTables").toString()
                                : "";
                        String funcolumnslist = (rowData.get("funcolumnslist") != null)
                                ? rowData.get("funcolumnslist").toString()
                                : "";
                        String dataFunobjstr = (String) rowData.get("data-funobjstr");
                        String dataColumnClause = (String) rowData.get("data-columnClause");

                        columnMappingStr += "<tr style = 'height: 1px'>"
                                + "<td width='1.5%' class=\"visionColMappingImgTd1\" ><img src=\"images/Delete_Red_Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionColMappingImg\""
                                + " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>" + "</td>";

                        columnMappingStr += "<td width='2%' class=\"visionColMappingImgTd1\" ><input type=\"checkbox\" class=\"visionPKSelectCbx\" "
                                + pkChecked + " />" + "</td>"; // ravi pk
                        if (toIconType != null && !"".equalsIgnoreCase(toIconType)
                                && !"null".equalsIgnoreCase(toIconType) && !"SQL".equalsIgnoreCase(toIconType)) {
                            columnMappingStr += "<td width='19%' ><input class='visionColMappingInput' type='text' tableName='"
                                    + destTable + "' value='" + destinationColumn + "'  actual-value='"
                                    + destinationColumnActualValue + "' />"
                                    + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                                    + " onclick=\"selectColumn(this,'"
                                    + ((toTableColsArray != null && !toTableColsArray.isEmpty()) ? "toColumn"
                                    : "fromColumn")
                                    + "')\"" + " style=\"\"></td>";
                        } else {
                            columnMappingStr += "<td width='19%' ><input class='visionColMappingInput' type='text' tableName='"
                                    + destTable + "' value='" + destinationColumn + "' actual-value='"
                                    + destinationColumnActualValue + "' />"
                                    + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                                    + " onclick=\"selectColumn(this,'toColumn')\" style=\"\"></td>";
                        }

                        columnMappingStr += "<td width='20%'><input class='visionColMappingInput' type='text' tableName='"
                                + sourceTable + "' value='" + sourceColumn + "' actual-value='"
                                + sourceColumnActualValue + "'  readonly='true'/>"
                                + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                                + " onclick=\"selectColumn(this,'fromColumn')\" style=\"\"></td>"
                                + "<td width='20%'><input class='visionColMappingTextarea' type='text' value='"
                                + defaultValue + "' >"
                                + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                                + " onclick=\"selectNumberGeneration(this)\" style=\"\"></td>"
                                + "<td width='20%' style='display:none;'><input class='visionColMappingTextarea' type='text' value='"
                                + appendValue + "'></td>"
                                + "<td width='20%'><input id='visionETLFuncFromColId' class='visionColMappingInput' type='text' tableName='"
                                + dataFunTables + "'  value='" + columnClause + "' actual-value='"
                                + columnClauseActualValue + "' funcolumnslist='" + funcolumnslist + "'  >"
                                + "<img title='Select Function' src=\"images/Fx icon-01.svg\" class=\"visionETLColMapImage \" "
                                + " onclick=\"selectColumnFun(this,'fromColumn')\" style=\"\">" + "</td>" + "</tr>";

                    }
                }
            } else {
                columnMappingStr += colMapSingleTrString;
            }

            columnsObject.put("selectedColumnStr", colMapSingleTrString);
            columnsObject.put("colMappingStr", columnMappingStr);
            columnsObject.put("toTableColsArray", toTableColsArray);
            columnsObject.put("fromTableColsArray", fromTableColsArray);
            columnsObject.put("sourceTablesArray", sourceTablesArray);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return columnsObject;
    }

    public JSONObject columnMappingTrfmRulesAPIComp(HttpServletRequest request, HttpServletResponse response) {

        JSONObject columnsObject = new JSONObject();
        JSONArray sourceTablesArray = new JSONArray();
        JSONArray fromTableColsArray = new JSONArray();
        JSONArray toTableColsArray = new JSONArray();
        Connection fromConnection = null;
        Connection queryConnection = null;
        Connection toConnection = null;
        JCO.Client fromJCOConnection = null;
        JCO.Client toJCOConnection = null;

        JSONObject fromConnectObj = new JSONObject();
        JSONObject toConnectObj = new JSONObject();

        try {
            String flowchartDataStr = request.getParameter("flowchartData");
            JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
            JSONObject operators = (JSONObject) flowchartData.get("operators");

            String sourceOperatorsStr = request.getParameter("sourceOperators");
//            JSONObject sourceOperatorsObj = (JSONObject) JSONValue.parse(sourceOperatorsStr);
            List<Map> fromOperatorsList = (List<Map>) JSONValue.parse(sourceOperatorsStr);

            JSONObject fromOpConnObj = new JSONObject();
            List simpleColumnsList = new ArrayList();
            for (Map fromOperatorMap : fromOperatorsList) {
                if (fromOperatorMap != null && !fromOperatorMap.isEmpty()) {
                    fromConnectObj = (JSONObject) fromOperatorMap.get("connObj");

                    List fromColumnsList = new ArrayList();
                    List fromAliasColumnsList = new ArrayList();
                    List toColumnsList = new ArrayList();

                    JSONObject connObj = (JSONObject) fromOperatorMap.get("connObj");

                    JSONObject trfmRules = (JSONObject) fromOperatorMap.get("trfmRules");

                    List<String> columnsList = (List) fromOperatorMap.get("simpleColumnsList");
                    List<String> dataTypeList = columnsList.stream().map(e -> "varchar2(4000)").collect(Collectors.toList());
                    simpleColumnsList.addAll(columnsList);

                    JSONArray fromTableColsArrayForClause = new JSONArray();
                    Object fromConnObj = componentUtilities.getConnection(fromConnectObj);

//                    String tableName = (String) "ZZ_TEMP_" + fromOperatorMap.get("timeStamp");
                    String tableName = (String) fromOperatorMap.get("iconType") + "_OUTPUT_"
                            + fromOperatorMap.get("operatorId");

                    sourceTablesArray.add(tableName);
                    fromOperatorMap.put("tableName", tableName);
//                    fromOpConnObj.put(tableName, fromConnectObj);
                    if (tableName != null && !"".equalsIgnoreCase(tableName)) {
                        JSONObject tableObj = new JSONObject();
                        tableObj.put("id", tableName);
                        tableObj.put("text", tableName);
                        tableObj.put("value", tableName);
                        tableObj.put("icon", "images/GridDB.svg");
                        fromTableColsArray.add(tableObj);
                        fromTableColsArrayForClause.add(tableObj);

                        for (int j = 0; j < columnsList.size(); j++) {
                            if (columnsList.get(j) != null && !"".equalsIgnoreCase(columnsList.get(j))) {
                                JSONObject columnObj = new JSONObject();
                                columnObj.put("id", columnsList.get(j));
                                columnObj.put("text", columnsList.get(j));
                                columnObj.put("value", columnsList.get(j));
                                columnObj.put("parentid", tableName);
                                if (dataTypeList != null && !dataTypeList.isEmpty()) {
                                    columnObj.put("dataType", dataTypeList.get(j) != null ? dataTypeList.get(j) : ""); // RAVI
                                    // DM

                                }

                                fromTableColsArray.add(columnObj);
                                fromTableColsArrayForClause.add(columnObj);
                            }

                        }
                    }

                }

            } // end loop

            // destination table obj start
            String targetOperatorId = request.getParameter("selectedOperatorId");
            JSONObject targetOperator = (JSONObject) operators.get(targetOperatorId);
            String toIconType = (String) targetOperator.get("iconType");

            if (targetOperator.get("statusLabel") == null) {
                targetOperator.put("statusLabel", targetOperator.get("tableName"));
            }
            String destinationTableName = (String) targetOperator.get("statusLabel");
            toConnectObj = (JSONObject) targetOperator.get("connObj");

            List<Object[]> toTableColumnList = new ArrayList<>();
            Object toConnObj = componentUtilities.getConnection(toConnectObj);
            if (toConnObj instanceof Connection) {
                toConnection = (Connection) toConnObj;
            } else if (toConnObj instanceof JCO.Client) {
                toJCOConnection = (JCO.Client) toConnObj;
            }
            if (toConnObj instanceof Connection) {
                toTableColumnList = componentUtilities.getTreeDMTableColumnsOpt(toConnection, request, toConnectObj,
                        destinationTableName);
            } else if (toConnObj instanceof JCO.Client) {
                toTableColumnList = componentUtilities.getSAPTableColumns(request, toJCOConnection,
                        (String) destinationTableName);
            }
            if (destinationTableName != null && !"".equalsIgnoreCase(destinationTableName)) {
                JSONObject tableObj = new JSONObject();
                tableObj.put("id",
                        ("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1")))
                        ? destinationTableName
                        : toConnectObj.get("CONN_USER_NAME") + "." + destinationTableName));
                tableObj.put("text", fromConnectObj.get("CONNECTION_NAME") + "." + destinationTableName);
                tableObj.put("value",
                        ("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1")))
                        ? destinationTableName
                        : toConnectObj.get("CONN_USER_NAME") + "." + destinationTableName));
                tableObj.put("icon", "images/GridDB.svg");
                toTableColsArray.add(tableObj);

                List<String> columnsList = toTableColumnList.stream().filter(
                        tableColsArray -> (destinationTableName.equalsIgnoreCase(String.valueOf(tableColsArray[0]))))
                        .map(tableColsArray -> String.valueOf(tableColsArray[1])).collect(Collectors.toList());

                // RAVI DM
                List<String> dataTypeList = new ArrayList();
                try {
                    dataTypeList = toTableColumnList.stream()
                            .filter(tableColsArray -> (destinationTableName
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
                        columnObj.put("id",
                                (("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1")))
                                ? destinationTableName
                                : toConnectObj.get("CONN_USER_NAME") + "." + destinationTableName) + ":"
                                + columnsList.get(j)));
                        columnObj.put("text", columnsList.get(j));
                        columnObj.put("value",
                                (("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1")))
                                ? destinationTableName
                                : toConnectObj.get("CONN_USER_NAME") + "." + destinationTableName) + ":"
                                + columnsList.get(j)));
                        columnObj.put("parentid",
                                ("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnectObj.get("CONN_CUST_COL1")))
                                ? destinationTableName
                                : toConnectObj.get("CONN_USER_NAME") + "." + destinationTableName));
                        if (dataTypeList != null && !dataTypeList.isEmpty()) {
                            columnObj.put("dataType", dataTypeList.get(j) != null ? dataTypeList.get(j) : ""); // RAVI
                            // DM

                        }

                        toTableColsArray.add(columnObj);

                    }

                }
            }

            // dest table obj end
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

//            columnMappingStr += "<tr style = 'height: 1px'>"
//                    + "<td width='1.5%' class=\"visionColMappingImgTd1\" ><img src=\"images/Delete_Red_Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionColMappingImg\""
//                    + " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>"
//                    + "</td>";
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
                    + "<td width='20%'><input class='visionColMappingTextarea' type='text' value='' />"
                    + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                    + " onclick=\"selectNumberGeneration(this)\" style=\"\"></td>"
                    + "<td width='20%' style='display:none;'><input class='visionColMappingTextarea' type='text' value=''></td>"
                    + "<td width='20%'><input id='visionETLFuncFromColId' class='visionColMappingInput' type='text' value=''>"
                    + "<img title='Select Function' src=\"images/Fx icon-01.svg\" class=\"visionETLColMapImage \" "
                    + " onclick=\"selectColumnFun(this,'fromColumn')\" style=\"\">" + "</td>" + "</tr>";

            JSONObject trfmRules = (JSONObject) targetOperator.get("trfmRules");
            if (trfmRules != null && !trfmRules.isEmpty()) {
                JSONArray columnMappingData = (JSONArray) trfmRules.get("colMappingsData");

                if (columnMappingData != null && !columnMappingData.isEmpty()) {

                    for (int i = 0; i < columnMappingData.size(); i++) {

                        JSONObject rowData = (JSONObject) columnMappingData.get(i);
                        String primaryKey = (String) rowData.get("primaryKey");
                        String pkChecked = (primaryKey != null && "Y".equalsIgnoreCase(primaryKey) ? " checked " : "");
                        String destinationColumn = (String) rowData.get("destinationColumn");
                        String destinationColumnActualValue = (String) rowData.get("destinationColumnActualValue");
                        String destTable = (rowData.get("destTable") != null) ? rowData.get("destTable").toString()
                                : "";

                        String sourceColumn = (String) rowData.get("sourceColumn");
                        String sourceColumnActualValue = (String) rowData.get("sourceColumnActualValue");

                        String sourceTable = (rowData.get("sourceTable") != null)
                                ? rowData.get("sourceTable").toString()
                                : "";

                        String defaultValue = (String) rowData.get("defaultValue");
                        String appendValue = (String) rowData.get("appendValue");
                        String columnClause = (String) rowData.get("columnClause");
                        String columnClauseActualValue = (String) rowData.get("columnClauseActualValue");
                        String dataFunTables = (rowData.get("dataFunTables") != null)
                                ? rowData.get("dataFunTables").toString()
                                : "";
                        String funcolumnslist = (rowData.get("funcolumnslist") != null)
                                ? rowData.get("funcolumnslist").toString()
                                : "";
                        String dataFunobjstr = (String) rowData.get("data-funobjstr");
                        String dataColumnClause = (String) rowData.get("data-columnClause");

                        columnMappingStr += "<tr style = 'height: 1px'>"
                                + "<td width='1.5%' class=\"visionColMappingImgTd1\" ><img src=\"images/Delete_Red_Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionColMappingImg\""
                                + " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>" + "</td>";

                        columnMappingStr += "<td width='2%' class=\"visionColMappingImgTd1\" ><input type=\"checkbox\" class=\"visionPKSelectCbx\" "
                                + pkChecked + " />" + "</td>"; // ravi pk
                        if (toIconType != null && !"".equalsIgnoreCase(toIconType)
                                && !"null".equalsIgnoreCase(toIconType) && !"SQL".equalsIgnoreCase(toIconType)) {
                            columnMappingStr += "<td width='19%' ><input class='visionColMappingInput' type='text' tableName='"
                                    + destTable + "' value='" + destinationColumn + "'  actual-value='"
                                    + destinationColumnActualValue + "' />"
                                    + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                                    + " onclick=\"selectColumn(this,'"
                                    + ((toTableColsArray != null && !toTableColsArray.isEmpty()) ? "toColumn"
                                    : "fromColumn")
                                    + "')\"" + " style=\"\"></td>";
                        } else {
                            columnMappingStr += "<td width='19%' ><input class='visionColMappingInput' type='text' tableName='"
                                    + destTable + "' value='" + destinationColumn + "' actual-value='"
                                    + destinationColumnActualValue + "' />"
                                    + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                                    + " onclick=\"selectColumn(this,'toColumn')\" style=\"\"></td>";
                        }

                        columnMappingStr += "<td width='20%'><input class='visionColMappingInput' type='text' tableName='"
                                + sourceTable + "' value='" + sourceColumn + "' actual-value='"
                                + sourceColumnActualValue + "'  readonly='true'/>"
                                + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                                + " onclick=\"selectColumn(this,'fromColumn')\" style=\"\"></td>"
                                + "<td width='20%'><input class='visionColMappingTextarea' type='text' value='"
                                + defaultValue + "' >"
                                + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                                + " onclick=\"selectNumberGeneration(this)\" style=\"\"></td>"
                                + "<td width='20%' style='display:none;'><input class='visionColMappingTextarea' type='text' value='"
                                + appendValue + "'></td>"
                                + "<td width='20%'><input id='visionETLFuncFromColId' class='visionColMappingInput' type='text' tableName='"
                                + dataFunTables + "'  value='" + columnClause + "' actual-value='"
                                + columnClauseActualValue + "' funcolumnslist='" + funcolumnslist + "'  >"
                                + "<img title='Select Function' src=\"images/Fx icon-01.svg\" class=\"visionETLColMapImage \" "
                                + " onclick=\"selectColumnFun(this,'fromColumn')\" style=\"\">" + "</td>" + "</tr>";

                    }
                }
            } else {
                columnMappingStr += colMapSingleTrString;
            }

            columnsObject.put("selectedColumnStr", colMapSingleTrString);
            columnsObject.put("colMappingStr", columnMappingStr);
            columnsObject.put("toTableColsArray", toTableColsArray);
            columnsObject.put("fromTableColsArray", fromTableColsArray);
            columnsObject.put("sourceTablesArray", sourceTablesArray);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return columnsObject;
    }

    public JSONObject getAPIColumns(HttpServletRequest request, HttpServletResponse response) {
        JSONObject resultObj = new JSONObject();
        try {

            //String apiEndpoint = "https://api.nytimes.com/svc/books/v3/lists.json?list=hardcover-fiction&api-key=PAG4AsGEeQSaCDN5pDBorQSFfzZCHebb";
            String operatorDataStr = request.getParameter("operatorData");

            JSONObject operatorData = (JSONObject) JSONValue.parse(operatorDataStr);
            JSONObject trfmRules = (JSONObject) operatorData.get("trfmRules");
            String tableStr = "";
            String apiUrl = "";
            String relativePath = "";
            String httpMethod = "";
            String acceptType = "";
            JSONObject parametersObj = new JSONObject();
            if (trfmRules != null) {
                apiUrl = (String) trfmRules.get("apiUrl");
                relativePath = (String) trfmRules.get("relativePath");
                httpMethod = (String) trfmRules.get("httpMethod");
                acceptType = (String) trfmRules.get("acceptType");
                parametersObj = (JSONObject) trfmRules.get("parametersObj");
            }
            String apiEndpoint = apiUrl + "/" + relativePath;
            if (parametersObj != null && !parametersObj.isEmpty()) {

                apiEndpoint += "?";
                int i = 0;
                for (Object key : parametersObj.keySet()) {
                    Object value = parametersObj.get(key);
                    apiEndpoint += key + "=" + value;
                    i++;
                    if (i != parametersObj.size()) {
                        apiEndpoint += "&";
                    }

                }
            }
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
            MultiValueMap<String, String> inputMap = new LinkedMultiValueMap();
//				             inputMap.add("list", "hardcover-fiction");
//				             inputMap.add("api-key", "PAG4AsGEeQSaCDN5pDBorQSFfzZCHebb");

            HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<>(inputMap,
                    headers);

            RestTemplate restTemplate = new RestTemplate();
            ResponseEntity<Object> apiResponse = restTemplate.exchange(apiEndpoint, HttpMethod.GET, entity, Object.class);

            Object responseBody = apiResponse.getBody();
			ArrayList<LinkedHashMap> responseList;

			if (responseBody instanceof LinkedHashMap) {
			    // If response is a LinkedHashMap, retrieve the "results" key
			    LinkedHashMap responseMap = (LinkedHashMap) responseBody;
			    responseList = (ArrayList<LinkedHashMap>) responseMap.get("results");
			} else if (responseBody instanceof ArrayList) {
			    // If response is already an ArrayList, cast it directly
			    responseList = (ArrayList<LinkedHashMap>) responseBody;
			} else {
			    // Handle unexpected response format
			    throw new IllegalStateException("Unexpected response format: " + responseBody.getClass().getName());
			}
            List<Object[]> dataList = new ArrayList<>();
            List columnsList = new ArrayList();
            int itr = 0;

            for (LinkedHashMap rowMap : responseList) {
                Object[] rowData = new Object[rowMap.size()];
                if (itr == 0) {
                    columnsList = new ArrayList(rowMap.keySet());
                }
                break;
//				            	 int i=0;
//				            	 for (Object key : rowMap.keySet()) {
//				            		 rowData[i] = rowMap.get(key);
//				            		i++;
//				            	 }
//				            	 dataList.add(rowData);
//				            	 itr++;
            }

            resultObj.put("simpleColumnsList", columnsList);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    public JSONObject columnMappingTrfmRulesForComponent(HttpServletRequest request, HttpServletResponse response) {

        JSONObject columnsObject = new JSONObject();
        JSONArray sourceTablesArray = new JSONArray();
        JSONArray fromTableColsArray = new JSONArray();
        JSONArray toTableColsArray = new JSONArray();
        Connection fromConnection = null;
        Connection toConnection = null;
        JCO.Client fromJCOConnection = null;
        JCO.Client toJCOConnection = null;

        JSONObject fromConnectObj = new JSONObject();
        JSONObject toConnectObj = new JSONObject();
        List allFromcolumns = new ArrayList();
        try {
            String flowchartDataStr = request.getParameter("flowchartData");
            JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
            JSONObject operators = (JSONObject) flowchartData.get("operators");

            String sourceOperatorsStr = request.getParameter("sourceOperators");
//            JSONObject sourceOperatorsObj = (JSONObject) JSONValue.parse(sourceOperatorsStr);
            List<Map> fromOperatorsList = (List<Map>) JSONValue.parse(sourceOperatorsStr);

            JSONObject fromOpConnObj = new JSONObject();
            List simpleColumnsList = new ArrayList();

            List simpleTargetColumnsList = new ArrayList();

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
                        List<String> headers = componentUtilities.getHeadersOfImportedFile(request,
                                (String) fromConnectObj.get("filePath"));

                        headers = componentUtilities.fileHeaderValidations(headers);
                        simpleColumnsList.addAll(headers);
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
                                    columnObj.put("id", fromConnectObj.get("fileName") + ":" + headerName);
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
                                    allFromcolumns.add(headerName);
                                }
                            }
                        }
                        fromOpConnObj.put(fromConnectObj.get("fileName"), fromConnectObj);

                    } else {
                        fromOpConnObj.put(fromOperatorMap.get("tableName"), fromConnectObj);
                        JSONArray fromTableColsArrayForClause = new JSONArray();
                        Object fromConnObj = componentUtilities.getConnection(fromConnectObj);
                        if (fromConnObj instanceof Connection || fromConnObj instanceof JCO.Client) {
                            if (fromConnObj instanceof Connection) {
                                fromConnection = (Connection) fromConnObj;
                            } else if (fromConnObj instanceof JCO.Client) {
                                fromJCOConnection = (JCO.Client) fromConnObj;
                            }
                            List<Object[]> fromTableColumnList = new ArrayList<>();
                            String tableName = (String) fromOperatorMap.get("statusLabel");
                            String tableNameLabel = (String) fromOperatorMap.get("tableNameLabel");
                            boolean tableExist = false;
                            if (fromConnObj instanceof Connection) {
                                tableExist = checkTableExsist(fromConnection,
                                        (String) fromOperatorMap.get("statusLabel"));
                                if (tableExist) {
                                    fromTableColumnList = componentUtilities.getTreeDMTableColumnsOpt(fromConnection,
                                            request, fromConnectObj, (String) fromOperatorMap.get("statusLabel"));
                                } else {
                                }
                            } else if (fromConnObj instanceof JCO.Client) {
                                tableExist = true;
                                fromTableColumnList = componentUtilities.getSAPTableColumns(request, fromJCOConnection,
                                        (String) fromOperatorMap.get("statusLabel"));
                            }

                            sourceTablesArray.add(tableName);
                            fromOperatorMap.put("tableName", tableName);

                            if (tableName != null && !"".equalsIgnoreCase(tableName)) {
                                String treeObjTableName = "";
                                if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))) {
                                    treeObjTableName = tableName;
                                } else if (!tableExist) {
                                    treeObjTableName = tableNameLabel;
                                } else {
                                    treeObjTableName = fromConnectObj.get("CONN_USER_NAME") + "." + tableName;
                                }
                                JSONObject tableObj = new JSONObject();
                                tableObj.put("id", treeObjTableName);
                                tableObj.put("text", treeObjTableName);
                                tableObj.put("value", treeObjTableName);
                                tableObj.put("icon", "images/GridDB.svg");
                                fromTableColsArray.add(tableObj);
                                fromTableColsArrayForClause.add(tableObj);
                                List<String> columnsList = new ArrayList();
                                if (tableExist) {
                                    columnsList = fromTableColumnList.stream()
                                            .filter(tableColsArray -> (tableName
                                            .equalsIgnoreCase(String.valueOf(tableColsArray[0]))))
                                            .map(tableColsArray -> String.valueOf(tableColsArray[1]))
                                            .collect(Collectors.toList());
                                } else {
                                    columnsList = (List) fromOperatorMap.get("simpleColumnsList");
                                }

                                List<String> fromOperatorsColumnsList = getSourceColumnsList(fromOperatorMap);
                                if (fromOperatorsColumnsList != null && !fromOperatorsColumnsList.isEmpty()) {
                                    simpleColumnsList.addAll(fromOperatorsColumnsList);
                                } else {
                                    simpleColumnsList.addAll(columnsList);
                                }

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
                                        String treeObjColumnId = "";
                                        String treeObjColumnText = "";
                                        String treeObjColumnValue = "";
                                        String treeObjColumnParentId = "";
                                        if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))) {
                                            treeObjColumnId = tableName + ":" + columnsList.get(j);
                                            treeObjColumnText = columnsList.get(j);
                                            treeObjColumnValue = tableName + ":" + columnsList.get(j);
                                            treeObjColumnParentId = tableName;
                                        } else if (!tableExist) {
                                            treeObjColumnId = tableName + ":" + columnsList.get(j);
                                            treeObjColumnText = columnsList.get(j);
                                            treeObjColumnValue = tableNameLabel + ":" + columnsList.get(j);
                                            treeObjColumnParentId = tableName;
                                        } else {
                                            treeObjColumnId = fromConnectObj.get("CONN_USER_NAME") + "." + tableName
                                                    + ":" + columnsList.get(j);
                                            treeObjColumnText = columnsList.get(j);
                                            treeObjColumnValue = fromConnectObj.get("CONN_USER_NAME") + "." + tableName
                                                    + ":" + columnsList.get(j);
                                            treeObjColumnParentId = fromConnectObj.get("CONN_USER_NAME") + "."
                                                    + tableName;
                                        }
                                        JSONObject columnObj = new JSONObject();
                                        columnObj.put("id", treeObjColumnId);
                                        columnObj.put("text", treeObjColumnText);
                                        columnObj.put("value", treeObjColumnValue);
                                        columnObj.put("parentid", treeObjColumnParentId);
                                        if (dataTypeList != null && !dataTypeList.isEmpty()) {
                                            columnObj.put("dataType",
                                                    dataTypeList.get(j) != null ? dataTypeList.get(j) : ""); // RAVI DM

                                        }

                                        fromTableColsArray.add(columnObj);
                                        fromTableColsArrayForClause.add(columnObj);
                                        allFromcolumns.add(treeObjColumnText);
                                    }
                                }
                            }

                        } else {
                            columnsObject.put("connectionFlag", "N");
                            columnsObject.put("connectionMessage", fromConnObj);
                        }

                    } // end else for file type
                }

            } // end loop

            // destination table obj start
            String targetOperatorId = request.getParameter("selectedOperatorId");
            JSONObject targetOperator = (JSONObject) operators.get(targetOperatorId);

            if (targetOperator.get("iconType") != null && "MERGE".equalsIgnoreCase(String.valueOf(targetOperator.get("iconType")))) {
                JSONArray targetOperatorIds = processJobComponentsService.getConnectedToOperatorIds(request,
                        String.valueOf(targetOperator.get("operatorId")), flowchartData);
                targetOperatorId = String.valueOf(targetOperatorIds.get(0));
                targetOperator = (JSONObject) operators.get(targetOperatorId);
            }
            String toIconType = (String) targetOperator.get("iconType");
            String component = (String) targetOperator.get("component");

            if (targetOperator.get("statusLabel") == null) {
                targetOperator.put("statusLabel", targetOperator.get("tableName"));
            }
//            String destinationTableName = (String) targetOperator.get("statusLabel");
            String destinationTableName = (String) targetOperator.get("statusLabel");
            String destinationTableNameLabel = (String) targetOperator.get("tableNameLabel");
            if (destinationTableNameLabel == null) {
                destinationTableNameLabel = destinationTableNameLabel;
            }
//            toConnectObj = (JSONObject) targetOperator.get("connObj");
            toConnectObj = (JSONObject) targetOperator.get("connObj");

            if (toConnectObj != null) {
                List<Object[]> toTableColumnList = new ArrayList<>();
                Object toConnObj = componentUtilities.getConnection(toConnectObj);
                if (toConnObj instanceof Connection) {
                    toConnection = (Connection) toConnObj;
                } else if (toConnObj instanceof JCO.Client) {
                    toJCOConnection = (JCO.Client) toConnObj;
                }
                boolean tableExist = false;
                if (toConnObj instanceof Connection) {
                    tableExist = checkTableExsist(toConnection, (String) destinationTableName);
                    if (tableExist) {
                        toTableColumnList = componentUtilities.getTreeDMTableColumnsOpt(toConnection, request,
                                toConnectObj, destinationTableName);
                    } else {

                    }
                } else if (toConnObj instanceof JCO.Client) {
                    tableExist = true;
                    toTableColumnList = componentUtilities.getSAPTableColumns(request, toJCOConnection,
                            (String) destinationTableName);
                }

                if (destinationTableName != null && !"".equalsIgnoreCase(destinationTableName)) {
                    JSONObject tableObj = new JSONObject();
                    tableObj.put("id", destinationTableName);
                    tableObj.put("text", destinationTableNameLabel);
                    tableObj.put("value", destinationTableNameLabel);
                    tableObj.put("icon", "images/GridDB.svg");
                    toTableColsArray.add(tableObj);

                    List<String> columnsList = allFromcolumns;

                    for (int j = 0; j < columnsList.size(); j++) {
                        if (columnsList.get(j) != null && !"".equalsIgnoreCase(columnsList.get(j))) {
                            JSONObject columnObj = new JSONObject();
                            columnObj.put("id", destinationTableName + ":" + columnsList.get(j));
                            columnObj.put("text", columnsList.get(j));
                            columnObj.put("value", destinationTableNameLabel + ":" + columnsList.get(j));
                            columnObj.put("parentid", destinationTableName);

                            toTableColsArray.add(columnObj);
                            simpleTargetColumnsList.add(columnsList.get(j));
                        }

                    }
                }
            } else {
                destinationTableName = targetOperator.get("iconType") + "_" + targetOperator.get("operatorId");
                destinationTableNameLabel = targetOperator.get("iconType") + "_" + targetOperator.get("operatorId");
                if (destinationTableName != null && !"".equalsIgnoreCase(destinationTableName)) {
                    JSONObject tableObj = new JSONObject();
                    tableObj.put("id", destinationTableName);
                    tableObj.put("text", destinationTableNameLabel);
                    tableObj.put("value", destinationTableNameLabel);
                    tableObj.put("icon", "images/GridDB.svg");
                    toTableColsArray.add(tableObj);

                    List<String> columnsList = allFromcolumns;

                    for (int j = 0; j < columnsList.size(); j++) {
                        if (columnsList.get(j) != null && !"".equalsIgnoreCase(columnsList.get(j))) {
                            JSONObject columnObj = new JSONObject();
                            columnObj.put("id", destinationTableName + ":" + columnsList.get(j));
                            columnObj.put("text", columnsList.get(j));
                            columnObj.put("value", destinationTableNameLabel + ":" + columnsList.get(j));
                            columnObj.put("parentid", destinationTableName);

                            toTableColsArray.add(columnObj);
                            simpleTargetColumnsList.add(columnsList.get(j));
                        }

                    }
                }
            }

            // dest table obj end
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

//            columnMappingStr += "<tr style = 'height: 1px'>"
//                    + "<td width='1.5%' class=\"visionColMappingImgTd1\" ><img src=\"images/Delete_Red_Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionColMappingImg\""
//                    + " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>"
//                    + "</td>";
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
                    + "<td width='20%'><input class='visionColMappingTextarea' type='text' value='' />"
                    + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                    + " onclick=\"selectNumberGeneration(this)\" style=\"\"></td>"
                    + "<td width='20%' style='display:none;'><input class='visionColMappingTextarea' type='text' value=''></td>"
                    + "<td width='20%'><input id='visionETLFuncFromColId' class='visionColMappingInput' type='text' value=''>"
                    + "<img title='Select Function' src=\"images/Fx icon-01.svg\" class=\"visionETLColMapImage \" "
                    + " onclick=\"selectColumnFun(this,'fromColumn')\" style=\"\">" + "</td>" + "</tr>";

            JSONObject trfmRules = (JSONObject) targetOperator.get("trfmRules");
            if (trfmRules != null && !trfmRules.isEmpty()) {
                JSONArray columnMappingData = (JSONArray) trfmRules.get("colMappingsData");

                if (columnMappingData != null && !columnMappingData.isEmpty()) {

                    for (int i = 0; i < columnMappingData.size(); i++) {

                        JSONObject rowData = (JSONObject) columnMappingData.get(i);
                        String primaryKey = (String) rowData.get("primaryKey");
                        String pkChecked = (primaryKey != null && "Y".equalsIgnoreCase(primaryKey) ? " checked " : "");
                        String destinationColumn = (String) rowData.get("destinationColumn");
                        String destinationColumnActualValue = (String) rowData.get("destinationColumnActualValue");
                        String destTable = (rowData.get("destTable") != null) ? rowData.get("destTable").toString()
                                : "";

                        String sourceColumn = (String) rowData.get("sourceColumn");
                        String sourceColumnActualValue = (String) rowData.get("sourceColumnActualValue");

                        String sourceTable = (rowData.get("sourceTable") != null)
                                ? rowData.get("sourceTable").toString()
                                : "";

                        String defaultValue = (String) rowData.get("defaultValue");
                        String appendValue = (String) rowData.get("appendValue");
                        String columnClause = (String) rowData.get("columnClause");
                        String columnClauseActualValue = (String) rowData.get("columnClauseActualValue");

                        String dataFunTables = (rowData.get("dataFunTables") != null)
                                ? rowData.get("dataFunTables").toString()
                                : "";
                        String funcolumnslist = (rowData.get("funcolumnslist") != null)
                                ? rowData.get("funcolumnslist").toString()
                                : "";

                        String dataFunobjstr = (String) rowData.get("data-funobjstr");
                        String dataColumnClause = (String) rowData.get("data-columnClause");

                        columnMappingStr += "<tr style = 'height: 1px'>"
                                + "<td width='1.5%' class=\"visionColMappingImgTd1\" ><img src=\"images/Delete_Red_Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionColMappingImg\""
                                + " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>" + "</td>";

                        columnMappingStr += "<td width='2%' class=\"visionColMappingImgTd1\" ><input type=\"checkbox\" class=\"visionPKSelectCbx\" "
                                + pkChecked + " />" + "</td>"; // ravi pk
                        if (toIconType != null && !"".equalsIgnoreCase(toIconType)
                                && !"null".equalsIgnoreCase(toIconType) && !"SQL".equalsIgnoreCase(toIconType)) {
                            columnMappingStr += "<td width='19%' ><input class='visionColMappingInput' type='text' tableName='"
                                    + destTable + "' value='" + destinationColumn + "' actual-value='"
                                    + destinationColumnActualValue + "' />"
                                    + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                                    + " onclick=\"selectColumn(this,'"
                                    + ((toTableColsArray != null && !toTableColsArray.isEmpty()) ? "toColumn"
                                    : "fromColumn")
                                    + "')\"" + " style=\"\"></td>";
                        } else {
                            columnMappingStr += "<td width='19%' ><input class='visionColMappingInput' type='text' tableName='"
                                    + destTable + "' value='" + destinationColumn + "' actual-value='"
                                    + destinationColumnActualValue + "' />"
                                    + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                                    + " onclick=\"selectColumn(this,'toColumn')\" style=\"\"></td>";
                        }

                        columnMappingStr += "<td width='20%'><input class='visionColMappingInput' type='text' tableName='"
                                + sourceTable + "' value='" + sourceColumn + "' actual-value='"
                                + sourceColumnActualValue + "' readonly='true'/>"
                                + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                                + " onclick=\"selectColumn(this,'fromColumn')\" style=\"\"></td>"
                                + "<td width='20%'><input class='visionColMappingTextarea' type='text' value='"
                                + defaultValue + "' >"
                                + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                                + " onclick=\"selectNumberGeneration(this)\" style=\"\"></td>"
                                + "<td width='20%' style='display:none;'><input class='visionColMappingTextarea' type='text' value='"
                                + appendValue + "'></td>"
                                + "<td width='20%'><input id='visionETLFuncFromColId' class='visionColMappingInput' type='text' tableName='"
                                + dataFunTables + "'  value='" + columnClause + "' actual-value='"
                                + columnClauseActualValue + "' funcolumnslist = '" + funcolumnslist + "'>"
                                + "<img title='Select Function' src=\"images/Fx icon-01.svg\" class=\"visionETLColMapImage \" "
                                + " onclick=\"selectColumnFun(this,'fromColumn')\" style=\"\">" + "</td>" + "</tr>";

                    }
                }
            } else {
                columnMappingStr += colMapSingleTrString;
            }

            columnsObject.put("selectedColumnStr", colMapSingleTrString);
            columnsObject.put("colMappingStr", columnMappingStr);
            columnsObject.put("toTableColsArray", toTableColsArray);
            columnsObject.put("fromTableColsArray", fromTableColsArray);
            columnsObject.put("sourceTablesArray", sourceTablesArray);
            columnsObject.put("fromOpConnObj", fromOpConnObj);
            columnsObject.put("simpleColumnsList", simpleColumnsList);
            columnsObject.put("simpleTargetColumnsList", simpleTargetColumnsList);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return columnsObject;
    }

    public JSONObject columnMappingTrfmRulesForMergeComponent(HttpServletRequest request, HttpServletResponse response) {

        JSONObject columnsObject = new JSONObject();
        JSONArray sourceTablesArray = new JSONArray();
        JSONArray fromTableColsArray = new JSONArray();
        JSONArray toTableColsArray = new JSONArray();
        Connection fromConnection = null;
        Connection toConnection = null;
        JCO.Client fromJCOConnection = null;
        JCO.Client toJCOConnection = null;

        JSONObject fromConnectObj = new JSONObject();
        JSONObject toConnectObj = new JSONObject();
        List allFromcolumns = new ArrayList();
        try {
            String flowchartDataStr = request.getParameter("flowchartData");
            JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
            JSONObject operators = (JSONObject) flowchartData.get("operators");

            String sourceOperatorsStr = request.getParameter("sourceOperators");
//            JSONObject sourceOperatorsObj = (JSONObject) JSONValue.parse(sourceOperatorsStr);
            List<Map> fromOperatorsList = (List<Map>) JSONValue.parse(sourceOperatorsStr);

            JSONObject fromOpConnObj = new JSONObject();
            List simpleColumnsList = new ArrayList();

            List simpleTargetColumnsList = new ArrayList();

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
                        List<String> headers = componentUtilities.getHeadersOfImportedFile(request,
                                (String) fromConnectObj.get("filePath"));

                        headers = componentUtilities.fileHeaderValidations(headers);
                        simpleColumnsList.addAll(headers);
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
                                    columnObj.put("id", fromConnectObj.get("fileName") + ":" + headerName);
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
                                    allFromcolumns.add(headerName);
                                }
                            }
                        }
                        fromOpConnObj.put(fromConnectObj.get("fileName"), fromConnectObj);

                    } else {
                        fromOpConnObj.put(fromOperatorMap.get("tableName"), fromConnectObj);
                        JSONArray fromTableColsArrayForClause = new JSONArray();
                        Object fromConnObj = componentUtilities.getConnection(fromConnectObj);
                        if (fromConnObj instanceof Connection || fromConnObj instanceof JCO.Client) {
                            if (fromConnObj instanceof Connection) {
                                fromConnection = (Connection) fromConnObj;
                            } else if (fromConnObj instanceof JCO.Client) {
                                fromJCOConnection = (JCO.Client) fromConnObj;
                            }

                            List<Object[]> fromTableColumnList = new ArrayList<>();
                            String tableName = (String) fromOperatorMap.get("statusLabel");
                            String tableNameLabel = (String) fromOperatorMap.get("tableNameLabel");
                            tableNameLabel = tableNameLabel != null ? tableNameLabel : tableName;
                            boolean tableExist = false;
                            if (fromConnObj instanceof Connection) {
                                tableExist = checkTableExsist(fromConnection,
                                        (String) fromOperatorMap.get("statusLabel"));
                                if (tableExist) {
                                    fromTableColumnList = componentUtilities.getTreeDMTableColumnsOpt(fromConnection,
                                            request, fromConnectObj, (String) fromOperatorMap.get("statusLabel"));
                                } else {
                                }
                            } else if (fromConnObj instanceof JCO.Client) {
                                tableExist = true;
                                fromTableColumnList = componentUtilities.getSAPTableColumns(request, fromJCOConnection,
                                        (String) fromOperatorMap.get("statusLabel"));
                            }

                            sourceTablesArray.add(tableName);
                            fromOperatorMap.put("tableName", tableName);

                            if (tableName != null && !"".equalsIgnoreCase(tableName)) {
                                String treeObjTableName = "";
                                if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))) {
                                    treeObjTableName = tableName;
                                } else if (!tableExist) {
                                    treeObjTableName = tableNameLabel;
                                } else {
                                    treeObjTableName = fromConnectObj.get("CONN_USER_NAME") + "." + tableName;
                                }
                                JSONObject tableObj = new JSONObject();
                                tableObj.put("id", treeObjTableName);
                                tableObj.put("text", treeObjTableName);
                                tableObj.put("value", treeObjTableName);
                                tableObj.put("icon", "images/GridDB.svg");
                                fromTableColsArray.add(tableObj);
                                fromTableColsArrayForClause.add(tableObj);
                                List<String> columnsList = new ArrayList();
                                if (tableExist) {
                                    columnsList = fromTableColumnList.stream()
                                            .filter(tableColsArray -> (tableName
                                            .equalsIgnoreCase(String.valueOf(tableColsArray[0]))))
                                            .map(tableColsArray -> String.valueOf(tableColsArray[1]))
                                            .collect(Collectors.toList());
                                } else {
                                    columnsList = (List) fromOperatorMap.get("simpleColumnsList");
                                }

                                List<String> fromOperatorsColumnsList = getSourceColumnsList(fromOperatorMap);
                                if (fromOperatorsColumnsList != null && !fromOperatorsColumnsList.isEmpty()) {
                                    simpleColumnsList.addAll(fromOperatorsColumnsList);
                                } else {
                                    simpleColumnsList.addAll(columnsList);
                                }

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
                                        String treeObjColumnId = "";
                                        String treeObjColumnText = "";
                                        String treeObjColumnValue = "";
                                        String treeObjColumnParentId = "";
                                        if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))) {
                                            treeObjColumnId = tableName + ":" + columnsList.get(j);
                                            treeObjColumnText = columnsList.get(j);
                                            treeObjColumnValue = tableName + ":" + columnsList.get(j);
                                            treeObjColumnParentId = tableName;
                                        } else if (!tableExist) {
                                            treeObjColumnId = tableName + ":" + columnsList.get(j);
                                            treeObjColumnText = columnsList.get(j);
                                            treeObjColumnValue = tableNameLabel + ":" + columnsList.get(j);
                                            treeObjColumnParentId = tableName;
                                        } else {
                                            treeObjColumnId = fromConnectObj.get("CONN_USER_NAME") + "." + tableName
                                                    + ":" + columnsList.get(j);
                                            treeObjColumnText = columnsList.get(j);
                                            treeObjColumnValue = fromConnectObj.get("CONN_USER_NAME") + "." + tableName
                                                    + ":" + columnsList.get(j);
                                            treeObjColumnParentId = fromConnectObj.get("CONN_USER_NAME") + "."
                                                    + tableName;
                                        }
                                        JSONObject columnObj = new JSONObject();
                                        columnObj.put("id", treeObjColumnId);
                                        columnObj.put("text", treeObjColumnText);
                                        columnObj.put("value", treeObjColumnValue);
                                        columnObj.put("parentid", treeObjColumnParentId);
                                        if (dataTypeList != null && !dataTypeList.isEmpty()) {
                                            columnObj.put("dataType",
                                                    dataTypeList.get(j) != null ? dataTypeList.get(j) : ""); // RAVI DM

                                        }

                                        fromTableColsArray.add(columnObj);
                                        fromTableColsArrayForClause.add(columnObj);
                                        allFromcolumns.add(treeObjColumnText);
                                    }
                                }
                            }

                        } else {
                            columnsObject.put("connectionFlag", "N");
                            columnsObject.put("connectionMessage", fromConnObj);
                        }

                    } // end else for file type
                }

            } // end loop

            // destination table obj start
            String targetOperatorId = request.getParameter("selectedOperatorId");
            JSONObject mergeOperator = (JSONObject) operators.get(targetOperatorId);

            JSONObject targetOperator = new JSONObject();

            if (mergeOperator.get("iconType") != null && "MERGE".equalsIgnoreCase(String.valueOf(mergeOperator.get("iconType")))) {
                JSONArray targetOperatorIds = processJobComponentsService.getConnectedToOperatorIds(request,
                        String.valueOf(mergeOperator.get("operatorId")), flowchartData);
                targetOperatorId = String.valueOf(targetOperatorIds.get(0));
                targetOperator = (JSONObject) operators.get(targetOperatorId);
            }
            String toIconType = (String) targetOperator.get("iconType");
            String component = (String) targetOperator.get("component");

            if (targetOperator.get("statusLabel") == null) {
                targetOperator.put("statusLabel", targetOperator.get("tableName"));
            }
//            String destinationTableName = (String) targetOperator.get("statusLabel");
            String destinationTableName = (String) targetOperator.get("statusLabel");
            String destinationTableNameLabel = (String) targetOperator.get("tableNameLabel");
            destinationTableNameLabel = destinationTableNameLabel != null ? destinationTableNameLabel : destinationTableName;
//            toConnectObj = (JSONObject) targetOperator.get("connObj");
            toConnectObj = (JSONObject) targetOperator.get("connObj");

            if (toConnectObj != null) {
                List<Object[]> toTableColumnList = new ArrayList<>();
                Object toConnObj = componentUtilities.getConnection(toConnectObj);
                if (toConnObj instanceof Connection) {
                    toConnection = (Connection) toConnObj;
                } else if (toConnObj instanceof JCO.Client) {
                    toJCOConnection = (JCO.Client) toConnObj;
                }
                boolean tableExist = false;
                if (toConnObj instanceof Connection) {
                    tableExist = checkTableExsist(toConnection, (String) destinationTableName);
                    if (tableExist) {
                        toTableColumnList = componentUtilities.getTreeDMTableColumnsOpt(toConnection, request,
                                toConnectObj, destinationTableName);
                    } else {

                    }
                } else if (toConnObj instanceof JCO.Client) {
                    tableExist = true;
                    toTableColumnList = componentUtilities.getSAPTableColumns(request, toJCOConnection,
                            (String) destinationTableName);
                }

                if (destinationTableName != null && !"".equalsIgnoreCase(destinationTableName)) {
                    JSONObject tableObj = new JSONObject();
                    tableObj.put("id", destinationTableName);
                    tableObj.put("text", destinationTableNameLabel);
                    tableObj.put("value", destinationTableNameLabel);
                    tableObj.put("icon", "images/GridDB.svg");
                    toTableColsArray.add(tableObj);

                    List<String> toColumnsList = (List) toTableColumnList.stream().map(rowdata -> String.valueOf(rowdata[1])).collect(Collectors.toList());

                    for (int j = 0; j < toColumnsList.size(); j++) {
                        if (toTableColumnList.get(j) != null && !"".equalsIgnoreCase(toColumnsList.get(j))) {
                            JSONObject columnObj = new JSONObject();
                            columnObj.put("id", destinationTableName + ":" + toColumnsList.get(j));
                            columnObj.put("text", toColumnsList.get(j));
                            columnObj.put("value", destinationTableNameLabel + ":" + toColumnsList.get(j));
                            columnObj.put("parentid", destinationTableName);

                            toTableColsArray.add(columnObj);
                            simpleTargetColumnsList.add(toColumnsList.get(j));
                        }
                    }
                }
            } else {
                destinationTableName = targetOperator.get("iconType") + "_" + targetOperator.get("operatorId");
                destinationTableNameLabel = targetOperator.get("iconType") + "_" + targetOperator.get("operatorId");
                destinationTableNameLabel = destinationTableNameLabel != null ? destinationTableNameLabel : destinationTableName;
                if (destinationTableName != null && !"".equalsIgnoreCase(destinationTableName)) {
                    JSONObject tableObj = new JSONObject();
                    tableObj.put("id", destinationTableName);
                    tableObj.put("text", destinationTableNameLabel);
                    tableObj.put("value", destinationTableNameLabel);
                    tableObj.put("icon", "images/GridDB.svg");
                    toTableColsArray.add(tableObj);

                    List<String> columnsList = allFromcolumns;

                    for (int j = 0; j < columnsList.size(); j++) {
                        if (columnsList.get(j) != null && !"".equalsIgnoreCase(columnsList.get(j))) {
                            JSONObject columnObj = new JSONObject();
                            columnObj.put("id", destinationTableName + ":" + columnsList.get(j));
                            columnObj.put("text", columnsList.get(j));
                            columnObj.put("value", destinationTableNameLabel + ":" + columnsList.get(j));
                            columnObj.put("parentid", destinationTableName);

                            toTableColsArray.add(columnObj);
                            simpleTargetColumnsList.add(columnsList.get(j));
                        }

                    }
                }
            }

            // dest table obj end
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
                    + "<input name=\"importColMapFile\" id=\"importColMapFile\" type=\"file\" style=\"display:none\">";

            // merge type start
            JSONArray primaryKeys = new JSONArray();
            JSONArray updateCols = new JSONArray();
            String operatorType = "Insert Or Update";
            String selected = "";
            JSONObject trfmRules = new JSONObject();
            if (mergeOperator != null && !mergeOperator.isEmpty()) {
                trfmRules = (JSONObject) mergeOperator.get("trfmRules");
                if (trfmRules != null && !trfmRules.isEmpty()) {
                    primaryKeys = (JSONArray) trfmRules.get("uniqueKeys");
                    updateCols = (JSONArray) trfmRules.get("updateColsList");
                    operatorType = (trfmRules.get("operatorType") != null) ? (String) trfmRules.get("operatorType")
                            : "Insert Or Update";
                }
            }

            columnMappingStr += "<div class='visionOperatorType'>"
                    + "<div class='visionMergeOperatorType'><span>Operator Type</span></div>"
                    + "<div class='visionSelectMergeOperatorType'>" + "<select id='operatorType'>" + "<option "
                    + ((operatorType.equalsIgnoreCase("Insert")) ? " selected " : "") + " >Insert</option>" + "<option "
                    + ((operatorType.equalsIgnoreCase("Update")) ? " selected " : "") + " >Update</option>"
                    // + "<option " + ((operatorType.equalsIgnoreCase("Delete")) ? " selected " :
                    // "") + " >Delete</option>"
                    + "<option " + ((operatorType.equalsIgnoreCase("Insert Or Update")) ? " selected " : "")
                    + " >Insert Or Update</option>"
                    // + "<option " + ((operatorType.equalsIgnoreCase("Delete or Insert")) ? "
                    // selected " : "") + " >Delete or Insert</option>"
                    + "</select>" + "</div>" + "</div>"
                    // merge type end

                    + "</div>" + "" + "<div id='visionColMappScrollDiv' class='visionColMappScrollDiv1'>"
                    + "<table id=\"sourceDestColsTableId\" class=\"visionEtlJoinClauseTable visionEtlSourceDestColsTable1\" style='width: 100%;' border='1'>"
                    + "<thead>" + "<tr>"
                    + "<th width='1.5%' class=\"visionColMappingImgTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'></th>";

            columnMappingStr += "<th width='2%' class=\"visionColMappingImgTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>"
                    + "PK <input  class='visionUniqueCompSelectAll' id='uniqueCompSelectAll' type='checkbox' /></th>"; // RAVI
            columnMappingStr += "<th width='2%' class=\"visionColMappingImgTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>"
                    + "Update <input  class='visionUpdateCompSelectAll' id='updateCompSelectAll' type='checkbox' /></th>"; // RAVI
            // PK
            columnMappingStr += "<th width='19%' class=\"mappedColsTh1\" "
                    + "style='background: #0071c5 none repeat scroll 0 0;"
                    + "color: #FFF;text-align: center;'> Destination Columns</th>";
            columnMappingStr += "<th width='20%' class=\"mappedColsTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Source Columns</th>"
                    + "<th width='20%' class=\"mappedColsTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Default Values</th>"
                    + "<th width='20%' class=\"mappedColsTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center;display:none;'>Append Values</th>"
                    + "<th width='20%' class=\"mappedColsTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Column Clause</th>"
                    + "</tr>" + "</thead>" + "<tbody>";

//            columnMappingStr += "<tr style = 'height: 1px'>"
//                    + "<td width='1.5%' class=\"visionColMappingImgTd1\" ><img src=\"images/Delete_Red_Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionColMappingImg\""
//                    + " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>"
//                    + "</td>";
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
            // UPDATE COLUMN SELECTION
            colMapSingleTrString += "<td width='2%' class=\"visionColMappingImgTd1\" ><input type=\"checkbox\" class=\"visionUpdateSelectCbx\" />"
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
                    + "<td width='20%'><input class='visionColMappingTextarea' type='text' value='' />"
                    + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                    + " onclick=\"selectNumberGeneration(this)\" style=\"\"></td>"
                    + "<td width='20%' style='display:none;'><input class='visionColMappingTextarea' type='text' value=''></td>"
                    + "<td width='20%'><input id='visionETLFuncFromColId' class='visionColMappingInput' type='text' value=''>"
                    + "<img title='Select Function' src=\"images/Fx icon-01.svg\" class=\"visionETLColMapImage \" "
                    + " onclick=\"selectColumnFun(this,'fromColumn')\" style=\"\">" + "</td>" + "</tr>";

            //JSONObject trfmRules = (JSONObject) targetOperator.get("trfmRules");
            if (trfmRules != null && !trfmRules.isEmpty()) {
                JSONArray columnMappingData = (JSONArray) trfmRules.get("colMappingsData");

                if (columnMappingData != null && !columnMappingData.isEmpty()) {

                    for (int i = 0; i < columnMappingData.size(); i++) {

                        JSONObject rowData = (JSONObject) columnMappingData.get(i);
                        String primaryKey = (String) rowData.get("primaryKey");
                        String pkChecked = (primaryKey != null && "Y".equalsIgnoreCase(primaryKey) ? " checked " : "");

                        String updateKey = (String) rowData.get("updateKey");
                        String updateKeyChecked = (updateKey != null && "Y".equalsIgnoreCase(updateKey) ? " checked " : "");

                        String destinationColumn = (String) rowData.get("destinationColumn");
                        String destinationColumnActualValue = (String) rowData.get("destinationColumnActualValue");
                        String destTable = (rowData.get("destTable") != null) ? rowData.get("destTable").toString()
                                : "";

                        String sourceColumn = (String) rowData.get("sourceColumn");
                        String sourceColumnActualValue = (String) rowData.get("sourceColumnActualValue");

                        String sourceTable = (rowData.get("sourceTable") != null)
                                ? rowData.get("sourceTable").toString()
                                : "";

                        String defaultValue = (String) rowData.get("defaultValue");
                        String appendValue = (String) rowData.get("appendValue");
                        String columnClause = (String) rowData.get("columnClause");
                        String columnClauseActualValue = (String) rowData.get("columnClauseActualValue");

                        String dataFunTables = (rowData.get("dataFunTables") != null)
                                ? rowData.get("dataFunTables").toString()
                                : "";
                        String funcolumnslist = (rowData.get("funcolumnslist") != null)
                                ? rowData.get("funcolumnslist").toString()
                                : "";

                        String dataFunobjstr = (String) rowData.get("data-funobjstr");
                        String dataColumnClause = (String) rowData.get("data-columnClause");

                        columnMappingStr += "<tr style = 'height: 1px'>"
                                + "<td width='1.5%' class=\"visionColMappingImgTd1\" ><img src=\"images/Delete_Red_Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionColMappingImg\""
                                + " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>" + "</td>";

                        columnMappingStr += "<td width='2%' class=\"visionColMappingImgTd1\" ><input type=\"checkbox\" class=\"visionPKSelectCbx visionUniqueKeyColSelectBox\" "
                                + pkChecked + " />" + "</td>"; // ravi pk
                        columnMappingStr += "<td width='2%' class=\"visionColMappingImgTd1\" ><input type=\"checkbox\" class=\"visionUpdateSelectCbx\" "
                                + updateKeyChecked + " />" + "</td>"; // ravi pk
                        if (toIconType != null && !"".equalsIgnoreCase(toIconType)
                                && !"null".equalsIgnoreCase(toIconType) && !"SQL".equalsIgnoreCase(toIconType)) {
                            columnMappingStr += "<td width='19%' ><input class='visionColMappingInput' type='text' tableName='"
                                    + destTable + "' value='" + destinationColumn + "' actual-value='"
                                    + destinationColumnActualValue + "' />"
                                    + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                                    + " onclick=\"selectColumn(this,'"
                                    + ((toTableColsArray != null && !toTableColsArray.isEmpty()) ? "toColumn"
                                    : "fromColumn")
                                    + "')\" style=\"\">"
                                    + "<img src=\"images/Delete_Red_Icon.svg\" class=\"visionETLIcons\" title=\"Delete Source Column\" style=\"width:15px;height:15px;cursor:pointer;\" id=\"deleteSourceColumnEmpty\" onclick=\"deleteSourceColumnEmpty(this)\">"
                                    + "</td>";
                        } else {
                            columnMappingStr += "<td width='19%' ><input class='visionColMappingInput' type='text' tableName='"
                                    + destTable + "' value='" + destinationColumn + "' actual-value='"
                                    + destinationColumnActualValue + "' />"
                                    + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                                    + " onclick=\"selectColumn(this,'toColumn')\" style=\"\">"
                                    + "<img src=\"images/Delete_Red_Icon.svg\" class=\"visionETLIcons\" title=\"Delete Source Column\" style=\"width:15px;height:15px;cursor:pointer;\" id=\"deleteSourceColumnEmpty\" onclick=\"deleteSourceColumnEmpty(this)\">"
                                    + "</td>";
                        }

                        columnMappingStr += "<td width='20%'><input class='visionColMappingInput' type='text' tableName='"
                                + sourceTable + "' value='" + sourceColumn + "' actual-value='"
                                + sourceColumnActualValue + "' readonly='true'/>"
                                + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                                + " onclick=\"selectColumn(this,'fromColumn')\" style=\"\">"
                                + "<img src=\"images/Delete_Red_Icon.svg\" class=\"visionETLIcons\" title=\"Delete Source Column\" style=\"width:15px;height:15px;cursor:pointer;\" id=\"deleteSourceColumnEmpty\" onclick=\"deleteSourceColumnEmpty(this)\">"
                                + "</td>"
                                + "<td width='20%'><input class='visionColMappingTextarea' type='text' value='"
                                + defaultValue + "' >"
                                + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                                + " onclick=\"selectNumberGeneration(this)\" style=\"\"></td>"
                                + "<td width='20%' style='display:none;'><input class='visionColMappingTextarea' type='text' value='"
                                + appendValue + "'></td>"
                                + "<td width='20%'><input id='visionETLFuncFromColId' class='visionColMappingInput' type='text' tableName='"
                                + dataFunTables + "'  value='" + columnClause + "' actual-value='"
                                + columnClauseActualValue + "' funcolumnslist = '" + funcolumnslist + "'>"
                                + "<img title='Select Function' src=\"images/Fx icon-01.svg\" class=\"visionETLColMapImage \" "
                                + " onclick=\"selectColumnFun(this,'fromColumn')\" style=\"\">" + "</td>" + "</tr>";

                    }
                }
            } else {
                columnMappingStr += colMapSingleTrString;
            }

            columnsObject.put("selectedColumnStr", colMapSingleTrString);
            columnsObject.put("colMappingStr", columnMappingStr);
            columnsObject.put("toTableColsArray", toTableColsArray);
            columnsObject.put("fromTableColsArray", fromTableColsArray);
            columnsObject.put("sourceTablesArray", sourceTablesArray);
            columnsObject.put("fromOpConnObj", fromOpConnObj);
            columnsObject.put("simpleColumnsList", simpleColumnsList);
            columnsObject.put("simpleTargetColumnsList", simpleTargetColumnsList);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return columnsObject;
    }

    public JSONObject joinComponentTrfmRules(HttpServletRequest request, HttpServletResponse response) {

        JSONObject resultObj = new JSONObject();
        JSONArray sourceTablesArray = new JSONArray();
        JSONArray fromTableColsArray = new JSONArray();
        Connection fromConnection = null;
        JCO.Client fromJCOConnection = null;
        JSONObject fromConnectObj = new JSONObject();
        String joinTableString = "";

        try {
            String flowchartDataStr = request.getParameter("flowchartData");
            JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
            JSONObject operators = (JSONObject) flowchartData.get("operators");
            String sourceOperatorsStr = request.getParameter("sourceOperators");
            List<Map> fromOperatorsList = (List<Map>) JSONValue.parse(sourceOperatorsStr);
            List<Map> updatedFromOperatorsList = new ArrayList();

            String selectedOperatorId = request.getParameter("selectedOperatorId");
            JSONObject operator = (JSONObject) operators.get(selectedOperatorId);
            JSONObject trfmRulesData = (JSONObject) operator.get("trfmRules");

            JSONObject colMappingObj = columnMappingTrfmRulesForComponent(request,
                    response);
            JSONObject fromOpConnObj = (JSONObject) colMappingObj.get("fromOpConnObj");
            resultObj.putAll(colMappingObj);

            int index = 0;

            joinTableString = joinTransformationRules(request, fromOpConnObj, trfmRulesData, fromOperatorsList);

            resultObj.put("joinTableString", joinTableString);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    public String joinTransformationRules(HttpServletRequest request, JSONObject dbFromObj, JSONObject trfmRulesData,
            List<Map> fromOperatorsList) {
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
                                tableName = tableName.replaceAll("[^a-zA-Z0-9]", "_");
                            }
                        }
                    } catch (Exception e) {

                    }
                    return tableName;
                }).collect(Collectors.toList());

                List<String> fromTablesLebelList = fromOperatorsList.stream().map(fromOp -> {

                    String tableNameLebel = "";
                    try {
                        if (fromOp != null && !fromOp.isEmpty()) {
                            if (fromOp.get("tableName") != null
                                    && !"".equalsIgnoreCase(String.valueOf(fromOp.get("tableName")))
                                    && !"null".equalsIgnoreCase(String.valueOf(fromOp.get("tableName")))) {

                                tableNameLebel = fromOp.get("tableNameLabel") != null
                                        ? String.valueOf(fromOp.get("tableNameLabel"))
                                        : String.valueOf(fromOp.get("tableName"));

                            }
                            if ("file".equalsIgnoreCase(String.valueOf(fromOp.get("dragType")))) {
                                JSONObject fileObj = (JSONObject) fromOp.get("connObj");
                                tableNameLebel = String.valueOf(fileObj.get("fileName"));
                                tableNameLebel = tableNameLebel.replaceAll("[^a-zA-Z0-9]", "_");
                            }
                        }
                    } catch (Exception e) {

                    }
                    return tableNameLebel;
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
                        String tableNameLebel = (String) fromOperator.get("tableNameLebel");
                        JSONObject connObj = (JSONObject) fromOperator.get("connObj");
                        if (!(tableName != null && !"".equalsIgnoreCase(tableName)
                                && !"null".equalsIgnoreCase(tableName))) {
                            tableName = String.valueOf(connObj.get("fileName"));
                            tableName = tableName.replaceAll("[^a-zA-Z0-9]", "_");
                        }
                        joinTableString += "<tr><td class=\"sourceJoinColsTd\">"
                                + "<select id=\"SOURCE_SELECT_JOIN_TABLES_" + i
                                + "\" onchange=changeSelectedTableDb(id," + i + ")  class=\"sourceColsJoinSelectBox\""
                                + " data-table-db='" + (connObj.toJSONString()) + "'>" + ""
                                + generateTableSelectBoxStr(fromTablesList, fromTablesLebelList, tableName,
                                        tableNameLebel, "SOURCE_SELECT_JOIN_TABLES_" + i + "")
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

    public String generateTableSelectBoxStr(List<String> columnList, List<String> labelsList, String selectedTable,
            String selectedTableLabel, String selectBoxId) {
        String selectBoxStr = "<option>Select</option>";
        try {
            for (int i = 0; i < labelsList.size(); i++) {
                String table = columnList.get(i);
                String tableLabel = labelsList.get(i);
                String selectedStr = "";
                if (selectedTable != null && !"".equalsIgnoreCase(selectedTable)
                        && selectedTable.equalsIgnoreCase(String.valueOf(table))) {
                    selectedStr = "selected";
                }
                selectBoxStr += "<option  value='" + table + "'" + " id ='" + selectBoxId + "_" + table
                        + "' data-tablename='" + table + "' data-tableLabel='" + tableLabel + "' " + "" + selectedStr
                        + ">" + tableLabel + "</option>";
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return selectBoxStr;
    }

    public JSONObject filterComponentTrfmRules(HttpServletRequest request, HttpServletResponse response) {

        JSONObject resultObj = new JSONObject();
        JSONArray sourceTablesArray = new JSONArray();
        JSONArray fromTableColsArray = new JSONArray();
        Connection fromConnection = null;
        JCO.Client fromJCOConnection = null;
        JSONObject fromConnectObj = new JSONObject();

        try {
            String flowchartDataStr = request.getParameter("flowchartData");
            JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
            JSONObject operators = (JSONObject) flowchartData.get("operators");
            String sourceOperatorsStr = request.getParameter("sourceOperators");
            List<Map> fromOperatorsList = (List<Map>) JSONValue.parse(sourceOperatorsStr);

            String selectedOperatorId = request.getParameter("selectedOperatorId");
            JSONObject operator = (JSONObject) operators.get(selectedOperatorId);
            JSONObject trfmRulesData = (JSONObject) operator.get("trfmRules");
            List simpleColumnsList = new ArrayList();
//                JSONObject whereClauseConditionsObj = (JSONObject) trfmRulesData.get("whereClauseConditions");
            if (fromOperatorsList != null && !fromOperatorsList.isEmpty()) {
                JSONObject fromOpConnObj = new JSONObject();
                JSONObject fromTableWhereColsObj = new JSONObject();

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
                            List<String> headers = componentUtilities.getHeadersOfImportedFile(request,
                                    (String) fromConnectObj.get("filePath"));
//                            // ravi headers trim start
                            headers = componentUtilities.fileHeaderValidations(headers);
                            simpleColumnsList.addAll(headers);
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
                                        columnObj.put("id", fromConnectObj.get("fileName") + ":" + headerName);
                                        columnObj.put("text", headerName);
                                        columnObj.put("value", fromConnectObj.get("fileName") + ":" + headerName);
                                        columnObj.put("parentid", fromConnectObj.get("fileName"));
                                        fromTableColsArray.add(columnObj);
                                        fromTableColsArrayForClause.add(columnObj);
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
                                    trfmRulesData, fromOperatorMap, (String) fromConnectObj.get("fileName"), true,
                                    index);
                        } else {
                            fromOpConnObj.put(fromOperatorMap.get("tableName"), fromConnectObj);
                            JSONArray fromTableColsArrayForClause = new JSONArray();
                            Object fromConnObj = componentUtilities.getConnection(fromConnectObj);
                            if (fromConnObj instanceof Connection || fromConnObj instanceof JCO.Client) {
                                if (fromConnObj instanceof Connection) {
                                    fromConnection = (Connection) fromConnObj;
                                } else if (fromConnObj instanceof JCO.Client) {
                                    fromJCOConnection = (JCO.Client) fromConnObj;
                                }
                                List<Object[]> fromTableColumnList = new ArrayList<>();
                                String tableName = (String) fromOperatorMap.get("statusLabel");
                                String tableNameLabel = (String) fromOperatorMap.get("tableNameLabel");
                                boolean tableExist = false;
                                if (fromConnObj instanceof Connection) {
                                    tableExist = checkTableExsist(fromConnection,
                                            (String) fromOperatorMap.get("statusLabel"));
                                    if (tableExist) {
                                        fromTableColumnList = componentUtilities.getTreeDMTableColumnsOpt(
                                                fromConnection, request, fromConnectObj,
                                                (String) fromOperatorMap.get("statusLabel"));
                                    } else {
                                    }
                                } else if (fromConnObj instanceof JCO.Client) {
                                    tableExist = true;
                                    fromTableColumnList = componentUtilities.getSAPTableColumns(request,
                                            fromJCOConnection, (String) fromOperatorMap.get("statusLabel"));
                                }

                                sourceTablesArray.add(tableName);
                                fromOperatorMap.put("tableName", tableName);

                                if (tableName != null && !"".equalsIgnoreCase(tableName)) {
                                    String treeObjTableName = "";
                                    if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))) {
                                        treeObjTableName = tableName;
                                    } else if (!tableExist) {
                                        treeObjTableName = tableNameLabel;
                                    } else {
                                        treeObjTableName = fromConnectObj.get("CONN_USER_NAME") + "." + tableName;
                                    }
                                    JSONObject tableObj = new JSONObject();
                                    tableObj.put("id", tableName);
                                    tableObj.put("text", treeObjTableName);
                                    tableObj.put("value", treeObjTableName);
                                    tableObj.put("icon", "images/GridDB.svg");
                                    fromTableColsArray.add(tableObj);
                                    fromTableColsArrayForClause.add(tableObj);
                                    List<String> columnsList = new ArrayList();
                                    if (tableExist) {
                                        columnsList = fromTableColumnList.stream()
                                                .filter(tableColsArray -> (tableName
                                                .equalsIgnoreCase(String.valueOf(tableColsArray[0]))))
                                                .map(tableColsArray -> String.valueOf(tableColsArray[1]))
                                                .collect(Collectors.toList());
                                    } else {
                                        columnsList = (List) fromOperatorMap.get("simpleColumnsList");
                                    }

                                    List<String> fromOperatorsColumnsList = getSourceColumnsList(fromOperatorMap);
                                    if (fromOperatorsColumnsList != null && !fromOperatorsColumnsList.isEmpty()) {
                                        simpleColumnsList.addAll(fromOperatorsColumnsList);
                                    } else {
                                        simpleColumnsList.addAll(columnsList);
                                    }

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
                                            String treeObjColumnId = "";
                                            String treeObjColumnText = "";
                                            String treeObjColumnValue = "";
                                            String treeObjColumnParentId = "";
                                            if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))) {
                                                treeObjColumnId = tableName + ":" + columnsList.get(j);
                                                treeObjColumnText = columnsList.get(j);
                                                treeObjColumnValue = tableName + ":" + columnsList.get(j);
                                                treeObjColumnParentId = tableName;
                                            } else if (!tableExist) {
                                                treeObjColumnId = tableName + ":" + columnsList.get(j);
                                                treeObjColumnText = columnsList.get(j);
                                                treeObjColumnValue = tableNameLabel + ":" + columnsList.get(j);
                                                treeObjColumnParentId = tableName;
                                            } else {
                                                treeObjColumnId = fromConnectObj.get("CONN_USER_NAME") + "." + tableName
                                                        + ":" + columnsList.get(j);
                                                treeObjColumnText = columnsList.get(j);
                                                treeObjColumnValue = fromConnectObj.get("CONN_USER_NAME") + "."
                                                        + tableName + ":" + columnsList.get(j);
                                                //treeObjColumnParentId = fromConnectObj.get("CONN_USER_NAME") + "." + tableName;
                                                treeObjColumnParentId = tableName;
                                            }
                                            JSONObject columnObj = new JSONObject();
                                            columnObj.put("id", treeObjColumnId);
                                            columnObj.put("text", treeObjColumnText);
                                            columnObj.put("value", treeObjColumnValue);
                                            columnObj.put("parentid", treeObjColumnParentId);
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
                                fromTableWhereColsObj.put((String) tableName, fromTableColsArrayForClause);
                                whereClauseCondition += fetchTableWhereClauseTrfnRules(request, fromTableColumnList,
                                        trfmRulesData, fromOperatorMap, tableName, tableExist, index);

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

                resultObj.put("whereClauseCondition", whereClauseCondition);
                resultObj.put("fromTableColsArray", fromTableWhereColsObj);
                resultObj.put("sourceTablesArray", sourceTablesArray);
                resultObj.put("simpleColumnsList", simpleColumnsList);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    public String fetchTableWhereClauseTrfnRules(HttpServletRequest request, List<Object[]> fromTableColumnList,
            JSONObject trfmRulesData, Map fromOperatorMap, String tableName, boolean tableExist, int index) {
        String whereClauseCondition = "";

        try {

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
                                	if("LIKE".equalsIgnoreCase(String.valueOf(whereClauseColObj.get("operator")))) {
                                		whereClauseStr += " "
                                                + String.valueOf(whereClauseColObj.get("columnName")).replace(":", ".")
                                                + " " + "" + whereClauseColObj.get("operator") + " " + " '%"
                                                + whereClauseColObj.get("staticValue") + "%' ";// operator
                                	} else {
                                		whereClauseStr += " "
                                                + String.valueOf(whereClauseColObj.get("columnName")).replace(":", ".")
                                                + " " + "" + whereClauseColObj.get("operator") + " " + " '"
                                                + whereClauseColObj.get("staticValue") + "' ";// operator
                                	}
                                    
                                    if (j != whereClauseObjList.size() - 1) {
                                        whereClauseStr += " " + whereClauseColObj.get("andOrOperator") + " "; // andOrOperator
                                    }
                                    j++;
                                }

                            }
                        }
                    }
                }
                whereClauseCondition += "<tr>" + "<td width='20%'>"
                        + (tableExist ? tableName : fromOperatorMap.get("tableNameLabel")) + "</td>"
                        + "<td width='75%'>" + "<textarea readonly='true' id=\"whereClauseConditionsArea_" + index
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

    public JSONObject rowsRangeComponentTrfmRules(HttpServletRequest request, HttpServletResponse response) {

        JSONObject resultObj = new JSONObject();
        JSONArray sourceTablesArray = new JSONArray();
        JSONArray fromTableColsArray = new JSONArray();
        Connection fromConnection = null;
        JCO.Client fromJCOConnection = null;
        JSONObject fromConnectObj = new JSONObject();
        try {
            List simpleColumnsList = new ArrayList();
            String rowsRangeSelectionStr = "";

            JSONObject rowsRangeObj = new JSONObject();
            String flowchartDataStr = request.getParameter("flowchartData");
            JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
            JSONObject operators = (JSONObject) flowchartData.get("operators");
            String sourceOperatorsStr = request.getParameter("sourceOperators");
            List<Map> fromOperatorsList = (List<Map>) JSONValue.parse(sourceOperatorsStr);

            if (fromOperatorsList != null && !fromOperatorsList.isEmpty()) {
                JSONObject fromOpConnObj = new JSONObject();
                JSONObject fromTableJoinColsObj = new JSONObject();

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
                            List<String> headers = componentUtilities.getHeadersOfImportedFile(request,
                                    (String) fromConnectObj.get("filePath"));
//                            // ravi headers trim start
                            headers = componentUtilities.fileHeaderValidations(headers);
                            simpleColumnsList.add(headers);
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
                                        columnObj.put("id", fromConnectObj.get("fileName") + ":" + headerName);
                                        columnObj.put("text", headerName);
                                        columnObj.put("value", fromConnectObj.get("fileName") + ":" + headerName);
                                        columnObj.put("parentid", fromConnectObj.get("fileName"));
                                        fromTableColsArray.add(columnObj);
                                        fromTableColsArrayForClause.add(columnObj);
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
                            fromTableJoinColsObj.put((String) fromConnectObj.get("fileName"),
                                    fromTableColsArrayForClause);

                        } else {
                            fromOpConnObj.put(fromOperatorMap.get("tableName"), fromConnectObj);
                            JSONArray fromTableColsArrayForClause = new JSONArray();
                            Object fromConnObj = componentUtilities.getConnection(fromConnectObj);
                            if (fromConnObj instanceof Connection || fromConnObj instanceof JCO.Client) {
                                if (fromConnObj instanceof Connection) {
                                    fromConnection = (Connection) fromConnObj;
                                } else if (fromConnObj instanceof JCO.Client) {
                                    fromJCOConnection = (JCO.Client) fromConnObj;
                                }
                                List<Object[]> fromTableColumnList = new ArrayList<>();
                                String tableName = (String) fromOperatorMap.get("statusLabel");
                                String tableNameLabel = (String) fromOperatorMap.get("tableNameLabel");
                                boolean tableExist = false;
                                if (fromConnObj instanceof Connection) {
                                    tableExist = checkTableExsist(fromConnection,
                                            (String) fromOperatorMap.get("statusLabel"));
                                    if (tableExist) {
                                        fromTableColumnList = componentUtilities.getTreeDMTableColumnsOpt(
                                                fromConnection, request, fromConnectObj,
                                                (String) fromOperatorMap.get("statusLabel"));
                                    } else {
                                    }
                                } else if (fromConnObj instanceof JCO.Client) {
                                    tableExist = true;
                                    fromTableColumnList = componentUtilities.getSAPTableColumns(request,
                                            fromJCOConnection, (String) fromOperatorMap.get("statusLabel"));
                                }

                                sourceTablesArray.add(tableName);
                                fromOperatorMap.put("tableName", tableName);

                                if (tableName != null && !"".equalsIgnoreCase(tableName)) {
                                    String treeObjTableName = "";
                                    if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))) {
                                        treeObjTableName = tableName;
                                    } else if (!tableExist) {
                                        treeObjTableName = tableNameLabel;
                                    } else {
                                        treeObjTableName = fromConnectObj.get("CONN_USER_NAME") + "." + tableName;
                                    }
                                    JSONObject tableObj = new JSONObject();
                                    tableObj.put("id", tableName);
                                    tableObj.put("text", treeObjTableName);
                                    tableObj.put("value", treeObjTableName);
                                    tableObj.put("icon", "images/GridDB.svg");
                                    fromTableColsArray.add(tableObj);
                                    fromTableColsArrayForClause.add(tableObj);
                                    List<String> columnsList = new ArrayList();
                                    if (tableExist) {
                                        columnsList = fromTableColumnList.stream()
                                                .filter(tableColsArray -> (tableName
                                                .equalsIgnoreCase(String.valueOf(tableColsArray[0]))))
                                                .map(tableColsArray -> String.valueOf(tableColsArray[1]))
                                                .collect(Collectors.toList());
                                    } else {
                                        columnsList = (List) fromOperatorMap.get("simpleColumnsList");
                                    }

                                    List<String> fromOperatorsColumnsList = getSourceColumnsList(fromOperatorMap);
                                    if (fromOperatorsColumnsList != null && !fromOperatorsColumnsList.isEmpty()) {
                                        simpleColumnsList.addAll(fromOperatorsColumnsList);
                                    } else {
                                        simpleColumnsList.addAll(columnsList);
                                    }

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
                                            String treeObjColumnId = "";
                                            String treeObjColumnText = "";
                                            String treeObjColumnValue = "";
                                            String treeObjColumnParentId = "";
                                            if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))) {
                                                treeObjColumnId = tableName + ":" + columnsList.get(j);
                                                treeObjColumnText = columnsList.get(j);
                                                treeObjColumnValue = tableName + ":" + columnsList.get(j);
                                                treeObjColumnParentId = tableName;
                                            } else if (!tableExist) {
                                                treeObjColumnId = tableName + ":" + columnsList.get(j);
                                                treeObjColumnText = columnsList.get(j);
                                                treeObjColumnValue = tableNameLabel + ":" + columnsList.get(j);
                                                treeObjColumnParentId = tableName;
                                            } else {
                                                treeObjColumnId = fromConnectObj.get("CONN_USER_NAME") + "." + tableName
                                                        + ":" + columnsList.get(j);
                                                treeObjColumnText = columnsList.get(j);
                                                treeObjColumnValue = fromConnectObj.get("CONN_USER_NAME") + "."
                                                        + tableName + ":" + columnsList.get(j);
                                                treeObjColumnParentId = fromConnectObj.get("CONN_USER_NAME") + "."
                                                        + tableName;
                                            }
                                            JSONObject columnObj = new JSONObject();
                                            columnObj.put("id", treeObjColumnId);
                                            columnObj.put("text", treeObjColumnText);
                                            columnObj.put("value", treeObjColumnValue);
                                            columnObj.put("parentid", treeObjColumnParentId);
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
                                fromTableJoinColsObj.put((String) tableName, fromTableColsArrayForClause);

                            } else {
                                resultObj.put("connectionFlag", "N");
                                resultObj.put("connectionMessage", fromConnObj);
                            }

                        } // end else for file type
                    }
                    index++;
                } // end loop

                resultObj.put("fromTableColsArray", fromTableJoinColsObj);

            }

            String selectedOperatorId = request.getParameter("selectedOperatorId");
            JSONObject operator = (JSONObject) operators.get(selectedOperatorId);
            JSONObject trfmRulesData = (JSONObject) operator.get("trfmRules");

            String startIndex = "";
            String limit = "";
            if (trfmRulesData != null && !trfmRulesData.isEmpty() && trfmRulesData.size() != 0) {
                rowsRangeObj = (JSONObject) trfmRulesData.get("rowsRangeObj");
                startIndex = rowsRangeObj.get("startIndex") != null ? String.valueOf(rowsRangeObj.get("startIndex")) : "";
                limit = rowsRangeObj.get("limit") != null ? String.valueOf(rowsRangeObj.get("limit")) : "";
            }
            // ravi start
            String rowsRangeTable = "<div class='rowsRangeSelectionDiv'>"
                    + "<table class='rowsRangeSelectionTable'>"
                    + "<tr><td width='70%' >Start Index</td><td width='30%'><input id='rowsRangeStartIndex' value='" + startIndex + "'></td></tr>"
                    + "<tr><td width='70%'>Limit</td><td width='30%'><input id='rowsRangeLimit' value='" + limit + "'></td></tr>"
                    + "</table>"
                    + "<div>";

            resultObj.put("rowsRangeTable", rowsRangeTable);
            resultObj.put("simpleColumnsList", simpleColumnsList);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    public JSONObject sortComponentTrfmRules(HttpServletRequest request, HttpServletResponse response) {

        JSONObject resultObj = new JSONObject();
        JSONArray sourceTablesArray = new JSONArray();
        JSONArray fromTableColsArray = new JSONArray();
        Connection fromConnection = null;
        JCO.Client fromJCOConnection = null;
        JSONObject fromConnectObj = new JSONObject();
        try {
            List simpleColumnsList = new ArrayList();
            String orderByColsCondition = "";
            List<Object[]> orderByDataList = new ArrayList<>();
            JSONArray orderByData = new JSONArray();
            String flowchartDataStr = request.getParameter("flowchartData");
            JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
            JSONObject operators = (JSONObject) flowchartData.get("operators");
            String sourceOperatorsStr = request.getParameter("sourceOperators");
            List<Map> fromOperatorsList = (List<Map>) JSONValue.parse(sourceOperatorsStr);

            if (fromOperatorsList != null && !fromOperatorsList.isEmpty()) {
                JSONObject fromOpConnObj = new JSONObject();
                JSONObject fromTableJoinColsObj = new JSONObject();

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
                            List<String> headers = componentUtilities.getHeadersOfImportedFile(request,
                                    (String) fromConnectObj.get("filePath"));
//                            // ravi headers trim start
                            headers = componentUtilities.fileHeaderValidations(headers);
                            simpleColumnsList.add(headers);
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
                                        columnObj.put("id", fromConnectObj.get("fileName") + ":" + headerName);
                                        columnObj.put("text", headerName);
                                        columnObj.put("value", fromConnectObj.get("fileName") + ":" + headerName);
                                        columnObj.put("parentid", fromConnectObj.get("fileName"));
                                        fromTableColsArray.add(columnObj);
                                        fromTableColsArrayForClause.add(columnObj);
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
                            fromTableJoinColsObj.put((String) fromConnectObj.get("fileName"),
                                    fromTableColsArrayForClause);

                        } else {
                            fromOpConnObj.put(fromOperatorMap.get("tableName"), fromConnectObj);
                            JSONArray fromTableColsArrayForClause = new JSONArray();
                            Object fromConnObj = componentUtilities.getConnection(fromConnectObj);
                            if (fromConnObj instanceof Connection || fromConnObj instanceof JCO.Client) {
                                if (fromConnObj instanceof Connection) {
                                    fromConnection = (Connection) fromConnObj;
                                } else if (fromConnObj instanceof JCO.Client) {
                                    fromJCOConnection = (JCO.Client) fromConnObj;
                                }
                                List<Object[]> fromTableColumnList = new ArrayList<>();
                                String tableName = (String) fromOperatorMap.get("statusLabel");
                                String tableNameLabel = (String) fromOperatorMap.get("tableNameLabel");
                                boolean tableExist = false;
                                if (fromConnObj instanceof Connection) {
                                    tableExist = checkTableExsist(fromConnection,
                                            (String) fromOperatorMap.get("statusLabel"));
                                    if (tableExist) {
                                        fromTableColumnList = componentUtilities.getTreeDMTableColumnsOpt(
                                                fromConnection, request, fromConnectObj,
                                                (String) fromOperatorMap.get("statusLabel"));
                                    } else {
                                    }
                                } else if (fromConnObj instanceof JCO.Client) {
                                    tableExist = true;
                                    fromTableColumnList = componentUtilities.getSAPTableColumns(request,
                                            fromJCOConnection, (String) fromOperatorMap.get("statusLabel"));
                                }

                                sourceTablesArray.add(tableName);
                                fromOperatorMap.put("tableName", tableName);

                                if (tableName != null && !"".equalsIgnoreCase(tableName)) {
                                    String treeObjTableName = "";
                                    if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))) {
                                        treeObjTableName = tableName;
                                    } else if (!tableExist) {
                                        treeObjTableName = tableNameLabel;
                                    } else {
                                        treeObjTableName = fromConnectObj.get("CONN_USER_NAME") + "." + tableName;
                                    }
                                    JSONObject tableObj = new JSONObject();
                                    tableObj.put("id", tableName);
                                    tableObj.put("text", treeObjTableName);
                                    tableObj.put("value", treeObjTableName);
                                    tableObj.put("icon", "images/GridDB.svg");
                                    fromTableColsArray.add(tableObj);
                                    fromTableColsArrayForClause.add(tableObj);
                                    List<String> columnsList = new ArrayList();
                                    if (tableExist) {
                                        columnsList = fromTableColumnList.stream()
                                                .filter(tableColsArray -> (tableName
                                                .equalsIgnoreCase(String.valueOf(tableColsArray[0]))))
                                                .map(tableColsArray -> String.valueOf(tableColsArray[1]))
                                                .collect(Collectors.toList());
                                    } else {
                                        columnsList = (List) fromOperatorMap.get("simpleColumnsList");
                                    }

                                    List<String> fromOperatorsColumnsList = getSourceColumnsList(fromOperatorMap);
                                    if (fromOperatorsColumnsList != null && !fromOperatorsColumnsList.isEmpty()) {
                                        simpleColumnsList.addAll(fromOperatorsColumnsList);
                                    } else {
                                        simpleColumnsList.addAll(columnsList);
                                    }

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
                                            String treeObjColumnId = "";
                                            String treeObjColumnText = "";
                                            String treeObjColumnValue = "";
                                            String treeObjColumnParentId = "";
                                            if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))) {
                                                treeObjColumnId = tableName + ":" + columnsList.get(j);
                                                treeObjColumnText = columnsList.get(j);
                                                treeObjColumnValue = tableName + ":" + columnsList.get(j);
                                                treeObjColumnParentId = tableName;
                                            } else if (!tableExist) {
                                                treeObjColumnId = tableName + ":" + columnsList.get(j);
                                                treeObjColumnText = columnsList.get(j);
                                                treeObjColumnValue = tableNameLabel + ":" + columnsList.get(j);
                                                treeObjColumnParentId = tableName;
                                            } else {
                                                treeObjColumnId = fromConnectObj.get("CONN_USER_NAME") + "." + tableName
                                                        + ":" + columnsList.get(j);
                                                treeObjColumnText = columnsList.get(j);
                                                treeObjColumnValue = fromConnectObj.get("CONN_USER_NAME") + "."
                                                        + tableName + ":" + columnsList.get(j);
                                                treeObjColumnParentId = fromConnectObj.get("CONN_USER_NAME") + "."
                                                        + tableName;
                                            }
                                            JSONObject columnObj = new JSONObject();
                                            columnObj.put("id", treeObjColumnId);
                                            columnObj.put("text", treeObjColumnText);
                                            columnObj.put("value", treeObjColumnValue);
                                            columnObj.put("parentid", treeObjColumnParentId);
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
                                fromTableJoinColsObj.put((String) tableName, fromTableColsArrayForClause);

                            } else {
                                resultObj.put("connectionFlag", "N");
                                resultObj.put("connectionMessage", fromConnObj);
                            }

                        } // end else for file type
                    }
                    index++;
                } // end loop

                resultObj.put("fromTableColsArray", fromTableJoinColsObj);

            }

            String selectedOperatorId = request.getParameter("selectedOperatorId");
            JSONObject operator = (JSONObject) operators.get(selectedOperatorId);
            JSONObject trfmRulesData = (JSONObject) operator.get("trfmRules");

            orderByColsCondition = "<div class=\"visionEtlJoinClauseMain\">" + "<div class=\"visionEtlAddIconDiv\">"
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

            if (trfmRulesData != null && !trfmRulesData.isEmpty() && trfmRulesData.size() != 0) {
                orderByData = (JSONArray) trfmRulesData.get("orderByData");
            }
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
                            + ((JSONObject) orderByData.get(i)).get("columnName") + "\" actual-value=\""
                            + ((JSONObject) orderByData.get(i)).get("columnNameActualValue") + "\" readonly=\"true\">"
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
            } else {
                orderByTrString = orderBySingleTrString;
            }

            // ravi end
            orderByColsCondition = orderByColsCondition + orderByTrString + "</tbody></table></div>";
            resultObj.put("orderByCondition", orderByColsCondition);
            resultObj.put("orderByTrString", orderBySingleTrString);
            resultObj.put("simpleColumnsList", simpleColumnsList);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    public JSONObject groupByComponentTrfmRules(HttpServletRequest request, HttpServletResponse response) {

        JSONObject resultObj = new JSONObject();
        JSONArray sourceTablesArray = new JSONArray();
        JSONArray fromTableColsArray = new JSONArray();
        Connection fromConnection = null;
        JCO.Client fromJCOConnection = null;
        JSONObject fromConnectObj = new JSONObject();
        try {
            String orderByColsCondition = "";
            List<Object[]> orderByDataList = new ArrayList<>();
            JSONArray orderByData = new JSONArray();
            String flowchartDataStr = request.getParameter("flowchartData");
            JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
            JSONObject operators = (JSONObject) flowchartData.get("operators");
            String sourceOperatorsStr = request.getParameter("sourceOperators");
            List<Map> fromOperatorsList = (List<Map>) JSONValue.parse(sourceOperatorsStr);

            JSONObject colMappingObj = columnMappingTrfmRulesForComponent(request,
                    response);
            resultObj.putAll(colMappingObj);
            String selectedOperatorId = request.getParameter("selectedOperatorId");
            JSONObject operator = (JSONObject) operators.get(selectedOperatorId);
            JSONObject trfmRulesData = (JSONObject) operator.get("trfmRules");

            String groupByCols = "<div class=\"visionEtlJoinClauseMain\">" + "<div class=\"visionEtlAddIconDiv\">"
                    + "<img data-trstring='' src=\"images/Add icon.svg\" id=\"visionEtlAddRowIcon\" "
                    + "class=\"visionEtlAddRowIcon\" title=\"Add new where clause\""
                    + " onclick=addNewGroupClauseRow(event,id,this) "
                    + "style=\"width:15px;height: 15px;cursor:pointer; float: left;\"/>" + "</div>"
                    + "<div class=\"visionEtlJoinClauseTablesDiv\">" + "<table id=\"fromTablesGroupCauseTable\""
                    + " class=\"visionEtlJoinClauseTable\" style=\"width: 100%;\" border=\"1\">" + "<thead>" + "<tr>"
                    + "<th width='2%' class=\"\" "
                    + "style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\"></th>"
                    + "<th width='98%' class=\"\" "
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

            if (trfmRulesData != null && !trfmRulesData.isEmpty() && trfmRulesData.size() != 0) {
                groupByData = (JSONArray) trfmRulesData.get("groupByData");

            }

            if (groupByData != null && groupByData.size() != 0 // && "Y".equalsIgnoreCase(trfmRulesChanged)
                    ) {
                for (int i = 0; i < groupByData.size(); i++) {
                    groupByTrString += "<tr>" + "<td width='1%'>" + "<img src=\"images/Delete_Red_Icon.svg\" "
                            + "onclick=\"deleteSelectedRow(this)\" class=\"visionColMappingImg\" "
                            + "title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"></td>"
                            + "<td width='99%'  class=\"sourceJoinColsTd\">"
                            + "<input class=\"visionColMappingInput\" type=\"text\" value=\""
                            + ((JSONObject) groupByData.get(i)).get("columnName") + "\" actual-value=\""
                            + ((JSONObject) groupByData.get(i)).get("columnNameActualValue") + "\" readonly=\"true\">"
                            + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \""
                            + " onclick=\"selectColumn(this,'fromColumn')\" style=\"\"></td>" + "</td>" + "</tr>";
                }
            } else {
                groupByTrString = groupBySingleTrString;
            }
            // ravi end

            groupByCols = groupByCols + groupByTrString + "";
            groupByCols += "</tbody></table></div>";
            resultObj.put("groupByCondition", groupByCols);
            resultObj.put("groupByTrString", groupBySingleTrString);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
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
            JSONArray masterTableLabelsArray = new JSONArray();
            String dbObjStr = request.getParameter("dbObj");
            if (dbObjStr != null && !"".equalsIgnoreCase(dbObjStr) && !"null".equalsIgnoreCase(dbObjStr)) {
                dbObj = (JSONObject) JSONValue.parse(dbObjStr);
            }
            String childTableName = request.getParameter("tableName");
            String childTableLabel = request.getParameter("tableNameLabel");
            if (childTableName != null && !"".equalsIgnoreCase(childTableName)
                    && !"null".equalsIgnoreCase(childTableName) && childTableName.contains(".") && dbObj != null
                    && !dbObj.isEmpty() && !dbObj.containsKey("fileName")) {
                childTableName = childTableName.substring(childTableName.lastIndexOf(".") + 1);
            }
            String masterTables = request.getParameter("sourceTables");
            String masterTableLabels = request.getParameter("sourceTablesLabels");
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
            if (masterTableLabels != null && !"".equalsIgnoreCase(masterTableLabels)
                    && !"".equalsIgnoreCase(masterTableLabels)) {
                masterTableLabelsArray = (JSONArray) JSONValue.parse(masterTableLabels);
            }
            JSONArray childTableColsTreeArray = new JSONArray();
            Object fromReturendObj = null;
            if (dbObj != null && !dbObj.isEmpty() && !dbObj.containsKey("fileName")) {
                fromReturendObj = componentUtilities.getConnection(dbObj);

                String matchedSelectStr = "";
                boolean tableExist = true;
                List<Object[]> childTableColumnList = new ArrayList<>();
                if (fromReturendObj instanceof Connection) {
                    connection = (Connection) fromReturendObj;
                    childTableColumnList = componentUtilities.getTreeDMTableColumnsOpt(connection, request, dbObj,
                            childTableName);
                } else if (fromReturendObj instanceof JCO.Client) {
                    tableExist = true;
                    fromJCOConnection = (JCO.Client) fromReturendObj;
                    childTableColumnList = componentUtilities.getSAPTableColumns(request, fromJCOConnection,
                            childTableName);
                }
                if (childTableColumnList != null && childTableColumnList.isEmpty()) {
                    tableExist = false;
                    String selectedOperatorId = childTableLabel.substring(childTableLabel.lastIndexOf("_") + 1);
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
                        tableId = ("SAP_ECC".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1"))) ? childTableName
                                : dbObj.get("CONN_USER_NAME") + "." + childTableName);
                        tabletext = dbObj.get("CONNECTION_NAME") + "." + childTableName;
                        tablevalue = ("SAP_ECC".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1")))
                                ? childTableName
                                : dbObj.get("CONN_USER_NAME") + "." + childTableName);
                    } else {
                        tableId = childTableName;
                        tabletext = childTableLabel;
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
                                columnId = (String) (("SAP_ECC".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1"))) ? childColsArray[0]
                                        : dbObj.get("CONN_USER_NAME") + "." + childColsArray[0])
                                        + ":" + childColsArray[1]);
                                columntext = (String) childColsArray[1];
                                columnValue = (String) (("SAP_ECC".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1"))) ? childColsArray[0]
                                        : dbObj.get("CONN_USER_NAME") + "." + childColsArray[0])
                                        + ":" + childColsArray[1]);
                                columnParentId = (String) ("SAP_ECC".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1"))) ? childColsArray[0]
                                        : dbObj.get("CONN_USER_NAME") + "." + childColsArray[0]);

                                columnObj.put("id", columnId);
                                columnObj.put("text", columntext);
                                columnObj.put("value", columnValue);
                                columnObj.put("parentid", columnParentId);
                                childTableColsTreeArray.add(columnObj);
                            } else {
                                columnId = (String) childColsArray[1];
                                columntext = (String) childColsArray[1];
                                columnValue = (String) childColsArray[1];
                                columnParentId = (String) childColsArray[0];

                                columnObj.put("id", childTableName + ":" + columnId);
                                columnObj.put("text", columntext);
                                columnObj.put("value", childTableLabel + ":" + columnValue);
                                columnObj.put("parentid", columnParentId);
                                childTableColsTreeArray.add(columnObj);
                            }

                        }

                    }
                }

            } else {
                String fileType = (String) dbObj.get("fileType");
                String fromFileName = (String) dbObj.get("fileName");
                String filePath = (String) dbObj.get("filePath");
                List<String> headers = componentUtilities.getHeadersOfImportedFile(request, filePath);
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
                String masterTableNameLabel = (String) masterTableLabelsArray.get(i);
                JSONObject masterDBObject = (JSONObject) joinDBObject.get(masterTableName);
                if (masterDBObject != null && !masterDBObject.isEmpty() && !masterDBObject.containsKey("fileName")) {
                    Object masterConection = null;
                    if (masterDBObject != null && !masterDBObject.isEmpty()
                            && !((String) masterDBObject.get("CONNECTION_NAME"))
                                    .equalsIgnoreCase((String) dbObj.get("CONNECTION_NAME"))) {
                        masterConection = componentUtilities.getConnection(masterDBObject);
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
                        columnList = componentUtilities.getTreeDMTableColumnsOpt(connection, request, masterDBObject,
                                masterTableName);
                    } else if (masterConection instanceof JCO.Client) {
                        columnList = componentUtilities.getSAPTableColumns(request, fromJCOConnection, masterTableName);
                    }

                    if (columnList != null && columnList.isEmpty()) {
                        masterTableExist = false;

                        String selectedOperatorId = masterTableNameLabel
                                .substring(masterTableNameLabel.lastIndexOf("_") + 1);
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
                            tableId = ("SAP_ECC".equalsIgnoreCase(String.valueOf(masterDBObject.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1")))
                                    ? masterTableName
                                    : masterDBObject.get("CONN_USER_NAME") + "." + masterTableName);
                            tabletext = masterDBObject.get("CONNECTION_NAME") + "." + masterTableName;
                            tablevalue = ("SAP_ECC".equalsIgnoreCase(String.valueOf(masterDBObject.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1")))
                                    ? masterTableName
                                    : masterDBObject.get("CONN_USER_NAME") + "." + masterTableName);
                        } else {
                            tableId = masterTableName;
                            tabletext = masterTableNameLabel;
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
                                    columnId = (String) (("SAP_ECC".equalsIgnoreCase(String.valueOf(masterDBObject.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1"))) ? masterColsArray[0]
                                            : masterDBObject.get("CONN_USER_NAME") + "." + masterColsArray[0])
                                            + ":" + masterColsArray[1]);
                                    columntext = (String) masterColsArray[1];
                                    columnValue = (String) (("SAP_ECC".equalsIgnoreCase(String.valueOf(masterDBObject.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1"))) ? masterColsArray[0]
                                            : masterDBObject.get("CONN_USER_NAME") + "." + masterColsArray[0])
                                            + ":" + masterColsArray[1]);
                                    columnParentId = (String) ("SAP_ECC".equalsIgnoreCase(String.valueOf(masterDBObject.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1"))) ? masterColsArray[0]
                                            : masterDBObject.get("CONN_USER_NAME") + "." + masterColsArray[0]);

                                    columnObj.put("id", columnId);
                                    columnObj.put("text", columntext);
                                    columnObj.put("value", columnValue);
                                    columnObj.put("parentid", columnParentId);
                                    masterTableColsTreeArray.add(columnObj);
                                } else {
                                    columnId = (String) masterColsArray[1];
                                    columntext = (String) masterColsArray[1];
                                    columnValue = (String) masterColsArray[1];
                                    columnParentId = (String) masterColsArray[0];

                                    columnObj.put("id", masterTableName + ":" + columnId);
                                    columnObj.put("text", columntext);
                                    columnObj.put("value", masterTableNameLabel + ":" + columnValue);
                                    columnObj.put("parentid", columnParentId);
                                    masterTableColsTreeArray.add(columnObj);
                                }

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
                    List<String> headers = componentUtilities.getHeadersOfImportedFile(request, filePath);
                    headers = componentUtilities.fileHeaderValidations(headers);
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
                                + "<input class='visionColJoinMappingInput visionColFuncInput' type='text' "
                                + " value='"
                                + String.valueOf(joinColMapObj.get("childTableColumn")).replace("'", "&#39;") + "' "
                                + " funcolumnslist='"
                                + String.valueOf(joinColMapObj.get("childFunColumnsList")).replace("'", "&#39;") + "' "
                                + " actual-value='"
                                + String.valueOf(joinColMapObj.get("childTableColumnActualValue")).replace("'", "&#39;")
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
                                + "<input class='visionColJoinMappingInput visionColFuncInput' type='text' "
                                + " value='"
                                + String.valueOf(joinColMapObj.get("masterTableColumn")).replace("'", "&#39;") + "' "
                                + " funcolumnslist='"
                                + String.valueOf(joinColMapObj.get("masterFunColumnsList")).replace("'", "&#39;") + "' "
                                + " actual-value='"
                                + String.valueOf(joinColMapObj.get("masterTableColumnActualValue")).replace("'",
                                        "&#39;")
                                + "' " + " data-funobjstr='"
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

    public boolean checkTableExsist(Connection connection, String tableName) {
        boolean tableExsist = false;
        PreparedStatement preparedStatement = null;
        try {
            String query = "SELECT COUNT(*) FROM " + tableName + " WHERE 1=2";
            preparedStatement = connection.prepareStatement(query);
            preparedStatement.execute();
            tableExsist = true;
        } catch (Exception e) {
            tableExsist = false;
        } finally {
            try {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
            } catch (Exception e) {
            }
        }
        return tableExsist;
    }

    public List<String> getSourceColumnsList(Map fromOperatorMap) {
        List columnsList = new ArrayList();
        try {
            String iconType = (String) fromOperatorMap.get("iconType");
            String tableName = (String) fromOperatorMap.get("tableName");
            String tableNameLabel = (String) fromOperatorMap.get("tableNameLabel");
            JSONObject trfmRules = (JSONObject) fromOperatorMap.get("trfmRules");
            if (iconType != null && !"".equalsIgnoreCase(iconType) && trfmRules != null) {
                if ("JOIN".equalsIgnoreCase(iconType)) {
                    JSONArray columnMappingArray = (JSONArray) trfmRules.get("colMappingsData");

                    for (int i = 0; i < columnMappingArray.size(); i++) {
                        JSONObject columnMappingObj = (JSONObject) columnMappingArray.get(i);
                        String sourceColumn = (String) columnMappingObj.get("sourceColumn");
                        if (sourceColumn != null && sourceColumn.contains(":")) {
                            sourceColumn = sourceColumn.split(":")[1];
                            columnsList.add(sourceColumn);
                        } else {
                            columnsList.add(sourceColumn);
                        }

                    }

                    String joinColumnMapping = (String) trfmRules.get("joinColumnMapping");
                    JSONObject joinColumnMappingObj = new JSONObject();
                    JSONObject joinDBObject = new JSONObject();

                    if (joinColumnMapping != null && !"".equalsIgnoreCase(joinColumnMapping)) {
                        joinColumnMappingObj = (JSONObject) JSONValue.parse(joinColumnMapping);
                    }

                    if (joinColumnMappingObj != null && !joinColumnMappingObj.isEmpty()) {
                        Set keySet = joinColumnMappingObj.keySet();
                        List keysList = new ArrayList();
                        keysList.addAll(keySet);
                        Collections.sort(keysList);
                        for (int i = 0; i < keysList.size(); i++) {
                            Object keyName = keysList.get(i);
                            JSONObject joinColMapObj = (JSONObject) joinColumnMappingObj.get(keysList.get(i));
                            if (joinColMapObj != null && !joinColMapObj.isEmpty()) {
                                String childColumnName = (String) joinColMapObj.get("childTableColumn");
                                String masterColumnName = (String) joinColMapObj.get("masterTableColumn");
                                if (childColumnName.contains(tableNameLabel)) {
                                    childColumnName = childColumnName.split(":")[1];
                                    columnsList.add(childColumnName);
                                }
                                if (masterColumnName.contains(tableNameLabel)) {
                                    masterColumnName = masterColumnName.split(":")[1];
                                    columnsList.add(masterColumnName);
                                }
                            }
                        }
                    }

                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return columnsList;
    }

    public JSONObject etlProgressBarInfo(HttpServletRequest request) {
        return processJobComponentsDAO.etlProgressBarInfo(request);
    }

    public JSONObject fetchAvailableConnections(HttpServletRequest request) {
        return processJobComponentsDAO.fetchAvailableConnections(request);
    }

    public JSONObject pivotComponentTrfmRules(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        List columnsList = new ArrayList();
        Object connection = null;
        try {
            String selectedOperatorId = request.getParameter("selectedOperatorId");
            String flowchartDataStr = request.getParameter("flowchartData");
            JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
            JSONArray connectedFromOpArray = processJobComponentsService.getConnectedFromOperatorIds(request,
                    selectedOperatorId, flowchartData);
            if (connectedFromOpArray != null && !connectedFromOpArray.isEmpty()) {
                String fromOperatorId = String.valueOf(connectedFromOpArray.get(0));
                JSONObject operators = (JSONObject) flowchartData.get("operators");
                JSONObject fromOperator = (JSONObject) operators.get(fromOperatorId);
                JSONObject connObj = (JSONObject) fromOperator.get("connObj");
                String fromTableName = (String) fromOperator.get("tableName");
                if (fromTableName != null && fromTableName.contains(".")) {
                    fromTableName = fromTableName.split("\\.")[1];
                }
                connection = componentUtilities.getConnection(connObj);
                if (connection instanceof Connection) {
                    List<Object[]> columnsListObj = componentUtilities.getTableColumnsOpt((Connection) connection,
                            connObj, fromTableName);
                    columnsList = (List) columnsListObj.stream().map(rowData -> rowData[2])
                            .collect(Collectors.toList());
                } else {
                    List<Object[]> columnsListObj = componentUtilities.getSAPTableColumns(request,
                            (JCO.Client) connection, fromTableName);
                    columnsList = (List) columnsListObj.stream().map(rowData -> rowData[2])
                            .collect(Collectors.toList());
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (connection != null && connection instanceof Connection) {
                    ((Connection) connection).close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        resultObj.put("columnsList", columnsList);
        return resultObj;
//        return processJobComponentsDAO.pivotComponentTrfmRules(request);
    }

    public JSONObject unpivotComponentTrfmRules(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        List columnsList = new ArrayList();
        Object connection = null;
        try {
            String selectedOperatorId = request.getParameter("selectedOperatorId");
            String flowchartDataStr = request.getParameter("flowchartData");
            JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
            JSONArray connectedFromOpArray = processJobComponentsService.getConnectedFromOperatorIds(request,
                    selectedOperatorId, flowchartData);
            if (connectedFromOpArray != null && !connectedFromOpArray.isEmpty()) {
                String fromOperatorId = String.valueOf(connectedFromOpArray.get(0));
                JSONObject operators = (JSONObject) flowchartData.get("operators");
                JSONObject fromOperator = (JSONObject) operators.get(fromOperatorId);
                JSONObject connObj = (JSONObject) fromOperator.get("connObj");
                String fromTableName = (String) fromOperator.get("tableName");
                if (fromTableName != null && fromTableName.contains(".")) {
                    fromTableName = fromTableName.split("\\.")[1];
                }
                connection = componentUtilities.getConnection(connObj);
                if (connection instanceof Connection) {
                    List<Object[]> columnsListObj = componentUtilities.getTableColumnsOpt((Connection) connection,
                            connObj, fromTableName);
                    columnsList = (List) columnsListObj.stream().map(rowData -> rowData[2])
                            .collect(Collectors.toList());
                } else {
                    List<Object[]> columnsListObj = componentUtilities.getSAPTableColumns(request,
                            (JCO.Client) connection, fromTableName);
                    columnsList = (List) columnsListObj.stream().map(rowData -> rowData[2])
                            .collect(Collectors.toList());
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (connection != null && connection instanceof Connection) {
                    ((Connection) connection).close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        resultObj.put("columnsList", columnsList);
        return resultObj;
    }

    public JSONObject getComponentInfo(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        List<Object[]> dataList = new ArrayList();
        Object connection = null;
        try {
            String compName = request.getParameter("compName");
            dataList = iVisionTransformComponentsDAO.getComponentInfo(request, compName);
            if (dataList != null && !dataList.isEmpty()) {
                Object[] rowData = dataList.get(0);
                String comp = String.valueOf(rowData[0]);
                String image = String.valueOf(rowData[1]);
                Clob compDesc = rowData[2] != null ? (Clob) rowData[2] : null;
                String compDesString = new PilogUtilities().clobToString(compDesc);
                String base64String = "";
                Blob compVideo = rowData[3] != null ? (Blob) rowData[3] : null;
                if (compVideo != null) {

                    int blobLength = (int) compVideo.length();
                    byte[] byteArray = compVideo.getBytes(1, blobLength);

                    base64String = Base64.getEncoder().encodeToString(byteArray);
                }
                resultObj.put("componentName", comp);
                resultObj.put("image", image);
                resultObj.put("compDesc", compDesString);
                resultObj.put("compVideo", base64String);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
        return resultObj;
    }

    public JSONObject getETLJobPreviewCount(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {
            String jobId = request.getParameter("jobId");
            List dataList = componentUtilities.getETLJobPreviewCount(
                    (String) request.getSession(false).getAttribute("ssUsername"),
                    (String) request.getSession(false).getAttribute("ssOrgId"),
                    jobId);
            if (dataList != null && !dataList.isEmpty()) {
                for (int i = 0; i < dataList.size(); i++) {
                    Object[] rowdata = (Object[]) dataList.get(i);
                    String operator = String.valueOf(rowdata[3]);
                    String count = String.valueOf(rowdata[4]);
                    resultObj.put(operator, count);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    public List getETLJObReconciliation(HttpServletRequest request) {
        List dataList = new ArrayList();
        try {
            String jobId = request.getParameter("jobId");
            String subJobId = request.getParameter("subJobId");
            dataList = componentUtilities.getETLJObReconciliation(
                    (String) request.getSession(false).getAttribute("ssUsername"),
                    (String) request.getSession(false).getAttribute("ssOrgId"),
                    jobId, subJobId);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dataList;
    }

    public List getColumnReconciliation(HttpServletRequest request, String jobId, String sourceTable,
            String targetTable, JSONObject targetTrfmRules, JSONObject sourceConnObj, JSONObject targetConnObj,
            String sessionUserName, String orgnId, String subJobId) {
        List dataList = new ArrayList();
        try {
            dataList = componentUtilities.getColumnReconciliation(request, jobId, sourceTable,
                    targetTable, targetTrfmRules, sourceConnObj, targetConnObj, sessionUserName, orgnId, subJobId);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dataList;
    }

public void etlGridExport(HttpServletRequest request, HttpServletResponse response) {

        String message = "";
        try {
            String userName = (String) request.getSession(false).getAttribute("ssUsername");

            String tableName = request.getParameter("etlExportTableName");
            String fileType = request.getParameter("fileType");
            String rowDataArayStr = request.getParameter("rowDataArray");
            JSONArray rowDataAray = (JSONArray) JSONValue.parse(rowDataArayStr);
            List dataList = new ArrayList();
            List columnsList = new ArrayList();
            for (int i = 0; i < rowDataAray.size(); i++) {

                JSONObject rowdata = (JSONObject) rowDataAray.get(i);
                Object[] rowdataArr = new Object[rowdata.size()];
                int j = 0;
                for (Object key : rowdata.keySet()) {
                    if (i == 0) {
                        columnsList.add(key);
                    }
                    rowdataArr[j] = rowdata.get(key);
                    j++;
                }
                dataList.add(rowdataArr);
            }

            String filePath = etlFilePath + "ETL_EXPORT_" + File.separator + userName;

            String fileName = "V10ETLExport_" + AuditIdGenerator.genRandom32Hex() + "." + fileType;
            int insertCount = 0;
            if (fileType != null && !"null".equalsIgnoreCase(fileType) && ("XLSX".equalsIgnoreCase(fileType) || "XLS".equalsIgnoreCase(fileType))) {
                insertCount = fileProcessJobService.insertIntoXLSXFile(request, null, columnsList, dataList, filePath, fileName);

            } else if (fileType != null && !"null".equalsIgnoreCase(fileType) && ("CSV".equalsIgnoreCase(fileType) || "TXT".equalsIgnoreCase(fileType))) {
                insertCount = fileProcessJobService.insertIntoTextOrCSVFile(request, null, columnsList, dataList, filePath, fileName);
            } else if (fileType != null && !"null".equalsIgnoreCase(fileType) && "XML".equalsIgnoreCase(fileType)) {
                insertCount = fileProcessJobService.insertIntoXMLFile(request, null, columnsList, dataList, filePath, fileName);
            } else if (fileType != null && !"null".equalsIgnoreCase(fileType) && "JSON".equalsIgnoreCase(fileType)) {
                insertCount = fileProcessJobService.insertIntoJSONFile(request, null, columnsList, dataList, filePath, fileName);
            }

            String outPutfilePath = etlFilePath + "ETL_EXPORT_" + File.separator + userName + File.separator + fileName;
            File outputFile = new File(outPutfilePath);
            ServletContext ctx = request.getSession().getServletContext();
            InputStream fis = new FileInputStream(outputFile);
            String mimeType = ctx.getMimeType(outputFile.getAbsolutePath());
            response.setContentType(mimeType != null ? mimeType : "application/octet-stream");

            response.setHeader("Content-Disposition", "attachment; filename=\"" + (tableName + "." + fileType).trim() + "\"");

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
            e.printStackTrace();
        } finally {

        }

    }

    public int createTemPreviewTables(HttpServletRequest request) {
        int count = 0;
        try {
            String flowChartDataStr = request.getParameter("flowChartData");
            JSONObject flowChartData = (JSONObject) JSONValue.parse(flowChartDataStr);
            JSONObject operators = (JSONObject) flowChartData.get("operators");
            for (Object key : operators.keySet()) {
                JSONObject operator = (JSONObject) operators.get(key);
                if (operator.get("nonPreviewtableName") != null) {
                    String previewTable = String.valueOf(operator.get("tableName"));
                    String actualTable = String.valueOf(operator.get("nonPreviewtableName"));
                    JSONObject connObj = (JSONObject) operator.get("connObj");
                    Connection connection = (Connection) componentUtilities.getConnection(connObj);
                    if (actualTable.contains(".")) {
                        actualTable = actualTable.split("\\.")[1];
                    }
                    String createTableQuery = "CREATE TABLE " + previewTable + " AS SELECT * FROM " + actualTable;
                    if (!previewTable.endsWith("MERGE")) {
                        createTableQuery += " WHERE 1=2";
                    }
                    boolean tableCreated = componentUtilities.createTableWithQuery(createTableQuery, connection);
                    System.out.println(previewTable + " preview table created");
                    if (connection != null) {
                        connection.close();
                    }
                    count++;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return count;
    }

    public int deleteTemPreviewTables(HttpServletRequest request) {
        int count = 0;
        try {
            String flowChartDataStr = request.getParameter("flowChartData");
            JSONObject flowChartData = (JSONObject) JSONValue.parse(flowChartDataStr);
            JSONObject operators = (JSONObject) flowChartData.get("operators");
            for (Object key : operators.keySet()) {
                JSONObject operator = (JSONObject) operators.get(key);
                if (operator.get("nonPreviewtableName") != null) {
                    String previewTable = String.valueOf(operator.get("tableName"));
                    String actualTable = String.valueOf(operator.get("nonPreviewtableName"));
                    JSONObject connObj = (JSONObject) operator.get("connObj");
                    Connection connection = (Connection) componentUtilities.getConnection(connObj);
                    componentUtilities.dropStagingTable(previewTable, connection);
                    System.out.println(previewTable + " preview table deleted");
                    connection.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return count;
    }

    public JSONObject checkDataTypeValidations(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        boolean dataTypeMismatch = false;
        String misMatchTable = "<table style='max-height: 300px;'>"
                + "<thead>"
                + "<th style=\"background: #ddd none repeat scroll 0 0;color: #000;text-align: center; border: 1px solid #FFF;\" >Source Table</th>"
                + "<th style=\"background: #ddd none repeat scroll 0 0;color: #000;text-align: center; border: 1px solid #FFF;\" >Source Column</th>"
                + "<th style=\"background: #ddd none repeat scroll 0 0;color: #000;text-align: center; border: 1px solid #FFF;\" >Source DataType</th>"
                + "<th style=\"background: #ddd none repeat scroll 0 0;color: #000;text-align: center; border: 1px solid #FFF;\" >Source length</th>"
                + "<th style=\"background: #ddd none repeat scroll 0 0;color: #000;text-align: center; border: 1px solid #FFF;\" >Target Table</th>"
                + "<th style=\"background: #ddd none repeat scroll 0 0;color: #000;text-align: center; border: 1px solid #FFF;\" >Target Column</th>"
                + "<th style=\"background: #ddd none repeat scroll 0 0;color: #000;text-align: center; border: 1px solid #FFF;\" >Target DataType</th>"
                + "<th style=\"background: #ddd none repeat scroll 0 0;color: #000;text-align: center; border: 1px solid #FFF;\" >Target length</th>"
                + "</thead></tbody>";

        try {
            String flowChartDataStr = request.getParameter("flowChartData");
            JSONObject flowChartData = (JSONObject) JSONValue.parse(flowChartDataStr);
            JSONObject operators = (JSONObject) flowChartData.get("operators");
            JSONObject subJobsObj = new JSONObject();
            for (Object key : operators.keySet()) {
                JSONObject operator = (JSONObject) operators.get(key);
                String subJobId = String.valueOf(operator.get("subJobId"));
                List opList = subJobsObj.get(subJobId) != null ? (List) subJobsObj.get(subJobId) : new ArrayList();
                opList.add(key);
                subJobsObj.put(subJobId, opList);
            }

            JSONObject sourceAndTargetObject = new JSONObject();
            for (Object key : subJobsObj.keySet()) {
                List subJobIdList = (List) subJobsObj.get(key);
                JSONObject sourceOperator = new JSONObject();
                JSONObject targetOperator = new JSONObject();
                for (int i = 0; i < subJobIdList.size(); i++) {
                    String operatorId = String.valueOf(subJobIdList.get(i));
                    JSONObject operatorData = (JSONObject) operators.get(operatorId);
                    if (operatorData.get("sourceOperator") != null && "Y".equalsIgnoreCase(String.valueOf(operatorData.get("sourceOperator")))) {
                        sourceOperator = operatorData;
                    }
                    if (operatorData.get("targetOperator") != null && "Y".equalsIgnoreCase(String.valueOf(operatorData.get("targetOperator")))) {
                        targetOperator = operatorData;
                    }

                }
                JSONObject sourceConnObj = (JSONObject) sourceOperator.get("connObj");
                String sourceTableName = (String) sourceOperator.get("tableName");
                if (sourceTableName.contains(".")) {
                    sourceTableName = sourceTableName.split("\\.")[1];
                }

                List<Object[]> sourceColumnsObjList = new ArrayList();
                Object srcconnection = componentUtilities.getConnection(sourceConnObj);
                String srcdbType = (String) sourceConnObj.get("CONN_CUST_COL1");

                if (srcconnection instanceof Connection) {
                    sourceColumnsObjList = componentUtilities.getTableColumnsOpt((Connection) srcconnection, sourceConnObj, sourceTableName);
                    ((Connection) srcconnection).close();

                } else if (srcconnection instanceof JCO.Client) {
                    sourceColumnsObjList = erpProcessJobService.getSAPTableColumnsWithType(request, (JCO.Client) srcconnection, sourceTableName);
                }

                List<Object[]> convSourceColumnsObject = convertColumnsObjectToOracle(request, sourceColumnsObjList, srcdbType);

                JSONObject targetConnObj = (JSONObject) targetOperator.get("connObj");
                String targetTableName = (String) targetOperator.get("tableName");
                if (targetTableName.contains(".")) {
                    targetTableName = targetTableName.split("\\.")[1];
                }
                Connection targetconnection = (Connection) componentUtilities.getConnection(targetConnObj);

                List<Object[]> targetColumnsObjList = new ArrayList();
                if (targetconnection instanceof Connection) {
                    targetColumnsObjList = componentUtilities.getTableColumnsOpt((Connection) targetconnection, targetConnObj, targetTableName);
                    ((Connection) targetconnection).close();

                } else if (targetconnection instanceof JCO.Client) {
                    targetColumnsObjList = erpProcessJobService.getSAPTableColumnsWithType(request, (JCO.Client) targetconnection, targetTableName);
                }
                String targetdbType = (String) targetConnObj.get("CONN_CUST_COL1");
                List<Object[]> convTargetColumnsObject = convertColumnsObjectToOracle(request, targetColumnsObjList, targetdbType);

                //	targetOperator
                for (int k = 0; k < sourceColumnsObjList.size(); k++) {
                    Object[] sourceRowData = sourceColumnsObjList.get(k);
                    Object[] targetRowData = targetColumnsObjList.get(k);

                    Object[] convSourceRowData = convSourceColumnsObject.get(k);
                    Object[] convTargetRowData = convTargetColumnsObject.get(k);
                    if (String.valueOf(convSourceRowData[3]).equalsIgnoreCase(String.valueOf(convTargetRowData[3]))) {

                    } else {
                        dataTypeMismatch = true;
                        misMatchTable += "<tr>"
                                + "<td style=\"border: 1px solid #ddd;\" >" + sourceTableName + "</td>"
                                + "<td style=\"border: 1px solid #ddd;\">" + sourceRowData[2] + "</td>"
                                + "<td style=\"border: 1px solid #ddd;\">" + sourceRowData[3] + "</td>"
                                + "<td style=\"border: 1px solid #ddd;\">" + sourceRowData[4] + "</td>"
                                + "<td style=\"border: 1px solid #ddd;\">" + targetTableName + "</td>"
                                + "<td style=\"border: 1px solid #ddd;\" >" + targetRowData[2] + "</td>"
                                + "<td style=\"border: 1px solid #ddd;\" >" + targetRowData[3] + "</td>"
                                + "<td style=\"border: 1px solid #ddd;\" >" + targetRowData[4] + "</td>"
                                + "</tr>";
                    }
                }

            }

            misMatchTable += "</tbody></table>";
            resultObj.put("dataTypeMismatch", dataTypeMismatch);
            resultObj.put("misMatchTable", misMatchTable);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    public List<Object[]> convertColumnsObjectToOracle(HttpServletRequest request, List<Object[]> columnsObject, String dbType) {
        List<Object[]> newColumnsObject = new ArrayList(columnsObject);
        try {

            // if (!"ORACLE".equalsIgnoreCase(dbType)) {
            String convQuery = "SELECT FROM_TYPE, TO_TYPE  FROM  B_DM_DATA_TYPE_CONV WHERE  FROM_SYS_TYPE ='" + dbType + "' AND TO_SYS_TYPE='ORACLE' ";
            List<Object[]> dataList = componentsDAO.getDataListFromQuery(request, convQuery, Collections.EMPTY_MAP);
            if (dataList != null && !dataList.isEmpty()) {
                JSONObject convObj = new JSONObject();
                for (int i = 0; i < dataList.size(); i++) {
                    Object[] rowData = dataList.get(i);
                    convObj.put(rowData[0], rowData[1]);
                }
                for (int i = 0; i < columnsObject.size(); i++) {
                    Object[] rowData = newColumnsObject.get(i);
                    String oldDataType = (String) rowData[3];
                    String newDataType = (String) convObj.get(oldDataType);
                    if (newDataType.equalsIgnoreCase("VARCHAR2")) {
                        //newDataType = "VARCHAR2 (4000)";
                    }
                    rowData[3] = newDataType;
                    newColumnsObject.set(i, rowData);
                }
            }
            // } 

        } catch (Exception e) {

        }
        return newColumnsObject;
    }

    public JSONObject apiCompnentTrfmRules(HttpServletRequest request) {
        JSONObject resultObject = new JSONObject();
        try {

            String flowchartDataString = request.getParameter("flowchartData");
            String selectedOperatorId = request.getParameter("selectedOperatorId");

            JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataString);
            JSONObject operators = (JSONObject) flowchartData.get("operators");
            JSONObject operatorData = (JSONObject) operators.get(selectedOperatorId);
            JSONObject trfmRules = (JSONObject) operatorData.get("trfmRules");
            String tableStr = "";
            String apiUrl = "";
            String relativePath = "";
            String httpMethod = "";
            String acceptType = "";
            JSONObject parametersObj = new JSONObject();
            if (trfmRules != null && !trfmRules.isEmpty()) {
                apiUrl = (String) trfmRules.get("apiUrl");
                relativePath = (String) trfmRules.get("relativePath");
                httpMethod = (String) trfmRules.get("httpMethod");
                acceptType = (String) trfmRules.get("acceptType");
                parametersObj = (JSONObject) trfmRules.get("parametersObj");
            }

            //String apiEndpoint = "https://api.nytimes.com/svc/books/v3/lists.json?list=hardcover-fiction&api-key=PAG4AsGEeQSaCDN5pDBorQSFfzZCHebb";
            JSONObject connObj = new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName, password, "Current_V10");
            resultObject.put("currentConnObj", connObj);

            tableStr = "<div>"
                    + "<table  style='width:400px;margin-top:30px;margin-left:20px;' >"
                    + "<tr><td width='30%' >Api url</td><td width='70%' ><input id='apiUrl' style='width:100%;' type='text' value='" + apiUrl + "' /></td></tr>"
                    + "<tr><td width='30%' >Relative Path</td><td width='70%' ><input id='relativePath' style='width:100%;' type='text' value='" + relativePath + "' /></td></tr>"
                    + "<tr><td width='30%' >Http Method</td><td width='70%' ><input id='httpMethod' style='width:100%;' type='text' value='" + httpMethod + "' /></td></tr>"
                    + "<tr><td width='30%' >Accept Type</td><td width='70%' ><input id='acceptType' style='width:100%;' type='text' value='" + acceptType + "' /></td></tr>"
                    + "</table>";

            tableStr += "<table id='apiParametersTable' style='width:400px;margin-top:15px;margin-left:20px;' >"
                    + "<tr><td width='50%' >Parameters</td><td width='50%'><img id='addApiParameter' src='images/add.png' /></td></tr>";
            if (parametersObj.size() > 0) {
                for (Object key : parametersObj.keySet()) {
                    Object val = parametersObj.get(key);
                    tableStr += "<tr><td width='50%'><input style='width:100%;' class='apiParameterKey' type='text' value='" + key + "' /></td><td width='50%'><input style='width:100%;' class='apiParameterVal' type='text' value='" + val + "' /></td></tr>";
                }
            } else {
                tableStr += "<tr><td width='50%' ><input style='width:100%;' class='apiParameterKey' type='text' value='' /></td><td width='50%' ><input style='width:100%;' class='apiParameterVal' type='text' value='' /></td></tr>";

            }

            tableStr += "</table>"
                    + "</div>";
            resultObject.put("result", tableStr);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObject;
    }

    public <T> JSONObject getResponseFromApi(String apiEndPoint, HttpEntity<MultiValueMap<String, String>> entity, Class<T> reponseType) {
        JSONObject apiResponseObject = new JSONObject();
        try {
            RestTemplate template = new RestTemplate();
            ResponseEntity<T> responseEntity = template.postForEntity(apiEndPoint, entity, reponseType);
            T response = responseEntity.getBody();
            apiResponseObject.put("apiResponse", response);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return apiResponseObject;
    }

    public JSONObject performDataProfilingFromETL(HttpServletRequest request) {
        JSONObject resultObject = new JSONObject();
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
            MultiValueMap<String, String> inputMap = new LinkedMultiValueMap<>();
            String connObjStr = (String) request.getParameter("connObj");
            JSONObject connObj = (JSONObject) JSONValue.parse(connObjStr);
            String USER_NAME = (String) connObj.get("CONN_USER_NAME");
            String PASSWORD = (String) connObj.get("CONN_PASSWORD");
            String HOST = (String) connObj.get("HOST_NAME");
            String PORT = (String) connObj.get("CONN_PORT");
            String SERVICE_NAME = (String) connObj.get("CONN_DB_NAME");
            System.out.println("CONN_USER_NAME : "+USER_NAME);
            System.out.println("HOST_NAME : "+HOST);
            inputMap.add("tableName", request.getParameter("tableName"));
            inputMap.add("colsArray", request.getParameter("colsArray"));
            inputMap.add("USER_NAME", USER_NAME);
            inputMap.add("PASSWORD", PASSWORD);
            inputMap.add("HOST", HOST);
            inputMap.add("PORT", PORT);
            inputMap.add("SERVICE_NAME", SERVICE_NAME);

            //inputMap.add("analysisType", "DHA");
            //inputMap.add("accessName", "DR101413");
            //inputMap.add("BATCH_ID", "B00000000001435");
            HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<MultiValueMap<String, String>>(inputMap,
                    headers);
            RestTemplate template = new RestTemplate();
            ResponseEntity<JSONObject> response = template.postForEntity("http://apihub.pilogcloud.com:6650/profiling", entity,
                    JSONObject.class);
            JSONObject apiDataObj = response.getBody();
            if (apiDataObj != null && !apiDataObj.isEmpty()) {
                resultObject.put("result", apiDataObj.get("report"));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObject;
    }

    public JSONObject performDataCleansingFromETL(HttpServletRequest request) {
        JSONObject resultObject = new JSONObject();
        String result = "";
        final int numberOfThreads = 1;
        final ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        final List<Future<?>> futures = new ArrayList<>();
        try {
            String responseId = AuditIdGenerator.genRandom32Hex();
            executor.submit(() -> {
                try {
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
                    MultiValueMap<String, String> inputMap = new LinkedMultiValueMap<>();

                    String connObjStr = (String) request.getParameter("connObj");
                    JSONObject connObj = (JSONObject) JSONValue.parse(connObjStr);
                    String USER_NAME = (String) connObj.get("CONN_USER_NAME");
                    String PASSWORD = (String) connObj.get("CONN_PASSWORD");
                    String HOST = (String) connObj.get("HOST_NAME");
                    String PORT = (String) connObj.get("CONN_PORT");
                    String SERVICE_NAME = (String) connObj.get("CONN_DB_NAME");

                    inputMap.add("tableName", request.getParameter("tableName"));
                    

                    inputMap.add("RESPONSE_ID", responseId);
                    inputMap.add("USER_NAME", USER_NAME);
                    inputMap.add("PASSWORD", PASSWORD);
                    inputMap.add("HOST", HOST);
                    inputMap.add("PORT", PORT);
                    inputMap.add("SERVICE_NAME", SERVICE_NAME);

                    //inputMap.add("colsArray", request.getParameter("colsArray"));
                    //inputMap.add("analysisType", "DHA");
                    //inputMap.add("accessName", "DR101413");
                    //inputMap.add("BATCH_ID", "B00000000001435");
                    HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<MultiValueMap<String, String>>(inputMap,
                            headers);
                    RestTemplate template = new RestTemplate();
                    long startTime = System.currentTimeMillis();
                    System.out.println("API called :: " + System.currentTimeMillis());
                    ResponseEntity<String> response = template.postForEntity("http://idxp.pilogcloud.com:6648/data_cleaning/", entity,
                            String.class);
                    String responseBody = response.getBody();

                    System.out.println("Response time :: " + (System.currentTimeMillis() - startTime) / 1000 + " Sec");
                } catch (Exception e) {
                    resultObject.put("result", "Error..." + e.getMessage());
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
            result = componentUtilities.getApiResponse(request, responseId, executor, 0);
            resultObject.put("result", result);
        } catch (Exception e) {
            resultObject.put("result", "Error..." + e.getMessage());
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

        return resultObject;
    }

    public JSONObject performVendorValidation(HttpServletRequest request) {
        JSONObject resultObject = new JSONObject();
        try {

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
            MultiValueMap<String, String> inputMap = new LinkedMultiValueMap<>();
            inputMap.add("tableName", request.getParameter("tableName"));
            inputMap.add("colsArray", "VENDOR_NO, NAME1, NAME2, STREET, ADDRESS_1, ADDRESS_2, ADDRESS_3, VENDOR_TYPE, CITY, COUNTRY, POSTAL_CODE, TELEPHONE_1, TELEPHONE_2, EMAIL_ID, NATIONAL_ID, GST_NUMBER, ACCOUNT_NUMBER, IFSC_CODE, ECC_NUMBER, CREATE_DATE, CREATE_BY, EDIT_DATE, EDIT_BY, AUDIT_ID, BATCH_ID, COMMENTS, PLUS_CODES, DUPLICATE_CHECK_COMMENTS, UPDATE_FLAG, VNDR_VALD_CUST_COL3, VNDR_VALD_CUST_COL4, VNDR_VALD_CUST_COL5");
            inputMap.add("analysisType", "VDV");
            //inputMap.add("accessName", "DR101413");
            inputMap.add("BATCH_ID", request.getParameter("batchId"));
//            inputMap.add("BATCH_ID", "B00000000001234");
            inputMap.add("accessName", request.getParameter("accessName"));

            String connObjStr = (String) request.getParameter("connObj");
            JSONObject connObj = (JSONObject) JSONValue.parse(connObjStr);
            String USER_NAME = (String) connObj.get("CONN_USER_NAME");
            String PASSWORD = (String) connObj.get("CONN_PASSWORD");
            String HOST = (String) connObj.get("HOST_NAME");
            String PORT = (String) connObj.get("CONN_PORT");
            String SERVICE_NAME = (String) connObj.get("CONN_DB_NAME");

            inputMap.add("USER_NAME", USER_NAME);
            inputMap.add("PASSWORD", PASSWORD);
            inputMap.add("HOST", HOST);
            inputMap.add("PORT", PORT);
            inputMap.add("SERVICE_NAME", SERVICE_NAME);

            HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<MultiValueMap<String, String>>(inputMap,
                    headers);
            RestTemplate template = new RestTemplate();
            ResponseEntity<String> response = template.postForEntity("http://apihub.pilogcloud.com:6650/VC_VALIDATION", entity,
                    String.class);
            String result = response.getBody();

            resultObject.put("result", result);
            // JSONObject apiDataObj = new JSONObject();
            // if (apiDataObj != null && !apiDataObj.isEmpty()) {
            // resultObject.put("result", apiDataObj.get("report"));
            //}

        } catch (Exception e) {
            e.printStackTrace();
            resultObject.put("result", "Error..." + e.getMessage());
        }
        return resultObject;
    }
    
    
    public JSONObject etlReferenceDataExtraction(HttpServletRequest request) {
        JSONObject resultObject = new JSONObject();
        String result = "";
        final int numberOfThreads = 1;
        final ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        final List<Future<?>> futures = new ArrayList<>();
        try {
            String responseId = AuditIdGenerator.genRandom32Hex();
            executor.submit(() -> {
                try {
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
                    MultiValueMap<String, String> inputMap = new LinkedMultiValueMap<>();

                    String connObjStr = (String) request.getParameter("connObj");
                    JSONObject connObj = (JSONObject) JSONValue.parse(connObjStr);
                    String USER_NAME = (String) connObj.get("CONN_USER_NAME");
                    String PASSWORD = (String) connObj.get("CONN_PASSWORD");
                    String HOST = (String) connObj.get("HOST_NAME");
                    String PORT = (String) connObj.get("CONN_PORT");
                    String SERVICE_NAME = (String) connObj.get("CONN_DB_NAME");

                    inputMap.add("tableName", request.getParameter("tableName"));
//                    inputMap.add("fun_call", request.getParameter("fun_call"));

                    inputMap.add("responseId", responseId);
                    inputMap.add("USER_NAME", USER_NAME);
                    inputMap.add("PASSWORD", PASSWORD);
                    inputMap.add("HOST", HOST);
                    inputMap.add("PORT", PORT);
                    inputMap.add("SERVICE_NAME", SERVICE_NAME);

                    inputMap.add("colsArray", request.getParameter("colsArray"));
                    inputMap.add("BATCH_ID", request.getParameter("batchId"));
                    
                    HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<MultiValueMap<String, String>>(inputMap,
                            headers);
                    RestTemplate template = new RestTemplate();
                    long startTime = System.currentTimeMillis();
                    System.out.println("API called :: " + System.currentTimeMillis());
                    System.out.println("responseId :: " + responseId);
                    ResponseEntity<String> response = template.postForEntity("http://apihub.pilogcloud.com:6660/ref", entity,
                            String.class);
                    String responseBody = response.getBody();

                    System.out.println("Response time :: " + (System.currentTimeMillis() - startTime) / 1000 + " Sec");
                } catch (Exception e) {
                    resultObject.put("result", "Error..." + e.getMessage());
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
            result = componentUtilities.getApiResponse(request, responseId, executor, 0);
            resultObject.put("result", result);
        } catch (Exception e) {
            resultObject.put("result", "Error..." + e.getMessage());
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

        return resultObject;
    }

    public JSONObject getTableColumnsList(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        List columnsList = new ArrayList();
        
        String logText = "";
        Connection connection = null;
        try {

            String tableName = request.getParameter("tableName");
            String connectionObjStr = request.getParameter("connObj");
            JSONObject connObj = (JSONObject) JSONValue.parse(connectionObjStr);
            connection = (Connection) componentUtilities.getConnection(connObj);
            List<Object[]> colsListObj = componentUtilities.getTableColumnsOpt(connection, connObj, tableName);
            columnsList = colsListObj.stream().map(rowdata -> rowdata[2]).collect(Collectors.toList());

            
            if (request.getParameter("maskedColumns")!=null && "Y".equalsIgnoreCase(request.getParameter("maskedColumns"))) {
                String ssUsername = (String) request.getSession(false).getAttribute("ssUsername");
            
                String hostName = (String) connObj.get("HOST_NAME");
                String port = (String) connObj.get("CONN_PORT");
                hostName = hostName + ":" + port;
                
                List maskedColumnsList = fetchMaskedColumnsList(request, tableName, hostName, ssUsername);
                resultObj.put("maskedColumnsList", maskedColumnsList);
            }
            if (request.getParameter("batchIds")!=null && "Y".equalsIgnoreCase(request.getParameter("batchIds"))) {
                List batchIds = fetchBatchIdsFromTable(request, tableName, connection);
                resultObj.put("batchIdsArray", batchIds);
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            
            try {
                if (connection!=null){
                connection.close();
                }
            } catch (Exception e) {
            }
            
        }

        resultObj.put("columnsList", columnsList);
        
        return resultObj;
    }
    
    public List fetchBatchIdsFromTable(HttpServletRequest request, String tableName,Connection connection) { 
        List batchIdsList =  new ArrayList();
        PreparedStatement pstmt = null;
        ResultSet resultset = null;
        try {
            String searchString = request.getParameter("searchString");
            String query = "SELECT DISTINCT BATCH_ID FROM "+tableName +" ";
            if (searchString!=null && !"".equalsIgnoreCase(searchString) && !"null".equalsIgnoreCase(searchString)) {
            query += " WHERE BATCH_ID LIKE '%"+searchString+"%'";
            }
            query += " OFFSET 0 ROWS FETCH NEXT 20 ROWS ONLY";
            pstmt = connection.prepareStatement(query);
            resultset = pstmt.executeQuery();
            while(resultset.next()){
                batchIdsList.add(resultset.getObject(1));
            }
            
        } catch (Exception e) {
            e.printStackTrace();
            
        } finally {
            try {
                if (resultset!=null){
                    resultset.close();
                }
                if (pstmt!=null){
                    pstmt.close();
                }
            } catch (Exception e){}
           
        }
        return batchIdsList;
    }

    public JSONObject updateMakedColumns(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        
        try {
            String tableName = request.getParameter("tableName");
            String ssUsername = (String) request.getSession(false).getAttribute("ssUsername");
            String colsArray = request.getParameter("colsArray");
            String connectionObjStr = request.getParameter("connObj");
            JSONObject connObj = (JSONObject) JSONValue.parse(connectionObjStr);
            String hostName = (String) connObj.get("HOST_NAME");
            String port = (String) connObj.get("CONN_PORT");
            hostName = hostName + ":" + port;

            List maskedColumnsList = fetchMaskedColumnsList(request, tableName, hostName, ssUsername);
            if (maskedColumnsList != null && !maskedColumnsList.isEmpty()) {
                String updateQuery = "UPDATE C_ETL_DATA_MASKING SET COLUMN_NAME = ?"
                        + " WHERE  USER_NAME =? AND TABLE_NAME=? AND CONNECTION_HOST=?";
                Map map = new HashedMap();
                map.put(1, colsArray);
                map.put(2, ssUsername);
                map.put(3, tableName);
                map.put(4, hostName);
                access.executeNativeUpdateSQLWithSimpleParamsNoAudit(updateQuery, map);
                resultObj.put("result", "Data Updated!");
            } else {
                String insertQuery = "INSERT INTO C_ETL_DATA_MASKING (USER_NAME, TABLE_NAME, COLUMN_NAME, CONNECTION_HOST) VALUES (?,?,?,?) ";
                Map map = new HashedMap();
                map.put(1, ssUsername);
                map.put(2, tableName);
                map.put(3, colsArray);
                map.put(4, hostName);
                access.executeNativeUpdateSQLWithSimpleParamsNoAudit(insertQuery, map);
                resultObj.put("result", "Data Saved !");
            }

        } catch (Exception e) {
            resultObj.put("result", "Error.. !");
            e.printStackTrace();
        }

        return resultObj;
    }

    @Transactional
    public List<String> fetchMaskedColumnsList(HttpServletRequest request, String tableName, String host, String ssUsername) {
        List<String> columnsList = new ArrayList<>();
        try {

            String selectQuery = "SELECT "
                    + " COLUMN_NAME "
                    + " FROM C_ETL_DATA_MASKING"
                    + " WHERE USER_NAME =:USER_NAME "
                    + " AND USER_NAME =:USER_NAME "
                    + " AND CONNECTION_HOST =:CONNECTION_HOST "
                    + " AND TABLE_NAME =:TABLE_NAME ";

            Map selectMap = new HashMap();
            selectMap.put("USER_NAME", ssUsername);
            selectMap.put("CONNECTION_HOST", host);
            selectMap.put("TABLE_NAME", tableName);

            columnsList = access.sqlqueryWithParams(selectQuery, selectMap);
            if (columnsList != null && !columnsList.isEmpty()) {
                columnsList = Arrays.asList(columnsList.get(0).split(","));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return columnsList;
    }
    
    public String insertDivIntoDB(HttpServletRequest request) {
        return iVisionTransformComponentsDAO.insertDivIntoDB();        
     }
    
    public JSONObject validateComponentTrfmRules(HttpServletRequest request) {

    	JSONObject columnsObject = new JSONObject();
        JSONArray sourceTablesArray = new JSONArray();
        JSONArray fromTableColsArray = new JSONArray();
        JSONArray toTableColsArray = new JSONArray();
        Connection fromConnection = null;
        Connection toConnection = null;
        JCO.Client fromJCOConnection = null;
        JCO.Client toJCOConnection = null;

        JSONObject fromConnectObj = new JSONObject();
        JSONObject toConnectObj = new JSONObject();
        List allFromcolumns = new ArrayList();
        //List dataTypesList = new ArrayList();
        try {
            String flowchartDataStr = request.getParameter("flowchartData");
            JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
            JSONObject operators = (JSONObject) flowchartData.get("operators");

            String sourceOperatorsStr = request.getParameter("sourceOperators");
//            JSONObject sourceOperatorsObj = (JSONObject) JSONValue.parse(sourceOperatorsStr);
            List<Map> fromOperatorsList = (List<Map>) JSONValue.parse(sourceOperatorsStr);

            JSONObject fromOpConnObj = new JSONObject();
            List simpleColumnsList = new ArrayList();

            List simpleTargetColumnsList = new ArrayList();
            

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
                        List<String> headers = componentUtilities.getHeadersOfImportedFile(request,
                                (String) fromConnectObj.get("filePath"));

                        headers = componentUtilities.fileHeaderValidations(headers);
                        simpleColumnsList.addAll(headers);
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
                                    columnObj.put("id", fromConnectObj.get("fileName") + ":" + headerName);
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
                                    allFromcolumns.add(headerName);
                                }
                            }
                        }
                        fromOpConnObj.put(fromConnectObj.get("fileName"), fromConnectObj);

                    } else {
                        fromOpConnObj.put(fromOperatorMap.get("tableName"), fromConnectObj);
                        JSONArray fromTableColsArrayForClause = new JSONArray();
                        Object fromConnObj = componentUtilities.getConnection(fromConnectObj);
                        if (fromConnObj instanceof Connection || fromConnObj instanceof JCO.Client) {
                            if (fromConnObj instanceof Connection) {
                                fromConnection = (Connection) fromConnObj;
                            } else if (fromConnObj instanceof JCO.Client) {
                                fromJCOConnection = (JCO.Client) fromConnObj;
                            }
                            List<Object[]> fromTableColumnList = new ArrayList<>();
                            String tableName = (String) fromOperatorMap.get("statusLabel");
                            String tableNameLabel = (String) fromOperatorMap.get("tableNameLabel");
                            boolean tableExist = false;
                            if (fromConnObj instanceof Connection) {
                                tableExist = checkTableExsist(fromConnection,
                                        (String) fromOperatorMap.get("statusLabel"));
                                if (tableExist) {
                                    fromTableColumnList = componentUtilities.getTreeDMTableColumnsOpt(fromConnection,
                                            request, fromConnectObj, (String) fromOperatorMap.get("statusLabel"));
                                } else {
                                }
                            } else if (fromConnObj instanceof JCO.Client) {
                                tableExist = true;
                                fromTableColumnList = componentUtilities.getSAPTableColumns(request, fromJCOConnection,
                                        (String) fromOperatorMap.get("statusLabel"));
                            }

                            sourceTablesArray.add(tableName);
                            fromOperatorMap.put("tableName", tableName);

                            if (tableName != null && !"".equalsIgnoreCase(tableName)) {
                                String treeObjTableName = "";
                                if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))) {
                                    treeObjTableName = tableName;
                                } else if (!tableExist) {
                                    treeObjTableName = tableNameLabel;
                                } else {
                                    treeObjTableName = fromConnectObj.get("CONN_USER_NAME") + "." + tableName;
                                }
                                JSONObject tableObj = new JSONObject();
                                tableObj.put("id", treeObjTableName);
                                tableObj.put("text", treeObjTableName);
                                tableObj.put("value", treeObjTableName);
                                tableObj.put("icon", "images/GridDB.svg");
                                fromTableColsArray.add(tableObj);
                                fromTableColsArrayForClause.add(tableObj);
                                List<String> columnsList = new ArrayList();
                                if (tableExist) {
                                    columnsList = fromTableColumnList.stream()
                                            .filter(tableColsArray -> (tableName
                                            .equalsIgnoreCase(String.valueOf(tableColsArray[0]))))
                                            .map(tableColsArray -> String.valueOf(tableColsArray[1]))
                                            .collect(Collectors.toList());
                                } else {
                                    columnsList = (List) fromOperatorMap.get("simpleColumnsList");
                                }

                                List<String> fromOperatorsColumnsList = getSourceColumnsList(fromOperatorMap);
                                if (fromOperatorsColumnsList != null && !fromOperatorsColumnsList.isEmpty()) {
                                    simpleColumnsList.addAll(fromOperatorsColumnsList);
                                } else {
                                    simpleColumnsList.addAll(columnsList);
                                }

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
                                        String treeObjColumnId = "";
                                        String treeObjColumnText = "";
                                        String treeObjColumnValue = "";
                                        String treeObjColumnParentId = "";
                                        if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))) {
                                            treeObjColumnId = tableName + ":" + columnsList.get(j);
                                            treeObjColumnText = columnsList.get(j);
                                            treeObjColumnValue = tableName + ":" + columnsList.get(j);
                                            treeObjColumnParentId = tableName;
                                        } else if (!tableExist) {
                                            treeObjColumnId = tableName + ":" + columnsList.get(j);
                                            treeObjColumnText = columnsList.get(j);
                                            treeObjColumnValue = tableNameLabel + ":" + columnsList.get(j);
                                            treeObjColumnParentId = tableName;
                                        } else {
                                            treeObjColumnId = fromConnectObj.get("CONN_USER_NAME") + "." + tableName
                                                    + ":" + columnsList.get(j);
                                            treeObjColumnText = columnsList.get(j);
                                            treeObjColumnValue = fromConnectObj.get("CONN_USER_NAME") + "." + tableName
                                                    + ":" + columnsList.get(j);
                                            treeObjColumnParentId = fromConnectObj.get("CONN_USER_NAME") + "."
                                                    + tableName;
                                        }
                                        JSONObject columnObj = new JSONObject();
                                        columnObj.put("id", treeObjColumnId);
                                        columnObj.put("text", treeObjColumnText);
                                        columnObj.put("value", treeObjColumnValue);
                                        columnObj.put("parentid", treeObjColumnParentId);
                                        if (dataTypeList != null && !dataTypeList.isEmpty()) {
                                            columnObj.put("dataType",
                                                    dataTypeList.get(j) != null ? dataTypeList.get(j) : ""); // RAVI DM

                                        }

                                        fromTableColsArray.add(columnObj);
                                        fromTableColsArrayForClause.add(columnObj);
                                        allFromcolumns.add(treeObjColumnText);
                                    }
                                }
                            }

                        } else {
                            columnsObject.put("connectionFlag", "N");
                            columnsObject.put("connectionMessage", fromConnObj);
                        }

                    } // end else for file type
                }

            } // end loop

            // destination table obj start
            String targetOperatorId = request.getParameter("selectedOperatorId");
            JSONObject targetOperator = (JSONObject) operators.get(targetOperatorId);

            if (targetOperator.get("iconType") != null && "MERGE".equalsIgnoreCase(String.valueOf(targetOperator.get("iconType")))) {
                JSONArray targetOperatorIds = processJobComponentsService.getConnectedToOperatorIds(request,
                        String.valueOf(targetOperator.get("operatorId")), flowchartData);
                targetOperatorId = String.valueOf(targetOperatorIds.get(0));
                targetOperator = (JSONObject) operators.get(targetOperatorId);
            }
            String toIconType = (String) targetOperator.get("iconType");
            String component = (String) targetOperator.get("component");

            if (targetOperator.get("statusLabel") == null) {
                targetOperator.put("statusLabel", targetOperator.get("tableName"));
            }
//            String destinationTableName = (String) targetOperator.get("statusLabel");
            String destinationTableName = (String) targetOperator.get("statusLabel");
            String destinationTableNameLabel = (String) targetOperator.get("tableNameLabel");
            if (destinationTableNameLabel == null) {
                destinationTableNameLabel = destinationTableNameLabel;
            }
//            toConnectObj = (JSONObject) targetOperator.get("connObj");
            toConnectObj = (JSONObject) targetOperator.get("connObj");

            if (toConnectObj != null) {
                List<Object[]> toTableColumnList = new ArrayList<>();
                Object toConnObj = componentUtilities.getConnection(toConnectObj);
                if (toConnObj instanceof Connection) {
                    toConnection = (Connection) toConnObj;
                } else if (toConnObj instanceof JCO.Client) {
                    toJCOConnection = (JCO.Client) toConnObj;
                }
                boolean tableExist = false;
                if (toConnObj instanceof Connection) {
                    tableExist = checkTableExsist(toConnection, (String) destinationTableName);
                    if (tableExist) {
                        toTableColumnList = componentUtilities.getTreeDMTableColumnsOpt(toConnection, request,
                                toConnectObj, destinationTableName);
                    } else {

                    }
                } else if (toConnObj instanceof JCO.Client) {
                    tableExist = true;
                    toTableColumnList = componentUtilities.getSAPTableColumns(request, toJCOConnection,
                            (String) destinationTableName);
                }

                if (destinationTableName != null && !"".equalsIgnoreCase(destinationTableName)) {
                    JSONObject tableObj = new JSONObject();
                    tableObj.put("id", destinationTableName);
                    tableObj.put("text", destinationTableNameLabel);
                    tableObj.put("value", destinationTableNameLabel);
                    tableObj.put("icon", "images/GridDB.svg");
                    toTableColsArray.add(tableObj);

                    List<String> columnsList = allFromcolumns;

                    for (int j = 0; j < columnsList.size(); j++) {
                        if (columnsList.get(j) != null && !"".equalsIgnoreCase(columnsList.get(j))) {
                            JSONObject columnObj = new JSONObject();
                            columnObj.put("id", destinationTableName + ":" + columnsList.get(j));
                            columnObj.put("text", columnsList.get(j));
                            columnObj.put("value", destinationTableNameLabel + ":" + columnsList.get(j));
                            columnObj.put("parentid", destinationTableName);

                            toTableColsArray.add(columnObj);
                            simpleTargetColumnsList.add(columnsList.get(j));
                        }

                    }
                }
            } else {
                destinationTableName = targetOperator.get("iconType") + "_" + targetOperator.get("operatorId");
                destinationTableNameLabel = targetOperator.get("iconType") + "_" + targetOperator.get("operatorId");
                if (destinationTableName != null && !"".equalsIgnoreCase(destinationTableName)) {
                    JSONObject tableObj = new JSONObject();
                    tableObj.put("id", destinationTableName);
                    tableObj.put("text", destinationTableNameLabel);
                    tableObj.put("value", destinationTableNameLabel);
                    tableObj.put("icon", "images/GridDB.svg");
                    toTableColsArray.add(tableObj);

                    List<String> columnsList = allFromcolumns;

                    for (int j = 0; j < columnsList.size(); j++) {
                        if (columnsList.get(j) != null && !"".equalsIgnoreCase(columnsList.get(j))) {
                            JSONObject columnObj = new JSONObject();
                            columnObj.put("id", destinationTableName + ":" + columnsList.get(j));
                            columnObj.put("text", columnsList.get(j));
                            columnObj.put("value", destinationTableNameLabel + ":" + columnsList.get(j));
                            columnObj.put("parentid", destinationTableName);

                            toTableColsArray.add(columnObj);
                            simpleTargetColumnsList.add(columnsList.get(j));
                        }

                    }
                }
            }
//            String sysType = (String) fromConnectObj.get("CONN_CUST_COL1");
//            dataTypesList = tableOperationsDAO.getListOfDataTypes(request, sysType.toUpperCase());

            // dest table obj end
            String[] validationTypes = {"LENGTH", "MANDATORY","COLUMNVALIDATE","UNIQUENESS", "RANGE", "DATATYPE"};
			String columnMappingStr = "<div id='colMappinAddIconDiv'>"
					+ "<img src=\"images/Add icon.svg\" data-mappedcolumns='' "
					+ "class=\"visionEtlColumnMapIcon\" title=\"Add New Column Map\""
					+ " onclick=addColumnMapping(event,this)" + " style=\"width:15px;height: 15px;cursor:pointer;\"/>"
					+ "<img src=\"images/Delete_Red_Icon.svg\" data-mappedcolumns='' "
					+ "class=\"visionEtlColumnMapIcon\" title=\"Delete All column mappings\""
					+ " onclick=deleteAllTableTrs('sourceDestColsTableId')"
					+ " style=\"width:15px;height: 15px;cursor:pointer;margin-left: 5px;\"/>" + "</div>" + ""
					+ "<div id='visionColMappScrollDiv' class='visionColMappScrollDiv1'>"
					+ "<table id=\"sourceDestColsTableId\" class=\"visionEtlJoinClauseTable visionEtlSourceDestColsTable1\" style='width: 100%;' border='1'>"
					+ "<thead>" + "<tr>"
					+ "<th width='1.5%' class=\"visionColMappingImgTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'></th>";

			columnMappingStr += "<th width='20%' class=\"mappedColsTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Source Column</th>"
					+ "<th width='20%' class=\"mappedColsTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Destination Table</th>"
					+ "<th width='20%' class=\"mappedColsTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Validation Type</th>"
					+ "<th width='20%' class=\"mappedColsTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Parameter</th>"
					+ "<th width='20%' class=\"mappedColsTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>TableColumnFilter</th>"
					+ "</tr>" + "</thead>" + "<tbody>";

//            columnMappingStr += "<tr style = 'height: 1px'>"
//                    + "<td width='1.5%' class=\"visionColMappingImgTd1\" ><img src=\"images/Delete_Red_Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionColMappingImg\""
//                    + " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>"
//                    + "</td>";
			String colMapSingleTrString = "";

			colMapSingleTrString += "<tr style = 'height: 1px'>"
					+ "<td width='1.5%' class=\"visionColMappingImgTd1\" ><img src=\"images/Delete_Red_Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionColMappingImg\""
					+ " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>" + "</td>";

			colMapSingleTrString += "<td width='20%'><input class='visionColMappingInput' type='text' value='' readonly='true'/>"
					+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
					+ " onclick=\"selectColumn(this,'fromColumn')\" style=\"\"></td>"
					+ "<td width='20%'><input class='visionColMappingInput' type='text' value='' connObj='' readonly='true'/>"
					+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
					+ " onclick=\"tableFilterDBConnection(this,'ALL_SCHEMA','Data Source')\" style=\"\"></td>"
					+ "<td width='20%'><select id='validationTypeId' class ='validationTypeTrfm'>";
			for (int k = 0; k < validationTypes.length; k++) {
				colMapSingleTrString += "<option value='" + validationTypes[k] + "'>" + validationTypes[k]
						+ "</option>";

			}
			colMapSingleTrString += "</select>";
//            for(int i = 0; i < dataTypesList.size(); i++) {
//            	colMapSingleTrString += "<option value='"+dataTypesList.get(i)+"'>"+dataTypesList.get(i)+"</option>";
//            }
            
            colMapSingleTrString += "</td>"
                    + "<td width='20%'><input class='visionColMappingInput' type='text' value='' />"
                    + "<img title='Select DataType' src=\"images/Change Datatype-Icon.png\" class=\"visionETLColMapImage \" "
					+ " onclick=\"selectDataType(this)\" style=\"\"></td>"
                    + "<td width='20%'><input class='visionColMappingInput' type='text' value='' readonly='true'/>"
                    + "<img title='Select Column' src=\"images/Filter-Iocn.png\" class=\"visionETLColMapImage \" "
                    + " onclick=\"tableFilterDBConnection(this,'ALL_SCHEMA','Data Source')\" style=\"\"></td>"
                    + "</tr>";

            JSONObject trfmRules = (JSONObject) targetOperator.get("trfmRules");
            if (trfmRules != null && !trfmRules.isEmpty()) {
                JSONArray columnMappingData = (JSONArray) trfmRules.get("colMappingsData");

                if (columnMappingData != null && !columnMappingData.isEmpty()) {

                    for (int i = 0; i < columnMappingData.size(); i++) {

                        JSONObject rowData = (JSONObject) columnMappingData.get(i);

                        String sourceColumn = (String) rowData.get("sourceColumn");
                        String sourceColumnActualValue = (String) rowData.get("sourceColumnActualValue");

                        String sourceTable = (rowData.get("sourceTable") != null)
                                ? rowData.get("sourceTable").toString()
                                : "";
                        String destinationColumn = (String) rowData.get("destinationColumn");
                        String destinationColumnActualValue = (String) rowData.get("destinationColumnActualValue");
                        String destinationConnObj = (String) rowData.get("destinationConnObj");
                        String validationType = (String) rowData.get("validationType");
                        String parameter = (String) rowData.get("parameter");
                        String tableColumnValidate = (String) rowData.get("tableColumnValidate");
                        String tableColumnValidateActualValue = (String) rowData.get("tableColumnValidateActualValue");
                        

                        columnMappingStr += "<tr style = 'height: 1px'>"
                                + "<td width='1.5%' class=\"visionColMappingImgTd1\" ><img src=\"images/Delete_Red_Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionColMappingImg\""
                                + " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>" + "</td>";

                        

						columnMappingStr += "<td width='20%'><input class='visionColMappingInput' type='text' tableName='"
								+ sourceTable + "' value='" + sourceColumn + "' actual-value='"
								+ sourceColumnActualValue + "' readonly='true'/>"
								+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
								+ " onclick=\"selectColumn(this,'fromColumn')\" style=\"\"></td>"
								+ "<td width='20%'><input class='visionColMappingInput' type='text'" + "value='"
								+ destinationColumn + "' actual-value='" + destinationColumnActualValue + "' connObj='"
								+ destinationConnObj + "' readonly='true'/>"
								+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
								+ " onclick=\"tableFilterDBConnection(this,'ALL_SCHEMA','Data Source')\" style=\"\"></td>"
								+ "<td width='20%'><select id='validationTypeId' class ='validationTypeTrfm'>"
								+ "<option value='" + validationType + "'>" + validationType + "</option>";
						for (int j = 0; j < validationTypes.length; j++) {
							if(!validationType.equalsIgnoreCase(validationTypes[j])) {
								columnMappingStr += "<option value='" + validationTypes[j] + "'>" + validationTypes[j] + "</option>";
							}

						}
//                        for(int j = 0; j < dataTypesList.size(); j++) {
//                        	columnMappingStr += "<option value='"+dataTypesList.get(j)+"'>"+dataTypesList.get(j)+"</option>";
//                        }
                        
                        columnMappingStr += "</select></td>"
                        		+ "<td width='20%'><input class='visionColMappingInput' type='text' value='" + parameter + "'>"
                        		+ "<img title='Select DataType' src=\"images/Change Datatype-Icon.png\" class=\"visionETLColMapImage \" "
            					+ " onclick=\"selectDataType(this)\" style=\"\"></td>"
                                + "<td width='20%'><input class='visionColMappingInput' type='text' value='"+tableColumnValidate+"'"
                                + "actual-value='"+tableColumnValidateActualValue+"' readonly='true'/>"
                                + "<img title='Select Column' src=\"images/Filter-Iocn.png\" class=\"visionETLColMapImage \" "
                                + " onclick=\"tableFilterDBConnection(this,'ALL_SCHEMA','Data Source')\" style=\"\"></td>"
                                + "</tr>";

                    }
                }
            } else {
                columnMappingStr += colMapSingleTrString;
            }

            columnsObject.put("selectedColumnStr", colMapSingleTrString);
            columnsObject.put("colMappingStr", columnMappingStr);
            columnsObject.put("toTableColsArray", toTableColsArray);
            columnsObject.put("fromTableColsArray", fromTableColsArray);
            columnsObject.put("sourceTablesArray", sourceTablesArray);
            columnsObject.put("fromOpConnObj", fromOpConnObj);
            columnsObject.put("simpleColumnsList", simpleColumnsList);
            columnsObject.put("simpleTargetColumnsList", simpleTargetColumnsList);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return columnsObject;
    }
    
    
    public JSONObject fetchComponentTrfmRules(HttpServletRequest request) {

    	JSONObject columnsObject = new JSONObject();
        JSONArray sourceTablesArray = new JSONArray();
        JSONArray fromTableColsArray = new JSONArray();
        JSONArray toTableColsArray = new JSONArray();
        Connection fromConnection = null;
        Connection toConnection = null;
        JCO.Client fromJCOConnection = null;
        JCO.Client toJCOConnection = null;

        JSONObject fromConnectObj = new JSONObject();
        JSONObject toConnectObj = new JSONObject();
        List allFromcolumns = new ArrayList();
        //List dataTypesList = new ArrayList();
        try {
            String flowchartDataStr = request.getParameter("flowchartData");
            JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
            JSONObject operators = (JSONObject) flowchartData.get("operators");

            String sourceOperatorsStr = request.getParameter("sourceOperators");
//            JSONObject sourceOperatorsObj = (JSONObject) JSONValue.parse(sourceOperatorsStr);
            List<Map> fromOperatorsList = (List<Map>) JSONValue.parse(sourceOperatorsStr);

            JSONObject fromOpConnObj = new JSONObject();
            List simpleColumnsList = new ArrayList();

            List simpleTargetColumnsList = new ArrayList();
            

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
                        List<String> headers = componentUtilities.getHeadersOfImportedFile(request,
                                (String) fromConnectObj.get("filePath"));

                        headers = componentUtilities.fileHeaderValidations(headers);
                        simpleColumnsList.addAll(headers);
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
                                    columnObj.put("id", fromConnectObj.get("fileName") + ":" + headerName);
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
                                    allFromcolumns.add(headerName);
                                }
                            }
                        }
                        fromOpConnObj.put(fromConnectObj.get("fileName"), fromConnectObj);

                    } else {
                        fromOpConnObj.put(fromOperatorMap.get("tableName"), fromConnectObj);
                        JSONArray fromTableColsArrayForClause = new JSONArray();
                        Object fromConnObj = componentUtilities.getConnection(fromConnectObj);
                        if (fromConnObj instanceof Connection || fromConnObj instanceof JCO.Client) {
                            if (fromConnObj instanceof Connection) {
                                fromConnection = (Connection) fromConnObj;
                            } else if (fromConnObj instanceof JCO.Client) {
                                fromJCOConnection = (JCO.Client) fromConnObj;
                            }
                            List<Object[]> fromTableColumnList = new ArrayList<>();
                            String tableName = (String) fromOperatorMap.get("statusLabel");
                            String tableNameLabel = (String) fromOperatorMap.get("tableNameLabel");
                            boolean tableExist = false;
                            if (fromConnObj instanceof Connection) {
                                tableExist = checkTableExsist(fromConnection,
                                        (String) fromOperatorMap.get("statusLabel"));
                                if (tableExist) {
                                    fromTableColumnList = componentUtilities.getTreeDMTableColumnsOpt(fromConnection,
                                            request, fromConnectObj, (String) fromOperatorMap.get("statusLabel"));
                                } else {
                                }
                            } else if (fromConnObj instanceof JCO.Client) {
                                tableExist = true;
                                fromTableColumnList = componentUtilities.getSAPTableColumns(request, fromJCOConnection,
                                        (String) fromOperatorMap.get("statusLabel"));
                            }

                            sourceTablesArray.add(tableName);
                            fromOperatorMap.put("tableName", tableName);

                            if (tableName != null && !"".equalsIgnoreCase(tableName)) {
                                String treeObjTableName = "";
                                if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))) {
                                    treeObjTableName = tableName;
                                } else if (!tableExist) {
                                    treeObjTableName = tableNameLabel;
                                } else {
                                    treeObjTableName = fromConnectObj.get("CONN_USER_NAME") + "." + tableName;
                                }
                                JSONObject tableObj = new JSONObject();
                                tableObj.put("id", treeObjTableName);
                                tableObj.put("text", treeObjTableName);
                                tableObj.put("value", treeObjTableName);
                                tableObj.put("icon", "images/GridDB.svg");
                                fromTableColsArray.add(tableObj);
                                fromTableColsArrayForClause.add(tableObj);
                                List<String> columnsList = new ArrayList();
                                if (tableExist) {
                                    columnsList = fromTableColumnList.stream()
                                            .filter(tableColsArray -> (tableName
                                            .equalsIgnoreCase(String.valueOf(tableColsArray[0]))))
                                            .map(tableColsArray -> String.valueOf(tableColsArray[1]))
                                            .collect(Collectors.toList());
                                } else {
                                    columnsList = (List) fromOperatorMap.get("simpleColumnsList");
                                }

                                List<String> fromOperatorsColumnsList = getSourceColumnsList(fromOperatorMap);
                                if (fromOperatorsColumnsList != null && !fromOperatorsColumnsList.isEmpty()) {
                                    simpleColumnsList.addAll(fromOperatorsColumnsList);
                                } else {
                                    simpleColumnsList.addAll(columnsList);
                                }

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
                                        String treeObjColumnId = "";
                                        String treeObjColumnText = "";
                                        String treeObjColumnValue = "";
                                        String treeObjColumnParentId = "";
                                        if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))) {
                                            treeObjColumnId = tableName + ":" + columnsList.get(j);
                                            treeObjColumnText = columnsList.get(j);
                                            treeObjColumnValue = tableName + ":" + columnsList.get(j);
                                            treeObjColumnParentId = tableName;
                                        } else if (!tableExist) {
                                            treeObjColumnId = tableName + ":" + columnsList.get(j);
                                            treeObjColumnText = columnsList.get(j);
                                            treeObjColumnValue = tableNameLabel + ":" + columnsList.get(j);
                                            treeObjColumnParentId = tableName;
                                        } else {
                                            treeObjColumnId = fromConnectObj.get("CONN_USER_NAME") + "." + tableName
                                                    + ":" + columnsList.get(j);
                                            treeObjColumnText = columnsList.get(j);
                                            treeObjColumnValue = fromConnectObj.get("CONN_USER_NAME") + "." + tableName
                                                    + ":" + columnsList.get(j);
                                            treeObjColumnParentId = fromConnectObj.get("CONN_USER_NAME") + "."
                                                    + tableName;
                                        }
                                        JSONObject columnObj = new JSONObject();
                                        columnObj.put("id", treeObjColumnId);
                                        columnObj.put("text", treeObjColumnText);
                                        columnObj.put("value", treeObjColumnValue);
                                        columnObj.put("parentid", treeObjColumnParentId);
                                        if (dataTypeList != null && !dataTypeList.isEmpty()) {
                                            columnObj.put("dataType",
                                                    dataTypeList.get(j) != null ? dataTypeList.get(j) : ""); // RAVI DM

                                        }

                                        fromTableColsArray.add(columnObj);
                                        fromTableColsArrayForClause.add(columnObj);
                                        allFromcolumns.add(treeObjColumnText);
                                    }
                                }
                            }

                        } else {
                            columnsObject.put("connectionFlag", "N");
                            columnsObject.put("connectionMessage", fromConnObj);
                        }

                    } // end else for file type
                }

            } // end loop

            // destination table obj start
            String targetOperatorId = request.getParameter("selectedOperatorId");
            JSONObject targetOperator = (JSONObject) operators.get(targetOperatorId);

            if (targetOperator.get("iconType") != null && "MERGE".equalsIgnoreCase(String.valueOf(targetOperator.get("iconType")))) {
                JSONArray targetOperatorIds = processJobComponentsService.getConnectedToOperatorIds(request,
                        String.valueOf(targetOperator.get("operatorId")), flowchartData);
                targetOperatorId = String.valueOf(targetOperatorIds.get(0));
                targetOperator = (JSONObject) operators.get(targetOperatorId);
            }
            String toIconType = (String) targetOperator.get("iconType");
            String component = (String) targetOperator.get("component");

            if (targetOperator.get("statusLabel") == null) {
                targetOperator.put("statusLabel", targetOperator.get("tableName"));
            }
//            String destinationTableName = (String) targetOperator.get("statusLabel");
            String destinationTableName = (String) targetOperator.get("statusLabel");
            String destinationTableNameLabel = (String) targetOperator.get("tableNameLabel");
            if (destinationTableNameLabel == null) {
                destinationTableNameLabel = destinationTableNameLabel;
            }
//            toConnectObj = (JSONObject) targetOperator.get("connObj");
            toConnectObj = (JSONObject) targetOperator.get("connObj");

            if (toConnectObj != null) {
                List<Object[]> toTableColumnList = new ArrayList<>();
                Object toConnObj = componentUtilities.getConnection(toConnectObj);
                if (toConnObj instanceof Connection) {
                    toConnection = (Connection) toConnObj;
                } else if (toConnObj instanceof JCO.Client) {
                    toJCOConnection = (JCO.Client) toConnObj;
                }
                boolean tableExist = false;
                if (toConnObj instanceof Connection) {
                    tableExist = checkTableExsist(toConnection, (String) destinationTableName);
                    if (tableExist) {
                        toTableColumnList = componentUtilities.getTreeDMTableColumnsOpt(toConnection, request,
                                toConnectObj, destinationTableName);
                    } else {

                    }
                } else if (toConnObj instanceof JCO.Client) {
                    tableExist = true;
                    toTableColumnList = componentUtilities.getSAPTableColumns(request, toJCOConnection,
                            (String) destinationTableName);
                }

                if (destinationTableName != null && !"".equalsIgnoreCase(destinationTableName)) {
                    JSONObject tableObj = new JSONObject();
                    tableObj.put("id", destinationTableName);
                    tableObj.put("text", destinationTableNameLabel);
                    tableObj.put("value", destinationTableNameLabel);
                    tableObj.put("icon", "images/GridDB.svg");
                    toTableColsArray.add(tableObj);

                    List<String> columnsList = allFromcolumns;

                    for (int j = 0; j < columnsList.size(); j++) {
                        if (columnsList.get(j) != null && !"".equalsIgnoreCase(columnsList.get(j))) {
                            JSONObject columnObj = new JSONObject();
                            columnObj.put("id", destinationTableName + ":" + columnsList.get(j));
                            columnObj.put("text", columnsList.get(j));
                            columnObj.put("value", destinationTableNameLabel + ":" + columnsList.get(j));
                            columnObj.put("parentid", destinationTableName);

                            toTableColsArray.add(columnObj);
                            simpleTargetColumnsList.add(columnsList.get(j));
                        }

                    }
                }
            } else {
                destinationTableName = targetOperator.get("iconType") + "_" + targetOperator.get("operatorId");
                destinationTableNameLabel = targetOperator.get("iconType") + "_" + targetOperator.get("operatorId");
                if (destinationTableName != null && !"".equalsIgnoreCase(destinationTableName)) {
                    JSONObject tableObj = new JSONObject();
                    tableObj.put("id", destinationTableName);
                    tableObj.put("text", destinationTableNameLabel);
                    tableObj.put("value", destinationTableNameLabel);
                    tableObj.put("icon", "images/GridDB.svg");
                    toTableColsArray.add(tableObj);

                    List<String> columnsList = allFromcolumns;

//                    for (int j = 0; j < columnsList.size(); j++) {
//                        if (columnsList.get(j) != null && !"".equalsIgnoreCase(columnsList.get(j))) {
//                            JSONObject columnObj = new JSONObject();
//                            columnObj.put("id", destinationTableName + ":" + columnsList.get(j));
//                            columnObj.put("text", columnsList.get(j));
//                            columnObj.put("value", destinationTableNameLabel + ":" + columnsList.get(j));
//                            columnObj.put("parentid", destinationTableName);
//
//                            toTableColsArray.add(columnObj);
//                            simpleTargetColumnsList.add(columnsList.get(j));
//                        }
//
//                    }
                }
            }
//            String sysType = (String) fromConnectObj.get("CONN_CUST_COL1");
//            dataTypesList = tableOperationsDAO.getListOfDataTypes(request, sysType.toUpperCase());

            // dest table obj end
            String columnMappingStr = "<div id='colMappinAddIconDiv'>"
                    + "<img src=\"images/Add icon.svg\" data-mappedcolumns='' "
                    + "class=\"visionEtlColumnMapIcon\" title=\"Add New Column Map\""
                    + " onclick=addColumnMapping(event,this)" + " style=\"width:15px;height: 15px;cursor:pointer;\"/>"
                    + "<img src=\"images/Delete_Red_Icon.svg\" data-mappedcolumns='' "
                    + "class=\"visionEtlColumnMapIcon\" title=\"Delete All column mappings\""
                    + " onclick=deleteAllTableTrs('sourceDestColsTableId')"
                    + " style=\"width:15px;height: 15px;cursor:pointer;margin-left: 5px;\"/>"
                    + "</div>" + "" + "<div id='visionColMappScrollDiv' class='visionColMappScrollDiv1'>"
                    + "<table id=\"sourceDestColsTableId\" class=\"visionEtlJoinClauseTable visionEtlSourceDestColsTable1\" style='width: 100%;' border='1'>"
                    + "<thead>" + "<tr>"
                    + "<th width='1.5%' class=\"visionColMappingImgTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'></th>";
                    

            
            columnMappingStr += "<th width='5%' class=\"mappedColsTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Source Column</th>"
            		+ "<th width='20%' class=\"mappedColsTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Compare Column</th>"
                    + "<th width='20%' class=\"mappedColsTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>TableColumnFilter</th>"
                    + "</tr>" + "</thead>" + "<tbody>";

//            columnMappingStr += "<tr style = 'height: 1px'>"
//                    + "<td width='1.5%' class=\"visionColMappingImgTd1\" ><img src=\"images/Delete_Red_Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionColMappingImg\""
//                    + " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>"
//                    + "</td>";
            String colMapSingleTrString = "";

            colMapSingleTrString += "<tr style = 'height: 1px'>"
                    + "<td width='1.5%' class=\"visionColMappingImgTd1\" ><img src=\"images/Delete_Red_Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionColMappingImg\""
                    + " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>" + "</td>";

           

            colMapSingleTrString += "<td width='5%'><div id='fetchColumnsComboBox'></div></td>";
//            for(int i = 0; i < dataTypesList.size(); i++) {
//            	colMapSingleTrString += "<option value='"+dataTypesList.get(i)+"'>"+dataTypesList.get(i)+"</option>";
//            }
            
            colMapSingleTrString += "<td width='20%'><input class='visionColMappingInput' type='text' value='' connObj='' readonly='true'/>"
            		+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                    + " onclick=\"selectColumn(this,'fromColumn')\" style=\"\"></td>"
            		+ "<td width='20%'><input class='visionColMappingInput' type='text' value='' readonly='true'/>"
                    + "<img title='Select Column' src=\"images/Filter-Iocn.png\" class=\"visionETLColMapImage \" "
                    + " onclick=\"tableFilterDBConnection(this,'ALL_SCHEMA','Data Source')\" style=\"\"></td>"
                    + "</tr>";

            JSONObject trfmRules = (JSONObject) targetOperator.get("trfmRules");
            if (trfmRules != null && !trfmRules.isEmpty()) {
                JSONArray columnMappingData = (JSONArray) trfmRules.get("colMappingsData");

                if (columnMappingData != null && !columnMappingData.isEmpty()) {

                    for (int i = 0; i < columnMappingData.size(); i++) {

                        JSONObject rowData = (JSONObject) columnMappingData.get(i);
                        String compareColumn = (String) rowData.get("compareColumn");
                        String compareColumnActualValue = (String) rowData.get("compareColumnActualValue");
                        String tableColumnValidate = (String) rowData.get("tableColumnValidate");
                        String tableColumnValidateActualValue = (String) rowData.get("tableColumnValidateActualValue");

                        columnMappingStr += "<tr style = 'height: 1px'>"
                                + "<td width='1.5%' class=\"visionColMappingImgTd1\" ><img src=\"images/Delete_Red_Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionColMappingImg\""
                                + " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>" + "</td>";

                        

                        columnMappingStr += "<td width='5%'><div id='fetchColumnsComboBox'></div></td>"
                        		+ "<td width='20%'><input class='visionColMappingInput' type='text'"
                                + "value='" + compareColumn + "' actual-value='"
                                + compareColumnActualValue + "' readonly='true'/>"
                                + "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
                                + " onclick=\"selectColumn(this,'fromColumn')\" style=\"\"></td>"
                                + "<td width='20%'><input class='visionColMappingInput' type='text' value='"+tableColumnValidate+"'"
                                + "actual-value='"+tableColumnValidateActualValue+"' readonly='true'/>"
                                + "<img title='Select Column' src=\"images/Filter-Iocn.png\" class=\"visionETLColMapImage \" "
                                + " onclick=\"tableFilterDBConnection(this,'ALL_SCHEMA','Data Source')\" style=\"\"></td>"
                                + "</tr>";

                    }
                }
            } else {
                columnMappingStr += colMapSingleTrString;
            }

            columnsObject.put("selectedColumnStr", colMapSingleTrString);
            columnsObject.put("colMappingStr", columnMappingStr);
            columnsObject.put("toTableColsArray", toTableColsArray);
            columnsObject.put("fromTableColsArray", fromTableColsArray);
            columnsObject.put("sourceTablesArray", sourceTablesArray);
            columnsObject.put("fromOpConnObj", fromOpConnObj);
            columnsObject.put("simpleColumnsList", simpleColumnsList);
            columnsObject.put("simpleTargetColumnsList", simpleTargetColumnsList);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return columnsObject;
    }
    
    public JSONObject sapHanaLoadTrfmRules(HttpServletRequest request) {

    	JSONObject columnsObject = new JSONObject();
        JSONArray sourceTablesArray = new JSONArray();
        JSONArray fromTableColsArray = new JSONArray();
        JSONArray toTableColsArray = new JSONArray();
        Connection fromConnection = null;
        Connection toConnection = null;
        JCO.Client fromJCOConnection = null;
        JCO.Client toJCOConnection = null;

        JSONObject fromConnectObj = new JSONObject();
        JSONObject toConnectObj = new JSONObject();
        List allFromcolumns = new ArrayList();
        //List dataTypesList = new ArrayList();
        try {
            String flowchartDataStr = request.getParameter("flowchartData");
            JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
            JSONObject operators = (JSONObject) flowchartData.get("operators");

            String sourceOperatorsStr = request.getParameter("sourceOperators");
//            JSONObject sourceOperatorsObj = (JSONObject) JSONValue.parse(sourceOperatorsStr);
            List<Map> fromOperatorsList = (List<Map>) JSONValue.parse(sourceOperatorsStr);

            JSONObject fromOpConnObj = new JSONObject();
            List simpleColumnsList = new ArrayList();

            List simpleTargetColumnsList = new ArrayList();
            

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
                        List<String> headers = componentUtilities.getHeadersOfImportedFile(request,
                                (String) fromConnectObj.get("filePath"));

                        headers = componentUtilities.fileHeaderValidations(headers);
                        simpleColumnsList.addAll(headers);
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
                                    columnObj.put("id", fromConnectObj.get("fileName") + ":" + headerName);
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
                                    allFromcolumns.add(headerName);
                                }
                            }
                        }
                        fromOpConnObj.put(fromConnectObj.get("fileName"), fromConnectObj);

                    } else {
                        fromOpConnObj.put(fromOperatorMap.get("tableName"), fromConnectObj);
                        JSONArray fromTableColsArrayForClause = new JSONArray();
                        Object fromConnObj = componentUtilities.getConnection(fromConnectObj);
                        if (fromConnObj instanceof Connection || fromConnObj instanceof JCO.Client) {
                            if (fromConnObj instanceof Connection) {
                                fromConnection = (Connection) fromConnObj;
                            } else if (fromConnObj instanceof JCO.Client) {
                                fromJCOConnection = (JCO.Client) fromConnObj;
                            }
                            List<Object[]> fromTableColumnList = new ArrayList<>();
                            String tableName = (String) fromOperatorMap.get("statusLabel");
                            String tableNameLabel = (String) fromOperatorMap.get("tableNameLabel");
                            boolean tableExist = false;
                            if (fromConnObj instanceof Connection) {
                                tableExist = checkTableExsist(fromConnection,
                                        (String) fromOperatorMap.get("statusLabel"));
                                if (tableExist) {
                                    fromTableColumnList = componentUtilities.getTreeDMTableColumnsOpt(fromConnection,
                                            request, fromConnectObj, (String) fromOperatorMap.get("statusLabel"));
                                } else {
                                }
                            } else if (fromConnObj instanceof JCO.Client) {
                                tableExist = true;
                                fromTableColumnList = componentUtilities.getSAPTableColumns(request, fromJCOConnection,
                                        (String) fromOperatorMap.get("statusLabel"));
                            }

                            sourceTablesArray.add(tableName);
                            fromOperatorMap.put("tableName", tableName);

                            if (tableName != null && !"".equalsIgnoreCase(tableName)) {
                                String treeObjTableName = "";
                                if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))) {
                                    treeObjTableName = tableName;
                                } else if (!tableExist) {
                                    treeObjTableName = tableNameLabel;
                                } else {
                                    treeObjTableName = fromConnectObj.get("CONN_USER_NAME") + "." + tableName;
                                }
                                JSONObject tableObj = new JSONObject();
                                tableObj.put("id", treeObjTableName);
                                tableObj.put("text", treeObjTableName);
                                tableObj.put("value", treeObjTableName);
                                tableObj.put("icon", "images/GridDB.svg");
                                fromTableColsArray.add(tableObj);
                                fromTableColsArrayForClause.add(tableObj);
                                List<String> columnsList = new ArrayList();
                                if (tableExist) {
                                    columnsList = fromTableColumnList.stream()
                                            .filter(tableColsArray -> (tableName
                                            .equalsIgnoreCase(String.valueOf(tableColsArray[0]))))
                                            .map(tableColsArray -> String.valueOf(tableColsArray[1]))
                                            .collect(Collectors.toList());
                                } else {
                                    columnsList = (List) fromOperatorMap.get("simpleColumnsList");
                                }

                                List<String> fromOperatorsColumnsList = getSourceColumnsList(fromOperatorMap);
                                if (fromOperatorsColumnsList != null && !fromOperatorsColumnsList.isEmpty()) {
                                    simpleColumnsList.addAll(fromOperatorsColumnsList);
                                } else {
                                    simpleColumnsList.addAll(columnsList);
                                }

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
                                        String treeObjColumnId = "";
                                        String treeObjColumnText = "";
                                        String treeObjColumnValue = "";
                                        String treeObjColumnParentId = "";
                                        if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnectObj.get("CONN_CUST_COL1")))) {
                                            treeObjColumnId = tableName + ":" + columnsList.get(j);
                                            treeObjColumnText = columnsList.get(j);
                                            treeObjColumnValue = tableName + ":" + columnsList.get(j);
                                            treeObjColumnParentId = tableName;
                                        } else if (!tableExist) {
                                            treeObjColumnId = tableName + ":" + columnsList.get(j);
                                            treeObjColumnText = columnsList.get(j);
                                            treeObjColumnValue = tableNameLabel + ":" + columnsList.get(j);
                                            treeObjColumnParentId = tableName;
                                        } else {
                                            treeObjColumnId = fromConnectObj.get("CONN_USER_NAME") + "." + tableName
                                                    + ":" + columnsList.get(j);
                                            treeObjColumnText = columnsList.get(j);
                                            treeObjColumnValue = fromConnectObj.get("CONN_USER_NAME") + "." + tableName
                                                    + ":" + columnsList.get(j);
                                            treeObjColumnParentId = fromConnectObj.get("CONN_USER_NAME") + "."
                                                    + tableName;
                                        }
                                        JSONObject columnObj = new JSONObject();
                                        columnObj.put("id", treeObjColumnId);
                                        columnObj.put("text", treeObjColumnText);
                                        columnObj.put("value", treeObjColumnValue);
                                        columnObj.put("parentid", treeObjColumnParentId);
                                        if (dataTypeList != null && !dataTypeList.isEmpty()) {
                                            columnObj.put("dataType",
                                                    dataTypeList.get(j) != null ? dataTypeList.get(j) : ""); // RAVI DM

                                        }

                                        fromTableColsArray.add(columnObj);
                                        fromTableColsArrayForClause.add(columnObj);
                                        allFromcolumns.add(treeObjColumnText);
                                    }
                                }
                            }

                        } else {
                            columnsObject.put("connectionFlag", "N");
                            columnsObject.put("connectionMessage", fromConnObj);
                        }

                    } // end else for file type
                }

            } // end loop

            // destination table obj start
            String targetOperatorId = request.getParameter("selectedOperatorId");
            JSONObject targetOperator = (JSONObject) operators.get(targetOperatorId);

            if (targetOperator.get("iconType") != null && "MERGE".equalsIgnoreCase(String.valueOf(targetOperator.get("iconType")))) {
                JSONArray targetOperatorIds = processJobComponentsService.getConnectedToOperatorIds(request,
                        String.valueOf(targetOperator.get("operatorId")), flowchartData);
                targetOperatorId = String.valueOf(targetOperatorIds.get(0));
                targetOperator = (JSONObject) operators.get(targetOperatorId);
            }
            String toIconType = (String) targetOperator.get("iconType");
            String component = (String) targetOperator.get("component");

            if (targetOperator.get("statusLabel") == null) {
                targetOperator.put("statusLabel", targetOperator.get("tableName"));
            }
//            String destinationTableName = (String) targetOperator.get("statusLabel");
            String destinationTableName = (String) targetOperator.get("statusLabel");
            String destinationTableNameLabel = (String) targetOperator.get("tableNameLabel");
            if (destinationTableNameLabel == null) {
                destinationTableNameLabel = destinationTableNameLabel;
            }
//            toConnectObj = (JSONObject) targetOperator.get("connObj");
            toConnectObj = (JSONObject) targetOperator.get("connObj");

            if (toConnectObj != null) {
                List<Object[]> toTableColumnList = new ArrayList<>();
                Object toConnObj = componentUtilities.getConnection(toConnectObj);
                if (toConnObj instanceof Connection) {
                    toConnection = (Connection) toConnObj;
                } else if (toConnObj instanceof JCO.Client) {
                    toJCOConnection = (JCO.Client) toConnObj;
                }
                boolean tableExist = false;
                if (toConnObj instanceof Connection) {
                    tableExist = checkTableExsist(toConnection, (String) destinationTableName);
                    if (tableExist) {
                        toTableColumnList = componentUtilities.getTreeDMTableColumnsOpt(toConnection, request,
                                toConnectObj, destinationTableName);
                    } else {

                    }
                } else if (toConnObj instanceof JCO.Client) {
                    tableExist = true;
                    toTableColumnList = componentUtilities.getSAPTableColumns(request, toJCOConnection,
                            (String) destinationTableName);
                }

                if (destinationTableName != null && !"".equalsIgnoreCase(destinationTableName)) {
                    JSONObject tableObj = new JSONObject();
                    tableObj.put("id", destinationTableName);
                    tableObj.put("text", destinationTableNameLabel);
                    tableObj.put("value", destinationTableNameLabel);
                    tableObj.put("icon", "images/GridDB.svg");
                    toTableColsArray.add(tableObj);

                    List<String> columnsList = allFromcolumns;

                    for (int j = 0; j < columnsList.size(); j++) {
                        if (columnsList.get(j) != null && !"".equalsIgnoreCase(columnsList.get(j))) {
                            JSONObject columnObj = new JSONObject();
                            columnObj.put("id", destinationTableName + ":" + columnsList.get(j));
                            columnObj.put("text", columnsList.get(j));
                            columnObj.put("value", destinationTableNameLabel + ":" + columnsList.get(j));
                            columnObj.put("parentid", destinationTableName);

                            toTableColsArray.add(columnObj);
                            simpleTargetColumnsList.add(columnsList.get(j));
                        }

                    }
                }
            } else {
                destinationTableName = targetOperator.get("iconType") + "_" + targetOperator.get("operatorId");
                destinationTableNameLabel = targetOperator.get("iconType") + "_" + targetOperator.get("operatorId");
                if (destinationTableName != null && !"".equalsIgnoreCase(destinationTableName)) {
                    JSONObject tableObj = new JSONObject();
                    tableObj.put("id", destinationTableName);
                    tableObj.put("text", destinationTableNameLabel);
                    tableObj.put("value", destinationTableNameLabel);
                    tableObj.put("icon", "images/GridDB.svg");
                    toTableColsArray.add(tableObj);

                    List<String> columnsList = allFromcolumns;

                    for (int j = 0; j < columnsList.size(); j++) {
                        if (columnsList.get(j) != null && !"".equalsIgnoreCase(columnsList.get(j))) {
                            JSONObject columnObj = new JSONObject();
                            columnObj.put("id", destinationTableName + ":" + columnsList.get(j));
                            columnObj.put("text", columnsList.get(j));
                            columnObj.put("value", destinationTableNameLabel + ":" + columnsList.get(j));
                            columnObj.put("parentid", destinationTableName);

                            toTableColsArray.add(columnObj);
                            simpleTargetColumnsList.add(columnsList.get(j));
                        }

                    }
                }
            }
//            String sysType = (String) fromConnectObj.get("CONN_CUST_COL1");
//            dataTypesList = tableOperationsDAO.getListOfDataTypes(request, sysType.toUpperCase());

            // dest table obj end
            String[] validationTypes = {"LENGTH", "MANDATORY","COLUMNVALIDATE","UNIQUENESS", "RANGE", "DATATYPE"};
			String columnMappingStr = "<div id='colMappinAddIconDiv'>"
					+ "<img src=\"images/Add icon.svg\" data-mappedcolumns='' "
					+ "class=\"visionEtlColumnMapIcon\" title=\"Add New Column Map\""
					+ " onclick=addColumnMapping(event,this)" + " style=\"width:15px;height: 15px;cursor:pointer;\"/>"
					+ "<img src=\"images/Delete_Red_Icon.svg\" data-mappedcolumns='' "
					+ "class=\"visionEtlColumnMapIcon\" title=\"Delete All column mappings\""
					+ " onclick=deleteAllTableTrs('sourceDestColsTableId')"
					+ " style=\"width:15px;height: 15px;cursor:pointer;margin-left: 5px;\"/>" + "</div>" + ""
					+ "<div id='visionColMappScrollDiv' class='visionColMappScrollDiv1'>"
					+ "<table id=\"sourceDestColsTableId\" class=\"visionEtlJoinClauseTable visionEtlSourceDestColsTable1\" style='width: 100%;' border='1'>"
					+ "<thead>" + "<tr>"
					+ "<th width='1.5%' class=\"visionColMappingImgTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'></th>";

			columnMappingStr += "<th width='20%' class=\"mappedColsTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Source Column</th>"
					+ "<th width='20%' class=\"mappedColsTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Destination Table</th>"
					+ "<th width='20%' class=\"mappedColsTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Validation Type</th>"
					+ "<th width='20%' class=\"mappedColsTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Parameter</th>"
					+ "<th width='20%' class=\"mappedColsTh1\" style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>TableColumnFilter</th>"
					+ "</tr>" + "</thead>" + "<tbody>";

//            columnMappingStr += "<tr style = 'height: 1px'>"
//                    + "<td width='1.5%' class=\"visionColMappingImgTd1\" ><img src=\"images/Delete_Red_Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionColMappingImg\""
//                    + " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>"
//                    + "</td>";
			String colMapSingleTrString = "";

			colMapSingleTrString += "<tr style = 'height: 1px'>"
					+ "<td width='1.5%' class=\"visionColMappingImgTd1\" ><img src=\"images/Delete_Red_Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionColMappingImg\""
					+ " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>" + "</td>";

			colMapSingleTrString += "<td width='20%'><input class='visionColMappingInput' type='text' value='' readonly='true'/>"
					+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
					+ " onclick=\"selectColumn(this,'fromColumn')\" style=\"\"></td>"
					+ "<td width='20%'><input class='visionColMappingInput' type='text' value='' connObj='' readonly='true'/>"
					+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
					+ " onclick=\"tableFilterDBConnection(this,'ALL_SCHEMA','Data Source')\" style=\"\"></td>"
					+ "<td width='20%'><select id='validationTypeId' class ='validationTypeTrfm'>";
			for (int k = 0; k < validationTypes.length; k++) {
				colMapSingleTrString += "<option value='" + validationTypes[k] + "'>" + validationTypes[k]
						+ "</option>";

			}
			colMapSingleTrString += "</select>";
//            for(int i = 0; i < dataTypesList.size(); i++) {
//            	colMapSingleTrString += "<option value='"+dataTypesList.get(i)+"'>"+dataTypesList.get(i)+"</option>";
//            }
            
            colMapSingleTrString += "</td>"
                    + "<td width='20%'><input class='visionColMappingInput' type='text' value='' />"
                    + "<img title='Select DataType' src=\"images/Change Datatype-Icon.png\" class=\"visionETLColMapImage \" "
					+ " onclick=\"selectDataType(this)\" style=\"\"></td>"
                    + "<td width='20%'><input class='visionColMappingInput' type='text' value='' readonly='true'/>"
                    + "<img title='Select Column' src=\"images/Filter-Iocn.png\" class=\"visionETLColMapImage \" "
                    + " onclick=\"tableFilterDBConnection(this,'ALL_SCHEMA','Data Source')\" style=\"\"></td>"
                    + "</tr>";

            JSONObject trfmRules = (JSONObject) targetOperator.get("trfmRules");
            if (trfmRules != null && !trfmRules.isEmpty()) {
                JSONArray columnMappingData = (JSONArray) trfmRules.get("colMappingsData");

                if (columnMappingData != null && !columnMappingData.isEmpty()) {

                    for (int i = 0; i < columnMappingData.size(); i++) {

                        JSONObject rowData = (JSONObject) columnMappingData.get(i);

                        String sourceColumn = (String) rowData.get("sourceColumn");
                        String sourceColumnActualValue = (String) rowData.get("sourceColumnActualValue");

                        String sourceTable = (rowData.get("sourceTable") != null)
                                ? rowData.get("sourceTable").toString()
                                : "";
                        String destinationColumn = (String) rowData.get("destinationColumn");
                        String destinationColumnActualValue = (String) rowData.get("destinationColumnActualValue");
                        String destinationConnObj = (String) rowData.get("destinationConnObj");
                        String validationType = (String) rowData.get("validationType");
                        String parameter = (String) rowData.get("parameter");
                        String tableColumnValidate = (String) rowData.get("tableColumnValidate");
                        String tableColumnValidateActualValue = (String) rowData.get("tableColumnValidateActualValue");
                        

                        columnMappingStr += "<tr style = 'height: 1px'>"
                                + "<td width='1.5%' class=\"visionColMappingImgTd1\" ><img src=\"images/Delete_Red_Icon.svg\" onclick='deleteSelectedRow(this)'  class=\"visionColMappingImg\""
                                + " title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"/>" + "</td>";

                        

						columnMappingStr += "<td width='20%'><input class='visionColMappingInput' type='text' tableName='"
								+ sourceTable + "' value='" + sourceColumn + "' actual-value='"
								+ sourceColumnActualValue + "' readonly='true'/>"
								+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
								+ " onclick=\"selectColumn(this,'fromColumn')\" style=\"\"></td>"
								+ "<td width='20%'><input class='visionColMappingInput' type='text'" + "value='"
								+ destinationColumn + "' actual-value='" + destinationColumnActualValue + "' connObj='"
								+ destinationConnObj + "' readonly='true'/>"
								+ "<img title='Select Column' src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \" "
								+ " onclick=\"tableFilterDBConnection(this,'ALL_SCHEMA','Data Source')\" style=\"\"></td>"
								+ "<td width='20%'><select id='validationTypeId' class ='validationTypeTrfm'>"
								+ "<option value='" + validationType + "'>" + validationType + "</option>";
						for (int j = 0; j < validationTypes.length; j++) {
							if(!validationType.equalsIgnoreCase(validationTypes[j])) {
								columnMappingStr += "<option value='" + validationTypes[j] + "'>" + validationTypes[j] + "</option>";
							}

						}
//                        for(int j = 0; j < dataTypesList.size(); j++) {
//                        	columnMappingStr += "<option value='"+dataTypesList.get(j)+"'>"+dataTypesList.get(j)+"</option>";
//                        }
                        
                        columnMappingStr += "</select></td>"
                        		+ "<td width='20%'><input class='visionColMappingInput' type='text' value='" + parameter + "'>"
                        		+ "<img title='Select DataType' src=\"images/Change Datatype-Icon.png\" class=\"visionETLColMapImage \" "
            					+ " onclick=\"selectDataType(this)\" style=\"\"></td>"
                                + "<td width='20%'><input class='visionColMappingInput' type='text' value='"+tableColumnValidate+"'"
                                + "actual-value='"+tableColumnValidateActualValue+"' readonly='true'/>"
                                + "<img title='Select Column' src=\"images/Filter-Iocn.png\" class=\"visionETLColMapImage \" "
                                + " onclick=\"tableFilterDBConnection(this,'ALL_SCHEMA','Data Source')\" style=\"\"></td>"
                                + "</tr>";

                    }
                }
            } else {
                columnMappingStr += colMapSingleTrString;
            }

            columnsObject.put("selectedColumnStr", colMapSingleTrString);
            columnsObject.put("colMappingStr", columnMappingStr);
            columnsObject.put("toTableColsArray", toTableColsArray);
            columnsObject.put("fromTableColsArray", fromTableColsArray);
            columnsObject.put("sourceTablesArray", sourceTablesArray);
            columnsObject.put("fromOpConnObj", fromOpConnObj);
            columnsObject.put("simpleColumnsList", simpleColumnsList);
            columnsObject.put("simpleTargetColumnsList", simpleTargetColumnsList);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return columnsObject;
    }
    

}
