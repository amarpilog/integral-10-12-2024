/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.DAO;


import com.pilog.mdm.access.DataAccess;
import com.pilog.mdm.utilities.PilogUtilities;
import com.pilog.mdm.pojo.DalDlov;
import com.sap.mw.jco.JCO;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.persistence.PersistenceException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import oracle.sql.RAW;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;
/**
 *
 * @author Ravindar
 */
@Repository
public class V10GenericDataPipingDAO {

    private V10DataMigrationAccessDAO dataMigrationAccessDAO = new V10DataMigrationAccessDAO();
    private PilogUtilities visionUtills = new PilogUtilities();

    @Autowired
    private DataAccess access;

    @Value("${jdbc.driver}")
    private String dataBaseDriver;
    @Value("${jdbc.username}")
    private String userName;
    @Value("${jdbc.password}")
    private String password;
    @Value("${jdbc.url}")
    private String dbURL;

    public List getTreeOracleSelectedColumnsData(Connection connection, JSONObject tableColsObj, Integer start, Integer limit, JSONObject tablesObj,
            String serviceName, JSONArray sourceTablesArr, JSONArray joinArray) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        String result = "";
        List totalData = new ArrayList();
        String whereCond = "";
        try {
            if (sourceTablesArr != null && !sourceTablesArr.isEmpty() && sourceTablesArr.size() > 1) {
                String colsObj = "";
                String colsObjStr = "";
                int k = 0;
                if (tableColsObj != null && !tableColsObj.isEmpty()) {
                    for (Object tableName : tableColsObj.keySet()) {
                        if (tableName != null && tableColsObj.get(tableName) != null) {
                            String colsObject = (String) tableColsObj.get(tableName);
                            String[] colsArr = colsObject.split(",");
                            String colsNameStr = "";
                            String colsStr = "";
                            for (int i = 0; i < colsArr.length; i++) {
                                colsNameStr += tableName + "_Alias" + "." + colsArr[i];
                                colsStr += colsArr[i];
                                if (i != colsArr.length - 1) {
                                    colsNameStr += ",";
                                    colsStr += ",";
                                }
                            }
                            if (colsNameStr != null && !"".equalsIgnoreCase(colsNameStr) && !"null".equalsIgnoreCase(colsNameStr)) {
                                if (k == 0) {
                                    colsObj += colsNameStr;
                                    colsObjStr += colsStr;
                                } else {
                                    colsObj += "," + colsNameStr;
                                    colsObjStr += "," + colsStr;
                                }
                            }
                        }
                        if (tablesObj != null && !tablesObj.isEmpty()) {
                            String tableInputVal = (String) tablesObj.get(tableName);
                            if (tableInputVal != null && !"".equalsIgnoreCase(tableInputVal) && !"null".equalsIgnoreCase(tableInputVal)) {
                                if (whereCond != null && !"".equalsIgnoreCase(whereCond) && !"null".equalsIgnoreCase(whereCond)) {
                                    whereCond += " AND " + tableInputVal;
                                } else {
                                    whereCond += " WHERE " + tableInputVal;
                                }
                            }
                        }
                        k++;
                    }
                }
                if (colsObj != null && !colsObj.isEmpty()) {
                    String query = "SELECT ROWNUM AS RNO," + colsObj;
                    List<String> columnsList = new ArrayList(Arrays.asList(colsObj.split(",")));
                    List<String> columnsListObj = new ArrayList(Arrays.asList(colsObjStr.split(",")));

                    query += " FROM ";
                    for (int i = 0; i < joinArray.size(); i++) {
                        JSONObject joinObj = (JSONObject) joinArray.get(i);
                        String fromJoinTable = (String) joinObj.get("fromJoinTable");
                        String join = (String) joinObj.get("join");
                        String joinCond = (String) joinObj.get("joinCondition");
                        String toJoinTable = (String) joinObj.get("toJoinTable");
                        if (i == 0) {
                            query += fromJoinTable + " " + fromJoinTable + "_Alias " + join + " " + toJoinTable + " " + toJoinTable + "_Alias " + " ON ";
                        } else {
                            query += " " + join + " " + toJoinTable + " " + toJoinTable + "_Alias " + " ON ";
                        }
                        if (joinCond != null && !"".equalsIgnoreCase(joinCond) && !"".equalsIgnoreCase(joinCond)) {
                            query += " " + joinCond;
                        }

                    }
                    if (whereCond != null && !"".equalsIgnoreCase(whereCond) && !"null".equalsIgnoreCase(whereCond)) {
                        query += " " + whereCond;
                    }
                    query = "SELECT * FROM (" + query + ") WHERE RNO BETWEEN " + start + " AND " + (start + limit - 1) + "";
                    System.out.println("query::::" + query);
                    preparedStatement = connection.prepareStatement(query);
                    resultSet = preparedStatement.executeQuery();
                    while (resultSet.next()) {
                        Map dataObj = new HashMap();
                        if (columnsList != null && !columnsList.isEmpty()) {
                            for (int i = 0; i < columnsList.size(); i++) {
                                String colsValue = columnsList.get(i).replace(".", ":");
                                if (colsValue != null && !"".equalsIgnoreCase(colsValue) && !"".equalsIgnoreCase(colsValue)) {
                                    colsValue = colsValue.replace("_Alias", "");
                                }
                                dataObj.put(colsValue, resultSet.getString(columnsListObj.get(i)));
                            }
                            totalData.add(dataObj);
                        }

                    }

                }

            } else {
                if (tableColsObj != null && !tableColsObj.isEmpty()) {
                    for (Object tableName : tableColsObj.keySet()) {
                        if (tableName != null && tableColsObj.get(tableName) != null) {
                            String colsObj = (String) tableColsObj.get(tableName);
                            if (colsObj != null && !colsObj.isEmpty()) {
                                String query = "SELECT ROWNUM AS RNO," + colsObj;
                                List<String> columnsList = new ArrayList(Arrays.asList(colsObj.split(",")));
                                if (tablesObj != null && !tablesObj.isEmpty()) {
                                    String tableInputVal = (String) tablesObj.get(tableName);
                                    query += " FROM " + tableName + " ";
                                    if (tableInputVal != null && !"".equalsIgnoreCase(tableInputVal) && !"null".equalsIgnoreCase(tableInputVal)) {
                                        query += " WHERE " + tableInputVal + "";
                                    }
                                }
                                query = "SELECT * FROM (" + query + ") WHERE RNO BETWEEN " + start + " AND " + (start + limit - 1) + "";
                                System.out.println("query::::" + query);
                                preparedStatement = connection.prepareStatement(query);
                                resultSet = preparedStatement.executeQuery();
                                while (resultSet.next()) {
                                    Map dataObj = new HashMap();
                                    if (columnsList != null && !columnsList.isEmpty()) {
                                        for (int i = 0; i < columnsList.size(); i++) {
                                            dataObj.put(tableName + ":" + columnsList.get(i), resultSet.getString(columnsList.get(i)));
                                        }
                                        totalData.add(dataObj);
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
                if (resultSet != null) {
                    resultSet.close();
                }
                if (preparedStatement != null) {
                    preparedStatement.close();
                }

            } catch (Exception e) {
            }
        }
        return totalData;
    }

    public List<Object[]> getTreeOracleTableColumns(Connection connection, HttpServletRequest request, String serviceName, String tableName) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        List<Object[]> sourceColumnsList = new ArrayList<>();
        try {
            if (tableName != null && !"".equalsIgnoreCase(tableName) && !"null".equalsIgnoreCase(tableName)) {
                System.out.println("tableName:::" + tableName);
                String query = "SELECT DISTINCT TABLE_NAME,COLUMN_NAME FROM USER_TAB_COLUMNS WHERE "
                        + " TABLE_NAME IN ('" + tableName + "') ORDER BY TABLE_NAME";
                preparedStatement = connection.prepareStatement(query);
                resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    Object[] columnsArray = new Object[2];
                    columnsArray[0] = (resultSet.getString("TABLE_NAME") != null ? resultSet.getString("TABLE_NAME").toUpperCase() : "");
                    columnsArray[1] = resultSet.getString("COLUMN_NAME");
                    sourceColumnsList.add(columnsArray);
                }
            }
//            System.out.println("columnsArray::::" + sourceColumnsList);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
            }
        }
        return sourceColumnsList;
    }

//    @Transactional
//    public int saveJobs(HttpServletRequest request,
//            Map mappingOperatorObj,
//            Map operatorsMap,
//            Map linksMap, String jobId, String trfmRulesId, String jobName) {
//        int insertCount = 0;
//        try {
//            String insertJobQuery = "INSERT INTO C_DM_JOBS(ORGN_ID, SEQUENCE_NO, JOB_ID, JOB_DESCR, USER_NAME, TRFM_RULES_ID,CREATE_BY, EDIT_BY)"
//                    + " VALUES(?,?,?,?,?,?,?,?)";
//            Map<Integer, Object> insertMap = new HashMap<>();
//            insertMap.put(1, request.getSession(false).getAttribute("ssOrgId"));
//            insertMap.put(2, 10);
//            insertMap.put(3, jobId);
//            insertMap.put(4, jobName);
//            insertMap.put(5, request.getSession(false).getAttribute("ssUsername"));
//            insertMap.put(6, trfmRulesId);
//            insertMap.put(7, request.getSession(false).getAttribute("ssUsername"));
//            insertMap.put(8, request.getSession(false).getAttribute("ssUsername"));
//            insertCount = access.executeNativeUpdateSQLWithSimpleParamsNoAudit(insertJobQuery, insertMap);
//            System.out.println("insertCount:::" + insertCount);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return insertCount;
//    }
    @Transactional
    public int saveTrfmRules(HttpServletRequest request, Map mappingOperatorObj,
            Map operatorsMap, Map linksMap, String jobId, String trfmRulesId) {
        int insertCount = 0;
        try {
//            String insertJobQuery = "INSERT INTO C_DM_JOBS(ORGN_ID, SEQUENCE_NO, JOB_ID, JOB_DESCR, USER_NAME, TRFM_RULES_ID,CREATE_BY, EDIT_BY)"
//                    + " VALUES(?,?,?,?,?,?,?,?)";
//            Map<Integer, Object> insertMap = new HashMap<>();
//            insertMap.put(1, request.getSession(false).getAttribute("ssOrgId"));
//            insertMap.put(2, 10);
//            insertMap.put(3, jobId);
//            insertMap.put(4, jobName);
//            insertMap.put(5, request.getSession(false).getAttribute("ssUsername"));
//            insertMap.put(6, trfmRulesId);
//            insertMap.put(7, request.getSession(false).getAttribute("ssUsername"));
//            insertMap.put(8, request.getSession(false).getAttribute("ssUsername"));
//            insertCount = access.executeNativeUpdateSQLWithSimpleParamsNoAudit(insertJobQuery, insertMap);
//            System.out.println("insertCount:::" + insertCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return insertCount;
    }

    @Transactional
    public List<Object[]> getDBFunctions(HttpServletRequest request, String hlFunId) {
        List<Object[]> dbFunctionsArray = new ArrayList<>();
        try {
            String selectQuery = "SELECT  "
                    + " SEQUENCE_NO,"//0
                    + "         FUN_ID,"//1
                    + "         FUN_NAME,"//2
                    + "         FUN_DISP_NAME,"//3
                    + "         FUN_DESCR,"//4
                    + "         HL_FUN_ID,"//5
                    + "         FUN_PARAMS_COUNT,"//6
                    + "         FUN_DB_TYPE,"//7
                    + "         FUN_TYPE,"//8
                    + "         FUN_LVL_TYPE,"//9
                    + "         ACTIVE_FLAG,"//10
                    + "         ICON_PATH,"//11
                    + "         DM_FUN_CUST_COL1,"//12
                    + "         DM_FUN_CUST_COL2,"//13
                    + "         DM_FUN_CUST_COL3,"//14
                    + "         DM_FUN_CUST_COL4,"//15
                    + "         DM_FUN_CUST_COL5,"//16
                    + "         DM_FUN_CUST_COL6,"//17
                    + "         DM_FUN_CUST_COL7,"//18
                    + "         DM_FUN_CUST_COL8,"//19
                    + "         DM_FUN_CUST_COL9,"//20
                    + "         DM_FUN_CUST_COL10,"//21
                    + "  FUN_FORM_ID"//22
                    + "  FROM   B_DM_FUNCTIONS WHERE ACTIVE_FLAG ='Y' ";
            if (hlFunId != null && !"".equalsIgnoreCase(hlFunId) && !"null".equalsIgnoreCase(hlFunId)) {
                selectQuery += " AND HL_FUN_ID = '" + hlFunId + "' ";
            } else {
                selectQuery += " AND HL_FUN_ID IS NULL ";
            }
            selectQuery += " ORDER BY SEQUENCE_NO";
            System.out.println("selectQuery::::" + selectQuery);
            dbFunctionsArray = access.sqlqueryWithParams(selectQuery, Collections.EMPTY_MAP);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dbFunctionsArray;
    }

    @Transactional
    public List<Object[]> getFunFormList(HttpServletRequest request, JSONObject selectedFunRowData) {
        List<Object[]> funFormList = new ArrayList<>();
        try {
            String selectQuery = "SELECT   SEQUENCE_NO,"//0
                    + "         FUN_FORM_ID,"//1
                    + "         FUN_ID,"//2
                    + "         FUN_COL_LBL,"//3
                    + "         FUN_COL_LBL_ID,"//4
                    + "         FUN_COL_TYPE,"//5
                    + "         ACTIVE_FLAG,"//6
                    + "         DM_FUN_FRM_CUST_COL1,"//7
                    + "         DM_FUN_FRM_CUST_COL2,"//8
                    + "         DM_FUN_FRM_CUST_COL3,"//9
                    + "         DM_FUN_FRM_CUST_COL4,"//10
                    + "         DM_FUN_FRM_CUST_COL5,"//11
                    + "         DM_FUN_FRM_CUST_COL6,"//12
                    + "         DM_FUN_FRM_CUST_COL7,"//13
                    + "         DM_FUN_FRM_CUST_COL8,"//14
                    + "         DM_FUN_FRM_CUST_COL9,"//15
                    + "         DM_FUN_FRM_CUST_COL10"//16
                    + "  FROM   B_DM_FUNCTIONS_FORM "
                    + " WHERE FUN_FORM_ID =:FUN_FORM_ID AND FUN_ID =:FUN_ID ORDER BY SEQUENCE_NO ";
            Map<String, Object> selectMap = new HashMap<>();
            selectMap.put("FUN_FORM_ID", selectedFunRowData.get("FUN_FORM_ID"));
            selectMap.put("FUN_ID", selectedFunRowData.get("FUN_ID"));
            System.out.println("selectQuery::" + selectQuery);
            System.out.println("selectMap::" + selectMap);
            funFormList = access.sqlqueryWithParams(selectQuery, selectMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return funFormList;
    }

    @Transactional
    public JSONArray getTreeSapListOfTable(HttpServletRequest request, JCO.Client connection, String filterTableVal) {
        dataMigrationAccessDAO.retrieveRepository(connection);
        JCO.Table theConnection = null;
        String tableDataStr = "";
        JCO.Table countTableData = null;
        JCO.Table fetchTableData = null;
        JCO.Table countFields = null;
        JCO.Table dataFields = null;
        JCO.Function countFunction = null;
        JCO.Function dataFunction = null;
        JSONArray treeDataObjArray = new JSONArray();
        try {
            if (filterTableVal != null && !"".equalsIgnoreCase(filterTableVal) && !"null".equalsIgnoreCase(filterTableVal)) {
                filterTableVal = filterTableVal.toUpperCase();
            }
            long startTreeIndex = 0;
            long endTreeIndex = 0;
            long count = 0;
            String tabClass = "";
            String startIndex = request.getParameter("startIndex");
            String endIndex = request.getParameter("endIndex");
            String levelStr = request.getParameter("level");
            String columnsObjStr = request.getParameter("columnsObj");
            String filterTableCond = request.getParameter("filterCondition");
            String parentKeyData = request.getParameter("parentkey");
            String showMoreIcon = "";
            if (startIndex != null && !"".equalsIgnoreCase(startIndex) && !"null".equalsIgnoreCase(startIndex)
                    && endIndex != null && !"".equalsIgnoreCase(endIndex) && !"null".equalsIgnoreCase(endIndex)) {
                startTreeIndex = Integer.parseInt(startIndex);
                endTreeIndex = Integer.parseInt(endIndex);
                if (startTreeIndex > 0) {
                    startTreeIndex = startTreeIndex - 1;
                }
            }
            if (columnsObjStr != null && !"".equalsIgnoreCase(columnsObjStr)) {
                JSONObject treeColumnsObj = (JSONObject) JSONValue.parse(columnsObjStr);
                if (treeColumnsObj != null && !treeColumnsObj.isEmpty()) {
                    JSONObject columnsObj = (JSONObject) treeColumnsObj.get(levelStr);
                    if (columnsObj != null && !columnsObj.isEmpty()) {
                        JSONObject treeInitParams = (JSONObject) columnsObj.get("TREE_INIT_PARAMS");
                        if (treeInitParams != null && !treeInitParams.isEmpty()) {
                            showMoreIcon = (String) treeInitParams.get("uuu_treeEtlShowMoreIcon");
                        }
                    }
                }
            }
            if (filterTableCond != null && !"".equalsIgnoreCase(filterTableCond) && "!=".equalsIgnoreCase(filterTableCond)) {
                filterTableCond = "<>";
            }
            if (parentKeyData != null && !"".equalsIgnoreCase(parentKeyData) && !"".equalsIgnoreCase(parentKeyData)) {
                if ("TABLES".equalsIgnoreCase(parentKeyData)) {
                    tabClass = " TABCLASS EQ 'TRANSP' ";
                } else if ("VIEWS".equalsIgnoreCase(parentKeyData)) {
                    tabClass = " TABCLASS EQ 'VIEW' ";
                }
                countFunction = dataMigrationAccessDAO.getFunction("RFC_READ_TABLE");
                dataFunction = dataMigrationAccessDAO.getFunction("RFC_READ_TABLE");
                if (filterTableVal != null && !filterTableVal.isEmpty()) {
                    JCO.ParameterList countListParams = countFunction.getImportParameterList();
                    countListParams.setValue("DD02L", "QUERY_TABLE");//For MARA
                    countListParams.setValue("|", "DELIMITER");
                    countTableData = countFunction.getTableParameterList().getTable("DATA");
                    countFields = countFunction.getTableParameterList().getTable("FIELDS");
                    JCO.Table countOption = countFunction.getTableParameterList().getTable("OPTIONS");
                    countFields.appendRow();
                    countFields.setValue("TABNAME", "FIELDNAME");
                    countOption.appendRow();
//                    countOption.setValue("TABNAME " + filterTableCond + " '" + filterTableVal + "' AND (" + tabClass + ")", "TEXT");
                    countOption.setValue("(" + tabClass + ")", "TEXT");
                    countOption.setValue("TABNAME " + filterTableCond + " '" + filterTableVal + "'", "TEXT");
//                  
                    connection.execute(countFunction);
                    count = countTableData.getNumRows();

                    JCO.ParameterList dataListParams = dataFunction.getImportParameterList();
                    dataListParams.setValue("DD02L", "QUERY_TABLE");//For MARA
                    dataListParams.setValue("|", "DELIMITER");
                    fetchTableData = dataFunction.getTableParameterList().getTable("DATA");
                    dataFields = dataFunction.getTableParameterList().getTable("FIELDS");
                    JCO.Table fetchOptions = dataFunction.getTableParameterList().getTable("OPTIONS");
                    dataFields.appendRow();
                    dataFields.setValue("TABNAME", "FIELDNAME");
                    fetchOptions.appendRow();
                    fetchOptions.setValue("(" + tabClass + ")", "TEXT");
                    fetchOptions.setValue("TABNAME " + filterTableCond + " '" + filterTableVal + "'", "TEXT");
//                    fetchOptions.setValue(" ", "TEXT");
                    dataListParams.setValue(startTreeIndex, "ROWSKIPS");
                    dataListParams.setValue((endTreeIndex - startTreeIndex), "ROWCOUNT");
                    connection.execute(dataFunction);

                } else {
                    JCO.ParameterList countListParams = countFunction.getImportParameterList();
                    countListParams.setValue("DD02L", "QUERY_TABLE");//For MARA
                    countListParams.setValue("|", "DELIMITER");
                    countTableData = countFunction.getTableParameterList().getTable("DATA");
                    countFields = countFunction.getTableParameterList().getTable("FIELDS");
                    JCO.Table countOption = countFunction.getTableParameterList().getTable("OPTIONS");
                    countFields.appendRow();
                    countFields.setValue("TABNAME", "FIELDNAME");
                    countOption.appendRow();
                    countOption.setValue("(" + tabClass + ")", "TEXT");
//                     countOption.setValue("TABNAME NOT LIKE '/%'", "TEXT");
                    connection.execute(countFunction);
                    count = countTableData.getNumRows();

                    JCO.ParameterList dataListParams = dataFunction.getImportParameterList();
                    dataListParams.setValue("DD02L", "QUERY_TABLE");//For MARA
                    dataListParams.setValue("|", "DELIMITER");
                    dataListParams.setValue(startTreeIndex, "ROWSKIPS");
                    dataListParams.setValue((endTreeIndex - startTreeIndex), "ROWCOUNT");
                    fetchTableData = dataFunction.getTableParameterList().getTable("DATA");
                    dataFields = dataFunction.getTableParameterList().getTable("FIELDS");
                    JCO.Table dataOption = dataFunction.getTableParameterList().getTable("OPTIONS");
                    dataFields.appendRow();
                    dataFields.setValue("TABNAME", "FIELDNAME");
                    dataOption.appendRow();
                    dataOption.setValue("(" + tabClass + ")", "TEXT");
//                    dataOption.setValue("TABNAME NOT LIKE '/%'", "TEXT"); // RAVI SAP FILTER / TABLES
                    connection.execute(dataFunction);
                }
                if (fetchTableData != null) {
                    JSONObject treeObj = new JSONObject();
                    treeObj.put("label", (fetchTableData.getString("WA") != null ? fetchTableData.getString("WA").toUpperCase() : ""));
                    treeObj.put("description", (fetchTableData.getString("WA") != null ? fetchTableData.getString("WA").toUpperCase() : ""));
                    JSONArray childArray = new JSONArray();
                    JSONObject dummyObj = new JSONObject();
                    dummyObj.put("value", "ajax");
                    dummyObj.put("label", (fetchTableData.getString("WA") != null ? fetchTableData.getString("WA").toUpperCase() : ""));
                    childArray.add(dummyObj);
                    treeObj.put("items", childArray);
                    treeObj.put("value", (fetchTableData.getString("WA") != null ? fetchTableData.getString("WA").toUpperCase() : ""));
                    treeDataObjArray.add(treeObj);
                }
                while (fetchTableData.nextRow()) {
                    JSONObject treeObj = new JSONObject();
                    treeObj.put("label", (fetchTableData.getString("WA") != null ? fetchTableData.getString("WA").toUpperCase() : ""));
                    treeObj.put("description", (fetchTableData.getString("WA") != null ? fetchTableData.getString("WA").toUpperCase() : ""));
                    JSONArray childArray = new JSONArray();
                    JSONObject dummyObj = new JSONObject();
                    dummyObj.put("value", "ajax");
                    dummyObj.put("label", (fetchTableData.getString("WA") != null ? fetchTableData.getString("WA").toUpperCase() : ""));
                    childArray.add(dummyObj);
                    treeObj.put("items", childArray);
                    treeObj.put("value", (fetchTableData.getString("WA") != null ? fetchTableData.getString("WA").toUpperCase() : ""));
                    treeDataObjArray.add(treeObj);
                }
                if (levelStr != null && !"".equalsIgnoreCase(levelStr) && "4".equalsIgnoreCase(levelStr)) {
                    if (count > endTreeIndex) {
                        JSONObject treeObj = new JSONObject();
                        treeObj.put("label", "Show More...");
                        treeObj.put("description", "Show More...");
                        JSONArray childArray = new JSONArray();
                        JSONObject dummyObj = new JSONObject();
                        dummyObj.put("value", "ajax");
                        dummyObj.put("label", "Show More...");
                        childArray.add(dummyObj);
                        treeObj.put("items", childArray);
                        treeObj.put("value", "Show More");
                        treeObj.put("icon", showMoreIcon);
                        treeDataObjArray.add(treeObj);
                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return treeDataObjArray;
    }

    @Transactional
    public JSONArray getTreeSapListOfTableColumns(HttpServletRequest request, JCO.Client connection, String tableName) {
        JCO.Function function = null;
        JSONArray treeDataObjArr = new JSONArray();
        JCO.Table fields = null;
        try {
            function = dataMigrationAccessDAO.getFunction("DDIF_FIELDINFO_GET");
            if (function != null) {
                JCO.ParameterList listParams = function.getImportParameterList();
                listParams.setValue(tableName, "TABNAME");//For MARA
                fields = function.getTableParameterList().getTable("DFIES_TAB");
                connection.execute(function);

                if (fields != null) {
                    JSONObject treeObj = new JSONObject();
                    treeObj.put("label", (fields.getString("FIELDNAME") != null ? fields.getString("FIELDNAME").toUpperCase() : ""));
                    treeObj.put("description", (fields.getString("FIELDNAME") != null ? fields.getString("FIELDNAME").toUpperCase() : ""));
                    JSONArray childArray = new JSONArray();
                    JSONObject dummyObj = new JSONObject();
                    dummyObj.put("value", "ajax");
                    dummyObj.put("label", (fields.getString("FIELDNAME") != null ? fields.getString("FIELDNAME").toUpperCase() : ""));
                    childArray.add(dummyObj);
                    treeObj.put("items", childArray);
                    treeObj.put("value", (fields.getString("FIELDNAME") != null ? fields.getString("FIELDNAME").toUpperCase() : ""));
                    treeDataObjArr.add(treeObj);
                }
                while (fields.nextRow()) {
                    JSONObject treeObj = new JSONObject();

                    treeObj.put("label", (fields.getString("FIELDNAME") != null ? fields.getString("FIELDNAME").toUpperCase() : ""));
                    treeObj.put("description", (fields.getString("FIELDNAME") != null ? fields.getString("FIELDNAME").toUpperCase() : ""));
                    JSONArray childArray = new JSONArray();
                    JSONObject dummyObj = new JSONObject();
                    dummyObj.put("value", "ajax");
                    dummyObj.put("label", (fields.getString("FIELDNAME") != null ? fields.getString("FIELDNAME").toUpperCase() : ""));
                    childArray.add(dummyObj);
                    treeObj.put("items", childArray);
                    treeObj.put("value", (fields.getString("FIELDNAME") != null ? fields.getString("FIELDNAME").toUpperCase() : ""));
                    treeDataObjArr.add(treeObj);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return treeDataObjArr;
    }

    @Transactional
    public JSONObject createTableInETL(HttpServletRequest request, JSONObject createTableObj) {
        JSONObject resultObj = new JSONObject();
        try {
            if (createTableObj != null && !createTableObj.isEmpty()) {
                String tableName = (String) createTableObj.get("tableName");
                JSONArray columnsObjArray = (JSONArray) createTableObj.get("colsObj");
                if (tableName != null && !"".equalsIgnoreCase(tableName) && !"null".equalsIgnoreCase(tableName)) {
                    if (columnsObjArray != null && !columnsObjArray.isEmpty()) {
                        String query = "";
                        List<String> pkColumns = new ArrayList<>();
                        for (int i = 0; i < columnsObjArray.size(); i++) {
                            JSONObject columnObj = (JSONObject) columnsObjArray.get(i);
                            if (columnObj != null
                                    && !columnObj.isEmpty()
                                    && columnObj.get("COLUMN_NAME") != null
                                    && !"".equalsIgnoreCase(String.valueOf(columnObj.get("COLUMN_NAME")))
                                    && !"null".equalsIgnoreCase(String.valueOf(columnObj.get("COLUMN_NAME")))
                                    && columnObj.get("DATA_TYPE") != null
                                    && !"".equalsIgnoreCase(String.valueOf(columnObj.get("DATA_TYPE")))
                                    && !"null".equalsIgnoreCase(String.valueOf(columnObj.get("DATA_TYPE")))) {
                                query += " " + columnObj.get("COLUMN_NAME") + " " + columnObj.get("DATA_TYPE");
                                if ("Y".equalsIgnoreCase(String.valueOf(columnObj.get("PK")))) {
                                    pkColumns.add(String.valueOf(columnObj.get("COLUMN_NAME")));
                                }
                                if (i != columnsObjArray.size() - 1) {
                                    query += " , ";
                                }
                            }
                        }
                        if (query != null
                                && !"".equalsIgnoreCase(query)
                                && !"null".equalsIgnoreCase(query)) {
                            query = "CREATE TABLE " + tableName.toUpperCase() + " (" + query + ")\n"
                                    + "";
                            System.out.println("Create table Query:::" + query);
                            int createCount = access.executeUpdateSQLNoAudit(query, Collections.EMPTY_MAP);
                            System.out.println("createTableCount:::" + createCount);
                            try {
                                if (pkColumns != null && !pkColumns.isEmpty()) {
                                    String pkQuery = "ALTER TABLE " + tableName.toUpperCase() + " ADD (\n"
                                            + "  CONSTRAINT " + tableName.toUpperCase() + "_PK\n"
                                            + " PRIMARY KEY\n"
                                            + " (" + StringUtils.collectionToCommaDelimitedString(pkColumns) + "))";
                                    System.out.println("pkQuery:::" + pkQuery);
                                    int createPKCount = access.executeUpdateSQLNoAudit(pkQuery, Collections.EMPTY_MAP);
                                    System.out.println("createPKCount:::" + createPKCount);
                                }
                            } catch (Exception e) {
                            }

                            resultObj.put("message", "Table created successfully.");
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
        } catch (PersistenceException e) {
            System.err.println("" + e.getCause().getLocalizedMessage());
            System.err.println("" + e.getLocalizedMessage());
            System.err.println("" + e.fillInStackTrace().getMessage());
            System.err.println("" + e.getMessage());
            resultObj.put("message", e.getMessage());
            resultObj.put("messageFlag", false);
        } catch (Exception e) {
            System.out.println("" + e.getClass().getName());
            System.err.println("" + e.getCause().getClass().getName());
            System.err.println("" + e.getLocalizedMessage());
            System.err.println("" + e.fillInStackTrace().getMessage());
            System.err.println("" + e.getCause().getCause().getCause().getClass().getCanonicalName());
            System.err.println("" + e.getCause().getCause().getMessage());
            resultObj.put("message", e.getMessage());
            resultObj.put("messageFlag", false);

        }
        return resultObj;
    }

//    @Transactional
//    public JSONObject getObjectdata(HttpServletRequest request,
//            String parentkeyData, JSONObject columnsObj,
//            String connObj, String levelStr, Connection connection) {
//        JSONObject resultObj = new JSONObject();
//        JSONArray dataArray = new JSONArray();
//        JSONArray dataFieldsArray = new JSONArray();
//        JSONArray columnsArray = new JSONArray();
//
//        PreparedStatement preparedStatement = null;
//        ResultSet resultSet = null;
//        List<String> columnsList = new ArrayList();
//        int recordsCount = 0;
//        try {
//
//            JSONObject connectionObj = (JSONObject) JSONValue.parse(connObj);
////            JSONObject dbObj = new JSONObject();
////            dbObj.put("selectedItemLabel", connectionObj.get("CONN_CUST_COL1"));
////            dbObj.put("hostName", connectionObj.get("HOST_NAME"));
////            dbObj.put("port", connectionObj.get("CONN_PORT"));
////            dbObj.put("userName", connectionObj.get("CONN_USER_NAME"));
////            dbObj.put("password", connectionObj.get("CONN_PASSWORD"));
////            dbObj.put("serviceName", connectionObj.get("CONN_DB_NAME"));
//
//            System.out.println("columnsObj:::" + columnsObj);
//            if (levelStr != null && !"".equalsIgnoreCase(levelStr) && "3".equalsIgnoreCase(levelStr)) {
//                parentkeyData = "%";
//            }
//
//            String groupscount = request.getParameter("groupscount");
//            String gridId = request.getParameter("gridId");
//            if (gridId != null
//                    && !"".equalsIgnoreCase(gridId)
//                    && !"null".equalsIgnoreCase(gridId)) {
//                gridId = gridId.trim();
//            }
//            String pagenum = request.getParameter("pagenum");
//            String pagesize = request.getParameter("pagesize") != null ? request.getParameter("pagesize") : "10";
//            String recordendindex = request.getParameter("recordendindex");
//            String recordstartindex = (request.getParameter("recordstartindex"));
//
//            String getOnlyDataArray = (request.getParameter("getOnlyDataArray"));
//
//            int startIndex = 0;
//            int endIndex = 0;
//            if (recordstartindex != null && recordendindex != null && pagesize != null) {
//                // query = "SELECT " + columnsQuery + " FROM (" + selectQuery + ") WHERE RNO  BETWEEN  " + recordstartindex + "  AND " + recordendindex + " ";
//                startIndex = Integer.parseInt(recordstartindex);
//                endIndex = Integer.parseInt(recordendindex);
//            }
//
//            String conditionQuery = "";
//
//            //JSONObject paginationObj = visionSearchDAO.buildPaginationQuery(request, selectQuery);
//            Integer filterscount = 0;
//            String filterCondition = "";
//            if (request.getParameter("filterscount") != null) {
//                filterscount = new Integer(request.getParameter("filterscount"));
//                filterCondition = buildFilterCondition(filterscount, request, (String) connectionObj.get("CONN_CUST_COL1"));
//                if (!"".equalsIgnoreCase(filterCondition)) {
//                    conditionQuery += " WHERE " + filterCondition;
//                }
//
//            }
//
//            String countQuery = "SELECT count(*) FROM " + parentkeyData + conditionQuery;
//            ResultSet countResultSet = connection.prepareStatement(countQuery).executeQuery();
//            while (countResultSet.next()) {
//                recordsCount = countResultSet.getInt(1);
//
//            }
//
//            String orderby = "";
//            String sortdatafield = request.getParameter("sortdatafield");
//            System.out.println("sortdatafield::::" + sortdatafield);
//            String sortorder = request.getParameter("sortorder");
//            if (!(sortdatafield != null && !"".equalsIgnoreCase(sortdatafield))) {
//                sortdatafield = (String) request.getAttribute("sortdatafield");
//            }
//            if (!(sortorder != null && !"".equalsIgnoreCase(sortorder))) {
//                sortorder = (String) request.getAttribute("sortorder");
//            }
//            System.out.println("sortorder::::" + sortorder);
//            if (sortdatafield != null && sortorder != null && (sortorder.equals("asc") || sortorder.equals("desc"))) {
//                //sortdatafield = sortdatafield.replaceAll("([^A-Za-z0-9])", "");
//                orderby = " ORDER BY " + sortdatafield + " " + sortorder;
//            }
//
//            conditionQuery += orderby;
//            if (connectionObj != null
//                    && "ORACLE".equalsIgnoreCase(String.valueOf(connectionObj.get("CONN_CUST_COL1")))) {
//                conditionQuery += " OFFSET " + startIndex + " ROWS FETCH NEXT " + pagesize + " ROWS ONLY";
//            } else if (connectionObj != null
//                    && "MYSQL".equalsIgnoreCase(String.valueOf(connectionObj.get("CONN_CUST_COL1")))) {
//                conditionQuery += " LIMIT " + startIndex + "," + pagesize + "";
//            } else if (connectionObj != null
//                    && "SQLSERVER".equalsIgnoreCase(String.valueOf(connectionObj.get("CONN_CUST_COL1")))) {
//                if (!(orderby != null
//                        && !"".equalsIgnoreCase(orderby)
//                        && !"null".equalsIgnoreCase(orderby))) {
//                    conditionQuery += " ORDER BY (SELECT NULL) ";
//                }
//                conditionQuery += " OFFSET " + startIndex + " ROWS FETCH NEXT " + pagesize + " ROWS ONLY";
//            }
//
//            String selectQuery = "SELECT * FROM " + parentkeyData + conditionQuery;
//
//            System.out.println("Tree Data query::" + selectQuery);
//            preparedStatement = connection.prepareStatement(selectQuery);
//            resultSet = preparedStatement.executeQuery();
//            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
//            int columnCount = resultSetMetaData.getColumnCount();
//
//            if (getOnlyDataArray != null && "Y".equalsIgnoreCase(getOnlyDataArray)) {
//
//                while (resultSet.next()) {
//                    JSONObject dataObj = new JSONObject();
//
//                    for (int i = 1; i <= columnCount; i++) {
//                        JSONObject dataFieldsObj = new JSONObject();
//                        String columnType = resultSetMetaData.getColumnTypeName(i);
//                        String columnName = resultSetMetaData.getColumnName(i);
//                        Object data = null;
//                        if ("DATE".equalsIgnoreCase(columnType)
//                                || "DATETIME".equalsIgnoreCase(columnType)
//                                || "TIMESTAMP".equalsIgnoreCase(columnType)) {
//                            data = resultSet.getString(columnName);
//                        } else {
//                            data = resultSet.getObject(columnName);
//                        }
//                        if (data instanceof byte[]) {
//                            byte[] bytesArray = (byte[]) data;
//                            data = new RAW(bytesArray).stringValue();
//                        }
//                        dataObj.put(columnName, data);
//
//                    }
//
//                    dataArray.add(dataObj);
//
//                }
//                if (recordsCount != 0) {
//                    dataArray.add(recordsCount);
//                }
//
//                resultObj.put("dataArray", dataArray);
//            } else {
//                String gridPersonalizeStr = "";
//                for (int i = 1; i <= columnCount; i++) {
//                    JSONObject dataFieldsObj = new JSONObject();
//                    String columnType = resultSetMetaData.getColumnTypeName(i);
//                    String columnName = resultSetMetaData.getColumnName(i);
//                    dataFieldsObj.put("name", columnName);
//                    dataFieldsObj.put("type", "string");
//
//                    dataFieldsArray.add(dataFieldsObj);
//
//                    JSONObject columnsObject = new JSONObject();
//
//                    columnsObject.put("text", columnName);
//                    columnsObject.put("datafield", columnName);
//                    columnsObject.put("width", 120);
//                    columnsObject.put("sortable", true);
//                    columnsArray.add(columnsObject);
//                    gridPersonalizeStr += "<tr>"
//                            + "<td>"
//                            + columnName
//                            + "</td>"
//                            + "<td>"
//                            + "<input type='checkbox' data-gridid='" + gridId + "' checked id='" + gridId + "_" + columnName + "_DISPLAY' data-type='display' "
//                            + " data-colname='" + columnName + "' onchange=\"updateETLPersonalize(id)\""
//                            + "</td>"
//                            + "<td>"
//                            + "<input type='checkbox' id='" + gridId + "_" + columnName + "_FREEZE' data-gridid='" + gridId + "' data-type='pinned' "
//                            + " data-colname='" + columnName + "' onchange=\"updateETLPersonalize(id)\""
//                            + "</td>"
//                            + "</tr>";
//
//                }
//                gridPersonalizeStr = "<div class=\"personaliseoption visionSearchPersonaliseoption\" style=\"margin-top:5px;\">"
//                        + "<div onclick=slideSettingsETL('" + gridId + "') class=\"layoutoptions ui-accordion\">"
//                        + "<h3 class=\"ui-accordion-header1\"><span class=\"ui-accordion-header-icon ui-icon1 "
//                        + " ui-icon-triangle-1-e ui-icon-triangle-1-s\" id=\"" + gridId + "_personalizeid\"></span>"
//                        + "<img alt=\"\" class=\"navIcon gear\" src=\"images/f_spacer.gif\">Personalize</h3>"
//                        + "</div><div id=\"" + gridId + "_settings_panel\" class=\"VisionETLSettings_panel\" style=\"display: none;\">"
//                        + "<div class=\"personalize\" id=\"" + gridId + "_personalize_fields\"> <div class=\"pers_content\">"
//                        + " <div id=\"tg-wrap4\" class=\"VisionETL-tg-wrap visionSearchPersonalise\"> "
//                        + "<div class=\"visionPersonaliseSticky\"> <div class=\"sticky-wrap\"> "
//                        + " <div class=\"sticky-wrap\">"
//                        + "<table class=\"personalize_tbl sticky-enabled\" id=\"pers_criteria\" style=\"margin: 0px; width: 100%;\"> "
//                        + "  <thead> <tr style=\"\"><th>Parameter</th><th>Display</th><th>Freeze</th>	   </tr>   </thead>  "
//                        + " <tbody>"
//                        + gridPersonalizeStr
//                        + "</tbody>"
//                        + "</table></div></div></div></div></div></div></div></div>";
//                
//                JSONObject dataFieldsObjHidden = new JSONObject();
//                dataFieldsObjHidden.put("name", parentkeyData+"_HIDDEN");
//                dataFieldsObjHidden.put("type", "string");
//
//                dataFieldsArray.add(dataFieldsObjHidden);
//
//                JSONObject columnsObjecthidden = new JSONObject();
//
//                columnsObjecthidden.put("text", parentkeyData+"_HIDDEN" );
//                columnsObjecthidden.put("datafield", parentkeyData+"_HIDDEN");
//            
//                columnsObjecthidden.put("hidden", true);
//                columnsArray.add(columnsObjecthidden);
//                
//                
//                resultObj.put("dataFieldsArray", dataFieldsArray);
//                resultObj.put("columnsArray", columnsArray);
//                resultObj.put("gridPersonalizeStr", gridPersonalizeStr);
//                //resultObj.put("totalCount", recordsCount);
//
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//
//            if (connection != null) {
//                try {
//                    connection.close();
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//
//            }
//        }
//        return resultObj;
//
//    }

   
    @Transactional
	public JSONObject getObjectdata(HttpServletRequest request, String parentkeyData, JSONObject columnsObj,
			String connObj, String levelStr, Connection connection) {
		JSONObject resultObj = new JSONObject();
		JSONArray dataArray = new JSONArray();
		JSONArray dataFieldsArray = new JSONArray();
		JSONArray columnsArray = new JSONArray();

		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		List<String> columnsList = new ArrayList();
		int recordsCount = 0;
		try {

			JSONObject connectionObj = (JSONObject) JSONValue.parse(connObj);

			System.out.println("columnsObj:::" + columnsObj);
			if (levelStr != null && !"".equalsIgnoreCase(levelStr) && "3".equalsIgnoreCase(levelStr)) {
				parentkeyData = "%";
			}

			String groupscount = request.getParameter("groupscount");
			String gridId = request.getParameter("gridId");
			if (gridId != null && !"".equalsIgnoreCase(gridId) && !"null".equalsIgnoreCase(gridId)) {
				gridId = gridId.trim();
			}
			String pagenum = request.getParameter("pagenum");
			String pagesize = request.getParameter("pagesize") != null ? request.getParameter("pagesize") : "10";
			String recordendindex = request.getParameter("recordendindex");
			String recordstartindex = (request.getParameter("recordstartindex"));

			String getOnlyDataArray = (request.getParameter("getOnlyDataArray"));

			int startIndex = 0;
			int endIndex = 0;
			if (recordstartindex != null && recordendindex != null && pagesize != null) {
				// query = "SELECT " + columnsQuery + " FROM (" + selectQuery + ") WHERE RNO
				// BETWEEN " + recordstartindex + " AND " + recordendindex + " ";
				startIndex = Integer.parseInt(recordstartindex);
				endIndex = Integer.parseInt(recordendindex);
			}

			String conditionQuery = "";

			// JSONObject paginationObj = visionSearchDAO.buildPaginationQuery(request,
			// selectQuery);
			Integer filterscount = 0;
			String filterCondition = "";
			if (request.getParameter("filterscount") != null && !"0".equalsIgnoreCase(request.getParameter("filterscount")) ) {
				filterscount = new Integer(request.getParameter("filterscount"));
				filterCondition = buildFilterCondition(filterscount, request,
						(String) connectionObj.get("CONN_CUST_COL1"));
				if (!"".equalsIgnoreCase(filterCondition)) {
					conditionQuery += " WHERE " + filterCondition;
				}

			} else if (request.getParameter("whereClause")!=null && !"".equalsIgnoreCase(request.getParameter("whereClause"))){
                            conditionQuery+= " "+request.getParameter("whereClause");
                        }

			String countQuery =  "SELECT /*+ MAX_EXECUTION_TIME(60000) */ count(*) FROM " + parentkeyData + conditionQuery;

			ResultSet countResultSet = connection.prepareStatement(countQuery).executeQuery();
			while (countResultSet.next()) {
				recordsCount = countResultSet.getInt(1);

			}

			String orderby = "";
			String sortdatafield = request.getParameter("sortdatafield");
			System.out.println("sortdatafield::::" + sortdatafield);
			String sortorder = request.getParameter("sortorder");
			if (!(sortdatafield != null && !"".equalsIgnoreCase(sortdatafield))) {
				sortdatafield = (String) request.getAttribute("sortdatafield");
			}
			if (!(sortorder != null && !"".equalsIgnoreCase(sortorder))) {
				sortorder = (String) request.getAttribute("sortorder");
			}
			System.out.println("sortorder::::" + sortorder);
			if (sortdatafield != null && sortorder != null && (sortorder.equals("asc") || sortorder.equals("desc"))) {
				// sortdatafield = sortdatafield.replaceAll("([^A-Za-z0-9])", "");
				orderby = " ORDER BY " + sortdatafield + " " + sortorder;
			}

			conditionQuery += orderby;
		
			String selectQuery = "";
                        
			if (connectionObj != null
					&& "ORACLE".equalsIgnoreCase(String.valueOf(connectionObj.get("CONN_CUST_COL1")))) {
			
				conditionQuery += " OFFSET " + startIndex + " ROWS FETCH NEXT " + pagesize + " ROWS ONLY";
				selectQuery = "SELECT /*+ MAX_EXECUTION_TIME(60000) */  ROWNUM , A.* FROM  " + parentkeyData + " A " + conditionQuery;
			} else if (connectionObj != null
					&& "MYSQL".equalsIgnoreCase(String.valueOf(connectionObj.get("CONN_CUST_COL1")))) {
				
				conditionQuery += " LIMIT " + startIndex + "," + pagesize + "";
				selectQuery = "SELECT /*+ MAX_EXECUTION_TIME(60000) */ ROW_NUMBER() OVER (ORDER BY 'A') AS ROWNUM, A.* FROM  " + parentkeyData + " A " + conditionQuery;
			} else if (connectionObj != null
					&& "SQLSERVER".equalsIgnoreCase(String.valueOf(connectionObj.get("CONN_CUST_COL1")))) {
				
				  String columnName = "";
                  
                  String query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                              + " WHERE TABLE_SCHEMA = '"+connectionObj.get("CONN_USER_NAME")+"' "
                              + " AND TABLE_NAME LIKE '%"+parentkeyData+"%'"
                              + "  ORDER BY TABLE_NAME OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY";
                  
                 System.out.println("Tree Data query::" + query);
                  preparedStatement = connection.prepareStatement(query);
                  resultSet = preparedStatement.executeQuery();
                  while (resultSet.next()) {
                   columnName = resultSet.getString("COLUMN_NAME");
                  }
         
					if (!(orderby != null && !"".equalsIgnoreCase(orderby) && !"null".equalsIgnoreCase(orderby))) {
						conditionQuery += " ORDER BY (SELECT NULL) ";
					}
					conditionQuery += " OFFSET " + startIndex + " ROWS FETCH NEXT " + pagesize + " ROWS ONLY";
					selectQuery = "SELECT /*+ MAX_EXECUTION_TIME(60000) */ ROW_NUMBER() OVER (ORDER BY "+columnName+") AS ROWNUM, A.* FROM  " + parentkeyData + " A " + conditionQuery;

			}
			

		
			System.out.println("Tree Data query::" + selectQuery);
			preparedStatement = connection.prepareStatement(selectQuery);
			resultSet = preparedStatement.executeQuery();
			ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
			int columnCount = resultSetMetaData.getColumnCount();
			// 291222
			JSONObject columnsDataTypeObj = new JSONObject();
			if (getOnlyDataArray != null && "Y".equalsIgnoreCase(getOnlyDataArray)) {

				while (resultSet.next()) {
					JSONObject dataObj = new JSONObject();
					String rownum = resultSetMetaData.getColumnName(1);
					int rownumVal = 0;
					// 020123
					if (rownum != null && "ROWNUM".equalsIgnoreCase(rownum)) {
						rownumVal = resultSet.getInt(rownum)-1;
					}
					for (int i = 1; i <= columnCount; i++) {
						// 020123 if you want the rownum values in clientside just comment this condition 
						//anyway im hiding this in the grid
//						if (rownum != null && "ROWNUM".equalsIgnoreCase(rownum)) {
//							rownum = null;
//							continue;
//						}

						JSONObject dataFieldsObj = new JSONObject();
						String columnType = resultSetMetaData.getColumnTypeName(i);
						String columnName = resultSetMetaData.getColumnName(i);
						Object data = null;
						if ("DATE".equalsIgnoreCase(columnType) || "DATETIME".equalsIgnoreCase(columnType)
								|| "TIMESTAMP".equalsIgnoreCase(columnType)) {
							data = resultSet.getString(columnName);
						} else if ("CLOB".equalsIgnoreCase(columnType)) {
							data = resultSet.getString(columnName);
						} else if ("BLOB".equalsIgnoreCase(columnType)) {
							Blob blobData = (Blob) resultSet.getBlob(columnName);
							if (blobData != null) {
//								int blobLength = (int) blobData.length();
//								byte[] byteArray = blobData.getBytes(1, blobLength);
								data = "<img src=\"images/ETL_File.png\" class=\"gridBlobImg\" id=gridBlobImg"
										+ rownumVal + " onclick=\"getGridBlobDetails('" + gridId
										+ "',this.id)\" title=\"download\">";
//								data = Base64.getEncoder().encodeToString(byteArray);
							}
						} else {
							data = resultSet.getObject(columnName);
						}
						if (data instanceof byte[]) {
							byte[] bytesArray = (byte[]) data;
							data = new RAW(bytesArray).stringValue();
						}
						dataObj.put(columnName, data);

					}

					dataArray.add(dataObj);

				}
				if (recordsCount != 0) {
					dataArray.add(recordsCount);
				}

				resultObj.put("dataArray", dataArray);
			} else {
				
				String host = connectionObj.get("HOST_NAME")+":"+connectionObj.get("CONN_PORT");
				String ssUsername = (String)request.getSession(false).getAttribute("ssUsername");
				List maskedColumnsList = fetchMaskedColumnsList(request, parentkeyData, host, ssUsername);
				String gridPersonalizeStr = "";
				for (int i = 1; i <= columnCount; i++) {
					JSONObject dataFieldsObj = new JSONObject();
					String columnType = resultSetMetaData.getColumnTypeName(i);
					String columnName = resultSetMetaData.getColumnName(i);
					dataFieldsObj.put("name", columnName);
					dataFieldsObj.put("type", "string");

					dataFieldsArray.add(dataFieldsObj);

					JSONObject columnsObject = new JSONObject();

					columnsDataTypeObj.put(columnName, columnType);
//                    columnsArray.add(columnsDataTypeObj);

					columnsObject.put("text", columnName);
					columnsObject.put("datafield", columnName);
					columnsObject.put("width", 120);
					columnsObject.put("sortable", true);
					
					if (maskedColumnsList.contains(columnName)) {
						columnsObject.put("cellsrenderer", "maskcolumnRenderer");
						columnsObject.put("editable", false);
						
					}
					
					if (columnName.equalsIgnoreCase("ROWNUM")) {
						columnsObject.put("hidden", true);
					}
                                        if (columnType.equalsIgnoreCase("CLOB")) {
                                            columnsObject.put("cellsrenderer", "clobColumnRenderer");
                                        }
                                        
					columnsArray.add(columnsObject);
					gridPersonalizeStr += "<tr>" + "<td>" + columnName + "</td>" + "<td>"
							+ "<input type='checkbox' data-gridid='" + gridId + "' checked id='" + gridId + "_"
							+ columnName + "_DISPLAY' data-type='display' " + " data-colname='" + columnName
							+ "' onchange=\"updateETLPersonalize(id)\"" + "</td>" + "<td>"
							+ "<input type='checkbox' id='" + gridId + "_" + columnName + "_FREEZE' data-gridid='"
							+ gridId + "' data-type='pinned' " + " data-colname='" + columnName
							+ "' onchange=\"updateETLPersonalize(id)\"" + "</td>" + "</tr>";

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

				// 291222
				JSONObject dataFieldsObjHidden = new JSONObject();
				dataFieldsObjHidden.put("name", parentkeyData + "_HIDDEN");
				dataFieldsObjHidden.put("type", "string");
				dataFieldsArray.add(dataFieldsObjHidden);

				JSONObject columnsObjecthidden = new JSONObject();
				columnsObjecthidden.put("text", parentkeyData + "_HIDDEN");
				columnsObjecthidden.put("datafield", parentkeyData + "_HIDDEN");

				columnsObjecthidden.put("hidden", true);
				columnsArray.add(columnsObjecthidden);

				resultObj.put("dataFieldsArray", dataFieldsArray);
				resultObj.put("columnsArray", columnsArray);
				resultObj.put("gridPersonalizeStr", gridPersonalizeStr);
				resultObj.put("columnsDataTypeObj", columnsDataTypeObj);
				
                                DatabaseMetaData primaryKeyMetaData = connection.getMetaData();
                                 ResultSet primaryKeys = primaryKeyMetaData.getPrimaryKeys(null, null, parentkeyData);
                                 List primaryKeyList = new ArrayList<>();
                                  while (primaryKeys.next()) {
                                        String columnName = primaryKeys.getString("COLUMN_NAME");
                                        primaryKeyList.add(columnName);
                                        System.out.println("Primary key column name: " + columnName);
                                    }
                                    resultObj.put("primaryKeysList", primaryKeyList);
                                    // Close the ResultSet and the connection
                                    primaryKeys.close();

			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			if (connection != null) {
				try {
					connection.close();
				} catch (Exception e) {
					e.printStackTrace();
				}

			}
		}
		return resultObj;

	}

    
    
//    @Transactional
//	public JSONObject getObjectdata(HttpServletRequest request, String parentkeyData, JSONObject columnsObj,
//			String connObj, String levelStr, Connection connection) {
//		JSONObject resultObj = new JSONObject();
//		JSONArray dataArray = new JSONArray();
//		JSONArray dataFieldsArray = new JSONArray();
//		JSONArray columnsArray = new JSONArray();
//
//		PreparedStatement preparedStatement = null;
//		ResultSet resultSet = null;
//		List<String> columnsList = new ArrayList();
//		int recordsCount = 0;
//		try {
//
//			JSONObject connectionObj = (JSONObject) JSONValue.parse(connObj);
//
//			System.out.println("columnsObj:::" + columnsObj);
//			if (levelStr != null && !"".equalsIgnoreCase(levelStr) && "3".equalsIgnoreCase(levelStr)) {
//				parentkeyData = "%";
//			}
//
//			String groupscount = request.getParameter("groupscount");
//			String gridId = request.getParameter("gridId");
//			if (gridId != null && !"".equalsIgnoreCase(gridId) && !"null".equalsIgnoreCase(gridId)) {
//				gridId = gridId.trim();
//			}
//			String pagenum = request.getParameter("pagenum");
//			String pagesize = request.getParameter("pagesize") != null ? request.getParameter("pagesize") : "10";
//			String recordendindex = request.getParameter("recordendindex");
//			String recordstartindex = (request.getParameter("recordstartindex"));
//
//			String getOnlyDataArray = (request.getParameter("getOnlyDataArray"));
//
//			int startIndex = 0;
//			int endIndex = 0;
//			if (recordstartindex != null && recordendindex != null && pagesize != null) {
//				// query = "SELECT " + columnsQuery + " FROM (" + selectQuery + ") WHERE RNO
//				// BETWEEN " + recordstartindex + " AND " + recordendindex + " ";
//				startIndex = Integer.parseInt(recordstartindex);
//				endIndex = Integer.parseInt(recordendindex);
//			}
//
//			String conditionQuery = "";
//
//			// JSONObject paginationObj = visionSearchDAO.buildPaginationQuery(request,
//			// selectQuery);
//			Integer filterscount = 0;
//			String filterCondition = "";
//			if (request.getParameter("filterscount") != null && !"0".equalsIgnoreCase(request.getParameter("filterscount")) ) {
//				filterscount = new Integer(request.getParameter("filterscount"));
//				filterCondition = buildFilterCondition(filterscount, request,
//						(String) connectionObj.get("CONN_CUST_COL1"));
//				if (!"".equalsIgnoreCase(filterCondition)) {
//					conditionQuery += " WHERE " + filterCondition;
//				}
//
//			} else if (request.getParameter("whereClause")!=null && !"".equalsIgnoreCase(request.getParameter("whereClause"))){
//                            conditionQuery+= " "+request.getParameter("whereClause");
//                        }
//
//			String countQuery =  "SELECT /*+ MAX_EXECUTION_TIME(60000) */ count(*) FROM " + parentkeyData + conditionQuery;
//
//			ResultSet countResultSet = connection.prepareStatement(countQuery).executeQuery();
//			while (countResultSet.next()) {
//				recordsCount = countResultSet.getInt(1);
//
//			}
//
//			String orderby = "";
//			String sortdatafield = request.getParameter("sortdatafield");
//			System.out.println("sortdatafield::::" + sortdatafield);
//			String sortorder = request.getParameter("sortorder");
//			if (!(sortdatafield != null && !"".equalsIgnoreCase(sortdatafield))) {
//				sortdatafield = (String) request.getAttribute("sortdatafield");
//			}
//			if (!(sortorder != null && !"".equalsIgnoreCase(sortorder))) {
//				sortorder = (String) request.getAttribute("sortorder");
//			}
//			System.out.println("sortorder::::" + sortorder);
//			if (sortdatafield != null && sortorder != null && (sortorder.equals("asc") || sortorder.equals("desc"))) {
//				// sortdatafield = sortdatafield.replaceAll("([^A-Za-z0-9])", "");
//				orderby = " ORDER BY " + sortdatafield + " " + sortorder;
//			}
//
//			conditionQuery += orderby;
//		
//			String selectQuery = "";
//                        
//			if (connectionObj != null
//					&& "ORACLE".equalsIgnoreCase(String.valueOf(connectionObj.get("CONN_CUST_COL1")))) {
//			
//				conditionQuery += " OFFSET " + startIndex + " ROWS FETCH NEXT " + pagesize + " ROWS ONLY";
//				selectQuery = "SELECT /*+ MAX_EXECUTION_TIME(60000) */  ROWNUM , A.* FROM  " + parentkeyData + " A " + conditionQuery;
//			} else if (connectionObj != null
//					&& "MYSQL".equalsIgnoreCase(String.valueOf(connectionObj.get("CONN_CUST_COL1")))) {
//				
//				conditionQuery += " LIMIT " + startIndex + "," + pagesize + "";
//				selectQuery = "SELECT /*+ MAX_EXECUTION_TIME(60000) */ ROW_NUMBER() OVER (ORDER BY 'A') AS ROWNUM, A.* FROM  " + parentkeyData + " A " + conditionQuery;
//			} else if (connectionObj != null
//					&& "SQLSERVER".equalsIgnoreCase(String.valueOf(connectionObj.get("CONN_CUST_COL1")))) {
//				
//				  String columnName = "";
//                  
//                  String query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
//                              + " WHERE TABLE_SCHEMA = '"+connectionObj.get("CONN_USER_NAME")+"' "
//                              + " AND TABLE_NAME LIKE '%"+parentkeyData+"%'"
//                              + "  ORDER BY TABLE_NAME OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY";
//                  
//                 System.out.println("Tree Data query::" + query);
//                  preparedStatement = connection.prepareStatement(query);
//                  resultSet = preparedStatement.executeQuery();
//                  while (resultSet.next()) {
//                   columnName = resultSet.getString("COLUMN_NAME");
//                  }
//         
//					if (!(orderby != null && !"".equalsIgnoreCase(orderby) && !"null".equalsIgnoreCase(orderby))) {
//						conditionQuery += " ORDER BY (SELECT NULL) ";
//					}
//					conditionQuery += " OFFSET " + startIndex + " ROWS FETCH NEXT " + pagesize + " ROWS ONLY";
//					selectQuery = "SELECT /*+ MAX_EXECUTION_TIME(60000) */ ROW_NUMBER() OVER (ORDER BY "+columnName+") AS ROWNUM, A.* FROM  " + parentkeyData + " A " + conditionQuery;
//
//			}
//			
//
//		
//			System.out.println("Tree Data query::" + selectQuery);
//			preparedStatement = connection.prepareStatement(selectQuery);
//			resultSet = preparedStatement.executeQuery();
//			ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
//			int columnCount = resultSetMetaData.getColumnCount();
//			// 291222
//			JSONObject columnsDataTypeObj = new JSONObject();
//			if (getOnlyDataArray != null && "Y".equalsIgnoreCase(getOnlyDataArray)) {
//
//				while (resultSet.next()) {
//					JSONObject dataObj = new JSONObject();
//					String rownum = resultSetMetaData.getColumnName(1);
//					int rownumVal = 0;
//					// 020123
//					if (rownum != null && "ROWNUM".equalsIgnoreCase(rownum)) {
//						rownumVal = resultSet.getInt(rownum)-1;
//					}
//					for (int i = 1; i <= columnCount; i++) {
//						// 020123 if you want the rownum values in clientside just comment this condition 
//						//anyway im hiding this in the grid
////						if (rownum != null && "ROWNUM".equalsIgnoreCase(rownum)) {
////							rownum = null;
////							continue;
////						}
//
//						JSONObject dataFieldsObj = new JSONObject();
//						String columnType = resultSetMetaData.getColumnTypeName(i);
//						String columnName = resultSetMetaData.getColumnName(i);
//						Object data = null;
//						if ("DATE".equalsIgnoreCase(columnType) || "DATETIME".equalsIgnoreCase(columnType)
//								|| "TIMESTAMP".equalsIgnoreCase(columnType)) {
//							data = resultSet.getString(columnName);
//						} else if ("CLOB".equalsIgnoreCase(columnType)) {
//							data = resultSet.getString(columnName);
//						} else if ("BLOB".equalsIgnoreCase(columnType)) {
//							Blob blobData = (Blob) resultSet.getBlob(columnName);
//							if (blobData != null) {
////								int blobLength = (int) blobData.length();
////								byte[] byteArray = blobData.getBytes(1, blobLength);
//								data = "<img src=\"images/ETL_File.png\" class=\"gridBlobImg\" id=gridBlobImg"
//										+ rownumVal + " onclick=\"getGridBlobDetails('" + gridId
//										+ "',this.id)\" title=\"download\">";
////								data = Base64.getEncoder().encodeToString(byteArray);
//							}
//						} else {
//							data = resultSet.getObject(columnName);
//						}
//						if (data instanceof byte[]) {
//							byte[] bytesArray = (byte[]) data;
//							data = new RAW(bytesArray).stringValue();
//						}
//						dataObj.put(columnName, data);
//
//					}
//
//					dataArray.add(dataObj);
//
//				}
////				if (recordsCount != 0) {
////					dataArray.add(recordsCount);
////				}
//
//				resultObj.put("dataArray", dataArray);
//			} else {
//				
//				String host = connectionObj.get("HOST_NAME")+":"+connectionObj.get("CONN_PORT");
//				String ssUsername = (String)request.getSession(false).getAttribute("ssUsername");
//				List maskedColumnsList = fetchMaskedColumnsList(request, parentkeyData, host, ssUsername);
//				String gridPersonalizeStr = "";
//				for (int i = 1; i <= columnCount; i++) {
//					JSONObject dataFieldsObj = new JSONObject();
//					String columnType = resultSetMetaData.getColumnTypeName(i);
//					String columnName = resultSetMetaData.getColumnName(i);
//					dataFieldsObj.put("name", columnName);
//					dataFieldsObj.put("type", "string");
//
//					dataFieldsArray.add(dataFieldsObj);
//
//					JSONObject columnsObject = new JSONObject();
//
//					columnsDataTypeObj.put(columnName, columnType);
////                    columnsArray.add(columnsDataTypeObj);
//
//					columnsObject.put("text", columnName);
//					columnsObject.put("datafield", columnName);
//					columnsObject.put("width", 120);
//					columnsObject.put("sortable", true);
//					
//					if (maskedColumnsList.contains(columnName)) {
//						columnsObject.put("cellsrenderer", "maskcolumnRenderer");
//						columnsObject.put("editable", false);
//						
//					}
//					
//					if (columnName.equalsIgnoreCase("ROWNUM")) {
//						columnsObject.put("hidden", true);
//					}
//                                        if (columnType.equalsIgnoreCase("CLOB")) {
//                                            columnsObject.put("cellsrenderer", "clobColumnRenderer");
//                                        }
//                                        
//					columnsArray.add(columnsObject);
//					gridPersonalizeStr += "<tr>" + "<td>" + columnName + "</td>" + "<td>"
//							+ "<input type='checkbox' data-gridid='" + gridId + "' checked id='" + gridId + "_"
//							+ columnName + "_DISPLAY' data-type='display' " + " data-colname='" + columnName
//							+ "' onchange=\"updateETLPersonalize(id)\"" + "</td>" + "<td>"
//							+ "<input type='checkbox' id='" + gridId + "_" + columnName + "_FREEZE' data-gridid='"
//							+ gridId + "' data-type='pinned' " + " data-colname='" + columnName
//							+ "' onchange=\"updateETLPersonalize(id)\"" + "</td>" + "</tr>";
//
//				}
//				gridPersonalizeStr = "<div class=\"personaliseoption visionSearchPersonaliseoption\" style=\"margin-top:5px;\">"
//						+ "<div onclick=slideSettingsETL('" + gridId + "') class=\"layoutoptions ui-accordion\">"
//						+ "<h3 class=\"ui-accordion-header1\"><span class=\"ui-accordion-header-icon ui-icon1 "
//						+ " ui-icon-triangle-1-e ui-icon-triangle-1-s\" id=\"" + gridId + "_personalizeid\"></span>"
//						+ "<img alt=\"\" class=\"navIcon gear\" src=\"images/f_spacer.gif\">Personalize</h3>"
//						+ "</div><div id=\"" + gridId
//						+ "_settings_panel\" class=\"VisionETLSettings_panel\" style=\"display: none;\">"
//						+ "<div class=\"personalize\" id=\"" + gridId
//						+ "_personalize_fields\"> <div class=\"pers_content\">"
//						+ " <div id=\"tg-wrap4\" class=\"VisionETL-tg-wrap visionSearchPersonalise\"> "
//						+ "<div class=\"visionPersonaliseSticky\"> <div class=\"sticky-wrap\"> "
//						+ " <div class=\"sticky-wrap\">"
//						+ "<table class=\"personalize_tbl sticky-enabled\" id=\"pers_criteria\" style=\"margin: 0px; width: 100%;\"> "
//						+ "  <thead> <tr style=\"\"><th>Parameter</th><th>Display</th><th>Freeze</th>	   </tr>   </thead>  "
//						+ " <tbody>" + gridPersonalizeStr + "</tbody>"
//						+ "</table></div></div></div></div></div></div></div></div>";
//
//				// 291222
//				JSONObject dataFieldsObjHidden = new JSONObject();
//				dataFieldsObjHidden.put("name", parentkeyData + "_HIDDEN");
//				dataFieldsObjHidden.put("type", "string");
//				dataFieldsArray.add(dataFieldsObjHidden);
//
//				JSONObject columnsObjecthidden = new JSONObject();
//				columnsObjecthidden.put("text", parentkeyData + "_HIDDEN");
//				columnsObjecthidden.put("datafield", parentkeyData + "_HIDDEN");
//
//				columnsObjecthidden.put("hidden", true);
//				columnsArray.add(columnsObjecthidden);
//
//				resultObj.put("dataFieldsArray", dataFieldsArray);
//				resultObj.put("columnsArray", columnsArray);
//				resultObj.put("gridPersonalizeStr", gridPersonalizeStr);
//				resultObj.put("columnsDataTypeObj", columnsDataTypeObj);
//				
//                                DatabaseMetaData primaryKeyMetaData = connection.getMetaData();
//                                 ResultSet primaryKeys = primaryKeyMetaData.getPrimaryKeys(null, null, parentkeyData);
//                                 List primaryKeyList = new ArrayList<>();
//                                  while (primaryKeys.next()) {
//                                        String columnName = primaryKeys.getString("COLUMN_NAME");
//                                        primaryKeyList.add(columnName);
//                                        System.out.println("Primary key column name: " + columnName);
//                                    }
//                                    resultObj.put("primaryKeysList", primaryKeyList);
//                                    // Close the ResultSet and the connection
//                                    primaryKeys.close();
//
//			}
//
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//
//			if (connection != null) {
//				try {
//					connection.close();
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//
//			}
//		}
//		return resultObj;
//
//	}

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public String buildFilterCondition(int filterscount, HttpServletRequest request, String dataBaseDriver) {
        String conditionQuery = "";
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");

            for (int i = 0; i < filterscount; i++) {
                String columnName = request.getParameter("filterdatafield" + i);
                String condition = request.getParameter("filtercondition" + i);
                String value = request.getParameter("filtervalue" + i);
                String filteroperator = request.getParameter("filteroperator" + i);
//                //System.out.println("columnName::::" + columnName);
//                //System.out.println("filtercondition::::" + condition);
//                //System.out.println("filtervalue::::" + value);
//                //System.out.println("filteroperator::::" + filteroperator);
                String condtionQuery = "";

                value = value.toUpperCase();
//                //System.out.println("value::::Before:::" + value);
                if (columnName.contains("DATE")) {
                    // value = "TO_DATE('" + value + "', 'MM/DD/YYYY')";
                    value = value.substring(0, value.indexOf("GMT") - 9).trim();
                    if (dataBaseDriver != null && !"".equalsIgnoreCase(dataBaseDriver)) {
                        if (dataBaseDriver.toUpperCase().contains("ORACLE")) {
                            columnName = "TO_DATE(TO_CHAR(" + columnName + ",'DY MON DD YYYY'), 'DY MON DD YYYY')";
                            value = "TO_DATE('" + value + "','DY MON DD YYYY')";
                        } else if (dataBaseDriver.toUpperCase().contains("MYSQL")) {
                            columnName = "STR_TO_DATE(DATE_FORMAT(" + columnName + ",'DY MON DD YYYY'), 'DY MON DD YYYY')";
                            value = "STR_TO_DATE('" + value + "','DY MON DD YYYY')";
                        } else if (dataBaseDriver.toUpperCase().contains("SQLSERVER")) {
                            columnName = "CONVERT(CONVERT(VARCHAR(10)," + columnName + ",'DY MON DD YYYY'), 'DY MON DD YYYY')";
                            value = "CONVERT('" + value + "','DY MON DD YYYY')";
                        } else if (dataBaseDriver.toUpperCase().contains("DB2")) {

                        }

                    }

                }

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
                //  query = query + " AND " + getCondition(filterdatafield, filtercondition, filtervalue);
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

    @Transactional
    public List<Object[]> getProcesslog(String userName, String orgnId, String currentProcesslogIndex, String jobId) {
        List<Object[]> processlogDataList = new ArrayList<>();
        try {
            Map<String, Object> selectMap = new HashMap<>();
            String selectQuery = "SELECT LOG_TYPE, "//0
                    + "LOG_TXT, "//1
                    + "CREATE_DATE,"//2
                    + "PROCESS_FLAG, "//3
                    + "SEQUENCE_NO, "//4
                    + "DM_LOG_CUST_COL2, "//5
                    + "DM_LOG_CUST_COL3, "//6
                    + "DM_LOG_CUST_COL4, "//7
                    + "DM_LOG_CUST_COL5 "//8
                    + " FROM RECORD_DM_PROCESS_LOG"
                    + " WHERE USER_NAME =:USER_NAME AND ORGN_ID =:ORGN_ID AND DM_LOG_CUST_COL1 IS NULL AND JOB_ID =:JOB_ID";
            if (currentProcesslogIndex != null && !"".equalsIgnoreCase(currentProcesslogIndex) && !"null".equalsIgnoreCase(currentProcesslogIndex)) {
                selectQuery += " AND SEQUENCE_NO >  " + currentProcesslogIndex;
            }
            selectQuery += ""
                    + " ORDER BY CREATE_DATE,SEQUENCE_NO";

            selectMap.put("USER_NAME", userName);
            selectMap.put("ORGN_ID", orgnId);
            selectMap.put("JOB_ID", jobId);
//            System.out.println("selectQuery:getProcesslog::"+selectQuery);
//            System.out.println("selectMap:getProcesslog::"+selectMap);
            processlogDataList = access.sqlqueryWithParams(selectQuery, selectMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return processlogDataList;
    }
    
    
    @Transactional
    public List<Object[]> getOperatorProcessStatus(String userName, String orgnId, String currentProcesslogIndex, String jobId) {
        List<Object[]> processlogDataList = new ArrayList<>();
        try {
            Map<String, Object> selectMap = new HashMap<>();
            String selectQuery = "SELECT "
            		+ " STEP_OPERATOR, PROCESS_FLAG, STEP_TYPE, STEP_STATUS "
            		+ " FROM RECORD_DM_JOB_PROCESS_STEPS "
                    + " WHERE USER_NAME =:USER_NAME AND ORGN_ID =:ORGN_ID AND JOB_ID =:JOB_ID";
         
            selectQuery += ""
                    + " ORDER BY CREATE_DATE,SEQUENCE_NO";

            selectMap.put("USER_NAME", userName);
            selectMap.put("ORGN_ID", orgnId);
            selectMap.put("JOB_ID", jobId);
           // System.out.println("selectQuery:getProcesslog::"+selectQuery);
           // System.out.println("selectMap:getProcesslog::"+selectMap);
            processlogDataList = access.sqlqueryWithParams(selectQuery, selectMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return processlogDataList;
    }

    @Transactional
    public void deleteProcesslog(String userName, String orgnId) {
        try {
            String deleteQuery = "DELETE FROM RECORD_DM_PROCESS_LOG"
                    + " WHERE USER_NAME =:USER_NAME AND ORGN_ID =:ORGN_ID";
            Map<String, Object> deleteMap = new HashMap<>();
            deleteMap.put("USER_NAME", userName);
            deleteMap.put("ORGN_ID", orgnId);
            int deleteCount = access.executeUpdateSQLNoAudit(deleteQuery, deleteMap);
            System.out.println("deleteCount:::" + deleteCount);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Transactional
    public void deleteProcesslog(String userName, String orgnId, String jobId) {
        try {
            String deleteQuery = "DELETE FROM RECORD_DM_PROCESS_LOG"
                    + " WHERE USER_NAME =:USER_NAME AND ORGN_ID =:ORGN_ID AND JOB_ID =:JOB_ID";
            Map<String, Object> deleteMap = new HashMap<>();
            deleteMap.put("USER_NAME", userName);
            deleteMap.put("ORGN_ID", orgnId);
            deleteMap.put("JOB_ID", jobId);
            int deleteCount = access.executeUpdateSQLNoAudit(deleteQuery, deleteMap);
            System.out.println("deleteCount:::" + deleteCount);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Transactional
    public int getLastLogSequenceNo(String userName, String orgnId) {
        int lastSequenceNo = 0;
        try {
            Map<String, Object> selectMap = new HashMap<>();
            String selectQuery = "SELECT MAX(SEQUENCE_NO) FROM RECORD_DM_PROCESS_LOG"
                    + " WHERE USER_NAME =:USER_NAME AND ORGN_ID =:ORGN_ID ";
            selectMap.put("USER_NAME", userName);
            selectMap.put("ORGN_ID", orgnId);
            List processlogDataList = access.sqlqueryWithParams(selectQuery, selectMap);
            if (processlogDataList != null && !processlogDataList.isEmpty()) {
                lastSequenceNo = new PilogUtilities().convertIntoInteger(processlogDataList.get(0));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return lastSequenceNo;
    }

    @Transactional
    public List<Object[]> getTargetDataType(String fromSysType) {
        List<Object[]> dataTypeList = new ArrayList<>();
        try {
            String dataTypeSelectQuery = "SELECT TO_SYS_TYPE, "//0
                    + "TO_TYPE, "//1
                    + "TO_MAX_LEN,"//2
                    + "TO_LENGTH_FLAG, "//3
                    + "TO_BYTE_CHAR, "//4
                    + " FROM_SYS_TYPE, "//5
                    + " FROM_TYPE, "//6
                    + " FROM_MAX_LEN, "//7
                    + " FROM_LENGTH_FLAG, "//8
                    + " FROM_BYTE_CHAR "//9
                    + " FROM B_DM_DATA_TYPE_CONV WHERE "
                    + " FROM_SYS_TYPE =:FROM_SYS_TYPE AND ACTIVE_FLAG='Y' ORDER BY SEQUENCE_NO";
            Map<String, Object> dataTypeSelectMap = new HashMap<>();
            dataTypeSelectMap.put("FROM_SYS_TYPE", fromSysType.toUpperCase());
            dataTypeList = access.sqlqueryWithParams(dataTypeSelectQuery, dataTypeSelectMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dataTypeList;
    }

    @Transactional
    public List<Object[]> getTargetDataType(String fromSysType, String toSysType) {
        List<Object[]> dataTypeList = new ArrayList<>();
        try {
            String dataTypeSelectQuery = "SELECT TO_SYS_TYPE, "//0
                    + "TO_TYPE, "//1
                    + "TO_MAX_LEN,"//2
                    + "TO_LENGTH_FLAG, "//3
                    + "TO_BYTE_CHAR, "//4
                    + " FROM_SYS_TYPE, "//5
                    + " FROM_TYPE, "//6
                    + " FROM_MAX_LEN, "//7
                    + " FROM_LENGTH_FLAG, "//8
                    + " FROM_BYTE_CHAR "//9
                    + " FROM B_DM_DATA_TYPE_CONV WHERE "
                    + " FROM_SYS_TYPE =:FROM_SYS_TYPE AND ACTIVE_FLAG='Y' AND TO_SYS_TYPE =:TO_SYS_TYPE ORDER BY SEQUENCE_NO";
            Map<String, Object> dataTypeSelectMap = new HashMap<>();
            dataTypeSelectMap.put("FROM_SYS_TYPE", fromSysType.toUpperCase());
            dataTypeSelectMap.put("TO_SYS_TYPE", toSysType.toUpperCase());
            dataTypeList = access.sqlqueryWithParams(dataTypeSelectQuery, dataTypeSelectMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dataTypeList;
    }

    @Transactional
    public List<Object[]> getAllDataTypes() {
        List<Object[]> dataTypeList = new ArrayList<>();
        try {
            String dataTypeSelectQuery = "SELECT "
                    + "SYS_TYPE, "//0
                    + "DATA_TYPE, "//1
                    + "MAX_LEN, "//2
                    + "LENGTH_FLAG, "//3
                    + "BYTE_CHAR_FLAG, "//4
                    + "PRECESION_FLAG,"//5
                    + "SCALE_FLAG"//6
                    + " FROM B_DM_DATA_TYPES WHERE "
                    + "ACTIVE_FLAG='Y' ORDER BY SEQUENCE_NO";
            Map<String, Object> dataTypeSelectMap = new HashMap<>();
            dataTypeList = access.sqlqueryWithParams(dataTypeSelectQuery, dataTypeSelectMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dataTypeList;
    }

    @Transactional
    public void saveUserFiles(HttpServletRequest request,
            String orginalFileName,
            String fileName,
            String filePath,
            String fileType
    ) {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = DriverManager.getConnection(dbURL, userName, password);
            String insertQuery = "INSERT INTO DAL_DM_SAVED_FILES(ORGN_ID, USER_NAME, "
                    + "FILE_ORG_NAME, FILE_NAME, FILE_PATH, FILE_TYPE, CREATE_BY, EDIT_BY,FILE_CONTENT)"
                    + " VALUES(?,?,?,?,?,?,?,?,?)";
            statement = connection.prepareStatement(insertQuery);

            statement.setObject(1, request.getSession(false).getAttribute("ssOrgId"));//ORGN_ID
            statement.setObject(2, request.getSession(false).getAttribute("ssUsername"));//USER_NAME
            statement.setObject(3, orginalFileName);//FILE_ORG_NAME
            statement.setObject(4, fileName);//FILE_NAME
            statement.setObject(5, filePath);//FILE_PATH
            statement.setObject(6, fileType);//FILE_TYPE
            statement.setObject(7, request.getSession(false).getAttribute("ssUsername"));//CREATE_BY
            statement.setObject(8, request.getSession(false).getAttribute("ssUsername"));//EDIT_BY
            File folderPath = new File(filePath);
            if (!folderPath.exists()) {
                folderPath.mkdirs();
            }
            File toBeSaveFile = new File(folderPath.getAbsolutePath() + File.separator + fileName);
//            FileInputStream fis = new FileInputStream(toBeSaveFile);
//            statement.setBinaryStream(9, fis, (int) toBeSaveFile.length());//FILE_CONTENT
//            System.out.println("insertQuery:::" + insertQuery);
//            int insertCount = statement.executeUpdate();
//            System.out.println("insertCount::" + insertCount);

            try (FileInputStream fis = new FileInputStream(toBeSaveFile)) {
                statement.setBinaryStream(9, fis, (int) toBeSaveFile.length()); 
                System.out.println("insertQuery:::" + insertQuery);
                int insertCount = statement.executeUpdate();
                System.out.println("insertCount::" + insertCount);
            } catch (IOException e) {
                e.printStackTrace(); 
            }   
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
            }
        }
    }

    @Transactional
    public List<String> getAllFilesType(HttpServletRequest request) {
        List<String> allFileTypeList = new ArrayList<>();
        try {
            String selectQuery = "SELECT DISTINCT FILE_TYPE "
                    + "FROM DAL_DM_SAVED_FILES "
                    + "WHERE USER_NAME =:USER_NAME AND ORGN_ID =:ORGN_ID ";
            Map<String, Object> selectMap = new HashMap<>();
            selectMap.put("ORGN_ID", request.getSession(false).getAttribute("ssOrgId"));//ORGN_ID
            selectMap.put("USER_NAME", request.getSession(false).getAttribute("ssUsername"));//USER_NAME
            allFileTypeList = access.sqlqueryWithParams(selectQuery, selectMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return allFileTypeList;
    }

    @Transactional
    public List<Object[]> allFileNamesListByType(HttpServletRequest request, String parentkey) {
        List<Object[]> allFileNamesList = new ArrayList<>();
        try {
            String selectQuery = "SELECT FILE_ORG_NAME, "//0
                    + "FILE_NAME, "//1
                    + "FILE_PATH, "//2
                    + "FILE_TYPE, "//3
                    + "FILE_CONTENT "//4 
                    + "FROM DAL_DM_SAVED_FILES "
                    + " WHERE UPPER(FILE_TYPE) =:FILE_TYPE AND USER_NAME =:USER_NAME AND ORGN_ID =:ORGN_ID "
                    + " ORDER BY CREATE_DATE";
            Map<String, Object> selectMap = new HashMap<>();
            selectMap.put("FILE_TYPE", ((parentkey != null && !"".equalsIgnoreCase(parentkey) && !"null".equalsIgnoreCase(parentkey))
                    ? parentkey.toUpperCase() : ""));//FILE_TYPE
            selectMap.put("ORGN_ID", request.getSession(false).getAttribute("ssOrgId"));//ORGN_ID
            selectMap.put("USER_NAME", request.getSession(false).getAttribute("ssUsername"));//USER_NAME
            allFileNamesList = access.sqlqueryWithParams(selectQuery, selectMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return allFileNamesList;
    }

    @Transactional
    public List getUniqueRowsRange(HttpServletRequest request, String trfmRulesId) {

        List<Object[]> uniqueRowsRangelist = new ArrayList();
        Map<String, Object> map = new HashMap<>();
        try {
            String query = "SELECT UNIQUE_ROWS_FLAG, ROWS_FROM, ROWS_TO, DM_MSTR_CUST_COL2 FROM C_DM_MASTER_TABLES  " // ravi operator Type
                    + "WHERE TRFM_RULES_ID =:TRFM_RULES_ID";
            map.put("TRFM_RULES_ID", trfmRulesId);
            System.out.println(" query ::: " + query);
            uniqueRowsRangelist = access.sqlqueryWithParams(query, map);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return uniqueRowsRangelist;

    }

    @Transactional
    public String getGetListOfValues(String dlovId,
            String value,
            String columnName,
            HttpServletRequest request
    ) {
        String listOfValues = "";
        try {
            JSONObject labelsObj = new PilogUtilities().getMultilingualObject(request);
            String orgnId = (String) request.getSession(false).getAttribute("ssOrgId");
            if (dlovId != null && !"".equalsIgnoreCase(dlovId) && !"".equalsIgnoreCase(dlovId)) {

//                System.out.println(" pannelObject[2]:::::" + pannelObject[15] + ":::::::::::" + orgnId);
                String query = "from DalDlov where id.dlovName=:dlovName and id.orgnId=:orgnId order by id.sequenceNo";

                Map<String, Object> map = new HashMap<>();
                map.put("dlovName", dlovId);
//                map.put("dlovName", "DLOV_" + pannelObject[15]);
                map.put("orgnId", orgnId);
                List list = access.queryWithParams(query, map);
                if (list != null && !list.isEmpty()) {
                    for (int i = 0; i < list.size(); i++) {

                        DalDlov dalDlov = (DalDlov) list.get(i);
                        if (dalDlov != null && "SQL".equalsIgnoreCase(dalDlov.getDataType()) && dalDlov.getProcessValue() != null) {
                            String lovQuery = dalDlov.getProcessValue();
                            if (lovQuery != null && lovQuery.contains("<<--") && lovQuery.contains("-->>")) {
//                                lovQuery = visionGenericDAO.replaceSessionValues(lovQuery, request);
                            }
                            System.out.println("lovQuery::::" + lovQuery);
                            List dataList = access.sqlqueryWithParams(lovQuery, new HashMap<>());
                            if (dataList != null && !dataList.isEmpty()) {
                                for (int j = 0; j < dataList.size(); j++) {
                                    Object dataObject = dataList.get(j);
                                    if (dataObject instanceof Object[]) {
                                        Object[] dataObjectArray = (Object[]) dataList.get(j);
                                        String fetchSelect = (dataObjectArray[0] != null && ((String) dataObjectArray[0]).equalsIgnoreCase(String.valueOf(value))) ? "selected = selected" : "";
                                        listOfValues += "<option value=\"" + (dataObjectArray[0] != null ? dataObjectArray[0] : "") + "\" "
                                                + (((fetchSelect != null && !"".equalsIgnoreCase(fetchSelect)) && !((String.valueOf(value)).equalsIgnoreCase(""))) ? fetchSelect : "") + ">" + new PilogUtilities().convertIntoMultilingualValue(labelsObj, dataObjectArray[1]) + "</option>";
//                                        listOfValues += "<option value='" + (dataObjectArray[0] != null ? dataObjectArray[0] : "") + "' "
//                                                + ">" + new PilogUtilities().convertIntoMultilingualValue(labelsObj, dataObjectArray[1]) + "</option>";

                                    } else {
                                        String fetchSelect = (dataObject != null && ((String) dataObject).equalsIgnoreCase(String.valueOf(value))) ? "selected = selected" : "";
                                        listOfValues += "<option value=\"" + (dataObject != null ? dataObject : "") + "\" "
                                                + (((fetchSelect != null && !"".equalsIgnoreCase(fetchSelect)) && !((String.valueOf(value)).equalsIgnoreCase(""))) ? fetchSelect : "") + ">" + new PilogUtilities().convertIntoMultilingualValue(labelsObj, dataObject) + "</option>";

//                                        listOfValues += "<option value='" + (dataObject != null ? dataObject : "") + "' "
//                                                + ">" + new PilogUtilities().convertIntoMultilingualValue(labelsObj, dataObject) + "</option>";
                                    }

                                }
                            }

                        } else {
                            String fetchSelect = (dalDlov.getProcessValue() != null && ((String) dalDlov.getProcessValue()).equalsIgnoreCase(String.valueOf(value))) ? "selected = selected" : "";
                            String defaultSelect = (dalDlov.getDefaultFlag() != null && dalDlov.getDefaultFlag().equalsIgnoreCase("Y")) ? "selected=\"selected\"" : "";
                            listOfValues += "<option value=\"" + (dalDlov.getProcessValue() != null ? dalDlov.getProcessValue() : "") + "\" "
                                    + (((value != null) && !((String.valueOf(value)).equalsIgnoreCase(""))) ? fetchSelect : defaultSelect) + ">" + new PilogUtilities().convertIntoMultilingualValue(labelsObj, dalDlov.getDisplay()) + "</option>";

                        }
//                        dalDlov.setDisplay((String) new PilogUtilities().convertIntoMultilingualValue(labelsObj, dalDlov.getDisplay()));

                    }

                }

            }
//            System.out.println("listOfValues::::"+listOfValues);
        } catch (Exception e) {
            e.printStackTrace();
        }
//        System.out.println("listOfValues::::::::" + listOfValues);
        return listOfValues;
    }

    @Transactional
    public JSONObject getDetailsOfQuery(String query, String typeOfQuery) {
        return access.getDetailsOfQuery(query, typeOfQuery);
    }

    @Transactional
    public JSONObject getSapTableData(HttpServletRequest request, JCO.Client connection, String tableName) {
        JSONObject resultObj = new JSONObject();
        dataMigrationAccessDAO.retrieveRepository(connection);

        JCO.Table fields = null;
        JCO.Table tableData = null;
        JCO.Table countTableData = null;
        JCO.Function function = null;
        JCO.Function countFunction = null;
        long count = 0;

        JSONArray dataArray = new JSONArray();
        JSONArray dataFieldsArray = new JSONArray();
        JSONArray columnsArray = new JSONArray();
        List<String> columnsList = new ArrayList<>();

        try {

            String whereCondition = "";
            String pagenum = request.getParameter("pagenum");
            String pagesize = request.getParameter("pagesize") != null ? request.getParameter("pagesize") : "10";
            String recordendindex = request.getParameter("recordendindex");
            String recordstartindex = (request.getParameter("recordstartindex"));
            String gridId = (request.getParameter("gridId"));
            if (gridId != null
                    && !"".equalsIgnoreCase(gridId)
                    && !"null".equalsIgnoreCase(gridId)) {
                gridId = gridId.trim();
            }
            String getOnlyDataArray = (request.getParameter("getOnlyDataArray"));
            String columnsArrayStr = request.getParameter("columnsArray");
            if (columnsArrayStr != null
                    && !"".equalsIgnoreCase(columnsArrayStr)
                    && !"null".equalsIgnoreCase(columnsArrayStr)) {
                columnsList = (List<String>) JSONValue.parse(columnsArrayStr);
            }
//            String selectColumnsStr = request.getParameter("selectSapTableColumns"); // ravi start
//            JSONArray selectedColumns = new JSONArray();
//            if (selectColumnsStr != null) {
//                selectedColumns = (JSONArray) JSONValue.parse(selectColumnsStr);
//            }

            function = dataMigrationAccessDAO.getFunction("DDIF_FIELDINFO_GET");
            if (function != null && !"Y".equalsIgnoreCase(getOnlyDataArray)) {
                JCO.ParameterList listParams = function.getImportParameterList();
                listParams.setValue(tableName, "TABNAME");//For MARA
                fields = function.getTableParameterList().getTable("DFIES_TAB");
                connection.execute(function);
                String gridPersonalizeStr = "";
                if (fields != null) {

                    JSONObject dataFieldsObj = new JSONObject();
                    String columnName = fields.getString("FIELDNAME");
//                    if (selectedColumns != null && selectedColumns.contains(columnName)) {
                    dataFieldsObj.put("name", columnName);
                    dataFieldsObj.put("type", "string");

                    dataFieldsArray.add(dataFieldsObj);

                    JSONObject columnsObject = new JSONObject();
                    columnsObject.put("text", columnName);
                    columnsObject.put("datafield", columnName);
                    columnsObject.put("width", 120);
                    columnsObject.put("sortable", true);
                    columnsArray.add(columnsObject);

                    columnsList.add(columnName);
                    gridPersonalizeStr += "<tr>"
                            + "<td>"
                            + columnName
                            + "</td>"
                            + "<td>"
                            + "<input type='checkbox' data-gridid='" + gridId + "' checked id='" + gridId + "_" + columnName + "_DISPLAY' data-type='display' "
                            + " data-colname='" + columnName + "' onchange=\"updateETLPersonalize(id)\""
                            + "</td>"
                            + "<td>"
                            + "<input type='checkbox' id='" + gridId + "_" + columnName + "_FREEZE' data-gridid='" + gridId + "' data-type='pinned' "
                            + " data-colname='" + columnName + "' onchange=\"updateETLPersonalize(id)\""
                            + "</td>"
                            + "</tr>";
//                    }

                }
                while (fields.nextRow()) {

                    JSONObject dataFieldsObj = new JSONObject();
                    String columnName = fields.getString("FIELDNAME");
//                    if (selectedColumns != null && selectedColumns.contains(columnName)) {
                    dataFieldsObj.put("name", columnName);
                    dataFieldsObj.put("type", "string");

                    dataFieldsArray.add(dataFieldsObj);

                    JSONObject columnsObject = new JSONObject();
                    columnsObject.put("text", columnName);
                    columnsObject.put("datafield", columnName);
                    columnsObject.put("width", 120);
                    columnsObject.put("sortable", true);
                    columnsArray.add(columnsObject);

                    columnsList.add(columnName);
                    gridPersonalizeStr += "<tr>"
                            + "<td>"
                            + columnName
                            + "</td>"
                            + "<td>"
                            + "<input type='checkbox' data-gridid='" + gridId + "' checked id='" + gridId + "_" + columnName + "_DISPLAY' data-type='display' "
                            + " data-colname='" + columnName + "' onchange=\"updateETLPersonalize(id)\""
                            + "</td>"
                            + "<td>"
                            + "<input type='checkbox' id='" + gridId + "_" + columnName + "_FREEZE' data-gridid='" + gridId + "' data-type='pinned' "
                            + " data-colname='" + columnName + "' onchange=\"updateETLPersonalize(id)\""
                            + "</td>"
                            + "</tr>";
//                    }

                }
                gridPersonalizeStr = "<div class=\"personaliseoption visionSearchPersonaliseoption\" style=\"margin-top:5px;\">"
                        + "<div onclick=slideSettingsETL('" + gridId + "') class=\"layoutoptions ui-accordion\">"
                        + "<h3 class=\"ui-accordion-header1\"><span class=\"ui-accordion-header-icon ui-icon1 "
                        + " ui-icon-triangle-1-e ui-icon-triangle-1-s\" id=\"" + gridId + "_personalizeid\"></span>"
                        + "<img alt=\"\" class=\"navIcon gear\" src=\"images/f_spacer.gif\">Personalize</h3>"
                        + "</div><div id=\"" + gridId + "_settings_panel\" class=\"VisionETLSettings_panel\" style=\"display: none;\">"
                        + "<div class=\"personalize\" id=\"" + gridId + "_personalize_fields\"> <div class=\"pers_content\">"
                        + " <div id=\"tg-wrap4\" class=\"VisionETL-tg-wrap visionSearchPersonalise\"> "
                        + "<div class=\"visionPersonaliseSticky\"> <div class=\"sticky-wrap\"> "
                        + " <div class=\"sticky-wrap\">"
                        + "<table class=\"personalize_tbl sticky-enabled\" id=\"pers_criteria\" style=\"margin: 0px; width: 100%;\"> "
                        + "  <thead> <tr style=\"\"><th>Parameter</th><th>Display</th><th>Freeze</th>	   </tr>   </thead>  "
                        + " <tbody>"
                        + gridPersonalizeStr
                        + "</tbody>"
                        + "</table></div></div></div></div></div></div></div></div>";
                resultObj.put("gridPersonalizeStr", gridPersonalizeStr);

            }
            resultObj.put("columnList", columnsList);
            if (getOnlyDataArray != null && "Y".equalsIgnoreCase(getOnlyDataArray)) {
                if (columnsList.size() > 5) { // ravi etl integration 50->25
                    for (int k = 0; k < columnsList.size(); k += 5) {
                        int endIndex = k + 5 < columnsList.size() ? k + 5 : columnsList.size();
                        List colsSubList = columnsList.subList(k, endIndex);
                        if (colsSubList != null && !colsSubList.isEmpty()) {
                            if (k == 0) {
                                countFunction = dataMigrationAccessDAO.getFunction("RFC_READ_TABLE");
                                JCO.ParameterList countListParams = countFunction.getImportParameterList();
                                fields = countFunction.getTableParameterList().getTable("FIELDS");
                                countTableData = countFunction.getTableParameterList().getTable("DATA");
                                JCO.Table countOption = countFunction.getTableParameterList().getTable("OPTIONS");

                                for (int i = 0; i < colsSubList.size(); i++) {
                                    String columnName = (String) colsSubList.get(i);
                                    fields.appendRow();
                                    fields.setValue(columnName, "FIELDNAME");
                                }
                                countListParams.setValue(tableName, "QUERY_TABLE");//For MARA
                                countListParams.setValue("|", "DELIMITER");
                                if (request.getParameter("filterscount") != null) {
                                    int filterscount = new Integer(request.getParameter("filterscount"));
                                    whereCondition = sapFilterCondition(filterscount, request);
                                }
                                if (whereCondition != null && !"".equalsIgnoreCase(whereCondition)) {

                                    countOption.appendRow();
                                    countOption.setValue(whereCondition, "TEXT");
                                }

                                connection.execute(countFunction);
                                count = countTableData.getNumRows();

                            }

                            function = dataMigrationAccessDAO.getFunction("RFC_READ_TABLE");
                            if (function != null) {
                                JCO.ParameterList listParams = function.getImportParameterList();
                                fields = function.getTableParameterList().getTable("FIELDS");
                                tableData = function.getTableParameterList().getTable("DATA");
                                JCO.Table Option = function.getTableParameterList().getTable("OPTIONS");
                                for (int i = 0; i < colsSubList.size(); i++) {
                                    String columnName = (String) colsSubList.get(i);
                                    fields.appendRow();
                                    fields.setValue(columnName, "FIELDNAME");
                                }
                                listParams.setValue(tableName, "QUERY_TABLE");//For MARA
                                listParams.setValue("♥", "DELIMITER");
                                listParams.setValue(Integer.parseInt(recordstartindex), "ROWSKIPS");
                                listParams.setValue((Integer.parseInt(recordendindex) - Integer.parseInt(recordstartindex)), "ROWCOUNT");

                                if (whereCondition != null && !whereCondition.isEmpty()) {
                                    Option.appendRow();
                                    Option.setValue(whereCondition, "TEXT");
                                }

                                connection.execute(function);
                                int fetchCount = tableData.getNumRows();
                                if (dataArray != null
                                        && !dataArray.isEmpty() //&& dataArray.size() == fetchCount
                                        ) {
                                    int index = 0;
                                    if (fetchCount != 0) {
                                        Map dataObj = (Map) dataArray.get(index);
                                        if (colsSubList != null && !colsSubList.isEmpty()) {
                                            for (int i = 0; i < colsSubList.size(); i++) {
                                                String[] FieldValues = tableData.getString("WA").split("♥", -1);
                                                //dataObj.put(tableName + ":" + columnsList.get(i), FieldValues[i]);
                                                String value = FieldValues[i];
                                                if (value != null
                                                        && !"".equalsIgnoreCase(value)
                                                        && !"null".equalsIgnoreCase(value)) {
                                                    value = value.trim();
                                                }
                                                dataObj.put(colsSubList.get(i), value);
                                            }
                                            dataArray.remove(index);
                                            dataArray.add(index, dataObj);
                                        }
                                        index++;
                                    }
                                    while (tableData.nextRow()) {
                                        Map dataObj = (Map) dataArray.get(index);
                                        if (colsSubList != null && !colsSubList.isEmpty()) {
                                            for (int i = 0; i < colsSubList.size(); i++) {
                                                String[] FieldValues = tableData.getString("WA").split("♥", -1);
                                                //dataObj.put(tableName + ":" + columnsList.get(i), FieldValues[i]);
                                                String value = FieldValues[i];
                                                if (value != null
                                                        && !"".equalsIgnoreCase(value)
                                                        && !"null".equalsIgnoreCase(value)) {
                                                    value = value.trim();
                                                }
                                                dataObj.put(colsSubList.get(i), value);
                                            }
                                            dataArray.remove(index);
                                            dataArray.add(index, dataObj);
                                        }
                                        index++;
                                    }
                                } else {
                                    if (fetchCount != 0) {
                                        Map dataObj = new HashMap();
                                        if (colsSubList != null && !colsSubList.isEmpty()) {
                                            for (int i = 0; i < colsSubList.size(); i++) {
                                                String[] FieldValues = tableData.getString("WA").split("♥", -1);
                                                //dataObj.put(tableName + ":" + columnsList.get(i), FieldValues[i]);
                                                String value = FieldValues[i];
                                                if (value != null
                                                        && !"".equalsIgnoreCase(value)
                                                        && !"null".equalsIgnoreCase(value)) {
                                                    value = value.trim();
                                                }
                                                dataObj.put(colsSubList.get(i), value);
                                            }
                                            dataArray.add(dataObj);
                                        }
                                    }
                                    while (tableData.nextRow()) {
                                        Map dataObj = new HashMap();
                                        if (colsSubList != null && !colsSubList.isEmpty()) {
                                            for (int i = 0; i < colsSubList.size(); i++) {
                                                String[] FieldValues = tableData.getString("WA").split("♥", -1);
                                                //dataObj.put(tableName + ":" + columnsList.get(i), FieldValues[i]);
                                                String value = FieldValues[i];
                                                if (value != null
                                                        && !"".equalsIgnoreCase(value)
                                                        && !"null".equalsIgnoreCase(value)) {
                                                    value = value.trim();
                                                }
                                                dataObj.put(colsSubList.get(i), value);
                                            }
                                            dataArray.add(dataObj);
                                        }
                                    }
                                }

                            }

                        }

                    }
                    if (count != 0) {
                        dataArray.add(count);
                    }
                    resultObj.put("dataArray", dataArray);
                } else {
                    countFunction = dataMigrationAccessDAO.getFunction("RFC_READ_TABLE");
                    JCO.ParameterList countListParams = countFunction.getImportParameterList();
                    fields = countFunction.getTableParameterList().getTable("FIELDS");
                    countTableData = countFunction.getTableParameterList().getTable("DATA");
                    JCO.Table countOption = countFunction.getTableParameterList().getTable("OPTIONS");

                    for (int i = 0; i < columnsList.size(); i++) {
                        String columnName = (String) columnsList.get(i);
                        fields.appendRow();
                        fields.setValue(columnName, "FIELDNAME");
                    }
                    countListParams.setValue(tableName, "QUERY_TABLE");//For MARA
                    countListParams.setValue("|", "DELIMITER");
                    if (request.getParameter("filterscount") != null) {
                        int filterscount = new Integer(request.getParameter("filterscount"));
                        whereCondition = sapFilterCondition(filterscount, request);
                    }
                    if (whereCondition != null && !"".equalsIgnoreCase(whereCondition)) {

                        countOption.appendRow();
                        countOption.setValue(whereCondition, "TEXT");
                    }

                    connection.execute(countFunction);
                    count = countTableData.getNumRows();

                    function = dataMigrationAccessDAO.getFunction("RFC_READ_TABLE");
                    if (function != null) {
                        JCO.ParameterList listParams = function.getImportParameterList();
                        fields = function.getTableParameterList().getTable("FIELDS");
                        tableData = function.getTableParameterList().getTable("DATA");
                        JCO.Table Option = function.getTableParameterList().getTable("OPTIONS");
                        for (int i = 0; i < columnsList.size(); i++) {
                            String columnName = (String) columnsList.get(i);
                            fields.appendRow();
                            fields.setValue(columnName, "FIELDNAME");
                        }
                        listParams.setValue(tableName, "QUERY_TABLE");//For MARA
                        listParams.setValue("♥", "DELIMITER");
                        listParams.setValue(Integer.parseInt(recordstartindex), "ROWSKIPS");
                        listParams.setValue((Integer.parseInt(recordendindex) - Integer.parseInt(recordstartindex)), "ROWCOUNT");

                        if (whereCondition != null && !whereCondition.isEmpty()) {
                            Option.appendRow();
                            Option.setValue(whereCondition, "TEXT");
                        }

                        connection.execute(function);
                        // count = tableData.getNumRows();
                        int fetchCount = tableData.getNumRows();
                        if (fetchCount != 0) {
                            Map dataObj = new HashMap();
                            if (columnsList != null && !columnsList.isEmpty()) {
                                for (int i = 0; i < columnsList.size(); i++) {
                                    String[] FieldValues = tableData.getString("WA").split("♥", -1);
                                    String value = FieldValues[i];
                                    if (value != null
                                            && !"".equalsIgnoreCase(value)
                                            && !"null".equalsIgnoreCase(value)) {
                                        value = value.trim();
                                    }
                                    dataObj.put(columnsList.get(i), value);
                                }
                                dataArray.add(dataObj);
                            }
                        }
                        while (tableData.nextRow()) {
                            Map dataObj = new HashMap();
                            if (columnsList != null && !columnsList.isEmpty()) {
                                for (int i = 0; i < columnsList.size(); i++) {
                                    String[] FieldValues = tableData.getString("WA").split("♥", -1);
                                    String value = FieldValues[i];
                                    if (value != null
                                            && !"".equalsIgnoreCase(value)
                                            && !"null".equalsIgnoreCase(value)) {
                                        value = value.trim();
                                    }
                                    //dataObj.put(tableName + ":" + columnsList.get(i), FieldValues[i]);
                                    dataObj.put(columnsList.get(i), value);
                                }
                                dataArray.add(dataObj);
                            }
                        }
                    }

                    if (count != 0) {
                        dataArray.add(count);
                    }

                    resultObj.put("dataArray", dataArray);
                }

            } else {

                resultObj.put("columnsArray", columnsArray);

                resultObj.put("dataFieldsArray", dataFieldsArray);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public String sapFilterCondition(int filterscount, HttpServletRequest request) {
        String conditionQuery = "";
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");

            for (int i = 0; i < filterscount; i++) {
                String columnName = request.getParameter("filterdatafield" + i);
                String condition = request.getParameter("filtercondition" + i);
                String value = request.getParameter("filtervalue" + i);
                String filteroperator = request.getParameter("filteroperator" + i);

                value = value.toUpperCase();

                if (condition != null && !"".equalsIgnoreCase(condition)) {

                    String query = " " + columnName + " LIKE '%" + value + "%' ";
                    if (query != null && !"".equalsIgnoreCase(query)) {
                        conditionQuery += query;
                        if (i != filterscount - 1) {
                            conditionQuery += " AND ";
                        }
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return conditionQuery;
    }

    public JSONObject selectSapTableColumns(HttpServletRequest request, JCO.Client connection, String tableName) {
        JSONObject resultObj = new JSONObject();

        JCO.Function function = null;
        JSONArray columnsArray = new JSONArray();
        JCO.Table fields = null;
        try {
        	dataMigrationAccessDAO.retrieveRepository(connection);
            function = dataMigrationAccessDAO.getFunction("DDIF_FIELDINFO_GET");
            if (function != null) {
                JCO.ParameterList listParams = function.getImportParameterList();
                listParams.setValue(tableName, "TABNAME");//For MARA
                fields = function.getTableParameterList().getTable("DFIES_TAB");

                String filterColumn = request.getParameter("filterColumn");

                connection.execute(function);

                if (fields != null) {
                    if (filterColumn != null && !"".equalsIgnoreCase(filterColumn)) {
                        if (fields.getString("FIELDNAME") != null 
//                                && !fields.getString("FIELDNAME").contains("/") 
                                && fields.getString("FIELDNAME").contains(filterColumn.toUpperCase())) {
                            columnsArray.add(fields.getString("FIELDNAME"));
                        }

                    } else {
                        if (fields.getString("FIELDNAME") != null 
//                                && !fields.getString("FIELDNAME").contains("/")
                                ) {
                            columnsArray.add(fields.getString("FIELDNAME"));
                        }

                    }
                }
                while (fields.nextRow()) {
                    if (filterColumn != null && !"".equalsIgnoreCase(filterColumn)) {
                        if (fields.getString("FIELDNAME") != null 
//                                && !fields.getString("FIELDNAME").contains("/") 
                                && fields.getString("FIELDNAME").contains(filterColumn.toUpperCase())) {
                            columnsArray.add(fields.getString("FIELDNAME"));
                        }

                    } else {
                        if (fields.getString("FIELDNAME") != null 
//                                && !fields.getString("FIELDNAME").contains("/")
                                ) {
                            columnsArray.add(fields.getString("FIELDNAME"));
                        }
                    }
                }
                resultObj.put("columnsArray", columnsArray);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return resultObj;
    }



    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List getPredefinedSourceData(HttpServletRequest request) {

        List<Object[]> dataMapList = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        try {
            String query = "SELECT "
                    + "RECORD_NO, "
                    + "ERP_NO, "
                    + "CLASS_TERM, "
                    + "ERP_COMMENT, "
                    + "SHORT_DESR, "
                    + "LONG_DESR, "
                    + "INSTANCE, "
                    + "BUSINESS_UNIT, "
                    + "PLANT, "
                    + "RECORD_TYPE, "
                    + "RECORD_GROUP, "
                    + "UOM, "
                    + "HSN_CODE, "
                    + "STATUS, "
                    + "LOCALE, "
                    + "ABBREVIATION, "
                    + "SOURCE, "
                    + "DATA_SOURCE, "
                    + "CREATE_BY, "
                    + "CREATE_DATE, "
                    + "EDIT_BY, "
                    + "EDIT_DATE, "
                    + "CONCEPT_ID, "
                    + "CUSTOM_COLUMN15, "
                    + "REGION, "
                    + "DR_ID1 "
                    + "FROM ZZ_V_MM_CREATE WHERE STATUS = 'A12-APPROVED FOR ERP' AND ORGN_ID = 'C1F5CFB03F2E444DAE78ECCEAD80D27D'";

            //map.put("CLAUSE_ID", ClauseId);
            System.out.println(" query ::: " + query);
            dataMapList = access.sqlqueryWithParams(query, map);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return dataMapList;
    }

    @Transactional
    public List<Object[]> getEtlComponents(HttpServletRequest request) {
        List<Object[]> etlComponentsArray = new ArrayList<>();
        Map inputMap = new HashMap();
        try {
            String orgnId = (String) request.getSession(false).getAttribute("ssOrgId");
            String selectQuery = "SELECT  "
                    + " ORGN_ID,"//0
                    + " SEQUENCE_NO,"//1
                    + " DATA_TYPE,"//2
                    + " DATA_OPTITLE,"//3
                    + " DATA_FUN_NAME,"//4
                    + " IMAGE,"//5
                    + " DESCRIPTION,"//6
                    + " DIV_ID, "//7
                    + " COMPONENT_FLAG"//8
                    + " FROM C_ETL_COMPONENTS WHERE ACTIVE_FLAG='Y' AND ORGN_ID=:ORGN_ID ORDER BY SEQUENCE_NO ";
            System.out.println("selectQuery::::" + selectQuery);
            inputMap.put("ORGN_ID", orgnId);
            etlComponentsArray = access.sqlqueryWithParams(selectQuery, inputMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return etlComponentsArray;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
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

    @Transactional
    public List getTreeListOpt(HttpServletRequest request, String treeId) {
        List<Object[]> treeList = new ArrayList();
        try {
            JSONObject labelObj = new PilogUtilities().getMultilingualObject(request);
            String query = "SELECT   TREE.TREE_ID,"//0
                    + "         HIER.TREE_REF_TABLE,"//1
                    + "         TREE.TREE_DESCR,"//2
                    + "         TREE.THEME,"//3
                    + "         TREE.WIDTH,"//4
                    + "         TREE.HEIGHT,"//5
                    + "         TREE.SELECTION_TYPE,"//6
                    + "         TREE.ORGN_ID,"//7
                    + "         TREE.ROOT_DESCR,"//8
                    + "         HIER.ROLE_ID,"//9
                    + "        HIER.TREE_PARAMS_ID,"//10
                    + "         HIER.EDIT_FLAG,"//11
                    + "         HIER.SEQUENCE_NO,"//12
                    + "         HIER.HL_FLD_NAME,"//13
                    + "         HIER.FLD_NAME,"//14
                    + "         HIER.DISP_FLD_NAME,"//15
                    + "         HIER.FOLLOWUP_COMP_ID,"//16
                    + "         HIER.FOLLOWUP_COMP_TYPE,"//17
                    + "         HIER.FOLLOWOP_COMP_DESCR,"//18
                    + "         HIER.TREE_INIT_PARAMS,"//19
                    + "         HIER.TREE_HIER_CUST_COL1,"//20
                    + "         HIER.TREE_HIER_CUST_COL2,"//21
                    + "         HIER.TREE_HIER_CUST_COL3,"//22
                    + "         HIER.TREE_HIER_CUST_COL4,"//23
                    + "         HIER.TREE_HIER_CUST_COL5,"//24
                    + "         HIER.TREE_HIER_CUST_COL6,"//25
                    + "         HIER.TREE_HIER_CUST_COL7,"//26
                    + "         HIER.TREE_HIER_CUST_COL8,"//27
                    + "         HIER.TREE_HIER_CUST_COL9,"//28
                    + "         HIER.TREE_HIER_CUST_COL10,"//29
                    + "         HIER.TREE_HIER_CUST_COL11,"//30
                    + "         HIER.TREE_HIER_CUST_COL12,"//31
                    + "         HIER.TREE_HIER_CUST_COL13,"//32
                    + "         HIER.TREE_HIER_CUST_COL14,"//33
                    + "         HIER.TREE_HIER_CUST_COL15"//34
                    + "  FROM DAL_TREE TREE"
                    + " INNER JOIN"
                    + " DAL_TREE_ROLE_HIER HIER"
                    + " ON HIER.TREE_ID = TREE.TREE_ID"
                    + " WHERE TREE.ORGN_ID = :ORGN_ID"
                    + " AND TREE.TREE_ID = :TREE_ID"
                    + " AND HIER.ROLE_ID =:ROLE_ID ORDER BY HIER.SEQUENCE_NO";
            Map<String, Object> treeMap = new HashMap<>();
            treeMap.put("ORGN_ID", request.getSession(false).getAttribute("ssOrgId"));
            treeMap.put("ROLE_ID", request.getSession(false).getAttribute("ssRole"));
            treeMap.put("TREE_ID", treeId);
            System.out.println("query::" + query);
            System.out.println("treeMap:::" + treeMap);
            treeList = access.sqlqueryWithParams(query, treeMap);
            if (treeList != null && !treeList.isEmpty()) {
                treeList = treeList.stream().map(treeArray -> {
                    treeArray[2] = new PilogUtilities().convertIntoMultilingualValue(labelObj, treeArray[2]);
                    treeArray[8] = new PilogUtilities().convertIntoMultilingualValue(labelObj, treeArray[8]);
                    return treeArray;
                }).collect(Collectors.toList());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return treeList;
    }

    @Transactional
    public String getLOV(HttpServletRequest request, String lovName) {
        String lovString = "";
        try {
            HttpSession httpSession = request.getSession(false);
            String ssOrgnId = (String) httpSession.getAttribute("ssOrgId");
            JSONObject labelsObj = new PilogUtilities().getMultilingualObject(request);
            List lovValuesList = getLOVList(request, lovName);
            if (lovValuesList != null && !lovValuesList.isEmpty()) {
                for (int i = 0; i < lovValuesList.size(); i++) {
                    Object[] lovDataArray = (Object[]) lovValuesList.get(i);
                    if (lovDataArray != null && lovDataArray[0] != null) {
                        String selected = "";
                        if (lovDataArray[3] != null && "SQL".equalsIgnoreCase(String.valueOf(lovDataArray[3]))) {
                            String lovQuery = (String) lovDataArray[1];
                            if (lovQuery != null && lovQuery.contains("<<--") && lovQuery.contains("-->>")) {
//                                lovQuery = visionGenericDAO.replaceSessionValues(lovQuery, request);
                            }
                            System.out.println("lovQuery::::" + lovQuery);
                            List dataList = access.sqlqueryWithParams(lovQuery, new HashMap<>());
                            if (dataList != null && !dataList.isEmpty()) {
                                for (int j = 0; j < dataList.size(); j++) {
                                    Object dataObject = dataList.get(j);
                                    if (dataObject instanceof Object[]) {
                                        Object[] dataObjectArray = (Object[]) dataList.get(j);
                                        lovString += "<option value='" + (dataObjectArray[0] != null ? dataObjectArray[0] : "") + "' "
                                                + ">" + new PilogUtilities().convertIntoMultilingualValue(labelsObj, dataObjectArray[1]) + "</option>";

                                    } else {
                                        lovString += "<option value='" + (dataObject != null ? dataObject : "") + "' "
                                                + ">" + new PilogUtilities().convertIntoMultilingualValue(labelsObj, dataObject) + "</option>";
                                    }

                                }
                            }

                        } else {

                            lovString += "<option data-optlabel='" + new PilogUtilities().convertIntoMultilingualValue(labelsObj, lovDataArray[0]) + "' "
                                    + "value='" + (lovDataArray[1] != null ? lovDataArray[1] : "") + "'"
                                    + " " + selected + ">" + new PilogUtilities().convertIntoMultilingualValue(labelsObj, lovDataArray[0]) + "</option>";

                        }
                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return lovString;
    }

    @Transactional
    public List getLOVList(HttpServletRequest request, String lovName) {
        List lovValuesList = new ArrayList();
        try {
            HttpSession httpSession = request.getSession(false);
            String ssOrgnId = (String) httpSession.getAttribute("ssOrgId");
            JSONObject labelsObj = new PilogUtilities().getMultilingualObject(request);
            String lovQuery = " SELECT DISPLAY,PROCESS_VALUE,DEFAULT_FLAG,DATA_TYPE FROM DAL_DLOV WHERE DLOV_NAME =:DLOV_NAME AND ORGN_ID =:ORGN_ID ORDER BY SEQUENCE_NO";
            Map<String, Object> lovMap = new HashMap<>();
            lovMap.put("DLOV_NAME", lovName);
            lovMap.put("ORGN_ID", ssOrgnId);
            lovValuesList = access.sqlqueryWithParams(lovQuery, lovMap);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return lovValuesList;
    }

    @Transactional
    public Timestamp getCurrentDBDate() {
        Timestamp sqlDate = null;
        try {
        	String sqlDateQuery = "";
        	if (dataBaseDriver.contains("oracle")) {
        		 sqlDateQuery = "SELECT SYSDATE FROM DUAL";
        	} else if (dataBaseDriver.contains("mysql")) {
        		 sqlDateQuery = "SELECT CURRENT_TIMESTAMP FROM DUAL";
        	} else if (dataBaseDriver.contains("SQLSERVER")) {
       		 sqlDateQuery = "SELECT CURRENT_TIMESTAMP FROM DUAL";
        	}
            
            List dateList = access.executeNativeSQL(sqlDateQuery, new HashMap());
            sqlDate = (Timestamp) dateList.get(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sqlDate;
    }
    
     @Transactional
    public List<Object[]> getSourceConnectionsList(HttpServletRequest request, String parentMenuId) {
        List<Object[]> sourceConnectionsList = new ArrayList();
         try {
        	 String subscriptionType = (String) request.getSession(false).getAttribute("ssSubscriptionType");
            String sqlQuery = "SELECT "
                                    + "ORGN_ID, "  // 0
                                    + "ROLE_ID, " // 1
                                    + "CONNECTION_ID, "// 2
                                    + "CONNECTION_NAME, "// 3
                                    + "CONNECTION_DESC, "// 4
                                    + "CONNECTION_IMAGE, "// 5
                                    + "PARENT_CONNECTION_ID, "// 6
                                    + "FUNCTION_NAME, "// 7
                                    + "TOOL_TIP, "// 8
                                    + "SEQUENCE_NO, "// 9
                                    + "ACTIVE_FLAG, "// 10
                                    + "ICON_SIZE "// 11
                                    + " FROM C_SOURCE_CONNECTIONS "
                                    + " WHERE "
//                                    + " ORGN_ID='"+request.getSession(false).getAttribute("ssOrgId")+"' "
                                    + " ROLE_ID='"+request.getSession(false).getAttribute("ssRole")+"' "
                                    + " AND ACTIVE_FLAG='Y'"
                                    + " AND UPPER(SUBSCRIPTION_TYPE) LIKE '%" + subscriptionType.toUpperCase() + "%'"
                                    + " ORDER BY SEQUENCE_NO";
           sourceConnectionsList = access.executeNativeSQL(sqlQuery, new HashMap());
            
        } catch (Exception e) {
            e.printStackTrace();
        }
     return sourceConnectionsList;
    }
    
    
    @Transactional
    public List<Object[]> provideDALAuthorisation(HttpServletRequest request, String tableName,String userName) {
        List<Object[]> sourceConnectionsList = new ArrayList();
         try {
        	 
            String insertQuery = "INSERT INTO C_ETL_DAL_AUTHORIZATION (TABLE_NAME, CREATE_BY) VALUES ('"+tableName+"','"+userName+"')";
            int insertCount = access.executeUpdateSQLNoAudit(insertQuery, Collections.EMPTY_MAP);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
     return sourceConnectionsList;
    }
    
  
    
    @Transactional
    public List<Object[]> fetchSubJobProcessLog(String userName, String orgnId, String jobId, String subJobId) {
        List<Object[]> processlogDataList = new ArrayList<>();
        try {
        	
            String selectQuery = "SELECT LOG_TYPE, "//0
                    + "LOG_TXT, "//1
                    + "CREATE_DATE,"//2
                    + "PROCESS_FLAG, "//3
                    + "SEQUENCE_NO, "//4
                    + "DM_LOG_CUST_COL2, "//5
                    + "DM_LOG_CUST_COL3, "//6
                    + "DM_LOG_CUST_COL4, "//7
                    + "DM_LOG_CUST_COL5 "//8
                    + " FROM RECORD_DM_PROCESS_LOG"
                    + " WHERE USER_NAME =:USER_NAME AND ORGN_ID =:ORGN_ID AND DM_LOG_CUST_COL1 IS NULL AND JOB_ID =:JOB_ID";
            
                    //+ " AND SUB_JOB_ID =:SUB_JOB_ID";
            
            selectQuery += ""
                    + " ORDER BY CREATE_DATE,SEQUENCE_NO";
            Map selectMap = new HashMap();
            selectMap.put("USER_NAME", userName);
            selectMap.put("ORGN_ID", orgnId);
            selectMap.put("JOB_ID", jobId);
         //   selectMap.put("SUB_JOB_ID", subJobId);
//            System.out.println("selectQuery:getProcesslog::"+selectQuery);
//            System.out.println("selectMap:getProcesslog::"+selectMap);
            processlogDataList = access.sqlqueryWithParams(selectQuery, selectMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return processlogDataList;
    }
    
   
    @Transactional
    public List<String> fetchMaskedColumnsList(HttpServletRequest request, String tableName, String host, String ssUsername){
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
             if (columnsList!=null && !columnsList.isEmpty()) {
            	 columnsList = Arrays.asList(columnsList.get(0).split(","));
             }
             
         } catch (Exception e) {
             e.printStackTrace();
         }
         return columnsList;
    }
    
    public JSONObject getDocumentData(HttpServletRequest request, String parentkeyData, String connObj, MongoClient client) {

        JSONObject resultObj = new JSONObject();
        JSONArray dataArray = new JSONArray();
        JSONArray dataFieldsArray = new JSONArray();
        JSONArray columnsArray = new JSONArray();

        try {
        	JSONObject connectionObj = (JSONObject) JSONValue.parse(connObj);
        	String databaseName = (String) connectionObj.get("CONN_DB_NAME");
        	String gridId = request.getParameter("gridId");
        	MongoDatabase database = client.getDatabase(databaseName);
        	MongoCollection<Document> collection = database.getCollection(parentkeyData);
        	
        	int pageSize = request.getParameter("pagesize") != null ? Integer.parseInt(request.getParameter("pagesize")) : 10; // Set your desired page size

            // Get the requested page number or default to 1 if not provided
            int pageNum = request.getParameter("pagenum") != null ?
                    Integer.parseInt(request.getParameter("pagenum")) : 1;

            // Skip documents based on pagination
           int skipCount = (pageNum - 1) * pageSize;
           int recordsCount = 0;
           
           
           Integer filterscount = 0;
           Document filterCriteria = null;
           if (request.getParameter("filterscount") != null && !"0".equalsIgnoreCase(request.getParameter("filterscount")) ) {
				filterscount = new Integer(request.getParameter("filterscount"));
				filterCriteria = constructFilterCriteria(request, filterscount);
           }
           
           FindIterable<Document> findIterable;

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
           
            //JSONObject columnsDataTypeObj = new JSONObject();
            if (request.getParameter("getOnlyDataArray") != null && "Y".equalsIgnoreCase(request.getParameter("getOnlyDataArray"))) {
                for (Document document : findIterable) {
                    JSONObject dataObj = new JSONObject();
                    for (String columnName : document.keySet()) {
                    	if(!(columnName.equalsIgnoreCase("_id"))) {
                        Object data = document.get(columnName);
                        dataObj.put(columnName, data);
                    	}
                    }
                    dataArray.add(dataObj);
                }
                if (recordsCount != 0) {
					dataArray.add(recordsCount);
				}
                resultObj.put("dataArray", dataArray);
            } else {
                String host = connectionObj.get("HOST_NAME") + ":" + connectionObj.get("CONN_PORT");
                String ssUsername = (String) request.getSession(false).getAttribute("ssUsername");
                List<String> maskedColumnsList = fetchMaskedColumnsList(request, parentkeyData, host, ssUsername);
                String gridPersonalizeStr = "";

                FindIterable<Document> documents = collection.find().limit(1);
                Document firstDocument = documents.first();

                if (firstDocument != null) {
                    for (Map.Entry<String, Object> entry : firstDocument.entrySet()) {
                        String columnName = entry.getKey();
                        
                        if(!(columnName.equalsIgnoreCase("_id"))) {
                        
                        JSONObject dataFieldsObj = new JSONObject();
                        dataFieldsObj.put("name", columnName);
                        dataFieldsObj.put("type", "string");
                        dataFieldsArray.add(dataFieldsObj);

                        JSONObject columnsObject = new JSONObject();
                        //columnsDataTypeObj.put(columnName, "Assuming String Type for MongoDB"); // Change this based on actual type

                        columnsObject.put("text", columnName);
                        columnsObject.put("datafield", columnName);
                        columnsObject.put("width", 120);
                        columnsObject.put("sortable", true);

                        if (maskedColumnsList.contains(columnName)) {
                            columnsObject.put("cellsrenderer", "maskcolumnRenderer");
                            columnsObject.put("editable", false);
                        }

                        if (columnName.equalsIgnoreCase("ROWNUM")) {
                            columnsObject.put("hidden", true);
                        }

                        // Additional configurations for columns can be added here
                        // For example, handling specific column types or adding more properties

                        columnsArray.add(columnsObject);
                        gridPersonalizeStr += "<tr>" + "<td>" + columnName + "</td>" + "<td>"
                                + "<input type='checkbox' data-gridid='" + gridId + "' checked id='" + gridId + "_"
                                + columnName + "_DISPLAY' data-type='display' " + " data-colname='" + columnName
                                + "' onchange=\"updateETLPersonalize(id)\"" + "</td>" + "<td>"
                                + "<input type='checkbox' id='" + gridId + "_" + columnName + "_FREEZE' data-gridid='"
                                + gridId + "' data-type='pinned' " + " data-colname='" + columnName
                                + "' onchange=\"updateETLPersonalize(id)\"" + "</td>" + "</tr>";
                    }
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

                JSONObject dataFieldsObjHidden = new JSONObject();
                dataFieldsObjHidden.put("name", parentkeyData + "_HIDDEN");
                dataFieldsObjHidden.put("type", "string");
                dataFieldsArray.add(dataFieldsObjHidden);

                JSONObject columnsObjecthidden = new JSONObject();
                columnsObjecthidden.put("text", parentkeyData + "_HIDDEN");
                columnsObjecthidden.put("datafield", parentkeyData + "_HIDDEN");
                columnsObjecthidden.put("hidden", true);
                columnsArray.add(columnsObjecthidden);

                resultObj.put("dataFieldsArray", dataFieldsArray);
                resultObj.put("columnsArray", columnsArray);
                resultObj.put("gridPersonalizeStr", gridPersonalizeStr);
                //resultObj.put("columnsDataTypeObj", columnsDataTypeObj);

                // Assuming primary keys are not applicable in MongoDB, you may adjust based on your requirements
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
       // Close the MongoClient in a finally block to ensure it gets closed even if an exception occurs
       			if (client != null) {
       				client.close();
       			}
       		}

        return resultObj;
    }
    
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Document constructFilterCriteria(HttpServletRequest request, int filterscount) {
        Document filterCriteria = new Document();
        

        for (int i = 0; i < filterscount; i++) {
            String columnName = request.getParameter("filterdatafield" + i);
            String condition = request.getParameter("filtercondition" + i);
            String value = request.getParameter("filtervalue" + i);
            String filteroperator = request.getParameter("filteroperator" + i);

            // Assuming value is always in uppercase
            value = value.toUpperCase();

            if (columnName.contains("DATE")) {
                // Adjust the date handling based on your MongoDB date format
                // Here, we assume date is stored as a string in the format 'YYYY-MM-DD'
                // Modify this part based on how your dates are stored in MongoDB
                value = value.substring(0, value.indexOf("GMT") - 9).trim();
                filterCriteria.append(columnName, value);
            } else {
                // Adjust the conditions based on your MongoDB requirements
                switch (condition) {
                    case "CONTAINS":
                        filterCriteria.append(columnName, new Document("$regex", ".*" + value + ".*"));
                        break;

                    case "DOES_NOT_CONTAIN":
                        filterCriteria.append(columnName, new Document("$not", new Document("$regex", ".*" + value + ".*")));
                        break;

                    case "EQUAL":
                        filterCriteria.append(columnName, value);
                        break;

                    // ... (other conditions)

                    default:
                        // Handle other conditions as needed
                        break;
                }
            }
        }

        return filterCriteria;
    }
    @Transactional
    public JSONObject getDriveApiDetails(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {

            String query = "SELECT "
                    + "KEYNAME, "//0
                    + "PROCESS_VALUE " //1
                    + "FROM B_APPL_PROPERTIES "
                    + "WHERE KEYNAME IN ('DRIVE_API_KEY', 'DRIVE_CLIENT_ID', 'DRIVE_APP_ID', 'DRIVE_DISCOVERY_DOC', 'DRIVE_SCOPES')";
            System.out.println(" query ::: " + query);
            List<Object[]> listResultObj = access.sqlqueryWithParams(query, Collections.EMPTY_MAP);
            if (listResultObj != null && !listResultObj.isEmpty()) {
                for (Object[] row : listResultObj) {
                    String keyName = (String) row[0];
                    String processValue = (String) row[1];
                    resultObj.put(keyName, processValue);
                }
            }
            //System.out.println(listResultObj);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;

    }
}
