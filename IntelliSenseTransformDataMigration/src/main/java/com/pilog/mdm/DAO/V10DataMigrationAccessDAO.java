/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.DAO;


import com.pilog.mdm.transformutills.V10DataPipingUtills;
import com.pilog.mdm.utilities.PilogUtilities;
import com.sap.mw.jco.IRepository;
import com.sap.mw.jco.JCO;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.util.StringUtils;

/**
 *
 * @author PiLog
 */
public class V10DataMigrationAccessDAO {

    private IRepository theRepository;

    public String getOracleDatabaseTables(Connection connection, String serviceName, String filterTableVal) {

        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        JSONArray tablesArray = new JSONArray();
        String tableDataStr = "";
        try {
            String query = "";
            if (filterTableVal != null && !"".equalsIgnoreCase(filterTableVal) && !"null".equalsIgnoreCase(filterTableVal)) {
                filterTableVal = filterTableVal.toUpperCase();
                query = "SELECT DISTINCT TABLE_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME LIKE '%" + filterTableVal + "%' "
                        + "ORDER BY TABLE_NAME OFFSET 0 ROWS FETCH NEXT 20 ROWS ONLY";
//                query = "SELECT * FROM (SELECT ROWNUM AS RNO,TABLE_NAME FROM (SELECT DISTINCT TABLE_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME LIKE '%" + filterTableVal + "%' ORDER BY TABLE_NAME )) WHERE RNO BETWEEN 0 AND 20";

            } else {
                query = "SELECT DISTINCT TABLE_NAME FROM USER_TAB_COLUMNS ORDER BY TABLE_NAME OFFSET 0 ROWS FETCH NEXT 20 ROWS ONLY ";
//                query = "SELECT * FROM (SELECT ROWNUM AS RNO,TABLE_NAME FROM (SELECT DISTINCT TABLE_NAME FROM USER_TAB_COLUMNS ORDER BY TABLE_NAME)) WHERE RNO BETWEEN 0 AND 20";

            }
            System.out.println("query:::" + query);
            preparedStatement = connection.prepareStatement(query);
            resultSet = preparedStatement.executeQuery();
            int i = 0;
            while (resultSet.next()) {
                tableDataStr += "<div id='TABLE_" + resultSet.getString("TABLE_NAME").toUpperCase() + "' class='visionTableName' title='" + (resultSet.getString("TABLE_NAME") != null ? resultSet.getString("TABLE_NAME").toUpperCase() : "") + "' data-table-name = '"
                        + "" + (resultSet.getString("TABLE_NAME") != null ? resultSet.getString("TABLE_NAME").toUpperCase() : "") + "'>" + (resultSet.getString("TABLE_NAME") != null ? resultSet.getString("TABLE_NAME").toUpperCase() : "") + "<img src=\"images/crossicon.png\"   title='Clear Data' onclick='moveToSource(TABLE_" + resultSet.getString("TABLE_NAME").toUpperCase() + ")' class='visionCloseDestTableBtn'></div>";
                i++;
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
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
            }
        }
        return tableDataStr;
    }

    public String getMYSQLDatabaseTables(Connection connection, String serviceName, String filterTableVal) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        JSONArray tablesArray = new JSONArray();
        String tableDataStr = "";
        try {
            //SELECT TABLE_NAME,COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='TEST_DATABASE' AND TABLE_NAME ='B_CITY' ORDER BY COLUMN_NAME
            //String query = "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + serviceName + "' LIMIT 0,20 ";
            String query = "";
            if (filterTableVal != null && !"".equalsIgnoreCase(filterTableVal) && !"null".equalsIgnoreCase(filterTableVal)) {
                filterTableVal = filterTableVal.toUpperCase();
                query = "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + serviceName + "' AND TABLE_NAME LIKE '%" + filterTableVal + "%'  LIMIT 0,20";

            } else {
                query = "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + serviceName + "'  LIMIT 0,20";

            }

            preparedStatement = connection.prepareStatement(query);
            resultSet = preparedStatement.executeQuery();
            int i = 0;
            while (resultSet.next()) {
                tableDataStr += "<div id='TABLE_" + resultSet.getString("TABLE_NAME").toUpperCase() + "' class='visionTableName' title='" + (resultSet.getString("TABLE_NAME") != null ? resultSet.getString("TABLE_NAME").toUpperCase() : "") + "' data-table-name = '"
                        + "" + (resultSet.getString("TABLE_NAME") != null ? resultSet.getString("TABLE_NAME").toUpperCase() : "") + "'>" + (resultSet.getString("TABLE_NAME") != null ? resultSet.getString("TABLE_NAME").toUpperCase() : "") + "<img src=\"images/crossicon.png\"   title='Clear Data' onclick='moveToSource(TABLE_" + resultSet.getString("TABLE_NAME").toUpperCase() + ")' class='visionCloseDestTableBtn'></div>";
                i++;
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
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
            }
        }
        return tableDataStr;
    }

    public String getMSSQLDatabaseTables(Connection connection, String serviceName, String filterTableVal) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        JSONArray tablesArray = new JSONArray();
        String tableDataStr = "";
        try {
            //SELECT TABLE_NAME,COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='TEST_DATABASE' AND TABLE_NAME ='B_CITY' ORDER BY COLUMN_NAME
            // String query = "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + serviceName + "'";
            String query = "";
            if (filterTableVal != null && !"".equalsIgnoreCase(filterTableVal) && !"null".equalsIgnoreCase(filterTableVal)) {
                filterTableVal = filterTableVal.toUpperCase();
                query = "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + serviceName + "' AND TABLE_NAME LIKE '%" + filterTableVal + "%' ORDER BY TABLE_NAME  OFFSET 0 ROWS FETCH NEXT 20 ROWS ONLY";
            } else {
                query = "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + serviceName + "' ORDER BY TABLE_NAME OFFSET 0 ROWS FETCH NEXT 20 ROWS ONLY";

            }

            preparedStatement = connection.prepareStatement(query);
            resultSet = preparedStatement.executeQuery();
            int i = 0;
            while (resultSet.next()) {
                tableDataStr += "<div id='TABLE_" + resultSet.getString("TABLE_NAME").toUpperCase() + "' class='visionTableName' title='" + (resultSet.getString("TABLE_NAME") != null ? resultSet.getString("TABLE_NAME").toUpperCase() : "") + "' data-table-name = '"
                        + "" + (resultSet.getString("TABLE_NAME") != null ? resultSet.getString("TABLE_NAME").toUpperCase() : "") + "'>" + (resultSet.getString("TABLE_NAME") != null ? resultSet.getString("TABLE_NAME").toUpperCase() : "") + "<img src=\"images/crossicon.png\"   title='Clear Data' onclick='moveToSource(TABLE_" + resultSet.getString("TABLE_NAME").toUpperCase() + ")' class='visionCloseDestTableBtn'></div>";
                i++;
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
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
            }
        }
        return tableDataStr;
    }

    public List<Object[]> getOracleTableColumns(Connection connection, HttpServletRequest request, String serviceName) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        List<Object[]> sourceColumnsList = new ArrayList<>();
        try {

            String tablesValues = request.getParameter("tablesArray");
            if (tablesValues != null && !"".equalsIgnoreCase(tablesValues) && !"null".equalsIgnoreCase(tablesValues)) {
                JSONArray tablesValuesArr = (JSONArray) JSONValue.parse(tablesValues);
                List tablesList = new ArrayList();
                tablesList.addAll(tablesValuesArr);
                String tableStr = StringUtils.collectionToDelimitedString(tablesList, "','");
                System.out.println("tableStr:::" + tableStr);
                String query = "SELECT DISTINCT TABLE_NAME,COLUMN_NAME,DATA_TYPE,DATA_LENGTH FROM USER_TAB_COLUMNS WHERE "
                        + " TABLE_NAME IN ('" + tableStr + "') ORDER BY TABLE_NAME";
                preparedStatement = connection.prepareStatement(query);
                resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    Object[] columnsArray = new Object[4];
                    columnsArray[0] = (resultSet.getString("TABLE_NAME") != null ? resultSet.getString("TABLE_NAME").toUpperCase() : "");
                    columnsArray[1] = resultSet.getString("COLUMN_NAME");
                    columnsArray[2] = resultSet.getString("DATA_TYPE");
                    columnsArray[3] = resultSet.getString("DATA_LENGTH");
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

    public List<Object[]> getMYSQLTableColumns(Connection connection, HttpServletRequest request, String serviceName) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        List<Object[]> sourceColumnsList = new ArrayList<>();
        try {

            String tablesValues = request.getParameter("tablesArray");
            if (tablesValues != null && !"".equalsIgnoreCase(tablesValues) && !"null".equalsIgnoreCase(tablesValues)) {
                JSONArray tablesValuesArr = (JSONArray) JSONValue.parse(tablesValues);
                List tablesList = new ArrayList();
                tablesList.addAll(tablesValuesArr);
                String tableStr = StringUtils.collectionToDelimitedString(tablesList, "','");
                System.out.println("tableStr:::" + tableStr);
                //SELECT TABLE_NAME,COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='TEST_DATABASE' AND TABLE_NAME IN ('" + tableStr + "') ORDER BY TABLE_NAME
                String query = "SELECT DISTINCT TABLE_NAME,COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + serviceName + "' "
                        + "AND TABLE_NAME IN ('" + tableStr + "') ORDER BY TABLE_NAME";
                preparedStatement = connection.prepareStatement(query);
                resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    Object[] columnsArray = new Object[4];
                    columnsArray[0] = (resultSet.getString("TABLE_NAME") != null ? resultSet.getString("TABLE_NAME").toUpperCase() : "");
                    columnsArray[1] = resultSet.getString("COLUMN_NAME");
                    columnsArray[2] = resultSet.getString("DATA_TYPE");
                    columnsArray[3] = resultSet.getString("CHARACTER_MAXIMUM_LENGTH");
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

    public List<Object[]> getSQLSERVERTableColumns(Connection connection, HttpServletRequest request, String serviceName) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        List<Object[]> sourceColumnsList = new ArrayList<>();
        try {

            String tablesValues = request.getParameter("tablesArray");
            if (tablesValues != null && !"".equalsIgnoreCase(tablesValues) && !"null".equalsIgnoreCase(tablesValues)) {
                JSONArray tablesValuesArr = (JSONArray) JSONValue.parse(tablesValues);
                List tablesList = new ArrayList();
                tablesList.addAll(tablesValuesArr);
                String tableStr = StringUtils.collectionToDelimitedString(tablesList, "','");
                System.out.println("tableStr:::" + tableStr);
                //SELECT TABLE_NAME,COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='TEST_DATABASE' AND TABLE_NAME IN ('" + tableStr + "') ORDER BY TABLE_NAME
                String query = "SELECT DISTINCT TABLE_NAME,COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + serviceName + "' "
                        + "AND TABLE_NAME IN ('" + tableStr + "') ORDER BY TABLE_NAME";
                preparedStatement = connection.prepareStatement(query);
                resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    Object[] columnsArray = new Object[4];
                    columnsArray[0] = (resultSet.getString("TABLE_NAME") != null ? resultSet.getString("TABLE_NAME").toUpperCase() : "");
                    columnsArray[1] = resultSet.getString("COLUMN_NAME");
                    columnsArray[2] = resultSet.getString("DATA_TYPE");
                    columnsArray[3] = resultSet.getString("CHARACTER_MAXIMUM_LENGTH");
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

    public List getOracleSelectedColumnsData(Connection connection,
            //JSONObject tableColsObj,
            Map tableColsObj,// ravi etl integration
            Integer start,
            Integer limit,
            JSONObject tablesObj,
            String serviceName) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        String result = "";
        List totalData = new ArrayList();

        try {
            if (tableColsObj != null && !tableColsObj.isEmpty()) {
                for (Object tableName : tableColsObj.keySet()) {
                    if (tableName != null && tableColsObj.get(tableName) != null) {
                        String colsObj = (String) tableColsObj.get(tableName);
                        if (colsObj != null && !colsObj.isEmpty()) {
                            String query = "SELECT " + colsObj;

//                            String query = "SELECT ROWNUM AS RNO," + colsObj;
                            List<String> columnsList = new ArrayList(Arrays.asList(colsObj.split(",")));
                            query += " FROM " + tableName + " ";
                            if (tablesObj != null && !tablesObj.isEmpty()) {
                                String tableInputVal = (String) tablesObj.get(tableName);
                                if (tableInputVal != null && !"".equalsIgnoreCase(tableInputVal) && !"null".equalsIgnoreCase(tableInputVal)) {
                                    query += " WHERE " + tableInputVal + "";
                                }
                            }
                            int end = 0;
                            if (start == 0) {
                                end = limit;
                            } else {
                                end = (start + limit - 1);
                            }
                            query += " OFFSET " + (start - 1) + " ROWS FETCH NEXT " + limit + " ROWS ONLY";
//                            query = "SELECT * FROM (" + query + ") WHERE RNO BETWEEN " + start + " AND " + (end) + "";
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

    public List getMYSQLSelectedColumnsData(Connection connection,
            //JSONObject tableColsObj,
            Map tableColsObj,// ravi etl integration
            Integer start, Integer limit, JSONObject tablesObj, String serviceName) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        String result = "";
        List totalData = new ArrayList();

        try {
            if (tableColsObj != null && !tableColsObj.isEmpty()) {
                for (Object tableName : tableColsObj.keySet()) {
                    if (tableName != null && tableColsObj.get(tableName) != null) {
                        String colsObj = (String) tableColsObj.get(tableName);
                        if (colsObj != null && !colsObj.isEmpty()) {
                            // String query = "SELECT @row_number:=@row_number+1 AS RNO," + colsObj;
                            List<String> columnsList = new ArrayList(Arrays.asList(colsObj.split(",")));
                            String query = "";
                            if (tablesObj != null && !tablesObj.isEmpty()) {
                                String tableInputVal = (String) tablesObj.get(tableName);
                                query += "SELECT " + colsObj + " FROM " + tableName + "";
                                if (tableInputVal != null && !"".equalsIgnoreCase(tableInputVal) && !"null".equalsIgnoreCase(tableInputVal)) {
                                    query += " WHERE " + tableInputVal + " ";
                                }
                                query += " LIMIT " + (start - 1) + " , " + limit + " ";
                            }

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

    public List getMSSQLSelectedColumnsData(Connection connection,
            //JSONObject tableColsObj,
            Map tableColsObj,// ravi etl integration
            Integer start,
            Integer limit, JSONObject tablesObj, String serviceName) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        String result = "";
        List totalData = new ArrayList();

        try {
            if (tableColsObj != null && !tableColsObj.isEmpty()) {
                for (Object tableName : tableColsObj.keySet()) {
                    if (tableName != null && tableColsObj.get(tableName) != null) {
                        String colsObj = (String) tableColsObj.get(tableName);
                        if (colsObj != null && !colsObj.isEmpty()) {
                            // String query = "SELECT @row_number:=@row_number+1 AS RNO," + colsObj;
                            List<String> columnsList = new ArrayList(Arrays.asList(colsObj.split(",")));
                            String query = "";
                            if (tablesObj != null && !tablesObj.isEmpty()) {
                                String tableInputVal = (String) tablesObj.get(tableName);
                                query += "SELECT " + colsObj + " FROM " + tableName + "";
                                if (tableInputVal != null && !"".equalsIgnoreCase(tableInputVal) && !"null".equalsIgnoreCase(tableInputVal)) {
                                    query += " WHERE " + tableInputVal + " ";
                                }
                                query += " ORDER BY " + columnsList.get(0) + " OFFSET " + (start - 1) + " ROWS FETCH NEXT " + limit + " ROWS ONLY";
                            }

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

    // ravi new mwthod
    public List getOracle_ERPSelectedColumnsData(Connection connection,
            //JSONObject tableColsObj,
            Map tableColsObj,// ravi etl integration
            Integer start,
            Integer limit,
            JSONObject tablesObj,
            String serviceName,
            String loginUserName,
            String loginOrgnId,
            String dataBaseDriver,
            String dbURL,
            String dbUserName,
            String dbPassword,
            JSONObject orderAndGroupByObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj,
            Connection toConnection) {

        return getOracleSelectedColumnsData(connection,
                tableColsObj, start, limit, tablesObj, serviceName,
                loginUserName, loginOrgnId, dataBaseDriver, dbURL,
                dbUserName, dbPassword, orderAndGroupByObj,
                columnClauseObj, selectTabObj, toConnection);
    }

    public List getOracleSelectedColumnsData(Connection connection,
            //JSONObject tableColsObj,
            Map tableColsObj,// ravi etl integration
            Integer start,
            Integer limit,
            JSONObject tablesObj,
            String serviceName,
            String loginUserName,
            String loginOrgnId,
            String dataBaseDriver,
            String dbURL,
            String dbUserName,
            String dbPassword,
            JSONObject orderAndGroupByObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj,
            Connection toConnection) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        String result = "";
        List totalData = new ArrayList();
        JSONObject lastSeqObj = new JSONObject();
        try {
            if (tableColsObj != null && !tableColsObj.isEmpty()) {
                for (Object tableName : tableColsObj.keySet()) {
                    if (tableName != null && tableColsObj.get(tableName) != null) {
                        String colsObj = (String) tableColsObj.get(tableName);
                        if (colsObj != null && !colsObj.isEmpty()) {
//                            String rowNumStr = "ROWNUM AS RNO,";
                            String query = "SELECT ";
                            if ((selectTabObj != null
                                    && !selectTabObj.isEmpty()
                                    && "Y".equalsIgnoreCase(String.valueOf(selectTabObj.get("uniqueRowsFlag"))))) {//uniqueRowsFlag
                                query += " DISTINCT ";
                            }
                            String fetchColumns = "";
                            List<String> columnsList = new ArrayList(Arrays.asList(colsObj.split(",")));
                            Set<String> items = new HashSet<>();
                            Set matchedColsSet = columnsList.stream()
                                    .filter(n -> !items.add(n)) // Set.add() returns false if the element was already in the set.
                                    .collect(Collectors.toSet());

                            for (int j = 0; j < columnsList.size(); j++) {
                                if (matchedColsSet != null && !matchedColsSet.isEmpty() && matchedColsSet.contains(columnsList.get(j))) {
                                    fetchColumns += tableName + "." + columnsList.get(j) + " AS " + columnsList.get(j) + "_0" + j;
                                } else {
                                    fetchColumns += tableName + "." + columnsList.get(j);
                                }
                                if (j != columnsList.size() - 1) {
                                    fetchColumns += ",";
                                }
                            }
                            columnsList = new ArrayList(Arrays.asList(fetchColumns.split(",")));
                            query += " " + fetchColumns;
                            if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
                                for (Object toColumnName : columnClauseObj.keySet()) {
                                    if (columnClauseObj.get(toColumnName) instanceof JSONObject) {
                                        JSONObject funObj = (JSONObject) columnClauseObj.get(toColumnName);
                                        if (funObj != null
                                                && !funObj.isEmpty()) {
                                            String functionMainObjStr = (String) funObj.get("funobjstr");
                                            if (functionMainObjStr != null
                                                    && !"".equalsIgnoreCase(functionMainObjStr)
                                                    && !"null".equalsIgnoreCase(functionMainObjStr)) {
                                                JSONObject functionMainObj = (JSONObject) JSONValue.parse(functionMainObjStr);
                                                if (functionMainObj != null && !functionMainObj.isEmpty()
                                                        && ("QUERY".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
                                                        || "COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE")))) {//mainFunStr
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
                                                    query += ",(" + String.valueOf(functionMainObj.get("mainFunStr")).replace(":", ".") + ") AS  " + toColumnName;

                                                } else if (functionMainObj != null && !functionMainObj.isEmpty()
                                                        && "MULTI_COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))) {
                                                    JSONObject multiColumnsObj = (JSONObject) functionMainObj.get("multiColumnsObj");
                                                    if (multiColumnsObj != null && !multiColumnsObj.isEmpty()) {
                                                        String multColStr = new V10DataPipingUtills().generateMultiColumnsObj(multiColumnsObj, (String) functionMainObj.get("functionName"));
                                                        if (multColStr != null
                                                                && !"".equalsIgnoreCase(multColStr)
                                                                && !"null".equalsIgnoreCase(multColStr)) {
                                                            query += ",(" + multColStr + ") AS  " + toColumnName;
                                                        }
                                                    }

                                                }
                                            }

                                        }
                                    }

                                }
                            }

                            query += " FROM " + tableName + " ";
                            if (tablesObj != null && !tablesObj.isEmpty()) {
                                String tableInputVal = (String) tablesObj.get(tableName);
                                if (tableInputVal != null && !"".equalsIgnoreCase(tableInputVal) && !"null".equalsIgnoreCase(tableInputVal)) {
                                    query += " WHERE " + tableInputVal + "";
                                }
                            }
                            int end = 0;
                            if (start == 0) {
                                end = limit;
                            } else {
                                end = (start + limit - 1);
                            }
//                            if ((selectTabObj != null
//                                    && !selectTabObj.isEmpty()
//                                    && "Y".equalsIgnoreCase(String.valueOf(selectTabObj.get("uniqueRowsFlag"))))) {
//                                query = "SELECT " + rowNumStr + "A.* FROM (" + query + ") A";
//                            }
                            if (orderAndGroupByObj != null && !orderAndGroupByObj.isEmpty()) {
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
                                String groupByQuery = (String) orderAndGroupByObj.get("groupByQuery");
                                if (groupByQuery != null
                                        && !"".equalsIgnoreCase(groupByQuery)
                                        && !"null".equalsIgnoreCase(groupByQuery)) {
                                    query += " GROUP BY  " + groupByQuery + " ";
                                }

                            }
                            query += " OFFSET " + (start - 1) + " ROWS FETCH NEXT " + limit + " ROWS ONLY ";
//                            query = "SELECT * FROM (" + query + ") WHERE RNO BETWEEN " + start + " AND " + (end) + "";
                            System.out.println("query::::" + query);
                            preparedStatement = connection.prepareStatement(query);
                            resultSet = preparedStatement.executeQuery();
                            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                            int columnCount = resultSetMetaData.getColumnCount();
                            while (resultSet.next()) {
                                Map dataObj = new HashMap();
                                if (columnsList != null && !columnsList.isEmpty()) {
                                    for (int i = 0; i < columnCount; i++) {
                                        dataObj.put(tableName + ":" + resultSetMetaData.getColumnName(i + 1), resultSet.getString(resultSetMetaData.getColumnName(i + 1)));
                                    }
                                    if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
                                        for (Object toColumnName : columnClauseObj.keySet()) {
                                            if (columnClauseObj.get(toColumnName) instanceof JSONObject) {
                                                JSONObject funObj = (JSONObject) columnClauseObj.get(toColumnName);
                                                if (funObj != null
                                                        && !funObj.isEmpty()) {
                                                    String functionMainObjStr = (String) funObj.get("funobjstr");
                                                    if (functionMainObjStr != null
                                                            && !"".equalsIgnoreCase(functionMainObjStr)
                                                            && !"null".equalsIgnoreCase(functionMainObjStr)) {
                                                        JSONObject functionMainObj = (JSONObject) JSONValue.parse(functionMainObjStr);
                                                        if (functionMainObj != null && !functionMainObj.isEmpty()
                                                                && "DATA".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))) {
                                                            String processClass = (String) functionMainObj.get("DM_FUN_CUST_COL1");
                                                            String processMethod = (String) functionMainObj.get("DM_FUN_CUST_COL2");
                                                            if (processClass != null
                                                                    && !"".equalsIgnoreCase(processClass)
                                                                    && !"null".equalsIgnoreCase(processClass)
                                                                    && processMethod != null
                                                                    && !"".equalsIgnoreCase(processMethod)
                                                                    && !"null".equalsIgnoreCase(processMethod)) {
                                                                try {
                                                                    Class clazz = Class.forName(processClass);
                                                                    Class<?>[] paramTypes = {JSONObject.class,
                                                                        Connection.class, String.class, String.class,
                                                                        String.class, String.class, String.class, String.class, String.class, String.class};
                                                                    Object targetObj = new PilogUtilities().createObjectByName(processClass);
                                                                    Method method = clazz.getMethod(processMethod.trim(), paramTypes);
                                                                    String resultStr = (String) method.invoke(targetObj,
                                                                            functionMainObj,
                                                                            (toConnection != null ? toConnection : connection),
                                                                            dataBaseDriver,
                                                                            dbURL,
                                                                            dbUserName,
                                                                            dbPassword,
                                                                            loginUserName,
                                                                            loginOrgnId, String.valueOf(lastSeqObj.get(toColumnName)), String.valueOf(toColumnName));
                                                                    if (resultStr != null
                                                                            && !"".equalsIgnoreCase(resultStr)
                                                                            && !"null".equalsIgnoreCase(resultStr)) {
                                                                        lastSeqObj.put(toColumnName, resultStr);
                                                                        dataObj.put(toColumnName, resultStr);
                                                                    }
                                                                } catch (Exception e) {
                                                                    continue;
                                                                }

                                                            } else {
                                                            }
                                                        } else if (functionMainObj != null && !functionMainObj.isEmpty()
                                                                && ("QUERY".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
                                                                || "COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
                                                                || "MULTI_COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE")))) {
                                                            dataObj.put(toColumnName, resultSet.getString((String) toColumnName));
                                                        }
                                                    }

                                                }
                                            }

                                        }
                                    }
                                    totalData.add(dataObj);
                                }

                            }

                        }

                    }
                }

            }
            try {
                processETLLog(loginUserName,
                        loginOrgnId, "   ", "INFO", 1, "Y",
                        dataBaseDriver, dbURL, dbUserName, dbPassword, lastSeqObj.toJSONString(), String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }
        } catch (Exception e) {
            e.printStackTrace();
            try {
                processETLLog(loginUserName,
                        loginOrgnId, "Failed to fetch the data due to " + e.getMessage(), "ERROR", 1, "Y",
                        dataBaseDriver, dbURL, dbUserName, dbPassword, String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }
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

    public List getMYSQLSelectedColumnsData(Connection connection,
            //JSONObject tableColsObj,
            Map tableColsObj,// ravi etl integration
            Integer start,
            Integer limit, JSONObject tablesObj, String serviceName,
            String loginUserName, String loginOrgnId, String dataBaseDriver,
            String dbURL, String dbUserName, String dbPassword,
            JSONObject orderAndGroupByObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj, Connection toConnection) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        String result = "";
        List totalData = new ArrayList();
        JSONObject lastSeqObj = new JSONObject();
        try {
            if (tableColsObj != null && !tableColsObj.isEmpty()) {
                for (Object tableName : tableColsObj.keySet()) {
                    if (tableName != null && tableColsObj.get(tableName) != null) {
                        String colsObj = (String) tableColsObj.get(tableName);
                        if (colsObj != null && !colsObj.isEmpty()) {
//                            String rowNumStr = "ROWNUM AS RNO,";
                            String query = "SELECT ";
                            if ((selectTabObj != null
                                    && !selectTabObj.isEmpty()
                                    && "Y".equalsIgnoreCase(String.valueOf(selectTabObj.get("uniqueRowsFlag"))))) {//uniqueRowsFlag
                                query += " DISTINCT ";
                            }
                            String fetchColumns = "";
                            List<String> columnsList = new ArrayList(Arrays.asList(colsObj.split(",")));
                            Set<String> items = new HashSet<>();
                            Set matchedColsSet = columnsList.stream()
                                    .filter(n -> !items.add(n)) // Set.add() returns false if the element was already in the set.
                                    .collect(Collectors.toSet());

                            for (int j = 0; j < columnsList.size(); j++) {
                                if (matchedColsSet != null && !matchedColsSet.isEmpty() && matchedColsSet.contains(columnsList.get(j))) {
                                    fetchColumns += tableName + "." + columnsList.get(j) + " AS " + columnsList.get(j) + "_0" + j;
                                } else {
                                    fetchColumns += tableName + "." + columnsList.get(j);
                                }
                                if (j != columnsList.size() - 1) {
                                    fetchColumns += ",";
                                }
                            }
                            columnsList = new ArrayList(Arrays.asList(fetchColumns.split(",")));
                            query += " " + fetchColumns;
                            if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
                                for (Object toColumnName : columnClauseObj.keySet()) {
                                    if (columnClauseObj.get(toColumnName) instanceof JSONObject) {
                                        JSONObject funObj = (JSONObject) columnClauseObj.get(toColumnName);
                                        if (funObj != null
                                                && !funObj.isEmpty()) {
                                            String functionMainObjStr = (String) funObj.get("funobjstr");
                                            if (functionMainObjStr != null
                                                    && !"".equalsIgnoreCase(functionMainObjStr)
                                                    && !"null".equalsIgnoreCase(functionMainObjStr)) {
                                                JSONObject functionMainObj = (JSONObject) JSONValue.parse(functionMainObjStr);
                                                if (functionMainObj != null && !functionMainObj.isEmpty()
                                                        && ("QUERY".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
                                                        || "COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE")) //                                                        || "MULTI_COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
                                                        )) {
                                                    if ("DM_COL_AGGREGATE".equalsIgnoreCase(String.valueOf(functionMainObj.get("HL_FUN_ID")))) {
                                                        String aggTableName = String.valueOf(functionMainObj.get("COLUMN_NAME"));
                                                        if (aggTableName != null
                                                                && !"".equalsIgnoreCase(aggTableName)
                                                                && !"null".equalsIgnoreCase(aggTableName)
                                                                && aggTableName.contains(":")) {
                                                            aggTableName = aggTableName.split(":")[0];
                                                        }

                                                        query += ",(SELECT " + String.valueOf(functionMainObj.get("mainFunStr")).replace(":", ".") + " FROM " + aggTableName + " ) AS  " + toColumnName;
                                                    } else {
                                                        query += ",(" + String.valueOf(functionMainObj.get("mainFunStr")).replace(":", ".") + ") AS  " + toColumnName;
                                                    }
//                                                    query += ",(" + String.valueOf(columnClauseObj.get(toColumnName)).replace(":", ".") + ") AS  " + toColumnName;
                                                } else if (functionMainObj != null && !functionMainObj.isEmpty()
                                                        && "MULTI_COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))) {
                                                    JSONObject multiColumnsObj = (JSONObject) functionMainObj.get("multiColumnsObj");
                                                    if (multiColumnsObj != null && !multiColumnsObj.isEmpty()) {
                                                        String multColStr = new V10DataPipingUtills().generateMultiColumnsObj(multiColumnsObj, (String) functionMainObj.get("functionName"));
                                                        if (multColStr != null
                                                                && !"".equalsIgnoreCase(multColStr)
                                                                && !"null".equalsIgnoreCase(multColStr)) {
                                                            query += ",(" + multColStr + ") AS  " + toColumnName;
                                                        }
                                                    }

                                                }
                                            }

                                        }
                                    }

                                }
                            }

                            query += " FROM " + tableName + " ";
                            if (tablesObj != null && !tablesObj.isEmpty()) {
                                String tableInputVal = (String) tablesObj.get(tableName);
                                if (tableInputVal != null && !"".equalsIgnoreCase(tableInputVal) && !"null".equalsIgnoreCase(tableInputVal)) {
                                    query += " WHERE " + tableInputVal + "";
                                }
                            }
                            int end = 0;
                            if (start == 0) {
                                end = limit;
                            } else {
                                end = (start + limit - 1);
                            }
//                            if ((selectTabObj != null
//                                    && !selectTabObj.isEmpty()
//                                    && "Y".equalsIgnoreCase(String.valueOf(selectTabObj.get("uniqueRowsFlag"))))) {
//                                query = "SELECT " + rowNumStr + "A.* FROM (" + query + ") A";
//                            }
                            if (orderAndGroupByObj != null && !orderAndGroupByObj.isEmpty()) {
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
                                String groupByQuery = (String) orderAndGroupByObj.get("groupByQuery");
                                if (groupByQuery != null
                                        && !"".equalsIgnoreCase(groupByQuery)
                                        && !"null".equalsIgnoreCase(groupByQuery)) {
                                    query += " GROUP BY  " + groupByQuery + " ";
                                }

                            }
                            query += " LIMIT " + (start - 1) + " , " + limit + " ";
//                            query += " OFFSET " + start + " ROWS FETCH NEXT " + limit + " ROWS ONLY ";
//                            query = "SELECT * FROM (" + query + ") WHERE RNO BETWEEN " + start + " AND " + (end) + "";
                            System.out.println("query::::" + query);
                            preparedStatement = connection.prepareStatement(query);
                            resultSet = preparedStatement.executeQuery();
                            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                            int columnCount = resultSetMetaData.getColumnCount();
                            while (resultSet.next()) {
                                Map dataObj = new HashMap();
                                if (columnsList != null && !columnsList.isEmpty()) {
                                    for (int i = 0; i < columnCount; i++) {
                                        dataObj.put(tableName + ":" + resultSetMetaData.getColumnName(i + 1), resultSet.getString(resultSetMetaData.getColumnName(i + 1)));
                                    }
                                    if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
                                        for (Object toColumnName : columnClauseObj.keySet()) {
                                            if (columnClauseObj.get(toColumnName) instanceof JSONObject) {
                                                JSONObject funObj = (JSONObject) columnClauseObj.get(toColumnName);
                                                if (funObj != null
                                                        && !funObj.isEmpty()) {
                                                    String functionMainObjStr = (String) funObj.get("funobjstr");
                                                    if (functionMainObjStr != null
                                                            && !"".equalsIgnoreCase(functionMainObjStr)
                                                            && !"null".equalsIgnoreCase(functionMainObjStr)) {
                                                        JSONObject functionMainObj = (JSONObject) JSONValue.parse(functionMainObjStr);
                                                        if (functionMainObj != null && !functionMainObj.isEmpty()
                                                                && "DATA".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))) {
                                                            String processClass = (String) functionMainObj.get("DM_FUN_CUST_COL1");
                                                            String processMethod = (String) functionMainObj.get("DM_FUN_CUST_COL2");
                                                            if (processClass != null
                                                                    && !"".equalsIgnoreCase(processClass)
                                                                    && !"null".equalsIgnoreCase(processClass)
                                                                    && processMethod != null
                                                                    && !"".equalsIgnoreCase(processMethod)
                                                                    && !"null".equalsIgnoreCase(processMethod)) {
                                                                try {
                                                                    Class clazz = Class.forName(processClass);
                                                                    Class<?>[] paramTypes = {JSONObject.class,
                                                                        Connection.class, String.class, String.class,
                                                                        String.class, String.class, String.class, String.class, String.class, String.class};
                                                                    Object targetObj = new PilogUtilities().createObjectByName(processClass);
                                                                    Method method = clazz.getMethod(processMethod.trim(), paramTypes);
                                                                    String resultStr = (String) method.invoke(targetObj,
                                                                            functionMainObj,
                                                                            (toConnection != null ? toConnection : connection),
                                                                            dataBaseDriver,
                                                                            dbURL,
                                                                            dbUserName,
                                                                            dbPassword,
                                                                            loginUserName,
                                                                            loginOrgnId, String.valueOf(lastSeqObj.get(toColumnName)), String.valueOf(toColumnName));
                                                                    if (resultStr != null
                                                                            && !"".equalsIgnoreCase(resultStr)
                                                                            && !"null".equalsIgnoreCase(resultStr)) {
                                                                        lastSeqObj.put(toColumnName, resultStr);
                                                                        dataObj.put(toColumnName, resultStr);
                                                                    }
                                                                } catch (Exception e) {
                                                                    continue;
                                                                }

                                                            } else {
                                                            }
                                                        } else if (functionMainObj != null && !functionMainObj.isEmpty()
                                                                && ("QUERY".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
                                                                || "COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
                                                                || "MULTI_COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE")))) {
                                                            dataObj.put(toColumnName, resultSet.getString((String) toColumnName));
                                                        }
                                                    }

                                                }
                                            }

                                        }
                                    }
                                    totalData.add(dataObj);
                                }

                            }

                        }

                    }
                }

            }
            try {
                processETLLog(loginUserName,
                        loginOrgnId, "   ", "INFO", 1, "Y",
                        dataBaseDriver, dbURL, dbUserName, dbPassword, lastSeqObj.toJSONString(), String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }
        } catch (Exception e) {
            e.printStackTrace();
            try {
                processETLLog(loginUserName,
                        loginOrgnId, "Failed to fetch the data due to " + e.getMessage(), "ERROR", 1, "Y",
                        dataBaseDriver, dbURL, dbUserName, dbPassword, String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }
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
//    public List getMYSQLSelectedColumnsData(Connection connection, JSONObject tableColsObj, Integer start,
//            Integer limit, JSONObject tablesObj, String serviceName,
//            String loginUserName, String loginOrgnId, String dataBaseDriver,
//            String dbURL, String dbUserName, String dbPassword,
//            JSONObject orderAndGroupByObj,
//            JSONObject columnClauseObj,
//            JSONObject selectTabObj, Connection toConnection) {
//        PreparedStatement preparedStatement = null;
//        ResultSet resultSet = null;
//        String result = "";
//        List totalData = new ArrayList();
//        JSONObject lastSeqObj = new JSONObject();
//        try {
//            if (tableColsObj != null && !tableColsObj.isEmpty()) {
//                for (Object tableName : tableColsObj.keySet()) {
//                    if (tableName != null && tableColsObj.get(tableName) != null) {
//                        String colsObj = (String) tableColsObj.get(tableName);
//                        if (colsObj != null && !colsObj.isEmpty()) {
//                            String fetchColumns = "";
//                            List<String> columnsList = new ArrayList(Arrays.asList(colsObj.split(",")));
//                            Set<String> items = new HashSet<>();
//                            Set matchedColsSet = columnsList.stream()
//                                    .filter(n -> !items.add(n)) // Set.add() returns false if the element was already in the set.
//                                    .collect(Collectors.toSet());
//
//                            for (int j = 0; j < columnsList.size(); j++) {
//                                if (matchedColsSet != null && !matchedColsSet.isEmpty() && matchedColsSet.contains(columnsList.get(j))) {
//                                    fetchColumns += tableName + "." + columnsList.get(j) + " AS " + columnsList.get(j) + "_0" + j;
//                                } else {
//                                    fetchColumns += tableName + "." + columnsList.get(j);
//                                }
//                                if (j != columnsList.size() - 1) {
//                                    fetchColumns += ",";
//                                }
//                            }
//                            colsObj = fetchColumns;
//                            // String query = "SELECT @row_number:=@row_number+1 AS RNO," + colsObj;
//                            columnsList = new ArrayList(Arrays.asList(colsObj.split(",")));
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
//                                                        || "COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
//                                                        || "MULTI_COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE")))) {
//                                                    colsObj += ",(" + String.valueOf(columnClauseObj.get(toColumnName)).replace(":", ".") + ") AS  " + toColumnName;
//                                                }
//                                            }
//
//                                        }
//                                    }
//
//                                }
//                            }
//                            String query = "";
//                            query += " FROM " + tableName + " ";
//                            if (tablesObj != null && !tablesObj.isEmpty()) {
//                                String tableInputVal = (String) tablesObj.get(tableName);
//                                if (tableInputVal != null && !"".equalsIgnoreCase(tableInputVal) && !"null".equalsIgnoreCase(tableInputVal)) {
//                                    query += " WHERE " + tableInputVal + "";
//                                }
//                            }
//
////                            if ((selectTabObj != null
////                                    && !selectTabObj.isEmpty()
////                                    && "Y".equalsIgnoreCase(String.valueOf(selectTabObj.get("uniqueRowsFlag"))))) {
////                                query = "SELECT " + rowNumStr + "A.* FROM (" + query + ") A";
////                            }
//                            if (orderAndGroupByObj != null && !orderAndGroupByObj.isEmpty()) {
//                                String orderByClause = "";
//                                JSONObject orderByObj = (JSONObject) orderAndGroupByObj.get("orderByObj");
//                                if (orderByObj != null
//                                        && !orderByObj.isEmpty()) {
//                                    int orderIndex = 0;
//                                    for (Object orderByKey : orderByObj.keySet()) {
//                                        JSONObject orderObj = (JSONObject) orderByObj.get(orderByKey);
//                                        if (orderObj != null
//                                                && !orderObj.isEmpty()
//                                                && orderObj.get("columnName") != null
//                                                && !"".equalsIgnoreCase(String.valueOf(orderObj.get("columnName")))
//                                                && !"null".equalsIgnoreCase(String.valueOf(orderObj.get("columnName")))) {
//                                            orderByClause += "" + String.valueOf(orderObj.get("columnName")).replaceAll(":", ".") + " "
//                                                    + " " + (("DESC".equalsIgnoreCase(String.valueOf(orderObj.get("direction")))) ? "DESC" : "ASC");
//                                            if (orderIndex != orderByObj.size() - 1) {
//                                                orderByClause += ",";
//                                            }
//                                            orderIndex++;
//                                        }
//                                    }
//                                    //columnName,direction
//                                    if (orderByClause != null
//                                            && !"".equalsIgnoreCase(orderByClause)
//                                            && !"null".equalsIgnoreCase(orderByClause)) {
//                                        query += " ORDER BY  " + orderByClause + " ";
//                                    }
//                                }
//                                String groupByQuery = (String) orderAndGroupByObj.get("groupByQuery");
//                                if (groupByQuery != null
//                                        && !"".equalsIgnoreCase(groupByQuery)
//                                        && !"null".equalsIgnoreCase(groupByQuery)) {
//                                    query += " GROUP BY  " + groupByQuery + " ";
//                                }
//
//                            }
////                            if (tablesObj != null && !tablesObj.isEmpty()) {
////                                String tableInputVal = (String) tablesObj.get(tableName);
////                                query += "" + colsObj + " FROM " + tableName + "";
////                                if (tableInputVal != null && !"".equalsIgnoreCase(tableInputVal) && !"null".equalsIgnoreCase(tableInputVal)) {
////                                    query += " WHERE " + tableInputVal + " ";
////                                }
////                                if (orderAndGroupByObj != null && !orderAndGroupByObj.isEmpty()) {
////                                    String orderByClause = "";
////                                    JSONObject orderByObj = (JSONObject) orderAndGroupByObj.get("orderByObj");
////                                    if (orderByObj != null
////                                            && !orderByObj.isEmpty()) {
////                                        int orderIndex = 0;
////                                        for (Object orderByKey : orderByObj.keySet()) {
////                                            JSONObject orderObj = (JSONObject) orderByObj.get(orderByKey);
////                                            if (orderObj != null
////                                                    && !orderObj.isEmpty()
////                                                    && orderObj.get("columnName") != null
////                                                    && !"".equalsIgnoreCase(String.valueOf(orderObj.get("columnName")))
////                                                    && !"null".equalsIgnoreCase(String.valueOf(orderObj.get("columnName")))) {
////                                                orderByClause += "" + String.valueOf(orderObj.get("columnName")).replaceAll(":", ".") + " "
////                                                        + " " + (("DESC".equalsIgnoreCase(String.valueOf(orderObj.get("direction")))) ? "DESC" : "ASC");
////                                                if (orderIndex != orderByObj.size() - 1) {
////                                                    orderByClause += ",";
////                                                }
////                                                orderIndex++;
////                                            }
////                                        }
////                                        //columnName,direction
////                                        if (orderByClause != null
////                                                && !"".equalsIgnoreCase(orderByClause)
////                                                && !"null".equalsIgnoreCase(orderByClause)) {
////                                            query += " ORDER BY  " + orderByClause + " ";
////                                        }
////                                    }
////                                    String groupByQuery = (String) orderAndGroupByObj.get("groupByQuery");
////                                    if (groupByQuery != null
////                                            && !"".equalsIgnoreCase(groupByQuery)
////                                            && !"null".equalsIgnoreCase(groupByQuery)) {
////                                        query += " GROUP BY  " + groupByQuery + " ";
////                                    }
////
////                                }
////                                query += " LIMIT " + start + " , " + limit + " ";
////                            }
//                            query += " LIMIT " + start + " , " + limit + " ";
//                            if ((selectTabObj != null
//                                    && !selectTabObj.isEmpty()
//                                    && "Y".equalsIgnoreCase(String.valueOf(selectTabObj.get("uniqueRowsFlag"))))) {//uniqueRowsFlag
//                                query = "SELECT  DISTINCT " + query;
//                            } else {
//                                query = "SELECT  " + query;
//                            }
//                            System.out.println("query::::" + query);
//                            preparedStatement = connection.prepareStatement(query);
//                            resultSet = preparedStatement.executeQuery();
//                            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
//                            int columnCount = resultSetMetaData.getColumnCount();
//                            while (resultSet.next()) {
//                                Map dataObj = new HashMap();
//                                if (columnsList != null && !columnsList.isEmpty()) {
//                                    for (int i = 0; i < columnsList.size(); i++) {
//                                        dataObj.put(tableName + ":" + resultSetMetaData.getColumnName(i + 1), resultSet.getString(resultSetMetaData.getColumnName(i + 1)));
//                                    }
//                                    if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
//                                        for (Object toColumnName : columnClauseObj.keySet()) {
//                                            if (columnClauseObj.get(toColumnName) instanceof JSONObject) {
//                                                JSONObject funObj = (JSONObject) columnClauseObj.get(toColumnName);
//                                                if (funObj != null
//                                                        && !funObj.isEmpty()) {
//                                                    String functionMainObjStr = (String) funObj.get("funobjstr");
//                                                    if (functionMainObjStr != null
//                                                            && !"".equalsIgnoreCase(functionMainObjStr)
//                                                            && !"null".equalsIgnoreCase(functionMainObjStr)) {
//                                                        JSONObject functionMainObj = (JSONObject) JSONValue.parse(functionMainObjStr);
//                                                        if (functionMainObj != null && !functionMainObj.isEmpty()
//                                                                && "DATA".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))) {
//                                                            String processClass = (String) functionMainObj.get("DM_FUN_CUST_COL1");
//                                                            String processMethod = (String) functionMainObj.get("DM_FUN_CUST_COL2");
//                                                            if (processClass != null
//                                                                    && !"".equalsIgnoreCase(processClass)
//                                                                    && !"null".equalsIgnoreCase(processClass)
//                                                                    && processMethod != null
//                                                                    && !"".equalsIgnoreCase(processMethod)
//                                                                    && !"null".equalsIgnoreCase(processMethod)) {
//                                                                try {
//                                                                    Class clazz = Class.forName(processClass);
//                                                                    Class<?>[] paramTypes = {JSONObject.class,
//                                                                        Connection.class, String.class, String.class,
//                                                                        String.class, String.class, String.class, String.class, String.class};
//                                                                    Object targetObj = new PilogUtilities().createObjectByName(processClass);
//                                                                    Method method = clazz.getMethod(processMethod.trim(), paramTypes);
//                                                                    String resultStr = (String) method.invoke(targetObj,
//                                                                            functionMainObj,
//                                                                            (toConnection != null ? toConnection : connection),
//                                                                            dataBaseDriver,
//                                                                            dbURL,
//                                                                            dbUserName,
//                                                                            dbPassword,
//                                                                            loginUserName,
//                                                                            loginOrgnId, String.valueOf(lastSeqObj.get(toColumnName)));
//                                                                    if (resultStr != null
//                                                                            && !"".equalsIgnoreCase(resultStr)
//                                                                            && !"null".equalsIgnoreCase(resultStr)) {
//                                                                        lastSeqObj.put(toColumnName, resultStr);
//                                                                        dataObj.put(toColumnName, resultStr);
//                                                                    }
//                                                                } catch (Exception e) {
//                                                                    continue;
//                                                                }
//
//                                                            } else {
//                                                            }
//                                                        } else if (functionMainObj != null && !functionMainObj.isEmpty()
//                                                                && ("QUERY".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
//                                                                || "COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
//                                                                || "MULTI_COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE")))) {
//                                                            dataObj.put(toColumnName, resultSet.getString((String) toColumnName));
//                                                        }
//                                                    }
//
//                                                }
//                                            }
//
//                                        }
//                                    }
//                                    totalData.add(dataObj);
//                                }
//
//                            }
//
//                        }
//
//                    }
//                }
//
//            }
//            try {
//                processETLLog(loginUserName,
//                        loginOrgnId, "   ", "INFO", 1, "Y",
//                        dataBaseDriver, dbURL, dbUserName, dbPassword, lastSeqObj.toJSONString(), String.valueOf(selectTabObj.get("jobId")));
//            } catch (Exception ex) {
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//            try {
//                processETLLog(loginUserName,
//                        loginOrgnId, "Failed to fetch the data due to " + e.getMessage(), "ERROR", 1, "Y",
//                        dataBaseDriver, dbURL, dbUserName, dbPassword, String.valueOf(selectTabObj.get("jobId")));
//            } catch (Exception ex) {
//            }
//        } finally {
//            try {
//                if (resultSet != null) {
//                    resultSet.close();
//                }
//                if (preparedStatement != null) {
//                    preparedStatement.close();
//                }
//
//            } catch (Exception e) {
//            }
//        }
//        return totalData;
//    }

    public List getMSSQLSelectedColumnsData(Connection connection,
            //JSONObject tableColsObj,
            Map tableColsObj,// ravi etl integration
            Integer start,
            Integer limit, JSONObject tablesObj, String serviceName,
            String loginUserName, String loginOrgnId, String dataBaseDriver,
            String dbURL, String dbUserName, String dbPassword,
            JSONObject orderAndGroupByObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj, Connection toConnection) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        String result = "";
        List totalData = new ArrayList();
        JSONObject lastSeqObj = new JSONObject();
        try {
            if (tableColsObj != null && !tableColsObj.isEmpty()) {
                for (Object tableName : tableColsObj.keySet()) {
                    if (tableName != null && tableColsObj.get(tableName) != null) {
                        String colsObj = (String) tableColsObj.get(tableName);
                        if (colsObj != null && !colsObj.isEmpty()) {
//                            String rowNumStr = "ROWNUM AS RNO,";
                            String query = "SELECT ";
                            if ((selectTabObj != null
                                    && !selectTabObj.isEmpty()
                                    && "Y".equalsIgnoreCase(String.valueOf(selectTabObj.get("uniqueRowsFlag"))))) {//uniqueRowsFlag
                                query += " DISTINCT ";
                            }
                            String fetchColumns = "";
                            List<String> columnsList = new ArrayList(Arrays.asList(colsObj.split(",")));
                            Set<String> items = new HashSet<>();
                            Set matchedColsSet = columnsList.stream()
                                    .filter(n -> !items.add(n)) // Set.add() returns false if the element was already in the set.
                                    .collect(Collectors.toSet());

                            for (int j = 0; j < columnsList.size(); j++) {
                                if (matchedColsSet != null && !matchedColsSet.isEmpty() && matchedColsSet.contains(columnsList.get(j))) {
                                    fetchColumns += tableName + "." + columnsList.get(j) + " AS " + columnsList.get(j) + "_0" + j;
                                } else {
                                    fetchColumns += tableName + "." + columnsList.get(j);
                                }
                                if (j != columnsList.size() - 1) {
                                    fetchColumns += ",";
                                }
                            }
                            columnsList = new ArrayList(Arrays.asList(fetchColumns.split(",")));
                            query += " " + fetchColumns;
                            if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
                                for (Object toColumnName : columnClauseObj.keySet()) {
                                    if (columnClauseObj.get(toColumnName) instanceof JSONObject) {
                                        JSONObject funObj = (JSONObject) columnClauseObj.get(toColumnName);
                                        if (funObj != null
                                                && !funObj.isEmpty()) {
                                            String functionMainObjStr = (String) funObj.get("funobjstr");
                                            if (functionMainObjStr != null
                                                    && !"".equalsIgnoreCase(functionMainObjStr)
                                                    && !"null".equalsIgnoreCase(functionMainObjStr)) {
                                                JSONObject functionMainObj = (JSONObject) JSONValue.parse(functionMainObjStr);
                                                if (functionMainObj != null && !functionMainObj.isEmpty()
                                                        && ("QUERY".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
                                                        || "COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE")) //                                                        || "MULTI_COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
                                                        )) {
                                                    if ("DM_COL_AGGREGATE".equalsIgnoreCase(String.valueOf(functionMainObj.get("HL_FUN_ID")))) {
                                                        String aggTableName = String.valueOf(functionMainObj.get("COLUMN_NAME"));
                                                        if (aggTableName != null
                                                                && !"".equalsIgnoreCase(aggTableName)
                                                                && !"null".equalsIgnoreCase(aggTableName)
                                                                && aggTableName.contains(":")) {
                                                            aggTableName = aggTableName.split(":")[0];
                                                        }

                                                        query += ",(SELECT " + String.valueOf(functionMainObj.get("mainFunStr")).replace(":", ".") + " FROM " + aggTableName + " ) AS  " + toColumnName;
                                                    } else {
                                                        query += ",(" + String.valueOf(functionMainObj.get("mainFunStr")).replace(":", ".") + ") AS  " + toColumnName;
                                                    }
//                                                    query += ",(" + String.valueOf(columnClauseObj.get(toColumnName)).replace(":", ".") + ") AS  " + toColumnName;
                                                } else if (functionMainObj != null && !functionMainObj.isEmpty()
                                                        && "MULTI_COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))) {
                                                    JSONObject multiColumnsObj = (JSONObject) functionMainObj.get("multiColumnsObj");
                                                    if (multiColumnsObj != null && !multiColumnsObj.isEmpty()) {
                                                        String multColStr = new V10DataPipingUtills().generateMultiColumnsObj(multiColumnsObj, (String) functionMainObj.get("functionName"));
                                                        if (multColStr != null
                                                                && !"".equalsIgnoreCase(multColStr)
                                                                && !"null".equalsIgnoreCase(multColStr)) {
                                                            query += ",(" + multColStr + ") AS  " + toColumnName;
                                                        }
                                                    }

                                                }
                                            }

                                        }
                                    }

                                }
                            }

                            query += " FROM " + tableName + " ";
                            if (tablesObj != null && !tablesObj.isEmpty()) {
                                String tableInputVal = (String) tablesObj.get(tableName);
                                if (tableInputVal != null && !"".equalsIgnoreCase(tableInputVal) && !"null".equalsIgnoreCase(tableInputVal)) {
                                    query += " WHERE " + tableInputVal + "";
                                }
                            }
                            int end = 0;
                            if (start == 0) {
                                end = limit;
                            } else {
                                end = (start + limit - 1);
                            }
//                            if ((selectTabObj != null
//                                    && !selectTabObj.isEmpty()
//                                    && "Y".equalsIgnoreCase(String.valueOf(selectTabObj.get("uniqueRowsFlag"))))) {
//                                query = "SELECT " + rowNumStr + "A.* FROM (" + query + ") A";
//                            }
                            if (orderAndGroupByObj != null && !orderAndGroupByObj.isEmpty()) {
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
                                    } else {
                                        query += " ORDER BY " + columnsList.get(0);
                                    }
                                } else {
                                    query += " ORDER BY " + columnsList.get(0);
                                }

                                String groupByQuery = (String) orderAndGroupByObj.get("groupByQuery");
                                if (groupByQuery != null
                                        && !"".equalsIgnoreCase(groupByQuery)
                                        && !"null".equalsIgnoreCase(groupByQuery)) {
                                    query += " GROUP BY  " + groupByQuery + " ";
                                }

                            } else {
                                query += " ORDER BY " + columnsList.get(0);
                            }
                            query += " OFFSET " + (start - 1) + " ROWS FETCH NEXT " + limit + " ROWS ONLY";
//                            query += " OFFSET " + start + " ROWS FETCH NEXT " + limit + " ROWS ONLY ";
//                            query = "SELECT * FROM (" + query + ") WHERE RNO BETWEEN " + start + " AND " + (end) + "";
                            System.out.println("query::::" + query);
                            preparedStatement = connection.prepareStatement(query);
                            resultSet = preparedStatement.executeQuery();
                            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                            int columnCount = resultSetMetaData.getColumnCount();
                            while (resultSet.next()) {
                                Map dataObj = new HashMap();
                                if (columnsList != null && !columnsList.isEmpty()) {
                                    for (int i = 0; i < columnCount; i++) {
                                        dataObj.put(tableName + ":" + resultSetMetaData.getColumnName(i + 1), resultSet.getString(resultSetMetaData.getColumnName(i + 1)));
                                    }
                                    if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
                                        for (Object toColumnName : columnClauseObj.keySet()) {
                                            if (columnClauseObj.get(toColumnName) instanceof JSONObject) {
                                                JSONObject funObj = (JSONObject) columnClauseObj.get(toColumnName);
                                                if (funObj != null
                                                        && !funObj.isEmpty()) {
                                                    String functionMainObjStr = (String) funObj.get("funobjstr");
                                                    if (functionMainObjStr != null
                                                            && !"".equalsIgnoreCase(functionMainObjStr)
                                                            && !"null".equalsIgnoreCase(functionMainObjStr)) {
                                                        JSONObject functionMainObj = (JSONObject) JSONValue.parse(functionMainObjStr);
                                                        if (functionMainObj != null && !functionMainObj.isEmpty()
                                                                && "DATA".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))) {
                                                            String processClass = (String) functionMainObj.get("DM_FUN_CUST_COL1");
                                                            String processMethod = (String) functionMainObj.get("DM_FUN_CUST_COL2");
                                                            if (processClass != null
                                                                    && !"".equalsIgnoreCase(processClass)
                                                                    && !"null".equalsIgnoreCase(processClass)
                                                                    && processMethod != null
                                                                    && !"".equalsIgnoreCase(processMethod)
                                                                    && !"null".equalsIgnoreCase(processMethod)) {
                                                                try {
                                                                    Class clazz = Class.forName(processClass);
                                                                    Class<?>[] paramTypes = {JSONObject.class,
                                                                        Connection.class, String.class, String.class,
                                                                        String.class, String.class, String.class, String.class, String.class, String.class};
                                                                    Object targetObj = new PilogUtilities().createObjectByName(processClass);
                                                                    Method method = clazz.getMethod(processMethod.trim(), paramTypes);
                                                                    String resultStr = (String) method.invoke(targetObj,
                                                                            functionMainObj,
                                                                            (toConnection != null ? toConnection : connection),
                                                                            dataBaseDriver,
                                                                            dbURL,
                                                                            dbUserName,
                                                                            dbPassword,
                                                                            loginUserName,
                                                                            loginOrgnId, String.valueOf(lastSeqObj.get(toColumnName)), String.valueOf(toColumnName));
                                                                    if (resultStr != null
                                                                            && !"".equalsIgnoreCase(resultStr)
                                                                            && !"null".equalsIgnoreCase(resultStr)) {
                                                                        lastSeqObj.put(toColumnName, resultStr);
                                                                        dataObj.put(toColumnName, resultStr);
                                                                    }
                                                                } catch (Exception e) {
                                                                    continue;
                                                                }

                                                            } else {
                                                            }
                                                        } else if (functionMainObj != null && !functionMainObj.isEmpty()
                                                                && ("QUERY".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
                                                                || "COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
                                                                || "MULTI_COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE")))) {
                                                            dataObj.put(toColumnName, resultSet.getString((String) toColumnName));
                                                        }
                                                    }

                                                }
                                            }

                                        }
                                    }
                                    totalData.add(dataObj);
                                }

                            }

                        }

                    }
                }

            }
            try {
                processETLLog(loginUserName,
                        loginOrgnId, "   ", "INFO", 1, "Y",
                        dataBaseDriver, dbURL, dbUserName, dbPassword, lastSeqObj.toJSONString(), String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }
        } catch (Exception e) {
            e.printStackTrace();
            try {
                processETLLog(loginUserName,
                        loginOrgnId, "Failed to fetch the data due to " + e.getMessage(), "ERROR", 1, "Y",
                        dataBaseDriver, dbURL, dbUserName, dbPassword, String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }
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
//    public List getMSSQLSelectedColumnsData(Connection connection, JSONObject tableColsObj, Integer start,
//            Integer limit, JSONObject tablesObj, String serviceName,
//            String loginUserName, String loginOrgnId, String dataBaseDriver,
//            String dbURL, String dbUserName, String dbPassword,
//            JSONObject orderAndGroupByObj,
//            JSONObject columnClauseObj,
//            JSONObject selectTabObj, Connection toConnection) {
//        PreparedStatement preparedStatement = null;
//        ResultSet resultSet = null;
//        String result = "";
//        List totalData = new ArrayList();
//        JSONObject lastSeqObj = new JSONObject();
//        try {
//            if (tableColsObj != null && !tableColsObj.isEmpty()) {
//                for (Object tableName : tableColsObj.keySet()) {
//                    if (tableName != null && tableColsObj.get(tableName) != null) {
//                        String colsObj = (String) tableColsObj.get(tableName);
//                        if (colsObj != null && !colsObj.isEmpty()) {
//                            // String query = "SELECT @row_number:=@row_number+1 AS RNO," + colsObj;
//                            List<String> columnsList = new ArrayList(Arrays.asList(colsObj.split(",")));
//                            String fetchColumns = "";
//
//                            Set<String> items = new HashSet<>();
//                            Set matchedColsSet = columnsList.stream()
//                                    .filter(n -> !items.add(n)) // Set.add() returns false if the element was already in the set.
//                                    .collect(Collectors.toSet());
//
//                            for (int j = 0; j < columnsList.size(); j++) {
//                                if (matchedColsSet != null && !matchedColsSet.isEmpty() && matchedColsSet.contains(columnsList.get(j))) {
//                                    fetchColumns += tableName + "." + columnsList.get(j) + " AS " + columnsList.get(j) + "_0" + j;
//                                } else {
//                                    fetchColumns += tableName + "." + columnsList.get(j);
//                                }
//                                if (j != columnsList.size() - 1) {
//                                    fetchColumns += ",";
//                                }
//                            }
//                            colsObj = fetchColumns;
//                            columnsList = new ArrayList(Arrays.asList(colsObj.split(",")));
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
//                                                        || "COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
//                                                        || "MULTI_COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE")))) {
//                                                    colsObj += ",(" + String.valueOf(columnClauseObj.get(toColumnName)).replace(":", ".") + ") AS  " + toColumnName;
//                                                }
//                                            }
//
//                                        }
//                                    }
//
//                                }
//                            }
//                            String query = "";
//                            query += " FROM " + tableName + " ";
//                            if (tablesObj != null && !tablesObj.isEmpty()) {
//                                String tableInputVal = (String) tablesObj.get(tableName);
//                                if (tableInputVal != null && !"".equalsIgnoreCase(tableInputVal) && !"null".equalsIgnoreCase(tableInputVal)) {
//                                    query += " WHERE " + tableInputVal + "";
//                                }
//                            }
//
////                            if ((selectTabObj != null
////                                    && !selectTabObj.isEmpty()
////                                    && "Y".equalsIgnoreCase(String.valueOf(selectTabObj.get("uniqueRowsFlag"))))) {
////                                query = "SELECT " + rowNumStr + "A.* FROM (" + query + ") A";
////                            }
//                            if (orderAndGroupByObj != null && !orderAndGroupByObj.isEmpty()) {
//                                String orderByClause = "";
//                                JSONObject orderByObj = (JSONObject) orderAndGroupByObj.get("orderByObj");
//                                if (orderByObj != null
//                                        && !orderByObj.isEmpty()) {
//                                    int orderIndex = 0;
//                                    for (Object orderByKey : orderByObj.keySet()) {
//                                        JSONObject orderObj = (JSONObject) orderByObj.get(orderByKey);
//                                        if (orderObj != null
//                                                && !orderObj.isEmpty()
//                                                && orderObj.get("columnName") != null
//                                                && !"".equalsIgnoreCase(String.valueOf(orderObj.get("columnName")))
//                                                && !"null".equalsIgnoreCase(String.valueOf(orderObj.get("columnName")))) {
//                                            orderByClause += "" + String.valueOf(orderObj.get("columnName")).replaceAll(":", ".") + " "
//                                                    + " " + (("DESC".equalsIgnoreCase(String.valueOf(orderObj.get("direction")))) ? "DESC" : "ASC");
//                                            if (orderIndex != orderByObj.size() - 1) {
//                                                orderByClause += ",";
//                                            }
//                                            orderIndex++;
//                                        }
//                                    }
//                                    //columnName,direction
//                                    if (orderByClause != null
//                                            && !"".equalsIgnoreCase(orderByClause)
//                                            && !"null".equalsIgnoreCase(orderByClause)) {
//                                        query += " ORDER BY  " + orderByClause + " ";
//                                    }
//                                }
//                                String groupByQuery = (String) orderAndGroupByObj.get("groupByQuery");
//                                if (groupByQuery != null
//                                        && !"".equalsIgnoreCase(groupByQuery)
//                                        && !"null".equalsIgnoreCase(groupByQuery)) {
//                                    query += " GROUP BY  " + groupByQuery + " ";
//                                }
//
//                            }
////                            if (tablesObj != null && !tablesObj.isEmpty()) {
////                                String tableInputVal = (String) tablesObj.get(tableName);
////                                query += " " + colsObj + " FROM " + tableName + "";
////                                if (tableInputVal != null && !"".equalsIgnoreCase(tableInputVal) && !"null".equalsIgnoreCase(tableInputVal)) {
////                                    query += " WHERE " + tableInputVal + " ";
////                                }
////                                if (orderAndGroupByObj != null && !orderAndGroupByObj.isEmpty()) {
////                                    String orderByClause = "";
////                                    JSONObject orderByObj = (JSONObject) orderAndGroupByObj.get("orderByObj");
////                                    if (orderByObj != null
////                                            && !orderByObj.isEmpty()) {
////                                        int orderIndex = 0;
////                                        for (Object orderByKey : orderByObj.keySet()) {
////                                            JSONObject orderObj = (JSONObject) orderByObj.get(orderByKey);
////                                            if (orderObj != null
////                                                    && !orderObj.isEmpty()
////                                                    && orderObj.get("columnName") != null
////                                                    && !"".equalsIgnoreCase(String.valueOf(orderObj.get("columnName")))
////                                                    && !"null".equalsIgnoreCase(String.valueOf(orderObj.get("columnName")))) {
////                                                orderByClause += "" + String.valueOf(orderObj.get("columnName")).replaceAll(":", ".") + " "
////                                                        + " " + (("DESC".equalsIgnoreCase(String.valueOf(orderObj.get("direction")))) ? "DESC" : "ASC");
////                                                if (orderIndex != orderByObj.size() - 1) {
////                                                    orderByClause += ",";
////                                                }
////                                                orderIndex++;
////                                            }
////                                        }
////                                        //columnName,direction
////                                        if (orderByClause != null
////                                                && !"".equalsIgnoreCase(orderByClause)
////                                                && !"null".equalsIgnoreCase(orderByClause)) {
////                                            query += " ORDER BY  " + orderByClause + " ";
////                                        } else {
////                                            query += " ORDER BY " + columnsList.get(0);
////                                        }
////                                    } else {
////                                        query += " ORDER BY " + columnsList.get(0);
////                                    }
////
////                                    String groupByQuery = (String) orderAndGroupByObj.get("groupByQuery");
////                                    if (groupByQuery != null
////                                            && !"".equalsIgnoreCase(groupByQuery)
////                                            && !"null".equalsIgnoreCase(groupByQuery)) {
////                                        query += " GROUP BY  " + groupByQuery + " ";
////                                    }
////
////                                } else {
////                                    query += " ORDER BY " + columnsList.get(0);
////                                }
////
////                                query += " OFFSET " + start + " ROWS FETCH NEXT " + limit + " ROWS ONLY";
////                            }
//
//                            query += " OFFSET " + start + " ROWS FETCH NEXT " + limit + " ROWS ONLY";
//                            if ((selectTabObj != null
//                                    && !selectTabObj.isEmpty()
//                                    && "Y".equalsIgnoreCase(String.valueOf(selectTabObj.get("uniqueRowsFlag"))))) {//uniqueRowsFlag
//                                query = "SELECT  DISTINCT " + query;
//                            } else {
//                                query = "SELECT  " + query;
//                            }
//                            System.out.println("query::::" + query);
//                            preparedStatement = connection.prepareStatement(query);
//                            resultSet = preparedStatement.executeQuery();
//                            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
//                            int columnCount = resultSetMetaData.getColumnCount();
//                            while (resultSet.next()) {
//                                Map dataObj = new HashMap();
//                                if (columnsList != null && !columnsList.isEmpty()) {
//                                    for (int i = 0; i < columnsList.size(); i++) {
//                                        dataObj.put(tableName + ":" + resultSetMetaData.getColumnName(i + 1), resultSet.getString(resultSetMetaData.getColumnName(i + 1)));
//                                    }
//                                    if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
//                                        for (Object toColumnName : columnClauseObj.keySet()) {
//                                            if (columnClauseObj.get(toColumnName) instanceof JSONObject) {
//                                                JSONObject funObj = (JSONObject) columnClauseObj.get(toColumnName);
//                                                if (funObj != null
//                                                        && !funObj.isEmpty()) {
//                                                    String functionMainObjStr = (String) funObj.get("funobjstr");
//                                                    if (functionMainObjStr != null
//                                                            && !"".equalsIgnoreCase(functionMainObjStr)
//                                                            && !"null".equalsIgnoreCase(functionMainObjStr)) {
//                                                        JSONObject functionMainObj = (JSONObject) JSONValue.parse(functionMainObjStr);
//                                                        if (functionMainObj != null && !functionMainObj.isEmpty()
//                                                                && "DATA".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))) {
//                                                            String processClass = (String) functionMainObj.get("DM_FUN_CUST_COL1");
//                                                            String processMethod = (String) functionMainObj.get("DM_FUN_CUST_COL2");
//                                                            if (processClass != null
//                                                                    && !"".equalsIgnoreCase(processClass)
//                                                                    && !"null".equalsIgnoreCase(processClass)
//                                                                    && processMethod != null
//                                                                    && !"".equalsIgnoreCase(processMethod)
//                                                                    && !"null".equalsIgnoreCase(processMethod)) {
//                                                                try {
//                                                                    Class clazz = Class.forName(processClass);
//                                                                    Class<?>[] paramTypes = {JSONObject.class,
//                                                                        Connection.class, String.class, String.class,
//                                                                        String.class, String.class, String.class, String.class, String.class};
//                                                                    Object targetObj = new PilogUtilities().createObjectByName(processClass);
//                                                                    Method method = clazz.getMethod(processMethod.trim(), paramTypes);
//                                                                    String resultStr = (String) method.invoke(targetObj,
//                                                                            functionMainObj,
//                                                                            (toConnection != null ? toConnection : connection),
//                                                                            dataBaseDriver,
//                                                                            dbURL,
//                                                                            dbUserName,
//                                                                            dbPassword,
//                                                                            loginUserName,
//                                                                            loginOrgnId, String.valueOf(lastSeqObj.get(toColumnName)));
//                                                                    if (resultStr != null
//                                                                            && !"".equalsIgnoreCase(resultStr)
//                                                                            && !"null".equalsIgnoreCase(resultStr)) {
//                                                                        lastSeqObj.put(toColumnName, resultStr);
//                                                                        dataObj.put(toColumnName, resultStr);
//                                                                    }
//                                                                } catch (Exception e) {
//                                                                    continue;
//                                                                }
//
//                                                            } else {
//                                                            }
//                                                        } else if (functionMainObj != null && !functionMainObj.isEmpty()
//                                                                && ("QUERY".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
//                                                                || "COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
//                                                                || "MULTI_COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE")))) {
//                                                            dataObj.put(toColumnName, resultSet.getString((String) toColumnName));
//                                                        }
//                                                    }
//
//                                                }
//                                            }
//
//                                        }
//                                    }
//                                    totalData.add(dataObj);
//                                }
//
//                            }
//
//                        }
//
//                    }
//                }
//
//            }
//            try {
//                processETLLog(loginUserName,
//                        loginOrgnId, "   ", "INFO", 1, "Y",
//                        dataBaseDriver, dbURL, dbUserName, dbPassword, lastSeqObj.toJSONString(), String.valueOf(selectTabObj.get("jobId")));
//            } catch (Exception ex) {
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//            try {
//                processETLLog(loginUserName,
//                        loginOrgnId, "Failed to fetch the data due to " + e.getMessage(), "ERROR", 1, "Y",
//                        dataBaseDriver, dbURL, dbUserName, dbPassword, String.valueOf(selectTabObj.get("jobId")));
//            } catch (Exception ex) {
//            }
//        } finally {
//            try {
//                if (resultSet != null) {
//                    resultSet.close();
//                }
//                if (preparedStatement != null) {
//                    preparedStatement.close();
//                }
//
//            } catch (Exception e) {
//            }
//        }
//        return totalData;
//    }

    public List getOracleSelectedJoinColumnsData(Connection connection,
            //JSONObject tableColsObj,
            Map tableColsObj,// ravi etl integration
            Integer start, Integer limit,
            JSONObject tablesWhereClauseObj, String serviceName,
            JSONObject joinColsObj, String joinQuery,
            String loginUserName, String loginOrgnId,
            String dataBaseDriver, String dbURL,
            String dbUserName,
            String dbPassword,
            JSONObject orderAndGroupByObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj, Connection toConnection) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        String result = "";
        List totalData = new ArrayList();
        JSONObject lastSeqObj = new JSONObject();
        try {
            if (joinColsObj != null && !joinColsObj.isEmpty()) {
                String fetchColumns = "";
                int i = 0;

                for (Object tableName : joinColsObj.keySet()) {
                    if (tableColsObj != null && !tableColsObj.isEmpty() && tableColsObj.containsKey(tableName)) {
                        String currentTableCols = (String) tableColsObj.get(tableName);
                        if (currentTableCols != null && !"".equalsIgnoreCase(currentTableCols)) {
                            List<String> colsList = new ArrayList<>();
                            Set matchedColsSet = new HashSet();
                            colsList.addAll(Arrays.asList(currentTableCols.trim().replaceAll(" ", "_").split(",")));
                            for (Object tableObjKey : tableColsObj.keySet()) {
                                if (!String.valueOf(tableObjKey).equalsIgnoreCase(String.valueOf(tableName))) {
                                    List<String> colsListCompare = new ArrayList<>();
                                    colsListCompare.addAll(Arrays.asList((String.valueOf(tableColsObj.get(tableObjKey))).split(",")));
                                    List matchedList = colsList.stream().filter(colName -> (colsListCompare.contains(colName))).collect(Collectors.toList());
                                    matchedColsSet.addAll(matchedList);

                                }
                            }
                            Set<String> items = new HashSet<>();
                            Set matchedSet = colsList.stream()
                                    .filter(n -> !items.add(n)) // Set.add() returns false if the element was already in the set.
                                    .collect(Collectors.toSet());
                            if (matchedSet != null && !matchedSet.isEmpty()) {
                                matchedColsSet.addAll(matchedSet);
                            }
                            for (int j = 0; j < colsList.size(); j++) {
                                if (matchedColsSet != null && !matchedColsSet.isEmpty() && matchedColsSet.contains(colsList.get(j))) {
                                    fetchColumns += tableName + "." + colsList.get(j) + " AS " + colsList.get(j) + "_" + i + "" + j;
                                } else {
                                    fetchColumns += tableName + "." + colsList.get(j);
                                }
                                if (j != colsList.size() - 1) {
                                    fetchColumns += ",";
                                }
                            }

//                            currentTableCols = tableName + "." + currentTableCols.replaceAll(",", "," + tableName + ".");
//                            fetchColumns += currentTableCols;
                            if (i != joinColsObj.size() - 1) {
                                fetchColumns += ",";
                            }

                        }
                    }

                    i++;
                }
                if (fetchColumns != null && !"".equalsIgnoreCase(fetchColumns)) {
                    List<String> columnsList = Arrays.asList(fetchColumns.split(","));
                    fetchColumns = new PilogUtilities().trimChar(fetchColumns, ',');
                    if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
                        for (Object toColumnName : columnClauseObj.keySet()) {
                            if (columnClauseObj.get(toColumnName) instanceof JSONObject) {
                                JSONObject funObj = (JSONObject) columnClauseObj.get(toColumnName);
                                if (funObj != null
                                        && !funObj.isEmpty()) {
                                    String functionMainObjStr = (String) funObj.get("funobjstr");
                                    if (functionMainObjStr != null
                                            && !"".equalsIgnoreCase(functionMainObjStr)
                                            && !"null".equalsIgnoreCase(functionMainObjStr)) {
                                        JSONObject functionMainObj = (JSONObject) JSONValue.parse(functionMainObjStr);
                                        if (functionMainObj != null && !functionMainObj.isEmpty()
                                                && ("QUERY".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
                                                || "COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE")) //                                                || "MULTI_COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
                                                )) {
                                            if ("DM_COL_AGGREGATE".equalsIgnoreCase(String.valueOf(functionMainObj.get("HL_FUN_ID")))) {
                                                String aggTableName = String.valueOf(functionMainObj.get("COLUMN_NAME"));
                                                if (aggTableName != null
                                                        && !"".equalsIgnoreCase(aggTableName)
                                                        && !"null".equalsIgnoreCase(aggTableName)
                                                        && aggTableName.contains(":")) {
                                                    aggTableName = aggTableName.split(":")[0];
                                                }

                                                fetchColumns += ",(SELECT " + String.valueOf(functionMainObj.get("mainFunStr")).replace(":", ".") + " FROM " + aggTableName + " ) AS  " + toColumnName;
                                            } else {
                                                fetchColumns += ",(" + String.valueOf(functionMainObj.get("mainFunStr")).replace(":", ".") + ") AS  " + toColumnName;
                                            }
//                                            fetchColumns += ",(" + String.valueOf(columnClauseObj.get(toColumnName)).replace(":", ".") + ") AS  " + toColumnName;
                                        } else if (functionMainObj != null && !functionMainObj.isEmpty()
                                                && "MULTI_COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))) {
                                            JSONObject multiColumnsObj = (JSONObject) functionMainObj.get("multiColumnsObj");
                                            if (multiColumnsObj != null && !multiColumnsObj.isEmpty()) {
                                                String multColStr = new V10DataPipingUtills().generateMultiColumnsObj(multiColumnsObj, (String) functionMainObj.get("functionName"));
                                                if (multColStr != null
                                                        && !"".equalsIgnoreCase(multColStr)
                                                        && !"null".equalsIgnoreCase(multColStr)) {
                                                    fetchColumns += ",(" + multColStr + ") AS  " + toColumnName;
                                                }
                                            }

                                        }
                                    }

                                }
                            }

                        }
                    }
//                    String rowNumStr = "ROWNUM AS RNO,";
                    String selectQuery = "SELECT ";
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
                    int end = 0;
                    if (start == 0) {
                        end = limit;
                    } else {
                        end = (start + limit - 1);
                    }
//                    if ((selectTabObj != null
//                            && !selectTabObj.isEmpty()
//                            && "Y".equalsIgnoreCase(String.valueOf(selectTabObj.get("uniqueRowsFlag"))))) {
//                        selectQuery = "SELECT " + rowNumStr + "A.* FROM (" + selectQuery + ") A";
//                    }
                    if (orderAndGroupByObj != null && !orderAndGroupByObj.isEmpty()) {
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
                        String groupByQuery = (String) orderAndGroupByObj.get("groupByQuery");
                        if (groupByQuery != null
                                && !"".equalsIgnoreCase(groupByQuery)
                                && !"null".equalsIgnoreCase(groupByQuery)) {
                            selectQuery += " GROUP BY  " + groupByQuery + " ";
                        }

                    }
                    selectQuery += " OFFSET " + (start - 1) + " ROWS FETCH NEXT " + limit + " ROWS ONLY ";
//                    selectQuery = "SELECT * FROM (" + selectQuery + ") WHERE RNO BETWEEN " + start + " AND " + (end) + "";
                    System.out.println("query::::" + selectQuery);
                    preparedStatement = connection.prepareStatement(selectQuery);
                    resultSet = preparedStatement.executeQuery();
                    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

                    while (resultSet.next()) {
                        Map dataObj = new HashMap();
                        if (columnsList != null && !columnsList.isEmpty()) {
                            for (int j = 0; j < columnsList.size(); j++) {
                                dataObj.put(columnsList.get(j).replace(".", ":").replaceFirst(":", ".").split(" AS ")[0], resultSet.getString(j + 1));
                            }
                            if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
                                for (Object toColumnName : columnClauseObj.keySet()) {
                                    if (columnClauseObj.get(toColumnName) instanceof JSONObject) {
                                        JSONObject funObj = (JSONObject) columnClauseObj.get(toColumnName);
                                        if (funObj != null
                                                && !funObj.isEmpty()) {
                                            String functionMainObjStr = (String) funObj.get("funobjstr");
                                            if (functionMainObjStr != null
                                                    && !"".equalsIgnoreCase(functionMainObjStr)
                                                    && !"null".equalsIgnoreCase(functionMainObjStr)) {
                                                JSONObject functionMainObj = (JSONObject) JSONValue.parse(functionMainObjStr);
                                                if (functionMainObj != null && !functionMainObj.isEmpty()
                                                        && "DATA".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))) {
                                                    String processClass = (String) functionMainObj.get("DM_FUN_CUST_COL1");
                                                    String processMethod = (String) functionMainObj.get("DM_FUN_CUST_COL2");
                                                    if (processClass != null
                                                            && !"".equalsIgnoreCase(processClass)
                                                            && !"null".equalsIgnoreCase(processClass)
                                                            && processMethod != null
                                                            && !"".equalsIgnoreCase(processMethod)
                                                            && !"null".equalsIgnoreCase(processMethod)) {
                                                        try {
                                                            Class clazz = Class.forName(processClass);
                                                            Class<?>[] paramTypes = {JSONObject.class,
                                                                Connection.class, String.class, String.class,
                                                                String.class, String.class, String.class, String.class, String.class, String.class};
                                                            Object targetObj = new PilogUtilities().createObjectByName(processClass);
                                                            Method method = clazz.getMethod(processMethod.trim(), paramTypes);
                                                            String resultStr = (String) method.invoke(targetObj,
                                                                    functionMainObj,
                                                                    (toConnection != null ? toConnection : connection),
                                                                    dataBaseDriver,
                                                                    dbURL,
                                                                    dbUserName,
                                                                    dbPassword,
                                                                    loginUserName,
                                                                    loginOrgnId, String.valueOf(lastSeqObj.get(toColumnName)), String.valueOf(toColumnName));
                                                            if (resultStr != null
                                                                    && !"".equalsIgnoreCase(resultStr)
                                                                    && !"null".equalsIgnoreCase(resultStr)) {
                                                                lastSeqObj.put(toColumnName, resultStr);
                                                                dataObj.put(toColumnName, resultStr);
                                                            }
                                                        } catch (Exception e) {
                                                            continue;
                                                        }

                                                    } else {

                                                    }
                                                } else if (functionMainObj != null && !functionMainObj.isEmpty()
                                                        && ("QUERY".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
                                                        || "COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
                                                        || "MULTI_COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE")))) {
                                                    dataObj.put(toColumnName, resultSet.getString((String) toColumnName));
                                                }
                                            }

                                        }
                                    }

                                }
                            }
                            totalData.add(dataObj);
                        }

                    }

                }

            }
            try {
                processETLLog(loginUserName,
                        loginOrgnId, "   ", "INFO", 1, "Y",
                        dataBaseDriver, dbURL, dbUserName, dbPassword, lastSeqObj.toJSONString(), String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }
        } catch (Exception e) {
            e.printStackTrace();
            try {
                processETLLog(loginUserName,
                        loginOrgnId, "Failed to fetch the data due to " + e.getMessage(), "ERROR", 1, "Y",
                        dataBaseDriver, dbURL, dbUserName, dbPassword, String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }
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

    public List getMYSQLSelectedJoinColumnsData(Connection connection,
            //JSONObject tableColsObj,
            Map tableColsObj,// ravi etl integration
            Integer start, Integer limit,
            JSONObject tablesWhereClauseObj, String serviceName, JSONObject joinColsObj,
            String joinQuery, String loginUserName, String loginOrgnId,
            String dataBaseDriver, String dbURL, String dbUserName, String dbPassword,
            JSONObject orderAndGroupByObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj, Connection toConnection) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        String result = "";
        List totalData = new ArrayList();
        JSONObject lastSeqObj = new JSONObject();
        try {
            if (joinColsObj != null && !joinColsObj.isEmpty()) {
                String fetchColumns = "";
                int i = 0;
                for (Object tableName : joinColsObj.keySet()) {
                    if (tableColsObj != null && !tableColsObj.isEmpty() && tableColsObj.containsKey(tableName)) {
                        String currentTableCols = (String) tableColsObj.get(tableName);
                        if (currentTableCols != null && !"".equalsIgnoreCase(currentTableCols)) {
                            List<String> colsList = new ArrayList<>();
                            Set matchedColsSet = new HashSet();
                            colsList.addAll(Arrays.asList(currentTableCols.split(",")));
                            for (Object tableObjKey : tableColsObj.keySet()) {
                                if (!String.valueOf(tableObjKey).equalsIgnoreCase(String.valueOf(tableName))) {
                                    List<String> colsListCompare = new ArrayList<>();
                                    colsListCompare.addAll(Arrays.asList((String.valueOf(tableColsObj.get(tableObjKey))).split(",")));
                                    List matchedList = colsList.stream().filter(colName -> (colsListCompare.contains(colName))).collect(Collectors.toList());
                                    matchedColsSet.addAll(matchedList);
                                }
                            }
                            Set<String> items = new HashSet<>();
                            Set matchedSet = colsList.stream()
                                    .filter(n -> !items.add(n)) // Set.add() returns false if the element was already in the set.
                                    .collect(Collectors.toSet());
                            if (matchedSet != null && !matchedSet.isEmpty()) {
                                matchedColsSet.addAll(matchedSet);
                            }
                            for (int j = 0; j < colsList.size(); j++) {
                                if (matchedColsSet != null && !matchedColsSet.isEmpty() && matchedColsSet.contains(colsList.get(j))) {
                                    fetchColumns += tableName + "." + colsList.get(j) + " AS " + colsList.get(j) + "_" + i + "" + j;
                                } else {
                                    fetchColumns += tableName + "." + colsList.get(j);
                                }

                                if (j != colsList.size() - 1) {
                                    fetchColumns += ",";
                                }
                            }

//                            currentTableCols = tableName + "." + currentTableCols.replaceAll(",", "," + tableName + ".");
//                            fetchColumns += currentTableCols;
                            if (i != joinColsObj.size() - 1) {
                                fetchColumns += ",";
                            }

                        }
                    }

                    i++;
                }

                if (fetchColumns != null && !"".equalsIgnoreCase(fetchColumns)) {
                    List<String> columnsList = Arrays.asList(fetchColumns.split(","));
                    fetchColumns = new PilogUtilities().trimChar(fetchColumns, ',');
                    if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
                        for (Object toColumnName : columnClauseObj.keySet()) {
                            if (columnClauseObj.get(toColumnName) instanceof JSONObject) {
                                JSONObject funObj = (JSONObject) columnClauseObj.get(toColumnName);
                                if (funObj != null
                                        && !funObj.isEmpty()) {
                                    String functionMainObjStr = (String) funObj.get("funobjstr");
                                    if (functionMainObjStr != null
                                            && !"".equalsIgnoreCase(functionMainObjStr)
                                            && !"null".equalsIgnoreCase(functionMainObjStr)) {
                                        JSONObject functionMainObj = (JSONObject) JSONValue.parse(functionMainObjStr);
                                        if (functionMainObj != null && !functionMainObj.isEmpty()
                                                && ("QUERY".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
                                                || "COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE")) //                                                || "MULTI_COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
                                                )) {
                                            if ("DM_COL_AGGREGATE".equalsIgnoreCase(String.valueOf(functionMainObj.get("HL_FUN_ID")))) {
                                                String aggTableName = String.valueOf(functionMainObj.get("COLUMN_NAME"));
                                                if (aggTableName != null
                                                        && !"".equalsIgnoreCase(aggTableName)
                                                        && !"null".equalsIgnoreCase(aggTableName)
                                                        && aggTableName.contains(":")) {
                                                    aggTableName = aggTableName.split(":")[0];
                                                }

                                                fetchColumns += ",(SELECT " + String.valueOf(functionMainObj.get("mainFunStr")).replace(":", ".") + " FROM " + aggTableName + " ) AS  " + toColumnName;
                                            } else {
                                                fetchColumns += ",(" + String.valueOf(functionMainObj.get("mainFunStr")).replace(":", ".") + ") AS  " + toColumnName;
                                            }
//                                            fetchColumns += ",(" + String.valueOf(columnClauseObj.get(toColumnName)).replace(":", ".") + ") AS  " + toColumnName;
                                        } else if (functionMainObj != null && !functionMainObj.isEmpty()
                                                && "MULTI_COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))) {
                                            JSONObject multiColumnsObj = (JSONObject) functionMainObj.get("multiColumnsObj");
                                            if (multiColumnsObj != null && !multiColumnsObj.isEmpty()) {
                                                String multColStr = new V10DataPipingUtills().generateMultiColumnsObj(multiColumnsObj, (String) functionMainObj.get("functionName"));
                                                if (multColStr != null
                                                        && !"".equalsIgnoreCase(multColStr)
                                                        && !"null".equalsIgnoreCase(multColStr)) {
                                                    fetchColumns += ",(" + multColStr + ") AS  " + toColumnName;
                                                }
                                            }

                                        }
                                    }

                                }
                            }

                        }
                    }
                    String selectQuery = "SELECT ";
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
                    if (orderAndGroupByObj != null && !orderAndGroupByObj.isEmpty()) {
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
                                selectQuery += " ORDER BY  " + orderByClause + " ";
                            }
                        }
                        String groupByQuery = (String) orderAndGroupByObj.get("groupByQuery");
                        if (groupByQuery != null
                                && !"".equalsIgnoreCase(groupByQuery)
                                && !"null".equalsIgnoreCase(groupByQuery)) {
                            selectQuery += " GROUP BY  " + groupByQuery + " ";
                        }

                    }
                    selectQuery += " LIMIT " + (start - 1) + " , " + limit + " ";
                    System.out.println("query::::" + selectQuery);
                    preparedStatement = connection.prepareStatement(selectQuery);
                    resultSet = preparedStatement.executeQuery();
                    while (resultSet.next()) {
                        Map dataObj = new HashMap();
                        if (columnsList != null && !columnsList.isEmpty()) {
                            for (int j = 0; j < columnsList.size(); j++) {
                                dataObj.put(columnsList.get(j).replace(".", ":").replaceFirst(":", ".").split(" AS ")[0], resultSet.getString(j + 1));
                            }
                            if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
                                for (Object toColumnName : columnClauseObj.keySet()) {
                                    if (columnClauseObj.get(toColumnName) instanceof JSONObject) {
                                        JSONObject funObj = (JSONObject) columnClauseObj.get(toColumnName);
                                        if (funObj != null
                                                && !funObj.isEmpty()) {
                                            String functionMainObjStr = (String) funObj.get("funobjstr");
                                            if (functionMainObjStr != null
                                                    && !"".equalsIgnoreCase(functionMainObjStr)
                                                    && !"null".equalsIgnoreCase(functionMainObjStr)) {
                                                JSONObject functionMainObj = (JSONObject) JSONValue.parse(functionMainObjStr);
                                                if (functionMainObj != null && !functionMainObj.isEmpty()
                                                        && "DATA".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))) {
                                                    String processClass = (String) functionMainObj.get("DM_FUN_CUST_COL1");
                                                    String processMethod = (String) functionMainObj.get("DM_FUN_CUST_COL2");
                                                    if (processClass != null
                                                            && !"".equalsIgnoreCase(processClass)
                                                            && !"null".equalsIgnoreCase(processClass)
                                                            && processMethod != null
                                                            && !"".equalsIgnoreCase(processMethod)
                                                            && !"null".equalsIgnoreCase(processMethod)) {
                                                        try {
                                                            Class clazz = Class.forName(processClass);
                                                            Class<?>[] paramTypes = {JSONObject.class,
                                                                Connection.class, String.class, String.class,
                                                                String.class, String.class, String.class, String.class, String.class, String.class};
                                                            Object targetObj = new PilogUtilities().createObjectByName(processClass);
                                                            Method method = clazz.getMethod(processMethod.trim(), paramTypes);
                                                            String resultStr = (String) method.invoke(targetObj,
                                                                    functionMainObj,
                                                                    (toConnection != null ? toConnection : connection),
                                                                    dataBaseDriver,
                                                                    dbURL,
                                                                    dbUserName,
                                                                    dbPassword,
                                                                    loginUserName,
                                                                    loginOrgnId, String.valueOf(lastSeqObj.get(toColumnName)), String.valueOf(toColumnName));
                                                            if (resultStr != null
                                                                    && !"".equalsIgnoreCase(resultStr)
                                                                    && !"null".equalsIgnoreCase(resultStr)) {
                                                                lastSeqObj.put(toColumnName, resultStr);
                                                                dataObj.put(toColumnName, resultStr);
                                                            }
                                                        } catch (Exception e) {
                                                            continue;
                                                        }

                                                    } else {
                                                    }
                                                } else if (functionMainObj != null && !functionMainObj.isEmpty()
                                                        && ("QUERY".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
                                                        || "COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
                                                        || "MULTI_COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE")))) {
                                                    dataObj.put(toColumnName, resultSet.getString((String) toColumnName));
                                                }
                                            }

                                        }
                                    }

                                }
                            }
                            totalData.add(dataObj);
                        }

                    }

                }

            }
            try {
                processETLLog(loginUserName,
                        loginOrgnId, "   ", "INFO", 1, "Y",
                        dataBaseDriver, dbURL, dbUserName, dbPassword, lastSeqObj.toJSONString(), String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }
        } catch (Exception e) {
            e.printStackTrace();
            try {
                processETLLog(loginUserName,
                        loginOrgnId, "Failed to fetch the data due to " + e.getMessage(), "ERROR", 1, "Y",
                        dataBaseDriver, dbURL, dbUserName, dbPassword, String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }
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

    public List getMSSQLSelectedJoinColumnsData(Connection connection,
            //JSONObject tableColsObj,
            Map tableColsObj,// ravi etl integration
            Integer start, Integer limit,
            JSONObject tablesWhereClauseObj, String serviceName, JSONObject joinColsObj,
            String joinQuery, String loginUserName, String loginOrgnId,
            String dataBaseDriver, String dbURL, String dbUserName, String dbPassword,
            JSONObject orderAndGroupByObj,
            JSONObject columnClauseObj,
            JSONObject selectTabObj, Connection toConnection) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        String result = "";
        List totalData = new ArrayList();
        JSONObject lastSeqObj = new JSONObject();
        try {
            if (joinColsObj != null && !joinColsObj.isEmpty()) {
                String fetchColumns = "";
                int i = 0;
                for (Object tableName : joinColsObj.keySet()) {
                    if (tableColsObj != null && !tableColsObj.isEmpty() && tableColsObj.containsKey(tableName)) {
                        String currentTableCols = (String) tableColsObj.get(tableName);
                        if (currentTableCols != null && !"".equalsIgnoreCase(currentTableCols)) {
                            List<String> colsList = new ArrayList<>();
                            Set matchedColsSet = new HashSet();
                            colsList.addAll(Arrays.asList(currentTableCols.split(",")));
                            for (Object tableObjKey : tableColsObj.keySet()) {
                                if (!String.valueOf(tableObjKey).equalsIgnoreCase(String.valueOf(tableName))) {
                                    List<String> colsListCompare = new ArrayList<>();
                                    colsListCompare.addAll(Arrays.asList((String.valueOf(tableColsObj.get(tableObjKey))).split(",")));
                                    List matchedList = colsList.stream().filter(colName -> (colsListCompare.contains(colName))).collect(Collectors.toList());
                                    matchedColsSet.addAll(matchedList);
                                }
                            }
                            Set<String> items = new HashSet<>();
                            Set matchedSet = colsList.stream()
                                    .filter(n -> !items.add(n)) // Set.add() returns false if the element was already in the set.
                                    .collect(Collectors.toSet());
                            if (matchedSet != null && !matchedSet.isEmpty()) {
                                matchedColsSet.addAll(matchedSet);
                            }
                            for (int j = 0; j < colsList.size(); j++) {
                                if (matchedColsSet != null && !matchedColsSet.isEmpty() && matchedColsSet.contains(colsList.get(j))) {
                                    fetchColumns += tableName + "." + colsList.get(j) + " AS " + colsList.get(j) + "_" + i + "" + j;
                                } else {
                                    fetchColumns += tableName + "." + colsList.get(j);
                                }

                                if (j != colsList.size() - 1) {
                                    fetchColumns += ",";
                                }
                            }

//                            currentTableCols = tableName + "." + currentTableCols.replaceAll(",", "," + tableName + ".");
//                            fetchColumns += currentTableCols;
                            if (i != joinColsObj.size() - 1) {
                                fetchColumns += ",";
                            }

                        }
                    }

                    i++;
                }

                if (fetchColumns != null && !"".equalsIgnoreCase(fetchColumns)) {
                    List<String> columnsList = Arrays.asList(fetchColumns.split(","));
                    fetchColumns = new PilogUtilities().trimChar(fetchColumns, ',');
                    if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
                        for (Object toColumnName : columnClauseObj.keySet()) {
                            if (columnClauseObj.get(toColumnName) instanceof JSONObject) {
                                JSONObject funObj = (JSONObject) columnClauseObj.get(toColumnName);
                                if (funObj != null
                                        && !funObj.isEmpty()) {
                                    String functionMainObjStr = (String) funObj.get("funobjstr");
                                    if (functionMainObjStr != null
                                            && !"".equalsIgnoreCase(functionMainObjStr)
                                            && !"null".equalsIgnoreCase(functionMainObjStr)) {
                                        JSONObject functionMainObj = (JSONObject) JSONValue.parse(functionMainObjStr);
                                        if (functionMainObj != null && !functionMainObj.isEmpty()
                                                && ("QUERY".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
                                                || "COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE")) //                                                || "MULTI_COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
                                                )) {
                                            if ("DM_COL_AGGREGATE".equalsIgnoreCase(String.valueOf(functionMainObj.get("HL_FUN_ID")))) {
                                                String aggTableName = String.valueOf(functionMainObj.get("COLUMN_NAME"));
                                                if (aggTableName != null
                                                        && !"".equalsIgnoreCase(aggTableName)
                                                        && !"null".equalsIgnoreCase(aggTableName)
                                                        && aggTableName.contains(":")) {
                                                    aggTableName = aggTableName.split(":")[0];
                                                }

                                                fetchColumns += ",(SELECT " + String.valueOf(functionMainObj.get("mainFunStr")).replace(":", ".") + " FROM " + aggTableName + " ) AS  " + toColumnName;
                                            } else {
                                                fetchColumns += ",(" + String.valueOf(functionMainObj.get("mainFunStr")).replace(":", ".") + ") AS  " + toColumnName;
                                            }
//                                            fetchColumns += ",(" + String.valueOf(columnClauseObj.get(toColumnName)).replace(":", ".") + ") AS  " + toColumnName;
                                        } else if (functionMainObj != null && !functionMainObj.isEmpty()
                                                && "MULTI_COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))) {
                                            JSONObject multiColumnsObj = (JSONObject) functionMainObj.get("multiColumnsObj");
                                            if (multiColumnsObj != null && !multiColumnsObj.isEmpty()) {
                                                String multColStr = new V10DataPipingUtills().generateMultiColumnsObj(multiColumnsObj, (String) functionMainObj.get("functionName"));
                                                if (multColStr != null
                                                        && !"".equalsIgnoreCase(multColStr)
                                                        && !"null".equalsIgnoreCase(multColStr)) {
                                                    fetchColumns += ",(" + multColStr + ") AS  " + toColumnName;
                                                }
                                            }

                                        }
                                    }

                                }
                            }

                        }
                    }
                    String selectQuery = "SELECT ";
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
                    if (orderAndGroupByObj != null && !orderAndGroupByObj.isEmpty()) {
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
                                selectQuery += " ORDER BY  " + orderByClause + " ";
                            } else {
                                selectQuery += "  ORDER BY " + columnsList.get(0) + " ";
                            }
                        } else {
                            selectQuery += "  ORDER BY " + columnsList.get(0) + " ";
                        }

                        String groupByQuery = (String) orderAndGroupByObj.get("groupByQuery");
                        if (groupByQuery != null
                                && !"".equalsIgnoreCase(groupByQuery)
                                && !"null".equalsIgnoreCase(groupByQuery)) {
                            selectQuery += " GROUP BY  " + groupByQuery + " ";
                        }
                    } else {
                        selectQuery += "  ORDER BY " + columnsList.get(0) + " ";
                    }
                    selectQuery += "  OFFSET " + (start - 1) + " ROWS FETCH NEXT " + limit + " ROWS ONLY";
//                    int end = 0;
//                    if (start == 0) {
//                        end = limit;
//                    }else{
//                        end = (start + limit - 1);
//                    }
//                    selectQuery = "SELECT * FROM (" + selectQuery + ") WHERE RNO BETWEEN " + start + " AND " + (end) + "";
                    System.out.println("query::::" + selectQuery);
                    preparedStatement = connection.prepareStatement(selectQuery);
                    resultSet = preparedStatement.executeQuery();

                    while (resultSet.next()) {
                        Map dataObj = new HashMap();
                        if (columnsList != null && !columnsList.isEmpty()) {
                            for (int j = 0; j < columnsList.size(); j++) {
                                dataObj.put(columnsList.get(j).replace(".", ":").replaceFirst(":", ".").split(" AS ")[0], resultSet.getString(j + 1));
                            }
                            if (columnClauseObj != null && !columnClauseObj.isEmpty()) {
                                for (Object toColumnName : columnClauseObj.keySet()) {
                                    if (columnClauseObj.get(toColumnName) instanceof JSONObject) {
                                        JSONObject funObj = (JSONObject) columnClauseObj.get(toColumnName);
                                        if (funObj != null
                                                && !funObj.isEmpty()) {
                                            String functionMainObjStr = (String) funObj.get("funobjstr");
                                            if (functionMainObjStr != null
                                                    && !"".equalsIgnoreCase(functionMainObjStr)
                                                    && !"null".equalsIgnoreCase(functionMainObjStr)) {
                                                JSONObject functionMainObj = (JSONObject) JSONValue.parse(functionMainObjStr);
                                                if (functionMainObj != null && !functionMainObj.isEmpty()
                                                        && "DATA".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))) {
                                                    String processClass = (String) functionMainObj.get("DM_FUN_CUST_COL1");
                                                    String processMethod = (String) functionMainObj.get("DM_FUN_CUST_COL2");
                                                    if (processClass != null
                                                            && !"".equalsIgnoreCase(processClass)
                                                            && !"null".equalsIgnoreCase(processClass)
                                                            && processMethod != null
                                                            && !"".equalsIgnoreCase(processMethod)
                                                            && !"null".equalsIgnoreCase(processMethod)) {
                                                        try {
                                                            Class clazz = Class.forName(processClass);
                                                            Class<?>[] paramTypes = {JSONObject.class,
                                                                Connection.class, String.class, String.class,
                                                                String.class, String.class, String.class, String.class, String.class, String.class};
                                                            Object targetObj = new PilogUtilities().createObjectByName(processClass);
                                                            Method method = clazz.getMethod(processMethod.trim(), paramTypes);
                                                            String resultStr = (String) method.invoke(targetObj,
                                                                    functionMainObj,
                                                                    (toConnection != null ? toConnection : connection),
                                                                    dataBaseDriver,
                                                                    dbURL,
                                                                    dbUserName,
                                                                    dbPassword,
                                                                    loginUserName,
                                                                    loginOrgnId, String.valueOf(lastSeqObj.get(toColumnName)), String.valueOf(toColumnName));
                                                            if (resultStr != null
                                                                    && !"".equalsIgnoreCase(resultStr)
                                                                    && !"null".equalsIgnoreCase(resultStr)) {
                                                                lastSeqObj.put(toColumnName, resultStr);
                                                                dataObj.put(toColumnName, resultStr);
                                                            }
                                                        } catch (Exception e) {
                                                            continue;
                                                        }

                                                    } else {
                                                    }
                                                } else if (functionMainObj != null && !functionMainObj.isEmpty()
                                                        && ("QUERY".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
                                                        || "COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE"))
                                                        || "MULTI_COLUMNS".equalsIgnoreCase((String) functionMainObj.get("FUN_LVL_TYPE")))) {
                                                    dataObj.put(toColumnName, resultSet.getString((String) toColumnName));
                                                }
                                            }

                                        }
                                    }

                                }
                            }
                            totalData.add(dataObj);
                        }

                    }

                }

            }
            try {
                processETLLog(loginUserName,
                        loginOrgnId, "   ", "INFO", 1, "Y",
                        dataBaseDriver, dbURL, dbUserName, dbPassword, lastSeqObj.toJSONString(), String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }
        } catch (Exception e) {
            e.printStackTrace();
            try {
                processETLLog(loginUserName,
                        loginOrgnId, "Failed to fetch the data due to " + e.getMessage(), "ERROR", 1, "Y",
                        dataBaseDriver, dbURL, dbUserName, dbPassword, String.valueOf(selectTabObj.get("jobId")));
            } catch (Exception ex) {
            }
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

    public String GetSapListOfTable(JCO.Client connection, String filterTableVal) {
        retrieveRepository(connection);
        JCO.Table theConnection = null;
        String tableDataStr = "";
        JCO.Table tableData = null;
        JCO.Table fields = null;
        JCO.Function function = null;
        try {
            function = getFunction("RFC_READ_TABLE");
            if (filterTableVal != null && !filterTableVal.isEmpty()) {
                JCO.ParameterList listParams = function.getImportParameterList();
                listParams.setValue("DD02L", "QUERY_TABLE");//For MARA
                listParams.setValue("|", "DELIMITER");
                tableData = function.getTableParameterList().getTable("DATA");
                fields = function.getTableParameterList().getTable("FIELDS");
                JCO.Table Option = function.getTableParameterList().getTable("OPTIONS");
                fields.appendRow();
                fields.setValue("TABNAME", "FIELDNAME");
                Option.appendRow();
                Option.setValue("TABNAME LIKE '" + filterTableVal + "%' AND ( TABCLASS EQ 'TRANSP')", "TEXT");
                listParams.setValue("50", "ROWCOUNT");
                connection.execute(function);
            } else {
                JCO.ParameterList listParams = function.getImportParameterList();
                listParams.setValue("DD02L", "QUERY_TABLE");//For MARA
                listParams.setValue("|", "DELIMITER");
                listParams.setValue("20", "ROWCOUNT");
                tableData = function.getTableParameterList().getTable("DATA");
                fields = function.getTableParameterList().getTable("FIELDS");
                JCO.Table Option = function.getTableParameterList().getTable("OPTIONS");
                fields.appendRow();
                fields.setValue("TABNAME", "FIELDNAME");
                Option.appendRow();
                Option.setValue("( TABCLASS EQ 'TRANSP')", "TEXT");
                connection.execute(function);
            }
            int i = 0;
            if (tableData != null) {
                tableDataStr += "<div id='TABLE_" + i + "' class='visionTableName' title='" + (tableData.getString("WA") != null ? tableData.getString("WA").toUpperCase() : "") + "' data-table-name = '"
                        + "" + (tableData.getString("WA") != null ? tableData.getString("WA").toUpperCase() : "") + "'>" + (tableData.getString("WA") != null ? tableData.getString("WA").toUpperCase() : "") + "<img src=\"images/crossicon.png\"   title='Clear Data' onclick='moveToErpSource(TABLE_" + i + ")' class='visionCloseDestTableBtn'></div>";
                i++;
            }
            while (tableData.nextRow()) {
                tableDataStr += "<div id='TABLE_" + i + "' class='visionTableName' title='" + (tableData.getString("WA") != null ? tableData.getString("WA").toUpperCase() : "") + "' data-table-name = '"
                        + "" + (tableData.getString("WA") != null ? tableData.getString("WA").toUpperCase() : "") + "'>" + (tableData.getString("WA") != null ? tableData.getString("WA").toUpperCase() : "") + "<img src=\"images/crossicon.png\"   title='Clear Data' onclick='moveToErpSource(TABLE_" + i + ")' class='visionCloseDestTableBtn'></div>";
                i++;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return tableDataStr;
    }

    public List getErpTablecolumn(HttpServletRequest request, JCO.Client connection) {
        retrieveRepository(connection);
        JCO.Table theConnection = null;
        String tableDataStr = "";
        JCO.Table tableData = null;
        JCO.Table fields = null;
        List<Object[]> sourceColumnsList = new ArrayList<>();
        JCO.Function function = null;
        try {
            String tablesValues = request.getParameter("tablesArray");
            JSONArray tablesValuesArr = (JSONArray) JSONValue.parse(tablesValues);
            List tablesList = new ArrayList();
            tablesList.addAll(tablesValuesArr);
            String tableStr = StringUtils.collectionToDelimitedString(tablesList, "','");
            for (int i = 0; i < tablesList.size(); i++) {
                String tablename = (String) tablesList.get(i);
                function = getFunction("DDIF_FIELDINFO_GET");
                if (function != null) {
                    JCO.ParameterList listParams = function.getImportParameterList();
                    listParams.setValue(tablename, "TABNAME");//For MARA
                    fields = function.getTableParameterList().getTable("DFIES_TAB");
                    connection.execute(function);
                    while (fields.nextRow()) {
                        Object[] columnsArray = new Object[2];
                        String feildName = fields.getString("FIELDNAME");
//                        if (feildName.startsWith("/")) {
//                                feildName = feildName.substring(1);
//                            }
//                            if (feildName.contains("/")) {
//                                feildName = feildName.replaceAll("/", "_");
//                            }
                        columnsArray[0] = (listParams.getString("TABNAME") != null ? listParams.getString("TABNAME").toUpperCase() : "");
                        columnsArray[1] = feildName;
                        sourceColumnsList.add(columnsArray);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sourceColumnsList;
    }

    public List getErpSelectedColumnsData(
            //JSONObject tableColsObj,
            Map tableColsObj,// ravi etl integration
            JSONObject tablesObj, JCO.Client connection) {
        retrieveRepository(connection);
        JCO.Table theConnection = null;
        JCO.Table tableData = null;
        JCO.Table fields = null;
        List totalErpData = new ArrayList();
        List<Object[]> sourceColumnsList = new ArrayList<>();
        JCO.Function function = null;
        String whereCond = "";
        try {
            if (tableColsObj != null && !tableColsObj.isEmpty()) {
                for (Object tableName : tableColsObj.keySet()) {
                    if (tableName != null && tableColsObj.get(tableName) != null) {
                        String colsObj = (String) tableColsObj.get(tableName);
                        if (colsObj != null && !colsObj.isEmpty()) {
                            List<String> columnsList = new ArrayList(Arrays.asList(colsObj.split(",")));
                            function = getFunction("RFC_READ_TABLE");
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
                                String WhereCondition = (String) tablesObj.get(tableName);
                                if (WhereCondition != null && !WhereCondition.isEmpty()) {
                                    WhereCondition = WhereCondition.replaceAll(tableName + ".", ""); // ravi etl integration
                                    Option.appendRow();
                                    Option.setValue(WhereCondition, "TEXT");
                                }
                                connection.execute(function);

                                while (tableData.nextRow()) {
                                    Map dataObj = new HashMap();
                                    if (columnsList != null && !columnsList.isEmpty()) {
                                        for (int i = 0; i < columnsList.size(); i++) {
                                            String[] FieldValues = tableData.getString("WA").split("♥", -1);
                                            dataObj.put(tableName + ":" + columnsList.get(i), FieldValues[i]);
                                        }
                                        totalErpData.add(dataObj);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return totalErpData;
    }

    public IRepository retrieveRepository(JCO.Client connection) {
        try {
            theRepository = new JCO.Repository("saprep", connection);
        } catch (Exception ex) {
            System.out.println("failed to retrieve repository");
        }
        return theRepository;
    }

    public JCO.Function getFunction(String name) {
        try {
            return theRepository.getFunctionTemplate(name.toUpperCase()).getFunction();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public String getOracle_ERPDatabaseTables(Connection connection, String serviceName, String filterTableVal) { // ravi new mwthod
        return getOracleDatabaseTables(connection, serviceName, filterTableVal);
    }

    public List<Object[]> getOracle_ERPTableColumns(Connection connection, HttpServletRequest request, String serviceName) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        List<Object[]> sourceColumnsList = new ArrayList<>();
        try {

            String tablesValues = request.getParameter("tablesArray");
            if (tablesValues != null && !"".equalsIgnoreCase(tablesValues) && !"null".equalsIgnoreCase(tablesValues)) {
                JSONArray tablesValuesArr = (JSONArray) JSONValue.parse(tablesValues);
                List tablesList = new ArrayList();
                tablesList.addAll(tablesValuesArr);
                String tableStr = StringUtils.collectionToDelimitedString(tablesList, "','");
                System.out.println("tableStr:::" + tableStr);
                String query = "SELECT DISTINCT TABLE_NAME,COLUMN_NAME,DATA_TYPE,DATA_LENGTH FROM USER_TAB_COLUMNS WHERE "
                        + " TABLE_NAME IN ('" + tableStr + "') ORDER BY TABLE_NAME";
                //String query = "select";
                preparedStatement = connection.prepareStatement(query);
                resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    Object[] columnsArray = new Object[2];
                    columnsArray[0] = (resultSet.getString("TABLE_NAME") != null ? resultSet.getString("TABLE_NAME").toUpperCase() : "");
                    columnsArray[1] = resultSet.getString("COLUMN_NAME");
                    columnsArray[2] = resultSet.getString("DATA_TYPE");
                    columnsArray[3] = resultSet.getString("DATA_LENGTH");

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

    public List getOracle_ERPSelectedColumnsData(Connection connection,
            //JSONObject tableColsObj,
            Map tableColsObj,// ravi etl integration
            Integer start, Integer limit,
            JSONObject tablesObj, String serviceName) {
        return getOracleSelectedColumnsData(connection, tableColsObj, start, limit, tablesObj, serviceName);
    }

    public List<Object[]> getTreeOracle_ERPTableColumns(Connection connection, HttpServletRequest request, String serviceName, String tableName) {
        return getTreeOracleTableColumns(connection, request, serviceName, tableName);
    }

    // ravi data modeller datatype addition
    public List<Object[]> getTreeOracleTableColumns(Connection connection, HttpServletRequest request, String serviceName, String tableName) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        List<Object[]> sourceColumnsList = new ArrayList<>();
        try {
            if (tableName != null && !"".equalsIgnoreCase(tableName) && !"null".equalsIgnoreCase(tableName)) {
                System.out.println("tableName:::" + tableName);
                String query = "SELECT DISTINCT TABLE_NAME,COLUMN_NAME, DATA_TYPE, DATA_LENGTH FROM USER_TAB_COLUMNS WHERE "
                        + " TABLE_NAME IN ('" + tableName + "') ORDER BY TABLE_NAME,COLUMN_NAME";
                preparedStatement = connection.prepareStatement(query);
                resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    Object[] columnsArray = new Object[4];
                    columnsArray[0] = (resultSet.getString("TABLE_NAME") != null ? resultSet.getString("TABLE_NAME").toUpperCase() : "");
                    columnsArray[1] = resultSet.getString("COLUMN_NAME");
                    columnsArray[2] = resultSet.getString("DATA_TYPE");
                    columnsArray[3] = resultSet.getString("DATA_LENGTH");
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

            } catch (Exception e) {
            }
        }
        return sourceColumnsList;
    }

    public List<Object[]> getTreeMYSQLTableColumns(Connection connection, HttpServletRequest request, String serviceName, String tableName) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        List<Object[]> sourceColumnsList = new ArrayList<>();
        try {
            if (tableName != null && !"".equalsIgnoreCase(tableName) && !"null".equalsIgnoreCase(tableName)) {
                System.out.println("tableName:::" + tableName);
                String query = "SELECT DISTINCT TABLE_NAME,COLUMN_NAME, DATA_TYPE,CHARACTER_MAXIMUM_LENGTH  FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + serviceName + "' "
                        + "AND TABLE_NAME IN ('" + tableName + "') ORDER BY TABLE_NAME,COLUMN_NAME";
                preparedStatement = connection.prepareStatement(query);
                resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    Object[] columnsArray = new Object[4];
                    columnsArray[0] = (resultSet.getString("TABLE_NAME") != null ? resultSet.getString("TABLE_NAME").toUpperCase() : "");
                    columnsArray[1] = resultSet.getString("COLUMN_NAME");
                    columnsArray[2] = resultSet.getString("DATA_TYPE");
                    columnsArray[3] = resultSet.getString("CHARACTER_MAXIMUM_LENGTH");
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

            } catch (Exception e) {
            }
        }
        return sourceColumnsList;
    }

    public List<Object[]> getTreeSQLSERVERTableColumns(Connection connection, HttpServletRequest request, String serviceName, String tableName) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        List<Object[]> sourceColumnsList = new ArrayList<>();
        try {
            if (tableName != null && !"".equalsIgnoreCase(tableName) && !"null".equalsIgnoreCase(tableName)) {
                System.out.println("tableName:::" + tableName);
                String query = "SELECT DISTINCT TABLE_NAME,COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + serviceName + "' "
                        + "AND TABLE_NAME IN ('" + tableName + "') ORDER BY TABLE_NAME,COLUMN_NAME";
                preparedStatement = connection.prepareStatement(query);
                resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    Object[] columnsArray = new Object[4];
                    columnsArray[0] = (resultSet.getString("TABLE_NAME") != null ? resultSet.getString("TABLE_NAME").toUpperCase() : "");
                    columnsArray[1] = resultSet.getString("COLUMN_NAME");
                    columnsArray[2] = resultSet.getString("DATA_TYPE");
                    columnsArray[3] = resultSet.getString("CHARACTER_MAXIMUM_LENGTH");
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

            } catch (Exception e) {
            }
        }
        return sourceColumnsList;
    }

    public List getTreeOracleSelectedColumnsData(Connection connection,
            //JSONObject tableColsObj,
            Map tableColsObj, // ravi etl integration
            Integer start, Integer limit, JSONObject tablesObj,
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
                    String query = "SELECT " + colsObj;
//                    String query = "SELECT ROWNUM AS RNO," + colsObj;
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
                    query += " OFFSET " + (start - 1) + " ROWS FETCH NEXT " + limit + " ROWS ONLY ";
//                    query = "SELECT * FROM (" + query + ") WHERE RNO BETWEEN " + start + " AND " + (start + limit - 1) + "";
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
                                String query = "SELECT " + colsObj;
//                                String query = "SELECT ROWNUM AS RNO," + colsObj;
                                List<String> columnsList = new ArrayList(Arrays.asList(colsObj.split(",")));
                                if (tablesObj != null && !tablesObj.isEmpty()) {
                                    String tableInputVal = (String) tablesObj.get(tableName);
                                    query += " FROM " + tableName + " ";
                                    if (tableInputVal != null && !"".equalsIgnoreCase(tableInputVal) && !"null".equalsIgnoreCase(tableInputVal)) {
                                        query += " WHERE " + tableInputVal + "";
                                    }
                                }
                                query += " OFFSET " + (start - 1) + " ROWS FETCH NEXT " + limit + " ROWS ONLY ";
//                                query = "SELECT * FROM (" + query + ") WHERE RNO BETWEEN " + start + " AND " + (start + limit - 1) + "";
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

    public JSONArray getOracleDatabaseTables(Connection connection, String serviceName) {

        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        JSONArray tablesArray = new JSONArray();
        String tableDataStr = "";
        try {
            String query = "";

            query = "SELECT TABLE_NAME FROM USER_TABLES";
//            query = "SELECT ROWNUM AS RNO,TABLE_NAME FROM USER_TABLES";

            System.out.println("query:::" + query);
            preparedStatement = connection.prepareStatement(query);
            resultSet = preparedStatement.executeQuery();
            int i = 0;
            while (resultSet.next()) {
                tablesArray.add(resultSet.getString("TABLE_NAME"));
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
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
            }
        }
        return tablesArray;
    }

    public JSONArray getMYSQLDatabaseTables(Connection connection, String serviceName) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        JSONArray tablesArray = new JSONArray();
        String tableDataStr = "";
        try {
            //SELECT TABLE_NAME,COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='TEST_DATABASE' AND TABLE_NAME ='B_CITY' ORDER BY COLUMN_NAME
            //String query = "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + serviceName + "' LIMIT 0,20 ";
            String query = "";
            query = "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + serviceName + "' ";

            preparedStatement = connection.prepareStatement(query);
            resultSet = preparedStatement.executeQuery();
            int i = 0;
            while (resultSet.next()) {
                tablesArray.add(resultSet.getString("TABLE_NAME"));
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
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
            }
        }
        return tablesArray;
    }

    public JSONArray getMSSQLDatabaseTables(Connection connection, String serviceName) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        JSONArray tablesArray = new JSONArray();
        String tableDataStr = "";
        try {
            //SELECT TABLE_NAME,COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='TEST_DATABASE' AND TABLE_NAME ='B_CITY' ORDER BY COLUMN_NAME
            // String query = "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + serviceName + "'";
            String query = "";
            query = "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + serviceName + "' ";

            preparedStatement = connection.prepareStatement(query);
            resultSet = preparedStatement.executeQuery();
            int i = 0;
            while (resultSet.next()) {
                tablesArray.add(resultSet.getString("TABLE_NAME"));
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
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
            }
        }
        return tablesArray;
    }

    public List<Object[]> getOracle_ERPTableColumns(Connection connection, String serviceName, String tableName) {
        return getOracleTableColumns(connection, serviceName, tableName); // ravi new Method
    }

    public List<Object[]> getOracleTableColumns(Connection connection, String serviceName, String tableName) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        List<Object[]> sourceColumnsList = new ArrayList<>();
        try {
        	
            if (tableName != null && !"".equalsIgnoreCase(tableName) && !"null".equalsIgnoreCase(tableName)) {
                System.out.println("tableName:::" + tableName);
                String query = " SELECT A.COLUMN_ID,"//0
                        + " A.TABLE_NAME,"//1
                        + " A.COLUMN_NAME,"//2
                        + " DATA_TYPE,"//3
                        + " (CASE WHEN DATA_LENGTH = 0 THEN 1 ELSE DATA_LENGTH END) DATA_LENGTH,"//4
                        + " B.POSITION, "//5
                        + " B.STATUS,"//6
                        + " B.CONSTRAINT_TYPE, "//7
                        + " CASE "
                        + " WHEN (DATA_TYPE ='CLOB' OR DATA_TYPE ='BLOB' OR DATA_TYPE ='DATE'  OR DATA_TYPE ='NUMBER') THEN DATA_TYPE"
                        + " ELSE DATA_TYPE||'('||(CASE WHEN DATA_LENGTH = 0 THEN 1 WHEN DATA_LENGTH < 3900 THEN DATA_LENGTH+100 ELSE 4000 END ) ||' '|| (CASE WHEN (A.CHAR_USED ='C') THEN 'CHAR'  WHEN (A.CHAR_USED='B') THEN 'BYTE' ELSE '' END) ||')'  END AS COLUMN_TYPE "//8 // RAVI CREATE TABLE
                        + " FROM USER_TAB_COLUMNS A"
                        + " LEFT OUTER JOIN ("
                        + " SELECT COLS.TABLE_NAME, COLS.COLUMN_NAME, COLS.POSITION, CONS.STATUS, CONS.OWNER,CONS.CONSTRAINT_TYPE "
                        + " FROM USER_CONSTRAINTS CONS, USER_CONS_COLUMNS COLS"
                        + " WHERE COLS.TABLE_NAME in ( '" + tableName + "')"
                        + " AND CONS.CONSTRAINT_TYPE = 'P'"
                        + " AND CONS.CONSTRAINT_NAME = COLS.CONSTRAINT_NAME"
                        + " AND CONS.OWNER = COLS.OWNER"
                        + " ORDER BY COLS.TABLE_NAME, COLS.POSITION"
                        + " ) B ON A.TABLE_NAME = B.TABLE_NAME AND A.COLUMN_NAME = B.COLUMN_NAME"
                        + "  WHERE A.TABLE_NAME  in ( '" + tableName + "' ) ORDER BY A.TABLE_NAME,COLUMN_ID";
//                String query = " SELECT A.COLUMN_ID,"//0
//                        + " A.TABLE_NAME,"//1
//                        + " A.COLUMN_NAME,"//2
//                        + " DATA_TYPE,"//3
//                        + " DATA_LENGTH,"//4
//                        + " B.POSITION, "//5
//                        + " B.STATUS,"//6
//                        + " B.CONSTRAINT_TYPE, "//7
//                        + " CASE \n"
//                        + "WHEN (DATA_TYPE ='CLOB' OR DATA_TYPE ='BLOB' OR DATA_TYPE ='DATE') THEN DATA_TYPE\n"
//                        + " ELSE DATA_TYPE||'('||DATA_LENGTH||')'   END AS COLUMN_TYPE "//8
//                        + " FROM USER_TAB_COLUMNS A"
//                        + " LEFT OUTER JOIN ("
//                        + " SELECT COLS.TABLE_NAME, COLS.COLUMN_NAME, COLS.POSITION, CONS.STATUS, CONS.OWNER,CONS.CONSTRAINT_TYPE "
//                        + " FROM USER_CONSTRAINTS CONS, USER_CONS_COLUMNS COLS"
//                        + " WHERE COLS.TABLE_NAME in ( '" + tableName + "')"
//                        + " AND CONS.CONSTRAINT_TYPE = 'P'"
//                        + " AND CONS.CONSTRAINT_NAME = COLS.CONSTRAINT_NAME"
//                        + " AND CONS.OWNER = COLS.OWNER"
//                        + " ORDER BY COLS.TABLE_NAME, COLS.POSITION"
//                        + " ) B ON A.TABLE_NAME = B.TABLE_NAME AND A.COLUMN_NAME = B.COLUMN_NAME"
//                        + "  WHERE A.TABLE_NAME  in ( '" + tableName + "' ) ORDER BY A.TABLE_NAME,COLUMN_ID";
//                String query = "SELECT DISTINCT TABLE_NAME,COLUMN_NAME FROM USER_TAB_COLUMNS WHERE "
//                        + " TABLE_NAME IN ('" A.COLUMN_ID,+ tableName + "') ORDER BY TABLE_NAME";
//System.out.println("query::::"+query);
                preparedStatement = connection.prepareStatement(query);
                resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    Object[] columnsArray = new Object[9];
                    columnsArray[0] = (resultSet.getString("COLUMN_ID"));
                    columnsArray[1] = (resultSet.getString("TABLE_NAME") != null ? resultSet.getString("TABLE_NAME").toUpperCase() : "");
                    columnsArray[2] = resultSet.getString("COLUMN_NAME");
                    columnsArray[3] = resultSet.getString("DATA_TYPE");
                    columnsArray[4] = resultSet.getString("DATA_LENGTH");
                    columnsArray[5] = resultSet.getString("POSITION");
                    columnsArray[6] = resultSet.getString("STATUS");
                    columnsArray[7] = resultSet.getString("CONSTRAINT_TYPE");
                    columnsArray[8] = resultSet.getString("COLUMN_TYPE");
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

            } catch (Exception e) {
            }
        }
        return sourceColumnsList;
    }

    public List<Object[]> getMYSQLTableColumns(Connection connection, String serviceName, String tableName) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        List<Object[]> sourceColumnsList = new ArrayList<>();
        try {
            if (tableName != null && !"".equalsIgnoreCase(tableName) && !"null".equalsIgnoreCase(tableName)) {
                System.out.println("tableName:::" + tableName);
                String query = "SELECT ORDINAL_POSITION,"//0
                        + " TABLE_NAME,"//1
                        + " COLUMN_NAME,"//2
                        + " DATA_TYPE,"//3
                        + " (case when CHARACTER_MAXIMUM_LENGTH = 0 then 1 else CHARACTER_MAXIMUM_LENGTH end) CHARACTER_MAXIMUM_LENGTH,"//4
                        + " ORDINAL_POSITION as POSITION, "//5
                        + " 'ENABLED' AS STATUS,"//6
                        + " COLUMN_KEY,"//7
                        + " (CASE WHEN COLUMN_TYPE ='char(0)' then 'char(1)' else COLUMN_TYPE end) COLUMN_TYPE"//8
                        + " FROM INFORMATION_SCHEMA.COLUMNS"
                        + " WHERE TABLE_SCHEMA = '" + serviceName + "' AND TABLE_NAME in ( '" + tableName + "' )"
                        + " ORDER BY TABLE_NAME,ORDINAL_POSITION";
                preparedStatement = connection.prepareStatement(query);
                resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    Object[] columnsArray = new Object[9];
                    columnsArray[0] = (resultSet.getString("ORDINAL_POSITION"));
                    columnsArray[1] = (resultSet.getString("TABLE_NAME") != null ? resultSet.getString("TABLE_NAME").toUpperCase() : "");
                    columnsArray[2] = resultSet.getString("COLUMN_NAME");
                    columnsArray[3] = resultSet.getString("DATA_TYPE");
                    columnsArray[4] = resultSet.getString("CHARACTER_MAXIMUM_LENGTH");
                    columnsArray[5] = resultSet.getString("POSITION");
                    columnsArray[6] = resultSet.getString("STATUS");
                    columnsArray[7] = resultSet.getString("COLUMN_KEY");
                    columnsArray[8] = resultSet.getString("COLUMN_TYPE");
                    sourceColumnsList.add(columnsArray);
                }
            }
            System.out.println("columnsArray::::" + sourceColumnsList);
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
        return sourceColumnsList;
    }

    public List<Object[]> getSQLSERVERTableColumns(Connection connection, String serviceName, String tableName) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        List<Object[]> sourceColumnsList = new ArrayList<>();
        try {
            if (tableName != null && !"".equalsIgnoreCase(tableName) && !"null".equalsIgnoreCase(tableName)) {
                System.out.println("tableName:::" + tableName);
                String query = " SELECT A.ORDINAL_POSITION,"//0
                        + " A.TABLE_NAME,"//1
                        + " A.COLUMN_NAME,"//2
                        + " A.DATA_TYPE,"//3
                        + "( CASE WHEN A.CHARACTER_MAXIMUM_LENGTH =0 THEN 1 ELSE A.CHARACTER_MAXIMUM_LENGTH END) CHARACTER_MAXIMUM_LENGTH,"//4
                        + " ORDINAL_POSITION as POSITION, "//5
                        + " 'ENABLED' AS STATUS,"//6
                        + " B.CONSTRAINT_TYPE AS COLUMN_KEY,"//7
                        + " CASE "
                        + "    WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN CONCAT(CONCAT(DATA_TYPE, '(', CHARACTER_MAXIMUM_LENGTH),')')"
                        + "    ELSE DATA_TYPE "
                        + " END AS COLUMN_TYPE "//8
                        + " FROM INFORMATION_SCHEMA.COLUMNS A"
                        + " LEFT OUTER JOIN ("
                        + " SELECT TAB.TABLE_NAME,COL.COLUMN_NAME,TAB.CONSTRAINT_TYPE FROM "
                        + "    INFORMATION_SCHEMA.TABLE_CONSTRAINTS TAB, "
                        + "    INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE COL"
                        + " WHERE "
                        + "    COL.CONSTRAINT_NAME = TAB.CONSTRAINT_NAME"
                        + "    AND COL.TABLE_NAME = TAB.TABLE_NAME"
                        + "    AND TAB.TABLE_SCHEMA = COL.TABLE_SCHEMA"
                        + "    AND CONSTRAINT_TYPE = 'PRIMARY KEY'"
                        + "    AND COL.TABLE_NAME IN ('" + tableName + "')"
                        + "    AND TAB.TABLE_SCHEMA = '" + serviceName + "'"
                        + " ) B ON A.TABLE_NAME = B.TABLE_NAME AND A.COLUMN_NAME = B.COLUMN_NAME"
                        + " WHERE A.TABLE_SCHEMA = '" + serviceName + "'"
                        + " AND A.TABLE_NAME IN ('" + tableName + "') ORDER BY A.TABLE_NAME,A.ORDINAL_POSITION";
//                String query = "SELECT ORDINAL_POSITION,"//0
//                        + " TABLE_NAME,"//1
//                        + " COLUMN_NAME,"//2
//                        + " DATA_TYPE,"//3
//                        + " CHARACTER_MAXIMUM_LENGTH,"//4
//                        + " ORDINAL_POSITION as POSITION "//5
//                        + " 'ENABLED' AS STATUS,"//6
//                        + " COLUMN_KEY,"//7
//                        + " COLUMN_TYPE"//8
//                        + " FROM INFORMATION_SCHEMA.COLUMNS"
//                        + " WHERE TABLE_SCHEMA = '"+serviceName+"' AND TABLE_NAME in ( '" + tableName + "' )"
//                        + " ORDER BY TABLE_NAME,ORDINAL_POSITION";
                preparedStatement = connection.prepareStatement(query);
                resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    Object[] columnsArray = new Object[9];
                    columnsArray[0] = (resultSet.getString("ORDINAL_POSITION"));
                    columnsArray[1] = (resultSet.getString("TABLE_NAME") != null ? resultSet.getString("TABLE_NAME").toUpperCase() : "");
                    columnsArray[2] = resultSet.getString("COLUMN_NAME");
                    columnsArray[3] = resultSet.getString("DATA_TYPE");
                    columnsArray[4] = resultSet.getString("CHARACTER_MAXIMUM_LENGTH");
                    columnsArray[5] = resultSet.getString("POSITION");
                    columnsArray[6] = resultSet.getString("STATUS");
                    columnsArray[7] = resultSet.getString("COLUMN_KEY");
                    columnsArray[8] = resultSet.getString("COLUMN_TYPE");
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

            } catch (Exception e) {
            }
        }
        return sourceColumnsList;
    }

    private void processETLLog(String sessionUserName, String orgnId, String logTxt, String logType,
            int sequenceNo, String processFlag, String dataBaseDriver, String dbURL, String dbUserName, String dbPassword, String jobId) {
        Connection logConnection = null;
        PreparedStatement logStmt = null;
        try {
            if (logTxt != null
                    && !"".equalsIgnoreCase(logTxt)
                    && !"null".equalsIgnoreCase(logTxt)
                    && logTxt.length() > 3000) {
                logTxt = logTxt.substring(0, 3000);
            }
            int lastSequenceNo = getLastLogSequenceNo(sessionUserName, orgnId, dataBaseDriver, dbURL, dbUserName, dbPassword);
            lastSequenceNo++;
            String insertQuery = "INSERT INTO RECORD_DM_PROCESS_LOG (SEQUENCE_NO, USER_NAME, ORGN_ID, LOG_TXT,LOG_TYPE,PROCESS_FLAG,JOB_ID)"
                    + " VALUES (?,?,?,?,?,?,?)";
            Class.forName(dataBaseDriver);
            logConnection = DriverManager.getConnection(dbURL, dbUserName, dbPassword);
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

    private void processETLLog(String sessionUserName, String orgnId, String logTxt, String logType,
            int sequenceNo, String processFlag, String dataBaseDriver, String dbURL, String dbUserName,
            String dbPassword, String lastAutoGenSeq, String jobId) {
        Connection logConnection = null;
        PreparedStatement logStmt = null;
        try {
            if (logTxt != null
                    && !"".equalsIgnoreCase(logTxt)
                    && !"null".equalsIgnoreCase(logTxt)
                    && logTxt.length() > 3000) {
                logTxt = logTxt.substring(0, 3000);
            }
            int lastSequenceNo = getLastLogSequenceNo(sessionUserName, orgnId, dataBaseDriver, dbURL, dbUserName, dbPassword);
            lastSequenceNo++;
            String insertQuery = "INSERT INTO RECORD_DM_PROCESS_LOG (SEQUENCE_NO, USER_NAME, ORGN_ID, LOG_TXT,LOG_TYPE,PROCESS_FLAG,DM_LOG_CUST_COL1,JOB_ID)"
                    + " VALUES (?,?,?,?,?,?,?,?)";
            Class.forName(dataBaseDriver);
            logConnection = DriverManager.getConnection(dbURL, dbUserName, dbPassword);
            logStmt = logConnection.prepareStatement(insertQuery);
            logStmt.setObject(1, lastSequenceNo);//SEQUENCE_NO
            logStmt.setObject(2, sessionUserName);//USER_NAME
            logStmt.setObject(3, orgnId);//ORGN_ID
            logStmt.setObject(4, logTxt);//LOG_TXT
            logStmt.setObject(5, logType);//LOG_TYPE
            logStmt.setObject(6, processFlag);//PROCESS_FLAG
            logStmt.setObject(7, lastAutoGenSeq);//DM_LOG_CUST_COL1
            logStmt.setObject(8, jobId);//JOB_ID
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

    private int getLastLogSequenceNo(String sessionUserName, String orgnId,
            String dataBaseDriver, String dbURL, String dbUserName, String dbPassword) {
        int lastSequenceNo = 0;
        Connection logConnection = null;
        PreparedStatement logStmt = null;
        ResultSet logSet = null;
        try {
            Map<String, Object> selectMap = new HashMap<>();
            String selectQuery = "SELECT MAX(SEQUENCE_NO) AS SEQ_NO FROM RECORD_DM_PROCESS_LOG"
                    + " WHERE USER_NAME =? AND ORGN_ID =? ";
            Class.forName(dataBaseDriver);
            logConnection = DriverManager.getConnection(dbURL, dbUserName, dbPassword);
            logStmt = logConnection.prepareStatement(selectQuery);
            logStmt.setObject(1, sessionUserName);//USER_NAME
            logStmt.setObject(2, orgnId);//ORGN_ID

            logSet = logStmt.executeQuery();
            if (logSet.next()) {
                lastSequenceNo = new PilogUtilities().convertIntoInteger(logSet.getObject("SEQ_NO"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (logSet != null) {
                    logSet.close();
                }
                if (logStmt != null) {
                    logStmt.close();
                }
                if (logConnection != null) {
                    logConnection.close();
                }
            } catch (Exception e) {

            }
        }
        return lastSequenceNo;
    }

    public List getSAPTableColumns(HttpServletRequest request, JCO.Client connection, String tableName) {
        retrieveRepository(connection);
        JCO.Table theConnection = null;
        String tableDataStr = "";
        JCO.Table tableData = null;
        JCO.Table fields = null;
        List<Object[]> sourceColumnsList = new ArrayList<>();
        JCO.Function function = null;
        try {
            List tablesList = new ArrayList();
            tablesList = Arrays.asList(tableName.split(","));
            for (int i = 0; i < tablesList.size(); i++) {
                String tablename = (String) tablesList.get(i);
                function = getFunction("DDIF_FIELDINFO_GET");
                if (function != null) {
                    JCO.ParameterList listParams = function.getImportParameterList();
                    listParams.setValue(tablename, "TABNAME");//For MARA
                    fields = function.getTableParameterList().getTable("DFIES_TAB");
                    connection.execute(function);
                    int fetchCount = fields.getNumRows();
                    if (fetchCount != 0) {
                        String feildName = fields.getString("FIELDNAME");
                        if (feildName != null
                                && !"".equalsIgnoreCase(feildName)
                                && !"null".equalsIgnoreCase(feildName)
//                                && !feildName.contains("/")
                                ) {
                            
//                        if (feildName.startsWith("/")) {
//                                feildName = feildName.substring(1);
//                            }
//                            if (feildName.contains("/")) {
//                                feildName = feildName.replaceAll("/", "_");
//                            }
                            Object[] columnsArray = new Object[2];
                            columnsArray[0] = (listParams.getString("TABNAME") != null ? listParams.getString("TABNAME").toUpperCase() : "");
                            columnsArray[1] = feildName;

                            sourceColumnsList.add(columnsArray);
                        }
                    }
                    while (fields.nextRow()) {
                        String feildName = fields.getString("FIELDNAME");
                        if (feildName != null
                                && !"".equalsIgnoreCase(feildName)
                                && !"null".equalsIgnoreCase(feildName)
                                && !feildName.contains("/")) {
                            Object[] columnsArray = new Object[2];
                            columnsArray[0] = (listParams.getString("TABNAME") != null ? listParams.getString("TABNAME").toUpperCase() : "");
                            columnsArray[1] = fields.getString("FIELDNAME");

                            sourceColumnsList.add(columnsArray);
                        }

                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sourceColumnsList;
    }

    public List getSAPTableColumns(JCO.Client connection, String tableName) {
        retrieveRepository(connection);
        JCO.Table theConnection = null;
        String tableDataStr = "";
        JCO.Table tableData = null;
        JCO.Table fields = null;
        List<Object[]> sourceColumnsList = new ArrayList<>();
        JCO.Function function = null;
        try {
            List tablesList = new ArrayList();
            tablesList = Arrays.asList(tableName.split(","));
            for (int i = 0; i < tablesList.size(); i++) {
                String tablename = (String) tablesList.get(i);
                function = getFunction("DDIF_FIELDINFO_GET");
                if (function != null) {
                    JCO.ParameterList listParams = function.getImportParameterList();
                    listParams.setValue(tablename, "TABNAME");//For MARA
                    fields = function.getTableParameterList().getTable("DFIES_TAB");
                    connection.execute(function);
                    int fetchCount = fields.getNumRows();
                    if (fetchCount != 0) {
                        String feildName = fields.getString("FIELDNAME");
                        if (feildName != null
                                && !"".equalsIgnoreCase(feildName)
                                && !"null".equalsIgnoreCase(feildName)
//                                && !feildName.contains("/")
                                ) {
//                            if (feildName.startsWith("/")) {
//                                feildName = feildName.substring(1);
//                            }
//                            if (feildName.contains("/")) {
//                                feildName = feildName.replaceAll("/", "_");
//                            }
                            Object[] columnsArray = new Object[2];
                            columnsArray[0] = (listParams.getString("TABNAME") != null ? listParams.getString("TABNAME").toUpperCase() : "");
                            columnsArray[1] = feildName;

                            sourceColumnsList.add(columnsArray);
                        }
                    }
                    while (fields.nextRow()) {
                        String feildName = fields.getString("FIELDNAME");
                        if (feildName != null
                                && !"".equalsIgnoreCase(feildName)
                                && !"null".equalsIgnoreCase(feildName)
                                && !feildName.contains("/")) {
                            Object[] columnsArray = new Object[2];
                            columnsArray[0] = (listParams.getString("TABNAME") != null ? listParams.getString("TABNAME").toUpperCase() : "");
                            columnsArray[1] = fields.getString("FIELDNAME");

                            sourceColumnsList.add(columnsArray);
                        }

                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sourceColumnsList;
    }

    public List getSAPTableColumnsWithType(HttpServletRequest request, JCO.Client connection, String tableName) {
        retrieveRepository(connection);
        JCO.Table theConnection = null;
        String tableDataStr = "";
        JCO.Table tableData = null;
        JCO.Table fields = null;
        List<Object[]> sourceColumnsList = new ArrayList<>();
        JCO.Function function = null;
        try {
            List tablesList = new ArrayList();
            tablesList = Arrays.asList(tableName.split(","));
            for (int i = 0; i < tablesList.size(); i++) {
                String tablename = (String) tablesList.get(i);
                function = getFunction("DDIF_FIELDINFO_GET");
                if (function != null) {
                    JCO.ParameterList listParams = function.getImportParameterList();
                    listParams.setValue(tablename, "TABNAME");//For MARA
                    fields = function.getTableParameterList().getTable("DFIES_TAB");
                    connection.execute(function);
                    int fetchCount = fields.getNumRows();
                    if (fetchCount != 0) {
                        String feildName = fields.getString("FIELDNAME");
                        if (feildName != null
                                && !"".equalsIgnoreCase(feildName)
                                && !"null".equalsIgnoreCase(feildName)
                                //                                && !feildName.contains("/")
                                ) {
//                            if (feildName.startsWith("/")) {
//                                feildName = feildName.substring(1);
//                            }
//                            if (feildName.contains("/")) {
//                                feildName = feildName.replaceAll("/", "_");
//                            }
                            Object[] columnsArray = new Object[10];
                            columnsArray[0] = (fields.getString("POSITION"));
                            columnsArray[1] = (listParams.getString("TABNAME") != null ? listParams.getString("TABNAME").toUpperCase() : "");
//                            columnsArray[2] = fields.getString("FIELDNAME");
                            columnsArray[2] = feildName;
                            columnsArray[3] = fields.getString("DATATYPE");
                            columnsArray[4] = fields.getString("LENG");
                            columnsArray[5] = fields.getString("POSITION");
                            columnsArray[6] = "";
                            columnsArray[7] = "";
                            columnsArray[8] = fields.getString("DATATYPE") + "(" + fields.getString("LENG") + ")";
                            columnsArray[9] = fields.getString("KEYFLAG");
                            sourceColumnsList.add(columnsArray);
                        }

                    }
                    while (fields.nextRow()) {
                        String feildName = fields.getString("FIELDNAME");
                        if (feildName != null
                                && !"".equalsIgnoreCase(feildName)
                                && !"null".equalsIgnoreCase(feildName)
//                                && !feildName.contains("/")
                                ) {
//                            if (feildName.startsWith("/")) {
//                                feildName = feildName.substring(1);
//                            }
//                            if (feildName.contains("/")) {
//                                feildName = feildName.replaceAll("/", "_");
//                            }
                            Object[] columnsArray = new Object[10];
                            columnsArray[0] = (fields.getString("POSITION"));
                            columnsArray[1] = (listParams.getString("TABNAME") != null ? listParams.getString("TABNAME").toUpperCase() : "");
                            columnsArray[2] = feildName;
                            columnsArray[3] = fields.getString("DATATYPE");
                            columnsArray[4] = fields.getString("LENG");
                            columnsArray[5] = fields.getString("POSITION");
                            columnsArray[6] = "";
                            columnsArray[7] = "";
                            columnsArray[8] = fields.getString("DATATYPE") + "(" + fields.getString("LENG") + ")";
                            columnsArray[9] = fields.getString("KEYFLAG");
                            sourceColumnsList.add(columnsArray);
                        }
                    }
//                    while (fields.nextRow()) {
//                        Object[] columnsArray = new Object[9];
//                        columnsArray[0] = (fields.getString("POSITION"));
//                        columnsArray[1] = (listParams.getString("TABNAME") != null ? listParams.getString("TABNAME").toUpperCase() : "");
//                        columnsArray[2] = fields.getString("FIELDNAME");
//                        columnsArray[3] = fields.getString("DATATYPE");
//                        columnsArray[4] = fields.getString("LENG");
//                        columnsArray[5] = fields.getString("POSITION");
//                        columnsArray[6] = "";
//                        columnsArray[7] = "";
//                        columnsArray[8] = fields.getString("DATATYPE") + "(" + fields.getString("LENG") + ")";
//                        sourceColumnsList.add(columnsArray);
//                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sourceColumnsList;
    }

    public List getSAPTableColumnsWithType(JCO.Client connection, String tableName) {
        retrieveRepository(connection);
        JCO.Table theConnection = null;
        String tableDataStr = "";
        JCO.Table tableData = null;
        JCO.Table fields = null;
        List<Object[]> sourceColumnsList = new ArrayList<>();
        JCO.Function function = null;
        try {
            List tablesList = new ArrayList();
            tablesList = Arrays.asList(tableName.split(","));
            for (int i = 0; i < tablesList.size(); i++) {
                String tablename = (String) tablesList.get(i);
                function = getFunction("DDIF_FIELDINFO_GET");
                if (function != null) {
                    JCO.ParameterList listParams = function.getImportParameterList();
                    listParams.setValue(tablename, "TABNAME");//For MARA
                    fields = function.getTableParameterList().getTable("DFIES_TAB");
                    connection.execute(function);
                    int fetchCount = fields.getNumRows();
                    if (fetchCount != 0) {
                        String feildName = fields.getString("FIELDNAME");
                        if (feildName != null
                                && !"".equalsIgnoreCase(feildName)
                                && !"null".equalsIgnoreCase(feildName)
                                //                                && !feildName.contains("/")
                                ) {
//                            if (feildName.startsWith("/")) {
//                                feildName = feildName.substring(1);
//                            }
//                            if (feildName.contains("/")) {
//                                feildName = feildName.replaceAll("/", "_");
//                            }
                            Object[] columnsArray = new Object[10];
                            columnsArray[0] = (fields.getString("POSITION"));
                            columnsArray[1] = (listParams.getString("TABNAME") != null ? listParams.getString("TABNAME").toUpperCase() : "");
//                            columnsArray[2] = fields.getString("FIELDNAME");
                            columnsArray[2] = feildName;
                            columnsArray[3] = fields.getString("DATATYPE");
                            columnsArray[4] = fields.getString("LENG");
                            columnsArray[5] = fields.getString("POSITION");
                            columnsArray[6] = "";
                            columnsArray[7] = "";
                            columnsArray[8] = fields.getString("DATATYPE") + "(" + fields.getString("LENG") + ")";
                            columnsArray[9] = fields.getString("KEYFLAG");
                            sourceColumnsList.add(columnsArray);
                        }

                    }
                    while (fields.nextRow()) {
                        String feildName = fields.getString("FIELDNAME");
                        if (feildName != null
                                && !"".equalsIgnoreCase(feildName)
                                && !"null".equalsIgnoreCase(feildName)
//                                && !feildName.contains("/")
                                ) {
//                             if (feildName.startsWith("/")) {
//                                feildName = feildName.substring(1);
//                            }
//                            if (feildName.contains("/")) {
//                                feildName = feildName.replaceAll("/", "_");
//                            }
                            Object[] columnsArray = new Object[10];
                            columnsArray[0] = (fields.getString("POSITION"));
                            columnsArray[1] = (listParams.getString("TABNAME") != null ? listParams.getString("TABNAME").toUpperCase() : "");
                            columnsArray[2] = feildName;
                            columnsArray[3] = fields.getString("DATATYPE");
                            columnsArray[4] = fields.getString("LENG");
                            columnsArray[5] = fields.getString("POSITION");
                            columnsArray[6] = "";
                            columnsArray[7] = "";
                            columnsArray[8] = fields.getString("DATATYPE") + "(" + fields.getString("LENG") + ")";
                            columnsArray[9] = fields.getString("KEYFLAG");
                            sourceColumnsList.add(columnsArray);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sourceColumnsList;
    }

    public List getErpSelectedColumnsData(
            //JSONObject tableColsObj,
            Map tableColsObj,// ravi etl integration
            JSONObject tablesObj,
            JCO.Client connection,
            int start,
            int limit) {
        retrieveRepository(connection);
        JCO.Table theConnection = null;
        JCO.Table tableData = null;
        JCO.Table fields = null;
        List totalErpData = new ArrayList();
        List<Object[]> sourceColumnsList = new ArrayList<>();
        JCO.Function function = null;
        String whereCond = "";
        try {
            if (tableColsObj != null && !tableColsObj.isEmpty()) {
                for (Object tableName : tableColsObj.keySet()) {
                    if (tableName != null && tableColsObj.get(tableName) != null) {
                        String colsObj = (String) tableColsObj.get(tableName);
                        if (colsObj != null && !colsObj.isEmpty()) {
                            List<String> columnsList = new ArrayList(Arrays.asList(colsObj.split(",")));

                            if (columnsList.size() > 5) { // ravi etl integration 50 -> 25
                                List totalDataList = new ArrayList();
                                for (int k = 0; k < columnsList.size(); k += 5) {

                                    function = getFunction("RFC_READ_TABLE"); // ravi updated code changes
                                    int endIndex = k + 5 < columnsList.size() ? k + 5 : columnsList.size();

                                    List colsSubList = columnsList.subList(k, endIndex);
                                    if (colsSubList != null && !colsSubList.isEmpty()) {
                                        if (function != null) {
                                            JCO.ParameterList listParams = function.getImportParameterList();
                                            fields = function.getTableParameterList().getTable("FIELDS");
                                            tableData = function.getTableParameterList().getTable("DATA");
                                            JCO.Table Option = function.getTableParameterList().getTable("OPTIONS");
                                            for (int i = 0; i < colsSubList.size(); i++) {
                                                //String columnName = (String) columnsList.get(i);
                                                String columnName = (String) colsSubList.get(i); // ravi etl integration
                                                fields.appendRow();
                                                fields.setValue(columnName, "FIELDNAME");
                                            }
                                            listParams.setValue(tableName, "QUERY_TABLE");//For MARA
                                            listParams.setValue("♥", "DELIMITER");
//                                            listParams.setValue((start == 0 ? (start) : (start - 1)), "ROWSKIPS");
//                                            listParams.setValue((limit + 1), "ROWCOUNT");
                                            listParams.setValue((start == 0 ? (start) : (start)), "ROWSKIPS"); // ravi etl integration
                                            listParams.setValue((limit), "ROWCOUNT"); // ravi etl integration
                                            String WhereCondition = (String) tablesObj.get(tableName);
                                            if (WhereCondition != null && !WhereCondition.isEmpty()) {
                                                WhereCondition = WhereCondition.replaceAll(tableName + ".", ""); // ravi etl integration
                                                Option.appendRow();
                                                Option.setValue(WhereCondition, "TEXT");
                                            }
                                            connection.execute(function);
                                            int fetchCount = tableData.getNumRows();
                                            if (totalDataList != null && !totalDataList.isEmpty()) {
                                                int index = 0;
                                                if (fetchCount != 0) {
                                                    Map dataObj = (Map) totalDataList.get(index);
                                                    if (colsSubList != null && !colsSubList.isEmpty()) {
                                                        for (int i = 0; i < colsSubList.size(); i++) {
                                                            String[] FieldValues = tableData.getString("WA").split("♥", -1);
                                                            String value = FieldValues[i];
                                                            if (value != null
                                                                    && !"".equalsIgnoreCase(value)
                                                                    && !"null".equalsIgnoreCase(value)) {
                                                                value = value.trim();
                                                            }
                                                            //dataObj.put(tableName + ":" + columnsList.get(i), FieldValues[i]);
//                                                            //dataObj.put(tableName + ":" + colsSubList.get(i), FieldValues[i]);
                                                            dataObj.put(tableName + ":" + colsSubList.get(i), value); // ravi etl integration
                                                        }
                                                        totalDataList.remove(index);
                                                        totalDataList.add(index, dataObj);
                                                    }
                                                    index++;
                                                }
                                                while (tableData.nextRow()) {

                                                    Map dataObj = (Map) totalDataList.get(index);
                                                    if (colsSubList != null && !colsSubList.isEmpty()) {
                                                        for (int i = 0; i < colsSubList.size(); i++) {
                                                            String[] FieldValues = tableData.getString("WA").split("♥", -1);
                                                            String value = FieldValues[i];
                                                            if (value != null
                                                                    && !"".equalsIgnoreCase(value)
                                                                    && !"null".equalsIgnoreCase(value)) {
                                                                value = value.trim();
                                                            }
                                                            dataObj.put(tableName + ":" + colsSubList.get(i), value); // ravi etl integration
                                                        }
                                                        totalDataList.remove(index);
                                                        totalDataList.add(index, dataObj);
                                                    }
                                                    index++;
                                                }
                                            } else {
                                                if (fetchCount != 0) {
                                                    Map dataObj = new HashMap();
                                                    if (colsSubList != null && !colsSubList.isEmpty()) {
                                                        for (int i = 0; i < colsSubList.size(); i++) {
                                                            String[] FieldValues = tableData.getString("WA").split("♥", -1);
                                                            //dataObj.put(tableName + ":" + colsSubList.get(i), FieldValues[i]);
                                                            String value = FieldValues[i];
                                                            if (value != null
                                                                    && !"".equalsIgnoreCase(value)
                                                                    && !"null".equalsIgnoreCase(value)) {
                                                                value = value.trim();
                                                            }
                                                            dataObj.put(tableName + ":" + colsSubList.get(i), value); // ravi etl integration
                                                        }
                                                        totalDataList.add(dataObj);
                                                    }
                                                }
                                                while (tableData.nextRow()) {
                                                    Map dataObj = new HashMap();
                                                    if (colsSubList != null && !colsSubList.isEmpty()) {
                                                        for (int i = 0; i < colsSubList.size(); i++) {
                                                            String[] FieldValues = tableData.getString("WA").split("♥", -1);
                                                            //dataObj.put(tableName + ":" + colsSubList.get(i), FieldValues[i]);
                                                            String value = FieldValues[i];
                                                            if (value != null
                                                                    && !"".equalsIgnoreCase(value)
                                                                    && !"null".equalsIgnoreCase(value)) {
                                                                value = value.trim();
                                                            }
                                                            dataObj.put(tableName + ":" + colsSubList.get(i), value); // ravi etl integration
                                                        }
                                                        totalDataList.add(dataObj);
                                                    }
                                                }
                                            }

                                        }
                                    }

                                }
                                if (totalDataList != null && !totalDataList.isEmpty()) {
                                    totalErpData.addAll(totalDataList);
                                }
                            } else {
                                function = getFunction("RFC_READ_TABLE");
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
//                                    listParams.setValue((start == 0 ? (start) : (start - 1)), "ROWSKIPS");
//                                    listParams.setValue((limit + 1), "ROWCOUNT");
                                    listParams.setValue((start == 0 ? (start) : (start)), "ROWSKIPS"); // ravi etl integration
                                    listParams.setValue((limit), "ROWCOUNT"); // ravi etl integration

                                    String WhereCondition = (String) tablesObj.get(tableName);
                                    if (WhereCondition != null && !WhereCondition.isEmpty()) {
                                        WhereCondition = WhereCondition.replaceAll(tableName + ".", ""); // ravi etl integration
                                        Option.appendRow();
                                        Option.setValue(WhereCondition, "TEXT");
                                    }
                                    connection.execute(function);
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
                                                dataObj.put(tableName + ":" + columnsList.get(i), value);
                                            }
                                            totalErpData.add(dataObj);
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
                                                dataObj.put(tableName + ":" + columnsList.get(i), value);
                                            }
                                            totalErpData.add(dataObj);
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
        }
        return totalErpData;
    }

    public List getErpSelectedColumnsData(List<String> columnsList,
            JSONObject tablesObj,
            JCO.Client connection,
            int start,
            int limit, String tableName) {
        retrieveRepository(connection);
        JCO.Table theConnection = null;
        JCO.Table tableData = null;
        JCO.Table fields = null;
        List totalErpData = new ArrayList();
        List<Object[]> sourceColumnsList = new ArrayList<>();
        JCO.Function function = null;
        String whereCond = "";
        try {

            if (columnsList != null && !columnsList.isEmpty()) {

                if (function != null) {
                    if (columnsList.size() > 5) {
                        List totalDataList = new ArrayList();
                        for (int k = 0; k < sourceColumnsList.size(); k++) {
                            function = getFunction("RFC_READ_TABLE");
                            int endIndex = k + 5 < columnsList.size() ? k + 5 : columnsList.size();
                            List colsSubList = sourceColumnsList.subList(k, endIndex);
                            if (colsSubList != null && !colsSubList.isEmpty()) {
                                JCO.ParameterList listParams = function.getImportParameterList();
                                fields = function.getTableParameterList().getTable("FIELDS");
                                tableData = function.getTableParameterList().getTable("DATA");
                                JCO.Table Option = function.getTableParameterList().getTable("OPTIONS");
                                for (int i = 0; i < colsSubList.size(); i++) {
                                    String columnName = (String) colsSubList.get(i);
                                    fields.appendRow();
                                    fields.setValue(columnName.split(":")[1], "FIELDNAME");
                                }
                                listParams.setValue(tableName, "QUERY_TABLE");//For MARA
                                listParams.setValue("♥", "DELIMITER");
//                                listParams.setValue((start == 0 ? (start) : (start - 1)), "ROWSKIPS");
//                                listParams.setValue((limit + 1), "ROWCOUNT");
                                listParams.setValue((start == 0 ? (start) : (start)), "ROWSKIPS"); // ravi etl integration
                                listParams.setValue((limit), "ROWCOUNT"); // ravi etl integration
                                String WhereCondition = (String) tablesObj.get(tableName);
                                if (WhereCondition != null && !WhereCondition.isEmpty()) {
                                    WhereCondition = WhereCondition.replaceAll(tableName + ".", ""); // ravi etl integration
                                    Option.appendRow();
                                    Option.setValue(WhereCondition, "TEXT");
                                }
                                connection.execute(function);
                                int fetchCount = tableData.getNumRows();
                                if (totalDataList != null && !totalDataList.isEmpty()) {
                                    int index = 0;
                                    if (fetchCount != 0) {
                                        Map dataObj = (Map) totalDataList.get(index);
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
                                            totalDataList.remove(index);
                                            totalDataList.add(index, dataObj);
                                        }
                                        index++;
                                    }
                                    while (tableData.nextRow()) {
                                        Map dataObj = (Map) totalDataList.get(index);
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
                                            totalDataList.remove(index);
                                            totalDataList.add(index, dataObj);
                                        }
                                        index++;
                                    }
                                } else {
                                    if (fetchCount != 0) {
                                        Map dataObj = new HashMap();
                                        if (colsSubList != null && !colsSubList.isEmpty()) {
                                            for (int i = 0; i < colsSubList.size(); i++) {
                                                String[] FieldValues = tableData.getString("WA").split("♥", -1);
                                                String value = FieldValues[i];
                                                if (value != null
                                                        && !"".equalsIgnoreCase(value)
                                                        && !"null".equalsIgnoreCase(value)) {
                                                    value = value.trim();
                                                }
                                                dataObj.put(colsSubList.get(i), value);
                                            }
                                            totalDataList.add(dataObj);
                                        }
                                    }
                                    while (tableData.nextRow()) {
                                        Map dataObj = new HashMap();
                                        if (colsSubList != null && !colsSubList.isEmpty()) {
                                            for (int i = 0; i < colsSubList.size(); i++) {
                                                String[] FieldValues = tableData.getString("WA").split("♥", -1);
                                                String value = FieldValues[i];
                                                if (value != null
                                                        && !"".equalsIgnoreCase(value)
                                                        && !"null".equalsIgnoreCase(value)) {
                                                    value = value.trim();
                                                }
                                                dataObj.put(colsSubList.get(i), value);
                                            }
                                            totalDataList.add(dataObj);
                                        }
                                    }
                                }
                            }

                        }
                        //totalDataList
                        if (totalDataList != null && !totalDataList.isEmpty()) {
                            totalErpData.addAll(totalDataList);
                        }
                    } else {
                        function = getFunction("RFC_READ_TABLE");
                        JCO.ParameterList listParams = function.getImportParameterList();
                        fields = function.getTableParameterList().getTable("FIELDS");
                        tableData = function.getTableParameterList().getTable("DATA");
                        JCO.Table Option = function.getTableParameterList().getTable("OPTIONS");
                        for (int i = 0; i < columnsList.size(); i++) {
                            String columnName = (String) columnsList.get(i);
                            fields.appendRow();
                            fields.setValue(columnName.split(":")[1], "FIELDNAME");
                        }
                        listParams.setValue(tableName, "QUERY_TABLE");//For MARA
                        listParams.setValue("♥", "DELIMITER");
//                        listParams.setValue((start == 0 ? (start) : (start - 1)), "ROWSKIPS");
//                        listParams.setValue((limit + 1), "ROWCOUNT");
                        listParams.setValue((start == 0 ? (start) : (start)), "ROWSKIPS"); // ravi etl integration
                        listParams.setValue((limit), "ROWCOUNT"); // ravi etl integration
                        String WhereCondition = (String) tablesObj.get(tableName);
                        if (WhereCondition != null && !WhereCondition.isEmpty()) {
                            WhereCondition = WhereCondition.replaceAll(tableName + ".", ""); //  ravi etl integration
                            Option.appendRow();
                            Option.setValue(WhereCondition, "TEXT");
                        }
                        connection.execute(function);
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
                                totalErpData.add(dataObj);
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
                                    dataObj.put(columnsList.get(i), value);
                                }
                                totalErpData.add(dataObj);
                            }
                        }
                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return totalErpData;
    }

    public String getDatabaseTables(HttpServletRequest request, JSONObject columnsObj,
            String parentKeyData, String levelStr, String startIndex, String endIndex, String serviceName, String selectedDbLabel, String filterVal, String filterCond) {
        String query = "";
        String processClass = "com.pilog.mdm.DAO.V10DataMigrationAccessDAO";
        String processMethod = "get" + selectedDbLabel + "DatabaseTypes";
        try {

            Class clazz = Class.forName(processClass);
            Class<?>[] paramTypes = {HttpServletRequest.class, JSONObject.class,
                String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class};
            Object targetObj = new PilogUtilities().createObjectByName(processClass);
            Method method = clazz.getMethod(processMethod.trim(), paramTypes);
            query = (String) method.invoke(targetObj, request, columnsObj, parentKeyData,
                    levelStr, startIndex, endIndex, serviceName, selectedDbLabel, filterVal, filterCond);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return query;
    }

    public String getOracle_ERPDatabaseTypes(HttpServletRequest request, JSONObject columnsObj,
            String parentKeyData, String levelStr, String startIndex, String endIndex, String serviceName, String selectedDbLabel, String filterVal, String filterCond) {
        String query = "";
        if (parentKeyData != null && !"".equalsIgnoreCase(parentKeyData) && "VIEWS".equalsIgnoreCase(parentKeyData)) {

            query = "SELECT DISTINCT VIEW_NAME AS FLD_NAME, VIEW_NAME AS DISP_FLD_NAME FROM USER_VIEWS";
            if (filterVal != null && !"".equalsIgnoreCase(filterVal) && !"null".equalsIgnoreCase(filterVal)) {
                query = query + " WHERE VIEW_NAME " + filterCond + "  '" + filterVal + "'";
            }

        } else if (parentKeyData != null && !"".equalsIgnoreCase(parentKeyData) && "TABLES".equalsIgnoreCase(parentKeyData)) {

            query = "SELECT DISTINCT TABLE_NAME AS FLD_NAME, TABLE_NAME AS DISP_FLD_NAME FROM USER_TABLES";
            if (filterVal != null && !"".equalsIgnoreCase(filterVal) && !"null".equalsIgnoreCase(filterVal)) {
                query = query + " WHERE TABLE_NAME " + filterCond + "  '" + filterVal + "'";
            }
        } else if (parentKeyData != null && !"".equalsIgnoreCase(parentKeyData) && "SYNONYMS".equalsIgnoreCase(parentKeyData)) {

            query = "SELECT DISTINCT SYNONYM_NAME AS FLD_NAME, SYNONYM_NAME AS DISP_FLD_NAME FROM USER_SYNONYMS";
            if (filterVal != null && !"".equalsIgnoreCase(filterVal) && !"null".equalsIgnoreCase(filterVal)) {
                query = query + " WHERE SYNONYM_NAME " + filterCond + "  '" + filterVal + "'";
            }
        }

        return query;
    }

    public String getMYSQLDatabaseTypes(HttpServletRequest request, JSONObject columnsObj,
            String parentKeyData, String levelStr, String startIndex, String endIndex, String serviceName, String selectedDbLabel, String filterVal, String filterCond) {
        String query = "";
        try {
            if (parentKeyData != null && !"".equalsIgnoreCase(parentKeyData) && "VIEWS".equalsIgnoreCase(parentKeyData)) {
                query = "SELECT DISTINCT TABLE_NAME AS FLD_NAME,TABLE_NAME AS DISP_FLD_NAME FROM INFORMATION_SCHEMA.VIEWS  WHERE TABLE_SCHEMA = '" + serviceName + "'";
            } else if (parentKeyData != null && !"".equalsIgnoreCase(parentKeyData) && "TABLES".equalsIgnoreCase(parentKeyData)) {
                query = "SELECT DISTINCT TABLE_NAME AS FLD_NAME,TABLE_NAME AS DISP_FLD_NAME FROM INFORMATION_SCHEMA.TABLES  WHERE TABLE_SCHEMA = '" + serviceName + "' AND TABLE_TYPE != 'VIEW'";
            }
//            else if (parentKeyData != null && !"".equalsIgnoreCase(parentKeyData) && "SYNONYMS".equalsIgnoreCase(parentKeyData)) {
//                query = "SELECT DISTINCT TABLE_NAME AS FLD_NAME,TABLE_NAME AS DISP_FLD_NAME FROM INFORMATION_SCHEMA.COLUMNS  WHERE TABLE_SCHEMA = 'MYSQL_" + serviceName + "'";
//            }
            if (filterVal != null && !"".equalsIgnoreCase(filterVal) && !"null".equalsIgnoreCase(filterVal)) {
                query = query + " AND TABLE_NAME " + filterCond + "  '" + filterVal + "'";
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return query;
    }

    public String getMSSQLDatabaseTypes(HttpServletRequest request, JSONObject columnsObj,
            String parentKeyData, String levelStr, String startIndex, String endIndex, String serviceName, String selectedDbLabel, String filterVal, String filterCond) {
        String query = "";
        try {
            if (parentKeyData != null && !"".equalsIgnoreCase(parentKeyData) && "VIEWS".equalsIgnoreCase(parentKeyData)) {
                query = "SELECT DISTINCT TABLE_NAME AS FLD_NAME,TABLE_NAME AS DISP_FLD_NAME FROM INFORMATION_SCHEMA.VIEWS  WHERE TABLE_SCHEMA = '" + serviceName + "'";
            } else if (parentKeyData != null && !"".equalsIgnoreCase(parentKeyData) && "TABLES".equalsIgnoreCase(parentKeyData)) {
                query = "SELECT DISTINCT TABLE_NAME AS FLD_NAME,TABLE_NAME AS DISP_FLD_NAME FROM INFORMATION_SCHEMA.TABLES  WHERE TABLE_SCHEMA = '" + serviceName + "' AND TABLE_TYPE != 'VIEW'";
            } else if (parentKeyData != null && !"".equalsIgnoreCase(parentKeyData) && "SYNONYMS".equalsIgnoreCase(parentKeyData)) {
                query = "SELECT DISTINCT name AS FLD_NAME,name AS DISP_FLD_NAME FROM sys.synonyms where COALESCE (PARSENAME (base_object_name, 2), SCHEMA_NAME (SCHEMA_ID ())) ='" + serviceName + "'";
            }
            if (filterVal != null && !"".equalsIgnoreCase(filterVal) && !"null".equalsIgnoreCase(filterVal)) {
                query = query + " AND TABLE_NAME " + filterCond + "  '" + filterVal + "'";
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return query;
    }

    public String getDatabaseColumns(HttpServletRequest request, JSONObject columnsObj,
            String parentKeyData, String levelStr, String startIndex, String endIndex, String serviceName, String selectedDbLabel, String filterVal, String filterCond) {
        String query = "";
        try {
            if (parentKeyData != null && !"".equalsIgnoreCase(parentKeyData) && !"null".equalsIgnoreCase(parentKeyData)) {
                query = "SELECT DISTINCT COLUMN_NAME AS FLD_NAME,COLUMN_NAME AS DISP_FLD_NAME FROM INFORMATION_SCHEMA.COLUMNS  WHERE TABLE_SCHEMA = '" + serviceName + "' AND TABLE_NAME='" + parentKeyData + "'";
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return query;
    }

}
