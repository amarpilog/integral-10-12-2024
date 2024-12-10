/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.service;


import com.mongodb.client.MongoClient;
import com.pilog.mdm.DAO.iVisionTransformTableOperationsDAO;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 *
 * @author Ravindar.P
 */
@Service
public class iVisionTransformTableOperationsService {

    @Autowired
    private iVisionTransformTableOperationsDAO tableOperationsDAO;

    @Autowired
    private V10GenericDataPipingService dataPipingService;
    
    public JSONObject insertRecordsToTable(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        Connection connection = null;
        ResultSet resultSet = null;
        DatabaseMetaData dmd = null;
        JSONArray tablePKColumns = new JSONArray();
        String insertMessage = "";
        String updateMessage = "";
        MongoClient client = null;
        try {
            String tableName = request.getParameter("tableName");

            String updateRowsDataStr = request.getParameter("updateRowsData");
            String insertRowsDataStr = request.getParameter("insertRowsData");
            JSONArray updateRowsData = (JSONArray) JSONValue.parse(updateRowsDataStr);
            JSONArray insertRowsData = (JSONArray) JSONValue.parse(insertRowsDataStr);
            if (insertRowsData != null && !insertRowsData.isEmpty()) {
                JSONArray columnsList = new JSONArray();
                columnsList.addAll(((JSONObject) insertRowsData.get(0)).keySet());
                columnsList.remove("uid");
                columnsList.remove("boundindex");
                columnsList.remove("visibleindex");
                columnsList.remove("uniqueid");
                columnsList.remove(tableName + "_HIDDEN");
                columnsList.remove("ROWNUM");

                String connObjStr = request.getParameter("connObj");
                JSONObject connObject = (JSONObject) JSONValue.parse(connObjStr);
                if("MongoDb".equalsIgnoreCase(String.valueOf(connObject.get("CONN_CUST_COL1")))) {
                	client = (MongoClient) dataPipingService.getGlobalConnection(connObject);
                	String databaseName = String.valueOf(connObject.get("CONN_DB_NAME"));
                	insertMessage = tableOperationsDAO.insertDocumentsToMongoCollection(client, databaseName, tableName, insertRowsData);
                } else {
                	connection = (Connection) dataPipingService.getConnection(connObject);
                    insertMessage = tableOperationsDAO.insertRecordsToTable(request, connection, tableName, columnsList, insertRowsData);
                }
            }
            if (updateRowsData != null && !updateRowsData.isEmpty()) {
                String updateDataInfoStr = request.getParameter("updateDataInfo");
                JSONObject updateDataInfo = (JSONObject) JSONValue.parse(updateDataInfoStr);

                JSONArray columnsList = new JSONArray();
                columnsList.addAll(((JSONObject) updateRowsData.get(0)).keySet());
                columnsList.remove("uid");
                columnsList.remove("boundindex");
                columnsList.remove("visibleindex");
                columnsList.remove("uniqueid");
                columnsList.remove(tableName + "_HIDDEN");

                String connObjStr = request.getParameter("connObj");
                JSONObject connObject = (JSONObject) JSONValue.parse(connObjStr);
                connection = (Connection) dataPipingService.getConnection(connObject);
                dmd = connection.getMetaData();
                resultSet = dmd.getPrimaryKeys(null, null, tableName);
//                while (resultSet.next()) {
//                    String name = resultSet.getString("COLUMN_NAME");
//                    tablePKColumns.add(name);
//                }
//                if (tablePKColumns != null && !tablePKColumns.isEmpty()) {
//                    updateMessage = tableOperationsDAO.updateRecordsInTable(request, connection, tableName, tablePKColumns, columnsList, updateRowsData, updateDataInfo);
//                } else {
                updateMessage = tableOperationsDAO.updateRecordsInTable(request, connection, tableName, columnsList, columnsList, updateRowsData, updateDataInfo);
//                }

            }

        } catch (Exception e) {
        } finally {

        }
        resultObj.put("updateMessage", updateMessage);
        resultObj.put("insertMessage", insertMessage);
        return resultObj;
    }

    public JSONObject updateRecordsInTable(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {

        } catch (Exception e) {
        } finally {

        }
        return resultObj;
    }

    public JSONObject deleteRecordsInTable(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        Connection connection = null;
        ResultSet resultSet = null;
        DatabaseMetaData dmd = null;
        JSONArray tablePKColumns = new JSONArray();
        String message = "";
        try {
            String tableName = request.getParameter("tableName");

            String totalDataStr = request.getParameter("selectedRowsData");
            JSONArray totalDataList = (JSONArray) JSONValue.parse(totalDataStr);

            JSONArray columnsList = new JSONArray();
            columnsList.addAll(((JSONObject) totalDataList.get(0)).keySet());
            columnsList.remove("uid");
            columnsList.remove("boundindex");
            columnsList.remove("visibleindex");
            columnsList.remove("uniqueid");
            columnsList.remove(tableName + "_HIDDEN");

            String connObjStr = request.getParameter("connObj");
            JSONObject connObject = (JSONObject) JSONValue.parse(connObjStr);
            connection = (Connection) dataPipingService.getConnection(connObject);
            dmd = connection.getMetaData();
            resultSet = dmd.getPrimaryKeys(null, null, tableName);
            while (resultSet.next()) {
                String name = resultSet.getString("COLUMN_NAME");
                if (!tablePKColumns.contains(name)) {
					tablePKColumns.add(name);
				}
            }
            if (tablePKColumns != null && !tablePKColumns.isEmpty()) {//040123
				message = tableOperationsDAO.deleteRecordsInTableWithPK(request, connection, tableName, tablePKColumns,
						columnsList, totalDataList);
			} else {
				message = tableOperationsDAO.deleteRecordsInTableWithoutPK(request, connection, tableName,
						columnsList, totalDataList);
			}
        } catch (Exception e) {
        } finally {

        }
        resultObj.put("message", message);
        return resultObj;
    }

    public JSONObject truncateTableData(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        Connection connection = null;

        String message = "";

        try {
            String tableName = request.getParameter("tableName");
            String connObjStr = request.getParameter("connObj");
            JSONObject connObject = (JSONObject) JSONValue.parse(connObjStr);
            connection = (Connection) dataPipingService.getConnection(connObject);
            message = tableOperationsDAO.truncateTableData(request, connection, tableName);

        } catch (Exception e) {

            e.printStackTrace();
        } finally {
            try {
                System.out.println("connection closed :: " + connection.isClosed());
            } catch (Exception e) {

            }
        }
        resultObj.put("message", message);
        return resultObj;
    }

  public JSONObject alterTable(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        Connection connection = null;

        String message = "";
        ResultSet columns = null;
        ResultSet pkColumns = null;
        List pkColumnsList = new ArrayList();
        List dataTypesList = new ArrayList();
        DatabaseMetaData metaData = null;
        JSONArray columnsArray = new JSONArray();
        JSONArray dataFieldsArray = new JSONArray();
        JSONArray data = new JSONArray();

        try {

            String tableName = request.getParameter("tableName");
            String connObjStr = request.getParameter("connObj");
            JSONObject connObject = (JSONObject) JSONValue.parse(connObjStr);
            String sysType = (String) connObject.get("CONN_CUST_COL1");
            String conType = (String) connObject.get("CONNECTION_NAME");
            dataTypesList = tableOperationsDAO.getListOfDataTypes(request, sysType.toUpperCase());
            resultObj.put("dataTypesList", dataTypesList);

            connection = (Connection) dataPipingService.getConnection(connObject);
            metaData = connection.getMetaData();
            pkColumns = metaData.getPrimaryKeys(null, null, tableName);
            while (pkColumns.next()) {
                String columnName = pkColumns.getString("COLUMN_NAME");
                pkColumnsList.add(columnName);
            }
            
            if (sysType.equalsIgnoreCase("ORACLE")) {
                String sql = "SELECT column_name, data_type, data_length, data_precision, data_scale, nullable "
                        + "FROM user_tab_columns "
                        + "WHERE table_name = ?";
                PreparedStatement statement = connection.prepareStatement(sql);
                statement.setString(1, tableName);
                columns = statement.executeQuery();

                while (columns.next()) {
                    JSONObject row = new JSONObject();
                    String columnName = columns.getString("COLUMN_NAME");
                    row.put("columnName", columnName);
                    if (pkColumnsList.contains(columnName)) {
                        row.put("primaryKey", "Y");
                    } else {
                        row.put("primaryKey", "N");
                    }
                    row.put("datatypeName", columns.getString("DATA_TYPE"));
                    row.put("columnsize", columns.getString("DATA_LENGTH"));
                    row.put("precision", columns.getString("DATA_PRECISION"));
                    row.put("scale", columns.getString("DATA_SCALE"));
                    row.put("notNull", ("Y".equals(columns.getString("NULLABLE"))) ? "NOT NULL" : "NULL");

                    // Add any other information you need
                    data.add(row);
                }
            } else {
                columns = metaData.getColumns(null, null, tableName, null);

                while (columns.next()) {

                    JSONObject row = new JSONObject();
                    String defaultVal = columns.getString("COLUMN_DEF");
                    row.put("defaultValue", defaultVal);
                    String columnName = columns.getString("COLUMN_NAME");

//                columnsList.add(columnName);
                    row.put("columnName", columnName);
                    if (pkColumnsList.contains(columnName)) {
                        row.put("primaryKey", "Y");
                    } else {
                        row.put("primaryKey", "N");
                    }
                    String datatype = columns.getString("DATA_TYPE");
                    String typeName = columns.getString("TYPE_NAME");
                    row.put("datatypeName", typeName);
                    String columnsize = columns.getString("COLUMN_SIZE");
                    String decimaldigits = columns.getString("DECIMAL_DIGITS");

                    if (typeName != null && ("NUMBER".equalsIgnoreCase(typeName)
                            || "NUMERIC".equalsIgnoreCase(typeName)
                            || "DECIMAL".equalsIgnoreCase(typeName)
                            || "FLOAT".equalsIgnoreCase(typeName))) {
                        row.put("columnsize", "");
                        row.put("precision", columnsize);
                        row.put("scale", decimaldigits != null ? decimaldigits : "");

                    } else {
                        row.put("columnsize", columnsize);
                        row.put("precision", "");
                        row.put("scale", "");
                    }

                    String isNullable = columns.getString("IS_NULLABLE");
                    row.put("notNull", ("YES".equalsIgnoreCase(isNullable)) ? "NULL" : "NOT NULL");
                    String is_autoIncrment = columns.getString("IS_AUTOINCREMENT");
                    String RADIX = columns.getString("NUM_PREC_RADIX");
                    row.put("alterTableHidden", "UPDATE");
                    data.add(row);
                }
                //Printing results
                // System.out.println(RADIX+"--- "+columnName + "---" + datatype + "---" + columnsize + "---" + decimaldigits + "---" + isNullable + "---" + is_autoIncrment+"--- "+defaultVal);
                //System.out.println(columnName + "---" + datatype + "---" + columnsize + "---" + decimaldigits + "---" + isNullable + "---" + is_autoIncrment);

            }
            String pkColsListStr = (String) pkColumnsList.stream().map(e -> e).collect(Collectors.joining(","));

            resultObj.put("pkColsList", pkColsListStr);
            resultObj.put("data", data);

        } catch (Exception e) {

            e.printStackTrace();
        } finally {
            try {
                System.out.println("connection closed :: " + connection.isClosed());
            } catch (Exception e) {

            }
        }

        return resultObj;
    }

  
    public JSONObject executeAlterTable(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        Connection connection = null;
        String message = "";
        DatabaseMetaData dmd = null;
        ResultSet resultSet = null;
        try {

            String tableName = request.getParameter("tableName");

            String connObjStr = request.getParameter("connObj");
            JSONObject connObject = (JSONObject) JSONValue.parse(connObjStr);
            connection = (Connection) dataPipingService.getConnection(connObject);

            String alterTableDataStr = request.getParameter("alterTableData");
            JSONObject alterTableData = (JSONObject) JSONValue.parse(alterTableDataStr);

//            List renameColumnsList = (List) alterTableData.keySet().stream().filter(col
//                    -> ((JSONObject) alterTableData.get(col)).keySet().contains("columnName")
//            ).map(e -> e).collect(Collectors.toList());
            for (Object col : alterTableData.keySet()) {
                JSONObject alterColsObj = (JSONObject) alterTableData.get(col);
//                String columnName = (alterColsObj.get("columnName") != null) ? ((String) alterColsObj.get("columnName")) : "";
//                String primaryKey = (alterColsObj.get("primaryKey") != null) ? ((String) alterColsObj.get("primaryKey")) : "";
//                String datatypeName = (alterColsObj.get("datatypeName") != null) ? ((String) alterColsObj.get("datatypeName")) : "";
//                String columnsize = (alterColsObj.get("columnsize") != null) ? ((String) alterColsObj.get("columnsize")) : "";
//                String precision = (alterColsObj.get("precision") != null) ? ((String) alterColsObj.get("precision")) : "";
//                String scale = (alterColsObj.get("scale") != null) ? ((String) alterColsObj.get("scale")) : "";
//                String notNull = (alterColsObj.get("notNull") != null) ? ((String) alterColsObj.get("notNull")) : "";
//                String defaultValue = (alterColsObj.get("defaultValue") != null) ? ((String) alterColsObj.get("defaultValue")) : "";

                String alterTableQuery = "";

                String dataTypeWithSize = "";
                if (alterColsObj.keySet().contains("datatypeName")) {

                    dataTypeWithSize += alterColsObj.get("datatypeName");
                    if (alterColsObj.keySet().contains("columnsize") && alterColsObj.get("columnsize") != null && !"".equalsIgnoreCase((String) alterColsObj.get("columnsize"))) {
                        dataTypeWithSize += "(" + alterColsObj.get("columnsize") + ")";
                    } else if (alterColsObj.keySet().contains("precision") && alterColsObj.get("precision") != null && !"".equalsIgnoreCase((String) alterColsObj.get("precision"))) {
                        dataTypeWithSize += "(" + alterColsObj.get("precision");
                        if (alterColsObj.keySet().contains("scale") && alterColsObj.get("scale") != null && !"".equalsIgnoreCase((String) alterColsObj.get("scale"))) {
                            dataTypeWithSize += "," + alterColsObj.get("scale");
                        }
                        dataTypeWithSize += ")";
                    }
                }

                String defaultValStr = "";
                if (alterColsObj.keySet().contains("defaultValue")) {
                    String defaultValue = (alterColsObj.get("defaultValue") != null && !"".equalsIgnoreCase((String) alterColsObj.get("defaultValue"))) ? ((String) alterColsObj.get("defaultValue")) : "NULL";
                    defaultValStr = " DEFAULT " + defaultValue;
                }
                String notNull = "";
                if (alterColsObj.keySet().contains("notNull") 
                        && alterColsObj.get("notNull") != null
                        && !"".equalsIgnoreCase((String) alterColsObj.get("notNull"))) {
                    notNull = " " + alterColsObj.get("notNull");
                }

                if (alterColsObj.keySet().contains("columnName")
                        && alterColsObj.get("columnName") != null
                        && !"".equalsIgnoreCase((String) alterColsObj.get("columnName"))
                        && ((String) col).startsWith("newRow_")) {

                    String addAltTblQuery = "ALTER TABLE " + tableName + " ADD (" + alterColsObj.get("columnName") + " " + dataTypeWithSize + " " + defaultValStr + " " + notNull + " )";
                    try {
                        Boolean tableAltered = tableOperationsDAO.executeAlterSQLQuery(request, connection, addAltTblQuery);
                        message += "Column Added SuceesfUlly <br>";
                    } catch (Exception e) {
                        message += "Error  Adding column " + e.getMessage() + "<br>";
                        e.printStackTrace();
                    }
                }  else if (!alterColsObj.keySet().contains("columnName") &&
                          ( dataTypeWithSize!=null && !"".equalsIgnoreCase(dataTypeWithSize) 
                            ||  defaultValStr!=null && !"".equalsIgnoreCase(defaultValStr) 
                            ||  notNull!=null && !"".equalsIgnoreCase(notNull)  ) 
                        ) {

                    String modifyAltTblQuery = "ALTER TABLE " + tableName + " MODIFY (" + col + " " + dataTypeWithSize + " " + defaultValStr + " " + notNull + " )";
                    try {
                        Boolean tableAltered = tableOperationsDAO.executeAlterSQLQuery(request, connection, modifyAltTblQuery);
                        message += "Table Alterd SuceesfUlly <br>";
                    } catch (Exception e) {
                        message += "Error  Alter table " + e.getMessage() + "<br>";
                        e.printStackTrace();
                    }

                }

            }

            List finalPKColsList = new ArrayList();
            List existingPKcolsList = new ArrayList();
            String primaryKeysExist = "N";
            String existingPKcols = "";
            
           
                 dmd = connection.getMetaData();
                resultSet = dmd.getPrimaryKeys(null, null, tableName);
                while (resultSet.next()) {
                    String name = resultSet.getString("COLUMN_NAME");
                    existingPKcolsList.add(name);
                    primaryKeysExist = "Y";
                }
                existingPKcols = (String)existingPKcolsList.stream().map(e -> e).collect(Collectors.joining(","));
                

//            if (existingPKcols != null && !"".equalsIgnoreCase(existingPKcols)) {
//                existingPKcolsList = Arrays.asList(existingPKcols.split(","));
//                finalPKColsList.addAll(existingPKcolsList);
//            }
            List addPrimaryKeyColumnsList = (List) alterTableData.keySet().stream().filter(col
                    -> {
                Boolean addPkFlag = false;
                JSONObject dataFieldsObj = (JSONObject) alterTableData.get(col);
                if (dataFieldsObj.keySet().contains("primaryKey")) {
                    String currentVal = (String) dataFieldsObj.get("primaryKey");

                    if ("Y".equalsIgnoreCase(currentVal)) {
                        addPkFlag = true;
                    } else {
                        addPkFlag = false;
                    }
                }
                return addPkFlag;

            }
            ).map(col -> {
                if (((String) col).startsWith("newRow_")) {
                    JSONObject dataFieldsObj = (JSONObject) alterTableData.get(col);
                    if (dataFieldsObj.keySet().contains("columnName")) {
                        col = (String) dataFieldsObj.get("columnName");
                    }
                }
                finalPKColsList.add(col);
                return col;
            }).collect(Collectors.toList());

//            List dropPrimaryKeyColumnsList = (List) alterTableData.keySet().stream().filter(col
//                    -> {
//                Boolean dropPkFlag = false;
//                JSONObject dataFieldsObj = (JSONObject) alterTableData.get(col);
//                if (!((String) col).startsWith("newRow_") && dataFieldsObj.keySet().contains("primaryKey")) {
//                    String currentVal = (String) dataFieldsObj.get("primaryKey");
//                    if ("N".equalsIgnoreCase(currentVal)) {
//                        dropPkFlag = true;
//                    } else {
//                        dropPkFlag = false;
//                    }
//                }
//
//                return dropPkFlag;
//
//            }
//            ).map(e -> {
//                finalPKColsList.remove(e);
//                return e;
//            }).collect(Collectors.toList());

            try {
                if (addPrimaryKeyColumnsList != null && !addPrimaryKeyColumnsList.isEmpty() 
                        || (addPrimaryKeyColumnsList.isEmpty() && "Y".equalsIgnoreCase(primaryKeysExist) ) ) {

                    
                    if ( "Y".equalsIgnoreCase(primaryKeysExist) ) {
                        try {
                        String alterTblDropPKQuery = "ALTER TABLE " + tableName + " DROP  PRIMARY KEY";
                        Boolean tableAltered = tableOperationsDAO.executeAlterSQLQuery(request, connection, alterTblDropPKQuery);

                        message += "Table Alterd SuceesfUlly <br>";
                        } catch (Exception e) {
                            message += "Error  Alter table " + e.getMessage() + "<br>";
                            e.printStackTrace();
                        }
                    }
                    
                    if (addPrimaryKeyColumnsList != null && !addPrimaryKeyColumnsList.isEmpty() ) {
                        String addPrimaryKeyColsStr = (String) finalPKColsList.stream().map(e -> e).collect(Collectors.joining(","));

                        String alterTblAddPKQuery = "ALTER TABLE " + tableName + " ADD  PRIMARY KEY ( " + addPrimaryKeyColsStr + " )";
                        Boolean tableAltered = tableOperationsDAO.executeAlterSQLQuery(request, connection, alterTblAddPKQuery);
                        message += "Table Alterd SuceesfUlly <br>";
                     }
                }
            } catch (Exception e) {
                message += "Error  Alter table " + e.getMessage() + "<br>";
                String alterTblAddExistingPKQuery = "ALTER TABLE " + tableName + " ADD  PRIMARY KEY ( " + existingPKcols + " )";
                Boolean tableAltered = tableOperationsDAO.executeAlterSQLQuery(request, connection, alterTblAddExistingPKQuery);
                e.printStackTrace();
            }

            for (Object col : alterTableData.keySet()) {
                if (!((String) col).startsWith("newRow_")) {
                    JSONObject alterColsObj = (JSONObject) alterTableData.get(col);

                    if (alterColsObj.keySet().contains("columnName")) {
                        String renameColVal = (String) alterColsObj.get("columnName");
                        if (!renameColVal.equalsIgnoreCase(String.valueOf(col))) {
                        String alterTableRenameQuery = "ALTER TABLE " + tableName + " RENAME COLUMN " + col + " TO " + renameColVal;
                        try {
                            Boolean tableAltered = tableOperationsDAO.executeAlterSQLQuery(request, connection, alterTableRenameQuery);
                            message += "Table Alterd SuceesfUlly <br>";
                        } catch (Exception e) {
                            message += "Error  Alter table " + e.getMessage() + "<br>";
                            e.printStackTrace();
                        }
                        }
                        
                    }
                }

            }

        } catch (Exception e) {
            message += "Error  Alter table " + e.getMessage() + "<br>";
            e.printStackTrace();
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
                System.out.println("connection closed :: " + connection.isClosed());
            } catch (Exception e) {

            }
        }
        resultObj.put("message", message);
        return resultObj;
    }

    
     public JSONObject generateAlterTableQuery(HttpServletRequest request) {
       
        
        JSONObject resultObj = new JSONObject();
        List alterTableQueryList = new ArrayList();
        Connection connection = null;
        String message = "";
        DatabaseMetaData dmd = null;
        ResultSet resultSet = null;
        try {

            String tableName = request.getParameter("tableName");

            String connObjStr = request.getParameter("connObj");
            JSONObject connObject = (JSONObject) JSONValue.parse(connObjStr);
            connection = (Connection) dataPipingService.getConnection(connObject);

            String alterTableDataStr = request.getParameter("alterTableData");
            JSONObject alterTableData = (JSONObject) JSONValue.parse(alterTableDataStr);
            if (alterTableData!=null && !alterTableData.isEmpty()){
            for (Object col : alterTableData.keySet()) {
                JSONObject alterColsObj = (JSONObject) alterTableData.get(col);

                String alterTableQuery = "";

                String dataTypeWithSize = "";
                if (alterColsObj.keySet().contains("datatypeName")) {

                    dataTypeWithSize += alterColsObj.get("datatypeName");
                    if (alterColsObj.keySet().contains("columnsize") && alterColsObj.get("columnsize") != null && !"".equalsIgnoreCase((String) alterColsObj.get("columnsize"))) {
                        dataTypeWithSize += "(" + alterColsObj.get("columnsize") + ")";
                    } else if (alterColsObj.keySet().contains("precision") && alterColsObj.get("precision") != null && !"".equalsIgnoreCase((String) alterColsObj.get("precision"))) {
                        dataTypeWithSize += "(" + alterColsObj.get("precision");
                        if (alterColsObj.keySet().contains("scale") && alterColsObj.get("scale") != null && !"".equalsIgnoreCase((String) alterColsObj.get("scale"))) {
                            dataTypeWithSize += "," + alterColsObj.get("scale");
                        }
                        dataTypeWithSize += ")";
                    }
                }

                String defaultValStr = "";
                if (alterColsObj.keySet().contains("defaultValue")) {
                    String defaultValue = (alterColsObj.get("defaultValue") != null && !"".equalsIgnoreCase((String) alterColsObj.get("defaultValue"))) ? ((String) alterColsObj.get("defaultValue")) : "NULL";
                    defaultValStr = " DEFAULT " + defaultValue;
                }
                String notNull = "";
                if (alterColsObj.keySet().contains("notNull") 
                        && alterColsObj.get("notNull") != null
                        && !"".equalsIgnoreCase((String) alterColsObj.get("notNull"))) {
                    notNull = " " + alterColsObj.get("notNull");
                }

                if (alterColsObj.keySet().contains("columnName")
                        && alterColsObj.get("columnName") != null
                        && !"".equalsIgnoreCase((String) alterColsObj.get("columnName"))
                        && ((String) col).startsWith("newRow_")) {

                    String addAltTblQuery = "ALTER TABLE " + tableName + " ADD (" + alterColsObj.get("columnName") + " " + dataTypeWithSize + " " + defaultValStr + " " + notNull + " )";
                    alterTableQueryList.add(addAltTblQuery);
                    
                }  else if (!alterColsObj.keySet().contains("columnName") &&
                          ( dataTypeWithSize!=null && !"".equalsIgnoreCase(dataTypeWithSize) 
                            ||  defaultValStr!=null && !"".equalsIgnoreCase(defaultValStr) 
                            ||  notNull!=null && !"".equalsIgnoreCase(notNull)  ) 
                        ) {

                    String modifyAltTblQuery = "ALTER TABLE " + tableName + " MODIFY (" + col + " " + dataTypeWithSize + " " + defaultValStr + " " + notNull + " )";
                    alterTableQueryList.add(modifyAltTblQuery);
                    

                }

            }

            List finalPKColsList = new ArrayList();
            List existingPKcolsList = new ArrayList();
            String primaryKeysExist = "N";
            String existingPKcols = "";
            
           
                 dmd = connection.getMetaData();
                resultSet = dmd.getPrimaryKeys(null, null, tableName);
                while (resultSet.next()) {
                    String name = resultSet.getString("COLUMN_NAME");
                    existingPKcolsList.add(name);
                    primaryKeysExist = "Y";
                }
                existingPKcols = (String)existingPKcolsList.stream().map(e -> e).collect(Collectors.joining(","));
                

            List addPrimaryKeyColumnsList = (List) alterTableData.keySet().stream().filter(col
                    -> {
                Boolean addPkFlag = false;
                JSONObject dataFieldsObj = (JSONObject) alterTableData.get(col);
                if (dataFieldsObj.keySet().contains("primaryKey")) {
                    String currentVal = (String) dataFieldsObj.get("primaryKey");

                    if ("Y".equalsIgnoreCase(currentVal)) {
                        addPkFlag = true;
                    } else {
                        addPkFlag = false;
                    }
                }
                return addPkFlag;

            }
            ).map(col -> {
                if (((String) col).startsWith("newRow_")) {
                    JSONObject dataFieldsObj = (JSONObject) alterTableData.get(col);
                    if (dataFieldsObj.keySet().contains("columnName")) {
                        col = (String) dataFieldsObj.get("columnName");
                    }
                }
                finalPKColsList.add(col);
                return col;
            }).collect(Collectors.toList());


            try {
                if (addPrimaryKeyColumnsList != null && !addPrimaryKeyColumnsList.isEmpty()
                        || (addPrimaryKeyColumnsList.isEmpty() && "Y".equalsIgnoreCase(primaryKeysExist) ) ) {

                    if ( "Y".equalsIgnoreCase(primaryKeysExist) ) {
                      
                        String alterTblDropPKQuery = "ALTER TABLE " + tableName + " DROP  PRIMARY KEY";
                        
                        alterTableQueryList.add(alterTblDropPKQuery);
                        
                    }
                    
                    if (addPrimaryKeyColumnsList != null && !addPrimaryKeyColumnsList.isEmpty()) { 
                        String addPrimaryKeyColsStr = (String) finalPKColsList.stream().map(e -> e).collect(Collectors.joining(","));

                        String alterTblAddPKQuery = "ALTER TABLE " + tableName + " ADD  PRIMARY KEY ( " + addPrimaryKeyColsStr + " )";
                        alterTableQueryList.add(alterTblAddPKQuery);
                     }
                }
                
          
            } catch (Exception e) {
                e.printStackTrace();
            }

            for (Object col : alterTableData.keySet()) {
                if (!((String) col).startsWith("newRow_")) {
                    JSONObject alterColsObj = (JSONObject) alterTableData.get(col);

                    if (alterColsObj.keySet().contains("columnName")) {
                        String renameColVal = (String) alterColsObj.get("columnName");
                        if (!renameColVal.equalsIgnoreCase(String.valueOf(col))) {
                              String alterTableRenameQuery = "ALTER TABLE " + tableName + " RENAME COLUMN " + col + " TO " + renameColVal;
                               alterTableQueryList.add(alterTableRenameQuery);
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
                if (connection != null) {
                    resultSet.close();
                }
                System.out.println("connection closed :: " + connection.isClosed());
            } catch (Exception e) {

            }
        }
    
        resultObj.put("alterTableQueryList", alterTableQueryList);
        return resultObj;
    }

   public JSONObject dropColumnAlterTable(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        Connection connection = null;
        String message = "";
        try {

            String tableName = request.getParameter("tableName");
            String connObjStr = request.getParameter("connObj");
            JSONObject connObject = (JSONObject) JSONValue.parse(connObjStr);
            connection = (Connection) dataPipingService.getConnection(connObject);

            String selectedColumnsListStr = request.getParameter("selectedColumnsList");
            JSONArray selectedColumnsList = (JSONArray) JSONValue.parse(selectedColumnsListStr);

//            String selectedColumns = (String)selectedColumnsList.stream().map(col->col).collect(Collectors.joining(", "));
//            
            for (int i = 0; i < selectedColumnsList.size(); i++) {
                String addAltTblQuery = "ALTER TABLE " + tableName + " DROP COLUMN " + selectedColumnsList.get(i);
                try {
                    Boolean tableAltered = tableOperationsDAO.executeAlterSQLQuery(request, connection, addAltTblQuery);
                    message += "Column "+ selectedColumnsList.get(i)+" Dropped Successfully <br>";
                } catch (Exception e) {
                    message += "Error  Dropped column " + e.getMessage() + "<br>";
                    e.printStackTrace();
                }
            }

        } catch (Exception e) {
            message += "Error  Alter table " + e.getMessage() + "<br>";
            e.printStackTrace();
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
                System.out.println("connection closed :: " + connection.isClosed());
            } catch (Exception e) {

            }
        }
        resultObj.put("message", message);
        return resultObj;
    }


    
    
    
    public JSONObject updateColumnFuntion(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        String message = "";
        try {

            String function = request.getParameter("functionOperation");
            String columnName = request.getParameter("columnName");
            String dataStr = request.getParameter("data");
            String dataString = (String) JSONValue.parse(dataStr);
            //JSONArray dataArray = (JSONArray) JSONValue.parse(dataString);
            JSONObject data = (JSONObject) JSONValue.parse(dataString);
            String connObjectStr = (String)data.get("connectionObj");
            JSONObject connObject = (JSONObject)JSONValue.parse(connObjectStr);
            String tableName = (String)data.get("tableName");
            connection = (Connection) dataPipingService.getConnection(connObject);

            
            String query = "";
            if ("UPPERCASE".equalsIgnoreCase(function)) {
            	query = "UPDATE "+tableName+" SET "+columnName+ " =  UPPER("+columnName+")";
            } else if ("LOWERCASE".equalsIgnoreCase(function)) {
            	query = "UPDATE "+tableName+" SET "+columnName+ " =  LOWER("+columnName+")";
            }
            
            System.out.println(" query ::: " + query);
            preparedStatement = connection.prepareStatement(query);

            int updateCount = preparedStatement.executeUpdate();

            resultObj.put("result", updateCount+" record(s) updated");
        } catch (Exception e) {
            resultObj.put("result", "Updated failed : "+e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                
                if (preparedStatement != null) {
                	preparedStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }
                System.out.println("connection closed :: " + connection.isClosed());
            } catch (Exception e) {

            }
        }
        
        return resultObj;
    }
    
    
    public JSONObject updateColumnReplace(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        String message = "";
        try {

            String function = request.getParameter("functionOperation");
            String columnName = request.getParameter("columnName");
            String dataStr = request.getParameter("data");
            String dataString = (String) JSONValue.parse(dataStr);
            //JSONArray dataArray = (JSONArray) JSONValue.parse(dataString);
            JSONObject data = (JSONObject) JSONValue.parse(dataString);
            String connObjectStr = (String)data.get("connectionObj");
            JSONObject connObject = (JSONObject)JSONValue.parse(connObjectStr);
            String tableName = (String)data.get("tableName");
            connection = (Connection) dataPipingService.getConnection(connObject);
            
            String find = request.getParameter("find");
            String replace = request.getParameter("replace");
            
            connection = (Connection) dataPipingService.getConnection(connObject);

            String query = "UPDATE  "+tableName+" SET "+columnName+ " = REPLACE("+columnName+",'"+find+"','"+replace+"') WHERE "+columnName+" LIKE '%"+find+"%'";
            
            System.out.println(" query ::: " + query);
            preparedStatement = connection.prepareStatement(query);

            int updateCount = preparedStatement.executeUpdate();

            resultObj.put("result", updateCount+" record(s) updated");
        } catch (Exception e) {
            resultObj.put("result", "Updated failed : "+e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                
                if (preparedStatement != null) {
                	preparedStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }
                System.out.println("connection closed :: " + connection.isClosed());
            } catch (Exception e) {

            }
        }
        
        return resultObj;
    }
    
    public JSONObject getTableScript(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        Connection connection = null;
        String query = "";
        try {
            
            String tableName = request.getParameter("tableName");
            String tableType = request.getParameter("tableType");

            String connectionObjStr = request.getParameter("connObj");
            JSONObject connObj = (JSONObject) JSONValue.parse(connectionObjStr);
            String databaseName = (String) connObj.get("CONN_CUST_COL1");
            connection = (Connection) dataPipingService.getConnection(connObj);
            Statement statement = connection.createStatement();
            DatabaseMetaData metaData = null;
            ResultSet resultSet = null;
            int ddlIndex = 1;
            String tableScript = "";
            if ("ORACLE".equalsIgnoreCase(databaseName)) {
                query = "SELECT DBMS_METADATA.GET_DDL('" + tableType + "', '" + tableName + "') FROM DUAL";

                System.out.println("query ::: " + query);
                resultSet = statement.executeQuery(query);
            } else if ("MYSQL".equalsIgnoreCase(databaseName)) {
                query = "SHOW CREATE TABLE " + tableName;
                System.out.println("query ::: " + query);
                ddlIndex = 2;
                resultSet = statement.executeQuery(query);
            } else if ("SQLSERVER".equalsIgnoreCase(databaseName)) {
                metaData = connection.getMetaData();
                resultSet = metaData.getTables(null, null, tableName, new String[]{"TABLE"});
            }

            if (resultSet.next()) {
                if ("SQLSERVER".equalsIgnoreCase(databaseName)) {
                    String catalog = resultSet.getString("TABLE_CAT");
                    String schema = resultSet.getString("TABLE_SCHEM");
                    String name = resultSet.getString("TABLE_NAME");

                    ResultSet ddlResultSet = metaData.getColumns(catalog, schema, name, null);
                    StringBuilder ddlBuilder = new StringBuilder();
                    while (ddlResultSet.next()) {
                        String columnName = ddlResultSet.getString("COLUMN_NAME");
                        String columnType = ddlResultSet.getString("TYPE_NAME");
                        int columnSize = ddlResultSet.getInt("COLUMN_SIZE");
                        ddlBuilder.append(columnName).append(" ").append(columnType).append("(").append(columnSize).append(")\n ");
                    }
                    tableScript = ddlBuilder.toString();
                    tableScript = tableScript.substring(0, tableScript.length() - 2); // Remove the trailing comma and space

                    System.out.println("Table SQL Script:\n" + tableScript);
                    resultObj.put("tableScript", tableScript);
                } else if ("ORACLE".equalsIgnoreCase(databaseName) || ("MYSQL".equalsIgnoreCase(databaseName))) {
                    tableScript = resultSet.getString(ddlIndex);
                    System.out.println("Table SQL Script:");
                    System.out.println(tableScript);
                    resultObj.put("tableScript", tableScript);
                }
            }

            // Close the resources
            resultSet.close();
            statement.close();
            connection.close();

        } catch (Exception e) {
            resultObj.put("tableScript", "Error ::: Query not executed properly");
            e.printStackTrace();
        }

        return resultObj;
    }	
    
    public JSONObject getTableTriggers(HttpServletRequest request) {
        
        JSONObject resultObj = new JSONObject();
        JSONArray dataArray = new JSONArray();
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String query = "";
        try {
            String connectionObjStr = request.getParameter("connObj");
            String tableName = request.getParameter("tableName");
            JSONObject connObj = (JSONObject) JSONValue.parse(connectionObjStr);
            String databaseName = (String) connObj.get("CONN_CUST_COL1");
            connection = (Connection) dataPipingService.getConnection(connObj);
            
            List columns = null;            
            if ("ORACLE".equalsIgnoreCase(databaseName)) {
                query =  "SELECT TRIGGER_NAME, TRIGGER_TYPE, OWNER, TRIGGERING_EVENT, STATUS, TABLE_NAME, TRIGGER_BODY "
                         + "FROM ALL_TRIGGERS "
                         + "WHERE TABLE_NAME = ?";
                //+ "WHERE TABLE_NAME = '"+ "O_RECORD_MASTER" + "'";
                columns =  Arrays.asList("TRIGGER_NAME", "TRIGGER_TYPE", "OWNER", "TRIGGERING_EVENT", "STATUS", "TABLE_NAME");
                resultObj.put("columns", columns);
                System.out.println("query ::: " + query);
                statement = connection.prepareStatement(query);
                statement.setString(1, tableName.toUpperCase());  //tableName
                resultSet = statement.executeQuery();
            } else if ("MYSQL".equalsIgnoreCase(databaseName)) {
                query = "SELECT TRIGGER_NAME, ACTION_TIMING, EVENT_MANIPULATION, ACTION_STATEMENT "
                         + "FROM INFORMATION_SCHEMA.TRIGGERS "
                         + "WHERE EVENT_OBJECT_TABLE = ?";
                columns =  Arrays.asList("TRIGGER_NAME", "ACTION_TIMING", "EVENT_MANIPULATION");
                resultObj.put("columns", columns);
                System.out.println("query ::: " + query);
                statement = connection.prepareStatement(query);
                statement.setString(1, tableName.toUpperCase());
                resultSet = statement.executeQuery();
            } else if ("SQLSERVER".equalsIgnoreCase(databaseName)) {
                query = "SELECT "
                    + "t.name AS TRIGGER_NAME, "
                    + "OBJECT_NAME(t.parent_id) AS TABLE_NAME, "
                    + "OBJECT_SCHEMA_NAME(t.parent_id) AS SchemaName, "
                    + "OBJECTPROPERTY(t.object_id, 'ExecIsUpdateTrigger') AS IsUpdateTrigger, "
                    + "OBJECTPROPERTY(t.object_id, 'ExecIsInsertTrigger') AS IsInsertTrigger, "
                    + "OBJECTPROPERTY(t.object_id, 'ExecIsDeleteTrigger') AS IsDeleteTrigger, "
                    + "m.definition AS TRIGGER_BODY "
                    + "FROM sys.triggers AS t "
                    + "INNER JOIN sys.sql_modules AS m ON t.object_id = m.object_id "
                    + "WHERE OBJECT_NAME(t.parent_id) = ?";
                columns =  Arrays.asList("TRIGGER_NAME", "TABLE_NAME", "SchemaName", "IsUpdateTrigger", "IsInsertTrigger", "IsDeleteTrigger");
                resultObj.put("columns", columns);
                System.out.println("query ::: " + query);
                statement = connection.prepareStatement(query);
                statement.setString(1, tableName.toUpperCase());
                resultSet = statement.executeQuery();

            }
           
            boolean dataFlag = false;
            while (resultSet.next()) {
                JSONObject jsonObj =  new JSONObject();
                
                String triggerName = resultSet.getString("TRIGGER_NAME");
                String triggerBody = "";
                String table_Name = "";
                
                if ("ORACLE".equalsIgnoreCase(databaseName)) {
                    triggerBody = resultSet.getString("TRIGGER_BODY");
                    String triggerType = resultSet.getString("TRIGGER_TYPE");
                    String owner = resultSet.getString("OWNER");
                    String triggeringEvent = resultSet.getString("TRIGGERING_EVENT");
                    String status = resultSet.getString("STATUS");
                    table_Name = resultSet.getString("TABLE_NAME");
                    jsonObj.put("TRIGGER_TYPE", triggerType);
                    jsonObj.put("OWNER", owner);
                    jsonObj.put("TRIGGERING_EVENT", triggeringEvent);
                    jsonObj.put("STATUS", status);
                    jsonObj.put("TABLE_NAME", table_Name);
                    jsonObj.put("TRIGGER_BODY", triggerBody);
                } else if("MYSQL".equalsIgnoreCase(databaseName)){
                    String actionTiming = resultSet.getString("ACTION_TIMING");
                    String eventManipulation = resultSet.getString("EVENT_MANIPULATION");
                    String accountStatement = resultSet.getString("ACTION_STATEMENT");
                    jsonObj.put("ACTION_TIMING", actionTiming);
                    jsonObj.put("EVENT_MANIPULATION", eventManipulation);
                    jsonObj.put("TRIGGER_BODY", accountStatement);
                } else if("SQLSERVER".equalsIgnoreCase(databaseName)){
                    triggerBody = resultSet.getString("TRIGGER_BODY");
                    table_Name = resultSet.getString("TABLE_NAME");
                    String SchemaName = resultSet.getString("SchemaName");
                    String IsUpdateTrigger = resultSet.getString("IsUpdateTrigger");
                    String IsInsertTrigger = resultSet.getString("IsInsertTrigger");
                    String IsDeleteTrigger = resultSet.getString("IsDeleteTrigger");
                    jsonObj.put("TRIGGER_BODY", triggerBody);
                    jsonObj.put("TABLE_NAME", table_Name);
                    jsonObj.put("SchemaName", SchemaName);
                    jsonObj.put("IsUpdateTrigger", IsUpdateTrigger);
                    jsonObj.put("IsInsertTrigger", IsInsertTrigger);
                    jsonObj.put("IsDeleteTrigger", IsDeleteTrigger);
                    
                }
                
                jsonObj.put("TRIGGER_NAME", triggerName);
                dataArray.add(jsonObj);
                dataFlag = true;
                
            }
            resultObj.put("data", dataArray);
            
            resultObj.put("connObj", connectionObjStr);
            resultObj.put("tableName", tableName);
            
            if(dataFlag){
                resultObj.put("triggerMessage", "Successfully fetched triggers");
            } else{
                resultObj.put("triggerMessage", "No triggers found matching the specified criteria.");
            }
            
            // Close the resources
            

        } catch (Exception e) {
            resultObj.put("triggerMessage", "No triggers found matching the specified criteria.");
            e.printStackTrace();
        } finally {
            try {
                if (resultSet!=null){
                resultSet.close();
                }
                if (statement!=null){
                statement.close();
                }
                if (connection!=null){
                connection.close();
                }
                
            } catch (Exception e) {
            
            }
            
        }

        return resultObj;
    }
    
    public JSONObject getDataseFunctions(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        JSONArray dataArray = new JSONArray();
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String query = "";
        try {
            String connectionObjStr = request.getParameter("connObj");
            String functionName = request.getParameter("objectName");
            JSONObject connObj = (JSONObject) JSONValue.parse(connectionObjStr);
            String databaseName = (String) connObj.get("CONN_CUST_COL1");
            connection = (Connection) dataPipingService.getConnection(connObj);
            
            if ("ORACLE".equalsIgnoreCase(databaseName)) {
                query =  "SELECT TEXT FROM USER_SOURCE WHERE TYPE = 'FUNCTION' AND NAME = ? ORDER BY LINE";
                System.out.println("query ::: " + query);
                statement = connection.prepareStatement(query);
                statement.setString(1, functionName);  //functionName
                resultSet = statement.executeQuery();
            } else if ("MYSQL".equalsIgnoreCase(databaseName)) {   
                query =  "SELECT ROUTINE_DEFINITION "
                        + "FROM information_schema.ROUTINES"
                        + " WHERE ROUTINE_TYPE = 'FUNCTION'"
                        + " AND ROUTINE_SCHEMA = ? AND"
                        + " ROUTINE_NAME = ?";
                System.out.println("query ::: " + query);
                statement = connection.prepareStatement(query);
                statement.setString(1, (String) connObj.get("CONN_DB_NAME"));  //(String) connObj.get("CONNECTION_NAME")
                statement.setString(2, functionName);  //functionName
                resultSet = statement.executeQuery();
            } else if ("SQLSERVER".equalsIgnoreCase(databaseName)) {
                query =  "SELECT definition AS FUNCTION_DEFINITION "
                           + "FROM sys.sql_modules " 
                           + "WHERE object_id = OBJECT_ID(?)";
                System.out.println("query ::: " + query);
                statement = connection.prepareStatement(query);
                statement.setString(1, functionName);  //functionName
                resultSet = statement.executeQuery();
            }
            String text = "";
            while (resultSet.next()) {
                
                if ("ORACLE".equalsIgnoreCase(databaseName)) {   
                    text += resultSet.getString("TEXT");
                } else if("MYSQL".equalsIgnoreCase(databaseName)){  
                    text += resultSet.getString("ROUTINE_DEFINITION");
                } else if("SQLSERVER".equalsIgnoreCase(databaseName)){  
                    text += resultSet.getString("FUNCTION_DEFINITION");
                }
            } 
            System.out.println(text);
            resultObj.put("result", text);
        } catch (Exception e) {
            resultObj.put("message", "No function definition found matching the specified criteria.");
            e.printStackTrace();
        } finally{
            // Close the resources
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (Exception e) {
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (Exception e) {
                }
            }
            if (connection != null) {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (Exception e) {

                    }
                }

            }
        }

        return resultObj;  
    
    }
    
    public JSONObject getDataseProcedures(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        JSONArray dataArray = new JSONArray();
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String query = "";
        try {
            String connectionObjStr = request.getParameter("connObj");
            String objectName = request.getParameter("objectName");
            JSONObject connObj = (JSONObject) JSONValue.parse(connectionObjStr);
            String databaseName = (String) connObj.get("CONN_CUST_COL1");
            connection = (Connection) dataPipingService.getConnection(connObj);
            
            if ("ORACLE".equalsIgnoreCase(databaseName)) {
                query =  "SELECT DBMS_METADATA.GET_DDL('PROCEDURE', ?) AS PROCEDURE_CODE "
                            + "FROM DUAL";
                System.out.println("query ::: " + query);
                statement = connection.prepareStatement(query);
                statement.setString(1, objectName);  //ProcedureName
                resultSet = statement.executeQuery();
            } else if ("MYSQL".equalsIgnoreCase(databaseName)) {   
                query =  "SHOW CREATE PROCEDURE " + objectName;
                System.out.println("query ::: " + query);
                statement = connection.prepareStatement(query);  //ProcedureName
                resultSet = statement.executeQuery();
            } else if ("SQLSERVER".equalsIgnoreCase(databaseName)) {
                query =  "SELECT OBJECT_DEFINITION(OBJECT_ID(?)) AS PROCEDURE_CODE";
                System.out.println("query ::: " + query);
                statement = connection.prepareStatement(query);
                statement.setString(1, objectName);  //ProcedureName
                resultSet = statement.executeQuery();
            }
            String text = "";
            while (resultSet.next()) {                
                if ("ORACLE".equalsIgnoreCase(databaseName) || "SQLSERVER".equalsIgnoreCase(databaseName)) {   
                    text += resultSet.getString("PROCEDURE_CODE");
                } else if("MYSQL".equalsIgnoreCase(databaseName)){  
                    text += resultSet.getString("CREATE PROCEDURE");
                }
            } 
            System.out.println(text);
            resultObj.put("result", text);
        } catch (Exception e) {
            resultObj.put("message", "No procedure code found matching the specified criteria.");
            e.printStackTrace();
        } finally{
            // Close the resources
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (Exception e) {
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (Exception e) {
                }
            }
            if (connection != null) {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (Exception e) {

                    }
                }

            }
        }

        return resultObj;  
    
    }
    
 public JSONObject enableOrDisableTrigger(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String query = "";
        try {
            String connectionObjStr = request.getParameter("connObjStr");
            String tableName = request.getParameter("tableName");
            String triggerName = request.getParameter("triggerName");
            String enableDisableFlag = request.getParameter("enableDisableFlag");
            JSONObject connObj = (JSONObject) JSONValue.parse(connectionObjStr);
            String databaseName = (String) connObj.get("CONN_CUST_COL1");
            connection = (Connection) dataPipingService.getConnection(connObj);
            if ("ORACLE".equalsIgnoreCase(databaseName)) {
                query =  "ALTER TRIGGER " + triggerName + " " + enableDisableFlag;
                System.out.println("query ::: " + query);
                statement = connection.prepareStatement(query);
                resultSet = statement.executeQuery();
            }
//            while (resultSet.next()) {                
              resultObj.put("message", triggerName + " Trigger " + enableDisableFlag.toLowerCase()  + "d successfully");
//            }
            resultObj.put("triggerName",triggerName);
            resultObj.put("tableName", tableName);
            resultObj.put("connObj", connectionObjStr);
            resultObj.put("enableDisableFlag", enableDisableFlag);
            resultObj.put("flag", "T");
            
        } catch (Exception e) {
            resultObj.put("message", "Error executing script");
            e.printStackTrace();
        } finally{
            // Close the resources
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (Exception e) {
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (Exception e) {
                }
            }
            if (connection != null) {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (Exception e) {

                    }
                }

            }
        }

        return resultObj;
    }

    public JSONObject dropTable(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        Connection connection = null;

        String message = "";

        try {
            String tableName = request.getParameter("tableName");
            String connObjStr = request.getParameter("connObj");
            JSONObject connObject = (JSONObject) JSONValue.parse(connObjStr);
            connection = (Connection) dataPipingService.getConnection(connObject);
            message = tableOperationsDAO.dropTable(request, connection, tableName);

        } catch (Exception e) {

            e.printStackTrace();
        } finally {
            try {
                System.out.println("connection closed :: " + connection.isClosed());
            } catch (Exception e) {

            }
        }
        resultObj.put("message", message);
        return resultObj;
    }
}
