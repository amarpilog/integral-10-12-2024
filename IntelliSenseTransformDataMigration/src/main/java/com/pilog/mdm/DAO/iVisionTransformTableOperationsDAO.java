/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.DAO;


import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.pilog.mdm.access.DataAccess;
import com.pilog.mdm.transformutills.V10DataPipingUtills;
import com.pilog.mdm.utilities.PilogUtilities;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;

import org.bson.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 *
 * @author Ravindar.P
 */
@Repository
public class iVisionTransformTableOperationsDAO {

    @Autowired
    private DataAccess access;
    private V10DataPipingUtills dataPipingUtills = new V10DataPipingUtills();

    @Transactional
    public String truncateTableData(HttpServletRequest request, Connection connection, String tableName) {
        PreparedStatement preparedStatement = null;
        String message = "";
        JSONObject labelObj = new PilogUtilities().getMultilingualObject(request);
        try {

            if (tableName != null && !"".equalsIgnoreCase(tableName) && !"null".equalsIgnoreCase(tableName)) {
                System.out.println("tableName:::" + tableName);
                String truncateQuery = "TRUNCATE TABLE " + tableName;
                preparedStatement = connection.prepareStatement(truncateQuery);
                boolean tableTruncated = preparedStatement.execute();
                message = String.valueOf(new PilogUtilities().convertIntoMultilingualValue(labelObj, "Table truncated sucessfully"));

            }
//            System.out.println("columnsArray::::" + sourceColumnsList);
        } catch (Exception e) {
            message = String.valueOf(new PilogUtilities().convertIntoMultilingualValue(labelObj, "error truncating table"));
            e.printStackTrace();
        } finally {
            try {

                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
            }
        }
        return message;
    }

//    @Transactional
//    public String updateRecordsInTable(HttpServletRequest request, Connection connection,
//            String tableName, List tablePKColumns, JSONArray columnsList, JSONArray totalData, JSONObject updateInfo) {
//        PreparedStatement preparedStatement = null;
//        String message = "";
//
//        int totalUpdateCount = 0;
//
//        JSONObject labelObj = new PiLogCloudUtills().getMultilingualObject(request);
//        try {
//
//            if (tableName != null && !"".equalsIgnoreCase(tableName) && !"null".equalsIgnoreCase(tableName)) {
//                System.out.println("tableName:::" + tableName);
//
//                for (int i = 0; i < totalData.size(); i++) {
//                    JSONObject rowdata = (JSONObject) totalData.get(i);
//
//                    String rowIndex = String.valueOf(rowdata.get("boundindex"));
//                    JSONObject rowUpdateInfo = (JSONObject) updateInfo.get(rowIndex);
//
//                    rowdata.remove("uid");
//                    rowdata.remove("boundindex");
//                    rowdata.remove("visibleindex");
//                    rowdata.remove("uniqueid");
//                    rowdata.remove(tableName + "_HIDDEN");
//
//                    String whereClauseCond = "";
//
//                    List updateNotNullColsList = new ArrayList();
//                    List notnullPkColsList = new ArrayList();
//                    List updateColList = new ArrayList(rowUpdateInfo.keySet());
//                    String updateColValuesStr = (String) updateColList.stream().map(col -> String.valueOf(col + "=?")).collect(Collectors.joining(","));
//
//                    List<String> whereClauseCondList = new ArrayList();
//
//                    updateNotNullColsList = (List) updateColList.stream().filter(col -> rowUpdateInfo.get(col) != null).map(col -> col).collect(Collectors.toList());
//
//                    String updateColsWhereClauseCondNotNull = (String) updateColList.stream().filter(col -> rowUpdateInfo.get(col) != null).map(col -> String.valueOf(col + "=?")).collect(Collectors.joining(" AND "));
//                    whereClauseCondList.add(updateColsWhereClauseCondNotNull);
//
//                    String updateColsWhereClauseCondNull = (String) updateColList.stream().filter(col -> (rowUpdateInfo.get(col) == null)).map(col -> String.valueOf(col + " IS NULL ")).collect(Collectors.joining(" AND "));
//                    whereClauseCondList.add(updateColsWhereClauseCondNull);
//
//                    notnullPkColsList = (List) tablePKColumns.stream().filter(col -> !updateColList.contains(col)).filter(col -> (rowdata.get(col) != null)).map(col -> col).collect(Collectors.toList());
//
//                    String whereClauseCondNotNull = (String) tablePKColumns.stream().filter(col -> !updateColList.contains(col)).filter(col -> (rowdata.get(col) != null)).map(col -> String.valueOf(col + "=?")).collect(Collectors.joining(" AND "));
//                    whereClauseCondList.add(whereClauseCondNotNull);
//                    String whereClauseCondNull = (String) tablePKColumns.stream().filter(col -> !updateColList.contains(col)).filter(col -> (rowdata.get(col) == null)).map(col -> String.valueOf(col + " IS NULL ")).collect(Collectors.joining(" AND "));
//                    whereClauseCondList.add(whereClauseCondNull);
//
//                    whereClauseCond = whereClauseCondList.stream().filter(cond -> (cond != null && !"".equalsIgnoreCase(cond))).map(cond -> cond).collect(Collectors.joining(" AND "));
//
//                    String updateQuery = "UPDATE " + tableName + " SET " + updateColValuesStr + " WHERE " + whereClauseCond;
//                    System.out.println("updateQuery :: " + updateQuery);
//                    preparedStatement = connection.prepareStatement(updateQuery);
//
//                    for (int j = 0; j < updateColList.size(); j++) {
//                        String columnName = String.valueOf(updateColList.get(j));
//                        Object value = rowdata.get(columnName);
//
//                        preparedStatement.setObject(j + 1, (value != null) ? value : "");
//                    }
//
//                    for (int j = 0; j < updateNotNullColsList.size(); j++) {
//                        String columnName = String.valueOf(updateNotNullColsList.get(j));
//                        Object value = null;
//
//                        value = rowUpdateInfo.get(columnName);
//
//                        preparedStatement.setObject(j + updateColList.size() + 1, value);
//                    }
//                    for (int j = 0; j < notnullPkColsList.size(); j++) {
//                        String columnName = String.valueOf(notnullPkColsList.get(j));
//                        Object value = null;
//
//                        value = rowdata.get(columnName);
//
//                        preparedStatement.setObject(j + updateColList.size() + updateNotNullColsList.size() + 1, value);
//                    }
//                    int updateCount = preparedStatement.executeUpdate();
//                    totalUpdateCount += updateCount;
//                }
//
//                connection.commit();
//                message = String.valueOf(new PiLogCloudUtills().convertIntoMultilingualValue(labelObj, totalUpdateCount + " records(s) updated sucessfully"));
//
//            }
////            System.out.println("columnsArray::::" + sourceColumnsList);
//        } catch (Exception e) {
//            message = String.valueOf(new PiLogCloudUtills().convertIntoMultilingualValue(labelObj, "error updating row(s)" + e.getMessage()));
//            e.printStackTrace();
//        } finally {
//            try {
//
//                if (preparedStatement != null) {
//                    preparedStatement.close();
//                }
//                if (connection != null) {
//                    connection.close();
//                }
//            } catch (Exception e) {
//            }
//        }
//        return message;
//    }

    
//    @Transactional
//	public String updateRecordsInTable(HttpServletRequest request, Connection connection, String tableName,
//			List<String> tablePKColumns, JSONArray columnsList, JSONArray totalData, JSONObject updateInfo) {
//		PreparedStatement preparedStatement = null;
//		String message = "";
//
//		int totalUpdateCount = 0;
//
//		JSONObject labelObj = new PiLogCloudUtills().getMultilingualObject(request);
//		try {
//
//			if (tableName != null && !"".equalsIgnoreCase(tableName) && !"null".equalsIgnoreCase(tableName)) {
//				System.out.println("tableName::::" + tableName);
//				JSONObject columnDataType = getTablesColumnDataTypes(tableName, connection);
//				for (int i = 0; i < totalData.size(); i++) {
//					JSONObject rowdata = (JSONObject) totalData.get(i);
//
//					String rowIndex = String.valueOf(rowdata.get("boundindex"));
//					JSONObject rowUpdateInfo = (JSONObject) updateInfo.get(rowIndex);
//
//					rowdata.remove("uid");
//					rowdata.remove("boundindex");
//					rowdata.remove("visibleindex");
//					rowdata.remove("uniqueid");
//					rowdata.remove("ROWNUM");// 050123
//					tablePKColumns.remove("ROWNUM");// 050123
//					rowdata.remove(tableName + "_HIDDEN");
//
//					String whereClauseCond = "";
//
//					List<String> updateNotNullColsList = new ArrayList<String>();
//					List<String> notnullPkColsList = new ArrayList<String>();
//					List<String> updateColList = new ArrayList<String>(rowUpdateInfo.keySet());
//					String updateColValuesStr = (String) updateColList.stream().map(col -> String.valueOf(col + "=?"))
//							.collect(Collectors.joining(","));
//
//					List<String> whereClauseCondList = new ArrayList<String>();
//
//					updateNotNullColsList = (List<String>) updateColList.stream()
//							.filter(col -> rowUpdateInfo.get(col) != null && (columnDataType.get(col) != null)
//									&& !"CLOB".equalsIgnoreCase(columnDataType.get(col).toString())
//									&& !"BLOB".equalsIgnoreCase(columnDataType.get(col).toString()))
//							.map(col -> col).collect(Collectors.toList());
//
//					String updateColsWhereClauseCondNotNull = (String) updateColList.stream()
//							.filter(col -> rowUpdateInfo.get(col) != null && (columnDataType.get(col) != null)
//									&& !"CLOB".equalsIgnoreCase(columnDataType.get(col).toString())
//									&& !"BLOB".equalsIgnoreCase(columnDataType.get(col).toString()))
//							.map(col -> String.valueOf(col + "=?")).collect(Collectors.joining(" AND "));
//					whereClauseCondList.add(updateColsWhereClauseCondNotNull);
//
//					String updateColsWhereClauseCondNull = (String) updateColList.stream()
//							.filter(col -> (rowUpdateInfo.get(col) == null) && (columnDataType.get(col) != null)
//									&& !"CLOB".equalsIgnoreCase(columnDataType.get(col).toString())
//									&& !"BLOB".equalsIgnoreCase(columnDataType.get(col).toString()))
//							.map(col -> String.valueOf(col + " IS NULL ")).collect(Collectors.joining(" AND "));
//					whereClauseCondList.add(updateColsWhereClauseCondNull);
////040123 
//					notnullPkColsList = (List<String>) tablePKColumns.stream()
//							.filter(col -> (rowdata.get(col) != null) && !updateColList.contains(col)
//									&& (columnDataType.get(col) != null)
//									&& !"CLOB".equalsIgnoreCase(columnDataType.get(col).toString())
//									&& !"BLOB".equalsIgnoreCase(columnDataType.get(col).toString()))
//							.map(col -> col).collect(Collectors.toList());
//
//					String whereClauseCondNotNull = (String) tablePKColumns.stream()
//							.filter(col -> !updateColList.contains(col) && rowdata.get(col) != null
//									&& (columnDataType.get(col) != null)
//									&& !"CLOB".equalsIgnoreCase(columnDataType.get(col).toString())
//									&& !"BLOB".equalsIgnoreCase(columnDataType.get(col).toString()))
//							.map(col -> String.valueOf(col + "=?")).collect(Collectors.joining(" AND "));
//					whereClauseCondList.add(whereClauseCondNotNull);
//					String whereClauseCondNull = (String) tablePKColumns.stream()
//							.filter(col -> !updateColList.contains(col) && rowdata.get(col) == null
//									&& (columnDataType.get(col) != null)
//									&& !"CLOB".equalsIgnoreCase(columnDataType.get(col).toString())
//									&& !"BLOB".equalsIgnoreCase(columnDataType.get(col).toString()))
//							.map(col -> String.valueOf(col + " IS NULL ")).collect(Collectors.joining(" AND "));
//					whereClauseCondList.add(whereClauseCondNull);
//
//					whereClauseCond = whereClauseCondList.stream()
//							.filter(cond -> (cond != null && !"".equalsIgnoreCase(cond))).map(cond -> cond)
//							.collect(Collectors.joining(" AND "));
//
//					String updateQuery = "UPDATE " + tableName + " SET " + updateColValuesStr + " WHERE "
//							+ whereClauseCond;
//					System.out.println("updateQuery :: " + updateQuery);
//					preparedStatement = connection.prepareStatement(updateQuery);
//
//					if (columnDataType != null && !columnDataType.isEmpty()) {
//						for (int j = 0; j < updateColList.size(); j++) {
//							String columnName = String.valueOf(updateColList.get(j));
//							Object value = rowdata.get(columnName);
//							String columnType = columnDataType.get(columnName).toString();
//							value = setPreparedStatementObject(columnName, columnType, value, connection);
//
//							System.out.println(value);
//							preparedStatement.setObject(j + 1, (value != null) ? value : "");
//						}
//
//						for (int j = 0; j < updateNotNullColsList.size(); j++) {
//							String columnName = String.valueOf(updateNotNullColsList.get(j));
//							Object value = null;
//							String columnType = columnDataType.get(columnName).toString();
//							value = rowUpdateInfo.get(columnName);
//							value = setPreparedStatementObject(columnName, columnType, value, connection);
//
//							preparedStatement.setObject(j + updateColList.size() + 1, value);
//						}
//						for (int j = 0; j < notnullPkColsList.size(); j++) {
//							String columnName = String.valueOf(notnullPkColsList.get(j));
//
//							Object value = null;
//							String columnType = columnDataType.get(columnName).toString();
//							value = rowdata.get(columnName);
//							value = setPreparedStatementObject(columnName, columnType, value, connection);
//							preparedStatement.setObject(j + updateColList.size() + updateNotNullColsList.size() + 1,
//									value);
//						}
//					} else {
//						System.out.println("Cannot setObject::: columnDataType is empty");
//					}
//					int updateCount = preparedStatement.executeUpdate();
//					totalUpdateCount += updateCount;
//				}
//
////291222                connection.commit();
//				message = String.valueOf(new PiLogCloudUtills().convertIntoMultilingualValue(labelObj,
//						totalUpdateCount + " records(s) updated sucessfully"));
//
//			}
////            System.out.println("columnsArray::::" + sourceColumnsList);
//		} catch (Exception e) {
//			message = String.valueOf(new PiLogCloudUtills().convertIntoMultilingualValue(labelObj,
//					"error updating row(s)" + e.getMessage()));
//			e.printStackTrace();
//		} finally {
//			try {
//
//				if (preparedStatement != null) {
//					preparedStatement.close();
//				}
//				if (connection != null) {
//					connection.close();
//				}
//			} catch (Exception e) {
//			}
//		}
//		return message;
//	}

    
    @Transactional
	public String updateRecordsInTable(HttpServletRequest request, Connection connection, String tableName,
			List<String> tablePKColumns, JSONArray columnsList, JSONArray totalData, JSONObject updateInfo) {
		PreparedStatement preparedStatement = null;
		String message = "";

		int totalUpdateCount = 0;
                List queries = new ArrayList();
		JSONObject labelObj = new PilogUtilities().getMultilingualObject(request);
		try {
                        
			if (tableName != null && !"".equalsIgnoreCase(tableName) && !"null".equalsIgnoreCase(tableName)) {
				System.out.println("tableName::::" + tableName);
                                String duplicateFlag = "";
                                String isDupRow = "";
                                String hasPkColumn = "";
				JSONObject columnDataType = getTablesColumnDataTypes(tableName, connection);
				for (int i = 0; i < totalData.size(); i++) {
					JSONObject rowdata = (JSONObject) totalData.get(i);
                                        
					String rowIndex = String.valueOf(rowdata.get("boundindex"));
					JSONObject rowUpdateInfo = (JSONObject) updateInfo.get(rowIndex);
                                        String dupRowIndex = String.valueOf(rowdata.get("ROWNUM"));
                                        if(rowdata.containsKey("isDupRow")){
                                            isDupRow = String.valueOf(rowdata.get("isDupRow"));
                                            rowdata.remove("isDupRow");
                                        }
                                        if(rowdata.containsKey("pkColumn")){
                                            hasPkColumn = String.valueOf(rowdata.get("pkColumn"));
                                            rowdata.remove("pkColumn");
                                        }
                                        System.out.println("hasPkCOlumn  ::: " + hasPkColumn);
					rowdata.remove("uid");
					rowdata.remove("boundindex");
					rowdata.remove("visibleindex");
					rowdata.remove("uniqueid");
					rowdata.remove("ROWNUM");// 050123
					tablePKColumns.remove("ROWNUM");// 050123
					rowdata.remove(tableName + "_HIDDEN");
                                        
                                        
					String whereClauseCond = "";

					List<String> updateNotNullColsList = new ArrayList<String>();
					List<String> notnullPkColsList = new ArrayList<String>();
					List<String> updateColList = new ArrayList<String>(rowUpdateInfo.keySet());//
                                        
					String updateColValuesStr = (String) updateColList.stream().map(col -> String.valueOf(col + "=?"))
							.collect(Collectors.joining(","));
                                        
                                        String updateDupColValuesStr = (String) updateColList.stream().map(col -> String.valueOf("t." + col + "=?"))
							.collect(Collectors.joining(","));

					List<String> whereClauseCondList = new ArrayList<String>();

					updateNotNullColsList = (List<String>) updateColList.stream()
							.filter(col -> rowUpdateInfo.get(col) != null && (columnDataType.get(col) != null)
									&& !"CLOB".equalsIgnoreCase(columnDataType.get(col).toString())
									&& !"BLOB".equalsIgnoreCase(columnDataType.get(col).toString()))
							.map(col -> col).collect(Collectors.toList());

					String updateColsWhereClauseCondNotNull = (String) updateColList.stream()
							.filter(col -> rowUpdateInfo.get(col) != null && (columnDataType.get(col) != null)
									&& !"CLOB".equalsIgnoreCase(columnDataType.get(col).toString())
									&& !"BLOB".equalsIgnoreCase(columnDataType.get(col).toString()))
							.map(col -> String.valueOf(col + "=?")).collect(Collectors.joining(" AND "));
					whereClauseCondList.add(updateColsWhereClauseCondNotNull);

					String updateColsWhereClauseCondNull = (String) updateColList.stream()
							.filter(col -> (rowUpdateInfo.get(col) == null) && (columnDataType.get(col) != null)
									&& !"CLOB".equalsIgnoreCase(columnDataType.get(col).toString())
									&& !"BLOB".equalsIgnoreCase(columnDataType.get(col).toString()))
							.map(col -> String.valueOf(col + " IS NULL ")).collect(Collectors.joining(" AND "));
					whereClauseCondList.add(updateColsWhereClauseCondNull);
//040123 
					notnullPkColsList = (List<String>) tablePKColumns.stream()
							.filter(col -> (rowdata.get(col) != null) && !updateColList.contains(col)
									&& (columnDataType.get(col) != null)
									&& !"CLOB".equalsIgnoreCase(columnDataType.get(col).toString())
									&& !"BLOB".equalsIgnoreCase(columnDataType.get(col).toString()))
							.map(col -> col).collect(Collectors.toList());

					String whereClauseCondNotNull = (String) tablePKColumns.stream()
							.filter(col -> !updateColList.contains(col) && rowdata.get(col) != null
									&& (columnDataType.get(col) != null)
									&& !"CLOB".equalsIgnoreCase(columnDataType.get(col).toString())
									&& !"BLOB".equalsIgnoreCase(columnDataType.get(col).toString()))
							.map(col -> String.valueOf(col + "=?")).collect(Collectors.joining(" AND "));
					whereClauseCondList.add(whereClauseCondNotNull);
					String whereClauseCondNull = (String) tablePKColumns.stream()
							.filter(col -> !updateColList.contains(col) && rowdata.get(col) == null
									&& (columnDataType.get(col) != null)
									&& !"CLOB".equalsIgnoreCase(columnDataType.get(col).toString())
									&& !"BLOB".equalsIgnoreCase(columnDataType.get(col).toString()))
							.map(col -> String.valueOf(col + " IS NULL ")).collect(Collectors.joining(" AND "));
					whereClauseCondList.add(whereClauseCondNull);

					whereClauseCond = whereClauseCondList.stream()
							.filter(cond -> (cond != null && !"".equalsIgnoreCase(cond))).map(cond -> cond)
							.collect(Collectors.joining(" AND "));
                                        String updateQuery = "";
                                        
                                        if(!"Y".equalsIgnoreCase(hasPkColumn) && "Y".equalsIgnoreCase(isDupRow)){
                                            String duplicateQueryColumn = "";
                                            if (columnDataType != null && !columnDataType.isEmpty()) {
                                                for(Object key : columnDataType.keySet()){
                                                    if(updateColList.get(0) != (String) key){
                                                       duplicateQueryColumn = (String) key;
                                                       break;
                                                    }
                                                }
                                            } else {
                                                Set<String> rowdataKeys = rowdata.keySet();
                                                Iterator<String> iterator = rowdataKeys.iterator();
                                                if (iterator.hasNext()) {
                                                    duplicateQueryColumn = iterator.next();
                                                }
                                            }
                                            queries = generateDuplicateRecordsQuery(connection,duplicateQueryColumn,tableName
                                                    ,dupRowIndex,updateColValuesStr,updateDupColValuesStr);
                                            if(queries.size() > 1){
                                                preparedStatement = connection.prepareStatement((String) queries.get(0));
                                                preparedStatement.execute();
                                                updateQuery = (String) queries.get(1);
                                            } else if(queries.size() == 1){
                                                updateQuery = (String) queries.get(0);
                                            }
                                        } else {
                                            updateQuery = "UPDATE " + tableName + " SET " + updateColValuesStr + " WHERE "
							+ whereClauseCond;
                                        }
					System.out.println("updateQuery :: " + updateQuery);
					preparedStatement = connection.prepareStatement(updateQuery);

					if (columnDataType != null && !columnDataType.isEmpty()) {
						for (int j = 0; j < updateColList.size(); j++) {
							String columnName = String.valueOf(updateColList.get(j));
							Object value = rowdata.get(columnName);
							String columnType = columnDataType.get(columnName).toString();
							value = setPreparedStatementObject(columnName, columnType, value, connection);

							System.out.println(value);
							preparedStatement.setObject(j + 1, (value != null) ? value : "");
						}
                                                if(!"Y".equalsIgnoreCase(isDupRow)){
                                                    for (int j = 0; j < updateNotNullColsList.size(); j++) {
							String columnName = String.valueOf(updateNotNullColsList.get(j));
							Object value = null;
							String columnType = columnDataType.get(columnName).toString();
							value = rowUpdateInfo.get(columnName);
							value = setPreparedStatementObject(columnName, columnType, value, connection);

							preparedStatement.setObject(j + updateColList.size() + 1, value);
                                                    }
                                                    for (int j = 0; j < notnullPkColsList.size(); j++) {
                                                            String columnName = String.valueOf(notnullPkColsList.get(j));

                                                            Object value = null;
                                                            String columnType = columnDataType.get(columnName).toString();
                                                            value = rowdata.get(columnName);
                                                            value = setPreparedStatementObject(columnName, columnType, value, connection);
                                                            preparedStatement.setObject(j + updateColList.size() + updateNotNullColsList.size() + 1,
                                                                            value);
                                                    }
                                                }
						
					} else {
						System.out.println("Cannot setObject::: columnDataType is empty");
					}
                                        
					int updateCount = preparedStatement.executeUpdate();
					totalUpdateCount += updateCount;
                                        if(queries.size() > 1){
                                                preparedStatement = connection.prepareStatement((String) queries.get(2));
                                                preparedStatement.execute();
                                            }
				}

//291222                connection.commit();
				message = String.valueOf(new PilogUtilities().convertIntoMultilingualValue(labelObj,
						totalUpdateCount + " records(s) updated sucessfully"));

			}
//            System.out.println("columnsArray::::" + sourceColumnsList);
		} catch (Exception e) {
                    if(queries.size() > 1){
                        try {
                            preparedStatement = connection.prepareStatement((String) queries.get(2));
                            preparedStatement.execute();
                        } catch (SQLException ex) {
                            
                        }
                        
                    }
			message = String.valueOf(new PilogUtilities().convertIntoMultilingualValue(labelObj,
					"error updating row(s)" + e.getMessage()));
			e.printStackTrace();
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
			}
		}
		return message;
	}
        
    
    @Transactional
    private List generateDuplicateRecordsQuery(Connection connection, String duplicateQueryColumn,
            String tableName, String dupRowIndex,String updateColValuesStr, String updateDupColValuesStr) {
      
        DatabaseMetaData metaData = null;
        List queries = new ArrayList();
        
        try {
            String query = "";
            metaData = connection.getMetaData();
            String databaseName = metaData.getDatabaseProductName(); 
            if("ORACLE".equalsIgnoreCase(databaseName)){
                query = "MERGE INTO " + tableName + " t " +
                        "USING (" +
                        "SELECT " +
                        "ROWID " +
                        "FROM ( " +
                        "SELECT " +
                        "ROWID, " +
                        "ROW_NUMBER() OVER (PARTITION BY " + duplicateQueryColumn + " ORDER BY " + duplicateQueryColumn + ") AS rn " +
                        "FROM TEMP_PK " +
                        "WHERE " + duplicateQueryColumn + " IN ( " +
                        "SELECT " + duplicateQueryColumn + " " +
                        "FROM TEMP_PK " +
                        "GROUP BY " + duplicateQueryColumn + " " +
                        "HAVING COUNT(*) > 1 " +
                        ") " +
                        ") " +
                        "WHERE rn = " + dupRowIndex +
                        " ) dup ON (t.ROWID = dup.ROWID) " +
                        "WHEN MATCHED THEN " +
                        "UPDATE SET " + updateDupColValuesStr;
                queries.add(query);
            } else if("MYSQL".equalsIgnoreCase(databaseName) || "Microsoft SQL Server".equalsIgnoreCase(databaseName)) {
                String s1 = "";
                String s2 = "";
                String s3 = "";
                if("MYSQL".equalsIgnoreCase(databaseName)){
                    s1 = "ALTER TABLE " + tableName + " ADD COLUMN TEMP_" + tableName + "_PK INT AUTO_INCREMENT PRIMARY KEY";
                    s3 = "ALTER TABLE " + tableName + " DROP COLUMN TEMP_" + tableName + "_PK";
                    s2 = "UPDATE " + tableName + " SET " + updateColValuesStr + " WHERE TEMP_" + tableName + "_PK = " + dupRowIndex;
                } else if("Microsoft SQL Server".equalsIgnoreCase(databaseName)){
                    s1 = "ALTER TABLE " + tableName + " ADD P_K INT IDENTITY(1,1)";
                    //ALTER TABLE temp_up ADD new_column INT IDENTITY(1,1);

                    s2 = "UPDATE " + tableName + " SET " + updateColValuesStr + " WHERE P_K = " + dupRowIndex;
                    s3 = "ALTER TABLE " + tableName + " DROP COLUMN P_K";
                }
                 
                 
                
                queries.add(s1);
                queries.add(s2);
                queries.add(s3);
            } 
        } catch (Exception e) {
            e.printStackTrace();
        } 

        return queries;   
    }    

    
    public Object setPreparedStatementObject(String columnName, String columnType, Object value, Connection connection) {
		// TODO Auto-generated method stub
		// this method converts the value into the required type which is defined in the
		// table
		// defaults to string
		try {
			if (columnName == null || columnType == null || "".equalsIgnoreCase(columnType)
					|| "".equalsIgnoreCase(columnName)) {
				System.out.println("value remained unchanged");
				return value;
			}
			if ("DATE".equalsIgnoreCase(columnType) || "DATETIME".equalsIgnoreCase(columnType)
					|| "TIMESTAMP".equalsIgnoreCase(columnType)) {
				// 291222
				java.util.Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(String.valueOf(value));
				java.sql.Date sqlDate = new java.sql.Date(date.getTime());
				value = sqlDate;
			} else if ("VARCHAR2".equalsIgnoreCase(columnType) || "VARCHAR".equalsIgnoreCase(columnType)
					|| "RAW".equalsIgnoreCase(columnType)) {
				value = value.toString();
			} else if ("CLOB".equalsIgnoreCase(columnType)) {
				Clob clob = connection.createClob();
				clob.setString(1, value.toString());
				value = clob;
			} else if ("NUMBER".equalsIgnoreCase(columnType)) {
				value = Double.parseDouble(value.toString());
			} else {
				System.out.println("setPreparedStatementObject::: value remained unchanged");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return value;
	}
    
    @Transactional
	public JSONObject getTablesColumnDataTypes(String tableName, Connection connection) {
		// TODO Auto-generated method stub
		JSONObject resultObj = new JSONObject();
		try {
			if (tableName == null || "".equalsIgnoreCase(tableName) || connection == null) {
				System.err.println("getColumnDataType empty tableName or connection");
				return null;
			}
			String selectQuery = "SELECT * FROM " + tableName + " OFFSET 0 ROWS FETCH NEXT 2 ROWS ONLY";
			PreparedStatement preparedStatement = connection.prepareStatement(selectQuery);
			ResultSet resultSet = preparedStatement.executeQuery();
			ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
			int columnCount = resultSetMetaData.getColumnCount();
			if (columnCount >= 1) {
				
                            for (int i = 1; i <= columnCount; i++) {
                                    JSONObject dataFieldsObj = new JSONObject();
                                    String columnType = resultSetMetaData.getColumnTypeName(i);
                                    String columnName = resultSetMetaData.getColumnName(i);
                                    resultObj.put(columnName, columnType);
                            }
				
			} else {
				System.out.println("getColumnDataType::  table does not exist or has no columns");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}
    
    @Transactional
    public String deleteRecordsInTable(HttpServletRequest request, Connection connection,
            String tableName, List tablePKColumns, JSONArray columnsList, JSONArray totalData) {
        PreparedStatement preparedStatement = null;
        String message = "";

        int totalDeleteCount = 0;

        JSONObject labelObj = new PilogUtilities().getMultilingualObject(request);
        try {

            if (tableName != null && !"".equalsIgnoreCase(tableName) && !"null".equalsIgnoreCase(tableName)) {
                System.out.println("tableName:::" + tableName);
//                Map columnsTypeObj = processJobTransformationsService.getColumnsType(tableName, columnsList, connection);
//                whereClauseCond = (String) tablePKColumns.stream().map(pk -> String.valueOf(pk + "=?")).collect(Collectors.joining(" AND "));

                for (int i = 0; i < totalData.size(); i++) {

                    JSONObject rowdata = (JSONObject) totalData.get(i);
                    rowdata.remove("uid");
                    rowdata.remove("boundindex");
                    rowdata.remove("visibleindex");
                    rowdata.remove("uniqueid");
                    rowdata.remove(tableName + "_HIDDEN");
                    List<String> notnullColList = (List) rowdata.keySet().stream().filter(key -> (rowdata.get(key) != null)).map(col -> col).collect(Collectors.toList());

                    String whereClauseNotNullCond = (String) rowdata.keySet().stream().filter(key -> (rowdata.get(key) != null)).map(pk -> String.valueOf(pk + "=?")).collect(Collectors.joining(" AND "));

                    String whereClauseNullCond = (String) rowdata.keySet().stream().filter(key -> (rowdata.get(key) == null)).map(pk -> String.valueOf(pk + " IS NULL ")).collect(Collectors.joining(" AND "));
                    String whereClauseCond = "";
                    if (whereClauseNotNullCond != null && !"".equalsIgnoreCase(whereClauseNotNullCond)
                            && whereClauseNullCond != null && !"".equalsIgnoreCase(whereClauseNullCond)) {
                        whereClauseCond += whereClauseNotNullCond + " AND " + whereClauseNullCond;
                    } else {
                        whereClauseCond += whereClauseNotNullCond;
                        whereClauseCond += whereClauseNullCond;
                    }
//                    for (Object key : rowdata.keySet()) {
//                     
//                            if (tablePKColumns.contains(key)) {
//                                Object value = rowdata.get(key);
//                                preparedStatement.setObject(tablePKColumns.indexOf(key)+1, value);
//                            }
//                      
//
//                    }
                    String deleteQuery = "DELETE FROM " + tableName + " WHERE " + whereClauseCond;
                    System.out.println("deleteQuery :: " + deleteQuery);
                    preparedStatement = connection.prepareStatement(deleteQuery);
                    for (int j = 0; j < notnullColList.size(); j++) {
                        String columnName = (String) notnullColList.get(j);
                        Object value = rowdata.get(columnName);
                        preparedStatement.setObject(j + 1, value);
                    }
                    int deleteCOunt = preparedStatement.executeUpdate();
                    totalDeleteCount += deleteCOunt;
                }
                
                try {
                	 connection.commit();
                } catch (Exception e) {}
               
                message = String.valueOf(new PilogUtilities().convertIntoMultilingualValue(labelObj, totalDeleteCount + " row(s) deleted sucessfully"));

            }
//            System.out.println("columnsArray::::" + sourceColumnsList);
        } catch (Exception e) {
            message = String.valueOf(new PilogUtilities().convertIntoMultilingualValue(labelObj, "error deleting row(s)" + e.getMessage()));
            e.printStackTrace();
        } finally {
            try {

                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
            }
        }
        return message;
    }

    @Transactional
    public String insertRecordsToTable(HttpServletRequest request, Connection connection,
            String tableName, JSONArray columnsList, JSONArray totalData) {
        PreparedStatement preparedStatement = null;
        String message = "";
        String whereClauseCond = "";
        JSONObject labelObj = new PilogUtilities().getMultilingualObject(request);
        try {
        	JSONObject columnDataType = getTablesColumnDataTypes(tableName, connection);
            if (tableName != null && !"".equalsIgnoreCase(tableName) && !"null".equalsIgnoreCase(tableName)) {
            	
            	if (columnDataType != null && !columnDataType.isEmpty()) {
            		
                System.out.println("tableName:::" + tableName);

                String columnsListStr = (String) columnsList.stream().map(pk -> pk).collect(Collectors.joining(","));
                String valueParams = (String) columnsList.stream().map(pk -> "?").collect(Collectors.joining(","));

                String insertQuery = "INSERT INTO " + tableName + " ( " + columnsListStr + " ) values (" + valueParams + ")";
                preparedStatement = connection.prepareStatement(insertQuery);;
                for (int i = 0; i < totalData.size(); i++) {

                    JSONObject rowdata = (JSONObject) totalData.get(i);
                    rowdata.remove("ROWNUM");
                    rowdata.remove("uid");
                    rowdata.remove("boundindex");
                    rowdata.remove("visibleindex");
                    rowdata.remove("uniqueid");
                    rowdata.remove(tableName + "_HIDDEN");
                    for (int j = 0; j < columnsList.size(); j++) {
						Object value = rowdata.get(columnsList.get(j));// 050123 add sql.Date while insert
						String columnName = String.valueOf(columnsList.get(j));
						String columnType = columnDataType.get(columnName).toString();
						value = setPreparedStatementObject(columnName, columnType, value, connection);

						System.out.println(value);
						preparedStatement.setObject(j + 1, (value != null) ? value : "");
					}

                    preparedStatement.addBatch();
                }
                int[] countArray = preparedStatement.executeBatch();
                try {
               	 connection.commit();
               } catch (Exception e) {}
                message = String.valueOf(new PilogUtilities().convertIntoMultilingualValue(labelObj, countArray.length + " row(s) Inserted"));
            }
            }
//            System.out.println("columnsArray::::" + sourceColumnsList);
        } catch (Exception e) {
            message = String.valueOf(new PilogUtilities().convertIntoMultilingualValue(labelObj, "error Inserting row(s)" + e.getMessage()));
            e.printStackTrace();
        } finally {
            try {

                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
            }
        }
        return message;
    }

    @Transactional
    public List getListOfDataTypes(HttpServletRequest request, String sysType) {
        List datatypesList = new ArrayList();
        Map<String, Object> map = new HashMap<>();
        try {
            String query = "SELECT DATA_TYPE FROM B_DM_DATA_TYPES WHERE  SYS_TYPE=:SYS_TYPE ORDER BY SEQUENCE_NO";

            map.put("SYS_TYPE", sysType);

            System.out.println(" query ::: " + query);
            datatypesList = access.sqlqueryWithParams(query, map);
//            System.out.println("columnsArray::::" + sourceColumnsList);
        } catch (Exception e) {

            e.printStackTrace();
        } finally {

        }
        return datatypesList;
    }

    @Transactional
    public Boolean executeAlterSQLQuery(HttpServletRequest request, Connection connection, String query) throws SQLException {
        Boolean tableAltered = null;
        PreparedStatement preparedStatement = null;
        try {

            System.out.println(" query ::: " + query);
            preparedStatement = connection.prepareStatement(query);

            tableAltered = preparedStatement.execute();

        }  finally {
            try {

                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                
            } catch (Exception e) {
            }
        }
        return tableAltered;
    }
    
    @Transactional
	public String deleteRecordsInTableWithPK(HttpServletRequest request, Connection connection, String tableName,
			List<String> tablePKColumns, JSONArray columnsList, JSONArray totalData) {
		PreparedStatement preparedStatement = null;
		String message = "";

		int totalDeleteCount = 0;

		JSONObject labelObj = new PilogUtilities().getMultilingualObject(request);
		try {
			JSONObject columnDataType = getTablesColumnDataTypes(tableName, connection);
			JSONObject totalDataObj = new JSONObject();
			if (totalData != null && !totalData.isEmpty()) {
				for (int i = 0; i < totalData.size(); i++) {
					totalDataObj = (JSONObject) totalData.get(i);					
				}
			} else {
				System.out.println("method: deleteRecordsInTable ::: empty totalData");
			}
			if (tableName != null && !"".equalsIgnoreCase(tableName) && !"null".equalsIgnoreCase(tableName)) {
				System.out.println("tableName:::" + tableName);
				if (tablePKColumns != null && !tablePKColumns.isEmpty()) {
					StringBuilder deleteWhereCond = new StringBuilder();
					for (int i = 0; i < tablePKColumns.size(); i++) {
						String columnName = tablePKColumns.get(i);
						StringBuilder s = new StringBuilder(columnName);
						if (i == tablePKColumns.size() - 1) {
							s.append("=?");
						} else {
							s.append("=? AND ");
						}
						deleteWhereCond.append(s);
					}
					String deleteQuery = "DELETE FROM " + tableName + " WHERE " + deleteWhereCond;
					System.out.println("deleteQuery :: " + deleteQuery);
					preparedStatement = connection.prepareStatement(deleteQuery);
					if (columnDataType != null && !columnDataType.isEmpty()) {
						for (int j = 0; j < tablePKColumns.size(); j++) {
							String columnName = (String) tablePKColumns.get(j);
							Object value = totalDataObj.get(columnName);
							preparedStatement.setObject(j + 1, value);
							String columnType = columnDataType.get(columnName).toString();
							value = setPreparedStatementObject(columnName, columnType, value, connection);
							preparedStatement.setObject(j + 1, (value != null) ? value : "");
						}
						int deleteCOunt = preparedStatement.executeUpdate();
						totalDeleteCount += deleteCOunt;
					} else {
						System.out.println("method: deleteRecordsInTable ::: empty columnDataType");
					}
				}
			} else {
				System.out.println("method:deleteRecordsInTable-> deleteRecordsInTable:: empty tablePKColumns");
			}
//                connection.commit();
			message = String.valueOf(new PilogUtilities().convertIntoMultilingualValue(labelObj,
					totalDeleteCount + " row(s) deleted sucessfully"));

//            System.out.println("columnsArray::::" + sourceColumnsList);
		} catch (Exception e) {
			message = String.valueOf(new PilogUtilities().convertIntoMultilingualValue(labelObj,
					"error deleting row(s)" + e.getMessage()));
			e.printStackTrace();
		} finally {
			try {

				if (preparedStatement != null) {
					preparedStatement.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
			}
		}
		return message;
	}
    
    @Transactional
    public String deleteRecordsInTableWithoutPK(HttpServletRequest request, Connection connection,
            String tableName, JSONArray columnsList, JSONArray totalData) {
        PreparedStatement preparedStatement = null;
        String message = "";

        int totalDeleteCount = 0;

        JSONObject labelObj = new PilogUtilities().getMultilingualObject(request);
        try {
        	JSONObject columnDataType = getTablesColumnDataTypes(tableName, connection);
            if (tableName != null && !"".equalsIgnoreCase(tableName) && !"null".equalsIgnoreCase(tableName)) {
                System.out.println("tableName:::" + tableName);

                for (int i = 0; i < totalData.size(); i++) {

                    JSONObject rowdata = (JSONObject) totalData.get(i);
                    rowdata.remove("uid");
                    rowdata.remove("boundindex");
                    rowdata.remove("visibleindex");
                    rowdata.remove("uniqueid");
                    rowdata.remove(tableName + "_HIDDEN");
                    //040123 remove rownum
                    rowdata.remove("ROWNUM");
                    List<String> notnullColList = (List) rowdata.keySet().stream().filter(key -> (rowdata.get(key) != null)).map(col -> col).collect(Collectors.toList());

                    String whereClauseNotNullCond = (String) rowdata.keySet().stream().filter(key -> (rowdata.get(key) != null)).map(pk -> String.valueOf(pk + "=?")).collect(Collectors.joining(" AND "));

                    String whereClauseNullCond = (String) rowdata.keySet().stream().filter(key -> (rowdata.get(key) == null)).map(pk -> String.valueOf(pk + " IS NULL ")).collect(Collectors.joining(" AND "));
                    String whereClauseCond = "";
                    if (whereClauseNotNullCond != null && !"".equalsIgnoreCase(whereClauseNotNullCond)
                            && whereClauseNullCond != null && !"".equalsIgnoreCase(whereClauseNullCond)) {
                        whereClauseCond += whereClauseNotNullCond + " AND " + whereClauseNullCond;
                    } else {
                        whereClauseCond += whereClauseNotNullCond;
                        whereClauseCond += whereClauseNullCond;
                    }

                    String deleteQuery = "DELETE FROM " + tableName + " WHERE " + whereClauseCond;
                    System.out.println("deleteQuery :: " + deleteQuery);
                    preparedStatement = connection.prepareStatement(deleteQuery);
                    for (int j = 0; j < notnullColList.size(); j++) {
                        String columnName = (String) notnullColList.get(j);
                        Object value = rowdata.get(columnName);	                        
						String columnType = columnDataType.get(columnName).toString();
						value = setPreparedStatementObject(columnName, columnType, value, connection);
						System.out.println(value);
						preparedStatement.setObject(j + 1, (value != null) ? value : "");
                    }
                    int deleteCOunt = preparedStatement.executeUpdate();
                    totalDeleteCount += deleteCOunt;
                }

//040123	                connection.commit();
                message = String.valueOf(new PilogUtilities().convertIntoMultilingualValue(labelObj, totalDeleteCount + " row(s) deleted sucessfully"));

            }
//            System.out.println("columnsArray::::" + sourceColumnsList);
        } catch (Exception e) {
            message = String.valueOf(new PilogUtilities().convertIntoMultilingualValue(labelObj, "error deleting row(s)" + e.getMessage()));
            e.printStackTrace();
        } finally {
            try {

                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
            }
        }
        return message;
    }
    
    public String insertDocumentsToMongoCollection(MongoClient client, String databaseName, String collectionName, JSONArray totalData) {
        try {
            // Get the collection from the client using the collection name
            MongoCollection<Document> collection = client.getDatabase(databaseName).getCollection(collectionName);
            
            List<Document> documents = new ArrayList<>();

            // Loop through the JSONArray of data
            for (int i = 0; i < totalData.size(); i++) {
                JSONObject rowdata = (JSONObject) totalData.get(i);
                // Remove any unnecessary fields from the rowdata
                rowdata.remove("ROWNUM");
                rowdata.remove("uid");
                rowdata.remove("boundindex");
                rowdata.remove("visibleindex");
                rowdata.remove("uniqueid");
                rowdata.remove(collectionName+"_HIDDEN");
                // Handle other specific field removals if needed

                // Create a Document to represent the data to be inserted
                Document document = new Document();
                for (Object key : rowdata.keySet()) {
                    // Set each key-value pair in the Document
                    Object value = rowdata.get(key);
                    document.append((String) key, value);
                }
                // Add the document to the list
                documents.add(document);
            }

            // Insert the list of documents into the MongoDB collection
            collection.insertMany(documents);

            // Return success message
            return "Successfully inserted " + documents.size() + " document(s) into collection " + collectionName;
        } catch (Exception e) {
            // Handle any exceptions
            e.printStackTrace();
            return "Error inserting documents into collection: " + e.getMessage();
        } finally {
            // Close the MongoDB client in the finally block
            if (client != null) {
                client.close();
            }
        }
    }
    @Transactional
    public String dropTable(HttpServletRequest request, Connection connection, String tableName) {
        PreparedStatement preparedStatement = null;
        String message = "";
        JSONObject labelObj = new PilogUtilities().getMultilingualObject(request);
        try {

            if (tableName != null && !"".equalsIgnoreCase(tableName) && !"null".equalsIgnoreCase(tableName)) {
                System.out.println("tableName:::" + tableName);
                String dropQuery = "DROP TABLE " + tableName;
                preparedStatement = connection.prepareStatement(dropQuery);
                boolean tabledrop = preparedStatement.execute();
                if(tabledrop){
                    message = String.valueOf(new PilogUtilities().convertIntoMultilingualValue(labelObj, "Table Dropped sucessfully"));
                }

            }
//            System.out.println("columnsArray::::" + sourceColumnsList);
        } catch (Exception e) {
            message = String.valueOf(new PilogUtilities().convertIntoMultilingualValue(labelObj, "error dropping table"));
            e.printStackTrace();
        } finally {
            try {

                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
            }
        }
        return message;
    }
    
}
