/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.service;


import com.pilog.mdm.DAO.IVisionTransformProcessJobComponentsDAO;
import com.pilog.mdm.transformcomputilities.JCOUtills;
import com.sap.mw.jco.IRepository;
import com.sap.mw.jco.JCO;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang.ArrayUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

/**
 *
 * @author Ravindar.P
 */

@Service
public class IVisionTransformERPProcessJobService {

	@Autowired
	private IVisionTransformComponentUtilities componentUtilities;
	private IRepository theRepository;

	@Autowired
	IVisionTransformProcessJobComponentsDAO processJobComponentsDAO;

	@Autowired
	IVisionTransformERPProcessJobService erpProcessJobService;

	@Autowired
	IVisionTransformFileProcessJobService fileProcessJobService;

	public int insertIntoSapTable(HttpServletRequest request, String destTableName, List totalData,
			JCO.Client toJCOConnection, List<String> columnsList, List<String> dataTypesList, String jobId) {
		int insertCount = 0;
		String loginUserName = (String) request.getParameter("ssUsername");
		String loginOrgnId = (String) request.getParameter("ssOrgId");
//        String jobId = (String) request.getParameter("jobId");
		try {

			JCOUtills jCOUtills = new JCOUtills();
			if (totalData != null && !totalData.isEmpty()) {

				String functionName = "/PILOG/BAPI_DATA_INSERT";
				if (destTableName != null && !"".equalsIgnoreCase(destTableName)
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
								Object[] rowData = (Object[]) totalData.get(i);

								int stmtIndex = 1;
								if (rowData != null && rowData.length > 0) {
									String dataStr = "";

									for (int j = 0; j < columnsList.size(); j++) {
										String sourceColumnName = columnsList.get(j);
										try {
											if (rowData[columnsList.indexOf(sourceColumnName)] != null
													&& !"".equalsIgnoreCase(String
															.valueOf(rowData[columnsList.indexOf(sourceColumnName)]))
													&& !"null".equalsIgnoreCase(String
															.valueOf(rowData[columnsList.indexOf(sourceColumnName)]))) {
												String dataType = dataTypesList.get(j);
												if (dataType != null && "DATE".equalsIgnoreCase(dataType)) {
													Object value = rowData[columnsList.indexOf(sourceColumnName)];

													dataStr += "" + rowData[columnsList.indexOf(sourceColumnName)];
												} else {
													dataStr += "" + rowData[columnsList.indexOf(sourceColumnName)];
												}

											} else {
												dataStr += "";
											}

										} catch (Exception e) {
											dataStr += "";
//                                                     
											continue;
										}
										if (j != columnsList.size() - 1) {
											dataStr += "|";
										}

									}

									if (dataStr != null && !"".equalsIgnoreCase(dataStr)) {
										tableData.appendRow();
										tableData.setValue(dataStr, "FIELD_VAL");
									}

								}

							} // end loop
							function.getTableParameterList().setValue(tableData, "INPTABVAL");
							toJCOConnection.execute(function);

						} else {
							insertCount = 0;
							componentUtilities.processETLLog(loginUserName, loginOrgnId,
									"Failed to Connect BAPI (" + functionName + ") ", "ERROR", 20, "Y", jobId);
							System.err.println("Function(" + functionName + ") Not exist.");
							request.setAttribute(jobId, "Fail");
							Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");

						}

					} else {
						insertCount = 0;
						componentUtilities.processETLLog(loginUserName, loginOrgnId,
								"Failed to Connect BAPI (" + functionName + ") ", "ERROR", 20, "Y", jobId);
						System.err.println("Function(" + functionName + ") Not exist.");
						request.setAttribute(jobId, "Fail");
						Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
					}

				}
			}

		} catch (Exception e) {
			insertCount = 0;
			e.printStackTrace();
			try {
				componentUtilities.processETLLog(loginUserName, loginOrgnId,
						"Faild to load the data into target table due to " + e.getMessage(), "Error", 20, "Y", jobId);
				Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
			} catch (Exception ex) {
			}
			request.setAttribute(jobId, "Fail");

		}
		return insertCount;
	}
	
	public List getErpSelectedColumnsDataHib(
			// JSONObject tableColsObj,
			Map tableColsObj, // ravi etl integration
			JSONObject tablesObj, JCO.Client connection, JSONObject sapConnObj, int start, int limit) {
//        retrieveRepository(connection);
		JCO.Table theConnection = null;
		JCO.Table tableData = null;
		JCO.Table fields = null;
		List totalErpData = new ArrayList();
		List<Object[]> sourceColumnsList = new ArrayList<>();
		JCO.Function function = null;
		String whereCond = "";
		JCO.Client sapConnection = null;
		try {
//            System.out.println("Current Thread :: " + Thread.currentThread().getId());
			try {
				sapConnection = (JCO.Client) componentUtilities.getConnection(sapConnObj);
				sapConnection = (JCO.Client) getSAPConnection(String.valueOf(sapConnObj.get("CONN_PORT")),
						String.valueOf(sapConnObj.get("CONN_USER_NAME")),
						String.valueOf(sapConnObj.get("CONN_PASSWORD")), "EN",
						String.valueOf(sapConnObj.get("HOST_NAME")), String.valueOf(sapConnObj.get("CONN_DB_NAME")));
			} catch (Exception e) {
				e.printStackTrace();
			}

			if (tableColsObj != null && !tableColsObj.isEmpty()) {
				for (Object tableName : tableColsObj.keySet()) {
					if (tableName != null && tableColsObj.get(tableName) != null) {
						String colsObj = (String) tableColsObj.get(tableName);
						if (colsObj != null && !colsObj.isEmpty()) {
							List<String> columnsList = new ArrayList(Arrays.asList(colsObj.split(",")));

							if (columnsList.size() > 5) { // ravi etl integration 50 -> 25
								List<Object[]> totalDataList = new ArrayList();
								for (int k = 0; k < columnsList.size(); k += 5) {

									function = getFunction("RFC_READ_TABLE"); // ravi updated code changes
									int endIndex = k + 5 < columnsList.size() ? k + 5 : columnsList.size();

//                                    System.out.println("iteration:: " + k + " Time" + System.currentTimeMillis());
									List colsSubList = columnsList.subList(k, endIndex);
									if (colsSubList != null && !colsSubList.isEmpty()) {
										if (function != null) {
											JCO.ParameterList listParams = function.getImportParameterList();
											fields = function.getTableParameterList().getTable("FIELDS");
											tableData = function.getTableParameterList().getTable("DATA");
											JCO.Table Option = function.getTableParameterList().getTable("OPTIONS");
											for (int i = 0; i < colsSubList.size(); i++) {
												// String columnName = (String) columnsList.get(i);
												String columnName = (String) colsSubList.get(i); // ravi etl integration
												fields.appendRow();
												fields.setValue(columnName, "FIELDNAME");
											}
											listParams.setValue(tableName, "QUERY_TABLE");// For MARA
											listParams.setValue("♥", "DELIMITER");
//                                            listParams.setValue((start == 0 ? (start) : (start - 1)), "ROWSKIPS");
//                                            listParams.setValue((limit + 1), "ROWCOUNT");
											listParams.setValue((start == 0 ? (start) : (start)), "ROWSKIPS"); // ravi
																												// etl
																												// integration
											listParams.setValue((limit), "ROWCOUNT"); // ravi etl integration
											String WhereCondition = (String) tablesObj.get(tableName);
											if (WhereCondition != null && !WhereCondition.isEmpty()) {
												WhereCondition = WhereCondition.replaceAll(tableName + "\\.", ""); // ravi
																													// etl
																													// integration
												Option.appendRow();
												Option.setValue(WhereCondition, "TEXT");
											}
											long startTime = System.currentTimeMillis();
											try {
												sapConnection.execute(function);
											} catch (Exception e) {
												System.out.println("SAP error :: " + e.getMessage());
												sapConnection = (JCO.Client) componentUtilities
														.getConnection(sapConnObj);
												sapConnection.execute(function);
											}
											long endTime = System.currentTimeMillis();
											System.out.println("SAP executionTime :: " + (endTime - startTime) / 1000);
											System.out.println("Before gc Free Memory (in bytes): "
													+ Runtime.getRuntime().freeMemory());
											System.gc();
											System.out.println("After gc  Free Memory (in bytes): "
													+ Runtime.getRuntime().freeMemory());

											int fetchCount = tableData.getNumRows();

											if (totalDataList != null && !totalDataList.isEmpty()) {
												int index = 0;
												if (fetchCount != 0) {
													// Map dataObj = (Map) totalDataList.get(index);
													// Map dataObj = (Map) totalDataList.get(index);
													Object[] dataObj = new Object[colsSubList.size()];
													if (colsSubList != null && !colsSubList.isEmpty()) {
														String[] FieldValues = tableData.getString("WA").split("\\s+♥",
																-1);
														for (int i = 0; i < colsSubList.size(); i++) {

															String value = FieldValues[i];
															if (value != null && !"".equalsIgnoreCase(value)
																	&& !"null".equalsIgnoreCase(value)) {
																value = value.trim();
															}
															// dataObj.put(tableName + ":" + columnsList.get(i),
															// FieldValues[i]);
//                                                            //dataObj.put(tableName + ":" + colsSubList.get(i), FieldValues[i]);
//                                                            dataObj.put(tableName + ":" + colsSubList.get(i), value); // ravi etl integration
															dataObj[i] = value; // ravi etl integration
														}

														Object[] newDataObj = ArrayUtils
																.addAll(totalDataList.get(index), dataObj);
														totalDataList.remove(index);
														totalDataList.add(index, newDataObj);
													}
													index++;
												}
												while (tableData.nextRow()) {

//                                                    Map dataObj = (Map) totalDataList.get(index);
													Object[] dataObj = new Object[colsSubList.size()];
													if (colsSubList != null && !colsSubList.isEmpty()) {
														String[] FieldValues = tableData.getString("WA").split("\\s+♥",
																-1);
														for (int i = 0; i < colsSubList.size(); i++) {

															String value = FieldValues[i];
															if (value != null && !"".equalsIgnoreCase(value)
																	&& !"null".equalsIgnoreCase(value)) {
																value = value.trim();
															}
															dataObj[i] = value; // ravi etl integration
														}

														Object[] newDataObj = ArrayUtils
																.addAll(totalDataList.get(index), dataObj);
														totalDataList.remove(index);
														totalDataList.add(index, newDataObj);
													}
													index++;
												}
											} else {
												if (fetchCount != 0) {
													// Map dataObj = new HashMap();
													Object[] dataObj = new Object[colsSubList.size()];
													if (colsSubList != null && !colsSubList.isEmpty()) {
														for (int i = 0; i < colsSubList.size(); i++) {
															String[] FieldValues = tableData.getString("WA")
																	.split("\\s+♥", -1);
															// dataObj.put(tableName + ":" + colsSubList.get(i),
															// FieldValues[i]);
															String value = FieldValues[i];
															if (value != null && !"".equalsIgnoreCase(value)
																	&& !"null".equalsIgnoreCase(value)) {
																value = value.trim();
															}
															dataObj[i] = value; // ravi etl integration
														}
														totalDataList.add(dataObj);
													}
												}
												while (tableData.nextRow()) {
													// Map dataObj = new HashMap();
													Object[] dataObj = new Object[colsSubList.size()];
													if (colsSubList != null && !colsSubList.isEmpty()) {
														for (int i = 0; i < colsSubList.size(); i++) {
															String[] FieldValues = tableData.getString("WA")
																	.split("\\s+♥", -1);
															// dataObj.put(tableName + ":" + colsSubList.get(i),
															// FieldValues[i]);
															String value = FieldValues[i];
															if (value != null && !"".equalsIgnoreCase(value)
																	&& !"null".equalsIgnoreCase(value)) {
																value = value.trim();
															}
															// dataObj.put(tableName + ":" + colsSubList.get(i), value);
															// // ravi etl integration
															dataObj[i] = value; // ravi etl integration
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
									listParams.setValue(tableName, "QUERY_TABLE");// For MARA
									listParams.setValue("♥", "DELIMITER");
//                                    listParams.setValue((start == 0 ? (start) : (start - 1)), "ROWSKIPS");
//                                    listParams.setValue((limit + 1), "ROWCOUNT");
									listParams.setValue((start == 0 ? (start) : (start)), "ROWSKIPS"); // ravi etl
																										// integration
									listParams.setValue((limit), "ROWCOUNT"); // ravi etl integration

									String WhereCondition = (String) tablesObj.get(tableName);
									if (WhereCondition != null && !WhereCondition.isEmpty()) {
										WhereCondition = WhereCondition.replaceAll(tableName + "\\.", ""); // ravi etl
																											// integration
										Option.appendRow();
										Option.setValue(WhereCondition, "TEXT");
									}
									long startTime = System.currentTimeMillis();
									try {
										sapConnection.execute(function);
									} catch (Exception e) {
										System.out.println("SAP error :: " + e.getMessage());
										sapConnection = (JCO.Client) componentUtilities.getConnection(sapConnObj);
										sapConnection.execute(function);
									}
									long endTime = System.currentTimeMillis();
									System.out.println("SAP executionTime :: " + (endTime - startTime) / 1000);
									System.out.println(
											"Before gc Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
									System.gc();
									System.out.println(
											"After gc  Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
									int fetchCount = tableData.getNumRows();

									if (fetchCount != 0) {
//                                        Map dataObj = new HashMap();
										Object[] dataObj = new Object[columnsList.size()];
										if (columnsList != null && !columnsList.isEmpty()) {
											for (int i = 0; i < columnsList.size(); i++) {
												String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
												String value = FieldValues[i];
												if (value != null && !"".equalsIgnoreCase(value)
														&& !"null".equalsIgnoreCase(value)) {
													value = value.trim();
												}
												dataObj[i] = value;
											}
											totalErpData.add(dataObj);
										}
									}
									while (tableData.nextRow()) {
//                                        Map dataObj = new HashMap();
										Object[] dataObj = new Object[columnsList.size()];
										if (columnsList != null && !columnsList.isEmpty()) {
											for (int i = 0; i < columnsList.size(); i++) {
												String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
												String value = FieldValues[i];
												if (value != null && !"".equalsIgnoreCase(value)
														&& !"null".equalsIgnoreCase(value)) {
													value = value.trim();
												}
//                                                dataObj.put(tableName + ":" + columnsList.get(i), value);
												dataObj[i] = value;
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
		} finally {

			if (connection != null) {
//                connection.reset();
//                connection.disconnect();
//                JCO.releaseClient(connection);
			}
//            System.out.println("Before gc Total Memory (in bytes): " + Runtime.getRuntime().totalMemory());
			System.out.println("Before gc Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
//          System.out.println("Before gc Max Memory (in bytes): " + Runtime.getRuntime().maxMemory());
			System.gc();
//            System.out.println("After gc Total Memory (in bytes): " + Runtime.getRuntime().totalMemory());
			System.out.println("After gc  Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
//          System.out.println("After gc  Max Memory (in bytes): " + Runtime.getRuntime().maxMemory());
		}
		return totalErpData;
	}

//    public List insertSAPDataToDBTableOld(HttpServletRequest request,
//            String tableName,
//            List columnsList,
//            JCO.Client sapConnection,
//            Connection connection,
//            PreparedStatement preparedStatement,
//            String toTableName,
//            List toDataTypesList,
//            int totalRowCount,
//            String WhereCondition,
//            int start,
//            int limit,
//            int end,
//            String jobId) {
////            //JSONObject tableColsObj,
////            List columnsList,// ravi etl integration
////            JCO.Client connection,
////            JSONObject sapConnObj,
////            int start,
////            int limit) {
//        retrieveRepository(sapConnection);
//        JCO.Table tableData = null;
//        JCO.Table fields = null;
//        List totalErpData = new ArrayList();
//        JCO.Function function = null;
//        String whereCond = "";
//        List rowIdList = new ArrayList();
//        try {
//            if (columnsList.size() > 5) { // ravi etl integration 50 -> 25
//                for (int k = 0; k < columnsList.size(); k += 5) {
//                    List<Object[]> totalDataList = new ArrayList();
//                    function = getFunction("RFC_READ_TABLE"); // ravi updated code changes
//                    int endIndex = k + 5 < columnsList.size() ? k + 5 : columnsList.size();
//
////                  System.out.println("iteration:: " + k + " Time" + System.currentTimeMillis());
//                    List colsSubList = columnsList.subList(k, endIndex);
//                    List dataTypesSubList = toDataTypesList.subList(k, endIndex);
//                    if (colsSubList != null && !colsSubList.isEmpty()) {
//                        if (function != null) {
//                            JCO.ParameterList listParams = function.getImportParameterList();
//                            fields = function.getTableParameterList().getTable("FIELDS");
//                            tableData = function.getTableParameterList().getTable("DATA");
//                            JCO.Table Option = function.getTableParameterList().getTable("OPTIONS");
//                            for (int i = 0; i < colsSubList.size(); i++) {
//                                //String columnName = (String) columnsList.get(i);
//                                String columnName = (String) colsSubList.get(i); // ravi etl integration
//                                fields.appendRow();
//                                fields.setValue(columnName, "FIELDNAME");
//                            }
//                            listParams.setValue(tableName, "QUERY_TABLE");//For MARA
//                            listParams.setValue("♥", "DELIMITER");
////                                            listParams.setValue((start == 0 ? (start) : (start - 1)), "ROWSKIPS");
////                                            listParams.setValue((limit + 1), "ROWCOUNT");
//                            if (limit > 0) {
//                                listParams.setValue((start == 0 ? (start) : (start)), "ROWSKIPS"); // ravi etl integration
//                                listParams.setValue((limit), "ROWCOUNT"); // ravi etl integration
//                            }
////                            String WhereCondition = (String) tablesObj.get(tableName);
//                            if (WhereCondition != null && !"".equalsIgnoreCase(WhereCondition)) {
////                                WhereCondition = WhereCondition.replaceAll(tableName + "\\.", ""); // ravi etl integration
//                                Option.appendRow();
//                                Option.setValue(WhereCondition, "TEXT");
//                            }
//                            long startTime = System.currentTimeMillis();
//                            sapConnection.execute(function);
//                            long endTime = System.currentTimeMillis();
//                            System.out.println("SAP executionTime :: " + (endTime - startTime) / 1000);
////                            System.out.println("Before gc Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
////                            System.gc();
////                            System.out.println("After gc  Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
//
//                            int fetchCount = tableData.getNumRows();
//
//                            if (fetchCount != 0) {
//                                // Map dataObj = new HashMap();
//                                Object[] dataObj = new Object[colsSubList.size()];
//                                if (colsSubList != null && !colsSubList.isEmpty()) {
//                                    for (int i = 0; i < colsSubList.size(); i++) {
//                                        String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
//                                        //dataObj.put(tableName + ":" + colsSubList.get(i), FieldValues[i]);
//                                        String value = FieldValues[i];
//                                        if (value != null
//                                                && !"".equalsIgnoreCase(value)
//                                                && !"null".equalsIgnoreCase(value)) {
//                                            value = value.trim();
//                                        }
//                                        dataObj[i] = value; // ravi etl integration
//                                    }
//                                    totalDataList.add(dataObj);
//                                }
//                            }
//                            while (tableData.nextRow()) {
//                                //Map dataObj = new HashMap();
//                                Object[] dataObj = new Object[colsSubList.size()];
//                                if (colsSubList != null && !colsSubList.isEmpty()) {
//                                    for (int i = 0; i < colsSubList.size(); i++) {
//                                        String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
//                                        //dataObj.put(tableName + ":" + colsSubList.get(i), FieldValues[i]);
//                                        String value = FieldValues[i];
//                                        if (value != null
//                                                && !"".equalsIgnoreCase(value)
//                                                && !"null".equalsIgnoreCase(value)) {
//                                            value = value.trim();
//                                        }
//                                        //dataObj.put(tableName + ":" + colsSubList.get(i), value); // ravi etl integration
//                                        dataObj[i] = value; // ravi etl integration
//                                    }
//                                    totalDataList.add(dataObj);
//                                }
//                            }
//
//                        }
//                    }
//                    if (k == 0) {
//                        String insertQuery = generateInsertQuery(toTableName, colsSubList);
//
//                        preparedStatement = connection.prepareStatement(insertQuery);
//                        int insertcount = processJobComponentsDAO.insertDataIntoTable(request, toTableName,
//                                preparedStatement, colsSubList, totalDataList, colsSubList, dataTypesSubList, "SAP_HANA", jobId);
//                        String query = "SELECT ROWID FROM " + toTableName;
//                        preparedStatement = connection.prepareStatement(query);
//                        ResultSet resultSet = preparedStatement.executeQuery();
//                        while (resultSet.next()) {
//                            rowIdList.add(resultSet.getString("ROWID"));
//                        }
//                        System.out.println("rowIdList ::");
//                    } else {
//                        String colsSubListStr = (String) colsSubList.stream().map(e -> e + "=?").collect(Collectors.joining(", "));
//                        String updateQuery = "UPDATE " + toTableName + " SET " + colsSubListStr + " WHERE ROWID =?";
//                        preparedStatement = connection.prepareStatement(updateQuery);
//                        int updatecount = processJobComponentsDAO.updateColumnswithRowNum(request, tableName, preparedStatement,
//                                colsSubList, totalDataList, rowIdList, dataTypesSubList, "SAP_HANA", jobId);
//                    }
//
//                }
//
//            } else {
//                List<Object[]> totalDataList = new ArrayList();
//                function = getFunction("RFC_READ_TABLE");
//                if (function != null) {
//                    JCO.ParameterList listParams = function.getImportParameterList();
//                    fields = function.getTableParameterList().getTable("FIELDS");
//                    tableData = function.getTableParameterList().getTable("DATA");
//                    JCO.Table Option = function.getTableParameterList().getTable("OPTIONS");
//                    for (int i = 0; i < columnsList.size(); i++) {
//                        String columnName = (String) columnsList.get(i);
//                        fields.appendRow();
//                        fields.setValue(columnName, "FIELDNAME");
//                    }
//                    listParams.setValue(tableName, "QUERY_TABLE");//For MARA
//                    listParams.setValue("♥", "DELIMITER");
////                                    listParams.setValue((start == 0 ? (start) : (start - 1)), "ROWSKIPS");
////                                    listParams.setValue((limit + 1), "ROWCOUNT");
//                    if (limit > 0) {
//                        listParams.setValue((start == 0 ? (start) : (start)), "ROWSKIPS"); // ravi etl integration
//                        listParams.setValue((limit), "ROWCOUNT"); // ravi etl integration
//                    }
//
////                    String WhereCondition = (String) tablesObj.get(tableName);
//                    if (WhereCondition != null && !WhereCondition.isEmpty()) {
////                        WhereCondition = WhereCondition.replaceAll(tableName + "\\.", ""); // ravi etl integration
//                        Option.appendRow();
//                        Option.setValue(WhereCondition, "TEXT");
//                    }
//                    long startTime = System.currentTimeMillis();
//                    sapConnection.execute(function);
//                    long endTime = System.currentTimeMillis();
//                    System.out.println("SAP executionTime :: " + (endTime - startTime) / 1000);
//                    System.out.println("Before gc Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
//                    System.gc();
//                    System.out.println("After gc  Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
//                    int fetchCount = tableData.getNumRows();
//
//                    if (fetchCount != 0) {
////                                        Map dataObj = new HashMap();
//                        Object[] dataObj = new Object[columnsList.size()];
//                        if (columnsList != null && !columnsList.isEmpty()) {
//                            for (int i = 0; i < columnsList.size(); i++) {
//                                String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
//                                String value = FieldValues[i];
//                                if (value != null
//                                        && !"".equalsIgnoreCase(value)
//                                        && !"null".equalsIgnoreCase(value)) {
//                                    value = value.trim();
//                                }
//                                dataObj[i] = value;
//                            }
//                            totalDataList.add(dataObj);
//                        }
//                    }
//                    while (tableData.nextRow()) {
////                                        Map dataObj = new HashMap();
//                        Object[] dataObj = new Object[columnsList.size()];
//                        if (columnsList != null && !columnsList.isEmpty()) {
//                            for (int i = 0; i < columnsList.size(); i++) {
//                                String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
//                                String value = FieldValues[i];
//                                if (value != null
//                                        && !"".equalsIgnoreCase(value)
//                                        && !"null".equalsIgnoreCase(value)) {
//                                    value = value.trim();
//                                }
////                                                dataObj.put(tableName + ":" + columnsList.get(i), value);
//                                dataObj[i] = value;
//                            }
//                            totalDataList.add(dataObj);
//                        }
//                    }
//                }
//
//                int insertcount = processJobComponentsDAO.insertDataIntoTable(request, toTableName, preparedStatement,
//                        columnsList, totalDataList, columnsList, toDataTypesList, "SAP_HANA", jobId);
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//
//            if (connection != null) {
////                connection.reset();
////                connection.disconnect();
////                JCO.releaseClient(connection);
//            }
////            System.out.println("Before gc Total Memory (in bytes): " + Runtime.getRuntime().totalMemory());
//            System.out.println("Before gc Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
////          System.out.println("Before gc Max Memory (in bytes): " + Runtime.getRuntime().maxMemory());
//            System.gc();
////            System.out.println("After gc Total Memory (in bytes): " + Runtime.getRuntime().totalMemory());
//            System.out.println("After gc  Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
////          System.out.println("After gc  Max Memory (in bytes): " + Runtime.getRuntime().maxMemory());
//        }
//        return totalErpData;
//    }
	public int getSapTableRowCount(HttpServletRequest request, JCO.Client sapConnection, List fromColumnsList,
			String fromTable, // ravi etl integration
			String jobId) {
		int totalCount = 0;

		try {

			retrieveRepository(sapConnection);

			JCO.Function function = getFunction("RFC_READ_TABLE");
			JCO.ParameterList listParams = function.getImportParameterList();
			JCO.Table fields = function.getTableParameterList().getTable("FIELDS");
			JCO.Table tableData = function.getTableParameterList().getTable("DATA");
			String columnName = (String) fromColumnsList.get(0); // ravi etl integration
			fields.appendRow();
			fields.setValue(columnName, "FIELDNAME");
//            String tableName = (String) tableColsObj.keySet().iterator().next();
			listParams.setValue(fromTable, "QUERY_TABLE");// For MARA
			listParams.setValue("♥", "DELIMITER");
			sapConnection.execute(function);
			totalCount = tableData.getNumRows();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (sapConnection != null) {
//                sapConnection.reset();
//                sapConnection.disconnect();
//                JCO.releaseClient(sapConnection);
			}
		}

		return totalCount;
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

	public Object getSAPConnection(String ClientId, String userName, String password, String LanguageId,
			String hostName, String ERPSystemId) {
		JCO.Client theConnection = null;
		Object returnObj = null;
		try {
			theConnection = JCO.createClient(ClientId, userName, password, LanguageId, hostName, ERPSystemId);
			theConnection.connect();
			returnObj = theConnection;
			System.out.println(" Succesfully connect to SAP system");
			// System.out.println("connection attribute:"+ theConnection.getAttributes());
		} catch (Exception ex) {
			ex.printStackTrace();
			returnObj = ex.getMessage();
			System.out.println("Failed to connect to SAP system");
		}

		return returnObj;

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
					listParams.setValue(tablename, "TABNAME");// For MARA
					fields = function.getTableParameterList().getTable("DFIES_TAB");
					connection.execute(function);
					int fetchCount = fields.getNumRows();
					if (fetchCount != 0) {
						String feildName = fields.getString("FIELDNAME");
						if (feildName != null && !"".equalsIgnoreCase(feildName) && !"null".equalsIgnoreCase(feildName) // &&
																														// !feildName.contains("/")
						) {
//                            if (feildName.startsWith("/")) {
//                                feildName = feildName.substring(1);
//                            }
//                            if (feildName.contains("/")) {
//                                feildName = feildName.replaceAll("/", "_");
//                            }
							Object[] columnsArray = new Object[10];
							columnsArray[0] = (fields.getString("POSITION"));
							columnsArray[1] = (listParams.getString("TABNAME") != null
									? listParams.getString("TABNAME").toUpperCase()
									: "");
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
						if (feildName != null && !"".equalsIgnoreCase(feildName) && !"null".equalsIgnoreCase(feildName) // &&
																														// !feildName.contains("/")
						) {
//                            if (feildName.startsWith("/")) {
//                                feildName = feildName.substring(1);
//                            }
//                            if (feildName.contains("/")) {
//                                feildName = feildName.replaceAll("/", "_");
//                            }
							Object[] columnsArray = new Object[10];
							columnsArray[0] = (fields.getString("POSITION"));
							columnsArray[1] = (listParams.getString("TABNAME") != null
									? listParams.getString("TABNAME").toUpperCase()
									: "");
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

	public List insertSAPDataToDBTable(HttpServletRequest request, String tableName, List columnsList,
			JCO.Client sapConnection, JSONObject connObj, PreparedStatement preparedStatement, String toTableName,
			List toDataTypesList, int totalRowCount,
			// String WhereCondition,
			JSONArray whereClauseObjArray, int start, int limit, int end, String jobId) {
//            //JSONObject tableColsObj,
//            List columnsList,// ravi etl integration
//            JCO.Client connection,
//            JSONObject sapConnObj,
//            int start,
//            int limit) {
		retrieveRepository(sapConnection);

		JCO.Table tableData = null;
		JCO.Table fields = null;
		List totalErpData = new ArrayList();
		JCO.Function function = null;
		String whereCond = "";
		List rowIdList = new ArrayList();
		int totalDataCount = 0;
		JSONObject infoObject = new JSONObject();
		try {
			if (limit > 0) {
				boolean loop = true;
				int maxLimit = 10000;
				if (limit <= maxLimit) {
					maxLimit = limit;
				}
				while (loop) {
					if (end > 0 && ((totalDataCount + limit) >= end)) {
						limit = (end - start);
						loop = false;
					}
					List<Object[]> totalDataList = new ArrayList();
//                  function = getFunction("RFC_READ_TABLE");
					function = getFunction("/PILOG/BULK_READ_TABLE"); // ravi updated code changes
//                    function = getFunction("/PILOG/BAPI_REVERSE_ETL"); // ravi updated code changes
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
						listParams.setValue(tableName, "QUERY_TABLE");// For MARA
						listParams.setValue("♥", "DELIMITER");
//                                    listParams.setValue((start == 0 ? (start) : (start - 1)), "ROWSKIPS");
//                                    listParams.setValue((limit + 1), "ROWCOUNT");
						if (maxLimit > 0) {
							listParams.setValue((start == 0 ? (start) : (start)), "ROWSKIPS"); // ravi etl integration
							listParams.setValue((maxLimit), "ROWCOUNT"); // ravi etl integration
						}

//                    String WhereCondition = (String) tablesObj.get(tableName);
//                        if (WhereCondition != null && !WhereCondition.isEmpty()) {
////                        WhereCondition = WhereCondition.replaceAll(tableName + "\\.", ""); // ravi etl integration
//                            Option.appendRow();
//                            Option.setValue(WhereCondition, "TEXT");
//                        }
						if (whereClauseObjArray != null && !whereClauseObjArray.isEmpty()) {
//                            WhereCondition = (String) whereClauseObjArray.stream().map(whereClauseObject -> {
//                                String column = (String) ((JSONObject) whereClauseObject).get("column");
//                                String condOperator = (String) ((JSONObject) whereClauseObject).get("operator");
//                                String condition = "";
//                                String value = (String) ((JSONObject) whereClauseObject).get("value");
//                                if (condOperator != null && "=".equalsIgnoreCase(condOperator)) {
//                                    value = "'" + value + "'";
//                                    condition = column + " " + condOperator + " " + value + "";
//                                } else if (condOperator != null && "LIKE".equalsIgnoreCase(condOperator)) {
//                                    value = "'%" + value + "%'";
//                                    condition = column + " " + condOperator + " " + value + "";
//                                } else if (condOperator != null && "BETWEEN".equalsIgnoreCase(condOperator)) {
//                                    String fromValue = (String) ((JSONObject) whereClauseObject).get("fromValue");
//                                    String toValue = (String) ((JSONObject) whereClauseObject).get("toValue");
//
//                                    condition = column + " " + condOperator + " '" + fromValue + "' AND '" + toValue + "'";
//                                }
//                                return condition;
//                            }).collect(Collectors.joining(" AND "));

							for (int i = 0; i < whereClauseObjArray.size(); i++) {
								JSONObject whereClauseObject = (JSONObject) whereClauseObjArray.get(i);

								String column = (String) whereClauseObject.get("column");
								String condOperator = (String) whereClauseObject.get("operator");
								String condition = "";
								String value = (String) whereClauseObject.get("value");
								if (condOperator != null && "=".equalsIgnoreCase(condOperator)) {
									value = "'" + value + "'";
									condition = column + " " + condOperator + " " + value + "";
								} else if (condOperator != null && "!=".equalsIgnoreCase(condOperator)) {
									value = "'" + value + "'";
									condition = column + " <> " + value + "";
								} else if (condOperator != null && "LIKE".equalsIgnoreCase(condOperator)) {
									value = "'%" + value + "%'";
									condition = column + " " + condOperator + " " + value + "";
								} else if (condOperator != null && "NOT LIKE".equalsIgnoreCase(condOperator)) {
									value = "'%" + value + "%'";
									condition = column + " " + condOperator + " " + value + "";
								} else if (condOperator != null && "IN".equalsIgnoreCase(condOperator)) {
//                                    value = "'%" + value + "%'";
									condition = column + " " + condOperator + " " + value + "";
								} else if (condOperator != null && "NOT IN".equalsIgnoreCase(condOperator)) {
//                                    value = "'%" + value + "%'";
									condition = column + " " + condOperator + " " + value + "";
								} else if (condOperator != null && "BETWEEN".equalsIgnoreCase(condOperator)) {
									String fromValue = (String) whereClauseObject.get("value");
									String toValue = (String) whereClauseObject.get("toValue");

									condition = column + " " + condOperator + " '" + fromValue + "' AND '" + toValue
											+ "'";
								} else if (condOperator != null && "ISNULL".equalsIgnoreCase(condOperator)) {
//                                    value = "'%" + value + "%'";
									condition = column + " = SPACE ";
								} else if (condOperator != null && "ISNOTNULL".equalsIgnoreCase(condOperator)) {
//                                    value = "'%" + value + "%'";
									condition = column + " <> SPACE ";
								}
								if (i < (whereClauseObjArray.size() - 1)) {
									condition = condition + " AND ";
									Option.appendRow();
									Option.setValue(condition, "TEXT");
								} else {
									Option.appendRow();
									Option.setValue(condition, "TEXT");

								}
							}

						}
						long startTime = System.currentTimeMillis();
						sapConnection.execute(function);
						long endTime = System.currentTimeMillis();
						System.out.println("SAP executionTime :: " + (endTime - startTime) / 1000);
						System.out.println("Before gc Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
						System.gc();
						System.out.println("After gc  Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
						int fetchCount = tableData.getNumRows();

						if (fetchCount != 0) {
//                                        Map dataObj = new HashMap();
//                            Object[] dataObj = new Object[columnsList.size()];
//                            if (columnsList != null && !columnsList.isEmpty()) {
//                                for (int i = 0; i < columnsList.size(); i++) {
//                                    String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
//                                    String value = FieldValues[i];
//                                    if (value != null
//                                            && !"".equalsIgnoreCase(value)
//                                            && !"null".equalsIgnoreCase(value)) {
//                                        value = value.trim();
//                                    }
//                                    dataObj[i] = value;
//                                }
//                                totalDataList.add(dataObj);
//                            }
							String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
							totalDataList.add(FieldValues);
						}
						while (tableData.nextRow()) {
							// Map dataObj = new HashMap();
//                            Object[] dataObj = new Object[columnsList.size()];
//                            if (columnsList != null && !columnsList.isEmpty()) {
//                                for (int i = 0; i < columnsList.size(); i++) {
//                                    String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
//                                    //dataObj.put(tableName + ":" + colsSubList.get(i), FieldValues[i]);
//                                    String value = FieldValues[i];
//                                    if (value != null
//                                            && !"".equalsIgnoreCase(value)
//                                            && !"null".equalsIgnoreCase(value)) {
//                                        value = value.trim();
//                                    }
//                                    //dataObj.put(tableName + ":" + colsSubList.get(i), value); // ravi etl integration
//                                    dataObj[i] = value; // ravi etl integration
//                                }
//                                totalDataList.add(dataObj);
//                            }
							String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
							totalDataList.add(FieldValues);
						}
					}
					String insertQuery = generateInsertQuery(toTableName, columnsList);
					Connection connection = null;
					try {
						connection = (Connection) componentUtilities.getConnection(connObj);
						preparedStatement = connection.prepareStatement(insertQuery);
						
						infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName,
								preparedStatement, columnsList, totalDataList, columnsList, toDataTypesList, "SAP_HANA",
								jobId, infoObject);
						int insertcount = (int)infoObject.get("insertcount");
						totalDataCount = totalDataCount + insertcount;
					} catch (Exception e) {
						loop = false;
					} finally {
						if (connection != null) {
							connection.close();
						}
					}
					if (totalDataList != null && !totalDataList.isEmpty()) {
						start = start + limit;
						if (totalDataList.size() < limit) {
							loop = false;
						}
					}

				}
			} else {
				boolean loop = true;
				start = 0;
				limit = 10000;
				while (loop) {
					List<Object[]> totalDataList = new ArrayList();
//                  function = getFunction("RFC_READ_TABLE");
					function = getFunction("/PILOG/BULK_READ_TABLE"); // ravi updated code changes
//                    function = getFunction("/PILOG/BAPI_REVERSE_ETL"); // ravi updated code changes
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
						listParams.setValue(tableName, "QUERY_TABLE");// For MARA
						listParams.setValue("♥", "DELIMITER");
//                                    listParams.setValue((start == 0 ? (start) : (start - 1)), "ROWSKIPS");
//                                    listParams.setValue((limit + 1), "ROWCOUNT");
						if (limit > 0) {
							listParams.setValue((start == 0 ? (start) : (start)), "ROWSKIPS"); // ravi etl integration
							listParams.setValue((limit), "ROWCOUNT"); // ravi etl integration
						}

//                    String WhereCondition = (String) tablesObj.get(tableName);
//                        if (WhereCondition != null && !WhereCondition.isEmpty()) {
////                        WhereCondition = WhereCondition.replaceAll(tableName + "\\.", ""); // ravi etl integration
//                            Option.appendRow();
//                            Option.setValue(WhereCondition, "TEXT");
//                        }
						if (whereClauseObjArray != null && !whereClauseObjArray.isEmpty()) {
//                            WhereCondition = (String) whereClauseObjArray.stream().map(whereClauseObject -> {
//                                String column = (String) ((JSONObject) whereClauseObject).get("column");
//                                String condOperator = (String) ((JSONObject) whereClauseObject).get("operator");
//                                String condition = "";
//                                String value = (String) ((JSONObject) whereClauseObject).get("value");
//                                if (condOperator != null && "=".equalsIgnoreCase(condOperator)) {
//                                    value = "'" + value + "'";
//                                    condition = column + " " + condOperator + " " + value + "";
//                                } else if (condOperator != null && "LIKE".equalsIgnoreCase(condOperator)) {
//                                    value = "'%" + value + "%'";
//                                    condition = column + " " + condOperator + " " + value + "";
//                                } else if (condOperator != null && "BETWEEN".equalsIgnoreCase(condOperator)) {
//                                    String fromValue = (String) ((JSONObject) whereClauseObject).get("fromValue");
//                                    String toValue = (String) ((JSONObject) whereClauseObject).get("toValue");
//
//                                    condition = column + " " + condOperator + " '" + fromValue + "' AND '" + toValue + "'";
//                                }
//                                return condition;
//                            }).collect(Collectors.joining(" AND "));

							for (int i = 0; i < whereClauseObjArray.size(); i++) {
								JSONObject whereClauseObject = (JSONObject) whereClauseObjArray.get(i);

								String column = (String) whereClauseObject.get("column");
								String condOperator = (String) whereClauseObject.get("operator");
								String condition = "";
								String value = (String) whereClauseObject.get("value");
								if (condOperator != null && "=".equalsIgnoreCase(condOperator)) {
									value = "'" + value + "'";
									condition = column + " " + condOperator + " " + value + "";
								} else if (condOperator != null && "!=".equalsIgnoreCase(condOperator)) {
									value = "'" + value + "'";
									condition = column + " <> " + value + "";
								} else if (condOperator != null && "LIKE".equalsIgnoreCase(condOperator)) {
									value = "'%" + value + "%'";
									condition = column + " " + condOperator + " " + value + "";
								} else if (condOperator != null && "NOT LIKE".equalsIgnoreCase(condOperator)) {
									value = "'%" + value + "%'";
									condition = column + " " + condOperator + " " + value + "";
								} else if (condOperator != null && "IN".equalsIgnoreCase(condOperator)) {
//                                    value = "'%" + value + "%'";
									condition = column + " " + condOperator + " " + value + "";
								} else if (condOperator != null && "NOT IN".equalsIgnoreCase(condOperator)) {
//                                    value = "'%" + value + "%'";
									condition = column + " " + condOperator + " " + value + "";
								} else if (condOperator != null && "BETWEEN".equalsIgnoreCase(condOperator)) {
									String fromValue = (String) whereClauseObject.get("value");
									String toValue = (String) whereClauseObject.get("toValue");

									condition = column + " " + condOperator + " '" + fromValue + "' AND '" + toValue
											+ "'";
								} else if (condOperator != null && "ISNULL".equalsIgnoreCase(condOperator)) {
//                                    value = "'%" + value + "%'";
									condition = column + " = SPACE ";
								} else if (condOperator != null && "ISNOTNULL".equalsIgnoreCase(condOperator)) {
//                                    value = "'%" + value + "%'";
									condition = column + " <> SPACE ";
								}
								if (i < (whereClauseObjArray.size() - 1)) {
									condition = condition + " AND ";
									Option.appendRow();
									Option.setValue(condition, "TEXT");
								} else {
									Option.appendRow();
									Option.setValue(condition, "TEXT");

								}
							}

						}
						long startTime = System.currentTimeMillis();
						sapConnection.execute(function);
						long endTime = System.currentTimeMillis();
						System.out.println("SAP executionTime :: " + (endTime - startTime) / 1000);
						System.out.println("Before gc Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
						System.gc();
						System.out.println("After gc  Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
						int fetchCount = tableData.getNumRows();

						if (fetchCount != 0) {
//                                        Map dataObj = new HashMap();
//                            Object[] dataObj = new Object[columnsList.size()];
//                            if (columnsList != null && !columnsList.isEmpty()) {
//                                for (int i = 0; i < columnsList.size(); i++) {
//                                    String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
//                                    String value = FieldValues[i];
//                                    if (value != null
//                                            && !"".equalsIgnoreCase(value)
//                                            && !"null".equalsIgnoreCase(value)) {
//                                        value = value.trim();
//                                    }
//                                    dataObj[i] = value;
//                                }
//                                totalDataList.add(dataObj);
//                            }
							String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
							totalDataList.add(FieldValues);
						}
						while (tableData.nextRow()) {
							// Map dataObj = new HashMap();
//                            Object[] dataObj = new Object[columnsList.size()];
//                            if (columnsList != null && !columnsList.isEmpty()) {
//                                for (int i = 0; i < columnsList.size(); i++) {
//                                    String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
//                                    //dataObj.put(tableName + ":" + colsSubList.get(i), FieldValues[i]);
//                                    String value = FieldValues[i];
//                                    if (value != null
//                                            && !"".equalsIgnoreCase(value)
//                                            && !"null".equalsIgnoreCase(value)) {
//                                        value = value.trim();
//                                    }
//                                    //dataObj.put(tableName + ":" + colsSubList.get(i), value); // ravi etl integration
//                                    dataObj[i] = value; // ravi etl integration
//                                }
//                                totalDataList.add(dataObj);
//                            }
							String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
							totalDataList.add(FieldValues);
						}
					}

					String insertQuery = generateInsertQuery(toTableName, columnsList);
					Connection connection = null;
					try {
						connection = (Connection) componentUtilities.getConnection(connObj);
						preparedStatement = connection.prepareStatement(insertQuery);
						infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName,
								preparedStatement, columnsList, totalDataList, columnsList, toDataTypesList, "SAP_HANA",
								jobId, infoObject);
						int insertcount = (int)infoObject.get("insertcount");
					} catch (Exception e) {
						loop = false;
					} finally {
						if (connection != null) {
							connection.close();
						}
					}
					if (totalDataList != null && !totalDataList.isEmpty()) {
						start = start + limit;
						if (totalDataList.size() < limit) {
							loop = false;
						}
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

//            System.out.println("Before gc Total Memory (in bytes): " + Runtime.getRuntime().totalMemory());
			System.out.println("Before gc Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
//          System.out.println("Before gc Max Memory (in bytes): " + Runtime.getRuntime().maxMemory());
			System.gc();
//            System.out.println("After gc Total Memory (in bytes): " + Runtime.getRuntime().totalMemory());
			System.out.println("After gc  Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
//          System.out.println("After gc  Max Memory (in bytes): " + Runtime.getRuntime().maxMemory());
		}
		return totalErpData;
	}

	public List loadSAPJoinsDataToTarget(HttpServletRequest request, List columnsList, List toColumnsList,
			JSONObject fromOperator, JSONObject toOperator, String joinCondition, String childKeyColumn,
			String masterKeyColumn, String jobId) {
		JCO.Client sapConnection = null;
		JCO.Table tableData = null;
		JCO.Table fields = null;
		List totalErpData = new ArrayList();
		JCO.Function function = null;
		Object toConnection = null;
		PreparedStatement preparedStatement = null;
		int totalDataCount = 0;
		int rowByRowInsertCount = 0;
		int rowByRowRejectCount = 0;
		try {
//            int totalCount= 0;
			int start = 0;
			int limit = 0;

			int end = 0;
			long startTime = System.currentTimeMillis();
			JSONObject rowsRangeObj = (JSONObject) fromOperator.get("rowsRangeObj");
			if (rowsRangeObj != null && !rowsRangeObj.isEmpty()) {
				if (String.valueOf(rowsRangeObj.get("start")) != null
						&& !"".equalsIgnoreCase(String.valueOf(rowsRangeObj.get("start")))
						&& !"null".equalsIgnoreCase(String.valueOf(rowsRangeObj.get("start")))
						&& String.valueOf(rowsRangeObj.get("end")) != null
						&& !"".equalsIgnoreCase(String.valueOf(rowsRangeObj.get("end")))
						&& !"null".equalsIgnoreCase(String.valueOf(rowsRangeObj.get("end")))) {
					try {
						start = Integer.valueOf(String.valueOf(rowsRangeObj.get("start"))) - 1;
						end = Integer.valueOf(String.valueOf(rowsRangeObj.get("end")));

						limit = (end - start);

					} catch (Exception ex) {
						start = 0;
						limit = 0;
						end = 0;
					}
				}
			}

			String tableName = String.valueOf(fromOperator.get("tableName"));
			JSONObject connObj = (JSONObject) fromOperator.get("connObj");
			sapConnection = (JCO.Client) componentUtilities.getConnection(connObj);

			retrieveRepository(sapConnection);

			String WhereCondition = "";
			JSONArray whereClauseObjArray = (JSONArray) fromOperator.get("whereClauseObjArray");

			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");
			String fromTable = (String) fromOperator.get("tableName");
			JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
			String toTableName = (String) toOperator.get("tableName");
//            String toColsStr = (String) toColumnsList.stream().map(col -> col).collect(Collectors.joining(","));
			String toColsStr = (String) toColumnsList.stream().map(col -> {
				if (String.valueOf(col).contains("/")) {
					col = "\"" + col + "\"";
				}
				return col;
			}).collect(Collectors.joining(","));
			String selectQuery = "SELECT " + toColsStr + " FROM " + toTableName;
			if (toConnection == null) {
				toConnection = componentUtilities.getConnection(toConnObj);
			}

			JSONObject toColsObj = componentUtilities.getColumnsObjFromQuery(selectQuery, (Connection) toConnection,
					toConnObj);
			List toDataTypesList = (List) toColsObj.get("dataTypesList");

			List columnNamesList = (List) columnsList.stream().map(col -> {
				if (String.valueOf(col).contains("/")) {
					col = "\"" + col + "\"";
				}
				col = String.valueOf(col).contains(".") ? String.valueOf(col).split("\\.")[1] : col;
				return col;
			}).collect(Collectors.toList());

			if (limit > 0) {
				boolean loop = true;
				int maxLimit = 10000;
				if (limit <= maxLimit) {
					maxLimit = limit;
				}
				while (loop) {
					if (totalDataCount + maxLimit >= limit) {
						maxLimit = (end - start);
						loop = false;
					}
//                    List<Object[]> totalDataList = new ArrayList();

					List<Object[]> totalDataList = fetchSapDataJoins(request, columnsList, tableName,
							whereClauseObjArray, WhereCondition, connObj, sapConnection, joinCondition, childKeyColumn,
							masterKeyColumn, start, maxLimit, jobId);

					if (totalDataList != null && !totalDataList.isEmpty()) {
						JSONObject insertResultObj = dumpSapDataToDB(request, loop, toConnObj, toTableName,
								columnNamesList, toDataTypesList, totalDataList, totalDataCount, startTime, jobId, rowByRowInsertCount, rowByRowRejectCount);

						loop = (boolean) insertResultObj.get("loop");
						totalDataCount = (int) insertResultObj.get("totalDataCount");

					}
					start = start + maxLimit;
					if (totalDataList.size() < maxLimit) {
						loop = false;
					}

				}
			} else {
				boolean loop = true;
				start = 0;
				limit = 10000;
				while (loop) {
//                    if (end > 0 && ((totalDataCount + limit) >= end)) {
//                        limit = (end - start);
//                        loop = false;
//                    }
					List<Object[]> totalDataList = fetchSapDataJoins(request, columnsList, tableName,
							whereClauseObjArray, WhereCondition, connObj, sapConnection, joinCondition, childKeyColumn,
							masterKeyColumn, start, limit, jobId);

					if (totalDataList != null && !totalDataList.isEmpty()) {
						JSONObject insertResultObj = dumpSapDataToDB(request, loop, toConnObj, toTableName,
								columnNamesList, toDataTypesList, totalDataList, totalDataCount, startTime, jobId, rowByRowInsertCount, rowByRowRejectCount);

						loop = (boolean) insertResultObj.get("loop");
						totalDataCount = (int) insertResultObj.get("totalDataCount");

					}
					start = start + limit;
					if (totalDataList.size() < limit) {
						loop = false;
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (sapConnection != null) {
//                    sapConnection.reset();
//                    sapConnection.disconnect();
//                    JCO.releaseClient(sapConnection);
				}
				if (toConnection != null && toConnection instanceof Connection) {
					((Connection) toConnection).close();
				}
				if (toConnection != null && toConnection instanceof JCO.Client) {
//                    ((JCO.Client) toConnection).reset();
//                    ((JCO.Client) toConnection).disconnect();
//                    JCO.releaseClient((JCO.Client) toConnection);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		return totalErpData;
	}

	public List loadSAPJoinsDataToTarget1(HttpServletRequest request, List columnsList, List toColumnsList,
			JSONObject fromOperator, JSONObject toOperator, String joinCondition, String jobId) {
		JCO.Client sapConnection = null;
		JCO.Table tableData = null;
		JCO.Table fields = null;
		List totalErpData = new ArrayList();
		JCO.Function function = null;
		Object toConnection = null;
		PreparedStatement preparedStatement = null;
		int totalDataCount = 0;
		JSONObject infoObject =  new JSONObject();
		try {
//            int totalCount= 0;
			int start = 0;
			int limit = 0;

			int end = 0;

			JSONObject rowsRangeObj = (JSONObject) fromOperator.get("rowsRangeObj");
			if (rowsRangeObj != null && !rowsRangeObj.isEmpty()) {
				if (String.valueOf(rowsRangeObj.get("start")) != null
						&& !"".equalsIgnoreCase(String.valueOf(rowsRangeObj.get("start")))
						&& !"null".equalsIgnoreCase(String.valueOf(rowsRangeObj.get("start")))
						&& String.valueOf(rowsRangeObj.get("end")) != null
						&& !"".equalsIgnoreCase(String.valueOf(rowsRangeObj.get("end")))
						&& !"null".equalsIgnoreCase(String.valueOf(rowsRangeObj.get("end")))) {
					try {
						start = Integer.valueOf(String.valueOf(rowsRangeObj.get("start"))) - 1;
						end = Integer.valueOf(String.valueOf(rowsRangeObj.get("end")));
						if (end - start > 10000) {
							limit = 10000;
						} else {
							limit = (end - start);
						}

					} catch (Exception ex) {
						start = 0;
						limit = 0;
						end = 0;
					}
				}
			}

			String tableName = String.valueOf(fromOperator.get("tableName"));
			JSONObject connObj = (JSONObject) fromOperator.get("connObj");
			sapConnection = (JCO.Client) componentUtilities.getConnection(connObj);

			retrieveRepository(sapConnection);

			String WhereCondition = "";
			JSONArray whereClauseObjArray = (JSONArray) fromOperator.get("whereClauseObjArray");
			if (whereClauseObjArray != null && !whereClauseObjArray.isEmpty()) {
				WhereCondition = (String) whereClauseObjArray.stream().map(whereClauseObject -> {
					String column = (String) ((JSONObject) whereClauseObject).get("column");
					String condOperator = (String) ((JSONObject) whereClauseObject).get("operator");

					String value = (String) ((JSONObject) whereClauseObject).get("value");
					if (condOperator != null && "=".equalsIgnoreCase(condOperator)) {
						value = "'" + value + "'";
					} else if (condOperator != null && "LIKE".equalsIgnoreCase(condOperator)) {
						value = "'%" + value + "%'";
					}
					return column + " " + condOperator + " " + value + "";
				}).collect(Collectors.joining(" AND "));

			}

			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");
			String fromTable = (String) fromOperator.get("tableName");
			JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
			String toTableName = (String) toOperator.get("tableName");
			String toColsStr = (String) toColumnsList.stream().map(col -> {
				if (String.valueOf(col).contains("/")) {
					col = "\"" + col + "\"";
				}
				return col;
			}).collect(Collectors.joining(","));
			String selectQuery = "SELECT " + toColsStr + " FROM " + toTableName;
			if (toConnection == null) {
				toConnection = componentUtilities.getConnection(toConnObj);
			}

			JSONObject toColsObj = componentUtilities.getColumnsObjFromQuery(selectQuery, (Connection) toConnection,
					toConnObj);
			List toDataTypesList = (List) toColsObj.get("dataTypesList");

			if (limit > 0) {
				boolean loop = true;
				int maxLimit = 10000;
				if (limit <= maxLimit) {
					maxLimit = limit;
				}
				while (loop) {
					if (end > 0 && ((totalDataCount + limit) >= end)) {
						limit = (end - start);
						loop = false;
					}
					List<Object[]> totalDataList = new ArrayList();
//                  function = getFunction("RFC_READ_TABLE");
//                    function = getFunction("/PILOG/BULK_READ_TABLE"); // ravi updated code changes
					function = getFunction("/PILOG/BAPI_REVERSE_ETL"); // ravi updated code changes
					if (function != null) {
						JCO.ParameterList listParams = function.getImportParameterList();
						Object fieldsList = function.getTableParameterList();
						fields = function.getTableParameterList().getTable("INPTABFLDS1");
						tableData = function.getTableParameterList().getTable("OTRETURN");
//                        JCO.Table Option = function.getTableParameterList().getTable("OPTIONS");
						for (int i = 0; i < columnsList.size(); i++) {
							String columnName = (String) columnsList.get(i);
							fields.appendRow();
							fields.setValue(columnName, "TABLE_FIELD");
						}
//                        String joinCondition = "MARA INNERJOIN MARC ON MARA~MATNR = MARC~MATNR";

						String whereClauseCond = "";
						listParams.setValue(joinCondition, "INJOINCON");// For MARA
//                        listParams.setValue("♥", "INDELIMITER");
//                        listParams.setValue(whereClauseCond, "INWHERECL");//For MARA
//                        listParams.setValue("♥", "INDELIMITER");
//                                    listParams.setValue((start == 0 ? (start) : (start - 1)), "ROWSKIPS");
//                                    listParams.setValue((limit + 1), "ROWCOUNT");
						if (maxLimit > 0) {
							listParams.setValue((start == 0 ? (start) : (start)), "ROWSKIPS"); // ravi etl integration
							listParams.setValue((maxLimit), "ROWCOUNT"); // ravi etl integration
						}

//                    String WhereCondition = (String) tablesObj.get(tableName);
//                        if (WhereCondition != null && !WhereCondition.isEmpty()) {
//                            WhereCondition = WhereCondition.replaceAll(tableName + "\\.", ""); // ravi etl integration
//                            Option.appendRow();
//                            Option.setValue(WhereCondition, "TEXT");
//                        }
						long startTime = System.currentTimeMillis();
						sapConnection.execute(function);
						long endTime = System.currentTimeMillis();
						System.out.println("SAP executionTime :: " + (endTime - startTime) / 1000);
						System.out.println("Before gc Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
						System.gc();
						System.out.println("After gc  Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
						int fetchCount = tableData.getNumRows();

						if (fetchCount != 0) {
//                                        Map dataObj = new HashMap();
//                            Object[] dataObj = new Object[columnsList.size()];
//                            if (columnsList != null && !columnsList.isEmpty()) {
//                                for (int i = 0; i < columnsList.size(); i++) {
//                                    String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
//                                    String value = FieldValues[i];
//                                    if (value != null
//                                            && !"".equalsIgnoreCase(value)
//                                            && !"null".equalsIgnoreCase(value)) {
//                                        value = value.trim();
//                                    }
//                                    dataObj[i] = value;
//                                }
//                                totalDataList.add(dataObj);
//                            }
							String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
							totalDataList.add(FieldValues);
						}
						while (tableData.nextRow()) {
							String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
							totalDataList.add(FieldValues);
							// Map dataObj = new HashMap();
//                            Object[] dataObj = new Object[columnsList.size()];
//                            if (columnsList != null && !columnsList.isEmpty()) {
//                                for (int i = 0; i < columnsList.size(); i++) {
//                                    String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
//                                    //dataObj.put(tableName + ":" + colsSubList.get(i), FieldValues[i]);
//                                    String value = FieldValues[i];
//                                    if (value != null
//                                            && !"".equalsIgnoreCase(value)
//                                            && !"null".equalsIgnoreCase(value)) {
//                                        value = value.trim();
//                                    }
//                                    //dataObj.put(tableName + ":" + colsSubList.get(i), value); // ravi etl integration
//                                    dataObj[i] = value; // ravi etl integration
//                                }
//                                totalDataList.add(dataObj);
//                            }
						}
					}
					List columnNamesList = (List) columnsList.stream().map(col -> {
						if (String.valueOf(col).contains("/")) {
							col = "\"" + col + "\"";
						}
						col = String.valueOf(col).contains(".") ? String.valueOf(col).split("\\.")[1] : col;
						return col;
					}).collect(Collectors.toList());
					String insertQuery = generateInsertQuery(toTableName, columnNamesList);
					Connection connection = null;
					try {
						connection = (Connection) componentUtilities.getConnection(toConnObj);
						preparedStatement = connection.prepareStatement(insertQuery);
						infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName,
								preparedStatement, columnNamesList, totalDataList, columnNamesList, toDataTypesList,
								"SAP_HANA", jobId, infoObject);
						int insertcount = (int)infoObject.get("insertcount");
						if (insertcount == 0) {
							loop = false;
						}
						totalDataCount = totalDataCount + insertcount;

						try {
							componentUtilities.processETLLog(
									(String) request.getSession(false).getAttribute("ssUsername"),
									(String) request.getSession(false).getAttribute("ssOrgId"),
									"Inserted " + totalDataCount + " Records ", "INFO", 20, "Y", jobId);
						} catch (Exception ex) {
						}
					} catch (Exception e) {
						loop = false;
					} finally {
						if (connection != null) {
							connection.close();
						}
					}
					if (totalDataList != null && !totalDataList.isEmpty()) {
						start = start + limit;
						if (totalDataList.size() < limit) {
							loop = false;
						}
					}

				}
			} else {
				boolean loop = true;
				start = 0;
				limit = 10000;
				while (loop) {
					if (end > 0 && ((totalDataCount + limit) >= end)) {
						limit = (end - start);
						loop = false;
					}
					List<Object[]> totalDataList = new ArrayList();
//                  function = getFunction("RFC_READ_TABLE");
//                    function = getFunction("/PILOG/BULK_READ_TABLE"); // ravi updated code changes
					function = getFunction("/PILOG/BAPI_REVERSE_ETL"); // ravi updated code changes
					if (function != null) {
						JCO.ParameterList listParams = function.getImportParameterList();
						Object fieldsList = function.getTableParameterList();
						fields = function.getTableParameterList().getTable("INPTABFLDS1");
						tableData = function.getTableParameterList().getTable("OTRETURN");
//                        JCO.Table Option = function.getTableParameterList().getTable("OPTIONS");
						for (int i = 0; i < columnsList.size(); i++) {
							String columnName = (String) columnsList.get(i);
//                            fields.appendRow();
//                            fields.setValue("", "MAN");
							fields.appendRow();
							fields.setValue(columnName, "TABLE_FIELD");
						}
//                        String joinCondition = "MARA INNER JOIN MARC ON MARA~MATNR = MARC~MATNR";
//                        String joinCondition = "MARA";
						String whereClauseCond = "";
						listParams.setValue(joinCondition, "INJOINCON");// For MARA
						listParams.setValue("", "INDELIMITOR");
//                        listParams.setValue(whereClauseCond, "INWHERECL");//For MARA
//                        listParams.setValue("", "INDELIMITOR");
//                                    listParams.setValue((start == 0 ? (start) : (start - 1)), "ROWSKIPS");
//                                    listParams.setValue((limit + 1), "ROWCOUNT");
						if (limit > 0) {
							listParams.setValue((start == 0 ? (start + 1) : (start)), "ROW_SKIP"); // ravi etl
																									// integration
							listParams.setValue((limit), "ROW_COUNT"); // ravi etl integration
//                            listParams.setValue((100), "ROW_COUNT"); // ravi etl integration
						}

//                    String WhereCondition = (String) tablesObj.get(tableName);
//                        if (WhereCondition != null && !WhereCondition.isEmpty()) {
////                        WhereCondition = WhereCondition.replaceAll(tableName + "\\.", ""); // ravi etl integration
//                            Option.appendRow();
//                            Option.setValue(WhereCondition, "TEXT");
//                        }
						long startTime = System.currentTimeMillis();
						sapConnection.execute(function);
						long endTime = System.currentTimeMillis();
						System.out.println("SAP executionTime :: " + (endTime - startTime) / 1000);
						System.out.println("Before gc Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
						System.gc();
						System.out.println("After gc  Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
						int fetchCount = tableData.getNumRows();

						if (fetchCount != 0) {
//                                        Map dataObj = new HashMap();
//                            Object[] dataObj = new Object[columnsList.size()];
//                            if (columnsList != null && !columnsList.isEmpty()) {
//                                for (int i = 0; i < columnsList.size(); i++) {
//                                    String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
//                                    String value = FieldValues[i];
//                                    if (value != null
//                                            && !"".equalsIgnoreCase(value)
//                                            && !"null".equalsIgnoreCase(value)) {
//                                        value = value.trim();
//                                    }
//                                    dataObj[i] = value;
//                                }
//                                totalDataList.add(dataObj);
//                            }
							String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
							totalDataList.add(FieldValues);
						}
						while (tableData.nextRow()) {
							// Map dataObj = new HashMap();
//                            Object[] dataObj = new Object[columnsList.size()];
//                            if (columnsList != null && !columnsList.isEmpty()) {
//                                for (int i = 0; i < columnsList.size(); i++) {
//                                    String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
//                                    //dataObj.put(tableName + ":" + colsSubList.get(i), FieldValues[i]);
////                                    FieldValues
//                                    String value = FieldValues[i];
//                                    if (value != null
//                                            && !"".equalsIgnoreCase(value)
//                                            && !"null".equalsIgnoreCase(value)) {
//                                        value = value.trim();
//                                    }
//                                    //dataObj.put(tableName + ":" + colsSubList.get(i), value); // ravi etl integration
//                                    dataObj[i] = value; // ravi etl integration
//                                }
//                                totalDataList.add(dataObj);
//                            }
							String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
							totalDataList.add(FieldValues);
						}
					}
					List columnNamesList = (List) columnsList.stream().map(col -> {
						if (String.valueOf(col).contains("/")) {
							col = "\"" + col + "\"";
						}
						col = String.valueOf(col).contains(".") ? String.valueOf(col).split("\\.")[1] : col;
						return col;
					}).collect(Collectors.toList());

					String insertQuery = generateInsertQuery(toTableName, columnNamesList);
					Connection connection = null;
					try {
						connection = (Connection) componentUtilities.getConnection(toConnObj);
						preparedStatement = connection.prepareStatement(insertQuery);
						infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName,
								preparedStatement, columnNamesList, totalDataList, columnNamesList, toDataTypesList,
								"SAP_HANA", jobId, infoObject);
						int insertcount = (int)infoObject.get("insertcount");
						totalDataCount = totalDataCount + insertcount;
						try {
							componentUtilities.processETLLog(
									(String) request.getSession(false).getAttribute("ssUsername"),
									(String) request.getSession(false).getAttribute("ssOrgId"),
									"Inserted " + totalDataCount + " Records ", "INFO", 20, "Y", jobId);
						} catch (Exception ex) {
						}
						if (insertcount == 0) {
							loop = false;
						}
					} catch (Exception e) {
						loop = false;
					} finally {
						if (connection != null) {
							connection.close();
						}
					}
					if (totalDataList != null && !totalDataList.isEmpty()) {
						start = start + limit;
						if (totalDataList.size() < limit) {
							loop = false;
						}
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (sapConnection != null) {
//                    sapConnection.reset();
//                    sapConnection.disconnect();
//                    JCO.releaseClient(sapConnection);
				}
				if (toConnection != null && toConnection instanceof Connection) {
					((Connection) toConnection).close();
				}
				if (toConnection != null && toConnection instanceof JCO.Client) {
//                    ((JCO.Client) toConnection).reset();
//                    ((JCO.Client) toConnection).disconnect();
//                    JCO.releaseClient((JCO.Client) toConnection);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		return totalErpData;
	}

	public List loadSAPDataToTargetReverse(HttpServletRequest request, List columnsList, List toColumnsList,
			JSONObject fromOperator, JSONObject toOperator, String jobId) {
		JCO.Client sapConnection = null;
		JCO.Table tableData = null;
		JCO.Table fields = null;
		List totalErpData = new ArrayList();
		JCO.Function function = null;
		Object toConnection = null;
		Object anotherTableConnection = null;
		PreparedStatement preparedStatement = null;
		int totalDataCount = 0;
		int rowByRowInsertCount = 0;
		int rowByRowRejectCount = 0;
		try {

			long startTime = System.currentTimeMillis();

			String tableName = String.valueOf(fromOperator.get("tableName"));
			JSONObject connObj = (JSONObject) fromOperator.get("connObj");
			sapConnection = (JCO.Client) componentUtilities.getConnection(connObj);

			retrieveRepository(sapConnection);

			List anotherTableFilterData = new ArrayList();
			String filterSapTableColumnName = "";
			String filterSapSourceTableCompareColumn = "";

			String WhereCondition = "";
			JSONArray whereClauseObjArray = (JSONArray) fromOperator.get("whereClauseObjArray");

			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");
			String fromTable = (String) fromOperator.get("tableName");
			JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
			String toTableName = (String) toOperator.get("tableName");
			String toColsStr = (String) toColumnsList.stream().map(col -> {
				if (String.valueOf(col).contains("/")) {
					col = "\"" + col + "\"";
				}
				return col;
			}).collect(Collectors.joining(","));
			String selectQuery = "SELECT " + toColsStr + " FROM " + toTableName;
			if (toConnection == null) {
				toConnection = componentUtilities.getConnection(toConnObj);
			}

			JSONObject toColsObj = componentUtilities.getColumnsObjFromQuery(selectQuery, (Connection) toConnection,
					toConnObj);
			List toDataTypesList = (List) toColsObj.get("dataTypesList");

			int totalfetchCount = 0;

			function = getFunction("RFC_READ_TABLE"); // ravi updated code changes

			if (function != null) {
				tableData = function.getTableParameterList().getTable("DATA");
//                fields = function.getTableParameterList().getTable("FIELDS");
//
//                fields.appendRow();
//                fields.setValue(columnsList.get(0), "FIELDNAME");

				JCO.ParameterList listParams = function.getImportParameterList();
				listParams.setValue(tableName, "QUERY_TABLE");// For MARA
				try {
					System.out.println("fetchCount Execution start:: " + totalfetchCount);
					sapConnection.execute(function);
					totalfetchCount = tableData.getNumRows();

					System.out.println("fetchCount Execution end :: fetchCount :: " + totalfetchCount);

				} catch (Exception e) {
					System.out.println("SAP error :: " + e.getMessage());

				}
			}

			if (totalfetchCount > 0) {
				boolean loop = true;
				int limit = 10000;
				int start = totalfetchCount - limit;
				while (loop) {
					if (start < 0) {
						limit = -1 * start;
						start = 0;
						loop = false;
					}

//                    List<Object[]> totalDataList = new ArrayList();
					List<Object[]> totalDataList = fetchSapData(request, columnsList, tableName, whereClauseObjArray,
							anotherTableFilterData, WhereCondition, filterSapSourceTableCompareColumn, connObj,
							sapConnection, start, limit, jobId);

					if (totalDataList != null && !totalDataList.isEmpty()) {
						JSONObject insertResultObj = dumpSapDataToDB(request, loop, toConnObj, toTableName, columnsList,
								toDataTypesList, totalDataList, totalDataCount, startTime, jobId, rowByRowInsertCount, rowByRowRejectCount);

						loop = (boolean) insertResultObj.get("loop");
						totalDataCount = (int) insertResultObj.get("totalDataCount");

					}
					start = start - limit;

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (sapConnection != null) {
//                    sapConnection.reset();
//                    sapConnection.disconnect();
//                    JCO.releaseClient(sapConnection);
				}
				if (toConnection != null && toConnection instanceof Connection) {
					((Connection) toConnection).close();
				}
				if (toConnection != null && toConnection instanceof JCO.Client) {
//                    ((JCO.Client) toConnection).reset();
//                    ((JCO.Client) toConnection).disconnect();
//                    JCO.releaseClient((JCO.Client) toConnection);
				}
				if (anotherTableConnection != null && anotherTableConnection instanceof Connection) {
					((Connection) anotherTableConnection).close();
				}

			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		return totalErpData;
	}

	public List loadSAPDataToTarget(HttpServletRequest request, List columnsList, List toColumnsList,
			JSONObject fromOperator, JSONObject toOperator, String jobId) {
		JCO.Client sapConnection = null;
		JCO.Table tableData = null;
		JCO.Table fields = null;
		List totalErpData = new ArrayList();
		JCO.Function function = null;
		Connection toConnection = null;
		Object anotherTableConnection = null;
		PreparedStatement preparedStatement = null;
		int totalDataCount = 0;
		int rowByRowInsertCount = 0;
		int rowByRowRejectCount = 0;
		try {

			int start = 0;
			int limit = 0;

			int end = 0;
			long startTime = System.currentTimeMillis();
			System.out.println("  Thread :: " + Thread.currentThread() + " --> fromOperatorId :: "
					+ fromOperator.get("operatorId"));
			JSONObject rowsRangeObj = (JSONObject) fromOperator.get("rowsRangeObj");
			if (rowsRangeObj != null && !rowsRangeObj.isEmpty()) {
				if (String.valueOf(rowsRangeObj.get("start")) != null
						&& !"".equalsIgnoreCase(String.valueOf(rowsRangeObj.get("start")))
						&& !"null".equalsIgnoreCase(String.valueOf(rowsRangeObj.get("start")))
						&& String.valueOf(rowsRangeObj.get("end")) != null
						&& !"".equalsIgnoreCase(String.valueOf(rowsRangeObj.get("end")))
						&& !"null".equalsIgnoreCase(String.valueOf(rowsRangeObj.get("end")))) {
					try {
						start = Integer.valueOf(String.valueOf(rowsRangeObj.get("start"))) - 1;
						end = Integer.valueOf(String.valueOf(rowsRangeObj.get("end")));

						limit = (end - start);

						System.out.println(" loadSAPDataToTarget -> Thread :: " + Thread.currentThread() + " start ::"
								+ start + " - limit ::" + limit);

					} catch (Exception ex) {
						start = 0;
						limit = 0;
						end = 0;
					}
				}
			}

			String tableName = String.valueOf(fromOperator.get("tableName"));
			JSONObject connObj = (JSONObject) fromOperator.get("connObj");
			sapConnection = (JCO.Client) componentUtilities.getConnection(connObj);

			retrieveRepository(sapConnection);

			String WhereCondition = "";
			JSONArray whereClauseObjArray = (JSONArray) fromOperator.get("whereClauseObjArray");

			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");
			String fromTable = (String) fromOperator.get("tableName");
			JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
			String toTableName = (String) toOperator.get("tableName");
			String toColsStr = (String) toColumnsList.stream().map(col -> {
				if (String.valueOf(col).contains("/")) {
					col = "\"" + col + "\"";
				}
				return col;
			}).collect(Collectors.joining(","));
			String selectQuery = "SELECT " + toColsStr + " FROM " + toTableName;
			if (toConnection == null) {
				toConnection = (Connection) componentUtilities.getConnection(toConnObj);
			}

			JSONObject toColsObj = componentUtilities.getColumnsObjFromQuery(selectQuery, toConnection, toConnObj);
			List toDataTypesList = (List) toColsObj.get("dataTypesList");

			List totalAnotherTableFilterData = new ArrayList();
			String filterSapTableColumnName = "";
			String filterSapSourceTableCompareColumn = "";

			JSONObject anotherTableFilterObj = (JSONObject) fromOperator.get("anotherTableFilterObj");
			if (anotherTableFilterObj != null && !anotherTableFilterObj.isEmpty()) {
				String filterSapTableConnectionNameStr = (String) anotherTableFilterObj
						.get("filterSapTableConnectionName");
				JSONObject filterSapTableConnection = (JSONObject) JSONValue.parse(filterSapTableConnectionNameStr);
				String filterSapTableName = (String) anotherTableFilterObj.get("filterSapTableName");
				filterSapTableColumnName = (String) anotherTableFilterObj.get("filterSapTableColumnName");
				String filterSapTableRowrangeStartStr = (String) anotherTableFilterObj
						.get("filterSapTableRowrangeStart");
				String filterSapTableRowrangeEndStr = (String) anotherTableFilterObj.get("filterSapTableRowrangeEnd");
				String filterSapTableWhereCond = (String) anotherTableFilterObj.get("filterSapTableWhereCond");
				filterSapSourceTableCompareColumn = (String) anotherTableFilterObj
						.get("filterSapSourceTableCompareColumn");

//                String[] filterSapTableColumnArray = filterSapTableColumnName.split(",");
//                List filterSapTableColumnList = new ArrayList(Arrays.asList(filterSapTableColumnArray));
				List filterSapTableColumnList = new ArrayList();
				filterSapTableColumnList.add(filterSapTableColumnName);
				int filterSapTableRowrangeStart = (filterSapTableRowrangeStartStr != null
						&& !"".equalsIgnoreCase(filterSapTableRowrangeStartStr))
								? Integer.valueOf(filterSapTableRowrangeStartStr)
								: 0;
				int filterSapTableRowrangeEnd = (filterSapTableRowrangeEndStr != null
						&& !"".equalsIgnoreCase(filterSapTableRowrangeEndStr))
								? Integer.valueOf(filterSapTableRowrangeEndStr)
								: 0;
				int filterSapTableLimit = filterSapTableRowrangeEnd - filterSapTableRowrangeStart;
				anotherTableConnection = componentUtilities.getConnection(filterSapTableConnection);
				if (anotherTableConnection != null && anotherTableConnection instanceof Connection) {
					String query = "SELECT " + filterSapTableColumnName + " FROM " + filterSapTableName;
					if (filterSapTableWhereCond != null && !"".equalsIgnoreCase(filterSapTableWhereCond)) {
						query += " WHERE " + filterSapTableWhereCond;
					}
					if (filterSapTableLimit != 0) {
						query += " OFFSET " + filterSapTableRowrangeStart + " ROWS FETCH NEXT " + filterSapTableLimit
								+ " ROWS ONLY";
					}

					totalAnotherTableFilterData = processJobComponentsDAO.getTableDataWithQuery(request, query,
							filterSapTableConnection, "");
				} else if (anotherTableConnection != null && anotherTableConnection instanceof JCO.Client) {
					totalAnotherTableFilterData = getSAPDataWithLimits(filterSapTableName, filterSapTableColumnList,
							filterSapTableConnection, filterSapTableWhereCond, filterSapTableRowrangeStart,
							filterSapTableRowrangeEnd, filterSapTableLimit);
				}

			}

			if (totalAnotherTableFilterData != null && !totalAnotherTableFilterData.isEmpty()) {
				for (int j = 0; j < totalAnotherTableFilterData.size(); j = j + 10000) {
					int startIndex = j;
					int endIndex = (j + 10000 > totalAnotherTableFilterData.size()) ? totalAnotherTableFilterData.size()
							: j + 10000;
					List anotherTableFilterData = totalAnotherTableFilterData.subList(startIndex, endIndex);

					boolean loop = true;
					start = 0;
					limit = 10000;
					while (loop) {
//                        if (end > 0 && ((totalDataCount + limit) >= end)) {
//                            limit = (end - start);
//                            loop = false;
//                        }
						List<Object[]> totalDataList = fetchSapData(request, columnsList, tableName,
								whereClauseObjArray, anotherTableFilterData, WhereCondition,
								filterSapSourceTableCompareColumn, connObj, sapConnection, start, limit, jobId);

						if (totalDataList != null && !totalDataList.isEmpty()) {
							JSONObject insertResultObj = dumpSapDataToDB(request, loop, toConnObj, toTableName,
									columnsList, toDataTypesList, totalDataList, totalDataCount, startTime, jobId, 
									rowByRowInsertCount, rowByRowRejectCount);

							loop = (boolean) insertResultObj.get("loop");
							totalDataCount = (int) insertResultObj.get("totalDataCount");

						}
						start = start + limit;
						if (totalDataList.size() < limit) {
							loop = false;
						}

					}

				}

			} else {
				List anotherTableFilterData = totalAnotherTableFilterData;

				if (limit > 0) {
					boolean loop = true;
					int maxLimit = 10000;
					if (limit <= maxLimit) {
						maxLimit = limit;
					}
					while (loop) {
						if (totalDataCount + maxLimit >= limit) {
							maxLimit = (end - start);
							loop = false;
						}

						List<Object[]> totalDataList = fetchSapData(request, columnsList, tableName,
								whereClauseObjArray, anotherTableFilterData, WhereCondition,
								filterSapSourceTableCompareColumn, connObj, sapConnection, start, maxLimit, jobId);

						if (totalDataList != null && !totalDataList.isEmpty()) {
							JSONObject insertResultObj = dumpSapDataToDB(request, loop, toConnObj, toTableName,
									columnsList, toDataTypesList, totalDataList, totalDataCount, startTime, jobId, 
									rowByRowInsertCount, rowByRowRejectCount);

							loop = (boolean) insertResultObj.get("loop");
							totalDataCount = (int) insertResultObj.get("totalDataCount");

						}
						start = start + maxLimit;
						if (totalDataList.size() < maxLimit) {
							loop = false;
						}

					}
				} else {
					boolean loop = true;
					start = 0;
					limit = 10000;
					while (loop) {
//                        if (end > 0 && ((totalDataCount + limit) >= end)) {
//                            limit = (end - start);
//                            loop = false;
//                        }
//                     long startTime = System.currentTimeMillis();
//                        List<Object[]> totalDataList = new ArrayList();
						List<Object[]> totalDataList = fetchSapData(request, columnsList, tableName,
								whereClauseObjArray, anotherTableFilterData, WhereCondition,
								filterSapSourceTableCompareColumn, connObj, sapConnection, start, limit, jobId);

						if (totalDataList != null && !totalDataList.isEmpty()) {
							JSONObject insertResultObj = dumpSapDataToDB(request, loop, toConnObj, toTableName,
									columnsList, toDataTypesList, totalDataList, totalDataCount, startTime, jobId, 
									rowByRowInsertCount, rowByRowRejectCount);

							loop = (boolean) insertResultObj.get("loop");
							totalDataCount = (int) insertResultObj.get("totalDataCount");

						}
						start = start + limit;
						if (totalDataList.size() < limit) {
							loop = false;
						}
					}
				}

			}

			try {
				if (rowByRowInsertCount  > 0 && rowByRowRejectCount!=totalDataCount) {
					componentUtilities.processETLReconciliationUpdateTarget(
							(String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"), jobId, toTableName,
							String.valueOf(rowByRowInsertCount), String.valueOf(toOperator.get("subJobId")), String.valueOf(columnsList.size()));
					
					componentUtilities.processETLReconciliationUpdateRejectRecords(
							(String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"), jobId,
							String.valueOf(rowByRowRejectCount), String.valueOf(toOperator.get("subJobId")));
				} else {
					componentUtilities.processETLReconciliationUpdateTarget(
							(String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"), jobId, toTableName,
							String.valueOf(totalDataCount), String.valueOf(toOperator.get("subJobId")), String.valueOf(columnsList.size()));
				}
			
				try {
					componentUtilities.processETLJobPreview((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), jobId, String.valueOf(toOperator.get("operatorId")),
						String.valueOf(totalDataCount));
				} catch (Exception ex) {
				}

			} catch (Exception ex) {
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (sapConnection != null) {
//                    sapConnection.reset();
//                    sapConnection.disconnect();
//                    JCO.releaseClient(sapConnection);
				}
				if (toConnection != null && toConnection instanceof Connection) {
					((Connection) toConnection).close();
				}
				if (toConnection != null && toConnection instanceof JCO.Client) {
//                    ((JCO.Client) toConnection).reset();
//                    ((JCO.Client) toConnection).disconnect();
//                    JCO.releaseClient((JCO.Client) toConnection);
				}
				if (anotherTableConnection != null && anotherTableConnection instanceof Connection) {
					((Connection) anotherTableConnection).close();
				}

			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		return totalErpData;
	}

	public JSONObject dumpSapDataToDB(HttpServletRequest request, boolean loop, JSONObject toConnObj,
			String toTableName, List columnsList, List toDataTypesList, List totalDataList, int totalDataCount,
			long startTime, String jobId,int rowByRowInsertCount, int rowByRowRejectCount) {
		JSONObject resultObj = new JSONObject();
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		JSONObject infoObject = new JSONObject();
		try {
			long loopStartTime = System.currentTimeMillis();
			String insertQuery = generateInsertQuery(toTableName, columnsList);
			connection = (Connection) componentUtilities.getConnection(toConnObj);
			preparedStatement = connection.prepareStatement(insertQuery);
			infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName, preparedStatement,
					columnsList, totalDataList, columnsList, toDataTypesList, "SAP_HANA", jobId, infoObject);
			int insertcount = (int)infoObject.get("insertCount");
			if (insertcount == 0) {
				loop = false;
			}

			totalDataCount = totalDataCount + insertcount;
			long endTime = System.currentTimeMillis();
			System.out.println(" executionTime :: " + (endTime - startTime) / 1000);
			String logToTableName = toTableName;
			if (toTableName.startsWith("ZZ_TEMP")) {
				logToTableName = "Staging Table";
			}

			if (infoObject.get("rowByRowInsertCount") != null) {
				rowByRowInsertCount += (int)infoObject.get("rowByRowInsertCount");
				rowByRowRejectCount += (int)infoObject.get("rowByRowRejectCount");
				try {
					componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"),
							"Processed " + infoObject.get("rowByRowInsertCount") + " Records into "
									+ logToTableName + " in " + (endTime - loopStartTime) / 1000 + " sec",
							"INFO", 20, "Y", jobId);
				} catch (Exception ex) {
				}
			} else {
				rowByRowInsertCount += totalDataCount;
				try {
					componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"),
							"Inserted " + totalDataCount + " Records into " + logToTableName + " in "
									+ (endTime - startTime) / 1000 + " sec",
							"INFO", 20, "Y", jobId);
				} catch (Exception ex) {
				}
			}

		} catch (Exception e) {
			loop = false;
		} finally {
			try {
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		resultObj.put("loop", loop);
		resultObj.put("totalDataCount", totalDataCount);
		return resultObj;
	}

	public List fetchSapData(HttpServletRequest request, List columnsList, String tableName,
			JSONArray whereClauseObjArray, List anotherTableFilterData, String WhereCondition,
			String filterSapSourceTableCompareColumn, JSONObject sapConnObj, JCO.Client sapConnection, int start,
			int limit, String jobId) {
//        JCO.Client sapConnection = null;
		JCO.Table tableData = null;
		JCO.Table fields = null;
		List totalErpData = new ArrayList();
		JCO.Function function = null;
		List totalDataList = new ArrayList();
		try {
			System.out.println(" fetchSapData -> Thread :: " + Thread.currentThread() + " start ::" + start
					+ " - limit ::" + limit);

			JCOUtills jCOUtills = new JCOUtills();
			function = getFunction("/PILOG/BULK_READ_TABLE"); // ravi updated code changes
			if (function == null) {
				retrieveRepository(sapConnection);
				function = getFunction("/PILOG/BULK_READ_TABLE"); // ravi updated code changes
			}
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
				listParams.setValue(tableName, "QUERY_TABLE");// For MARA
//                        listParams.setValue("♥", "DELIMITER");
				listParams.setValue("♥", "DELIMITER");
//                                    listParams.setValue((start == 0 ? (start) : (start - 1)), "ROWSKIPS");
//                                    listParams.setValue((limit + 1), "ROWCOUNT");
				if (limit > 0) {
					listParams.setValue((start == 0 ? (start) : (start)), "ROWSKIPS"); // ravi etl integration
					listParams.setValue((limit), "ROWCOUNT"); // ravi etl integration
				}

				if (whereClauseObjArray != null && !whereClauseObjArray.isEmpty()) {

					for (int i = 0; i < whereClauseObjArray.size(); i++) {
						JSONObject whereClauseObject = (JSONObject) whereClauseObjArray.get(i);

						String column = (String) whereClauseObject.get("column");
						String condOperator = (String) whereClauseObject.get("operator");
						String condition = "";
						String value = (String) whereClauseObject.get("value");
						if (condOperator != null && "=".equalsIgnoreCase(condOperator)) {
							value = "'" + value + "'";
							condition = column + " " + condOperator + " " + value + "";
						} else if (condOperator != null && "!=".equalsIgnoreCase(condOperator)) {
							value = "'" + value + "'";
							condition = column + " <> " + value + "";
						} else if (condOperator != null && "LIKE".equalsIgnoreCase(condOperator)) {
							value = "'%" + value + "%'";
							condition = column + " " + condOperator + " " + value + "";
						} else if (condOperator != null && "NOT LIKE".equalsIgnoreCase(condOperator)) {
							value = "'%" + value + "%'";
							condition = column + " " + condOperator + " " + value + "";
						} else if (condOperator != null && "IN".equalsIgnoreCase(condOperator)) {
//                                    value = "'%" + value + "%'";
							condition = column + " " + condOperator + " " + value + "";
						} else if (condOperator != null && "NOT IN".equalsIgnoreCase(condOperator)) {
//                                    value = "'%" + value + "%'";
							condition = column + " " + condOperator + " " + value + "";
						} else if (condOperator != null && "BETWEEN".equalsIgnoreCase(condOperator)) {
							String fromValue = (String) whereClauseObject.get("value");
							String toValue = (String) whereClauseObject.get("toValue");

							condition = column + " " + condOperator + " '" + fromValue + "' AND '" + toValue + "'";
						} else if (condOperator != null && "ISNULL".equalsIgnoreCase(condOperator)) {
//                                    value = "'%" + value + "%'";
							condition = column + " = SPACE ";
						} else if (condOperator != null && "ISNOTNULL".equalsIgnoreCase(condOperator)) {
//                                    value = "'%" + value + "%'";
							condition = column + " <> SPACE ";
						}
						if (i < (whereClauseObjArray.size() - 1)) {
							condition = condition + " AND ";
							if ((condOperator.equalsIgnoreCase("IN") || condOperator.equalsIgnoreCase("NOT IN"))
									&& condition.length() > 20) {
								for (int j = 0; j < condition.length(); j = j + 20) {
									int substringstartIndex = j;
									int substringendIndex = (j + 20) >= condition.length() ? condition.length()
											: (j + 20);
									String subcondition = condition.substring(substringstartIndex, substringendIndex);
									Option.appendRow();
									Option.setValue(subcondition, "TEXT");
								}
							} else {
								Option.appendRow();
								Option.setValue(condition, "TEXT");
							}

						} else {
							if ((condOperator.equalsIgnoreCase("IN") || condOperator.equalsIgnoreCase("NOT IN"))
									&& condition.length() > 20) {
								for (int j = 0; j < condition.length(); j = j + 20) {
									int substringstartIndex = j;
									int substringendIndex = (j + 20) >= condition.length() ? condition.length()
											: (j + 20);
									String subcondition = condition.substring(substringstartIndex, substringendIndex);
									Option.appendRow();
									Option.setValue(subcondition, "TEXT");
								}
							} else {
								Option.appendRow();
								Option.setValue(condition, "TEXT");
							}

						}
						WhereCondition += condition;

					}

				}

				if (anotherTableFilterData != null && !anotherTableFilterData.isEmpty()) {
					if (WhereCondition != null && !"".equalsIgnoreCase(WhereCondition)) {
						Option.appendRow();
						Option.setValue(" AND ", "TEXT");
					}
					Option.appendRow();
					Option.setValue(" " + filterSapSourceTableCompareColumn + " IN (", "TEXT");
					for (int i = 0; i < anotherTableFilterData.size(); i++) {
						if (anotherTableFilterData.get(i) instanceof Object[]) {
							Object[] rowdata = (Object[]) anotherTableFilterData.get(i);
							String value = String.valueOf(rowdata[0]).trim();
							if (i != (anotherTableFilterData.size() - 1)) {
								Option.appendRow();
								Option.setValue("'" + value + "',", "TEXT");
							} else {
								Option.appendRow();
								Option.setValue("'" + value + "'", "TEXT");
							}
						} else {
							Object rowdata = anotherTableFilterData.get(i);
							String value = String.valueOf(rowdata).trim();
							if (i != (anotherTableFilterData.size() - 1)) {
								Option.appendRow();
								Option.setValue("'" + value + "',", "TEXT");
							} else {
								Option.appendRow();
								Option.setValue("'" + value + "'", "TEXT");
							}
						}

					}
					Option.appendRow();
					Option.setValue(")", "TEXT");
				}

				long sapStartime = System.currentTimeMillis();
				try {
					sapConnection.execute(function);
				} catch (Exception e) {
					System.out.println("SAP error :: " + e.getMessage());
					sapConnection = (JCO.Client) componentUtilities.getConnection(sapConnObj);
					sapConnection.execute(function);
				}

				long sapEndTime = System.currentTimeMillis();
				System.out.println("SAP executionTime :: " + (sapEndTime - sapStartime));

				System.out.println("Before gc Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
				System.gc();
				System.out.println("After gc  Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
				int fetchCount = tableData.getNumRows();

				if (fetchCount != 0) {
					String FieldValuesStr = tableData.getString("WA");
//                            String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
					String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
					if (FieldValues.length == columnsList.size()) {
						totalDataList.add(FieldValues);
					} else {
						String[] FieldValues1 = tableData.getString("WA").split("♥", -1);
						if (FieldValues1.length == columnsList.size()) {
							totalDataList.add(FieldValues1);
						} else {
							try {
								componentUtilities.processETLLog(
										(String) request.getSession(false).getAttribute("ssUsername"),
										(String) request.getSession(false).getAttribute("ssOrgId"),
										"Fields Data Mismatched", "Error", 20, "Y", jobId);
								Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
							} catch (Exception ex) {
							}
						}
					}

				}
				while (tableData.nextRow()) {
					String FieldValuesStr = tableData.getString("WA");
					String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
					if (FieldValues.length == columnsList.size()) {
						totalDataList.add(FieldValues);
					} else {
						String[] FieldValues1 = tableData.getString("WA").split("♥", -1);
						if (FieldValues1.length == columnsList.size()) {
							totalDataList.add(FieldValues1);
						} else {
							try {
								componentUtilities.processETLLog(
										(String) request.getSession(false).getAttribute("ssUsername"),
										(String) request.getSession(false).getAttribute("ssOrgId"),
										"Fields Data Mismatched", "Error", 20, "Y", jobId);
								Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
							} catch (Exception ex) {
							}
						}
					}

				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return totalDataList;
	}

	public List fetchSapDataStandard(HttpServletRequest request, List columnsList, String tableName,
			JSONArray whereClauseObjArray, List anotherTableFilterData, String WhereCondition,
			String filterSapSourceTableCompareColumn, JSONObject sapConnObj, JCO.Client sapConnection, int start,
			int limit, String jobId) {
//        JCO.Client sapConnection = null;
		JCO.Table tableData = null;
		JCO.Table fields = null;
		List totalErpData = new ArrayList();
		JCO.Function function = null;
		List totalDataList = new ArrayList();
		try {

			if (columnsList.size() > 5) { // ravi etl integration 50 -> 25
				List<Object[]> subDataList = new ArrayList();
				for (int k = 0; k < columnsList.size(); k += 5) {

					function = getFunction("RFC_READ_TABLE"); // ravi updated code changes
					int endIndex = k + 5 < columnsList.size() ? k + 5 : columnsList.size();

//                                    System.out.println("iteration:: " + k + " Time" + System.currentTimeMillis());
					List colsSubList = columnsList.subList(k, endIndex);
					if (colsSubList != null && !colsSubList.isEmpty()) {
						if (function != null) {
							JCO.ParameterList listParams = function.getImportParameterList();
							fields = function.getTableParameterList().getTable("FIELDS");
							tableData = function.getTableParameterList().getTable("DATA");
							JCO.Table Option = function.getTableParameterList().getTable("OPTIONS");
							for (int i = 0; i < colsSubList.size(); i++) {
								// String columnName = (String) columnsList.get(i);
								String columnName = (String) colsSubList.get(i); // ravi etl integration
								fields.appendRow();
								fields.setValue(columnName, "FIELDNAME");
							}
							listParams.setValue(tableName, "QUERY_TABLE");// For MARA
							listParams.setValue("♥", "DELIMITER");

							if (limit > 0) {
								listParams.setValue((start == 0 ? (start) : (start)), "ROWSKIPS"); // ravi etl
																									// integration
								listParams.setValue((limit), "ROWCOUNT"); // ravi etl integration
							}

							if (whereClauseObjArray != null && !whereClauseObjArray.isEmpty()) {

								for (int i = 0; i < whereClauseObjArray.size(); i++) {
									JSONObject whereClauseObject = (JSONObject) whereClauseObjArray.get(i);

									String column = (String) whereClauseObject.get("column");
									String condOperator = (String) whereClauseObject.get("operator");
									String condition = "";
									String value = (String) whereClauseObject.get("value");
									if (condOperator != null && "=".equalsIgnoreCase(condOperator)) {
										value = "'" + value + "'";
										condition = column + " " + condOperator + " " + value + "";
									} else if (condOperator != null && "!=".equalsIgnoreCase(condOperator)) {
										value = "'" + value + "'";
										condition = column + " <> " + value + "";
									} else if (condOperator != null && "LIKE".equalsIgnoreCase(condOperator)) {
										value = "'%" + value + "%'";
										condition = column + " " + condOperator + " " + value + "";
									} else if (condOperator != null && "NOT LIKE".equalsIgnoreCase(condOperator)) {
										value = "'%" + value + "%'";
										condition = column + " " + condOperator + " " + value + "";
									} else if (condOperator != null && "IN".equalsIgnoreCase(condOperator)) {
//                                    value = "'%" + value + "%'";
										condition = column + " " + condOperator + " " + value + "";
									} else if (condOperator != null && "NOT IN".equalsIgnoreCase(condOperator)) {
//                                    value = "'%" + value + "%'";
										condition = column + " " + condOperator + " " + value + "";
									} else if (condOperator != null && "BETWEEN".equalsIgnoreCase(condOperator)) {
										String fromValue = (String) whereClauseObject.get("value");
										String toValue = (String) whereClauseObject.get("toValue");

										condition = column + " " + condOperator + " '" + fromValue + "' AND '" + toValue
												+ "'";
									} else if (condOperator != null && "ISNULL".equalsIgnoreCase(condOperator)) {
//                                    value = "'%" + value + "%'";
										condition = column + " = SPACE ";
									} else if (condOperator != null && "ISNOTNULL".equalsIgnoreCase(condOperator)) {
//                                    value = "'%" + value + "%'";
										condition = column + " <> SPACE ";
									}
									if (i < (whereClauseObjArray.size() - 1)) {
										condition = condition + " AND ";
										Option.appendRow();
										Option.setValue(condition, "TEXT");
									} else {
										Option.appendRow();
										Option.setValue(condition, "TEXT");

									}

								}

							}

							if (anotherTableFilterData != null && !anotherTableFilterData.isEmpty()) {
								if (WhereCondition != null && !"".equalsIgnoreCase(WhereCondition)) {
									Option.appendRow();
									Option.setValue(" AND ", "TEXT");
								}
								Option.appendRow();
								Option.setValue(" " + filterSapSourceTableCompareColumn + " IN (", "TEXT");
								for (int i = 0; i < anotherTableFilterData.size(); i++) {
									if (anotherTableFilterData.get(i) instanceof Object[]) {
										Object[] rowdata = (Object[]) anotherTableFilterData.get(i);
										String value = String.valueOf(rowdata[0]).trim();
										if (i != (anotherTableFilterData.size() - 1)) {
											Option.appendRow();
											Option.setValue("'" + value + "',", "TEXT");
										} else {
											Option.appendRow();
											Option.setValue("'" + value + "'", "TEXT");
										}
									} else {
										Object rowdata = anotherTableFilterData.get(i);
										String value = String.valueOf(rowdata).trim();
										if (i != (anotherTableFilterData.size() - 1)) {
											Option.appendRow();
											Option.setValue("'" + value + "',", "TEXT");
										} else {
											Option.appendRow();
											Option.setValue("'" + value + "'", "TEXT");
										}
									}

								}
								Option.appendRow();
								Option.setValue(")", "TEXT");
							}

							long startTime1 = System.currentTimeMillis();
							try {
								sapConnection.execute(function);
							} catch (Exception e) {
								System.out.println("SAP error :: " + e.getMessage());
								sapConnection = (JCO.Client) componentUtilities.getConnection(sapConnObj);
								sapConnection.execute(function);
							}
							long endTime = System.currentTimeMillis();
							System.out.println("SAP executionTime :: " + (endTime - startTime1) / 1000);
							System.out
									.println("Before gc Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
							System.gc();
							System.out
									.println("After gc  Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());

							int fetchCount = tableData.getNumRows();

							if (subDataList != null && !subDataList.isEmpty()) {
								int index = 0;
								if (fetchCount != 0) {

									if (colsSubList != null && !colsSubList.isEmpty()) {
										String[] FieldValues1 = tableData.getString("WA").split("♥", -1);
										String[] FieldValues = Arrays.copyOfRange(FieldValues1, 0, colsSubList.size());
										if (FieldValues.length == colsSubList.size()) {
											Object[] newDataObj = ArrayUtils.addAll(subDataList.get(index),
													FieldValues);
											subDataList.remove(index);
											subDataList.add(index, newDataObj);
										} else {
											try {
												componentUtilities.processETLLog(
														(String) request.getSession(false).getAttribute("ssUsername"),
														(String) request.getSession(false).getAttribute("ssOrgId"),
														"Fields Data Mismatched", "Error", 20, "Y", jobId);
												Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
											} catch (Exception ex) {
											}
										}
									}
									index++;
								}
								while (tableData.nextRow()) {

									if (colsSubList != null && !colsSubList.isEmpty()) {
										String[] FieldValues1 = tableData.getString("WA").split("♥", -1);
										String[] FieldValues = Arrays.copyOfRange(FieldValues1, 0, colsSubList.size());
										if (FieldValues.length == colsSubList.size()) {
											Object[] newDataObj = ArrayUtils.addAll(subDataList.get(index),
													FieldValues);
											subDataList.remove(index);
											subDataList.add(index, newDataObj);
										} else {
											try {
												componentUtilities.processETLLog(
														(String) request.getSession(false).getAttribute("ssUsername"),
														(String) request.getSession(false).getAttribute("ssOrgId"),
														"Fields Data Mismatched", "Error", 20, "Y", jobId);
												Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
											} catch (Exception ex) {
											}
										}
									}
									index++;
								}
							} else {
								if (fetchCount != 0) {

									if (colsSubList != null && !colsSubList.isEmpty()) {
										String[] FieldValues1 = tableData.getString("WA").split("♥", -1);
										String[] FieldValues = Arrays.copyOfRange(FieldValues1, 0, colsSubList.size());
										if (FieldValues.length == colsSubList.size()) {
											subDataList.add(FieldValues);
										} else {
											try {
												componentUtilities.processETLLog(
														(String) request.getSession(false).getAttribute("ssUsername"),
														(String) request.getSession(false).getAttribute("ssOrgId"),
														"Fields Data Mismatched", "Error", 20, "Y", jobId);
												Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
											} catch (Exception ex) {
											}
										}

									}
								}
								while (tableData.nextRow()) {
									if (colsSubList != null && !colsSubList.isEmpty()) {
										String[] FieldValues1 = tableData.getString("WA").split("♥", -1);
										String[] FieldValues = Arrays.copyOfRange(FieldValues1, 0, colsSubList.size());
										if (FieldValues.length == colsSubList.size()) {
											subDataList.add(FieldValues);
										} else {
											try {
												componentUtilities.processETLLog(
														(String) request.getSession(false).getAttribute("ssUsername"),
														(String) request.getSession(false).getAttribute("ssOrgId"),
														"Fields Data Mismatched", "Error", 20, "Y", jobId);
												Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
											} catch (Exception ex) {
											}
										}

									}
								}
							}

						}
					}

				}
				if (subDataList != null && !subDataList.isEmpty()) {
					totalDataList.addAll(subDataList);
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
					listParams.setValue(tableName, "QUERY_TABLE");// For MARA
					listParams.setValue("♥", "DELIMITER");
//                                    listParams.setValue((start == 0 ? (start) : (start - 1)), "ROWSKIPS");
//                                    listParams.setValue((limit + 1), "ROWCOUNT");
					if (limit > 0) {
						listParams.setValue((start == 0 ? (start) : (start)), "ROWSKIPS"); // ravi etl integration
						listParams.setValue((limit), "ROWCOUNT"); // ravi etl integration
					}

					if (whereClauseObjArray != null && !whereClauseObjArray.isEmpty()) {

						for (int i = 0; i < whereClauseObjArray.size(); i++) {
							JSONObject whereClauseObject = (JSONObject) whereClauseObjArray.get(i);

							String column = (String) whereClauseObject.get("column");
							String condOperator = (String) whereClauseObject.get("operator");
							String condition = "";
							String value = (String) whereClauseObject.get("value");
							if (condOperator != null && "=".equalsIgnoreCase(condOperator)) {
								value = "'" + value + "'";
								condition = column + " " + condOperator + " " + value + "";
							} else if (condOperator != null && "!=".equalsIgnoreCase(condOperator)) {
								value = "'" + value + "'";
								condition = column + " <> " + value + "";
							} else if (condOperator != null && "LIKE".equalsIgnoreCase(condOperator)) {
								value = "'%" + value + "%'";
								condition = column + " " + condOperator + " " + value + "";
							} else if (condOperator != null && "NOT LIKE".equalsIgnoreCase(condOperator)) {
								value = "'%" + value + "%'";
								condition = column + " " + condOperator + " " + value + "";
							} else if (condOperator != null && "IN".equalsIgnoreCase(condOperator)) {
//                                    value = "'%" + value + "%'";
								condition = column + " " + condOperator + " " + value + "";
							} else if (condOperator != null && "NOT IN".equalsIgnoreCase(condOperator)) {
//                                    value = "'%" + value + "%'";
								condition = column + " " + condOperator + " " + value + "";
							} else if (condOperator != null && "BETWEEN".equalsIgnoreCase(condOperator)) {
								String fromValue = (String) whereClauseObject.get("value");
								String toValue = (String) whereClauseObject.get("toValue");

								condition = column + " " + condOperator + " '" + fromValue + "' AND '" + toValue + "'";
							} else if (condOperator != null && "ISNULL".equalsIgnoreCase(condOperator)) {
//                                    value = "'%" + value + "%'";
								condition = column + " = SPACE ";
							} else if (condOperator != null && "ISNOTNULL".equalsIgnoreCase(condOperator)) {
//                                    value = "'%" + value + "%'";
								condition = column + " <> SPACE ";
							}
							if (i < (whereClauseObjArray.size() - 1)) {
								condition = condition + " AND ";
								Option.appendRow();
								Option.setValue(condition, "TEXT");
							} else {
								Option.appendRow();
								Option.setValue(condition, "TEXT");

							}

						}

					}

					if (anotherTableFilterData != null && !anotherTableFilterData.isEmpty()) {
						if (WhereCondition != null && !"".equalsIgnoreCase(WhereCondition)) {
							Option.appendRow();
							Option.setValue(" AND ", "TEXT");
						}
						Option.appendRow();
						Option.setValue(" " + filterSapSourceTableCompareColumn + " IN (", "TEXT");
						for (int i = 0; i < anotherTableFilterData.size(); i++) {
							if (anotherTableFilterData.get(i) instanceof Object[]) {
								Object[] rowdata = (Object[]) anotherTableFilterData.get(i);
								String value = String.valueOf(rowdata[0]).trim();
								if (i != (anotherTableFilterData.size() - 1)) {
									Option.appendRow();
									Option.setValue("'" + value + "',", "TEXT");
								} else {
									Option.appendRow();
									Option.setValue("'" + value + "'", "TEXT");
								}
							} else {
								Object rowdata = anotherTableFilterData.get(i);
								String value = String.valueOf(rowdata).trim();
								if (i != (anotherTableFilterData.size() - 1)) {
									Option.appendRow();
									Option.setValue("'" + value + "',", "TEXT");
								} else {
									Option.appendRow();
									Option.setValue("'" + value + "'", "TEXT");
								}
							}

						}
						Option.appendRow();
						Option.setValue(")", "TEXT");
					}

					long sapStartime = System.currentTimeMillis();
					try {
						sapConnection.execute(function);
					} catch (Exception e) {
						System.out.println("SAP error :: " + e.getMessage());
						sapConnection = (JCO.Client) componentUtilities.getConnection(sapConnObj);
						sapConnection.execute(function);
					}

					long sapEndTime = System.currentTimeMillis();
					System.out.println("SAP executionTime :: " + (sapEndTime - sapStartime));

					System.out.println("Before gc Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
					System.gc();
					System.out.println("After gc  Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
					int fetchCount = tableData.getNumRows();

					if (fetchCount != 0) {
						String FieldValuesStr = tableData.getString("WA");
						String[] FieldValues1 = tableData.getString("WA").split("♥", -1);
						if (FieldValues1.length == columnsList.size()) {
							totalDataList.add(FieldValues1);
						} else {
							try {
								componentUtilities.processETLLog(
										(String) request.getSession(false).getAttribute("ssUsername"),
										(String) request.getSession(false).getAttribute("ssOrgId"),
										"Fields Data Mismatched", "Error", 20, "Y", jobId);
								Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
							} catch (Exception ex) {
							}
						}

					}
					while (tableData.nextRow()) {
						String FieldValuesStr = tableData.getString("WA");
						String[] FieldValues1 = tableData.getString("WA").split("♥", -1);
						if (FieldValues1.length == columnsList.size()) {
							totalDataList.add(FieldValues1);
						} else {
							try {
								componentUtilities.processETLLog(
										(String) request.getSession(false).getAttribute("ssUsername"),
										(String) request.getSession(false).getAttribute("ssOrgId"),
										"Fields Data Mismatched", "Error", 20, "Y", jobId);
								Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
							} catch (Exception ex) {
							}
						}
					}

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return totalDataList;
	}

	public List fetchSapDataJoins(HttpServletRequest request, List columnsList, String tableName,
			JSONArray whereClauseObjArray, String WhereCondition, JSONObject sapConnObj, JCO.Client sapConnection,
			String joinCondition, String childKeyColumn, String masterKeyColumn, int start, int limit, String jobId) {
//        JCO.Client sapConnection = null;
		JCO.Table tableData = null;
		JCO.Table fields = null;
		List totalErpData = new ArrayList();
		JCO.Function function = null;
		List totalDataList = new ArrayList();
		try {

			if (columnsList.size() > 5) { // ravi etl integration 50 -> 25
				List<Object[]> subDataList = new ArrayList();
				for (int k = 0; k < columnsList.size(); k += 5) {

					function = getFunction("/PILOG/BAPI_REVERSE_ETL"); // ravi updated code changes
					int endIndex = k + 5 < columnsList.size() ? k + 5 : columnsList.size();

//                                    System.out.println("iteration:: " + k + " Time" + System.currentTimeMillis());
					List colsSubList = columnsList.subList(k, endIndex);
					if (colsSubList != null && !colsSubList.isEmpty()) {
						if (function != null) {
							JCO.ParameterList listParams = function.getImportParameterList();
							Object fieldsList = function.getTableParameterList();
							fields = function.getTableParameterList().getTable("INPTABFLDS1");
							tableData = function.getTableParameterList().getTable("OTRETURN");
//                        JCO.Table Option = function.getTableParameterList().getTable("OPTIONS");
							for (int i = 0; i < colsSubList.size(); i++) {
								String columnName = (String) colsSubList.get(i);
								fields.appendRow();
								fields.setValue(columnName, "TABLE_FIELD");
							}
							int fieldsSize = colsSubList.size();
							if (!colsSubList.contains(childKeyColumn)) {
								fields.appendRow();
								fields.setValue(childKeyColumn, "TABLE_FIELD");

							}
							if (!colsSubList.contains(masterKeyColumn)) {
								fields.appendRow();
								fields.setValue(masterKeyColumn, "TABLE_FIELD");

							}

//                        String joinCondition = "MARA INNERJOIN MARC ON MARA~MATNR = MARC~MATNR";
							String whereClauseCond = "";
							listParams.setValue(joinCondition, "INJOINCON");// For MARA
							listParams.setValue("♥", "INDELIMITOR");
//                        listParams.setValue(whereClauseCond, "INWHERECL");//For MARA
//                        listParams.setValue("♥", "INDELIMITER");

							if (limit > 0) {
								listParams.setValue((start == 0 ? (start + 1) : (start)), "ROW_SKIP"); // ravi etl
																										// integration
								listParams.setValue((limit), "ROW_COUNT"); // ravi etl integration
							}

							long startTime = System.currentTimeMillis();
							sapConnection.execute(function);
							long endTime = System.currentTimeMillis();
							System.out.println("SAP executionTime :: " + (endTime - startTime) / 1000);
							System.out
									.println("Before gc Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
							System.gc();
							System.out
									.println("After gc  Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
							int fetchCount = tableData.getNumRows();

							if (subDataList != null && !subDataList.isEmpty()) {
								int index = 0;
								if (fetchCount != 0) {

									if (colsSubList != null && !colsSubList.isEmpty()) {
										String[] FieldValues1 = tableData.getString("WA").split("♥", -1);
										String[] FieldValues = Arrays.copyOfRange(FieldValues1, 0, colsSubList.size());
										if (FieldValues.length == fieldsSize) {
											Object[] newDataObj = ArrayUtils.addAll(subDataList.get(index),
													FieldValues);
											subDataList.remove(index);
											subDataList.add(index, newDataObj);
										} else {
											try {
												componentUtilities.processETLLog(
														(String) request.getSession(false).getAttribute("ssUsername"),
														(String) request.getSession(false).getAttribute("ssOrgId"),
														"Fields Data Mismatched", "Error", 20, "Y", jobId);
												Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
											} catch (Exception ex) {
											}
										}

									}
									index++;
								}
								while (tableData.nextRow()) {
									String[] FieldValues1 = tableData.getString("WA").split("♥", -1);
									String[] FieldValues = Arrays.copyOfRange(FieldValues1, 0, colsSubList.size());
									if (FieldValues.length == fieldsSize) {
										Object[] newDataObj = ArrayUtils.addAll(subDataList.get(index), FieldValues);
										subDataList.remove(index);
										subDataList.add(index, newDataObj);
									} else {
										try {
											componentUtilities.processETLLog(
													(String) request.getSession(false).getAttribute("ssUsername"),
													(String) request.getSession(false).getAttribute("ssOrgId"),
													"Fields Data Mismatched", "Error", 20, "Y", jobId);
											Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
										} catch (Exception ex) {
										}
									}
									index++;
								}
							} else {
								if (fetchCount != 0) {

									if (colsSubList != null && !colsSubList.isEmpty()) {
										String[] FieldValues1 = tableData.getString("WA").split("♥", -1);
										String[] FieldValues = Arrays.copyOfRange(FieldValues1, 0, colsSubList.size());
										if (FieldValues.length == fieldsSize) {
											subDataList.add(FieldValues);
										} else {
											try {
												componentUtilities.processETLLog(
														(String) request.getSession(false).getAttribute("ssUsername"),
														(String) request.getSession(false).getAttribute("ssOrgId"),
														"Fields Data Mismatched", "Error", 20, "Y", jobId);
												Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
											} catch (Exception ex) {
											}
										}

									}
								}
								while (tableData.nextRow()) {
									if (colsSubList != null && !colsSubList.isEmpty()) {
										String[] FieldValues1 = tableData.getString("WA").split("♥", -1);
										String[] FieldValues = Arrays.copyOfRange(FieldValues1, 0, colsSubList.size());
										if (FieldValues.length == fieldsSize) {
											subDataList.add(FieldValues);
										} else {
											try {
												componentUtilities.processETLLog(
														(String) request.getSession(false).getAttribute("ssUsername"),
														(String) request.getSession(false).getAttribute("ssOrgId"),
														"Fields Data Mismatched", "Error", 20, "Y", jobId);
												Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
											} catch (Exception ex) {
											}
										}

									}
								}
							}

						}
					}

				}
				if (subDataList != null && !subDataList.isEmpty()) {
					totalDataList.addAll(subDataList);
				}
			} else {
				// function = getFunction("RFC_READ_TABLE");
//                    function = getFunction("/PILOG/BULK_READ_TABLE"); // ravi updated code changes
				function = getFunction("/PILOG/BAPI_REVERSE_ETL"); // ravi updated code changes
				if (function != null) {
					JCO.ParameterList listParams = function.getImportParameterList();
					Object fieldsList = function.getTableParameterList();
					fields = function.getTableParameterList().getTable("INPTABFLDS1");
					tableData = function.getTableParameterList().getTable("OTRETURN");
//                        JCO.Table Option = function.getTableParameterList().getTable("OPTIONS");
					for (int i = 0; i < columnsList.size(); i++) {
						String columnName = (String) columnsList.get(i);
						fields.appendRow();
						fields.setValue(columnName, "TABLE_FIELD");
					}

					int fieldsSize = columnsList.size();
					if (!columnsList.contains(childKeyColumn)) {
						fields.appendRow();
						fields.setValue(childKeyColumn, "TABLE_FIELD");

					}
					if (!columnsList.contains(masterKeyColumn)) {
						fields.appendRow();
						fields.setValue(masterKeyColumn, "TABLE_FIELD");

					}

//                        String joinCondition = "MARA INNERJOIN MARC ON MARA~MATNR = MARC~MATNR";
					String whereClauseCond = "";
					listParams.setValue(joinCondition, "INJOINCON");// For MARA
					listParams.setValue("♥", "INDELIMITOR");
//                        listParams.setValue(whereClauseCond, "INWHERECL");//For MARA
//                        listParams.setValue("♥", "INDELIMITER");

					if (limit > 0) {
						listParams.setValue((start == 0 ? (start + 1) : (start)), "ROW_SKIP"); // ravi etl integration
						listParams.setValue((limit), "ROW_COUNT"); // ravi etl integration
					}

//                    String WhereCondition = (String) tablesObj.get(tableName);
//                        if (WhereCondition != null && !WhereCondition.isEmpty()) {
//                            WhereCondition = WhereCondition.replaceAll(tableName + "\\.", ""); // ravi etl integration
//                            Option.appendRow();
//                            Option.setValue(WhereCondition, "TEXT");
//                        }
					long startTime = System.currentTimeMillis();
					sapConnection.execute(function);
					long endTime = System.currentTimeMillis();
					System.out.println("SAP executionTime :: " + (endTime - startTime) / 1000);
					System.out.println("Before gc Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
					System.gc();
					System.out.println("After gc  Free Memory (in bytes): " + Runtime.getRuntime().freeMemory());
					int fetchCount = tableData.getNumRows();

					if (fetchCount != 0) {
						String FieldValuessTR = tableData.getString("WA");
						String[] FieldValues1 = tableData.getString("WA").split("♥", -1);
						String[] FieldValues = Arrays.copyOfRange(FieldValues1, 0, columnsList.size());
						if (FieldValues.length == fieldsSize) {
							totalDataList.add(FieldValues);
						} else {
							try {
								componentUtilities.processETLLog(
										(String) request.getSession(false).getAttribute("ssUsername"),
										(String) request.getSession(false).getAttribute("ssOrgId"),
										"Fields Data Mismatched", "Error", 20, "Y", jobId);
								Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
							} catch (Exception ex) {
							}
						}
					}
					while (tableData.nextRow()) {
						String FieldValuessTR = tableData.getString("WA");
						String[] FieldValues1 = tableData.getString("WA").split("♥", -1);
						String[] FieldValues = Arrays.copyOfRange(FieldValues1, 0, columnsList.size());
						if (FieldValues.length == fieldsSize) {
							totalDataList.add(FieldValues);
						} else {
							try {
								componentUtilities.processETLLog(
										(String) request.getSession(false).getAttribute("ssUsername"),
										(String) request.getSession(false).getAttribute("ssOrgId"),
										"Fields Data Mismatched", "Error", 20, "Y", jobId);
								Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
							} catch (Exception ex) {
							}
						}
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return totalDataList;
	}

	public List loadSAPDataToTargetStandard(HttpServletRequest request, List columnsList, List toColumnsList,
			JSONObject fromOperator, JSONObject toOperator, String jobId) {
		JCO.Client sapConnection = null;
		JCO.Table tableData = null;
		JCO.Table fields = null;
		List totalErpData = new ArrayList();
		JCO.Function function = null;
		Object toConnection = null;
		Object anotherTableConnection = null;
		PreparedStatement preparedStatement = null;
		int totalDataCount = 0;
		int rowByRowInsertCount = 0;
		int rowByRowRejectCount = 0;
		try {

			int start = 0;
			int limit = 0;

			int end = 0;
			long startTime = System.currentTimeMillis();
			JSONObject rowsRangeObj = (JSONObject) fromOperator.get("rowsRangeObj");
			if (rowsRangeObj != null && !rowsRangeObj.isEmpty()) {
				if (String.valueOf(rowsRangeObj.get("start")) != null
						&& !"".equalsIgnoreCase(String.valueOf(rowsRangeObj.get("start")))
						&& !"null".equalsIgnoreCase(String.valueOf(rowsRangeObj.get("start")))
						&& String.valueOf(rowsRangeObj.get("end")) != null
						&& !"".equalsIgnoreCase(String.valueOf(rowsRangeObj.get("end")))
						&& !"null".equalsIgnoreCase(String.valueOf(rowsRangeObj.get("end")))) {
					try {
						start = Integer.valueOf(String.valueOf(rowsRangeObj.get("start"))) - 1;
						end = Integer.valueOf(String.valueOf(rowsRangeObj.get("end")));

						limit = (end - start);

					} catch (Exception ex) {
						start = 0;
						limit = 0;
						end = 0;
					}
				}
			}

			String tableName = String.valueOf(fromOperator.get("tableName"));
			JSONObject connObj = (JSONObject) fromOperator.get("connObj");
			sapConnection = (JCO.Client) componentUtilities.getConnection(connObj);

			retrieveRepository(sapConnection);

			List anotherTableFilterData = new ArrayList();
			String filterSapTableColumnName = "";
			String filterSapSourceTableCompareColumn = "";

			JSONObject anotherTableFilterObj = (JSONObject) fromOperator.get("anotherTableFilterObj");
			if (anotherTableFilterObj != null && !anotherTableFilterObj.isEmpty()) {
				String filterSapTableConnectionNameStr = (String) anotherTableFilterObj
						.get("filterSapTableConnectionName");
				JSONObject filterSapTableConnection = (JSONObject) JSONValue.parse(filterSapTableConnectionNameStr);
				String filterSapTableName = (String) anotherTableFilterObj.get("filterSapTableName");
				filterSapTableColumnName = (String) anotherTableFilterObj.get("filterSapTableColumnName");
				String filterSapTableRowrangeStartStr = (String) anotherTableFilterObj
						.get("filterSapTableRowrangeStart");
				String filterSapTableRowrangeEndStr = (String) anotherTableFilterObj.get("filterSapTableRowrangeEnd");
				String filterSapTableWhereCond = (String) anotherTableFilterObj.get("filterSapTableWhereCond");
				filterSapSourceTableCompareColumn = (String) anotherTableFilterObj
						.get("filterSapSourceTableCompareColumn");

//                String[] filterSapTableColumnArray = filterSapTableColumnName.split(",");
//                List filterSapTableColumnList = new ArrayList(Arrays.asList(filterSapTableColumnArray));
				List filterSapTableColumnList = new ArrayList();
				filterSapTableColumnList.add(filterSapTableColumnName);
				int filterSapTableRowrangeStart = (filterSapTableRowrangeStartStr != null
						&& !"".equalsIgnoreCase(filterSapTableRowrangeStartStr))
								? Integer.valueOf(filterSapTableRowrangeStartStr)
								: 0;
				int filterSapTableRowrangeEnd = (filterSapTableRowrangeEndStr != null
						&& !"".equalsIgnoreCase(filterSapTableRowrangeEndStr))
								? Integer.valueOf(filterSapTableRowrangeEndStr)
								: 0;
				int filterSapTableLimit = filterSapTableRowrangeEnd - filterSapTableRowrangeStart;
				anotherTableConnection = componentUtilities.getConnection(filterSapTableConnection);
				if (anotherTableConnection != null && anotherTableConnection instanceof Connection) {
					String query = "SELECT " + filterSapTableColumnName + " FROM " + filterSapTableName;
					if (filterSapTableWhereCond != null && !"".equalsIgnoreCase(filterSapTableWhereCond)) {
						query += " WHERE " + filterSapTableWhereCond;
					}

					anotherTableFilterData = processJobComponentsDAO.getTableDataWithQuery(request, query,
							filterSapTableConnection, "");
				} else if (anotherTableConnection != null && anotherTableConnection instanceof JCO.Client) {
					anotherTableFilterData = getSAPDataWithLimits(filterSapTableName, filterSapTableColumnList,
							filterSapTableConnection, filterSapTableWhereCond, filterSapTableRowrangeStart,
							filterSapTableRowrangeEnd, filterSapTableLimit);
				}

			}

			String WhereCondition = "";
			JSONArray whereClauseObjArray = (JSONArray) fromOperator.get("whereClauseObjArray");

			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");
			String fromTable = (String) fromOperator.get("tableName");
			JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
			String toTableName = (String) toOperator.get("tableName");
			String toColsStr = (String) toColumnsList.stream().map(col -> {
				if (String.valueOf(col).contains("/")) {
					col = "\"" + col + "\"";
				}
				return col;
			}).collect(Collectors.joining(","));
			String selectQuery = "SELECT " + toColsStr + " FROM " + toTableName;
			if (toConnection == null) {
				toConnection = componentUtilities.getConnection(toConnObj);
			}
//            if (toTableName.startsWith("ZZ_TEMP")) {
//                limit = 100000;
//            }
			JSONObject toColsObj = componentUtilities.getColumnsObjFromQuery(selectQuery, (Connection) toConnection,
					toConnObj);
			List toDataTypesList = (List) toColsObj.get("dataTypesList");

			if (limit > 0) {
				boolean loop = true;
				int maxLimit = 10000;
				if (limit <= maxLimit) {
					maxLimit = limit;
				}
				while (loop) {
					if (totalDataCount + maxLimit >= limit) {
						maxLimit = (end - start);
						loop = false;
					}

//                    List<Object[]> totalDataList = new ArrayList();
					List<Object[]> totalDataList = fetchSapDataStandard(request, columnsList, tableName,
							whereClauseObjArray, anotherTableFilterData, WhereCondition,
							filterSapSourceTableCompareColumn, connObj, sapConnection, start, maxLimit, jobId);

					if (totalDataList != null && !totalDataList.isEmpty()) {
						JSONObject insertResultObj = dumpSapDataToDB(request, loop, toConnObj, toTableName, columnsList,
								toDataTypesList, totalDataList, totalDataCount, startTime, jobId, rowByRowInsertCount, rowByRowRejectCount);

						loop = (boolean) insertResultObj.get("loop");
						totalDataCount = (int) insertResultObj.get("totalDataCount");

					}
					start = start + maxLimit;
					if (totalDataList.size() < maxLimit) {
						loop = false;
					}

				}
			} else {
				boolean loop = true;
				start = 0;
				limit = 10000;
				while (loop) {
//                    if (end > 0 && ((totalDataCount + limit) >= end)) {
//                        limit = (end - start);
//                        loop = false;
//                    }

					List<Object[]> totalDataList = fetchSapDataStandard(request, columnsList, tableName,
							whereClauseObjArray, anotherTableFilterData, WhereCondition,
							filterSapSourceTableCompareColumn, connObj, sapConnection, start, limit, jobId);

					if (totalDataList != null && !totalDataList.isEmpty()) {
						JSONObject insertResultObj = dumpSapDataToDB(request, loop, toConnObj, toTableName, columnsList,
								toDataTypesList, totalDataList, totalDataCount, startTime, jobId, rowByRowInsertCount, rowByRowRejectCount);

						loop = (boolean) insertResultObj.get("loop");
						totalDataCount = (int) insertResultObj.get("totalDataCount");

					}
					start = start + limit;
					if (totalDataList.size() < limit) {
						loop = false;
					}
				}
			}

			try {
				if (rowByRowInsertCount  > 0 && rowByRowRejectCount!=totalDataCount) {
					componentUtilities.processETLReconciliationUpdateTarget(
							(String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"), jobId, toTableName,
							String.valueOf(rowByRowInsertCount), String.valueOf(toOperator.get("subJobId")), String.valueOf(columnsList.size()));
					
					componentUtilities.processETLReconciliationUpdateRejectRecords(
							(String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"), jobId,
							String.valueOf(rowByRowRejectCount), String.valueOf(toOperator.get("subJobId")));
				} else {
					componentUtilities.processETLReconciliationUpdateTarget(
							(String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"), jobId, toTableName,
							String.valueOf(totalDataCount), String.valueOf(toOperator.get("subJobId")), String.valueOf(columnsList.size()));
				}

					componentUtilities.processETLJobPreview((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), jobId, String.valueOf(toOperator.get("operatorId")),
						String.valueOf(totalDataCount));
			

			} catch (Exception ex) {
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (sapConnection != null) {
//                    sapConnection.reset();
//                    sapConnection.disconnect();
//                    JCO.releaseClient(sapConnection);
				}
				if (toConnection != null && toConnection instanceof Connection) {
					((Connection) toConnection).close();
				}
				if (toConnection != null && toConnection instanceof JCO.Client) {
//                    ((JCO.Client) toConnection).reset();
//                    ((JCO.Client) toConnection).disconnect();
//                    JCO.releaseClient((JCO.Client) toConnection);
				}
				if (anotherTableConnection != null && anotherTableConnection instanceof Connection) {
					((Connection) anotherTableConnection).close();
				}

			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		return totalErpData;
	}
	
	public List loadRemainingDataToTarget(HttpServletRequest request, String pkTable, String toTableName,
			List pkColumnsList, List fromColumnsList, List toColumnsList, JSONObject fromOperator,
			JSONObject toOperator, String jobId) {
		List<Object[]> totalErpData = new ArrayList();
		Object toConnection = null;
		int totalDataCount = 0;
		JSONObject infoObject = new JSONObject();
		try {

			int start = 0;
			int limit = 10000;
			int end = 0;
			long startTime = System.currentTimeMillis();
			JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
			toConnection = componentUtilities.getConnection(toConnObj);
			String tableName = String.valueOf(fromOperator.get("tableName"));
			JSONObject connObj = (JSONObject) fromOperator.get("connObj");

			String whereClauseCondtion = "";
			JSONArray whereClauseObjArray = (JSONArray) fromOperator.get("whereClauseObjArray");
			if (whereClauseObjArray != null && !whereClauseObjArray.isEmpty()) {
				whereClauseCondtion = (String) whereClauseObjArray.stream().map(whereClauseObject -> {
					String column = (String) ((JSONObject) whereClauseObject).get("column");
					String condOperator = (String) ((JSONObject) whereClauseObject).get("operator");

					String value = (String) ((JSONObject) whereClauseObject).get("value");
					if (condOperator != null && "=".equalsIgnoreCase(condOperator)) {
						value = "'" + value + "'";
					} else if (condOperator != null && "LIKE".equalsIgnoreCase(condOperator)) {
						value = "'%" + value + "%'";
					}
					return column + " " + condOperator + " " + value + "";
				}).collect(Collectors.joining(" AND "));

			}

			String toColsStr = (String) toColumnsList.stream().map(col -> {
				if (String.valueOf(col).contains("/")) {
					col = "\"" + col + "\"";
				}
				return col;
			}).collect(Collectors.joining(","));
			String selectQuery = "SELECT " + toColsStr + " FROM " + toTableName;
			if (toConnection == null) {
				toConnection = componentUtilities.getConnection(toConnObj);
			}
			JSONObject toColsObj = componentUtilities.getColumnsObjFromQuery(selectQuery, (Connection) toConnection,
					toConnObj);
			List toDataTypesList = (List) toColsObj.get("dataTypesList");

			boolean loop = true;
			while (loop) {
				if (end > 0 && ((totalDataCount + limit) >= end)) {
					limit = (end - start);
					loop = false;
				}

				List remainingDataList = processJobComponentsDAO.getRemainingRecords(request, pkColumnsList, pkTable,
						toTableName, (Connection) toConnection);
				if (remainingDataList != null && !remainingDataList.isEmpty()) {
					List totalDataList = getRemainingSAPData(tableName, pkTable, pkColumnsList, fromColumnsList,
							connObj, remainingDataList, whereClauseCondtion, start, end, limit);

					String insertQuery = generateInsertQuery(toTableName, toColumnsList);
					if (toConnObj != null && !toConnObj.isEmpty() && toConnection == null) {
						toConnection = componentUtilities.getConnection(toConnObj);
					}
					PreparedStatement preparedStatement = ((Connection) toConnection).prepareStatement(insertQuery);

					long startInsertTime = System.currentTimeMillis();
//                    int insertCount = processJobComponentsDAO.insertDataIntoTable(request, toTableName, preparedStatement, toColumnsList, totalDataList, jobId);
//                    List<Object[]> toColsObj = componentUtilities.getTableColumnsOpt((Connection) toConnection, toConnObj, toTableName);
//                    List toColumnsDataTypes = (List) toColsObj.stream().map(rowData -> ((Object[]) rowData)[3]).collect(Collectors.toList());

					infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName,
							preparedStatement, toColumnsList, totalDataList, toColumnsList, toDataTypesList, "SAP_HANA",
							jobId, infoObject);
					int insertCount = (int)infoObject.get("insertcount");
					if (insertCount == 0) {
						loop = false;
					}
					totalDataCount += insertCount;
					System.out.println(
							"Insert time  :: " + (System.currentTimeMillis() - startInsertTime) / 1000 + " Sec");
					long endTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								"Inserted " + totalDataCount + " Records into " + toTableName + " in "
										+ (endTime - startTime) / 1000 + " sec",
								"INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}

					if (totalDataList != null && !totalDataList.isEmpty()) {
						start = start + limit;
						if (totalDataList.size() < limit) {
							loop = false;
						}
					}

				} else {
					loop = false;
				}

				// sapConnection = null;
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
//                if (sapConnection != null) {
//                    sapConnection.reset();
//                    sapConnection.disconnect();
//                    JCO.releaseClient(sapConnection);
//                }
				if (toConnection != null && toConnection instanceof Connection) {
					((Connection) toConnection).close();
				}
				if (toConnection != null && toConnection instanceof JCO.Client) {
//                    ((JCO.Client) toConnection).reset();
//                    ((JCO.Client) toConnection).disconnect();
//                    JCO.releaseClient((JCO.Client) toConnection);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		return totalErpData;
	}
	
	public List getRemainingSAPData(String tableName, String pkTable, List pkColumnsList, List fromColumnsList,
			JSONObject connObj, List remainingDataList, String whereClauseCondtion, int start, int end, int limit) {
		List<Object[]> totalDataList = new ArrayList();
		JCO.Client sapConnection = null;
		JCO.Table tableData = null;
		JCO.Table fields = null;
		JCO.Function function = null;
		try {
			sapConnection = (JCO.Client) componentUtilities.getConnection(connObj);

			retrieveRepository(sapConnection);

//            function = getFunction("RFC_READ_TABLE"); 
			function = getFunction("/PILOG/BULK_READ_TABLE");
			if (function != null) {
				JCO.ParameterList listParams = function.getImportParameterList();
				fields = function.getTableParameterList().getTable("FIELDS");
				tableData = function.getTableParameterList().getTable("DATA");
				JCO.Table Option = function.getTableParameterList().getTable("OPTIONS");
				for (int i = 0; i < fromColumnsList.size(); i++) {
					String columnName = (String) fromColumnsList.get(i);
					fields.appendRow();
					fields.setValue(columnName, "FIELDNAME");
				}
				listParams.setValue(tableName, "QUERY_TABLE");// For MARA
				listParams.setValue("♥", "DELIMITER");
//                                    listParams.setValue((start == 0 ? (start) : (start - 1)), "ROWSKIPS");
//                                    listParams.setValue((limit + 1), "ROWCOUNT");
//                    listParams.setValue((start == 0 ? (start) : (start)), "ROWSKIPS"); // ravi etl integration
//                    listParams.setValue((limit), "ROWCOUNT"); // ravi etl integration

//                        String WhereCondition = (String) tablesObj.get(tableName);
				if (whereClauseCondtion != null && !whereClauseCondtion.isEmpty()) {
					whereClauseCondtion = whereClauseCondtion.replaceAll(tableName + "\\.", ""); // ravi etl integration
					Option.appendRow();
					Option.setValue(whereClauseCondtion, "TEXT");
				}

				if (remainingDataList != null && !remainingDataList.isEmpty()) {
					String whereCond2 = "";
					for (int i = 0; i < pkColumnsList.size(); i++) {
						String columnName = (String) pkColumnsList.get(i);

						for (int j = 0; j < remainingDataList.size(); j++) {
							Object[] rowData = (Object[]) remainingDataList.get(j);
							String value = String.valueOf(rowData[i]);
							value = "'" + value + "'";
							if (j == 0) {
								whereCond2 = columnName + " IN (" + value;
								Option.appendRow();
								Option.setValue(whereCond2, "TEXT");
							} else {
								if (!whereCond2.contains(value)) {
									whereCond2 = "," + value;
									Option.appendRow();
									Option.setValue(whereCond2, "TEXT");
								}
							}

						}
						if (i == (pkColumnsList.size() - 1)) {
							whereCond2 = ") ";
							Option.appendRow();
							Option.setValue(whereCond2, "TEXT");
						} else {
							whereCond2 = ") AND ";
							Option.appendRow();
							Option.setValue(whereCond2, "TEXT");
						}

//                                                      System.out.println("columnName:::" + columnName);
					}

				}

				long startTime = System.currentTimeMillis();
				try {
					sapConnection.execute(function);
				} catch (Exception e) {
					System.out.println("SAP error :: " + e.getMessage());
					sapConnection = (JCO.Client) componentUtilities.getConnection(connObj);
					sapConnection.execute(function);
				}
				long endTime = System.currentTimeMillis();
				System.out.println("SAP executionTime :: " + (endTime - startTime) / 1000);

				int fetchCount = tableData.getNumRows();

				if (fetchCount != 0) {
//                                        Map dataObj = new HashMap();
//                    Object[] dataObj = new Object[fromColumnsList.size()];
//                    if (fromColumnsList != null && !fromColumnsList.isEmpty()) {
//                        for (int i = 0; i < fromColumnsList.size(); i++) {
//                            String[] FieldValues = tableData.getString("WA").split("", -1);
//                            String value = FieldValues[i];
//                            if (value != null
//                                    && !"".equalsIgnoreCase(value)
//                                    && !"null".equalsIgnoreCase(value)) {
//                                value = value.trim();
//                            }
//                            dataObj[i] = value;
//                        }
//                        totalDataList.add(dataObj);
//                    }

					String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
					totalDataList.add(FieldValues);
				}
				while (tableData.nextRow()) {
//                                        Map dataObj = new HashMap();
//                    Object[] dataObj = new Object[fromColumnsList.size()];
//                    if (fromColumnsList != null && !fromColumnsList.isEmpty()) {
//                        for (int i = 0; i < fromColumnsList.size(); i++) {
//                            String[] FieldValues = tableData.getString("WA").split("", -1);
//                            String value = FieldValues[i];
//                            if (value != null
//                                    && !"".equalsIgnoreCase(value)
//                                    && !"null".equalsIgnoreCase(value)) {
//                                value = value.trim();
//                            }
////                                                dataObj.put(tableName + ":" + columnsList.get(i), value);
//                            dataObj[i] = value;
//                        }
//                        totalDataList.add(dataObj);
//                    }

					String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
					totalDataList.add(FieldValues);
				}
			}

		} catch (Exception e) {

		} finally {
			if (sapConnection != null) {
//                sapConnection.reset();
//                sapConnection.disconnect();
//                JCO.releaseClient(sapConnection);
			}
		}
		return totalDataList;
	}

	public List getSAPDataWithLimits(String tableName, List fromColumnsList, JSONObject connObj,
			String whereClauseCondtion, int start, int end, int limit) {
		List<Object[]> totalDataList = new ArrayList();
		JCO.Client sapConnection = null;
		JCO.Table tableData = null;
		JCO.Table fields = null;
		JCO.Function function = null;
		try {
			sapConnection = (JCO.Client) componentUtilities.getConnection(connObj);

			retrieveRepository(sapConnection);

//            function = getFunction("RFC_READ_TABLE"); 
			function = getFunction("/PILOG/BULK_READ_TABLE");
			if (function != null) {
				JCO.ParameterList listParams = function.getImportParameterList();
				fields = function.getTableParameterList().getTable("FIELDS");
				tableData = function.getTableParameterList().getTable("DATA");
				JCO.Table Option = function.getTableParameterList().getTable("OPTIONS");
				for (int i = 0; i < fromColumnsList.size(); i++) {
					String columnName = (String) fromColumnsList.get(i);
					fields.appendRow();
					fields.setValue(columnName, "FIELDNAME");
				}
				listParams.setValue(tableName, "QUERY_TABLE");// For MARA
				listParams.setValue("♥", "DELIMITER");
//                                    listParams.setValue((start == 0 ? (start) : (start - 1)), "ROWSKIPS");
//                                    listParams.setValue((limit + 1), "ROWCOUNT");
				if (limit > 0) {
					listParams.setValue((start == 0 ? (start) : (start - 1)), "ROWSKIPS"); // ravi etl integration
					listParams.setValue((limit), "ROWCOUNT"); // ravi etl integration
				}

//                        String WhereCondition = (String) tablesObj.get(tableName);
				if (whereClauseCondtion != null && !whereClauseCondtion.isEmpty()) {
					whereClauseCondtion = whereClauseCondtion.replaceAll(tableName + "\\.", ""); // ravi etl integration
					String[] whereClauseCondArray = whereClauseCondtion.split(",");
					for (int i = 0; i < whereClauseCondArray.length; i++) {
						Option.appendRow();
						Option.setValue(whereClauseCondArray[i], "TEXT");
					}

				}

				long startTime = System.currentTimeMillis();
				try {
					sapConnection.execute(function);
				} catch (Exception e) {
					System.out.println("SAP error :: " + e.getMessage());
					sapConnection = (JCO.Client) componentUtilities.getConnection(connObj);
					sapConnection.execute(function);
				}
				long endTime = System.currentTimeMillis();
				System.out.println("SAP executionTime :: " + (endTime - startTime) / 1000);

				int fetchCount = tableData.getNumRows();

				if (fetchCount != 0) {

					String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
					totalDataList.add(FieldValues);
				}
				while (tableData.nextRow()) {

					String[] FieldValues = tableData.getString("WA").split("\\s+♥", -1);
					totalDataList.add(FieldValues);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (sapConnection != null) {
//                sapConnection.reset();
//                sapConnection.disconnect();
//                JCO.releaseClient(sapConnection);
			}
		}
		return totalDataList;
	}

	public String generateInsertQuery(String tableName, List<String> columnsList) {
		String query = "";
		try {
			String columnsStr = (String) columnsList.stream().map(col -> {
				if (col.contains("/")) {
					col = "\"" + col + "\"";
				}
				col = col.replaceAll(":", ".");
				return col;
			}).collect(Collectors.joining(","));
			String paramsStr = (String) columnsList.stream().map(e -> "?").collect(Collectors.joining(","));
			query = " INSERT INTO " + tableName + " (" + columnsStr + ")" + " VALUES (" + paramsStr + ")";
		} catch (Exception e) {
			e.printStackTrace();
		}
		return query;
	}

}
