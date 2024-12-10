/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.DAO;


import com.pilog.mdm.access.DataAccess;
import com.pilog.mdm.service.IVisionTransformComponentUtilities;
import com.pilog.mdm.transformaccess.iVisioniTransformCompHibAccess;
import com.pilog.mdm.utilities.PilogUtilities;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
//import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 *
 * @author Ravindar.P
 */
@Repository
public class IVisionTransformProcessJobComponentsDAO {

	@Autowired
	iVisioniTransformCompHibAccess hibernateAccess;

	@Autowired
	IVisionTransformComponentUtilities componentUtilities;

	@Autowired
	private DataAccess access;

	@Transactional
	public JSONObject getJobTransformationRules(HttpServletRequest request, String jobId) {

		JSONObject processJobDataObj = new JSONObject();
		String processJobData = "";
		String mappedObjectData = "";
		Map<String, Object> map = new HashMap<>();
		try {
			String query = "SELECT " + "TRFM_DEGN_DATA, "// 0
					+ "JOBS_CUST_COL1 " // 1
					+ "FROM C_DM_JOBS  " + "WHERE JOB_ID =:JOB_ID ORDER BY SEQUENCE_NO";
			map.put("JOB_ID", jobId);
			System.out.println(" query ::: " + query);

			List<Object[]> jobTransformationRulesList = access.sqlqueryWithParams(query, map);
			if (jobTransformationRulesList != null && !jobTransformationRulesList.isEmpty()) {
				mappedObjectData = new PilogUtilities().clobToString((Clob) jobTransformationRulesList.get(0)[0]);
				processJobData = new PilogUtilities().clobToString((Clob) jobTransformationRulesList.get(0)[1]);
				processJobDataObj.put("mappedObjectData", mappedObjectData);
				processJobDataObj.put("processJobDataObj", processJobData);
			}

		} catch (Exception e) {
			e.printStackTrace();
			try {
				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), e.getMessage(), "Error", 20, "Y",
						jobId);
				Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
			} catch (Exception ex) {
			}
			request.setAttribute(jobId, "Fail");
		}
		return processJobDataObj;

	}

	@Transactional
	public int deleteDuplicates(HttpServletRequest request, String tableName, List uniqueKeys, Connection connection,
								String jobId) {
		PreparedStatement preparedStatement = null;
		int deleteCount = 0;
		try {
			String uniqueKeysStr = (String) uniqueKeys.stream().map(e -> e).collect(Collectors.joining(","));
			String deleteQuery = "DELETE FROM " + tableName + " WHERE ROWID NOT IN " + "(SELECT MIN(ROWID) FROM "
					+ tableName + " GROUP BY " + uniqueKeysStr + ")";
			preparedStatement = connection.prepareStatement(deleteQuery);
			deleteCount = preparedStatement.executeUpdate();
			try {
				connection.commit();
			} catch(Exception ex) {}

		} catch (Exception e) {
			e.printStackTrace();
			try {
				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), e.getMessage(), "Error", 20, "Y",
						jobId);
				Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
			} catch (Exception ex) {
			}
			request.setAttribute(jobId, "Fail");
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			} catch (Exception ex) {
			}
		}
		return deleteCount;
	}

	@Transactional
	public int deleteUniqueRecords(HttpServletRequest request, String tableName, List uniqueKeys, Connection connection,
								   String jobId) {
		PreparedStatement preparedStatement = null;
		int deleteCount = 0;
		try {
			String uniqueKeysStr = (String) uniqueKeys.stream().map(e -> e).collect(Collectors.joining(","));
			String deleteQuery = "DELETE FROM " + tableName + " WHERE ROWID IN " + "(SELECT MIN(ROWID) FROM "
					+ tableName + " GROUP BY " + uniqueKeysStr + ")";
			preparedStatement = connection.prepareStatement(deleteQuery);
			deleteCount = preparedStatement.executeUpdate();
			try {
				connection.commit();
			} catch(Exception ex) {}
		} catch (Exception e) {
			e.printStackTrace();
			try {
				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), e.getMessage(), "Error", 20, "Y",
						jobId);
				Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
			} catch (Exception ex) {
			}
			request.setAttribute(jobId, "Fail");
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			} catch (Exception ex) {
			}
		}
		return deleteCount;
	}

	@Transactional
	public int processUniqueComponentJob(HttpServletRequest request, String fromTable, String toTable,
										 JSONArray uniqueKeys, JSONArray fromColumnsList, JSONArray toColumnsList, Connection connection,
										 String jobId) {
		int count = 0;
		PreparedStatement preparedStatement = null;
		try {
			long startTime = System.currentTimeMillis();
			String joinCondition = (String) uniqueKeys.stream().map(e -> "SRC." + e + " = " + " DEST." + e)
					.collect(Collectors.joining(" AND "));
			String srcColumns = (String) fromColumnsList.stream().map(e -> "SRC." + e)
					.collect(Collectors.joining(", "));
			String destColumns = (String) toColumnsList.stream().map(e -> "DEST." + e)
					.collect(Collectors.joining(", "));
			String query = "MERGE INTO " + toTable + " DEST USING  " + fromTable + " SRC ON (" + joinCondition + ")"
					+ " WHEN NOT MATCHED THEN " + " INSERT (" + destColumns + ") VALUES (" + srcColumns + ")";
			System.out.println("query :: " + query);
			preparedStatement = connection.prepareStatement(query);
			count = preparedStatement.executeUpdate();
			try {
				connection.commit();
			} catch(Exception ex) {}
			System.out.println("Merge time :: " + (System.currentTimeMillis() - startTime) / 1000 + " Sec");

		} catch (Exception e) {
			e.printStackTrace();
			try {
				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), e.getMessage(), "Error", 20, "Y",
						jobId);
				Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
			} catch (Exception ex) {
			}
			request.setAttribute(jobId, "Fail");
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return count;
	}

	public List getTableDataWithQuery(HttpServletRequest request, String query, JSONObject connObj, String jobId) {
		List totalDataList = new ArrayList();
		List dataList = new ArrayList();
		SessionFactory sessionFactory = null;
		int limit = 100000;
		int start = 0;
		try {

			sessionFactory = hibernateAccess.getSessionFactoryObject((String) connObj.get("HOST_NAME"),
					(String) connObj.get("CONN_PORT"), (String) connObj.get("CONN_USER_NAME"),
					(String) connObj.get("CONN_PASSWORD"), (String) connObj.get("CONN_DB_NAME"),
					(String) connObj.get("CONN_CUST_COL1"));

			dataList = hibernateAccess.queryWithParamsWithLimit(query, null, sessionFactory, limit, start);
			totalDataList.addAll(dataList);
			System.out.println("totalDataList Size :: " + totalDataList.size());
			int itrGc = 1;
			while (dataList.size() >= limit) {
//                 sessionFactory = hibernateAccess.getSessionFactoryObject((String) connObj.get("HOST_NAME"),
//                    (String) connObj.get("CONN_PORT"), (String) connObj.get("CONN_USER_NAME"), (String) connObj.get("CONN_PASSWORD"),
//                    (String) connObj.get("CONN_DB_NAME"), (String) connObj.get("CONN_CUST_COL1"));
				start = start + limit;
				dataList = hibernateAccess.queryWithParamsWithLimit(query, null, sessionFactory, limit, start);
				totalDataList.addAll(dataList);

				System.out.println("totalDataList Size :: " + totalDataList.size());
				System.gc();
//                if (totalDataList.size()==itrGc*limit*10){
//                    System.gc();
//                    itrGc++;
//                }

			}
			// method for getting Hibernate SessionFactory

		} catch (Exception e) {
			e.printStackTrace();
			if (jobId != null) {
				try {
					componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"), e.getMessage(), "Error", 20,
							"Y", jobId);
					Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
				} catch (Exception ex) {
				}
				request.setAttribute(jobId, "Fail");
			}

		} finally {
			if (sessionFactory != null) {
				sessionFactory.close();
			}
		}
		return totalDataList;
	}


	public List getTableDataWithQueryNoLimit(HttpServletRequest request, String query, JSONObject connObj, String jobId) {

		List dataList = new ArrayList();
		SessionFactory sessionFactory = null;
		int limit = 100000;
		int start = 0;
		try {

			sessionFactory = hibernateAccess.getSessionFactoryObject((String) connObj.get("HOST_NAME"),
					(String) connObj.get("CONN_PORT"), (String) connObj.get("CONN_USER_NAME"),
					(String) connObj.get("CONN_PASSWORD"), (String) connObj.get("CONN_DB_NAME"),
					(String) connObj.get("CONN_CUST_COL1"));

			dataList = hibernateAccess.queryWithParamsNoLimits(query, null, sessionFactory);



		} catch (Exception e) {
			e.printStackTrace();
			if (jobId != null) {
				try {
					componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"), e.getMessage(), "Error", 20,
							"Y", jobId);
					Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
				} catch (Exception ex) {
				}
				request.setAttribute(jobId, "Fail");
			}

		} finally {
			if (sessionFactory != null) {
				sessionFactory.close();
			}
		}
		return dataList;
	}


	@Transactional
	public int mergeUsingQuery(HttpServletRequest request, String toTableName, List fromColumnsList, List toColumnsList,
							   Connection fromConnection, String query, String jobId) {
		int insertCount = 0;
		PreparedStatement preparedStatement = null;
		try {
			String mergeQuery ="";
			String srcColumns = (String) fromColumnsList.stream().map(e -> {
				if (((String) e).contains(".")) {
					e = ((String) e).split("\\.")[1];
				}
				if (((String) e).contains(".")) {
					e = ((String) e).split("\\.")[1];
				}
				return "SRC." + e;
			}).collect(Collectors.joining(", "));
			String destColumns = (String) toColumnsList.stream().map(e -> {
				if (((String) e).contains(":")) {
					e = ((String) e).split(":")[1];
				}
				return "DEST." + e;
			}).collect(Collectors.joining(", "));
			long startTime = System.currentTimeMillis();
			if((destColumns != null && !"".equalsIgnoreCase(destColumns)) && (srcColumns != null && !"".equalsIgnoreCase(srcColumns))) {
				mergeQuery = "MERGE INTO " + toTableName + " DEST USING (" + query + ") SRC ON (1=2)"

						+ " WHEN NOT MATCHED THEN" + " INSERT (" + destColumns + ") VALUES (" + srcColumns + ")";
			} else {
				String Truncatequery = "TRUNCATE TABLE " + toTableName + "";
				preparedStatement = fromConnection.prepareStatement(Truncatequery);
				preparedStatement.executeUpdate();
				mergeQuery = "INSERT INTO " + toTableName + " " + query + "";
			}

			System.out.println("mergeQuery :: " + mergeQuery);
			preparedStatement = fromConnection.prepareStatement(mergeQuery);
			insertCount = preparedStatement.executeUpdate();
			//fromConnection.commit();
			try {
				fromConnection.commit();
			} catch(Exception ex) {}

		} catch (Exception e) {
			e.printStackTrace();
			try {
				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), e.getMessage(), "Error", 20, "Y",
						jobId);
				Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
			} catch (Exception ex) {
			}
			request.setAttribute(jobId, "Fail");
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}

			} catch (Exception ex) {

			}
		}
		return insertCount;
	}

	@Transactional
	public JSONObject mergeInsertOrUpdateSCDType2(HttpServletRequest request, String toTableName,
												  List<String> columnsList, List updateColsList, Connection fromConnection, String fromTable,
												  List<String> pkColumnsList, String jobId) {
		JSONObject resultObj = new JSONObject();
		int insertCount = 0;
		int updateCount = 0;
		PreparedStatement preparedStatement = null;
		try {
			pkColumnsList.remove("ADDRESS_KEY");
			String joinCond = pkColumnsList.stream().map(col -> " DEST." + col + " = SRC." + col)
					.collect(Collectors.joining(" AND "));
			String joinCondition = joinCond + " AND DEST.FLAG='Y'";
			String updateSetCond = columnsList.stream().filter(col -> !pkColumnsList.contains(col))
					.map(col -> " DEST." + col + " = SRC." + col).collect(Collectors.joining(", "));
			String insertValuesCond = columnsList.stream().map(col -> "SRC." + col).collect(Collectors.joining(", "));
			String insertColumns = columnsList.stream().map(col -> "DEST." + col).collect(Collectors.joining(", "));

			updateSetCond += ",ADDRESS_KEY = SYS_GUID(), DEST.END_DATE = SYSDATE";
			insertColumns += ",DEST.ADDRESS_KEY,  DEST.FLAG, DEST.START_DATE";
			insertValuesCond += ",SYS_GUID(),  'Y', SYSDATE";
//            "DEST.LOCATION = SRC.LOCATION,  DEST.FLAG = SRC.FLAG,  DEST.START_DATE = SRC.START_DATE,  DEST.END_DATE = SRC.END_DATE";
//            "SRC.NAME, SRC.LOCATION, SRC.ADDRESS_KEY, SRC.FLAG, SRC.START_DATE, SRC.END_DATE";
//            "DEST.NAME, DEST.LOCATION, DEST.ADDRESS_KEY, DEST.FLAG, DEST.START_DATE, DEST.END_DATE";

			String mergerQuery = "MERGE INTO " + toTableName + " DEST USING " + fromTable + " SRC  ON (" + joinCondition
					+ ")  " + "WHEN MATCHED THEN  UPDATE SET " + updateSetCond;
			// + "WHEN NOT MATCHED THEN INSERT ("+insertColumns+") VALUES
			// ("+insertValuesCond+")";

			System.out.println("Merge Query -->>" + mergerQuery);
			PreparedStatement stmtUpdate = fromConnection.prepareStatement(mergerQuery);
			int executeUpdateCount = stmtUpdate.executeUpdate();
			try {
				fromConnection.commit();
			} catch(Exception ex) {}
			resultObj.put("updateCount", executeUpdateCount);

			String mergerQueryUpdateActiveFlag = "MERGE INTO " + toTableName + " DEST USING  " + fromTable
					+ " SRC  ON (" + joinCond + ")  " + "WHEN MATCHED THEN  UPDATE SET DEST.FLAG='N'";
			// + "WHEN NOT MATCHED THEN INSERT ("+insertColumns+") VALUES
			// ("+insertValuesCond+")";

			System.out.println("mergerQueryUpdateActiveFlag Query -->>" + mergerQueryUpdateActiveFlag);
			PreparedStatement stmtUpdateActFlg = fromConnection.prepareStatement(mergerQueryUpdateActiveFlag);
			int executeActFlgUpdateCount = stmtUpdateActFlg.executeUpdate();
			try {
				fromConnection.commit();
			} catch(Exception ex) {}

			String insertQuery = "MERGE INTO " + toTableName + " DEST USING " + fromTable + " SRC  ON (1=2)  "
					// + "WHEN MATCHED THEN UPDATE SET "+updateSetCond
					+ "WHEN NOT MATCHED THEN  INSERT (" + insertColumns + ") VALUES (" + insertValuesCond + ")";

			System.out.println("Merge Query -->>" + insertQuery);
			PreparedStatement stmtInsert = fromConnection.prepareStatement(insertQuery);
			int executeInsertCount = stmtInsert.executeUpdate();
			try {
				fromConnection.commit();
			} catch(Exception ex) {}
			resultObj.put("insertCount", executeInsertCount);

		} catch (Exception e) {
			e.printStackTrace();
			try {
				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), e.getMessage(), "Error", 20, "Y",
						jobId);
				Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
			} catch (Exception ex) {
			}
			request.setAttribute(jobId, "Fail");
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			} catch (Exception ex) {

			}
		}
		return resultObj;
	}

	@Transactional
	public JSONObject mergeInsertOrUpdateSCDType3(HttpServletRequest request, String toTableName,
												  List<String> columnsList, List updateColsList, Connection fromConnection, String fromTable,
												  List<String> pkColumnsList, List<String> historyCols, String jobId) {
		JSONObject resultObj = new JSONObject();
		int insertCount = 0;
		int updateCount = 0;
		PreparedStatement preparedStatement = null;
		try {

			String joinCondition = pkColumnsList.stream().map(col -> " DEST." + col + " = SRC." + col)
					.collect(Collectors.joining(" AND "));
			String updateSetCond = historyCols.stream().map(col -> " DEST.PREV_" + col + " = DEST." + col)
					.collect(Collectors.joining(","));
			updateSetCond += "," + columnsList.stream().filter(col -> !pkColumnsList.contains(col))
					.map(col -> " DEST." + col + " = SRC." + col).collect(Collectors.joining(", "));

			String insertValuesCond = columnsList.stream().map(col -> "SRC." + col).collect(Collectors.joining(", "));
			String insertColumns = columnsList.stream().map(col -> "DEST." + col).collect(Collectors.joining(", "));

//            "DEST.LOCATION = SRC.LOCATION,  DEST.FLAG = SRC.FLAG,  DEST.START_DATE = SRC.START_DATE,  DEST.END_DATE = SRC.END_DATE";
//            "SRC.NAME, SRC.LOCATION, SRC.ADDRESS_KEY, SRC.FLAG, SRC.START_DATE, SRC.END_DATE";
//            "DEST.NAME, DEST.LOCATION, DEST.ADDRESS_KEY, DEST.FLAG, DEST.START_DATE, DEST.END_DATE";
			String mergerQuery = "MERGE INTO " + toTableName + " DEST USING " + fromTable + " SRC  ON (" + joinCondition
					+ ")  " + " WHEN MATCHED THEN  UPDATE SET " + updateSetCond;
//                    + " WHEN NOT MATCHED THEN  INSERT (" + insertColumns + ") VALUES (" + insertValuesCond + ")";

			System.out.println("Merge Query -->>" + mergerQuery);
			PreparedStatement stmtUpdate = fromConnection.prepareStatement(mergerQuery);
			int executeUpdateCount = stmtUpdate.executeUpdate();
			try {
				fromConnection.commit();
			} catch(Exception ex) {}
			resultObj.put("updateCount", executeUpdateCount);

			String insertQuery = "MERGE INTO " + toTableName + " DEST USING " + fromTable + " SRC  ON (" + joinCondition
					+ ")  "
					// + "WHEN MATCHED THEN UPDATE SET "+updateSetCond
					+ "WHEN NOT MATCHED THEN  INSERT (" + insertColumns + ") VALUES (" + insertValuesCond + ")";

			System.out.println("Merge Query -->>" + insertQuery);
			PreparedStatement stmtInsert = fromConnection.prepareStatement(insertQuery);
			int executeInsertCount = stmtInsert.executeUpdate();
			try {
				fromConnection.commit();
			} catch(Exception ex) {}

			resultObj.put("insertCount", executeInsertCount);

		} catch (Exception e) {
			e.printStackTrace();
			try {
				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), e.getMessage(), "Error", 20, "Y",
						jobId);
				Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
			} catch (Exception ex) {
			}
			request.setAttribute(jobId, "Fail");
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			} catch (Exception ex) {

			}
		}
		return resultObj;
	}

	@Transactional
	public JSONObject mergeSCDType4HistoryTable(HttpServletRequest request, String toTableName,
												List<String> columnsList, List updateColsList, Connection fromConnection, String fromTable,
												List<String> pkColumnsList, String jobId) {
		JSONObject resultObj = new JSONObject();
		int insertCount = 0;
		int updateCount = 0;
		PreparedStatement preparedStatement = null;
		try {
			String joinCond = pkColumnsList.stream().map(col -> " DEST." + col + " = SRC." + col)
					.collect(Collectors.joining(" AND "));
			String joinCondition = joinCond + " AND DEST.FLAG='Y'";
			String updateSetCond = " DEST.END_DATE = SYSDATE";
			String insertValuesCond = columnsList.stream().map(col -> "SRC." + col).collect(Collectors.joining(", "));
			String insertColumns = columnsList.stream().map(col -> "DEST." + col).collect(Collectors.joining(", "));

			insertColumns += ", DEST.FLAG, DEST.START_DATE";
			insertValuesCond += ", 'Y', SYSDATE";
//            "DEST.LOCATION = SRC.LOCATION,  DEST.FLAG = SRC.FLAG,  DEST.START_DATE = SRC.START_DATE,  DEST.END_DATE = SRC.END_DATE";
//            "SRC.NAME, SRC.LOCATION, SRC.ADDRESS_KEY, SRC.FLAG, SRC.START_DATE, SRC.END_DATE";
//            "DEST.NAME, DEST.LOCATION, DEST.ADDRESS_KEY, DEST.FLAG, DEST.START_DATE, DEST.END_DATE";

			String mergerQuery = "MERGE INTO " + toTableName + " DEST USING " + fromTable + " SRC  ON (" + joinCondition
					+ ")  " + "WHEN MATCHED THEN  UPDATE SET " + updateSetCond;
			// + "WHEN NOT MATCHED THEN INSERT ("+insertColumns+") VALUES
			// ("+insertValuesCond+")";

			System.out.println("Merge Query -->>" + mergerQuery);
			PreparedStatement stmtUpdate = fromConnection.prepareStatement(mergerQuery);
			int executeUpdateCount = stmtUpdate.executeUpdate();
			try {
				fromConnection.commit();
			}catch(Exception e) {}
			resultObj.put("updateCount", executeUpdateCount);

			String mergerQueryUpdateActiveFlag = "MERGE INTO " + toTableName + " DEST USING  " + fromTable
					+ " SRC  ON (" + joinCond + ")  " + "WHEN MATCHED THEN  UPDATE SET DEST.FLAG='N'";
			// + "WHEN NOT MATCHED THEN INSERT ("+insertColumns+") VALUES
			// ("+insertValuesCond+")";

			System.out.println("mergerQueryUpdateActiveFlag Query -->>" + mergerQueryUpdateActiveFlag);
			PreparedStatement stmtUpdateActFlg = fromConnection.prepareStatement(mergerQueryUpdateActiveFlag);
			int executeActFlgUpdateCount = stmtUpdateActFlg.executeUpdate();
			try {
				fromConnection.commit();
			}catch(Exception e) {}

			String insertQuery = "MERGE INTO " + toTableName + " DEST USING " + fromTable + " SRC  ON (1=2)  "
					// + "WHEN MATCHED THEN UPDATE SET "+updateSetCond
					+ "WHEN NOT MATCHED THEN  INSERT (" + insertColumns + ") VALUES (" + insertValuesCond + ")";

			System.out.println("Merge Query -->>" + insertQuery);
			PreparedStatement stmtInsert = fromConnection.prepareStatement(insertQuery);
			int executeInsertCount = stmtInsert.executeUpdate();
			try {
				fromConnection.commit();
			}catch(Exception e) {}
			resultObj.put("insertCount", executeInsertCount);

		} catch (Exception e) {
			e.printStackTrace();
			try {
				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), e.getMessage(), "Error", 20, "Y",
						jobId);
				Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
			} catch (Exception ex) {
			}
			request.setAttribute(jobId, "Fail");
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			} catch (Exception ex) {

			}
		}
		return resultObj;
	}

	@Transactional
	public JSONObject mergeInsertOrUpdateSCDType6(HttpServletRequest request, String toTableName,
												  List<String> columnsList, List updateColsList, Connection fromConnection, String fromTable,
												  List<String> pkColumnsList, List<String> historyCols, String jobId) {
		JSONObject resultObj = new JSONObject();
		int insertCount = 0;
		int updateCount = 0;
		PreparedStatement preparedStatement = null;
		try {
			pkColumnsList.remove("ADDRESS_KEY");
			String joinCond = pkColumnsList.stream().map(col -> " DEST." + col + " = SRC." + col)
					.collect(Collectors.joining(" AND "));
			String joinCondition = joinCond + " AND DEST.FLAG='Y'";
			String updateSetCond = historyCols.stream().map(col -> " DEST.PREV_" + col + " = DEST." + col)
					.collect(Collectors.joining(","));
			updateSetCond += "," + columnsList.stream().filter(col -> !pkColumnsList.contains(col))
					.map(col -> " DEST." + col + " = SRC." + col).collect(Collectors.joining(", "));

			updateSetCond += ",ADDRESS_KEY = SYS_GUID(), DEST.END_DATE = SYSDATE";

			String insertValuesCond = columnsList.stream().map(col -> "SRC." + col).collect(Collectors.joining(", "));
			String insertColumns = columnsList.stream().map(col -> "DEST." + col).collect(Collectors.joining(", "));

			insertColumns += ",DEST.ADDRESS_KEY,  DEST.FLAG, DEST.START_DATE";
			insertValuesCond += ",SYS_GUID(),  'Y', SYSDATE";
//            "DEST.LOCATION = SRC.LOCATION,  DEST.FLAG = SRC.FLAG,  DEST.START_DATE = SRC.START_DATE,  DEST.END_DATE = SRC.END_DATE";
//            "SRC.NAME, SRC.LOCATION, SRC.ADDRESS_KEY, SRC.FLAG, SRC.START_DATE, SRC.END_DATE";
//            "DEST.NAME, DEST.LOCATION, DEST.ADDRESS_KEY, DEST.FLAG, DEST.START_DATE, DEST.END_DATE";

			String mergerQuery = "MERGE INTO " + toTableName + " DEST USING " + fromTable + " SRC  ON (" + joinCondition
					+ ")  " + "WHEN MATCHED THEN  UPDATE SET " + updateSetCond;
			// + "WHEN NOT MATCHED THEN INSERT ("+insertColumns+") VALUES
			// ("+insertValuesCond+")";

			System.out.println("Merge Query -->>" + mergerQuery);
			PreparedStatement stmtUpdate = fromConnection.prepareStatement(mergerQuery);
			int executeUpdateCount = stmtUpdate.executeUpdate();
			try {
				fromConnection.commit();
			}catch(Exception e) {}
			resultObj.put("updateCount", executeUpdateCount);

			String mergerQueryUpdateActiveFlag = "MERGE INTO " + toTableName + " DEST USING  " + fromTable
					+ " SRC  ON (" + joinCond + ")  " + "WHEN MATCHED THEN  UPDATE SET DEST.FLAG='N'";
			// + "WHEN NOT MATCHED THEN INSERT ("+insertColumns+") VALUES
			// ("+insertValuesCond+")";

			System.out.println("mergerQueryUpdateActiveFlag Query -->>" + mergerQueryUpdateActiveFlag);
			PreparedStatement stmtUpdateActFlg = fromConnection.prepareStatement(mergerQueryUpdateActiveFlag);
			int executeActFlgUpdateCount = stmtUpdateActFlg.executeUpdate();
			try {
				fromConnection.commit();
			}catch(Exception e) {}

			String insertQuery = "MERGE INTO " + toTableName + " DEST USING " + fromTable + " SRC  ON (1=2)  "
					// + "WHEN MATCHED THEN UPDATE SET "+updateSetCond
					+ "WHEN NOT MATCHED THEN  INSERT (" + insertColumns + ") VALUES (" + insertValuesCond + ")";

			System.out.println("Merge Query -->>" + insertQuery);
			PreparedStatement stmtInsert = fromConnection.prepareStatement(insertQuery);
			int executeInsertCount = stmtInsert.executeUpdate();
			try {
				fromConnection.commit();
			}catch(Exception e) {}
			resultObj.put("insertCount", executeInsertCount);

		} catch (Exception e) {
			e.printStackTrace();
			try {
				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), e.getMessage(), "Error", 20, "Y",
						jobId);
				Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
			} catch (Exception ex) {
			}
			request.setAttribute(jobId, "Fail");
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			} catch (Exception ex) {

			}
		}
		return resultObj;
	}

	@Transactional
	public int mergeInsertOrUpdate(HttpServletRequest request, String toTableName, List<String> columnsList,
								   List updateColsList, Connection fromConnection, String fromTable, List<String> pkColumnsList,
								   String jobId) {
		int insertCount = 0;
		PreparedStatement preparedStatement = null;
		try {
			String joinCondition = pkColumnsList.stream().map(col -> " DEST." + col + " = SRC." + col)
					.collect(Collectors.joining(" AND "));
			String updateSetCond = (String) updateColsList.stream().filter(col -> !pkColumnsList.contains(col))
					.map(col -> " DEST." + col + " = SRC." + col).collect(Collectors.joining(", "));
			String insertValuesCond = columnsList.stream().map(col -> "SRC." + col).collect(Collectors.joining(", "));
			String insertColumns = columnsList.stream().map(col -> "DEST." + col).collect(Collectors.joining(", "));
//            fromTable = "O_RECORD_MASTER";
//            String sourceTable = "VISION_DEV.ZZ_1618992709975";
//            String sourceTable = String.valueOf(fromOperator.get("tableName"));
			String mergeQueryInsert = "MERGE INTO " + toTableName + " DEST USING " + " ( SELECT DISTINCT * FROM "
					+ fromTable + " ) SRC " + " ON (" + joinCondition + ") " + " WHEN NOT MATCHED THEN " + " INSERT ("
					+ insertColumns + ") VALUES (" + insertValuesCond + ")"
					// + " UPDATE SET DEST.REJECT_FLAG = 'Y'"
					+ " WHEN MATCHED THEN " + " UPDATE SET " + updateSetCond;
			System.out.println("Merge Query -->>" + mergeQueryInsert);
			PreparedStatement stmt = fromConnection.prepareStatement(mergeQueryInsert);
			insertCount = stmt.executeUpdate();
			try {
				fromConnection.commit();
			}catch(Exception e) {}

		} catch (Exception e) {
			e.printStackTrace();
			try {
				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), e.getMessage(), "Error", 20, "Y",
						jobId);
				Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
			} catch (Exception ex) {
			}
			request.setAttribute(jobId, "Fail");
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			} catch (Exception ex) {

			}
		}
		return insertCount;
	}

	@Transactional
	public int mergeInsert(HttpServletRequest request, String toTableName, List<Object[]> columnsList,
						   List<Object[]> updateColsList, Connection fromConnection, String fromTable, List<Object[]> pkColumnsList,
						   String jobId) {
		int insertCount = 0;
		PreparedStatement preparedStatement = null;
		try {
			String joinCondition = pkColumnsList.stream().map(pkColData -> " DEST." + pkColData[1] + " = SRC." + pkColData[0])
					.collect(Collectors.joining(" AND "));
			String insertValuesCond = columnsList.stream().map(colData -> "SRC." + colData[0]).collect(Collectors.joining(", "));
			String insertColumns = columnsList.stream().map(colData -> "DEST." + colData[1]).collect(Collectors.joining(", "));

			List fromColumnsList =  (List)columnsList.stream().map(colsData -> String.valueOf(colsData[0])).collect(Collectors.toList());
			String fromColsString = (String)fromColumnsList.stream().map(e->e).collect(Collectors.joining(", "));

			//            fromTable = "O_RECORD_MASTER";
//            String sourceTable = "VISION_DEV.ZZ_1618992709975";
//            String sourceTable = String.valueOf(fromOperator.get("tableName"));
			String mergeQueryInsert = "MERGE INTO " + toTableName + " DEST USING " + " ( SELECT "+fromColsString+" FROM "
					+ fromTable + " ) SRC " + " ON (" + joinCondition + ") " + " WHEN NOT MATCHED THEN " + " INSERT ("
					+ insertColumns + ") VALUES (" + insertValuesCond + ")";
			System.out.println("Merge Query -->>" + mergeQueryInsert);
			PreparedStatement stmt = fromConnection.prepareStatement(mergeQueryInsert);
			insertCount = stmt.executeUpdate();
			try {
				fromConnection.commit();
			}catch(Exception e) {}
		} catch (Exception e) {
			e.printStackTrace();
			try {
				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), e.getMessage(), "Error", 20, "Y",
						jobId);
				Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
				System.out.println("Thread.currentThread() :: "+Thread.currentThread());
			} catch (Exception ex) {
			}
			request.setAttribute(jobId, "Fail");
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			} catch (Exception ex) {

			}
		}
		return insertCount;
	}

	@Transactional
	public int mergeUpdate(HttpServletRequest request, String toTableName, List<Object []> columnsList,
						   List<Object []> updateColsList, Connection fromConnection, String fromTable, List<Object []> pkColumnsList,
						   String jobId) {
		int insertCount = 0;
		PreparedStatement preparedStatement = null;
		try {
			//String pkString = pkColumnsList.stream().map(pk -> pk).collect(Collectors.joining(","));

			String joinCondition = pkColumnsList.stream().map(pkColData -> " DEST." + pkColData[1] + " = SRC." + pkColData[0])
					.collect(Collectors.joining(" AND "));
			String updateSetCond = (String) updateColsList.stream().filter(updateColData ->
							!pkColumnsList.stream().anyMatch(pkcolArray -> Arrays.toString(pkcolArray).equalsIgnoreCase(Arrays.toString(updateColData)))
					//!pkColumnsList.contains(updateColData)
			).map(updateColData -> " DEST." + updateColData[1] + " = SRC." + updateColData[0]).collect(Collectors.joining(", "));
			String insertValuesCond = columnsList.stream().map(colData -> "SRC." + colData[0]).collect(Collectors.joining(", "));
			String insertColumns = columnsList.stream().map(colData -> "DEST." + colData[1]).collect(Collectors.joining(", "));
//            fromTable = "O_RECORD_MASTER";
//            String sourceTable = "VISION_DEV.ZZ_1618992709975";
//            String sourceTable = String.valueOf(fromOperator.get("tableName"));
			List fromColumnsList =  (List)columnsList.stream().map(colsData -> String.valueOf(colsData[0])).collect(Collectors.toList());
			String fromColsString = (String)fromColumnsList.stream().map(e->e).collect(Collectors.joining(", "));

			String mergeQueryInsert = "MERGE INTO " + toTableName + " DEST USING " + " ( SELECT "+fromColsString+" FROM "
					+ fromTable + " ) SRC " + " ON (" + joinCondition + ") " + " WHEN MATCHED THEN " + " UPDATE SET "
					+ updateSetCond;
			System.out.println("Merge Query -->>" + mergeQueryInsert);
			PreparedStatement stmt = fromConnection.prepareStatement(mergeQueryInsert);
			insertCount = stmt.executeUpdate();
			try {
				fromConnection.commit();
			}catch(Exception e) {}

		} catch (Exception e) {
			e.printStackTrace();
			try {
				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), e.getMessage(), "Error", 20, "Y",
						jobId);
				Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
			} catch (Exception ex) {
			}
			request.setAttribute(jobId, "Fail");
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			} catch (Exception ex) {

			}
		}
		return insertCount;
	}

	//    @Transactional
	public List getTableData(HttpServletRequest request, String fromTable, JSONArray columnsList, JSONObject connObj,
							 String jobId) {
		List totalDataList = new ArrayList();
		List dataList = new ArrayList();
		SessionFactory sessionFactory = null;
		int limit = 100000;
		int start = 0;
		try {
			String columnsListStr = (String) columnsList.stream().map(e -> e).collect(Collectors.joining(", "));
			String query = "SELECT " + columnsListStr + " FROM " + fromTable;
			System.out.println("query :: " + query);

			sessionFactory = hibernateAccess.getSessionFactoryObject((String) connObj.get("HOST_NAME"),
					(String) connObj.get("CONN_PORT"), (String) connObj.get("CONN_USER_NAME"),
					(String) connObj.get("CONN_PASSWORD"), (String) connObj.get("CONN_DB_NAME"),
					(String) connObj.get("CONN_CUST_COL1"));

			dataList = hibernateAccess.queryWithParamsWithLimit(query, null, sessionFactory, limit, start);
			totalDataList.addAll(dataList);
			int itrGc = 1;
			while (dataList.size() >= limit) {
				start = start + limit;
				dataList = hibernateAccess.queryWithParamsWithLimit(query, null, sessionFactory, limit, start);
				totalDataList.addAll(dataList);

				System.out.println("totalDataList Size :: " + totalDataList.size());
				System.gc();
//                if (totalDataList.size()==itrGc*limit*10){
//                    System.gc();
//                    itrGc++;
//                }

			}
			// method for getting Hibernate SessionFactory

		} catch (Exception e) {
			e.printStackTrace();
			try {
				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), e.getMessage(), "Error", 20, "Y",
						jobId);
				Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
			} catch (Exception ex) {
			}
			request.setAttribute(jobId, "Fail");
		} finally {
			if (sessionFactory != null) {
				try {
					sessionFactory.close();
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		}
		return totalDataList;
	}

	@Transactional
	public JSONObject insertDataIntoTable(HttpServletRequest request, String tableName,
										  PreparedStatement preparedStatement, List columnsList, List totalData, String jobId,
										  JSONObject infoObject) {

		int count = 0;
		int insertedCount = 0;
		try {
			preparedStatement.getConnection().setAutoCommit(false);
			int batchSize = 10000;
			if (totalData.size() > batchSize) {
				while (insertedCount < totalData.size()) {
//                    System.err.println("insertedCount::: "+insertedCount);
//                    System.err.println("batchSize::: "+batchSize);
					if ((insertedCount + batchSize) > totalData.size()) {
						batchSize = (totalData.size() - insertedCount);
					}
					for (int i = insertedCount; i < (insertedCount + batchSize); i++) {
						Object rowDataObj =  totalData.get(i);
						if (rowDataObj instanceof Object []) {
							Object[] rowData = (Object[]) totalData.get(i);
							for (int j = 0; j < columnsList.size(); j++) {
								preparedStatement.setObject(j + 1, rowData[j]);
							}
						} else {
							preparedStatement.setObject(1, rowDataObj);
						}

						preparedStatement.addBatch();
					}
					int[] countarray = preparedStatement.executeBatch();
					count = countarray.length;
					insertedCount += batchSize;
					preparedStatement.getConnection().commit();
//                    if ((insertedCount + batchSize) < totalData.size()) {
//                        insertedCount += batchSize;
//                    } else {
//                        batchSize = (totalData.size() - insertedCount);
//                        insertedCount += batchSize;
//                    }

				}

			} else {
				for (int i = 0; i < totalData.size(); i++) {
					Object rowDataObj =  totalData.get(i);
					if (rowDataObj instanceof Object []) {
						Object[] rowData = (Object[]) totalData.get(i);
						for (int j = 0; j < columnsList.size(); j++) {
							preparedStatement.setObject(j + 1, rowData[j]);
						}
					} else {
						preparedStatement.setObject(1, rowDataObj);
					}
					preparedStatement.addBatch();
				}
				int[] countarray = preparedStatement.executeBatch();
				insertedCount = countarray.length;
				preparedStatement.getConnection().commit();
			}

		} catch (Exception e) {
			e.printStackTrace();
			try {
				preparedStatement.getConnection().rollback();
			} catch (Exception ex) {
			}

			if (infoObject.get("skipRejectedRecords") != null
					&& "Y".equalsIgnoreCase(String.valueOf(infoObject.get("skipRejectedRecords")))) {
				infoObject = insertDataIntoTableRowByRow(request, tableName, preparedStatement, columnsList, totalData,
						jobId, infoObject);
				insertedCount = (int) infoObject.get("totalCount");
			} else {
				try {
					componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"), e.getMessage(), "Error", 20,
							"Y", jobId);
					Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
				} catch (Exception ex) {
				}
				request.setAttribute(jobId, "Fail");
			}

		} finally {
			try {
				preparedStatement.getConnection().setAutoCommit(true);
//                if (preparedStatement != null) {
//                    preparedStatement.close();
//                }
			} catch (Exception e) {
			}
		}
		infoObject.put("insertCount", insertedCount);
		return infoObject;

	}

	@Transactional
	public int updateColumnswithRowNum(HttpServletRequest request, String tableName,
									   PreparedStatement preparedStatement, List columnsList, List totalData, List rowIdList,
									   List toColumnsDataTypes, String fromDBType, String jobId) {

		int count = 0;
		int updatedCount = 0;
		int rownum = -1;
		try {

			int batchSize = 10000;
			if (totalData.size() > batchSize) {
				while (updatedCount < totalData.size()) {
//                    System.err.println("insertedCount::: "+insertedCount);
//                    System.err.println("batchSize::: "+batchSize);
					if ((updatedCount + batchSize) > totalData.size()) {
						batchSize = (totalData.size() - updatedCount);
					}
					for (int i = updatedCount; i < (updatedCount + batchSize); i++) {
						rownum = rownum + 1;
//                        System.out.println("rownum :: "+ rownum);
						Object[] rowData = (Object[]) totalData.get(i);
						for (int j = 0; j <= columnsList.size(); j++) {

//                            String dataType = String.valueOf(toColumnsDataTypes.get(j));
//                            if (dataType.equalsIgnoreCase("NUMBER")) {
//                                Object value1 = value;
//                            }
							if (j == columnsList.size()) {
								preparedStatement.setString(j + 1, (String) rowIdList.get(rownum));
							} else {
								Object value = convertIntoDBValue(rowData[j], (String) toColumnsDataTypes.get(j),
										fromDBType);
								preparedStatement.setObject(j + 1, value);
							}

						}
						preparedStatement.addBatch();
					}
					int[] countarray = preparedStatement.executeBatch();
					count = countarray.length;
					updatedCount += batchSize;
//                    if ((insertedCount + batchSize) < totalData.size()) {
//                        insertedCount += batchSize;
//                    } else {
//                        batchSize = (totalData.size() - insertedCount);
//                        insertedCount += batchSize;
//                    }

				}

			} else {
				for (int i = 0; i < totalData.size(); i++) {
					rownum = rownum + 1;
					Object[] rowData = (Object[]) totalData.get(i);
					for (int j = 0; j <= columnsList.size(); j++) {

//                        String col = String.valueOf(columnsList.get(j));
//                        String dataType = String.valueOf(toColumnsDataTypes.get(j));
//                        if (dataType.equalsIgnoreCase("FLOAT(24)")) {
//                            System.out.println(col + " :: " + value);
//                            Object value1 = value;
//                            if (col.equalsIgnoreCase("WKURS")) {
//                               value = convertIntoDBValue(rowData[j], (String) toColumnsDataTypes.get(j), fromDBType);
//                            }
//
//                        }
						if (j == columnsList.size()) {
							preparedStatement.setString(j + 1, (String) rowIdList.get(rownum));
						} else {
							Object value = convertIntoDBValue(rowData[j], (String) toColumnsDataTypes.get(j),
									fromDBType);
							preparedStatement.setObject(j + 1, value);
						}
//                        preparedStatement.setObject(j + 1, rowData[j]);
					}

					preparedStatement.addBatch();
				}
				int[] countarray = preparedStatement.executeBatch();
				count = countarray.length;
			}

		} catch (Exception e) {
			e.printStackTrace();
			try {
				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), e.getMessage(), "Error", 20, "Y",
						jobId);
				Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
			} catch (Exception ex) {
			}
			request.setAttribute(jobId, "Fail");
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			} catch (Exception e) {
			}
		}
		return updatedCount;
	}

	@Transactional
	public JSONObject insertDataIntoTable(HttpServletRequest request, String tableName,
										  PreparedStatement preparedStatement, List columnsList, List totalData, List toColumnsList,
										  List toColumnsDataTypes, String fromDBType, String jobId, JSONObject infoObject) {

		int count = 0;
		int insertedCount = 0;
		try {

			preparedStatement.getConnection().setAutoCommit(false);
			//preparedStatement.getConnection().beginRequest();
			int batchSize = 10000;
			if (totalData.size() > batchSize) {
				while (insertedCount < totalData.size()) {
//                    System.err.println("insertedCount::: "+insertedCount);
//                    System.err.println("batchSize::: "+batchSize);
					if ((insertedCount + batchSize) > totalData.size()) {
						batchSize = (totalData.size() - insertedCount);
					}
					for (int i = insertedCount; i < (insertedCount + batchSize); i++) {
						Object rowDataObj =  totalData.get(i);
						if (rowDataObj instanceof Object []) {
							Object[] rowData = (Object[]) totalData.get(i);
							Boolean flag = Arrays.stream(rowData).allMatch(obj -> obj instanceof String && ((String) obj).isEmpty());
							if (!flag) {
								for (int j = 0; j < columnsList.size(); j++) {
									Object value = convertIntoDBValue(rowData[j], (String) toColumnsDataTypes.get(j),
											fromDBType);
									preparedStatement.setObject(j + 1, value);
								}
							} else {
								continue;
							}
						} else {
							preparedStatement.setObject(1, rowDataObj);
						}

						preparedStatement.addBatch();
					}
					int[] countarray = preparedStatement.executeBatch();
					count = countarray.length;
					insertedCount += batchSize;
//                    if ((insertedCount + batchSize) < totalData.size()) {
//                        insertedCount += batchSize;
//                    } else {
//                        batchSize = (totalData.size() - insertedCount);
//                        insertedCount += batchSize;
//                    }

				}

			} else {
				for (int i = 0; i < totalData.size(); i++) {
					Object rowDataObj =  totalData.get(i);
					if (rowDataObj instanceof Object []) {
						Object[] rowData = (Object[]) totalData.get(i);
						Boolean flag = Arrays.stream(rowData).allMatch(obj -> obj instanceof String && ((String) obj).isEmpty());
						if(!flag) {
							for (int j = 0; j < columnsList.size(); j++) {
								Object value = convertIntoDBValue(rowData[j], (String) toColumnsDataTypes.get(j),
										fromDBType);
								preparedStatement.setObject(j + 1, value);

							}
						} else {
							continue;
						}

					} else {
						preparedStatement.setObject(1, rowDataObj);

					}
					preparedStatement.addBatch();
				}
				int[] countarray = preparedStatement.executeBatch();
				preparedStatement.getConnection().commit();
				insertedCount = countarray.length;
			}

		} catch (Exception e) {

			e.printStackTrace();
			try {
				preparedStatement.getConnection().rollback();
			} catch (Exception ex) {
			}
			if (infoObject.get("skipRejectedRecords") != null
					&& "Y".equalsIgnoreCase(String.valueOf(infoObject.get("skipRejectedRecords")))) {
				infoObject = insertDataIntoTableRowByRow(request, tableName, preparedStatement, columnsList, totalData,
						toColumnsList, toColumnsDataTypes, fromDBType, jobId, infoObject);
				insertedCount = (int) infoObject.get("totalCount");
			} else {
				try {
					componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"), e.getMessage(), "Error", 20,
							"Y", jobId);
					Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
				} catch (Exception ex) {
				}
				request.setAttribute(jobId, "Fail");
			}
		} finally {
			try {
				preparedStatement.getConnection().setAutoCommit(true);
//                if (preparedStatement != null) {
//                    preparedStatement.close();
//                }
			} catch (Exception e) {
			}
		}
		infoObject.put("insertCount", insertedCount);
		return infoObject;
	}


	@Transactional
	public JSONObject insertDataIntoTableSingleRow(HttpServletRequest request, String tableName,
												   PreparedStatement preparedStatement, List columnsList, List totalData, List toColumnsList,
												   List toColumnsDataTypes, String fromDBType, String jobId, JSONObject infoObject) {

		int count = 0;
		int insertedCount = 0;
		JSONObject targetDataObj = new JSONObject();
		try {

			preparedStatement.getConnection().setAutoCommit(false);
			//preparedStatement.getConnection().beginRequest();
			int batchSize = 10000;

			for (int i = 0; i < totalData.size(); i++) {
				Object rowDataObj =  totalData.get(i);
				if (rowDataObj instanceof Object []) {
					Object[] rowData = (Object[]) totalData.get(i);
					for (int j = 0; j < columnsList.size(); j++) {
						Object value = convertIntoDBValue(rowData[j], (String) toColumnsDataTypes.get(j),
								fromDBType);
						preparedStatement.setObject(j + 1, value);
						targetDataObj.put(columnsList.get(j), value);
					}
				} else {
					preparedStatement.setObject(1, rowDataObj);
				}


				preparedStatement.addBatch();
			}
			int[] countarray = preparedStatement.executeBatch();
			preparedStatement.getConnection().commit();
			insertedCount = countarray.length;
			infoObject.put("targetDataObj", targetDataObj);

		} catch (Exception e) {
			targetDataObj.put("error", e.getMessage());

			infoObject.put("targetDataObj", targetDataObj);
			e.printStackTrace();
			try {
				preparedStatement.getConnection().rollback();
			} catch (Exception ex) {
			}

		} finally {
			try {
				preparedStatement.getConnection().setAutoCommit(true);
//                if (preparedStatement != null) {
//                    preparedStatement.close();
//                }
			} catch (Exception e) {
			}
		}
		infoObject.put("insertCount", insertedCount);
		return infoObject;
	}

	public JSONObject insertDataIntoTableRowByRow(HttpServletRequest request, String tableName,
												  PreparedStatement preparedStatement, List columnsList, List totalData, String jobId,
												  JSONObject infoObject) {
		int insertCount = 0;
		int rejectedCount = 0;
		int totalCount = 0;
		try {
			if (tableName.contains(".")) {
				tableName = tableName.split("\\.")[1];
			}
			String rejectedTempTable = "TEMP_REJ_" + tableName;
			try {
				componentUtilities.dropStagingTable(rejectedTempTable, preparedStatement.getConnection());
			} catch (Exception ex) {
			}
			List rejectTableColumns = new ArrayList(columnsList);
			rejectTableColumns.add("RECJECTED_RECORD_COMMENTS");
			List dataTypeList = (List) rejectTableColumns.stream().map(e -> "VARCHAR2(4000)")
					.collect(Collectors.toList());
			try {
				componentUtilities.createTable(rejectedTempTable, rejectTableColumns, dataTypeList,
						preparedStatement.getConnection());
			} catch (Exception ex) {
			}
			for (int i = 0; i < totalData.size(); i++) {
				Object[] rowData = (Object[]) totalData.get(i);
				for (int j = 0; j < columnsList.size(); j++) {
					preparedStatement.setObject(j + 1, rowData[j]);
				}
				try {
					preparedStatement.executeUpdate();
					// preparedStatement.getConnection().commit();
					insertCount++;
				} catch (Exception ex) {
					ex.printStackTrace();
					ex.getCause();

					String insertQuery = componentUtilities.generateInsertQuery(rejectedTempTable, rejectTableColumns);
					PreparedStatement ps = preparedStatement.getConnection().prepareStatement(insertQuery);

					saveRejectedRecords(request, columnsList, ps, rowData, ex.getLocalizedMessage());
					rejectedCount++;

				}
				totalCount++;
			}

			preparedStatement.getConnection().commit();
			infoObject.put("rowByRowInsertCount", insertCount);
			infoObject.put("rowByRowRejectCount",
					request.getAttribute("rowByRowRejectCount") != null
							? ((int) request.getAttribute("rowByRowRejectCount") + rejectedCount)
							: rejectedCount);
			infoObject.put("rowByRowTotalCount",
					request.getAttribute("rowByRowTotalCount") != null
							? ((int) request.getAttribute("rowByRowTotalCount") + insertCount)
							: insertCount);

		} catch (Exception ex) {
		}
		infoObject.put("totalCount", totalCount);
		return infoObject;
	}

	public JSONObject insertDataIntoTableRowByRow(HttpServletRequest request, String tableName,
												  PreparedStatement preparedStatement, List columnsList, List totalData, List toColumnsList,
												  List toColumnsDataTypes, String fromDBType, String jobId, JSONObject infoObject) {
		int insertCount = 0;
		int rejectedCount = 0;
		int totalCount = 0;
		try {
			if (tableName.contains(".")) {
				tableName = tableName.split("\\.")[1];
			}
			String rejectedTempTable = "TEMP_REJ_" + tableName;
			componentUtilities.dropStagingTable(rejectedTempTable, preparedStatement.getConnection());

			List rejectTableColumns = new ArrayList(toColumnsList);
			rejectTableColumns.add("RECJECTED_RECORD_COMMENTS");
			List dataTypeList = (List) rejectTableColumns.stream().map(e -> "VARCHAR2(4000)")
					.collect(Collectors.toList());
			componentUtilities.createTable(rejectedTempTable, rejectTableColumns, dataTypeList,
					preparedStatement.getConnection());
			for (int i = 0; i < totalData.size(); i++) {
				Object rowDataObj =  totalData.get(i);
				if (rowDataObj instanceof Object []) {
					Object[] rowData = (Object[]) totalData.get(i);
					for (int j = 0; j < columnsList.size(); j++) {
						Object value = convertIntoDBValue(rowData[j], (String) toColumnsDataTypes.get(j),
								fromDBType);
						preparedStatement.setObject(j + 1, value);
					}
				} else {
					preparedStatement.setObject(1, rowDataObj);
				}


				try {
					preparedStatement.executeUpdate();
					// preparedStatement.getConnection().commit();
					insertCount++;
				} catch (Exception ex) {
					ex.printStackTrace();
					ex.getCause();

					String insertQuery = componentUtilities.generateInsertQuery(rejectedTempTable, rejectTableColumns);
					PreparedStatement ps = preparedStatement.getConnection().prepareStatement(insertQuery);

					saveRejectedRecords(request, toColumnsList, ps, rowDataObj, ex.getLocalizedMessage());
					rejectedCount++;

				}
				totalCount++;
			}

			preparedStatement.getConnection().commit();
			infoObject.put("rowByRowInsertCount", insertCount);
			infoObject.put("rowByRowRejectCount",
					request.getAttribute("rowByRowRejectCount") != null
							? ((int) request.getAttribute("rowByRowRejectCount") + rejectedCount)
							: rejectedCount);
			infoObject.put("rowByRowTotalCount",
					request.getAttribute("rowByRowTotalCount") != null
							? ((int) request.getAttribute("rowByRowTotalCount") + insertCount)
							: insertCount);

		} catch (Exception ex) {
		}
		infoObject.put("totalCount", totalCount);
		return infoObject;
	}

	public int saveRejectedRecords(HttpServletRequest request, List columnsList, PreparedStatement preparedStatement,
								   Object rowDataObj, String errorMessage) {
		int count = 0;
		int rowDataLength =0;
		try {
			System.out.println("Record Rejected");

			if (rowDataObj instanceof Object []) {

				Object[] rowData = (Object[]) rowDataObj;
				rowDataLength = rowData.length;
				for (int j = 0; j < columnsList.size(); j++) {
					Object value = rowData[j];
					preparedStatement.setObject(j + 1, value);
				}
			} else {
				rowDataLength =1;
				preparedStatement.setObject(1, rowDataObj);
			}

			Object value = errorMessage;
			preparedStatement.setObject(rowDataLength + 1, value);

			count = preparedStatement.executeUpdate();
			preparedStatement.getConnection().commit();
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				preparedStatement.close();
			} catch (Exception ex) {

			}
		}
		return count;
	}

	// method to convert type of value from Object to its actual type (like String,
	// Int, etc)
	public Object convertIntoDBValue(Object value, String columnType, String fromDBType) {
		try {

			if (value != null && columnType != null && !"".equalsIgnoreCase(columnType)
					&& !"null".equalsIgnoreCase(columnType)) {
				if (value instanceof String) {
					value = String.valueOf(value).trim();
				}
				if (value instanceof byte[]) {
//                    value = new BASE64Encoder().encode((byte[])value);
//                 value = new String((byte[])value, StandardCharsets.UTF_16BE);
//                    String abc = HexFormat.of().formatHex(value);
					String hash = "";

					for (byte aux : (byte[]) value) {
						int b = aux & 0xff;
						if (Integer.toHexString(b).length() == 1) {
							hash += "0";
						}
						hash += Integer.toHexString(b);
					}
					value = hash.toUpperCase();
				}
				if ("DATE".equalsIgnoreCase(columnType) || "TIMESTAMP".equalsIgnoreCase(columnType)
						|| "DATETIME".equalsIgnoreCase(columnType)) {
					try {
						if (fromDBType != null
								&& (fromDBType == "SAP" || fromDBType == "SAP_ECC" || fromDBType == "SAP_HANA")) {
							SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
							try {
								if (String.valueOf(value).equals("00000000")) {
//                                    Date localDate = simpleDateFormat.parse(simpleDateFormat.format(new Date()));
//                                    if (localDate != null) {
//                                        java.sql.Timestamp sqlDate = new java.sql.Timestamp(localDate.getTime());
//                                        value = sqlDate;
//                                    }
									value = null;
								} else {
									Date localDate = new SimpleDateFormat("yyyyMMdd").parse(String.valueOf(value));
									if (localDate != null) {
										java.sql.Timestamp sqlDate = new java.sql.Timestamp(localDate.getTime());
										value = sqlDate;
									}
								}
							} catch (Exception e) {
								Date localDate = simpleDateFormat.parse(simpleDateFormat.format(new Date()));
								if (localDate != null) {
									java.sql.Timestamp sqlDate = new java.sql.Timestamp(localDate.getTime());
									value = sqlDate;
								}
							}

						} else {

							SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");

							try {
								Date localDate = null;
								if (value instanceof String) {
									localDate = simpleDateFormat.parse(String.valueOf(value));
								} else {
									localDate = simpleDateFormat.parse(simpleDateFormat.format(value));
								}

								if (localDate != null) {
									java.sql.Timestamp sqlDate = new java.sql.Timestamp(localDate.getTime());
									value = sqlDate;
								}
							} catch (Exception e) {
								simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
								try {
									Date localDate = null;
									if (value instanceof String) {
										localDate = simpleDateFormat.parse(String.valueOf(value));
									} else {
										localDate = simpleDateFormat.parse(simpleDateFormat.format(value));
									}

									if (localDate != null) {
										java.sql.Timestamp sqlDate = new java.sql.Timestamp(localDate.getTime());
										value = sqlDate;
									}
								} catch (Exception ex) {
									Date localDate = simpleDateFormat.parse(simpleDateFormat.format(new Date()));
									if (localDate != null) {
										java.sql.Timestamp sqlDate = new java.sql.Timestamp(localDate.getTime());
										value = sqlDate;
									}
								}

							}
						}

					} catch (Exception e) {
					}

				} else if ("NUMBER".equalsIgnoreCase(columnType) || "NUMERIC".equalsIgnoreCase(columnType)
						|| "INTEGER".equalsIgnoreCase(columnType) || "INT".equalsIgnoreCase(columnType)
						|| "BIGINT".equalsIgnoreCase(columnType) || "TINYINT".equalsIgnoreCase(columnType)
						|| "SMALLINT".equalsIgnoreCase(columnType) || "MEDIUMINT".equalsIgnoreCase(columnType)) {

					BigInteger integerObj = null; // // ravi etl integration
					try {
						integerObj = new BigInteger(String.valueOf(value));

					} catch (Exception e) {
						value = 0;
					}
					if (integerObj != null) {
						value = integerObj.longValue();
					}

//                    BigInteger integerObj = new BigInteger(String.valueOf(value));
//                    if (integerObj != null) {
//                        value = integerObj.intValue();
//                    }
				} else if ("FLOAT".equalsIgnoreCase(columnType) || "FLOAT(24)".equalsIgnoreCase(columnType)
						|| "DECIMAL".equalsIgnoreCase(columnType) || "DOUBLE".equalsIgnoreCase(columnType)) {

					BigDecimal  integerObj = null; // ravi etl integration
					try {
						integerObj = new BigDecimal(String.valueOf(value));

					} catch (Exception e) {
						value = 0;
					}
					if (integerObj != null) {
						value = integerObj.doubleValue();
					}

//                    BigDecimal integerObj = new BigDecimal(String.valueOf(value));
//                    if (integerObj != null) {
//                        value = integerObj.doubleValue();
//                    }
				} else if ("VARCHAR".equalsIgnoreCase(columnType) || "VARCHAR2".equalsIgnoreCase(columnType) || "TEXT".equalsIgnoreCase(columnType)) {
					if (value instanceof Clob) {
						//value = new PiLogCloudUtills().clobToString((Clob)value);
						try {
							Clob data = (Clob)value;
							String testStr = null;
							if (data != null) {
								testStr = data.getSubString(1, (int)data.length());

							}
							value = testStr;
						}
						catch (SQLException es) {
							es.printStackTrace();
						}

					} else {
						value = String.valueOf(value);
					}

				}  else if ("CLOB".equalsIgnoreCase(columnType)) {
					if (value instanceof Clob) {
						value = new PilogUtilities().clobToString((Clob)value);
					}
					System.out.println(value);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return value;
	}

	@Transactional
	public JSONObject etlProgressBarInfo(HttpServletRequest request) {
		JSONObject processBarInfo = new JSONObject();
		String currentProcessBarIndex = request.getParameter("currentProcessBarIndex");
		String jobId = request.getParameter("jobId");
		try {
			Map map = new HashMap();
			String selectQuery = "SELECT " + "SEQUENCE_NO, "// 0
					+ "STEP_COMPLETE_FLAG, "// 1
					+ "STEP_COMPLETION_TIME, "// 2
					+ "STEP_OPERATOR, "// 3
					+ "PROCESS_FLAG "// 4
					+ "FROM RECORD_DM_PROCESS_BAR "
					+ " WHERE USER_NAME =:USER_NAME AND ORGN_ID =:ORGN_ID AND  JOB_ID =:JOB_ID ";
			if (currentProcessBarIndex != null && !"".equalsIgnoreCase(currentProcessBarIndex)
					&& !"null".equalsIgnoreCase(currentProcessBarIndex)) {
				selectQuery += " AND SEQUENCE_NO > " + currentProcessBarIndex;
			}
			selectQuery += "" + " ORDER BY CREATE_DATE,SEQUENCE_NO";
			map.put("JOB_ID", jobId);
			map.put("ORGN_ID", request.getSession(false).getAttribute("ssOrgId"));
			map.put("USER_NAME", request.getSession(false).getAttribute("ssUsername"));
			System.out.println(" query ::: " + selectQuery);

			List<Object[]> processBarInfoList = access.sqlqueryWithParams(selectQuery, map);
			if (processBarInfoList != null && !processBarInfoList.isEmpty()) {
				Object[] rowdata = processBarInfoList.get(0);
				processBarInfo.put("currentProcessBarIndex", rowdata[0]);
				processBarInfo.put("stepFlag", rowdata[1]);
				processBarInfo.put("stepCompletionTime", rowdata[2]);
				processBarInfo.put("stepOperatorId", rowdata[3]);
				processBarInfo.put("processFlag", rowdata[4]);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return processBarInfo;

	}

	@Transactional
	public List getRemainingRecords(HttpServletRequest request, List pkColumnsList, String pkTable, String toTableName,
									Connection toConnection) {
		List dataList = new ArrayList();
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		try {
			Map map = new HashMap();
			String pkColumnsListStr = (String) pkColumnsList.stream().map(col -> col).collect(Collectors.joining(","));
			String compareQuery = "SELECT " + pkColumnsListStr + " FROM " + "( SELECT " + pkColumnsListStr + " FROM "
					+ pkTable + " MINUS SELECT " + pkColumnsListStr + " FROM " + toTableName + " ) "
					// + " WHERE ROWNUM BETWEEN "+start+" AND "+end+" ";
					+ " WHERE ROWNUM BETWEEN 1 AND 10000 ";
			System.out.println("compareQuery :: " + compareQuery);
			preparedStatement = toConnection.prepareStatement(compareQuery);
			resultSet = preparedStatement.executeQuery();

			while (resultSet.next()) {
				Object[] rowdata = new Object[pkColumnsList.size()];
				for (int i = 0; i < pkColumnsList.size(); i++) {
					rowdata[i] = resultSet.getObject(i + 1);
				}
				dataList.add(rowdata);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				preparedStatement.close();
				resultSet.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return dataList;

	}

	@Transactional
	public int getRemainingRecordsCount(HttpServletRequest request, List pkColumnsList, String pkTable,
										String toTableName, Connection toConnection) {
		int count = 0;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		try {
			Map map = new HashMap();
			String pkColumnsListStr = (String) pkColumnsList.stream().map(col -> col).collect(Collectors.joining(","));
			String compareQuery = "SELECT COUNT(*) FROM " + "( SELECT " + pkColumnsListStr + " FROM " + pkTable
					+ " MINUS SELECT " + pkColumnsListStr + " FROM " + toTableName + " ) ";
//                    + " WHERE ROWNUM BETWEEN "+start+" AND "+end+" ";
//                    + " WHERE ROWNUM BETWEEN 1 AND 10000 ";
			System.out.println("compareQuery :: " + compareQuery);
			preparedStatement = toConnection.prepareStatement(compareQuery);
			resultSet = preparedStatement.executeQuery();

			while (resultSet.next()) {
				count = resultSet.getInt(1);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				preparedStatement.close();
				resultSet.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return count;

	}

	@Transactional
	public JSONObject fetchAvailableConnections(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		List connectionsList = new ArrayList();

		try {
			Map map = new HashMap();
			String query = "SELECT CONNECTION_NAME FROM DAL_DM_SAVED_CONNECTIONS "
					+ "WHERE ORGN_ID=:ORGN_ID AND USER_NAME=:USER_NAME AND CONN_CUST_COL1=:CONN_CUST_COL1";
			System.out.println("query :: " + query);
			map.put("ORGN_ID", request.getSession(false).getAttribute("ssOrgId"));
			map.put("USER_NAME", request.getSession(false).getAttribute("ssUsername"));
			map.put("CONN_CUST_COL1", request.getParameter("dbType"));

			connectionsList = access.sqlqueryWithParams(query, map);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}
		resultObj.put("connectionsList", connectionsList);
		return resultObj;

	}


	public JSONObject insertOrMergeCount(HttpServletRequest request, String toTableName, List<Object[]> columnsList,
										 List<Object[]>  updateColsList, Connection fromConnection, String fromTable, List<Object[]> pkColumnsList,
										 String jobId) {
		JSONObject resultObj = new JSONObject();
		PreparedStatement preparedStatement = null;
		try {

			String joinCondition = pkColumnsList.stream().map(pkData -> " DEST." + pkData[1] + " = SRC." + pkData[0])
					.collect(Collectors.joining(" AND "));
			String insertValuesCond = columnsList.stream().map(colData -> "SRC." + colData[0]).collect(Collectors.joining(", "));
			String insertColumns = columnsList.stream().map(colData -> "DEST." + colData[1]).collect(Collectors.joining(", "));
//		String mergeQueryInsert = "MERGE INTO " + toTableName + " DEST USING " + " ( SELECT DISTINCT * FROM "
//				+ fromTable + " ) SRC " + " ON (" + joinCondition + ") " + " WHEN NOT MATCHED THEN " + " INSERT ("
//				+ insertColumns + ") VALUES (" + insertValuesCond + ")";
//
//		System.out.println("Merge Query -->>" + mergeQueryInsert);
//		PreparedStatement insertStmt = fromConnection.prepareStatement(mergeQueryInsert);
//		int insertCount = insertStmt.executeUpdate();
//		resultObj.put("insert", insertCount);

			List fromColumnsList =  (List)columnsList.stream().map(colsData -> String.valueOf(colsData[0])).collect(Collectors.toList());
			String fromColsString = (String)fromColumnsList.stream().map(e->e).collect(Collectors.joining(", "));
			for (int i = 0; i < updateColsList.size(); i++) {
				Object[] updateColsData = updateColsList.get(i);

				String colName = updateColsList.get(i).toString();
				//281222
				// Object destPkList =
//			Object [] dummyUpdateObj = columnsList.stream().filter(colsObj ->
//
//			!pkColumnsList.stream().anyMatch(pkcolArray -> Arrays.toString(pkcolArray).equalsIgnoreCase(Arrays.toString(colsObj)))
//			&& !Arrays.toString(updateColsData).equalsIgnoreCase(Arrays.toString(colsObj))
//
//			).findFirst().get();
				//String updateSetCond = "DEST."+dummyUpdateObj[1]+" = DEST."+dummyUpdateObj[1];
				String updateSetCond = "DEST."+updateColsData[1]+" = DEST."+updateColsData[1];
				String updateWhereCond = "SRC."+updateColsData[0]+" != DEST."+updateColsData[1];


				String currentJoinCondition = joinCondition;//281222 to generate correct join condition

				//String joinConditionWithUpdateParams = "DEST." + updateColsData[1] + " <> SRC." + updateColsData[0];
				//currentJoinCondition = currentJoinCondition.concat(" AND ").concat(joinConditionWithUpdateParams);
				String mergeQueryUpdate = "MERGE INTO " + toTableName + " DEST USING " + " ( SELECT  "+fromColsString+" FROM "
						+ fromTable + " ) SRC " + " ON (" + joinCondition + ") " + " WHEN MATCHED THEN "
						+ " UPDATE SET " + updateSetCond + " WHERE "+updateWhereCond;
				System.out.println("Merge Query -->>" + mergeQueryUpdate);
				PreparedStatement updateStmt = fromConnection.prepareStatement(mergeQueryUpdate);
				int updateCount = updateStmt.executeUpdate();
				resultObj.put(updateColsData[1], updateCount);
			}
			try {
				fromConnection.commit();
			} catch (Exception e) {
			}

		} catch (Exception e) {
			e.printStackTrace();
			try {
				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), e.getMessage(), "Error", 20, "Y",
						jobId);
				Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
			} catch (Exception ex) {
			}
			request.setAttribute(jobId, "Fail");
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			} catch (Exception ex) {

			}
		}
		return resultObj;
	}


	public int executeUpdateQuery(HttpServletRequest request, String query, Connection connection, String jobId) {
		// TODO Auto-generated method stub
		int updateCount=0;
		PreparedStatement preparedStatement = null;
		try {

			System.out.println("Execution Query -->>" + query);
			PreparedStatement updateStmt = connection.prepareStatement(query);
			updateCount = updateStmt.executeUpdate();


			try {
				connection.commit();
			} catch (Exception e) {
			}

		} catch (Exception e) {
			e.printStackTrace();
			try {
				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), e.getMessage(), "Error", 20, "Y",
						jobId);
				Thread.currentThread().setName(Thread.currentThread().getName()+"_ERROR");
			} catch (Exception ex) {
			}
			request.setAttribute(jobId, "Fail");
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			} catch (Exception ex) {

			}
		}
		return updateCount;
	}

}
